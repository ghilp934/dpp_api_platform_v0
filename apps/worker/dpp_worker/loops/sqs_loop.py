"""SQS message processing loop for worker."""

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from sqlalchemy.orm import Session

from dpp_api.budget import BudgetManager
from dpp_api.db.repo_runs import RunRepository
from dpp_worker.executor.stub_decision import StubDecisionExecutor
from dpp_worker.finalize.optimistic_commit import (
    ClaimError,
    FinalizeError,
    finalize_failure,
    finalize_success,
)
from dpp_worker.pack_envelope import compute_envelope_sha256, create_pack_envelope

logger = logging.getLogger(__name__)


class WorkerLoop:
    """SQS message processing loop for DPP Worker.

    Implements the complete worker lifecycle:
    1. Receive message from SQS
    2. QUEUED -> PROCESSING (DB-CAS + lease)
    3. Execute pack
    4. Upload result to S3
    5. 2-phase finalize (claim + settle + commit)
    6. ACK/Delete message
    """

    def __init__(
        self,
        sqs_client: Any,
        s3_client: Any,
        db_session: Session,
        budget_manager: BudgetManager,
        queue_url: str,
        result_bucket: str,
        lease_ttl_sec: int = 120,
    ):
        """Initialize worker loop.

        Args:
            sqs_client: boto3 SQS client
            s3_client: boto3 S3 client
            db_session: Database session
            budget_manager: Budget manager instance
            queue_url: SQS queue URL
            result_bucket: S3 bucket for results
            lease_ttl_sec: Lease TTL in seconds (default 120)
        """
        self.sqs = sqs_client
        self.s3 = s3_client
        self.db = db_session
        self.budget_manager = budget_manager
        self.queue_url = queue_url
        self.result_bucket = result_bucket
        self.lease_ttl_sec = lease_ttl_sec
        self.repo = RunRepository(db_session)

        # Pack executors
        self.executors = {
            "decision": StubDecisionExecutor(),
            # Add more pack types here
        }

    def run_once(self) -> None:
        """Process one batch of messages from SQS."""
        # Receive messages (long polling)
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,  # Long polling
            VisibilityTimeout=self.lease_ttl_sec,
        )

        messages = response.get("Messages", [])
        if not messages:
            logger.debug("No messages received")
            return

        for message in messages:
            receipt_handle = message["ReceiptHandle"]
            body = json.loads(message["Body"])

            try:
                self._process_message(body)
                # Success - delete message
                self.sqs.delete_message(
                    QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
                )
                logger.info(f"Message processed and deleted: {body.get('run_id')}")

            except Exception as e:
                logger.error(f"Failed to process message: {e}", exc_info=True)
                # Message will become visible again after visibility timeout
                # or go to DLQ after max receives

    def _process_message(self, message: dict[str, Any]) -> None:
        """Process a single SQS message.

        Args:
            message: SQS message body
                {
                    "run_id": "uuid",
                    "tenant_id": "t_...",
                    "pack_type": "decision",
                    "enqueued_at": "2026-02-13T00:00:00Z",
                    "schema_version": "1"
                }
        """
        run_id = message["run_id"]
        tenant_id = message["tenant_id"]
        pack_type = message["pack_type"]

        logger.info(f"Processing run {run_id}, pack_type={pack_type}")

        # 1. Get run from DB
        run = self.repo.get_by_id(run_id, tenant_id)
        if not run:
            logger.error(f"Run {run_id} not found")
            return

        if run.status != "QUEUED":
            logger.warning(f"Run {run_id} status is {run.status}, expected QUEUED (skip)")
            return

        # 2. QUEUED -> PROCESSING (DB-CAS + lease)
        lease_token = str(uuid.uuid4())
        lease_expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=self.lease_ttl_sec
        )
        current_version = run.version

        processing_success = self.repo.update_with_version_check(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=current_version,
            updates={
                "status": "PROCESSING",
                "lease_token": lease_token,
                "lease_expires_at": lease_expires_at,
            },
        )

        if not processing_success:
            logger.warning(f"Run {run_id} already processing (0 rows affected) - skip")
            return

        logger.info(f"Run {run_id} transitioned to PROCESSING with lease {lease_token}")

        # 3. Execute pack
        try:
            executor = self.executors.get(pack_type)
            if not executor:
                raise ValueError(f"Unknown pack_type: {pack_type}")

            # Parse inputs from run (stored as JSON in DB or separate table)
            # For now, assume inputs are minimal
            inputs = {"question": "Sample question", "mode": "brief"}
            timebox_sec = 90  # Default

            envelope_data, actual_cost_usd_micros = executor.execute(
                run_id=run_id,
                inputs=inputs,
                timebox_sec=timebox_sec,
                max_cost_usd_micros=run.reservation_max_cost_usd_micros,
            )

            # 4. Create pack_envelope.json
            envelope_json = create_pack_envelope(
                run_id=run_id,
                pack_type=pack_type,
                status="COMPLETED",
                reserved_usd_micros=run.reservation_max_cost_usd_micros,
                used_usd_micros=actual_cost_usd_micros,
                minimum_fee_usd_micros=run.minimum_fee_usd_micros,
                envelope_data=envelope_data,
                trace_id=run.trace_id,
            )

            sha256_hash = compute_envelope_sha256(envelope_json)

            # 5. Upload to S3
            # Key: dpp/{tenant_id}/{yyyy}/{mm}/{dd}/{run_id}/pack_envelope.json
            now = datetime.now(timezone.utc)
            s3_key = (
                f"dpp/{tenant_id}/{now.year}/{now.month:02d}/{now.day:02d}/"
                f"{run_id}/pack_envelope.json"
            )

            self.s3.put_object(
                Bucket=self.result_bucket,
                Key=s3_key,
                Body=envelope_json.encode("utf-8"),
                ContentType="application/json; charset=utf-8",
            )

            logger.info(f"Uploaded result to s3://{self.result_bucket}/{s3_key}")

            # 6. 2-phase finalize (SUCCESS)
            try:
                result = finalize_success(
                    run_id=run_id,
                    tenant_id=tenant_id,
                    lease_token=lease_token,
                    actual_cost_usd_micros=actual_cost_usd_micros,
                    result_bucket=self.result_bucket,
                    result_key=s3_key,
                    result_sha256=sha256_hash,
                    db=self.db,
                    budget_manager=self.budget_manager,
                )

                if result == "WINNER":
                    logger.info(f"Run {run_id} finalized successfully (WINNER)")
                else:
                    logger.warning(f"Run {run_id} finalize lost race (LOSER)")

            except ClaimError as e:
                logger.warning(f"Run {run_id} claim failed (LOSER): {e}")
                # Another worker or reaper already finalized - this is OK
                return

            except FinalizeError as e:
                logger.error(f"Run {run_id} finalize failed after claim: {e}")
                # This is a problem - claim succeeded but side-effects failed
                # Reconciliation job will handle this
                raise

        except Exception as e:
            logger.error(f"Run {run_id} execution failed: {e}", exc_info=True)

            # 7. 2-phase finalize (FAILURE)
            try:
                result = finalize_failure(
                    run_id=run_id,
                    tenant_id=tenant_id,
                    lease_token=lease_token,
                    minimum_fee_usd_micros=run.minimum_fee_usd_micros,
                    error_reason_code="PACK_EXECUTION_FAILED",
                    error_detail=str(e)[:500],
                    db=self.db,
                    budget_manager=self.budget_manager,
                )

                if result == "WINNER":
                    logger.info(f"Run {run_id} finalized as FAILED (WINNER)")
                else:
                    logger.warning(f"Run {run_id} failure finalize lost race (LOSER)")

            except ClaimError as e:
                logger.warning(f"Run {run_id} failure claim failed (LOSER): {e}")
                return

            except FinalizeError as e:
                logger.error(f"Run {run_id} failure finalize failed after claim: {e}")
                raise

    def run_forever(self) -> None:
        """Run worker loop forever."""
        logger.info("Worker loop starting...")
        while True:
            try:
                self.run_once()
            except KeyboardInterrupt:
                logger.info("Worker loop stopping...")
                break
            except Exception as e:
                logger.error(f"Worker loop error: {e}", exc_info=True)
                # Continue processing
