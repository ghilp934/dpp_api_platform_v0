"""SQS message processing loop for worker."""

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import boto3
import redis
from sqlalchemy.orm import Session

from dpp_api.budget import BudgetManager
from dpp_api.db.repo_runs import RunRepository
from dpp_worker.executor.stub_decision import StubDecisionExecutor
from dpp_worker.finalize.optimistic_commit import (
    ClaimError,
    FinalizeError,
    claim_finalize,
    commit_finalize,
    finalize_failure,
    finalize_success,
)
from dpp_worker.heartbeat import HeartbeatThread
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
        session_factory: Callable[[], Session],
        budget_manager: BudgetManager,
        queue_url: str,
        result_bucket: str,
        redis_client: redis.Redis | None = None,
        lease_ttl_sec: int = 120,
    ):
        """Initialize worker loop.

        Args:
            sqs_client: boto3 SQS client
            s3_client: boto3 S3 client
            db_session: Database session (main thread)
            session_factory: SessionLocal factory for creating thread-safe sessions (P0-1)
            budget_manager: Budget manager instance
            queue_url: SQS queue URL
            result_bucket: S3 bucket for results
            redis_client: Redis client for lease management (DEC-4205)
            lease_ttl_sec: Lease TTL in seconds (default 120)
        """
        self.sqs = sqs_client
        self.s3 = s3_client
        self.db = db_session
        self.session_factory = session_factory
        self.budget_manager = budget_manager
        self.queue_url = queue_url
        self.result_bucket = result_bucket
        self.redis = redis_client
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
                # P0-1: _process_message returns bool (True=delete ok, False=no delete)
                should_delete = self._process_message(body, receipt_handle)
                if should_delete:
                    # Success - delete message
                    self.sqs.delete_message(
                        QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
                    )
                    logger.info(f"Message processed and deleted: {body.get('run_id')}")
                else:
                    logger.warning(
                        f"Message processing incomplete (claim failed) - "
                        f"message will be retried: {body.get('run_id')}"
                    )

            except Exception as e:
                logger.error(f"Failed to process message: {e}", exc_info=True)
                # Message will become visible again after visibility timeout
                # or go to DLQ after max receives

    def _process_message(self, message: dict[str, Any], receipt_handle: str) -> bool:
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
            receipt_handle: SQS message receipt handle for heartbeat

        Returns:
            True if message should be deleted (success or permanent failure)
            False if message should NOT be deleted (claim failed - can retry)
        """
        run_id = message["run_id"]
        tenant_id = message["tenant_id"]
        pack_type = message["pack_type"]

        logger.info(f"Processing run {run_id}, pack_type={pack_type}")

        # 1. Get run from DB
        run = self.repo.get_by_id(run_id, tenant_id)
        if not run:
            logger.error(f"Run {run_id} not found")
            return True  # Permanent error - delete message

        if run.status != "QUEUED":
            logger.warning(f"Run {run_id} status is {run.status}, expected QUEUED (skip)")
            return True  # Already processed or failed - delete message

        # 2. QUEUED -> PROCESSING (DB-CAS + lease)
        lease_token = str(uuid.uuid4())
        lease_expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=self.lease_ttl_sec
        )
        current_version = run.version

        # WKR-01: Strict state transition with extra_conditions
        processing_success = self.repo.update_with_version_check(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=current_version,
            updates={
                "status": "PROCESSING",
                "lease_token": lease_token,
                "lease_expires_at": lease_expires_at,
            },
            extra_conditions={
                "status": "QUEUED",  # Ensure we're transitioning from QUEUED
            },
        )

        if not processing_success:
            logger.warning(f"Run {run_id} already processing (0 rows affected) - skip")
            return True  # Another worker claimed it - delete message

        # Step 4 (Spec 9.1): Redis lease:{run_id} SETNX TTL=120 (DEC-4205)
        if self.redis:
            lease_key = f"lease:{run_id}"
            self.redis.set(lease_key, lease_token, ex=self.lease_ttl_sec, nx=True)

        logger.info(f"Run {run_id} transitioned to PROCESSING with lease {lease_token}")

        # P0-D: Start heartbeat thread to prevent zombie detection
        # P0-1: Pass session_factory instead of db_session for thread-safety
        # Version after PROCESSING transition is current_version + 1
        processing_version = current_version + 1
        heartbeat = HeartbeatThread(
            run_id=run_id,
            tenant_id=tenant_id,
            lease_token=lease_token,
            current_version=processing_version,
            session_factory=self.session_factory,
            sqs_client=self.sqs,
            queue_url=self.queue_url,
            receipt_handle=receipt_handle,
            heartbeat_interval_sec=30,  # Send heartbeat every 30s
            lease_extension_sec=self.lease_ttl_sec,  # Extend by 120s each time
        )
        heartbeat.start()

        # 3. Execute pack
        try:
            executor = self.executors.get(pack_type)
            if not executor:
                raise ValueError(f"Unknown pack_type: {pack_type}")

            # P1-7: Use persisted reservation parameters and inputs from DB
            inputs = run.inputs_json or {"question": "Sample question", "mode": "brief"}
            timebox_sec = run.timebox_sec or 90  # Use persisted value or default
            # min_reliability_score = run.min_reliability_score or 0.8  # Available if executor needs it

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

            # 5. PHASE 1: CLAIM (P0-2: Claim-Check pattern)
            # CRITICAL: Claim BEFORE any side-effects (S3 upload)
            # P0-1: Stop heartbeat BEFORE finalize to prevent version conflict
            heartbeat.stop()
            logger.debug(f"Heartbeat stopped before finalize for run {run_id}")

            try:
                finalize_token, claimed_version = claim_finalize(
                    run_id=run_id,
                    tenant_id=tenant_id,
                    extra_claim_conditions={"lease_token": lease_token},
                    db=self.db,
                )
                logger.info(f"Run {run_id} claimed for finalize (token={finalize_token})")

            except ClaimError as e:
                logger.warning(f"Run {run_id} claim failed (LOSER): {e}")
                # P0-1: Another worker or reaper already finalized - this is OK
                # Do NOT upload to S3 since we lost the race
                # Do NOT delete SQS message - allow retry in case of transient issue
                return False

            # 6. PHASE 2: S3 UPLOAD (only after successful claim)
            # Key: dpp/{tenant_id}/{yyyy}/{mm}/{dd}/{run_id}/pack_envelope.json
            now = datetime.now(timezone.utc)
            s3_key = (
                f"dpp/{tenant_id}/{now.year}/{now.month:02d}/{now.day:02d}/"
                f"{run_id}/pack_envelope.json"
            )

            try:
                # MS-6: Include actual_cost in S3 metadata for idempotent reconciliation
                self.s3.put_object(
                    Bucket=self.result_bucket,
                    Key=s3_key,
                    Body=envelope_json.encode("utf-8"),
                    ContentType="application/json; charset=utf-8",
                    Metadata={
                        "actual-cost-usd-micros": str(actual_cost_usd_micros),
                    },
                )
                logger.info(
                    f"Uploaded result to s3://{self.result_bucket}/{s3_key} "
                    f"(actual_cost={actual_cost_usd_micros})"
                )

            except Exception as e:
                logger.error(f"S3 upload failed after claim: {e}", exc_info=True)
                # S3 upload failed after claim - run is stuck in CLAIMED state
                # Reaper will eventually handle this
                raise FinalizeError(f"S3 upload failed after claim: {e}")

            # 7. PHASE 3: COMMIT (settle + final DB commit)
            try:
                result = commit_finalize(
                    run_id=run_id,
                    tenant_id=tenant_id,
                    finalize_token=finalize_token,
                    claimed_version=claimed_version,
                    charge_usd_micros=actual_cost_usd_micros,
                    final_status="COMPLETED",
                    extra_final_updates={
                        "result_bucket": self.result_bucket,
                        "result_key": s3_key,
                        "result_sha256": sha256_hash,
                    },
                    db=self.db,
                    budget_manager=self.budget_manager,
                )

                if result == "WINNER":
                    logger.info(f"Run {run_id} finalized successfully (WINNER)")
                else:
                    logger.warning(f"Run {run_id} finalize commit returned unexpected result")

                # P0-1: Success - delete message
                return True

            except FinalizeError as e:
                logger.error(f"Run {run_id} commit failed after claim and S3 upload: {e}")
                # This is a problem - claim succeeded, S3 uploaded, but commit failed
                # Reconciliation job will handle this
                raise

        except Exception as e:
            logger.error(f"Run {run_id} execution failed: {e}", exc_info=True)

            # P0-1: Stop heartbeat before failure finalize
            heartbeat.stop()
            logger.debug(f"Heartbeat stopped before failure finalize for run {run_id}")

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

                # P0-1: Failure finalized - delete message
                return True

            except ClaimError as e:
                logger.warning(f"Run {run_id} failure claim failed (LOSER): {e}")
                # P0-1: Another worker finalized - don't delete message, allow retry
                return False

            except FinalizeError as e:
                logger.error(f"Run {run_id} failure finalize failed after claim: {e}")
                # Claim succeeded but finalize failed - raise to avoid deleting message
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
