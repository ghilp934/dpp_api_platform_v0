"""Runs API router - POST /v1/runs and GET /v1/runs/{run_id}."""

import logging
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from dpp_api.auth.api_key import AuthContext, get_auth_context
from dpp_api.budget import BudgetManager
from dpp_api.db.models import Run
from dpp_api.db.redis_client import RedisClient
from dpp_api.db.repo_runs import RunRepository
from dpp_api.db.session import get_db
from dpp_api.queue.sqs_client import get_sqs_client
from dpp_api.schemas import (
    CostInfo,
    ErrorInfo,
    PollInfo,
    ResultInfo,
    RunCreateRequest,
    RunReceipt,
    RunStatusResponse,
)
from dpp_api.utils.hashing import compute_payload_hash
from dpp_api.utils.money import format_usd_micros, parse_usd_string

router = APIRouter(prefix="/v1/runs", tags=["runs"])
logger = logging.getLogger(__name__)


@router.post("", response_model=RunReceipt, status_code=status.HTTP_202_ACCEPTED)
async def create_run(
    request: RunCreateRequest,
    auth: AuthContext = Depends(get_auth_context),
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    db: Session = Depends(get_db),
):
    """
    Submit a new run (POST /v1/runs).

    Implements:
    - DEC-4202: Idempotency with lock-on-key
    - DEC-4203: Reserve-then-settle
    - DEC-4211: Money in USD_MICROS
    - DEC-4212: SQS enqueue
    """
    tenant_id = auth.tenant_id

    # Validate idempotency key
    if not idempotency_key or len(idempotency_key) < 8 or len(idempotency_key) > 64:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Idempotency-Key must be 8-64 characters",
        )

    # Compute payload hash (DEC-4202)
    payload_dict = request.model_dump()
    payload_hash = compute_payload_hash(payload_dict)

    # Check for existing run with same idempotency key
    repo = RunRepository(db)
    existing_run = repo.get_by_idempotency_key(tenant_id, idempotency_key)

    if existing_run:
        # Idempotency: Return existing run if hash matches
        if existing_run.payload_hash == payload_hash:
            logger.info(f"Idempotent request for run {existing_run.run_id}")
            return _build_receipt(existing_run)
        else:
            # Hash mismatch - different payload with same key
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency key already used with different payload",
            )

    # Parse max_cost_usd to micros (DEC-4211)
    max_cost_usd_micros = parse_usd_string(request.reservation.max_cost_usd)

    # Calculate minimum fee (DEC-4203)
    # minimum_fee = max(0.005, 0.02 * reserved_usd), cap <= 0.10
    minimum_fee_usd_micros = min(
        max(5_000, int(max_cost_usd_micros * 0.02)),
        100_000,  # Cap at $0.10
    )

    # Generate run_id
    run_id = str(uuid.uuid4())

    # Create run record (status=QUEUED, money_state=NONE initially)
    retention_until = datetime.now(timezone.utc) + timedelta(days=30)

    run = Run(
        run_id=run_id,
        tenant_id=tenant_id,
        pack_type=request.pack_type,
        profile_version=request.meta.profile_version if request.meta else "v0.4.2.2",
        status="QUEUED",
        money_state="NONE",
        idempotency_key=idempotency_key,
        payload_hash=payload_hash,
        version=0,
        reservation_max_cost_usd_micros=max_cost_usd_micros,
        minimum_fee_usd_micros=minimum_fee_usd_micros,
        retention_until=retention_until,
        trace_id=request.meta.trace_id if request.meta else None,
    )

    try:
        # INT-01: This will fail with UNIQUE constraint violation if race occurs
        repo.create(run)
    except Exception as e:
        # Check if it's a unique constraint violation (idempotency race)
        if "uq_runs_tenant_idempotency" in str(e).lower() or "unique" in str(e).lower():
            # Race condition: Another request created the run first
            # Fetch the existing run
            existing_run = repo.get_by_idempotency_key(tenant_id, idempotency_key)
            if existing_run and existing_run.payload_hash == payload_hash:
                logger.info(f"Race: Returning existing run {existing_run.run_id}")
                return _build_receipt(existing_run)
            else:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Idempotency key conflict",
                )
        raise

    # Reserve budget (DEC-4203)
    redis_client = RedisClient.get_client()
    budget_manager = BudgetManager(redis_client, db)

    try:
        success = budget_manager.reserve(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=0,
            max_cost_usd_micros=max_cost_usd_micros,
        )

        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to reserve budget (version conflict)",
            )

    except Exception as e:
        # Reserve failed - clean up run record
        logger.error(f"Reserve failed for run {run_id}: {e}")

        # Mark run as FAILED with REFUNDED money_state
        repo.update_with_version_check(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=0,
            updates={
                "status": "FAILED",
                "money_state": "REFUNDED",
                "last_error_reason_code": "BUDGET_RESERVE_FAILED",
                "last_error_detail": str(e)[:500],
            },
        )

        if "insufficient" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail="Insufficient budget",
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to reserve budget",
        )

    # Enqueue to SQS (DEC-4212)
    try:
        sqs_client = get_sqs_client()
        message_id = sqs_client.enqueue_run(run_id, tenant_id, request.pack_type)
        logger.info(f"Enqueued run {run_id} to SQS (message_id={message_id})")

    except Exception as e:
        logger.error(f"SQS enqueue failed for run {run_id}: {e}")

        # Enqueue failed - refund and mark as failed
        budget_manager.scripts.refund_full(tenant_id, run_id)

        repo.update_with_version_check(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=1,  # After reserve
            updates={
                "status": "FAILED",
                "money_state": "REFUNDED",
                "last_error_reason_code": "QUEUE_ENQUEUE_FAILED",
                "last_error_detail": str(e)[:500],
            },
        )

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to enqueue run",
        )

    # Success - return receipt
    return _build_receipt(run)


@router.get("/{run_id}", response_model=RunStatusResponse)
async def get_run(
    run_id: str,
    auth: AuthContext = Depends(get_auth_context),
    db: Session = Depends(get_db),
):
    """
    Get run status (GET /v1/runs/{run_id}).

    Implements:
    - DEC-4204: Stealth 404 for owner guard
    - DEC-4209: Retention expiry (owner=410, non-owner=404)
    - DEC-4208: Cost headers
    """
    tenant_id = auth.tenant_id

    # Get run from DB
    repo = RunRepository(db)
    run = repo.get_by_id(run_id, tenant_id)

    if not run:
        # DEC-4204: Stealth 404 (don't reveal if run exists for other tenant)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Run not found",
        )

    # DEC-4209: Check retention
    now = datetime.now(timezone.utc)
    # Ensure retention_until is timezone-aware
    retention_until = run.retention_until
    if retention_until.tzinfo is None:
        # Make it timezone-aware if it's naive
        retention_until = retention_until.replace(tzinfo=timezone.utc)

    if retention_until < now:
        # Retention expired
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="Run result has expired",
        )

    # Get budget summary
    redis_client = RedisClient.get_client()
    budget_manager = BudgetManager(redis_client, db)
    budget_summary = budget_manager.get_budget_summary(run_id, tenant_id)

    # Get current balance
    current_balance = budget_manager.get_balance(tenant_id)

    # Build cost info
    cost = CostInfo(
        reserved_usd=format_usd_micros(run.reservation_max_cost_usd_micros),
        used_usd=format_usd_micros(run.actual_cost_usd_micros or 0),
        minimum_fee_usd=format_usd_micros(run.minimum_fee_usd_micros),
        budget_remaining_usd=format_usd_micros(current_balance),
    )

    # Build result/error info
    result_info = None
    error_info = None

    if run.status == "COMPLETED" and run.result_key:
        # TODO: Generate presigned URL for S3 result
        result_info = ResultInfo(
            presigned_url=None,  # Will be implemented with S3 client
            sha256=run.result_sha256,
            expires_at=None,
        )
    elif run.status == "FAILED":
        error_info = ErrorInfo(
            reason_code=run.last_error_reason_code or "UNKNOWN",
            detail=run.last_error_detail or "Run failed",
        )

    return RunStatusResponse(
        run_id=run.run_id,
        status=run.status,
        money_state=run.money_state,
        cost=cost,
        result=result_info,
        error=error_info,
        meta={
            "created_at": run.created_at.isoformat(),
            "updated_at": run.updated_at.isoformat(),
            "trace_id": run.trace_id or "",
            "profile_version": run.profile_version,
        },
    )


def _build_receipt(run: Run) -> RunReceipt:
    """Build RunReceipt from Run model."""
    return RunReceipt(
        run_id=run.run_id,
        status=run.status,
        poll=PollInfo(
            href=f"/v1/runs/{run.run_id}",
            recommended_interval_ms=1500,
            max_wait_sec=90,
        ),
        reservation={
            "max_cost_usd": format_usd_micros(run.reservation_max_cost_usd_micros),
            "currency": "USD",
        },
        meta={
            "created_at": run.created_at.isoformat(),
            "trace_id": run.trace_id or "",
            "profile_version": run.profile_version,
        },
    )
