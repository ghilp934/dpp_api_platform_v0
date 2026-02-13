"""Runs API router - POST /v1/runs and GET /v1/runs/{run_id}."""

import logging
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dpp_api.auth.api_key import AuthContext, get_auth_context
from dpp_api.budget import BudgetManager
from dpp_api.context import run_id_var, tenant_id_var
from dpp_api.db.models import Run
from dpp_api.db.redis_client import RedisClient
from dpp_api.db.repo_runs import RunRepository
from dpp_api.db.session import get_db
from dpp_api.enforce import PlanEnforcer, PlanViolationError
from dpp_api.queue.sqs_client import get_sqs_client
from dpp_api.schemas import (
    CostInfo,
    ErrorInfo,
    PollInfo,
    ProblemDetail,
    ResultInfo,
    RunCreateRequest,
    RunReceipt,
    RunStatusResponse,
)
from dpp_api.storage import get_s3_client
from dpp_api.utils.hashing import compute_payload_hash
from dpp_api.utils.money import format_usd_micros, parse_usd_string

router = APIRouter(prefix="/v1/runs", tags=["runs"])
logger = logging.getLogger(__name__)


@router.post("", response_model=RunReceipt, status_code=status.HTTP_202_ACCEPTED)
async def create_run(
    request: RunCreateRequest,
    response: Response,
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

    # MS-6: Set tenant_id in context for all subsequent logs
    tenant_id_var.set(tenant_id)

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
            # P1-9: Include trace_id in logs
            logger.info(
                f"Idempotent request for run {existing_run.run_id}",
                extra={"trace_id": existing_run.trace_id} if existing_run.trace_id else {},
            )
            # P1-6: Add cost headers
            response.headers["X-DPP-Cost-Reserved"] = format_usd_micros(existing_run.reservation_max_cost_usd_micros)
            response.headers["X-DPP-Cost-Minimum-Fee"] = format_usd_micros(existing_run.minimum_fee_usd_micros)
            if existing_run.actual_cost_usd_micros is not None:
                response.headers["X-DPP-Cost-Actual"] = format_usd_micros(existing_run.actual_cost_usd_micros)
            return _build_receipt(existing_run)
        else:
            # Hash mismatch - different payload with same key
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency key already used with different payload",
            )

    # Parse max_cost_usd to micros (DEC-4211)
    max_cost_usd_micros = parse_usd_string(request.reservation.max_cost_usd)

    # STEP B: Plan enforcement (API monetization)
    # PlanViolationError is handled by global exception handler (RFC 9457)
    redis_client = RedisClient.get_client()
    plan_enforcer = PlanEnforcer(db, redis_client)
    plan = plan_enforcer.enforce(
        tenant_id=tenant_id,
        pack_type=request.pack_type,
        max_cost_usd_micros=max_cost_usd_micros,
    )

    # Calculate minimum fee (DEC-4203)
    # minimum_fee = max(0.005, 0.02 * reserved_usd), cap <= 0.10
    # P0-4: CRITICAL - minimum_fee must NEVER exceed reserved
    minimum_fee_usd_micros = min(
        max(5_000, int(max_cost_usd_micros * 0.02)),
        max_cost_usd_micros,  # P0-4: Cannot exceed reservation
        100_000,  # Cap at $0.10
    )

    # Generate run_id
    run_id = str(uuid.uuid4())

    # MS-6: Set run_id in context for all subsequent logs
    run_id_var.set(run_id)

    # Create run record (status=QUEUED, money_state=NONE initially)
    retention_until = datetime.now(timezone.utc) + timedelta(days=30)

    # P1-7: Store reservation parameters and inputs for worker
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
        timebox_sec=request.reservation.timebox_sec,  # P1-7
        min_reliability_score=request.reservation.min_reliability_score,  # P1-7
        inputs_json=request.inputs,  # P1-7
        retention_until=retention_until,
        trace_id=request.meta.trace_id if request.meta else None,
    )

    try:
        # INT-01: This will fail with UNIQUE constraint violation if race occurs
        repo.create(run)
    except IntegrityError as e:
        # P1-3: Explicit IntegrityError handling for idempotency key conflicts
        # Check if it's the idempotency key constraint violation
        error_str = str(e.orig) if hasattr(e, 'orig') else str(e)

        if "uq_runs_tenant_idempotency" in error_str.lower():
            # Race condition: Another request created the run first
            # Fetch the existing run
            existing_run = repo.get_by_idempotency_key(tenant_id, idempotency_key)
            if existing_run and existing_run.payload_hash == payload_hash:
                # P1-9: Include trace_id in logs
                logger.info(
                    f"Idempotency race: Returning existing run {existing_run.run_id}",
                    extra={"trace_id": existing_run.trace_id} if existing_run.trace_id else {},
                )
                # P1-6: Add cost headers
                response.headers["X-DPP-Cost-Reserved"] = format_usd_micros(existing_run.reservation_max_cost_usd_micros)
                response.headers["X-DPP-Cost-Minimum-Fee"] = format_usd_micros(existing_run.minimum_fee_usd_micros)
                if existing_run.actual_cost_usd_micros is not None:
                    response.headers["X-DPP-Cost-Actual"] = format_usd_micros(existing_run.actual_cost_usd_micros)
                return _build_receipt(existing_run)
            else:
                # Hash mismatch - different payload with same key
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Idempotency key already used with different payload",
                )
        else:
            # Other integrity error (e.g., foreign key, check constraint)
            logger.error(f"IntegrityError during run creation: {error_str}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database constraint violation",
            )

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
    # P0-5: Transaction Script pattern - if enqueue fails, rollback DB and refund
    try:
        sqs_client = get_sqs_client()
        # Observability: Pass trace_id to Worker for end-to-end tracing
        message_id = sqs_client.enqueue_run(run_id, tenant_id, request.pack_type, trace_id=run.trace_id)
        # P1-9: Include trace_id in logs
        logger.info(
            f"Enqueued run {run_id} to SQS (message_id={message_id})",
            extra={"trace_id": run.trace_id} if run.trace_id else {},
        )

    except Exception as e:
        logger.error(f"SQS enqueue failed for run {run_id}: {e}", exc_info=True)

        # P0-5: CRITICAL - Refund reserved funds (Transaction Script rollback)
        try:
            refund_result = budget_manager.scripts.refund_full(tenant_id, run_id)
            logger.info(f"Refunded {refund_result} micros for run {run_id} after enqueue failure")
        except Exception as refund_error:
            logger.error(
                f"CRITICAL: Refund failed after enqueue failure for run {run_id}: {refund_error}",
                exc_info=True,
            )
            # Continue to mark run as FAILED even if refund fails
            # Manual reconciliation will be needed

        # P0-5: Update DB with error details so GET shows failure reason
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

        # P0-5: Re-raise as 500 Internal Server Error per Auditor's guide
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to enqueue run: {str(e)[:100]}",
        )

    # P1-6: Add X-DPP-* cost headers (DEC-4208)
    response.headers["X-DPP-Cost-Reserved"] = format_usd_micros(run.reservation_max_cost_usd_micros)
    response.headers["X-DPP-Cost-Minimum-Fee"] = format_usd_micros(run.minimum_fee_usd_micros)
    if run.actual_cost_usd_micros is not None:
        response.headers["X-DPP-Cost-Actual"] = format_usd_micros(run.actual_cost_usd_micros)

    # P1-2: Add rate limit headers
    rate_limit_headers = plan_enforcer.get_rate_limit_headers_post(plan, tenant_id)
    for header_name, header_value in rate_limit_headers.items():
        response.headers[header_name] = header_value

    # Success - return receipt
    return _build_receipt(run)


@router.get("/{run_id}", response_model=RunStatusResponse)
async def get_run(
    run_id: str,
    response: Response,
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

    # MS-6: Set context for all subsequent logs
    tenant_id_var.set(tenant_id)
    run_id_var.set(run_id)

    # P1-8: Rate limit check for polling
    redis_client = RedisClient.get_client()
    plan_enforcer = PlanEnforcer(db, redis_client)
    plan = plan_enforcer.get_active_plan(tenant_id)
    plan_enforcer.check_rate_limit_poll(plan, tenant_id)

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
        # P1-1: Generate presigned URL for S3 result (TTL: 600s)
        presigned_url = None
        expires_at = None

        if run.result_bucket and run.result_key:
            try:
                s3_client = get_s3_client()
                presigned_url, expires_at = s3_client.generate_presigned_url(
                    bucket=run.result_bucket,
                    key=run.result_key,
                    ttl_seconds=600,  # P1-1: 10 minutes TTL
                )
            except Exception as e:
                # Log error but don't fail the request
                # Client can still see that run is COMPLETED, just can't download result
                logger.error(
                    f"Failed to generate presigned URL for run {run_id}: {e}",
                    exc_info=True,
                )

        result_info = ResultInfo(
            presigned_url=presigned_url,
            sha256=run.result_sha256,
            expires_at=expires_at,
        )
    elif run.status == "FAILED":
        error_info = ErrorInfo(
            reason_code=run.last_error_reason_code or "UNKNOWN",
            detail=run.last_error_detail or "Run failed",
        )

    # P1-6: Add X-DPP-* cost headers (DEC-4208)
    response.headers["X-DPP-Cost-Reserved"] = format_usd_micros(run.reservation_max_cost_usd_micros)
    response.headers["X-DPP-Cost-Minimum-Fee"] = format_usd_micros(run.minimum_fee_usd_micros)
    if run.actual_cost_usd_micros is not None:
        response.headers["X-DPP-Cost-Actual"] = format_usd_micros(run.actual_cost_usd_micros)

    # P1-2: Add rate limit headers
    rate_limit_headers = plan_enforcer.get_rate_limit_headers_poll(plan, tenant_id)
    for header_name, header_value in rate_limit_headers.items():
        response.headers[header_name] = header_value

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
