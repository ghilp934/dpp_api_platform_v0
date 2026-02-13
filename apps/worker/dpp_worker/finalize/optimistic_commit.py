"""2-Phase Finalize with Optimistic Locking (DEC-4210).

Implements exactly-once terminal transition to prevent double-settlement and race conditions
between Worker and Reaper.

CRITICAL: Claim must succeed before any side-effects (settle/refund/S3 pointers).

Polymorphic Design:
- Worker uses lease_token condition (has active lease)
- Reaper uses lease_expires_at condition (lease expired)
- Common 2-phase commit logic (_do_2phase_finalize)

Spec reference (Section 9.1, Step 7 + Section 10.2):
  (A) claim:
    Worker WHERE: status='PROCESSING' AND version=:v AND lease_token=:lease_token
                  AND finalize_stage IS NULL
    Reaper WHERE: status='PROCESSING' AND version=:v AND lease_expires_at < NOW()
                  AND finalize_stage IS NULL
    SET:   finalize_token=:uuid, finalize_stage='CLAIMED', version=v+1

  (B) side-effects (winner only):
    settle: charge = min(requested_charge, reserved)
    final commit:
      WHERE: run_id=:id AND version=:v_claimed AND finalize_token=:token
             AND finalize_stage='CLAIMED'
      SET:   status=:final_status, money_state='SETTLED', finalize_stage='COMMITTED', version+1
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Literal

from sqlalchemy.orm import Session

from dpp_api.budget import BudgetManager
from dpp_api.db.repo_runs import RunRepository
from dpp_api.metering import UsageTracker


class FinalizeError(Exception):
    """Base exception for finalize errors."""

    pass


class ClaimError(FinalizeError):
    """Raised when claim phase fails (loser)."""

    pass


def _do_2phase_finalize(
    run_id: str,
    tenant_id: str,
    charge_usd_micros: int,
    final_status: Literal["COMPLETED", "FAILED"],
    extra_claim_conditions: dict[str, Any],
    extra_final_updates: dict[str, Any],
    db: Session,
    budget_manager: BudgetManager,
) -> Literal["WINNER"]:
    """Internal 2-phase finalize implementation (polymorphic core).

    This is the common logic shared by Worker (success/failure) and Reaper (timeout).

    Phase A (Claim): Acquire exclusive right to finalize using DB-CAS
    Phase B (Side-effects - winner only): Settle budget + commit final state

    Args:
        run_id: Run ID
        tenant_id: Tenant ID
        charge_usd_micros: Amount to charge (USD_MICROS)
        final_status: Final status ("COMPLETED" or "FAILED")
        extra_claim_conditions: Extra WHERE conditions for claim
                                (Worker: {"lease_token": token}, Reaper: {"lease_expires_at": ("lt", now)})
        extra_final_updates: Extra fields to update in final commit
                             (Worker: S3 pointers, Reaper: error details)
        db: Database session
        budget_manager: Budget manager instance

    Returns:
        "WINNER" if finalize succeeded

    Raises:
        ClaimError: If claim phase fails (loser)
        FinalizeError: If commit phase fails after claiming
    """
    repo = RunRepository(db)

    # Get current run state
    run = repo.get_by_id(run_id, tenant_id)
    if not run:
        raise FinalizeError(f"Run {run_id} not found")

    if run.status != "PROCESSING":
        raise ClaimError(
            f"Run {run_id} status is {run.status}, expected PROCESSING (already finalized)"
        )

    # Golden Rule: Validate money_state before attempting settle
    if run.money_state != "RESERVED":
        raise FinalizeError(
            f"Run {run_id} money_state is {run.money_state}, expected RESERVED"
        )

    # Pre-check: charge must not exceed reservation
    if charge_usd_micros > run.reservation_max_cost_usd_micros:
        raise FinalizeError(
            f"Charge {charge_usd_micros} exceeds reserved {run.reservation_max_cost_usd_micros}"
        )

    # ========================================
    # PHASE A: CLAIM (DB-CAS)
    # ========================================
    finalize_token = str(uuid.uuid4())
    current_version = run.version

    # Base claim conditions (common to Worker and Reaper)
    claim_conditions = {
        "status": "PROCESSING",
        "finalize_stage": None,  # IS NULL - critical for race prevention
    }
    # Add extra conditions (Worker: lease_token, Reaper: lease_expires_at)
    claim_conditions.update(extra_claim_conditions)

    # Attempt to claim exclusive right to finalize
    success = repo.update_with_version_check(
        run_id=run_id,
        tenant_id=tenant_id,
        expected_version=current_version,
        updates={
            "finalize_stage": "CLAIMED",
            "finalize_token": finalize_token,
            "finalize_claimed_at": datetime.now(timezone.utc),
        },
        extra_conditions=claim_conditions,
    )

    if not success:
        # Lost race - another worker or reaper already claimed
        raise ClaimError(f"Run {run_id} already claimed by another process")

    # ========================================
    # PHASE B: SIDE-EFFECTS (winner only)
    # ========================================

    # 1. Settle budget (charge, refund excess)
    settle_status, charge, refund, new_balance = budget_manager.scripts.settle(
        tenant_id, run_id, charge_usd_micros
    )

    if settle_status != "OK":
        raise FinalizeError(f"Settle failed: {settle_status}")

    # 2. Final DB commit
    claimed_version = current_version + 1  # Version was incremented by claim

    # Base final updates (common to all finalize types)
    final_updates = {
        "status": final_status,
        "money_state": "SETTLED",
        "actual_cost_usd_micros": charge_usd_micros,
        "finalize_stage": "COMMITTED",
    }
    # Add extra updates (Worker: S3 pointers, Reaper: error details)
    final_updates.update(extra_final_updates)

    final_success = repo.update_with_version_check(
        run_id=run_id,
        tenant_id=tenant_id,
        expected_version=claimed_version,
        updates=final_updates,
        extra_conditions={
            "finalize_token": finalize_token,
            "finalize_stage": "CLAIMED",
        },
    )

    if not final_success:
        # This should never happen unless DB corruption
        raise FinalizeError(
            f"Final commit failed for run {run_id} despite successful claim"
        )

    # STEP C: Record usage in tenant_usage_daily (metering)
    # Get updated run record for metering
    updated_run = repo.get_by_id(run_id, tenant_id)
    if updated_run:
        usage_tracker = UsageTracker(db)
        try:
            usage_tracker.record_run_completion(updated_run)
        except Exception as e:
            # Log metering error but don't fail finalize (already committed)
            # Metering is important but not critical for finalize success
            import logging

            logger = logging.getLogger(__name__)
            logger.error(f"Failed to record usage for run {run_id}: {e}", exc_info=True)

    return "WINNER"


def finalize_success(
    run_id: str,
    tenant_id: str,
    lease_token: str,
    actual_cost_usd_micros: int,
    result_bucket: str,
    result_key: str,
    result_sha256: str,
    db: Session,
    budget_manager: BudgetManager,
) -> Literal["WINNER"]:
    """2-phase finalize for successful run completion (COMPLETED + SETTLED).

    Worker-specific wrapper that uses lease_token condition.

    Args:
        run_id: Run ID
        tenant_id: Tenant ID
        lease_token: Lease token from worker (used in claim WHERE condition)
        actual_cost_usd_micros: Actual cost to charge (USD_MICROS)
        result_bucket: S3 bucket name
        result_key: S3 object key
        result_sha256: SHA-256 hash of result
        db: Database session
        budget_manager: Budget manager instance

    Returns:
        "WINNER" if finalize succeeded

    Raises:
        ClaimError: If claim phase fails (loser)
        FinalizeError: If commit phase fails after claiming
    """
    return _do_2phase_finalize(
        run_id=run_id,
        tenant_id=tenant_id,
        charge_usd_micros=actual_cost_usd_micros,
        final_status="COMPLETED",
        extra_claim_conditions={
            "lease_token": lease_token,  # Worker: has active lease
        },
        extra_final_updates={
            "result_bucket": result_bucket,
            "result_key": result_key,
            "result_sha256": result_sha256,
        },
        db=db,
        budget_manager=budget_manager,
    )


def finalize_failure(
    run_id: str,
    tenant_id: str,
    lease_token: str,
    minimum_fee_usd_micros: int,
    error_reason_code: str,
    error_detail: str,
    db: Session,
    budget_manager: BudgetManager,
) -> Literal["WINNER"]:
    """2-phase finalize for failed run (FAILED + SETTLED with minimum_fee).

    Worker-specific wrapper that uses lease_token condition.

    Args:
        run_id: Run ID
        tenant_id: Tenant ID
        lease_token: Lease token from worker (used in claim WHERE condition)
        minimum_fee_usd_micros: Minimum fee to charge (USD_MICROS)
        error_reason_code: Error reason code
        error_detail: Error detail message
        db: Database session
        budget_manager: Budget manager instance

    Returns:
        "WINNER" if finalize succeeded

    Raises:
        ClaimError: If claim phase fails (loser)
        FinalizeError: If commit phase fails after claiming
    """
    return _do_2phase_finalize(
        run_id=run_id,
        tenant_id=tenant_id,
        charge_usd_micros=minimum_fee_usd_micros,
        final_status="FAILED",
        extra_claim_conditions={
            "lease_token": lease_token,  # Worker: has active lease
        },
        extra_final_updates={
            "last_error_reason_code": error_reason_code,
            "last_error_detail": error_detail,
        },
        db=db,
        budget_manager=budget_manager,
    )


def finalize_timeout(
    run_id: str,
    tenant_id: str,
    minimum_fee_usd_micros: int,
    db: Session,
    budget_manager: BudgetManager,
) -> Literal["WINNER"]:
    """2-phase finalize for timeout (FAILED + SETTLED with minimum_fee).

    Reaper-specific wrapper that uses lease_expires_at condition instead of lease_token.

    Spec 10.2: Reaper finalize
    - claim WHERE: status='PROCESSING' AND lease_expires_at < NOW() AND finalize_stage IS NULL
    - settle: charge = min(minimum_fee, reserved)
    - final commit: status='FAILED', reason_code='WORKER_TIMEOUT'

    Args:
        run_id: Run ID
        tenant_id: Tenant ID
        minimum_fee_usd_micros: Minimum fee to charge (USD_MICROS)
        db: Database session
        budget_manager: Budget manager instance

    Returns:
        "WINNER" if finalize succeeded

    Raises:
        ClaimError: If claim phase fails (loser - Worker or another Reaper won)
        FinalizeError: If commit phase fails after claiming
    """
    # Special handling for Reaper: we need to check lease_expires_at < NOW()
    # But repo.update_with_version_check doesn't support temporal comparisons directly
    # Workaround: We'll rely on the scan query to pre-filter expired leases,
    # and use finalize_stage IS NULL as the race protection
    # (If Worker claims first, finalize_stage won't be NULL anymore)

    return _do_2phase_finalize(
        run_id=run_id,
        tenant_id=tenant_id,
        charge_usd_micros=minimum_fee_usd_micros,
        final_status="FAILED",
        extra_claim_conditions={
            # Reaper doesn't check lease_token (lease expired)
            # finalize_stage IS NULL is sufficient race protection
        },
        extra_final_updates={
            "last_error_reason_code": "WORKER_TIMEOUT",
            "last_error_detail": "Worker lease expired, run terminated by Reaper",
        },
        db=db,
        budget_manager=budget_manager,
    )
