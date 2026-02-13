"""2-Phase Finalize with Optimistic Locking (DEC-4210).

Implements exactly-once terminal transition to prevent double-settlement and race conditions
between Worker and Reaper.

CRITICAL: Claim must succeed before any side-effects (settle/refund/S3 pointers).

Spec reference (Section 9.1, Step 7):
  (A) claim:
    WHERE: status='PROCESSING' AND version=:v AND lease_token=:lease_token
           AND finalize_stage IS NULL
    SET:   finalize_token=:uuid, finalize_stage='CLAIMED', version=v+1

  (B) side-effects (winner only):
    settle: charge = min(actual_cost, reserved)
    final commit:
      WHERE: run_id=:id AND version=:v_claimed AND finalize_token=:token
             AND finalize_stage='CLAIMED'
      SET:   status='COMPLETED', money_state='SETTLED', finalize_stage='COMMITTED', version+1
"""

import uuid
from datetime import datetime, timezone
from typing import Literal

from sqlalchemy.orm import Session

from dpp_api.budget import BudgetManager
from dpp_api.db.repo_runs import RunRepository


class FinalizeError(Exception):
    """Base exception for finalize errors."""

    pass


class ClaimError(FinalizeError):
    """Raised when claim phase fails (loser)."""

    pass


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

    Phase A (Claim): Acquire exclusive right to finalize using DB-CAS
    Phase B (Side-effects - winner only): Settle budget + commit final state

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
    repo = RunRepository(db)

    # Get current run state
    run = repo.get_by_id(run_id, tenant_id)
    if not run:
        raise FinalizeError(f"Run {run_id} not found")

    if run.status != "PROCESSING":
        raise ClaimError(
            f"Run {run_id} status is {run.status}, expected PROCESSING (already finalized)"
        )

    # C-5: Validate money_state before attempting settle (Golden Rule)
    if run.money_state != "RESERVED":
        raise FinalizeError(
            f"Run {run_id} money_state is {run.money_state}, expected RESERVED"
        )

    # DEC-4211: actual_cost must not exceed reservation (pre-check before claim)
    if actual_cost_usd_micros > run.reservation_max_cost_usd_micros:
        raise FinalizeError(
            f"Actual cost {actual_cost_usd_micros} exceeds "
            f"reserved {run.reservation_max_cost_usd_micros}"
        )

    # ========================================
    # PHASE A: CLAIM (DB-CAS)
    # ========================================
    # Generate finalize_token for this finalize attempt
    finalize_token = str(uuid.uuid4())
    current_version = run.version

    # Spec 9.1 Step 7-A: Attempt to claim exclusive right to finalize
    # WHERE: status='PROCESSING' AND version=:v AND lease_token=:lease_token
    #        AND finalize_stage IS NULL
    success = repo.update_with_version_check(
        run_id=run_id,
        tenant_id=tenant_id,
        expected_version=current_version,
        updates={
            "finalize_stage": "CLAIMED",
            "finalize_token": finalize_token,
            "finalize_claimed_at": datetime.now(timezone.utc),
        },
        extra_conditions={
            "status": "PROCESSING",
            "lease_token": lease_token,
            "finalize_stage": None,  # IS NULL
        },
    )

    if not success:
        # Lost race - another worker or reaper already claimed
        raise ClaimError(f"Run {run_id} already claimed by another process")

    # ========================================
    # PHASE B: SIDE-EFFECTS (winner only)
    # ========================================

    # 1. Settle budget (charge actual cost, refund excess)
    settle_status, charge, refund, new_balance = budget_manager.scripts.settle(
        tenant_id, run_id, actual_cost_usd_micros
    )

    if settle_status != "OK":
        raise FinalizeError(f"Settle failed: {settle_status}")

    # 2. Final DB commit with result pointers
    # Spec 9.1 Step 7-B:
    # WHERE: run_id=:id AND version=:v_claimed AND finalize_token=:token
    #        AND finalize_stage='CLAIMED'
    claimed_version = current_version + 1  # Version was incremented by claim

    final_success = repo.update_with_version_check(
        run_id=run_id,
        tenant_id=tenant_id,
        expected_version=claimed_version,
        updates={
            "status": "COMPLETED",
            "money_state": "SETTLED",
            "actual_cost_usd_micros": actual_cost_usd_micros,
            "result_bucket": result_bucket,
            "result_key": result_key,
            "result_sha256": result_sha256,
            "finalize_stage": "COMMITTED",
        },
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

    return "WINNER"


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

    Similar to finalize_success but:
    - Charges minimum_fee instead of actual_cost
    - Sets status=FAILED with error details
    - No result pointers

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
    repo = RunRepository(db)

    # Get current run state
    run = repo.get_by_id(run_id, tenant_id)
    if not run:
        raise FinalizeError(f"Run {run_id} not found")

    if run.status != "PROCESSING":
        raise ClaimError(
            f"Run {run_id} status is {run.status}, expected PROCESSING (already finalized)"
        )

    # C-5: Validate money_state before attempting settle (Golden Rule)
    if run.money_state != "RESERVED":
        raise FinalizeError(
            f"Run {run_id} money_state is {run.money_state}, expected RESERVED"
        )

    # ========================================
    # PHASE A: CLAIM
    # ========================================
    finalize_token = str(uuid.uuid4())
    current_version = run.version

    # Spec 9.1 Step 7-A (same conditions as success path)
    success = repo.update_with_version_check(
        run_id=run_id,
        tenant_id=tenant_id,
        expected_version=current_version,
        updates={
            "finalize_stage": "CLAIMED",
            "finalize_token": finalize_token,
            "finalize_claimed_at": datetime.now(timezone.utc),
        },
        extra_conditions={
            "status": "PROCESSING",
            "lease_token": lease_token,
            "finalize_stage": None,  # IS NULL
        },
    )

    if not success:
        raise ClaimError(f"Run {run_id} already claimed by another process")

    # ========================================
    # PHASE B: SIDE-EFFECTS (winner only)
    # ========================================

    # 1. Settle budget with minimum_fee
    if minimum_fee_usd_micros > run.reservation_max_cost_usd_micros:
        raise FinalizeError(
            f"Minimum fee {minimum_fee_usd_micros} exceeds "
            f"reserved {run.reservation_max_cost_usd_micros}"
        )

    settle_status, charge, refund, new_balance = budget_manager.scripts.settle(
        tenant_id, run_id, minimum_fee_usd_micros
    )

    if settle_status != "OK":
        raise FinalizeError(f"Settle failed: {settle_status}")

    # 2. Final DB commit with error details
    # Spec 9.1 Step 7-B (with finalize_token + finalize_stage conditions)
    claimed_version = current_version + 1

    final_success = repo.update_with_version_check(
        run_id=run_id,
        tenant_id=tenant_id,
        expected_version=claimed_version,
        updates={
            "status": "FAILED",
            "money_state": "SETTLED",
            "actual_cost_usd_micros": minimum_fee_usd_micros,
            "last_error_reason_code": error_reason_code,
            "last_error_detail": error_detail,
            "finalize_stage": "COMMITTED",
        },
        extra_conditions={
            "finalize_token": finalize_token,
            "finalize_stage": "CLAIMED",
        },
    )

    if not final_success:
        raise FinalizeError(
            f"Final commit failed for run {run_id} despite successful claim"
        )

    return "WINNER"
