"""2-Phase Finalize with Optimistic Locking (DEC-4210).

Implements exactly-once terminal transition to prevent double-settlement and race conditions
between Worker and Reaper.

CRITICAL: Claim must succeed before any side-effects (settle/refund/S3 pointers).
"""

import uuid
from datetime import datetime, timezone
from typing import Literal, Optional

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
) -> Literal["WINNER", "LOSER"]:
    """2-phase finalize for successful run completion (COMPLETED + SETTLED).

    Phase A (Claim): Acquire exclusive right to finalize using DB-CAS
    Phase B (Side-effects - winner only): Settle budget + commit final state

    Args:
        run_id: Run ID
        tenant_id: Tenant ID
        lease_token: Lease token from worker
        actual_cost_usd_micros: Actual cost to charge (USD_MICROS)
        result_bucket: S3 bucket name
        result_key: S3 object key
        result_sha256: SHA-256 hash of result
        db: Database session
        budget_manager: Budget manager instance

    Returns:
        "WINNER" if finalize succeeded, "LOSER" if lost race

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

    # PHASE A: CLAIM (DB-CAS)
    # Generate finalize_token for this finalize attempt
    finalize_token = str(uuid.uuid4())
    current_version = run.version

    # Attempt to claim exclusive right to finalize
    # CRITICAL: This must succeed before any side-effects
    success = repo.update_with_version_check(
        run_id=run_id,
        tenant_id=tenant_id,
        expected_version=current_version,
        updates={
            "finalize_stage": "CLAIMED",
            "finalize_token": finalize_token,
            "finalize_claimed_at": datetime.now(timezone.utc),
            # version will be incremented automatically
        },
    )

    if not success:
        # Lost race - another worker or reaper already claimed
        raise ClaimError(f"Run {run_id} already claimed by another process")

    # WINNER - Claim succeeded!
    # Now it's safe to perform side-effects

    try:
        # PHASE B: SIDE-EFFECTS (winner only)

        # 1. Settle budget (charge actual cost, refund excess)
        # DEC-4211: actual_cost must not exceed reservation
        if actual_cost_usd_micros > run.reservation_max_cost_usd_micros:
            raise FinalizeError(
                f"Actual cost {actual_cost_usd_micros} exceeds "
                f"reserved {run.reservation_max_cost_usd_micros}"
            )

        settle_status, charge, refund, new_balance = budget_manager.scripts.settle(
            tenant_id, run_id, actual_cost_usd_micros
        )

        if settle_status != "OK":
            raise FinalizeError(f"Settle failed: {settle_status}")

        # 2. Final DB commit with result pointers
        # Get current version after claim (version was incremented)
        run_after_claim = repo.get_by_id(run_id, tenant_id)
        if not run_after_claim:
            raise FinalizeError(f"Run {run_id} disappeared after claim")

        claimed_version = run_after_claim.version

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
                # finalize_token remains (for audit)
                # version will be incremented automatically
            },
        )

        if not final_success:
            # This should never happen unless DB corruption
            raise FinalizeError(
                f"Final commit failed for run {run_id} despite successful claim"
            )

        return "WINNER"

    except Exception as e:
        # If side-effects fail after claim, we have a problem
        # The claim succeeded but we couldn't complete
        # This will be handled by reconciliation job
        raise FinalizeError(f"Failed during side-effects after claim: {e}") from e


def finalize_failure(
    run_id: str,
    tenant_id: str,
    lease_token: str,
    minimum_fee_usd_micros: int,
    error_reason_code: str,
    error_detail: str,
    db: Session,
    budget_manager: BudgetManager,
) -> Literal["WINNER", "LOSER"]:
    """2-phase finalize for failed run (FAILED + SETTLED with minimum_fee).

    Similar to finalize_success but:
    - Charges minimum_fee instead of actual_cost
    - Sets status=FAILED with error details
    - No result pointers

    Args:
        run_id: Run ID
        tenant_id: Tenant ID
        lease_token: Lease token from worker
        minimum_fee_usd_micros: Minimum fee to charge (USD_MICROS)
        error_reason_code: Error reason code
        error_detail: Error detail message
        db: Database session
        budget_manager: Budget manager instance

    Returns:
        "WINNER" if finalize succeeded, "LOSER" if lost race

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

    # PHASE A: CLAIM
    finalize_token = str(uuid.uuid4())
    current_version = run.version

    success = repo.update_with_version_check(
        run_id=run_id,
        tenant_id=tenant_id,
        expected_version=current_version,
        updates={
            "finalize_stage": "CLAIMED",
            "finalize_token": finalize_token,
            "finalize_claimed_at": datetime.now(timezone.utc),
        },
    )

    if not success:
        raise ClaimError(f"Run {run_id} already claimed by another process")

    # WINNER - Claim succeeded!

    try:
        # PHASE B: SIDE-EFFECTS

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
        run_after_claim = repo.get_by_id(run_id, tenant_id)
        if not run_after_claim:
            raise FinalizeError(f"Run {run_id} disappeared after claim")

        claimed_version = run_after_claim.version

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
        )

        if not final_success:
            raise FinalizeError(
                f"Final commit failed for run {run_id} despite successful claim"
            )

        return "WINNER"

    except Exception as e:
        raise FinalizeError(f"Failed during side-effects after claim: {e}") from e
