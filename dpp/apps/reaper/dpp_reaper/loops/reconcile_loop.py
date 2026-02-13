"""Reconcile loop for recovering stuck 'CLAIMED' runs.

P0-2: DEC-4206 Atomic Commit Safety
- Problem: Worker crashes after CLAIM but before COMMIT → run stuck in finalize_stage='CLAIMED'
- Solution: Reconcile loop detects stuck runs and recovers them

Recovery Logic (Claim-Check pattern):
1. Scan: status='PROCESSING' AND finalize_stage='CLAIMED' AND finalize_claimed_at < (NOW - 5min)
2. Check S3: Does result_bucket/result_key exist?
3. Roll-forward (S3 exists): Complete the finalize → status='COMPLETED', money_state='SETTLED'
4. Roll-back (S3 missing): Abort the finalize → status='FAILED', refund reservation

Interval: 60 seconds (less aggressive than reaper, as CLAIMED stuck is rarer)
"""

import logging
import signal
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from dpp_api.budget import BudgetManager
from dpp_api.db.models import Run
from dpp_api.db.redis_client import RedisClient
from dpp_api.db.repo_runs import RunRepository

logger = logging.getLogger(__name__)

# Global shutdown event for graceful termination
_shutdown_event = threading.Event()


def _signal_handler(signum, frame):
    """Handle shutdown signals (SIGTERM, SIGINT) gracefully."""
    sig_name = signal.Signals(signum).name
    logger.info(f"Received {sig_name} signal, initiating graceful shutdown of reconcile loop...")
    _shutdown_event.set()


# Register signal handlers
signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


def scan_stuck_claimed_runs(db: Session, stuck_threshold_minutes: int = 5, limit: int = 100) -> list[Run]:
    """Scan for runs stuck in 'CLAIMED' state for too long.

    P0-2: Detect runs where Worker crashed after claim but before final commit.

    Args:
        db: Database session
        stuck_threshold_minutes: Minutes after which a CLAIMED run is considered stuck (default 5)
        limit: Maximum number of runs to scan per iteration

    Returns:
        List of stuck runs
    """
    now = datetime.now(timezone.utc)
    threshold = now - timedelta(minutes=stuck_threshold_minutes)

    stmt = (
        select(Run)
        .where(
            and_(
                Run.status == "PROCESSING",
                Run.finalize_stage == "CLAIMED",
                Run.finalize_claimed_at < threshold,
            )
        )
        .limit(limit)
    )

    result = db.execute(stmt)
    runs = result.scalars().all()

    if runs:
        logger.info(
            f"Reconcile scan found {len(runs)} stuck CLAIMED runs",
            extra={"stuck_count": len(runs), "threshold_minutes": stuck_threshold_minutes},
        )

    return runs


def check_s3_result_exists(run: Run) -> bool:
    """Check if S3 result exists for a run.

    P0-2: Determines whether to roll-forward (complete) or roll-back (fail).

    Args:
        run: Run to check

    Returns:
        True if S3 result exists, False otherwise
    """
    # Check if run has S3 pointers
    if not run.result_bucket or not run.result_key:
        logger.debug(
            f"Run {run.run_id} has no S3 pointers (bucket={run.result_bucket}, key={run.result_key})",
            extra={"run_id": run.run_id, "s3_exists": False},
        )
        return False

    try:
        # MS-6: Use actual S3 API call to verify object exists
        from dpp_api.storage.s3_client import get_s3_client
        s3_client = get_s3_client()
        exists = s3_client.object_exists(run.result_bucket, run.result_key)

        logger.info(
            f"S3 result {'exists' if exists else 'NOT FOUND'} for run {run.run_id} "
            f"(bucket={run.result_bucket}, key={run.result_key})",
            extra={"run_id": run.run_id, "s3_exists": exists},
        )
        return exists

    except Exception as e:
        # If S3 check fails, treat as "does not exist" for safety (roll-back)
        logger.error(
            f"S3 check failed for run {run.run_id}: {e}",
            exc_info=True,
            extra={"run_id": run.run_id, "s3_exists": False},
        )
        return False


def roll_forward_stuck_run(
    run: Run,
    db: Session,
    budget_manager: BudgetManager,
) -> bool:
    """Roll-forward: Complete a stuck CLAIMED run (S3 result exists).

    P0-2: Worker successfully uploaded to S3 but crashed before final commit.
    We complete the finalize on behalf of the crashed Worker.

    Args:
        run: Stuck run to complete
        db: Database session
        budget_manager: Budget manager for settlement

    Returns:
        True if roll-forward succeeded, False otherwise
    """
    run_id = run.run_id
    tenant_id = run.tenant_id

    try:
        repo = RunRepository(db)

        # Calculate charge (actual_cost should already be in run record, but defensive)
        charge_usd_micros = run.actual_cost_usd_micros or run.reservation_max_cost_usd_micros

        # STEP 1: Settle budget
        # NOTE: settle() is NOT idempotent (deletes reservation on first call)
        # Protection: optimistic locking (version check) below prevents double-commit
        settle_status, returned_charge, refund, new_balance = budget_manager.scripts.settle(
            tenant_id, run_id, charge_usd_micros
        )

        if settle_status != "OK":
            logger.error(
                f"Reconcile roll-forward: settle failed for run {run_id}: {settle_status}",
                extra={"run_id": run_id, "settle_status": settle_status},
            )
            return False

        # STEP 2: Final commit (verify finalize_token and finalize_stage='CLAIMED')
        final_success = repo.update_with_version_check(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=run.version,
            updates={
                "status": "COMPLETED",
                "money_state": "SETTLED",
                "actual_cost_usd_micros": returned_charge,
                "finalize_stage": "COMMITTED",
                "completed_at": datetime.now(timezone.utc),
            },
            extra_conditions={
                "finalize_token": run.finalize_token,
                "finalize_stage": "CLAIMED",
            },
        )

        if not final_success:
            logger.warning(
                f"Reconcile roll-forward: final commit failed for run {run_id} (concurrent update?)",
                extra={"run_id": run_id},
            )
            return False

        # STEP 3: Record usage (metering)
        updated_run = repo.get_by_id(run_id, tenant_id)
        if updated_run:
            from dpp_api.metering import UsageTracker
            usage_tracker = UsageTracker(db)
            try:
                usage_tracker.record_run_completion(updated_run)
            except Exception as e:
                logger.error(f"Failed to record usage for run {run_id}: {e}", exc_info=True)

        logger.info(
            f"Reconcile ROLL-FORWARD: Completed stuck run {run_id} (S3 existed)",
            extra={
                "run_id": run_id,
                "tenant_id": tenant_id,
                "charge_usd_micros": returned_charge,
                "recovery_type": "roll_forward",
            },
        )
        return True

    except Exception as e:
        logger.error(
            f"Reconcile roll-forward error for run {run_id}: {e}",
            exc_info=True,
            extra={"run_id": run_id},
        )
        return False


def roll_back_stuck_run(
    run: Run,
    db: Session,
    budget_manager: BudgetManager,
) -> bool:
    """Roll-back: Fail a stuck CLAIMED run (S3 result does not exist).

    P0-2: Worker crashed before S3 upload completed.
    We abort the finalize and charge minimum_fee.

    Args:
        run: Stuck run to fail
        db: Database session
        budget_manager: Budget manager for settlement

    Returns:
        True if roll-back succeeded, False otherwise
    """
    run_id = run.run_id
    tenant_id = run.tenant_id

    try:
        repo = RunRepository(db)

        # Charge minimum_fee (capped by reservation)
        charge_usd_micros = min(
            run.minimum_fee_usd_micros or 0,
            run.reservation_max_cost_usd_micros or 0,
        )

        # STEP 1: Settle budget with minimum_fee
        settle_status, returned_charge, refund, new_balance = budget_manager.scripts.settle(
            tenant_id, run_id, charge_usd_micros
        )

        if settle_status != "OK":
            logger.error(
                f"Reconcile roll-back: settle failed for run {run_id}: {settle_status}",
                extra={"run_id": run_id, "settle_status": settle_status},
            )
            return False

        # STEP 2: Final commit (verify finalize_token and finalize_stage='CLAIMED')
        final_success = repo.update_with_version_check(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=run.version,
            updates={
                "status": "FAILED",
                "money_state": "SETTLED",
                "actual_cost_usd_micros": returned_charge,
                "finalize_stage": "COMMITTED",
                "completed_at": datetime.now(timezone.utc),
                "last_error_reason_code": "WORKER_CRASH_DURING_FINALIZE",
                "last_error_detail": "Worker crashed after claim but before S3 upload completed",
            },
            extra_conditions={
                "finalize_token": run.finalize_token,
                "finalize_stage": "CLAIMED",
            },
        )

        if not final_success:
            logger.warning(
                f"Reconcile roll-back: final commit failed for run {run_id} (concurrent update?)",
                extra={"run_id": run_id},
            )
            return False

        # STEP 3: Record usage (metering)
        updated_run = repo.get_by_id(run_id, tenant_id)
        if updated_run:
            from dpp_api.metering import UsageTracker
            usage_tracker = UsageTracker(db)
            try:
                usage_tracker.record_run_completion(updated_run)
            except Exception as e:
                logger.error(f"Failed to record usage for run {run_id}: {e}", exc_info=True)

        logger.info(
            f"Reconcile ROLL-BACK: Failed stuck run {run_id} (S3 missing, charged minimum_fee)",
            extra={
                "run_id": run_id,
                "tenant_id": tenant_id,
                "charge_usd_micros": returned_charge,
                "recovery_type": "roll_back",
            },
        )
        return True

    except Exception as e:
        logger.error(
            f"Reconcile roll-back error for run {run_id}: {e}",
            exc_info=True,
            extra={"run_id": run_id},
        )
        return False


def reconcile_stuck_run(
    run: Run,
    db: Session,
    budget_manager: BudgetManager,
) -> bool:
    """Reconcile a single stuck CLAIMED run.

    P0-2: Decides whether to roll-forward or roll-back based on S3 existence.

    Args:
        run: Stuck run to reconcile
        db: Database session
        budget_manager: Budget manager

    Returns:
        True if reconcile succeeded, False otherwise
    """
    s3_exists = check_s3_result_exists(run)

    if s3_exists:
        # Roll-forward: S3 exists, complete the finalize
        return roll_forward_stuck_run(run, db, budget_manager)
    else:
        # Roll-back: S3 missing, abort the finalize
        return roll_back_stuck_run(run, db, budget_manager)


def reconcile_stuck_claimed_run(
    run: Run,
    db: Session,
    budget_manager: BudgetManager,
) -> bool:
    """MS-6: Receipt-based idempotent finalize reconciliation.

    CRITICAL PRINCIPLE: Settlement receipt is the ONLY authoritative proof
    that settle() succeeded. NO inference, NO estimation - proof only.

    This function handles the critical scenario where:
    1. Worker claimed finalize (finalize_stage='CLAIMED')
    2. Redis settle() succeeded → settlement receipt created
    3. DB commit failed (still money_state='RESERVED')
    → Result: Inconsistent state (Redis settled, DB not updated)

    Resolution:
    - Case 1) Reservation exists → Use standard reconcile (roll-forward/back)
    - Case 2) Reservation missing + Receipt exists → Proof-based DB update
    - Case 3) Reservation missing + No receipt → AUDIT_REQUIRED (no auto-settle)

    Args:
        run: Stuck run to reconcile
        db: Database session
        budget_manager: Budget manager

    Returns:
        True if reconcile succeeded, False otherwise
    """
    run_id = run.run_id
    tenant_id = run.tenant_id
    repo = RunRepository(db)

    # Check if reservation exists in Redis
    reservation = budget_manager.scripts.get_reservation(run_id)

    if reservation:
        # Normal case: reservation exists, use standard reconcile path
        logger.debug(
            f"MS-6: Run {run_id} has reservation, using standard reconcile",
            extra={"run_id": run_id, "reconcile_type": "standard"},
        )
        return reconcile_stuck_run(run, db, budget_manager)

    # CRITICAL: Reservation missing - check settlement receipt
    # MS-6 PRINCIPLE: Receipt is the ONLY proof of settlement
    receipt = budget_manager.scripts.get_settlement_receipt(run_id)

    if not receipt:
        # NO RECEIPT = NO PROOF of settlement
        # MUST NOT auto-settle (violates "proof-only" principle)
        logger.warning(
            f"MS-6: Run {run_id} has no reservation AND no receipt, marking AUDIT_REQUIRED",
            extra={
                "run_id": run_id,
                "reconcile_type": "no_receipt_audit",
            },
        )

        # Mark for manual audit (NO charge - unknown state)
        success = repo.update_with_version_check(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=run.version,
            updates={
                "status": "FAILED",
                "money_state": "AUDIT_REQUIRED",
                "actual_cost_usd_micros": 0,  # Unknown - manual verification needed
                "finalize_stage": "COMMITTED",
                "last_error_reason_code": "MS6_NO_SETTLEMENT_RECEIPT",
                "last_error_detail": "Reservation missing with no settlement receipt (manual reconciliation required)",
                "completed_at": datetime.now(timezone.utc),
            },
        )
        return success

    # RECEIPT FOUND! This is AUTHORITATIVE PROOF that settle() succeeded
    # Use receipt.charged_usd_micros as the ONLY source of truth
    charged_usd_micros = int(receipt["charged_usd_micros"])
    tenant_id_from_receipt = receipt["tenant_id"]

    # Security: Verify tenant_id matches
    if tenant_id_from_receipt != tenant_id:
        logger.error(
            f"MS-6: Receipt tenant mismatch! run={run_id} db_tenant={tenant_id} receipt_tenant={tenant_id_from_receipt}",
            extra={
                "run_id": run_id,
                "db_tenant_id": tenant_id,
                "receipt_tenant_id": tenant_id_from_receipt,
            },
        )
        return False

    logger.info(
        f"MS-6: Found settlement receipt for run {run_id} (charged={charged_usd_micros})",
        extra={
            "run_id": run_id,
            "charged_usd_micros": charged_usd_micros,
            "reconcile_type": "receipt_reconcile",
        },
    )

    # Determine final status based on S3 existence
    s3_exists = check_s3_result_exists(run)

    if s3_exists:
        final_status = "COMPLETED"
        error_reason = None
        error_detail = None
    else:
        final_status = "FAILED"
        error_reason = "MS6_S3_MISSING_AFTER_SETTLEMENT"
        error_detail = "S3 upload missing despite successful settlement"

    # Guarded update (NOT force update)
    # Only update if run is still in CLAIMED+RESERVED state
    updates = {
        "status": final_status,
        "money_state": "SETTLED",
        "actual_cost_usd_micros": charged_usd_micros,  # From receipt (authoritative!)
        "finalize_stage": "COMMITTED",
        "completed_at": datetime.now(timezone.utc),
    }
    if error_reason:
        updates["last_error_reason_code"] = error_reason
    if error_detail:
        updates["last_error_detail"] = error_detail

    # Use update_with_version_check (guarded) NOT force_update
    # This ensures idempotency: if another path already committed, we skip
    success = repo.update_with_version_check(
        run_id=run_id,
        tenant_id=tenant_id,
        expected_version=run.version,
        updates=updates,
        extra_conditions={
            "finalize_stage": "CLAIMED",  # Must still be CLAIMED
            "finalize_token": run.finalize_token,  # Token must match
        },
    )

    if not success:
        # Version mismatch or already committed by another path
        logger.info(
            f"MS-6: Receipt reconcile skipped for run {run_id} (already processed by another path)",
            extra={"run_id": run_id},
        )
        return False

    # Success! DB is now SETTLED based on receipt proof
    logger.info(
        f"MS-6: Receipt-based reconcile succeeded for run {run_id} "
        f"(status={final_status}, charged={charged_usd_micros})",
        extra={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "final_status": final_status,
            "charged_usd_micros": charged_usd_micros,
            "s3_exists": s3_exists,
        },
    )

    # Record usage (metering)
    try:
        updated_run = repo.get_by_id(run_id, tenant_id)
        if updated_run:
            from dpp_api.metering import UsageTracker
            usage_tracker = UsageTracker(db)
            usage_tracker.record_run_completion(updated_run)
    except Exception as e:
        logger.error(f"Failed to record usage for run {run_id}: {e}", exc_info=True)

    return True


def reconcile_loop(
    db: Session,
    budget_manager: Optional[BudgetManager] = None,
    interval_seconds: int = 60,
    stuck_threshold_minutes: int = 5,
    limit_per_scan: int = 100,
    stop_after_one_iteration: bool = False,
) -> None:
    """Main reconcile loop - periodically scan and recover stuck CLAIMED runs.

    P0-2: DEC-4206 Atomic Commit Safety
    - Interval: 60 seconds (less aggressive than reaper)
    - Threshold: 5 minutes (CLAIMED runs older than this are considered stuck)

    Args:
        db: Database session
        budget_manager: Budget manager (optional, will create if not provided)
        interval_seconds: Sleep interval between scans (default 60)
        stuck_threshold_minutes: Minutes after which CLAIMED is considered stuck (default 5)
        limit_per_scan: Max runs to process per iteration (default 100)
        stop_after_one_iteration: For testing only - exit after one scan

    Returns:
        None (runs forever unless stop_after_one_iteration=True)
    """
    if budget_manager is None:
        redis_client = RedisClient.get_client()
        budget_manager = BudgetManager(redis_client, db)

    logger.info(
        f"Reconcile loop started (interval={interval_seconds}s, threshold={stuck_threshold_minutes}min, limit={limit_per_scan})"
    )

    iteration = 0
    total_roll_forward = 0
    total_roll_back = 0
    total_scanned = 0

    while not _shutdown_event.is_set():
        iteration += 1
        iteration_start = time.time()
        logger.debug(f"Reconcile iteration {iteration} starting")

        try:
            # Clear session cache to prevent stale data
            db.expire_all()

            # Scan for stuck CLAIMED runs
            stuck_runs = scan_stuck_claimed_runs(db, stuck_threshold_minutes, limit_per_scan)

            if not stuck_runs:
                logger.debug("No stuck CLAIMED runs found")
            else:
                # Reconcile each stuck run using MS-6 idempotent logic
                roll_forwards = 0
                roll_backs = 0

                for run in stuck_runs:
                    success = reconcile_stuck_claimed_run(run, db, budget_manager)
                    if success:
                        # Determine recovery type by re-reading run
                        from dpp_api.db.repo_runs import RunRepository
                        repo = RunRepository(db)
                        updated_run = repo.get_by_id(run.run_id, run.tenant_id)
                        if updated_run and updated_run.status == "COMPLETED":
                            roll_forwards += 1
                        else:
                            roll_backs += 1

                # Update totals
                total_roll_forward += roll_forwards
                total_roll_back += roll_backs
                total_scanned += len(stuck_runs)

                # Calculate iteration duration
                duration_ms = int((time.time() - iteration_start) * 1000)

                logger.info(
                    f"Reconcile iteration {iteration}: "
                    f"{roll_forwards} roll-forward, {roll_backs} roll-back, "
                    f"{len(stuck_runs)} total scanned",
                    extra={
                        "iteration": iteration,
                        "roll_forwards": roll_forwards,
                        "roll_backs": roll_backs,
                        "scanned": len(stuck_runs),
                        "duration_ms": duration_ms,
                        "total_roll_forward": total_roll_forward,
                        "total_roll_back": total_roll_back,
                        "total_scanned": total_scanned,
                    },
                )

        except Exception as e:
            logger.error(f"Reconcile loop error in iteration {iteration}: {e}", exc_info=True)

        # For testing: stop after one iteration
        if stop_after_one_iteration:
            logger.info("Reconcile loop stopping after one iteration (test mode)")
            break

        # Interruptible sleep - allows immediate shutdown on signal
        logger.debug(f"Reconcile sleeping for {interval_seconds}s")
        _shutdown_event.wait(interval_seconds)

    # Graceful shutdown summary
    logger.info(
        f"Reconcile loop stopped gracefully after {iteration} iterations",
        extra={
            "total_iterations": iteration,
            "total_roll_forward": total_roll_forward,
            "total_roll_back": total_roll_back,
            "total_scanned": total_scanned,
        },
    )
