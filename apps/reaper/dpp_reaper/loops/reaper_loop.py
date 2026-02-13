"""Reaper loop for detecting and terminating zombie runs.

Spec 10.1, 10.2: Reaper Service
- Scan: status='PROCESSING' AND lease_expires_at < NOW()
- Finalize: 2-phase commit with minimum_fee charge
- Interval: 30 seconds (configurable)
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from dpp_api.budget import BudgetManager
from dpp_api.db.models import Run
from dpp_api.db.redis_client import RedisClient
from dpp_worker.finalize.optimistic_commit import ClaimError, finalize_timeout

logger = logging.getLogger(__name__)


def scan_expired_runs(db: Session, limit: int = 100) -> list[Run]:
    """Scan for runs with expired leases (zombie runs).

    Spec 10.1: Scan
    - Query: status='PROCESSING' AND lease_expires_at < NOW()
    - Limit: Prevent overwhelming reaper with too many runs

    Args:
        db: Database session
        limit: Maximum number of runs to scan per iteration

    Returns:
        List of expired runs
    """
    now = datetime.now(timezone.utc)

    stmt = (
        select(Run)
        .where(
            and_(
                Run.status == "PROCESSING",
                Run.lease_expires_at < now,
            )
        )
        .limit(limit)
    )

    result = db.execute(stmt)
    runs = result.scalars().all()

    if runs:
        logger.info(f"Reaper scan found {len(runs)} expired runs")

    return list(runs)


def reap_run(
    run: Run,
    db: Session,
    budget_manager: BudgetManager,
) -> bool:
    """Attempt to terminate a single zombie run.

    Spec 10.2: Reaper finalize (winner-only)
    - Claim: DB-CAS with finalize_stage IS NULL
    - Settle: minimum_fee = min(minimum_fee, reserved)
    - Commit: status='FAILED', reason_code='WORKER_TIMEOUT'

    Args:
        run: Run to terminate
        db: Database session
        budget_manager: Budget manager for settlement

    Returns:
        True if reaper won (successfully reaped), False if lost race
    """
    run_id = run.run_id
    tenant_id = run.tenant_id

    # Calculate minimum fee (defensive: use min with reserved)
    # Spec: charge = min(minimum_fee, reserved)
    charge_usd_micros = min(
        run.minimum_fee_usd_micros or 0,
        run.reservation_max_cost_usd_micros or 0,
    )

    try:
        # Call finalize_timeout from refactored optimistic_commit module
        # This uses the 2-phase commit logic:
        # - Phase A: CLAIM (with finalize_stage IS NULL)
        # - Phase B: SETTLE + COMMIT (FAILED + WORKER_TIMEOUT)
        finalize_timeout(
            run_id=run_id,
            tenant_id=tenant_id,
            minimum_fee_usd_micros=charge_usd_micros,
            db=db,
            budget_manager=budget_manager,
        )

        logger.info(
            f"Reaper WINNER: Terminated zombie run {run_id}, "
            f"charged {charge_usd_micros} micros"
        )
        return True

    except ClaimError as e:
        # Lost race - Worker or another Reaper already claimed
        # This is expected and normal - just log and move on
        logger.debug(f"Reaper lost race for run {run_id}: {e}")
        return False

    except Exception as e:
        # Unexpected error (DB issue, budget issue, etc.)
        logger.error(f"Reaper failed to terminate run {run_id}: {e}", exc_info=True)
        return False


def reaper_loop(
    db: Session,
    budget_manager: Optional[BudgetManager] = None,
    interval_seconds: int = 30,
    limit_per_scan: int = 100,
    stop_after_one_iteration: bool = False,
) -> None:
    """Main reaper loop - periodically scan and terminate zombie runs.

    Spec 10.1, 10.2: Reaper Service
    - Interval: 30 seconds (default)
    - Scan limit: 100 runs per iteration (prevent overload)
    - Graceful handling: Lost races are expected and logged

    Args:
        db: Database session
        budget_manager: Budget manager (optional, will create if not provided)
        interval_seconds: Sleep interval between scans (default 30)
        limit_per_scan: Max runs to process per iteration (default 100)
        stop_after_one_iteration: For testing only - exit after one scan

    Returns:
        None (runs forever unless stop_after_one_iteration=True)
    """
    if budget_manager is None:
        redis_client = RedisClient.get_client()
        budget_manager = BudgetManager(redis_client, db)

    logger.info(
        f"Reaper loop started (interval={interval_seconds}s, limit={limit_per_scan})"
    )

    iteration = 0
    while True:
        iteration += 1
        logger.debug(f"Reaper iteration {iteration} starting")

        try:
            # Scan for expired runs
            expired_runs = scan_expired_runs(db, limit=limit_per_scan)

            if not expired_runs:
                logger.debug("No expired runs found")
            else:
                # Attempt to reap each expired run
                wins = 0
                losses = 0

                for run in expired_runs:
                    won = reap_run(run, db, budget_manager)
                    if won:
                        wins += 1
                    else:
                        losses += 1

                logger.info(
                    f"Reaper iteration {iteration}: "
                    f"{wins} reaped, {losses} lost races, "
                    f"{len(expired_runs)} total scanned"
                )

        except Exception as e:
            logger.error(f"Reaper loop error in iteration {iteration}: {e}", exc_info=True)

        # For testing: stop after one iteration
        if stop_after_one_iteration:
            logger.info("Reaper loop stopping after one iteration (test mode)")
            break

        # Sleep before next scan
        logger.debug(f"Reaper sleeping for {interval_seconds}s")
        time.sleep(interval_seconds)
