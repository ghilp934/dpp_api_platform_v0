"""DPP Reaper main entry point.

Reaper Service: Two independent loops for run lifecycle management.

1. Reaper Loop (Spec 10.1, 10.2):
   - Scan: status='PROCESSING' AND lease_expires_at < NOW()
   - Finalize: 2-phase commit with minimum_fee charge
   - Interval: 30 seconds

2. Reconcile Loop (P0-2: DEC-4206):
   - Scan: status='PROCESSING' AND finalize_stage='CLAIMED' AND finalize_claimed_at < NOW-5min
   - Recover: Roll-forward (S3 exists) or Roll-back (S3 missing)
   - Interval: 60 seconds
"""

import logging
import os
import sys
import threading

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../api"))

from dpp_api.budget import BudgetManager
from dpp_api.db.redis_client import RedisClient
from dpp_reaper.loops.reaper_loop import reaper_loop
from dpp_reaper.loops.reconcile_loop import reconcile_loop

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Main entry point for reaper.

    P0-2: Runs two independent loops in separate threads:
    - Reaper Loop: Detect and terminate zombie runs (lease expired)
    - Reconcile Loop: Recover stuck CLAIMED runs (Worker crash during finalize)
    """
    # ENV-01: Configuration from environment with fail-fast
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        # Default to docker-compose configuration (ENV-01: unified to 'dpp')
        database_url = "postgresql://dpp_user:dpp_pass@localhost:5432/dpp"
        logger.warning(
            "DATABASE_URL not set, using default: %s",
            database_url.replace("dpp_pass", "***"),
        )

    # Reaper configuration
    reaper_interval_sec = int(os.getenv("REAPER_INTERVAL_SEC", "30"))
    reaper_scan_limit = int(os.getenv("REAPER_SCAN_LIMIT", "100"))

    # Reconcile configuration (P0-2)
    reconcile_interval_sec = int(os.getenv("RECONCILE_INTERVAL_SEC", "60"))
    reconcile_threshold_min = int(os.getenv("RECONCILE_THRESHOLD_MIN", "5"))
    reconcile_scan_limit = int(os.getenv("RECONCILE_SCAN_LIMIT", "100"))

    # Database engine (shared)
    engine = create_engine(database_url, echo=False)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # Create separate sessions for each loop (SQLAlchemy sessions are NOT thread-safe)
    reaper_session = SessionLocal()
    reconcile_session = SessionLocal()

    # Redis (shared - redis-py is thread-safe)
    redis_client = RedisClient.get_client()

    # Budget managers (separate instances for each loop)
    reaper_budget_manager = BudgetManager(redis_client, reaper_session)
    reconcile_budget_manager = BudgetManager(redis_client, reconcile_session)

    logger.info("Starting DPP Reaper with dual loops...")
    logger.info(f"Reaper Loop: interval={reaper_interval_sec}s, limit={reaper_scan_limit}")
    logger.info(f"Reconcile Loop: interval={reconcile_interval_sec}s, threshold={reconcile_threshold_min}min, limit={reconcile_scan_limit}")

    # P0-2: Run both loops in separate threads
    reaper_thread = threading.Thread(
        target=reaper_loop,
        kwargs={
            "db": reaper_session,
            "budget_manager": reaper_budget_manager,
            "interval_seconds": reaper_interval_sec,
            "limit_per_scan": reaper_scan_limit,
        },
        name="ReaperLoop",
        daemon=False,
    )

    reconcile_thread = threading.Thread(
        target=reconcile_loop,
        kwargs={
            "db": reconcile_session,
            "budget_manager": reconcile_budget_manager,
            "interval_seconds": reconcile_interval_sec,
            "stuck_threshold_minutes": reconcile_threshold_min,
            "limit_per_scan": reconcile_scan_limit,
        },
        name="ReconcileLoop",
        daemon=False,
    )

    try:
        # Start both threads
        logger.info("Starting Reaper Loop thread...")
        reaper_thread.start()

        logger.info("Starting Reconcile Loop thread...")
        reconcile_thread.start()

        # Wait for both threads to complete (blocks until SIGTERM/SIGINT)
        reaper_thread.join()
        reconcile_thread.join()

    except KeyboardInterrupt:
        logger.info("Reaper stopped by user (KeyboardInterrupt)")

    finally:
        # Clean up sessions
        reaper_session.close()
        reconcile_session.close()
        logger.info("Reaper shutdown complete")


if __name__ == "__main__":
    main()
