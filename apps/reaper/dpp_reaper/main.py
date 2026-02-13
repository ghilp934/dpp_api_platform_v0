"""DPP Reaper main entry point.

Reaper Service: Periodically scan for zombie runs (lease expired) and terminate them.

Spec 10.1, 10.2: Reaper Service
- Scan: status='PROCESSING' AND lease_expires_at < NOW()
- Finalize: 2-phase commit with minimum_fee charge
- Interval: 30 seconds (configurable)
"""

import logging
import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../api"))

from dpp_api.budget import BudgetManager
from dpp_api.db.redis_client import RedisClient
from dpp_reaper.loops.reaper_loop import reaper_loop

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Main entry point for reaper."""
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

    # Database
    engine = create_engine(database_url, echo=False)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db_session = SessionLocal()

    # Redis
    redis_client = RedisClient.get_client()

    # Budget manager
    budget_manager = BudgetManager(redis_client, db_session)

    logger.info("Starting DPP Reaper...")
    logger.info(f"Scan interval: {reaper_interval_sec}s")
    logger.info(f"Scan limit: {reaper_scan_limit} runs per iteration")

    try:
        # Run reaper loop (blocks forever)
        reaper_loop(
            db=db_session,
            budget_manager=budget_manager,
            interval_seconds=reaper_interval_sec,
            limit_per_scan=reaper_scan_limit,
        )
    except KeyboardInterrupt:
        logger.info("Reaper stopped by user")
    finally:
        db_session.close()
        logger.info("Reaper shutdown complete")


if __name__ == "__main__":
    main()
