"""DPP Worker main entry point."""

import logging
import os
import sys

import boto3
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../api"))

from dpp_api.budget import BudgetManager
from dpp_api.db.redis_client import RedisClient
from dpp_worker.loops.sqs_loop import WorkerLoop

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Main entry point for worker."""
    # Configuration from environment
    database_url = os.getenv(
        "DATABASE_URL", "postgresql://dpp_user:dpp_pass@localhost:5432/dpp_db"
    )
    sqs_queue_url = os.getenv("SQS_QUEUE_URL", "http://localhost:4566/000000000000/dpp-runs")
    s3_result_bucket = os.getenv("S3_RESULT_BUCKET", "dpp-results")

    # AWS clients (LocalStack for dev)
    sqs_endpoint = os.getenv("SQS_ENDPOINT_URL", "http://localhost:4566")
    s3_endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")

    sqs_client = boto3.client(
        "sqs",
        endpoint_url=sqs_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    # Database
    engine = create_engine(database_url, echo=False)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db_session = SessionLocal()

    # Redis
    redis_client = RedisClient.get_client()

    # Budget manager
    budget_manager = BudgetManager(redis_client, db_session)

    # Worker loop
    worker = WorkerLoop(
        sqs_client=sqs_client,
        s3_client=s3_client,
        db_session=db_session,
        budget_manager=budget_manager,
        queue_url=sqs_queue_url,
        result_bucket=s3_result_bucket,
        lease_ttl_sec=120,
    )

    logger.info("Starting DPP Worker...")
    logger.info(f"Queue URL: {sqs_queue_url}")
    logger.info(f"Result bucket: {s3_result_bucket}")

    try:
        worker.run_forever()
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    finally:
        db_session.close()
        logger.info("Worker shutdown complete")


if __name__ == "__main__":
    main()
