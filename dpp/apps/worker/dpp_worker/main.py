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
from dpp_api.utils import configure_json_logging
from dpp_worker.loops.sqs_loop import WorkerLoop

# P1-H: Configure structured JSON logging (same as API)
configure_json_logging(log_level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


def main() -> None:
    """Main entry point for worker."""
    # ENV-01: Configuration from environment with fail-fast
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        # Default to docker-compose configuration (ENV-01: unified to 'dpp')
        database_url = "postgresql://dpp_user:dpp_pass@localhost:5432/dpp"
        logger.warning(
            "DATABASE_URL not set, using default: %s",
            database_url.replace("dpp_pass", "***"),
        )
    sqs_queue_url = os.getenv("SQS_QUEUE_URL", "http://localhost:4566/000000000000/dpp-runs")
    s3_result_bucket = os.getenv("S3_RESULT_BUCKET", "dpp-results")

    # AWS clients (LocalStack for dev)
    sqs_endpoint = os.getenv("SQS_ENDPOINT_URL", "http://localhost:4566")
    s3_endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")

    # P0-2: Only use test credentials for LocalStack
    # Production uses boto3 default credential chain (IAM roles, env vars, etc.)
    def is_localstack(endpoint: str | None) -> bool:
        """Check if endpoint is LocalStack."""
        return endpoint is not None and ("localhost" in endpoint or "127.0.0.1" in endpoint)

    sqs_kwargs = {
        "endpoint_url": sqs_endpoint,
        "region_name": "us-east-1",
    }
    if is_localstack(sqs_endpoint):
        sqs_kwargs["aws_access_key_id"] = "test"
        sqs_kwargs["aws_secret_access_key"] = "test"
        logger.info("Using LocalStack test credentials for SQS")

    sqs_client = boto3.client("sqs", **sqs_kwargs)

    s3_kwargs = {
        "endpoint_url": s3_endpoint,
        "region_name": "us-east-1",
    }
    if is_localstack(s3_endpoint):
        s3_kwargs["aws_access_key_id"] = "test"
        s3_kwargs["aws_secret_access_key"] = "test"
        logger.info("Using LocalStack test credentials for S3")

    s3_client = boto3.client("s3", **s3_kwargs)

    # Database
    engine = create_engine(database_url, echo=False)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db_session = SessionLocal()

    # Redis
    redis_client = RedisClient.get_client()

    # Budget manager
    budget_manager = BudgetManager(redis_client, db_session)

    # Worker loop
    # P0-1: Pass session_factory for HeartbeatThread thread-safety
    worker = WorkerLoop(
        sqs_client=sqs_client,
        s3_client=s3_client,
        db_session=db_session,
        session_factory=SessionLocal,
        budget_manager=budget_manager,
        queue_url=sqs_queue_url,
        result_bucket=s3_result_bucket,
        redis_client=redis_client,
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
