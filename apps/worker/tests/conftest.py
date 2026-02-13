"""Pytest configuration and fixtures for worker tests."""

import sys
from pathlib import Path

import pytest
import redis
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

# Add API path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "api"))

from dpp_api.budget import BudgetManager
from dpp_api.db.models import Base

# Use in-memory SQLite for tests
TEST_DATABASE_URL = "sqlite:///:memory:"

# Redis test settings
REDIS_TEST_HOST = "localhost"
REDIS_TEST_PORT = 6379
REDIS_TEST_DB = 15  # Use separate DB for tests


@pytest.fixture(scope="function")
def db_session() -> Session:
    """Create a fresh database session for each test.

    Uses in-memory SQLite for fast, isolated tests.
    """
    # Create in-memory engine
    engine = create_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    # Create all tables
    Base.metadata.create_all(engine)

    # Create session
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()

    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def redis_client() -> redis.Redis:
    """Create a fresh Redis client for each test.

    Uses Redis DB 15 for tests and flushes it before each test.
    """
    client = redis.Redis(
        host=REDIS_TEST_HOST,
        port=REDIS_TEST_PORT,
        db=REDIS_TEST_DB,
        decode_responses=True,
    )

    # Flush test database before each test
    client.flushdb()

    try:
        yield client
    finally:
        # Clean up after test
        client.flushdb()
        client.close()


@pytest.fixture(scope="function")
def budget_manager(redis_client: redis.Redis, db_session: Session) -> BudgetManager:
    """Create BudgetManager instance for tests."""
    return BudgetManager(redis_client, db_session)
