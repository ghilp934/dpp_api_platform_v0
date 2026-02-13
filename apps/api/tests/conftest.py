"""Pytest configuration and fixtures."""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dpp_api.db.models import Base

# Use in-memory SQLite for tests
TEST_DATABASE_URL = "sqlite:///:memory:"


@pytest.fixture(scope="function")
def db_session() -> Session:
    """
    Create a fresh database session for each test.

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
