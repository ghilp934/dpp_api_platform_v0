"""Tests for RunRepository (DEC-4210 optimistic locking)."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.orm import Session

from dpp_api.db.models import Run
from dpp_api.db.repo_runs import RunRepository


@pytest.fixture
def run_repo(db_session: Session) -> RunRepository:
    """Create RunRepository instance."""
    return RunRepository(db_session)


@pytest.fixture
def sample_run(db_session: Session) -> Run:
    """Create a sample run for testing."""
    run = Run(
        run_id=str(uuid.uuid4()),
        tenant_id="tenant_test",
        pack_type="urlpack",
        profile_version="v0.4.2.2",
        status="QUEUED",
        money_state="NONE",
        payload_hash="test_hash",
        version=0,
        reservation_max_cost_usd_micros=1000000,  # $1.00
        minimum_fee_usd_micros=10000,  # $0.01
        retention_until=datetime.now(timezone.utc) + timedelta(days=7),
    )
    db_session.add(run)
    db_session.commit()
    db_session.refresh(run)
    return run


def test_create_run(run_repo: RunRepository):
    """Test creating a run."""
    run = Run(
        run_id=str(uuid.uuid4()),
        tenant_id="tenant_create",
        pack_type="urlpack",
        profile_version="v0.4.2.2",
        status="QUEUED",
        money_state="NONE",
        payload_hash="hash_create",
        version=0,
        reservation_max_cost_usd_micros=2000000,
        minimum_fee_usd_micros=10000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=7),
    )

    created = run_repo.create(run)

    assert created.run_id == run.run_id
    assert created.version == 0
    assert created.reservation_max_cost_usd_micros == 2000000


def test_get_by_id(run_repo: RunRepository, sample_run: Run):
    """Test getting run by ID with tenant ownership check."""
    # Get with correct tenant
    found = run_repo.get_by_id(sample_run.run_id, sample_run.tenant_id)
    assert found is not None
    assert found.run_id == sample_run.run_id

    # Get with wrong tenant (owner guard)
    not_found = run_repo.get_by_id(sample_run.run_id, "wrong_tenant")
    assert not_found is None


def test_update_with_version_check_success(run_repo: RunRepository, sample_run: Run):
    """Test successful update with version check (DEC-4210)."""
    # Update with correct version
    success = run_repo.update_with_version_check(
        run_id=sample_run.run_id,
        tenant_id=sample_run.tenant_id,
        expected_version=0,
        updates={"status": "PROCESSING"},
    )

    assert success is True

    # Verify update
    updated = run_repo.get_by_id(sample_run.run_id, sample_run.tenant_id)
    assert updated.status == "PROCESSING"
    assert updated.version == 1  # Version incremented


def test_update_with_version_check_failure(run_repo: RunRepository, sample_run: Run):
    """Test failed update with wrong version (DEC-4210 loser)."""
    # First update succeeds
    success1 = run_repo.update_with_version_check(
        run_id=sample_run.run_id,
        tenant_id=sample_run.tenant_id,
        expected_version=0,
        updates={"status": "PROCESSING"},
    )
    assert success1 is True

    # Second update with old version fails (race loser)
    success2 = run_repo.update_with_version_check(
        run_id=sample_run.run_id,
        tenant_id=sample_run.tenant_id,
        expected_version=0,  # Wrong version (now it's 1)
        updates={"status": "COMPLETED"},
    )
    assert success2 is False

    # Verify original update is preserved
    final = run_repo.get_by_id(sample_run.run_id, sample_run.tenant_id)
    assert final.status == "PROCESSING"  # Not COMPLETED
    assert final.version == 1


def test_get_by_idempotency_key(run_repo: RunRepository, sample_run: Run):
    """Test getting run by idempotency key."""
    # Update run with idempotency key
    run_repo.update_with_version_check(
        run_id=sample_run.run_id,
        tenant_id=sample_run.tenant_id,
        expected_version=0,
        updates={"idempotency_key": "idem_test"},
    )

    # Find by idempotency key
    found = run_repo.get_by_idempotency_key(sample_run.tenant_id, "idem_test")
    assert found is not None
    assert found.run_id == sample_run.run_id

    # Not found with wrong tenant
    not_found = run_repo.get_by_idempotency_key("wrong_tenant", "idem_test")
    assert not_found is None


def test_claim_for_processing(run_repo: RunRepository, sample_run: Run):
    """Test claiming a run for processing."""
    lease_token = str(uuid.uuid4())
    lease_expires_at = datetime.now(timezone.utc) + timedelta(seconds=120)

    claimed = run_repo.claim_for_processing(
        sample_run.run_id, lease_token, lease_expires_at
    )

    assert claimed is not None
    assert claimed.status == "PROCESSING"
    assert claimed.lease_token == lease_token

    # Second claim fails (already claimed)
    claimed2 = run_repo.claim_for_processing(
        sample_run.run_id, str(uuid.uuid4()), lease_expires_at
    )
    assert claimed2 is None


def test_claim_for_finalize(run_repo: RunRepository, sample_run: Run):
    """Test claiming a run for finalization (DEC-4210 2-phase)."""
    finalize_token = str(uuid.uuid4())

    # First claim succeeds
    success1 = run_repo.claim_for_finalize(
        run_id=sample_run.run_id,
        tenant_id=sample_run.tenant_id,
        expected_version=0,
        finalize_token=finalize_token,
    )
    assert success1 is True

    # Verify claim
    claimed = run_repo.get_by_id(sample_run.run_id, sample_run.tenant_id)
    assert claimed.finalize_stage == "CLAIMED"
    assert claimed.finalize_token == finalize_token
    assert claimed.version == 1

    # Second claim fails (race loser)
    success2 = run_repo.claim_for_finalize(
        run_id=sample_run.run_id,
        tenant_id=sample_run.tenant_id,
        expected_version=0,  # Old version
        finalize_token=str(uuid.uuid4()),
    )
    assert success2 is False


def test_list_expired_leases(run_repo: RunRepository, db_session: Session):
    """Test listing runs with expired leases."""
    now = datetime.now(timezone.utc)

    # Create run with expired lease
    expired_run = Run(
        run_id=str(uuid.uuid4()),
        tenant_id="tenant_expired",
        pack_type="urlpack",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        payload_hash="hash_expired",
        version=0,
        reservation_max_cost_usd_micros=1000000,
        minimum_fee_usd_micros=10000,
        retention_until=now + timedelta(days=7),
        lease_token=str(uuid.uuid4()),
        lease_expires_at=now - timedelta(seconds=10),  # Expired
    )
    db_session.add(expired_run)

    # Create run with valid lease
    valid_run = Run(
        run_id=str(uuid.uuid4()),
        tenant_id="tenant_valid",
        pack_type="urlpack",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        payload_hash="hash_valid",
        version=0,
        reservation_max_cost_usd_micros=1000000,
        minimum_fee_usd_micros=10000,
        retention_until=now + timedelta(days=7),
        lease_token=str(uuid.uuid4()),
        lease_expires_at=now + timedelta(seconds=60),  # Valid
    )
    db_session.add(valid_run)
    db_session.commit()

    # List expired leases
    expired = run_repo.list_expired_leases()

    assert len(expired) == 1
    assert expired[0].run_id == expired_run.run_id
