"""Tests for Reconcile Loop (P0-2: DEC-4206 Atomic Commit Safety).

Tests recovery of runs stuck in finalize_stage='CLAIMED' state.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.orm import Session, sessionmaker

from dpp_api.budget import BudgetManager
from dpp_api.db.models import Run
from dpp_api.db.repo_runs import RunRepository
from dpp_reaper.loops.reconcile_loop import (
    reconcile_loop,
    reconcile_stuck_run,
    roll_back_stuck_run,
    roll_forward_stuck_run,
    scan_stuck_claimed_runs,
)


def test_scan_stuck_claimed_runs_finds_old_claimed(
    db_session: Session,
    redis_client,
) -> None:
    """Test that scan finds CLAIMED runs older than threshold."""
    tenant_id = "tenant_reconcile_scan"
    repo = RunRepository(db_session)

    # Create stuck CLAIMED run (10 minutes old)
    stuck_run_id = str(uuid.uuid4())
    stuck_run = Run(
        run_id=stuck_run_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        finalize_stage="CLAIMED",
        finalize_token=str(uuid.uuid4()),
        finalize_claimed_at=datetime.now(timezone.utc) - timedelta(minutes=10),
        version=1,
        payload_hash="hash123",
        reservation_max_cost_usd_micros=500000,
        minimum_fee_usd_micros=100000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=7),
    )
    db_session.add(stuck_run)

    # Create recent CLAIMED run (2 minutes old - should be ignored)
    recent_run_id = str(uuid.uuid4())
    recent_run = Run(
        run_id=recent_run_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        finalize_stage="CLAIMED",
        finalize_token=str(uuid.uuid4()),
        finalize_claimed_at=datetime.now(timezone.utc) - timedelta(minutes=2),
        version=1,
        payload_hash="hash456",
        reservation_max_cost_usd_micros=500000,
        minimum_fee_usd_micros=100000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=7),
    )
    db_session.add(recent_run)

    db_session.commit()

    # Scan with 5 minute threshold
    stuck_runs = scan_stuck_claimed_runs(db_session, stuck_threshold_minutes=5)

    # Should find only the 10-minute-old run
    assert len(stuck_runs) == 1
    assert stuck_runs[0].run_id == stuck_run_id


def test_roll_forward_completes_stuck_run(
    db_session: Session,
    redis_client,
) -> None:
    """Test roll-forward: Stuck CLAIMED run with S3 result → COMPLETED."""
    tenant_id = "tenant_roll_forward"
    run_id = str(uuid.uuid4())
    repo = RunRepository(db_session)
    budget_manager = BudgetManager(redis_client, db_session)

    # Setup: Create budget and reserve
    budget_manager.scripts.set_balance(tenant_id, 1000000)
    budget_manager.scripts.reserve(tenant_id, run_id, 500000)

    # Create stuck CLAIMED run with S3 pointers (simulates successful S3 upload)
    stuck_run = Run(
        run_id=run_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        finalize_stage="CLAIMED",
        finalize_token=str(uuid.uuid4()),
        finalize_claimed_at=datetime.now(timezone.utc) - timedelta(minutes=10),
        version=1,
        payload_hash="hash789",
        reservation_max_cost_usd_micros=500000,
        actual_cost_usd_micros=300000,  # Worker calculated cost before crash
        minimum_fee_usd_micros=100000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=7),
        # S3 pointers set (Worker uploaded before crash)
        result_bucket="dpp-results",
        result_key="results/test.json",
        result_sha256="abc123",
    )
    db_session.add(stuck_run)
    db_session.commit()

    # Roll-forward
    success = roll_forward_stuck_run(stuck_run, db_session, budget_manager)

    assert success is True

    # Verify run is COMPLETED
    updated_run = repo.get_by_id(run_id, tenant_id)
    assert updated_run is not None
    assert updated_run.status == "COMPLETED"
    assert updated_run.money_state == "SETTLED"
    assert updated_run.finalize_stage == "COMMITTED"
    assert updated_run.completed_at is not None

    # Verify budget settled (charged actual_cost)
    balance = budget_manager.get_balance(tenant_id)
    assert balance == 700000  # 1000000 - 300000


def test_roll_back_fails_stuck_run(
    db_session: Session,
    redis_client,
) -> None:
    """Test roll-back: Stuck CLAIMED run without S3 result → FAILED."""
    tenant_id = "tenant_roll_back"
    run_id = str(uuid.uuid4())
    repo = RunRepository(db_session)
    budget_manager = BudgetManager(redis_client, db_session)

    # Setup: Create budget and reserve
    budget_manager.scripts.set_balance(tenant_id, 1000000)
    budget_manager.scripts.reserve(tenant_id, run_id, 500000)

    # Create stuck CLAIMED run WITHOUT S3 pointers (Worker crashed before S3 upload)
    stuck_run = Run(
        run_id=run_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        finalize_stage="CLAIMED",
        finalize_token=str(uuid.uuid4()),
        finalize_claimed_at=datetime.now(timezone.utc) - timedelta(minutes=10),
        version=1,
        payload_hash="hash999",
        reservation_max_cost_usd_micros=500000,
        minimum_fee_usd_micros=100000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=7),
        # NO S3 pointers (Worker crashed before upload)
        result_bucket=None,
        result_key=None,
        result_sha256=None,
    )
    db_session.add(stuck_run)
    db_session.commit()

    # Roll-back
    success = roll_back_stuck_run(stuck_run, db_session, budget_manager)

    assert success is True

    # Verify run is FAILED
    updated_run = repo.get_by_id(run_id, tenant_id)
    assert updated_run is not None
    assert updated_run.status == "FAILED"
    assert updated_run.money_state == "SETTLED"
    assert updated_run.finalize_stage == "COMMITTED"
    assert updated_run.completed_at is not None
    assert updated_run.last_error_reason_code == "WORKER_CRASH_DURING_FINALIZE"

    # Verify budget settled (charged minimum_fee)
    balance = budget_manager.get_balance(tenant_id)
    assert balance == 900000  # 1000000 - 100000 (minimum_fee)


def test_reconcile_stuck_run_decides_correctly(
    db_session: Session,
    redis_client,
) -> None:
    """Test reconcile_stuck_run routes to roll-forward or roll-back based on S3."""
    tenant_id = "tenant_reconcile_decision"
    budget_manager = BudgetManager(redis_client, db_session)

    # Test case 1: S3 exists → roll-forward
    run1_id = str(uuid.uuid4())
    budget_manager.scripts.set_balance(tenant_id, 2000000)
    budget_manager.scripts.reserve(tenant_id, run1_id, 500000)

    run1 = Run(
        run_id=run1_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        finalize_stage="CLAIMED",
        finalize_token=str(uuid.uuid4()),
        finalize_claimed_at=datetime.now(timezone.utc) - timedelta(minutes=10),
        version=1,
        payload_hash="hash_rf",
        reservation_max_cost_usd_micros=500000,
        actual_cost_usd_micros=300000,
        minimum_fee_usd_micros=100000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=7),
        result_bucket="dpp-results",  # S3 exists
        result_key="results/test1.json",
        result_sha256="sha1",
    )
    db_session.add(run1)
    db_session.commit()

    success1 = reconcile_stuck_run(run1, db_session, budget_manager)
    assert success1 is True

    repo = RunRepository(db_session)
    updated_run1 = repo.get_by_id(run1_id, tenant_id)
    assert updated_run1.status == "COMPLETED"  # Roll-forward

    # Test case 2: S3 missing → roll-back
    run2_id = str(uuid.uuid4())
    budget_manager.scripts.reserve(tenant_id, run2_id, 500000)

    run2 = Run(
        run_id=run2_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        finalize_stage="CLAIMED",
        finalize_token=str(uuid.uuid4()),
        finalize_claimed_at=datetime.now(timezone.utc) - timedelta(minutes=10),
        version=1,
        payload_hash="hash_rb",
        reservation_max_cost_usd_micros=500000,
        minimum_fee_usd_micros=100000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=7),
        result_bucket=None,  # S3 missing
        result_key=None,
        result_sha256=None,
    )
    db_session.add(run2)
    db_session.commit()

    success2 = reconcile_stuck_run(run2, db_session, budget_manager)
    assert success2 is True

    updated_run2 = repo.get_by_id(run2_id, tenant_id)
    assert updated_run2.status == "FAILED"  # Roll-back


def test_reconcile_loop_one_iteration(
    db_engine,
    redis_client,
) -> None:
    """Test reconcile loop processes stuck runs correctly."""
    # Create separate session for test
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
    db_session = SessionLocal()

    tenant_id = "tenant_reconcile_loop"
    budget_manager = BudgetManager(redis_client, db_session)
    budget_manager.scripts.set_balance(tenant_id, 2000000)

    # Create two stuck CLAIMED runs
    run1_id = str(uuid.uuid4())
    run2_id = str(uuid.uuid4())

    budget_manager.scripts.reserve(tenant_id, run1_id, 500000)
    budget_manager.scripts.reserve(tenant_id, run2_id, 500000)

    # Run 1: S3 exists → should roll-forward
    run1 = Run(
        run_id=run1_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        finalize_stage="CLAIMED",
        finalize_token=str(uuid.uuid4()),
        finalize_claimed_at=datetime.now(timezone.utc) - timedelta(minutes=10),
        version=1,
        payload_hash="hash_loop1",
        reservation_max_cost_usd_micros=500000,
        actual_cost_usd_micros=300000,
        minimum_fee_usd_micros=100000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=7),
        result_bucket="dpp-results",
        result_key="results/loop1.json",
        result_sha256="sha_loop1",
    )

    # Run 2: S3 missing → should roll-back
    run2 = Run(
        run_id=run2_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        finalize_stage="CLAIMED",
        finalize_token=str(uuid.uuid4()),
        finalize_claimed_at=datetime.now(timezone.utc) - timedelta(minutes=10),
        version=1,
        payload_hash="hash_loop2",
        reservation_max_cost_usd_micros=500000,
        minimum_fee_usd_micros=100000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=7),
        result_bucket=None,
        result_key=None,
        result_sha256=None,
    )

    db_session.add(run1)
    db_session.add(run2)
    db_session.commit()

    # Run reconcile loop for one iteration
    reconcile_loop(
        db=db_session,
        budget_manager=budget_manager,
        interval_seconds=1,
        stuck_threshold_minutes=5,
        stop_after_one_iteration=True,
    )

    # Verify results
    repo = RunRepository(db_session)

    updated_run1 = repo.get_by_id(run1_id, tenant_id)
    assert updated_run1.status == "COMPLETED"  # Roll-forward
    assert updated_run1.finalize_stage == "COMMITTED"

    updated_run2 = repo.get_by_id(run2_id, tenant_id)
    assert updated_run2.status == "FAILED"  # Roll-back
    assert updated_run2.finalize_stage == "COMMITTED"
    assert updated_run2.last_error_reason_code == "WORKER_CRASH_DURING_FINALIZE"

    # Budget: 2000000 - 300000 (run1) - 100000 (run2) = 1600000
    balance = budget_manager.get_balance(tenant_id)
    assert balance == 1600000

    db_session.close()
