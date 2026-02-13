"""Reaper tests - Zombie run termination and race conditions.

Critical tests:
1. Worker vs Reaper race condition (exactly-once finalize)
2. Scan expired runs (lease_expires_at < NOW())
3. Minimum fee charging
4. Graceful handling of lost races
"""

import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from threading import Thread

# Add API and Worker directories to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../api"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../worker"))

import pytest
import redis
from sqlalchemy.orm import Session

from dpp_api.budget import BudgetManager
from dpp_api.budget.redis_scripts import BudgetScripts
from dpp_api.db.models import Run
from dpp_api.db.redis_client import RedisClient
from dpp_api.db.repo_runs import RunRepository
from dpp_reaper.loops.reaper_loop import reap_run, reaper_loop, scan_expired_runs
from dpp_worker.finalize.optimistic_commit import ClaimError, finalize_success


@pytest.fixture
def test_run(db_session: Session, redis_client: redis.Redis) -> Run:
    """Create a test run in PROCESSING state with expired lease."""
    tenant_id = f"tenant_{uuid.uuid4().hex[:8]}"
    run_id = str(uuid.uuid4())
    lease_token = str(uuid.uuid4())

    # Create run in PROCESSING state with EXPIRED lease
    run = Run(
        run_id=run_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        idempotency_key=f"test-{uuid.uuid4()}",
        payload_hash="dummy_hash_" + uuid.uuid4().hex,
        version=1,
        lease_token=lease_token,
        lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=30),  # Expired!
        reservation_max_cost_usd_micros=500_000,  # $0.50
        minimum_fee_usd_micros=10_000,  # $0.01
        retention_until=datetime.now(timezone.utc) + timedelta(days=30),
    )

    repo = RunRepository(db_session)
    repo.create(run)

    # Set budget and create reserve (CRITICAL: Reaper expects active reserve)
    budget_scripts = BudgetScripts(redis_client)
    budget_scripts.set_balance(tenant_id, 1_000_000)  # $1.00

    # Create reserve entry in Redis (Worker would do this during POST /v1/runs)
    budget_scripts.reserve(
        tenant_id=tenant_id,
        run_id=run_id,
        reserved_usd_micros=run.reservation_max_cost_usd_micros,
    )

    return run


def test_scan_expired_runs_finds_zombie(db_session: Session, test_run: Run):
    """Test that scan finds runs with expired leases."""
    expired_runs = scan_expired_runs(db_session)

    assert len(expired_runs) == 1
    assert expired_runs[0].run_id == test_run.run_id
    assert expired_runs[0].status == "PROCESSING"


def test_scan_expired_runs_ignores_active_lease(db_session: Session):
    """Test that scan ignores runs with active (non-expired) leases."""
    tenant_id = f"tenant_{uuid.uuid4().hex[:8]}"
    run_id = str(uuid.uuid4())

    # Create run with ACTIVE (future) lease
    run = Run(
        run_id=run_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        idempotency_key=f"test-{uuid.uuid4()}",
        payload_hash="dummy_hash_" + uuid.uuid4().hex,
        version=1,
        lease_token=str(uuid.uuid4()),
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),  # Active!
        reservation_max_cost_usd_micros=100_000,
        minimum_fee_usd_micros=5_000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=30),
    )

    repo = RunRepository(db_session)
    repo.create(run)

    # Should not find it
    expired_runs = scan_expired_runs(db_session)
    assert len(expired_runs) == 0


def test_scan_ignores_non_processing_status(db_session: Session):
    """Test that scan only finds PROCESSING runs, not QUEUED/COMPLETED/FAILED."""
    tenant_id = f"tenant_{uuid.uuid4().hex[:8]}"

    for status in ["QUEUED", "COMPLETED", "FAILED"]:
        run = Run(
            run_id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            pack_type="decision",
            profile_version="v0.4.2.2",
            status=status,
            money_state="RESERVED" if status == "QUEUED" else "SETTLED",
            idempotency_key=f"test-{uuid.uuid4()}",
            payload_hash="dummy_hash_" + uuid.uuid4().hex,
            version=1,
            lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=30),  # Expired
            reservation_max_cost_usd_micros=100_000,
            minimum_fee_usd_micros=5_000,
            retention_until=datetime.now(timezone.utc) + timedelta(days=30),
        )

        repo = RunRepository(db_session)
        repo.create(run)

    # Should not find any (only PROCESSING runs are scanned)
    expired_runs = scan_expired_runs(db_session)
    assert len(expired_runs) == 0


def test_reap_run_success_winner(
    db_session: Session, redis_client: redis.Redis, test_run: Run
):
    """Test that reaper successfully terminates a zombie run (WINNER)."""
    budget_manager = BudgetManager(redis_client, db_session)

    # Reaper attempts to reap
    won = reap_run(test_run, db_session, budget_manager)

    assert won is True

    # Verify run state
    repo = RunRepository(db_session)
    run = repo.get_by_id(test_run.run_id, test_run.tenant_id)

    assert run.status == "FAILED"
    assert run.money_state == "SETTLED"
    assert run.last_error_reason_code == "WORKER_TIMEOUT"
    assert run.finalize_stage == "COMMITTED"
    assert run.actual_cost_usd_micros == test_run.minimum_fee_usd_micros
    assert run.version == 3  # 0 -> 1 (processing) -> 2 (claim) -> 3 (commit)


def test_reap_run_charges_minimum_fee(
    db_session: Session, redis_client: redis.Redis, test_run: Run
):
    """Test that reaper charges minimum_fee correctly."""
    budget_manager = BudgetManager(redis_client, db_session)

    # Reaper attempts to reap
    reap_run(test_run, db_session, budget_manager)

    # Verify run was charged minimum_fee
    repo = RunRepository(db_session)
    final_run = repo.get_by_id(test_run.run_id, test_run.tenant_id)

    # Should charge minimum_fee (10_000)
    assert final_run.actual_cost_usd_micros == test_run.minimum_fee_usd_micros
    assert final_run.status == "FAILED"
    assert final_run.money_state == "SETTLED"


def test_reap_run_respects_reserved_cap(db_session: Session, redis_client: redis.Redis):
    """Test that reaper uses min(minimum_fee, reserved) to prevent overcharge."""
    tenant_id = f"tenant_{uuid.uuid4().hex[:8]}"
    run_id = str(uuid.uuid4())

    # Create run with reserved < minimum_fee (edge case)
    run = Run(
        run_id=run_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        idempotency_key=f"test-{uuid.uuid4()}",
        payload_hash="dummy_hash_" + uuid.uuid4().hex,
        version=1,
        lease_token=str(uuid.uuid4()),
        lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=30),
        reservation_max_cost_usd_micros=5_000,  # $0.005 (very cheap)
        minimum_fee_usd_micros=10_000,  # $0.01 (higher than reserved!)
        retention_until=datetime.now(timezone.utc) + timedelta(days=30),
    )

    repo = RunRepository(db_session)
    repo.create(run)

    # Set budget
    budget_scripts = BudgetScripts(redis_client)
    budget_scripts.set_balance(tenant_id, 1_000_000)

    # Create reserve (CRITICAL: Reaper expects active reserve)
    budget_scripts.reserve(
        tenant_id=tenant_id,
        run_id=run_id,
        reserved_usd_micros=run.reservation_max_cost_usd_micros,
    )

    # Reaper attempts to reap
    budget_manager = BudgetManager(redis_client, db_session)
    reap_run(run, db_session, budget_manager)

    # Verify it charged min(minimum_fee, reserved) = min(10_000, 5_000) = 5_000
    repo = RunRepository(db_session)
    final_run = repo.get_by_id(run_id, tenant_id)

    # Should charge min(minimum_fee, reserved) = 5_000, NOT 10_000
    assert final_run.actual_cost_usd_micros == 5_000
    assert final_run.status == "FAILED"
    assert final_run.money_state == "SETTLED"


def test_reap_run_loser_already_finalized(
    db_session: Session, redis_client: redis.Redis
):
    """Test that reaper gracefully loses race if run is already finalized."""
    tenant_id = f"tenant_{uuid.uuid4().hex[:8]}"
    run_id = str(uuid.uuid4())

    # Create run that's already COMPLETED (finalized)
    run = Run(
        run_id=run_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="COMPLETED",  # Already done!
        money_state="SETTLED",
        idempotency_key=f"test-{uuid.uuid4()}",
        payload_hash="dummy_hash_" + uuid.uuid4().hex,
        version=3,
        finalize_stage="COMMITTED",
        lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=30),
        reservation_max_cost_usd_micros=100_000,
        minimum_fee_usd_micros=5_000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=30),
    )

    repo = RunRepository(db_session)
    repo.create(run)

    budget_scripts = BudgetScripts(redis_client)
    budget_scripts.set_balance(tenant_id, 1_000_000)

    budget_manager = BudgetManager(redis_client, db_session)

    # Reaper attempts to reap (should fail gracefully)
    won = reap_run(run, db_session, budget_manager)

    assert won is False  # Lost race (run already finalized)


def test_reaper_loop_one_iteration(db_session: Session, redis_client: redis.Redis):
    """Test that reaper loop runs one iteration successfully."""
    tenant_id = f"tenant_{uuid.uuid4().hex[:8]}"

    # Create 3 expired runs
    for i in range(3):
        run = Run(
            run_id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            pack_type="decision",
            profile_version="v0.4.2.2",
            status="PROCESSING",
            money_state="RESERVED",
            idempotency_key=f"test-{uuid.uuid4()}",
            payload_hash="dummy_hash_" + uuid.uuid4().hex,
            version=1,
            lease_token=str(uuid.uuid4()),
            lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=30),
            reservation_max_cost_usd_micros=100_000,
            minimum_fee_usd_micros=5_000,
            retention_until=datetime.now(timezone.utc) + timedelta(days=30),
        )

        repo = RunRepository(db_session)
        repo.create(run)

    budget_scripts = BudgetScripts(redis_client)
    budget_scripts.set_balance(tenant_id, 10_000_000)

    # Create reserves for all runs (CRITICAL: Reaper expects active reserves)
    repo = RunRepository(db_session)
    runs = repo.db.query(Run).filter(Run.tenant_id == tenant_id).all()
    for run in runs:
        budget_scripts.reserve(
            tenant_id=tenant_id,
            run_id=run.run_id,
            reserved_usd_micros=run.reservation_max_cost_usd_micros,
        )

    budget_manager = BudgetManager(redis_client, db_session)

    # Run one iteration
    reaper_loop(
        db=db_session,
        budget_manager=budget_manager,
        interval_seconds=1,
        stop_after_one_iteration=True,
    )

    # Verify all 3 runs were reaped
    repo = RunRepository(db_session)
    runs = scan_expired_runs(db_session)

    assert len(runs) == 0  # All reaped


def test_worker_vs_reaper_race_exactly_once_finalize(
    db_session: Session, redis_client: redis.Redis
):
    """CRITICAL TEST: Worker vs Reaper race - exactly one WINNER, no double-settle.

    Scenario:
    - Worker finishes processing and attempts finalize_success
    - Reaper detects expired lease and attempts finalize_timeout
    - Both try to finalize at the same time
    - Expected: Exactly ONE wins (WINNER), other loses gracefully (ClaimError)
    - No crashes, no double-settlement
    """
    tenant_id = f"tenant_{uuid.uuid4().hex[:8]}"
    run_id = str(uuid.uuid4())
    lease_token = str(uuid.uuid4())

    # Create run in PROCESSING state with EXPIRED lease (Reaper will try to reap)
    run = Run(
        run_id=run_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        idempotency_key=f"test-{uuid.uuid4()}",
        payload_hash="dummy_hash_" + uuid.uuid4().hex,
        version=1,
        lease_token=lease_token,
        lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),  # EXPIRED!
        reservation_max_cost_usd_micros=500_000,  # $0.50
        minimum_fee_usd_micros=10_000,  # $0.01
        retention_until=datetime.now(timezone.utc) + timedelta(days=30),
    )

    repo = RunRepository(db_session)
    repo.create(run)

    budget_scripts = BudgetScripts(redis_client)
    budget_scripts.set_balance(tenant_id, 10_000_000)  # $10.00

    # Create reserve (CRITICAL: Both Worker and Reaper expect active reserve)
    budget_scripts.reserve(
        tenant_id=tenant_id,
        run_id=run_id,
        max_cost_usd_micros=run.reservation_max_cost_usd_micros,
    )

    budget_manager = BudgetManager(redis_client, db_session)

    # Get initial balance for verification
    initial_balance = budget_manager.get_balance(tenant_id)

    # Results
    worker_result = {"won": None, "error": None}
    reaper_result = {"won": None, "error": None}

    def worker_finalize():
        """Worker thread: Attempts finalize_success."""
        try:
            finalize_success(
                run_id=run_id,
                tenant_id=tenant_id,
                lease_token=lease_token,
                actual_cost_usd_micros=50_000,  # $0.05
                result_bucket="test-bucket",
                result_key="test-key",
                result_sha256="abc123",
                db=db_session,
                budget_manager=budget_manager,
            )
            worker_result["won"] = True
        except ClaimError as e:
            worker_result["won"] = False
            worker_result["error"] = str(e)
        except Exception as e:
            worker_result["error"] = f"Unexpected: {e}"

    def reaper_finalize():
        """Reaper thread: Attempts finalize_timeout."""
        try:
            won = reap_run(run, db_session, budget_manager)
            reaper_result["won"] = won
        except Exception as e:
            reaper_result["error"] = f"Unexpected: {e}"

    # Start both threads simultaneously
    worker_thread = Thread(target=worker_finalize)
    reaper_thread = Thread(target=reaper_finalize)

    worker_thread.start()
    reaper_thread.start()

    # Wait for both to finish
    worker_thread.join()
    reaper_thread.join()

    # ========================================
    # CRITICAL ASSERTIONS
    # ========================================

    # 1. No unexpected errors
    assert worker_result["error"] is None or "already claimed" in worker_result["error"]
    assert reaper_result["error"] is None

    # 2. Exactly ONE winner
    winners = sum([worker_result["won"] is True, reaper_result["won"] is True])
    assert winners == 1, f"Expected exactly 1 winner, got {winners}"

    # 3. Verify final run state (should be finalized by winner)
    final_run = repo.get_by_id(run_id, tenant_id)
    assert final_run.status in ["COMPLETED", "FAILED"]
    assert final_run.money_state == "SETTLED"
    assert final_run.finalize_stage == "COMMITTED"

    # 4. Verify budget settled exactly ONCE (no double-charge)
    final_balance = budget_manager.get_balance(tenant_id)
    charged = initial_balance - final_balance

    if final_run.status == "COMPLETED":
        # Worker won - charged actual_cost
        assert charged == 50_000
    else:
        # Reaper won - charged minimum_fee
        assert charged == 10_000

    # 5. Verify loser got ClaimError (graceful failure)
    loser_got_claim_error = (
        worker_result["won"] is False or reaper_result["won"] is False
    )
    assert loser_got_claim_error


def test_reaper_vs_reaper_race_exactly_once_finalize(
    db_session: Session, redis_client: redis.Redis, test_run: Run
):
    """Test that two Reapers racing for the same run results in exactly one WINNER."""
    budget_manager = BudgetManager(redis_client, db_session)

    reaper1_result = {"won": None}
    reaper2_result = {"won": None}

    def reaper1():
        reaper1_result["won"] = reap_run(test_run, db_session, budget_manager)

    def reaper2():
        reaper2_result["won"] = reap_run(test_run, db_session, budget_manager)

    # Start both reapers simultaneously
    t1 = Thread(target=reaper1)
    t2 = Thread(target=reaper2)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    # Exactly ONE winner
    winners = sum([reaper1_result["won"], reaper2_result["won"]])
    assert winners == 1

    # Verify final state
    repo = RunRepository(db_session)
    final_run = repo.get_by_id(test_run.run_id, test_run.tenant_id)

    assert final_run.status == "FAILED"
    assert final_run.money_state == "SETTLED"
    assert final_run.finalize_stage == "COMMITTED"
