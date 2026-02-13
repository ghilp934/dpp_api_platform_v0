"""
Chaos / resilience tests for DEC-4210 2-phase finalize.

These tests intentionally simulate crashes and races around the
Claim → (external side-effects) → Commit pattern implemented in:
  dpp_worker.finalize.optimistic_commit

MS-6 intent
- Crash right after CLAIM should leave a DB state that a Reaper (or a future
  reconcile job) can recover: finalize_stage='CLAIMED', status='PROCESSING', and
  money_state='RESERVED' (i.e., money hasn't moved).
- Worker vs Reaper races should yield exactly one winner (single settlement).

Notes on concurrency
- SQLAlchemy Session objects must NOT be shared across threads. Each thread
  creates its own Session instance.
- SQLite concurrency is limited; the race test uses a file-based sqlite DB with
  per-thread sessions to exercise the optimistic-locking CAS path.
"""

from __future__ import annotations

import os
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from unittest.mock import patch

# Mirror apps/worker/tests/test_finalize.py: add API package root to sys.path.
THIS_DIR = os.path.dirname(__file__)
DPP_API_PATH = os.path.abspath(os.path.join(THIS_DIR, "..", "..", "api"))
if DPP_API_PATH not in sys.path:
    sys.path.insert(0, DPP_API_PATH)

from dpp_api.budget import BudgetManager
from dpp_api.db.models import Base, Run
from dpp_api.db.repo_runs import RunRepository
from dpp_worker.finalize.optimistic_commit import (
    ClaimError,
    claim_finalize,
    commit_finalize,
    finalize_success,
    finalize_timeout,
)


# -----------------------------
# Helpers
# -----------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _create_processing_run(
    db: Session,
    budget_manager: BudgetManager,
    *,
    reserved_usd_micros: int = 1_000_000,
    minimum_fee_usd_micros: int = 50_000,
    lease_expires_at: datetime | None = None,
) -> Run:
    """Create a PROCESSING run with RESERVED money + a live reservation in Redis."""
    if lease_expires_at is None:
        lease_expires_at = _utcnow() + timedelta(minutes=5)

    run_id = str(uuid.uuid4())
    tenant_id = "chaos-tenant"
    lease_token = str(uuid.uuid4())

    # Budget + reservation (Redis)
    budget_manager.set_balance(tenant_id, 10_000_000)
    status, new_balance = budget_manager.scripts.reserve(tenant_id, run_id, reserved_usd_micros)
    assert status == "OK"

    run = Run(
        run_id=run_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        idempotency_key=f"chaos-{run_id}",
        payload_hash=f"sha256:{run_id}",
        lease_token=lease_token,
        lease_expires_at=lease_expires_at,
        money_state="RESERVED",
        reservation_max_cost_usd_micros=reserved_usd_micros,
        minimum_fee_usd_micros=minimum_fee_usd_micros,
        finalize_stage=None,
        version=0,
        retention_until=_utcnow() + timedelta(days=30),
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def _new_sqlite_sessionmaker(db_path: str) -> sessionmaker:
    """Create a sqlite engine suitable for multi-threaded tests."""
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False, "timeout": 30},
        future=True,
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autocommit=False, autoflush=False)


# -----------------------------
# Chaos tests
# -----------------------------

def test_chaos_crash_after_claim_before_commit(db_session, budget_manager):
    """
    Scenario: The worker CLAIMS finalize in DB, but crashes before COMMIT.
    Goal: DB must be left in a recoverable state:
      - finalize_stage == 'CLAIMED'
      - status still 'PROCESSING' (not COMPLETED/FAILED)
      - money_state still 'RESERVED' (money hasn't moved)
      - reservation still exists in Redis (since settle never happened)
    """
    run = _create_processing_run(db_session, budget_manager)

    # 1) CLAIM phase
    initial_version = run.version
    finalize_token, claimed_version = claim_finalize(
        run_id=run.run_id,
        tenant_id=run.tenant_id,
        db=db_session,
        extra_claim_conditions={"lease_token": run.lease_token},
    )
    assert claimed_version == initial_version + 1

    # 2) Simulate a crash at the exact point commit would call settle()
    with patch.object(
        budget_manager.scripts, "settle", side_effect=Exception("POWER FAILURE")
    ):
        with pytest.raises(Exception, match="POWER FAILURE"):
            commit_finalize(
                run_id=run.run_id,
                tenant_id=run.tenant_id,
                finalize_token=finalize_token,
                claimed_version=claimed_version,
                charge_usd_micros=500_000,
                final_status="COMPLETED",
                extra_final_updates={
                    "result_bucket": "test-bucket",
                    "result_key": "test-key",
                    "result_sha256": "test-sha",
                },
                db=db_session,
                budget_manager=budget_manager,
            )

    # 3) Verdict: DB should still show CLAIMED-but-not-COMMITTED
    db_session.refresh(run)
    assert run.finalize_stage == "CLAIMED"
    assert run.finalize_token == finalize_token
    assert run.status == "PROCESSING"
    assert run.money_state == "RESERVED"

    # And the reservation should still exist (money has not moved)
    reservation = budget_manager.scripts.get_reservation(run.run_id)
    assert reservation is not None

    print("\n[PASS] Crash-after-claim leaves run in CLAIMED+RESERVED state for recovery.")


def test_chaos_race_condition_worker_vs_reaper(tmp_path, redis_client):
    """
    Scenario: Worker and Reaper try to finalize the SAME run at (nearly) the same time.
    Goal: Exactly one wins (CAS). Settlement must happen exactly once.
    """
    db_path = str(tmp_path / "chaos_race.db")
    SessionLocal = _new_sqlite_sessionmaker(db_path)

    # Create shared budget manager (only scripts are used, which are Redis-backed).
    bootstrap_db = SessionLocal()
    budget_manager = BudgetManager(redis_client, bootstrap_db)

    # Make the lease expired so the Reaper *would* see it in its scan.
    run = _create_processing_run(
        bootstrap_db,
        budget_manager,
        lease_expires_at=_utcnow() - timedelta(seconds=1),
        reserved_usd_micros=1_000_000,
        minimum_fee_usd_micros=50_000,
    )
    bootstrap_db.close()

    # Spy on settle() while still executing the real script.
    settle_calls = {"n": 0}
    settle_lock = threading.Lock()
    real_settle = budget_manager.scripts.settle

    def settle_spy(tenant_id: str, run_id: str, charge_usd_micros: int):
        with settle_lock:
            settle_calls["n"] += 1
        return real_settle(tenant_id, run_id, charge_usd_micros)

    budget_manager.scripts.settle = settle_spy  # type: ignore[assignment]

    barrier = threading.Barrier(2)

    def attempt_worker() -> str:
        db = SessionLocal()
        try:
            barrier.wait()
            finalize_success(
                run_id=run.run_id,
                tenant_id=run.tenant_id,
                lease_token=run.lease_token,
                actual_cost_usd_micros=500_000,
                result_bucket="bucket",
                result_key="key",
                result_sha256="sha",
                db=db,
                budget_manager=budget_manager,
            )
            return "WORKER_WINNER"
        except ClaimError:
            return "WORKER_LOSER"
        finally:
            db.close()

    def attempt_reaper() -> str:
        db = SessionLocal()
        try:
            barrier.wait()
            finalize_timeout(
                run_id=run.run_id,
                tenant_id=run.tenant_id,
                minimum_fee_usd_micros=50_000,
                db=db,
                budget_manager=budget_manager,
            )
            return "REAPER_WINNER"
        except ClaimError:
            return "REAPER_LOSER"
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=2) as ex:
        r_worker = ex.submit(attempt_worker).result()
        r_reaper = ex.submit(attempt_reaper).result()

    assert {r_worker, r_reaper} in (
        {"WORKER_WINNER", "REAPER_LOSER"},
        {"WORKER_LOSER", "REAPER_WINNER"},
    )

    # Settlement must happen exactly once.
    assert settle_calls["n"] == 1

    # Final DB state must be COMMITTED, with money SETTLED.
    verify_db = SessionLocal()
    updated = RunRepository(verify_db).get_by_id(run.run_id)
    assert updated is not None
    assert updated.finalize_stage == "COMMITTED"
    assert updated.money_state == "SETTLED"
    assert updated.status in ("COMPLETED", "FAILED")

    # Reservation must be gone after settlement.
    assert budget_manager.scripts.get_reservation(run.run_id) is None
    verify_db.close()

    print(f"\n[PASS] Race handled: {r_worker=} {r_reaper=} | settle_calls=1")


def test_chaos_crash_after_settle_before_db_commit_requires_reconcile(db_session, budget_manager):
    """
    Scenario: We crash AFTER Redis settle succeeds, but BEFORE the DB commit succeeds.
    MS-6: System should be able to reconcile this to a consistent COMMITTED+SETTLED state
    using idempotent finalize reconciliation, without double-charging.
    """
    run = _create_processing_run(db_session, budget_manager)

    finalize_token, claimed_version = claim_finalize(
        run_id=run.run_id,
        tenant_id=run.tenant_id,
        db=db_session,
        extra_claim_conditions={"lease_token": run.lease_token},
    )

    # Force DB commit failure after settlement.
    with patch.object(
        RunRepository, "update_with_version_check", side_effect=RuntimeError("DB DOWN")
    ):
        with pytest.raises(RuntimeError, match="DB DOWN"):
            commit_finalize(
                run_id=run.run_id,
                tenant_id=run.tenant_id,
                finalize_token=finalize_token,
                claimed_version=claimed_version,
                charge_usd_micros=500_000,
                final_status="COMPLETED",
                extra_final_updates={
                    "result_bucket": "bucket",
                    "result_key": "key",
                    "result_sha256": "sha",
                },
                db=db_session,
                budget_manager=budget_manager,
            )

    # At this point: Redis reservation is consumed...
    assert budget_manager.scripts.get_reservation(run.run_id) is None

    # ...but DB is still CLAIMED+RESERVED (inconsistent).
    db_session.refresh(run)
    assert run.finalize_stage == "CLAIMED"
    assert run.money_state == "RESERVED"

    # MS-6: Verify settlement receipt was created (PROOF of settlement!)
    receipt = budget_manager.scripts.get_settlement_receipt(run.run_id)
    assert receipt is not None, "Settlement receipt should exist (proof of settle success)"
    assert int(receipt["charged_usd_micros"]) == 500_000, "Receipt should have correct charge"
    assert receipt["tenant_id"] == run.tenant_id, "Receipt should have correct tenant_id"

    print(f"\n[MS-6] Settlement receipt verified: charged={receipt['charged_usd_micros']}")

    # Setup S3 result info (simulating Worker had uploaded before crash)
    run.result_bucket = "test-bucket"
    run.result_key = "test-results/test-key"
    run.finalize_claimed_at = _utcnow() - timedelta(minutes=10)  # Stuck >5min
    db_session.commit()
    db_session.refresh(run)

    # MS-6: Simulate Reconcile Loop detecting and fixing the stuck run
    # Import reconcile function from reaper (need to add reaper path)
    import os
    import sys
    REAPER_PATH = os.path.abspath(os.path.join(THIS_DIR, "..", "..", "reaper"))
    if REAPER_PATH not in sys.path:
        sys.path.insert(0, REAPER_PATH)

    from dpp_reaper.loops.reconcile_loop import reconcile_stuck_claimed_run

    # Mock S3 API to determine final status (COMPLETED vs FAILED)
    # NOTE: actual_cost comes from receipt, NOT S3!
    with patch("dpp_reaper.loops.reconcile_loop.check_s3_result_exists", return_value=True):
        # Run MS-6 receipt-based idempotent reconcile
        success = reconcile_stuck_claimed_run(run, db_session, budget_manager)
        assert success, "MS-6 reconcile should succeed"

    # VERIFY: DB should now be consistent (COMMITTED+SETTLED)
    db_session.refresh(run)
    assert run.finalize_stage == "COMMITTED"
    assert run.money_state == "SETTLED"
    assert run.status == "COMPLETED"
    assert run.actual_cost_usd_micros == 500_000  # From receipt (authoritative proof!)

    print("\n[PASS] MS-6: Receipt-based idempotent reconcile recovered stuck CLAIMED run.")
