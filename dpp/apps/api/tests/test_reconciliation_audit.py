"""Tests for Reconciliation Audit (MS-6).

Validates that DB and Redis money totals match with 1 micros precision.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
import redis
from sqlalchemy.orm import Session

from dpp_api.budget.redis_scripts import BudgetScripts
from dpp_api.db.models import Run
from scripts.audit_reconciliation import (
    get_all_tenants,
    get_db_settled_total,
    get_redis_balances,
    get_redis_reserved_total,
)


def test_get_all_tenants(db_session: Session) -> None:
    """Test getting all tenant IDs from DB."""
    # Tenants are created by conftest fixtures
    tenant_ids = get_all_tenants(db_session)

    # Should have at least the test tenants
    assert len(tenant_ids) > 0
    assert all(isinstance(tid, str) for tid in tenant_ids)


def test_get_redis_balances(redis_client: redis.Redis, db_session: Session) -> None:
    """Test getting initial and current balances from Redis."""
    tenant_id = "tenant_balance_test"
    budget_scripts = BudgetScripts(redis_client)

    # Set initial and current balances
    budget_scripts.set_initial_balance(tenant_id, 5_000_000)  # $5.00
    budget_scripts.set_balance(tenant_id, 3_500_000)  # $3.50

    # Get balances
    balances = get_redis_balances(budget_scripts, [tenant_id])

    assert tenant_id in balances
    assert balances[tenant_id]["initial"] == 5_000_000
    assert balances[tenant_id]["current"] == 3_500_000


def test_get_redis_reserved_total(redis_client: redis.Redis) -> None:
    """Test calculating total reserved amount from Redis."""
    budget_scripts = BudgetScripts(redis_client)
    tenant_id = "tenant_reserve_test"

    # Set up budget
    budget_scripts.set_initial_balance(tenant_id, 10_000_000)  # $10.00
    budget_scripts.set_balance(tenant_id, 10_000_000)

    # Create 3 reservations
    run_id_1 = str(uuid.uuid4())
    run_id_2 = str(uuid.uuid4())
    run_id_3 = str(uuid.uuid4())

    budget_scripts.reserve(tenant_id, run_id_1, 1_000_000)  # $1.00
    budget_scripts.reserve(tenant_id, run_id_2, 2_000_000)  # $2.00
    budget_scripts.reserve(tenant_id, run_id_3, 500_000)    # $0.50

    # Get reserved total
    reserved_total, count = get_redis_reserved_total(budget_scripts)

    # Should include all 3 reservations
    assert reserved_total >= 3_500_000  # At least our 3
    assert count >= 3


def test_get_db_settled_total(db_session: Session) -> None:
    """Test calculating total settled amount from DB."""
    tenant_id = "tenant_settled_test"

    # Create settled runs
    run1 = Run(
        run_id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="COMPLETED",
        money_state="SETTLED",
        payload_hash="hash1",
        reservation_max_cost_usd_micros=2_000_000,
        actual_cost_usd_micros=1_500_000,  # $1.50
        minimum_fee_usd_micros=100_000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=30),
    )

    run2 = Run(
        run_id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="COMPLETED",
        money_state="SETTLED",
        payload_hash="hash2",
        reservation_max_cost_usd_micros=3_000_000,
        actual_cost_usd_micros=2_800_000,  # $2.80
        minimum_fee_usd_micros=100_000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=30),
    )

    # Create non-settled run (should be excluded)
    run3 = Run(
        run_id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        payload_hash="hash3",
        reservation_max_cost_usd_micros=1_000_000,
        actual_cost_usd_micros=None,  # Not settled yet
        minimum_fee_usd_micros=100_000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=30),
    )

    db_session.add_all([run1, run2, run3])
    db_session.commit()

    # Get settled total
    settled_total, count = get_db_settled_total(db_session)

    # Should include run1 + run2 only (excluding run3)
    assert settled_total >= 4_300_000  # $1.50 + $2.80 = $4.30
    assert count >= 2


def test_reconciliation_perfect_match(redis_client: redis.Redis, db_session: Session) -> None:
    """Test reconciliation audit with perfect match (MS-6 happy path)."""
    tenant_id = f"tenant_perfect_match_{uuid.uuid4().hex[:8]}"  # Unique tenant per run
    budget_scripts = BudgetScripts(redis_client)

    # Setup: Start with $10.00
    initial_balance = 10_000_000  # $10.00
    budget_scripts.set_initial_balance(tenant_id, initial_balance)
    budget_scripts.set_balance(tenant_id, initial_balance)

    # Scenario:
    # 1. Reserve $3.00 for run1
    run_id_1 = str(uuid.uuid4())
    status, new_balance = budget_scripts.reserve(tenant_id, run_id_1, 3_000_000)
    assert status == "OK"
    assert new_balance == 7_000_000  # $10 - $3 = $7

    # 2. Settle run1 with $2.50 actual cost (refund $0.50)
    status, charge, refund, new_balance = budget_scripts.settle(tenant_id, run_id_1, 2_500_000)
    assert status == "OK"
    assert charge == 2_500_000
    assert refund == 500_000
    assert new_balance == 7_500_000  # $7 + $0.50 = $7.50

    # 3. Create DB record
    run1 = Run(
        run_id=run_id_1,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="COMPLETED",
        money_state="SETTLED",
        payload_hash="hash_perfect",
        reservation_max_cost_usd_micros=3_000_000,
        actual_cost_usd_micros=2_500_000,
        minimum_fee_usd_micros=100_000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=30),
    )
    db_session.add(run1)
    db_session.commit()

    # Audit check for THIS TENANT ONLY
    balances = get_redis_balances(budget_scripts, [tenant_id])

    # Get reserved amount for this tenant's runs only
    reservation = budget_scripts.get_reservation(run_id_1)
    reserved_for_tenant = reservation["reserved_usd_micros"] if reservation else 0

    # Get settled amount for this tenant only
    from sqlalchemy import func, select
    stmt = select(func.sum(Run.actual_cost_usd_micros)).where(
        Run.money_state == "SETTLED",
        Run.tenant_id == tenant_id,
    )
    settled_for_tenant = int(db_session.execute(stmt).scalar() or 0)

    initial_total = balances[tenant_id]["initial"]
    current_total = balances[tenant_id]["current"]

    # MS-6: initial = current + reserved + settled
    expected_initial = current_total + reserved_for_tenant + settled_for_tenant
    discrepancy = initial_total - expected_initial

    # Should match perfectly (0 micros discrepancy)
    assert discrepancy == 0, f"Expected 0 discrepancy, got {discrepancy} micros (initial={initial_total}, current={current_total}, reserved={reserved_for_tenant}, settled={settled_for_tenant})"


def test_reconciliation_with_active_reservation(
    redis_client: redis.Redis, db_session: Session
) -> None:
    """Test reconciliation audit with active reservation still pending."""
    tenant_id = f"tenant_with_reservation_{uuid.uuid4().hex[:8]}"  # Unique tenant per run
    budget_scripts = BudgetScripts(redis_client)

    # Setup: Start with $20.00
    initial_balance = 20_000_000  # $20.00
    budget_scripts.set_initial_balance(tenant_id, initial_balance)
    budget_scripts.set_balance(tenant_id, initial_balance)

    # 1. Reserve and settle run1: $5.00 → $4.50
    run_id_1 = str(uuid.uuid4())
    budget_scripts.reserve(tenant_id, run_id_1, 5_000_000)
    budget_scripts.settle(tenant_id, run_id_1, 4_500_000)

    run1 = Run(
        run_id=run_id_1,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="COMPLETED",
        money_state="SETTLED",
        payload_hash="hash_settled",
        reservation_max_cost_usd_micros=5_000_000,
        actual_cost_usd_micros=4_500_000,
        minimum_fee_usd_micros=100_000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=30),
    )
    db_session.add(run1)
    db_session.commit()

    # 2. Reserve run2: $3.00 (still active)
    run_id_2 = str(uuid.uuid4())
    budget_scripts.reserve(tenant_id, run_id_2, 3_000_000)

    # Current state:
    # - Initial: $20.00
    # - Settled: $4.50 (run1)
    # - Reserved: $3.00 (run2)
    # - Current: $20 - $4.50 - $3.00 = $12.50

    balances = get_redis_balances(budget_scripts, [tenant_id])

    # Get reserved amount for this tenant's runs only
    reservation_1 = budget_scripts.get_reservation(run_id_1)  # Should be None (settled)
    reservation_2 = budget_scripts.get_reservation(run_id_2)  # Should exist
    reserved_for_tenant = (
        (reservation_1["reserved_usd_micros"] if reservation_1 else 0) +
        (reservation_2["reserved_usd_micros"] if reservation_2 else 0)
    )

    # Get settled amount for this tenant only
    from sqlalchemy import func, select
    stmt = select(func.sum(Run.actual_cost_usd_micros)).where(
        Run.money_state == "SETTLED",
        Run.tenant_id == tenant_id,
    )
    settled_for_tenant = int(db_session.execute(stmt).scalar() or 0)

    initial_total = balances[tenant_id]["initial"]
    current_total = balances[tenant_id]["current"]

    # MS-6: initial = current + reserved + settled
    expected_initial = current_total + reserved_for_tenant + settled_for_tenant
    discrepancy = initial_total - expected_initial

    # Should still match (0 micros discrepancy)
    assert discrepancy == 0, f"Expected 0 discrepancy with active reservation, got {discrepancy} micros (initial={initial_total}, current={current_total}, reserved={reserved_for_tenant}, settled={settled_for_tenant})"


def test_reconciliation_detects_discrepancy(redis_client: redis.Redis, db_session: Session) -> None:
    """Test that reconciliation audit detects money discrepancy (MS-6 failure case)."""
    tenant_id = f"tenant_discrepancy_{uuid.uuid4().hex[:8]}"  # Unique tenant per run
    budget_scripts = BudgetScripts(redis_client)

    # Setup: Start with $15.00
    initial_balance = 15_000_000  # $15.00
    budget_scripts.set_initial_balance(tenant_id, initial_balance)
    budget_scripts.set_balance(tenant_id, initial_balance)

    # Reserve and settle normally
    run_id = str(uuid.uuid4())
    budget_scripts.reserve(tenant_id, run_id, 2_000_000)
    budget_scripts.settle(tenant_id, run_id, 2_000_000)

    run = Run(
        run_id=run_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="COMPLETED",
        money_state="SETTLED",
        payload_hash="hash_discrep",
        reservation_max_cost_usd_micros=2_000_000,
        actual_cost_usd_micros=2_000_000,
        minimum_fee_usd_micros=100_000,
        retention_until=datetime.now(timezone.utc) + timedelta(days=30),
    )
    db_session.add(run)
    db_session.commit()

    # SIMULATE BUG: Manually add money to Redis (bypassing atomic operations)
    # This creates a discrepancy
    current_balance = budget_scripts.get_balance(tenant_id)
    budget_scripts.set_balance(tenant_id, current_balance + 1_000_000)  # Add $1.00 incorrectly

    # Audit check for THIS TENANT ONLY
    balances = get_redis_balances(budget_scripts, [tenant_id])

    # Get reserved amount for this tenant's runs only
    reservation = budget_scripts.get_reservation(run_id)
    reserved_for_tenant = reservation["reserved_usd_micros"] if reservation else 0

    # Get settled amount for this tenant only
    from sqlalchemy import func, select
    stmt = select(func.sum(Run.actual_cost_usd_micros)).where(
        Run.money_state == "SETTLED",
        Run.tenant_id == tenant_id,
    )
    settled_for_tenant = int(db_session.execute(stmt).scalar() or 0)

    initial_total = balances[tenant_id]["initial"]
    current_total = balances[tenant_id]["current"]

    expected_initial = current_total + reserved_for_tenant + settled_for_tenant
    discrepancy = initial_total - expected_initial

    # Should detect the $1.00 discrepancy
    assert discrepancy != 0, "Audit should detect money discrepancy"
    assert discrepancy == -1_000_000, f"Expected -$1.00 discrepancy, got {discrepancy} micros (initial={initial_total}, current={current_total}, reserved={reserved_for_tenant}, settled={settled_for_tenant})"
