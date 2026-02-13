"""Tests for BudgetManager with Redis Lua scripts."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
import redis
from sqlalchemy.orm import Session

from dpp_api.budget.manager import (
    AlreadyReservedError,
    BudgetError,
    BudgetManager,
    InsufficientBudgetError,
    InvalidMoneyStateError,
    NoReservationError,
)
from dpp_api.budget.redis_scripts import BudgetScripts
from dpp_api.db.models import Run
from dpp_api.utils.money import NegativeAmountError


@pytest.fixture
def budget_manager(db_session: Session, redis_client: redis.Redis) -> BudgetManager:
    """Create BudgetManager instance."""
    return BudgetManager(redis_client, db_session)


@pytest.fixture
def budget_scripts(redis_client: redis.Redis) -> BudgetScripts:
    """Create BudgetScripts instance."""
    return BudgetScripts(redis_client)


@pytest.fixture
def sample_run_for_budget(db_session: Session) -> Run:
    """Create a sample run for budget testing."""
    run = Run(
        run_id=str(uuid.uuid4()),
        tenant_id="tenant_budget_test",
        pack_type="urlpack",
        profile_version="v0.4.2.2",
        status="QUEUED",
        money_state="NONE",
        payload_hash="budget_test_hash",
        version=0,
        reservation_max_cost_usd_micros=0,
        minimum_fee_usd_micros=10_000,  # $0.01
        retention_until=datetime.now(timezone.utc) + timedelta(days=7),
    )
    db_session.add(run)
    db_session.commit()
    db_session.refresh(run)
    return run


def test_redis_reserve_lua(budget_scripts: BudgetScripts):
    """Test Redis Reserve.lua script directly."""
    tenant_id = "tenant_reserve_test"
    run_id = str(uuid.uuid4())

    # Set initial balance
    budget_scripts.set_balance(tenant_id, 5_000_000)  # $5.00

    # Reserve $2.00
    status, new_balance = budget_scripts.reserve(tenant_id, run_id, 2_000_000)

    assert status == "OK"
    assert new_balance == 3_000_000  # $5.00 - $2.00 = $3.00

    # Verify reservation exists
    reservation = budget_scripts.get_reservation(run_id)
    assert reservation is not None
    assert reservation["tenant_id"] == tenant_id
    assert reservation["reserved_usd_micros"] == 2_000_000


def test_redis_reserve_insufficient(budget_scripts: BudgetScripts):
    """Test Redis Reserve.lua with insufficient balance."""
    tenant_id = "tenant_insufficient"
    run_id = str(uuid.uuid4())

    # Set balance to $1.00
    budget_scripts.set_balance(tenant_id, 1_000_000)

    # Try to reserve $2.00 (insufficient)
    status, balance = budget_scripts.reserve(tenant_id, run_id, 2_000_000)

    assert status == "ERR_INSUFFICIENT"
    assert balance == 1_000_000  # Original balance unchanged


def test_redis_reserve_already_reserved(budget_scripts: BudgetScripts):
    """Test Redis Reserve.lua when already reserved."""
    tenant_id = "tenant_duplicate"
    run_id = str(uuid.uuid4())

    budget_scripts.set_balance(tenant_id, 5_000_000)

    # First reserve succeeds
    status1, _ = budget_scripts.reserve(tenant_id, run_id, 1_000_000)
    assert status1 == "OK"

    # Second reserve fails
    status2, _ = budget_scripts.reserve(tenant_id, run_id, 1_000_000)
    assert status2 == "ERR_ALREADY_RESERVED"


def test_redis_settle_lua(budget_scripts: BudgetScripts):
    """Test Redis Settle.lua script directly."""
    tenant_id = "tenant_settle"
    run_id = str(uuid.uuid4())

    # Reserve $2.00
    budget_scripts.set_balance(tenant_id, 5_000_000)
    budget_scripts.reserve(tenant_id, run_id, 2_000_000)

    # Settle with $1.50 actual cost
    status, charge, refund, new_balance = budget_scripts.settle(
        tenant_id, run_id, 1_500_000
    )

    assert status == "OK"
    assert charge == 1_500_000  # $1.50 charged
    assert refund == 500_000  # $0.50 refunded
    assert new_balance == 3_500_000  # $3.00 + $0.50 = $3.50

    # Reservation should be deleted
    assert budget_scripts.get_reservation(run_id) is None


def test_redis_settle_no_reserve(budget_scripts: BudgetScripts):
    """Test Redis Settle.lua without reservation."""
    tenant_id = "tenant_no_reserve"
    run_id = str(uuid.uuid4())

    status, charge, refund, new_balance = budget_scripts.settle(
        tenant_id, run_id, 1_000_000
    )

    assert status == "ERR_NO_RESERVE"


def test_redis_refund_full_lua(budget_scripts: BudgetScripts):
    """Test Redis RefundFull.lua script directly."""
    tenant_id = "tenant_refund"
    run_id = str(uuid.uuid4())

    # Reserve $2.00
    budget_scripts.set_balance(tenant_id, 5_000_000)
    budget_scripts.reserve(tenant_id, run_id, 2_000_000)

    # Full refund
    status, refund, new_balance = budget_scripts.refund_full(tenant_id, run_id)

    assert status == "OK"
    assert refund == 2_000_000  # $2.00 refunded
    assert new_balance == 5_000_000  # Back to $5.00

    # Reservation should be deleted
    assert budget_scripts.get_reservation(run_id) is None


def test_reserve_success(budget_manager: BudgetManager, sample_run_for_budget: Run):
    """Test successful budget reservation."""
    max_cost = 1_500_000  # $1.50

    # Set tenant balance
    budget_manager.set_balance(sample_run_for_budget.tenant_id, 5_000_000)

    success = budget_manager.reserve(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=0,
        max_cost_usd_micros=max_cost,
    )

    assert success is True

    # Verify DB state
    summary = budget_manager.get_budget_summary(
        sample_run_for_budget.run_id, sample_run_for_budget.tenant_id
    )
    assert summary["money_state"] == "RESERVED"
    assert summary["reservation_max_cost_usd_micros"] == max_cost
    assert summary["redis_reservation_exists"] is True


def test_reserve_insufficient_budget(
    budget_manager: BudgetManager, sample_run_for_budget: Run
):
    """Test reservation with insufficient budget."""
    # Set balance to $1.00
    budget_manager.set_balance(sample_run_for_budget.tenant_id, 1_000_000)

    # Try to reserve $2.00
    with pytest.raises(InsufficientBudgetError, match="Insufficient budget"):
        budget_manager.reserve(
            run_id=sample_run_for_budget.run_id,
            tenant_id=sample_run_for_budget.tenant_id,
            expected_version=0,
            max_cost_usd_micros=2_000_000,
        )


def test_reserve_invalid_amount(
    budget_manager: BudgetManager, sample_run_for_budget: Run
):
    """Test reservation with invalid amount."""
    # Negative amount
    with pytest.raises(NegativeAmountError):
        budget_manager.reserve(
            run_id=sample_run_for_budget.run_id,
            tenant_id=sample_run_for_budget.tenant_id,
            expected_version=0,
            max_cost_usd_micros=-1000,
        )


def test_reserve_wrong_money_state(
    budget_manager: BudgetManager, sample_run_for_budget: Run
):
    """Test reservation when money_state is not NONE."""
    budget_manager.set_balance(sample_run_for_budget.tenant_id, 5_000_000)

    # First reservation succeeds
    budget_manager.reserve(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=0,
        max_cost_usd_micros=1_000_000,
    )

    # Second reservation fails (money_state is now RESERVED)
    with pytest.raises(InvalidMoneyStateError, match="expected NONE"):
        budget_manager.reserve(
            run_id=sample_run_for_budget.run_id,
            tenant_id=sample_run_for_budget.tenant_id,
            expected_version=1,
            max_cost_usd_micros=2_000_000,
        )


def test_settle_success(budget_manager: BudgetManager, sample_run_for_budget: Run):
    """Test successful budget settlement."""
    max_cost = 2_000_000  # $2.00
    actual_cost = 1_500_000  # $1.50

    budget_manager.set_balance(sample_run_for_budget.tenant_id, 5_000_000)

    # Reserve first
    budget_manager.reserve(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=0,
        max_cost_usd_micros=max_cost,
    )

    # Settle
    success = budget_manager.settle(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=1,
        actual_cost_usd_micros=actual_cost,
    )

    assert success is True

    # Verify DB state
    summary = budget_manager.get_budget_summary(
        sample_run_for_budget.run_id, sample_run_for_budget.tenant_id
    )
    assert summary["money_state"] == "SETTLED"
    assert summary["actual_cost_usd_micros"] == actual_cost
    assert summary["redis_reservation_exists"] is False  # Deleted after settle

    # Verify Redis balance
    balance = budget_manager.get_balance(sample_run_for_budget.tenant_id)
    assert balance == 3_500_000  # $3.00 + $0.50 refund = $3.50


def test_settle_exceeds_reservation(
    budget_manager: BudgetManager, sample_run_for_budget: Run
):
    """Test settlement with cost exceeding reservation."""
    max_cost = 1_000_000  # $1.00
    actual_cost = 1_500_000  # $1.50 (exceeds)

    budget_manager.set_balance(sample_run_for_budget.tenant_id, 5_000_000)

    # Reserve first
    budget_manager.reserve(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=0,
        max_cost_usd_micros=max_cost,
    )

    # Settle should fail
    with pytest.raises(BudgetError, match="exceeds reserved amount"):
        budget_manager.settle(
            run_id=sample_run_for_budget.run_id,
            tenant_id=sample_run_for_budget.tenant_id,
            expected_version=1,
            actual_cost_usd_micros=actual_cost,
        )


def test_settle_wrong_money_state(
    budget_manager: BudgetManager, sample_run_for_budget: Run
):
    """Test settlement when money_state is not RESERVED."""
    # Try to settle without reserving first
    with pytest.raises(InvalidMoneyStateError, match="expected RESERVED"):
        budget_manager.settle(
            run_id=sample_run_for_budget.run_id,
            tenant_id=sample_run_for_budget.tenant_id,
            expected_version=0,
            actual_cost_usd_micros=1_000_000,
        )


def test_refund_success(budget_manager: BudgetManager, sample_run_for_budget: Run):
    """Test successful budget refund."""
    max_cost = 2_000_000  # $2.00
    minimum_fee = 10_000  # $0.01

    budget_manager.set_balance(sample_run_for_budget.tenant_id, 5_000_000)

    # Reserve first
    budget_manager.reserve(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=0,
        max_cost_usd_micros=max_cost,
    )

    # Refund (e.g., job failed)
    success = budget_manager.refund(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=1,
        minimum_fee_usd_micros=minimum_fee,
    )

    assert success is True

    # Verify DB state
    summary = budget_manager.get_budget_summary(
        sample_run_for_budget.run_id, sample_run_for_budget.tenant_id
    )
    assert summary["money_state"] == "REFUNDED"
    assert summary["actual_cost_usd_micros"] == minimum_fee
    assert summary["redis_reservation_exists"] is False

    # Verify Redis balance
    balance = budget_manager.get_balance(sample_run_for_budget.tenant_id)
    assert balance == 4_990_000  # $3.00 + $1.99 refund = $4.99


def test_version_check_on_reserve(
    budget_manager: BudgetManager, sample_run_for_budget: Run
):
    """Test optimistic locking with wrong version (DEC-4210)."""
    budget_manager.set_balance(sample_run_for_budget.tenant_id, 5_000_000)

    # Reserve with correct version succeeds
    success1 = budget_manager.reserve(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=0,
        max_cost_usd_micros=2_000_000,
    )
    assert success1 is True

    # Settle with wrong version fails (race loser)
    success2 = budget_manager.settle(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=0,  # Wrong version (should be 1 after reserve)
        actual_cost_usd_micros=1_500_000,
    )
    assert success2 is False

    # Verify state is still RESERVED (settle didn't happen)
    summary = budget_manager.get_budget_summary(
        sample_run_for_budget.run_id, sample_run_for_budget.tenant_id
    )
    assert summary["money_state"] == "RESERVED"
    assert summary["actual_cost_usd_micros"] is None


def test_get_budget_summary(
    budget_manager: BudgetManager, sample_run_for_budget: Run
):
    """Test getting budget summary."""
    # Initial state
    summary = budget_manager.get_budget_summary(
        sample_run_for_budget.run_id, sample_run_for_budget.tenant_id
    )
    assert summary["money_state"] == "NONE"
    assert summary["redis_reservation_exists"] is False

    # After reservation
    budget_manager.set_balance(sample_run_for_budget.tenant_id, 5_000_000)
    budget_manager.reserve(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=0,
        max_cost_usd_micros=1_500_000,
    )

    summary = budget_manager.get_budget_summary(
        sample_run_for_budget.run_id, sample_run_for_budget.tenant_id
    )
    assert summary["money_state"] == "RESERVED"
    assert summary["reservation_max_cost_usd_micros"] == 1_500_000
    assert summary["redis_reservation_exists"] is True
    assert summary["redis_reserved_amount"] == 1_500_000


def test_get_budget_summary_not_found(budget_manager: BudgetManager):
    """Test getting budget summary for non-existent run."""
    summary = budget_manager.get_budget_summary("nonexistent", "tenant_test")
    assert summary is None


def test_full_reserve_settle_flow(
    budget_manager: BudgetManager, sample_run_for_budget: Run
):
    """Test complete reserve → settle flow."""
    max_cost = 2_000_000  # $2.00
    actual_cost = 1_500_000  # $1.50

    budget_manager.set_balance(sample_run_for_budget.tenant_id, 5_000_000)

    # Step 1: Reserve
    budget_manager.reserve(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=0,
        max_cost_usd_micros=max_cost,
    )

    summary = budget_manager.get_budget_summary(
        sample_run_for_budget.run_id, sample_run_for_budget.tenant_id
    )
    assert summary["money_state"] == "RESERVED"

    # Step 2: Settle
    budget_manager.settle(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=1,
        actual_cost_usd_micros=actual_cost,
    )

    summary = budget_manager.get_budget_summary(
        sample_run_for_budget.run_id, sample_run_for_budget.tenant_id
    )
    assert summary["money_state"] == "SETTLED"
    assert summary["actual_cost_usd_micros"] == actual_cost


def test_full_reserve_refund_flow(
    budget_manager: BudgetManager, sample_run_for_budget: Run
):
    """Test complete reserve → refund flow (failure case)."""
    max_cost = 2_000_000  # $2.00
    minimum_fee = 10_000  # $0.01

    budget_manager.set_balance(sample_run_for_budget.tenant_id, 5_000_000)

    # Step 1: Reserve
    budget_manager.reserve(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=0,
        max_cost_usd_micros=max_cost,
    )

    # Step 2: Refund (job failed)
    budget_manager.refund(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=1,
        minimum_fee_usd_micros=minimum_fee,
    )

    summary = budget_manager.get_budget_summary(
        sample_run_for_budget.run_id, sample_run_for_budget.tenant_id
    )
    assert summary["money_state"] == "REFUNDED"
    assert summary["actual_cost_usd_micros"] == minimum_fee


def test_settle_overcharge_attempt(budget_scripts: BudgetScripts):
    """
    CRITICAL: Test that Settle.lua caps charge at reserved amount.

    This prevents an attacker from charging more than the reservation,
    which could drain user balance into negative.
    """
    tenant_id = "tenant_overcharge"
    run_id = str(uuid.uuid4())

    # Set balance to $5.00
    budget_scripts.set_balance(tenant_id, 5_000_000)

    # Reserve $2.00
    status, _ = budget_scripts.reserve(tenant_id, run_id, 2_000_000)
    assert status == "OK"

    # ATTACK: Try to charge $100.00 (way more than reserved)
    status, charge, refund, new_balance = budget_scripts.settle(
        tenant_id, run_id, 100_000_000  # $100.00 attack!
    )

    assert status == "OK"
    # CRITICAL: Charge should be capped at reserved amount ($2.00)
    assert charge == 2_000_000  # Not $100!
    assert refund == 0  # No refund (all reserved was used)
    assert new_balance == 3_000_000  # $5.00 - $2.00 = $3.00

    # Verify balance is correct (not negative!)
    final_balance = budget_scripts.get_balance(tenant_id)
    assert final_balance == 3_000_000
    assert final_balance >= 0  # NEVER negative


def test_settle_negative_charge_attempt(budget_scripts: BudgetScripts):
    """
    CRITICAL: Test that Settle.lua rejects negative charge.

    This prevents an attacker from using negative charge to add money.
    """
    tenant_id = "tenant_negative"
    run_id = str(uuid.uuid4())

    # Set balance to $5.00
    budget_scripts.set_balance(tenant_id, 5_000_000)

    # Reserve $2.00
    budget_scripts.reserve(tenant_id, run_id, 2_000_000)

    # ATTACK: Try to charge -$1.00 (negative!)
    status, charge, refund, new_balance = budget_scripts.settle(
        tenant_id, run_id, -1_000_000  # Negative attack!
    )

    assert status == "OK"
    # CRITICAL: Negative charge should be treated as 0
    assert charge == 0
    assert refund == 2_000_000  # Full refund (no charge)
    # Balance should be $5.00 + $2.00 refund = $7.00? No!
    # Actually it should just return the reserved amount
    assert new_balance == 5_000_000  # Back to original
