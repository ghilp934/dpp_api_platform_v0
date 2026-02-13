"""Tests for BudgetManager."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.orm import Session

from dpp_api.budget.manager import (
    BudgetError,
    BudgetManager,
    InvalidMoneyStateError,
)
from dpp_api.db.models import Run
from dpp_api.utils.money import NegativeAmountError


@pytest.fixture
def budget_manager(db_session: Session) -> BudgetManager:
    """Create BudgetManager instance."""
    return BudgetManager(db_session)


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


def test_reserve_success(budget_manager: BudgetManager, sample_run_for_budget: Run):
    """Test successful budget reservation."""
    max_cost = 1_500_000  # $1.50

    success = budget_manager.reserve(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=0,
        max_cost_usd_micros=max_cost,
    )

    assert success is True

    # Verify state
    summary = budget_manager.get_budget_summary(
        sample_run_for_budget.run_id, sample_run_for_budget.tenant_id
    )
    assert summary["money_state"] == "RESERVED"
    assert summary["reservation_max_cost_usd_micros"] == max_cost


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

    # Verify state
    summary = budget_manager.get_budget_summary(
        sample_run_for_budget.run_id, sample_run_for_budget.tenant_id
    )
    assert summary["money_state"] == "SETTLED"
    assert summary["actual_cost_usd_micros"] == actual_cost


def test_settle_exceeds_reservation(
    budget_manager: BudgetManager, sample_run_for_budget: Run
):
    """Test settlement with cost exceeding reservation."""
    max_cost = 1_000_000  # $1.00
    actual_cost = 1_500_000  # $1.50 (exceeds)

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

    # Verify state
    summary = budget_manager.get_budget_summary(
        sample_run_for_budget.run_id, sample_run_for_budget.tenant_id
    )
    assert summary["money_state"] == "REFUNDED"
    assert summary["actual_cost_usd_micros"] == minimum_fee


def test_refund_exceeds_reservation(
    budget_manager: BudgetManager, sample_run_for_budget: Run
):
    """Test refund with fee exceeding reservation."""
    max_cost = 10_000  # $0.01
    minimum_fee = 20_000  # $0.02 (exceeds)

    # Reserve first
    budget_manager.reserve(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=0,
        max_cost_usd_micros=max_cost,
    )

    # Refund should fail
    with pytest.raises(BudgetError, match="exceeds reserved amount"):
        budget_manager.refund(
            run_id=sample_run_for_budget.run_id,
            tenant_id=sample_run_for_budget.tenant_id,
            expected_version=1,
            minimum_fee_usd_micros=minimum_fee,
        )


def test_refund_wrong_money_state(
    budget_manager: BudgetManager, sample_run_for_budget: Run
):
    """Test refund when money_state is not RESERVED."""
    # Try to refund without reserving first
    with pytest.raises(InvalidMoneyStateError, match="expected RESERVED"):
        budget_manager.refund(
            run_id=sample_run_for_budget.run_id,
            tenant_id=sample_run_for_budget.tenant_id,
            expected_version=0,
            minimum_fee_usd_micros=10_000,
        )


def test_version_check_on_reserve(
    budget_manager: BudgetManager, sample_run_for_budget: Run
):
    """Test optimistic locking with wrong version (DEC-4210)."""
    # Reserve with correct version succeeds
    success1 = budget_manager.reserve(
        run_id=sample_run_for_budget.run_id,
        tenant_id=sample_run_for_budget.tenant_id,
        expected_version=0,
        max_cost_usd_micros=2_000_000,
    )
    assert success1 is True

    # Settle with wrong version fails (race loser)
    # Note: This simulates a race condition where another process
    # already updated the run, incrementing the version
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
    assert summary["reservation_max_cost_usd_micros"] == 0

    # After reservation
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
