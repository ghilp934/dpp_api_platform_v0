"""Tests for 2-phase finalize (DEC-4210)."""

import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

# Add API path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "api"))

from dpp_api.budget import BudgetManager
from dpp_api.db.models import Run
from dpp_api.db.repo_runs import RunRepository
from dpp_worker.finalize.optimistic_commit import (
    ClaimError,
    FinalizeError,
    finalize_failure,
    finalize_success,
)


def create_processing_run(
    db_session: Session, budget_manager: BudgetManager, reserved_usd_micros: int = 1_000_000
) -> tuple[Run, str]:
    """Create a run in PROCESSING state with lease.

    Returns:
        (run, lease_token)
    """
    tenant_id = f"tenant_{uuid.uuid4().hex[:8]}"
    run_id = str(uuid.uuid4())
    lease_token = str(uuid.uuid4())

    # Set budget
    budget_manager.set_balance(tenant_id, 10_000_000)  # $10.00

    # Create run
    run = Run(
        run_id=run_id,
        tenant_id=tenant_id,
        pack_type="decision",
        profile_version="v0.4.2.2",
        status="PROCESSING",
        money_state="RESERVED",
        payload_hash="test_hash",
        version=1,  # Already transitioned from QUEUED
        reservation_max_cost_usd_micros=reserved_usd_micros,
        minimum_fee_usd_micros=50_000,  # $0.05
        retention_until=datetime.now(timezone.utc) + timedelta(days=7),
        lease_token=lease_token,
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=120),
    )

    repo = RunRepository(db_session)
    repo.create(run)

    # Create Redis reservation
    budget_manager.scripts.reserve(tenant_id, run_id, reserved_usd_micros)

    return (run, lease_token)


def test_finalize_success_winner(
    db_session: Session, redis_client: Any, budget_manager: BudgetManager
):
    """Test successful finalize (winner)."""
    run, lease_token = create_processing_run(db_session, budget_manager)

    result = finalize_success(
        run_id=run.run_id,
        tenant_id=run.tenant_id,
        lease_token=lease_token,
        actual_cost_usd_micros=500_000,  # $0.50
        result_bucket="dpp-results",
        result_key="test/key",
        result_sha256="abc123",
        db=db_session,
        budget_manager=budget_manager,
    )

    assert result == "WINNER"

    # Verify final state
    repo = RunRepository(db_session)
    final_run = repo.get_by_id(run.run_id, run.tenant_id)

    assert final_run.status == "COMPLETED"
    assert final_run.money_state == "SETTLED"
    assert final_run.actual_cost_usd_micros == 500_000
    assert final_run.result_bucket == "dpp-results"
    assert final_run.result_key == "test/key"
    assert final_run.result_sha256 == "abc123"
    assert final_run.finalize_stage == "COMMITTED"
    assert final_run.version == 3  # 1 -> 2 (claim) -> 3 (commit)

    # Verify Redis reservation deleted
    reservation = budget_manager.scripts.get_reservation(run.run_id)
    assert reservation is None


def test_finalize_success_loser(
    db_session: Session, redis_client: Any, budget_manager: BudgetManager
):
    """Test finalize when another process already claimed (loser)."""
    run, lease_token = create_processing_run(db_session, budget_manager)

    # First finalize (winner)
    result1 = finalize_success(
        run_id=run.run_id,
        tenant_id=run.tenant_id,
        lease_token=lease_token,
        actual_cost_usd_micros=500_000,
        result_bucket="dpp-results",
        result_key="test/key1",
        result_sha256="abc123",
        db=db_session,
        budget_manager=budget_manager,
    )

    assert result1 == "WINNER"

    # Second finalize attempt (loser)
    with pytest.raises(ClaimError):
        finalize_success(
            run_id=run.run_id,
            tenant_id=run.tenant_id,
            lease_token=lease_token,
            actual_cost_usd_micros=600_000,
            result_bucket="dpp-results",
            result_key="test/key2",
            result_sha256="def456",
            db=db_session,
            budget_manager=budget_manager,
        )

    # Verify original finalize is preserved
    repo = RunRepository(db_session)
    final_run = repo.get_by_id(run.run_id, run.tenant_id)

    assert final_run.status == "COMPLETED"
    assert final_run.actual_cost_usd_micros == 500_000
    assert final_run.result_key == "test/key1"  # Not key2


def test_finalize_failure_winner(
    db_session: Session, redis_client: Any, budget_manager: BudgetManager
):
    """Test finalize for failed run."""
    run, lease_token = create_processing_run(db_session, budget_manager)

    result = finalize_failure(
        run_id=run.run_id,
        tenant_id=run.tenant_id,
        lease_token=lease_token,
        minimum_fee_usd_micros=run.minimum_fee_usd_micros,
        error_reason_code="PACK_EXECUTION_FAILED",
        error_detail="Test error",
        db=db_session,
        budget_manager=budget_manager,
    )

    assert result == "WINNER"

    # Verify final state
    repo = RunRepository(db_session)
    final_run = repo.get_by_id(run.run_id, run.tenant_id)

    assert final_run.status == "FAILED"
    assert final_run.money_state == "SETTLED"
    assert final_run.actual_cost_usd_micros == run.minimum_fee_usd_micros
    assert final_run.last_error_reason_code == "PACK_EXECUTION_FAILED"
    assert final_run.last_error_detail == "Test error"
    assert final_run.finalize_stage == "COMMITTED"


def test_finalize_prevents_overcharge(
    db_session: Session, redis_client: Any, budget_manager: BudgetManager
):
    """Test that finalize rejects actual_cost > reserved (DEC-4211)."""
    run, lease_token = create_processing_run(db_session, budget_manager, 1_000_000)

    with pytest.raises(FinalizeError, match="exceeds reserved"):
        finalize_success(
            run_id=run.run_id,
            tenant_id=run.tenant_id,
            lease_token=lease_token,
            actual_cost_usd_micros=2_000_000,  # More than reserved!
            result_bucket="dpp-results",
            result_key="test/key",
            result_sha256="abc123",
            db=db_session,
            budget_manager=budget_manager,
        )


# Note: Need to import 'Any' from typing
from typing import Any
