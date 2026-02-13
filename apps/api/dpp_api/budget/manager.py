"""Budget management for DPP runs.

Implements reserve-then-settle pattern:
1. Reserve: Lock maximum budget on submit (money_state = RESERVED)
2. Settle: Charge actual cost on completion (money_state = SETTLED)
3. Refund: Charge minimum fee on failure (money_state = REFUNDED)
"""

from typing import Optional

from sqlalchemy.orm import Session

from dpp_api.db.repo_runs import RunRepository
from dpp_api.utils.money import validate_usd_micros


class BudgetError(Exception):
    """Base exception for budget-related errors."""

    pass


class InsufficientBudgetError(BudgetError):
    """Raised when budget is insufficient for requested operation."""

    pass


class InvalidMoneyStateError(BudgetError):
    """Raised when money_state transition is invalid."""

    pass


class BudgetManager:
    """
    Manages budget reservations and settlements for runs.

    Follows DEC-4211: All money is stored as USD_MICROS (BIGINT).
    """

    def __init__(self, db: Session):
        self.db = db
        self.repo = RunRepository(db)

    def reserve(
        self,
        run_id: str,
        tenant_id: str,
        expected_version: int,
        max_cost_usd_micros: int,
    ) -> bool:
        """
        Reserve budget for a run.

        Validates amount and transitions money_state from NONE to RESERVED.

        Args:
            run_id: Run ID
            tenant_id: Tenant ID for ownership verification
            expected_version: Expected version for optimistic locking
            max_cost_usd_micros: Maximum cost to reserve

        Returns:
            True if reservation succeeded, False if version mismatch

        Raises:
            ValueError: If amount is invalid
            InvalidMoneyStateError: If money_state is not NONE
        """
        # Validate amount
        validate_usd_micros(max_cost_usd_micros)

        # Get current run to check money_state
        run = self.repo.get_by_id(run_id, tenant_id)
        if not run:
            raise BudgetError(f"Run {run_id} not found")

        if run.money_state != "NONE":
            raise InvalidMoneyStateError(
                f"Cannot reserve: money_state is {run.money_state}, expected NONE"
            )

        # Update with version check
        success = self.repo.update_with_version_check(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=expected_version,
            updates={
                "money_state": "RESERVED",
                "reservation_max_cost_usd_micros": max_cost_usd_micros,
            },
        )

        return success

    def settle(
        self,
        run_id: str,
        tenant_id: str,
        expected_version: int,
        actual_cost_usd_micros: int,
    ) -> bool:
        """
        Settle budget for a completed run.

        Charges actual cost and transitions money_state from RESERVED to SETTLED.

        Args:
            run_id: Run ID
            tenant_id: Tenant ID for ownership verification
            expected_version: Expected version for optimistic locking
            actual_cost_usd_micros: Actual cost to charge

        Returns:
            True if settlement succeeded, False if version mismatch

        Raises:
            ValueError: If amount is invalid or exceeds reservation
            InvalidMoneyStateError: If money_state is not RESERVED
        """
        # Validate amount
        validate_usd_micros(actual_cost_usd_micros)

        # Get current run to check money_state and reservation
        run = self.repo.get_by_id(run_id, tenant_id)
        if not run:
            raise BudgetError(f"Run {run_id} not found")

        if run.money_state != "RESERVED":
            raise InvalidMoneyStateError(
                f"Cannot settle: money_state is {run.money_state}, expected RESERVED"
            )

        if actual_cost_usd_micros > run.reservation_max_cost_usd_micros:
            raise BudgetError(
                f"Actual cost {actual_cost_usd_micros} exceeds reserved amount "
                f"{run.reservation_max_cost_usd_micros}"
            )

        # Update with version check
        success = self.repo.update_with_version_check(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=expected_version,
            updates={
                "money_state": "SETTLED",
                "actual_cost_usd_micros": actual_cost_usd_micros,
            },
        )

        return success

    def refund(
        self,
        run_id: str,
        tenant_id: str,
        expected_version: int,
        minimum_fee_usd_micros: int,
    ) -> bool:
        """
        Refund reservation and charge minimum fee for failed run.

        Transitions money_state from RESERVED to REFUNDED.

        Args:
            run_id: Run ID
            tenant_id: Tenant ID for ownership verification
            expected_version: Expected version for optimistic locking
            minimum_fee_usd_micros: Minimum fee to charge

        Returns:
            True if refund succeeded, False if version mismatch

        Raises:
            ValueError: If amount is invalid or exceeds reservation
            InvalidMoneyStateError: If money_state is not RESERVED
        """
        # Validate amount
        validate_usd_micros(minimum_fee_usd_micros)

        # Get current run to check money_state and reservation
        run = self.repo.get_by_id(run_id, tenant_id)
        if not run:
            raise BudgetError(f"Run {run_id} not found")

        if run.money_state != "RESERVED":
            raise InvalidMoneyStateError(
                f"Cannot refund: money_state is {run.money_state}, expected RESERVED"
            )

        if minimum_fee_usd_micros > run.reservation_max_cost_usd_micros:
            raise BudgetError(
                f"Minimum fee {minimum_fee_usd_micros} exceeds reserved amount "
                f"{run.reservation_max_cost_usd_micros}"
            )

        # Update with version check
        success = self.repo.update_with_version_check(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=expected_version,
            updates={
                "money_state": "REFUNDED",
                "actual_cost_usd_micros": minimum_fee_usd_micros,
            },
        )

        return success

    def get_budget_summary(
        self, run_id: str, tenant_id: str
    ) -> Optional[dict[str, int]]:
        """
        Get budget summary for a run.

        Args:
            run_id: Run ID
            tenant_id: Tenant ID for ownership verification

        Returns:
            Dictionary with budget information, or None if run not found
        """
        run = self.repo.get_by_id(run_id, tenant_id)
        if not run:
            return None

        return {
            "money_state": run.money_state,
            "reservation_max_cost_usd_micros": run.reservation_max_cost_usd_micros,
            "actual_cost_usd_micros": run.actual_cost_usd_micros,
            "minimum_fee_usd_micros": run.minimum_fee_usd_micros,
        }
