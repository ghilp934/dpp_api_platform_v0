"""Budget management for DPP runs using Redis Lua scripts.

Implements reserve-then-settle pattern with atomic Redis operations:
1. Reserve: Lock maximum budget on submit (budget key update + reserve key create)
2. Settle: Charge actual cost on completion (delete reserve + refund to budget)
3. RefundFull: Full refund on failure/timeout

All operations use Redis Lua scripts for atomicity (DEC-4203).
Money is always stored as USD_MICROS (BIGINT) per DEC-4211.

After Redis operations, caller must update DB money_state accordingly.
"""

from typing import Optional

import redis
from sqlalchemy.orm import Session

from dpp_api.budget.redis_scripts import BudgetScripts
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


class AlreadyReservedError(BudgetError):
    """Raised when run already has a reservation."""

    pass


class NoReservationError(BudgetError):
    """Raised when reservation is not found."""

    pass


class BudgetManager:
    """
    Manages budget reservations and settlements using Redis.

    Follows DEC-4211: All money is stored as USD_MICROS (BIGINT).
    Follows DEC-4203: Reserve-then-settle pattern.

    Redis keys:
    - budget:{tenant_id}:balance_usd_micros - Current balance (string int)
    - reserve:{run_id} - Reservation (hash: tenant_id, reserved_usd_micros, created_at_ms)
    """

    def __init__(self, redis_client: redis.Redis, db: Session):
        self.redis = redis_client
        self.db = db
        self.scripts = BudgetScripts(redis_client)
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

        Steps:
        1. Validate amount
        2. Check DB money_state = NONE
        3. Call Redis Reserve.lua (atomic budget check + reserve)
        4. Update DB money_state = RESERVED with version check

        Args:
            run_id: Run ID
            tenant_id: Tenant ID for ownership verification
            expected_version: Expected version for optimistic locking
            max_cost_usd_micros: Maximum cost to reserve

        Returns:
            True if reservation succeeded, False if version mismatch

        Raises:
            ValueError: If amount is invalid
            InsufficientBudgetError: If budget is insufficient
            AlreadyReservedError: If run already has a reservation
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

        # Call Redis Reserve.lua
        status, balance_or_error = self.scripts.reserve(
            tenant_id, run_id, max_cost_usd_micros
        )

        if status == "ERR_INSUFFICIENT":
            raise InsufficientBudgetError(
                f"Insufficient budget: requested {max_cost_usd_micros}, "
                f"available {balance_or_error}"
            )
        elif status == "ERR_ALREADY_RESERVED":
            raise AlreadyReservedError(f"Run {run_id} already has a reservation")

        # Update DB with version check
        success = self.repo.update_with_version_check(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=expected_version,
            updates={
                "money_state": "RESERVED",
                "reservation_max_cost_usd_micros": max_cost_usd_micros,
            },
        )

        if not success:
            # Version mismatch - rollback Redis reservation
            self.scripts.refund_full(tenant_id, run_id)
            return False

        return True

    def settle(
        self,
        run_id: str,
        tenant_id: str,
        expected_version: int,
        actual_cost_usd_micros: int,
    ) -> bool:
        """
        Settle budget for a completed run.

        Steps:
        1. Validate amount
        2. Check DB money_state = RESERVED
        3. Call Redis Settle.lua (charge actual + refund excess + delete reserve)
        4. Update DB money_state = SETTLED with version check

        Args:
            run_id: Run ID
            tenant_id: Tenant ID for ownership verification
            expected_version: Expected version for optimistic locking
            actual_cost_usd_micros: Actual cost to charge

        Returns:
            True if settlement succeeded, False if version mismatch

        Raises:
            ValueError: If amount is invalid
            InvalidMoneyStateError: If money_state is not RESERVED
            NoReservationError: If reservation is not found
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

        # Validate actual cost doesn't exceed reservation (sanity check)
        if actual_cost_usd_micros > run.reservation_max_cost_usd_micros:
            raise BudgetError(
                f"Actual cost {actual_cost_usd_micros} exceeds reserved amount "
                f"{run.reservation_max_cost_usd_micros}"
            )

        # Call Redis Settle.lua
        status, charge, refund, new_balance = self.scripts.settle(
            tenant_id, run_id, actual_cost_usd_micros
        )

        if status == "ERR_NO_RESERVE":
            raise NoReservationError(f"No reservation found for run {run_id}")

        # Update DB with version check
        success = self.repo.update_with_version_check(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=expected_version,
            updates={
                "money_state": "SETTLED",
                "actual_cost_usd_micros": actual_cost_usd_micros,
            },
        )

        # Note: If version mismatch, Redis has already settled but DB hasn't.
        # This is acceptable - Redis is authoritative for budget balance.
        # Reconciliation job can fix DB state later if needed.

        return success

    def refund(
        self,
        run_id: str,
        tenant_id: str,
        expected_version: int,
        minimum_fee_usd_micros: int,
    ) -> bool:
        """
        Charge minimum fee and refund the rest for failed run.

        Steps:
        1. Validate amount
        2. Check DB money_state = RESERVED
        3. Call Redis Settle.lua with minimum_fee (charges fee + refunds rest)
        4. Update DB money_state = REFUNDED with version check

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
            NoReservationError: If reservation is not found
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

        # Call Redis Settle.lua (same as settle, but with minimum_fee as charge)
        status, charge, refund, new_balance = self.scripts.settle(
            tenant_id, run_id, minimum_fee_usd_micros
        )

        if status == "ERR_NO_RESERVE":
            raise NoReservationError(f"No reservation found for run {run_id}")

        # Update DB with version check
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

    def get_balance(self, tenant_id: str) -> int:
        """
        Get current budget balance for tenant.

        Args:
            tenant_id: Tenant ID

        Returns:
            Current balance in USD_MICROS
        """
        return self.scripts.get_balance(tenant_id)

    def set_balance(self, tenant_id: str, balance_usd_micros: int) -> None:
        """
        Set budget balance for tenant (admin/testing only).

        Args:
            tenant_id: Tenant ID
            balance_usd_micros: Balance to set in USD_MICROS
        """
        validate_usd_micros(balance_usd_micros)
        self.scripts.set_balance(tenant_id, balance_usd_micros)

    def get_budget_summary(
        self, run_id: str, tenant_id: str
    ) -> Optional[dict[str, int]]:
        """
        Get budget summary for a run (combines Redis + DB state).

        Args:
            run_id: Run ID
            tenant_id: Tenant ID for ownership verification

        Returns:
            Dictionary with budget information, or None if run not found
        """
        run = self.repo.get_by_id(run_id, tenant_id)
        if not run:
            return None

        # Get reservation from Redis (if exists)
        reservation = self.scripts.get_reservation(run_id)

        return {
            "money_state": run.money_state,
            "reservation_max_cost_usd_micros": run.reservation_max_cost_usd_micros,
            "actual_cost_usd_micros": run.actual_cost_usd_micros,
            "minimum_fee_usd_micros": run.minimum_fee_usd_micros,
            "redis_reservation_exists": reservation is not None,
            "redis_reserved_amount": (
                reservation["reserved_usd_micros"] if reservation else None
            ),
        }
