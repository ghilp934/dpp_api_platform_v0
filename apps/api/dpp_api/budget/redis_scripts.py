"""Redis Lua scripts for atomic budget operations.

All budget operations use Lua scripts to ensure atomicity and avoid race conditions.
Scripts follow DEC-4211: all money values are USD_MICROS (integers).
"""

import time
from typing import Literal, Optional

import redis

# Key naming conventions (locked spec)
# - budget:{tenant_id}:balance_usd_micros (string int)
# - reserve:{run_id} (hash: reserved_usd_micros, tenant_id, created_at_ms) TTL=3600s


# Reserve.lua - Atomically reserve budget
RESERVE_LUA = """
local budget_key = KEYS[1]
local reserve_key = KEYS[2]
local tenant_id = ARGV[1]
local reserved = tonumber(ARGV[2])
local created_at_ms = ARGV[3]

if redis.call("EXISTS", reserve_key) == 1 then
  return {"ERR_ALREADY_RESERVED"}
end

local bal = tonumber(redis.call("GET", budget_key) or "0")
if bal < reserved then
  return {"ERR_INSUFFICIENT", tostring(bal)}
end

redis.call("SET", budget_key, tostring(bal - reserved))
redis.call("HSET", reserve_key,
  "tenant_id", tenant_id,
  "reserved_usd_micros", tostring(reserved),
  "created_at_ms", created_at_ms
)
return {"OK", tostring(bal - reserved)}
"""

# Settle.lua - Settle reservation and return refund
# CRITICAL: Prevents overcharge attacks and negative balance
SETTLE_LUA = """
local budget_key = KEYS[1]
local reserve_key = KEYS[2]
local charge = tonumber(ARGV[1])

if redis.call("EXISTS", reserve_key) ~= 1 then
  return {"ERR_NO_RESERVE"}
end

local reserved = tonumber(redis.call("HGET", reserve_key, "reserved_usd_micros") or "0")

-- CRITICAL: Prevent negative charge (attack vector)
if charge < 0 then
  charge = 0
end

-- CRITICAL: Cap charge at reserved amount (prevent overcharge)
if charge > reserved then
  charge = reserved
end

local refund = reserved - charge

local bal = tonumber(redis.call("GET", budget_key) or "0")
bal = bal + refund

-- CRITICAL: Final sanity check - balance should never go negative
if bal < 0 then
  bal = 0
end

redis.call("SET", budget_key, tostring(bal))
redis.call("DEL", reserve_key)
return {"OK", tostring(charge), tostring(refund), tostring(bal)}
"""

# RefundFull.lua - Refund entire reservation
REFUND_FULL_LUA = """
local budget_key = KEYS[1]
local reserve_key = KEYS[2]

if redis.call("EXISTS", reserve_key) ~= 1 then
  return {"ERR_NO_RESERVE"}
end

local reserved = tonumber(redis.call("HGET", reserve_key, "reserved_usd_micros") or "0")
local bal = tonumber(redis.call("GET", budget_key) or "0")
bal = bal + reserved

redis.call("SET", budget_key, tostring(bal))
redis.call("DEL", reserve_key)
return {"OK", tostring(reserved), tostring(bal)}
"""


class BudgetScripts:
    """Redis Lua scripts for budget operations."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        # Pre-load scripts using SCRIPT LOAD for efficiency
        self.reserve_sha: Optional[str] = None
        self.settle_sha: Optional[str] = None
        self.refund_full_sha: Optional[str] = None
        self._load_scripts()

    def _load_scripts(self) -> None:
        """Load Lua scripts into Redis."""
        self.reserve_sha = self.redis.script_load(RESERVE_LUA)
        self.settle_sha = self.redis.script_load(SETTLE_LUA)
        self.refund_full_sha = self.redis.script_load(REFUND_FULL_LUA)

    @staticmethod
    def budget_key(tenant_id: str) -> str:
        """Generate budget key."""
        return f"budget:{tenant_id}:balance_usd_micros"

    @staticmethod
    def reserve_key(run_id: str) -> str:
        """Generate reserve key."""
        return f"reserve:{run_id}"

    def reserve(
        self, tenant_id: str, run_id: str, reserved_usd_micros: int
    ) -> tuple[Literal["OK", "ERR_ALREADY_RESERVED", "ERR_INSUFFICIENT"], int]:
        """
        Atomically reserve budget.

        Args:
            tenant_id: Tenant ID
            run_id: Run ID
            reserved_usd_micros: Amount to reserve in USD_MICROS

        Returns:
            Tuple of (status, new_balance or current_balance)
            - ("OK", new_balance) - Success
            - ("ERR_ALREADY_RESERVED", 0) - Already reserved
            - ("ERR_INSUFFICIENT", current_balance) - Insufficient budget
        """
        budget_key = self.budget_key(tenant_id)
        reserve_key = self.reserve_key(run_id)
        created_at_ms = int(time.time() * 1000)

        result = self.redis.evalsha(
            self.reserve_sha,
            2,  # num keys
            budget_key,
            reserve_key,
            tenant_id,
            str(reserved_usd_micros),
            str(created_at_ms),
        )

        status = result[0]
        if status == "OK":
            new_balance = int(result[1])
            # Set TTL on reserve key (3600s = 1 hour)
            self.redis.expire(reserve_key, 3600)
            return ("OK", new_balance)
        elif status == "ERR_INSUFFICIENT":
            current_balance = int(result[1])
            return ("ERR_INSUFFICIENT", current_balance)
        else:  # ERR_ALREADY_RESERVED
            return ("ERR_ALREADY_RESERVED", 0)

    def settle(
        self, tenant_id: str, run_id: str, charge_usd_micros: int
    ) -> tuple[
        Literal["OK", "ERR_NO_RESERVE"], int, int, int
    ]:  # (status, charge, refund, new_balance)
        """
        Settle reservation with actual charge.

        Args:
            tenant_id: Tenant ID
            run_id: Run ID
            charge_usd_micros: Actual amount to charge (success=actual_cost, fail=minimum_fee)

        Returns:
            Tuple of (status, charge, refund, new_balance)
            - ("OK", charge, refund, new_balance) - Success
            - ("ERR_NO_RESERVE", 0, 0, 0) - No reservation found
        """
        budget_key = self.budget_key(tenant_id)
        reserve_key = self.reserve_key(run_id)

        result = self.redis.evalsha(
            self.settle_sha,
            2,  # num keys
            budget_key,
            reserve_key,
            str(charge_usd_micros),
        )

        status = result[0]
        if status == "OK":
            charge = int(result[1])
            refund = int(result[2])
            new_balance = int(result[3])
            return ("OK", charge, refund, new_balance)
        else:  # ERR_NO_RESERVE
            return ("ERR_NO_RESERVE", 0, 0, 0)

    def refund_full(
        self, tenant_id: str, run_id: str
    ) -> tuple[Literal["OK", "ERR_NO_RESERVE"], int, int]:  # (status, refund, new_balance)
        """
        Refund entire reservation.

        Args:
            tenant_id: Tenant ID
            run_id: Run ID

        Returns:
            Tuple of (status, refund, new_balance)
            - ("OK", refund, new_balance) - Success
            - ("ERR_NO_RESERVE", 0, 0) - No reservation found
        """
        budget_key = self.budget_key(tenant_id)
        reserve_key = self.reserve_key(run_id)

        result = self.redis.evalsha(
            self.refund_full_sha,
            2,  # num keys
            budget_key,
            reserve_key,
        )

        status = result[0]
        if status == "OK":
            refund = int(result[1])
            new_balance = int(result[2])
            return ("OK", refund, new_balance)
        else:  # ERR_NO_RESERVE
            return ("ERR_NO_RESERVE", 0, 0)

    def get_balance(self, tenant_id: str) -> int:
        """
        Get current budget balance.

        Args:
            tenant_id: Tenant ID

        Returns:
            Current balance in USD_MICROS
        """
        budget_key = self.budget_key(tenant_id)
        balance = self.redis.get(budget_key)
        return int(balance) if balance else 0

    def set_balance(self, tenant_id: str, balance_usd_micros: int) -> None:
        """
        Set budget balance (for testing/admin).

        Args:
            tenant_id: Tenant ID
            balance_usd_micros: Balance to set in USD_MICROS
        """
        budget_key = self.budget_key(tenant_id)
        self.redis.set(budget_key, str(balance_usd_micros))

    def get_reservation(self, run_id: str) -> Optional[dict]:
        """
        Get reservation details.

        Args:
            run_id: Run ID

        Returns:
            Reservation dict or None if not found
        """
        reserve_key = self.reserve_key(run_id)
        data = self.redis.hgetall(reserve_key)
        if not data:
            return None
        return {
            "tenant_id": data["tenant_id"],
            "reserved_usd_micros": int(data["reserved_usd_micros"]),
            "created_at_ms": int(data["created_at_ms"]),
        }
