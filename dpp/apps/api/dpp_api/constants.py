"""DPP API Constants.

Centralized constants to prevent configuration drift and ensure consistency
across modules.
"""

# Budget & Reservation
RESERVATION_TTL_SECONDS = 3600  # 1 hour - Redis reservation expiration
"""
TTL for Redis reservation keys (reserve:{run_id}).

CRITICAL: This value MUST match:
- redis_scripts.py: self.redis.expire(reserve_key, RESERVATION_TTL_SECONDS)
- reconcile_loop.py: TTL Safety Check for idempotent force-settle

If values drift, MS-6 idempotent reconciliation may incorrectly assume
settle() succeeded when reservation actually expired naturally.
"""

# Money
USD_MICROS_PER_DOLLAR = 1_000_000
"""1 USD = 1,000,000 micros (DEC-4211 money precision)"""
