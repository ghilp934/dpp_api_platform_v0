"""Plan enforcement logic for API monetization.

Implements STEP B: Gateway enforce
- allowed_pack_types check
- pack_type max_cost check
- rate_limit_post_per_min check (Redis TTL)
- RFC 9457 Problem Details responses (DEC-4213)
"""

import logging
from typing import Optional

import redis
from sqlalchemy.orm import Session

from dpp_api.db.models import Plan
from dpp_api.db.repo_plans import TenantPlanRepository

logger = logging.getLogger(__name__)


class PlanViolationError(Exception):
    """Exception raised when plan limits are violated.

    Attributes:
        status_code: HTTP status code (429, 402, 400)
        error_type: RFC 9457 type URI
        title: Short error title
        detail: Detailed error message
        retry_after: Seconds to wait before retry (for 429 errors, optional)
    """

    def __init__(
        self,
        status_code: int,
        error_type: str,
        title: str,
        detail: str,
        retry_after: int | None = None,
    ):
        self.status_code = status_code
        self.error_type = error_type
        self.title = title
        self.detail = detail
        self.retry_after = retry_after
        super().__init__(detail)


class PlanEnforcer:
    """Plan enforcement service for API gateway.

    Validates requests against tenant's active plan limits.
    """

    def __init__(self, db: Session, redis_client: redis.Redis):
        self.db = db
        self.redis = redis_client
        self.tenant_plan_repo = TenantPlanRepository(db)

    def get_active_plan(self, tenant_id: str) -> Plan:
        """Get tenant's active plan or raise violation error.

        Args:
            tenant_id: Tenant ID

        Returns:
            Active Plan object

        Raises:
            PlanViolationError: If no active plan found (400)
        """
        plan = self.tenant_plan_repo.get_active_plan(tenant_id)

        if not plan:
            raise PlanViolationError(
                status_code=400,
                error_type="https://api.dpp.example/problems/no-active-plan",
                title="No Active Plan",
                detail=f"Tenant {tenant_id} has no active plan assigned",
            )

        return plan

    def check_allowed_pack_type(self, plan: Plan, pack_type: str) -> None:
        """Check if pack_type is allowed in plan's features.

        Args:
            plan: Active Plan object
            pack_type: Requested pack type

        Raises:
            PlanViolationError: If pack_type not allowed (400)
        """
        features = plan.features_json or {}
        allowed_pack_types = features.get("allowed_pack_types", [])

        if pack_type not in allowed_pack_types:
            raise PlanViolationError(
                status_code=400,
                error_type="https://api.dpp.example/problems/pack-type-not-allowed",
                title="Pack Type Not Allowed",
                detail=f"Pack type '{pack_type}' is not allowed in plan '{plan.plan_id}'. "
                f"Allowed types: {allowed_pack_types}",
            )

    def check_pack_type_max_cost(
        self,
        plan: Plan,
        pack_type: str,
        requested_max_cost_usd_micros: int,
    ) -> None:
        """Check if requested max_cost is within pack_type limit.

        Args:
            plan: Active Plan object
            pack_type: Requested pack type
            requested_max_cost_usd_micros: Requested max cost in USD micros

        Raises:
            PlanViolationError: If max_cost exceeds limit (402) or below minimum (400)
        """
        # P0-4: Enforce minimum max_cost to ensure minimum_fee never exceeds reserved
        # minimum_fee = max($0.005, 0.02 * reserved), so reserved must be >= $0.005
        MINIMUM_MAX_COST_USD_MICROS = 5_000  # $0.005

        if requested_max_cost_usd_micros < MINIMUM_MAX_COST_USD_MICROS:
            raise PlanViolationError(
                status_code=400,
                error_type="https://api.dpp.example/problems/max-cost-too-low",
                title="Maximum Cost Too Low",
                detail=f"Requested max_cost ({requested_max_cost_usd_micros} micros) "
                f"is below minimum ({MINIMUM_MAX_COST_USD_MICROS} micros). "
                f"Minimum reservation is $0.005 to cover minimum fee.",
            )

        limits = plan.limits_json or {}
        pack_type_limits = limits.get("pack_type_limits", {})
        pack_limit = pack_type_limits.get(pack_type, {})

        max_cost_limit = pack_limit.get("max_cost_usd_micros")

        if max_cost_limit is not None and requested_max_cost_usd_micros > max_cost_limit:
            raise PlanViolationError(
                status_code=402,
                error_type="https://api.dpp.example/problems/max-cost-exceeded",
                title="Maximum Cost Exceeded",
                detail=f"Requested max_cost ({requested_max_cost_usd_micros} micros) "
                f"exceeds plan limit ({max_cost_limit} micros) for pack_type '{pack_type}'",
            )

    def check_rate_limit_post(self, plan: Plan, tenant_id: str) -> None:
        """Check rate limit for POST /runs using Redis.

        P1-1: Atomic rate limiting using INCR-first pattern.
        Uses INCR → check → EXPIRE to avoid race conditions.

        Args:
            plan: Active Plan object
            tenant_id: Tenant ID

        Raises:
            PlanViolationError: If rate limit exceeded (429)
        """
        limits = plan.limits_json or {}
        rate_limit_post_per_min = limits.get("rate_limit_post_per_min")

        if rate_limit_post_per_min is None:
            # No rate limit configured
            return

        # Redis key for rate limiting
        rate_key = f"rate_limit:post_runs:{tenant_id}"

        # P1-1: INCR first (atomic) - returns value AFTER increment
        new_count = self.redis.incr(rate_key)

        # If this is the first request, set TTL
        if new_count == 1:
            self.redis.expire(rate_key, 60)  # 60 seconds TTL

        # Check if limit exceeded
        if new_count > rate_limit_post_per_min:
            # Rate limit exceeded - decrement to rollback
            self.redis.decr(rate_key)
            ttl = self.redis.ttl(rate_key)
            # P1-2: Include retry_after field for 429 errors
            raise PlanViolationError(
                status_code=429,
                error_type="https://api.dpp.example/problems/rate-limit-exceeded",
                title="Rate Limit Exceeded",
                detail=f"Rate limit of {rate_limit_post_per_min} POST /runs per minute exceeded. "
                f"Retry after {ttl} seconds.",
                retry_after=max(1, ttl) if ttl > 0 else 60,
            )

    def check_rate_limit_poll(self, plan: Plan, tenant_id: str) -> None:
        """Check rate limit for GET /runs/{id} polling using Redis.

        P1-1: Atomic rate limiting using INCR-first pattern.
        P1-8: Rate limiting for GET endpoints to prevent excessive polling.

        Args:
            plan: Active Plan object
            tenant_id: Tenant ID

        Raises:
            PlanViolationError: If rate limit exceeded (429 with Retry-After)
        """
        limits = plan.limits_json or {}
        rate_limit_poll_per_min = limits.get("rate_limit_poll_per_min")

        if rate_limit_poll_per_min is None:
            # No rate limit configured
            return

        # Redis key for rate limiting
        rate_key = f"rate_limit:poll_runs:{tenant_id}"

        # P1-1: INCR first (atomic) - returns value AFTER increment
        new_count = self.redis.incr(rate_key)

        # If this is the first request, set TTL
        if new_count == 1:
            self.redis.expire(rate_key, 60)  # 60 seconds TTL

        # Check if limit exceeded
        if new_count > rate_limit_poll_per_min:
            # Rate limit exceeded - decrement to rollback
            self.redis.decr(rate_key)
            ttl = self.redis.ttl(rate_key)
            # P1-2: Include retry_after field for 429 errors
            raise PlanViolationError(
                status_code=429,
                error_type="https://api.dpp.example/problems/rate-limit-exceeded",
                title="Rate Limit Exceeded",
                detail=f"Rate limit of {rate_limit_poll_per_min} GET /runs polling per minute exceeded. "
                f"Retry after {ttl} seconds.",
                retry_after=max(1, ttl) if ttl > 0 else 60,
            )

    def get_rate_limit_headers_post(self, plan: Plan, tenant_id: str) -> dict[str, str]:
        """Get rate limit headers for POST /runs endpoint.

        P1-2: Standardized rate limit headers for all responses.

        Args:
            plan: Active Plan object
            tenant_id: Tenant ID

        Returns:
            Dict of rate limit headers (X-RateLimit-*)
        """
        limits = plan.limits_json or {}
        rate_limit = limits.get("rate_limit_post_per_min", 0)

        if rate_limit == 0:
            # No rate limit configured
            return {}

        # Redis key for rate limiting
        rate_key = f"rate_limit:post_runs:{tenant_id}"

        # Get current count and TTL
        current_count = self.redis.get(rate_key)
        ttl = self.redis.ttl(rate_key)

        # Calculate remaining
        used = int(current_count) if current_count else 0
        remaining = max(0, rate_limit - used)

        # Calculate reset time (current time + TTL)
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        reset_time = int(now.timestamp()) + (ttl if ttl > 0 else 60)

        return {
            "X-RateLimit-Limit": str(rate_limit),
            "X-RateLimit-Remaining": str(remaining),
            "X-RateLimit-Reset": str(reset_time),
        }

    def get_rate_limit_headers_poll(self, plan: Plan, tenant_id: str) -> dict[str, str]:
        """Get rate limit headers for GET /runs/{id} endpoint.

        P1-2: Standardized rate limit headers for all responses.

        Args:
            plan: Active Plan object
            tenant_id: Tenant ID

        Returns:
            Dict of rate limit headers (X-RateLimit-*)
        """
        limits = plan.limits_json or {}
        rate_limit = limits.get("rate_limit_poll_per_min", 0)

        if rate_limit == 0:
            # No rate limit configured
            return {}

        # Redis key for rate limiting
        rate_key = f"rate_limit:poll_runs:{tenant_id}"

        # Get current count and TTL
        current_count = self.redis.get(rate_key)
        ttl = self.redis.ttl(rate_key)

        # Calculate remaining
        used = int(current_count) if current_count else 0
        remaining = max(0, rate_limit - used)

        # Calculate reset time (current time + TTL)
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        reset_time = int(now.timestamp()) + (ttl if ttl > 0 else 60)

        return {
            "X-RateLimit-Limit": str(rate_limit),
            "X-RateLimit-Remaining": str(remaining),
            "X-RateLimit-Reset": str(reset_time),
        }

    def enforce(
        self,
        tenant_id: str,
        pack_type: str,
        max_cost_usd_micros: int,
    ) -> Plan:
        """Enforce all plan constraints for a POST /runs request.

        This is the main entry point for plan enforcement.

        Args:
            tenant_id: Tenant ID
            pack_type: Requested pack type
            max_cost_usd_micros: Requested max cost in USD micros

        Returns:
            Active Plan object (for logging/auditing)

        Raises:
            PlanViolationError: If any constraint is violated
        """
        # 1. Get active plan
        plan = self.get_active_plan(tenant_id)

        # 2. Check allowed pack types
        self.check_allowed_pack_type(plan, pack_type)

        # 3. Check pack type max cost
        self.check_pack_type_max_cost(plan, pack_type, max_cost_usd_micros)

        # 4. Check rate limit (POST /runs)
        self.check_rate_limit_post(plan, tenant_id)

        logger.info(
            f"Plan enforcement passed for tenant {tenant_id}, "
            f"plan {plan.plan_id}, pack_type {pack_type}"
        )

        return plan
