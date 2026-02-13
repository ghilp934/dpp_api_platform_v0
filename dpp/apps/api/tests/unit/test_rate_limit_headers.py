"""Unit tests for Rate Limit Headers (P1-2).

Tests that X-RateLimit-* headers are correctly generated.
"""

import time

import pytest
import redis

from dpp_api.db.models import Plan
from dpp_api.enforce.plan_enforcer import PlanEnforcer


def test_get_rate_limit_headers_post_with_limit(redis_client: redis.Redis) -> None:
    """Test get_rate_limit_headers_post returns correct headers when limit is configured."""
    tenant_id = "tenant_rl_post_test"

    # Create plan with rate limit
    plan = Plan(
        plan_id="plan_test",
        name="Test Plan",
        status="ACTIVE",
        default_profile_version="v0.4.2.2",
        features_json={},
        limits_json={
            "rate_limit_post_per_min": 10,  # 10 requests per minute
        },
    )

    # Create plan enforcer (without DB, just for header generation)
    enforcer = PlanEnforcer(None, redis_client)  # DB not needed for this test

    # Simulate 3 requests
    for i in range(3):
        rate_key = f"rate_limit:post_runs:{tenant_id}"
        redis_client.incr(rate_key)
        if i == 0:
            redis_client.expire(rate_key, 60)

    # Get headers
    headers = enforcer.get_rate_limit_headers_post(plan, tenant_id)

    # Validate
    assert headers["X-RateLimit-Limit"] == "10"
    assert headers["X-RateLimit-Remaining"] == "7"  # 10 - 3 = 7
    assert "X-RateLimit-Reset" in headers

    # Reset time should be a Unix timestamp in the future
    reset_time = int(headers["X-RateLimit-Reset"])
    now = int(time.time())
    assert now < reset_time < now + 120  # Should be within next 2 minutes


def test_get_rate_limit_headers_post_no_limit(redis_client: redis.Redis) -> None:
    """Test get_rate_limit_headers_post returns empty dict when no limit configured."""
    tenant_id = "tenant_rl_post_no_limit"

    # Create plan WITHOUT rate limit
    plan = Plan(
        plan_id="plan_no_limit",
        name="No Limit Plan",
        status="ACTIVE",
        default_profile_version="v0.4.2.2",
        features_json={},
        limits_json={},  # No rate limits
    )

    enforcer = PlanEnforcer(None, redis_client)
    headers = enforcer.get_rate_limit_headers_post(plan, tenant_id)

    # Should be empty
    assert headers == {}


def test_get_rate_limit_headers_poll_with_limit(redis_client: redis.Redis) -> None:
    """Test get_rate_limit_headers_poll returns correct headers when limit is configured."""
    tenant_id = "tenant_rl_poll_test"

    # Create plan with rate limit
    plan = Plan(
        plan_id="plan_poll_test",
        name="Test Plan",
        status="ACTIVE",
        default_profile_version="v0.4.2.2",
        features_json={},
        limits_json={
            "rate_limit_poll_per_min": 50,  # 50 polls per minute
        },
    )

    enforcer = PlanEnforcer(None, redis_client)

    # Simulate 5 polls
    for i in range(5):
        rate_key = f"rate_limit:poll_runs:{tenant_id}"
        redis_client.incr(rate_key)
        if i == 0:
            redis_client.expire(rate_key, 60)

    # Get headers
    headers = enforcer.get_rate_limit_headers_poll(plan, tenant_id)

    # Validate
    assert headers["X-RateLimit-Limit"] == "50"
    assert headers["X-RateLimit-Remaining"] == "45"  # 50 - 5 = 45
    assert "X-RateLimit-Reset" in headers

    reset_time = int(headers["X-RateLimit-Reset"])
    now = int(time.time())
    assert now < reset_time < now + 120


def test_get_rate_limit_headers_poll_no_limit(redis_client: redis.Redis) -> None:
    """Test get_rate_limit_headers_poll returns empty dict when no limit configured."""
    tenant_id = "tenant_rl_poll_no_limit"

    # Create plan WITHOUT rate limit
    plan = Plan(
        plan_id="plan_poll_no_limit",
        name="No Limit Plan",
        status="ACTIVE",
        default_profile_version="v0.4.2.2",
        features_json={},
        limits_json={},  # No rate limits
    )

    enforcer = PlanEnforcer(None, redis_client)
    headers = enforcer.get_rate_limit_headers_poll(plan, tenant_id)

    # Should be empty
    assert headers == {}


def test_get_rate_limit_headers_remaining_zero(redis_client: redis.Redis) -> None:
    """Test headers show remaining=0 when limit is reached."""
    tenant_id = "tenant_rl_exhausted"

    # Create plan with small limit
    plan = Plan(
        plan_id="plan_small_limit",
        name="Small Limit Plan",
        status="ACTIVE",
        default_profile_version="v0.4.2.2",
        features_json={},
        limits_json={
            "rate_limit_post_per_min": 3,  # Only 3 requests
        },
    )

    enforcer = PlanEnforcer(None, redis_client)

    # Use up all 3 requests
    rate_key = f"rate_limit:post_runs:{tenant_id}"
    for i in range(3):
        redis_client.incr(rate_key)
        if i == 0:
            redis_client.expire(rate_key, 60)

    # Get headers
    headers = enforcer.get_rate_limit_headers_post(plan, tenant_id)

    # Remaining should be 0
    assert headers["X-RateLimit-Limit"] == "3"
    assert headers["X-RateLimit-Remaining"] == "0"


def test_get_rate_limit_headers_first_request(redis_client: redis.Redis) -> None:
    """Test headers for first request (no Redis key exists yet)."""
    tenant_id = "tenant_rl_first_request"

    # Create plan with rate limit
    plan = Plan(
        plan_id="plan_first_req",
        name="Test Plan",
        status="ACTIVE",
        default_profile_version="v0.4.2.2",
        features_json={},
        limits_json={
            "rate_limit_post_per_min": 20,
        },
    )

    enforcer = PlanEnforcer(None, redis_client)

    # Get headers (no requests made yet)
    headers = enforcer.get_rate_limit_headers_post(plan, tenant_id)

    # Limit should be full (no requests used)
    assert headers["X-RateLimit-Limit"] == "20"
    assert headers["X-RateLimit-Remaining"] == "20"  # All available
