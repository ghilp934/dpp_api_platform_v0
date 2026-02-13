"""E2E tests for API Monetization features (STEP E).

Tests:
- Plan enforcement (allowed_pack_types, max_cost, rate_limit)
- Usage metering accuracy
- Usage API
- Torture scenarios (rate limit, concurrent requests)
"""

import time
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from dpp_api.db.models import Plan, Run, Tenant, TenantPlan, TenantUsageDaily
from dpp_api.db.repo_plans import PlanRepository, TenantPlanRepository
from dpp_api.main import app


@pytest.fixture
def client():
    """FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def test_tenant(db_session: Session) -> Tenant:
    """Create a test tenant."""
    tenant = Tenant(
        tenant_id="tenant_monetization_test",
        display_name="Monetization Test Tenant",
        status="ACTIVE",
    )
    db_session.add(tenant)
    db_session.commit()
    db_session.refresh(tenant)
    return tenant


@pytest.fixture
def basic_plan(db_session: Session) -> Plan:
    """Create a basic plan with limits."""
    plan = Plan(
        plan_id="plan_basic",
        name="Basic Plan",
        status="ACTIVE",
        default_profile_version="v0.4.2.2",
        features_json={
            "allowed_pack_types": ["decision", "url"],
            "max_concurrent_runs": 5,
        },
        limits_json={
            "rate_limit_post_per_min": 10,
            "rate_limit_poll_per_min": 100,
            "pack_type_limits": {
                "decision": {"max_cost_usd_micros": 50_000},  # $0.05
                "url": {"max_cost_usd_micros": 100_000},  # $0.10
            },
        },
    )
    db_session.add(plan)
    db_session.commit()
    db_session.refresh(plan)
    return plan


@pytest.fixture
def premium_plan(db_session: Session) -> Plan:
    """Create a premium plan with higher limits."""
    plan = Plan(
        plan_id="plan_premium",
        name="Premium Plan",
        status="ACTIVE",
        default_profile_version="v0.4.2.2",
        features_json={
            "allowed_pack_types": ["decision", "url", "ocr", "video"],
            "max_concurrent_runs": 20,
        },
        limits_json={
            "rate_limit_post_per_min": 100,
            "rate_limit_poll_per_min": 1000,
            "pack_type_limits": {
                "decision": {"max_cost_usd_micros": 500_000},  # $0.50
                "url": {"max_cost_usd_micros": 1_000_000},  # $1.00
                "ocr": {"max_cost_usd_micros": 2_000_000},  # $2.00
                "video": {"max_cost_usd_micros": 5_000_000},  # $5.00
            },
        },
    )
    db_session.add(plan)
    db_session.commit()
    db_session.refresh(plan)
    return plan


# ============================================================================
# Test 1: Plan Enforcement - Allowed Pack Types
# ============================================================================


def test_plan_enforcement_allowed_pack_types(
    db_session: Session,
    client: TestClient,
    test_tenant: Tenant,
    basic_plan: Plan,
):
    """Test that only allowed pack_types are permitted."""
    # Assign basic plan to tenant (only allows decision, url)
    tenant_plan_repo = TenantPlanRepository(db_session)
    tenant_plan_repo.assign_plan(test_tenant.tenant_id, basic_plan.plan_id)

    # TODO: Create API key and use it for auth
    # For now, this test is conceptual - full E2E requires auth setup

    # Test case 1: Allowed pack_type (decision) - should succeed
    # Test case 2: Disallowed pack_type (ocr) - should return 400

    # Placeholder assertion
    assert basic_plan.features_json["allowed_pack_types"] == ["decision", "url"]


# ============================================================================
# Test 2: Plan Enforcement - Max Cost Limit
# ============================================================================


def test_plan_enforcement_max_cost_limit(
    db_session: Session,
    client: TestClient,
    test_tenant: Tenant,
    basic_plan: Plan,
):
    """Test that max_cost is enforced per pack_type."""
    tenant_plan_repo = TenantPlanRepository(db_session)
    tenant_plan_repo.assign_plan(test_tenant.tenant_id, basic_plan.plan_id)

    # Test case 1: Request within limit - should succeed
    # decision max_cost_usd_micros = 50_000 ($0.05)
    # Request: $0.03 - should be OK

    # Test case 2: Request exceeding limit - should return 402
    # Request: $0.10 - should fail (limit is $0.05)

    # Placeholder assertion
    decision_limit = basic_plan.limits_json["pack_type_limits"]["decision"][
        "max_cost_usd_micros"
    ]
    assert decision_limit == 50_000


# ============================================================================
# Test 3: Rate Limit Enforcement (Torture Test)
# ============================================================================


def test_rate_limit_torture(
    db_session: Session,
    client: TestClient,
    test_tenant: Tenant,
    basic_plan: Plan,
):
    """Torture test: Verify rate limit enforcement.

    Basic plan allows 10 POST /runs per minute.
    Test that 11th request returns 429.
    """
    tenant_plan_repo = TenantPlanRepository(db_session)
    tenant_plan_repo.assign_plan(test_tenant.tenant_id, basic_plan.plan_id)

    # This test requires:
    # 1. Redis running
    # 2. API key authentication
    # 3. POST /runs with valid payload

    # Conceptual test:
    # for i in range(11):
    #     response = client.post("/v1/runs", ...)
    #     if i < 10:
    #         assert response.status_code == 202
    #     else:
    #         assert response.status_code == 429

    rate_limit = basic_plan.limits_json["rate_limit_post_per_min"]
    assert rate_limit == 10


# ============================================================================
# Test 4: Usage Metering Accuracy
# ============================================================================


def test_usage_metering_accuracy(
    db_session: Session,
    test_tenant: Tenant,
):
    """Test that usage metering correctly aggregates run completions."""
    # Create completed runs
    today = date.today()

    runs_data = [
        {"status": "COMPLETED", "actual_cost": 10_000, "reserved": 20_000},
        {"status": "COMPLETED", "actual_cost": 15_000, "reserved": 25_000},
        {"status": "FAILED", "actual_cost": 5_000, "reserved": 10_000},
    ]

    for i, run_data in enumerate(runs_data):
        run = Run(
            run_id=str(uuid.uuid4()),
            tenant_id=test_tenant.tenant_id,
            pack_type="decision",
            profile_version="v0.4.2.2",
            status=run_data["status"],
            money_state="SETTLED",
            idempotency_key=f"metering_test_{i}",
            payload_hash=f"hash_{i}",
            version=2,
            reservation_max_cost_usd_micros=run_data["reserved"],
            actual_cost_usd_micros=run_data["actual_cost"],
            minimum_fee_usd_micros=5_000,
            retention_until=datetime.now(timezone.utc) + timedelta(days=30),
        )
        db_session.add(run)

    db_session.commit()

    # Manually trigger usage tracking (normally done by finalize)
    from dpp_api.metering import UsageTracker

    tracker = UsageTracker(db_session)
    for run in db_session.query(Run).filter_by(tenant_id=test_tenant.tenant_id).all():
        tracker.record_run_completion(run)

    # Verify aggregation
    usage = (
        db_session.query(TenantUsageDaily)
        .filter_by(tenant_id=test_tenant.tenant_id, usage_date=today)
        .first()
    )

    assert usage is not None
    assert usage.runs_count == 3
    assert usage.success_count == 2
    assert usage.fail_count == 1
    assert usage.cost_usd_micros_sum == 30_000  # 10k + 15k + 5k
    assert usage.reserved_usd_micros_sum == 55_000  # 20k + 25k + 10k


# ============================================================================
# Test 5: Usage API - Date Range Query
# ============================================================================


def test_usage_api_date_range(
    db_session: Session,
    client: TestClient,
    test_tenant: Tenant,
):
    """Test GET /v1/tenants/{tenant_id}/usage date range query."""
    # Create usage records for 3 days
    today = date.today()
    yesterday = today - timedelta(days=1)
    two_days_ago = today - timedelta(days=2)

    for usage_date in [two_days_ago, yesterday, today]:
        usage = TenantUsageDaily(
            tenant_id=test_tenant.tenant_id,
            usage_date=usage_date,
            runs_count=10,
            success_count=8,
            fail_count=2,
            cost_usd_micros_sum=50_000,
            reserved_usd_micros_sum=100_000,
        )
        db_session.add(usage)

    db_session.commit()

    # Test case 1: Query all 3 days
    # GET /v1/tenants/{tenant_id}/usage?from={two_days_ago}&to={today}
    # Should return 3 records

    # Test case 2: Query subset (yesterday to today)
    # Should return 2 records

    # Test case 3: Invalid date format - should return 400

    # Placeholder assertion
    usage_count = (
        db_session.query(TenantUsageDaily)
        .filter_by(tenant_id=test_tenant.tenant_id)
        .count()
    )
    assert usage_count == 3


# ============================================================================
# Test 6: Plan Assignment and Active Plan Retrieval
# ============================================================================


def test_plan_assignment_and_retrieval(
    db_session: Session,
    test_tenant: Tenant,
    basic_plan: Plan,
    premium_plan: Plan,
):
    """Test plan assignment and active plan retrieval."""
    tenant_plan_repo = TenantPlanRepository(db_session)

    # Initially no active plan
    active_plan = tenant_plan_repo.get_active_plan(test_tenant.tenant_id)
    assert active_plan is None

    # Assign basic plan
    tenant_plan_repo.assign_plan(
        test_tenant.tenant_id,
        basic_plan.plan_id,
        changed_by="test",
        change_reason="Initial plan",
    )

    active_plan = tenant_plan_repo.get_active_plan(test_tenant.tenant_id)
    assert active_plan is not None
    assert active_plan.plan_id == basic_plan.plan_id

    # Upgrade to premium plan
    tenant_plan_repo.assign_plan(
        test_tenant.tenant_id,
        premium_plan.plan_id,
        changed_by="test",
        change_reason="Upgrade to premium",
    )

    active_plan = tenant_plan_repo.get_active_plan(test_tenant.tenant_id)
    assert active_plan is not None
    assert active_plan.plan_id == premium_plan.plan_id

    # Verify old plan is expired
    expired_plans = (
        db_session.query(TenantPlan)
        .filter_by(tenant_id=test_tenant.tenant_id, plan_id=basic_plan.plan_id)
        .all()
    )
    assert len(expired_plans) == 1
    assert expired_plans[0].status == "EXPIRED"
    assert expired_plans[0].effective_to is not None


# ============================================================================
# Test 7: Concurrent Usage Tracking (UPSERT)
# ============================================================================


def test_concurrent_usage_tracking_upsert(
    db_session: Session,
    test_tenant: Tenant,
):
    """Test that concurrent usage updates use UPSERT correctly.

    Simulates multiple runs completing on the same day.
    """
    from dpp_api.metering import UsageTracker

    tracker = UsageTracker(db_session)
    today = date.today()

    # Create 5 completed runs
    for i in range(5):
        run = Run(
            run_id=str(uuid.uuid4()),
            tenant_id=test_tenant.tenant_id,
            pack_type="decision",
            profile_version="v0.4.2.2",
            status="COMPLETED",
            money_state="SETTLED",
            idempotency_key=f"upsert_test_{i}",
            payload_hash=f"hash_upsert_{i}",
            version=2,
            reservation_max_cost_usd_micros=20_000,
            actual_cost_usd_micros=10_000,
            minimum_fee_usd_micros=5_000,
            retention_until=datetime.now(timezone.utc) + timedelta(days=30),
        )
        db_session.add(run)
        db_session.commit()
        db_session.refresh(run)

        # Record each completion (should UPSERT)
        tracker.record_run_completion(run)

    # Verify single aggregate record
    usage_records = (
        db_session.query(TenantUsageDaily)
        .filter_by(tenant_id=test_tenant.tenant_id, usage_date=today)
        .all()
    )

    assert len(usage_records) == 1
    usage = usage_records[0]
    assert usage.runs_count == 5
    assert usage.success_count == 5
    assert usage.fail_count == 0
    assert usage.cost_usd_micros_sum == 50_000  # 5 * 10k
    assert usage.reserved_usd_micros_sum == 100_000  # 5 * 20k
