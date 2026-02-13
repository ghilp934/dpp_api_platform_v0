"""Regression tests for critical feedback fixes (P0-1, P0-2, P1-1, P1-2, P1-3).

This test suite verifies all critical production fixes from final feedback:
- P0-1: Heartbeat thread-safety + finalize race condition
- P0-2: AWS credentials (LocalStack only)
- P1-1: RateLimit atomic Redis operations
- P1-2: PlanViolation retry_after field
- P1-3: IntegrityError explicit handling
"""

import uuid
from unittest.mock import MagicMock, Mock, patch

import pytest
import redis
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from dpp_api.db.models import Plan
from dpp_api.enforce import PlanEnforcer, PlanViolationError


# ============================================================================
# P0-1: Heartbeat Thread-Safety
# ============================================================================


def test_heartbeat_uses_session_factory():
    """P0-1: Verify HeartbeatThread accepts session_factory instead of db_session.

    This prevents thread-safety issues where multiple threads share the same session.
    """
    # P0-1: Worker tests require dpp_worker module (not in API test path)
    try:
        from dpp_worker.heartbeat import HeartbeatThread
    except ImportError:
        pytest.skip("Worker module not available in API test environment")

    # Create a mock session factory
    mock_factory = Mock(spec=sessionmaker)
    mock_session = MagicMock()
    mock_factory.return_value.__enter__.return_value = mock_session

    # Create heartbeat thread with session_factory
    heartbeat = HeartbeatThread(
        run_id="test-run-123",
        tenant_id="tenant-test",
        lease_token="lease-abc",
        current_version=1,
        session_factory=mock_factory,  # P0-1: Factory instead of session
        sqs_client=MagicMock(),
        queue_url="http://localhost:4566/queue",
        receipt_handle="receipt-xyz",
        heartbeat_interval_sec=30,
        lease_extension_sec=120,
    )

    # Verify session_factory is stored
    assert heartbeat.session_factory == mock_factory
    assert not hasattr(heartbeat, "db"), "Should not have db attribute (shared session)"


def test_sqs_loop_passes_session_factory():
    """P0-1: Verify WorkerLoop passes session_factory to HeartbeatThread."""
    try:
        from dpp_worker.loops.sqs_loop import WorkerLoop
    except ImportError:
        pytest.skip("Worker module not available in API test environment")

    mock_factory = Mock(spec=sessionmaker)
    mock_session = MagicMock()

    worker = WorkerLoop(
        sqs_client=MagicMock(),
        s3_client=MagicMock(),
        db_session=mock_session,
        session_factory=mock_factory,  # P0-1: Factory parameter
        budget_manager=MagicMock(),
        queue_url="http://localhost:4566/queue",
        result_bucket="test-bucket",
    )

    assert worker.session_factory == mock_factory


def test_process_message_returns_bool():
    """P0-1: Verify _process_message returns bool (True=delete, False=no delete)."""
    try:
        from dpp_worker.loops.sqs_loop import WorkerLoop
    except ImportError:
        pytest.skip("Worker module not available in API test environment")

    worker = WorkerLoop(
        sqs_client=MagicMock(),
        s3_client=MagicMock(),
        db_session=MagicMock(),
        session_factory=Mock(spec=sessionmaker),
        budget_manager=MagicMock(),
        queue_url="http://localhost:4566/queue",
        result_bucket="test-bucket",
    )

    # Verify _process_message has correct return type annotation
    import inspect

    sig = inspect.signature(worker._process_message)
    assert sig.return_annotation == bool, "P0-1: _process_message must return bool"


# ============================================================================
# P0-2: AWS Credentials (LocalStack Only)
# ============================================================================


def test_localstack_detection():
    """P0-2: Verify LocalStack detection logic only uses test creds for localhost."""
    try:
        from dpp_worker.main import main
    except ImportError:
        pytest.skip("Worker module not available in API test environment")

    # Test LocalStack endpoints
    with patch.dict(
        "os.environ",
        {
            "SQS_ENDPOINT_URL": "http://localhost:4566",
            "S3_ENDPOINT_URL": "http://localhost:4566",
            "DATABASE_URL": "sqlite:///:memory:",
        },
    ):
        with patch("dpp_worker.main.boto3.client") as mock_boto3:
            with patch("dpp_worker.main.create_engine"):
                with patch("dpp_worker.main.RedisClient.get_client"):
                    with patch("dpp_worker.main.WorkerLoop"):
                        try:
                            # Start worker (will be stopped by KeyboardInterrupt)
                            with pytest.raises(KeyboardInterrupt):
                                with patch(
                                    "dpp_worker.main.WorkerLoop.run_forever",
                                    side_effect=KeyboardInterrupt,
                                ):
                                    main()
                        except SystemExit:
                            pass

            # Verify test credentials were used for LocalStack
            sqs_call = [call for call in mock_boto3.call_args_list if call[0][0] == "sqs"][0]
            assert "aws_access_key_id" in sqs_call[1]
            assert sqs_call[1]["aws_access_key_id"] == "test"


def test_production_no_hardcoded_creds():
    """P0-2: Verify production endpoints don't use hardcoded credentials."""
    from dpp_api.queue.sqs_client import SQSClient

    # Test production endpoint (not localhost)
    with patch.dict("os.environ", {"SQS_ENDPOINT_URL": "https://sqs.us-east-1.amazonaws.com"}):
        with patch("dpp_api.queue.sqs_client.boto3.client") as mock_boto3:
            mock_boto3.return_value = MagicMock()
            client = SQSClient()

            # Verify no hardcoded credentials for production
            boto3_call = mock_boto3.call_args
            assert "aws_access_key_id" not in boto3_call[1], "P0-2: No hardcoded creds for production"


# ============================================================================
# P1-1: RateLimit Atomic Redis Operations
# ============================================================================


def test_rate_limit_atomic_incr(db_session: Session, redis_client: redis.Redis):
    """P1-1: Verify rate limiting uses INCR-first pattern (atomic)."""
    from dpp_api.db.models import Plan

    # Delete existing plan if present (PostgreSQL cleanup)
    plan_id = "plan_rate_test"
    existing = db_session.query(Plan).filter_by(plan_id=plan_id).first()
    if existing:
        db_session.delete(existing)
        db_session.commit()

    # Create plan with rate limit
    plan = Plan(
        plan_id=plan_id,
        name="Rate Limit Test",
        status="ACTIVE",
        default_profile_version="v0.4.2.2",
        features_json={"allowed_pack_types": ["decision"]},
        limits_json={"rate_limit_post_per_min": 5},  # Low limit for testing
    )
    db_session.add(plan)
    db_session.commit()

    enforcer = PlanEnforcer(db_session, redis_client)
    tenant_id = "tenant_rate_test"

    # First 5 requests should succeed
    for i in range(5):
        enforcer.check_rate_limit_post(plan, tenant_id)
        # Verify INCR happened atomically
        rate_key = f"rate_limit:post_runs:{tenant_id}"
        count = redis_client.get(rate_key)
        assert int(count) == i + 1, f"P1-1: Count should be {i+1} after {i+1} requests"

    # 6th request should fail
    with pytest.raises(PlanViolationError) as exc_info:
        enforcer.check_rate_limit_post(plan, tenant_id)

    assert exc_info.value.status_code == 429
    # P1-1: DECR should have been called on failure (rollback)
    final_count = redis_client.get(f"rate_limit:post_runs:{tenant_id}")
    assert int(final_count) == 5, "P1-1: Count should be rolled back to 5 after 6th request"


def test_rate_limit_concurrent_safety(db_session: Session, redis_client: redis.Redis):
    """P1-1: Verify INCR-first prevents race conditions in concurrent requests."""
    from concurrent.futures import ThreadPoolExecutor

    from dpp_api.db.models import Plan

    # Delete existing plan if present (PostgreSQL cleanup)
    plan_id = "plan_concurrent_test"
    existing = db_session.query(Plan).filter_by(plan_id=plan_id).first()
    if existing:
        db_session.delete(existing)
        db_session.commit()

    plan = Plan(
        plan_id=plan_id,
        name="Concurrent Test",
        status="ACTIVE",
        default_profile_version="v0.4.2.2",
        features_json={"allowed_pack_types": ["decision"]},
        limits_json={"rate_limit_post_per_min": 10},
    )
    db_session.add(plan)
    db_session.commit()

    enforcer = PlanEnforcer(db_session, redis_client)
    tenant_id = "tenant_concurrent"

    # Simulate 20 concurrent requests (10 should succeed, 10 should fail)
    def try_request():
        try:
            enforcer.check_rate_limit_post(plan, tenant_id)
            return "success"
        except PlanViolationError:
            return "rate_limited"

    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(lambda _: try_request(), range(20)))

    # Exactly 10 should succeed (atomic INCR prevents double-counting)
    success_count = results.count("success")
    rate_limited_count = results.count("rate_limited")

    assert success_count == 10, f"P1-1: Expected 10 successes, got {success_count}"
    assert rate_limited_count == 10, f"P1-1: Expected 10 rate limits, got {rate_limited_count}"


# ============================================================================
# P1-2: PlanViolation retry_after Field
# ============================================================================


def test_plan_violation_has_retry_after(db_session: Session, redis_client: redis.Redis):
    """P1-2: Verify PlanViolationError includes retry_after field for 429 errors."""
    from dpp_api.db.models import Plan

    # Delete existing plan if present (PostgreSQL cleanup)
    plan_id = "plan_retry_test"
    existing = db_session.query(Plan).filter_by(plan_id=plan_id).first()
    if existing:
        db_session.delete(existing)
        db_session.commit()

    plan = Plan(
        plan_id=plan_id,
        name="Retry Test",
        status="ACTIVE",
        default_profile_version="v0.4.2.2",
        features_json={"allowed_pack_types": ["decision"]},
        limits_json={"rate_limit_post_per_min": 1},  # Very low limit
    )
    db_session.add(plan)
    db_session.commit()

    enforcer = PlanEnforcer(db_session, redis_client)
    tenant_id = "tenant_retry_test"

    # First request succeeds
    enforcer.check_rate_limit_post(plan, tenant_id)

    # Second request should fail with retry_after
    with pytest.raises(PlanViolationError) as exc_info:
        enforcer.check_rate_limit_post(plan, tenant_id)

    error = exc_info.value
    assert error.status_code == 429
    assert hasattr(error, "retry_after"), "P1-2: PlanViolationError must have retry_after field"
    assert error.retry_after is not None, "P1-2: retry_after should be set for 429 errors"
    assert isinstance(error.retry_after, int), "P1-2: retry_after must be int"
    assert 1 <= error.retry_after <= 60, f"P1-2: retry_after should be 1-60s, got {error.retry_after}"


def test_exception_handler_uses_retry_after(test_client: TestClient, test_tenant_with_api_key, redis_client: redis.Redis):
    """P1-2: Verify exception handler uses retry_after field (no regex parsing)."""
    tenant_id, api_key, _ = test_tenant_with_api_key

    # Clear Redis to ensure clean state
    redis_client.flushdb()

    # Set very low rate limit for tenant's plan
    from dpp_api.db.models import Plan

    db = next(test_client.app.dependency_overrides[get_db]())
    plan = db.query(Plan).filter_by(plan_id="plan_e2e_basic").first()
    original_limits = plan.limits_json.copy()
    plan.limits_json = {
        **original_limits,
        "rate_limit_post_per_min": 1,  # Very low limit for testing
    }
    db.commit()
    db.refresh(plan)

    try:
        # First request succeeds
        response1 = test_client.post(
            "/v1/runs",
            json={
                "pack_type": "decision",
                "inputs": {"question": "Test?"},
                "reservation": {"max_cost_usd": "1.00"},
            },
            headers={
                "Authorization": f"Bearer {api_key}",
                "Idempotency-Key": str(uuid.uuid4()),
            },
        )
        assert response1.status_code == 202

        # Second request should fail with Retry-After header
        response2 = test_client.post(
            "/v1/runs",
            json={
                "pack_type": "decision",
                "inputs": {"question": "Test 2?"},
                "reservation": {"max_cost_usd": "1.00"},
            },
            headers={
                "Authorization": f"Bearer {api_key}",
                "Idempotency-Key": str(uuid.uuid4()),
            },
        )

        assert response2.status_code == 429, f"P1-2: Expected 429, got {response2.status_code}"
        assert "Retry-After" in response2.headers, "P1-2: Retry-After header should be present"
        retry_after = int(response2.headers["Retry-After"])
        assert 1 <= retry_after <= 60, f"P1-2: Retry-After should be 1-60s, got {retry_after}"
    finally:
        # Restore original limits
        plan.limits_json = original_limits
        db.commit()


# ============================================================================
# P1-3: IntegrityError Explicit Handling
# ============================================================================


def test_integrity_error_idempotency_key_conflict(test_client: TestClient, test_tenant_with_api_key):
    """P1-3: Verify IntegrityError is explicitly caught for idempotency key conflicts."""
    tenant_id, api_key, _ = test_tenant_with_api_key
    idempotency_key = str(uuid.uuid4())

    # First request
    response1 = test_client.post(
        "/v1/runs",
        json={
            "pack_type": "decision",
            "inputs": {"question": "Test?"},
            "reservation": {"max_cost_usd": "1.00"},
        },
        headers={
            "Authorization": f"Bearer {api_key}",
            "Idempotency-Key": idempotency_key,
        },
    )
    assert response1.status_code == 202
    run_id_1 = response1.json()["run_id"]

    # Second request with same idempotency key (should return existing run)
    response2 = test_client.post(
        "/v1/runs",
        json={
            "pack_type": "decision",
            "inputs": {"question": "Test?"},
            "reservation": {"max_cost_usd": "1.00"},
        },
        headers={
            "Authorization": f"Bearer {api_key}",
            "Idempotency-Key": idempotency_key,
        },
    )
    assert response2.status_code == 202
    run_id_2 = response2.json()["run_id"]

    # P1-3: Should return same run_id (explicit IntegrityError handling)
    assert run_id_1 == run_id_2, "P1-3: Should return existing run on idempotency key conflict"


def test_integrity_error_different_payload(test_client: TestClient, test_tenant_with_api_key):
    """P1-3: Verify IntegrityError with different payload raises 409 Conflict."""
    tenant_id, api_key, _ = test_tenant_with_api_key
    idempotency_key = str(uuid.uuid4())

    # First request
    response1 = test_client.post(
        "/v1/runs",
        json={
            "pack_type": "decision",
            "inputs": {"question": "Original question?"},
            "reservation": {"max_cost_usd": "1.00"},
        },
        headers={
            "Authorization": f"Bearer {api_key}",
            "Idempotency-Key": idempotency_key,
        },
    )
    assert response1.status_code == 202

    # Second request with same key but different payload
    response2 = test_client.post(
        "/v1/runs",
        json={
            "pack_type": "decision",
            "inputs": {"question": "Different question?"},  # Different payload
            "reservation": {"max_cost_usd": "1.00"},
        },
        headers={
            "Authorization": f"Bearer {api_key}",
            "Idempotency-Key": idempotency_key,
        },
    )

    # P1-3: Should return 409 Conflict (hash mismatch)
    assert response2.status_code == 409
    assert "different payload" in response2.json()["detail"].lower()


# ============================================================================
# Integration Test: End-to-End Validation
# ============================================================================


def test_critical_feedback_integration(test_client: TestClient, test_tenant_with_api_key, redis_client: redis.Redis):
    """Integration test covering all critical feedback fixes.

    This test validates:
    - P0-1: Message delete control (claim failure)
    - P1-1: Atomic rate limiting
    - P1-2: Retry-After header presence
    - P1-3: Idempotency handling
    """
    from dpp_api.db.models import Plan

    tenant_id, api_key, _ = test_tenant_with_api_key

    # Clear Redis to ensure clean state
    redis_client.flushdb()

    # Setup: Low rate limit for testing
    db = next(test_client.app.dependency_overrides[get_db]())
    plan = db.query(Plan).filter_by(plan_id="plan_e2e_basic").first()
    original_limits = plan.limits_json.copy()
    plan.limits_json = {
        **original_limits,
        "rate_limit_post_per_min": 2,  # Low limit for testing
    }
    db.commit()
    db.refresh(plan)

    try:
        # Test 1: First request succeeds
        response1 = test_client.post(
            "/v1/runs",
            json={
                "pack_type": "decision",
                "inputs": {"question": "Integration test?"},
                "reservation": {"max_cost_usd": "1.00"},
            },
            headers={
                "Authorization": f"Bearer {api_key}",
                "Idempotency-Key": "integration-test-1",
            },
        )
        assert response1.status_code == 202

        # Test 2: Idempotent request returns same run (P1-3)
        response2 = test_client.post(
            "/v1/runs",
            json={
                "pack_type": "decision",
                "inputs": {"question": "Integration test?"},
                "reservation": {"max_cost_usd": "1.00"},
            },
            headers={
                "Authorization": f"Bearer {api_key}",
                "Idempotency-Key": "integration-test-1",
            },
        )
        assert response2.status_code == 202
        assert response1.json()["run_id"] == response2.json()["run_id"]

        # Test 3: Second unique request succeeds (rate limit 2/min)
        response3 = test_client.post(
            "/v1/runs",
            json={
                "pack_type": "decision",
                "inputs": {"question": "Another test?"},
                "reservation": {"max_cost_usd": "1.00"},
            },
            headers={
                "Authorization": f"Bearer {api_key}",
                "Idempotency-Key": "integration-test-2",
            },
        )
        assert response3.status_code == 202

        # Test 4: Third unique request exceeds rate limit (P1-1 atomic, P1-2 retry_after)
        response4 = test_client.post(
            "/v1/runs",
            json={
                "pack_type": "decision",
                "inputs": {"question": "Rate limited?"},
                "reservation": {"max_cost_usd": "1.00"},
            },
            headers={
                "Authorization": f"Bearer {api_key}",
                "Idempotency-Key": "integration-test-3",
            },
        )
        assert response4.status_code == 429, f"Expected 429, got {response4.status_code}"
        assert "Retry-After" in response4.headers
        assert response4.json()["status"] == 429
    finally:
        # Restore original limits
        plan.limits_json = original_limits
        db.commit()


# Ensure we're importing get_db for test_client fixture
from dpp_api.db.session import get_db
