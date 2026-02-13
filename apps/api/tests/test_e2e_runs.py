"""End-to-End integration tests for runs API (POST → GET flow)."""

import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from dpp_api.budget import BudgetManager
from dpp_api.db.models import APIKey, Tenant
from dpp_api.db.repo_api_keys import APIKeyRepository
from dpp_api.db.repo_tenants import TenantRepository
from dpp_api.db.session import get_db
from dpp_api.main import app


@pytest.fixture
def mock_sqs_client():
    """Mock SQS client to avoid LocalStack dependency in tests."""
    with patch("dpp_api.routers.runs.get_sqs_client") as mock_get_sqs:
        mock_client = MagicMock()
        mock_client.enqueue_run.return_value = "mock-message-id-123"
        mock_get_sqs.return_value = mock_client
        yield mock_client


@pytest.fixture
def test_client(db_session: Session, mock_sqs_client):
    """TestClient with db_session dependency override and mocked SQS."""

    def override_get_db():
        try:
            yield db_session
        finally:
            pass  # Don't close - conftest will handle it

    app.dependency_overrides[get_db] = override_get_db
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


@pytest.fixture
def test_tenant_with_api_key(db_session: Session) -> tuple[str, str, str]:
    """Create a test tenant with API key and budget.

    Returns:
        Tuple of (tenant_id, api_key, key_hash)
    """
    tenant_id = f"tenant_{uuid.uuid4().hex[:8]}"
    key_id = str(uuid.uuid4())
    secret = uuid.uuid4().hex
    api_key = f"sk_{key_id}_{secret}"

    # Hash the key
    import hashlib

    key_hash = hashlib.sha256(api_key.encode()).hexdigest()

    # Create tenant
    tenant_repo = TenantRepository(db_session)
    tenant_repo.create(
        Tenant(tenant_id=tenant_id, display_name=f"Test Tenant {tenant_id}", status="ACTIVE")
    )

    # Create API key
    api_key_repo = APIKeyRepository(db_session)
    api_key_repo.create(
        APIKey(
            key_id=key_id,
            tenant_id=tenant_id,
            key_hash=key_hash,
            label="Test Key",
            status="ACTIVE",
        )
    )

    # Set budget
    import redis

    from dpp_api.budget.redis_scripts import BudgetScripts
    from dpp_api.db.redis_client import RedisClient

    redis_client = RedisClient.get_client()
    budget_scripts = BudgetScripts(redis_client)
    budget_scripts.set_balance(tenant_id, 10_000_000)  # $10.00

    return (tenant_id, api_key, key_hash)


def test_e2e_post_run_success(db_session: Session, test_client, test_tenant_with_api_key):
    """Test successful POST /v1/runs with valid API key and budget."""
    tenant_id, api_key, key_hash = test_tenant_with_api_key

    idempotency_key = f"test-{uuid.uuid4()}"

    request_body = {
        "pack_type": "decision",
        "inputs": {"question": "Should we proceed?", "context": "Budget is limited"},
        "reservation": {"max_cost_usd": "0.5000", "timebox_sec": 90, "min_reliability_score": 0.8},
        "meta": {"trace_id": "trace-123", "profile_version": "v0.4.2.2"},
    }

    response = test_client.post(
        "/v1/runs",
        json=request_body,
        headers={"Authorization": f"Bearer {api_key}", "Idempotency-Key": idempotency_key},
    )

    assert response.status_code == 202
    data = response.json()

    assert "run_id" in data
    assert data["status"] == "QUEUED"
    assert data["poll"]["href"] == f"/v1/runs/{data['run_id']}"
    assert data["reservation"]["max_cost_usd"] == "0.5000"


def test_e2e_post_run_idempotency(db_session: Session, test_client, test_tenant_with_api_key):
    """Test idempotency: same key + same payload returns same run_id."""
    tenant_id, api_key, key_hash = test_tenant_with_api_key

    idempotency_key = f"test-idem-{uuid.uuid4()}"

    request_body = {
        "pack_type": "decision",
        "inputs": {"question": "Test question"},
        "reservation": {"max_cost_usd": "0.1000"},
    }

    # First request
    response1 = test_client.post(
        "/v1/runs",
        json=request_body,
        headers={"Authorization": f"Bearer {api_key}", "Idempotency-Key": idempotency_key},
    )

    assert response1.status_code == 202
    run_id_1 = response1.json()["run_id"]

    # Second request (same key, same payload)
    response2 = test_client.post(
        "/v1/runs",
        json=request_body,
        headers={"Authorization": f"Bearer {api_key}", "Idempotency-Key": idempotency_key},
    )

    assert response2.status_code == 202
    run_id_2 = response2.json()["run_id"]

    # Should return the same run_id
    assert run_id_1 == run_id_2


def test_e2e_post_run_idempotency_conflict(db_session: Session, test_client, test_tenant_with_api_key):
    """Test idempotency conflict: same key + different payload returns 409."""
    tenant_id, api_key, key_hash = test_tenant_with_api_key

    idempotency_key = f"test-conflict-{uuid.uuid4()}"

    request_body_1 = {
        "pack_type": "decision",
        "inputs": {"question": "Question 1"},
        "reservation": {"max_cost_usd": "0.1000"},
    }

    request_body_2 = {
        "pack_type": "decision",
        "inputs": {"question": "Question 2"},  # Different!
        "reservation": {"max_cost_usd": "0.1000"},
    }

    # First request
    response1 = test_client.post(
        "/v1/runs",
        json=request_body_1,
        headers={"Authorization": f"Bearer {api_key}", "Idempotency-Key": idempotency_key},
    )

    assert response1.status_code == 202

    # Second request (same key, DIFFERENT payload)
    response2 = test_client.post(
        "/v1/runs",
        json=request_body_2,
        headers={"Authorization": f"Bearer {api_key}", "Idempotency-Key": idempotency_key},
    )

    assert response2.status_code == 409
    assert "different payload" in response2.json()["detail"].lower()


def test_e2e_post_run_insufficient_budget(db_session: Session, test_client, test_tenant_with_api_key):
    """Test POST /v1/runs fails with 402 when budget is insufficient."""
    tenant_id, api_key, key_hash = test_tenant_with_api_key

    # Drain budget
    import redis

    from dpp_api.budget.redis_scripts import BudgetScripts
    from dpp_api.db.redis_client import RedisClient

    redis_client = RedisClient.get_client()
    budget_scripts = BudgetScripts(redis_client)
    budget_scripts.set_balance(tenant_id, 1_000)  # Only $0.001

    idempotency_key = f"test-budget-{uuid.uuid4()}"

    request_body = {
        "pack_type": "decision",
        "inputs": {"question": "Test"},
        "reservation": {"max_cost_usd": "0.5000"},  # Need $0.50
    }

    response = test_client.post(
        "/v1/runs",
        json=request_body,
        headers={"Authorization": f"Bearer {api_key}", "Idempotency-Key": idempotency_key},
    )

    assert response.status_code == 402
    assert "insufficient" in response.json()["detail"].lower()


def test_e2e_get_run_success(db_session: Session, test_client, test_tenant_with_api_key):
    """Test GET /v1/runs/{run_id} returns correct status."""
    tenant_id, api_key, key_hash = test_tenant_with_api_key

    # Create a run first
    idempotency_key = f"test-get-{uuid.uuid4()}"

    request_body = {
        "pack_type": "decision",
        "inputs": {"question": "Test"},
        "reservation": {"max_cost_usd": "0.2000"},
    }

    post_response = test_client.post(
        "/v1/runs",
        json=request_body,
        headers={"Authorization": f"Bearer {api_key}", "Idempotency-Key": idempotency_key},
    )

    assert post_response.status_code == 202
    run_id = post_response.json()["run_id"]

    # Get the run
    get_response = test_client.get(
        f"/v1/runs/{run_id}",
        headers={"Authorization": f"Bearer {api_key}"},
    )

    assert get_response.status_code == 200
    data = get_response.json()

    assert data["run_id"] == run_id
    assert data["status"] == "QUEUED"
    assert data["money_state"] == "RESERVED"
    assert data["cost"]["reserved_usd"] == "0.2000"


def test_e2e_get_run_not_found(db_session: Session, test_client, test_tenant_with_api_key):
    """Test GET /v1/runs/{run_id} returns 404 for non-existent run (stealth)."""
    tenant_id, api_key, key_hash = test_tenant_with_api_key

    fake_run_id = str(uuid.uuid4())

    response = test_client.get(
        f"/v1/runs/{fake_run_id}",
        headers={"Authorization": f"Bearer {api_key}"},
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_e2e_get_run_wrong_tenant_stealth_404(db_session: Session, test_client):
    """Test GET /v1/runs/{run_id} returns 404 for run owned by different tenant (DEC-4204 stealth)."""
    # Create two tenants
    tenant1_id = f"tenant_{uuid.uuid4().hex[:8]}"
    tenant2_id = f"tenant_{uuid.uuid4().hex[:8]}"

    # Create API keys for both
    import hashlib

    key1_id = str(uuid.uuid4())
    secret1 = uuid.uuid4().hex
    api_key_1 = f"sk_{key1_id}_{secret1}"
    key1_hash = hashlib.sha256(api_key_1.encode()).hexdigest()

    key2_id = str(uuid.uuid4())
    secret2 = uuid.uuid4().hex
    api_key_2 = f"sk_{key2_id}_{secret2}"
    key2_hash = hashlib.sha256(api_key_2.encode()).hexdigest()

    tenant_repo = TenantRepository(db_session)
    api_key_repo = APIKeyRepository(db_session)

    tenant_repo.create(Tenant(tenant_id=tenant1_id, display_name="Tenant 1", status="ACTIVE"))
    tenant_repo.create(Tenant(tenant_id=tenant2_id, display_name="Tenant 2", status="ACTIVE"))

    api_key_repo.create(
        APIKey(key_id=key1_id, tenant_id=tenant1_id, key_hash=key1_hash, status="ACTIVE")
    )
    api_key_repo.create(
        APIKey(key_id=key2_id, tenant_id=tenant2_id, key_hash=key2_hash, status="ACTIVE")
    )

    # Set budgets (use RedisClient singleton to match API code)
    from dpp_api.budget.redis_scripts import BudgetScripts
    from dpp_api.db.redis_client import RedisClient

    redis_client = RedisClient.get_client()
    budget_scripts = BudgetScripts(redis_client)
    budget_scripts.set_balance(tenant1_id, 10_000_000)
    budget_scripts.set_balance(tenant2_id, 10_000_000)

    # Tenant 1 creates a run
    idempotency_key = f"test-stealth-{uuid.uuid4()}"

    request_body = {"pack_type": "decision", "inputs": {"question": "Test"}, "reservation": {"max_cost_usd": "0.1000"}}

    post_response = test_client.post(
        "/v1/runs",
        json=request_body,
        headers={"Authorization": f"Bearer {api_key_1}", "Idempotency-Key": idempotency_key},
    )

    assert post_response.status_code == 202
    run_id = post_response.json()["run_id"]

    # Tenant 2 tries to access Tenant 1's run
    get_response = test_client.get(
        f"/v1/runs/{run_id}",
        headers={"Authorization": f"Bearer {api_key_2}"},  # Different tenant!
    )

    # DEC-4204: Should return 404 (stealth), not 403
    assert get_response.status_code == 404
    assert "not found" in get_response.json()["detail"].lower()
