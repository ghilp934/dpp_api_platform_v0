"""Smoke tests for DPP API - MS-0 validation."""

import pytest
from fastapi.testclient import TestClient

from dpp_api.main import app

client = TestClient(app)


def test_root_endpoint() -> None:
    """Test root endpoint returns service info."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "DPP API"
    assert data["version"] == "0.4.2.2"
    assert data["status"] == "running"


def test_health_endpoint() -> None:
    """Test health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["version"] == "0.4.2.2"
    assert "services" in data
    assert data["services"]["api"] == "up"


def test_readyz_endpoint() -> None:
    """Test readiness check endpoint."""
    response = client.get("/readyz")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ready"
    assert data["version"] == "0.4.2.2"


def test_openapi_docs_available() -> None:
    """Test OpenAPI docs are accessible."""
    response = client.get("/docs")
    assert response.status_code == 200


def test_redoc_available() -> None:
    """Test ReDoc is accessible."""
    response = client.get("/redoc")
    assert response.status_code == 200


def test_openapi_schema() -> None:
    """Test OpenAPI schema is valid."""
    response = client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    assert schema["info"]["title"] == "DPP API"
    assert schema["info"]["version"] == "0.4.2.2"
