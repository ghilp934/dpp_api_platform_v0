"""Tests for RFC 9457 exception handlers (P0-1).

Validates that HTTPException with dict detail fields are properly serialized
as JSON instead of str(dict).
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.exceptions import HTTPException


def test_http_exception_with_str_detail() -> None:
    """Test HTTPException with string detail returns RFC 9457 format."""
    app = FastAPI()

    @app.get("/test-str")
    async def test_endpoint_str():
        raise HTTPException(status_code=404, detail="Resource not found")

    # Import exception handler from main.py
    from dpp_api.main import http_exception_handler
    app.add_exception_handler(HTTPException, http_exception_handler)

    client = TestClient(app)
    response = client.get("/test-str")

    # Validate response
    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"

    data = response.json()
    assert data["type"] == "https://dpp.example.com/problems/http-404"
    assert data["title"] == "Not Found"
    assert data["status"] == 404
    assert data["detail"] == "Resource not found"  # String detail preserved
    assert data["instance"] == "/test-str"


def test_http_exception_with_dict_detail() -> None:
    """Test HTTPException with dict detail returns RFC 9457 format with structured detail.

    P0-1: This was previously broken - dict was converted to str(dict).
    Now it should be preserved as a proper JSON object.
    """
    app = FastAPI()

    @app.get("/test-dict")
    async def test_endpoint_dict():
        raise HTTPException(
            status_code=400,
            detail={
                "field": "email",
                "error": "Invalid email format",
                "provided": "not-an-email",
            }
        )

    # Import exception handler from main.py
    from dpp_api.main import http_exception_handler
    app.add_exception_handler(HTTPException, http_exception_handler)

    client = TestClient(app)
    response = client.get("/test-dict")

    # Validate response
    assert response.status_code == 400
    assert response.headers["content-type"] == "application/problem+json"

    data = response.json()
    assert data["type"] == "https://dpp.example.com/problems/http-400"
    assert data["title"] == "Bad Request"
    assert data["status"] == 400

    # P0-1 CRITICAL: detail should be a dict, NOT a string like "{'field': 'email', ...}"
    assert isinstance(data["detail"], dict), f"Expected dict, got {type(data['detail'])}"
    assert data["detail"]["field"] == "email"
    assert data["detail"]["error"] == "Invalid email format"
    assert data["detail"]["provided"] == "not-an-email"

    assert data["instance"] == "/test-dict"


def test_http_exception_with_none_detail() -> None:
    """Test HTTPException with None detail falls back to title."""
    app = FastAPI()

    @app.get("/test-none")
    async def test_endpoint_none():
        raise HTTPException(status_code=500, detail=None)

    # Import exception handler from main.py
    from dpp_api.main import http_exception_handler
    app.add_exception_handler(HTTPException, http_exception_handler)

    client = TestClient(app)
    response = client.get("/test-none")

    # Validate response
    assert response.status_code == 500
    assert response.headers["content-type"] == "application/problem+json"

    data = response.json()
    assert data["detail"] == "Internal Server Error"  # Fallback to title


def test_http_exception_with_nested_dict_detail() -> None:
    """Test HTTPException with nested dict detail (complex structured errors)."""
    app = FastAPI()

    @app.get("/test-nested")
    async def test_endpoint_nested():
        raise HTTPException(
            status_code=422,
            detail={
                "errors": [
                    {"field": "username", "message": "Already taken"},
                    {"field": "password", "message": "Too weak"},
                ],
                "suggestion": "Use a stronger password",
            }
        )

    # Import exception handler from main.py
    from dpp_api.main import http_exception_handler
    app.add_exception_handler(HTTPException, http_exception_handler)

    client = TestClient(app)
    response = client.get("/test-nested")

    # Validate response
    assert response.status_code == 422
    assert response.headers["content-type"] == "application/problem+json"

    data = response.json()
    assert isinstance(data["detail"], dict)
    assert "errors" in data["detail"]
    assert len(data["detail"]["errors"]) == 2
    assert data["detail"]["errors"][0]["field"] == "username"
    assert data["detail"]["suggestion"] == "Use a stronger password"
