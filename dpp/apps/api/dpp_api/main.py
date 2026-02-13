"""DPP API - FastAPI Application Entry Point."""

import logging
import os
import uuid

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from dpp_api.context import request_id_var
from dpp_api.enforce import PlanViolationError
from dpp_api.routers import health, runs, usage
from dpp_api.schemas import ProblemDetail
from dpp_api.utils import configure_json_logging

app = FastAPI(
    title="DPP API",
    description="Decision Pack Platform - Agent-Centric API Platform",
    version="0.4.2.2",
    docs_url="/docs",
    redoc_url="/redoc",
)

# P1-9: Configure structured JSON logging
# Set DPP_JSON_LOGS=false to disable (defaults to true for production)
if os.getenv("DPP_JSON_LOGS", "true").lower() != "false":
    configure_json_logging(log_level=os.getenv("LOG_LEVEL", "INFO"))
    logger = logging.getLogger(__name__)
    logger.info("Structured JSON logging enabled")

# CORS middleware (adjust for production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# P1-9: Request ID Middleware
# ============================================================================


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """Generate and propagate request_id for observability.

    P1-9: Each request gets a unique request_id (UUID v4).
    - Accepts X-Request-ID header from client (optional)
    - Generates new UUID if not provided
    - Sets context variable for logging
    - Returns X-Request-ID in response headers
    """
    # Get or generate request_id
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())

    # Set context variable for logging
    request_id_var.set(request_id)

    # Process request
    response = await call_next(request)

    # Add request_id to response headers
    response.headers["X-Request-ID"] = request_id

    return response


# ============================================================================
# RFC 9457 Global Exception Handlers
# ============================================================================


@app.exception_handler(PlanViolationError)
async def plan_violation_handler(request: Request, exc: PlanViolationError) -> JSONResponse:
    """Handle plan violation errors with RFC 9457 Problem Details format.

    Returns application/problem+json with plan-specific error details.
    P1-8: Includes Retry-After header for 429 responses.
    """
    problem = ProblemDetail(
        type=exc.error_type,
        title=exc.title,
        status=exc.status_code,
        detail=exc.detail,
        instance=request.url.path,
    )

    headers = {}
    # P1-8: Add Retry-After header for rate limit errors
    if exc.status_code == 429 and "Retry after" in exc.detail:
        # Extract TTL from detail message (format: "Retry after {ttl} seconds")
        import re
        match = re.search(r"Retry after (\d+) seconds", exc.detail)
        if match:
            headers["Retry-After"] = match.group(1)

    return JSONResponse(
        status_code=exc.status_code,
        content=problem.model_dump(exclude_none=True),
        media_type="application/problem+json",
        headers=headers,
    )


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    """Handle HTTP exceptions with RFC 9457 Problem Details format.

    Returns application/problem+json with top-level RFC 9457 fields.
    No {"detail": ...} wrapper.

    P0-1: Preserves dict detail fields for structured error responses (RFC 9457 compliant).
    """
    # P0-1: Don't force-cast detail to str - preserve dict if provided
    detail_value = exc.detail if exc.detail is not None else _get_title_for_status(exc.status_code)

    problem = ProblemDetail(
        type=f"https://dpp.example.com/problems/http-{exc.status_code}",
        title=_get_title_for_status(exc.status_code),
        status=exc.status_code,
        detail=detail_value,
        instance=request.url.path,
    )

    return JSONResponse(
        status_code=exc.status_code,
        content=problem.model_dump(exclude_none=True),
        media_type="application/problem+json",
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Handle request validation errors with RFC 9457 Problem Details format.

    Returns 400 Bad Request with application/problem+json.
    """
    # Extract first error for detail message
    first_error = exc.errors()[0] if exc.errors() else {}
    field = ".".join(str(loc) for loc in first_error.get("loc", []))
    msg = first_error.get("msg", "Validation error")

    problem = ProblemDetail(
        type="https://dpp.example.com/problems/validation-error",
        title="Request Validation Failed",
        status=status.HTTP_400_BAD_REQUEST,
        detail=f"Invalid field '{field}': {msg}",
        instance=request.url.path,
    )

    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=problem.model_dump(exclude_none=True),
        media_type="application/problem+json",
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle uncaught exceptions with RFC 9457 Problem Details format.

    Returns 500 Internal Server Error with application/problem+json.
    """
    problem = ProblemDetail(
        type="https://dpp.example.com/problems/internal-error",
        title="Internal Server Error",
        status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="An unexpected error occurred. Please try again later.",
        instance=request.url.path,
    )

    # Log the actual exception for debugging (TODO: add structured logging)
    import logging
    logger = logging.getLogger(__name__)
    logger.error(f"Unhandled exception: {exc}", exc_info=True)

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=problem.model_dump(exclude_none=True),
        media_type="application/problem+json",
    )


def _get_title_for_status(status_code: int) -> str:
    """Get human-readable title for HTTP status code."""
    titles = {
        400: "Bad Request",
        401: "Unauthorized",
        402: "Payment Required",
        403: "Forbidden",
        404: "Not Found",
        409: "Conflict",
        429: "Too Many Requests",
        500: "Internal Server Error",
        502: "Bad Gateway",
        503: "Service Unavailable",
    }
    return titles.get(status_code, f"HTTP {status_code}")


# Include routers
app.include_router(health.router, tags=["health"])
app.include_router(runs.router)  # API-01: Runs endpoints
app.include_router(usage.router)  # STEP D: Usage analytics


@app.get("/")
async def root() -> dict[str, str]:
    """Root endpoint."""
    return {
        "service": "DPP API",
        "version": "0.4.2.2",
        "status": "running",
    }
