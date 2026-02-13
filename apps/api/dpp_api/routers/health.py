"""Health check endpoints."""

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class HealthResponse(BaseModel):
    """Health check response model."""

    status: str
    version: str
    services: dict[str, str]


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    Health check endpoint.

    Returns service status and dependency health.
    """
    # TODO: add actual health checks for DB, Redis, S3, SQS
    return HealthResponse(
        status="healthy",
        version="0.4.2.2",
        services={
            "api": "up",
            "database": "unknown",
            "redis": "unknown",
            "s3": "unknown",
            "sqs": "unknown",
        },
    )


@router.get("/readyz", response_model=HealthResponse)
async def readiness_check() -> HealthResponse:
    """
    Readiness check endpoint.

    Returns whether the service is ready to accept requests.
    """
    # TODO: add actual readiness checks
    return HealthResponse(
        status="ready",
        version="0.4.2.2",
        services={
            "api": "up",
            "database": "unknown",
            "redis": "unknown",
            "s3": "unknown",
            "sqs": "unknown",
        },
    )
