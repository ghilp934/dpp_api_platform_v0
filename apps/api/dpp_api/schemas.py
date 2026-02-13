"""Pydantic schemas for API requests/responses."""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


# ============================================================================
# POST /v1/runs - Request/Response
# ============================================================================


class RunReservation(BaseModel):
    """Reservation parameters for run submission."""

    max_cost_usd: str = Field(
        ..., description="Maximum cost in USD (4dp string)", pattern=r"^\d+(\.\d{1,4})?$"
    )
    timebox_sec: int = Field(default=90, ge=1, le=90, description="Execution timeout")
    min_reliability_score: float = Field(
        default=0.8, ge=0.0, le=1.0, description="Minimum reliability score"
    )


class RunMeta(BaseModel):
    """Metadata for run."""

    trace_id: Optional[str] = None
    profile_version: str = "v0.4.2.2"


class RunCreateRequest(BaseModel):
    """Request body for POST /v1/runs."""

    pack_type: str = Field(..., description="Pack type (decision, url, ocr, etc.)")
    inputs: dict[str, Any] = Field(..., description="Pack-specific inputs")
    reservation: RunReservation
    meta: Optional[RunMeta] = None


class PollInfo(BaseModel):
    """Polling information."""

    href: str
    recommended_interval_ms: int = 1500
    max_wait_sec: int = 90


class RunReceipt(BaseModel):
    """Response for POST /v1/runs (202 Accepted)."""

    run_id: str
    status: str
    poll: PollInfo
    reservation: dict[str, str]
    meta: dict[str, Any]


# ============================================================================
# GET /v1/runs/{run_id} - Response
# ============================================================================


class CostInfo(BaseModel):
    """Cost information."""

    reserved_usd: str
    used_usd: str
    minimum_fee_usd: str
    budget_remaining_usd: str


class ResultInfo(BaseModel):
    """Result information for completed runs."""

    presigned_url: Optional[str] = None
    sha256: Optional[str] = None
    expires_at: Optional[datetime] = None


class ErrorInfo(BaseModel):
    """Error information for failed runs."""

    reason_code: str
    detail: str


class RunStatusResponse(BaseModel):
    """Response for GET /v1/runs/{run_id}."""

    run_id: str
    status: str
    money_state: str
    cost: CostInfo
    result: Optional[ResultInfo] = None
    error: Optional[ErrorInfo] = None
    meta: dict[str, Any]
