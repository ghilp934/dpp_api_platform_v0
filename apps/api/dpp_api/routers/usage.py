"""Usage API router - GET /v1/tenants/{tenant_id}/usage.

STEP D: Usage API for tenant usage analytics.
"""

import logging
from datetime import date, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from dpp_api.auth.api_key import AuthContext, get_auth_context
from dpp_api.db.models import TenantUsageDaily
from dpp_api.db.session import get_db
from dpp_api.schemas import ProblemDetail, UsageDailySummary, UsageResponse

router = APIRouter(prefix="/v1/tenants", tags=["usage"])
logger = logging.getLogger(__name__)


@router.get("/{tenant_id}/usage", response_model=UsageResponse)
async def get_tenant_usage(
    tenant_id: str,
    from_date: str = Query(..., alias="from", description="Start date (YYYY-MM-DD, inclusive)"),
    to_date: str = Query(..., alias="to", description="End date (YYYY-MM-DD, inclusive)"),
    auth: AuthContext = Depends(get_auth_context),
    db: Session = Depends(get_db),
):
    """
    Get tenant usage statistics for a date range.

    Implements STEP D: Usage API
    - Date range query on tenant_usage_daily
    - Returns daily usage rollups
    - RFC 9457 Problem Details for validation errors
    """
    # DEC-4204: Owner guard - only allow tenant to see their own usage
    if auth.tenant_id != tenant_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tenant not found",
        )

    # Parse and validate dates
    try:
        from_date_obj = date.fromisoformat(from_date)
        to_date_obj = date.fromisoformat(to_date)
    except ValueError as e:
        # DEC-4213: RFC 9457 Problem Detail for validation error
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ProblemDetail(
                type="https://api.dpp.example/problems/invalid-date-format",
                title="Invalid Date Format",
                status=400,
                detail=f"Date must be in YYYY-MM-DD format: {e}",
                instance=f"/v1/tenants/{tenant_id}/usage",
            ).model_dump(),
        )

    # Validate date range
    if from_date_obj > to_date_obj:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ProblemDetail(
                type="https://api.dpp.example/problems/invalid-date-range",
                title="Invalid Date Range",
                status=400,
                detail=f"from_date ({from_date}) must be <= to_date ({to_date})",
                instance=f"/v1/tenants/{tenant_id}/usage",
            ).model_dump(),
        )

    # Query tenant_usage_daily for date range
    stmt = (
        select(TenantUsageDaily)
        .where(
            and_(
                TenantUsageDaily.tenant_id == tenant_id,
                TenantUsageDaily.usage_date >= from_date_obj,
                TenantUsageDaily.usage_date <= to_date_obj,
            )
        )
        .order_by(TenantUsageDaily.usage_date.asc())
    )

    result = db.execute(stmt)
    usage_records = result.scalars().all()

    # Build response
    daily_usage = [
        UsageDailySummary(
            usage_date=record.usage_date.isoformat(),
            runs_count=record.runs_count,
            success_count=record.success_count,
            fail_count=record.fail_count,
            cost_usd_micros_sum=record.cost_usd_micros_sum,
            reserved_usd_micros_sum=record.reserved_usd_micros_sum,
        )
        for record in usage_records
    ]

    logger.info(
        f"Retrieved usage for tenant {tenant_id} from {from_date} to {to_date}: "
        f"{len(daily_usage)} days"
    )

    return UsageResponse(
        tenant_id=tenant_id,
        from_date=from_date,
        to_date=to_date,
        daily_usage=daily_usage,
    )
