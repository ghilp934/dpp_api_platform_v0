"""Usage tracking and metering for API monetization.

Implements STEP C: Metering pipeline
- RunRecord completion → tenant_usage_daily update
- UPSERT (ON CONFLICT) logic
- Incremental updates: runs_count++, cost_sum+=actual_cost
"""

import logging
from datetime import date, datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from dpp_api.db.models import Run

logger = logging.getLogger(__name__)


class UsageTracker:
    """Usage tracking service for metering run completions."""

    def __init__(self, db: Session):
        self.db = db

    def record_run_completion(self, run: Run) -> None:
        """Record a completed run in tenant_usage_daily.

        Uses UPSERT (ON CONFLICT DO UPDATE) for atomic incremental updates.

        Args:
            run: Completed Run object (status=COMPLETED or FAILED)
        """
        from sqlalchemy import select
        from sqlalchemy.dialects.postgresql import insert

        # Extract data from run
        tenant_id = run.tenant_id
        usage_date = run.created_at.date() if run.created_at else date.today()

        # Determine success/fail
        is_success = run.status == "COMPLETED"
        success_count = 1 if is_success else 0
        fail_count = 0 if is_success else 1

        # Get actual cost (may be None for some failures)
        actual_cost = run.actual_cost_usd_micros or 0
        reserved_cost = run.reservation_max_cost_usd_micros or 0

        now = datetime.now(timezone.utc)

        # Check database dialect
        dialect_name = self.db.bind.dialect.name if self.db.bind else "unknown"

        if dialect_name == "sqlite":
            # SQLite: Use simpler SELECT + INSERT or UPDATE approach
            # Query existing record
            from dpp_api.db.models import TenantUsageDaily

            stmt = select(TenantUsageDaily).where(
                (TenantUsageDaily.tenant_id == tenant_id)
                & (TenantUsageDaily.usage_date == usage_date)
            )
            result = self.db.execute(stmt)
            existing = result.scalar_one_or_none()

            if existing:
                # Update existing
                existing.runs_count += 1
                existing.success_count += success_count
                existing.fail_count += fail_count
                existing.cost_usd_micros_sum += actual_cost
                existing.reserved_usd_micros_sum += reserved_cost
                existing.updated_at = now
            else:
                # Create new
                new_usage = TenantUsageDaily(
                    tenant_id=tenant_id,
                    usage_date=usage_date,
                    runs_count=1,
                    success_count=success_count,
                    fail_count=fail_count,
                    cost_usd_micros_sum=actual_cost,
                    reserved_usd_micros_sum=reserved_cost,
                    created_at=now,
                    updated_at=now,
                )
                self.db.add(new_usage)

            self.db.commit()

        else:
            # PostgreSQL: Use ON CONFLICT DO UPDATE (UPSERT)
            upsert_sql = text(
                """
                INSERT INTO tenant_usage_daily (
                    tenant_id,
                    usage_date,
                    runs_count,
                    success_count,
                    fail_count,
                    cost_usd_micros_sum,
                    reserved_usd_micros_sum,
                    created_at,
                    updated_at
                ) VALUES (
                    :tenant_id,
                    :usage_date,
                    1,
                    :success_count,
                    :fail_count,
                    :actual_cost,
                    :reserved_cost,
                    :now,
                    :now
                )
                ON CONFLICT (tenant_id, usage_date)
                DO UPDATE SET
                    runs_count = tenant_usage_daily.runs_count + 1,
                    success_count = tenant_usage_daily.success_count + :success_count,
                    fail_count = tenant_usage_daily.fail_count + :fail_count,
                    cost_usd_micros_sum = tenant_usage_daily.cost_usd_micros_sum + :actual_cost,
                    reserved_usd_micros_sum = tenant_usage_daily.reserved_usd_micros_sum + :reserved_cost,
                    updated_at = :now
                """
            )

            self.db.execute(
                upsert_sql,
                {
                    "tenant_id": tenant_id,
                    "usage_date": usage_date,
                    "success_count": success_count,
                    "fail_count": fail_count,
                    "actual_cost": actual_cost,
                    "reserved_cost": reserved_cost,
                    "now": now,
                },
            )
            self.db.commit()

        logger.info(
            f"Recorded usage for tenant {tenant_id} on {usage_date}: "
            f"run_id={run.run_id}, status={run.status}, cost={actual_cost} micros"
        )
