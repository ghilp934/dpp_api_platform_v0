"""Repository for Plan and TenantPlan operations."""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from dpp_api.db.models import Plan, TenantPlan


class PlanRepository:
    """Repository for Plan operations."""

    def __init__(self, db: Session):
        self.db = db

    def get_by_id(self, plan_id: str) -> Optional[Plan]:
        """Get a plan by ID."""
        stmt = select(Plan).where(Plan.plan_id == plan_id)
        result = self.db.execute(stmt)
        return result.scalar_one_or_none()

    def create(self, plan: Plan) -> Plan:
        """Create a new plan."""
        self.db.add(plan)
        self.db.commit()
        self.db.refresh(plan)
        return plan


class TenantPlanRepository:
    """Repository for TenantPlan operations."""

    def __init__(self, db: Session):
        self.db = db

    def get_active_plan(self, tenant_id: str) -> Optional[Plan]:
        """Get the currently active plan for a tenant.

        Returns the Plan object, not the TenantPlan mapping.
        """
        now = datetime.now(timezone.utc)

        # Find active TenantPlan mapping
        stmt = (
            select(TenantPlan)
            .where(
                and_(
                    TenantPlan.tenant_id == tenant_id,
                    TenantPlan.status == "ACTIVE",
                    TenantPlan.effective_from <= now,
                    # effective_to is NULL or in the future
                    (TenantPlan.effective_to.is_(None)) | (TenantPlan.effective_to > now),
                )
            )
            .order_by(TenantPlan.effective_from.desc())
            .limit(1)
        )

        result = self.db.execute(stmt)
        tenant_plan = result.scalar_one_or_none()

        if not tenant_plan:
            return None

        # Fetch the associated Plan
        plan_stmt = select(Plan).where(Plan.plan_id == tenant_plan.plan_id)
        plan_result = self.db.execute(plan_stmt)
        return plan_result.scalar_one_or_none()

    def assign_plan(
        self,
        tenant_id: str,
        plan_id: str,
        changed_by: Optional[str] = None,
        change_reason: Optional[str] = None,
    ) -> TenantPlan:
        """Assign a plan to a tenant.

        This will expire any existing active plans and create a new active mapping.
        """
        now = datetime.now(timezone.utc)

        # Expire existing active plans
        stmt = (
            select(TenantPlan)
            .where(
                and_(
                    TenantPlan.tenant_id == tenant_id,
                    TenantPlan.status == "ACTIVE",
                    TenantPlan.effective_to.is_(None),
                )
            )
        )

        result = self.db.execute(stmt)
        existing_plans = result.scalars().all()

        for existing in existing_plans:
            existing.status = "EXPIRED"
            existing.effective_to = now

        # Create new active plan
        new_tenant_plan = TenantPlan(
            tenant_id=tenant_id,
            plan_id=plan_id,
            status="ACTIVE",
            effective_from=now,
            effective_to=None,
            changed_by=changed_by,
            change_reason=change_reason,
        )

        self.db.add(new_tenant_plan)
        self.db.flush()  # Flush to generate ID before commit
        self.db.commit()
        self.db.refresh(new_tenant_plan)

        return new_tenant_plan
