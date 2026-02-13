"""Repository for Tenant entity."""

from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from dpp_api.db.models import Tenant


class TenantRepository:
    """Repository for Tenant operations."""

    def __init__(self, db: Session):
        self.db = db

    def create(self, tenant: Tenant) -> Tenant:
        """
        Create a new tenant.

        Args:
            tenant: Tenant instance to create

        Returns:
            Created tenant
        """
        self.db.add(tenant)
        self.db.commit()
        self.db.refresh(tenant)
        return tenant

    def get_by_id(self, tenant_id: str) -> Optional[Tenant]:
        """
        Get tenant by ID.

        Args:
            tenant_id: Tenant ID

        Returns:
            Tenant if found, None otherwise
        """
        stmt = select(Tenant).where(Tenant.tenant_id == tenant_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_active_by_id(self, tenant_id: str) -> Optional[Tenant]:
        """
        Get active tenant by ID.

        Args:
            tenant_id: Tenant ID

        Returns:
            Tenant if found and active, None otherwise
        """
        stmt = select(Tenant).where(
            Tenant.tenant_id == tenant_id, Tenant.status == "ACTIVE"
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def list_all(self) -> list[Tenant]:
        """
        List all tenants.

        Returns:
            List of tenants
        """
        stmt = select(Tenant)
        return list(self.db.execute(stmt).scalars().all())
