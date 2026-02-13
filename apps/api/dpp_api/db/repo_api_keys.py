"""Repository for APIKey entity."""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dpp_api.db.models import APIKey


class APIKeyRepository:
    """Repository for API Key operations."""

    def __init__(self, db: Session):
        self.db = db

    def create(self, api_key: APIKey) -> APIKey:
        """
        Create a new API key.

        Args:
            api_key: APIKey instance to create

        Returns:
            Created API key
        """
        self.db.add(api_key)
        self.db.commit()
        self.db.refresh(api_key)
        return api_key

    def get_by_key_id(self, key_id: str) -> Optional[APIKey]:
        """
        Get API key by key_id.

        Args:
            key_id: API key ID (UUID)

        Returns:
            APIKey if found, None otherwise
        """
        stmt = select(APIKey).where(APIKey.key_id == key_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_active_by_key_id(self, key_id: str) -> Optional[APIKey]:
        """
        Get active API key by key_id.

        Args:
            key_id: API key ID (UUID)

        Returns:
            APIKey if found and active, None otherwise
        """
        stmt = select(APIKey).where(APIKey.key_id == key_id, APIKey.status == "ACTIVE")
        return self.db.execute(stmt).scalar_one_or_none()

    def update_last_used(self, key_id: str) -> None:
        """
        Update last_used_at timestamp for API key.

        Args:
            key_id: API key ID
        """
        stmt = (
            update(APIKey)
            .where(APIKey.key_id == key_id)
            .values(last_used_at=datetime.now(timezone.utc))
        )
        self.db.execute(stmt)
        self.db.commit()

    def list_by_tenant(self, tenant_id: str) -> list[APIKey]:
        """
        List all API keys for a tenant.

        Args:
            tenant_id: Tenant ID

        Returns:
            List of API keys
        """
        stmt = select(APIKey).where(APIKey.tenant_id == tenant_id)
        return list(self.db.execute(stmt).scalars().all())

    def revoke(self, key_id: str, tenant_id: str) -> bool:
        """
        Revoke an API key (set status to REVOKED).

        Args:
            key_id: API key ID
            tenant_id: Tenant ID for ownership verification

        Returns:
            True if revoked, False if not found or not owned by tenant
        """
        stmt = (
            update(APIKey)
            .where(APIKey.key_id == key_id, APIKey.tenant_id == tenant_id)
            .values(status="REVOKED")
        )
        result = self.db.execute(stmt)
        self.db.commit()
        return result.rowcount == 1
