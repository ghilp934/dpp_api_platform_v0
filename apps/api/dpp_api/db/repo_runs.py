"""Repository for Run entity with DEC-4210 optimistic locking."""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dpp_api.db.models import Run


class RunRepository:
    """Repository for Run operations with optimistic locking (DEC-4210)."""

    def __init__(self, db: Session):
        self.db = db

    def create(self, run: Run) -> Run:
        """
        Create a new run.

        Args:
            run: Run instance to create

        Returns:
            Created run
        """
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def get_by_id(self, run_id: str, tenant_id: str) -> Optional[Run]:
        """
        Get run by ID with tenant ownership check.

        Args:
            run_id: Run ID
            tenant_id: Tenant ID for ownership verification

        Returns:
            Run if found and owned by tenant, None otherwise
        """
        stmt = select(Run).where(Run.run_id == run_id, Run.tenant_id == tenant_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_by_idempotency_key(
        self, tenant_id: str, idempotency_key: str
    ) -> Optional[Run]:
        """
        Get run by idempotency key.

        Args:
            tenant_id: Tenant ID
            idempotency_key: Idempotency key

        Returns:
            Run if found, None otherwise
        """
        stmt = select(Run).where(
            Run.tenant_id == tenant_id, Run.idempotency_key == idempotency_key
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def update_with_version_check(
        self,
        run_id: str,
        tenant_id: str,
        expected_version: int,
        updates: dict,
    ) -> bool:
        """
        Update run with optimistic locking (DEC-4210).

        Args:
            run_id: Run ID
            tenant_id: Tenant ID for ownership verification
            expected_version: Expected version for optimistic locking
            updates: Dictionary of fields to update

        Returns:
            True if update succeeded (1 row affected), False if version mismatch (0 rows)
        """
        # Add version increment and updated_at
        updates["version"] = expected_version + 1
        updates["updated_at"] = datetime.now(timezone.utc)

        # Build UPDATE with WHERE version=expected_version
        stmt = (
            update(Run)
            .where(
                Run.run_id == run_id,
                Run.tenant_id == tenant_id,
                Run.version == expected_version,
            )
            .values(**updates)
        )

        result = self.db.execute(stmt)
        self.db.commit()

        # DEC-4210: 0 rows affected = loser, already finalized
        return result.rowcount == 1

    def claim_for_processing(
        self, run_id: str, lease_token: str, lease_expires_at: datetime
    ) -> Optional[Run]:
        """
        Claim a run for processing by setting lease.

        Args:
            run_id: Run ID
            lease_token: Lease token (UUID)
            lease_expires_at: Lease expiration time

        Returns:
            Run if claimed successfully, None if already claimed or not found
        """
        stmt = (
            update(Run)
            .where(Run.run_id == run_id, Run.status == "QUEUED")
            .values(
                status="PROCESSING",
                lease_token=lease_token,
                lease_expires_at=lease_expires_at,
                updated_at=datetime.now(timezone.utc),
            )
        )

        result = self.db.execute(stmt)
        self.db.commit()

        # If update succeeded, fetch and return the run
        if result.rowcount == 1:
            return self.db.get(Run, run_id)
        return None

    def claim_for_finalize(
        self, run_id: str, tenant_id: str, expected_version: int, finalize_token: str
    ) -> bool:
        """
        Claim a run for finalization (DEC-4210 2-phase finalize).

        Args:
            run_id: Run ID
            tenant_id: Tenant ID
            expected_version: Expected version for optimistic locking
            finalize_token: Finalize token (UUID)

        Returns:
            True if claimed (winner), False if lost race
        """
        return self.update_with_version_check(
            run_id=run_id,
            tenant_id=tenant_id,
            expected_version=expected_version,
            updates={
                "finalize_stage": "CLAIMED",
                "finalize_token": finalize_token,
                "finalize_claimed_at": datetime.now(timezone.utc),
            },
        )

    def list_expired_leases(self, limit: int = 100) -> list[Run]:
        """
        List runs with expired leases (for reaper).

        Args:
            limit: Maximum number of runs to return

        Returns:
            List of runs with expired leases
        """
        now = datetime.now(timezone.utc)
        stmt = (
            select(Run)
            .where(
                Run.status == "PROCESSING",
                Run.lease_expires_at < now,
            )
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())
