"""SQLAlchemy ORM Models for DPP."""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import BIGINT, TEXT, TIMESTAMP, UUID, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all ORM models."""

    pass


class Tenant(Base):
    """Tenant model for multi-tenancy."""

    __tablename__ = "tenants"

    tenant_id: Mapped[str] = mapped_column(TEXT, primary_key=True)
    display_name: Mapped[str] = mapped_column(TEXT, nullable=False)
    status: Mapped[str] = mapped_column(TEXT, nullable=False, default="ACTIVE")
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class APIKey(Base):
    """API Key model for authentication."""

    __tablename__ = "api_keys"

    key_id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(
        TEXT, nullable=False, index=True
    )  # FK to tenants
    key_hash: Mapped[str] = mapped_column(TEXT, nullable=False)
    label: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    status: Mapped[str] = mapped_column(TEXT, nullable=False, default="ACTIVE")
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    last_used_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    __table_args__ = (Index("idx_api_keys_tenant", "tenant_id"),)


class Run(Base):
    """Run model - authoritative state for async executions."""

    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(
        TEXT, nullable=False, index=True
    )  # FK to tenants

    pack_type: Mapped[str] = mapped_column(TEXT, nullable=False)
    profile_version: Mapped[str] = mapped_column(TEXT, nullable=False, default="v0.4.2.2")

    # Execution state
    status: Mapped[str] = mapped_column(TEXT, nullable=False)  # QUEUED/PROCESSING/etc
    money_state: Mapped[str] = mapped_column(TEXT, nullable=False)  # NONE/RESERVED/etc

    # Idempotency
    idempotency_key: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    payload_hash: Mapped[str] = mapped_column(TEXT, nullable=False)

    # DEC-4210: Optimistic locking
    version: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)

    # DEC-4211: Money in USD_MICROS (BIGINT)
    reservation_max_cost_usd_micros: Mapped[int] = mapped_column(BIGINT, nullable=False)
    actual_cost_usd_micros: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    minimum_fee_usd_micros: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)

    # Result persistence
    result_bucket: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    result_key: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    result_sha256: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    retention_until: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )

    # Lease management (zombie protection)
    lease_token: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    # Finalize stage (2-phase commit)
    finalize_token: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    finalize_stage: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    finalize_claimed_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    # Error tracking
    last_error_reason_code: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    last_error_detail: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)

    # Observability
    trace_id: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("idx_runs_tenant_created", "tenant_id", "created_at"),
        Index("idx_runs_status_lease", "status", "lease_expires_at"),
        Index("idx_runs_idem", "tenant_id", "idempotency_key"),
    )
