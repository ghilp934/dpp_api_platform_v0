"""SQLAlchemy ORM Models for DPP."""

from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import BIGINT, DATE, FLOAT, JSON, TEXT, TIMESTAMP, UUID, Index, Integer
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

    # P1-7: Reservation parameters and inputs
    timebox_sec: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    min_reliability_score: Mapped[Optional[float]] = mapped_column(FLOAT, nullable=True)
    inputs_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

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

    # P1-10: Completion timestamp
    completed_at: Mapped[Optional[datetime]] = mapped_column(
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


class Plan(Base):
    """Plan model for API monetization tiers/products.

    Defines rate limits, allowed pack types, and cost constraints per plan.
    """

    __tablename__ = "plans"

    plan_id: Mapped[str] = mapped_column(TEXT, primary_key=True)
    name: Mapped[str] = mapped_column(TEXT, nullable=False)
    status: Mapped[str] = mapped_column(TEXT, nullable=False, default="ACTIVE")

    # Default profile version for this plan
    default_profile_version: Mapped[str] = mapped_column(TEXT, nullable=False, default="v0.4.2.2")

    # Features and limits (JSON fields)
    # features_json: {"allowed_pack_types": ["decision", "url"], "max_concurrent_runs": 10}
    features_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    # limits_json: {
    #   "rate_limit_post_per_min": 60,
    #   "rate_limit_poll_per_min": 300,
    #   "pack_type_limits": {"decision": {"max_cost_usd_micros": 1000000}}
    # }
    limits_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

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


class TenantPlan(Base):
    """TenantPlan model - maps tenants to their active plan.

    A tenant has exactly one active plan at any time.
    Audit trail for plan changes.
    """

    __tablename__ = "tenant_plans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(TEXT, nullable=False, index=True)
    plan_id: Mapped[str] = mapped_column(TEXT, nullable=False)

    status: Mapped[str] = mapped_column(TEXT, nullable=False, default="ACTIVE")

    effective_from: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    effective_to: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    # Audit fields
    changed_by: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    change_reason: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("idx_tenant_plans_tenant_status", "tenant_id", "status"),
        Index("idx_tenant_plans_effective", "tenant_id", "effective_from", "effective_to"),
    )


class TenantUsageDaily(Base):
    """TenantUsageDaily model - daily rollup of usage metrics per tenant.

    Usage metering for monetization analytics.
    Source of truth: RunRecord (no PII, only metadata).
    """

    __tablename__ = "tenant_usage_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(TEXT, nullable=False, index=True)
    usage_date: Mapped[date] = mapped_column(DATE, nullable=False)

    # Counts
    runs_count: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    success_count: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    fail_count: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)

    # Costs (DEC-4211: USD_MICROS only)
    cost_usd_micros_sum: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    reserved_usd_micros_sum: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)

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
        Index("idx_tenant_usage_daily_tenant_date", "tenant_id", "usage_date", unique=True),
    )
