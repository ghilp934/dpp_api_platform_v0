"""Tests for API Key authentication."""

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy.orm import Session

from dpp_api.auth.api_key import hash_api_key, parse_api_key
from dpp_api.db.models import APIKey, Tenant
from dpp_api.db.repo_api_keys import APIKeyRepository
from dpp_api.db.repo_tenants import TenantRepository


def test_parse_api_key_valid():
    """Test parsing valid API key."""
    api_key = "sk_abc123_secret456"
    key_id, secret = parse_api_key(api_key)

    assert key_id == "abc123"
    assert secret == "secret456"


def test_parse_api_key_invalid_prefix():
    """Test parsing API key with invalid prefix."""
    with pytest.raises(ValueError, match="must start with 'sk_'"):
        parse_api_key("invalid_key")


def test_parse_api_key_invalid_format():
    """Test parsing API key with invalid format."""
    with pytest.raises(ValueError, match="must be in format"):
        parse_api_key("sk_only_two_parts")


def test_hash_api_key():
    """Test API key hashing."""
    api_key1 = "sk_test_secret123"
    api_key2 = "sk_test_secret123"
    api_key3 = "sk_test_different"

    hash1 = hash_api_key(api_key1)
    hash2 = hash_api_key(api_key2)
    hash3 = hash_api_key(api_key3)

    # Same key produces same hash
    assert hash1 == hash2

    # Different key produces different hash
    assert hash1 != hash3

    # Hash is SHA256 (64 hex chars)
    assert len(hash1) == 64


def test_api_key_repository_create(db_session: Session):
    """Test creating API key."""
    repo = APIKeyRepository(db_session)

    # Create tenant first
    tenant = Tenant(
        tenant_id="tenant_api_key_test",
        display_name="Test Tenant",
        status="ACTIVE",
    )
    db_session.add(tenant)
    db_session.commit()

    # Create API key
    api_key = "sk_test123_secret456"
    key_hash = hash_api_key(api_key)

    db_key = APIKey(
        key_id=str(uuid.uuid4()),
        tenant_id=tenant.tenant_id,
        key_hash=key_hash,
        label="Test Key",
        status="ACTIVE",
    )

    created = repo.create(db_key)

    assert created.key_id == db_key.key_id
    assert created.key_hash == key_hash
    assert created.tenant_id == tenant.tenant_id


def test_api_key_repository_get_active(db_session: Session):
    """Test getting active API key."""
    repo = APIKeyRepository(db_session)

    # Create tenant
    tenant = Tenant(
        tenant_id="tenant_active_test",
        display_name="Test Tenant",
        status="ACTIVE",
    )
    db_session.add(tenant)

    # Create active key
    active_key = APIKey(
        key_id=str(uuid.uuid4()),
        tenant_id=tenant.tenant_id,
        key_hash=hash_api_key("sk_active_secret"),
        status="ACTIVE",
    )
    db_session.add(active_key)

    # Create revoked key
    revoked_key = APIKey(
        key_id=str(uuid.uuid4()),
        tenant_id=tenant.tenant_id,
        key_hash=hash_api_key("sk_revoked_secret"),
        status="REVOKED",
    )
    db_session.add(revoked_key)
    db_session.commit()

    # Get active key
    found_active = repo.get_active_by_key_id(active_key.key_id)
    assert found_active is not None
    assert found_active.key_id == active_key.key_id

    # Revoked key not returned by get_active
    found_revoked = repo.get_active_by_key_id(revoked_key.key_id)
    assert found_revoked is None


def test_api_key_repository_update_last_used(db_session: Session):
    """Test updating last_used_at."""
    repo = APIKeyRepository(db_session)

    # Create tenant and key
    tenant = Tenant(
        tenant_id="tenant_last_used",
        display_name="Test Tenant",
        status="ACTIVE",
    )
    db_session.add(tenant)

    api_key = APIKey(
        key_id=str(uuid.uuid4()),
        tenant_id=tenant.tenant_id,
        key_hash=hash_api_key("sk_last_used_secret"),
        status="ACTIVE",
        last_used_at=None,
    )
    db_session.add(api_key)
    db_session.commit()

    # Update last_used_at
    repo.update_last_used(api_key.key_id)

    # Verify update
    updated = repo.get_by_key_id(api_key.key_id)
    assert updated.last_used_at is not None
    # SQLite doesn't preserve timezone info, so just check it was set recently
    # Convert both to naive datetimes for comparison
    now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    last_used_naive = updated.last_used_at.replace(tzinfo=None) if updated.last_used_at.tzinfo else updated.last_used_at
    assert (now_naive - last_used_naive).total_seconds() < 2


def test_api_key_repository_revoke(db_session: Session):
    """Test revoking API key."""
    repo = APIKeyRepository(db_session)

    # Create tenant and key
    tenant = Tenant(
        tenant_id="tenant_revoke",
        display_name="Test Tenant",
        status="ACTIVE",
    )
    db_session.add(tenant)

    api_key = APIKey(
        key_id=str(uuid.uuid4()),
        tenant_id=tenant.tenant_id,
        key_hash=hash_api_key("sk_revoke_secret"),
        status="ACTIVE",
    )
    db_session.add(api_key)
    db_session.commit()

    # Revoke with correct tenant
    success = repo.revoke(api_key.key_id, tenant.tenant_id)
    assert success is True

    # Verify revoked
    revoked = repo.get_by_key_id(api_key.key_id)
    assert revoked.status == "REVOKED"

    # Revoke with wrong tenant fails
    api_key2 = APIKey(
        key_id=str(uuid.uuid4()),
        tenant_id=tenant.tenant_id,
        key_hash=hash_api_key("sk_revoke2_secret"),
        status="ACTIVE",
    )
    db_session.add(api_key2)
    db_session.commit()

    failed = repo.revoke(api_key2.key_id, "wrong_tenant")
    assert failed is False
