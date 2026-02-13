"""API Key authentication for DPP."""

import hashlib
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from dpp_api.db.repo_api_keys import APIKeyRepository
from dpp_api.db.repo_tenants import TenantRepository
from dpp_api.db.session import get_db

security = HTTPBearer(auto_error=False)


class AuthContext:
    """Authentication context for a request."""

    def __init__(self, tenant_id: str, key_id: str):
        self.tenant_id = tenant_id
        self.key_id = key_id


def parse_api_key(api_key: str) -> tuple[str, str]:
    """
    Parse API key into key_id and secret.

    API Key format: sk_{key_id}_{secret}

    Args:
        api_key: API key string

    Returns:
        Tuple of (key_id, secret)

    Raises:
        ValueError: If API key format is invalid
    """
    if not api_key.startswith("sk_"):
        raise ValueError("API key must start with 'sk_'")

    parts = api_key.split("_")
    if len(parts) != 3:
        raise ValueError("API key must be in format 'sk_{key_id}_{secret}'")

    return parts[1], parts[2]


def hash_api_key(api_key: str) -> str:
    """
    Hash API key for storage.

    Args:
        api_key: Full API key (sk_{key_id}_{secret})

    Returns:
        SHA256 hash of the API key
    """
    return hashlib.sha256(api_key.encode()).hexdigest()


async def get_auth_context(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: Session = Depends(get_db),
) -> AuthContext:
    """
    Get authentication context from request.

    Validates API key and returns tenant_id and key_id.

    Args:
        request: FastAPI request
        credentials: HTTP Bearer credentials
        db: Database session

    Returns:
        AuthContext with tenant_id and key_id

    Raises:
        HTTPException: 401 if authentication fails
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    api_key = credentials.credentials

    # Parse API key
    try:
        key_id, secret = parse_api_key(api_key)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid API key format: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Get API key from database
    api_key_repo = APIKeyRepository(db)
    db_api_key = api_key_repo.get_active_by_key_id(key_id)

    if not db_api_key:
        # Stealth 401: Don't reveal if key exists
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Verify key hash
    provided_hash = hash_api_key(api_key)
    if provided_hash != db_api_key.key_hash:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Verify tenant is active
    tenant_repo = TenantRepository(db)
    tenant = tenant_repo.get_active_by_id(db_api_key.tenant_id)

    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Tenant not active",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Update last_used_at asynchronously (fire-and-forget)
    # In production, this should be done in background task or separate transaction
    try:
        api_key_repo.update_last_used(key_id)
    except Exception:
        # Log error but don't fail the request
        pass

    return AuthContext(tenant_id=db_api_key.tenant_id, key_id=key_id)


def require_owner(
    run_tenant_id: Optional[str], auth: AuthContext = Depends(get_auth_context)
) -> AuthContext:
    """
    Require that the authenticated tenant owns the resource.

    Implements "stealth 404" behavior: returns 404 instead of 403
    to avoid leaking information about resource existence.

    Args:
        run_tenant_id: Tenant ID of the resource (None if resource not found)
        auth: Authentication context

    Returns:
        AuthContext if authorized

    Raises:
        HTTPException: 404 if not authorized (stealth behavior)
    """
    if run_tenant_id is None:
        # Resource not found
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Resource not found",
        )

    if run_tenant_id != auth.tenant_id:
        # Not authorized, but return 404 (stealth)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Resource not found",
        )

    return auth
