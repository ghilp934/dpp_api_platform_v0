"""Hashing utilities for idempotency and integrity."""

import hashlib
import json
from typing import Any


def compute_payload_hash(payload: dict[str, Any], exclude_keys: set[str] | None = None) -> str:
    """
    Compute SHA-256 hash of request payload for idempotency.

    Uses canonical JSON (sorted keys, no whitespace) to ensure deterministic hashing.

    Args:
        payload: Request payload dictionary
        exclude_keys: Keys to exclude from hash (e.g., trace_id, client_version)

    Returns:
        SHA-256 hash (hex string)

    Examples:
        >>> compute_payload_hash({"pack_type": "decision", "inputs": {"q": "test"}})
        'abc123...'
    """
    if exclude_keys is None:
        exclude_keys = {"trace_id", "client_version", "client_name"}

    # Create a filtered copy without excluded keys
    filtered = _recursive_filter(payload, exclude_keys)

    # Canonical JSON: sorted keys, no whitespace
    canonical_json = json.dumps(filtered, sort_keys=True, separators=(",", ":"))

    # SHA-256 hash
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _recursive_filter(obj: Any, exclude_keys: set[str]) -> Any:
    """Recursively filter out excluded keys from nested dict/list."""
    if isinstance(obj, dict):
        return {k: _recursive_filter(v, exclude_keys) for k, v in obj.items() if k not in exclude_keys}
    elif isinstance(obj, list):
        return [_recursive_filter(item, exclude_keys) for item in obj]
    else:
        return obj
