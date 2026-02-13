"""Pack Envelope generation (S3 저장 포맷)."""

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from dpp_api.utils.money import usd_micros_to_decimal


def create_pack_envelope(
    run_id: str,
    pack_type: str,
    status: str,
    reserved_usd_micros: int,
    used_usd_micros: int,
    minimum_fee_usd_micros: int,
    envelope_data: dict[str, Any],
    trace_id: str | None = None,
) -> str:
    """Create pack_envelope.json content.

    Args:
        run_id: Run ID
        pack_type: Pack type
        status: COMPLETED or FAILED
        reserved_usd_micros: Reserved cost in USD_MICROS
        used_usd_micros: Used cost in USD_MICROS
        minimum_fee_usd_micros: Minimum fee in USD_MICROS
        envelope_data: Pack-specific data from executor
        trace_id: Optional trace ID

    Returns:
        JSON string of pack_envelope
    """
    # Convert micros to 4dp decimal strings
    reserved_usd = str(usd_micros_to_decimal(reserved_usd_micros))
    used_usd = str(usd_micros_to_decimal(used_usd_micros))
    minimum_fee_usd = str(usd_micros_to_decimal(minimum_fee_usd_micros))

    envelope = {
        "schema_version": "0.4.2.2",
        "run_id": run_id,
        "pack_type": pack_type,
        "status": status,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cost": {
            "reserved_usd": reserved_usd,
            "used_usd": used_usd,
            "minimum_fee_usd": minimum_fee_usd,
        },
        "data": envelope_data.get("data", {}),
        "artifacts": envelope_data.get("artifacts", {}),
        "logs": envelope_data.get("logs", {"discard_log": [], "blocked_log": []}),
        "meta": {
            "trace_id": trace_id or "",
            "profile_version": "PROFILE_DPP_0_4_2_2",
        },
    }

    # Canonical JSON (keys sorted, no whitespace)
    envelope_json = json.dumps(envelope, indent=2, ensure_ascii=False)

    return envelope_json


def compute_envelope_sha256(envelope_json: str) -> str:
    """Compute SHA-256 hash of envelope JSON.

    Args:
        envelope_json: Pack envelope JSON string

    Returns:
        SHA-256 hash (hex string)
    """
    return hashlib.sha256(envelope_json.encode("utf-8")).hexdigest()
