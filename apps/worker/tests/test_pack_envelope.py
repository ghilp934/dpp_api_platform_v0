"""Tests for pack_envelope generation."""

import json
import sys
from pathlib import Path

# Add API path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "api"))

from dpp_worker.pack_envelope import compute_envelope_sha256, create_pack_envelope


def test_create_pack_envelope():
    """Test pack_envelope.json generation."""
    run_id = "test-run-123"
    pack_type = "decision"
    status = "COMPLETED"
    reserved_usd_micros = 2_000_000  # $2.00
    used_usd_micros = 500_000  # $0.50
    minimum_fee_usd_micros = 50_000  # $0.05

    envelope_data = {
        "data": {
            "answer_text": "Test answer",
            "confidence": 0.85,
        },
        "artifacts": {},
        "logs": {
            "discard_log": [],
            "blocked_log": [],
        },
    }

    envelope_json = create_pack_envelope(
        run_id=run_id,
        pack_type=pack_type,
        status=status,
        reserved_usd_micros=reserved_usd_micros,
        used_usd_micros=used_usd_micros,
        minimum_fee_usd_micros=minimum_fee_usd_micros,
        envelope_data=envelope_data,
        trace_id="trace-123",
    )

    # Parse and verify
    envelope = json.loads(envelope_json)

    assert envelope["schema_version"] == "0.4.2.2"
    assert envelope["run_id"] == run_id
    assert envelope["pack_type"] == pack_type
    assert envelope["status"] == status

    # Verify cost (4dp decimal strings)
    assert envelope["cost"]["reserved_usd"] == "2.0000"
    assert envelope["cost"]["used_usd"] == "0.5000"
    assert envelope["cost"]["minimum_fee_usd"] == "0.0500"

    # Verify data
    assert envelope["data"]["answer_text"] == "Test answer"
    assert envelope["data"]["confidence"] == 0.85

    # Verify meta
    assert envelope["meta"]["trace_id"] == "trace-123"
    assert envelope["meta"]["profile_version"] == "PROFILE_DPP_0_4_2_2"


def test_compute_envelope_sha256():
    """Test SHA-256 hash computation."""
    envelope_json = '{"test": "data"}'

    sha256 = compute_envelope_sha256(envelope_json)

    # Verify it's a valid hex string
    assert len(sha256) == 64
    assert all(c in "0123456789abcdef" for c in sha256)

    # Verify determinism
    sha256_2 = compute_envelope_sha256(envelope_json)
    assert sha256 == sha256_2
