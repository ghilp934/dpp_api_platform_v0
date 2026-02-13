"""Tests for API key format validation (P0-3).

Ensures API keys are generated and parsed correctly.
"""

import uuid

import pytest

from dpp_api.auth.api_key import hash_api_key, parse_api_key


def test_parse_api_key_valid() -> None:
    """Test parsing valid API key in format sk_{key_id}_{secret}."""
    key_id = str(uuid.uuid4())
    secret = uuid.uuid4().hex[:32]
    api_key = f"sk_{key_id}_{secret}"

    parsed_key_id, parsed_secret = parse_api_key(api_key)

    assert parsed_key_id == key_id
    assert parsed_secret == secret


def test_parse_api_key_invalid_prefix() -> None:
    """Test parsing API key with invalid prefix raises ValueError."""
    api_key = "dpp_test_12345678"  # P0-3: Old incorrect format

    with pytest.raises(ValueError, match="must start with 'sk_'"):
        parse_api_key(api_key)


def test_parse_api_key_invalid_format() -> None:
    """Test parsing API key with invalid format raises ValueError."""
    api_key = "sk_only_two_parts"  # Missing secret

    with pytest.raises(ValueError, match="must be in format"):
        parse_api_key(api_key)


def test_parse_api_key_too_many_underscores() -> None:
    """Test parsing API key with extra underscores raises ValueError."""
    api_key = "sk_part1_part2_part3_extra"  # Too many parts

    with pytest.raises(ValueError, match="must be in format"):
        parse_api_key(api_key)


def test_hash_api_key_deterministic() -> None:
    """Test API key hashing is deterministic."""
    api_key = "sk_test-key-id_test-secret-12345"

    hash1 = hash_api_key(api_key)
    hash2 = hash_api_key(api_key)

    assert hash1 == hash2
    assert len(hash1) == 64  # SHA256 produces 64 hex chars


def test_hash_api_key_different_for_different_keys() -> None:
    """Test different API keys produce different hashes."""
    api_key1 = "sk_key1_secret1"
    api_key2 = "sk_key2_secret2"

    hash1 = hash_api_key(api_key1)
    hash2 = hash_api_key(api_key2)

    assert hash1 != hash2


def test_seed_api_key_format() -> None:
    """Test that seed script generates valid API key format (P0-3 regression test)."""
    # Simulate seed script logic
    key_id = str(uuid.uuid4())
    secret = uuid.uuid4().hex[:32]
    api_key_plaintext = f"sk_{key_id}_{secret}"

    # Should parse without error
    parsed_key_id, parsed_secret = parse_api_key(api_key_plaintext)

    assert parsed_key_id == key_id
    assert parsed_secret == secret
    assert api_key_plaintext.startswith("sk_")

    # Should hash without error
    key_hash = hash_api_key(api_key_plaintext)
    assert len(key_hash) == 64


def test_old_incorrect_format_rejected() -> None:
    """Test that old incorrect seed format (dpp_test_*) is rejected (P0-3)."""
    # Old incorrect format from seed script before P0-3 fix
    old_format_key = f"dpp_test_{uuid.uuid4().hex[:16]}"

    with pytest.raises(ValueError, match="must start with 'sk_'"):
        parse_api_key(old_format_key)
