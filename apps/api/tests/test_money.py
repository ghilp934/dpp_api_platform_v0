"""Tests for money utilities (DEC-4211)."""

from decimal import Decimal

import pytest

from dpp_api.utils.money import (
    AmountTooLargeError,
    NegativeAmountError,
    decimal_to_usd_micros,
    format_usd_micros,
    parse_usd_string,
    usd_micros_to_decimal,
    validate_usd_micros,
)


def test_usd_micros_to_decimal():
    """Test converting USD_MICROS to Decimal."""
    # Standard amounts
    assert usd_micros_to_decimal(1_500_000) == Decimal("1.5000")
    assert usd_micros_to_decimal(0) == Decimal("0.0000")
    assert usd_micros_to_decimal(10_000) == Decimal("0.0100")
    assert usd_micros_to_decimal(1) == Decimal("0.0000")  # Rounds to 4dp

    # Large amounts
    assert usd_micros_to_decimal(10_000_000_000) == Decimal("10000.0000")

    # Fractional cents
    assert usd_micros_to_decimal(1_234_567) == Decimal("1.2346")  # Rounded


def test_decimal_to_usd_micros():
    """Test converting Decimal to USD_MICROS."""
    # Standard amounts
    assert decimal_to_usd_micros(Decimal("1.50")) == 1_500_000
    assert decimal_to_usd_micros(Decimal("0.01")) == 10_000
    assert decimal_to_usd_micros(Decimal("0.00")) == 0
    assert decimal_to_usd_micros(Decimal("1.5000")) == 1_500_000

    # Large amounts
    assert decimal_to_usd_micros(Decimal("10000.00")) == 10_000_000_000

    # Many decimal places (should round)
    assert decimal_to_usd_micros(Decimal("1.123456")) == 1_123_456


def test_decimal_to_usd_micros_negative():
    """Test that negative amounts raise error."""
    with pytest.raises(NegativeAmountError, match="cannot be negative"):
        decimal_to_usd_micros(Decimal("-1.00"))


def test_decimal_to_usd_micros_too_large():
    """Test that amounts exceeding maximum raise error."""
    with pytest.raises(AmountTooLargeError, match="exceeds maximum"):
        decimal_to_usd_micros(Decimal("10001.00"))


def test_format_usd_micros():
    """Test formatting USD_MICROS as string."""
    assert format_usd_micros(1_500_000) == "1.5000"
    assert format_usd_micros(0) == "0.0000"
    assert format_usd_micros(10_000) == "0.0100"
    assert format_usd_micros(1) == "0.0000"


def test_parse_usd_string():
    """Test parsing string to USD_MICROS."""
    # Standard formats
    assert parse_usd_string("1.50") == 1_500_000
    assert parse_usd_string("1.5000") == 1_500_000
    assert parse_usd_string("0.01") == 10_000
    assert parse_usd_string("0") == 0

    # Integer strings
    assert parse_usd_string("10") == 10_000_000


def test_parse_usd_string_invalid():
    """Test that invalid strings raise error."""
    with pytest.raises(Exception):  # MoneyError
        parse_usd_string("invalid")

    with pytest.raises(Exception):
        parse_usd_string("")


def test_parse_usd_string_negative():
    """Test that negative strings raise error."""
    with pytest.raises(NegativeAmountError):
        parse_usd_string("-1.00")


def test_validate_usd_micros():
    """Test validating USD_MICROS values."""
    # Valid amounts should not raise
    validate_usd_micros(0)
    validate_usd_micros(1_500_000)
    validate_usd_micros(10_000_000_000)  # $10,000


def test_validate_usd_micros_negative():
    """Test that negative micros raise error."""
    with pytest.raises(NegativeAmountError, match="cannot be negative"):
        validate_usd_micros(-1)


def test_validate_usd_micros_too_large():
    """Test that micros exceeding maximum raise error."""
    with pytest.raises(AmountTooLargeError, match="exceeds maximum"):
        validate_usd_micros(10_001_000_000)  # $10,001


def test_round_trip_conversion():
    """Test that round-trip conversion preserves values."""
    test_values = [0, 10_000, 1_500_000, 10_000_000_000]

    for micros in test_values:
        decimal = usd_micros_to_decimal(micros)
        back_to_micros = decimal_to_usd_micros(decimal)
        assert back_to_micros == micros


def test_no_float_precision_issues():
    """Test that Decimal avoids float precision issues."""
    # This would fail with float: 0.1 + 0.2 != 0.3
    amount1 = Decimal("0.10")
    amount2 = Decimal("0.20")
    expected = Decimal("0.30")

    micros1 = decimal_to_usd_micros(amount1)
    micros2 = decimal_to_usd_micros(amount2)
    micros_sum = micros1 + micros2

    assert usd_micros_to_decimal(micros_sum) == expected


def test_four_decimal_places_precision():
    """Test that 4 decimal places are preserved."""
    # Test with exact 4dp values
    test_cases = [
        (Decimal("1.2345"), 1_234_500),  # Exact
        (Decimal("0.0001"), 100),  # Smallest 4dp unit
        (Decimal("9999.9999"), 9_999_999_900),  # Large with 4dp
    ]

    for decimal_val, expected_micros in test_cases:
        micros = decimal_to_usd_micros(decimal_val)
        assert micros == expected_micros

        # Round trip
        back_to_decimal = usd_micros_to_decimal(micros)
        # Note: May lose precision beyond 4dp, but that's expected
        assert abs(back_to_decimal - decimal_val) < Decimal("0.0001")
