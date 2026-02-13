"""Money utilities for DEC-4211 USD_MICROS type.

All money is stored as BIGINT representing micro-dollars (1/1,000,000 of a dollar).
API uses 4 decimal place strings for human readability.

NEVER use float or double for money calculations.
"""

from decimal import Decimal, ROUND_HALF_UP

# Constants
MICROS_PER_DOLLAR = 1_000_000
MAX_AMOUNT_USD = Decimal("10000.0000")  # $10,000 max per request
MIN_AMOUNT_USD = Decimal("0.0000")  # No negative amounts


class MoneyError(ValueError):
    """Base exception for money-related errors."""

    pass


class NegativeAmountError(MoneyError):
    """Raised when amount is negative."""

    pass


class AmountTooLargeError(MoneyError):
    """Raised when amount exceeds maximum allowed."""

    pass


def usd_micros_to_decimal(micros: int) -> Decimal:
    """
    Convert USD_MICROS (BIGINT) to Decimal for display.

    Args:
        micros: Amount in micro-dollars (1/1,000,000 of a dollar)

    Returns:
        Decimal with 4 decimal places (e.g., Decimal("1.5000"))

    Examples:
        >>> usd_micros_to_decimal(1_500_000)
        Decimal('1.5000')
        >>> usd_micros_to_decimal(0)
        Decimal('0.0000')
        >>> usd_micros_to_decimal(1)
        Decimal('0.0000')  # Rounded to 4dp
    """
    # Convert to Decimal to avoid float precision issues
    decimal_value = Decimal(micros) / Decimal(MICROS_PER_DOLLAR)

    # Quantize to 4 decimal places
    return decimal_value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def decimal_to_usd_micros(amount: Decimal) -> int:
    """
    Convert Decimal to USD_MICROS (BIGINT) for storage.

    Args:
        amount: Decimal amount (e.g., Decimal("1.50"))

    Returns:
        Amount in micro-dollars

    Raises:
        NegativeAmountError: If amount is negative
        AmountTooLargeError: If amount exceeds MAX_AMOUNT_USD

    Examples:
        >>> decimal_to_usd_micros(Decimal("1.50"))
        1500000
        >>> decimal_to_usd_micros(Decimal("0.01"))
        10000
        >>> decimal_to_usd_micros(Decimal("1.5000"))
        1500000
    """
    if amount < MIN_AMOUNT_USD:
        raise NegativeAmountError(f"Amount cannot be negative: {amount}")

    if amount > MAX_AMOUNT_USD:
        raise AmountTooLargeError(
            f"Amount {amount} exceeds maximum {MAX_AMOUNT_USD}"
        )

    # Multiply by MICROS_PER_DOLLAR and convert to int
    micros = amount * Decimal(MICROS_PER_DOLLAR)

    # Round to nearest integer (should already be whole number)
    return int(micros.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def format_usd_micros(micros: int) -> str:
    """
    Format USD_MICROS as 4 decimal place string for API response.

    Args:
        micros: Amount in micro-dollars

    Returns:
        Formatted string (e.g., "1.5000")

    Examples:
        >>> format_usd_micros(1_500_000)
        '1.5000'
        >>> format_usd_micros(0)
        '0.0000'
        >>> format_usd_micros(10_000)
        '0.0100'
    """
    decimal_value = usd_micros_to_decimal(micros)
    return f"{decimal_value:.4f}"


def parse_usd_string(amount_str: str) -> int:
    """
    Parse 4dp string from API request to USD_MICROS.

    Args:
        amount_str: Amount string (e.g., "1.50", "1.5000")

    Returns:
        Amount in micro-dollars

    Raises:
        MoneyError: If string is invalid
        NegativeAmountError: If amount is negative
        AmountTooLargeError: If amount exceeds maximum

    Examples:
        >>> parse_usd_string("1.50")
        1500000
        >>> parse_usd_string("1.5000")
        1500000
        >>> parse_usd_string("0.01")
        10000
    """
    try:
        decimal_value = Decimal(amount_str)
    except Exception as e:
        raise MoneyError(f"Invalid amount string: {amount_str}") from e

    return decimal_to_usd_micros(decimal_value)


def validate_usd_micros(micros: int) -> None:
    """
    Validate that USD_MICROS value is within acceptable range.

    Args:
        micros: Amount in micro-dollars

    Raises:
        NegativeAmountError: If amount is negative
        AmountTooLargeError: If amount exceeds maximum
    """
    if micros < 0:
        raise NegativeAmountError(f"Amount cannot be negative: {micros} micros")

    max_micros = decimal_to_usd_micros(MAX_AMOUNT_USD)
    if micros > max_micros:
        raise AmountTooLargeError(
            f"Amount {format_usd_micros(micros)} exceeds maximum {MAX_AMOUNT_USD}"
        )
