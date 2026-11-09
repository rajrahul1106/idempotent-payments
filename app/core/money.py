"""The only module that converts between minor units and Decimal.

Blueprint section 5.2. Keeping every conversion here means the rule
"money is never a float" is auditable by reading one file.

Wire format is an integer count of minor units plus an ISO 4217 code:
    {"amount_minor": 200000, "currency": "INR"}  ->  Decimal("2000.0000")

Storage is NUMERIC(20,4): four decimal places covers three-decimal
currencies such as the Kuwaiti dinar, plus one more place for fee or FX
arithmetic.
"""

from __future__ import annotations

from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation

SCALE = 4
QUANTUM = Decimal(1).scaleb(-SCALE)  # Decimal("0.0001")

# Minor units per major unit. Anything not listed uses 2, which covers
# the large majority of ISO 4217 currencies.
_MINOR_UNIT_EXPONENT: dict[str, int] = {
    "JPY": 0,
    "KRW": 0,
    "VND": 0,
    "CLP": 0,
    "ISK": 0,
    "BHD": 3,
    "KWD": 3,
    "OMR": 3,
    "JOD": 3,
    "TND": 3,
}
_DEFAULT_EXPONENT = 2

MAX_AMOUNT_MINOR = 10**12


class MoneyError(ValueError):
    """Raised when a value cannot be represented as money."""


def minor_unit_exponent(currency: str) -> int:
    return _MINOR_UNIT_EXPONENT.get(currency.upper(), _DEFAULT_EXPONENT)


def to_decimal(amount_minor: int, currency: str) -> Decimal:
    """Minor units to a Decimal at scale 4.

    Rejects floats outright rather than coercing them: a float that
    reached this function is a bug upstream, and silently accepting it
    is how precision loss enters a ledger.
    """
    if isinstance(amount_minor, bool) or not isinstance(amount_minor, int):
        raise MoneyError(
            f"amount_minor must be an int, got {type(amount_minor).__name__}"
        )
    if abs(amount_minor) > MAX_AMOUNT_MINOR:
        raise MoneyError(f"amount_minor out of range: {amount_minor}")

    exponent = minor_unit_exponent(currency)
    try:
        value = Decimal(amount_minor).scaleb(-exponent)
    except InvalidOperation as exc:  # pragma: no cover - defensive
        raise MoneyError(str(exc)) from exc
    return value.quantize(QUANTUM, rounding=ROUND_HALF_EVEN)


def to_minor(amount: Decimal, currency: str) -> int:
    """Decimal back to minor units.

    Raises when the value carries more precision than the currency can
    express, so a rounding decision is never made silently.
    """
    if not isinstance(amount, Decimal):
        raise MoneyError(f"amount must be a Decimal, got {type(amount).__name__}")

    exponent = minor_unit_exponent(currency)
    shifted = amount.scaleb(exponent)
    rounded = shifted.quantize(Decimal(1), rounding=ROUND_HALF_EVEN)
    if shifted != rounded:
        raise MoneyError(
            f"{amount} cannot be expressed in {currency} minor units without rounding"
        )
    return int(rounded)


def quantize(amount: Decimal) -> Decimal:
    """Force a Decimal to the storage scale."""
    if not isinstance(amount, Decimal):
        raise MoneyError(f"amount must be a Decimal, got {type(amount).__name__}")
    return amount.quantize(QUANTUM, rounding=ROUND_HALF_EVEN)


def format_amount(amount: Decimal) -> str:
    """Canonical string form for API responses: always 4 decimal places."""
    return str(quantize(amount))
