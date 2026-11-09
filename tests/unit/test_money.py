"""Phase 2 gate: Decimal conversion is lossless and floats are rejected."""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given, settings as hyp_settings, strategies as st

from app.core.money import (
    MAX_AMOUNT_MINOR,
    MoneyError,
    format_amount,
    minor_unit_exponent,
    to_decimal,
    to_minor,
)


def test_inr_two_decimal_places():
    assert to_decimal(200000, "INR") == Decimal("2000.0000")
    assert to_minor(Decimal("2000.0000"), "INR") == 200000


def test_jpy_has_no_minor_units():
    assert minor_unit_exponent("JPY") == 0
    assert to_decimal(2000, "JPY") == Decimal("2000.0000")
    assert to_minor(Decimal("2000.0000"), "JPY") == 2000


def test_kwd_has_three_minor_units():
    assert minor_unit_exponent("KWD") == 3
    assert to_decimal(2000, "KWD") == Decimal("2.0000")
    assert to_minor(Decimal("2.0000"), "KWD") == 2000


def test_unknown_currency_defaults_to_two():
    assert minor_unit_exponent("XYZ") == 2


@pytest.mark.parametrize("bad", [200000.0, 2.5, "200000", None, True])
def test_float_and_non_int_rejected(bad):
    with pytest.raises(MoneyError):
        to_decimal(bad, "INR")


def test_out_of_range_rejected():
    with pytest.raises(MoneyError):
        to_decimal(MAX_AMOUNT_MINOR + 1, "INR")


def test_to_minor_rejects_non_decimal():
    with pytest.raises(MoneyError):
        to_minor(20.0, "INR")


def test_to_minor_rejects_silent_rounding():
    # 0.00005 INR cannot be expressed in paise. Refusing is the point:
    # a silent round here is how money goes missing.
    with pytest.raises(MoneyError):
        to_minor(Decimal("0.0005"), "INR")


def test_format_always_scale_four():
    assert format_amount(Decimal("5")) == "5.0000"
    assert format_amount(Decimal("-2000.5")) == "-2000.5000"


@given(
    amount_minor=st.integers(min_value=-MAX_AMOUNT_MINOR, max_value=MAX_AMOUNT_MINOR),
    currency=st.sampled_from(["INR", "USD", "EUR", "JPY", "KWD", "BHD", "KRW"]),
)
@hyp_settings(max_examples=400)
def test_round_trip_is_lossless(amount_minor: int, currency: str):
    """I: minor -> Decimal -> minor returns the original integer, always."""
    assert to_minor(to_decimal(amount_minor, currency), currency) == amount_minor


@given(
    values=st.lists(
        st.integers(min_value=1, max_value=10**9), min_size=2, max_size=200
    )
)
@hyp_settings(max_examples=200)
def test_summation_has_no_drift(values: list[int]):
    """Decimal addition matches integer addition exactly, at any length."""
    total = sum((to_decimal(v, "INR") for v in values), Decimal("0"))
    assert to_minor(total, "INR") == sum(values)


def test_float_drift_is_real():
    """Documents why this module exists, rather than asserting it away."""
    naive = 0.0
    for _ in range(1000):
        naive += 0.1
    assert naive != 100.0

    exact = sum((to_decimal(10, "INR") for _ in range(1000)), Decimal("0"))
    assert exact == Decimal("100.0000")
