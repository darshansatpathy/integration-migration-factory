from __future__ import annotations

from decimal import ROUND_HALF_EVEN, Decimal

import pytest

from imf.transforms import Rejected, apply, check_filter


def test_pipeline_steps():
    assert apply(["trim", "upper"], ["  josé peña "], "n") == "JOSÉ PEÑA"
    assert apply(["date:%d.%m.%Y->%Y-%m-%d"], ["05.03.2025"], "d") == "2025-03-05"
    assert apply(["join:-", "upper"], ["ord1 ", "0002"], "k") == "ORD1-0002"
    assert apply(["trim", "default:N30"], ["   "], "t") == "N30"
    assert apply(["int"], [" 42 "], "q") == 42


def test_decimal_rounds_half_up_exactly():
    assert apply(["decimal:2"], [Decimal("12.345")], "p") == Decimal("12.35")
    assert apply(["decimal:2"], ["2.675"], "p") == Decimal("2.68")  # 2.675 is below the half as a binary float
    assert apply(["decimal:2"], [Decimal("12.345")], "p", rounding=ROUND_HALF_EVEN) == Decimal("12.34")
    assert apply(["multiply:1.18", "decimal:2"], [Decimal("873.75")], "g") == Decimal("1031.03")  # 1031.025 exactly


@pytest.mark.parametrize(
    ("steps", "value", "reason"),
    [
        (["date:%Y%m%d->%Y-%m-%d"], "20240230", "invalid_date"),
        (["decimal:2"], "12A45", "invalid_decimal"),
        (["decimal:2"], "NaN", "invalid_decimal"),
        (["int"], "12.5", "invalid_integer"),
    ],
)
def test_bad_values_are_rejected_not_blanked(steps, value, reason):
    with pytest.raises(Rejected) as e:
        apply(steps, [value], "f")
    assert e.value.reason == reason


def test_empty_values_pass_through():
    assert apply(["date:%Y%m%d->%Y-%m-%d"], [""], "d") == ""


def test_filters():
    assert check_filter({"field": "STATUS", "op": "!=", "value": "X"}, {"STATUS": "O"})
    assert not check_filter({"field": "QTY", "op": ">", "value": 0}, {"QTY": 0})
    assert check_filter({"field": "r", "op": "in", "value": ["damaged"]}, {"r": "damaged"})
    assert not check_filter({"field": "r", "op": "in", "value": ["damaged"]}, {"r": "Damaged"})
    with pytest.raises(Rejected, match="invalid_filter_value"):
        check_filter({"field": "QTY", "op": ">", "value": 0}, {"QTY": "abc"})
    assert check_filter(None, {})
