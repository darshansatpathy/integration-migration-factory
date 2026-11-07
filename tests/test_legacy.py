"""The legacy engine's quirks, pinned down: they are what the regression harness has to catch."""

from __future__ import annotations

from imf.legacy import _transform


def test_upper_changes_ascii_only():
    assert _transform(("trim", "upper"), [" josé "]) == "JOSé"


def test_invalid_date_becomes_blank():
    assert _transform(("date:%Y%m%d->%Y-%m-%d",), ["20240230"]) == ""


def test_bad_number_becomes_zero():
    assert _transform(("decimal:2",), ["12A45"]) == "0.00"


def test_float_multiplication_rounds_down_on_the_half():
    # 873.75 x 1.18 = 1031.025 exactly, but 1031.0249999... in binary floating point.
    assert _transform(("multiply:1.18", "decimal:2"), ["873.75"]) == "1031.02"
