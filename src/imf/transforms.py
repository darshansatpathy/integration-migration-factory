"""Field transforms on the new platform.

Strict where the legacy engine was lenient: a value that cannot be converted raises a
`Rejected` with a reason, and the record goes to the dead-letter topic instead of being
written with a blank or a zero. Decimal arithmetic is exact and rounds half up, the
legacy engine's documented rounding.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

Value = str | int | Decimal | None


class Rejected(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


def _decimal(v: Value, field: str) -> Decimal:
    if isinstance(v, Decimal):
        return v
    s = "" if v is None else str(v).strip()
    try:
        d = Decimal(s)
    except InvalidOperation as e:
        raise Rejected("invalid_decimal", f"{field}={v!r}") from e
    if not d.is_finite():
        raise Rejected("invalid_decimal", f"{field}={v!r}")
    return d


def step(name: str, arg: str | None, v: Value, field: str, rounding: str = ROUND_HALF_UP) -> Value:
    if name == "trim":
        return None if v is None else str(v).strip()
    if name == "upper":
        return None if v is None else str(v).upper()
    if name == "lower":
        return None if v is None else str(v).lower()
    if name == "default":
        return arg if v is None or str(v).strip() == "" else v
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return v  # empty stays empty; required fields are enforced by the schema
    if name == "date":
        assert arg is not None
        fmt_in, fmt_out = arg.split("->")
        try:
            return datetime.strptime(str(v).strip(), fmt_in).strftime(fmt_out)
        except ValueError as e:
            raise Rejected("invalid_date", f"{field}={v!r}") from e
    if name == "decimal":
        places = Decimal(1).scaleb(-int(arg or 0))
        return _decimal(v, field).quantize(places, rounding=rounding)
    if name == "multiply":
        return _decimal(v, field) * Decimal(arg or "1")
    if name == "int":
        s = str(v).strip()
        if not s.lstrip("-").isdigit():
            raise Rejected("invalid_integer", f"{field}={v!r}")
        return int(s)
    raise ValueError(f"unknown transform {name!r}")


def apply(steps: Sequence[str], values: Sequence[Value], field: str, rounding: str = ROUND_HALF_UP) -> Value:
    """Run a field's transform pipeline. Several source values must be joined first."""
    v: Value
    rest = list(steps)
    if len(values) > 1:
        if not rest or not rest[0].startswith("join:"):
            raise ValueError(f"{field}: several sources need a join first")
        sep = rest.pop(0).split(":", 1)[1]
        v = sep.join("" if x is None else str(x).strip() for x in values)
    else:
        v = values[0]
    for s in rest:
        name, _, arg = s.partition(":")
        if name == "join":
            continue
        v = step(name, arg or None, v, field, rounding)
    return v


def check_filter(flt: dict[str, Any] | None, record: dict[str, Value]) -> bool:
    """True when the record passes. Comparisons are on the raw source value."""
    if not flt:
        return True
    v = record.get(flt["field"])
    op, target = flt["op"], flt["value"]
    if op == "in":
        return str(v) in target
    if isinstance(target, (int, float)):
        try:
            x = float(_decimal(v, flt["field"]))
        except Rejected:
            raise Rejected("invalid_filter_value", f"{flt['field']}={v!r}") from None
        t = float(target)
    else:
        x, t = str(v), str(target)  # type: ignore[assignment]
    return {
        "==": x == t,
        "!=": x != t,
        ">": x > t,
        ">=": x >= t,
        "<": x < t,
        "<=": x <= t,
    }[op]
