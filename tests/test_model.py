from __future__ import annotations

from pathlib import Path

import pytest

from imf.model import EstateError, load_estate, parse_filter


def test_estate_loads_every_flow(estate):
    assert len(estate.flows) == 10
    ord1 = estate.flows["ORD-001"]
    assert ord1.topic == "order.ord-001.v1"
    assert ord1.depends_on == ("CUST-001",)
    assert ord1.filter is not None and ord1.filter.op == "!="
    assert estate.flows["INV-001"].key == ("sku", "location")


@pytest.mark.parametrize(
    ("expr", "op", "value"),
    [("STATUS != 'X'", "!=", "X"), ("QTY > 0", ">", 0.0), ("TAX_CD in ('S1','S2')", "in", ("S1", "S2"))],
)
def test_filters(expr, op, value):
    f = parse_filter(expr)
    assert (f.op, f.value) == (op, value)


@pytest.mark.parametrize("expr", ["STATUS ~ 'X'", "QTY > abc", "CODE in 'S1'"])
def test_bad_filters_are_refused(expr):
    with pytest.raises(EstateError):
        parse_filter(expr)


def _edit(root: Path, flow: str, old: str, new: str) -> None:
    p = root / "flows" / f"{flow}.xml"
    p.write_text(p.read_text().replace(old, new))


@pytest.mark.parametrize(
    ("flow", "old", "new", "message"),
    [
        ("CUST-001", 'transform="trim|upper"', 'transform="trim|soundex"', "unknown transform"),
        ("ORD-002", 'transform="join:-"', 'transform="trim"', "does not join"),
        ("SUP-001", 'key="supplier_id"', 'key="vendor"', "not in the map"),
        ("ORD-001", '<depends-on flow="CUST-001"/>', '<depends-on flow="CUST-999"/>', "unknown flows"),
        ("ORD-001", 'layout="orders_v2"', 'layout="orders_v9"', "unknown layout"),
    ],
)
def test_invalid_definitions_are_refused(estate_copy, flow, old, new, message):
    _edit(estate_copy, flow, old, new)
    with pytest.raises(EstateError, match=message):
        load_estate(estate_copy)


def test_overlapping_layout_fields_are_refused(estate_copy):
    p = estate_copy / "layouts" / "orders_v2.yaml"
    p.write_text(p.read_text().replace("start: 11, length: 8", "start: 9, length: 8"))
    with pytest.raises(EstateError, match="overlaps"):
        load_estate(estate_copy)
