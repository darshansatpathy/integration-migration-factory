from __future__ import annotations

import pytest

from imf import inventory
from imf.model import EstateError, load_estate


def test_waves_follow_dependencies(estate):
    entries = inventory.build(estate)
    wave = {e.flow.id: e.wave for e in entries}
    for e in entries:
        for d in e.flow.depends_on:
            assert wave[d] < e.wave
    assert wave["PAY-001"] == 4  # CUST-001 -> ORD-001 -> INVC-001 -> PAY-001


def test_scoring_is_explained(estate):
    s, reasons = inventory.score(estate.flows["INVC-001"])
    assert s == 12.5
    assert "arithmetic" in reasons and "fixed-width source" in reasons
    tiers = {e.flow.id: e.tier for e in inventory.build(estate)}
    assert tiers["INVC-001"] == "high" and tiers["SUP-001"] == "low"


def test_cycles_are_refused(estate_copy):
    p = estate_copy / "flows" / "CUST-001.xml"
    p.write_text(p.read_text().replace("</flow>", '  <depends-on flow="PAY-001"/>\n</flow>'))
    with pytest.raises(EstateError, match="cycle"):
        inventory.build(load_estate(estate_copy))


def test_markdown_has_graph(estate):
    md = inventory.markdown(inventory.build(estate))
    assert "```mermaid" in md and "CUST_001 --> ORD_001" in md
