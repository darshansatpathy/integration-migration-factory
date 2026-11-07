from __future__ import annotations

import copy
import shutil
from decimal import ROUND_HALF_EVEN
from pathlib import Path

import yaml

from imf import harness, runtime


def _run(estate, generated, rules, tmp_path, flows=None, **kw):
    bus = runtime.MemoryBus()
    ids = flows or list(estate.flows)
    return {f: harness.run_flow(estate.flows[f], estate, generated, bus, tmp_path, rules, **kw) for f in ids}


def test_every_flow_passes_with_its_accepted_differences(estate, generated, rules, tmp_path):
    results = _run(estate, generated, rules, tmp_path)
    assert all(r.passed for r in results.values())
    assert results["INVC-001"].differences and {d.accepted_by for d in results["INVC-001"].differences} == {"AD-004"}
    assert {d.reason for d in results["ORD-001"].differences if d.kind == "rejected"} == {"invalid_date", "invalid_decimal"}
    report = harness.report(list(results.values()), rules, "memory")
    assert "**10 of 10 flows pass.**" in report


def test_wrong_rounding_mode_is_caught(estate, generated, rules, tmp_path):
    """A rewrite that rounds half to even (Python's default) breaks prices on the half cent."""
    results = _run(estate, generated, rules, tmp_path, ["ORD-002", "PRC-001", "INVC-001"], rounding=ROUND_HALF_EVEN)
    for r in results.values():
        assert not r.passed
        assert {d.field for d in r.unexplained} <= {"unit_price", "list_price", "net", "gross"}


def test_template_regression_is_caught(estate, generated, rules, tmp_path):
    """A mapping that loses a step (here: trim on the customer id) fails the harness."""
    gen = tmp_path / "generated"
    shutil.copytree(generated, gen)
    p = gen / "CUST-001" / "ingest.yaml"
    spec = yaml.safe_load(p.read_text())
    spec["fields"][0]["steps"] = []
    p.write_text(yaml.safe_dump(spec))
    r = _run(estate, gen, rules, tmp_path, ["CUST-001"])["CUST-001"]
    assert not r.passed
    assert any(d.field == "customer_id" and d.new != d.legacy for d in r.unexplained)


def test_unaccepted_difference_fails(estate, generated, rules, tmp_path):
    without = [r for r in rules if r["id"] != "AD-004"]
    r = _run(estate, generated, without, tmp_path, ["INVC-001"])["INVC-001"]
    assert not r.passed and len(r.unexplained) == 7


def test_accepted_difference_over_its_limit_fails(estate, generated, rules, tmp_path):
    tight = copy.deepcopy(rules)
    next(r for r in tight if r["id"] == "AD-004")["max_records"] = 3
    r = _run(estate, generated, tight, tmp_path, ["INVC-001"])["INVC-001"]
    assert not r.passed and r.over_limit == ["AD-004: 7 records, at most 3 accepted"]


def test_tolerance_rule_does_not_hide_larger_errors(rules):
    d = harness.Difference("INVC-001", "k", "field", "gross", "10.00", "10.05")
    assert harness.explain([d], rules) == [] and d.accepted_by is None


def test_accepted_file_must_be_complete(tmp_path: Path):
    p = tmp_path / "a.yaml"
    p.write_text("- {id: X, rule: tolerance, flows: [A]}\n")
    try:
        harness.load_accepted(p)
    except ValueError as e:
        assert "rationale" in str(e)
    else:
        raise AssertionError("an incomplete rule was accepted")
