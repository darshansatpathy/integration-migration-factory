from __future__ import annotations

import json
import shutil

from jsonschema import Draft202012Validator

from imf import generate


def test_committed_artifacts_match_templates(estate, generated):
    assert generate.drift(estate, generated) == []


def test_drift_is_detected(estate, generated, tmp_path):
    copy = tmp_path / "generated"
    shutil.copytree(generated, copy)
    (copy / "ORD-001" / "ingest.yaml").write_text("tampered\n")
    (copy / "OLD-001").mkdir()
    (copy / "OLD-001" / "ingest.yaml").write_text("orphan\n")
    problems = generate.drift(estate, copy)
    assert "ORD-001/ingest.yaml" in problems
    assert any(p.startswith("OLD-001") for p in problems)


def test_schema_types(estate):
    s = generate.json_schema(estate.flows["INVC-001"], estate)
    v = Draft202012Validator(s)
    good = {"invoice_id": "INV1", "invoice_date": "2025-01-31", "customer_id": "C1", "net": "10.00", "gross": "11.80"}
    assert v.is_valid(good)
    assert not v.is_valid(good | {"net": "10.0"})
    assert not v.is_valid(good | {"invoice_id": ""})
    assert not v.is_valid(good | {"extra": 1})
    lines = generate.json_schema(estate.flows["ORD-002"], estate)["properties"]
    assert lines["quantity"] == {"type": "integer"}  # from the fixed-width layout


def test_patterns_are_portable(generated):
    for p in generated.glob("*/schema.json"):
        for prop in json.loads(p.read_text())["properties"].values():
            assert "\\-" not in prop.get("pattern", "")
