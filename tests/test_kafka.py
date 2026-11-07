"""The full regression on a real broker. Runs when IMF_BOOTSTRAP is set (as in CI)."""

from __future__ import annotations

import os
import time

import pytest

from imf import harness, runtime

BOOTSTRAP = os.environ.get("IMF_BOOTSTRAP")
pytestmark = [pytest.mark.kafka, pytest.mark.skipif(not BOOTSTRAP, reason="IMF_BOOTSTRAP not set")]


def test_regression_on_kafka(estate, generated, rules, tmp_path):
    assert BOOTSTRAP
    bus = runtime.KafkaBus(BOOTSTRAP, prefix=f"test-{time.time_ns()}.")
    results = [harness.run_flow(f, estate, generated, bus, tmp_path, rules) for f in estate.flows.values()]
    assert all(r.passed for r in results), [d for r in results for d in r.unexplained][:5]
    assert sum(r.new_records for r in results) == 2337


def test_duplicate_send_on_kafka_is_dropped(estate, generated, tmp_path):
    assert BOOTSTRAP
    from imf import generate
    from imf.samples import sample_path

    bus = runtime.KafkaBus(BOOTSTRAP, prefix=f"test-{time.time_ns()}.")
    ing = generate.load_spec(generated, "SUP-001", "ingest.yaml")
    dl = generate.load_spec(generated, "SUP-001", "deliver.yaml")
    schema = generate.load_spec(generated, "SUP-001", "schema.json")
    res = runtime.ingest(ing, schema, sample_path(estate, estate.flows["SUP-001"]), bus, batch_id="b1")
    bus.produce(
        ing["topic"],
        "V0000003",
        {"supplier_id": "V0000003", "name": "dup", "country": "DE", "payment_terms": "N30"},
        {"batch_id": "b1", "seq": "4"},
    )
    bus.flush()
    out = runtime.deliver(dl, bus, tmp_path / "s.jsonl", res.batch_id)
    assert out.records == 150 and out.duplicates == 1
