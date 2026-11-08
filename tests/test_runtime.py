from __future__ import annotations

from pathlib import Path

import pytest

from imf import formats, generate, runtime
from imf.samples import sample_path


def _specs(generated: Path, flow: str):
    return (
        generate.load_spec(generated, flow, "ingest.yaml"),
        generate.load_spec(generated, flow, "deliver.yaml"),
        generate.load_spec(generated, flow, "schema.json"),
    )


def test_ingest_and_deliver(estate, generated, tmp_path):
    ing, dl, schema = _specs(generated, "ORD-001")
    bus = runtime.MemoryBus()
    res = runtime.ingest(ing, schema, sample_path(estate, estate.flows["ORD-001"]), bus, batch_id="b1")
    assert res.read == 300 and res.filtered == 28 and res.published == 264
    assert res.rejected == {"invalid_date": 5, "invalid_decimal": 3}
    out = runtime.deliver(dl, bus, tmp_path / "orders.csv", "b1")
    assert out.records == 264
    dead = runtime.dead_letters(ing, bus, "b1")
    assert len(dead) == 8 and all(d["source"] and d["key"].startswith("ORD") for d in dead)
    (control,) = [m.value for m in bus.read(ing["control_topic"])]
    assert control["records"] == 264 and control["rejected"] == res.rejected


def test_incomplete_batch_is_not_delivered(estate, generated, tmp_path):
    ing, dl, schema = _specs(generated, "PAY-001")
    bus = runtime.MemoryBus()
    runtime.ingest(ing, schema, sample_path(estate, estate.flows["PAY-001"]), bus, batch_id="b1")
    bus.topics[ing["topic"]].pop(17)  # a record lost in transit
    with pytest.raises(runtime.IncompleteBatch, match="299 records"):
        runtime.deliver(dl, bus, tmp_path / "p.csv", "b1", wait_seconds=0)
    assert not (tmp_path / "p.csv").exists()


def test_late_records_are_waited_for(estate, generated, tmp_path, monkeypatch):
    """Records that become visible after the control message are waited for, not reported missing."""
    ing, dl, schema = _specs(generated, "PAY-001")
    bus = runtime.MemoryBus()
    runtime.ingest(ing, schema, sample_path(estate, estate.flows["PAY-001"]), bus, batch_id="b1")
    late = bus.topics[ing["topic"]][-4:]
    del bus.topics[ing["topic"]][-4:]
    reads = {"n": 0}
    original = bus.read

    def read(topic: str) -> list[runtime.Message]:
        if topic == ing["topic"]:
            reads["n"] += 1
            if reads["n"] == 2:  # the records arrive while deliver is waiting
                bus.topics[topic].extend(late)
        return original(topic)

    monkeypatch.setattr(bus, "read", read)
    monkeypatch.setattr(runtime.time, "sleep", lambda s: None)
    out = runtime.deliver(dl, bus, tmp_path / "p.csv", "b1", wait_seconds=5)
    assert out.records == 300 and reads["n"] == 2


def test_control_totals_catch_a_changed_record(estate, generated, tmp_path):
    ing, dl, schema = _specs(generated, "PAY-001")
    bus = runtime.MemoryBus()
    runtime.ingest(ing, schema, sample_path(estate, estate.flows["PAY-001"]), bus, batch_id="b1")
    bus.topics[ing["topic"]][3].value["amount"] = "0.01"
    with pytest.raises(runtime.IncompleteBatch, match="control totals"):
        runtime.deliver(dl, bus, tmp_path / "p.csv", "b1", wait_seconds=0)


def test_duplicates_are_dropped_and_order_is_kept(estate, generated, tmp_path):
    ing, dl, schema = _specs(generated, "SUP-001")
    bus = runtime.MemoryBus()
    runtime.ingest(ing, schema, sample_path(estate, estate.flows["SUP-001"]), bus, batch_id="b1")
    msgs = bus.topics[ing["topic"]]
    msgs.insert(0, msgs[10])  # a retried send, arriving out of order
    out = runtime.deliver(dl, bus, tmp_path / "s.jsonl", "b1")
    assert out.duplicates == 1 and out.records == 150
    ids = [r["supplier_id"] for r in formats.read_target("jsonl", tmp_path / "s.jsonl")]
    assert ids == sorted(ids)


def test_batches_are_kept_apart(estate, generated, tmp_path):
    ing, dl, schema = _specs(generated, "SUP-001")
    bus = runtime.MemoryBus()
    for b in ("b1", "b2"):
        runtime.ingest(ing, schema, sample_path(estate, estate.flows["SUP-001"]), bus, batch_id=b)
    assert runtime.deliver(dl, bus, tmp_path / "s.jsonl", "b2").records == 150
    with pytest.raises(runtime.IncompleteBatch, match="no control message"):
        runtime.deliver(dl, bus, tmp_path / "s.jsonl", "b3", wait_seconds=0)


def test_malformed_rows_and_schema_violations_go_to_the_dlq(generated, tmp_path):
    ing, _, schema = _specs(generated, "PAY-001")
    src = tmp_path / "pay.csv"
    src.write_text("ref,invoice,amount,value_date\n,INV1,10.00,01/02/2025\nP2,INV2,10.00\nP3,INV3,10.00,01/02/2025\n")
    bus = runtime.MemoryBus()
    res = runtime.ingest(ing, schema, src, bus, batch_id="b1")
    assert res.published == 1
    assert res.rejected == {"schema_violation": 1, "malformed_row": 1}
    reasons = {d["reason"]: d for d in runtime.dead_letters(ing, bus, "b1")}
    assert "payment_ref" in reasons["schema_violation"]["detail"]
    assert reasons["malformed_row"]["source"] == "P2,INV2,10.00"


def test_fixed_width_output_truncates_and_counts(estate, generated, tmp_path):
    ing, dl, schema = _specs(generated, "INV-001")
    bus = runtime.MemoryBus()
    runtime.ingest(ing, schema, sample_path(estate, estate.flows["INV-001"]), bus, batch_id="b1")
    out = runtime.deliver(dl, bus, tmp_path / "stock.dat", "b1")
    assert out.truncated == {"description": 11}
    widths = {len(line) for line in (tmp_path / "stock.dat").read_text().splitlines()}
    assert widths == {46}


def test_fixed_width_overflow_is_refused():
    layout = [{"name": "n", "start": 1, "length": 3, "type": "integer"}]
    with pytest.raises(formats.Rejected, match="field_overflow"):
        formats.render_fixed({"n": 12345}, layout)


def test_a_refused_send_fails_the_flush():
    """A message the broker refused must surface, not vanish from the batch."""

    class Msg:
        def topic(self) -> str:
            return "order.ord-001.v1"

    bus = runtime.KafkaBus("127.0.0.1:1")  # never contacted: the queue is empty
    bus._on_delivery("Broker: Not enough in-sync replicas", Msg())
    with pytest.raises(RuntimeError, match="1 messages were not delivered; first: order"):
        bus.flush()
    bus.flush()  # the error is reported once
