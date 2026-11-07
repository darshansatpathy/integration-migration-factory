"""The migrated flows at run time: ingest (file to topic) and deliver (topic to file).

Both read only the generated specs. Ingest validates every record against the flow's schema
and sends rejects to the dead-letter topic with the reason and the original source line. After
the records it publishes a control message with the batch's record count and control totals;
deliver writes the target file only once the batch is complete and the totals agree, so a
partially delivered batch never reaches the target system.
"""

from __future__ import annotations

import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Protocol

from jsonschema import Draft202012Validator

from imf import formats
from imf.transforms import Rejected, Value, apply, check_filter


@dataclass
class Message:
    key: str
    value: dict[str, Any]
    headers: dict[str, str]


class Bus(Protocol):
    def produce(self, topic: str, key: str, value: dict[str, Any], headers: dict[str, str]) -> None: ...
    def flush(self) -> None: ...
    def read(self, topic: str) -> list[Message]: ...


@dataclass
class MemoryBus:
    """In-process topics, for unit tests and quick regressions without a broker."""

    topics: dict[str, list[Message]] = field(default_factory=lambda: defaultdict(list))

    def produce(self, topic: str, key: str, value: dict[str, Any], headers: dict[str, str]) -> None:
        self.topics[topic].append(Message(key, json.loads(json.dumps(value)), dict(headers)))

    def flush(self) -> None:
        pass

    def read(self, topic: str) -> list[Message]:
        return list(self.topics.get(topic, []))


class KafkaBus:
    """Kafka through confluent-kafka: an idempotent producer, and reads of a whole topic."""

    def __init__(self, bootstrap: str, prefix: str = "", partitions: int = 3) -> None:
        from confluent_kafka import Producer

        self.bootstrap, self.prefix, self.partitions = bootstrap, prefix, partitions
        self.producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all", "linger.ms": 5})
        self._created: set[str] = set()

    def _name(self, topic: str) -> str:
        return self.prefix + topic

    def ensure(self, topic: str) -> None:
        from confluent_kafka.admin import AdminClient
        from confluent_kafka.cimpl import NewTopic

        name = self._name(topic)
        if name in self._created:
            return
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        if name not in admin.list_topics(timeout=10).topics:
            parts = 1 if topic.endswith((".dlq", "control.v1")) else self.partitions
            for fut in admin.create_topics([NewTopic(name, num_partitions=parts, replication_factor=1)]).values():
                try:
                    fut.result()
                except Exception as e:  # already created by a concurrent caller
                    if "TOPIC_ALREADY_EXISTS" not in str(e):
                        raise
        self._created.add(name)

    def produce(self, topic: str, key: str, value: dict[str, Any], headers: dict[str, str]) -> None:
        self.ensure(topic)
        self.producer.produce(
            self._name(topic), key=key.encode(), value=json.dumps(value, ensure_ascii=False).encode(), headers=list(headers.items())
        )
        self.producer.poll(0)

    def flush(self) -> None:
        if self.producer.flush(30) != 0:
            raise RuntimeError("messages still undelivered after 30 s")

    def read(self, topic: str) -> list[Message]:
        from confluent_kafka import Consumer, TopicPartition

        self.ensure(topic)
        name = self._name(topic)
        c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": f"imf-read-{uuid.uuid4().hex}", "enable.auto.commit": False})
        try:
            meta = c.list_topics(name, timeout=10).topics[name]
            ends, assign = {}, []
            for p in meta.partitions:
                lo, hi = c.get_watermark_offsets(TopicPartition(name, p), timeout=10)
                if hi > lo:
                    ends[p] = hi
                    assign.append(TopicPartition(name, p, lo))
            out: list[Message] = []
            if not assign:
                return out
            c.assign(assign)
            done: set[int] = set()
            deadline = time.monotonic() + 60
            while len(done) < len(ends):
                if time.monotonic() > deadline:
                    raise TimeoutError(f"could not read {name} to its end")
                m = c.poll(1.0)
                if m is None:
                    continue
                if m.error():
                    raise RuntimeError(str(m.error()))
                hdrs: dict[str, str] = {}
                for item in m.headers() or []:
                    k, v = item[0], item[1]
                    hdrs[str(k)] = v.decode() if isinstance(v, bytes) else str(v)
                key = m.key()
                key_text = key.decode() if isinstance(key, bytes) else (key or "")
                out.append(Message(key_text, json.loads(m.value() or b"{}"), hdrs))
                offset, partition = m.offset(), m.partition()
                assert offset is not None and partition is not None
                if offset + 1 >= ends[partition]:
                    done.add(partition)
            return out
        finally:
            c.close()


def _event_value(v: Value) -> Any:
    return str(v) if isinstance(v, Decimal) else v


def _total(values: list[Any]) -> str:
    return str(sum((Decimal(str(v)) for v in values if v not in (None, "")), Decimal(0)))


@dataclass
class IngestResult:
    batch_id: str
    read: int = 0
    filtered: int = 0
    published: int = 0
    rejected: dict[str, int] = field(default_factory=dict)


def ingest(
    spec: dict[str, Any], schema: dict[str, Any], source: Path, bus: Bus, batch_id: str | None = None, rounding: str = ROUND_HALF_UP
) -> IngestResult:
    batch_id = batch_id or uuid.uuid4().hex[:12]
    res = IngestResult(batch_id)
    validator = Draft202012Validator(schema)
    published: list[dict[str, Any]] = []
    flow = spec["flow"]

    def reject(rec: formats.SourceRecord, err: Rejected, key: str) -> None:
        res.rejected[err.reason] = res.rejected.get(err.reason, 0) + 1
        bus.produce(
            spec["dlq_topic"],
            key,
            {"flow": flow, "batch_id": batch_id, "seq": rec.seq, "reason": err.reason, "detail": err.detail, "key": key, "source": rec.raw},
            {"batch_id": batch_id, "seq": str(rec.seq)},
        )

    for rec in formats.read_source(spec["source"], source):
        res.read += 1
        key = ""
        try:
            if rec.error:
                raise rec.error
            if not check_filter(spec["filter"], rec.values):
                res.filtered += 1
                continue
            event: dict[str, Any] = {}
            errors: list[Rejected] = []
            for f in spec["fields"]:
                try:
                    event[f["to"]] = _event_value(apply(f["steps"], [rec.values.get(s, "") for s in f["from"]], f["to"], rounding))
                except Rejected as e:
                    errors.append(e)
                    event[f["to"]] = None
            key = "|".join("" if event.get(k) is None else str(event[k]) for k in spec["key"])
            if errors:
                raise errors[0]
            problems = sorted(validator.iter_errors(event), key=lambda e: list(e.path))
            if problems:
                p = problems[0]
                raise Rejected("schema_violation", f"{'/'.join(map(str, p.path)) or 'record'}: {p.message}")
        except Rejected as e:
            if not key:
                key = _best_effort_key(spec, rec)
            reject(rec, e, key)
            continue
        bus.produce(spec["topic"], key, event, {"batch_id": batch_id, "seq": str(rec.seq)})
        published.append(event)
        res.published += 1
    control = {
        "flow": flow,
        "batch_id": batch_id,
        "source": source.name,
        "records": res.published,
        "read": res.read,
        "filtered": res.filtered,
        "rejected": res.rejected,
        "totals": {c: _total([e[c] for e in published]) for c in spec["control_totals"]},
    }
    bus.flush()  # the control message goes out only after every record is acknowledged
    bus.produce(spec["control_topic"], f"{flow}|{batch_id}", control, {"batch_id": batch_id})
    bus.flush()
    return res


def _best_effort_key(spec: dict[str, Any], rec: formats.SourceRecord) -> str:
    """The record key computed leniently, so a dead letter can be matched to the legacy output."""
    parts = []
    for k in spec["key"]:
        f = next(f for f in spec["fields"] if f["to"] == k)
        values = [rec.values.get(s, "") for s in f["from"]]
        try:
            parts.append(str(apply([s for s in f["steps"] if s.split(":")[0] in ("trim", "upper", "lower", "join")], values, k)))
        except Exception:
            parts.append("")
    return "|".join(parts)


class IncompleteBatch(RuntimeError):
    pass


@dataclass
class DeliverResult:
    batch_id: str
    records: int
    duplicates: int
    truncated: dict[str, int]


def deliver(spec: dict[str, Any], bus: Bus, target: Path, batch_id: str) -> DeliverResult:
    controls = [
        m.value for m in bus.read(spec["control_topic"]) if m.headers.get("batch_id") == batch_id and m.value["flow"] == spec["flow"]
    ]
    if not controls:
        raise IncompleteBatch(f"no control message for batch {batch_id}")
    control = controls[-1]
    by_seq: dict[int, dict[str, Any]] = {}
    duplicates = 0
    for m in bus.read(spec["topic"]):
        if m.headers.get("batch_id") != batch_id:
            continue
        seq = int(m.headers["seq"])
        if seq in by_seq:
            duplicates += 1  # a retried send; the first copy wins
            continue
        by_seq[seq] = m.value
    if len(by_seq) != control["records"]:
        raise IncompleteBatch(f"batch {batch_id}: {len(by_seq)} records on the topic, control message announces {control['records']}")
    records = [by_seq[s] for s in sorted(by_seq)]
    totals = {c: _total([r[c] for r in records]) for c in spec["control_totals"]}
    if totals != control["totals"]:
        raise IncompleteBatch(f"batch {batch_id}: control totals {totals} differ from announced {control['totals']}")
    truncated = formats.write_target(spec["target"] | {"columns": spec["columns"]}, records, target)
    return DeliverResult(batch_id, len(records), duplicates, truncated)


def dead_letters(spec: dict[str, Any], bus: Bus, batch_id: str) -> list[dict[str, Any]]:
    return [m.value for m in bus.read(spec["dlq_topic"]) if m.headers.get("batch_id") == batch_id]
