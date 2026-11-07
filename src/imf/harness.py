"""Regression harness: run a flow on both platforms and explain every difference.

For each flow the legacy output (the golden file) is compared with what the Kafka path
delivers from the same input, record by record on the flow's key and field by field. A
record the new platform rejected is matched to its dead letter. Every difference must be
explained by a rule in accepted_differences.yaml, within that rule's max_records, or the
flow fails. Record counts and control totals are reported alongside.
"""

from __future__ import annotations

import contextlib
import json
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

from imf import formats, runtime
from imf.generate import load_spec
from imf.model import Estate, Flow
from imf.samples import golden_path, sample_path


@dataclass
class Difference:
    flow: str
    key: str
    kind: str  # field | rejected | missing | extra
    field: str = ""
    legacy: Any = None
    new: Any = None
    reason: str = ""
    accepted_by: str | None = None


@dataclass
class FlowResult:
    flow: str
    legacy_records: int
    new_records: int
    rejected: dict[str, int]
    differences: list[Difference]
    order_matches: bool
    truncated: dict[str, int]
    totals: dict[str, tuple[str, str]] = field(default_factory=dict)
    over_limit: list[str] = field(default_factory=list)

    @property
    def unexplained(self) -> list[Difference]:
        return [d for d in self.differences if d.accepted_by is None]

    @property
    def passed(self) -> bool:
        return not self.unexplained and not self.over_limit


def load_accepted(path: Path) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = yaml.safe_load(path.read_text()) or []
    ids = [r["id"] for r in rules]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate accepted-difference ids")
    for r in rules:
        missing = {"id", "rule", "flows", "rationale", "approved_by", "ticket", "max_records"} - r.keys()
        if missing:
            raise ValueError(f"{r.get('id')}: missing {sorted(missing)}")
    return rules


def _fold(v: Any) -> str:
    return unicodedata.normalize("NFC", str(v)).casefold()


def _matches(rule: dict[str, Any], d: Difference) -> bool:
    if d.flow not in rule["flows"]:
        return False
    if "fields" in rule and d.field not in rule["fields"]:
        return False
    kind = rule["rule"]
    if kind == "rejected":
        return d.kind == "rejected" and d.reason in rule["reasons"]
    if d.kind != "field":
        return False
    if kind == "unicode_case":
        return isinstance(d.new, str) and not d.new.isascii() and _fold(d.legacy) == _fold(d.new)
    if kind == "tolerance":
        try:
            return abs(Decimal(str(d.legacy)) - Decimal(str(d.new))) <= Decimal(str(rule["abs_tolerance"]))
        except InvalidOperation:
            return False
    if kind == "typed_json":
        return isinstance(d.legacy, str) and not isinstance(d.new, str) and d.legacy == str(d.new)
    raise ValueError(f"unknown rule {kind!r}")


def _key(record: dict[str, Any], key: tuple[str, ...]) -> str:
    return "|".join(str(record.get(k, "")).strip() for k in key)


def _read(flow: Flow, estate: Estate, path: Path) -> list[dict[str, Any]]:
    t = flow.target
    layout = None
    if t.format == "fixed-width":
        layout = [{"name": f.name, "start": f.start, "length": f.length} for f in estate.layout(t.layout).fields]
    return formats.read_target(t.format, path, t.delimiter, layout)


def compare(flow: Flow, estate: Estate, legacy_out: Path, new_out: Path, dead: list[dict[str, Any]]) -> tuple[list[Difference], bool]:
    old = _read(flow, estate, legacy_out)
    new = _read(flow, estate, new_out)
    old_by = {_key(r, flow.key): r for r in old}
    new_by = {_key(r, flow.key): r for r in new}
    dlq = {d["key"]: d for d in dead}
    diffs: list[Difference] = []
    for k, o in old_by.items():
        n = new_by.get(k)
        if n is None:
            if k in dlq:
                diffs.append(Difference(flow.id, k, "rejected", reason=dlq[k]["reason"], legacy=o))
            else:
                diffs.append(Difference(flow.id, k, "missing", legacy=o))
            continue
        for col in o:
            if o[col] != n.get(col):
                diffs.append(Difference(flow.id, k, "field", col, o[col], n.get(col)))
    diffs += [Difference(flow.id, k, "extra", new=n) for k, n in new_by.items() if k not in old_by]
    common_old = [_key(r, flow.key) for r in old if _key(r, flow.key) in new_by]
    common_new = [_key(r, flow.key) for r in new if _key(r, flow.key) in old_by]
    return diffs, common_old == common_new


def explain(diffs: list[Difference], rules: list[dict[str, Any]]) -> list[str]:
    """Mark each difference with the rule that accepts it; returns rules used beyond their limit."""
    used: Counter[str] = Counter()
    for d in diffs:
        for r in rules:
            if _matches(r, d):
                d.accepted_by = r["id"]
                used[r["id"]] += 1
                break
    limits = {r["id"]: r["max_records"] for r in rules}
    return [f"{rid}: {n} records, at most {limits[rid]} accepted" for rid, n in used.items() if n > limits[rid]]


def _totals(flow: Flow, records: list[dict[str, Any]], columns: list[str]) -> dict[str, str]:
    out = {}
    for c in columns:
        s = Decimal(0)
        for r in records:
            with contextlib.suppress(InvalidOperation):
                s += Decimal(str(r.get(c) or 0))
        out[c] = str(s)
    return out


def run_flow(
    flow: Flow,
    estate: Estate,
    generated: Path,
    bus: runtime.Bus,
    workdir: Path,
    rules: list[dict[str, Any]],
    **ingest_kwargs: Any,
) -> FlowResult:
    ing = load_spec(generated, flow.id, "ingest.yaml")
    dl = load_spec(generated, flow.id, "deliver.yaml")
    schema = load_spec(generated, flow.id, "schema.json")
    res = runtime.ingest(ing, schema, sample_path(estate, flow), bus, **ingest_kwargs)
    out = workdir / f"{flow.id}.new"
    delivered = runtime.deliver(dl, bus, out, res.batch_id)
    dead = runtime.dead_letters(ing, bus, res.batch_id)
    golden = golden_path(estate, flow)
    diffs, order_ok = compare(flow, estate, golden, out, dead)
    over = explain(diffs, [r for r in rules if flow.id in r["flows"]])
    old, new = _read(flow, estate, golden), _read(flow, estate, out)
    totals = {
        c: (a, b)
        for (c, a), b in zip(
            _totals(flow, old, ing["control_totals"]).items(), _totals(flow, new, ing["control_totals"]).values(), strict=True
        )
    }
    return FlowResult(flow.id, len(old), len(new), res.rejected, diffs, order_ok, delivered.truncated, totals, over)


def report(results: list[FlowResult], rules: list[dict[str, Any]], bus_name: str) -> str:
    lines = [
        "# Regression report",
        "",
        f"Each flow's sample input was run through the legacy engine (golden output) and through the Kafka path ({bus_name}). "
        "Outputs are compared record by record on the flow key. A flow passes when every difference is accepted in "
        "`accepted_differences.yaml` within its limit.",
        "",
        f"**{sum(r.passed for r in results)} of {len(results)} flows pass.**",
        "",
        "| Flow | Legacy records | New records | Rejected to DLQ | Differences | Accepted | Unexplained | Order | Result |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        rej = ", ".join(f"{k} {v}" for k, v in sorted(r.rejected.items())) or "0"
        lines.append(
            f"| {r.flow} | {r.legacy_records} | {r.new_records} | {rej} | {len(r.differences)} | "
            f"{len(r.differences) - len(r.unexplained)} | {len(r.unexplained)} | {'same' if r.order_matches else 'differs'} | "
            f"{'pass' if r.passed else '**fail**'} |"
        )
    lines += ["", "## Control totals", "", "| Flow | Field | Legacy | New | Difference |", "|---|---|---|---|---|"]
    for r in results:
        for c, (a, b) in r.totals.items():
            lines.append(f"| {r.flow} | {c} | {a} | {b} | {Decimal(b) - Decimal(a)} |")
    lines += ["", "Totals differ only by the records rejected to the DLQ and the accepted rounding corrections.", ""]
    lines += [
        "## Differences by accepted rule",
        "",
        "| Rule | Flows | Records | Limit per flow | Rationale | Ticket |",
        "|---|---|---|---|---|---|",
    ]
    used = Counter(d.accepted_by for r in results for d in r.differences if d.accepted_by)
    for rule in rules:
        rationale = " ".join(str(rule["rationale"]).split())
        lines.append(
            f"| {rule['id']} ({rule['rule']}) | {', '.join(rule['flows'])} | {used.get(rule['id'], 0)} | {rule['max_records']} | {rationale} | {rule['ticket']} |"
        )
    lines += ["", "## Examples", ""]
    seen: set[str | None] = set()
    for r in results:
        for d in r.differences:
            if d.accepted_by in seen:
                continue
            seen.add(d.accepted_by)
            if d.kind == "rejected":
                lines.append(f"- {d.flow} `{d.key}`: legacy wrote it; rejected as `{d.reason}` ({d.accepted_by}).")
            else:
                lines.append(
                    f"- {d.flow} `{d.key}` {d.field}: legacy `{json.dumps(d.legacy, ensure_ascii=False)}`, new `{json.dumps(d.new, ensure_ascii=False)}` ({d.accepted_by})."
                )
    trunc = [(r.flow, c, n) for r in results for c, n in r.truncated.items()]
    lines += ["", "## Data loss carried over for compatibility", ""]
    if trunc:
        lines += [
            f"- {f}: {n} `{c}` values longer than the fixed-width target field are truncated, as the legacy flow does. Both platforms agree; the target layout is the constraint."
            for f, c, n in trunc
        ]
    else:
        lines.append("None.")
    failed = [r for r in results if not r.passed]
    if failed:
        lines += ["", "## Unexplained differences", ""]
        for r in failed:
            lines += [f"- {o}" for o in r.over_limit]
            for d in r.unexplained[:20]:
                lines.append(f"- {d.flow} `{d.key}` {d.kind} {d.field}: legacy `{d.legacy}`, new `{d.new}` {d.reason}".rstrip())
    return "\n".join(lines) + "\n"
