"""Generate each flow's Kafka replacement from templates.

For every legacy flow the factory renders the same set of artifacts: the topics, a JSON Schema
for the canonical event, the ingest spec (file to topic) and the deliver spec (topic to file),
and a cutover runbook. The runtime reads nothing but these specs, so a migrated flow is fully
described by reviewed, versioned files, and `imf generate --check` fails when they drift from
the estate or the templates.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jinja2 import Environment, PackageLoader, StrictUndefined

from imf.model import Estate, FieldMap, Flow

CONTROL_TOPIC = "imf.control.v1"
RETENTION_DAYS = {"confidential": 7, "internal": 14, "public": 30}
FILES = ["topics.yaml", "schema.json", "ingest.yaml", "deliver.yaml", "RUNBOOK.md"]

_STRF = {"%Y": r"\d{4}", "%m": r"\d{2}", "%d": r"\d{2}", "%H": r"\d{2}", "%M": r"\d{2}", "%S": r"\d{2}"}


@dataclass(frozen=True)
class FieldType:
    kind: str  # string | integer | decimal | date
    scale: int = 0
    pattern: str | None = None


def _date_pattern(fmt: str) -> str:
    """A regular expression for strftime output; portable to ECMA-262, so no needless escapes."""
    out = "".join("\\" + c if c in ".^$*+?()[]{}|\\" else c for c in fmt)
    for code, rx in _STRF.items():
        out = out.replace(code, rx)
    return out


def field_type(flow: Flow, m: FieldMap, estate: Estate) -> FieldType:
    """The type of a mapped field, from the last step that decides it, else from the source layout."""
    for s in reversed(m.steps):
        name, _, arg = s.partition(":")
        if name == "decimal":
            return FieldType("decimal", int(arg))
        if name == "int":
            return FieldType("integer")
        if name == "date":
            return FieldType("date", pattern=_date_pattern(arg.split("->")[1]))
        if name == "multiply":
            return FieldType("decimal", -1)
        if name == "join":
            return FieldType("string")
    if flow.source.format == "fixed-width" and len(m.sources) == 1:
        f = estate.layout(flow.source.layout).field(m.sources[0])
        if f.type == "integer":
            return FieldType("integer")
        if f.type == "decimal":
            return FieldType("decimal", f.scale)
    return FieldType("string")


def json_schema(flow: Flow, estate: Estate) -> dict[str, Any]:
    props: dict[str, Any] = {}
    for m in flow.fields:
        t = field_type(flow, m, estate)
        if t.kind == "integer":
            p: dict[str, Any] = {"type": "integer"}
        elif t.kind == "decimal":
            digits = r"\d+" if t.scale < 0 else (rf"\d+\.\d{{{t.scale}}}" if t.scale else r"\d+")
            p = {"type": "string", "pattern": rf"^-?{digits}$", "description": "decimal, carried as a string to keep its precision"}
        elif t.kind == "date":
            p = {"type": "string", "pattern": rf"^({t.pattern})?$"}
        else:
            p = {"type": "string"}
        if m.to in flow.key:
            p["minLength"] = 1
            if p.get("type") == "string" and "pattern" in p:
                p["pattern"] = p["pattern"].replace(")?$", ")$")
        props[m.to] = p
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"urn:imf:{flow.topic}",
        "title": flow.name,
        "type": "object",
        "properties": props,
        "required": [m.to for m in flow.fields],
        "additionalProperties": False,
    }


def _layout(estate: Estate, name: str | None) -> list[dict[str, Any]]:
    return [{"name": f.name, "start": f.start, "length": f.length, "type": f.type, "scale": f.scale} for f in estate.layout(name).fields]


def context(flow: Flow, estate: Estate) -> dict[str, Any]:
    src, tgt = flow.source, flow.target
    source: dict[str, Any] = {"system": src.system, "protocol": src.protocol, "path": src.path, "format": src.format}
    if src.format == "csv":
        source |= {"delimiter": src.delimiter, "header": src.header}
    if src.format == "xml":
        source["record"] = src.record
    if src.format == "fixed-width":
        source["layout"] = _layout(estate, src.layout)
    target: dict[str, Any] = {"system": tgt.system, "protocol": tgt.protocol, "path": tgt.path, "format": tgt.format}
    if tgt.format == "csv":
        target["delimiter"] = tgt.delimiter
    if tgt.format == "fixed-width":
        target["layout"] = _layout(estate, tgt.layout)
    types = {m.to: field_type(flow, m, estate) for m in flow.fields}
    flt = flow.filter
    return {
        "flow": flow,
        "topic": flow.topic,
        "dlq_topic": flow.topic + ".dlq",
        "control_topic": CONTROL_TOPIC,
        "retention_ms": RETENTION_DAYS[flow.classification] * 86_400_000,
        "source": source,
        "target": target,
        "filter": None
        if flt is None
        else {"field": flt.field, "op": flt.op, "value": list(flt.value) if isinstance(flt.value, tuple) else flt.value},
        "fields": [{"to": m.to, "from": list(m.sources), "steps": list(m.steps), "type": types[m.to].kind} for m in flow.fields],
        "control_totals": [m.to for m in flow.fields if types[m.to].kind == "decimal"],
        "columns": [m.to for m in flow.fields],
        "schema": json_schema(flow, estate),
    }


def _env() -> Environment:
    env = Environment(
        loader=PackageLoader("imf", "templates"),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["j"] = lambda v: json.dumps(v, ensure_ascii=False)
    env.filters["pretty"] = lambda v: json.dumps(v, indent=2, ensure_ascii=False)
    return env


def render(flow: Flow, estate: Estate) -> dict[str, str]:
    env, ctx = _env(), context(flow, estate)
    return {name: env.get_template(name + ".j2").render(**ctx) for name in FILES}


def render_all(estate: Estate) -> dict[Path, str]:
    out: dict[Path, str] = {}
    for flow in estate.flows.values():
        for name, text in render(flow, estate).items():
            out[Path(flow.id) / name] = text
    out[Path("platform.yaml")] = (
        _env().get_template("platform.yaml.j2").render(control_topic=CONTROL_TOPIC, flows=list(estate.flows.values()))
    )
    return out


def write(estate: Estate, dest: Path) -> list[Path]:
    paths = []
    for rel, text in render_all(estate).items():
        p = dest / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
        paths.append(p)
    return paths


def drift(estate: Estate, dest: Path) -> list[str]:
    """Files whose committed content differs from what the templates produce now."""
    expected = render_all(estate)
    problems = [str(rel) for rel, text in expected.items() if not (dest / rel).exists() or (dest / rel).read_text(encoding="utf-8") != text]
    on_disk = {p.relative_to(dest) for p in dest.rglob("*") if p.is_file()} if dest.exists() else set()
    problems += [f"{rel} (not generated by any flow)" for rel in sorted(on_disk - expected.keys())]
    return problems


def load_spec(dest: Path, flow_id: str, name: str) -> dict[str, Any]:
    import yaml

    text = (dest / flow_id / name).read_text(encoding="utf-8")
    loaded: dict[str, Any] = json.loads(text) if name.endswith(".json") else yaml.safe_load(text)
    return loaded
