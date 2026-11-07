"""The legacy estate as data: flow definitions (ESB-style XML) and fixed-width layouts (YAML)."""

from __future__ import annotations

import itertools
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

import yaml

KNOWN_TRANSFORMS = {"trim", "upper", "lower", "date", "decimal", "default", "multiply", "join", "int"}
FORMATS = {"csv", "fixed-width", "xml", "jsonl"}


class EstateError(ValueError):
    """A flow or layout definition that cannot be migrated as written."""


@dataclass(frozen=True)
class LayoutField:
    name: str
    start: int  # 1-based, as in the copybook
    length: int
    type: str = "string"  # string | integer | decimal
    scale: int = 0  # implied decimal places


@dataclass(frozen=True)
class Layout:
    name: str
    fields: tuple[LayoutField, ...]

    @property
    def width(self) -> int:
        return max(f.start + f.length - 1 for f in self.fields)

    def field(self, name: str) -> LayoutField:
        for f in self.fields:
            if f.name == name:
                return f
        raise KeyError(name)


@dataclass(frozen=True)
class Endpoint:
    system: str
    protocol: str
    path: str
    format: str
    delimiter: str = ","
    header: bool = True
    layout: str | None = None
    record: str | None = None  # element name of one record, for xml


@dataclass(frozen=True)
class FieldMap:
    to: str
    sources: tuple[str, ...]
    steps: tuple[str, ...]  # e.g. ("trim", "date:%Y%m%d->%Y-%m-%d")


@dataclass(frozen=True)
class Filter:
    field: str
    op: str  # == != > >= < <= in
    value: str | float | tuple[str, ...]


@dataclass(frozen=True)
class Flow:
    id: str
    name: str
    domain: str
    owner: str
    schedule: str
    classification: str
    source: Endpoint
    target: Endpoint
    fields: tuple[FieldMap, ...]
    key: tuple[str, ...]
    filter: Filter | None = None
    depends_on: tuple[str, ...] = ()

    @property
    def topic(self) -> str:
        return f"{self.domain}.{self.id.lower()}.v1"

    def field_map(self, name: str) -> FieldMap:
        for f in self.fields:
            if f.to == name:
                return f
        raise KeyError(name)


@dataclass
class Estate:
    flows: dict[str, Flow]
    layouts: dict[str, Layout]
    root: Path = field(default_factory=Path)

    def layout(self, name: str | None) -> Layout:
        if name is None or name not in self.layouts:
            raise EstateError(f"unknown layout {name!r}")
        return self.layouts[name]


_FILTER = re.compile(r"^\s*(\w+)\s*(==|!=|>=|<=|>|<|in)\s*(.+?)\s*$")


def parse_filter(expr: str) -> Filter:
    m = _FILTER.match(expr)
    if not m:
        raise EstateError(f"unsupported filter expression {expr!r}")
    name, op, raw = m.groups()
    value: str | float | tuple[str, ...]
    if op == "in":
        if not (raw.startswith("(") and raw.endswith(")")):
            raise EstateError(f"'in' needs a parenthesised list: {expr!r}")
        value = tuple(v.strip().strip("'\"") for v in raw[1:-1].split(","))
    elif raw[:1] in "'\"":
        value = raw.strip("'\"")
    else:
        try:
            value = float(raw)
        except ValueError as e:
            raise EstateError(f"filter value must be a quoted string or a number: {expr!r}") from e
    return Filter(name, op, value)


def _endpoint(el: ET.Element | None, flow_id: str, which: str) -> Endpoint:
    if el is None:
        raise EstateError(f"{flow_id}: missing <{which}>")
    a = el.attrib
    fmt = a.get("format", "")
    if fmt not in FORMATS:
        raise EstateError(f"{flow_id}: unsupported {which} format {fmt!r}")
    if fmt == "fixed-width" and not a.get("layout"):
        raise EstateError(f"{flow_id}: fixed-width {which} needs a layout")
    if fmt == "xml" and not a.get("record"):
        raise EstateError(f"{flow_id}: xml {which} needs a record element")
    return Endpoint(
        system=a["system"],
        protocol=a.get("protocol", "sftp"),
        path=a["path"],
        format=fmt,
        delimiter=a.get("delimiter", ","),
        header=a.get("header", "true") == "true",
        layout=a.get("layout"),
        record=a.get("record"),
    )


def parse_flow(path: Path) -> Flow:
    root = ET.parse(path).getroot()
    if root.tag != "flow":
        raise EstateError(f"{path.name}: root element must be <flow>")
    a = root.attrib
    flow_id = a["id"]
    fields = []
    for f in root.iterfind("map/field"):
        steps = tuple(s for s in f.get("transform", "").split("|") if s)
        for s in steps:
            if s.split(":", 1)[0] not in KNOWN_TRANSFORMS:
                raise EstateError(f"{flow_id}: unknown transform {s!r} on {f.get('to')}")
        sources = tuple(s.strip() for s in f.attrib["from"].split(","))
        if len(sources) > 1 and not any(s.startswith("join:") for s in steps):
            raise EstateError(f"{flow_id}: {f.get('to')} reads several fields but does not join them")
        fields.append(FieldMap(f.attrib["to"], sources, steps))
    if not fields:
        raise EstateError(f"{flow_id}: empty map")
    target = root.find("target")
    key_el = root.find("key")
    if key_el is not None:
        key = tuple(k.strip() for k in key_el.attrib["fields"].split(","))
    elif target is not None and target.get("key"):
        key = (target.attrib["key"],)
    else:
        raise EstateError(f"{flow_id}: no record key")
    names = {f.to for f in fields}
    if not set(key) <= names:
        raise EstateError(f"{flow_id}: key {key} is not in the map")
    filt = root.find("filter")
    return Flow(
        id=flow_id,
        name=a["name"],
        domain=a["domain"],
        owner=a["owner"],
        schedule=a["schedule"],
        classification=a.get("classification", "internal"),
        source=_endpoint(root.find("source"), flow_id, "source"),
        target=_endpoint(target, flow_id, "target"),
        fields=tuple(fields),
        key=key,
        filter=parse_filter(filt.attrib["expr"]) if filt is not None else None,
        depends_on=tuple(d.attrib["flow"] for d in root.iterfind("depends-on")),
    )


def parse_layout(path: Path) -> Layout:
    doc = yaml.safe_load(path.read_text())
    fields = tuple(
        LayoutField(f["name"], int(f["start"]), int(f["length"]), f.get("type", "string"), int(f.get("scale", 0))) for f in doc["fields"]
    )
    spans = sorted((f.start, f.start + f.length - 1, f.name) for f in fields)
    for (_, end, a), (start, _, b) in itertools.pairwise(spans):
        if start <= end:
            raise EstateError(f"layout {doc['name']}: {a} overlaps {b}")
    return Layout(doc["name"], fields)


def load_estate(root: Path) -> Estate:
    layouts = {lay.name: lay for lay in (parse_layout(p) for p in sorted((root / "layouts").glob("*.yaml")))}
    flows: dict[str, Flow] = {}
    for p in sorted((root / "flows").glob("*.xml")):
        flow = parse_flow(p)
        if flow.id in flows:
            raise EstateError(f"duplicate flow id {flow.id}")
        for end in (flow.source, flow.target):
            if end.format == "fixed-width" and end.layout not in layouts:
                raise EstateError(f"{flow.id}: unknown layout {end.layout!r}")
        flows[flow.id] = flow
    for flow in flows.values():
        missing = set(flow.depends_on) - flows.keys()
        if missing:
            raise EstateError(f"{flow.id}: depends on unknown flows {sorted(missing)}")
    return Estate(flows, layouts, root)
