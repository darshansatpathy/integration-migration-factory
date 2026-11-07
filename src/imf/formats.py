"""Reading source files and writing target files on the new platform.

Layouts are passed as plain dicts (as they appear in the generated ingest and deliver specs),
so the runtime needs nothing but those specs.
"""

from __future__ import annotations

import csv
import io
import json
import xml.etree.ElementTree as ET
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from imf.transforms import Rejected, Value


@dataclass
class SourceRecord:
    seq: int  # 1-based position in the source file
    values: dict[str, Value]
    error: Rejected | None = None
    raw: str = ""


def _fixed_value(raw: str, f: dict[str, Any]) -> Value:
    kind = f.get("type", "string")
    if kind == "string":
        return raw
    s = raw.strip()
    neg = s.startswith("-")
    digits = s[1:] if neg else s
    if not digits.isdigit():
        reason = "invalid_integer" if kind == "integer" else "invalid_decimal"
        raise Rejected(reason, f"{f['name']}={raw!r}")
    n = int(digits) * (-1 if neg else 1)
    if kind == "integer":
        return n
    return Decimal(n).scaleb(-int(f.get("scale", 0)))


def read_source(spec: dict[str, Any], path: Path) -> Iterator[SourceRecord]:
    fmt = spec["format"]
    text = path.read_text(encoding="utf-8")
    if fmt == "csv":
        reader = csv.reader(io.StringIO(text), delimiter=spec.get("delimiter", ","))
        header = next(reader) if spec.get("header", True) else spec["columns"]
        for i, row in enumerate(reader, 1):
            if len(row) != len(header):
                yield SourceRecord(
                    i, {}, Rejected("malformed_row", f"{len(row)} columns, expected {len(header)}"), spec.get("delimiter", ",").join(row)
                )
                continue
            yield SourceRecord(i, dict(zip(header, row, strict=True)), raw=spec.get("delimiter", ",").join(row))
    elif fmt == "fixed-width":
        fields = spec["layout"]
        width = max(f["start"] + f["length"] - 1 for f in fields)
        for i, line in enumerate(text.splitlines(), 1):
            if len(line) != width:
                yield SourceRecord(i, {}, Rejected("malformed_row", f"length {len(line)}, expected {width}"), line)
                continue
            values: dict[str, Value] = {}
            try:
                for f in fields:
                    values[f["name"]] = _fixed_value(line[f["start"] - 1 : f["start"] - 1 + f["length"]], f)
            except Rejected as e:
                # Keep the string fields, so the dead letter still identifies the record.
                for f in fields:
                    if f.get("type", "string") == "string":
                        values[f["name"]] = line[f["start"] - 1 : f["start"] - 1 + f["length"]]
                yield SourceRecord(i, values, e, line)
                continue
            yield SourceRecord(i, values, raw=line)
    elif fmt == "xml":
        root = ET.fromstring(text)
        for i, el in enumerate(root.iter(spec["record"]), 1):
            yield SourceRecord(i, {c.tag: (c.text or "") for c in el}, raw=ET.tostring(el, encoding="unicode"))
    else:
        raise ValueError(f"unsupported source format {fmt!r}")


def _text(v: Any) -> str:
    return "" if v is None else str(v)


def render_fixed(record: dict[str, Any], layout: Sequence[dict[str, Any]]) -> tuple[str, list[str]]:
    """One fixed-width line, and the names of fields that had to be truncated to fit."""
    out, truncated = [], []
    for f in sorted(layout, key=lambda f: f["start"]):
        v, n = record.get(f["name"]), f["length"]
        if f.get("type", "string") == "string":
            s = _text(v)
            if len(s) > n:
                truncated.append(f["name"])
            out.append(s[:n].ljust(n))
        else:
            scale = int(f.get("scale", 0))
            number = Decimal(_text(v) or 0).scaleb(scale)
            s = str(int(number)).rjust(n, "0")
            if len(s) > n:
                raise Rejected("field_overflow", f"{f['name']}={v!r} does not fit {n} digits")
            out.append(s)
    return "".join(out), truncated


def write_target(spec: dict[str, Any], records: Sequence[dict[str, Any]], path: Path) -> dict[str, int]:
    """Write the target file; returns counts of truncated values per field."""
    fmt = spec["format"]
    columns = spec["columns"]
    truncated: dict[str, int] = {}
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        with path.open("w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh, delimiter=spec.get("delimiter", ","), lineterminator="\n")
            w.writerow(columns)
            for r in records:
                w.writerow([_text(r.get(c)) for c in columns])
    elif fmt == "fixed-width":
        lines = []
        for r in records:
            line, cut = render_fixed(r, spec["layout"])
            for c in cut:
                truncated[c] = truncated.get(c, 0) + 1
            lines.append(line)
        path.write_text("".join(line + "\n" for line in lines), encoding="utf-8")
    elif fmt == "jsonl":
        path.write_text("".join(json.dumps({c: r.get(c) for c in columns}, ensure_ascii=False) + "\n" for r in records), encoding="utf-8")
    else:
        raise ValueError(f"unsupported target format {fmt!r}")
    return truncated


def read_target(fmt: str, path: Path, delimiter: str = ",", layout: Sequence[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Read a target file (from either platform) back into records, for comparison."""
    text = path.read_text(encoding="utf-8")
    if fmt == "csv":
        return list(csv.DictReader(io.StringIO(text), delimiter=delimiter))
    if fmt == "jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    if fmt == "fixed-width":
        assert layout is not None
        return [{f["name"]: line[f["start"] - 1 : f["start"] - 1 + f["length"]] for f in layout} for line in text.splitlines()]
    raise ValueError(f"unsupported target format {fmt!r}")
