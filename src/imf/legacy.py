"""A faithful stand-in for the legacy ESB's mapping engine.

In a real migration the golden files come from production runs of the legacy platform. This
repository has no ESB, so this module reproduces its documented behaviour, quirks included,
and produces the golden outputs the new platform is tested against. The quirks are the point:
they are what a rewrite silently changes.

- Every value is a string. Fixed-width numbers are converted through binary floating point.
- A value that does not parse becomes a blank (dates) or a zero (numbers). Nothing is rejected.
- `upper` and `lower` change only the ASCII letters a-z (the engine predates Unicode).
- `decimal:n` rounds half up from the float's shortest representation.
- `multiply` is done in binary floating point.
- Fixed-width output silently truncates long values.
"""

from __future__ import annotations

import csv
import io
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from imf.model import Estate, Flow, Layout

_ASCII_UP = str.maketrans("abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
_ASCII_DOWN = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


def _float(s: str) -> float:
    try:
        return float(s)
    except ValueError:
        return 0.0


def _num_text(x: float) -> str:
    return repr(x) if x != int(x) else str(int(x))


def _read(flow: Flow, estate: Estate, path: Path) -> list[dict[str, str]]:
    src = flow.source
    text = path.read_text(encoding="utf-8")
    if src.format == "csv":
        return list(csv.DictReader(io.StringIO(text), delimiter=src.delimiter))
    if src.format == "xml":
        assert src.record is not None
        return [{c.tag: c.text or "" for c in el} for el in ET.fromstring(text).iter(src.record)]
    layout = estate.layout(src.layout)
    rows = []
    for line in text.splitlines():
        row = {}
        for f in layout.fields:
            raw = line[f.start - 1 : f.start - 1 + f.length]
            if f.type == "decimal":
                digits = raw.strip()
                n = int(digits) if digits.lstrip("-").isdigit() else 0
                row[f.name] = _num_text(n / 10**f.scale)
            elif f.type == "integer":
                digits = raw.strip()
                row[f.name] = str(int(digits)) if digits.lstrip("-").isdigit() else "0"
            else:
                row[f.name] = raw
        rows.append(row)
    return rows


def _transform(steps: tuple[str, ...], values: list[str]) -> str:
    rest = list(steps)
    v = rest.pop(0).split(":", 1)[1].join(x.strip() for x in values) if len(values) > 1 else values[0]
    for s in rest:
        name, _, arg = s.partition(":")
        if name == "trim":
            v = v.strip()
        elif name == "upper":
            v = v.translate(_ASCII_UP)
        elif name == "lower":
            v = v.translate(_ASCII_DOWN)
        elif name == "default":
            v = v if v.strip() else arg
        elif name == "date":
            fmt_in, fmt_out = arg.split("->")
            try:
                v = datetime.strptime(v.strip(), fmt_in).strftime(fmt_out) if v.strip() else ""
            except ValueError:
                v = ""
        elif name == "decimal":
            v = str(Decimal(repr(_float(v))).quantize(Decimal(1).scaleb(-int(arg)), rounding=ROUND_HALF_UP))
        elif name == "multiply":
            v = _num_text(_float(v) * float(arg))
        elif name == "int":
            v = str(int(_float(v)))
    return v


def _passes(flow: Flow, row: dict[str, str]) -> bool:
    f = flow.filter
    if f is None:
        return True
    v = row.get(f.field, "")
    if f.op == "in":
        assert isinstance(f.value, tuple)
        return v in f.value
    if isinstance(f.value, float):
        x: float | str = _float(v)
        t: float | str = f.value
    else:
        x, t = v, str(f.value)
    return {"==": x == t, "!=": x != t, ">": x > t, ">=": x >= t, "<": x < t, "<=": x <= t}[f.op]  # type: ignore[operator]


def _fixed_line(row: dict[str, str], layout: Layout) -> str:
    out = []
    for f in sorted(layout.fields, key=lambda f: f.start):
        v = row.get(f.name, "")
        if f.type == "string":
            out.append(v[: f.length].ljust(f.length))
        else:
            out.append(str(int(_float(v) * 10**f.scale)).rjust(f.length, "0")[-f.length :])
    return "".join(out)


def run(flow: Flow, estate: Estate, source: Path, target: Path) -> int:
    """Map one source file to the target file as the legacy engine does. Returns records written."""
    out = []
    for row in _read(flow, estate, source):
        if not _passes(flow, row):
            continue
        out.append({m.to: _transform(m.steps, [row.get(s, "") for s in m.sources]) for m in flow.fields})
    columns = [m.to for m in flow.fields]
    tgt = flow.target
    target.parent.mkdir(parents=True, exist_ok=True)
    if tgt.format == "csv":
        with target.open("w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh, delimiter=tgt.delimiter, lineterminator="\n")
            w.writerow(columns)
            w.writerows([r[c] for c in columns] for r in out)
    elif tgt.format == "jsonl":
        target.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in out), encoding="utf-8")
    elif tgt.format == "fixed-width":
        layout = estate.layout(tgt.layout)
        target.write_text("".join(_fixed_line(r, layout) + "\n" for r in out), encoding="utf-8")
    else:
        raise ValueError(tgt.format)
    return len(out)
