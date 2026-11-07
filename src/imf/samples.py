"""Deterministic sample input files for every flow, with the edge cases a migration has to get right.

Each generator plants specific cases at known rates: invalid dates, malformed numbers,
non-ASCII names, values that sit exactly on a rounding boundary, values too long for a
fixed-width target, and records the flow's filter removes.
"""

from __future__ import annotations

import random
import xml.etree.ElementTree as ET
from collections.abc import Callable
from datetime import date, datetime, timedelta
from pathlib import Path

from imf.model import Estate, Flow

FIRST = ["Anna", "Ben", "Chen", "Dana", "Eli", "Femi", "Gita", "Hugo", "Ines", "Jon", "Kai", "Lena"]
LAST = ["Smith", "Garcia", "Okafor", "Novak", "Rossi", "Tanaka", "Weber", "Silva", "Khan", "Berg"]
NON_ASCII = ["Müller", "Ørsted", "Çelik", "José Peña", "Šimić", "Łukasz Żak", "Ångström"]
COUNTRIES = ["de", "fr", "gb", "in", "us", "nl", "se", "pl"]
SKUS = [f"SKU-{i:05d}" for i in range(1, 121)]
BINS = ["a-01", "a-02", "b-11", "c-07", "d-03", "recv", "ret-1"]

N_RECORDS = 300


def _name(rng: random.Random) -> str:
    if rng.random() < 0.08:
        return f"{rng.choice(FIRST)} {rng.choice(NON_ASCII)}"
    return f"{rng.choice(FIRST)} {rng.choice(LAST)}"


def _day(rng: random.Random) -> date:
    return date(2025, 1, 1) + timedelta(days=rng.randrange(600))


def _fixed(values: list[tuple[str, int, bool]]) -> str:
    """values: (text, width, numeric). Numbers are right-aligned with zeros, text left-aligned."""
    return "".join(v.rjust(w, "0")[-w:] if num else v[:w].ljust(w) for v, w, num in values)


def customers(rng: random.Random) -> str:
    lines = ["KUNNR;NAME1;SMTP_ADDR;LAND1;ERDAT"]
    for i in range(N_RECORDS):
        name = _name(rng)
        d = _day(rng).strftime("%d.%m.%Y")
        if i % 47 == 13:
            d = "31.02.2024"  # invalid date
        email = f"  {name.split()[0]}.{i}@Example.COM ".replace(" ", "", 1)
        lines.append(f" C{i:06d} ;  {name} ;{email};{rng.choice(COUNTRIES)};{d}")
    return "\n".join(lines) + "\n"


def orders(rng: random.Random) -> str:
    lines = []
    for i in range(N_RECORDS):
        d = _day(rng).strftime("%Y%m%d")
        if i % 61 == 7:
            d = "20240230"
        amount = str(rng.randrange(100, 2_500_000))
        if i % 97 == 41:
            amount = "12A45"  # corrupted numeric field
        status = "X" if i % 11 == 0 else rng.choice("ONS")
        lines.append(
            _fixed(
                [
                    (f"ORD{i:07d}", 10, False),
                    (d, 8, False),
                    (f"C{rng.randrange(N_RECORDS):06d}", 8, False),
                    (_name(rng), 30, False),
                    (amount, 11, True),
                    (rng.choice(["EUR", "USD", "GBP"]), 3, False),
                    (status, 1, False),
                ]
            )
        )
    return "\n".join(lines) + "\n"


def order_lines(rng: random.Random) -> str:
    lines = []
    for i in range(N_RECORDS):
        qty = "0" if i % 23 == 5 else str(rng.randrange(1, 40))
        price = rng.randrange(500, 900_000)
        if i % 5 == 0:
            price = price // 10 * 10 + 5  # exactly half a cent: the rounding rule decides
        lines.append(
            _fixed(
                [
                    (f"ORD{i // 3:07d}", 10, False),
                    (str(i % 3 + 1), 4, True),
                    (rng.choice(SKUS), 12, False),
                    (qty, 6, True),
                    (str(price), 9, True),
                ]
            )
        )
    return "\n".join(lines) + "\n"


def stock(rng: random.Random) -> str:
    lines = ["sku,bin,qty,desc"]
    seen = set()
    while len(lines) <= N_RECORDS:
        sku, b = rng.choice(SKUS), rng.choice(BINS)
        if (sku, b) in seen:
            continue
        seen.add((sku, b))
        desc = rng.choice(["Widget", "Gadget", "Sprocket", "Bracket"]) + " " + rng.choice(["small", "large", "steel", "blue"])
        if len(lines) % 29 == 3:
            desc = "Heavy duty stainless steel bracket, pack of 12"  # longer than the 20-character target field
        if "," in desc:
            desc = f'"{desc}"'
        lines.append(f"{sku},{b},{rng.randrange(0, 5000)},{desc}")
    return "\n".join(lines) + "\n"


def movements(rng: random.Random) -> str:
    lines = ["id,sku,from_bin,to_bin,qty,ts"]
    for i in range(N_RECORDS):
        ts = (datetime(2025, 6, 1) + timedelta(minutes=rng.randrange(200_000))).strftime("%Y-%m-%d %H:%M:%S")
        if i % 53 == 17:
            ts = "2025-13-01 10:00:00"
        frm = "" if i % 9 == 0 else rng.choice(BINS)
        lines.append(f"MV{i:07d},{rng.choice(SKUS)},{frm},{rng.choice(BINS)},{rng.randrange(1, 200)},{ts}")
    return "\n".join(lines) + "\n"


def prices(rng: random.Random) -> str:
    root = ET.Element("prices")
    for i, sku in enumerate(SKUS):
        el = ET.SubElement(root, "price")
        list_price = rng.randrange(99, 99_999) / 100
        if i % 4 == 0:
            list_price = rng.randrange(99, 9_999) / 100 + 0.005  # three decimals, on the half
        ET.SubElement(el, "sku").text = sku
        ET.SubElement(el, "list").text = f"{list_price:.3f}"
        ET.SubElement(el, "promo").text = "" if i % 3 else f"{list_price * 0.8:.2f}"
        ET.SubElement(el, "from").text = _day(rng).isoformat()
    ET.indent(root)
    return ET.tostring(root, encoding="unicode") + "\n"


def invoices(rng: random.Random) -> str:
    lines = []
    for i in range(N_RECORDS):
        net_milli = rng.randrange(1_000, 5_000_000)
        if i % 10 == 0:
            # Net amounts whose gross (net x 1.18) lands exactly on half a cent: binary floating
            # point rounds some of them down, exact decimal arithmetic rounds them up.
            while net_milli * 118 % 1000 != 500:
                net_milli += 1
        lines.append(
            _fixed(
                [
                    (f"INV{i:09d}", 12, False),
                    (_day(rng).strftime("%Y%m%d"), 8, False),
                    (f"C{rng.randrange(N_RECORDS):06d}", 8, False),
                    (str(net_milli), 12, True),
                    (rng.choice(["S1", "S1", "S2", "Z0"]), 2, False),
                ]
            )
        )
    return "\n".join(lines) + "\n"


def payments(rng: random.Random) -> str:
    lines = ["ref,invoice,amount,value_date"]
    for i in range(N_RECORDS):
        lines.append(
            f" pay-{i:06d},INV{rng.randrange(N_RECORDS):09d},{rng.randrange(100, 500_000) / 100:.2f},{_day(rng).strftime('%d/%m/%Y')}"
        )
    return "\n".join(lines) + "\n"


def suppliers(rng: random.Random) -> str:
    lines = ["LIFNR|NAME1|LAND1|ZTERM"]
    for i in range(N_RECORDS // 2):
        lines.append(f"V{i:07d}|{_name(rng)} Supplies|{rng.choice(COUNTRIES)}|{'' if i % 4 == 0 else rng.choice(['N30', 'N45', 'N60'])}")
    return "\n".join(lines) + "\n"


def returns(rng: random.Random) -> str:
    lines = ["rma,order,sku,qty,reason"]
    for i in range(N_RECORDS):
        reason = rng.choice(["damaged", "wrong_item", "changed_mind", "Damaged", "late"])
        lines.append(f"rma-{i:06d},ORD{rng.randrange(N_RECORDS):07d},{rng.choice(SKUS)},{rng.randrange(1, 5)},{reason}")
    return "\n".join(lines) + "\n"


GENERATORS: dict[str, tuple[Callable[[random.Random], str], str]] = {
    "CUST-001": (customers, "customers.csv"),
    "ORD-001": (orders, "orders.dat"),
    "ORD-002": (order_lines, "orderlines.dat"),
    "INV-001": (stock, "stock.csv"),
    "INV-002": (movements, "movements.csv"),
    "PRC-001": (prices, "prices.xml"),
    "INVC-001": (invoices, "invoices.dat"),
    "PAY-001": (payments, "payments.csv"),
    "SUP-001": (suppliers, "suppliers.csv"),
    "RET-001": (returns, "returns.csv"),
}

TARGET_EXT = {"csv": "csv", "jsonl": "jsonl", "fixed-width": "dat"}


def sample_path(estate: Estate, flow: Flow) -> Path:
    return estate.root / "samples" / flow.id / GENERATORS[flow.id][1]


def golden_path(estate: Estate, flow: Flow) -> Path:
    return estate.root / "golden" / f"{flow.id}.{TARGET_EXT[flow.target.format]}"


def write_samples(estate: Estate) -> list[Path]:
    paths = []
    for flow in estate.flows.values():
        gen, _ = GENERATORS[flow.id]
        path = sample_path(estate, flow)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(gen(random.Random(f"imf-{flow.id}")), encoding="utf-8")
        paths.append(path)
    return paths
