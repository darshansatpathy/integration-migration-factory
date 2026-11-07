from __future__ import annotations

import os
from pathlib import Path

import pytest

from imf.cli import main

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _cwd():
    old = Path.cwd()
    os.chdir(ROOT)
    yield
    os.chdir(old)


def test_checks_pass(capsys):
    assert main(["generate", "--check"]) == 0
    assert main(["samples", "--check"]) == 0
    assert "reproducible" in capsys.readouterr().out


def test_inventory(tmp_path):
    assert main(["inventory", "--out", str(tmp_path / "inv.md"), "--csv", str(tmp_path / "inv.csv")]) == 0
    assert "Interface inventory" in (tmp_path / "inv.md").read_text()
    assert len((tmp_path / "inv.csv").read_text().splitlines()) == 11


def test_regress_and_run(tmp_path, capsys):
    assert main(["regress", "--report", str(tmp_path / "r.md")]) == 0
    assert "10 of 10" in (tmp_path / "r.md").read_text()
    assert main(["run", "--flow", "PRC-001", "--input", "estate/samples/PRC-001/prices.xml", "--output", str(tmp_path / "p.csv")]) == 0
    assert "published 120" in capsys.readouterr().out


def test_generate_writes(tmp_path):
    assert main(["generate", "--out", str(tmp_path / "g")]) == 0
    assert (tmp_path / "g" / "ORD-001" / "RUNBOOK.md").exists()


def test_compare(tmp_path, capsys):
    assert main(["run", "--flow", "PRC-001", "--input", "estate/samples/PRC-001/prices.xml", "--output", str(tmp_path / "p.csv")]) == 0
    assert main(["compare", "--flow", "PRC-001", "--legacy", "estate/golden/PRC-001.csv", "--new", str(tmp_path / "p.csv")]) == 0
    tampered = tmp_path / "t.csv"
    tampered.write_text((tmp_path / "p.csv").read_text().replace("35.36", "35.37"))
    assert main(["compare", "--flow", "PRC-001", "--legacy", "estate/golden/PRC-001.csv", "--new", str(tampered)]) == 1
    assert "list_price legacy='35.36' new='35.37'" in capsys.readouterr().out


def test_topics_against_a_broker(capsys):
    bootstrap = os.environ.get("IMF_BOOTSTRAP")
    if not bootstrap:
        pytest.skip("IMF_BOOTSTRAP not set")
    import time

    assert main(["topics", "--bootstrap", bootstrap, "--prefix", f"topics-{time.time_ns()}."]) == 0
    assert "21 topics defined, 21 created" in capsys.readouterr().out
