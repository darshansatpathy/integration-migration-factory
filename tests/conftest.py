from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from imf.harness import load_accepted
from imf.model import Estate, load_estate

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def estate() -> Estate:
    return load_estate(ROOT / "estate")


@pytest.fixture(scope="session")
def generated() -> Path:
    return ROOT / "generated"


@pytest.fixture(scope="session")
def rules() -> list[dict]:
    return load_accepted(ROOT / "accepted_differences.yaml")


@pytest.fixture
def estate_copy(tmp_path: Path) -> Path:
    dest = tmp_path / "estate"
    shutil.copytree(ROOT / "estate", dest)
    return dest
