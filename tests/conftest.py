from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest

PROJECT_ROOT = Path(__file__).parent.parent
FIXTURES_DIR = Path(__file__).parent / "fixtures"
MAPPINGS_DIR = PROJECT_ROOT / "mappings"


@dataclass
class MappingFixture:
    name: str
    mapping_yml: Path
    csv: Path
    table: str
    primary_key: str
    row_count: int


def _materialize_mapping(tmp_path: Path, mapping_filename: str) -> Path:
    src = MAPPINGS_DIR / mapping_filename
    dst = tmp_path / mapping_filename
    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
    return dst


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture
def telemetry(tmp_path: Path) -> MappingFixture:
    return MappingFixture(
        name="telemetry",
        mapping_yml=_materialize_mapping(tmp_path, "telemetry.yml"),
        csv=FIXTURES_DIR / "telemetry_sample.csv",
        table="telemetry_records",
        primary_key="record_id",
        row_count=20,
    )


@pytest.fixture
def qualification(tmp_path: Path) -> MappingFixture:
    return MappingFixture(
        name="qualification",
        mapping_yml=_materialize_mapping(tmp_path, "qualification.yml"),
        csv=FIXTURES_DIR / "qualification_sample.csv",
        table="qualification_results",
        primary_key="test_id",
        row_count=20,
    )


@pytest.fixture
def parts_inventory(tmp_path: Path) -> MappingFixture:
    return MappingFixture(
        name="parts_inventory",
        mapping_yml=_materialize_mapping(tmp_path, "parts_inventory.yml"),
        csv=FIXTURES_DIR / "parts_inventory_sample.csv",
        table="parts_inventory",
        primary_key="nsn",
        row_count=20,
    )


@pytest.fixture(params=["telemetry", "qualification", "parts_inventory"])
def any_mapping(request: pytest.FixtureRequest) -> MappingFixture:
    """Parameterized fixture — yields each mapping in turn."""
    return cast(MappingFixture, request.getfixturevalue(request.param))
