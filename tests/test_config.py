from pathlib import Path

import pytest

from dataingest.config import Mapping
from dataingest.errors import MappingError

from .conftest import MappingFixture


def test_telemetry_mapping_loads(telemetry: MappingFixture) -> None:
    m = Mapping.from_yaml(telemetry.mapping_yml)
    assert m.name == "flight-test-telemetry"
    assert m.spec_version == 1
    assert m.target.table == "telemetry_records"
    assert m.target.primary_key == "record_id"
    assert "record_id" in m.fields
    assert m.fields["value"].required is True


def test_qualification_mapping_loads(qualification: MappingFixture) -> None:
    m = Mapping.from_yaml(qualification.mapping_yml)
    assert m.name == "component-qualification-tests"
    assert m.target.table == "qualification_results"
    assert m.target.primary_key == "test_id"
    assert m.fields["measured_value"].required is True


def test_parts_inventory_mapping_loads(parts_inventory: MappingFixture) -> None:
    m = Mapping.from_yaml(parts_inventory.mapping_yml)
    assert m.name == "parts-inventory"
    assert m.target.table == "parts_inventory"
    assert m.target.primary_key == "nsn"
    assert m.fields["qty_on_hand"].required is True


def test_unknown_cleaner_rejected(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yml"
    bad.write_text(
        """
spec_version: 1
name: bad
source: { format: csv }
target: { table: t, primary_key: x }
fields:
  x:
    column: 0
    cleaners: [strip, no_such_cleaner]
""",
        encoding="utf-8",
    )
    with pytest.raises(MappingError, match="no_such_cleaner"):
        Mapping.from_yaml(bad)


def test_primary_key_must_be_a_field(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yml"
    bad.write_text(
        """
spec_version: 1
name: bad
source: { format: csv }
target: { table: t, primary_key: missing_field }
fields:
  x:
    column: 0
""",
        encoding="utf-8",
    )
    with pytest.raises(MappingError, match="primary_key"):
        Mapping.from_yaml(bad)


def test_invalid_yaml_raises(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yml"
    bad.write_text(":\n  - this is not\n  valid yaml: [", encoding="utf-8")
    with pytest.raises(MappingError):
        Mapping.from_yaml(bad)
