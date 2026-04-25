from pathlib import Path

import pytest

from dataingest.config import Mapping
from dataingest.errors import MappingError


def test_clay_mapping_loads(clay_mapping: Path):
    m = Mapping.from_yaml(clay_mapping)
    assert m.vendor == "clay-sheriff-ky"
    assert m.spec_version == 1
    assert m.target.table == "tax_bills"
    assert m.target.primary_key == "bill_number"
    assert "bill_number" in m.fields
    assert m.fields["face_amount"].required is True


def test_unknown_cleaner_rejected(tmp_path: Path):
    bad = tmp_path / "bad.yml"
    bad.write_text(
        """
spec_version: 1
vendor: bad
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


def test_primary_key_must_be_a_field(tmp_path: Path):
    bad = tmp_path / "bad.yml"
    bad.write_text(
        """
spec_version: 1
vendor: bad
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


def test_invalid_yaml_raises(tmp_path: Path):
    bad = tmp_path / "bad.yml"
    bad.write_text(":\n  - this is not\n  valid yaml: [", encoding="utf-8")
    with pytest.raises(MappingError):
        Mapping.from_yaml(bad)
