from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from dataingest.config import Mapping
from dataingest.errors import MappingError
from dataingest.pipeline import Pipeline

_MAPPING = """
spec_version: 1
name: bills
source:
  format: csv
  group_by: BillId
target:
  table: TaxBill
  primary_key: BillId
  on_conflict: skip
fields:
  BillId:
    column: 0
    type: str
    required: true
    cleaners: [strip]
  OwnerName:
    column: 1
    type: str
    transient: true
    cleaners: [strip]
  Levy:
    column: 2
    type: str
    transient: true
    cleaners: [strip]
  Amount:
    column: 3
    type: decimal
    transient: true
    cleaners: [strip, parse_decimal]
children:
  - table: TaxParty
    foreign_key: BillId
    fields:
      Name: { from: OwnerName }
      PartyType: { value: owner }
  - table: TaxAssessment
    foreign_key: BillId
    for_each_row: true
    fields:
      Levy: { from: Levy }
      Amount: { from: Amount }
"""

_CSV = (
    "BillId,OwnerName,Levy,Amount\n"
    "B-1,John Smith,county,100.00\n"
    "B-1,John Smith,school,250.00\n"
    "B-1,John Smith,fire,30.00\n"
    "B-2,Jane Doe,county,80.00\n"
)


def _setup(tmp_path: Path) -> Path:
    (tmp_path / "bills.yml").write_text(_MAPPING, encoding="utf-8")
    (tmp_path / "bills.csv").write_text(_CSV, encoding="utf-8")
    return tmp_path


def test_grouping_one_parent_n_children(tmp_path: Path) -> None:
    _setup(tmp_path)
    db = tmp_path / "out.db"
    result = Pipeline(
        source_uri=f"csv:///{(tmp_path / 'bills.csv').as_posix()}",
        sink_uri=f"sqlite:///{db.as_posix()}",
        mapping=Mapping.from_yaml(tmp_path / "bills.yml"),
        error_log=tmp_path / "errors.jsonl",
    ).run()

    assert result.rows_in == 4
    assert result.rows_ok == 4
    assert result.rows_failed == 0

    engine = create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM TaxBill")).scalar() == 2
        assert conn.execute(text("SELECT COUNT(*) FROM TaxParty")).scalar() == 2
        assert conn.execute(text("SELECT COUNT(*) FROM TaxAssessment")).scalar() == 4
        b1_levies = (
            conn.execute(text("SELECT Levy FROM TaxAssessment WHERE BillId='B-1'")).scalars().all()
        )
        b2 = conn.execute(text("SELECT COUNT(*) FROM TaxAssessment WHERE BillId='B-2'")).scalar()

    assert set(b1_levies) == {"county", "school", "fire"}
    assert b2 == 1


def test_unknown_group_by_rejected(tmp_path: Path) -> None:
    p = tmp_path / "bad.yml"
    p.write_text(
        """
spec_version: 1
name: bad
source: { format: csv, group_by: NoSuchField }
target: { table: T, primary_key: id }
fields:
  id: { column: 0, type: str, required: true }
""",
        encoding="utf-8",
    )
    with pytest.raises(MappingError, match="group_by"):
        Mapping.from_yaml(p)
