from pathlib import Path

from sqlalchemy import create_engine, text

from dataingest.config import Mapping
from dataingest.pipeline import Pipeline

_MAPPING = """
spec_version: 1
name: bills
source: { format: csv, group_by: BillId }
target: { table: TaxBill, primary_key: BillId, on_conflict: skip }
fields:
  BillId: { column: 0, type: str, required: true, cleaners: [strip] }
  Description: { column: 1, type: str, transient: true, cleaners: [strip] }
  Amount: { column: 2, type: decimal, transient: true, cleaners: [strip, parse_decimal] }
  AssessmentType: { type: str, transient: true }
  TypeId: { type: int, transient: true }
transforms:
  - classify:
      source: Description
      target: AssessmentType
      rules:
        - { contains: [fire, acre], value: FIRE_ACRE }
        - { contains: real, value: REAL_ESTATE }
        - { contains: school, value: SCHOOL }
      default: OTHER
  - lookup:
      source: AssessmentType
      target: TypeId
      table: { REAL_ESTATE: 1, SCHOOL: 4, FIRE_ACRE: 26, OTHER: 0 }
children:
  - table: TaxAssessment
    foreign_key: BillId
    for_each_row: true
    fields:
      TypeId: { from: TypeId }
      Amount: { from: Amount }
"""

_CSV = (
    "BillId,Description,Amount\n"
    "B-1,Real Estate Tax,100.00\n"
    "B-1,School Levy,250.00\n"
    "B-1,Fire District Acreage,30.00\n"
)


def test_classify_lookup_grouping_relational_end_to_end(tmp_path: Path) -> None:
    (tmp_path / "m.yml").write_text(_MAPPING, encoding="utf-8")
    (tmp_path / "d.csv").write_text(_CSV, encoding="utf-8")
    db = tmp_path / "out.db"

    result = Pipeline(
        source_uri=f"csv:///{(tmp_path / 'd.csv').as_posix()}",
        sink_uri=f"sqlite:///{db.as_posix()}",
        mapping=Mapping.from_yaml(tmp_path / "m.yml"),
        error_log=tmp_path / "errors.jsonl",
    ).run()

    assert result.rows_ok == 3

    engine = create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM TaxBill")).scalar() == 1
        type_ids = (
            conn.execute(text("SELECT TypeId FROM TaxAssessment WHERE BillId='B-1'"))
            .scalars()
            .all()
        )
    assert sorted(type_ids) == [1, 4, 26]
