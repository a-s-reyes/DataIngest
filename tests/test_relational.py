from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text

from dataingest.config import Mapping
from dataingest.pipeline import Pipeline


def _write_mapping(tmp_path: Path) -> Path:
    p = tmp_path / "bills.yml"
    p.write_text(
        """
spec_version: 1
name: bills
source:
  format: csv
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
  FaceAmount:
    column: 1
    type: decimal
    cleaners: [strip, parse_decimal]
  OwnerName:
    column: 2
    type: str
    transient: true
    cleaners: [strip]
  MailerName:
    column: 3
    type: str
    transient: true
    cleaners: [strip]
children:
  - table: TaxParty
    foreign_key: BillId
    fields:
      Name: { from: OwnerName }
      PartyType: { value: owner }
  - table: TaxParty
    foreign_key: BillId
    fields:
      Name: { from: MailerName }
      PartyType: { value: mailer }
""",
        encoding="utf-8",
    )
    return p


def _write_csv(tmp_path: Path) -> Path:
    csv = tmp_path / "bills.csv"
    csv.write_text(
        "BillId,FaceAmount,OwnerName,MailerName\n"
        "B-1,100.50,John Smith,Jane Doe\n"
        "B-2,200.00,Acme LLC,Acme LLC\n",
        encoding="utf-8",
    )
    return csv


def _run(tmp_path: Path, db: Path) -> None:
    Pipeline(
        source_uri=f"csv:///{_write_csv(tmp_path).as_posix()}",
        sink_uri=f"sqlite:///{db.as_posix()}",
        mapping=Mapping.from_yaml(_write_mapping(tmp_path)),
        error_log=tmp_path / "errors.jsonl",
    ).run()


def test_parent_table_excludes_transient_child_source_fields(tmp_path: Path) -> None:
    db = tmp_path / "out.db"
    _run(tmp_path, db)
    engine = create_engine(f"sqlite:///{db}")
    cols = {c["name"] for c in inspect(engine).get_columns("TaxBill")}
    assert cols == {"BillId", "FaceAmount"}


def test_children_written_with_foreign_key(tmp_path: Path) -> None:
    db = tmp_path / "out.db"
    _run(tmp_path, db)
    engine = create_engine(f"sqlite:///{db}")

    with engine.connect() as conn:
        bills = conn.execute(text("SELECT COUNT(*) FROM TaxBill")).scalar()
        parties = conn.execute(text("SELECT COUNT(*) FROM TaxParty")).scalar()
        b1 = conn.execute(
            text("SELECT Name, PartyType FROM TaxParty WHERE BillId = 'B-1' ORDER BY PartyType")
        ).all()

    assert bills == 2
    assert parties == 4
    assert [(r.Name, r.PartyType) for r in b1] == [("Jane Doe", "mailer"), ("John Smith", "owner")]


def test_skip_rerun_does_not_duplicate_children(tmp_path: Path) -> None:
    db = tmp_path / "out.db"
    _run(tmp_path, db)
    _run(tmp_path, db)
    engine = create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        bills = conn.execute(text("SELECT COUNT(*) FROM TaxBill")).scalar()
        parties = conn.execute(text("SELECT COUNT(*) FROM TaxParty")).scalar()
    assert bills == 2
    assert parties == 4


def test_relational_with_multi_sink_rejected(tmp_path: Path) -> None:
    db1 = tmp_path / "a.db"
    db2 = tmp_path / "b.db"
    pipeline = Pipeline(
        source_uri=f"csv:///{_write_csv(tmp_path).as_posix()}",
        sink_uri=[f"sqlite:///{db1.as_posix()}", f"sqlite:///{db2.as_posix()}"],
        mapping=Mapping.from_yaml(_write_mapping(tmp_path)),
        error_log=tmp_path / "errors.jsonl",
    )
    with pytest.raises(ValueError, match="single SQL sink"):
        pipeline.run()


def test_child_references_unknown_parent_field_rejected(tmp_path: Path) -> None:
    p = tmp_path / "bad.yml"
    p.write_text(
        """
spec_version: 1
name: bad
source: { format: csv }
target: { table: T, primary_key: id }
fields:
  id: { column: 0, type: str, required: true }
children:
  - table: C
    foreign_key: id
    fields:
      Name: { from: NoSuchField }
""",
        encoding="utf-8",
    )
    with pytest.raises(Exception, match="unknown parent field"):
        Mapping.from_yaml(p)
