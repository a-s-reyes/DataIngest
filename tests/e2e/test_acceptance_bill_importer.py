from pathlib import Path

from sqlalchemy import create_engine, inspect, text

from dataingest.config import Mapping
from dataingest.pipeline import Pipeline

_MAPPING = """
spec_version: 2
name: ddi-delinquent-unpaid
description: DDI Generic UnPaid multi-line delinquent tax bills.
source:
  format: csv
  group_by: BillNumber
target:
  table: TaxBill
  primary_key: BillNumber
  on_conflict: skip
fields:
  BillNumber: { column: BillNumber, type: str, required: true, cleaners: [strip] }
  OwnerName: { column: Name, type: str, transient: true, cleaners: [strip, upper] }
  CityStateZip: { column: CityStateZip, type: str, transient: true, cleaners: [strip] }
  Description: { column: Description, type: str, transient: true, cleaners: [strip, upper] }
  Tax: { column: Tax, type: decimal, transient: true, cleaners: [strip, parse_decimal] }
  OwnerFirst: { type: str }
  OwnerLast: { type: str }
  City: { type: str, transient: true }
  State: { type: str, transient: true }
  Zip: { type: str, transient: true }
  AssessmentType: { type: str, transient: true }
  AssessmentTypeId: { type: int, transient: true }
transforms:
  - split_name:
      source: OwnerName
      into: { first: OwnerFirst, last: OwnerLast }
      firm_list: [LLC, INC, TRUST, ESTATE, COMPANY]
  - split_city_state_zip:
      source: CityStateZip
      into: { city: City, state: State, zip: Zip }
  - classify:
      source: Description
      target: AssessmentType
      rules:
        - { contains: [FIRE, ACRE], value: FIRE_ACRE }
        - { contains: [TANGIBLE, FULL], value: TANGIBLE }
        - { contains: REAL, value: REAL_ESTATE }
        - { contains: SCHOOL, value: SCHOOL }
        - { contains: FLOOD, value: FLOOD }
      default: OTHER
  - lookup:
      source: AssessmentType
      target: AssessmentTypeId
      table: { REAL_ESTATE: 1, SCHOOL: 4, TANGIBLE: 6, FIRE_ACRE: 26, FLOOD: 7, OTHER: 0 }
children:
  - table: TaxParty
    foreign_key: BillNumber
    fields:
      FirstName: { from: OwnerFirst }
      LastName: { from: OwnerLast }
      City: { from: City }
      State: { from: State }
      Zip: { from: Zip }
      PartyType: { value: owner }
  - table: TaxAssessment
    foreign_key: BillNumber
    for_each_row: true
    fields:
      TypeId: { from: AssessmentTypeId }
      Description: { from: Description }
      Amount: { from: Tax }
"""

_CSV = (
    "BillNumber,Name,CityStateZip,Description,Tax\n"
    '2024-00001,JOHN SMITH,"GLASGOW, KY 42141",REAL ESTATE,450.25\n'
    '2024-00001,JOHN SMITH,"GLASGOW, KY 42141",FIRE DISTRICT ACREAGE,30.00\n'
    '2024-00001,JOHN SMITH,"GLASGOW, KY 42141",SCHOOL,250.50\n'
    '2024-00002,ACME FARMS LLC,"CAVE CITY, KY 42127",REAL ESTATE,1200.00\n'
    '2024-00002,ACME FARMS LLC,"CAVE CITY, KY 42127",TANGIBLE FULL,80.00\n'
)


def _run(tmp_path: Path) -> Path:
    (tmp_path / "m.yml").write_text(_MAPPING, encoding="utf-8")
    (tmp_path / "bills.csv").write_text(_CSV, encoding="utf-8")
    db = tmp_path / "out.db"
    result = Pipeline(
        source_uri=f"csv:///{(tmp_path / 'bills.csv').as_posix()}",
        sink_uri=f"sqlite:///{db.as_posix()}",
        mapping=Mapping.from_yaml(tmp_path / "m.yml"),
        error_log=tmp_path / "errors.jsonl",
    ).run()
    assert result.rows_in == 5
    assert result.rows_ok == 5
    assert result.rows_failed == 0
    return db


def test_taxbill_parent_rows(tmp_path: Path) -> None:
    db = _run(tmp_path)
    engine = create_engine(f"sqlite:///{db}")
    cols = {c["name"] for c in inspect(engine).get_columns("TaxBill")}
    assert cols == {"BillNumber", "OwnerFirst", "OwnerLast"}

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT BillNumber, OwnerFirst, OwnerLast FROM TaxBill ORDER BY BillNumber")
        ).all()
    assert [(r.BillNumber, r.OwnerFirst, r.OwnerLast) for r in rows] == [
        ("2024-00001", "JOHN", "SMITH"),
        ("2024-00002", ".", "ACME FARMS LLC"),
    ]


def test_taxparty_child_one_per_bill(tmp_path: Path) -> None:
    db = _run(tmp_path)
    engine = create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM TaxParty")).scalar() == 2
        party = conn.execute(
            text(
                "SELECT FirstName, LastName, City, State, Zip, PartyType "
                "FROM TaxParty WHERE BillNumber='2024-00001'"
            )
        ).first()
    assert party is not None
    assert party.FirstName == "JOHN"
    assert party.LastName == "SMITH"
    assert party.City == "GLASGOW"
    assert party.State == "KY"
    assert party.Zip == "42141"
    assert party.PartyType == "owner"


def test_taxassessment_children_classified_and_fanned_out(tmp_path: Path) -> None:
    db = _run(tmp_path)
    engine = create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM TaxAssessment")).scalar() == 5
        b1 = (
            conn.execute(text("SELECT TypeId FROM TaxAssessment WHERE BillNumber='2024-00001'"))
            .scalars()
            .all()
        )
        b2 = (
            conn.execute(text("SELECT TypeId FROM TaxAssessment WHERE BillNumber='2024-00002'"))
            .scalars()
            .all()
        )
    assert sorted(b1) == [1, 4, 26]
    assert sorted(b2) == [1, 6]
