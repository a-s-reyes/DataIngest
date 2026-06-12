from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text

from dataingest.config import Mapping
from dataingest.errors import MappingError
from dataingest.pipeline import Pipeline
from dataingest.transforms import Transform, build


def _split_name(firm_list: list[str] | None = None) -> Transform:
    config: dict[str, object] = {"source": "owner", "into": {"first": "F", "last": "L"}}
    if firm_list is not None:
        config["firm_list"] = firm_list
    return build([{"split_name": config}])[0]


def test_split_name_individual() -> None:
    rec = _split_name().apply({"owner": "John Smith"})
    assert rec["F"] == "John"
    assert rec["L"] == "Smith"


def test_split_name_multi_token_last() -> None:
    rec = _split_name().apply({"owner": "Mary Anne Van Der Berg"})
    assert rec["F"] == "Mary"
    assert rec["L"] == "Anne Van Der Berg"


def test_split_name_firm_is_not_split() -> None:
    rec = _split_name(firm_list=["LLC", "INC"]).apply({"owner": "Acme Holdings LLC"})
    assert rec["F"] == "."
    assert rec["L"] == "Acme Holdings LLC"


def test_split_name_empty() -> None:
    rec = _split_name().apply({"owner": None})
    assert rec["F"] == ""
    assert rec["L"] == ""


def _split_csz() -> Transform:
    return build(
        [
            {
                "split_city_state_zip": {
                    "source": "csz",
                    "into": {"city": "C", "state": "S", "zip": "Z"},
                }
            }
        ]
    )[0]


def test_split_city_state_zip_with_comma() -> None:
    rec = _split_csz().apply({"csz": "GLASGOW, KY 42141"})
    assert (rec["C"], rec["S"], rec["Z"]) == ("GLASGOW", "KY", "42141")


def test_split_city_state_zip_without_comma() -> None:
    rec = _split_csz().apply({"csz": "BOWLING GREEN KY 42101"})
    assert (rec["C"], rec["S"], rec["Z"]) == ("BOWLING GREEN", "KY", "42101")


def test_split_city_state_zip_invalid_raises() -> None:
    with pytest.raises(ValueError, match="cannot parse city/state/zip"):
        _split_csz().apply({"csz": "not an address"})


def test_unknown_transform_rejected(tmp_path: Path) -> None:
    p = tmp_path / "m.yml"
    p.write_text(
        """
spec_version: 1
name: x
source: { format: csv }
transforms:
  - nope: {}
target: { table: t, primary_key: id }
fields:
  id: { column: 0, type: str, required: true }
""",
        encoding="utf-8",
    )
    with pytest.raises(MappingError, match="unknown transform"):
        Mapping.from_yaml(p)


def _write_transform_mapping(tmp_path: Path) -> Path:
    p = tmp_path / "people.yml"
    p.write_text(
        """
spec_version: 1
name: people
source:
  format: csv
transforms:
  - split_city_state_zip:
      source: csz
      into: { city: City, state: State, zip: Zip }
target:
  table: people
  primary_key: id
fields:
  id:
    column: 0
    type: str
    required: true
    cleaners: [strip]
  csz:
    column: 1
    type: str
    transient: true
    cleaners: [strip]
  City:
    type: str
  State:
    type: str
  Zip:
    type: str
""",
        encoding="utf-8",
    )
    return p


def test_pipeline_transform_produces_derived_columns(tmp_path: Path) -> None:
    csv = tmp_path / "data.csv"
    csv.write_text(
        'id,csz\nA,"GLASGOW, KY 42141"\nB,"BOWLING GREEN, KY 42101"\n',
        encoding="utf-8",
    )
    db = tmp_path / "out.db"
    result = Pipeline(
        source_uri=f"csv:///{csv.as_posix()}",
        sink_uri=f"sqlite:///{db.as_posix()}",
        mapping=Mapping.from_yaml(_write_transform_mapping(tmp_path)),
        error_log=tmp_path / "errors.jsonl",
    ).run()

    assert result.rows_ok == 2

    engine = create_engine(f"sqlite:///{db}")
    cols = {c["name"] for c in inspect(engine).get_columns("people")}
    assert cols == {"id", "City", "State", "Zip"}

    with engine.connect() as conn:
        row = conn.execute(text("SELECT id, City, State, Zip FROM people ORDER BY id")).first()
    assert row is not None
    assert (row.City, row.State, row.Zip) == ("GLASGOW", "KY", "42141")
