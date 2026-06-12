from pathlib import Path

from sqlalchemy import create_engine, inspect, text

from dataingest.config import Mapping
from dataingest.pipeline import Pipeline


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
