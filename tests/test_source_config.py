from pathlib import Path

from sqlalchemy import create_engine, text

from dataingest.config import Mapping
from dataingest.pipeline import Pipeline


def _write_mapping(tmp_path: Path, delimiter: str) -> Path:
    p = tmp_path / "m.yml"
    p.write_text(
        f"""
spec_version: 1
name: srccfg
source:
  format: csv
  delimiter: "{delimiter}"
target:
  table: t
  primary_key: id
fields:
  id:
    column: id
    type: str
    required: true
    cleaners: [strip]
  name:
    column: name
    type: str
    cleaners: [strip]
""",
        encoding="utf-8",
    )
    return p


def _run(tmp_path: Path, mapping_path: Path, source_uri: str) -> Path:
    db = tmp_path / "out.db"
    Pipeline(
        source_uri=source_uri,
        sink_uri=f"sqlite:///{db.as_posix()}",
        mapping=Mapping.from_yaml(mapping_path),
        error_log=tmp_path / "errors.jsonl",
    ).run()
    return db


def test_mapping_delimiter_is_honored(tmp_path: Path) -> None:
    csv = tmp_path / "data.csv"
    csv.write_text("id;name\nA;alpha\nB;beta\n", encoding="utf-8")
    mapping = _write_mapping(tmp_path, ";")
    db = _run(tmp_path, mapping, f"csv:///{csv.as_posix()}")
    engine = create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, name FROM t ORDER BY id")).all()
    assert [(r.id, r.name) for r in rows] == [("A", "alpha"), ("B", "beta")]


def test_uri_delimiter_overrides_mapping(tmp_path: Path) -> None:
    csv = tmp_path / "data.csv"
    csv.write_text("id,name\nA,alpha\n", encoding="utf-8")
    mapping = _write_mapping(tmp_path, ";")
    db = _run(tmp_path, mapping, f"csv:///{csv.as_posix()}?delimiter=,")
    engine = create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, name FROM t")).all()
    assert [(r.id, r.name) for r in rows] == [("A", "alpha")]
