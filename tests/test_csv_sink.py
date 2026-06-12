from pathlib import Path

from sqlalchemy import create_engine, text

from dataingest.config import Mapping
from dataingest.pipeline import Pipeline

from .conftest import MappingFixture


def _csv_uri(p: Path) -> str:
    return f"csv:///{p.as_posix()}"


def test_csv_sink_writes_output_file(telemetry: MappingFixture, tmp_path: Path) -> None:
    out = tmp_path / "out.csv"
    result = Pipeline(
        source_uri=_csv_uri(telemetry.csv),
        sink_uri=f"csv:///{out.as_posix()}",
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        error_log=tmp_path / "errors.jsonl",
    ).run()

    assert result.rows_ok == 20
    lines = out.read_text(encoding="utf-8").splitlines()
    assert lines[0].split(",")[0] == "record_id"
    assert len(lines) == 21
    assert "TM-00001" in lines[1]


def test_csv_sink_dry_run_writes_no_file(telemetry: MappingFixture, tmp_path: Path) -> None:
    out = tmp_path / "out.csv"
    Pipeline(
        source_uri=_csv_uri(telemetry.csv),
        sink_uri=f"csv:///{out.as_posix()}",
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        dry_run=True,
        error_log=tmp_path / "errors.jsonl",
    ).run()

    assert not out.exists()


def test_multi_sink_writes_to_both(telemetry: MappingFixture, tmp_path: Path) -> None:
    db = tmp_path / "out.db"
    out = tmp_path / "out.csv"
    Pipeline(
        source_uri=_csv_uri(telemetry.csv),
        sink_uri=[f"sqlite:///{db.as_posix()}", f"csv:///{out.as_posix()}"],
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        error_log=tmp_path / "errors.jsonl",
    ).run()

    assert len(out.read_text(encoding="utf-8").splitlines()) == 21
    engine = create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM telemetry_records")).scalar()
    assert count == 20
