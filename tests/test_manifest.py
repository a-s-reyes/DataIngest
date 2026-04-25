"""Integration tests for the run manifest table (`_dataingest_runs`)."""

from pathlib import Path

from sqlalchemy import create_engine, text

from dataingest import __version__
from dataingest.config import Mapping
from dataingest.manifest import MANIFEST_TABLE_NAME, derive_status
from dataingest.pipeline import Pipeline

from .conftest import MappingFixture


def _csv_uri(p: Path) -> str:
    return f"csv:///{p.as_posix()}"


def _sqlite_uri(p: Path) -> str:
    return f"sqlite:///{p.as_posix()}"


def _manifest_rows(db_path: Path) -> list[dict[str, object]]:
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {MANIFEST_TABLE_NAME} ORDER BY started_at"))
        return [dict(row._mapping) for row in result]


def test_first_run_creates_manifest_table(telemetry: MappingFixture, tmp_path: Path) -> None:
    db_path = tmp_path / "out.db"
    Pipeline(
        source_uri=_csv_uri(telemetry.csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        error_log=tmp_path / "errors.jsonl",
    ).run()

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        tables = (
            conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"))
            .scalars()
            .all()
        )
        assert MANIFEST_TABLE_NAME in tables
        assert "telemetry_records" in tables


def test_manifest_row_records_run_metadata(telemetry: MappingFixture, tmp_path: Path) -> None:
    db_path = tmp_path / "out.db"
    pipeline = Pipeline(
        source_uri=_csv_uri(telemetry.csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        error_log=tmp_path / "errors.jsonl",
    )
    result = pipeline.run()

    rows = _manifest_rows(db_path)
    assert len(rows) == 1
    row = rows[0]

    assert row["run_id"] == result.run_id
    assert row["mapping_name"] == "flight-test-telemetry"
    assert row["target_table"] == "telemetry_records"
    assert row["rows_in"] == 20
    assert row["rows_ok"] == 20
    assert row["rows_failed"] == 0
    assert row["chunks_written"] == 1
    assert row["dataingest_version"] == __version__
    assert row["dry_run"] == 0  # Boolean → 0 in SQLite
    assert row["status"] == "success"
    assert row["started_at"] is not None
    assert row["finished_at"] is not None


def test_each_run_appends_a_new_manifest_row(telemetry: MappingFixture, tmp_path: Path) -> None:
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(telemetry.mapping_yml)
    run_ids = []
    for _ in range(3):
        result = Pipeline(
            source_uri=_csv_uri(telemetry.csv),
            sink_uri=_sqlite_uri(db_path),
            mapping=mapping,
            error_log=tmp_path / "errors.jsonl",
        ).run()
        run_ids.append(result.run_id)

    rows = _manifest_rows(db_path)
    assert len(rows) == 3
    assert {row["run_id"] for row in rows} == set(run_ids)
    # Every run gets a unique id
    assert len(set(run_ids)) == 3


def test_dry_run_does_not_write_manifest(telemetry: MappingFixture, tmp_path: Path) -> None:
    db_path = tmp_path / "out.db"
    Pipeline(
        source_uri=_csv_uri(telemetry.csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        dry_run=True,
        error_log=tmp_path / "errors.jsonl",
    ).run()

    # Dry-run never opens the sink, so the DB file should not exist at all.
    assert not db_path.exists()


def test_partial_failure_writes_partial_status(
    qualification: MappingFixture, tmp_path: Path
) -> None:
    """Some rows succeed, some fail → status='partial'."""
    mixed_csv = tmp_path / "mixed.csv"
    mixed_csv.write_text(
        "test_id,part_number,run_date,parameter,measured_value,tolerance,result,technician\n"
        "QT-1,P-1,03/12/2026,TEMP,1.0,100,PASS,T\n"
        "QT-2,P-2,03/12/2026,TEMP,not-a-number,100,PASS,T\n"
        "QT-3,P-3,03/12/2026,TEMP,2.0,100,PASS,T\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "out.db"
    Pipeline(
        source_uri=_csv_uri(mixed_csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=Mapping.from_yaml(qualification.mapping_yml),
        error_log=tmp_path / "errors.jsonl",
    ).run()

    rows = _manifest_rows(db_path)
    assert len(rows) == 1
    assert rows[0]["status"] == "partial"
    assert rows[0]["rows_in"] == 3
    assert rows[0]["rows_ok"] == 2
    assert rows[0]["rows_failed"] == 1


def test_all_rows_fail_writes_failed_status(qualification: MappingFixture, tmp_path: Path) -> None:
    """rows_in > 0, rows_ok == 0 → status='failed'."""
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(
        "test_id,part_number,run_date,parameter,measured_value,tolerance,result,technician\n"
        "QT-1,P-1,03/12/2026,TEMP,not-a-number,100,PASS,T\n"
        "QT-2,P-2,03/12/2026,TEMP,nope,100,PASS,T\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "out.db"
    Pipeline(
        source_uri=_csv_uri(bad_csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=Mapping.from_yaml(qualification.mapping_yml),
        error_log=tmp_path / "errors.jsonl",
    ).run()

    rows = _manifest_rows(db_path)
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert rows[0]["rows_ok"] == 0


def test_manifest_records_source_uri_and_error_log_path(
    telemetry: MappingFixture, tmp_path: Path
) -> None:
    db_path = tmp_path / "out.db"
    err_path = tmp_path / "custom_errors.jsonl"
    source_uri = _csv_uri(telemetry.csv)
    Pipeline(
        source_uri=source_uri,
        sink_uri=_sqlite_uri(db_path),
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        error_log=err_path,
    ).run()

    rows = _manifest_rows(db_path)
    assert rows[0]["source_uri"] == source_uri
    assert rows[0]["error_log_path"] == str(err_path)


def test_derive_status_truth_table() -> None:
    # rows_in == 0 → vacuous success
    assert derive_status(0, 0, errored=False) == "success"
    # all rows ok
    assert derive_status(10, 10, errored=False) == "success"
    # mixed
    assert derive_status(10, 7, errored=False) == "partial"
    # all rows failed
    assert derive_status(10, 0, errored=False) == "failed"
    # exception always wins
    assert derive_status(10, 10, errored=True) == "failed"
    assert derive_status(0, 0, errored=True) == "failed"


def test_idempotent_rerun_with_skip_logs_each_attempt(
    telemetry: MappingFixture, tmp_path: Path
) -> None:
    """`on_conflict: skip` data writes are idempotent, but the manifest still
    records every invocation as a separate run."""
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(telemetry.mapping_yml)
    for _ in range(2):
        Pipeline(
            source_uri=_csv_uri(telemetry.csv),
            sink_uri=_sqlite_uri(db_path),
            mapping=mapping,
            error_log=tmp_path / "errors.jsonl",
        ).run()

    rows = _manifest_rows(db_path)
    assert len(rows) == 2
    # First run inserts 20, second run finds 20 dupes and skips them.
    # rows_ok counts pre-skip (the rows that survived validation), so both
    # manifest rows should report 20.
    assert all(row["rows_ok"] == 20 for row in rows)
    assert all(row["status"] == "success" for row in rows)

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM telemetry_records")).scalar()
        assert count == 20  # idempotent at the data level
