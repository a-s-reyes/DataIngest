from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine, text

from dataingest.config import Mapping
from dataingest.pipeline import Pipeline

from .conftest import MappingFixture


def _csv_uri(p: Path) -> str:
    return f"csv:///{p.as_posix()}"


def _sqlite_uri(p: Path) -> str:
    return f"sqlite:///{p.as_posix()}"


def test_full_pipeline_writes_rows(any_mapping: MappingFixture, tmp_path: Path) -> None:
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(any_mapping.mapping_yml)
    pipeline = Pipeline(
        source_uri=_csv_uri(any_mapping.csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=mapping,
        error_log=tmp_path / "errors.jsonl",
    )
    result = pipeline.run()

    assert result.rows_in == any_mapping.row_count
    assert result.rows_ok == any_mapping.row_count
    assert result.rows_failed == 0
    assert db_path.exists()

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {any_mapping.table}")).scalar()
        assert count == any_mapping.row_count


def test_dry_run_does_not_create_db(any_mapping: MappingFixture, tmp_path: Path) -> None:
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(any_mapping.mapping_yml)
    pipeline = Pipeline(
        source_uri=_csv_uri(any_mapping.csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=mapping,
        dry_run=True,
        error_log=tmp_path / "errors.jsonl",
    )
    result = pipeline.run()

    assert result.rows_in == any_mapping.row_count
    assert result.rows_ok == any_mapping.row_count
    assert not db_path.exists()


def test_limit_caps_rows(any_mapping: MappingFixture, tmp_path: Path) -> None:
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(any_mapping.mapping_yml)
    pipeline = Pipeline(
        source_uri=_csv_uri(any_mapping.csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=mapping,
        limit=5,
        error_log=tmp_path / "errors.jsonl",
    )
    result = pipeline.run()

    assert result.rows_in == 5
    assert result.rows_ok == 5

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {any_mapping.table}")).scalar()
        assert count == 5


def test_rerun_with_skip_is_idempotent(any_mapping: MappingFixture, tmp_path: Path) -> None:
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(any_mapping.mapping_yml)
    for _ in range(2):
        pipeline = Pipeline(
            source_uri=_csv_uri(any_mapping.csv),
            sink_uri=_sqlite_uri(db_path),
            mapping=mapping,
            error_log=tmp_path / "errors.jsonl",
        )
        pipeline.run()

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {any_mapping.table}")).scalar()
        assert count == any_mapping.row_count


def test_telemetry_decimal_value_round_trips(telemetry: MappingFixture, tmp_path: Path) -> None:
    """Specific assertion: telemetry value column survives the Decimal round-trip."""
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(telemetry.mapping_yml)
    Pipeline(
        source_uri=_csv_uri(telemetry.csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=mapping,
        error_log=tmp_path / "errors.jsonl",
    ).run()

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT record_id, channel, value, unit "
                "FROM telemetry_records ORDER BY record_id LIMIT 1"
            )
        ).first()
        assert row is not None
        assert row.record_id == "TM-00001"
        assert row.channel == "ACC_X_FUSELAGE"
        assert Decimal(str(row.value)) == Decimal("0.4823")
        assert row.unit == "g"


def test_qualification_us_date_parses(qualification: MappingFixture, tmp_path: Path) -> None:
    """Specific assertion: qualification MM/DD/YYYY dates parse correctly."""
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(qualification.mapping_yml)
    Pipeline(
        source_uri=_csv_uri(qualification.csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=mapping,
        error_log=tmp_path / "errors.jsonl",
    ).run()

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT test_id, run_date, result FROM qualification_results ORDER BY test_id LIMIT 1"
            )
        ).first()
        assert row is not None
        assert row.test_id == "QT-2026-0001"
        assert str(row.run_date) == "2026-03-12"
        assert row.result == "PASS"


def test_parts_inventory_iso_date_parses(parts_inventory: MappingFixture, tmp_path: Path) -> None:
    """Specific assertion: parts inventory ISO dates parse correctly."""
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(parts_inventory.mapping_yml)
    Pipeline(
        source_uri=_csv_uri(parts_inventory.csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=mapping,
        error_log=tmp_path / "errors.jsonl",
    ).run()

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT nsn, qty_on_hand, last_audit FROM parts_inventory ORDER BY nsn LIMIT 1")
        ).first()
        assert row is not None
        assert row.nsn == "1560-01-789-0123"
        assert row.qty_on_hand == 18
        assert str(row.last_audit) == "2026-03-18"


def test_invalid_row_logged_to_errors(qualification: MappingFixture, tmp_path: Path) -> None:
    """A bad decimal in a required field routes the row to errors.jsonl."""
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(
        "test_id,part_number,run_date,parameter,measured_value,tolerance,result,technician\n"
        "QT-9001,P-1,03/12/2026,TEMP,not-a-number,100.0,PASS,J. RIVERA\n"
        "QT-9002,P-2,03/12/2026,TEMP,42.0,100.0,PASS,J. RIVERA\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "out.db"
    err_path = tmp_path / "errors.jsonl"
    mapping = Mapping.from_yaml(qualification.mapping_yml)
    pipeline = Pipeline(
        source_uri=_csv_uri(bad_csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=mapping,
        error_log=err_path,
    )
    result = pipeline.run()

    assert result.rows_in == 2
    assert result.rows_ok == 1
    assert result.rows_failed == 1
    assert err_path.exists()
    assert "not-a-number" in err_path.read_text(encoding="utf-8")


def test_validation_error_logged(tmp_path: Path) -> None:
    """Pydantic validation error (decimal coercion failure with no cleaners)."""
    bad_mapping = tmp_path / "no_cleaners.yml"
    bad_mapping.write_text(
        """
spec_version: 1
name: bad
source: { format: csv }
target: { table: t, primary_key: id }
fields:
  id: { column: 0, type: str, required: true }
  amount: { column: 1, type: decimal, required: true }
""",
        encoding="utf-8",
    )
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(
        "id,amount\nROW1,abc\n",
        encoding="utf-8",
    )
    err_path = tmp_path / "errors.jsonl"
    pipeline = Pipeline(
        source_uri=_csv_uri(bad_csv),
        sink_uri=_sqlite_uri(tmp_path / "out.db"),
        mapping=Mapping.from_yaml(bad_mapping),
        error_log=err_path,
    )
    result = pipeline.run()

    assert result.rows_in == 1
    assert result.rows_ok == 0
    assert result.rows_failed == 1
    assert err_path.exists()
    assert "amount" in err_path.read_text(encoding="utf-8")
