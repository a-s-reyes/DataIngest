from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine, text

from dataingest.config import Mapping
from dataingest.pipeline import Pipeline


def _csv_uri(p: Path) -> str:
    return f"csv:///{p.as_posix()}"


def _sqlite_uri(p: Path) -> str:
    return f"sqlite:///{p.as_posix()}"


def test_full_pipeline_writes_clay_rows(
    clay_csv: Path, clay_mapping: Path, tmp_path: Path
):
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(clay_mapping)
    pipeline = Pipeline(
        source_uri=_csv_uri(clay_csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=mapping,
        error_log=tmp_path / "errors.jsonl",
    )
    result = pipeline.run()

    assert result.rows_in == 20
    assert result.rows_ok == 20
    assert result.rows_failed == 0
    assert db_path.exists()

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM tax_bills")).scalar()
        assert count == 20

        first = conn.execute(
            text(
                "SELECT bill_number, owner_name, face_amount, date_due "
                "FROM tax_bills ORDER BY bill_number LIMIT 1"
            )
        ).first()
        assert first is not None
        assert first.bill_number == "00001"
        assert first.owner_name == "SMITH JOHN"
        assert Decimal(str(first.face_amount)) == Decimal("650.42")
        assert str(first.date_due) == "2024-08-29"


def test_dry_run_does_not_create_db(
    clay_csv: Path, clay_mapping: Path, tmp_path: Path
):
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(clay_mapping)
    pipeline = Pipeline(
        source_uri=_csv_uri(clay_csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=mapping,
        dry_run=True,
        error_log=tmp_path / "errors.jsonl",
    )
    result = pipeline.run()

    assert result.rows_in == 20
    assert result.rows_ok == 20
    assert not db_path.exists()


def test_limit_caps_rows(clay_csv: Path, clay_mapping: Path, tmp_path: Path):
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(clay_mapping)
    pipeline = Pipeline(
        source_uri=_csv_uri(clay_csv),
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
        count = conn.execute(text("SELECT COUNT(*) FROM tax_bills")).scalar()
        assert count == 5


def test_invalid_row_logged_to_errors(clay_mapping: Path, tmp_path: Path):
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(
        "Bill Number,Account Number,Map Number,Owner Name,Property Address,City,State,Zip,Assessed Value,Face Amount,Date Due\n"
        "00001,A1,M1,SMITH,123 MAIN,CLAY,KY,40808,$50000,not-a-number,8/29/2024\n"
        "00002,A2,M2,JONES,456 OAK,CLAY,KY,40808,$60000,$700.00,8/29/2024\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "out.db"
    err_path = tmp_path / "errors.jsonl"
    mapping = Mapping.from_yaml(clay_mapping)
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


def test_validation_error_logged(clay_mapping: Path, tmp_path: Path):
    """Trigger a Pydantic validation error (not a cleaner error) by feeding a
    non-decimal-parseable value through cleaners that pass it through.
    """
    # Custom mapping where face_amount has no cleaners — raw string goes straight
    # to Pydantic and fails decimal coercion.
    bad_mapping = tmp_path / "no_cleaners.yml"
    bad_mapping.write_text(
        """
spec_version: 1
vendor: bad
source: { format: csv }
target: { table: t, primary_key: bill_number }
fields:
  bill_number: { column: 0, type: str, required: true }
  face_amount: { column: 1, type: decimal, required: true }
""",
        encoding="utf-8",
    )
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(
        "Bill Number,Face Amount\n00001,abc\n",
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
    assert "face_amount" in err_path.read_text(encoding="utf-8")


def test_rerun_with_skip_is_idempotent(
    clay_csv: Path, clay_mapping: Path, tmp_path: Path
):
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(clay_mapping)
    for _ in range(2):
        pipeline = Pipeline(
            source_uri=_csv_uri(clay_csv),
            sink_uri=_sqlite_uri(db_path),
            mapping=mapping,
            error_log=tmp_path / "errors.jsonl",
        )
        pipeline.run()

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM tax_bills")).scalar()
        assert count == 20
