import math
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from dataingest.config import Mapping
from dataingest.pipeline import DEFAULT_CHUNK_SIZE, Pipeline

from .conftest import MappingFixture


def _csv_uri(p: Path) -> str:
    return f"csv:///{p.as_posix()}"


def _sqlite_uri(p: Path) -> str:
    return f"sqlite:///{p.as_posix()}"


def _make_synthetic_telemetry_csv(path: Path, n_rows: int) -> None:
    """Write a CSV matching mappings/telemetry.yml with ``n_rows`` data rows."""
    header = "record_id,flight_id,recorded_at,channel,value,unit,quality\n"
    lines = [header]
    for i in range(1, n_rows + 1):
        lines.append(
            f"TM-{i:08d},FT-2026-X,2026-04-12T14:22:01.250Z,acc_x_fuselage,{i * 0.001:.4f},g,OK\n"
        )
    path.write_text("".join(lines), encoding="utf-8")


def test_default_chunk_size_is_1000(telemetry: MappingFixture, tmp_path: Path) -> None:
    pipeline = Pipeline(
        source_uri=_csv_uri(telemetry.csv),
        sink_uri=_sqlite_uri(tmp_path / "out.db"),
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        error_log=tmp_path / "errors.jsonl",
    )
    assert pipeline.chunk_size == DEFAULT_CHUNK_SIZE
    assert DEFAULT_CHUNK_SIZE == 1000


@pytest.mark.parametrize("bad", [0, -1, -100])
def test_chunk_size_must_be_positive(telemetry: MappingFixture, tmp_path: Path, bad: int) -> None:
    with pytest.raises(ValueError, match="chunk_size must be >= 1"):
        Pipeline(
            source_uri=_csv_uri(telemetry.csv),
            sink_uri=_sqlite_uri(tmp_path / "out.db"),
            mapping=Mapping.from_yaml(telemetry.mapping_yml),
            chunk_size=bad,
        )


@pytest.mark.parametrize("chunk_size", [1, 3, 7, 19, 20, 21, 100, 1000])
def test_chunks_written_matches_ceil_rows_over_chunk_size(
    telemetry: MappingFixture, tmp_path: Path, chunk_size: int
) -> None:
    """chunks_written should be ceil(rows_ok / chunk_size) regardless of chunk_size."""
    pipeline = Pipeline(
        source_uri=_csv_uri(telemetry.csv),
        sink_uri=_sqlite_uri(tmp_path / "out.db"),
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        error_log=tmp_path / "errors.jsonl",
        chunk_size=chunk_size,
    )
    result = pipeline.run()

    assert result.rows_ok == 20
    assert result.chunks_written == math.ceil(20 / chunk_size)


@pytest.mark.parametrize("chunk_size", [1, 7, 100, 1000])
def test_all_rows_land_regardless_of_chunk_size(
    telemetry: MappingFixture, tmp_path: Path, chunk_size: int
) -> None:
    """The DB content must be identical no matter how rows were batched."""
    db_path = tmp_path / "out.db"
    Pipeline(
        source_uri=_csv_uri(telemetry.csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        error_log=tmp_path / "errors.jsonl",
        chunk_size=chunk_size,
    ).run()

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM telemetry_records")).scalar()
        assert count == 20

        ids = (
            conn.execute(text("SELECT record_id FROM telemetry_records ORDER BY record_id"))
            .scalars()
            .all()
        )
        assert ids[0] == "TM-00001"
        assert ids[-1] == "TM-00020"


def test_dry_run_writes_no_chunks(telemetry: MappingFixture, tmp_path: Path) -> None:
    pipeline = Pipeline(
        source_uri=_csv_uri(telemetry.csv),
        sink_uri=_sqlite_uri(tmp_path / "out.db"),
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        dry_run=True,
        error_log=tmp_path / "errors.jsonl",
        chunk_size=5,
    )
    result = pipeline.run()

    assert result.rows_ok == 20
    assert result.chunks_written == 0
    assert not (tmp_path / "out.db").exists()


def test_zero_valid_rows_writes_zero_chunks(qualification: MappingFixture, tmp_path: Path) -> None:
    """A CSV where every row fails should produce 0 chunks_written."""
    bad_csv = tmp_path / "all_bad.csv"
    bad_csv.write_text(
        "test_id,part_number,run_date,parameter,measured_value,tolerance,result,technician\n"
        "QT-1,P-1,03/12/2026,TEMP,not-a-number,100,PASS,T\n"
        "QT-2,P-2,03/12/2026,TEMP,nope,100,PASS,T\n",
        encoding="utf-8",
    )
    pipeline = Pipeline(
        source_uri=_csv_uri(bad_csv),
        sink_uri=_sqlite_uri(tmp_path / "out.db"),
        mapping=Mapping.from_yaml(qualification.mapping_yml),
        error_log=tmp_path / "errors.jsonl",
        chunk_size=10,
    )
    result = pipeline.run()

    assert result.rows_in == 2
    assert result.rows_ok == 0
    assert result.rows_failed == 2
    assert result.chunks_written == 0


def test_limit_caps_chunks(telemetry: MappingFixture, tmp_path: Path) -> None:
    """--limit should cap rows BEFORE chunking, so chunks_written reflects the cap."""
    pipeline = Pipeline(
        source_uri=_csv_uri(telemetry.csv),
        sink_uri=_sqlite_uri(tmp_path / "out.db"),
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        limit=7,
        error_log=tmp_path / "errors.jsonl",
        chunk_size=3,
    )
    result = pipeline.run()

    assert result.rows_in == 7
    assert result.rows_ok == 7
    # 7 rows / 3 per chunk = 3 chunks (3, 3, 1)
    assert result.chunks_written == 3


def test_synthetic_large_run_streams_in_chunks(telemetry: MappingFixture, tmp_path: Path) -> None:
    """Generate a CSV bigger than chunk_size to verify multi-chunk behavior end-to-end."""
    big_csv = tmp_path / "big.csv"
    _make_synthetic_telemetry_csv(big_csv, n_rows=5000)

    db_path = tmp_path / "out.db"
    pipeline = Pipeline(
        source_uri=_csv_uri(big_csv),
        sink_uri=_sqlite_uri(db_path),
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        error_log=tmp_path / "errors.jsonl",
        chunk_size=1000,
    )
    result = pipeline.run()

    assert result.rows_in == 5000
    assert result.rows_ok == 5000
    assert result.rows_failed == 0
    assert result.chunks_written == 5

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM telemetry_records")).scalar()
        assert count == 5000
