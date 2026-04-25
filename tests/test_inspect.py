"""Tests for ``inspect_sink`` and the ``dataingest tables`` CLI command."""

from pathlib import Path

import pytest

from dataingest.config import Mapping
from dataingest.inspect import (
    SinkInspection,
    inspect_sink,
    render_inspection,
)
from dataingest.pipeline import Pipeline

from .conftest import MappingFixture


def _seed_sqlite(telemetry: MappingFixture, tmp_path: Path, runs: int = 1) -> Path:
    """Run the pipeline ``runs`` times to populate a sqlite sink + manifest."""
    db_path = tmp_path / "out.db"
    mapping = Mapping.from_yaml(telemetry.mapping_yml)
    for _ in range(runs):
        Pipeline(
            source_uri=f"csv:///{telemetry.csv.as_posix()}",
            sink_uri=f"sqlite:///{db_path.as_posix()}",
            mapping=mapping,
            error_log=tmp_path / "errors.jsonl",
        ).run()
    return db_path


def test_inspect_lists_data_and_manifest_tables(telemetry: MappingFixture, tmp_path: Path) -> None:
    db_path = _seed_sqlite(telemetry, tmp_path)
    info = inspect_sink(f"sqlite:///{db_path.as_posix()}")

    table_names = {t.name for t in info.tables}
    assert "telemetry_records" in table_names
    assert "_dataingest_runs" in table_names

    rows_by_table = {t.name: t.row_count for t in info.tables}
    assert rows_by_table["telemetry_records"] == 20
    assert rows_by_table["_dataingest_runs"] == 1


def test_inspect_returns_recent_run_entries(telemetry: MappingFixture, tmp_path: Path) -> None:
    db_path = _seed_sqlite(telemetry, tmp_path, runs=3)
    info = inspect_sink(f"sqlite:///{db_path.as_posix()}", recent_runs=2)

    assert len(info.recent_runs) == 2
    for run in info.recent_runs:
        assert run.mapping_name == "flight-test-telemetry"
        assert run.status == "success"
        assert run.rows_in == 20
        assert run.rows_ok == 20


def test_inspect_runs_param_zero_returns_no_run_history(
    telemetry: MappingFixture, tmp_path: Path
) -> None:
    db_path = _seed_sqlite(telemetry, tmp_path)
    info = inspect_sink(f"sqlite:///{db_path.as_posix()}", recent_runs=0)
    assert info.recent_runs == ()


def test_inspect_handles_empty_database(tmp_path: Path) -> None:
    """Inspecting a fresh, empty sink returns no tables and no runs without raising."""
    db_path = tmp_path / "empty.db"
    db_path.touch()
    info = inspect_sink(f"sqlite:///{db_path.as_posix()}")
    assert info.tables == ()
    assert info.recent_runs == ()


def test_render_inspection_includes_table_and_run_lines(
    telemetry: MappingFixture, tmp_path: Path
) -> None:
    db_path = _seed_sqlite(telemetry, tmp_path)
    info = inspect_sink(f"sqlite:///{db_path.as_posix()}")
    rendered = render_inspection(info)
    assert "telemetry_records" in rendered
    assert "_dataingest_runs" in rendered
    assert "20" in rendered  # row count
    assert "recent runs" in rendered
    assert "success" in rendered


def test_render_inspection_handles_empty_db() -> None:
    rendered = render_inspection(SinkInspection(url="sqlite:///x", tables=(), recent_runs=()))
    assert "(no tables)" in rendered


def test_inspect_unknown_scheme_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no sink"):
        inspect_sink("nosuchscheme:///nope")
