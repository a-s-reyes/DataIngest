from pathlib import Path

from typer.testing import CliRunner

from dataingest.cli import (
    EXIT_OK,
    EXIT_PARTIAL_FAILURE,
    EXIT_PREFLIGHT_ERROR,
    EXIT_TOTAL_FAILURE,
    app,
)

from .conftest import MappingFixture

runner = CliRunner()


def test_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.stdout


def test_validate_ok(telemetry: MappingFixture) -> None:
    result = runner.invoke(app, ["validate", str(telemetry.mapping_yml)])
    assert result.exit_code == 0
    assert "OK:" in result.stdout
    assert "name=flight-test-telemetry" in result.stdout


def test_validate_bad_yaml(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yml"
    bad.write_text("not: a valid: mapping: structure", encoding="utf-8")
    result = runner.invoke(app, ["validate", str(bad)])
    assert result.exit_code == 1
    assert "error" in result.stderr.lower()


def test_run_dry_run(telemetry: MappingFixture, tmp_path: Path) -> None:
    db_path = tmp_path / "out.db"
    result = runner.invoke(
        app,
        [
            "run",
            "--source",
            f"csv:///{telemetry.csv.as_posix()}",
            "--sink",
            f"sqlite:///{db_path.as_posix()}",
            "--mapping",
            str(telemetry.mapping_yml),
            "--dry-run",
            "--errors",
            str(tmp_path / "errors.jsonl"),
        ],
    )
    assert result.exit_code == 0
    assert "rows_in=20" in result.stdout
    assert "ok=20" in result.stdout
    assert "chunks=0" in result.stdout  # dry-run never writes
    assert not db_path.exists()


def test_run_chunk_size_flag(telemetry: MappingFixture, tmp_path: Path) -> None:
    db_path = tmp_path / "out.db"
    result = runner.invoke(
        app,
        [
            "run",
            "--source",
            f"csv:///{telemetry.csv.as_posix()}",
            "--sink",
            f"sqlite:///{db_path.as_posix()}",
            "--mapping",
            str(telemetry.mapping_yml),
            "--chunk-size",
            "7",
            "--errors",
            str(tmp_path / "errors.jsonl"),
        ],
    )
    assert result.exit_code == 0
    # 20 rows / 7 per chunk = 3 chunks (7, 7, 6)
    assert "chunks=3" in result.stdout


def test_run_chunk_size_zero_rejected(telemetry: MappingFixture, tmp_path: Path) -> None:
    db_path = tmp_path / "out.db"
    result = runner.invoke(
        app,
        [
            "run",
            "--source",
            f"csv:///{telemetry.csv.as_posix()}",
            "--sink",
            f"sqlite:///{db_path.as_posix()}",
            "--mapping",
            str(telemetry.mapping_yml),
            "--chunk-size",
            "0",
        ],
    )
    assert result.exit_code != 0


# --- Exit codes (T2.6) ---


def test_clean_run_exits_zero(telemetry: MappingFixture, tmp_path: Path) -> None:
    """All rows valid → exit 0."""
    result = runner.invoke(
        app,
        [
            "run",
            "--source",
            f"csv:///{telemetry.csv.as_posix()}",
            "--sink",
            f"sqlite:///{(tmp_path / 'out.db').as_posix()}",
            "--mapping",
            str(telemetry.mapping_yml),
            "--errors",
            str(tmp_path / "errors.jsonl"),
        ],
    )
    assert result.exit_code == EXIT_OK


def test_partial_failure_exits_two(qualification: MappingFixture, tmp_path: Path) -> None:
    """Some rows succeed, some fail → exit 2."""
    mixed = tmp_path / "mixed.csv"
    mixed.write_text(
        "test_id,part_number,run_date,parameter,measured_value,tolerance,result,technician\n"
        "QT-1,P-1,03/12/2026,TEMP,1.0,100,PASS,T\n"
        "QT-2,P-2,03/12/2026,TEMP,not-a-number,100,PASS,T\n"
        "QT-3,P-3,03/12/2026,TEMP,2.0,100,PASS,T\n",
        encoding="utf-8",
    )
    result = runner.invoke(
        app,
        [
            "run",
            "--source",
            f"csv:///{mixed.as_posix()}",
            "--sink",
            f"sqlite:///{(tmp_path / 'out.db').as_posix()}",
            "--mapping",
            str(qualification.mapping_yml),
            "--errors",
            str(tmp_path / "errors.jsonl"),
        ],
    )
    assert result.exit_code == EXIT_PARTIAL_FAILURE


def test_total_failure_exits_three(qualification: MappingFixture, tmp_path: Path) -> None:
    """No rows survive → exit 3."""
    bad = tmp_path / "bad.csv"
    bad.write_text(
        "test_id,part_number,run_date,parameter,measured_value,tolerance,result,technician\n"
        "QT-1,P-1,03/12/2026,TEMP,not-a-number,100,PASS,T\n"
        "QT-2,P-2,03/12/2026,TEMP,nope,100,PASS,T\n",
        encoding="utf-8",
    )
    result = runner.invoke(
        app,
        [
            "run",
            "--source",
            f"csv:///{bad.as_posix()}",
            "--sink",
            f"sqlite:///{(tmp_path / 'out.db').as_posix()}",
            "--mapping",
            str(qualification.mapping_yml),
            "--errors",
            str(tmp_path / "errors.jsonl"),
        ],
    )
    assert result.exit_code == EXIT_TOTAL_FAILURE


def test_preflight_error_exits_one(tmp_path: Path) -> None:
    bad_mapping = tmp_path / "bad.yml"
    bad_mapping.write_text("garbage: [[", encoding="utf-8")
    result = runner.invoke(
        app,
        [
            "run",
            "--source",
            "csv:///nope.csv",
            "--sink",
            "sqlite:///./nope.db",
            "--mapping",
            str(bad_mapping),
        ],
    )
    assert result.exit_code == EXIT_PREFLIGHT_ERROR


# --- --quiet (T2.6) ---


def test_quiet_suppresses_summary(telemetry: MappingFixture, tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "run",
            "--source",
            f"csv:///{telemetry.csv.as_posix()}",
            "--sink",
            f"sqlite:///{(tmp_path / 'out.db').as_posix()}",
            "--mapping",
            str(telemetry.mapping_yml),
            "--errors",
            str(tmp_path / "errors.jsonl"),
            "--quiet",
        ],
    )
    assert result.exit_code == EXIT_OK
    assert "rows_in=" not in result.stdout
    assert "run_id=" not in result.stdout


# --- --errors - (stderr streaming) (T2.6) ---


def test_errors_dash_streams_to_stderr(qualification: MappingFixture, tmp_path: Path) -> None:
    mixed = tmp_path / "mixed.csv"
    mixed.write_text(
        "test_id,part_number,run_date,parameter,measured_value,tolerance,result,technician\n"
        "QT-1,P-1,03/12/2026,TEMP,1.0,100,PASS,T\n"
        "QT-2,P-2,03/12/2026,TEMP,not-a-number,100,PASS,T\n",
        encoding="utf-8",
    )
    # Mix stdout and stderr so we can read them separately.
    runner_split = CliRunner()
    result = runner_split.invoke(
        app,
        [
            "run",
            "--source",
            f"csv:///{mixed.as_posix()}",
            "--sink",
            f"sqlite:///{(tmp_path / 'out.db').as_posix()}",
            "--mapping",
            str(qualification.mapping_yml),
            "--errors",
            "-",
        ],
    )
    assert result.exit_code == EXIT_PARTIAL_FAILURE
    # The bad row's value should appear in stderr as part of the JSONL stream.
    assert "not-a-number" in result.stderr
    # No errors.jsonl file should have been created in cwd.
    assert not (Path.cwd() / "errors.jsonl").exists()


# --- --verbose (T2.6) ---


def test_verbose_emits_pipeline_log_lines(telemetry: MappingFixture, tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "run",
            "-v",
            "--source",
            f"csv:///{telemetry.csv.as_posix()}",
            "--sink",
            f"sqlite:///{(tmp_path / 'out.db').as_posix()}",
            "--mapping",
            str(telemetry.mapping_yml),
            "--errors",
            str(tmp_path / "errors.jsonl"),
        ],
    )
    assert result.exit_code == EXIT_OK
    # logging goes to stderr by default under basicConfig
    assert "pipeline starting" in result.stderr
    assert "pipeline finished" in result.stderr
