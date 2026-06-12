from pathlib import Path

from typer.testing import CliRunner

from dataingest.cli import (
    EXIT_OK,
    EXIT_PARTIAL_FAILURE,
    EXIT_PREFLIGHT_ERROR,
    EXIT_TOTAL_FAILURE,
    app,
)

from ..conftest import MappingFixture

runner = CliRunner()


def test_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.stdout


def test_app_help_strings_are_ascii_only() -> None:
    """Regression: Windows cp1252 terminals crash on non-ASCII characters when
    rendering Typer's app description. Rich auto-falls-back to ASCII for its
    own frame chars, but values we set in ``help=`` go through verbatim — so
    keep every help string we control ASCII-clean."""

    def assert_ascii(label: str, value: str | None) -> None:
        if value is None:
            return
        try:
            value.encode("ascii")
        except UnicodeEncodeError as err:
            raise AssertionError(f"non-ASCII in {label}: {value!r}") from err

    assert_ascii("app.info.help", app.info.help)
    for cmd in app.registered_commands:
        assert_ascii(f"{cmd.name} help", cmd.help)
        if cmd.callback is not None and cmd.callback.__doc__ is not None:
            assert_ascii(f"{cmd.name} docstring", cmd.callback.__doc__)


def test_all_helps_render_without_crashing() -> None:
    """Sanity: every --help invocation exits 0 (rich handles its own framing)."""
    for argv in [
        ["--help"],
        ["run", "--help"],
        ["validate", "--help"],
        ["infer", "--help"],
        ["tables", "--help"],
        ["version", "--help"],
    ]:
        result = runner.invoke(app, argv)
        assert result.exit_code == 0, f"{argv} failed: {result.output}"


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


def test_infer_stdout_emits_runnable_yaml(tmp_path: Path) -> None:
    csv = tmp_path / "things.csv"
    csv.write_text("id,name,amount\nA,alpha,1.5\nB,beta,2.75\n", encoding="utf-8")
    result = runner.invoke(app, ["infer", str(csv)])
    assert result.exit_code == EXIT_OK
    assert "spec_version: 1" in result.stdout
    assert "name: things" in result.stdout
    assert "primary_key: id" in result.stdout


def test_infer_writes_to_output_file(tmp_path: Path) -> None:
    csv = tmp_path / "things.csv"
    csv.write_text("id,name\nA,alpha\nB,beta\n", encoding="utf-8")
    out = tmp_path / "out.yml"
    result = runner.invoke(app, ["infer", str(csv), "-o", str(out)])
    assert result.exit_code == EXIT_OK
    assert out.exists()
    assert "spec_version: 1" in out.read_text(encoding="utf-8")


def test_infer_missing_file_exits_one(tmp_path: Path) -> None:
    result = runner.invoke(app, ["infer", str(tmp_path / "no_such.csv")])
    assert result.exit_code == EXIT_PREFLIGHT_ERROR


# --- tables (T3.3) ---


def test_tables_lists_data_and_manifest(telemetry: MappingFixture, tmp_path: Path) -> None:
    """End-to-end: run a pipeline, then `tables` shows the resulting tables + run."""
    from dataingest.config import Mapping
    from dataingest.pipeline import Pipeline

    db_path = tmp_path / "out.db"
    Pipeline(
        source_uri=f"csv:///{telemetry.csv.as_posix()}",
        sink_uri=f"sqlite:///{db_path.as_posix()}",
        mapping=Mapping.from_yaml(telemetry.mapping_yml),
        error_log=tmp_path / "errors.jsonl",
    ).run()

    result = runner.invoke(app, ["tables", f"sqlite:///{db_path.as_posix()}"])
    assert result.exit_code == EXIT_OK
    assert "telemetry_records" in result.stdout
    assert "_dataingest_runs" in result.stdout
    assert "20" in result.stdout
    assert "recent runs" in result.stdout


def test_tables_unknown_scheme_exits_one() -> None:
    result = runner.invoke(app, ["tables", "bogus:///nowhere"])
    assert result.exit_code == EXIT_PREFLIGHT_ERROR


# --- Exception-handling regression: B5 / B7 / B8 ---


def test_bad_source_path_exits_clean(telemetry: MappingFixture, tmp_path: Path) -> None:
    """B5: a typo'd source path used to raise raw FileNotFoundError with a
    full Python traceback. Now it must exit 1 with a one-line error."""
    result = runner.invoke(
        app,
        [
            "run",
            "--source",
            "csv:///./does_not_exist.csv",
            "--sink",
            f"sqlite:///{(tmp_path / 'out.db').as_posix()}",
            "--mapping",
            str(telemetry.mapping_yml),
            "--errors",
            str(tmp_path / "errors.jsonl"),
        ],
    )
    assert result.exit_code == EXIT_PREFLIGHT_ERROR
    assert "Traceback" not in result.stderr
    assert "FileNotFoundError" in result.stderr or "No such file" in result.stderr


def test_tables_on_missing_sqlite_exits_clean(tmp_path: Path) -> None:
    """B7: ``tables`` on a non-existent sqlite path used to silently create
    the file. Now it must exit 1 with a clear "database not found" message
    and leave no stray file behind."""
    target = tmp_path / "does_not_exist.db"
    result = runner.invoke(app, ["tables", f"sqlite:///{target.as_posix()}"])
    assert result.exit_code == EXIT_PREFLIGHT_ERROR
    assert "not found" in result.stderr
    assert not target.exists(), "tables must not create the sqlite file"


def test_bad_xlsx_sheet_exits_clean(tmp_path: Path, telemetry: MappingFixture) -> None:
    """B8: a missing sheet name used to raise raw KeyError with a traceback."""
    import openpyxl

    xlsx = tmp_path / "one_sheet.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["record_id", "channel", "value"])
    ws.append(["TM-1", "ACC_X", 0.1])
    wb.save(xlsx)

    result = runner.invoke(
        app,
        [
            "run",
            "--source",
            f"xlsx:///{xlsx.as_posix()}?sheet=Nonexistent",
            "--sink",
            f"sqlite:///{(tmp_path / 'out.db').as_posix()}",
            "--mapping",
            str(telemetry.mapping_yml),
            "--errors",
            str(tmp_path / "errors.jsonl"),
        ],
    )
    assert result.exit_code == EXIT_PREFLIGHT_ERROR
    assert "Traceback" not in result.stderr


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
