from pathlib import Path

from typer.testing import CliRunner

from dataingest.cli import app

from .conftest import MappingFixture

runner = CliRunner()


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.stdout


def test_validate_ok(telemetry: MappingFixture):
    result = runner.invoke(app, ["validate", str(telemetry.mapping_yml)])
    assert result.exit_code == 0
    assert "OK:" in result.stdout
    assert "name=flight-test-telemetry" in result.stdout


def test_validate_bad_yaml(tmp_path: Path):
    bad = tmp_path / "bad.yml"
    bad.write_text("not: a valid: mapping: structure", encoding="utf-8")
    result = runner.invoke(app, ["validate", str(bad)])
    assert result.exit_code == 1
    assert "error" in result.stderr.lower()


def test_run_dry_run(telemetry: MappingFixture, tmp_path: Path):
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
    assert not db_path.exists()
