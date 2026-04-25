from pathlib import Path

from typer.testing import CliRunner

from dataingest.cli import app

runner = CliRunner()


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.stdout


def test_validate_ok(clay_mapping: Path):
    result = runner.invoke(app, ["validate", str(clay_mapping)])
    assert result.exit_code == 0
    assert "OK:" in result.stdout
    assert "vendor=clay-sheriff-ky" in result.stdout


def test_validate_bad_yaml(tmp_path: Path):
    bad = tmp_path / "bad.yml"
    bad.write_text("not: a valid: mapping: structure", encoding="utf-8")
    result = runner.invoke(app, ["validate", str(bad)])
    assert result.exit_code == 1
    assert "error" in result.stderr.lower()


def test_run_dry_run(clay_csv: Path, clay_mapping: Path, tmp_path: Path):
    db_path = tmp_path / "out.db"
    result = runner.invoke(
        app,
        [
            "run",
            "--source",
            f"csv:///{clay_csv.as_posix()}",
            "--sink",
            f"sqlite:///{db_path.as_posix()}",
            "--mapping",
            str(clay_mapping),
            "--dry-run",
            "--errors",
            str(tmp_path / "errors.jsonl"),
        ],
    )
    assert result.exit_code == 0
    assert "rows_in=20" in result.stdout
    assert "ok=20" in result.stdout
    assert not db_path.exists()
