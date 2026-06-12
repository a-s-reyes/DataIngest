from pathlib import Path

import pytest
from typer.testing import CliRunner

from dataingest.cli import app

runner = CliRunner()


def test_jobs_lists_configured_jobs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "dataingest.toml").write_text(
        """
[destination]
sink = "out.db"

[jobs.bills]
mapping = "mappings/bills.yml"
description = "Delinquent bills"
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["jobs"])
    assert result.exit_code == 0
    assert "bills" in result.stdout
    assert "Delinquent bills" in result.stdout


def test_jobs_without_config_is_friendly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["jobs"])
    assert result.exit_code == 1
    assert "No dataingest.toml" in result.stderr
