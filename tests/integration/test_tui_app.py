from pathlib import Path

import pytest

pytest.importorskip("textual")

from dataingest.appconfig import AppConfig, Destination, Job
from dataingest.tui.app import DataIngestApp


async def test_app_mounts_and_lists_jobs() -> None:
    config = AppConfig(
        destination=Destination(sink="out.db"),
        jobs={"bills": Job(mapping="m.yml", description="Bills")},
    )
    app = DataIngestApp(config=config)
    async with app.run_test():
        assert len(app.query("ListItem")) == 1
        assert app.sub_title == "out.db (sqlite)"


async def test_app_mounts_with_no_config() -> None:
    app = DataIngestApp(config=AppConfig())
    async with app.run_test():
        assert len(app.query("ListItem")) == 0
        assert app.sub_title == "not configured"


async def test_highlighting_a_job_shows_detail(tmp_path: Path) -> None:
    (tmp_path / "m.yml").write_text(
        """
spec_version: 1
name: bills
source: { format: csv }
target: { table: TaxBill, primary_key: id }
fields:
  id: { column: 0, type: str, required: true }
""",
        encoding="utf-8",
    )
    config = AppConfig(
        destination=Destination(sink="out.db"),
        jobs={"bills": Job(mapping="m.yml")},
    )
    app = DataIngestApp(config=config, base_dir=tmp_path)
    async with app.run_test():
        from textual.widgets import RichLog

        output = app.query_one("#output", RichLog)
        assert len(output.lines) > 0
