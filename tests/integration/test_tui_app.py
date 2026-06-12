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
