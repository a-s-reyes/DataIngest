from pathlib import Path

import pytest

from dataingest.appconfig import (
    AppConfig,
    load_config,
    path_to_sink_uri,
    path_to_source_uri,
    resolve_job,
)


def test_path_to_source_uri_csv() -> None:
    uri = path_to_source_uri("data.csv")
    assert uri.startswith("csv:///")
    assert uri.endswith("/data.csv")


def test_path_to_source_uri_xlsx() -> None:
    assert path_to_source_uri("book.xlsx").startswith("xlsx:///")


def test_path_to_source_uri_passes_through_existing_uri() -> None:
    assert path_to_source_uri("csv:///x/y.csv") == "csv:///x/y.csv"


def test_path_to_sink_uri_defaults_sqlite() -> None:
    assert path_to_sink_uri("out.db").startswith("sqlite:///")


def test_path_to_sink_uri_csv() -> None:
    assert path_to_sink_uri("out.csv").startswith("csv:///")


def test_path_to_sink_uri_passes_through_postgres() -> None:
    assert path_to_sink_uri("postgres://u:p@h/db") == "postgres://u:p@h/db"


def _write_config(tmp_path: Path) -> Path:
    p = tmp_path / "dataingest.toml"
    p.write_text(
        """
[destination]
sink = "out.db"

[jobs.bills]
mapping = "mappings/bills.yml"
description = "Delinquent bills"

[jobs.parts]
mapping = "mappings/parts.yml"
""",
        encoding="utf-8",
    )
    return p


def test_load_config_parses_destination_and_jobs(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path))
    assert config.destination is not None
    assert config.destination.sink == "out.db"
    assert set(config.jobs) == {"bills", "parts"}
    assert config.jobs["bills"].description == "Delinquent bills"


def test_resolve_job_builds_uris_and_mapping_path(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path))
    resolved = resolve_job(config, "bills", "thisweek.csv", tmp_path)
    assert resolved.source_uri.startswith("csv:///")
    assert resolved.source_uri.endswith("/thisweek.csv")
    assert resolved.sink_uri.startswith("sqlite:///")
    assert resolved.mapping == (tmp_path / "mappings/bills.yml").resolve()


def test_resolve_job_unknown_name_raises() -> None:
    config = AppConfig.model_validate({"destination": {"sink": "out.db"}, "jobs": {}})
    with pytest.raises(ValueError, match="unknown job"):
        resolve_job(config, "nope", "x.csv", Path.cwd())


def test_resolve_job_without_destination_raises(tmp_path: Path) -> None:
    config = AppConfig.model_validate({"jobs": {"bills": {"mapping": "m.yml"}}})
    with pytest.raises(ValueError, match="destination"):
        resolve_job(config, "bills", "x.csv", tmp_path)
