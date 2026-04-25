"""Sanity checks for source/sink registries."""

import pytest

from dataingest import sinks, sources


def test_csv_source_registered() -> None:
    assert "csv" in sources.REGISTRY
    cls = sources.get("csv")
    assert cls.__name__ == "CsvSource"


def test_sqlite_sink_registered() -> None:
    assert "sqlite" in sinks.REGISTRY
    cls = sinks.get("sqlite")
    assert cls.__name__ == "SqliteSink"


def test_xlsx_source_registered() -> None:
    assert "xlsx" in sources.REGISTRY
    cls = sources.get("xlsx")
    assert cls.__name__ == "XlsxSource"


def test_postgres_sink_registered() -> None:
    assert "postgres" in sinks.REGISTRY
    cls = sinks.get("postgres")
    assert cls.__name__ == "PostgresSink"


def test_postgresql_alias_resolves_to_same_class() -> None:
    """Both ``postgres://`` and ``postgresql://`` should map to the same sink."""
    assert sinks.REGISTRY["postgres"] is sinks.REGISTRY["postgresql"]


def test_unknown_scheme_raises() -> None:
    with pytest.raises(ValueError, match="no source"):
        sources.get("json")
    with pytest.raises(ValueError, match="no sink"):
        sinks.get("mssql")
