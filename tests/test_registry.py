"""Sanity checks for source/sink registries."""

import pytest

from dataingest import sinks, sources


def test_csv_source_registered():
    assert "csv" in sources.REGISTRY
    cls = sources.get("csv")
    assert cls.__name__ == "CsvSource"


def test_sqlite_sink_registered():
    assert "sqlite" in sinks.REGISTRY
    cls = sinks.get("sqlite")
    assert cls.__name__ == "SqliteSink"


def test_unknown_scheme_raises():
    with pytest.raises(ValueError, match="no source"):
        sources.get("xlsx")
    with pytest.raises(ValueError, match="no sink"):
        sinks.get("postgres")
