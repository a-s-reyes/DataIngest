import pytest

from dataingest.uri import parse


def test_csv_uri() -> None:
    p = parse("csv:///data/telemetry.csv")
    assert p.scheme == "csv"
    assert p.path == "/data/telemetry.csv"
    assert p.params == {}


def test_sqlite_uri_relative() -> None:
    # sqlite:///./out.db — three slashes + relative path, per SQLAlchemy convention
    p = parse("sqlite:///./out.db")
    assert p.scheme == "sqlite"
    assert p.path == "/./out.db"


def test_sqlite_uri_absolute() -> None:
    # sqlite:////abs/path.db — four slashes for absolute, per SQLAlchemy convention
    p = parse("sqlite:////tmp/out.db")
    assert p.scheme == "sqlite"
    assert p.path == "//tmp/out.db"


def test_query_params() -> None:
    p = parse("csv:///data/telemetry.csv?delimiter=;&encoding=latin-1")
    assert p.params == {"delimiter": ";", "encoding": "latin-1"}


def test_missing_scheme_raises() -> None:
    with pytest.raises(ValueError, match="missing scheme"):
        parse("/just/a/path.csv")
