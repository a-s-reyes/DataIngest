import pytest

from dataingest.uri import parse


def test_csv_uri():
    p = parse("csv:///data/clay.csv")
    assert p.scheme == "csv"
    assert p.path == "/data/clay.csv"
    assert p.params == {}


def test_sqlite_uri_relative():
    # sqlite:///./out.db — three slashes + relative path, per SQLAlchemy convention
    p = parse("sqlite:///./out.db")
    assert p.scheme == "sqlite"
    assert p.path == "/./out.db"


def test_sqlite_uri_absolute():
    # sqlite:////abs/path.db — four slashes for absolute, per SQLAlchemy convention
    p = parse("sqlite:////tmp/out.db")
    assert p.scheme == "sqlite"
    assert p.path == "//tmp/out.db"


def test_query_params():
    p = parse("csv:///data/clay.csv?delimiter=;&encoding=latin-1")
    assert p.params == {"delimiter": ";", "encoding": "latin-1"}


def test_missing_scheme_raises():
    with pytest.raises(ValueError, match="missing scheme"):
        parse("/just/a/path.csv")
