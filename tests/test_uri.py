import pytest

from dataingest.uri import parse, resolve_uri_path


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


# resolve_uri_path: the four shapes a parsed URI path can take


def test_resolve_windows_absolute_strips_leading_slash() -> None:
    assert resolve_uri_path("/C:/data/file.csv") == "C:/data/file.csv"


def test_resolve_windows_absolute_lowercase_drive() -> None:
    assert resolve_uri_path("/c:/data/file.csv") == "c:/data/file.csv"


def test_resolve_windows_absolute_with_backslash() -> None:
    assert resolve_uri_path("/C:\\data\\file.csv") == "C:\\data\\file.csv"


def test_resolve_posix_absolute_strips_one_slash() -> None:
    # Four-slash URI lands here: csv:////tmp/x.csv -> path "//tmp/x.csv"
    assert resolve_uri_path("//tmp/file.csv") == "/tmp/file.csv"


def test_resolve_relative_strips_leading_slash() -> None:
    # Three-slash URI with relative path: csv:///./x.csv -> path "/./x.csv"
    assert resolve_uri_path("/./file.csv") == "./file.csv"


def test_resolve_plain_path_passthrough() -> None:
    # No special prefix - return as-is.
    assert resolve_uri_path("/some/path.csv") == "/some/path.csv"


def test_resolve_empty_passthrough() -> None:
    assert resolve_uri_path("") == ""
