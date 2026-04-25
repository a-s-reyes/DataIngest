from pathlib import Path

import pytest

from dataingest.sources.csv import CsvSource
from dataingest.uri import parse

from .conftest import MappingFixture


def test_yields_rows_with_index_and_header_keys(telemetry: MappingFixture) -> None:
    src = CsvSource(str(telemetry.csv), {})
    rows = list(src.rows())
    assert len(rows) == telemetry.row_count

    first = rows[0]
    # both index and header keys present
    assert first["0"] == "TM-00001"
    assert first["record_id"] == "TM-00001"
    assert first["3"] == "acc_x_fuselage"
    assert first["channel"] == "acc_x_fuselage"


def test_no_header_uses_index_only(tmp_path: Path) -> None:
    raw = tmp_path / "noheader.csv"
    raw.write_text("a,b,c\nd,e,f\n", encoding="utf-8")
    src = CsvSource(str(raw), {"header": "false"})
    rows = list(src.rows())
    assert rows == [
        {"0": "a", "1": "b", "2": "c"},
        {"0": "d", "1": "e", "2": "f"},
    ]


def test_custom_delimiter(tmp_path: Path) -> None:
    raw = tmp_path / "semi.csv"
    raw.write_text("name;value\nfoo;1\nbar;2\n", encoding="utf-8")
    src = CsvSource(str(raw), {"delimiter": ";"})
    rows = list(src.rows())
    assert rows[0]["name"] == "foo"
    assert rows[1]["value"] == "2"


def test_relative_csv_uri_resolves_against_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: ``csv:///./file.csv`` used to fail because CsvSource only
    handled the Windows drive-letter URI shape. Three-slash relative URIs are
    legal per the same SQLAlchemy convention SqliteSink already followed."""
    raw = tmp_path / "data.csv"
    raw.write_text("a,b\n1,2\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    parsed = parse("csv:///./data.csv")
    src = CsvSource(parsed.path, parsed.params)
    rows = list(src.rows())
    assert rows == [{"0": "1", "1": "2", "a": "1", "b": "2"}]
