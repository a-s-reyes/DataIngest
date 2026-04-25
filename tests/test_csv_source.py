from pathlib import Path

from dataingest.sources.csv import CsvSource


def test_yields_rows_with_index_and_header_keys(clay_csv: Path):
    src = CsvSource(str(clay_csv), {})
    rows = list(src.rows())
    assert len(rows) == 20

    first = rows[0]
    # both index and header keys present
    assert first["0"] == "00001"
    assert first["Bill Number"] == "00001"
    assert first["3"] == "SMITH JOHN"
    assert first["Owner Name"] == "SMITH JOHN"


def test_no_header_uses_index_only(tmp_path: Path):
    raw = tmp_path / "noheader.csv"
    raw.write_text("a,b,c\nd,e,f\n", encoding="utf-8")
    src = CsvSource(str(raw), {"header": "false"})
    rows = list(src.rows())
    assert rows == [
        {"0": "a", "1": "b", "2": "c"},
        {"0": "d", "1": "e", "2": "f"},
    ]


def test_custom_delimiter(tmp_path: Path):
    raw = tmp_path / "semi.csv"
    raw.write_text("name;value\nfoo;1\nbar;2\n", encoding="utf-8")
    src = CsvSource(str(raw), {"delimiter": ";"})
    rows = list(src.rows())
    assert rows[0]["name"] == "foo"
    assert rows[1]["value"] == "2"
