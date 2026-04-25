"""Tests for XlsxSource. openpyxl is generated/read in-process so no binary
fixtures need to be checked in."""

from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from openpyxl import Workbook
from sqlalchemy import create_engine, text

from dataingest.config import Mapping
from dataingest.pipeline import Pipeline
from dataingest.sources.xlsx import XlsxSource


def _make_workbook(path: Path, sheets: dict[str, list[list[Any]]]) -> None:
    """Write a multi-sheet workbook. First key becomes the active sheet."""
    wb = Workbook()
    # Workbook ships with an empty default sheet; replace it with the first one we want.
    default = wb.active
    if default is not None:
        wb.remove(default)
    for name, rows in sheets.items():
        ws = wb.create_sheet(title=name)
        for row in rows:
            ws.append(row)
    wb.save(str(path))


def test_yields_rows_with_index_and_header_keys(tmp_path: Path) -> None:
    xlsx = tmp_path / "data.xlsx"
    _make_workbook(
        xlsx,
        {
            "Sheet1": [
                ["record_id", "channel", "value"],
                ["TM-1", "ACC_X", 0.4823],
                ["TM-2", "ACC_Y", -0.0117],
            ],
        },
    )
    src = XlsxSource(str(xlsx), {})
    rows = list(src.rows())

    assert len(rows) == 2
    assert rows[0]["0"] == "TM-1"
    assert rows[0]["record_id"] == "TM-1"
    assert rows[0]["channel"] == "ACC_X"
    assert rows[0]["value"] == 0.4823
    assert rows[1]["record_id"] == "TM-2"


def test_no_header_uses_index_only(tmp_path: Path) -> None:
    xlsx = tmp_path / "noheader.xlsx"
    _make_workbook(xlsx, {"Sheet1": [["a", "b", "c"], ["d", "e", "f"]]})

    src = XlsxSource(str(xlsx), {"header": "false"})
    rows = list(src.rows())
    assert rows == [
        {"0": "a", "1": "b", "2": "c"},
        {"0": "d", "1": "e", "2": "f"},
    ]


def test_sheet_param_selects_named_sheet(tmp_path: Path) -> None:
    xlsx = tmp_path / "multi.xlsx"
    _make_workbook(
        xlsx,
        {
            "Bills": [["id", "amount"], ["B1", 100]],
            "Owners": [["id", "name"], ["O1", "ALICE"]],
        },
    )
    src = XlsxSource(str(xlsx), {"sheet": "Owners"})
    rows = list(src.rows())
    assert rows == [{"0": "O1", "1": "ALICE", "id": "O1", "name": "ALICE"}]


def test_default_sheet_is_active(tmp_path: Path) -> None:
    """When no sheet param is given, fall back to wb.active (first user sheet)."""
    xlsx = tmp_path / "multi.xlsx"
    _make_workbook(
        xlsx,
        {
            "First": [["id"], ["A1"]],
            "Second": [["id"], ["B1"]],
        },
    )
    src = XlsxSource(str(xlsx), {})  # no 'sheet' param
    rows = list(src.rows())
    assert rows[0]["id"] == "A1"


def test_native_types_preserved(tmp_path: Path) -> None:
    """openpyxl returns native types (int, float, datetime) — the cleaner chain
    handles coercion downstream. The source should not stringify."""
    xlsx = tmp_path / "types.xlsx"
    _make_workbook(
        xlsx,
        {
            "Sheet1": [
                ["int_col", "float_col", "dt_col"],
                [42, 3.14, datetime(2026, 4, 12, 14, 22, 1)],
            ],
        },
    )
    rows = list(XlsxSource(str(xlsx), {}).rows())
    assert rows[0]["int_col"] == 42
    assert rows[0]["float_col"] == 3.14
    assert isinstance(rows[0]["dt_col"], datetime)


def test_empty_workbook_yields_nothing(tmp_path: Path) -> None:
    xlsx = tmp_path / "empty.xlsx"
    _make_workbook(xlsx, {"Sheet1": [["header_only"]]})  # header but no data rows
    rows = list(XlsxSource(str(xlsx), {}).rows())
    assert rows == []


def test_close_is_safe_to_call(tmp_path: Path) -> None:
    xlsx = tmp_path / "data.xlsx"
    _make_workbook(xlsx, {"Sheet1": [["a"], ["1"]]})
    src = XlsxSource(str(xlsx), {})
    list(src.rows())
    src.close()  # Should not raise even though workbook is already closed.


def test_unknown_sheet_raises(tmp_path: Path) -> None:
    xlsx = tmp_path / "data.xlsx"
    _make_workbook(xlsx, {"Sheet1": [["a"], ["1"]]})
    src = XlsxSource(str(xlsx), {"sheet": "Nonexistent"})
    with pytest.raises(KeyError):
        list(src.rows())


def test_xlsx_e2e_through_pipeline(tmp_path: Path) -> None:
    """Full pipeline: xlsx:// source -> mapping -> sqlite sink, 20 synthetic rows."""
    xlsx = tmp_path / "telemetry.xlsx"
    rows: list[list[Any]] = [["record_id", "channel", "value", "unit"]]
    for i in range(1, 21):
        rows.append([f"TM-{i:05d}", "ACC_X", round(i * 0.001, 4), "g"])
    _make_workbook(xlsx, {"Sheet1": rows})

    yml = tmp_path / "mapping.yml"
    yml.write_text(
        """
spec_version: 1
name: xlsx-e2e
source: { format: csv }
target:
  table: readings
  primary_key: record_id
fields:
  record_id:
    column: record_id
    type: str
    required: true
    cleaners: [strip, upper]
  channel:
    column: channel
    type: str
    required: true
    cleaners: [strip, upper]
  value:
    column: value
    type: decimal
    required: true
    cleaners: [parse_decimal]
  unit:
    column: unit
    type: str
    required: true
    cleaners: [strip]
""",
        encoding="utf-8",
    )

    db_path = tmp_path / "out.db"
    pipeline = Pipeline(
        source_uri=f"xlsx:///{xlsx.as_posix()}",
        sink_uri=f"sqlite:///{db_path.as_posix()}",
        mapping=Mapping.from_yaml(yml),
        error_log=tmp_path / "errors.jsonl",
    )
    result = pipeline.run()

    assert result.rows_in == 20
    assert result.rows_ok == 20
    assert result.rows_failed == 0

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM readings")).scalar()
        assert count == 20
        first = conn.execute(
            text("SELECT record_id, channel, unit FROM readings ORDER BY record_id LIMIT 1")
        ).first()
        assert first is not None
        assert first.record_id == "TM-00001"
        assert first.channel == "ACC_X"
        assert first.unit == "g"
