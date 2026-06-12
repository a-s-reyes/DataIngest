import json
from pathlib import Path

import pytest

from dataingest.errors import JsonlErrorLog, RowError


def test_jsonl_error_log_roundtrip(tmp_path: Path) -> None:
    log_path = tmp_path / "errors.jsonl"
    err = RowError(
        row_number=42,
        source_file="telemetry.csv",
        field="value",
        value="not a number",
        rule="parse_decimal",
        message="cannot parse decimal",
    )
    with JsonlErrorLog(log_path) as log:
        log.write(err)

    line = log_path.read_text(encoding="utf-8").strip()
    parsed = json.loads(line)
    assert parsed["row_number"] == 42
    assert parsed["field"] == "value"
    assert parsed["rule"] == "parse_decimal"


def test_write_outside_context_raises(tmp_path: Path) -> None:
    log = JsonlErrorLog(tmp_path / "errors.jsonl")
    err = RowError(0, "x", None, None, "r", "m")
    with pytest.raises(RuntimeError, match="context manager"):
        log.write(err)
