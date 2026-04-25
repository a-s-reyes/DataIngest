"""Schema inference: sniff a CSV's first N rows and emit a starter mapping YAML.

The output is *opinionated but safe*. We default to ``str`` whenever a column
isn't unambiguously something else, and we always pair each typed cleaner
with ``strip`` so that incidental whitespace doesn't break a re-run. Users
should review the emitted YAML and tighten it — the goal is to remove the
80% of mechanical work, not to be perfect.

Inference order (most specific wins, all non-empty samples must parse):

    int  ->  decimal  ->  datetime (ISO)  ->  date (ISO or US)
                 ->  bool (literals only)  ->  str (fallback)
"""

from __future__ import annotations

import csv as _csv
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal

import yaml

InferredType = Literal["str", "int", "decimal", "date", "datetime", "bool"]
DateFormat = Literal["iso", "us"]

DEFAULT_SAMPLE_SIZE = 100

_BOOL_LITERALS = {"true", "false", "yes", "no", "y", "n", "t", "f"}

# Cleaner chain templates per inferred type.
_CLEANERS: dict[str, list[str]] = {
    "str": ["strip"],
    "int": ["strip", "parse_int"],
    "decimal": ["strip", "parse_decimal"],
    "date_iso": ["strip", "parse_date_iso"],
    "date_us": ["strip", "parse_date_us"],
    "datetime": ["strip", "parse_datetime_iso"],
    "bool": ["strip"],
}


def _try_int(s: str) -> bool:
    try:
        int(s.replace("_", ""))
    except ValueError:
        return False
    return True


def _try_decimal(s: str) -> bool:
    try:
        Decimal(s)
    except InvalidOperation:
        return False
    return True


def _try_datetime_iso(s: str) -> bool:
    try:
        datetime.fromisoformat(s)
    except ValueError:
        return False
    return True


def _try_date_iso(s: str) -> bool:
    try:
        date.fromisoformat(s)
    except ValueError:
        return False
    return True


def _try_date_us(s: str) -> bool:
    try:
        datetime.strptime(s, "%m/%d/%Y")
    except ValueError:
        return False
    return True


def _is_bool_literal(s: str) -> bool:
    return s.strip().lower() in _BOOL_LITERALS


def _infer_column(samples: list[str]) -> tuple[InferredType, str]:
    """Return (pydantic_type_name, cleaner_key)."""
    non_empty = [s.strip() for s in samples if s.strip() != ""]
    if not non_empty:
        return ("str", "str")
    if all(_try_int(s) for s in non_empty):
        return ("int", "int")
    if all(_try_decimal(s) for s in non_empty):
        return ("decimal", "decimal")
    if all(_try_datetime_iso(s) for s in non_empty) and any(
        "T" in s or " " in s for s in non_empty
    ):
        return ("datetime", "datetime")
    if all(_try_date_iso(s) for s in non_empty):
        return ("date", "date_iso")
    if all(_try_date_us(s) for s in non_empty):
        return ("date", "date_us")
    if all(_is_bool_literal(s) for s in non_empty):
        return ("bool", "bool")
    return ("str", "str")


def _read_samples_csv(
    path: Path,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    delimiter: str = ",",
    encoding: str = "utf-8",
) -> tuple[list[str], list[list[str]]]:
    """Read header + up to ``sample_size`` data rows from a CSV."""
    with path.open("r", encoding=encoding, newline="") as fp:
        reader = _csv.reader(fp, delimiter=delimiter)
        header = next(reader, [])
        rows: list[list[str]] = []
        for i, row in enumerate(reader):
            if i >= sample_size:
                break
            rows.append(row)
    return header, rows


def _read_samples_xlsx(
    path: Path,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    sheet: str | None = None,
) -> tuple[list[str], list[list[str]]]:
    """Read header + up to ``sample_size`` data rows from an .xlsx workbook.

    Native cell types (int, float, datetime) are stringified at read time so
    the same type-classifier helpers work for both csv and xlsx inputs.
    """
    import openpyxl

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb[sheet] if sheet else wb.active
        if ws is None:
            return [], []
        rows_iter = ws.iter_rows(values_only=True)
        first = next(rows_iter, None)
        if first is None:
            return [], []
        header = [str(h) if h is not None else "" for h in first]
        samples: list[list[str]] = []
        for i, raw in enumerate(rows_iter):
            if i >= sample_size:
                break
            samples.append(["" if v is None else str(v) for v in raw])
        return header, samples
    finally:
        wb.close()


def _detect_format(path: Path) -> Literal["csv", "xlsx"]:
    return "xlsx" if path.suffix.lower() in (".xlsx", ".xlsm") else "csv"


def _pick_primary_key(headers: list[str], rows: list[list[str]]) -> str | None:
    """First column where every sampled value is non-empty and unique."""
    for col_idx, name in enumerate(headers):
        values = [row[col_idx].strip() if col_idx < len(row) else "" for row in rows]
        if any(v == "" for v in values):
            continue
        if len(set(values)) == len(values):
            return name
    return None


def infer_mapping(
    path: Path,
    *,
    name: str | None = None,
    table: str | None = None,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    delimiter: str = ",",
    encoding: str = "utf-8",
    sheet: str | None = None,
    format: Literal["csv", "xlsx"] | None = None,
) -> dict[str, Any]:
    """Sniff a tabular file and return a mapping dict.

    ``format`` defaults to autodetection from the file extension
    (``.xlsx`` / ``.xlsm`` -> xlsx, everything else -> csv). Override
    explicitly when the extension is misleading.
    """
    fmt = format or _detect_format(path)
    if fmt == "xlsx":
        # ``sheet`` only steers which sheet we *sample* during inference.
        # The runtime sheet selection happens via URI param (xlsx:///...?sheet=X),
        # not the mapping — so we do not echo it back into the output YAML.
        header, rows = _read_samples_xlsx(path, sample_size, sheet)
        source_block: dict[str, Any] = {"format": "xlsx", "header": True}
    else:
        header, rows = _read_samples_csv(path, sample_size, delimiter, encoding)
        source_block = {
            "format": "csv",
            "encoding": encoding,
            "header": True,
            "delimiter": delimiter,
        }

    if not header:
        raise ValueError(f"{path}: no header row found (empty file?)")

    fields: dict[str, dict[str, Any]] = {}
    for col_idx, col_name in enumerate(header):
        samples = [row[col_idx] if col_idx < len(row) else "" for row in rows]
        type_name, cleaner_key = _infer_column(samples)
        is_required = bool(samples) and all(s.strip() != "" for s in samples)
        fields[col_name] = {
            "column": col_name,
            "type": type_name,
            "required": is_required,
            "cleaners": _CLEANERS[cleaner_key],
        }

    primary_key = _pick_primary_key(header, rows) or header[0]
    # Mark whichever field we picked as the PK as required, regardless of sample.
    if primary_key in fields:
        fields[primary_key]["required"] = True

    stem = path.stem.replace("-", "_").replace(" ", "_") or "ingested"
    return {
        "spec_version": 1,
        "name": name or stem,
        "description": f"Inferred from {path.name} ({len(rows)} sample rows)",
        "source": source_block,
        "target": {
            "table": table or stem,
            "primary_key": primary_key,
            "on_conflict": "skip",
        },
        "fields": fields,
    }


def dump_mapping(mapping: dict[str, Any]) -> str:
    """Serialize a mapping dict to a human-friendly YAML string."""
    return yaml.safe_dump(
        mapping,
        sort_keys=False,
        indent=2,
        default_flow_style=False,
        allow_unicode=True,
    )
