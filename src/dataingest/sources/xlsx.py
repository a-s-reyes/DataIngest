"""Excel (.xlsx) source.

Lazy-imports ``openpyxl`` so CSV-only users do not pay the dep cost. The
extra is declared in ``pyproject.toml`` as ``[project.optional-dependencies]
xlsx``; install with ``uv sync --extra xlsx`` (or ``--all-extras`` in CI).
"""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

from ..uri import resolve_uri_path
from . import register


@register("xlsx")
class XlsxSource:
    """Reads rows from an .xlsx workbook.

    URI parameters:
        ``sheet``  -- sheet name to read; defaults to the workbook's active sheet
        ``header`` -- ``true`` (default) treats the first row as column names;
                      ``false`` keeps numeric-index keys only
    """

    def __init__(self, path: str, params: dict[str, str]) -> None:
        self.path = Path(resolve_uri_path(path))
        self.sheet = params.get("sheet")
        self.has_header = params.get("header", "true").lower() in ("true", "1", "yes")

    def rows(self) -> Iterator[dict[str, Any]]:
        try:
            import openpyxl
        except ImportError as err:  # pragma: no cover - exercised only when extra missing
            raise ImportError(
                "xlsx:// source requires openpyxl. Install with: uv sync --extra xlsx"
            ) from err

        wb = openpyxl.load_workbook(self.path, read_only=True, data_only=True)
        try:
            ws = wb[self.sheet] if self.sheet else wb.active
            if ws is None:
                return
            rows_iter = ws.iter_rows(values_only=True)
            headers: list[str] | None = None
            if self.has_header:
                first = next(rows_iter, None)
                if first is not None:
                    headers = [str(h) if h is not None else "" for h in first]
            for raw in rows_iter:
                row: dict[str, Any] = {}
                for i, value in enumerate(raw):
                    row[str(i)] = value
                    if headers and i < len(headers):
                        row[headers[i]] = value
                yield row
        finally:
            wb.close()

    def close(self) -> None:
        # Workbook is opened/closed inside rows() via try/finally.
        pass
