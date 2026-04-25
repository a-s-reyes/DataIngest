import csv as _csv
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from . import register


def _resolve_path(uri_path: str) -> Path:
    """Convert a URI path to a usable filesystem Path.

    On Windows, urlparse turns ``csv:///C:/data/x.csv`` into ``/C:/data/x.csv``.
    Strip the leading slash when followed by a drive letter.
    """
    if re.match(r"^/[A-Za-z]:[/\\]", uri_path):
        return Path(uri_path[1:])
    return Path(uri_path)


@register("csv")
class CsvSource:
    """Reads rows from a delimited text file.

    Yields each data row as a dict with both ``"<index>"`` and (when present)
    header-name keys, so a YAML mapping may reference columns by either.
    """

    def __init__(self, path: str, params: dict[str, str]) -> None:
        self.path = _resolve_path(path)
        self.encoding = params.get("encoding", "utf-8")
        self.delimiter = params.get("delimiter", ",")
        self.has_header = params.get("header", "true").lower() in ("true", "1", "yes")

    def rows(self) -> Iterator[dict[str, Any]]:
        with self.path.open("r", encoding=self.encoding, newline="") as fp:
            reader = _csv.reader(fp, delimiter=self.delimiter)
            headers: list[str] | None = next(reader) if self.has_header else None
            for raw in reader:
                row: dict[str, Any] = {}
                for i, value in enumerate(raw):
                    row[str(i)] = value
                    if headers is not None and i < len(headers):
                        row[headers[i]] = value
                yield row

    def close(self) -> None:
        # File is opened/closed inside rows() via context manager.
        pass
