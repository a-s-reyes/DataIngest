import csv as _csv
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Any

from pydantic import BaseModel

from ..manifest import RunManifest
from ..uri import resolve_uri_path
from . import register


@register("csv")
class CsvSink:
    def __init__(self, path: str, params: dict[str, str]) -> None:
        self.fs_path = Path(resolve_uri_path(path))
        self.delimiter = params.get("delimiter", ",")
        self.encoding = params.get("encoding", "utf-8")
        self._fp: IO[str] | None = None
        self._writer: Any = None
        self._columns: list[str] = []

    def begin(
        self,
        model: type[BaseModel],
        *,
        table: str,
        primary_key: str,
        on_conflict: str = "error",
    ) -> None:
        self._columns = list(model.model_fields.keys())
        self.fs_path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = self.fs_path.open("w", encoding=self.encoding, newline="")
        self._writer = _csv.DictWriter(self._fp, fieldnames=self._columns, delimiter=self.delimiter)
        self._writer.writeheader()

    def write(self, rows: Iterable[BaseModel]) -> int:
        if self._writer is None:
            raise RuntimeError("call begin() before write()")
        count = 0
        for row in rows:
            self._writer.writerow(row.model_dump())
            count += 1
        return count

    def write_manifest(self, manifest: RunManifest) -> None:
        return

    def commit(self) -> None:
        if self._fp is not None:
            self._fp.flush()

    def close(self) -> None:
        if self._fp is not None:
            self._fp.close()
            self._fp = None
            self._writer = None
