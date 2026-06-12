from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel

from ..manifest import RunManifest


class MultiSink:
    def __init__(self, sinks: list[Any]) -> None:
        self._sinks = sinks

    def begin(
        self,
        model: type[BaseModel],
        *,
        table: str,
        primary_key: str,
        on_conflict: str = "error",
    ) -> None:
        for sink in self._sinks:
            sink.begin(model, table=table, primary_key=primary_key, on_conflict=on_conflict)

    def write(self, rows: Iterable[BaseModel]) -> int:
        materialized = list(rows)
        count = 0
        for sink in self._sinks:
            count = sink.write(materialized)
        return count

    def write_manifest(self, manifest: RunManifest) -> None:
        for sink in self._sinks:
            sink.write_manifest(manifest)

    def commit(self) -> None:
        for sink in self._sinks:
            sink.commit()

    def close(self) -> None:
        for sink in self._sinks:
            sink.close()
