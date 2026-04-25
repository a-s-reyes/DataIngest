import types
import typing
from collections.abc import Iterable
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

from ..manifest import MANIFEST_TABLE_NAME, RunManifest
from ..uri import resolve_uri_path
from . import register

_TYPE_TO_SQLA: dict[type, type] = {
    str: String,
    int: Integer,
    Decimal: Numeric,
    date: Date,
    datetime: DateTime,
    bool: Boolean,
}


def _unwrap_optional(annotation: Any) -> Any:
    """Reduce ``X | None`` / ``Optional[X]`` to ``X``."""
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


@register("sqlite")
class SqliteSink:
    """Writes rows to a SQLite database via SQLAlchemy 2.0 Core.

    Builds the target table from the row model on ``begin()``, then bulk-inserts
    on ``write()``. Honors ``on_conflict`` (``error`` | ``skip``).
    """

    def __init__(self, path: str, params: dict[str, str]) -> None:
        self.fs_path = resolve_uri_path(path)
        self.params = params
        self.engine: Engine | None = None
        self.table: Table | None = None
        self.primary_key: str = ""
        self.on_conflict: str = "error"

    def _url(self) -> str:
        return f"sqlite:///{self.fs_path}"

    def begin(
        self,
        model: type[BaseModel],
        *,
        table: str,
        primary_key: str,
        on_conflict: str = "error",
    ) -> None:
        if on_conflict not in ("error", "skip"):
            raise ValueError(
                f"on_conflict={on_conflict!r} not supported by SqliteSink (v1 supports: error, skip)"
            )
        self.engine = create_engine(self._url(), future=True)
        self.primary_key = primary_key
        self.on_conflict = on_conflict

        metadata = MetaData()
        cols: list[Column[Any]] = []
        for fname, finfo in model.model_fields.items():
            py_type = _unwrap_optional(finfo.annotation)
            sqla_cls = _TYPE_TO_SQLA.get(py_type, String)
            is_pk = fname == primary_key
            nullable = (not finfo.is_required()) and not is_pk
            cols.append(Column(fname, sqla_cls(), primary_key=is_pk, nullable=nullable))
        self.table = Table(table, metadata, *cols)
        metadata.create_all(self.engine)

    def write(self, rows: Iterable[BaseModel]) -> int:
        if self.engine is None or self.table is None:
            raise RuntimeError("call begin() before write()")
        payload = [r.model_dump() for r in rows]
        if not payload:
            return 0

        if self.on_conflict == "skip":
            stmt = sqlite_insert(self.table).on_conflict_do_nothing(
                index_elements=[self.primary_key]
            )
        else:
            stmt = sqlite_insert(self.table)

        with self.engine.begin() as conn:
            conn.execute(stmt, payload)
        return len(payload)

    def write_manifest(self, manifest: RunManifest) -> None:
        if self.engine is None:
            raise RuntimeError("call begin() before write_manifest()")
        metadata = MetaData()
        manifest_table = Table(
            MANIFEST_TABLE_NAME,
            metadata,
            Column("run_id", String, primary_key=True),
            Column("started_at", String, nullable=False),
            Column("finished_at", String, nullable=False),
            Column("mapping_name", String, nullable=False),
            Column("source_uri", String, nullable=False),
            Column("target_table", String, nullable=False),
            Column("rows_in", Integer, nullable=False),
            Column("rows_ok", Integer, nullable=False),
            Column("rows_failed", Integer, nullable=False),
            Column("chunks_written", Integer, nullable=False),
            Column("error_log_path", String, nullable=True),
            Column("dataingest_version", String, nullable=False),
            Column("dry_run", Boolean, nullable=False),
            Column("status", String, nullable=False),
        )
        metadata.create_all(self.engine)
        with self.engine.begin() as conn:
            conn.execute(
                manifest_table.insert(),
                {
                    "run_id": manifest.run_id,
                    "started_at": manifest.started_at,
                    "finished_at": manifest.finished_at,
                    "mapping_name": manifest.mapping_name,
                    "source_uri": manifest.source_uri,
                    "target_table": manifest.target_table,
                    "rows_in": manifest.rows_in,
                    "rows_ok": manifest.rows_ok,
                    "rows_failed": manifest.rows_failed,
                    "chunks_written": manifest.chunks_written,
                    "error_log_path": manifest.error_log_path,
                    "dataingest_version": manifest.dataingest_version,
                    "dry_run": manifest.dry_run,
                    "status": manifest.status,
                },
            )

    def commit(self) -> None:
        # engine.begin() commits on context exit; this is a no-op for SQLite.
        pass

    def close(self) -> None:
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
            self.table = None
