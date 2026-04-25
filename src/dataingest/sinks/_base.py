"""Shared SQL-sink machinery: table creation, bulk insert, manifest write.

Concrete sinks (`SqliteSink`, `PostgresSink`, future `MssqlSink`) inherit from
``_BaseSqlSink`` and override only the two dialect-specific bits:

  * ``_make_url()`` — turn the parsed URI back into a SQLAlchemy URL
  * ``_make_insert_stmt()`` — build the dialect-aware insert with the
    appropriate ``on_conflict`` semantics

Everything else — Pydantic-model → SQLAlchemy-table introspection, the
manifest table schema, transaction management, type unwrapping — lives here.
"""

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
from sqlalchemy.engine import Engine

from ..manifest import MANIFEST_TABLE_NAME, RunManifest

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


class _BaseSqlSink:
    """Common SQL sink behavior. Subclasses override URL + insert builder."""

    # Override in subclasses to widen.
    SUPPORTED_CONFLICT_MODES: tuple[str, ...] = ("error", "skip")

    def __init__(self, path: str, params: dict[str, str]) -> None:
        self.path = path
        self.params = params
        self.engine: Engine | None = None
        self.table: Table | None = None
        self.primary_key: str = ""
        self.on_conflict: str = "error"

    # --- Hooks subclasses MUST implement ---

    def _make_url(self) -> str:
        raise NotImplementedError

    def _make_insert_stmt(self, table: Table) -> Any:
        """Build an executable INSERT statement honoring ``self.on_conflict``."""
        raise NotImplementedError

    # --- Shared protocol surface ---

    def begin(
        self,
        model: type[BaseModel],
        *,
        table: str,
        primary_key: str,
        on_conflict: str = "error",
    ) -> None:
        if on_conflict not in self.SUPPORTED_CONFLICT_MODES:
            cls_name = type(self).__name__
            raise ValueError(
                f"on_conflict={on_conflict!r} not supported by {cls_name} "
                f"(supports: {', '.join(self.SUPPORTED_CONFLICT_MODES)})"
            )
        self.engine = create_engine(self._make_url(), future=True)
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
        stmt = self._make_insert_stmt(self.table)
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
        # engine.begin() commits on context exit; explicit commit is a no-op.
        pass

    def close(self) -> None:
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
            self.table = None
