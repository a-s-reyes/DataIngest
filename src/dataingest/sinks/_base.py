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
from .relational import ChildSpec, RelationalRow

_TYPE_TO_SQLA: dict[type, type] = {
    str: String,
    int: Integer,
    Decimal: Numeric,
    date: Date,
    datetime: DateTime,
    bool: Boolean,
}


def _unwrap_optional(annotation: Any) -> Any:
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


class _BaseSqlSink:
    SUPPORTED_CONFLICT_MODES: tuple[str, ...] = ("error", "skip")

    def __init__(self, path: str, params: dict[str, str]) -> None:
        self.path = path
        self.params = params
        self.engine: Engine | None = None
        self.table: Table | None = None
        self.primary_key: str = ""
        self.on_conflict: str = "error"
        self._children: list[ChildSpec] = []
        self._child_tables: dict[str, Table] = {}

    def _make_url(self) -> str:
        raise NotImplementedError

    def _make_insert_stmt(self, table: Table) -> Any:
        raise NotImplementedError

    def begin(
        self,
        model: type[BaseModel],
        *,
        table: str,
        primary_key: str,
        on_conflict: str = "error",
        children: list[Any] | None = None,
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

        self._children = children or []
        self._child_tables = {}
        for spec in self._children:
            if spec.table not in self._child_tables:
                self._child_tables[spec.table] = self._build_child_table(spec, metadata)

        metadata.create_all(self.engine)

    def write(self, rows: Iterable[Any]) -> int:
        if self.engine is None or self.table is None:
            raise RuntimeError("call begin() before write()")
        items = list(rows)
        if not items:
            return 0
        if isinstance(items[0], RelationalRow):
            return self._write_relational(items)
        payload = [r.model_dump() for r in items]
        stmt = self._make_insert_stmt(self.table)
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)
        return len(payload)

    def _write_relational(self, records: list[RelationalRow]) -> int:
        assert self.table is not None and self.engine is not None
        parent_stmt = self._make_insert_stmt(self.table)
        with self.engine.begin() as conn:
            for rec in records:
                result = conn.execute(parent_stmt, rec.parent.model_dump())
                if self.on_conflict == "skip" and (result.rowcount or 0) == 0:
                    continue
                for table_name, child_rows in rec.children.items():
                    if child_rows:
                        conn.execute(self._child_tables[table_name].insert(), child_rows)
        return len(records)

    def _build_child_table(self, spec: ChildSpec, metadata: MetaData) -> Table:
        cols: list[Column[Any]] = [
            Column(spec.foreign_key, _TYPE_TO_SQLA.get(spec.fk_type, String)(), nullable=False)
        ]
        for col, cf in spec.fields.items():
            cols.append(Column(col, _TYPE_TO_SQLA.get(cf.py_type, String)(), nullable=True))
        return Table(spec.table, metadata, *cols)

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
        pass

    def close(self) -> None:
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
            self.table = None
