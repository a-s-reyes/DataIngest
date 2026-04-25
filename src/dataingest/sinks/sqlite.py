import re
import types
import typing
from collections.abc import Iterable
from datetime import date, datetime
from decimal import Decimal

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

from . import register

_TYPE_TO_SQLA: dict[type, type] = {
    str: String,
    int: Integer,
    Decimal: Numeric,
    date: Date,
    datetime: DateTime,
    bool: Boolean,
}


def _unwrap_optional(annotation: object) -> object:
    """Reduce ``X | None`` / ``Optional[X]`` to ``X``."""
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _resolve_path(uri_path: str) -> str:
    """Turn a URI path into a SQLAlchemy-friendly absolute or relative path.

    ``/./out.db`` (relative) → ``./out.db``
    ``//tmp/out.db`` (POSIX absolute) → ``/tmp/out.db``
    ``/C:/data/out.db`` (Windows absolute) → ``C:/data/out.db``
    """
    if re.match(r"^/[A-Za-z]:[/\\]", uri_path):
        return uri_path[1:]
    if uri_path.startswith("/./"):
        return uri_path[1:]
    if uri_path.startswith("//"):
        return uri_path[1:]
    return uri_path


@register("sqlite")
class SqliteSink:
    """Writes rows to a SQLite database via SQLAlchemy 2.0 Core.

    Builds the target table from the row model on ``begin()``, then bulk-inserts
    on ``write()``. Honors ``on_conflict`` (``error`` | ``skip``).
    """

    def __init__(self, path: str, params: dict[str, str]) -> None:
        self.fs_path = _resolve_path(path)
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
        cols: list[Column] = []
        for fname, finfo in model.model_fields.items():
            py_type = _unwrap_optional(finfo.annotation)
            sqla_cls = _TYPE_TO_SQLA.get(py_type, String)  # type: ignore[arg-type]
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

    def commit(self) -> None:
        # engine.begin() commits on context exit; this is a no-op for SQLite.
        pass

    def close(self) -> None:
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
            self.table = None
