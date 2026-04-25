"""SQLite sink — dialect-specific glue around ``_BaseSqlSink``."""

from typing import Any

from sqlalchemy import Table
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ..uri import resolve_uri_path
from . import register
from ._base import _BaseSqlSink


@register("sqlite")
class SqliteSink(_BaseSqlSink):
    """Writes rows to a SQLite database via SQLAlchemy 2.0 Core.

    Supports ``error`` and ``skip`` conflict modes. ``replace`` is deferred
    (sqlite has ``ON CONFLICT DO UPDATE`` but we haven't built the column
    map yet — see roadmap).
    """

    SUPPORTED_CONFLICT_MODES = ("error", "skip")

    def __init__(self, path: str, params: dict[str, str]) -> None:
        super().__init__(path, params)
        self.fs_path = resolve_uri_path(path)

    def _make_url(self) -> str:
        return f"sqlite:///{self.fs_path}"

    def _make_insert_stmt(self, table: Table) -> Any:
        if self.on_conflict == "skip":
            return sqlite_insert(table).on_conflict_do_nothing(index_elements=[self.primary_key])
        return sqlite_insert(table)
