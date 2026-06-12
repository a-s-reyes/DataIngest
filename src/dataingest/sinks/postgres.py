from typing import Any

from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

from . import register
from ._base import _BaseSqlSink


@register("postgres")
@register("postgresql")
class PostgresSink(_BaseSqlSink):
    SUPPORTED_CONFLICT_MODES = ("error", "skip", "replace")

    def _make_url(self) -> str:
        try:
            import psycopg  # noqa: F401
        except ImportError as err:  # pragma: no cover
            raise ImportError(
                "postgres:// sink requires psycopg. Install with: uv sync --extra postgres"
            ) from err
        return f"postgresql+psycopg://{self.path}"

    def _make_insert_stmt(self, table: Table) -> Any:
        stmt = pg_insert(table)
        if self.on_conflict == "skip":
            return stmt.on_conflict_do_nothing(index_elements=[self.primary_key])
        if self.on_conflict == "replace":
            update_cols = {
                c.name: stmt.excluded[c.name] for c in table.columns if c.name != self.primary_key
            }
            return stmt.on_conflict_do_update(
                index_elements=[self.primary_key],
                set_=update_cols,
            )
        return stmt
