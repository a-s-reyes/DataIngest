"""PostgreSQL sink.

Lazy-imports the ``psycopg`` driver so users without postgres pay zero deps.
Install the optional dep with ``uv sync --extra postgres`` (or ``--all-extras``
in CI). Registered for both ``postgres://`` and ``postgresql://`` URI schemes.

Conflict modes:
    error    raises on duplicate primary key
    skip     ``INSERT ... ON CONFLICT DO NOTHING``
    replace  ``INSERT ... ON CONFLICT DO UPDATE SET ...`` — every non-PK
             column is overwritten with the incoming value (full upsert).
"""

from typing import Any

from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

from . import register
from ._base import _BaseSqlSink


@register("postgres")
@register("postgresql")
class PostgresSink(_BaseSqlSink):
    """Writes rows to a PostgreSQL database via SQLAlchemy 2.0 Core."""

    SUPPORTED_CONFLICT_MODES = ("error", "skip", "replace")

    def _make_url(self) -> str:
        # ``self.path`` is ``netloc + path`` from the URI parser:
        # postgres://user:pass@host:port/db -> "user:pass@host:port/db"
        # SQLAlchemy needs a ``postgresql+psycopg://`` URL with the driver
        # name, so we re-prefix here.
        try:
            import psycopg  # noqa: F401  -- presence check
        except ImportError as err:  # pragma: no cover - exercised only when extra missing
            raise ImportError(
                "postgres:// sink requires psycopg. Install with: uv sync --extra postgres"
            ) from err
        return f"postgresql+psycopg://{self.path}"

    def _make_insert_stmt(self, table: Table) -> Any:
        stmt = pg_insert(table)
        if self.on_conflict == "skip":
            return stmt.on_conflict_do_nothing(index_elements=[self.primary_key])
        if self.on_conflict == "replace":
            # Update every non-PK column with the incoming row's value.
            update_cols = {
                c.name: stmt.excluded[c.name] for c in table.columns if c.name != self.primary_key
            }
            return stmt.on_conflict_do_update(
                index_elements=[self.primary_key],
                set_=update_cols,
            )
        return stmt
