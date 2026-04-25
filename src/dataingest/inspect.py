"""Sink inspection — power for the ``dataingest tables`` CLI command.

Lists user tables with their row counts and the most recent entries from the
``_dataingest_runs`` audit table. Works against any sink registered in the
``sinks.REGISTRY`` because URL construction is delegated to the sink class.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import MetaData, create_engine, text

from .manifest import MANIFEST_TABLE_NAME
from .sinks import get as get_sink
from .uri import parse as parse_uri


@dataclass(frozen=True)
class TableInfo:
    name: str
    row_count: int


@dataclass(frozen=True)
class RunInfo:
    run_id: str
    started_at: str
    mapping_name: str
    rows_in: int
    rows_ok: int
    rows_failed: int
    status: str


@dataclass(frozen=True)
class SinkInspection:
    url: str
    tables: tuple[TableInfo, ...]
    recent_runs: tuple[RunInfo, ...]


def inspect_sink(sink_uri: str, *, recent_runs: int = 5) -> SinkInspection:
    """Connect to ``sink_uri``, enumerate tables + last ``recent_runs`` manifest rows."""
    parsed = parse_uri(sink_uri)
    sink_cls = get_sink(parsed.scheme)
    sink_inst = sink_cls(parsed.path, parsed.params)
    url = sink_inst._make_url()

    engine = create_engine(url, future=True)
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        tables: list[TableInfo] = []
        runs: list[RunInfo] = []
        with engine.connect() as conn:
            for table_name in sorted(metadata.tables):
                count_q = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()
                tables.append(TableInfo(name=table_name, row_count=int(count_q or 0)))

            if MANIFEST_TABLE_NAME in metadata.tables:
                rs = conn.execute(
                    text(
                        f"SELECT run_id, started_at, mapping_name, rows_in, "
                        f"rows_ok, rows_failed, status "
                        f"FROM {MANIFEST_TABLE_NAME} "
                        f"ORDER BY started_at DESC LIMIT :n"
                    ),
                    {"n": recent_runs},
                )
                for row in rs:
                    runs.append(
                        RunInfo(
                            run_id=str(row.run_id),
                            started_at=str(row.started_at),
                            mapping_name=str(row.mapping_name),
                            rows_in=int(row.rows_in),
                            rows_ok=int(row.rows_ok),
                            rows_failed=int(row.rows_failed),
                            status=str(row.status),
                        )
                    )
    finally:
        engine.dispose()

    return SinkInspection(url=url, tables=tuple(tables), recent_runs=tuple(runs))


def render_inspection(insp: SinkInspection) -> str:
    """Format a :class:`SinkInspection` as a human-readable ASCII report."""
    lines: list[str] = [f"sink: {insp.url}", ""]
    if not insp.tables:
        lines.append("  (no tables)")
    else:
        name_w = max(5, max(len(t.name) for t in insp.tables))
        rows_w = max(4, max(len(str(t.row_count)) for t in insp.tables))
        lines.append(f"  {'TABLE'.ljust(name_w)}  {'ROWS'.rjust(rows_w)}")
        lines.append(f"  {'-' * name_w}  {'-' * rows_w}")
        for t in insp.tables:
            lines.append(f"  {t.name.ljust(name_w)}  {str(t.row_count).rjust(rows_w)}")

    if insp.recent_runs:
        lines.append("")
        lines.append(f"recent runs (last {len(insp.recent_runs)}):")
        for r in insp.recent_runs:
            lines.append(
                f"  {r.started_at}  {r.run_id[:8]}  "
                f"{r.status:<8}  in={r.rows_in} ok={r.rows_ok} failed={r.rows_failed}  "
                f"({r.mapping_name})"
            )

    return "\n".join(lines)
