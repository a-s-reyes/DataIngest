"""Run manifest types and helpers.

Each pipeline invocation writes a single row to the sink's ``_dataingest_runs``
table — the audit trail. ``RunManifest`` is the data carrier; sinks know how to
persist it.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

MANIFEST_TABLE_NAME = "_dataingest_runs"

ManifestStatus = Literal["success", "partial", "failed"]


@dataclass
class RunManifest:
    """Audit-trail record for one pipeline invocation."""

    run_id: str
    started_at: str  # ISO 8601 UTC
    finished_at: str  # ISO 8601 UTC
    mapping_name: str
    source_uri: str
    target_table: str
    rows_in: int
    rows_ok: int
    rows_failed: int
    chunks_written: int
    error_log_path: str | None
    dataingest_version: str
    dry_run: bool
    status: ManifestStatus


def now_iso() -> str:
    """Current UTC time as an ISO 8601 string."""
    return datetime.now(UTC).isoformat()


def derive_status(rows_in: int, rows_ok: int, errored: bool) -> ManifestStatus:
    """Compute the manifest status from final pipeline counters."""
    if errored:
        return "failed"
    if rows_in == 0:
        return "success"  # vacuous success: nothing to ingest
    if rows_ok == rows_in:
        return "success"
    if rows_ok == 0:
        return "failed"
    return "partial"
