from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

MANIFEST_TABLE_NAME = "_dataingest_runs"

ManifestStatus = Literal["success", "partial", "failed"]


@dataclass
class RunManifest:
    run_id: str
    started_at: str
    finished_at: str
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
    return datetime.now(UTC).isoformat()


def derive_status(rows_in: int, rows_ok: int, errored: bool) -> ManifestStatus:
    if errored:
        return "failed"
    if rows_in == 0:
        return "success"
    if rows_ok == rows_in:
        return "success"
    if rows_ok == 0:
        return "failed"
    return "partial"
