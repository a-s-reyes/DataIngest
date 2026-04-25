import contextlib
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import IO, Any, cast

from pydantic import BaseModel, ValidationError, create_model

from . import __version__
from .cleaners import chain as build_cleaner
from .config import FieldType, Mapping
from .errors import JsonlErrorLog, RowError
from .manifest import RunManifest, derive_status, now_iso
from .sinks import get as get_sink
from .sources import get as get_source
from .uri import parse as parse_uri

_TYPE_MAP: dict[FieldType, type] = {
    "str": str,
    "int": int,
    "decimal": Decimal,
    "date": date,
    "datetime": datetime,
    "bool": bool,
}


def _build_row_model(mapping: Mapping) -> type[BaseModel]:
    """Build a Pydantic row model dynamically from the YAML field declarations."""
    fields: dict[str, tuple[Any, Any]] = {}
    for name, fc in mapping.fields.items():
        py_type: Any = _TYPE_MAP[fc.type]
        if fc.required:
            fields[name] = (py_type, ...)
        else:
            fields[name] = (py_type | None, fc.default)
    safe_name = mapping.name.replace("-", "_") + "_Row"
    return cast(type[BaseModel], create_model(safe_name, **fields))  # type: ignore[call-overload]


class _CleanerError(Exception):
    """Raised internally when a cleaner chain raises on a row's field.

    Carries the offending field, raw value, and underlying message so the
    pipeline can route the row to the JSONL error log without losing context.
    """

    def __init__(self, field: str, value: Any, message: str) -> None:
        super().__init__(message)
        self.field = field
        self.value = value
        self.message = message


DEFAULT_CHUNK_SIZE = 1000


@dataclass
class RunResult:
    rows_in: int = 0
    rows_ok: int = 0
    rows_failed: int = 0
    chunks_written: int = 0
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))


class Pipeline:
    """Orchestrates source → mapping → validation → sink.

    Validated rows are flushed to the sink in batches of ``chunk_size`` so that
    peak memory stays bounded regardless of input size.
    """

    def __init__(
        self,
        source_uri: str,
        sink_uri: str,
        mapping: Mapping,
        *,
        dry_run: bool = False,
        limit: int | None = None,
        error_log: Path | IO[str] | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        if chunk_size < 1:
            raise ValueError(f"chunk_size must be >= 1, got {chunk_size}")
        self.source_uri = source_uri
        self.sink_uri = sink_uri
        self.mapping = mapping
        self.dry_run = dry_run
        self.limit = limit
        self.error_log = error_log
        self.chunk_size = chunk_size

    def run(self) -> RunResult:
        log = logging.getLogger(__name__)
        src_parsed = parse_uri(self.source_uri)
        sink_parsed = parse_uri(self.sink_uri)

        source_cls = get_source(src_parsed.scheme)
        sink_cls = get_sink(sink_parsed.scheme)

        source = source_cls(src_parsed.path, src_parsed.params)
        sink = sink_cls(sink_parsed.path, sink_parsed.params)

        cleaners: dict[str, Callable[[Any], Any]] = {
            name: build_cleaner(fc.cleaners) for name, fc in self.mapping.fields.items()
        }
        row_model = _build_row_model(self.mapping)

        err_target: Path | IO[str] = self.error_log or Path.cwd() / "errors.jsonl"
        # Path used for the manifest's error_log_path field. For file-like
        # targets (e.g. sys.stderr) we record the stream name.
        err_path_for_manifest = (
            str(err_target)
            if isinstance(err_target, Path)
            else getattr(err_target, "name", "<stream>")
        )
        result = RunResult()
        started_at = now_iso()
        batch: list[BaseModel] = []
        errored = False
        log.info(
            "pipeline starting run_id=%s mapping=%s source=%s sink=%s dry_run=%s",
            result.run_id,
            self.mapping.name,
            self.source_uri,
            self.sink_uri,
            self.dry_run,
        )

        if not self.dry_run:
            sink.begin(
                row_model,
                table=self.mapping.target.table,
                primary_key=self.mapping.target.primary_key,
                on_conflict=self.mapping.target.on_conflict,
            )

        def flush() -> None:
            if self.dry_run or not batch:
                return
            sink.write(batch)
            result.chunks_written += 1
            log.debug(
                "flushed chunk %d (rows_ok=%d, rows_failed=%d)",
                result.chunks_written,
                result.rows_ok,
                result.rows_failed,
            )
            batch.clear()

        try:
            with JsonlErrorLog(err_target) as err_log:
                for i, raw in enumerate(source.rows(), start=1):
                    if self.limit is not None and result.rows_in >= self.limit:
                        break
                    result.rows_in += 1

                    try:
                        cleaned = self._apply_mapping(raw, cleaners)
                    except _CleanerError as cf:
                        err_log.write(
                            RowError(
                                row_number=i,
                                source_file=src_parsed.path,
                                field=cf.field,
                                value=cf.value,
                                rule="cleaner",
                                message=cf.message,
                            )
                        )
                        result.rows_failed += 1
                        continue

                    try:
                        batch.append(row_model(**cleaned))
                        result.rows_ok += 1
                    except ValidationError as exc:
                        for err in exc.errors():
                            loc = err["loc"]
                            field_name = str(loc[0]) if loc else None
                            err_log.write(
                                RowError(
                                    row_number=i,
                                    source_file=src_parsed.path,
                                    field=".".join(str(p) for p in loc) or None,
                                    value=cleaned.get(field_name) if field_name else None,
                                    rule=str(err["type"]),
                                    message=str(err["msg"]),
                                )
                            )
                        result.rows_failed += 1
                        continue

                    if len(batch) >= self.chunk_size:
                        flush()

            flush()
            if not self.dry_run:
                sink.commit()
        except Exception:
            errored = True
            raise
        finally:
            source.close()
            if not self.dry_run:
                # Best-effort manifest write — never mask the original exception.
                with contextlib.suppress(Exception):
                    sink.write_manifest(
                        RunManifest(
                            run_id=result.run_id,
                            started_at=started_at,
                            finished_at=now_iso(),
                            mapping_name=self.mapping.name,
                            source_uri=self.source_uri,
                            target_table=self.mapping.target.table,
                            rows_in=result.rows_in,
                            rows_ok=result.rows_ok,
                            rows_failed=result.rows_failed,
                            chunks_written=result.chunks_written,
                            error_log_path=err_path_for_manifest,
                            dataingest_version=__version__,
                            dry_run=False,
                            status=derive_status(result.rows_in, result.rows_ok, errored),
                        )
                    )
                sink.close()

        log.info(
            "pipeline finished run_id=%s rows_in=%d rows_ok=%d rows_failed=%d chunks=%d",
            result.run_id,
            result.rows_in,
            result.rows_ok,
            result.rows_failed,
            result.chunks_written,
        )
        return result

    def _apply_mapping(
        self,
        raw: dict[str, Any],
        cleaners: dict[str, Callable[[Any], Any]],
    ) -> dict[str, Any]:
        cleaned: dict[str, Any] = {}
        for name, fc in self.mapping.fields.items():
            key = str(fc.column)
            raw_value = raw.get(key)
            try:
                cleaned[name] = cleaners[name](raw_value)
            except Exception as exc:
                raise _CleanerError(name, raw_value, str(exc)) from exc
        return cleaned
