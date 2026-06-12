import contextlib
import logging
import uuid
from collections.abc import Callable, Iterator
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
from .sinks._base import _BaseSqlSink
from .sinks._multi import MultiSink
from .sinks.relational import ChildField, ChildSpec, RelationalRow
from .sources import get as get_source
from .transforms import Transform
from .transforms import build as build_transforms
from .uri import parse as parse_uri

_TYPE_MAP: dict[FieldType, type] = {
    "str": str,
    "int": int,
    "decimal": Decimal,
    "date": date,
    "datetime": datetime,
    "bool": bool,
}


def _literal_py_type(value: Any) -> type:
    if isinstance(value, bool):
        return bool
    if isinstance(value, int):
        return int
    if isinstance(value, float | Decimal):
        return Decimal
    return str


_GROUP_SENTINEL: Any = object()


def _grouped(
    stream: Iterator[tuple[int, dict[str, Any]]],
    key_field: str | None,
) -> Iterator[list[tuple[int, dict[str, Any]]]]:
    if key_field is None:
        for item in stream:
            yield [item]
        return
    group: list[tuple[int, dict[str, Any]]] = []
    current_key: Any = _GROUP_SENTINEL
    for index, cleaned in stream:
        key = cleaned.get(key_field)
        if group and key != current_key:
            yield group
            group = []
        group.append((index, cleaned))
        current_key = key
    if group:
        yield group


def _build_row_model(mapping: Mapping) -> type[BaseModel]:
    fields: dict[str, tuple[Any, Any]] = {}
    for name, fc in mapping.fields.items():
        if fc.transient:
            continue
        py_type: Any = _TYPE_MAP[fc.type]
        if fc.required:
            fields[name] = (py_type, ...)
        else:
            fields[name] = (py_type | None, fc.default)
    safe_name = mapping.name.replace("-", "_") + "_Row"
    return cast(type[BaseModel], create_model(safe_name, **fields))  # type: ignore[call-overload]


class _CleanerError(Exception):
    def __init__(self, field: str, value: Any, message: str) -> None:
        super().__init__(message)
        self.field = field
        self.value = value
        self.message = message


class _TransformError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
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
    def __init__(
        self,
        source_uri: str,
        sink_uri: str | list[str],
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

    def _merge_source_params(self, uri_params: dict[str, str]) -> dict[str, str]:
        sc = self.mapping.source
        merged: dict[str, str] = {}
        if "encoding" in sc.model_fields_set:
            merged["encoding"] = sc.encoding
        if "delimiter" in sc.model_fields_set:
            merged["delimiter"] = sc.delimiter
        if "header" in sc.model_fields_set:
            merged["header"] = "true" if sc.header else "false"
        merged.update(uri_params)
        return merged

    def _build_sink(self) -> Any:
        uris = [self.sink_uri] if isinstance(self.sink_uri, str) else list(self.sink_uri)
        sinks: list[Any] = []
        for uri in uris:
            parsed = parse_uri(uri)
            sinks.append(get_sink(parsed.scheme)(parsed.path, parsed.params))
        return sinks[0] if len(sinks) == 1 else MultiSink(sinks)

    def _build_child_specs(self) -> list[ChildSpec]:
        pk = self.mapping.target.primary_key
        fk_type = _TYPE_MAP[self.mapping.fields[pk].type]
        specs: list[ChildSpec] = []
        for child in self.mapping.children:
            fields: dict[str, ChildField] = {}
            for col, cf in child.fields.items():
                if cf.from_ is not None:
                    fields[col] = ChildField(
                        "from", cf.from_, _TYPE_MAP[self.mapping.fields[cf.from_].type]
                    )
                else:
                    fields[col] = ChildField("value", cf.value, _literal_py_type(cf.value))
            specs.append(
                ChildSpec(child.table, child.foreign_key, fk_type, fields, child.for_each_row)
            )
        return specs

    def run(self) -> RunResult:
        log = logging.getLogger(__name__)
        src_parsed = parse_uri(self.source_uri)
        source_cls = get_source(src_parsed.scheme)
        source = source_cls(src_parsed.path, self._merge_source_params(src_parsed.params))
        sink = self._build_sink()

        cleaners: dict[str, Callable[[Any], Any]] = {
            name: build_cleaner(fc.cleaners) for name, fc in self.mapping.fields.items()
        }
        row_model = _build_row_model(self.mapping)
        transforms = build_transforms(self.mapping.transforms)
        transient = {name for name, fc in self.mapping.fields.items() if fc.transient}
        child_specs = self._build_child_specs()
        if child_specs and (isinstance(sink, MultiSink) or not isinstance(sink, _BaseSqlSink)):
            raise ValueError("relational mappings (children) require a single SQL sink")

        err_target: Path | IO[str] = self.error_log or Path.cwd() / "errors.jsonl"
        err_path_for_manifest = (
            str(err_target)
            if isinstance(err_target, Path)
            else getattr(err_target, "name", "<stream>")
        )
        result = RunResult()
        started_at = now_iso()
        batch: list[Any] = []
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
                children=child_specs or None,
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

                def cleaned_stream() -> Iterator[tuple[int, dict[str, Any]]]:
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
                            cleaned = self._apply_transforms(cleaned, transforms)
                        except _TransformError as tf:
                            err_log.write(
                                RowError(
                                    row_number=i,
                                    source_file=src_parsed.path,
                                    field=None,
                                    value=None,
                                    rule="transform",
                                    message=tf.message,
                                )
                            )
                            result.rows_failed += 1
                            continue
                        yield i, cleaned

                for group in _grouped(cleaned_stream(), self.mapping.source.group_by):
                    first_i, parent_cleaned = group[0]
                    record = (
                        {k: v for k, v in parent_cleaned.items() if k not in transient}
                        if transient
                        else parent_cleaned
                    )

                    try:
                        parent_model = row_model(**record)
                    except ValidationError as exc:
                        for err in exc.errors():
                            loc = err["loc"]
                            field_name = str(loc[0]) if loc else None
                            err_log.write(
                                RowError(
                                    row_number=first_i,
                                    source_file=src_parsed.path,
                                    field=".".join(str(p) for p in loc) or None,
                                    value=record.get(field_name) if field_name else None,
                                    rule=str(err["type"]),
                                    message=str(err["msg"]),
                                )
                            )
                        result.rows_failed += len(group)
                        continue

                    if child_specs:
                        pk_value = parent_cleaned[self.mapping.target.primary_key]
                        children_rows: dict[str, list[dict[str, Any]]] = {}
                        for spec in child_specs:
                            members = group if spec.for_each_row else group[:1]
                            for _, member in members:
                                child_row: dict[str, Any] = {spec.foreign_key: pk_value}
                                for col, cfield in spec.fields.items():
                                    if cfield.kind == "from":
                                        child_row[col] = member[cfield.ref]
                                    else:
                                        child_row[col] = cfield.ref
                                children_rows.setdefault(spec.table, []).append(child_row)
                        batch.append(RelationalRow(parent_model, children_rows))
                    else:
                        batch.append(parent_model)
                    result.rows_ok += len(group)

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
            if fc.column is None:
                continue
            key = str(fc.column)
            raw_value = raw.get(key)
            try:
                cleaned[name] = cleaners[name](raw_value)
            except Exception as exc:
                raise _CleanerError(name, raw_value, str(exc)) from exc
        return cleaned

    def _apply_transforms(
        self,
        record: dict[str, Any],
        transforms: list[Transform],
    ) -> dict[str, Any]:
        for transform in transforms:
            try:
                record = transform.apply(record)
            except Exception as exc:
                raise _TransformError(str(exc)) from exc
        return record
