from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel, ValidationError, create_model

from .cleaners import chain as build_cleaner
from .config import FieldType, Mapping
from .errors import JsonlErrorLog, RowError
from .sinks import get as get_sink
from .sources import get as get_source
from .uri import parse as parse_uri

_TYPE_MAP: dict[FieldType, type] = {
    "str": str,
    "int": int,
    "decimal": Decimal,
    "date": date,
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
    return cast(type[BaseModel], create_model(safe_name, **fields))


@dataclass
class RunResult:
    rows_in: int = 0
    rows_ok: int = 0
    rows_failed: int = 0


class Pipeline:
    """Orchestrates source → mapping → validation → sink."""

    def __init__(
        self,
        source_uri: str,
        sink_uri: str,
        mapping: Mapping,
        *,
        dry_run: bool = False,
        limit: int | None = None,
        error_log: Path | None = None,
    ) -> None:
        self.source_uri = source_uri
        self.sink_uri = sink_uri
        self.mapping = mapping
        self.dry_run = dry_run
        self.limit = limit
        self.error_log = error_log

    def run(self) -> RunResult:
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

        err_path = self.error_log or Path.cwd() / "errors.jsonl"
        result = RunResult()
        validated: list[BaseModel] = []

        if not self.dry_run:
            sink.begin(
                row_model,
                table=self.mapping.target.table,
                primary_key=self.mapping.target.primary_key,
                on_conflict=self.mapping.target.on_conflict,
            )

        try:
            with JsonlErrorLog(err_path) as err_log:
                for i, raw in enumerate(source.rows(), start=1):
                    if self.limit is not None and result.rows_in >= self.limit:
                        break
                    result.rows_in += 1

                    cleaned, cleaner_err = self._apply_mapping(raw, cleaners)
                    if cleaner_err is not None:
                        err_log.write(
                            RowError(
                                row_number=i,
                                source_file=src_parsed.path,
                                field=cleaner_err["field"],
                                value=cleaner_err["value"],
                                rule="cleaner",
                                message=cleaner_err["message"],
                            )
                        )
                        result.rows_failed += 1
                        continue

                    try:
                        validated.append(row_model(**cleaned))
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

            if not self.dry_run and validated:
                sink.write(validated)
                sink.commit()
        finally:
            source.close()
            if not self.dry_run:
                sink.close()

        return result

    def _apply_mapping(
        self,
        raw: dict[str, Any],
        cleaners: dict[str, Callable[[Any], Any]],
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        cleaned: dict[str, Any] = {}
        for name, fc in self.mapping.fields.items():
            key = str(fc.column)
            raw_value = raw.get(key)
            try:
                cleaned[name] = cleaners[name](raw_value)
            except Exception as exc:
                return None, {
                    "field": name,
                    "value": raw_value,
                    "message": str(exc),
                }
        return cleaned, None
