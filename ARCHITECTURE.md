# DataIngest — Architecture

This document describes how DataIngest is built and how to extend it. Read the [README](README.md) first for what the project does; this file covers *how*.

The codebase is small and opinionated. The numbers and section headings below are stable contracts — if you change one, update this file in the same commit.

## Pipeline

```
  ┌────────┐     ┌──────────┐     ┌──────────┐     ┌────────┐
  │ Source │ ──▶ │  Mapper  │ ──▶ │ Validator│ ──▶ │  Sink  │
  └────────┘     └──────────┘     └──────────┘     └────────┘
       │              │                 │              │
   raw rows       column→field      Pydantic       SQL DB
   as dicts       cleaner chain     row models     (with audit)
```

Four stages, each behind a small `Protocol`. The orchestrator [`Pipeline`](src/dataingest/pipeline.py) wires them together.

| Stage | Module | Responsibility |
|---|---|---|
| **Source** | [`sources/`](src/dataingest/sources/) | Yield raw rows as `dict[str, Any]` from some external location. Every row dict carries both numeric-index keys (`"0"`, `"1"`, ...) and header-name keys, so a YAML mapping may reference columns either way. |
| **Mapper** | [`pipeline._apply_mapping`](src/dataingest/pipeline.py) | Per row: pull out each declared field by `column:` lookup, run its named cleaner chain, build a Python `dict`. Cleaner failures raise an internal `_CleanerError` and route the row to `errors.jsonl`. |
| **Validator** | [`pipeline._build_row_model`](src/dataingest/pipeline.py) | A Pydantic `BaseModel` is constructed *dynamically* from the YAML field declarations on every run. Type coercion, required-field enforcement, and default-fallback all happen at this layer. Validation failures route to `errors.jsonl`. |
| **Sink** | [`sinks/`](src/dataingest/sinks/) | Bulk-insert validated rows in chunks of `chunk_size` (default 1000). Honors `on_conflict` (`error` / `skip` / `replace` per sink). Writes one row to `_dataingest_runs` per invocation. |

## The five invariants

These are load-bearing. Changing any of them is a deliberate, documented decision — never a side-effect of another change.

### 1. URI dispatch is the only file-type/backend selector

Sources and sinks are addressed by URI scheme: `csv:///`, `xlsx:///`, `sqlite:///`, `postgres://`. The CLI exposes `--source` and `--sink` and nothing else for picking the format. New formats *only* arrive as new schemes — never as new CLI flags. This keeps the CLI surface stable across the project's lifetime.

The full URI handling lives in [`uri.py`](src/dataingest/uri.py): `parse(uri)` → `(scheme, path, params)`, `resolve_uri_path(path)` → filesystem path. Every source and sink uses the same helper to handle Windows drive letters, POSIX absolute paths, and SQLAlchemy-style relative paths uniformly.

### 2. Cleaning is separate from validation

[`cleaners.py`](src/dataingest/cleaners.py) **normalizes**: `"$1,234.56"` → `Decimal("1234.56")`, `"  hello  "` → `"hello"`. Pydantic **enforces**: required, type, format. Cleaners run *first*; validation runs *second*.

Mixing the two is a category error. A cleaner that throws "this is required" is doing validation; a validator that strips whitespace is doing cleaning. Keep them apart.

### 3. Mappings declare; they don't implement

A mapping is a YAML file. It names cleaners by string (`cleaners: [strip, parse_decimal]`); it does not embed Python. The mapping schema itself is validated by Pydantic at load time — bad YAML, unknown cleaner names, malformed parameterized-cleaner syntax (`truncate(not-a-number)`), or a primary key not declared as a field all fail fast in [`config.from_yaml`](src/dataingest/config.py), before any data is touched.

Onboarding a new file format does not mean writing Python. It means writing a YAML file. If you ever find yourself reaching for a custom Python cleaner, ask first whether the parameterized cleaner registry (`regex_replace`, `remove_chars`, `truncate`, `default_if_empty`) covers it.

### 4. `--dry-run` is real

`--dry-run` runs the *same code path* as a real run — source, mapper, cleaners, validator. It just never opens the sink. There is no mock, no fake, no separate code path. If `--dry-run` succeeds, the real run will too (modulo sink-side schema mismatches, which are rare because the sink builds its schema from the row model on `begin()`).

### 5. Errors are durable

Every failed row is written to a JSONL log: `{row_number, source_file, field, value, rule, message}`. One JSON object per line, greppable, importable, post-mortem-friendly. The default location is `./errors.jsonl`; `--errors -` streams to stderr instead. The location is also recorded in the `_dataingest_runs` audit table so you can correlate a run to its error log months later.

## Audit trail: `_dataingest_runs`

Every non-dry-run pipeline invocation writes one row to a `_dataingest_runs` table in the same database as the data. The schema lives in [`manifest.py`](src/dataingest/manifest.py): `run_id` (UUID, PK), timestamps, mapping name, source URI, target table, row counters, chunks written, error log path, dataingest version, dry-run flag, status (`success` / `partial` / `failed`).

This is the audit-trail concession that turns DataIngest from a tool into a *traceable* tool. "What did we ingest yesterday and how many rows failed?" is a SQL query, not a guessing game.

## Three extension points

### Adding a new source

```python
# src/dataingest/sources/json.py
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from ..uri import resolve_uri_path
from . import register


@register("json")
class JsonSource:
    def __init__(self, path: str, params: dict[str, str]) -> None:
        self.path = Path(resolve_uri_path(path))

    def rows(self) -> Iterator[dict[str, Any]]:
        import json  # lazy if optional
        with self.path.open(encoding="utf-8") as fp:
            data = json.load(fp)
        for raw in data:
            yield raw  # already a dict; Mapper handles the rest

    def close(self) -> None:
        pass
```

Then add `from . import json as _json` to [`sources/__init__.py`](src/dataingest/sources/__init__.py) so the registration runs at import time. That's it. The CLI picks it up the moment a user passes `--source json:///...`.

### Adding a new sink

Inherit from [`_BaseSqlSink`](src/dataingest/sinks/_base.py) and override only the dialect-specific bits:

```python
# src/dataingest/sinks/mssql.py
from typing import Any
from sqlalchemy import Table

from . import register
from ._base import _BaseSqlSink


@register("mssql")
class MssqlSink(_BaseSqlSink):
    SUPPORTED_CONFLICT_MODES = ("error", "skip", "replace")

    def _make_url(self) -> str:
        try:
            import pyodbc  # noqa: F401
        except ImportError as err:
            raise ImportError("mssql:// requires pyodbc + ODBC Driver 18") from err
        return f"mssql+pyodbc://{self.path}"

    def _make_insert_stmt(self, table: Table) -> Any:
        # SQL Server has no ON CONFLICT — use MERGE for replace mode.
        ...
```

Table creation, manifest writes, transaction management, `Pydantic-model → SQLAlchemy-table` introspection — all inherited from `_BaseSqlSink`. Only the URL builder and the dialect-aware insert statement are sink-specific.

### Adding a new cleaner

Two flavors. Zero-arg cleaners use `@register`:

```python
# In cleaners.py
@register("normalize_phone")
def normalize_phone(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return re.sub(r"\D", "", value)
```

Argument-bearing cleaners use `@register_factory` — the factory takes config args and returns a cleaner closure:

```python
@register_factory("pad_left")
def pad_left(width: int, fill: str = "0") -> CleanerFn:
    if width < 0:
        raise ValueError(f"pad width must be >= 0, got {width}")

    def cleaner(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        return value.rjust(width, fill)

    return cleaner
```

YAML usage:

```yaml
cleaners: [strip, normalize_phone, pad_left(10, '0')]
```

The spec parser uses `ast.literal_eval` for arguments — only Python literals are allowed, never code. Pre-flight validation in `FieldConfig._validate_cleaners` reports every malformed cleaner spec at YAML load time, so users see all issues at once instead of one-per-rerun.

## Module layout

```
src/dataingest/
├── __init__.py
├── cli.py             # Typer commands: run, validate, infer, tables, version
├── pipeline.py        # Pipeline orchestrator, RunResult, _CleanerError, chunking
├── config.py          # Pydantic models for the YAML mapping schema
├── cleaners.py        # Cleaner registry + factory registry + spec parser
├── manifest.py        # RunManifest dataclass + status-derivation helpers
├── errors.py          # DataIngestError, RowError, JsonlErrorLog (file or stream)
├── uri.py             # parse(), resolve_uri_path(), ParsedURI
├── sources/
│   ├── __init__.py    # Source Protocol + dict registry + register decorator
│   ├── csv.py         # CsvSource
│   └── xlsx.py        # XlsxSource (lazy openpyxl)
└── sinks/
    ├── __init__.py    # Sink Protocol + dict registry + register decorator
    ├── _base.py       # _BaseSqlSink (shared SQL machinery)
    ├── sqlite.py      # SqliteSink (~30 lines of dialect glue)
    └── postgres.py    # PostgresSink (lazy psycopg, full upsert via ON CONFLICT)
```

The `src/` layout is deliberate. It prevents accidental imports from the working directory and forces tests to run against the installed package — the same way users will invoke it.

## Test architecture

Tests live in [`tests/`](tests/). Three categories:

- **Unit tests** (`test_cleaners.py`, `test_uri.py`, `test_config.py`) — small, fast, no DB.
- **Integration tests** (`test_csv_source.py`, `test_xlsx_source.py`, `test_sqlite_sink.py`, `test_postgres_sink.py`, `test_chunking.py`, `test_manifest.py`, `test_e2e.py`) — exercise real I/O against tmp files / in-memory DBs / a postgres service container.
- **CLI tests** (`test_cli.py`) — `typer.testing.CliRunner` covering each exit-code path and flag combination.

Postgres integration tests are gated on `DATAINGEST_TEST_POSTGRES_URL`. CI sets it via the `services: postgres:16` GitHub Actions container. Locally, run a Docker postgres and export the URL — see the [README's Development section](README.md#development).

## CI gates

Every push to `main` and every PR runs four gates ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)):

1. **pytest** — across Python 3.11, 3.12, 3.13. Coverage XML uploaded as an artifact.
2. **ruff check** — lint.
3. **ruff format --check** — formatting.
4. **mypy --strict** — type-check.

Coverage gate is `fail_under = 80` in [`pyproject.toml`](pyproject.toml). Current coverage runs above 90%.

If any gate fails, the run fails. There are no soft warnings, no `continue-on-error: true` escape hatches.

## What's deliberately out of scope

These will not happen in this codebase, no matter how convenient they would be in the moment:

- Web UI / dashboard — wrong layer; DataIngest writes to SQL, use Datasette / DBeaver / metabase against the output.
- Workflow orchestration (Airflow / Prefect / Dagster) — DataIngest is a *unit* of work, not a graph.
- Streaming / real-time ingest — fundamentally different problem.
- Distributed / parallel engine — premature; profile first.
- File-watcher daemon — belongs in cron / systemd path units / orchestrator triggers, never inside DataIngest.
- Slack / email notifications — also belongs upstream of DataIngest. Read the exit code or the manifest table.

If a feature you want is in this list, the answer isn't "let's add a flag." The answer is "use the right tool around DataIngest."
