# Changelog

All notable changes to DataIngest will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Nothing yet.

## [0.1.0] — 2026-04-25

Initial release. Production-ready credibility floor + work-tool capability +
portfolio polish all shipped together.

### Added

#### Pipeline core
- Four-stage pipeline: Source → Mapper → Validator → Sink, wired by URI scheme dispatch.
- `Pipeline` orchestrator with chunked streaming writes (default 1000 rows per flush) for bounded memory regardless of input size.
- Dynamic Pydantic row-model construction from YAML field declarations.
- JSONL error log: one JSON object per failed row with `{row_number, source_file, field, value, rule, message}`. Path-or-stream target — `--errors -` streams to stderr.
- `_dataingest_runs` audit table: one row per non-dry-run invocation, recording timestamps, mapping name, row counters, status (`success` / `partial` / `failed`), DataIngest version. Auto-created in any sink.

#### Sources
- `csv://` — reads delimited text files. Supports header-or-no-header, custom delimiter, custom encoding via URI params.
- `xlsx://` — Excel via lazy-imported `openpyxl`. Sheet selection via `?sheet=Name` URI param. Native types (int/float/datetime) yielded for downstream cleaner coercion.

#### Sinks
- `_BaseSqlSink` — shared SQL-sink machinery: model-to-table introspection, manifest write, transaction management, type-unwrap helper.
- `sqlite://` — SQLite via SQLAlchemy 2.0 Core. `error` and `skip` conflict modes.
- `postgres://` / `postgresql://` — Postgres via lazy-imported `psycopg`. Full `error` / `skip` / `replace` (upsert via `INSERT ... ON CONFLICT DO UPDATE`) modes.

#### Cleaners
- Zero-arg: `strip`, `upper`, `lower`, `remove_extra_whitespace`, `remove_currency_symbols`, `parse_decimal`, `parse_int`, `parse_date_us`, `parse_date_iso`, `parse_datetime_iso`.
- Parameterized factories: `regex_replace(pattern, repl)`, `remove_chars(chars)`, `truncate(n)`, `default_if_empty(value)`. Specs parsed via `ast.literal_eval` (literals only — no code execution).
- Pre-flight validation in `FieldConfig` reports every malformed cleaner spec at YAML load time.

#### Mapping types
- `str`, `int`, `decimal`, `date`, `datetime`, `bool`. ISO 8601 datetime with trailing `Z` natively supported (Python 3.11+).

#### CLI
- `dataingest run` — full ingestion pipeline with structured exit codes (`0` clean, `1` preflight error, `2` partial failure, `3` total failure).
- `dataingest validate` — verify mapping syntax + cleaner references without touching data.
- `dataingest infer` — sniff a CSV's first N rows and emit a starter mapping YAML.
- `dataingest tables` — list tables in a sink with row counts and recent run-manifest entries.
- `dataingest version`.
- Flags: `--dry-run`, `--limit`, `--chunk-size`, `--errors` (path or `-` for stderr), `-v` / `-vv` verbose, `-q` / `--quiet`.

#### Reference mappings + fixtures
- `mappings/telemetry.yml` — flight test telemetry (sensor channels, ISO timestamps, decimal values).
- `mappings/qualification.yml` — component qualification test results (measured values vs. tolerances, MM/DD/YYYY dates, PASS/FAIL).
- `mappings/parts_inventory.yml` — parts master / NSN inventory (string identifiers, integer quantities, ISO audit dates).
- Hand-authored 20-row synthetic CSV fixture per mapping.

#### Extensibility
- Plugin discovery via `importlib.metadata` entry points (groups: `dataingest.sources`, `dataingest.sinks`). Built-ins always win — third-party plugins cannot override `csv`, `xlsx`, `sqlite`, or `postgres`. Failed plugin loads silently no-op so a broken third-party package never breaks startup.

#### Infrastructure
- GitHub Actions CI on every push and pull request: pytest matrix across Python 3.11 / 3.12 / 3.13, ruff lint + format check, mypy strict, coverage gate at 80% (current: 93%). Postgres integration tests run against a `services: postgres:16` container.
- `uv.lock` committed for reproducible builds.
- Coverage and CI badges in README.

#### Documentation
- [README.md](README.md) — quickstart, mapping summary, CLI reference, exit codes, demo session, Docker recipe for local postgres testing.
- [ARCHITECTURE.md](ARCHITECTURE.md) — pipeline diagram, five invariants, three extension points with code stubs, module layout, test architecture, CI gates, explicit out-of-scope list.

### Known limitations
- `mssql://` sink is not yet implemented. `_BaseSqlSink` is structured to make this a small follow-up — override `_make_url` for `mssql+pyodbc://` and `_make_insert_stmt` to use `MERGE` for replace mode.
- `replace` conflict mode is supported on `postgres://` only; `sqlite://` accepts `error` and `skip`.

[Unreleased]: https://github.com/a-s-reyes/DataIngest/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/a-s-reyes/DataIngest/releases/tag/v0.1.0
