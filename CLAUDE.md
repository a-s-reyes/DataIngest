# DataIngest — Project Context

Local-only design notes live in `plan/` (gitignored): `00-master-plan.md` for architecture and scope, `01-references.md` for prior-art analysis, `02-decisions.md` for the rationale behind specific design decisions. If those files exist locally, read them first; on a fresh clone they won't be present — work from this file and the README.

## Quick context

- **Stack:** Python 3.13, uv, SQLAlchemy 2.0, Pydantic v2, Typer, pytest
- **v1 scope:** CSV → SQLite with declarative YAML schema mappings
- **Reference mappings:** `mappings/telemetry.yml` (flight test telemetry), `mappings/qualification.yml` (component qualification tests), `mappings/parts_inventory.yml` (parts inventory / NSN catalog)
- **Source:** `src/dataingest/`
- **Tests:** `tests/` — fixtures under `tests/fixtures/`, parameterized e2e in `tests/test_e2e.py`

## Architectural rules

- Source/sink discovery is by URI scheme (`csv://`, `sqlite://`). Never add new file-type CLI flags — extend the URI scheme registry.
- Cleaners are named callables in `cleaners.py`, referenced by name in YAML. Validation is a separate Pydantic concern — don't conflate cleaning and validation.
- `--dry-run` and JSONL error log are first-class. Every mutating path supports them.
- Lazy-import optional format dependencies (Excel needs openpyxl, Postgres needs psycopg). CSV-only users should not need to install them.
- All test fixtures are synthetic. Never commit real engineering, telemetry, or supply-chain data — even from sources that *seem* public.

## Implementation status

**Implemented:** cleaners, URI parser, error types + JSONL log, Pydantic config models, CLI, `Pipeline.run()` with chunked streaming writes, `CsvSource`, `SqliteSink` (with `error` / `skip` conflict modes), three reference mappings + fixtures, parameterized e2e tests.

**Roadmap:** see `plan/03-roadmap.md` for the production-readiness sequencing.

**Tier 1 complete:** T1.1 chunked streaming writes ✅, T1.2 GitHub Actions CI ✅, T1.3 mypy strict clean ✅, T1.4 coverage gate at 80% ✅, T1.5 run manifest ✅.

**Tier 2 (work-tool capability):** T2.1 xlsx source ✅, T2.2 datetime field type ✅, T2.3 parameterized cleaners ✅, T2.4 parse_int cleaner ✅, T2.5 postgres sink with full upsert ✅, T2.6 CLI ergonomics ✅. T2.5b (mssql sink) carved out — explicitly deferred per user direction.

**Tier 3 (portfolio polish) shipped:** T3.1 ARCHITECTURE.md ✅, T3.2 `dataingest infer` ✅, T3.3 `dataingest tables` ✅, T3.4 plugin entry points via `importlib.metadata` ✅, T3.5 badges + README demo session ✅ (asciinema cast remains a manual user step).

**Project status:** production-ready, portfolio-polished, 183 tests passing (+8 postgres-skipped without env var), 90%+ coverage, mypy strict clean, full CI on every push.

**Deferred:** `xlsx://` and `json://` sources, `replace` conflict mode (full upsert), Postgres / SQL Server sinks, plugin entry points, schema inference, parameterized cleaners — all scheduled in the roadmap.
