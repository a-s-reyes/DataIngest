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

**Implemented:** cleaners, URI parser, error types + JSONL log, Pydantic config models, CLI, `Pipeline.run()`, `CsvSource`, `SqliteSink` (with `error` / `skip` conflict modes), three reference mappings + fixtures, parameterized e2e tests.

**Deferred to v2+:** `xlsx://` and `json://` sources, `replace` conflict mode (full upsert), Postgres / SQL Server sinks, plugin entry points, schema inference, file-watcher daemon.
