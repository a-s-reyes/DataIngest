# DataIngest — Project Context

Read [`plan/00-master-plan.md`](plan/00-master-plan.md) first for the full architecture, scope, and reference patterns. [`plan/01-references.md`](plan/01-references.md) has the prior-art analysis.

## Quick context

- **Stack:** Python 3.13, uv, SQLAlchemy 2.0, Pydantic v2, Typer, pytest
- **v1 scope:** CSV → SQLite with declarative YAML vendor mappings (see `mappings/clay.yml`)
- **First reference vendor:** Clay County KY sheriff delinquent tax bill export
- **Source:** `src/dataingest/`
- **Tests:** `tests/`

## Architectural rules

- Source/sink discovery is by URI scheme (`csv://`, `sqlite://`). Never add new file-type CLI flags — extend the URI scheme registry.
- Cleaners are named callables in `cleaners.py`, referenced by name in YAML. Validation is a separate Pydantic concern — don't conflate cleaning and validation.
- `--dry-run` and JSONL error log are first-class. Every mutating path supports them.
- Lazy-import optional format dependencies (Excel needs openpyxl, Postgres needs psycopg). CSV-only users should not need to install them.

## What's stubbed vs implemented

**Implemented:** cleaners, URI parser, error types + JSONL log, Pydantic config models, CLI shell, mapping fixture.

**Stubbed (raises `NotImplementedError`):** `Pipeline.run()`, `CsvSource.rows()`, `SqliteSink.write()`. v1 fills these in.
