# DataIngest — Master Plan

> A modern, modular Python framework for ingesting messy CSV/Excel/JSON files, validating and cleaning them by declarative rules, and loading them into a SQL database. Domain-agnostic core; first reference vendor is the Clay County (KY) sheriff delinquent-bill CSV format.

---

## 1. What we're building

A command-line tool and Python library that does one thing well:

**Take any tabular file → run it through a declared validation + cleaning pipeline → load it into a database.**

The "declared" part is the differentiator. Onboarding a new vendor format does not mean writing Python — it means writing a YAML mapping file. The same engine handles every vendor. New file formats and new database backends extend the engine via small, well-defined interfaces.

## 2. Why

Three audiences, one project:
- **Portfolio:** a credible, real-engineering Python codebase for the Huntsville defense/aerospace job search. Demonstrates: type-safe Python, validation, modular design, testing, packaging, CLI ergonomics, SQL integration.
- **Learning vehicle:** practice modern Python (Pydantic v2, SQLAlchemy 2.0, Typer, uv, pytest) on a real problem with real edge cases.
- **Possible work tool:** the patterns transfer directly to telemetry/test data ingestion, supply chain feeds, sensor calibration data, audit/compliance pipelines — work that defense primes (Lockheed, Leidos, SAIC, Northrop, RTX) and DRMS itself do every day.

## 3. Non-goals (so we don't drift)

We are **not** building:
- A web UI or dashboard
- An orchestrator (Airflow/Prefect/Dagster live elsewhere)
- A streaming or real-time system
- A distributed/parallel ingestion engine
- A general-purpose data warehouse
- A schema inference tool (v1 — humans declare schemas)

If a feature isn't in §7 (v1 scope), it's not in v1. Period.

## 4. Architecture

### 4.1 High-level pipeline

```
  ┌────────┐     ┌──────────┐     ┌──────────┐     ┌────────┐
  │ Source │ ──▶ │  Mapper  │ ──▶ │ Validator│ ──▶ │  Sink  │
  └────────┘     └──────────┘     └──────────┘     └────────┘
       │              │                 │              │
   CSV/JSON/      column→field      Pydantic        SQLite
   Excel...       cleaner chain    row models      DuckDB/PG
```

Four pluggable stages. Each stage is a small abstraction:
- **Source** — emits an iterator of raw `dict[str, Any]` rows
- **Mapper** — applies the YAML-declared column→field mapping and runs cleaner chains
- **Validator** — Pydantic row model for type/required/enum/format validation
- **Sink** — writes validated rows to a database table; handles batching, idempotency

### 4.2 URI-driven I/O (stolen from `ingestr`)

Sources and sinks are addressed by URI scheme:

```bash
dataingest run \
  --source csv:///data/clay_2024.csv \
  --sink sqlite:///./out.db \
  --mapping mappings/clay.yml
```

The CLI surface stays stable as new formats and backends arrive. v2 adds `xlsx://`, `json://`, `postgres://`, `mssql://` without changing the command shape.

### 4.3 Module structure (src layout, stolen from `tablib`)

```
DataIngest/
├── pyproject.toml
├── README.md
├── LICENSE                          # MIT
├── plan/                            # this folder
│   ├── 00-master-plan.md
│   └── 01-references.md
├── src/
│   └── dataingest/
│       ├── __init__.py
│       ├── cli.py                   # Typer commands, thin wrapper over Pipeline
│       ├── pipeline.py              # Pipeline class — the orchestrator
│       ├── config.py                # Pydantic models for the YAML mapping schema
│       ├── cleaners.py              # Named cleaner registry (strip, parse_decimal, ...)
│       ├── errors.py                # Exception types + JSONL error sink
│       ├── uri.py                   # URI parser → (scheme, path, params)
│       ├── sources/
│       │   ├── __init__.py          # registry: scheme → Source class
│       │   └── csv.py               # CsvSource implementation
│       └── sinks/
│           ├── __init__.py          # registry: scheme → Sink class
│           └── sqlite.py            # SqliteSink implementation
├── mappings/
│   └── clay.yml                     # first reference vendor mapping
├── tests/
│   ├── fixtures/
│   │   └── clay_sample.csv          # synthetic 20-row test fixture
│   ├── test_cleaners.py
│   ├── test_pipeline.py
│   ├── test_csv_source.py
│   ├── test_sqlite_sink.py
│   └── test_e2e_clay.py             # full pipeline end-to-end
└── docs/                            # v2+
```

### 4.4 Key abstractions

```python
# sources/__init__.py
from typing import Iterator, Protocol, Any

class Source(Protocol):
    """Emits raw rows from some external source."""
    def __init__(self, uri: str, params: dict[str, Any]) -> None: ...
    def rows(self) -> Iterator[dict[str, Any]]: ...
    def close(self) -> None: ...

# sinks/__init__.py
class Sink(Protocol):
    """Writes validated rows to some destination."""
    def __init__(self, uri: str, params: dict[str, Any]) -> None: ...
    def begin(self, schema: type[BaseModel]) -> None: ...
    def write(self, rows: Iterable[BaseModel]) -> int: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...

# cleaners.py
CleanerFn = Callable[[Any], Any]
REGISTRY: dict[str, CleanerFn] = {}

def register(name: str):
    def deco(fn: CleanerFn) -> CleanerFn:
        REGISTRY[name] = fn
        return fn
    return deco
```

Discovery for v1 is a **dict keyed by URI scheme** (visidata's naming-convention pattern, simplified). v2 layers on `importlib.metadata` entry points without breaking v1.

### 4.5 YAML mapping schema (the hardest decision)

```yaml
# mappings/clay.yml
spec_version: 1
vendor: clay-sheriff-ky
description: Clay County KY sheriff delinquent tax bill export
source:
  format: csv
  encoding: utf-8
  header: true
  delimiter: ","

target:
  table: tax_bills
  primary_key: bill_number
  on_conflict: skip          # one of: skip | replace | error

fields:
  bill_number:
    column: 0                # or: name: "Bill Number"
    type: str
    required: true
    cleaners: [strip, upper]

  account_number:
    column: 3
    type: str
    cleaners: [strip]

  property_address:
    column: 6
    type: str
    cleaners: [strip, remove_extra_whitespace]
    required: false
    default: ""

  face_amount:
    column: 27
    type: decimal
    cleaners: [strip, remove_currency_symbols, parse_decimal]
    required: true

  date_due:
    column: 10
    type: date
    cleaners: [parse_date_us]
    required: true
```

Schema is itself validated by a top-level Pydantic model in `config.py`. Bad YAML fails fast with line-numbered errors before any data is read.

### 4.6 Cleaner registry (stolen from `rows`)

Cleaners are small, named, pure functions. v1 ships ~10 built-ins:

```
strip                         strip whitespace
upper / lower                 case normalization
remove_extra_whitespace       collapse runs of spaces
remove_chars(chars)           strip arbitrary chars
regex_replace(pattern, repl)  regex substitution
parse_decimal                 to Decimal, handles $, commas, parens
parse_date_us                 to date, MM/DD/YYYY
parse_date_iso                to date, YYYY-MM-DD
default_if_empty(value)       null/empty → value
truncate(n)                   max length
```

Custom cleaners come from a project-local Python file declared in the YAML's `cleaners_module:` field — no plugin packaging required for v1.

### 4.7 CLI shape (stolen from `sqlite-utils`, `csvs-to-sqlite`, `ingestr`)

```bash
# Run a full pipeline
dataingest run \
  --source csv:///data/clay_2024.csv \
  --sink sqlite:///./out.db \
  --mapping mappings/clay.yml

# Validate mapping + first N rows without writing
dataingest run --dry-run --limit 100 ...

# Validate mapping syntax only
dataingest validate --mapping mappings/clay.yml

# Inspect what's in a sink
dataingest tables sqlite:///./out.db
```

Errors stream to `<output_dir>/errors.jsonl` — one JSON object per failed row, with `{row_number, source_file, field, value, rule, message}`. Greppable, importable, post-mortem-friendly.

## 5. Tech stack

| Concern | Pick | Rationale |
|---|---|---|
| Python | **3.13.13** | Already installed; current stable |
| Project tool | **uv** | Already installed; replaces pip+venv+pyenv+pipx |
| Validation | **Pydantic v2** | The standard; fast, type-safe |
| DB access | **SQLAlchemy 2.0 (Core)** | One API for SQLite/Postgres/SQL Server later |
| Migrations | **Alembic** | Standard pair with SQLAlchemy (v2+) |
| CLI | **Typer** | Clean, type-hint-driven, small |
| Config | **PyYAML + Pydantic** | YAML parsed into typed config objects |
| Tests | **pytest** + **pytest-cov** | Standard |
| Lint/format | **ruff** | Replaces flake8/isort/black |
| Type check | **mypy** (strict) | Catches type errors before tests |
| Logging | **stdlib `logging`** + JSONL error log | No structlog dependency in v1 |

## 6. License + repo

- **License:** MIT
- **GitHub:** new public repo `a-s-reyes/DataIngest`
- **Branch model:** trunk-based. `main` is always green. Feature branches → PRs (even if you're the only reviewer; it builds the habit).
- **CI:** v2. v1 ships locally with `make test` + `make lint`.

## 7. v1 scope (the exact cut)

What ships in v1, and nothing else:

- [ ] `csv://` source
- [ ] `sqlite://` sink
- [ ] One reference vendor mapping: `mappings/clay.yml`
- [ ] One synthetic test fixture: `tests/fixtures/clay_sample.csv` (~20 rows)
- [ ] Pydantic-validated YAML mapping schema
- [ ] 10 built-in cleaners (§4.6)
- [ ] `dataingest run` command
- [ ] `dataingest validate` command
- [ ] `--dry-run` flag
- [ ] `--limit N` flag
- [ ] `errors.jsonl` error sink
- [ ] >80% test coverage on the core engine; full e2e test on Clay
- [ ] README with one quickstart example
- [ ] `pyproject.toml` with all deps + project metadata

**Lines budget:** ~800-1200 LOC of Python in `src/`, ~400-600 LOC of tests. If you're trending higher, scope is wrong.

**Time budget:** 3-4 weekend evenings to v1.

## 8. v2+ roadmap (everything else, explicitly deferred)

- v2 — `xlsx://` source, `json://` source, `--upsert` mode, schema diff/inference (`dataingest infer file.csv > mapping.yml`)
- v3 — `postgres://` sink, `mssql://` sink, plugin entry points via `importlib.metadata`, run-manifest table
- v4 — file watcher (drop folder → auto-ingest), Slack/email error notifications

## 9. Open questions

1. **Test data — real or synthetic?** Clay County tax data is public record, so a real (sanitized) sample is acceptable. But for the public repo, a synthetic 20-row fixture you author yourself is safer. **Default to synthetic.**
2. **Decimal precision** — Pydantic 2 supports `Decimal` natively; how do we round-trip through SQLite (which stores as TEXT/REAL)? Probably use `NUMERIC` type and let SQLAlchemy handle. Verify in v1.
3. **Date parsing** — `dateutil.parser.parse` is forgiving but slow and ambiguous (MM/DD vs DD/MM). v1 uses explicit format cleaners (`parse_date_us`, `parse_date_iso`) and refuses to guess.
4. **Logging verbosity** — `--quiet`, `--verbose`, `--debug`? Pick one convention. Recommend `-v / -vv / -vvv` (Click/Typer idiom).
5. **Chunking** — v1 reads/writes rows one-at-a-time for simplicity. If a single 100k-row CSV is too slow, batch into 1000-row chunks at the sink boundary. Don't optimize until measured.

---

## Reference architecture credits

This plan synthesizes patterns from six prior-art projects. Detailed analysis lives in `01-references.md`. Quick map:

| Pattern | Stolen from |
|---|---|
| Library-first, CLI-as-wrapper | `sqlite-utils` |
| Single-binary command shape | `csvs-to-sqlite` |
| `import_from_X` / `export_to_X` symmetry | `rows` |
| `src/` layout + format-as-class | `tablib` |
| Naming-convention discovery + lazy imports | `visidata` |
| URI scheme for source/sink | `ingestr` |
| Cleaner-chain-as-named-callables | `rows` + custom |
