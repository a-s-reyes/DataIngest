# Design Decisions

Distilled from the planning conversation. Each entry: the decision, what we considered, the reasoning, and the condition that would trigger reconsideration.

---

## Project shape

### D1. General-purpose framework, not tax-bill-specific
- **Decision:** Build a domain-agnostic CSV/file → SQL ingestion framework. Tax bills are the *first reference vendor*, not the product.
- **Considered:**
  - BillBridge — direct rewrite of the C# delinquent bill importer
  - TaxWatch — scheduled drop-folder + auto-ingest
  - TaxReview — same scope as BillBridge but with a pre-import validation report
- **Why:** patterns transfer to aerospace/defense work (telemetry, supply chain, sensor calibration). Domain agnosticism is the differentiator.
- **Revisit if:** the abstraction collapses under a 3rd vendor — i.e., we find ourselves writing per-vendor Python instead of YAML.

### D2. Build from scratch, not fork
- **Decision:** Build from a hand-written skeleton, not fork `simonw/csvs-to-sqlite`.
- **Considered:** forking csvs-to-sqlite (≈500 LOC, MIT, similar problem).
- **Why:** every line is ours, smaller risk of inheriting unwanted scope, faster path to under 1k LOC.
- **Revisit:** N/A — this is a one-time choice.

### D3. v1 single-table loading, not multi-table normalization
- **Decision:** v1 loads one CSV → one table per run. The C# original wrote four files (TaxBills, TaxOwners, TaxMailers, TaxAssessments) per run via foreign-key relationships.
- **Considered:** v1 schema with `relations:` declarations in YAML.
- **Why:** keeps the Mapper as a per-row 1:1 transform; relations add real complexity (FK ordering, transactional batching, error rollback).
- **Revisit when:** a vendor genuinely needs normalization in v1, or v2 adds it as a first-class feature.

---

## Storage & access

### D4. SQLite over DuckDB for v1
- **Decision:** SQLite is the v1 sink; DuckDB stays a `pip install` away.
- **Considered:** DuckDB (native `SELECT * FROM 'file.csv'`, columnar, faster on analytics).
- **Why:**
  - SQLite is in stdlib (`sqlite3`) — zero install drama.
  - Our problem is OLTP-shaped (insert normalized rows), not analytical.
  - Universal tooling: DBeaver, datasette, browser viewers, sqlite-utils.
  - Portfolio reviewers know SQLite at a glance.
- **Revisit when:** ingest workloads exceed ~1M rows/run, or the project grows analytical queries (group-bys over big tables, window functions).

### D5. SQLAlchemy 2.0 Core, not an ORM
- **Decision:** Use SQLAlchemy 2.0 Core (Table, MetaData, insert) — not the ORM, not SQLModel, not raw `sqlite3`.
- **Considered:**
  - Raw `sqlite3`: fine for one-off scripts, painful when we add Postgres/SQL Server.
  - SQLAlchemy ORM: heavier, session/identity-map machinery we don't need for bulk insert.
  - SQLModel: redundant once we already have a Pydantic validation layer per row.
  - Tortoise / Peewee / Pony: smaller communities, weaker SQL Server support.
  - Django ORM: only if you're inside Django.
- **Why:** one API targets SQLite (dev), Postgres (likely prod elsewhere), and SQL Server (DRMS's actual database). Connection-string swap → zero code change.
- **Revisit:** unlikely. SQLAlchemy is the boring correct choice and we'd be wrong to leave it.

### D6. Pydantic v2 for row validation, separate from cleaning
- **Decision:** Pydantic v2 row models built dynamically from the YAML field declarations. Cleaners run *first*; validation runs *second*.
- **Considered:** Pandera (DataFrame-shaped — wrong fit for row-by-row), Marshmallow (older), bare type checks.
- **Why:** cleaning normalizes (`"$1,234.56"` → `Decimal("1234.56")`); validation enforces (`required`, `type`, `enum`). Keeping them separate keeps each layer's contract clean and testable.
- **Revisit:** N/A.

---

## Architecture

### D7. URI-driven sources/sinks (stolen from `ingestr`)
- **Decision:** `dataingest run --source csv:///path --sink sqlite:///out.db --mapping clay.yml`. Source and sink are addressed by URI scheme.
- **Considered:** typed flags (`--source-type csv --source-path foo.csv`).
- **Why:** the CLI surface stays stable as v2 adds `xlsx://`, `json://`, `postgres://`, `mssql://`. New schemes register into the dispatcher; the command shape doesn't move.
- **Revisit:** N/A.

### D8. Plugin discovery: dict registry now, entry points later
- **Decision:** v1 source/sink discovery is a `dict[str, type]` keyed by URI scheme, with lazy imports in each `__init__.py` to trigger registration. v2+ adds `importlib.metadata` entry points without breaking v1.
- **Considered:**
  - `pluggy` (sqlite-utils uses it for SQL functions): too heavy for v1.
  - `importlib.metadata` entry points: needed only when third-party plugin authors exist.
  - Manual factory function: more verbose than decorator + registry.
- **Why:** smallest correct mechanism for v1's scope. visidata-style naming-convention discovery proved this works.
- **Revisit when:** someone wants to ship a third-party DataIngest plugin without forking.

### D9. Cleaners are named pure callables, not a DSL
- **Decision:** Cleaners are `Callable[[Any], Any]` registered by name in `cleaners.py`. YAML references them by string (`cleaners: [strip, parse_decimal]`). Composition is left-to-right via `chain([...])`.
- **Considered:** Jinja-like template DSL, embedded Python expressions in YAML, lambda strings.
- **Why:** testable, composable, no language design problem. Custom cleaners come from a project-local module via a future `cleaners_module:` field — not a packaging concern in v1.
- **Revisit:** N/A.

### D10. Library-first, CLI-as-wrapper (sqlite-utils + tablib pattern)
- **Decision:** `Pipeline` is callable from Python. The Typer CLI is a thin shell that constructs a `Pipeline` and calls `.run()`.
- **Considered:** CLI-internal-only (faster to write, harder to test, can't be embedded).
- **Why:** tests hit the Pipeline directly; programmatic use is possible without subprocess; sqlite-utils precedent is well-trodden.
- **Revisit:** N/A.

### D11. `src/` layout (tablib pattern), not flat package
- **Decision:** Code lives at `src/dataingest/`, not `dataingest/` at the repo root.
- **Considered:** flat layout (sqlite-utils, csvs-to-sqlite use this).
- **Why:** prevents accidental imports from the working directory; forces tests to run against the installed package; portfolio signal that you know modern Python packaging.
- **Revisit:** N/A.

---

## Operational concerns

### D12. `--dry-run` and JSONL error log are first-class
- **Decision:** Every mutating command takes `--dry-run`. Failed rows write to `errors.jsonl` with `{row_number, source_file, field, value, rule, message}`.
- **Considered:** stdout error reporting only, structured logging with structlog, raw exception traces.
- **Why:** these are the features that make a portfolio project look production-aware. Greppable, importable, post-mortem-friendly. Stolen from csvs-to-sqlite's `--replace-tables` posture.
- **Revisit:** N/A.

### D13. Synthetic test fixture, not real Clay CSV
- **Decision:** `tests/fixtures/clay_sample.csv` is a hand-authored 20-row synthetic file matching the mapping shape.
- **Considered:** sanitized real Clay County data (it's public record; legally fine).
- **Why:** the repo will be public on GitHub. Synthetic data has no chance of accidentally exposing anything sensitive. Portfolio reviewers can run `pytest` without provisioning files.
- **Revisit:** N/A — real data lives outside the repo for actual ingest runs.

### D14. No async I/O in v1
- **Decision:** Sync I/O throughout. Single-threaded.
- **Considered:** `aiofiles` + async SQLAlchemy for parallel CSV reads.
- **Why:** most CSVs fit in memory; the bottleneck is usually the validator, not I/O. Async adds real complexity to error reporting.
- **Revisit when:** measured profiling shows I/O is the bottleneck on real workloads (not before).

### D15. v1 supports `on_conflict: error | skip` only
- **Decision:** `replace` is documented in the YAML schema but not implemented in `SqliteSink.write()` for v1.
- **Considered:** full upsert support via `INSERT ... ON CONFLICT DO UPDATE SET ...`.
- **Why:** `error` and `skip` cover 95% of ingest workflows. `replace` requires deciding column-by-column which fields to overwrite — a real product decision, not a quick add.
- **Revisit:** v2.

---

## What we explicitly are NOT building

(From master plan §3, plus topics raised in conversation.)

| Feature | Reason |
|---|---|
| Web UI / dashboard | Out of scope; portfolio project ≠ web app |
| Workflow orchestration (Airflow, Prefect, Dagster) | Belongs in a separate layer — DataIngest is a unit, not a graph |
| Streaming / real-time ingest | v1 is batch; streaming is a different problem |
| Distributed/parallel engine | Premature; profile first |
| Schema inference (`dataingest infer ...`) | Deferred to v2 — explicit declarations are honest for v1 |
| File watcher (auto-ingest on file drop) | Deferred to v4 |
| Multi-vendor parallel processing | Deferred — single-vendor sequential is the v1 contract |
| `pyodbc` / `psycopg` drivers | Deferred to v2/v3 — only when we need those sinks |
| Frictionless Data spec compatibility | Considered; rejected — extra surface area for v1 |

---

## Reference-pattern attribution

| Pattern in DataIngest | Stolen from |
|---|---|
| URI-driven source/sink scheme | `ingestr` |
| Library-first, CLI-as-wrapper | `sqlite-utils` |
| `src/` layout + format-as-class registration | `tablib` |
| Naming-convention dispatch + lazy imports | `visidata` |
| Cleaner-chain as named callables | `rows` (simplified) |
| Single-binary `paths... dbname`-style command | `csvs-to-sqlite` |

Detailed analysis of each: `01-references.md`.

---

## Open questions

These remain unresolved as of the v1 cut. They don't block v1 functionality but warrant attention before v2.

1. **Decimal precision round-trip through SQLite.** Pydantic 2 `Decimal` → SQLAlchemy `Numeric()` → SQLite TEXT/REAL. Confirm precision survives in practice; today's tests verify a single 6.2-digit value.
2. **Date parsing strictness.** Current cleaners (`parse_date_us`, `parse_date_iso`) refuse to guess. `dateutil.parser` would auto-detect but with ambiguity risk (MM/DD vs DD/MM). Stay strict for v1; revisit if user-facing complaints emerge.
3. **Logging verbosity convention.** No `--verbose` / `--quiet` flags yet. Recommended: Click/Typer's `-v / -vv / -vvv` idiom on `run`.
4. **Chunking threshold.** v1 reads/writes one row at a time. If a 100k+ row CSV is too slow, batch into 1k-row chunks at the sink boundary. Don't optimize until measured.
5. **Required vs optional columns.** YAML `required: false` defaults a missing field to `None` (or `default:` if specified). Should missing *columns* (not just empty values) raise a setup error or pass through? Currently passes through as `None`.
