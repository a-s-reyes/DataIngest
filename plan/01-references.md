# Reference Architecture Analysis

Analysis of six prior-art projects, used to inform DataIngest's design. Source: deep read of each repo's README + key source files.

---

## sqlite-utils (simonw/sqlite-utils)

- **Goal:** Python CLI + library for manipulating SQLite databases.
- **Layout:** Flat `sqlite_utils/` package, `tests/`, `docs/`. Not heavily subdivided.
- **Patterns:**
  - **Library-first design** — CLI is a thin Click wrapper over the Python API.
  - `Database` and `Table` objects expose chainable methods (`.insert_all()`, `.create_index()`).
  - Schema inferred from first N rows on insert.
  - Plugin system via **pluggy** for custom SQL functions.
- **CLI shape:** `sqlite-utils insert DB TABLE FILE`, `memory`, `query`, `tables`, `rows`, `extract`. Flags: `--csv`, `--tsv`, `--json`, `--pk`, `--alter`, `--batch-size`, `--detect-types`.
- **What we steal:**
  - Library-first with CLI as wrapper.
  - Chainable `Database` / `Table` API.
  - `--alter` semantics for schema drift.
  - `pluggy` for v2+ plugin discovery (deferred).

---

## csvs-to-sqlite (simonw/csvs-to-sqlite)

- **Goal:** Convert one or more CSVs into a SQLite database (companion to Datasette).
- **Layout:** Single-purpose, minimal `csvs_to_sqlite/` package, `tests/`.
- **Patterns:**
  - Click CLI.
  - pandas under the hood for type inference.
  - Recursive directory walk.
  - FTS hookup, column extraction into lookup tables.
- **CLI shape:** `csvs-to-sqlite [OPTIONS] PATHS... DBNAME`. Flags: `-s/--separator`, `-c/--extract-column`, `-d/--date`, `-pk/--primary-key`, `--shape`, `-f/--fts`, `--replace-tables`, `--just-strings`.
- **What we steal:**
  - Single-binary command shape (`paths... dbname`).
  - `--shape` for explicit type override.
  - `--replace-tables` flag — model dry-run/upsert semantics on this.

---

## rows (turicas/rows)

- **Goal:** A common, beautiful interface to tabular data regardless of format.
- **Layout:** `rows/` package, `rows/plugins/` (one file per format: `plugin_csv.py`, `plugin_json.py`, `plugin_xlsx.py`, ...), `tests/`, `docs/`.
- **Patterns:**
  - Each format plugin exposes a pair of free functions (`import_from_<fmt>` / `export_to_<fmt>`).
  - Shared helpers from `rows.plugins.utils` (`create_table`, `get_filename_and_fobj`, `serialize`, `ipartition`).
  - `Table` and `Field` are the universal abstractions.
  - Type detection is centralized.
  - **No decorators or registration** — plugins are wired via direct imports in `rows/__init__.py`. Convention over configuration.
- **CLI shape:** Minor `rows` CLI exists (convert / query) but not the focus.
- **What we steal:**
  - Symmetric `import_from_X` / `export_to_X` pair as a clean v1 pattern.
  - Shared utility helpers extracted early — adding Excel later becomes trivial.
  - `Table` / `Field` separation maps cleanly to Pydantic schema models.

---

## tablib (jazzband/tablib)

- **Goal:** Pythonic data import/export module for tabular datasets across many formats.
- **Layout:** `src/tablib/` (src layout), `src/tablib/formats/` with one module per format, `tests/`, `docs/`.
- **Patterns:**
  - `Dataset` is the central object.
  - Each format is a class with `export_set` / `import_set` methods conforming to an implicit interface.
  - Formats register themselves into a registry at import time.
  - `Databook` for multi-sheet collections.
- **CLI shape:** None — library only.
- **What we steal:**
  - **`src/` layout** — keeps tests honest (you can't accidentally import from working directory).
  - Format-as-class with a stable interface (more typesafe than `rows`' free-function style; better fit for Python 3.13 + Pydantic).
  - `Dataset` / `Databook` separation as a precedent for single-table vs. multi-table loads.

---

## visidata (saulpw/visidata)

- **Goal:** Terminal multi-tool for exploring and arranging tabular data.
- **Layout:** `visidata/`, `visidata/loaders/` (~70 files: `csv.py`, `json.py`, `xlsx.py`, `parquet.py`, `postgres.py`, ...), `plugins/`, `tests/`, `sample_data/`.
- **Patterns:**
  - Each loader file defines `open_<ext>(p)` returning a `Sheet` subclass.
  - Registration is by **naming convention** (`open_csv` for `.csv`).
  - `@VisiData.api` decorator attaches methods to the global `vd` object.
  - `Sheet` is the universal abstraction; loaders subclass it.
  - **Lazy imports** keep optional deps (openpyxl, etc.) opt-in.
- **CLI shape:** `vd <input>` or piped stdin; format auto-detected by extension, overridable with `-f`.
- **What we steal:**
  - **Naming-convention discovery** (`open_<ext>`) — dead simple, no plugin manifest needed for v1.
  - Lazy imports for optional format deps (Excel needs openpyxl, Postgres needs psycopg — don't make CSV users install them).
  - Extension → loader dispatch with `-f` override.

---

## ingestr (bruin-data/ingestr)

- **Goal:** Copy data from any source to any destination, no code — declarative URI-driven CLI.
- **Layout:** Compact `ingestr/` package wrapping `dlt` and SQLAlchemy.
- **Patterns:**
  - **Source/dest as URIs** (`postgresql://...`, `csv://./file.csv`, `bigquery://...`).
  - Incremental load modes (`append`, `merge`, `delete+insert`) declared via flags.
  - Backend abstraction normalizes dlt + SQLAlchemy.
- **CLI shape:** `ingestr ingest --source-uri ... --source-table ... --dest-uri ... --dest-table ... --incremental-strategy ...`.
- **What we steal:**
  - **URI scheme for source/sink** — the killer pattern for the portfolio. `csv:///path/to/file.csv` and `sqlite:///./out.db` is far more elegant than separate `--source-type csv --source-path ...` flags.
  - Declarative incremental modes (`append` / `merge` / `replace`) — design these into v1's contract even if v1 only implements `replace`.

---

## Five concrete design decisions for DataIngest

1. **`src/` layout + library-first, CLI-as-wrapper.** `src/dataingest/` with Typer commands as thin shells over a `Pipeline` class callable from Python. Tests can hit either layer. (sqlite-utils + tablib consensus.)

2. **Adopt ingestr's URI scheme for sources/sinks even in v1.** `dataingest run --source csv:///data/vendor_a.csv --sink sqlite:///./out.db --mapping mappings/vendor_a.yml`. Parse with `urllib.parse`. The YAML mapping describes only *transform/validation*, not connection plumbing. v2+ extension is invisible to existing CLI users.

3. **Plugin discovery: visidata-style naming convention now, entry points later.** v1: `Source` / `Sink` Protocols, with concrete `CsvSource` / `SqliteSink` registered in a `dict` keyed by URI scheme. v2: layer on `importlib.metadata` entry points (`dataingest.sources`) without breaking v1. **Don't reach for `pluggy` until you actually have third-party plugin authors.**

4. **Cleaner chains as named, composable callables — not a DSL.** YAML references cleaner names (`strip`, `parse_currency`, `normalize_phone`); the registry is `dict[str, Callable[[Any], Any]]`. Validation is a **separate** Pydantic model per vendor — don't conflate cleaning and validation. (rows pattern, simplified.)

5. **Dry-run and error log are first-class, not afterthoughts.** Every mutating command takes `--dry-run` that runs the full pipeline through validation and logs planned writes without touching the sink. Errors stream to `errors.jsonl` with `{row_number, source_file, field, value, rule, message}` — greppable, importable, post-mortem-friendly. **This is the single feature that makes a portfolio project look production-aware.**
