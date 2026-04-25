# DataIngest

> Config-driven CSV → SQL ingestion tool with declarative vendor mappings.

DataIngest takes messy tabular files (CSV today, Excel/JSON next), runs them through a YAML-declared validation and cleaning pipeline, and loads them into a SQL database. Onboarding a new vendor format does not mean writing Python — it means writing a YAML file.

## Quick start

```bash
# Install with uv
uv sync

# Validate a vendor mapping
uv run dataingest validate mappings/clay.yml

# Dry-run the first 100 rows of a CSV through the pipeline
uv run dataingest run \
  --source csv:///path/to/clay_2024.csv \
  --sink sqlite:///./out.db \
  --mapping mappings/clay.yml \
  --dry-run --limit 100

# Run for real
uv run dataingest run \
  --source csv:///path/to/clay_2024.csv \
  --sink sqlite:///./out.db \
  --mapping mappings/clay.yml
```

## How it works

```
  ┌────────┐     ┌──────────┐     ┌──────────┐     ┌────────┐
  │ Source │ ──▶ │  Mapper  │ ──▶ │ Validator│ ──▶ │  Sink  │
  └────────┘     └──────────┘     └──────────┘     └────────┘
       │              │                 │              │
   CSV today       column→field      Pydantic       SQLite today
   Excel/JSON      cleaner chain     row models     Postgres/SQL Server next
   next
```

Each stage is pluggable via a small `Protocol`. New formats and backends extend the engine without touching the core.

## Vendor mapping example

```yaml
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
  on_conflict: skip          # skip | replace | error

fields:
  bill_number:
    column: 0
    type: str
    required: true
    cleaners: [strip, upper]

  face_amount:
    column: 9
    type: decimal
    cleaners: [strip, remove_currency_symbols, parse_decimal]
    required: true

  date_due:
    column: 10
    type: date
    cleaners: [parse_date_us]
    required: true
```

## Built-in cleaners

| Name | Effect |
|---|---|
| `strip` | Trim leading/trailing whitespace |
| `upper` / `lower` | Case normalization |
| `remove_extra_whitespace` | Collapse runs of whitespace into single spaces |
| `remove_currency_symbols` | Strip `$`, `£`, `€`, `¥`, commas |
| `parse_decimal` | Parse string → `Decimal` |
| `parse_date_us` | Parse `MM/DD/YYYY` → `date` |
| `parse_date_iso` | Parse `YYYY-MM-DD` → `date` |

Cleaners compose via the `cleaners:` list in the YAML mapping — they run left-to-right.

## CLI

```
dataingest run        Run a full ingestion pipeline
dataingest validate   Validate a YAML mapping file
```

Common flags on `run`:

```
--source     URI    Source URI (e.g. csv:///path/to/file.csv)
--sink       URI    Sink URI (e.g. sqlite:///./out.db)
--mapping    PATH   Path to YAML mapping file
--dry-run           Validate without writing to the sink
--limit      N      Process at most N rows
--errors     PATH   Path for JSONL error log (default: ./errors.jsonl)
```

## Project layout

```
DataIngest/
├── pyproject.toml
├── README.md
├── plan/                  # design docs (read 00-master-plan.md first)
├── mappings/              # vendor YAML mappings
├── src/dataingest/
│   ├── cli.py             # Typer CLI
│   ├── pipeline.py        # the orchestrator
│   ├── config.py          # Pydantic models for the YAML schema
│   ├── cleaners.py        # named cleaner registry
│   ├── errors.py          # error types + JSONL log
│   ├── uri.py             # URI parser
│   ├── sources/           # input adapters (csv, ...)
│   └── sinks/             # output adapters (sqlite, ...)
└── tests/
```

## Development

```bash
uv sync                    # install deps + create venv
uv run pytest              # run tests
uv run ruff check .        # lint
uv run ruff format .       # format
uv run mypy src tests      # type-check
```

## Roadmap

**v1 (current)** — CSV source, SQLite sink, declarative YAML mappings, validation, named cleaner chains, dry-run mode, JSONL error log.

**v2** — Excel and JSON sources, `--upsert` mode, schema inference (`dataingest infer file.csv > mapping.yml`).

**v3** — Postgres + SQL Server sinks, plugin entry points (`importlib.metadata`), run manifest table.

**v4** — File-watcher daemon (drop folder → auto-ingest), notifications.

## Why this exists

Most ETL tools are either too small (a one-off pandas script) or too big (Airbyte, Meltano, dlt). DataIngest sits in between: small enough to read in an afternoon, declarative enough that an analyst can onboard a new vendor without writing Python, and type-safe enough to trust in production.

The first reference implementation is the Clay County (KY) sheriff delinquent tax bill format — a real public-records dataset shaped like every other vendor file in the world: messy CSV, mixed types, currency symbols, inconsistent dates, owner names that may or may not be people.

## License

MIT
