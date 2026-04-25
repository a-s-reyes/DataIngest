# DataIngest

> Config-driven CSV → SQL ingestion tool with declarative schema mappings.

DataIngest takes messy tabular files (CSV today, Excel/JSON next), runs them through a YAML-declared validation and cleaning pipeline, and loads them into a SQL database. Onboarding a new file format does not mean writing Python — it means writing a YAML file.

It is domain-agnostic by design: the same engine handles sensor calibration records, supply-chain part feeds, telemetry exports, audit logs, financial extracts, and anything else shaped like a table.

## Quick start

```bash
# Install with uv
uv sync

# Validate a mapping
uv run dataingest validate mappings/your_mapping.yml

# Dry-run the first 100 rows of a CSV through the pipeline
uv run dataingest run \
  --source csv:///path/to/data.csv \
  --sink sqlite:///./out.db \
  --mapping mappings/your_mapping.yml \
  --dry-run --limit 100

# Run for real
uv run dataingest run \
  --source csv:///path/to/data.csv \
  --sink sqlite:///./out.db \
  --mapping mappings/your_mapping.yml
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

## Mapping example

A mapping declares the source format, the target table, and the per-field cleaning and validation rules. The example below is a sensor calibration export — the same shape works for any tabular feed.

```yaml
spec_version: 1
name: sensor-calibration-v1
description: Vibration sensor calibration records, ISO-8601 dates

source:
  format: csv
  encoding: utf-8
  header: true
  delimiter: ","

target:
  table: calibration_records
  primary_key: record_id
  on_conflict: skip          # skip | replace | error

fields:
  record_id:
    column: 0
    type: str
    required: true
    cleaners: [strip, upper]

  sensor_id:
    column: 1
    type: str
    required: true
    cleaners: [strip, upper]

  calibration_date:
    column: 2
    type: date
    cleaners: [parse_date_iso]
    required: true

  measured_value:
    column: 3
    type: decimal
    cleaners: [strip, parse_decimal]
    required: true

  technician:
    column: 4
    type: str
    cleaners: [strip, remove_extra_whitespace]
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
├── mappings/              # YAML mapping files
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

Most ETL tools are either too small (a one-off pandas script) or too big (Airbyte, Meltano, dlt). DataIngest sits in between: small enough to read in an afternoon, declarative enough that someone who isn't a Python developer can onboard a new file format without writing code, and type-safe enough to trust in production.

The patterns transfer directly to aerospace and defense work — telemetry archives, sensor calibration logs, supply-chain part feeds, sustainment records, audit and compliance pipelines — and to anything else where a messy file has to land in a database with the right types, the right constraints, and a clean error trail.

## License

MIT
