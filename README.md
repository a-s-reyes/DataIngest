# DataIngest

> Config-driven CSV → SQL ingestion tool with declarative schema mappings.

DataIngest takes messy tabular files (CSV today, Excel/JSON next), runs them through a YAML-declared validation and cleaning pipeline, and loads them into a SQL database. Onboarding a new file format does not mean writing Python — it means writing a YAML file.

It is domain-agnostic by design. The shipped reference mappings cover three different shapes of real engineering data:

- **Flight test telemetry** (`mappings/telemetry.yml`) — sensor channel readings with ISO timestamps
- **Component qualification tests** (`mappings/qualification.yml`) — measured values vs. tolerances, MM/DD/YYYY dates
- **Parts inventory** (`mappings/parts_inventory.yml`) — NSNs, lot codes, condition codes, integer quantities

The same engine handles all three. New shapes — supply-chain feeds, audit logs, financial extracts, lab notebooks, anything tabular — drop in as another YAML file.

## Quick start

```bash
# Install with uv
uv sync

# Validate a mapping
uv run dataingest validate mappings/telemetry.yml

# Dry-run the first 100 rows of a CSV through the pipeline
uv run dataingest run \
  --source csv:///path/to/data.csv \
  --sink sqlite:///./out.db \
  --mapping mappings/telemetry.yml \
  --dry-run --limit 100

# Run for real
uv run dataingest run \
  --source csv:///path/to/data.csv \
  --sink sqlite:///./out.db \
  --mapping mappings/telemetry.yml
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

A mapping declares the source format, the target table, and the per-field cleaning and validation rules. Three example mappings ship in `mappings/`:

### Example 1 — Flight test telemetry

Sensor channel readings exported from a data acquisition system. Wide variety of channels (accelerometers, gyros, temperatures, pressures); ISO-8601 timestamps; decimal values with mixed precision.

```yaml
spec_version: 1
name: flight-test-telemetry
description: Sensor channel readings exported from a flight test data acquisition system.

source:
  format: csv
  encoding: utf-8
  header: true
  delimiter: ","

target:
  table: telemetry_records
  primary_key: record_id
  on_conflict: skip

fields:
  record_id:
    column: 0
    type: str
    required: true
    cleaners: [strip, upper]

  flight_id:
    column: 1
    type: str
    required: true
    cleaners: [strip, upper]

  recorded_at:
    column: 2
    type: str
    required: true
    cleaners: [strip]

  channel:
    column: 3
    type: str
    required: true
    cleaners: [strip, upper]

  value:
    column: 4
    type: decimal
    required: true
    cleaners: [strip, parse_decimal]

  unit:
    column: 5
    type: str
    required: true
    cleaners: [strip]

  quality:
    column: 6
    type: str
    cleaners: [strip, upper]
    default: "OK"
```

### Example 2 — Component qualification test results

One row per measured parameter per qualification run. Pairs `measured_value` against `tolerance` and stores a `PASS`/`FAIL` result. Dates arrive in `MM/DD/YYYY`.

```yaml
spec_version: 1
name: component-qualification-tests
description: Component qualification test results — one row per measured parameter per run.

source:
  format: csv
  encoding: utf-8
  header: true
  delimiter: ","

target:
  table: qualification_results
  primary_key: test_id
  on_conflict: skip

fields:
  test_id:
    column: 0
    type: str
    required: true
    cleaners: [strip, upper]

  part_number:
    column: 1
    type: str
    required: true
    cleaners: [strip, upper]

  run_date:
    column: 2
    type: date
    required: true
    cleaners: [strip, parse_date_us]

  parameter:
    column: 3
    type: str
    required: true
    cleaners: [strip, remove_extra_whitespace, upper]

  measured_value:
    column: 4
    type: decimal
    required: true
    cleaners: [strip, parse_decimal]

  tolerance:
    column: 5
    type: decimal
    required: true
    cleaners: [strip, parse_decimal]

  result:
    column: 6
    type: str
    required: true
    cleaners: [strip, upper]

  technician:
    column: 7
    type: str
    cleaners: [strip, remove_extra_whitespace]
```

### Example 3 — Parts inventory

Parts master export keyed on National Stock Number. Mixes string identifiers, integer quantities, and ISO-8601 audit dates.

```yaml
spec_version: 1
name: parts-inventory
description: Parts master export with stock numbers, lot codes, and condition codes.

source:
  format: csv
  encoding: utf-8
  header: true
  delimiter: ","

target:
  table: parts_inventory
  primary_key: nsn
  on_conflict: skip

fields:
  nsn:
    column: 0
    type: str
    required: true
    cleaners: [strip, upper]

  part_number:
    column: 1
    type: str
    required: true
    cleaners: [strip, upper]

  lot_code:
    column: 2
    type: str
    required: true
    cleaners: [strip, upper]

  description:
    column: 3
    type: str
    cleaners: [strip, remove_extra_whitespace]

  qty_on_hand:
    column: 4
    type: int
    required: true
    cleaners: [strip]

  unit_of_issue:
    column: 5
    type: str
    cleaners: [strip, upper]
    default: "EA"

  condition_code:
    column: 6
    type: str
    required: true
    cleaners: [strip, upper]

  last_audit:
    column: 7
    type: date
    required: true
    cleaners: [strip, parse_date_iso]
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
│   ├── telemetry.yml
│   ├── qualification.yml
│   └── parts_inventory.yml
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
    └── fixtures/          # synthetic CSV samples for each mapping
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
