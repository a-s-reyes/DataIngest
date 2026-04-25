# DataIngest

[![CI](https://github.com/a-s-reyes/DataIngest/actions/workflows/ci.yml/badge.svg)](https://github.com/a-s-reyes/DataIngest/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-%E2%89%A580%25-brightgreen)](https://github.com/a-s-reyes/DataIngest/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

> Config-driven CSV → SQL ingestion tool with declarative schema mappings.

DataIngest takes messy tabular files (CSV today, Excel/JSON next), runs them through a YAML-declared validation and cleaning pipeline, and loads them into a SQL database. Onboarding a new file format does not mean writing Python — it means writing a YAML file.

For implementation details and how to extend the engine, read [ARCHITECTURE.md](ARCHITECTURE.md).

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

## Mappings

A mapping is a YAML file describing one input shape: the source format, the target table, and the per-field type, required-flag, default, and cleaner chain. The pipeline reads the mapping, builds a Pydantic row model from it, runs each row through the declared cleaners, validates against the model, and writes the survivors to the sink. Failed rows go to the JSONL error log with the offending field, value, and rule.

Onboarding a new input shape means writing a new YAML file and pointing `--mapping` at it. No Python changes, no recompile, no plugin registration.

Three reference mappings ship in [`mappings/`](mappings/):

| File | Shape | Notable fields |
|---|---|---|
| `telemetry.yml` | Flight test telemetry | ISO-8601 timestamps, decimal sensor values, channel name normalization |
| `qualification.yml` | Component qualification test results | MM/DD/YYYY dates, measured-value vs. tolerance pairs, PASS/FAIL result |
| `parts_inventory.yml` | Parts master / NSN inventory | String identifiers, integer quantities, ISO-8601 audit dates |

Each has a matching synthetic 20-row CSV under `tests/fixtures/`. Read any of the YAML files for the full shape — they're the schema documentation.

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
dataingest infer      Sniff a CSV and emit a starter mapping YAML
dataingest tables     Inspect a sink: list tables and recent run history
dataingest version    Print the DataIngest version
```

### Demo session

```text
$ dataingest infer data.csv -o mappings/data.yml
wrote mappings/data.yml (4 fields)

$ dataingest validate mappings/data.yml
OK: mappings/data.yml (name=data, fields=4)

$ dataingest run --source csv:///./data.csv --sink sqlite:///./out.db --mapping mappings/data.yml
rows_in=1247 ok=1245 failed=2 chunks=2 run_id=6b54cec8-079e-4dd9-864e-5af2e6f0d4bc

$ dataingest tables sqlite:///./out.db
sink: sqlite:///./out.db

  TABLE              ROWS
  ----------------  -----
  _dataingest_runs      1
  data               1245

recent runs (last 1):
  2026-04-25T...  6b54cec8  partial   in=1247 ok=1245 failed=2  (data)
```

Common flags on `run`:

```
--source       URI    Source URI (e.g. csv:///path/to/file.csv, xlsx://..., postgres://...)
--sink         URI    Sink URI (e.g. sqlite:///./out.db, postgres://user:pass@host/db)
--mapping      PATH   Path to YAML mapping file
--dry-run             Validate without writing to the sink
--limit        N      Process at most N rows
--errors       PATH   Path for JSONL error log, or '-' for stderr (default: ./errors.jsonl)
--chunk-size   N      Rows per sink batch flush (default: 1000, min: 1)
-v / --verbose        Increase log verbosity. -v info, -vv debug. Default: warnings only.
-q / --quiet          Suppress the summary line; rely on the exit code.
```

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Clean run, or vacuous success (no rows arrived) |
| `1` | Preflight error — bad mapping, malformed URI, missing optional dep |
| `2` | Partial failure — some rows landed, others routed to `errors.jsonl` |
| `3` | Total failure — rows arrived but none survived validation |

Shell-script integrators can branch on these without parsing stdout.

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
uv sync --all-extras --dev    # install deps + optional extras (xlsx, postgres)
uv run pytest                 # run tests (postgres tests skip without DB)
uv run ruff check .           # lint
uv run ruff format .          # format
uv run mypy src tests         # type-check (strict)
```

### Running the postgres test suite locally

The postgres integration tests are skipped unless `DATAINGEST_TEST_POSTGRES_URL` is set. To run them:

```bash
# Spin up a throwaway postgres
docker run -d --name dataingest-pg \
  -e POSTGRES_USER=dataingest \
  -e POSTGRES_PASSWORD=dataingest \
  -e POSTGRES_DB=dataingest_test \
  -p 5432:5432 postgres:16

# Point the test suite at it
export DATAINGEST_TEST_POSTGRES_URL=postgres://dataingest:dataingest@localhost:5432/dataingest_test

uv run pytest tests/test_postgres_sink.py -v
```

CI runs the full suite (sqlite + postgres) on every push via a `services: postgres` container.

## Roadmap

Production-readiness sequencing lives in `plan/03-roadmap.md` (gitignored — local design notes). Current state:

**Tier 1 — production-ready credibility floor (shipped):** chunked streaming writes, GitHub Actions CI, mypy strict clean, coverage gate at 80%, run manifest table.

**Tier 2 — practical for real work (in progress):** xlsx source ✅, datetime field type ✅, parameterized cleaners ✅, parse_int cleaner ✅, postgres sink with full upsert ✅, mssql sink (next), CLI ergonomics (next).

**Tier 3 — portfolio polish (planned):** ARCHITECTURE.md, schema inference, `tables` inspect command, plugin entry points.

Permanently out of scope: file-watcher daemon, Slack/email notifications, web UI, orchestration, streaming.

## Why this exists

Most ETL tools are either too small (a one-off pandas script) or too big (Airbyte, Meltano, dlt). DataIngest sits in between: small enough to read in an afternoon, declarative enough that someone who isn't a Python developer can onboard a new file format without writing code, and type-safe enough to trust in production.

The patterns transfer directly to aerospace and defense work — telemetry archives, sensor calibration logs, supply-chain part feeds, sustainment records, audit and compliance pipelines — and to anything else where a messy file has to land in a database with the right types, the right constraints, and a clean error trail.

## License

MIT
