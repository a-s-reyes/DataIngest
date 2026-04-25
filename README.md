# DataIngest

[![CI](https://github.com/a-s-reyes/DataIngest/actions/workflows/ci.yml/badge.svg)](https://github.com/a-s-reyes/DataIngest/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-%E2%89%A580%25-brightgreen)](https://github.com/a-s-reyes/DataIngest/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

> Config-driven CSV/Excel → SQL ingestion tool with declarative schema mappings.

DataIngest takes messy tabular files, runs them through a YAML-declared validation and cleaning pipeline, and loads them into a SQL database. Onboarding a new file format means writing a YAML file, not Python code.

For implementation details, the four-stage pipeline, invariants, and how to extend the engine, see **[ARCHITECTURE.md](ARCHITECTURE.md)**. For version history, see **[CHANGELOG.md](CHANGELOG.md)**.

## Install

Requires [`uv`](https://docs.astral.sh/uv/) and Python 3.11+.

```bash
git clone https://github.com/a-s-reyes/DataIngest.git
cd DataIngest
uv sync --all-extras --dev
```

Verify:

```bash
uv run pytest                 # 187 tests; postgres ones skip without DB
uv run dataingest --help
```

## Quick start

```bash
# Sniff a file and emit a starter mapping
uv run dataingest infer mydata.csv -o mappings/mydata.yml

# Edit the YAML to taste, then ingest
uv run dataingest run \
  --source csv:///path/to/mydata.csv \
  --sink sqlite:///./out.db \
  --mapping mappings/mydata.yml

# Confirm what landed
uv run dataingest tables sqlite:///./out.db
```

`uv run dataingest --help` lists every command and flag.

## License

MIT — see [LICENSE](LICENSE).
