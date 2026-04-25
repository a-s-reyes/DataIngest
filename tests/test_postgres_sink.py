"""Integration tests for PostgresSink.

Skipped unless ``DATAINGEST_TEST_POSTGRES_URL`` is set in the environment.
CI sets it via the ``services: postgres`` container; locally, run a Docker
postgres and export the URL — see README for the exact recipe.

Each test runs against a unique table name so reruns are clean. The shared
``_dataingest_runs`` manifest table is dropped between tests so manifest
assertions stay deterministic.
"""

import os
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import BaseModel
from sqlalchemy import create_engine, text

from dataingest.config import Mapping
from dataingest.pipeline import Pipeline
from dataingest.sinks.postgres import PostgresSink
from dataingest.uri import parse as parse_uri

PG_URL = os.environ.get("DATAINGEST_TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(
    not PG_URL,
    reason="DATAINGEST_TEST_POSTGRES_URL not set; skipping postgres integration tests",
)


@pytest.fixture
def pg_path() -> str:
    assert PG_URL is not None
    return parse_uri(PG_URL).path


@pytest.fixture
def pg_table(pg_path: str) -> Iterator[str]:
    name = f"test_{uuid.uuid4().hex[:12]}"
    yield name
    engine = create_engine(f"postgresql+psycopg://{pg_path}", future=True)
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{name}"'))
        conn.execute(text("DROP TABLE IF EXISTS _dataingest_runs"))
    engine.dispose()


class _Row(BaseModel):
    id: str
    name: str | None = None
    amount: int | None = None


def test_begin_creates_table(pg_path: str, pg_table: str) -> None:
    sink = PostgresSink(pg_path, {})
    sink.begin(_Row, table=pg_table, primary_key="id")
    sink.close()

    engine = create_engine(f"postgresql+psycopg://{pg_path}", future=True)
    with engine.connect() as conn:
        cols = (
            conn.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name = :name"),
                {"name": pg_table},
            )
            .scalars()
            .all()
        )
    assert set(cols) == {"id", "name", "amount"}


def test_write_inserts_rows(pg_path: str, pg_table: str) -> None:
    sink = PostgresSink(pg_path, {})
    sink.begin(_Row, table=pg_table, primary_key="id")
    n = sink.write([_Row(id="A", name="alpha", amount=1), _Row(id="B", name="beta", amount=2)])
    sink.commit()
    sink.close()

    assert n == 2
    engine = create_engine(f"postgresql+psycopg://{pg_path}", future=True)
    with engine.connect() as conn:
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{pg_table}"')).scalar()
    assert count == 2


def test_skip_mode_drops_duplicates(pg_path: str, pg_table: str) -> None:
    sink = PostgresSink(pg_path, {})
    sink.begin(_Row, table=pg_table, primary_key="id", on_conflict="skip")
    sink.write([_Row(id="A", name="first")])
    sink.write([_Row(id="A", name="second"), _Row(id="B", name="new")])
    sink.close()

    engine = create_engine(f"postgresql+psycopg://{pg_path}", future=True)
    with engine.connect() as conn:
        rows = conn.execute(text(f'SELECT id, name FROM "{pg_table}" ORDER BY id')).all()
    assert [(r.id, r.name) for r in rows] == [("A", "first"), ("B", "new")]


def test_replace_mode_overwrites_duplicates(pg_path: str, pg_table: str) -> None:
    sink = PostgresSink(pg_path, {})
    sink.begin(_Row, table=pg_table, primary_key="id", on_conflict="replace")
    sink.write([_Row(id="A", name="first", amount=1)])
    sink.write([_Row(id="A", name="updated", amount=99)])
    sink.close()

    engine = create_engine(f"postgresql+psycopg://{pg_path}", future=True)
    with engine.connect() as conn:
        row = conn.execute(text(f'SELECT id, name, amount FROM "{pg_table}"')).first()
    assert row is not None
    assert row.name == "updated"
    assert row.amount == 99


def test_replace_mode_inserts_new_rows_too(pg_path: str, pg_table: str) -> None:
    """``replace`` mode should still insert non-conflicting rows."""
    sink = PostgresSink(pg_path, {})
    sink.begin(_Row, table=pg_table, primary_key="id", on_conflict="replace")
    sink.write([_Row(id="A", name="alpha")])
    sink.write([_Row(id="B", name="beta"), _Row(id="C", name="gamma")])
    sink.close()

    engine = create_engine(f"postgresql+psycopg://{pg_path}", future=True)
    with engine.connect() as conn:
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{pg_table}"')).scalar()
    assert count == 3


def test_unsupported_conflict_mode_rejected(pg_path: str, pg_table: str) -> None:
    sink = PostgresSink(pg_path, {})
    with pytest.raises(ValueError, match="not supported"):
        sink.begin(_Row, table=pg_table, primary_key="id", on_conflict="bogus")


def test_e2e_through_pipeline(pg_path: str, pg_table: str, tmp_path: Path) -> None:
    """Full pipeline: csv source -> postgres sink, including manifest write."""
    csv = tmp_path / "data.csv"
    csv.write_text("id,name,amount\nA,alpha,1\nB,beta,2\nC,gamma,3\n", encoding="utf-8")

    mapping_yml = tmp_path / "m.yml"
    mapping_yml.write_text(
        f"""
spec_version: 1
name: pg-e2e
source: {{ format: csv }}
target:
  table: {pg_table}
  primary_key: id
fields:
  id:
    column: id
    type: str
    required: true
    cleaners: [strip]
  name:
    column: name
    type: str
    cleaners: [strip]
  amount:
    column: amount
    type: int
    cleaners: [strip, parse_int]
""",
        encoding="utf-8",
    )

    pipeline = Pipeline(
        source_uri=f"csv:///{csv.as_posix()}",
        sink_uri=f"postgres://{pg_path}",
        mapping=Mapping.from_yaml(mapping_yml),
        error_log=tmp_path / "errors.jsonl",
    )
    result = pipeline.run()
    assert result.rows_in == 3
    assert result.rows_ok == 3
    assert result.rows_failed == 0

    engine = create_engine(f"postgresql+psycopg://{pg_path}", future=True)
    with engine.connect() as conn:
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{pg_table}"')).scalar()
        assert count == 3
        manifest = conn.execute(
            text("SELECT run_id, status, mapping_name FROM _dataingest_runs WHERE run_id = :rid"),
            {"rid": result.run_id},
        ).first()
    assert manifest is not None
    assert manifest.status == "success"
    assert manifest.mapping_name == "pg-e2e"


def test_postgresql_uri_alias_resolves_to_same_sink() -> None:
    """``postgres://`` and ``postgresql://`` should both register the same sink."""
    from dataingest.sinks import REGISTRY

    assert REGISTRY["postgres"] is REGISTRY["postgresql"]
    assert REGISTRY["postgres"].__name__ == "PostgresSink"
