from pathlib import Path

import pytest
from pydantic import BaseModel
from sqlalchemy import create_engine, text

from dataingest.sinks.sqlite import SqliteSink


class _Row(BaseModel):
    id: str
    name: str | None = None
    amount: int | None = None


def test_begin_creates_table(tmp_path: Path):
    db_path = tmp_path / "out.db"
    sink = SqliteSink(str(db_path), {})
    sink.begin(_Row, table="things", primary_key="id")
    sink.close()

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info(things)")).all()
        cols = {row.name: row for row in result}
        assert set(cols) == {"id", "name", "amount"}
        assert cols["id"].pk == 1


def test_write_inserts_rows(tmp_path: Path):
    db_path = tmp_path / "out.db"
    sink = SqliteSink(str(db_path), {})
    sink.begin(_Row, table="things", primary_key="id")
    rows = [_Row(id="A", name="alpha", amount=1), _Row(id="B", name="beta", amount=2)]
    n = sink.write(rows)
    sink.commit()
    sink.close()

    assert n == 2
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM things")).scalar()
        assert count == 2


def test_on_conflict_skip_drops_dupes(tmp_path: Path):
    db_path = tmp_path / "out.db"
    sink = SqliteSink(str(db_path), {})
    sink.begin(_Row, table="things", primary_key="id", on_conflict="skip")
    sink.write([_Row(id="A", name="first")])
    sink.write([_Row(id="A", name="second"), _Row(id="B", name="new")])
    sink.close()

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, name FROM things ORDER BY id")).all()
        assert [(r.id, r.name) for r in rows] == [("A", "first"), ("B", "new")]


def test_unsupported_on_conflict_raises(tmp_path: Path):
    sink = SqliteSink(str(tmp_path / "x.db"), {})
    with pytest.raises(ValueError, match="not supported"):
        sink.begin(_Row, table="x", primary_key="id", on_conflict="replace")


def test_write_before_begin_raises(tmp_path: Path):
    sink = SqliteSink(str(tmp_path / "x.db"), {})
    with pytest.raises(RuntimeError, match="begin"):
        sink.write([_Row(id="A")])
