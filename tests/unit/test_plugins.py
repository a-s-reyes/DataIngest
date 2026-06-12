from collections.abc import Callable
from dataclasses import dataclass

import pytest

from dataingest._plugins import register_entry_points


@dataclass
class _FakeEntryPoint:
    name: str
    loader: Callable[[], type]

    def load(self) -> type:
        return self.loader()


class _FakeSource:
    pass


class _OtherFakeSource:
    pass


def test_entry_point_registers_new_scheme() -> None:
    registry: dict[str, type] = {}
    register_entry_points([_FakeEntryPoint("s3", lambda: _FakeSource)], registry)
    assert registry == {"s3": _FakeSource}


def test_existing_scheme_is_not_overridden() -> None:
    registry: dict[str, type] = {"csv": _FakeSource}
    register_entry_points([_FakeEntryPoint("csv", lambda: _OtherFakeSource)], registry)
    assert registry["csv"] is _FakeSource


def test_failed_load_is_silently_skipped() -> None:
    def boom() -> type:
        raise ImportError("simulated broken plugin")

    registry: dict[str, type] = {}
    register_entry_points(
        [
            _FakeEntryPoint("broken", boom),
            _FakeEntryPoint("ok", lambda: _FakeSource),
        ],
        registry,
    )
    assert registry == {"ok": _FakeSource}


def test_empty_iterable_is_a_no_op() -> None:
    registry: dict[str, type] = {}
    register_entry_points([], registry)
    assert registry == {}


def test_multiple_plugins_all_register() -> None:
    registry: dict[str, type] = {}
    register_entry_points(
        [
            _FakeEntryPoint("s3", lambda: _FakeSource),
            _FakeEntryPoint("gcs", lambda: _OtherFakeSource),
        ],
        registry,
    )
    assert registry == {"s3": _FakeSource, "gcs": _OtherFakeSource}


def test_load_entry_points_handles_missing_group(monkeypatch: pytest.MonkeyPatch) -> None:
    from dataingest import _plugins

    def raise_(**_kwargs: object) -> object:
        raise RuntimeError("simulated metadata read failure")

    monkeypatch.setattr(_plugins, "entry_points", raise_)
    registry: dict[str, type] = {"csv": _FakeSource}
    _plugins.load_entry_points("dataingest.sources", registry)
    assert registry == {"csv": _FakeSource}


def test_builtin_sources_are_present_after_import() -> None:
    from dataingest.sources import REGISTRY

    assert "csv" in REGISTRY
    assert "xlsx" in REGISTRY


def test_builtin_sinks_are_present_after_import() -> None:
    from dataingest.sinks import REGISTRY

    assert "sqlite" in REGISTRY
    assert "postgres" in REGISTRY
    assert "postgresql" in REGISTRY
