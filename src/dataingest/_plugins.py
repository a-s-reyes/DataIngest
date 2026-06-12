from __future__ import annotations

from collections.abc import Iterable
from importlib.metadata import entry_points
from typing import Any


def register_entry_points(eps: Iterable[Any], registry: dict[str, type]) -> None:
    for ep in eps:
        if ep.name in registry:
            continue
        try:
            cls = ep.load()
        except Exception:
            continue
        registry[ep.name] = cls


def load_entry_points(group: str, registry: dict[str, type]) -> None:
    try:
        eps = entry_points(group=group)
    except Exception:
        return
    register_entry_points(eps, registry)
