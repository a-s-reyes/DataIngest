"""Plugin discovery via ``importlib.metadata`` entry points.

Third-party packages can ship sources or sinks without forking by declaring
entry points::

    [project.entry-points."dataingest.sources"]
    s3 = "my_pkg.sources:S3Source"

    [project.entry-points."dataingest.sinks"]
    bigquery = "my_pkg.sinks:BigQuerySink"

Discovery happens at import time of ``sources/__init__.py`` and
``sinks/__init__.py``. Built-in entries always win — a third-party plugin
cannot override ``csv``, ``xlsx``, ``sqlite``, or ``postgres``.

Failed plugin loads are silently dropped so a broken third-party package
does not break ``dataingest`` startup. (We could log; we deliberately don't,
to keep the no-args ``dataingest version`` invocation noiseless.)
"""

from __future__ import annotations

from collections.abc import Iterable
from importlib.metadata import entry_points
from typing import Any


def register_entry_points(eps: Iterable[Any], registry: dict[str, type]) -> None:
    """Add discovered entry points to ``registry``. Pure, testable."""
    for ep in eps:
        if ep.name in registry:
            continue
        try:
            cls = ep.load()
        except Exception:
            continue
        registry[ep.name] = cls


def load_entry_points(group: str, registry: dict[str, type]) -> None:
    """Discover ``group`` entry points and register them into ``registry``."""
    try:
        eps = entry_points(group=group)
    except Exception:
        return
    register_entry_points(eps, registry)
