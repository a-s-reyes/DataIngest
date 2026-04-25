from collections.abc import Iterator
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Source(Protocol):
    """Emits raw rows from some external source."""

    def rows(self) -> Iterator[dict[str, Any]]: ...
    def close(self) -> None: ...


REGISTRY: dict[str, type] = {}


def register(scheme: str):
    def decorator(cls: type) -> type:
        if scheme in REGISTRY:
            raise ValueError(f"source scheme {scheme!r} already registered")
        REGISTRY[scheme] = cls
        return cls

    return decorator


def get(scheme: str) -> type:
    if scheme not in REGISTRY:
        raise ValueError(f"no source registered for scheme {scheme!r}")
    return REGISTRY[scheme]


from . import csv as _csv  # noqa: F401, E402  -- triggers registration
