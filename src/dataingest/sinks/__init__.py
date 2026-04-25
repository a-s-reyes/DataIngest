from collections.abc import Callable, Iterable
from typing import Protocol, runtime_checkable

from pydantic import BaseModel

from ..manifest import RunManifest


@runtime_checkable
class Sink(Protocol):
    """Writes validated rows to some destination."""

    def begin(
        self,
        model: type[BaseModel],
        *,
        table: str,
        primary_key: str,
        on_conflict: str = "error",
    ) -> None: ...
    def write(self, rows: Iterable[BaseModel]) -> int: ...
    def write_manifest(self, manifest: RunManifest) -> None: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


REGISTRY: dict[str, type] = {}


def register(scheme: str) -> Callable[[type], type]:
    def decorator(cls: type) -> type:
        if scheme in REGISTRY:
            raise ValueError(f"sink scheme {scheme!r} already registered")
        REGISTRY[scheme] = cls
        return cls

    return decorator


def get(scheme: str) -> type:
    if scheme not in REGISTRY:
        raise ValueError(f"no sink registered for scheme {scheme!r}")
    return REGISTRY[scheme]


from . import sqlite as _sqlite  # noqa: F401, E402  -- triggers registration
