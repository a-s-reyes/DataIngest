import re
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from ._plugins import load_entry_points


@runtime_checkable
class Transform(Protocol):
    def apply(self, record: dict[str, Any]) -> dict[str, Any]: ...


REGISTRY: dict[str, type] = {}


def register(name: str) -> Callable[[type], type]:
    def decorator(cls: type) -> type:
        if name in REGISTRY:
            raise ValueError(f"transform {name!r} already registered")
        REGISTRY[name] = cls
        return cls

    return decorator


def get(name: str) -> type:
    if name not in REGISTRY:
        raise ValueError(f"no transform registered for {name!r}")
    return REGISTRY[name]


def build(specs: list[dict[str, Any]]) -> list[Transform]:
    built: list[Transform] = []
    for spec in specs:
        name, config = next(iter(spec.items()))
        built.append(get(name)(config or {}))
    return built


@register("split_name")
class SplitName:
    def __init__(self, config: dict[str, Any]) -> None:
        self.source = config["source"]
        into = config["into"]
        self.first_field = into["first"]
        self.last_field = into["last"]
        self.firm_list = [str(s).upper() for s in config.get("firm_list", [])]

    def apply(self, record: dict[str, Any]) -> dict[str, Any]:
        raw = record.get(self.source)
        name = raw.strip() if isinstance(raw, str) else ""
        if not name:
            record[self.first_field] = ""
            record[self.last_field] = ""
            return record
        if any(firm in name.upper() for firm in self.firm_list):
            record[self.first_field] = "."
            record[self.last_field] = name
            return record
        parts = name.split()
        record[self.first_field] = parts[0]
        record[self.last_field] = " ".join(parts[1:])
        return record


_CSZ_RE = re.compile(r"^\s*(.*?)[,\s]+([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)\s*$")


@register("split_city_state_zip")
class SplitCityStateZip:
    def __init__(self, config: dict[str, Any]) -> None:
        self.source = config["source"]
        into = config["into"]
        self.city_field = into["city"]
        self.state_field = into["state"]
        self.zip_field = into["zip"]

    def apply(self, record: dict[str, Any]) -> dict[str, Any]:
        value = record.get(self.source)
        if not isinstance(value, str):
            raise ValueError(f"cannot parse city/state/zip from non-string: {value!r}")
        match = _CSZ_RE.match(value)
        if not match:
            raise ValueError(f"cannot parse city/state/zip: {value!r}")
        record[self.city_field] = match.group(1).strip().rstrip(",").strip()
        record[self.state_field] = match.group(2).upper()
        record[self.zip_field] = match.group(3)
        return record


load_entry_points("dataingest.transforms", REGISTRY)
