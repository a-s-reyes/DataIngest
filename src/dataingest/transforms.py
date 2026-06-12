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


@register("classify")
class Classify:
    def __init__(self, config: dict[str, Any]) -> None:
        self.source = config["source"]
        self.target = config["target"]
        self.rules: list[tuple[list[str], Any]] = []
        for rule in config["rules"]:
            contains = rule["contains"]
            if isinstance(contains, str):
                contains = [contains]
            self.rules.append(([str(c).lower() for c in contains], rule["value"]))
        self._has_default = "default" in config
        self.default = config.get("default")

    def apply(self, record: dict[str, Any]) -> dict[str, Any]:
        text = record.get(self.source)
        haystack = text.lower() if isinstance(text, str) else ""
        for needles, value in self.rules:
            if all(n in haystack for n in needles):
                record[self.target] = value
                return record
        if self._has_default:
            record[self.target] = self.default
            return record
        raise ValueError(f"no classification rule matched {self.source}={text!r}")


@register("lookup")
class Lookup:
    def __init__(self, config: dict[str, Any]) -> None:
        self.source = config["source"]
        self.target = config["target"]
        self.table = config["table"]
        self._has_default = "default" in config
        self.default = config.get("default")

    def apply(self, record: dict[str, Any]) -> dict[str, Any]:
        key = record.get(self.source)
        if key in self.table:
            record[self.target] = self.table[key]
        elif self._has_default:
            record[self.target] = self.default
        else:
            raise ValueError(f"no lookup entry for {self.source}={key!r}")
        return record


load_entry_points("dataingest.transforms", REGISTRY)
