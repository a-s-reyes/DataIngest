import re
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

CleanerFn = Callable[[Any], Any]
REGISTRY: dict[str, CleanerFn] = {}


def register(name: str) -> Callable[[CleanerFn], CleanerFn]:
    def decorator(fn: CleanerFn) -> CleanerFn:
        if name in REGISTRY:
            raise ValueError(f"cleaner {name!r} already registered")
        REGISTRY[name] = fn
        return fn

    return decorator


def chain(names: list[str]) -> CleanerFn:
    """Compose a left-to-right cleaner chain referenced by name."""
    unknown = [n for n in names if n not in REGISTRY]
    if unknown:
        raise ValueError(f"unknown cleaner(s): {unknown}")
    fns = [REGISTRY[n] for n in names]

    def composed(value: Any) -> Any:
        for fn in fns:
            value = fn(value)
        return value

    return composed


@register("strip")
def strip(value: Any) -> Any:
    return value.strip() if isinstance(value, str) else value


@register("upper")
def upper(value: Any) -> Any:
    return value.upper() if isinstance(value, str) else value


@register("lower")
def lower(value: Any) -> Any:
    return value.lower() if isinstance(value, str) else value


_WHITESPACE_RE = re.compile(r"\s+")


@register("remove_extra_whitespace")
def remove_extra_whitespace(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return _WHITESPACE_RE.sub(" ", value).strip()


_CURRENCY_RE = re.compile(r"[$£€¥,]")


@register("remove_currency_symbols")
def remove_currency_symbols(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return _CURRENCY_RE.sub("", value).strip()


@register("parse_decimal")
def parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as err:
        raise ValueError(f"cannot parse decimal: {value!r}") from err


@register("parse_date_us")
def parse_date_us(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value).strip(), "%m/%d/%Y").date()


@register("parse_date_iso")
def parse_date_iso(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())
