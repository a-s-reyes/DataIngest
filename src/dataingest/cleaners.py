import ast
import re
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

CleanerFn = Callable[[Any], Any]
CleanerFactory = Callable[..., CleanerFn]

REGISTRY: dict[str, CleanerFn] = {}
FACTORY_REGISTRY: dict[str, CleanerFactory] = {}

_CLEANER_CALL_RE = re.compile(r"^(\w+)\s*\((.*)\)\s*$", re.DOTALL)


def register(name: str) -> Callable[[CleanerFn], CleanerFn]:
    def decorator(fn: CleanerFn) -> CleanerFn:
        if name in REGISTRY or name in FACTORY_REGISTRY:
            raise ValueError(f"cleaner {name!r} already registered")
        REGISTRY[name] = fn
        return fn

    return decorator


def register_factory(name: str) -> Callable[[CleanerFactory], CleanerFactory]:
    """Register a cleaner *factory* — a callable that takes config args and
    returns a ``CleanerFn``. Referenced from YAML as ``name(arg1, arg2)``."""

    def decorator(fn: CleanerFactory) -> CleanerFactory:
        if name in REGISTRY or name in FACTORY_REGISTRY:
            raise ValueError(f"cleaner {name!r} already registered")
        FACTORY_REGISTRY[name] = fn
        return fn

    return decorator


def _parse_args(args_text: str) -> tuple[Any, ...]:
    """Parse a cleaner spec's argument text via ``ast.literal_eval``.

    Safe: ``literal_eval`` accepts only Python literals (str, int, float,
    bool, None, tuple, list, dict, set), never code.
    """
    args_text = args_text.strip()
    if not args_text:
        return ()
    try:
        result = ast.literal_eval(f"({args_text},)")
    except (ValueError, SyntaxError) as err:
        raise ValueError(f"malformed cleaner arguments: {args_text!r}") from err
    return result if isinstance(result, tuple) else (result,)


def resolve(spec: str) -> CleanerFn:
    """Resolve a single cleaner spec string into a callable.

    Forms:
      ``"strip"``                        -> zero-arg cleaner from REGISTRY
      ``"truncate(80)"``                 -> factory call from FACTORY_REGISTRY
      ``"regex_replace('\\\\s+', ' ')"`` -> factory with multiple args
    """
    spec = spec.strip()
    m = _CLEANER_CALL_RE.match(spec)
    if not m:
        if spec in REGISTRY:
            return REGISTRY[spec]
        if spec in FACTORY_REGISTRY:
            raise ValueError(f"cleaner {spec!r} requires arguments — use {spec}(...) syntax")
        raise ValueError(f"unknown cleaner: {spec!r}")
    name, args_text = m.group(1), m.group(2)
    if name in FACTORY_REGISTRY:
        return FACTORY_REGISTRY[name](*_parse_args(args_text))
    if name in REGISTRY:
        raise ValueError(f"cleaner {name!r} does not take arguments")
    raise ValueError(f"unknown cleaner: {name!r}")


def validate_spec(spec: str) -> None:
    """Verify a cleaner spec parses and references a known cleaner.

    Used by ``FieldConfig`` to fail-fast at YAML load time without actually
    constructing the cleaner closure.
    """
    spec = spec.strip()
    m = _CLEANER_CALL_RE.match(spec)
    if not m:
        if spec in REGISTRY:
            return
        if spec in FACTORY_REGISTRY:
            raise ValueError(f"cleaner {spec!r} requires arguments — use {spec}(...) syntax")
        raise ValueError(f"unknown cleaner: {spec!r}")
    name, args_text = m.group(1), m.group(2)
    if name in FACTORY_REGISTRY:
        _parse_args(args_text)  # raises on malformed args
        return
    if name in REGISTRY:
        raise ValueError(f"cleaner {name!r} does not take arguments")
    raise ValueError(f"unknown cleaner: {name!r}")


def chain(names: list[str]) -> CleanerFn:
    """Compose a left-to-right cleaner chain from spec strings."""
    fns = [resolve(n) for n in names]

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


@register("parse_int")
def parse_int(value: Any) -> int | None:
    """Parse a string into an ``int``. Strips whitespace and underscores.

    Rejects ``bool`` (a misleading int subclass), floats, and non-numeric
    strings. Empty / ``None`` -> ``None``.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"cannot parse int from bool: {value!r}")
    if isinstance(value, int):
        return value
    s = str(value).strip().replace("_", "")
    if not s:
        return None
    try:
        return int(s)
    except ValueError as err:
        raise ValueError(f"cannot parse int: {value!r}") from err


@register("parse_date_us")
def parse_date_us(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value).strip(), "%m/%d/%Y").date()


@register("parse_date_iso")
def parse_date_iso(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())


@register("parse_datetime_iso")
def parse_datetime_iso(value: Any) -> datetime | None:
    """Parse an ISO 8601 timestamp into a ``datetime``.

    Accepts native ``datetime`` (passes through), ``date`` (promotes to
    midnight), or any ISO 8601 string. The trailing ``Z`` UTC marker is
    supported natively in Python 3.11+.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    return datetime.fromisoformat(str(value).strip())


# --- Parameterized cleaners (factories) ---


@register_factory("regex_replace")
def regex_replace(pattern: str, repl: str) -> CleanerFn:
    """Substitute regex matches. Non-string inputs pass through unchanged.

    In YAML, prefer raw-string syntax to avoid Python escape warnings::

        cleaners: [regex_replace(r'\\s+', ' ')]

    or double-backslashes::

        cleaners: [regex_replace('\\\\s+', ' ')]
    """
    compiled = re.compile(pattern)

    def cleaner(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        return compiled.sub(repl, value)

    return cleaner


@register_factory("remove_chars")
def remove_chars(chars: str) -> CleanerFn:
    """Strip every character in ``chars`` from string inputs."""
    table = str.maketrans("", "", chars)

    def cleaner(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        return value.translate(table)

    return cleaner


@register_factory("truncate")
def truncate(n: int) -> CleanerFn:
    """Cap string length at ``n`` characters."""
    if not isinstance(n, int):
        raise ValueError(f"truncate length must be int, got {type(n).__name__}")
    if n < 0:
        raise ValueError(f"truncate length must be >= 0, got {n}")

    def cleaner(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        return value[:n]

    return cleaner


@register_factory("default_if_empty")
def default_if_empty(default: Any) -> CleanerFn:
    """Replace ``None`` or ``""`` with ``default``."""

    def cleaner(value: Any) -> Any:
        if value is None or value == "":
            return default
        return value

    return cleaner
