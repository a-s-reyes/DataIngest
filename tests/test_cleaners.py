from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from dataingest.cleaners import REGISTRY, chain


def test_strip_string() -> None:
    assert REGISTRY["strip"]("  hello  ") == "hello"


def test_strip_passes_non_strings() -> None:
    assert REGISTRY["strip"](42) == 42
    assert REGISTRY["strip"](None) is None


def test_upper_lower() -> None:
    assert REGISTRY["upper"]("hello") == "HELLO"
    assert REGISTRY["lower"]("HELLO") == "hello"


def test_remove_extra_whitespace() -> None:
    assert REGISTRY["remove_extra_whitespace"]("  a   b\tc\nd  ") == "a b c d"


def test_remove_currency_symbols() -> None:
    fn = REGISTRY["remove_currency_symbols"]
    assert fn("$1,234.56") == "1234.56"
    assert fn("£999.99") == "999.99"
    # The cleaner targets US-style currency: strips the symbol AND commas.
    # European decimal-comma formats need a different cleaner.
    assert fn("$ 50,000.00 ") == "50000.00"


def test_parse_decimal() -> None:
    fn = REGISTRY["parse_decimal"]
    assert fn("123.45") == Decimal("123.45")
    assert fn("  -99.99  ") == Decimal("-99.99")
    assert fn("") is None
    assert fn(None) is None


def test_parse_decimal_invalid() -> None:
    with pytest.raises(ValueError):
        REGISTRY["parse_decimal"]("not a number")


def test_parse_int_basic() -> None:
    fn = REGISTRY["parse_int"]
    assert fn("142") == 142
    assert fn("  142  ") == 142
    assert fn(142) == 142


def test_parse_int_strips_underscore_separators() -> None:
    fn = REGISTRY["parse_int"]
    assert fn("1_000_000") == 1_000_000


def test_parse_int_handles_negative() -> None:
    assert REGISTRY["parse_int"]("-42") == -42


def test_parse_int_empty_returns_none() -> None:
    fn = REGISTRY["parse_int"]
    assert fn("") is None
    assert fn(None) is None
    assert fn("   ") is None


def test_parse_int_rejects_floats() -> None:
    with pytest.raises(ValueError, match="cannot parse int"):
        REGISTRY["parse_int"]("1.5")


def test_parse_int_rejects_non_numeric() -> None:
    with pytest.raises(ValueError, match="cannot parse int"):
        REGISTRY["parse_int"]("abc")


def test_parse_int_rejects_bool() -> None:
    with pytest.raises(ValueError, match="bool"):
        REGISTRY["parse_int"](True)
    with pytest.raises(ValueError, match="bool"):
        REGISTRY["parse_int"](False)


def test_parse_date_us() -> None:
    assert REGISTRY["parse_date_us"]("8/29/2024") == date(2024, 8, 29)
    assert REGISTRY["parse_date_us"]("") is None


def test_parse_date_us_extracts_from_datetime() -> None:
    """Excel/openpyxl returns datetime even for date-only cells; the cleaner
    must drop the time component cleanly."""
    dt = datetime(2026, 4, 12, 14, 22, 1)
    assert REGISTRY["parse_date_us"](dt) == date(2026, 4, 12)


def test_parse_date_iso() -> None:
    assert REGISTRY["parse_date_iso"]("2024-08-29") == date(2024, 8, 29)
    assert REGISTRY["parse_date_iso"]("") is None


def test_parse_date_iso_extracts_from_datetime() -> None:
    dt = datetime(2026, 4, 12, 14, 22, 1)
    assert REGISTRY["parse_date_iso"](dt) == date(2026, 4, 12)


def test_parse_datetime_iso_basic() -> None:
    fn = REGISTRY["parse_datetime_iso"]
    assert fn("2026-04-12T14:22:01") == datetime(2026, 4, 12, 14, 22, 1)
    assert fn("") is None
    assert fn(None) is None


def test_parse_datetime_iso_handles_z_marker() -> None:
    """Python 3.11+ accepts trailing Z natively."""
    fn = REGISTRY["parse_datetime_iso"]
    expected = datetime(2026, 4, 12, 14, 22, 1, 250000, tzinfo=UTC)
    assert fn("2026-04-12T14:22:01.250Z") == expected


def test_parse_datetime_iso_handles_offset() -> None:
    fn = REGISTRY["parse_datetime_iso"]
    expected = datetime(2026, 4, 12, 14, 22, 1, tzinfo=timezone(timedelta(hours=-5)))
    assert fn("2026-04-12T14:22:01-05:00") == expected


def test_parse_datetime_iso_passes_through_native_datetime() -> None:
    fn = REGISTRY["parse_datetime_iso"]
    dt = datetime(2026, 4, 12, 14, 22, 1)
    assert fn(dt) is dt


def test_parse_datetime_iso_promotes_date_to_midnight() -> None:
    fn = REGISTRY["parse_datetime_iso"]
    assert fn(date(2026, 4, 12)) == datetime(2026, 4, 12, 0, 0, 0)


def test_parse_datetime_iso_invalid_raises() -> None:
    with pytest.raises(ValueError):
        REGISTRY["parse_datetime_iso"]("not a timestamp")


def test_chain_composes_left_to_right() -> None:
    fn = chain(["strip", "upper"])
    assert fn("  hello  ") == "HELLO"


def test_chain_currency_to_decimal() -> None:
    fn = chain(["strip", "remove_currency_symbols", "parse_decimal"])
    assert fn(" $1,234.56 ") == Decimal("1234.56")


def test_chain_unknown_cleaner_raises() -> None:
    with pytest.raises(ValueError, match="unknown cleaner"):
        chain(["strip", "no_such_cleaner"])


# --- Parameterized cleaners (T2.3) ---


def test_regex_replace_substitutes_pattern() -> None:
    # Use raw-string literal in the spec so Python doesn't warn on `\s`.
    fn = chain([r"regex_replace(r'\s+', ' ')"])
    assert fn("a   b\tc\nd") == "a b c d"


def test_regex_replace_passes_non_strings() -> None:
    fn = chain([r"regex_replace('x', 'y')"])
    assert fn(42) == 42
    assert fn(None) is None


def test_remove_chars_strips_specified_chars() -> None:
    fn = chain(["remove_chars(':;,')"])
    assert fn("a:b;c,d") == "abcd"


def test_truncate_caps_string_length() -> None:
    fn = chain(["truncate(5)"])
    assert fn("hello world") == "hello"
    assert fn("hi") == "hi"


def test_truncate_passes_non_strings() -> None:
    fn = chain(["truncate(3)"])
    assert fn(123) == 123


def test_truncate_rejects_non_int() -> None:
    with pytest.raises(ValueError, match="truncate length must be int"):
        chain(["truncate('five')"])


def test_truncate_rejects_negative() -> None:
    with pytest.raises(ValueError, match=">= 0"):
        chain(["truncate(-1)"])


def test_default_if_empty_replaces_none_and_empty() -> None:
    fn = chain(["default_if_empty('OK')"])
    assert fn(None) == "OK"
    assert fn("") == "OK"
    assert fn("PASS") == "PASS"


def test_default_if_empty_supports_non_string_default() -> None:
    fn = chain(["default_if_empty(0)"])
    assert fn(None) == 0
    assert fn("") == 0


def test_chain_mixes_zero_arg_and_factory_cleaners() -> None:
    fn = chain(["strip", r"regex_replace(r'\s+', '_')", "upper", "truncate(8)"])
    assert fn("  hello   world  ") == "HELLO_WO"


def test_factory_cleaner_without_args_raises() -> None:
    with pytest.raises(ValueError, match="requires arguments"):
        chain(["truncate"])


def test_zero_arg_cleaner_called_with_args_raises() -> None:
    with pytest.raises(ValueError, match="does not take arguments"):
        chain(["strip(' ')"])


def test_malformed_args_raises_clear_error() -> None:
    with pytest.raises(ValueError, match="malformed cleaner arguments"):
        chain(["truncate(not a number)"])
