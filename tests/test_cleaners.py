from datetime import date
from decimal import Decimal

import pytest

from dataingest.cleaners import REGISTRY, chain


def test_strip_string():
    assert REGISTRY["strip"]("  hello  ") == "hello"


def test_strip_passes_non_strings():
    assert REGISTRY["strip"](42) == 42
    assert REGISTRY["strip"](None) is None


def test_upper_lower():
    assert REGISTRY["upper"]("hello") == "HELLO"
    assert REGISTRY["lower"]("HELLO") == "hello"


def test_remove_extra_whitespace():
    assert REGISTRY["remove_extra_whitespace"]("  a   b\tc\nd  ") == "a b c d"


def test_remove_currency_symbols():
    fn = REGISTRY["remove_currency_symbols"]
    assert fn("$1,234.56") == "1234.56"
    assert fn("£999.99") == "999.99"
    # The cleaner targets US-style currency: strips the symbol AND commas.
    # European decimal-comma formats need a different cleaner.
    assert fn("$ 50,000.00 ") == "50000.00"


def test_parse_decimal():
    fn = REGISTRY["parse_decimal"]
    assert fn("123.45") == Decimal("123.45")
    assert fn("  -99.99  ") == Decimal("-99.99")
    assert fn("") is None
    assert fn(None) is None


def test_parse_decimal_invalid():
    with pytest.raises(ValueError):
        REGISTRY["parse_decimal"]("not a number")


def test_parse_date_us():
    assert REGISTRY["parse_date_us"]("8/29/2024") == date(2024, 8, 29)
    assert REGISTRY["parse_date_us"]("") is None


def test_parse_date_iso():
    assert REGISTRY["parse_date_iso"]("2024-08-29") == date(2024, 8, 29)
    assert REGISTRY["parse_date_iso"]("") is None


def test_chain_composes_left_to_right():
    fn = chain(["strip", "upper"])
    assert fn("  hello  ") == "HELLO"


def test_chain_currency_to_decimal():
    fn = chain(["strip", "remove_currency_symbols", "parse_decimal"])
    assert fn(" $1,234.56 ") == Decimal("1234.56")


def test_chain_unknown_cleaner_raises():
    with pytest.raises(ValueError, match="unknown cleaner"):
        chain(["strip", "no_such_cleaner"])
