from pathlib import Path

import pytest

from dataingest.config import Mapping
from dataingest.errors import MappingError
from dataingest.transforms import Transform, build


def _split_name(firm_list: list[str] | None = None) -> Transform:
    config: dict[str, object] = {"source": "owner", "into": {"first": "F", "last": "L"}}
    if firm_list is not None:
        config["firm_list"] = firm_list
    return build([{"split_name": config}])[0]


def test_split_name_individual() -> None:
    rec = _split_name().apply({"owner": "John Smith"})
    assert rec["F"] == "John"
    assert rec["L"] == "Smith"


def test_split_name_multi_token_last() -> None:
    rec = _split_name().apply({"owner": "Mary Anne Van Der Berg"})
    assert rec["F"] == "Mary"
    assert rec["L"] == "Anne Van Der Berg"


def test_split_name_firm_is_not_split() -> None:
    rec = _split_name(firm_list=["LLC", "INC"]).apply({"owner": "Acme Holdings LLC"})
    assert rec["F"] == "."
    assert rec["L"] == "Acme Holdings LLC"


def test_split_name_empty() -> None:
    rec = _split_name().apply({"owner": None})
    assert rec["F"] == ""
    assert rec["L"] == ""


def _split_csz() -> Transform:
    return build(
        [
            {
                "split_city_state_zip": {
                    "source": "csz",
                    "into": {"city": "C", "state": "S", "zip": "Z"},
                }
            }
        ]
    )[0]


def test_split_city_state_zip_with_comma() -> None:
    rec = _split_csz().apply({"csz": "GLASGOW, KY 42141"})
    assert (rec["C"], rec["S"], rec["Z"]) == ("GLASGOW", "KY", "42141")


def test_split_city_state_zip_without_comma() -> None:
    rec = _split_csz().apply({"csz": "BOWLING GREEN KY 42101"})
    assert (rec["C"], rec["S"], rec["Z"]) == ("BOWLING GREEN", "KY", "42101")


def test_split_city_state_zip_invalid_raises() -> None:
    with pytest.raises(ValueError, match="cannot parse city/state/zip"):
        _split_csz().apply({"csz": "not an address"})


def _classify() -> Transform:
    return build(
        [
            {
                "classify": {
                    "source": "Description",
                    "target": "Type",
                    "rules": [
                        {"contains": ["fire", "acre"], "value": "FIRE_ACRE"},
                        {"contains": "real", "value": "REAL_ESTATE"},
                    ],
                    "default": "OTHER",
                }
            }
        ]
    )[0]


def test_classify_all_contains_must_match() -> None:
    rec = _classify().apply({"Description": "Fire District Acreage"})
    assert rec["Type"] == "FIRE_ACRE"


def test_classify_single_contains() -> None:
    rec = _classify().apply({"Description": "Real Estate Tax"})
    assert rec["Type"] == "REAL_ESTATE"


def test_classify_default_when_no_match() -> None:
    rec = _classify().apply({"Description": "Mystery Levy"})
    assert rec["Type"] == "OTHER"


def test_classify_no_match_no_default_raises() -> None:
    transform = build(
        [{"classify": {"source": "d", "target": "t", "rules": [{"contains": "x", "value": "X"}]}}]
    )[0]
    with pytest.raises(ValueError, match="no classification rule matched"):
        transform.apply({"d": "nope"})


def test_lookup_hit_and_default() -> None:
    transform = build(
        [{"lookup": {"source": "t", "target": "id", "table": {"A": 1, "B": 2}, "default": 0}}]
    )[0]
    assert transform.apply({"t": "A"})["id"] == 1
    assert transform.apply({"t": "Z"})["id"] == 0


def test_lookup_miss_no_default_raises() -> None:
    transform = build([{"lookup": {"source": "t", "target": "id", "table": {"A": 1}}}])[0]
    with pytest.raises(ValueError, match="no lookup entry"):
        transform.apply({"t": "Z"})


def test_unknown_transform_rejected(tmp_path: Path) -> None:
    p = tmp_path / "m.yml"
    p.write_text(
        """
spec_version: 1
name: x
source: { format: csv }
transforms:
  - nope: {}
target: { table: t, primary_key: id }
fields:
  id: { column: 0, type: str, required: true }
""",
        encoding="utf-8",
    )
    with pytest.raises(MappingError, match="unknown transform"):
        Mapping.from_yaml(p)
