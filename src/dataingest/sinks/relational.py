from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel


@dataclass(frozen=True)
class ChildField:
    kind: str
    ref: Any
    py_type: type


@dataclass(frozen=True)
class ChildSpec:
    table: str
    foreign_key: str
    fk_type: type
    fields: dict[str, ChildField]
    for_each_row: bool = False


@dataclass
class RelationalRow:
    parent: BaseModel
    children: dict[str, list[dict[str, Any]]]
