from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, Field, model_validator

from .cleaners import validate_spec
from .errors import MappingError
from .transforms import REGISTRY as TRANSFORM_REGISTRY

SourceFormat = Literal["csv", "xlsx"]
FieldType = Literal["str", "int", "decimal", "date", "datetime", "bool"]
ConflictMode = Literal["skip", "replace", "error"]


class SourceConfig(BaseModel):
    format: SourceFormat
    encoding: str = "utf-8"
    header: bool = True
    delimiter: str = ","


class TargetConfig(BaseModel):
    table: str
    primary_key: str
    on_conflict: ConflictMode = "skip"


class FieldConfig(BaseModel):
    column: int | str | None = None
    type: FieldType = "str"
    required: bool = False
    default: Any = None
    transient: bool = False
    cleaners: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_cleaners(self) -> Self:
        errors: list[str] = []
        for spec in self.cleaners:
            try:
                validate_spec(spec)
            except ValueError as err:
                errors.append(str(err))
        if errors:
            raise ValueError("; ".join(errors))
        return self


class Mapping(BaseModel):
    spec_version: int
    name: str
    description: str | None = None
    source: SourceConfig
    target: TargetConfig
    fields: dict[str, FieldConfig]
    transforms: list[dict[str, Any]] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_primary_key_exists(self) -> Self:
        if self.target.primary_key not in self.fields:
            raise ValueError(f"primary_key {self.target.primary_key!r} not declared in fields")
        return self

    @model_validator(mode="after")
    def _validate_transforms(self) -> Self:
        for spec in self.transforms:
            if len(spec) != 1:
                raise ValueError(f"each transform must be a single-key mapping, got {spec!r}")
            name = next(iter(spec))
            if name not in TRANSFORM_REGISTRY:
                raise ValueError(f"unknown transform: {name!r}")
        return self

    @classmethod
    def from_yaml(cls, path: Path) -> "Mapping":
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as err:
            raise MappingError(f"invalid YAML in {path}: {err}") from err
        if not isinstance(data, dict):
            raise MappingError(f"{path}: top-level YAML must be a mapping")
        try:
            return cls.model_validate(data)
        except Exception as err:
            raise MappingError(f"{path}: {err}") from err
