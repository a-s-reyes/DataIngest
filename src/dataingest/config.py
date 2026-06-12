from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

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
    group_by: str | None = None


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


class ChildFieldConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str | None = Field(default=None, alias="from")
    value: Any = None

    @model_validator(mode="after")
    def _one_source(self) -> Self:
        if (self.from_ is not None) == (self.value is not None):
            raise ValueError("child field must set exactly one of 'from' or 'value'")
        return self


class ChildConfig(BaseModel):
    table: str
    foreign_key: str
    fields: dict[str, ChildFieldConfig]
    for_each_row: bool = False


class Mapping(BaseModel):
    spec_version: int
    name: str
    description: str | None = None
    source: SourceConfig
    target: TargetConfig
    fields: dict[str, FieldConfig]
    transforms: list[dict[str, Any]] = Field(default_factory=list)
    children: list[ChildConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_primary_key_exists(self) -> Self:
        if self.target.primary_key not in self.fields:
            raise ValueError(f"primary_key {self.target.primary_key!r} not declared in fields")
        return self

    @model_validator(mode="after")
    def _validate_children(self) -> Self:
        for child in self.children:
            for col, cf in child.fields.items():
                if cf.from_ is not None and cf.from_ not in self.fields:
                    raise ValueError(
                        f"child field {col!r} references unknown parent field {cf.from_!r}"
                    )
        return self

    @model_validator(mode="after")
    def _validate_group_by(self) -> Self:
        gb = self.source.group_by
        if gb is not None and gb not in self.fields:
            raise ValueError(f"source.group_by {gb!r} not declared in fields")
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
