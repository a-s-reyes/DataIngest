import tomllib
from dataclasses import dataclass
from pathlib import Path

from pydantic import BaseModel, Field

CONFIG_FILENAME = "dataingest.toml"

_XLSX_SUFFIXES = {".xlsx", ".xlsm"}


def path_to_source_uri(path: str) -> str:
    if "://" in path:
        return path
    scheme = "xlsx" if Path(path).suffix.lower() in _XLSX_SUFFIXES else "csv"
    return f"{scheme}:///{Path(path).resolve().as_posix()}"


def path_to_sink_uri(path: str) -> str:
    if "://" in path:
        return path
    scheme = "csv" if Path(path).suffix.lower() == ".csv" else "sqlite"
    return f"{scheme}:///{Path(path).resolve().as_posix()}"


class Destination(BaseModel):
    sink: str


class Job(BaseModel):
    mapping: str
    description: str | None = None


class AppConfig(BaseModel):
    destination: Destination | None = None
    jobs: dict[str, Job] = Field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedJob:
    source_uri: str
    sink_uri: str
    mapping: Path


def find_config(start: Path | None = None) -> Path | None:
    candidate = (start or Path.cwd()) / CONFIG_FILENAME
    return candidate if candidate.exists() else None


def load_config(path: Path) -> AppConfig:
    with path.open("rb") as fp:
        data = tomllib.load(fp)
    return AppConfig.model_validate(data)


def resolve_job(config: AppConfig, name: str, source_path: str, base_dir: Path) -> ResolvedJob:
    if name not in config.jobs:
        available = ", ".join(sorted(config.jobs)) or "(none)"
        raise ValueError(f"unknown job {name!r}. Configured jobs: {available}")
    if config.destination is None:
        raise ValueError("no [destination] configured in dataingest.toml")
    job = config.jobs[name]
    sink = config.destination.sink
    if "://" not in sink:
        sink = str((base_dir / sink).resolve())
    return ResolvedJob(
        source_uri=path_to_source_uri(source_path),
        sink_uri=path_to_sink_uri(sink),
        mapping=(base_dir / job.mapping).resolve(),
    )
