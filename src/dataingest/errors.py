import json
from dataclasses import asdict, dataclass
from pathlib import Path
from types import TracebackType
from typing import IO, Self


class DataIngestError(Exception):
    """Base class for all DataIngest errors."""


class MappingError(DataIngestError):
    """Raised when a YAML mapping is invalid."""


class RowValidationError(DataIngestError):
    """Raised when a single row fails validation."""


@dataclass(frozen=True)
class RowError:
    row_number: int
    source_file: str
    field: str | None
    value: object | None
    rule: str
    message: str


class JsonlErrorLog:
    """Append-only JSONL error log. One JSON object per failed row.

    Use as a context manager:

        with JsonlErrorLog(Path("errors.jsonl")) as log:
            log.write(RowError(row_number=42, ...))
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fp: IO[str] | None = None

    def __enter__(self) -> Self:
        self._fp = self.path.open("a", encoding="utf-8")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._fp is not None:
            self._fp.close()
            self._fp = None

    def write(self, error: RowError) -> None:
        if self._fp is None:
            raise RuntimeError("JsonlErrorLog must be used as a context manager")
        self._fp.write(json.dumps(asdict(error), default=str) + "\n")
