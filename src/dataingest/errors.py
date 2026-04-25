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

    Accepts either a ``Path`` (opened for append in ``__enter__`` and closed in
    ``__exit__``) or an already-open ``IO[str]`` file-like (e.g. ``sys.stderr``)
    that the caller owns.

    Use as a context manager either way::

        with JsonlErrorLog(Path("errors.jsonl")) as log:
            log.write(RowError(row_number=42, ...))

        with JsonlErrorLog(sys.stderr) as log:
            log.write(RowError(...))
    """

    def __init__(self, target: Path | IO[str]) -> None:
        if isinstance(target, Path):
            self.path: Path | None = target
            self._fp: IO[str] | None = None
            self._owns_fp = True
        else:
            self.path = None
            self._fp = target
            self._owns_fp = False

    def __enter__(self) -> Self:
        if self._owns_fp:
            assert self.path is not None
            self._fp = self.path.open("a", encoding="utf-8")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._owns_fp and self._fp is not None:
            self._fp.close()
            self._fp = None

    def write(self, error: RowError) -> None:
        if self._fp is None:
            raise RuntimeError("JsonlErrorLog must be used as a context manager")
        self._fp.write(json.dumps(asdict(error), default=str) + "\n")
        self._fp.flush()
