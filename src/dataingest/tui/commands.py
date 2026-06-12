from dataclasses import dataclass
from pathlib import Path

from ..appconfig import AppConfig


@dataclass(frozen=True)
class Command:
    kind: str
    job: str | None = None
    file: str | None = None
    text: str = ""


def parse_command(line: str) -> Command:
    text = line.strip()
    if text.startswith(":"):
        text = text[1:].strip()
    if text in ("q", "quit"):
        return Command(kind="quit")
    if text in ("help", "h", "?"):
        return Command(kind="help")
    parts = text.split()
    if parts and parts[0] == "run":
        return Command(
            kind="run",
            job=parts[1] if len(parts) > 1 else None,
            file=parts[2] if len(parts) > 2 else None,
        )
    return Command(kind="unknown", text=line.strip())


def job_rows(config: AppConfig) -> list[tuple[str, str]]:
    return [(name, job.description or "") for name, job in config.jobs.items()]


def destination_label(config: AppConfig) -> str:
    if config.destination is None:
        return "not configured"
    sink = config.destination.sink
    if "://" in sink:
        scheme = sink.split("://", 1)[0]
        return f"{scheme} database"
    return f"{Path(sink).name} (sqlite)"
