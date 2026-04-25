from pathlib import Path
from typing import Annotated

import typer

from . import __version__
from .config import Mapping
from .errors import MappingError
from .pipeline import Pipeline

app = typer.Typer(
    name="dataingest",
    help="Config-driven CSV → SQL ingestion tool.",
    no_args_is_help=True,
    add_completion=False,
)


@app.command()
def run(
    source: Annotated[str, typer.Option(help="Source URI, e.g. csv:///path/to/file.csv")],
    sink: Annotated[str, typer.Option(help="Sink URI, e.g. sqlite:///./out.db")],
    mapping: Annotated[Path, typer.Option(help="Path to YAML mapping file")],
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Validate without writing")] = False,
    limit: Annotated[int | None, typer.Option(help="Process at most N rows")] = None,
    errors: Annotated[
        Path | None,
        typer.Option(help="Path to JSONL error log (default: ./errors.jsonl)"),
    ] = None,
) -> None:
    """Run a full ingestion pipeline."""
    try:
        m = Mapping.from_yaml(mapping)
    except MappingError as err:
        typer.echo(f"error: {err}", err=True)
        raise typer.Exit(code=1) from err

    pipeline = Pipeline(
        source_uri=source,
        sink_uri=sink,
        mapping=m,
        dry_run=dry_run,
        limit=limit,
        error_log=errors,
    )
    result = pipeline.run()
    typer.echo(f"rows_in={result.rows_in} ok={result.rows_ok} failed={result.rows_failed}")


@app.command()
def validate(
    mapping: Annotated[Path, typer.Argument(help="Path to YAML mapping file")],
) -> None:
    """Validate a YAML mapping file's syntax and cleaner references."""
    try:
        m = Mapping.from_yaml(mapping)
    except MappingError as err:
        typer.echo(f"error: {err}", err=True)
        raise typer.Exit(code=1) from err
    typer.echo(f"OK: {mapping} (name={m.name}, fields={len(m.fields)})")


@app.command()
def version() -> None:
    """Print the DataIngest version."""
    typer.echo(__version__)


if __name__ == "__main__":
    app()
