import logging
import sys
from pathlib import Path
from typing import IO, Annotated

import typer

from . import __version__
from .config import Mapping
from .errors import MappingError
from .infer import DEFAULT_SAMPLE_SIZE, dump_mapping, infer_mapping
from .inspect import inspect_sink, render_inspection
from .pipeline import DEFAULT_CHUNK_SIZE, Pipeline, RunResult

app = typer.Typer(
    name="dataingest",
    help="Config-driven CSV → SQL ingestion tool.",
    no_args_is_help=True,
    add_completion=False,
)

# Exit codes — see README "Exit codes" section.
EXIT_OK = 0
EXIT_PREFLIGHT_ERROR = 1  # mapping load / URI / config error before any rows touched
EXIT_PARTIAL_FAILURE = 2  # rows_in > 0 and 0 < rows_ok < rows_in
EXIT_TOTAL_FAILURE = 3  # rows_in > 0 and rows_ok == 0


_LOG_LEVELS = [logging.WARNING, logging.INFO, logging.DEBUG]


def _configure_logging(verbose: int, quiet: bool) -> None:
    # Quiet silences non-error logs; verbose lifts the floor.
    level = logging.ERROR if quiet else _LOG_LEVELS[min(verbose, len(_LOG_LEVELS) - 1)]
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        force=True,  # override any prior basicConfig from imports
    )


def _exit_code_from(result: RunResult) -> int:
    if result.rows_in == 0:
        return EXIT_OK  # vacuous success — nothing to ingest
    if result.rows_ok == 0:
        return EXIT_TOTAL_FAILURE
    if result.rows_failed > 0:
        return EXIT_PARTIAL_FAILURE
    return EXIT_OK


@app.command()
def run(
    source: Annotated[str, typer.Option(help="Source URI, e.g. csv:///path/to/file.csv")],
    sink: Annotated[str, typer.Option(help="Sink URI, e.g. sqlite:///./out.db")],
    mapping: Annotated[Path, typer.Option(help="Path to YAML mapping file")],
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Validate without writing")] = False,
    limit: Annotated[int | None, typer.Option(help="Process at most N rows")] = None,
    errors: Annotated[
        str | None,
        typer.Option(
            help="Path to JSONL error log, or '-' for stderr (default: ./errors.jsonl)",
        ),
    ] = None,
    chunk_size: Annotated[
        int,
        typer.Option(
            "--chunk-size",
            help=f"Rows per sink batch flush (default: {DEFAULT_CHUNK_SIZE}, min: 1)",
            min=1,
        ),
    ] = DEFAULT_CHUNK_SIZE,
    verbose: Annotated[
        int,
        typer.Option(
            "--verbose",
            "-v",
            count=True,
            help="Increase verbosity. -v = info, -vv = debug. Default: warnings only.",
        ),
    ] = 0,
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help="Suppress the summary line; rely on the exit code.",
        ),
    ] = False,
) -> None:
    """Run a full ingestion pipeline.

    Exit codes:
      0  clean (or vacuous success when no rows arrived)
      1  preflight error (bad mapping, missing dep, etc.)
      2  partial failure (some rows landed, some hit errors.jsonl)
      3  total failure (rows arrived but none survived validation)
    """
    _configure_logging(verbose, quiet)

    try:
        m = Mapping.from_yaml(mapping)
    except MappingError as err:
        typer.echo(f"error: {err}", err=True)
        raise typer.Exit(code=EXIT_PREFLIGHT_ERROR) from err

    error_target: Path | IO[str] | None
    if errors == "-":
        error_target = sys.stderr
    elif errors is None:
        error_target = None
    else:
        error_target = Path(errors)

    try:
        pipeline = Pipeline(
            source_uri=source,
            sink_uri=sink,
            mapping=m,
            dry_run=dry_run,
            limit=limit,
            error_log=error_target,
            chunk_size=chunk_size,
        )
        result = pipeline.run()
    except ValueError as err:
        # e.g. chunk_size validation, unknown URI scheme
        typer.echo(f"error: {err}", err=True)
        raise typer.Exit(code=EXIT_PREFLIGHT_ERROR) from err

    if not quiet:
        typer.echo(
            f"rows_in={result.rows_in} ok={result.rows_ok} "
            f"failed={result.rows_failed} chunks={result.chunks_written} "
            f"run_id={result.run_id}"
        )

    exit_code = _exit_code_from(result)
    if exit_code != EXIT_OK:
        raise typer.Exit(code=exit_code)


@app.command()
def validate(
    mapping: Annotated[Path, typer.Argument(help="Path to YAML mapping file")],
) -> None:
    """Validate a YAML mapping file's syntax and cleaner references."""
    try:
        m = Mapping.from_yaml(mapping)
    except MappingError as err:
        typer.echo(f"error: {err}", err=True)
        raise typer.Exit(code=EXIT_PREFLIGHT_ERROR) from err
    typer.echo(f"OK: {mapping} (name={m.name}, fields={len(m.fields)})")


@app.command()
def infer(
    csv_file: Annotated[Path, typer.Argument(help="Path to the CSV to inspect")],
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Write YAML here instead of stdout",
        ),
    ] = None,
    sample_size: Annotated[
        int,
        typer.Option(
            "--sample-size",
            help=f"Rows to sample for type inference (default: {DEFAULT_SAMPLE_SIZE})",
            min=1,
        ),
    ] = DEFAULT_SAMPLE_SIZE,
    delimiter: Annotated[
        str,
        typer.Option(help="CSV delimiter (default: ',')"),
    ] = ",",
    encoding: Annotated[
        str,
        typer.Option(help="File encoding (default: utf-8)"),
    ] = "utf-8",
    name: Annotated[
        str | None,
        typer.Option(help="Mapping name (default: filename stem)"),
    ] = None,
    table: Annotated[
        str | None,
        typer.Option(help="Target table name (default: filename stem)"),
    ] = None,
) -> None:
    """Sniff a CSV and emit a starter YAML mapping.

    The output is a runnable mapping that you should review and tighten —
    types and cleaners are inferred from the first N rows, primary key is
    the first column with all-unique non-null values, and ``on_conflict``
    defaults to ``skip``.

    Pipe the output to a file:

        dataingest infer data.csv > mappings/data.yml

    Or write directly:

        dataingest infer data.csv -o mappings/data.yml
    """
    try:
        mapping = infer_mapping(
            csv_file,
            name=name,
            table=table,
            sample_size=sample_size,
            delimiter=delimiter,
            encoding=encoding,
        )
    except (FileNotFoundError, ValueError) as err:
        typer.echo(f"error: {err}", err=True)
        raise typer.Exit(code=EXIT_PREFLIGHT_ERROR) from err

    yaml_text = dump_mapping(mapping)
    if output is not None:
        output.write_text(yaml_text, encoding="utf-8")
        typer.echo(f"wrote {output} ({len(mapping['fields'])} fields)", err=True)
    else:
        typer.echo(yaml_text, nl=False)


@app.command()
def tables(
    sink: Annotated[str, typer.Argument(help="Sink URI to inspect, e.g. sqlite:///./out.db")],
    runs: Annotated[
        int,
        typer.Option(
            "--runs",
            help="Show the most recent N entries from the _dataingest_runs audit table",
            min=0,
        ),
    ] = 5,
) -> None:
    """List tables in a sink and the recent run-manifest entries.

    Closes the loop on the CLI: ``run`` writes data, ``tables`` confirms what
    landed and shows the audit trail.
    """
    try:
        info = inspect_sink(sink, recent_runs=runs)
    except Exception as err:
        typer.echo(f"error: {err}", err=True)
        raise typer.Exit(code=EXIT_PREFLIGHT_ERROR) from err
    typer.echo(render_inspection(info))


@app.command()
def version() -> None:
    """Print the DataIngest version."""
    typer.echo(__version__)


if __name__ == "__main__":
    app()
