import contextlib
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


def _reconfigure_streams_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        with contextlib.suppress(AttributeError, ValueError):
            reconfigure(encoding="utf-8", errors="replace")


_reconfigure_streams_utf8()

app = typer.Typer(
    name="dataingest",
    help="Config-driven CSV/Excel to SQL ingestion tool.",
    no_args_is_help=True,
    add_completion=False,
)

EXIT_OK = 0
EXIT_PREFLIGHT_ERROR = 1
EXIT_PARTIAL_FAILURE = 2
EXIT_TOTAL_FAILURE = 3


_LOG_LEVELS = [logging.WARNING, logging.INFO, logging.DEBUG]


def _configure_logging(verbose: int, quiet: bool) -> None:
    level = logging.ERROR if quiet else _LOG_LEVELS[min(verbose, len(_LOG_LEVELS) - 1)]
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        force=True,
    )


def _exit_code_from(result: RunResult) -> int:
    if result.rows_in == 0:
        return EXIT_OK
    if result.rows_ok == 0:
        return EXIT_TOTAL_FAILURE
    if result.rows_failed > 0:
        return EXIT_PARTIAL_FAILURE
    return EXIT_OK


@app.command()
def run(
    source: Annotated[str, typer.Option(help="Source URI, e.g. csv:///path/to/file.csv")],
    sink: Annotated[
        list[str],
        typer.Option(help="Sink URI (repeatable), e.g. sqlite:///./out.db"),
    ],
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
    except Exception as err:
        logging.getLogger(__name__).debug("pipeline raised", exc_info=err)
        typer.echo(f"error: {type(err).__name__}: {err}", err=True)
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
    file: Annotated[Path, typer.Argument(help="Path to the CSV or .xlsx to inspect")],
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
        typer.Option(help="CSV delimiter (csv only, default: ',')"),
    ] = ",",
    encoding: Annotated[
        str,
        typer.Option(help="File encoding (csv only, default: utf-8-sig; handles BOM)"),
    ] = "utf-8-sig",
    sheet: Annotated[
        str | None,
        typer.Option(help="xlsx sheet name to sample (default: first sheet)"),
    ] = None,
    name: Annotated[
        str | None,
        typer.Option(help="Mapping name (default: filename stem)"),
    ] = None,
    table: Annotated[
        str | None,
        typer.Option(help="Target table name (default: filename stem)"),
    ] = None,
) -> None:
    """Sniff a CSV or .xlsx and emit a starter YAML mapping.

    Format autodetected from the file extension (``.xlsx`` / ``.xlsm`` ->
    xlsx, everything else -> csv). The output is a runnable mapping that
    you should review and tighten: types and cleaners are inferred from
    the first N rows, primary key is the first column with all-unique
    non-null values, and ``on_conflict`` defaults to ``skip``.

    Pipe the output to a file:

        dataingest infer data.csv > mappings/data.yml

    Or write directly:

        dataingest infer data.xlsx -o mappings/data.yml --sheet Bills
    """
    try:
        mapping = infer_mapping(
            file,
            name=name,
            table=table,
            sample_size=sample_size,
            delimiter=delimiter,
            encoding=encoding,
            sheet=sheet,
        )
    except Exception as err:
        logging.getLogger(__name__).debug("infer raised", exc_info=err)
        typer.echo(f"error: {type(err).__name__}: {err}", err=True)
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
        logging.getLogger(__name__).debug("inspect_sink raised", exc_info=err)
        typer.echo(f"error: {type(err).__name__}: {err}", err=True)
        raise typer.Exit(code=EXIT_PREFLIGHT_ERROR) from err
    typer.echo(render_inspection(info))


@app.command()
def version() -> None:
    """Print the DataIngest version."""
    typer.echo(__version__)


if __name__ == "__main__":
    app()
