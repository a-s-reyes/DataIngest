from pathlib import Path

from dataingest.appconfig import AppConfig, Destination, Job
from dataingest.tui.commands import (
    destination_label,
    format_run_summary,
    job_detail,
    job_rows,
    parse_command,
)


def test_parse_quit() -> None:
    assert parse_command(":q").kind == "quit"
    assert parse_command("quit").kind == "quit"


def test_parse_help() -> None:
    assert parse_command(":help").kind == "help"


def test_parse_run() -> None:
    cmd = parse_command(":run bills file.csv")
    assert cmd.kind == "run"
    assert cmd.job == "bills"
    assert cmd.file == "file.csv"


def test_parse_unknown() -> None:
    assert parse_command(":frobnicate").kind == "unknown"


def test_job_rows() -> None:
    config = AppConfig(jobs={"bills": Job(mapping="m.yml", description="Bills")})
    assert job_rows(config) == [("bills", "Bills")]


def test_destination_label_sqlite() -> None:
    assert destination_label(AppConfig(destination=Destination(sink="out.db"))) == "out.db (sqlite)"


def test_destination_label_postgres_hides_credentials() -> None:
    label = destination_label(AppConfig(destination=Destination(sink="postgres://u:p@h/db")))
    assert "p@h" not in label
    assert "database" in label


def test_destination_label_none() -> None:
    assert destination_label(AppConfig()) == "not configured"


def test_job_detail_includes_table_and_fields(tmp_path: Path) -> None:
    (tmp_path / "m.yml").write_text(
        """
spec_version: 1
name: bills
source: { format: csv }
target: { table: TaxBill, primary_key: id }
fields:
  id: { column: 0, type: str, required: true }
  amount: { column: 1, type: decimal }
""",
        encoding="utf-8",
    )
    detail = job_detail(AppConfig(jobs={"bills": Job(mapping="m.yml")}), tmp_path, "bills")
    assert "TaxBill" in detail
    assert "id" in detail
    assert "amount" in detail


def test_job_detail_missing_mapping_is_friendly(tmp_path: Path) -> None:
    detail = job_detail(AppConfig(jobs={"bills": Job(mapping="nope.yml")}), tmp_path, "bills")
    assert "Could not load mapping" in detail


def test_format_run_summary_clean() -> None:
    assert format_run_summary(20, 20, 0) == "Done. 20 rows loaded."


def test_format_run_summary_partial() -> None:
    summary = format_run_summary(20, 18, 2)
    assert "18" in summary
    assert "2 failed" in summary
    assert "errors.jsonl" in summary


def test_format_run_summary_empty() -> None:
    assert format_run_summary(0, 0, 0) == "No rows found in the file."
