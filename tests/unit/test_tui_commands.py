from dataingest.appconfig import AppConfig, Destination, Job
from dataingest.tui.commands import destination_label, job_rows, parse_command


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
