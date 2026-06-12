from pathlib import Path
from typing import ClassVar

from textual import work
from textual.app import App, ComposeResult
from textual.binding import BindingType
from textual.containers import Horizontal
from textual.widgets import Footer, Header, Input, Label, ListItem, ListView, RichLog

from ..appconfig import AppConfig, find_config, load_config, resolve_job
from ..config import Mapping
from ..pipeline import Pipeline
from .commands import (
    destination_label,
    format_run_summary,
    job_detail,
    job_rows,
    parse_command,
)


class DataIngestApp(App[None]):
    CSS = """
    Horizontal {
        height: 1fr;
    }
    #jobs {
        width: 30%;
        border: round $accent;
    }
    #output {
        width: 1fr;
        border: round $accent;
    }
    """

    BINDINGS: ClassVar[list[BindingType]] = [
        ("q", "quit", "Quit"),
        ("question_mark", "show_help", "Help"),
        ("colon", "focus_command", "Command"),
    ]

    def __init__(self, config: AppConfig | None = None, base_dir: Path | None = None) -> None:
        super().__init__()
        self._config = config
        self._base_dir = base_dir or Path.cwd()

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            yield ListView(id="jobs")
            yield RichLog(id="output", wrap=True, markup=False)
        yield Input(placeholder="type a command, e.g. :help", id="command")
        yield Footer()

    def on_mount(self) -> None:
        config = self._config
        if config is None:
            cfg_path = find_config()
            if cfg_path is not None:
                config = load_config(cfg_path)
                self._base_dir = cfg_path.parent
            else:
                config = AppConfig()
            self._config = config
        self.title = "DataIngest"
        self.sub_title = destination_label(config)
        jobs = self.query_one("#jobs", ListView)
        for name, _description in job_rows(config):
            jobs.append(ListItem(Label(name), name=name))
        output = self.query_one("#output", RichLog)
        if config.jobs:
            output.write("Welcome. Select a job, or type a command below (:help).")
        else:
            output.write("No jobs configured. An admin sets up dataingest.toml.")

    def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
        if event.item is None or self._config is None:
            return
        name = event.item.name
        if name is None:
            return
        output = self.query_one("#output", RichLog)
        output.clear()
        output.write(job_detail(self._config, self._base_dir, name))

    def action_focus_command(self) -> None:
        self.query_one("#command", Input).focus()

    def action_show_help(self) -> None:
        self._write_help()

    def _write_help(self) -> None:
        self.query_one("#output", RichLog).write(
            "Commands: :run <job> <file>, :help, :q (quit).  Keys: : command, ? help, q quit."
        )

    def on_input_submitted(self, event: Input.Submitted) -> None:
        command = parse_command(event.value)
        event.input.value = ""
        if command.kind == "quit":
            self.exit()
        elif command.kind == "help":
            self._write_help()
        elif command.kind == "run":
            if command.job is None or command.file is None:
                self.query_one("#output", RichLog).write("Usage: :run <job> <file>")
            else:
                self._run_job(command.job, command.file)
        else:
            self.query_one("#output", RichLog).write(f"Unknown command: {event.value.strip()}")

    @work(thread=True, exclusive=True)
    def _run_job(self, job: str, file: str) -> None:
        output = self.query_one("#output", RichLog)
        self.call_from_thread(output.write, f"Running '{job}' on {file} ...")
        if self._config is None:
            self.call_from_thread(output.write, "No configuration loaded.")
            return
        try:
            resolved = resolve_job(self._config, job, file, self._base_dir)
            mapping = Mapping.from_yaml(resolved.mapping)
            result = Pipeline(
                source_uri=resolved.source_uri,
                sink_uri=resolved.sink_uri,
                mapping=mapping,
                error_log=Path.cwd() / "errors.jsonl",
            ).run()
        except Exception as err:
            self.call_from_thread(output.write, f"Error: {err}")
            return
        self.call_from_thread(
            output.write,
            format_run_summary(result.rows_in, result.rows_ok, result.rows_failed),
        )
