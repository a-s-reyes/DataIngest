from typing import ClassVar

from textual.app import App, ComposeResult
from textual.binding import BindingType
from textual.containers import Horizontal
from textual.widgets import Footer, Header, Input, Label, ListItem, ListView, RichLog

from ..appconfig import AppConfig, find_config, load_config
from .commands import destination_label, job_rows, parse_command


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

    def __init__(self, config: AppConfig | None = None) -> None:
        super().__init__()
        self._config = config

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            yield ListView(id="jobs")
            yield RichLog(id="output", wrap=True, markup=True)
        yield Input(placeholder="type a command, e.g. :help", id="command")
        yield Footer()

    def on_mount(self) -> None:
        config = self._config
        if config is None:
            cfg_path = find_config()
            config = load_config(cfg_path) if cfg_path is not None else AppConfig()
            self._config = config
        self.title = "DataIngest"
        self.sub_title = destination_label(config)
        jobs = self.query_one("#jobs", ListView)
        for name, _description in job_rows(config):
            jobs.append(ListItem(Label(name)))
        output = self.query_one("#output", RichLog)
        if config.jobs:
            output.write("Welcome. Select a job, or type a command below (:help).")
        else:
            output.write("No jobs configured. An admin sets up dataingest.toml.")

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
        else:
            self.query_one("#output", RichLog).write(
                f"'{event.value.strip()}' is not wired yet (coming in a later step)."
            )
