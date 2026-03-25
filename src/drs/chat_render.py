#
# Copyright (C) 2017-2026 Dremio Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Rich terminal renderer for Dremio AI Agent chat sessions."""

from __future__ import annotations

import json
import sys
import threading
from typing import Any

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.syntax import Syntax
from rich.text import Text

# Spinner frames for the "Thinking..." animation.
_SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
_SPINNER_INTERVAL = 0.08


class _Spinner:
    """A lightweight terminal spinner that does NOT use Rich's Live display.

    Rich's ``Status`` / ``Live`` captures all ``console.print()`` calls and
    renders them on its own refresh cycle, which can visually delay SSE events.
    This spinner writes its animation directly to *stderr* using ANSI escape
    codes so that ``console.print()`` output flows to the terminal immediately.
    """

    def __init__(self, message: str = "Thinking...") -> None:
        self._message = message
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._thread is None:
            return
        self._stop_event.set()
        self._thread.join(timeout=1.0)
        self._thread = None
        # Clear the spinner line
        sys.stderr.write("\r\033[K")
        sys.stderr.flush()

    def _run(self) -> None:
        idx = 0
        while not self._stop_event.is_set():
            frame = _SPINNER_FRAMES[idx % len(_SPINNER_FRAMES)]
            sys.stderr.write(f"\r{frame} {self._message}")
            sys.stderr.flush()
            idx += 1
            self._stop_event.wait(_SPINNER_INTERVAL)


class ChatRenderer:
    """Renders agent SSE events to a Rich console (interactive mode)."""

    def __init__(self, console: Console | None = None) -> None:
        self.console = console or Console()
        self._spinner: _Spinner | None = None

    # -- Model output --

    def render_model_chunk(self, name: str, result: dict) -> None:
        """Render a model output chunk based on the task type."""
        text = result.get("text", "")
        if not text:
            return

        if name == "modelGenerateSql":
            self.console.print(Syntax(text, "sql", theme="monokai", line_numbers=False))
        elif name == "modelReject":
            self.console.print(Text(text, style="bold yellow"))
        else:
            # modelGeneric, modelSqlAnswer, and others
            self.console.print(Markdown(text))

    # -- Tool events --

    def render_tool_request(
        self,
        call_id: str,
        name: str,
        arguments: dict | None = None,
        title: str | None = None,
    ) -> None:
        """Show a tool call request in a bordered panel."""
        display_name = title or name
        args_summary = ""
        if arguments:
            args_summary = _summarize_args(arguments)

        body = Text(args_summary, style="dim") if args_summary else Text("(no arguments)", style="dim")
        self.console.print(
            Panel(body, title=f"[bold cyan]Tool: {display_name}[/]", border_style="cyan", expand=False),
        )

    def render_tool_response(self, call_id: str, name: str, result: Any) -> None:
        """Show a tool result in a muted panel."""
        if isinstance(result, dict):
            text = json.dumps(result, indent=2, default=str)
            if len(text) > 500:
                text = text[:500] + "\n..."
        elif isinstance(result, str):
            text = result[:500] + ("..." if len(result) > 500 else "")
        else:
            text = str(result)[:500]

        self.console.print(
            Panel(Text(text, style="dim"), title=f"[dim]{name} result[/]", border_style="dim", expand=False),
        )

    def render_tool_progress(self, status: str, message: str) -> None:
        """Inline progress for long-running tools."""
        self.console.print(Text(f"  ⏳ {message}", style="dim italic"))

    # -- Errors --

    def render_error(self, error_type: str, message: str) -> None:
        """Red error display."""
        self.console.print(Text(f"Error ({error_type}): {message}", style="bold red"))

    # -- Conversation metadata --

    def render_conversation_title(self, title: str) -> None:
        """Show conversation title update."""
        self.console.print(Text(f"📝 {title}", style="bold"))

    # -- Spinner --

    def start_spinner(self) -> None:
        """Start an animated 'Thinking...' indicator."""
        if self._spinner is None:
            self._spinner = _Spinner()
            self._spinner.start()

    def stop_spinner(self) -> None:
        """Stop the spinner."""
        if self._spinner is not None:
            self._spinner.stop()
            self._spinner = None

    # -- Tool approval --

    def prompt_tool_approval(self, nonce: str, tools: list[dict]) -> dict:
        """Ask user Y/n for each pending tool call; return approval payload.

        Returns a dict suitable for the ``approvals`` field of the message body.
        """
        decisions: list[dict] = []
        for tool in tools:
            tool_name = tool.get("name", "unknown")
            tool_id = tool.get("callId", tool.get("id", ""))
            args = tool.get("arguments", {})
            self.render_tool_request(tool_id, tool_name, args)
            try:
                answer = self.console.input(f"  Approve [bold cyan]{tool_name}[/]? [Y/n] ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = "n"
            approved = answer in ("", "y", "yes")
            decisions.append(
                {
                    "callId": tool_id,
                    "decision": "approved" if approved else "denied",
                }
            )
        return {
            "approvalNonce": nonce,
            "toolDecisions": decisions,
        }

    # -- Separators --

    def print_separator(self) -> None:
        """Print a visual separator between exchanges."""
        self.console.print(Text("─" * 40, style="dim"))

    def print_welcome(self, conv_id: str | None = None) -> None:
        """Print welcome banner for interactive mode."""
        self.console.print(
            Panel(
                "[bold]Dremio AI Chat[/]\n"
                "Type a question or use /help for commands.\n"
                "Press [bold]Ctrl+D[/] or type [bold]/quit[/] to exit.",
                border_style="blue",
                expand=False,
            ),
        )
        if conv_id:
            self.console.print(Text(f"Resuming conversation: {conv_id}", style="dim"))

    def print_help(self) -> None:
        """Print slash command help."""
        help_text = (
            "[bold]Commands:[/]\n"
            "  /new          Start a new conversation\n"
            "  /list         List recent conversations\n"
            "  /continue <id> Resume a conversation by ID\n"
            "  /history      Show message history for current conversation\n"
            "  /cancel       Cancel the active run\n"
            "  /delete [id]  Delete current or specified conversation\n"
            "  /info         Show current conversation metadata\n"
            "  /quit         Exit (or Ctrl+D)"
        )
        self.console.print(Panel(help_text, border_style="blue", expand=False))


class PlainRenderer:
    """Non-interactive renderer.

    When stdout is a terminal, model output is rendered as Rich Markdown.
    When piped, plain text is written with no ANSI codes.
    Tool events and progress always go to stderr.
    """

    def __init__(self) -> None:
        self._is_tty = sys.stdout.isatty()
        self._console = Console() if self._is_tty else None
        self._stderr_console = Console(stderr=True, highlight=False)
        self._spinner: _Spinner | None = None

    def render_model_chunk(self, name: str, result: dict) -> None:
        text = result.get("text", "")
        if not text:
            return
        if self._console is not None:
            if name == "modelGenerateSql":
                self._console.print(Syntax(text, "sql", theme="monokai", line_numbers=False))
            elif name == "modelReject":
                self._console.print(Text(text, style="bold yellow"))
            else:
                self._console.print(Markdown(text))
        else:
            sys.stdout.write(text)
            sys.stdout.flush()

    def render_tool_request(
        self,
        call_id: str,
        name: str,
        arguments: dict | None = None,
        title: str | None = None,
    ) -> None:
        self._stderr_console.print(
            Text(f"  ⚙ {title or name}", style="dim cyan"),
        )

    def render_tool_response(self, call_id: str, name: str, result: Any) -> None:
        self._stderr_console.print(
            Text(f"  ✓ {name} done", style="dim"),
        )

    def render_tool_progress(self, status: str, message: str) -> None:
        self._stderr_console.print(
            Text(f"  ⏳ {message}", style="dim italic"),
        )

    def render_error(self, error_type: str, message: str) -> None:
        self._stderr_console.print(
            Text(f"Error ({error_type}): {message}", style="bold red"),
        )

    def render_conversation_title(self, title: str) -> None:
        pass

    def start_spinner(self) -> None:
        if self._is_tty and self._spinner is None:
            self._spinner = _Spinner()
            self._spinner.start()

    def stop_spinner(self) -> None:
        if self._spinner is not None:
            self._spinner.stop()
            self._spinner = None

    def print_separator(self) -> None:
        sys.stdout.write("\n")
        sys.stdout.flush()


def _summarize_args(args: dict, max_len: int = 200) -> str:
    """Produce a compact summary of tool arguments."""
    parts: list[str] = []
    for k, v in args.items():
        s = str(v)
        if len(s) > 60:
            s = s[:57] + "..."
        parts.append(f"{k}={s}")
    text = ", ".join(parts)
    if len(text) > max_len:
        text = text[:max_len] + "..."
    return text
