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
"""dremio chat — interactive and non-interactive chat with the Dremio AI Agent."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sys
from enum import StrEnum
from pathlib import Path
from typing import Any

import httpx
import typer
from prompt_toolkit import PromptSession
from prompt_toolkit.history import InMemoryHistory
from rich.console import Console
from rich.json import JSON as RichJSON
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from drs.chat_render import ChatRenderer, PlainRenderer
from drs.client import DremioClient
from drs.output import error as print_error
from drs.sse import parse_sse_stream
from drs.utils import DremioAPIError, handle_api_error

logger = logging.getLogger(__name__)

app = typer.Typer(help="Chat with the Dremio AI Agent.", context_settings={"help_option_names": ["-h", "--help"]})


# ---------------------------------------------------------------------------
# Output format helpers
# ---------------------------------------------------------------------------


class ChatFormat(StrEnum):
    json = "json"
    table = "table"


def _chat_output(data: Any, fmt: ChatFormat) -> None:
    """Print chat subcommand results as JSON or a Rich table."""
    console = Console()

    if fmt == ChatFormat.json:
        console.print(RichJSON(json.dumps(data, indent=2, default=str)))
        return

    rows = data.get("data", data.get("conversations", data.get("messages", [])))
    if not rows:
        console.print("[dim]No results.[/]")
        return

    if not isinstance(rows, list) or not isinstance(rows[0], dict):
        console.print("[dim]No results.[/]")
        return

    first = rows[0]
    if "chunkType" in first:
        _render_history_table(console, rows)
    elif "conversationId" in first and "title" in first:
        _render_conversations_table(console, rows)
    else:
        _render_generic_table(console, rows)


def _render_conversations_table(console: Console, rows: list[dict]) -> None:
    """Render conversation list as a curated Rich table."""
    table = Table(show_edge=False, pad_edge=False, expand=False)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Title")
    table.add_column("Model")
    table.add_column("Modified")

    for row in rows:
        modified = str(row.get("modifiedAt", ""))
        if "T" in modified:
            modified = modified.replace("T", " ").rstrip("Z").split(".")[0]
        table.add_row(
            row.get("conversationId", ""),
            row.get("title", ""),
            row.get("modelName", ""),
            modified,
        )
    console.print(table)


def _render_history_table(console: Console, rows: list[dict]) -> None:
    """Render conversation history as a readable transcript."""
    for row in rows:
        chunk_type = row.get("chunkType", "")
        timestamp = str(row.get("createdAt", ""))
        if "T" in timestamp:
            timestamp = timestamp.replace("T", " ").rstrip("Z").split(".")[0]

        if chunk_type == "userMessage":
            text = row.get("text", "")
            console.print(Text(f"  [{timestamp}]", style="dim"))
            console.print(Panel(text, title="[bold green]You[/]", border_style="green", expand=False))

        elif chunk_type == "model":
            result = row.get("result", {})
            text = result.get("text", "") if isinstance(result, dict) else str(result)
            name = row.get("name", "")
            title = "[bold blue]Agent[/]"
            if name and name != "modelGeneric":
                title += f" [dim]({name})[/]"
            console.print(Text(f"  [{timestamp}]", style="dim"))
            console.print(Panel(Markdown(text), title=title, border_style="blue", expand=False))

        elif chunk_type == "toolRequest":
            tool_name = row.get("name", "")
            summarized = row.get("summarizedTitle", tool_name)
            console.print(Text(f"  ⚙ {summarized}", style="dim cyan"))

        elif chunk_type == "toolResponse":
            tool_name = row.get("name", "")
            console.print(Text(f"  ✓ {tool_name} done", style="dim"))


def _render_generic_table(console: Console, rows: list[dict]) -> None:
    """Fallback: render all columns as a Rich table."""
    columns = list(rows[0].keys())
    table = Table(show_edge=False, pad_edge=False, expand=False)
    for col in columns:
        table.add_column(col)
    for row in rows:
        table.add_row(*[str(row.get(c, "")) for c in columns])
    console.print(table)


# ---------------------------------------------------------------------------
# Core async functions (reusable, no UI)
# ---------------------------------------------------------------------------


async def create_conversation(
    client: DremioClient,
    text: str,
    model: str | None = None,
) -> dict:
    """POST /agent/conversations — start a new conversation."""
    body: dict[str, Any] = {"prompt": {"text": text}}
    if model:
        body["model"] = model
    try:
        return await client.create_conversation(body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def send_message(
    client: DremioClient,
    conversation_id: str,
    text: str | None = None,
    approvals: dict | None = None,
    model: str | None = None,
) -> dict:
    """POST /agent/conversations/{id}/messages."""
    body: dict[str, Any] = {"prompt": {}}
    if text:
        body["prompt"]["text"] = text
    if approvals:
        body["prompt"]["approvals"] = approvals
    if model:
        body["model"] = model
    try:
        return await client.send_conversation_message(conversation_id, body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def stream_run(
    client: DremioClient,
    conversation_id: str,
    run_id: str,
):
    """GET /agent/conversations/{id}/runs/{runId} as SSE — yields parsed events."""
    try:
        resp = await client.stream_run(conversation_id, run_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc

    try:
        async for event in parse_sse_stream(resp.aiter_bytes()):
            yield event
    finally:
        await resp.aclose()


async def list_conversations(client: DremioClient, limit: int = 25) -> dict:
    """GET /agent/conversations"""
    try:
        return await client.list_conversations(limit=limit)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def get_messages(
    client: DremioClient,
    conversation_id: str,
    limit: int = 50,
) -> dict:
    """GET /agent/conversations/{id}/messages"""
    try:
        return await client.get_conversation_messages(conversation_id, limit=limit)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def delete_conversation(client: DremioClient, conversation_id: str) -> dict:
    """DELETE /agent/conversations/{id}"""
    try:
        return await client.delete_conversation(conversation_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def cancel_run(
    client: DremioClient,
    conversation_id: str,
    run_id: str,
) -> dict:
    """POST /agent/conversations/{id}/runs/{runId}:cancel"""
    try:
        return await client.cancel_conversation_run(conversation_id, run_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


def _extract_ids(result: dict) -> tuple[str | None, str | None]:
    """Extract conversation_id and run_id from an API response."""
    conv_id = result.get("conversationId", result.get("id"))
    run_id = result.get("currentRunId", result.get("runId", result.get("run", {}).get("id")))
    return conv_id, run_id


# ---------------------------------------------------------------------------
# SSE event dispatch
# ---------------------------------------------------------------------------


async def dispatch_events(
    client: DremioClient,
    renderer: ChatRenderer | PlainRenderer,
    conversation_id: str,
    run_id: str,
    auto_approve: bool = False,
    interactive: bool = True,
    log_file: Any | None = None,
) -> str | None:
    """Stream a run's SSE events and dispatch to the renderer.

    Returns the latest run_id (which may change after an approval cycle).
    """
    renderer.start_spinner()
    first_model_chunk = True

    try:
        async for event in stream_run(client, conversation_id, run_id):
            data = event.get("data", {})
            chunk_type = data.get("chunkType")

            if log_file:
                log_file.write(json.dumps(data, default=str) + "\n")
                log_file.flush()

            # All fields are at the top level of data (flat structure).
            if chunk_type == "model":
                if first_model_chunk:
                    renderer.stop_spinner()
                    first_model_chunk = False
                name = data.get("name", "")
                result = data.get("result", {})
                renderer.render_model_chunk(name, result)

            elif chunk_type == "toolRequest":
                renderer.stop_spinner()
                renderer.render_tool_request(
                    call_id=data.get("callId", ""),
                    name=data.get("name", ""),
                    arguments=data.get("arguments"),
                    title=data.get("summarizedTitle"),
                )
                renderer.start_spinner()

            elif chunk_type == "toolResponse":
                renderer.stop_spinner()
                renderer.render_tool_response(
                    call_id=data.get("callId", ""),
                    name=data.get("name", ""),
                    result=data.get("result"),
                )
                renderer.start_spinner()

            elif chunk_type == "toolProgress":
                renderer.render_tool_progress(
                    status=data.get("status", ""),
                    message=data.get("message", ""),
                )

            elif chunk_type == "error":
                renderer.stop_spinner()
                renderer.render_error(
                    error_type=data.get("type", "unknown"),
                    message=data.get("message", str(data)),
                )

            elif chunk_type == "interrupt":
                renderer.stop_spinner()
                nonce = data.get("approvalNonce", "")
                tools = data.get("toolDecisions", [])

                if interactive and isinstance(renderer, ChatRenderer):
                    approvals = renderer.prompt_tool_approval(nonce, tools)
                else:
                    decisions = []
                    for tool in tools:
                        decisions.append(
                            {
                                "callId": tool.get("callId", tool.get("id", "")),
                                "decision": "approved" if auto_approve else "denied",
                            }
                        )
                    approvals = {"approvalNonce": nonce, "toolDecisions": decisions}

                resp = await send_message(
                    client,
                    conversation_id,
                    approvals=approvals,
                )
                _, new_run_id = _extract_ids(resp)
                if new_run_id:
                    run_id = new_run_id
                    renderer.start_spinner()
                    first_model_chunk = True
                    return await dispatch_events(
                        client,
                        renderer,
                        conversation_id,
                        run_id,
                        auto_approve=auto_approve,
                        interactive=interactive,
                        log_file=log_file,
                    )

            elif chunk_type == "conversationUpdate":
                title = data.get("title", "")
                if title:
                    renderer.render_conversation_title(title)

            elif chunk_type == "endOfStream":
                renderer.stop_spinner()
                break

            elif chunk_type == "userMessage":
                pass

    finally:
        renderer.stop_spinner()

    return run_id


# ---------------------------------------------------------------------------
# Interactive REPL
# ---------------------------------------------------------------------------


async def chat_repl(
    client: DremioClient,
    renderer: ChatRenderer,
    conv_id: str | None = None,
    run_id: str | None = None,
    model: str | None = None,
    log_file: Any | None = None,
) -> None:
    """Interactive REPL loop."""
    session: PromptSession = PromptSession(history=InMemoryHistory())
    renderer.print_welcome(conv_id)

    # If we were given a conversation + run_id, stream it first
    if conv_id and run_id:
        try:
            run_id = await dispatch_events(
                client,
                renderer,
                conv_id,
                run_id,
                interactive=True,
                log_file=log_file,
            )
        except DremioAPIError as exc:
            renderer.render_error("api", str(exc))

    while True:
        try:
            text = await session.prompt_async("You > ")
        except (EOFError, KeyboardInterrupt):
            break

        text = text.strip()
        if not text:
            continue

        # -- Slash commands --
        if text.startswith("/"):
            parts = text.split(maxsplit=1)
            cmd = parts[0].lower()
            arg = parts[1].strip() if len(parts) > 1 else ""

            if cmd == "/quit":
                break
            if cmd == "/help":
                renderer.print_help()
            elif cmd == "/new":
                conv_id = None
                run_id = None
                renderer.console.print("[dim]Starting new conversation.[/]")
            elif cmd == "/list":
                try:
                    result = await list_conversations(client, limit=25)
                    _render_conversations_table(
                        renderer.console,
                        result.get("data", result.get("conversations", [])),
                    )
                except DremioAPIError as exc:
                    renderer.render_error("api", str(exc))
            elif cmd == "/continue":
                if not arg:
                    renderer.console.print("[yellow]Usage: /continue <conversation-id>[/]")
                else:
                    conv_id = arg
                    run_id = None
                    renderer.console.print(f"[dim]Switched to conversation: {conv_id}[/]")
            elif cmd == "/history":
                if not conv_id:
                    renderer.console.print("[yellow]No active conversation. Start one first.[/]")
                else:
                    try:
                        result = await get_messages(client, conv_id, limit=50)
                        _render_history_table(
                            renderer.console,
                            result.get("data", result.get("messages", [])),
                        )
                    except DremioAPIError as exc:
                        renderer.render_error("api", str(exc))
            elif cmd == "/cancel":
                if not conv_id or not run_id:
                    renderer.console.print("[yellow]No active run to cancel.[/]")
                else:
                    try:
                        await cancel_run(client, conv_id, run_id)
                        renderer.console.print("[dim]Run cancelled.[/]")
                    except DremioAPIError as exc:
                        renderer.render_error("api", str(exc))
            elif cmd == "/delete":
                target = arg or conv_id
                if not target:
                    renderer.console.print("[yellow]No conversation to delete. Provide an ID or start one first.[/]")
                else:
                    try:
                        await delete_conversation(client, target)
                        renderer.console.print(f"[dim]Deleted conversation: {target}[/]")
                        if target == conv_id:
                            conv_id = None
                            run_id = None
                    except DremioAPIError as exc:
                        renderer.render_error("api", str(exc))
            elif cmd == "/info":
                if not conv_id:
                    renderer.console.print("[yellow]No active conversation.[/]")
                else:
                    renderer.console.print(f"  Conversation: [cyan]{conv_id}[/]")
                    renderer.console.print(f"  Run:          [cyan]{run_id or '(none)'}[/]")
            else:
                renderer.console.print(f"[yellow]Unknown command: {cmd}. Type /help for commands.[/]")
            continue

        # -- Send message --
        try:
            if conv_id is None:
                result = await create_conversation(client, text, model=model)
                logger.debug("create_conversation response: %s", json.dumps(result, default=str))
                conv_id, run_id = _extract_ids(result)
            else:
                result = await send_message(client, conv_id, text=text, model=model)
                logger.debug("send_message response: %s", json.dumps(result, default=str))
                _, run_id = _extract_ids(result)

            if run_id:
                try:
                    run_id = await dispatch_events(
                        client,
                        renderer,
                        conv_id,
                        run_id,
                        interactive=True,
                        log_file=log_file,
                    )
                except KeyboardInterrupt:
                    renderer.stop_spinner()
                    renderer.console.print("\n[dim]Cancelling...[/]")
                    with contextlib.suppress(DremioAPIError):
                        await cancel_run(client, conv_id, run_id)
            renderer.print_separator()
        except DremioAPIError as exc:
            renderer.render_error("api", str(exc))


# ---------------------------------------------------------------------------
# Non-interactive (one-shot) mode
# ---------------------------------------------------------------------------


async def chat_oneshot(
    client: DremioClient,
    message: str,
    conversation_id: str | None = None,
    auto_approve: bool = False,
    model: str | None = None,
    log_file: Any | None = None,
) -> None:
    """Send a single message and stream the response to stdout."""
    renderer = PlainRenderer()

    if conversation_id is None:
        result = await create_conversation(client, message, model=model)
        logger.debug("create_conversation response: %s", json.dumps(result, default=str))
        conversation_id, run_id = _extract_ids(result)
    else:
        result = await send_message(client, conversation_id, text=message, model=model)
        logger.debug("send_message response: %s", json.dumps(result, default=str))
        _, run_id = _extract_ids(result)

    logger.debug("conversation_id=%s run_id=%s", conversation_id, run_id)

    if run_id:
        await dispatch_events(
            client,
            renderer,
            conversation_id,
            run_id,
            auto_approve=auto_approve,
            interactive=False,
            log_file=log_file,
        )
    else:
        logger.warning("No run_id found in response — cannot stream events")
    # Ensure trailing newline for piped output
    sys.stdout.write("\n")
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# CLI entry points
# ---------------------------------------------------------------------------


def _get_client() -> DremioClient:
    # Deferred import to avoid circular dependency: cli.py imports this module.
    from drs.cli import get_client

    return get_client()


@app.callback(invoke_without_command=True)
def chat_main(
    ctx: typer.Context,
    message: str | None = typer.Option(None, "--message", "-m", help="Send a single message (non-interactive mode)"),
    conversation: str | None = typer.Option(None, "--conversation", "-C", help="Resume an existing conversation by ID"),
    auto_approve: bool = typer.Option(False, "--auto-approve", help="Auto-approve tool calls (non-interactive only)"),
    log_file: str | None = typer.Option(None, "--log-file", help="Path to JSON-lines event log file"),
    model: str | None = typer.Option(None, "--model", help="Model override"),
) -> None:
    """Chat with the Dremio AI Agent. Launches interactive REPL by default."""
    if ctx.invoked_subcommand is not None:
        return

    client = _get_client()

    async def _run() -> None:
        log_fh = None
        try:
            if log_file:
                log_fh = Path(log_file).open("a")  # noqa: SIM115
            if message is not None:
                # Read from stdin if message is "-"
                msg = sys.stdin.read().strip() if message == "-" else message
                await chat_oneshot(
                    client,
                    msg,
                    conversation_id=conversation,
                    auto_approve=auto_approve,
                    model=model,
                    log_file=log_fh,
                )
            else:
                renderer = ChatRenderer()
                await chat_repl(
                    client,
                    renderer,
                    conv_id=conversation,
                    model=model,
                    log_file=log_fh,
                )
        finally:
            await client.close()
            if log_fh:
                log_fh.close()

    try:
        asyncio.run(_run())
    except DremioAPIError as exc:
        print_error(str(exc))
        raise typer.Exit(1)


@app.command("list")
def chat_list(
    limit: int = typer.Option(25, "--limit", "-n", help="Maximum conversations to return"),
    fmt: ChatFormat = typer.Option(ChatFormat.table, "--format", "-f", help="Output format: json, table"),
) -> None:
    """List recent conversations."""
    client = _get_client()

    async def _run():
        try:
            return await list_conversations(client, limit=limit)
        finally:
            await client.close()

    try:
        result = asyncio.run(_run())
    except DremioAPIError as exc:
        print_error(str(exc))
        raise typer.Exit(1)
    _chat_output(result, fmt)


@app.command("history")
def chat_history(
    conversation_id: str = typer.Argument(help="Conversation ID"),
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum messages to return"),
    fmt: ChatFormat = typer.Option(ChatFormat.table, "--format", "-f", help="Output format: json, table"),
) -> None:
    """Show message history for a conversation."""
    client = _get_client()

    async def _run():
        try:
            return await get_messages(client, conversation_id, limit=limit)
        finally:
            await client.close()

    try:
        result = asyncio.run(_run())
    except DremioAPIError as exc:
        print_error(str(exc))
        raise typer.Exit(1)
    _chat_output(result, fmt)


@app.command("delete")
def chat_delete(
    conversation_id: str = typer.Argument(help="Conversation ID to delete"),
    fmt: ChatFormat = typer.Option(ChatFormat.json, "--format", "-f", help="Output format: json, table"),
) -> None:
    """Delete a conversation."""
    client = _get_client()

    async def _run():
        try:
            return await delete_conversation(client, conversation_id)
        finally:
            await client.close()

    try:
        result = asyncio.run(_run())
    except DremioAPIError as exc:
        print_error(str(exc))
        raise typer.Exit(1)
    _chat_output(result, fmt)
