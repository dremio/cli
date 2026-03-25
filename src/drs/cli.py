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
"""dremio — Developer CLI for Dremio Cloud. Entry point and command registration."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path

import httpx
import typer

from drs.auth import DrsConfig, load_config
from drs.client import DremioClient
from drs.commands import chat, engine, folder, grant, job, project, query, reflection, role, schema, tag, user, wiki

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}

app = typer.Typer(
    name="dremio",
    help="Developer CLI for Dremio Cloud.",
    no_args_is_help=True,
    context_settings=CONTEXT_SETTINGS,
)

# Register command groups
app.add_typer(query.app, name="query")
app.add_typer(folder.app, name="folder")
app.add_typer(schema.app, name="schema")
app.add_typer(wiki.app, name="wiki")
app.add_typer(tag.app, name="tag")
app.add_typer(reflection.app, name="reflection")
app.add_typer(job.app, name="job")
app.add_typer(engine.app, name="engine")
app.add_typer(user.app, name="user")
app.add_typer(role.app, name="role")
app.add_typer(grant.app, name="grant")
app.add_typer(project.app, name="project")
app.add_typer(chat.app, name="chat")

# Global state for config
_config: DrsConfig | None = None
_cli_opts: dict = {}


@app.callback()
def main(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to config file"),
    token: str | None = typer.Option(None, "--token", help="Dremio personal access token (PAT)"),
    project_id: str | None = typer.Option(None, "--project-id", help="Dremio Cloud project ID"),
    uri: str | None = typer.Option(
        None, "--uri", help="Dremio API base URI (e.g., https://api.dremio.cloud, https://api.eu.dremio.cloud)"
    ),
    verbose: int = typer.Option(
        0, "--verbose", "-v", count=True, help="Increase logging verbosity (-v for debug, -vv for trace)"
    ),
) -> None:
    """Global options for dremio CLI."""
    # Configure logging based on verbosity
    if verbose >= 2:
        log_level = logging.DEBUG
        # Also enable httpx/httpcore debug logging for -vv
        logging.getLogger("httpx").setLevel(logging.DEBUG)
        logging.getLogger("httpcore").setLevel(logging.DEBUG)
    elif verbose == 1:
        log_level = logging.DEBUG
    else:
        log_level = logging.WARNING

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    global _cli_opts
    _cli_opts = {
        "config_path": Path(config) if config else None,
        "cli_token": token,
        "cli_project_id": project_id,
        "cli_uri": uri,
    }


def get_config() -> DrsConfig:
    global _config
    if _config is None:
        try:
            _config = load_config(
                _cli_opts.get("config_path"),
                cli_token=_cli_opts.get("cli_token"),
                cli_project_id=_cli_opts.get("cli_project_id"),
                cli_uri=_cli_opts.get("cli_uri"),
            )
        except Exception as e:
            print(f"Error loading config: {e}", file=sys.stderr)
            print(
                "Provide credentials via --token, DREMIO_TOKEN env var, "
                "or config file (~/.config/dremioai/config.yaml)",
                file=sys.stderr,
            )
            raise typer.Exit(1)
    return _config


def get_client() -> DremioClient:
    return DremioClient(get_config())


# -- Top-level commands --


@app.command("search")
def search_command(
    term: str = typer.Argument(help="Search term (matches table names, view names, source names)"),
    fmt: str = typer.Option("json", "--output", "-o", help="Output format: json, csv, pretty"),
) -> None:
    """Full-text search across all catalog entities (tables, views, sources)."""
    from drs.output import OutputFormat, error, output
    from drs.utils import DremioAPIError, handle_api_error

    client = get_client()

    async def _execute():
        try:
            try:
                return await client.search(term)
            except httpx.HTTPStatusError as exc:
                raise handle_api_error(exc) from exc
        finally:
            await client.close()

    try:
        result = asyncio.run(_execute())
    except DremioAPIError as exc:
        error(str(exc))
        raise typer.Exit(1)
    output(result, OutputFormat(fmt))


@app.command("describe")
def describe_command(
    command: str = typer.Argument(help="Command to describe (e.g., 'query.run', 'folder.get', 'reflection.delete')"),
) -> None:
    """Show machine-readable schema for a command — parameters, types, and descriptions."""
    from drs.introspect import describe_command as _describe
    from drs.introspect import list_commands

    result = _describe(command)
    if result is None:
        print(f"Unknown command: {command}", file=sys.stderr)
        commands = list_commands()
        print(f"Available commands: {', '.join(commands)}", file=sys.stderr)
        raise typer.Exit(1)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    app()
