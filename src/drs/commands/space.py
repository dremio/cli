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
"""dremio space — manage spaces in the Dremio catalog."""

from __future__ import annotations

import asyncio

import typer

from drs.client import DremioClient
from drs.commands.folder import delete_entity, get_entity, list_catalog
from drs.commands.query import run_query
from drs.output import OutputFormat, error, output

app = typer.Typer(
    help="Manage spaces in the Dremio catalog.", context_settings={"help_option_names": ["-h", "--help"]}
)


async def list_spaces(client: DremioClient) -> dict:
    """List all spaces in the catalog."""
    result = await list_catalog(client)
    spaces = [e for e in result.get("entities", []) if e.get("containerType") == "SPACE"]
    return {"entities": spaces}


async def create_space(client: DremioClient, name: str) -> dict:
    """Create a space using CREATE SPACE SQL."""
    sql = f'CREATE SPACE "{name}"'
    return await run_query(client, sql)


async def get_space(client: DremioClient, name: str) -> dict:
    """Get space metadata by name."""
    return await get_entity(client, name)


async def delete_space(client: DremioClient, name: str) -> dict:
    """Delete a space by name."""
    return await delete_entity(client, name)


# -- CLI wrappers --


def _get_client() -> DremioClient:
    from drs.cli import get_client

    return get_client()


def _run_command(coro, client, fmt: OutputFormat = OutputFormat.json, fields: str | None = None) -> None:
    async def _execute():
        try:
            return await coro
        finally:
            await client.close()

    try:
        result = asyncio.run(_execute())
    except Exception as exc:
        from drs.utils import DremioAPIError

        if isinstance(exc, DremioAPIError):
            error(str(exc))
            raise typer.Exit(1)
        if isinstance(exc, ValueError):
            error(str(exc))
            raise typer.Exit(1)
        raise
    output(result, fmt, fields=fields)


@app.command("list")
def cli_list(
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """List all spaces in the catalog."""
    client = _get_client()
    _run_command(list_spaces(client), client, fmt, fields=fields)


@app.command("get")
def cli_get(
    name: str = typer.Argument(help="Space name"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """Get metadata for a space by name."""
    client = _get_client()
    _run_command(get_space(client, name), client, fmt, fields=fields)


@app.command("create")
def cli_create(
    name: str = typer.Argument(help="Space name to create"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Create a new space."""
    client = _get_client()
    _run_command(create_space(client, name), client, fmt)


@app.command("delete")
def cli_delete(
    name: str = typer.Argument(help="Space name to delete"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be deleted without deleting"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Delete a space. Cannot be undone."""
    client = _get_client()
    if dry_run:
        _run_command(get_space(client, name), client, fmt)
        return
    _run_command(delete_space(client, name), client, fmt)
