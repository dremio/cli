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
"""drs role — manage Dremio Cloud roles."""

from __future__ import annotations

import asyncio

import httpx
import typer

from drs.client import DremioClient
from drs.output import OutputFormat, output, error
from drs.utils import handle_api_error

app = typer.Typer(help="Manage Dremio Cloud roles.")


async def list_roles(client: DremioClient) -> dict:
    """List all roles."""
    try:
        return await client.list_roles()
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def get_role(client: DremioClient, identifier: str) -> dict:
    """Get role by name or ID. Tries name first, falls back to ID."""
    try:
        return await client.get_role_by_name(identifier)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            try:
                return await client.get_role(identifier)
            except httpx.HTTPStatusError as exc2:
                raise handle_api_error(exc2) from exc2
        raise handle_api_error(exc) from exc


async def create_role(client: DremioClient, name: str) -> dict:
    """Create a new role."""
    try:
        return await client.create_role({"name": name})
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def update_role(client: DremioClient, role_id: str, name: str) -> dict:
    """Update a role's name."""
    try:
        existing = await client.get_role(role_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    body = dict(existing)
    body["name"] = name
    try:
        return await client.update_role(role_id, body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def delete_role(client: DremioClient, role_id: str) -> dict:
    """Delete a role."""
    try:
        return await client.delete_role(role_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


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
    """List all roles in the organization."""
    client = _get_client()
    _run_command(list_roles(client), client, fmt, fields=fields)


@app.command("get")
def cli_get(
    identifier: str = typer.Argument(help="Role name or role ID"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """Get role details by name or ID."""
    client = _get_client()
    _run_command(get_role(client, identifier), client, fmt, fields=fields)


@app.command("create")
def cli_create(
    name: str = typer.Argument(help="Name for the new role"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Create a new role."""
    client = _get_client()
    _run_command(create_role(client, name), client, fmt)


@app.command("update")
def cli_update(
    role_id: str = typer.Argument(help="Role ID to update"),
    name: str = typer.Option(..., "--name", help="New role name"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Update a role's name."""
    client = _get_client()
    _run_command(update_role(client, role_id, name), client, fmt)


@app.command("delete")
def cli_delete(
    role_id: str = typer.Argument(help="Role ID to delete"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show role details without deleting"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Delete a role. Cannot be undone."""
    client = _get_client()
    if dry_run:
        _run_command(get_role(client, role_id), client, fmt)
        return
    _run_command(delete_role(client, role_id), client, fmt)
