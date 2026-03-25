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
"""dremio project — manage Dremio Cloud projects."""

from __future__ import annotations

import asyncio

import httpx
import typer

from drs.client import DremioClient
from drs.output import OutputFormat, error, output
from drs.utils import handle_api_error

app = typer.Typer(help="Manage Dremio Cloud projects.", context_settings={"help_option_names": ["-h", "--help"]})


async def list_projects(client: DremioClient) -> dict:
    """List all projects in the organization."""
    try:
        return await client.list_projects()
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def get_project(client: DremioClient, project_id: str) -> dict:
    """Get project details by ID."""
    try:
        return await client.get_project(project_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def create_project(client: DremioClient, name: str) -> dict:
    """Create a new project."""
    body = {"name": name}
    try:
        return await client.create_project(body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def update_project(client: DremioClient, project_id: str, name: str | None = None) -> dict:
    """Update project attributes."""
    try:
        existing = await client.get_project(project_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    body = dict(existing)
    if name:
        body["name"] = name
    try:
        return await client.update_project(project_id, body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def delete_project(client: DremioClient, project_id: str) -> dict:
    """Delete a project."""
    try:
        return await client.delete_project(project_id)
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
    """List all projects in the organization."""
    client = _get_client()
    _run_command(list_projects(client), client, fmt, fields=fields)


@app.command("get")
def cli_get(
    project_id: str = typer.Argument(help="Project ID (UUID)"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """Get details for a specific project."""
    client = _get_client()
    _run_command(get_project(client, project_id), client, fmt, fields=fields)


@app.command("create")
def cli_create(
    name: str = typer.Argument(help="Name for the new project"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Create a new project. Automatically provisions a default 2XS engine."""
    client = _get_client()
    _run_command(create_project(client, name), client, fmt)


@app.command("update")
def cli_update(
    project_id: str = typer.Argument(help="Project ID to update"),
    name: str = typer.Option(None, "--name", help="New project name"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Update project attributes (e.g., name)."""
    client = _get_client()
    _run_command(update_project(client, project_id, name=name), client, fmt)


@app.command("delete")
def cli_delete(
    project_id: str = typer.Argument(help="Project ID to delete"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show project details without deleting"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Delete a project. Cannot delete the sole project in an organization."""
    client = _get_client()
    if dry_run:
        _run_command(get_project(client, project_id), client, fmt)
        return
    _run_command(delete_project(client, project_id), client, fmt)
