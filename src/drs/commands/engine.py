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
"""drs engine — manage Dremio Cloud engines."""

from __future__ import annotations

import asyncio

import httpx
import typer

from drs.client import DremioClient
from drs.output import OutputFormat, output, error
from drs.utils import handle_api_error

app = typer.Typer(help="Manage Dremio Cloud engines.")


async def list_engines(client: DremioClient) -> dict:
    """List all engines in the project."""
    try:
        return await client.list_engines()
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def get_engine(client: DremioClient, engine_id: str) -> dict:
    """Get engine details by ID."""
    try:
        return await client.get_engine(engine_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def create_engine(client: DremioClient, name: str, size: str = "SMALL") -> dict:
    """Create a new engine."""
    body = {"name": name, "size": size.upper()}
    try:
        return await client.create_engine(body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def update_engine(client: DremioClient, engine_id: str, name: str | None = None, size: str | None = None) -> dict:
    """Update engine configuration."""
    try:
        existing = await client.get_engine(engine_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    body = dict(existing)
    if name:
        body["name"] = name
    if size:
        body["size"] = size.upper()
    try:
        return await client.update_engine(engine_id, body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def delete_engine(client: DremioClient, engine_id: str) -> dict:
    """Delete an engine."""
    try:
        return await client.delete_engine(engine_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def enable_engine(client: DremioClient, engine_id: str) -> dict:
    """Enable a disabled engine."""
    try:
        return await client.enable_engine(engine_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def disable_engine(client: DremioClient, engine_id: str) -> dict:
    """Disable a running engine."""
    try:
        return await client.disable_engine(engine_id)
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
    """List all engines in the project."""
    client = _get_client()
    _run_command(list_engines(client), client, fmt, fields=fields)


@app.command("get")
def cli_get(
    engine_id: str = typer.Argument(help="Engine ID"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """Get details for a specific engine."""
    client = _get_client()
    _run_command(get_engine(client, engine_id), client, fmt, fields=fields)


@app.command("create")
def cli_create(
    name: str = typer.Argument(help="Name for the new engine"),
    size: str = typer.Option("SMALL", "--size", "-s", help="Engine size (e.g., SMALL, MEDIUM, LARGE, XLARGE, XXLARGE)"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Create a new engine."""
    client = _get_client()
    _run_command(create_engine(client, name, size=size), client, fmt)


@app.command("update")
def cli_update(
    engine_id: str = typer.Argument(help="Engine ID to update"),
    name: str = typer.Option(None, "--name", help="New engine name"),
    size: str = typer.Option(None, "--size", "-s", help="New engine size"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Update engine configuration (name, size)."""
    client = _get_client()
    _run_command(update_engine(client, engine_id, name=name, size=size), client, fmt)


@app.command("delete")
def cli_delete(
    engine_id: str = typer.Argument(help="Engine ID to delete"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show engine details without deleting"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Delete an engine. Cannot be undone."""
    client = _get_client()
    if dry_run:
        _run_command(get_engine(client, engine_id), client, fmt)
        return
    _run_command(delete_engine(client, engine_id), client, fmt)


@app.command("enable")
def cli_enable(
    engine_id: str = typer.Argument(help="Engine ID to enable"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Enable a disabled engine."""
    client = _get_client()
    _run_command(enable_engine(client, engine_id), client, fmt)


@app.command("disable")
def cli_disable(
    engine_id: str = typer.Argument(help="Engine ID to disable"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Disable a running engine."""
    client = _get_client()
    _run_command(disable_engine(client, engine_id), client, fmt)
