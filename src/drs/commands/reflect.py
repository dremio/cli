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
"""drs reflect — manage Dremio reflections (materialized views)."""

from __future__ import annotations

import asyncio

import httpx
import typer

from drs.client import DremioClient
from drs.commands.query import run_query
from drs.output import OutputFormat, output, error
from drs.utils import handle_api_error, parse_path

app = typer.Typer(help="Manage reflections (materialized views).")


async def list_reflections(client: DremioClient, path: str) -> dict:
    """List reflections on a dataset via sys.project.reflections."""
    parts = parse_path(path)
    try:
        entity = await client.get_catalog_by_path(parts)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    dataset_id = entity["id"]
    sql = f"SELECT * FROM sys.project.reflections WHERE dataset_id = '{dataset_id}'"
    return await run_query(client, sql)


async def status(client: DremioClient, reflection_id: str) -> dict:
    try:
        return await client.get_reflection(reflection_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def refresh(client: DremioClient, reflection_id: str) -> dict:
    try:
        return await client.refresh_reflection(reflection_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def drop(client: DremioClient, reflection_id: str) -> dict:
    try:
        return await client.delete_reflection(reflection_id)
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
    path: str = typer.Argument(help="Dot-separated dataset path to list reflections for"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """List all reflections defined on a dataset.

    Queries sys.project.reflections filtered by the dataset ID. Shows reflection type,
    status, and configuration.
    """
    client = _get_client()
    _run_command(list_reflections(client, path), client, fmt)


@app.command("status")
def cli_status(
    reflection_id: str = typer.Argument(help="Reflection ID (get IDs from 'drs reflect list')"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Show detailed status of a reflection.

    Includes freshness, staleness, size, last refresh time, and configuration.
    """
    client = _get_client()
    _run_command(status(client, reflection_id), client, fmt)


@app.command("refresh")
def cli_refresh(
    reflection_id: str = typer.Argument(help="Reflection ID to refresh"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate the request without executing it"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Trigger an immediate refresh of a reflection.

    Use --dry-run to validate the reflection ID without triggering the refresh.
    """
    if dry_run:
        client = _get_client()
        _run_command(status(client, reflection_id), client, fmt)
        return
    client = _get_client()
    _run_command(refresh(client, reflection_id), client, fmt)


@app.command("drop")
def cli_drop(
    reflection_id: str = typer.Argument(help="Reflection ID to delete"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate the request without executing it"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Permanently delete a reflection. Cannot be undone.

    Use --dry-run to verify the reflection exists before deleting.
    """
    if dry_run:
        client = _get_client()
        _run_command(status(client, reflection_id), client, fmt)
        return
    client = _get_client()
    _run_command(drop(client, reflection_id), client, fmt)
