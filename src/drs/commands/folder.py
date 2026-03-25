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
"""dremio folder — manage spaces and folders in the Dremio catalog."""

from __future__ import annotations

import asyncio

import httpx
import typer

from drs.client import DremioClient
from drs.commands.query import run_query
from drs.output import OutputFormat, error, output
from drs.utils import handle_api_error, parse_path, quote_path_sql

app = typer.Typer(help="Manage spaces and folders in the Dremio catalog.", context_settings={"help_option_names": ["-h", "--help"]})


async def list_catalog(client: DremioClient) -> dict:
    """List top-level catalog entities (sources, spaces, home)."""
    try:
        root = await client.get_catalog_entity("")
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    children = root.get("data", root.get("children", []))
    return {"entities": children}


async def get_entity(client: DremioClient, path: str) -> dict:
    """Get a catalog entity by dot-separated path."""
    parts = parse_path(path)
    try:
        return await client.get_catalog_by_path(parts)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def create_folder(client: DremioClient, path: str) -> dict:
    """Create a space (single component) or folder (nested path) using SQL."""
    parts = parse_path(path)
    if len(parts) == 1:
        sql = f'CREATE SPACE "{parts[0]}"'
    else:
        quoted = quote_path_sql(path)
        sql = f"CREATE FOLDER {quoted}"
    return await run_query(client, sql)


async def delete_entity(client: DremioClient, path: str) -> dict:
    """Delete a catalog entity by path."""
    parts = parse_path(path)
    try:
        entity = await client.get_catalog_by_path(parts)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    entity_id = entity["id"]
    tag = entity.get("tag")
    try:
        return await client.delete_catalog_entity(entity_id, tag=tag)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def grants(client: DremioClient, path: str) -> dict:
    """Get ACL grants on a catalog entity."""
    parts = parse_path(path)
    try:
        entity = await client.get_catalog_by_path(parts)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    acl = entity.get("accessControlList", {})
    return {
        "path": path,
        "id": entity.get("id"),
        "accessControlList": acl,
    }


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
    """List top-level catalog entities: sources, spaces, and home folder."""
    client = _get_client()
    _run_command(list_catalog(client), client, fmt, fields=fields)


@app.command("get")
def cli_get(
    path: str = typer.Argument(help="Dot-separated entity path (e.g., myspace.folder.table)"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """Get full metadata for a catalog entity by path."""
    client = _get_client()
    _run_command(get_entity(client, path), client, fmt, fields=fields)


@app.command("create")
def cli_create(
    path: str = typer.Argument(
        help="Space name (single component) or dot-separated folder path (e.g., myspace.newfolder)"
    ),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Create a space or folder.

    Single path component (e.g., 'Analytics') creates a space.
    Nested path (e.g., 'Analytics.reports') creates a folder.
    """
    client = _get_client()
    _run_command(create_folder(client, path), client, fmt)


@app.command("delete")
def cli_delete(
    path: str = typer.Argument(help="Dot-separated entity path to delete"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be deleted without deleting"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Delete a catalog entity (space, folder, view, etc.). Cannot be undone."""
    client = _get_client()
    if dry_run:
        _run_command(get_entity(client, path), client, fmt)
        return
    _run_command(delete_entity(client, path), client, fmt)


@app.command("grants")
def cli_grants(
    path: str = typer.Argument(help="Dot-separated entity path"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Show ACL grants on a catalog entity."""
    client = _get_client()
    _run_command(grants(client, path), client, fmt)
