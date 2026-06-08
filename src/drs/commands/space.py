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
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager

import typer

from drs.client import DremioClient
from drs.commands.folder import delete_entity, get_entity, list_catalog
from drs.commands.query import run_query
from drs.output import OutputFormat, error, output
from drs.utils import (
    DremioAPIError,
    NestedPathUnsupported,
    SpaceEntityTypeUnsupported,
    catalog_entity_kind,
    parse_path,
    quote_path_sql,
)

app = typer.Typer(help="Manage spaces in the Dremio catalog.", context_settings={"help_option_names": ["-h", "--help"]})


async def list_spaces(client: DremioClient) -> dict:
    """List all spaces in the catalog."""
    result = await list_catalog(client)
    spaces = [e for e in result.get("entities", []) if catalog_entity_kind(e) == "SPACE"]
    return {"entities": spaces}


_LEGACY_SPACE_NOT_SUPPORTED = "Legacy spaces are not supported"


async def create_space(client: DremioClient, name: str) -> dict:
    """Create a space.

    Uses CREATE SPACE SQL. On older Dremio Cloud deployments that reject it with
    "Legacy spaces are not supported", falls back to CREATE FOLDER with a
    single-component path. All other failures are propagated as DremioAPIError.
    """
    quoted = quote_path_sql(name)
    result = await run_query(client, f"CREATE SPACE {quoted}")
    if result.get("state") == "FAILED":
        err = result.get("error", "")
        if _LEGACY_SPACE_NOT_SUPPORTED in err:
            fallback = await run_query(client, f"CREATE FOLDER {quoted}")
            if fallback.get("state") == "FAILED":
                fallback_err = fallback.get("error", "")
                if "already exists" in fallback_err:
                    raise DremioAPIError(0, f"Space [{name}] already exists.")
                raise DremioAPIError(0, f"{err}; fallback CREATE FOLDER also failed: {fallback_err}")
            return fallback
        raise DremioAPIError(0, err)
    return result


async def get_space(client: DremioClient, name: str) -> dict:
    """Get space metadata by name."""
    if len(parse_path(name)) > 1:
        raise NestedPathUnsupported(name, "space.get", f"dremio folder get {name}")
    entity = await get_entity(client, name)
    _require_space_entity(name, entity)
    return entity


async def delete_space(client: DremioClient, name: str) -> dict:
    """Delete a space by name."""
    if len(parse_path(name)) > 1:
        raise NestedPathUnsupported(name, "space.delete", f"dremio folder delete {name}")
    entity = await get_entity(client, name)
    _require_space_entity(name, entity)
    return await delete_entity(client, name)


def _require_space_entity(name: str, entity: dict) -> None:
    kind = catalog_entity_kind(entity)
    if kind != "SPACE":
        kind = kind.lower() or "unknown entity"
        raise SpaceEntityTypeUnsupported(name, kind)


@asynccontextmanager
async def managed_client():
    from drs.cli import get_client

    client = get_client()
    try:
        yield client
    finally:
        await client.close()


async def _execute_command(command: Callable[[DremioClient], Awaitable[dict]]) -> dict:
    async with managed_client() as client:
        return await command(client)


@app.command("list")
def cli_list(
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """List all spaces in the catalog."""
    try:
        result = asyncio.run(_execute_command(list_spaces))
    except (DremioAPIError, NestedPathUnsupported) as exc:
        error(str(exc))
        raise typer.Exit(1)
    output(result, fmt, fields=fields)


@app.command("get")
def cli_get(
    name: str = typer.Argument(help="Space name"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """Get metadata for a space by name."""
    try:
        result = asyncio.run(_execute_command(lambda client: get_space(client, name)))
    except (DremioAPIError, NestedPathUnsupported, SpaceEntityTypeUnsupported) as exc:
        error(str(exc))
        raise typer.Exit(1)
    output(result, fmt, fields=fields)


@app.command("create")
def cli_create(
    name: str = typer.Argument(help="Space name to create"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Create a new space."""
    try:
        result = asyncio.run(_execute_command(lambda client: create_space(client, name)))
    except (DremioAPIError, NestedPathUnsupported) as exc:
        error(str(exc))
        raise typer.Exit(1)
    output(result, fmt)


@app.command("delete")
def cli_delete(
    name: str = typer.Argument(help="Space name to delete"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be deleted without deleting"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Delete a space. Cannot be undone."""
    if dry_run:
        try:
            result = asyncio.run(_execute_command(lambda client: _dry_run_space_delete(client, name)))
        except (DremioAPIError, NestedPathUnsupported, SpaceEntityTypeUnsupported) as exc:
            error(str(exc))
            raise typer.Exit(1)
        output(result, fmt)
        return
    try:
        result = asyncio.run(_execute_command(lambda client: delete_space(client, name)))
    except (DremioAPIError, NestedPathUnsupported, SpaceEntityTypeUnsupported) as exc:
        error(str(exc))
        raise typer.Exit(1)
    output(result, fmt)


async def _dry_run_space_delete(client: DremioClient, name: str) -> dict:
    return await get_space(client, name)
