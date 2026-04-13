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
"""dremio reflection — manage Dremio reflections (materialized views)."""

from __future__ import annotations

import asyncio

import httpx
import typer

from drs.client import DremioClient
from drs.commands.query import run_query
from drs.output import OutputFormat, error, output
from drs.utils import handle_api_error, parse_path

app = typer.Typer(
    help="Manage reflections (materialized views).", context_settings={"help_option_names": ["-h", "--help"]}
)


async def create(client: DremioClient, path: str, rtype: str, display_fields: list[str] | None = None) -> dict:
    """Create a reflection on a dataset."""
    parts = parse_path(path)
    try:
        entity = await client.get_catalog_by_path(parts)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc

    dataset_id = entity["id"]
    body: dict = {
        "type": rtype.upper(),
        "datasetId": dataset_id,
    }

    fields = entity.get("fields", [])
    if rtype.lower() == "raw":
        display = display_fields or [f["name"] for f in fields]
        body["displayFields"] = [{"name": n} for n in display]
    elif rtype.lower() == "aggregation":
        if display_fields:
            body["dimensionFields"] = [{"name": n, "granularity": "DATE"} for n in display_fields]

    try:
        return await client.create_reflection(body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def list_reflections(
    client: DremioClient,
    path: str | None = None,
    *,
    rtype: str | None = None,
    status: str | None = None,
    dataset_name: str | None = None,
    limit: int | None = None,
) -> dict:
    """List reflections via sys.project.reflections.

    When *path* is given, only reflections for that dataset are returned.
    When omitted, all reflections in the project are returned.
    Optional filters narrow results by *rtype*, *status*, or *dataset_name*.
    An optional *limit* caps the number of rows returned.
    """
    sql = "SELECT * FROM sys.project.reflections"
    conditions: list[str] = []
    if path is not None:
        parts = parse_path(path)
        try:
            entity = await client.get_catalog_by_path(parts)
        except httpx.HTTPStatusError as exc:
            raise handle_api_error(exc) from exc
        dataset_id = entity["id"]
        conditions.append(f"dataset_id = '{dataset_id}'")
    if rtype is not None:
        conditions.append(f"type = '{rtype.upper()}'")
    if status is not None:
        conditions.append(f"status = '{status.upper()}'")
    if dataset_name is not None:
        conditions.append(f"dataset_name ILIKE '%{dataset_name}%'")
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    if limit is not None:
        sql += f" LIMIT {limit}"
    return await run_query(client, sql)


async def get_reflection(client: DremioClient, reflection_id: str) -> dict:
    """Get detailed status of a reflection."""
    try:
        return await client.get_reflection(reflection_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def refresh(client: DremioClient, reflection_id: str) -> dict:
    """Trigger an immediate refresh of a reflection."""
    try:
        return await client.refresh_reflection(reflection_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def delete(client: DremioClient, reflection_id: str) -> dict:
    """Delete a reflection."""
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


@app.command("create")
def cli_create(
    path: str = typer.Argument(help="Dot-separated dataset path to create a reflection on"),
    rtype: str = typer.Option("raw", "--type", "-t", help="Reflection type: raw or aggregation"),
    fields_list: str = typer.Option(None, "--fields", "-f", help="Comma-separated field names to include"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Create a new reflection on a dataset."""
    client = _get_client()
    display = [f.strip() for f in fields_list.split(",") if f.strip()] if fields_list else None
    _run_command(create(client, path, rtype, display_fields=display), client, fmt)


@app.command("list")
def cli_list(
    path: str = typer.Argument(None, help="Dot-separated dataset path (omit to list all reflections)"),
    rtype: str = typer.Option(None, "--type", "-t", help="Filter by reflection type: raw or aggregation"),
    status: str = typer.Option(None, "--status", "-s", help="Filter by status (e.g. CAN_ACCELERATE, FAILED, EXPIRED)"),
    dataset_name: str = typer.Option(None, "--dataset-name", "-d", help="Filter by dataset name (substring match)"),
    limit: int = typer.Option(None, "--limit", "-l", help="Maximum number of reflections to return"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """List reflections. Shows all project reflections, or those for a specific dataset."""
    client = _get_client()
    _run_command(
        list_reflections(client, path, rtype=rtype, status=status, dataset_name=dataset_name, limit=limit),
        client,
        fmt,
    )


@app.command("get")
def cli_get(
    reflection_id: str = typer.Argument(help="Reflection ID"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Get detailed status of a reflection."""
    client = _get_client()
    _run_command(get_reflection(client, reflection_id), client, fmt)


@app.command("refresh")
def cli_refresh(
    reflection_id: str = typer.Argument(help="Reflection ID to refresh"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate without executing"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Trigger an immediate refresh of a reflection."""
    if dry_run:
        client = _get_client()
        _run_command(get_reflection(client, reflection_id), client, fmt)
        return
    client = _get_client()
    _run_command(refresh(client, reflection_id), client, fmt)


@app.command("delete")
def cli_delete(
    reflection_id: str = typer.Argument(help="Reflection ID to delete"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate without executing"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Permanently delete a reflection. Cannot be undone."""
    if dry_run:
        client = _get_client()
        _run_command(get_reflection(client, reflection_id), client, fmt)
        return
    client = _get_client()
    _run_command(delete(client, reflection_id), client, fmt)
