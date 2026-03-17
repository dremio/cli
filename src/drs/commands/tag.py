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
"""dremio tag — get and update tags on catalog entities."""

from __future__ import annotations

import asyncio

import httpx
import typer

from drs.client import DremioClient
from drs.output import OutputFormat, output, error
from drs.utils import handle_api_error, parse_path

app = typer.Typer(help="Get and update tags on catalog entities.")


async def get_tags(client: DremioClient, path: str) -> dict:
    """Get tags for an entity."""
    parts = parse_path(path)
    try:
        entity = await client.get_catalog_by_path(parts)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    entity_id = entity["id"]

    tags_list: list[str] = []
    try:
        tags_data = await client.get_tags(entity_id)
        tags_list = tags_data.get("tags", [])
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            pass  # no tags exist for this entity
        else:
            raise handle_api_error(exc) from exc

    return {
        "path": path,
        "id": entity_id,
        "tags": tags_list,
    }


async def update_tags(client: DremioClient, path: str, tags: list[str]) -> dict:
    """Set tags for an entity."""
    parts = parse_path(path)
    try:
        entity = await client.get_catalog_by_path(parts)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    entity_id = entity["id"]

    # Try to get existing tags for version number
    version = None
    try:
        existing = await client.get_tags(entity_id)
        version = existing.get("version")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            pass  # no tags exist yet
        else:
            raise handle_api_error(exc) from exc

    try:
        result = await client.set_tags(entity_id, tags, version=version)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    return {"path": path, "id": entity_id, "tags": tags, "result": result}


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


@app.command("get")
def cli_get(
    path: str = typer.Argument(help='Dot-separated entity path'),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Get tags for a catalog entity."""
    client = _get_client()
    _run_command(get_tags(client, path), client, fmt)


@app.command("update")
def cli_update(
    path: str = typer.Argument(help='Dot-separated entity path'),
    tags: str = typer.Argument(help='Comma-separated list of tags (e.g., "pii,finance,daily")'),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Set tags on a catalog entity. Replaces all existing tags."""
    client = _get_client()
    tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    _run_command(update_tags(client, path, tag_list), client, fmt)
