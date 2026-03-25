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
"""dremio wiki — get and update wiki descriptions on catalog entities."""

from __future__ import annotations

import asyncio

import httpx
import typer

from drs.client import DremioClient
from drs.output import OutputFormat, error, output
from drs.utils import handle_api_error, parse_path

app = typer.Typer(
    help="Get and update wiki descriptions on catalog entities.",
    context_settings={"help_option_names": ["-h", "--help"]},
)


async def get_wiki(client: DremioClient, path: str) -> dict:
    """Get wiki description for an entity."""
    parts = parse_path(path)
    try:
        entity = await client.get_catalog_by_path(parts)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    entity_id = entity["id"]

    wiki_text = ""
    try:
        wiki_data = await client.get_wiki(entity_id)
        wiki_text = wiki_data.get("text", "")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            pass  # no wiki exists for this entity
        else:
            raise handle_api_error(exc) from exc

    return {
        "path": path,
        "id": entity_id,
        "wiki": wiki_text,
    }


async def update_wiki(client: DremioClient, path: str, text: str) -> dict:
    """Set or update wiki description text for an entity."""
    parts = parse_path(path)
    try:
        entity = await client.get_catalog_by_path(parts)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    entity_id = entity["id"]

    # Try to get existing wiki for version number (optimistic concurrency)
    version = None
    try:
        existing = await client.get_wiki(entity_id)
        version = existing.get("version")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            pass  # no wiki exists yet
        else:
            raise handle_api_error(exc) from exc

    try:
        result = await client.set_wiki(entity_id, text, version=version)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    return {"path": path, "id": entity_id, "wiki": text, "result": result}


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
    path: str = typer.Argument(help="Dot-separated entity path"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Get wiki description for a catalog entity."""
    client = _get_client()
    _run_command(get_wiki(client, path), client, fmt)


@app.command("update")
def cli_update(
    path: str = typer.Argument(help="Dot-separated entity path"),
    text: str = typer.Argument(help="Wiki text to set (Markdown supported)"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Set or update the wiki description for a catalog entity."""
    client = _get_client()
    _run_command(update_wiki(client, path, text), client, fmt)
