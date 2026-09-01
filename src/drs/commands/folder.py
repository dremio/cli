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
"""dremio folder — manage nested folders and list top-level catalog entities."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import typer

from drs.client import DremioClient
from drs.commands.query import run_query
from drs.output import OutputFormat, error, output, warn
from drs.utils import (
    DremioAPIError,
    NestedPathUnsupported,
    handle_api_error,
    parse_path,
    quote_path_sql,
    sanitize_input,
)

app = typer.Typer(
    help="Manage nested folders and list top-level catalog entities. Use `dremio space` for top-level spaces.",
    context_settings={"help_option_names": ["-h", "--help"]},
)


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
    """Create a folder at the given path using SQL."""
    parts = parse_path(path)
    if len(parts) == 1:
        warn(
            f"Top-level folder creation is deprecated. "
            f"Use `dremio space create {parts[0]}` instead. "
            "On Dremio Cloud, top-level folder creation is deprecated and may fail once spaces are enforced."
        )
    quoted = quote_path_sql(path)
    result = await run_query(client, f"CREATE FOLDER {quoted}")
    if result.get("state") == "FAILED":
        raise DremioAPIError(0, result.get("error", ""))
    return result


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


async def get_folder(client: DremioClient, path: str) -> dict:
    """Get a folder by path; rejects top-level (single-component) paths."""
    parts = parse_path(path)
    if len(parts) == 1:
        raise NestedPathUnsupported(parts[0], "folder.get", f"dremio space get {parts[0]}")
    return await get_entity(client, path)


async def delete_folder(client: DremioClient, path: str) -> dict:
    """Delete a folder by path; rejects top-level (single-component) paths."""
    parts = parse_path(path)
    if len(parts) == 1:
        raise NestedPathUnsupported(parts[0], "folder.delete", f"dremio space delete {parts[0]}")
    return await delete_entity(client, path)


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


def _split_slash_path(path: str) -> list[str]:
    """Split a slash-separated relative path into validated path parts."""
    sanitize_input(path, "path")
    parts = [part.strip() for part in path.split("/") if part.strip()]
    if not parts:
        raise ValueError("Path is empty.")
    if any(part in {".", ".."} for part in parts):
        raise ValueError(f"Invalid path '{path}': '.' and '..' path segments are not allowed.")
    return parts


async def promote_path_as_table(client: DremioClient, path_parts: list[str], format_type: str = "Delta") -> dict:
    """Format a file or folder as a physical dataset using the Catalog API."""
    try:
        entity = await client.get_catalog_by_path(path_parts)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc

    body = {
        "entityType": "dataset",
        "type": "PHYSICAL_DATASET",
        # Use the caller-resolved catalog path parts directly. The lookup response
        # can normalize or collapse file-source segments containing dots, which
        # breaks the subsequent format request for file/folder sources.
        "path": path_parts,
        "format": {"type": format_type},
    }

    try:
        return await client.format_catalog_table(entity["id"], body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def promote_folder(client: DremioClient, path: str, format_type: str = "Delta") -> dict:
    """Format a dot-separated file or folder path as a physical dataset."""
    return await promote_path_as_table(client, parse_path(path), format_type=format_type)


async def promote_from_file(
    client: DremioClient,
    paths_file: Path,
    source: str,
    under: str | None = None,
    format_type: str = "Delta",
) -> dict:
    """Format multiple slash-separated relative paths from a file as datasets."""
    source_part = sanitize_input(source.strip(), "source")
    if not source_part:
        raise ValueError("Source is empty.")

    base_parts = [source_part]
    if under:
        base_parts.extend(_split_slash_path(under))

    results: list[dict] = []
    for line_no, raw in enumerate(paths_file.read_text(encoding="utf-8").splitlines(), start=1):
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            continue

        rel_parts = _split_slash_path(raw)
        full_parts = [*base_parts, *rel_parts]
        result = await promote_path_as_table(client, full_parts, format_type=format_type)
        results.append(
            {
                "line": line_no,
                "input": raw,
                "path": result.get("path", full_parts),
                "id": result.get("id"),
                "entityType": result.get("entityType"),
                "type": result.get("type"),
                "format": result.get("format"),
            }
        )

    return {
        "source": source_part,
        "under": under,
        "formatType": format_type,
        "count": len(results),
        "results": results,
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
        if isinstance(exc, (DremioAPIError, NestedPathUnsupported)):
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
    _run_command(get_folder(client, path), client, fmt, fields=fields)


@app.command("create")
def cli_create(
    path: str = typer.Argument(
        help="Dot-separated folder path (e.g., myspace.newfolder). For top-level spaces use `dremio space create`."
    ),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Create a folder at the given path.

    Nested paths (e.g. 'Analytics.reports') create a folder inside a space.
    Single-component paths attempt top-level creation for compatibility with
    older Dremio Cloud deployments; this is deprecated — use `dremio space create`
    instead. On deployments where spaces are enforced, single-component paths
    will fail server-side.
    """
    client = _get_client()
    _run_command(create_folder(client, path), client, fmt)


@app.command("delete")
def cli_delete(
    path: str = typer.Argument(help="Dot-separated entity path to delete"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be deleted without deleting"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Delete a nested catalog entity by path. Cannot be undone.

    Single-component paths (top-level spaces) are rejected — use `dremio space delete` instead.
    """
    client = _get_client()
    if dry_run:
        _run_command(get_folder(client, path), client, fmt)
        return
    _run_command(delete_folder(client, path), client, fmt)


@app.command("grants")
def cli_grants(
    path: str = typer.Argument(help="Dot-separated entity path"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Show ACL grants on a catalog entity."""
    client = _get_client()
    _run_command(grants(client, path), client, fmt)


@app.command("promote")
def cli_promote(
    path: str = typer.Argument(
        None,
        help="Dot-separated file or folder path to format as a table (e.g., source.folder.table_dir)",
    ),
    paths_file: Path | None = typer.Option(
        None,
        "--paths-file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="File containing slash-separated relative paths, one per line",
    ),
    source: str | None = typer.Option(
        None,
        "--source",
        help="Source name to prefix to every line from --paths-file",
    ),
    under: str | None = typer.Option(
        None,
        "--under",
        help="Optional slash-separated base path under the source for every line from --paths-file",
    ),
    format_type: str = typer.Option("Delta", "--format-type", help="Dataset format type to promote as"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """Format a file or folder as a table using the Catalog API.

    For batch promotion, pass --paths-file with slash-separated relative paths and
    prefix them with --source and optional --under.
    """
    if bool(path) == bool(paths_file):
        error("Provide exactly one of PATH or --paths-file.")
        raise typer.Exit(1)

    if paths_file and not source:
        error("--source is required when using --paths-file.")
        raise typer.Exit(1)

    client = _get_client()
    if paths_file:
        _run_command(
            promote_from_file(client, paths_file, source, under=under, format_type=format_type), client, fmt, fields
        )
        return
    _run_command(promote_folder(client, path, format_type=format_type), client, fmt, fields=fields)
