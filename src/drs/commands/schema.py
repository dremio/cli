"""drs schema — describe tables, trace lineage, sample data."""

from __future__ import annotations

import asyncio

import httpx
import typer

from drs.client import DremioClient
from drs.commands.query import run_query
from drs.output import OutputFormat, output, error
from drs.utils import handle_api_error, parse_path, quote_path_sql

app = typer.Typer(help="Describe schemas, trace lineage, and sample data.")


async def describe(client: DremioClient, path: str) -> dict:
    """Get column names, types, and nullability for a table/view."""
    parts = parse_path(path)
    try:
        entity = await client.get_catalog_by_path(parts)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    fields = entity.get("fields", [])
    columns = [
        {"name": f["name"], "type": f["type"]["name"], "nullable": f.get("isNullable", True)}
        for f in fields
    ]
    return {
        "path": path,
        "entityType": entity.get("entityType"),
        "columns": columns,
    }


async def lineage(client: DremioClient, path: str) -> dict:
    """Get upstream and downstream dependency graph."""
    parts = parse_path(path)
    try:
        entity = await client.get_catalog_by_path(parts)
        entity_id = entity["id"]
        graph = await client.get_lineage(entity_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    return {"path": path, "id": entity_id, "graph": graph}


async def wiki(client: DremioClient, path: str) -> dict:
    """Get wiki description and tags for an entity."""
    parts = parse_path(path)
    try:
        entity = await client.get_catalog_by_path(parts)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    entity_id = entity["id"]

    wiki_text = ""
    tags_list: list[str] = []
    try:
        wiki_data = await client.get_wiki(entity_id)
        wiki_text = wiki_data.get("text", "")
    except httpx.HTTPStatusError:
        pass  # 404 is expected when no wiki exists
    try:
        tags_data = await client.get_tags(entity_id)
        tags_list = tags_data.get("tags", [])
    except httpx.HTTPStatusError:
        pass  # 404 is expected when no tags exist

    return {
        "path": path,
        "id": entity_id,
        "wiki": wiki_text,
        "tags": tags_list,
    }


async def sample(client: DremioClient, path: str, limit: int = 10) -> dict:
    """Return sample rows from a table/view."""
    quoted = quote_path_sql(path)
    sql = f"SELECT * FROM {quoted} LIMIT {limit}"
    return await run_query(client, sql)


# -- CLI wrappers --

def _get_client() -> DremioClient:
    from drs.cli import get_client
    return get_client()


def _run_command(coro, client, fmt: OutputFormat = OutputFormat.json, fields: str | None = None) -> None:
    try:
        result = asyncio.run(coro)
    except Exception as exc:
        asyncio.run(client.close())
        from drs.utils import DremioAPIError
        if isinstance(exc, DremioAPIError):
            error(str(exc))
            raise typer.Exit(1)
        raise
    asyncio.run(client.close())
    output(result, fmt, fields=fields)


@app.command("describe")
def cli_describe(
    path: str = typer.Argument(help='Dot-separated table/view path (e.g., myspace.mytable)'),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include (e.g., 'columns.name,columns.type')"),
) -> None:
    """Show column names, data types, and nullability for a table or view."""
    client = _get_client()
    _run_command(describe(client, path), client, fmt, fields=fields)


@app.command("lineage")
def cli_lineage(
    path: str = typer.Argument(help='Dot-separated table/view path'),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Show upstream and downstream dependency graph for a table or view."""
    client = _get_client()
    _run_command(lineage(client, path), client, fmt)


@app.command("wiki")
def cli_wiki(
    path: str = typer.Argument(help='Dot-separated entity path'),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Show wiki description and tags for a catalog entity."""
    client = _get_client()
    _run_command(wiki(client, path), client, fmt)


@app.command("sample")
def cli_sample(
    path: str = typer.Argument(help='Dot-separated table/view path'),
    limit: int = typer.Option(10, help="Number of sample rows (default 10)"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include in output"),
) -> None:
    """Return sample rows from a table or view (default 10 rows)."""
    client = _get_client()
    _run_command(sample(client, path, limit=limit), client, fmt, fields=fields)
