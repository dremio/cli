"""drs query — run SQL queries against Dremio."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import typer

from drs.client import DremioClient
from drs.output import OutputFormat, output, error
from drs.utils import handle_api_error

app = typer.Typer(help="Run SQL queries against Dremio.")

TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELED", "CANCELLED"}
POLL_INTERVAL = 1.0
MAX_POLL_INTERVAL = 10.0


async def run_query(client: DremioClient, sql: str, context: list[str] | None = None) -> dict[str, Any]:
    """Submit SQL, poll until done, return results. Core logic shared by CLI and MCP."""
    try:
        job = await client.submit_sql(sql, context=context)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc

    job_id = job["id"]

    interval = POLL_INTERVAL
    while True:
        status = await client.get_job_status(job_id)
        state = status.get("jobState", status.get("state", "UNKNOWN"))
        if state in TERMINAL_STATES:
            break
        await asyncio.sleep(interval)
        interval = min(interval * 1.5, MAX_POLL_INTERVAL)

    if state != "COMPLETED":
        return {"job_id": job_id, "state": state, "error": status.get("errorMessage", "")}

    row_count = status.get("rowCount", 0)
    all_rows: list[dict] = []
    offset = 0
    limit = 500
    while offset < row_count:
        page = await client.get_job_results(job_id, limit=limit, offset=offset)
        columns = page.get("columns", [])
        col_names = [c.get("name", f"col_{i}") for i, c in enumerate(columns)]
        for raw_row in page.get("rows", []):
            row = dict(zip(col_names, raw_row.get("values", raw_row.get("row", []))))
            all_rows.append(row)
        offset += limit

    return {"job_id": job_id, "state": state, "rowCount": len(all_rows), "rows": all_rows}


async def get_status(client: DremioClient, job_id: str) -> dict:
    try:
        return await client.get_job_status(job_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def cancel(client: DremioClient, job_id: str) -> dict:
    try:
        return await client.cancel_job(job_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


# -- CLI wrappers --

def _get_client() -> DremioClient:
    from drs.cli import get_client
    return get_client()


def _run_command(coro, client, fmt: OutputFormat = OutputFormat.json, fields: str | None = None) -> None:
    """Run an async command with error handling and cleanup."""
    try:
        result = asyncio.run(coro)
    except Exception as exc:
        from drs.utils import DremioAPIError
        if isinstance(exc, DremioAPIError):
            error(str(exc))
            raise typer.Exit(1)
        raise
    finally:
        asyncio.run(client.close())
    output(result, fmt, fields=fields)


@app.command("run")
def cli_run(
    sql: str = typer.Argument(help="SQL query to execute"),
    context: str = typer.Option(None, help="Dot-separated default schema context (e.g., myspace.folder)"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include in output (reduces context window usage)"),
) -> None:
    """Execute a SQL query, wait for completion, and return results.

    Submits the query, polls until the job completes, then fetches all result
    rows. For long-running queries, use 'drs query run' to submit and
    'drs query status' to check progress separately.
    """
    client = _get_client()
    ctx = context.split(".") if context else None
    try:
        result = asyncio.run(run_query(client, sql, context=ctx))
    except Exception as exc:
        asyncio.run(client.close())
        from drs.utils import DremioAPIError
        if isinstance(exc, DremioAPIError):
            error(str(exc))
            raise typer.Exit(1)
        raise
    asyncio.run(client.close())
    if result.get("state") != "COMPLETED":
        error(f"Query {result.get('state')}: {result.get('error', '')}")
        raise typer.Exit(1)
    output(result, fmt, fields=fields)


@app.command("status")
def cli_status(
    job_id: str = typer.Argument(help="Job ID (UUID) to check"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include in output"),
) -> None:
    """Check the status of a running or completed job by its ID."""
    client = _get_client()
    _run_command(get_status(client, job_id), client, fmt, fields=fields)


@app.command("cancel")
def cli_cancel(
    job_id: str = typer.Argument(help="Job ID (UUID) to cancel"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate the job ID without cancelling"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Cancel a running job. No effect if the job has already completed.

    Use --dry-run to check the job status without cancelling it.
    """
    if dry_run:
        client = _get_client()
        _run_command(get_status(client, job_id), client, fmt)
        return
    client = _get_client()
    _run_command(cancel(client, job_id), client, fmt)
