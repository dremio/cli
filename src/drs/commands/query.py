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
        try:
            status = await client.get_job_status(job_id)
        except httpx.HTTPStatusError as exc:
            raise handle_api_error(exc) from exc
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
        try:
            page = await client.get_job_results(job_id, limit=limit, offset=offset)
        except httpx.HTTPStatusError as exc:
            raise handle_api_error(exc) from exc
        for raw_row in page.get("rows", []):
            if isinstance(raw_row, dict) and "values" not in raw_row and "row" not in raw_row:
                # API returns rows as named dicts already (e.g., {"col1": "val1"})
                all_rows.append(raw_row)
            else:
                # Fallback: rows as list of values with separate column schema
                columns = page.get("schema", page.get("columns", []))
                col_names = [c.get("name", f"col_{i}") for i, c in enumerate(columns)]
                values = raw_row.get("values", raw_row.get("row", []))
                all_rows.append(dict(zip(col_names, values)))
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

    async def _execute():
        try:
            return await run_query(client, sql, context=ctx)
        finally:
            await client.close()

    try:
        result = asyncio.run(_execute())
    except Exception as exc:
        from drs.utils import DremioAPIError
        if isinstance(exc, DremioAPIError):
            error(str(exc))
            raise typer.Exit(1)
        raise
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
