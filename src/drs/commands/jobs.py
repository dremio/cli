"""drs jobs — list and inspect query jobs."""

from __future__ import annotations

import asyncio
from typing import Optional

import httpx
import typer

from drs.client import DremioClient
from drs.commands.query import run_query
from drs.output import OutputFormat, output, error
from drs.utils import handle_api_error, validate_job_state, validate_job_id

app = typer.Typer(help="List and inspect query jobs.")


async def list_jobs(
    client: DremioClient, status_filter: str | None = None, limit: int = 25
) -> dict:
    """List recent jobs via sys.project.jobs_recent."""
    sql = "SELECT job_id, user_name, query_type, job_state, start_time, finish_time FROM sys.project.jobs_recent"
    if status_filter:
        validated = validate_job_state(status_filter)
        sql += f" WHERE job_state = '{validated}'"
    sql += f" ORDER BY start_time DESC LIMIT {limit}"
    return await run_query(client, sql)


async def get_job(client: DremioClient, job_id: str) -> dict:
    try:
        return await client.get_job_status(job_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def profile(client: DremioClient, job_id: str) -> dict:
    """Get execution profile for a job via system table."""
    validated = validate_job_id(job_id)
    sql = f"SELECT * FROM sys.project.job_profiles WHERE job_id = '{validated}'"
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
        if isinstance(exc, ValueError):
            error(str(exc))
            raise typer.Exit(1)
        raise
    asyncio.run(client.close())
    output(result, fmt, fields=fields)


@app.command("list")
def cli_list(
    status_filter: Optional[str] = typer.Option(None, "--status", "-s", help="Filter by job state: COMPLETED, FAILED, RUNNING, CANCELED, PLANNING, ENQUEUED"),
    limit: int = typer.Option(25, "--limit", "-n", help="Max jobs to return (default 25)"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include in output"),
) -> None:
    """List recent query jobs from sys.project.jobs_recent.

    Shows job ID, user, query type, state, and timing. Results are ordered
    by start time (most recent first).
    """
    client = _get_client()
    _run_command(list_jobs(client, status_filter=status_filter, limit=limit), client, fmt, fields=fields)


@app.command("get")
def cli_get(
    job_id: str = typer.Argument(help="Job ID (UUID)"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Get detailed status and metadata for a specific job."""
    client = _get_client()
    _run_command(get_job(client, job_id), client, fmt)


@app.command("profile")
def cli_profile(
    job_id: str = typer.Argument(help="Job ID (UUID) to get execution profile for"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Show operator-level execution profile for a completed job.

    Queries sys.project.job_profiles for plan phases, row counts,
    and timing per operator. Useful for diagnosing slow queries.
    """
    client = _get_client()
    _run_command(profile(client, job_id), client, fmt)
