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
"""drs grant — manage grants on Dremio Cloud resources."""

from __future__ import annotations

import asyncio

import httpx
import typer

from drs.client import DremioClient
from drs.output import OutputFormat, output, error
from drs.utils import handle_api_error

app = typer.Typer(help="Manage grants on projects, engines, and org resources.")


async def get_grants(
    client: DremioClient, scope: str, scope_id: str, grantee_type: str, grantee_id: str
) -> dict:
    """Get grants for a grantee on a resource."""
    try:
        return await client.get_grants(scope, scope_id, grantee_type, grantee_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def set_grants(
    client: DremioClient, scope: str, scope_id: str, grantee_type: str, grantee_id: str,
    privileges: list[str],
) -> dict:
    """Set grants for a grantee on a resource."""
    body = {"privileges": privileges}
    try:
        return await client.set_grants(scope, scope_id, grantee_type, grantee_id, body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def remove_grants(
    client: DremioClient, scope: str, scope_id: str, grantee_type: str, grantee_id: str
) -> dict:
    """Remove all grants for a grantee on a resource."""
    try:
        return await client.delete_grants(scope, scope_id, grantee_type, grantee_id)
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


@app.command("get")
def cli_get(
    scope: str = typer.Argument(help="Resource scope (projects, orgs, clouds)"),
    scope_id: str = typer.Argument(help="Resource ID (project ID, org ID, etc.)"),
    grantee_type: str = typer.Argument(help="Grantee type (user or role)"),
    grantee_id: str = typer.Argument(help="Grantee ID (user ID or role ID)"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Get grants for a user or role on a resource."""
    client = _get_client()
    _run_command(get_grants(client, scope, scope_id, grantee_type, grantee_id), client, fmt)


@app.command("update")
def cli_update(
    scope: str = typer.Argument(help="Resource scope (projects, orgs, clouds)"),
    scope_id: str = typer.Argument(help="Resource ID"),
    grantee_type: str = typer.Argument(help="Grantee type (user or role)"),
    grantee_id: str = typer.Argument(help="Grantee ID"),
    privileges: str = typer.Argument(help='Comma-separated privileges (e.g., "MANAGE_GRANTS,CREATE_TABLE")'),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Set grants (privileges) for a user or role on a resource.

    Replaces any existing grants for this grantee on the resource.
    """
    client = _get_client()
    priv_list = [p.strip() for p in privileges.split(",") if p.strip()]
    _run_command(set_grants(client, scope, scope_id, grantee_type, grantee_id, priv_list), client, fmt)


@app.command("delete")
def cli_delete(
    scope: str = typer.Argument(help="Resource scope (projects, orgs, clouds)"),
    scope_id: str = typer.Argument(help="Resource ID"),
    grantee_type: str = typer.Argument(help="Grantee type (user or role)"),
    grantee_id: str = typer.Argument(help="Grantee ID"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show current grants without removing"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Remove all grants for a user or role on a resource. Cannot be undone."""
    client = _get_client()
    if dry_run:
        _run_command(get_grants(client, scope, scope_id, grantee_type, grantee_id), client, fmt)
        return
    _run_command(remove_grants(client, scope, scope_id, grantee_type, grantee_id), client, fmt)
