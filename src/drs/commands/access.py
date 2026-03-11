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
"""drs access — inspect grants, roles, and user permissions."""

from __future__ import annotations

import asyncio

import httpx
import typer

from drs.client import DremioClient
from drs.output import OutputFormat, output, error
from drs.utils import handle_api_error, parse_path

app = typer.Typer(help="Inspect grants, roles, and user permissions.")


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


async def roles(client: DremioClient) -> dict:
    """List all roles."""
    try:
        return await client.list_roles()
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def whoami(client: DremioClient) -> dict:
    """Get info about the current user.

    Note: Dremio Cloud lacks a dedicated 'whoami' endpoint. This returns
    the first user from the user list, which may not be the PAT owner
    in all configurations. Use 'drs access audit <username>' for reliable
    user lookups.
    """
    try:
        return await client.list_users(max_results=1)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def audit(client: DremioClient, username: str) -> dict:
    """Audit a user's effective permissions: user -> roles."""
    try:
        user = await client.get_user_by_name(username)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    user_roles = user.get("roles", [])

    role_grants: list[dict] = []
    for role in user_roles:
        role_id = role.get("id", role) if isinstance(role, dict) else role
        role_name = role.get("name", role_id) if isinstance(role, dict) else role_id
        role_grants.append({"role_id": role_id, "role_name": role_name})

    return {
        "username": username,
        "user_id": user.get("id"),
        "roles": role_grants,
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
        from drs.utils import DremioAPIError
        if isinstance(exc, DremioAPIError):
            error(str(exc))
            raise typer.Exit(1)
        if isinstance(exc, ValueError):
            error(str(exc))
            raise typer.Exit(1)
        raise
    output(result, fmt, fields=fields)


@app.command("grants")
def cli_grants(
    path: str = typer.Argument(help="Dot-separated entity path (e.g., myspace.mytable)"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Show ACL grants on a catalog entity.

    Displays which users and roles have permissions on the specified
    table, view, source, or space.
    """
    client = _get_client()
    _run_command(grants(client, path), client, fmt)


@app.command("roles")
def cli_roles(
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """List all roles in the organization."""
    client = _get_client()
    _run_command(roles(client), client, fmt)


@app.command("whoami")
def cli_whoami(
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Show current authenticated user info (best-effort, see 'audit' for reliable lookups)."""
    client = _get_client()
    _run_command(whoami(client), client, fmt)


@app.command("audit")
def cli_audit(
    username: str = typer.Argument(help="Username to look up and audit"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Audit a user's roles and effective permissions.

    Looks up the user by name, then lists all roles they belong to.
    """
    client = _get_client()
    _run_command(audit(client, username), client, fmt)
