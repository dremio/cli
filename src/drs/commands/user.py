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
"""dremio user — manage Dremio Cloud users."""

from __future__ import annotations

import asyncio

import httpx
import typer

from drs.client import DremioClient
from drs.output import OutputFormat, error, output
from drs.utils import handle_api_error

app = typer.Typer(help="Manage Dremio Cloud users.", context_settings={"help_option_names": ["-h", "--help"]})


async def list_users(client: DremioClient, max_results: int = 100) -> dict:
    """List all users."""
    try:
        return await client.list_users(max_results=max_results)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def get_user(client: DremioClient, identifier: str) -> dict:
    """Get user by name or ID. Tries name first, falls back to ID."""
    try:
        return await client.get_user_by_name(identifier)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            try:
                return await client.get_user(identifier)
            except httpx.HTTPStatusError as exc2:
                raise handle_api_error(exc2) from exc2
        raise handle_api_error(exc) from exc


async def create_user(client: DremioClient, email: str, role_id: str | None = None) -> dict:
    """Create (invite) a user by email."""
    body: dict = {"email": email}
    if role_id:
        body["roleId"] = role_id
    try:
        return await client.invite_user(body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def whoami(client: DremioClient) -> dict:
    """Get info about the current user."""
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


async def update_user(client: DremioClient, user_id: str, name: str | None = None) -> dict:
    """Update a user."""
    try:
        existing = await client.get_user(user_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc
    body = dict(existing)
    if name:
        body["name"] = name
    try:
        return await client.update_user(user_id, body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def delete_user(client: DremioClient, user_id: str) -> dict:
    """Delete a user."""
    try:
        return await client.delete_user(user_id)
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


@app.command("list")
def cli_list(
    limit: int = typer.Option(100, "--limit", "-n", help="Max users to return"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """List all users in the organization."""
    client = _get_client()
    _run_command(list_users(client, max_results=limit), client, fmt, fields=fields)


@app.command("get")
def cli_get(
    identifier: str = typer.Argument(help="Username or user ID to look up"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """Get user details by name or ID."""
    client = _get_client()
    _run_command(get_user(client, identifier), client, fmt, fields=fields)


@app.command("create")
def cli_create(
    email: str = typer.Argument(help="Email address to invite"),
    role_id: str = typer.Option(None, "--role-id", help="Role ID to assign to the new user"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Create (invite) a new user by email address."""
    client = _get_client()
    _run_command(create_user(client, email, role_id=role_id), client, fmt)


@app.command("update")
def cli_update(
    user_id: str = typer.Argument(help="User ID to update"),
    name: str = typer.Option(None, "--name", help="New display name"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Update a user's properties."""
    client = _get_client()
    _run_command(update_user(client, user_id, name=name), client, fmt)


@app.command("delete")
def cli_delete(
    user_id: str = typer.Argument(help="User ID to delete"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show user details without deleting"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Delete a user from the organization. Cannot be undone."""
    client = _get_client()
    if dry_run:
        _run_command(get_user(client, user_id), client, fmt)
        return
    _run_command(delete_user(client, user_id), client, fmt)


@app.command("whoami")
def cli_whoami(
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Show current authenticated user info (best-effort)."""
    client = _get_client()
    _run_command(whoami(client), client, fmt)


@app.command("audit")
def cli_audit(
    username: str = typer.Argument(help="Username to look up and audit"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Audit a user's roles and effective permissions."""
    client = _get_client()
    _run_command(audit(client, username), client, fmt)
