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
"""``dremio login`` and ``dremio logout`` commands — OAuth browser flow."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import httpx
import typer
import yaml
from rich.console import Console
from rich.panel import Panel

from drs import oauth, token_store
from drs.auth import DEFAULT_CONFIG_PATH, DEFAULT_URI

console = Console()
err_console = Console(stderr=True)


def _resolve_uri(ctx: typer.Context, explicit_uri: str | None = None) -> str:
    """Resolve the Dremio API URI from explicit arg, CLI flags, config file, or default."""
    if explicit_uri:
        return explicit_uri

    # Check if parent set cli_uri (global --uri flag)
    from drs.cli import _cli_opts

    cli_uri = _cli_opts.get("cli_uri")
    if cli_uri:
        return cli_uri

    # Try config file
    config_path_obj = ctx.obj.get("config_path") if ctx.obj else None
    path: Path = config_path_obj if config_path_obj else DEFAULT_CONFIG_PATH
    if path.exists():
        with path.open() as f:
            raw = yaml.safe_load(f) or {}
        uri = raw.get("uri", raw.get("endpoint"))
        if uri:
            return uri

    return DEFAULT_URI


def _api_url(uri: str) -> str:
    """Derive the API URL from a Dremio URL (app.X -> api.X)."""
    parsed = urlparse(uri)
    host = parsed.hostname or ""
    if host.startswith("app."):
        host = "api." + host[4:]
    return f"{parsed.scheme}://{host}"


_ACTIVE_STATES = {"ACTIVE", "HIBERNATED"}


def _fetch_projects(api_base: str, access_token: str) -> list[dict]:
    """Fetch active/hibernated projects using the OAuth access token."""
    resp = httpx.get(
        f"{api_base}/v0/projects",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30.0,
    )
    resp.raise_for_status()
    data = resp.json()
    projects = data.get("data", data) if isinstance(data, dict) else data
    return [p for p in projects if p.get("state", "").upper() in _ACTIVE_STATES]


def _format_date(raw: str | None) -> str:
    """Format an ISO timestamp to a short date string."""
    if not raw:
        return ""
    return raw[:10]  # YYYY-MM-DD


def _prompt_project_selection(projects: list[dict]) -> str:
    """Display a numbered list of projects and let the user choose."""
    console.print()
    lines = "[bold]Select a project:[/bold]\n"
    for i, proj in enumerate(projects, 1):
        name = proj.get("name", "unnamed")
        pid = proj.get("id", "")
        desc = proj.get("description", "")
        state = proj.get("state", "")
        created = _format_date(proj.get("createdAt"))
        lines += f"\n  [cyan]{i}[/cyan]) [bold]{name}[/bold]  [dim]({pid})[/dim]"
        if desc:
            lines += f"\n     {desc}"
        details = [s for s in [state, f"created {created}" if created else ""] if s]
        if details:
            lines += f"\n     [dim]{' · '.join(details)}[/dim]"
    console.print(Panel(lines, title="Projects", border_style="blue"))
    choice = typer.prompt(f"Enter 1-{len(projects)}").strip()
    try:
        idx = int(choice) - 1
        if 0 <= idx < len(projects):
            selected = projects[idx]
            console.print(f"  -> [bold]{selected.get('name')}[/bold]")
            return selected["id"]
    except (ValueError, KeyError):
        pass
    err_console.print("[yellow]Invalid choice — please enter a project ID manually.[/yellow]")
    return typer.prompt("Enter your Dremio Cloud Project ID").strip()


def _resolve_project_id(ctx: typer.Context, uri: str, access_token: str) -> str:
    """Resolve project_id from CLI flags, config, or interactive project picker."""
    from drs.cli import _cli_opts

    cli_project_id = _cli_opts.get("cli_project_id")
    if cli_project_id:
        return cli_project_id

    config_path_obj = ctx.obj.get("config_path") if ctx.obj else None
    path: Path = config_path_obj if config_path_obj else DEFAULT_CONFIG_PATH
    if path.exists():
        with path.open() as f:
            raw = yaml.safe_load(f) or {}
        project_id = raw.get("project_id", raw.get("projectId"))
        if project_id:
            return project_id

    # Fetch projects and let the user pick
    api_base = _api_url(uri)
    try:
        projects = _fetch_projects(api_base, access_token)
    except Exception:
        console.print("[yellow]Could not fetch project list.[/yellow]")
        return typer.prompt("Enter your Dremio Cloud Project ID").strip()

    if not projects:
        console.print("[yellow]No projects found in this organization.[/yellow]")
        return typer.prompt("Enter your Dremio Cloud Project ID").strip()

    if len(projects) == 1:
        proj = projects[0]
        console.print(f"  Auto-selected project: [bold]{proj.get('name')}[/bold] ({proj['id']})")
        return proj["id"]

    return _prompt_project_selection(projects)


def login_command(
    ctx: typer.Context,
    uri: str = typer.Option(None, "--uri", "-u", help="Dremio API URL (e.g. https://app.dev.dremio.site)"),
) -> None:
    """Log in to Dremio Cloud via OAuth (opens your browser)."""
    uri = _resolve_uri(ctx, explicit_uri=uri)
    console.print(f"\nLogging in to [bold]{uri}[/bold] ...")

    try:
        tokens = oauth.run_login_flow(uri)
    except Exception as exc:
        err_console.print(f"\n[bold red]Login failed:[/bold red] {exc}")
        raise typer.Exit(1)

    # Ensure we have a project_id to write into the config
    project_id = _resolve_project_id(ctx, uri, tokens.access_token)

    token_store.save(uri, tokens)

    # Also persist auth_method + project_id in config file so subsequent
    # commands pick up OAuth automatically.
    config_path_obj = ctx.obj.get("config_path") if ctx.obj else None
    config_path: Path = config_path_obj if config_path_obj else DEFAULT_CONFIG_PATH
    _update_config_file(config_path, uri, project_id)

    console.print(f"\n[green]Logged in successfully.[/green]  Tokens saved for {uri}")


def _update_config_file(config_path: Path, uri: str, project_id: str) -> None:
    """Ensure the config file records auth_method=oauth and project_id."""
    data: dict = {}
    if config_path.exists():
        with config_path.open() as f:
            data = yaml.safe_load(f) or {}

    if uri != DEFAULT_URI:
        data["uri"] = uri
    data["auth_method"] = "oauth"
    data["project_id"] = project_id
    # Remove PAT if present — OAuth replaces it.
    data.pop("pat", None)
    data.pop("token", None)

    config_path.parent.mkdir(parents=True, exist_ok=True)
    header = "# Dremio CLI config — generated by 'dremio login'\n"
    config_path.write_text(header + yaml.dump(data, default_flow_style=False, sort_keys=False))
    config_path.chmod(0o600)


def logout_command(
    ctx: typer.Context,
    uri: str = typer.Option(None, "--uri", "-u", help="Dremio API URL to log out from"),
) -> None:
    """Log out of Dremio Cloud (removes stored OAuth tokens)."""
    uri = _resolve_uri(ctx, explicit_uri=uri)
    token_store.clear(uri)
    console.print(f"Logged out of [bold]{uri}[/bold]. OAuth tokens removed.")
