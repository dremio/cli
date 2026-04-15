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
"""Interactive setup wizard for Dremio Cloud CLI."""

from __future__ import annotations

import asyncio
import re
import sys
from pathlib import Path
from typing import Any

import httpx
import typer
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from drs.auth import DEFAULT_CONFIG_PATH, DEFAULT_URI, DrsConfig, _write_raw_config, read_config_file
from drs.client import DremioClient

REGIONS = {
    "1": ("US", "https://api.dremio.cloud", "https://app.dremio.cloud"),
    "2": ("EU", "https://api.eu.dremio.cloud", "https://app.eu.dremio.cloud"),
}

console = Console()
err_console = Console(stderr=True)


async def validate_credentials(uri: str, pat: str) -> tuple[bool, str, list[dict[str, Any]] | None]:
    """Test credentials by calling list_projects(). Returns (success, message, projects_list)."""
    # We need a minimal config to create a client — project_id is not needed for listing projects.
    config = DrsConfig(uri=uri, pat=pat, project_id="__discovery__")
    client = DremioClient(config)
    try:
        result = await client.list_projects()
        projects = result.get("data", []) if isinstance(result, dict) else result
        if not projects:
            return False, "No projects found — your account may not have any projects in this region.", None
        return True, f"Authenticated — found {len(projects)} project(s).", projects
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        if code == 401:
            return False, "Authentication failed — your PAT is invalid or expired.", None
        if code == 403:
            return False, "Access denied — your PAT may lack permissions.", None
        return False, f"API error (HTTP {code}): {exc.response.text[:200]}", None
    except httpx.ConnectError:
        return False, f"Cannot reach {uri} — check your region selection and network.", None
    except Exception as exc:
        return False, f"Unexpected error: {exc}", None
    finally:
        await client.close()


def _slugify(name: str) -> str:
    """Convert a project/org name to a URL-friendly profile name."""
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug or "default"


def write_profile(
    uri: str,
    pat: str,
    project_id: str,
    profile_name: str,
    set_default: bool,
    config_path: Path,
) -> None:
    """Write a profile to the config file, preserving existing profiles."""
    raw = read_config_file(config_path)

    # Migrate legacy flat config to profiles format if needed
    if "profiles" not in raw:
        old_values: dict[str, Any] = {}
        for key in ("uri", "endpoint", "pat", "token", "project_id", "projectId"):
            if key in raw:
                old_values[key] = raw.pop(key)
        if old_values:
            migrated = {}
            if v := old_values.get("uri", old_values.get("endpoint")):
                migrated["uri"] = v
            if v := old_values.get("pat", old_values.get("token")):
                migrated["pat"] = v
            if v := old_values.get("project_id", old_values.get("projectId")):
                migrated["project_id"] = v
            if migrated:
                raw.setdefault("profiles", {})["default"] = migrated
                if "default_profile" not in raw:
                    raw["default_profile"] = "default"

    # Add/update the profile
    raw.setdefault("profiles", {})[profile_name] = _build_profile_dict(uri, pat, project_id)

    if set_default or "default_profile" not in raw:
        raw["default_profile"] = profile_name

    _write_raw_config(raw, config_path)


def _build_profile_dict(uri: str, pat: str, project_id: str) -> dict[str, str]:
    """Build a profile dict, omitting uri if it's the default."""
    data: dict[str, str] = {}
    if uri != DEFAULT_URI:
        data["uri"] = uri
    data["pat"] = pat
    data["project_id"] = project_id
    return data


def _prompt_region() -> tuple[str, str]:
    """Prompt for region selection. Returns (api_uri, app_url)."""
    console.print()
    console.print(
        Panel(
            "[bold]Step 1: Choose your region[/bold]\n\n"
            "  [cyan]1[/cyan]) US  (api.dremio.cloud) — default\n"
            "  [cyan]2[/cyan]) EU  (api.eu.dremio.cloud)",
            title="Region",
            border_style="blue",
        )
    )
    choice = typer.prompt("Enter 1 or 2", default="1").strip()
    if choice not in REGIONS:
        console.print("[yellow]Invalid choice, defaulting to US.[/yellow]")
        choice = "1"
    region_name, api_uri, app_url = REGIONS[choice]
    console.print(f"  → Region: [bold]{region_name}[/bold]")
    return api_uri, app_url


def _prompt_pat(app_url: str) -> str:
    """Prompt for Personal Access Token with step-by-step instructions."""
    console.print()
    console.print(
        Panel(
            "[bold]Step 2: Create a Personal Access Token (PAT)[/bold]\n\n"
            f"  1. Open [link={app_url}]{app_url}[/link] and sign in\n"
            "  2. Click your profile icon (bottom-left) → [bold]Account Settings[/bold]\n"
            "  3. Go to [bold]Personal Access Tokens[/bold]\n"
            "  4. Click [bold]New Token[/bold], give it a name, and copy the token\n\n"
            "[dim]The token starts with [bold]dremio_pat_[/bold] and will only be shown once.[/dim]",
            title="Personal Access Token",
            border_style="blue",
        )
    )
    while True:
        pat = typer.prompt("Paste your PAT", hide_input=True).strip()
        if pat:
            return pat
        console.print("[red]PAT cannot be empty.[/red]")


def _prompt_project(projects: list[dict[str, Any]]) -> dict[str, Any]:
    """Show discovered projects and let the user pick one."""
    console.print()
    lines = ["[bold]Step 3: Choose a project[/bold]\n"]
    for i, proj in enumerate(projects, 1):
        name = proj.get("name", "Unnamed")
        pid = proj.get("id", "???")
        lines.append(f"  [cyan]{i}[/cyan]) {name}  [dim]({pid})[/dim]")

    console.print(Panel("\n".join(lines), title="Projects", border_style="blue"))

    if len(projects) == 1:
        console.print(f"  → Auto-selected: [bold]{projects[0].get('name', 'Unnamed')}[/bold]")
        return projects[0]

    while True:
        choice = typer.prompt(f"Enter 1-{len(projects)}", default="1").strip()
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(projects):
                selected = projects[idx]
                console.print(f"  → Project: [bold]{selected.get('name', 'Unnamed')}[/bold]")
                return selected
        except ValueError:
            pass
        console.print(f"[red]Please enter a number between 1 and {len(projects)}.[/red]")


def _prompt_profile_name(suggested: str, existing_profiles: dict[str, Any]) -> str:
    """Prompt for a profile name with a suggested default."""
    console.print()
    console.print(
        Panel(
            "[bold]Step 4: Name this profile[/bold]\n\n"
            "  A short name to identify this configuration.\n"
            f"  [dim]Existing profiles: {', '.join(existing_profiles) if existing_profiles else '(none)'}[/dim]",
            title="Profile Name",
            border_style="blue",
        )
    )
    while True:
        name = typer.prompt("Profile name", default=suggested).strip()
        if not name:
            console.print("[red]Profile name cannot be empty.[/red]")
            continue
        if name in existing_profiles:
            if typer.confirm(f"Profile '{name}' already exists. Overwrite it?", default=False):
                return name
            continue
        return name


def setup_command(
    ctx: typer.Context,
) -> None:
    """Interactive setup wizard — configure credentials for Dremio Cloud.

    Walks you through connecting to your Dremio Cloud account. Discovers
    your projects automatically and saves the configuration as a named
    profile in ~/.config/dremioai/config.yaml (or the path specified
    with --config).
    """
    if not sys.stdin.isatty():
        err_console.print(
            "[bold]dremio setup[/bold] requires an interactive terminal.\n\n"
            "To configure manually, set these environment variables:\n"
            "  export DREMIO_TOKEN=your_pat\n"
            "  export DREMIO_PROJECT_ID=your_project_id\n\n"
            f"Or create a config file at {DEFAULT_CONFIG_PATH}\n"
            "See: dremio --help",
        )
        raise typer.Exit(1)

    # Honor the global --config flag (e.g. dremio --config /tmp/my.yaml setup)
    global_config = ctx.obj.get("config_path") if ctx.obj else None
    config_path = global_config if global_config else DEFAULT_CONFIG_PATH

    # Read existing config for profile awareness
    existing_raw = read_config_file(config_path)
    existing_profiles = existing_raw.get("profiles", {})

    # Welcome
    console.print()
    console.print(
        Panel(
            "This wizard will help you connect the Dremio CLI to your Dremio Cloud account.\n\n"
            "You'll need:\n"
            "  • A [bold]Dremio Cloud account[/bold] (sign up at [link=https://app.dremio.cloud]app.dremio.cloud[/link])\n"
            "  • A [bold]Personal Access Token[/bold] (we'll walk you through creating one)\n\n"
            "We'll discover your projects automatically after you authenticate.",
            title="[bold]Dremio CLI Setup[/bold]",
            border_style="cyan",
        )
    )

    # Step 1: Region
    api_uri, app_url = _prompt_region()

    # Step 2: PAT (with retry loop)
    pat = _prompt_pat(app_url)

    # Validate and discover projects
    console.print()
    with console.status("[bold]Authenticating and discovering projects...[/bold]", spinner="dots"):
        ok, message, projects = asyncio.run(validate_credentials(api_uri, pat))

    while not ok:
        console.print(f"\n[red]✗ {message}[/red]")
        if not typer.confirm("Would you like to try again?", default=True):
            console.print("Setup cancelled.")
            raise typer.Exit(1)

        if "Authentication" in message:
            console.print("[dim]Let's try the PAT again.[/dim]")
            pat = _prompt_pat(app_url)
        elif "Cannot reach" in message:
            console.print("[dim]Let's try the region again.[/dim]")
            api_uri, app_url = _prompt_region()
            pat = _prompt_pat(app_url)
        else:
            pat = _prompt_pat(app_url)

        console.print()
        with console.status("[bold]Authenticating and discovering projects...[/bold]", spinner="dots"):
            ok, message, projects = asyncio.run(validate_credentials(api_uri, pat))

    assert projects is not None
    console.print(f"\n[green]✓ {message}[/green]")

    # Step 3: Choose a project
    selected_project = _prompt_project(projects)
    project_id = selected_project["id"]
    project_name = selected_project.get("name", project_id)

    # Step 4: Name this profile
    suggested_name = _slugify(project_name)
    profile_name = _prompt_profile_name(suggested_name, existing_profiles)

    # Step 5: Set as default?
    set_default = True
    if existing_profiles and existing_raw.get("default_profile"):
        set_default = typer.confirm("Set as default profile?", default=True)

    # Write config
    write_profile(api_uri, pat, project_id, profile_name, set_default, config_path)

    console.print()
    success = Text()
    success.append("Config saved to ", style="bold")
    success.append(str(config_path), style="cyan")
    success.append(f"\nProfile: {profile_name}")
    success.append(f"\nProject: {project_name}")
    if set_default:
        success.append(" (default)", style="dim")
    success.append("\n\nTry it out:\n  ")
    success.append('dremio query run "SELECT 1 AS hello"', style="bold cyan")
    if not set_default:
        success.append(f'\n  dremio --profile {profile_name} query run "SELECT 1"', style="bold cyan")
    console.print(Panel(success, title="[bold green]Setup complete[/bold green]", border_style="green"))
