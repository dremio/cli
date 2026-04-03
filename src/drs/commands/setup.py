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
import sys
from pathlib import Path
from typing import Any

import httpx
import typer
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from drs.auth import DEFAULT_CONFIG_PATH, DEFAULT_URI, DrsConfig
from drs.client import DremioClient

REGIONS = {
    "1": ("US", "https://api.dremio.cloud", "https://app.dremio.cloud"),
    "2": ("EU", "https://api.eu.dremio.cloud", "https://app.eu.dremio.cloud"),
}

console = Console()
err_console = Console(stderr=True)


async def validate_credentials(uri: str, pat: str, project_id: str) -> tuple[bool, str, dict[str, Any] | None]:
    """Test credentials by calling get_project(). Returns (success, message, project_data)."""
    config = DrsConfig(uri=uri, pat=pat, project_id=project_id)
    client = DremioClient(config)
    try:
        project = await client.get_project(project_id)
        return True, f"Connected to project: {project.get('name', project_id)}", project
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        if code == 401:
            return False, "Authentication failed — your PAT is invalid or expired.", None
        if code in (403, 404):
            return False, "Project not found or you don't have access to it.", None
        return False, f"API error (HTTP {code}): {exc.response.text[:200]}", None
    except httpx.ConnectError:
        return False, f"Cannot reach {uri} — check your region selection and network.", None
    except Exception as exc:
        return False, f"Unexpected error: {exc}", None
    finally:
        await client.close()


def write_config(uri: str, pat: str, project_id: str, config_path: Path) -> None:
    """Write YAML config file, creating parent directories as needed."""
    data: dict[str, str] = {}
    if uri != DEFAULT_URI:
        data["uri"] = uri
    data["pat"] = pat
    data["project_id"] = project_id

    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
    config_path.chmod(0o600)


def _prompt_region() -> tuple[str, str]:
    """Prompt for region selection. Returns (api_uri, app_url)."""
    console.print()
    console.print(Panel(
        "[bold]Step 1: Choose your region[/bold]\n\n"
        "  [cyan]1[/cyan]) US  (api.dremio.cloud) — default\n"
        "  [cyan]2[/cyan]) EU  (api.eu.dremio.cloud)",
        title="Region",
        border_style="blue",
    ))
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
    console.print(Panel(
        "[bold]Step 2: Create a Personal Access Token (PAT)[/bold]\n\n"
        f"  1. Open [link={app_url}]{app_url}[/link] and sign in\n"
        "  2. Click your profile icon (bottom-left) → [bold]Account Settings[/bold]\n"
        "  3. Go to [bold]Personal Access Tokens[/bold]\n"
        "  4. Click [bold]New Token[/bold], give it a name, and copy the token\n\n"
        "[dim]The token starts with [bold]dremio_pat_[/bold] and will only be shown once.[/dim]",
        title="Personal Access Token",
        border_style="blue",
    ))
    while True:
        pat = typer.prompt("Paste your PAT", hide_input=True).strip()
        if pat:
            return pat
        console.print("[red]PAT cannot be empty.[/red]")


def _prompt_project_id(app_url: str) -> str:
    """Prompt for Project ID with step-by-step instructions."""
    console.print()
    console.print(Panel(
        "[bold]Step 3: Find your Project ID[/bold]\n\n"
        f"  1. Open [link={app_url}]{app_url}[/link]\n"
        "  2. Select your project from the top-left dropdown\n"
        "  3. Go to [bold]Project Settings[/bold] → [bold]General[/bold]\n"
        "  4. Copy the [bold]Project ID[/bold] (a UUID like [dim]a1b2c3d4-...[/dim])\n\n"
        "[dim]Tip: The project ID is also visible in the URL bar.[/dim]",
        title="Project ID",
        border_style="blue",
    ))
    while True:
        project_id = typer.prompt("Paste your Project ID").strip()
        if project_id:
            return project_id
        console.print("[red]Project ID cannot be empty.[/red]")


def setup_command(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to write config file"),
) -> None:
    """Interactive setup wizard — configure credentials for Dremio Cloud."""
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

    config_path = Path(config) if config else DEFAULT_CONFIG_PATH

    # Welcome
    console.print()
    console.print(Panel(
        "This wizard will help you connect the Dremio CLI to your Dremio Cloud account.\n\n"
        "You'll need:\n"
        "  • A [bold]Dremio Cloud account[/bold] (sign up at [link=https://app.dremio.cloud]app.dremio.cloud[/link])\n"
        "  • A [bold]Personal Access Token[/bold] (we'll walk you through creating one)\n"
        "  • A [bold]Project ID[/bold] (we'll show you where to find it)",
        title="[bold]Dremio CLI Setup[/bold]",
        border_style="cyan",
    ))

    # Check existing config
    if config_path.exists():
        console.print(f"\n[yellow]A config file already exists at {config_path}[/yellow]")
        if not typer.confirm("Overwrite it?", default=False):
            console.print("Setup cancelled.")
            raise typer.Exit(0)

    # Step 1: Region
    api_uri, app_url = _prompt_region()

    # Step 2: PAT (with retry loop)
    pat = _prompt_pat(app_url)

    # Step 3: Project ID (with retry loop)
    project_id = _prompt_project_id(app_url)

    # Validate
    console.print()
    with console.status("[bold]Validating credentials...[/bold]", spinner="dots"):
        ok, message, project_data = asyncio.run(validate_credentials(api_uri, pat, project_id))

    if not ok:
        # Retry loop — let user fix the failing step
        while not ok:
            console.print(f"\n[red]✗ {message}[/red]")
            if "PAT" in message or "Authentication" in message:
                console.print("[dim]Let's try the PAT again.[/dim]")
                pat = _prompt_pat(app_url)
            elif "Project" in message:
                console.print("[dim]Let's try the Project ID again.[/dim]")
                project_id = _prompt_project_id(app_url)
            else:
                console.print("[dim]Let's try the region again.[/dim]")
                api_uri, app_url = _prompt_region()
                pat = _prompt_pat(app_url)
                project_id = _prompt_project_id(app_url)

            console.print()
            with console.status("[bold]Validating credentials...[/bold]", spinner="dots"):
                ok, message, project_data = asyncio.run(validate_credentials(api_uri, pat, project_id))

    # Success — write config
    project_name = project_data.get("name", project_id) if project_data else project_id
    console.print(f"\n[green]✓ {message}[/green]")

    write_config(api_uri, pat, project_id, config_path)

    console.print()
    success = Text()
    success.append("Config saved to ", style="bold")
    success.append(str(config_path), style="cyan")
    success.append(f"\nProject: {project_name}")
    success.append("\n\nTry it out:\n  ")
    success.append('dremio query run "SELECT 1 AS hello"', style="bold cyan")
    console.print(Panel(success, title="[bold green]Setup complete[/bold green]", border_style="green"))
