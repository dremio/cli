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
"""dremio context — switch between named profiles."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from drs.auth import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_URI,
    get_default_profile_name,
    list_profiles,
    set_default_profile,
)

app = typer.Typer(
    help="Manage named configuration profiles.",
    context_settings={"help_option_names": ["-h", "--help"]},
)

console = Console()
err_console = Console(stderr=True)


def _get_config_path(ctx: typer.Context) -> Path:
    """Resolve config path from the global --config flag."""
    if ctx.obj and ctx.obj.get("config_path"):
        return ctx.obj["config_path"]
    return DEFAULT_CONFIG_PATH


@app.command("list")
def cli_list(ctx: typer.Context) -> None:
    """List all configured profiles."""
    config_path = _get_config_path(ctx)
    profiles = list_profiles(config_path)

    if not profiles:
        err_console.print(
            "[yellow]No profiles configured.[/yellow]\nRun [bold cyan]dremio setup[/bold cyan] to create one."
        )
        raise typer.Exit(1)

    default_name = get_default_profile_name(config_path)

    table = Table(show_header=True, header_style="bold")
    table.add_column("")
    table.add_column("Profile")
    table.add_column("Region")
    table.add_column("Project ID")

    for name, values in profiles.items():
        is_active = name == default_name
        marker = "*" if is_active else " "
        uri = values.get("uri", DEFAULT_URI)
        region = "EU" if "eu.dremio" in uri else "US"
        project_id = values.get("project_id", "—")
        style = "bold" if is_active else ""
        table.add_row(marker, name, region, project_id, style=style)

    console.print(table)


@app.command("use")
def cli_use(
    ctx: typer.Context,
    name: str = typer.Argument(help="Profile name to set as default"),
) -> None:
    """Switch the default profile."""
    config_path = _get_config_path(ctx)
    try:
        set_default_profile(name, config_path)
    except ValueError as exc:
        err_console.print(f"[red]{exc}[/red]")
        profiles = list_profiles(config_path)
        if profiles:
            err_console.print(f"Available profiles: {', '.join(profiles)}")
        raise typer.Exit(1)

    console.print(f"Switched to profile [bold]{name}[/bold].")


@app.command("current")
def cli_current(ctx: typer.Context) -> None:
    """Show the active (default) profile name."""
    config_path = _get_config_path(ctx)
    name = get_default_profile_name(config_path)

    if not name:
        err_console.print(
            "[yellow]No profiles configured.[/yellow]\nRun [bold cyan]dremio setup[/bold cyan] to create one."
        )
        raise typer.Exit(1)

    console.print(name)
