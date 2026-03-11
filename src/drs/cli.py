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
"""drs — Developer CLI for Dremio Cloud. Entry point and command registration."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import typer

from drs.auth import DrsConfig, load_config
from drs.client import DremioClient
from drs.commands import query, catalog, schema, reflect, jobs, access

app = typer.Typer(
    name="drs",
    help="Developer CLI for Dremio Cloud — query, catalog, schema, reflections, jobs, and access.",
    no_args_is_help=True,
)

# Register command groups
app.add_typer(query.app, name="query")
app.add_typer(catalog.app, name="catalog")
app.add_typer(schema.app, name="schema")
app.add_typer(reflect.app, name="reflect")
app.add_typer(jobs.app, name="jobs")
app.add_typer(access.app, name="access")

# Global state for config
_config: DrsConfig | None = None
_cli_opts: dict = {}


@app.callback()
def main(
    config: Optional[str] = typer.Option(None, "--config", "-c", help="Path to config file"),
    token: Optional[str] = typer.Option(None, "--token", help="Dremio personal access token (PAT)"),
    project_id: Optional[str] = typer.Option(None, "--project-id", help="Dremio Cloud project ID"),
    uri: Optional[str] = typer.Option(None, "--uri", help="Dremio API base URI (e.g., https://api.dremio.cloud, https://api.eu.dremio.cloud)"),
) -> None:
    """Global options for drs CLI."""
    global _cli_opts
    _cli_opts = {
        "config_path": Path(config) if config else None,
        "cli_token": token,
        "cli_project_id": project_id,
        "cli_uri": uri,
    }


def get_config() -> DrsConfig:
    global _config
    if _config is None:
        try:
            _config = load_config(
                _cli_opts.get("config_path"),
                cli_token=_cli_opts.get("cli_token"),
                cli_project_id=_cli_opts.get("cli_project_id"),
                cli_uri=_cli_opts.get("cli_uri"),
            )
        except Exception as e:
            print(f"Error loading config: {e}", file=sys.stderr)
            print(
                "Provide credentials via --token, DREMIO_TOKEN env var, "
                "or config file (~/.config/dremioai/config.yaml)",
                file=sys.stderr,
            )
            raise typer.Exit(1)
    return _config


def get_client() -> DremioClient:
    return DremioClient(get_config())


@app.command("describe")
def describe_command(
    command: str = typer.Argument(help="Command to describe (e.g., 'query.run', 'catalog.get', 'reflect.drop')"),
) -> None:
    """Show machine-readable schema for a command — parameters, types, and descriptions.

    Use this to discover what parameters a command accepts before calling it.
    Outputs JSON with parameter names, types, required/optional, and descriptions.
    """
    from drs.introspect import describe_command as _describe
    result = _describe(command)
    if result is None:
        print(f"Unknown command: {command}", file=sys.stderr)
        print("Available commands: query.run, query.status, query.cancel, catalog.list, catalog.get, "
              "catalog.search, schema.describe, schema.lineage, schema.wiki, schema.sample, "
              "reflect.list, reflect.status, reflect.refresh, reflect.drop, jobs.list, jobs.get, "
              "jobs.profile, access.grants, access.roles, access.whoami, access.audit", file=sys.stderr)
        raise typer.Exit(1)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    app()
