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
_config_path: Path | None = None


@app.callback()
def main(
    config: Optional[str] = typer.Option(None, "--config", "-c", help="Path to config file"),
) -> None:
    """Global options for drs CLI."""
    global _config_path
    if config:
        _config_path = Path(config)


def get_config() -> DrsConfig:
    global _config
    if _config is None:
        try:
            _config = load_config(_config_path)
        except Exception as e:
            print(f"Error loading config: {e}", file=sys.stderr)
            print(
                "Set DREMIO_PAT and DREMIO_PROJECT_ID env vars, or create ~/.config/dremioai/config.yaml",
                file=sys.stderr,
            )
            raise typer.Exit(1)
    return _config


def get_client() -> DremioClient:
    return DremioClient(get_config())


@app.command("mcp")
def mcp_command(
    services: Optional[str] = typer.Option(None, "--services", help="Comma-separated tool groups to expose"),
) -> None:
    """Start MCP stdio server for Claude Desktop / AI agent integration."""
    from drs.mcp_server import create_server
    server = create_server(services=services.split(",") if services else None)
    server.run(transport="stdio")


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
