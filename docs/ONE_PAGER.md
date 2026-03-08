# drs — Dremio Developer CLI

**One CLI. Three channels. Zero duplicated logic.**

## The Problem

Dremio's developer tooling is fragmented across repos with no CLI layer:
- `dremio-mcp` — MCP server only, no CLI, can't script or debug without an AI agent
- `claude-plugins` — skills reference raw curl, not actionable commands
- `dremio-agent-skill` — knowledge pack, different owner, multi-tool targeting

## The Solution

`dremio/cli` is a single repo that ships:

| Channel | Users | Example |
|---------|-------|---------|
| **CLI** (`drs`) | Developers, CI scripts | `drs query run "SELECT * FROM orders LIMIT 10"` |
| **MCP Server** | Claude Desktop, AI agents | `drs mcp` → 19 tools auto-available |
| **Plugin** | Claude Code | 8 workflow skills (slow query diagnosis, access audit, etc.) |

All three call the same async command functions → `client.py` (single HTTP layer) → Dremio Cloud API.

## What ships in v1

- **18 CLI commands** in 6 groups: query, catalog, schema, reflect, jobs, access
- **19 MCP tools** with LLM-optimized descriptions
- **8 Claude Code skills** (3 absorbed from claude-plugins + 5 new workflows)
- **3 output formats**: JSON (default), CSV, pretty table
- **Config compatibility** with existing dremio-mcp format
- **43 unit tests**, input validation, structured error handling

## The Discovery Service Path

Today: hand-maintained client methods + spec validator that catches drift against OpenAPI specs.

Next: auto-generate the client layer from OpenAPI specs already in the Dremio monorepo (40+ YAML files). SQL-based commands (jobs, reflections by dataset) coexist alongside auto-generated REST commands.

End state: Dremio Cloud serves `/api/openapi.json`, CLI fetches at startup — new endpoints appear without a release.

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| CLI-first, MCP wraps | Every operation is testable/scriptable without an AI agent |
| Cloud-only for v1 | Software has different auth, URLs, API behavior — separate concern |
| SQL + REST hybrid | Some data only available via system tables (jobs, reflections by dataset) |
| Single `client.py` | One file makes all HTTP calls — easy to audit, replace, or auto-generate |
| Google CLI naming | Repo: `dremio/cli`, binary: `drs` (matches `googleworkspace/cli` → `gws`) |

## Repo: `github.com/dremio/cli`
