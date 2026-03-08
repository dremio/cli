# drs — Developer CLI for Dremio Cloud

A unified CLI + MCP server + Claude Code plugin for Dremio Cloud. Query data, browse catalogs, inspect schemas, manage reflections, monitor jobs, and audit access — all from the terminal or your AI agent.

> **Scope:** Dremio Cloud only. Dremio Software has different auth and API behavior — not supported in this version.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│  drs CLI     │────▶│              │────▶│ Dremio Cloud API │
│  (typer)     │     │  client.py   │     │ (REST + SQL)     │
├─────────────┤     │  (httpx)     │     └─────────────────┘
│  drs mcp     │────▶│              │
│  (FastMCP)   │     └──────────────┘
└─────────────┘
```

Commands talk to `client.py` (the single HTTP layer). Some operations use the REST API directly (catalog, reflections, access), others use SQL via system tables (jobs, reflection listing by dataset). The MCP server is a thin transport wrapper over the same command functions — zero duplicated logic.

## Quickstart

### Install

```bash
# From source
uv tool install .

# Or with pip
pip install .
```

### Configure

Create `~/.config/dremioai/config.yaml`:

```yaml
pat: dremio_pat_xxxxxxxxxxxxx
project_id: your-project-id
# uri: https://api.dremio.cloud  # default, change for EU region
```

Or use environment variables:

```bash
export DREMIO_PAT=dremio_pat_xxxxxxxxxxxxx
export DREMIO_PROJECT_ID=your-project-id
```

### First command

```bash
drs query run "SELECT 1 AS hello"
```

## Commands

| Group | Commands | Description |
|-------|----------|-------------|
| `drs query` | `run`, `status`, `cancel` | Execute SQL queries |
| `drs catalog` | `list`, `get`, `search` | Browse and search the catalog |
| `drs schema` | `describe`, `lineage`, `wiki`, `sample` | Inspect table schemas and data |
| `drs reflect` | `list`, `status`, `refresh`, `drop` | Manage reflections |
| `drs jobs` | `list`, `get`, `profile` | Monitor query jobs |
| `drs access` | `grants`, `roles`, `whoami`, `audit` | Audit permissions |

### Output formats

All commands support `--output json` (default), `--output csv`, or `--output pretty`:

```bash
drs jobs list --status FAILED --output pretty
drs schema describe myspace.mytable --output csv
```

## MCP Server (AI Agent Integration)

Start the MCP stdio server:

```bash
drs mcp
```

Optionally filter which tool groups are exposed:

```bash
drs mcp --services query,schema
```

### Claude Desktop configuration

Add to your Claude Desktop config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "Dremio": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/drs", "drs", "mcp"]
    }
  }
}
```

### Available MCP tools (19)

| Tool | Description |
|------|-------------|
| `dremio_query_run` | Execute SQL query, return rows as JSON |
| `dremio_query_status` | Check job status by ID |
| `dremio_query_cancel` | Cancel a running job |
| `dremio_catalog_list` | List top-level sources, spaces, home |
| `dremio_catalog_search` | Full-text search for tables, views, sources |
| `dremio_catalog_get` | Get metadata by dot-separated path |
| `dremio_schema_describe` | Column names, types, nullability |
| `dremio_schema_lineage` | Upstream/downstream dependency graph |
| `dremio_schema_wiki` | Wiki descriptions and tags |
| `dremio_schema_sample` | Preview sample rows (default 10) |
| `dremio_reflect_list` | List reflections on a dataset |
| `dremio_reflect_status` | Reflection freshness and refresh timing |
| `dremio_reflect_refresh` | Trigger reflection refresh |
| `dremio_reflect_drop` | Delete a reflection |
| `dremio_jobs_list` | Recent jobs, filterable by status |
| `dremio_jobs_get` | Detailed job metadata |
| `dremio_jobs_profile` | Operator-level execution profile |
| `dremio_access_grants` | ACL grants on a catalog entity |
| `dremio_access_roles` | List all org roles |
| `dremio_access_whoami` | Current authenticated user info |
| `dremio_access_audit` | Audit a user's roles and permissions |

## Claude Code Plugin

Install as a Claude Code plugin for Dremio-aware skills:

```
/plugin marketplace add dremio/cli
/plugin install dremio@dremio-cli
```

### Skills (8)

| Skill | Description |
|-------|-------------|
| `dremio` | Core Dremio Cloud SQL reference, system tables, REST patterns |
| `dremio-setup` | Setup wizard for drs CLI + MCP server |
| `dremio-dbt` | dbt-dremio integration guide (Cloud) |
| `investigate-slow-query` | Diagnose slow queries via job profiles and reflections |
| `audit-dataset-access` | Trace grants and role inheritance |
| `document-dataset` | Generate dataset documentation cards |
| `investigate-data-quality` | Data quality checks: nulls, duplicates, anomalies |
| `onboard-new-source` | Catalog, describe, reflect, verify access |

## Relationship to existing repos

| Repo | Relationship |
|------|-------------|
| `dremio/dremio-mcp` | **Predecessor.** `drs mcp` supersedes it with a CLI layer underneath. Config format preserved. |
| `dremio/claude-plugins` | **Absorbed.** Skills rewritten to use `drs` commands instead of raw curl. |
| `developer-advocacy-dremio/dremio-agent-skill` | **Referenced.** Wizard patterns informed skill design. |

## License

Apache 2.0
