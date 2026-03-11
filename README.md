# drs — Developer CLI for Dremio Cloud

A command-line tool for working with Dremio Cloud. Run SQL queries, browse the catalog, inspect table schemas, manage reflections, monitor jobs, and audit access — from your terminal or any automation pipeline.

Built for developers who want to script against Dremio without clicking through a UI, and for AI agents that need structured access to Dremio metadata and query execution.

> **Dremio Cloud only.** Dremio Software (self-hosted) has different auth and API behavior and is not supported in this version.
>
> API reference: [docs.dremio.com/dremio-cloud/api](https://docs.dremio.com/dremio-cloud/api/)

## Why this exists

Dremio Cloud has a powerful REST API and rich system tables, but no official CLI. That means:

- Debugging a slow query requires navigating the UI to find the job, then manually inspecting the profile
- Scripting catalog operations means hand-rolling `curl` commands with auth headers
- AI agents (Claude, GPT, etc.) need structured tool interfaces, not raw HTTP

`drs` wraps all of this into a single binary with consistent output formats, input validation, and structured error handling.

## Prerequisites

- **Python 3.11+** (check with `python3 --version`)
- **A Dremio Cloud account** with a project
- **A Personal Access Token (PAT)** — generate one from Dremio Cloud under Account Settings > Personal Access Tokens

## Quickstart

### 1. Install

```bash
# Recommended — install as a standalone tool
uv tool install .

# Or with pip
pip install .

# Or for development
git clone https://github.com/dremio/cli.git
cd cli
uv sync
```

### 2. Configure

There are three ways to authenticate, in order of priority:

**Option A: CLI flags** (highest priority — override everything)

```bash
drs --token YOUR_PAT --project-id YOUR_PROJECT_ID query run "SELECT 1"

# EU region
drs --uri https://api.eu.dremio.cloud --token YOUR_PAT --project-id YOUR_PROJECT_ID query run "SELECT 1"
```

**Option B: Environment variables**

```bash
export DREMIO_TOKEN=dremio_pat_xxxxxxxxxxxxx
export DREMIO_PROJECT_ID=your-project-id
# export DREMIO_URI=https://api.eu.dremio.cloud  # optional, for EU region
```

**Option C: Config file** (lowest priority)

```bash
mkdir -p ~/.config/dremioai
cat > ~/.config/dremioai/config.yaml << 'EOF'
pat: dremio_pat_xxxxxxxxxxxxx
project_id: your-project-id
# uri: https://api.dremio.cloud  # default; change for EU region
EOF
chmod 600 ~/.config/dremioai/config.yaml
```

**Where to find these values:**
- **PAT**: Dremio Cloud > Account Settings > Personal Access Tokens > New Token
- **Project ID**: Dremio Cloud > Project Settings (the UUID in the URL works too)

### 3. Verify

```bash
drs query run "SELECT 1 AS hello"
```

If this returns `{"job_id": "...", "state": "COMPLETED", "rowCount": 1, "rows": [{"hello": "1"}]}`, you're set.

## Commands

### Overview

| Group | Commands | What it does |
|-------|----------|--------------|
| `drs query` | `run`, `status`, `cancel` | Execute SQL, check job status, cancel running jobs |
| `drs catalog` | `list`, `get`, `search` | Browse sources/spaces, get entity metadata, full-text search |
| `drs schema` | `describe`, `lineage`, `wiki`, `sample` | Column types, upstream/downstream deps, wiki docs, preview rows |
| `drs reflect` | `list`, `status`, `refresh`, `drop` | List reflections on a dataset, check freshness, trigger refresh |
| `drs jobs` | `list`, `get`, `profile` | Recent jobs with filters, job details, operator-level profiles |
| `drs access` | `grants`, `roles`, `whoami`, `audit` | ACLs on entities, org roles, user permission audit |

### Examples

```bash
# Run a query and get results as a pretty table
drs query run "SELECT * FROM myspace.orders LIMIT 5" --output pretty

# Search the catalog for anything matching "revenue"
drs catalog search "revenue"

# Describe a table's columns
drs schema describe myspace.analytics.monthly_revenue

# Check what reflections exist on a dataset
drs reflect list myspace.orders

# Find failed jobs from recent history
drs jobs list --status FAILED --output pretty

# Audit what roles and permissions a user has
drs access audit rahim.bhojani
```

### Output formats

Every command supports three output formats via `--output` / `-o`:

| Format | Flag | Use case |
|--------|------|----------|
| **JSON** | `--output json` (default) | Piping to `jq`, programmatic consumption, AI agents |
| **CSV** | `--output csv` | Spreadsheets, data pipelines, `awk`/`cut` processing |
| **Pretty** | `--output pretty` | Human reading in the terminal |

### Field filtering

Reduce output to just the fields you need with `--fields` / `-f`. Supports dot notation for nested data:

```bash
# Only show column names and types
drs schema describe myspace.orders --fields columns.name,columns.type

# Only show job ID and state
drs jobs list --fields job_id,job_state
```

This is especially useful for AI agents to keep context windows small.

### Command introspection

Discover parameters for any command programmatically:

```bash
drs describe query.run
drs describe reflect.list
```

Returns a JSON schema with parameter names, types, required/optional, and descriptions. Useful for building automation on top of `drs`.

## How it works

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│  drs CLI     │────▶│  client.py   │────▶│ Dremio Cloud API │
│  (typer)     │     │  (httpx)     │     │ (REST + SQL)     │
└─────────────┘     └──────────────┘     └─────────────────┘
```

- **One HTTP layer** — `client.py` is the only file that makes network calls. Every command goes through it.
- **REST + SQL hybrid** — Some operations use the REST API (catalog, reflections, access), others query system tables via SQL (jobs, reflection listing by dataset). The user doesn't need to know which.
- **Async throughout** — All command logic is `async`. The CLI wraps with `asyncio.run()`.
- **Input validation** — SQL-interpolated values (job IDs, state filters) are validated before use. Catalog paths are checked for traversal attacks. This matters when AI agents are constructing commands.

### API endpoints used

All endpoints target `https://api.dremio.cloud`. See the [Dremio Cloud API reference](https://docs.dremio.com/dremio-cloud/api/) for full details.

| URL pattern | Used by | Docs |
|-------------|---------|------|
| `POST /v0/projects/{pid}/sql` | `query run` | [SQL](https://docs.dremio.com/dremio-cloud/api/sql) |
| `GET /v0/projects/{pid}/job/{id}` | `query status`, `jobs get` | [Job](https://docs.dremio.com/dremio-cloud/api/job/) |
| `GET /v0/projects/{pid}/job/{id}/results` | `query run` (result fetch) | [Job Results](https://docs.dremio.com/dremio-cloud/api/job/job-results/) |
| `POST /v0/projects/{pid}/job/{id}/cancel` | `query cancel` | [Job](https://docs.dremio.com/dremio-cloud/api/job/) |
| `GET /v0/projects/{pid}/catalog` | `catalog list` | [Catalog](https://docs.dremio.com/dremio-cloud/api/catalog/) |
| `GET /v0/projects/{pid}/catalog/by-path/{path}` | `catalog get`, `schema describe`, `schema lineage`, `schema wiki`, `access grants` | [Catalog](https://docs.dremio.com/dremio-cloud/api/catalog/) |
| `GET /v0/projects/{pid}/catalog/{id}/graph` | `schema lineage` | [Lineage](https://docs.dremio.com/dremio-cloud/api/catalog/lineage) |
| `GET /v0/projects/{pid}/catalog/{id}/collaboration/wiki` | `schema wiki` | [Wiki](https://docs.dremio.com/dremio-cloud/api/catalog/wiki) |
| `GET /v0/projects/{pid}/catalog/{id}/collaboration/tag` | `schema wiki` | [Tag](https://docs.dremio.com/dremio-cloud/api/catalog/tag) |
| `POST /v0/projects/{pid}/search` | `catalog search` | [Search](https://docs.dremio.com/dremio-cloud/api/search) |
| `GET /v0/projects/{pid}/reflection/{id}` | `reflect status` | [Reflection](https://docs.dremio.com/dremio-cloud/api/reflection/) |
| `POST /v0/projects/{pid}/reflection/{id}/refresh` | `reflect refresh` | [Reflection](https://docs.dremio.com/dremio-cloud/api/reflection/) |
| `DELETE /v0/projects/{pid}/reflection/{id}` | `reflect drop` | [Reflection](https://docs.dremio.com/dremio-cloud/api/reflection/) |
| `GET /v1/users`, `GET /v1/users/name/{name}` | `access whoami`, `access audit` | — |
| `GET /v1/roles` | `access roles` | — |

Commands that query system tables (`jobs list`, `jobs profile`, `reflect list`, `schema sample`) use `POST /v0/projects/{pid}/sql` to submit SQL against `sys.project.*` tables.

## Configuration reference

`drs` resolves each setting using the first match (highest priority first):

| Priority | Token | Project ID | API URI |
|----------|-------|------------|---------|
| CLI flag | `--token` | `--project-id` | `--uri` |
| Env var | `DREMIO_TOKEN` | `DREMIO_PROJECT_ID` | `DREMIO_URI` |
| Env var | `DREMIO_PAT` *(legacy)* | | |
| Config file | `pat:` / `token:` | `project_id:` / `projectId:` | `uri:` / `endpoint:` |
| Default | *(required)* | *(required)* | `https://api.dremio.cloud` |

The config file also accepts the legacy `dremio-mcp` format (`token`, `projectId`, `endpoint`) for backwards compatibility.

```bash
# Custom config file
drs --config /path/to/my/config.yaml query run "SELECT 1"

# EU region
drs --uri https://api.eu.dremio.cloud query run "SELECT 1"
```

## Claude Code Plugin

`drs` ships with a Claude Code plugin that adds Dremio-aware skills to your coding sessions:

| Skill | What it does |
|-------|-------------|
| `dremio` | Core reference — SQL dialect, system tables, functions, REST patterns |
| `dremio-setup` | Interactive setup wizard for `drs` |
| `dremio-dbt` | dbt-dremio Cloud integration guide and patterns |
| `investigate-slow-query` | Walks through job profile analysis and reflection recommendations |
| `audit-dataset-access` | Traces grants, role inheritance, and effective permissions |
| `document-dataset` | Generates a documentation card from schema + lineage + wiki + sample data |
| `investigate-data-quality` | Null analysis, duplicate detection, outlier checks, freshness |
| `onboard-new-source` | End-to-end: discover, profile, reflect, set access, verify |

## For AI agents

`drs` is designed to be agent-friendly:

- **Structured JSON output** by default — no parsing needed
- **`drs describe <command>`** lets agents self-discover parameter schemas at runtime
- **`--fields` filtering** reduces output size to fit context windows
- **Input validation** catches hallucinated paths, malformed UUIDs, and injection attempts before they hit the API
- **Consistent error format** — all API errors return `{"error": "...", "status_code": N}` rather than raw HTTP tracebacks

If you're building an agent that talks to Dremio, you can either shell out to `drs` commands or import the async functions directly:

```python
from drs.auth import load_config
from drs.client import DremioClient
from drs.commands.query import run_query

config = load_config()
client = DremioClient(config)
result = await run_query(client, "SELECT * FROM myspace.orders LIMIT 10")
await client.close()
```

## Development

```bash
git clone https://github.com/dremio/cli.git
cd cli
uv sync

# Run tests (no Dremio instance needed — all HTTP is mocked)
uv run pytest tests/ -v

# Run a specific test file
uv run pytest tests/test_commands/test_query.py -v
```

### Project structure

```
src/drs/
  cli.py           # Entry point, command group registration
  auth.py          # Config loading (env > file > defaults)
  client.py        # The single HTTP layer (all API calls)
  output.py        # JSON / CSV / pretty formatting
  utils.py         # Path parsing, input validation, error handling
  introspect.py    # Command schema registry for drs describe
  commands/
    query.py       # run, status, cancel
    catalog.py     # list, get, search
    schema.py      # describe, lineage, wiki, sample
    reflect.py     # list, status, refresh, drop
    jobs.py        # list, get, profile
    access.py      # grants, roles, whoami, audit
```

## Related projects

| Repo | Relationship |
|------|-------------|
| `dremio/dremio-mcp` | Sibling — MCP server for AI agent integration. `drs` focuses on CLI; config format is shared. |
| `dremio/claude-plugins` | Predecessor — skills have been rewritten to use `drs` commands instead of raw curl. |

## License

Apache 2.0
