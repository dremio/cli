[![CI](https://github.com/dremio/cli/actions/workflows/ci.yml/badge.svg)](https://github.com/dremio/cli/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/dremio-cli)](https://pypi.org/project/dremio-cli/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

# dremio — Developer CLI for Dremio Cloud

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

`dremio` wraps all of this into a single binary with consistent output formats, input validation, and structured error handling.

## Prerequisites

- **Python 3.11+** (check with `python3 --version`)
- **A Dremio Cloud account** with a project
- **A Personal Access Token (PAT)** — generate one from Dremio Cloud under Account Settings > Personal Access Tokens

## Quickstart

### 1. Install

The package name is **`dremio-cli`** (not `dremio-client`, which is an unrelated third-party package).

```bash
# Recommended — isolated install, no venv needed
pipx install dremio-cli

# Or with uv (fast, also isolated)
uv tool install dremio-cli

# Or with pip (requires a virtual environment on modern Python)
pip install dremio-cli

# Or install from source
git clone https://github.com/dremio/cli.git
cd cli
uv tool install .

# Or for development (editable install)
uv sync
```

> **Tip:** On macOS and recent Linux distros, `pip install` into the system Python is blocked
> (`externally-managed-environment` error). Use `pipx` or `uv tool install` instead — they
> automatically create an isolated environment for you.

After install, verify the binary is available:

```bash
dremio --help
```

### 2. Configure

There are three ways to authenticate, in order of priority:

**Option A: CLI flags** (highest priority — override everything)

```bash
dremio --token YOUR_PAT --project-id YOUR_PROJECT_ID query run "SELECT 1"

# EU region
dremio --uri https://api.eu.dremio.cloud --token YOUR_PAT --project-id YOUR_PROJECT_ID query run "SELECT 1"
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
dremio query run "SELECT 1 AS hello"
```

If this returns `{"job_id": "...", "state": "COMPLETED", "rowCount": 1, "rows": [{"hello": "1"}]}`, you're set.

## Commands

### Overview

| Group | Commands | What it does |
|-------|----------|--------------|
| `dremio query` | `run`, `status`, `cancel` | Execute SQL, check job status, cancel running jobs |
| `dremio folder` | `list`, `get`, `create`, `delete`, `grants` | Browse and manage spaces/folders, view ACLs |
| `dremio schema` | `describe`, `lineage`, `sample` | Column types, dependency graph, preview rows |
| `dremio wiki` | `get`, `update` | Read and update wiki documentation on entities |
| `dremio tag` | `get`, `update` | Read and update tags on entities |
| `dremio reflection` | `create`, `list`, `get`, `refresh`, `delete` | Full CRUD for reflections (materialized views) |
| `dremio job` | `list`, `get`, `profile` | Recent jobs with filters, job details, operator-level profiles |
| `dremio engine` | `list`, `get`, `create`, `update`, `delete`, `enable`, `disable` | Full CRUD for Dremio Cloud engines |
| `dremio user` | `list`, `get`, `create`, `delete`, `whoami`, `audit` | Manage org users, check identity, audit permissions |
| `dremio role` | `list`, `get`, `create`, `update`, `delete` | Full CRUD for organization roles |
| `dremio grant` | `get`, `update`, `delete` | Manage grants on projects, engines, org resources |
| `dremio project` | `list`, `get`, `create`, `update`, `delete` | Full CRUD for Dremio Cloud projects |
| `dremio search` | *(top-level)* | Full-text search across all catalog entities |
| `dremio describe` | *(top-level)* | Machine-readable schema for any command |

### Examples

```bash
# Run a query and get results as a pretty table
dremio query run "SELECT * FROM myspace.orders LIMIT 5" --output pretty

# Search the catalog for anything matching "revenue"
dremio search "revenue"

# Create a space, then a folder inside it
dremio folder create "Analytics"
dremio folder create Analytics.reports

# Describe a table's columns
dremio schema describe myspace.analytics.monthly_revenue

# Read and update wiki docs on a table
dremio wiki get myspace.orders
dremio wiki update myspace.orders "Primary orders table. Refreshed daily from Salesforce."

# Update tags on a table
dremio tag update myspace.orders "pii,finance,daily"

# Create a raw reflection on a dataset
dremio reflection create myspace.orders --type raw
dremio reflection list myspace.orders

# Manage engines
dremio engine list
dremio engine create "analytics-engine" --size LARGE
dremio engine disable eng-abc-123

# Manage users and roles
dremio user list --output pretty
dremio role create "data-analyst"
dremio grant update projects my-project-id role role-abc "MANAGE_GRANTS,CREATE_TABLE"

# Find failed jobs from recent history
dremio job list --status FAILED --output pretty

# Audit what roles and permissions a user has
dremio user audit rahim.bhojani
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
dremio schema describe myspace.orders --fields columns.name,columns.type

# Only show job ID and state
dremio job list --fields job_id,job_state
```

This is especially useful for AI agents to keep context windows small.

### Command introspection

Discover parameters for any command programmatically:

```bash
dremio describe query.run
dremio describe reflection.list
```

Returns a JSON schema with parameter names, types, required/optional, and descriptions. Useful for building automation on top of `dremio`.

## CRUD design principle

Every Dremio object has consistent CLI commands using standard CRUD verbs (`list`, `get`, `create`, `update`, `delete`):

| Object | List | Get | Create | Update | Delete |
|--------|------|-----|--------|--------|--------|
| **Spaces/Folders** | `folder list` | `folder get` | `folder create` | — | `folder delete` |
| **Tables/Views** | `folder get` | `schema describe/sample` | `query run` (DDL) | `query run` (DDL) | `folder delete` |
| **Wiki** | — | `wiki get` | `wiki update` | `wiki update` | — |
| **Tags** | — | `tag get` | `tag update` | `tag update` | — |
| **Reflections** | `reflection list` | `reflection get` | `reflection create` | `reflection refresh` | `reflection delete` |
| **Engines** | `engine list` | `engine get` | `engine create` | `engine update` | `engine delete` |
| **Users** | `user list` | `user get` | `user create` | — | `user delete` |
| **Roles** | `role list` | `role get` | `role create` | `role update` | `role delete` |
| **Grants** | `grant get` | `grant get` | `grant update` | `grant update` | `grant delete` |
| **Projects** | `project list` | `project get` | `project create` | `project update` | `project delete` |
| **Jobs** | `job list` | `job get/profile` | `query run` | — | `query cancel` |

`folder create` uses SQL under the hood (`CREATE SPACE` for top-level, `CREATE FOLDER` for nested paths). All other mutations use the REST API.

## How it works

```
┌──────────────┐     ┌──────────────┐     ┌─────────────────┐
│  dremio CLI  │────▶│  client.py   │────▶│ Dremio Cloud API │
│  (typer)     │     │  (httpx)     │     │ (REST + SQL)     │
└──────────────┘     └──────────────┘     └─────────────────┘
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
| `GET /v0/projects/{pid}/job/{id}` | `query status`, `job get` | [Job](https://docs.dremio.com/dremio-cloud/api/job/) |
| `GET /v0/projects/{pid}/job/{id}/results` | `query run` (result fetch) | [Job Results](https://docs.dremio.com/dremio-cloud/api/job/job-results/) |
| `POST /v0/projects/{pid}/job/{id}/cancel` | `query cancel` | [Job](https://docs.dremio.com/dremio-cloud/api/job/) |
| `GET /v0/projects/{pid}/catalog` | `folder list` | [Catalog](https://docs.dremio.com/dremio-cloud/api/catalog/) |
| `GET /v0/projects/{pid}/catalog/by-path/{path}` | `folder get`, `schema describe`, `wiki get`, `tag get`, `folder grants` | [Catalog](https://docs.dremio.com/dremio-cloud/api/catalog/) |
| `DELETE /v0/projects/{pid}/catalog/{id}` | `folder delete` | [Catalog](https://docs.dremio.com/dremio-cloud/api/catalog/) |
| `GET /v0/projects/{pid}/catalog/{id}/graph` | `schema lineage` | [Lineage](https://docs.dremio.com/dremio-cloud/api/catalog/lineage) |
| `GET/PUT /v0/projects/{pid}/catalog/{id}/collaboration/wiki` | `wiki get`, `wiki update` | [Wiki](https://docs.dremio.com/dremio-cloud/api/catalog/wiki) |
| `GET/PUT /v0/projects/{pid}/catalog/{id}/collaboration/tag` | `tag get`, `tag update` | [Tag](https://docs.dremio.com/dremio-cloud/api/catalog/tag) |
| `POST /v0/projects/{pid}/search` | `search` | [Search](https://docs.dremio.com/dremio-cloud/api/search) |
| `POST /v0/projects/{pid}/reflection` | `reflection create` | [Reflection](https://docs.dremio.com/dremio-cloud/api/reflection/) |
| `GET /v0/projects/{pid}/reflection/{id}` | `reflection get` | [Reflection](https://docs.dremio.com/dremio-cloud/api/reflection/) |
| `POST /v0/projects/{pid}/reflection/{id}/refresh` | `reflection refresh` | [Reflection](https://docs.dremio.com/dremio-cloud/api/reflection/) |
| `DELETE /v0/projects/{pid}/reflection/{id}` | `reflection delete` | [Reflection](https://docs.dremio.com/dremio-cloud/api/reflection/) |
| `GET/POST/PUT/DELETE /v0/projects/{pid}/engines[/{id}]` | `engine list/get/create/update/delete` | [Engines](https://docs.dremio.com/dremio-cloud/api/) |
| `PUT /v0/projects/{pid}/engines/{id}/enable\|disable` | `engine enable`, `engine disable` | [Engines](https://docs.dremio.com/dremio-cloud/api/) |
| `GET /v1/users`, `GET /v1/users/name/{name}`, `GET /v1/users/{id}` | `user list/get`, `user whoami/audit` | [Users](https://docs.dremio.com/dremio-cloud/api/) |
| `POST /v1/users/invite` | `user create` | [Users](https://docs.dremio.com/dremio-cloud/api/) |
| `DELETE /v1/users/{id}` | `user delete` | [Users](https://docs.dremio.com/dremio-cloud/api/) |
| `GET /v1/roles[/{id}]`, `GET /v1/roles/name/{name}` | `role list/get` | [Roles](https://docs.dremio.com/dremio-cloud/api/) |
| `POST /v1/roles`, `PUT /v1/roles/{id}`, `DELETE /v1/roles/{id}` | `role create/update/delete` | [Roles](https://docs.dremio.com/dremio-cloud/api/) |
| `GET/PUT/DELETE /v1/{scope}/{id}/grants/{type}/{id}` | `grant get/update/delete` | [Grants](https://docs.dremio.com/dremio-cloud/api/) |

Commands that query system tables (`job list`, `job profile`, `reflection list`, `schema sample`) use `POST /v0/projects/{pid}/sql` to submit SQL against `sys.project.*` tables.

## Configuration reference

`dremio` resolves each setting using the first match (highest priority first):

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
dremio --config /path/to/my/config.yaml query run "SELECT 1"

# EU region
dremio --uri https://api.eu.dremio.cloud query run "SELECT 1"
```

## Claude Code Plugin

`dremio` ships with a Claude Code plugin that adds Dremio-aware skills to your coding sessions:

| Skill | What it does |
|-------|-------------|
| `dremio` | Core reference — SQL dialect, system tables, functions, REST patterns |
| `dremio-setup` | Interactive setup wizard for `dremio` |
| `dremio-dbt` | dbt-dremio Cloud integration guide and patterns |
| `investigate-slow-query` | Walks through job profile analysis and reflection recommendations |
| `audit-dataset-access` | Traces grants, role inheritance, and effective permissions |
| `document-dataset` | Generates a documentation card from schema + lineage + wiki + sample data |
| `investigate-data-quality` | Null analysis, duplicate detection, outlier checks, freshness |
| `onboard-new-source` | End-to-end: discover, profile, reflect, set access, verify |

## For AI agents

`dremio` is designed to be agent-friendly:

- **Structured JSON output** by default — no parsing needed
- **`dremio describe <command>`** lets agents self-discover parameter schemas at runtime
- **`--fields` filtering** reduces output size to fit context windows
- **Input validation** catches hallucinated paths, malformed UUIDs, and injection attempts before they hit the API
- **Consistent error format** — all API errors return `{"error": "...", "status_code": N}` rather than raw HTTP tracebacks

If you're building an agent that talks to Dremio, you can either shell out to `dremio` commands or import the async functions directly:

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
  introspect.py    # Command schema registry for dremio describe
  commands/
    query.py       # run, status, cancel
    folder.py      # list, get, create, delete, grants
    schema.py      # describe, lineage, sample
    wiki.py        # get, update
    tag.py         # get, update
    reflection.py  # create, list, get, refresh, delete
    job.py         # list, get, profile
    engine.py      # list, get, create, update, delete, enable, disable
    user.py        # list, get, create, delete, whoami, audit
    role.py        # list, get, create, update, delete
    grant.py       # get, update, delete
    project.py     # list, get, create, update, delete
```

## Related projects

| Repo | Relationship |
|------|-------------|
| `dremio/dremio-mcp` | Sibling — MCP server for AI agent integration. `dremio` focuses on CLI; config format is shared. |
| `dremio/claude-plugins` | Predecessor — skills have been rewritten to use `dremio` commands instead of raw curl. |

## License

Apache 2.0
