# drs — Dremio Developer CLI: Architecture Review

**Repo:** `github.com/dremio/cli`
**Binary:** `drs`
**Target:** Dremio Cloud only (PAT + project ID)
**Stack:** Python 3.11+, typer, httpx, FastMCP, pydantic

---

## 1. What is this?

A single repo that ships three distribution channels for the same Dremio Cloud operations:

```
                    ┌──────────────────────────────────┐
                    │         dremio/cli repo           │
                    │                                   │
                    │  ┌───────┐ ┌───────┐ ┌────────┐  │
                    │  │  CLI  │ │  MCP  │ │ Plugin │  │
                    │  │ (drs) │ │Server │ │(Skills)│  │
                    │  └───┬───┘ └───┬───┘ └────────┘  │
                    │      │         │                  │
                    │      ▼         ▼                  │
                    │  ┌─────────────────┐              │
                    │  │   Command Layer  │              │
                    │  │  (async funcs)   │              │
                    │  └────────┬────────┘              │
                    │           │                       │
                    │           ▼                       │
                    │  ┌─────────────────┐              │
                    │  │   client.py      │              │
                    │  │  (single HTTP    │              │
                    │  │   layer — httpx) │              │
                    │  └────────┬────────┘              │
                    └───────────┼───────────────────────┘
                                │
                    ┌───────────▼───────────────────────┐
                    │      Dremio Cloud REST API         │
                    │  v0 (SQL/Jobs) │ v3 (Catalog/     │
                    │                │  Reflections)     │
                    │  v1 (Users/    │                   │
                    │     Roles/RBAC)│                   │
                    └───────────────────────────────────┘
```

| Channel | What | Who | How |
|---------|------|-----|-----|
| **CLI** (`drs`) | Terminal commands | Developers, scripts, CI | `drs query run "SELECT 1"` |
| **MCP Server** (`drs mcp`) | Stdio tools for AI agents | Claude Desktop, Claude Code, any MCP client | `drs mcp` → 19 tools |
| **Plugin** (skills) | Claude Code workflow skills | Claude Code users | `/plugin install dremio@dremio-cli` → 8 skills |

**Key constraint:** The CLI commands and MCP tools call the **exact same async functions**. Zero duplicated business logic. `client.py` is the only file that makes HTTP calls.

---

## 2. Why not just dremio-mcp?

| | `dremio-mcp` (existing) | `dremio/cli` (this) |
|---|---|---|
| CLI layer | None | Full CLI with 18 commands |
| Debugging | Requires MCP client | `drs query run "..."` directly |
| Output formats | JSON only | JSON, CSV, pretty table |
| Scripting | Not possible | Pipe-friendly (`drs jobs list --output csv \| ...`) |
| Skills | Separate repo (`claude-plugins`) | Same repo, references CLI commands |
| Organization | Flat tool list | Grouped by domain (query, catalog, schema, etc.) |

`dremio-mcp` goes straight from API to MCP tools. `drs` puts a CLI layer in between, making every operation testable, scriptable, and debuggable without an AI agent.

---

## 3. Command Inventory

### CLI Commands (18) across 6 groups

| Group | Subcommands | Mechanism | Description |
|-------|-------------|-----------|-------------|
| **query** | `run`, `status`, `cancel` | REST (v0) | Execute SQL, poll for results, cancel jobs |
| **catalog** | `list`, `get`, `search` | REST (v3) | Browse sources/spaces, get entity metadata, full-text search |
| **schema** | `describe`, `lineage`, `wiki`, `sample` | REST (v3) + SQL | Column types, dependency graph, wiki/tags, sample rows |
| **reflect** | `list`, `status`, `refresh`, `drop` | SQL + REST (v3) | List reflections (via sys.reflections), CRUD single reflection |
| **jobs** | `list`, `get`, `profile` | SQL + REST (v0) | Recent jobs (via sys.project.jobs_recent), job details, execution profile |
| **access** | `grants`, `roles`, `whoami`, `audit` | REST (v3, v1) | ACLs, role listing, user info, permission audit |

### MCP Tools (19)

Every CLI command plus `drs mcp` itself. Each tool has an LLM-optimized description — this is the #1 quality bar for agent usability.

### Skills (8)

| Tier | Skill | What it does |
|------|-------|-------------|
| **Core** | `dremio` | SQL reference, system tables, functions, REST patterns |
| **Setup** | `dremio-setup` | Install wizard: config file, PAT, verify, MCP setup |
| **Integration** | `dremio-dbt` | dbt-dremio profiles, materializations, troubleshooting |
| **Workflow** | `investigate-slow-query` | Job profile → reflection check → optimization recommendations |
| **Workflow** | `audit-dataset-access` | Grants → roles → effective permissions trace |
| **Workflow** | `document-dataset` | Schema + lineage + sample + wiki → markdown doc card |
| **Workflow** | `investigate-data-quality` | Null analysis, duplicates, outliers, freshness checks |
| **Workflow** | `onboard-new-source` | Discover → describe → reflect → verify access |

Skill naming follows the Google Workspace CLI taxonomy:
- **Tier 1** (API): `dremio` (core reference)
- **Tier 2** (Action): `dremio-setup`, `dremio-dbt`
- **Tier 3** (Workflow): `investigate-*`, `audit-*`, `document-*`, `onboard-*`

---

## 4. SQL vs REST: The Dual-Mechanism Design

Some Dremio operations are only available via REST, others only via SQL system tables. The CLI abstracts this — users don't need to know which mechanism is used.

```
┌─────────────────────────────────────────────────┐
│              Command Layer                       │
│                                                  │
│  REST-based              SQL-based               │
│  ─────────              ─────────               │
│  catalog list/get/search  jobs list              │
│  schema describe/lineage  jobs profile           │
│  schema wiki              reflect list (by path) │
│  reflect status/refresh   schema sample          │
│  reflect drop                                    │
│  access grants/roles                             │
│  access whoami/audit                             │
│  query run/status/cancel                         │
│         │                       │                │
│         ▼                       ▼                │
│  ┌──────────────┐    ┌──────────────────┐       │
│  │  client.py   │    │  run_query()      │       │
│  │  (HTTP calls)│    │  (submits SQL via │       │
│  │              │    │   client.py)      │       │
│  └──────────────┘    └──────────────────┘       │
└─────────────────────────────────────────────────┘
```

**Why SQL for some commands?**
- **No REST endpoint** to list reflections by dataset — must query `sys.reflections`
- **No REST endpoint** to list recent jobs with filtering — must query `sys.project.jobs_recent`
- **Sample data** is naturally a SQL operation (`SELECT * FROM ... LIMIT 10`)

**SQL injection mitigation:** All SQL-interpolated parameters are validated before insertion. Job states are checked against a whitelist; job IDs must match UUID format.

---

## 5. API Coverage

### Source of truth: `dremio/js-sdk`

The [dremio/js-sdk](https://github.com/dremio/js-sdk) TypeScript SDK is the canonical, auto-maintained API surface. `scripts/parse_jssdk.py` parses it to extract every endpoint:

```
js-sdk endpoints:      79 unique (97 total calls across 15 resources)
Covered by drs:        14
Not in drs:            65  (engines, AI, projects, scripts, etc.)
drs-only (SQL-based):   5  (jobs list, profiles, reflection list, sample, lineage)
```

**Resources by coverage:**

| Resource | js-sdk endpoints | drs coverage | Priority |
|----------|-----------------|--------------|----------|
| jobs | 6 | 4 covered | Core |
| catalog | 16 | 4 covered | Core |
| users | 8 | 3 covered | Core |
| roles | 5 | 2 covered | Core |
| grants | 1 | — (matched differently) | Core |
| ai | 18 | 0 | High — agent conversations, model providers |
| engines | 5 | 0 | Medium — list, start, stop |
| scripts | 3 | 0 | Medium — saved SQL |
| projects | 6 | 0 | Low — admin |
| organizations | 2 | 0 | Low — admin |
| arctic | 2 | 0 | Deferred — Open Catalog |

Run coverage yourself:
```bash
git clone --depth 1 https://github.com/dremio/js-sdk.git /tmp/js-sdk
python scripts/parse_jssdk.py --sdk-path /tmp/js-sdk --compare
```

---

## 6. Discovery Service Roadmap

Current state: hand-maintained client methods. Future: auto-generated from js-sdk.

### Phase progression:

```
Phase 1 (now)          Phase 2 (next)          Phase 3 (future)
──────────────         ───────────────         ────────────────
Hand-written      →    js-sdk parsed      →    Runtime discovery
+ js-sdk parser        + auto-generated         + zero-release updates
+ coverage report      client & CLI
```

**Phase 1 (done):** Regex parser proves the concept — `parse_jssdk.py` extracts 79 endpoints from js-sdk, compares against drs coverage. Not production-grade (can't extract types, breaks on refactors).

**Phase 2 (next — requires js-sdk team):** Request `discovery.json` export from js-sdk build. The SDK already has typed Zod schemas and TypeScript interfaces for every endpoint — we need them serialized to JSON Schema. This gives us parameter types, enums, required fields, response schemas, deprecation markers.

**Phase 3 (after discovery.json exists):**
1. `registry.py` loads `discovery.json` into typed `ApiOperation` objects
2. `executor.py` replaces hand-written client methods with generic HTTP execution
3. CLI and MCP tools auto-generated from registry — including `--help`, `--fields`, `--dry-run`
4. SQL-based commands coexist via explicit `SQL_COMMANDS` registry
5. CI fetches new `discovery.json` on js-sdk release → auto-detect new endpoints

**Phase 4 (optional, requires server-side):**
1. Dremio Cloud serves `/api/openapi.json`
2. `drs` fetches at startup, caches 24h
3. New endpoints appear in CLI without a drs release

Full plan: `DISCOVERY_SERVICE_PLAN.md`

---

## 7. Relationship to Existing Repos

```
┌─────────────────────┐
│  dremio/dremio-mcp   │ ──── Predecessor. drs mcp supersedes it.
│  (existing MCP)      │      Config format preserved for compatibility.
└─────────────────────┘

┌─────────────────────┐
│  dremio/claude-plugins│ ──── Absorbed. 3 skills rewritten to use drs
│  (existing skills)   │      commands. Repo can be archived.
└─────────────────────┘

┌──────────────────────────────┐
│  developer-advocacy-dremio/   │ ──── Referenced. Wizard patterns
│  dremio-agent-skill           │      informed skill design.
│  (knowledge pack)             │      Not merged — different owner/scope.
└──────────────────────────────┘
```

---

## 8. Project Structure

```
dremio/cli/
├── .claude-plugin/marketplace.json    # Claude Code marketplace registration
├── plugins/dremio/
│   ├── .claude-plugin/plugin.json     # Plugin manifest
│   └── skills/                        # 8 SKILL.md files
├── src/drs/
│   ├── cli.py                         # Typer entry point, registers all groups
│   ├── auth.py                        # PAT + config loading (env > file > defaults)
│   ├── client.py                      # ONLY file making HTTP calls (18 methods)
│   ├── output.py                      # JSON/CSV/pretty formatting
│   ├── utils.py                       # Path parsing, validation, error handling
│   ├── mcp_server.py                  # FastMCP adapter (19 tools, zero logic)
│   └── commands/                      # 6 command modules
│       ├── query.py                   # run, status, cancel
│       ├── catalog.py                 # list, get, search
│       ├── schema.py                  # describe, lineage, wiki, sample
│       ├── reflect.py                 # list, status, refresh, drop
│       ├── jobs.py                    # list, get, profile
│       └── access.py                  # grants, roles, whoami, audit
├── scripts/
│   ├── parse_jssdk.py                 # Parses dremio/js-sdk → api_registry.json
│   └── validate_api_coverage.py       # Legacy OpenAPI spec validator
├── docs/
│   ├── api_registry.json              # Machine-readable endpoint catalog (79 endpoints)
│   ├── coverage_report.json           # drs vs js-sdk coverage comparison
│   ├── ARCHITECTURE_REVIEW.md         # This document
│   └── ONE_PAGER.md                   # Concise overview
├── tests/                             # 56 unit tests (mocked HTTP)
├── README.md
├── TESTING.md
├── SPIKE_NOTES.md
└── DISCOVERY_SERVICE_PLAN.md
```

---

## 9. Open Questions for Review

1. **Naming:** Repo is `dremio/cli`, binary is `drs`. Any concerns with the short name?

2. **Scope:** Currently Dremio Cloud only. Should we design the auth layer now to accommodate Software later, or keep it strictly Cloud?

3. **Write operations:** Current commands are read-only + reflection refresh/drop. Should we add catalog CRUD (create source, create VDS), or keep the first release read-heavy for safety?

4. **Discovery service priority:** `parse_jssdk.py` already extracts 79 endpoints from js-sdk. Should we build the auto-generation pipeline (registry → executor → dynamic CLI) before the first release, or ship hand-maintained and iterate?

5. **Skill taxonomy:** Following Google's pattern (Tier 1 API → Tier 2 Action → Tier 3 Workflow → Personas → Recipes). How deep should we go for v1?

6. **Auth evolution:** PAT-only today. OAuth2 device flow for interactive login — priority for v1 or later?

7. **Distribution:** PyPI (`pip install dremio-cli`), Homebrew tap, or both?
