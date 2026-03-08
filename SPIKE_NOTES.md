# Spike Notes — drs CLI

## Target: Dremio Cloud Only

This spike targets **Dremio Cloud** exclusively. Dremio Software has different auth (username/password), different base URLs, and API behavior differences. Software support is out of scope.

## API Gaps & Workarounds

### No list-reflections-by-dataset endpoint
The REST API has `GET /api/v3/reflection/{id}` but no way to list reflections for a dataset. Workaround: query `sys.reflections` via SQL with `WHERE dataset_id = ?`.

### No "whoami" endpoint
There's no REST endpoint to identify the current user from a PAT. The `GET /v1/users` endpoint lists users but doesn't indicate which one matches the current token. Future: decode the PAT JWT to extract user info, or add a dedicated endpoint.

### Job ID type inconsistency
The SQL submission endpoint returns a string UUID job ID, but some older endpoints use integer job IDs. The v0 project-scoped endpoints consistently use string UUIDs — this is what `drs` targets.

### Search API behavior
`POST /api/v3/search` accepts free-text queries. The response format varies between entity types. Some results include full metadata, others just IDs requiring a follow-up `GET /api/v3/catalog/{id}`.

### SQL vs REST boundary
Some operations are only available via SQL (jobs listing, reflection listing by dataset, job profiles), while others are only via REST (catalog browsing, reflection CRUD, user/role management). `drs` abstracts this — the user doesn't need to know which mechanism is used under the hood.

## Design Decisions

### Tool descriptions are the #1 quality bar
MCP tool descriptions determine whether an LLM selects the right tool. Each description was written to be self-contained — an LLM should know when to use it without reading other tool descriptions.

### Jobs via system tables
`drs jobs list` and `drs jobs profile` use `sys.project.jobs_recent` and `sys.project.job_profiles` SQL queries rather than REST endpoints. This is more flexible (supports filtering, ordering) and avoids undocumented pagination in the REST API.

### Async throughout
All command functions are `async`. The CLI wraps them with `asyncio.run()`. This avoids the overhead of maintaining both sync and async code paths, and the MCP server benefits from native async.

### Input validation
SQL-interpolated parameters (`status_filter`, `job_id`) are validated before string interpolation. Job states are checked against a whitelist; job IDs must match UUID format. This prevents injection via CLI arguments.

## What dremio-mcp does well
- Established config format (`~/.config/dremioai/config.yaml`) — preserved in `drs`
- MCP tool naming convention (`dremio_*`) — preserved
- Direct SQL execution and catalog browsing — core of `drs`

## What drs improves
- **CLI layer underneath MCP**: Every MCP tool has a corresponding CLI command for debugging, scripting, and non-AI workflows
- **Output formatting**: JSON, CSV, pretty table — MCP only had JSON
- **Command groups**: Organized by domain (query, catalog, schema, reflect, jobs, access) instead of flat tool list
- **Skills**: Claude Code plugin with 8 workflow-oriented skills

## What claude-plugins covered vs. what drs absorbs
- `claude-plugins` had 3 skills: core Dremio knowledge, setup, dbt
- All 3 are adapted into `drs` — rewritten to reference `drs` commands instead of raw curl
- `claude-plugins` repo can be archived or point to `drs`

## What dremio-agent-skill informed
- Wizard patterns (query_triage, source_onboarding, security_model) → `drs` workflow skills
- SQL reference depth → incorporated into core `dremio` skill
- Not merged directly — different owner (`developer-advocacy-dremio`), multi-tool targeting, different distribution model

## Path to Production

### Error handling
- Add retry with exponential backoff for transient HTTP errors (429, 503)
- Better error messages for common failures (expired PAT, wrong project ID, network errors)

### Pagination
- `list_users()` and `list_roles()` currently fetch one page. Add cursor-based pagination for large orgs.
- Job results pagination is implemented but could be parallelized.

### OAuth support
- Currently PAT-only. Add OAuth2 device flow for interactive CLI login.

### Open Catalog integration
- The new catalog is [Dremio Open Catalog](https://docs.dremio.com/current/data-sources/open-catalog/) (replacing Arctic/Nessie). Does not support branching/tagging yet. When it does, add catalog-versioning commands.

### PyPI publication
- Publish to PyPI as `drs` for `pip install drs` / `uv tool install drs`
- Add GitHub Actions CI for testing and release

### CI/CD
- GitHub Actions workflow: lint (ruff), test (pytest), build, optional publish
- Test matrix: Python 3.11, 3.12, 3.13
