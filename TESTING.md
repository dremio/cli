# Testing drs

## Prerequisites

- Python 3.11+
- `uv` package manager
- Dremio Cloud account with a PAT and project ID

## Setup

1. Install dependencies:
   ```bash
   uv sync
   ```

2. Create config file at `~/.config/dremioai/config.yaml`:
   ```yaml
   pat: dremio_pat_xxxxxxxxxxxxx
   project_id: your-project-id
   ```

   Or export env vars:
   ```bash
   export DREMIO_PAT=dremio_pat_xxxxxxxxxxxxx
   export DREMIO_PROJECT_ID=your-project-id
   ```

## Unit tests

```bash
uv run pytest tests/ -v
```

All unit tests use mocked HTTP — no Dremio Cloud connection needed.

## Smoke tests (requires live Dremio Cloud)

### Query
```bash
drs query run "SELECT 1 AS test_value"
drs query run "SELECT 1" --output csv
drs query run "SELECT 1" --output pretty
```

### Catalog
```bash
drs catalog list
drs catalog search "orders"
drs catalog get "your_space.your_table"
```

### Schema
```bash
drs schema describe "your_space.your_table"
drs schema sample "your_space.your_table"
drs schema lineage "your_space.your_table"
drs schema describe-source "your_space.your_table"
```

### Reflections
```bash
drs reflect list "your_space.your_table"
```

### Jobs
```bash
drs jobs list
drs jobs list --status FAILED --limit 5
```

### Access
```bash
drs access roles
drs access whoami
drs access grants "your_space.your_table"
```

## MCP server verification

1. Start the server:
   ```bash
   drs mcp
   ```
   (Should block on stdio — Ctrl+C to stop)

2. Test with Claude Desktop: add the MCP config from README.md, restart Claude Desktop, verify all 13 tools appear.

3. Filtered start:
   ```bash
   drs mcp --services query,schema
   ```
   Only `dremio_query_*` and `dremio_schema_*` tools should appear.
