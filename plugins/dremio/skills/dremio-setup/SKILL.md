<!--
Copyright (C) 2017-2026 Dremio Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

---
name: dremio-setup
description: Setup wizard for drs CLI — install, configure, verify connection to Dremio Cloud
user_invocable: true
triggers:
  - "setup dremio"
  - "install drs"
  - "configure dremio"
  - "connect to dremio"
  - "dremio config"
  - "getting started"
---

# Dremio Setup Wizard

Follow these steps to install and configure the `drs` CLI for Dremio Cloud.

## Step 1: Install drs

Choose one method:

```bash
# Recommended: install as a standalone tool via uv
uv tool install drs

# Alternative: install via pip
pip install drs

# Verify installation
drs --version
```

## Step 2: Get Your Credentials

You need three values from Dremio Cloud:

1. **URI** — Your Dremio Cloud endpoint (e.g., `https://app.dremio.cloud`)
2. **Personal Access Token (PAT)** — Generate at: Dremio UI > Account Settings > Personal Access Tokens > Create Token
3. **Project ID** — Found in the URL when viewing your project: `https://app.dremio.cloud/project/<project_id>/...`

## Step 3: Create Config File

Create the config file at `~/.config/dremioai/config.yaml`:

```bash
mkdir -p ~/.config/dremioai
```

Write the following content, replacing placeholders with your values:

```yaml
# ~/.config/dremioai/config.yaml
uri: https://app.dremio.cloud
pat: YOUR_PERSONAL_ACCESS_TOKEN
project_id: YOUR_PROJECT_ID
```

Secure the file since it contains your PAT:

```bash
chmod 600 ~/.config/dremioai/config.yaml
```

## Step 4: Verify Connection

```bash
# Run a simple test query
drs query run "SELECT 1 AS connected"
```

Expected output: a single row with `connected = 1`.

If this fails, check:
- **401 Unauthorized** — PAT is invalid or expired. Generate a new one.
- **403 Forbidden** — PAT lacks permissions for the project. Check project membership.
- **Connection refused** — URI is wrong. Verify the endpoint.
- **Project not found** — Project ID is incorrect. Copy it from the URL bar.

## Step 5: Explore Your Catalog

```bash
# List top-level sources and spaces
drs catalog list

# Describe a specific source
drs catalog get my_source

# Run a real query against your data
drs query run "SELECT * FROM my_source.my_schema.my_table LIMIT 5"
```

## Step 6 (Optional): Configure MCP Integration

To use `drs` as an MCP server (e.g., with Claude Code or other AI tools):

```bash
drs mcp
```

This starts the MCP server, exposing Dremio operations as tools. Configure your MCP client to connect to the `drs` process.

For Claude Code, add to your MCP config:

```json
{
  "mcpServers": {
    "dremio": {
      "command": "drs",
      "args": ["mcp"]
    }
  }
}
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `command not found: drs` | Ensure `~/.local/bin` (uv) or pip's bin dir is on your PATH |
| Config file not found | Verify path is exactly `~/.config/dremioai/config.yaml` |
| SSL errors | Check your network/proxy settings; try `export REQUESTS_CA_BUNDLE=/path/to/cert` |
| Timeout on queries | Large queries may take time; check job status with `drs jobs list` |
