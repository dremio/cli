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
name: dremio-dbt
description: Set up and use dbt-dremio for transformations on Dremio Cloud
user_invocable: true
triggers:
  - "dbt dremio"
  - "dbt-dremio"
  - "dbt profiles"
  - "dbt setup"
  - "dbt cloud"
---

# dbt-dremio Integration

## Step 1: Install dbt-dremio

```bash
pip install dbt-dremio
```

Verify:

```bash
dbt --version
# Should list dremio among installed adapters
```

## Step 2: Configure profiles.yml

```yaml
# ~/.dbt/profiles.yml
my_dremio_project:
  target: dev
  outputs:
    dev:
      type: dremio
      threads: 4
      cloud_host: api.dremio.cloud
      cloud_project_id: YOUR_PROJECT_ID
      user: YOUR_EMAIL
      pat: YOUR_PERSONAL_ACCESS_TOKEN
      use_ssl: true
      # Where dbt materializes models
      root_path: "@dremio"  # or a specific space like "my_space"
      # Default schema for source references
      path: my_source.my_schema
```

## Step 3: Verify Connection

```bash
dbt debug --profiles-dir ~/.dbt
```

All checks should pass. If not, see Troubleshooting below.

## Step 4: Common dbt Commands

```bash
# Run all models
dbt run

# Run a specific model
dbt run --select my_model

# Test all models
dbt test

# Generate and serve docs
dbt docs generate
dbt docs serve

# Freshness check on sources
dbt source freshness
```

## Dremio-Specific dbt Patterns

### Materializations

dbt-dremio supports these materializations:

| Type | Dremio Object | Notes |
|---|---|---|
| `view` | Virtual dataset (VDS) | Default. No storage cost. |
| `table` | Physical dataset (PDS) | Creates a CTAS into your `root_path` |
| `incremental` | Physical dataset | Appends or merges new data |

### Model Config Example

```sql
-- models/staging/stg_orders.sql
{{
  config(
    materialized='view',
    schema='staging'
  )
}}

SELECT
  order_id,
  customer_id,
  DATE_TRUNC('day', order_date) AS order_day,
  total_amount
FROM {{ source('raw', 'orders') }}
WHERE order_date >= '2025-01-01'
```

### Using Dremio Functions in dbt

Dremio-specific functions work directly in model SQL:

```sql
SELECT
  FLATTEN(line_items) AS item,
  APPROX_COUNT_DISTINCT(customer_id) AS unique_customers
FROM {{ ref('stg_orders') }}
```

## Cross-Referencing with drs

Use `drs` to inspect what dbt created:

```bash
# See the VDS dbt created
drs schema describe "@dremio".staging.stg_orders

# Check lineage
drs schema lineage "@dremio".staging.stg_orders

# Verify data
drs query run "SELECT * FROM \"@dremio\".staging.stg_orders LIMIT 5"
```

## Troubleshooting

| Issue | Fix |
|---|---|
| `Could not connect to Dremio` | Verify `cloud_host` is `api.dremio.cloud` (not `app.dremio.cloud`) |
| `Authentication failed` | Regenerate PAT; ensure `user` matches the PAT owner email |
| `Schema not found` | The `path` in profiles.yml must be a valid dot-path to an existing schema |
| `Permission denied on CTAS` | Ensure your user has write access to `root_path` |
| `Reflection not used` | dbt views/tables are separate from reflections; use `drs reflect` to manage those |
| Models run but data is stale | Check `dbt source freshness`; verify source data is up to date in Dremio |
