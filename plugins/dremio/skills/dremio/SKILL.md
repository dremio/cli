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
name: dremio-knowledge
description: Core Dremio Cloud SQL reference, system tables, REST API patterns, and built-in functions
user_invocable: true
triggers:
  - "dremio sql"
  - "dremio functions"
  - "system tables"
  - "dremio rest api"
  - "reflections"
  - "how do I query"
---

# Core Dremio Cloud Knowledge

## SQL Dialect Quick Reference

Dremio uses Apache Calcite SQL with Dremio-specific extensions. All queries run through `drs query run`.

### Running Queries

```bash
# Simple query
drs query run "SELECT * FROM my_source.my_table LIMIT 10"

# Query with a specific context (schema)
drs query run --context my_source "SELECT * FROM my_table LIMIT 10"
```

### Dremio-Specific Functions

| Function | Purpose | Example |
|---|---|---|
| `FLATTEN(col)` | Unnest arrays/maps into rows | `SELECT FLATTEN(tags) FROM events` |
| `APPROX_COUNT_DISTINCT(col)` | HyperLogLog distinct count | `SELECT APPROX_COUNT_DISTINCT(user_id) FROM clicks` |
| `CONVERT_FROM(col, 'JSON')` | Parse binary/varchar as JSON | `SELECT CONVERT_FROM(payload, 'JSON') FROM raw` |
| `DATE_TRUNC('month', ts)` | Truncate timestamp | `SELECT DATE_TRUNC('week', created_at) FROM orders` |
| `REGEXP_LIKE(col, pattern)` | Regex match | `SELECT * FROM t WHERE REGEXP_LIKE(name, '^A.*')` |
| `TO_DATE(str, fmt)` | Parse string to date | `SELECT TO_DATE('2026-01-15', 'YYYY-MM-DD')` |

### Path Syntax

Dremio uses dot-separated paths with double quotes for special characters:

```sql
-- Standard path
SELECT * FROM my_source.my_schema.my_table

-- Path with special characters
SELECT * FROM my_source."my-schema"."my table"
```

## System Tables

Query system tables for operational insight:

```bash
# Recent jobs (last 30 days)
drs query run "SELECT job_id, query_type, status, query, start_time, finish_time
  FROM sys.project.jobs_recent
  WHERE usr = 'alice@example.com'
  ORDER BY start_time DESC LIMIT 20"

# Reflection status
drs query run "SELECT reflection_id, name, type, status, dataset_name, num_failures
  FROM sys.reflections
  WHERE dataset_name = 'my_table'"

# Active reflection refresh jobs
drs query run "SELECT reflection_id, submitted, started, status
  FROM sys.materializations"
```

| System Table | Contents |
|---|---|
| `sys.project.jobs_recent` | Job history: query text, user, status, timing |
| `sys.reflections` | All reflections: type, status, dataset, columns |
| `sys.materializations` | Reflection refresh jobs and their status |
| `sys.options` | Current system option settings |
| `INFORMATION_SCHEMA.TABLES` | All tables/views visible to current user |
| `INFORMATION_SCHEMA.COLUMNS` | Column metadata for all tables |

## REST API Patterns

The `drs` CLI wraps the Dremio Cloud REST API. For operations not covered by CLI commands:

```bash
# The CLI uses these endpoints under the hood:
# GET  /api/v3/catalog           — list top-level sources
# GET  /api/v3/catalog/by-path/{path} — get dataset/folder by path
# POST /v0/projects/{id}/sql    — submit a SQL query
# GET  /v0/projects/{id}/job/{id} — get job status
# GET  /v0/projects/{id}/job/{id}/results — get job results
# GET  /api/v3/reflection/{id}  — get reflection details
```

## Common Patterns

### Check if a table exists

```bash
drs query run "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA = 'my_source.my_schema' AND TABLE_NAME = 'my_table'"
```

### Find large/slow queries

```bash
drs query run "SELECT job_id, usr, query, finish_time - start_time AS duration
  FROM sys.project.jobs_recent
  WHERE status = 'COMPLETED'
  ORDER BY (finish_time - start_time) DESC LIMIT 10"
```

### Check reflection usage for a query

```bash
drs jobs profile <job_id>
# Look for "Reflection Used" vs "Table Scan" in the output
```
