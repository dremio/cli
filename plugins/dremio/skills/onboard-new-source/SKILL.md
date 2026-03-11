---
name: onboard-new-source
description: Catalog, describe, reflect, and verify access for a new data source in Dremio
user_invocable: true
triggers:
  - "new source"
  - "onboard source"
  - "add source"
  - "catalog source"
  - "source setup"
  - "connect source"
---

# Onboard New Source

Systematic workflow to catalog, document, optimize, and secure a newly added data source in Dremio.

## Agent Rules

- Use `--fields` on list/query commands to reduce output size
- Use `--dry-run` on `reflect refresh` and `reflect drop` to validate before executing
- Use `drs describe <command>` to check parameter schemas before calling unfamiliar commands
- Never interpolate user input directly into SQL — use it as filter values only
- Paths must be dot-separated; quote components containing dots: `"my.source".table`

## Prerequisites

The data source must already be configured in Dremio (via UI or API). This skill covers the post-connection steps: discovery, documentation, reflection setup, and access control.

## Step 1: Discover the Source

```bash
# List all sources in the project
drs catalog list

# Get details about the new source
drs catalog get <source_name>
```

This shows:
- Source type (S3, PostgreSQL, Snowflake, etc.)
- Connection status
- Top-level schemas/databases within the source

## Step 2: Enumerate Tables

```bash
# List schemas within the source
drs query run "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA LIKE '<source_name>%'
  ORDER BY TABLE_SCHEMA, TABLE_NAME"
```

For each significant table, describe its schema:

```bash
drs schema describe <source_name>.<schema>.<table>
```

Record:
- Column names, types, and nullability
- Primary key candidates (unique, non-null columns)
- Partition columns (date/time columns, region codes)
- Approximate row counts

## Step 3: Profile Key Tables

For the most important tables, run a quick data profile:

```bash
drs schema sample <source_name>.<schema>.<table>
```

Then run basic statistics:

```bash
drs query run "SELECT
    COUNT(*) AS row_count,
    MIN(created_at) AS earliest,
    MAX(created_at) AS latest
  FROM <source_name>.<schema>.<table>"
```

This helps understand data volume, time range, and freshness.

## Step 4: Set Up Reflections

For tables that will be queried frequently, create reflections to accelerate performance.

### Raw Reflections (for SELECT queries)

```bash
drs query run "ALTER DATASET <source_name>.<schema>.<table>
  CREATE RAW REFLECTION raw_<table>
  USING DISPLAY (col1, col2, col3, col4)
  PARTITION BY (date_col)
  DISTRIBUTE BY (id_col)"
```

### Aggregate Reflections (for dashboards/rollups)

```bash
drs query run "ALTER DATASET <source_name>.<schema>.<table>
  CREATE AGGREGATE REFLECTION agg_<table>
  USING DIMENSIONS (dim1, dim2)
  MEASURES (measure1, measure2)
  PARTITION BY (date_col)"
```

### Verify Reflection Status

```bash
drs reflect list <source_name>.<schema>.<table>

# Or check across all tables in the source
drs query run "SELECT reflection_id, name, type, status, dataset_name
  FROM sys.reflections
  WHERE dataset_name LIKE '<source_name>%'"
```

Wait for reflections to reach `ACTIVE` status before relying on them.

## Step 5: Configure Access Control

```bash
# Check current grants on the source
drs access grants <source_name>

# Check grants on a specific dataset
drs access grants <source_name>.<schema>.<table>
```

Set up appropriate access:
- Grant `SELECT` on specific datasets to analyst roles
- Grant `ALTER` to data engineers who manage reflections
- Avoid granting `PUBLIC` role access to sensitive data

## Step 6: Verify End-to-End

Run test queries as different personas to confirm the setup works:

```bash
# Basic query to verify data access
drs query run "SELECT * FROM <source_name>.<schema>.<table> LIMIT 10"

# Verify reflection acceleration
drs query run "SELECT dim1, COUNT(*), SUM(measure1)
  FROM <source_name>.<schema>.<table>
  GROUP BY dim1"
# Check job profile to confirm reflection was used
drs jobs profile <job_id>
```

## Step 7: Onboarding Checklist

| Step | Check |
|------|-------|
| Source connected and browsable | `drs catalog get <source>` returns details |
| All tables enumerated | INFORMATION_SCHEMA query returns expected tables |
| Key tables profiled | Row counts, time ranges, sample data reviewed |
| Reflections created | `drs reflect list` shows ACTIVE reflections for key tables |
| Access grants configured | `drs access grants` shows correct role-based access |
| Test queries succeed | End-to-end query returns data with reflection acceleration |
| Documentation generated | Use the `document-dataset` skill for each key table |

After completing this checklist, the source is ready for production use. Notify downstream teams and update any data catalog or wiki entries.
