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
name: investigate-slow-query
description: Diagnose and optimize slow Dremio queries using job profiles, reflections, and execution plans
user_invocable: true
triggers:
  - "slow query"
  - "query performance"
  - "query optimization"
  - "why is my query slow"
  - "job profile"
  - "execution plan"
---

# Investigate Slow Query

Systematic workflow to diagnose why a Dremio query is slow and how to fix it.

## Agent Rules

- Use `--fields` on list/query commands to reduce output size (e.g., `--fields job_id,job_state,start_time`)
- Use `drs describe <command>` to check parameter schemas before calling unfamiliar commands
- Never interpolate user input directly into SQL — use it as filter values only
- Paths must be dot-separated; quote components containing dots: `"my.source".table`

## Step 1: Identify the Job

If the user provides a query but not a job ID, find it:

```bash
drs query run "SELECT job_id, status, start_time, finish_time,
    (finish_time - start_time) AS duration, query
  FROM sys.project.jobs_recent
  WHERE query LIKE '%<partial_query_text>%'
  ORDER BY start_time DESC LIMIT 5"
```

Note the `job_id` for the slow execution.

## Step 2: Get the Job Profile

```bash
drs jobs profile <job_id>
```

Key things to look for in the profile:

- **Planning time** vs **execution time** — If planning is slow, the query optimizer is struggling (too many joins, complex views).
- **Rows scanned** vs **rows returned** — Large ratio means missing filters or partition pruning failure.
- **Phases with high runtime** — Identify the bottleneck phase (scan, hash join, sort, etc.).
- **Spilling to disk** — Indicates memory pressure; look for "Spilled" indicators.

## Step 3: Check Reflection Usage

```bash
# List reflections on the primary dataset
drs reflect list <dataset_path>
```

Evaluate:

- **Are reflections available?** If none exist, the query hits raw data every time.
- **Are reflections ACTIVE?** Status must be `ACTIVE` to be used. `FAILED` or `REFRESHING` reflections are skipped.
- **Does the reflection match the query pattern?** Aggregate reflections must cover the query's GROUP BY and measures. Raw reflections must include all selected columns.

```bash
# Check reflection details in system table
drs query run "SELECT reflection_id, name, type, status, num_failures,
    dataset_name, created_at
  FROM sys.reflections
  WHERE dataset_name = '<dataset_name>'"
```

## Step 4: Analyze Table Scans

```bash
# Check if partition pruning is working
drs query run "SELECT job_id, status, start_time,
    input_records, output_records
  FROM sys.project.jobs_recent
  WHERE job_id = '<job_id>'"
```

Common scan problems:

| Symptom | Cause | Fix |
|---|---|---|
| Full table scan on large table | No filter on partition column | Add WHERE clause on partition column |
| Scans millions, returns dozens | Missing reflection | Create a raw reflection with filtered columns |
| Multiple scans of same table | Self-join pattern | Refactor to use window functions or CTE |

## Step 5: Check for Common Anti-Patterns

Run these diagnostic queries:

```bash
# Find queries with no reflection match (last 7 days)
drs query run "SELECT job_id, query, start_time, finish_time - start_time AS duration
  FROM sys.project.jobs_recent
  WHERE status = 'COMPLETED'
    AND start_time > CURRENT_TIMESTAMP - INTERVAL '7' DAY
  ORDER BY (finish_time - start_time) DESC LIMIT 20"
```

Common anti-patterns:
- **SELECT \*** — Forces full row reads. Select only needed columns.
- **LIKE '%value%'** — Leading wildcard prevents index/reflection use.
- **ORDER BY on non-indexed columns** — Causes expensive sorts.
- **Cross joins or Cartesian products** — Exponential row explosion.
- **Nested FLATTEN** — Each FLATTEN multiplies rows; combine when possible.

## Step 6: Recommend Optimizations

Based on findings, recommend one or more:

1. **Create a reflection** — Match the query's column set and aggregation pattern:
   ```bash
   # Raw reflection for frequently selected columns
   drs query run "ALTER DATASET my_source.my_table
     CREATE RAW REFLECTION my_reflection
     USING DISPLAY (col1, col2, col3)
     PARTITION BY (date_col)"
   ```

2. **Add partition pruning** — Ensure WHERE clauses target partition columns.

3. **Rewrite the query** — Eliminate anti-patterns found in Step 5.

4. **Check data layout** — Sorted/partitioned data scans faster:
   ```bash
   drs query run "SELECT * FROM TABLE(table_files('my_source.my_table')) LIMIT 10"
   ```

5. **Increase reflection refresh frequency** if data freshness is causing re-scans of raw data.

## Step 7: Verify Improvement

After applying changes, re-run the query and compare:

```bash
drs query run "<original_query>"
# Note the new job ID from output

drs jobs profile <new_job_id>
# Compare planning time, scan rows, and total duration to Step 2
```
