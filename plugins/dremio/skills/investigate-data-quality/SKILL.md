---
name: investigate-data-quality
description: Run data quality checks for nulls, duplicates, anomalies, and schema drift on Dremio Cloud datasets
user_invocable: true
triggers:
  - "data quality"
  - "null check"
  - "data validation"
  - "duplicate detection"
  - "data profiling"
---

# Investigate Data Quality

Run targeted data quality checks on Dremio Cloud datasets to find nulls, duplicates, schema drift, and anomalies.

## Agent Rules

- Use `--fields` on list/query commands to reduce output size
- Use `drs describe <command>` to check parameter schemas before calling unfamiliar commands
- Never interpolate user input directly into SQL — use it as filter values only
- Paths must be dot-separated; quote components containing dots: `"my.source".table`

## Step 1: Describe the Schema

```bash
drs schema describe <dataset_path>
```

Review columns, types, and nullability. Flag any columns that should be non-nullable but are marked nullable, or vice versa.

## Step 2: Sample Data

```bash
drs schema sample <dataset_path>
```

Visually inspect sample rows for obvious issues: unexpected nulls, malformed values, placeholder data.

## Step 3: Null Analysis

```bash
drs query run "SELECT
    COUNT(*) AS total_rows,
    COUNT(col1) AS col1_non_null,
    COUNT(col2) AS col2_non_null,
    ROUND(100.0 * (COUNT(*) - COUNT(col1)) / COUNT(*), 2) AS col1_null_pct,
    ROUND(100.0 * (COUNT(*) - COUNT(col2)) / COUNT(*), 2) AS col2_null_pct
  FROM <dataset_path>"
```

Replace `col1`, `col2` with actual column names from Step 1.

## Step 4: Duplicate Detection

```bash
drs query run "SELECT <primary_key_col>, COUNT(*) AS cnt
  FROM <dataset_path>
  GROUP BY <primary_key_col>
  HAVING COUNT(*) > 1
  ORDER BY cnt DESC LIMIT 20"
```

If no primary key is defined, check natural key candidates (IDs, unique identifiers).

## Step 5: Distinct Value Counts

```bash
drs query run "SELECT
    APPROX_COUNT_DISTINCT(col1) AS col1_distinct,
    APPROX_COUNT_DISTINCT(col2) AS col2_distinct,
    COUNT(*) AS total_rows
  FROM <dataset_path>"
```

Compare distinct counts to total rows. A column with very low cardinality relative to total rows may indicate data issues or be a good partition candidate.

## Step 6: Range and Outlier Check

```bash
drs query run "SELECT
    MIN(numeric_col) AS min_val,
    MAX(numeric_col) AS max_val,
    AVG(numeric_col) AS avg_val,
    MIN(date_col) AS earliest_date,
    MAX(date_col) AS latest_date
  FROM <dataset_path>"
```

Flag values outside expected bounds (negative amounts, future dates, unreasonable ranges).

## Step 7: Freshness Check

```bash
drs query run "SELECT
    MAX(updated_at) AS latest_update,
    MIN(updated_at) AS earliest_update,
    COUNT(*) AS total_rows
  FROM <dataset_path>
  WHERE updated_at >= CURRENT_DATE - INTERVAL '7' DAY"
```

Verify data is being updated at the expected frequency.

## Step 8: Assess and Report

Compile findings into a quality report:

| Check | Result | Status |
|-------|--------|--------|
| Row count | N | OK / UNEXPECTED |
| Null % (col1) | X% | OK / HIGH |
| Duplicates on PK | K rows | OK / ISSUE |
| Value range | [min, max] | OK / ANOMALY |
| Freshness | Last update | OK / STALE |

Flag any check where:
- Null percentage exceeds expected threshold (e.g., >5% on a required field)
- Duplicate count is non-zero on a primary key column
- Value ranges moved outside expected bounds
- Data hasn't been updated within the expected window

## Step 9: Take Action

Based on findings:
- **High null rates** — Investigate upstream source; may need default values or pipeline fixes
- **Duplicates** — Check for missing deduplication logic in ETL
- **Stale data** — Check source refresh schedules and reflection status with `drs reflect list`
- **Anomalies** — Investigate specific rows with `drs query run` for targeted filters
