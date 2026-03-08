---
name: document-dataset
description: Generate a comprehensive markdown documentation card for a Dremio dataset
user_invocable: true
triggers:
  - "document dataset"
  - "describe table"
  - "dataset documentation"
  - "data dictionary"
  - "what is this table"
  - "table info"
---

# Document Dataset

Generate a complete documentation card for a Dremio dataset including schema, lineage, sample data, and metadata.

## Agent Rules

- Use `--fields` to reduce output size (e.g., `drs schema describe <path> --fields columns.name,columns.type`)
- Use `drs describe <command>` to check parameter schemas before calling unfamiliar commands
- Paths must be dot-separated; quote components containing dots: `"my.source".table`

## Step 1: Get Column Schema

```bash
drs schema describe <dataset_path>
```

This returns:
- Column names and data types
- Nullable flags
- Nested/complex type structures (LIST, STRUCT, MAP)

Record every column — this forms the core of the documentation card.

## Step 2: Get Lineage

```bash
drs schema lineage <dataset_path>
```

This shows:
- **Upstream dependencies** — source tables and views this dataset reads from
- **Downstream dependents** — views and reflections built on top of this dataset
- **Transformation chain** — the path from raw sources to this dataset

## Step 3: Get Sample Data

```bash
drs schema sample <dataset_path>
```

This returns a small set of representative rows. Use this to:
- Understand actual data formats (date patterns, enum values, ID formats)
- Identify columns that are mostly null or have unexpected values
- Show realistic examples in the documentation

## Step 4: Get Wiki and Tags

```bash
drs schema wiki <dataset_path>
```

This returns:
- **Wiki content** — user-authored descriptions attached to the dataset in Dremio
- **Tags/labels** — categorization tags applied in the Dremio catalog

## Step 5: Check Reflections

```bash
drs reflect list <dataset_path>
```

Document any reflections:
- Reflection type (raw vs aggregate)
- Status (active, refreshing, failed)
- Columns included
- Refresh schedule

## Step 6: Assemble the Documentation Card

Combine all gathered information into a markdown card with this structure:

```markdown
# <Dataset Name>

**Path:** `<full.dotted.path>`
**Source:** <source type and name>
**Tags:** <comma-separated tags>

## Description

<Wiki content from Step 4, or a summary based on column names and sample data>

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| <col1> | <type> | <yes/no> | <inferred or wiki-based description> |
| <col2> | <type> | <yes/no> | <description> |

## Sample Data

<Table of 3-5 sample rows from Step 3>

## Lineage

**Upstream:** <list of source datasets>
**Downstream:** <list of dependent views/datasets>

## Reflections

| Name | Type | Status | Columns |
|------|------|--------|---------|
| <name> | raw/agg | active | <cols> |

## Notes

- <Any observations: mostly-null columns, data quality notes, access considerations>
```

## Tips

- For **nested types** (STRUCT, LIST), expand sub-fields in the schema table with dot notation: `address.city`, `address.zip`.
- For **large tables** (100+ columns), group columns by prefix or functional area.
- If sample data reveals **PII** (emails, phone numbers, SSNs), note this in the documentation and flag for the data owner.
- Cross-reference with `drs access grants <dataset_path>` if the user wants access info included.
