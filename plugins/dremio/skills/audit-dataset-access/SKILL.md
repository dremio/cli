---
name: audit-dataset-access
description: Trace grants, role inheritance, and effective permissions for users and datasets in Dremio
user_invocable: true
triggers:
  - "audit access"
  - "who has access"
  - "check permissions"
  - "role grants"
  - "dataset access"
  - "user permissions"
---

# Audit Dataset Access

Trace who can access a dataset, what roles grant that access, and what effective permissions a user has.

## Agent Rules

- Use `--fields` on list commands to reduce output size
- Use `drs describe <command>` to check parameter schemas before calling unfamiliar commands
- Never interpolate user input directly into SQL — use it as filter values only
- Paths must be dot-separated; quote components containing dots: `"my.source".table`

## Step 1: Check Grants on a Dataset

```bash
# See the ACL (access control list) for a specific dataset or folder
drs access grants <dataset_path>
```

This shows:
- **Users** with direct grants and their permission level (SELECT, ALTER, MANAGE GRANTS, etc.)
- **Roles** with grants on this dataset
- **Inherited permissions** from parent folders/spaces

Example:

```bash
drs access grants my_source.my_schema.my_table
```

## Step 2: List All Roles

```bash
# List all roles in the project
drs access roles
```

Review the role list to understand the permission hierarchy. Key built-in roles:
- **ADMIN** — Full control over the project
- **PUBLIC** — Default role assigned to all users

Custom roles will also appear here.

## Step 3: Inspect Role Members and Privileges

For each role that has grants on the target dataset:

```bash
# See what privileges a role has and where
drs access roles <role_name>
```

This shows:
- Members of the role (users and groups)
- All grants held by the role across the catalog

## Step 4: Audit a Specific User

```bash
# Trace a user's effective permissions across the catalog
drs access audit <username_or_email>
```

This resolves:
- Direct grants to the user
- Grants inherited through role memberships
- Grants inherited from parent folders/spaces

The output shows the **effective permission set** — what the user can actually do.

## Step 5: Cross-Reference with Query History

Verify whether a user has actually accessed the dataset:

```bash
drs query run "SELECT job_id, usr, query, status, start_time
  FROM sys.project.jobs_recent
  WHERE usr = '<username>'
    AND query LIKE '%<table_name>%'
  ORDER BY start_time DESC LIMIT 10"
```

## Step 6: Check for Overly Broad Access

Look for security risks:

```bash
# Find datasets with PUBLIC role access
drs access grants <dataset_path>
# Check if PUBLIC appears in the output

# Find all datasets a role can access
drs access roles <role_name>
# Review the grant list for unexpected breadth
```

### Common Access Issues

| Symptom | Diagnosis | Resolution |
|---|---|---|
| User can't query a table | No grant on table or parent | `drs access grants <path>` to check; add grant |
| User sees table but can't SELECT | Has MANAGE but not SELECT | Check grant type; add SELECT privilege |
| Too many people have access | PUBLIC role has grants | Remove PUBLIC grant; use specific roles |
| User lost access after move | Grants on old path don't follow | Re-grant on new path |
| New team member can't see anything | Not added to any role | Add to appropriate role |

## Step 7: Document Findings

Compile the audit results:

1. **Dataset path** and its direct grants
2. **Roles** with access and their members
3. **Users** with effective access (direct + inherited)
4. **Risk items** — overly broad grants, PUBLIC access, unused grants
5. **Recommendations** — tighten roles, remove stale grants, add missing grants
