"""Runtime schema introspection for drs commands.

Provides machine-readable descriptions of command parameters, types, and
constraints. Agents call `drs describe <command>` to self-serve schema info
instead of relying on pre-stuffed documentation (cheaper on token budget,
always up to date).
"""

from __future__ import annotations

from drs.utils import VALID_JOB_STATES

# Command registry — one entry per CLI command with full parameter schema.
# This is the canonical source of truth for what each command accepts.

COMMAND_SCHEMAS: dict[str, dict] = {
    "query.run": {
        "group": "query",
        "command": "run",
        "description": "Execute a SQL query against Dremio Cloud, wait for completion, return results as JSON.",
        "mechanism": "REST",
        "endpoints": ["POST /v0/projects/{pid}/sql", "GET /v0/projects/{pid}/job/{id}", "GET /v0/projects/{pid}/job/{id}/results"],
        "parameters": [
            {"name": "sql", "type": "string", "required": True, "positional": True, "description": "SQL query to execute"},
            {"name": "context", "type": "string", "required": False, "description": "Dot-separated default schema context (e.g., myspace.folder)"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"], "description": "Output format"},
            {"name": "fields", "type": "string", "required": False, "flag": "--fields/-f", "description": "Comma-separated fields to include in output (reduces context window usage)"},
        ],
    },
    "query.status": {
        "group": "query",
        "command": "status",
        "description": "Check the status of a Dremio job by UUID.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects/{pid}/job/{id}"],
        "parameters": [
            {"name": "job_id", "type": "string", "required": True, "positional": True, "format": "uuid", "description": "Job ID (UUID)"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "query.cancel": {
        "group": "query",
        "command": "cancel",
        "description": "Cancel a running Dremio job. No effect if already completed.",
        "mechanism": "REST",
        "endpoints": ["POST /v0/projects/{pid}/job/{id}/cancel"],
        "mutating": True,
        "parameters": [
            {"name": "job_id", "type": "string", "required": True, "positional": True, "format": "uuid", "description": "Job ID (UUID) to cancel"},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False, "flag": "--dry-run", "description": "Check job status without cancelling"},
        ],
    },
    "catalog.list": {
        "group": "catalog",
        "command": "list",
        "description": "List top-level catalog entities: sources, spaces, and home folder.",
        "mechanism": "REST",
        "endpoints": ["GET /api/v3/catalog"],
        "parameters": [
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False, "description": "Comma-separated fields to include"},
        ],
    },
    "catalog.get": {
        "group": "catalog",
        "command": "get",
        "description": "Get full metadata for a catalog entity by dot-separated path.",
        "mechanism": "REST",
        "endpoints": ["GET /api/v3/catalog/by-path/{path}"],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True, "description": "Dot-separated entity path (e.g., myspace.folder.table)"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False, "description": "Comma-separated fields to include"},
        ],
    },
    "catalog.search": {
        "group": "catalog",
        "command": "search",
        "description": "Full-text search for tables, views, and sources by keyword.",
        "mechanism": "REST",
        "endpoints": ["POST /api/v3/search"],
        "parameters": [
            {"name": "term", "type": "string", "required": True, "positional": True, "description": "Search term"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "schema.describe": {
        "group": "schema",
        "command": "describe",
        "description": "Get column names, data types, and nullability for a table or view.",
        "mechanism": "REST",
        "endpoints": ["GET /api/v3/catalog/by-path/{path}"],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True, "description": "Dot-separated table/view path"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False, "description": "Comma-separated fields to include (e.g., 'columns.name,columns.type')"},
        ],
    },
    "schema.lineage": {
        "group": "schema",
        "command": "lineage",
        "description": "Get upstream and downstream dependency graph for a table or view.",
        "mechanism": "REST",
        "endpoints": ["GET /api/v3/catalog/by-path/{path}", "GET /api/v3/catalog/{id}/graph"],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True, "description": "Dot-separated table/view path"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "schema.wiki": {
        "group": "schema",
        "command": "wiki",
        "description": "Get wiki documentation text and tags for a catalog entity.",
        "mechanism": "REST",
        "endpoints": ["GET /api/v3/catalog/by-path/{path}", "GET /api/v3/catalog/{id}/collaboration/wiki", "GET /api/v3/catalog/{id}/collaboration/tag"],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True, "description": "Dot-separated entity path"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "schema.sample": {
        "group": "schema",
        "command": "sample",
        "description": "Return sample rows from a table or view.",
        "mechanism": "SQL",
        "sql_template": "SELECT * FROM {path} LIMIT {limit}",
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True, "description": "Dot-separated table/view path"},
            {"name": "limit", "type": "integer", "required": False, "default": 10, "description": "Number of sample rows"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False, "description": "Comma-separated fields to include"},
        ],
    },
    "reflect.list": {
        "group": "reflect",
        "command": "list",
        "description": "List all reflections defined on a dataset.",
        "mechanism": "SQL",
        "sql_template": "SELECT * FROM sys.reflections WHERE dataset_id = '{dataset_id}'",
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True, "description": "Dot-separated dataset path"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "reflect.status": {
        "group": "reflect",
        "command": "status",
        "description": "Get detailed status of a reflection by ID.",
        "mechanism": "REST",
        "endpoints": ["GET /api/v3/reflection/{id}"],
        "parameters": [
            {"name": "reflection_id", "type": "string", "required": True, "positional": True, "description": "Reflection ID (get from 'drs reflect list')"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "reflect.refresh": {
        "group": "reflect",
        "command": "refresh",
        "description": "Trigger an immediate refresh of a reflection.",
        "mechanism": "REST",
        "endpoints": ["POST /api/v3/reflection/{id}/refresh"],
        "mutating": True,
        "parameters": [
            {"name": "reflection_id", "type": "string", "required": True, "positional": True, "description": "Reflection ID to refresh"},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False, "description": "Validate without executing"},
        ],
    },
    "reflect.drop": {
        "group": "reflect",
        "command": "drop",
        "description": "Permanently delete a reflection. Cannot be undone.",
        "mechanism": "REST",
        "endpoints": ["DELETE /api/v3/reflection/{id}"],
        "mutating": True,
        "parameters": [
            {"name": "reflection_id", "type": "string", "required": True, "positional": True, "description": "Reflection ID to delete"},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False, "description": "Validate without executing"},
        ],
    },
    "jobs.list": {
        "group": "jobs",
        "command": "list",
        "description": "List recent query jobs, optionally filtered by status.",
        "mechanism": "SQL",
        "sql_template": "SELECT ... FROM sys.project.jobs_recent WHERE ... ORDER BY start_time DESC LIMIT {limit}",
        "parameters": [
            {"name": "status", "type": "enum", "required": False, "enum": sorted(VALID_JOB_STATES), "description": "Filter by job state"},
            {"name": "limit", "type": "integer", "required": False, "default": 25, "description": "Max jobs to return"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False, "description": "Comma-separated fields to include"},
        ],
    },
    "jobs.get": {
        "group": "jobs",
        "command": "get",
        "description": "Get detailed status and metadata for a specific job.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects/{pid}/job/{id}"],
        "parameters": [
            {"name": "job_id", "type": "string", "required": True, "positional": True, "format": "uuid", "description": "Job ID (UUID)"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "jobs.profile": {
        "group": "jobs",
        "command": "profile",
        "description": "Get operator-level execution profile for a completed job.",
        "mechanism": "SQL",
        "sql_template": "SELECT * FROM sys.project.job_profiles WHERE job_id = '{job_id}'",
        "parameters": [
            {"name": "job_id", "type": "string", "required": True, "positional": True, "format": "uuid", "description": "Job ID (UUID)"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "access.grants": {
        "group": "access",
        "command": "grants",
        "description": "Get ACL grants on a catalog entity.",
        "mechanism": "REST",
        "endpoints": ["GET /api/v3/catalog/by-path/{path}"],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True, "description": "Dot-separated entity path"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "access.roles": {
        "group": "access",
        "command": "roles",
        "description": "List all roles in the organization.",
        "mechanism": "REST",
        "endpoints": ["GET /v1/roles"],
        "parameters": [
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "access.whoami": {
        "group": "access",
        "command": "whoami",
        "description": "Get current authenticated user info (best-effort).",
        "mechanism": "REST",
        "endpoints": ["GET /v1/users"],
        "parameters": [
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "access.audit": {
        "group": "access",
        "command": "audit",
        "description": "Audit a user's roles and effective permissions by username.",
        "mechanism": "REST",
        "endpoints": ["GET /v1/user/by-name/{name}"],
        "parameters": [
            {"name": "username", "type": "string", "required": True, "positional": True, "description": "Username to audit"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
}


def describe_command(command: str) -> dict | None:
    """Return the schema for a command, or None if not found."""
    return COMMAND_SCHEMAS.get(command)


def list_commands() -> list[str]:
    """Return all available command names."""
    return sorted(COMMAND_SCHEMAS.keys())
