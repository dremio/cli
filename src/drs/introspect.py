#
# Copyright (C) 2017-2026 Dremio Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Runtime schema introspection for dremio commands.

Provides machine-readable descriptions of command parameters, types, and
constraints. Agents call `dremio describe <command>` to self-serve schema info.
"""

from __future__ import annotations

from drs.utils import VALID_JOB_STATES

COMMAND_SCHEMAS: dict[str, dict] = {
    # -- Query --
    "query.run": {
        "group": "query",
        "command": "run",
        "description": "Execute a SQL query against Dremio Cloud, wait for completion, return results.",
        "mechanism": "REST",
        "endpoints": [
            "POST /v0/projects/{pid}/sql",
            "GET /v0/projects/{pid}/job/{id}",
            "GET /v0/projects/{pid}/job/{id}/results",
        ],
        "parameters": [
            {
                "name": "sql",
                "type": "string",
                "required": True,
                "positional": True,
                "description": "SQL query to execute",
            },
            {
                "name": "context",
                "type": "string",
                "required": False,
                "description": "Dot-separated default schema context",
            },
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {
                "name": "fields",
                "type": "string",
                "required": False,
                "flag": "--fields/-f",
                "description": "Comma-separated fields to include",
            },
        ],
    },
    "query.status": {
        "group": "query",
        "command": "status",
        "description": "Check the status of a Dremio job by UUID.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects/{pid}/job/{id}"],
        "parameters": [
            {"name": "job_id", "type": "string", "required": True, "positional": True, "format": "uuid"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "query.cancel": {
        "group": "query",
        "command": "cancel",
        "description": "Cancel a running Dremio job.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["POST /v0/projects/{pid}/job/{id}/cancel"],
        "parameters": [
            {"name": "job_id", "type": "string", "required": True, "positional": True, "format": "uuid"},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False, "flag": "--dry-run"},
        ],
    },
    # -- Folder --
    "folder.list": {
        "group": "folder",
        "command": "list",
        "description": "List top-level catalog entities: sources, spaces, and home folder.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects/{pid}/catalog"],
        "parameters": [
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False},
        ],
    },
    "folder.get": {
        "group": "folder",
        "command": "get",
        "description": "Get full metadata for a catalog entity by dot-separated path.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects/{pid}/catalog/by-path/{path}"],
        "parameters": [
            {
                "name": "path",
                "type": "string",
                "required": True,
                "positional": True,
                "description": "Dot-separated entity path",
            },
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False},
        ],
    },
    "folder.create": {
        "group": "folder",
        "command": "create",
        "description": "Create a space (single name) or folder (nested path) using SQL.",
        "mechanism": "SQL",
        "mutating": True,
        "sql_template": 'CREATE SPACE "{name}" / CREATE FOLDER {path}',
        "parameters": [
            {
                "name": "path",
                "type": "string",
                "required": True,
                "positional": True,
                "description": "Space name or dot-separated folder path",
            },
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "folder.delete": {
        "group": "folder",
        "command": "delete",
        "description": "Delete a catalog entity (space, folder, view, etc.). Cannot be undone.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["GET /v0/projects/{pid}/catalog/by-path/{path}", "DELETE /v0/projects/{pid}/catalog/{id}"],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "folder.grants": {
        "group": "folder",
        "command": "grants",
        "description": "Show ACL grants on a catalog entity.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects/{pid}/catalog/by-path/{path}"],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    # -- Search (top-level) --
    "search": {
        "group": None,
        "command": "search",
        "description": "Full-text search for tables, views, and sources by keyword.",
        "mechanism": "REST",
        "endpoints": ["POST /v0/projects/{pid}/search"],
        "parameters": [
            {"name": "term", "type": "string", "required": True, "positional": True, "description": "Search term"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    # -- Schema --
    "schema.describe": {
        "group": "schema",
        "command": "describe",
        "description": "Get column names, data types, and nullability for a table or view.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects/{pid}/catalog/by-path/{path}"],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False},
        ],
    },
    "schema.lineage": {
        "group": "schema",
        "command": "lineage",
        "description": "Get upstream and downstream dependency graph for a table or view.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects/{pid}/catalog/by-path/{path}", "GET /v0/projects/{pid}/catalog/{id}/graph"],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True},
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
            {"name": "path", "type": "string", "required": True, "positional": True},
            {"name": "limit", "type": "integer", "required": False, "default": 10},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False},
        ],
    },
    # -- Wiki --
    "wiki.get": {
        "group": "wiki",
        "command": "get",
        "description": "Get wiki description for a catalog entity.",
        "mechanism": "REST",
        "endpoints": [
            "GET /v0/projects/{pid}/catalog/by-path/{path}",
            "GET /v0/projects/{pid}/catalog/{id}/collaboration/wiki",
        ],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "wiki.update": {
        "group": "wiki",
        "command": "update",
        "description": "Set or update the wiki description for a catalog entity.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": [
            "GET /v0/projects/{pid}/catalog/by-path/{path}",
            "POST /v0/projects/{pid}/catalog/{id}/collaboration/wiki",
        ],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True},
            {
                "name": "text",
                "type": "string",
                "required": True,
                "positional": True,
                "description": "Wiki text (Markdown supported)",
            },
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    # -- Tag --
    "tag.get": {
        "group": "tag",
        "command": "get",
        "description": "Get tags for a catalog entity.",
        "mechanism": "REST",
        "endpoints": [
            "GET /v0/projects/{pid}/catalog/by-path/{path}",
            "GET /v0/projects/{pid}/catalog/{id}/collaboration/tag",
        ],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "tag.update": {
        "group": "tag",
        "command": "update",
        "description": "Set tags on a catalog entity. Replaces all existing tags.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": [
            "GET /v0/projects/{pid}/catalog/by-path/{path}",
            "POST /v0/projects/{pid}/catalog/{id}/collaboration/tag",
        ],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True},
            {
                "name": "tags",
                "type": "string",
                "required": True,
                "positional": True,
                "description": "Comma-separated tags",
            },
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    # -- Reflection --
    "reflection.create": {
        "group": "reflection",
        "command": "create",
        "description": "Create a new reflection on a dataset.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["GET /v0/projects/{pid}/catalog/by-path/{path}", "POST /v0/projects/{pid}/reflection"],
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True},
            {
                "name": "type",
                "type": "enum",
                "required": False,
                "default": "raw",
                "enum": ["raw", "aggregation"],
                "flag": "--type/-t",
            },
            {"name": "fields", "type": "string", "required": False, "flag": "--fields/-f"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "reflection.list": {
        "group": "reflection",
        "command": "list",
        "description": "List all reflections defined on a dataset.",
        "mechanism": "SQL",
        "sql_template": "SELECT * FROM sys.project.reflections WHERE dataset_id = '{dataset_id}'",
        "parameters": [
            {"name": "path", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "reflection.get": {
        "group": "reflection",
        "command": "get",
        "description": "Get detailed status of a reflection by ID.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects/{pid}/reflection/{id}"],
        "parameters": [
            {"name": "reflection_id", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "reflection.refresh": {
        "group": "reflection",
        "command": "refresh",
        "description": "Trigger an immediate refresh of a reflection.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["POST /v0/projects/{pid}/reflection/{id}/refresh"],
        "parameters": [
            {"name": "reflection_id", "type": "string", "required": True, "positional": True},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False},
        ],
    },
    "reflection.delete": {
        "group": "reflection",
        "command": "delete",
        "description": "Permanently delete a reflection. Cannot be undone.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["DELETE /v0/projects/{pid}/reflection/{id}"],
        "parameters": [
            {"name": "reflection_id", "type": "string", "required": True, "positional": True},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False},
        ],
    },
    # -- Job --
    "job.list": {
        "group": "job",
        "command": "list",
        "description": "List recent query jobs, optionally filtered by status.",
        "mechanism": "SQL",
        "sql_template": "SELECT ... FROM sys.project.jobs WHERE ... ORDER BY submitted_ts DESC LIMIT {limit}",
        "parameters": [
            {"name": "status", "type": "enum", "required": False, "enum": sorted(VALID_JOB_STATES)},
            {"name": "limit", "type": "integer", "required": False, "default": 25},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False},
        ],
    },
    "job.get": {
        "group": "job",
        "command": "get",
        "description": "Get detailed status and metadata for a specific job.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects/{pid}/job/{id}"],
        "parameters": [
            {"name": "job_id", "type": "string", "required": True, "positional": True, "format": "uuid"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False, "flag": "--fields/-f"},
        ],
    },
    "job.profile": {
        "group": "job",
        "command": "profile",
        "description": "Get execution profile for a completed job.",
        "mechanism": "SQL",
        "sql_template": "SELECT ... FROM sys.project.jobs WHERE job_id = '{job_id}'",
        "parameters": [
            {"name": "job_id", "type": "string", "required": True, "positional": True, "format": "uuid"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    # -- Engine --
    "engine.list": {
        "group": "engine",
        "command": "list",
        "description": "List all engines in the project.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects/{pid}/engines"],
        "parameters": [
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False},
        ],
    },
    "engine.get": {
        "group": "engine",
        "command": "get",
        "description": "Get details for a specific engine.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects/{pid}/engines/{id}"],
        "parameters": [
            {"name": "engine_id", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False},
        ],
    },
    "engine.create": {
        "group": "engine",
        "command": "create",
        "description": "Create a new engine.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["POST /v0/projects/{pid}/engines"],
        "parameters": [
            {"name": "name", "type": "string", "required": True, "positional": True},
            {
                "name": "size",
                "type": "enum",
                "required": False,
                "default": "SMALL",
                "enum": ["SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE"],
            },
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "engine.update": {
        "group": "engine",
        "command": "update",
        "description": "Update engine configuration (name, size).",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["GET /v0/projects/{pid}/engines/{id}", "PUT /v0/projects/{pid}/engines/{id}"],
        "parameters": [
            {"name": "engine_id", "type": "string", "required": True, "positional": True},
            {"name": "name", "type": "string", "required": False, "flag": "--name"},
            {"name": "size", "type": "string", "required": False, "flag": "--size/-s"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "engine.delete": {
        "group": "engine",
        "command": "delete",
        "description": "Delete an engine. Cannot be undone.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["DELETE /v0/projects/{pid}/engines/{id}"],
        "parameters": [
            {"name": "engine_id", "type": "string", "required": True, "positional": True},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "engine.enable": {
        "group": "engine",
        "command": "enable",
        "description": "Enable a disabled engine.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["PUT /v0/projects/{pid}/engines/{id}/enable"],
        "parameters": [
            {"name": "engine_id", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "engine.disable": {
        "group": "engine",
        "command": "disable",
        "description": "Disable a running engine.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["PUT /v0/projects/{pid}/engines/{id}/disable"],
        "parameters": [
            {"name": "engine_id", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    # -- User --
    "user.list": {
        "group": "user",
        "command": "list",
        "description": "List all users in the organization.",
        "mechanism": "REST",
        "endpoints": ["GET /v1/users"],
        "parameters": [
            {"name": "limit", "type": "integer", "required": False, "default": 100, "flag": "--limit/-n"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False},
        ],
    },
    "user.get": {
        "group": "user",
        "command": "get",
        "description": "Get user details by name or ID.",
        "mechanism": "REST",
        "endpoints": ["GET /v1/users/name/{userName}", "GET /v1/users/{userId}"],
        "parameters": [
            {"name": "identifier", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False},
        ],
    },
    "user.create": {
        "group": "user",
        "command": "create",
        "description": "Create (invite) a new user by email address.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["POST /v1/users/invite"],
        "parameters": [
            {"name": "email", "type": "string", "required": True, "positional": True},
            {"name": "role_id", "type": "string", "required": False, "flag": "--role-id"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "user.update": {
        "group": "user",
        "command": "update",
        "description": "Update a user's properties.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["GET /v1/users/{userId}", "PUT /v1/users/{userId}"],
        "parameters": [
            {"name": "user_id", "type": "string", "required": True, "positional": True},
            {"name": "name", "type": "string", "required": False, "flag": "--name"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "user.delete": {
        "group": "user",
        "command": "delete",
        "description": "Delete a user from the organization. Cannot be undone.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["DELETE /v1/users/{userId}"],
        "parameters": [
            {"name": "user_id", "type": "string", "required": True, "positional": True},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "user.whoami": {
        "group": "user",
        "command": "whoami",
        "description": "Show current authenticated user info (best-effort).",
        "mechanism": "REST",
        "endpoints": ["GET /v1/users"],
        "parameters": [
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "user.audit": {
        "group": "user",
        "command": "audit",
        "description": "Audit a user's roles and effective permissions by username.",
        "mechanism": "REST",
        "endpoints": ["GET /v1/users/name/{userName}"],
        "parameters": [
            {"name": "username", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    # -- Role --
    "role.list": {
        "group": "role",
        "command": "list",
        "description": "List all roles in the organization.",
        "mechanism": "REST",
        "endpoints": ["GET /v1/roles"],
        "parameters": [
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False},
        ],
    },
    "role.get": {
        "group": "role",
        "command": "get",
        "description": "Get role details by name or ID.",
        "mechanism": "REST",
        "endpoints": ["GET /v1/roles/name/{roleName}", "GET /v1/roles/{roleId}"],
        "parameters": [
            {"name": "identifier", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False},
        ],
    },
    "role.create": {
        "group": "role",
        "command": "create",
        "description": "Create a new role.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["POST /v1/roles"],
        "parameters": [
            {"name": "name", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "role.update": {
        "group": "role",
        "command": "update",
        "description": "Update a role's name.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["GET /v1/roles/{roleId}", "PUT /v1/roles/{roleId}"],
        "parameters": [
            {"name": "role_id", "type": "string", "required": True, "positional": True},
            {"name": "name", "type": "string", "required": True, "flag": "--name"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "role.delete": {
        "group": "role",
        "command": "delete",
        "description": "Delete a role. Cannot be undone.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["DELETE /v1/roles/{roleId}"],
        "parameters": [
            {"name": "role_id", "type": "string", "required": True, "positional": True},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    # -- Project --
    "project.list": {
        "group": "project",
        "command": "list",
        "description": "List all projects in the organization.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects"],
        "parameters": [
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False},
        ],
    },
    "project.get": {
        "group": "project",
        "command": "get",
        "description": "Get details for a specific project.",
        "mechanism": "REST",
        "endpoints": ["GET /v0/projects/{id}"],
        "parameters": [
            {"name": "project_id", "type": "string", "required": True, "positional": True, "format": "uuid"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
            {"name": "fields", "type": "string", "required": False},
        ],
    },
    "project.create": {
        "group": "project",
        "command": "create",
        "description": "Create a new project. Automatically provisions a default 2XS engine.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["POST /v0/projects"],
        "parameters": [
            {"name": "name", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "project.update": {
        "group": "project",
        "command": "update",
        "description": "Update project attributes (e.g., name).",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["GET /v0/projects/{id}", "PUT /v0/projects/{id}"],
        "parameters": [
            {"name": "project_id", "type": "string", "required": True, "positional": True, "format": "uuid"},
            {"name": "name", "type": "string", "required": False, "flag": "--name"},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "project.delete": {
        "group": "project",
        "command": "delete",
        "description": "Delete a project. Cannot delete the sole project in an organization.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["DELETE /v0/projects/{id}"],
        "parameters": [
            {"name": "project_id", "type": "string", "required": True, "positional": True, "format": "uuid"},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    # -- Grant --
    "grant.get": {
        "group": "grant",
        "command": "get",
        "description": "Get grants for a user or role on a resource.",
        "mechanism": "REST",
        "endpoints": ["GET /v1/{scope}/{scopeId}/grants/{granteeType}/{granteeId}"],
        "parameters": [
            {
                "name": "scope",
                "type": "string",
                "required": True,
                "positional": True,
                "description": "Resource scope (projects, orgs, clouds)",
            },
            {"name": "scope_id", "type": "string", "required": True, "positional": True},
            {
                "name": "grantee_type",
                "type": "string",
                "required": True,
                "positional": True,
                "description": "user or role",
            },
            {"name": "grantee_id", "type": "string", "required": True, "positional": True},
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "grant.update": {
        "group": "grant",
        "command": "update",
        "description": "Set grants (privileges) for a user or role on a resource.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["PUT /v1/{scope}/{scopeId}/grants/{granteeType}/{granteeId}"],
        "parameters": [
            {"name": "scope", "type": "string", "required": True, "positional": True},
            {"name": "scope_id", "type": "string", "required": True, "positional": True},
            {"name": "grantee_type", "type": "string", "required": True, "positional": True},
            {"name": "grantee_id", "type": "string", "required": True, "positional": True},
            {
                "name": "privileges",
                "type": "string",
                "required": True,
                "positional": True,
                "description": "Comma-separated privileges",
            },
            {"name": "output", "type": "enum", "required": False, "default": "json", "enum": ["json", "csv", "pretty"]},
        ],
    },
    "grant.delete": {
        "group": "grant",
        "command": "delete",
        "description": "Remove all grants for a user or role on a resource.",
        "mechanism": "REST",
        "mutating": True,
        "endpoints": ["DELETE /v1/{scope}/{scopeId}/grants/{granteeType}/{granteeId}"],
        "parameters": [
            {"name": "scope", "type": "string", "required": True, "positional": True},
            {"name": "scope_id", "type": "string", "required": True, "positional": True},
            {"name": "grantee_type", "type": "string", "required": True, "positional": True},
            {"name": "grantee_id", "type": "string", "required": True, "positional": True},
            {"name": "dry_run", "type": "boolean", "required": False, "default": False},
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
