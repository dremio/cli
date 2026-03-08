"""MCP stdio server — thin transport wrapper over drs commands.

Each tool calls the same async function the CLI uses. Zero duplicated logic.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from drs.auth import load_config
from drs.client import DremioClient
from drs.commands import query, catalog, schema, reflect, jobs, access
from drs.utils import filter_fields


ALL_SERVICES = {"query", "catalog", "schema", "reflect", "jobs", "access"}


def create_server(services: list[str] | None = None) -> FastMCP:
    allowed = set(services) if services else ALL_SERVICES
    mcp = FastMCP("Dremio", description="Dremio Cloud developer tools — query, catalog, schema, reflections, jobs, access")

    config = load_config()
    client = DremioClient(config)

    # -- Query tools --
    if "query" in allowed:
        @mcp.tool(
            name="dremio_query_run",
            description="Execute a SQL query against Dremio Cloud and return result rows as JSON. Submits the query, polls until completion, and fetches all results. Use for ad-hoc queries, data exploration, DDL statements, and analysis. Use 'fields' to limit output fields and reduce context window usage.",
        )
        async def tool_query_run(sql: str, context: str | None = None, fields: str | None = None) -> dict:
            ctx = context.split(".") if context else None
            result = await query.run_query(client, sql, context=ctx)
            if fields:
                result = filter_fields(result, [f.strip() for f in fields.split(",")])
            return result

        @mcp.tool(
            name="dremio_query_status",
            description="Check the status of a Dremio Cloud job by its UUID job ID. Returns job state (RUNNING, COMPLETED, FAILED, etc.), timing, and error details if failed.",
        )
        async def tool_query_status(job_id: str) -> dict:
            return await query.get_status(client, job_id)

        @mcp.tool(
            name="dremio_query_cancel",
            description="Cancel a running Dremio Cloud job by its UUID job ID. No effect if the job has already completed or failed.",
        )
        async def tool_query_cancel(job_id: str) -> dict:
            return await query.cancel(client, job_id)

    # -- Catalog tools --
    if "catalog" in allowed:
        @mcp.tool(
            name="dremio_catalog_list",
            description="List top-level Dremio Cloud catalog entities: data sources, spaces, and home folder. Use this to discover what's available before drilling into specific paths. Use 'fields' to limit output fields.",
        )
        async def tool_catalog_list(fields: str | None = None) -> dict:
            result = await catalog.list_catalog(client)
            if fields:
                result = filter_fields(result, [f.strip() for f in fields.split(",")])
            return result

        @mcp.tool(
            name="dremio_catalog_search",
            description="Full-text search for tables, views, and sources in the Dremio Cloud catalog by keyword. Returns matching entity names, paths, and types. Use when you know part of a name but not the full path.",
        )
        async def tool_catalog_search(term: str) -> dict:
            return await catalog.search_catalog(client, term)

        @mcp.tool(
            name="dremio_catalog_get",
            description="Get full metadata for a Dremio Cloud catalog entity by dot-separated path (e.g., 'myspace.folder.table'). Returns entity type, ID, children (for containers), fields (for datasets), and access control info. Use when you know the exact path. Use 'fields' to limit output fields.",
        )
        async def tool_catalog_get(path: str, fields: str | None = None) -> dict:
            result = await catalog.get_entity(client, path)
            if fields:
                result = filter_fields(result, [f.strip() for f in fields.split(",")])
            return result

    # -- Schema tools --
    if "schema" in allowed:
        @mcp.tool(
            name="dremio_schema_describe",
            description="Get column names, data types, and nullability for a Dremio Cloud table or view by dot-separated path. Essential for understanding data structure before writing queries. Use 'fields' to limit output (e.g., 'columns.name,columns.type').",
        )
        async def tool_schema_describe(path: str, fields: str | None = None) -> dict:
            result = await schema.describe(client, path)
            if fields:
                result = filter_fields(result, [f.strip() for f in fields.split(",")])
            return result

        @mcp.tool(
            name="dremio_schema_lineage",
            description="Get the upstream and downstream dependency graph for a table or view by dot-separated path. Shows which datasets feed into or depend on this entity.",
        )
        async def tool_schema_lineage(path: str) -> dict:
            return await schema.lineage(client, path)

        @mcp.tool(
            name="dremio_schema_wiki",
            description="Get wiki documentation text and tags for a catalog entity by dot-separated path. Returns human-written descriptions and classification tags, if any exist.",
        )
        async def tool_schema_wiki(path: str) -> dict:
            return await schema.wiki(client, path)

        @mcp.tool(
            name="dremio_schema_sample",
            description="Return sample rows from a table or view as JSON (default 10 rows, adjustable via limit). Quick way to preview actual data values, formats, and distributions. Use 'fields' to limit output fields.",
        )
        async def tool_schema_sample(path: str, limit: int = 10, fields: str | None = None) -> dict:
            result = await schema.sample(client, path, limit=limit)
            if fields:
                result = filter_fields(result, [f.strip() for f in fields.split(",")])
            return result

    # -- Reflection tools --
    if "reflect" in allowed:
        @mcp.tool(
            name="dremio_reflect_list",
            description="List all reflections (materialized views) defined on a dataset by dot-separated path. Shows reflection type (raw/aggregation), status, and configuration. Uses sys.reflections system table.",
        )
        async def tool_reflect_list(path: str) -> dict:
            return await reflect.list_reflections(client, path)

        @mcp.tool(
            name="dremio_reflect_status",
            description="Get detailed status of a single reflection by its ID. Includes freshness, staleness, size, last refresh time, and full configuration. Get reflection IDs from dremio_reflect_list.",
        )
        async def tool_reflect_status(reflection_id: str) -> dict:
            return await reflect.status(client, reflection_id)

        @mcp.tool(
            name="dremio_reflect_refresh",
            description="Trigger an immediate refresh of a reflection by its ID. The refresh runs asynchronously — use dremio_reflect_status to monitor progress.",
        )
        async def tool_reflect_refresh(reflection_id: str) -> dict:
            return await reflect.refresh(client, reflection_id)

        @mcp.tool(
            name="dremio_reflect_drop",
            description="Permanently delete a reflection by its ID. This cannot be undone — the materialized data is removed.",
        )
        async def tool_reflect_drop(reflection_id: str) -> dict:
            return await reflect.drop(client, reflection_id)

    # -- Jobs tools --
    if "jobs" in allowed:
        @mcp.tool(
            name="dremio_jobs_list",
            description="List recent Dremio Cloud query jobs, optionally filtered by status (COMPLETED, FAILED, RUNNING, CANCELED, PLANNING, ENQUEUED). Shows job ID, user, query type, state, and timing. Default limit 25. Use 'fields' to limit output fields.",
        )
        async def tool_jobs_list(status_filter: str | None = None, limit: int = 25, fields: str | None = None) -> dict:
            result = await jobs.list_jobs(client, status_filter=status_filter, limit=limit)
            if fields:
                result = filter_fields(result, [f.strip() for f in fields.split(",")])
            return result

        @mcp.tool(
            name="dremio_jobs_get",
            description="Get detailed status and metadata for a specific Dremio Cloud job by its UUID job ID. Returns more detail than the jobs list, including query text and resource usage.",
        )
        async def tool_jobs_get(job_id: str) -> dict:
            return await jobs.get_job(client, job_id)

        @mcp.tool(
            name="dremio_jobs_profile",
            description="Get operator-level execution profile for a completed Dremio Cloud job. Shows query plan phases, row counts, and timing per operator. Essential for diagnosing slow queries.",
        )
        async def tool_jobs_profile(job_id: str) -> dict:
            return await jobs.profile(client, job_id)

    # -- Access tools --
    if "access" in allowed:
        @mcp.tool(
            name="dremio_access_grants",
            description="Get ACL grants on a Dremio Cloud catalog entity (table, view, source, space) by dot-separated path. Shows which users and roles have what permissions.",
        )
        async def tool_access_grants(path: str) -> dict:
            return await access.grants(client, path)

        @mcp.tool(
            name="dremio_access_roles",
            description="List all roles defined in the Dremio Cloud organization. Returns role names and IDs.",
        )
        async def tool_access_roles() -> dict:
            return await access.roles(client)

        @mcp.tool(
            name="dremio_access_whoami",
            description="Get information about the currently authenticated Dremio Cloud user (best-effort — use dremio_access_audit with a known username for reliable lookups).",
        )
        async def tool_access_whoami() -> dict:
            return await access.whoami(client)

        @mcp.tool(
            name="dremio_access_audit",
            description="Audit a Dremio Cloud user's effective permissions by username. Looks up the user, then lists all roles they belong to. Use this to understand what a specific user can access.",
        )
        async def tool_access_audit(username: str) -> dict:
            return await access.audit(client, username)

    return mcp
