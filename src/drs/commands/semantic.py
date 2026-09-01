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
"""dremio semantic — semantic layer operations and ingestion helpers."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import typer
import yaml

from drs.client import DremioClient
from drs.output import OutputFormat, error, output
from drs.utils import handle_api_error, parse_path

app = typer.Typer(
    help="Manage semantic layer lifecycle, entities, and ingestion helpers.",
    context_settings={"help_option_names": ["-h", "--help"]},
)
entity_app = typer.Typer(
    help="Manage semantic layer entities, including wiki/glossary imports.",
    context_settings={"help_option_names": ["-h", "--help"]},
)
task_app = typer.Typer(
    help="Manage semantic layer ingestion tasks.",
    context_settings={"help_option_names": ["-h", "--help"]},
)

app.add_typer(task_app, name="task")
app.add_typer(entity_app, name="entity")

VALID_ENTITY_TYPES = {"TABLE", "METRIC"}
VALID_AUTOPILOT_MODES = {"OFF", "OBSERVE", "ASSIST", "AUTOMATE", "CUSTOM"}
VALID_KEY_TYPES = {"PRIMARY", "FOREIGN", "NONE"}
MAX_INITIALIZE_ENTITIES = 10


def _get_client() -> DremioClient:
    from drs.cli import get_client

    return get_client()


def _run_command(coro, client, fmt: OutputFormat = OutputFormat.json) -> None:
    async def _execute():
        try:
            return await coro
        finally:
            await client.close()

    try:
        result = asyncio.run(_execute())
    except Exception as exc:
        from drs.utils import DremioAPIError

        if isinstance(exc, DremioAPIError | ValueError):
            error(str(exc))
            raise typer.Exit(1)
        if isinstance(exc, httpx.HTTPError):
            message = str(exc)
            if isinstance(exc, httpx.ConnectError):
                message = f"Failed to connect to Dremio API at {client.config.uri}: {exc}"
            error(message)
            raise typer.Exit(1)
        raise
    output(result, fmt)


def _read_text_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"Failed to read file '{path}': {exc}") from exc


def _load_structured_file(path: Path) -> Any:
    raw = _read_text_file(path)
    try:
        if path.suffix.lower() == ".json":
            return json.loads(raw)
        return yaml.safe_load(raw)
    except Exception as exc:
        raise ValueError(f"Failed to parse structured file '{path}': {exc}") from exc


def _load_dictionary_text(dictionary_file: Path | None, dictionary_text: str | None) -> str | None:
    if dictionary_file and dictionary_text:
        raise ValueError("Use either --dictionary-file or --dictionary-text, not both.")
    if dictionary_file:
        return _read_text_file(dictionary_file)
    return dictionary_text


def _extract_path_string(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return ".".join(str(part) for part in value)
    if isinstance(value, dict):
        if "components" in value and isinstance(value["components"], list):
            return ".".join(str(part) for part in value["components"])
        if "path" in value:
            return _extract_path_string(value["path"])
    raise ValueError(f"Unsupported path entry: {value!r}")


def _load_paths(paths: list[str] | None, paths_file: Path | None) -> list[str]:
    cli_paths = [path for path in (paths or []) if path.strip()]
    if paths_file is None:
        if not cli_paths:
            raise ValueError("Provide at least one dataset path or use --paths-file.")
        return cli_paths
    if cli_paths:
        raise ValueError("Use either positional paths or --paths-file, not both.")

    if paths_file.suffix.lower() in {".json", ".yaml", ".yml"}:
        payload = _load_structured_file(paths_file)
        if isinstance(payload, dict):
            payload = payload.get("paths") or payload.get("entities")
        if not isinstance(payload, list):
            raise ValueError("Structured paths file must contain a list or an object with 'paths' or 'entities'.")
        resolved = [_extract_path_string(entry) for entry in payload]
    else:
        resolved = [
            line.strip()
            for line in _read_text_file(paths_file).splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]

    cleaned = [path for path in resolved if path]
    if not cleaned:
        raise ValueError(f"No dataset paths found in '{paths_file}'.")
    return cleaned


def _validate_entity_count(paths: list[str]) -> None:
    if len(paths) > MAX_INITIALIZE_ENTITIES:
        raise ValueError(
            f"Semantic layer initialize/task add accepts at most {MAX_INITIALIZE_ENTITIES} entities per request; got {len(paths)}."
        )


def _chunk_paths(paths: list[str], chunk_size: int = MAX_INITIALIZE_ENTITIES) -> list[list[str]]:
    return [paths[index : index + chunk_size] for index in range(0, len(paths), chunk_size)]


def _load_task_ids(task_ids: list[str] | None, task_ids_file: Path | None) -> list[str]:
    cli_task_ids = [task_id.strip() for task_id in (task_ids or []) if task_id.strip()]
    if task_ids_file is None:
        if not cli_task_ids:
            raise ValueError("Provide at least one task ID or use --task-ids-file.")
        return cli_task_ids
    if cli_task_ids:
        raise ValueError("Use either positional task IDs or --task-ids-file, not both.")

    if task_ids_file.suffix.lower() in {".json", ".yaml", ".yml"}:
        payload = _load_structured_file(task_ids_file)
        if isinstance(payload, dict):
            if isinstance(payload.get("results"), list):
                resolved = [
                    str(item.get("task", {}).get("taskId") or "").strip()
                    for item in payload["results"]
                    if isinstance(item, dict)
                ]
            else:
                payload = payload.get("taskIds") or payload.get("tasks")
                if not isinstance(payload, list):
                    raise ValueError("Structured task ID file must contain 'results', 'taskIds', or 'tasks'.")
                resolved = [str(item).strip() for item in payload]
        elif isinstance(payload, list):
            resolved = [str(item).strip() for item in payload]
        else:
            raise ValueError("Structured task ID file must contain a list or supported object shape.")
    else:
        resolved = [
            line.strip()
            for line in _read_text_file(task_ids_file).splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]

    cleaned = [task_id for task_id in resolved if task_id]
    if not cleaned:
        raise ValueError(f"No task IDs found in '{task_ids_file}'.")
    return cleaned


def _normalize_entity_type(entity_type: str) -> str:
    normalized = entity_type.upper()
    if normalized not in VALID_ENTITY_TYPES:
        raise ValueError(f"Invalid entity type '{entity_type}'. Valid values: {', '.join(sorted(VALID_ENTITY_TYPES))}")
    return normalized


def _normalize_autopilot_mode(mode: str) -> str:
    normalized = mode.upper()
    if normalized not in VALID_AUTOPILOT_MODES:
        raise ValueError(f"Invalid autopilot mode '{mode}'. Valid values: {', '.join(sorted(VALID_AUTOPILOT_MODES))}")
    return normalized


def _entity_reference_from_path(path: str) -> dict[str, Any]:
    return {"type": "TABLE", "path": parse_path(path)}


def _coerce_attribute(entry: dict[str, Any]) -> dict[str, str]:
    source_column = str(entry.get("sourceColumn") or entry.get("column") or entry.get("name") or "").strip()
    if not source_column:
        raise ValueError("Each glossary attribute must include sourceColumn, column, or name.")

    alias = str(entry.get("alias") or entry.get("displayName") or source_column).strip()
    description = str(entry.get("description") or entry.get("wiki") or "").strip()
    key_type = str(entry.get("keyType") or "NONE").upper()
    if key_type not in VALID_KEY_TYPES:
        raise ValueError(f"Invalid keyType '{key_type}' for column '{source_column}'.")

    return {
        "sourceColumn": source_column,
        "alias": alias,
        "description": description,
        "keyType": key_type,
    }


def _normalize_glossary_entries(payload: Any) -> list[dict[str, str]]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        payload = payload.get("attributes") or payload.get("columns") or payload.get("glossary")
    if not isinstance(payload, list):
        raise ValueError("Glossary input must be a list or an object with attributes/columns/glossary.")
    return [_coerce_attribute(item) for item in payload]


def _field_name(field: dict[str, Any]) -> str:
    return str(field.get("name") or field.get("fieldName") or field.get("sourceColumn") or "").strip()


def _format_entity_path(path: list[str] | None) -> str:
    return ".".join(path or [])


def _flatten_semantic_entity(entity: dict[str, Any]) -> dict[str, Any]:
    attributes = entity.get("attributes") or []
    relationships = entity.get("relationships") or []
    return {
        "id": entity.get("id", ""),
        "type": entity.get("type", ""),
        "name": entity.get("name", ""),
        "path": _format_entity_path(entity.get("path")),
        "description": entity.get("description", "") or "",
        "confidenceScore": entity.get("confidenceScore", ""),
        "attributeCount": len(attributes),
        "relationshipCount": len(relationships),
        "relatedMetricCount": entity.get("relatedMetricCount", ""),
    }


def _build_default_attributes(catalog_entity: dict[str, Any]) -> list[dict[str, str]]:
    fields = catalog_entity.get("fields") or []
    attributes: list[dict[str, str]] = []
    for field in fields:
        name = _field_name(field)
        if not name:
            continue
        attributes.append(
            {
                "sourceColumn": name,
                "alias": name,
                "description": "",
                "keyType": "NONE",
            }
        )
    if not attributes:
        raise ValueError("Dataset schema is unavailable. Provide a glossary file with column definitions.")
    return attributes


def _merge_attributes(
    catalog_entity: dict[str, Any], glossary_entries: list[dict[str, str]] | None = None
) -> list[dict[str, str]]:
    merged = {entry["sourceColumn"]: entry for entry in _build_default_attributes(catalog_entity)}
    for entry in glossary_entries or []:
        merged[entry["sourceColumn"]] = entry
    return list(merged.values())


async def _get_catalog_entity_by_path(client: DremioClient, path: str) -> dict[str, Any]:
    try:
        return await client.get_catalog_by_path(parse_path(path))
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def _resolve_wiki_text(client: DremioClient, entity_id: str) -> str:
    try:
        wiki = await client.get_wiki(entity_id)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return ""
        raise handle_api_error(exc) from exc
    return str(wiki.get("text") or "").strip()


async def _build_table_payload(
    client: DremioClient,
    path: str,
    *,
    name: str | None,
    description: str | None,
    wiki_from_catalog: bool,
    glossary_file: Path | None,
    glossary_inline: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    catalog_entity = await _get_catalog_entity_by_path(client, path)
    glossary_entries = glossary_inline
    if glossary_file:
        glossary_entries = _normalize_glossary_entries(_load_structured_file(glossary_file))
    resolved_description = (description or "").strip()
    if wiki_from_catalog and not resolved_description:
        resolved_description = await _resolve_wiki_text(client, catalog_entity["id"])

    payload = {
        "type": "TABLE",
        "path": parse_path(path),
        "name": name or parse_path(path)[-1],
        "description": resolved_description,
        "attributes": _merge_attributes(catalog_entity, glossary_entries),
    }
    return payload


async def initialize_semantic_layer(
    client: DremioClient,
    paths: list[str],
    *,
    dictionary_file: Path | None,
    dictionary_text: str | None,
    scoped_dictionaries_file: Path | None,
    include_query_history: bool,
    autopilot_mode: str | None,
    max_jobs_to_process: int | None,
) -> dict[str, Any]:
    _validate_entity_count(paths)
    body: dict[str, Any] = {
        "entities": [_entity_reference_from_path(path) for path in paths],
        "includeQueryHistory": include_query_history,
    }
    if autopilot_mode:
        body["autopilotMode"] = _normalize_autopilot_mode(autopilot_mode)
    if max_jobs_to_process is not None:
        body["maxJobsToProcess"] = max_jobs_to_process

    dictionary = _load_dictionary_text(dictionary_file, dictionary_text)
    if dictionary and scoped_dictionaries_file:
        raise ValueError("Use either a global dictionary input or --scoped-dictionaries-file, not both.")
    if dictionary:
        body["dataDictionaries"] = [{"dataDictionary": dictionary}]
    elif scoped_dictionaries_file:
        scoped = _load_structured_file(scoped_dictionaries_file)
        if not isinstance(scoped, list):
            raise ValueError("Scoped dictionaries file must contain a list of scoped dictionaries.")
        body["dataDictionaries"] = scoped

    try:
        return await client.initialize_semantic_layer(body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def add_semantic_layer_task(
    client: DremioClient,
    paths: list[str],
    *,
    dictionary_file: Path | None,
    dictionary_text: str | None,
    include_query_history: bool,
    max_jobs_to_process: int | None,
) -> dict[str, Any]:
    _validate_entity_count(paths)
    body: dict[str, Any] = {"entities": [_entity_reference_from_path(path) for path in paths]}
    dictionary = _load_dictionary_text(dictionary_file, dictionary_text)
    if dictionary:
        body["dataDictionary"] = dictionary
    if include_query_history:
        body["includeQueryHistory"] = True
    if max_jobs_to_process is not None:
        body["maxJobsToProcess"] = max_jobs_to_process

    try:
        return await client.add_semantic_layer_task(body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def add_semantic_layer_task_batched(
    client: DremioClient,
    paths: list[str],
    *,
    dictionary_file: Path | None,
    dictionary_text: str | None,
    include_query_history: bool,
    max_jobs_to_process: int | None,
) -> dict[str, Any]:
    dictionary = _load_dictionary_text(dictionary_file, dictionary_text)
    chunks = _chunk_paths(paths)
    results = []
    for index, chunk in enumerate(chunks, start=1):
        result = await add_semantic_layer_task(
            client,
            chunk,
            dictionary_file=None,
            dictionary_text=dictionary,
            include_query_history=include_query_history,
            max_jobs_to_process=max_jobs_to_process,
        )
        results.append(
            {
                "batch": index,
                "entitiesSubmitted": len(chunk),
                "paths": chunk,
                "task": result,
            }
        )
    return {
        "batches": len(chunks),
        "totalEntities": len(paths),
        "results": results,
    }


async def _run_task_action_batched(
    client: DremioClient,
    task_ids: list[str],
    *,
    action_name: str,
    action_coro,
) -> dict[str, Any]:
    results = []
    for task_id in task_ids:
        result = await action_coro(task_id)
        results.append({"taskId": task_id, "result": result})
    return {"action": action_name, "count": len(task_ids), "results": results}


async def deploy_semantic_layer_tasks_batched(client: DremioClient, task_ids: list[str]) -> dict[str, Any]:
    return await _run_task_action_batched(
        client,
        task_ids,
        action_name="deploy",
        action_coro=client.deploy_semantic_layer_entities,
    )


async def abandon_semantic_layer_tasks_batched(client: DremioClient, task_ids: list[str]) -> dict[str, Any]:
    return await _run_task_action_batched(
        client,
        task_ids,
        action_name="abandon",
        action_coro=client.abandon_semantic_layer_entities,
    )


async def list_semantic_entities(
    client: DremioClient,
    entity_type: str,
    *,
    task_id: str | None,
    page_token: str | None,
    limit: int,
    fetch_all: bool,
    fmt: OutputFormat,
) -> dict[str, Any] | list[dict[str, Any]]:
    entities: list[dict[str, Any]] = []
    current_page_token = page_token
    total_count: int | None = None
    next_page_token: str | None = None

    while True:
        response = await client.list_semantic_layer_entities(
            entity_type,
            task_id=task_id,
            page_token=current_page_token,
            limit=limit,
        )
        page_entities = response.get("entities", [])
        entities.extend(page_entities)
        total_count = response.get("totalCount", total_count)
        next_page_token = response.get("nextPageToken")
        if not fetch_all or not next_page_token:
            break
        current_page_token = next_page_token

    if fmt in {OutputFormat.pretty, OutputFormat.csv}:
        return [_flatten_semantic_entity(entity) for entity in entities]

    result: dict[str, Any] = {
        "entities": entities,
        "entityType": entity_type,
        "count": len(entities),
    }
    if total_count is not None:
        result["totalCount"] = total_count
    if task_id:
        result["taskId"] = task_id
    if next_page_token and not fetch_all:
        result["nextPageToken"] = next_page_token
    if fetch_all:
        result["allPagesFetched"] = True
    return result


async def upsert_table_entity(
    client: DremioClient,
    path: str,
    *,
    name: str | None,
    description: str | None,
    wiki_from_catalog: bool,
    glossary_file: Path | None,
    task_id: str | None,
) -> dict[str, Any]:
    payload = await _build_table_payload(
        client,
        path,
        name=name,
        description=description,
        wiki_from_catalog=wiki_from_catalog,
        glossary_file=glossary_file,
    )
    try:
        result = await client.update_semantic_layer_entity(path, payload, task_id=task_id)
        action = "updated"
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != 404:
            raise handle_api_error(exc) from exc
        result = await client.add_semantic_layer_entity(payload, task_id=task_id)
        action = "created"
    return {"action": action, "path": path, "entity": result}


async def bulk_upsert_entities(
    client: DremioClient,
    spec_file: Path,
    *,
    wiki_from_catalog: bool,
    task_id: str | None,
) -> dict[str, Any]:
    payload = _load_structured_file(spec_file)
    entities = payload.get("entities") if isinstance(payload, dict) else payload
    if not isinstance(entities, list):
        raise ValueError("Bulk entity spec must be a list or an object with an 'entities' list.")

    results = []
    for raw_entity in entities:
        if not isinstance(raw_entity, dict):
            raise ValueError("Each bulk entity entry must be an object.")
        path = raw_entity.get("path")
        dot_path = ".".join(path) if isinstance(path, list) else str(path or "").strip()
        if not dot_path:
            raise ValueError("Each bulk entity entry must include a path.")
        glossary_file = raw_entity.get("glossaryFile")
        glossary_path = (spec_file.parent / glossary_file) if glossary_file else None
        glossary_inline = _normalize_glossary_entries(raw_entity.get("attributes") or raw_entity.get("glossary") or [])
        entity_payload = await _build_table_payload(
            client,
            dot_path,
            name=raw_entity.get("name"),
            description=raw_entity.get("description"),
            wiki_from_catalog=bool(raw_entity.get("wikiFromCatalog", wiki_from_catalog)),
            glossary_file=glossary_path,
            glossary_inline=glossary_inline,
        )
        try:
            entity_result = await client.update_semantic_layer_entity(dot_path, entity_payload, task_id=task_id)
            action = "updated"
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise handle_api_error(exc) from exc
            entity_result = await client.add_semantic_layer_entity(entity_payload, task_id=task_id)
            action = "created"
        results.append({"path": dot_path, "action": action, "entity": entity_result})
    return {"count": len(results), "results": results}


async def patch_semantic_layer_scope(
    client: DremioClient, add_paths: list[str], remove_paths: list[str]
) -> dict[str, Any]:
    body = {
        "add": [{"components": parse_path(path)} for path in add_paths],
        "remove": [{"components": parse_path(path)} for path in remove_paths],
    }
    try:
        return await client.patch_semantic_layer_scope(body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


@app.command("delete")
def cli_delete(
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Delete the semantic layer."""
    client = _get_client()
    _run_command(client.delete_semantic_layer(), client, fmt)


@app.command("initialize")
def cli_initialize(
    paths: list[str] | None = typer.Argument(
        None, help="One or more dot-separated dataset paths to initialize into the semantic layer."
    ),
    paths_file: Path | None = typer.Option(
        None, "--paths-file", help="Text, JSON, or YAML file containing dataset paths."
    ),
    dictionary_file: Path | None = typer.Option(
        None, "--dictionary-file", help="Path to a global data dictionary text file."
    ),
    dictionary_text: str | None = typer.Option(None, "--dictionary-text", help="Inline global data dictionary text."),
    scoped_dictionaries_file: Path | None = typer.Option(
        None, "--scoped-dictionaries-file", help="JSON/YAML file containing a list of scoped dictionaries."
    ),
    include_query_history: bool = typer.Option(
        False, "--include-query-history", help="Analyze historical query jobs during initialization."
    ),
    autopilot_mode: str | None = typer.Option(
        None, "--autopilot-mode", help="OFF, OBSERVE, ASSIST, AUTOMATE, or CUSTOM."
    ),
    max_jobs_to_process: int | None = typer.Option(None, "--max-jobs-to-process", min=1, help="Historical job limit."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Initialize the semantic layer from one or more datasets."""
    client = _get_client()
    resolved_paths = _load_paths(paths, paths_file)
    _run_command(
        initialize_semantic_layer(
            client,
            resolved_paths,
            dictionary_file=dictionary_file,
            dictionary_text=dictionary_text,
            scoped_dictionaries_file=scoped_dictionaries_file,
            include_query_history=include_query_history,
            autopilot_mode=autopilot_mode,
            max_jobs_to_process=max_jobs_to_process,
        ),
        client,
        fmt,
    )


@task_app.command("add")
def cli_task_add(
    paths: list[str] | None = typer.Argument(
        None, help="One or more dot-separated dataset paths to hydrate into a draft semantic layer task."
    ),
    paths_file: Path | None = typer.Option(
        None, "--paths-file", help="Text, JSON, or YAML file containing dataset paths."
    ),
    dictionary_file: Path | None = typer.Option(None, "--dictionary-file", help="Path to a data dictionary text file."),
    dictionary_text: str | None = typer.Option(None, "--dictionary-text", help="Inline data dictionary text."),
    include_query_history: bool = typer.Option(False, "--include-query-history", help="Analyze historical query jobs."),
    max_jobs_to_process: int | None = typer.Option(None, "--max-jobs-to-process", min=1, help="Historical job limit."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Create a semantic layer hydration task."""
    client = _get_client()
    resolved_paths = _load_paths(paths, paths_file)
    _run_command(
        add_semantic_layer_task(
            client,
            resolved_paths,
            dictionary_file=dictionary_file,
            dictionary_text=dictionary_text,
            include_query_history=include_query_history,
            max_jobs_to_process=max_jobs_to_process,
        ),
        client,
        fmt,
    )


@task_app.command("add-batch")
def cli_task_add_batch(
    paths: list[str] | None = typer.Argument(None, help="One or more dot-separated dataset paths to hydrate."),
    paths_file: Path | None = typer.Option(
        None, "--paths-file", help="Text, JSON, or YAML file containing dataset paths."
    ),
    dictionary_file: Path | None = typer.Option(None, "--dictionary-file", help="Path to a data dictionary text file."),
    dictionary_text: str | None = typer.Option(None, "--dictionary-text", help="Inline data dictionary text."),
    include_query_history: bool = typer.Option(False, "--include-query-history", help="Analyze historical query jobs."),
    max_jobs_to_process: int | None = typer.Option(None, "--max-jobs-to-process", min=1, help="Historical job limit."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Split a large entity list into max-10 task-add requests and submit them sequentially."""
    client = _get_client()
    resolved_paths = _load_paths(paths, paths_file)
    _run_command(
        add_semantic_layer_task_batched(
            client,
            resolved_paths,
            dictionary_file=dictionary_file,
            dictionary_text=dictionary_text,
            include_query_history=include_query_history,
            max_jobs_to_process=max_jobs_to_process,
        ),
        client,
        fmt,
    )


@task_app.command("list")
def cli_task_list(
    task_state: str | None = typer.Option(None, "--state", help="Optional task state filter."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """List semantic layer tasks."""
    client = _get_client()
    _run_command(client.list_semantic_layer_tasks(task_state), client, fmt)


@task_app.command("get")
def cli_task_get(
    task_id: str = typer.Argument(help="Semantic layer task ID."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Get semantic layer task status."""
    client = _get_client()
    _run_command(client.get_semantic_layer_task(task_id), client, fmt)


@task_app.command("deploy")
def cli_task_deploy(
    task_id: str = typer.Argument(help="Semantic layer task ID."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Deploy a draft semantic layer task."""
    client = _get_client()
    _run_command(client.deploy_semantic_layer_entities(task_id), client, fmt)


@task_app.command("deploy-batch")
def cli_task_deploy_batch(
    task_ids: list[str] | None = typer.Argument(None, help="One or more semantic layer task IDs."),
    task_ids_file: Path | None = typer.Option(
        None, "--task-ids-file", help="Text, JSON, or YAML file containing task IDs."
    ),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Deploy multiple semantic layer tasks sequentially."""
    client = _get_client()
    resolved_task_ids = _load_task_ids(task_ids, task_ids_file)
    _run_command(deploy_semantic_layer_tasks_batched(client, resolved_task_ids), client, fmt)


@task_app.command("abandon")
def cli_task_abandon(
    task_id: str = typer.Argument(help="Semantic layer task ID."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Abandon a draft semantic layer task."""
    client = _get_client()
    _run_command(client.abandon_semantic_layer_entities(task_id), client, fmt)


@task_app.command("abandon-batch")
def cli_task_abandon_batch(
    task_ids: list[str] | None = typer.Argument(None, help="One or more semantic layer task IDs."),
    task_ids_file: Path | None = typer.Option(
        None, "--task-ids-file", help="Text, JSON, or YAML file containing task IDs."
    ),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Abandon multiple semantic layer tasks sequentially."""
    client = _get_client()
    resolved_task_ids = _load_task_ids(task_ids, task_ids_file)
    _run_command(abandon_semantic_layer_tasks_batched(client, resolved_task_ids), client, fmt)


@task_app.command("cancel")
def cli_task_cancel(
    task_id: str = typer.Argument(help="Semantic layer task ID."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Cancel an in-progress semantic layer task."""
    client = _get_client()
    _run_command(client.cancel_semantic_layer_task(task_id), client, fmt)


@entity_app.command("list")
def cli_entity_list(
    entity_type: str = typer.Argument(help="Entity type: TABLE or METRIC."),
    task_id: str | None = typer.Option(None, "--task-id", help="Optional draft task ID."),
    page_token: str | None = typer.Option(None, "--page-token", help="Pagination token."),
    limit: int = typer.Option(50, "--limit", min=1, max=500, help="Page size."),
    all_pages: bool = typer.Option(False, "--all", help="Fetch all pages by following nextPageToken."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """List semantic layer entities."""
    client = _get_client()
    _run_command(
        list_semantic_entities(
            client,
            _normalize_entity_type(entity_type),
            task_id=task_id,
            page_token=page_token,
            limit=limit,
            fetch_all=all_pages,
            fmt=fmt,
        ),
        client,
        fmt,
    )


@entity_app.command("get")
def cli_entity_get(
    entity_type: str = typer.Argument(help="Entity type: TABLE or METRIC."),
    entity_id: str = typer.Argument(help="Entity ID or dot-separated table path."),
    task_id: str | None = typer.Option(None, "--task-id", help="Optional draft task ID."),
    include_relationships: bool = typer.Option(False, "--include-relationships", help="Include relationships."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Get a semantic layer entity."""
    client = _get_client()
    _run_command(
        client.get_semantic_layer_entity(
            entity_id,
            _normalize_entity_type(entity_type),
            task_id=task_id,
            include_relationships=include_relationships,
        ),
        client,
        fmt,
    )


@entity_app.command("delete")
def cli_entity_delete(
    entity_type: str = typer.Argument(help="Entity type: TABLE or METRIC."),
    entity_id: str = typer.Argument(help="Entity ID or dot-separated table path."),
    task_id: str | None = typer.Option(None, "--task-id", help="Optional draft task ID."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Delete a semantic layer entity."""
    client = _get_client()
    _run_command(
        client.delete_semantic_layer_entity(entity_id, _normalize_entity_type(entity_type), task_id), client, fmt
    )


@entity_app.command("upsert-table")
def cli_entity_upsert_table(
    path: str = typer.Argument(help="Dot-separated dataset path."),
    name: str | None = typer.Option(None, "--name", help="Display name to use in the semantic layer."),
    description: str | None = typer.Option(
        None, "--description", help="Table description to store in the semantic layer."
    ),
    wiki_from_catalog: bool = typer.Option(
        False, "--wiki-from-catalog", help="Use the catalog wiki as the table description when --description is absent."
    ),
    glossary_file: Path | None = typer.Option(
        None, "--glossary-file", help="JSON/YAML file with column glossary entries."
    ),
    task_id: str | None = typer.Option(None, "--task-id", help="Optional draft task ID."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Create or update a TABLE entity from catalog schema, wiki, and glossary metadata."""
    client = _get_client()
    _run_command(
        upsert_table_entity(
            client,
            path,
            name=name,
            description=description,
            wiki_from_catalog=wiki_from_catalog,
            glossary_file=glossary_file,
            task_id=task_id,
        ),
        client,
        fmt,
    )


@entity_app.command("bulk-upsert")
def cli_entity_bulk_upsert(
    spec_file: Path = typer.Argument(help="JSON/YAML file describing table entities to create or update."),
    wiki_from_catalog: bool = typer.Option(
        False, "--wiki-from-catalog", help="Use catalog wiki as the fallback description for entries that omit one."
    ),
    task_id: str | None = typer.Option(None, "--task-id", help="Optional draft task ID."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Bulk create or update TABLE entities from a JSON/YAML spec."""
    client = _get_client()
    _run_command(
        bulk_upsert_entities(client, spec_file, wiki_from_catalog=wiki_from_catalog, task_id=task_id), client, fmt
    )


@app.command("scope")
def cli_scope_get(
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Get semantic layer scope."""
    client = _get_client()
    _run_command(client.get_semantic_layer_scope(), client, fmt)


@app.command("scope-patch")
def cli_scope_patch(
    add: list[str] = typer.Option([], "--add", help="Dataset path to add to semantic layer scope."),
    remove: list[str] = typer.Option([], "--remove", help="Dataset path to remove from semantic layer scope."),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Add or remove datasets from semantic layer scope."""
    if not add and not remove:
        raise typer.BadParameter("Provide at least one --add or --remove dataset path.")
    client = _get_client()
    _run_command(patch_semantic_layer_scope(client, add, remove), client, fmt)
