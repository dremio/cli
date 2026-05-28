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
"""dremio skill — manage saved AI Agent Skills."""

from __future__ import annotations

import asyncio
from enum import StrEnum
from pathlib import Path

import httpx
import typer

from drs.client import DremioClient
from drs.output import OutputFormat, error, output
from drs.utils import handle_api_error

app = typer.Typer(help="Manage saved AI Agent Skills.", context_settings={"help_option_names": ["-h", "--help"]})


class SkillStatus(StrEnum):
    draft = "DRAFT"
    published = "PUBLISHED"


class SkillActivationScope(StrEnum):
    all = "ALL"
    agent = "AGENT"
    manual = "MANUAL"


async def list_skills(
    client: DremioClient,
    status: str | None = None,
    limit: int = 100,
    page_token: str | None = None,
) -> dict:
    try:
        return await client.list_skills(status=_normalize_optional_upper(status), limit=limit, page_token=page_token)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def get_skill(client: DremioClient, skill_id: str) -> dict:
    try:
        return await client.get_skill(skill_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def create_skill(
    client: DremioClient,
    name: str,
    description: str,
    prompt_text: str | None = None,
    prompt_file: str | None = None,
    status: str | None = "DRAFT",
    activation_scope: str | None = "ALL",
    when_to_use: str | None = None,
    tags: str | None = None,
    metadata: list[str] | None = None,
) -> dict:
    body = {
        "name": name,
        "description": description,
        "promptText": _resolve_prompt_text(prompt_text, prompt_file, required=True),
    }
    if status:
        body["status"] = _normalize_upper(status, "status")
    if activation_scope:
        body["activationScope"] = _normalize_upper(activation_scope, "activationScope")
    if when_to_use is not None:
        body["whenToUse"] = when_to_use
    parsed_tags = _parse_tags(tags)
    if parsed_tags is not None:
        body["tags"] = parsed_tags
    parsed_metadata = _parse_metadata(metadata)
    if parsed_metadata is not None:
        body["metadata"] = parsed_metadata

    try:
        return await client.create_skill(body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def update_skill(
    client: DremioClient,
    skill_id: str,
    name: str | None = None,
    description: str | None = None,
    prompt_text: str | None = None,
    prompt_file: str | None = None,
    status: str | None = None,
    activation_scope: str | None = None,
    tag: str | None = None,
    when_to_use: str | None = None,
    tags: str | None = None,
    metadata: list[str] | None = None,
) -> dict:
    prompt = _resolve_prompt_text(prompt_text, prompt_file, required=False)
    try:
        existing = await client.get_skill(skill_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc

    body = {
        "name": name if name is not None else existing["name"],
        "description": description if description is not None else existing["description"],
        "promptText": prompt if prompt is not None else existing["promptText"],
        "status": _normalize_upper(status, "status") if status is not None else existing["status"],
        "tag": tag if tag is not None else existing["tag"],
    }
    if activation_scope is not None:
        body["activationScope"] = _normalize_upper(activation_scope, "activationScope")
    elif existing.get("activationScope") is not None:
        body["activationScope"] = existing["activationScope"]

    manifest_update = _build_manifest_update(when_to_use, tags, metadata)
    if manifest_update:
        body["manifestUpdate"] = manifest_update

    try:
        return await client.update_skill(skill_id, body)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


async def delete_skill(client: DremioClient, skill_id: str) -> dict:
    try:
        return await client.delete_skill(skill_id)
    except httpx.HTTPStatusError as exc:
        raise handle_api_error(exc) from exc


def _resolve_prompt_text(prompt_text: str | None, prompt_file: str | None, required: bool) -> str | None:
    if prompt_text is not None and prompt_file is not None:
        raise ValueError("Use either --prompt-text or --prompt-file, not both")
    if prompt_file is not None:
        return Path(prompt_file).read_text()
    if prompt_text is not None:
        return prompt_text
    if required:
        raise ValueError("One of --prompt-text or --prompt-file is required")
    return None


def _normalize_optional_upper(value: str | None) -> str | None:
    return _normalize_upper(value, "value") if value is not None else None


def _normalize_upper(value: str, field_name: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} cannot be empty")
    return stripped.upper()


def _parse_tags(tags: str | None) -> list[str] | None:
    if tags is None:
        return None
    if not tags.strip():
        return []
    return [tag.strip() for tag in tags.split(",") if tag.strip()]


def _parse_metadata(metadata: list[str] | None) -> dict[str, str] | None:
    if metadata is None:
        return None
    if not metadata or (len(metadata) == 1 and not metadata[0].strip()):
        return {}
    parsed: dict[str, str] = {}
    for item in metadata:
        if "=" not in item:
            raise ValueError(f"Invalid metadata entry '{item}'. Expected key=value.")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError("Metadata keys cannot be empty")
        parsed[key] = value
    return parsed


def _build_manifest_update(
    when_to_use: str | None,
    tags: str | None,
    metadata: list[str] | None,
) -> dict:
    update: dict = {}
    if when_to_use is not None:
        update["whenToUse"] = when_to_use
    parsed_tags = _parse_tags(tags)
    if parsed_tags is not None:
        update["tags"] = {"values": parsed_tags}
    parsed_metadata = _parse_metadata(metadata)
    if parsed_metadata is not None:
        update["metadata"] = parsed_metadata
    return update


# -- CLI wrappers --


def _get_client() -> DremioClient:
    from drs.cli import get_client

    return get_client()


def _run_command(coro, client, fmt: OutputFormat = OutputFormat.json, fields: str | None = None) -> None:
    async def _execute():
        try:
            return await coro
        finally:
            await client.close()

    try:
        result = asyncio.run(_execute())
    except Exception as exc:
        from drs.utils import DremioAPIError

        if isinstance(exc, DremioAPIError):
            error(str(exc))
            raise typer.Exit(1)
        if isinstance(exc, ValueError):
            error(str(exc))
            raise typer.Exit(1)
        raise
    output(result, fmt, fields=fields)


@app.command("list")
def cli_list(
    status: SkillStatus | None = typer.Option(None, "--status", "-s", help="Filter by status: DRAFT or PUBLISHED"),
    limit: int = typer.Option(100, "--limit", "-n", help="Maximum skills to return"),
    page_token: str | None = typer.Option(None, "--page-token", help="Pagination token from a previous response"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """List saved Skills visible to the current user."""
    client = _get_client()
    _run_command(list_skills(client, status=status, limit=limit, page_token=page_token), client, fmt, fields=fields)


@app.command("get")
def cli_get(
    skill_id: str = typer.Argument(help="Skill ID"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """Get one saved Skill by ID."""
    client = _get_client()
    _run_command(get_skill(client, skill_id), client, fmt, fields=fields)


@app.command("create")
def cli_create(
    name: str = typer.Option(..., "--name", help="Skill name"),
    description: str = typer.Option(..., "--description", help="Skill description"),
    prompt_text: str | None = typer.Option(None, "--prompt-text", help="Skill prompt text"),
    prompt_file: str | None = typer.Option(None, "--prompt-file", help="Path to a file containing Skill prompt text"),
    status: SkillStatus = typer.Option(SkillStatus.draft, "--status", "-s", help="Skill status"),
    activation_scope: SkillActivationScope = typer.Option(
        SkillActivationScope.all, "--activation-scope", help="Activation scope"
    ),
    when_to_use: str | None = typer.Option(None, "--when-to-use", help="Optional discovery selection hint"),
    tags: str | None = typer.Option(None, "--tags", help="Comma-separated tags"),
    metadata: list[str] | None = typer.Option(None, "--metadata", help="Metadata entry as key=value; repeatable"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Create a saved Skill."""
    client = _get_client()
    _run_command(
        create_skill(
            client,
            name,
            description,
            prompt_text=prompt_text,
            prompt_file=prompt_file,
            status=status,
            activation_scope=activation_scope,
            when_to_use=when_to_use,
            tags=tags,
            metadata=metadata,
        ),
        client,
        fmt,
    )


@app.command("update")
def cli_update(
    skill_id: str = typer.Argument(help="Skill ID to update"),
    name: str | None = typer.Option(None, "--name", help="New Skill name"),
    description: str | None = typer.Option(None, "--description", help="New Skill description"),
    prompt_text: str | None = typer.Option(None, "--prompt-text", help="New Skill prompt text"),
    prompt_file: str | None = typer.Option(None, "--prompt-file", help="Path to a file containing new prompt text"),
    status: SkillStatus | None = typer.Option(None, "--status", "-s", help="New Skill status"),
    activation_scope: SkillActivationScope | None = typer.Option(
        None, "--activation-scope", help="New activation scope"
    ),
    tag: str | None = typer.Option(None, "--tag", help="Optimistic concurrency tag; defaults to fetched Skill tag"),
    when_to_use: str | None = typer.Option(None, "--when-to-use", help="Replace or clear discovery selection hint"),
    tags: str | None = typer.Option(None, "--tags", help="Comma-separated tags; empty string clears tags"),
    metadata: list[str] | None = typer.Option(
        None, "--metadata", help="Metadata entry as key=value; repeatable; empty string clears metadata"
    ),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
) -> None:
    """Update a saved Skill by fetching existing values and applying overrides."""
    client = _get_client()
    _run_command(
        update_skill(
            client,
            skill_id,
            name=name,
            description=description,
            prompt_text=prompt_text,
            prompt_file=prompt_file,
            status=status,
            activation_scope=activation_scope,
            tag=tag,
            when_to_use=when_to_use,
            tags=tags,
            metadata=metadata,
        ),
        client,
        fmt,
    )


@app.command("delete")
def cli_delete(
    skill_id: str = typer.Argument(help="Skill ID to delete"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show Skill details without deleting"),
    fmt: OutputFormat = typer.Option(OutputFormat.json, "--output", "-o", help="Output format"),
    fields: str = typer.Option(None, "--fields", "-f", help="Comma-separated fields to include"),
) -> None:
    """Delete a saved Skill."""
    client = _get_client()
    if dry_run:
        _run_command(get_skill(client, skill_id), client, fmt, fields=fields)
        return
    _run_command(delete_skill(client, skill_id), client, fmt)
