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
"""Tests for dremio skill commands."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

from drs.commands.skill import create_skill, delete_skill, get_skill, list_skills, update_skill
from drs.utils import DremioAPIError


@pytest.mark.asyncio
async def test_list_skills(mock_client) -> None:
    mock_client.list_skills = AsyncMock(return_value={"data": [{"id": "skill-1", "name": "Demo"}]})

    result = await list_skills(mock_client, status="draft", limit=25, page_token="next-page")

    mock_client.list_skills.assert_called_once_with(status="DRAFT", limit=25, page_token="next-page")
    assert result == {"data": [{"id": "skill-1", "name": "Demo"}]}


@pytest.mark.asyncio
async def test_get_skill(mock_client) -> None:
    mock_client.get_skill = AsyncMock(return_value={"id": "skill-1", "name": "Demo"})

    result = await get_skill(mock_client, "skill-1")

    mock_client.get_skill.assert_called_once_with("skill-1")
    assert result == {"id": "skill-1", "name": "Demo"}


@pytest.mark.asyncio
async def test_create_skill_builds_full_body(mock_client) -> None:
    mock_client.create_skill = AsyncMock(return_value={"id": "skill-1", "name": "Demo"})

    result = await create_skill(
        mock_client,
        "Demo",
        "Use this for demos.",
        prompt_text="Follow the demo playbook.",
        status="published",
        activation_scope="agent",
        when_to_use="When preparing demos.",
        tags="demo, sales",
        metadata=["owner=field", "tier=gold"],
    )

    mock_client.create_skill.assert_called_once_with(
        {
            "name": "Demo",
            "description": "Use this for demos.",
            "promptText": "Follow the demo playbook.",
            "status": "PUBLISHED",
            "activationScope": "AGENT",
            "whenToUse": "When preparing demos.",
            "tags": ["demo", "sales"],
            "metadata": {"owner": "field", "tier": "gold"},
        }
    )
    assert result == {"id": "skill-1", "name": "Demo"}


@pytest.mark.asyncio
async def test_create_skill_reads_prompt_file(mock_client, tmp_path: Path) -> None:
    prompt_file = tmp_path / "prompt.md"
    prompt_file.write_text("Prompt from file")
    mock_client.create_skill = AsyncMock(return_value={"id": "skill-1"})

    await create_skill(mock_client, "Demo", "Description", prompt_file=str(prompt_file))

    mock_client.create_skill.assert_called_once_with(
        {
            "name": "Demo",
            "description": "Description",
            "promptText": "Prompt from file",
            "status": "DRAFT",
            "activationScope": "ALL",
        }
    )


@pytest.mark.asyncio
async def test_create_skill_requires_exactly_one_prompt_source(mock_client, tmp_path: Path) -> None:
    prompt_file = tmp_path / "prompt.md"
    prompt_file.write_text("Prompt from file")

    with pytest.raises(ValueError, match="One of --prompt-text or --prompt-file is required"):
        await create_skill(mock_client, "Demo", "Description")

    with pytest.raises(ValueError, match="Use either --prompt-text or --prompt-file"):
        await create_skill(mock_client, "Demo", "Description", prompt_text="Prompt", prompt_file=str(prompt_file))


@pytest.mark.asyncio
async def test_update_skill_preserves_existing_values_and_uses_existing_tag(mock_client) -> None:
    existing = {
        "id": "skill-1",
        "name": "Old",
        "description": "Old description",
        "promptText": "Old prompt",
        "status": "DRAFT",
        "activationScope": "ALL",
        "tag": "7",
    }
    mock_client.get_skill = AsyncMock(return_value=existing)
    mock_client.update_skill = AsyncMock(return_value={**existing, "name": "New"})

    result = await update_skill(mock_client, "skill-1", name="New")

    mock_client.update_skill.assert_called_once_with(
        "skill-1",
        {
            "name": "New",
            "description": "Old description",
            "promptText": "Old prompt",
            "status": "DRAFT",
            "tag": "7",
            "activationScope": "ALL",
        },
    )
    assert result == {**existing, "name": "New"}


@pytest.mark.asyncio
async def test_update_skill_applies_overrides_manifest_update_and_explicit_tag(mock_client) -> None:
    mock_client.get_skill = AsyncMock(
        return_value={
            "id": "skill-1",
            "name": "Old",
            "description": "Old description",
            "promptText": "Old prompt",
            "status": "DRAFT",
            "activationScope": "ALL",
            "tag": "7",
        }
    )
    mock_client.update_skill = AsyncMock(return_value={"id": "skill-1", "tag": "9"})

    result = await update_skill(
        mock_client,
        "skill-1",
        description="New description",
        prompt_text="New prompt",
        status="published",
        activation_scope="manual",
        tag="8",
        when_to_use="",
        tags="analytics, finance",
        metadata=["priority=high"],
    )

    mock_client.update_skill.assert_called_once_with(
        "skill-1",
        {
            "name": "Old",
            "description": "New description",
            "promptText": "New prompt",
            "status": "PUBLISHED",
            "tag": "8",
            "activationScope": "MANUAL",
            "manifestUpdate": {
                "whenToUse": "",
                "tags": {"values": ["analytics", "finance"]},
                "metadata": {"priority": "high"},
            },
        },
    )
    assert result == {"id": "skill-1", "tag": "9"}


@pytest.mark.asyncio
async def test_update_skill_can_clear_metadata(mock_client) -> None:
    mock_client.get_skill = AsyncMock(
        return_value={
            "id": "skill-1",
            "name": "Old",
            "description": "Old description",
            "promptText": "Old prompt",
            "status": "DRAFT",
            "activationScope": "ALL",
            "tag": "7",
        }
    )
    mock_client.update_skill = AsyncMock(return_value={"id": "skill-1"})

    await update_skill(mock_client, "skill-1", metadata=[""])

    mock_client.update_skill.assert_called_once_with(
        "skill-1",
        {
            "name": "Old",
            "description": "Old description",
            "promptText": "Old prompt",
            "status": "DRAFT",
            "tag": "7",
            "activationScope": "ALL",
            "manifestUpdate": {"metadata": {}},
        },
    )


@pytest.mark.asyncio
async def test_update_skill_rejects_two_prompt_sources(mock_client, tmp_path: Path) -> None:
    prompt_file = tmp_path / "prompt.md"
    prompt_file.write_text("Prompt from file")
    mock_client.get_skill = AsyncMock()

    with pytest.raises(ValueError, match="Use either --prompt-text or --prompt-file"):
        await update_skill(mock_client, "skill-1", prompt_text="Prompt", prompt_file=str(prompt_file))

    mock_client.get_skill.assert_not_called()


@pytest.mark.asyncio
async def test_delete_skill(mock_client) -> None:
    mock_client.delete_skill = AsyncMock(return_value={"status": "ok"})

    result = await delete_skill(mock_client, "skill-1")

    mock_client.delete_skill.assert_called_once_with("skill-1")
    assert result == {"status": "ok"}


@pytest.mark.asyncio
async def test_skill_http_error_is_converted(mock_client) -> None:
    request = httpx.Request("GET", "https://api.dremio.cloud/v1/projects/p/agent/skills/skill-1")
    response = httpx.Response(404, request=request, json={"errorMessage": "Skill not found"})
    mock_client.get_skill = AsyncMock(
        side_effect=httpx.HTTPStatusError("Not Found", request=request, response=response)
    )

    with pytest.raises(DremioAPIError) as exc_info:
        await get_skill(mock_client, "skill-1")

    assert exc_info.value.status_code == 404
    assert "Skill not found" in exc_info.value.message


@pytest.mark.asyncio
async def test_invalid_metadata_entry_raises_value_error(mock_client) -> None:
    with pytest.raises(ValueError, match="Expected key=value"):
        await create_skill(mock_client, "Demo", "Description", prompt_text="Prompt", metadata=["invalid"])
