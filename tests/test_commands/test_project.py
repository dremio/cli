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
"""Tests for dremio project commands."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from drs.commands.project import create_project, delete_project, get_project, list_projects, update_project


@pytest.mark.asyncio
async def test_list_projects(mock_client) -> None:
    mock_client.list_projects = AsyncMock(return_value={"data": [{"id": "p1", "name": "My Project"}]})
    result = await list_projects(mock_client)
    mock_client.list_projects.assert_called_once()
    assert result["data"][0]["name"] == "My Project"


@pytest.mark.asyncio
async def test_get_project(mock_client) -> None:
    mock_client.get_project = AsyncMock(return_value={"id": "p1", "name": "My Project"})
    result = await get_project(mock_client, "p1")
    mock_client.get_project.assert_called_once_with("p1")
    assert result["name"] == "My Project"


@pytest.mark.asyncio
async def test_create_project(mock_client) -> None:
    mock_client.create_project = AsyncMock(return_value={"id": "p2", "name": "New Project"})
    result = await create_project(mock_client, "New Project")
    mock_client.create_project.assert_called_once_with({"name": "New Project"})
    assert result["name"] == "New Project"


@pytest.mark.asyncio
async def test_update_project(mock_client) -> None:
    mock_client.get_project = AsyncMock(return_value={"id": "p1", "name": "Old Name"})
    mock_client.update_project = AsyncMock(return_value={"id": "p1", "name": "New Name"})
    result = await update_project(mock_client, "p1", name="New Name")
    mock_client.update_project.assert_called_once_with("p1", {"id": "p1", "name": "New Name"})
    assert result["name"] == "New Name"


@pytest.mark.asyncio
async def test_delete_project(mock_client) -> None:
    mock_client.delete_project = AsyncMock(return_value={"status": "ok"})
    await delete_project(mock_client, "p1")
    mock_client.delete_project.assert_called_once_with("p1")
