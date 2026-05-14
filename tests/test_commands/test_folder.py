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
"""Tests for dremio folder commands."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from drs.commands.folder import create_folder, delete_entity, delete_folder, get_entity, get_folder, grants
from drs.utils import DremioAPIError


@pytest.mark.asyncio
async def test_get_entity_splits_path(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(
        return_value={
            "id": "abc",
            "entityType": "dataset",
            "path": ["space", "table"],
        }
    )
    result = await get_entity(mock_client, "space.table")
    mock_client.get_catalog_by_path.assert_called_once_with(["space", "table"])
    assert result["id"] == "abc"


@pytest.mark.asyncio
async def test_get_entity_handles_dots_in_quotes(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "abc"})
    await get_entity(mock_client, '"My Source"."my.table"')
    mock_client.get_catalog_by_path.assert_called_once_with(["My Source", "my.table"])


@pytest.mark.asyncio
async def test_create_folder_single_raises_error(mock_client) -> None:
    """Single-component path should raise ValueError pointing to `dremio space create`."""
    with pytest.raises(ValueError, match="dremio space create"):
        await create_folder(mock_client, "Analytics")


@pytest.mark.asyncio
async def test_get_folder_single_raises_error(mock_client) -> None:
    """Single-component path in get_folder should raise ValueError pointing to `dremio space get`."""
    with pytest.raises(ValueError, match="dremio space get"):
        await get_folder(mock_client, "Analytics")


@pytest.mark.asyncio
async def test_delete_folder_single_raises_error(mock_client) -> None:
    """Single-component path in delete_folder should raise ValueError pointing to `dremio space delete`."""
    with pytest.raises(ValueError, match="dremio space delete"):
        await delete_folder(mock_client, "Analytics")


@pytest.mark.asyncio
async def test_create_folder_nested_creates_folder(mock_client) -> None:
    """Nested path should CREATE FOLDER."""
    mock_client.submit_sql = AsyncMock(return_value={"id": "job-1"})
    mock_client.get_job_status = AsyncMock(return_value={"jobState": "COMPLETED", "rowCount": 0})
    mock_client.get_job_results = AsyncMock(return_value={"rows": []})
    await create_folder(mock_client, "Analytics.reports")
    sql = mock_client.submit_sql.call_args[0][0]
    assert "CREATE FOLDER" in sql
    assert '"Analytics"."reports"' in sql


@pytest.mark.asyncio
async def test_create_folder_failure_is_raised(mock_client) -> None:
    """SQL failures (e.g., namespace not found) are raised as DremioAPIError."""
    mock_client.submit_sql = AsyncMock(return_value={"id": "job-1"})
    mock_client.get_job_status = AsyncMock(
        return_value={
            "jobState": "FAILED",
            "errorMessage": "NoSuchNamespaceException: Namespace does not exist: NoSuchSpace",
        }
    )
    mock_client.get_job_results = AsyncMock(return_value={"rows": []})
    with pytest.raises(DremioAPIError, match="Namespace does not exist"):
        await create_folder(mock_client, "NoSuchSpace.folder1")


@pytest.mark.asyncio
async def test_get_folder_nested_delegates_to_get_entity(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "f1", "entityType": "folder"})
    result = await get_folder(mock_client, "myspace.reports")
    mock_client.get_catalog_by_path.assert_called_once_with(["myspace", "reports"])
    assert result["id"] == "f1"


@pytest.mark.asyncio
async def test_delete_folder_nested_delegates_to_delete_entity(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "f1", "tag": "v1"})
    mock_client.delete_catalog_entity = AsyncMock(return_value={"status": "ok"})
    await delete_folder(mock_client, "myspace.reports")
    mock_client.delete_catalog_entity.assert_called_once_with("f1", tag="v1")


@pytest.mark.asyncio
async def test_delete_entity(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "entity-1", "tag": "v1", "entityType": "space"})
    mock_client.delete_catalog_entity = AsyncMock(return_value={"status": "ok"})
    await delete_entity(mock_client, "myspace")
    mock_client.delete_catalog_entity.assert_called_once_with("entity-1", tag="v1")


@pytest.mark.asyncio
async def test_grants(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "entity-1", "accessControlList": {"users": []}})
    result = await grants(mock_client, "myspace.table")
    assert result["path"] == "myspace.table"
    assert "accessControlList" in result
