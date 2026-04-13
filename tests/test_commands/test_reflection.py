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
"""Tests for dremio reflection commands."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from drs.commands.reflection import delete, get_reflection, list_reflections, refresh

QUERY_RESULT = {"job_id": "j1", "state": "COMPLETED", "rowCount": 2, "rows": [{"id": "r1"}, {"id": "r2"}]}


@pytest.mark.asyncio
async def test_list_reflections_all(mock_client) -> None:
    """Omitting path queries all reflections without a WHERE clause."""
    with patch("drs.commands.reflection.run_query", new_callable=AsyncMock, return_value=QUERY_RESULT) as mock_rq:
        result = await list_reflections(mock_client)
    mock_rq.assert_called_once_with(mock_client, "SELECT * FROM sys.project.reflections")
    assert result["rowCount"] == 2


@pytest.mark.asyncio
async def test_list_reflections_for_dataset(mock_client) -> None:
    """Providing a path filters by dataset_id."""
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "ds-123"})
    with patch("drs.commands.reflection.run_query", new_callable=AsyncMock, return_value=QUERY_RESULT) as mock_rq:
        result = await list_reflections(mock_client, path="space.my_table")
    mock_rq.assert_called_once_with(mock_client, "SELECT * FROM sys.project.reflections WHERE dataset_id = 'ds-123'")
    assert result["rowCount"] == 2


@pytest.mark.asyncio
async def test_list_reflections_with_limit(mock_client) -> None:
    """--limit appends a SQL LIMIT clause."""
    with patch("drs.commands.reflection.run_query", new_callable=AsyncMock, return_value=QUERY_RESULT) as mock_rq:
        await list_reflections(mock_client, limit=50)
    mock_rq.assert_called_once_with(mock_client, "SELECT * FROM sys.project.reflections LIMIT 50")


@pytest.mark.asyncio
async def test_list_reflections_dataset_with_limit(mock_client) -> None:
    """Both path and limit combine WHERE and LIMIT."""
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "ds-456"})
    with patch("drs.commands.reflection.run_query", new_callable=AsyncMock, return_value=QUERY_RESULT) as mock_rq:
        await list_reflections(mock_client, path="space.ds", limit=10)
    mock_rq.assert_called_once_with(
        mock_client, "SELECT * FROM sys.project.reflections WHERE dataset_id = 'ds-456' LIMIT 10"
    )


@pytest.mark.asyncio
async def test_get_reflection(mock_client) -> None:
    mock_client.get_reflection = AsyncMock(return_value={"id": "r1", "status": "CAN_ACCELERATE"})
    result = await get_reflection(mock_client, "r1")
    assert result["status"] == "CAN_ACCELERATE"


@pytest.mark.asyncio
async def test_refresh(mock_client) -> None:
    mock_client.refresh_reflection = AsyncMock(return_value={"status": "ok"})
    await refresh(mock_client, "r1")
    mock_client.refresh_reflection.assert_called_once_with("r1")


@pytest.mark.asyncio
async def test_delete(mock_client) -> None:
    mock_client.delete_reflection = AsyncMock(return_value={"status": "ok"})
    await delete(mock_client, "r1")
    mock_client.delete_reflection.assert_called_once_with("r1")
