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
"""Tests for dremio space commands."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from drs.commands.space import create_space, delete_space, get_space, list_spaces


@pytest.mark.asyncio
async def test_list_spaces_filters_by_container_type(mock_client) -> None:
    mock_client.get_catalog_entity = AsyncMock(
        return_value={
            "data": [
                {"id": "s1", "containerType": "SPACE", "path": ["Analytics"]},
                {"id": "src1", "containerType": "SOURCE", "path": ["S3Source"]},
                {"id": "s2", "containerType": "SPACE", "path": ["Engineering"]},
                {"id": "home1", "containerType": "HOME", "path": ["@admin"]},
            ]
        }
    )
    result = await list_spaces(mock_client)
    assert result == {
        "entities": [
            {"id": "s1", "containerType": "SPACE", "path": ["Analytics"]},
            {"id": "s2", "containerType": "SPACE", "path": ["Engineering"]},
        ]
    }


@pytest.mark.asyncio
async def test_list_spaces_empty_catalog(mock_client) -> None:
    mock_client.get_catalog_entity = AsyncMock(return_value={"data": []})
    result = await list_spaces(mock_client)
    assert result == {"entities": []}


@pytest.mark.asyncio
async def test_create_space_runs_correct_sql(mock_client) -> None:
    mock_client.submit_sql = AsyncMock(return_value={"id": "job-1"})
    mock_client.get_job_status = AsyncMock(return_value={"jobState": "COMPLETED", "rowCount": 0})
    mock_client.get_job_results = AsyncMock(return_value={"rows": []})
    await create_space(mock_client, "Analytics")
    sql = mock_client.submit_sql.call_args[0][0]
    assert sql == 'CREATE SPACE "Analytics"'


@pytest.mark.asyncio
async def test_get_space(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(
        return_value={"id": "s1", "containerType": "SPACE", "path": ["Analytics"]}
    )
    result = await get_space(mock_client, "Analytics")
    mock_client.get_catalog_by_path.assert_called_once_with(["Analytics"])
    assert result["id"] == "s1"


@pytest.mark.asyncio
async def test_delete_space(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "s1", "tag": "v1", "containerType": "SPACE"})
    mock_client.delete_catalog_entity = AsyncMock(return_value={"status": "ok"})
    await delete_space(mock_client, "Analytics")
    mock_client.delete_catalog_entity.assert_called_once_with("s1", tag="v1")
