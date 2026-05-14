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
async def test_create_space_sp_enabled(mock_client) -> None:
    """On SP-enabled clusters CREATE SPACE SQL succeeds on first try."""
    mock_client.submit_sql = AsyncMock(return_value={"id": "job-1"})
    mock_client.get_job_status = AsyncMock(return_value={"jobState": "COMPLETED", "rowCount": 0})
    mock_client.get_job_results = AsyncMock(return_value={"rows": []})
    result = await create_space(mock_client, "Analytics")
    assert mock_client.submit_sql.call_count == 1
    assert mock_client.submit_sql.call_args[0][0] == 'CREATE SPACE "Analytics"'
    assert result["state"] == "COMPLETED"


@pytest.mark.asyncio
async def test_create_space_pre_sp_falls_back_to_create_folder(mock_client) -> None:
    """On pre-SP clusters, CREATE SPACE fails with the known sentinel; falls back to CREATE FOLDER."""
    mock_client.submit_sql = AsyncMock(side_effect=[{"id": "job-1"}, {"id": "job-2"}])
    mock_client.get_job_status = AsyncMock(
        side_effect=[
            {
                "jobState": "FAILED",
                "errorMessage": "Cannot create space [Analytics]. Legacy spaces are not supported.",
            },
            {"jobState": "COMPLETED", "rowCount": 0},
        ]
    )
    mock_client.get_job_results = AsyncMock(return_value={"rows": []})
    result = await create_space(mock_client, "Analytics")
    assert mock_client.submit_sql.call_count == 2
    assert mock_client.submit_sql.call_args_list[0][0][0] == 'CREATE SPACE "Analytics"'
    assert mock_client.submit_sql.call_args_list[1][0][0] == 'CREATE FOLDER "Analytics"'
    assert result["state"] == "COMPLETED"


@pytest.mark.asyncio
async def test_create_space_pre_sp_fallback_failure_is_raised(mock_client) -> None:
    """If the CREATE FOLDER fallback also fails (e.g., already exists), the error is raised."""
    from drs.utils import DremioAPIError

    mock_client.submit_sql = AsyncMock(side_effect=[{"id": "job-1"}, {"id": "job-2"}])
    mock_client.get_job_status = AsyncMock(
        side_effect=[
            {
                "jobState": "FAILED",
                "errorMessage": "Cannot create space [Analytics]. Legacy spaces are not supported.",
            },
            {
                "jobState": "FAILED",
                "errorMessage": "Folder [[serverlessproject, Analytics]] already exists.",
            },
        ]
    )
    mock_client.get_job_results = AsyncMock(return_value={"rows": []})
    with pytest.raises(DremioAPIError, match="Space \\[Analytics\\] already exists"):
        await create_space(mock_client, "Analytics")
    assert mock_client.submit_sql.call_count == 2


@pytest.mark.asyncio
async def test_create_space_other_failure_is_raised(mock_client) -> None:
    """Failures unrelated to the SP sentinel (e.g., already exists) are raised, not fallen back."""
    from drs.utils import DremioAPIError

    mock_client.submit_sql = AsyncMock(return_value={"id": "job-1"})
    mock_client.get_job_status = AsyncMock(
        return_value={"jobState": "FAILED", "errorMessage": "Space [Analytics] already exists."}
    )
    mock_client.get_job_results = AsyncMock(return_value={"rows": []})
    with pytest.raises(DremioAPIError, match="already exists"):
        await create_space(mock_client, "Analytics")
    assert mock_client.submit_sql.call_count == 1


@pytest.mark.asyncio
async def test_get_space(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(
        return_value={"id": "s1", "containerType": "SPACE", "path": ["Analytics"]}
    )
    result = await get_space(mock_client, "Analytics")
    mock_client.get_catalog_by_path.assert_called_once_with(["Analytics"])
    assert result["id"] == "s1"


@pytest.mark.asyncio
async def test_get_space_nested_path_raises_error(mock_client) -> None:
    """Multi-component path should raise ValueError pointing to `dremio folder get`."""
    with pytest.raises(ValueError, match="dremio folder get"):
        await get_space(mock_client, "Analytics.reports")


@pytest.mark.asyncio
async def test_delete_space(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "s1", "tag": "v1", "containerType": "SPACE"})
    mock_client.delete_catalog_entity = AsyncMock(return_value={"status": "ok"})
    await delete_space(mock_client, "Analytics")
    mock_client.delete_catalog_entity.assert_called_once_with("s1", tag="v1")


@pytest.mark.asyncio
async def test_delete_space_wrong_container_type_raises_error(mock_client) -> None:
    """Deleting a non-space entity (e.g. a source) via space delete should raise ValueError."""
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "src1", "tag": "v1", "containerType": "SOURCE"})
    with pytest.raises(ValueError, match="source, not a space"):
        await delete_space(mock_client, "S3Source")


@pytest.mark.asyncio
async def test_delete_space_nested_path_raises_error(mock_client) -> None:
    """Multi-component path should raise ValueError pointing to `dremio folder delete`."""
    with pytest.raises(ValueError, match="dremio folder delete"):
        await delete_space(mock_client, "Analytics.reports")
