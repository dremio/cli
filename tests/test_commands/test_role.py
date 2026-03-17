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
"""Tests for drs role commands."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from drs.commands.role import list_roles, get_role, create_role, update_role, delete_role


@pytest.mark.asyncio
async def test_list_roles(mock_client) -> None:
    mock_client.list_roles = AsyncMock(return_value={"data": [{"id": "r1", "name": "admin"}]})
    result = await list_roles(mock_client)
    mock_client.list_roles.assert_called_once()


@pytest.mark.asyncio
async def test_get_role_by_name(mock_client) -> None:
    mock_client.get_role_by_name = AsyncMock(return_value={"id": "r1", "name": "admin"})
    result = await get_role(mock_client, "admin")
    mock_client.get_role_by_name.assert_called_once_with("admin")
    assert result["name"] == "admin"


@pytest.mark.asyncio
async def test_get_role_falls_back_to_id(mock_client) -> None:
    request = httpx.Request("GET", "https://api.dremio.cloud/v1/roles/name/r1")
    response = httpx.Response(404, request=request)
    mock_client.get_role_by_name = AsyncMock(
        side_effect=httpx.HTTPStatusError("Not Found", request=request, response=response)
    )
    mock_client.get_role = AsyncMock(return_value={"id": "r1", "name": "admin"})
    result = await get_role(mock_client, "r1")
    mock_client.get_role.assert_called_once_with("r1")


@pytest.mark.asyncio
async def test_create_role(mock_client) -> None:
    mock_client.create_role = AsyncMock(return_value={"id": "r2", "name": "analyst"})
    result = await create_role(mock_client, "analyst")
    mock_client.create_role.assert_called_once_with({"name": "analyst"})
    assert result["name"] == "analyst"


@pytest.mark.asyncio
async def test_update_role(mock_client) -> None:
    mock_client.get_role = AsyncMock(return_value={"id": "r1", "name": "old"})
    mock_client.update_role = AsyncMock(return_value={"id": "r1", "name": "new"})
    result = await update_role(mock_client, "r1", "new")
    call_body = mock_client.update_role.call_args[0][1]
    assert call_body["name"] == "new"


@pytest.mark.asyncio
async def test_delete_role(mock_client) -> None:
    mock_client.delete_role = AsyncMock(return_value={"status": "ok"})
    await delete_role(mock_client, "r1")
    mock_client.delete_role.assert_called_once_with("r1")
