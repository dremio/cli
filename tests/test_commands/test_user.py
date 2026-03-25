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
"""Tests for drs user commands."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from drs.commands.user import audit, create_user, delete_user, get_user, list_users, whoami


@pytest.mark.asyncio
async def test_list_users(mock_client) -> None:
    mock_client.list_users = AsyncMock(return_value={"data": [{"id": "u1", "name": "alice"}]})
    await list_users(mock_client)
    mock_client.list_users.assert_called_once_with(max_results=100)


@pytest.mark.asyncio
async def test_get_user_by_name(mock_client) -> None:
    mock_client.get_user_by_name = AsyncMock(return_value={"id": "u1", "name": "alice"})
    result = await get_user(mock_client, "alice")
    mock_client.get_user_by_name.assert_called_once_with("alice")
    assert result["name"] == "alice"


@pytest.mark.asyncio
async def test_get_user_falls_back_to_id(mock_client) -> None:
    request = httpx.Request("GET", "https://api.dremio.cloud/v1/users/name/u1")
    response = httpx.Response(404, request=request)
    mock_client.get_user_by_name = AsyncMock(
        side_effect=httpx.HTTPStatusError("Not Found", request=request, response=response)
    )
    mock_client.get_user = AsyncMock(return_value={"id": "u1", "name": "alice"})
    await get_user(mock_client, "u1")
    mock_client.get_user.assert_called_once_with("u1")


@pytest.mark.asyncio
async def test_create_user(mock_client) -> None:
    mock_client.invite_user = AsyncMock(return_value={"id": "u2", "email": "bob@example.com"})
    await create_user(mock_client, "bob@example.com", role_id="role-1")
    mock_client.invite_user.assert_called_once_with({"email": "bob@example.com", "roleId": "role-1"})


@pytest.mark.asyncio
async def test_delete_user(mock_client) -> None:
    mock_client.delete_user = AsyncMock(return_value={"status": "ok"})
    await delete_user(mock_client, "u1")
    mock_client.delete_user.assert_called_once_with("u1")


@pytest.mark.asyncio
async def test_whoami(mock_client) -> None:
    mock_client.list_users = AsyncMock(return_value={"data": [{"id": "u1", "name": "me"}]})
    await whoami(mock_client)
    mock_client.list_users.assert_called_once_with(max_results=1)


@pytest.mark.asyncio
async def test_audit(mock_client) -> None:
    mock_client.get_user_by_name = AsyncMock(return_value={"id": "u1", "roles": [{"id": "r1", "name": "admin"}]})
    result = await audit(mock_client, "alice")
    assert result["username"] == "alice"
    assert result["roles"] == [{"role_id": "r1", "role_name": "admin"}]
