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
"""Tests for drs grant commands."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from drs.commands.grant import get_grants, set_grants, remove_grants


@pytest.mark.asyncio
async def test_get_grants(mock_client) -> None:
    mock_client.get_grants = AsyncMock(return_value={"privileges": ["MANAGE_GRANTS"]})
    result = await get_grants(mock_client, "projects", "proj-1", "role", "role-1")
    mock_client.get_grants.assert_called_once_with("projects", "proj-1", "role", "role-1")


@pytest.mark.asyncio
async def test_set_grants(mock_client) -> None:
    mock_client.set_grants = AsyncMock(return_value={"privileges": ["MANAGE_GRANTS", "CREATE_TABLE"]})
    result = await set_grants(mock_client, "projects", "proj-1", "role", "role-1", ["MANAGE_GRANTS", "CREATE_TABLE"])
    mock_client.set_grants.assert_called_once_with(
        "projects", "proj-1", "role", "role-1", {"privileges": ["MANAGE_GRANTS", "CREATE_TABLE"]}
    )


@pytest.mark.asyncio
async def test_remove_grants(mock_client) -> None:
    mock_client.delete_grants = AsyncMock(return_value={"status": "ok"})
    await remove_grants(mock_client, "projects", "proj-1", "user", "user-1")
    mock_client.delete_grants.assert_called_once_with("projects", "proj-1", "user", "user-1")
