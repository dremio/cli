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

from unittest.mock import AsyncMock

import pytest

from drs.commands.reflection import delete, get_reflection, refresh


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
