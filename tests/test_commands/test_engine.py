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
"""Tests for drs engine commands."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from drs.commands.engine import list_engines, get_engine, create_engine, update_engine, delete_engine, enable_engine, disable_engine


@pytest.mark.asyncio
async def test_list_engines(mock_client) -> None:
    mock_client.list_engines = AsyncMock(return_value={"data": [{"id": "eng-1", "name": "default"}]})
    result = await list_engines(mock_client)
    mock_client.list_engines.assert_called_once()
    assert result["data"][0]["name"] == "default"


@pytest.mark.asyncio
async def test_get_engine(mock_client) -> None:
    mock_client.get_engine = AsyncMock(return_value={"id": "eng-1", "name": "default", "size": "SMALL"})
    result = await get_engine(mock_client, "eng-1")
    mock_client.get_engine.assert_called_once_with("eng-1")
    assert result["size"] == "SMALL"


@pytest.mark.asyncio
async def test_create_engine(mock_client) -> None:
    mock_client.create_engine = AsyncMock(return_value={"id": "eng-2", "name": "analytics", "size": "LARGE"})
    result = await create_engine(mock_client, "analytics", size="LARGE")
    mock_client.create_engine.assert_called_once_with({"name": "analytics", "size": "LARGE"})
    assert result["name"] == "analytics"


@pytest.mark.asyncio
async def test_update_engine(mock_client) -> None:
    mock_client.get_engine = AsyncMock(return_value={"id": "eng-1", "name": "old", "size": "SMALL"})
    mock_client.update_engine = AsyncMock(return_value={"id": "eng-1", "name": "new", "size": "MEDIUM"})
    result = await update_engine(mock_client, "eng-1", name="new", size="MEDIUM")
    call_body = mock_client.update_engine.call_args[0][1]
    assert call_body["name"] == "new"
    assert call_body["size"] == "MEDIUM"


@pytest.mark.asyncio
async def test_delete_engine(mock_client) -> None:
    mock_client.delete_engine = AsyncMock(return_value={"status": "ok"})
    result = await delete_engine(mock_client, "eng-1")
    mock_client.delete_engine.assert_called_once_with("eng-1")


@pytest.mark.asyncio
async def test_enable_engine(mock_client) -> None:
    mock_client.enable_engine = AsyncMock(return_value={"id": "eng-1", "state": "ACTIVE"})
    result = await enable_engine(mock_client, "eng-1")
    mock_client.enable_engine.assert_called_once_with("eng-1")


@pytest.mark.asyncio
async def test_disable_engine(mock_client) -> None:
    mock_client.disable_engine = AsyncMock(return_value={"id": "eng-1", "state": "DISABLED"})
    result = await disable_engine(mock_client, "eng-1")
    mock_client.disable_engine.assert_called_once_with("eng-1")
