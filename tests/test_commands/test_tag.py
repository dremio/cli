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
"""Tests for dremio tag commands."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from drs.commands.tag import get_tags, update_tags


@pytest.mark.asyncio
async def test_get_tags(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "e1"})
    mock_client.get_tags = AsyncMock(return_value={"tags": ["pii", "finance"]})
    result = await get_tags(mock_client, "myspace.table")
    assert result["tags"] == ["pii", "finance"]


@pytest.mark.asyncio
async def test_get_tags_404(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "e1"})
    request = httpx.Request("GET", "https://example.com")
    response = httpx.Response(404, request=request)
    mock_client.get_tags = AsyncMock(side_effect=httpx.HTTPStatusError("Not Found", request=request, response=response))
    result = await get_tags(mock_client, "myspace.table")
    assert result["tags"] == []


@pytest.mark.asyncio
async def test_update_tags(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "e1"})
    mock_client.get_tags = AsyncMock(return_value={"tags": ["old"], "version": 1})
    mock_client.set_tags = AsyncMock(return_value={"tags": ["pii", "finance"], "version": 2})
    await update_tags(mock_client, "myspace.table", ["pii", "finance"])
    mock_client.set_tags.assert_called_once_with("e1", ["pii", "finance"], version=1)
