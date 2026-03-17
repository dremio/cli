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
"""Tests for dremio wiki commands."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from drs.commands.wiki import get_wiki, update_wiki


@pytest.mark.asyncio
async def test_get_wiki(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "e1"})
    mock_client.get_wiki = AsyncMock(return_value={"text": "Hello", "version": 1})
    result = await get_wiki(mock_client, "myspace.table")
    assert result["wiki"] == "Hello"


@pytest.mark.asyncio
async def test_get_wiki_404(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "e1"})
    request = httpx.Request("GET", "https://example.com")
    response = httpx.Response(404, request=request)
    mock_client.get_wiki = AsyncMock(
        side_effect=httpx.HTTPStatusError("Not Found", request=request, response=response)
    )
    result = await get_wiki(mock_client, "myspace.table")
    assert result["wiki"] == ""


@pytest.mark.asyncio
async def test_update_wiki(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "e1"})
    mock_client.get_wiki = AsyncMock(return_value={"text": "old", "version": 2})
    mock_client.set_wiki = AsyncMock(return_value={"text": "new", "version": 3})
    result = await update_wiki(mock_client, "myspace.table", "new")
    mock_client.set_wiki.assert_called_once_with("e1", "new", version=2)
    assert result["wiki"] == "new"
