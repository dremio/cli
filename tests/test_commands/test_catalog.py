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
"""Tests for drs catalog commands."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from drs.commands.catalog import get_entity, search_catalog


@pytest.mark.asyncio
async def test_get_entity_splits_path(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={
        "id": "abc", "entityType": "dataset", "path": ["space", "table"],
    })

    result = await get_entity(mock_client, "space.table")

    mock_client.get_catalog_by_path.assert_called_once_with(["space", "table"])
    assert result["id"] == "abc"


@pytest.mark.asyncio
async def test_get_entity_strips_quotes(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "abc"})

    await get_entity(mock_client, '"space"."table"')

    mock_client.get_catalog_by_path.assert_called_once_with(["space", "table"])


@pytest.mark.asyncio
async def test_get_entity_handles_dots_in_quotes(mock_client) -> None:
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "abc"})

    await get_entity(mock_client, '"My Source"."my.table"')

    mock_client.get_catalog_by_path.assert_called_once_with(["My Source", "my.table"])


@pytest.mark.asyncio
async def test_search_catalog(mock_client) -> None:
    mock_client.search = AsyncMock(return_value={"data": [{"name": "orders"}]})

    result = await search_catalog(mock_client, "orders")

    mock_client.search.assert_called_once_with("orders")
    assert result["data"][0]["name"] == "orders"
