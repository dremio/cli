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
"""Tests for dremio semantic commands."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from drs.commands.semantic import (
    _chunk_paths,
    _flatten_semantic_entity,
    _load_paths,
    _load_task_ids,
    abandon_semantic_layer_tasks_batched,
    add_semantic_layer_task,
    add_semantic_layer_task_batched,
    bulk_upsert_entities,
    deploy_semantic_layer_tasks_batched,
    initialize_semantic_layer,
    list_semantic_entities,
    patch_semantic_layer_scope,
    upsert_table_entity,
)
from drs.output import OutputFormat


@pytest.mark.asyncio
async def test_add_semantic_layer_task_with_dictionary_file(mock_client, tmp_path) -> None:
    dictionary = tmp_path / "dictionary.md"
    dictionary.write_text("orders glossary", encoding="utf-8")
    mock_client.add_semantic_layer_task = AsyncMock(return_value={"taskId": "t1"})

    result = await add_semantic_layer_task(
        mock_client,
        ["Samples.sales.orders"],
        dictionary_file=dictionary,
        dictionary_text=None,
        include_query_history=True,
        max_jobs_to_process=25,
    )

    mock_client.add_semantic_layer_task.assert_called_once_with(
        {
            "entities": [{"type": "TABLE", "path": ["Samples", "sales", "orders"]}],
            "dataDictionary": "orders glossary",
            "includeQueryHistory": True,
            "maxJobsToProcess": 25,
        }
    )
    assert result == {"taskId": "t1"}


@pytest.mark.asyncio
async def test_initialize_semantic_layer_with_scoped_dictionaries(mock_client, tmp_path) -> None:
    scoped = tmp_path / "scoped.yaml"
    scoped.write_text(
        """
- scope:
    components: [Samples, sales]
  dataDictionary: Sales glossary
- dataDictionary: Global glossary
""".strip(),
        encoding="utf-8",
    )
    mock_client.initialize_semantic_layer = AsyncMock(return_value={"taskId": "init-1"})

    result = await initialize_semantic_layer(
        mock_client,
        ["Samples.sales.orders"],
        dictionary_file=None,
        dictionary_text=None,
        scoped_dictionaries_file=scoped,
        include_query_history=False,
        autopilot_mode="assist",
        max_jobs_to_process=None,
    )

    mock_client.initialize_semantic_layer.assert_called_once_with(
        {
            "entities": [{"type": "TABLE", "path": ["Samples", "sales", "orders"]}],
            "includeQueryHistory": False,
            "autopilotMode": "ASSIST",
            "dataDictionaries": [
                {"scope": {"components": ["Samples", "sales"]}, "dataDictionary": "Sales glossary"},
                {"dataDictionary": "Global glossary"},
            ],
        }
    )
    assert result == {"taskId": "init-1"}


@pytest.mark.asyncio
async def test_upsert_table_entity_uses_catalog_wiki_and_glossary(mock_client, tmp_path) -> None:
    glossary = tmp_path / "glossary.yaml"
    glossary.write_text(
        """
attributes:
  - sourceColumn: order_id
    alias: Order ID
    description: Unique order identifier
    keyType: PRIMARY
""".strip(),
        encoding="utf-8",
    )
    mock_client.get_catalog_by_path = AsyncMock(
        return_value={"id": "catalog-1", "fields": [{"name": "order_id"}, {"name": "customer_id"}]}
    )
    mock_client.get_wiki = AsyncMock(return_value={"text": "Orders business definition"})
    request = httpx.Request("PUT", "https://example.com")
    response = httpx.Response(404, request=request)
    mock_client.update_semantic_layer_entity = AsyncMock(
        side_effect=httpx.HTTPStatusError("Not Found", request=request, response=response)
    )
    mock_client.add_semantic_layer_entity = AsyncMock(return_value={"id": "entity-1"})

    result = await upsert_table_entity(
        mock_client,
        "Samples.sales.orders",
        name=None,
        description=None,
        wiki_from_catalog=True,
        glossary_file=glossary,
        task_id="draft-1",
    )

    mock_client.add_semantic_layer_entity.assert_called_once_with(
        {
            "type": "TABLE",
            "path": ["Samples", "sales", "orders"],
            "name": "orders",
            "description": "Orders business definition",
            "attributes": [
                {
                    "sourceColumn": "order_id",
                    "alias": "Order ID",
                    "description": "Unique order identifier",
                    "keyType": "PRIMARY",
                },
                {
                    "sourceColumn": "customer_id",
                    "alias": "customer_id",
                    "description": "",
                    "keyType": "NONE",
                },
            ],
        },
        task_id="draft-1",
    )
    assert result["action"] == "created"


@pytest.mark.asyncio
async def test_bulk_upsert_entities_supports_relative_glossary_files(mock_client, tmp_path) -> None:
    glossary = tmp_path / "customers-glossary.json"
    glossary.write_text(
        '[{"sourceColumn":"customer_id","alias":"Customer ID","description":"Business customer key","keyType":"PRIMARY"}]',
        encoding="utf-8",
    )
    spec = tmp_path / "entities.yaml"
    spec.write_text(
        """
entities:
  - path: Samples.sales.customers
    description: Customer dimension
    glossaryFile: customers-glossary.json
""".strip(),
        encoding="utf-8",
    )
    mock_client.get_catalog_by_path = AsyncMock(return_value={"id": "catalog-2", "fields": [{"name": "customer_id"}]})
    mock_client.update_semantic_layer_entity = AsyncMock(return_value={"id": "entity-2"})

    result = await bulk_upsert_entities(mock_client, spec, wiki_from_catalog=False, task_id=None)

    assert result["count"] == 1
    mock_client.update_semantic_layer_entity.assert_called_once()
    payload = mock_client.update_semantic_layer_entity.call_args.args[1]
    assert payload["description"] == "Customer dimension"
    assert payload["attributes"][0]["alias"] == "Customer ID"


@pytest.mark.asyncio
async def test_patch_semantic_layer_scope(mock_client) -> None:
    mock_client.patch_semantic_layer_scope = AsyncMock(return_value={"datasets": []})

    result = await patch_semantic_layer_scope(
        mock_client,
        ["Samples.sales.orders"],
        ["Samples.sales.customers"],
    )

    mock_client.patch_semantic_layer_scope.assert_called_once_with(
        {
            "add": [{"components": ["Samples", "sales", "orders"]}],
            "remove": [{"components": ["Samples", "sales", "customers"]}],
        }
    )
    assert result == {"datasets": []}


def test_chunk_paths() -> None:
    paths = [f"Samples.sales.table_{i}" for i in range(23)]

    result = _chunk_paths(paths)

    assert [len(chunk) for chunk in result] == [10, 10, 3]


def test_load_paths_from_text_file(tmp_path) -> None:
    paths_file = tmp_path / "paths.txt"
    paths_file.write_text(
        '\n# comment\n"Samples"."sales"."orders"\n"Samples"."sales"."customers"\n',
        encoding="utf-8",
    )

    result = _load_paths(None, paths_file)

    assert result == ['"Samples"."sales"."orders"', '"Samples"."sales"."customers"']


def test_load_paths_from_yaml_file(tmp_path) -> None:
    paths_file = tmp_path / "paths.yaml"
    paths_file.write_text(
        """
paths:
  - path: [Samples, sales, orders]
  - components: [Samples, sales, customers]
""".strip(),
        encoding="utf-8",
    )

    result = _load_paths(None, paths_file)

    assert result == ["Samples.sales.orders", "Samples.sales.customers"]


def test_load_task_ids_from_add_batch_output(tmp_path) -> None:
    task_file = tmp_path / "tasks.json"
    task_file.write_text(
        """
{
  "batches": 2,
  "results": [
    {"task": {"taskId": "11111111-1111-1111-1111-111111111111"}},
    {"task": {"taskId": "22222222-2222-2222-2222-222222222222"}}
  ]
}
""".strip(),
        encoding="utf-8",
    )

    result = _load_task_ids(None, task_file)

    assert result == [
        "11111111-1111-1111-1111-111111111111",
        "22222222-2222-2222-2222-222222222222",
    ]


@pytest.mark.asyncio
async def test_initialize_semantic_layer_rejects_more_than_ten_paths(mock_client) -> None:
    paths = [f"Samples.sales.table_{i}" for i in range(11)]

    with pytest.raises(ValueError, match="at most 10 entities"):
        await initialize_semantic_layer(
            mock_client,
            paths,
            dictionary_file=None,
            dictionary_text=None,
            scoped_dictionaries_file=None,
            include_query_history=False,
            autopilot_mode=None,
            max_jobs_to_process=None,
        )


@pytest.mark.asyncio
async def test_add_semantic_layer_task_batched_submits_multiple_requests(mock_client, tmp_path) -> None:
    dictionary = tmp_path / "dictionary.md"
    dictionary.write_text("global dictionary", encoding="utf-8")
    mock_client.add_semantic_layer_task = AsyncMock(
        side_effect=[
            {"taskId": "t1", "state": "PENDING", "entitiesSubmitted": 10},
            {"taskId": "t2", "state": "PENDING", "entitiesSubmitted": 2},
        ]
    )

    result = await add_semantic_layer_task_batched(
        mock_client,
        [f"Samples.sales.table_{i}" for i in range(12)],
        dictionary_file=dictionary,
        dictionary_text=None,
        include_query_history=False,
        max_jobs_to_process=None,
    )

    assert result["batches"] == 2
    assert result["totalEntities"] == 12
    assert [entry["task"]["taskId"] for entry in result["results"]] == ["t1", "t2"]
    first_call = mock_client.add_semantic_layer_task.call_args_list[0].args[0]
    second_call = mock_client.add_semantic_layer_task.call_args_list[1].args[0]
    assert first_call["dataDictionary"] == "global dictionary"
    assert second_call["dataDictionary"] == "global dictionary"
    assert len(first_call["entities"]) == 10
    assert len(second_call["entities"]) == 2


@pytest.mark.asyncio
async def test_deploy_semantic_layer_tasks_batched(mock_client) -> None:
    mock_client.deploy_semantic_layer_entities = AsyncMock(
        side_effect=[
            {"taskId": "t1", "success": True},
            {"taskId": "t2", "success": True},
        ]
    )

    result = await deploy_semantic_layer_tasks_batched(mock_client, ["t1", "t2"])

    assert result["action"] == "deploy"
    assert result["count"] == 2
    assert [entry["taskId"] for entry in result["results"]] == ["t1", "t2"]


@pytest.mark.asyncio
async def test_abandon_semantic_layer_tasks_batched(mock_client) -> None:
    mock_client.abandon_semantic_layer_entities = AsyncMock(
        side_effect=[
            {"taskId": "t1", "success": True},
            {"taskId": "t2", "success": True},
        ]
    )

    result = await abandon_semantic_layer_tasks_batched(mock_client, ["t1", "t2"])

    assert result["action"] == "abandon"
    assert result["count"] == 2
    assert [entry["taskId"] for entry in result["results"]] == ["t1", "t2"]


def test_flatten_semantic_entity() -> None:
    result = _flatten_semantic_entity(
        {
            "id": "table-1",
            "type": "TABLE",
            "name": "orders",
            "path": ["Samples", "sales", "orders"],
            "description": "Orders fact table",
            "confidenceScore": 92.5,
            "attributes": [{"id": "a1"}, {"id": "a2"}],
            "relationships": [{"id": "r1"}],
            "relatedMetricCount": 4,
        }
    )

    assert result["path"] == "Samples.sales.orders"
    assert result["attributeCount"] == 2
    assert result["relationshipCount"] == 1


@pytest.mark.asyncio
async def test_list_semantic_entities_fetch_all_pages_for_json(mock_client) -> None:
    mock_client.list_semantic_layer_entities = AsyncMock(
        side_effect=[
            {
                "entities": [{"id": "e1", "type": "TABLE", "path": ["a"], "attributes": []}],
                "nextPageToken": "p2",
                "totalCount": 2,
            },
            {
                "entities": [{"id": "e2", "type": "TABLE", "path": ["b"], "attributes": []}],
                "totalCount": 2,
            },
        ]
    )

    result = await list_semantic_entities(
        mock_client,
        "TABLE",
        task_id="draft-1",
        page_token=None,
        limit=50,
        fetch_all=True,
        fmt=OutputFormat.json,
    )

    assert result["count"] == 2
    assert result["allPagesFetched"] is True
    assert [entity["id"] for entity in result["entities"]] == ["e1", "e2"]
    assert mock_client.list_semantic_layer_entities.call_count == 2


@pytest.mark.asyncio
async def test_list_semantic_entities_flattens_for_pretty(mock_client) -> None:
    mock_client.list_semantic_layer_entities = AsyncMock(
        return_value={
            "entities": [
                {
                    "id": "e1",
                    "type": "TABLE",
                    "name": "orders",
                    "path": ["Samples", "sales", "orders"],
                    "description": "Orders fact table",
                    "attributes": [{"id": "a1"}],
                    "relationships": [],
                }
            ],
            "totalCount": 1,
        }
    )

    result = await list_semantic_entities(
        mock_client,
        "TABLE",
        task_id=None,
        page_token=None,
        limit=50,
        fetch_all=False,
        fmt=OutputFormat.pretty,
    )

    assert result == [
        {
            "id": "e1",
            "type": "TABLE",
            "name": "orders",
            "path": "Samples.sales.orders",
            "description": "Orders fact table",
            "confidenceScore": "",
            "attributeCount": 1,
            "relationshipCount": 0,
            "relatedMetricCount": "",
        }
    ]
