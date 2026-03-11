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
"""Tests for drs query commands."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from drs.commands.query import run_query
from drs.utils import DremioAPIError


@pytest.mark.asyncio
async def test_run_query_success(mock_client) -> None:
    """Test successful query execution with polling and result fetching."""
    mock_client.submit_sql = AsyncMock(return_value={"id": "job-1"})
    mock_client.get_job_status = AsyncMock(
        side_effect=[
            {"jobState": "RUNNING"},
            {"jobState": "COMPLETED", "rowCount": 1},
        ]
    )
    mock_client.get_job_results = AsyncMock(return_value={
        "columns": [{"name": "col1"}],
        "rows": [{"values": ["hello"]}],
    })

    result = await run_query(mock_client, "SELECT 1")

    assert result["state"] == "COMPLETED"
    assert result["rowCount"] == 1
    assert result["rows"] == [{"col1": "hello"}]
    mock_client.submit_sql.assert_called_once_with("SELECT 1", context=None)


@pytest.mark.asyncio
async def test_run_query_failed(mock_client) -> None:
    """Test query that fails returns error info."""
    mock_client.submit_sql = AsyncMock(return_value={"id": "job-2"})
    mock_client.get_job_status = AsyncMock(return_value={
        "jobState": "FAILED",
        "errorMessage": "Table not found",
    })

    result = await run_query(mock_client, "SELECT * FROM nonexistent")

    assert result["state"] == "FAILED"
    assert "Table not found" in result["error"]


@pytest.mark.asyncio
async def test_run_query_with_context(mock_client) -> None:
    """Test query with schema context."""
    mock_client.submit_sql = AsyncMock(return_value={"id": "job-3"})
    mock_client.get_job_status = AsyncMock(return_value={
        "jobState": "COMPLETED", "rowCount": 0,
    })

    result = await run_query(mock_client, "SELECT 1", context=["myspace", "folder"])

    mock_client.submit_sql.assert_called_once_with("SELECT 1", context=["myspace", "folder"])


@pytest.mark.asyncio
async def test_run_query_pagination(mock_client) -> None:
    """Test result pagination when rowCount > 500."""
    mock_client.submit_sql = AsyncMock(return_value={"id": "job-4"})
    mock_client.get_job_status = AsyncMock(return_value={
        "jobState": "COMPLETED", "rowCount": 600,
    })
    mock_client.get_job_results = AsyncMock(side_effect=[
        {"columns": [{"name": "id"}], "rows": [{"values": [str(i)]} for i in range(500)]},
        {"columns": [{"name": "id"}], "rows": [{"values": [str(i)]} for i in range(500, 600)]},
    ])

    result = await run_query(mock_client, "SELECT id FROM big_table")

    assert result["rowCount"] == 600
    assert mock_client.get_job_results.call_count == 2


@pytest.mark.asyncio
async def test_run_query_dict_rows(mock_client) -> None:
    """Test result parsing when API returns rows as named dicts (real Cloud behavior)."""
    mock_client.submit_sql = AsyncMock(return_value={"id": "job-dict"})
    mock_client.get_job_status = AsyncMock(return_value={
        "jobState": "COMPLETED", "rowCount": 2,
    })
    mock_client.get_job_results = AsyncMock(return_value={
        "rows": [{"hello": "1"}, {"hello": "2"}],
    })

    result = await run_query(mock_client, "SELECT 1 AS hello UNION SELECT 2")

    assert result["rowCount"] == 2
    assert result["rows"] == [{"hello": "1"}, {"hello": "2"}]


# -- Polling error handling tests (P2 fix) --


def _make_http_error(status_code: int, method: str = "GET", url: str = "https://api.dremio.cloud/test") -> httpx.HTTPStatusError:
    request = httpx.Request(method, url)
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(f"{status_code}", request=request, response=response)


@pytest.mark.asyncio
async def test_polling_token_expiry_raises_api_error(mock_client) -> None:
    """401 during polling raises DremioAPIError, not raw httpx traceback."""
    mock_client.submit_sql = AsyncMock(return_value={"id": "job-5"})
    mock_client.get_job_status = AsyncMock(
        side_effect=[
            {"jobState": "RUNNING"},
            _make_http_error(401),
        ]
    )

    with pytest.raises(DremioAPIError) as exc_info:
        await run_query(mock_client, "SELECT 1")

    assert exc_info.value.status_code == 401
    assert "Authentication failed" in exc_info.value.message


@pytest.mark.asyncio
async def test_polling_server_error_raises_api_error(mock_client) -> None:
    """503 during polling raises DremioAPIError."""
    mock_client.submit_sql = AsyncMock(return_value={"id": "job-6"})
    mock_client.get_job_status = AsyncMock(
        side_effect=_make_http_error(503)
    )

    with pytest.raises(DremioAPIError) as exc_info:
        await run_query(mock_client, "SELECT 1")

    assert exc_info.value.status_code == 503


@pytest.mark.asyncio
async def test_result_fetch_error_raises_api_error(mock_client) -> None:
    """HTTP error during result fetching raises DremioAPIError, not raw traceback."""
    mock_client.submit_sql = AsyncMock(return_value={"id": "job-7"})
    mock_client.get_job_status = AsyncMock(return_value={
        "jobState": "COMPLETED", "rowCount": 100,
    })
    mock_client.get_job_results = AsyncMock(
        side_effect=_make_http_error(404)
    )

    with pytest.raises(DremioAPIError) as exc_info:
        await run_query(mock_client, "SELECT * FROM evicted")

    assert exc_info.value.status_code == 404
    assert "Not found" in exc_info.value.message


@pytest.mark.asyncio
async def test_result_fetch_error_on_second_page(mock_client) -> None:
    """HTTP error on second page of results raises DremioAPIError."""
    mock_client.submit_sql = AsyncMock(return_value={"id": "job-8"})
    mock_client.get_job_status = AsyncMock(return_value={
        "jobState": "COMPLETED", "rowCount": 600,
    })
    mock_client.get_job_results = AsyncMock(side_effect=[
        {"columns": [{"name": "id"}], "rows": [{"values": [str(i)]} for i in range(500)]},
        _make_http_error(403),
    ])

    with pytest.raises(DremioAPIError) as exc_info:
        await run_query(mock_client, "SELECT id FROM big_table")

    assert exc_info.value.status_code == 403
    assert "Permission denied" in exc_info.value.message


@pytest.mark.asyncio
async def test_submit_error_still_handled(mock_client) -> None:
    """Original behavior: HTTP error on submit still raises DremioAPIError."""
    mock_client.submit_sql = AsyncMock(
        side_effect=_make_http_error(400, "POST")
    )

    with pytest.raises(DremioAPIError) as exc_info:
        await run_query(mock_client, "INVALID SQL")

    assert exc_info.value.status_code == 400
