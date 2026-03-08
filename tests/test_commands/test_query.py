"""Tests for drs query commands."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from drs.commands.query import run_query


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
