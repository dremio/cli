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
"""Tests for retry logic in DremioClient."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from drs.client import DremioClient


@pytest.mark.asyncio
async def test_retry_on_timeout(config) -> None:
    """Should retry on timeout and succeed on second attempt."""
    client = DremioClient(config)
    ok_response = httpx.Response(200, json={"ok": True}, request=httpx.Request("GET", "https://example.com"))

    client._client.request = AsyncMock(side_effect=[
        httpx.TimeoutException("timed out"),
        ok_response,
    ])

    with patch("drs.client.asyncio.sleep", new_callable=AsyncMock):
        result = await client._get("https://example.com/test")

    assert result == {"ok": True}
    assert client._client.request.call_count == 2


@pytest.mark.asyncio
async def test_retry_on_429(config) -> None:
    """Should retry on 429 Too Many Requests."""
    client = DremioClient(config)
    rate_limited = httpx.Response(429, request=httpx.Request("GET", "https://example.com"))
    ok_response = httpx.Response(200, json={"ok": True}, request=httpx.Request("GET", "https://example.com"))

    client._client.request = AsyncMock(side_effect=[rate_limited, ok_response])

    with patch("drs.client.asyncio.sleep", new_callable=AsyncMock):
        result = await client._get("https://example.com/test")

    assert result == {"ok": True}
    assert client._client.request.call_count == 2


@pytest.mark.asyncio
async def test_retry_on_503(config) -> None:
    """Should retry on 503 Service Unavailable."""
    client = DremioClient(config)
    unavailable = httpx.Response(503, request=httpx.Request("POST", "https://example.com"))
    ok_response = httpx.Response(200, json={"data": 1}, request=httpx.Request("POST", "https://example.com"))

    client._client.request = AsyncMock(side_effect=[unavailable, ok_response])

    with patch("drs.client.asyncio.sleep", new_callable=AsyncMock):
        result = await client._post("https://example.com/test", json={"sql": "SELECT 1"})

    assert result == {"data": 1}
    assert client._client.request.call_count == 2


@pytest.mark.asyncio
async def test_no_retry_on_400(config) -> None:
    """Should NOT retry on 400 Bad Request."""
    client = DremioClient(config)
    bad_request = httpx.Response(400, json={"error": "bad"}, request=httpx.Request("GET", "https://example.com"))

    client._client.request = AsyncMock(return_value=bad_request)

    with pytest.raises(httpx.HTTPStatusError):
        await client._get("https://example.com/test")

    assert client._client.request.call_count == 1


@pytest.mark.asyncio
async def test_no_retry_on_404(config) -> None:
    """Should NOT retry on 404 Not Found."""
    client = DremioClient(config)
    not_found = httpx.Response(404, json={"error": "not found"}, request=httpx.Request("GET", "https://example.com"))

    client._client.request = AsyncMock(return_value=not_found)

    with pytest.raises(httpx.HTTPStatusError):
        await client._get("https://example.com/test")

    assert client._client.request.call_count == 1


@pytest.mark.asyncio
async def test_exhausted_retries_raises_timeout(config) -> None:
    """Should raise TimeoutException after all retries exhausted."""
    client = DremioClient(config)

    client._client.request = AsyncMock(side_effect=httpx.TimeoutException("timed out"))

    with patch("drs.client.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        with pytest.raises(httpx.TimeoutException):
            await client._get("https://example.com/test")

    assert client._client.request.call_count == 3
    assert mock_sleep.call_count == 2


@pytest.mark.asyncio
async def test_exhausted_retries_returns_last_status(config) -> None:
    """Should raise HTTPStatusError if all retries return retryable status."""
    client = DremioClient(config)
    unavailable = httpx.Response(503, request=httpx.Request("GET", "https://example.com"))

    client._client.request = AsyncMock(return_value=unavailable)

    with patch("drs.client.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(httpx.HTTPStatusError):
            await client._get("https://example.com/test")

    assert client._client.request.call_count == 3


@pytest.mark.asyncio
async def test_retry_backoff_delays(config) -> None:
    """Should use exponential backoff delays (1s, 2s)."""
    client = DremioClient(config)
    ok_response = httpx.Response(200, json={"ok": True}, request=httpx.Request("GET", "https://example.com"))

    client._client.request = AsyncMock(side_effect=[
        httpx.TimeoutException("timed out"),
        httpx.TimeoutException("timed out"),
        ok_response,
    ])

    with patch("drs.client.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        result = await client._get("https://example.com/test")

    assert result == {"ok": True}
    assert mock_sleep.call_count == 2
    mock_sleep.assert_any_call(1.0)
    mock_sleep.assert_any_call(2.0)
