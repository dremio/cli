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
"""Tests for drs access commands — user lookup endpoint and audit."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from drs.commands.access import audit
from drs.utils import DremioAPIError


@pytest.mark.asyncio
async def test_audit_calls_correct_user_endpoint(mock_client) -> None:
    """Verify get_user_by_name hits /v1/users/name/{userName} (not /v1/user/by-name/)."""
    mock_client.get_user_by_name = AsyncMock(return_value={
        "id": "user-abc",
        "roles": [{"id": "role-1", "name": "admin"}],
    })

    result = await audit(mock_client, "rahim")

    mock_client.get_user_by_name.assert_called_once_with("rahim")
    assert result["username"] == "rahim"
    assert result["user_id"] == "user-abc"
    assert result["roles"] == [{"role_id": "role-1", "role_name": "admin"}]


@pytest.mark.asyncio
async def test_audit_user_not_found(mock_client) -> None:
    """Verify 404 from user lookup raises DremioAPIError, not raw httpx traceback."""
    request = httpx.Request("GET", "https://api.dremio.cloud/v1/users/name/nobody")
    response = httpx.Response(404, request=request)
    mock_client.get_user_by_name = AsyncMock(
        side_effect=httpx.HTTPStatusError("Not Found", request=request, response=response)
    )

    with pytest.raises(DremioAPIError) as exc_info:
        await audit(mock_client, "nobody")

    assert exc_info.value.status_code == 404
    assert "Not found" in exc_info.value.message


@pytest.mark.asyncio
async def test_audit_user_with_multiple_roles(mock_client) -> None:
    """Verify audit correctly maps multiple roles."""
    mock_client.get_user_by_name = AsyncMock(return_value={
        "id": "user-xyz",
        "roles": [
            {"id": "role-1", "name": "admin"},
            {"id": "role-2", "name": "analyst"},
            {"id": "role-3", "name": "viewer"},
        ],
    })

    result = await audit(mock_client, "testuser")

    assert len(result["roles"]) == 3
    assert result["roles"][1] == {"role_id": "role-2", "role_name": "analyst"}


@pytest.mark.asyncio
async def test_audit_expired_pat(mock_client) -> None:
    """Verify 401 from user lookup raises structured auth error."""
    request = httpx.Request("GET", "https://api.dremio.cloud/v1/users/name/rahim")
    response = httpx.Response(401, request=request)
    mock_client.get_user_by_name = AsyncMock(
        side_effect=httpx.HTTPStatusError("Unauthorized", request=request, response=response)
    )

    with pytest.raises(DremioAPIError) as exc_info:
        await audit(mock_client, "rahim")

    assert exc_info.value.status_code == 401
    assert "Authentication failed" in exc_info.value.message
