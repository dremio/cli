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
"""Tests for dremio setup command."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest
import yaml

from drs.auth import DEFAULT_URI
from drs.commands.setup import validate_credentials, write_config


def test_write_config(tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    write_config("https://api.eu.dremio.cloud", "my-pat", "my-project", config_path)

    data = yaml.safe_load(config_path.read_text())
    assert data["uri"] == "https://api.eu.dremio.cloud"
    assert data["pat"] == "my-pat"
    assert data["project_id"] == "my-project"
    # File should be owner-only readable
    assert oct(config_path.stat().st_mode & 0o777) == "0o600"


def test_write_config_omits_default_uri(tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    write_config(DEFAULT_URI, "my-pat", "my-project", config_path)

    data = yaml.safe_load(config_path.read_text())
    assert "uri" not in data
    assert data["pat"] == "my-pat"
    assert data["project_id"] == "my-project"


def test_write_config_creates_dirs(tmp_path) -> None:
    config_path = tmp_path / "nested" / "deep" / "config.yaml"
    write_config(DEFAULT_URI, "my-pat", "my-project", config_path)

    assert config_path.exists()
    data = yaml.safe_load(config_path.read_text())
    assert data["pat"] == "my-pat"


@pytest.mark.asyncio
async def test_validate_credentials_success() -> None:
    mock_client = AsyncMock()
    mock_client.get_project = AsyncMock(return_value={"id": "p1", "name": "My Project"})
    mock_client.close = AsyncMock()

    with patch("drs.commands.setup.DremioClient", return_value=mock_client):
        ok, msg, data = await validate_credentials(DEFAULT_URI, "good-pat", "p1")

    assert ok is True
    assert "My Project" in msg
    assert data["name"] == "My Project"


@pytest.mark.asyncio
async def test_validate_credentials_bad_pat() -> None:
    mock_client = AsyncMock()
    response = httpx.Response(401, request=httpx.Request("GET", "https://api.dremio.cloud"))
    mock_client.get_project = AsyncMock(side_effect=httpx.HTTPStatusError("Unauthorized", request=response.request, response=response))
    mock_client.close = AsyncMock()

    with patch("drs.commands.setup.DremioClient", return_value=mock_client):
        ok, msg, data = await validate_credentials(DEFAULT_URI, "bad-pat", "p1")

    assert ok is False
    assert "PAT" in msg or "Authentication" in msg
    assert data is None


@pytest.mark.asyncio
async def test_validate_credentials_bad_project() -> None:
    mock_client = AsyncMock()
    response = httpx.Response(404, request=httpx.Request("GET", "https://api.dremio.cloud"))
    mock_client.get_project = AsyncMock(side_effect=httpx.HTTPStatusError("Not Found", request=response.request, response=response))
    mock_client.close = AsyncMock()

    with patch("drs.commands.setup.DremioClient", return_value=mock_client):
        ok, msg, data = await validate_credentials(DEFAULT_URI, "good-pat", "bad-project")

    assert ok is False
    assert "Project" in msg
    assert data is None


@pytest.mark.asyncio
async def test_validate_credentials_connection_error() -> None:
    mock_client = AsyncMock()
    mock_client.get_project = AsyncMock(side_effect=httpx.ConnectError("Connection refused"))
    mock_client.close = AsyncMock()

    with patch("drs.commands.setup.DremioClient", return_value=mock_client):
        ok, msg, data = await validate_credentials("https://api.bad.dremio.cloud", "pat", "p1")

    assert ok is False
    assert "Cannot reach" in msg
    assert data is None
