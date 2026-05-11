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
from typer.testing import CliRunner

from drs.auth import DEFAULT_URI
from drs.cli import app
from drs.commands.setup import validate_credentials, write_config
from drs.token_store import OAuthTokens

runner = CliRunner()


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
    mock_client.get_project = AsyncMock(
        side_effect=httpx.HTTPStatusError("Unauthorized", request=response.request, response=response)
    )
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
    mock_client.get_project = AsyncMock(
        side_effect=httpx.HTTPStatusError("Not Found", request=response.request, response=response)
    )
    mock_client.close = AsyncMock()

    with patch("drs.commands.setup.DremioClient", return_value=mock_client):
        ok, msg, data = await validate_credentials(DEFAULT_URI, "good-pat", "bad-project")

    assert ok is False
    assert "Project" in msg
    assert data is None


@pytest.mark.asyncio
async def test_validate_credentials_forbidden() -> None:
    mock_client = AsyncMock()
    response = httpx.Response(403, request=httpx.Request("GET", "https://api.dremio.cloud"))
    mock_client.get_project = AsyncMock(
        side_effect=httpx.HTTPStatusError("Forbidden", request=response.request, response=response)
    )
    mock_client.close = AsyncMock()

    with patch("drs.commands.setup.DremioClient", return_value=mock_client):
        ok, msg, data = await validate_credentials(DEFAULT_URI, "limited-pat", "p1")

    assert ok is False
    assert "Access denied" in msg
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


def test_write_config_includes_header(tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    write_config(DEFAULT_URI, "my-pat", "my-project", config_path)

    raw = config_path.read_text()
    assert raw.startswith("# Dremio CLI config")
    assert "mode 600" in raw


def test_setup_non_interactive(tmp_path) -> None:
    """Non-TTY stdin should print instructions and exit 1."""
    with patch("drs.commands.setup.sys") as mock_sys:
        mock_sys.stdin.isatty.return_value = False
        result = runner.invoke(app, ["setup"])

    assert result.exit_code == 1
    assert "interactive terminal" in result.output or "DREMIO_TOKEN" in result.output


def test_setup_happy_path(tmp_path) -> None:
    """Full wizard flow: region, PAT, project ID, validation, config write."""
    config_path = tmp_path / "config.yaml"

    mock_client = AsyncMock()
    mock_client.get_project = AsyncMock(return_value={"id": "p1", "name": "Test Project"})
    mock_client.close = AsyncMock()

    with (
        patch("drs.commands.setup.sys") as mock_sys,
        patch("drs.commands.setup.DremioClient", return_value=mock_client),
        patch("drs.commands.setup.DEFAULT_CONFIG_PATH", config_path),
    ):
        mock_sys.stdin.isatty.return_value = True
        # Input: region=1, auth=2(PAT), PAT=test-pat, project_id=test-proj
        result = runner.invoke(app, ["setup"], input="1\n2\ntest-pat\ntest-proj\n")

    assert result.exit_code == 0
    assert "Setup complete" in result.output
    assert config_path.exists()
    data = yaml.safe_load(config_path.read_text())
    assert data["pat"] == "test-pat"
    assert data["project_id"] == "test-proj"


def test_setup_existing_config_decline(tmp_path) -> None:
    """Declining to overwrite existing config should abort."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("pat: old\n")

    with (
        patch("drs.commands.setup.sys") as mock_sys,
        patch("drs.commands.setup.DEFAULT_CONFIG_PATH", config_path),
    ):
        mock_sys.stdin.isatty.return_value = True
        # Input: decline overwrite (n)
        result = runner.invoke(app, ["setup"], input="n\n")

    assert result.exit_code == 0
    assert "cancelled" in result.output.lower()
    # Config should be unchanged
    assert config_path.read_text() == "pat: old\n"


def test_setup_existing_config_overwrite(tmp_path) -> None:
    """Accepting overwrite should proceed with the wizard."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("pat: old\n")

    mock_client = AsyncMock()
    mock_client.get_project = AsyncMock(return_value={"id": "p1", "name": "New Project"})
    mock_client.close = AsyncMock()

    with (
        patch("drs.commands.setup.sys") as mock_sys,
        patch("drs.commands.setup.DremioClient", return_value=mock_client),
        patch("drs.commands.setup.DEFAULT_CONFIG_PATH", config_path),
    ):
        mock_sys.stdin.isatty.return_value = True
        # Input: overwrite=y, region=1, auth=2(PAT), PAT=new-pat, project_id=new-proj
        result = runner.invoke(app, ["setup"], input="y\n1\n2\nnew-pat\nnew-proj\n")

    assert result.exit_code == 0
    data = yaml.safe_load(config_path.read_text())
    assert data["pat"] == "new-pat"


def test_setup_retry_then_abort(tmp_path) -> None:
    """Validation failure followed by declining retry should exit 1."""
    config_path = tmp_path / "config.yaml"

    mock_client = AsyncMock()
    response = httpx.Response(401, request=httpx.Request("GET", "https://api.dremio.cloud"))
    mock_client.get_project = AsyncMock(
        side_effect=httpx.HTTPStatusError("Unauthorized", request=response.request, response=response)
    )
    mock_client.close = AsyncMock()

    with (
        patch("drs.commands.setup.sys") as mock_sys,
        patch("drs.commands.setup.DremioClient", return_value=mock_client),
        patch("drs.commands.setup.DEFAULT_CONFIG_PATH", config_path),
    ):
        mock_sys.stdin.isatty.return_value = True
        # Input: region=1, auth=2(PAT), PAT=bad, project_id=p1, then decline retry
        result = runner.invoke(app, ["setup"], input="1\n2\nbad-pat\np1\nn\n")

    assert result.exit_code == 1
    assert "cancelled" in result.output.lower()
    assert not config_path.exists()


def test_setup_global_config_passthrough(tmp_path) -> None:
    """dremio --config /custom/path setup should write to the custom path."""
    config_path = tmp_path / "custom.yaml"

    mock_client = AsyncMock()
    mock_client.get_project = AsyncMock(return_value={"id": "p1", "name": "Test"})
    mock_client.close = AsyncMock()

    with (
        patch("drs.commands.setup.sys") as mock_sys,
        patch("drs.commands.setup.DremioClient", return_value=mock_client),
    ):
        mock_sys.stdin.isatty.return_value = True
        result = runner.invoke(app, ["--config", str(config_path), "setup"], input="1\n2\nmy-pat\nmy-proj\n")

    assert result.exit_code == 0
    assert config_path.exists()
    data = yaml.safe_load(config_path.read_text())
    assert data["pat"] == "my-pat"


def test_setup_oauth_path(tmp_path) -> None:
    """OAuth path through setup wizard: region, auth=1(OAuth), project_id."""
    config_path = tmp_path / "config.yaml"

    fake_tokens = OAuthTokens(access_token="at-oauth", client_id="cid")

    with (
        patch("drs.commands.setup.sys") as mock_sys,
        patch("drs.oauth.run_login_flow", return_value=fake_tokens),
        patch("drs.token_store.save") as mock_save,
        patch("drs.commands.setup.DEFAULT_CONFIG_PATH", config_path),
    ):
        mock_sys.stdin.isatty.return_value = True
        # Input: region=1, auth=1(OAuth), project_id=my-proj
        result = runner.invoke(app, ["setup"], input="1\n1\nmy-proj\n")

    assert result.exit_code == 0
    assert config_path.exists()
    data = yaml.safe_load(config_path.read_text())
    assert data.get("auth_method") == "oauth"
    assert "pat" not in data
    assert data["project_id"] == "my-proj"
    mock_save.assert_called_once()
