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
from drs.commands.setup import _slugify, validate_credentials, write_profile

runner = CliRunner()

# -- Unit tests for helpers --


def test_write_profile_new_file(tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    write_profile("https://api.eu.dremio.cloud", "my-pat", "my-project", "eu-prod", True, config_path)

    data = yaml.safe_load(config_path.read_text())
    assert data["default_profile"] == "eu-prod"
    assert data["profiles"]["eu-prod"]["uri"] == "https://api.eu.dremio.cloud"
    assert data["profiles"]["eu-prod"]["pat"] == "my-pat"
    assert data["profiles"]["eu-prod"]["project_id"] == "my-project"
    assert oct(config_path.stat().st_mode & 0o777) == "0o600"


def test_write_profile_omits_default_uri(tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    write_profile(DEFAULT_URI, "my-pat", "my-project", "us-prod", True, config_path)

    data = yaml.safe_load(config_path.read_text())
    assert "uri" not in data["profiles"]["us-prod"]
    assert data["profiles"]["us-prod"]["pat"] == "my-pat"


def test_write_profile_creates_dirs(tmp_path) -> None:
    config_path = tmp_path / "nested" / "deep" / "config.yaml"
    write_profile(DEFAULT_URI, "my-pat", "my-project", "test", True, config_path)

    assert config_path.exists()
    data = yaml.safe_load(config_path.read_text())
    assert data["profiles"]["test"]["pat"] == "my-pat"


def test_write_profile_preserves_existing(tmp_path) -> None:
    """Adding a second profile should not overwrite the first."""
    config_path = tmp_path / "config.yaml"
    write_profile(DEFAULT_URI, "pat-1", "proj-1", "first", True, config_path)
    write_profile("https://api.eu.dremio.cloud", "pat-2", "proj-2", "second", False, config_path)

    data = yaml.safe_load(config_path.read_text())
    assert "first" in data["profiles"]
    assert "second" in data["profiles"]
    assert data["profiles"]["first"]["pat"] == "pat-1"
    assert data["profiles"]["second"]["pat"] == "pat-2"
    assert data["default_profile"] == "first"  # not changed


def test_write_profile_migrates_legacy(tmp_path) -> None:
    """Legacy flat config should be migrated into a 'default' profile."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump({"pat": "old-pat", "project_id": "old-proj"}))

    write_profile(DEFAULT_URI, "new-pat", "new-proj", "new-profile", False, config_path)

    data = yaml.safe_load(config_path.read_text())
    assert "default" in data["profiles"]
    assert data["profiles"]["default"]["pat"] == "old-pat"
    assert "new-profile" in data["profiles"]
    assert data["default_profile"] == "default"  # legacy becomes default


def test_write_profile_set_default(tmp_path) -> None:
    """set_default=True should update default_profile."""
    config_path = tmp_path / "config.yaml"
    write_profile(DEFAULT_URI, "pat-1", "proj-1", "first", True, config_path)
    write_profile(DEFAULT_URI, "pat-2", "proj-2", "second", True, config_path)

    data = yaml.safe_load(config_path.read_text())
    assert data["default_profile"] == "second"


def test_slugify() -> None:
    assert _slugify("My Project") == "my-project"
    assert _slugify("Production Analytics") == "production-analytics"
    assert _slugify("dev_sandbox-123") == "dev-sandbox-123"
    assert _slugify("  ") == "default"


# -- Validation tests --


@pytest.mark.asyncio
async def test_validate_credentials_success() -> None:
    mock_client = AsyncMock()
    mock_client.list_projects = AsyncMock(return_value={"data": [{"id": "p1", "name": "My Project"}]})
    mock_client.close = AsyncMock()

    with patch("drs.commands.setup.DremioClient", return_value=mock_client):
        ok, msg, projects = await validate_credentials(DEFAULT_URI, "good-pat")

    assert ok is True
    assert "1 project" in msg
    assert projects[0]["name"] == "My Project"


@pytest.mark.asyncio
async def test_validate_credentials_multiple_projects() -> None:
    mock_client = AsyncMock()
    mock_client.list_projects = AsyncMock(
        return_value={
            "data": [
                {"id": "p1", "name": "Prod"},
                {"id": "p2", "name": "Dev"},
            ]
        }
    )
    mock_client.close = AsyncMock()

    with patch("drs.commands.setup.DremioClient", return_value=mock_client):
        ok, msg, projects = await validate_credentials(DEFAULT_URI, "good-pat")

    assert ok is True
    assert "2 project" in msg
    assert len(projects) == 2


@pytest.mark.asyncio
async def test_validate_credentials_bad_pat() -> None:
    mock_client = AsyncMock()
    response = httpx.Response(401, request=httpx.Request("GET", "https://api.dremio.cloud"))
    mock_client.list_projects = AsyncMock(
        side_effect=httpx.HTTPStatusError("Unauthorized", request=response.request, response=response)
    )
    mock_client.close = AsyncMock()

    with patch("drs.commands.setup.DremioClient", return_value=mock_client):
        ok, msg, projects = await validate_credentials(DEFAULT_URI, "bad-pat")

    assert ok is False
    assert "PAT" in msg or "Authentication" in msg
    assert projects is None


@pytest.mark.asyncio
async def test_validate_credentials_forbidden() -> None:
    mock_client = AsyncMock()
    response = httpx.Response(403, request=httpx.Request("GET", "https://api.dremio.cloud"))
    mock_client.list_projects = AsyncMock(
        side_effect=httpx.HTTPStatusError("Forbidden", request=response.request, response=response)
    )
    mock_client.close = AsyncMock()

    with patch("drs.commands.setup.DremioClient", return_value=mock_client):
        ok, msg, projects = await validate_credentials(DEFAULT_URI, "limited-pat")

    assert ok is False
    assert "Access denied" in msg
    assert projects is None


@pytest.mark.asyncio
async def test_validate_credentials_connection_error() -> None:
    mock_client = AsyncMock()
    mock_client.list_projects = AsyncMock(side_effect=httpx.ConnectError("Connection refused"))
    mock_client.close = AsyncMock()

    with patch("drs.commands.setup.DremioClient", return_value=mock_client):
        ok, msg, projects = await validate_credentials("https://api.bad.dremio.cloud", "pat")

    assert ok is False
    assert "Cannot reach" in msg
    assert projects is None


@pytest.mark.asyncio
async def test_validate_credentials_no_projects() -> None:
    mock_client = AsyncMock()
    mock_client.list_projects = AsyncMock(return_value={"data": []})
    mock_client.close = AsyncMock()

    with patch("drs.commands.setup.DremioClient", return_value=mock_client):
        ok, msg, _projects = await validate_credentials(DEFAULT_URI, "good-pat")

    assert ok is False
    assert "No projects" in msg


# -- CLI integration tests --


def test_setup_non_interactive(tmp_path) -> None:
    """Non-TTY stdin should print instructions and exit 1."""
    with patch("drs.commands.setup.sys") as mock_sys:
        mock_sys.stdin.isatty.return_value = False
        result = runner.invoke(app, ["setup"])

    assert result.exit_code == 1
    assert "interactive terminal" in result.output or "DREMIO_TOKEN" in result.output


def test_setup_happy_path(tmp_path) -> None:
    """Full wizard flow: region, PAT, project discovery, profile naming."""
    config_path = tmp_path / "config.yaml"

    mock_client = AsyncMock()
    mock_client.list_projects = AsyncMock(return_value={"data": [{"id": "p1", "name": "Test Project"}]})
    mock_client.close = AsyncMock()

    with (
        patch("drs.commands.setup.sys") as mock_sys,
        patch("drs.commands.setup.DremioClient", return_value=mock_client),
        patch("drs.commands.setup.DEFAULT_CONFIG_PATH", config_path),
    ):
        mock_sys.stdin.isatty.return_value = True
        # Input: region=1, PAT=test-pat, profile_name=test-project (accept default)
        result = runner.invoke(app, ["setup"], input="1\ntest-pat\ntest-project\n")

    assert result.exit_code == 0
    assert "Setup complete" in result.output
    assert config_path.exists()
    data = yaml.safe_load(config_path.read_text())
    assert data["profiles"]["test-project"]["pat"] == "test-pat"
    assert data["profiles"]["test-project"]["project_id"] == "p1"


def test_setup_multiple_projects(tmp_path) -> None:
    """Should let user pick from multiple discovered projects."""
    config_path = tmp_path / "config.yaml"

    mock_client = AsyncMock()
    mock_client.list_projects = AsyncMock(
        return_value={
            "data": [
                {"id": "p1", "name": "Production"},
                {"id": "p2", "name": "Dev Sandbox"},
            ]
        }
    )
    mock_client.close = AsyncMock()

    with (
        patch("drs.commands.setup.sys") as mock_sys,
        patch("drs.commands.setup.DremioClient", return_value=mock_client),
        patch("drs.commands.setup.DEFAULT_CONFIG_PATH", config_path),
    ):
        mock_sys.stdin.isatty.return_value = True
        # Input: region=1, PAT=my-pat, pick project 2, profile_name=dev
        result = runner.invoke(app, ["setup"], input="1\nmy-pat\n2\ndev\n")

    assert result.exit_code == 0
    data = yaml.safe_load(config_path.read_text())
    assert data["profiles"]["dev"]["project_id"] == "p2"


def test_setup_retry_then_abort(tmp_path) -> None:
    """Validation failure followed by declining retry should exit 1."""
    config_path = tmp_path / "config.yaml"

    mock_client = AsyncMock()
    response = httpx.Response(401, request=httpx.Request("GET", "https://api.dremio.cloud"))
    mock_client.list_projects = AsyncMock(
        side_effect=httpx.HTTPStatusError("Unauthorized", request=response.request, response=response)
    )
    mock_client.close = AsyncMock()

    with (
        patch("drs.commands.setup.sys") as mock_sys,
        patch("drs.commands.setup.DremioClient", return_value=mock_client),
        patch("drs.commands.setup.DEFAULT_CONFIG_PATH", config_path),
    ):
        mock_sys.stdin.isatty.return_value = True
        # Input: region=1, PAT=bad, then decline retry
        result = runner.invoke(app, ["setup"], input="1\nbad-pat\nn\n")

    assert result.exit_code == 1
    assert "cancelled" in result.output.lower()
    assert not config_path.exists()


def test_setup_global_config_passthrough(tmp_path) -> None:
    """dremio --config /custom/path setup should write to the custom path."""
    config_path = tmp_path / "custom.yaml"

    mock_client = AsyncMock()
    mock_client.list_projects = AsyncMock(return_value={"data": [{"id": "p1", "name": "Test"}]})
    mock_client.close = AsyncMock()

    with (
        patch("drs.commands.setup.sys") as mock_sys,
        patch("drs.commands.setup.DremioClient", return_value=mock_client),
    ):
        mock_sys.stdin.isatty.return_value = True
        # Input: region=1, PAT=my-pat, profile_name=test
        result = runner.invoke(app, ["--config", str(config_path), "setup"], input="1\nmy-pat\ntest\n")

    assert result.exit_code == 0
    assert config_path.exists()
    data = yaml.safe_load(config_path.read_text())
    assert data["profiles"]["test"]["pat"] == "my-pat"


def test_write_profile_includes_header(tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    write_profile(DEFAULT_URI, "my-pat", "my-project", "test", True, config_path)

    raw = config_path.read_text()
    assert raw.startswith("# Dremio CLI config")
    assert "plaintext" in raw
