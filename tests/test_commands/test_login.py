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
"""Tests for dremio login / dremio logout commands."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from drs.cli import app
from drs.token_store import OAuthTokens, load, save

runner = CliRunner()


def test_login_saves_tokens(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("project_id: proj-1\n")
    store_path = tmp_path / "oauth_tokens.yaml"

    fake_tokens = OAuthTokens(
        access_token="at-new",
        refresh_token="rt-new",
        client_id="cid",
    )

    with (
        patch("drs.commands.login.oauth.run_login_flow", return_value=fake_tokens),
        patch("drs.commands.login.token_store.save") as mock_save,
        patch("drs.commands.login.DEFAULT_CONFIG_PATH", config_path),
        patch("drs.commands.login._update_config_file"),
    ):
        result = runner.invoke(app, ["--config", str(config_path), "login"])

    assert result.exit_code == 0
    assert "successfully" in result.output.lower() or "logged in" in result.output.lower()
    mock_save.assert_called_once()
    saved_tokens = mock_save.call_args[0][1]
    assert saved_tokens.access_token == "at-new"


def test_login_picks_project_from_list(tmp_path: Path) -> None:
    """When project_id is not in config, login should show project list."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("uri: https://api.dremio.cloud\n")

    fake_tokens = OAuthTokens(access_token="at", client_id="cid")
    fake_projects = [
        {"id": "proj-aaa", "name": "Alpha"},
        {"id": "proj-bbb", "name": "Beta"},
    ]

    with (
        patch("drs.commands.login.oauth.run_login_flow", return_value=fake_tokens),
        patch("drs.commands.login.token_store.save"),
        patch("drs.commands.login.DEFAULT_CONFIG_PATH", config_path),
        patch("drs.commands.login._update_config_file") as mock_update,
        patch("drs.commands.login._fetch_projects", return_value=fake_projects),
    ):
        result = runner.invoke(app, ["--config", str(config_path), "login"], input="2\n")

    assert result.exit_code == 0
    assert "Beta" in result.output
    mock_update.assert_called_once()
    assert mock_update.call_args[0][2] == "proj-bbb"


def test_login_auto_selects_single_project(tmp_path: Path) -> None:
    """When only one project exists, auto-select it."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("uri: https://api.dremio.cloud\n")

    fake_tokens = OAuthTokens(access_token="at", client_id="cid")
    fake_projects = [{"id": "proj-only", "name": "OnlyProject"}]

    with (
        patch("drs.commands.login.oauth.run_login_flow", return_value=fake_tokens),
        patch("drs.commands.login.token_store.save"),
        patch("drs.commands.login.DEFAULT_CONFIG_PATH", config_path),
        patch("drs.commands.login._update_config_file") as mock_update,
        patch("drs.commands.login._fetch_projects", return_value=fake_projects),
    ):
        result = runner.invoke(app, ["--config", str(config_path), "login"])

    assert result.exit_code == 0
    assert "OnlyProject" in result.output
    mock_update.assert_called_once()
    assert mock_update.call_args[0][2] == "proj-only"


def test_logout_clears_tokens(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("uri: https://api.dremio.cloud\nproject_id: proj-1\n")

    with (
        patch("drs.commands.login.token_store.clear") as mock_clear,
        patch("drs.commands.login.DEFAULT_CONFIG_PATH", config_path),
    ):
        result = runner.invoke(app, ["--config", str(config_path), "logout"])

    assert result.exit_code == 0
    assert "logged out" in result.output.lower() or "removed" in result.output.lower()
    mock_clear.assert_called_once_with("https://api.dremio.cloud")


def test_login_failure_exits_1(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("project_id: proj-1\n")

    with (
        patch("drs.commands.login.oauth.run_login_flow", side_effect=RuntimeError("network error")),
        patch("drs.commands.login.DEFAULT_CONFIG_PATH", config_path),
    ):
        result = runner.invoke(app, ["--config", str(config_path), "login"])

    assert result.exit_code == 1
    assert "failed" in result.output.lower()
