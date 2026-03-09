#
# Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
#
"""Tests for drs auth/config loading."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import yaml

from drs.auth import DrsConfig, load_config


def test_config_from_env_vars(tmp_path: Path) -> None:
    """Env vars should take precedence over file."""
    env = {
        "DREMIO_TOKEN": "env-token",
        "DREMIO_PROJECT_ID": "env-project",
        "DREMIO_URI": "https://custom.dremio.cloud",
    }
    with patch.dict(os.environ, env, clear=False):
        config = load_config(tmp_path / "nonexistent.yaml")

    assert config.pat == "env-token"
    assert config.project_id == "env-project"
    assert config.uri == "https://custom.dremio.cloud"


def test_config_from_file(tmp_path: Path) -> None:
    """Config file values should load correctly."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump({
        "pat": "file-token",
        "project_id": "file-project",
    }))

    with patch.dict(os.environ, {}, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_PROJECT_ID", "DREMIO_URI"]:
            os.environ.pop(k, None)
        config = load_config(config_file)

    assert config.pat == "file-token"
    assert config.project_id == "file-project"
    assert config.uri == "https://api.dremio.cloud"  # default


def test_config_env_overrides_file(tmp_path: Path) -> None:
    """Env vars should override config file values."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump({
        "pat": "file-token",
        "project_id": "file-project",
    }))

    with patch.dict(os.environ, {"DREMIO_TOKEN": "env-token"}, clear=False):
        os.environ.pop("DREMIO_PAT", None)
        os.environ.pop("DREMIO_PROJECT_ID", None)
        os.environ.pop("DREMIO_URI", None)
        config = load_config(config_file)

    assert config.pat == "env-token"  # env wins
    assert config.project_id == "file-project"  # file value


def test_config_missing_required_field(tmp_path: Path) -> None:
    """Missing pat or project_id should raise ValidationError."""
    with patch.dict(os.environ, {}, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_PROJECT_ID", "DREMIO_URI",
                   "DREMIO_USER", "DREMIO_PASSWORD"]:
            os.environ.pop(k, None)
        with pytest.raises(Exception):
            load_config(tmp_path / "nonexistent.yaml")


def test_config_dremio_mcp_compat(tmp_path: Path) -> None:
    """Should support dremio-mcp config format (token, endpoint, projectId)."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump({
        "token": "mcp-token",
        "projectId": "mcp-project",
        "endpoint": "https://mcp.dremio.cloud",
    }))

    with patch.dict(os.environ, {}, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_PROJECT_ID", "DREMIO_URI"]:
            os.environ.pop(k, None)
        config = load_config(config_file)

    assert config.pat == "mcp-token"
    assert config.project_id == "mcp-project"
    assert config.uri == "https://mcp.dremio.cloud"


def test_legacy_dremio_pat_env(tmp_path: Path) -> None:
    """DREMIO_PAT should still work as legacy env var."""
    with patch.dict(os.environ, {"DREMIO_PAT": "legacy-pat", "DREMIO_PROJECT_ID": "proj"}, clear=False):
        os.environ.pop("DREMIO_TOKEN", None)
        config = load_config(tmp_path / "nonexistent.yaml")

    assert config.pat == "legacy-pat"


def test_dremio_token_overrides_dremio_pat(tmp_path: Path) -> None:
    """DREMIO_TOKEN takes priority over DREMIO_PAT."""
    env = {"DREMIO_TOKEN": "new-token", "DREMIO_PAT": "old-pat", "DREMIO_PROJECT_ID": "proj"}
    with patch.dict(os.environ, env, clear=False):
        config = load_config(tmp_path / "nonexistent.yaml")

    assert config.pat == "new-token"


def test_cli_args_override_env(tmp_path: Path) -> None:
    """CLI args should override env vars and file values."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump({
        "pat": "file-token",
        "project_id": "file-project",
        "uri": "https://file.dremio.cloud",
    }))

    env = {"DREMIO_TOKEN": "env-token", "DREMIO_PROJECT_ID": "env-project"}
    with patch.dict(os.environ, env, clear=False):
        config = load_config(
            config_file,
            cli_token="cli-token",
            cli_project_id="cli-project",
            cli_uri="https://api.eu.dremio.cloud",
        )

    assert config.pat == "cli-token"
    assert config.project_id == "cli-project"
    assert config.uri == "https://api.eu.dremio.cloud"


def test_user_password_login(tmp_path: Path) -> None:
    """--user + --password should call login API to get token."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"token": "session-token-123"}
    mock_response.raise_for_status = MagicMock()

    with patch.dict(os.environ, {"DREMIO_PROJECT_ID": "proj"}, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_USER", "DREMIO_PASSWORD"]:
            os.environ.pop(k, None)
        with patch("drs.auth.httpx.post", return_value=mock_response) as mock_post:
            config = load_config(
                tmp_path / "nonexistent.yaml",
                cli_user="admin",
                cli_password="secret",
            )

    assert config.pat == "session-token-123"
    mock_post.assert_called_once_with(
        "https://api.dremio.cloud/apiv2/login",
        json={"userName": "admin", "password": "secret"},
        timeout=30.0,
    )


def test_user_password_from_env(tmp_path: Path) -> None:
    """DREMIO_USER + DREMIO_PASSWORD env vars should trigger login."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"token": "env-session-token"}
    mock_response.raise_for_status = MagicMock()

    env = {"DREMIO_USER": "envuser", "DREMIO_PASSWORD": "envpass", "DREMIO_PROJECT_ID": "proj"}
    with patch.dict(os.environ, env, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT"]:
            os.environ.pop(k, None)
        with patch("drs.auth.httpx.post", return_value=mock_response):
            config = load_config(tmp_path / "nonexistent.yaml")

    assert config.pat == "env-session-token"


def test_cli_user_password_overrides_cli_token(tmp_path: Path) -> None:
    """--user/--password should override --token (fresh login wins over stale PAT)."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"token": "fresh-session"}
    mock_response.raise_for_status = MagicMock()

    env = {"DREMIO_PROJECT_ID": "proj"}
    with patch.dict(os.environ, env, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_USER", "DREMIO_PASSWORD"]:
            os.environ.pop(k, None)
        with patch("drs.auth.httpx.post", return_value=mock_response) as mock_post:
            config = load_config(
                tmp_path / "nonexistent.yaml",
                cli_token="stale-token",
                cli_user="admin",
                cli_password="secret",
            )

    assert config.pat == "fresh-session"
    mock_post.assert_called_once()


def test_cli_token_used_without_user_password(tmp_path: Path) -> None:
    """--token should be used when no --user/--password provided."""
    env = {"DREMIO_PROJECT_ID": "proj"}
    with patch.dict(os.environ, env, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_USER", "DREMIO_PASSWORD"]:
            os.environ.pop(k, None)
        with patch("drs.auth.httpx.post") as mock_post:
            config = load_config(
                tmp_path / "nonexistent.yaml",
                cli_token="direct-token",
            )

    assert config.pat == "direct-token"
    mock_post.assert_not_called()
