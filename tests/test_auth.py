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
"""Tests for drs auth/config loading."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

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
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_PROJECT_ID", "DREMIO_URI"]:
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
