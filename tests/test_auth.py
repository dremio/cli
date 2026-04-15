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
from pydantic import ValidationError

from drs.auth import (
    get_default_profile_name,
    list_profiles,
    load_config,
    set_default_profile,
)

# ---------------------------------------------------------------------------
# Legacy flat config tests (backwards compatibility)
# ---------------------------------------------------------------------------


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
    config_file.write_text(
        yaml.dump(
            {
                "pat": "file-token",
                "project_id": "file-project",
            }
        )
    )

    with patch.dict(os.environ, {}, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_PROJECT_ID", "DREMIO_URI", "DREMIO_PROFILE"]:
            os.environ.pop(k, None)
        config = load_config(config_file)

    assert config.pat == "file-token"
    assert config.project_id == "file-project"
    assert config.uri == "https://api.dremio.cloud"  # default


def test_config_env_overrides_file(tmp_path: Path) -> None:
    """Env vars should override config file values."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        yaml.dump(
            {
                "pat": "file-token",
                "project_id": "file-project",
            }
        )
    )

    with patch.dict(os.environ, {"DREMIO_TOKEN": "env-token"}, clear=False):
        os.environ.pop("DREMIO_PAT", None)
        os.environ.pop("DREMIO_PROJECT_ID", None)
        os.environ.pop("DREMIO_URI", None)
        os.environ.pop("DREMIO_PROFILE", None)
        config = load_config(config_file)

    assert config.pat == "env-token"  # env wins
    assert config.project_id == "file-project"  # file value


def test_config_missing_required_field(tmp_path: Path) -> None:
    """Missing pat or project_id should raise ValidationError."""
    with patch.dict(os.environ, {}, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_PROJECT_ID", "DREMIO_URI", "DREMIO_PROFILE"]:
            os.environ.pop(k, None)
        with pytest.raises(ValidationError):
            load_config(tmp_path / "nonexistent.yaml")


def test_config_dremio_mcp_compat(tmp_path: Path) -> None:
    """Should support dremio-mcp config format (token, endpoint, projectId)."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        yaml.dump(
            {
                "token": "mcp-token",
                "projectId": "mcp-project",
                "endpoint": "https://mcp.dremio.cloud",
            }
        )
    )

    with patch.dict(os.environ, {}, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_PROJECT_ID", "DREMIO_URI", "DREMIO_PROFILE"]:
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
    config_file.write_text(
        yaml.dump(
            {
                "pat": "file-token",
                "project_id": "file-project",
                "uri": "https://file.dremio.cloud",
            }
        )
    )

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


# ---------------------------------------------------------------------------
# Profile-based config tests
# ---------------------------------------------------------------------------


def _write_profiles_config(tmp_path: Path, profiles: dict, default_profile: str | None = None) -> Path:
    """Helper to write a profiles-format config file."""
    config_file = tmp_path / "config.yaml"
    data: dict = {"profiles": profiles}
    if default_profile:
        data["default_profile"] = default_profile
    config_file.write_text(yaml.dump(data))
    return config_file


def test_load_config_with_profiles(tmp_path: Path) -> None:
    """Should load values from the default profile."""
    config_file = _write_profiles_config(
        tmp_path,
        {
            "prod": {"pat": "prod-pat", "project_id": "prod-proj"},
            "dev": {"pat": "dev-pat", "project_id": "dev-proj", "uri": "https://api.eu.dremio.cloud"},
        },
        default_profile="prod",
    )

    with patch.dict(os.environ, {}, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_PROJECT_ID", "DREMIO_URI", "DREMIO_PROFILE"]:
            os.environ.pop(k, None)
        config = load_config(config_file)

    assert config.pat == "prod-pat"
    assert config.project_id == "prod-proj"
    assert config.uri == "https://api.dremio.cloud"


def test_load_config_profile_arg(tmp_path: Path) -> None:
    """--profile arg should select a specific profile."""
    config_file = _write_profiles_config(
        tmp_path,
        {
            "prod": {"pat": "prod-pat", "project_id": "prod-proj"},
            "dev": {"pat": "dev-pat", "project_id": "dev-proj", "uri": "https://api.eu.dremio.cloud"},
        },
        default_profile="prod",
    )

    with patch.dict(os.environ, {}, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_PROJECT_ID", "DREMIO_URI", "DREMIO_PROFILE"]:
            os.environ.pop(k, None)
        config = load_config(config_file, profile="dev")

    assert config.pat == "dev-pat"
    assert config.project_id == "dev-proj"
    assert config.uri == "https://api.eu.dremio.cloud"


def test_load_config_dremio_profile_env(tmp_path: Path) -> None:
    """DREMIO_PROFILE env var should select a profile."""
    config_file = _write_profiles_config(
        tmp_path,
        {
            "prod": {"pat": "prod-pat", "project_id": "prod-proj"},
            "dev": {"pat": "dev-pat", "project_id": "dev-proj"},
        },
        default_profile="prod",
    )

    with patch.dict(os.environ, {"DREMIO_PROFILE": "dev"}, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_PROJECT_ID", "DREMIO_URI"]:
            os.environ.pop(k, None)
        config = load_config(config_file)

    assert config.pat == "dev-pat"
    assert config.project_id == "dev-proj"


def test_profile_arg_overrides_env(tmp_path: Path) -> None:
    """CLI --profile should override DREMIO_PROFILE env var."""
    config_file = _write_profiles_config(
        tmp_path,
        {
            "prod": {"pat": "prod-pat", "project_id": "prod-proj"},
            "dev": {"pat": "dev-pat", "project_id": "dev-proj"},
        },
        default_profile="prod",
    )

    with patch.dict(os.environ, {"DREMIO_PROFILE": "prod"}, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_PROJECT_ID", "DREMIO_URI"]:
            os.environ.pop(k, None)
        config = load_config(config_file, profile="dev")

    assert config.pat == "dev-pat"


def test_env_vars_override_profile_values(tmp_path: Path) -> None:
    """Env vars (token, project_id) should override profile file values."""
    config_file = _write_profiles_config(
        tmp_path,
        {"prod": {"pat": "file-pat", "project_id": "file-proj"}},
        default_profile="prod",
    )

    with patch.dict(os.environ, {"DREMIO_TOKEN": "env-pat"}, clear=False):
        for k in ["DREMIO_PAT", "DREMIO_PROJECT_ID", "DREMIO_URI", "DREMIO_PROFILE"]:
            os.environ.pop(k, None)
        config = load_config(config_file)

    assert config.pat == "env-pat"  # env wins
    assert config.project_id == "file-proj"  # from profile


def test_profiles_fallback_to_first(tmp_path: Path) -> None:
    """When no default_profile is set, fall back to the first profile."""
    config_file = _write_profiles_config(
        tmp_path,
        {"alpha": {"pat": "a-pat", "project_id": "a-proj"}},
    )

    with patch.dict(os.environ, {}, clear=False):
        for k in ["DREMIO_TOKEN", "DREMIO_PAT", "DREMIO_PROJECT_ID", "DREMIO_URI", "DREMIO_PROFILE"]:
            os.environ.pop(k, None)
        config = load_config(config_file)

    assert config.pat == "a-pat"


# ---------------------------------------------------------------------------
# list_profiles / get_default_profile_name / set_default_profile
# ---------------------------------------------------------------------------


def test_list_profiles_with_profiles_format(tmp_path: Path) -> None:
    config_file = _write_profiles_config(
        tmp_path,
        {
            "prod": {"pat": "p1", "project_id": "proj1"},
            "dev": {"pat": "p2", "project_id": "proj2"},
        },
    )
    profiles = list_profiles(config_file)
    assert set(profiles.keys()) == {"prod", "dev"}
    assert profiles["prod"]["pat"] == "p1"


def test_list_profiles_with_legacy_flat(tmp_path: Path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump({"pat": "tok", "project_id": "proj"}))
    profiles = list_profiles(config_file)
    assert set(profiles.keys()) == {"default"}
    assert profiles["default"]["pat"] == "tok"


def test_list_profiles_empty(tmp_path: Path) -> None:
    profiles = list_profiles(tmp_path / "nonexistent.yaml")
    assert profiles == {}


def test_get_default_profile_name_profiles(tmp_path: Path) -> None:
    config_file = _write_profiles_config(
        tmp_path,
        {"prod": {"pat": "p1", "project_id": "proj1"}},
        default_profile="prod",
    )
    assert get_default_profile_name(config_file) == "prod"


def test_get_default_profile_name_legacy(tmp_path: Path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump({"pat": "tok", "project_id": "proj"}))
    assert get_default_profile_name(config_file) == "default"


def test_set_default_profile(tmp_path: Path) -> None:
    config_file = _write_profiles_config(
        tmp_path,
        {
            "prod": {"pat": "p1", "project_id": "proj1"},
            "dev": {"pat": "p2", "project_id": "proj2"},
        },
        default_profile="prod",
    )
    set_default_profile("dev", config_file)
    assert get_default_profile_name(config_file) == "dev"


def test_set_default_profile_not_found(tmp_path: Path) -> None:
    config_file = _write_profiles_config(
        tmp_path,
        {"prod": {"pat": "p1", "project_id": "proj1"}},
        default_profile="prod",
    )
    with pytest.raises(ValueError, match="not found"):
        set_default_profile("nonexistent", config_file)
