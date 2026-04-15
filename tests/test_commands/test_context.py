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
"""Tests for dremio context command."""

from __future__ import annotations

from pathlib import Path

import yaml
from typer.testing import CliRunner

from drs.cli import app

runner = CliRunner()


def _write_config(tmp_path: Path, data: dict) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(data))
    return config_path


def test_context_list(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        {
            "default_profile": "prod",
            "profiles": {
                "prod": {"pat": "p1", "project_id": "proj-1"},
                "dev": {"pat": "p2", "project_id": "proj-2", "uri": "https://api.eu.dremio.cloud"},
            },
        },
    )
    result = runner.invoke(app, ["--config", str(config_path), "context", "list"])
    assert result.exit_code == 0
    assert "prod" in result.output
    assert "dev" in result.output
    assert "EU" in result.output


def test_context_list_empty(tmp_path: Path) -> None:
    config_path = tmp_path / "nonexistent.yaml"
    result = runner.invoke(app, ["--config", str(config_path), "context", "list"])
    assert result.exit_code == 1
    assert "No profiles" in result.output


def test_context_use(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        {
            "default_profile": "prod",
            "profiles": {
                "prod": {"pat": "p1", "project_id": "proj-1"},
                "dev": {"pat": "p2", "project_id": "proj-2"},
            },
        },
    )
    result = runner.invoke(app, ["--config", str(config_path), "context", "use", "dev"])
    assert result.exit_code == 0
    assert "dev" in result.output

    # Verify the file was updated
    data = yaml.safe_load(config_path.read_text())
    assert data["default_profile"] == "dev"


def test_context_use_not_found(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        {
            "default_profile": "prod",
            "profiles": {"prod": {"pat": "p1", "project_id": "proj-1"}},
        },
    )
    result = runner.invoke(app, ["--config", str(config_path), "context", "use", "nonexistent"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_context_current(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        {
            "default_profile": "staging",
            "profiles": {
                "staging": {"pat": "p1", "project_id": "proj-1"},
            },
        },
    )
    result = runner.invoke(app, ["--config", str(config_path), "context", "current"])
    assert result.exit_code == 0
    assert "staging" in result.output


def test_context_current_no_config(tmp_path: Path) -> None:
    config_path = tmp_path / "nonexistent.yaml"
    result = runner.invoke(app, ["--config", str(config_path), "context", "current"])
    assert result.exit_code == 1
    assert "No profiles" in result.output
