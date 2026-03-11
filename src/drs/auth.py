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
"""Configuration and authentication for Dremio Cloud."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel


DEFAULT_CONFIG_PATH = Path.home() / ".config" / "dremioai" / "config.yaml"
DEFAULT_URI = "https://api.dremio.cloud"


class DrsConfig(BaseModel):
    uri: str = DEFAULT_URI
    pat: str
    project_id: str


def load_config(
    config_path: Path | None = None,
    *,
    cli_token: str | None = None,
    cli_project_id: str | None = None,
    cli_uri: str | None = None,
) -> DrsConfig:
    """Load config with resolution order: CLI args > env vars > config file > defaults.

    Authentication priority:
      1. --token CLI arg
      2. DREMIO_TOKEN / DREMIO_PAT env var
      3. Config file pat/token field
    """
    # -- Config file (lowest priority) --
    file_values: dict[str, Any] = {}
    path = config_path or DEFAULT_CONFIG_PATH
    if path.exists():
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        file_values = {
            "uri": raw.get("uri", raw.get("endpoint")),
            "pat": raw.get("pat", raw.get("token")),
            "project_id": raw.get("project_id", raw.get("projectId")),
        }
        file_values = {k: v for k, v in file_values.items() if v is not None}

    # -- Env vars (override file) --
    env_values: dict[str, Any] = {}
    if v := os.environ.get("DREMIO_URI"):
        env_values["uri"] = v
    if v := os.environ.get("DREMIO_TOKEN"):
        env_values["pat"] = v
    elif v := os.environ.get("DREMIO_PAT"):  # legacy compat
        env_values["pat"] = v
    if v := os.environ.get("DREMIO_PROJECT_ID"):
        env_values["project_id"] = v

    # -- Merge: defaults < file < env --
    merged: dict[str, Any] = {"uri": DEFAULT_URI}
    merged.update(file_values)
    merged.update(env_values)

    # -- CLI args (highest priority, override everything) --
    if cli_uri:
        merged["uri"] = cli_uri
    if cli_project_id:
        merged["project_id"] = cli_project_id
    if cli_token:
        merged["pat"] = cli_token

    return DrsConfig(**merged)
