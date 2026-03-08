"""Configuration and authentication for Dremio Cloud."""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel


DEFAULT_CONFIG_PATH = Path.home() / ".config" / "dremioai" / "config.yaml"
DEFAULT_URI = "https://api.dremio.cloud"


class DrsConfig(BaseModel):
    uri: str = DEFAULT_URI
    pat: str
    project_id: str


def load_config(config_path: Path | None = None) -> DrsConfig:
    """Load config with resolution order: env vars > config file > defaults."""
    file_values: dict = {}
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

    env_values: dict = {}
    if v := os.environ.get("DREMIO_URI"):
        env_values["uri"] = v
    if v := os.environ.get("DREMIO_PAT"):
        env_values["pat"] = v
    if v := os.environ.get("DREMIO_PROJECT_ID"):
        env_values["project_id"] = v

    merged = {"uri": DEFAULT_URI}
    merged.update(file_values)
    merged.update(env_values)

    return DrsConfig(**merged)
