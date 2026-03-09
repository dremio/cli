#
# Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
#
"""Configuration and authentication for Dremio Cloud."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import httpx
import yaml
from pydantic import BaseModel


DEFAULT_CONFIG_PATH = Path.home() / ".config" / "dremioai" / "config.yaml"
DEFAULT_URI = "https://api.dremio.cloud"


class DrsConfig(BaseModel):
    uri: str = DEFAULT_URI
    pat: str
    project_id: str


def _login(uri: str, user: str, password: str) -> str:
    """Exchange username + password for a session token via Dremio login API."""
    resp = httpx.post(
        f"{uri}/apiv2/login",
        json={"userName": user, "password": password},
        timeout=30.0,
    )
    resp.raise_for_status()
    token = resp.json().get("token")
    if not token:
        raise ValueError("Login succeeded but no token returned")
    return token


def load_config(
    config_path: Path | None = None,
    *,
    cli_token: str | None = None,
    cli_user: str | None = None,
    cli_password: str | None = None,
    cli_project_id: str | None = None,
    cli_uri: str | None = None,
) -> DrsConfig:
    """Load config with resolution order: CLI args > env vars > config file > defaults.

    Authentication priority:
      1. --user + --password CLI args → login for fresh session token
      2. --token CLI arg
      3. DREMIO_TOKEN / DREMIO_PAT env var
      4. Config file pat/token field
      5. DREMIO_USER + DREMIO_PASSWORD env vars → login for token
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

    # -- Token resolution: CLI user/pass > CLI token > env user/pass > env/file token --
    if cli_user and cli_password:
        # CLI credentials always win — login even if a token exists
        merged["pat"] = _login(merged.get("uri", DEFAULT_URI), cli_user, cli_password)
    elif cli_token:
        merged["pat"] = cli_token
    elif "pat" not in merged:
        # No token from env/file — try env user/password as last resort
        user = os.environ.get("DREMIO_USER")
        password = os.environ.get("DREMIO_PASSWORD")
        if user and password:
            merged["pat"] = _login(merged.get("uri", DEFAULT_URI), user, password)

    return DrsConfig(**merged)
