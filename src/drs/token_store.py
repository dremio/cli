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
"""Persistent storage for OAuth tokens, keyed by Dremio URL."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel

STORE_PATH = Path.home() / ".config" / "dremioai" / "oauth_tokens.yaml"


class OAuthTokens(BaseModel):
    access_token: str
    refresh_token: str | None = None
    expires_at: float | None = None
    client_id: str
    client_secret: str | None = None


def load(dremio_url: str, store_path: Path = STORE_PATH) -> OAuthTokens | None:
    """Read stored OAuth tokens for *dremio_url*. Returns ``None`` if absent."""
    if not store_path.exists():
        return None
    with store_path.open() as f:
        data = yaml.safe_load(f) or {}
    entry = data.get(dremio_url)
    if entry is None:
        return None
    return OAuthTokens(**entry)


def save(dremio_url: str, tokens: OAuthTokens, store_path: Path = STORE_PATH) -> None:
    """Persist *tokens* under *dremio_url*. Creates dirs and sets mode 600."""
    store_path.parent.mkdir(parents=True, exist_ok=True)

    data: dict = {}
    if store_path.exists():
        with store_path.open() as f:
            data = yaml.safe_load(f) or {}

    data[dremio_url] = tokens.model_dump(exclude_none=True)
    store_path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
    store_path.chmod(0o600)


def clear(dremio_url: str, store_path: Path = STORE_PATH) -> None:
    """Remove stored tokens for *dremio_url*."""
    if not store_path.exists():
        return
    with store_path.open() as f:
        data = yaml.safe_load(f) or {}
    data.pop(dremio_url, None)
    store_path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
    store_path.chmod(0o600)
