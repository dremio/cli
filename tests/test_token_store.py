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
"""Tests for drs.token_store — OAuth token persistence."""

from __future__ import annotations

from pathlib import Path

import pytest

from drs.token_store import OAuthTokens, clear, load, save


@pytest.fixture
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "oauth_tokens.yaml"


@pytest.fixture
def sample_tokens() -> OAuthTokens:
    return OAuthTokens(
        access_token="at-123",
        refresh_token="rt-456",
        expires_at=1700000000.0,
        client_id="test-client",
        client_secret="test-secret",
    )


def test_round_trip(store_path: Path, sample_tokens: OAuthTokens) -> None:
    url = "https://api.dremio.cloud"
    save(url, sample_tokens, store_path=store_path)
    loaded = load(url, store_path=store_path)
    assert loaded is not None
    assert loaded == sample_tokens


def test_load_missing_url(store_path: Path, sample_tokens: OAuthTokens) -> None:
    save("https://api.dremio.cloud", sample_tokens, store_path=store_path)
    assert load("https://api.eu.dremio.cloud", store_path=store_path) is None


def test_load_nonexistent_file(store_path: Path) -> None:
    assert load("https://api.dremio.cloud", store_path=store_path) is None


def test_clear_removes_entry(store_path: Path, sample_tokens: OAuthTokens) -> None:
    url = "https://api.dremio.cloud"
    save(url, sample_tokens, store_path=store_path)
    clear(url, store_path=store_path)
    assert load(url, store_path=store_path) is None


def test_clear_nonexistent_is_noop(store_path: Path) -> None:
    clear("https://api.dremio.cloud", store_path=store_path)  # should not raise


def test_file_mode_600(store_path: Path, sample_tokens: OAuthTokens) -> None:
    save("https://api.dremio.cloud", sample_tokens, store_path=store_path)
    assert oct(store_path.stat().st_mode & 0o777) == "0o600"


def test_multi_instance_keying(store_path: Path) -> None:
    url_us = "https://api.dremio.cloud"
    url_eu = "https://api.eu.dremio.cloud"
    tokens_us = OAuthTokens(access_token="us-token", client_id="cid")
    tokens_eu = OAuthTokens(access_token="eu-token", client_id="cid")

    save(url_us, tokens_us, store_path=store_path)
    save(url_eu, tokens_eu, store_path=store_path)

    loaded_us = load(url_us, store_path=store_path)
    loaded_eu = load(url_eu, store_path=store_path)
    assert loaded_us is not None
    assert loaded_eu is not None
    assert loaded_us.access_token == "us-token"
    assert loaded_eu.access_token == "eu-token"


def test_save_creates_parent_dirs(tmp_path: Path) -> None:
    deep_path = tmp_path / "a" / "b" / "tokens.yaml"
    tokens = OAuthTokens(access_token="at", client_id="cid")
    save("https://api.dremio.cloud", tokens, store_path=deep_path)
    assert deep_path.exists()
