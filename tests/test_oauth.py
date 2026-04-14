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
"""Tests for drs.oauth — OAuth 2.0 PKCE flow mechanics."""

from __future__ import annotations

import base64
import hashlib
import urllib.request
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from drs.oauth import (
    build_authorization_url,
    discover,
    exchange_code,
    find_free_port,
    generate_pkce,
    refresh_access_token,
    run_login_flow,
    start_callback_server,
)


class TestPKCE:
    def test_generate_pkce_format(self) -> None:
        verifier, challenge = generate_pkce()
        assert len(verifier) > 20
        assert len(challenge) > 20
        # No padding characters
        assert "=" not in challenge

    def test_pkce_challenge_matches_verifier(self) -> None:
        verifier, challenge = generate_pkce()
        expected = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=").decode("ascii")
        assert challenge == expected

    def test_pkce_uniqueness(self) -> None:
        v1, _ = generate_pkce()
        v2, _ = generate_pkce()
        assert v1 != v2


class TestBuildAuthorizationURL:
    def test_url_construction(self) -> None:
        url = build_authorization_url(
            auth_endpoint="https://auth.example.com/authorize",
            client_id="my-client",
            redirect_uri="http://localhost:8080/callback",
            code_challenge="abc123",
            state="state-xyz",
        )
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        assert parsed.scheme == "https"
        assert parsed.netloc == "auth.example.com"
        assert parsed.path == "/authorize"
        assert params["response_type"] == ["code"]
        assert params["client_id"] == ["my-client"]
        assert params["redirect_uri"] == ["http://localhost:8080/callback"]
        assert params["code_challenge"] == ["abc123"]
        assert params["code_challenge_method"] == ["S256"]
        assert params["scope"] == ["openid offline_access"]
        assert params["state"] == ["state-xyz"]


class TestDiscover:
    def test_discover_parses_metadata(self) -> None:
        metadata_json = {
            "authorization_endpoint": "https://auth.example.com/authorize",
            "token_endpoint": "https://auth.example.com/token",
            "registration_endpoint": "https://auth.example.com/register",
        }

        mock_response = MagicMock()
        mock_response.json.return_value = metadata_json
        mock_response.raise_for_status = MagicMock()

        with patch("drs.oauth.httpx.get", return_value=mock_response) as mock_get:
            result = discover("https://api.dremio.cloud")

        mock_get.assert_called_once_with(
            "https://api.dremio.cloud/.well-known/oauth-authorization-server",
            timeout=30.0,
        )
        assert result.authorization_endpoint == "https://auth.example.com/authorize"
        assert result.token_endpoint == "https://auth.example.com/token"
        assert result.registration_endpoint == "https://auth.example.com/register"

    def test_discover_optional_registration(self) -> None:
        metadata_json = {
            "authorization_endpoint": "https://auth.example.com/authorize",
            "token_endpoint": "https://auth.example.com/token",
        }

        mock_response = MagicMock()
        mock_response.json.return_value = metadata_json
        mock_response.raise_for_status = MagicMock()

        with patch("drs.oauth.httpx.get", return_value=mock_response):
            result = discover("https://api.dremio.cloud")

        assert result.registration_endpoint is None


class TestExchangeCode:
    def test_exchange_code_success(self) -> None:
        token_json = {
            "access_token": "at-new",
            "refresh_token": "rt-new",
            "expires_in": 3600,
        }
        mock_response = MagicMock()
        mock_response.json.return_value = token_json
        mock_response.raise_for_status = MagicMock()

        with patch("drs.oauth.httpx.post", return_value=mock_response):
            tokens = exchange_code(
                "https://auth.example.com/token",
                "auth-code-123",
                "http://localhost:8080/callback",
                "my-client",
                "my-secret",
                "my-verifier",
            )

        assert tokens.access_token == "at-new"
        assert tokens.refresh_token == "rt-new"
        assert tokens.expires_at is not None
        assert tokens.client_id == "my-client"
        assert tokens.client_secret == "my-secret"


class TestRefreshAccessToken:
    def test_refresh_success(self) -> None:
        token_json = {
            "access_token": "at-refreshed",
            "refresh_token": "rt-new",
            "expires_in": 3600,
        }
        mock_response = MagicMock()
        mock_response.json.return_value = token_json
        mock_response.raise_for_status = MagicMock()

        with patch("drs.oauth.httpx.post", return_value=mock_response):
            tokens = refresh_access_token(
                "https://auth.example.com/token",
                "my-client",
                "my-secret",
                "rt-old",
            )

        assert tokens.access_token == "at-refreshed"
        assert tokens.refresh_token == "rt-new"

    def test_refresh_preserves_old_refresh_token(self) -> None:
        """When server omits refresh_token in response, keep the old one."""
        token_json = {
            "access_token": "at-refreshed",
            "expires_in": 3600,
        }
        mock_response = MagicMock()
        mock_response.json.return_value = token_json
        mock_response.raise_for_status = MagicMock()

        with patch("drs.oauth.httpx.post", return_value=mock_response):
            tokens = refresh_access_token(
                "https://auth.example.com/token",
                "my-client",
                None,
                "rt-old",
            )

        assert tokens.refresh_token == "rt-old"


class TestCallbackServer:
    def test_callback_captures_code_and_state(self) -> None:
        port = find_free_port()
        server, future = start_callback_server(port)
        try:
            url = f"http://localhost:{port}/callback?code=test-code&state=test-state"
            urllib.request.urlopen(url, timeout=5)
            code, state = future.result(timeout=5)
            assert code == "test-code"
            assert state == "test-state"
        finally:
            server.shutdown()

    def test_find_free_port_returns_positive(self) -> None:
        port = find_free_port()
        assert port > 0


class TestRunLoginFlow:
    @pytest.mark.parametrize("browser_behavior", ["raises", "returns_false"])
    def test_headless_fallback_prints_url(self, browser_behavior: str) -> None:
        """When webbrowser.open raises or returns False, the URL should be printed."""
        metadata_json = {
            "authorization_endpoint": "https://auth.example.com/authorize",
            "token_endpoint": "https://auth.example.com/token",
            "registration_endpoint": "https://auth.example.com/register",
        }
        dcr_json = {"client_id": "cid", "client_secret": "cs"}
        token_json = {"access_token": "at", "refresh_token": "rt", "expires_in": 3600}

        mock_get = MagicMock()
        mock_get.json.return_value = metadata_json
        mock_get.raise_for_status = MagicMock()

        post_responses = []
        # DCR call
        dcr_resp = MagicMock()
        dcr_resp.json.return_value = dcr_json
        dcr_resp.raise_for_status = MagicMock()
        post_responses.append(dcr_resp)
        # Token exchange
        token_resp = MagicMock()
        token_resp.json.return_value = token_json
        token_resp.raise_for_status = MagicMock()
        post_responses.append(token_resp)

        if browser_behavior == "raises":
            browser_mock = MagicMock(side_effect=RuntimeError("no browser"))
        else:
            browser_mock = MagicMock(return_value=False)

        with (
            patch("drs.oauth.httpx.get", return_value=mock_get),
            patch("drs.oauth.httpx.post", side_effect=post_responses),
            patch("drs.oauth.webbrowser.open", browser_mock),
            patch("drs.oauth.start_callback_server") as mock_server,
            patch("builtins.print") as mock_print,
        ):
            mock_future = MagicMock()
            mock_future.result.return_value = ("code-123", None)
            mock_srv = MagicMock()
            mock_server.return_value = (mock_srv, mock_future)

            # The state won't match because we bypass the real flow,
            # so we need to patch around the state check too.
            try:
                run_login_flow("https://api.dremio.cloud")
            except RuntimeError:
                pass  # state mismatch expected in mocked test

            # Verify fallback print was called
            mock_print.assert_called_once()
            printed_text = mock_print.call_args[0][0]
            assert "browser" in printed_text.lower() or "url" in printed_text.lower() or "http" in printed_text.lower()
