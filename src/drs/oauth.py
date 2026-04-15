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
"""OAuth 2.0 PKCE browser-login flow for Dremio Cloud."""

from __future__ import annotations

import base64
import hashlib
import logging
import secrets
import socket
import threading
import time
import webbrowser
from concurrent.futures import Future
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

import httpx

from drs.token_store import OAuthTokens

logger = logging.getLogger(__name__)

_CLIENT_ID = "https://connectors.dremio.app/claude"

_SUCCESS_HTML = """\
<!DOCTYPE html>
<html><head><title>Dremio CLI</title></head>
<body style="font-family:sans-serif;text-align:center;padding:60px">
<h2>Login successful</h2>
<p>You can close this tab and return to the terminal.</p>
</body></html>
"""


@dataclass
class OAuthServerMetadata:
    authorization_endpoint: str
    token_endpoint: str
    registration_endpoint: str | None = None


def _login_url(dremio_url: str) -> str:
    """Derive the OAuth login host from a Dremio URL (app.X -> login.X)."""
    parsed = urlparse(dremio_url)
    host = parsed.hostname or ""
    if host.startswith("app."):
        host = "login." + host[4:]
    return f"{parsed.scheme}://{host}"


def discover(dremio_url: str) -> OAuthServerMetadata:
    """Fetch OAuth Authorization Server Metadata from *dremio_url*."""
    base = _login_url(dremio_url)
    url = f"{base}/.well-known/oauth-authorization-server"
    resp = httpx.get(url, timeout=30.0)
    resp.raise_for_status()
    data = resp.json()
    return OAuthServerMetadata(
        authorization_endpoint=data["authorization_endpoint"],
        token_endpoint=data["token_endpoint"],
        registration_endpoint=data.get("registration_endpoint"),
    )


def register_client(registration_endpoint: str, redirect_uri: str) -> tuple[str, str | None]:
    """Dynamic Client Registration. Returns ``(client_id, client_secret)``.

    Returns ``None`` if the server does not support DCR (403/400).
    """
    body = {
        "client_name": _CLIENT_ID,
        "redirect_uris": [redirect_uri],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
    }
    resp = httpx.post(registration_endpoint, json=body, timeout=30.0)
    if resp.status_code in (400, 403):
        logger.info("DCR not available (%s) — using well-known client_id.", resp.status_code)
        return None, None
    resp.raise_for_status()
    data = resp.json()
    return data["client_id"], data.get("client_secret")


def generate_pkce() -> tuple[str, str]:
    """Generate PKCE ``(code_verifier, code_challenge)`` pair."""
    code_verifier = secrets.token_urlsafe(32)
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return code_verifier, code_challenge


def build_authorization_url(
    auth_endpoint: str,
    client_id: str,
    redirect_uri: str,
    code_challenge: str,
    state: str,
) -> str:
    """Construct the authorization URL the user's browser should open."""
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "scope": "dremio.all offline_access",
        "state": state,
    }
    return f"{auth_endpoint}?{urlencode(params)}"


def find_free_port() -> int:
    """Bind to port 0 to obtain a free ephemeral port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_callback_server(port: int) -> tuple[HTTPServer, Future[tuple[str, str]]]:
    """Start a localhost HTTP server that captures the OAuth callback.

    Returns ``(server, future)`` where the future resolves to ``(code, state)``.
    """
    future: Future[tuple[str, str]] = Future()

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            qs = parse_qs(urlparse(self.path).query)
            code = qs.get("code", [None])[0]
            state = qs.get("state", [None])[0]
            error = qs.get("error", [None])[0]

            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()

            if code and state:
                self.wfile.write(_SUCCESS_HTML.encode())
                future.set_result((code, state))
            else:
                error_msg = error or "unknown"
                self.wfile.write(f"<h2>Login failed: {error_msg}</h2>".encode())
                if not future.done():
                    future.set_exception(RuntimeError(f"OAuth callback error: {error_msg}"))

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            logger.debug(format, *args)

    server = HTTPServer(("127.0.0.1", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, future


def exchange_code(
    token_endpoint: str,
    code: str,
    redirect_uri: str,
    client_id: str,
    client_secret: str | None,
    code_verifier: str,
) -> OAuthTokens:
    """Exchange an authorization code for tokens."""
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "code_verifier": code_verifier,
    }
    if client_secret:
        data["client_secret"] = client_secret
    resp = httpx.post(token_endpoint, data=data, timeout=30.0)
    resp.raise_for_status()
    body = resp.json()
    expires_at: float | None = None
    if "expires_in" in body:
        expires_at = time.time() + body["expires_in"]
    return OAuthTokens(
        access_token=body["access_token"],
        refresh_token=body.get("refresh_token"),
        expires_at=expires_at,
        client_id=client_id,
        client_secret=client_secret,
    )


def refresh_access_token(
    token_endpoint: str,
    client_id: str,
    client_secret: str | None,
    refresh_token: str,
) -> OAuthTokens:
    """Use a refresh token to obtain a new access token."""
    data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "refresh_token": refresh_token,
    }
    if client_secret:
        data["client_secret"] = client_secret
    resp = httpx.post(token_endpoint, data=data, timeout=30.0)
    resp.raise_for_status()
    body = resp.json()
    expires_at: float | None = None
    if "expires_in" in body:
        expires_at = time.time() + body["expires_in"]
    return OAuthTokens(
        access_token=body["access_token"],
        refresh_token=body.get("refresh_token", refresh_token),
        expires_at=expires_at,
        client_id=client_id,
        client_secret=client_secret,
    )


def run_login_flow(dremio_url: str) -> OAuthTokens:
    """Orchestrate the full browser-based OAuth login flow.

    1. Discover OAuth endpoints
    2. Find a free port & register the client (DCR)
    3. Generate PKCE codes
    4. Start localhost callback server
    5. Open the browser (fall back to printing URL)
    6. Wait for the callback
    7. Exchange the code for tokens
    """
    metadata = discover(dremio_url)

    port = find_free_port()
    redirect_uri = f"http://localhost:{port}/Callback"

    client_id, client_secret = None, None
    if metadata.registration_endpoint:
        client_id, client_secret = register_client(metadata.registration_endpoint, redirect_uri)
    if not client_id:
        client_id, client_secret = _CLIENT_ID, None

    code_verifier, code_challenge = generate_pkce()
    state = secrets.token_urlsafe(16)

    auth_url = build_authorization_url(
        metadata.authorization_endpoint,
        client_id,
        redirect_uri,
        code_challenge,
        state,
    )

    server, future = start_callback_server(port)
    try:
        try:
            opened = webbrowser.open(auth_url)
        except Exception:
            opened = False
        if opened:
            logger.info("Opened browser for OAuth login.")
        else:
            # Headless / no-browser fallback
            print(f"\nOpen this URL in your browser to log in:\n\n  {auth_url}\n")

        code, returned_state = future.result(timeout=300)
        if returned_state != state:
            raise RuntimeError("OAuth state mismatch — possible CSRF attack.")
    finally:
        server.shutdown()

    return exchange_code(
        metadata.token_endpoint,
        code,
        redirect_uri,
        client_id,
        client_secret,
        code_verifier,
    )
