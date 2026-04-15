# OAuth Browser Login — Design Spec

**Jira**: DX-118868
**Status**: Proposed

## Summary

Add browser-based OAuth login to the Dremio CLI as an alternative to PAT authentication.
Users run `dremio login`, authenticate in their browser, and the CLI stores tokens for
subsequent use — with automatic refresh on 401.

## OAuth Flow

```
CLI                         Browser                    Dremio OAuth Server
 │                            │                              │
 │── GET /.well-known/oauth-authorization-server ──────────►│
 │◄─────────────── metadata (endpoints) ────────────────────│
 │                            │                              │
 │── POST /register (DCR) ──────────────────────────────────►│
 │◄─────────────── client_id, client_secret ────────────────│
 │                            │                              │
 │── generate PKCE pair ──►   │                              │
 │── open auth URL ─────────►│                              │
 │   (localhost callback      │── user authenticates ──────►│
 │    server listening)       │◄─────── redirect ───────────│
 │◄── code + state ──────────│                              │
 │                            │                              │
 │── POST /token (exchange code) ───────────────────────────►│
 │◄─────────────── access_token, refresh_token ─────────────│
 │                            │                              │
 │── save tokens to ~/.config/dremioai/oauth_tokens.yaml    │
```

### Step 1: OAuth Discovery

```
GET {dremio_url}/.well-known/oauth-authorization-server
```

Response provides: `authorization_endpoint`, `token_endpoint`, `registration_endpoint`, etc.

### Step 2: Dynamic Client Registration (DCR)

```
POST {registration_endpoint}
{
  "client_name": "https://connectors.dremio.app/claude",
  "redirect_uris": ["http://localhost:<port>/callback"],
  "grant_types": ["authorization_code", "refresh_token"],
  "response_types": ["code"],
  "token_endpoint_auth_method": "client_secret_basic"
}
```

The port is a random free port found by binding to port 0.

### Step 3: Authorization Code + PKCE

- Generate `code_verifier` (random 43-128 char URL-safe string) and `code_challenge` (S256 hash)
- Build authorization URL with params: `response_type=code`, `client_id`, `redirect_uri`,
  `code_challenge`, `code_challenge_method=S256`, `state` (random)
- Start a minimal `http.server.HTTPServer` on `localhost:{port}` listening for `/callback`
- Open browser via `webbrowser.open()` (fallback: print URL for manual copy-paste)
- Wait for callback with `code` and `state`, serve a "You can close this tab" HTML page

### Step 4: Token Exchange

```
POST {token_endpoint}
Authorization: Basic base64(client_id:client_secret)
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code&code={code}&redirect_uri={redirect_uri}&code_verifier={code_verifier}
```

### Step 5: Token Storage

Store in `~/.config/dremioai/oauth_tokens.yaml` (mode 600), keyed by Dremio URL:

```yaml
tokens:
  "https://api.dremio.cloud":
    access_token: "..."
    refresh_token: "..."
    expires_at: 1713100000.0
    token_endpoint: "https://..."
    client_id: "..."
    client_secret: "..."
```

### Step 6: Automatic Token Refresh

On 401 from any Dremio API call:

1. POST to `token_endpoint` with `grant_type=refresh_token`
2. Update `Authorization` header with new access token
3. Save new tokens (handles refresh token rotation)
4. Retry the original request **once**

## File Changes

### New Files

| File | Purpose |
|------|---------|
| `src/drs/oauth.py` | OAuth flow engine: discovery, DCR, PKCE, browser auth, callback server, token exchange, refresh |
| `src/drs/token_store.py` | Token persistence to `~/.config/dremioai/oauth_tokens.yaml` |
| `src/drs/commands/login.py` | `dremio login` and `dremio logout` commands |

### Modified Files

| File | Changes |
|------|---------|
| `src/drs/auth.py` | Make `pat` optional (`str \| None = None`), add `auth_method: str = "pat"` field. In `load_config()`, fall back to OAuth tokens when PAT is missing. |
| `src/drs/client.py` | Accept optional `oauth_tokens` in `__init__`. In `_request_with_retry`, intercept 401 to refresh token and retry once. |
| `src/drs/cli.py` | Wire OAuth tokens into `get_client()`. Register `login`/`logout` commands. Update error messages to mention `dremio login`. |
| `src/drs/commands/setup.py` | Add auth method prompt (OAuth vs PAT) after region selection. |

## Key Decisions

- **No new dependencies** — stdlib `http.server`, `webbrowser`, `secrets`, `hashlib`, `threading`, `socket` + existing `httpx`
- **PKCE always enabled** — defense-in-depth even though DCR provides a client_secret
- **Headless fallback** — print URL when `webbrowser.open()` fails (SSH, containers)
- **Multi-instance support** — tokens keyed by Dremio URL
- **Refresh token rotation** — always persist new refresh token if server returns one
- **Single retry on 401** — `_refreshing` flag prevents infinite loops

## Implementation Phases

1. **Phase 1**: `oauth.py` + `token_store.py` + tests (standalone, no behavioral changes)
2. **Phase 2**: `auth.py` — make PAT optional, add `auth_method`
3. **Phase 3**: `client.py` — 401 intercept + refresh logic
4. **Phase 4**: `cli.py` + `login.py` + `setup.py` — wire up commands and wizard
5. **Phase 5**: Manual E2E test against real Dremio instance

## Risks

- **OAuth metadata host**: `uri` in config is `https://api.dremio.cloud` — the `.well-known` endpoint may be on a different host. Need to confirm.
- **DCR availability**: Not all Dremio deployments may support `/register`. Fail gracefully with a clear message.
- **Port race**: `find_free_port()` has a tiny race window between close and rebind. Mitigate by binding the HTTPServer socket directly to port 0.
