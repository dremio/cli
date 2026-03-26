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
"""REST client for Dremio Cloud API — the single HTTP layer."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from drs.auth import DrsConfig

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRY_BACKOFF = (1.0, 2.0, 4.0)
_RETRYABLE_STATUS_CODES = {429, 502, 503, 504}


class DremioClient:
    """Async HTTP client for Dremio Cloud REST APIs.

    This is the ONLY file that makes HTTP calls. All commands and MCP tools
    call methods on this class.

    Transient failures (timeouts, 429, 502, 503, 504) are retried up to 3
    times with exponential backoff (1s, 2s, 4s).
    """

    def __init__(self, config: DrsConfig) -> None:
        self.config = config
        self._client = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {config.pat}",
                "Content-Type": "application/json",
            },
            timeout=120.0,
        )

    async def close(self) -> None:
        await self._client.aclose()

    # -- URL builders --

    def _v0(self, path: str) -> str:
        """Project-scoped URL: /v0/projects/{pid}/..."""
        return f"{self.config.uri}/v0/projects/{self.config.project_id}{path}"

    def _v3(self, path: str) -> str:
        """Alias for _v0 — Cloud serves catalog/reflection under the same project-scoped path."""
        return self._v0(path)

    def _v1(self, path: str) -> str:
        return f"{self.config.uri}/v1{path}"

    # -- HTTP helpers with retry --

    async def _request_with_retry(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Execute an HTTP request with retry on transient errors."""
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                resp = await self._client.request(method, url, **kwargs)
                if resp.status_code in _RETRYABLE_STATUS_CODES and attempt < _MAX_RETRIES - 1:
                    delay = _RETRY_BACKOFF[attempt]
                    logger.warning(
                        "Retryable HTTP %d on %s %s — retrying in %.1fs (attempt %d/%d)",
                        resp.status_code,
                        method,
                        url,
                        delay,
                        attempt + 1,
                        _MAX_RETRIES,
                    )
                    await asyncio.sleep(delay)
                    continue
                return resp
            except httpx.TimeoutException as exc:
                last_exc = exc
                if attempt < _MAX_RETRIES - 1:
                    delay = _RETRY_BACKOFF[attempt]
                    logger.warning(
                        "Timeout on %s %s — retrying in %.1fs (attempt %d/%d)",
                        method,
                        url,
                        delay,
                        attempt + 1,
                        _MAX_RETRIES,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise
        raise last_exc  # type: ignore[misc]

    async def _get(self, url: str, params: dict | None = None) -> Any:
        logger.debug("GET %s params=%s", url, params)
        resp = await self._request_with_retry("GET", url, params=params)
        logger.debug("GET %s → %d (%d bytes)", url, resp.status_code, len(resp.content))
        resp.raise_for_status()
        return resp.json()

    async def _post(self, url: str, json: dict | None = None) -> Any:
        logger.debug("POST %s body=%s", url, json)
        resp = await self._request_with_retry("POST", url, json=json)
        logger.debug("POST %s → %d (%d bytes)", url, resp.status_code, len(resp.content))
        resp.raise_for_status()
        return resp.json()

    async def _put(self, url: str, json: dict | None = None) -> Any:
        logger.debug("PUT %s body=%s", url, json)
        resp = await self._request_with_retry("PUT", url, json=json)
        logger.debug("PUT %s → %d (%d bytes)", url, resp.status_code, len(resp.content))
        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return {"status": "ok"}

    async def _delete(self, url: str, params: dict | None = None) -> Any:
        logger.debug("DELETE %s params=%s", url, params)
        resp = await self._request_with_retry("DELETE", url, params=params)
        logger.debug("DELETE %s → %d (%d bytes)", url, resp.status_code, len(resp.content))
        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return {"status": "ok"}

    # -- Projects (v0, org-scoped) --

    def _v0_org(self, path: str) -> str:
        """Org-scoped URL: /v0/... (no project ID)."""
        return f"{self.config.uri}/v0{path}"

    async def list_projects(self) -> dict:
        return await self._get(self._v0_org("/projects"))

    async def get_project(self, project_id: str) -> dict:
        return await self._get(self._v0_org(f"/projects/{project_id}"))

    async def create_project(self, body: dict) -> dict:
        return await self._post(self._v0_org("/projects"), json=body)

    async def update_project(self, project_id: str, body: dict) -> dict:
        return await self._put(self._v0_org(f"/projects/{project_id}"), json=body)

    async def delete_project(self, project_id: str) -> dict:
        return await self._delete(self._v0_org(f"/projects/{project_id}"))

    # -- SQL / Jobs (v0) --

    async def submit_sql(self, sql: str, context: list[str] | None = None) -> dict:
        """Submit a SQL query. Returns job metadata including job_id."""
        body: dict[str, Any] = {"sql": f"/* dremio-cli: submitter=cli */ {sql}"}
        if context:
            body["context"] = context
        return await self._post(self._v0("/sql"), json=body)

    async def get_job_status(self, job_id: str) -> dict:
        return await self._get(self._v0(f"/job/{job_id}"))

    async def get_job_results(self, job_id: str, limit: int = 500, offset: int = 0) -> dict:
        return await self._get(
            self._v0(f"/job/{job_id}/results"),
            params={"limit": limit, "offset": offset},
        )

    async def cancel_job(self, job_id: str) -> dict:
        return await self._post(self._v0(f"/job/{job_id}/cancel"))

    # -- Catalog (v3) --

    async def get_catalog_entity(
        self, entity_id: str, include: list[str] | None = None, exclude: list[str] | None = None
    ) -> dict:
        params: dict[str, str] = {}
        if include:
            params["include"] = ",".join(include)
        if exclude:
            params["exclude"] = ",".join(exclude)
        path = "/catalog" if not entity_id else f"/catalog/{entity_id}"
        return await self._get(self._v3(path), params=params or None)

    async def get_catalog_by_path(self, path_parts: list[str]) -> dict:
        joined = "/".join(path_parts)
        return await self._get(self._v3(f"/catalog/by-path/{joined}"))

    async def search(self, query: str, filter_: str | None = None) -> dict:
        body: dict[str, Any] = {"query": query}
        if filter_:
            body["filter"] = filter_
        return await self._post(self._v0("/search"), json=body)

    async def create_catalog_entity(self, body: dict) -> dict:
        """Create a catalog entity (space, folder, etc.). POST /catalog."""
        return await self._post(self._v3("/catalog"), json=body)

    async def update_catalog_entity(self, entity_id: str, body: dict) -> dict:
        """Update a catalog entity. PUT /catalog/{id}."""
        return await self._put(self._v3(f"/catalog/{entity_id}"), json=body)

    async def delete_catalog_entity(self, entity_id: str, tag: str | None = None) -> dict:
        """Delete a catalog entity. DELETE /catalog/{id}."""
        params = {"tag": tag} if tag else None
        return await self._delete(self._v3(f"/catalog/{entity_id}"), params=params)

    async def get_lineage(self, entity_id: str) -> dict:
        return await self._get(self._v3(f"/catalog/{entity_id}/graph"))

    async def get_wiki(self, entity_id: str) -> dict:
        return await self._get(self._v3(f"/catalog/{entity_id}/collaboration/wiki"))

    async def get_tags(self, entity_id: str) -> dict:
        return await self._get(self._v3(f"/catalog/{entity_id}/collaboration/tag"))

    async def set_wiki(self, entity_id: str, text: str, version: int | None = None) -> dict:
        """Set wiki text for a catalog entity. POST /catalog/{id}/collaboration/wiki."""
        body: dict[str, Any] = {"text": text}
        if version is not None:
            body["version"] = version
        return await self._post(self._v3(f"/catalog/{entity_id}/collaboration/wiki"), json=body)

    async def set_tags(self, entity_id: str, tags: list[str], version: int | None = None) -> dict:
        """Set tags for a catalog entity. POST /catalog/{id}/collaboration/tag."""
        body: dict[str, Any] = {"tags": tags}
        if version is not None:
            body["version"] = version
        return await self._post(self._v3(f"/catalog/{entity_id}/collaboration/tag"), json=body)

    # -- Reflections (v3) --

    async def get_reflection(self, reflection_id: str) -> dict:
        return await self._get(self._v3(f"/reflection/{reflection_id}"))

    async def refresh_reflection(self, reflection_id: str) -> dict:
        return await self._post(self._v3(f"/reflection/{reflection_id}/refresh"))

    async def create_reflection(self, body: dict) -> dict:
        """Create a reflection. POST /reflection."""
        return await self._post(self._v3("/reflection"), json=body)

    async def delete_reflection(self, reflection_id: str) -> dict:
        return await self._delete(self._v3(f"/reflection/{reflection_id}"))

    # -- Engines (v0) --

    async def list_engines(self) -> dict:
        return await self._get(self._v0("/engines"))

    async def get_engine(self, engine_id: str) -> dict:
        return await self._get(self._v0(f"/engines/{engine_id}"))

    async def create_engine(self, body: dict) -> dict:
        return await self._post(self._v0("/engines"), json=body)

    async def update_engine(self, engine_id: str, body: dict) -> dict:
        return await self._put(self._v0(f"/engines/{engine_id}"), json=body)

    async def delete_engine(self, engine_id: str) -> dict:
        return await self._delete(self._v0(f"/engines/{engine_id}"))

    async def enable_engine(self, engine_id: str) -> dict:
        return await self._put(self._v0(f"/engines/{engine_id}/enable"))

    async def disable_engine(self, engine_id: str) -> dict:
        return await self._put(self._v0(f"/engines/{engine_id}/disable"))

    # -- Users & Roles (v1) --

    async def list_users(self, max_results: int = 100) -> dict:
        return await self._get(self._v1("/users"), params={"maxResults": max_results})

    async def get_user_by_name(self, name: str) -> dict:
        return await self._get(self._v1(f"/users/name/{name}"))

    async def get_user(self, user_id: str) -> dict:
        return await self._get(self._v1(f"/users/{user_id}"))

    async def invite_user(self, body: dict) -> dict:
        """Invite a user. POST /v1/users/invite."""
        return await self._post(self._v1("/users/invite"), json=body)

    async def update_user(self, user_id: str, body: dict) -> dict:
        return await self._put(self._v1(f"/users/{user_id}"), json=body)

    async def delete_user(self, user_id: str) -> dict:
        return await self._delete(self._v1(f"/users/{user_id}"))

    async def list_roles(self, max_results: int = 100) -> dict:
        return await self._get(self._v1("/roles"), params={"maxResults": max_results})

    async def get_role(self, role_id: str) -> dict:
        return await self._get(self._v1(f"/roles/{role_id}"))

    async def get_role_by_name(self, name: str) -> dict:
        return await self._get(self._v1(f"/roles/name/{name}"))

    async def create_role(self, body: dict) -> dict:
        return await self._post(self._v1("/roles"), json=body)

    async def update_role(self, role_id: str, body: dict) -> dict:
        return await self._put(self._v1(f"/roles/{role_id}"), json=body)

    async def delete_role(self, role_id: str) -> dict:
        return await self._delete(self._v1(f"/roles/{role_id}"))

    # -- Agent / Chat (SSE) --

    def _agent(self, path: str) -> str:
        """Agent API URL: /v1/projects/{pid}/agent/..."""
        return f"{self.config.uri}/v1/projects/{self.config.project_id}/agent{path}"

    async def create_conversation(self, body: dict) -> dict:
        """POST /agent/conversations — start a new conversation."""
        return await self._post(self._agent("/conversations"), json=body)

    async def send_conversation_message(self, conversation_id: str, body: dict) -> dict:
        """POST /agent/conversations/{id}/messages — send a message or approval."""
        return await self._post(
            self._agent(f"/conversations/{conversation_id}/messages"),
            json=body,
        )

    async def stream_run(self, conversation_id: str, run_id: str) -> httpx.Response:
        """GET /agent/conversations/{id}/runs/{runId} — returns raw SSE response.

        Returns the raw ``httpx.Response`` with ``stream=True``.  Caller must
        iterate ``resp.aiter_bytes()`` and close the response via ``async with``.

        We explicitly disable compression (``Accept-Encoding: identity``) so
        that reverse proxies (GCP LB, envoy, etc.) do **not** gzip-buffer the
        event stream — otherwise every SSE event is held until the stream ends.
        """
        url = self._agent(f"/conversations/{conversation_id}/runs/{run_id}")
        logger.debug("SSE GET %s", url)
        resp = await self._client.send(
            self._client.build_request(
                "GET",
                url,
                headers={
                    "Accept": "text/event-stream",
                    "Accept-Encoding": "identity",
                    "Cache-Control": "no-cache",
                },
            ),
            stream=True,
        )
        logger.debug("SSE GET %s → %d", url, resp.status_code)
        resp.raise_for_status()
        return resp

    async def list_conversations(
        self,
        limit: int = 25,
        page_token: str | None = None,
    ) -> dict:
        """GET /agent/conversations"""
        params: dict[str, str | int] = {"maxResults": limit}
        if page_token:
            params["pageToken"] = page_token
        return await self._get(self._agent("/conversations"), params=params)

    async def get_conversation_messages(
        self,
        conversation_id: str,
        limit: int = 50,
        page_token: str | None = None,
    ) -> dict:
        """GET /agent/conversations/{id}/messages"""
        params: dict[str, str | int] = {"maxResults": limit}
        if page_token:
            params["pageToken"] = page_token
        return await self._get(
            self._agent(f"/conversations/{conversation_id}/messages"),
            params=params,
        )

    async def delete_conversation(self, conversation_id: str) -> dict:
        """DELETE /agent/conversations/{id}"""
        return await self._delete(self._agent(f"/conversations/{conversation_id}"))

    async def cancel_conversation_run(
        self,
        conversation_id: str,
        run_id: str,
    ) -> dict:
        """POST /agent/conversations/{id}/runs/{runId}:cancel"""
        return await self._post(
            self._agent(f"/conversations/{conversation_id}/runs/{run_id}:cancel"),
        )

    # -- Grants (v1) --

    async def get_grants(self, scope: str, scope_id: str, grantee_type: str, grantee_id: str) -> dict:
        """Get grants. scope is 'projects', 'orgs', 'clouds', etc."""
        return await self._get(self._v1(f"/{scope}/{scope_id}/grants/{grantee_type}/{grantee_id}"))

    async def set_grants(self, scope: str, scope_id: str, grantee_type: str, grantee_id: str, body: dict) -> dict:
        """Set grants. PUT /v1/{scope}/{scopeId}/grants/{granteeType}/{granteeId}."""
        return await self._put(self._v1(f"/{scope}/{scope_id}/grants/{grantee_type}/{grantee_id}"), json=body)

    async def delete_grants(self, scope: str, scope_id: str, grantee_type: str, grantee_id: str) -> dict:
        """Remove grants. DELETE /v1/{scope}/{scopeId}/grants/{granteeType}/{granteeId}."""
        return await self._delete(self._v1(f"/{scope}/{scope_id}/grants/{grantee_type}/{grantee_id}"))
