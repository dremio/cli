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

from typing import Any

import httpx

from drs.auth import DrsConfig


class DremioClient:
    """Async HTTP client for Dremio Cloud REST APIs.

    This is the ONLY file that makes HTTP calls. All commands and MCP tools
    call methods on this class.
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

    # -- HTTP helpers --

    async def _get(self, url: str, params: dict | None = None) -> Any:
        resp = await self._client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    async def _post(self, url: str, json: dict | None = None) -> Any:
        resp = await self._client.post(url, json=json)
        resp.raise_for_status()
        return resp.json()

    async def _delete(self, url: str) -> Any:
        resp = await self._client.delete(url)
        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return {"status": "ok"}

    # -- SQL / Jobs (v0) --

    async def submit_sql(self, sql: str, context: list[str] | None = None) -> dict:
        """Submit a SQL query. Returns job metadata including job_id."""
        body: dict[str, Any] = {"sql": sql}
        if context:
            body["context"] = context
        return await self._post(self._v0("/sql"), json=body)

    async def get_job_status(self, job_id: str) -> dict:
        return await self._get(self._v0(f"/job/{job_id}"))

    async def get_job_results(
        self, job_id: str, limit: int = 500, offset: int = 0
    ) -> dict:
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

    async def get_lineage(self, entity_id: str) -> dict:
        return await self._get(self._v3(f"/catalog/{entity_id}/graph"))

    async def get_wiki(self, entity_id: str) -> dict:
        return await self._get(self._v3(f"/catalog/{entity_id}/collaboration/wiki"))

    async def get_tags(self, entity_id: str) -> dict:
        return await self._get(self._v3(f"/catalog/{entity_id}/collaboration/tag"))

    # -- Reflections (v3) --

    async def get_reflection(self, reflection_id: str) -> dict:
        return await self._get(self._v3(f"/reflection/{reflection_id}"))

    async def refresh_reflection(self, reflection_id: str) -> dict:
        return await self._post(self._v3(f"/reflection/{reflection_id}/refresh"))

    async def delete_reflection(self, reflection_id: str) -> dict:
        return await self._delete(self._v3(f"/reflection/{reflection_id}"))

    # -- Users & Roles (v1) --

    async def list_users(self, max_results: int = 100) -> dict:
        return await self._get(self._v1("/users"), params={"maxResults": max_results})

    async def get_user_by_name(self, name: str) -> dict:
        return await self._get(self._v1(f"/users/name/{name}"))

    async def list_roles(self, max_results: int = 100) -> dict:
        return await self._get(self._v1("/roles"), params={"maxResults": max_results})

    async def get_grants(
        self, scope: str, scope_id: str, grantee_type: str, grantee_id: str
    ) -> dict:
        """Get grants. scope is 'catalog' or 'org', grantee_type is 'user' or 'role'."""
        return await self._get(
            self._v1(f"/{scope}/{scope_id}/grants/{grantee_type}/{grantee_id}")
        )
