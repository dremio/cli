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
"""Tests for the DremioClient URL construction and method routing."""

from __future__ import annotations

import httpx
import pytest

from drs.auth import DrsConfig
from drs.client import DremioClient


@pytest.fixture
def client() -> DremioClient:
    config = DrsConfig(
        uri="https://api.dremio.cloud",
        pat="test-token",
        project_id="proj-123",
    )
    return DremioClient(config)


class TestURLBuilders:
    def test_v0_url(self, client: DremioClient) -> None:
        assert client._v0("/sql") == "https://api.dremio.cloud/v0/projects/proj-123/sql"

    def test_v0_job_url(self, client: DremioClient) -> None:
        assert client._v0("/job/abc") == "https://api.dremio.cloud/v0/projects/proj-123/job/abc"

    def test_v3_url(self, client: DremioClient) -> None:
        assert client._v3("/catalog/123") == "https://api.dremio.cloud/v0/projects/proj-123/catalog/123"

    def test_search_url(self, client: DremioClient) -> None:
        """Search uses /v0/projects/{pid}/search per Dremio Cloud docs."""
        assert client._v0("/search") == "https://api.dremio.cloud/v0/projects/proj-123/search"

    def test_v3_is_v0(self, client: DremioClient) -> None:
        """v3 is an alias for v0 — Cloud serves all project APIs under /v0/projects/."""
        assert client._v3("/catalog") == client._v0("/catalog")

    def test_v1_url(self, client: DremioClient) -> None:
        assert client._v1("/users") == "https://api.dremio.cloud/v1/users"

    def test_v1_roles_url(self, client: DremioClient) -> None:
        assert client._v1("/roles") == "https://api.dremio.cloud/v1/roles"

    def test_v1_user_by_name_url(self, client: DremioClient) -> None:
        """Verify user lookup uses /v1/users/name/ (Cloud endpoint), not /v1/user/by-name/."""
        url = client._v1("/users/name/rahim")
        assert url == "https://api.dremio.cloud/v1/users/name/rahim"
        assert "/user/by-name/" not in url


class TestCatalogURL:
    def test_catalog_root_no_trailing_slash(self, client: DremioClient) -> None:
        """Catalog list with empty entity_id should not produce trailing slash."""
        # Verify the client method builds the right path
        assert client._v3("/catalog") == "https://api.dremio.cloud/v0/projects/proj-123/catalog"

    def test_catalog_entity_with_id(self, client: DremioClient) -> None:
        assert client._v3("/catalog/abc-123") == "https://api.dremio.cloud/v0/projects/proj-123/catalog/abc-123"

    @pytest.mark.asyncio
    async def test_format_catalog_table_posts_expected_body(self, client: DremioClient) -> None:
        captured: dict = {}

        async def _capture(request: httpx.Request) -> httpx.Response:
            import json

            captured["url"] = str(request.url)
            captured["body"] = json.loads(request.content)
            return httpx.Response(200, json={"id": "ds-1", "path": ["dataproducts", "dre-unstructured", "orders"]})

        client._client = httpx.AsyncClient(transport=httpx.MockTransport(_capture))

        await client.format_catalog_table(
            "folder-123",
            {
                "entityType": "dataset",
                "type": "PHYSICAL_DATASET",
                "path": ["dataproducts", "dre-unstructured", "orders"],
                "format": {"type": "Delta"},
            },
        )

        assert captured["url"] == "https://api.dremio.cloud/v0/projects/proj-123/catalog/folder-123"
        assert captured["body"]["entityType"] == "dataset"
        assert captured["body"]["type"] == "PHYSICAL_DATASET"
        assert captured["body"]["format"]["type"] == "Delta"


class TestEngineURLs:
    def test_engines_list_url(self, client: DremioClient) -> None:
        assert client._v0("/engines") == "https://api.dremio.cloud/v0/projects/proj-123/engines"

    def test_engine_enable_url(self, client: DremioClient) -> None:
        assert (
            client._v0("/engines/eng-1/enable") == "https://api.dremio.cloud/v0/projects/proj-123/engines/eng-1/enable"
        )


class TestGrantURLs:
    def test_project_grant_url(self, client: DremioClient) -> None:
        url = client._v1("/projects/proj-1/grants/role/role-1")
        assert url == "https://api.dremio.cloud/v1/projects/proj-1/grants/role/role-1"

    def test_org_grant_url(self, client: DremioClient) -> None:
        url = client._v1("/orgs/org-1/grants/user/user-1")
        assert url == "https://api.dremio.cloud/v1/orgs/org-1/grants/user/user-1"


class TestClientHeaders:
    def test_auth_header(self, client: DremioClient) -> None:
        assert client._client.headers["authorization"] == "Bearer test-token"

    def test_content_type(self, client: DremioClient) -> None:
        assert client._client.headers["content-type"] == "application/json"


class TestSearch:
    @pytest.mark.asyncio
    async def test_search_includes_optional_filter_and_max_results(self, client: DremioClient) -> None:
        captured: dict = {}

        async def _capture(request: httpx.Request) -> httpx.Response:
            import json

            captured["body"] = json.loads(request.content)
            return httpx.Response(200, json={"results": []})

        client._client = httpx.AsyncClient(transport=httpx.MockTransport(_capture))

        await client.search("revenue", filter_='category in ["JOB"]', max_results=20)

        assert captured["body"] == {
            "query": "revenue",
            "filter": 'category in ["JOB"]',
            "maxResults": 20,
        }

    @pytest.mark.asyncio
    async def test_search_includes_page_token(self, client: DremioClient) -> None:
        captured: dict = {}

        async def _capture(request: httpx.Request) -> httpx.Response:
            import json

            captured["body"] = json.loads(request.content)
            return httpx.Response(200, json={"results": []})

        client._client = httpx.AsyncClient(transport=httpx.MockTransport(_capture))

        await client.search("revenue", next_page_token="token-123")

        assert captured["body"] == {
            "query": "revenue",
            "pageToken": "token-123",
        }


class TestSQLBreadcrumb:
    @pytest.mark.asyncio
    async def test_submit_sql_prepends_breadcrumb(self, client: DremioClient) -> None:
        """SQL submitted via the client must carry the dremio-cli breadcrumb comment."""
        captured: dict = {}

        async def _capture(request: httpx.Request) -> httpx.Response:
            import json

            captured["body"] = json.loads(request.content)
            return httpx.Response(200, json={"id": "job-1"})

        client._client = httpx.AsyncClient(transport=httpx.MockTransport(_capture))

        await client.submit_sql("SELECT 1")

        assert captured["body"]["sql"] == "/* dremio-cli: submitter=cli */\nSELECT 1"

    @pytest.mark.asyncio
    async def test_submit_sql_breadcrumb_with_context(self, client: DremioClient) -> None:
        """Breadcrumb should be present even when a schema context is provided."""
        captured: dict = {}

        async def _capture(request: httpx.Request) -> httpx.Response:
            import json

            captured["body"] = json.loads(request.content)
            return httpx.Response(200, json={"id": "job-2"})

        client._client = httpx.AsyncClient(transport=httpx.MockTransport(_capture))

        await client.submit_sql("SELECT * FROM orders", context=["myspace"])

        assert captured["body"]["sql"].startswith("/* dremio-cli: submitter=cli */")
        assert "SELECT * FROM orders" in captured["body"]["sql"]
        assert captured["body"]["context"] == ["myspace"]
