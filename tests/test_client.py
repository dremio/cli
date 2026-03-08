"""Tests for the DremioClient URL construction and method routing."""

from __future__ import annotations

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
        assert client._v3("/catalog/123") == "https://api.dremio.cloud/api/v3/catalog/123"

    def test_v3_search_url(self, client: DremioClient) -> None:
        assert client._v3("/search") == "https://api.dremio.cloud/api/v3/search"

    def test_v1_url(self, client: DremioClient) -> None:
        assert client._v1("/users") == "https://api.dremio.cloud/v1/users"

    def test_v1_roles_url(self, client: DremioClient) -> None:
        assert client._v1("/roles") == "https://api.dremio.cloud/v1/roles"


class TestClientHeaders:
    def test_auth_header(self, client: DremioClient) -> None:
        assert client._client.headers["authorization"] == "Bearer test-token"

    def test_content_type(self, client: DremioClient) -> None:
        assert client._client.headers["content-type"] == "application/json"
