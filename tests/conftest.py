"""Shared test fixtures for drs tests."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from drs.auth import DrsConfig
from drs.client import DremioClient


@pytest.fixture
def config() -> DrsConfig:
    return DrsConfig(
        uri="https://api.dremio.cloud",
        pat="test-pat-token",
        project_id="test-project-id",
    )


@pytest.fixture
def mock_client(config: DrsConfig) -> DremioClient:
    """DremioClient with all HTTP methods mocked."""
    client = DremioClient(config)
    client._get = AsyncMock()
    client._post = AsyncMock()
    client._delete = AsyncMock()
    return client
