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
    client._put = AsyncMock()
    client._delete = AsyncMock()
    return client
