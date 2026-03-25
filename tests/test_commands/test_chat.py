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
"""Tests for drs.commands.chat — core async functions."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from drs.commands.chat import (
    cancel_run,
    create_conversation,
    delete_conversation,
    get_messages,
    list_conversations,
    send_message,
)


@pytest.mark.asyncio
async def test_create_conversation(mock_client) -> None:
    mock_client.create_conversation = AsyncMock(
        return_value={"id": "conv-1", "runId": "run-1"},
    )
    result = await create_conversation(mock_client, "hello")
    mock_client.create_conversation.assert_called_once_with(
        {"prompt": {"text": "hello"}},
    )
    assert result["id"] == "conv-1"
    assert result["runId"] == "run-1"


@pytest.mark.asyncio
async def test_create_conversation_with_model(mock_client) -> None:
    mock_client.create_conversation = AsyncMock(return_value={"id": "conv-1"})
    await create_conversation(mock_client, "hello", model="gpt-test")
    call_args = mock_client.create_conversation.call_args[0][0]
    assert call_args["model"] == "gpt-test"


@pytest.mark.asyncio
async def test_send_message_text(mock_client) -> None:
    mock_client.send_conversation_message = AsyncMock(
        return_value={"runId": "run-2"},
    )
    result = await send_message(mock_client, "conv-1", text="follow-up")
    mock_client.send_conversation_message.assert_called_once()
    body = mock_client.send_conversation_message.call_args[0][1]
    assert body["prompt"]["text"] == "follow-up"
    assert result["runId"] == "run-2"


@pytest.mark.asyncio
async def test_send_message_approval(mock_client) -> None:
    mock_client.send_conversation_message = AsyncMock(
        return_value={"runId": "run-3"},
    )
    approvals = {
        "approvalNonce": "nonce-1",
        "toolDecisions": [{"callId": "c1", "decision": "approved"}],
    }
    result = await send_message(mock_client, "conv-1", approvals=approvals)
    body = mock_client.send_conversation_message.call_args[0][1]
    assert body["prompt"]["approvals"] == approvals
    assert result["runId"] == "run-3"


@pytest.mark.asyncio
async def test_list_conversations(mock_client) -> None:
    mock_client.list_conversations = AsyncMock(
        return_value={"data": [{"id": "c1", "title": "test"}]},
    )
    result = await list_conversations(mock_client, limit=10)
    mock_client.list_conversations.assert_called_once_with(limit=10)
    assert len(result["data"]) == 1


@pytest.mark.asyncio
async def test_get_messages(mock_client) -> None:
    mock_client.get_conversation_messages = AsyncMock(
        return_value={"data": [{"role": "user", "content": "hi"}]},
    )
    result = await get_messages(mock_client, "conv-1", limit=25)
    mock_client.get_conversation_messages.assert_called_once_with("conv-1", limit=25)
    assert len(result["data"]) == 1


@pytest.mark.asyncio
async def test_delete_conversation(mock_client) -> None:
    mock_client.delete_conversation = AsyncMock(return_value={"status": "ok"})
    result = await delete_conversation(mock_client, "conv-1")
    mock_client.delete_conversation.assert_called_once_with("conv-1")
    assert result["status"] == "ok"


@pytest.mark.asyncio
async def test_cancel_run(mock_client) -> None:
    mock_client.cancel_conversation_run = AsyncMock(return_value={"status": "ok"})
    result = await cancel_run(mock_client, "conv-1", "run-1")
    mock_client.cancel_conversation_run.assert_called_once_with("conv-1", "run-1")
    assert result["status"] == "ok"
