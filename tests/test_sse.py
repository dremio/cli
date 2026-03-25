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
"""Tests for drs.sse — SSE stream parser."""

from __future__ import annotations

import json

import pytest

from drs.sse import parse_sse_stream


async def _bytes_iter(chunks: list[bytes]):
    """Helper: async iterator over byte chunks."""
    for chunk in chunks:
        yield chunk


@pytest.mark.asyncio
async def test_parse_single_event():
    data = {"chunkType": "model", "model": {"name": "test", "result": {"text": "hello"}}}
    raw = f"data: {json.dumps(data)}\n\n".encode()
    events = [e async for e in parse_sse_stream(_bytes_iter([raw]))]
    assert len(events) == 1
    assert events[0]["event"] == "message"
    assert events[0]["data"] == data


@pytest.mark.asyncio
async def test_parse_multiple_events():
    data1 = {"chunkType": "model", "text": "a"}
    data2 = {"chunkType": "endOfStream"}
    raw = (f"data: {json.dumps(data1)}\n\ndata: {json.dumps(data2)}\n\n").encode()
    events = [e async for e in parse_sse_stream(_bytes_iter([raw]))]
    assert len(events) == 2
    assert events[0]["data"] == data1
    assert events[1]["data"] == data2


@pytest.mark.asyncio
async def test_parse_event_type():
    data = {"foo": "bar"}
    raw = f"event: custom\ndata: {json.dumps(data)}\n\n".encode()
    events = [e async for e in parse_sse_stream(_bytes_iter([raw]))]
    assert len(events) == 1
    assert events[0]["event"] == "custom"
    assert events[0]["data"] == data


@pytest.mark.asyncio
async def test_comment_lines_ignored():
    data = {"chunkType": "model"}
    raw = f": this is a comment\ndata: {json.dumps(data)}\n\n".encode()
    events = [e async for e in parse_sse_stream(_bytes_iter([raw]))]
    assert len(events) == 1
    assert events[0]["data"] == data


@pytest.mark.asyncio
async def test_partial_chunks():
    """Data split across multiple byte chunks."""
    data = {"chunkType": "model", "text": "hello world"}
    full = f"data: {json.dumps(data)}\n\n"
    mid = len(full) // 2
    chunk1 = full[:mid].encode()
    chunk2 = full[mid:].encode()
    events = [e async for e in parse_sse_stream(_bytes_iter([chunk1, chunk2]))]
    assert len(events) == 1
    assert events[0]["data"] == data


@pytest.mark.asyncio
async def test_multiline_data():
    """Multiple data: lines for one event get joined."""
    raw = b'data: {"a":\ndata: 1}\n\n'
    events = [e async for e in parse_sse_stream(_bytes_iter([raw]))]
    assert len(events) == 1
    # Multiline data lines get joined with newline, parsed as raw if not valid JSON
    assert "data" in events[0]


@pytest.mark.asyncio
async def test_flush_on_stream_end():
    """Data without trailing blank line gets flushed at end of stream."""
    data = {"chunkType": "endOfStream"}
    raw = f"data: {json.dumps(data)}\n".encode()  # No trailing blank line
    events = [e async for e in parse_sse_stream(_bytes_iter([raw]))]
    assert len(events) == 1
    assert events[0]["data"] == data


@pytest.mark.asyncio
async def test_flush_no_trailing_newline():
    """Data without any trailing newline gets flushed at end of stream."""
    data = {"chunkType": "endOfStream"}
    raw = f"data: {json.dumps(data)}".encode()  # No newline at all
    events = [e async for e in parse_sse_stream(_bytes_iter([raw]))]
    assert len(events) == 1
    assert events[0]["data"] == data


@pytest.mark.asyncio
async def test_empty_stream():
    events = [e async for e in parse_sse_stream(_bytes_iter([]))]
    assert events == []


@pytest.mark.asyncio
async def test_event_type_resets_after_event():
    """Event type resets to 'message' after each event."""
    data1 = {"a": 1}
    data2 = {"b": 2}
    raw = (f"event: custom\ndata: {json.dumps(data1)}\n\ndata: {json.dumps(data2)}\n\n").encode()
    events = [e async for e in parse_sse_stream(_bytes_iter([raw]))]
    assert events[0]["event"] == "custom"
    assert events[1]["event"] == "message"
