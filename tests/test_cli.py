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
"""Tests for CLI --version and --help flags."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

from typer.testing import CliRunner

from drs import __version__
from drs.cli import app

runner = CliRunner()


def test_version_flag() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert f"dremio-cli {__version__}" in result.output


def test_help_includes_version() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert f"(version {__version__})" in result.output


def test_help_short_flag() -> None:
    result = runner.invoke(app, ["-h"])
    assert result.exit_code == 0
    assert f"(version {__version__})" in result.output


def test_chat_gantt_command(tmp_path) -> None:
    dump_path = tmp_path / "history.json"
    dump_path.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "chunkType": "toolRequest",
                        "callId": "c1",
                        "name": "searchViewsAndTables",
                        "createdAt": "2026-07-13T12:39:29.860Z",
                        "arguments": {"arg0": "supplier contract risk exposure"},
                    },
                    {
                        "chunkType": "toolResponse",
                        "callId": "c1",
                        "name": "searchViewsAndTables",
                        "createdAt": "2026-07-13T12:39:32.719Z",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["chat", "gantt", str(dump_path), "--ascii", "--width", "20"])

    assert result.exit_code == 0
    assert "Timeline start: 2026-07-13T12:39:29.860000+00:00" in result.output
    assert "S1 searchViewsAndTables" in result.output


def test_chat_history_gantt_command(monkeypatch) -> None:
    from drs.commands import chat

    client = type("DummyClient", (), {"close": AsyncMock()})()
    monkeypatch.setattr(chat, "_get_client", lambda: client)

    async def fake_get_messages(client, conversation_id, limit=50):
        return {
            "data": [
                {
                    "chunkType": "toolRequest",
                    "callId": "c1",
                    "name": "searchViewsAndTables",
                    "createdAt": "2026-07-13T12:39:29.860Z",
                    "arguments": {"arg0": "supplier contract risk exposure"},
                },
                {
                    "chunkType": "toolResponse",
                    "callId": "c1",
                    "name": "searchViewsAndTables",
                    "createdAt": "2026-07-13T12:39:32.719Z",
                },
            ]
        }

    monkeypatch.setattr(chat, "get_messages", fake_get_messages)

    result = runner.invoke(app, ["chat", "history", "conv-1", "--gantt", "--ascii", "--think-time"])

    assert result.exit_code == 0
    assert "Timeline start: 2026-07-13T12:39:29.860000+00:00" in result.output
    assert "S1 searchViewsAndTables" in result.output


def test_chat_history_gantt_command_accepts_messages_envelope(monkeypatch) -> None:
    from drs.commands import chat

    client = type("DummyClient", (), {"close": AsyncMock()})()
    monkeypatch.setattr(chat, "_get_client", lambda: client)

    async def fake_get_messages(client, conversation_id, limit=50):
        return {
            "messages": [
                {
                    "chunkType": "toolRequest",
                    "callId": "c1",
                    "name": "searchViewsAndTables",
                    "createdAt": "2026-07-13T12:39:29.860Z",
                    "arguments": {"arg0": "supplier contract risk exposure"},
                },
                {
                    "chunkType": "toolResponse",
                    "callId": "c1",
                    "name": "searchViewsAndTables",
                    "createdAt": "2026-07-13T12:39:32.719Z",
                },
            ]
        }

    monkeypatch.setattr(chat, "get_messages", fake_get_messages)

    result = runner.invoke(app, ["chat", "history", "conv-1", "--gantt", "--ascii"])

    assert result.exit_code == 0
    assert "S1 searchViewsAndTables" in result.output


def test_chat_html_command_from_dump(tmp_path) -> None:
    dump_path = tmp_path / "history.json"
    output_path = tmp_path / "report.html"
    dump_path.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "chunkType": "toolRequest",
                        "callId": "c1",
                        "name": "searchViewsAndTables",
                        "createdAt": "2026-07-13T12:39:29.860Z",
                        "arguments": {"arg0": "supplier contract risk exposure"},
                    },
                    {
                        "chunkType": "toolResponse",
                        "callId": "c1",
                        "name": "searchViewsAndTables",
                        "createdAt": "2026-07-13T12:39:32.719Z",
                        "result": {"rows": []},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["chat", "html", "-o", str(output_path), "--dump-file", str(dump_path)])

    assert result.exit_code == 0
    report = output_path.read_text(encoding="utf-8")
    assert "<!doctype html>" in report
    assert '"conversationCount": 1' in report
    assert '"callId": "c1"' in report


def test_chat_html_command_accepts_positional_conversation_ids(monkeypatch, tmp_path) -> None:
    from drs.commands import chat

    client = type("DummyClient", (), {"close": AsyncMock()})()
    monkeypatch.setattr(chat, "_get_client", lambda: client)

    async def fake_get_all_messages(client, conversation_id, page_size=200):
        return {
            "data": [
                {
                    "chunkType": "toolRequest",
                    "callId": f"{conversation_id}-c1",
                    "name": "runSql",
                    "createdAt": "2026-07-31T12:10:33.467Z",
                    "arguments": {"sqlText": "select 1"},
                },
                {
                    "chunkType": "toolResponse",
                    "callId": f"{conversation_id}-c1",
                    "name": "runSql",
                    "createdAt": "2026-07-31T12:10:35.270Z",
                    "result": {"rows": []},
                },
            ]
        }

    monkeypatch.setattr(chat, "get_all_messages", fake_get_all_messages)

    output_path = tmp_path / "report.html"
    result = runner.invoke(app, ["chat", "html", "conv-1", "conv-2", "-o", str(output_path)])

    assert result.exit_code == 0
    report = output_path.read_text(encoding="utf-8")
    assert '"conversationCount": 2' in report
    assert '"id": "conv-1"' in report
    assert '"id": "conv-2"' in report


def test_search_command_passes_filter_and_max_results(monkeypatch) -> None:
    search_mock = AsyncMock(return_value={"results": []})
    close_mock = AsyncMock()
    client = MagicMock()
    client.search = search_mock
    client.close = close_mock

    monkeypatch.setattr("drs.cli.get_client", lambda: client)

    result = runner.invoke(app, ["search", "revenue", "--filter", 'category in ["JOB"]', "--max-results", "20"])

    assert result.exit_code == 0
    search_mock.assert_awaited_once_with(["revenue"], filter_='category in ["JOB"]', max_results=20)
    close_mock.assert_awaited_once()


def test_search_command_passes_next_page_token(monkeypatch) -> None:
    search_mock = AsyncMock(return_value={"results": []})
    close_mock = AsyncMock()
    client = MagicMock()
    client.search = search_mock
    client.close = close_mock

    monkeypatch.setattr("drs.cli.get_client", lambda: client)

    result = runner.invoke(app, ["search", "revenue", "--next-page-token", "token-123"])

    assert result.exit_code == 0
    search_mock.assert_awaited_once_with(["revenue"], filter_=None, max_results=None, next_page_token="token-123")
    close_mock.assert_awaited_once()


def test_search_command_passes_multiple_terms(monkeypatch) -> None:
    search_mock = AsyncMock(return_value={"results": []})
    close_mock = AsyncMock()
    client = MagicMock()
    client.search = search_mock
    client.close = close_mock

    monkeypatch.setattr("drs.cli.get_client", lambda: client)

    result = runner.invoke(app, ["search", "query a", "query b"])

    assert result.exit_code == 0
    search_mock.assert_awaited_once_with(["query a", "query b"], filter_=None, max_results=None)
    close_mock.assert_awaited_once()


def test_search_command_requires_a_term() -> None:
    result = runner.invoke(app, ["search"])

    assert result.exit_code != 0
