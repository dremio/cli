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
"""Tests for shared utilities — path parsing, validation, error handling, filtering."""

from __future__ import annotations

import pytest

from drs.utils import (
    parse_path, quote_path_sql, validate_job_state, validate_job_id,
    sanitize_input, sanitize_path, filter_fields,
)


class TestParsePath:
    def test_simple_path(self) -> None:
        assert parse_path("myspace.folder.table") == ["myspace", "folder", "table"]

    def test_quoted_components(self) -> None:
        assert parse_path('"My Source".folder.table') == ["My Source", "folder", "table"]

    def test_dots_inside_quotes(self) -> None:
        assert parse_path('"myspace"."my.table"') == ["myspace", "my.table"]

    def test_mixed_quoted_unquoted(self) -> None:
        assert parse_path('myspace."my.table"') == ["myspace", "my.table"]

    def test_single_component(self) -> None:
        assert parse_path("myspace") == ["myspace"]

    def test_quoted_single_component(self) -> None:
        assert parse_path('"My Source"') == ["My Source"]

    def test_empty_string(self) -> None:
        assert parse_path("") == []

    def test_strips_empty_parts(self) -> None:
        assert parse_path(".myspace.") == ["myspace"]

    def test_deeply_nested(self) -> None:
        assert parse_path("a.b.c.d.e") == ["a", "b", "c", "d", "e"]


class TestQuotePathSql:
    def test_simple(self) -> None:
        assert quote_path_sql("myspace.table") == '"myspace"."table"'

    def test_preserves_dots_in_quotes(self) -> None:
        assert quote_path_sql('"myspace"."my.table"') == '"myspace"."my.table"'


class TestValidateJobState:
    def test_valid_states(self) -> None:
        assert validate_job_state("FAILED") == "FAILED"
        assert validate_job_state("completed") == "COMPLETED"
        assert validate_job_state("Running") == "RUNNING"

    def test_invalid_state(self) -> None:
        with pytest.raises(ValueError, match="Invalid job state"):
            validate_job_state("BOGUS")


class TestValidateJobId:
    def test_valid_uuid(self) -> None:
        assert validate_job_id("12345678-1234-1234-1234-123456789abc") == "12345678-1234-1234-1234-123456789abc"

    def test_invalid_format(self) -> None:
        with pytest.raises(ValueError, match="Invalid job ID"):
            validate_job_id("not-a-uuid")

    def test_invalid_injection_attempt(self) -> None:
        with pytest.raises(ValueError, match="Invalid job ID"):
            validate_job_id("'; DROP TABLE --")


class TestSanitizeInput:
    def test_clean_input(self) -> None:
        assert sanitize_input("hello world") == "hello world"

    def test_rejects_control_chars(self) -> None:
        with pytest.raises(ValueError, match="control characters"):
            sanitize_input("hello\x00world")

    def test_rejects_newlines(self) -> None:
        with pytest.raises(ValueError, match="control characters"):
            sanitize_input("hello\nworld")


class TestSanitizePath:
    def test_clean_path(self) -> None:
        assert sanitize_path("myspace.folder.table") == "myspace.folder.table"

    def test_rejects_traversal(self) -> None:
        with pytest.raises(ValueError, match="traversal"):
            sanitize_path("myspace..folder")

    def test_rejects_question_mark(self) -> None:
        with pytest.raises(ValueError, match="special characters"):
            sanitize_path("myspace.folder?fields=name")

    def test_allows_dots_inside_quotes(self) -> None:
        assert sanitize_path('"my.source".table') == '"my.source".table'


class TestFilterFields:
    def test_filter_dict(self) -> None:
        data = {"name": "foo", "id": "123", "extra": "bar"}
        result = filter_fields(data, ["name", "id"])
        assert result == {"name": "foo", "id": "123"}

    def test_filter_nested(self) -> None:
        data = {"columns": [{"name": "id", "type": "INT", "nullable": True}]}
        result = filter_fields(data, ["columns.name", "columns.type"])
        assert result == {"columns": [{"name": "id", "type": "INT"}]}

    def test_filter_list(self) -> None:
        data = [{"name": "a", "extra": 1}, {"name": "b", "extra": 2}]
        result = filter_fields(data, ["name"])
        assert result == [{"name": "a"}, {"name": "b"}]

    def test_preserves_structural_keys(self) -> None:
        data = {"rows": [{"name": "a", "extra": 1}], "meta": "info"}
        result = filter_fields(data, ["name"])
        assert "rows" in result
        assert result["rows"] == [{"name": "a"}]

    def test_empty_fields_returns_original(self) -> None:
        data = {"name": "foo"}
        assert filter_fields(data, []) is data
