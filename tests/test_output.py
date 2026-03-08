"""Tests for output formatting."""

from __future__ import annotations

import json

from drs.output import OutputFormat, render


def test_json_output() -> None:
    data = {"rows": [{"id": 1, "name": "test"}]}
    result = render(data, OutputFormat.json)
    parsed = json.loads(result)
    assert parsed["rows"][0]["name"] == "test"


def test_csv_output_from_rows() -> None:
    data = {"rows": [{"id": "1", "name": "alice"}, {"id": "2", "name": "bob"}]}
    result = render(data, OutputFormat.csv)
    lines = [l.strip() for l in result.strip().splitlines()]
    assert lines[0] == "id,name"
    assert lines[1] == "1,alice"


def test_csv_output_from_list() -> None:
    data = [{"a": "1"}, {"a": "2"}]
    result = render(data, OutputFormat.csv)
    lines = [l.strip() for l in result.strip().splitlines()]
    assert lines[0] == "a"
    assert len(lines) == 3


def test_pretty_output_table() -> None:
    data = {"rows": [{"id": "1", "name": "alice"}, {"id": "2", "name": "bob"}]}
    result = render(data, OutputFormat.pretty)
    assert "id" in result
    assert "alice" in result
    assert "---" in result or "--" in result


def test_pretty_output_dict() -> None:
    data = {"key1": "value1", "key2": "value2"}
    result = render(data, OutputFormat.pretty)
    assert "key1" in result
    assert "value1" in result


def test_pretty_empty_list() -> None:
    data = {"rows": []}
    result = render(data, OutputFormat.pretty)
    assert "no results" in result.lower()


def test_output_with_fields_filter(capsys) -> None:
    from drs.output import output
    data = {"name": "foo", "id": "123", "extra": "bar"}
    output(data, OutputFormat.json, fields="name,id")
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert "name" in parsed
    assert "id" in parsed
    assert "extra" not in parsed
