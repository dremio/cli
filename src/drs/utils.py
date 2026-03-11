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
"""Shared utilities for drs — path parsing, input validation, error handling."""

from __future__ import annotations

import re
from typing import Any

import httpx


# -- Input hardening (defend against agent hallucinations) --

CONTROL_CHAR_PATTERN = re.compile(r"[\x00-\x1f]", re.UNICODE)
DANGEROUS_PATH_PATTERN = re.compile(r"\.\.|[?#%]")


def sanitize_input(value: str, field_name: str = "input") -> str:
    """Reject control characters and suspicious patterns in agent-provided input.

    Agents hallucinate path traversals (../../.ssh), embedded query params
    (fileId?fields=name), double-encoded strings (%2e%2e), and control chars.
    """
    if CONTROL_CHAR_PATTERN.search(value):
        raise ValueError(f"Invalid {field_name}: contains control characters")
    return value


def sanitize_path(path: str) -> str:
    """Validate a Dremio catalog path — reject traversal and injection attempts."""
    sanitize_input(path, "path")
    if DANGEROUS_PATH_PATTERN.search(path.replace('"', "")):
        # Allow dots inside quoted identifiers (handled by parse_path)
        # but reject .. traversal, ?, #, % outside quotes
        stripped = ""
        in_quotes = False
        for ch in path:
            if ch == '"':
                in_quotes = not in_quotes
            elif not in_quotes:
                stripped += ch
        if DANGEROUS_PATH_PATTERN.search(stripped):
            raise ValueError(
                f"Invalid path '{path}': contains '..' traversal or special characters (?, #, %)"
            )
    return path


def filter_fields(data: Any, fields: list[str]) -> Any:
    """Filter response data to only include specified fields (context window discipline).

    Supports dot-notation for nested fields: 'columns.name,columns.type' filters each
    dict in 'columns' to only include 'name' and 'type'.
    """
    if not fields:
        return data

    if isinstance(data, dict):
        result: dict[str, Any] = {}
        # Group nested fields by top-level key so we merge them properly
        nested: dict[str, list[str]] = {}
        for field in fields:
            if "." in field:
                top, rest = field.split(".", 1)
                nested.setdefault(top, []).append(rest)
            elif field in data:
                result[field] = data[field]
        for top, sub_fields in nested.items():
            if top in data:
                val = data[top]
                if isinstance(val, list):
                    result[top] = [filter_fields(item, sub_fields) for item in val]
                else:
                    result[top] = filter_fields(val, sub_fields)
        # Always include structural keys
        for key in ("rows", "data", "entities"):
            if key in data and key not in result:
                result[key] = [filter_fields(item, fields) for item in data[key]] if isinstance(data[key], list) else data[key]
        return result if result else data
    elif isinstance(data, list):
        return [filter_fields(item, fields) for item in data]
    return data


def parse_path(path: str) -> list[str]:
    """Parse a dot-separated Dremio path, respecting double-quoted identifiers.

    Examples:
        "myspace.folder.table"          -> ["myspace", "folder", "table"]
        '"My Source".folder."my.table"'  -> ["My Source", "folder", "my.table"]
        'myspace."folder"."table"'       -> ["myspace", "folder", "table"]
    """
    sanitize_path(path)
    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    i = 0

    while i < len(path):
        ch = path[i]
        if ch == '"':
            in_quotes = not in_quotes
        elif ch == '.' and not in_quotes:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1

    if current or path.endswith("."):
        parts.append("".join(current))

    # Filter empty parts from leading/trailing dots
    return [p for p in parts if p]


def quote_path_sql(path: str) -> str:
    """Convert a dot-separated path to SQL-quoted form: "part1"."part2"."part3"."""
    parts = parse_path(path)
    return ".".join(f'"{p}"' for p in parts)


# Valid job states for filtering
VALID_JOB_STATES = {
    "COMPLETED", "FAILED", "RUNNING", "CANCELED", "CANCELLED",
    "PLANNING", "ENQUEUED", "STARTING", "PENDING",
}

UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)


def validate_job_state(state: str) -> str:
    """Validate and normalize a job state filter value."""
    upper = state.upper()
    if upper not in VALID_JOB_STATES:
        raise ValueError(
            f"Invalid job state '{state}'. Valid states: {', '.join(sorted(VALID_JOB_STATES))}"
        )
    return upper


def validate_job_id(job_id: str) -> str:
    """Validate a job ID looks like a UUID."""
    if not UUID_PATTERN.match(job_id):
        raise ValueError(f"Invalid job ID format: '{job_id}'. Expected UUID format.")
    return job_id


class DremioAPIError(Exception):
    """Structured error from a Dremio API call."""

    def __init__(self, status_code: int, message: str, url: str = "") -> None:
        self.status_code = status_code
        self.message = message
        self.url = url
        super().__init__(f"HTTP {status_code}: {message}")

    def to_dict(self) -> dict:
        d: dict[str, Any] = {"error": self.message, "status_code": self.status_code}
        if self.url:
            d["url"] = self.url
        return d


def handle_api_error(exc: httpx.HTTPStatusError) -> DremioAPIError:
    """Convert an httpx HTTP error to a structured DremioAPIError."""
    status = exc.response.status_code
    url = str(exc.request.url)

    if status == 401:
        msg = "Authentication failed — check your PAT token"
    elif status == 403:
        msg = "Permission denied — insufficient privileges for this operation"
    elif status == 404:
        msg = "Not found — check that the path or ID exists"
    else:
        try:
            body = exc.response.json()
            msg = body.get("errorMessage", body.get("message", str(exc)))
        except Exception:
            msg = exc.response.text or str(exc)

    return DremioAPIError(status, msg, url)
