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
"""Output formatting for drs CLI — JSON, CSV, or pretty table."""

from __future__ import annotations

import csv
import io
import json
import sys
from enum import Enum
from typing import Any


class OutputFormat(str, Enum):
    json = "json"
    csv = "csv"
    pretty = "pretty"


def render(data: Any, fmt: OutputFormat = OutputFormat.json) -> str:
    """Render data to the specified format string."""
    if fmt == OutputFormat.json:
        return json.dumps(data, indent=2, default=str)
    elif fmt == OutputFormat.csv:
        return _to_csv(data)
    elif fmt == OutputFormat.pretty:
        return _to_pretty(data)
    return json.dumps(data, indent=2, default=str)


def output(data: Any, fmt: OutputFormat = OutputFormat.json, fields: str | None = None) -> None:
    """Render and print data to stdout, optionally filtering to specified fields."""
    if fields:
        from drs.utils import filter_fields
        data = filter_fields(data, [f.strip() for f in fields.split(",")])
    print(render(data, fmt))


def _to_csv(data: Any) -> str:
    if isinstance(data, dict):
        rows = data.get("rows", data.get("data", [data]))
    elif isinstance(data, list):
        rows = data
    else:
        rows = [data]

    if not rows:
        return ""

    buf = io.StringIO()
    if isinstance(rows[0], dict):
        writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    else:
        writer = csv.writer(buf)
        for row in rows:
            writer.writerow(row if isinstance(row, (list, tuple)) else [row])
    return buf.getvalue().rstrip()


def _to_pretty(data: Any) -> str:
    if isinstance(data, dict):
        rows = data.get("rows", data.get("data", None))
        if rows is None:
            return _dict_table(data)
        return _list_table(rows)
    elif isinstance(data, list):
        return _list_table(data)
    return str(data)


def _dict_table(d: dict) -> str:
    if not d:
        return "(empty)"
    max_key = max(len(str(k)) for k in d.keys())
    lines = []
    for k, v in d.items():
        lines.append(f"{str(k).ljust(max_key)}  {v}")
    return "\n".join(lines)


def _list_table(rows: list) -> str:
    if not rows:
        return "(no results)"
    if not isinstance(rows[0], dict):
        return "\n".join(str(r) for r in rows)

    cols = list(rows[0].keys())
    widths = {c: len(c) for c in cols}
    str_rows = []
    for row in rows:
        sr = {c: str(row.get(c, "")) for c in cols}
        for c in cols:
            widths[c] = max(widths[c], len(sr[c]))
        str_rows.append(sr)

    header = "  ".join(c.ljust(widths[c]) for c in cols)
    sep = "  ".join("-" * widths[c] for c in cols)
    lines = [header, sep]
    for sr in str_rows:
        lines.append("  ".join(sr[c].ljust(widths[c]) for c in cols))
    return "\n".join(lines)


def error(msg: str) -> None:
    """Print error to stderr."""
    print(f"Error: {msg}", file=sys.stderr)
