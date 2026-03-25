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
"""SSE (Server-Sent Events) stream parser for text/event-stream responses."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator


async def parse_sse_stream(byte_stream: AsyncIterator[bytes]) -> AsyncIterator[dict]:
    """Yield ``{"event": str, "data": dict}`` for each SSE event.

    Handles multi-line ``data:`` fields, ``event:`` types, comment lines
    (``:`` prefix), empty-line delimiters, and partial chunk buffering.
    """
    buf = ""
    event_type = "message"
    data_lines: list[str] = []

    async for chunk in byte_stream:
        buf += chunk.decode("utf-8", errors="replace")
        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            line = line.rstrip("\r")

            if not line:
                # Empty line = event boundary
                if data_lines:
                    raw = "\n".join(data_lines)
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        data = {"raw": raw}
                    yield {"event": event_type, "data": data}
                event_type = "message"
                data_lines = []
                continue

            if line.startswith(":"):
                # SSE comment — ignore
                continue

            if line.startswith("event:"):
                event_type = line[len("event:") :].strip()
            elif line.startswith("data:"):
                data_lines.append(line[len("data:") :].strip())

    # Flush any remaining content left in buf (server closed without trailing \n)
    if buf:
        for line in buf.split("\n"):
            line = line.rstrip("\r")
            if line.startswith("data:"):
                data_lines.append(line[len("data:") :].strip())
            elif line.startswith("event:"):
                event_type = line[len("event:") :].strip()

    # Flush any remaining data (stream ended without trailing blank line)
    if data_lines:
        raw = "\n".join(data_lines)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = {"raw": raw}
        yield {"event": event_type, "data": data}
