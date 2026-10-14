"""SSE streaming bridge for ASGI integration."""
from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator


class SseEvent:
    __slots__ = ("id", "event", "data")

    def __init__(self, *, id: str | None = None, event: str = "message", data: str = ""):
        self.id = id
        self.event = event
        self.data = data

    def encode(self) -> str:
        lines: list[str] = []
        if self.id is not None:
            lines.append(f"id: {self.id}")
        if self.event != "message":
            lines.append(f"event: {self.event}")
        for line in self.data.split("\n"):
            lines.append(f"data: {line}")
        lines.append("")
        lines.append("")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"event": self.event, "data": self.data}
        if self.id is not None:
            result["id"] = self.id
        return result


class SseEventStream:
    """Async iterator yielding SSE-formatted strings.

    Use with StreamingResponse(stream, media_type="text/event-stream").
    """

    def __init__(
        self,
        source: AsyncIterator[dict[str, Any]],
        *,
        keep_alive_interval: float = 15.0,
    ):
        self._source = source
        self._keep_alive_interval = keep_alive_interval

    async def __aiter__(self) -> AsyncIterator[str]:
        source_iter = self._source.__aiter__()
        while True:
            try:
                event_data = await asyncio.wait_for(
                    source_iter.__anext__(),
                    timeout=self._keep_alive_interval,
                )
                data_str = (
                    json.dumps(event_data.get("data", {}))
                    if not isinstance(event_data.get("data"), str)
                    else event_data.get("data", "")
                )
                event = SseEvent(
                    id=event_data.get("id"),
                    event=event_data.get("event", "message"),
                    data=data_str,
                )
                yield event.encode()
            except asyncio.TimeoutError:
                yield ": keep-alive\n\n"
            except StopAsyncIteration:
                break


async def parse_sse_stream(
    response_body: AsyncIterator[bytes],
) -> AsyncIterator[SseEvent]:
    """Parse an SSE byte stream into SseEvent objects."""
    buffer = ""
    current_id: str | None = None
    current_event = "message"
    current_data: list[str] = []

    async for chunk in response_body:
        buffer += chunk.decode("utf-8", errors="replace")
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            line = line.rstrip("\r")

            if not line:
                if current_data:
                    yield SseEvent(
                        id=current_id,
                        event=current_event,
                        data="\n".join(current_data),
                    )
                current_id = None
                current_event = "message"
                current_data = []
            elif line.startswith(":"):
                continue
            elif line.startswith("id: "):
                current_id = line[4:]
            elif line.startswith("event: "):
                current_event = line[7:]
            elif line.startswith("data: "):
                current_data.append(line[6:])
            elif line.startswith("data:"):
                current_data.append(line[5:])
