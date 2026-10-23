/**
 * SSE (Server-Sent Events) parsing and encoding.
 *
 * Internal module — not exported from the barrel. Used by the runtime
 * SSE bridge for streaming agent and mob events.
 */

// -- Types ----------------------------------------------------------------

export interface SseEvent {
  readonly id: string | null;
  readonly event: string;
  readonly data: string;
}

// -- Encoding -------------------------------------------------------------

/** Serialize an SSE event to the wire format. */
export function encodeSseEvent(event: SseEvent): string {
  const lines: string[] = [];
  if (event.id !== null) {
    lines.push(`id: ${event.id}`);
  }
  if (event.event !== "message") {
    lines.push(`event: ${event.event}`);
  }
  for (const line of event.data.split("\n")) {
    lines.push(`data: ${line}`);
  }
  lines.push("");
  lines.push("");
  return lines.join("\n");
}

// -- Parsing --------------------------------------------------------------

/**
 * Parse an SSE byte stream into typed {@link SseEvent} objects.
 *
 * Handles `id:`, `event:`, `data:` fields and comment lines (`:` prefix).
 * Events are emitted on blank-line boundaries per the SSE spec.
 *
 * The stream is decoded as UTF-8 incrementally, so a character that a chunk
 * boundary splits arrives whole. Bytes that are not UTF-8 throw the decoder's
 * `TypeError` instead of being replaced with U+FFFD.
 */
export async function* parseSseStream(
  body: AsyncIterable<Uint8Array>,
): AsyncGenerator<SseEvent, void, undefined> {
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "";
  let currentId: string | null = null;
  let currentEvent = "message";
  const currentData: string[] = [];

  for await (const chunk of body) {
    buffer += decoder.decode(chunk, { stream: true });

    while (buffer.includes("\n")) {
      const idx = buffer.indexOf("\n");
      let line = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 1);
      line = line.replace(/\r$/, "");

      if (line === "") {
        // Blank line — emit event if we have data
        if (currentData.length > 0) {
          yield {
            id: currentId,
            event: currentEvent,
            data: currentData.join("\n"),
          };
        }
        currentId = null;
        currentEvent = "message";
        currentData.length = 0;
      } else if (line.startsWith(":")) {
        // Comment — skip
        continue;
      } else if (line.startsWith("id:")) {
        // Pre-fix only matched `id: ` with the optional space, so SSE-
        // spec-legal `id:42` (no space) was dropped, breaking resume
        // cursors. Strip a single optional leading space, mirroring
        // the existing `data:` fallback below.
        const v = line.slice(3);
        currentId = v.startsWith(" ") ? v.slice(1) : v;
      } else if (line.startsWith("event:")) {
        const v = line.slice(6);
        currentEvent = v.startsWith(" ") ? v.slice(1) : v;
      } else if (line.startsWith("data:")) {
        const v = line.slice(5);
        currentData.push(v.startsWith(" ") ? v.slice(1) : v);
      }
    }
  }

  // Flush any remaining event if the stream ended without a trailing blank line.
  if (currentData.length > 0) {
    yield {
      id: currentId,
      event: currentEvent,
      data: currentData.join("\n"),
    };
  }
}
