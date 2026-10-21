import type { ConsoleAgent, ConsoleFrame } from "../types";
import { cursorSeq } from "./identity-log";

/// The member status poll is the server's authoritative idle signal: the
/// console projection reports no response phase and Core reports no open
/// run. A frame fold can still hold an owner open when the frames that
/// opened it never get a matching terminal (a runtime-started continuation
/// that finishes a paused interaction under another lineage, or a frame
/// with no ids at all), and the chat then reads "working" and queues every
/// send. Two idle polls in a row, with no frame for the member between or
/// during them, settle every frame the console had seen.
export function agentReportsIdle(agent: Pick<ConsoleAgent, "response_phase" | "progress">): boolean {
  return agent.response_phase === null && agent.progress?.run_state === "idle";
}

/// What one status poll does to one identity's fold:
/// - "reset": the poll does not count toward settling (the server reports
///   the member busy, a frame arrived while the poll was in flight, this
///   console's own send is in flight, or nothing is held open);
/// - "arm": the first idle poll; nothing changes yet;
/// - "settle": the second idle poll in a row with no frame since the first.
///
/// One idle poll is not enough: a run the runtime starts itself shows no
/// response phase until its first output, and the run state the poll reads
/// can predate the run, so a single poll can catch a live run looking idle.
export type IdleSettlement = "reset" | "arm" | "settle";

export function decideIdleSettlement(args: {
  agent: Pick<ConsoleAgent, "response_phase" | "progress">;
  logVersionAtRequest: number | undefined;
  logVersionNow: number;
  sendInFlight: boolean;
  foldBusy: boolean;
  foldPhase: string | null;
  armedAtVersion: number | undefined;
}): IdleSettlement {
  if (!agentReportsIdle(args.agent)) return "reset";
  if (args.logVersionAtRequest === undefined || args.logVersionAtRequest !== args.logVersionNow) return "reset";
  if (args.sendInFlight) return "reset";
  if (!args.foldBusy && args.foldPhase === null) return "reset";
  return args.armedAtVersion === args.logVersionNow ? "settle" : "arm";
}

/// The newest frame the console had seen when the member was settled, in
/// transcript order: timestamp, then the console cursor sequence.
export interface IdleWatermark {
  timestampMs: number;
  cursorSeq: number | null;
}

export function idleWatermarkFor(frames: readonly ConsoleFrame[]): IdleWatermark | undefined {
  let newest: IdleWatermark | undefined;
  for (const frame of frames) {
    if (typeof frame.timestampMs !== "number") continue;
    const seq = cursorSeq(frame.cursor);
    if (!newest || frame.timestampMs > newest.timestampMs
      || (frame.timestampMs === newest.timestampMs && seq !== null
        && (newest.cursorSeq === null || seq > newest.cursorSeq))) {
      newest = { timestampMs: frame.timestampMs, cursorSeq: seq };
    }
  }
  return newest;
}

/// Whether a frame is newer than an idle watermark. A frame at or before it
/// was settled, and so is a frame without a timestamp. A frame stamped in
/// the watermark's millisecond is newer only when both carry a console
/// cursor and its cursor is later; without cursors it counts as settled.
export function isAfterIdleWatermark(frame: ConsoleFrame, watermark: IdleWatermark | undefined): boolean {
  if (!watermark) return true;
  if (typeof frame.timestampMs !== "number") return false;
  if (frame.timestampMs !== watermark.timestampMs) return frame.timestampMs > watermark.timestampMs;
  const seq = cursorSeq(frame.cursor);
  return seq !== null && watermark.cursorSeq !== null && seq > watermark.cursorSeq;
}

/// The frames a fold should still read after an idle watermark. Without a
/// watermark the same array is returned, so memoised derivations keep their
/// identity.
export function framesAfterIdleWatermark(
  frames: ConsoleFrame[],
  watermark: IdleWatermark | undefined,
): ConsoleFrame[] {
  if (!watermark) return frames;
  return frames.filter((frame) => isAfterIdleWatermark(frame, watermark));
}
