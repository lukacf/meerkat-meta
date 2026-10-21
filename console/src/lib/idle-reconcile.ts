import type { ConsoleAgent, ConsoleFrame } from "../types";

/// The member status poll is the server's authoritative idle signal: the
/// console projection reports no response phase and Core reports no open
/// run. A frame fold can still hold an owner open when the frames that
/// opened it never get a matching terminal (a runtime-started continuation
/// that finishes a paused interaction under another lineage, or a frame
/// with no ids at all), and the chat then reads "working" and queues every
/// send. When the poll says idle, every frame the console had seen when it
/// asked is settled.
export function agentReportsIdle(agent: Pick<ConsoleAgent, "response_phase" | "progress">): boolean {
  return agent.response_phase === null && agent.progress?.run_state === "idle";
}

/// Whether an idle poll result may settle one identity's fold. Every guard
/// must hold:
/// - the server reports the member idle;
/// - no frame for the identity arrived after the poll was issued (a frame
///   the server had not covered, or a new turn, must not be settled);
/// - no send of this console is in flight for the identity (its local
///   phase wins until the send settles);
/// - the fold actually holds something open.
export function shouldSettleIdleIdentity(args: {
  agent: Pick<ConsoleAgent, "response_phase" | "progress">;
  logVersionAtRequest: number | undefined;
  logVersionNow: number;
  sendInFlight: boolean;
  foldBusy: boolean;
  foldPhase: string | null;
}): boolean {
  if (!agentReportsIdle(args.agent)) return false;
  if (args.logVersionAtRequest === undefined || args.logVersionAtRequest !== args.logVersionNow) return false;
  if (args.sendInFlight) return false;
  return args.foldBusy || args.foldPhase !== null;
}

/// The settled watermark: the newest timestamp among the frames the console
/// has seen for the identity.
export function idleWatermarkFor(frames: readonly ConsoleFrame[]): number {
  let newest = Number.NEGATIVE_INFINITY;
  for (const frame of frames) {
    if (typeof frame.timestampMs === "number" && frame.timestampMs > newest) newest = frame.timestampMs;
  }
  return newest;
}

/// Whether a frame is newer than an idle watermark. A frame at or below it
/// (or without a timestamp) was settled by the idle poll.
export function isAfterIdleWatermark(frame: ConsoleFrame, watermark: number | undefined): boolean {
  if (watermark === undefined || watermark === Number.NEGATIVE_INFINITY) return true;
  return typeof frame.timestampMs === "number" && frame.timestampMs > watermark;
}

/// The frames a fold should still read after an idle watermark. Without a
/// watermark the same array is returned, so memoised derivations keep their
/// identity.
export function framesAfterIdleWatermark(
  frames: ConsoleFrame[],
  watermark: number | undefined,
): ConsoleFrame[] {
  if (watermark === undefined || watermark === Number.NEGATIVE_INFINITY) return frames;
  return frames.filter((frame) => isAfterIdleWatermark(frame, watermark));
}
