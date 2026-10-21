import assert from "node:assert/strict";
import test from "node:test";
import { ConsoleActivityProjection, resolvePanelResponsePhase } from "@console-core";
import type { ConsoleAgent, ConsoleFrame } from "../types";
import {
  agentReportsIdle,
  framesAfterIdleWatermark,
  idleWatermarkFor,
  isAfterIdleWatermark,
  shouldSettleIdleIdentity,
} from "./idle-reconcile";

const scope = { runtimeKey: "runtime", identity: "original:main", sessionId: "session" };
let sequence = 0;
function frame(event: string, timestampMs: number, extra: Partial<ConsoleFrame> = {}): ConsoleFrame {
  sequence += 1;
  return {
    ...scope,
    id: `${event}:${sequence}`,
    cursor: `console:${sequence}`,
    event,
    sourceKind: "console_event",
    timestampMs,
    data: {},
    ...extra,
  } as ConsoleFrame;
}

function fold(frames: ConsoleFrame[]): ConsoleActivityProjection {
  const projection = new ConsoleActivityProjection();
  for (const row of frames) projection.fold(row);
  return projection;
}

/// The user's turn stops at a host callback, and a runtime-started
/// continuation finishes the work under its own lineage: the paused
/// interaction never sees a terminal of its own.
function callbackPauseThenContinuation(): ConsoleFrame[] {
  return [
    frame("user_input", 1000, { interactionId: "I1", status: "delivered" }),
    frame("run_started", 1001, { interactionId: "I1", runId: "R1" }),
    frame("tool_call_requested", 1002, { interactionId: "I1", runId: "R1" }),
    frame("turn_completed", 1003, { interactionId: "I1", runId: "R1", data: { stop_reason: "tool_use" } }),
    frame("interaction_callback_pending", 1004, { interactionId: "I1" }),
    frame("run_started", 2000, { runId: "R2" }),
    frame("text_delta", 2001, { runId: "R2" }),
    frame("text_complete", 2002, { runId: "R2" }),
    frame("turn_completed", 2003, { runId: "R2", data: { stop_reason: "end_turn" } }),
    frame("interaction_complete", 2004, { runId: "R2" }),
  ];
}

/// The user's turn settles, then a frame with no lifecycle ids (the approved
/// setup's tool completion) arrives before the continuation runs.
function idlessFrameThenContinuation(): ConsoleFrame[] {
  return [
    frame("user_input", 1000, { interactionId: "I1", status: "delivered" }),
    frame("run_started", 1001, { interactionId: "I1", runId: "R1" }),
    frame("text_complete", 1002, { interactionId: "I1", runId: "R1" }),
    frame("turn_completed", 1003, { interactionId: "I1", runId: "R1", data: { stop_reason: "end_turn" } }),
    frame("interaction_complete", 1004, { interactionId: "I1", runId: "R1" }),
    frame("tool_execution_completed", 1500),
    frame("run_started", 2000, { runId: "R2" }),
    frame("text_delta", 2001, { runId: "R2" }),
    frame("turn_completed", 2003, { runId: "R2", data: { stop_reason: "end_turn" } }),
    frame("interaction_complete", 2004, { runId: "R2" }),
  ];
}

const idleAgent: Pick<ConsoleAgent, "response_phase" | "progress"> = {
  response_phase: null,
  progress: { run_state: "idle", in_flight_work: 0, last_progress_at_ms: 0, last_progress_event: "", health: "ok" },
};

for (const [name, build] of [
  ["a paused interaction a continuation finished", callbackPauseThenContinuation],
  ["an anonymous owner opened by an id-less frame", idlessFrameThenContinuation],
] as const) {
  test(`${name} holds the fold open until the idle watermark settles it`, () => {
    const frames = build();
    const stuck = fold(frames);
    assert.equal(stuck.busy, true, "the frames alone leave an owner open: the stale 'working'");
    assert.notEqual(
      resolvePanelResponsePhase({ frames, serverPhase: null }),
      null,
      "the chat reads 'working' although the server is idle",
    );

    const watermark = idleWatermarkFor(frames);
    assert.equal(watermark, 2004);
    const settled = framesAfterIdleWatermark(frames, watermark);
    assert.deepEqual(settled, []);
    assert.equal(fold(settled).busy, false);
    assert.equal(resolvePanelResponsePhase({ frames: settled, serverPhase: null }), null);
  });
}

test("work that starts after the idle watermark is busy again", () => {
  const frames = callbackPauseThenContinuation();
  const watermark = idleWatermarkFor(frames);
  const next = [
    ...frames,
    frame("user_input", 3000, { interactionId: "I2", status: "delivered" }),
    frame("run_started", 3001, { interactionId: "I2", runId: "R3" }),
  ];
  const projection = fold(framesAfterIdleWatermark(next, watermark));
  assert.equal(projection.busy, true);
  assert.equal(projection.phase, "waiting");
  projection.fold(frame("interaction_complete", 3005, { interactionId: "I2", runId: "R3" }));
  assert.equal(projection.busy, false);
});

test("a frame at or before the watermark is settled, a newer one is not", () => {
  assert.equal(isAfterIdleWatermark(frame("tool_execution_completed", 2004), 2004), false);
  assert.equal(isAfterIdleWatermark(frame("tool_execution_completed", 1500), 2004), false);
  assert.equal(isAfterIdleWatermark(frame("run_started", 2005), 2004), true);
  assert.equal(isAfterIdleWatermark({ ...frame("run_started", 0), timestampMs: undefined }, 2004), false);
  assert.equal(isAfterIdleWatermark(frame("run_started", 1), undefined), true);
});

test("without a watermark the frames pass through unchanged", () => {
  const frames = callbackPauseThenContinuation();
  assert.equal(framesAfterIdleWatermark(frames, undefined), frames);
  assert.equal(framesAfterIdleWatermark(frames, Number.NEGATIVE_INFINITY), frames);
});

test("only an idle status poll settles: response phase null and no open run", () => {
  assert.equal(agentReportsIdle(idleAgent), true);
  assert.equal(agentReportsIdle({ ...idleAgent, response_phase: "waiting" }), false);
  assert.equal(agentReportsIdle({ ...idleAgent, response_phase: undefined }), false, "a silent server is not idle");
  assert.equal(agentReportsIdle({ ...idleAgent, progress: { ...idleAgent.progress!, run_state: "run_open" } }), false);
  assert.equal(agentReportsIdle({ response_phase: null }), false, "no run state is not idle");
});

test("each guard keeps the fold as it is", () => {
  const base = {
    agent: idleAgent,
    logVersionAtRequest: 7,
    logVersionNow: 7,
    sendInFlight: false,
    foldBusy: true,
    foldPhase: "waiting",
  };
  assert.equal(shouldSettleIdleIdentity(base), true);
  assert.equal(shouldSettleIdleIdentity({ ...base, agent: { ...idleAgent, response_phase: "generating" } }), false,
    "never while the server is not idle");
  assert.equal(shouldSettleIdleIdentity({ ...base, logVersionNow: 8 }), false,
    "never when a frame arrived after the poll was issued");
  assert.equal(shouldSettleIdleIdentity({ ...base, logVersionAtRequest: undefined }), false,
    "never for a log the poll did not cover");
  assert.equal(shouldSettleIdleIdentity({ ...base, sendInFlight: true }), false,
    "never while this console's own send is in flight");
  assert.equal(shouldSettleIdleIdentity({ ...base, foldBusy: false, foldPhase: null }), false,
    "nothing to settle");
  assert.equal(shouldSettleIdleIdentity({ ...base, foldBusy: false, foldPhase: "waiting" }), true,
    "a stale phase alone is settled too");
});
