/**
 * Panel data refreshes are event-driven. On a server slower than the event
 * rate they used to overlap without limit: every tool or lifecycle frame from
 * any agent re-queried topology (and every other docked panel kind), even for
 * panels in hidden tabs, until the browser's per-origin connection limit
 * starved every other console request. These pin the contract: hidden panels
 * are not refreshed, and a visible panel has at most one request in flight,
 * with requests made meanwhile coalesced into one trailing refresh.
 */
import React from "react";
import { act, render } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { ConsoleApp } from "../ConsoleApp";
import { buildControlTarget } from "./adapters";
import { CONSOLE_RPC_METHODS } from "./contract";
import type { MobKitConsoleTransport } from "./headless";
import type { ConsoleFrame } from "../types";

const CHAT = "identity:agent-000";

function experience() {
  return {
    contract_version: "test",
    runtime_id: "refresh-runtime",
    storage_scope: "refresh-principal",
    console_config: {},
    console_policy: {},
    agent_sidebar: {
      live_snapshot: {
        agents: [0, 1].map((i) => ({
          identity: `identity:agent-00${i}`,
          member_id: `identity:agent-00${i}`,
          agent_id: `identity:agent-00${i}`,
          label: `Agent ${i}`,
          kind: "member",
          role: "worker",
          state: "running",
          addressable: true,
          affordances: {},
          model_capabilities: { image_input: false },
        })),
      },
    },
    activity_feed: { filter_presets: [], active_preset_id: "all" },
  };
}

interface Harness extends MobKitConsoleTransport {
  live?: (frame: ConsoleFrame) => void;
  topologyQueries: number;
  inFlight: number;
  maxInFlight: number;
  release: Array<() => void>;
}

function transport(): Harness {
  const harness: Harness = {
    topologyQueries: 0,
    inFlight: 0,
    maxInFlight: 0,
    release: [],
    loadExperience: async () => experience() as never,
    loadModules: async () => ({ modules: [] }) as never,
    capabilities: async () => ({ version: "test", methods: Object.values(CONSOLE_RPC_METHODS) }) as never,
    queryTimeline: async () => ({ frames: [], available: true }) as never,
    subscribeTimeline: (_input, onFrame) => {
      harness.live = onFrame;
      return () => { harness.live = undefined; };
    },
    send: async (input) => ({ interaction_id: "sent", identity: input.identity, cursor: "console:x" }) as never,
    executeCommand: async (input) => {
      if (input.command === "topologyQuery") {
        harness.topologyQueries += 1;
        harness.inFlight += 1;
        harness.maxInFlight = Math.max(harness.maxInFlight, harness.inFlight);
        // A server that answers only when the test says so.
        await new Promise<void>((resolve) => harness.release.push(resolve));
        harness.inFlight -= 1;
        return { command: input.command, accepted: true, result: { members: [], edges: [] } } as never;
      }
      return { command: input.command, accepted: true, result: {} } as never;
    },
  };
  return harness;
}

function seedDock(topologyVisible: boolean): void {
  const topology = { id: "panel-1", mode: "console", target: buildControlTarget("topology") };
  const chat = {
    id: "panel-2",
    mode: "console",
    target: { id: `chat:${CHAT}`, kind: "agent-chat", title: "Agent 0", identity: CHAT, memberId: CHAT },
  };
  window.localStorage.setItem(`mobkit-console-dock-state:${JSON.stringify(["", "refresh-principal"])}`, JSON.stringify({
    tabs: [
      { id: "tab-1", presetId: "single", layout: { kind: "panel", panelId: "panel-1" } },
      { id: "tab-2", presetId: "single", layout: { kind: "panel", panelId: "panel-2" } },
    ],
    panels: [topology, chat],
    activeTabId: topologyVisible ? "tab-1" : "tab-2",
    focusedPanelId: topologyVisible ? "panel-1" : "panel-2",
  }));
}

async function wait(ms: number): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, ms));
  });
}

/// Tool activity from another agent: each frame schedules a panel refresh.
async function agentActivity(harness: Harness, frames: number): Promise<void> {
  for (let i = 0; i < frames; i += 1) {
    await act(async () => {
      harness.live?.({
        id: `tool:${Date.now()}:${i}`,
        event: "tool_call_requested",
        identity: "identity:agent-001",
        interactionId: "busy",
        timestampMs: Date.now(),
        cursor: `console:${1000 + i}`,
        data: { id: `call-${i}`, name: "read_file" },
      });
    });
    await wait(200);
  }
}

describe("event-driven panel refresh", () => {
  beforeEach(() => window.localStorage.clear());
  afterEach(() => window.localStorage.clear());

  it("never queries topology for a panel in a hidden tab", async () => {
    seedDock(false);
    const harness = transport();
    const view = render(<ConsoleApp baseUrl="" transport={harness} />);
    await wait(300);
    await agentActivity(harness, 12);
    expect(harness.topologyQueries).toBe(0);
    view.unmount();
  }, 30_000);

  it("keeps one topology query in flight and coalesces events into trailing queries", async () => {
    seedDock(true);
    const harness = transport();
    const view = render(<ConsoleApp baseUrl="" transport={harness} />);
    await wait(300);
    await agentActivity(harness, 12);
    expect(harness.maxInFlight).toBe(1);
    expect(harness.topologyQueries).toBe(1);
    // The slow answers arrive one by one. The events that arrived meanwhile
    // collapse into trailing refreshes (one per coalescing level: the
    // event-driven refresh and the topology refresh), never overlapping, and
    // the queries stop once the backlog is answered.
    for (let answered = 0; answered < 5 && harness.release.length; answered += 1) {
      await act(async () => { harness.release.shift()?.(); });
      await wait(300);
    }
    expect(harness.release).toHaveLength(0);
    expect(harness.maxInFlight).toBe(1);
    expect(harness.topologyQueries).toBeLessThanOrEqual(3);
    const settled = harness.topologyQueries;
    await wait(600);
    expect(harness.topologyQueries).toBe(settled);
    view.unmount();
  }, 30_000);
});
