/**
 * Host-provided attention items through the whole console: configured sources
 * reach the "Needs you" count and the inbox, and an aggregate console (no
 * gating surface) still opens a view that lists them.
 */
import React from "react";
import { act, fireEvent, render, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ConsoleApp } from "../ConsoleApp";
import { CONSOLE_RPC_METHODS } from "./contract";
import type { MetaConsoleTransport } from "./headless";

const SOURCE = "/host/attention";

function experience(runtimeId: string, options: { readOnly?: boolean; decideHref?: string } = {}) {
  return {
    contract_version: "test",
    runtime_id: runtimeId,
    console_config: { attention: { sources: [{ id: "host", label: "Host", url: SOURCE, ...(options.decideHref ? { decide_href: options.decideHref } : {}) }] } },
    console_policy: options.readOnly ? { read_only: true } : {},
    agent_sidebar: { live_snapshot: { agents: [] } },
    activity_feed: { filter_presets: [], active_preset_id: "all" },
  };
}

function transport(runtimeId: string, options: { readOnly?: boolean; decideHref?: string; pending?: unknown[] } = {}): MetaConsoleTransport {
  return {
    loadExperience: async () => experience(runtimeId, options) as never,
    loadModules: async () => ({ modules: [] }) as never,
    capabilities: async () => ({ version: "test", methods: Object.values(CONSOLE_RPC_METHODS) }) as never,
    queryTimeline: async () => ({ frames: [], available: true }) as never,
    subscribeTimeline: () => () => {},
    send: async (input) => ({ interaction_id: "sent", identity: input.identity, cursor: "console:x" }) as never,
    executeCommand: async (input) => ({ command: input.command, accepted: true, result: { pending: options.pending || [] } }) as never,
  } as MetaConsoleTransport;
}

async function settle(): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 300));
  });
}

describe("host-provided attention in the console", () => {
  let realFetch: typeof fetch;
  beforeEach(() => {
    window.localStorage?.clear?.();
    realFetch = window.fetch;
    window.fetch = vi.fn(async (url: RequestInfo | URL) => String(url) === SOURCE
      ? new Response(JSON.stringify({ items: [{ id: "a", title: "Confirm a setting", href: "/host/items/a" }] }), { status: 200 })
      : new Response("not found", { status: 404 })) as typeof fetch;
  });
  afterEach(() => {
    window.fetch = realFetch;
    window.localStorage?.clear?.();
  });

  it("an aggregate console counts host items and opens a view that lists them", async () => {
    const view = render(<ConsoleApp baseUrl="" transport={transport("console-aggregator")} />);
    await settle();
    const attention = view.getByTestId("approval-attention");
    expect(attention.querySelector(".cc-approval-attention__count")?.textContent).toBe("1");
    await act(async () => { fireEvent.click(within(attention).getByRole("button")); });
    await settle();
    expect(view.getByTestId("host-attention:host:a")).toBeTruthy();
    expect(view.getByText("Approvals are not available for this connection")).toBeTruthy();
    expect(view.queryByText("This view requires a mob runtime control surface.")).toBeNull();
    expect(vi.mocked(window.fetch).mock.calls.some(([url]) => String(url) === SOURCE)).toBe(true);
    view.unmount();
  }, 30_000);

  it("a runtime console shows the configured source's items in Needs you", async () => {
    const view = render(<ConsoleApp baseUrl="" transport={transport("host-attention-runtime")} />);
    await settle();
    const attention = view.getByTestId("approval-attention");
    expect(within(attention).getByRole("button").getAttribute("aria-label")).toContain("1 item from the host");
    view.unmount();
  }, 30_000);

  it("an open aggregate inbox stays when the host items drop to zero", async () => {
    let answered = 0;
    window.fetch = vi.fn(async (url: RequestInfo | URL) => {
      if (String(url) !== SOURCE) return new Response("not found", { status: 404 });
      answered += 1;
      const items = answered === 1 ? [{ id: "a", title: "Confirm a setting", href: "/host/items/a" }] : [];
      return new Response(JSON.stringify({ items }), { status: 200 });
    }) as typeof fetch;
    const view = render(<ConsoleApp baseUrl="" transport={transport("console-aggregator")} />);
    await settle();
    await act(async () => { fireEvent.click(within(view.getByTestId("approval-attention")).getByRole("button")); });
    await settle();
    expect(view.getByTestId("host-attention:host:a")).toBeTruthy();
    await act(async () => { document.dispatchEvent(new Event("visibilitychange")); });
    await settle();
    expect(answered).toBeGreaterThanOrEqual(2);
    expect(view.queryByTestId("host-attention:host:a")).toBeNull();
    expect(view.getByText("Approvals are not available for this connection")).toBeTruthy();
    expect(view.getByText("· not available on this connection")).toBeTruthy();
    expect(view.queryByText("This view requires a mob runtime control surface.")).toBeNull();
    view.unmount();
  }, 30_000);

  it("a read-only runtime console links its pending approval to the source's decide page", async () => {
    const view = render(<ConsoleApp baseUrl="" transport={transport("host-attention-runtime", {
      readOnly: true,
      decideHref: "/host/decide?decision={decision_id}",
      pending: [{ pending_id: "p 1", action_id: "a1", action: "Deploy release" }],
    })} />);
    await settle();
    await act(async () => { fireEvent.click(within(view.getByTestId("approval-attention")).getByRole("button")); });
    await settle();
    const link = view.getByRole("link", { name: "Decide in Host: Deploy release" });
    expect(link.getAttribute("href")).toBe("/host/decide?decision=p%201");
    view.unmount();
  }, 30_000);

  it.each([
    { name: "another signed-in user (new storage scope)", next: { storage_scope: "user-b" } },
    { name: "another runtime under the same user", next: { runtime_id: "runtime-b" } },
  ])("hides the previous scope's host items at once on $name", async ({ next }) => {
    let reads = 0;
    window.fetch = vi.fn(async (url: RequestInfo | URL) => {
      if (String(url) !== SOURCE) return new Response("not found", { status: 404 });
      reads += 1;
      // The new scope's source has not answered yet.
      if (reads > 1) return new Promise<Response>(() => {});
      return new Response(JSON.stringify({ items: [{ id: "a", title: "Previous user's item", href: "/host/items/a" }] }), { status: 200 });
    }) as typeof fetch;
    let loads = 0;
    const base = transport("console-aggregator");
    const switching = {
      ...base,
      loadExperience: async () => {
        loads += 1;
        const first = { ...experience("console-aggregator"), storage_scope: "user-a" };
        return (loads === 1 ? first : { ...first, ...next }) as never;
      },
    } as MetaConsoleTransport;
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
    try {
      const view = render(<ConsoleApp baseUrl="" transport={switching} />);
      await settle();
      await act(async () => { fireEvent.click(within(view.getByTestId("approval-attention")).getByRole("button")); });
      await settle();
      expect(view.getByTestId("host-attention:host:a")).toBeTruthy();
      // The periodic experience reload sees the new scope.
      await act(async () => { vi.advanceTimersByTime(15_000); });
      await settle();
      expect(loads).toBeGreaterThanOrEqual(2);
      expect(view.queryByTestId("host-attention:host:a")).toBeNull();
      view.unmount();
    } finally {
      vi.useRealTimers();
    }
  }, 30_000);
});

describe("host decide links in an agent's chat", () => {
  const identity = "chat-agent";
  const scope = "chat-principal";
  function chatTransport(): MetaConsoleTransport {
    return {
      loadExperience: async () => ({
        runtime_id: "chat-runtime",
        storage_scope: scope,
        console_config: { attention: { sources: [{ id: "host", label: "Host", url: SOURCE, decide_href: "/host/decide/{decision_id}" }] } },
        console_policy: { read_only: true },
        agent_sidebar: { live_snapshot: { agents: [{ identity, member_id: identity, agent_id: identity, label: "Chat agent", kind: "member", state: "running", addressable: true, affordances: { can_send_message: true } }] } },
        activity_feed: { filter_presets: [], active_preset_id: "all" },
      }) as never,
      loadModules: async () => ({ modules: [] }) as never,
      capabilities: async () => ({ version: "test", methods: Object.values(CONSOLE_RPC_METHODS) }) as never,
      queryTimeline: async () => ({ available: true, frames: [{ id: "turn-frame", event: "interaction_complete", identity, interactionId: "turn-1", timestampMs: 1, data: { text: "Deployment needs approval" } }] }) as never,
      subscribeTimeline: () => () => {},
      send: async (input) => ({ interaction_id: "sent", identity: input.identity, cursor: "console:x" }) as never,
      executeCommand: async (input) => ({ command: input.command, accepted: true, result: { pending: [
        { pending_id: "chat 1", action_id: "deploy", action: "Deploy reviewed change", origin: { identity, interaction_id: "turn-1" } },
      ] } }) as never,
    } as MetaConsoleTransport;
  }
  let realFetch: typeof fetch;
  beforeEach(() => {
    window.localStorage?.clear?.();
    const target = { id: `chat:${identity}`, kind: "agent-chat", title: "Chat agent", identity, memberId: identity };
    window.localStorage?.setItem?.(`meta-console-dock-state:${JSON.stringify(["", scope])}`, JSON.stringify({
      tabs: [{ id: "tab", presetId: "single", layout: { kind: "panel", panelId: "panel" } }],
      panels: [{ id: "panel", mode: "console", target }], activeTabId: "tab", focusedPanelId: "panel",
    }));
    realFetch = window.fetch;
    window.fetch = vi.fn(async () => new Response(JSON.stringify({ items: [] }), { status: 200 })) as typeof fetch;
  });
  afterEach(() => {
    window.fetch = realFetch;
    window.localStorage?.clear?.();
  });

  it("a read-only chat links its pending approval to the source's decide page", async () => {
    const view = render(<ConsoleApp baseUrl="" transport={chatTransport()} />);
    await settle();
    await settle();
    expect(view.getByTestId("gating-pending:chat 1")).toBeTruthy();
    const link = view.getByRole("link", { name: "Decide in Host: Deploy reviewed change" });
    expect(link.getAttribute("href")).toBe("/host/decide/chat%201");
    // The card is the chat's own, in the turn that asked, not the inbox's.
    expect(link.closest(`[data-testid^="chat-turn:${identity}:"]`)).not.toBeNull();
    expect(view.queryByTestId("gating-panel")).toBeNull();
    view.unmount();
  }, 30_000);
});
