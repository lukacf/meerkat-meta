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

function experience(runtimeId: string) {
  return {
    contract_version: "test",
    runtime_id: runtimeId,
    console_config: { attention: { sources: [{ id: "host", label: "Host", url: SOURCE }] } },
    console_policy: {},
    agent_sidebar: { live_snapshot: { agents: [] } },
    activity_feed: { filter_presets: [], active_preset_id: "all" },
  };
}

function transport(runtimeId: string): MetaConsoleTransport {
  return {
    loadExperience: async () => experience(runtimeId) as never,
    loadModules: async () => ({ modules: [] }) as never,
    capabilities: async () => ({ version: "test", methods: Object.values(CONSOLE_RPC_METHODS) }) as never,
    queryTimeline: async () => ({ frames: [], available: true }) as never,
    subscribeTimeline: () => () => {},
    send: async (input) => ({ interaction_id: "sent", identity: input.identity, cursor: "console:x" }) as never,
    executeCommand: async (input) => ({ command: input.command, accepted: true, result: { pending: [] } }) as never,
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
});
