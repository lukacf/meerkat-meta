import { it } from "node:test";
import assert from "node:assert/strict";
import { CallbackDispatcher } from "../src/agent-builder.js";
import type { ContinuityStore, ProviderCallbackContext } from "../src/types.js";

function store(overrides: Partial<ContinuityStore> = {}): ContinuityStore {
  return {
    async resolveMany() { return {}; },
    async loadSessionSnapshot() { return null; },
    async resolveRecordBySession() { return null; },
    async saveSessionSnapshot() {},
    async upsertContinuityRecord() {},
    async deleteContinuityRecord() {},
    ...overrides,
  };
}

it("session owner uses historical callback with exact session and request context", async () => {
  const dispatcher = new CallbackDispatcher();
  const context: ProviderCallbackContext = {
    signal: new AbortController().signal,
    deadlineMs: Date.now() + 1000,
  };
  for (const owner of ["triage:main", null]) {
    dispatcher.registerContinuityStore(store({
      async sessionOwner(sessionId, callbackContext) {
        assert.equal(sessionId, "old-session");
        assert.equal(callbackContext, context);
        return owner;
      },
      async resolveRecordBySession() {
        throw new Error("historical owner must not depend on current binding");
      },
    }));
    assert.equal(await dispatcher.handleCallback(
      "callback/continuity_store/session_owner", { session_id: "old-session" }, context,
    ), owner);
  }
});

it("session owner current-provider fallback checks the exact session", async () => {
  const dispatcher = new CallbackDispatcher();
  for (const returnedSession of ["current-session", "different-session", null]) {
    dispatcher.registerContinuityStore(store({
      async resolveRecordBySession() {
        return returnedSession === null ? null : {
          record: { identity: "triage:main", agentRuntimeId: "rt-1", sessionId: returnedSession,
            generation: 0, checkpointVersion: 1 },
          fencingToken: 1, checkpointVersion: 1,
        };
      },
    }));
    const call = dispatcher.handleCallback(
      "callback/continuity_store/session_owner", { session_id: "current-session" },
    );
    if (returnedSession === "different-session") {
      await assert.rejects(call, /different session/);
    } else {
      assert.equal(await call, returnedSession === null ? null : "triage:main");
    }
  }
});

it("session owner refuses malformed callback results", async () => {
  const dispatcher = new CallbackDispatcher();
  dispatcher.registerContinuityStore(store({
    async sessionOwner() { return { identity: "triage:main" } as unknown as string; },
  }));
  await assert.rejects(dispatcher.handleCallback(
    "callback/continuity_store/session_owner", { session_id: "old-session" },
  ), /identity string/);
});
