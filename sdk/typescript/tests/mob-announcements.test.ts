/**
 * Mob announcement sources on the TypeScript SDK: wire names, typed results,
 * open vocabularies, typed errors and the capability seam. The transport is
 * mocked; each row checks the exact method name and params the SDK sends and
 * how it reads the gateway's answer.
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";

import * as sdk from "../dist/index.js";
import type {
  AnnouncementConflictError as ConflictError,
  AnnouncementRefusedError as RefusedError,
  MobHandle,
  SourceStaleRevisionError as StaleRevisionError,
  SourceVersionConflictError as VersionConflictError,
} from "../dist/index.js";

const { MetaRuntime, RpcError, parseCapabilitiesResult, parseErrorEvent } = sdk;

// The announcement names are read from the module namespace, so a missing
// export fails only the rows that use it.
const api = sdk as unknown as Record<string, any>;
const supportsAnnouncementSources = (value: unknown): boolean => api.supportsAnnouncementSources(value);
const parseAnnouncementRpcError = (...args: unknown[]): Error | null => api.parseAnnouncementRpcError(...args);
const parseAnnouncement = (raw: unknown): any => api.parseAnnouncement(raw);
const errorClass = (name: string): Function => {
  const cls = api[name];
  assert.equal(typeof cls, "function", `${name} is exported`);
  return cls;
};

const MEMBER = { mob_id: "mob-a", identity: "lead:main" };

const ANNOUNCEMENT = {
  mob_id: "mob-a",
  sequence: 7,
  announced_by: { kind: "host" },
  source_key: "ops/1",
  topics: ["ops.alert"],
  body: "Disk full on db-2",
  correlation_id: "c-1",
  causes: [{ mob_id: "mob-a", source_key: "ops/0" }],
  digest: "sha256:ab",
  announced_at_ms: 1000,
  a_future_field: true,
};

const SUBSCRIPTION = {
  id: "sub-1",
  member: MEMBER,
  selector: { topics: ["ops.alert"], mobs: ["mob-a"] },
  state: "active",
  revision: 2,
  selector_revision: 1,
  frontier: { "mob-a": 6 },
  created_at_ms: 10,
  updated_at_ms: 20,
};

const BATCH = {
  id: "batch-1",
  member: MEMBER,
  items: [
    { subscription_id: "sub-1", subscription_revision: 2, mob_id: "mob-a", sequence: 7, source_key: "ops/1" },
  ],
  gaps: [{ mob_id: "mob-a", count: 2, reason: "expired" }],
  route: "local",
  body: "1 announcement",
  content_digest: "sha256:cd",
  state: { state: "delivered", receipt: { delivery_id: "d-1" } },
  sealed_at_ms: 30,
  updated_at_ms: 40,
  handling: { state: "handled" },
};

const PRINCIPAL = {
  kind: "connector_verified_account",
  issuer: "https://issuer.example",
  strategy_id: "oauth",
  subject: "user-1",
};

const SOURCE = {
  mob_id: "mob-a",
  source_id: "mail",
  generation: 1,
  revision: 3,
  identity: { provider: "mail", principal: PRINCIPAL, scope: { kind: "inbox", id: "INBOX" } },
  connection: { kind: "connect_setup", id: "conn-1" },
  observation: { baseline: "snapshot", deletions: "explicit" },
  topics: ["mail.new"],
  audience: [{ kind: "role", mob_id: "mob-a", role: "lead" }],
  routes: [{ kind: "direct" }, { kind: "a_future_route", lane: 2 }],
  conversation_members: [],
  start: { kind: "baseline" },
  delivery: { priority: "normal", max_per_batch: 20, on_unhandled: "redeliver" },
  health_policy: {
    escalate_after_failures: 3,
    escalate_after_ms: 3600000,
    stale_after_ms: 7200000,
    quiet_after_ms: null,
    empty_after_ms: null,
  },
  runner: { host_runnable: "mail_poll", every_seconds: 300 },
  state: "active",
  collection: "live",
  cursor: { version: 4, token: "opaque-token", advanced_at_ms: 50 },
  streams: { live: 1, onboarding: 0 },
  backfilled: 0,
  health: {
    status: "ok",
    last_success_at_ms: 50,
    last_new_item_at_ms: 40,
    last_observed: { kind: "delta" },
    last_observed_at_ms: 50,
    empty_snapshot_since_ms: null,
    consecutive_failures: 0,
    totals: { announced: 5, replayed: 1, conflicts: 0, own_writes: 0, refusals: 0, gaps: 0 },
    last_failure: null,
    last_conflict: null,
  },
  created_at_ms: 1,
  updated_at_ms: 50,
};

const ADVANCE = {
  source: SOURCE,
  replayed: false,
  items: [
    { item_id: "m1", version: "v1", source_key: "src:mail/m1", outcome: "announced", sequence: 8, captured: 2 },
    { item_id: "m2", version: "v2", source_key: "src:mail/m2", outcome: "a_future_outcome" },
  ],
  streams: [{ stream: "INBOX", phase: "live" }],
};

const AUDIT_ENTRY = {
  id: 1,
  at_ms: 9,
  action: "capture_withheld",
  mob_id: "mob-a",
  member: { mob_id: "mob-a", identity: "research:1" },
  source_id: "mail",
  generation: 1,
  reason: "no_direct_route",
  count: 3,
  first_at_ms: 5,
  last_at_ms: 9,
};

const RESPONSES: Record<string, unknown> = {
  "meta/mob_announcements/announce": { announcement: ANNOUNCEMENT, replayed: false, captured: 2 },
  "meta/mob_announcements/get": { announcement: ANNOUNCEMENT },
  "meta/mob_announcements/subscriptions/put": { subscription: SUBSCRIPTION },
  "meta/mob_announcements/subscriptions/pause": { subscription: SUBSCRIPTION, unresolved_batches: [] },
  "meta/mob_announcements/subscriptions/resume": { subscription: SUBSCRIPTION, unresolved_batches: [] },
  "meta/mob_announcements/subscriptions/cancel": { subscription: SUBSCRIPTION, unresolved_batches: ["batch-1"] },
  "meta/mob_announcements/subscriptions/get": { subscription: SUBSCRIPTION, marker: null },
  "meta/mob_announcements/subscriptions/list": { subscriptions: [SUBSCRIPTION] },
  "meta/mob_announcements/deliveries/list": { deliveries: [BATCH] },
  "meta/mob_announcements/deliveries/observe": { batch: BATCH, native: { kind: "settled" } },
  "meta/mob_announcements/deliveries/reconcile": { batch: BATCH, native: { kind: "settled" } },
  "meta/mob_announcements/sources/put": { source: SOURCE },
  "meta/mob_announcements/sources/pause": { source: SOURCE },
  "meta/mob_announcements/sources/resume": { source: SOURCE },
  "meta/mob_announcements/sources/end": { source: SOURCE },
  "meta/mob_announcements/sources/get": {
    source: SOURCE,
    streams: [{ stream: "INBOX", phase: "live", since_ms: 5, backfilled: 0 }],
  },
  "meta/mob_announcements/sources/list": { sources: [SOURCE] },
  "meta/mob_announcements/sources/advance": ADVANCE,
  "meta/mob_announcements/sources/record_failure": { source: SOURCE },
  "meta/mob_announcements/audit/list": { entries: [AUDIT_ENTRY] },
};

interface RpcCall {
  method: string;
  params: Record<string, unknown> | undefined;
}

function newRuntime(): InstanceType<typeof MetaRuntime> {
  const rt = new MetaRuntime({
    mobConfigPath: null,
    sessionBuilder: null,
    errorCallback: null,
    eventLog: null,
    consoleConfigPath: null,
    consoleRequireAppAuth: null,
    consoleReadOnly: null,
    consoleFetchTimeoutMs: null,
    gatingConfigPath: null,
    routingConfigPath: null,
    workgraphEnabled: null,
    memoryConfig: null,
    agentMemoryConfig: null,
    authConfig: null,
    implicitDelegateIdleRetireSecs: undefined,
    maxSessions: null,
    gatewayTimeoutMs: null,
    gatewayBin: null,
    modules: [],
    persistentState: null,
    rosterProvider: null,
    agentCustomizer: null,
    topologyProvider: null,
  } as any);
  (rt as any)._running = true;
  return rt;
}

/** A handle whose RPC layer records each call and answers from `answer`. */
function mockHandle(
  answer: (method: string) => unknown = (method) => RESPONSES[method] ?? {},
): { handle: MobHandle; calls: RpcCall[] } {
  const rt = newRuntime();
  const calls: RpcCall[] = [];
  (rt as any)._rpc = async (method: string, params?: Record<string, unknown>) => {
    calls.push({ method, params });
    return answer(method);
  };
  return { handle: rt.mobHandle(), calls };
}

/** A handle whose transport answers every request with one JSON-RPC error. */
function failingHandle(error: Record<string, unknown>): MobHandle {
  const rt = newRuntime();
  (rt as any)._transport = {
    sendAsync: async (request: Record<string, unknown>) => ({ jsonrpc: "2.0", id: request.id, error }),
  };
  return rt.mobHandle();
}

const SERVED: [string, (h: MobHandle) => Promise<unknown>, string, Record<string, unknown>][] = [
  ["announce", (h) => h.announce("ops/1", ["ops.alert"], "Disk full", { correlationId: "c-1" }),
    "meta/mob_announcements/announce",
    { source_key: "ops/1", topics: ["ops.alert"], body: "Disk full", correlation_id: "c-1" }],
  ["announcementGet", (h) => h.announcementGet("mob-a", "ops/1"),
    "meta/mob_announcements/get", { mob_id: "mob-a", source_key: "ops/1" }],
  ["announcementSubscriptionPut",
    (h) => h.announcementSubscriptionPut("op-1", "sub-1", MEMBER, ["ops.alert"], { mobs: ["mob-a"] }),
    "meta/mob_announcements/subscriptions/put",
    { operation_id: "op-1", subscription_id: "sub-1", member: MEMBER, topics: ["ops.alert"], mobs: ["mob-a"] }],
  ["announcementSubscriptionPause", (h) => h.announcementSubscriptionPause("op-2", "sub-1", 2),
    "meta/mob_announcements/subscriptions/pause",
    { operation_id: "op-2", subscription_id: "sub-1", expected_revision: 2 }],
  ["announcementSubscriptionResume", (h) => h.announcementSubscriptionResume("op-3", "sub-1", 3),
    "meta/mob_announcements/subscriptions/resume",
    { operation_id: "op-3", subscription_id: "sub-1", expected_revision: 3 }],
  ["announcementSubscriptionCancel", (h) => h.announcementSubscriptionCancel("op-4", "sub-1", 4),
    "meta/mob_announcements/subscriptions/cancel",
    { operation_id: "op-4", subscription_id: "sub-1", expected_revision: 4 }],
  ["announcementSubscriptionGet", (h) => h.announcementSubscriptionGet("sub-1"),
    "meta/mob_announcements/subscriptions/get", { subscription_id: "sub-1" }],
  ["announcementSubscriptionList", (h) => h.announcementSubscriptionList({ member: MEMBER, limit: 5 }),
    "meta/mob_announcements/subscriptions/list", { member: MEMBER, limit: 5 }],
  ["announcementDeliveryList", (h) => h.announcementDeliveryList({ subscriptionId: "sub-1" }),
    "meta/mob_announcements/deliveries/list", { subscription_id: "sub-1" }],
  ["announcementDeliveryObserve", (h) => h.announcementDeliveryObserve("batch-1"),
    "meta/mob_announcements/deliveries/observe", { batch_id: "batch-1" }],
  ["announcementDeliveryReconcile", (h) => h.announcementDeliveryReconcile("batch-1"),
    "meta/mob_announcements/deliveries/reconcile", { batch_id: "batch-1" }],
  ["announcementSourcePut",
    (h) => h.announcementSourcePut("op-5", "mail", SOURCE.identity, SOURCE.observation, ["mail.new"],
      SOURCE.audience, { runner: { host_runnable: "mail_poll", every_seconds: 300 } }),
    "meta/mob_announcements/sources/put",
    { operation_id: "op-5", source_id: "mail", identity: SOURCE.identity, observation: SOURCE.observation,
      topics: ["mail.new"], audience: SOURCE.audience,
      runner: { host_runnable: "mail_poll", every_seconds: 300 } }],
  ["announcementSourcePause", (h) => h.announcementSourcePause("op-6", "mail", 3),
    "meta/mob_announcements/sources/pause", { operation_id: "op-6", source_id: "mail", expected_revision: 3 }],
  ["announcementSourceResume", (h) => h.announcementSourceResume("op-7", "mail", 4, { rebaseline: true }),
    "meta/mob_announcements/sources/resume",
    { operation_id: "op-7", source_id: "mail", expected_revision: 4, rebaseline: true }],
  ["announcementSourceEnd", (h) => h.announcementSourceEnd("op-8", "mail", 5),
    "meta/mob_announcements/sources/end", { operation_id: "op-8", source_id: "mail", expected_revision: 5 }],
  ["announcementSourceGet", (h) => h.announcementSourceGet("mail", { includeStreams: true }),
    "meta/mob_announcements/sources/get", { source_id: "mail", include_streams: true }],
  ["announcementSourceList", (h) => h.announcementSourceList({ state: "active", limit: 10 }),
    "meta/mob_announcements/sources/list", { state: "active", limit: 10 }],
  ["announcementSourceAdvance",
    (h) => h.announcementSourceAdvance("op-9", "mail", 3, 4, PRINCIPAL, "cursor-5",
      { items: [{ item_id: "m1" }], observed: { kind: "delta" } }),
    "meta/mob_announcements/sources/advance",
    { operation_id: "op-9", source_id: "mail", expected_revision: 3, expected_version: 4,
      principal: PRINCIPAL, next_cursor: "cursor-5", items: [{ item_id: "m1" }], observed: { kind: "delta" } }],
  ["announcementSourceRecordFailure",
    (h) => h.announcementSourceRecordFailure("op-10", "mail", 3, 4, PRINCIPAL,
      { class: "rate_limited", retry_after_ms: 60000 }),
    "meta/mob_announcements/sources/record_failure",
    { operation_id: "op-10", source_id: "mail", expected_revision: 3, expected_version: 4,
      principal: PRINCIPAL, failure: { class: "rate_limited", retry_after_ms: 60000 } }],
  ["announcementAuditList", (h) => h.announcementAuditList({ sourceId: "mail", after: 4 }),
    "meta/mob_announcements/audit/list", { source_id: "mail", after: 4 }],
];

describe("MobHandle mob announcement wire", () => {
  for (const [name, call, method, params] of SERVED) {
    it(`${name} sends ${method} with its params`, async () => {
      const { handle, calls } = mockHandle();
      await call(handle);
      assert.deepEqual(calls, [{ method, params }]);
    });
  }
});

describe("MobHandle mob announcement results", () => {
  it("announce reads the announcement and counts captures", async () => {
    const { handle } = mockHandle();
    const result = await handle.announce("ops/1", ["ops.alert"], "Disk full");
    assert.equal(result.replayed, false);
    assert.equal(result.captured, 2);
    assert.equal(result.announcement.sequence, 7);
    assert.equal(result.announcement.sourceKey, "ops/1");
    assert.equal(result.announcement.announcedBy.kind, "host");
    assert.deepEqual(result.announcement.topics, ["ops.alert"]);
    assert.equal(result.announcement.causes[0].sourceKey, "ops/0");
  });

  it("announcementGet answers null for an unknown key", async () => {
    const { handle } = mockHandle(() => ({ announcement: null }));
    assert.equal(await handle.announcementGet("mob-a", "ops/9"), null);
  });

  it("an unknown announcer kind keeps its raw object", () => {
    const parsed = parseAnnouncement({ ...ANNOUNCEMENT, announced_by: { kind: "a_future_announcer", lane: 3 } });
    assert.equal(parsed.announcedBy.kind, "a_future_announcer");
    assert.deepEqual(parsed.announcedBy.raw, { kind: "a_future_announcer", lane: 3 });
    const source = parseAnnouncement({ ...ANNOUNCEMENT, announced_by: { kind: "source", source_id: "mail" } });
    assert.equal(source.announcedBy.sourceId, "mail");
  });

  it("subscription records keep an open state and the frontier", async () => {
    const { handle } = mockHandle(() => ({ subscriptions: [{ ...SUBSCRIPTION, state: "a_future_state" }] }));
    const [subscription] = await handle.announcementSubscriptionList();
    assert.equal(subscription.state, "a_future_state");
    assert.deepEqual(subscription.frontier, { "mob-a": 6 });
    assert.equal(subscription.member.identity, "lead:main");
    assert.deepEqual(subscription.topics, ["ops.alert"]);
    assert.deepEqual(subscription.mobs, ["mob-a"]);
  });

  it("a subscription change reads its unresolved batches", async () => {
    const { handle } = mockHandle();
    const change = await handle.announcementSubscriptionCancel("op-4", "sub-1", 4);
    assert.equal(change.subscription.id, "sub-1");
    assert.deepEqual(change.unresolvedBatches, ["batch-1"]);
  });

  it("subscription get reads the marker, the backlog and an absent subscription", async () => {
    const marker = { cause: { kind: "peer_composing" }, evidence: "observed", since_ms: 5 };
    let { handle } = mockHandle(() => ({
      subscription: SUBSCRIPTION, marker, backlog: { eligible_count: 3, outstanding: true },
    }));
    let view = await handle.announcementSubscriptionGet("sub-1");
    assert.equal(view.subscription?.id, "sub-1");
    assert.equal(view.marker?.causeKind, "peer_composing");
    assert.equal(view.marker?.sinceMs, 5);
    assert.deepEqual(view.backlog, { eligible_count: 3, outstanding: true });
    ({ handle } = mockHandle(() => ({ subscription: null, marker: null })));
    view = await handle.announcementSubscriptionGet("sub-9");
    assert.equal(view.subscription, null);
    assert.equal(view.marker, null);
    assert.equal(view.backlog, null);
  });

  it("deliveries read the batch state, handling and gaps", async () => {
    const { handle } = mockHandle();
    const [batch] = await handle.announcementDeliveryList();
    assert.equal(batch.id, "batch-1");
    assert.equal(batch.state, "delivered");
    assert.deepEqual(batch.stateDetail, { state: "delivered", receipt: { delivery_id: "d-1" } });
    assert.deepEqual(batch.handling, { state: "handled" });
    assert.equal(batch.route, "local");
    assert.equal(batch.items[0].sequence, 7);
    assert.equal(batch.gaps[0].reason, "expired");
  });

  it("delivery observe reads the observation", async () => {
    const { handle } = mockHandle();
    const observation = await handle.announcementDeliveryObserve("batch-1");
    assert.equal(observation.batch.id, "batch-1");
    assert.deepEqual(observation.native, { kind: "settled" });
    assert.equal(observation.marker, null);
  });

  it("a source record reads typed fields and keeps its vocabulary open", async () => {
    const { handle } = mockHandle(() => ({
      sources: [{
        ...SOURCE, state: "a_future_state", collection: "a_future_phase",
        health: { ...SOURCE.health, status: "a_future_status" },
      }],
    }));
    const [source] = await handle.announcementSourceList();
    assert.equal(source.sourceId, "mail");
    assert.equal(source.generation, 1);
    assert.equal(source.revision, 3);
    assert.equal(source.state, "a_future_state");
    assert.equal(source.collection, "a_future_phase");
    assert.equal(source.health.status, "a_future_status");
    assert.equal(source.health.totals.announced, 5);
    assert.equal(source.cursor.version, 4);
    assert.equal(source.cursor.token, "opaque-token");
    assert.equal(source.runner?.hostRunnable, "mail_poll");
    assert.equal(source.runner?.everySeconds, 300);
    assert.equal((source.identity.principal as Record<string, unknown>).kind, "connector_verified_account");
    assert.deepEqual(source.routes.map((route) => route.kind), ["direct", "a_future_route"]);
    assert.deepEqual(source.routes[1].raw, { kind: "a_future_route", lane: 2 });
    assert.deepEqual(source.topics, ["mail.new"]);
    assert.deepEqual(source.streams, { live: 1, onboarding: 0 });
  });

  it("source put reads affected subscriptions and delivery readiness", async () => {
    let { handle } = mockHandle(() => ({
      source: SOURCE, affected_subscriptions: 4, delivery_readiness: { kind: "ready" },
    }));
    let put = await handle.announcementSourcePut("op-5", "mail", SOURCE.identity, SOURCE.observation,
      ["mail.new"], SOURCE.audience);
    assert.equal(put.source.sourceId, "mail");
    assert.equal(put.affectedSubscriptions, 4);
    assert.deepEqual(put.deliveryReadiness, { kind: "ready" });
    ({ handle } = mockHandle());
    put = await handle.announcementSourcePut("op-5", "mail", SOURCE.identity, SOURCE.observation,
      ["mail.new"], SOURCE.audience);
    assert.equal(put.affectedSubscriptions, null);
    assert.equal(put.deliveryReadiness, null);
  });

  it("source get reads the streams when asked", async () => {
    const { handle } = mockHandle();
    const view = await handle.announcementSourceGet("mail", { includeStreams: true });
    assert.equal(view.source.sourceId, "mail");
    assert.equal(view.streams?.[0].phase, "live");
    assert.equal(view.streams?.[0].sinceMs, 5);
  });

  it("source advance reads item outcomes and streams", async () => {
    const { handle } = mockHandle();
    const advance = await handle.announcementSourceAdvance("op-9", "mail", 3, 4, PRINCIPAL, "cursor-5");
    assert.equal(advance.replayed, false);
    const [first, second] = advance.items;
    assert.equal(first.outcome, "announced");
    assert.equal(first.sequence, 8);
    assert.equal(first.captured, 2);
    assert.equal(second.outcome, "a_future_outcome");
    assert.equal(second.sequence, null);
    assert.equal(advance.streams[0].stream, "INBOX");
    assert.equal(advance.streams[0].phase, "live");
  });

  it("audit list reads counted entries", async () => {
    const { handle } = mockHandle(() => ({
      entries: [AUDIT_ENTRY, { ...AUDIT_ENTRY, id: 2, action: "a_future_action" }],
    }));
    const [first, second] = await handle.announcementAuditList();
    assert.equal(first.id, 1);
    assert.equal(first.action, "capture_withheld");
    assert.equal(first.reason, "no_direct_route");
    assert.equal(first.count, 3);
    assert.equal(first.member.identity, "research:1");
    assert.equal(second.action, "a_future_action");
  });
});

describe("mob announcement errors", () => {
  const error = (code: number, kind: string, data: Record<string, unknown> = {}) =>
    parseAnnouncementRpcError(code, "refused", "r-1", "meta/mob_announcements/sources/put", { kind, ...data });

  const cases: [number, string, string][] = [
    [-32044, "mob_announcements_unavailable", "MobAnnouncementsUnavailableError"],
    [-32045, "operation_reused", "AnnouncementConflictError"],
    [-32046, "gap", "AnnouncementFrontierError"],
    [-32047, "topic_owned", "AnnouncementRefusedError"],
    [-32048, "unknown_source", "AnnouncementNotFoundError"],
  ];
  for (const [code, kind, name] of cases) {
    it(`${code} ${kind} maps to ${name}`, () => {
      const cls = errorClass(name);
      const reified = error(code, kind, { topic: "mail.new", source_id: "mail" }) as RpcError | null;
      assert.ok(reified instanceof cls);
      assert.ok(reified instanceof RpcError);
      assert.equal(reified?.constructor, cls);
      assert.equal(reified?.code, code);
      assert.equal((reified as ConflictError).kind, kind);
    });
  }

  it("source conflicts carry the current position", () => {
    const conflictBase = errorClass("AnnouncementConflictError");
    const cursor = { version: 9, token: "t", advanced_at_ms: 1 };
    const stale = error(-32045, "stale_revision", {
      current_revision: 5, current_version: 9, generation: 2, cursor,
    }) as StaleRevisionError;
    assert.equal(stale.constructor, errorClass("SourceStaleRevisionError"));
    assert.ok(stale instanceof conflictBase);
    assert.equal(stale.currentRevision, 5);
    assert.equal(stale.currentVersion, 9);
    assert.equal(stale.generation, 2);
    assert.deepEqual(stale.cursor, cursor);
    const conflict = error(-32045, "source_version_conflict", {
      current_version: 9, cursor, state: "active", revision: 5,
    }) as VersionConflictError;
    assert.equal(conflict.constructor, errorClass("SourceVersionConflictError"));
    assert.ok(conflict instanceof conflictBase);
    assert.equal(conflict.currentVersion, 9);
    assert.equal(conflict.revision, 5);
    assert.equal(conflict.state, "active");
    const plain = error(-32045, "stale_revision", { current_revision: 3 });
    assert.equal(plain?.constructor, conflictBase);
  });

  it("an unknown refusal kind keeps the family base with its kind", () => {
    const reified = error(-32047, "a_future_refusal", { detail: 1 }) as RefusedError;
    assert.equal(reified.constructor, errorClass("AnnouncementRefusedError"));
    assert.equal(reified.kind, "a_future_refusal");
  });

  it("an unrelated error code is not an announcement error", () => {
    assert.equal(error(-32042, "stale_revision"), null);
  });

  it("a gateway conflict reaches the caller as its typed class", async () => {
    const handle = failingHandle({
      code: -32045,
      message: "stale revision",
      data: { kind: "stale_revision", current_revision: 5, current_version: 9, generation: 2, cursor: null },
    });
    await assert.rejects(
      handle.announcementSourceAdvance("op-9", "mail", 3, 4, PRINCIPAL, "cursor-5"),
      (failure: unknown) => {
        assert.equal((failure as Error).constructor, errorClass("SourceStaleRevisionError"));
        assert.equal((failure as StaleRevisionError).currentRevision, 5);
        assert.equal((failure as StaleRevisionError).method, "meta/mob_announcements/sources/advance");
        return true;
      },
    );
  });

  const newInSources: [string, (h: MobHandle) => Promise<unknown>][] = [
    ["sources/get", (h) => h.announcementSourceGet("mail")],
    ["audit/list", (h) => h.announcementAuditList()],
  ];
  for (const [name, call] of newInSources) {
    it(`${name} on an older Meta says sources are unsupported`, async () => {
      const handle = failingHandle({ code: -32601, message: "Method not found" });
      await assert.rejects(call(handle), (failure: unknown) => {
        assert.ok(failure instanceof RpcError);
        assert.equal(failure.code, -32601);
        assert.match(failure.message, /Meta does not support sources/);
        return true;
      });
    });
  }

  it("a subscription method on an older Meta keeps the plain error", async () => {
    const handle = failingHandle({ code: -32601, message: "Method not found" });
    await assert.rejects(handle.announcementSubscriptionGet("sub-1"), (failure: unknown) => {
      assert.ok(failure instanceof RpcError);
      assert.equal(failure.code, -32601);
      assert.doesNotMatch(failure.message, /sources/);
      return true;
    });
  });
});

describe("mob announcement exports", () => {
  it("exports the error codes the gateway answers with", () => {
    const codes = [
      "MOB_ANNOUNCEMENTS_UNAVAILABLE_CODE",
      "ANNOUNCEMENT_CONFLICT_CODE",
      "ANNOUNCEMENT_FRONTIER_CODE",
      "ANNOUNCEMENT_REFUSED_CODE",
      "ANNOUNCEMENT_NOT_FOUND_CODE",
    ].map((name) => api[name]);
    assert.deepEqual(codes, [-32044, -32045, -32046, -32047, -32048]);
  });

  for (const name of [
    "parseAnnouncement",
    "parseAnnouncementSubscription",
    "parseAnnouncementBatch",
    "parseAnnouncementSource",
    "parseSourceAdvanceResult",
    "parseAnnouncementAuditEntry",
    "parseAnnouncementRpcError",
    "supportsAnnouncementSources",
    "MobAnnouncementsUnavailableError",
    "AnnouncementConflictError",
    "SourceVersionConflictError",
    "SourceStaleRevisionError",
    "AnnouncementRefusedError",
    "AnnouncementNotFoundError",
    "AnnouncementFrontierError",
  ]) {
    it(`exports ${name}`, () => {
      assert.equal(typeof api[name], "function");
    });
  }
});

describe("mob announcement capability seam", () => {
  it("detects sources and reads the announcements object", () => {
    const data = {
      contract_version: "0.5.0",
      methods: ["meta/mob_announcements/announce"],
      loaded_modules: [],
      feature_capabilities: ["mob_announcements.sources.v1"],
      mob_announcements: {
        sources: "v1",
        local_delivery: { kind: "refused", cause: "governed_runtime" },
        governed_admission: false,
      },
    };
    const capabilities = parseCapabilitiesResult(data);
    assert.equal(supportsAnnouncementSources(capabilities), true);
    assert.equal(supportsAnnouncementSources(["mob_announcements.sources.v1"]), true);
    assert.equal(capabilities.mobAnnouncements?.sources, "v1");
    assert.equal(capabilities.mobAnnouncements?.governedAdmission, false);
    assert.deepEqual(capabilities.mobAnnouncements?.localDelivery, { kind: "refused", cause: "governed_runtime" });
    assert.equal(capabilities.mobAnnouncements?.localDeliveryKind, "refused");
    assert.equal(supportsAnnouncementSources(parseCapabilitiesResult({ ...data, feature_capabilities: [] })), false);
    assert.equal(parseCapabilitiesResult({ ...data, mob_announcements: null }).mobAnnouncements, null);
    const { mob_announcements: _omitted, ...absent } = data;
    assert.equal(parseCapabilitiesResult(absent).mobAnnouncements, null);
  });

  it("an escalation error event passes through before the SDK names it", () => {
    const event = parseErrorEvent({
      category: "mob_announcement_escalated", escalation_id: "e-1", cause: "source_failing",
    });
    assert.equal(event.category, "mob_announcement_escalated");
    assert.deepEqual(event.context, { escalation_id: "e-1", cause: "source_failing" });
  });
});
