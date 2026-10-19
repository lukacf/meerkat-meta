import assert from "node:assert/strict";

import {
  normalizeConsoleInteractionRejectedError,
  normalizeActivityFilterPreset,
  normalizeExperienceSectionMeta,
  normalizeGatingActionRequest,
  normalizeGatingActionResult,
  normalizeGatingAuditEntry,
  normalizeIdentityInspectViewState,
  normalizeIdentitySessionRepair,
  normalizeIdentityStatusRow,
  identityStateLabel,
  normalizeReplayUnavailableError,
  normalizeResponsePhase,
  normalizeRoutingSectionView,
  normalizeSidebarWatchFields,
  normalizeToolCallAccumulatorState,
  normalizeUnobservedFacts,
} from "./control-plane";
import { normalizeConsoleSidebarViewState } from "./sidebar";

// Dual-runner: meta's console gates bundle this file with esbuild and run it
// under `node --test`; the meerkat-studio desktop app picks it up with vitest
// (globals enabled). Resolve the registrar for whichever runner is active.
type TestFn = (name: string, fn: () => void | Promise<void>) => void;
const nodeTestModule = "node:test";
const test: TestFn = process.env.VITEST
  ? ((globalThis as Record<string, unknown>).test as TestFn)
  : ((await import(/* @vite-ignore */ nodeTestModule)).default as unknown as TestFn);

test("normalizeExperienceSectionMeta keeps valid poll and stream metadata", () => {
  assert.deepEqual(
    normalizeExperienceSectionMeta({
      schema_version: "1",
      refresh: { mode: "stream", topic: "identity:luka", update_semantics: "append" },
      capabilities: ["inspect", " inspect ", "", null],
    }),
    {
      schema_version: "1",
      refresh: { mode: "stream", topic: "identity:luka", update_semantics: "append" },
      capabilities: ["inspect"],
    },
  );

  assert.equal(
    normalizeExperienceSectionMeta({
      schema_version: "1",
      refresh: { mode: "poll", interval_ms: 0 },
    }),
    null,
  );
});

test("normalizeIdentityStatusRow and response phase trim data without inventing fields", () => {
  assert.deepEqual(
    normalizeIdentityStatusRow({
      identity: " identity:luka ",
      display_name: " Luka ",
      role: " operator ",
      state: " active ",
      addressability: "addressable",
      labels: { team: " console ", empty: "   ", " ": "ignored" },
      generation: 4,
      checkpoint_version: 8,
      lease_healthy: true,
    }),
    {
      identity: "identity:luka",
      display_name: "Luka",
      role: "operator",
      state: "active",
      addressability: "addressable",
      labels: { team: "console" },
      generation: 4,
      checkpoint_version: 8,
      lease_healthy: true,
    },
  );

  assert.equal(normalizeResponsePhase("tool-executing"), "tool-executing");
  assert.equal(normalizeResponsePhase("thinking"), null);
});

test("normalizeIdentityStatusRow carries the machine-owned progress projection", () => {
  const row = normalizeIdentityStatusRow({
    identity: "domain:network",
    state: "active",
    addressability: "addressable",
    labels: {},
    progress: {
      run_state: "run_open",
      in_flight_work: 2,
      last_progress_at_ms: 1752300000000,
      last_progress_event: "execution_advanced",
      health: "degraded",
    },
  });
  assert.deepEqual(row?.progress, {
    run_state: "run_open",
    in_flight_work: 2,
    last_progress_at_ms: 1752300000000,
    last_progress_event: "execution_advanced",
    health: "degraded",
  });

  // Absent on older gateways: no invented field.
  const bare = normalizeIdentityStatusRow({
    identity: "domain:network",
    state: "active",
    addressability: "addressable",
    labels: {},
  });
  assert.equal(bare && "progress" in bare, false);

  // Open vocabulary + partial shapes degrade to defaults, never null out the row.
  const partial = normalizeIdentityStatusRow({
    identity: "domain:network",
    state: "active",
    addressability: "addressable",
    labels: {},
    progress: { health: "quantum" },
  });
  assert.equal(partial?.progress?.health, "quantum");
  assert.equal(partial?.progress?.run_state, "unknown");
  assert.equal(partial?.progress?.in_flight_work, 0);
});

test("normalize sidebar watch fields and filter presets preserves the phase-0 contract", () => {
  assert.deepEqual(
    normalizeSidebarWatchFields({
      watched: true,
      alertLevel: "elevated",
      degraded: true,
      degradedReason: " lease_expired ",
    }),
    {
      watched: true,
      alertLevel: "elevated",
      degraded: true,
      degradedReason: "lease_expired",
    },
  );

  assert.deepEqual(
    normalizeActivityFilterPreset({
      id: " watched-critical ",
      label: " Critical ",
      watchedOnly: true,
      alertLevels: ["critical", "invalid", "critical"],
      eventTypeFilter: ["tool_call", "", "tool_call", "interaction_failed"],
    }),
    {
      id: "watched-critical",
      label: "Critical",
      watchedOnly: true,
      alertLevels: ["critical"],
      eventTypeFilter: ["tool_call", "interaction_failed"],
    },
  );
});

test("sidebar normalization preserves warning tone and watch fields", () => {
  const viewState = normalizeConsoleSidebarViewState({
    blocks: [
      {
        id: "agents",
        kind: "list",
        sections: [
          {
            id: "operators",
            title: "Operators",
            items: [
              {
                id: "identity:luka",
                title: "Luka",
                watched: true,
                alertLevel: "elevated",
                degraded: true,
                degradedReason: "lease_expired",
                meta: [{ id: "status", label: "elevated", tone: "warning" }],
              },
            ],
          },
        ],
      },
    ],
  });

  const item = viewState.blocks[0]?.sections?.[0]?.items?.[0];
  assert.equal(item?.watched, true);
  assert.equal(item?.alertLevel, "elevated");
  assert.equal(item?.degraded, true);
  assert.equal(item?.degradedReason, "lease_expired");
  assert.equal(item?.meta?.[0]?.tone, "warning");
});

test("normalize inspect views and typed console errors", () => {
  assert.deepEqual(
    normalizeIdentityInspectViewState({
      identity: " identity:luka ",
      display_name: " Luka ",
      role: "operator",
      state: "running",
      addressability: "addressable",
      labels: { role: " primary " },
      continuity: {
        generation: 4,
        checkpoint_version: 8,
        session_id: " session-1 ",
        agent_runtime_id: " runtime-1 ",
      },
      lease: {
        fencing_token: 12,
        ttl_remaining_ms: 9000,
        healthy: true,
      },
      topology_peers: [" peer-a ", "", "peer-b"],
      is_final: false,
    }),
    {
      identity: "identity:luka",
      display_name: "Luka",
      role: "operator",
      state: "running",
      addressability: "addressable",
      labels: { role: "primary" },
      continuity: {
        generation: 4,
        checkpoint_version: 8,
        session_id: "session-1",
        agent_runtime_id: "runtime-1",
      },
      lease: {
        fencing_token: 12,
        ttl_remaining_ms: 9000,
        healthy: true,
      },
      topology_peers: ["peer-a", "peer-b"],
      is_final: false,
    },
  );

  assert.deepEqual(
    normalizeReplayUnavailableError({
      error: "replay_unavailable",
      stream: "identity",
      requested_last_event_id: "evt-1",
      latest_event_id: "evt-99",
    }),
    {
      error: "replay_unavailable",
      stream: "identity",
      requested_last_event_id: "evt-1",
      latest_event_id: "evt-99",
    },
  );

  assert.deepEqual(
    normalizeReplayUnavailableError({
      error: "replay_unavailable",
      requested_cursor: "console:1",
      latest_cursor: "console:99",
    }),
    {
      error: "replay_unavailable",
      stream: "timeline",
      requested_last_event_id: "console:1",
      latest_event_id: "console:99",
    },
  );

  assert.deepEqual(
    normalizeReplayUnavailableError({
      type: "replay_unavailable",
      requested_cursor: "lagged:10",
      latest_cursor: "console:99",
    }),
    {
      error: "replay_unavailable",
      stream: "timeline",
      requested_last_event_id: "lagged:10",
      latest_event_id: "console:99",
    },
  );

  assert.equal(
    normalizeReplayUnavailableError({
      error: "replay_unavailable",
      requested_cursor: "bad",
    }),
    null,
  );
  assert.equal(
    normalizeReplayUnavailableError({
      error: "replay_unavailable",
      latest_cursor: "console:99",
    }),
    null,
  );
  assert.equal(normalizeReplayUnavailableError({ error: "replay_unavailable" }), null);

  assert.deepEqual(
    normalizeConsoleInteractionRejectedError({
      code: -32002,
      message: " identity not addressable ",
    }),
    {
      code: -32002,
      message: "identity not addressable",
    },
  );
});

test("gating audit classifies only exact owner evaluation allowances", () => {
  for (const outcome of ["allowed", "allowed_with_audit"]) {
    assert.equal(normalizeGatingAuditEntry({ event_type: "evaluated", outcome }).automaticAllowance, true);
  }
  for (const value of [
    { event_type: "automation_failed", outcome: "allowed" },
    { event_type: "approval_decided", outcome: "allowed" },
    { event_type: " evaluated ", outcome: "allowed" },
    { event_type: "evaluated", outcome: " allowed " },
    { event_type: "evaluated", outcome: "pending_approval" },
    { decision: "auto_approve" },
    null,
    [],
  ]) {
    assert.equal(normalizeGatingAuditEntry(value).automaticAllowance, false);
  }
});

test("gating audit preserves owner tiers and marks other values unknown", () => {
  for (const risk_tier of ["r0", "r1", "r2", "r3"]) {
    assert.equal(normalizeGatingAuditEntry({ risk_tier }).riskTier, risk_tier);
  }
  for (const risk_tier of ["low", "high", "r4", "R2", 2, undefined]) {
    assert.equal(normalizeGatingAuditEntry({ risk_tier }).riskTier, "unknown");
  }
  assert.equal(normalizeGatingAuditEntry({ risk_tier: "r4" }).recordedRiskTier, "r4");
});

test("gating audit distinguishes actor from approver and reads the recorded detail decision", () => {
  const record = normalizeGatingAuditEntry({
    audit_id: "audit-1", pending_id: "pending-1", action_id: "action-1", actor_id: "requesting-agent",
    event_type: "rejection_decided", risk_tier: "r3", outcome: "safe_draft",
    decision: "approve", actor: "wrong-actor",
    detail: { decision: "reject", approver_id: "human" },
  });
  assert.equal(record.auditId, "audit-1");
  assert.equal(record.pendingId, "pending-1");
  assert.equal(record.actorId, "requesting-agent");
  assert.equal(record.approverId, "human");
  assert.equal(record.decision, "reject");
  assert.equal(record.outcome, "safe_draft");
  assert.equal(normalizeGatingAuditEntry({ decision: "approve", detail: null }).decision, undefined);
  assert.equal(normalizeGatingAuditEntry({ outcome: "future_outcome", detail: { decision: "future_decision" } }).decision, "future_decision");
  assert.equal(normalizeGatingAuditEntry(null).riskTier, "unknown");
});

test("normalize gating and routing payloads for the shared control-plane contract", () => {
  assert.deepEqual(
    normalizeGatingActionRequest({
      pending_id: " pending-1 ",
      approver_id: " luka ",
      decision: "escalate",
      reason: " needs higher approval ",
    }),
    {
      pending_id: "pending-1",
      approver_id: "luka",
      decision: "escalate",
      reason: "needs higher approval",
    },
  );

  assert.deepEqual(
    normalizeGatingActionResult({
      pending_id: "pending-1",
      action_id: "action-1",
      approver_id: "luka",
      decision: "escalate",
      outcome: "pending_approval",
      decided_at_ms: 1717171717,
      next_pending_id: " pending-2 ",
    }),
    {
      pending_id: "pending-1",
      action_id: "action-1",
      approver_id: "luka",
      decision: "escalate",
      outcome: "pending_approval",
      decided_at_ms: 1717171717,
      next_pending_id: "pending-2",
    },
  );

  assert.deepEqual(
    normalizeRoutingSectionView({
      routes: [
        {
          route_key: " route:1 ",
          recipient: " ops ",
          sink: "slack",
          target_module: "routing",
          channel: " approvals ",
        },
      ],
      deliveries: [
        {
          delivery_id: " delivery-1 ",
          route_id: " route-1 ",
          recipient: "ops",
          sink: "slack",
          target_module: "routing",
          status: "delivered",
          first_attempt_ms: 1,
          final_attempt_ms: 2,
          attempts: [
            { attempt: 1, status: "sent", backoff_ms: 0 },
          ],
        },
      ],
    }),
    {
      routes: [
        {
          route_key: "route:1",
          recipient: "ops",
          sink: "slack",
          target_module: "routing",
          channel: "approvals",
        },
      ],
      deliveries: [
        {
          delivery_id: "delivery-1",
          route_id: "route-1",
          recipient: "ops",
          sink: "slack",
          target_module: "routing",
          status: "delivered",
          first_attempt_ms: 1,
          final_attempt_ms: 2,
          attempts: [
            { attempt: 1, status: "sent", backoff_ms: 0 },
          ],
        },
      ],
    },
  );
});

test("normalize tool-call accumulator state for pending and out-of-order results", () => {
  assert.deepEqual(
    normalizeToolCallAccumulatorState({
      timeoutMs: 60000,
      toolCalls: {
        " tool-1 ": {
          type: "tool-call",
          name: "search",
          arguments: " {\"q\":\"test\"} ",
          status: "pending",
        },
      },
      pendingResults: {
        " tool-2 ": " result before call ",
      },
    }),
    {
      timeoutMs: 60000,
      toolCalls: {
        "tool-1": {
          type: "tool-call",
          toolCallId: "tool-1",
          name: "search",
          arguments: "{\"q\":\"test\"}",
          status: "pending",
        },
      },
      pendingResults: {
        "tool-2": "result before call",
      },
    },
  );
});

test("session_repair hold rides the status row and the inspect view and wins the state label", () => {
  const hold = {
    session_id: " 01a000bb-b69e-7570-933d-ffd5d61d51ee ",
    hold: "audited_endpoint_divergence",
    diagnose_command:
      "rkat --state-root /srv/state --realm meta session repair-wholeblob 01a000bb-b69e-7570-933d-ffd5d61d51ee --json",
    apply_command:
      "rkat --state-root /srv/state --realm meta session repair-wholeblob 01a000bb-b69e-7570-933d-ffd5d61d51ee --apply --json",
    detail: "session needs the sanctioned audited-endpoint repair",
  };
  const row = normalizeIdentityStatusRow({
    identity: "domain:security",
    state: "broken",
    addressability: "addressable",
    labels: {},
    session_repair: hold,
  });
  assert.equal(row?.session_repair?.session_id, "01a000bb-b69e-7570-933d-ffd5d61d51ee");
  assert.equal(row?.session_repair?.hold, "audited_endpoint_divergence");
  assert.equal(identityStateLabel(row!), "needs repair");

  const inspect = normalizeIdentityInspectViewState({
    identity: "domain:security",
    state: "broken",
    addressability: "addressable",
    labels: {},
    continuity: {},
    session_repair: hold,
  });
  assert.equal(inspect?.session_repair?.apply_command, hold.apply_command);
  assert.equal(inspect?.session_repair?.diagnose_command, hold.diagnose_command);
  assert.equal(identityStateLabel(inspect!), "needs repair");

  // A Broken member without the hold keeps its raw state; a hold missing one
  // of the two commands is not a hold.
  assert.equal(
    identityStateLabel(normalizeIdentityStatusRow({ identity: "x", state: "broken", addressability: "addressable", labels: {} })!),
    "broken",
  );
  assert.equal(normalizeIdentitySessionRepair({ ...hold, apply_command: "" }), null);
  assert.equal(normalizeIdentitySessionRepair(null), null);
});

test("normalizeIdentityStatusRow carries the unobserved marker of a pending row", () => {
  const row = normalizeIdentityStatusRow({
    identity: "voice-keeper",
    state: "unknown",
    addressability: "internal_only",
    labels: { agent_identity: "voice-keeper" },
    session_id: "successor-session",
    unobserved: {
      reason: "refresh_pending",
      fields: ["state", " role ", "", 7, "labels"],
    },
  });
  assert.deepEqual(row?.unobserved, {
    reason: "refresh_pending",
    fields: ["state", "role", "labels"],
  });
  assert.equal(row?.state, "unknown");

  assert.equal(normalizeUnobservedFacts(null), null);
  assert.equal(normalizeUnobservedFacts({ fields: ["state"] }), null, "a reason is required");
  assert.deepEqual(
    normalizeUnobservedFacts({ reason: "not_observed" }),
    { reason: "not_observed", fields: [] },
    "an unrecognized reason is kept, not dropped",
  );
  assert.equal(
    normalizeIdentityStatusRow({
      identity: "voice-keeper",
      state: "active",
      addressability: "addressable",
      labels: {},
    })?.unobserved,
    undefined,
    "an observed row carries no marker",
  );
});
