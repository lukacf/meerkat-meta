# Changelog — @rkat/meta-sdk (TypeScript SDK)

## Unreleased

- Breaking (access control): `MetaBuilder.accessControl(path)` now needs an
  existing, valid access file. The gateway refuses to start with a missing
  path, a directory, or an unreadable or invalid file instead of starting
  with access control disabled, and an explicit empty path throws. A
  configuration that names no admin grants administration to nobody; name
  the first admin in the file. Omitting the call leaves access control off
  only when nothing is at the conventional `config/access.toml` (a missing
  or dangling parent directory counts as nothing there). Discovery now
  forwards any other entry there (even a directory or a dangling
  `access.toml` link) and any lookup error other than ENOENT, and the
  gateway's strict loader refuses initialization when it cannot open, read,
  parse or validate the file. The loader does not check the file type, so a
  FIFO there blocks startup instead of refusing it.
- Breaking (gating): `MetaBuilder.gating(path)` throws for an explicit empty
  path, and the runtime forwards any explicit gating path instead of
  dropping an empty one, so the gateway refuses it. Omitting the call leaves
  gating off only when nothing is at the conventional `config/gating.toml`
  (a missing or dangling parent directory counts as nothing there).
  Discovery now forwards any other entry there (even a directory or a
  dangling `gating.toml` link) and any lookup error other than ENOENT, and
  the gateway refuses initialization when it cannot open or read the file or
  rejects its contents. The loader does not check the file type, so a FIFO
  there blocks startup instead of refusing it.

## 0.8.19

- No TypeScript equivalent of the Python `role_migrations` builder surface
  exists, so a TypeScript host cannot authorize a role-migrating resume. A
  durable member whose role changed refuses to resume until some host declares
  the migration; use the Python builder, or send `role_migrations` at the top
  level of a `meta/init` payload.

## 0.8.16

- Breaking (mobkit 0.8.16, tracking meerkat 0.8.22's exact helper contract):
  `spawnHelper` and `forkHelper` now take required `resultLabel: string` and
  `maxTextBytes: number` parameters (wire fields `result_label` /
  `max_text_bytes`); the byte bound is validated upstream before admission.
  The response regains `session_id`, populating the existing
  `HelperResult.sessionId` that older releases left `null`.

- Breaking: removed `MetaBuilder.scheduling(...)`, static schedule-file
  discovery, and `MobHandle.schedulingEvaluate` / `schedulingDispatch`.
  Create durable schedules through Meerkat schedule tools or
  `ScheduleService`.

## 0.8.9

### Gateway stderr passthrough

- Gateway stderr (tracing lines, panic hooks, storage-migration progress) is
  piped to the host process's stderr by default; previously it was ignored
  entirely. Opt out with `META_GATEWAY_STDERR=devnull`, or append to a file
  with `META_GATEWAY_STDERR_FILE=<path>` — same shape as the Python SDK.

## 0.8.6

### Durable detached jobs

- Added durable detached callback registration with runner identity, restart
  and idempotency policy, submission timeout, and credential scopes.
- Added `runtime.jobs` and `runtime.monitors` domain handles plus the fenced
  asynchronous start/reconcile/cancel and report protocol. Reconciliation
  reuses the exact committed attempt, fence, lease, checkpoint, and handle
  without minting authority on reopen.
- Detached credentials are resolved from the runner's bound SDK profile for
  each committed attempt and never enter durable job state or reports.

## 0.8.0

### Lease renewal authority

- Documented the `LeaseProvider.renewLeases()` commit boundary: rejected
  promises are pre-commit and must leave every input grant unchanged; returned
  renewed or lost results are committed per identity.

### Graceful gateway shutdown

- `MetaRuntime.shutdown()` now closes the persistent gateway's stdin and
  awaits its runtime cleanup. Handshake-capable gateways advertise a bounded
  shutdown horizon (335 seconds for the stock gateway), keeping provider
  callbacks routable through their public 120-second completion contract and
  the gateway's 130-second hard wire deadline before the bounded `SIGTERM` /
  `SIGKILL` fallback. Cleanup acknowledgement is strictly boolean and fails
  closed when missing or malformed; older/custom gateways retain the EOF
  protocol. Authority-mutating providers receive an optional cancellation and
  deadline context; the host aborts at 125 seconds and suppresses late callback
  responses so a timed-out renewal cannot publish unnoticed authority.

## 0.7.17

### Identity-first agent memory

- Added `Meta.builder().agentMemory(...)` for gateway-backed
  identity-first runtimes with persistent state.
- Added `MobHandle.rememberAgentMemory(...)`,
  `MobHandle.recallAgentMemory(...)`, and
  `MobHandle.forgetAgentMemory(...)` for durable per-identity memory
  records backed by `meta/agent_memory/*`.
- Added strict exported types/parsers for `AgentMemoryRecord`,
  `AgentMemoryRecallResult`, and `AgentMemoryForgetResult`.

## 0.6.29

### Console fetch policy

- Added `Meta.builder().consoleFetchTimeoutMs(ms)` for gateway-backed
  runtimes. The value is sent as `runtime_options.console_fetch_timeout_ms`
  and projected through `/console/experience` for the bundled console.

## 0.6.28

### Runtime state builders

- `.persistentState(path)` no longer creates a local redb mob store.
  Gateway-backed SDK runtimes keep mob storage in-memory while sessions,
  runtime state, metadata, console logs, and binary blobs remain under
  the configured state directory.

## 0.6.6

### Multimodal attachments

- Added `MobHandle.uploadBlob(...)` / `upload_blob(...)` for efficient
  binary image upload through the console multipart endpoint.
- `MobHandle.send(...)` accepts image attachments and sends multipart
  placeholders instead of inlining large base64 payloads in JSON.

### Streaming structural events + durable cursors

Parity with the Python SDK: structural events are now durable
end-to-end. Cursors come from the meerkat ledger and survive meta
restarts on SQLite-backed deployments.

- `MobStructuralEvent.cursor` is the meerkat ledger cursor (not a
  per-process counter). The runtime auto-installs a SQLite metadata
  store next to `.persistent_state(path)` builds.
- New `MobEventsStaleError extends RpcError` raised by
  `queryMobEvents()` / `subscribeMobEvents()` on JSON-RPC code
  `-32010`. Carries `afterCursor` and `latestCursor`. The shared
  `MOB_EVENTS_STALE_CURSOR_CODE` constant is exported.
- `meta/mob_events/subscribe` returns a `subscribe_url` pointing
  to the new `/meta/mob_events/stream?after_seq=...&...` SSE
  route; original filters echo back so the SSE handler picks up
  gaplessly with the same predicate.
- `meta/mob_events/query` always returns a numeric
  `next_after_seq` even on empty match (was `null`).

### `MobHandle.listRuns(flowId?)`

New SDK method wrapping `meta/list_runs`. Returns `MobRun[]` with
the full meerkat ledger projection: `stepLedger`, `failureLedger`,
`frames` (map keyed by frame id), `loops` (map keyed by loop id),
`loopIterationLedger`, `flowState`, `activationParams`,
`schemaVersion`, `rootStepOutputs`, `loopIterationOutputs`.
Meerkat-internal sub-shapes pass through opaquely as `unknown`.

New interfaces and parsers exported from `@rkat/meta-sdk`:
`MobRun`, `MobRunStatus`, `StepRecord`, `FailureRecord`,
`FrameRecord`, `LoopRecord`, `LoopIterationRecord` plus
`parseMobRun` / `parseStepRecord` / etc.

### Bug-hunt fixes (PR #69)

- `RpcError.data` propagates the JSON-RPC `error.data` payload.
- New `isRpcError(err)` / `isMobEventsStaleError(err)` structural
  type guards. **Use these instead of `instanceof RpcError`** —
  dual CJS+ESM packaging, vitest module isolation, and
  hoisted-vs-nested monorepos all produce two `RpcError`
  constructors that fail `instanceof` for each other's instances.
  The structural checks survive the split.
- `MetaRuntime.start()` no longer rewrites `meta/init`
  `RpcError` as a generic `TransportError`. Original code/message/
  data flow through; only synthesizes `TransportError` when the
  subprocess actually died.
- `SseBridge` ties the underlying HTTP request to the generator
  lifetime. Breaking out of `for await (const e of
  handle.subscribeAgent(...))` now destroys the request — pre-fix
  it leaked an `http.ClientRequest` per iteration.
- `createJsonRpcHttpTransport` accepts a `timeoutMs` option
  (default 60s) and aborts via `AbortController`. Pre-fix a server
  that accepted but never replied hung the caller's `await`
  forever.
- SSE parser handles `id:N` / `event:foo` lines without the
  optional space (SSE-spec-legal). Pre-fix only the
  `"id: "` / `"event: "` prefixes matched and resume cursors were
  lost.
- `parseSubscribeResult` filters non-object entries from `events[]`
  before mapping. Pre-fix null/string/number entries were coerced
  into silently-empty envelopes — data loss.
- `parseDispatchInput` validates `origin` against the closed
  `DispatchOrigin` union and falls back to `"system"` for unknown
  values. Pre-fix it was an unchecked cast.

### Cross-mob signed-peer surface

- New `meta/peer_pubkey` RPC returns the local gateway's Ed25519
  signing pubkey as a base64 string. Use this to bootstrap trust before
  populating a peer meta's contact directory with this gateway's
  pubkey. (Rust + Python wrappers ship in parallel; TS wrapper to
  follow once the cross-process transport lands.)
- `meta/cross_mob/wire_local` and `meta/cross_mob/unwire_local` now
  accept an optional `remote_pubkey_b64` (base64 of the peer gateway's
  Ed25519 verifying key). Non-inproc transports (`tcp://`, `uds://`)
  require it — the gateway rejects unsigned descriptors on real
  transports so meerkat-comms can verify envelope signatures at ingress.
- `meta/cross_mob/directory` entries gain an optional `pubkey` field
  (base64) when the contact-directory TOML uses the new table form
  (`{ transport = "...", pubkey = "ed25519:..." }`). Bare-string entries
  remain backward compatible.

## 0.6.0 — Meerkat 0.6 wire rename (BREAKING)

### Structural mob events surface

Added `MobHandle.queryMobEvents()` and `MobHandle.subscribeMobEvents()`,
backed by the new `meta/mob_events/query` and
`meta/mob_events/subscribe` JSON-RPC methods. The structural surface
preserves the full `MobEventKind` vocabulary from meerkat-mob (25
variants — `flow_started`, `step_dispatched`, `members_wired`,
`supervisor_escalation`, ...) along with the `mobId`, `runId`,
`stepId`, and `agentIdentity` fields that the legacy lossy
`UnifiedEvent::Agent` projection discards.

- New `MobStructuralEvent` interface with `parseMobStructuralEvent`
  carries `eventId`, `cursor`, `mobId`, `timestampMs`, `kind`, `runId`,
  `stepId`, `agentIdentity`, `data`.
- `EventQuery` extended with `mobId`, `runId`, `stepId`, `identity`
  filters. `afterSeq` is the cursor — pass the highest-seen `cursor`
  to paginate strictly-newer events.



Aligns the TypeScript SDK's wire-level field names with the meerkat 0.6
platform. Consumers upgrading from 0.5.x need to rename a handful of field
references — see the table below. No behavioural changes.

### Renamed fields

| Before | After | Surfaces |
|--------|-------|----------|
| `meerkatId` / `meerkat_id` | `agentIdentity` / `agent_identity` | `SpawnResult`, `MemberSnapshot`, `DiscoverySpec`, `meta/*` RPC params that accept a member identity |
| `profile` (as a mob-member role) | `role` | `SpawnResult`, `MemberSnapshot`, `DiscoverySpec`, `meta/ensure_member`, `meta/attach_existing_session`, `meta/spawn_helper` / `fork_helper` options, admin-console live snapshots |

Rename in your code:

```diff
- const spec: DiscoverySpec = { profile: "worker", meerkatId: "agent-1", labels: {...} };
+ const spec: DiscoverySpec = { role: "worker", agentIdentity: "agent-1", labels: {...} };

- await handle.ensureMember("agent-1", "worker");            // positional `profile` arg
+ await handle.ensureMember("agent-1", "worker");            // positional `role` arg (same call, renamed semantically)

- snap.meerkatId       // MemberSnapshot / SpawnResult
+ snap.agentIdentity

- snap.profile
+ snap.role
```

### Other wire shifts that ride the same rename

- `MemberSnapshot.state` is now `"Active"` / `"Retiring"` (title-cased from
  meerkat's native `MemberState` enum) where 0.5 emitted `"active"` /
  `"retiring"`. Update any string-comparison code.

- `meta/spawn_helper` and `meta/fork_helper` no longer emit
  `session_id` on the response. Meerkat 0.6 retires the helper before
  the call returns, so meta cannot recover the bridge session id
  post-hoc. If your code correlated helper history via this id, you need
  an alternative route (subscribe to the helper's events during the
  call, or wait for an upstream `HelperResult.bridge_session_id` field).

### Reconcile error semantics + report shape

- Native Rust roster reconcile reports may now include a `failures` array
  when meerkat's native `MobHandle::reconcile` records per-identity failures. On the
  Rust side, `UnifiedRuntime::reconcile()` re-lifts a non-empty `failures`
  list into `UnifiedRuntimeReconcileError::PartialFailure` so callers
  using `?` see pre-0.6 propagation behaviour.

- The `spawned` field in that native roster report's wire projection is now
  an array of
  `{ agent_identity: string, member_ref: string }` objects (the canonical
  `MobSpawnReceiptWire` shape from meerkat-contracts) rather than a list
  of identity strings. `member_ref` is a server-resolved opaque handle
  for subsequent member-targeted control calls.

These are native Rust roster changes, not TypeScript SDK module-reconcile
results. The SDK's `MobHandle.reconcile(modules)` is a different operation:
it calls `meta/reconcile` and returns `ReconcileResult` with
`accepted`, `reconciledModules`, and `added`, not `failures` or `spawned`.

### Lightweight roster

- `meta/list_members`, `meta/get_member`, `meta/find_members` no
  longer inject a `session_id` field per entry. Aligns with meerkat's
  lightweight-roster design. Use `meta/member_status` to bridge a
  member to a realtime session — its `MobMemberSnapshot` carries
  `current_session_id` natively.

### Removed RPCs

- `meta/member_current_session_id` and `meta/member_session_ref`
  are gone. Both were one-liners over an internal session-id lookup
  and are subsumed by `meta/member_status`.

### New: server-side readiness

- `MobHandle.waitReady(timeoutSeconds?)` — blocks until all current mob
  members are startup-ready for orchestration, then returns
  `{ ready: [...], timeout: false }`. On deadline expiry returns
  `{ ready: [], timeout: true }` instead of throwing. Relays meerkat
  0.6's `MobHandle::wait_for_ready`. Replaces ad-hoc client-side
  polling loops on `member_status`.

### New: flow enumeration + start

- `MobHandle.listFlows()` — returns the flow IDs declared by the mob's
  `[flows.*]` tables. Relays meerkat 0.6's `MobHandle::list_flows` via
  the new `meta/list_flows` RPC.
- `MobHandle.runFlow(flowId, params?)` — starts a flow run and returns
  its `run_id` string. Relays `MobHandle::run_flow` via
  `meta/run_flow`.

### Unchanged

- The `DurableAgentSpec` identity-first surface keeps its `profile` field (it
  mirrors meerkat's internal Rust `DurableAgentSpec`, which is a different
  surface from `MobMemberListEntry`).
- RPC method names (e.g. `meta/ensure_member`, `meta/spawn_helper`) are
  unchanged. Only their parameter and response field names shifted.
