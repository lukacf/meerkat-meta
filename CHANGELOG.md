# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Breaking (Rust source)

- Custom specs that install agent mob tools must explicitly provide
  `MobRuntimeDelivery` with `with_runtime_delivery` before `with_agent_mob_tools`.
  Use the session service's exact runtime-store facade and the agent builder's
  detached-job store. Stock constructors supply this composition themselves.
  `StorageResolutionError`, `MobRuntimeError` and `UnifiedRuntimeBuilderError`
  gain typed delivery errors. `MobRuntimeError::DeliveryStartup` retains the
  stop report if owner arming fails after a mob handle was acquired.
- Gateway job health now exposes `detached_jobs.runtime_delivery_pass` from
  the native delivery owner. Row-level `blocked_deliveries` is `null` with
  `blocked_deliveries_reading: "unavailable"`; the native owner exposes blocked
  sessions, runtimes and failures, but does not expose row identities. The
  separate `runtime_inbox_backlog` remains an exact store census.

- Meerkat's fallible runtime acquisition (Meerkat 0.8.52) reaches MobKit's
  composition. Building a runtime machine can now refuse, and so can the
  agent mob tools' runtime adapter:
  - `MobBootstrapSpec::ephemeral` and `ephemeral_with_hook` return
    `Result<Self, StorageResolutionError>`. `with_agent_mob_tools` returns
    `Result<Self, MobRuntimeDeliveryError>`, preserving native acquisition
    and continuation-binding errors. `with_session_runtime_adapter` returns
    `Result<Self, RuntimeDriverError>`.
  - `StorageResolutionError`, `MobRuntimeError` and
    `UnifiedRuntimeBuilderError` each gain `Runtime(RuntimeDriverError)`.
    Exhaustive matches must add the arm.
  - Session-service wrappers replace `runtime_adapter()` with
    `acquire_runtime_adapter(explicit)` and preserve the inner service's
    `Some`, `None` or error result. MobKit's wrapper supplies the explicit
    owner when provided, otherwise its configured override; supplying an
    owner does not turn the inner service's `None` into an acquired
    adapter.
- `GatingDecideError` gains `IdsUnavailable(GatingIdUnavailable)`, and
  `GatingStateSnapshot` gains `owner_epoch: Option<String>` (see Fixed).
  Exhaustive matches and struct literals must handle the new variant and
  field.
- `StewardStore` gains the required method `review_quarantined` (see Added).
  Implementors must add it; a store without a quarantine queue can return
  `QuarantineReviewError::Store(AgentMemoryError::Unsupported(..))`.
- `StewardStore::resolve_pending_promotion` and `rekey_pending_promotion`
  return `Result<bool, AgentMemoryError>`: whether the promotion was still
  pending and changed. Implementors must report it and must leave a
  resolved promotion (including one an operator's tombstone invalidated)
  as it is.
- `MemoryTimelineEvent::QuarantineVerdict` gains `successor_id:
  Option<String>` (see Added). Code constructing it must set it; the console
  payload omits it when absent.
- `MemoryRecord` gains `ever_quarantined: bool` (serde default `false`), the
  durable taint marker the store already kept. Code building the struct with
  a literal must set it.
- `AccessConfigError` gains `RevisionExhausted` (see Added): the access owner
  refuses a mutation that would overflow its `u64` revision before persisting
  or publishing it. The enum is public and not `#[non_exhaustive]`, so
  exhaustive matches must add the arm.
- `MobCompositionProvenanceError` gains `CandidateDivergent { fields }`
  (see Changed). Exhaustive matches must handle it. `MobBootstrapSpec` gains
  the pub field `candidate_definition: CandidateDefinition`; code that builds
  the spec with a struct literal must set it (`CandidateDefinition::default()`
  keeps the refusing default).
- `MobRuntimeError` gains `MobStopFlowRunsUnsettled(Box<MobStopFlowRunsUnsettled>)`
  (see Changed). Exhaustive matches must handle it.
- `UnifiedRuntimeShutdownReport` gains `mob_terminal_shutdown:
  MobTerminalShutdownOutcome` (see Changed). Code constructing the report
  must set it; `MobTerminalShutdownOutcome` is `#[non_exhaustive]`.
- `ContinuityStore::as_incremental_sessions` no longer has a default. Every
  continuity store now states whether it serves MobKit's session-delta
  channel: a decorator forwards its inner store's channel, and a
  whole-snapshot store returns `None` and says why.
  - The default `None` let wrappers around incremental-capable stores, and
    native stores that never considered the channel, silently fall back to
    writing the whole session document at every turn boundary. One
    production coordinator session was rewritten in full, at 286 MB, 44
    times a day.
  - Implementors without the method must add it.
- `UnifiedRuntimeBuilderError` gains
  `SessionStoreNotIncremental { store_kind }` (see Added). Exhaustive matches
  must handle it.
- `identity_first::ResumeRejectionKind` and
  `identity_first::ContinuityFailureKind` gain `RoleMigrationNotApplied`
  (see Fixed). Exhaustive matches must handle it. On the wire,
  `identity_bootstrap` restore progress can carry the new kind
  `role_migration_not_applied`.

- `StorageSlotSummary.declaration: DurabilityDeclaration` is replaced by
  `durability: StorageSlotDurability`. Match `Declared(declaration)` or
  `Unverified { domain, class }`; use the enum's accessors to inspect the
  common domain/class or an optional declaration. This MobKit census type
  leaves Meerkat's `DurabilityResolution` unchanged.

- `ProcessBoundaryError` adds `CleanupUnconfirmed { pid, source }` for an OS
  wait failure that cannot confirm direct-child reaping. Exhaustive matches
  must handle this explicit uncertainty.

- `CrossMobError` gains `PlacedMemberUnavailable { member_id, mob_id, reason }`
  for a placed member that has no usable endpoint (Broken, or none
  registered), and `PlacedRouteInstallPending { member_id, mob_id, peer_id,
  host }` for a committed placed edge whose host has not yet acknowledged
  the trust row (converging, not rolled back); exhaustive matches must
  handle them.
- `ConsoleSendRequest` gains `skill_refs: Vec<meerkat_core::skills::SkillRef>`
  and `BridgeDelivery` gains `skill_references: Vec<SkillKey>` (see Added).
  Struct literals must set them, usually to `Vec::new()`; the JSON field is
  optional and omitted when empty.
- `MobRuntimeError` gains `HostHumanInput(HostHumanInputError)`: a typed
  refusal on the explicit host-human lane (a skill selection sent to an
  externally bound member on the member-only console lane). Exhaustive
  matches must add the arm.

- `ContinuityRepairPolicy` loses `initial_backoff` and `max_backoff` and is
  `#[non_exhaustive]`: the continuity repair supervisor no longer runs on a
  timer (see Changed). Construct it with `ContinuityRepairPolicy::default()`.
- `IdentityBootstrapEntry` gains `restore: Option<IdentityRestoreProgress>`
  (see Added). Struct literals must set it, usually to `None`. The JSON field
  is optional, so older snapshots still parse.

- `BridgeError` and `BridgeAdmissionError` gain an `UnsupportedForMode {
  identity, mode, detail }` variant: meerkat's typed pre-admission refusal
  (`MobError::UnsupportedForMode`) for the member's live runtime mode, which
  was flattened into `Mob(String)` before. Both enums are public and not
  `#[non_exhaustive]`, so exhaustive matches (custom `SessionBridge`
  implementations, callers mapping bridge errors) must add the arm, as with
  `ProviderAuthRejected`. The `Display` text is unchanged from the former
  `Mob` rendering.

- `ConsoleAgentLiveSnapshot.response_phase` is now `Option<Option<String>>`:
  `None` means no activity observation, `Some(None)` means known quiet, and
  `Some(Some(phase))` carries the recorded activity phase. JSON keeps the
  existing string phase and now preserves explicit null separately from a
  missing field. Update Rust constructors that set an active phase accordingly.

- `ConsoleFrameSource` adds `member_provenance`; `GatingPendingEntry` adds
  `rationale` and `origin`. Update Rust struct literals for these fields.
- `MobKitConsoleAggregator::query_timeline_windowed` returns
  `ConsoleTimelineQueryResult<ConsoleTimelineWindowPage>`. Custom
  `ConsoleLogStore` implementations must preserve member provenance when
  storing and reading frames so retired-member visibility remains enforceable.
- Hosts can initialize the shared console projection with a fixed ingestion
  redaction policy before creating request views. Later view policies cannot
  replace that host policy or change another view's authorized data.
- `identity_first::bridge::MemberInspection` gains `preview_unavailable:
  Option<meerkat_mob::MemberPreviewUnavailable>` (see Added). The struct is
  public and not `#[non_exhaustive]`, so struct literals (custom
  `SessionBridge` implementations and test doubles) must set it, usually to
  `None`.

- `IdentityRuntimeError` gains `PeerWiringTimedOut { operation, waited }` and
  `PeerTopologySuperseded { attempts }` (see Fixed): a managed-topology wiring
  call that exceeds its budget, and a topology plan that stayed stale across
  its bounded replans. The enum is public and not `#[non_exhaustive]`, so
  exhaustive matches must add both arms. `PeerWiringTimedOut.operation` can
  also be `"wake_topology_locks"` (the wake path's bounded lock acquisition).

### Storage and wire compatibility

- Memory panel reads (`mobkit/memory/panel/*`) treat a tombstoned record
  that was ever quarantined as quarantine evidence: a caller without
  `memory.quarantine.review` no longer receives it, as it never received a
  quarantined one. `memory.quarantine.verdict` frames may carry
  `successor_id`, and the steward's open-loop nudges
  (`verdict: "open_loop_escalated"`) no longer carry the dream's rationale
  (`rationale` is null).
- `mobkit/access/*` failures are typed and no longer echo server detail (see
  Added). A failed save of the access file answers `-32000` with
  `data.kind = "access_persistence_failed"` instead of `-32602`; an exhausted
  revision answers `-32004` with `access_mutation_unavailable`. Invalid
  configurations keep `-32602` and `invalid_access_config` with the fixed
  message "Invalid access configuration.", and unparseable payloads answer
  "Invalid access config.", "Invalid access rule." or "Invalid access
  group." instead of the parser text. `mobkit/access/get`, `preview` and
  every write recheck the caller's current administrator access at the owner
  instead of the request's captured view. Unchecked top-level writes
  otherwise keep their shape.
- `mobkit/init` composition provenance refusals now answer `-32019` with
  refusal `data` instead of `-32603` (see Added). A client that matched these
  refusals on `-32603` must match `-32019`; an SDK older than this release
  raises them as a plain `RpcError` with the new code.
- `mobkit/console/voice/captions` batches may carry a `playback_hint` entry
  (`{"kind": "playback_hint", "hint": "duck" | "restore"}`) next to
  `caption` and `retracted`. The embedded console ships with the gateway that
  emits it; a client that parses captions strictly must accept the new kind.
- Storage census slots add `resolution: "unverified"` when a bare injected
  WorkGraph store supplied no durability declaration. Existing declared
  resolutions and slot fields retain their wire shape. Both persistent and
  ephemeral constructors report the actually selected injected store,
  rather than a persistent, default-memory or unselected provider label.

- `ConsoleLogStore::history_prefix_revision` is an optional continuity witness
  for cached unsuccessful provenance searches. Its default returns `None`, so
  existing custom stores continue to rescan. Implementations must change the
  witness when stored provenance or cursor positions are replaced or removed.
- Typed compaction summaries remain runtime context and no longer project as
  human conversation rows. Ordinary user messages with identical text remain
  visible; the distinction comes from their typed origin.
- `runtime_notice_snapshot` carries complete `SystemNotice` bodies, including
  notice text and structured blocks. `ConsoleVisibilityPolicy` implementations
  that redact notices must cover this frame kind as well as `system_notice`;
  the fixed host policy runs before snapshot storage and broadcast. Compaction
  snapshots use the existing complete `history_positions` map over their
  `observed_through` boundary, while older sparse snapshots remain readable.
- The console SQLite store migrates to schema v3, including normalized member
  provenance. Migration accepts v1 and development v2 stores. This is a
  one-way upgrade: MobKit 0.8.42 cannot open the upgraded store. Keep a backup
  before upgrading if rollback to that release is required.
- Timeline storage faults now return HTTP 500 / RPC `-32000` with
  `timeline_unavailable`. Cursor replay loss remains HTTP 409 / RPC `-32013`
  with `replay_unavailable`; consumers must distinguish recovery from failure.
- Public timelines retain both live and canonical history rows with typed
  lineage. Consumers reconcile their message ownership using the typed
  source/run/interaction identity; authored text is not a deduplication key.
- Authorized `/console/experience` responses provide an opaque `storage_scope`
  for runtime/principal-separated browser drafts and queues. Explicit host
  `storageNamespace` values continue to take precedence.
- The memory steward's `memory.quarantine.verdict` frames report applied
  releases and tombstones only, with `rationale` null; its `hold` and
  `promote_pending_gate` verdicts no longer emit one (see Changed).

### Changed

- Process-local launches now run a native delivery owner over persistent
  composition. Both gateways' default process-local launches and the library
  `MobBootstrapSpec::ephemeral_runtime_backed*` path use a
  `PersistentSessionService` over an in-memory `MemoryStore` instead of
  `EphemeralSessionService`, so their storage-health session label reads
  `MemoryStore` instead of `EphemeralSessionService`.
- The session-service wrapper no longer absorbs the ephemeral service's
  `Unsupported` refusal of a boundary acknowledgement; the refusal reaches the
  caller.
- Image-generation ephemeral specs build their machine with
  `MeerkatMachine::ephemeral()`.
- Ephemeral launches now install the agent mob tools with an in-memory
  detached-job store, so their members can create child mobs, delegate and run
  detached jobs.
- The persistent `mobkit_gateway` opens the canonical `jobs.sqlite3` detached-job
  store.
- A restored classic mob resumes through a deferred activation that runs only
  after its address resolver has its handle and the delivery owner is armed.
- Runtime shutdown and teardown stop the native delivery owner explicitly
  after the mob stops, instead of when the last runtime clone drops.
  Undelivered rows stay pending in the durable inbox.
- The published crate no longer includes in-repo forensic test captures.
- The memory steward's quarantine `release` and `tombstone` verdicts run the
  operator review's store transaction (`StewardStore::review_quarantined`,
  reviewer `steward`; see Added), so a record is decided once, by whichever
  reviewer commits first; the other is skipped or replays.
  - A release's successor is `<memory_id>-released` instead of a fresh id,
    and a quarantined update is released as a supersede of the record it
    updates instead of leaving two active versions. When that record is
    gone, the release is refused (`stale_update`) and skipped loudly; the
    update stays quarantined, tombstone remaining its exit.
  - `memory.quarantine.verdict` comes from the store once the review
    commits, without the steward's rationale (its audit row keeps it).
    `hold` changes nothing and `promote_pending_gate` reports through
    `memory.promotion.pending_gate`, so neither emits a verdict any more.
- CI and release jobs on hosted Linux run on `ubuntu-24.04` instead of
  `ubuntu-latest`, which GitHub moves to Ubuntu 26 from 2026-10-19
  (actions/runner-images#14748). The move to 26 will be validated
  separately (meerkat#1767) instead of being inherited.
- Candidate launches now refuse a config that differs from the stored mob
  definition. A non-authoritative launch (`runtime_options.mob_composition =
  {"authority": "candidate"}`) resumes on the mob definition stored in its
  event log, because a resume cannot apply a new definition. It used to do
  that without comparing it to the supplied config, so an edited config (for
  example a profile's newly added `tools.deny`) silently stayed out of effect
  while the boot looked healthy. Such a boot now refuses with
  `MobCompositionProvenanceError::CandidateDivergent`, naming the diverged
  fields as dotted paths (`profiles.lead.tools.deny`).
  - To boot the stored definition knowingly, for example in a certification
    pass that deliberately differs from the stored composition, set
    `runtime_options.mob_composition.candidate_definition = "stored"`
    (`MobBootstrapSpec::with_candidate_definition(CandidateDefinition::Stored)`).
    The boot then runs the stored definition, logs the diverged fields at
    WARN, and reports them in a degraded `mob_composition` storage-health
    slot.
  - `candidate_definition` is candidate-only: an authoritative launch that
    sets `"stored"` is refused at parse time, since the composition pin
    judges it.
  - Candidates still never write or claim the composition pin. Authoritative
    launches are unchanged.
  - Upgrading: a candidate-then-promote pipeline whose candidate config
    differs from the stored definition must add
    `candidate_definition = "stored"` to the candidate launch, or run the
    candidate on its own storage path.
- `wait_for_output` and `wait_for_output_containing` no longer poll.
  - **Rust:** the `IdentityRuntime` methods read the member once, then again
    only when the identity table signals a change. A completion, a
    registration and a lease change all signal it. Before, the methods
    re-read every 500 ms. The caller's timeout is the only timer.
  - **Python:** a bare `IdentityAgentHandle.wait_for_output()`, the
    deprecated `baseline=` form and `wait_for_output_containing()` read the
    member, then wait server-side (`mobkit/wait_for_completion`) for the next
    completion past the cursor that read saw. Before, they called
    `mobkit/inspect_identity` every 1.5 s. `poll_interval` now applies only
    to gateways that predate the server-side wait.
  - **Behavior change:** a bare `wait_for_output()` or
    `wait_for_output_containing()` on a live alias with no completion cursor
    now raises the typed `CompletionCursorUnavailableError` (exported from
    `meerkat_mobkit`, carrying `identity`) at once. Before, it polled
    `inspect_identity` until its timeout. **Migration:** a live alias has no
    completion to wait for, so wait on its turn instead: send or dispatch
    with `track_turn=True` and pass the ticket as `wait_for_output(turn=...)`
    or `wait_for_turn(...)`. Identity-first handles are unaffected.
    `baseline=` is not a migration path, because it now waits on completions
    too. The cursor waits that already refused a live alias (`after=`,
    `wait_for_completion`) now raise the same class.
    `CompletionCursorUnavailableError` subclasses `RuntimeError`, which
    those waits raised before.

- Behaviour change (identity-first runtimes with a topology provider): with
  a topology provider, reconcile unwires identity edges that neither the
  provider nor the definition's wiring declares, including edges wired
  before a restart; to keep such an edge, declare it in your provider.
  Runtimes without a topology provider are unchanged: they unwire only the
  edges they wired.
- Behaviour change: dropping a `MobkitRuntimeHandle` without `shutdown()`
  now terminates its live module processes; previously they kept running.
  Call `shutdown()` for an orderly stop (see Fixed, module subprocess reaping).
- MobKit runs on meerkat 0.8.51:
  - `UnifiedRuntime::shutdown` drives the mob actor's terminal teardown with
    one `MobHandle::shutdown_with_report` call bounded by
    `MOB_TERMINAL_SHUTDOWN_BUDGET` (5 s), instead of retrying `shutdown` for
    up to 5 s. Meerkat now owns the convergence (stuck retirements, held
    unregisters and in-flight runs are settled or reported, not refused), and
    the outcome is `UnifiedRuntimeShutdownReport::mob_terminal_shutdown`:
    `Completed(MobShutdownReport)`, `AlreadyShutDown` (the mob answered with a
    closed command channel, which a mob shutting down answers to every caller
    request), `Refused` or `SkippedMobStopFailed`. It stays outside
    `cleanup_completed()`, as before.
  - The gateway's runtime shutdown budget counts that 5 s teardown:
    `GATEWAY_RUNTIME_SHUTDOWN_TIMEOUT` (and `UnifiedRuntime::shutdown_horizon`)
    is 317 s, and the advertised `stdio_shutdown_horizon_ms` is 352 000, so
    the outer timeout can never fire inside the teardown and drop the
    per-member shutdown report. Hosts that honour the advertised horizon need
    no change. Check the deployment's configured termination grace period
    against 352 s plus process exit: a shorter grace can cut a shutdown short
    and lose its per-member report.
  - The implicit-delegate idle sweep warns when a retirement does not
    complete within `retire`'s wait, then awaits the member's
    `retirement_settlement()` and reports how it settled: retired, or stuck
    at a named stage with its cause. A stuck retirement stays owned by the
    mob until it is re-driven (`MobHandle::redrive_retirement` or a mob
    resume); the sweep no longer loses sight of a member that stays
    `Retiring`.
  - The mob stop logs each member whose run starts the stop could not hold.
  - The teardown mob stop (`UnifiedRuntime::shutdown`,
    `stop_mob_for_teardown`) drops its 10 s retry loop. That loop re-sent
    Stop every 250 ms and cancelled every member's work on a refusal.
    - Most stops are now one Stop call.
    - When an active flow run refuses the Stop (meerkat's `no_active_runs`
      guard), teardown cancels each non-terminal run, awaits its terminal
      event on the mob event ledger, and stops the mob.
    - A run's terminal event lands before the actor retires the run, so a
      refused re-send is re-issued on each machine commit until it is
      admitted, never on a timer. meerkat#1593 tracks replacing this with an
      exact signal.
    - It is bounded by `MOB_STOP_FLOW_SETTLE_BUDGET` (10 s, the old window).
      Past it, `MobRuntimeError::MobStopFlowRunsUnsettled` names the
      unsettled runs and the last refusal; that is also where a refusal from
      another guard surfaces once teardown has settled runs.
    - Without flow runs to settle, a refusal is returned as is.
    - `MobStopOutcome::ProceededWithoutInterrupt` and
      `ErrorEvent::MobStopProceededWithoutInterrupt` are no longer produced.
      The `Runtime not ready: attached` refusal they degraded came from
      meerkat's old stop interrupt path. Both stay for wire and SDK
      compatibility.
  - Live channel closes pass their typed cause to meerkat (`OpenAbandoned`
    for an open-failure cleanup, `ClientRequested` for `live/close`).
  - A profile naming a Rust tool bundle that is not registered is refused
    with the typed `MobRuntimeError::Mob(MobError::ToolBundleUnavailable {
    bundle })` instead of an internal error string; match the variant.
- Docs: `[profiles.*.tools] deny` (meerkat 0.8.51's per-profile tool deny
  list) is documented in the configuration reference, including both
  failure paths, and the `comms` row points at it.
- Console: member-kickoff status renders as a typed status card instead of a
  peer request. Meerkat sends kickoff status (`mob.kickoff_pending`,
  `_starting`, `_started`, `_callback_pending`, `_failed`, `_cancelled`) as
  one-way comms `lifecycle` notices (meerkat #1608); older sessions keep the
  `request` form. Both forms render, by their intent, as one compact row:
  the member, its role and the phase (Pending, Starting, Started, Waiting for
  callback, Failed, Cancelled). The signals rail labels them "Kickoff
  <phase>", and a failed kickoff reads as a warning there. A lifecycle
  notice's content is model-facing notice text and is never shown: any other
  lifecycle notice renders by its summary (`Peer lifecycle: <kind>`). The
  shared host previously showed that text verbatim.
- **Upgrade action for hosts that serve application tool policies:** a
  member's `delegate` and agent `mob_create` are refused until the host
  chooses a child application tool policy (see Added). MobKit now hands its
  tool-policy registry to the agent mob tools, which it never did before, so
  meerkat 0.8.51's child policy applies: without a child policy, a governed
  member's delegate helpers and the members of the mobs it created ran
  outside the host's policy. There is no implicit default. The refusal is
  the typed tool error `policy_denied` with code
  `child_tool_policy_required`; the member's turn goes on, and the message
  says why and names the fixes (the `child_application_tool_policy` init
  parameter, `with_child_application_tool_policy`, or an explicit
  `{"kind":"unmanaged"}`). Same-mob `fork_off`, `mob_spawn_member` and
  councils are unaffected, and hosts without application tool policies see
  no change.

- Python and TypeScript SDKs: `*_and_wait` / `*AndWait` never pass off
  another turn's output as attributed and never lose an admitted delivery.
  Any other peer's or scheduled turn can satisfy the identity-wide wait used
  for an untracked delivery, and an observation error after a successful
  admission (for example `observation_lane_saturated`) discarded the
  `DispatchResult`, inviting callers to redispatch business work.
  - An untracked delivery (any delivered `turn_unavailable.code`, including
    `autonomous_host`, the default mode, or a gateway predating turn
    tickets) keeps the identity-wide wait and its
    `TurnTrackingUnavailableWarning`, now naming the code, and the typed
    outcome says the result is not attributed. Nothing raises by default
    that did not before; `require_attribution` opts into raising (see
    Added).
  - A transport or RPC failure of the exact-ticket wait retries only that
    ticket's observation, with bounded backoff, until the caller's deadline,
    then raises `PostAdmissionObservationError` with `admission`, `ticket`
    and the last failure as its cause. The delivery is never repeated.
  - Every error these calls raise after admission keeps its type and carries
    the exact `admission` (and `ticket`, `None`/`null` when untracked):
    `TurnFailedError`, `TurnUnknownError`, `WaitEndedError`,
    `TurnNotDeliveredError`, the identity-wide wait's missing-baseline,
    superseded-incarnation and live-alias errors, a raw persistent-transport
    timeout or closed-process error, and in Python a caller's
    `asyncio.CancelledError` (the same instance, re-raised). A turn still
    pending at the deadline raises `TurnWaitTimeoutError` (a Python
    `TimeoutError`); the Python identity-wide deadline raises it too.
  - The server-side `mobkit/wait_for_turn` stays the primary wait; only the
    exact-ticket `mobkit/turn_result` poll remains as the fallback for
    gateways without it.
- Console: the chat turn-navigation rail no longer draws a gray capsule track
  behind its ticks; the ticks stand on their own.

- The console forwarder and the identity health monitor no longer need
  MobKit's create-time live event tap (meerkat #1236). Meerkat 0.8.49
  retains each session actor's recent events and replays them on
  `MobHandle::subscribe_agent_events_from(identity,
  SessionEventCursor::Earliest)`, so both attach whenever they reconcile and
  still read a run that started before they attached, including a restored
  member's first run after a gateway restart, from `run_started`. A gap
  beyond meerkat's 1024-envelope window arrives as meerkat's typed
  `StreamTruncated(StreamLagged)` marker. The console subscription names the
  exact actor incarnation for every local member, so a revoked predecessor's
  stream still drains (bounded by the drain deadline) before its successor
  attaches, now also for members the tap never captured (eager creates, and
  creates outside the witness-bearing routes). Replayed envelopes an earlier
  stream of the same member already forwarded are not repeated. The
  completion-cursor drain replays the session's retained events past the
  health monitor's credited position instead of draining an unadopted
  capture. The tap module (both lanes, capture queues, pumps, byte bounds,
  arming) and its spec and session-service plumbing are removed.
  Session-service decorators forward meerkat's new
  `subscribe_session_events_from` and `subscribe_agent_session_events_from`.
- Identity completion crediting is exactly once by construction. The health
  monitor's stream and the completion-cursor drain claim each event's
  sequence in a per-session ledger with a high-water per meerkat sequence
  space (`SessionEventEpoch`): a placed member's host restart starts a fresh
  high-water instead of refusing its first events, and a late stream of an
  older space re-attaching claims nothing it already credited. A terminal is claimed
  and credited under the identity's completion-credit lock, which a cursor
  read holds across its drain, so a read never sees a claim without its
  credit. The drain credits failed runs too, so a failure-aware wait ends
  typed. A runtime without a drain (no session service) keeps the monitor
  on live-only subscriptions, since replay could credit a completion after a
  baseline read.
- `/agents/{id}/events` SSE replays the member actor's retained events on a
  connection without `Last-Event-ID` (meerkat #1236), so a client that
  connects after a run started still receives it from `run_started`. Event
  ids are now `<epoch>:<seq>` positions in the member's event sequence
  instead of connection-local `<agent_id>:<n>` counters, and an
  `EventSource` reconnect resumes right after its `Last-Event-ID` instead of
  replaying the window again. A `stream_truncated` gap event carries no id.
  Routers built with the public `agent_events_sse_router*` constructors from
  a live-only subscribe function keep the old ids and do not replay.
- A member event subscription whose member has no live session actor now
  fails with meerkat's typed `MemberSessionNotLive` ("mob member '...' has no
  live session actor for session ..."), no longer an internal "failed to
  subscribe to agent events" error.
- A successor actor for the same session now continues the session's
  meerkat event sequence instead of restarting at 1 (meerkat #1236), so the
  console's per-session `source_sequence` ordering places a successor's run
  after its predecessor's.
- Behaviour: queue-mode sends and dispatches return their admission receipt
  before the target's peer neighbourhood is hydrated. A send to an
  already-Active member no longer builds its Dormant peers or reconciles
  managed edges before admission. A woken member's first turn can reach only
  peers that were already Active; Dormant peers are built and wired right
  after admission.
- Behaviour: topology reconcile and topology mutations no longer take
  identity lifecycle locks, so a lifecycle operation (reset, retire, alias
  rebind) can run concurrently with wiring. The commit revalidates bindings
  and replans instead of serializing.
- Behaviour (console): a failed send is classified from typed transport
  facts. 401, 403, read-only, -32602 and listed pre-ingress statuses are
  definite rejections ("Retry same attempt"). A connection failure (no HTTP
  answer, e.g. a lost acknowledgement, shown as "Acceptance unknown"),
  timeout, 429, 5xx, non-JSON answers, interrupted and other typed refusals
  stay reconcilable
  ("Check acceptance"), each with its named reason. A saved attempt answered
  after its console view ended is settled in its own storage namespace
  instead of staying "Awaiting acceptance".

- The continuity repair supervisor is event-driven. It used to retry Broken
  identities on a doubling timer (30 s, capped at 10 min), so a reseed boot
  left members parked for 1 to 15 minutes while the process sat idle. It now
  runs one pass when it starts and then retries on a typed trigger: another
  identity's embodiment settled (a resume or a fresh durable mint committed,
  releasing the store writer it held), a host restore or reconcile pass
  settled (boot, a roster refresh, `mobkit/reconcile_identity`), a delivery or
  materialization refused because the identity is Broken (a caller's
  demand), or an explicit request. It defers a pass while other embodiments
  are in flight, so a warm-up burst runs one pass and cannot trip the
  identical-failure park in seconds. Its own passes never re-trigger it. Only
  a transient cause with no typed signal (an unreachable heal authority, a
  pass failing on a roster provider or store error, a member Broken on an
  unavailable store) arms a last-resort fallback wake (1 s doubling to 30 s,
  shared with the agent-memory observer), so no Broken identity is stranded
  in a steady deployment. The byte-identical-failure park and the typed
  terminal parks are unchanged.

- Improve console reading and composition in the stock and reusable hosts:
  preserve the reading position while output streams, render complete Markdown
  with exact source copy, and use a compact latest-message arrow that animates
  during agent work. Quoting appears beside a selection; quoted context can be
  edited before sending and is displayed as readable cards after delivery.
- Add roster activity filters, shared approval state with precisely correlated
  conversation cards, and durable text/quote send attempts that reconcile lost
  acknowledgements without erasing a newer draft.
- Preserve repeated authored text, reasoning and tool outcomes across live
  output, history paging and reconnection. Saved assistant messages no longer
  create synthetic run-completion events. Fold only routine tool activity with
  matched successful outcomes, and keep failures and unknown outcomes visible.

- **BEHAVIOR CHANGE: `idle_retire_secs: null` on the `delegate`, `fork_off`
  and `mob_spawn_member` agent tools now means the same as omitting it.** It
  used to disable idle retirement for the spawned member. MobKit's argument
  was the lone exception to the rule every other optional tool argument
  follows (meerkat's own tool args, such as `fork_off`'s `message_count` and
  `max_run_secs`, are `Option` fields where null and absent both mean "not
  set"), and models routinely send null for optional fields. Now: omitted or
  null on `delegate` uses the runtime default, on `fork_off` opts the child
  in on the runtime default, and on `mob_spawn_member` leaves the member out
  of retirement; an integer is that many idle seconds on all three; any
  other value is still refused as invalid arguments. There is no
  model-facing way to disable retirement any more: a host that wants
  spawned members kept sets `implicit_delegate_idle_retire_secs` to `null`
  (Python `.implicit_delegate_idle_retirement(None)`, TypeScript
  `.implicitDelegateIdleRetirement(null)`). The tool descriptions and schemas
  say so; the schema still accepts null. `DelegateIdleRetireOverride::Disabled`
  stays: `member_idle_retire` rows an earlier version persisted with it
  still load and still keep their member, and host-side code may still set
  it.

- Bind the Meerkat family to published 0.8.47. Voice readiness no longer
  loads the session body (lukacf/meerkat#1244): meerkat adds the required
  `MobSessionService::observe_live_durable_source` (forwarded by both MobKit
  session decorators) and `MobHandle::observe_live_durable_source_readiness`,
  and MobKit's live binding authorities override
  `observe_live_durable_source_readiness` with it. The probe checks the
  member's live binding against the published machine snapshot, observes the
  durable source body-free, authorizes the binding and checks credential
  readiness, with no mob actor command, no preface and no pending channel.
  The real open keeps the full, actor-validated durable check.
- Bind the Meerkat family to published 0.8.46. Public Live under
  `ProviderManagedUnmeasured` (console voice on gpt-live-1) now commits one
  canonical assistant row per provider turn segment instead of one row per
  transcript delta (lukacf/meerkat#1237), so the spoken answer lands as one
  message when its segment seals (turn end, a typed between-speech boundary,
  or channel close). Also included: a reopen whose context summary missed the
  pre-open bound is no longer silent when a typed turn is queued
  (#474), and playback snapshot rows always name their channel. MobKit builds
  against 0.8.46 without source changes. See Added for the console captions
  that cover the time until the row lands.
- Bind the Meerkat family to published 0.8.45: bounded per-member status
  observations with a typed `preview_unavailable` reason, fork lineage on the
  builds of fork-derived members (`SessionBuildOptions::fork_source`), the
  runtime's uncommitted-run-input read, and assistant message ids on streamed
  and committed events. See Added and Fixed for what MobKit surfaces.
- Bind the Meerkat family to published 0.8.44, including durable runtime-notice
  origin and transcript lineage. The 0.8.43 lifecycle changes remain included:
  `fork_off` and council
  completions arrive in the owner's transcript as a persisted `BackgroundJob`
  notice delivered in-turn (MobKit's callers are all mob members, so no
  `DetachedOwnerHost` is needed), with the typed `no_owner_revival_host`
  in-turn fallback; retiring a member retires everything it spawned
  (`spawned_by`); member status never waits for a running turn; legacy
  `model_fallback` tables load as no fallback policy with a warning (the
  gateway's host config file included, now with fallback off and a warning
  naming the file instead of a startup refusal); a whole-crew stop and resume
  keeps coordinator-owned workers.
- Bind the Meerkat family to published 0.8.42 (Claude Opus 5.5 as the default
  Anthropic model, GPT-6 Sol and Luna in the model catalog, upstream crates/
  repository layout).
- Repository layout: the workspace crates live under `crates/`
  (`crates/meerkat-mobkit`, `crates/mobkit-store-conformance`), the memory
  calibration harness under `tests/memory-evals`, the documentation review
  archives under `docs/internal/audit`, and the Bazel execution platforms under
  `tools/bazel/platforms`. The dangling `vendor` symlink is gone. Crate names,
  published packages, release asset names and the embedded console bundle path
  inside the crate (`console-dist/`) are unchanged; scripts, CI, the console and
  flow-editor build scripts, the Bazel labels and the docs point at the new
  paths.

### Added

- `MobRuntimeDelivery` (`new`, `inbox`, `last_pass`, `is_running`) composes a
  host's native job and continuation delivery owner, and
  `MobRuntime::runtime_delivery_pass` and `MobRuntime::stop_runtime_delivery`
  observe and stop it.
- Operators can decide quarantined agent memory from the console:
  `mobkit/memory/quarantine/decide` releases or tombstones one quarantined
  identity-scope record, without the memory steward or the gating flow.
  - Until now only the steward's dream could decide a quarantined record.
    With the steward disabled, a write quarantined by session taint could
    never become memory, and the console's quarantine queue was read-only.
  - A release keeps the native review semantics. The content is re-staged
    in the same scope at `agent_observed` as `<memory_id>-released`, with
    `derived_from` the original, which is tombstoned and kept with its
    provenance. The successor stays capped at `agent_observed` forever, the
    secret gate refuses secret-shaped content, and a quarantined update is
    released as a supersede of its still-active prior instead of forking.
  - The decision binds the record's `content_hash` (now returned by
    `mobkit/memory/panel/record`), refuses typed with `-32043`
    (`MEMORY_QUARANTINE_REVIEW_REFUSED_CODE`; `not_found`,
    `content_mismatch`, `not_quarantined`, `gate_pending`,
    `successor_conflict`, `secret_detected`, `stale_update`) and records
    the review (verdict, authenticated reviewer, bound hash, quarantine
    reason, rationale) in each audit row.
  - Replays (`already_released`, `already_tombstoned`) are recognized from
    that audit evidence alone and return the original decision without
    writing; a row merely holding the successor id is refused.
  - Gated promotions expire after seven days (shared with the steward's
    dream as `GATED_PROMOTION_EXPIRY_MS`). Until then a pending promotion
    blocks a release of its record (`gate_pending`, with `expires_at_ms`);
    after that a review expires it itself, so a promotion orphaned by a
    restart blocks a release only until its expiry, whether or not a
    steward runs.
  - An operator's tombstone is never blocked by a pending promotion: it
    invalidates the record's pending promotions in the same transaction
    (mappings expired, staged batches discarded, listed as
    `invalidated_promotions` in the decision), so a later approval
    publishes nothing. Gating resolutions now change only a still-pending
    promotion, so a late approval, denial or escalation can neither
    overwrite an invalidated promotion nor revive it.
  - Quarantined content stays reviewer-only after a review or `forget`
    tombstones it: the Memory panel gates tombstoned records that were ever
    quarantined behind `memory.quarantine.review`, as it gates quarantined
    ones. The released successor is ordinary memory.
  - A review emits `memory.quarantine.verdict` only once its decision
    commits, and the event never carries the reviewer or the rationale.
  - Authority composes existing grants: `memory.quarantine.review`,
    `agent.view` and `agent.memory.read` on the identity, plus
    `agent.memory.write` to release or `agent.memory.delete` to tombstone.
    Read-only consoles refuse it.
  - The store operation is `StewardStore::review_quarantined`; the Memory
    panel offers Release and Tombstone on a quarantined record.

- Checked access saves (`checked_v1`). `mobkit/access/get`, and
  `mobkit/access/status` for a caller who can administer, advertise
  `conditional_mutations: "checked_v1"` with an opaque `owner_instance` for
  the controller lifetime. The six access writes accept a nested
  `params.checked_v1` carrying `owner_instance`, `expected_revision` and the
  write's payload. The owner rechecks the caller's administrator access, its
  instance and its revision inside the serialized mutation, and answers
  `-32009` with `access_revision_conflict` (expected and actual revisions) or
  `access_owner_changed` without writing. Malformed, mixed or unknown checked
  envelopes fail with `-32602` and never fall back to an unchecked write. A
  controller that could not draw an instance identity advertises no checked
  capability and refuses checked writes with `-32004`
  `access_mutation_unavailable`; unchecked writes stay available.
- Console Access saves are checked. Each edit keeps the owner instance,
  revision and configuration from when it began. A conflict, owner change or
  unavailable checked save keeps the draft with a fixed notice and requires
  **Review and reapply** before another explicit save; an invalid
  configuration keeps the draft editable for correction. Refreshing never
  rebases or resends a draft, and edits stay disabled while owner state is
  loading or stale, or when the owner does not advertise `checked_v1`. A
  preview survives an unchanged refresh and is discarded when the owner,
  revision or configuration changes. The real-browser scenario
  `real-checked-save-recovery` drives this flow against the acceptance
  fixture's real access owner.
- A typed `mobkit/init` code for mob composition provenance refusals: `-32019`
  (`COMPOSITION_PROVENANCE_CODE`), with Python and TypeScript
  `CompositionProvenanceError` (#613).
  - Every refusal the gateway raises because a persistent mob storage's
    recorded composition cannot be proven to match the launch used to reach
    the client as a generic `-32603`, so an SDK could not tell a deliberate
    refusal from an internal error without parsing the message. This covers
    a diverged definition, a candidate launch whose definition differs from
    the stored one, a rehearsal-created store, an unreadable, malformed,
    unsupported or unwritten manifest, and undeclared non-empty storage.
  - The error `data` carries the refusal: `kind` (`divergent`,
    `candidate_divergent`, `created_by_rehearsal`, `missing`, `unreadable`,
    `malformed`, `unsupported_version`, `not_recorded`, `unproven_storage`),
    `fields` (the diverged definition fields as dotted paths, always
    present), and the manifest path, storage path and manifest versions
    where the refusal has them. The SDK error exposes `kind` and `fields`.
  - The message is unchanged, and an accepted init carries the same code and
    data in its `failed` settlement.
  - Rust: `MobCompositionProvenanceError::refusal()` returns the data as
    `CompositionProvenanceRefusal`, and
    `UnifiedRuntimeBootstrapError::composition_provenance()` finds the
    refusal behind a bootstrap failure.
  - `mobkit_gateway` (the console/HTTP binary) still reports these refusals
    as `-32603`.

- Host-only `mobkit/member_activate_instruction` and
  `mobkit/member_instruction_activations` (Python
  `activate_member_instruction` / `member_instruction_activations`,
  TypeScript `activateMemberInstruction` / `memberInstructionActivations`).
  A host can change a member's standing instructions after its session was
  minted. Resume authors nothing, so a restored member otherwise keeps its
  creation-time prompt, for example after its skills changed.
  - The verb appends one keyed, immutable instruction activation to the
    member's current session through Meerkat's
    `MobHandle::activate_member_instruction`. It is forward-only, survives
    resume, and re-applying it is a typed `duplicate`.
  - A call made mid-turn blocks until that turn finalizes, so it can wait
    for a whole turn, and applies at the boundary. The SDK methods take an
    optional per-call reply timeout (`transport_timeout` /
    `transportTimeoutMs`) for members with long turns. Refusals stay typed per
    Meerkat class in `data.class`, and nothing retries in the gateway:
    - `admission` with `instruction_activation_code`; `session_busy`,
      `live_channel_open` and `target_not_materialized` answer `-32015`;
    - `session` with `session_error_code` and, for a refused request, the
      typed `instruction_activation_error_code`, such as `digest_mismatch`
      or `effective_activation_conflict`, answering `-32602`;
    - `runtime`;
    - `owner_task`.
  - The read verb returns the member's durable activation records and, for
    one exact key, its `key_state`.
  - Neither verb is an agent tool. The audit actor is `host`.

- The console renders recognized current operation refusals, confinement
  causes, pre-tool hook decisions/launch failures and outcome-audit failures as
  distinct local feedback. Permitted sibling results and error status remain
  visible; an outcome-audit notice does not change the action's actual result.
  Unknown or malformed tool causes use the ordinary tool-result path, which may
  display raw output, without a permission or mechanical-refusal classification.
  Custom workspace hosts should retain `ConversationMessageEntry.operationFeedback`;
  these projections do not configure native authorization or supply a grant editor.
- Console voice applies Meerkat's barge-in playback hint
  (`live/assistant_playback_hint`, meerkat #1651). When the user speaks over
  audible assistant audio, Meerkat asks the client to duck its playback, and
  to restore it when the overlap ends; the provider has no command that
  clears audio it already queued. Console voice's public observation
  publisher now implements `publish_playback_hint` (playback-output
  publications are still refused) and puts the newest hint on the call's
  caption stream as `{"kind": "playback_hint", "hint": "duck" | "restore"}`.
  The console routes the assistant's audio through a playback gate after the
  speaker analyser and ramps it to silence on `duck` and back on `restore`.
  Mute and speaker state stay on the master gain.
- Python SDK: `MobKitBuilder.gateway_init_timeout(seconds)` sets how long
  `connect()` waits for the gateway to answer `mobkit/init` (its `accepted`
  acknowledgement, or an older gateway's single response). The default stays
  60 s. It takes a positive finite number of seconds; booleans, NaN,
  infinities, zero and negatives raise `ValueError`. It bounds a slow host
  startup and does not fix one: running out still raises
  `InitOutcomeUnknownError` and cleans the gateway up. Only the init request
  uses it. Ordinary RPCs and provider callback deadlines keep their own
  timeouts, and `init_deadline(seconds)` still bounds the wait for an
  accepted init to settle.
- `child_application_tool_policy`, the application tool policy members of
  child mobs (agent `mob_create`, and the implicit mob `delegate` helpers run
  in) are built with, as an `ApplicationToolPolicyBinding`:
  `{"kind":"provider","provider_id":...,"policy_id":...}` or the explicit
  opt-out `{"kind":"unmanaged"}`.
  - `mobkit/init` top-level parameter, next to `application_tool_policies`.
    The gateway refuses the boot (`-32602`) for a malformed value,
    `{"kind":"inherit"}`, or a provider binding whose provider or policy no
    served policy carries.
  - Python `MobKitBuilder.child_application_tool_policy(binding)`.
  - Rust `MobBootstrapSpec::with_child_application_tool_policy` (and
    `with_optional_child_application_tool_policy`) and
    `UnifiedRuntimeBuilder::child_application_tool_policy`; two different
    bindings refuse the build with `ConflictingConfiguration`.
  - `member_tool_policy::child_application_tool_policy_from_init_params` and
    `validate_child_application_tool_policy`.
- Child-available host tool bundles:
  `MobBootstrapSpec::register_tool_bundle_with_availability` and
  `UnifiedRuntimeBuilder::register_tool_bundle_with_availability` take a
  `ChildToolBundleAvailability` (re-exported from MobKit).
  `register_tool_bundle` keeps registering `HostOnly` bundles, which child
  mobs never receive; a `ChildAvailable` bundle is supplied to every child
  member, still narrowed by the child profile's `deny` list and governed by
  the child application tool policy. Documented under "Child mob tool policy
  and bundles" in the configuration reference.
- `MobBootstrapSpec::register_tool_bundle(name, dispatcher)` and
  `UnifiedRuntimeBuilder::register_tool_bundle(name, dispatcher)` forward
  host Rust tool bundles to meerkat-mob's `MobBuilder::register_tool_bundle`,
  so a profile's `tools.rust_bundles` reaches its members on spawn, resume
  (including the members resume revives) and respawn. A profile naming an
  unregistered bundle is refused when its member is built. Registering one
  name on both the builder and a supplied spec is refused as conflicting
  configuration. Agent-created child mobs receive only the bundles
  registered as child-available (see above).
- Console: Find in transcript searches every loaded message, including turns
  the windowed transcript keeps out of the DOM where the browser's
  find-in-page cannot reach. Open it from the search button beside Copy
  transcript or with Control+Shift+F in a chat pane; Enter and Shift+Enter
  step through matching rows (newest first), each brought into view (and
  revealed when it is behind Show earlier messages) and highlighted with
  the CSS Custom Highlight API, with a live "n of m" count; Escape closes it.
- `mobkit/init` can run as accepted-then-settled (#550), so startup is no
  longer cut off by the SDK's 60 s request timeout. Both SDKs opt in by
  default.
  - The SDK sends `init_protocol: "accepted_then_settled"` and an `init_id` it
    is already listening for. After the cheap pre-durable gates pass, the
    gateway answers `accepted` at once. It then emits
    `mobkit/init_progress {init_id, phase}` at each startup phase: `storage`,
    `owner_publication`, `prepare`, `roster`, `register_owners`, `prewarm`,
    `activate`, `restore`, `schedules`, `serve`, and `cleanup` on failure.
    Finally it emits exactly one `mobkit/init_settled`, either
    `{outcome: "ready", ...init result}` or
    `{outcome: "failed", code, message, data, durable_effects}`.
  - `durable_effects` is `"none"` only when init failed before the `storage`
    phase, and `"possible"` otherwise, including when cleanup fails.
  - A gateway shutdown requested during init stops at the next phase
    boundary, runs runtime cleanup, settles `failed`
    (`data.reason: "shutdown_requested"`), and only then answers the shutdown
    request.
  - A client that does not opt in gets the single legacy response unchanged
    and never sees `accepted`. A new SDK talking to an older gateway uses that
    gateway's single response as before.
  - The SDKs have no built-in total deadline for an accepted init, because
    persisted-authority prewarm on a large roster is deliberately unbounded.
    The wait ends on the settlement, the gateway exiting, a reader failure,
    or the caller's deadline (Python `MobKitBuilder.init_deadline(seconds)`,
    TypeScript `initDeadline(ms)`).
  - A `failed` settlement raises the same typed error as before, with
    `data.durable_effects`.
  - Once the init request is written, losing the answer, the reader or the
    gateway raises the new `InitOutcomeUnknownError(init_id, last_phase,
    reason)`, never a refusal, and is never retried automatically.
  - New Rust API: `UnifiedRuntime::set_bootstrap_phase_observer` with
    `UnifiedRuntimeBootstrapPhase` (`#[non_exhaustive]`), which reports
    owner registration, prewarm, activation, roster restore and failure
    cleanup.
  - Startup provider callbacks must not issue ordinary RPCs on the same
    gateway, because ordinary dispatch starts only after init settles.
- `mobkit_store_conformance::chapters::continuity_wrapper_preserves_incremental_channel`
  takes an incremental-capable substrate and the embedder's own wrapping
  function. It fails when the wrapped store, or the
  `ContinuitySessionStoreAdapter` over it, loses the session-delta channel.
- `UnifiedRuntimeBuilder::require_incremental_session_persistence(true)`
  turns on opt-in strict mode. `build()` then refuses with
  `SessionStoreNotIncremental` instead of starting on the whole-blob fallback.
- `ContinuityStore::store_type_name()` is defaulted to the concrete type
  name, and `ContinuitySessionStoreAdapter::continuity_store_type_name()`
  exposes it. The startup warning for a whole-blob session store now names
  the continuity store behind the adapter and says how to fix it.

- `ConsoleSendDedupeDurability` (`Durable` | `ProcessLifetime`) and
  `ConsoleLogStore::send_dedupe_durability()`, a defaulted trait method that
  returns `ProcessLifetime` unless a store vouches for durability.
  `SqliteConsoleLogStore::open` reports `Durable`;
  `SqliteConsoleLogStore::in_memory` and `InMemoryConsoleLogStore` report
  `ProcessLifetime`. `MobKitConsoleAggregator::send_dedupe_durability()`
  reads it from the store in use, and `GET /console/experience` exposes it
  as `send_dedupe: { durable: bool }` (absent without a console
  aggregator). Custom stores that persist dedupe records across restarts
  should override the method.

- Typed wait outcomes in both SDKs: `send_and_wait_outcome`,
  `dispatch_and_wait_outcome` and `dispatch_text_and_wait_outcome` (Python)
  and `sendAndWaitOutcome` / `dispatchAndWaitOutcome` (TypeScript) return
  `AwaitedTurn` with `text`, `attributed` (whether the text provably belongs
  to this delivery's turn), `admission`, `ticket`, `output_status` and
  `untracked_code`. The plain `*_and_wait` methods return its text.
- `require_attribution=True` (Python) / `requireAttribution: true`
  (TypeScript) on the wait helpers raises `TurnTrackingUnavailableError` in
  every untracked case instead of waiting identity-wide.
- Error types `TurnTrackingUnavailableError`, `PostAdmissionObservationError`
  and `TurnWaitTimeoutError` in both SDKs.

- `mobkit/stop_member_run` relays meerkat's run-fenced Stop
  (`MobHandle::stop_member_run`, lukacf/meerkat#1279) on the unified RPC and
  the console gateway. Params are `{ member_id, run_id, reason }`, and the
  result is `{ member_id, receipt }`, with meerkat's typed receipt verbatim
  (`stopped` with contributors, `not_current`, or `not_stoppable`). Read the
  run id from the member's `run_started` event (`identity.run_id`).
  - The member's runtime stops exactly that run and terminalizes every input
    already bound to it, including durable steers that joined it.
  - A stale run id is `not_current` and never interrupts newer work.
  - Malformed params are `-32602`. The console authorizes the method like
    `mobkit/force_cancel_member`.
  - SDKs: Python `MobHandle.stop_member_run(member_id, run_id, reason=)`
    returns the typed `MemberRunStopReceipt`. TypeScript
    `MobHandle.stopMemberRun(memberId, runId, reason)` returns
    `MemberRunStopReceipt`, with `not_stoppable.state` typed as
    `MemberRuntimeState`. Both fail closed on a malformed receipt.
  - Unknown members and host refusals keep meerkat's typed code and
    structured data.
  - Console: a member's chat header offers **Stop run** while the timeline
    names an in-flight run (its `run_started` run id). It is gated like
    Retire, and force-cancel is unchanged. The receipt is summarized in the
    header: the cancelled inputs, "that run already ended", or the runtime
    state that refused the stop.
- ConsoleSend (`POST /console/send`, `mobkit/console/send`, and the
  multipart form) accepts typed `skill_refs` for the exact addressed member
  (#486). The selection rides meerkat's fenced host-human admission
  (`MobHandle::submit_host_human_input_with_options_bounded`), so the member
  resolves it natively (`skills_resolved` / `skill_resolution_failed`,
  durable `SkillContext`) with the same exact-target and delivery-identity
  receipts as the content. A nonempty selection joins the idempotency
  fingerprint (an empty one keeps the previous fingerprint): a same-key retry
  with the same selection returns the original acceptance, and a changed
  selection is `idempotency_conflict`. Remote and externally bound members
  refuse a selection typed instead of delivering without it.
- Typed per-member restore progress on the identity bootstrap status. During
  an eager restore pass each entry carries `restore`: `registered`,
  `resuming`, `minted`, `resumed`, or `broken` with the typed continuity
  failure kind, and a member's bootstrap state settles (`active` / `broken`)
  as soon as its own restore does, before the whole pass ends.
  `IdentityRuntime::watch_identity_bootstrap_status` streams the snapshot in
  process; the Python SDK parses it as `IdentityRestoreProgress`.
- `mobkit/request_continuity_repair` (and `request_continuity_repair()` /
  `requestContinuityRepair()` in the SDKs,
  `IdentityFirstRuntimeContext::request_continuity_repair` in Rust) asks the
  continuity repair supervisor for a pass and answers, typed, what it reached:
  `scheduled`, `nothing_to_repair` or `no_supervisor`. `mobkit/reconcile_identity`
  reports the same for the supervisor pass its settled pass triggered
  (`continuity_repair`), and a `broken` outcome now carries its typed `kind`.
- The TypeScript SDK gains `identityBootstrapStatus()` and
  `waitIdentityBootstrap()`, parsing each identity's typed `restore` progress.

- Server-side, event-driven completion waits (#468). `mobkit/wait_for_completion`
  resolves when an identity's completion cursor moves past `after` (or, with no
  `after`, counts its first completed turn), returning the typed `outcome`
  (`completed`, `incarnation_changed`, `timed_out`, `untracked`) and the
  cursor. `mobkit/wait_for_turn` is `mobkit/turn_result` taken once the ticket
  settles. The gateway parks the request on the runtime's typed change signals
  (the identity table and completion cursor, and the ticket registry), so
  neither side polls; `timeout_ms` is only the caller's deadline.
  `mobkit/completion_cursor` is a one-shot cursor read. None of them touch the
  member session. A completion wait also ends typed on `run_failed`,
  `broken`, `retiring`, `identity_gone` and `shutting_down` instead of running
  to its deadline, and a turn wait reports `wait: shutting_down`. Waits default
  to a 60 s deadline and clamp `timeout_ms` to 10 min (the SDKs re-issue up to
  their own deadline). `IdentityRuntime::await_completion` returns the typed,
  non-exhaustive `CompletionWait`, `IdentityRuntime::wait_for_turn_outcome`
  returns `TurnWait`, and `IdentityRuntime::wait_for_completion` /
  `wait_for_turn` wait on those signals instead of sleeping between reads.
  The identity health monitor records failed runs
  (`IdentityRuntime::record_turn_failed`). The SDKs raise the typed
  `WaitEndedError` (with `outcome`) for the non-completion outcomes.
- The SDKs' `wait_for_completion` / `waitForCompletion`, `wait_for_turn` /
  `waitForTurn` (and so `send_and_wait` / `sendAndWait`), `wait_for_output(after=...)`
  and `wait_until_ready` / `waitUntilReady` are now one server-side wait each
  and read the member's output once, at completion, instead of polling
  `mobkit/inspect_identity` or `mobkit/turn_result`. They fall back to the
  previous polling only against a gateway that predates the server-side waits.
  New `completion_cursor()` / `completionCursor()` reads, and TypeScript gains
  `waitUntilReady`. The docs name `track_turn` plus `wait_for_turn` as the
  recommended completion barrier.

- Console voice shows the agent's speech while it is still speaking. The
  console voice host installs a provisional caption sink on its public Live
  authority (`with_public_provisional_caption_sink`, meerkat 0.8.46). Each
  caption carries the open segment's whole text so far and its item id, the
  id the committed assistant row later lists in
  `realtime_origin.provider_item_ids`. The browser reads them through the new
  `mobkit/console/voice/captions {identity, request_id, channel_id, after,
  wait_ms}` long poll, scoped to the owned call exactly like
  `context_status` (agent view access; permitted on a read-only console). It
  answers `{identity, request_id, channel_id, cursor, captions}`, where each
  caption is `{kind: "caption", item_id, text}` or `{kind: "retracted",
  item_id}`. The gateway coalesces captions per item in a bounded buffer and
  never makes the provider path wait on the browser. The console upserts the
  live row by item id, drops it on a retraction, and retires it as soon as
  the committed row names that item. Assistant live rows no longer come from
  data-channel `session.output_transcript.*` events, which name no committed
  item. Captions are display state only and are cleared on close and on a
  replacement channel.
- Fork lineage in `callback/build_agent` (meerkat 0.8.45). The build callback
  options carry `fork_source`, meerkat's `SessionBuildOptions::fork_source`
  forwarded as meerkat serializes it (`{"source_member": {"mob_id", "role",
  "member"}, "source_session_id"}`), and `fork_source_identity`, the source's
  durable identity (for example `domain:calendar`). meerkat sets the lineage
  on the build that seats a durable fork (fork_off children, fork_member
  children, local council participants) and on every later rebuild of that
  member, so a host can build a fork child as its source. In a MobKit mob
  `source_member.member` is MobKit's encoded roster id (`mk--...`), which is
  why the identity rides beside it, resolved through MobKit's own roster
  mapping (also public as `member_comms_id::durable_identity_for_roster_member`).
  Both are null for every other build. The child's own labels and session
  still name the child and never carry the source's `agent_identity`.
  Lineage is not caller-mintable: a build response naming `fork_source` is
  ignored. The Python `SessionBuildOptions` gains typed `fork_source`
  (`ForkBuildSource`, `MobMemberBinding`) and `fork_source_identity`; the
  TypeScript one gains `forkSource`, `forkSourceIdentity` and
  `parseForkBuildSource`. Both are receive-only, never sent back, and ignore
  fields a newer gateway adds.
- `MobRuntime::session_commit_pending(session_id) -> Option<bool>` (meerkat
  0.8.45): whether the session holds run input its runtime has taken up but
  not yet committed at a run boundary, forwarding
  `MeerkatMachine::session_has_uncommitted_run_input` within
  `SESSION_COMMIT_PENDING_READ_BOUND` (2 s). `Some(false)` means durability
  is ready and no input is staged, applied or pending consumption (queued
  input that has not started does not count). `None` is inconclusive, never
  a guessed `false`: no runtime machine, an unparsable session id, any
  meerkat error (no runtime holds the session, a replaced driver, degraded
  durability), or a read still waiting when the bound elapses (a boundary
  commit holds the driver until it lands).
- TypeScript `SessionBuildOptions.resumeSessionId`, matching Python's
  `resume_session_id`: the build callback's resume session is decoded, and a
  builder can set it to ask the gateway to resume a session (persistent mode).
- `mobkit/inspect_identity` (and its raw-alias projection) and the console's
  per-member progress (also when it falls back to the runtime machine's run
  state, as for a retiring member) carry `preview_unavailable` when meerkat
  could not observe the member's session for its status
  (`observation_deadline`, `read_failed`, `session_absent`,
  `not_observed_while_retiring`; meerkat 0.8.45 `MemberPreviewUnavailable`).
  When it is set, a missing
  `output_preview` does not mean an empty member. Python
  `IdentityInspection.preview_unavailable` and TypeScript
  `IdentityInspection.previewUnavailable` carry it; unknown future values
  pass through.

- Turn tickets: per-admission completion for identity deliveries.
  `mobkit/send`, `mobkit/interact` and `mobkit/dispatch` accept
  `"track_turn": true` and then return `"turn": {"ticket": ...}`, a ticket
  the runtime mints for that one admission and keeps the admitted turn's own
  completion handle under (the caller's interaction id and a dispatch's
  correlation id still ride unchanged; neither names the ticket, since both
  may repeat across admissions). When the turn cannot be tracked the result
  carries `"turn": null` plus `"turn_unavailable": {"code", "reason",
  "delivered"}`. Every code but `not_delivered` means the delivery still
  happened, exactly once, on the ingress lane: `autonomous_host` (the
  member's live roster entry, the mode meerkat checks, says autonomous_host;
  meerkat reports no per-turn completion for autonomous inbox delivery, the
  default runtime mode), `runtime_refused` (meerkat refused per-turn
  completion for the member's live mode for another reason),
  `externally_bound`, `host_human_input`, `bridge_cannot_report_output` and
  `session_rotated` (a re-dispatch meant to be deduplicated onto a tracked
  admission landed on a different session after a repair or respawn, so it
  ran as its own turn). `not_delivered` (`delivered: false`) means nothing
  was delivered.
  Trackability is decided from the live roster entry right before admission,
  never from MobKit's desired spec, so a same-profile `runtime_mode_override`
  hot reload, a repair respawn that drops the override, or a profile MobKit
  cannot read never turns into a failed send: an `autonomous_host` member is
  refused typed before anything is submitted and delivered once untracked.
  meerkat's own refusal (`MobError::UnsupportedForMode`, now carried typed as
  `BridgeError::UnsupportedForMode` / `BridgeAdmissionError::UnsupportedForMode`
  instead of a mob-error string) takes the same fallback. An idempotent
  re-dispatch of a key whose tracked admission to the identity's current
  session is still pending or completed names the original's ticket (meerkat
  deduplicates per session ledger, so after a session rotation it is a turn of
  its own). The new `mobkit/turn_result {identity, ticket}` reports that turn
  as `pending`, `completed`, `failed` (with `error`) or a typed `unknown`; a
  completed turn carries `output_status`: `text` (its own `output`, from the
  turn's committed run result via meerkat's exact-turn `wait_bounded`,
  bounded to 256 KiB with `output_truncated`), `empty`, `no_own_result` (the
  runtime folded the input into a run already in progress or deduplicated it
  onto an earlier admission that had ended; a deduplicated retry of a still
  running input shares that input's result) or `unavailable`. Rust:
  `IdentityRuntime::send_with_turn_ticket`, `dispatch_with_turn_ticket`,
  `turn_outcome` and `wait_for_turn`, with `TurnTicket`, `TurnOutput`,
  `TurnOutcome`, `TurnTracking`, `TurnUntrackable` and `Ticketed`;
  `SessionBridge` gains `tracks_turn_output` and `begin_delivery_with_output`
  (default: untracked) and `BridgeTurnReceipt` gains `with_output`,
  `wait_with_output`, `wait_turn_output` and `session_resolution_error` (the
  existing `new` / `wait` are unchanged). Python: `send` / `dispatch` /
  `dispatch_text` take `track_turn=`, results carry `turn_ticket` and
  `turn_unavailable` (`TurnUnavailable`, with `delivered`), and handles gain
  `turn_result`, `wait_for_turn` (returns the typed `TurnResult`) and
  `wait_for_output(turn=...)`, with `TurnOutputStatus`, `TurnState`,
  `TurnFailedError`, `TurnUnknownError`, `TurnOutputUnavailableError`,
  `TurnTrackingUnavailableWarning`, `TurnOutputTruncatedWarning` and
  `TurnOutputUnavailableWarning`. TypeScript: `send` / `dispatch` take
  `{ trackTurn }`, results carry `turnTicket` and `turnUnavailable`, plus
  `turnResult`, `waitForTurn` (resolves the typed `TurnResult`),
  `TurnOutputStatus`, `TurnUnavailable`, `TurnFailedError` and
  `TurnUnknownError`, and typed process warnings (`TurnWarningType`).

### Fixed

- The rpc_gateway's SDK callback builder hands a member's external tools to
  the agent by ownership (`SessionAgentBuilder::build_agent_taking_tools`,
  Meerkat 0.8.52). Before, an ops-capable external tool source (one that
  binds to the session's ops registry, such as a detached-job tool bundle)
  stayed shared with the build request. Meerkat 0.8.52 refuses to bind a
  shared one, so that member's build failed with `SharedOwnership`.
  - Both builder branches take the request's tool slot before any copy,
    whether or not an SDK SessionBuilder is configured. The callback path
    composes the SDK's tools over the drained source.
  - `ComposedExternalTools` now forwards `capabilities()` and
    `bind_ops_lifecycle`. An ops-capable dispatcher composed under other
    tools is reported and bound instead of silently skipped. A composition
    or an ops-capable half still held elsewhere refuses with
    `SharedOwnership`, never running unbound.

- Identity repair no longer destroys a member's queued inputs when the runtime
  owner refuses to serve: a refused acquisition defers the repair disposal
  instead of being read as an empty queue.
- A conversation pane following the live edge no longer stops following
  when a tool opened at the end grows the transcript before the browser's
  own one-pixel scroll event arrives. The pane only leaves the live edge on
  an upward move; this fixes an intermittent console acceptance failure
  ("opening the actual tool at live edge remains following").
- Identity members: tools published by `customize_build` now resolve their
  execution plan through the dispatcher that serves them. The identity
  dispatcher used Meerkat's default resolution from its own catalog, so a
  published tool set composed through a dynamic composite never added the
  composite's owner witness, and the composite then refused the forwarded
  plan: the call failed as `tool_unavailable` ("tool execution owner changed
  after plan resolution"). The serving dispatcher's plan is now kept whole
  (its owner witnesses, an argument-chosen mode and the deadline chain),
  validation goes to the same dispatcher, and the identity dispatcher adds
  its own witness: a plan resolved before a later publication, even one
  with identical tool metadata, is refused as `ExecutionOwnerChanged`, and a
  plan for a tool the current publication no longer advertises is refused
  as not found.

- Gating: gate IDs are no longer reissued after a restart. A runtime
  restarted without restored gating state minted IDs from the start again,
  so a staged memory promotion keyed by a pending ID from before the
  restart could be committed, discarded or rekeyed by a decision on an
  unrelated gate that reused the ID; a newly staged promotion whose ID
  collided with an old mapping also lost its own stage while the old
  mapping stayed. Each runtime now mints action, pending and audit IDs as
  `gate-<kind>-v2-<epoch>-<sequence>` under its own random 128-bit epoch, so
  new IDs do not match earlier or restored IDs, barring a random epoch
  collision, and never match legacy IDs. Treat gate IDs as opaque.
  Exhausting the sequence, or a missing epoch when the platform's entropy
  source failed, refuses the evaluation (`safe_draft` with
  `gating_sequence_exhausted` or `gating_identity_unavailable`) or the
  decision (`GatingDecideError::IdsUnavailable`) instead of reusing an ID.
  Over JSON-RPC and the console, that decision refusal is an internal
  error (`-32603`) with `data.error = "gating_ids_unavailable"` and the
  reason, not invalid params. Gating snapshots export as version 2 with
  the owner's epoch; version 1 snapshots still restore, and restored IDs
  are kept as they are. The agent memory steward no longer lets a
  decision under a pre-epoch sequential pending ID (`gate-pending-NNNNNN`,
  including a gate restored from a version 1 snapshot) commit, discard or
  rekey a staged promotion, because such an ID cannot prove which
  promotion it was about; those promotions keep their expiry instead.
- Console: clicking a tool or other control at the bottom of a transcript
  no longer stops the shared conversation pane following the live edge. A
  pointer action can bring its target into view before the press, and the
  browser can then settle the scroll position a pixel above the end the
  console computed (scrollHeight and clientHeight are whole numbers rounded
  from fractional sizes, so the two ends can differ by under a pixel). After
  an earlier click that settle read as the reader moving up, so the opened
  tool was held in place instead of followed. A move of at most that
  rounding distance, landing at the end while following, after a primary
  mouse press on the transcript's content, is now a settle. Any larger
  upward move, and any move after a scrollbar, middle-button, touch or pen
  press, still leaves the live edge.

- Console: a workbench click made right as the console first loads is no
  longer undone. The saved dock layout, or with no saved layout the
  configured initial panel (`layout.initial_control`, `initial_agent`,
  `initial_preset`), was applied a moment after the sidebar appeared, so a
  panel opened in that gap (for example Access) was replaced. Both now land
  in a synchronous follow-up commit before paint, before any input can be
  handled.

- The rpc_gateway's SDK callback builder hands a member's external tools to
  the agent by ownership (`SessionAgentBuilder::build_agent_taking_tools`,
  Meerkat 0.8.52). Before, an ops-capable external tool source (one that
  binds to the session's ops registry, such as a detached-job tool bundle)
  stayed shared with the build request. Meerkat 0.8.52 refuses to bind a
  shared one, so that member's build failed with `SharedOwnership`.
  - Both builder branches take the request's tool slot before any copy,
    whether or not an SDK SessionBuilder is configured. The callback path
    composes the SDK's tools over the drained source.
  - `ComposedExternalTools` now forwards `capabilities()` and
    `bind_ops_lifecycle`. An ops-capable dispatcher composed under other
    tools is reported and bound instead of silently skipped. A composition
    or an ops-capable half still held elsewhere refuses with
    `SharedOwnership`, never running unbound.

- Console: a downward wheel or End at the bottom of a transcript no longer
  stops following the live edge. Such a gesture can leave the scroll position
  a pixel above the computed end (scrollHeight and clientHeight are rounded,
  the scroll range is not), and since the scroll-up fix (#597) that settle
  read as the reader moving up, so a tool expanded afterwards no longer
  followed. The console now remembers the direction of the reader's last
  wheel or key gesture: an upward move of at most a pixel right after a
  downward gesture is a settle, while a larger upward move, or a small one
  after a click or scrollbar press, still leaves the live edge.

- The gateway delivers Event-kind detached job deliveries again. These come
  from `monitors/start` in event-steer or event-queue mode and from
  `jobs/subscribe` with an `event` kind.
  - **The bug:** the delivery path admitted event inputs on the machine its
    session service built for itself, not on the machine that serves the
    sessions. Admission failed ("Runtime not ready: destroyed"). Delivery
    rows queue under the session that started the job, so that failed row
    blocked every later delivery for jobs of the same origin session,
    notifications included, retried without a warning every second.
  - **The fix:** the delivery path now admits on the serving machine, which
    the gateway passes explicitly. Both gateway compositions also bind
    their persistent session service's runtime machine to it.
  - **A blocked delivery queue is now visible.** It is logged once at WARN
    with the origin session, the row and the reason, reported in the job
    health projection under `detached_jobs.blocked_deliveries`, and it
    degrades that projection's status.
  - **Not affected:** host-level notification and record deliveries to
    sessions without an event row ahead of them. `fork_off` and `council`
    were not affected either.

- The gateway's agent-memory recorder (`memory`) reaches members whenever
  memory is enabled, not only when the host also supplies an
  `AgentCustomizer`. With memory enabled and no host customizer, members
  had `memory_search` but no recorder. There were two causes:
  - A member materialized outside the roster bootstrap (lazily on first
    dispatch, or repaired at delivery) ran no customizer, because the
    gateway never installed its composed customizer (the host's and/or the
    agent-memory one) as the identity runtime's own. It now does, as the
    library builder already did.
  - A member restored natively at start (persistent mob storage), whose
    later customized resume attaches the restored occupant, only receives
    customizer tools through the per-identity customizer tool registry.
    The gateway created that registry only for a host customizer. It now
    creates it whenever a customizer is composed.
  - Custody, grants and taint are unchanged. A profile with memory
    disabled still never carries the recorder.

- The console's Approvals inbox shows the gating owner's recorded audit
  outcomes as recorded (#594, shipped in 0.8.46).
  - **Audit rows:** each shows its tier (`r0` to `r3`, or "Unknown" with the
    recorded value), actor, event, decision, approver and outcome, with
    "Not recorded" where the record has no value. The view states that an
    approval record does not confirm execution.
  - **The Auto tab and count** now cover exactly the `evaluated` events whose
    outcome is `allowed` or `allowed_with_audit`. Before, the tab guessed
    from loosely matched `decision` and `event_type` strings and invented
    risk labels.
  - The console core package (`packages/console-core`) exports
    `normalizeGatingAuditEntry` and `GatingAuditEntryView`, a display
    projection of one audit record (not execution state).
  - The console guide documents the Approvals tabs and audit records
    (#603).

- The console RPC handler no longer reserves a 1.17 MB stack frame. At
  opt-level 0 LLVM gives every local of every branch its own stack slot, so
  the handler's poll frame reserved every method arm's future at once, while
  only one arm runs. On the console send path that left under 0.5 MB of a
  2 MiB worker stack for everything beneath it.
  - Each awaiting arm is now its own boxed future, built in
    `box_in_own_frame`'s frame.
  - Measured on the `api-routine-tool-owner` console scenario (debug,
    default worker stack):
    - the handler frame drops from 1,168,696 to 30,336 bytes, and the send
      arm adds a 9,872-byte frame of its own;
    - the stack in use where the path reaches its deepest native frame
      drops from 1,983,704 to 809,256 bytes;
    - the handler's future drops from 52,248 to 6,256 bytes, and a test
      fails it above 16 KiB.

- `UnifiedRuntimeBuilder::build()` now returns a `Send` future, so a host
  can `tokio::spawn(builder.build())` or drive it under a harness that
  requires `Send`. Two causes, both present since at least 0.8.44:
  - The builder kept its `PreSpawnHook` (`Send`, not `Sync`) as a plain
    field while `build` held `&self` across awaits, which made the builder
    `!Sync` (E0277). The hook now sits behind an uncontended mutex. The
    public `PreSpawnHook` type is unchanged.
  - The persisted-authority prewarm that `build` awaits mapped borrowed
    session ids through an async closure, which the compiler cannot prove
    `Send` for every lifetime ("implementation of `Send` is not general
    enough"). It now maps to owned runtime ids first.
  - A compile-time test pins both: the builder is `Send + Sync`, `build()`'s
    future is `Send`, and a builder carrying a pre-spawn hook can be spawned.

- The gateway now builds one runtime delivery inbox and shares it between
  the shell job projector and the detached-job delivery runtime. Before,
  each built its own `RuntimeDeliveryInbox` over the same runtime store. An
  inbox's commit signal is per instance (clones share it), so a delivery
  committed through one path never woke subscribers of the other. Both
  instances still read the same rows, so backlog counts agreed and hid the
  split. Waking on commits, rather than on the gateway's timer, depends on
  this.

- The published `meerkat-mobkit` crate package now includes the license texts
  its manifest declares (`LICENSE-MIT`, `LICENSE-APACHE`). Cargo packages only
  files under the crate directory, so earlier releases published the crate
  without them. The crate directory now carries symlinks to the root files,
  which `cargo package` follows, and `scripts/verify-crate-package-files.py`
  fails CI when a published crate's `cargo package --list` lacks either one.
- Removed an inaccurate third-party attribution from the console scroll
  geometry, its notices files and the embedded console bundles; the code is
  original.
- Console: scrolling up from the bottom of a transcript no longer sticks
  before it scrolls (#597). The first 32 px of an upward scroll stayed in
  the live-edge band and kept following the end, and a scroll within 1 px of
  the console's last write was taken for its own, so a streamed reply's next
  layout pass snapped the reader back to the end. Any upward movement the
  console did not write (wheel, scrollbar, touch, or a gesture chained out
  of a code block) now leaves the live edge at once; following resumes on
  the way down, within the live-edge band or at the end.
- MobKit's session-service decorators forward
  `SessionServiceHistoryExt::read_instruction_activation_records`. Without the
  forward, the defaulted method answered `Unsupported` through MobKit.
  `verify-session-service-decorators` now also checks the base
  `SessionService` and `SessionServiceHistoryExt` traits, so a decorator
  missing a defaulted base method fails the gate.
- Python and TypeScript SDK callbacks retain their originating gateway process
  across reconnect. Late results, errors and deadline/cancellation responses
  cannot answer reused callback IDs on a replacement process, and retired
  readers cannot fail its pending requests.
- A voice channel reopened through MobKit while delegated work from an
  earlier channel was still running now opens with Meerkat's "work started
  before this call is still finishing" startup line (meerkat #1652), so the
  model does not claim that work is done before its result arrives. MobKit's
  live composition now binds its delegation coordinator as the open
  authority's post-close work source, as Meerkat's RPC router does; before,
  no MobKit door bound one.
- A declared role migration is applied on the boot path, or refused typed;
  it is never silently dropped (children that declared `role_migrations`
  to a restricted profile kept their old role and its tools, such as
  `spawn_member` and `delegate`, after a restart).
  - The mob's explicit resume restores every member under its durable
    role before the session bridge runs, and Meerkat lets neither that
    restore nor its spawn customizer change the profile. The migration
    rode only on the bridge's resume Spawn, which collided with the
    restored member, and the collision handling adopted the occupant as
    it was.
  - When a declared migration meets an occupant on another role and
    MobKit has repair custody, the bridge applies it: it retires the
    restored member to absence (after proving the resume source exists,
    carrying queued input) and resumes the same session under the
    declared role with `resume_from_role`, which Meerkat re-verifies
    against the durable predecessor role.
  - Where a Present identity intent owns the member's materialization,
    retiring it would fight MobMachine, so the resume is refused with
    `ResumeRejectionKind::RoleMigrationNotApplied` and the identity is
    degraded with a detail naming the migration. The `mobkit/init` result
    shows it: `identity_bootstrap` reports the identity `broken` with
    restore kind `role_migration_not_applied` and `ready: false`, so a
    host never deploys believing the migration landed. Meerkat's own typed
    refusals (`MemberRoleMigrationRequired`, `MemberRoleMigrationRejected`)
    now classify as the same kind.
  - An occupant whose role is not the declared predecessor is refused
    typed before any retire: the declaration does not describe it, and
    Meerkat would refuse the restamp after the member was already gone.
  - Any attach under a declaration checks that the member runs the
    declared role, and refuses typed when it does not.
- A managed peer edge the topology no longer wants is unwired after a
  restart (children's agents kept reaching their parents' agents after the
  topology provider stopped declaring those edges).
  - The managed-edge set was process memory, empty at boot, while the mob's
    wiring is durable and replays. A dropped edge was neither managed nor
    desired, so reconcile never unwired it, and agents cannot unwire it
    either (external-managed custody denies `unwire_members`).
  - Under a topology provider, the identities' external-managed custody
    makes the topology the edges' desired-state owner. Reconcile now prunes
    every live edge between two identities that no declared owner wants:
    not the provider plus operator intent, and not the mob definition's
    wiring over the identity roster, which keeps its own edges so the two
    never fight.
- Console: a member-kickoff notice no longer shows Meerkat's peer transport
  projection. Meerkat 0.8.50 sends kickoff status (`mob.kickoff_*`) as peer
  requests whose content is model-facing routing text (the peer spec with its
  public key, and send_response coaching). Only `mob.kickoff_started` was
  special-cased: the shared console-core adapter, in the markdown text mode
  the shared host uses, showed that text verbatim for `mob.kickoff_failed`
  and `mob.kickoff_cancelled`, and in legacy mode even for
  `mob.kickoff_started`. The stock console in markdown mode did the same.
  Every kickoff request, and every typed `lifecycle` notice (meerkat #1608),
  now shows its typed summary (`Peer request: mob.kickoff_failed`); an
  ordinary peer request still shows its authored content.
- The console aggregator no longer re-reads a large session's whole history
  twice after registration (#570). Registration's recovery pass could land
  while a member was idle between runs of a queued burst and read the
  complete document (4.4 s here) as its later inputs and their commits kept
  landing. The epoch it recorded was then stale, and a discovery tick re-read
  the whole document (5.4 s) once the fresh watermark expired: in
  `idle_cpu_gate`, about half the time inside its measured idle window.
  - The refresh gate now reads `Draining`, not settled, while a member that
    is idle on the session still has active inputs or a pending commit. That
    pass skips its read, and the refresh is re-driven by the transition that
    drains the session: the waiter wakes on the session's durable writes (a
    new change channel on the write-epoch witness, not a timer) and fires
    once no commit is pending, no input is active, and every input it saw
    active has a finalized receipt. A refresh that lands while a run is open
    on the session still restores positive frames and arms the same
    re-drive, so the settled refresh follows the run's end, not a tick.
  - No exit path leaves that waiter armed: it also wakes on the mob's
    lifecycle changes and refreshes at once, from what is durable, when the
    member is no longer bound to the session and live in a running mob (a
    stop, retire or destroy with inputs still active), when the runtime no
    longer holds or can progress the session, or when either change source
    closes. A restarted gateway re-runs registration's recovery pass.
  - A completed pass whose session write epoch moved during its read
    re-reads once at once, instead of waiting for a tick and watermark
    expiry. It runs only because the epoch moved, so it cannot loop.
  - `MobKitConsoleAggregator::history_backfill_converged` resolves once no
    session-history backfill is running, queued or armed to re-run.
    `idle_cpu_gate` waits on it instead of a 2 s CPU probe that could not see
    a pass scheduled after it. The large session is now read once (one full
    read, converged after about 7 s), and the measured idle window stays
    near 15 ms. Verifying only the new suffix of a history is #573.
- A restored identity member whose customizer tools could not be published
  before the mob build (an early `customize_build` failure, #563) no longer
  starts a run without them. The build asks meerkat to hold its run starts
  (`MobBuilder::hold_restored_member_run_starts`, reason
  `HostRunStartHoldReason::ToolsNotPublished`), so a queued input, a kickoff or
  a peer message admitted when meerkat's own resume revives it waits. The
  materialization that publishes its tools releases the hold
  (`MobHandle::release_member_run_starts`). A release that fails keeps the
  identity in `IdentityStatus::customizer_tools_pending` with the error.
- Durable-behind admission skips transcript commits that meerkat 0.8.51's
  history retention has retired. Their parent projection now returns
  `TranscriptRevisionRetired` (a retired commit has no body to prove
  against); admission treated it as a store write failure, so a session
  compacted past its retention cut could no longer be persisted
  (`identity_first_repeated_compaction`). Other projection errors still
  fail the write.
- `npm run embedded:freshness` fails when a generated console bundle contains
  a module path outside the repository or an absolute local path. A
  worktree whose `node_modules` is a symlink into another checkout bundled
  paths such as `../../<other-worktree>/console/node_modules/...` into
  `console/dist/index.cjs`, which passed the freshness check there and
  reached main once (regenerated in 4e337578).
- The stdio gateway no longer queues ordinary requests behind `mobkit/init`
  (#550). Before, a request that arrived during startup waited in a 64-slot
  queue that nothing drained until init finished. A startup provider callback
  that awaited such a request waited on itself until its 130 s deadline, and
  a full queue stalled the stdin reader, so callback responses stopped being
  routed. An ordinary request sent before init settles now gets an immediate
  typed refusal: `-32018`, `data.kind: "init_in_progress"`, exported as
  `INIT_IN_PROGRESS_CODE` and raised as `InitInProgressError` in both SDKs.
  `mobkit/shutdown` and callback responses are still admitted. Startup
  callbacks must not issue ordinary RPCs on the runtime that is starting.
- Python SDK: a callback response is no longer written behind host requests
  queued for the gateway's stdin. Writes stay whole lines, and a callback
  response now waits only for the line currently being written (#550).
- SDK transports (Python and TypeScript) no longer leave a request or a
  gateway callback waiting out its deadline because one stdout or stdin line
  was bad (#550):
  - The Python reader skips a line that is not UTF-8 or not a JSON object.
    Before, such a line ended the reader thread, so every waiter hung to its
    timeout and every gateway callback to its 130 s deadline. The TypeScript
    reader skips a line that is not a JSON object instead of throwing from its
    listener.
  - Any reader exit now fails every waiter with the typed
    `TransportReaderFailedError` (a `TransportError` with a `reason`), and later
    requests to the same gateway process fail at once. Before, a gateway that
    closed its stdout answered waiters with an untyped `-32099 "subprocess
    died"` RPC error. A response that arrived before the exit (such as a
    fail-closed `mobkit/init` refusal) still reaches its waiter.
  - A callback result containing NaN or Infinity is answered with a typed
    error (`data.kind: "non_finite_result"`). Before, Python wrote a `NaN`
    token the gateway could not parse, so the callback waited out its deadline,
    and TypeScript silently sent `null`. The Python transport writes strict
    JSON only (`allow_nan=False`).
  - A callback request that arrives with no registered handler is answered at
    once with a typed error (`data.kind: "callback_handler_unavailable"`)
    instead of no reply.
  - The gateway logs a stdin line that is not valid JSON (its length and any
    callback id, never its content). When it is a reply to a pending callback,
    that callback now fails at once with "malformed callback response" instead
    of waiting out its 130 s deadline.
- Module subprocesses are now reaped on every path that stops owning them.
  - A module that closes stdout without exiting no longer blocks module start
    until it exits; the start fails with `EmptyOutput` and the module is
    killed and reaped.
  - A bootstrap that fails after starting modules (for example on a
    malformed memory state) now kills and reaps those modules instead of
    leaking them.
  - A cleanup failure after a failed start, an aborted respawn's replacement
    and dropping a `MobkitRuntimeHandle` without `shutdown()` all kill and
    reap the process when its owner is released.
  - `RuntimeShutdownReport::orphan_processes` still counts children whose
    synchronous termination failed; those children are now also reaped as
    the shutdown releases them.
  - On Linux and macOS the first event line is read on the calling thread
    within the start timeout and the module's stdout is closed afterwards.
    No reader thread is left blocked on a pipe that a descendant still
    holds; such a descendant now gets EPIPE.
  - Behavior change: dropping the runtime handle now terminates its live
    module processes; previously they kept running.
- Identity members no longer lose their `customize_build` tools on a
  restart, an adoption, a respawn or a delivery-time repair (#563). Before,
  those tools reached a member only as the per-spawn overlay of the one
  spawn that ran the customizer, and meerkat never persists that overlay.
  The failure modes were:
  - a restart restore rebuilt the member without the tools;
  - when the next materialization's spawn hit "member already exists", every
    adopt branch discarded the new tools;
  - delivery-time repair respawned with role and labels only.

  The host's handlers stayed registered, but the live agent stopped
  advertising the tools (`mobkit/identity/resolved_tools`). Now each identity
  member carries one stable, dynamic dispatcher for its whole life:
  - Every successful `customize_build` publishes into it, swapping the tool
    list and its handler scope together. A tool from an earlier build is
    refused typed, never routed to a stale scope.
  - A new `CustomizerToolsSpawnCustomizer` attaches it to every meerkat-side
    build of a registered identity. MobKit installs it whenever an agent
    customizer exists, and composes it with the memory customizer in meerkat's
    single slot.
  - An adopted occupant picks up the current tools without a respawn.
  - On a restart, `customize_build` runs for every roster identity before the
    mob is restored (before activation, or before the mob build when meerkat
    restores members inside it). Restored members therefore start with their
    tools, `mobkit/identity/resolved_tools` lists them before the first turn,
    and runs that start at activation see them. The materialization still runs
    `customize_build` and republishes. An early failure is logged per identity
    and reported as the new optional
    `IdentityStatus::customizer_tools_pending` (`{reason}`) until the
    materialization publishes.
  - Members that are not roster identities (helpers, forks, flow-provisioned
    and raw-spawned members under their own ids) get no customizer tools.
  - New public types: `identity_first::{CustomizerToolRegistry,
    IdentityCustomizerTools, CustomizerToolsSpawnCustomizer,
    ComposedSpawnMemberCustomizer}`,
    `IdentityRuntime::set_customizer_tool_registry`, and
    `MobBootstrapSpec::with_spawn_member_customizer`, which composes instead of
    replacing.
- An identity member that had not run a turn before a shutdown is
  materialized again at the next boot. The local continuity store creates its
  head-canonical tables only at the first delta write, but its per-session
  record lookup always read them. On such a file the lookup failed, MobKit
  skipped the member's owner pre-registration, and the member stayed
  unmaterialized with no live session (`mobkit/identity/resolved_tools`
  answered "session not found").

- Console: queued rows that did not go through now say what happened in
  plain words. A refused row reads "Not sent: this message never reached
  <agent>." with Send again and Discard; a row whose answer was lost reads
  "We couldn't confirm <agent> got this." with Check, Send again and Discard.
  Transport failures read "Couldn't reach <host> (offline or signed out)",
  where <host> is `console_config.brand.label` (else "the server"). Rows
  saved by 0.8.43 and earlier, which carry only a raw error string, are
  reworded the same way on load.

- Console: Check always ends in a visible result. The row shows "Checking..."
  at once, then "Delivered at <time>" (the row clears and a dismissible
  notice stays), "Not found in <agent>'s recent messages." (the row keeps its
  options and its saved state), or a plain "Couldn't check: ..." error. Before,
  some paths (an unresolved owner, a lock or scope miss) ended with no change
  on screen.

- Console: an uncertain row offers Send again only when the gateway
  advertises `send_dedupe.durable: true`. Send again resends the saved
  message with its original idempotency key, so the gateway returns the
  original acceptance if the first send did arrive instead of delivering it
  twice. Send dedupe records have no expiry or cap, but a gateway without a
  storage layout keeps them in memory, where a restart between the two
  sends could admit the message twice; such gateways (and older ones that
  omit the field) get Check and Discard only. Refused rows keep Send again
  either way, since nothing was admitted.

- Console: typing while a reply streams after a send no longer measures the
  whole transcript on every streamed token. After a send the conversation
  holds the submitted turn in place, and in that mode every content commit
  read every mounted row's rect (about 1,200 element rect reads per token
  with 200 turns mounted), forcing layout over the transcript while the
  operator typed the next message. Holding an anchor now measures only the
  anchor row (and its recorded neighbors); every row is read only when the
  anchor is gone. Against the real gateway in a production-style nested iframe with
  300 turns and 4x CPU throttling, main-thread time per keystroke while the
  reply streams drops from 29-31 ms to 13-15 ms (p95 52-58 ms to 36-37 ms).
  `npm run perf:typing:browser` adds a send-then-type scenario and fails on
  more than 40 element rect reads per streamed token (13 now, 1,207 before).

- Console: after a gateway restart, a streamed reply no longer re-derives the
  whole transcript on every token. Agent console events carry the member
  stream's `source_sequence`, which Meerkat numbers from the start again in
  each process. The transcript projection ordered sequences across the whole
  session, so the restarted sequences (1, 2, 3, ... under a stored history
  numbered into the thousands) could never be proven to append, the
  incremental derivation fell back to a full derivation per token
  (about 12 ms per token at 300 turns), and live frames from before and after
  the restart could be interleaved. The gateway now stamps each agent event
  with `source_epoch` (this process and the member's generation and fence),
  and the projection orders sequences only within one epoch; across epochs
  arrival order decides. Frames without an epoch keep the session-wide
  ordering. `npm run perf:typing:browser` adds a restarted-session run that
  fails on full derivations per streamed token (0.00 now, 0.88 before).

- Console: a streamed token no longer forces a synchronous layout of the
  transcript. Two reads ran with layout dirty on every content commit: the
  scroll controller measured its anchor row in the commit's layout effect,
  and the chat pane re-read the transcript's scroll height to decide whether
  the turn rail is needed. Both now read geometry in `ResizeObserver`
  callbacks, which run after the frame's layout and before it paints, so a
  steady session (following the live edge or holding an anchor) still
  corrects its position in the same frame. Submissions, reveals, restores and
  row insertion keep their immediate pass. This also stops a streamed commit
  from pinning a reader back to the live edge after a scroll whose event has
  not arrived yet (the console e2e chat-pane-older-history-demand-paging
  flake). Scroll writes also keep snapping and browser scroll anchoring off:
  returning to a conversation restored its reading position while the
  pane's turn snapping was briefly re-enabled, and the browser snapped the
  restored row 62 px away. A change outside the rows, such as the
  older-history control appearing above them, still schedules a pass: it
  moves rows without resizing any, and the shift was left for the next
  viewport resize to correct. A send now empties the composer in the
  keystroke's own render, as intended; the composer applied the cleared
  value in an effect, which painted the submitted text again and cleared it
  20-35 ms later. A native scroll that lands at the
  live edge no longer measures every mounted row to capture an anchor that
  following discards. `npm run perf:typing:browser` now fails on more than
  0.02 script-forced layouts per streamed token (0.00 now, 0.96 before; needs
  `--trace`, and `--trace-invalidations` names the source) and lowers the
  rect-read limit to 12 per token (about 9 now, 13 before).
- Console: a streaming reply no longer re-parses its whole Markdown source on
  every token. While a reply streams, its document renders as closed blocks,
  each parsed once, plus the open tail. A boundary is a blank line outside a
  fenced code or HTML block, followed by an unindented line that does not
  continue a list, so the blocks before it cannot change. Only the tail is
  parsed again as text arrives (about 160 characters per token in the typing
  harness, where the whole reply was parsed before), and closing a block
  changes no rendered node. A completed reply keeps that rendering, so its
  nodes and any selection survive completion; tests assert it is
  DOM-identical to a whole parse. A source with link-reference or footnote
  definitions, which resolve across blocks, renders as a whole parse on
  completion. `npm run perf:typing:browser` fails on more than 500 Markdown
  source characters parsed per streamed token.
- Console: a streamed token no longer presents the whole transcript again.
  The continued (incremental) transcript derivation still rendered every
  entry on each token: run durations, the visibility filter, per-entry
  render keys and assistant presentation all ran over the full history.
  Rendering now resumes from the presentation state after the committed
  entries, so a token that only grows the open reply presents just that
  reply; per-entry run-duration eligibility is cached, and interning skips
  the unchanged prefix. Derivation time per token in the typing harness
  drops from about 1.5 ms to about 0.2 ms at 300 turns, and the equivalence
  oracle and seeded fuzz still prove it equal to a full derivation.
  `npm run perf:typing:browser` fails on more than 4 transcript entries
  presented per streamed token (about 2 now, one per derivation).
- Console: two per-render scans of the whole history no longer run on every
  streamed token. Day separators formatted their full date with locale date
  formatting each time the streaming turn rendered; a day key's label is now
  formatted once. The active run (for Stop) was found by replaying every
  frame of the identity log; it is now read back from the end, stopping at
  the latest `run_started` that names its run, which gives the same answer
  (a seeded test compares it with the replay) at the cost of the current
  run instead of the whole log. `npm run perf:typing:browser` reports
  active-run frames read per token and fails on more than 0.05 day-label
  formats per streamed token.
- Console: streamed text renders on every third animation frame instead of
  every frame, as a React transition. A frame that only adds streamed text
  waits for the third animation frame (counted in frames, so the pace slows
  with the frame rate instead of fighting it) and commits as a transition,
  which a keystroke always preempts. Any other frame still renders on the
  next animation frame, and a stream's completion (`text_complete`, a run or
  interaction terminal, or a terminal turn) renders at once. In the typing
  harness the console renders about 19 times per second while a reply
  streams instead of about once per token, and main-thread time per streamed
  token falls by about a third. `npm run perf:typing:browser` fails on more
  than 25 console renders per second while streaming.
- Console: browser find-in-page and keyboard and screen-reader navigation
  still reach the windowed transcript. Measured turns within 20 turns of the
  mounted window stay in the DOM, parked as `hidden="until-found"` at their
  measured size: the browser lays out and paints nothing inside them, but
  find-in-page finds their text and reveals the turn, and the window keeps a
  revealed turn mounted. The transcript is now a WAI-ARIA feed (`role=feed`,
  `aria-busy` while history loads) of turn articles with `aria-posinset` and
  `aria-setsize`; PageDown and PageUp on a turn move focus to the next or
  previous turn, mounting it (and revealing earlier turns at the top), and
  Control+End moves to the composer. The parked band adds no measurable
  CPU per key at the live edge. The browser equivalence run checks that
  parked turns carry their full text, that the path find takes on a match
  keeps the geometry, and that keyboard focus reaches every loaded turn in
  order.
- Console: the transcript mounts only the turns near the viewport. Every
  revealed turn used to stay mounted, so an operator who scrolled back kept
  the whole loaded history in the DOM (about 16,000 elements at 300 turns),
  and every keystroke and streamed token paid a frame lifecycle over all of
  it. Turns farther than 1.5 viewports from the viewport are replaced by a
  spacer whose height is the sum of their measured heights and the gaps
  between them, so scroll height, scroll position and everything visible
  are unchanged. Heights are measured (ResizeObserver, after layout) and
  never estimated: a turn not measured at the current width with its current
  content stays mounted. The newest turn, a selection's turns and the turn
  holding keyboard focus stay mounted, and a rail jump or a restored anchor
  mounts its turn. A turn holding a pending approval stays mounted wherever
  the reader is: the request blocks the agent, so windowing must never hide
  its card. In the typing harness the transcript mounts about 1,100
  elements after a full fill (16,000 before), and CPU per key falls to about
  1.6-2.5 ms idle and 2.5-4 ms while a reply streams. `npm run
  perf:typing:browser` fails above 3,000 mounted elements and adds an
  equivalence run: the same history windowed and unwindowed
  (`windowed=false`) must match in scroll height, visible rows and their
  offsets, turn rail and pixels (exactly: the compared pages render local
  fonts, without LCD text or subpixel glyph positions) at 17 scroll
  positions including after a resize, and a selection, a focused control, an opened tool call and a rail
  jump must survive.
- Console: a transcript row keeps what the reader opened if it unmounts and
  mounts again. Disclosure state lived in the DOM or in component state, so
  a remounted row reset it: event payloads, thinking blocks, tool calls,
  approval details and flow-run cards. Rows now name a scope by their stable
  row id (`ConversationRowStateScope`), and their parts keep that state in
  the pane's presentation store through `useRowState` and `RowDetails`.
  The first render records its initial value, so a thinking block that was
  open while it streamed stays open. The store is per pane, bounded at 2,000
  entries and cleared with the pane's authority, like completed-tool
  disclosures. Nothing changes for a row that stays mounted; this prepares
  the windowed transcript (#544). A flow-run card still applies its status
  default when the run's status changes, but no longer resets the reader's
  choice when it mounts again.

- Console and example acceptance fixtures no longer race other processes for
  their port. Harnesses reserved a free port, closed it and handed the number
  to the fixture, which bound it later; anything on the host could take it in
  between (CI saw `AddrInUse`, OS error 98, at console fixture startup).
  Fixtures and the flow editor now bind the port-0 address the harness passes
  and print `MOBKIT_FIXTURE_READY {"addr":"..."}` with the address they
  actually bound, which the harnesses (`console/fixture-ready.cjs` and the
  example scripts) wait for. The flow editor smokes no longer use fixed
  ports, and the Python reference flow hands uvicorn an already-listening
  socket. A source guard fails if a harness reintroduces a reserve-then-bind
  helper.

- Console: typing in the chat composer no longer lags with a long history,
  idle or while a reply streams. In Chromium against the real gateway with
  300 turns mounted (nested iframe, as the operator deployment embeds it), keystroke-to-next-paint
  p95 drops from 18-20 ms to 4-5 ms; with a full 5000-event identity log and
  a reply streaming it drops from 97-269 ms to 20-25 ms. Each keystroke
  re-laid out every mounted transcript row: the textarea was a flex item,
  which Blink never treats as a relayout boundary, and the pane root was a
  size container, which re-lays out its whole subtree on every pass. The
  composer input now sits in a fixed-size, strictly contained block and the
  compact-header query container wraps only the header. While a reply
  streamed, every token re-derived the whole identity log (about 30 ms at
  5000 events), re-parsed every history snapshot's growing id list, rebuilt
  every row and re-scanned the log for the panel phase, and the scroll
  controller and turn rail measured every mounted row. Transcript derivation
  now continues incrementally for appended live text (proven equal to a full
  derivation over every adapter suite and a fuzz corpus); unchanged entries,
  rows and turns keep their identity so only the streaming turn re-renders;
  the panel phase extends incrementally; and geometry is read only where it
  is used. Periodic experience refreshes no longer replace unchanged agent
  records, which re-derived every docked transcript each time.
  `npm run perf:typing:browser` drives Chromium in CI and fails on
  runner-independent work counts (layout objects per idle keystroke, full
  derivations and turn renders per streamed token), reporting wall-clock
  p95 as advisory; the jsdom benchmark could not see layout or paint.

- Console: a failed send's typed `message_delivery_failed` frame is now
  committed before its input frame turns `delivery_failed`, so any client
  that observes the status (a stream subscriber or a timeline query) can
  already read why. The status used to commit first, which also made
  `console_send_selected_skills_member_lane_admission_refusal_surfaces_typed`
  intermittently miss the failure frame.

- Console: panel data refreshes no longer pile up on a slow server. Every
  tool or lifecycle event from any agent refreshed the roster and every
  docked panel kind, including panels in hidden tabs, with no limit on
  requests in flight. With a topology panel docked and a 60 s
  `mobkit/topology/query`, the browser queued 28 topology queries a minute
  and every other console request waited behind them at the per-origin
  connection limit (p95 53 s). Each panel kind and the event-driven roster
  refresh now keep at most one request in flight and coalesce later events
  into one trailing refresh, and only panels in the active tab refresh
  (hidden tabs refresh when shown). Other requests stay at p95 45-55 ms.

- Console: returning to a conversation restores the remembered reading
  position even when the host's history arrives after the next frame. A
  host's asynchronous `revealAnchor(true)` was followed by one re-check on
  the next animation frame; when the host's rows committed later than that,
  the controller declared the position unavailable, replaced the anchor and
  left the view at the top of the transcript ("Earlier position is
  unavailable"). A confirmed reveal now restores when the row actually
  mounts and reports the position unavailable only if a newer content commit
  still lacks it, or on the existing timeout.

- `*_and_wait` / `*AndWait` custody follow-up. Python: an SDK warning raised
  after admission (the untracked-wait, truncated-output or no-own-output
  warning) that a caller escalated with `filterwarnings("error")` now carries
  `admission` and `ticket` like every other post-admission failure; warning
  behaviour is unchanged and nothing is replayed. TypeScript: a tracked
  `WaitEndedError` now carries the top-level `ticket` as well as `admission`.
  The docs name the one exception: a thrown value that cannot carry fields.
- `mobkit/topology/query` no longer serializes behind the topology mutation
  lock. In a production deployment (MobKit 0.8.43) queries arriving about once a second
  with a roughly 0.8 s edge discovery queued behind each other, with p50 67 s
  and p95 119 s, almost all of it waiting before the provider ran: every read
  took the exclusive mutation lock and ran reconcile and recovery under it. A
  read now takes no mutation lock. It reports the committed intent under one
  state read: while a mutation's write-ahead journal is pending, the journal's
  pre-operation view, so a read sees the state before or after an apply and
  never a half-applied one. Concurrent reads share one snapshot computation:
  a caller arriving while one runs joins the single fresh computation that
  follows it, so N concurrent queries cost at most two edge discoveries and a
  result is never older than its call. Nothing is cached by time.
  Reconcile and recovery run only when an interrupted journal or a
  non-terminal audit record exists and no plan, apply or recovery holds the
  mutation lock. A read no longer delays an apply. The same-process bilateral
  coordinator query still takes its coordinator and runtime locks.

- Console access uses the server's action catalog and current administrator
  state. Stale previews and mutation responses cannot replace another account's
  state; failed saves retain the draft, and typed refusal details stay private
  in both current and reconstructed conversation history.
- Test hygiene with no product change:
  - the identity background lease renewal test waits for the renewal's own
    `LeaseUpdated` event instead of a 45 ms wall-clock window (#566);
  - the console stock-presentation scenario checks the table, link and image
    in the reply to its accepted input, not the first table in the
    transcript, which the windowed transcript can park out of view (#583);
  - the console startup-deadline test expires its deadline once the HTTP
    read has started, instead of after a fixed 150 ms budget (#586).

- On Linux and macOS, one-shot process boundaries apply the caller timeout
  to both stdout and child exit, drain excess stdout after the first line,
  and join the non-reaping exit observer before returning. A timeout closes
  the reader and kills/reaps the direct child; OS cleanup can outlast the
  deadline. Windows and other targets retain their narrower legacy behavior.

- MobKit's two `MobSessionService` wrappers (`delegate_mob_session_service!`
  and `AfterCreateMobSessionService`) forward meerkat 0.8.49's
  `append_system_notice_under_runtime_turn_boundary`. The trait default
  refuses, so interrupted-run notices stayed owed behind the wrappers
  instead of being recorded in the transcript without a model turn.
- The bundled adaptive layer-decision schema is regenerated from meerkat
  0.8.49's canonical `layer_decision_schema()`, which adds the
  `between_tools` thinking mode.
- The voice readiness body-read test gets a positive control that meerkat
  0.8.49's verified-body cache cannot answer: a direct read of the member's
  committed body through the counting store. The open path's full validation
  may now be served from that cache, so it no longer proved the counter
  works.
- The identity health monitor no longer misses the completion of a turn
  that ran before it attached, so `wait_for_completion` on that turn's
  baseline no longer times out (the operator-method seed-turn timeouts). The
  monitor attaches a member only once its identity lease exists, and it used
  to learn about the lease only from a later mob machine change or its 30 s
  safety tick. It is now woken, typed, when any identity lease lands or
  moves (`IdentityRuntime::install_lease_observer`). Its subscription
  replays the actor's retained events from the first (meerkat #1236), so a
  turn completed between the lease landing and the attach is counted exactly
  once, in order. A completion that already happened is credited before the
  next completion-cursor read, replayed from meerkat's retained session
  events, so a baseline taken after it can never be satisfied by it: the
  cursor under-counts at worst, never over-counts. A per-session completion
  ledger, with a high-water per sequence space, lets the monitor's stream and
  a cursor read's drain each credit an event at most once between them, even
  when a stream of an older space re-attaches. An identity lease rotation cuts the
  monitor's previous ordinary subscription off instead of counting its
  completions a second time.

- `mobkit/force_cancel_member` on the unified RPC keyed an identity-resolved
  member by the undecoded `rt:` runtime alias, so an alias call missed the
  durable identity's roster row. It now resolves the row like the console
  path (`roster_member_id_for_supplied_id`), and so does
  `mobkit/stop_member_run`.
- Each turn's WholeBlob projection into the continuity store now costs
  O(delta) instead of O(document), in two ways.
  - The committed document is no longer read back. `SessionStoreBackedRuntimeStore`
    keeps the typed session a prepared boundary commits
    (`PreparedRuntimeSessionCommit::committing_whole_blob_session`, meerkat
    #1280) and binds it to the authority its commit returned. It projects
    that session through `reuse_or_load_committed_whole_blob_snapshot`. The
    ordinary turn therefore pays no full decode and no rewrite-graph
    validation of the document it just wrote. A head that moved before the
    projection, or a boundary without a typed WholeBlob document, still takes
    the authoritative read.
  - The durable body is no longer loaded to prove an append. After any
    compaction, every append boundary used to materialize the whole durable
    transcript just to learn that only the new turn was missing. The compact
    durable head row (revision, rewrite generation, message count) now proves
    that directly. Any other shape still reads the durable body and runs the
    rewrite-chain provers as before.
  - A verbatim re-commit is no longer decoded. The startup compaction
    refresh re-commits the committed bytes through `commit_session_snapshot`,
    which carries no typed session. Each runtime now keeps a small typed
    projection receipt: the committed WholeBlob authority it projected and
    the durable head's CAS token. No Session is retained. The receipt is
    recorded only once the durable head row is re-proved to carry the
    committed transcript. The resume's freshness probe records it when it
    proves the row current, and hands its already-decoded snapshot to its
    own projections. While the committed row digest and the durable head
    token are both unchanged, the projection answers from two body-free
    reads. Either one moving takes the full path.
  - `prepared_append_boundaries_project_without_a_decode_or_graph_validation`
    bounds per-turn projection at zero decodes, zero graph validations, zero
    durable body loads (a new counter) and digest work proportional to the
    appended turns. `the_startup_compaction_refresh_projects_without_a_decode`
    pins the refresh at zero decodes and zero durable body loads, and
    `a_moved_durable_head_voids_the_projection_receipt` pins the fallback.
    `a_prepared_snapshot_whose_head_moved_falls_back_to_the_committed_read`
    pins the fallback when the head moved.
- A console send that fails now says why, in both queue and steer modes and
  on the multipart (attachment) door. Before, a refused or failed request
  (e.g. a 401 from an off-network browser with no console token) could leave
  the queued row on "Awaiting acceptance / Waiting for confirmation" with the
  agent shown as busy. Every other failure collapsed into a generic
  "Acceptance unknown". The console now classifies failures from typed
  transport facts: HTTP status, JSON-RPC code or `data.kind`, and the fetch
  layer's timeout, connection-failed or non-JSON marker. A 401 is shown as "Not
  authorized from this network (401)", with the same treatment for 403,
  read-only, invalid params and listed pre-ingress statuses. Each of these
  is a definite rejection with "Retry same attempt", and the saved message
  is kept. A connection failure (the browser cannot tell an unreachable
  gateway from a lost acknowledgement, so it reads "Acceptance unknown"),
  timeout, 429 (the REST send answers it after
  reservation), 5xx, unreadable answers, a send interrupted by a console
  reload, and other typed refusals stay reconcilable, each with its named
  reason. A saved attempt answered after its console view ended is settled
  in its own storage namespace, and a failed send also removes its
  optimistic topology frame. The row shows the
  typed label and message instead of hiding them under "Details", and the
  optimistic busy mark is cleared on every failure. "Check acceptance" now
  reports its own typed outcome on the row: could not check (401, a failed
  connection, ...) or no receipt. The gateway's console RPC 401 (JSON and
  multipart) carries `error.data.kind: "unauthenticated"` and
  `http_status: 401`, and the JSON-RPC code stays `-32600`.

- Queued input to a busy member is admitted at once instead of waiting
  behind peer-topology work. Every queue-mode send and dispatch went through
  the ordinary-send peer hydration before admission: console input, the
  `send_*` and `dispatch*` lanes, and the RPC gateway and SDK `dispatch`
  that use them. That hydration serializes on the process-global topology
  guard, the managed-peer reconcile lock and every topology identity's
  lifecycle lock, and awaits unbounded mob wiring and member builds (host
  callbacks bounded only by the 130 s callback wire deadline) under them. So
  slow or wedged topology work held back the admission receipt, including
  for a member mid-turn. Now:
  - For an already-Active target, admission waits only on its lifecycle
    lock, alias validation, its lease, the bounded recall and the bounded
    bridge admission. meerkat queues the input behind the running turn.
  - A send that wakes a Dormant or retired target first reconciles that
    target's own managed edges (only edges touching it, under one 30 s
    end-to-end budget, and skipped if a concurrent wake already activated
    it), so the
    woken member's first turn can reach its peers. A failure there is logged
    and retried after admission, never a refused message.
  - Peer hydration (peer builds plus the managed-edge reconcile) runs after
    admission. It is detached and runtime-tracked when the runtime's owning
    `Arc` is known. Concurrent post-admission hydrations for one target merge
    into one.
  - The managed topology reconcile and topology edge mutations no longer
    hold lifecycle locks across mob wiring calls. They plan against one
    snapshot of the endpoints' runtime bindings, run the wiring calls with no
    lifecycle lock held, and commit the managed-edge bookkeeping under the
    entries read lock, only if no binding moved.
  - Each wiring call is bounded and settles as
    `IdentityRuntimeError::PeerWiringTimedOut` instead of parking.
  - A reset or respawn during the wiring, or a wiring refusal while an
    endpoint's binding moved, makes the plan stale. Nothing is committed and
    the reconcile replans, up to three times, then settles as
    `PeerTopologySuperseded`.
  - A wire or unwire whose reply timed out may still be queued in the mob
    actor, where inspection cannot see it, so its edge is recorded as
    unsettled. A reconcile settles it only with an ordered command that
    succeeded after it: an idempotent wire if still desired, a tolerant
    unwire if not, whatever inspection reports.
  A wedged wiring call or peer build therefore no longer blocks admission to
  any other identity, queue or steer.

- Cross-mob wiring and `LookupMember` address a placed (host-owned) member
  at its real remote endpoint (meerkat #1269). The member's dialable address
  used to be resolved only through a comms runtime in this process, so a
  placed member reported none, and the same-process bilateral paths built
  `inproc://` descriptors for it. One `member_dialable_address` helper now
  answers from the live local runtime, or from the placed member's durable
  host-acknowledged endpoint (`MobHandle::member_peer_endpoint`, owner
  `Host`). `LookupMember`, cross-process wire and unwire, and the
  same-process wire and unwire paths all use it. A local member that must
  be installed on a placed member is described by its dialable
  (non-`inproc`) address or fails typed (`LocalMemberNotRemotelyAddressable`).
  Readiness for a placed member is its committed edge naming the exact peer
  (name and peer id) with no outstanding external route install reported by
  meerkat's `route_installs()` (the host has acknowledged the trust row).
  Endpoint query faults surface as errors instead of "not addressable".
  Placement decides everywhere (each descriptor decision uses one
  `member_endpoint_status` snapshot per side): a placed member without a usable endpoint is
  `PlacedMemberUnavailable`, never an `inproc://` fallback, and its endpoint
  must be registered under its `MemberCommsName`. The same-process bilateral
  paths skip inproc alias installation and treat a placed side's route as
  satisfied by its committed edge, so they converge instead of rolling back.
  Same-process cross-mob wiring resolves a peer mob member's dialable address
  through that mob's own session service (registered with
  `register_peer_runtime`). Unwire removes by edge name
  (`PeerTarget::ExternalName`), so a down or Broken runtime no longer blocks
  removal (a Broken placed side is still treated as placed), on the
  same-process and cross-process paths alike (the cross-process local half
  finds the edge by its typed `MemberCommsName` when the peer is
  unreachable), and every unwire failure is reported instead of swallowed.
  A committed placed edge whose host has not yet acknowledged the trust row
  is reported as `PlacedRouteInstallPending` and kept, instead of being
  rolled back on every coordinator retry; readiness requires the committed
  edge's external descriptor to name the exact peer id. `local_member_peer_info`, `mobkit/cross_mob/peer_info` (RPC,
  used by the Python SDK) and the console peer-info read report a placed
  member's durable host address instead of `inproc://`.
- Cold boot no longer re-verifies unchanged transcripts in the durable
  projection (#487). Each resumed member re-commits its unchanged transcript
  at least twice at boot: the actor's generation-zero boundary and the
  compaction-outbox checkpoint. `SessionStoreBackedRuntimeStore` projected
  each commit by materializing and re-verifying the whole durable
  transcript, then digesting the committed transcript twice more. It now
  reads the durable body only when a rewrite-carrying successor needs the
  rewrite-chain provers, and proves "already at the committed head" from the
  compact head row (revision, generation, message count). When nothing was
  appended, the head shape check takes the full content digest, which seeds
  the digest the successor head mint then reuses. Measured through the
  shipped `rpc_gateway --persistent` identity-first composition on meerkat
  0.8.47, `mobkit/init` hashed 11.79x the durable transcript bytes before
  and 7.86x after (11.95x to 8.03x at about 4 MB). The two
  `unchanged_boundaries_over_*_hash_the_transcript_once` tests bound a
  projection of an unchanged transcript at one digest.
- The console's recovery history refresh no longer re-reads a busy member's
  whole session every discovery tick (#487). A member with a staged or
  running turn is not idle, so its refresh stays unsettled and the member
  stays on the retry list. Every 5 s pass used to skip the write-epoch gate
  and read the full document again. An unsettled pass over an unchanged
  durable write epoch now skips the read and keeps the retry armed. The pass
  that observes the member settled, any pass after a durable write, and any
  forced refresh still read
  (`unsettled_recovery_refresh_does_not_reread_an_unchanged_document`).

- Meerkat 0.8.49 typed shapes are handled. A member reload that re-attached
  a registration left without a runtime attachment (`Reattached`, #1248)
  reports the `reattached` disposition with `reloaded: true`. A delivery
  refused with the typed `MemberRuntimeDetached` is resume-repaired onto the
  same durable session on the variant, carrying queued input. The build
  callback sends `resume_session_id` only for a typed Resume (#1225): a
  spawn's pre-assigned id rides as `session_id` with
  `session_build_intent: "mint"`, and standing instructions apply on a mint.
  Both production `RuntimeStore` decorators forward the new
  `load_committed_whole_blob_metadata` (#1255) instead of answering the trait
  default.
- Identity-first members no longer wedge at their second auto-compaction when
  two compactions rewrite a session to the same transcript (#488). Record-only
  rewrites named the adopted continuity strand by the revision digest alone,
  so a recurring revision re-targeted the strand it was rewriting. Its
  immutable post-head rows refused the new base ("rewrite replay save at
  generation 2 ... not a continuation of persisted revision strand:<rev>
  seq:1"), the member went repair-blocked, and every later send was refused.
  The fix is in meerkat-core (lukacf/meerkat#1271): the rewrite
  validator mints the occurrence-named strand, shipped in meerkat 0.8.49. Strands already written under revision names
  stay readable, and a member refused this way recovers on its next replay.
  A regression test drives two identity-first members through `rpc_gateway
  --persistent` with a demo LLM, whose identical summaries make every
  compaction revision recur, to at least five adopted compactions each. It
  asserts that every tracked turn completes, that the durable rewrite chain
  is contiguous with a fresh strand per generation, and that the chain
  restores and keeps accepting sends after a cold reboot.
- Console voice readiness is fast and never a false "no". The server bounds
  a check at 4 s (below the console's 5 s request timeout) and, on expiry,
  logs the typed stage that was running with every stage's elapsed time, then
  answers the typed `voice_readiness_timed_out` error instead of
  "unavailable". Concurrent checks for one principal and identity share one
  check, which is dropped when the last caller gives up. The voice target is
  one typed lookup instead of a roster scan that cloned the whole mob machine
  state per member. The console shows the voice button at once in a checking
  state, never runs two checks for one agent at a time, retries a failed
  check after 1, 2, 4 and 8 s (capped at 15 s), and aborts the request when
  the view changes.
- `console_timeline_carries_a_revived_members_first_run_after_restart` (and
  its archived twin) no longer assert which stream the real console forwarder
  attached first, which raced: the forwarder's ordinary subscription can land
  between the revived session becoming subscribable and the create-time
  capture's install. Both paths precede the actor's first run, so both are
  lossless; the tests now assert the first run appears exactly once. The
  capture's adoption over a pending backoff stays pinned deterministically by
  the driven restore test.
- A mob activation whose explicit resume stalls no longer fails bootstrap.
  On a memory-starved host one member's resume can go past meerkat's 30 s
  patience window, which returns the retryable
  `MobError::LifecycleOperationProgressStalled`. MobKit treated it as fatal,
  shut down, and exited, so a supervisor restarted the whole boot from scratch
  in a loop (seen in a downstream app's production). Activation now re-joins the same
  meerkat operation in-process (`MobHandle::resume` on the prepared handle
  joins the still-current Resume) for as long as it makes progress: a changed
  member or stage resets the count. It gives up after
  `MOBKIT_ACTIVATION_STALL_RETRIES` consecutive stalls at one member and stage
  (default 10, clamped to 1-1000; about five minutes without progress) or
  twenty times that many stalls overall. Each stall logs a WARN with member,
  stage, attempt, and elapsed time, and completion after stalls logs an INFO.
  Any other activation error stays fatal.
- The agent-memory member-event observer no longer retries
  `subscribe_agent_events` once a second per member. It subscribes on the mob's
  machine-state change that makes a member live, skips passes while the mob is
  not Running, and backs a failing member off exponentially per identity (1 s
  doubling to 30 s), the backoff deadline being only the fallback wake. A
  stream that closes within a second of its subscribe counts as a failure, so
  it cannot spin. During a cold boot this was a flood of actor commands
  (meerkat #1250).
- `mobkit/send_message` to a raw roster member with an interaction id retries
  once on `StaleFenceToken` against the re-read binding (a respawn between the
  binding read and the submit no longer fails the send), and no longer makes a
  redundant `member()` round trip: SubmitWork admission already refuses a
  restore-failed member with the typed `MemberRestoreFailed` (#479).
- The console store's unobserved-start phase clear also clears a stale
  callback pause (#479).
- `mobkit/interact` refuses a caller-supplied `interaction_id` that is still in
  flight on the identity with `-32602` and `error.data.code:
  "interaction_id_in_flight"`, before delivering anything (#479). A settled id
  may be sent again.
- The gateway_composition console-drain tests use a unique mob id per
  composition, so in-process `cargo test` no longer fails them with
  `ParticipantNameOccupied` (#480).
- Console response phase no longer stays set after a run whose start was not
  observed (#469). A typed `run_completed` / `run_failed` that names a run but
  no interaction, with no tracked active run and no active interaction for the
  identity, now clears the phase that run left behind (for example
  `run_failed` with no final `turn_completed`), so the console drain gate stops
  parking the queue. Such a terminal never closes another current run or
  interaction and settles no reservation.
- `mobkit/send_message` console reservations now close when their run settles
  (#469). The reservation id is a UUID (was `mobkit-send-...`) threaded as the
  run's typed interaction lineage on both delivery targets: the identity
  bridge carries it into the WorkSpec, and raw roster members receive it on
  an external-origin WorkSpec. Previously the ids could not be threaded, so
  reservations accumulated to the 256 cap and each new send evicted a settled
  one with a false `interaction_failed { reason: "queue_overflow" }`. The
  untracked `mobkit/send` lane now threads its reserved id as well. The id is
  not part of the `send_message` response; it appears only as the console
  event `interaction_id`.

- Ordinary append boundaries no longer run the rewrite-chain provers. The
  committed-to-durable projection compared the durable row with the
  committed transcript's current revision, which includes the turn's own
  appends, so every boundary that appended rows after a compaction missed
  the at-head check. It then walked the rewrite chain and ran both repair
  admissions (durable-behind replays the graph per commit) inside the
  boundary commit, under the runtime's driver mutex, found nothing to
  repair, logged a "no repair admission holds" WARN and fell through to the
  plain projection save (the downstream app: about 3.6 s per pass across 16
  identities and one WARN per turn). A durable row that is the committed
  transcript's digest-prefix past its last rewrite, at the committed rewrite
  generation, now goes straight to that save. Reading the rewrite generation
  off the loaded row on stores without an incremental head keeps the
  at-head check correct there, and the WARN, which now fires only on
  unproven shapes, compares against the head commit's parent and names the
  relations it tested.

- `send_and_wait` / `dispatch_and_wait` / `dispatch_text_and_wait` (Python)
  and `sendAndWait` / `dispatchAndWait` (TypeScript) return the output of the
  turn they started. They waited until the identity-wide completion cursor
  passed the send's baseline and returned the session's latest
  `output_preview`, so a concurrent delivery to the same identity (a peer
  message, a scheduled turn, a fork completion wake) could satisfy the wait
  and hand the caller someone else's output. They now send with
  `track_turn` and wait on the turn ticket through `mobkit/turn_result`;
  signatures and return types are unchanged. A failed turn raises
  `TurnFailedError`, and an unknown ticket (for example after a gateway
  restart) raises `TurnUnknownError`. Text cut at the 256 KiB bound, and a
  turn that completed without output of its own, are never returned
  silently: each emits a typed warning (`TurnOutputTruncatedWarning`,
  `TurnOutputUnavailableWarning`). When a delivery cannot be tracked (an
  `autonomous_host` member, an externally bound member, a gateway that
  predates turn tickets) it is still delivered exactly once, and the helpers
  fall back to the cursor wait with a `TurnTrackingUnavailableWarning`
  naming the typed reason (a Python warning; a Node process warning in
  TypeScript); when nothing was delivered (`not_delivered`) they raise the
  typed `TurnNotDeliveredError` instead (nothing ran, so a retry is safe). On an `autonomous_host` member (meerkat's default mode) that
  fallback wait is still identity-wide, so the per-turn guarantee holds for
  `turn_driven` members. `wait_for_completion(baseline)` / `waitForCompletion` and
  `wait_for_output(after=cursor)` remain identity-wide primitives: any
  completion on the identity after the baseline satisfies them and the
  returned preview is the session's latest output, not necessarily a given
  send's answer; use `wait_for_turn` / `waitForTurn` for one specific turn.
  Do not mix ticket waits and cursor waits on one identity: a ticket can
  settle before the identity health monitor counts the same turn on the
  cursor, so a later `send` followed by `wait_for_output(after=baseline)` can
  be satisfied by the earlier `send_and_wait` turn.

- Show canonical peer display names while preserving exact peer identities in
  details. Keep empty WorkGraph query results inspectable, and avoid duplicate
  tool rows when event polls are already represented by a work card. WorkGraph
  event and deadline displays use the same local clock as the conversation.
- Calculate assistant work duration only from matching run-start and successful
  completion evidence. System notices never inherit assistant timing, and
  messages with unknown run timing omit the duration.
- Observe restored identity edges after eager continuity attachment so the
  bootstrap report reflects the wired sessions. Lazy restore remains deferred.
- Project durable runtime notices during their original active run and reconcile
  them against saved history without duplicating or reordering the instruction.

- Complete deferred activation before persistent gateway and builder startup
  returns. Classic compositions explicitly resume; identity compositions
  register continuity owners before resuming preserved sessions.
- Keep queued console sends, identity inspection and startup readiness within
  their existing deadlines when the owner's member-observation lane is busy.
  Only refused reads retry; message admission and delivery are not repeated.
- Keep persistent gateway identity stable when a storage directory beneath a
  symlink is created between launches by resolving its existing parent first.
- Preserve a streamed response across an overlapping queued steer, including
  its exact source and the subsequent reply's assistant header.
- Let Python text-dispatch helpers carry caller-supplied idempotency keys with
  correlation IDs, including the completion-waiting helper.

- `fork_off` children sent with `idle_retire_secs: null` are idle-retired on
  the runtime default. The null disabled retirement for the child, so a model
  that filled in every optional field with null pinned each fork child until
  the session ceiling refused new work, the very outcome `fork_off`'s
  runtime-default opt-in exists to prevent (reported by the downstream app). Helpers
  sent through `delegate` with a null had the same problem. See the behavior
  change under Changed.

- Concurrent `mobkit/inspect_identity` calls no longer each cost a
  member-status read. Meerkat admits one member-status read per mob at a time
  (refusing the rest as `observation_lane_saturated` before meerkat 0.8.45),
  so a once-a-second `inspect_identity` poller made `mob_check_member` fail.
  Concurrent inspections of one member incarnation (and `wait_for_output`,
  which polls the same path) now join one bridge read in flight. A settled
  read is never reused, and a caller never joins a read that started before a
  completion it already counts, so the output next to a completion cursor is
  never an older turn's.
- The console's per-member progress reads member status with bounded
  concurrency instead of one member at a time, and no non-final member is
  silently left without progress any more: a member whose status read errors,
  times out, or carries no progress shows its run state from the runtime
  machine it runs on (`run_open`, `idle` or `unknown`) with the typed
  `unknown` health class, instead of being dropped.
- A busy member's status returns its last committed preview. MobKit's session
  service wrappers forward meerkat 0.8.45's member-status observation exactly,
  so a status read joins meerkat's one bounded observation for that member
  instead of reading the whole session behind a running turn.

- A detached job's completion entry that shares its notice with a refresh
  block (a persisted `BackgroundJob` block and a non-persisted one, in either
  order) now counts as a completion entry. `is_detached_completion_entry`
  required every `BackgroundJob` block of the notice to be persisted, so the
  hygienist could collapse such a row and `mobkit/bound_member_transcript` did
  not count it in `dropped_completion_entries`. It now uses meerkat 0.8.43's
  typed `SystemNoticeMessage::persisted_background_job_id`: a `BackgroundJob`
  notice with at least one persisted background-job block. Refresh-only
  notices and text lookalikes are still not completion entries, and a row
  carrying several completions still counts once.

- The idle sweep no longer retires a member that still owns a live member it
  spawned. meerkat 0.8.43 retires everything a member spawned, directly or
  transitively, along with it (the roster's `spawned_by` provenance). The
  sweep judged a member idle by its own turn alone, so a `fork_off` child C
  that forked D for long work and then went idle was retired after its idle
  window, and the cascade killed the still-running D, whose outcome reached
  nobody. A member now counts as idle only while no member it spawned is
  still in the roster, retiring or not (the cascade reaches deeper members
  only through such a child). No member status is consulted, so a child seen
  retiring and then forked or respawned again under the same identity is
  never mistaken for a gone one. Once its children are gone, its idle window
  starts over.

- The idle sweep never waits for a member's running turn. It read each
  candidate's execution snapshot, which a session answers only between turns,
  so one opted-in member mid-turn held the whole pass (every idle member
  behind it waited) until that turn ended. The sweep now asks the runtime
  machine, which answers without queueing behind the turn, and reads the
  snapshot with a 250 ms bound: a member with a run open, or whose session
  does not answer in time, counts as busy and is left for a later pass.

- Councils a restart interrupted are recovered after the restart (requires
  the meerkat release carrying #1190's durable-store council sweep). MobKit
  built its agent-tool `MobMcpState` with `Arc::new`, so the state could not
  own a restore task and meerkat's council sweep never ran: councils the
  previous process left mid-run stayed unfinished, and a detached council's
  convener never heard back. The state is now shared with `into_shared`;
  restoring the mob through `mob_insert_handle` with MobKit's durable council
  store schedules the sweep (recovery, the retry once the dead coordinator's
  claim lease lapses, and the detached-council re-link).

- A persistent `MobBootstrapSpec` (`MobBootstrapSpec::persistent`, and the
  persistent `UnifiedRuntimeBuilder` path identity-first library hosts use)
  ran two `MeerkatMachine::persistent` instances over one runtime store: the
  spec's own machine, which `MobRuntime` hands to `MobBuilder` and which
  therefore hosts member sessions, and the persistent session service's
  internally cached one, which the wrapped session service served from
  `runtime_adapter()`. Consumers of the session service's adapter, among them
  the agent mob tools' `MobMcpState`, acted on a machine that never
  registered the member sessions. The session service now serves the spec's
  machine, as the gateway and runtime-backed ephemeral compositions already
  did.

- Spawned members' idle-retirement opt-ins survive a gateway restart. The
  opt-in (`idle_retire_secs` on `fork_off`, `delegate` and `mob_spawn_member`,
  and the runtime-default opt-in every `fork_off`, RPC and console fork child
  gets) lived only in an in-memory map, so after a restart the restored fork
  children were never idle-retired and held their sessions until retired by
  hand. Opt-ins are now written through to the persistent metadata store
  (new `member_idle_retire/<member_id>` rows in the existing `mobkit_metadata`
  table; no schema change) and restored when the idle sweep starts, so the
  sweep re-arms for restored members. Each opt-in is bound to the member
  instance it was set for, by the member's bridge session: the sweep honours
  it only while the member seated under that id still runs that session and
  drops it otherwise, so a member id reused after its member was retired
  outside the sweep never inherits the old opt-in. A respawn keeps the
  opt-in: MobKit's operator, RPC and console respawns carry it to the
  respawned session, and an identity member's continuity rebind (respawn or
  delivery repair) does the same. Every retirement of a member session,
  whatever surface caused it (the sweep, `mob_retire_member`, operator or
  console retire, reset, destroy), releases the opt-in bound to exactly that
  session, observed on the mob event stream; a reset starts the member fresh
  without its opt-in. Opt-ins for members that are not seated when the call
  returns (a `delegate` helper is already retired) or that sit in a mob the
  sweep does not manage are not recorded, and one undecodable row is skipped
  with a warning instead of aborting the restore. The agent-facing
  `mob_respawn` tool carries the opt-in like the other respawn surfaces;
  member reset, mob reset and mob destroy release only opt-ins recorded
  before the event; opt-ins of implicit delegation mobs are released when
  the sweep no longer finds their member or mob (a member with a MobKit
  respawn in flight is skipped); a member retired and then resumed onto its
  exact session gets its opt-in back (in the same process); and a release
  deletes a row by key and bound session, so rows written by other versions
  stay deletable. A respawn carries the opt-in only when it succeeded and
  the member now under the id is the immediate successor (next roster
  generation) of the incarnation read before it; if another surface retired
  the member and seated a new one, reset it, or respawned it meanwhile,
  nothing is carried. Limit: every doubt errs toward a leak: a failed or
  degraded respawn, an interleaved change of the member, or a crash or
  dropped request between the respawn and its carry loses that member's
  opt-in (it is then not idle-retired); it never makes a different member
  retirable.
  `PersistentMetadataStore` gains `load_member_idle_retire_overrides`,
  `set_member_idle_retire_override` and `clear_member_idle_retire_override`
  with default bodies that keep the old in-process behavior for external
  implementations; `MemberIdleRetireOverrideRecord` and
  `DelegateIdleRetireOverride` are now public.

- Detached `fork_off`/council completion entries are protected from MobKit's
  transcript curation (requires the meerkat release carrying #1190). Meerkat
  records a detached job's outcome in the owner's transcript as a durable
  `BackgroundJob` system notice whose `SystemNoticeBlock::BackgroundJob`
  blocks are `persisted`; `meerkat_mobkit::detached_completion::
  is_detached_completion_entry` recognizes exactly that typed shape
  (non-persisted `BackgroundJob` notices, such as shell job progress, stay
  ordinary refresh projections). The hygienist's `collapse` op could fold
  such a notice into a one-line summary like any other system notice; the
  entry now classifies as the new `HygieneRole::BackgroundJobResult`, which
  the validator treats as untouchable like the system prompt (a new variant
  on the public `HygieneRole` enum). `mobkit/bound_member_transcript` keeps
  its keep-last-N contract and still drops everything before the cut, but
  its response now reports `dropped_completion_entries` so losing completion
  entries is never silent.

- `reset_all` startup readiness (`startup_history`) now waits for the startup
  turn to really end. It counted any `turn_completed` frame as the end, but
  upcoming Meerkat emits `turn_completed` with `stop_reason: tool_use` after
  every tool-loop call, so a tool-using startup turn read as ready after its
  first tool round. Terminality now comes from typed data only: a
  `turn_completed` ends the turn unless its typed stop reason is `tool_use`
  (a payload without one stays terminal, the Meerkat 0.8.42 shape), the run's
  own `interaction_complete`/`run_completed` ends it, and a session-history
  `interaction_complete` projected from an assistant step whose stop reason is
  `tool_use` no longer counts. The rule is shared with the console event
  store's response phase and matches the console's
  `isTerminalTurnCompletedData`. The incident command center example's
  TypeScript smoke applies the same rule to its `query_timeline` replay
  terminal check.

- Forks of transcripts holding an image written before MobKit 0.8.41 no longer
  fail with "blob identity mismatch". Those references carry MobKit's raw-bytes
  address, `sha256(media_type || 0x00 || decoded_bytes)`, while Meerkat
  recomputes `content_blob_id(canonical_media_type, base64)` on read-back.
  `Base64BlobStoreAdapter` implements Meerkat's `BlobStore::attest_address`: it
  recomputes MobKit's raw-bytes recipe over the payload it returned and attests
  only an exact match, so Meerkat's durable-fork preflight re-homes the child's
  reference to the Meerkat content address and realtime history hydration
  accepts it. Meerkat asks only when the stored media type matches the image
  block's and the bytes are a valid image of that type, so a non-image
  MobKit object cannot pass as an image. The source transcript and its stored
  object are unchanged; any other reference stays unattested and is refused as
  before. Requires the Meerkat release that adds `BlobStore::attest_address`.

- Console WorkGraph graph view: labels no longer render smeared or tiny. The
  graph SVG inherited the global icon reset (`stroke: currentColor;
  stroke-width: 2`), which outlined every glyph, status dot and arrowhead, and
  its viewBox was the layout size, so `preserveAspectRatio` scaled all text with
  the graph (about 6px for a dozen items). The viewBox is now the measured frame
  (one user unit per CSS pixel), the initial and Fit view scale the layout down
  to fit but never below 0.85, labels use the console font tokens with
  geometric text rendering, titles and meta lines truncate by measured width
  for the active theme variant (full text stays in the hover tooltip), and
  `blocks` edge labels sit above the nodes beside their arrowhead.

- The console voice context status maps Meerkat 0.8.41's
  `LiveContextPreparationFailure::AuthorityRejected` (the generated authority
  refused the initial context, no fallback attempted) instead of failing to
  compile with the `openai-live` feature.

### Changed

- Bind the Meerkat family to published 0.8.41 (WorkGraph-backed parallel live
  delegation, typed WholeBlob repair hold, mob-realm WorkGraph attention rule,
  summary seeding as startup input, cargo-only PR CI upstream).

### Added

- Typed "session needs repair" hold for the WholeBlob audited-endpoint wedge
  (downstream app report 2026-09-22, session `01a000bb-b69e-7570-933d-ffd5d61d51ee`).
  meerkat 0.8.41 types the refusal (`SessionError::WholeBlobAuditedEndpointDivergence`,
  resume hold `audited_endpoint_divergence`): the committed document keeps
  every message, but every read refuses until the operator runs the sanctioned
  `rkat session repair-wholeblob` repair. MobKit now reads that hold typed
  (never from error text) on all three doors it can arrive through, the
  resume (`ResumeRejectionKind::AuditedEndpointDivergence`), the registration
  reload (`BridgeError::SessionRepairRequired`), and the heal authority
  (`CommittedBoundaryRepair::RepairRequired`), off every carrier meerkat
  0.8.41 uses (`MobError::SessionError(..).durable_resume_hold()`, the shared
  retirement / lifecycle wrappers, the `hold` a Broken member's
  `MemberRestoreFailed` carries, and the structured `durable_resume_hold` wire
  token), and parks the identity Broken
  with a `SessionRepairRequired` hold (`ContinuityFailureKind::RepairRequired`)
  carrying the session id and the exact diagnose and apply commands. The hold
  rides `mobkit/member_health.session_repair_required`, the identity status,
  the console status and inspect payloads (`session_repair`, console health
  `needs_repair`, rendered as **needs repair** with both commands), and the
  typed error's `error.data` (`kind: mob_member_session_repair_required`,
  meerkat's own `durable_resume_hold` key, `retryable: false`).
  The commands are scoped to the runtime store the runtime actually opened,
  recorded typed at composition as `ResolvedStorageSummary::runtime_store_locator`
  (`RuntimeStoreLocator::SqliteFile` for the default `<state_dir>/runtime.sqlite`,
  rendered as `rkat session repair-wholeblob <id> --runtime-store <path>`;
  `RuntimeStoreLocator::MeerkatRealm` for a composite storage provider's
  meerkat-level realm, rendered as `rkat --state-root <state_dir> --realm
  mobkit ...`). Both gateway binaries and `UnifiedRuntimeBuilder` derive the
  scope from that record; `UnifiedRuntimeBuilder::session_repair_scope`
  overrides it, and a runtime with no persistent runtime store declares no
  scope, so the commands carry explicit placeholders instead of a guessed path.

### Fixed

- A member whose durable session failed the audited-endpoint guard no longer
  retry-storms. Before, the refusal classified as a retryable resume
  rejection: the identity Broke, the continuity repair supervisor re-ran
  recovery and the resume on a timer, the registration reload reported "store
  not healthy yet, retry later", and every send stayed reload-required with
  no operator path anywhere. Now the FIRST typed refusal parks the identity;
  the supervisor makes no heal call and no reconcile retry against it, and
  reconcile keeps the Broken projection typed `RepairRequired`.
  `mobkit/reload_member` is the way back: after the operator's repair the same
  verb, with no extra flag, resumes the SAME session and generation and clears
  the hold; a document still refused re-parks the identity typed and records
  the attempt as `last_reload.outcome: repair_required`.
- Member-bound WorkGraph attention follows Meerkat's mob realm. Meerkat 0.8.41
  builds every mob member in the realm `mob.<mob_id>` and rescopes the
  WorkGraph service a host hands the mob runtime to that realm, so a member
  resolves attention bindings there and nowhere else; a goal, binding or
  reassignment that names a member (`mob/<mob_id>/agent/<identity>`) from any
  other realm is refused by Meerkat with the typed
  `AttentionTargetRealmMismatch`. MobKit's runtime service was already scoped
  to the mob realm, so console, RPC and agent-tool work bound to this mob's
  members lands where the member reads it. Two things changed:
  - `mobkit/workgraph/goal/create`, `attention/reassign` and
    `attention/break_glass_reassign` classify the lowered target by its typed
    owner key and refuse a member of another mob before the write with
    `-32602` and `data.kind = "attention_target_realm_mismatch"` (`owner_key`,
    `mob_id`, `required_realm_id`, `realm_id`), instead of storing a binding
    that member would never see. Such work is created through that mob's own
    runtime; the surface stays scoped to one realm (`realm_id` is still never
    accepted).
  - Bootstrap migrates bindings written before the rescoping: every active or
    paused binding in the runtime's realm whose owner key names a member of
    another mob (a child mob spawned by an agent tool shared the parent's
    unscoped service before 0.8.41) is re-created in that mob's realm over the
    same store, with title, description, mode, policies, priority, labels,
    timing fields, external and evidence refs, delegated authority, projection
    policy and paused state preserved, and the original goal is cancelled
    (which stops its binding), so a rerun finds nothing to do. Edges of the
    original item are not carried. `MobBootstrapSpec::with_workgraph_realm_migration`
    selects `apply` (default), `dry_run` or `off`; the report
    (`MobRuntime::workgraph_realm_migration`, `UnifiedRuntime::workgraph_realm_migration`)
    is serialized on `mobkit/capabilities` as `workgraph_realm_migration`, and a
    failed scan or write is recorded there rather than aborting bootstrap.
  - Every `MobSessionService` wrapper forwards the two methods Meerkat 0.8.41
    made required (`fork_persisted_session_at_turn_boundary`,
    `commit_live_delegation_final_transcript_at_turn_boundary`).
- Meerkat image blobs stored through MobKit's blob store now carry meerkat's
  required content address. `Base64BlobStoreAdapter::put_image` (the meerkat
  `BlobStore` face of every `ObjectStoreBlobStore` the gateway, the storage
  provider and the console voice host inject) decoded the base64 payload and
  delegated to `put_bytes`, which addresses blobs as
  `sha256(media_type || 0x00 || decoded_bytes)`. Meerkat requires
  `content_blob_id(canonical_media_type, base64_text)` and recomputes it on
  read-back, so every meerkat integrity gate refused those blobs with
  `BlobIdentityMismatch`: the durable-fork preflight (the default live
  delegation policy) for any session carrying an image, realtime user content
  with an image, and realtime history image hydration. The adapter now mints
  meerkat's id, stores under the canonical media type, and writes through the
  new `BinaryBlobStore::put_bytes_addressed`. MobKit-native blobs written with
  `put_bytes` (console image uploads, `mobkit/blob/*`) keep their raw-bytes
  address and stay readable through both faces, so existing stores need no
  migration. Console image uploads (`mobkit/blob/upload` and multipart
  `image_upload` placeholders) now store through
  `blob_store::put_meerkat_image_bytes`, so a console-attached image survives
  `verify_stored_image_blob` in a later durable fork; references to images
  uploaded before this change keep resolving through the same store.
  `BinaryBlobStore` gains the required method `put_bytes_addressed`; external
  implementors must add it.

### Changed

- Console voice time-to-talk. Clicking the voice button on an agent with a
  long history showed **Checking / Reading / Preparing / Sending context**
  for 6 to 11 seconds while the microphone had in fact been open since about
  1.5 seconds, and nothing told the user they could talk. Four changes:
  - The voice bar says **Listening, you can talk** as soon as the microphone
    is open; context preparation is a secondary hint (**Context arriving**,
    then **Context supplied**) that never gates talking. The stage is kept on
    `data-context-stage` for tests and tooltips; `data-phase` is unchanged.
  - The console requests the microphone during the click, concurrently with
    the readiness RPC (released again if readiness is negative), and waits
    for the WebRTC peer and data channel by event instead of issuing a
    `mobkit/live/status` poll every 100 ms until the transport connects.
    The connect timeout still bounds both.
  - One validation on the open path. `mobkit/console/voice/open` no longer
    repeats the readiness probe the console already ran on the click; the
    host's own target resolution and Meerkat's open admission still refuse a
    foreign principal (`access_denied`), an unknown member, a grant held by
    another principal, or a missing credential (`voice_unavailable`). A
    re-open of the same active request stays idempotent.
  - The context summary is bounded and cached. `console_voice.summary`
    (`PublicLiveRegistration.summary`) sets `max_input_bytes` (default
    65536: the most recent 64 KiB of the serialized transcript, oldest turns
    dropped whole, a size bound rather than a content heuristic; a newest
    message larger than the window is summarised whole rather than failing),
    `max_output_bytes` (default 4096, so the 500-byte GPT Live append
    fragments, each awaiting its own receipt, stay at nine or fewer) and
    `model` (default: the agent's own model; Meerkat's catalog exposes a
    support tier, not a speed tier, so no faster same-provider model can be
    derived from it). A produced summary is cached per member, keyed by the
    source session's transcript cursor and a digest of the summarised
    window, so reopening voice on an unchanged transcript reuses it without
    a model call. Meerkat's 4 MiB capture ceiling is unchanged. The Python
    and TypeScript SDK `OpenAiLiveGatewayConfig` wrappers accept the same
    block (`OpenAiLiveSummaryConfig` / `summary: { model, maxInputBytes,
    maxOutputBytes }`). Measured on a
    40-turn seeded session the previous bounds took 4.0 to 5.9 s of summary
    generation for a 1.3 to 1.5 KB result (3 fragments).
- Console voice readiness leaves a server-side trace. `mobkit/console/voice/
  readiness` used to answer `available: false` silently when the host refused,
  when the target could not be resolved, when the credential probe failed, or
  when the server's own 5 s budget expired; the console then showed "Voice
  readiness could not be checked" or "Voice is unavailable" with nothing in
  the gateway log. Every negative answer now logs at warn with the identity,
  elapsed time and classified cause (controller stopped, no live host
  composed, external live channel holding the voice path, identity lookup or
  live-target resolution failure, no or ambiguous member owning the session,
  grant held by another principal, credential probe failure, or server budget
  exceeded); a positive answer slower than 1 s logs at warn as slow, and a
  normal positive answer logs its duration at info.
- Console voice activation is traced. Every console voice control-plane RPC,
  the open stages, the durable-source validation, the WebRTC answer, the
  playback-owner registration, and the activation status polls log their wall
  time under `meerkat_mobkit::console_voice::timing` (polls at debug, the rest
  at info). `console/voice-e2e-live.cjs --time-to-talk` is an opt-in paid
  harness mode that seeds a long session and reports click-to-talk stage
  timings, including the new `'you can talk' visible` stage and the gateway's
  summary generation time, size, fragment count and cache reuse.

### Fixed

- Identity resume no longer retires a spawn that is still in asynchronous
  custody. When `resume_session` collides with `MemberAlreadyExists` for a
  member the roster does not hold yet, the collision came from the mob's
  in-flight spawn guard, not from a stale occupant: the bridge now awaits that
  spawn's convergence (bounded by the actor admission budget, re-offering the
  resume every 5 s so a build that ended empty is noticed) and attaches to
  the resulting member instead of retiring, retrying, colliding with the
  just-finished build, and marking the identity Broken. A committed member
  that is already bound to the resumed session (the spawn finished between
  the collision and classification) is adopted the same way; committed
  occupants bound elsewhere keep the existing custody-gated retire path, and
  a roster read failure refuses to retire and stays retryable. The operator deployment twin report 2026-09-22 item 3
  (`review:singleton`, spawn ticket 15).
- Capability invariant reporting. The post-materialization
  declared-versus-resolved capability invariant logged `requires operator
  attention` at WARN for every member whose decision was `Unverifiable
  { cause: NonExactCatalog }` (330 members per boot on one production
  gateway) although no action existed to take: the members' tools were
  correct and the invariant simply had no exact catalog to compare against.
  Reporting now follows the typed decision. A verified `Gap` is the only
  WarnOnly outcome that asks for operator action, named per missing
  category. `Unverifiable` is explained once per process per cause at INFO
  (what could not be verified, and that no action is required unless a
  member is missing tools), then recorded per member at DEBUG. A failed live
  catalog read stays at WARN as a read error, not as a mismatch. Decision
  and policy semantics are unchanged.
- MobKit's own tool dispatcher wrappers declare exact catalogs. The
  agent-memory `RecorderToolDispatcher`, the SDK `GatewayCallbackToolDispatcher`,
  `ComposedExternalTools`, both schedule wrappers, and the mob-tools auto-wire
  wrapper left `tool_catalog_capabilities()` / `tool_catalog()` on the
  non-exact trait defaults, so every member built with agent memory or
  SDK-registered tools carried a non-exact external slot and the capability
  invariant could never verify it. Each wrapper now derives exactness from
  what it wraps (composition is exact only when both halves are; a fixed
  callback tool list is exact on its own) and publishes the catalog it
  actually advertises, so members with MobKit-composed tools reach a real
  `Match` or `Gap` decision.
- Stuck voice close. `mobkit/console/voice/context_status` no longer
  validates durable-source availability on every poll; that validation loaded
  the full persisted session body on the mob actor once a second per preparing
  voice call, exceeded the actor's inline step budget, and queued every later
  mob command behind it. Availability is validated once at open admission,
  and the channel custody read remains the authority for the poll.
- `mobkit/live/close` on a channel that already completed its close reports
  `Closed` when the live host no longer holds an active binding but the
  machine records the channel's close status, instead of "the experimental
  live channel is not active for the session". Close failures are coded by
  origin: a binding or custody mismatch is `-32602`; a physical transport,
  terminal projection, or semantic failure is `-32000`, which callers may
  retry. Retiring a remote that already hung up needs meerkat 0.8.41 (the
  transport retirement bound fix); MobKit on 0.8.40 still surfaces
  `-32000` until the owner converges.
- Console voice close is bounded. A retryable close failure (network,
  timeout, or a `-32000` gateway fault) is retried on a fixed backoff
  (`VOICE_CLOSE_RETRY_DELAYS_MS`: 2, 4, 8 s) after the first attempt has
  released the microphone and speaker, then reported as unconfirmed with the
  attempt count. Caller-side rejections fail at once. The previous behaviour
  retried once per user click without a cap.

### Changed

- Documentation publication follows main, not releases. A new `Publish docs`
  workflow dispatches `mobkit-docs-updated` to docs.rkat.ai on every push to
  main that touches `docs/`, and the release workflow no longer publishes
  documentation. Published MobKit docs now match whatever is on main.

### Added

- Library embedders can register console voice and the external live
  channel through the builder: `UnifiedRuntimeBuilder::console_voice(
  PublicLiveRegistration)` and `UnifiedRuntimeBuilder::live(LiveOptions)`.
  The builder retains the typed persistent session service, machine, and
  agent factory it already creates, and `UnifiedRuntime::compose_live`
  composes one shared live context with the console voice controller and the
  `mobkit/live/*` handler over it (the same shared-owner arbitration as the
  gateway binaries). `build_reference_app_router_with_live(decisions)` mounts
  both beside the console and refuses an open console when voice is composed
  (`LiveComposeError::ConsoleVoiceRequiresAppAuth`); a live door on an
  ephemeral runtime fails `build()` closed
  (`UnifiedRuntimeBuilderError::LiveCompose(LiveRequiresPersistentSessions)`).
  `live(...)` requires a `public_base_url` (`LiveOptions::new(url)`), failing
  `build()` closed otherwise (`LiveRequiresPublicBaseUrl`). Accessors:
  `console_voice_controller()`, `live_context()`, `live_rpc_handler()`.
- `parse_console_auth_config` accepts `provider: "oidc"` with `issuer`,
  `audience`, a required `email_allowlist`, and exactly one of `jwks_json` or
  `jwks_uri` (fetched once, HTTPS only, failing closed), trusting an external
  issuer's RS256/ES256 keys such as Google's. Under this provider the console
  requires `email_verified: true` and never falls back to `sub`
  (`TrustedOidcRuntimeConfig::require_verified_email`, refused as
  `email_not_verified`). `provider: "jwt"` (HS256 shared secret) is unchanged.
- Deployment guide: build stages with `openai-live` install `libopus-dev` and
  `pkg-config` so bundled Opus is never compiled through CMake.
- Console voice and the external live channel coexist under one shared live
  owner. `rpc_gateway` accepts `runtime_options.console_voice` together with
  `runtime_options.live` (still exclusive with `openai_live` and
  `experimental_live`), composes one live context, and mounts the live
  WebSocket router and the stdio `mobkit/live/*` RPCs beside the console voice
  controller. `live_wiring::LiveOwnerArbiter` arbitrates the two doors "latest
  engaged wins": a console `voice/open` closes an active external channel and
  an external `mobkit/live/open` closes an active console call, each through
  the loser's own close sequence, failing the newcomer closed if that close
  fails. Losers learn why: console requests answer kind `voice_superseded`
  with `reason: superseded_by_external_live`; external `mobkit/live/status`
  and `mobkit/live/close` answer `close_reason: superseded_by_console_voice`
  (without demanding a phase receipt for a channel the arbiter closed); the
  stdio gateway emits `mobkit/live/superseded {owner, identity, channel_id,
  reason, superseded_by}` at once; `mobkit/live/open` reports what it
  `superseded`. `mobkit/console/voice/readiness` answers `available: false,
  reason: external_live_active, holder: {identity, channel_id}` while the
  external door holds the path. The browser ends a superseded call with
  "Voice moved to the external live channel" (or "another console call" when
  the reason says so). A holder whose channel already ended outside both doors
  is released rather than trusted, and a same-owner reopen closes its previous
  channel as `replaced_by_same_owner`, so neither a dropped socket nor a
  reopen can leave two audio owners or lock a door out. `voice_busy` stays
  reserved and unused for arbitration. Shared contract:
  `tests/fixtures/console_voice_v1.json`.

### Changed

- `TrustedOidcRuntimeConfig` gains the `require_verified_email` field
  (`#[serde(default)]`, so persisted or wire snapshots stay readable). Rust
  hosts that construct the struct literally add
  `require_verified_email: false` to keep the historical behaviour.

### Fixed

- Documentation: 148 corrections from the 2026-09-22 adversarial documentation
  audit across the public docs, hidden skills, examples, SDK docstrings, and
  the RCT contract fixture, with the complete evidence ledger under
  `audit/docs-review-2026-09-22/`. Overlapping corrections already landed by the
  27-finding pass are superseded by the audit's fuller wording where both
  described the same fact.
- Documentation corrections from the 2026-09-21 accuracy audit: 27 confirmed
  findings across the RPC catalog, sessions, roster, memory, access control,
  governance, configuration, console, quickstart, and the Python and Rust SDK
  pages, each re-verified against current source.
- The Linux release container installs a pinned Kitware CMake 3.31.12 (sha256
  verified) instead of Bullseye's apt package. The apt install failed on both
  Linux release legs when bullseye-security's pool dropped a dependency the
  CDN-cached index still listed, which no retry can fix.
- Release gateway binaries build again on all five targets. Meerkat 0.8.40's
  `openai-live` path compiles bundled Opus through CMake; the Linux release
  container now installs CMake 3.x and every leg sets
  `CMAKE_POLICY_VERSION_MINIMUM=3.5` so CMake 4 on macOS and Windows accepts
  Opus's 3.1 minimum.
- The session-store-backed `RuntimeStore` decorator forwards Meerkat 0.8.40's new
  `load_head_canonical_metadata` (exact HeadCanonical boundary metadata without
  materializing transcript rows) instead of answering the trait default.
- `MobSessionService::commit_live_delegation_final_transcript` implementations no
  longer gate the method on MobKit's `openai-live` feature. Meerkat 0.8.40 declares
  the trait method unconditionally, so a default-feature build against it failed
  with "not all trait items implemented".
- `fork_off` children are retired on the idle default. Idle retirement was
  registered only for `delegate` (implicit mobs) and for `mob_spawn_member`
  with an explicit `idle_retire_secs`; a fork child seated in the caller's
  mob was never a candidate and held one of the bounded sessions
  (`max_sessions`, default 64) until the ceiling refused new work. `fork_off`
  now accepts `idle_retire_secs` like `delegate` (integer overrides, `null`
  disables) and opts the child into the runtime default when it is omitted;
  `UnifiedRuntime::fork_member` registers the same default for RPC and
  console forks.

### Changed

- Pin the Meerkat dependency family to published `0.8.40`: concurrent voice
  context bootstrap with the summary on the provider's knowledge lane, the
  quiet causal-tail replay, spoken owner context that waits for the user's
  turn to end, a close that cannot stay stuck on an unconfirmed provider, and
  the observation provenance that ends post-acknowledgement echo. All
  Meerkat crates are consumed from crates.io again; no Git bindings remain.

- Pin the Meerkat dependency family to published `0.8.39`: public GPT Live
  playback now settles on the caller's `live/playback_complete` snapshot cut
  instead of a quiet-period heuristic, reopen seeds the canonical dialogue as
  native startup input, a new provider response no longer aborts the session
  when it supersedes an unreported output, and every host binary runs on one
  documented 8 MiB worker-stack budget.

### Added - public GPT Live (`gpt-live-1`)

- New `openai-live` Cargo feature forwarding meerkat's public OpenAI Live
  path (`meerkat/openai-live`, `meerkat-mob/openai-live`,
  `meerkat-mob-mcp/openai-live`, `meerkat-openai?/live`). The neutral GPT Live
  runtime machinery in `live_wiring`, `rpc`, and the stdio gateway now gates
  on `openai-live`; `experimental-gpt-live` builds on it and keeps only the
  deprecated private ChatGPT-brokered operator, Gate0, and factory-identity
  admission. Test twins: `openai-live-test`, and `test-realtime-fixtures`
  now needs only `openai-live`.
- New `runtime_options.openai_live` gateway registration
  (`{principal, realm, auth_binding {realm, binding, profile?}, voice,
  session_instructions?}`) composing `ExperimentalGptLiveOpenAuthority::new_public`
  with the host-fixed `gpt-live-1` OpenAI execution identity, plus
  `LiveCapabilityProvider::public`. Configuring both `openai_live` and the
  deprecated `experimental_live` is rejected.
- SDKs: `OpenAiLiveGatewayConfig` with builder `openaiLive` / `openai_live`,
  and the reserved public profile id constant
  `OPENAI_GPT_LIVE_PUBLIC_CLIENT_CONTEXT_PROFILE_ID`
  (`openai.gpt-live-1.client-context.v1`), which experimental
  `execution_profiles` may not reuse.

### Changed

- Pin the Meerkat dependency family to published `0.8.38`, which ships the
  public GPT Live execution path this release wires into the gateway.

### Fixed - explicit model fallback safety

- Automatic fallback now defaults off and requires an explicit non-empty
  chain (the Meerkat pin this release ships is `0.8.38`, above). Whole-table
  profile, mob-runtime and host policy precedence preserves explicit disable;
  unknown fallback keys and the removed catalog-default switch are rejected.
- Forward the full materialized fallback request and typed admission result
  through replay and taint decorators. Replay pressure uses the same sanitized
  messages as dispatch; admission and pressure probes do not mark ingestion.
- Preserve typed fallback observations and resume-hold error reasons through
  the existing console and SDK event payloads. Unsafe **new provenance-bearing**
  fallback resumes may be held upstream; historical unmarked sessions are not
  automatically inferred, reverted or migrated. Explicit operator resume masks
  are unchanged. No fallback overlay, reversion or new repair CLI is introduced.
- Honor explicit `CARGO_HOME` and `CARGO_TARGET_DIR` in `scripts/repo-cargo`
  while preserving its default repo/worktree isolation.

### Fixed - corrected 0.8.33 companion release

- Pin the complete Meerkat dependency family to registry `0.8.36`, retaining
  the identity-first runtime, builder, completion, typed health, memory and SDK
  integration from the prior candidate. The `0.8.32` tag and candidate are not
  replaced or promoted by this release.
- Include the upstream shared-SQLite cold-resume/cleanup fixes and
  run-result-aware checkpoint terminal-completion correlation.
- Bound stdio callback queue admission, live writer settlement and SDK response
  with one 130-second provider deadline. Closed writers and callback transport
  shutdown wake pending callers; reliable notification failures are reported.
  A timeout leaves delivery/execution fate unknown. An early live SDK response
  is retained but cannot replace successful writer settlement.
- Forward standalone turn admission through both session-service decorators to
  the original inner owner. The original notification sender, request and
  terminal result remain unchanged; decorators never fabricate admission.
- Keep timed-out compaction under runtime-owned exact-input observation instead
  of retiring a turn whose rewritten provisional checkpoint can still commit.
  Caller loss and the secondary observation deadline retain completion and
  profile-cleanup ownership. Only successful finalized completion for the same
  incarnation permits restoration; custom compositions must supply the
  persistent service's exact observer, not its bootstrap adapter override.

### Changed - release operator contract

- **Tag pushes now validate only; they no longer build or publish releases.**
  Use `make release-candidate` for an exact merged-main build, then explicitly
  select accepted immutable artifacts with `make release-promote`. Promotion
  preserves the original signed archive bytes. Registry retries and add-only
  recovery use separate Make targets and never rebuild or replace published assets.

### Changed - BREAKING for host/SDK implementors

- **Actor timeout evidence is preserved without an execution verdict.**
  Rust literals/matches for `BridgeError::ActorAdmissionTimeout` and
  `BridgeAdmissionError::ActorAdmissionTimeout` must handle the new
  `command: Option<ActorCommandTimeout>` field (`command_kind`, `stage`).
  `IdentityRuntimeError::ActorAdmissionTimeout` and
  `ConsoleSendError::{AdmissionTimeout, ActorProbe, RuntimeOperation}` are new variants;
  `IdentityRuntimeError::structured_data()` exposes their observations.
  `DeliveryErrorRecord` gains optional `data`. RPC `error.data`, console
  error data, and SDK member-health records preserve upstream command/stage
  exactly, without `executed: false` or `retryable: true`. A reply deadline
  can follow runtime admission; neither a timeout nor missing flags proves
  nonexecution. Timeouts never trigger automatic repair, reload, or retry.
  Runtime idempotency and durable fate remain authoritative.
  `BridgeError`, `BridgeAdmissionError`, and `IdentityRuntimeError` variants
  `ActorLoopStalled` / `ActorTerminated` also gain
  `observation: ActorCallObservation` (`BeforeCall` / `InFlight`), keeping a
  true early refusal distinct from a probe interrupting an already-started call.
- **Reload integration adds exhaustive Rust error/health variants.**
  `BridgeError`, `BridgeAdmissionError`, and `IdentityRuntimeError` gain
  `AdmissionBacklogFull`, `ReloadRefused`, and `ReloadTimedOut`;
  `DeliveryErrorClass` gains the same three variants. `MemberHealthReport`
  gains `last_reload: Option<ReloadAttemptRecord>`; Rust literals must add it.
  `BridgeMemberReload`, `ReloadAttemptRecord`, and `ReloadAttemptOutcome`
  are public, and `SessionBridge` gains defaulted
  `reload_member_registration` and `member_durability` observation seams.
- **Reload health is observational.** The upstream primitive's `not_current`
  is a no-op, not permission to retire/rematerialize. `last_reload` records
  early no-ops and failures as well as primitive outcomes. Custom durability
  reads run without the fleet entries lock; absence of an upstream degradation
  marker no longer reports `ready`, because it also covers absent registrations.
  `ReloadAttemptRecord.data` retains actor timeout/probe observations from
  explicit and automatic reloads instead of flattening them to reload-required.

- **`ConventionalPaths` gains a public `meerkat_config_toml: Option<PathBuf>`
  field.** The struct is all-`pub` and not `#[non_exhaustive]`, so a Rust
  host that builds it with a struct literal must add the field
  (`ConventionalPaths::discover(..)` and
  `.with_meerkat_config_from_workspace(..)` are unaffected). It carries the
  workspace `.rkat/config.toml` a gateway adopts as its Meerkat host config.

- **The dead `.discovery()` / `.pre_spawn()` builder knobs are gone from both
  gateway SDKs.** Python `MobKitBuilder.discovery(callback)` /
  `.pre_spawn(callback)` and TypeScript `.discovery()` / `.preSpawn()` stored
  the callback on the builder config and nothing ever read it: `_bootstrap`
  registered continuity, lease, roster, topology and customizer callbacks only,
  `mobkit/init` carried no field for it, and neither gateway binary has a
  discovery callback protocol (the Rust `Discovery` trait is a classic-plane
  bootstrap hook that only an in-process Rust builder can install). The knobs
  were born dead in v0.2 and `docs/sdks/python.mdx` listed them as a working
  "Discovery callback (mob-level)", so an SDK host following the page got no
  boot spawn and no error. Wiring them would mean a new callback family in
  Rust and both SDKs for a plane the identity roster already covers, so they
  are removed instead. Breaking only in the sense that a call now raises
  `AttributeError` / is a TypeScript compile error where it used to be
  accepted and dropped; no host that saw members spawn was using them. Boot
  membership is declared through `.roster(provider)` (identity plane) or
  `ensure_member` (worker plane); `[profiles.*]` are templates and spawn
  nothing by themselves. The docs page now says so.

### Added

- **`mobkit/reload_member`: a non-destructive cold reload verb.** A member
  whose runtime registration is durability-degraded (meerkat's
  `RecoveryRepairBlocked`, "registration-authorized cold reload is required")
  had no repair short of `retire_member` then a send, and the tempting
  `respawn_member` is a destructive continuity reset. The new verb discards
  the live registration and re-materializes the SAME durable session under
  the identity's lifecycle lock: same identity alias, same `session_id`, same
  continuity generation. Bridges without the upstream primitive retain the
  legacy retire/resume path with memory harvest skipped; revival goes through
  the existing addressed embodiment door, never a shell-forced
  `Retiring -> Dormant` transition. Result:
  `{reloaded, disposition: "discarded" | "not_degraded" | "not_current",
  session_id, generation}`. Wired on the console plane (`POST /console/rpc`,
  gated by `agent.respawn`), the SDK stdio surface, Python
  `MobHandle.reload_member`, TypeScript `MobHandle.reloadMember`, and
  `docs/api/rpc.mdx`. Broken/suspended identities and worker-plane members are
  refused typed. Retired identities return `not_current` without revival;
  only an operation addressed to them can use the existing revival door.
- **`mobkit/member_health`: an in-process health read for one identity.**
  Returns lifecycle state, whether a materialization is in flight, the last
  delivery error class (`reload_required`, `actor_loop_stalled`,
  `actor_terminated`, `admission_timeout`, `admission_failed`, `completion`),
  the actor-loop probe verdict, and the open `stall_id`. Unlike
  `member_status` it never crosses the mob actor loop, so it answers during
  the very stall it reports. Shaped to carry a `durability: "ready" |
  {reload_required: {operation, reason}}` field once meerkat exposes it
  (0.8.34). Python `MobHandle.member_health`, TypeScript
  `MobHandle.memberHealth`.
- **meerkat 0.8.34 wiring for the reload verb and member health.**
  `mobkit/reload_member` now calls `MobHandle::reload_member_registration`
  when the session bridge exposes it (`SessionBridge::reload_member_registration`,
  default `Ok(None)`): meerkat quiesces, discards and re-registers the
  executor for the same session off the actor loop and reports
  `discarded` / `not_degraded` / `not_current`; `not_degraded` is a success
  no-op and `not_current` is a no-op with no destructive fallback.
  `mobkit/member_health` carries `durability` from
  `MeerkatMachine::durability_reload_required` through the new
  `SessionBridge::member_durability` seam. The delivery classifier matches
  the typed `MobError::MemberReloadRequired` ahead of the 0.8.33 text
  fallback, maps the bounded submit's `ActorCommandTimedOut` to the
  admission-timeout class, maps the per-member lane backpressure
  `MobError::MemberAdmissionBacklogFull` to the typed retryable
  `BridgeError::AdmissionBacklogFull` / `IdentityRuntimeError::AdmissionBacklogFull`
  (delivery error class `admission_backlog_full`; never repair, never
  reload), types the reload primitive's `MemberReloadRefused` (store not
  healthy yet: retryable `ReloadRefused`, "retry later") and
  `MemberReloadTimedOut` (`ReloadTimedOut` naming the stage; inspect before
  retrying) on the verb and the automatic delivery reload alike, records the
  most recent reload attempt as `member_health.last_reload` (a refusal carries
  meerkat's reason), and the internal submit rides
  `submit_work_with_mode_bounded` / `..._and_delivery_identity_bounded` under
  the same deadline the bridge already enforces. The deadline can skip
  still-queued work but cannot retract runtime admission already in progress.
- **`ErrorEvent::ActorLoopTerminated`** (`actor_loop_terminated`): the
  actor-loop probe's round trip resolved with the actor's command or reply
  channel CLOSED. Terminal: the loop is gone and the process must restart.
  Mirrored into both SDK `ErrorCategory` sets.

- **A gateway that exits now says why, and a panic leaves a log line.** Both
  gateway binaries ended their run loop in silence on every designed path
  (stdin EOF, SIGINT, SIGTERM, the SDK shutdown handshake) and had no panic
  hook, so a report of "the gateway process exited, all endpoints 000" could
  not be told apart from a crash, a supervisor's SIGTERM after a slow console
  operation, or the SDK host closing the pipe; the only exit that named
  itself lived in a test name. Each binary now logs one INFO line when its
  `select!` resolves, before the shutdown sequence, with `reason=` naming the
  branch (`rpc_gateway`: `stdin_closed`, `signal`, `sdk_shutdown_handshake`;
  `mobkit_gateway`: `signal`, `http_server_ended`, `stdin_guard_ended`) and
  `signal=SIGINT|SIGTERM` when a signal did it, then a "shutdown complete;
  exiting" bookend, so a wedge between the two is visible as a missing
  bookend. The `rpc_gateway` stdin reader names EOF versus a read error;
  `mobkit_gateway` logs the stdin close it survives by design and, when the
  HTTP server task ends abnormally, logs the error at ERROR on stderr as well
  as failing init on stdout. `init_gateway_tracing` installs a panic hook
  (`install_gateway_panic_hook`, exported) that reports every panic through
  `tracing::error!` with thread, file, line, column and payload before the
  default hook runs, so a task panic that the runtime absorbs is in the log
  stream. `shutdown_signal::shutdown_signal()` now returns the new
  `ShutdownSignal` (`Interrupt` | `Terminate`, `Display` = `SIGINT` /
  `SIGTERM`) instead of `()`; a caller that pattern-matched the unit value
  must bind or ignore it. All lines pass the default filter. Covered by a
  subprocess suite (`tests/gateway_exit_reason.rs`) that drives both real
  binaries through each exit with `RUST_LOG` unset and by a hook unit test;
  `docs/guides/deployment.mdx` gains a "Reading a gateway exit" section that
  also states the two designed exit contracts (`rpc_gateway` exits on stdin
  EOF, `mobkit_gateway` does not).

- **`DurableAgentSpec` exposes `runtime_mode_override` and `initial_message`
  in both gateway SDKs.** The Rust spec has deserialized both since the
  identity-first plane landed (`#[serde(default)]`), and the bridge forwards
  them into the spawn (`with_initial_message`, `runtime_mode`), but the Python
  dataclass had neither field and the TypeScript interface lacked
  `initialMessage`, so an SDK roster could not pin a member's runtime mode per
  identity nor choose its kickoff turn: every fresh `autonomous_host` identity
  ran meerkat's fallback spawn prompt, and the only mode knob was the profile
  in mob.toml. Python `DurableAgentSpec(runtime_mode_override=...,
  initial_message=...)` and TypeScript `runtimeModeOverride` / `initialMessage`
  now serialize to `runtime_mode_override` (`autonomous_host` |
  `turn_driven`, validated) and `initial_message` (text or content blocks,
  the untagged `ContentInput` shape) in the roster callback payload, and parse
  back. Unset fields are omitted, so a roster that sets neither is
  byte-identical to before. Tests drive the roster callback through the
  dispatcher and assert the payload.

- **Gateways can bind a non-loopback address, and refuse to unless told
  how.** Both bundled gateways bound `127.0.0.1:0` unconditionally
  (`GatewayHttpBinding::bind_loopback` was the only constructor), so a reverse
  proxy in another container could not reach them at all. `rpc_gateway` now
  takes `runtime_options.http_listen` (`HOST:PORT`, default `127.0.0.1:0`),
  `http_public_base_url` and `allow_remote`; `mobkit_gateway` takes the same
  three as top-level init params, plus `--http-listen` / `--allow-remote` and
  `MOBKIT_HTTP_LISTEN_ADDR` / `MOBKIT_HTTP_ALLOW_REMOTE` below them, and folds
  the listen address AND the advertised base into its resume fingerprint like
  `--control-listen`, so a relaunch that changes `http_public_base_url` is not
  answered with the previous launch's value. The same four keys sent top-level
  to `rpc_gateway` (the other binary's shape) are refused with `-32602` naming
  their `runtime_options.*` home instead of being ignored. The
  gate is fail-closed and borrows Meerkat's vocabulary
  (`meerkat_rpc::secure_rpc::TcpBindPolicy`, `--allow-remote`): a
  non-loopback bind is refused at init, before any bootstrap, unless the
  console enforces app auth (`auth_config` with a trusted key) or the launch
  acknowledges the exposure with `allow_remote`; `mobkit_gateway` serves an
  open console, so on that binary only the acknowledgement opens it. Every
  non-loopback bind logs one WARN line. The init result's `http_base_url`
  keeps the same-host form (`127.0.0.1` for `0.0.0.0` and `::` binds) so the
  SDK that spawned the gateway still reaches it, and a new
  `http_public_base_url` field (`null` when undeclared) carries the
  proxy-facing base; Python exposes it as `rust_http_public_base_url`,
  TypeScript as `rustHttpPublicBaseUrl`. Builders: Python `.http_listen()`,
  `.http_public_base_url()`, `.allow_remote()`; TypeScript `.httpListen()`,
  `.httpPublicBaseUrl()`, `.allowRemote()`. The SDKs record the advertised
  base and expose it read-only; their own SSE, multipart and console calls
  keep dialling `http_base_url`, because the SDK process spawned the gateway
  and shares its network namespace, so the same-host form is the one that
  always answers (a deliberate reading of "the transport honours the
  advertised base": it is carried and surfaced, not dialled). Rust library
  hosts get `GatewayHttpBinding::bind(addr)`, `HttpBindPolicy`,
  `validate_http_bind_policy(surface, ..)` and
  `warn_on_non_loopback_bind(surface, ..)` in `gateway_composition`, where
  `surface` is the new `GatewaySurface` enum (`RpcGateway` | `MobkitGateway`)
  rather than a string, the console's auth state is the new
  `ConsoleAuthPosture` (`Open` | `ClosedToEveryCaller` | `Enforced`, one
  classifier consumed by the gate, the non-loopback warning and
  `rpc_gateway`'s closed-console warning), and the listen/base-URL parsers
  return a typed `HttpExposureParseError`. The new error enums are
  `#[non_exhaustive]`. Both binaries bind the listener right behind the gate,
  BEFORE any bootstrap, so a port another process holds (fixed ports are now
  possible) is a `-32603` init error on the request id with no runtime built
  and no schedule executor lease taken; previously the bind ran last and a
  collision exited past the graceful tail with the lease held. On
  `mobkit_gateway` every init-param refusal (`http_listen`, the gate,
  `http_public_base_url`, `meerkat_config_path`, `compaction`, host-config
  tables in `mob.toml`) is now `-32602` ON THE REQUEST ID, where it used to
  surface as `-32603` with a null id; the bind failure is `-32603` on the
  request id. Defaults are unchanged: an exact-pinned host that sets none of
  this binds loopback and sees a byte-identical `http_base_url`. A bind
  failure on `rpc_gateway` is a JSON-RPC init error naming the address
  instead of a panic. New `docs/guides/deployment.mdx` covers proxies,
  containers and the gate.

- **Gateways load a Meerkat host `config.toml`, so self-hosted models and
  realm bindings reach the members.** Both gateway binaries built every agent
  from `Config::default()` and had no ingress for the host config where
  Meerkat keeps `[self_hosted]`, `[realm]` and `[models]`, so a self-hosted
  profile was refused at init on `rpc_gateway` and died at the first member
  build on `mobkit_gateway` ("self-hosted model '..' is not registered in
  config"); the only route was a patched binary. `rpc_gateway` now takes
  `runtime_options.meerkat_config_path` (Python `.meerkat_config(path)`,
  TypeScript `.meerkatConfig(path)`); `mobkit_gateway` takes the same file as
  a top-level `meerkat_config_path` init param and adopts
  `<workspace>/.rkat/config.toml` when it exists, folding the file's content
  into its resume fingerprint and logging one INFO line naming the adopted
  file and which door (init param or convention) produced it, so adoption is
  never invisible. The file is loaded once at init, merged over
  `Config::default()` with Meerkat's own file semantics and validated with
  Meerkat's own `Config::validate` against the canonical catalog, the step
  `rkat` and `rkat-rpc` run after every load
  (`gateway_composition::load_gateway_host_config`; a new
  `GatewayHostConfigError::Invalid { path, source }` variant carries the
  verdict), with the gateway's comms and compaction overlays layered on top;
  a missing, malformed or non-validating file (a `[self_hosted.models.<id>]`
  alias naming an undeclared server, for one) refuses init naming the option
  and the path, never a silent default and never a first-member-build death.
  The same loaded config now also feeds `rpc_gateway`'s live/realtime door
  (`attach_live`), which used to start from `Config::default()` alone. A
  top-level `[self_hosted]` or `[realm]` table in `mob.toml`, which
  `MobDefinition::from_toml` dropped in silence, is now refused at init on
  both binaries with a message pointing at `meerkat_config_path`
  (`refuse_host_config_tables_in_mob_toml`, exported). `[models.<id>]` is
  not one of those tables: a profile's `model` must be catalogued,
  provider-annotated or defined under the definition's OWN `[models.<id>]`
  to pass the model check (`validate_definition` reads only the definition),
  so a host-config `[models]` row reaches the factory but does not satisfy
  the check. Defaults are unchanged: an exact-pinned host that names no
  file, has no `<workspace>/.rkat/config.toml` beside its `config/` directory
  (`mobkit_gateway` now adopts that file when present), and writes no such
  table builds from the default config exactly as before.

- **Console auth opt-out reaches every surface.** The Python builder gains
  `console_auth_required(required)` (emits
  `runtime_options.console_require_app_auth`) and `console_config(path)` (emits
  `runtime_options.console_config_path`), the two knobs Python hosts, including
  the in-repo memory live driver and the downstream app, had to inject by overriding
  `MobKitRuntime._build_init_params`. The Rust library gains
  `RuntimeDecisionState::local_console(console, bigquery)`, the one exported
  constructor for a console that trusts no identity provider, plus
  `with_modules(..)` for a Rust host that opted out of console auth but still
  runs trusted modules (the state serves `console`, the session-store naming
  and `modules`); both gateway
  binaries now build their keyless decision state through it instead of two
  private struct literals (mobkit_gateway keeps its `tux_local` /
  `runtime_events` session-store naming through the parameter). The default is
  unchanged and fail-closed: `ConsolePolicy::default()` requires app auth, and
  an SDK launch with neither `auth_config` nor `console_require_app_auth =
  false` still serves a console that refuses every request; `rpc_gateway` now
  logs one startup warning naming that condition and the two ways out.
  `validate_trusted_oidc_runtime_config` is exported for hosts that want the
  OIDC snapshot check on an open console.

- **`AgentBuildDraft`, `AgentBuildContext`, `DurableAgentSpec`,
  `ExternalToolDef`, and `AgentCustomizerProtocol` are importable from the
  top-level `meerkat_mobkit` package.** The identity-first customizer
  (`customize_build(context, spec, draft)` with `draft.register_tool`) is the
  per-identity equivalent of `SessionAgentBuilder.build_agent`, but its
  argument types were public only through
  `meerkat_mobkit.identity_first_models` / `identity_first_providers`, so the
  documented per-profile tool path needed a submodule import while every other
  public surface did not. Additive: the submodule paths keep working.

### Fixed

- **Memory completions consume canonical assistant output.** The distiller,
  steward, and hygienist now replace provisional deltas with
  `LlmEvent::AssistantOutput` through upstream `BlockAssembler`, preserving
  final-only, corrected, and empty final output without duplicating text or
  exposing reasoning. `LlmError::PolicyStop` remains a terminal client error:
  no authentication refresh, parse repair, automatic retry, or fallback.
  Output truncation remains distinct from successful completion.

- **Schedule watchdog stalls are attributed only to Active parents.**
  Both gateways' periodic ERROR and boot backlog WARN now count only overdue
  Pending occurrences whose authoritative schedule is Active; the oldest due
  time and lease/claim diagnosis use that same set. Paused and Deleted parents'
  leftover Pending rows remain untouched and are bucketed separately at DEBUG.
  Missing parents and failed/poisoned reads remain explicit incomplete
  observations with their own ERROR diagnostics, never healthy or implicitly
  Active/Deleted. Rust callers matching `ScheduleFiringProbe` must handle the
  new `Incomplete { report }` variant.

- **A retired identity is no longer stuck in `Retiring`
  ([#404](https://github.com/lukacf/meerkat-mobkit/issues/404)).** `retire_member`
  leaves the durable identity registered in `Retiring` (member gone, session
  archived, lease released), but the roster reconciler refused that state:
  every later topology refresh failed with `cannot reconcile_roster_remove
  identity ... in state Retiring`, forever (a 2026-09-05 production incident), and neither a
  send nor the embodiment door would re-materialize it. `Retiring` after
  `retire_locked` IS the retired terminal form (the console's
  `retired`/`RetiredReadable`, `reset_all`'s leaving-fleet classification and
  the delegate sweeper already read it that way), and every lifecycle
  mutation runs under the identity's lifecycle lock, so lock-holders can never
  observe the in-transaction form. The reconciler now removes or re-profiles a
  `Retiring` entry exactly like a `Dormant` one, and `send` / `materialize`
  re-embody it onto the SAME durable session and generation (the operator's
  "retire then send" recovery). `reload_member`'s retire-then-rematerialize
  path shares that door instead of flipping the state to `Dormant` by hand,
  and reports `not_current` for a retired identity. Revival is explicit:
  only an operation addressed to the identity (`send`, `dispatch`,
  `materialize`) re-materializes it, while indirect
  hydration (a peer hand-off's `materialize_reachable_peers`, fleet
  `materialize_all`, the background warm) still refuses a retired identity
  typed (`identity_materialization_failure` with the initiator, as before),
  so a hand-off never silently brings a retired member back.
  `respawn_member` remains the destructive continuity reset; unchanged.
- **Flow Editor: the inline source panel no longer opens empty when a
  read-only render races the editor's own autosave.** The source preview,
  validation and deploy-plan RPCs carry the optimistic draft store guard of
  the registry row the UI last saw. When the autosave triggered by an
  authoring projection had already bumped the store revision but its
  response was not yet applied to that row, the server refused the render
  with `mobkit/mobpacks/source draft revision conflict ... expected N, found
  N+1` even though the submitted document was the freshest authoring state;
  the app then left the panel open and empty behind a "Source render failed"
  sheet. Most visible when opening the Graph-mode source file right after
  the mode switch, whose graph-to-flow sync persists the document (#398).
  Read-only renders now retry once without the store guard, the same one-shot
  remedy authoring operations already had; a real deploy keeps the guard.
  Test-side: the browser-source smoke reports the panel and validation-sheet
  state when a source section never appears instead of a bare 120s timeout,
  and its misdescribed "virtualized editor" scroll probe is gone (the panel is
  a plain `<pre>`).

- **Deliveries no longer wait the whole 600 s admission budget behind a
  stalled mob actor loop.** A 2026-09-04 production incident: one repair-blocked member's inline
  work wedged meerkat-mob's single serialized command loop; the probe paged
  `actor_loop_stalled`, and every console send still queued behind the wedged
  command and was abandoned after exactly `MOBKIT_BRIDGE_ACTOR_ADMISSION_SECS`
  (600 s), five times over fifty minutes. The probe now publishes its verdict
  into a shared `ActorLoopHealth` slot that the session bridge's admission
  path reads: while a stall is open every actor round trip fails fast with the
  typed `BridgeError::ActorLoopStalled { stall_id, .. }` (surfaced as
  `IdentityRuntimeError::ActorLoopStalled`), naming the open stall so the
  refusal joins the paged incident, and a stall that opens while a round trip
  is already parked cuts that wait the instant the probe pages. Deliveries
  resume on the correlated `actor_loop_recovered`. Never `Mob(String)`, never
  routed into member repair.
- **A dead actor is no longer reported as `actor_loop_recovered`.** The probe
  treated any completion of its parked round trip as "the loop drains", but a
  fail-stopped actor closes its command channel, which resolves the parked
  probe with `ActorCommandChannelClosed` (meerkat 0.8.33 launders it into
  `MobError::Internal` text when the phase watch is not terminal). That
  emitted a recovery for a mob that was gone while every later send failed
  instantly. The probe now classifies both channel-closed forms as
  termination: it emits `ErrorEvent::ActorLoopTerminated`, logs at ERROR
  that the process must restart, marks the shared health terminated so
  deliveries fail fast with `ActorTerminated`, and ends (nothing left to
  watch).
- **Repair-blocked deliveries are typed and reloaded, not repaired or
  stringified.** meerkat's `RecoveryRepairBlocked` refusal reached the
  identity runtime as a bare `Internal("bridge deliver: ...")` string and
  could only be retried into the same wall. The bridge now classifies it at
  the boundary as `BridgeError::ReloadRequired` (matching 0.8.33's stable
  "Runtime recovery is repair-blocked" / "registration-authorized cold reload
  is required" fragments; 0.8.34's typed `MobError::MemberReloadRequired`
  slots in ahead of the text match), the identity runtime runs exactly ONE
  automatic `reload_member` under the lifecycle lock and retries the delivery
  once, and a second refusal (or a failed reload) surfaces as the typed
  `IdentityRuntimeError::ReloadRequired` naming the operator path. It is
  never routed to `repair_member_for_delivery` (a destructive retire that
  cannot clear a durability degradation and destroys queued inputs). The
  existing admission-timeout classification is unchanged.
- **SDK-hosted roster callbacks now receive the roster context instead of
  `{}`.** An external SDK user reported, and the maintainer confirmed, that a
  Python or TypeScript `roster(context)` callback behind `rpc_gateway` could
  not read either field the Rust `RosterContext` promises. Root cause was a
  wire-shape mismatch in `GatewayRosterProvider`: it serialised the context as
  the whole `callback/roster_provider/roster` params object
  (`{"mob_definition": ..., "previous_identities": [...]}`), while both SDK
  dispatchers, like the sibling topology and customizer callbacks, read
  `params["context"]`, so the callback saw an empty dict on every call. The
  bridge now nests the context under `context`, matching the envelope the SDKs
  already read, so the callback receives the booted mob definition (as JSON)
  and the identities the runtime has registered (empty on the bootstrap
  resolves, populated on later re-derivations such as `mobkit/topology/query`
  or an edge reconcile). Both SDKs gain a typed `RosterContext`
  (`mob_definition` / `mobDefinition`, `previous_identities` /
  `previousIdentities`) and parse the envelope into it before invoking the
  callback; a bare `{}` from an older gateway still parses to the defaults.
  The regression drives the real `rpc_gateway --persistent` binary over the
  stdin-JSONL host protocol and asserts on the wire that every bootstrap roster
  call carries `context.mob_definition.id` and an empty
  `context.previous_identities`, and that a later `mobkit/topology/query`
  re-asks the roster with the registered identity in `previous_identities`.
  Behaviour change for SDK implementors: the Python callback argument is now
  the frozen `RosterContext` dataclass instead of a `dict` (read
  `context.mob_definition`, not `context.get(...)`), and the TypeScript
  callback receives the typed `RosterContext` interface.

- **The configuration reference now states four rules hosts were discovering
  at spawn time or by reading source.** `docs/reference/configuration.mdx`
  gains a `[profiles.*.tools]` table: every flag defaults to `false`, and
  `comms` must be `true` because Meerkat keys a member's identity, wiring and
  peer messaging on its comms name and refuses `comms = false` (or an omitted
  key) with `profile '<name>' has tools.comms=false; mob meerkats require
  comms=true`, which `mobkit/ensure_member` reports as `-32602` on the worker
  plane and as outcome `broken` on the identity plane. The assertion ledger's
  `state_path` row now says what the file is: a runtime-owned snapshot loaded
  once at bootstrap and rewritten wholesale on every `mobkit/memory/index`
  (host calls and enabled Steward conflict signals alike), not a shared
  append log, with the 4,096-assertion cap and the load-time refusal and drop
  rules. A WorkGraph section documents the `runtime_options.workgraph`
  default, the store placement per launch mode including the memory-backed
  fallback without `persistent_state`, and that the `WorkGraphNamespaceGrant`
  (realm `mob.<id>`, namespace `default`) is issued by the gateway, never by
  the host. The `mobkit_gateway` `identity_roster` init param, shipped
  undocumented, gets its shape and boot-scoped semantics.
  `docs/sdks/typescript.mdx` documents `.onError(...)` beside the gateway
  logging section. Documentation only; no behaviour changed.

- **`mobkit_gateway` exits after SIGINT/SIGTERM even when stdin is still
  open.** With stdin held open (a terminal, or a parent process holding the
  pipe), a signalled `mobkit_gateway` completed its graceful shutdown and then
  never exited: the tokio runtime destructor waits forever for the blocking
  stdin read, which dropping its future cannot cancel. `rpc_gateway` has
  worked around exactly this with an explicit exit on its signal path since
  the stdin adapter landed; `mobkit_gateway` now does the same once cleanup
  is complete. With stdin already at EOF (the container case) nothing
  changes. Found by the new exit-reason subprocess suite, which asserts the
  process is gone after the "shutdown complete; exiting" line.

- **A `session_service()` builder that cannot answer `build_agent` is refused
  at `connect()` instead of being skipped in silence.** `MobKitRuntime._bootstrap`
  registered the builder only when `isinstance(builder, SessionAgentBuilder)`
  held (a `runtime_checkable` Protocol, so this is an attribute check: a bare
  function, a `partial`, or a class with the method misnamed fails it), yet
  `_build_init_params` still sent `has_session_builder = true`, so the gateway
  installed its callback wrapper and the first spawn died later with "no
  SessionAgentBuilder registered", far from the line that caused it. Two
  predicates for one fact. The builder is now resolved once, before any
  gateway process is spawned, and a non-conforming object raises `TypeError`
  naming the contract and the offending type; a conforming builder registers
  exactly as before. Hosts whose builders already conform (the downstream app) see no
  change. `MobHandle.reconcile_edges()`'s docstring no longer claims the call
  needs an `EdgeDiscovery` configured on the builder (there is no such knob;
  the definition-derived wiring policy is installed automatically) and now
  says when it is needed: after `runtime.reconcile()` or an identity-first
  boot, only when the definition declares `auto_wire_orchestrator` or
  `role_wiring`, and not after `ensure_member`, which already reconciles.

- **`rpc_gateway` no longer refuses `[models.<id>]` custom models and
  provider-annotated self-hosted profiles at init.** The `mobkit/init`
  pre-check compared every inline profile's `model` against the static
  `meerkat_models` catalog alone, so anything Meerkat's registry legitimately
  owns but the catalog does not (a `[models.<id>]` row, a
  `provider = "self_hosted"` alias) was refused with `Profile '<p>' uses
  unknown model '<m>'` and the process exited, while `MobBuilder::create`
  would have accepted the same definition; `mobkit_gateway` had no such check,
  so the two binaries disagreed. The check now delegates to meerkat-mob's
  exported `validate_definition`, narrowed to its `UnknownModel` diagnostic
  (catalogued, or defined under the definition's own `[models.<id>]`, or
  provider-annotated: the rule `rkat mob validate` documents), and keeps the
  `Did you mean one of: ...?` catalog hint for dated ids. Only that one
  diagnostic is adopted at init on purpose; the rest of the set stays
  Meerkat's to refuse at create time. Behaviour change an exact-pinned host
  observes: a PROVIDER-ANNOTATED profile is resolvable by Meerkat's rule
  whatever its model string says, so a dated or misspelled id on a
  `provider = "openai"` profile (every profile in the downstream app is annotated) now
  passes init and fails at the first LLM call, where the old catalog check
  refused it at init with the hint. `rpc_gateway` does not add a second
  refusal authority for that case; it logs one WARN per such profile at init
  naming the profile, the model and the closest catalog ids, and for
  `provider = "self_hosted"` warns only when the alias is not registered in
  the host config's `[self_hosted.models]`. Tests cover a custom model, a
  self-hosted alias, a catalogued id, a dated id with the hint, an unknown
  id without one, and a dated provider-annotated id that passes init and
  warns.

- **`build_runtime_decision_state` no longer demands an identity provider for
  a console that has none.** The trusted-OIDC checks (non-empty audience,
  discovery with `issuer` and `jwks_uri`, JWKS with at least one key) now run
  only when `console.require_app_auth` is true; the request-time fail-closed
  check is unchanged, and the release-metadata validation still runs on every
  build. An empty trust manifest is accepted (`TrustedMobkitToml.modules` is
  `#[serde(default)]`); hosts no longer have to write `modules = []`. A
  malformed release-metadata document is still `DecisionPolicyError::TomlParse`
  (the enum is not `#[non_exhaustive]`, so no variant was added) but its
  message now names the release metadata JSON, and the variant's `Display`
  prefix is `parse error:` with the document named in the message, instead of
  labelling a JSON failure a TOML one. Acceptance-widening only: every host
  that built before still builds. Observable to hosts that print the error:
  the `TomlParse` text changed. Unit tests cover each rule in both polarities.

- **A provider failure names itself.** Meerkat has carried `RunFailed` as the
  typed `error_report { class, reason, message }` with no flat `error` string
  since 0.7.0, and nothing in MobKit read it: the Python and TypeScript SDKs
  parsed only `error` (always empty), the console timeline rendered every LLM
  failure as `Failed: error`, the signals rail said "Needs attention", and
  the `HostLoopCrash` ERROR line carried only an event id, so a bad key, an
  exhausted quota and a Gemini 400 were indistinguishable from each other and
  from a hang. The single wire projection (`console_agent_event_payload`,
  which already re-derived the removed `result` string for tool completions)
  now derives `error` from `error_report.message` and `reason` from the stable
  `reason_type` for `run_failed`, mirrors the typed reason's `Display` into
  `error` for `interaction_failed`, and keeps the report beside them, so every
  SSE stream, console frame and event-log row carries both. The SDKs gain the
  report (`RunFailed.error_report` with `error_class`/`reason_type` accessors
  in Python, `RunFailedEvent.errorReport` in TypeScript) and derive `error`
  from it for payloads that predate the projection; the Python
  `UnifiedAgentEvent` now carries the agent `payload` that `query_events` and
  `subscribe_events` were dropping (the events doc already said it must not
  be). The console reads `error_report` first everywhere it renders a
  failure, and `HostLoopCrash` logs class, reason and message. A missing
  provider secret is now a typed wall instead of a heal loop: the mob bridge
  reads Meerkat's `SessionProviderAuthFailure` typed on every spawn door
  (create, resume, reset successor) and surfaces it as
  `BridgeError::ProviderAuthRejected`; the identity runtime parks the
  identity with an operator-visible reason for credential kinds no unattended
  retry can change (`missing_secret`, `unsupported_combination`,
  `missing_required_metadata`, `workspace_mismatch`,
  `host_owned_unavailable`), the same park the host-rejected-build gate uses,
  while kinds that can change out of band (expired, refresh failed, login
  required) stay on the repair lanes. The refusal is typed on the first
  attempt too (`HostRejectedBuild` from the materialize that recorded the
  park, not `Internal` prose), it is read through meerkat-mob's transparent
  `SharedRetirementFailure` / `SharedLifecycleFailure` wrappers (every resume
  failure a joined observer sees is wrapped that way, and so is the collision
  retire before a resume retry), and it survives the admission seam as
  `BridgeAdmissionError::ProviderAuthRejected` instead of being flattened
  back into text. The resume-rejection classifier reads through the same
  wrappers: a wrapped `SessionUnavailableForResume { ArchivedNotRevivable }`
  or `MemberRestoreFailed` classified as `Other` before, so the typed
  archived-not-revivable park never fired for it and the identity
  heal-looped (the production shape on a different door). Exact-pinned hosts
  observe `error` and `reason` keys on `run_failed` payloads and
  `interaction_failed` frames, a populated `RunFailed.error` in the SDKs, a
  `HostLoopCrash` message that names the cause, and a keyless identity that
  parks `Broken` with a reason instead of rebuilding forever.
  `docs/concepts/events.mdx` documents the payload. Rust API change for
  embedders that match `BridgeError` or
  `BridgeAdmissionError` exhaustively (one new variant each).

- **`mobkit/reset_all` now resets every registered identity instead of
  retiring the whole roster.** The console handler chose reset-versus-retire
  by membership in the baseline member-spec slot, which only the
  non-identity-first `UnifiedRuntime::reconcile` populates. On every shipped
  gateway that slot is empty, so each registered identity fell into the retire
  arm: the call answered `reset: []` and `retired_delegates: [every identity]`,
  the timeline carried one `identity_retired` frame per identity and no
  `identity_reset`, and the operator's fleet was gone (reproduced 2026-09-03
  against `rpc_gateway` with two identities; the gateway process itself
  survived, so the reported exit had another cause). The reset set is now
  every identity the identity runtime knows: each goes through
  `reset_tracked` (generation advance, session-bridge requirement, stale-alias
  preflight), and the baseline slot only decides the fate of members with no
  registered identity. Raw delegates and live-only members stay on the retire
  path. Exact-pinned hosts that call `reset_all` previously observed a
  fleet-wide retire; they now observe a per-identity reset with
  `identity_reset` lifecycle frames, and a registered identity on a runtime
  without a session bridge is a typed `identity_reset_requires_session_bridge`
  preflight failure (nothing touched) instead of a silent retire. The
  per-identity `stale_member_cleanup_skipped_after_identity_reset` warning is
  gone from the `reset_all` body (`warnings`, `reset_details[].cleanup_warning`)
  and its `identity_reset` payload: the bridge's successor transition is
  meerkat's respawn, which terminally retires the predecessor row, so the
  warning asserted a cleanup gap that does not exist, once per identity per
  call. `reset_details[]` now carries the successor `agent_runtime_id` and
  `generation` instead. The reset set is also classified by lifecycle state
  before anything is touched: a `Retiring` identity (the entry `mobkit/retire`
  leaves until delete or roster reconcile) is leaving the fleet and is neither
  resurrected nor reported, and a registered identity with no live roster row
  (Dormant under lazy bootstrap, Broken after a failed materialize such as the
  keyless park) is refused typed as `identity_not_resettable_in_state` with
  its `state` named and a caller path in `error`, instead of the
  `MemberNotFound` prose meerkat's respawn produced from inside
  `reset_tracked`; the identities beside it are still reset.
  Gateway-subprocess regression added, plus fail-closed regressions for the
  no-bridge preflight, the Retiring skip and the typed refusal.

- **A turn's terminal frame belongs to that turn.** Session-history backfill
  re-emitted every committed assistant step as an `interaction_complete`
  frame, including tool-only steps (a tool call with no text), whose
  `result: ""` has no live twin: meerkat emits `text_complete` only for
  non-empty text and the run's own terminal carries the final answer. Every
  tool-only step therefore landed on the timeline as an empty completion with
  `source.kind = session_history`, stamped with the SAME interaction id as the
  live turn and a timestamp between the turn's `run_started` and its answer
  (reproduced 2026-09-03 against `rpc_gateway` with claude-sonnet-4-6: one
  empty completion per tool-calling turn). A kind-driven client closed the
  turn before the answer arrived; the reported "later sends complete instantly
  with an empty interaction_complete" is this frame. History never emits a
  text-less `interaction_complete` now: a tool-only step projects as the live
  edge's `tool_call_requested` frames (collapsing with a live twin on the
  tool_use_id, rendering call and result as a pair on a history-only rebuild),
  and a silent step projects nothing. Two attribution defects in the live
  projection went with it: `run_started` matching read a `prompt` field that
  meerkat's `RunStarted` never carried (it carries `input.content`), so a run
  always bound to the queue front regardless of which send it started; and a
  runtime-minted `interaction_complete`, `interaction_failed` or
  `interaction_callback_pending` (peer, flow-step and schedule inputs mint
  their own ids) was attributed to whichever console send was pending. Those
  events now attribute only when `payload.interaction_id` names a reserved
  console interaction; otherwise they project with `interaction_id: null`.
  `interaction_complete` and `interaction_failed` then close the interaction,
  while `interaction_callback_pending` leaves it open: meerkat documents it as
  a pause at an external callback boundary, and the resumed run's frames and
  its eventual terminal carry the same id. Exact-pinned hosts no longer
  observe empty `session_history` completions; hosts that already excluded
  that source kind see no change. Full-runtime two-send regression with a scripted
  tool-then-text LLM, in both live and history-rebuild projection modes.

- **An idempotent replay on the identity-first console door no longer runs a
  second turn.** Resending an existing `idempotency_key` with identical
  content returned the original acceptance (`status: delivered`, instantly)
  AND dispatched a whole new LLM turn whose frames were stamped with the old
  interaction id: `console_send_identity_first` spawned the dispatch
  unconditionally because the reservation gave no fresh-versus-existing
  signal (the member-addressed `send` path returned early on
  `AppendDisposition::Existing` all along). Reproduced 2026-09-03: four
  `run_started` for three sends.
  `MobKitConsoleAggregator::reserve_identity_first_interaction` now returns
  the typed `IdentityFirstReservation` (`Fresh` / `Existing`) and the door
  dispatches only `Fresh`; a same-key different-content resend is still the
  `-32009` idempotency conflict. The per-send live projection the door started
  alongside the always-on event forwarder is gone: it encoded the runtime
  incarnation id into a member alias that does not exist, so it never attached
  and logged one `WARN ... mob member not found` per send. Rust API change for
  embedders that call the reservation directly.

- **`CHANGELOG.md` no longer carries committed merge-conflict markers, and the
  pre-push hook now catches them.** Merge aa28ea02 committed an unresolved
  conflict in the `[Unreleased]` / Added section (the typed per-turn injection
  skip reasons on one side, the `make e2e-live` lane on the other) and PR #381
  carried the `<<<<<<< HEAD` / `=======` / `>>>>>>>` lines to main. Both
  entries describe shipped changes and both are kept; only the three marker
  lines are gone. The markers passed pre-push because `check-merge-conflict`
  scans files only while `.git/MERGE_MSG` exists, so a marker committed inside
  a merge and pushed afterwards was never read; the hook now runs with
  `--assume-in-merge` and a committed marker in any tracked text file fails
  the push. The same pass gained a `bazel-builds-fresh` pre-push hook running
  `node scripts/generate-bazel-rust-builds.mjs --check`: nothing regenerated
  the BUILD files locally, so a new test binary reached CI stale and failed
  its `fmt-lint` job there instead of at the push (PR #393).

- **Prebuilt Linux gateway binaries load on glibc 2.31 and newer.** The
  `*-unknown-linux-gnu` release legs built directly on `ubuntu-latest`
  (Ubuntu 24.04, glibc 2.39) and nothing read the produced binary, so the
  v0.8.30 `mobkit-rpc-gateway` and `mobkit-gateway` archives carried a hard
  `GLIBC_2.39` version requirement (`pidfd_spawnp`/`pidfd_getpid`) and the
  loader refused them on Debian bookworm (2.36), Ubuntu 22.04 (2.35) and
  bullseye (2.31); only trixie and Ubuntu 24.04 could run them. The Linux legs
  now build inside `docker.io/library/buildpack-deps:bullseye`, the image
  meerkat's release lane uses, so both products ship one 2.31 floor, and the
  new `scripts/check-linux-release-binary-portability.sh` reads every produced
  binary before packaging and fails the release on any glibc reference above
  the floor or any dynamic OpenSSL dependency (the gateway's TLS is rustls).
  The Linux cache key names the container so a host-built target directory is
  not restored into it. macOS and Windows legs are unchanged. Hosts that
  download the Linux archives gain the wider set of loadable distributions and
  observe no other change; the quickstart now states the floor.

- **The Flow Editor's Agent Editor no longer labels the peer description
  "SYSTEM PROMPT".** The field it edits is the profile `peer_description`:
  roster text other members read through the `peers` tool and peer-added
  notifications, never part of the member's own prompt, which meerkat
  assembles from the profile's `skills` (`[skills.<id>] source = "inline"` or
  a path). Text typed under the old label therefore never reached the model.
  The section title now reads "PEER DESCRIPTION", the placeholder says what
  the text is for and where the prompt actually lives, and the starter
  member's own description no longer promises a prompt. The "APPLY SKELETON"
  button's behaviour is not changed: the bundled client's
  `memberPromptSkeleton` still inserts a second-person mandate template with
  operating rules ("You are <name>, a member of a Meerkat mob...") into this
  peer-facing field, so its tooltip now says exactly that and tells the author
  to rewrite the text as a description before exporting; replacing the
  template with peer-facing text is a client bundle change and is not in this
  release. The editor document key `systemPrompt` (and its
  `system_prompt` import alias) is unchanged, so saved editor documents and
  the bundled client keep round-tripping; the mapping is documented at the
  projection and export sites. MobKit's own test fixtures modelled the same
  mistake with an inert profile-level `system_prompt` key that meerkat
  silently drops; they now carry their prompts as inline skills, or omit them
  where no turn reads a prompt.

- **The durable-callback health projection no longer fails once per second
  with `Unsupported store operation: list_runtime_delivery_authorities`.**
  Production-like `rpc_gateway` runs with an SDK-hosted continuity store logged
  `durable callback health projection failed ... list_runtime_delivery_authorities`
  at WARN on every tick of the 1 Hz delivery driver, forever, and published the
  detached-jobs health as `degraded` (`job_health_projection_failed`) the whole
  time. Root cause: `runtime_inbox_backlog` is read through meerkat's
  `RuntimeDeliveryInbox::pending_delivery_total`, whose only cross-runtime
  store verb is `RuntimeStore::list_runtime_delivery_authorities` (trait
  default: a typed `Unsupported` refusal). Both gateway composition paths hand
  the detached-job runtime the epoch-tracking runtime-store facade
  (`SessionStoreBackedRuntimeStore`), and that decorator forwarded the other
  three delivery verbs but not this one, so every tick hit the default. The
  facade now forwards it (plus the two authority reads whose defaults route
  through `session_authority_ops()`, so every reachable `RuntimeStore` method
  on the decorator is an explicit choice). The projection also types the
  backlog read instead of stringifying it: a store that answers `Unsupported`
  yields a `RuntimeInboxBacklog::Unsupported` outcome that is announced at
  WARN once (then DEBUG), publishes `runtime_inbox_backlog: null` with
  `runtime_inbox_backlog_reading: "unsupported"`, and folds meerkat's
  `Unreadable` rung into the status rather than fabricating a zero; the `u64`
  `jobs/health` wire field refuses with a typed error rather than publishing
  `0`. Every other failure (read error, corrupt authority) keeps its own text
  and still surfaces on every tick. `scripts/verify-session-service-decorators.py`
  now checks `RuntimeStore` decorators the same way it checks
  `MobSessionService` ones (pinned meerkat-runtime trait, 79 methods,
  `commit_prepared_session_boundary_with_fence` exempt with meerkat's own
  decorator-opt-in reason) and fails on the pre-fix facade. Regressions: one
  submitted delivery counts as 1 through the facade and the inner store alike;
  the detached-job runtime over the facade projects `ok` with backlog `0` on
  repeated ticks; a store refusing the read projects `unreadable`/`null` with
  exactly one WARN across five ticks; a store failing the read surfaces its own
  error on every tick.

## [0.8.31] - 2026-09-04

### Changed

- **Pinned to Meerkat 0.8.33.** All exact Meerkat dependency sites move to
  `=0.8.33`; this release pairs with Meerkat 0.8.33, which makes the
  reopened-gateway resume path wait for a slow stale-session teardown instead
  of failing with `UnregisterInProgress`.

### Added

- **Identity-first boot registration now says how long each identity took.**
  `register_persisted_continuity_owners` logs one INFO per identity with the
  three step durations (roster resolve, continuity lookup, owner publish) and
  one summary line. On 2026-09-03 a production fleet's registration phase went
  from 49 s to 170 s for 17 identities with this code byte-identical, and the
  loop logged nothing, so nobody could name the slow step from the logs. Now
  the first launch does.

- **A committed->durable reconciliation now reports its cost when it finishes.**
  The runtime-authority freshness probe already logged when it started one;
  it now logs completion at INFO with the durable and committed
  (rewrite generation, message count) pair and `elapsed_ms`. On 2026-09-03 a
  production fleet spent 171 s across 16 such reconciliations before
  ResumeLifecycle with nothing at INFO to say so; the reproducer had to run at
  DEBUG to see it.

- **Autonomous-host members no longer lose the whole turn to memory injection.**
  meerkat refuses injected context on `runtime_mode = "autonomous_host"`
  ("autonomous inbox delivery carries no user-channel work boundary"), and
  that mode is meerkat's default when a profile omits `runtime_mode`. On 0.8.30
  a send through the identity door to such a member with per-turn injection on
  was a hard error. MobKit now checks the member's effective runtime mode at
  the delivery seam - resolved exactly as meerkat resolves it at spawn: the
  roster override, else the profile's `runtime_mode`, else the default (a
  first cut read only the override and wrongly skipped memory on `turn_driven`
  profiles; the real-API lane caught it before any release) - and skips recall
  with the typed reason
  `runtime_mode_autonomous_host` (first occurrence per identity at INFO), so
  the turn proceeds without ambient memory. Defang still runs. Ambient per-turn
  memory therefore requires `runtime_mode = "turn_driven"` until meerkat's
  carrier change lands with the peer/comms work. Real-API lane test added.

- **A per-turn memory injection that injects nothing now says why.**
  `RecallCoordinator::inject_for_turn_classified` returns a typed
  `TurnInjection::Skipped(reason)` with one of five reasons
  (`per_turn_injection_off`, `contextual_with_empty_query`,
  `budget_exhausted`, `no_records`, `nothing_renderable`), and the untyped
  wrapper the dispatch door uses logs the first skip per identity and reason at
  INFO with the effective config, then DEBUG. Before this, the injection ledger
  recorded only what was injected, so "off", "no memory" and "budget spent"
  were the same absence; the downstream app's two months of zero turn-surface rows would
  have named themselves on the first turn.

- **A real-API end-to-end lane (`make e2e-live`).** Boots the real
  `rpc_gateway` through the Python SDK, runs real Anthropic turns, and asserts
  positive observables that only exist if the whole chain worked: a
  surface=turn injection-ledger row when `per_turn_injection` is omitted (with
  an explicit-off negative control in the same run), a hydrated
  `routing_status` whose `session_provider` matches the profile declaration,
  and a non-empty `resolved_tools` set read after the turn completes. The lane
  is skipped unless `MOBKIT_E2E_LIVE=1`; once requested, a missing key or
  binary is a failure, never a skip. Nightly workflow `e2e-live.yml` (needs the
  `ANTHROPIC_API_KEY` repository secret; fails loudly without it).

### Fixed

- **The pre-resume runtime-authority probe no longer serialises documents or
  re-digests transcripts, and the 17 prewarm probes run concurrently.** A
  real-process sample on the 0.8.31 candidate put ~2.2 s per member into the
  freshness probe - two full serialisations of a ~1.2 MB session to feed an
  envelope byte compare, plus one transcript digest to re-derive a revision the
  head row already stores - run serially for 17 members, ~35 s before
  `ResumeLifecycle` (0.8.28: 48.7 s). The probe now takes a slim row's rewrite
  generation and transcript revision from its head row, compares envelope
  facts typed (version, creation time, metadata minus head-owned keys) with no
  serialisation, and `prewarm_persisted_runtime_authority` runs up to 8 probes
  at once. The head-row revision is used only for current-version heads: a
  released envelope-v2 head stores the released-format rows digest, which is
  not comparable, so released realms keep the full transcript digest.

- **Both session-service decorators now forward every reachable defaulted
  `MobSessionService` method, and CI proves it.** The 2026-09-03 audit behind
  the archive-lane fix found nine more defaulted methods neither decorator
  forwarded; five are reachable on main (`observe_persisted_session_authority`,
  `discard_live_session_actor_after_durability_reload_required`,
  `persisted_session_authority_read_cost`, `forked_participant_source_runtime`,
  `revalidate_session_resume_authority`), the other four sit behind meerkat's
  `experimental-gpt-live` feature which main does not enable. Their defaults
  are refusals or downgrades (read cost "Unsupported", fork source `None`), so a
  missing forward was a silent behaviour change. Both decorators forward all
  five; the regression drives each through both wrappers into a recording
  inner service. `scripts/verify-session-service-decorators.py` (fmt-lint)
  parses the trait from the pinned meerkat-mob source, reads the features Cargo
  enables, and fails when any production decorator lacks a reachable method, so
  the next defaulted method meerkat adds fails CI instead of production.

- **The runtime-authority freshness probe no longer re-walks a reconciled row
  on every boot.** The head-canonical durable read is slim by design and
  reports rewrite generation 0 on the `Session`, while its head row carries
  the adopted generation. The probe compared the Session's 0 against the
  committed generation, so every identity with a compaction history ordered
  "behind" on every launch after the first projection landed and re-ran the
  full committed->durable chain walk (production: 16 serial walks, ~170 s,
  before `ResumeLifecycle`, then the 30 s explicit-resume watchdog). The probe
  now reads the durable generation from `SessionHead::rewrite_count`, which
  advances 1:1 with each adopted commit, and compares a slim row's envelope
  like for like so the equal arm does not re-project forever.
  The regression drives the downstream app's A->B shape in process and asserts leg A
  walks once and leg B walks zero times.
- **The committed->durable projection no longer walks the rewrite chain for a
  row already at the committed head.** With the generation read fixed, the
  probe's equal arm still found the durable row's persisted envelope differing
  from the committed document and projected it - silently, and the projection
  ran all three rewrite-chain provers (extending-chain, inverse-append,
  durable-behind) before concluding nothing was missing. A real-process sample
  of the gateway put 3732 of 3766 frames in `durable_behind_prefix_chain`,
  entered from MobKit's own identity-first bootstrap on the main thread, one
  identity at a time. The projection now short-circuits the provers when the
  durable revision equals the sealed head and the head generation equals the
  committed generation (their conclusion, in O(1)) and still saves the
  envelope. "At head" means the committed session's current transcript
  revision, not the revision at the last rewrite commit: every live row has
  messages appended after its last compaction. Envelope currency compares
  normalised documents (`updated_at` is a write timestamp, not debt; the
  three metadata keys the head carries in its own columns - transcript
  history, rewrite-prefix authority, realtime transcript state - are a
  representation difference, not debt) and the
  equal arm logs at INFO when it projects, naming the differing keys with
  metadata one level deep. Regression: envelope debt on a row at head, with
  messages appended after the compaction, clears with zero chain walks.

- **Destructive resets during a recovered-operation handoff no longer fail
  typed.** meerkat-mob's `MobSessionService` carries a defaulted
  `archive_with_mob_lifecycle_authority_under_runtime_turn_boundary_and_hook_before`
  that refuses whenever a post-commit hook is present. MobKit's two session-service
  decorators forwarded the other archive variants but not this one, so the
  provisioner's hook-bearing retirement lane (taken only when a recovered
  operation must be rebound) hit the default and the reset failed with
  "session service cannot run a pre-retire archive hook". Both decorators now
  forward it; the regression proves the hook reaches the inner service and
  fires there. Latent since 0.8.28; surfaced as a flake in
  `live_reset_takes_a_turn_and_shuts_down_within_horizon`.

- **SDK identity-first sends now run MobKit delivery preparation exactly once.**
  `handle.send()` / `mobkit/send_message` previously let the stable identity
  roster row win as a raw mob member, bypassing `prepare_member_delivery` and
  therefore ambient recall, inbound defanging, injection budgets, and the turn
  ledger. Active identity-owned rows now route through `IdentityRuntime`; a
  real-runtime regression proves one matching recall and one `surface=turn`
  ledger offer for the delivered session while raw workers retain roster
  semantics.

- **The gateway no longer carries its own default for
  `agent_memory.per_turn_injection`.** The library default has been `budgeted`
  since 2026-07-01, when injected-context delivery became echo-safe, and the
  boolean form (`agent_memory: true`) followed it. The object form did not:
  `rpc_gateway` substituted a literal `"off"` when the field was omitted, so
  every SDK client that passed an options object and trusted the documented
  default got zero turn-surface injections. The downstream app measured the consequence
  as 10,283 build-surface injections and zero turn-surface ledger rows over the
  lifetime of its store. The parser now derives the fallback from
  `AgentMemoryPerTurnInjection::default()`, so one default governs both forms
  and the next change cannot fork them; a non-string value is a typed refusal
  instead of silently meaning off. The regression test is the downstream app's exact call
  shape. The design document's "off by default" text, which predated the
  library change, is corrected.

## [0.8.30] - 2026-09-03

Pairs with Meerkat `0.8.32` - unchanged from 0.8.29. All 25 exact dependency
pins are byte-identical to the 0.8.29 release commit; this train carries no
upstream movement.

The `v0.8.29` tag exists on `0fd01efe` but was never published to any registry:
its release run was refused for arriving before exact-main CI, and the recovery
run was cancelled once the two items below were judged part of the release. The
tag is left in place rather than rewritten. Consumers pin `0.8.30`.

### Fixed

- **`mobkit_gateway` now refuses `runtime_options` instead of dropping it in
  silence.** `runtime_options` is an `rpc_gateway` protocol (`demo_llm`,
  `max_sessions`, `identity_bootstrap_mode`, `experimental_live`, and since
  0.8.29 the definition-epoch ceremony's `declare_spec_update`). `mobkit_gateway`
  never read the object and its init params deserialize without
  `deny_unknown_fields`, so serde discarded it and every option inside it. After
  0.8.29 that meant an operator who declared a spec update against this binary
  booted straight into the pinned-definition refusal the declaration exists to
  clear, with no sign it was ever read. Both SDKs let `gateway_bin` /
  `gatewayBin` select either binary, so the wrong-binary case is reachable. The
  object is now refused with a typed error naming `rpc_gateway` and the option
  family, so the refusal redirects rather than dead-ends. Mutation-proven:
  deleting the refusal block turns its test red. (#378)

### Tests

- **A resumed member must materialise identically to a fresh one.** The
  cold-restart continuity test now captures the `LlmRequest` each member sends on
  its first turn after `Created` and after `Resumed`, and diffs model, system
  prompt content, and the tool-name set, reporting field names rather than
  counts. Vacuity guards refuse an empty prompt or tool set. Mutation-proven
  three ways: an extractor blind to tools, an extractor blind to the prompt, and
  a boot-2 definition on a different model each turn the test red, the last
  naming `model: fresh="gpt-5.5" resumed="gpt-5.4"`. (#377)

## [0.8.29] - 2026-09-03

Pairs with Meerkat `0.8.32` (annotated tag object
`5c4bb88c9fea0183ad161fd0114443f31f1e9aa1`, commit
`ed9fb06b2d93089ae2ede51874a136807f479108`) across all 25 exact dependency
pins.

### Changed

- **Declared mob-definition updates now advance Meerkat's canonical definition
  epoch.** `runtime_options.declare_spec_update` routes through the typed
  `MobStorage::update_definition(expected_revision, definition)` CAS seam instead
  of rewriting the spec projection. Exact retries converge, stale revisions and
  authority/projection disagreement remain typed refusals, and an added profile
  survives runtime restart. The composition manifest remains immutable creation
  provenance rather than a second mutable definition authority. Python hosts can
  invoke the one-shot ceremony through
  `MobKit.builder().declare_spec_update(expected_revision)`, using the builder's
  `mob(...)` or `mob_inline(...)` definition as the replacement. Authoritative
  resumes are bound to a storage-minted definition snapshot, so a concurrent
  epoch between preflight and replay refuses instead of booting unsupplied
  content. **BREAKING for direct Rust callers:** the `spec_update_ceremony`
  functions now require `&MobStorage` instead of a database path, and
  `mob_composition_manifest::verify_before_resume` requires the canonical
  definition witness. A path cannot prove whether event and projection stores
  share Meerkat's atomic bundle, so preserving the old signatures would make
  split/custom storage capable of advancing only one side of authority.

- **Ordinary inline profile model/provider declarations no longer become
  synthetic resume overrides.** Durable routing decisions, including a realized
  permanent `brain_swap`, now survive cold restart unless the host explicitly
  lists `model` or `provider` in `resume_overrides`. Explicit masks remain
  operator authority and are still completed to a coherent model/provider pair.
  Definition-epoch updates that only add profiles therefore leave existing
  members' durable routing identities unchanged.

- **BREAKING for hosts that supply `application_tool_policies`: a compiled
  artifact whose `member_identity` values name no roster row is now refused at
  install instead of accepted.** The grant lookup is exact string equality
  against `MobMemberBinding.member`, which Meerkat sets from the member's
  `AgentIdentity` - the ENCODED DURABLE identity. Nothing decodes either side.
  An artifact compiled against any other id space therefore matches no member at
  all, and since `validate()` forces `default_deny` true, that policy denies
  every tool for every member the moment it binds.

  Two id spaces are now refused by name, each with the id to compile instead:
  the encoded agent RUNTIME id (`mk--rt_cdomain_ccalendar_c0`) and an
  unencoded durable identity (`domain:calendar`). Both were reachable before
  and neither failed loudly.

  This is fail-closed in a direction that can stop a boot: a host holding a
  wrong-id-space artifact upgrades from "gateway starts, policy inert" to
  "gateway refuses to start". That is deliberate. The state it replaces is a
  126KB artifact that is reviewed, deployed, and either inert or total, with no
  surface that distinguishes those two - because `Allow` emits no observation
  and only `Deny` reaches the observer, so an operator's only evidence is an
  absence that reads identically either way.

  Found by the downstream app on 2026-08-31: 17 members compiled as the encoded runtime
  id, `default_deny = true`, 120 grants each, and three ungranted tools
  executing successfully with zero denials fleet-wide. Confirmed against their
  live continuity store rather than only against source - all 17 stored
  `MobMemberBinding.member` values are the encoded durable identity, and all 17
  declared tokens diverge from them. The operator deployment independently hit the same
  two-id-space trap the same night on event subscription, where the wrong space
  yields silence rather than an error. `member_comms_id.rs` already documented
  the conversion as producing "binding detail that no longer names a roster row
  at all" and warned that a site keeping it "does not fail loudly". This is the
  place where it now does.

  The two defects were cancelling, which is why warning would have been the
  wrong shape: with nothing Provider-bound the wrong ids were harmless, and a
  well-meaning "turn the policy on" change would have denied every tool for
  every identity in the household at once.

- **`mobkit_gateway` refuses `application_tool_policies` instead of silently
  dropping it.** Only `rpc_gateway` builds the application tool-policy
  registry. `mobkit_gateway`'s `InitParams` carries no `deny_unknown_fields`, so
  the key was discarded by serde: a host that compiled, reviewed and deployed an
  artifact against that binary got no registry, no error and no warning. That is
  the same "looks deployed, does nothing" state as above, one layer up and on
  the other binary. The refusal names `rpc_gateway` as where the parameter takes
  effect, so it is a redirection rather than a dead end.

### Added

- **`UnifiedRuntime::shutdown_horizon()` makes the shutdown budget discoverable
  to embedders.** The number was already published, documented, and
  arithmetically asserted - and on 2026-08-31 two independent consumers still
  bounded it far too tightly, each by applying an ordinary host stage timeout:
  30s against the Python SDK's 337s, and 120s against this crate's 312s. Neither
  was careless. A `pub const` in a module they had no reason to open was not
  discoverable at the point of use.

  The stdio surface has always advertised its horizon over the wire via
  `stdio_shutdown_horizon_ms`. Embedders now have the equivalent, reachable from
  the value they already hold and are about to shut down:

  ```rust
  tokio::time::timeout(runtime.shutdown_horizon(), runtime.shutdown()).await
  ```

  The doc states the consequence that makes it matter: abandoning the wait does
  not stop the work. `shutdown()` keeps running while the caller proceeds to
  exit, which is how cleanup gets killed mid-flight - and is why an
  executor lease could still be held after a "graceful" shutdown even with the
  0.8.28 release fix present.

  Guarded by a test that fails if the horizon collapses, because the failure is
  asymmetric: an embedder who sizes their wait on an advertised figure that has
  silently shrunk is stranded *and* more confident, precisely because they asked.

### Fixed

- **`identity_first_send_does_not_park_mob_actor_until_turn_completion` no
  longer hard-fails on loaded CI runners.** It budgeted 250ms against a 2s turn
  and failed twice in two days with `Elapsed` - the missed-deadline signature of
  a load flake, not of a parked loop - on PRs that changed no Rust at all. The
  binary has no retry group, so each occurrence was a red build.

  The property under test is "this acks at INGRESS rather than at turn
  completion", and its discriminator is the RATIO: a parked actor blocks until
  the turn finishes, so the test has teeth only while the budget sits well below
  the turn. The test never waits for the turn to end - it stops the mob while it
  is still running - so lengthening the turn costs no wall clock. Now 3s against
  a 30s turn: the same discriminator with ten times the margin, and the test
  still completes in ~0.1s.

  The relationship is now named (`TURN_DELAY`, `INGRESS_BUDGET`) and asserted in
  the test itself, so a later edit cannot quietly shrink the turn or grow the
  budget until it passes for everyone including a parked loop. Verified by
  collapsing the ratio, which fires the guard with its own explanation.

### Added

- **The identity-first flow barrier now names itself in the log.** Triggering a
  flow on an identity-first runtime materializes the ENTIRE registered identity
  set before the run id is minted, and those spawns were recorded as ordinary
  fleet materialization with nothing linking them to the flow that caused them.
  A consumer measuring a slow trigger saw a materialization burst and no
  connection between the two - which is how a 358-second trigger read as a hang
  rather than as work (the operator deployment, 2026-08-31: 144 spawns, 17 to 161, their entire
  identity count).

  The barrier now logs its cause on entry and its `materialized` count plus
  `elapsed_ms` on completion, so the most expensive consequence of triggering a
  flow is visible as a consequence of triggering a flow.

  **This is observability only; the cost is unchanged.** The scope cannot be
  fixed in MobKit: `FlowTargetProvisioner` is `Fn() -> ...` with no flow
  argument, so the callback cannot know which flow, which roles, or which
  identities are actually required. Upstream's own
  `ensure_flow_targets_provisioned` scopes correctly by role because it has the
  flow; this hook does not. Narrowing it needs the upstream signature to carry
  flow context, and the timing - upstream invokes the barrier before minting the
  run id - is upstream's ordering. Both are filed there rather than worked
  around here.

- **A never-run queued input is pinned against being reported as completed.**
  All three consumers had reported NULL coverage on this clause. The test drives
  a real wedged member, queues an input behind it, destroys the runtime with a
  retire, and asserts on the durable row.

  Writing it corrected the obligation as filed, in two ways worth recording.
  `retire_runtime` does **not** abandon a queued input - it leaves it `Queued`
  with no terminal outcome, deliberately, because identity-first repair carries
  pending ingress into the healed successor; abandoning there would re-introduce
  the defect the neighbouring test guards. And the abandoning teardowns
  (`Reset` / `Stopped`) are unreachable in that state, since reset is
  guard-rejected from `Running` and a wedged turn is exactly what holds the
  runtime `Running`. `InputAbandonReason::NeverExecuted` itself has one
  production producer - a run created then refused - which needs a store-commit
  failure and is not drivable from a black-box harness, so it remains covered
  only by meerkat's driver-level tests. This is deliberately **not** recorded as
  covering that clause.

  Mutation-checked, which changed the test: flipping the expected phase and
  selecting the wrong input id each turn it red, but neutering the
  `terminal_outcome.is_none()` assertion did **not** - it was redundant given
  the phase check, and the `Consumed` guard is vacuous on a path where the
  outcome is always `None`. The redundant assertion was removed and the
  remaining guard is labelled in-place as a consistency check rather than live
  coverage, so nobody reads it as protection it does not provide.

### Fixed

- **The console copy button lied on plain http, and one of the two still did.**
  `navigator.clipboard` exists only in a secure context - https, or localhost -
  and the console is routinely served over plain http on a LAN address, where it
  is `undefined`.

  ChatPane called `navigator.clipboard.writeText(...)` directly; it threw before
  reaching the clipboard, into a `catch {}` that existed to keep a clipboard
  error from breaking the hover affordance. The button did nothing, silently,
  for every LAN user.

  The tool-call button on the shared conversation path was worse. It ran
  `navigator.clipboard?.writeText(text).catch(() => {})`, and with `clipboard`
  undefined the optional chain short-circuits the **whole** expression - `.catch`
  is never reached, nothing throws - after which `setCopied(true)` ran
  unconditionally. It showed a checkmark having copied nothing, so the user
  pasted whatever was already on the clipboard. A silent lie is worse than a
  visible failure.

  Both now use `copyTextToClipboard`, which already existed in
  `packages/console-components/src/shared.ts` with an `execCommand` fallback and
  a boolean return. It was package-internal and unexported, which is how a
  second implementation came to be written elsewhere: a helper you cannot import
  is a helper you rewrite. Exported rather than duplicated, so one owner remains.
  Both buttons now show an explicit failure mark with matching `aria-label` and
  `title`.

  Also fixed: both leaked their reset timer into an unmounted tree, where React
  reads `window` before discovering the update is a no-op and therefore throws
  rather than no-opping.

  Tested with real clicks against a navigator carrying no `clipboard` - the
  plain-http shape, not a clipboard that rejects, which is a different branch and
  was never the bug. The secure-context trigger is in the test names because the
  defect is invisible on localhost, which is exactly where a copy button gets
  tested. Mutation-proven: restoring the original call plus the unconditional
  success mark turns both new tests red.

  `packages/flow-editor-components` carries the same raw call in two places and
  is deliberately untouched: it declares no dependency on `console-components`,
  so reusing the helper there would add a cross-package dependency purely to
  share a utility. It needs a decision about where a shared helper lives.

- **A steer no longer pins to the bottom of the transcript while its own turn
  streams over it.** Reported from a live console: a message typed while the
  agent was working stayed at the bottom while output produced after it kept
  appearing above. In the report a tool result stamped `22:24:21` rendered above
  a message stamped `22:23:54`.

  The transcript was not sorted by timestamp. `sortFramesForTranscript` compared
  the two frames' interaction GROUP START times and returned before own
  timestamps were ever consulted, so frames in different interactions never had
  their own timestamps compared at all. A steer is minted as its own interaction
  while the running turn's in-flight frames keep the original id - the active
  interaction is only reassigned by `run_started`, and a steer merely joins the
  pending queue - so the turn's earlier group start pinned everything it
  produced afterwards above the steer, permanently.

  Group-start ordering keeps an interaction's frames contiguous, which is right
  whenever interactions do not overlap and wrong the moment they do. Ordering by
  own timestamp costs nothing in the ordinary case, because contiguity falls out
  of chronology for free when interactions do not overlap.

  The same-interaction start-event rule deliberately still runs BEFORE the
  timestamp comparison: promoting own-timestamp above it broke an existing test
  by name, which pins that within one interaction an `interaction_started`
  stamped later than a `text_delta` must still render first. Only the
  cross-interaction group substitution is removed. The group timestamp also
  remains the fallback for frames carrying none, since reasoning frames often
  arrive without one and would otherwise render at the end of the turn.

  Fixed in both copies; `packages/console-core/src/adapters.ts` carries a
  structurally identical twin that differs only by a trailing comment, so a
  byte-level search for the duplicate does not find it.

  Neither property had any test coverage: nothing constructed two interleaved
  interactions and asserted rendered order. Two tests added, one for the
  reported defect and one guarding the opposite regression so this cannot later
  be "fixed" by abandoning grouping. Mutation-proven: restoring the group-min
  comparison turns the steer test red and only that test.

### Added

- **CI now runs the TypeScript SDK suite.** It ran nowhere. 705 tests across 20
  files existed and gated nothing: no job touched `sdk/typescript`, and the only
  Makefile reference is `publish-dry-run-typescript`, which builds and packs
  without testing. A wrong RPC method name, or a parser reading camelCase where
  the gateway sends snake_case, reached npm behind a typecheck that cannot see
  either.

  Found by applying the reviewer's own standard to this release's TypeScript
  contract test: having just been asked to prove the gateway-backed Python test
  executes rather than skips, the same question asked of the new TS test
  produced "it never runs in CI at all".

  The job runs `validate` (typecheck, then build, then test) rather than `test`,
  because most of these tests import the built `dist/` and `test` alone fails
  with `ERR_MODULE_NOT_FOUND`. It is wired into `gate`'s result comparison, not
  merely into `needs` - with `if: always()` a job listed only in `needs` is
  advisory and cannot fail the suite. Two contract tests in
  `scripts/test_ci_workflow.py` pin both halves, and both are mutation-checked:
  downgrading the step to `typecheck`, or dropping the job from the result
  comparison, each turn one red.

- **`mobkit/identity/routing_status` exposes meerkat's typed model-routing
  status per identity.** The result is
  `meerkat_core::image_generation::SessionModelRoutingStatus` verbatim
  (`WireSessionModelRoutingStatus` is a type alias to it), flattened under
  `identity` and `session_id`. MobKit declares **no mirror type**: a second
  declaration of an upstream shape is a second place for it to drift, so the
  payload cannot disagree with meerkat's contract.

  The fact this carries that nothing else does is `session_provider`, the typed
  provider of the session's current LLM identity. Meerkat documents re-deriving
  a provider from the effective model string as silently wrong for models owned
  by a custom `ModelRegistry`; an absent `session_provider` means the machine
  has no hydrated session LLM identity yet, and is **not** an invitation to
  re-derive one.

  Wired on **both** dispatch planes - the stdin JSON-RPC router in `rpc.rs` and
  the HTTP console's own match in `http_console.rs` - plus the console ABAC
  classifier (`ACTION_AGENT_VIEW`, the same grant as `identity/resolved_tools`),
  both capability advertisements, and the Python and TypeScript SDKs. The
  classifier entry is not optional bookkeeping: an unmapped console method
  returns `None` from `console_rpc_access_requirements`, which makes the access
  gate short-circuit to *allowed* and the capability filter advertise it to
  everyone. Omitting it produces no compile error and no test failure.

  **This method requires a resolved session.** Identity-first materializes an
  identity without activating it, so an identity that has been materialized but
  never addressed has no session and therefore no routing status. That is
  structural - the status is per-session machine state - and it is the expected
  state after a restart, not a defect. A fleet sweep must address before reading
  and label its coverage post-address.

  Failures carry a machine-readable `reason` so a sweep can classify an identity
  rather than only fail it. Each reason is derived from a fact MobKit actually
  observed, never inferred from a single upstream error:

  | `reason` | Derived from |
  |----------|--------------|
  | `runtime_unsupported` | the session service exposes no runtime adapter |
  | `no_current_session` | the roster reports no current session (see below) |
  | `member_lookup_failed` | the mob could not resolve the member at all |
  | `session_not_held` | a session resolved **and** meerkat answered `NotFound` |
  | `upstream_read_failed` | a session resolved and the read failed some other way |
  | `invalid_identity` | no usable identity was supplied |

  `no_current_session` is named for the fact observed, not a cause inferred from
  it. MobKit's `member_status` answers an unknown member with a well-formed
  "unknown" status carrying no session rather than an error, so this one
  observation covers **both** an identity materialized but never addressed (the
  normal state at boot) **and** an identity that does not exist at all,
  including a typo. This surface cannot distinguish them, so a caller sweeping a
  fleet must assert it received a status for every identity it expected rather
  than merely that nothing raised. Distinguishing them would need a roster read
  that reports absence as absence; that belongs to `member_status`, and is
  deliberately not papered over here. An earlier spelling of this reason
  asserted the first case and was wrong for the second.

  `session_not_held` is matched on the `RuntimeDriverError::NotFound` **variant**,
  never on message text. Because that error is `#[non_exhaustive]`, every other
  upstream failure - `NotReady`, `Destroyed`, `RecoveryCorruption`, and anything
  added upstream later - lands in `upstream_read_failed`, which deliberately
  diagnoses nothing. A consumer escalates on `session_not_held`; widening it to
  mean "the read failed" would fire that escalation on states that are not
  missing sessions at all.

  Mutation-proven rather than merely green, on every surface. On the Rust side,
  removing either dispatch arm, the typed error payload, the ABAC classifier
  entry, or the `NotFound` narrowing each turns a specific test red. The
  TypeScript contract test is executed rather than typechecked, and detects a
  wrong RPC method name, a parser reading camelCase instead of the wire's
  snake_case, and a coerced-away `session_provider`. The gateway-backed Python
  test was confirmed to **execute** rather than skip, by building `rpc_gateway`
  and then breaking the SDK method name to watch it go red. An earlier sweep was **discarded** as
  worthless - its three "failures" were rustc ICEs in `identity_first/runtime.rs`
  under incremental compilation, so the tests never ran. A build that breaks is
  not a test that detects.

## [0.8.28] - 2026-08-29

Pairs with meerkat `0.8.31` (tag `v0.8.31`, commit
`83d5be6d75ebdf9a54106bf7b102617fb8114669`) - unchanged from 0.8.27. All 25 pin
sites are byte-identical to the 0.8.27 release commit; this train carries no
upstream movement.

The backlog this release clears was known during 0.8.27 and left out of it. The
0.8.31 deadline was meerkat's, not MobKit's, so holding these items back bought
nothing.

### Fixed

- **A graceful gateway exit now releases the schedule executor lease.** Both
  gateway binaries bound `_schedule_host` and relied on `Drop`. Dropping a
  `ScheduleHostHandle` only *signals* the supervisor; only
  `ScheduleHostHandle::shutdown().await` calls
  `driver.release_executor_lease()`, and tokio gives no guarantee the woken
  supervisor is polled between main's completion and runtime teardown. Measured
  on a production store: `owner_id`, `lease_token`, `acquired_at_ms` and
  `expires_at_ms` all still set 57s past **gateway exit**. (That observation was
  originally reported as following a graceful shutdown; its owner has since
  corrected it. The exit was driven by stdin EOF, so it observed the `Drop` path
  rather than a graceful one. The row reading is unaffected, and both paths run
  the same teardown in `main`. Direct evidence for the graceful path is this
  release's own mutation test, below, which SIGTERMs the binary.) The
  replacement process
  then gets `AcquireScheduleExecutorLeaseOutcome::Busy`, its tick returns
  without claiming due occurrences, and **schedules do not fire for up to the
  60s lease duration after every restart**. Nothing surfaced this: the claim
  watchdog's overdue threshold is longer than the window, and the old code
  compiled clean with no warning. The release now runs before
  `composition.shutdown()`, because releasing the lease is a store write and
  composition teardown is what closes the store.
- **`delegate` now counts as evidence that the mob tool surface is present.**
  `DeclaredToolCategory::Mob` matched nine exact names plus the `mob_` prefix
  and missed `delegate`, so a catalog carrying only `delegate` read as a `Gap`
  against a declared Mob category and parked a healthy member. Latent rather
  than live: meerkat enables the mob tools as a group and a `mob_`-prefixed
  sibling has always co-occurred, so no spurious park was ever observed. The
  classifier should not depend on that.

### Added

- **The decorator-authority defect class is watched by a gate rather than by
  memory.** `scripts/verify-decorator-authority.py` enumerates every
  `impl AgentLlmClient`, classifies production from `#[cfg(test)]` structurally,
  and fails any production decorator missing `request_attempt_authority`. This
  is the 0.8.27 defect that stranded 72 identities while compiling clean; the
  per-wrapper unit tests cover the wrappers that exist, and this covers the ones
  that do not exist yet. It runs in the `fmt-lint` CI job and on pre-push, and a
  contract test in `scripts/test_ci_workflow.py` pins that invocation - a
  structural gate nobody runs is a control that cannot fail.
- A subprocess regression test for the schedule lease
  (`tests/gateway_schedule_lease_release.rs`), covering both binaries. It
  asserts that the running gateway holds the lease and then that a SIGTERM exit
  releases it; without the first assertion a gateway that never acquired a lease
  would also end with `owner_id` NULL and the test would pass while observing
  nothing.

  Reverting the fix and re-running is honest about what each leg is worth:
  `rpc_gateway` fails on the exact assertion first try, and `mobkit_gateway`
  passes 7 runs out of 7. Only the `rpc_gateway` leg is a mutation-proven guard.
  Under `mobkit_gateway`'s teardown the signalled supervisor does get polled in
  time and `Drop` releases the lease anyway - which is luck the harness cannot
  remove, not a property the code states, so the explicit call stays in both
  binaries. The `mobkit_gateway` leg is kept for the harder regression (a lease
  never released at all) and is documented in the test as not proving the
  narrower one.

### Changed

- The three hardcoded mob tool-name lists are now named constants that state the
  question each one answers: `MOB_SPAWN_TOOL_VOCABULARY` (did this call create a
  member the console must render?), `SPAWN_INITIAL_MESSAGE_TOOLS` (a documented
  subset whose argument shapes the initial-message extractor understands), and
  `MOB_UNPREFIXED_TOOL_NAMES` (does this catalog prove the mob surface is
  wired?). They are deliberately **not** unified: they answer different
  questions and are free to diverge. Each carries a pinning test that forces a
  human decision rather than accepting a diff.
- `memory::dispatch_taint`'s module doc no longer claims the module collapses
  onto an upstream hook slot once one lands. Hook points did land upstream and
  this module does not collapse onto them: meerkat's own reference states that
  post-commit hooks are not synchronous policy seams.

### Not in this release

- **The shutdown wedge remains open, and it is not MobKit's code.** A graceful
  stop can fail to converge in `shutdown_runtime_unregister`, escalating SIGTERM
  to SIGKILL. The symbols live upstream: `shutdown_runtime_unregister_observers`
  is defined and driven in `meerkat-mob/src/runtime/actor.rs`, as is the
  `shutdown session binding teardown failed` warning. Both are absent from
  `meerkat-mobkit` entirely, which only calls into them through
  `UnifiedRuntime::shutdown()`. Earlier notes, including mine, put this in
  MobKit's own teardown; that attribution was wrong.

  **It is not a regression.** The operator deployment reproduced it on meerkat 0.8.29 / MobKit
  0.8.24 - their current production pins - with the same harness, phases and a
  freshly cloned dataset. Across three pin pairs on the same arm: 0.8.29/0.8.24
  hung, 0.8.30/0.8.26 hung 2 of 2, 0.8.31/0.8.27 hung 4 of 6. It predates all of
  them, and no CHANGELOG should carry it as new in 0.8.30, 0.8.31 or here.

  It went unseen because the harness could not express it: before 2026-08-28 the
  twin's `Server.kill()` escalated SIGTERM to SIGKILL after 30s, silently and
  without an assertion, while a healthy shutdown on that fleet takes 74 to 92
  seconds. Every shutdown was killed at 30s and wedged and healthy runs looked
  identical - green. The defect is old; the instrument that can see it is days
  old.

  It is a **race**, not a deterministic trigger: the operator deployment measured 4 hangs in 6 runs
  on identical 0.8.31 Rust, alternating clean and hung with the same dataset
  shape, phases and spawn count. The conditions previously believed to form a
  required conjunction only raise the probability.

  **Do not read "4 in 6" as the odds that any given graceful stop hangs.** It is
  a conditional rate under adversarial setup: every one of those runs booted 161
  identities, drove a real OpenAI turn, a real Anthropic turn and a spawn, then
  SIGTERMed immediately with no idle period. The operator deployment also corroborated it in real
  production: 6 of 9 pod shutdowns in a 30-day window did not complete, with a
  control ruling out log loss.

  **There is no second-fleet counter-datum.** An earlier draft of this entry
  cited a second fleet's 79 clean restarts. Its owner retracted that measurement
  outright: the sample was from July, on versions no longer running, under a
  since-replaced deploy path. Their instrument then stopped emitting in mid-July
  - startup and shutdown lines disappeared together, which is a logging change
  rather than a behaviour change - so that fleet's current shutdown behaviour is
  simply **unobserved**, in either direction. It is withdrawn rather than
  weakened, and nothing here rests on it.

  So no fleet-shape hypothesis is offered either. The one previously carried
  here - fleet size, 17 identities against 161 - existed only to explain a
  discrepancy between two fleets, and it does not survive the discrepancy being
  withdrawn. The reproducer is held by its owner.

  Two consequences worth carrying forward. Any regression test for this must
  **repeat rather than sample** - a single green run has roughly a 1 in 3 chance
  of meaning nothing under the operator deployment's conditions. And the teardown retry count is
  **not** a severity signal: across two independent measurement sets the highest
  count (209, and earlier 161) sat on the only *clean* run each time.

  This release's lease fix is a different defect and almost certainly does not
  address this one. The lease release should nonetheless survive a wedged run,
  on structural grounds rather than measurement: `GatewayComposition::shutdown`
  is what calls `state.runtime.shutdown()`, and the lease release is placed
  before that call, with everything in between either bounded (the inflight
  drain, by `GATEWAY_RPC_DRAIN_TIMEOUT`) or non-awaiting. That ordering was
  chosen for an unrelated reason - the release is a store write and composition
  teardown closes the store - so the independence is a consequence, not a design
  goal, and it has not been observed against the operator deployment's reproducer.

  Named here so it is explicitly outstanding rather than silently dropped.

## [0.8.27] - 2026-08-29

Pairs with meerkat `0.8.31` (tag `v0.8.31`, commit
`83d5be6d75ebdf9a54106bf7b102617fb8114669`), bound to the published registry
release across all **25** pin sites (20 in `meerkat-mobkit`, 5 in
`mobkit-store-conformance`).

The 0.8.26 entry below says "26 pin sites". That number is wrong; it has been 25
in both directions, measured out of the transitional git-rev state and again
here. Correcting it in place would rewrite a shipped entry, so it is corrected
here instead.

### Fixed

- **Agent-LLM decorators no longer downgrade the clients they wrap.**
  `AgentLlmClient::request_attempt_authority` carries a default returning
  `LegacySplit`, so MobKit's decorators silently reported their own authority
  instead of forwarding the inner client's - compiling cleanly, with no error,
  warning or test failure. Meerkat 0.8.31 rejects `materialize resume` for a
  client reporting `LegacySplit` over a `Unified` adapter, which stranded every
  identity resuming a durable session. Measured downstream on a cloned
  production store: 72 identities marked Broken at boot before the fix, 0 after,
  against 0 on three separate 0.8.30 runs. Addressing a stranded identity did
  not recover it, because addressing triggers the resume that is rejected.
  Both production decorators - `ReplaySanitizingAgentLlmClient` and
  `TaintObservingLlmClient` - now forward. One contract test per wrapper, each
  using a non-default inner so forwarding is distinguishable from inheriting.

### Changed

- Repinned all 25 exact meerkat pins from `=0.8.30` to `=0.8.31`. Resolution is
  asserted rather than assumed: 35 meerkat packages, all from crates.io, none
  from git, none off-version.
- `ProviderBindingConfig` gained `credential_account`; MobKit's single
  construction site supplies `None`, which is behaviour-preserving.

### Added

- **The release path refuses a commit that was never green on main.** A
  `require_ci_green` job now gates the whole release DAG; previously
  `release_validate` had no `needs` at all, so a tag published from any commit
  and nothing checked it. It refuses rather than waits, and the dry-run lane is
  deliberately exempt because it publishes nothing.
- **Binaries carry build provenance.** `attest-build-provenance` over `dist/*`;
  the workflow granted `attestations: write` and never attested anything.
- **Every workspace member's publish status is explicit.**
  `verify-crate-publication.py` fails unless each member is either published by
  `release.yml` or declares `publish = false`, deriving the published set from
  the workflow rather than restating it.
- e2e suites can execute a prebuilt example via `MOBKIT_EXAMPLE_BIN_DIR` instead
  of invoking cargo. Set nowhere; behaviour is unchanged.

### CI

- Split the serial `console` job into `console`, `console-fixtures` and
  `console-acceptance`. Two steps accounted for 22.3 of its ~38 minutes while
  all nine npm check scripts took 0.0m each, so separating "fast checks" from
  "slow e2e" would have bought nothing. All three are named in `gate`.

## [0.8.26] - 2026-08-27

Pairs with meerkat `0.8.30` (exact rev `5e229e0b8379ac162a6b3c69187d186b570535e9`),
bound as an immutable Git rev across all 26 pin sites.

### Added

- **Experimental GPT Live channel surface** (`experimental-gpt-live`, off by
  default). Realtime audio/live sessions route through the client context, with
  a live surface in both SDKs (`sdk/typescript/src/live.ts`, Python live
  contracts) and console replay wired to live subscription. The feature is
  opt-in: a default-feature embedder acquires none of its dependency graph.
- An exclusive session-identity config root declared by the gateway, so the
  identity root a session resolves is stated rather than inherited ambiently.

### Changed - BREAKING for host/SDK implementors

- `resolve_record_by_session` is now **required** on `ContinuityStore`; its
  `Ok(None)` default is gone, and both SDKs answer it. A store that silently
  reported "no record" for a session it was holding made a resume look like a
  first boot, and a default is exactly how an implementor inherited that
  behaviour without choosing it. Hosts implementing the trait must add the
  method; the compiler names every site.

### Fixed

- Persisted owner authority is published **before** mob prepare, so a revived
  session's owner is registered when the lift happens instead of after it.
- Persisted runtime authority converges before the bounded resume window, moving
  memoized convergence work out of a budget it was exhausting on large rosters.
- MobKit satisfies meerkat 0.8.30's now-compile-required committed-parent
  projection seam. Wrappers holding an inner forward; genuinely non-persistent
  doubles answer zero behind a fail-closed `supports_persistent_sessions()`
  guard rather than a bare `Ok(0)`.

### Security / advisories

- RUSTSEC-2026-0150 (`audiopus_sys` 0.2.2 unmaintained) is waived with its scope
  recorded in `deny.toml`. It is informational with no patched release, reachable
  only under `experimental-gpt-live`, and its real exposure is a CMake 4 **build**
  failure rather than a vulnerability. See the waiver for the 0.8.31 follow-up.

### Note on the missing 0.8.24 and 0.8.25 entries

`v0.8.24` and `v0.8.25` were tagged and published without changelog entries; the
previous entry below is `0.8.23`. That gap is recorded here rather than papered
over, and it is deliberately NOT reconstructed after the fact - anything written
now would be inference about what shipped, not a record of it. For their actual
contents, read the tags directly:

    git log v0.8.23..v0.8.24
    git log v0.8.24..v0.8.25

Note also that this candidate's pre-bump commits self-reported `0.8.25` while
differing from the published `v0.8.25` tree, which is why this release is
`0.8.26` rather than a re-cut of `0.8.25`.

## [0.8.23] - 2026-08-25

Pins meerkat `=0.8.28`, which carries three upstream fixes this release depends
on. First, accepting an encoded stable roster identity as the successor of a
legacy generation-zero runtime binding - that is what lets already-persisted
sessions resume under the durable-roster contract with no migration on the
MobKit side. Second, `respawn_with_successor_spec`, the atomic successor-spec
respawn that restores reprofile-via-reset (see below). Third, treating an
omitted binding on a successor spec as "preserve the current one" rather than
"clear it"; without that, a successor respawn was refused for any locally-bound
member, because the roster does not expose a binding for the caller to restate.

### Changed

- **BEHAVIOR CHANGE: a persistent launch now pins `mob_config`.** Mob storage is
  persistent on launches that already persist everything else, which is what
  makes adopted identity declarations survive a restart. The cost is that the
  mob definition is pinned once the storage exists: meerkat refuses a definition
  that disagrees with the persisted spec store, on create and on resume alike
  (verified against 0.8.26 and unchanged in the pinned 0.8.28), so durable mob
  state and an editable `mob_config` cannot both hold on one storage path
  WITHOUT the declared spec update described under Added - which is the
  sanctioned way through this refusal, not around it.

  Editing `mob_config` and restarting against the same state directory is now
  refused with a typed error that names the diverged fields and states the
  remedy, instead of failing later as an internal store mismatch. Today the
  remedies are a new state directory (and re-adoption), or declaring mob state
  ephemeral with `runtime_options.mob_storage = {"storage": "memory"}`, which
  keeps `mob_config` editable at the cost of durable adoptions. A future
  upstream definition-migration path would update the authoritative event-log
  definition; rewriting the recorded provenance is deliberately NOT offered,
  because certifying a definition the event log does not hold would boot stale
  config silently.

### Added

- **A declared spec update, so the `mob_config` pin has a door.** A persistent
  launch pins the mob definition, and the typed refusal for a diverged definition
  is the right default - booting stale config silently is worse. But a refusal
  with no sanctioned way through it is a dead end, and it refuses the standard
  operating mode of any deployment that edits `mob.toml` between activations and
  clones its state directory (the persisted spec rides through every clone, so
  the next config-touching activation is refused).

  `runtime_options.declare_spec_update = {"expected_revision": N}` is an explicit
  operator declaration that the persisted spec now matches this definition,
  compare-and-swapped on the revision the divergence was observed at. Present
  only on the activation that intends to move the pin: a declared transition, not
  a mode. `expected_revision` is required rather than defaulted, because a
  declaration without the revision it was made against is not a declaration - it
  is "accept whatever is there", which is indistinguishable from having no pin.

  It refuses, fail-closed, when the revision moved between observation and
  declaration, when the payload names a different mob than its definition
  carries, when nothing is pinned, when the definition already matches, and when
  declared alongside an in-memory `mob_storage` (which pins nothing, so there
  would be no spec to move). Each of those would otherwise be a silent lie in an
  activation log. The receipt is logged with the previous revision, the committed
  revision and the declared fields.

  Divergence is reported as dotted field paths - `profiles.security.model` rather
  than `profiles` - because an operator about to declare through a refusal has to
  see what actually moved.

- **Reprofile-via-reset is restored.** Lowering destructive reset to one
  authoritative respawn removed it: a respawn preserves the predecessor's
  profile, so reset with a changed roster profile returned a typed refusal rather
  than silently keeping the old profile. meerkat 0.8.28 adds
  `respawn_with_successor_spec`, which applies the successor spec atomically, and
  reset now binds to it - so a reset that changes profile reprofiles again. The
  typed refusal remains for any case the successor transition cannot satisfy;
  what it will never do is report a reset that kept the old profile.

- **`runtime_options.mob_storage`.** Declares mob storage in-memory on an
  otherwise persistent launch, mirroring `runtime_options.runtime_store`. The
  storage census now reports the mob slot on both GATEWAY launch paths
  (`mobkit_gateway`, `rpc_gateway`); it previously declared nine slots and
  omitted this one, which is why an in-memory mob storage on a persistent launch
  was invisible to healthz and the storage doctor.

  Scope, stated because it is narrower than it first reads: the embedder path
  (`UnifiedRuntime::builder()` with no mob storage argument) is unchanged. It
  still composes `MobStorage::in_memory()`, so it declares no mob census slot
  and its `mob_config` stays editable - and its mob state is NOT durable, so an
  embedder that adopts identity declarations still loses them on restart. The
  durability fix below covers the two gateway paths only.

### Fixed

- **Mob roster identity is now the durable identity, for every binding.** MobKit
  lowered its own `AgentRuntimeId` into `SpawnMemberSpec.identity` for local and
  session-backed members, and the durable identity only for external ones.
  Meerkat defines `AgentRuntimeId` as a stable identity plus a
  machine-owned generation, where different generations are successive
  incarnations of the same member, and `SpawnMemberSpec.identity` is the roster
  identity. Nesting one inside the other made the roster id per-incarnation, so
  durable state keyed on it belonged to a single binding and could not survive a
  respawn or a restart. MobKit's `AgentRuntimeId` remains binding and
  incarnation detail; it is no longer a roster spelling, and every surface that
  used to convert it into a roster key now resolves through the durable
  identity with exact-session agreement.
- **A resumed mob is lifted out of the prior graceful stop before identity
  restore runs.** A replayed event log ends at the `MobStopped` of the last
  clean shutdown, so a resumed handle can come back `Stopped`. Bootstrap now
  lifts `Stopped` to `Running` once, and refuses `Completed`, `Destroyed`, and
  an unsettled `Creating` with typed errors, instead of handing identity restore
  a mob it can never spawn into.
- **Adopted identity declarations and mob events now survive a restart, on the
  two gateway launch paths.** `mobkit_gateway` and `rpc_gateway` composed
  in-memory mob storage while persisting sessions, so every restart presented as
  a healthy boot with the right member count and no durable mob state. The
  embedder path is NOT covered - see the scope note under Added. Storage mode is now attributed per launch branch: the
  ephemeral branches are unchanged and declare themselves in the census.
  Create-versus-resume is decided in one place from the event log rather than
  at each launch site, and storage supplied through the public bootstrap API
  with a non-empty log and no provenance declaration fails closed rather than
  resuming unverified.
- **A closing member event stream no longer races MobMachine for actuation.**
  When the identity store holds a valid `Present` intent, MobMachine is the
  sole actuator: the stream-health monitor no longer marks the identity-first
  runtime broken, and the resume path's roster-collision handler no longer
  retires the occupant. Both now read the same authoritative intent, and an
  unreadable or ambiguous intent fails closed and retryable instead of being
  treated as permission to destroy a live member. Only a valid `Absent` intent
  leaves identity-first its existing repair ownership.

## [0.8.22] - 2026-08-24

Pins meerkat `=0.8.26`.

### Fixed

- **Member declaration RPCs now have parity across stdin and the HTTP console.**
  Full-runtime console clients can adopt, read, and apply member tool declarations
  through the same canonical handler as stdin. Capabilities advertise only the
  methods the active console policy can serve, and read-only consoles expose the
  read method without exposing mutation methods.
- **Identity-first member aliases now resolve at the declaration boundary.**
  Public aliases are translated to the reserved roster identity internally and
  mirrored back on output, so callers never need to discover or submit an
  internal `mk--` identity. Scoped reads retain member-view ABAC checks, while
  adopt and apply remain guarded by runtime administration authority.

## [0.8.21] - 2026-08-23

Pins meerkat `=0.8.26`.

### Added

- **Member declaration control plane on the gateway.** Three typed stdin-RPC
  methods, mirroring meerkat's own catalog names and wire types exactly:
  - `mob/adopt_member_identity_declaration` - bring a live member under
    declarative management (one-shot CAS against live intent state).
  - `mob/apply_member_tool_declaration` - atomically update only the tool
    portion of an existing durable member intent.
  - `mob/member_tool_declaration` - read the live declaration and its desired
    intent revision, so the apply CAS is not guess-and-retry.

  Previously the gateway linked these types but reached none of them, so a host
  composing through MobKit could supply a compiled tool policy at init and never
  bind a member to it: every member stayed `Unmanaged` and the installed provider
  governed nothing. Requests are delegated straight to the canonical Meerkat Mob
  handle; no validation is reimplemented. Errors carry meerkat's typed jsonrpc
  codes and structured data rather than a generic failure. A request naming a
  different mob is refused with `foreign_mob_target` instead of being silently
  retargeted at the gateway's own mob.

  No SDK change is needed to call them: `MobKitRuntime._rpc_sync` takes an
  arbitrary method string. Note that `_rpc_error_from_payload` maps only MobKit's
  own codes to typed exceptions, so a typed Meerkat mob error arrives as a generic
  `RpcError` carrying `code` and `data`.

### Fixed

- **Supervisor cleanups retired by replacement are now owned and joined.**
  `start_identity_first_supervisors` cancelled the displaced lease-renewal and
  continuity-repair supervisors, then spawned their cleanup detached and dropped
  the handle, so a cleanup could outlive `shutdown()` while still holding the
  authority it was releasing. They are now retained and joined at shutdown, and a
  cleanup that does not reach its release boundary makes
  `runtime_cleanup_completed` false instead of being reported as a clean stop.

- **Shutdown responses name which phase blocked.** A failed runtime cleanup now
  carries typed per-phase diagnostics (drain timeout, mob stop, identity-authority
  release, orphan module processes, retired-supervisor cleanup) with numeric
  counts, rather than a single boolean. Successful responses are unchanged.

### Changed

- The advertised stdio shutdown horizon is 337s (was 335s), covering the new
  bounded retired-supervisor join. The Python and TypeScript SDK shutdown grace
  constants follow it.

## [0.8.20] - 2026-08-23

Pins meerkat `=0.8.26`.

### Added

- **MobKit now serves compiled application tool policies, because Meerkat
  defines that contract but ships no implementation of it.** Meerkat 0.8.26
  owns the compiled artifact, the provider and snapshot traits, and the registry
  that binds a member to a policy; the only implementations of those traits in
  the published crate live inside its own test module. `member_tool_policy`
  supplies the production pair: `CompiledPolicyProvider` serves the policies a
  boot was configured with, and `CompiledPolicySnapshot` evaluates one.
- **Declared in the gateway init payload as `"application_tool_policies":
  ["<canonical json>", ...]`.** Each entry is the exact canonical bytes of one
  compiled policy, carried as a string rather than a nested object on purpose:
  parsing verifies the digest against the bytes it is given, so re-serialising a
  nested object would check a digest against bytes MobKit had just manufactured
  instead of the ones the operator compiled. A malformed entry, a rejected
  policy or a registry that will not build refuses the boot, for the same reason
  a malformed `role_migrations` does - an operator who supplied a policy expects
  it in force, and arming nothing would resurface later as an unexplained access
  denial with nothing pointing at the payload.
- **Declarable from the Python SDK, which is how the only consumer composes.**
  `MobKitBuilder.application_tool_policies([...])` takes the compiler's canonical
  JSON as `str` or `bytes` and passes it through unchanged into the gateway's
  init params. An earlier draft parsed the parameter in the gateway while no SDK
  could send it, so the feature was reachable only by hand-writing raw
  InitParams JSON. The role-migration carrier shipped with the same gap and was
  caught the same way; one shared wire fixture is now read by both the Rust
  parser test and the Python builder test, so renaming the key goes red on one
  side instead of both staying green while a host arms nothing. TypeScript is
  deliberately not given this surface: no deployment composes through it.
- **The provider identity comes from the artifact, never from MobKit.** A
  compiled policy declares its own author, and a member's
  `ApplicationToolPolicyBinding::Provider { provider_id, policy_id }` is
  resolved by that carried id, so the gateway registers one provider per
  distinct author it was given and names none itself. An earlier draft
  hardcoded a single provider id, which would have refused every policy
  compiled by anyone else - not a narrower feature but a dead one.
- **Rollback refusal is MobKit's, by Meerkat's design.** The provider trait
  documents that the provider owns the snapshot pointer and must reject a
  revision below one already accepted, because Meerkat "deliberately keeps no
  second accepted-revision store". So the fence lives here, and it refuses in
  Meerkat's own vocabulary: a lower revision is `RevisionRollback`, and a repeat
  of an accepted revision whose bytes differ is `RevisionDigestConflict`. The
  second half matters because a content swap under a stable revision would leave
  the digest as the only evidence and nothing downstream re-checks it.
- **Execution authority is the grant, not the consequence class.** In v1 a grant
  is one exact allow entry with no wildcards and no deny entries, its only
  action is `Invoke`, and neither the request nor the verdict carries a
  threshold. So an exact member/tool grant allows, a miss defers to the
  artifact's own `default_deny`, and the `R0`-`R3` consequence class is reported
  without gating execution. Choosing a threshold here would have made MobKit the
  authority on what `R2` means, which is Meerkat's to define if it is ever
  wanted.

## [0.8.19] - 2026-08-21

Pins meerkat `=0.8.25`.

### Added

- **Boot-scoped member role migrations, so a role change is a decision the host
  states out loud rather than a silent restamp.** A durable member whose role
  changed refuses to resume (`MobError::MemberRoleMigrationRequired`) until this
  activation's host declares the migration. That refusal is deliberate: a stray
  role edit in a roster otherwise restamps the member's durable role, comms name
  and binding, and nothing afterwards points at the edit as the cause. Declare it
  in the gateway init payload as `"role_migrations": [{"identity":
  "domain:home-automation", "from_role": "domain"}]`, or from the Python SDK as
  `MobKitBuilder.role_migrations([...])`, which takes
  `RoleMigrationDeclaration` dataclasses or plain dicts and validates both.
- **The declaration is authority for one boot, not state.** It is installed on
  `MobSessionBridge` - which lives for exactly one boot - via
  `with_role_migration_declarations`, and `resume_session` passes it into
  `MemberLaunchMode::Resume { resume_from_role, .. }` by EXACT identity lookup:
  no durable read, no inference from stored role metadata, no retry of a resume
  meerkat refused. Nothing persists it, so it is absent by default and dropping
  it from the next boot payload is how the authority goes away. Meerkat decides;
  MobKit only carries. It re-verifies the declared predecessor against durable
  state and refuses on mismatch (`MobError::MemberRoleMigrationRejected`), so a
  mistyped `from_role` cannot authorize an unintended restamp, and it ignores
  the declaration entirely once the roles already agree - a declaration left in
  place after a completed migration is inert, not a repeat restamp.
- **Wired in BOTH gateway binaries, whose names differ by one word.**
  `rpc_gateway` reads it from its untyped init params and installs it inside the
  roster-provider branch (the identity-first path the SDKs drive);
  `mobkit_gateway` takes it as a typed `InitParams.role_migrations` field and
  installs it inside its identity-first block. Declarations that reach
  `rpc_gateway` with no roster provider, or `mobkit_gateway` with
  `identity_first: false`, arm nothing, because there is no identity plane to
  migrate on. On both binaries the per-declaration log line sits immediately
  before the install, inside that same branch, because a restamp of durable
  role, comms name and binding belongs in the boot record; a payload that
  reaches neither identity plane installs nothing and is not logged either.
- **A malformed payload refuses the boot** rather than arming nothing and
  leaving the operator to rediscover it as the original refusal. Both binaries
  refuse it with `-32602`, and neither gates that refusal on having an identity
  plane: `rpc_gateway` parses the key at init scope, outside its
  roster-provider branch, and `mobkit_gateway`'s typed init params fail to
  deserialize before any of the boot runs, so a host with no identity plane
  still learns its payload is broken. Declaring one identity twice with CONFLICTING `from_role`
  values is refused too, because collecting them into a map would let hash
  order pick which predecessor role becomes authority - but the two binaries
  place that check differently. `rpc_gateway` refuses with `-32602` alongside
  the malformed case, ungated. `mobkit_gateway` checks only inside its
  identity-first block, so it refuses from there as a `-32603` internal error
  with a null `id` rather than the answered `-32602`, and under
  `identity_first: false` a self-contradicting payload is not refused at all;
  `identity_first` defaults to true, so a fresh boot on the default path does
  refuse. The Python builder raises `ValueError` on the conflicting pair before
  any of this reaches a gateway. An IDENTICAL repeat is accepted on purpose: a host proves
  inertness by carrying the same declaration into a later activation and showing
  that nothing moves. Unused, the Python builder emits no
  `role_migrations` init-params key at all.
- The wire key names are pinned by one shared fixture,
  `meerkat-mobkit/tests/fixtures/role_migrations_init_params.json`, read by both
  the Rust parser test and its Python twin, so a rename on either side goes red
  instead of both staying green while a host arms nothing.

### Fixed

- **A gating caller could claim its own risk tier and have the claim
  honoured.** `apply_gateway_runtime_config_to_request` filled `risk_tier` only
  when the caller had omitted it, so a client sending `risk_tier: "r0"` on
  `mobkit/gating/evaluate` for an action the compiled `action_risk_tiers` table
  declares `r3` kept its own claim. `evaluate_gating_action` branches on
  whatever tier it is handed, so the table compiled from
  `runtime_options.gating_config_path` was advisory against exactly the caller
  it exists to constrain. A policy table a caller can override is not a policy,
  it is a default - and the name says otherwise, which is the part that bites:
  the next person wiring gating reads `action_risk_tiers` as authority. The
  configured tier now wins whenever the action appears in the table. An
  overridden caller claim is logged at WARN with both tiers rather than dropped
  silently, because a caller that keeps sending an overridden tier must be able
  to find out it is being overridden. An action absent from the table still
  keeps the caller's value: the table binds what it declares and no more.
  Reported by the downstream app's tools/grants fork with file:line, verified here before
  fixing. The regression test is mutation-proven: restoring the
  `!params.contains_key("risk_tier")` guard turns it red with left `"r0"`,
  right `"r3"`.

- Corrected the public documentation, crate rustdoc, and repository guidance
  to match the shipping gateway handshake, event subscription and SSE names,
  runtime-owned gating, delivery and module contracts, member roster
  projections, gateway profiles, and the Meerkat/MobKit ownership boundary.
  The obsolete storage-unification implementation plan is now retained only
  as a dated historical record.

## [0.8.18] - 2026-08-19

Pins meerkat `=0.8.24`.

### Operator and adopter guidance

This section carries the operator-facing guidance for the release. The GitHub
release body is written FROM it, not alongside it: 0.8.17 shipped with its
guidance only in the release body and no CHANGELOG section at all, and nothing
in CI can see that drift.

**If you switch on the new `ErrorCategory` members, write explicit match arms.**
Both SDKs have always passed unknown categories through as raw strings, so an
`if/elif` chain or a `switch` written against the old five members still
compiles, still runs, and now sends six additional categories - including every
one we tell you to page on - down the catch-all branch. Adding the members to
the enum does not route them; your arms do. In Python, match on
`ErrorCategory` and keep a final `case _:` that logs the raw tag so a future
addition is loud rather than silent. In TypeScript, exhaustive-check the union
with a `never` assertion in the default branch so the same addition fails at
compile time. Treat `actor_loop_recovered` as a RESOLUTION, not an incident -
use `ErrorEvent.is_resolution` / `isResolutionErrorEvent()` rather than
listing categories yourself, or a resolved stall raises a second page.

**Operator recovery matrix** (supersedes the 0.8.17 matrix; the
`compact_member` row changed this release):

| Situation | Action |
|---|---|
| Member's context has overgrown | `mobkit/compact_member`, then `mobkit/bound_member_transcript` if it is still too large |
| `compact_member` timed out | Read the error: it now names the turn's fate. "Quiesced by the rollback" means the member is yours again and you may retry. "Still running on the floored build" means the member is briefly unresponsive - wait, do not stack a second verb on it |
| Console reads returning `-32017` | The member is mid-turn on a long tool chain or compaction, not wedged. The `awaiting` field names which loop. Retry, or raise `MOBKIT_CONSOLE_READ_TIMEOUT_SECS` for a known-slow deployment |
| `ActorLoopStalled` paged | Wait for the `ActorLoopRecovered` carrying the same `stall_id` before escalating. Zero `prior_resolved_stalls` and no resolution arriving is the wedged shape; a high count is merely a chronically busy loop |
| `CompactionPersistenceRejected` paged | Check the typed fit and `attempted_entries`. On a meerkat pin older than 0.8.23 this is the epoch-seam defect and the member is permanently wedged: retire and respawn |
| Member is wedged | `respawn` does NOT recover it (continuity-preserving; it resumes the same strand). `reset` recovers but destroys the member's entire history - last resort |

**Quiesce before you stop.** `bound_member_transcript` already refuses with a
typed Busy on an in-flight turn, and teardown now reports
`ProceededWithoutInterrupt` rather than claiming a clean stop it did not
achieve - but both of those are the platform telling you it could not do the
safe thing, not the platform doing it for you. Let in-flight turns finish, or
stop them deliberately, before tearing a mob down. A stop that races a member's
kickoff leaves a turn admitted and possibly still running, and the honest
report of that is still an operational loose end you have to close.

### Fixed

- **Console read arms could hang indefinitely.** The gateway awaited the member
  session task or the mob actor loop with no bound at all, and both are strict
  sequential command loops, so any long turn (a tool chain, a post-cycle
  compaction) made every console read on that member queue silently instead of
  degrading. Reported by the operator deployment as `mobkit/identity/resolved_tools` hanging past 60
  seconds with no completion. The seven read arms that cross those two loops -
  `identity/resolved_tools`, `member_status`, `list_members`, `get_member`,
  `find_members`, `flow_status`, `list_runs` - are now bounded and return a
  typed error code `-32017` naming the arm and the seam it was awaiting, with
  structured `data` (`kind`, `arm`, `awaiting`, `timeout_secs`) for SDK callers.
  Budget is 30s, overridable via `MOBKIT_CONSOLE_READ_TIMEOUT_SECS` and clamped
  to `[1, 3600]`. Reads that do NOT cross those loops are deliberately left
  unbounded: `memory/query` can legitimately run long over a large HNSW index,
  and `cross_mob/peer_info` runs inside a member authority transaction where
  abandoning the future is not safe.
- **`mobkit/compact_member` left its maintenance turn running on timeout,** so
  later console reads queued behind the very turn the operator had given up on.
  The turn is now actually stopped, through the mechanism already on the path:
  the error path's rollback rebuild retires the member, and retirement quiesces
  the session's active runtime turn. The error now states the turn's real fate -
  quiesced by the rollback, or still running on the floored build with the
  member briefly unresponsive - rather than leaving the caller to guess. The
  post-timeout transcript read that decides whether the forced compaction had
  already landed runs under its own 5s bound, so unproven evidence is dropped
  from the message instead of traded for a second hang.
- **A member could appear to answer nothing at all, for days, while answering
  normally.** On identity-first gateways `MobKitConsoleAggregator::send`
  reserved its pending interaction under `(alias, alias)`, and since the roster
  alias there IS the fenced runtime incarnation, that self-map OVERWROTE the
  spawn path's incarnation-to-durable-identity registration - process-wide, for
  every ingress, until the process restarted. From then on the console resolved
  every live agent event for that member to `{identity}:{generation}`, while the
  `user_input` frames (built separately, from the durable identity) kept landing
  on the bare conversation the UI renders. The result is a thread showing
  questions and never answers.

  **Scope, corrected by the downstream app after the first fix:** this was reported as a
  `/dev/dispatch` problem because that is where it was first hit, and describing
  it that way understates it badly. A recurring SCHEDULED prompt reaches the
  runtime through `WorkSpec`/`MobHandle` and reserves nothing on the console
  store, so it never keyed anything itself - it was collateral damage of the
  poisoned map, and it is the path scheduled household work actually runs on.
  The operator-visible symptom is a scheduled agent that appears dead. If you
  have an agent that "stopped responding" and looked healthy when you checked,
  this is a candidate explanation, and it is not a dispatch story.

  **Whether you saw it depends on which history your UI renders, not on which
  version you run.** The split is in MobKit's console conversation store, so it
  reaches operators whose UI reads that store. An adopter whose dashboard reads
  its OWN session store (the operator deployment renders `/api/dashboard/chat/history` off their
  BigQuery store) saw completions normally throughout and would never have
  noticed. Check which endpoint your console calls before concluding you were
  unaffected - and note that mounting `/console/*` is enough to be exposed:
  anyone opening the stock console directly on such a deployment hits this in
  full, however insulated the primary dashboard is.

  Fixed at the existing mechanism, no new mirroring layer: the reservation keys
  the pending interaction under the member's DURABLE console identity with the
  incarnation as the mapping KEY - the same registration the spawn path makes.
  Classic (non-identity-first) mobs were never affected, because there the
  roster alias IS the identity and the self-map was benign.
- **`send_message` self-mapped the incarnation on its `AuthorityUnavailable`
  arm,** the same shape on a second call site, also found by the downstream app. It now
  reserves under the durable identity with the incarnation as the mapping key.
- **Every `ErrorEvent` now reaches the log whether or not an error hook is
  wired.** Previously the three fire points only logged through a registered
  hook, so a host that never called `on_error` - the downstream app's case - had every
  `ErrorEvent` this platform ever emitted go to `None` with nothing in the log
  either. The default sink logs at ERROR (INFO for resolutions) and records
  `hook_registered`, so an absent hook is visible as an absent hook rather than
  as an absence of failures.
- The Python and TypeScript SDK `ErrorCategory` enums declared only five of the
  nine Rust `ErrorEvent` variants, so a host could not write a typed alert arm
  for `compaction_persistence_rejected`, `actor_loop_stalled`,
  `event_log_flush_failure`, or `identity_materialization_failure` - the very
  events operators are told to page on. The events always arrived (both SDKs
  pass unknown categories through as raw strings), but only as a catch-all.
  All four are now declared in both SDKs, with `message` formatting mirroring
  the Rust `Display` impl instead of falling through to a raw JSON dump.
- Two further variants added by sibling lanes in this same release,
  `actor_loop_recovered` and `mob_stop_proceeded_without_interrupt`, are
  likewise declared in both SDKs with faithful `message` arms. The parity gate
  below caught both on its first integration run, before either could ship
  invisible to SDK hosts.

### Added

- **A per-turn wall-clock ceiling you can actually set: `max_turn_duration` in
  mobpack layer specs** (via meerkat 0.8.24's `BudgetLimits`). This is the
  missing owner for the question operators have been asking all cycle. Every
  segment of a turn already had its own bound - per-call LLM timeout,
  stream-inactivity watchdog, per-tool-call timeout - but their SUM had no
  ceiling, so a turn could legally spend an hour without anything deciding that
  was too long. Note the epoch: `max_duration` is agent-lifetime, measured once
  at agent construction and never re-armed, so it is not a per-turn deadline;
  `max_turn_duration` is re-armed at each turn entry.
  Absence stays absent - a spec that does not set it keeps its historical
  canonical bytes, so `spec_digest` pins already recorded in host stores still
  match, and leaving it unset is a real choice for unbounded turns rather than a
  default someone invented for you.
- **An ambiguous peer delivery is no longer rendered as a plain failure on the
  SSE surface.** `map_runtime_error` now has a typed arm for
  `SendError::AmbiguousDelivery`, returning `delivery_outcome_ambiguous` with the
  `envelope_id` and `required_action: "reconcile"` instead of collapsing into
  `internal_server_error`. The envelope may already be on the receiver's queue,
  so "retry" - the action a bare 500 invites - is the one unsafe move.
  `retry_safe` is deliberately ABSENT rather than `false`: 0.8.24 comms still
  folds connection-refused into this variant, where nothing left the host, so
  asserting non-retryability would be a claim we cannot substantiate. The HTTP
  status is not the signal here and is not meant to be read as one; the body is.
- **`ErrorEvent::ActorLoopRecovered`, and a stall you can close.** 0.8.17's
  `ActorLoopStalled` could only ever open an incident: nothing ever reported the
  stall ENDING, and its one number (`probe_waited_secs`) was the configured
  budget echoed back, identical on every page. The stall now carries `stall_id`
  plus `prior_resolved_stalls`, and the resolution arrives as a sibling event
  carrying the same `stall_id` and the true `stalled_for_secs`. Note the
  direction before wiring an alert: the probe parks on the same round trip
  instead of starting a new one, so a genuinely WEDGED loop pages once and never
  increments again, while a merely slow loop recovers and stalls afresh. A high
  `prior_resolved_stalls` therefore means "repeatedly late"; the wedged loop is
  the one sitting at zero priors with no resolution ever arriving. Distinguishing
  busy from wedged still needs a progress counter on the meerkat-owned loop,
  which mobkit cannot observe from here; that ask is filed upstream.
- **`UnifiedRuntime::stop_mob_for_teardown()` returning `MobStopOutcome`.**
  Teardown could be refused for its entire window on runtime-attach readiness
  and still be reported as a clean stop. The outcome is now explicit -
  `Stopped`, `ProceededWithoutInterrupt { waited_ms, member, error }`, or
  `Failed(..)` - and the middle variant says exactly that and nothing stronger:
  a mid-attach member has already had a turn admitted and its kickoff is about
  to bind, so calling that an interrupt would be a false success the caller
  cannot detect. It also fires as `ErrorEvent::MobStopProceededWithoutInterrupt`,
  so the condition reaches a pager and is never swallowed. Upstream root cause
  (a refusal the caller has no action to clear) is accepted as a meerkat P1;
  this degrade makes mobkit teardown independent of that timing either way.
- **The compaction rejection alert now carries meerkat's typed fit.**
  `CompactionPersistenceRejected` previously reported only a reason string, so a
  host could not tell a rejected commit that preserved usable history from one
  that did not. It now carries the typed `CompactionPreservedHistoryFit` and,
  where meerkat supplies it, `attempted_entries` - the count of real work at risk
  of being discarded.
- `ErrorEvent.is_resolution` (Python) / `isResolutionErrorEvent()` plus
  `RESOLUTION_ERROR_CATEGORIES` (TypeScript) distinguish the one category that
  reports a failure ENDING (`actor_loop_recovered`) from the rest. Mirrors the
  Rust runtime's `log_error_event`, which logs that variant at INFO rather than
  ERROR. Without it a host wired to a pager raises a second incident for a
  stall that already resolved.

- `meerkat-mobkit/tests/sdk_error_category_parity.rs` fails the build when the
  SDK `ErrorCategory` sets drift from the Rust `ErrorEvent` enum, naming the
  missing/extra tags and the file to edit. The authoritative Rust set is read
  back out of serde itself, so a per-variant `#[serde(rename = "...")]` cannot
  slip past it.

- `meerkat-mobkit/tests/sdk_enum_mirror_parity.rs` extends that gate to the two
  SDK mirrors of MEERKAT-owned enums, `MobRunStatus` and `MobMemberStatus`.
  These are the more fragile case despite being the smaller one: drift arrives
  with an upstream version bump and no mobkit commit involved, so it appears at
  the repin and is invisible in a review of the repin diff. Three declaration
  sites per vocabulary, not two - `parseMobRunStatus` carries a runtime
  allowlist that is a third mirror, and it is the one that decides what a host
  actually observes.

### Changed

- **Meerkat pins move to `=0.8.24`** (24 exact pins across `meerkat-mobkit` and
  `mobkit-store-conformance`).
- **BREAKING for anyone reading job health: `delivery_backlog` is gone, replaced
  by two fields that are not the same number.** Both gateway surfaces -
  `jobs/health` (typed) and the `healthz` projection (JSON) - now publish
  `pending_outbox_jobs` and `runtime_inbox_backlog` separately, plus `coverage`.
  This is a correctness fix, not a rename: 0.8.23 published the SUM of the two
  under one name, and they describe different wedges. `pending_outbox_jobs` is a
  job whose delivery was never handed to a runtime; `runtime_inbox_backlog` is a
  delivery a runtime accepted and never drained. Their sum is a number that means
  neither, and an operator reading it could not tell which failure they had - or
  which side to go look at. Alerts keying on `delivery_backlog` must be repointed;
  they will not silently keep working, which is deliberate.
  **Note the differing scopes, because they are not both realm numbers.**
  `pending_outbox_jobs` is realm-scoped. `runtime_inbox_backlog` is a HOST-STORE
  total and cannot be narrowed to a realm: the durable runtime store is one file
  per realm root and serves every session the host built, including members built
  under `mob.<mob_id>`, and runtime ids carry no realm. On a host serving more
  than one logical realm this number is therefore host-wide by construction.
  Over-inclusion is the safe direction - every counted row is a real undrained
  delivery on this host - whereas the realm-derived sum it replaces silently
  missed any runtime whose job had aged out, publishing a false zero exactly
  when a backlog was the thing being asked about.
- **`status` on both job-health surfaces gains a third value, `unreadable`.**
  Meerkat's census answers `Unreadable` when its scan window did not reach every
  row, and it is NOT a rung between `ok` and `degraded`: it says the census
  established nothing, so `stale_leases = 0` means "none seen", not "none exist".
  Mobkit now carries that rung through instead of collapsing it - folding it into
  `degraded` would assert a fault nobody observed, and folding it into `ok` would
  publish a green that means the instrument did not answer.
  **Repoint alerts from `status == "degraded"` to `status != "ok"`**, or an
  unreadable census will read as healthy.
  Honest limitation: mobkit cannot currently EMIT `unreadable`, because both of
  its census calls request an unbounded window and truncation only occurs when a
  bounded one fills. It is carried so that a future bounded window cannot
  silently collapse into a false rung.
- **`cross_mob/send` errors no longer prefix a verdict.** The message was
  `"cross_mob/send failed: {err}"`, which renders an ambiguous delivery as
  `"failed: ... is ambiguous"` - a contradiction whose leading verb wins for
  every reader, including the model-mediated ones that decide retries by reading
  prose. It is now `"cross_mob/send: {err}"`: context composes, verdicts do not.
- Test-suite timing hygiene: assertions that bounded a STRUCTURAL property with
  a wall clock (waiting for a spawn, a settle, a drain) now poll for the
  condition under a generous liveness backstop instead of asserting a duration
  the machine's load decides. Genuinely duration-shaped assertions - the ones
  that verify nothing happened inside a window - were deliberately left alone,
  since load only makes those safer. No production behaviour changes.
- The same sweep, extended to the JS suites and to one clock bug the first pass
  missed. The `flow-editor` browser smoke carried 31 unnamed 30s ceilings, every
  one guarding a structural question ("does this element ever appear"); they are
  now a named `STRUCTURAL_TIMEOUT_MS`. And
  `compact_member_forces_one_compaction_and_restores_the_profile` read the
  durable session surface on the identity's completion cursor, which leads it -
  the same clock split already documented in that file and already fixed once in
  its sibling. Its baseline is now settled and the growth polled, because a
  baseline snapshotted mid-write drifts upward on its own and would satisfy
  "did it grow" without the append it names. Both were found the same way: a
  diff that could not possibly have caused the failure it was blamed for.

### Known limitations

- **`MobRunStatus` coerces an unrecognized status to `pending` in both SDKs**
  (Python `MobRunStatus.parse`, TypeScript `parseMobRunStatus`). A status this
  release does not know is therefore reported to a host as NOT STARTED rather
  than as unknown - a wrong and actionable value. The new parity gate makes the
  drift that would trigger this a build failure, so the fallback cannot be
  reached silently by an upstream bump; changing the fallback itself is an SDK
  contract change and is deferred rather than made mid-release.
- **The console send path still flattens an ambiguous delivery into a failure.**
  The SSE surface and `cross_mob/send` were fixed this release; the console was
  not, and the reason is that its fix is not local. `IdentityRuntimeError`
  stringifies into `detail: String` at all four of its delivery phases, so the
  typed `AmbiguousDelivery` is already gone before `ConsoleSendError` sees it -
  it arrives as `Dispatch(String)` and is then reported as `"console send
  failed"` with a 500. Preserving it means changing that phase vocabulary, which
  is a multi-layer change to a hot path with no compile pressure forcing it, and
  release eve is the wrong time. Deferred to 0.8.19, named here rather than left
  for an operator to discover: **on the console surface specifically, a "console
  send failed" does not rule out that the message was delivered.**

## [0.8.17] - 2026-08-16

Paired release on meerkat v0.8.23.

**Mixed-version rollout warning (inherited from meerkat 0.8.23).**
`ToolAccessPolicy::ReadOnly` is serde-closed and persisted in durable session
metadata, so the hazard triggers on first USE, not on upgrade: once any session
carries a ReadOnly policy, pre-0.8.23 binaries cannot decode it. Roll the fleet
forward one-way BEFORE enabling read-only sessions.

### Changed
- Meerkat pins `=0.8.23`, which fixes the compaction-epoch seam defect: a
  seam-routed member with durable semantic memory could never persist a
  compaction, warn-only, retriggering every turn. Field evidence across the
  pin boundary: 36 attempts / 36 failures on 0.8.22, 2 attempts / 2 successes
  on 0.8.23.
- **Fail-closed supervisor routes** - no silent in-proc displacement anywhere.
  Same-id succession now requires the predecessor's terminal shutdown, which
  correctly releases the route (it previously leaked on every same-process clean
  restart). Two further shutdown-ordering bugs fixed: identities no longer park
  Broken during teardown, and a failed pre-spawn hook no longer leaks the route.
  **Adopter note:** a test suite that constructs multiple runtimes in one
  process under a shared participant name now fails closed with a message naming
  a public key. That reads as a crypto problem but is a test-isolation problem -
  before 0.8.17 each registration silently displaced the previous one, so such
  suites passed by accident. Use unique per-test mob ids, or have the incumbent
  stand down via terminal shutdown where succession is the intent.

### Added
- `ErrorEvent::CompactionPersistenceRejected` fires through the error hook on
  every rejected compaction commit (previously warn-only - fleets discovered
  wedged members by silence).
- `ErrorEvent::ActorLoopStalled` from a periodic heartbeat round trip, the only
  observation that can honestly claim loop scope. Knobs
  `MOBKIT_ACTOR_LOOP_PROBE_INTERVAL_SECS` / `_BUDGET_SECS`.
- Gateway operator verbs `mobkit/compact_member` (forced compaction via a
  profile floor plus rebuild) and `mobkit/bound_member_transcript` (pair-safe
  keep-last-N; typed Busy refusal on in-flight turns - quiesce first).
- `mobkit_repair` tool-result bounding: `--truncate-tool-results <max_bytes>`
  and `--drop-tool-results-older-than <N>`, in place, pair-preserving, and
  idempotent via the marker prefix `[mobkit-repair:tool-result-`.

### Fixed
- **P0**: `storage-migrate --apply` stamped the irreversible continuity ledger
  while converting nothing. Conversion now precedes the stamp, and blobs that
  changed after an earlier conversion are reconverted before it.
- `ActorAdmissionTimeout` stopped asserting loop scope it cannot observe.

## [0.8.16] - 2026-08-11

Paired release on meerkat v0.8.22. Delivers the owner-ratified 26-item
program; every item's disposition is stated below, including the items
deliberately refused and the ones shipped as documented partials.

### Removed

- **Item 14 phase B** - removed the second scheduling authority as one breaking
  contract change. The stateless `mobkit/scheduling/evaluate` and
  `mobkit/scheduling/dispatch` methods now return `-32601`,
  `runtime_options.scheduling_files` is rejected, and the Rust/Python/
  TypeScript static-evaluation surfaces are gone. `MOBKIT_CONTRACT_VERSION`
  is now `0.5.0`. Durable Meerkat `ScheduleService` storage, schedule tools,
  firing hosts, targets, `host_runnables`, and `callback/schedule_fire` remain
  the single scheduling path.

### Changed
- Meerkat pins `=0.8.22`. Ported nine adaptation seams plus the normalized
  provider-accounting contract: a stream that carries no `UsageUpdate` now
  fails its turn closed, so every LLM double must emit accounting.
- **Item 15** - console identity self-heal is demoted. The four
  identity-authority mutation sites in `inspect_identity` return typed errors
  instead of healing. A READ-ONLY console session (not merely a VIEW-scoped
  principal) could previously reach Suspend -> acquire_leases -> fence advance
  -> Broken; healing keeps its four write-path callers.
- **Item 13a** - the schedule executor lease is adopted as OBSERVATION ONLY.
  0.8.22's lease fences *claiming*, not firing-intent writes, so
  `FiringHostGatedScheduleTools` is RETAINED. Operator text now states that
  `Held` is a BOUNDED FACT and not proof of liveness: a host that died inside
  its lease window still reads `Held` for up to the lease duration.
- **Items 9, 10, 16** - dispatcher, gateway-composition and identity-resolution
  unification are partial BY DESIGN; the ratified text forbids a big-bang
  rewrite, so only domains that could be unified completely were, and the
  remainder is handed forward as a survey and a delta list.
- **Item 21** - scoped to the dead `Resumed.snapshot` field. `RestoreOutcome`
  has four live production consumers, so the item's title overreached.

### Added
- **Item 2** - remote runtime-host lifecycle: durable pairing, endpoint
  identity, capability discovery, health, reconnect and placement.
- **Item 3** - real fork through `UnifiedRuntime`, wrapping meerkat's durable
  `fork_member` and preserving running-source rejection.
- **Item 5** - per-slot WorkGraph store injection. `workgraph_store()` on
  `UnifiedRuntimeBuilder`, threaded through all THREE bootstrap paths;
  wiring only the persistent paths would have left a caller who sets a store
  without a state path silently on the memory fallback.
- **Item 6** - host-granted WorkGraph namespaces, scoped to meerkat's
  canonical mob realm.
- **Item 7** - WorkGraph transition facts bridged into event and SSE surfaces
  as lossy wake accelerants.
- **Item 11** - embodiment and restore doors merged into one per-identity
  park-and-continue path.
- **Item 12** - the declared-versus-resolved capability invariant.
- **Item 19** - thin transcript-edit exposure.
- **Item 23** - the LLM selector is re-homed and then deleted, in that order:
  `AnnotatedRecord` serves the lexical recall path, so deleting first would
  have broken default recall.
- **Item 25** - BREAKING Rust API removal: `MarkdownAgentMemoryStore` and
  `UnifiedRuntimeBuilder::persistent_agent_memory(...)` are deleted. Markdown
  is no longer a live agent-memory backend; its parser and file layout remain
  crate-private solely for the one-shot SQLite import, which preserves ids,
  tags and timestamps before renaming each source to `.md.imported`.
- **Item 26** - public-surface cleanup for deleted and parked capabilities.
- **Item 20** - Hygienist is PARKED, not deleted, pending real fork plus
  bounded projection proven on OpenAI and Anthropic.
- Memory output budgets (`max_output_tokens` on Steward and Distiller) are
  reachable from the Python and TypeScript SDKs and from the gateway
  allowlists. Hygienist is intentionally excluded from that public surface:
  absent/false/disabled config remains migration-compatible, while any
  activation-shaped value is refused with invalid params.

### Fixed
- **SIGTERM is handled by both gateway binaries.** They waited on
  `ctrl_c()`, which is SIGINT only, so container runtimes never ran the
  graceful path - and since 0.8.22 an ungraceful stop leaves the schedule
  executor lease held for up to 60s, silently suppressing schedule firing.
  The 2-minute claim watchdog is too coarse to observe it.
- The scope-pinned WorkGraph wrapper forwards the inner catalog surface.
  Leaving `tool_catalog_capabilities()`/`tool_catalog()` on their trait
  defaults silently downgraded the composed dispatcher to a non-exact, empty
  catalog - a different surface than the one being wrapped.
- Test integrity: MobKit's LLM doubles conform to the 0.8.22 accounting
  contract; assertions select their request by CONTENT rather than by
  position; turn readiness is a terminal event rather than a timer. A
  non-conformant double had been failing every turn in one file since 0.8.22
  while three tests passed anyway, because they only asserted that a request
  was ISSUED.

### Refused, with primary-source evidence
- **Item 22** (remove single-shot gateway mode) - REFUSED. Its gating
  condition was "if a downstream census confirms no use", and it does not
  clear: the TypeScript SDK publicly exports the one-shot transports and
  clients, and they spawn this binary with no arguments.
- **Item 24** (remove operator-scope machinery) - NO CHANGE. The premise
  ("zero activators") is false: `rpc_gateway` installs
  `ConsolePrincipalOperatorResolver`. `TrustTier::Operator` and re-derivation
  metadata carry real policy meaning and are retained.
- **Item 8** (delete `workgraph_admission`) - REFUSED pending the
  store-owned refusal contract actually being active.

### Not in this release
- **Item 4** (bounded helper-result path) and **item 27** (shell Allow-8
  deletion and heal) are not started.
- **Item 17** (recall and projection routing) is externally gated on field
  confirmation of typed recall without fallback.

## [0.8.15] - 2026-08-08

Paired release on meerkat v0.8.21.

### Changed
- Adapted to the meerkat 0.8.21 resume-verdict contract: session-service wrappers delegate `observe_session_resume_authority`; the operational resume entry is the upstream verdict composer, running through each wrapper's own overlaid load.
- `cross_head_canonical_authority` (required upstream format-door crossing): the continuity adapter routes to the owning store (superseded sessions present as absent), and the mobkit continuity trait carries a bridged default performing real reverification; absent/blob-canonical sessions return `NotApplicable`, preserving the legacy lane.
- Schedule claim results consume the upstream `TransitionCommit` carrier.
- Meerkat pins `=0.8.21`.

### Fixed
- Firing-intent schedule writes (`create`/`update`/`resume`) refuse typed while a gateway-owned store has no firing host bound (Bug C class); caller-injected library stores are never gated.
- External-tool composition warns per shadowed pre-installed tool (the scoped form of the recorder clobber).
- mobkit-repair prints a load-bearing post-apply reminder for the runtime scratch-store drop and ships `bytes_semantics` in its report (head-strand bytes, not disk).

### Added
- Regressions pinning the resume-tooling defect (activation-41) on both halves: a created member resolves declared categories, and a category declared AFTER creation resolves on resume (the meerkat 0.8.21 unified merge).
- Binary-level regression pinning same-identity typed agent-memory recall while the member's `callback/build_agent` is parked.
- Python SDK: typed `MobKitRuntime.list_identities()` returning `ConsoleIdentityRecord` (the supported identity -> session/profile map).
- `docs/proposals/simplification-audit-mobkit.md`: capability audit under the owner's simplification directive; seeds the 0.8.16 program.

## [0.8.13] - 2026-08-06

Paired release on meerkat 0.8.20 (the direct pairing: 0.8.19's
resume-authoring fix and 0.8.20's singular input-lane authority ship
together under one adoption; no intermediate 0.8.19 artifact exists).
Field-driven: every fix below was drilled against the downstream app's byte-exact
production corpus or the operator deployment's 69-board rig, and the exact shipping pair
(this tree + the 0.8.20 release tree) passed the 17-member restore gate,
the three-boot accretion drill, and a real end-to-end member
resurrection before tagging.

### Changed

- **Paired on meerkat 0.8.20.** All meerkat crates repin to the published
  0.8.20 line (both workspace pin sites). The pairing floor is MANDATORY:
  cold-mint graph hydration requires preserving_live audited-history
  acceptance (0.8.17+), the first-wake steer regression requires the
  queue-behind-kickoff fix (0.8.18+), and resumed builds author no prompt
  rows under 0.8.19+'s resume contract - all guarded live by this
  branch's regression suite on the exact pairing.

### Fixed

- **Cold mint hydrates the rewrite graph** (the downstream app's window-4 fleet
  blocker): `mint_runtime_authority_from_durable` seeded the runtime
  WholeBlob from the SLIM durable materialization, so on a REWRITTEN head
  every boundary projection composed rewrite-shaped state without compact
  graph authority and the store refused fail-closed ("rewritten session
  has no validated compact graph authority") - a sanctioned runtime-store
  reset then wedged every rewritten member in a refuse-retry loop
  (0 active / 17 broken). The mint now hydrates the graph from the
  store's own adopted rewrite records before sealing the seed bytes;
  legacy/import shapes whose records fail reconstruction pre-adoption
  seed slim as before, loudly. Drill-verified on the byte-exact field
  corpus: 17/17 mints at exact durable counts, zero refusals, echo turns
  green (parent-1: 4s/turn on the deduped head, down from 29s).
- **Broken registrations name their typed reason at warn**: both
  orchestrator Broken registration sites carried the typed
  `ContinuityFailure { kind, detail }` only into the roster outcome -
  never to the log stream - so a warn-baseline gateway saw 17 Broken
  identities with zero log lines. Each Broken registration now emits one
  warn line with identity + kind + detail.
- **App-supplied delivery correlations are canonicalized at the dispatch
  door** (the downstream app's 2h outage on 0.8.12 + meerkat 0.8.16): non-UUID app
  correlation strings now map deterministically to UUIDv5 under the
  mobkit delivery namespace instead of drawing typed refusals from the
  fail-closed delivery-identity matrix; the schedule lane stays strict.

### Added

- **Console WorkGraph graph view**: a Tree|Graph toggle in the workgraph
  panel rendering a layered DAG (hand-rolled SVG, zero new dependencies)
  with status-colored nodes, arrowed parent edges, dashed labeled blocks
  edges, wheel-zoom/drag-pan/fit, click-to-inspect, and a 200-node cap
  with honest "+N more" overflow on both views. Playwright e2e
  (`e2e:workgraph`) boots a real console against a seeded fixture and is
  wired into CI.
- **Operator surgery helper** `examples/dedup_system_rows.rs`: drops
  replayed duplicate System rows from a live session's durable transcript
  through the typed rewrite door - groups by (content, identity), keeps
  the first occurrence per group, hydrates the rewrite graph from the
  store's own records, one full-range rewrite commit + authoritative
  projection. Field-proven in the downstream app's window 4 (17/17 applies, parent-1
  36 -> 4 System rows, heads 80-460KB from 82MB-class documents).
- **Load representation traces**: `load_persisted_session` and
  `load_previous_session_for_save` emit representation+count debug lines
  so a mint-vs-save-guard representation split is attributable from a
  debug log instead of a store dump.

### Tests

- `reset_after_operator_rewrite_cold_mints_with_graph_authority`:
  window-5 mirror (operator dedup rewrite -> sanctioned reset -> cold
  boot -> real turn); red with the byte-exact field refusal pre-fix.
- `reset_after_repeated_customizer_boots_preserves_exact_counts_and_projects`:
  multi-boot count faithfulness with 12 seeded field-shape duplicate
  System rows, exact counts asserted at every hop from durable load to
  first projection.

## [0.8.12] - 2026-08-05

### Changed

- **Paired on meerkat 0.8.16.** All meerkat crates repin to the published
  0.8.16 registry release. What the pair carries for MobKit deployments:
  - The fan-in admission stall is fixed upstream: durable input admission
    returns without awaiting any ephemeral actor-boundary handshake, so a
    wedged live-boundary preparation can no longer silently starve every
    queued peer delivery behind it (the operator deployment's field class: 30-of-69 completion
    pings, then an idle member with queued inputs and no wake).
  - WorkGraph/Mob Flow interoperation (meerkat PR #941): flow executions
    bind WorkGraph work items (`MobRun.flow_definition_digest`,
    `mob/flow_status.execution_binding`, `Realizing`/`ExistingRealizing`
    realization states); MobKit's workgraph tool surface carries the new
    optional typed execution provenance on `WorkEvidenceRef`
    (`execution_binding_id`; MobKit's attention-keyed provenance paths and
    the RPC wire surface mint no execution binding, explicit `None`).

### Fixed

- **Converged rows no longer WARN from the tear-repair diagnostic.**
  Repair passes after a heal match no admission by design (nothing to
  repair), and the "no repair admission holds" WARN read as a failure in
  the downstream app's v0.8.11 production validation. Content equality with the
  committed authority now logs debug and returns.

## [0.8.11] - 2026-08-04

### Fixed

- **Internal deliveries dedup across crash redelivery, and the dispatch
  door runs full delivery preparation** (meerkat 0.8.15 pair; held
  uncommitted until the repin). Two halves of one change set:
  - Delivery-identity threading: both internal work doors - the identity
    bridge's `InternalBridgeWork` submit and the schedule host's direct
    fallback - now submit through meerkat's deduplicating
    `submit_work_with_mode_and_delivery_identity` whenever a delivery
    identity (idempotency key + occurrence-UUID correlation) is present.
    meerkat derives the WorkRef from mob + member identity + idempotency
    key, stable across lease-expiry reclaim, so a crash redelivery of the
    same occurrence resolves to the SAME work instead of a duplicate turn.
    The scheduler sink now threads BOTH identity halves through
    `DispatchInput` into runtime admission (the 0.8.12-era "internal lane
    closed upstream" note is retired with the mechanism it documented).
    Admission is fail closed and typed, validated BEFORE the bridge:
    (None, None) is an ordinary delivery; a full pair validates upstream
    (canonical non-nil UUID correlation) and carries, with the correlation
    id also riding as the interaction id; EVERY other combination -
    half-pair, invalid UUID - raises `InvalidDeliveryIdentity` and NO
    delivery occurs. `SessionBridge` deliveries were rebuilt around one
    typed request: the REQUIRED `deliver_admitted(runtime_id,
    BridgeDelivery)` method that every implementation must service (the
    convenience `deliver*` methods are provided forwarders that build the
    request), so no implementation - production or test double - can
    silently drop a delivery identity; a bridge that cannot honor one
    refuses typed.
  - Shared delivery preparation: `dispatch_with_expected_member_alias`
    previously delivered RAW - internal dispatches (schedules foremost)
    skipped inbound defanging, taint session attribution, and ambient
    memory injection for the member's whole lifetime (the downstream app: zero
    surface=Turn injection-ledger rows ever). Both member doors now run
    the ONE preparation helper (note_current_session + generation bind,
    defang, inject_for_turn), and dispatch populates the admitted
    delivery request with the injected recall as its own typed body.
    `DispatchOrigin` is untouched; Steer keeps its deliberate bypass on
    the send door.

- **Steward dream honesty: turn-only usage evidence, quarantine window
  pin-down, durable run rows** (the downstream app's dream rehearsal). Three findings,
  one root shape:
  - Usage-audit data boundary: the audit now judges only `surface=Turn`
    injection evidence - build-surface rows are spawn hydration
    bookkeeping, not proof a record earned its context slot. A store with
    zero Turn rows for the scope SKIPS the audit and queues NOTHING
    (the downstream app: 53/53 dead-weight noise verdicts minted from hydration
    counts on a store where turn evidence could not yet accrue).
  - Legacy quarantine review: proven from code that no
    "new-since-last-dream" window exists - every dream loads the full
    quarantine queue (capped per dream) regardless of record age; the
    rehearsal's unverdicted legacy records were starved by the pipeline
    aborting in the consolidate phase AFTER the audit sheet persisted.
    A regression pins the contract: a quarantined record older than the
    newest recorded dream still enters signals and gets its verdict.
  - Dream-run bookkeeping: the `dream_runs` row is now written at run
    START (in-flight), replaced at the tail with final numbers, and
    replaced with an honest failure row (committed-op count + failed
    phase) when the pipeline aborts - verdict rows can no longer
    reference run ids that resolve nowhere, and the console dream-runs
    panel sees failed runs instead of nothing.

- **WholeBlob-to-durable projection tear healed: missing rewrite commits
  replay through the typed rewrite door** (the downstream app's parent-1 production
  park). The runtime-store facade projected committed WholeBlob
  boundaries into the durable session store with a plain authoritative
  projection, which cannot INSTALL a new rewrite generation - a
  wedged-turn retire committing a rewrite-advanced boundary left the
  durable row torn (graph one generation ahead of its head), and
  meerkat's rewrite-save invariant then refused every subsequent resume.
  The projection now runs per-runtime single-flight, loads the exact
  committed successor and the durable predecessor, and when the
  successor's PROVED graph extends durable state it installs each
  missing rewrite commit through `SessionStore::save_transcript_rewrite`
  (exact prefix session projected at that commit) before the ordinary
  trailing-append/envelope projection. Every step is monotonic and
  validated against the head the previous step installed, so a partial
  failure re-converges on the exact retry; no branch overwrites durable
  state the successor graph does not prove. The freshness probe gained
  the matching half: durable-behind-committed is now distinguished from
  fresh and runs the same reconciliation on a PLAIN RESUME (no new
  committing verb required), including under a lifecycle terminal -
  repairing the durable projection of already-committed authority mints
  no runtime life; the terminal gate stays on the reseed direction where
  resurrection risk actually lives. Field-recovery property:
  parent-1-class tears self-heal on the first post-upgrade resume - the
  committed runtime authority still holds the full graph, and the
  resume-path freshness pass replays the missing commit into the durable
  row; no hand-repair of wedged stores. The append-before-compact shape
  (a committed rewrite whose parent revision is a strict APPEND-extension
  of the durable head) is covered too: the reconciler first installs the
  exact proof-carrying parent via meerkat 0.8.15's
  `Session::with_validated_transcript_rewrite_parent_projection`, then
  replays the rewrite commit against its exact parent.
  Real-bytes corpus hardening (frozen parent-1 backup, five field
  iterations): the repair works on a PARKED member whose session is
  explicitly unregistered - the projection doors admit the
  durable-observed non-superseded head as a parked repair, write
  authority hydrates from the durable continuity record via the new
  fail-closed `ContinuityStore::resolve_record_by_session` (identity,
  generation, fencing token, fence-current checkpoint re-seed), and
  archives, creation windows, removed documents, and suspended sessions
  keep their refusals. Two proof-carrying admissions extend the chain
  walk for tear shapes it cannot prove, both by exact content digest:
  the durable row extending PAST the sealed parent (its unacknowledged
  suffix is REBASED over the compacted head, preserved, with the
  committed snapshot converged to the rebased state in the same pass)
  and the true parent-1 shape - the durable row as a strict PREFIX of
  the sealed parent (a failed projection; admission authored by
  the downstream app and validated on the live corpus, GATE_PASS 17/17). All
  admission verdicts log unconditionally with both digests; a durable
  row proving neither shape is a foreign lineage and keeps the typed
  refusal, byte-untouched.

- **Memory scope keys are LOGICAL identities end to end** (the downstream app
  activation smoke, launch blocker). The platform's observe-stream paths
  keyed identity scopes by the mob-plane roster id - for identity-first
  members the comms-safe encoding of a generated runtime alias
  (`mk--rt_cidentity_cparent-1_c0`), one per respawn generation - while the
  SDK `agent_memory` RPC, per-turn injection, and the recorder key by the
  logical `AgentIdentity` (`identity:parent-1`). Result: distiller-extracted
  memories landed in per-incarnation scopes invisible to injection and to
  SDK reads, fragmenting further on every respawn. Fixes, all through ONE
  normalization primitive (`member_comms_id::logical_memory_identity`,
  the decode-then-strip parser the dispatch-taint join introduced,
  relocated to the codec owner):
  - the member event observer fans out the LOGICAL identity to every sink
    (distiller/hygienist triggers now key the same scope the SDK reads;
    taint attribution becomes consistent with the dispatch-time join);
  - the distiller and hygienist trigger sinks re-normalize as a cheap
    fixed point, so no direct caller can re-split scopes;
  - the classic-path spawn customizer strips the generated runtime-alias
    shape instead of keying build injection per incarnation;
  - store migration `mobkit-memory` v3 (ledger-gated, data-only) folds
    existing runtime-id-keyed rows into the logical scope across records,
    proposals, pending promotions, the injection ledger, and the harvest
    queue (PK collisions collapse; merged scopes may briefly hold
    duplicate content until the next steward dream - rows keep distinct
    ids, nothing is lost), and normalizes the `MemoryScope` embedded in
    surviving stage-table batches' Create ops - stage tokens outlive
    boots inside the 24h GC window and operator-gated promotions commit
    later, so a key-column rewrite alone could be undone by a stale
    batch apply. Existing v2 files migrate on first open behind a frozen
    v2 fingerprint verifier.
  - Composition-time collision WARN: a host callback tool named `memory`
    now warns loudly against the agent-memory recorder at build
    customization (the overlay layer already warned-and-shadowed; the
    wire-declared surface now warns too, naming both tools and the
    remediation).

- **Repair honesty: the typed `ArchivedNotRevivable` refusal parks on the
  FIRST pass instead of heal-looping** (the operator deployment's prod-data rehearsal at the
  0.8.14/0.8.10 pins, 4258-session corpus: 4 personal-agent identities whose
  latest sessions carry a 0.6.x body-written archived terminal with no
  runtime record entered an endless heal/refusal loop - continuity repair
  reported healed at the roster level, the next inbound turn re-hit
  meerkat's typed `SessionUnavailableForResume { reason:
  ArchivedNotRevivable }` at materialize and re-marked Broken, repeat, with
  user-facing canned failures and no convergence; the 0.8.10 N=3
  byte-identical-failure park never engaged because the heal itself kept
  succeeding). The refusal is a stable, deterministic materialize
  precondition no retry can change, so it is now classified typed
  (`ResumeRejectionKind::ArchivedNotRevivable`, from the error VARIANT,
  never wording) and both resume doors - eager restore and on-demand
  materialize - record the terminal `continuity_unrecoverable` verdict on
  the FIRST refusal, the same producer pattern as
  `CommittedBoundaryRepair::Unprovable`. The repair supervisor then parks:
  zero heal/re-Break cycles, no heal-authority calls, no reconcile churn,
  and the durable session (the transcript) stays bound and untouched. The
  eager restore outcome surfaces under the terminal
  `CheckpointUnrecoverable` kind rather than the reconcile-retried
  `ResumeRejected`. The verdict reason carries the operator path inline:
  upstream archived-session revive lands in meerkat 0.8.15 (promoted to
  upgrade blocker on real user data); until then `mobkit/reset` is the
  deliberate fresh start, and the park is process-local (a gateway restart
  re-attempts once after an upstream fix).

- **Memory-taint first-ingestion race closed at dispatch time** (§10.1
  launch-audit item). Content-trust classification no longer depends on the
  observe-only ASYNC agent-event stream alone: an LLM memory write in the
  same turn as the session's FIRST untrusted tool ingestion could reach the
  store before the taint observer processed the tool event
  (`llm_writes = "observed"` default), and MCP tools with unqualified names
  could not be attributed to a server at all (events carry only the tool
  NAME). Both are closed by a synchronous LLM-boundary join:

  - Every mob member session create (spawn, resume, revival - all funnel
    through the bootstrap spec's pre-build seam) now composes a
    taint-observing wrapper onto the member's agent-facing LLM client via
    `SessionBuildOptions.agent_llm_client_decorator`. Before each LLM call
    it classifies the tool results newly present in the request - joining
    the tool name against the request catalog's typed `ToolDef.provenance`
    (`ToolSourceKind::Mcp` + server id), so unqualified MCP tool names
    attribute correctly; absent provenance falls back to the existing
    name-based classification unchanged. After each call it classifies the
    typed `ServerToolContent` blocks (provider-executed web search /
    grounding) before the loop can dispatch any same-response tool call.
    An LLM-authored write is always downstream of an LLM call that carried
    the untrusted result, so the tracker is marked strictly before the
    write reaches the store's gate. Observe-and-mark only: the wrapper
    never denies, never mutates, does no I/O.
  - Mechanism note: meerkat 0.8.14 DOES fire
    `HookPoint::PostToolExecution` synchronously with the typed provenance,
    but the mob member build path has no hook-engine carrier
    (`SessionBuildOptions` cannot ship an `Arc<dyn HookEngine>`;
    `AgentBuildConfig.hook_engine_override` is reachable only from the
    standalone facade builder), so the join rides the sanctioned
    per-build LLM-client decorator seam - identical ordering guarantee
    for LLM-authored writes.
  - The tracker slot is late-bound (`MobBootstrapSpec::dispatch_taint_slot`):
    the unified builder and the RPC gateway fill it when the full
    agent-memory stack attaches, and members built earlier (bootstrap
    roster) pick it up on their next LLM call. Compositions without the
    taint firewall pay nothing. The async observer stays wired as
    belt-and-suspenders (it also serves session-rotation mirroring); the
    dispatch join simply gets there first.

## [0.8.10] - 2026-08-03

### Changed

- **meerkat dependencies repinned `=0.8.11` → `=0.8.12`** (all 19 meerkat
  crates in `meerkat-mobkit`, all 5 in `mobkit-store-conformance`). 0.8.12 is
  an upstream hotfix release: fan-in stale admission drops, abandonment
  flattening, the silent live-actor discard (the actor is now preserved on
  recoverable turn errors), transient-context transcript-identity false
  re-proof, terminal-receipt sequence reuse, and a live-boundary run-advance
  race. Full suite complete-run green against the registry crates before any
  mobkit-side change (2181/2181, idle gate excluded per its shared-runner
  disposition).

### Fixed

- **Continuity repair no longer destroys a member's queued work** (the operator deployment's field
  runs 33758a41 + 6bb7010e: repair healed a stream-dead-but-Attached review
  member by full disposal and took its 15 pending fan-in inputs with it —
  irrecoverably, on the ephemeral runtime-store shape). Three changes, all in
  the identity-first repair paths:

  - **Queue carry.** Before the destructive retire, the repair captures the
    member's pending machine ingress (admitted-but-not-run inputs, payloads
    included) through the composition's runtime machine, and re-admits it
    into the healed successor session after the resume — fresh input
    identity, idempotency keys dropped (the originals were durably
    terminalized by the disposal), admission order preserved. Prompt, peer,
    and external-event inputs carry; flow-step and runtime-internal inputs
    cannot (their correlation is owned by the flow engine / runtime) and are
    DESTROYED LOUDLY instead — one warn line per input id, with the class
    and reason, before disposal proceeds.

  - **Preconditions first.** The collision-repair retire now proves the
    resume source exists in DURABLE, NON-ACTOR authority (the continuity
    store row, else the session service's archive-authority predicate)
    BEFORE destroying the stale member. An actor-routed read is never used:
    it waits on the very wedged member the repair is disposing (proven live
    at the meerkat 0.8.13 repin as a self-deadlock inside the probe). The
    raw injected SessionStore row is also deliberately not consulted; since
    the 0.8.11 store-owned repin runtime-backed compositions never project
    into it. A confirmed-absent resume source refuses typed with no
    destructive step taken: retiring would destroy the only live copy of
    the session. A probe fault means absence cannot be confirmed, and repair
    proceeds as before the guard existed.

  - **Bounded non-identical retries.** The continuity repair supervisor
    tracks per-identity failure signatures across passes; three consecutive
    byte-identical failures park the identity typed
    (`continuity_unrecoverable`, reason naming the blocking failure verbatim)
    instead of re-executing destructive repair steps on a timer. An operator
    clears the park to retry; a changed failure signature resets the streak.

- The three tests parked on the upstream meerkat-mob actor defect family are
  re-armed — fixed by meerkat 0.8.12 (the lost-actor cleanup): S2
  (`explicit_identity_query_refreshes_stale_existing_session_history`, the
  "autonomous dispatch inject failed: inbox closed" class), S3
  (`agent_events_route_resolves_durable_identities_and_aliases`, HTTP 500 on
  the member-observation route), and S4
  (`studio_k0_identity_first_gateway_retire_respawn_succeed_on_idle_members`,
  the intermittent retire/respawn hang). Each passed 3/3 re-arm runs at the
  0.8.12 pin (S4 solo, well under its historical hang horizon); the
  `#[ignore]` attributes and their run records are removed.

## [0.8.9] - 2026-08-02

### Changed

- **BREAKING: meerkat dependencies repinned `=0.8.10` → `=0.8.11`** (all 19
  meerkat crates in `meerkat-mobkit`, all 5 in `mobkit-store-conformance`).
  0.8.11 is the store-owned session-authority reset; the visible behavior
  changes for MobKit embedders:

  - **Resume authors nothing.** The transcript is ordered rows; System
    messages are ordinary ordered `Message::System` entries preserved
    byte-for-byte across resumes. The per-boot "resume-system-prompt-refresh"
    rewrite is retired (and with it the 60-100x transcript-history revision
    bloat it minted). Per-turn System instructions ride the new explicit
    carrier: `WorkSpec.system_prompt` through the mob deliver path
    (`SessionBridge::deliver_with_mode_context_and_system_prompt` →
    `RuntimeTurnMetadata.system_prompts`), appended as ONE ordered System row
    at that turn's boundary.

  - **Pre-ledger storage refuses typed.** Databases written before the
    mobkit 0.8.8 schema-ledger floor (no `meerkat_schema` row for an owned
    domain) are refused at open with rows left untouched — across continuity,
    memory, metadata, and console stores. The former silent pre-floor
    convergence is retired; upgrade stepwise through a 0.8.8-0.8.10 binary
    first.

  - **Released 0.8.10 session envelopes import exactly once on load.** The
    continuity adapter's load path is the sole importer
    (`import_released_0810_session`); ordinary decoding rejects released
    carriers. Zero-rewrite released histories (the universal mob-supervisor
    shape: transcript graph with zero commits and a singleton live-head body)
    import via the upstream d6cafd405 acceptance; divergent bodies still
    refuse.

- **Profile-declared fields are auto-marked as resume overrides** ("profile
  declares it, profile means it"). Durable session metadata restores `model`,
  `provider`, and `provider_params` on resume, so a profile edit was inert on
  every identity that already had a session unless the profile also listed the
  field in `resume_overrides` — two production fleets shipped model migrations
  that silently did nothing (one had identities running a three-week-old model
  until a provider byte cap broke the deployment). Now, at runtime bootstrap,
  every inline definition profile gets its explicitly declared fields marked
  resume-overridden automatically. `model` and `provider` are treated as a
  **coherent pair, never independently masked** (the operator deployment's cutover incident: a
  model-only mask let the durable provider survive under a profile model it
  was never registered for, and the resume was rejected typed with an invalid
  `(model, provider)` pair): a declared `provider`/`self_hosted_server_id`
  masks the pair as written; a declared model with no provider key derives
  the provider from the canonical model catalog (or the definition's
  `[models.<id>]` entry) and writes it onto the profile so both apply
  together; when no coherent provider is resolvable, neither field is masked
  and durable truth wins whole (with a unified resume-divergence INFO line —
  once per identity per boot — printing both halves of the pair).
  `provider_params` masks independently when present. Undeclared fields keep
  durable-wins semantics, and an explicit `resume_overrides` list is
  preserved (declared fields are added, never removed). The same pair rule
  covers customizer/draft model pins: the pinned-profile spawn snapshot now
  carries the DRAFT model's catalog owner instead of a cleared provider,
  which on resume fell back to the durable provider under the pinned model —
  the same invalid-pair mint through a different door.

  **Migration note:** fleets that relied on durable-wins for a *declared*
  profile field — e.g. a profile that names `model = "x"` but expects resumed
  identities to keep whatever model their durable session recorded — must now
  express that per-identity intent through a draft/identity-level override
  instead of an outdated profile declaration, because the profile declaration
  now wins on resume. Realm-referenced profiles (`realm_profile = "..."`) are
  not auto-marked; the new resume-divergence INFO line is the tripwire for
  those.

- **The 0.8.8 observability lines are now visible at default configuration**
  (all four legs of the field investigation). (a) Both gateway binaries
  default their tracing filter to `warn,meerkat_mobkit=info,<gateway>=info`
  instead of a blanket `warn`, so this crate's own INFO reporting (conversion
  progress, continuity repair) passes at default config while dependencies
  stay at WARN; `RUST_LOG` still overrides everything. (b) The Python SDK now
  inherits gateway stderr by default (`MOBKIT_GATEWAY_STDERR=devnull` opts
  out; `MOBKIT_GATEWAY_STDERR_FILE=<path>` redirects). (c) The TypeScript SDK
  pipes gateway stderr to the host process's stderr by default, same opt-out
  shape. (d) `mobkit_gateway` installs its tracing subscriber BEFORE the
  storage maintenance verbs (`storage-migrate`, `storage-prune`,
  `storage-adopt-checkpoints`), so migration progress prints live instead of
  being dropped pre-init. A production deploy was aborted because a
  supervisor read a silent-but-working migration as a hang, and a week of
  panic-hook lines went to `/dev/null` — these four legs are why.

### Removed

- **BREAKING: checkpoint adoption tooling.** The 0.8.11 reset retired the
  checkpoint vocabulary meerkat no longer reads: the
  `storage-adopt-checkpoints` maintenance walk, checkpoint adoption module,
  digest-format marker stamping, and their tests are deleted.

- **BREAKING: storage doctor finding codes**
  `legacy-unverified-continuity-snapshots`, `checkpoint-digest-mismatch`, and
  `checkpoint-metadata-invalid` are removed (their subjects no longer exist at
  0.8.11). Replacements: `released-0810-continuity-snapshots` (censuses
  released-format rows awaiting one-time import) and
  `continuity-head-materialization-failed` (a head-canonical session whose
  slim materialization fails, now reported with strand, row counts, rewrite
  count, and prefix/anchor presence). Operator tooling keying on the removed
  codes must migrate.

### Added

- **Host-rejected builds park typed instead of Broken-with-continuous-repair**
  (the herd-investigation gate-rejection class). When a member build fails
  because the app-side `callback/build_agent` round trip COMPLETED and the
  host answered with an error (the candidate-mode effect gate), the identity
  now parks with a typed, operator-visible verdict on the first attempt:
  materialization fails fast with `HostRejectedBuild` (no bridge/callback
  churn) and the continuity repair supervisor skips the identity instead of
  retrying at 30s→10min forever — each retry previously re-asked the same
  deterministic gate the same question at the cost of a full member build
  plus a callback round trip. The park is scoped to the exact roster spec:
  a spec change (digest mismatch) clears it and re-admits exactly one new
  attempt, as does `clear_host_rejected_build_park` (operator retry).
  Transport-tier callback failures (closed transport, timeouts) are NOT
  parked — those stay on the existing retry lanes, whose backoff is fixed
  separately upstream.

- **Resume-divergence tripwire**: when a resumed identity's durable session
  metadata restores a `model`/`provider` that differs from the profile's
  declaration and no `resume_overrides` mask covers the field, the identity
  session bridge logs an INFO line with both values, the profile name, and the
  identity — once per identity per boot. After declared-field auto-mark the
  only case that can fire today is an inline profile whose declared model
  resolves no coherent provider; realm-ref profile declarations are NOT
  visible to the tripwire (the realm profile store is not threaded into the
  session bridge), so a realm-profile edit that loses to durable metadata is
  currently silent — threading the realm store is a tracked follow-up.

### Fixed

- **Live reset/reprofile no longer wedges the gateway when the superseded
  session commits a boundary mid-replacement.** Live reset replaces the
  identity's continuity record first and retires the superseded runtime
  through deferred cleanup debt - deliberately, per the reset contract (the
  old bridge projection is rollback authority until the replacement
  commits, and reset must not wait on a hung old retire). In that window
  the superseded session's runtime is still live, and any boundary it
  committed failed its durable write-through projection with the store's
  cursor refusal ("continuity record not found"); propagated, that failed
  the committing verb, escalated the runtime into repair-blocked retention,
  wedged the deferred retire behind it, and blew the gateway's bounded
  shutdown horizon (caught by PR CI's Python gateway test - the local
  pytest gap had hidden it). A record that names a newer binding for the
  same identity IS the supersede fact, discovered lazily under the identity
  fence, so the projection now drops such a write with the exact semantics
  the superseded-session pins already establish (terminal writes drop
  without parking) - with NO persistent supersede mark, so a reset that
  rolls back re-enforces the same session's writes cleanly. Every other
  projection failure stays fail-closed. Retire-before-replace was
  considered and excluded: it is structurally contrary to the reset
  contract's own pinned tests.

- **Zero-semantic-change boots no longer rewrite session heads (exactly-once
  adoption restored).** Upstream projects `ToolNameSet` (a HashSet) through
  serde when stamping `session_tool_visibility_state_v1`, so a session
  carrying a multi-tool Allow filter re-stamps the SAME visibility fact as a
  differently-ordered array every boot (per-process hash order), and the
  boot also touches `updated_at` - the strict exact-resave equality saw a
  changed head and rewrote it every boot (field: one fleet session's head
  churned bytes/checkpoint on every boot after its one-time adoption,
  violating exactly-once). The exact-resave equality now recognizes
  precisely those two facts as zero durable change: `updated_at`
  (timestamps are not durable content) and the ORDER of the
  tool-visibility Allow/Deny arrays (set semantics by the type's own
  definition; no other array in the document is touched). Pinned on the
  fleet's real consecutive-boot head rows and by a zero-turn eager-boot leg
  over the adopted closure that asserts ZERO head writes. Filed upstream:
  durable bytes minted from HashSet iteration order.

- **Released rewrite-carrying heads adopt on the first projected write
  instead of dead-ending the boot.** A released 0.8.10 head that RETAINS
  REWRITES structurally cannot authorize a current mutation: its
  rewrite-generation authority predates the compact graph/rewrite-prefix
  carriers, so `session_head_cas_token` refuses it typed ("rewritten current
  head has no compact graph-prefix authority") and every ordinary write arm
  is unreachable. The head-lane import (below) made such corpora READABLE,
  but the first projected boundary write at boot - the resume-spawn control
  snapshot - still failed fleet-wide (17/17 identities degraded pending
  retry; fail-closed held, durable rows preserved). The write path now takes
  a sanctioned adoption lane: the stored released document is re-proved
  through the one-time importer inside the write transaction (the import
  receipt is the authorization, never the released head), the incoming
  document must be a legal successor of that imported reading (genuine
  divergence refuses typed), and the released representation is replaced
  wholesale with the current-format layout - the same strand/rewrite/head
  writer the legacy-blob migration uses. Released heads with
  `rewrite_count = 0` (the committed R1 realms' shape) keep their ordinary
  write arms unchanged. Transcript-rewrite commits over a released head
  remain fail-closed by design: at 0.8.11 resume authors nothing, so the
  first projection always adopts before any rewrite can arrive.

- **Released 0.8.10 HEAD-CANONICAL continuity documents import on load**
  (the head-row lane of the released-envelope import). The one-time importer
  covered whole-blob rows only, while every 0.8.10-written head row carries a
  v2 session envelope that current materialization refuses typed
  ("failed to restore session from head row: ... expected current 3, got
  2") - so an entire released head-canonical fleet was unreadable at resume
  (field: 17/17 identities degraded on the reset-mint leg; the fail-closed
  side held, durable rows preserved). `materialize_slim_in_txn` now routes a
  v2 head into `import_released_head_in_txn`: the exact durable strand rows
  are first proved against the released head commitment
  (`released_0810_transcript_serialized_rows_digest` must equal
  `head_revision`), the released envelope is reassembled from those exact
  bytes plus the head's inline envelope facts, and the sanctioned
  `import_released_0810_session` boundary interprets it (receipt digest and
  session id re-proved). Read-side interpret only: durable adoption follows
  the first write-path decode, which rebases the strand under a
  current-format head. Regression drives the committed released baseline
  realm with its runtime store deleted - the downstream app's exact binding leg.

- **A restored/rolled-back runtime store no longer serves stale committed
  authority over the newer durable continuity row** (advisory Form 1, the
  0.8.9 stale-runtime-snapshot failure recurring at the 0.8.11 store-owned
  repin). The write-through projection makes "durable strictly newer than
  committed runtime authority" impossible in normal operation, so observing
  the inversion proves runtime-store loss; the facade now probes once per
  runtime per process on the store-owned read verbs and re-seeds the
  committed runtime authority from the durable session row (ordering on the
  monotonic pair: transcript rewrite generation, then message count, so a
  compacted-shorter durable document still orders ahead). Resume replays the
  full committed head instead of silently dropping durably recorded turns,
  and the following boundary can no longer project the regression back over
  the durable document. Catalog entries carrying a lifecycle terminal are
  left untouched.

- **A divergent runtime reseed refuses typed instead of silently adopting
  the durable side.** The staleness freshen above initially left fork
  detection to the inner store's seed verb, but `commit_session_snapshot`
  consults its LEGACY previous-row table for the boundary save guard - empty
  on 0.8.11 store shapes - so a durable row that ordered newer WITHOUT
  extending the committed document was classified as first-save adoption and
  silently replaced the committed runtime authority (the pick-a-winner
  data-loss class; fleet operators restoring runtime stores from backups hit
  this path). The boundary save guard now runs facade-side against the exact
  committed snapshot before any reseed: genuine divergence surfaces the
  guard's typed continuity refusal, repeatably, with both documents left
  untouched. Caught by the direction pin written for the freshen, not by the
  suite.

- **Archived sessions read as archived through the resume seam again.** At
  meerkat 0.8.11 the archive protocol never rewrites session BODIES to carry
  archive authority - the absorbing terminal is a RuntimeStore-owned fact
  (catalog entry or Retired/Destroyed lifecycle row) - while the mob resume
  seam still classifies from the body terminal, so a retired member's intact
  preserved document read back `Revivable` with no archived terminal (the
  0.8.6 field failure shape: hosts rotated identities off preserved
  transcripts). The session-service facade now overlays the store-owned
  terminal onto `load_session_for_resume` reads; both gateway binaries and
  the persistent library composition thread the shared runtime store as the
  overlay authority (`with_runtime_archived_terminal_authority` for
  externally-composed specs).

- **A byte-exact head resave no longer masks a stale fencing token.** The
  head-path exact-resave noop (which keeps a zero-change save from minting a
  checkpoint version) now requires the store-side
  `session_head_matches_current` probe - head equality AND current write
  authority, the head-path mirror of the whole-blob match probe's fence
  predicate - so a fenced-out writer falls through to the fencing write verb
  and hears the ordinary stale-fence refusal instead of a silent `Ok`.

- **An unregistered save refused over a durable head-canonical document now
  publishes positive lifecycle authority.** The refusal path recorded
  nothing, leaving the session's lifecycle to be re-inferred from absence; it
  now records `DurableObserved` (observation-grade: newer in-flight evidence
  wins), keeping the head-canonical refusal on the same lifecycle footing as
  the blob parking guard.

- **Ephemeral-runtime durability round trip is now facade-owned** (the production
  pod-scratch shape: durable truth in an injected session/continuity store,
  runtime store on ephemeral pod scratch). At 0.8.11 the session service
  keeps no plain `SessionStore` write path (WholeBlob session authority lives
  only in the `RuntimeStore`), so `SessionStoreBackedRuntimeStore` now owns
  both halves of durable interop: committed session boundaries WRITE THROUGH
  to the injected store after every committing verb (fail-closed: an external
  write failure fails the commit, and a retry of the same prepared boundary
  converges from the already-current inner successor), and cold activations
  RE-MINT store-issued runtime authority from the durable row (per-runtime
  single-flight fencing; a late seed never overwrites an advanced boundary).
  The mint arms on EVERY composition carrying a durable session source,
  durable SQLite/provider runtime stores included: an absent runtime record
  over a durable inner store is either a never-persisted session (the mint
  declines, the typed refusal stands) or a reset/lost runtime store — the
  sanctioned recovery path, which now reseeds instead of refusing every
  resume. Destroyed sessions cannot resurrect through it because identity
  deletion removes the durable row under the identity fence. Both gateway
  binaries and the persistent/identity-first compositions thread the same
  facade (`epoch_tracking_runtime_store_with_durable_projection`). On
  incremental-capable substrates the continuity adapter BIRTHS the
  head-canonical representation on a registered session's first projected
  boundary (strand rows + initial head under a create-CAS; legacy/imported
  blobs convert via the synthesizing head read), so the O(delta) steady
  state engages for new sessions exactly as it did when the 0.8.10 service
  drove the incremental channel - head birth moved seams, it did not
  disappear. The bounded-bridge composition (ephemeral session service +
  runtime machine) completes the post-commit boundary acknowledgement the
  upstream ephemeral service refuses on principle, so runtime-backed
  ephemeral turns no longer fail after their boundary committed.

- **Migrated head-canonical sessions no longer fail cold materialization
  after one 0.8.11 turn.** Both head-canonical plain-append writers (the
  continuity adapter and the local store) re-minted the head's byte-exact
  row commitment from TODAY's re-serialization while durable rows keep the
  bytes they were written with; `SessionHead::into_session` verifies that
  commitment at 0.8.11, so every fleet session migrated by the 0.8.10
  head-canonical bump would refuse to materialize after its first
  post-upgrade turn. The successor head now extends the STORED commitment
  with only the appended rows' bytes
  (`SessionHead::from_session_with_proved_inline_storage_authority`).

- **Deleting an identity now deletes its durable session row** (CAS-delete
  under the advanced identity fence inside the delete transaction, after
  projection writes are quiesced). Previously the row outlived the identity,
  and on the ephemeral-runtime shape the activation mint would faithfully
  resurrect the deleted transcript on the next cold boot or identity reuse.
  Stores that cannot support session-scoped deletion are surfaced loudly and
  keep the record-scoped deletion contract.

- **Schedule deliveries to identity-first members carry the driver's stable
  delivery identity.** The internal delivery sink rewrote the receipt
  correlation to the member id, which 0.8.11's `validate_dispatch_receipt`
  rejects — every intercepted member delivery errored AFTER the turn ran
  (false Misfire + `DriverTickFailed` per fire; duplicate turns under
  catch-up windows). The sink now echoes `ScheduleDeliveryIdentity`'s
  occurrence correlation exactly and stages the idempotency key on
  `DispatchInput`. NOTE: admission-boundary dedup for the internal member
  lane is still absent (the staged key has no reader yet) — crash-redelivery
  dedup is a tracked follow-up needing an upstream seam decision.

- **Sender-side conversation view now renders outgoing peer communications
  after a history rebuild.** An outgoing peer send exists in the sender's
  transcript only as an assistant tool call (`send_message` / `send_request` /
  `send_response`) plus a nameless tool result; the session-history projection
  kept assistant text blocks only, so any conversation rebuilt from session
  history (console log lost or reset, live projection never attached for the
  turn) showed the RECIPIENT's arrivals — incoming typed comms notices survive
  history — while silently dropping the SENDER's own outgoing communications.
  The history projection now re-emits the live edge's `tool_call_requested`
  frame ({id, tool_call_id, name, args}) for each comms send call, so the
  console pairs it with the backfilled tool result by `tool_call_id` and
  renders the same outgoing peer item as a live turn; a live twin collapses
  through a new tool-call arm of the history counterpart fingerprint (keyed on
  the provider-minted tool_use_id). Regression coverage runs the two-member
  scenario through both projection sources (live drain and history-only
  rebuild) in `unified_console`.

### Known Issues

- Gateway retire/respawn of idle members can hang intermittently (upstream
  meerkat-mob actor defect family, filed as S4 alongside S2/S3; admin-path
  operation, not exercised by fleet acceptance flows). The pinning test is
  `#[ignore]`d with its run records and re-arms on the upstream fix.

- Zero-turn boots re-stamp `session_tool_visibility_state_v1` from a
  HashSet (per-process order) and touch `updated_at` upstream, so session
  head BYTES are not boot-idempotent on their own - fleet operators
  comparing raw DB digests across boots will see byte drift on sessions
  carrying multi-tool filters. The scoped exact-resave equality (above)
  keeps the durable row untouched on zero-semantic-change boots; filed
  upstream as S5 (durable bytes minted from HashSet iteration order).

## [0.8.8] - 2026-07-29

Observability release. No behaviour changes to storage, resume, or provisioning
— every change here makes an existing path *report itself*. Cut in response to
two overnight field investigations where the platform behaved correctly but said
nothing, and the silence itself cost hours: one 42-minute diagnosis that a single
log line would have made instant, and one deploy abort caused by a supervisor
reading a silent-but-working phase as a hung one.

### Added

- **Head-canonical conversion of a legacy blob logs entry and completion**
  (identity, message count, strand/rewrite-row counts, elapsed). This one-time
  per-session conversion is the slowest phase of a boot on a large legacy
  document — minutes of CPU — and it previously emitted nothing at any level. A
  supervised deploy read that silence as a stalled candidate and aborted a live
  activation. A long migration is now visibly a long migration.

- **Deterministic cross-boot recall guards for the LAZY bootstrap arm**
  (`identity_first_lazy_recall_continuity`). Both existing continuity guards
  covered `EagerMaterialize` only, so `LazyMaterialize` and
  `LazyWithBackgroundWarm` — which register identities Dormant and defer
  materialization to first send — shipped with no proof that a resumed agent can
  see its own history. The new guards assert on the BYTES of the post-restart LLM
  request (never on model wording) and run in memo-free child processes, so
  process-global decode/digest/snapshot memos cannot fake a resume that a real
  `execve` would fail. Two further axes — resume of a pre-0.8.10-encoded document
  and a mid-turn kill — ship `#[ignore]`d with their arming requirements
  recorded: their fixtures fail loudly rather than certify a vacuous pass.

### Changed

- **The `no_incremental_channel` head-canonical read decline warns once per
  adapter**, then logs at debug. On a blob-canonical store the incremental
  channel is absent from construction, so the previous per-read WARN fired on
  every load of every session — 6,504 lines in one observed production boot,
  drowning the signal the level exists for. The genuine fault case (a
  head-canonical deployment losing its delta channel) still surfaces on the
  first line.

### Added
  O(delta) instead of rewriting the whole session document (M4b
  un-deferral).** `LocalContinuityStore` now advertises the session-delta
  channel (`ContinuityStore::as_incremental_sessions`), and head+rows become
  the canonical durable representation of a session in `continuity.sqlite3`:
  a new `continuity_session_heads` / `continuity_strand_messages` /
  `continuity_session_rewrites` trio (ledger `mobkit-continuity` v2), each
  row stamped with its owning `(identity, generation)`. Because
  `PersistentSessionService` routes every boundary save through the
  incremental branch once the capability is `Some`, an ordinary turn on an
  identity gateway now moves the turn's delta plus a few KB of head instead
  of reading, reparsing, re-serializing and rewriting the entire document.
  On the profiled downstream app 82 MB session that removes the continuity half of
  the per-turn write amplification, including the whole-document decode.

  The canonical-representation rule is the same one meerkat-store uses for
  its own head rows, and it is enforced by every verb rather than asserted:
  **a `continuity_session_heads` row means head+rows are that session's sole
  byte authority and its `session_snapshots` row is a frozen archive that is
  never read or written again.** A whole-document save on a head-canonical
  session converts into delta rows plus a small head (plain-append when the
  incoming transcript extends the persisted strand, otherwise a `rebase:`
  strand) and leaves the archive byte-identical; `load_session_snapshot`
  serves the slim materialization; the whole-blob exact-bytes no-op probe
  declines; CAS tokens derive from head+rows; and delete, identity deletion
  and reset rollback scope all four tables (rollback still deletes only the
  attempted generation's rows, so a prior generation's head+rows remain the
  rollback authority). Guard semantics are meerkat's published validators
  verbatim (`validate_save_head_transition`,
  `validate_commit_rewrite_transition`, `strand_layout_for_history`,
  `reconstruct_rewrite_record`), so the accept/reject boundary of the store
  mirror cannot drift from the session service's.

  Every delta mutation carries the continuity write discipline the
  whole-blob path applies — fencing-token compare-and-set and per
  `(identity, generation)` checkpoint-version monotonicity, per append and
  per head write — through a new mobkit-internal
  `ContinuityIncrementalSessions` trait taking an explicit
  `ContinuityWriteCursor`. Rows, the head row and the `continuity_records`
  advance commit in ONE transaction, so a partial append can never leave a
  torn document and the durable cursor can never point past durable data.

  Pre-registration delta writes **park** in process memory instead of being
  refused: member creation provably saves before the identity runtime
  publishes the owning cursor, and the service fails closed on projection
  errors. Parked state is never durable, serves the service's continuity
  preflight and head-CAS reads, and flushes under the real cursor when the
  session registers. Parked rows are dropped only once they have been
  adopted durably: a flush failure — and equally a registration that lands
  between the service's append and the head write that adopts it, leaving
  rows no head claims yet — restores the registry and markers, RETAINS the
  parked rows, and refuses the registration, so the retry replays the write
  instead of losing it. Parking a write and publishing its footprint are one
  operation, so the footprint the "nothing was parked" purge relies on
  cannot drift from what the store holds; abandoning a session (delete,
  unregister, retire) reclaims its parked rows rather than merely unrouting
  them.

  **Upgrade note — the ledger bump is one-way, and deliberately NOT applied
  at open.** Launching this release over an existing state directory leaves
  `mobkit-continuity` at v1; until a write creates head state, older
  binaries can still read the file. The v2 stamp is committed either by an
  incremental session write that actually CREATES HEAD STATE — the
  head-canonical DDL and the stamp ride inside that write's own transaction,
  so a write refused by a guard or by its own CAS rolls both back and leaves
  the file at v1 with no head rows — or by
  `mobkit_gateway storage-migrate --apply` under the exclusive maintenance
  fence; `--dry-run` announces the pending bump. An ACCEPTED
  write that creates no head state does not arm it either: an
  `append_messages` whose adopting head write has not landed yet commits its
  rows and its additive DDL and leaves the ledger at v1, because rows no
  head adopts are not part of any document and an older binary still
  correctly serves that session from its blob. Once a head row exists,
  binaries older than this release refuse the file at open with
  `SchemaFromTheFuture`, and there is no in-place path back: recovery from
  a bad upgrade is restoring the state directory — `continuity.*` and the
  rest — from a consistent backup taken before the upgrade, accepting the
  loss of the turns taken since. That backup must be SQLite-consistent and
  capture committed WAL state (a stopped-gateway copy of each database with
  its `-wal` sibling, or SQLite's own backup API) — never a bare copy of
  the main database file alone, and never a read through `immutable=1`.
  Take it before upgrading. The doctor reports frozen archives under the new
  `continuity-archived-snapshot` finding and censuses head-canonical
  sessions with the same stamp verification the blob path pays. Frozen
  archives are reclaimable dead weight until an archive-prune verb ships;
  none does yet, so a migrated realm's `continuity.sqlite3` transiently
  carries both representations for each migrated session.

- **Durability declaration completeness is enforced at the storage seam.**
  `enforce_fail_closed_store_set` now requires exactly one
  `DurabilityDeclaration` for every slot in the new
  `REQUIRED_MOBKIT_DURABILITY_DOMAINS` (`continuity`, `event_log`, `console`,
  `metadata`, `blobs`, `agent_memory`, `schedule`); a provider that omits or
  duplicates a domain is refused at composition instead of silently passing
  the fail-closed durability rule. Mirrors the meerkat-level
  `REQUIRED_DURABILITY_DOMAINS` hardening.

- **`mobkit_gateway storage-migrate` / `storage-prune` — MobKit migration
  participation (storage-unification Phase M6).** New library module
  `meerkat_mobkit::storage_migrate` plus two offline maintenance verbs on
  the gateway binary (argv verbs beside `storage-adopt-checkpoints`; no RPC
  method — migration is never an eager side effect of gateway startup).
  `storage-migrate --state-dir <dir> [--apply] [--adopt <path>] [--json]`
  runs the five migration cases under `MobKitMaintenanceFence` — one
  `meerkat_sqlite::ExclusiveFence` per materialized database (meerkat's
  Phase 6 primitive; MobKit builds no second fence), sorted acquisition
  order, all-or-nothing RAII:
  1. **Ledger baseline (auto-safe):** every existing MobKit-owned database
     opens through its normal M3 constructor so `meerkat_schema` domains
     stamp (continuity, metadata, console, per-realm agent-memory files);
     dry-run is a read-only version matrix. The workgraph admission sidecar
     is exempt by design (no ledger — M3 decision, noted in the report);
     meerkat-shared databases (sessions/runtime/schedule/workgraph) are
     report-only and converge through the owning meerkat store's next open.
  2. **File-name unification (auto-safe, rename-only):** a lone legacy
     spelling (`sessions.db`, `sessions.sqlite`, `continuity.db`,
     `identity_continuity.sqlite`, `mobkit_metadata.sqlite`,
     `mobkit_console.sqlite`, `agent-memory-sqlite/`) renames to its M2
     canonical name **only here, under the fence** (the resolver never
     renames at open), moving `-wal`/`-shm` siblings with the database — a
     non-empty WAL is checkpointed first and the move refuses if it cannot
     be. Every rename writes a registered marker
     (`<legacy>.pre-<version>-<ts>.renamed`) so doctor lists it and
     retention tooling recognizes the transition.
  3. **Twin reconciliation (manual, fail-closed):** both spellings
     populated → per-domain divergence report (row-level by primary key +
     content digest for continuity/metadata/console; file-digest for the
     rest) and a typed refusal that fails the whole run closed.
     Byte-identical twins dedup under plain `--apply`; divergent twins
     resolve only with `--apply --adopt <path>` — the adopted copy keeps
     its place (renamed to canonical if legacy-named), the rest archived
     read-only. **No synthesis**: continuity fencing tokens and console
     `AUTOINCREMENT` cursors are per-database sequences.
  4. **Continuity checkpoint adoption:** H3's snapshot-adoption walk runs
     under the same fence pass (new
     `adopt_continuity_snapshots_already_fenced` composition seam) and its
     report merges into the migrate report.
  5. **Leftovers (report-only):** legacy sharded-FS blob files, the
     admission sidecar, `*.pre-*`/`*.corrupt-*` artifacts, and dead
     `tux-runtimes.json` entries (recorded pid no longer alive).
  `storage-prune --state-dir <dir> [--apply] [--older-than-days N]
  [--json]` owns the registered-artifact lifecycle (`*.pre-*` backups,
  `*.corrupt-*` quarantines; default threshold 30 days; deletion refuses
  anything outside the registered naming). Backup naming is shared with
  meerkat verbatim (`<original>.pre-<version>-<timestamp>[.<purpose>]`,
  reusing `meerkat_store::migrate`'s helpers), so downstream-app-class
  generation-cloning recognizes MobKit artifacts the same way — but
  recognition validates the COMPLETE generated shape
  (`is_registered_backup_artifact_name` /
  `is_registered_quarantine_artifact_name`: dotted numeric version,
  all-digit timestamp, exact `.corrupt-<digits>` suffix), never a loose
  `.pre-` substring, so prune and doctor can never claim user files like
  `notes.pre-release`. Exit codes:
  0 clean, 1 refusals/fence/store failures, 2 usage errors; dry-run
  default. `MobKitStorageMigrator` gains concrete `migrate`/`prune`
  methods (the meerkat-owned `StorageMigrator` trait stays diagnose-only).
- **`MobKitStorageProvider` — the composite storage-provider seam
  (storage-unification Phase M4b, the "one remote bundle").** New module
  `meerkat_mobkit::storage_provider`: a downstream backend (BigQuery,
  Postgres, object stores) implements ONE provider that wraps or references
  a meerkat `RealmStorageProvider` (`meerkat_provider()`, the
  meerkat-shared stores) and opens the realm-wide MobKit store set
  (`open_realm(&MobKitRealmOpenContext) -> MobKitRealmStoreSet`:
  continuity, lease authority — a `LeaseProvider` or the fencing floor to
  seed one — event log, console timeline, metadata, binary blobs, agent
  memory, schedule), each slot paired with a machine-readable
  `meerkat_core::DurabilityDeclaration`; `migrator()` exposes the storage
  maintenance hook. Composition is fail-closed
  (`enforce_fail_closed_store_set`): a `Durable` slot resolving
  non-persistent without an explicit ephemeral declaration is a typed
  startup error, never a silent in-memory fallback. The built-in
  `DiskMobKitStorageProvider` reproduces today's SQLite/object-store layout
  through the M2 layout locators and M3 ledgered openers, and returns
  `MobKitStorageMigrator` as its maintenance hook.
  `UnifiedRuntimeBuilder::storage_provider(...)` installs a provider for
  identity-first builds (requires `roster_provider()`; the provider's
  continuity store is the session authority; realm root from
  `persistent_state()` or `scratch_dir()`); supplying the provider together
  with a per-slot seam it subsumes is a typed conflict. The acceptance
  proof lives in `mobkit-store-conformance`
  (`tests/one_remote_bundle.rs`): an in-crate, in-memory-declared reference
  provider (`ReferenceMemoryBundleProvider`) passes the full MobKit
  conformance suite through the single seam and the meerkat conformance
  profiles (baseline, append-only, incremental, blobs, artifacts) through
  its `meerkat_provider()`.
- **New `UnifiedRuntimeBuilder` seams:** `schedule_store(Arc<dyn
  ScheduleStore>)` (the public schedule-store seam the operator deployment lacked — schedule
  tools attach over the caller's store on both the persistent and the
  ephemeral/scratch paths; library mode wires no firing host, and injection
  is a foundation: shadow-scheduler deletion downstream still requires a
  feature-parity audit), `binary_blob_store(Arc<dyn BinaryBlobStore>)`
  (typed raw-bytes blob injection that skips the base64 adapter round-trip
  on the byte-serving paths; mutually exclusive with `blob_store()`), and
  `ephemeral_runtime_store(bool)` (the explicit in-memory runtime-store
  declaration; see the fail-closed change below).
  `schedule_wiring::attach_schedule_tools_with_store` and the
  `*_reporting` variants of the schedule/workgraph attach functions are
  public for gateway-grade composition.
- **Per-slot storage census on the health surfaces.**
  `ResolvedStorageSummary` grows a `slots` array (additive wire change on
  the `"storage"` object of `mobkit/status` / `mobkit/capabilities` /
  `mobkit/storage/doctor`): every composed slot reports its domain,
  durability class, resolution (`persistent` / `declared_ephemeral` /
  `non_persistent`), backend, degradation flag, and detail. Declared
  defaults are now visible instead of silent (the ephemeral gateway's
  in-memory console/metadata, `mobkit_gateway`'s in-memory console/metadata
  contract, an unconfigured event log = "events are not ingested"), the
  schedule/workgraph boot-without posture is a health-visible degraded slot
  entry instead of a warn line alone, and the three in-process ring buffers
  (gating audit 512 / delivery history 200 / routing resolutions 512) are
  classified `Scratch` explicitly — a documented decision; a durable
  gating-audit slot remains a flagged follow-up, not part of this arc.
- **Incremental-continuity capability seam (typed, discoverable —
  deliberately deferred for the bundled store).** `ContinuityStore` gains
  `as_incremental_sessions() -> Option<Arc<dyn IncrementalSessionStore>>`
  (default `None`), and `ContinuitySessionStoreAdapter::as_incremental`
  genuinely forwards it: `Some` exactly when the substrate advertises the
  session-delta channel, wrapped so every delta mutation preserves the
  adapter's registration/suspension/supersede discipline under the same
  per-session lock as the whole-blob paths (H3 lazy adoption included).
  The bundled `LocalContinuityStore` deliberately returns `None`: shipping
  a delta channel beside today's whole-snapshot byte authority (exact-match
  probes, parse-derived CAS revision tokens, `checkpoint_session`, H3's
  adoption byte custody) would create two write authorities over one
  session in one store with no reconciliation rule. The store-side channel
  lands when the snapshot representation itself moves to head+rows, gated
  by the meerkat-store-conformance incremental profile; the H2 whole-blob
  health flag and the M0 canary stay pinned `false` accordingly.
  `GatewayContinuityStore` cannot advertise the capability (its callback
  wire protocol has only whole-snapshot verbs).
- `runtime_options` (rpc_gateway) wire additions: `event_log.storage`
  accepts `"null"` (explicitly declared dropped events; the existing
  `"memory"` form keeps working), and the new `runtime_store` key accepts
  `{"storage": "memory"}` — the explicit ephemeral runtime-store
  declaration. Unknown keys still reject.
- **SDK storage durability vocabulary (storage-unification Phase M5).**
  Python gains the `config.runtime_store` (`runtime_store.memory()`) and
  `config.event_log` (`event_log.memory(batch_size, flush_interval_ms)` /
  `event_log.null()`) modules plus the builder methods
  `.runtime_store(config)` and the extended `.event_log(config)` (the
  legacy `event_log(storage=..., **kwargs)` keyword form keeps working);
  TypeScript mirrors them as the `runtimeStore` / `eventLog` config
  modules (`eventLog.nullStore()` — `null` is reserved) and
  `.runtimeStore(config)` / the extended `.eventLog(...)`. Both SDKs now
  parse the storage census: `StatusResult.storage` /
  `CapabilitiesResult.storage` carry a typed `StorageSummary` (H1 blob
  durability, the H2 incremental probe, and the M4 per-slot
  `StorageSlotSummary` census), with forward-tolerant parsing;
  TypeScript additionally exports `parseStorageSummary` for the raw
  `mobkit/storage/doctor` `storage` record. Blob-store durability
  remains embedder-only (no wire declaration; census-visible read-only).
- **Typed fail-closed storage refusals: JSON-RPC error `-32014`
  (`STORAGE_RESOLUTION_CODE`).** The `rpc_gateway` init storage refusals
  — file-name twins the layout refuses to pick between, a
  session/runtime/blob/metadata/console store that failed to open where
  the silent in-memory fallback used to be, an uncreatable state root —
  now answer `mobkit/init` with `-32014` instead of generic `-32603`,
  and both SDKs reify it as the new `StorageResolutionError`
  (`RpcError` subclass). Both SDK bootstraps also stop rewrapping a
  received structured init error as a transport failure when the gateway
  exits after writing it (the fail-closed refusals do exactly that).
  Refusals raised inside `UnifiedRuntime::bootstrap` (Rust builder
  surfaces) still surface as `-32603` with the typed message text.
- **M5 storage anti-regression gate** (`meerkat-mobkit/tests/storage_gate.rs`,
  default test lane): production code may not resolve ambient roots
  (`$HOME`/`$XDG_*`/`$LOCALAPPDATA`/`$TMPDIR` reads, `env::temp_dir`,
  `dirs::`-style derivations) or spell the canonical database file names
  outside `src/storage_layout.rs`; legitimate uses are allowlisted with
  documented reasons (mobpack's skill/MCP-config home discovery and
  scratch outputs, the feature-owned schedule/workgraph constants, the
  doctor's legacy-spelling census). The sweep it enforces moved the
  mobpack flow-editor draft-store default off its private
  `$XDG_STATE_HOME`/`$HOME` derivation onto
  `storage_layout::default_gateway_home()` (same resulting path).

### Fixed

- **Idle busy-loop, part 2: an idle gateway now costs ~zero CPU.** On
  0.8.2–0.8.4 a fully idle durable member burned ~0.3 core indefinitely
  (~1.1 cores for the two-identity production dump; ~5 cores extrapolated
  for a 17-member host). The 0.8.4 schema-caching fix removed the
  amplifier; this release fixes the drivers, in tandem with meerkat 0.8.6:
  - **Event-driven stream reconcile.** The console forwarder and identity
    health monitor replaced their 250 ms polling ticks (each paying a full
    fleet-state projection per pass, per handle) with `ReconcileCadence`:
    wakes on tracked mobs' machine-state watches, the managed-mob-set
    epoch, the earliest subscribe-backoff deadline, and a 30 s safety tick
    anchored at the last completed reconcile (so sustained event traffic
    cannot starve it). Stream closures still reconcile immediately —
    repair latency is unregressed; only watch-invisible signals (lease
    fencing motion without a machine transition) moved from 250 ms to
    ≤30 s detection.
  - **Steady-state console discovery is I/O-free.** The 5 s session-history
    discovery loop re-read the FULL session document every pass (two
    whole-document sqlite reads + deserialize + rewrite-replay canonical
    digest + a watermark refresh write, per member — the dominant burn on
    82 MB production documents). Session-scoped write epochs
    (`SessionSnapshotWriteEpochs`, bumped on both sides of every runtime
    store write) now gate the backfill: an unchanged session costs one
    in-memory epoch compare. Witnesses are dropped on both unregister and
    register-replacement so a restarted runtime's counter can never
    suppress its first backfill.
  - **The epoch gate actually engages in the gateway binaries.** Both
    gateways compose their own stores and session services through
    `MobBootstrapSpec::new`, which leaves the write-epoch witness absent —
    so the gate above silently stayed disabled in every production
    deployment while the library-composed test stayed green. New public
    seam for externally-composed runtimes:
    `epoch_tracking_runtime_store()` + `SessionWriteEpochsHandle` +
    `MobBootstrapSpec::with_session_write_epochs()`; both binaries now
    wrap their runtime store and thread the witness on all launch paths,
    and the idle gate composes at gateway parity so composition-path
    divergence in this class cannot pass the suite again. Real-dump A/B:
    ~0.97 cores idle pre-fix → ~0.03 post-fix.
  - **Restore elision.** `restore_flow` returns an empty snapshot for
    already-active identities instead of re-running adoption work.
  - **Idle-CPU regression gate** (`tests/idle_cpu_gate.rs`) strengthened to
    the class that escaped every suite: one member's session grows to
    ~12 MB, the console aggregator runs exactly as the gateways run it,
    then a bounded quiesce probe precedes a 30 s window asserting ≤3 s
    process CPU (verified red on the pre-fix code: 11.6 s).
- **BREAKING for external `ContinuityStore` implementors relying on the
  old default: `rollback_continuity_record`'s compatibility implementation
  now actually restores.** The previous default restored the pre-reset
  record via `upsert_continuity_record`, which the trait requires to be
  generation-monotonic — so on every conforming store the restore path
  failed with `StaleContinuityGeneration` and only the delete arm
  compensated. The default now validates the attempt row, deletes it under
  the fence CAS, and re-upserts the previous record as a fresh insert —
  semantics a conforming store CAN satisfy. Documented caveats (why stores
  should still override with one atomic CAS transaction, as
  `LocalContinuityStore` does): the path is non-atomic, and
  `delete_continuity_record` removes the identity's session snapshots —
  including the previous generation's rollback-authority snapshots, which
  the atomic override retains. The
  `mobkit-store-conformance` `RollbackPath::CompatibilityDefault` chapter
  pins the fixed behavior (working restore + the snapshot-loss caveat);
  external stores that rely on the default and depended on the old
  always-fail behavior must re-run the suite.

### Changed

- **BREAKING / fail-closed (M4): the runtime store no longer silently falls
  back to in-memory.** All three composition surfaces
  (`MobBootstrapSpec::persistent*` and the builder's `persistent_state()`
  path, `rpc_gateway`, `mobkit_gateway`) previously answered a failed
  `runtime.sqlite` open with a `tracing::warn!` plus a silent
  `InMemoryRuntimeStore` — a degraded mode in which resume across restart
  and archive operations fail long after boot (the fifth silent fallback of
  the storage-unification recon). A failed open is now a startup error
  whose message names the remediation (fix the database file, or declare
  the ephemeral choice explicitly via
  `UnifiedRuntimeBuilder::ephemeral_runtime_store(true)` /
  `runtime_options.runtime_store = {"storage": "memory"}`).
  `InMemoryRuntimeStore` remains constructible by declaration only.
  Signature note: `MobBootstrapSpec::persistent` /
  `persistent_with_hook` now return
  `Result<Self, StorageResolutionError>` (a composite of the H1 blob error
  and the new runtime-store error) instead of
  `Result<Self, BlobStoreResolutionError>`.
- **REQ-23 lifted (M4): `persistent_state()` and an external
  continuity/lease pair may coexist.** The external substrate (or a
  `storage_provider()`) stays the identity and session authority —
  `ContinuitySessionStoreAdapter` remains the session store — while the
  state directory supplies the meerkat-shared local stores (runtime,
  workgraph, blobs, topology control). The genuinely contradictory
  combinations keep typed errors: `persistent_state()` + `scratch_dir()`
  (two path roots for one realm), and half an external substrate
  (`continuity_store()` without `lease_provider()` or vice versa alongside
  `persistent_state()` — fencing floors are store-coupled).

- **BREAKING (pre-1.0): the judgment plane is de-welded onto capability
  traits (storage-unification Phase M4).** The taint firewall controls, the
  Steward's dream read/write surface, and the console Memory panel's read
  API — previously inherent methods on the concrete
  `SqliteAgentMemoryStore` — are now capability traits in
  `meerkat_mobkit::memory::capabilities`: `TaintableStore` (the five
  firewall setters `set_llm_write_gate`, `set_llm_write_gate_if_absent`,
  `set_evidence_resolver`, `set_event_sink`, `set_event_sink_if_absent`),
  `StewardStore: StagedMemoryStore + TombstoneSource` (the nineteen steward
  read/write methods, including the dream ledger writers), and
  `MemoryPanelStore: StewardStore` (the eight panel-only reads). The full
  plane — firewall, Steward, Hygienist span source, Selector fetch, console
  panel — now runs against ANY `AgentMemoryProvider` advertising the
  matching capability accessors (`as_taintable`, `as_steward_store`,
  `as_memory_panel_store`, `as_selected_record_fetch`,
  `as_tombstone_source`; all default `None`). Breaking API notes for
  embedders under the pre-1.0 policy:
  - `AgentMemoryProvider::as_sqlite_store()` is **deleted** (the trait no
    longer names its own implementation); probe the capability accessors
    instead.
  - The promoted store methods moved from inherent impls to the trait
    impls: call sites on the concrete `SqliteAgentMemoryStore` now need the
    corresponding trait in scope (`TaintableStore`, `StewardStore`,
    `MemoryPanelStore` — all re-exported from `meerkat_mobkit`).
  - `memory_wiring::attach_memory_engines` takes
    `Arc<dyn AgentMemoryProvider>` instead of the concrete store, and
    `AgentMemoryStack` carries `steward_store: Option<Arc<dyn StewardStore>>`
    and `panel: Option<Arc<dyn MemoryPanelStore>>` instead of the concrete
    `store` field. Missing capabilities are named errors, never silent
    downgrades.
  - `UnifiedRuntime::{set_memory_panel_store, memory_panel_store}` and the
    console router plumbing carry `Arc<dyn MemoryPanelStore>` instead of
    the concrete store by value.
  - `StewardEngine::new` and `StoreSpanReferenceSource::new` take
    `Arc<dyn StewardStore>`; `EvidenceRefResolver` moved to
    `memory::capabilities` (re-exported from the old `sqlite_store` path,
    together with the promoted row types).
  Behavior notes: the gateway's Selector wiring now fails with "requires a
  provider with selected-record fetch support (SelectedRecordFetch)"
  instead of "requires the sqlite agent-memory store", and reuses the
  configured provider's fetch handle instead of opening a second concrete
  store on the same files; the Markdown provider stays recall-only *by its
  capability flags* (it advertises none), and explicitly configuring
  distiller/steward/hygienist against a recall-only provider now fails
  init loudly instead of silently constructing nothing.

- **BREAKING / fail-closed (H1): persistent-mode blob storage no longer
  silently falls back to in-memory blobs.** Previously, when a
  persistent-mode runtime (`MobBootstrapSpec::persistent*`,
  `UnifiedRuntimeBuilder::persistent_state()`) failed to open the local blob
  directory (`<state>/blobs`), it logged one warning and continued on an
  in-memory blob store — every blob written after that point silently
  vanished on restart (the cause of a month-long silently-broken production
  deployment). **Deployments that are unknowingly running on this fallback
  today will now fail to boot.** That is deliberate: the failure was always
  there, it is now visible at startup instead of at data-loss time.
  Remediation: fix the blob directory (permissions, mount, read-only
  filesystem), or — if in-memory blobs are genuinely intended (tests,
  demos) — declare the choice explicitly with the new
  `UnifiedRuntimeBuilder::ephemeral_blobs(true)`.
  `MobBootstrapSpec::persistent` / `persistent_with_hook` now return
  `Result<_, BlobStoreResolutionError>`. Additionally, persistent mode now
  asserts `is_persistent()` on the *resolved* blob store at composition
  time, so a custom-injected non-persistent store also requires the
  `ephemeral_blobs(true)` declaration. Ephemeral launch modes (scratch or
  temp-dir) are unchanged — their in-memory blobs are the declared choice of
  the mode itself.

### Added

- **Continuity snapshot checkpoint adoption (storage-unification H3).**
  Session bytes written by 0.7.x-era MobKit live inside continuity
  `session_snapshots` rows; on 0.8.x they decode as legacy-unverified and
  hard-fail every resume through the identity-first path (the bridge
  correctly refuses fresh-spawn, so the identity stays `Broken` on every
  reconcile retry). Meerkat ≥0.8.3 auto-migrates the documents *it* reads
  (session store, runtime snapshots); continuity snapshots are the copies
  meerkat never sees, and this release heals them via the exported
  `meerkat_core::adopt_legacy_session` helper with the **observed cursor**
  (generation / checkpoint version) from the matching continuity record.
  Two sanctioned shapes:
  - **Batch, in a deploy/maintenance window**:
    `meerkat_mobkit::identity_first::adopt_continuity_snapshots` (library
    entry) and the new `mobkit_gateway storage-adopt-checkpoints
    (--db <path> | --state-dir <dir>) [--apply] [--json]` maintenance
    subcommand (never an eager side effect of gateway startup; dry-run by
    default and byte-identical without `--apply`; exit 1 on refusals or an
    unacquirable fence). The walk holds the exclusive maintenance fence on
    the database file, enumerates rows by direct SQL (the `ContinuityStore`
    trait has no enumeration API and cannot rewrite in place at the same
    `CheckpointVersion`), rewrites each legacy row's bytes in place, and
    deliberately leaves the row's `generation` / `checkpoint_version` /
    `fencing_token` columns untouched — the stamp binds to the observed
    cursor the row already records. Stale rows (record rebound away or
    generation superseded) are classified and reported, never adopted and
    never an error; a re-run is a byte-identical no-op.
  - **Lazy, at restore** (always-on single-replica deployments, where the
    pod restart is the window): `ContinuitySessionStoreAdapter` adopts a
    legacy snapshot at first load under the registered continuity cursor
    and persists the adopted bytes through the store's own CAS at the next
    checkpoint version. Named opt-in
    (`with_lazy_checkpoint_adoption(true)`), enabled on the identity-first
    gateway wirings (`rpc_gateway`, the builder's external-authoritative
    arm) — it is the fleet-unbrick behavior. Downstream lazy shims that
    stamped continuity copies by hand can retire on this release (retirement
    chain: meerkat #909 → this H3 → shim removal).

  **Ordering constraint:** meerkat's own lazy path seeds `INITIAL` cursors
  and a verified document never re-migrates, so on any fleet whose
  continuity rows record a **nonzero generation floor**, run H3 (batch verb,
  or restore through the lazy-enabled adapter) **before** meerkat's lazy
  path first touches those sessions. Generation-0 fleets are unaffected.
- **`MobKitStorageLayout` path authority + canonical-name-first probing
  (storage-unification Phase M2).** One module,
  `meerkat_mobkit::storage_layout`, now owns every storage root and
  canonical top-level database locator; `UnifiedRuntimeBuilder`,
  `mobkit_gateway`, and `rpc_gateway` all consume the layout instead of
  deriving file names inline (the three-way derivation duplication is
  deleted). Canonical spellings, decided once: stores shared with Meerkat
  keep Meerkat's names — sessions `sessions.sqlite3`, runtime
  `runtime.sqlite`, schedule `schedule.sqlite`, workgraph
  `workgraph.sqlite3` — and MobKit-owned files converge on `*.sqlite3`:
  continuity `continuity.sqlite3` (was `continuity.db` on the gateways,
  `identity_continuity.sqlite` in the builder), metadata
  `mobkit_metadata.sqlite3`, console `mobkit_console.sqlite3`, agent-memory
  root `agent-memory/` (was `agent-memory-sqlite/` for the builder's SQLite
  stack), blob root `blobs/`, and gateway-home files `peer_key.ed25519` /
  `tux-runtimes.json`.
  **Existing deployments keep working unchanged:** every locator resolves
  canonical-name-first and then probes the known legacy spellings in the
  same directory — exactly one spelling present means it is used *where it
  lies* (no rename at open; physical renames to canonical names arrive only
  with the Phase M6 migration verb, under the maintenance fence). Only
  *fresh* directories get the canonical names. When both the canonical and
  a legacy spelling (or two legacy spellings) of the same store exist, boot
  refuses with the typed `StorageLayoutError::FileNameTwins` pointing at
  the storage doctor — previously the surfaces would silently open one of
  the two and fork history. A gateway `store_path` with a file extension
  remains an explicit session-database override (now an explicit layout
  input instead of call-site extension sniffing). This also heals the
  cross-surface agent-memory hazard: `rpc_gateway` now finds and reuses a
  builder-created `agent-memory-sqlite/` corpus instead of silently
  starting an empty `agent-memory/` beside it.
- **Declared-ephemeral scratch layout for `rpc_gateway`.** With no
  `persistent_state`, the locally hosted identity substrate no longer lands
  on a silent pid-suffixed `$TMPDIR/mobkit-continuity-<pid>.db` at the call
  site; the gateway constructs the layout in explicit scratch mode rooted
  at `$TMPDIR/mobkit-scratch-<pid>/` and the choice is recorded in the
  serializable `layout_summary()` (`durability: declared_ephemeral`), which
  the Phase M1 storage doctor consumes.

- **Storage doctor (M1): `mobkit/storage/doctor` RPC + console read
  method.** A read-only diagnosis of a MobKit state directory, safe against
  a live gateway (read-only SQLite opens, no file creation, no leases, no
  schema-ledger runs), exposed on the module-only and unified stdin RPC
  surfaces and on `POST /console/rpc` (read method, `runtime.admin` grant;
  the console `mobkit/status` payload now advertises
  `storage.doctor_available: true`). Reports: the per-directory database
  inventory across every historical filename spelling, with schema-ledger
  domain versions (`no-schema-ledger` on pre-M3 files,
  `empty-database-shell` on table-less files); **file-name twins** as
  errors (`file-name-twins`) for `sessions.db`/`sessions.sqlite`/
  `sessions.sqlite3`, `continuity.db`/`identity_continuity.sqlite`/
  `continuity.sqlite3`, `mobkit_metadata.sqlite`/`.sqlite3`,
  `mobkit_console.sqlite`/`.sqlite3`, and `agent-memory/`/
  `agent-memory-sqlite/`; the continuity checkpoint-evidence census per
  identity (`legacy-unverified-continuity-snapshots`,
  `checkpoint-metadata-invalid`, `continuity-snapshot-undecodable`) that
  H3's adoption dry-run consumes; dangling console-frame blob references
  (`dangling-console-blob-reference`); blob-root inventory (`blob-root`,
  `legacy-fs-blobs`); gateway-home artifacts when scoped
  (`peer-key-file`, `runtime-registry`); the workgraph admission sidecar
  (`workgraph-admission-sidecar`); filesystem artifacts
  (`maintenance-fence-lock`, `backup-artifact`, `quarantine-artifact`); and
  the live H1/H2 durability resolution (`blob-durability`,
  `session-store-incremental`) when invoked through a live gateway, or
  `durability-census-unavailable` on a cold directory. `params.state_dir`
  is required until the Phase M2 layout authority lets the runtime report
  its own state directory — runtime-backed surfaces answer a missing
  `state_dir` with error `-32004`. New public seam
  `meerkat_mobkit::storage_doctor` (`diagnose_state_dir`,
  `MobKitStorageMigrator` implementing
  `meerkat_core::storage_diagnostics::StorageMigrator`) plus
  `storage_doctor(...)` / `storageDoctor(...)` wrappers in the Python and
  TypeScript SDKs. Additive method: `MOBKIT_CONTRACT_VERSION` stays at
  `0.4.0`.
- **Storage durability health surface (H1/H2).** `mobkit/status` (unified
  and console shapes) and the unified `mobkit/capabilities` now carry a
  `storage` object: `blob_durability`
  (`persistent_disk` | `declared_ephemeral` | `custom`),
  `blob_store_persistent` (bool), and `session_store_incremental`
  (bool, or null when no session persistence exists). New public vocabulary
  in `meerkat_mobkit::storage_health` (`ResolvedStorageSummary`,
  `BlobDurability`, `BlobStoreResolutionError`,
  `probe_session_store_incremental`), carried on
  `MobBootstrapSpec::resolved_storage` and readable via
  `UnifiedRuntime::resolved_storage()`.
- **Loud whole-blob degradation (H2).** At every composition site that
  builds a `PersistentSessionService`, MobKit now probes the session store's
  incremental-persistence capability (`as_incremental`) and logs a startup
  warning when it is absent, naming the store kind and the consequence
  (session persistence degrades to whole-blob saves on every turn). This
  makes the identity-first gateway shape — where `ContinuitySessionStoreAdapter`
  is the session authority and persists O(session) per turn — visible at
  startup and on the health surfaces (`session_store_incremental: false`)
  instead of silent. The structural fix (a genuine incremental channel on
  the continuity contract) is tracked as storage-unification Phase M4; this
  release makes the degradation honest, not fixed.

## [0.8.2] - 2026-07-22

### Changed

- Pinned the complete Meerkat dependency family to the public `0.8.3` crate
  set, including identity-first flow target provisioning and runtime reliability
  fixes.
- Memory-tool installation now follows the resolved profile's explicit memory
  policy, including realm-referenced profiles, instead of relying only on the
  provider-wide authored-write capability.

### Fixed

- Identity-first `run_flow` now materializes dormant targets through the shared
  Mob handle provisioner before dispatch, so admitted flows reliably become
  member turns.
- Permanent member event-stream loss now repairs the trusted durable identity,
  fences stale generations, and is detected independently of bounded console
  output consumption.
- Quarantined or torn-down identity sessions transition to `Broken` and enter
  the normal repair loop instead of remaining permanently `Active` zombies.

- All six in-crate SQLite openers (agent memory, continuity writer + read
  pool, runtime metadata, console aggregator, workgraph admission sidecar,
  schedule triage) now flow through the shared `meerkat-sqlite` mechanics
  crate under named connection profiles; MobKit no longer hand-rolls any
  PRAGMA setup (storage-unification Phase M3).
- **Console aggregator store gains WAL journaling, a busy timeout, and
  `synchronous=FULL` for the first time.** It previously set zero PRAGMAs
  (rollback journal, no busy handler). This is the highest-write-rate MobKit
  database: writes now fsync on commit and participate in WAL semantics, so
  concurrency-heavy deployments should re-run console-load benchmarks.
- **The continuity store's writer now runs with `synchronous=FULL`** (it
  previously set only WAL + busy timeout). Continuity writes sit on the
  identity-first hot path — every checkpoint/fencing CAS now fsyncs on
  commit. This is a deliberate durability upgrade; latency-sensitive
  deployments should note the fsync-rate change. The agent-memory and
  runtime-metadata stores gain the same explicit `synchronous=FULL`.
- Busy timeouts harmonized on the shared 60s production default (previously
  5s for memory/continuity/metadata, none for the console store). The
  admission sidecar keeps its deliberate 30s wait-then-fail-closed policy as
  a named per-open override.
- Every MobKit-owned SQLite database now carries a per-file migration ledger
  (`meerkat_schema` table, domains `mobkit-memory`, `mobkit-continuity`,
  `mobkit-metadata`, `mobkit-console`) and a `<file>.mfence` maintenance
  lock file appears next to each database. Files written by a newer MobKit
  are refused with a typed schema-from-the-future error instead of being
  mutated. The workgraph admission sidecar is deliberately exempt from the
  ledger (stamping it would take the very lock it arbitrates). Legacy files
  (including pre-`ever_quarantined`/`taint` agent-memory stores) converge on
  first open; the memory store's historical column probes are now versioned
  migrations.
- `ContinuityStoreError` gains a `Transient` variant: busy/locked SQLite
  failures are now classified transient (and corruption classified corrupt)
  at the store boundary, so identity reconcile can distinguish retry-worthy
  failures from poison without string-sniffing. Classification does not
  itself authorize retry; retry policy stays with callers.

## [0.8.1] - 2026-07-22

### Changed

- Pinned the complete Meerkat dependency family to the public `0.8.2` crate
  set. MobKit composes Meerkat's concrete `retire` authority for explicit
  member deletion but does not duplicate the still-unsupported generated
  `EnsureSessionAuthority` and `ReleaseSessionAuthority` actuators. Those
  generic recovery observations remain explicitly repair-blocked upstream.

### Fixed

- Snapshot coherence now treats a snapshot's fence as historical write
  provenance while requiring the presented fence to equal current write
  authority. Cross-identity and cross-generation snapshot replacement is
  rejected, and embedded session IDs must match their durable key.
- The continuity session adapter remains a strict projection store: it no
  longer runs a second transcript-recovery classifier or fabricates an empty
  session when durable bytes are missing. Authoritative projection CAS checks
  only the visible durable row.
- Identity registration now rejects conflicting owners, generations, and
  regressing fencing epochs, and ambiguous lost acknowledgements never rewind
  checkpoint allocation.

## [0.8.0] - 2026-07-18

### Added

- Exposed identity-first startup materialization through the SDK gateway and
  Python builder. Deployments can keep eager compatibility, register lazily,
  or return after metadata registration and warm the roster in a tracked,
  bounded background task. Typed status and wait RPCs report per-identity
  `dormant`, `warming`, `active`, and `broken` progress and support an exact
  startup-readiness barrier.
- `UnifiedRuntime::start_member_turn` now exposes completion-bearing member
  turns through `MemberTurnAdmission`. Callers can observe the exact bridge
  session, await the executor-applied model/provider/self-hosted route, stream
  live events, and separately await committed runtime completion without
  treating ingress admission as execution success.
- The shared console docking contract now includes a first-class browser host
  target, with collision-free grid placement and host-prop forwarding alongside
  normal topology panels.

### Changed

- The full Meerkat dependency family is now pinned exactly to the released
  `0.8.1` crate set, including memory, live, and scheduling surfaces.
- Explicit identity bootstrap modes now consistently declare identity-first
  intent and require a roster provider, including explicit eager mode; an
  omitted mode still preserves the classic gateway when no roster is present.
- Identity bootstrap and reconcile now share one mode-aware controller;
  background work is cancelled during shutdown and the concrete Mob bridge
  skips unused checkpoint payload reads without weakening custom bridge
  contracts.
- Identity continuity persistence now serializes mutations per session,
  short-circuits provenance-identical snapshot saves, and runs bundled SQLite
  work on blocking workers with one writer and a bounded WAL read pool.
- Gateway and console identity operations are now runtime-owned through their
  commit or rollback boundary. EOF, Ctrl-C, callback closure, HTTP draining,
  and the Python/TypeScript host transports preserve that cleanup ordering
  before escalating to bounded process termination.
- Persistent SDK shutdown now negotiates the stock gateway's explicit
  335-second bounded horizon and positively attests every cleanup boundary,
  including mob quiescence, exact authority release, event draining, and child
  process termination. Provider operations retain their public 120-second
  contract, with a 125-second SDK completion deadline and a 130-second gateway
  wire deadline; Python cancels the event-loop future and TypeScript exposes an
  abort signal while suppressing late responses. Older/custom gateways retain
  EOF compatibility.
- Destructive identity reset now retains exact old-generation cleanup debt
  after the replacement continuity head commits. MobKit captures memory first,
  quiesces the superseded member, CAS-deletes only its stale session snapshot,
  verifies structural roster absence, and retries the debt during shutdown
  before attesting cleanup or releasing identity authority.
- Machine-authorized boundary cancellation now treats typed missing-session and
  no-running-turn observations as already quiesced, while preserving every
  other cancellation error as a strict shutdown failure.
- Hosts embedding an `Arc<UnifiedRuntime>` can use
  `handle_unified_rpc_json_arc`; the gateway uses its live-handler counterpart
  so identity-owned cross-mob mutations remain supervised if the requesting
  connection disappears. The borrowed dispatcher fails those mutations
  closed because it cannot transfer runtime ownership into the supervisor.
- MobKit session-service wrappers now preserve Meerkat's runtime-turn-apply
  capability, allowing optional per-turn model selection in stock console
  integrations while unsupported autonomous, direct-session, and external
  member paths fail closed before admission.

### Fixed

- Release publication now requires all ten gateway archives, emits flat asset
  names in `index.json` and `checksums.sha256`, and keeps manual tagged builds,
  validation, and registry publication on the exact requested source tag. The
  registry job now retries public readback until the exact crate, Python wheel
  and source distribution, and npm package are all independently visible,
  non-yanked, downloadable, and checksum-valid.
- Runtime-turn diagnostics are structural-only and opt in with an explicit
  truthy value; prompts, appended context, tool dispatch metadata, and full
  runtime semantics are never written to the diagnostic log.
- The optional source-baseline verifier no longer embeds a developer machine
  path and fails closed with a typed error unless a checkout is supplied
  explicitly or through `MEERKAT_REPO`.
- Quiesced session persistence before lease rotation, retained exact committed
  grants across failed authority publication, reused live grants during eager
  reconcile, and drained identity-scoped orphan grants before direct lazy retry.
- Bound merged and protected structural event authorization to the emitting
  member's exact runtime incarnation and fence token, preventing a later
  public respawn of the same alias from disclosing an earlier secret event.
- Failed gateway identity bootstrap now installs runtime authority before
  materialization and runs the full ordered shutdown before returning the init
  error, releasing exact grants held by members that activated before a later
  eager-roster member failed.
- Serialized continuity generation changes with foreground materialization and
  reconcile, physically retired removed or changed roster members, and kept
  typed bootstrap readiness synchronized with lifecycle transitions.
- Hardened identity authority at raw member, RPC, console, SSE, and cross-mob
  boundaries: reserved aliases cannot be forged or preclaimed, policy checks
  use canonical targets, and operations resolve the current durable generation
  instead of a stale reset-era roster row.
- Prevented same-generation session rebinds from rewinding the durable
  checkpoint head, rejected continuity-generation regressions even under a
  newer fencing token, and moved persistent SQLite initialization and fencing
  floor reads off async executor workers.

## [0.7.39] - 2026-07-15

### Added

- Optional topology control plane, disabled by default: authorized operators
  can explicitly connect, disconnect, or reconnect agent pairs through
  revision-pinned, idempotent, durably journaled operations with recoverable
  receipts and a separately permissioned audit cursor. Cross-authority changes
  are supported only by the same-process bilateral coordinator, which checks
  both runtime authorities and rolls back partial physical changes; the local
  JSON-RPC surface fails cross-process mutation closed. The stock MobKit
  console gains a capability-gated Connections picker with pairwise actions
  and no implicit bulk-connect behavior.

### Changed

- The stock console now groups sidebar items semantically, keeps completed
  flow cards compact, targets flow restoration to the selected run, and gives
  long conversation tails their own scrollable region.

### Fixed

- Stabilized streamed conversation reconciliation: rich-content nodes retain
  their identity, conversation groups stay mounted, and group/message updates
  no longer replace or overwrite the active streaming tail.
- Restored canonical peer labels and transcript copy actions, and tightened
  activity-preview copy-action spacing across the shared console components.
- Hardened gateway test process draining and the stock Incident Commander
  acceptance path, including durable editable-topology state, replay-safe test
  sessions, and cold Cargo-lane fixture discovery.

## [0.7.38] - 2026-07-13

### Added

- Console health affordance from the machine-owned member liveness
  projection (ask 14 follow-up): sidebar rows show a `wedged`/`degraded`
  chip (silent when healthy), the roster dot tints by health, and the
  roster inspect pane gains Health / Run state / In flight / Last
  progress rows. Progress is fetched per non-final member and capped at
  64 members (`MOBKIT_CONSOLE_PROGRESS_MEMBER_CAP`; `0` disables) so
  large rosters do not fan out against the mob actor mailbox.

### Changed

- meerkat family pinned to `=0.7.31` (from `=0.7.30`): delivery-time
  mob member revival carries a machine-authorized
  missing-live-materialization intent, and executor publication is
  cancellation-safe (no false `Attached`/ready state without a live
  executor) — the delivery-time sibling of the ask 34 boot-revival fix.

## [0.7.37] - 2026-07-13

### Changed

- meerkat family pinned to `=0.7.30` (from `=0.7.29`): ask 34 ships —
  the retired-session revival flow completes end-to-end. The mobkit
  acceptance test (`identity_first_resume_revives_terminally_retired_
  runtime`, landed ignored in 0.7.36) is un-ignored and green: a
  terminally-retired durable session revives on ordinary resume with
  the transcript intact. Bug I is fully closed — existing victims
  revive automatically on their next boot; the manual `runtime_states`
  row repair is retired. Zero open upstream asks.

## [0.7.36] - 2026-07-12

### Changed

- Ask 29 (Bug G′) adopted: model-only reprofiles use the field-scoped
  `SpawnMemberSpec.model_override` seam — the pin is reapplied over the
  CURRENT definition profile on every materialization, so definition
  drift (tools, skills, peer posture) keeps reaching reprofiled members.
  The whole-profile snapshot survives only for provider-pinned profiles
  (upstream does not re-infer the provider under `model_override`).
  Realm-ref bindings now accept model overrides. Members frozen under
  the old whole-profile override heal on their next reset.

### Fixed

- The `MobSessionService` wrappers now forward the meerkat 0.7.29
  retired-session revival seam (`load_revivable_retired_session`,
  `promote_revivable_retired_session`,
  `create_session_with_machine_archived_resume_authority`,
  `load_persisted_session_metadata`) instead of masking it with the
  trait defaults. End-to-end revival still requires upstream ask 34
  (the revival flow stages executor registration against the
  still-Retired machine); the acceptance test ships `#[ignore]`d and
  un-ignores on the fixing meerkat release. Until then, existing Bug I
  victims still need the manual `runtime_states` row repair — new
  destruction is impossible on meerkat ≥0.7.29.

## [0.7.35] - 2026-07-12

### Changed

- meerkat family pinned to `=0.7.29` (from `=0.7.28`). Four upstream asks
  land and are adopted:
  - **Ask 32 (Bug I secondary-racer fence)**: retire disposition is
    machine-authorized and incarnation-scoped upstream; the 0.7.34
    collision drain-poll + retry-once workaround is removed from the
    resume bridge. The Bug I destruction-detection probe remains
    (ask 31 still open — `MOBKIT_IDENTITY_RESTORE_CONCURRENCY=1`
    remains the recommended boot mitigation until it ships).
  - **Ask 14 (member liveness)**: the machine-owned
    `MemberProgressSnapshot` (run_state, in_flight_work, last progress,
    health healthy/degraded/wedged) flows on `mobkit/member_status`,
    is typed as `MemberProgressSnapshot` on `RichMemberSnapshot` in the
    Python and TypeScript SDKs, and projects on the identity-inspect
    RPC as `progress`.
  - **Ask 28 (objective correlation)**:
    `objective_owner_bound`/`objective_concluded` mob events project
    through the structural event surface with member attribution.
  - **Ask 27 (delivery outcomes)**: typed
    `PeerDeliveryOutcome{Acked,HandedOff,Queued}` reaches agents
    upstream; verified no mobkit surface required exposure.

### Fixed

- `SessionStoreBackedRuntimeStore` now delegates every defaulted
  `RuntimeStore` method instead of masking the inner store's answers:
  the 0.7.29 compaction-projection outbox failed session create closed
  through the facade, and `is_runtime_projection_quarantined`
  (defaulting to "not quarantined") plus `delete_ops_lifecycle` were
  latent pre-existing masks.

## [0.7.34] - 2026-07-11

### Fixed

- Bug I mitigations (boot restore/retire race that terminally retires
  slow-restoring identities; root cause is upstream — asks 31/32/33):
  - `MOBKIT_IDENTITY_RESTORE_CONCURRENCY` env knob (clamped 1-16,
    default 4 unchanged); `=1` serializes identity restores, removing
    mobkit's contribution to SQLite writer contention during boot.
  - Collision-retry drain: after a roster-collision retire, the bridge
    polls roster absence (bounded 2s) before retrying the resume, and a
    retry canceled by the still-queued retire command is retried once
    more after a second drain.
  - Honest rejection contract: after any rejected resume the bridge
    probes the session store and logs explicitly when the durable
    session is GONE (destroyed by meerkat's spawn-failure rollback)
    instead of the now-sometimes-false "durable session preserved".

## [0.7.33] - 2026-07-11

### Changed

- meerkat family pinned to `=0.7.28` (from `=0.7.27`): GPT-5.6
  (Sol/Terra/Luna) in the catalog with `gpt-5.6-sol` as the new
  provider/catalog default (explicit GPT-5.5 pins stay honored; realtime
  capability unchanged), plus endpoint-recovery hardening (cold restart
  replays each member's exact generation peer endpoint; fail-closed on
  descriptor drift and PeerId reuse).
- Live seed bounding is upstream-owned: `seed_max_chars` (per-open or
  `runtime_options.live`) now delegates to meerkat's windowed projection —
  enabled root context, an affordable compaction summary, and the
  identity/tombstone/rewrite-generation/canonical-image sidecars are
  preserved, with explicit degraded-continuity reporting. The 0.7.32
  oldest-first clamp stopgap is removed; wire parameters are unchanged.
- The realtime open config and projection snapshot carry the
  `canonical_user_image_decoded_bytes` image-budget sidecar.

## [0.7.32] - 2026-07-10

### Added

- Live image input (meerkat 0.7.27): still images ride the existing
  `mobkit/live/send_input` as `{kind: "image", idempotency_key, mime,
  data}` — exact-retry deduplicated by the runtime's user-content identity
  lane, which also rides the open config so reopened channels do not
  replay committed images. SDK conveniences `live_send_input_image` /
  `liveSendInputImage`. Only `gpt-realtime-2` accepts image input in the
  shipped catalog.
- Deterministic (non-LLM) schedule targets from the SDK:
  `runtime_options.host_runnables` registers named host runnables whose
  fire forwards over the stdio callback bridge as `callback/schedule_fire`;
  Python `MobKitBuilder.host_runnables([...])` +
  `runtime.on_schedule_fire(name, handler)`. Agents target them through the
  `meerkat_schedule_*` tools' `host_runnable` target kind.
- Per-open instruction overlay on `mobkit/live/open` (`instructions`:
  string or array) — carried on the runtime-system-context lane, ephemeral
  to the open, never persisted into the durable transcript; drops on
  `live/refresh` by construction.
- Callback/SDK tools publish real input schemas:
  `SessionBuildOptions.register_tool(..., input_schema=...)` flows through
  the build callback into the tool defs the provider (and live seeds) see;
  schema-less registrations stay wire-compatible.
- `mobkit/live/truncate` + `live_truncate`/`liveTruncate` SDK methods.
- Live seed clamp: `runtime_options.live.seed_max_chars` (+ per-open
  override) drops whole seed messages oldest-first to fit the realtime
  provider's instruction cap — an explicit stopgap for upstream ask 30
  (seed-window/summarized projection belongs in core).

### Changed

- meerkat family pinned to `=0.7.27` (from `=0.7.26`): image input on the
  OpenAI Realtime live channel end to end, the realtime user-content
  identity lane (exact-retry + live rewrite guards), redacted image
  receipts synthesized only after durable reducer application, and an
  explicit durable start boundary for mob retirement (new mob event kinds,
  projected by the console/SSE surfaces).
- Identity-first bootstrap restores up to four durable members concurrently
  instead of serially, with bounded fan-out and per-member restore timing logs
  for slow resumes. This prevents large full-history rosters from accumulating
  every member's resume latency inside `mobkit/init`.

## [0.7.31] - 2026-07-09

### Added

- Live (realtime) member sessions through the gateway: meerkat's live/*
  surface on mob members. `mobkit/live/{open,status,close,refresh,
  send_input,commit_input,interrupt}` accept identity targets; the live
  WebSocket transport mounts on the gateway's EXISTING HTTP listener
  (`runtime_options.live = true | {public_base_url}`, default off,
  persistent mode only); tool calls made during live turns flow through
  the member's normal external-tool machinery (callback bridge + gating
  unchanged); live turns persist into the member's durable transcript
  through the continuity adapter. Machine-authority-backed single-use
  bootstrap tokens; per-open credential resolution matching text-turn
  auth; per-open `model` override. SDK methods in both languages. Design:
  docs/design/live-sessions.md.
- Definition wiring is a reconcilable desired state (the downstream app's field
  report): `auto_wire_orchestrator`/`role_wiring` now converge regardless
  of member bring-up order and re-converge after restarts — a
  definition-derived default edge policy installs at bootstrap,
  `ensure_member` reconciles after every materialization, and the console
  surface's `mobkit/reconcile_edges` noop stub is a real wire-only
  reconcile. Host-made manual edges are never unwired.
- Cross-mob DX: `cross_mob/peer_info` accepts durable identities (roster
  `agent_identity` label fallback); `cross_mob/wire_local` accepts the
  `ed25519:`-prefixed `transport_public_key` spelling.

### Fixed

- `ContinuitySessionStoreAdapter` adopts the machine-owned
  `RebuildToAuthority` rollback (the downstream app's Bug B-2): the torn-shutdown save
  wedge — a stamped intra-turn checkpoint head rejecting the resume's
  shorter committed-authority save with `MonotonicityViolation`, degrading
  the identity forever — now converges the row back onto committed truth.
  Unstamped rows and content forks keep failing closed.
- The BigQuery session-store adapter shares one process-wide HTTP client
  with per-request timeouts, and runtime bootstrap pre-warms the client
  stack: the realtime feature unification enabled reqwest's
  `system-proxy`, putting a ~700ms one-time macOS proxy scan inside the
  first HTTP-backed RPC's latency window.

### Changed

- meerkat family pinned to `=0.7.26` (from `=0.7.25`): cold revival of
  stopped sessions re-binds under the fresh registration epoch (the
  identity-first member-revival terminal failure), typed rejections are no
  longer laundered into "session not found in runtime adapter", one
  member's composition-dispatch rejection no longer terminates the whole
  mob actor, and the classic-store projection bridge consults the
  write-half rollback (the upstream half of Bug B-2).

## [0.7.30] - 2026-07-09

### Fixed

- The rpc_gateway's `callback/build_agent` path composes host-returned tools
  OVER pre-installed dispatchers instead of assigning the slot wholesale
  (the downstream app's "Bug D"): callback-built agents keep the native agent-memory
  recorder's `memory` tool (a host tool named `memory` now shadows it by
  design — primary wins name collisions). New
  `meerkat_mobkit::tool_compose::ComposedExternalTools` is the canonical
  compose utility; both dispatch entry points forward, so the
  `ToolDispatchContext` attention witness survives.
- The rpc_gateway stdin dispatch loop serves RPC requests concurrently
  (each on its own task): a turn- or build-running RPC blocked on a host
  callback round-trip no longer starves reentrant requests — an
  `agent_memory/recall` issued from inside a callback tool handler used to
  queue behind the turn until the 120s callback timeout.
- `UnifiedRuntime::shutdown` quiesces in-flight member work before stopping
  the mob: meerkat 0.7.25's machine refuses `Stop` mid-work
  (`InvalidTransition`), so shutdown cancels member work and retries over a
  bounded window — a gateway going down mid-turn stops cleanly.

- Console live tails no longer starve on identities the read model has not
  observed (issue #254): identity-scoped SSE streams get the same
  own-identity allowance the windowed query path always had, and a frame
  for an unknown identity triggers a debounced identity read-model
  refresh, so members spawned mid-run (`ensure_member`) become visible on
  unscoped streams without a reconnect.
- The console aggregator gained `unregister_runtime` (issue #254
  follow-up): removes the registration, signals the live-projection task
  (previously immortal — its broadcast receiver could never observe
  `Closed` while the runtime lived), terminates the discovery loop, and
  refreshes the identity read model. Re-registering a key now also
  replaces its projection task instead of double-projecting.
- `/agents/{id}/events` accepts durable identities (issue #254 item 4):
  ids resolve via the roster's `agent_identity` label when direct encoding
  misses (the #252 canonicalization class), so identity-first consumers no
  longer need a `list_members` round-trip; unknown ids keep the proper 404.
- Docs: `mobkit/console/*` methods are HTTP-only (the stdio surface never
  dispatched them), and session-history backfill frames
  (`source.kind == "session_history"`, `interaction_id` null) are a
  mandatory exclusion when correlating completions.

### Added

- Full WorkGraph integration (meerkat 0.7.23's goals, work items, and
  attention bindings): a realm-scoped `WorkGraphService` per runtime with
  the member tool surface (profile `tools.workgraph`, default off),
  apply-time attention overlays on every mob-executor turn, the
  `mobkit/workgraph/*` JSON-RPC group (22 methods on the unified and
  console surfaces, `workgraph.view`/`workgraph.manage` ABAC actions,
  capabilities and experience projection), full Python and TypeScript SDK
  parity with typed results and conflict errors, and a conversation-native
  inline WorkGraph card in the console chat pane (live goal/work-item tree
  folded from agent tool calls, ABAC-gated operator actions with CAS
  revisions) plus a WorkGraph workbench panel. A one-binding-per-target
  admission layer (occupancy guard across create/reassign/resume on both
  the RPC and agent tool planes, session/identity target unification,
  cross-process serialization) protects members from upstream's
  MultipleActiveBindings hard-fail until meerkat lands binding uniqueness
  (upstream ask 25). Hardened by a six-round adversarial review battery
  (64 verified findings fixed) and live-fire verified end to end.

- `mobkit/workgraph/attention/prune` (upstream ask 24): terminal-binding GC
  on both RPC surfaces plus `workgraph_attention_prune` /
  `workgraphAttentionPrune` in the SDKs.
- `mobkit/workgraph/attention/break_glass_reassign` (upstream ask 23,
  console surface ONLY): host-plane recovery for a binding stuck on a
  wedged/retired agent with no coordinator holding authority. The principal
  is the authenticated console principal — never a wire parameter — a
  non-empty reason is mandatory, and upstream records both in the workgraph
  event stream. Deliberately absent from the stdin surface and the SDKs.
- Interaction identity threads end to end for identity-first console sends
  (upstream ask 15): the console mints deterministic UUIDv5 interaction ids
  and threads them through `WorkSpec` into meerkat runtime admission, the
  session-history backfill stamps `interaction_id`/`run_id` from persisted
  transcript messages, and the console dedup treats UUID-form ids as
  authoritative twin identity — exact live↔history joins, and repeated
  identical replies from DISTINCT interactions both render (the over-cull
  class). Classic sends keep the text heuristic (the external work door
  cannot thread an id yet).

### Changed

- meerkat family pinned to `=0.7.25` (from `=0.7.23`): the outstanding-asks
  sweep. Store-owned attention-binding uniqueness with typed occupant-naming
  conflicts (ask 25 — mobkit's admission layer demotes to defense-in-depth
  plus session↔identity alias unification; `MultipleActiveBindings` is gone),
  machine-owned revival of Stopped sessions (0.7.24), O(delta) incremental
  session persistence, the structural `reply_to_peer` affordance (ask 26),
  metadata-only session reads (ask 24 clause 3), and the schedule
  single-owner fix (`SessionRuntime` arms its own firing host — mobkit's
  gateways already bound tools+host from one `ScheduleService`, so no
  mobkit-side change was needed).

## [0.7.29] - 2026-07-08

### Changed

- meerkat family pinned to `=0.7.23` (from `=0.7.22`). Ask 21d verified
  fixed: never-run identity-first mob workers no longer strand in
  archive-NotFound during disposal — the doctrine worker-respawn test now
  asserts success (tolerance branch removed). With 21c (0.7.22) and 21d
  (0.7.23) the whole never-run-member disposal family (asks 20/21/21b/21c/
  21d) is closed on BOTH constructions. API drift absorbed: the
  runtime-backed schedule host takes an optional `WorkGraphService`
  (mobkit passes `None` — WorkGraph is not wired yet), and
  `flow_tool_overlay` → `turn_tool_overlay` on turn semantics/metadata.

### Fixed

- Schedule self-delivery now reaches identity-first members: the internal
  delivery lane canonicalizes the binding's member id (decode-then-encode)
  before the roster lookup. Binding member ids arrive in ROSTER space —
  identity-first bridge members' roster ids are the comms-encoded runtime id
  (`mk--…`) — and the previous lookup re-encoded them (the codec re-encodes
  marker-prefixed input by design), missed the roster, and silently fell
  through to the external door, reproducing the addressability rejection the
  internal lane exists to bypass ("mob member is not externally addressable:
  mk--rt_cdomain_chome_c0" on the downstream app's 0.7.28). Plain member names are
  unaffected (canonicalization is the identity on them). E2e pins the
  roster-space binding shape end to end; the codec contract test pins
  decode-then-encode as the only correct roster-key derivation.

## [0.7.28] - 2026-07-07

### Changed

- meerkat family pinned to `=0.7.22` (from `=0.7.20`; 0.7.21 was skipped —
  its ask-21b archive arm routed never-run-member disposal into a
  pre-existing runtime-loop self-deadlock on the session mutation gate and
  wedged the whole mob, upstream ask 21c). 0.7.22 fixes the deadlock class
  structurally (stop realization is guard-free by construction) and with it
  the whole ask 20/21/21b/21c never-run-member family on the classic
  persistent chain: retire/respawn of never-run `ensure_member` crews now
  converges, and the K1 persistent regression test runs un-ignored.
  Residue: mob-plane workers under the identity-first gateway construction
  still archive-NotFound on respawn (upstream ask 21d, P1, fast-fail — no
  wedge, no new strand class).

### Fixed

- Schedule delivery to a mob member is now INTERNAL addressing: the schedule
  host wraps the mob delivery host so member-addressed prompts go through
  the internal work lane (`WorkOrigin::Internal` — the same door the
  identity bridge uses), not the external ingress door. Previously delivery
  routed through `member_send`'s external path, which rejected members whose
  profile is `external_addressable = false` ("mob member is not externally
  addressable") — the downstream app's domain agents are internal-only by design, and
  a schedule firing back into its own author's session was blocked by the
  external posture. Flows/helpers/probes keep the stock host behavior. E2e
  pins self-delivery to an internal_only author at `stage=completed`.

## [0.7.27] - 2026-07-07

### Fixed

- Agent-authored schedules now DELIVER on both gateways (the downstream app's 0.7.26
  "last link": authoring ✓ planning ✓ claim ✓ no runaway ✓ delivery ✗ with
  "scheduled identity targets are not supported by this session host").
  Root cause was mobkit-side: specs built via `MobBootstrapSpec::new` (both
  gateway binaries roll their own session services) never installed the
  agent mob tool surface, so `agent_mob_mcp_state()` was None — members
  still got mob tools through meerkat-mob's INTERNAL state, but mobkit's
  schedule host had no mob authority: the authoring-time rewrite to
  mob-member targets could not resolve sessions, and `spawn_schedule_host`
  fell back to the Noop mob host, leaving identity/mob targets undeliverable.
  New public seam `MobBootstrapSpec::with_agent_mob_tools` performs the
  install for externally-constructed specs; both gateways wire it. End-to-end
  tests pin the full chain (agent tool dispatch → planning → claim →
  delivery `completed`) for both the rewrite path and the delivery-time
  identity-recovery path.

## [0.7.26] - 2026-07-07

### Changed

- Upgraded the meerkat family 0.7.19 → 0.7.20 (exact pins). Carries the
  ask-22 fix for the P0 one-shot runaway (trigger yields/compares
  ms-truncated dues + a machine-owned planning-monotonicity invariant so
  future representation bugs converge as refill faults instead of
  generating occurrences unboundedly) — carry-verified by the previously
  failing `one_shot_misfire_must_not_regenerate` guard, now green. Also
  carries the ask-21 archive-scoped read fix.

### Known issues

- Ask 21b (docs/design/upstream-asks.md, ask 21 addendum): the 0.7.20
  archive-scoped read fix commits the durable document, but archiving a
  never-run registered session still returns NotFound afterwards — the
  `retiring` strand persists for mob-plane members that never ran.
  Two-adapter split ruled out empirically on the mobkit side. Identity-first
  gateways (default-on) remain unaffected for crews.

## [0.7.25] - 2026-07-07

### Changed

- Upgraded the meerkat family 0.7.18 → 0.7.19 (exact pins). Carries the full
  batch-3/4 ask set: schedule firing per-row fault tolerance + tick-health
  incidents + Deleted-tombstone healing + SQL-bounded claim (asks 16-19 —
  carry-verified: a poisoned occurrence row no longer starves healthy
  claims), bounded force_cancel (M1), declarative MCP for library embedders
  surviving revival (M2), versioning policy (M3), run-status reconciliation
  query (M4), and the ask-20 host-owned disposal fix. Also regenerates the
  bundled adaptive layer-decision schema against the 0.7.19 canonical.

### Fixed

- MobKit's session-service wrappers now forward meerkat 0.7.19's
  `session_known_to_archive_authority` disposal-routing seam (the trait
  default is fail-closed `true`; an unforwarded wrapper would have swallowed
  the inner persistent service's real store read — the compose-don't-assume
  wrapper class). The forwarding-probe test pins it.

### Known issues

- Ask 21 (docs/design/upstream-asks.md): mob-CREATED never-ran members are
  archive-authority-OWNED yet snapshotless, so the owned-path archive still
  strands them — 0.7.19's ask-20 fix reroutes only host-adopted sessions.
  Identity-first gateways (default-on) sidestep this for crews; the window
  on the worker plane is narrow (workers receive kickoff at spawn).

### Changed

- **`mobkit_gateway` is identity-first by default** (doctrine phase 2):
  every init now builds the durable-identity substrate — continuity store
  with fencing-floor seeding, lease provider, identity console RPC surface,
  Broken-identity repair task — via the new shared
  `gateway_wiring::open_identity_substrate` (the same code path
  `rpc_gateway` uses, ending the two-half-wired-gateways drift that caused
  the K1/K2 field failures). `identity_first: false` on init is a
  one-release opt-out restoring the pure mob-plane gateway. On an
  identity-first gateway `ensure_member` stands up durable identities;
  pass `plane: "worker"` to pin a spawn to the ephemeral mob plane
  (idle-retire reaping, no continuity record).
- `mobkit/capabilities` on both dispatchers now carries a top-level
  `identity_first` flag — the zero-flag-day migration signal consumers gate
  on (meerkat-studio contract point 5).

### Fixed

- Member-scoped RPCs can no longer mutate identity-owned members behind the
  IdentityRuntime's back: on any runtime carrying the identity substrate,
  `retire_member`/`respawn_member` addressed at a durable identity (plain
  name or `rt:` runtime alias) now route through the identity authority —
  tolerant retire, and reset-respawn (fresh session, same identity, new
  generation) — on BOTH dispatchers (SDK stdin `rpc.rs` and the console).
  Worker-plane members (not identity-registered) keep the classic path per
  the doctrine. Also parity: the console's classic `retire_member`/
  `respawn_member` arms now carry the same completed-disposal cleanup
  tolerance and respawn repair as the SDK dispatcher and the identity-named
  console paths (previously the console arms surfaced raw errors the other
  two surfaces tolerated).

### Added

- Identity-first doctrine recorded (docs/design/identity-first-doctrine.md):
  MobKit is dual-plane by decision — durable members on the identity plane
  (continuity, leases, reconcile), ephemeral workers on the mob plane
  (spawn/idle-retire); neither plane is deprecated, but building durable
  populations on the mob plane is. Published docs updated to match
  (roster.mdx frames the member API as the worker plane; the Python SDK's
  member-per-user "first contact" pattern now stands users up on the
  identity plane), and the mobkit-platform skill teaches the doctrine.

### Added

- Identity-first gateway mode (meerkat-studio ask K0): `identity_first: true`
  on `mobkit_gateway` init boots the durable-identity substrate — continuity
  records + lease-fenced embodiment (default providers constructed from the
  existing store paths), resume-first restore with the Broken-identity
  repair task, and the identity console RPC surface. An optional
  `identity_roster` init param seeds the desired identities;
  `mobkit/ensure_member` upserts the roster and reconciles at runtime, and
  `retire_member`/`respawn_member` route through the tolerant identity
  lifecycle — the ask-20 retire/respawn failure class does not exist on this
  surface (proved by test: retire + respawn of never-ran members succeed).
  Rust embedders get the same via the new
  `identity_first::MutableRosterProvider` + `UnifiedRuntime::set_console_identity_roster`
  + `attach_identity_first_context` (or `UnifiedRuntimeBuilder`'s existing
  roster/continuity/lease inputs on the definition path).

### Fixed

- `mobkit_gateway` now installs a tracing subscriber (stderr, `RUST_LOG`
  honored, default `warn`) and logs a version-stamped startup line.
  Previously the binary dropped every tracing event in the process —
  runtime failures, console internal-error logs, and the schedule claim
  watchdog's stall diagnosis were all invisible; meerkat-studio root-caused
  their opaque K1/K2 failures to exactly this gap on the child gateways
  their app spawns. `rpc_gateway` already had the subscriber.

### Added

- Mob-wide, revival-surviving external-tools seam (meerkat-studio ask K4):
  `MobBootstrapSpec::with_default_external_tools_provider` forwards to
  meerkat-mob's per-spawn provider, so tools attached there survive member
  respawn/revival — unlike the per-spawn `SpawnMemberSpec.external_tools`
  overlay, which revival drops. Note: a profile's `tools.mcp` allowlist gates
  what the provider exposes, and an EMPTY allowlist means the full surface.

### Changed

- Console JSON-RPC internal errors now carry the real failure reason on the
  wire (meerkat-studio ask K2): `message` and `data.detail` hold the error
  chain instead of an opaque `{"error":"internal_error"}` (the kind marker is
  kept for existing clients). This deliberately reverses the earlier
  redaction posture for THIS surface: every caller that reaches these
  handlers is an operator by construction (auth-gated consoles 401 first;
  open consoles are a trusted-local deployment choice), and the redaction
  made every -32000 undiagnosable. `console_send`'s public-message redaction
  is unchanged (that surface can reflect into agent-visible space). Also
  covers the intermittent mob-spawn 500s reported as K3 wherever they
  originate on mobkit's surface.
- The meerkat family is now exact-pinned (`=0.7.18`) in Cargo.toml
  (meerkat-studio ask K5): the compatible meerkat version is declared, not
  archaeologized from Cargo.lock at release tags.

### Known issues

- `mobkit/retire_member` / `mobkit/respawn_member` fail for never-ran
  members on persistent session services and strand them in `retiring`
  (meerkat-studio K1) — root-caused upstream (ask 20,
  docs/design/upstream-asks.md): the ArchiveSession disposal step NotFounds
  on a member that never committed a runtime snapshot. Repro ships as an
  `#[ignore]`d test in `tests/studio_k_asks.rs`; the fix lands with the next
  meerkat release.

- Schedule claim watchdog (both gateways): meerkat's firing driver discards
  its own tick errors, and one poisoned row anywhere in the schedule store
  (a Deleted tombstone the recovery invariant rejects, a stale-schema
  occurrence) aborts every tick before anything is claimed — due occurrences
  then sit `pending` forever with nothing in any log (the downstream app's 0.7.24
  report: 31/31 pending, no lease ever taken). The new read-only
  `spawn_schedule_claim_watchdog` probes the pipeline every 60s and, when
  due work is not being claimed, logs a row-level diagnosis naming the
  poisoned row. It cannot make the driver claim past a poisoned row — the
  driver fixes are upstream asks 16-19 (docs/design/upstream-asks.md).

## [0.7.24] - 2026-07-06

### Changed

- Upgraded the meerkat family 0.7.17 → 0.7.18. Carries the fix for the
  downstream app's cold-restart regression (meerkat #837): idle members whose
  turn-less boots chained resume-system-prompt-refresh rewrite commits were
  refused resume with "incoming append-only save would change retained
  transcript revision graph" — the rewrite-chain walk now proves continuity
  in authority order and no longer fails closed on the chained-refresh
  shape. New mobkit-side idle-member coverage: repeated turn-less restarts
  (with prompt drift presented) must keep resuming onto the same durable
  session and replay history on the eventual turn.

### Added

- Broken identities now self-heal: a background continuity-repair task
  (spawned by both the gateway and `UnifiedRuntimeBuilder` deployments)
  re-runs the reconcile flow with doubling backoff while any identity sits in
  the Broken state, so a resume that starts succeeding again — a transient
  cause cleared, or an upstream fix deployed — restores the identity to
  functional without a process restart. Previously "degraded pending
  reconcile retry" promised a retry that only a manual
  `mobkit/reconcile_identity` RPC or a restart delivered: delivery refuses
  Broken identities by design, and so does on-demand materialization, which
  is how the downstream app's 14 preserved-but-parked identities stayed parked. The
  task is quiet while nothing is Broken (no lease churn on healthy
  deployments) and the backoff bounds retry log noise.

- Console Memory panel Phase 2/3: Holdings reads real store totals with
  per-scope FLOOR PRESSURE markers and a data-driven STORE FLOOR verdict
  tile plus the pending-harvest queue; Knowledge shows the durable injection
  ledger with consecutive-duplicate DUP badges; Pipeline gains the pending
  proposals lane (taint badges) and the read-only "memories you might want
  to correct" audit review queue; Dreams renders the durable verdict sheets
  (per-partition runs with phases, verdict counters, and skips). Backed by
  four new read-only panel RPCs — `overview`, `proposals`, `injections`,
  `harvests` — under the existing memory read grant; the health snapshot
  stays deferred to the distinct-affordance design.

### Changed

- The rpc gateway's agent-memory assembly converged onto the `memory_wiring`
  module: the SQLite store, taint firewall, Distiller, and Steward are built
  by the same code path the Rust builder uses
  (`persistent_agent_memory_stack`), with the gateway's extras — outbound
  taint declarer, console panel registration, Hygienist, compaction-reset
  sink, observer spawn, and schedule-host dream suppression — layered as
  seams. One assembly, two hosts; behavior unchanged.

## [0.7.23] - 2026-07-05

### Changed

- Upgraded the meerkat family 0.7.15 → 0.7.17. Carries (from the batch-2
  upstream asks): typed `ToolProvenance` on tool definitions (ask 9's
  attribution half) and the compaction-archive fix that stranded compacted
  singletons on retire (`MonotonicityViolation`, ask 12 / the operator deployment's report).
  Remaining asks (dispatch-time taint surfacing, fork authorization,
  incremental session persistence, forwarder backoff, member health,
  transcript interaction-id persistence) land in later meerkat releases.
  One API break absorbed: `PersistentSessionService::new` now requires the
  runtime store (previously `Option`).

### Added

- The full agent-memory stack is now reachable from the Rust builder
  (`UnifiedRuntimeBuilder::persistent_agent_memory_stack`): the bundled
  SQLite store with the taint firewall plus the Distiller and Steward
  engines, member-event observer, console panel registration, and the
  steward dream loop — the same stack the rpc gateway assembles, for
  embedders that drive the builder directly (no gateway). Assembly lives in
  the new `memory_wiring` module. v1 boundaries documented there: the
  Hygienist stays gateway-only, and engines are driven by the observe
  stream (the gateway's extra injector-side rotation hooks are follow-up).
- Operator-scope memory recall is live in stock deployments (provisional
  OperatorId keying: the console auth principal). With
  `agent_memory.operator_scope = "provisional"`, the gateway now installs a
  console-principal resolver: when an authenticated principal sends to an
  identity from the console, that principal becomes the identity's active
  operator and operator-scope records compose into recall — the durable
  replacement for live-behavior-correction broadcast hacks. Unauthenticated
  consoles keep the scope inert (activation requires config AND resolver AND
  a real principal). Keying stays swappable behind the OperatorResolver seam.
- Durable dream verdict sheets: every steward dream run persists to a new
  `dream_runs` table (partition label, timings, ops, and the full
  phases/verdicts/skips detail), and dead-weight usage-audit verdicts land in
  a durable review queue (`dream_audit_verdicts`) — the operator-facing
  "memories you might want to correct" list, resolvable per record. Two new
  read-only console RPCs serve them: `mobkit/memory/panel/dream_runs` and
  `mobkit/memory/panel/audit_verdicts` (same read grant as `panel/dreams`).
- Per-mob steward dreams (§8.5): with `agent_memory.steward.per_mob = true` on
  a multi-mob host, each dream attempt runs one partition per mob — that mob's
  scope plus its members' identity scopes, consolidated against that mob's own
  purpose/roster context — plus a realm-remainder partition owning
  operator/realm scopes, unrostered identities, and promotion/operator review.
  Each partition run takes its own runs-per-day budget slot. With 0–1 mobs the
  whole-realm dream is used unchanged (the flag was previously parsed but
  inert).

### Fixed

- The single-embodiment guard fails loudly: restoring or materializing an
  identity whose durable lease another live runtime instance holds now
  returns the typed `AlreadyEmbodied { identity, holder }` error naming the
  holding instance, instead of collapsing into a generic missing-lease
  error. This is a policy point, not a structural assumption — future
  multi-bind/forked-session semantics relax exactly this arm.
- Delivery repair no longer rotates a wedged member onto a fresh, empty
  session. The identity bridge's deliver-time repair used to blind-respawn
  (`MobHandle::respawn` = retire + fresh spawn), abandoning the durable
  transcript and rotating the bridge session out from under the durable alias
  (the operator deployment's `identity_alias_respawn_rotation` report). Repair now rebuilds the
  member ONTO its recorded durable session (`MemberLaunchMode::Resume`) —
  transcript preserved, no session rotation; the fresh respawn survives only
  as the fallback for meerkat's explicit "durable session snapshot is gone"
  answer, and every other resume failure fails the delivery loudly instead of
  destroying state.

## [0.7.22] - 2026-07-04

### Changed

- Upgraded the meerkat family 0.7.14 → 0.7.15. Carries the upstream
  transcript-restart-loss guard fix: on resume, a byte-identical re-sent base
  prompt is preserved untouched and a changed prompt becomes an audited
  transcript rewrite — both survive, composing with this release's
  Inherit-on-resume default.

### Fixed

- **A rejected resume no longer destroys the conversation.** The identity-first
  session bridge used to catch *any* resume error and fall back to a fresh,
  empty member spawn — permanently abandoning the durable transcript (the
  downstream app's restart-loss bug). A resume failure now keeps the identity → session
  binding intact, marks the identity **Broken** with the real error attached,
  and the next reconcile retries the resume. A roster collision (in-process
  restart) retires the stale member and retries the *resume*, never a fresh
  spawn.
- Reconcile outcome reporting is honest: a bridge resume that fresh-spawned
  reports `created` (never `resumed`), and a successful resume without a
  checkpoint snapshot reports `resumed` (the persisted mob session carried the
  history). Resume errors are classified from meerkat's typed errors
  (`MemberRestoreFailed`, transcript-continuity violations) instead of being
  bucketed into one `runtime_identity_incompatible` reason.
- Resume inherits the persisted System message instead of re-sending the
  spec's explicit prompt. Re-sending made meerkat re-assemble the prompt,
  which trips the session store's transcript-continuity guard on meerkat
  ≤0.7.14 whenever the persisted prompt carries runtime context appends —
  the cold-restart transcript-loss class. Dynamic per-boot context belongs in
  runtime system-context appends, not the base prompt.

### Added

- The cold-restart continuity regression now runs in CI (previously an
  ignored scaffold): boot → turn → full restart against the same on-disk
  store → assert the identity resumes onto the same session id with the
  transcript replayed, twice (second-restart variant). Its earlier "harness"
  failure was in fact the outcome-reporting lie this release fixes.

## [0.7.21] - 2026-07-04

### Changed

- Upgraded the meerkat family 0.7.13 → 0.7.14. No API breaks.

### Fixed

- Carried schedule stores authored before 0.7.20 (which hold `schedule_json`
  as TEXT) now read after upgrade instead of failing every read with
  `Invalid column type Text at index: 0, name: schedule_json` — meerkat 0.7.14
  reads schedule JSON columns through a `Text`/`Blob`-tolerant boundary
  (upstream ask A). This unblocks the identity-target schedule repair and the
  steward-dream find-or-create on upgraded deployments.
- A full process restart against a carried on-disk store now resumes each
  agent's transcript instead of falling back to an empty fresh spawn. meerkat
  0.7.14's append-only continuity guard compares the shared transcript prefix
  by content address, tolerating the bookkeeping-only divergence (re-stamped
  run identity + timestamps) a re-created runtime authority produces on cold
  restart (upstream ask B).

### Added

- **P3-outbound (upstream ask 5, outbound half):** a member whose session
  ingests untrusted content now stamps its own signed content-taint on outbound
  peer envelopes (`declare_member_outbound_taint`), cleared on a clean session
  rotation or reset. Receivers read the sender's declaration instead of
  reconstructing taint from host-side joins, propagating taint cross-process.
- **P5 (upstream ask 7):** the memory steward's dream now runs as a durable,
  misfire-aware host-runnable schedule occurrence (idempotent find-or-create
  against the persistent schedule store) instead of a bare in-process interval
  loop; the loop is kept only as the fallback on gateways with no schedule host.
- Upgrade-carry regression tests crossing a store-version boundary
  (`schedule_store_text_carry`; `identity_first_cold_restart_continuity` as an
  ignored scaffold).

## [0.7.20] - 2026-07-03

### Added

- Identity-first agent memory injection for Rust, JSON-RPC, Python, and
  TypeScript users, including bundled markdown persistence, contextual recall,
  explicit remember/recall/forget APIs, and next-turn injection for live
  identity sends.
- Classic (roster-less) agent memory: the recorder `memory` tool and build-time
  recall injection now attach to any mob member keyed on its `AgentIdentity`,
  without requiring the roster/IdentityRuntime layer.
- Ambient per-turn memory recall is delivered as a typed injected-context
  message (excluded from compaction indexing) rather than fused into the user's
  text, and is now **on by default** on the identity-first path — echo-safe by
  construction (meerkat 0.7.12 upstream ask 1).
- Compaction-discard harvest reads the exact session-memory range via
  `enumerate_scoped`, and permanently-orphaned session scopes are reclaimed via
  `drop_scope` at the identity-delete seam (asks 2 + 8).
- Distilled memory records pin the transcript head revision their evidence was
  read at, and Hygienist rewrites compare-and-swap on it (ask 4 refinement).
- The taint tracker consumes the typed `AgentEvent::PeerContentIngested`
  sender-taint declaration (canonical peer id, synchronous at ingestion),
  superseding the projection-text join for declared taint (ask 5, inbound).

### Changed

- Upgraded the meerkat dependency family to 0.7.13.

### Fixed

- Host `SessionHook`/customizers that install `external_tools` or
  `additional_instructions` on the session build now compose over the
  agent-memory recorder instead of overwriting it — a plain assignment silently
  dropped the `memory` tool and the build-time recall injection (fixed in the
  incident-command-center example; the recorder is unaffected on paths that
  compose).

## [0.6.39] - 2026-05-21

### Fixed

- Identity-first console records now expose the durable `agent_identity` label
  as the UI/send identity while preserving the runtime member id for bridge
  operations.
- Identity-first external-authoritative runtimes now resume from the supplied
  `ContinuitySessionStoreAdapter` after process restart instead of looking only
  at the process-local runtime snapshot store.
- Console live snapshots no longer read each member's session while building
  `/console/experience`, `/console/modules`, or console member JSON. Snapshot
  model capabilities now come from the mob roster profile, so busy in-flight
  agent turns cannot block the admin console.
- The bundled console's JSON fetch timeout now defaults to 60s and reports an
  explicit timeout reason when aborted. `ConsolePolicy.fetch_timeout_ms` can
  project a custom fetch ceiling to the frontend.

## [0.5.2] - 2026-03-30

### Added

- **PreBuildHook** — `persistent_with_hook()` / `ephemeral_with_hook()` accept a `PreBuildHook` closure that mutates `CreateSessionRequest` at `create_session` time, before the session service captures labels and LLM identity. Use for per-agent external tools, system prompt augmentation, label injection, and model overrides.
- `PreBuildMobSessionService` — wraps any `MobSessionService`, intercepting only `create_session`. Implemented via `delegate_mob_session_service!` macro for maintainability.
- `PreBuildHook` type alias exported from crate root
- 3 new tests: `persistent_with_hook_wraps_session_service`, `ephemeral_with_hook_creates_spec`, `pre_build_hook_mutates_create_session_request`

### Fixed

- **Session checkpointing disabled in v0.5.1** — `persistent()` passed `Some(InMemoryRuntimeStore)` to `PersistentSessionService`, which disabled the `StoreCheckpointer` (`enabled: self.runtime_store.is_none()`). Session data was never persisted after turns. Fixed by keeping `runtime_store = None` on the session service and supplying the adapter directly via `MobBootstrapSpec.runtime_adapter`.
- **Stop/resume state recovery** — `RuntimeSessionAdapter::ephemeral()` has no state recovery. Replaced with `RuntimeSessionAdapter::persistent(InMemoryRuntimeStore, blob_store)` on the spec, giving the mob actor a `PersistentRuntimeDriver` that recovers queued runtime inputs across stop/resume within the process lifetime.
- `MobBootstrapSpec` now carries `runtime_adapter: Option<Arc<RuntimeSessionAdapter>>`, wired into `MobBuilder::with_runtime_adapter()` before `with_session_service()` at bootstrap time.

## [0.5.1] - 2026-03-30

### Fixed

- **Comms drain never spawned** — `MobBootstrapSpec::persistent()` and the gateway binary both passed `None` for `runtime_store`, causing `PersistentSessionService::runtime_adapter()` to return `None`. The mob actor's comms drain was never spawned, so AutonomousHost agents exited after their kickoff turn and could not receive subsequent messages. Fixed by supplying an `InMemoryRuntimeStore`.
- Added `meerkat-runtime` as a direct dependency
- Added regression test `persistent_bootstrap_provides_runtime_store`

## [0.5.0] - 2026-03-29

### Added

- **Cross-mob communication** — contact directory (TOML), `wire_cross_mob`/`unwire_cross_mob`/`send_cross_mob` via `PeerTarget::External`, `peer_info` + `wire_local`/`unwire_local` for SDK-level peering, `::` separator addressing
- **Console RPC** — `POST /console/rpc` JSON-RPC endpoint with full auth gating, capabilities, event log queries, cross-mob peer_info/directory/wire_local/unwire_local
- **10 new 0.5 API methods** — `member_status`, `force_cancel_member`, `spawn_helper`, `fork_helper`, `attach_existing_session`, `cancel_flow`, `flow_status`, `collect_completed`, `member_current_session_id`, `member_session_ref`
- **Console frontend** — conversation transcript with SSE streaming (session-ID correlated), history replay from event log, optimistic entries with rollback
- **Python SDK** — all new RPC methods on `MobHandle`, `wire_cross_mob`/`send_cross_mob`/`peer_info`/`wire_local`/`unwire_local`, `wait_one`/`wait_all` polling, new types (`RichMemberSnapshot`, `HelperResult`, `MemberSessionRef`, `CrossMobContactEntry`)
- **Build isolation** — `scripts/repo-cargo` wrapper isolating `CARGO_HOME`/`CARGO_TARGET_DIR` per repo/worktree, `shasum`/`sha256sum` cross-platform fallback
- Contact directory loaded from `config/contacts.toml` via `ConventionalPaths` in gateway startup
- `EventLogStore` exposed via `event_log_store()` for console RPC event queries
- `parse_helper_options` shared as `pub(crate)` between main RPC and console RPC
- 176-line `test_rpc_method_names.py` covering all new SDK methods

### Changed

- **Meerkat 0.5.0 migration** — all deps bumped to 0.5.0, `MemberHandle.send()` replaces `send_message()`, `PeerTarget::External` for cross-mob wiring, subagents folded into builtins
- `MobBootstrapSpec::ephemeral`/`persistent` now enable `builtins`/`shell`/`memory` (matching gateway factories)
- `console_json_router_with_runtime` accepts `contact_directory` and `event_log` params
- Cross-mob wire/unwire/send capabilities gated on `has_inproc_contacts() && has_peer_mob_handles()`
- `rpc::mob_methods` module visibility changed to `pub(crate)`
- Gateway binary renamed from `phase0b_rpc_gateway` to `mobkit_gateway`
- Console frontend embedded bundle must be rebuilt and synced after source changes

### Fixed

- `wire_cross_mob` rolls back local side if remote wire fails
- Console RPC 401/404 exits use proper JSON-RPC format (not bare `{"error": ...}`)
- Console `mobkit/capabilities` correctly reports `is_authenticated = true` after auth gate
- Console `mobkit/status` returns `loaded_modules: []` (not member IDs)
- Console `ensure_member` rejects malformed `resume_session_id` and `additional_instructions`
- `sendInteraction` opens SSE stream before send to avoid missing fast completions
- Stream failure after successful send no longer rolls back the user message
- History query filters out agent-kind events (no payload in `UnifiedEvent::Agent`)
- History visit-level cache prevents transcript duplication on agent re-selection

## [0.4.13] - 2026-03-16

### Added

- Full multimodal `send_message` — `ContentInput` (text + images) flows end-to-end through the mob pipeline to the agent session
- `mobkit/send_message` RPC accepts `"content"` field with multimodal blocks alongside existing `"message"` string
- Python SDK `MobHandle.send(content=[...])` for multimodal delivery
- `mobkit/models/catalog` enriched with per-model `profile` including `vision` and `image_tool_results` capability flags
- Python SDK `CatalogEntry` gains `vision` and `image_tool_results` fields
- `MobBootstrapSpec::persistent()` convenience constructor for `PersistentSessionService` with session store forwarding
- `MobBootstrapSpec::ephemeral()` convenience constructor with optional `AgentSessionStore` override

### Changed

- Bump all meerkat crate dependencies to 0.4.13
- MCP `call_tool` adapted for `Vec<ContentBlock>` return type (with lossy-serialization warning for non-text blocks)
- `ToolResult.content` updated from `String` to `Vec<ContentBlock>`
- `RealMobRuntime::send_message` and `UnifiedRuntime::send_message` now accept `impl Into<ContentInput>`

### Fixed

- `FactoryAgentBuilder` session store forwarding — custom stores (e.g. BigQuery) are now passed through to `AgentFactory`, eliminating unnecessary JSONL fallback, redb lock contention, and duplicate persistence

## [0.4.11] - 2026-03-15

### Changed

- Bump all meerkat crate dependencies to 0.4.11
- Map `SessionError::Unsupported` to HTTP 422 in SSE and interaction endpoints
- `/interactions/stream` is now observe-only (message sending stays in `mobkit/send_message` RPC)
- `NotExternallyAddressable` errors return 403 instead of 500
- Renamed all `phase*` files to semantic names (34 files: binaries, tests, docs)
- `validate_phase0_governance_contracts` deprecated in favor of `validate_governance_contracts`

### Added

- `mobkit/models/catalog` RPC method in both standard and unified dispatchers
- Python SDK: `CatalogEntry`, `ProviderDefaults`, `ModelsCatalogResult` types
- Python SDK: `models_catalog()` on sync/async typed clients and `MobHandle`
- `InteractionComplete`/`InteractionFailed` as terminal SSE events
- Console live snapshot derives `loaded_modules` from discovered agents

### Fixed

- Console live snapshot `loaded_modules` was hard-coded to empty
- `phase_g` test: corrected `repo_root()` path, `.js` → `.cjs` refs, NVM PATH resolution

## [0.4.8] - 2026-03-13

First public release. Version aligned with Meerkat v0.4.8.

### Added

- Rust crate `meerkat-mobkit` published to crates.io
- Python SDK `meerkat-mobkit` published to PyPI
- TypeScript SDK `@rkat/mobkit-sdk` published to npm (full Python SDK parity)
- Gateway binary builds for 5 platforms (linux x86/arm, macOS x86/arm, Windows)
- CI/CD pipeline (GitHub Actions: fmt, clippy, test, audit, release)
- Release workflow with automated registry publishing
- Comprehensive clippy lint config (pedantic + deny unwrap/expect/panic)
- Pre-commit hooks (fmt on commit, gitleaks + clippy + tests on push)
- cargo-deny security auditing
- Version parity scripts across Rust, Python, and TypeScript
- Documentation site with architecture overview, quickstart, API reference
- MobKit logo and architecture diagram

### Changed

- Crate renamed from `meerkat-mobkit-core` to `meerkat-mobkit`
- Crate layout flattened (`crates/meerkat-mobkit-core/` → `meerkat-mobkit/`)
- TypeScript SDK renamed from `@meerkat/mobkit-sdk` to `@rkat/mobkit-sdk`
- Edition upgraded to 2024, rust-version to 1.94.0
- Meerkat dependencies bumped to 0.4.8 (resolved from crates.io)
- `spawn_many` now runs concurrently via `futures::try_join_all`
- RPC mob handlers extracted to `rpc/mob_methods.rs`
- Event log type aliases (`EventLogError`, `EventFilter`)
- Python SDK: `ensure_member()` and `find_members()` return typed `MemberSnapshot`
- Python SDK: `send()` returns `SendMessageResult` with `session_id`

## [0.4.6] - 2026-03-11

Initial internal release. Version aligned with Meerkat v0.4.6.

### Added

- Rust core orchestration engine
  - Unified runtime with module loading, mob lifecycle, and RPC gateway
  - Roster API: list, get, retire, and respawn mob members
  - Routing engine with wildcard matching and retry policies
  - Delivery subsystem with history tracking
  - Gating framework for risk-tiered action approval
  - Memory stores (knowledge graph, vector, timeline, todo, top-of-mind)
  - Session persistence with BigQuery adapter
  - Scheduling engine with cron and interval evaluation
  - Persistent operational event log
  - SSE event streaming for agent and mob observation
  - JWT/JWKS authentication with OIDC discovery
  - Admin console REST API
- Python SDK
  - Builder pattern for runtime configuration
  - Typed `MobHandle` with 30+ methods covering all RPC operations
  - Typed result models for all API responses
  - SSE bridge for real-time event streaming
  - ASGI app for serving the runtime over HTTP
  - Session agent builder protocol for callback-driven agents
  - Error event hooks for operational alerting
- Admin console (React)
- Mintlify documentation site
