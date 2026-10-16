---
name: meerkat-meta
description: "Meerkat Meta (meerkat-meta) architecture, SDK patterns, console/runtime surfaces, gateways, release workflow, and Rust core internals. Use this skill whenever working on the meerkat-meta repository: adding SDK features (PyPI meerkat-meta, npm @rkat/meta-sdk), writing tests, debugging console/RPC/SSE behavior, running or changing the meta_gateway, rpc_gateway or meta_repair binaries, updating Meerkat dependencies, understanding Meta's relationship to Meerkat, releasing Meta, or authoring Meta examples."
---

# Meerkat Meta

Meta is the companion composition and product layer around the Meerkat multi-agent runtime. Meerkat owns agent and mob construction, member identity and lifecycle, spawning, wiring, comms, sessions, tools, and the core agent loop. Meta composes that authority with module supervision, routing and delivery, identity continuity bridges, REST/JSON-RPC/SSE gateways, console projections, operational policy, SDKs, and examples.

Apps own desired roster and product policy. Meerkat machines admit member and session transitions. Meta translates app intent into those authorities, supervises its own operational modules, and owns product-facing projection, streaming, inspection, and console composition.

## Current Baseline

Repository: `lukacf/meerkat-meta`. One workspace version covers every artifact:

| Artifact | Name |
| --- | --- |
| Rust crate (library `meerkat_meta`) | `meerkat-meta` |
| Rust crate for opaque extension documents (the `meerkat-meta` feature `extension-state`) | `meta-extension-state` |
| Python SDK (import `meerkat_meta`) | PyPI `meerkat-meta` |
| TypeScript SDK | npm `@rkat/meta-sdk` |
| Console/admin gateway | `meta_gateway` (release archive `meta-gateway`) |
| SDK stdin JSON-RPC gateway | `rpc_gateway` (release archive `meta-rpc-gateway`) |
| Operator repair of member transcripts | `meta_repair` (release archive `meta-repair`) |

`crates/meta-store-conformance` is a workspace-only conformance crate
(`publish = false`). `meta_flow_editor` is a development binary that CI builds;
it is not released.

The authoritative Meta release line is `[workspace.package].version` in the
root `Cargo.toml`; the 0.9 line pairs with Meerkat 0.9. `make verify-version-parity`
checks the Python and TypeScript SDK package versions, the TypeScript lockfile's
root versions, the root Bazel module, the primary `crates/meerkat-meta/BUILD.bazel`
version fields, and canonical Rust installation snippets. Separately, run
`node scripts/generate-bazel-rust-builds.mjs --check` to check generated
per-crate BUILD freshness.

For the current Meerkat dependency family, read the exact requirements in
`crates/meerkat-meta/Cargo.toml` and `crates/meta-store-conformance/Cargo.toml`, including
dev-dependencies, and the resolved versions in `Cargo.lock`. The family is
exact-pinned (`=X.Y.Z`). Verify these before release or dependency work rather
than relying on a duplicated version snapshot.

There should be no vendored `meerkat-comms` patch in this repo. Durable image forwarding belongs upstream in Meerkat; do not restore old vendoring.

Docs: Mintlify sources live in `docs/` (`docs/docs.json`). The `Publish docs`
workflow (`.github/workflows/publish-docs.yml`) asks the Meerkat docs site to
mirror them on every push to `main` that touches `docs/`; releases do not
publish documentation. The mirror is served under `https://docs.rkat.ai/mobkit/`
(the site path keeps the predecessor name), and the Meerkat repository's
`docs/mobkit/_source.json` records the mirrored Meta commit.

## 0.9 Rename And Upgrade Notes

0.9 renamed the product from MobKit to Meta as a clean break with no aliases;
historical CHANGELOG entries keep the old name. The operator checklist for an
upgrade (adoption, refused and newly honoured `mob.toml` keys, ignored keys,
empty access and gating paths, each with its exact error text) is
`docs/guides/upgrading-from-mobkit.mdx`. Current names:

- crate, Python distribution and repository `meerkat-meta`; Rust library and
  Python import `meerkat_meta`; npm `@rkat/meta-sdk`;
- `Meta`-prefixed public types (for example `MetaConfig`, `MetaBuilder`,
  `MetaStorageLayout`, `MetaTypedClient`), JSON-RPC methods and notifications
  under `meta/`, HTTP routes under `/meta/...`;
- `META_` environment variables. The gateways and the flow editor refuse to
  start while a variable the predecessor read is set;
- detached-job callbacks on Meerkat's `meta/jobs/*` methods, so Meta 0.9 needs
  Meerkat 0.9.

Existing installations carry over only through one explicit offline step.
Startup refuses typed (`StorageLayoutError::PredecessorState`,
`PredecessorGatewayHome`, `AdoptionIncomplete`, ...) instead of starting empty
beside predecessor state. Stop every process that uses the installation, then:

```bash
meta_gateway storage-adopt-predecessor --state-dir <dir>            # dry run, read-only
meta_gateway storage-adopt-predecessor --state-dir <dir> --apply    # optional --gateway-home / --predecessor-home
```

`--state-dir` is exactly the directory the refusal names (the
`persistent_state` of `rpc_gateway` and the SDK builders, or the store
directory of `meta_gateway`). Apply writes a `meta-adoption.json` journal and
resumes from it. An adopted installation keeps the meerkat-level realm id
`mobkit`; a fresh one uses `meta` (`DEFAULT_MEERKAT_LEVEL_REALM_ID`).

Other 0.9 contract changes to keep in mind:

- A mob stays publicly owned while the `UnifiedRuntime` or any handle, runtime
  or identity view exported from it lives. Dropping the last one without
  `shutdown()` hands the runtime to a retained teardown that closes admission
  once the mob is idle. `identity_runtime()`, `identity_first_context()` and
  `session_bridge()` return owned views (`PublicIdentityRuntime`,
  `PublicIdentityFirstContext`, `PublicSessionBridge`) that carry that owner.
- The memory backend config keeps only `MemoryBackendConfig::LocalJson`
  (`kind = "local_json"`); any other kind is refused, never reinterpreted.
- `MobDefinition::from_toml` warns about and ignores keys a profile does not
  define, such as `tools.mob_tasks` in configs written for earlier releases.
  The typed parse drops them before Meta records the composition on create or
  compares it on resume, so dropping (or adding) such a key never makes a
  persistent store `divergent`; a divergence refusal names only accepted
  fields. Pinned by `dropping_an_ignored_profile_key_resumes_the_store` and its
  siblings in `tests/mob_storage_persistence.rs`.
- Meerkat 0.9 refuses a profile `system_prompt`, `prompt` or `instructions`
  key, which MobKit 0.8.31 and earlier dropped silently. Every Meta mob-TOML
  ingress (`rpc_gateway` `mob_config`, `meta_gateway` `config/mob.toml`,
  `UnifiedRuntimeBuilder`'s TOML path) formats parse failures through
  `gateway_composition::describe_mob_definition_error`, which appends that
  removing the key keeps the old behaviour and shows the `[skills.<id>]`
  remedy. The other keys 0.9 newly refuses or newly honours are listed in the
  upgrade guide.
- An empty access or gating path no longer switches the feature off (the
  predecessor SDKs dropped it by truthiness). `rpc_gateway` refuses
  `runtime_options.access_config_path` or `gating_config_path` set to `""`, and
  the SDK builders' `accessControl("")` / `access_control("")` / `gating("")`
  throw. To keep the feature off, omit the key or send `null`; in the SDKs, do
  not call the builder method and keep nothing at `config/access.toml` /
  `config/gating.toml` under the working directory (conventional discovery).
- `AccessController::load_or_default` is gone: `AccessController::load` reads
  and validates an explicitly configured access file, and a missing or invalid
  file is a typed `AccessConfigError`.

## Repo Map

| Path | Purpose |
| --- | --- |
| `crates/meerkat-meta/` | Rust crate, runtime, HTTP, JSON-RPC, gateway binaries, and embedded console bundle. |
| `crates/meerkat-meta/src/lib.rs` | Public exports and the fastest map of current modules. Read this before trusting notes. |
| `crates/meerkat-meta/src/bin/` | `meta_gateway`, `rpc_gateway`, `meta_repair`, the `meta_flow_editor` dev binary, and internal check/eval binaries. |
| `crates/meerkat-meta/src/mob_handle_runtime.rs` | `MobRuntime` / `RealMobRuntime`, mob bootstrap, member operations, session hooks, identity bridge points. |
| `crates/meerkat-meta/src/runtime/` | Runtime subsystems: bootstrap, routing/delivery, gating, scheduling, memory, metadata, event transport, cross-mob control, session stores, supervisor. |
| `crates/meerkat-meta/src/unified_runtime/` | Unified runtime builder, discovery, lifecycle, mob ops, event log, console/mob event projection, edge reconciliation, cross-mob wiring. |
| `crates/meerkat-meta/src/console_aggregator/` | Console timeline/log aggregation, replay, visibility policy, SQL/in-memory stores, send state machine. |
| `crates/meerkat-meta/src/http_console.rs` | Embedded console, REST console JSON, JSON-RPC, multipart upload/send, timeline stream, blob serving. |
| `crates/meerkat-meta/src/http_sse.rs` | Agent events, mob events, structural mob-events SSE. |
| `crates/meerkat-meta/src/rpc.rs` and `src/rpc/` | JSON-RPC dispatcher and method groups for console ingress, mob ops/events, routing/delivery, gating, memory, scheduling, session store, subscriptions. |
| `crates/meerkat-meta/src/identity_first/` | Identity-first continuity, local store/lease, bridge, orchestrator, gateway bridges, contracts. |
| `crates/meerkat-meta/src/contact_directory.rs`, `src/auth/peer_keys.rs` | Cross-mob contacts and signed peer-key handling. |
| `crates/meta-extension-state/` | Provider-owned extension documents with atomic access control and retries (`ExtensionDocumentStore`). |
| `crates/meta-store-conformance/` | Workspace-only storage conformance harness for Meta store backends (per-trait chapters, extending Meerkat's `meerkat-store-conformance`). |
| `console/` | React console app source, tests, Vite/build entrypoints, generated `console/dist`. |
| `packages/console-core/` | Reusable console contracts, conversation/activity/sidebar/dock logic, formatting, rich content. |
| `packages/console-components/` | Reusable console UI components and CSS tokens/themes. |
| `crates/meerkat-meta/console-dist/` | Embedded console bundle copied from `console/dist` by `npm run build`, plus the MCP Apps sandbox proxy copied from its console sources (written back out by the gateways' `--write-mcp-apps-sandbox <dir>`). |
| `flow-editor/` | Flow editor app source. `npm --prefix flow-editor run build` also writes the bundle that `crates/meerkat-meta/flow-editor-dist/` embeds; `flow_editor_router` serves it at `/flow-editor` (the `meta_flow_editor` dev binary). |
| `sdk/python/` | Python SDK, gateway subprocess transport, identity-first models/providers, tests. |
| `sdk/typescript/` | TypeScript SDK, fetch/SSE client, runtime builders, tests. |
| `docs/` | Mintlify documentation sources (mirrored to the Meerkat docs site). |
| `examples/001-incident-command-center-pack/` | Self-contained Incident Commander example pack. Keep example-specific source here. |
| `examples/002-foresight-studio-pack/` | TypeScript SDK example with identity-first providers, callback tools, and a heavily customized console. |
| `scripts/repo-cargo` | Preferred Cargo wrapper for this repo. |

Do not put example-specific support code under `crates/meerkat-meta/src`. Same repo is fine; the core crate source tree is not.

## Console Surfaces

Current HTTP routes to remember:

- `GET /console`, `/console/assets/console-app.js`, `/console/assets/console-app.css` serve the embedded console.
- `GET /console/experience`, `/console/modules`, `/console/identities`, `/console/timeline` serve REST JSON.
- `GET /console/timeline/stream` streams projected console timeline frames.
- `GET /console/identity/{identity}/stream` is an identity-filtered timeline stream convenience route.
- `POST /console/send` submits console sends through the aggregator.
- `POST /console/rpc` is JSON-RPC.
- `POST /console/rpc/multipart` supports `meta/console/send` with image uploads and `meta/blob/upload`.
- `GET /blobs/{blob_id}` resolves console blob/image content.
- `GET /agents/{agent_id}/events`, `/mob/events`, and `/meta/mob_events/stream` are SSE observation surfaces.

Console projection is contract-boundary code. Prefer changing `console_aggregator`, `http_console`, `console/src/lib/adapters.ts`, or `packages/console-core` rather than leaking raw runtime text into React components.

Agent-tool spawns (`mob_spawn_member`, `delegate`, `spawn_member`/`spawn_many_members`) project into the console through `crates/meerkat-meta/src/console_spawn.rs`: the unified runtime installs a `ConsoleSpawnSink` on the mob runtime at bootstrap, and the `AutoWireParentMobToolDispatcher` (in `mob_handle_runtime.rs`) registers each spawned member's identity + labels (`spawned_by`, `via_tool`, spawn labels) and appends the `initial_message` as a deduped `spawn-kickoff:*` `user_input` console event. Live runtime events resolve to the same chat identity. No console sink → spawn behavior unchanged.

## Access Control (ABAC)

Optional attribute-based access control for console and SSE surfaces lives in `crates/meerkat-meta/src/access/` (model, engine, controller). Deny-by-default with deny-overrides when enabled; `admins` bypass rules; a disabled config (or absent controller) changes nothing.

Key facts:

- Opt-in via `config/access.toml` (meta_gateway conventional discovery), `UnifiedRuntimeBuilder::access_control_file`, `runtime.set_access_controller(...)`, or rpc_gateway `runtime_options.access_config_path` (TS SDK: `.accessControl(path)`, Python SDK: `.access_control(path)`; both auto-discover `config/access.toml`). Discovery skips `config/access.toml` only on NotFound/ENOENT (nothing there, including a missing or dangling parent); a directory, a dangling final link, ENOTDIR and other lookup errors go to the strict loader, which refuses startup when it cannot open, read, parse or validate the file (it does not check the file type, so a FIFO blocks startup instead).
- The closed action vocabulary lives in `access/model.rs::ACCESS_ACTIONS`. It covers agent view/send/lifecycle, scoped memory, gating, WorkGraph, mob observation, topology, mobpack author/deploy, runtime administration, and access administration. Do not maintain a shorter shadow list in surface code.
- Enforcement seams: experience filtering + affordance intersection in `runtime/console_ingress.rs` (`handle_console_rest_json_route_with_snapshot_and_access`), RPC gating + result filtering + `meta/access/*` admin methods in `http_console.rs` (`console_rpc_access_requirement`, `handle_access_admin_rpc`), SSE gating in `http_sse.rs` (`sse_access_context`).
- `mob.observe` gates the whole-mob event surfaces (`/mob/events`, `/meta/mob_events/stream`, `meta/mob_events/query`/`subscribe`) but does NOT override per-agent `agent.view`: events are filtered per source/`agent_identity`, mob-level (unattributed) events flow on `mob.observe` alone. "Observe all" = `mob.observe` + `agent.view` on `*`.
- Spawn-lineage inheritance: agent-spawned members carry a `spawned_by` console label (recorded by `src/console_spawn.rs`); agent checks walk the member plus its spawn ancestors (`evaluate_access_lineage`, depth 8, cycle-safe), so rules matching the parent also match its spawned members, with deny-overrides preserved across the chain. Lineage is runtime-derived ONLY: `spawned_by`/`via_tool` claims in caller-controlled labels (spawn specs, roster) are stripped at every enforcement seam (`sanitize_unverified_lineage_labels`) - only the in-memory spawn registry may assert them.
- `/blobs/{id}` is a capability surface (content-addressed `sha256:` ids, deduped across agents, no per-agent ACL) — authn + hash-unguessability, not an `agent.view` boundary.
- Live config: successful `AccessController` config mutations validate and advance the revision. They persist TOML only when a persistence path is configured through `load` (an existing, valid file; a missing or invalid one is an error) or `with_persist_path`; `AccessController::new` and `disabled` remain in-memory unless a path is attached. A configuration that names no admin grants administration to nobody. Per-request `AccessView` snapshots and cached agent label/role attributes from roster projections let label selectors work on identity-only surfaces.
- Console UI: `console/src/panels/AccessPanel.tsx`, nav kind `access` appears only when `experience.access.can_administer`.
- Denials: JSON-RPC `-32030` with `data.kind = "access_denied"`, HTTP 403 on REST/SSE.
- Anti-lockout invariant: enabling requires non-empty `admins`; validation lives in `access/model.rs`.
- Tests: `crates/meerkat-meta/tests/access_control.rs`, unit tests in `src/access/*`, console helper tests in `console/src/panels/AccessPanel.test.ts`. Docs: `docs/concepts/access-control.mdx`.

## Console Configuration

The stock console can be shaped by `config/console.toml` in the conventional workspace layout. The runtime projects the parsed config through `GET /console/experience` as `console_config`, so embedders and the bundled React console share the same contract.

Key files:

- `crates/meerkat-meta/src/console_config.rs` defines the TOML-backed config schema, normalization, realm overlays, and loader helpers.
- `crates/meerkat-meta/src/config_convention.rs` discovers `config/console.toml`.
- `crates/meerkat-meta/src/bin/meta_gateway.rs` loads the conventional config and applies the TUX init `realm` overlay.
- `crates/meerkat-meta/src/bin/rpc_gateway.rs` accepts `runtime_options.console_config_path`.
- `sdk/typescript/src/builder.ts` exposes `Meta.builder().consoleConfig("config/console.toml")` for RPC-gateway embedders, plus `.consoleAuthRequired(false)` for explicit local unauthenticated console demos; the Python builder has `console_config(path)`.
- `crates/meerkat-meta/src/runtime/console_ingress.rs` projects `console_config` and uses `title` for the base panel title.
- `console/src/ConsoleApp.tsx`, `console/src/panels/Topbar.tsx`, and `console/src/panels/Sidebar.tsx` consume the config.

Current shape:

```toml
title = "Ops"

[brand]
label = "Ops Console"
logo_url = "/assets/ops.svg"
logo_alt = "Ops"

[appearance]
default_theme = "dark"
default_variant = "graphite"

[environment]
label = "prod"

[layout]
initial_preset = "two_columns"
initial_control = "roster"
initial_agent = "identity:ops-lead"
sidebar_collapsed = false

[rail]
visible = true
collapsed = false
active_preset_id = "critical"
empty_text = "No meaningful signals yet."

[[rail.filter_presets]]
id = "critical"
label = "Critical"
alert_levels = ["critical"]

[sidebar]
visible_controls = ["topology", "roster", "logs", "health"]
hidden_controls = []

[[sidebar.buttons]]
id = "ops-board"
label = "Ops Board"
href = "https://example.test/ops"
target = "_blank"
icon_name = "external-link"

[agent_list]
group_by = ["labels.console_group", "labels.group", "role"]
subgroup_by = ["labels.org", "labels.realm"]
section_order = ["Personal", "Initiatives", "Internal"]
fallback_group = "Agents"
fallback_subgroup = "Other"
default_pinned_agent_ids = ["identity:ops-lead"]
collapse_single_subgroup = true

[[agent_list.badges]]
id = "org"
label = "Org"
field = "labels.org"
tone = "info"

[[agent_list.sections]]
name = "Initiatives"
collapsed = false
empty_title = "No initiatives"
empty_text = "Create one in Linear."

[actions]
inspect_label = "Profile"
chat_label = "Open chat"
send_label = "Send to agent"
respawn_label = "Respawn"
retire_label = "Retire"
reset_label = "Reset"
show_reset = false

[realms.ops]
title = "Ops"

[realms.ops.agent_list]
subgroup_by = ["labels.org"]
```

Agent grouping selectors and badge fields support `labels.<key>`, `label:<key>`, raw label keys, and direct fields such as `group`, `subgroup`, `role`, `kind`, `identity`, `member_id`, and `agent_id`. For configured `group_by` and `subgroup_by` selectors, the detected ancestor chain is searched from the highest ancestor down to the child; the first available configured value wins. This keeps spawned/delegate rows with their host even when their own labels differ. Child values apply only when no earlier ancestor supplies a configured value; this inheritance rule does not apply to badges. Prefer mob member labels such as `console_group = "Initiatives"` and `org = "Payments"` for domain-specific grouping.

When extending console customization, keep it view-level. The config should decide presentation, ordering, visibility, labels, defaults, and links; it should not decide runtime authorization, routing, or whether an agent exists. Be cautious with arbitrary CSS/theme color injection until there is a token contract.

Current console UX decisions to preserve:

- The main sidebar is navigation. Clicking an agent opens chat; no hover-driven chat/info toggle.
- Roster/details belong in roster-style surfaces, not in a redundant Inspect mode.
- Approvals owns pending, auto, audit, and policies. Do not reintroduce a separate `Gates` nav item.
- Signals should show meaningful user requests, peer-to-peer communication, assistant replies, image events, and relevant tool activity. Group repeated events from one interaction when useful.
- Roster should list actual wired peers, not only counts.
- Suppress scaffold/noise prompts, raw `checksum_token`, raw tool metadata, and transport envelopes from user-facing chat.
- Preserve user messages and timeline order. Ordering bugs usually come from merging history, live frames, and optimistic entries with partial timestamps.
- Render `assistant_image` and `assistant_image_appended` as images and dedupe by blob/reference.

## Cross-Mob And Comms

Comms delivers work; observation chooses scope. Do not design APIs that mix send and observe into one operation.

Cross-mob support includes in-process wiring and signed TCP/UDS contacts. Real transports require real Ed25519 peer pubkeys. In-process descriptors can stay unsigned because the in-process router authorizes through the identity map. The signing and digest contexts changed with the 0.9 rename, so a 0.9 gateway cannot exchange signed control requests or host facts with a pre-0.9 gateway.

Useful files:

- `crates/meerkat-meta/src/unified_runtime/cross_mob.rs`
- `crates/meerkat-meta/src/runtime/cross_mob_control.rs`
- `crates/meerkat-meta/src/runtime/cross_mob_remote.rs`
- `crates/meerkat-meta/src/contact_directory.rs`
- `crates/meerkat-meta/src/auth/peer_keys.rs`
- `crates/meerkat-meta/tests/cross_mob_signed.rs`
- `crates/meerkat-meta/tests/cross_mob_tcp.rs`
- `crates/meerkat-meta/tests/cross_mob_uds.rs`

Image forwarding gotcha: the old failure was `image_ref_unavailable: current_turn image 0 did not resolve to a current-turn image`. Same-turn generate-and-send worked because the image was still in current-turn scope. Durable forwarding should use current upstream Meerkat behavior, not a vendored patch.

## Identity-First Continuity

**Doctrine (DECIDED 2026-07-06, `docs/design/identity-first-doctrine.md`): Meta is dual-plane.** Durable members live on the identity plane (`IdentityRuntime`: continuity records, lease-fenced embodiment, tolerant disposal, reconcile from a roster). Ephemeral workers live on the mob plane (`MobHandle`: `spawn`/`mob_spawn_member`/`delegate`, idle-retire reaping). The mob plane is NOT legacy - it is the worker plane and the substrate the identity bridge is built on. What is wrong is building DURABLE populations on the mob plane (member-per-user, long-lived coordinators via `ensure_member`): use an identity roster instead. Never route worker churn through identities (per-worker continuity records are pure overhead - production-scale deployments pay for every dead identity at reconcile).

Identity-first makes stable `AgentIdentity` strings the control-plane key and treats runtime member IDs as generated bindings. Prefer identity-scoped APIs for durable agents and member-scoped APIs for ephemeral workers at the mob layer.

Gateway note: on an identity-first gateway (`meta_gateway` init
`identity_first: true`, or any runtime with the `console_identity_roster` slot
set), console member RPCs default to identity semantics. `ensure_member`
upserts the `MutableRosterProvider` and reconciles through `restore_flow`,
`retire_member` is an identity retire, and `respawn_member` is an identity
reset. Callers must select `plane: "worker"` explicitly for ephemeral
mob-plane workers.

Key files:

- `crates/meerkat-meta/src/identity_first/types.rs`
- `crates/meerkat-meta/src/identity_first/contracts.rs`
- `crates/meerkat-meta/src/identity_first/runtime.rs`
- `crates/meerkat-meta/src/identity_first/orchestrator.rs`
- `crates/meerkat-meta/src/identity_first/bridge.rs`
- `crates/meerkat-meta/src/identity_first/gateway_bridges.rs`
- `sdk/python/meerkat_meta/identity_first_models.py`
- `sdk/python/meerkat_meta/identity_first_providers.py`
- `sdk/typescript/src/runtime.ts`
- `sdk/typescript/src/models.ts`

Avoid manual runtime ID extraction for ordinary identity-scoped work. Do not assume a continuity record means an active mob member; retired/stale records can exist.

## Incident Commander Example

The Incident Commander example is self-contained:

- `examples/001-incident-command-center-pack/incident_command_center.rs`
- `examples/001-incident-command-center-pack/server.rs`
- `examples/001-incident-command-center-pack/scenario.yaml`
- `examples/001-incident-command-center-pack/README.md`
- `examples/001-incident-command-center-pack/ts_smoke.ts`
- `examples/001-incident-command-center-pack/python_smoke.py`

Cargo example name: `incident_command_center`, declared in `crates/meerkat-meta/Cargo.toml` with path `../../examples/001-incident-command-center-pack/server.rs`.

Useful commands:

```bash
./scripts/repo-cargo check -p meerkat-meta --example incident_command_center
./scripts/repo-cargo test -p meerkat-meta --test incident_command_center_pack
./scripts/repo-cargo run -p meerkat-meta --example incident_command_center
```

Default listen address: `127.0.0.1:63210` (the scenario's `listen_addr`). Override with `INCIDENT_COMMAND_CENTER_LISTEN_ADDR`.

## Development Workflow

Run these commands from the repository root with the existing development
dependencies installed.

For frontend/console changes:

```bash
npm --prefix console run phase0:types --silent
npm --prefix console run phase1:targets --silent
npm --prefix console run build --silent
```

`npm --prefix console run build` updates `console/dist` and `crates/meerkat-meta/console-dist`. Include generated bundle changes when the Rust server should serve the new console.

For Rust changes:

```bash
./scripts/repo-cargo fmt --check
./scripts/repo-cargo check -p meerkat-meta
./scripts/repo-cargo test -p meerkat-meta
```

For SDK changes:

```bash
(cd sdk/python && python3 -m pytest)
npm --prefix sdk/typescript run validate
```

For UI behavior, use the Browser plugin against a live console after code changes.

## Release Workflow

Inspect a release first with the repository wrapper:

```bash
./scripts/repo-cargo release <version>
```

`--execute` creates the release commit and normally tags and pushes it. The
release hook moves Cargo, Python, TypeScript, Bazel-module, generated BUILD,
and install-documentation versions together. For a PR-reviewed cut, prepare
without a tag or push, verify generated Rust BUILD freshness, and run the release
dry-run before opening the PR:

```bash
./scripts/repo-cargo release <version> --execute --no-tag --no-push
node scripts/generate-bazel-rust-builds.mjs --check
make release-dry-run
```

With the repository prerequisites installed, the release hook regenerates the
workspace per-crate BUILD files and stages all tracked `BUILD.bazel` files,
including `crates/meta-store-conformance/BUILD.bazel`. Keep the generator `--check`
command as verification. Tag
the exact merged main commit only after the release PR and push-to-main CI on
that exact commit are green, and the candidate has been accepted as described
below.

When updating Meerkat dependencies, update the family's exact requirements in
both `crates/meerkat-meta/Cargo.toml` and `crates/meta-store-conformance/Cargo.toml`,
including applicable dev-dependencies, before running
`./scripts/repo-cargo update -p ...` for the affected packages. Verify that
`Cargo.lock` resolves the intended coherent upstream family, then run the
appropriate workspace checks through `./scripts/repo-cargo` or Make.

**Release protocol: tags validate only; they no longer build or publish.**
After merging the final release-version/dependency commit, use
`make release-candidate SOURCE_SHA=<full-sha> TIER=core` and
`make release-candidate SOURCE_SHA=<full-sha> TIER=trailing`. The dispatch must
snapshot that same main SHA. Each run builds/signs/packages one tier (core:
Linux x86_64 and aarch64, macOS arm64; trailing: macOS x86_64, Windows) beside
the push-to-main CI run of that SHA, refuses at collection unless that CI run is
already green, and emits an immutable run/attempt/tier/artifact/digest selection
backed by signed provenance and outer/inner executable hashes. Save the
selections outside the repo and obtain consumer acceptance against those bytes.

The release owner then tags the same source as `vX.Y.Z` and invokes
`make release-promote RELEASE_TAG=vX.Y.Z ARTIFACT_SELECTION=<core json-file>`.
Promotion accepts only the core selection and verifies and publishes the
original bytes without rebuilding or signing again. The trailing tier is added
afterwards, add-only, with
`make release-assets-recover RELEASE_TAG=vX.Y.Z ARTIFACT_SELECTION=<trailing json-file>`. Assets precede the Python and TypeScript SDKs, then the crates
(`meta-extension-state`, then `meerkat-meta`) and an exact registry readback.
A failed SDK registry upload does not stop the crates; the readback still fails
the run while any registry lacks the exact version.
Releases do not publish documentation (see Current Baseline).
`REGISTRY_DRY_RUN=true` suppresses all publication;
`PUBLISH_RELEASE_PACKAGES=false` selects asset-only promotion.

Use `make release-registry-retry RELEASE_TAG=vX.Y.Z` for registry-only retry
(optional `REGISTRY_DRY_RUN=true`), and `make release-assets-recover
RELEASE_TAG=vX.Y.Z ARTIFACT_SELECTION=<json-file>` to add missing validated
original assets. Never replace existing hashes, rebuild expired candidates,
re-push historical tags, or rerun historical release workflows. Recovery must
dispatch the current workflow on main with the old tag as data. Missing originals
or conflicting hashes fail closed. See CONTRIBUTING.md for receipt/recovery
contracts and the full event/mode matrix.

Verify all registries directly:

```bash
./scripts/repo-cargo search meerkat-meta --limit 1
./scripts/repo-cargo search meta-extension-state --limit 1
npm view @rkat/meta-sdk version
python -m pip index versions meerkat-meta
```

Registry versions are immutable. If a version already published from the wrong commit, roll forward to the next patch version.

## Tests To Remember

Rust:

- `crates/meerkat-meta/tests/unified_console.rs`
- `crates/meerkat-meta/tests/console_experience.rs`
- `crates/meerkat-meta/tests/console_route_auth.rs`
- `crates/meerkat-meta/tests/cross_mob_signed.rs`
- `crates/meerkat-meta/tests/cross_mob_tcp.rs`
- `crates/meerkat-meta/tests/cross_mob_uds.rs`
- `crates/meerkat-meta/tests/e2e_target_contracts.rs`
- `crates/meerkat-meta/tests/e2e_sdk_wire.rs`
- `crates/meerkat-meta/tests/identity_first_*.rs`
- `crates/meerkat-meta/tests/incident_command_center_pack.rs`
- `crates/meerkat-meta/tests/mob_events_*.rs`
- `crates/meerkat-meta/tests/routing_delivery.rs`
- `crates/meerkat-meta/tests/sse_auth_gate.rs`
- `catalogs_response_exposes_filesystem_skill_catalog` in `crates/meerkat-meta/src/mobpack.rs` pins this skill: the repo's `.claude/skills/meerkat-meta/` is the expected filesystem skill (id `meerkat-meta`). Rename both together.

Console:

- `console/src/lib/adapters.test.ts`
- `console/src/lib/network.test.ts`
- `console/src/panels/Sidebar.test.ts`
- `console/src/panels/LogsPanel.test.ts`
- `console/src/panels/SignalsRail.test.ts`
- `packages/console-core/src/*.test.ts`
- `packages/console-components/src/**/*.test.tsx`
- `console/browser-e2e.cjs`

SDK:

- `sdk/python/tests/`
- `sdk/typescript/tests/`

## Practical Rules

- Follow the code that exists now. Start with `rg` and `crates/meerkat-meta/src/lib.rs`.
- Keep examples out of core source.
- Keep signed cross-mob transport fail-closed. Missing pubkeys on real transports are errors.
- Keep raw transport, checksum, scaffold, reasoning, and tool-envelope details out of user-facing console rendering.
- Use structured parsers/contract adapters rather than ad hoc string slicing where the repo already has a boundary helper.
- After frontend changes, rebuild the embedded console bundle.
