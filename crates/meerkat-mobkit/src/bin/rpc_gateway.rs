#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::uninlined_format_args,
    clippy::collapsible_if,
    clippy::redundant_clone,
    clippy::needless_raw_string_hashes,
    clippy::single_match,
    clippy::redundant_closure_for_method_calls,
    clippy::redundant_pattern_matching,
    clippy::ignored_unit_patterns,
    clippy::clone_on_copy,
    clippy::manual_assert,
    clippy::unwrap_in_result,
    clippy::useless_vec
)]
//! Phase 0b binary — JSON-RPC gateway bridging SDK clients to the unified runtime.

/// This binary's own tracing target. Feeds
/// `gateway_composition::default_tracing_filter`, which builds the same
/// filter string this binary used to carry as its own constant: this crate's
/// own targets at INFO, dependencies at WARN. Operationally significant boot
/// phases (the one-time head-canonical conversion, continuity repair) report
/// at INFO from `meerkat_mobkit`; the old blanket "warn" default hid them,
/// and a 2026-07 production deploy was aborted when a supervisor read a
/// silent-but-working migration as a hang.
const GATEWAY_TRACING_TARGET: &str = "rpc_gateway";

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Write;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use meerkat_mobkit::contact_directory::ContactDirectory;
use meerkat_mobkit::runtime::cross_mob_control::{
    ControlAuthorizer, ControlGrantTable, ControlListenAddr,
};
use meerkat_mobkit::shutdown_signal::ShutdownSignal;
use meerkat_mobkit::unified_runtime::EventLogError;
use meerkat_mobkit::unified_runtime::types::IdentityAuthorityReleaseOutcome;
use meerkat_mobkit::unified_runtime::types::RetiredSupervisorCleanupOutcome;
use meerkat_mobkit::{
    Base64BlobStoreAdapter, BinaryBlobStore, COMPOSITION_PROVENANCE_CODE, ConsolePolicy,
    ConsoleUiConfig, DiscoverySpec, EventLogConfig, EventLogStore, EventQuery,
    InMemoryMetadataStore, LocalJsonMemoryBackendConfig, MOBKIT_CONTRACT_VERSION,
    MemoryBackendConfig, MobBootstrapOptions, MobBootstrapSpec, MobKitConfig, ModuleConfig,
    ObjectStoreBlobStore, PersistedEvent, PersistentMetadataStore, PreSpawnData, RestartPolicy,
    RuntimeDecisionState, RuntimeOptions, RuntimeRoute, STORAGE_RESOLUTION_CODE,
    SqliteConsoleLogStore, SqliteMetadataStore, UnifiedRuntime, UnifiedRuntimeShutdownReport,
    handle_mobkit_rpc_json, load_console_ui_config_from_path_for_realm,
    mob_handle_runtime::{mob_definition_may_use_image_generation, mob_definition_may_use_shell},
    start_mobkit_runtime,
};
use sha2::{Digest, Sha256};

use async_trait::async_trait;
#[cfg(test)]
use meerkat::EphemeralSessionService;
use meerkat::{
    AgentEvent, AgentFactory, Config, CreateSessionRequest, FactoryAgent, FactoryAgentBuilder,
    SessionAgentBuilder, SessionError,
};
use meerkat_core::ContentBlock;
use meerkat_core::error::{AgentError, ToolError};
use meerkat_core::ops::ToolDispatchOutcome;
use meerkat_core::types::{ToolCallView, ToolDef, ToolProvenance, ToolResult, ToolSourceKind};
use meerkat_core::{
    AgentToolDispatcher, ToolCatalogCapabilities, ToolCatalogEntry, ToolDeadlineContributor,
    ToolDeadlineOwner, ToolExecutionContract, ToolExecutionMode,
};
use meerkat_mob::{MobDefinition, MobStorage};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc, oneshot};

#[derive(Clone, Default)]
struct GatewayGatingConfig {
    action_risk_tiers: HashMap<String, String>,
}

struct GatewayRuntimeOptions {
    runtime_options: RuntimeOptions,
    /// Presence is identity-first intent. `None` preserves the classic gateway
    /// when no roster is configured; a roster still defaults to eager restore.
    identity_bootstrap_mode: Option<meerkat_mobkit::IdentityBootstrapMode>,
    /// Exclusive root for session comms identities, when the launch declares
    /// one.
    ///
    /// `meerkat::canonical_session_comms_identity_root(Some(root))` resolves
    /// `<root>/.rkat/session_comms_identity` and consults no ambient location,
    /// so a declared root cannot silently fall back to this machine's
    /// canonical keys. `None` keeps the ambient resolution, which is correct
    /// for an ordinary launch and is exactly what must NOT happen when a
    /// rehearsal replays another host's durable member endpoints: the local
    /// keys are foreign to them, and `register_seeded_member_peer` rightly
    /// refuses to rewrite a trust endpoint with a foreign key.
    session_identity_config_root: Option<PathBuf>,
    max_sessions: usize,
    routing_routes: Vec<RuntimeRoute>,
    gating: GatewayGatingConfig,
    event_log: Option<EventLogConfig>,
    decisions: Option<RuntimeDecisionState>,
    console_ui: ConsoleUiConfig,
    console_require_app_auth: Option<bool>,
    console_read_only: Option<bool>,
    console_fetch_timeout_ms: Option<u64>,
    access: Option<meerkat_mobkit::AccessController>,
    demo_llm: bool,
    /// Bind each locally hosted member to a signed loopback TCP endpoint so
    /// peer-only external members in another process can return traffic.
    member_comms_address: Option<String>,
    /// `runtime_options.contacts_toml`: the cross-mob contact directory as
    /// inline TOML (same format as `config/contacts.toml` on
    /// mobkit_gateway). Inline because SDK-driven gateways receive all
    /// launch config through init params, and cross-process tests must
    /// write a peer's bound address into the directory at spawn time.
    contacts: Option<ContactDirectory>,
    /// Scoped caller grants for the cross-mob control listener. The empty
    /// default is deny-all, never an implicit open listener.
    control_grants: ControlGrantTable,
    agent_memory: Option<GatewayAgentMemoryOptions>,
    /// WorkGraph service construction switch (default on). `false` disables
    /// the store, member tools, overlays, and the mobkit/workgraph/* RPCs.
    workgraph: GatewayWorkgraphOption,
    /// Live (realtime) transport opt-in (default off). Persistent mode only.
    live: GatewayLiveOption,
    /// DEPRECATED strict experimental GPT Live registration (private
    /// ChatGPT-brokered path). Independent from the ordinary WebSocket live
    /// option so opting in does not mount an HTTP route. Every
    /// authority-bearing value is explicit and absence keeps capability
    /// projection fail-closed. Mutually exclusive with `openai_live`.
    #[cfg(feature = "experimental-gpt-live")]
    experimental_live: Option<GatewayExperimentalLiveOption>,
    /// Public OpenAI Live (`gpt-live-1`) registration. Independent from the
    /// ordinary WebSocket live option so opting in does not mount an HTTP
    /// route. The host names the principal, realm, configured OpenAI API-key
    /// binding, and voice; absence keeps capability projection fail-closed.
    #[cfg(feature = "openai-live")]
    openai_live: Option<GatewayOpenAiLiveOption>,
    /// Authenticated HTTP voice uses the shared summary/existing-member owner.
    /// Exclusive with the independently composed stdin/legacy live owners.
    #[cfg(feature = "openai-live")]
    console_voice: Option<GatewayOpenAiLiveOption>,
    /// SDK-registered deterministic schedule targets
    /// (`runtime_options.host_runnables`): each name registers a schedule
    /// host runnable whose fire forwards over the callback bridge as
    /// `callback/schedule_fire`.
    host_runnables: Vec<meerkat::HostRunnableName>,
    /// `runtime_options.runtime_store = {"storage": "memory"}`: the explicit
    /// declaration that the runtime store is in-memory on a persistent
    /// launch (M4). Without it, a failed `runtime.sqlite` open is a startup
    /// error — the former silent `InMemoryRuntimeStore` fallback is gone.
    runtime_store_ephemeral: bool,
    /// Declared in-memory mob storage. Persistent SQLite is the default on a
    /// persistent_state launch, so this exists for launches that would rather
    /// keep an editable mob_config than durable mob state: a persistent mob
    /// storage pins the definition, because Meerkat refuses a definition that
    /// disagrees with the canonical event-log definition.
    mob_storage_ephemeral: bool,
    /// `runtime_options.declare_spec_update = {"expected_revision": N}`: an
    /// explicit operator declaration that advances the canonical mob definition
    /// to the definition this launch supplies.
    ///
    /// The door through the pinned-`mob_config` refusal. Present only on the
    /// activation that intends to move the pin - it is a declared transition, not
    /// a mode. Compare-and-swapped on `expected_revision`, so an activation that
    /// names a revision the store has moved past is refused rather than
    /// overwriting a definition the operator never saw.
    declare_spec_update: Option<u64>,
    /// `runtime_options.mob_composition = {"authority": "candidate"}`: this
    /// launch does NOT speak for the durable composition, so it neither creates
    /// the composition pin nor is refused by one.
    ///
    /// For candidate/certification boots in a candidate-then-promote pipeline
    /// against one state directory. Without it such a boot pins its own
    /// deliberately-restricted composition and the promoted boot is refused -
    /// which in one production incident cost 929 supervisor respawns and a rollback.
    composition_authority: meerkat_mobkit::mob_composition_manifest::CompositionAuthority,
    /// `runtime_options.mob_composition.candidate_definition`: `"stored"` lets
    /// a candidate boot the stored mob definition when the supplied one
    /// differs (logged, degraded health). Absent, such a candidate refuses.
    candidate_definition: meerkat_mobkit::mob_composition_manifest::CandidateDefinition,
    /// `runtime_options.compaction = {"auto_compact_threshold": 120000, ...}`:
    /// the host-level session-compaction policy for every agent this gateway
    /// builds. Absent, the gateway inherits meerkat's model-aware default
    /// (`context_window * 4 / 5` — `840_000` tokens on a million-token model,
    /// i.e. effectively "never compact"). See
    /// [`meerkat_mobkit::compaction_policy`].
    compaction: Option<meerkat_core::config::CompactionRuntimeConfig>,
    /// `runtime_options.meerkat_config_path`: the meerkat host `config.toml`
    /// every gateway-built agent starts from, loaded once at init. This is
    /// the only ingress for `[self_hosted]`, `[realm]` and `[models]`, which
    /// meerkat keeps in the host config rather than in `mob.toml`. A missing
    /// or malformed file refuses init; `None` keeps `Config::default()`.
    host_config: Option<Config>,
    /// `runtime_options.http_listen`: the HTTP listener's bind address,
    /// default `127.0.0.1:0` (loopback, ephemeral port). A non-loopback
    /// address passes `http_bind_gate` only when the console enforces app
    /// auth or `allow_remote` acknowledges the exposure.
    http_listen: std::net::SocketAddr,
    /// `runtime_options.http_public_base_url`: the base URL clients reach
    /// this gateway at through a proxy. Reported in the init result as
    /// `http_public_base_url`; never used to bind.
    http_public_base_url: Option<String>,
    /// `runtime_options.allow_remote`: explicit acknowledgement that
    /// `http_listen` exposes the listener beyond this host. Same word and
    /// rule as `--allow-remote` on `rkat-rpc --tcp`.
    allow_remote: bool,
}

/// `runtime_options.live` wire forms: `true` mounts the live WebSocket
/// transport on the gateway's HTTP listener with bootstrap URLs derived from
/// the loopback base; the object form
/// `{"public_base_url": "ws://10.0.0.5:8080", "seed_max_chars": 200000}`
/// additionally rewrites the minted bootstrap URLs for clients that reach
/// the gateway through a proxy or a LAN address (the token/channel query
/// parameters are appended to this base) and/or clamps the projected seed
/// transcript at open time (upstream ask 30 stopgap; see
/// `live_wiring::clamp_seed_messages_oldest_first`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
enum GatewayLiveOption {
    #[default]
    Disabled,
    Enabled {
        public_base_url: Option<String>,
        seed_max_chars: Option<usize>,
    },
}

#[cfg(feature = "experimental-gpt-live")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct GatewayExperimentalLiveExecutionProfile {
    profile_id: String,
    session_instructions: String,
}

#[cfg(feature = "experimental-gpt-live")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct GatewayExperimentalLiveOption {
    principal: String,
    realm: meerkat_core::RealmId,
    factory: meerkat::ExperimentalLiveFactoryIdentity,
    qualification: meerkat::ExperimentalLiveGate0QualificationVersion,
    binding: meerkat_core::AuthBindingRef,
    voice: String,
    execution_profiles: Vec<GatewayExperimentalLiveExecutionProfile>,
}

/// `runtime_options.openai_live`: the public Live (`gpt-live-1`) host
/// registration. The execution identity is fixed by the gateway (provider
/// OpenAI, model `gpt-live-1`, this configured binding); the host supplies
/// only the principal, realm, binding, voice, and optional trusted session
/// instructions. There is no factory identity, Gate0 qualification, or named
/// execution profile catalogue on this path.
#[cfg(feature = "openai-live")]
type GatewayOpenAiLiveOption = meerkat_mobkit::public_live_config::PublicLiveRegistration;

/// `runtime_options.workgraph` wire forms. Booleans keep the original
/// semantics (on with defaulted store placement / off). A string is an
/// explicit DIRECTORY for the durable store — `workgraph.sqlite3` is created
/// inside it. The explicit form exists for identity-first launches that
/// persist through an SDK-hosted continuity store (`has_continuity_store`
/// without `persistent_state`): those have no state dir to default to, so a
/// bare `true` silently rides a memory store.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
enum GatewayWorkgraphOption {
    #[default]
    Enabled,
    Disabled,
    DurableDir(std::path::PathBuf),
}

struct GatewayAgentMemoryOptions {
    config: meerkat_mobkit::AgentMemoryConfig,
    path: std::path::PathBuf,
    store: GatewayAgentMemoryStoreKind,
    /// §8.4 distiller block from `agent_memory.distiller`. Disabled by
    /// default (flipping it is a calibration decision, §11).
    distiller: meerkat_mobkit::memory::distiller::DistillerConfig,
    /// §8.5 steward block from `agent_memory.steward`. Disabled by
    /// default; enablement is the application's call (mechanism from
    /// MobKit, policy from the app).
    steward: meerkat_mobkit::memory::steward::StewardConfig,
}

/// Which bundled store backs agent memory. SQLite is the default now that
/// the P1 recall coordinator and injection ledger ride on it
/// (docs/design/agent-memory-architecture.md §15); a realm's existing
/// markdown files are auto-imported when that realm is first accessed.
///
/// Markdown is still a RECOGNIZED value - it parses, it censuses, and the
/// per-knob `requires store='sqlite'` refusals below still name it - but it
/// is no longer a live execution backend: selecting it now fails init with
/// [`AgentMemoryStoreMigration`]. The markdown READER stays fully intact for
/// the deliberate one-shot import (the SQLite store migrates a realm's
/// un-imported `.md` files when that realm's connection is first opened),
/// which is the supported path off markdown and the thing the migration
/// error points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum GatewayAgentMemoryStoreKind {
    Markdown,
    #[default]
    Sqlite,
}

/// A typed refusal for an agent-memory store kind that is no longer a live
/// execution backend. Typed rather than a bare string so the refusal has one
/// definition, one code, and one message that tests can pin - a migration
/// verdict, not an incidental open failure.
///
/// It rides the existing `STORAGE_RESOLUTION_CODE` because that is exactly
/// what it is from a client's point of view: this durable storage slot
/// cannot be resolved as configured. No new wire code is minted, so SDKs
/// that already branch on -32014 keep working unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentMemoryStoreMigration {
    /// `agent_memory.store = "markdown"` selected as the LIVE backend.
    MarkdownIsImportOnly,
}

/// Typed invalid-params refusal for public gateway activation of a parked
/// capability. Keeping this separate from generic parse strings prevents a
/// future composition rewrite from accidentally turning a compatibility key
/// back into an executable feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParkedGatewayCapability {
    Hygienist,
}

impl ParkedGatewayCapability {
    const fn code(self) -> i64 {
        -32602
    }

    fn message(self) -> String {
        match self {
            Self::Hygienist => "runtime_options.agent_memory.hygienist is PARKED and cannot be \
                 enabled through the public gateway: remove the key, set it to false, or use \
                 {enabled:false} while migrating existing configuration. The internal engine is \
                 retained for future validation, but this release has no supported provider \
                 proof for transcript curation."
                .to_string(),
        }
    }
}

impl AgentMemoryStoreMigration {
    const fn code(self) -> i64 {
        STORAGE_RESOLUTION_CODE
    }

    fn message(self) -> String {
        match self {
            Self::MarkdownIsImportOnly => "runtime_options.agent_memory.store='markdown' is \
                 retired as a live store and cannot back a running gateway: it has no manifest, \
                 no injection ledger, no taint firewall and no judgment plane, so a markdown \
                 deployment silently loses every §7-§10 guarantee the sqlite store provides. \
                 Migration is one step and lossless: set store='sqlite' (or drop the key - \
                 sqlite is the default) against the SAME agent-memory directory. \
                 The SQLite store imports each realm's un-imported .md files when that realm's \
                 connection is first opened, preserving memory ids, tags and timestamps, and \
                 renames each source file to .md.imported rather than deleting it."
                .to_string(),
        }
    }
}

/// §7.2: `operator_scope = "provisional"` composes operator-scope recall
/// only when an `OperatorResolver` is installed. The shipped SDK gateway
/// installs the console-principal resolver when provisional scope is enabled;
/// this predicate remains a fail-loud guard for any composition path that
/// omits it.
fn operator_scope_recall_inert(
    agent_memory: Option<&GatewayAgentMemoryOptions>,
    resolver_installed: bool,
) -> bool {
    !resolver_installed
        && agent_memory.is_some_and(|memory| {
            memory.config.operator_scope == meerkat_mobkit::AgentMemoryOperatorScope::Provisional
        })
}

impl GatewayRuntimeOptions {
    /// True when a strict GPT Live stdio registration (public `openai_live`
    /// or deprecated `experimental_live`) was configured. Either one needs the
    /// persistent live inputs even when the ordinary WebSocket door is off.
    fn strict_live_registered(&self) -> bool {
        #[cfg(feature = "experimental-gpt-live")]
        if self.experimental_live.is_some() {
            return true;
        }
        #[cfg(feature = "openai-live")]
        if self.openai_live.is_some() || self.console_voice.is_some() {
            return true;
        }
        false
    }
}

impl Default for GatewayRuntimeOptions {
    fn default() -> Self {
        Self {
            runtime_options: RuntimeOptions::default(),
            identity_bootstrap_mode: None,
            session_identity_config_root: None,
            max_sessions: 16,
            routing_routes: Vec::new(),
            gating: GatewayGatingConfig::default(),
            event_log: None,
            decisions: None,
            console_ui: ConsoleUiConfig::default(),
            console_require_app_auth: None,
            console_read_only: None,
            console_fetch_timeout_ms: None,
            access: None,
            demo_llm: false,
            member_comms_address: None,
            contacts: None,
            control_grants: ControlGrantTable::new(),
            agent_memory: None,
            workgraph: GatewayWorkgraphOption::Enabled,
            live: GatewayLiveOption::Disabled,
            #[cfg(feature = "experimental-gpt-live")]
            experimental_live: None,
            #[cfg(feature = "openai-live")]
            openai_live: None,
            #[cfg(feature = "openai-live")]
            console_voice: None,
            host_runnables: Vec::new(),
            runtime_store_ephemeral: false,
            mob_storage_ephemeral: false,
            declare_spec_update: None,
            composition_authority:
                meerkat_mobkit::mob_composition_manifest::CompositionAuthority::default(),
            candidate_definition:
                meerkat_mobkit::mob_composition_manifest::CandidateDefinition::default(),
            compaction: None,
            host_config: None,
            http_listen: meerkat_mobkit::gateway_composition::DEFAULT_GATEWAY_HTTP_LISTEN,
            http_public_base_url: None,
            allow_remote: false,
        }
    }
}

/// The `meerkat::Config` every gateway-built agent is constructed from.
///
/// It starts from the host `config.toml` named by
/// `runtime_options.meerkat_config_path` when one was declared (the only door
/// for `[self_hosted]`, `[realm]` and `[models]`), else `Config::default()`,
/// and the gateway's own overlays are layered on top of it.
///
/// This is where the gateway's session-compaction policy becomes real:
/// meerkat's `AgentFactory::build_agent` builds its `DefaultCompactor` from
/// `config.compaction`, so an un-declared policy here is what leaves the
/// gateway on meerkat's model-aware `context_window * 4 / 5` trigger.
fn gateway_agent_config(options: &GatewayRuntimeOptions) -> Config {
    let mut config = options.host_config.clone().unwrap_or_default();
    if let Some(address) = options.member_comms_address.as_ref() {
        config.comms.mode = meerkat_core::CommsRuntimeMode::Tcp;
        config.comms.address = Some(address.clone());
    }
    if let Some(compaction) = options.compaction.as_ref() {
        // `parse_gateway_runtime_options` is the only producer of this slot
        // and validates before storing, so the error arm is unreachable from
        // a launched gateway. It is still handled rather than unwrapped: the
        // one rejected shape is a zero threshold, and refusing to install it
        // (falling back to the inherited policy) is strictly safer than the
        // compaction storm installing it would cause.
        if let Err(error) = meerkat_mobkit::apply_compaction_policy(&mut config, compaction) {
            tracing::error!(
                %error,
                "refusing an invalid runtime_options.compaction declaration; \
                 inheriting meerkat's default compaction policy instead"
            );
        }
    }
    config
}

/// The init-time model check, delegated to meerkat-mob's `validate_definition`
/// and narrowed to `DiagnosticCode::UnknownModel`.
///
/// meerkat's rule is the documented one (`rkat mob validate`): a profile
/// model is acceptable when it is catalogued, defined under this definition's
/// own `[models.<id>]`, or provider-annotated (`provider = "self_hosted"`
/// with a `self_hosted_server_id`, for one). The check this replaced compared
/// the model string against the static catalog alone, so it refused every
/// `[models.<id>]` custom model and every self-hosted alias before the mob
/// existed, while `MobBuilder::create` would have accepted them. Only this
/// one diagnostic is adopted at init on purpose: the rest of the set
/// (`MissingSkillRef`, wiring, flows) is meerkat's to refuse at create time,
/// and adopting it here would newly refuse existing hosts' definitions.
///
/// The "Did you mean" hint over catalog ids is kept for the fall-through
/// case, because the common cause is still a dated model id such as
/// `claude-sonnet-4-5-20250514` for `claude-sonnet-4-5`.
fn unknown_model_init_error(definition: &MobDefinition) -> Option<String> {
    let diagnostic = meerkat_mob::validate_definition(definition)
        .into_iter()
        .find(|diagnostic| diagnostic.code == meerkat_mob::DiagnosticCode::UnknownModel)?;
    let model = diagnostic
        .location
        .as_deref()
        .and_then(|location| location.strip_prefix("profiles.")?.strip_suffix(".model"))
        .and_then(|name| {
            definition
                .profiles
                .get(&meerkat_mob::ProfileName::from(name))
        })
        .and_then(|binding| binding.as_inline())
        .map(|profile| profile.model.as_str());
    let hint = model.map(catalog_model_hint).unwrap_or_default();
    Some(format!("{}{hint}", diagnostic.message))
}

/// `. Did you mean one of: ...?` over catalog ids that share a three-segment
/// prefix with `model` in either direction; empty when nothing is close.
fn catalog_model_hint(model: &str) -> String {
    let prefix = three_segment_prefix(model);
    let mut suggestions: Vec<&str> = meerkat_models::catalog::catalog()
        .iter()
        .filter(|entry| {
            entry.id.starts_with(prefix) || model.starts_with(three_segment_prefix(entry.id))
        })
        .map(|entry| entry.id)
        .collect();
    suggestions.sort_unstable();
    suggestions.dedup();
    suggestions.truncate(5);
    if suggestions.is_empty() {
        String::new()
    } else {
        format!(". Did you mean one of: {}?", suggestions.join(", "))
    }
}

/// The first three `-`-separated segments of a model id, borrowed
/// (`claude-sonnet-4` of `claude-sonnet-4-5-20250514`); the whole id when it
/// has fewer.
fn three_segment_prefix(id: &str) -> &str {
    id.match_indices('-')
        .nth(2)
        .map_or(id, |(index, _)| &id[..index])
}

/// Whether `model` is a catalog id.
fn is_catalogued_model(model: &str) -> bool {
    meerkat_models::catalog::catalog()
        .iter()
        .any(|entry| entry.id == model)
}

/// One WARN line per provider-annotated profile whose model is neither
/// catalogued nor defined under this definition's `[models.<id>]`.
///
/// meerkat's rule (`validate_definition`) accepts such a profile at init: the
/// provider annotation is what makes the model resolvable, so
/// `unknown_model_init_error` says nothing about it, and a dated or misspelled
/// id on a `provider = "openai"` profile (every downstream-app profile is annotated)
/// surfaces at the first LLM call rather than at init. These lines keep that
/// visible without making the gateway a second refusal authority; the catalog
/// hint rides along for the typo case. A `provider = "self_hosted"` alias is
/// registered in the meerkat host config rather than the catalog, so it is
/// checked against `[self_hosted.models]` there and warned about only when
/// absent (the member would then fail at its first build).
fn uncatalogued_provider_annotated_model_warnings(
    definition: &MobDefinition,
    host_config: Option<&Config>,
) -> Vec<String> {
    let mut warnings = Vec::new();
    for (name, binding) in &definition.profiles {
        let Some(profile) = binding.as_inline() else {
            continue;
        };
        let Some(provider) = profile.provider else {
            continue;
        };
        let model = profile.model.as_str();
        if is_catalogued_model(model) || definition.models.contains_key(&profile.model) {
            continue;
        }
        if provider == meerkat_core::Provider::SelfHosted {
            let registered =
                host_config.is_some_and(|config| config.self_hosted.models.contains_key(model));
            if !registered {
                warnings.push(format!(
                    "profiles.{name} model '{model}' (provider = \"self_hosted\") is not \
                     registered under [self_hosted.models] in the meerkat host config; init \
                     accepts it on meerkat's rule and the member will fail at its first build \
                     unless runtime_options.meerkat_config_path names a config that defines it",
                    name = name.as_str(),
                ));
            }
            continue;
        }
        warnings.push(format!(
            "profiles.{name} model '{model}' is provider-annotated (provider = \"{provider}\") \
             but is neither catalogued nor defined under [models.{model}]; init accepts it on \
             meerkat's rule and a typo surfaces at the first LLM call{hint}",
            name = name.as_str(),
            provider = provider.as_str(),
            hint = catalog_model_hint(model),
        ));
    }
    warnings
}

/// `mobkit/init` params that are `runtime_options` fields on this binary but
/// top-level init params on `mobkit_gateway`. A host that copies the other
/// binary's shape is told so, with the field's home named, instead of being
/// bound to loopback with the default config in silence: the same
/// refuse-only rule `mobkit_gateway` applies to a `runtime_options` object.
const RUNTIME_OPTIONS_ONLY_KEYS: [&str; 4] = [
    "http_listen",
    "allow_remote",
    "http_public_base_url",
    "meerkat_config_path",
];

fn refuse_misplaced_runtime_options(params: &Value) -> Result<(), String> {
    let misplaced: Vec<&str> = RUNTIME_OPTIONS_ONLY_KEYS
        .into_iter()
        .filter(|key| params.get(key).is_some())
        .collect();
    if misplaced.is_empty() {
        return Ok(());
    }
    let homes: Vec<String> = misplaced
        .iter()
        .map(|key| format!("runtime_options.{key}"))
        .collect();
    Err(format!(
        "top-level init param(s) {} are runtime_options fields on rpc_gateway: declare them as \
         {}. The top-level form is mobkit_gateway's init-param shape and this binary would \
         otherwise ignore it",
        misplaced.join(", "),
        homes.join(", "),
    ))
}

#[derive(Default)]
struct InMemoryEventLogStore {
    events: std::sync::Mutex<Vec<PersistedEvent>>,
}

impl EventLogStore for InMemoryEventLogStore {
    fn append_batch(
        &self,
        events: Vec<PersistedEvent>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), EventLogError>> + Send + '_>> {
        Box::pin(async move {
            self.events.lock().unwrap().extend(events);
            Ok(())
        })
    }

    fn query(
        &self,
        query: EventQuery,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<PersistedEvent>, EventLogError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let mut events = self.events.lock().unwrap().clone();
            events.retain(|event| {
                query
                    .since_ms
                    .is_none_or(|since| event.timestamp_ms >= since)
                    && query
                        .until_ms
                        .is_none_or(|until| event.timestamp_ms < until)
                    && query
                        .after_seq
                        .is_none_or(|after_seq| event.seq > after_seq)
                    && query
                        .member_id
                        .as_ref()
                        .is_none_or(|member_id| event.member_id.as_ref() == Some(member_id))
                    && (query.event_types.is_empty()
                        || query.event_types.iter().any(|event_type| {
                            matches!(
                                &event.event,
                                meerkat_mobkit::UnifiedEvent::Module(module)
                                    if &module.event_type == event_type
                            )
                        }))
            });
            events.sort_by_key(|event| event.seq);
            if let Some(limit) = query.limit {
                events.truncate(limit);
            }
            Ok(events)
        })
    }
}

/// Decision state for an SDK launch that configured no `auth_config`.
///
/// Fail-closed on purpose: `ConsolePolicy::default()` requires app auth and
/// the shared constructor trusts no signing key, so the console refuses every
/// request until the host either passes `runtime_options.auth_config` or opts
/// out with `runtime_options.console_require_app_auth = false`. The
/// session-store naming is the crate default (`default_dataset` /
/// `default_table`).
fn minimal_decision_state() -> RuntimeDecisionState {
    RuntimeDecisionState::local_console(ConsolePolicy::default(), None)
}

/// The HTTP listener exposure gate, run before any bootstrap work. The
/// decision state it consults is the one the console will serve with:
/// `auth_config` when the host passed one, else the keyless fallback, so
/// enforced console auth (a trusted key AND `require_app_auth`) opens the
/// gate on its own and `console_require_app_auth = false` does not.
fn http_bind_gate(
    options: &GatewayRuntimeOptions,
) -> Result<(), meerkat_mobkit::gateway_composition::HttpBindPolicyError> {
    let fallback;
    let decisions = match options.decisions.as_ref() {
        Some(decisions) => decisions,
        None => {
            fallback = minimal_decision_state();
            &fallback
        }
    };
    meerkat_mobkit::gateway_composition::validate_http_bind_policy(
        meerkat_mobkit::gateway_composition::GatewaySurface::RpcGateway,
        options.http_listen,
        meerkat_mobkit::gateway_composition::HttpBindPolicy::for_gateway(
            options.allow_remote,
            decisions,
        ),
    )
}

fn shell_module(id: &str, script: &str) -> ModuleConfig {
    ModuleConfig {
        id: id.to_string(),
        command: "sh".to_string(),
        args: vec!["-c".to_string(), script.to_string()],
        restart_policy: RestartPolicy::Never,
    }
}

const MODULE_BOUNDARY_ENV_KEY: &str = "MOBKIT_MODULE_BOUNDARY";
const MODULE_BOUNDARY_MCP: &str = "mcp";

#[derive(Debug, Deserialize)]
struct GatewayModuleConfig {
    id: String,
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default = "gateway_restart_policy_never")]
    restart_policy: RestartPolicy,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    boundary: Option<String>,
}

fn gateway_restart_policy_never() -> RestartPolicy {
    RestartPolicy::Never
}

impl GatewayModuleConfig {
    fn into_module_and_pre_spawn(self) -> (ModuleConfig, Option<PreSpawnData>) {
        let GatewayModuleConfig {
            id,
            command,
            args,
            restart_policy,
            mut env,
            boundary,
        } = self;
        if boundary
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case(MODULE_BOUNDARY_MCP))
        {
            env.insert(
                MODULE_BOUNDARY_ENV_KEY.to_string(),
                MODULE_BOUNDARY_MCP.to_string(),
            );
        }
        let pre_spawn = if env.is_empty() {
            None
        } else {
            Some(PreSpawnData {
                module_id: id.clone(),
                env: env.into_iter().collect(),
            })
        };
        (
            ModuleConfig {
                id,
                command,
                args,
                restart_policy,
            },
            pre_spawn,
        )
    }
}

fn parse_gateway_modules(params: &Value) -> (Vec<ModuleConfig>, Vec<PreSpawnData>) {
    let gateway_modules: Vec<GatewayModuleConfig> = params
        .get("modules")
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();

    let mut modules = Vec::with_capacity(gateway_modules.len());
    let mut pre_spawn = Vec::new();
    for gateway_module in gateway_modules {
        let (module, maybe_pre_spawn) = gateway_module.into_module_and_pre_spawn();
        modules.push(module);
        if let Some(pre_spawn_data) = maybe_pre_spawn {
            pre_spawn.push(pre_spawn_data);
        }
    }
    (modules, pre_spawn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerkat_mobkit::gateway_composition::ConsoleAuthPosture;
    use meerkat_mobkit::mob_handle_runtime::MobRuntimeError;
    use meerkat_mobkit::unified_runtime::types::IdentityAuthorityReleaseOutcome;
    use meerkat_mobkit::unified_runtime::types::RetiredSupervisorCleanupOutcome;
    use meerkat_mobkit::{RuntimeShutdownReport, ShutdownDrainReport};

    /// The compiled `action_risk_tiers` table must BIND a caller, not merely
    /// default for one. Before 0.8.19 the gateway filled the tier only when the
    /// caller omitted it, so any client could claim `r0` for an `r3` action and
    /// `evaluate_gating_action` would honour the claim - making the policy table
    /// advisory against exactly the caller it exists to constrain.
    ///
    /// Reverting the fix (filling only under `!params.contains_key("risk_tier")`)
    /// turns this red: the assertion below would see the caller's `r0` survive.
    #[test]
    fn configured_risk_tier_overrides_a_caller_supplied_tier() {
        let mut action_risk_tiers = HashMap::new();
        action_risk_tiers.insert("delete_workspace_data".to_string(), "r3".to_string());
        let gating = GatewayGatingConfig { action_risk_tiers };

        let claimed_low = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "mobkit/gating/evaluate",
            "params": {"action": "delete_workspace_data", "risk_tier": "r0"}
        })
        .to_string();

        let applied = apply_gateway_runtime_config_to_request(&claimed_low, &gating);
        let parsed: Value = serde_json::from_str(&applied).expect("rewritten request is json");
        assert_eq!(
            parsed["params"]["risk_tier"], "r3",
            "configured policy must win over a caller-supplied tier"
        );

        // The absent-tier path must keep working, and an action with no policy
        // entry must keep the caller's value rather than being silently dropped.
        let omitted = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "mobkit/gating/evaluate",
            "params": {"action": "delete_workspace_data"}
        })
        .to_string();
        let applied = apply_gateway_runtime_config_to_request(&omitted, &gating);
        let parsed: Value = serde_json::from_str(&applied).expect("rewritten request is json");
        assert_eq!(parsed["params"]["risk_tier"], "r3");

        let unknown_action = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "mobkit/gating/evaluate",
            "params": {"action": "not_in_policy", "risk_tier": "r1"}
        })
        .to_string();
        let applied = apply_gateway_runtime_config_to_request(&unknown_action, &gating);
        let parsed: Value = serde_json::from_str(&applied).expect("rewritten request is json");
        assert_eq!(
            parsed["params"]["risk_tier"], "r1",
            "an action absent from the policy table keeps the caller's tier"
        );
    }

    /// The default tracing filter (RUST_LOG unset) must surface this crate's
    /// own INFO lines — the 0.8.8 conversion-progress observability was
    /// invisible at the old blanket "warn" default — while keeping
    /// dependencies at WARN.
    #[test]
    fn default_tracing_filter_surfaces_own_info_keeps_deps_at_warn() {
        use tracing_subscriber::layer::SubscriberExt;
        let filter = tracing_subscriber::EnvFilter::try_new(
            meerkat_mobkit::gateway_composition::default_tracing_filter(GATEWAY_TRACING_TARGET),
        )
        .expect("default filter must parse");
        let subscriber = tracing_subscriber::registry().with(filter);
        tracing::subscriber::with_default(subscriber, || {
            assert!(
                tracing::enabled!(
                    target: "meerkat_mobkit::identity_first::local_store",
                    tracing::Level::INFO
                ),
                "the crate's own INFO lines (conversion progress) must pass the default filter"
            );
            assert!(
                tracing::enabled!(target: "rpc_gateway", tracing::Level::INFO),
                "the gateway binary's own INFO lines must pass the default filter"
            );
            assert!(
                !tracing::enabled!(target: "meerkat_runtime::ops_lifecycle", tracing::Level::INFO),
                "dependency INFO noise must stay filtered"
            );
            assert!(
                tracing::enabled!(target: "meerkat_runtime::ops_lifecycle", tracing::Level::WARN),
                "dependency warnings must still pass"
            );
        });
    }

    #[test]
    fn gateway_module_boundary_becomes_pre_spawn_data() {
        let params = json!({
            "modules": [{
                "id": "router",
                "command": "python3",
                "args": ["router.py"],
                "restart_policy": "on_failure",
                "boundary": "mcp",
                "env": {
                    "ROUTER_FIXTURE": "example"
                }
            }]
        });

        let (modules, pre_spawn) = parse_gateway_modules(&params);

        assert_eq!(
            modules,
            vec![ModuleConfig {
                id: "router".to_string(),
                command: "python3".to_string(),
                args: vec!["router.py".to_string()],
                restart_policy: RestartPolicy::OnFailure,
            }]
        );
        assert_eq!(
            pre_spawn,
            vec![PreSpawnData {
                module_id: "router".to_string(),
                env: vec![
                    ("MOBKIT_MODULE_BOUNDARY".to_string(), "mcp".to_string()),
                    ("ROUTER_FIXTURE".to_string(), "example".to_string()),
                ],
            }]
        );
    }

    #[test]
    fn gateway_module_without_env_does_not_create_pre_spawn_data() {
        let params = json!({
            "modules": [{
                "id": "delivery",
                "command": "python3"
            }]
        });

        let (modules, pre_spawn) = parse_gateway_modules(&params);

        assert_eq!(modules.len(), 1);
        assert_eq!(modules[0].id, "delivery");
        assert_eq!(modules[0].args, Vec::<String>::new());
        assert_eq!(modules[0].restart_policy, RestartPolicy::Never);
        assert!(pre_spawn.is_empty());
    }

    #[test]
    fn gateway_runtime_options_parse_host_runnables() {
        let params = json!({
            "runtime_options": {
                "host_runnables": ["digest", "backup.rotate"]
            }
        });
        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");
        assert_eq!(
            options
                .host_runnables
                .iter()
                .map(meerkat::HostRunnableName::as_str)
                .collect::<Vec<_>>(),
            vec!["digest", "backup.rotate"]
        );
    }

    #[test]
    fn gateway_runtime_options_reject_invalid_host_runnables() {
        for (runtime_options, needle) in [
            (json!({"host_runnables": "digest"}), "must be an array"),
            (json!({"host_runnables": [7]}), "must be strings"),
            (json!({"host_runnables": ["  "]}), "is invalid"),
            (
                json!({"host_runnables": ["digest", "digest"]}),
                "duplicated",
            ),
            (
                json!({"host_runnables": [
                    meerkat_mobkit::schedule_wiring::STEWARD_DREAM_RUNNABLE
                ]}),
                "reserved",
            ),
        ] {
            let params = json!({ "runtime_options": runtime_options });
            let err = match parse_gateway_runtime_options(&params, None) {
                Err(err) => err,
                Ok(_) => panic!("expected rejection for {params}"),
            };
            assert!(err.contains(needle), "{err} should mention '{needle}'");
        }
    }

    /// Every documented runtime_options door must survive the UNKNOWN-FIELD
    /// ALLOWLIST, not merely have a handler.
    ///
    /// Both of these shipped in 0.8.24 with a working handler, a documented
    /// struct field, and NO allowlist entry, so the unknown-field rejection ran
    /// first and the handlers were dead code in the published binary. A downstream app
    /// found it by sending the documented line to the released artifact and
    /// getting `unsupported runtime_options fields: mob_composition` - one step
    /// EARLIER than the refusal the flag existed to avoid.
    ///
    /// The persistence suite stayed green throughout because it constructs the
    /// authority through the builder, never through this parse path. A test that
    /// does not cross the door cannot prove the door opens.
    ///
    /// Asserts the PARSED VALUE, not the absence of an error: an allowlist entry
    /// with a broken handler would pass an is_ok() check.
    #[test]
    fn documented_runtime_options_doors_survive_the_unknown_field_allowlist() {
        let candidate = parse_gateway_runtime_options(
            &json!({ "runtime_options": { "mob_composition": { "authority": "candidate" } } }),
            None,
        )
        .expect("mob_composition must pass the allowlist");
        assert_eq!(
            candidate.composition_authority,
            meerkat_mobkit::mob_composition_manifest::CompositionAuthority::NonAuthoritative,
            "the candidate declaration must reach the parsed options, not just avoid rejection"
        );

        let authoritative = parse_gateway_runtime_options(
            &json!({ "runtime_options": { "mob_composition": { "authority": "authoritative" } } }),
            None,
        )
        .expect("explicit authoritative must parse");
        assert_eq!(
            authoritative.composition_authority,
            meerkat_mobkit::mob_composition_manifest::CompositionAuthority::Authoritative
        );

        let declared = parse_gateway_runtime_options(
            &json!({
                "mob_config": "[mob]\nid = \"declared\"",
                "runtime_options": {
                    "declare_spec_update": { "expected_revision": 7 }
                }
            }),
            Some(std::path::Path::new("/durable-state")),
        )
        .expect("declare_spec_update must pass the allowlist");
        assert_eq!(
            declared.declare_spec_update,
            Some(7),
            "the declared revision must reach the parsed options"
        );
        let candidate_update = parse_gateway_runtime_options(
            &json!({
                "mob_config": "[mob]\nid = \"declared\"",
                "runtime_options": {
                    "mob_composition": { "authority": "candidate" },
                    "declare_spec_update": { "expected_revision": 7 }
                }
            }),
            Some(std::path::Path::new("/durable-state")),
        )
        .err()
        .expect("a candidate launch must not mutate canonical definition authority");
        assert!(candidate_update.contains("requires an authoritative mob_composition"));

        // Default stays authoritative and undeclared, so a launch that says
        // nothing keeps the protection rather than silently losing it.
        let quiet = parse_gateway_runtime_options(&json!({ "runtime_options": {} }), None)
            .expect("empty options");
        assert_eq!(
            quiet.composition_authority,
            meerkat_mobkit::mob_composition_manifest::CompositionAuthority::Authoritative
        );
        assert_eq!(quiet.declare_spec_update, None);
    }

    /// `mob_composition.candidate_definition` reaches the parsed options, is
    /// candidate-only, and defaults to requiring a match.
    #[test]
    fn candidate_definition_option_parses_and_is_candidate_only() {
        use meerkat_mobkit::mob_composition_manifest::CandidateDefinition;
        let stored = parse_gateway_runtime_options(
            &json!({ "runtime_options": { "mob_composition": {
                "authority": "candidate",
                "candidate_definition": "stored"
            } } }),
            None,
        )
        .expect("a candidate may acknowledge the stored definition");
        assert_eq!(stored.candidate_definition, CandidateDefinition::Stored);

        let candidate = parse_gateway_runtime_options(
            &json!({ "runtime_options": { "mob_composition": { "authority": "candidate" } } }),
            None,
        )
        .expect("candidate");
        assert_eq!(
            candidate.candidate_definition,
            CandidateDefinition::RequireMatch,
            "a candidate that says nothing must still refuse a divergent definition"
        );

        let authoritative = parse_gateway_runtime_options(
            &json!({ "runtime_options": { "mob_composition": {
                "authority": "authoritative",
                "candidate_definition": "stored"
            } } }),
            None,
        )
        .err()
        .expect("an authoritative launch cannot acknowledge a stored definition");
        assert!(
            authoritative.contains("requires authority = 'candidate'"),
            "{authoritative}"
        );

        let unknown = parse_gateway_runtime_options(
            &json!({ "runtime_options": { "mob_composition": {
                "authority": "candidate",
                "candidate_definition": "supplied"
            } } }),
            None,
        )
        .err()
        .expect("an unknown choice refuses");
        assert!(unknown.contains("candidate_definition"), "{unknown}");
    }

    #[test]
    fn declared_spec_update_requires_exact_durable_authoritative_inputs() {
        let parse = |params: Value, persistent: bool| {
            parse_gateway_runtime_options(
                &params,
                persistent.then(|| std::path::Path::new("/durable-state")),
            )
        };

        let missing_state = parse(
            json!({
                "mob_config": "[mob]\nid = \"declared\"",
                "runtime_options": {
                    "declare_spec_update": { "expected_revision": 7 }
                }
            }),
            false,
        )
        .err()
        .expect("an ephemeral launch has no durable definition authority to update");
        assert!(missing_state.contains("requires persistent_state"));

        let missing_definition = parse(
            json!({
                "runtime_options": {
                    "declare_spec_update": { "expected_revision": 7 }
                }
            }),
            true,
        )
        .err()
        .expect("the built-in fallback definition must never become update content");
        assert!(missing_definition.contains("explicit mob_config"));

        let ephemeral_mob_storage = parse(
            json!({
                "mob_config": "[mob]\nid = \"declared\"",
                "runtime_options": {
                    "mob_storage": { "storage": "memory" },
                    "declare_spec_update": { "expected_revision": 7 }
                }
            }),
            true,
        )
        .err()
        .expect("in-memory mob storage pins no definition");
        assert!(ephemeral_mob_storage.contains("requires persistent mob storage"));

        let extra_declaration_field = parse(
            json!({
                "mob_config": "[mob]\nid = \"declared\"",
                "runtime_options": {
                    "declare_spec_update": {
                        "expected_revision": 7,
                        "accept_current": true
                    }
                }
            }),
            true,
        )
        .err()
        .expect("unknown declaration fields must not be silently ignored");
        assert!(extra_declaration_field.contains("declare_spec_update fields: accept_current"));
    }

    /// The identity-root door must REACH the parsed options, and its absence
    /// must stay absent.
    ///
    /// A declared root is the only thing standing between a rehearsal and this
    /// machine's ambient keys. If the field parsed but never reached the
    /// factory the launch would look configured and still resolve local keys,
    /// which is exactly the substitution that refused a production resume.
    #[test]
    fn a_declared_session_identity_root_reaches_the_parsed_options() {
        let declared = parse_gateway_runtime_options(
            &json!({
                "runtime_options": {
                    "session_identity_config_root": "/rehearsal/root"
                }
            }),
            None,
        )
        .expect("session_identity_config_root must pass the allowlist");
        assert_eq!(
            declared.session_identity_config_root.as_deref(),
            Some(std::path::Path::new("/rehearsal/root")),
            "the declared root must reach the parsed options, not merely avoid rejection"
        );

        // Silence stays silence: an ambient launch is the ordinary case and
        // must not acquire a root it never declared.
        let quiet = parse_gateway_runtime_options(&json!({ "runtime_options": {} }), None)
            .expect("empty options");
        assert_eq!(quiet.session_identity_config_root, None);

        // Present-but-empty is a configuration error, NOT an ambient fallback.
        // Treating it as absent would resolve local keys under a launch that
        // believed it had declared a root - the silent substitution again,
        // reached by a different route.
        let empty = parse_gateway_runtime_options(
            &json!({ "runtime_options": { "session_identity_config_root": "  " } }),
            None,
        );
        assert!(
            empty.is_err(),
            "an empty declared root must be refused rather than treated as absent"
        );
    }

    #[test]
    fn gateway_runtime_options_control_grants_are_closed_and_scoped() {
        let defaulted = parse_gateway_runtime_options(&json!({ "runtime_options": {} }), None)
            .expect("runtime options");
        assert!(
            defaulted.control_grants.is_empty(),
            "an omitted grant declaration must be deny-all"
        );

        let caller = meerkat_mobkit::GatewayPeerKeys::ephemeral();
        let params = json!({
            "runtime_options": {
                "control_grants_toml": format!(
                    "[control_grants.desktop]\npubkey = \"{}\"\nverbs = [\"lookup_member\", \"inject\"]\nmembers = [\"worker-1\"]\n",
                    caller.pubkey_b64()
                )
            }
        });
        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");
        let grant = options
            .control_grants
            .get(&caller.pubkey_bytes())
            .expect("desktop grant");
        assert!(
            grant
                .verbs()
                .contains(&meerkat_mobkit::runtime::cross_mob_control::ControlVerb::LookupMember)
        );
        assert!(
            grant
                .verbs()
                .contains(&meerkat_mobkit::runtime::cross_mob_control::ControlVerb::Inject)
        );
        assert_eq!(
            grant.members(),
            &meerkat_mobkit::runtime::cross_mob_control::ControlMemberScope::members(["worker-1"])
        );

        let missing_section = json!({
            "runtime_options": {
                "control_grants_toml": "[mobs]\nremote = \"inproc\"\n"
            }
        });
        let error = parse_gateway_runtime_options(&missing_section, None)
            .err()
            .expect("an explicit grant document without the section must fail");
        assert!(error.contains("must contain a [control_grants] section"));
    }

    /// The runtime_options allowlist stays closed: unknown keys are a hard
    /// init error naming the offender (M4 added `runtime_store`; anything
    /// else still rejects).
    #[test]
    fn gateway_runtime_options_reject_unknown_fields() {
        let params = json!({ "runtime_options": { "blob_storage": {} } });
        let err = match parse_gateway_runtime_options(&params, None) {
            Err(err) => err,
            Ok(_) => panic!("unknown runtime_options keys must be rejected"),
        };
        assert!(
            err.contains("unsupported runtime_options fields: blob_storage"),
            "{err}"
        );
    }

    /// `runtime_options.compaction` is the gateway's host-level compaction
    /// policy. Declaring a threshold must reach the `meerkat::Config` every
    /// agent is built from AND pin it — an un-pinned threshold is silently
    /// rescaled by meerkat to `context_window * 4 / 5` (840_000 tokens on a
    /// million-token model), which is the "compaction never fires" production
    /// failure this knob exists to prevent.
    #[test]
    fn gateway_runtime_options_parse_compaction_policy() {
        let defaulted = parse_gateway_runtime_options(&json!({ "runtime_options": {} }), None)
            .expect("runtime options");
        assert!(
            defaulted.compaction.is_none(),
            "an undeclared policy must inherit, not invent a threshold"
        );
        assert!(
            !gateway_agent_config(&defaulted)
                .compaction
                .auto_compact_threshold_explicit,
            "the inheriting form must stay un-pinned"
        );

        let params = json!({
            "runtime_options": {
                "compaction": {
                    "auto_compact_threshold": 120_000,
                    "recent_turn_budget": 6,
                }
            }
        });
        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");
        let config = gateway_agent_config(&options);
        assert_eq!(config.compaction.auto_compact_threshold, 120_000);
        assert!(
            config.compaction.auto_compact_threshold_explicit,
            "a declared gateway threshold must be pinned against model-aware scaling"
        );
        assert_eq!(config.compaction.recent_turn_budget, 6);
    }

    /// The compaction declaration is key-closed and validated at ingress: a
    /// typo or a zero threshold is a startup error, never a dead knob.
    #[test]
    fn gateway_runtime_options_reject_invalid_compaction_policy() {
        for (compaction, needle) in [
            (json!({"auto_compact_treshold": 100}), "unsupported fields"),
            (json!({"auto_compact_threshold": 0}), "greater than 0"),
            (json!({"auto_compact_threshold": "lots"}), "is invalid"),
            (json!(120_000), "must be a JSON object"),
        ] {
            let params = json!({ "runtime_options": { "compaction": compaction } });
            let err = match parse_gateway_runtime_options(&params, None) {
                Err(err) => err,
                Ok(_) => panic!("expected rejection for {params}"),
            };
            assert!(
                err.contains("runtime_options.compaction"),
                "{err} should name the offending path"
            );
            assert!(err.contains(needle), "{err} should mention '{needle}'");
        }
    }

    const HOST_CONFIG_FIXTURE: &str = r#"
[self_hosted.servers.local]
transport = "openai_compatible"
base_url = "http://127.0.0.1:11434"
api_style = "chat_completions"

[self_hosted.models.gemma-4-31b]
server = "local"
remote_model = "gemma4:31b"

[models.house-model]
provider = "openai"

[realm.global]
default_binding = "local"
"#;

    /// `runtime_options.meerkat_config_path` must pass the allowlist AND its
    /// tables must reach the `meerkat::Config` every agent is built from, with
    /// the gateway's own overlays still layered on top. Asserts the loaded
    /// VALUES, not the absence of an error: an allowlist entry with a handler
    /// that dropped the file would pass an is_ok() check.
    #[test]
    fn meerkat_config_path_reaches_the_agent_config_under_the_gateway_overlays() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, HOST_CONFIG_FIXTURE).expect("write host config");

        let options = parse_gateway_runtime_options(
            &json!({
                "runtime_options": {
                    "meerkat_config_path": path.to_string_lossy().to_string(),
                    "member_comms_address": "127.0.0.1:0",
                    "compaction": { "auto_compact_threshold": 120_000 }
                }
            }),
            None,
        )
        .expect("meerkat_config_path must pass the allowlist");
        let config = gateway_agent_config(&options);
        assert_eq!(
            config
                .self_hosted
                .models
                .get("gemma-4-31b")
                .map(|model| model.server.as_str()),
            Some("local"),
            "[self_hosted.models] must reach the agent config"
        );
        assert!(config.self_hosted.servers.contains_key("local"));
        assert!(
            config.models.custom.contains_key("house-model"),
            "[models.<id>] must reach the agent config"
        );
        assert!(
            config.realm.contains_key("global"),
            "[realm] must reach the agent config"
        );
        // The gateway overlays are layered on top of the host config, not
        // replaced by it.
        assert_eq!(config.comms.mode, meerkat_core::CommsRuntimeMode::Tcp);
        assert_eq!(config.compaction.auto_compact_threshold, 120_000);
        assert!(config.compaction.auto_compact_threshold_explicit);

        // Silence stays silence: no declared path, no host tables.
        let quiet = parse_gateway_runtime_options(&json!({ "runtime_options": {} }), None)
            .expect("empty options");
        assert!(quiet.host_config.is_none());
        assert!(gateway_agent_config(&quiet).self_hosted.models.is_empty());
    }

    #[test]
    fn model_fallback_host_policy_survives_gateway_overlays() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            "[model_fallback]\nenabled = false\n\
             chain = [{ model = \"gpt-5.5\", provider = \"openai\" }]\n\
             [model_fallback.policy]\ntrigger_after_attempts = 7\n",
        )
        .expect("write config");
        let options = parse_gateway_runtime_options(
            &json!({"runtime_options": {
                "meerkat_config_path": path,
                "member_comms_address": "127.0.0.1:0",
                "compaction": {"auto_compact_threshold": 120000}
            }}),
            None,
        )
        .expect("parse");
        let config = gateway_agent_config(&options);
        assert_eq!(config.model_fallback.enabled, Some(false));
        assert_eq!(config.model_fallback.chain.len(), 1);
        assert_eq!(config.model_fallback.policy.trigger_after_attempts, 7);
        assert_eq!(config.compaction.auto_compact_threshold, 120_000);
        assert_eq!(config.comms.mode, meerkat_core::CommsRuntimeMode::Tcp);
    }

    /// A missing or malformed host config is an init refusal that names the
    /// option and the file, never a silent `Config::default()`.
    #[test]
    fn meerkat_config_path_refuses_missing_malformed_and_untyped_values() {
        let dir = tempfile::tempdir().expect("tempdir");
        let malformed = dir.path().join("broken.toml");
        std::fs::write(&malformed, "[self_hosted\n").expect("write");
        for (value, needle) in [
            (
                json!(dir.path().join("absent.toml").to_string_lossy().to_string()),
                "absent.toml",
            ),
            (json!(malformed.to_string_lossy().to_string()), "is invalid"),
            (json!(7), "must be a string path"),
            (json!("   "), "must not be empty"),
        ] {
            let params = json!({ "runtime_options": { "meerkat_config_path": value } });
            let err = match parse_gateway_runtime_options(&params, None) {
                Err(err) => err,
                Ok(_) => panic!("expected rejection for {params}"),
            };
            assert!(
                err.contains("runtime_options.meerkat_config_path"),
                "{err} should name the option"
            );
            assert!(err.contains(needle), "{err} should mention '{needle}'");
        }
    }

    /// The init model check is meerkat's rule, not a bare catalog lookup: a
    /// `[models.<id>]` custom model and a provider-annotated self-hosted alias
    /// both pass (they are what `MobBuilder::create` accepts), while a dated
    /// catalog id is still refused with the typo hint and a nonsense id is
    /// refused without one.
    #[test]
    fn init_model_check_follows_meerkat_validate_definition_and_keeps_the_hint() {
        let custom = MobDefinition::from_toml(
            "[mob]\nid = \"m\"\n\n[profiles.w]\nmodel = \"house-model\"\n\n\
             [models.house-model]\nprovider = \"openai\"\n",
        )
        .expect("custom model definition");
        assert_eq!(
            unknown_model_init_error(&custom),
            None,
            "[models.<id>] must pass init"
        );

        let self_hosted = MobDefinition::from_toml(
            "[mob]\nid = \"m\"\n\n[profiles.w]\nmodel = \"gemma-4-31b\"\n\
             provider = \"self_hosted\"\nself_hosted_server_id = \"local\"\n",
        )
        .expect("self-hosted definition");
        assert_eq!(
            unknown_model_init_error(&self_hosted),
            None,
            "a provider-annotated self-hosted alias must pass init"
        );

        let catalogued =
            MobDefinition::from_toml("[mob]\nid = \"m\"\n\n[profiles.w]\nmodel = \"gpt-5.5\"\n")
                .expect("catalogued definition");
        assert_eq!(unknown_model_init_error(&catalogued), None);

        let dated = MobDefinition::from_toml(
            "[mob]\nid = \"m\"\n\n[profiles.w]\nmodel = \"claude-sonnet-4-5-20250514\"\n",
        )
        .expect("a dated id parses");
        let message =
            unknown_model_init_error(&dated).expect("a dated catalog id must be refused at init");
        assert!(message.contains("claude-sonnet-4-5-20250514"), "{message}");
        assert!(message.contains("profiles.w"), "{message}");
        assert!(
            message.contains("Did you mean one of:") && message.contains("claude-sonnet-4-5"),
            "{message}"
        );

        let unknown = MobDefinition::from_toml(
            "[mob]\nid = \"m\"\n\n[profiles.w]\nmodel = \"no-such-model-anywhere\"\n",
        )
        .expect("an unknown id parses");
        let message = unknown_model_init_error(&unknown).expect("an unknown model must be refused");
        assert!(
            !message.contains("Did you mean"),
            "no hint when nothing is close: {message}"
        );
    }

    /// A provider-annotated profile bypasses the init refusal by meerkat's
    /// rule, so a dated id on it passes init; the gateway then owes one WARN
    /// naming the profile, the model and the closest catalog ids. Catalogued
    /// and `[models.<id>]`-defined annotated models warn about nothing, and a
    /// self-hosted alias warns only when the host config does not register it.
    #[test]
    fn provider_annotated_dated_model_passes_init_and_warns_with_the_hint() {
        let dated = MobDefinition::from_toml(
            "[mob]\nid = \"m\"\n\n[profiles.w]\nmodel = \"claude-sonnet-4-5-20250514\"\n\
             provider = \"anthropic\"\n",
        )
        .expect("a dated annotated id parses");
        assert_eq!(
            unknown_model_init_error(&dated),
            None,
            "provider annotation must pass init (meerkat's rule)"
        );
        let warnings = uncatalogued_provider_annotated_model_warnings(&dated, None);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        let warning = &warnings[0];
        assert!(warning.contains("profiles.w"), "{warning}");
        assert!(warning.contains("claude-sonnet-4-5-20250514"), "{warning}");
        assert!(warning.contains("provider = \"anthropic\""), "{warning}");
        assert!(warning.contains("first LLM call"), "{warning}");
        assert!(
            warning.contains("Did you mean one of:") && warning.contains("claude-sonnet-4-5"),
            "{warning}"
        );

        let quiet = MobDefinition::from_toml(
            "[mob]\nid = \"m\"\n\n[profiles.a]\nmodel = \"gpt-5.5\"\nprovider = \"openai\"\n\n\
             [profiles.b]\nmodel = \"house-model\"\nprovider = \"openai\"\n\n\
             [models.house-model]\nprovider = \"openai\"\n",
        )
        .expect("catalogued and custom annotated models parse");
        assert!(
            uncatalogued_provider_annotated_model_warnings(&quiet, None).is_empty(),
            "catalogued and [models.<id>] models must not warn"
        );

        let self_hosted = MobDefinition::from_toml(
            "[mob]\nid = \"m\"\n\n[profiles.w]\nmodel = \"gemma-4-31b\"\n\
             provider = \"self_hosted\"\nself_hosted_server_id = \"local\"\n",
        )
        .expect("self-hosted definition");
        let unregistered = uncatalogued_provider_annotated_model_warnings(&self_hosted, None);
        assert_eq!(unregistered.len(), 1, "{unregistered:?}");
        assert!(
            unregistered[0].contains("[self_hosted.models]")
                && unregistered[0].contains("meerkat_config_path"),
            "{}",
            unregistered[0]
        );
        let mut host = Config::default();
        host.merge_toml_str(HOST_CONFIG_FIXTURE)
            .expect("host config fixture");
        assert!(
            uncatalogued_provider_annotated_model_warnings(&self_hosted, Some(&host)).is_empty(),
            "an alias the host config registers must not warn"
        );
    }

    /// The four exposure/config options are `runtime_options` fields here and
    /// top-level init params on `mobkit_gateway`; the top-level shape must be
    /// refused by name, never silently ignored.
    #[test]
    fn top_level_exposure_options_are_refused_and_pointed_at_runtime_options() {
        assert_eq!(
            refuse_misplaced_runtime_options(&json!({
                "mob_config": "",
                "runtime_options": { "http_listen": "127.0.0.1:0" }
            })),
            Ok(())
        );
        let error = refuse_misplaced_runtime_options(&json!({
            "http_listen": "0.0.0.0:8080",
            "meerkat_config_path": "/etc/example/config.toml"
        }))
        .err()
        .unwrap_or_default();
        assert!(error.contains("runtime_options.http_listen"), "{error}");
        assert!(
            error.contains("runtime_options.meerkat_config_path"),
            "{error}"
        );
        assert!(error.contains("mobkit_gateway"), "{error}");
        for key in RUNTIME_OPTIONS_ONLY_KEYS {
            let error = refuse_misplaced_runtime_options(&json!({ key: true }))
                .err()
                .unwrap_or_default();
            assert!(error.contains(&format!("runtime_options.{key}")), "{error}");
        }
    }

    /// M4: `runtime_store` accepts only the explicit in-memory declaration;
    /// persistent SQLite is the default and any other spelling rejects.
    #[test]
    fn gateway_runtime_options_parse_runtime_store_declaration() {
        let params = json!({ "runtime_options": { "runtime_store": { "storage": "memory" } } });
        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");
        assert!(options.runtime_store_ephemeral);

        let defaulted = parse_gateway_runtime_options(&json!({ "runtime_options": {} }), None)
            .expect("runtime options");
        assert!(!defaulted.runtime_store_ephemeral);

        for (value, needle) in [
            (json!({"storage": "sqlite"}), "unsupported"),
            (json!({"storage": 7}), "must be 'memory'"),
            (json!("memory"), "must be a JSON object"),
        ] {
            let params = json!({ "runtime_options": { "runtime_store": value } });
            let err = match parse_gateway_runtime_options(&params, None) {
                Err(err) => err,
                Ok(_) => panic!("expected rejection for {params}"),
            };
            assert!(err.contains(needle), "{err} should mention '{needle}'");
        }
    }

    /// M4: `event_log.storage` gains the explicit 'null' declaration; the
    /// pre-existing 'memory' wire form keeps working; everything else
    /// rejects.
    #[test]
    fn gateway_runtime_options_event_log_storage_declarations() {
        for storage in ["memory", "in_memory", "null"] {
            let params = json!({ "runtime_options": { "event_log": { "storage": storage } } });
            let options = parse_gateway_runtime_options(&params, None)
                .unwrap_or_else(|err| panic!("storage '{storage}' must parse: {err}"));
            assert!(options.event_log.is_some());
        }
        let err = match parse_gateway_runtime_options(
            &json!({ "runtime_options": { "event_log": { "storage": "sqlite" } } }),
            None,
        ) {
            Err(err) => err,
            Ok(_) => panic!("undeclared event_log storage must be rejected"),
        };
        assert!(
            err.contains("unsupported runtime_options.event_log.storage"),
            "{err}"
        );
    }

    #[test]
    fn gateway_runtime_options_parse_live_seed_max_chars() {
        let params = json!({
            "runtime_options": {
                "live": { "seed_max_chars": 200000 }
            }
        });
        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");
        assert_eq!(
            options.live,
            GatewayLiveOption::Enabled {
                public_base_url: None,
                seed_max_chars: Some(200_000),
            }
        );

        let err = match parse_gateway_runtime_options(
            &json!({"runtime_options": {"live": {"seed_max_chars": 0}}}),
            None,
        ) {
            Err(err) => err,
            Ok(_) => panic!("zero seed_max_chars must be rejected"),
        };
        assert!(err.contains("seed_max_chars"), "{err}");
    }

    fn test_callback_bridge() -> StdioCallbackBridge {
        let (stdout_tx, _stdout_rx) = mpsc::channel::<GatewayStdoutLine>(4);
        StdioCallbackBridge::new(stdout_tx)
    }

    #[cfg(feature = "openai-live")]
    fn stale_public_observation_binding() -> (
        Arc<meerkat_runtime::MeerkatMachine>,
        meerkat_live::ProviderWebrtcBinding,
    ) {
        (
            Arc::new(meerkat_runtime::MeerkatMachine::ephemeral()),
            meerkat_live::ProviderWebrtcBinding::new(
                meerkat_live::LiveChannelId::new("stale-output-channel"),
                meerkat_core::SessionId::new(),
                meerkat_live::LiveRuntimeBindingGeneration::new(1),
                meerkat_live::LiveRuntimeBindingFence::new(1),
            ),
        )
    }

    #[cfg(feature = "openai-live-test")]
    struct PublicationTestSideband;

    #[cfg(feature = "openai-live-test")]
    #[async_trait]
    impl meerkat_live::ProviderWebrtcSidebandSession for PublicationTestSideband {
        async fn send_command(
            &self,
            _command: meerkat_live::LiveSidebandCommand,
        ) -> Result<
            meerkat_live::LiveSidebandCommandDelivery,
            meerkat_live::ProviderWebrtcBrokerError,
        > {
            Err(meerkat_live::ProviderWebrtcBrokerError::Unavailable)
        }

        async fn next_observation(
            &self,
        ) -> Result<
            Option<meerkat_live::LiveSidebandObservation>,
            meerkat_live::ProviderWebrtcBrokerError,
        > {
            Ok(None)
        }

        async fn close(&self) -> Result<(), meerkat_live::ProviderWebrtcBrokerError> {
            Ok(())
        }
    }

    #[cfg(feature = "openai-live-test")]
    struct PublicationTestAnswerTransport;

    #[cfg(feature = "openai-live-test")]
    struct PublicationTestPendingBoundReady;

    #[cfg(feature = "openai-live-test")]
    #[async_trait]
    impl meerkat_live::ProviderWebrtcPendingBoundReadyResolver for PublicationTestPendingBoundReady {
        async fn resolve(self: Box<Self>) -> Result<u64, meerkat_live::ProviderWebrtcBrokerError> {
            Ok(0)
        }
    }

    #[cfg(feature = "openai-live-test")]
    #[async_trait]
    impl meerkat_live::LiveWebrtcAnswerTransport for PublicationTestAnswerTransport {
        async fn answer_admitted_offer(
            &self,
            offer: meerkat_live::LiveWebrtcAdmittedOffer,
        ) -> Result<meerkat_live::LiveWebrtcAnswerAccepted, meerkat_live::LiveWebrtcError> {
            let answer = offer
                .into_provider_offer()?
                .into_pending_bound_ready_answer(
                    "publication-answer".to_string(),
                    Arc::new(PublicationTestSideband),
                    Box::new(PublicationTestPendingBoundReady),
                );
            let (answer_sdp, _sideband, pending_bound_ready) = answer.into_parts();
            Ok(meerkat_live::LiveWebrtcAnswerAccepted {
                answer_sdp,
                answer_observation_sequence: 1,
                pending_bound_ready: Some(pending_bound_ready),
            })
        }

        async fn reject_answer(
            &self,
            _binding: &meerkat_live::LiveWebrtcBindingRequest,
            _answer_observation_sequence: u64,
        ) -> Result<(), meerkat_live::LiveWebrtcError> {
            Ok(())
        }

        async fn accept_answer(
            &self,
            _binding: &meerkat_live::LiveWebrtcBindingRequest,
            _answer_observation_sequence: u64,
        ) {
        }

        async fn wait_for_construction_cleanup(
            &self,
            _binding: &meerkat_live::LiveWebrtcBindingRequest,
        ) -> Result<(), meerkat_live::LiveWebrtcError> {
            Ok(())
        }

        async fn close_binding(
            &self,
            _binding: &meerkat_live::LiveWebrtcBindingRequest,
        ) -> Result<(), meerkat_live::LiveWebrtcError> {
            Ok(())
        }
    }

    #[cfg(feature = "openai-live-test")]
    struct PublicationTestBoundReadyBinder;

    #[cfg(feature = "openai-live-test")]
    struct PublicationTestBoundReadyCustody {
        authority:
            Option<meerkat_runtime::meerkat_machine::LiveWebrtcAnswerExecutionBindingAuthority>,
    }

    #[cfg(feature = "openai-live-test")]
    #[async_trait]
    impl meerkat::surface::LiveWebrtcBoundReadyCustody for PublicationTestBoundReadyCustody {
        async fn commit(mut self: Box<Self>) -> Result<(), String> {
            if let Some(authority) = self.authority.take() {
                let _ = authority.commit();
            }
            Ok(())
        }

        async fn rollback(mut self: Box<Self>) -> Result<(), String> {
            let _rollback = self
                .authority
                .take()
                .map(|authority| authority.into_rollback());
            Ok(())
        }
    }

    #[cfg(feature = "openai-live-test")]
    #[async_trait]
    impl meerkat::surface::LiveWebrtcBoundReadyBinder for PublicationTestBoundReadyBinder {
        async fn bind_answer_ready(
            &self,
            runtime: Arc<meerkat_runtime::MeerkatMachine>,
            binding: &meerkat_live::LiveWebrtcBindingRequest,
            receipt: meerkat_live::ProviderWebrtcBoundReadyReceipt,
            answer_observation_sequence: u64,
        ) -> Result<
            Box<dyn meerkat::surface::LiveWebrtcBoundReadyCustody>,
            meerkat::surface::LiveWebrtcBoundReadyBindFailure,
        > {
            let runtime_binding = binding
                .runtime_binding
                .expect("test answer carries runtime binding");
            let provider_binding = meerkat_live::ProviderWebrtcBinding::new(
                binding.channel_id.clone(),
                binding.session_id.clone(),
                meerkat_live::LiveRuntimeBindingGeneration::new(runtime_binding.generation),
                meerkat_live::LiveRuntimeBindingFence::new(runtime_binding.fence),
            );
            let authority = runtime
                .accept_live_webrtc_answer_and_bind_execution(
                    &provider_binding,
                    &receipt,
                    answer_observation_sequence,
                )
                .await
                .expect("test answer binds through generated authority");
            Ok(Box::new(PublicationTestBoundReadyCustody {
                authority: Some(authority),
            }))
        }
    }

    #[cfg(feature = "openai-live-test")]
    async fn current_public_observation_binding() -> (
        Arc<meerkat_runtime::MeerkatMachine>,
        meerkat_live::ProviderWebrtcBinding,
    ) {
        let machine = Arc::new(meerkat_runtime::MeerkatMachine::ephemeral());
        let session_id = meerkat_core::SessionId::new();
        machine
            .prepare_bindings(session_id.clone())
            .await
            .expect("prepare exact live binding");
        let channel_id = meerkat_live::LiveChannelId::new("current-output-channel");
        let identity = meerkat_core::SessionLlmIdentity {
            model: "gpt-realtime-2".to_string(),
            provider: meerkat_core::Provider::OpenAI,
            self_hosted_server_id: None,
            provider_params: None,
            auth_binding: None,
        };
        machine
            .resolve_live_open_admission(&session_id, &channel_id, &identity)
            .await
            .expect("admit exact live binding");
        // 0.8.30 gates staging on `execution_mode_is_resolved`. Open admission
        // does not resolve a mode, so this fixture went straight from admitted
        // to staging and the guard refused it. The sequence mirrors the
        // established helper in live_wiring.rs; the mode is incidental to what
        // this test asserts (writer/queue-ack mechanics), it is only what the
        // guard requires to be RESOLVED rather than absent.
        let execution_profile =
            meerkat_runtime::live_execution::LiveExecutionProfileSelection::__test_new(
                "mobkit-test-public-observation",
                meerkat_core::LiveExecutionMode::FunctionBridge,
                meerkat_core::LiveExecutionCapabilities {
                    function_bridge: true,
                    client_context: false,
                },
            )
            .expect("test execution profile");
        machine
            .resolve_live_execution_profile_admission(&session_id, &channel_id, &execution_profile)
            .await
            .expect("resolve test live execution profile");
        let stage = machine
            .stage_experimental_live_execution(&session_id, &channel_id, 0)
            .await
            .expect("stage exact live binding");
        // The answer-accept-and-bind guard requires a registered playback
        // owner; without it the bind is refused from Attached. Same sequence as
        // `stage_receipt_answer_on_machine` in live_wiring.rs.
        let _readiness = machine
            .register_live_playback_owner(&stage, "publication-test-playback-owner")
            .await
            .expect("register test playback owner");
        let token = "publication-test-token";
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis() as u64;
        machine
            .record_live_webrtc_token_issued(&session_id, &channel_id, token, now, 60_000)
            .await
            .expect("issue exact signaling token");
        let answer = meerkat::surface::coordinate_live_webrtc_answer(
            Arc::clone(&machine),
            Arc::new(PublicationTestAnswerTransport),
            Some(Arc::new(PublicationTestBoundReadyBinder)),
            channel_id.clone(),
            token.to_string(),
            "publication-offer".to_string(),
        )
        .await
        .expect("bind exact answer through production coordinator");
        answer
            .delivery_custody
            .delivered()
            .await
            .expect("commit exact answer publication");
        let runtime_binding = machine
            .live_delegation_runtime_binding(&session_id, &channel_id)
            .await
            .expect("project exact live runtime binding");
        let provider_binding = meerkat_live::ProviderWebrtcBinding::new(
            channel_id,
            session_id,
            meerkat_live::LiveRuntimeBindingGeneration::new(runtime_binding.generation()),
            meerkat_live::LiveRuntimeBindingFence::new(runtime_binding.fence_token()),
        );
        (machine, provider_binding)
    }

    #[cfg(feature = "openai-live")]
    #[tokio::test]
    async fn live_public_observation_ack_waits_for_outer_writer_settlement() {
        let (machine, binding) = stale_public_observation_binding();
        let (_line, delivered) = GatewayStdoutLine::public_observation(
            Arc::clone(&machine),
            binding.clone(),
            "{\"event\":true}".to_string(),
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), delivered)
                .await
                .is_err(),
            "enqueue alone must not attest outer publication"
        );

        let (mut line, delivered) =
            GatewayStdoutLine::public_observation(machine, binding, "{\"event\":true}".to_string());
        line.settle_delivery(true).await;
        assert!(delivered.await.expect("writer settlement"));
    }

    #[cfg(feature = "openai-live")]
    #[tokio::test]
    async fn dropped_live_public_observation_rejects_delivery() {
        let (machine, binding) = stale_public_observation_binding();
        let (line, delivered) =
            GatewayStdoutLine::public_observation(machine, binding, "{\"event\":true}".to_string());
        drop(line);
        assert!(!delivered.await.expect("drop settlement"));
    }

    #[cfg(feature = "openai-live")]
    #[tokio::test]
    async fn queued_stale_live_public_observation_is_rejected_before_write() {
        let (machine, binding) = stale_public_observation_binding();
        let (line, _delivered) =
            GatewayStdoutLine::public_observation(machine, binding, "{\"event\":true}".to_string());
        assert!(
            line.acquire_public_observation_custody().await.is_err(),
            "a queued stale generation/fence must be fenced before stdout"
        );
    }

    #[cfg(feature = "openai-live-test")]
    #[tokio::test]
    async fn live_public_observation_requires_writer_and_sdk_queue_ack() {
        let (machine, binding) = current_public_observation_binding().await;
        let (stdout_tx, mut stdout_rx) = mpsc::channel::<GatewayStdoutLine>(1);
        let bridge = StdioCallbackBridge::new(stdout_tx);
        let publish = tokio::spawn({
            let bridge = bridge.clone();
            async move {
                bridge
                    .call_live_public_observation(
                        machine,
                        binding,
                        json!({
                            "channel_id": "current-output-channel",
                            "output_id": "opaque-output-1",
                            "content_index": 0,
                        }),
                    )
                    .await
            }
        });

        let mut line = stdout_rx.recv().await.expect("queued callback request");
        let request: Value = serde_json::from_str(&line).expect("callback request JSON");
        assert_eq!(
            request.get("method"),
            Some(&json!("mobkit/live/assistant_output_available"))
        );
        let custody = line
            .acquire_public_observation_custody()
            .await
            .expect("current binding")
            .expect("live line requires custody");
        line.settle_delivery(true).await;
        bridge
            .route_callback_response(json!({
                "jsonrpc": "2.0",
                "id": request.get("id").expect("callback id"),
                "result": {"accepted": true},
            }))
            .await;
        drop(custody);

        assert_eq!(
            publish
                .await
                .expect("publisher task")
                .expect("accepted callback"),
            json!({"accepted": true})
        );
    }

    /// Fix 3: `register_tool(..., input_schema=...)` schemas cross the
    /// `callback/build_agent` wire and land on the dispatcher's `ToolDef`s
    /// (which is what `live_visible_tool_defs` and normal turns both read).
    #[test]
    fn callback_tool_dispatcher_defs_carry_wire_schemas() {
        let schema = json!({
            "type": "object",
            "properties": { "city": { "type": "string" } },
            "required": ["city"]
        });
        let specs = vec![
            CallbackToolSpec::parse(&json!("legacy_name")).expect("legacy string spec"),
            CallbackToolSpec::parse(&json!({
                "name": "weather",
                "description": "Look up the weather",
                "input_schema": schema,
                "execution": {
                    "mode": "detached",
                    "runner": {"name": "weather.scan", "version": "1"},
                    "restart_class": "non_resumable",
                    "idempotency_scope": "interaction_and_arguments",
                    "submission_timeout_ms": 30000,
                    "credential_scopes": ["weather.read"]
                }
            }))
            .expect("object spec"),
        ];
        let dispatcher =
            CallbackToolDispatcher::new(test_callback_bridge(), "build-1".to_string(), specs, None);
        let defs = AgentToolDispatcher::tools(&dispatcher);
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].name.as_ref(), "legacy_name");
        assert_eq!(defs[0].description, "Python callback tool");
        assert_eq!(defs[0].input_schema, json!({"type": "object"}));
        assert_eq!(
            defs[0].provenance,
            Some(ToolProvenance {
                kind: ToolSourceKind::Callback,
                source_id: "mobkit-callback:build-1".into(),
            })
        );
        assert_eq!(defs[1].name.as_ref(), "weather");
        assert_eq!(defs[1].description, "Look up the weather");
        assert_eq!(defs[1].input_schema, schema);
        assert_eq!(defs[1].provenance, defs[0].provenance);
        let catalog = AgentToolDispatcher::tool_catalog(&dispatcher);
        assert_eq!(
            catalog[1].execution.default_mode(),
            meerkat_core::ToolExecutionMode::Detached
        );
        let detached = catalog[1]
            .execution
            .detached_policy()
            .expect("detached policy");
        assert_eq!(detached.runner().name(), "weather.scan");
        assert_eq!(detached.runner().version(), "1");
        assert_eq!(
            detached.restart_class(),
            meerkat_core::RestartClass::NonResumable
        );
        assert_eq!(
            detached.idempotency_scope(),
            meerkat_core::IdempotencyScope::InteractionAndArguments
        );
        assert_eq!(
            detached.credential_scopes(),
            &std::collections::BTreeSet::from(["weather.read".to_string()])
        );
    }

    #[test]
    fn callback_tool_spec_rejects_malformed_wire_entries() {
        for value in [
            json!(7),
            json!({"description": "no name"}),
            json!({"name": ""}),
            json!({"name": "x", "input_schema": "not-an-object"}),
            json!({"name": "x", "description": 3}),
            json!({"name": "x", "execution": {"mode": "detached"}}),
        ] {
            assert!(
                CallbackToolSpec::parse(&value).is_err(),
                "expected rejection for {value}"
            );
        }
    }

    #[test]
    fn callback_event_delivery_builds_stable_runtime_owned_ingress() {
        let job_id = meerkat::JobId::new("019f74fb-1907-7b21-932d-ab22c4d1f500").expect("job id");
        let session_id = meerkat_core::SessionId::parse("019f74fb-1907-7b21-932d-ab22c4d1f501")
            .expect("session id");
        let subscription = meerkat::JobSubscription::new(
            meerkat::JobSubscriptionId::new("review-agent").expect("subscription"),
            session_id,
            meerkat::JobDeliveryKind::Event {
                handling_mode: meerkat_core::HandlingMode::Queue,
            },
        );
        let lineage =
            meerkat::InteractionLineageId::from_string("019f74fb-1907-7b21-932d-ab22c4d1f502")
                .expect("lineage");
        let content = meerkat::JobDeliveryContent::Terminal(meerkat::JobTerminalResult::WorkerLost);

        let input = meerkat::job_delivery_event_input(
            &job_id,
            7,
            &subscription,
            &lineage,
            meerkat_core::HandlingMode::Queue,
            &content,
        );
        let meerkat_runtime::Input::ExternalEvent(event) = input else {
            panic!("job event delivery must use canonical external-event ingress");
        };
        assert_eq!(event.event_type, "job.terminal");
        assert_eq!(event.handling_mode, meerkat_core::HandlingMode::Queue);
        assert!(event.header.ingress_context.is_none());
        assert!(event.header.authority_association.is_none());
        assert!(event.header.retained_resume.is_none());
        assert_eq!(
            event
                .header
                .idempotency_key
                .as_ref()
                .map(ToString::to_string),
            Some(format!("job:{job_id}:7:review-agent"))
        );
        assert_eq!(
            event.payload,
            json!({
                "job_id": job_id.to_string(),
                "delivery_sequence": 7,
                "content": {
                    "kind": "terminal",
                    "result": "WorkerLost"
                }
            })
        );
        assert_eq!(
            event.header.correlation_id.map(|id| id.to_string()),
            Some(lineage.as_str().to_string())
        );
    }

    /// Test double: a real in-memory runtime store whose one cross-runtime
    /// delivery read answers a configured failure, so the health projection
    /// can be driven through both the typed store refusal and a real fault.
    struct DeliveryCensusFailingStore {
        inner: meerkat_runtime::InMemoryRuntimeStore,
        failure: fn() -> meerkat_runtime::store::RuntimeStoreError,
    }

    #[async_trait]
    impl meerkat_runtime::RuntimeStore for DeliveryCensusFailingStore {
        fn session_authority_ops(&self) -> &dyn meerkat_runtime::store::RuntimeSessionAuthorityOps {
            self.inner.session_authority_ops()
        }

        async fn commit_session_snapshot(
            &self,
            runtime_id: &meerkat_runtime::LogicalRuntimeId,
            session_delta: meerkat_runtime::store::SerializedSessionSnapshot,
        ) -> Result<(), meerkat_runtime::store::RuntimeStoreError> {
            self.inner
                .commit_session_snapshot(runtime_id, session_delta)
                .await
        }

        async fn commit_prepared_whole_blob_rewrite_boundary(
            &self,
            runtime_id: &meerkat_runtime::LogicalRuntimeId,
            boundary: meerkat_runtime::store::PreparedWholeBlobRewriteStoreParts,
        ) -> Result<
            meerkat_runtime::store::WholeBlobStoreAuthority,
            meerkat_runtime::store::RuntimeStoreError,
        > {
            self.inner
                .commit_prepared_whole_blob_rewrite_boundary(runtime_id, boundary)
                .await
        }

        async fn atomic_apply(
            &self,
            runtime_id: &meerkat_runtime::LogicalRuntimeId,
            session_delta: Option<meerkat_runtime::store::SerializedSessionSnapshot>,
            receipt: meerkat_core::lifecycle::RunBoundaryReceipt,
            input_updates: Vec<meerkat_runtime::input_state::InputStatePersistenceRecord>,
            session_store_key: Option<meerkat_core::types::SessionId>,
        ) -> Result<(), meerkat_runtime::store::RuntimeStoreError> {
            self.inner
                .atomic_apply(
                    runtime_id,
                    session_delta,
                    receipt,
                    input_updates,
                    session_store_key,
                )
                .await
        }

        async fn load_input_states(
            &self,
            runtime_id: &meerkat_runtime::LogicalRuntimeId,
        ) -> Result<Vec<meerkat_runtime::InputStateRow>, meerkat_runtime::store::RuntimeStoreError>
        {
            self.inner.load_input_states(runtime_id).await
        }

        async fn load_boundary_receipt(
            &self,
            runtime_id: &meerkat_runtime::LogicalRuntimeId,
            run_id: &meerkat_core::lifecycle::RunId,
            sequence: u64,
        ) -> Result<
            Option<meerkat_core::lifecycle::RunBoundaryReceipt>,
            meerkat_runtime::store::RuntimeStoreError,
        > {
            self.inner
                .load_boundary_receipt(runtime_id, run_id, sequence)
                .await
        }

        async fn load_session_snapshot(
            &self,
            runtime_id: &meerkat_runtime::LogicalRuntimeId,
        ) -> Result<Option<Arc<Vec<u8>>>, meerkat_runtime::store::RuntimeStoreError> {
            self.inner.load_session_snapshot(runtime_id).await
        }

        async fn clear_session_snapshot(
            &self,
            runtime_id: &meerkat_runtime::LogicalRuntimeId,
        ) -> Result<(), meerkat_runtime::store::RuntimeStoreError> {
            self.inner.clear_session_snapshot(runtime_id).await
        }

        async fn replace_session_snapshot_if_current(
            &self,
            runtime_id: &meerkat_runtime::LogicalRuntimeId,
            expected_current: &[u8],
            replacement: Vec<u8>,
        ) -> Result<bool, meerkat_runtime::store::RuntimeStoreError> {
            self.inner
                .replace_session_snapshot_if_current(runtime_id, expected_current, replacement)
                .await
        }

        async fn clear_session_snapshot_if_current(
            &self,
            runtime_id: &meerkat_runtime::LogicalRuntimeId,
            expected_current: &[u8],
        ) -> Result<bool, meerkat_runtime::store::RuntimeStoreError> {
            self.inner
                .clear_session_snapshot_if_current(runtime_id, expected_current)
                .await
        }

        async fn persist_input_state(
            &self,
            runtime_id: &meerkat_runtime::LogicalRuntimeId,
            state: &meerkat_runtime::input_state::InputStatePersistenceRecord,
        ) -> Result<(), meerkat_runtime::store::RuntimeStoreError> {
            self.inner.persist_input_state(runtime_id, state).await
        }

        async fn load_input_state(
            &self,
            runtime_id: &meerkat_runtime::LogicalRuntimeId,
            input_id: &meerkat_core::lifecycle::InputId,
        ) -> Result<
            Option<meerkat_runtime::input_state::StoredInputState>,
            meerkat_runtime::store::RuntimeStoreError,
        > {
            self.inner.load_input_state(runtime_id, input_id).await
        }

        async fn load_machine_lifecycle_record(
            &self,
            runtime_id: &meerkat_runtime::LogicalRuntimeId,
        ) -> Result<Option<Vec<u8>>, meerkat_runtime::store::RuntimeStoreError> {
            self.inner.load_machine_lifecycle_record(runtime_id).await
        }

        async fn commit_machine_lifecycle(
            &self,
            runtime_id: &meerkat_runtime::LogicalRuntimeId,
            commit: meerkat_runtime::store::MachineLifecycleCommit,
            input_states: &[meerkat_runtime::input_state::InputStatePersistenceRecord],
        ) -> Result<(), meerkat_runtime::store::RuntimeStoreError> {
            self.inner
                .commit_machine_lifecycle(runtime_id, commit, input_states)
                .await
        }

        async fn list_runtime_delivery_authorities(
            &self,
        ) -> Result<
            Vec<(
                meerkat_runtime::LogicalRuntimeId,
                meerkat_runtime::store::RuntimeDeliveryAuthorityRecord,
            )>,
            meerkat_runtime::store::RuntimeStoreError,
        > {
            Err((self.failure)())
        }
    }

    fn detached_runtime_over(
        runtime_store: Arc<dyn meerkat_runtime::RuntimeStore>,
    ) -> DetachedCallbackJobRuntime {
        let binary: Arc<dyn BinaryBlobStore> = Arc::new(ObjectStoreBlobStore::memory());
        let blobs: Arc<dyn meerkat_core::BlobStore> = Arc::new(Base64BlobStoreAdapter::new(binary));
        let jobs: Arc<dyn meerkat::DetachedJobStore> =
            Arc::new(meerkat::MemoryDetachedJobStore::new());
        let delivery = GatewayRuntimeDelivery::new(runtime_store, Arc::clone(&jobs));
        DetachedCallbackJobRuntime::new(
            meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
            jobs,
            blobs,
        )
        .with_runtime_delivery(&delivery)
    }

    /// The shell job projector and the detached-job delivery runtime share
    /// one inbox: a delivery projected through the shell path wakes the
    /// delivery runtime's commit signal and is in its backlog. Two inboxes
    /// constructed separately over the same store agree on the backlog but
    /// never see each other's commits, which is what this pins.
    #[tokio::test]
    async fn a_shell_projected_delivery_wakes_the_delivery_runtime() {
        let runtime_store: Arc<dyn meerkat_runtime::RuntimeStore> =
            Arc::new(meerkat_runtime::InMemoryRuntimeStore::new());
        let job_store = Arc::new(meerkat::MemoryDetachedJobStore::new());
        let delivery = GatewayRuntimeDelivery::new(runtime_store, job_store.clone());
        let projector = delivery.shell_job_projector(job_store.clone());
        let binary: Arc<dyn BinaryBlobStore> = Arc::new(ObjectStoreBlobStore::memory());
        let blobs: Arc<dyn meerkat_core::BlobStore> = Arc::new(Base64BlobStoreAdapter::new(binary));
        let runtime = DetachedCallbackJobRuntime::new(
            meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
            job_store.clone(),
            blobs,
        )
        .with_runtime_delivery(&delivery);
        let commits = runtime
            .runtime_inbox
            .as_ref()
            .expect("the delivery runtime holds the gateway inbox")
            .subscribe_commits();

        let jobs = meerkat::DetachedJobService::new(job_store);
        let receipt = jobs
            .submit(meerkat::JobSpec::new(
                meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
                meerkat_core::SessionId::new(),
                meerkat::ExecutionIntentId::new(),
                meerkat::InteractionLineageId::new(),
                meerkat::ToolIdentity::new("shell", "1").expect("tool"),
                meerkat::RunnerIdentity::new("durable-shell", "1").expect("runner"),
                meerkat::RestartClass::Adoptable,
                meerkat::CanonicalArgumentsHash::new("hash-shared-inbox").expect("hash"),
                meerkat::JobSubmissionKey::new("shared-inbox").expect("submission key"),
            ))
            .await
            .expect("submit");
        let claim = jobs
            .claim_attempt(
                &receipt.job_id,
                meerkat::AttemptClaim::new(
                    meerkat::WorkerId::new("worker").expect("worker"),
                    1,
                    100,
                    meerkat::RunnerHandleRef::new("runner-handle").expect("handle"),
                ),
            )
            .await
            .expect("claim");
        jobs.complete_attempt(
            &receipt.job_id,
            (&claim).into(),
            2,
            Some(meerkat::JobResultRef::new("result").expect("result")),
        )
        .await
        .expect("complete");

        let pass = projector.project_pending(16).await.expect("project");
        assert!(
            !pass.projected.is_empty(),
            "the terminal is projected: {pass:?}"
        );
        assert!(
            commits.has_changed().expect("the inbox is alive"),
            "the shell path's commit must wake the delivery runtime's subscribers"
        );
        assert_eq!(
            runtime.runtime_inbox_backlog_count().await,
            Ok(u64::try_from(pass.projected.len()).expect("small"))
        );
    }

    /// A gateway-shaped callback job runtime with one completed job whose
    /// Event subscription targets `subscriber`. The delivery service is a
    /// concrete `PersistentSessionService` that acquires the explicit serving
    /// machine before its first runtime use.
    async fn event_delivery_fixture(
        temp: &std::path::Path,
        register_subscriber: bool,
    ) -> Result<
        (
            DetachedCallbackJobRuntime,
            Arc<meerkat_runtime::MeerkatMachine>,
            meerkat_core::SessionId,
            meerkat_mobkit::MobRuntime,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let session_store: Arc<dyn meerkat::SessionStore> = Arc::new(
            meerkat_store::SqliteSessionStore::open(temp.join("sessions.sqlite3"))?,
        );
        let runtime_store: Arc<dyn meerkat_runtime::RuntimeStore> = Arc::new(
            meerkat_runtime::store::SqliteRuntimeStore::new(temp.join("runtime.sqlite3"))?,
        );
        let binary: Arc<dyn BinaryBlobStore> = Arc::new(ObjectStoreBlobStore::memory());
        let blob_store: Arc<dyn meerkat_core::BlobStore> =
            Arc::new(Base64BlobStoreAdapter::new(binary));
        let serving = Arc::new(
            meerkat_runtime::MeerkatMachine::persistent(
                Arc::clone(&runtime_store),
                Arc::clone(&blob_store),
            )
            .expect("acquire the RPC gateway fixture runtime machine"),
        );
        let job_store: Arc<dyn meerkat::DetachedJobStore> =
            Arc::new(meerkat::MemoryDetachedJobStore::new());
        let factory = meerkat::AgentFactory::new(temp);
        let mut builder = meerkat::FactoryAgentBuilder::new(factory, meerkat::Config::default());
        builder.default_detached_job_store = Some(Arc::clone(&job_store));
        let concrete: Arc<dyn meerkat_mob::MobSessionService> =
            Arc::new(meerkat_session::PersistentSessionService::new(
                builder,
                16,
                session_store,
                Arc::clone(&runtime_store),
                Arc::clone(&blob_store),
            ));
        let acquired = concrete
            .acquire_runtime_adapter(Some(Arc::clone(&serving)))
            .expect("acquire the event delivery fixture's serving runtime owner")
            .expect("the event delivery service exposes its runtime owner");
        assert!(
            acquired.shares_runtime_execution_owner_with(&serving),
            "event delivery and the session service must share their runtime owner"
        );

        let origin = meerkat_core::SessionId::new();
        let subscriber = meerkat_core::SessionId::new();
        if register_subscriber {
            serving.register_session(subscriber.clone()).await?;
        }

        let jobs = meerkat::DetachedJobService::new(Arc::clone(&job_store));
        let receipt = jobs
            .submit(meerkat::JobSpec::new(
                meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
                origin,
                meerkat::ExecutionIntentId::new(),
                meerkat::InteractionLineageId::new(),
                meerkat::ToolIdentity::new("weather", "1")?,
                meerkat::RunnerIdentity::new("weather.scan", "1")?,
                meerkat::RestartClass::NonResumable,
                meerkat::CanonicalArgumentsHash::new("hash-event-probe")?,
                meerkat::JobSubmissionKey::new("event-probe")?,
            ))
            .await?;
        jobs.subscribe(
            &receipt.job_id,
            meerkat::JobSubscription::new(
                meerkat::JobSubscriptionId::new("event-probe")?,
                subscriber.clone(),
                meerkat::JobDeliveryKind::Event {
                    handling_mode: meerkat_core::HandlingMode::Queue,
                },
            ),
        )
        .await?;
        let claim = jobs
            .claim_attempt(
                &receipt.job_id,
                meerkat::AttemptClaim::new(
                    meerkat::WorkerId::new("worker")?,
                    1,
                    100,
                    meerkat::RunnerHandleRef::new("runner-handle")?,
                ),
            )
            .await?;
        jobs.complete_attempt(
            &receipt.job_id,
            (&claim).into(),
            2,
            Some(meerkat::JobResultRef::new("result")?),
        )
        .await?;

        let delivery = GatewayRuntimeDelivery::new(runtime_store, Arc::clone(&job_store));
        let runtime = DetachedCallbackJobRuntime::new(
            meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
            job_store,
            blob_store,
        )
        .with_runtime_delivery(&delivery);
        let definition = meerkat_mob::MobDefinition::from_toml(&format!(
            "[mob]\nid = \"delivery-{}\"\n\n[profiles.worker]\nmodel = \"gpt-5.5\"\n",
            uuid::Uuid::new_v4()
        ))?;
        let owner = meerkat_mobkit::MobRuntime::bootstrap(
            MobBootstrapSpec::new(definition, MobStorage::in_memory(), concrete)
                .with_session_runtime_adapter(Arc::clone(&serving))?
                .with_runtime_delivery(delivery.composition.clone()),
        )
        .await?;
        tokio::time::timeout(Duration::from_secs(10), async {
            while delivery.composition.last_pass().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok((runtime, serving, subscriber, owner))
    }

    /// A job Event delivery is admitted on the machine that serves the
    /// subscriber's session. The service acquires that explicit owner before
    /// registration, and the drain receives the same machine for admission.
    #[tokio::test]
    async fn a_job_event_delivery_is_admitted_on_the_serving_machine()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let temp = tempfile::tempdir()?;
        let (runtime, serving, subscriber, _owner) =
            event_delivery_fixture(temp.path(), true).await?;

        let admitted = meerkat_runtime::SessionServiceRuntimeExt::list_active_inputs(
            serving.as_ref(),
            &subscriber,
        )
        .await?;
        assert_eq!(
            admitted.len(),
            1,
            "the job's Event delivery must be admitted on the serving machine"
        );
        let pass = runtime
            .runtime_delivery
            .as_ref()
            .expect("delivery")
            .last_pass()
            .expect("native pass");
        assert!(pass.blocked_sessions.is_empty(), "{pass:?}");
        Ok(())
    }

    /// Native owner health reports the blocked address and exact failures;
    /// unavailable row-level diagnostics must never masquerade as an empty list.
    #[tokio::test]
    async fn a_blocked_job_delivery_degrades_native_owner_health()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let temp = tempfile::tempdir()?;
        let (runtime, _serving, _subscriber, _owner) =
            event_delivery_fixture(temp.path(), false).await?;
        let pass = runtime
            .runtime_delivery
            .as_ref()
            .expect("delivery")
            .last_pass()
            .expect("native pass");
        assert!(!pass.blocked_runtimes.is_empty(), "{pass:?}");
        assert!(!pass.failures.is_empty(), "{pass:?}");
        let projection = runtime.health_projection().await?;
        assert_eq!(projection["detached_jobs"]["status"], json!("degraded"));
        assert!(projection["detached_jobs"]["blocked_deliveries"].is_null());
        assert_eq!(
            projection["detached_jobs"]["blocked_deliveries_reading"],
            "unavailable"
        );
        assert_eq!(
            projection["detached_jobs"]["runtime_delivery_pass"]["failures"],
            json!(pass.failures)
        );
        Ok(())
    }

    #[derive(Clone, Default)]
    struct CaptureWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl CaptureWriter {
        fn contents(&self) -> String {
            String::from_utf8_lossy(
                &self
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            )
            .into_owned()
        }
    }

    impl std::io::Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureWriter;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Regression for the 1 Hz `durable callback health projection failed
    /// ... list_runtime_delivery_authorities` WARN. The gateway hands the
    /// detached job runtime the epoch-tracking runtime-store facade (both
    /// composition paths, SDK-hosted continuity store included), and the
    /// facade did not forward the one cross-runtime delivery read, so every
    /// delivery-driver tick failed on the trait default. Every tick must now
    /// count, and nothing may be classified as unsupported.
    #[tokio::test]
    async fn health_projection_counts_the_runtime_inbox_through_the_epoch_tracking_facade() {
        let (runtime_store, _epochs) =
            meerkat_mobkit::mob_handle_runtime::epoch_tracking_runtime_store(Arc::new(
                meerkat_runtime::InMemoryRuntimeStore::new(),
            ));
        let runtime = detached_runtime_over(runtime_store);
        for tick in 0..3 {
            let projection = runtime
                .health_projection()
                .await
                .unwrap_or_else(|error| panic!("tick {tick}: health projection failed: {error}"));
            // This helper composes delivery but arms no native owner, so the
            // owner's pass dimension has nothing to read and the overall
            // status says so. The backlog census itself is read and counted.
            assert_eq!(projection["status"], json!("unreadable"));
            assert_eq!(
                projection["detached_jobs"]["runtime_delivery_pass"],
                Value::Null
            );
            assert_eq!(
                projection["detached_jobs"]["runtime_inbox_backlog"],
                json!(0)
            );
            assert_eq!(
                projection["detached_jobs"]["runtime_inbox_backlog_reading"],
                json!("counted")
            );
        }
        assert!(
            !runtime
                .runtime_inbox_unsupported_announced
                .load(Ordering::Acquire),
            "a store that answers must never be classified as unsupported"
        );
        assert_eq!(runtime.runtime_inbox_backlog_count().await, Ok(0));
    }

    /// A runtime store that genuinely cannot enumerate delivery authorities
    /// is a typed store property, not a per-tick fault: the projection
    /// succeeds with `runtime_inbox_backlog: null`, the dimension reads
    /// `unreadable` (never a fabricated zero), and the WARN naming the
    /// operation fires exactly once across many ticks.
    #[tokio::test]
    async fn health_projection_types_an_unsupported_runtime_inbox_read_and_warns_once() {
        let runtime = detached_runtime_over(Arc::new(DeliveryCensusFailingStore {
            inner: meerkat_runtime::InMemoryRuntimeStore::new(),
            failure: || {
                meerkat_runtime::store::RuntimeStoreError::Unsupported(
                    "list_runtime_delivery_authorities".to_string(),
                )
            },
        }));
        let writer = CaptureWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_max_level(tracing::Level::WARN)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        for tick in 0..5 {
            let projection = runtime
                .health_projection()
                .await
                .unwrap_or_else(|error| panic!("tick {tick}: unsupported must not fail: {error}"));
            assert_eq!(projection["status"], json!("unreadable"));
            assert_eq!(projection["detached_jobs"]["status"], json!("unreadable"));
            assert_eq!(
                projection["detached_jobs"]["runtime_inbox_backlog"],
                Value::Null,
                "an unsupported census must not publish a number"
            );
            assert_eq!(
                projection["detached_jobs"]["runtime_inbox_backlog_reading"],
                json!("unsupported")
            );
        }
        let warnings = writer.contents();
        assert_eq!(
            warnings
                .matches("does not support the runtime inbox backlog read")
                .count(),
            1,
            "the unsupported store is announced exactly once, got: {warnings}"
        );
        assert!(
            warnings.contains("list_runtime_delivery_authorities"),
            "the announcement names the refused operation, got: {warnings}"
        );
        assert!(
            runtime
                .runtime_inbox_unsupported_announced
                .load(Ordering::Acquire)
        );
        // The u64 wire contract cannot carry "unreadable": a typed error, not 0.
        let wire = runtime.runtime_inbox_backlog_count().await;
        assert!(
            wire.as_ref()
                .is_err_and(|error| error.contains("list_runtime_delivery_authorities")),
            "jobs/health must refuse rather than fabricate a zero, got {wire:?}"
        );
    }

    /// A store that CAN answer but fails is a real fault and must keep
    /// surfacing on every tick with its own text; it must never be folded
    /// into the once-only unsupported classification.
    #[tokio::test]
    async fn health_projection_surfaces_a_real_runtime_inbox_read_failure_every_tick() {
        let runtime = detached_runtime_over(Arc::new(DeliveryCensusFailingStore {
            inner: meerkat_runtime::InMemoryRuntimeStore::new(),
            failure: || {
                meerkat_runtime::store::RuntimeStoreError::ReadFailed(
                    "runtime.sqlite: disk I/O error".to_string(),
                )
            },
        }));
        for tick in 0..3 {
            let error = runtime
                .health_projection()
                .await
                .expect_err("a real read failure must surface on every tick");
            assert!(
                error.contains("disk I/O error"),
                "tick {tick}: the store's own failure text must survive, got: {error}"
            );
        }
        assert!(
            !runtime
                .runtime_inbox_unsupported_announced
                .load(Ordering::Acquire),
            "a read failure is not an unsupported store"
        );
    }

    #[test]
    fn runtime_inbox_backlog_classification_is_typed() {
        assert_eq!(
            classify_runtime_inbox_backlog(Ok(3)),
            Ok(RuntimeInboxBacklog::Counted(3))
        );
        assert_eq!(
            classify_runtime_inbox_backlog(Err(meerkat_runtime::RuntimeDeliveryError::Store(
                meerkat_runtime::store::RuntimeStoreError::Unsupported("list_x".to_string()),
            ))),
            Ok(RuntimeInboxBacklog::Unsupported {
                operation: "list_x".to_string()
            })
        );
        let corrupt = classify_runtime_inbox_backlog(Err(
            meerkat_runtime::RuntimeDeliveryError::Corrupt("cursor ahead of sequence".to_string()),
        ));
        assert!(corrupt.is_err_and(|error| error.contains("cursor ahead of sequence")));
        let read_failed =
            classify_runtime_inbox_backlog(Err(meerkat_runtime::RuntimeDeliveryError::Store(
                meerkat_runtime::store::RuntimeStoreError::ReadFailed("locked".to_string()),
            )));
        assert!(read_failed.is_err_and(|error| error.contains("locked")));
        assert_eq!(
            RuntimeInboxBacklog::Counted(0).reading(),
            meerkat::JobHealthReading::Ok
        );
        assert_eq!(
            RuntimeInboxBacklog::Counted(1).reading(),
            meerkat::JobHealthReading::Degraded
        );
        assert_eq!(
            RuntimeInboxBacklog::Unsupported {
                operation: "list_x".to_string()
            }
            .reading(),
            meerkat::JobHealthReading::Unreadable
        );
    }

    #[test]
    fn callback_runner_ownership_requires_exact_tool_runner_and_version() {
        let binary: Arc<dyn BinaryBlobStore> = Arc::new(ObjectStoreBlobStore::memory());
        let blobs: Arc<dyn meerkat_core::BlobStore> = Arc::new(Base64BlobStoreAdapter::new(binary));
        let runtime = DetachedCallbackJobRuntime::new(
            meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
            Arc::new(meerkat::MemoryDetachedJobStore::new()),
            blobs,
        );
        let dispatcher = CallbackToolDispatcher::new(
            test_callback_bridge(),
            "build-1".to_string(),
            vec![
                CallbackToolSpec::parse(&json!({
                    "name": "security_scan",
                    "execution": {
                        "mode": "detached",
                        "runner": {"name": "example.security_scan", "version": "1"},
                        "restart_class": "adoptable",
                        "idempotency_scope": "interaction_and_arguments",
                        "submission_timeout_ms": 30000
                    }
                }))
                .expect("tool spec"),
            ],
            Some(runtime.clone()),
        );
        assert!(
            dispatcher.reconcile_registered_catalog,
            "the first exact callback owner must trigger recovery"
        );
        let duplicate = CallbackToolDispatcher::new(
            test_callback_bridge(),
            "build-2".to_string(),
            vec![
                CallbackToolSpec::parse(&json!({
                    "name": "security_scan",
                    "execution": {
                        "mode": "detached",
                        "runner": {"name": "example.security_scan", "version": "1"},
                        "restart_class": "adoptable",
                        "idempotency_scope": "interaction_and_arguments",
                        "submission_timeout_ms": 30000
                    }
                }))
                .expect("tool spec"),
            ],
            Some(runtime.clone()),
        );
        assert!(
            !duplicate.reconcile_registered_catalog,
            "rebuilding an already-known exact owner must not start a duplicate recovery census"
        );
        let later_runner = CallbackToolDispatcher::new(
            test_callback_bridge(),
            "build-3".to_string(),
            vec![
                CallbackToolSpec::parse(&json!({
                    "name": "report_export",
                    "execution": {
                        "mode": "detached",
                        "runner": {"name": "example.report_export", "version": "1"},
                        "restart_class": "adoptable",
                        "idempotency_scope": "interaction_and_arguments",
                        "submission_timeout_ms": 30000
                    }
                }))
                .expect("tool spec"),
            ],
            Some(runtime.clone()),
        );
        assert!(
            later_runner.reconcile_registered_catalog,
            "a later exact owner must get its own recovery census"
        );
        let make_spec = |tool: &str, runner: &str, version: &str| {
            meerkat::JobSpec::new(
                meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
                meerkat_core::SessionId::parse("019f74fb-1907-7b21-932d-ab22c4d1f503")
                    .expect("session"),
                meerkat::ExecutionIntentId::from_string(format!("intent:{tool}")).expect("intent"),
                meerkat::InteractionLineageId::from_string(format!("lineage:{tool}"))
                    .expect("lineage"),
                meerkat::ToolIdentity::new(tool, version).expect("tool"),
                meerkat::RunnerIdentity::new(runner, version).expect("runner"),
                meerkat::RestartClass::Adoptable,
                meerkat::CanonicalArgumentsHash::new(format!("sha256:{}", "a".repeat(64)))
                    .expect("hash"),
                meerkat::JobSubmissionKey::new(format!("submission:{tool}:{runner}:{version}"))
                    .expect("submission"),
            )
        };
        assert!(runtime.owns_callback_job(&make_spec(
            "security_scan",
            "example.security_scan",
            "1"
        )));
        assert!(!runtime.owns_callback_job(&make_spec("other_tool", "example.security_scan", "1")));
        assert!(!runtime.owns_callback_job(&make_spec(
            "security_scan",
            "example.security_scan",
            "2"
        )));
    }

    #[tokio::test]
    async fn callback_reconcile_rehydrates_exact_committed_authority_without_advancing_fence() {
        let store = Arc::new(meerkat::MemoryDetachedJobStore::new());
        let service = meerkat::DetachedJobService::new(store.clone());
        let session_id = meerkat_core::SessionId::parse("019f74fb-1907-7b21-932d-ab22c4d1f532")
            .expect("session id");
        let spec = meerkat::JobSpec::new(
            meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
            session_id,
            meerkat::ExecutionIntentId::from_string("intent:reconcile").expect("intent"),
            meerkat::InteractionLineageId::from_string("lineage:reconcile").expect("lineage"),
            meerkat::ToolIdentity::new("security_scan", "1").expect("tool"),
            meerkat::RunnerIdentity::new("example.security_scan", "1").expect("runner"),
            meerkat::RestartClass::Adoptable,
            meerkat::CanonicalArgumentsHash::new(format!("sha256:{}", "a".repeat(64)))
                .expect("arguments hash"),
            meerkat::JobSubmissionKey::new("callback:reconcile").expect("submission key"),
        );
        let receipt = service.submit(spec).await.expect("submit");
        let claim = service
            .claim_attempt(
                &receipt.job_id,
                meerkat::AttemptClaim::new(
                    meerkat::WorkerId::new("worker-1").expect("worker"),
                    100,
                    u64::MAX,
                    meerkat::RunnerHandleRef::new("external:scan-1").expect("handle"),
                ),
            )
            .await
            .expect("claim");

        let (stdout_tx, mut stdout_rx) = mpsc::channel::<GatewayStdoutLine>(4);
        let bridge = StdioCallbackBridge::new(stdout_tx);
        let binary: Arc<dyn BinaryBlobStore> = Arc::new(ObjectStoreBlobStore::memory());
        let blobs: Arc<dyn meerkat_core::BlobStore> = Arc::new(Base64BlobStoreAdapter::new(binary));
        let dispatcher = CallbackToolDispatcher::new(
            bridge.clone(),
            "build-1".to_string(),
            vec![
                CallbackToolSpec::parse(&json!({
                    "name": "security_scan",
                    "execution": {
                        "mode": "detached",
                        "runner": {"name": "example.security_scan", "version": "1"},
                        "restart_class": "adoptable",
                        "idempotency_scope": "interaction_and_arguments",
                        "submission_timeout_ms": 30000
                    }
                }))
                .expect("tool spec"),
            ],
            Some(DetachedCallbackJobRuntime::new(
                meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
                store.clone(),
                blobs,
            )),
        );
        let reconcile = tokio::spawn(async move { dispatcher.reconcile_detached_jobs().await });
        let request_line = stdout_rx.recv().await.expect("reconcile callback");
        let request: Value = serde_json::from_str(&request_line).expect("callback json");
        assert_eq!(
            request.pointer("/params/attempts/0/authority/fence"),
            Some(&json!(claim.fence.get()))
        );
        assert_eq!(
            request.pointer("/params/attempts/0/authority/attempt_id"),
            Some(&json!(claim.attempt_id.to_string()))
        );
        assert_eq!(
            request.pointer("/params/attempts/0/runner_handle"),
            Some(&json!("external:scan-1"))
        );
        bridge
            .route_callback_response(json!({
                "jsonrpc": "2.0",
                "id": request["id"].clone(),
                "result": {
                    "live_attempts": [{
                        "job_id": receipt.job_id.to_string(),
                        "attempt_id": claim.attempt_id.to_string(),
                        "fence": claim.fence.get()
                    }]
                }
            }))
            .await;
        reconcile
            .await
            .expect("reconcile task")
            .expect("reconcile succeeds");

        let reopened = meerkat::DetachedJobStore::get(&*store, &receipt.job_id)
            .await
            .expect("read")
            .expect("job");
        assert_eq!(reopened.machine_state.attempt_count, 1);
        assert_eq!(reopened.machine_state.current_fence, claim.fence.get());
        assert_eq!(
            reopened.machine_state.current_attempt_id.as_deref(),
            Some(claim.attempt_id.as_str())
        );
    }

    #[test]
    fn gateway_runtime_options_parse_implicit_delegate_retirement() {
        let params = json!({
            "runtime_options": {
                "implicit_delegate_idle_retire_secs": 42,
                "implicit_delegate_idle_sweep_interval_ms": 2500
            }
        });

        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");

        assert_eq!(
            options.runtime_options.implicit_delegate_idle_retire_secs,
            Some(42)
        );
        assert_eq!(
            options
                .runtime_options
                .implicit_delegate_idle_sweep_interval_ms,
            2500
        );
    }

    #[test]
    fn gateway_runtime_options_null_disables_implicit_delegate_retirement() {
        let params = json!({
            "runtime_options": {
                "implicit_delegate_idle_retire_secs": null
            }
        });

        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");

        assert_eq!(
            options.runtime_options.implicit_delegate_idle_retire_secs,
            None
        );
    }

    #[test]
    fn gateway_runtime_options_parse_console_config_path() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let path = tmp.path().join("console.toml");
        std::fs::write(
            &path,
            r#"
[sidebar]
visible_controls = ["topology", "roster"]

[agent_list]
group_by = ["labels.console_group", "group"]
"#,
        )
        .expect("write console config");
        let params = json!({
            "runtime_options": {
                "console_config_path": path.to_string_lossy()
            }
        });

        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");

        assert_eq!(
            options.console_ui.sidebar.visible_controls,
            Some(vec!["topology".to_string(), "roster".to_string()])
        );
        assert_eq!(
            options.console_ui.agent_list.group_by,
            vec!["labels.console_group".to_string(), "group".to_string()]
        );
    }

    #[test]
    fn gateway_runtime_options_parse_access_config_path() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let path = tmp.path().join("access.toml");
        std::fs::write(
            &path,
            r#"
enabled = true
admins = ["root@example.test"]

[[rules]]
id = "everyone-views"
actions = ["agent.view"]
"#,
        )
        .expect("write access config");
        let params = json!({
            "runtime_options": {
                "access_config_path": path.to_string_lossy()
            }
        });

        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");

        let access = options.access.expect("access controller");
        assert!(access.enabled());
        let (config, _) = access.snapshot();
        assert_eq!(config.admins, vec!["root@example.test".to_string()]);
        assert_eq!(config.rules.len(), 1);
    }

    #[test]
    fn gateway_runtime_options_reject_invalid_access_config() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let path = tmp.path().join("access.toml");
        // Enabled without admins would lock everyone out — must fail closed.
        std::fs::write(&path, "enabled = true").expect("write access config");
        let params = json!({
            "runtime_options": {
                "access_config_path": path.to_string_lossy()
            }
        });

        let err = match parse_gateway_runtime_options(&params, None) {
            Ok(_) => panic!("invalid access config must be rejected"),
            Err(err) => err,
        };
        assert!(err.contains("access_config_path"), "{err}");
    }

    #[test]
    fn gateway_runtime_options_can_disable_console_auth_for_local_console() {
        let params = json!({
            "runtime_options": {
                "console_require_app_auth": false
            }
        });

        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");

        let decisions = options.decisions.expect("decisions");
        assert!(!decisions.console.require_app_auth);
        assert_eq!(ConsoleAuthPosture::of(&decisions), ConsoleAuthPosture::Open);
    }

    /// The three HTTP exposure doors parse to typed values and default to the
    /// posture every earlier release had: loopback, ephemeral port, no
    /// advertised base, no acknowledgement.
    #[test]
    fn gateway_runtime_options_parse_http_listen_public_base_and_allow_remote() -> Result<(), String>
    {
        let defaulted = parse_gateway_runtime_options(&json!({ "runtime_options": {} }), None)?;
        assert_eq!(
            defaulted.http_listen,
            meerkat_mobkit::gateway_composition::DEFAULT_GATEWAY_HTTP_LISTEN
        );
        assert!(defaulted.http_listen.ip().is_loopback());
        assert_eq!(defaulted.http_listen.port(), 0);
        assert_eq!(defaulted.http_public_base_url, None);
        assert!(!defaulted.allow_remote);

        let declared = parse_gateway_runtime_options(
            &json!({
                "runtime_options": {
                    "http_listen": "0.0.0.0:8080",
                    "http_public_base_url": "https://mob.example.com/",
                    "allow_remote": true
                }
            }),
            None,
        )?;
        assert_eq!(
            declared.http_listen,
            "0.0.0.0:8080"
                .parse::<std::net::SocketAddr>()
                .map_err(|error| error.to_string())?
        );
        assert_eq!(
            declared.http_public_base_url.as_deref(),
            Some("https://mob.example.com")
        );
        assert!(declared.allow_remote);
        Ok(())
    }

    #[test]
    fn gateway_runtime_options_reject_malformed_http_exposure_values() {
        for (runtime_options, needle) in [
            (
                json!({ "http_listen": "localhost:8080" }),
                "runtime_options.http_listen",
            ),
            (
                json!({ "http_listen": 8080 }),
                "runtime_options.http_listen",
            ),
            (
                json!({ "http_public_base_url": "mob.example.com" }),
                "runtime_options.http_public_base_url",
            ),
            (
                json!({ "allow_remote": "yes" }),
                "runtime_options.allow_remote",
            ),
        ] {
            let err = match parse_gateway_runtime_options(
                &json!({ "runtime_options": runtime_options }),
                None,
            ) {
                Ok(_) => String::new(),
                Err(err) => err,
            };
            assert!(err.contains(needle), "{needle}: {err}");
        }
    }

    /// The exposure gate as this binary composes it: a non-loopback
    /// `http_listen` is refused with the keyless fallback state AND with the
    /// explicit `console_require_app_auth = false` opt-out; it passes with
    /// `allow_remote`, and with an `auth_config` that trusts a key (enforced
    /// console auth). Loopback always passes.
    #[test]
    fn non_loopback_http_listen_is_refused_without_auth_or_allow_remote() -> Result<(), String> {
        let refusal = |runtime_options: Value| -> Result<String, String> {
            let options = parse_gateway_runtime_options(
                &json!({ "runtime_options": runtime_options }),
                None,
            )?;
            Ok(http_bind_gate(&options)
                .err()
                .map(|error| error.to_string())
                .unwrap_or_default())
        };

        let message = refusal(json!({ "http_listen": "0.0.0.0:8080" }))?;
        assert!(message.contains("0.0.0.0:8080"), "{message}");
        assert!(message.contains("allow_remote"), "{message}");
        let message = refusal(json!({
            "http_listen": "0.0.0.0:8080",
            "console_require_app_auth": false
        }))?;
        assert!(message.contains("allow_remote"), "{message}");

        assert_eq!(
            refusal(json!({ "http_listen": "0.0.0.0:8080", "allow_remote": true }))?,
            ""
        );
        assert_eq!(
            refusal(json!({
                "http_listen": "0.0.0.0:8080",
                "auth_config": {
                    "provider": "jwt",
                    "shared_secret": "test-shared-secret-with-enough-bytes"
                }
            }))?,
            ""
        );
        assert_eq!(refusal(json!({ "http_listen": "127.0.0.1:8080" }))?, "");
        assert_eq!(refusal(json!({}))?, "");
        Ok(())
    }

    #[test]
    fn no_auth_config_fallback_is_closed_to_every_caller() {
        let state = minimal_decision_state();

        assert!(state.console.require_app_auth);
        assert_eq!(
            ConsoleAuthPosture::of(&state),
            ConsoleAuthPosture::ClosedToEveryCaller
        );
    }

    #[test]
    fn no_auth_config_fallback_keeps_the_sdk_gateway_session_store_naming() {
        let state = minimal_decision_state();

        assert_eq!(state.bigquery.dataset, "default_dataset");
        assert_eq!(state.bigquery.table, "default_table");
        assert!(state.modules.is_empty());
    }

    #[test]
    fn read_only_toggle_alone_still_leaves_the_console_closed() {
        let params = json!({
            "runtime_options": {
                "console_read_only": true
            }
        });

        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");

        let decisions = options.decisions.expect("decisions");
        assert!(decisions.console.read_only);
        assert_eq!(
            ConsoleAuthPosture::of(&decisions),
            ConsoleAuthPosture::ClosedToEveryCaller
        );
    }

    #[test]
    fn jwt_auth_config_console_is_not_closed() {
        let params = json!({
            "runtime_options": {
                "auth_config": {
                    "provider": "jwt",
                    "shared_secret": "test-shared-secret-with-enough-bytes"
                }
            }
        });

        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");

        let decisions = options.decisions.expect("decisions");
        assert!(decisions.console.require_app_auth);
        assert_eq!(
            ConsoleAuthPosture::of(&decisions),
            ConsoleAuthPosture::Enforced
        );
    }

    #[test]
    fn gateway_runtime_options_can_enable_read_only_console() {
        let params = json!({
            "runtime_options": {
                "console_read_only": true
            }
        });

        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");

        assert!(options.decisions.expect("decisions").console.read_only);
    }

    #[test]
    fn gateway_runtime_options_parse_console_fetch_timeout_ms() {
        let params = json!({
            "runtime_options": {
                "console_fetch_timeout_ms": 120_000
            }
        });

        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");

        assert_eq!(
            options
                .decisions
                .expect("decisions")
                .console
                .fetch_timeout_ms,
            Some(120_000)
        );
    }

    #[test]
    fn gateway_runtime_options_reject_zero_console_fetch_timeout_ms() {
        let params = json!({
            "runtime_options": {
                "console_fetch_timeout_ms": 0
            }
        });

        let err = match parse_gateway_runtime_options(&params, None) {
            Ok(_) => panic!("zero should fail"),
            Err(err) => err,
        };

        assert!(err.contains("runtime_options.console_fetch_timeout_ms"));
    }

    #[test]
    fn gateway_runtime_options_parse_demo_llm() {
        let params = json!({
            "runtime_options": {
                "demo_llm": true
            }
        });

        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");

        assert!(options.demo_llm);
    }

    #[test]
    fn gateway_runtime_options_parse_member_comms_address() {
        let params = json!({
            "runtime_options": {
                "member_comms_address": "127.0.0.1:0"
            }
        });

        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");

        assert_eq!(options.member_comms_address.as_deref(), Some("127.0.0.1:0"));
        let config = gateway_agent_config(&options);
        assert_eq!(config.comms.mode, meerkat_core::CommsRuntimeMode::Tcp);
        assert_eq!(config.comms.address.as_deref(), Some("127.0.0.1:0"));
    }

    #[test]
    fn gateway_runtime_options_parse_max_sessions() {
        let params = json!({
            "runtime_options": {
                "max_sessions": 320
            }
        });

        let options = parse_gateway_runtime_options(&params, None).expect("runtime options");

        assert_eq!(options.max_sessions, 320);
    }

    /// Task #62 (downstream app field ask): the build callback must carry the
    /// mint-vs-resume signal so a host can append standing instructions on
    /// MINT and inherit on RESUME. Measured field gap: both boots printed
    /// session_id=None resume_session_id=None, so every host composing
    /// instructions in build_agent silently accreted one durable System row
    /// per boot.
    #[test]
    fn callback_build_agent_options_carry_resume_session_identity() {
        let resumed_session = meerkat_core::Session::new();
        let resumed_id = resumed_session.id().to_string();
        let build = meerkat_core::service::SessionBuildOptions {
            resume_session: Some(resumed_session),
            ..Default::default()
        };
        let req = CreateSessionRequest {
            model: "m".to_string(),
            prompt: meerkat_core::ContentInput::Text("noop".to_string()),
            system_prompt: meerkat_core::config::SystemPromptOverride::Inherit,
            max_tokens: None,
            event_tx: None,
            initial_turn: meerkat_core::service::InitialTurnPolicy::Defer,
            build: Some(build),
            labels: None,
            deferred_prompt_policy: meerkat_core::service::DeferredPromptPolicy::default(),
            injected_context: Vec::new(),
        };

        let options = callback_build_agent_options(&req, "build-test");
        assert_eq!(options["resume_session_id"], resumed_id.as_str());
        assert_eq!(
            options["session_id"],
            resumed_id.as_str(),
            "session identity falls back to the resumed session when labels lack it"
        );

        let mint_req = CreateSessionRequest {
            model: "m".to_string(),
            prompt: meerkat_core::ContentInput::Text("noop".to_string()),
            system_prompt: meerkat_core::config::SystemPromptOverride::Inherit,
            max_tokens: None,
            event_tx: None,
            initial_turn: meerkat_core::service::InitialTurnPolicy::Defer,
            build: None,
            labels: None,
            deferred_prompt_policy: meerkat_core::service::DeferredPromptPolicy::default(),
            injected_context: Vec::new(),
        };
        let mint_options = callback_build_agent_options(&mint_req, "build-test");
        assert!(
            mint_options["resume_session_id"].is_null(),
            "a mint build carries no resume identity"
        );
    }

    #[test]
    fn callback_build_agent_options_include_profile_name_from_spawn_labels() {
        let build = meerkat_core::service::SessionBuildOptions {
            peer_meta: Some(
                meerkat_core::PeerMeta::default()
                    .with_label("role", "security")
                    .with_label("agent_identity", "domain:security"),
            ),
            ..Default::default()
        };
        let req = CreateSessionRequest {
            model: "security-model".to_string(),
            prompt: meerkat_core::ContentInput::Text("noop".to_string()),
            system_prompt: meerkat_core::config::SystemPromptOverride::Inherit,
            max_tokens: None,
            event_tx: None,
            initial_turn: meerkat_core::service::InitialTurnPolicy::Defer,
            build: Some(build),
            labels: None,
            deferred_prompt_policy: meerkat_core::service::DeferredPromptPolicy::default(),
            injected_context: Vec::new(),
        };

        let options = callback_build_agent_options(&req, "build-test");

        assert_eq!(options["scope_id"], "build-test");
        assert_eq!(
            options["profile_name"], "security",
            "SDK build_agent must see the adopted roster profile instead of \
             falling back to checkpoint-local defaults"
        );
        assert_eq!(options["labels"]["agent_identity"], "domain:security");
    }

    #[test]
    fn callback_build_agent_options_merge_request_and_spawn_labels() {
        let build = meerkat_core::service::SessionBuildOptions {
            peer_meta: Some(
                meerkat_core::PeerMeta::default()
                    .with_label("role", "security")
                    .with_label("agent_identity", "domain:security"),
            ),
            ..Default::default()
        };
        let mut request_labels = BTreeMap::new();
        request_labels.insert("session_id".to_string(), "session-123".to_string());
        let req = CreateSessionRequest {
            model: "security-model".to_string(),
            prompt: meerkat_core::ContentInput::Text("noop".to_string()),
            system_prompt: meerkat_core::config::SystemPromptOverride::Inherit,
            max_tokens: None,
            event_tx: None,
            initial_turn: meerkat_core::service::InitialTurnPolicy::Defer,
            build: Some(build),
            labels: Some(request_labels),
            deferred_prompt_policy: meerkat_core::service::DeferredPromptPolicy::default(),
            injected_context: Vec::new(),
        };

        let options = callback_build_agent_options(&req, "build-test");

        assert_eq!(options["profile_name"], "security");
        assert_eq!(options["session_id"], "session-123");
        assert_eq!(options["labels"]["session_id"], "session-123");
        assert_eq!(options["labels"]["agent_identity"], "domain:security");
    }

    fn callback_test_request(
        build: Option<meerkat_core::service::SessionBuildOptions>,
    ) -> CreateSessionRequest {
        CreateSessionRequest {
            model: "m".to_string(),
            prompt: meerkat_core::ContentInput::Text("noop".to_string()),
            system_prompt: meerkat_core::config::SystemPromptOverride::Inherit,
            max_tokens: None,
            event_tx: None,
            initial_turn: meerkat_core::service::InitialTurnPolicy::Defer,
            build,
            labels: None,
            deferred_prompt_policy: meerkat_core::service::DeferredPromptPolicy::default(),
            injected_context: Vec::new(),
        }
    }

    const CALENDAR_SOURCE_SESSION: &str = "0192f5c4-7a3e-7d21-9b0e-4c1d2e3f4a5b";

    /// The lineage meerkat stamps on a fork of MobKit's `domain:calendar`: the
    /// source member is named by its ENCODED roster id, as in a MobKit mob.
    fn calendar_fork_source() -> meerkat_core::service::ForkBuildSource {
        meerkat_core::service::ForkBuildSource::new(
            meerkat_core::MobMemberBinding {
                mob_id: "main".to_string(),
                role: "domain".to_string(),
                member: meerkat_mobkit::member_comms_id::roster_member_id_for_identity(
                    "domain:calendar",
                )
                .as_str()
                .to_string(),
            },
            meerkat_core::SessionId::parse(CALENDAR_SOURCE_SESSION).expect("session id"),
        )
    }

    /// meerkat 0.8.45 `SessionBuildOptions::fork_source` reaches the host
    /// build callback as the same JSON value meerkat serializes (every field,
    /// nothing added), with the source's durable identity beside it. The
    /// child's own session and labels are what name the child.
    #[test]
    fn callback_build_agent_options_carry_fork_lineage_exactly() {
        let source = calendar_fork_source();
        let child_session = meerkat_core::Session::new();
        let child_session_id = child_session.id().to_string();
        let build = meerkat_core::service::SessionBuildOptions {
            fork_source: Some(source.clone()),
            resume_session: Some(child_session),
            peer_meta: Some(
                meerkat_core::PeerMeta::default()
                    .with_label("role", "domain")
                    .with_label("team", "main"),
            ),
            ..Default::default()
        };

        let options = callback_build_agent_options(&callback_test_request(Some(build)), "b");

        assert_eq!(
            options["fork_source"],
            serde_json::to_value(&source).expect("serialize fork source"),
            "fork_source is forwarded as meerkat serializes it"
        );
        assert_eq!(
            options["fork_source"],
            json!({
                "source_member": {
                    "mob_id": "main",
                    "role": "domain",
                    "member": "mk--domain_ccalendar"
                },
                "source_session_id": CALENDAR_SOURCE_SESSION
            })
        );
        assert_eq!(options["fork_source_identity"], "domain:calendar");
        assert_eq!(options["resume_session_id"], child_session_id.as_str());
        assert_eq!(options["session_id"], child_session_id.as_str());
        assert_ne!(child_session_id, CALENDAR_SOURCE_SESSION);
        assert_eq!(options["labels"], json!({"role": "domain", "team": "main"}));

        for build in [
            None,
            Some(meerkat_core::service::SessionBuildOptions::default()),
        ] {
            let options = callback_build_agent_options(&callback_test_request(build), "b");
            assert!(
                options["fork_source"].is_null() && options["fork_source_identity"].is_null(),
                "an ordinary build carries no fork lineage: {options}"
            );
        }
    }

    /// Fork lineage is not caller-mintable: a `callback/build_agent` response
    /// naming `fork_source` neither sets it on an ordinary build nor alters a
    /// fork's, while the rest of the response still applies.
    /// meerkat #1225: a spawn seats its pre-assigned session id as a typed
    /// Mint carrier. The build callback must not present it as a resume
    /// (`resume_session_id` stays null, the id still rides as `session_id`,
    /// `session_build_intent` says `mint`), and standing instructions the host
    /// returns are applied, as on any mint. A typed Resume keeps both.
    #[tokio::test]
    async fn a_pre_assigned_mint_is_not_a_resume() {
        let minted_id = meerkat_core::types::SessionId::new();
        let mut mint = meerkat_core::service::SessionBuildOptions::default();
        mint.mint_session_with_id(minted_id.clone());
        let mint_req = callback_test_request(Some(mint));
        let options = callback_build_agent_options(&mint_req, "build-test");
        assert!(options["resume_session_id"].is_null(), "{options}");
        assert_eq!(options["session_id"], minted_id.to_string());
        assert_eq!(options["session_build_intent"], "mint");

        let mut resume = meerkat_core::service::SessionBuildOptions::default();
        let resumed_session = meerkat_core::Session::new();
        let resumed_id = resumed_session.id().to_string();
        resume.resume_existing_session(resumed_session);
        let resume_req = callback_test_request(Some(resume));
        let options = callback_build_agent_options(&resume_req, "build-test");
        assert_eq!(options["resume_session_id"], resumed_id.as_str());
        assert_eq!(options["session_build_intent"], "resume");

        let tmp = tempfile::tempdir().expect("temp dir");
        let (stdout_tx, _stdout_rx) = mpsc::channel(1);
        let builder = StdioCallbackAgentBuilder {
            inner: FactoryAgentBuilder::new(AgentFactory::new(tmp.path()), Config::default()),
            bridge: StdioCallbackBridge::new(stdout_tx),
            has_session_builder: true,
            session_store: None,
            detached_jobs: None,
        };
        let response = json!({"additional_instructions": ["be terse"]});
        let applied = builder
            .apply_build_agent_response(
                &mint_req,
                mint_req
                    .build
                    .as_ref()
                    .and_then(|build| build.external_tools.clone()),
                &response,
                "b",
            )
            .await
            .expect("response applies");
        assert_eq!(
            applied
                .build
                .as_ref()
                .and_then(|b| b.additional_instructions.clone()),
            Some(vec!["be terse".to_string()]),
            "a Mint carrier honors standing instructions"
        );
        let applied = builder
            .apply_build_agent_response(
                &resume_req,
                resume_req
                    .build
                    .as_ref()
                    .and_then(|build| build.external_tools.clone()),
                &response,
                "b",
            )
            .await
            .expect("response applies");
        assert_eq!(
            applied
                .build
                .as_ref()
                .and_then(|b| b.additional_instructions.clone()),
            None,
            "a resume inherits persisted prompt state"
        );
    }

    #[tokio::test]
    async fn a_build_response_cannot_set_or_alter_fork_lineage() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let (stdout_tx, _stdout_rx) = mpsc::channel(1);
        let builder = StdioCallbackAgentBuilder {
            inner: FactoryAgentBuilder::new(AgentFactory::new(tmp.path()), Config::default()),
            bridge: StdioCallbackBridge::new(stdout_tx),
            has_session_builder: true,
            session_store: None,
            detached_jobs: None,
        };
        let spoofing_response = json!({
            "fork_source": {
                "source_member": {"mob_id": "other", "role": "admin", "member": "mk--root"},
                "source_session_id": "0192f5c4-0000-7000-8000-000000000000"
            },
            "fork_source_identity": "root",
            "labels": {"note": "applied"}
        });

        let requests = [
            (
                callback_test_request(Some(meerkat_core::service::SessionBuildOptions {
                    fork_source: Some(calendar_fork_source()),
                    ..Default::default()
                })),
                Some(calendar_fork_source()),
            ),
            (
                callback_test_request(Some(meerkat_core::service::SessionBuildOptions::default())),
                None,
            ),
            (callback_test_request(None), None),
        ];
        for (req, expected) in requests {
            let applied = builder
                .apply_build_agent_response(
                    &req,
                    req.build
                        .as_ref()
                        .and_then(|build| build.external_tools.clone()),
                    &spoofing_response,
                    "b",
                )
                .await
                .expect("response applies");
            assert_eq!(
                applied.build.as_ref().and_then(|b| b.fork_source.clone()),
                expected,
                "the response must not set or alter fork lineage"
            );
            assert_eq!(
                applied
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("note"))
                    .map(String::as_str),
                Some("applied"),
                "the rest of the response still applies"
            );
        }
    }

    const FORK_SCENARIO_MOB: &str = r#"
[mob]
id = "fork-lineage-scenario"

[profiles.domain]
model = "gpt-5.5"
external_addressable = true
runtime_mode = "turn_driven"

[profiles.domain.tools]
comms = true
"#;

    /// A scripted SDK host: answers every `callback/build_agent` with
    /// `response` and records the options it was sent, in order.
    fn answer_build_callbacks(
        mut stdout_rx: mpsc::Receiver<GatewayStdoutLine>,
        bridge: StdioCallbackBridge,
        response: Value,
    ) -> (
        Arc<std::sync::Mutex<Vec<Value>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let task = tokio::spawn(async move {
            while let Some(mut line) = stdout_rx.recv().await {
                let request: Value = serde_json::from_str(&line).expect("callback JSON");
                line.settle_delivery(true).await;
                if request["method"] == "callback/build_agent" {
                    sink.lock()
                        .expect("build log")
                        .push(request["params"]["options"].clone());
                    bridge
                        .route_callback_response(json!({
                            "jsonrpc": "2.0",
                            "id": request["id"],
                            "result": response,
                        }))
                        .await;
                }
            }
        });
        (seen, task)
    }

    struct ForkScenarioRuntime {
        runtime: UnifiedRuntime,
        builds: Arc<std::sync::Mutex<Vec<Value>>>,
        responder: tokio::task::JoinHandle<()>,
    }

    impl ForkScenarioRuntime {
        /// The gateway's persistent callback composition (a
        /// `StdioCallbackAgentBuilder` inside a `PersistentSessionService`,
        /// durable session projection, persistent mob storage) over `state`,
        /// with a scripted model and host.
        async fn boot(
            state: &std::path::Path,
            blob_store: Arc<dyn meerkat_core::BlobStore>,
            response: Value,
        ) -> Self {
            let session_store: Arc<dyn meerkat::SessionStore> = Arc::new(
                meerkat_store::SqliteSessionStore::open(state.join("session-store.sqlite3"))
                    .expect("session store"),
            );
            let runtime_store: Arc<dyn meerkat_runtime::RuntimeStore> = Arc::new(
                meerkat_runtime::store::SqliteRuntimeStore::new(
                    state.join("runtime-store.sqlite3"),
                )
                .expect("runtime store"),
            );
            let (runtime_store, epochs) =
                meerkat_mobkit::mob_handle_runtime::epoch_tracking_runtime_store_with_durable_projection(
                    runtime_store,
                    session_store.clone(),
                );
            let adapter = Arc::new(
                meerkat_runtime::MeerkatMachine::persistent(
                    Arc::clone(&runtime_store),
                    Arc::clone(&blob_store),
                )
                .expect("acquire the RPC gateway fixture runtime machine"),
            );
            let factory = AgentFactory::new(state).builtins(false).comms(true);
            let mut inner = FactoryAgentBuilder::new(factory, Config::default());
            inner.default_session_store = Some(Arc::new(meerkat_store::StoreAdapter::new(
                session_store.clone(),
            )));
            inner.default_blob_store = Some(Arc::clone(&blob_store));
            let (stdout_tx, stdout_rx) = mpsc::channel(16);
            let bridge = StdioCallbackBridge::new(stdout_tx);
            let (builds, responder) = answer_build_callbacks(stdout_rx, bridge.clone(), response);
            let service = Arc::new(meerkat_session::PersistentSessionService::new(
                StdioCallbackAgentBuilder {
                    inner,
                    bridge,
                    has_session_builder: true,
                    session_store: Some(session_store.clone()),
                    detached_jobs: None,
                },
                16,
                session_store,
                Arc::clone(&runtime_store),
                blob_store,
            ));
            let (mob_storage, provenance) =
                meerkat_mobkit::mob_composition_manifest::persistent_mob_storage(
                    state.join("mob.sqlite3"),
                )
                .expect("mob storage");
            let mut spec = MobBootstrapSpec::new(
                MobDefinition::from_toml(FORK_SCENARIO_MOB).expect("mob definition"),
                mob_storage,
                service,
            )
            .with_mob_storage_provenance(provenance)
            .with_session_write_epochs(&epochs)
            .with_runtime_authority_prewarm(&runtime_store)
            .with_runtime_archived_terminal_authority(Arc::clone(&runtime_store))
            .with_session_runtime_adapter(adapter.clone())
            .expect("acquire the RPC gateway fixture session runtime owner")
            .with_options(MobBootstrapOptions {
                allow_ephemeral_sessions: true,
                notify_orchestrator_on_resume: true,
                default_llm_client: Some(Arc::new(meerkat_client::TestClient::default())),
            });
            spec.runtime_adapter = Some(adapter);
            let runtime = Box::pin(
                UnifiedRuntime::builder()
                    .mob_spec(spec)
                    .module_config(MobKitConfig {
                        modules: Vec::new(),
                        discovery: DiscoverySpec {
                            namespace: "fork-lineage-scenario".to_string(),
                            modules: Vec::new(),
                        },
                        pre_spawn: Vec::new(),
                    })
                    .timeout(Duration::from_secs(10))
                    .build(),
            )
            .await
            .expect("boot runtime");
            Self {
                runtime,
                builds,
                responder,
            }
        }

        fn builds(&self) -> Vec<Value> {
            self.builds.lock().expect("build log").clone()
        }

        async fn stop(self) {
            let _ = Box::pin(self.runtime.shutdown()).await;
            self.responder.abort();
        }
    }

    /// The real path: MobKit's durable `fork_member` seats a child whose
    /// `callback/build_agent` carries its source's lineage, and a gateway
    /// restart rebuilds the child with the same lineage. The host answers
    /// every build with a response that tries to rewrite `fork_source`; the
    /// rebuild proves meerkat's persisted lineage never took it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_fork_and_its_rebuild_reach_the_build_callback_with_the_source_lineage() {
        let temp = tempfile::tempdir().expect("temp dir");
        let state = temp.path().join("state");
        std::fs::create_dir_all(&state).expect("state dir");
        let blob_store: Arc<dyn meerkat_core::BlobStore> =
            Arc::new(meerkat_store::MemoryBlobStore::new());
        let spoofing_response = json!({
            "fork_source": {
                "source_member": {"mob_id": "other", "role": "admin", "member": "mk--root"},
                "source_session_id": "0192f5c4-0000-7000-8000-000000000000"
            },
            "fork_source_identity": "root"
        });

        let first =
            ForkScenarioRuntime::boot(&state, Arc::clone(&blob_store), spoofing_response.clone())
                .await;
        let source_id =
            meerkat_mobkit::member_comms_id::roster_member_id_for_identity("domain:calendar");
        let mut source_spec = meerkat_mob::SpawnMemberSpec::new("domain", source_id.clone());
        source_spec.labels = Some(BTreeMap::from([
            ("agent_identity".to_string(), "domain:calendar".to_string()),
            ("team".to_string(), "main".to_string()),
        ]));
        Box::pin(first.runtime.mob_handle().spawn_spec(source_spec))
            .await
            .expect("spawn the source");
        let source_session_id = first
            .runtime
            .mob_handle()
            .get_member(&source_id)
            .await
            .expect("read the source")
            .and_then(|entry| entry.bridge_session_id().cloned())
            .expect("the source has a session")
            .to_string();

        let outcome = Box::pin(first.runtime.fork_member(
            "domain:calendar",
            meerkat_mob::SpawnMemberSpec::new("domain", "domain:calendar-fork"),
            None,
        ))
        .await
        .expect("durable fork");
        let child_session_id = outcome.result.session_id.to_string();

        let expected_lineage = json!({
            "source_member": {
                "mob_id": "fork-lineage-scenario",
                "role": "domain",
                "member": "mk--domain_ccalendar"
            },
            "source_session_id": source_session_id
        });
        let builds = first.builds();
        let source_builds: Vec<&Value> = builds
            .iter()
            .filter(|options| options["fork_source"].is_null())
            .collect();
        assert!(
            !source_builds.is_empty(),
            "the source was built: {builds:?}"
        );
        assert!(
            source_builds
                .iter()
                .all(|options| options["fork_source_identity"].is_null()),
            "an ordinary build carries no lineage: {builds:?}"
        );
        // meerkat stamps `agent_identity` on every member build; compare the
        // child with the value the SOURCE's build actually carried.
        let source_agent_identity = source_builds[0]["labels"]["agent_identity"].clone();
        assert!(
            source_agent_identity.is_string(),
            "the source build names its agent_identity: {builds:?}"
        );
        let assert_child_build = |options: &Value, when: &str| {
            assert_eq!(
                options["fork_source"], expected_lineage,
                "{when}: {options}"
            );
            assert_eq!(options["fork_source_identity"], "domain:calendar", "{when}");
            assert_eq!(
                options["resume_session_id"],
                child_session_id.as_str(),
                "{when}: the child builds on its own session"
            );
            assert_ne!(child_session_id, source_session_id);
            assert_ne!(
                options["labels"]["agent_identity"], source_agent_identity,
                "{when}: the child never carries the source's agent_identity: {options}"
            );
        };
        let child_builds: Vec<&Value> = builds
            .iter()
            .filter(|options| !options["fork_source"].is_null())
            .collect();
        assert_eq!(
            child_builds.len(),
            1,
            "one seating build for the child: {builds:?}"
        );
        assert_child_build(child_builds[0], "fork seating");
        first.stop().await;

        let second = ForkScenarioRuntime::boot(&state, blob_store, spoofing_response).await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let rebuild = loop {
            if let Some(options) = second
                .builds()
                .into_iter()
                .find(|options| !options["fork_source"].is_null())
            {
                break options;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the restart never rebuilt the fork child: {:?}",
                second.builds()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_child_build(&rebuild, "rebuild after restart");
        second.stop().await;
    }

    #[test]
    fn gateway_runtime_options_parse_agent_memory_defaults() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({
            "runtime_options": {
                "agent_memory": true
            }
        });

        let options =
            parse_gateway_runtime_options(&params, Some(tmp.path())).expect("runtime options");
        let agent_memory = options.agent_memory.expect("agent memory options");

        assert_eq!(agent_memory.config.realm, "default");
        assert_eq!(
            agent_memory.config.selection,
            meerkat_mobkit::AgentMemorySelection::Contextual
        );
        assert_eq!(agent_memory.config.recall_timeout_ms, 500);
        assert_eq!(
            agent_memory.config.recall_failure_policy,
            meerkat_mobkit::AgentMemoryRecallFailurePolicy::Skip
        );
        assert!(agent_memory.config.defang_inbound);
        assert_eq!(agent_memory.path, tmp.path().join("agent-memory"));
    }

    /// The downstream app's exact call shape (`.agent_memory(selection="contextual",
    /// max_entries=3)`), which for two months yielded 10,283 build-surface
    /// injections and zero turn-surface rows: the object form omitted
    /// `per_turn_injection` and the gateway supplied its own "off" while the
    /// library default was Budgeted. The gateway must not carry a default of
    /// its own.
    #[test]
    fn gateway_runtime_options_per_turn_injection_omitted_uses_the_library_default() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({
            "runtime_options": {
                "agent_memory": { "selection": "contextual", "max_entries": 3 }
            }
        });

        let options =
            parse_gateway_runtime_options(&params, Some(tmp.path())).expect("runtime options");
        let agent_memory = options.agent_memory.expect("agent memory options");

        // One default per policy.
        assert_eq!(
            agent_memory.config.per_turn_injection,
            meerkat_mobkit::AgentMemoryPerTurnInjection::default()
        );
        // And that default is the documented one. If this line goes red the
        // library default moved; the CHANGELOG owes consumers a line, not this
        // test a weaker assertion.
        assert_eq!(
            agent_memory.config.per_turn_injection,
            meerkat_mobkit::AgentMemoryPerTurnInjection::Budgeted
        );
        // The boolean form must agree with the object form.
        let boolean = parse_gateway_runtime_options(
            &json!({ "runtime_options": { "agent_memory": true } }),
            Some(tmp.path()),
        )
        .expect("runtime options")
        .agent_memory
        .expect("agent memory options");
        assert_eq!(
            boolean.config.per_turn_injection,
            agent_memory.config.per_turn_injection
        );
    }

    #[test]
    fn gateway_runtime_options_per_turn_injection_omitted_on_markdown_derives_off() {
        // The store cannot carry budgeted injection, and init refuses markdown
        // anyway; an OMITTED field must still parse (so the refusal names the
        // migration) and must not be read as a request for budgeted.
        let tmp = tempfile::tempdir().expect("temp dir");
        let options = parse_gateway_runtime_options(
            &json!({ "runtime_options": { "agent_memory": { "store": "markdown" } } }),
            Some(tmp.path()),
        )
        .expect("markdown with omitted per_turn_injection must parse");
        assert_eq!(
            options
                .agent_memory
                .expect("agent memory options")
                .config
                .per_turn_injection,
            meerkat_mobkit::AgentMemoryPerTurnInjection::Off
        );
    }

    #[test]
    fn gateway_runtime_options_per_turn_injection_explicit_values_and_refusals() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let parse = |value: serde_json::Value| {
            parse_gateway_runtime_options(
                &json!({ "runtime_options": { "agent_memory": { "per_turn_injection": value } } }),
                Some(tmp.path()),
            )
        };
        assert_eq!(
            parse(json!("off"))
                .expect("off parses")
                .agent_memory
                .expect("agent memory")
                .config
                .per_turn_injection,
            meerkat_mobkit::AgentMemoryPerTurnInjection::Off
        );
        assert_eq!(
            parse(json!(" budgeted "))
                .expect("budgeted parses, trimmed")
                .agent_memory
                .expect("agent memory")
                .config
                .per_turn_injection,
            meerkat_mobkit::AgentMemoryPerTurnInjection::Budgeted
        );
        let Err(unknown) = parse(json!("sometimes")) else {
            panic!("an unknown per_turn_injection value must be refused");
        };
        assert!(unknown.contains("got 'sometimes'"), "{unknown}");
        // A non-string used to fall through `as_str()` to the literal default and
        // silently mean "off"; it is now a typed refusal naming the two values.
        let Err(non_string) = parse(json!(true)) else {
            panic!("a non-string per_turn_injection must be refused, not read as off");
        };
        assert!(non_string.contains("must be a string"), "{non_string}");
    }

    #[test]
    fn agent_memory_census_slot_covers_both_store_kinds() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({ "runtime_options": { "agent_memory": true } });
        let options =
            parse_gateway_runtime_options(&params, Some(tmp.path())).expect("runtime options");
        let slot = agent_memory_census_slot(&options.agent_memory.expect("agent memory options"));
        assert_eq!(slot.durability.domain(), "agent-memory");
        assert_eq!(
            slot.durability
                .declaration()
                .expect("declared slot")
                .resolution,
            meerkat_core::DurabilityResolution::Persistent
        );
        assert_eq!(slot.backend, "SqliteAgentMemoryStore");
        assert!(!slot.degraded);

        let params = json!({ "runtime_options": { "agent_memory": { "store": "markdown" } } });
        let options =
            parse_gateway_runtime_options(&params, Some(tmp.path())).expect("runtime options");
        let slot = agent_memory_census_slot(&options.agent_memory.expect("agent memory options"));
        assert_eq!(slot.durability.domain(), "agent-memory");
        assert_eq!(slot.backend, "MarkdownImportOnly");
    }

    /// The live markdown backend is retired, but the refusal has to be a
    /// MIGRATION verdict, not a shrug: it must name the target store and the
    /// lossless import that gets a deployment there. Pinned so the message
    /// cannot decay into "unsupported store".
    #[test]
    fn markdown_live_store_refusal_names_the_migration() {
        let migration = AgentMemoryStoreMigration::MarkdownIsImportOnly;
        assert_eq!(migration.code(), STORAGE_RESOLUTION_CODE);
        let message = migration.message();
        assert!(message.contains("store='sqlite'"), "{message}");
        assert!(message.contains("SAME agent-memory directory"), "{message}");
        assert!(message.contains(".md.imported"), "{message}");
    }

    /// The store kind still PARSES as markdown; only the live construction
    /// path refuses. Keeping the parse arm is what makes the refusal say
    /// "migrate" instead of "unknown value 'markdown'".
    #[test]
    fn markdown_store_kind_still_parses() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({ "runtime_options": { "agent_memory": { "store": "markdown" } } });
        let options =
            parse_gateway_runtime_options(&params, Some(tmp.path())).expect("runtime options");
        assert_eq!(
            options.agent_memory.expect("agent memory options").store,
            GatewayAgentMemoryStoreKind::Markdown
        );
    }

    #[test]
    fn gateway_runtime_options_parse_agent_memory_defang_inbound() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({
            "runtime_options": {
                "agent_memory": { "defang_inbound": false }
            }
        });

        let options =
            parse_gateway_runtime_options(&params, Some(tmp.path())).expect("runtime options");
        let agent_memory = options.agent_memory.expect("agent memory options");
        assert!(!agent_memory.config.defang_inbound);

        let params = json!({
            "runtime_options": {
                "agent_memory": { "defang_inbound": "yes" }
            }
        });
        let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
            Ok(_) => panic!("non-boolean defang_inbound should fail loudly"),
            Err(err) => err,
        };
        assert!(err.contains("defang_inbound"), "{err}");
    }

    #[test]
    fn gateway_runtime_options_parse_agent_memory_recall_policy() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({
            "runtime_options": {
                "agent_memory": {
                    "recall_timeout_ms": 1200,
                    "recall_failure_policy": "fail"
                }
            }
        });

        let options =
            parse_gateway_runtime_options(&params, Some(tmp.path())).expect("runtime options");
        let agent_memory = options.agent_memory.expect("agent memory options");

        assert_eq!(agent_memory.config.recall_timeout_ms, 1200);
        assert_eq!(
            agent_memory.config.recall_failure_policy,
            meerkat_mobkit::AgentMemoryRecallFailurePolicy::Fail
        );
    }

    #[test]
    fn gateway_runtime_options_reject_non_boolean_agent_memory_enabled() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({
            "runtime_options": {
                "agent_memory": {
                    "enabled": "false"
                }
            }
        });

        let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
            Ok(_) => panic!("non-boolean agent memory enabled should fail"),
            Err(err) => err,
        };

        assert!(err.contains("enabled"), "{err}");
    }

    #[test]
    fn gateway_runtime_options_reject_agent_memory_without_state_path() {
        let params = json!({
            "runtime_options": {
                "agent_memory": true
            }
        });

        let err = match parse_gateway_runtime_options(&params, None) {
            Ok(_) => panic!("agent memory without path should fail"),
            Err(err) => err,
        };

        assert!(err.contains("agent_memory"), "{err}");
    }

    #[test]
    fn gateway_runtime_options_reject_agent_memory_path_override() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({
            "runtime_options": {
                "agent_memory": {
                    "path": "/tmp/other-agent-memory"
                }
            }
        });

        let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
            Ok(_) => panic!("agent memory path override should fail"),
            Err(err) => err,
        };

        assert!(err.contains("path"), "{err}");
    }

    #[test]
    fn gateway_runtime_options_agent_memory_store_defaults_to_sqlite() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({
            "runtime_options": {
                "agent_memory": true
            }
        });

        let options = parse_gateway_runtime_options(&params, Some(tmp.path()))
            .expect("boolean agent memory config should parse");
        let agent_memory = options.agent_memory.expect("agent memory options");
        assert_eq!(agent_memory.store, GatewayAgentMemoryStoreKind::Sqlite);

        let params = json!({
            "runtime_options": {
                "agent_memory": { "enabled": true }
            }
        });
        let options = parse_gateway_runtime_options(&params, Some(tmp.path()))
            .expect("object agent memory config should parse");
        let agent_memory = options.agent_memory.expect("agent memory options");
        assert_eq!(agent_memory.store, GatewayAgentMemoryStoreKind::Sqlite);
    }

    #[test]
    fn gateway_runtime_options_agent_memory_store_accepts_markdown() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({
            "runtime_options": {
                "agent_memory": { "store": "markdown" }
            }
        });

        let options = parse_gateway_runtime_options(&params, Some(tmp.path()))
            .expect("markdown store config should parse");
        let agent_memory = options.agent_memory.expect("agent memory options");
        assert_eq!(agent_memory.store, GatewayAgentMemoryStoreKind::Markdown);
        assert_eq!(agent_memory.path, tmp.path().join("agent-memory"));
    }

    #[test]
    fn gateway_runtime_options_agent_memory_store_rejects_unknown_backend() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({
            "runtime_options": {
                "agent_memory": { "store": "postgres" }
            }
        });

        let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
            Ok(_) => panic!("unknown store backend should fail loudly"),
            Err(err) => err,
        };

        assert!(err.contains("'markdown' or 'sqlite'"), "{err}");
    }

    #[test]
    fn gateway_runtime_options_agent_memory_budgeted_injection_requires_sqlite() {
        // The §9.1 compaction budget-reset sink is wired only in the sqlite
        // arm; markdown + budgeted would silently stop injecting once the
        // session budget is spent — the config must fail loudly instead.
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({
            "runtime_options": {
                "agent_memory": { "store": "markdown", "per_turn_injection": "budgeted" }
            }
        });
        let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
            Ok(_) => panic!("markdown + budgeted injection should fail loudly"),
            Err(err) => err,
        };
        assert!(
            err.contains("per_turn_injection='budgeted' requires store='sqlite'"),
            "{err}"
        );

        // Both knobs stay valid apart: sqlite + budgeted, markdown + off.
        let params = json!({
            "runtime_options": {
                "agent_memory": { "per_turn_injection": "budgeted" }
            }
        });
        let options = parse_gateway_runtime_options(&params, Some(tmp.path()))
            .expect("sqlite + budgeted should parse");
        assert_eq!(
            options
                .agent_memory
                .expect("agent memory")
                .config
                .per_turn_injection,
            meerkat_mobkit::AgentMemoryPerTurnInjection::Budgeted
        );
        let params = json!({
            "runtime_options": {
                "agent_memory": { "store": "markdown", "per_turn_injection": "off" }
            }
        });
        parse_gateway_runtime_options(&params, Some(tmp.path()))
            .expect("markdown + off should parse");
    }

    #[test]
    fn operator_scope_warning_fires_exactly_when_provisional_without_resolver() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let parse = |scope: &str| {
            let params = json!({
                "runtime_options": {
                    "agent_memory": { "operator_scope": scope }
                }
            });
            parse_gateway_runtime_options(&params, Some(tmp.path()))
                .expect("parse")
                .agent_memory
                .expect("agent memory options")
        };
        let provisional = parse("provisional");
        let off = parse("off");

        assert!(operator_scope_recall_inert(Some(&provisional), false));
        assert!(!operator_scope_recall_inert(Some(&provisional), true));
        assert!(!operator_scope_recall_inert(Some(&off), false));
        assert!(!operator_scope_recall_inert(Some(&off), true));
        assert!(!operator_scope_recall_inert(None, false));
        assert!(!operator_scope_recall_inert(None, true));
    }

    #[test]
    fn gateway_runtime_options_agent_memory_distiller_parse_matrix() {
        let tmp = tempfile::tempdir().expect("temp dir");

        // Default: disabled (flipping the default is a calibration decision).
        let params = json!({ "runtime_options": { "agent_memory": true } });
        let options =
            parse_gateway_runtime_options(&params, Some(tmp.path())).expect("runtime options");
        let agent_memory = options.agent_memory.expect("agent memory options");
        assert!(!agent_memory.distiller.enabled);
        assert_eq!(agent_memory.distiller.runs_per_hour, 12);
        assert_eq!(agent_memory.distiller.min_interactions, 3);
        assert_eq!(agent_memory.distiller.model, None);

        // Full object block.
        let params = json!({
            "runtime_options": {
                "agent_memory": {
                    "distiller": {
                        "enabled": true,
                        "runs_per_hour": 6,
                        "min_interactions": 5,
                        "model": "claude-haiku-4-5"
                    }
                }
            }
        });
        let options =
            parse_gateway_runtime_options(&params, Some(tmp.path())).expect("runtime options");
        let distiller = options.agent_memory.expect("agent memory").distiller;
        assert!(distiller.enabled);
        assert_eq!(distiller.runs_per_hour, 6);
        assert_eq!(distiller.min_interactions, 5);
        assert_eq!(distiller.model.as_deref(), Some("claude-haiku-4-5"));

        // Boolean shorthand + object-without-enabled is an explicit opt-in.
        let params = json!({
            "runtime_options": { "agent_memory": { "distiller": true } }
        });
        let options =
            parse_gateway_runtime_options(&params, Some(tmp.path())).expect("runtime options");
        assert!(
            options
                .agent_memory
                .expect("agent memory")
                .distiller
                .enabled
        );
        let params = json!({
            "runtime_options": { "agent_memory": { "distiller": { "runs_per_hour": 2 } } }
        });
        let options =
            parse_gateway_runtime_options(&params, Some(tmp.path())).expect("runtime options");
        assert!(
            options
                .agent_memory
                .expect("agent memory")
                .distiller
                .enabled
        );

        // Fail-loud: unknown fields, bad types, out-of-range values.
        for (params, needle) in [
            (
                json!({ "runtime_options": { "agent_memory": { "distiller": { "cadence": 5 } } } }),
                "unsupported runtime_options.agent_memory.distiller fields",
            ),
            (
                json!({ "runtime_options": { "agent_memory": { "distiller": "on" } } }),
                "must be a boolean or object",
            ),
            (
                json!({ "runtime_options": { "agent_memory": { "distiller": { "runs_per_hour": 0 } } } }),
                "runs_per_hour must be between 1 and 240",
            ),
            (
                json!({ "runtime_options": { "agent_memory": { "distiller": { "min_interactions": 0 } } } }),
                "min_interactions must be between 1 and 100",
            ),
            (
                json!({ "runtime_options": { "agent_memory": { "distiller": { "model": "" } } } }),
                "model must be a non-empty string",
            ),
        ] {
            let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
                Ok(_) => panic!("expected fail-loud parse for {params}"),
                Err(err) => err,
            };
            assert!(err.contains(needle), "{err}");
        }

        // The markdown store has no manifest/tombstone/authored-write seams.
        let params = json!({
            "runtime_options": {
                "agent_memory": { "store": "markdown", "distiller": { "enabled": true } }
            }
        });
        let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
            Ok(_) => panic!("markdown + distiller should fail loudly"),
            Err(err) => err,
        };
        assert!(err.contains("distiller requires store='sqlite'"), "{err}");
    }

    #[test]
    fn gateway_runtime_options_agent_memory_steward_parse_matrix() {
        let tmp = tempfile::tempdir().expect("tempdir");

        // Defaults: disabled, */6h cadence, per-realm, 4 runs/day.
        let params = json!({ "runtime_options": { "agent_memory": true } });
        let options =
            parse_gateway_runtime_options(&params, Some(tmp.path())).expect("defaults parse");
        let steward = options.agent_memory.expect("agent memory").steward;
        assert!(!steward.enabled);
        assert_eq!(steward.cadence, "*/6h");
        assert!(!steward.per_mob);
        assert_eq!(steward.runs_per_day, 4);
        assert_eq!(steward.min_signals, 3);
        assert_eq!(steward.model, None);

        // Full object form.
        let params = json!({
            "runtime_options": {
                "agent_memory": {
                    "steward": {
                        "enabled": true,
                        "cadence": "*/30m",
                        "model": "claude-sonnet-4-6",
                        "per_mob": true,
                        "runs_per_day": 8,
                        "min_signals": 5
                    }
                }
            }
        });
        let steward = parse_gateway_runtime_options(&params, Some(tmp.path()))
            .expect("object form parses")
            .agent_memory
            .expect("agent memory")
            .steward;
        assert!(steward.enabled);
        assert_eq!(steward.cadence, "*/30m");
        assert_eq!(steward.model.as_deref(), Some("claude-sonnet-4-6"));
        assert!(steward.per_mob);
        assert_eq!(steward.runs_per_day, 8);
        assert_eq!(steward.min_signals, 5);

        // Bare true / bare object are opt-ins.
        let params = json!({
            "runtime_options": { "agent_memory": { "steward": true } }
        });
        assert!(
            parse_gateway_runtime_options(&params, Some(tmp.path()))
                .expect("bool form parses")
                .agent_memory
                .expect("agent memory")
                .steward
                .enabled
        );
        let params = json!({
            "runtime_options": { "agent_memory": { "steward": { "runs_per_day": 2 } } }
        });
        assert!(
            parse_gateway_runtime_options(&params, Some(tmp.path()))
                .expect("object without enabled parses")
                .agent_memory
                .expect("agent memory")
                .steward
                .enabled
        );

        // Fail-loud matrix.
        for (params, needle) in [
            (
                json!({ "runtime_options": { "agent_memory": { "steward": { "tempo": 5 } } } }),
                "unsupported runtime_options.agent_memory.steward fields",
            ),
            (
                json!({ "runtime_options": { "agent_memory": { "steward": "on" } } }),
                "must be a boolean or object",
            ),
            (
                json!({ "runtime_options": { "agent_memory": { "steward": { "cadence": "0 9 * * *" } } } }),
                "not an interval marker",
            ),
            (
                json!({ "runtime_options": { "agent_memory": { "steward": { "cadence": "" } } } }),
                "cadence must be a non-empty string",
            ),
            (
                json!({ "runtime_options": { "agent_memory": { "steward": { "runs_per_day": 0 } } } }),
                "runs_per_day must be between 1 and 96",
            ),
            (
                json!({ "runtime_options": { "agent_memory": { "steward": { "min_signals": 0 } } } }),
                "min_signals must be between 1 and 1000",
            ),
            (
                json!({ "runtime_options": { "agent_memory": { "steward": { "model": "" } } } }),
                "model must be a non-empty string",
            ),
            (
                json!({ "runtime_options": { "agent_memory": { "steward": { "per_mob": "yes" } } } }),
                "per_mob must be a boolean",
            ),
        ] {
            let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
                Ok(_) => panic!("expected fail-loud parse for {params}"),
                Err(err) => err,
            };
            assert!(err.contains(needle), "{err}");
        }

        // Staging/proposals/harvest tables are sqlite-store machinery.
        let params = json!({
            "runtime_options": {
                "agent_memory": { "store": "markdown", "steward": { "enabled": true } }
            }
        });
        let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
            Ok(_) => panic!("markdown + steward should fail loudly"),
            Err(err) => err,
        };
        assert!(err.contains("steward requires store='sqlite'"), "{err}");
    }

    #[test]
    fn gateway_distiller_max_output_tokens_reaches_effective_memory_profile() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let parse = |distiller| {
            parse_gateway_runtime_options(
                &json!({
                    "runtime_options": { "agent_memory": { "distiller": distiller } }
                }),
                Some(tmp.path()),
            )
            .expect("gateway host JSON parses")
            .agent_memory
            .expect("agent memory")
            .distiller
        };

        let default_config = parse(json!(true));
        let default_profile =
            meerkat_mobkit::memory_wiring::effective_distiller_profile(&default_config)
                .expect("memory wiring resolves default profile");
        assert_eq!(
            default_profile.params.max_output_tokens,
            meerkat_mobkit::memory::distiller::DistillerProfile::embedded_default()
                .params
                .max_output_tokens
        );

        let override_config = parse(json!({ "enabled": true, "max_output_tokens": 32_768 }));
        let override_profile =
            meerkat_mobkit::memory_wiring::effective_distiller_profile(&override_config)
                .expect("memory wiring applies host override");
        assert_eq!(override_profile.params.max_output_tokens, 32_768);

        let zero_result = parse_gateway_runtime_options(
            &json!({
                "runtime_options": {
                    "agent_memory": { "distiller": { "max_output_tokens": 0 } }
                }
            }),
            Some(tmp.path()),
        );
        let err = match zero_result {
            Ok(_) => panic!("zero output budget must fail at the host boundary"),
            Err(err) => err,
        };
        assert!(err.contains("must be a positive integer"), "{err}");
    }

    #[test]
    fn gateway_steward_max_output_tokens_reaches_effective_memory_profile() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let parse = |steward| {
            parse_gateway_runtime_options(
                &json!({
                    "runtime_options": { "agent_memory": { "steward": steward } }
                }),
                Some(tmp.path()),
            )
            .expect("gateway host JSON parses")
            .agent_memory
            .expect("agent memory")
            .steward
        };

        let default_config = parse(json!(true));
        let default_profile =
            meerkat_mobkit::memory_wiring::effective_steward_profile(&default_config)
                .expect("memory wiring resolves default profile");
        assert_eq!(
            default_profile.params.max_output_tokens,
            meerkat_mobkit::memory::steward::StewardProfile::embedded_default()
                .params
                .max_output_tokens
        );

        let override_config = parse(json!({ "enabled": true, "max_output_tokens": 65_536 }));
        let override_profile =
            meerkat_mobkit::memory_wiring::effective_steward_profile(&override_config)
                .expect("memory wiring applies host override");
        assert_eq!(override_profile.params.max_output_tokens, 65_536);

        let zero_result = parse_gateway_runtime_options(
            &json!({
                "runtime_options": {
                    "agent_memory": { "steward": { "max_output_tokens": 0 } }
                }
            }),
            Some(tmp.path()),
        );
        let err = match zero_result {
            Ok(_) => panic!("zero output budget must fail at the host boundary"),
            Err(err) => err,
        };
        assert!(err.contains("must be a positive integer"), "{err}");
    }

    #[test]
    fn gateway_hygienist_max_output_tokens_cannot_reach_memory_wiring() {
        let tmp = tempfile::tempdir().expect("tempdir");

        // The retained internal engine still has a bounded default, but the
        // public host has no Hygienist entry in MemoryEnginesConfig.
        assert!(
            meerkat_mobkit::memory::hygienist::HygienistProfile::embedded_default()
                .params
                .max_output_tokens
                > 0
        );

        // Omission is the supported posture.
        let params = json!({ "runtime_options": { "agent_memory": true } });
        parse_gateway_runtime_options(&params, Some(tmp.path())).expect("omission parses");

        // Disabled legacy forms remain accepted, including dormant tuning
        // fields that no longer reach a gateway engine.
        for value in [
            json!(false),
            json!({"enabled": false}),
            json!({
                "enabled": false,
                "runs_per_day": 4,
                "model": "legacy-model",
                "max_output_tokens": 8192
            }),
        ] {
            let params = json!({
                "runtime_options": { "agent_memory": { "hygienist": value } }
            });
            parse_gateway_runtime_options(&params, Some(tmp.path()))
                .expect("disabled compatibility form parses");
        }

        // Every activation-shaped value reaches the same typed refusal. This
        // matrix is mutation-sensitive: changing true to false makes it green.
        for value in [
            json!(true),
            json!({}),
            json!({"enabled": true}),
            json!({"enabled": true, "max_output_tokens": 32_768}),
            json!({"enabled": true, "max_output_tokens": 0}),
            json!({"runs_per_day": 1}),
            json!("on"),
        ] {
            let params = json!({
                "runtime_options": { "agent_memory": { "hygienist": value } }
            });
            let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
                Ok(_) => panic!("parked Hygienist activation must be refused for {params}"),
                Err(err) => err,
            };
            assert!(err.contains("hygienist is PARKED"), "{err}");
            assert!(err.contains("cannot be enabled"), "{err}");
        }

        let refusal = parse_gateway_hygienist_compatibility(&json!(true))
            .expect_err("true is a parked activation request");
        assert_eq!(refusal, ParkedGatewayCapability::Hygienist);
        assert_eq!(refusal.code(), -32602);
    }

    #[test]
    fn gateway_runtime_options_agent_memory_operator_scope_parse_matrix() {
        let tmp = tempfile::tempdir().expect("tempdir");

        // Default: off.
        let params = json!({ "runtime_options": { "agent_memory": true } });
        let options =
            parse_gateway_runtime_options(&params, Some(tmp.path())).expect("defaults parse");
        assert_eq!(
            options
                .agent_memory
                .expect("agent memory")
                .config
                .operator_scope,
            meerkat_mobkit::AgentMemoryOperatorScope::Off
        );

        // Explicit values.
        for (value, expected) in [
            ("off", meerkat_mobkit::AgentMemoryOperatorScope::Off),
            (
                "provisional",
                meerkat_mobkit::AgentMemoryOperatorScope::Provisional,
            ),
        ] {
            let params = json!({
                "runtime_options": { "agent_memory": { "operator_scope": value } }
            });
            let options = parse_gateway_runtime_options(&params, Some(tmp.path()))
                .expect("operator_scope parses");
            assert_eq!(
                options
                    .agent_memory
                    .expect("agent memory")
                    .config
                    .operator_scope,
                expected
            );
        }

        // Fail-loud on unknown values and wrong types.
        for params in [
            json!({ "runtime_options": { "agent_memory": { "operator_scope": "final" } } }),
            json!({ "runtime_options": { "agent_memory": { "operator_scope": true } } }),
        ] {
            let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
                Ok(_) => panic!("expected fail-loud parse for {params}"),
                Err(err) => err,
            };
            assert!(err.contains("operator_scope must be 'off' or"), "{err}");
        }

        // Operator composition/routing is sqlite-store machinery.
        let params = json!({
            "runtime_options": {
                "agent_memory": { "store": "markdown", "operator_scope": "provisional" }
            }
        });
        let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
            Ok(_) => panic!("markdown + operator_scope should fail loudly"),
            Err(err) => err,
        };
        assert!(
            err.contains("operator_scope requires store='sqlite'"),
            "{err}"
        );
    }

    // §7.2 mob-scope binding: the exact resolver the gateway installs on the
    // customizer and injector. The hosting mob composes only for the
    // configured realm — a foreign realm gets no mob scope.
    #[test]
    fn gateway_mob_binding_resolves_hosting_mob_only_for_matching_realm() {
        use meerkat_mobkit::memory::coordinator::{MobScopeResolver, StaticMobBinding};

        let binding = StaticMobBinding {
            realm: "default".to_string(),
            mob: "mob:example".to_string(),
        };
        assert_eq!(
            binding.active_mobs("default", "identity:frontdesk"),
            vec!["mob:example".to_string()],
            "matching realm must compose the hosting mob"
        );
        assert!(
            binding
                .active_mobs("other-realm", "identity:frontdesk")
                .is_empty(),
            "foreign realm must compose no mob scope"
        );
    }

    // §9.1 always-on compaction reset: the sink the gateway registers
    // unconditionally next to the taint tracker. CompactionCompleted with
    // session attribution fires the reset callback with that session key
    // (the gateway binds it to AgentMemoryRuntimeInjector::
    // on_session_compacted); other events and unattributed sources do not.
    #[test]
    fn compaction_reset_sink_fires_on_attributed_compaction_only() {
        use meerkat_core::event::AgentEvent;

        let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = meerkat_mobkit::CompactionResetSink::new({
            let seen = seen.clone();
            Arc::new(move |session: &str| {
                seen.lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .push(session.to_string());
            })
        });

        let session = meerkat_core::types::SessionId::new();
        let envelope = |source, payload| meerkat_core::event::EventEnvelope {
            event_id: Default::default(),
            source,
            seq: 0,
            mob_id: None,
            timestamp_ms: 0,
            payload,
        };
        let compaction = || AgentEvent::CompactionCompleted {
            summary_tokens: 10,
            messages_before: 20,
            messages_after: 2,
        };

        use meerkat_mobkit::MemberAgentEventSink as _;
        // Attributed compaction fires with the envelope's session key.
        sink.observe(
            "identity:a",
            &envelope(
                meerkat_core::event::EventSourceIdentity::Session {
                    session_id: session.clone(),
                },
                compaction(),
            ),
        );
        assert_eq!(
            seen.lock().unwrap_or_else(|err| err.into_inner()).clone(),
            vec![session.to_string()],
            "reset must fire with the compacted session's key"
        );

        // A non-compaction event on the same session never fires it.
        sink.observe(
            "identity:a",
            &envelope(
                meerkat_core::event::EventSourceIdentity::Session {
                    session_id: session.clone(),
                },
                AgentEvent::RunCompleted {
                    assistant_message_id: None,
                    identity: Default::default(),
                    session_id: session.clone(),
                    result: "done".to_string(),
                    structured_output: None,
                    extraction_required: false,
                    usage: Default::default(),
                    terminal_cause_kind: None,
                },
            ),
        );
        // Compaction without session attribution has no key to reset.
        sink.observe(
            "identity:a",
            &envelope(
                meerkat_core::event::EventSourceIdentity::Callback,
                compaction(),
            ),
        );
        assert_eq!(
            seen.lock().unwrap_or_else(|err| err.into_inner()).len(),
            1,
            "only attributed compaction events reset"
        );
    }

    #[test]
    fn gateway_runtime_options_parse_agent_memory_taint_knobs() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({
            "runtime_options": {
                "agent_memory": {
                    "llm_writes": "quarantined",
                    "recorder_tool": false,
                    "content_trust": {
                        "trusted_mcp_servers": ["knowledge_graph"],
                        "untrusted_tools": ["scrape_page"],
                        "trusted_tools": ["safe_calc"],
                    }
                }
            }
        });

        let options = parse_gateway_runtime_options(&params, Some(tmp.path()))
            .expect("taint knobs should parse");
        let agent_memory = options.agent_memory.expect("agent memory options");
        assert_eq!(
            agent_memory.config.llm_writes,
            meerkat_mobkit::AgentMemoryLlmWrites::Quarantined
        );
        assert!(!agent_memory.config.recorder_tool);
        assert_eq!(
            agent_memory.config.content_trust.trusted_mcp_servers,
            vec!["knowledge_graph"]
        );
        assert_eq!(
            agent_memory.config.content_trust.untrusted_tools,
            vec!["scrape_page"]
        );

        // Defaults: observed writes, recorder on, empty trust lists.
        let params = json!({
            "runtime_options": { "agent_memory": true }
        });
        let options = parse_gateway_runtime_options(&params, Some(tmp.path()))
            .expect("boolean agent memory config should parse");
        let agent_memory = options.agent_memory.expect("agent memory options");
        assert_eq!(
            agent_memory.config.llm_writes,
            meerkat_mobkit::AgentMemoryLlmWrites::Observed
        );
        assert!(agent_memory.config.recorder_tool);
        assert_eq!(
            agent_memory.config.content_trust,
            meerkat_mobkit::ContentTrustConfig::default()
        );
    }

    #[test]
    fn gateway_runtime_options_reject_bad_taint_knobs() {
        let tmp = tempfile::tempdir().expect("temp dir");
        for (block, needle) in [
            (
                json!({ "llm_writes": "yolo" }),
                "'observed' or 'quarantined'",
            ),
            (json!({ "llm_writes": true }), "'observed' or 'quarantined'"),
            (json!({ "recorder_tool": "yes" }), "must be a boolean"),
            (
                json!({ "content_trust": { "servers": [] } }),
                "unsupported content_trust fields",
            ),
            (
                json!({ "content_trust": { "trusted_mcp_servers": "kg" } }),
                "must be an array",
            ),
            (
                json!({ "store": "markdown", "llm_writes": "quarantined" }),
                "require store='sqlite'",
            ),
            (
                json!({ "store": "markdown", "content_trust": {} }),
                "require store='sqlite'",
            ),
        ] {
            let params = json!({ "runtime_options": { "agent_memory": block } });
            let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
                Ok(_) => panic!("config must fail loudly: {params}"),
                Err(err) => err,
            };
            assert!(err.contains(needle), "{err} (wanted '{needle}')");
        }
    }

    /// The §8.3 selector knob is retired. `off`/empty is still ACCEPTED,
    /// because that config asked for exactly today's behaviour and refusing
    /// it would brick an init for no gain. Every OTHER value is REFUSED,
    /// typed, at init: it asked for a stage that no longer exists, and
    /// accepting it would hand that caller something different from what it
    /// configured while the only notice went to a gateway log the consumer
    /// may not read. Silently-inert configuration is the exact class this
    /// release program exists to remove.
    #[test]
    fn retired_agent_memory_selector_accepts_off_and_refuses_the_rest() {
        let tmp = tempfile::tempdir().expect("temp dir");
        for value in ["off", ""] {
            let params = json!({
                "runtime_options": { "agent_memory": { "selector": value } }
            });
            parse_gateway_runtime_options(&params, Some(tmp.path())).unwrap_or_else(|err| {
                panic!("selector '{value}' states today's behaviour and must parse: {err}")
            });
        }
        for value in ["default", "profile:/etc/mobkit/selector.toml", "on"] {
            let params = json!({
                "runtime_options": { "agent_memory": { "selector": value } }
            });
            let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
                Ok(_) => {
                    panic!("retired selector '{value}' must be refused, not silently ignored")
                }
                Err(err) => err,
            };
            assert!(
                err.contains("RETIRED") && err.contains(value),
                "refusal must name the retirement and the offending value, got: {err}"
            );
        }
        // Retired EVERYWHERE, so the store kind no longer changes the answer.
        // The old refusal here was `selector requires store='sqlite'` - a
        // COUPLING complaint. That coupling is gone, but the value is still
        // refused, now for the retirement itself. Pinning the reason (not
        // merely "some error") is the point: a store-kind message reappearing
        // here would mean the coupling rule outlived the knob it guarded.
        let params = json!({
            "runtime_options": {
                "agent_memory": { "store": "markdown", "selector": "default" }
            }
        });
        let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
            Ok(_) => panic!("a retired selector must be refused whatever the store kind"),
            Err(err) => err,
        };
        assert!(
            err.contains("RETIRED"),
            "the refusal must cite the retirement, not the old store-kind coupling, got: {err}"
        );
    }

    /// Type validation does NOT loosen: the value is still a string or a
    /// loud parse refusal, so a structurally wrong config is never silently
    /// swallowed by the accept-and-ignore path.
    #[test]
    fn retired_agent_memory_selector_key_still_rejects_non_strings() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let params = json!({
            "runtime_options": { "agent_memory": { "selector": true } }
        });
        let err = match parse_gateway_runtime_options(&params, Some(tmp.path())) {
            Ok(_) => panic!("a non-string selector value must fail loudly"),
            Err(err) => err,
        };
        assert!(err.contains("must be a string"), "{err}");
    }

    #[test]
    fn gateway_runtime_options_reject_zero_max_sessions() {
        let params = json!({
            "runtime_options": {
                "max_sessions": 0
            }
        });

        let err = match parse_gateway_runtime_options(&params, None) {
            Ok(_) => panic!("zero should fail"),
            Err(err) => err,
        };

        assert!(err.contains("runtime_options.max_sessions"));
    }

    #[test]
    fn gateway_runtime_options_workgraph_parses_bool_and_path_forms() {
        let defaults = parse_gateway_runtime_options(&json!({}), None).expect("defaults parse");
        assert_eq!(
            defaults.workgraph,
            GatewayWorkgraphOption::Enabled,
            "workgraph defaults on"
        );

        let enabled = parse_gateway_runtime_options(
            &json!({ "runtime_options": { "workgraph": true } }),
            None,
        )
        .expect("explicit true parses");
        assert_eq!(enabled.workgraph, GatewayWorkgraphOption::Enabled);

        let disabled = parse_gateway_runtime_options(
            &json!({ "runtime_options": { "workgraph": false } }),
            None,
        )
        .expect("explicit false parses");
        assert_eq!(disabled.workgraph, GatewayWorkgraphOption::Disabled);

        // A string is an explicit durable store directory — the escape hatch
        // for SDK-hosted-continuity launches that have no persistent_state.
        let durable = parse_gateway_runtime_options(
            &json!({ "runtime_options": { "workgraph": "/var/lib/mobkit/workgraph" } }),
            None,
        )
        .expect("string path parses");
        assert_eq!(
            durable.workgraph,
            GatewayWorkgraphOption::DurableDir("/var/lib/mobkit/workgraph".into())
        );

        for invalid in [json!(42), json!(""), json!("   "), json!({ "dir": "x" })] {
            let err = match parse_gateway_runtime_options(
                &json!({ "runtime_options": { "workgraph": invalid } }),
                None,
            ) {
                Ok(_) => panic!("invalid workgraph value should fail: {invalid}"),
                Err(err) => err,
            };
            assert!(err.contains("runtime_options.workgraph"), "{err}");
        }
    }

    #[test]
    fn gateway_identity_bootstrap_mode_parse_matrix_is_strict() {
        use meerkat_mobkit::IdentityBootstrapMode;

        let defaults = parse_gateway_runtime_options(&json!({}), None).expect("defaults");
        assert_eq!(defaults.identity_bootstrap_mode, None);

        for (wire, expected) in [
            (
                json!({"mode": "eager_materialize"}),
                IdentityBootstrapMode::EagerMaterialize,
            ),
            (
                json!({"mode": "lazy_materialize"}),
                IdentityBootstrapMode::LazyMaterialize,
            ),
            (
                json!({"mode": "lazy_with_background_warm", "concurrency": 2}),
                IdentityBootstrapMode::LazyWithBackgroundWarm { concurrency: 2 },
            ),
        ] {
            let options = parse_gateway_runtime_options(
                &json!({"runtime_options": {"identity_bootstrap_mode": wire}}),
                None,
            )
            .expect("valid bootstrap mode");
            assert_eq!(options.identity_bootstrap_mode, Some(expected));
        }

        for invalid in [
            json!("lazy_materialize"),
            json!({}),
            json!({"mode": "unknown"}),
            json!({"mode": "lazy_materialize", "concurrency": 2}),
            json!({"mode": "lazy_with_background_warm"}),
            json!({"mode": "lazy_with_background_warm", "concurrency": 0}),
            json!({"mode": "lazy_with_background_warm", "concurrency": 17}),
            json!({"mode": "eager_materialize", "extra": true}),
        ] {
            let error = parse_gateway_runtime_options(
                &json!({"runtime_options": {"identity_bootstrap_mode": invalid}}),
                None,
            )
            .err()
            .expect("invalid bootstrap mode must fail");
            assert!(error.contains("identity_bootstrap_mode"), "{error}");
        }
    }

    #[cfg(feature = "openai-live")]
    #[test]
    fn console_voice_registration_requires_authenticated_composition_and_shares_live() {
        let registration = json!({
            "principal":"voice@example.com", "realm":"voice",
            "auth_binding":{"realm":"voice","binding":"openai"}, "voice":"marin"
        });
        let options = json!({
            "auth_config":{"provider":"jwt","shared_secret":"test-console-signing-key",
                "email_allowlist":["voice@example.com"]},
            "console_voice":registration
        });
        let parsed = parse_gateway_runtime_options(&json!({"runtime_options":options}), None)
            .expect("console voice registration");
        assert!(parsed.console_voice.is_some());
        assert!(parsed.openai_live.is_none());
        assert!(parsed.strict_live_registered());
        // Console voice and the external live channel coexist under one shared
        // live owner; the gateway arbitrates them, it does not refuse them.
        let mut with_live = options.clone();
        with_live["live"] = json!(true);
        let shared = parse_gateway_runtime_options(&json!({"runtime_options":with_live}), None)
            .expect("console voice beside the external live channel");
        assert!(shared.console_voice.is_some());
        assert!(matches!(shared.live, GatewayLiveOption::Enabled { .. }));
        for (field, value) in [
            ("auth_config", Value::Null),
            ("console_require_app_auth", json!(false)),
            ("openai_live", registration.clone()),
        ] {
            let mut invalid = options.clone();
            if value.is_null() {
                invalid.as_object_mut().expect("object").remove(field);
            } else {
                invalid[field] = value;
            }
            assert!(
                parse_gateway_runtime_options(&json!({"runtime_options":invalid}), None).is_err(),
                "{field} must not bypass console voice composition"
            );
        }
    }

    #[cfg(feature = "openai-live")]
    #[test]
    fn gateway_openai_live_registration_is_explicit_and_strict() {
        let defaults = parse_gateway_runtime_options(&json!({}), None).expect("defaults");
        assert!(defaults.openai_live.is_none());
        assert!(!defaults.strict_live_registered());

        let options = parse_gateway_runtime_options(
            &json!({
                "runtime_options": {
                    "openai_live": {
                        "principal": "user:luka",
                        "realm": "team",
                        "auth_binding": {
                            "realm": "team",
                            "binding": "openai-api-key",
                            "profile": "luka"
                        },
                        "voice": "marin",
                        "session_instructions": "Synthetic system prompt for the voice profile."
                    }
                }
            }),
            None,
        )
        .expect("public registration");
        let realm = meerkat_core::RealmId::parse("team").expect("realm");
        assert_eq!(
            options.openai_live,
            Some(GatewayOpenAiLiveOption {
                principal: "user:luka".to_string(),
                realm: realm.clone(),
                binding: meerkat_core::AuthBindingRef {
                    realm: realm.clone(),
                    binding: meerkat_core::BindingId::parse("openai-api-key").expect("binding"),
                    profile: Some(meerkat_core::ProfileId::parse("luka").expect("profile")),
                    origin: meerkat_core::BindingOrigin::Configured,
                },
                voice: "marin".to_string(),
                session_instructions: Some(
                    "Synthetic system prompt for the voice profile.".to_string()
                ),
                summary: Default::default(),
            })
        );
        assert!(options.strict_live_registered());

        let minimal = parse_gateway_runtime_options(
            &json!({
                "runtime_options": {
                    "openai_live": {
                        "principal": "user:luka",
                        "realm": "team",
                        "auth_binding": {"realm": "team", "binding": "openai-api-key"},
                        "voice": "marin",
                        "session_instructions": null
                    }
                }
            }),
            None,
        )
        .expect("minimal public registration");
        let minimal = minimal.openai_live.expect("registered");
        assert_eq!(minimal.binding.profile, None);
        assert_eq!(minimal.session_instructions, None);

        for invalid in [
            json!("gpt-live-1"),
            json!({"principal": "user:luka", "realm": "team", "voice": "marin"}),
            json!({
                "principal": " ",
                "realm": "team",
                "auth_binding": {"realm": "team", "binding": "openai-api-key"},
                "voice": "marin"
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "auth_binding": {"realm": "other", "binding": "openai-api-key"},
                "voice": "marin"
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "auth_binding": {"realm": "team", "binding": "openai-api-key", "token": "x"},
                "voice": "marin"
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "auth_binding": {"realm": "team", "binding": "openai-api-key"},
                "voice": " "
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "auth_binding": {"realm": "team", "binding": "openai-api-key"},
                "voice": "marin",
                "session_instructions": " "
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "auth_binding": {"realm": "team", "binding": "openai-api-key"},
                "voice": "marin",
                "model": "gpt-live-1"
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "auth_binding": {"realm": "team", "binding": "openai-api-key"},
                "voice": "marin",
                "execution_profiles": []
            }),
        ] {
            let error = parse_gateway_runtime_options(
                &json!({"runtime_options": {"openai_live": invalid}}),
                None,
            )
            .err()
            .expect("invalid public registration must fail");
            assert!(error.contains("openai_live"), "{error}");
        }
    }

    #[cfg(feature = "experimental-gpt-live")]
    #[test]
    fn gateway_rejects_both_strict_live_registrations_at_once() {
        let error = parse_gateway_runtime_options(
            &json!({
                "runtime_options": {
                    "openai_live": {
                        "principal": "user:luka",
                        "realm": "team",
                        "auth_binding": {"realm": "team", "binding": "openai-api-key"},
                        "voice": "marin"
                    },
                    "experimental_live": {
                        "principal": "user:luka",
                        "realm": "team",
                        "factory_kind": "private-live",
                        "factory_version": "v1",
                        "gate0_qualification": "gate0-v1",
                        "auth_binding": {"realm": "team", "binding": "chatgpt-oauth"},
                        "voice": "marin"
                    }
                }
            }),
            None,
        )
        .err()
        .expect("both strict registrations must be rejected");
        assert!(error.contains("mutually"), "{error}");
        assert!(
            error.contains("openai_live") && error.contains("experimental_live"),
            "{error}"
        );
    }

    #[cfg(feature = "experimental-gpt-live")]
    #[test]
    fn gateway_experimental_live_registration_is_explicit_and_strict() {
        let defaults = parse_gateway_runtime_options(&json!({}), None).expect("defaults");
        assert!(defaults.experimental_live.is_none());

        let options = parse_gateway_runtime_options(
            &json!({
                "runtime_options": {
                    "experimental_live": {
                        "principal": "user:luka",
                        "realm": "team",
                        "factory_kind": "private-live",
                        "factory_version": "v1",
                        "gate0_qualification": "gate0-v1",
                        "auth_binding": {
                            "realm": "team",
                            "binding": "chatgpt-oauth",
                            "profile": "luka"
                        },
                        "voice": "marin",
                        "execution_profiles": [{
                            "profile_id": "example.device.open-room.v1",
                            "session_instructions": "Synthetic system prompt for the voice profile."
                        }]
                    }
                }
            }),
            None,
        )
        .expect("explicit registration parses");
        let experimental = options.experimental_live.expect("registration");
        assert_eq!(experimental.principal, "user:luka");
        assert_eq!(experimental.realm.as_str(), "team");
        assert_eq!(experimental.binding.realm.as_str(), "team");
        assert_eq!(experimental.binding.binding.as_str(), "chatgpt-oauth");
        assert_eq!(experimental.voice, "marin");
        assert_eq!(
            experimental.execution_profiles,
            vec![GatewayExperimentalLiveExecutionProfile {
                profile_id: "example.device.open-room.v1".to_string(),
                session_instructions: "Synthetic system prompt for the voice profile.".to_string(),
            }]
        );
        assert_eq!(options.live, GatewayLiveOption::Disabled);

        for invalid in [
            json!({
                "realm": "team",
                "factory_kind": "private-live",
                "factory_version": "v1",
                "gate0_qualification": "gate0-v1",
                "auth_binding": {"realm": "team", "binding": "chatgpt-oauth"},
                "voice": "marin"
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "factory_kind": "private-live",
                "factory_version": "v1",
                "gate0_qualification": "gate0-v1",
                "auth_binding": {"realm": "other", "binding": "chatgpt-oauth"},
                "voice": "marin"
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "factory_kind": "private-live",
                "factory_version": "v1",
                "gate0_qualification": "gate0-v1",
                "auth_binding": {"realm": "team", "binding": "chatgpt-oauth"},
                "voice": "marin",
                "ambient_default": true
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "factory_kind": "private-live",
                "factory_version": "v1",
                "gate0_qualification": "gate0-v1",
                "auth_binding": {"realm": "team", "binding": "chatgpt-oauth"},
                "voice": "marin",
                "instructions": "caller-owned prompt"
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "factory_kind": "private-live",
                "factory_version": "v1",
                "gate0_qualification": "gate0-v1",
                "auth_binding": {"realm": "team", "binding": "chatgpt-oauth"},
                "voice": "marin",
                "execution_profiles": {}
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "factory_kind": "private-live",
                "factory_version": "v1",
                "gate0_qualification": "gate0-v1",
                "auth_binding": {"realm": "team", "binding": "chatgpt-oauth"},
                "voice": "marin",
                "execution_profiles": [{"profile_id": " ", "session_instructions": "voice"}]
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "factory_kind": "private-live",
                "factory_version": "v1",
                "gate0_qualification": "gate0-v1",
                "auth_binding": {"realm": "team", "binding": "chatgpt-oauth"},
                "voice": "marin",
                "execution_profiles": [{"profile_id": "device", "session_instructions": " "}]
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "factory_kind": "private-live",
                "factory_version": "v1",
                "gate0_qualification": "gate0-v1",
                "auth_binding": {"realm": "team", "binding": "chatgpt-oauth"},
                "voice": "marin",
                "execution_profiles": [{
                    "profile_id": "device",
                    "session_instructions": "voice",
                    "tools": []
                }]
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "factory_kind": "private-live",
                "factory_version": "v1",
                "gate0_qualification": "gate0-v1",
                "auth_binding": {"realm": "team", "binding": "chatgpt-oauth"},
                "voice": "marin",
                "execution_profiles": [
                    {"profile_id": "device", "session_instructions": "voice"},
                    {"profile_id": " device ", "session_instructions": "other"}
                ]
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "factory_kind": "private-live",
                "factory_version": "v1",
                "gate0_qualification": "gate0-v1",
                "auth_binding": {"realm": "team", "binding": "chatgpt-oauth"},
                "voice": "marin",
                "execution_profiles": [{
                    "profile_id": meerkat::GPT_LIVE_CLIENT_CONTEXT_PROFILE_ID,
                    "session_instructions": "override canonical policy"
                }]
            }),
            json!({
                "principal": "user:luka",
                "realm": "team",
                "factory_kind": "private-live",
                "factory_version": "v1",
                "gate0_qualification": "gate0-v1",
                "auth_binding": {"realm": "team", "binding": "chatgpt-oauth"},
                "voice": "marin",
                "execution_profiles": [{
                    "profile_id": meerkat::GPT_LIVE_FUNCTION_BRIDGE_PROFILE_ID,
                    "session_instructions": "relabel FunctionBridge"
                }]
            }),
        ] {
            let error = parse_gateway_runtime_options(
                &json!({"runtime_options": {"experimental_live": invalid}}),
                None,
            )
            .err()
            .expect("invalid registration must fail");
            assert!(error.contains("experimental_live"), "{error}");
        }

        for forbidden in [
            "mode",
            "model",
            "provider",
            "tools",
            "responses",
            "capabilities",
        ] {
            let mut profile = json!({
                "profile_id": "example.device.open-room.v1",
                "session_instructions": "Synthetic system prompt for the voice profile."
            });
            profile
                .as_object_mut()
                .expect("profile fixture object")
                .insert(forbidden.to_string(), json!("forbidden"));
            let error = parse_gateway_runtime_options(
                &json!({
                    "runtime_options": {
                        "experimental_live": {
                            "principal": "user:luka",
                            "realm": "team",
                            "factory_kind": "private-live",
                            "factory_version": "v1",
                            "gate0_qualification": "gate0-v1",
                            "auth_binding": {
                                "realm": "team",
                                "binding": "chatgpt-oauth"
                            },
                            "voice": "marin",
                            "execution_profiles": [profile]
                        }
                    }
                }),
                None,
            )
            .err()
            .expect("profile authority field must fail");
            assert!(error.contains(forbidden), "{error}");
        }
    }

    #[cfg(feature = "experimental-gpt-live")]
    #[test]
    fn production_experimental_live_operator_advertises_only_client_context() {
        let canonical = meerkat::ExperimentalLiveOperatorConfig::gpt_live_client_context();
        let selected_factory = canonical.factory().clone();
        let realm = meerkat_core::RealmId::parse("team").expect("realm");
        let experimental = GatewayExperimentalLiveOption {
            principal: "user:luka".to_string(),
            realm: realm.clone(),
            factory: selected_factory.clone(),
            qualification: canonical.required_gate0().clone(),
            binding: meerkat_core::AuthBindingRef {
                realm: realm.clone(),
                binding: meerkat_core::BindingId::parse("chatgpt-oauth").expect("binding"),
                profile: None,
                origin: meerkat_core::BindingOrigin::Configured,
            },
            voice: "marin".to_string(),
            execution_profiles: vec![GatewayExperimentalLiveExecutionProfile {
                profile_id: "example.device.open-room.v1".to_string(),
                session_instructions: "Synthetic system prompt for the voice profile.".to_string(),
            }],
        };
        let operator =
            gateway_experimental_live_operator(&experimental).expect("production operator profile");
        let factory =
            AgentFactory::minimal().with_experimental_live_admission(operator, [realm.clone()]);
        let capabilities = factory
            .experimental_live_execution_feature_capabilities(
                &realm,
                &selected_factory,
                meerkat::GPT_LIVE_CLIENT_CONTEXT_PROFILE_ID,
            )
            .expect("current build client-context qualification");

        assert_eq!(
            capabilities,
            vec![
                meerkat_contracts::LIVE_EXECUTION_IDENTITY_V1_CAPABILITY,
                meerkat_contracts::LIVE_CLIENT_CONTEXT_V1_CAPABILITY,
            ]
        );
        assert!(
            !capabilities.contains(&meerkat_contracts::LIVE_FUNCTION_BRIDGE_V1_CAPABILITY),
            "the production gateway must not advertise unqualified FunctionBridge",
        );
        let named_capabilities = factory
            .experimental_live_execution_feature_capabilities(
                &realm,
                &selected_factory,
                "example.device.open-room.v1",
            )
            .expect("host-trusted named client-context qualification");
        assert_eq!(named_capabilities, capabilities);
    }

    #[tokio::test]
    async fn callback_close_wakes_pending_call_and_rejects_late_admission() {
        let (stdout_tx, mut stdout_rx) = mpsc::channel(4);
        let bridge = StdioCallbackBridge::new(stdout_tx);
        let pending = tokio::spawn({
            let bridge = bridge.clone();
            async move { bridge.call("callback/build_agent", json!({})).await }
        });

        tokio::time::timeout(Duration::from_secs(1), stdout_rx.recv())
            .await
            .expect("pending callback should be written")
            .expect("stdout callback channel should remain open");
        bridge.close().await;

        let error = tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .expect("close should wake the pending callback")
            .expect("callback task should not panic")
            .expect_err("closed callback must fail");
        assert!(error.contains("channel dropped"), "{error}");

        let late_error = bridge
            .call("callback/build_agent", json!({}))
            .await
            .expect_err("close must reject callbacks admitted after EOF");
        assert!(late_error.contains("transport closed"), "{late_error}");
        assert!(bridge.state.lock().await.pending.is_empty());
    }

    #[test]
    fn init_in_progress_refusal_is_typed_and_skips_notifications() {
        let refusal = init_in_progress_refusal(&json!({
            "jsonrpc": "2.0", "id": "r-1", "method": "mobkit/status", "params": {}
        }))
        .expect("a request gets an answer");
        assert_eq!(refusal["id"], "r-1");
        assert_eq!(
            refusal["error"]["code"],
            meerkat_mobkit::INIT_IN_PROGRESS_CODE
        );
        assert_eq!(
            refusal["error"]["data"],
            json!({ "kind": "init_in_progress", "method": "mobkit/status" })
        );
        assert!(init_in_progress_refusal(&json!({ "jsonrpc": "2.0", "method": "note" })).is_none());
    }

    #[test]
    fn init_protocol_opt_in_needs_the_exact_name_and_an_init_id() {
        assert_eq!(
            init_protocol_request(
                &json!({ "init_protocol": "accepted_then_settled", "init_id": "i-1" })
            ),
            InitProtocolRequest::AcceptedThenSettled {
                init_id: "i-1".to_string()
            }
        );
        for params in [
            json!({}),
            json!({ "init_protocol": "accepted_then_settled" }),
            json!({ "init_protocol": "accepted_then_settled", "init_id": " " }),
            json!({ "init_protocol": "accepted_then_settled", "init_id": 7 }),
            json!({ "init_protocol": "some_future_protocol", "init_id": "i-1" }),
        ] {
            assert_eq!(
                init_protocol_request(&params),
                InitProtocolRequest::Legacy,
                "{params}"
            );
        }
    }

    fn session(protocol: InitProtocolRequest) -> InitSession {
        InitSession {
            request_id: json!("init-req"),
            protocol,
            accepted: AtomicBool::new(false),
            durable_phase_begun: AtomicBool::new(false),
            settled: AtomicBool::new(false),
        }
    }

    #[test]
    fn legacy_session_answers_the_request_and_never_settles() {
        let legacy = session(InitProtocolRequest::Legacy);
        legacy.accept();
        assert!(!legacy.is_accepted(), "a legacy SDK never sees accepted");
        assert!(legacy.progress_notification(InitPhase::Storage).is_none());
        let ready = legacy.ready_line(json!({ "http_base_url": "http://x" }));
        assert_eq!(ready["id"], "init-req");
        assert_eq!(ready["result"]["http_base_url"], "http://x");
        assert!(ready.get("method").is_none());
        let failed = legacy.failed_line(-32602, "bad", None);
        assert_eq!(failed["id"], "init-req");
        assert_eq!(failed["error"]["code"], -32602);
        assert!(failed["error"].get("durable_effects").is_none());
    }

    #[test]
    fn accepted_session_settles_with_durable_effects_from_the_phase_marker() {
        let opted_in = session(InitProtocolRequest::AcceptedThenSettled {
            init_id: "init-9".to_string(),
        });
        let accepted = opted_in.accepted_response("init-9");
        assert_eq!(accepted["id"], "init-req");
        assert_eq!(accepted["result"]["init_state"], "accepted");
        assert_eq!(accepted["result"]["init_id"], "init-9");
        assert_eq!(
            accepted["result"]["provider_callback_timeout_ms"],
            PROVIDER_CALLBACK_TIMEOUT.as_millis() as u64
        );
        opted_in.accepted.store(true, Ordering::Release);

        let progress = opted_in
            .progress_notification(InitPhase::Prewarm)
            .expect("progress");
        assert_eq!(progress["method"], INIT_PROGRESS_METHOD);
        assert_eq!(
            progress["params"],
            json!({ "init_id": "init-9", "phase": "prewarm" })
        );

        let before = opted_in.failed_line(-32602, "refused", None);
        assert_eq!(before["method"], INIT_SETTLED_METHOD);
        assert!(before.get("id").is_none());
        assert_eq!(before["params"]["outcome"], "failed");
        assert_eq!(before["params"]["durable_effects"], "none");

        opted_in.durable_phase_begun.store(true, Ordering::Release);
        let after = opted_in.failed_line(-32014, "storage", Some(json!({ "reason": "x" })));
        assert_eq!(after["params"]["durable_effects"], "possible");
        assert_eq!(after["params"]["data"]["reason"], "x");

        let ready = opted_in.ready_line(json!({ "http_base_url": "http://y" }));
        assert_eq!(ready["method"], INIT_SETTLED_METHOD);
        assert_eq!(ready["params"]["outcome"], "ready");
        assert_eq!(ready["params"]["init_id"], "init-9");
        assert_eq!(ready["params"]["http_base_url"], "http://y");

        opted_in.settled.store(true, Ordering::Release);
        assert!(
            opted_in.progress_notification(InitPhase::Serve).is_none(),
            "no progress after the settlement"
        );
    }

    #[test]
    fn malformed_callback_response_id_reads_only_a_top_level_cb_id() {
        for (line, expected) in [
            (
                r#"{"jsonrpc":"2.0","id":"cb-7","result":{"weight":NaN}}"#,
                Some("cb-7"),
            ),
            (
                r#"{"jsonrpc":"2.0","result":{"x":Infinity},"id" : "cb-12"}"#,
                Some("cb-12"),
            ),
            (r#"{"jsonrpc":"2.0","id":"req-1","result":NaN}"#, None),
            (r#"{"jsonrpc":"2.0","id":"cb-","result":NaN}"#, None),
            (r#"{"jsonrpc":"2.0","id":"cb-4x","result":NaN}"#, None),
            (
                r#"{"method":"m","params":{"text":"{\"id\":\"cb-3\"}"},NaN}"#,
                None,
            ),
            ("not json at all", None),
        ] {
            assert_eq!(
                malformed_callback_response_id(line).as_deref(),
                expected,
                "line: {line}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn malformed_callback_response_fails_its_callback_at_once() {
        let (stdout_tx, mut stdout_rx) = mpsc::channel(4);
        let bridge = StdioCallbackBridge::new(stdout_tx);
        let mut call = Box::pin(bridge.call("callback/roster_provider/roster", json!({})));
        assert!(futures::poll!(&mut call).is_pending());
        let request: Value = serde_json::from_str(
            &stdout_rx
                .recv()
                .await
                .expect("callback request")
                .to_string(),
        )
        .expect("request json");
        let id = request["id"].as_str().expect("callback id").to_string();
        let reply = format!(r#"{{"jsonrpc":"2.0","id":"{id}","result":{{"weight":NaN}}}}"#);
        assert!(serde_json::from_str::<Value>(&reply).is_err());

        let recovered = malformed_callback_response_id(&reply).expect("recoverable cb id");
        bridge.fail_malformed_callback_response(&recovered).await;

        // No time passes: the callback fails now, not at its 130 s deadline.
        let error = call.await.expect_err("malformed reply fails the callback");
        assert!(error.contains("malformed callback response"), "{error}");
        assert!(bridge.state.lock().await.pending.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn callback_blocked_stdout_times_out_without_enqueuing_or_leaking_pending() {
        let (stdout_tx, mut stdout_rx) = mpsc::channel(1);
        stdout_tx
            .try_send(GatewayStdoutLine::plain("occupied".to_string()))
            .expect("fill writer queue");
        let bridge = StdioCallbackBridge::new(stdout_tx);
        let mut call = Box::pin(bridge.call("callback/build_agent", json!({})));
        assert!(futures::poll!(&mut call).is_pending());
        assert_eq!(bridge.state.lock().await.pending.len(), 1);

        tokio::time::advance(
            PROVIDER_CALLBACK_TIMEOUT
                .checked_sub(Duration::from_secs(1))
                .expect("callback timeout exceeds one second"),
        )
        .await;
        assert!(futures::poll!(&mut call).is_pending());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            call.await.expect_err("full stdout must not hang forever"),
            "callback stdout send timed out after 130s; delivery outcome unknown"
        );
        assert!(bridge.state.lock().await.pending.is_empty());
        assert_eq!(
            stdout_rx.recv().await.expect("original line").to_string(),
            "occupied"
        );
        assert!(matches!(
            stdout_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn callback_closed_stdout_fails_immediately_and_cleans_pending() {
        for close_while_blocked in [false, true] {
            let (stdout_tx, stdout_rx) = mpsc::channel(1);
            stdout_tx
                .try_send(GatewayStdoutLine::plain("occupied".to_string()))
                .expect("fill writer queue");
            let bridge = StdioCallbackBridge::new(stdout_tx);
            let mut call = Box::pin(bridge.call("callback/build_agent", json!({})));
            if close_while_blocked {
                assert!(futures::poll!(&mut call).is_pending());
            }
            drop(stdout_rx);
            let start = tokio::time::Instant::now();
            assert_eq!(call.await, Err("stdout channel closed".to_string()));
            assert_eq!(tokio::time::Instant::now(), start);
            assert!(bridge.state.lock().await.pending.is_empty());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn callback_close_wakes_stdout_blocked_call_without_late_send() {
        let (stdout_tx, mut stdout_rx) = mpsc::channel(1);
        stdout_tx
            .try_send(GatewayStdoutLine::plain("occupied".to_string()))
            .expect("fill writer queue");
        let bridge = StdioCallbackBridge::new(stdout_tx);
        let mut call = Box::pin(bridge.call("callback/build_agent", json!({})));
        assert!(futures::poll!(&mut call).is_pending());
        bridge.close().await;
        let start = tokio::time::Instant::now();
        assert_eq!(
            call.await,
            Err("callback response channel dropped".to_string())
        );
        assert_eq!(tokio::time::Instant::now(), start);
        assert!(bridge.state.lock().await.pending.is_empty());
        stdout_rx.recv().await.expect("original line");
        assert!(matches!(
            stdout_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn callback_stdout_admission_and_response_share_one_deadline() {
        let (stdout_tx, mut stdout_rx) = mpsc::channel(1);
        stdout_tx
            .try_send(GatewayStdoutLine::plain("occupied".to_string()))
            .expect("fill writer queue");
        let bridge = StdioCallbackBridge::new(stdout_tx);
        let start = tokio::time::Instant::now();
        let mut call = Box::pin(bridge.call("callback/build_agent", json!({})));
        assert!(futures::poll!(&mut call).is_pending());
        tokio::time::advance(Duration::from_secs(100)).await;
        stdout_rx.recv().await.expect("free queue capacity");
        assert!(futures::poll!(&mut call).is_pending());
        let request: Value =
            serde_json::from_str(&stdout_rx.recv().await.expect("request admitted to writer"))
                .expect("callback JSON");
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(
            call.await,
            Err("callback timed out after 130s; execution outcome unknown".to_string())
        );
        assert_eq!(
            tokio::time::Instant::now() - start,
            PROVIDER_CALLBACK_TIMEOUT
        );
        assert!(bridge.state.lock().await.pending.is_empty());

        // A host can complete after the caller times out. It must not satisfy
        // a later callback or turn the earlier timeout into a non-execution claim.
        bridge
            .route_callback_response(json!({
                "id": request["id"], "result": {"completed": true},
            }))
            .await;
        let mut next = Box::pin(bridge.call("callback/build_agent", json!({})));
        assert!(futures::poll!(&mut next).is_pending());
        let next_request: Value =
            serde_json::from_str(&stdout_rx.recv().await.expect("next request"))
                .expect("next callback JSON");
        assert_ne!(request["id"], next_request["id"]);
        bridge
            .route_callback_response(json!({
                "id": next_request["id"], "result": {"current": true},
            }))
            .await;
        assert_eq!(next.await, Ok(json!({"current": true})));
        assert!(bridge.state.lock().await.pending.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn callback_host_error_remains_distinct_from_transport_timeout() {
        let (stdout_tx, mut stdout_rx) = mpsc::channel(1);
        let bridge = StdioCallbackBridge::new(stdout_tx);
        let mut call = Box::pin(bridge.call("callback/build_agent", json!({})));
        assert!(futures::poll!(&mut call).is_pending());
        let request: Value =
            serde_json::from_str(&stdout_rx.recv().await.expect("callback request"))
                .expect("callback JSON");
        bridge
            .route_callback_response(json!({
                "id": request["id"], "error": {"message": "host refused build"},
            }))
            .await;
        assert_eq!(
            call.await,
            Err("callback error: host refused build".to_string())
        );
        assert!(bridge.state.lock().await.pending.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn callback_reliable_notification_reports_blocked_and_closed_stdout() {
        let (stdout_tx, mut stdout_rx) = mpsc::channel(1);
        stdout_tx
            .try_send(GatewayStdoutLine::plain("occupied".to_string()))
            .expect("fill writer queue");
        let bridge = StdioCallbackBridge::new(stdout_tx);
        let mut notify = Box::pin(bridge.notify_reliable("callback/after_create", json!({})));
        assert!(futures::poll!(&mut notify).is_pending());
        tokio::time::advance(PROVIDER_CALLBACK_TIMEOUT).await;
        assert_eq!(
            notify.await,
            Err("callback stdout send timed out after 130s; delivery outcome unknown".to_string())
        );
        stdout_rx.recv().await.expect("original line");
        assert!(matches!(
            stdout_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        drop(stdout_rx);
        assert_eq!(
            bridge
                .notify_reliable("callback/after_create", json!({}))
                .await,
            Err("stdout channel closed".to_string())
        );
    }

    #[cfg(feature = "openai-live")]
    #[tokio::test(start_paused = true)]
    async fn callback_live_output_blocked_writer_times_out_without_claiming_rejection() {
        let (machine, binding) = stale_public_observation_binding();
        let (stdout_tx, mut stdout_rx) = mpsc::channel(1);
        let bridge = StdioCallbackBridge::new(stdout_tx);
        let mut call = Box::pin(bridge.call_live_public_observation(machine, binding, json!({})));
        assert!(futures::poll!(&mut call).is_pending());
        let mut line = stdout_rx.recv().await.expect("writer owns callback line");
        tokio::time::advance(PROVIDER_CALLBACK_TIMEOUT).await;
        assert_eq!(
            call.await,
            Err("callback stdout write timed out after 130s; delivery outcome unknown".to_string())
        );
        assert!(bridge.state.lock().await.pending.is_empty());
        line.settle_delivery(true).await;
    }

    #[cfg(feature = "openai-live")]
    #[tokio::test(start_paused = true)]
    async fn callback_live_output_early_sdk_ack_still_waits_for_writer_settlement() {
        for delivered in [false, true] {
            let (machine, binding) = stale_public_observation_binding();
            let (stdout_tx, mut stdout_rx) = mpsc::channel(1);
            let bridge = StdioCallbackBridge::new(stdout_tx);
            let mut call =
                Box::pin(bridge.call_live_public_observation(machine, binding, json!({})));
            assert!(futures::poll!(&mut call).is_pending());
            let mut line = stdout_rx.recv().await.expect("writer owns callback line");
            let request: Value = serde_json::from_str(&line).expect("callback JSON");
            bridge
                .route_callback_response(json!({
                    "id": request["id"], "result": {"accepted": true},
                }))
                .await;
            assert!(
                futures::poll!(&mut call).is_pending(),
                "an SDK acknowledgement cannot replace writer settlement"
            );
            line.settle_delivery(delivered).await;
            let expected = if delivered {
                Ok(json!({"accepted": true}))
            } else {
                Err("live output publication rejected before write".to_string())
            };
            assert_eq!(call.await, expected);
            assert!(bridge.state.lock().await.pending.is_empty());
        }
    }

    #[cfg(feature = "openai-live")]
    #[tokio::test(start_paused = true)]
    async fn callback_live_output_closed_writer_cleans_pending() {
        let (machine, binding) = stale_public_observation_binding();
        let (stdout_tx, stdout_rx) = mpsc::channel(1);
        let bridge = StdioCallbackBridge::new(stdout_tx);
        drop(stdout_rx);
        assert_eq!(
            bridge
                .call_live_public_observation(machine, binding, json!({}))
                .await,
            Err("stdout channel closed".to_string())
        );
        assert!(bridge.state.lock().await.pending.is_empty());
    }

    /// The (registry, owner session) an ops-capable source was bound to.
    type ObservedBinding = std::sync::Mutex<
        Option<(
            Arc<dyn meerkat_core::ops_lifecycle::OpsLifecycleRegistry>,
            meerkat_core::SessionId,
        )>,
    >;

    /// Ops-capable external tool source for the owned-build tests. Its
    /// observations live outside it, so a test holds no extra handle to it.
    struct ObservedOpsSource {
        seen: Arc<ObservedBinding>,
    }

    #[async_trait]
    impl meerkat_core::AgentToolDispatcher for ObservedOpsSource {
        fn tools(&self) -> Arc<[Arc<meerkat_core::ToolDef>]> {
            Arc::from([Arc::new(meerkat_core::ToolDef {
                audience: Default::default(),
                name: "ops_probe".into(),
                description: String::new(),
                input_schema: json!({"type": "object"}),
                provenance: None,
            })])
        }

        async fn dispatch(
            &self,
            call: meerkat_core::ToolCallView<'_>,
        ) -> Result<meerkat_core::ToolDispatchOutcome, ToolError> {
            Err(ToolError::not_found(call.name))
        }

        fn capabilities(&self) -> meerkat_core::agent::DispatcherCapabilities {
            meerkat_core::agent::DispatcherCapabilities {
                ops_lifecycle: true,
            }
        }

        fn bind_ops_lifecycle(
            self: Arc<Self>,
            registry: Arc<dyn meerkat_core::ops_lifecycle::OpsLifecycleRegistry>,
            owner_bridge_session_id: meerkat_core::SessionId,
        ) -> Result<meerkat_core::agent::BindOutcome, meerkat_core::agent::OpsLifecycleBindError>
        {
            let this = Arc::try_unwrap(self)
                .map_err(|_| meerkat_core::agent::OpsLifecycleBindError::SharedOwnership)?;
            *this.seen.lock().expect("source lock") = Some((registry, owner_bridge_session_id));
            Ok(meerkat_core::agent::BindOutcome::Bound(Arc::new(this)))
        }
    }

    /// How the scripted SDK host is configured for an owned-build test.
    enum SdkBuildHost {
        /// No SDK SessionBuilder: the gateway builder takes its no-callback
        /// branch. The host still answers, so a stray callback is recorded
        /// instead of hanging.
        Absent,
        /// An SDK SessionBuilder answering every `callback/build_agent` with
        /// this response.
        Answers(Value),
    }

    struct OwnedBuildFixture {
        _temp: tempfile::TempDir,
        service: EphemeralSessionService<StdioCallbackAgentBuilder>,
        request: CreateSessionRequest,
        session_id: meerkat_core::SessionId,
        machine: meerkat_runtime::MeerkatMachine,
        builds: Arc<std::sync::Mutex<Vec<Value>>>,
        _responder: tokio::task::JoinHandle<()>,
    }

    /// A session-owned create request carrying `external` as its external
    /// tools, and the gateway's non-persistent service composition (the
    /// gateway builder inside an `EphemeralSessionService`) to create it
    /// through. The service's own create path decides how the request
    /// reaches the builder, as it does in the gateway.
    async fn owned_build_fixture(
        external: Arc<dyn meerkat_core::AgentToolDispatcher>,
        host: SdkBuildHost,
    ) -> OwnedBuildFixture {
        let temp = tempfile::tempdir().expect("temp dir");
        let mut inner = FactoryAgentBuilder::new(
            AgentFactory::new(temp.path().join("sessions")),
            Config::default(),
        );
        inner.default_llm_client = Some(Arc::new(meerkat_client::TestClient::default()));
        let machine = meerkat_runtime::MeerkatMachine::ephemeral();
        let session = meerkat_core::Session::new();
        let session_id = session.id().clone();
        let bindings = machine
            .prepare_bindings(session_id.clone())
            .await
            .expect("prepare bindings");
        let request = CreateSessionRequest {
            injected_context: Vec::new(),
            model: "claude-sonnet-4-5".to_string(),
            prompt: "hello".to_string().into(),
            system_prompt: meerkat_core::config::SystemPromptOverride::Inherit,
            max_tokens: None,
            event_tx: None,
            initial_turn: meerkat_core::service::InitialTurnPolicy::Defer,
            deferred_prompt_policy: meerkat_core::service::DeferredPromptPolicy::Discard,
            build: Some(meerkat_core::service::SessionBuildOptions {
                resume_session: Some(session),
                runtime_build_mode: meerkat_core::RuntimeBuildMode::SessionOwned(bindings),
                external_tools: Some(external),
                ..meerkat_core::service::SessionBuildOptions::default()
            }),
            labels: None,
        };
        let (stdout_tx, stdout_rx) = mpsc::channel(16);
        let bridge = StdioCallbackBridge::new(stdout_tx);
        let (has_session_builder, response) = match host {
            SdkBuildHost::Absent => (false, json!({})),
            SdkBuildHost::Answers(response) => (true, response),
        };
        let (builds, responder) = answer_build_callbacks(stdout_rx, bridge.clone(), response);
        let builder = StdioCallbackAgentBuilder {
            inner,
            bridge,
            has_session_builder,
            session_store: None,
            detached_jobs: None,
        };
        OwnedBuildFixture {
            _temp: temp,
            service: EphemeralSessionService::new(builder, 4),
            request,
            session_id,
            machine,
            builds,
            _responder: responder,
        }
    }

    fn weather_callback_tools() -> Value {
        json!({ "tools": [{
            "name": "weather",
            "description": "Look up the weather",
            "input_schema": {"type": "object"}
        }]})
    }

    fn build_callbacks_seen(builds: &std::sync::Mutex<Vec<Value>>) -> usize {
        builds.lock().expect("build log").len()
    }

    /// The source was bound to the registry the runtime machine holds for
    /// the created session, with the created session as owner.
    async fn assert_bound_to_created_session(
        seen: &ObservedBinding,
        machine: &meerkat_runtime::MeerkatMachine,
        created: &meerkat_core::SessionId,
    ) {
        let (registry, owner) = seen
            .lock()
            .expect("source lock")
            .clone()
            .expect("the ops-capable external source was bound");
        let session_registry = machine
            .ops_lifecycle_registry(created)
            .await
            .expect("the created session has a runtime ops registry");
        assert!(
            std::ptr::addr_eq(Arc::as_ptr(&registry), Arc::as_ptr(&session_registry)),
            "bound to the created session's own ops registry"
        );
        assert_eq!(&owner, created);
    }

    async fn visible_tool_names(
        service: &EphemeralSessionService<StdioCallbackAgentBuilder>,
        session_id: &meerkat_core::SessionId,
    ) -> Vec<String> {
        service
            .live_visible_tool_defs(session_id)
            .await
            .expect("the created session is live")
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect()
    }

    /// No SDK SessionBuilder: creating the session through the service hands
    /// the owned request straight to the factory, so the ops-capable external
    /// source moves into the created session's agent and binds to that
    /// session's registry. Through the borrowed path it stayed shared and
    /// was refused.
    #[tokio::test]
    async fn a_session_created_without_a_callback_binds_its_ops_capable_external_source() {
        let seen = Arc::new(std::sync::Mutex::new(None));
        let fixture = owned_build_fixture(
            Arc::new(ObservedOpsSource {
                seen: Arc::clone(&seen),
            }),
            SdkBuildHost::Absent,
        )
        .await;
        let created = meerkat_core::service::SessionService::create_session(
            &fixture.service,
            fixture.request,
        )
        .await
        .expect("the session is created and its source bound, no SharedOwnership");
        assert_eq!(created.session_id, fixture.session_id);
        assert_eq!(
            build_callbacks_seen(&fixture.builds),
            0,
            "no SDK SessionBuilder, so no build callback"
        );
        assert_bound_to_created_session(&seen, &fixture.machine, &created.session_id).await;
        let names = visible_tool_names(&fixture.service, &created.session_id).await;
        assert!(names.iter().any(|name| name == "ops_probe"), "{names:?}");
    }

    /// An SDK SessionBuilder whose callback answers without `tools` takes the
    /// callback branch but installs nothing: the drained external source is
    /// put back into the transformed request uncomposed, and the created
    /// session binds it.
    #[tokio::test]
    async fn a_session_created_through_a_callback_without_tools_binds_its_ops_source() {
        let seen = Arc::new(std::sync::Mutex::new(None));
        let fixture = owned_build_fixture(
            Arc::new(ObservedOpsSource {
                seen: Arc::clone(&seen),
            }),
            SdkBuildHost::Answers(json!({})),
        )
        .await;
        let created = meerkat_core::service::SessionService::create_session(
            &fixture.service,
            fixture.request,
        )
        .await
        .expect("the session is created and its source bound, no SharedOwnership");
        assert_eq!(created.session_id, fixture.session_id);
        assert_eq!(
            build_callbacks_seen(&fixture.builds),
            1,
            "the build callback ran and answered without tools"
        );
        assert_bound_to_created_session(&seen, &fixture.machine, &created.session_id).await;
        let names = visible_tool_names(&fixture.service, &created.session_id).await;
        assert!(names.iter().any(|name| name == "ops_probe"), "{names:?}");
        assert!(!names.iter().any(|name| name == "weather"), "{names:?}");
    }

    /// An SDK SessionBuilder whose callback adds its own tools: they compose
    /// OVER the ops-capable external source, and the created session still
    /// binds it, because the original request's slot is drained before the
    /// transform and the composition forwards the binding.
    #[tokio::test]
    async fn a_session_created_through_a_callback_adding_tools_binds_its_ops_source() {
        let seen = Arc::new(std::sync::Mutex::new(None));
        let fixture = owned_build_fixture(
            Arc::new(ObservedOpsSource {
                seen: Arc::clone(&seen),
            }),
            SdkBuildHost::Answers(weather_callback_tools()),
        )
        .await;
        let created = meerkat_core::service::SessionService::create_session(
            &fixture.service,
            fixture.request,
        )
        .await
        .expect("the session is created and its source bound, no SharedOwnership");
        assert_eq!(created.session_id, fixture.session_id);
        assert_eq!(
            build_callbacks_seen(&fixture.builds),
            1,
            "the build callback ran"
        );
        assert_bound_to_created_session(&seen, &fixture.machine, &created.session_id).await;
        let names = visible_tool_names(&fixture.service, &created.session_id).await;
        assert!(names.iter().any(|name| name == "weather"), "{names:?}");
        assert!(names.iter().any(|name| name == "ops_probe"), "{names:?}");
    }

    /// A host that keeps its own handle to the ops-capable source cannot have
    /// it rebound: creation fails typed after the callback ran, instead of
    /// running the session with the source silently unbound, and no session
    /// is left behind.
    #[tokio::test]
    async fn a_retained_ops_source_fails_session_creation_with_shared_ownership() {
        let seen = Arc::new(std::sync::Mutex::new(None));
        let external: Arc<dyn meerkat_core::AgentToolDispatcher> = Arc::new(ObservedOpsSource {
            seen: Arc::clone(&seen),
        });
        let retained = Arc::clone(&external);
        let fixture =
            owned_build_fixture(external, SdkBuildHost::Answers(weather_callback_tools())).await;
        let error = match meerkat_core::service::SessionService::create_session(
            &fixture.service,
            fixture.request,
        )
        .await
        {
            Ok(_) => panic!("a shared ops-capable source must not build"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("shared ownership"), "{error}");
        assert!(seen.lock().expect("source lock").is_none());
        assert_eq!(
            build_callbacks_seen(&fixture.builds),
            1,
            "the callback ran; binding refused the build"
        );
        assert!(matches!(
            meerkat_core::service::SessionService::read(&fixture.service, &fixture.session_id)
                .await,
            Err(SessionError::NotFound { .. })
        ));
        drop(retained);
    }

    #[tokio::test]
    async fn callback_builder_delegates_absent_session_compaction_reconciliation() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let inner = FactoryAgentBuilder::new(AgentFactory::new(tmp.path()), Config::default());
        let (stdout_tx, _stdout_rx) = mpsc::channel(1);
        let builder = StdioCallbackAgentBuilder {
            inner,
            bridge: StdioCallbackBridge::new(stdout_tx),
            has_session_builder: false,
            session_store: None,
            detached_jobs: None,
        };
        let session_id = meerkat_core::SessionId::parse("019f74fb-1907-7b21-932d-ab22c4d1f532")
            .expect("valid session id");

        builder
            .abort_absent_session_compaction_stages(&session_id)
            .await
            .expect("callback wrapper must preserve the inner factory's durable-memory seam");
    }

    /// `cleanup_completed` is a four-way conjunction, and the response used to
    /// carry only its result, so a blocked shutdown named no phase. These pin
    /// that each blocking phase is now distinguishable ON THE WIRE, which is
    /// what four undiagnosed CI occurrences actually needed.
    #[test]
    fn a_blocked_shutdown_names_which_phase_blocked() {
        fn clean_report() -> UnifiedRuntimeShutdownReport {
            UnifiedRuntimeShutdownReport {
                drain: ShutdownDrainReport {
                    drained_count: 1,
                    timed_out: false,
                    drain_duration_ms: 2,
                },
                module_shutdown: RuntimeShutdownReport {
                    terminated_modules: vec!["router".to_string()],
                    orphan_processes: 0,
                },
                mob_stop: Ok(()),
                identity_authority_release: IdentityAuthorityReleaseOutcome::Released {
                    grant_count: 1,
                },
                retired_supervisor_cleanup: RetiredSupervisorCleanupOutcome::NothingPending,
                mob_terminal_shutdown: meerkat_mobkit::MobTerminalShutdownOutcome::AlreadyShutDown,
            }
        }
        fn diagnostics(report: &UnifiedRuntimeShutdownReport) -> Value {
            let response = gateway_shutdown_response(json!("shutdown"), Some(report));
            assert_eq!(response["result"]["shutdown"], false);
            assert_eq!(response["result"]["runtime_cleanup_completed"], false);
            response["result"]["runtime_cleanup_diagnostics"].clone()
        }

        // 1. drain timed out
        let mut report = clean_report();
        report.drain.timed_out = true;
        report.drain.drain_duration_ms = 4321;
        let blocked = diagnostics(&report);
        assert_eq!(blocked["drain"]["timed_out"], true);
        assert_eq!(blocked["drain"]["drain_duration_ms"], 4321);
        assert_eq!(blocked["mob_stop"], "ok");

        // 2. mob stop failed
        let mut report = clean_report();
        report.mob_stop =
            Err(meerkat_mobkit::mob_handle_runtime::MobRuntimeError::InvalidInput("stop refused"));
        let blocked = diagnostics(&report);
        assert_eq!(blocked["mob_stop"], "failed");
        assert_eq!(blocked["drain"]["timed_out"], false);

        // 3. identity authority not released
        let mut report = clean_report();
        report.identity_authority_release = IdentityAuthorityReleaseOutcome::SkippedMobStopFailed;
        let blocked = diagnostics(&report);
        assert_eq!(
            blocked["identity_authority_release"]["outcome"],
            "skipped_mob_stop_failed"
        );

        // 4. orphan processes remain
        let mut report = clean_report();
        report.module_shutdown.orphan_processes = 3;
        let blocked = diagnostics(&report);
        assert_eq!(blocked["module_shutdown"]["orphan_processes"], 3);

        // 5. a retired supervisor cleanup was still running when its join budget
        // expired. `diagnostics()` asserts shutdown=false and
        // runtime_cleanup_completed=false, so this also pins that this phase
        // ALONE is enough to withhold the attestation. It has to be: the
        // diagnostics block is attached only when cleanup did not complete, so
        // if this outcome did not gate, it could never be reported in the case
        // it exists for.
        let mut report = clean_report();
        report.retired_supervisor_cleanup = RetiredSupervisorCleanupOutcome::Incomplete {
            joined: 2,
            join_failed: 0,
            pending: 1,
        };
        let blocked = diagnostics(&report);
        assert_eq!(
            blocked["retired_supervisor_cleanup"]["outcome"],
            "incomplete"
        );
        assert_eq!(blocked["retired_supervisor_cleanup"]["pending"], 1);
        assert_eq!(blocked["retired_supervisor_cleanup"]["join_failed"], 0);

        // 6. a retired cleanup did not return normally. Distinct from case 5 on
        // the wire even though both are Incomplete: `join_failed` means a
        // cleanup whose release boundary cannot be attested, `pending` means a
        // task that may still be holding the authority.
        let mut report = clean_report();
        report.retired_supervisor_cleanup = RetiredSupervisorCleanupOutcome::Incomplete {
            joined: 0,
            join_failed: 1,
            pending: 0,
        };
        let blocked = diagnostics(&report);
        assert_eq!(
            blocked["retired_supervisor_cleanup"]["outcome"],
            "incomplete"
        );
        assert_eq!(blocked["retired_supervisor_cleanup"]["join_failed"], 1);
        assert_eq!(blocked["retired_supervisor_cleanup"]["pending"], 0);

        // A fully joined cleanup is NOT a blocking phase, so it must not be able
        // to withhold the attestation on its own.
        let mut report = clean_report();
        report.retired_supervisor_cleanup = RetiredSupervisorCleanupOutcome::Joined {
            lease_renewal: 1,
            continuity_repair: 2,
        };
        assert!(
            report.cleanup_completed(),
            "joining every retired cleanup is a completed phase, not a blocked one"
        );

        // Absent report is a DISTINCT case from any phase failing: nothing ran.
        let response = gateway_shutdown_response(json!("shutdown"), None);
        assert_eq!(response["result"]["shutdown"], false);
        assert_eq!(
            response["result"]["runtime_cleanup_diagnostics"]["runtime_shutdown_report"],
            "absent"
        );
    }

    /// The success response must stay byte-identical to what existing SDK
    /// checks already validate: no diagnostics key at all.
    #[test]
    fn a_successful_shutdown_response_gains_no_new_keys() {
        let report = UnifiedRuntimeShutdownReport {
            drain: ShutdownDrainReport {
                drained_count: 1,
                timed_out: false,
                drain_duration_ms: 2,
            },
            module_shutdown: RuntimeShutdownReport {
                terminated_modules: vec!["router".to_string()],
                orphan_processes: 0,
            },
            mob_stop: Ok(()),
            identity_authority_release: IdentityAuthorityReleaseOutcome::NotConfigured,
            retired_supervisor_cleanup: RetiredSupervisorCleanupOutcome::NothingPending,
            mob_terminal_shutdown: meerkat_mobkit::MobTerminalShutdownOutcome::AlreadyShutDown,
        };
        let response = gateway_shutdown_response(json!("shutdown"), Some(&report));
        assert_eq!(response["result"]["shutdown"], true);
        assert_eq!(response["result"]["runtime_cleanup_completed"], true);
        // Compare the key SET, not its order. JSON object order is not the
        // contract, and serde_json's map ordering varies with the
        // preserve_order feature, so an ordered assertion here would pin an
        // incidental property and fail for a reason that is not a defect.
        let keys: std::collections::BTreeSet<&str> = response["result"]
            .as_object()
            .expect("result is an object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            std::collections::BTreeSet::from(["shutdown", "runtime_cleanup_completed"]),
            "a successful shutdown must gain no diagnostics key"
        );
    }

    #[test]
    fn advertised_shutdown_horizon_covers_every_bounded_gateway_phase() {
        fn completed_report() -> UnifiedRuntimeShutdownReport {
            UnifiedRuntimeShutdownReport {
                drain: ShutdownDrainReport {
                    drained_count: 1,
                    timed_out: false,
                    drain_duration_ms: 2,
                },
                module_shutdown: RuntimeShutdownReport {
                    terminated_modules: vec!["router".to_string()],
                    orphan_processes: 0,
                },
                mob_stop: Ok(()),
                identity_authority_release: IdentityAuthorityReleaseOutcome::Released {
                    grant_count: 1,
                },
                retired_supervisor_cleanup: RetiredSupervisorCleanupOutcome::NothingPending,
                mob_terminal_shutdown: meerkat_mobkit::MobTerminalShutdownOutcome::AlreadyShutDown,
            }
        }

        fn assert_not_attested(report: Option<&UnifiedRuntimeShutdownReport>) {
            let response = gateway_shutdown_response(json!("shutdown"), report);
            assert_eq!(response["result"]["shutdown"], false);
            assert_eq!(response["result"]["runtime_cleanup_completed"], false);
        }

        assert_eq!(PROVIDER_CALLBACK_TIMEOUT, Duration::from_secs(130));
        let mob_quiesce_window = meerkat_mobkit::MOB_STOP_FLOW_SETTLE_BUDGET;
        let scheduler_overhead = Duration::from_secs(10);
        assert_eq!(
            meerkat_mobkit::gateway_composition::GATEWAY_RUNTIME_SHUTDOWN_TIMEOUT,
            PROVIDER_CALLBACK_TIMEOUT
                + PROVIDER_CALLBACK_TIMEOUT
                + GATEWAY_RUNTIME_EVENT_DRAIN_TIMEOUT
                + mob_quiesce_window
                + scheduler_overhead
                // A bounded phase inside runtime.shutdown(). It was added
                // without this term and the gate did not notice, because the
                // sum is a hand-written enumeration of phases rather than
                // anything derived from them. Whoever adds the next phase pays
                // the same tax: state it here, or silently overrun the horizon.
                + meerkat_mobkit::unified_runtime::lifecycle::RETIRED_SUPERVISOR_JOIN_BUDGET
                // The mob actor's terminal teardown (`shutdown_with_report`).
                // Uncounted, the outer timeout could fire inside it and drop
                // the whole report, per-member mob outcomes included.
                + meerkat_mobkit::unified_runtime::lifecycle::MOB_TERMINAL_SHUTDOWN_BUDGET,
            "runtime budget must exactly cover both provider callbacks, event drain, mob quiesce, scheduler overhead, the retired-supervisor join, and the mob terminal shutdown"
        );
        let gateway_phase_budget = GATEWAY_RPC_DRAIN_TIMEOUT
            + meerkat_mobkit::gateway_composition::GATEWAY_HTTP_DRAIN_TIMEOUT
            + meerkat_mobkit::gateway_composition::GATEWAY_RUNTIME_SHUTDOWN_TIMEOUT
            + GATEWAY_STDOUT_DRAIN_TIMEOUT
            + meerkat_mobkit::console_voice::CONSOLE_VOICE_SHUTDOWN_TIMEOUT;
        assert_eq!(gateway_phase_budget, Duration::from_secs(342));
        assert_eq!(
            Duration::from_millis(GATEWAY_SHUTDOWN_HORIZON_MS),
            Duration::from_secs(352)
        );
        assert_eq!(
            Duration::from_millis(GATEWAY_SHUTDOWN_HORIZON_MS).saturating_sub(gateway_phase_budget),
            Duration::from_secs(10),
            "advertised horizon must leave response/reaping margin"
        );

        assert_not_attested(None);

        let successful_report = completed_report();
        let completed = gateway_shutdown_response(json!("shutdown"), Some(&successful_report));
        assert_eq!(completed["result"]["shutdown"], true);
        assert_eq!(completed["result"]["runtime_cleanup_completed"], true);

        let mut report = completed_report();
        report.drain.timed_out = true;
        assert_not_attested(Some(&report));

        let mut report = completed_report();
        report.mob_stop = Err(MobRuntimeError::InvalidConfig(
            "mob stop failed".to_string(),
        ));
        report.identity_authority_release = IdentityAuthorityReleaseOutcome::SkippedMobStopFailed;
        assert_not_attested(Some(&report));

        let mut report = completed_report();
        report.identity_authority_release =
            IdentityAuthorityReleaseOutcome::SkippedResetCleanupFailed {
                error: "superseded generation cleanup remains pending".to_string(),
            };
        assert_not_attested(Some(&report));

        let mut report = completed_report();
        report.identity_authority_release = IdentityAuthorityReleaseOutcome::Failed {
            error: "provider release failed".to_string(),
        };
        assert_not_attested(Some(&report));

        let mut report = completed_report();
        report.module_shutdown.orphan_processes = 1;
        assert_not_attested(Some(&report));
    }

    #[test]
    fn gateway_explicit_identity_bootstrap_mode_always_requires_roster() {
        use meerkat_mobkit::IdentityBootstrapMode;

        validate_gateway_identity_bootstrap_intent(None, false)
            .expect("omitted mode preserves the classic gateway");
        validate_gateway_identity_bootstrap_intent(None, true)
            .expect("a roster may use the eager compatibility default");

        for mode in [
            IdentityBootstrapMode::EagerMaterialize,
            IdentityBootstrapMode::LazyMaterialize,
            IdentityBootstrapMode::LazyWithBackgroundWarm { concurrency: 2 },
        ] {
            let error = validate_gateway_identity_bootstrap_intent(Some(&mode), false)
                .expect_err("every explicit identity mode requires a roster");
            assert!(error.contains("roster provider"), "{error}");
            validate_gateway_identity_bootstrap_intent(Some(&mode), true)
                .expect("an explicit mode with a roster is valid");
        }
    }
}

fn parse_gateway_identity_bootstrap_mode(
    value: &Value,
) -> Result<meerkat_mobkit::IdentityBootstrapMode, String> {
    use meerkat_mobkit::identity_first::MAX_IDENTITY_BACKGROUND_WARM_CONCURRENCY;

    let object = value
        .as_object()
        .ok_or_else(|| "runtime_options.identity_bootstrap_mode must be an object".to_string())?;
    let unsupported = object
        .keys()
        .filter(|key| !matches!(key.as_str(), "mode" | "concurrency"))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        return Err(format!(
            "unsupported runtime_options.identity_bootstrap_mode fields: {}",
            unsupported.join(", ")
        ));
    }
    let mode = object.get("mode").and_then(Value::as_str).ok_or_else(|| {
        "runtime_options.identity_bootstrap_mode.mode must be a string".to_string()
    })?;
    let parsed = match mode {
        "eager_materialize" => {
            if object.contains_key("concurrency") {
                return Err(
                    "runtime_options.identity_bootstrap_mode.concurrency is only valid for lazy_with_background_warm"
                        .to_string(),
                );
            }
            meerkat_mobkit::IdentityBootstrapMode::EagerMaterialize
        }
        "lazy_materialize" => {
            if object.contains_key("concurrency") {
                return Err(
                    "runtime_options.identity_bootstrap_mode.concurrency is only valid for lazy_with_background_warm"
                        .to_string(),
                );
            }
            meerkat_mobkit::IdentityBootstrapMode::LazyMaterialize
        }
        "lazy_with_background_warm" => {
            let concurrency = object
                .get("concurrency")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    "runtime_options.identity_bootstrap_mode.concurrency must be a positive integer"
                        .to_string()
                })?;
            let concurrency = usize::try_from(concurrency).map_err(|_| {
                "runtime_options.identity_bootstrap_mode.concurrency is too large".to_string()
            })?;
            if !(1..=MAX_IDENTITY_BACKGROUND_WARM_CONCURRENCY).contains(&concurrency) {
                return Err(format!(
                    "runtime_options.identity_bootstrap_mode.concurrency must be between 1 and {MAX_IDENTITY_BACKGROUND_WARM_CONCURRENCY}"
                ));
            }
            meerkat_mobkit::IdentityBootstrapMode::LazyWithBackgroundWarm { concurrency }
        }
        _ => {
            return Err(format!(
                "runtime_options.identity_bootstrap_mode.mode '{mode}' is unsupported"
            ));
        }
    };
    Ok(parsed)
}

fn validate_gateway_identity_bootstrap_intent(
    configured_mode: Option<&meerkat_mobkit::IdentityBootstrapMode>,
    has_roster_provider: bool,
) -> Result<(), String> {
    if configured_mode.is_some() && !has_roster_provider {
        return Err(
            "runtime_options.identity_bootstrap_mode requires an identity-first roster provider"
                .to_string(),
        );
    }
    Ok(())
}

/// Parse the shared `auth_binding {realm, binding, profile}` object of a
/// strict live registration. The binding realm must equal the registration
/// realm and the origin is always `Configured`: callers never mint a binding.
#[cfg(feature = "experimental-gpt-live")]
fn parse_gateway_live_auth_binding(
    object: &serde_json::Map<String, Value>,
    option_name: &str,
    realm: &meerkat_core::RealmId,
) -> Result<meerkat_core::AuthBindingRef, String> {
    let binding_object = object
        .get("auth_binding")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("runtime_options.{option_name}.auth_binding must be an object"))?;
    let binding_supported = ["realm", "binding", "profile"];
    let binding_unsupported = binding_object
        .keys()
        .filter(|key| !binding_supported.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !binding_unsupported.is_empty() {
        return Err(format!(
            "unsupported runtime_options.{option_name}.auth_binding fields: {}",
            binding_unsupported.join(", ")
        ));
    }
    let binding_string = |name: &str| -> Result<String, String> {
        binding_object
            .get(name)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                format!(
                    "runtime_options.{option_name}.auth_binding.{name} must be a non-empty string"
                )
            })
    };
    let binding_realm =
        meerkat_core::RealmId::parse(binding_string("realm")?).map_err(|error| {
            format!("runtime_options.{option_name}.auth_binding.realm is invalid: {error}")
        })?;
    if binding_realm != *realm {
        return Err(format!(
            "runtime_options.{option_name}.auth_binding.realm must equal {option_name}.realm"
        ));
    }
    let binding = meerkat_core::BindingId::parse(binding_string("binding")?).map_err(|error| {
        format!("runtime_options.{option_name}.auth_binding.binding is invalid: {error}")
    })?;
    let profile = match binding_object.get("profile") {
        None | Some(Value::Null) => None,
        Some(value) => {
            let profile = value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    format!(
                        "runtime_options.{option_name}.auth_binding.profile must be a non-empty string or null"
                    )
                })?;
            Some(meerkat_core::ProfileId::parse(profile).map_err(|error| {
                format!("runtime_options.{option_name}.auth_binding.profile is invalid: {error}")
            })?)
        }
    };
    Ok(meerkat_core::AuthBindingRef {
        realm: binding_realm,
        binding,
        profile,
        origin: meerkat_core::BindingOrigin::Configured,
    })
}

#[cfg(feature = "openai-live")]
fn parse_gateway_openai_live_option(value: &Value) -> Result<GatewayOpenAiLiveOption, String> {
    GatewayOpenAiLiveOption::parse(value)
        .map_err(|error| format!("runtime_options.openai_live: {error}"))
}

#[cfg(feature = "experimental-gpt-live")]
fn parse_gateway_experimental_live_option(
    value: &Value,
) -> Result<GatewayExperimentalLiveOption, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "runtime_options.experimental_live must be an object".to_string())?;
    let supported = [
        "principal",
        "realm",
        "factory_kind",
        "factory_version",
        "gate0_qualification",
        "auth_binding",
        "voice",
        "execution_profiles",
    ];
    let unsupported = object
        .keys()
        .filter(|key| !supported.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        return Err(format!(
            "unsupported runtime_options.experimental_live fields: {}",
            unsupported.join(", ")
        ));
    }
    let required_string = |name: &str| -> Result<String, String> {
        object
            .get(name)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                format!("runtime_options.experimental_live.{name} must be a non-empty string")
            })
    };
    let principal = required_string("principal")?;
    let realm_text = required_string("realm")?;
    let realm = meerkat_core::RealmId::parse(&realm_text)
        .map_err(|error| format!("runtime_options.experimental_live.realm is invalid: {error}"))?;
    let factory = meerkat::ExperimentalLiveFactoryIdentity::parse(
        required_string("factory_kind")?,
        required_string("factory_version")?,
    )
    .map_err(|error| {
        format!("runtime_options.experimental_live factory identity is invalid: {error}")
    })?;
    let qualification = meerkat::ExperimentalLiveGate0QualificationVersion::parse(required_string(
        "gate0_qualification",
    )?)
    .map_err(|error| {
        format!("runtime_options.experimental_live Gate0 qualification is invalid: {error}")
    })?;
    let binding = parse_gateway_live_auth_binding(object, "experimental_live", &realm)?;
    let execution_profiles = match object.get("execution_profiles") {
        None => Vec::new(),
        Some(value) => {
            let profiles = value.as_array().ok_or_else(|| {
                "runtime_options.experimental_live.execution_profiles must be an array".to_string()
            })?;
            let mut seen = BTreeSet::new();
            let mut parsed = Vec::with_capacity(profiles.len());
            for (index, value) in profiles.iter().enumerate() {
                let profile_object = value.as_object().ok_or_else(|| {
                    format!(
                        "runtime_options.experimental_live.execution_profiles[{index}] must be an object"
                    )
                })?;
                let profile_supported = ["profile_id", "session_instructions"];
                let profile_unsupported = profile_object
                    .keys()
                    .filter(|key| !profile_supported.contains(&key.as_str()))
                    .cloned()
                    .collect::<Vec<_>>();
                if !profile_unsupported.is_empty() {
                    return Err(format!(
                        "unsupported runtime_options.experimental_live.execution_profiles[{index}] fields: {}",
                        profile_unsupported.join(", ")
                    ));
                }
                let profile_string = |name: &str| -> Result<String, String> {
                    profile_object
                        .get(name)
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                        .ok_or_else(|| {
                            format!(
                                "runtime_options.experimental_live.execution_profiles[{index}].{name} must be a non-empty string"
                            )
                        })
                };
                let profile_id = profile_string("profile_id")?;
                if [
                    meerkat::GPT_LIVE_CLIENT_CONTEXT_PROFILE_ID,
                    meerkat::GPT_LIVE_FUNCTION_BRIDGE_PROFILE_ID,
                    meerkat::GPT_LIVE_PUBLIC_CLIENT_CONTEXT_PROFILE_ID,
                ]
                .contains(&profile_id.as_str())
                {
                    return Err(format!(
                        "runtime_options.experimental_live.execution_profiles[{index}].profile_id is reserved"
                    ));
                }
                if !seen.insert(profile_id.clone()) {
                    return Err(format!(
                        "runtime_options.experimental_live.execution_profiles contains duplicate profile_id '{profile_id}'"
                    ));
                }
                parsed.push(GatewayExperimentalLiveExecutionProfile {
                    profile_id,
                    session_instructions: profile_string("session_instructions")?,
                });
            }
            parsed
        }
    };
    Ok(GatewayExperimentalLiveOption {
        principal,
        realm,
        factory,
        qualification,
        binding,
        voice: required_string("voice")?,
        execution_profiles,
    })
}

#[cfg(feature = "experimental-gpt-live")]
fn gateway_experimental_live_operator(
    experimental: &GatewayExperimentalLiveOption,
) -> Result<meerkat::ExperimentalLiveOperatorConfig, meerkat::ExperimentalLiveAdmissionError> {
    let mut operator = meerkat::ExperimentalLiveOperatorConfig::new(
        experimental.factory.clone(),
        experimental.qualification.clone(),
    )
    .with_gpt_live_client_context_profile()?;
    for profile in &experimental.execution_profiles {
        operator = operator.with_execution_profile_session_instructions(
            profile.profile_id.clone(),
            meerkat_core::LiveExecutionMode::ClientContext,
            meerkat_core::LiveExecutionCapabilities {
                function_bridge: false,
                client_context: true,
            },
            profile.session_instructions.clone(),
        )?;
    }
    Ok(operator)
}

fn parse_gateway_runtime_options(
    params: &Value,
    persistent_state: Option<&std::path::Path>,
) -> Result<GatewayRuntimeOptions, String> {
    let Some(runtime_options) = params.get("runtime_options") else {
        return Ok(GatewayRuntimeOptions::default());
    };
    let runtime_options = runtime_options
        .as_object()
        .ok_or_else(|| "runtime_options must be a JSON object".to_string())?;
    let supported = [
        "memory_config",
        "identity_bootstrap_mode",
        "routing_config_path",
        "gating_config_path",
        "auth_config",
        "access_config_path",
        "console_config_path",
        "console_require_app_auth",
        "console_read_only",
        "console_fetch_timeout_ms",
        "demo_llm",
        "member_comms_address",
        "contacts_toml",
        "control_grants_toml",
        "max_sessions",
        "event_log",
        "agent_memory",
        "implicit_delegate_idle_retire_secs",
        "implicit_delegate_idle_sweep_interval_ms",
        "workgraph",
        "live",
        #[cfg(feature = "experimental-gpt-live")]
        "experimental_live",
        #[cfg(feature = "openai-live")]
        "openai_live",
        #[cfg(feature = "openai-live")]
        "console_voice",
        "host_runnables",
        "runtime_store",
        "mob_storage",
        "mob_composition",
        "declare_spec_update",
        "compaction",
        "session_identity_config_root",
        "meerkat_config_path",
        "http_listen",
        "http_public_base_url",
        "allow_remote",
    ];
    let unsupported = runtime_options
        .keys()
        .filter(|key| !supported.contains(&key.as_str()))
        .map(String::as_str)
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        return Err(format!(
            "unsupported runtime_options fields: {}",
            unsupported.join(", ")
        ));
    }

    let mut parsed = GatewayRuntimeOptions::default();
    if let Some(value) = runtime_options.get("identity_bootstrap_mode") {
        parsed.identity_bootstrap_mode = Some(parse_gateway_identity_bootstrap_mode(value)?);
    }
    if let Some(value) = runtime_options.get("session_identity_config_root") {
        let root = value.as_str().ok_or_else(|| {
            "runtime_options.session_identity_config_root must be a string path".to_string()
        })?;
        if root.trim().is_empty() {
            return Err(
                "runtime_options.session_identity_config_root must not be empty".to_string(),
            );
        }
        // Deliberately NOT verified for existence here. This is a config ROOT;
        // meerkat owns the `.rkat/session_comms_identity` layout beneath it and
        // is the authority on whether the material is usable. Re-deriving that
        // rule here would make MobKit a second authority on the same fact.
        parsed.session_identity_config_root = Some(PathBuf::from(root));
    }
    if let Some(value) = runtime_options.get("meerkat_config_path") {
        let path = value.as_str().ok_or_else(|| {
            "runtime_options.meerkat_config_path must be a string path".to_string()
        })?;
        if path.trim().is_empty() {
            return Err("runtime_options.meerkat_config_path must not be empty".to_string());
        }
        // Loaded here, not lazily at build time: an unreadable or malformed
        // host config is an init refusal (-32602) naming the file, never a
        // silent fall back to `Config::default()` that would resurface as
        // "self-hosted model is not registered" at the first member build.
        parsed.host_config = Some(
            meerkat_mobkit::gateway_composition::load_gateway_host_config(std::path::Path::new(
                path,
            ))
            .map_err(|error| format!("runtime_options.meerkat_config_path: {error}"))?,
        );
    }
    if let Some(value) = runtime_options.get("http_listen") {
        let listen = value
            .as_str()
            .ok_or_else(|| "runtime_options.http_listen must be a HOST:PORT string".to_string())?;
        parsed.http_listen = meerkat_mobkit::gateway_composition::parse_http_listen_addr(listen)
            .map_err(|error| format!("runtime_options.http_listen: {error}"))?;
    }
    if let Some(value) = runtime_options.get("http_public_base_url") {
        let url = value.as_str().ok_or_else(|| {
            "runtime_options.http_public_base_url must be a URL string".to_string()
        })?;
        parsed.http_public_base_url = Some(
            meerkat_mobkit::gateway_composition::parse_http_public_base_url(url)
                .map_err(|error| format!("runtime_options.http_public_base_url: {error}"))?,
        );
    }
    if let Some(value) = runtime_options.get("allow_remote") {
        parsed.allow_remote = value
            .as_bool()
            .ok_or_else(|| "runtime_options.allow_remote must be a boolean".to_string())?;
    }
    if let Some(memory_config) = runtime_options.get("memory_config") {
        parsed.runtime_options.memory_backend = Some(parse_gateway_memory_config(
            memory_config,
            persistent_state,
        )?);
    }
    if let Some(agent_memory) = runtime_options.get("agent_memory") {
        parsed.agent_memory = parse_gateway_agent_memory_config(agent_memory, persistent_state)?;
    }
    if let Some(path) = runtime_options
        .get("routing_config_path")
        .and_then(Value::as_str)
    {
        parsed.routing_routes = parse_gateway_routing_config_path(path)?;
    }
    if let Some(path) = runtime_options
        .get("gating_config_path")
        .and_then(Value::as_str)
    {
        parsed.gating = parse_gateway_gating_config_path(path)?;
    }
    if let Some(auth_config) = runtime_options.get("auth_config") {
        parsed.decisions = Some(parse_gateway_auth_config(auth_config)?);
    }
    if let Some(path) = runtime_options
        .get("console_config_path")
        .and_then(Value::as_str)
    {
        parsed.console_ui = load_console_ui_config_from_path_for_realm(path, None)
            .map_err(|err| format!("runtime_options.console_config_path is invalid: {err}"))?;
    }
    if let Some(path) = runtime_options
        .get("access_config_path")
        .and_then(Value::as_str)
    {
        parsed.access = Some(
            meerkat_mobkit::AccessController::load_or_default(path)
                .map_err(|err| format!("runtime_options.access_config_path is invalid: {err}"))?,
        );
    }
    if let Some(value) = runtime_options.get("console_require_app_auth") {
        parsed.console_require_app_auth = Some(value.as_bool().ok_or_else(|| {
            "runtime_options.console_require_app_auth must be a boolean".to_string()
        })?);
    }
    if let Some(value) = runtime_options.get("console_read_only") {
        parsed.console_read_only =
            Some(value.as_bool().ok_or_else(|| {
                "runtime_options.console_read_only must be a boolean".to_string()
            })?);
    }
    if let Some(value) = runtime_options.get("console_fetch_timeout_ms") {
        if !value.is_null() {
            let timeout_ms = value.as_u64().ok_or_else(|| {
                "runtime_options.console_fetch_timeout_ms must be a positive integer or null"
                    .to_string()
            })?;
            if timeout_ms == 0 {
                return Err(
                    "runtime_options.console_fetch_timeout_ms must be greater than zero"
                        .to_string(),
                );
            }
            parsed.console_fetch_timeout_ms = Some(timeout_ms);
        }
    }
    if let Some(value) = runtime_options.get("workgraph") {
        parsed.workgraph = match value {
            Value::Bool(true) => GatewayWorkgraphOption::Enabled,
            Value::Bool(false) => GatewayWorkgraphOption::Disabled,
            Value::String(path) if !path.trim().is_empty() => {
                GatewayWorkgraphOption::DurableDir(std::path::PathBuf::from(path))
            }
            _ => {
                return Err(
                    "runtime_options.workgraph must be a boolean or a non-empty string \
                     (the directory for the durable workgraph store)"
                        .to_string(),
                );
            }
        };
    }
    if let Some(value) = runtime_options.get("live") {
        parsed.live = match value {
            Value::Bool(true) => GatewayLiveOption::Enabled {
                public_base_url: None,
                seed_max_chars: None,
            },
            Value::Bool(false) => GatewayLiveOption::Disabled,
            Value::Object(map) => {
                let enabled = map.get("enabled").and_then(Value::as_bool).unwrap_or(true);
                let seed_max_chars = match map.get("seed_max_chars") {
                    None | Some(Value::Null) => None,
                    Some(value) => {
                        let chars = value.as_u64().filter(|chars| *chars > 0).ok_or_else(|| {
                            "runtime_options.live.seed_max_chars must be a positive integer"
                                .to_string()
                        })?;
                        Some(usize::try_from(chars).map_err(|_| {
                            "runtime_options.live.seed_max_chars is too large".to_string()
                        })?)
                    }
                };
                if enabled {
                    GatewayLiveOption::Enabled {
                        public_base_url: map
                            .get("public_base_url")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        seed_max_chars,
                    }
                } else {
                    GatewayLiveOption::Disabled
                }
            }
            _ => {
                return Err(
                    "runtime_options.live must be a boolean or an object                      ({enabled?, public_base_url?, seed_max_chars?})"
                        .to_string(),
                );
            }
        };
    }
    #[cfg(feature = "experimental-gpt-live")]
    if let Some(value) = runtime_options.get("experimental_live") {
        parsed.experimental_live = Some(parse_gateway_experimental_live_option(value)?);
    }
    #[cfg(feature = "openai-live")]
    if let Some(value) = runtime_options.get("openai_live") {
        parsed.openai_live = Some(parse_gateway_openai_live_option(value)?);
    }
    #[cfg(feature = "openai-live")]
    if let Some(value) = runtime_options.get("console_voice") {
        parsed.console_voice = Some(
            GatewayOpenAiLiveOption::parse(value)
                .map_err(|error| format!("runtime_options.console_voice: {error}"))?,
        );
        if parsed.openai_live.is_some() {
            return Err(
                "console_voice is exclusive with openai_live (both register the same public \
                 GPT Live door); pair console_voice with `live: true` to expose the external \
                 live channel beside console voice"
                    .to_string(),
            );
        }
        #[cfg(feature = "experimental-gpt-live")]
        if parsed.experimental_live.is_some() {
            return Err("console_voice is exclusive with experimental_live".to_string());
        }
        if parsed.decisions.is_none() || parsed.console_require_app_auth == Some(false) {
            return Err(
                "console_voice requires auth_config and authenticated console access".to_string(),
            );
        }
    }
    #[cfg(feature = "experimental-gpt-live")]
    if parsed.experimental_live.is_some() && parsed.openai_live.is_some() {
        return Err(
            "runtime_options.experimental_live and runtime_options.openai_live are mutually \
             exclusive: register exactly one strict GPT Live path (experimental_live is \
             deprecated; prefer openai_live)"
                .to_string(),
        );
    }
    if let Some(value) = runtime_options.get("host_runnables") {
        let entries = value.as_array().ok_or_else(|| {
            "runtime_options.host_runnables must be an array of runnable names".to_string()
        })?;
        let mut names = Vec::with_capacity(entries.len());
        for entry in entries {
            let text = entry.as_str().ok_or_else(|| {
                "runtime_options.host_runnables entries must be strings".to_string()
            })?;
            let name = meerkat::HostRunnableName::parse(text).map_err(|err| {
                format!("runtime_options.host_runnables entry '{text}' is invalid: {err}")
            })?;
            if name.as_str() == meerkat_mobkit::schedule_wiring::STEWARD_DREAM_RUNNABLE {
                return Err(format!(
                    "runtime_options.host_runnables entry '{text}' collides with the \
                     reserved steward dream runnable"
                ));
            }
            if names.contains(&name) {
                return Err(format!(
                    "runtime_options.host_runnables entry '{text}' is duplicated"
                ));
            }
            names.push(name);
        }
        parsed.host_runnables = names;
    }
    if let Some(value) = runtime_options.get("demo_llm") {
        parsed.demo_llm = value
            .as_bool()
            .ok_or_else(|| "runtime_options.demo_llm must be a boolean".to_string())?;
    }
    if let Some(value) = runtime_options.get("member_comms_address") {
        let address = value.as_str().ok_or_else(|| {
            "runtime_options.member_comms_address must be a socket address string".to_string()
        })?;
        let socket = address
            .parse::<std::net::SocketAddr>()
            .map_err(|error| format!("runtime_options.member_comms_address is invalid: {error}"))?;
        if socket.ip().is_unspecified() {
            return Err(
                "runtime_options.member_comms_address must name a concrete interface; wildcard binds cannot be advertised to external peers"
                    .to_string(),
            );
        }
        parsed.member_comms_address = Some(address.to_string());
    }
    if let Some(value) = runtime_options.get("contacts_toml") {
        let text = value
            .as_str()
            .ok_or_else(|| "runtime_options.contacts_toml must be a TOML string".to_string())?;
        parsed.contacts = Some(
            ContactDirectory::from_toml(text)
                .map_err(|error| format!("runtime_options.contacts_toml is invalid: {error}"))?,
        );
    }
    if let Some(value) = runtime_options.get("control_grants_toml") {
        let text = value.as_str().ok_or_else(|| {
            "runtime_options.control_grants_toml must be a TOML string".to_string()
        })?;
        parsed.control_grants = ControlGrantTable::from_toml(text)
            .map_err(|error| format!("runtime_options.control_grants_toml is invalid: {error}"))?
            .ok_or_else(|| {
                "runtime_options.control_grants_toml must contain a [control_grants] section"
                    .to_string()
            })?;
    }
    if let Some(value) = runtime_options.get("max_sessions") {
        let max_sessions = value
            .as_u64()
            .ok_or_else(|| "runtime_options.max_sessions must be a positive integer".to_string())?;
        if max_sessions == 0 {
            return Err("runtime_options.max_sessions must be greater than zero".to_string());
        }
        parsed.max_sessions = usize::try_from(max_sessions)
            .map_err(|_| "runtime_options.max_sessions is too large".to_string())?;
    }
    if let Some(event_log) = runtime_options.get("event_log") {
        parsed.event_log = Some(parse_gateway_event_log_config(event_log)?);
    }
    if let Some(runtime_store) = runtime_options.get("runtime_store") {
        parsed.runtime_store_ephemeral = parse_gateway_runtime_store_config(runtime_store)?;
    }
    if let Some(mob_storage) = runtime_options.get("mob_storage") {
        parsed.mob_storage_ephemeral = parse_gateway_mob_storage_config(mob_storage)?;
    }
    if let Some(composition) = runtime_options.get("mob_composition") {
        parsed.composition_authority = parse_gateway_composition_authority(composition)?;
        parsed.candidate_definition =
            parse_gateway_candidate_definition(composition, parsed.composition_authority)?;
    }
    if let Some(declare) = runtime_options.get("declare_spec_update") {
        parsed.declare_spec_update = Some(parse_gateway_declare_spec_update(declare)?);
    }
    if let Some(compaction) = runtime_options.get("compaction") {
        parsed.compaction = Some(
            meerkat_mobkit::parse_compaction_policy(compaction)
                .map_err(|error| format!("runtime_options.compaction {error}"))?,
        );
    }
    if let Some(value) = runtime_options.get("implicit_delegate_idle_retire_secs") {
        parsed.runtime_options.implicit_delegate_idle_retire_secs = if value.is_null() {
            None
        } else {
            Some(value.as_u64().ok_or_else(|| {
                "runtime_options.implicit_delegate_idle_retire_secs must be a non-negative integer or null".to_string()
            })?)
        };
    }
    if let Some(value) = runtime_options.get("implicit_delegate_idle_sweep_interval_ms") {
        let interval = value.as_u64().ok_or_else(|| {
            "runtime_options.implicit_delegate_idle_sweep_interval_ms must be a positive integer"
                .to_string()
        })?;
        if interval == 0 {
            return Err(
                "runtime_options.implicit_delegate_idle_sweep_interval_ms must be greater than zero"
                    .to_string(),
            );
        }
        parsed
            .runtime_options
            .implicit_delegate_idle_sweep_interval_ms = interval;
    }
    if let Some(require_app_auth) = parsed.console_require_app_auth {
        parsed
            .decisions
            .get_or_insert_with(minimal_decision_state)
            .console
            .require_app_auth = require_app_auth;
    }
    if let Some(read_only) = parsed.console_read_only {
        parsed
            .decisions
            .get_or_insert_with(minimal_decision_state)
            .console
            .read_only = read_only;
    }
    if let Some(fetch_timeout_ms) = parsed.console_fetch_timeout_ms {
        parsed
            .decisions
            .get_or_insert_with(minimal_decision_state)
            .console
            .fetch_timeout_ms = Some(fetch_timeout_ms);
    }
    if let Some(decisions) = parsed.decisions.as_mut() {
        decisions.console.ui = parsed.console_ui.clone();
    }
    if parsed.declare_spec_update.is_some()
        && !parsed.composition_authority.speaks_for_composition()
    {
        return Err(
            "runtime_options.declare_spec_update requires an authoritative mob_composition; a \
             candidate launch cannot mutate durable definition authority"
                .to_string(),
        );
    }
    if parsed.declare_spec_update.is_some() {
        if persistent_state.is_none() {
            return Err(
                "runtime_options.declare_spec_update requires persistent_state so the canonical \
                 mob definition survives restart"
                    .to_string(),
            );
        }
        if parsed.mob_storage_ephemeral {
            return Err(
                "runtime_options.declare_spec_update requires persistent mob storage; in-memory \
                 mob storage pins no definition to update"
                    .to_string(),
            );
        }
        if params.get("mob_config").and_then(Value::as_str).is_none() {
            return Err(
                "runtime_options.declare_spec_update requires an explicit mob_config; the gateway \
                 fallback definition is not operator-declared update content"
                    .to_string(),
            );
        }
    }
    Ok(parsed)
}

fn read_gateway_config_file(path: &str, option_name: &str) -> Result<Value, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|err| format!("failed to read runtime_options.{option_name} '{path}': {err}"))?;
    if path.ends_with(".json") {
        return serde_json::from_str(&text)
            .map_err(|err| format!("invalid JSON in runtime_options.{option_name}: {err}"));
    }
    let toml_value: toml::Value = toml::from_str(&text)
        .map_err(|err| format!("invalid TOML in runtime_options.{option_name}: {err}"))?;
    serde_json::to_value(toml_value)
        .map_err(|err| format!("failed to normalize runtime_options.{option_name}: {err}"))
}

fn parse_gateway_routing_config_path(path: &str) -> Result<Vec<RuntimeRoute>, String> {
    let value = read_gateway_config_file(path, "routing_config_path")?;
    let routes_value = value
        .get("routes")
        .cloned()
        .unwrap_or_else(|| value.clone());
    serde_json::from_value(routes_value)
        .map_err(|err| format!("runtime_options.routing_config_path routes are invalid: {err}"))
}

fn parse_gateway_gating_config_path(path: &str) -> Result<GatewayGatingConfig, String> {
    let value = read_gateway_config_file(path, "gating_config_path")?;
    let actions = value
        .get("actions")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            "runtime_options.gating_config_path must define an actions object".to_string()
        })?;
    let mut action_risk_tiers = HashMap::new();
    for (action, config) in actions {
        let risk_tier = config.as_str().or_else(|| {
            config
                .as_object()
                .and_then(|object| object.get("risk_tier"))
                .and_then(Value::as_str)
        });
        let risk_tier = risk_tier.ok_or_else(|| {
            format!("runtime_options.gating_config_path action '{action}' must define risk_tier")
        })?;
        let normalized = risk_tier.trim().to_ascii_lowercase();
        if !matches!(normalized.as_str(), "r0" | "r1" | "r2" | "r3") {
            return Err(format!(
                "runtime_options.gating_config_path action '{action}' has unsupported risk_tier '{risk_tier}'"
            ));
        }
        action_risk_tiers.insert(action.trim().to_string(), normalized);
    }
    Ok(GatewayGatingConfig { action_risk_tiers })
}

fn parse_gateway_event_log_config(value: &Value) -> Result<EventLogConfig, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "runtime_options.event_log must be a JSON object".to_string())?;
    let storage = object
        .get("storage")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            "runtime_options.event_log.storage must be 'memory' or 'null'".to_string()
        })?;
    // Declared ephemeral choices only: 'memory'/'in_memory' keeps a bounded
    // queryable in-process store, 'null' (M4) explicitly declares dropped
    // events. Both are configurations, never fallbacks — the silent case is
    // the absence of the event_log key (no ingestion at all).
    let store: Box<dyn EventLogStore> = match storage {
        "memory" | "in_memory" => Box::new(InMemoryEventLogStore::default()),
        "null" => Box::new(meerkat_mobkit::unified_runtime::NullEventLogStore),
        other => {
            return Err(format!(
                "unsupported runtime_options.event_log.storage '{other}'"
            ));
        }
    };
    let batch_size = object
        .get("batch_size")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(64);
    // `tokio::time::interval` requires a non-zero period; a zero flush
    // interval would kill the ingestion task, so it is invalid params here
    // rather than a silently clamped value.
    let flush_interval_ms = match object.get("flush_interval_ms") {
        Some(value) => {
            let flush_interval_ms = value.as_u64().ok_or_else(|| {
                "runtime_options.event_log.flush_interval_ms must be a positive integer".to_string()
            })?;
            if flush_interval_ms == 0 {
                return Err(
                    "runtime_options.event_log.flush_interval_ms must be greater than zero"
                        .to_string(),
                );
            }
            flush_interval_ms
        }
        None => 1_000,
    };
    Ok(EventLogConfig {
        store,
        filter: None,
        batch_size,
        flush_interval: Duration::from_millis(flush_interval_ms),
    })
}

/// The event-log slot census entry: an explicitly declared in-process store
/// when `runtime_options.event_log` was configured, otherwise the
/// health-visible record that operational events are not ingested at all —
/// the formerly silent case (M4).
fn gateway_event_log_slot(
    options: &GatewayRuntimeOptions,
) -> meerkat_mobkit::storage_health::StorageSlotSummary {
    if options.event_log.is_some() {
        meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
            "event_log",
            "in-process store",
            "declared via runtime_options.event_log ('memory' retains a bounded queryable \
             buffer; 'null' drops events explicitly)",
        )
    } else {
        meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
            "event_log",
            "not configured",
            "operational events are not ingested; set runtime_options.event_log to declare a \
             store explicitly",
        )
    }
}

/// The agent-memory slot for the composition-time durability census.
/// Configured SQLite composes a disk-backed store under the persistent state
/// dir. Markdown remains recognized only so initialization can return its
/// typed migration refusal; it never becomes a running storage backend.
fn agent_memory_census_slot(
    agent_memory: &GatewayAgentMemoryOptions,
) -> meerkat_mobkit::storage_health::StorageSlotSummary {
    match agent_memory.store {
        GatewayAgentMemoryStoreKind::Sqlite => {
            meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                "agent-memory",
                "SqliteAgentMemoryStore",
            )
        }
        GatewayAgentMemoryStoreKind::Markdown => {
            meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                "agent-memory",
                "MarkdownImportOnly",
            )
        }
    }
}

/// Parse `runtime_options.runtime_store`. The single accepted form is the
/// explicit ephemeral declaration `{"storage": "memory"}` (persistent SQLite
/// is the default and needs no key); everything else is a typed init error.
fn parse_gateway_runtime_store_config(value: &Value) -> Result<bool, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "runtime_options.runtime_store must be a JSON object".to_string())?;
    let storage = object
        .get("storage")
        .and_then(Value::as_str)
        .ok_or_else(|| "runtime_options.runtime_store.storage must be 'memory'".to_string())?;
    if !matches!(storage, "memory" | "in_memory") {
        return Err(format!(
            "unsupported runtime_options.runtime_store.storage '{storage}' (persistent SQLite \
             is the default; the only declaration is 'memory')"
        ));
    }
    Ok(true)
}

fn parse_gateway_composition_authority(
    value: &Value,
) -> Result<meerkat_mobkit::mob_composition_manifest::CompositionAuthority, String> {
    let object = value.as_object().ok_or_else(|| {
        "runtime_options.mob_composition must be a JSON object with an authority".to_string()
    })?;
    let authority = object
        .get("authority")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            "runtime_options.mob_composition.authority must be 'candidate' or 'authoritative'"
                .to_string()
        })?;
    match authority {
        "candidate" | "non_authoritative" => {
            Ok(meerkat_mobkit::mob_composition_manifest::CompositionAuthority::NonAuthoritative)
        }
        "authoritative" => {
            Ok(meerkat_mobkit::mob_composition_manifest::CompositionAuthority::Authoritative)
        }
        other => Err(format!(
            "unsupported runtime_options.mob_composition.authority '{other}' (use 'candidate' for \
             a certification boot that must not pin its own composition, or 'authoritative' - the \
             default - for the launch that speaks for the durable composition)"
        )),
    }
}

/// `runtime_options.mob_composition.candidate_definition`: absent or
/// `"require_match"` (a candidate whose supplied definition differs from the
/// stored one refuses), or `"stored"` (it boots the stored definition
/// knowingly). Only a candidate launch may set it: an authoritative launch is
/// judged by the composition pin.
fn parse_gateway_candidate_definition(
    value: &Value,
    authority: meerkat_mobkit::mob_composition_manifest::CompositionAuthority,
) -> Result<meerkat_mobkit::mob_composition_manifest::CandidateDefinition, String> {
    use meerkat_mobkit::mob_composition_manifest::CandidateDefinition;
    let Some(choice) = value.get("candidate_definition") else {
        return Ok(CandidateDefinition::RequireMatch);
    };
    let policy = match choice.as_str() {
        Some("require_match") => CandidateDefinition::RequireMatch,
        Some("stored") => CandidateDefinition::Stored,
        _ => {
            return Err(format!(
                "unsupported runtime_options.mob_composition.candidate_definition {choice} (use \
                 'stored' to let a candidate boot the stored mob definition when your config \
                 differs from it, or 'require_match' - the default - to refuse such a boot)"
            ));
        }
    };
    if policy == CandidateDefinition::Stored && authority.speaks_for_composition() {
        return Err(
            "runtime_options.mob_composition.candidate_definition = 'stored' requires \
             authority = 'candidate'; an authoritative launch is judged by the composition pin"
                .to_string(),
        );
    }
    Ok(policy)
}

fn parse_gateway_declare_spec_update(value: &Value) -> Result<u64, String> {
    let object = value.as_object().ok_or_else(|| {
        "runtime_options.declare_spec_update must be a JSON object with an expected_revision"
            .to_string()
    })?;
    let unsupported = object
        .keys()
        .filter(|key| key.as_str() != "expected_revision")
        .map(String::as_str)
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        return Err(format!(
            "unsupported runtime_options.declare_spec_update fields: {}",
            unsupported.join(", ")
        ));
    }
    // Required, not defaulted. A declaration without the revision it was made
    // against is not a declaration - it is "accept whatever is there", which is
    // indistinguishable from having no pin.
    let revision = object
        .get("expected_revision")
        .ok_or_else(|| {
            "runtime_options.declare_spec_update.expected_revision is required: it is the \
             revision the divergence was observed at, and declaring without it would accept a \
             spec the operator has not seen"
                .to_string()
        })?
        .as_u64()
        .ok_or_else(|| {
            "runtime_options.declare_spec_update.expected_revision must be a non-negative integer"
                .to_string()
        })?;
    Ok(revision)
}

fn parse_gateway_mob_storage_config(value: &Value) -> Result<bool, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "runtime_options.mob_storage must be a JSON object".to_string())?;
    let storage = object
        .get("storage")
        .and_then(Value::as_str)
        .ok_or_else(|| "runtime_options.mob_storage.storage must be 'memory'".to_string())?;
    if !matches!(storage, "memory" | "in_memory") {
        return Err(format!(
            "unsupported runtime_options.mob_storage.storage '{storage}' (persistent SQLite \
             is the default on a persistent_state launch; the only declaration is 'memory', \
             which trades durable adopted identity declarations for an editable mob_config)"
        ));
    }
    Ok(true)
}

fn parse_gateway_auth_config(value: &Value) -> Result<RuntimeDecisionState, String> {
    meerkat_mobkit::console_auth_config::parse_console_auth_config(value)
        .map(|mut decisions| {
            decisions.release_metadata.support_matrix = "lts".to_string();
            decisions
        })
        .map_err(|error| format!("runtime_options.{error}"))
}

fn apply_gateway_runtime_config_to_request(
    request_line: &str,
    gating: &GatewayGatingConfig,
) -> String {
    let Ok(mut request) = serde_json::from_str::<Value>(request_line) else {
        return request_line.to_string();
    };
    let method = request.get("method").and_then(Value::as_str).unwrap_or("");
    match method {
        "mobkit/gating/evaluate" => {
            let params = request.get_mut("params").and_then(Value::as_object_mut);
            if let Some(params) = params
                && let Some(action) = params
                    .get("action")
                    .and_then(Value::as_str)
                    .map(|action| action.trim().to_string())
                && let Some(risk_tier) = gating.action_risk_tiers.get(action.as_str())
            {
                // The configured table WINS over a caller-supplied tier. Filling
                // only when absent made `action_risk_tiers` a default wearing a
                // policy's name: a caller could claim `r0` for an `r3` action and
                // `evaluate_gating_action` would honour it, so the compiled table
                // was advisory against exactly the caller it needs to bind.
                if let Some(claimed) = params.get("risk_tier").and_then(Value::as_str)
                    && claimed != risk_tier
                {
                    // Never drop the claim silently. A caller that keeps sending
                    // an overridden tier must be able to learn that it is being
                    // overridden, or it will keep sending it forever.
                    tracing::warn!(
                        action = %action,
                        caller_risk_tier = %claimed,
                        policy_risk_tier = %risk_tier,
                        "gating risk_tier supplied by caller overridden by configured policy"
                    );
                }
                params.insert("risk_tier".to_string(), Value::String(risk_tier.clone()));
            }
        }
        _ => {}
    }
    serde_json::to_string(&request).unwrap_or_else(|_| request_line.to_string())
}

// Legacy state file name written by the misleadingly-named "elephant" backend
// (which always persisted local JSON). Kept so existing deployments keep their
// ledger across the rename.
const LEGACY_MEMORY_LEDGER_STATE_FILE: &str = "elephant-memory-state.json";
const MEMORY_LEDGER_STATE_FILE: &str = "memory-ledger-state.json";

fn parse_gateway_memory_config(
    memory_config: &Value,
    persistent_state: Option<&std::path::Path>,
) -> Result<MemoryBackendConfig, String> {
    let object = memory_config
        .as_object()
        .ok_or_else(|| "runtime_options.memory_config must be a JSON object".to_string())?;
    let backend = object.get("backend").and_then(Value::as_str).ok_or_else(|| {
        "runtime_options.memory_config.backend must be 'local_json' (or the deprecated 'elephant')"
            .to_string()
    })?;
    let persistent_state = persistent_state.ok_or_else(|| {
        "runtime_options.memory_config requires persistent_state so the memory ledger has a stable path"
            .to_string()
    })?;
    match backend {
        "local_json" => {
            let health_check_endpoint = match object.get("health_check_endpoint") {
                None => None,
                Some(value) => Some(
                    value
                        .as_str()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .ok_or_else(|| {
                            "runtime_options.memory_config.health_check_endpoint must be a non-empty string when provided"
                                .to_string()
                        })?
                        .to_string(),
                ),
            };
            let unsupported = object
                .keys()
                .filter(|key| key.as_str() != "backend" && key.as_str() != "health_check_endpoint")
                .map(String::as_str)
                .collect::<Vec<_>>();
            if !unsupported.is_empty() {
                return Err(format!(
                    "unsupported runtime_options.memory_config fields: {}",
                    unsupported.join(", ")
                ));
            }
            // Adopt the legacy ledger file when it is the only one present so
            // switching backend shape does not lose persisted state.
            let new_path = persistent_state.join(MEMORY_LEDGER_STATE_FILE);
            let legacy_path = persistent_state.join(LEGACY_MEMORY_LEDGER_STATE_FILE);
            let state_path = if !new_path.exists() && legacy_path.exists() {
                legacy_path
            } else {
                new_path
            };
            Ok(MemoryBackendConfig::LocalJson(
                LocalJsonMemoryBackendConfig {
                    state_path: state_path.to_string_lossy().to_string(),
                    health_check_endpoint,
                },
            ))
        }
        "elephant" => {
            let endpoint = object
                .get("endpoint")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    "runtime_options.memory_config.endpoint must be a non-empty string".to_string()
                })?;
            let unsupported = object
                .keys()
                .filter(|key| key.as_str() != "backend" && key.as_str() != "endpoint")
                .map(String::as_str)
                .collect::<Vec<_>>();
            if !unsupported.is_empty() {
                return Err(format!(
                    "unsupported runtime_options.memory_config fields: {}",
                    unsupported.join(", ")
                ));
            }
            eprintln!(
                "[mobkit-gateway] runtime_options.memory_config.backend 'elephant' is deprecated: \
                 it only health-checks the endpoint and persists the ledger as local JSON; use \
                 backend 'local_json' with an optional health_check_endpoint"
            );
            let state_path = persistent_state.join(LEGACY_MEMORY_LEDGER_STATE_FILE);
            Ok(MemoryBackendConfig::LocalJson(
                LocalJsonMemoryBackendConfig {
                    state_path: state_path.to_string_lossy().to_string(),
                    health_check_endpoint: Some(endpoint.to_string()),
                },
            ))
        }
        other => Err(format!(
            "unsupported runtime_options.memory_config.backend '{other}'"
        )),
    }
}

/// Resolve the agent-memory root through the storage layout: canonical
/// `agent-memory/`, with a legacy `agent-memory-sqlite/` corpus honored
/// where it lies (both dirs present is a twins refusal).
fn resolve_agent_memory_root(
    persistent_state: &std::path::Path,
) -> Result<std::path::PathBuf, String> {
    meerkat_mobkit::MobKitStorageLayout::with_injected_roots(persistent_state.to_path_buf(), None)
        .agent_memory_root()
        .map(|resolved| resolved.path)
        .map_err(|e| e.to_string())
}

fn parse_gateway_agent_memory_config(
    agent_memory: &Value,
    persistent_state: Option<&std::path::Path>,
) -> Result<Option<GatewayAgentMemoryOptions>, String> {
    if let Some(enabled) = agent_memory.as_bool() {
        if !enabled {
            return Ok(None);
        }
        let path = resolve_agent_memory_root(persistent_state.ok_or_else(|| {
            "runtime_options.agent_memory=true requires persistent_state".to_string()
        })?)?;
        return Ok(Some(GatewayAgentMemoryOptions {
            config: meerkat_mobkit::AgentMemoryConfig::default(),
            path,
            store: GatewayAgentMemoryStoreKind::default(),
            distiller: meerkat_mobkit::memory::distiller::DistillerConfig::default(),
            steward: meerkat_mobkit::memory::steward::StewardConfig::default(),
        }));
    }

    let object = agent_memory
        .as_object()
        .ok_or_else(|| "runtime_options.agent_memory must be a boolean or object".to_string())?;
    let supported = [
        "enabled",
        "realm",
        "selection",
        "max_entries",
        "recall_timeout_ms",
        "recall_failure_policy",
        "instruction_header",
        "per_turn_injection",
        "defang_inbound",
        "store",
        "llm_writes",
        "recorder_tool",
        "content_trust",
        "selector",
        "distiller",
        "steward",
        "operator_scope",
        "hygienist",
    ];
    let unsupported = object
        .keys()
        .filter(|key| !supported.contains(&key.as_str()))
        .map(String::as_str)
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        return Err(format!(
            "unsupported runtime_options.agent_memory fields: {}",
            unsupported.join(", ")
        ));
    }
    if let Some(enabled) = object.get("enabled") {
        let enabled = enabled
            .as_bool()
            .ok_or_else(|| "runtime_options.agent_memory.enabled must be a boolean".to_string())?;
        if !enabled {
            return Ok(None);
        }
    }

    let realm = object
        .get("realm")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("default")
        .to_string();
    let selection = match object
        .get("selection")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("contextual")
    {
        "always" => meerkat_mobkit::AgentMemorySelection::Always,
        "contextual" => meerkat_mobkit::AgentMemorySelection::Contextual,
        other => {
            return Err(format!(
                "runtime_options.agent_memory.selection must be 'always' or 'contextual' (got '{other}')"
            ));
        }
    };
    let max_entries = match object.get("max_entries") {
        None => 8,
        Some(value) => {
            let Some(value) = value.as_u64() else {
                return Err(
                    "runtime_options.agent_memory.max_entries must be a positive integer"
                        .to_string(),
                );
            };
            if value == 0 || value > 64 {
                return Err(
                    "runtime_options.agent_memory.max_entries must be between 1 and 64".to_string(),
                );
            }
            value as usize
        }
    };
    let recall_timeout_ms = match object.get("recall_timeout_ms") {
        None => 500,
        Some(value) => {
            let Some(value) = value.as_u64() else {
                return Err(
                    "runtime_options.agent_memory.recall_timeout_ms must be a positive integer"
                        .to_string(),
                );
            };
            if value == 0 || value > 30_000 {
                return Err(
                    "runtime_options.agent_memory.recall_timeout_ms must be between 1 and 30000"
                        .to_string(),
                );
            }
            value
        }
    };
    let recall_failure_policy = match object
        .get("recall_failure_policy")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("skip")
    {
        "skip" => meerkat_mobkit::AgentMemoryRecallFailurePolicy::Skip,
        "fail" => meerkat_mobkit::AgentMemoryRecallFailurePolicy::Fail,
        other => {
            return Err(format!(
                "runtime_options.agent_memory.recall_failure_policy must be 'skip' or 'fail' (got '{other}')"
            ));
        }
    };
    let instruction_header = match object.get("instruction_header") {
        None => None,
        Some(value) => Some(
            value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    "runtime_options.agent_memory.instruction_header must be a non-empty string"
                        .to_string()
                })?
                .to_string(),
        ),
    };
    // One default per policy, and the library owns it. A literal "off" here
    // forked the policy for two months (2026-07-01 .. 0.8.30): the library
    // switched its default to Budgeted when injected-context delivery became
    // echo-safe, the boolean form (`agent_memory: true`) followed it through
    // AgentMemoryConfig::default(), and the object form silently did not. Every
    // SDK client that passed an object and trusted the documented default got
    // zero turn-surface injections. Deriving the fallback from the library
    // means the next default change cannot fork again.
    // Parsed as DECLARED-or-omitted here; the effective value is resolved once
    // the store kind is known (below), because an omitted field must not
    // manufacture a "budgeted requires sqlite" refusal for a markdown config
    // that is about to be refused at init with the migration message anyway.
    let per_turn_injection_declared = match object.get("per_turn_injection") {
        None => None,
        Some(value) => match value.as_str().map(str::trim) {
            Some("off") => Some(meerkat_mobkit::AgentMemoryPerTurnInjection::Off),
            Some("budgeted") => Some(meerkat_mobkit::AgentMemoryPerTurnInjection::Budgeted),
            Some(other) => {
                return Err(format!(
                    "runtime_options.agent_memory.per_turn_injection must be 'off' or 'budgeted' (got '{other}')"
                ));
            }
            None => {
                return Err(
                    "runtime_options.agent_memory.per_turn_injection must be a string: 'off' or 'budgeted'"
                        .to_string(),
                );
            }
        },
    };
    let defang_inbound = match object.get("defang_inbound") {
        None => true,
        Some(value) => value.as_bool().ok_or_else(|| {
            "runtime_options.agent_memory.defang_inbound must be a boolean".to_string()
        })?,
    };
    let store = match object
        .get("store")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("sqlite")
    {
        "markdown" => GatewayAgentMemoryStoreKind::Markdown,
        "sqlite" => GatewayAgentMemoryStoreKind::Sqlite,
        other => {
            return Err(format!(
                "runtime_options.agent_memory.store must be 'markdown' or 'sqlite' (got '{other}')"
            ));
        }
    };
    let llm_writes = match object.get("llm_writes") {
        None => meerkat_mobkit::AgentMemoryLlmWrites::Observed,
        Some(value) => match value.as_str().map(str::trim) {
            Some("observed") => meerkat_mobkit::AgentMemoryLlmWrites::Observed,
            Some("quarantined") => meerkat_mobkit::AgentMemoryLlmWrites::Quarantined,
            _ => {
                return Err(format!(
                    "runtime_options.agent_memory.llm_writes must be 'observed' or 'quarantined' \
                     (got '{value}')"
                ));
            }
        },
    };
    let recorder_tool = match object.get("recorder_tool") {
        None => true,
        Some(value) => value.as_bool().ok_or_else(|| {
            "runtime_options.agent_memory.recorder_tool must be a boolean".to_string()
        })?,
    };
    let content_trust = match object.get("content_trust") {
        None => meerkat_mobkit::ContentTrustConfig::default(),
        Some(value) => meerkat_mobkit::ContentTrustConfig::from_json_value(value)
            .map_err(|err| format!("runtime_options.agent_memory.{err}"))?,
    };
    // §7.2 operator-scope activation. PROVISIONAL keying (§16 Q1): the
    // value name says so on purpose — deployments opting in accept that the
    // keying (console auth principal) may change when the question closes.
    let operator_scope = match object.get("operator_scope") {
        None => meerkat_mobkit::AgentMemoryOperatorScope::Off,
        Some(value) => match value.as_str().map(str::trim) {
            Some("off") => meerkat_mobkit::AgentMemoryOperatorScope::Off,
            Some("provisional") => meerkat_mobkit::AgentMemoryOperatorScope::Provisional,
            _ => {
                return Err(format!(
                    "runtime_options.agent_memory.operator_scope must be 'off' or \
                     'provisional' (got '{value}')"
                ));
            }
        },
    };
    // §8.3 selector switch: RETIRED. The LLM Selector stage shipped behind
    // this knob and was never activated (default off, in config and in the
    // MOBKIT_AGENT_MEMORY_SELECTOR env fallback alike), so it is gone and
    // recall is the deterministic lexical path on every turn.
    //
    // The KEY is still ACCEPTED, not rejected: `runtime_options` rejects
    // unknown keys fail-loud, so dropping it from the supported set would
    // brick init for any deployment that pinned `selector = "off"` - a
    // config that already asked for exactly today's behaviour.
    //
    // The VALUE is a separate question, and the answer is NOT
    // warn-and-ignore: only off/empty is accepted, and every other value is
    // REFUSED at init just below, with the reasoning stated there. Both
    // halves are pinned by the unit test
    // `retired_agent_memory_selector_accepts_off_and_refuses_the_rest`.
    // Two SDK docstrings still describe the old accept-and-warn shape and
    // are wrong about this gateway - `sdk/python/meerkat_mobkit/builder.py`
    // and `sdk/typescript/src/builder.ts`. The code here is the authority.
    if let Some(value) = object.get("selector") {
        let value = value.as_str().map(str::trim).ok_or_else(|| {
            "runtime_options.agent_memory.selector must be a string \
             (the option is retired; only 'off' remains meaningful)"
                .to_string()
        })?;
        // `off`/empty asked for exactly today's behaviour, so it is accepted
        // silently and nothing is lost. ANY OTHER VALUE ASKED FOR A STAGE THAT
        // NO LONGER EXISTS, and accepting it would silently give that caller
        // something different from what it configured - the declared-but-inert
        // failure this release program exists to remove. A warning is not
        // enough: it goes to the GATEWAY log, which the configuring consumer
        // may not be reading. A downstream fleet spent thirteen days blind to
        // a real failure for exactly that reason. So this refuses, typed, at
        // init, and names the migration.
        if !matches!(value, "" | "off") {
            return Err(format!(
                "runtime_options.agent_memory.selector = '{value}' is RETIRED and cannot be \
                 honoured: the §8.3 LLM Selector stage was removed unactivated, so recall is \
                 now the deterministic lexical path on every turn. Remove the key, or set it \
                 to 'off' to state that intent explicitly. Refusing rather than ignoring, so \
                 you learn this here instead of discovering later that a configured stage \
                 never ran."
            ));
        }
    }
    // §8.4 distiller block: fail-loud parse; enabled defaults false.
    let distiller = match object.get("distiller") {
        None => meerkat_mobkit::memory::distiller::DistillerConfig::default(),
        Some(value) => parse_gateway_distiller_config(value)?,
    };
    // §8.5 steward block: fail-loud parse; enabled defaults false.
    let steward = match object.get("steward") {
        None => meerkat_mobkit::memory::steward::StewardConfig::default(),
        Some(value) => parse_gateway_steward_config(value)?,
    };
    // §8.6 Hygienist is parked. Keep only the disabled compatibility forms;
    // any activation intent is a typed invalid-params refusal.
    if let Some(value) = object.get("hygienist") {
        parse_gateway_hygienist_compatibility(value).map_err(|error| {
            debug_assert_eq!(error.code(), -32602);
            error.message()
        })?;
    }
    // The write gate and taint tracker are store-seam machinery; only the
    // sqlite store has the seam. Accepting these knobs with the markdown
    // store would silently enforce nothing — fail loud instead.
    if store == GatewayAgentMemoryStoreKind::Markdown
        && (llm_writes != meerkat_mobkit::AgentMemoryLlmWrites::Observed
            || object.contains_key("content_trust"))
    {
        return Err(
            "runtime_options.agent_memory.llm_writes/content_trust require store='sqlite'"
                .to_string(),
        );
    }
    // And for the distiller: manifests, tombstones, and the authored-write
    // seam all live on the sqlite store.
    if store == GatewayAgentMemoryStoreKind::Markdown && distiller.enabled {
        return Err("runtime_options.agent_memory.distiller requires store='sqlite'".to_string());
    }
    // And for the steward: staging, proposals, quarantine review, and the
    // pending-harvest/promotion tables are all sqlite-store machinery.
    if store == GatewayAgentMemoryStoreKind::Markdown && steward.enabled {
        return Err("runtime_options.agent_memory.steward requires store='sqlite'".to_string());
    }
    // Operator scope composes through manifests and steward routing — both
    // sqlite-store machinery.
    if store == GatewayAgentMemoryStoreKind::Markdown
        && operator_scope != meerkat_mobkit::AgentMemoryOperatorScope::Off
    {
        return Err(
            "runtime_options.agent_memory.operator_scope requires store='sqlite'".to_string(),
        );
    }
    // And for budgeted per-turn injection: the §9.1 compaction reset sink
    // (which restores the session injection budget and dedup set when a
    // session compacts) is wired only in the sqlite arm, so a markdown
    // deployment would silently stop injecting once a session's cumulative
    // budget is spent — fail loud instead.
    let per_turn_injection = match per_turn_injection_declared {
        Some(declared) => {
            if store == GatewayAgentMemoryStoreKind::Markdown
                && declared == meerkat_mobkit::AgentMemoryPerTurnInjection::Budgeted
            {
                return Err(
                    "runtime_options.agent_memory.per_turn_injection='budgeted' requires store='sqlite'"
                        .to_string(),
                );
            }
            declared
        }
        // Omitted: the library default (Budgeted) on the store that can carry
        // it; on markdown, which has neither the ledger nor the compaction
        // reset sink, the only value the store can honour is Off. This is a
        // derived rule stated once, not a second default: it exists only
        // because the markdown backend is import-only and refused at init.
        None => {
            if store == GatewayAgentMemoryStoreKind::Markdown {
                meerkat_mobkit::AgentMemoryPerTurnInjection::Off
            } else {
                meerkat_mobkit::AgentMemoryPerTurnInjection::default()
            }
        }
    };
    let path =
        resolve_agent_memory_root(persistent_state.ok_or_else(|| {
            "runtime_options.agent_memory requires persistent_state".to_string()
        })?)?;

    Ok(Some(GatewayAgentMemoryOptions {
        config: meerkat_mobkit::AgentMemoryConfig {
            realm,
            selection,
            max_entries,
            recall_timeout_ms,
            recall_failure_policy,
            instruction_header,
            per_turn_injection,
            defang_inbound,
            llm_writes,
            recorder_tool,
            content_trust,
            operator_scope,
        },
        path,
        store,
        distiller,
        steward,
    }))
}

/// Fail-loud parse of `runtime_options.agent_memory.distiller` (§8.4):
/// `{enabled, runs_per_hour, min_interactions, model}`; unknown fields and
/// wrong types are errors, never silently ignored.
fn parse_gateway_distiller_config(
    value: &Value,
) -> Result<meerkat_mobkit::memory::distiller::DistillerConfig, String> {
    let mut config = meerkat_mobkit::memory::distiller::DistillerConfig::default();
    if let Some(enabled) = value.as_bool() {
        config.enabled = enabled;
        return Ok(config);
    }
    let object = value.as_object().ok_or_else(|| {
        "runtime_options.agent_memory.distiller must be a boolean or object".to_string()
    })?;
    let supported = [
        "enabled",
        "runs_per_hour",
        "min_interactions",
        "model",
        "max_output_tokens",
    ];
    let unsupported = object
        .keys()
        .filter(|key| !supported.contains(&key.as_str()))
        .map(String::as_str)
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        return Err(format!(
            "unsupported runtime_options.agent_memory.distiller fields: {}",
            unsupported.join(", ")
        ));
    }
    if let Some(enabled) = object.get("enabled") {
        config.enabled = enabled.as_bool().ok_or_else(|| {
            "runtime_options.agent_memory.distiller.enabled must be a boolean".to_string()
        })?;
    } else {
        // An object block without `enabled` is an explicit opt-in.
        config.enabled = true;
    }
    if let Some(value) = object.get("runs_per_hour") {
        let runs = value.as_u64().ok_or_else(|| {
            "runtime_options.agent_memory.distiller.runs_per_hour must be a positive integer"
                .to_string()
        })?;
        if runs == 0 || runs > 240 {
            return Err(
                "runtime_options.agent_memory.distiller.runs_per_hour must be between 1 and 240"
                    .to_string(),
            );
        }
        config.runs_per_hour = runs as u32;
    }
    if let Some(value) = object.get("min_interactions") {
        let min = value.as_u64().ok_or_else(|| {
            "runtime_options.agent_memory.distiller.min_interactions must be a positive integer"
                .to_string()
        })?;
        if min == 0 || min > 100 {
            return Err(
                "runtime_options.agent_memory.distiller.min_interactions must be between 1 and 100"
                    .to_string(),
            );
        }
        config.min_interactions = min as u32;
    }
    if let Some(value) = object.get("model") {
        let model = value
            .as_str()
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .ok_or_else(|| {
                "runtime_options.agent_memory.distiller.model must be a non-empty string"
                    .to_string()
            })?;
        config.model = Some(model.to_string());
    }
    // See the steward parser: this ceiling is exposed because a hard-wired one
    // failed invisibly in the field. Upper bound is the profile method's job.
    if let Some(value) = object.get("max_output_tokens") {
        let max = value.as_u64().ok_or_else(|| {
            "runtime_options.agent_memory.distiller.max_output_tokens must be a positive integer"
                .to_string()
        })?;
        if max == 0 || max > u64::from(u32::MAX) {
            return Err(
                "runtime_options.agent_memory.distiller.max_output_tokens must be a positive integer"
                    .to_string(),
            );
        }
        config.max_output_tokens = Some(max as u32);
    }
    Ok(config)
}

/// Fail-loud parse of `runtime_options.agent_memory.steward` (§8.5):
/// `{enabled, cadence, model, per_mob, runs_per_day, min_signals}`;
/// unknown fields and wrong types are errors, never silently ignored. The
/// cadence uses the scheduling subsystem's interval-marker grammar.
fn parse_gateway_steward_config(
    value: &Value,
) -> Result<meerkat_mobkit::memory::steward::StewardConfig, String> {
    let mut config = meerkat_mobkit::memory::steward::StewardConfig::default();
    if let Some(enabled) = value.as_bool() {
        config.enabled = enabled;
        return Ok(config);
    }
    let object = value.as_object().ok_or_else(|| {
        "runtime_options.agent_memory.steward must be a boolean or object".to_string()
    })?;
    let supported = [
        "enabled",
        "cadence",
        "model",
        "per_mob",
        "runs_per_day",
        "min_signals",
        "max_output_tokens",
    ];
    let unsupported = object
        .keys()
        .filter(|key| !supported.contains(&key.as_str()))
        .map(String::as_str)
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        return Err(format!(
            "unsupported runtime_options.agent_memory.steward fields: {}",
            unsupported.join(", ")
        ));
    }
    if let Some(enabled) = object.get("enabled") {
        config.enabled = enabled.as_bool().ok_or_else(|| {
            "runtime_options.agent_memory.steward.enabled must be a boolean".to_string()
        })?;
    } else {
        // An object block without `enabled` is an explicit opt-in.
        config.enabled = true;
    }
    if let Some(value) = object.get("cadence") {
        let cadence = value
            .as_str()
            .map(str::trim)
            .filter(|cadence| !cadence.is_empty())
            .ok_or_else(|| {
                "runtime_options.agent_memory.steward.cadence must be a non-empty string"
                    .to_string()
            })?;
        meerkat_mobkit::memory::steward::StewardConfig::parse_cadence(cadence)
            .map_err(|err| format!("runtime_options.agent_memory.steward.cadence: {err}"))?;
        config.cadence = cadence.to_string();
    }
    if let Some(value) = object.get("model") {
        let model = value
            .as_str()
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .ok_or_else(|| {
                "runtime_options.agent_memory.steward.model must be a non-empty string".to_string()
            })?;
        config.model = Some(model.to_string());
    }
    if let Some(value) = object.get("per_mob") {
        config.per_mob = value.as_bool().ok_or_else(|| {
            "runtime_options.agent_memory.steward.per_mob must be a boolean".to_string()
        })?;
    }
    if let Some(value) = object.get("runs_per_day") {
        let runs = value.as_u64().ok_or_else(|| {
            "runtime_options.agent_memory.steward.runs_per_day must be a positive integer"
                .to_string()
        })?;
        if runs == 0 || runs > 96 {
            return Err(
                "runtime_options.agent_memory.steward.runs_per_day must be between 1 and 96"
                    .to_string(),
            );
        }
        config.runs_per_day = runs as u32;
    }
    // Exposed because a fleet ran this steward against a
    // hard-wired ceiling and committed zero ops for four days with no reachable
    // way to raise it. The upper bound is deliberately NOT enforced here: the
    // profile method owns validation, and a ceiling the provider rejects is a
    // loud 400, whereas one that is too low fails invisibly.
    if let Some(value) = object.get("max_output_tokens") {
        let max = value.as_u64().ok_or_else(|| {
            "runtime_options.agent_memory.steward.max_output_tokens must be a positive integer"
                .to_string()
        })?;
        if max == 0 || max > u64::from(u32::MAX) {
            return Err(
                "runtime_options.agent_memory.steward.max_output_tokens must be a positive integer"
                    .to_string(),
            );
        }
        config.max_output_tokens = Some(max as u32);
    }
    if let Some(value) = object.get("min_signals") {
        let min = value.as_u64().ok_or_else(|| {
            "runtime_options.agent_memory.steward.min_signals must be a positive integer"
                .to_string()
        })?;
        if min == 0 || min > 1000 {
            return Err(
                "runtime_options.agent_memory.steward.min_signals must be between 1 and 1000"
                    .to_string(),
            );
        }
        config.min_signals = min as u32;
    }
    Ok(config)
}

/// Compatibility parser for a parked public surface. Missing is handled by
/// the caller; `false` and `{enabled:false}` remain accepted so upgrades do
/// not brick deployments that already stated the disabled posture. Every
/// activation-shaped value returns the typed parked-capability verdict.
fn parse_gateway_hygienist_compatibility(value: &Value) -> Result<(), ParkedGatewayCapability> {
    if value.as_bool() == Some(false) {
        return Ok(());
    }
    if value
        .as_object()
        .and_then(|object| object.get("enabled"))
        .and_then(Value::as_bool)
        == Some(false)
    {
        return Ok(());
    }
    Err(ParkedGatewayCapability::Hygienist)
}

/// Environment variable carrying the one request the no-argument mode
/// answers. Set by the SDKs' per-call transports and by nothing else in this
/// tree: `create_gateway_sync_transport` / `create_gateway_async_transport`
/// (Python, private) and `createGatewaySyncTransport` /
/// `createGatewayAsyncTransport` (TypeScript, exported).
const SINGLE_SHOT_REQUEST_ENV: &str = "MOBKIT_RPC_REQUEST";

/// Usage for a genuinely bare invocation: no `--persistent`, and no
/// single-shot request in the environment either. That combination used to
/// land on an `.expect()` panic whose message named an environment variable
/// and never mentioned `--persistent`, which is the mode an operator running
/// `./rpc_gateway` by hand actually wants.
const BARE_INVOCATION_USAGE: &str = r#"rpc_gateway: no mode selected.

usage:
  rpc_gateway --persistent [--control-listen <tcp://host:port | uds:///path>]
      The long-lived gateway. Reads one `mobkit/init` request on stdin, then
      serves JSON-RPC over stdin/stdout until the host closes it. Every SDK
      persistent transport and every deployment uses this mode.

  MOBKIT_RPC_REQUEST='<one JSON-RPC request>' rpc_gateway
      Single-shot module-plane probe: answers exactly one request against a
      fixed built-in demo module and exits. It hosts no mob, no session and
      no agent. The SDK per-call transports use this mode.

  rpc_gateway --version"#;

/// Single-shot mode: reads one request from the environment, answers it
/// against a fixed built-in demo module, and exits.
///
/// RETAINED DELIBERATELY. The simplification program lists this mode for
/// deletion, but the TypeScript SDK still EXPORTS the constructors that spawn
/// this binary with no arguments - `MobkitTypedClient` and
/// `MobkitAsyncClient.fromGatewayBin`, plus the two transport factories,
/// under an explicit "Low-level clients (backward compat)" heading in
/// `sdk/typescript/src/index.ts`. Deleting the mode here without deleting
/// those exports turns every downstream caller into a runtime failure with no
/// compile-time signal anywhere. The Python twins are already private
/// (`meerkat_mobkit._client`, pinned off the public surface by
/// `TestLegacySymbolsRemoved`), but `sdk/python/scripts/productization.py`
/// still drives them. The cut is one atomic change across this file,
/// `handle_mobkit_rpc_json` in `src/rpc.rs`, and the SDK factories, and it is
/// gated on the downstream census.
///
/// `args` is argv without the program name, used only to name unrecognized
/// flags in the usage error.
fn run_single_shot(args: &[String]) {
    let request = match std::env::var(SINGLE_SHOT_REQUEST_ENV) {
        Ok(request) => request,
        Err(std::env::VarError::NotPresent) => {
            if !args.is_empty() {
                eprintln!("rpc_gateway: unrecognized arguments: {}", args.join(" "));
            }
            eprintln!("{BARE_INVOCATION_USAGE}");
            std::process::exit(2);
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            eprintln!(
                "rpc_gateway: {SINGLE_SHOT_REQUEST_ENV} is set but is not valid UTF-8; it must \
                 be one JSON-RPC request object"
            );
            std::process::exit(2);
        }
    };

    let config = MobKitConfig {
        modules: vec![shell_module(
            "routing",
            r#"printf '%s\n' '{"event_id":"evt-routing","source":"module","timestamp_ms":101,"event":{"kind":"module","module":"routing","event_type":"ready","payload":{"family":"routing","health":{"state":"healthy"},"tools":{"list_method":"routing/tools.list","representative_call":{"method":"routing/tool.call","params_schema":{"tool":"string","input":"json"}}}}}}'"#,
        )],
        discovery: DiscoverySpec {
            namespace: "mobkit-rpc".to_string(),
            modules: vec!["routing".to_string()],
        },
        pre_spawn: vec![],
    };

    let mut runtime =
        start_mobkit_runtime(config, vec![], Duration::from_secs(1)).expect("runtime starts");
    let response = handle_mobkit_rpc_json(&mut runtime, &request, Duration::from_secs(1));
    print!("{response}");
    let _ = runtime.shutdown();
}

// ---------------------------------------------------------------------------
// StdioCallbackAgentBuilder — wraps FactoryAgentBuilder, sends callback/build_agent
// to Python over stdout before building the agent.
// ---------------------------------------------------------------------------

struct GatewayStdoutLine {
    response: meerkat_mobkit::SerializedRpcResponseDelivery,
    #[cfg(feature = "openai-live")]
    public_observation_delivery: Option<oneshot::Sender<bool>>,
    #[cfg(feature = "openai-live")]
    public_observation_binding: Option<LivePublicObservationBinding>,
}

#[cfg(feature = "openai-live")]
struct LivePublicObservationBinding {
    machine: Arc<meerkat_runtime::MeerkatMachine>,
    binding: meerkat_live::ProviderWebrtcBinding,
}

impl GatewayStdoutLine {
    fn plain(line: String) -> Self {
        Self {
            response: meerkat_mobkit::SerializedRpcResponseDelivery::plain(line),
            #[cfg(feature = "openai-live")]
            public_observation_delivery: None,
            #[cfg(feature = "openai-live")]
            public_observation_binding: None,
        }
    }

    fn delivery(response: meerkat_mobkit::SerializedRpcResponseDelivery) -> Self {
        Self {
            response,
            #[cfg(feature = "openai-live")]
            public_observation_delivery: None,
            #[cfg(feature = "openai-live")]
            public_observation_binding: None,
        }
    }

    #[cfg(feature = "openai-live")]
    fn public_observation(
        machine: Arc<meerkat_runtime::MeerkatMachine>,
        binding: meerkat_live::ProviderWebrtcBinding,
        line: String,
    ) -> (Self, oneshot::Receiver<bool>) {
        let (delivery, delivered) = oneshot::channel();
        (
            Self {
                response: meerkat_mobkit::SerializedRpcResponseDelivery::plain(line),
                public_observation_delivery: Some(delivery),
                public_observation_binding: Some(LivePublicObservationBinding { machine, binding }),
            },
            delivered,
        )
    }

    #[cfg(feature = "openai-live")]
    async fn acquire_public_observation_custody(
        &self,
    ) -> Result<Option<meerkat_runtime::meerkat_machine::LiveBindingPublicationCustody>, ()> {
        let Some(publication) = self.public_observation_binding.as_ref() else {
            return Ok(None);
        };
        match publication
            .machine
            .acquire_live_binding_publication_custody(&publication.binding)
            .await
            .map_err(|_| ())?
        {
            meerkat_runtime::meerkat_machine::LiveBindingPublicationAdmission::Current(custody) => {
                Ok(Some(custody))
            }
            meerkat_runtime::meerkat_machine::LiveBindingPublicationAdmission::Stale => Err(()),
        }
    }

    async fn settle_delivery(&mut self, delivered: bool) {
        let _ = self.response.settle_delivery(delivered).await;
        #[cfg(feature = "openai-live")]
        if let Some(public_observation_delivery) = self.public_observation_delivery.take() {
            let _ = public_observation_delivery.send(delivered);
        }
    }
}

impl Drop for GatewayStdoutLine {
    fn drop(&mut self) {
        #[cfg(feature = "openai-live")]
        if let Some(public_observation_delivery) = self.public_observation_delivery.take() {
            let _ = public_observation_delivery.send(false);
        }
    }
}

impl std::ops::Deref for GatewayStdoutLine {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.response.response.as_str()
    }
}

#[cfg(feature = "openai-live")]
struct GatewayExperimentalLiveSessionBindingAuthority {
    handle: meerkat_mob::MobHandle,
    machine: Arc<meerkat_runtime::MeerkatMachine>,
    access: meerkat_mobkit::AccessController,
    principal: String,
    allowed_binding: meerkat_core::AuthBindingRef,
}

#[cfg(feature = "openai-live")]
impl GatewayExperimentalLiveSessionBindingAuthority {
    async fn resolve_exact_member(
        &self,
        canonical_session_id: &meerkat_core::SessionId,
    ) -> Result<
        meerkat_mob::MemberHandle,
        meerkat::experimental_gpt_live::ExperimentalLiveOpenAuthorityError,
    > {
        use meerkat::experimental_gpt_live::ExperimentalLiveOpenAuthorityError;

        let mut owners = Vec::new();
        for member in self.handle.list_members().await {
            if self
                .handle
                .resolve_bridge_session_id(&member.agent_identity)
                .await
                .as_ref()
                == Some(canonical_session_id)
            {
                owners.push(member.agent_identity);
            }
        }
        if owners.len() != 1 {
            return Err(ExperimentalLiveOpenAuthorityError::DurableTargetUnavailable);
        }
        let identity = owners
            .pop()
            .ok_or(ExperimentalLiveOpenAuthorityError::DurableTargetUnavailable)?;
        let member = self
            .handle
            .member(&identity)
            .await
            .map_err(|_| ExperimentalLiveOpenAuthorityError::DurableTargetUnavailable)?;
        if self
            .handle
            .resolve_bridge_session_id(&identity)
            .await
            .as_ref()
            != Some(canonical_session_id)
        {
            return Err(ExperimentalLiveOpenAuthorityError::DurableTargetUnavailable);
        }
        Ok(member)
    }
}

#[cfg(feature = "openai-live")]
#[async_trait]
impl meerkat::experimental_gpt_live::ExperimentalLiveSessionBindingAuthority
    for GatewayExperimentalLiveSessionBindingAuthority
{
    async fn validate_live_durable_source_availability(
        &self,
        canonical_session_id: &meerkat_core::SessionId,
    ) -> Result<(), meerkat::experimental_gpt_live::ExperimentalLiveOpenAuthorityError> {
        use meerkat::experimental_gpt_live::ExperimentalLiveOpenAuthorityError;

        let member = self.resolve_exact_member(canonical_session_id).await?;
        member
            .validate_live_durable_source_availability()
            .await
            .map_err(|_| ExperimentalLiveOpenAuthorityError::DurableTargetUnavailable)?;
        if self
            .handle
            .resolve_bridge_session_id(&member.identity())
            .await
            .as_ref()
            != Some(canonical_session_id)
        {
            return Err(ExperimentalLiveOpenAuthorityError::DurableTargetUnavailable);
        }
        Ok(())
    }

    async fn authorize_binding_use(
        &self,
        canonical_session_id: &meerkat_core::SessionId,
        selected_binding: &meerkat_core::AuthBindingRef,
    ) -> Result<
        meerkat::experimental_gpt_live::ExperimentalLiveSessionBindingAuthorization,
        meerkat::experimental_gpt_live::ExperimentalLiveOpenAuthorityError,
    > {
        use meerkat::experimental_gpt_live::ExperimentalLiveOpenAuthorityError;

        if selected_binding != &self.allowed_binding {
            return Err(ExperimentalLiveOpenAuthorityError::BindingUseDenied);
        }
        let owner = self.resolve_exact_member(canonical_session_id).await?;
        let durable_identity = owner.identity().as_str().to_string();
        let access = self.access.view_for_subject(Some(&self.principal));
        if !access.allows_agent(meerkat_mobkit::access::ACTION_AGENT_SEND, &durable_identity) {
            return Err(ExperimentalLiveOpenAuthorityError::AccessDenied);
        }

        let principal = meerkat_core::PrincipalRef::new(
            meerkat_core::PrincipalKind::Human,
            self.principal.clone(),
        )
        .map_err(|_| ExperimentalLiveOpenAuthorityError::BindingUseDenied)?;
        let target = meerkat_core::PrincipalRef::new(
            meerkat_core::PrincipalKind::PersonalAgent,
            durable_identity,
        )
        .map_err(|_| ExperimentalLiveOpenAuthorityError::BindingUseDenied)?;
        let request = meerkat_core::AuthBindingUseRequest::new(
            principal.clone(),
            target.clone(),
            selected_binding.clone(),
        );
        let grant = meerkat_core::AuthGrant {
            principal: principal.clone(),
            scope: meerkat_core::GrantScope::AuthBinding {
                realm_id: selected_binding.realm.clone(),
                binding_id: selected_binding.binding.clone(),
                profile_id: selected_binding.profile.clone(),
            },
            actions: std::collections::BTreeSet::from([meerkat_core::GrantAction::UseAuthBinding]),
            acting_on_behalf_of: Some(meerkat_core::ActingOnBehalfOf::new(principal, target)),
        };
        let binding_use = meerkat_core::authorize_explicit_auth_binding_use(&request, &[grant])
            .into_result()
            .map_err(|_| ExperimentalLiveOpenAuthorityError::BindingUseDenied)?;
        Ok(
            meerkat::experimental_gpt_live::ExperimentalLiveSessionBindingAuthorization::from_machine_authority(
                binding_use,
                self.machine.generated_auth_lease_handle(),
            ),
        )
    }
}

#[cfg(feature = "openai-live")]
#[derive(Clone)]
struct StdioExperimentalLivePublicObservationPublisher {
    machine: Arc<meerkat_runtime::MeerkatMachine>,
    callback_bridge: StdioCallbackBridge,
}

#[cfg(feature = "openai-live")]
impl StdioExperimentalLivePublicObservationPublisher {
    fn new(
        machine: Arc<meerkat_runtime::MeerkatMachine>,
        callback_bridge: StdioCallbackBridge,
    ) -> Self {
        Self {
            machine,
            callback_bridge,
        }
    }
}

#[cfg(feature = "openai-live")]
#[async_trait]
impl meerkat::experimental_gpt_live::ExperimentalLivePublicObservationPublisher
    for StdioExperimentalLivePublicObservationPublisher
{
    async fn publish(
        &self,
        observation: meerkat::experimental_gpt_live::ExperimentalLivePublicObservation,
    ) -> Result<(), meerkat::experimental_gpt_live::ExperimentalLivePublicObservationDeliveryError>
    {
        use meerkat::experimental_gpt_live::ExperimentalLivePublicObservationDeliveryError;

        if observation.binding().channel_id() != &observation.output().channel_id {
            return Err(ExperimentalLivePublicObservationDeliveryError::Rejected);
        }
        let result = self
            .callback_bridge
            .call_live_public_observation(
                Arc::clone(&self.machine),
                observation.binding().clone(),
                observation.output(),
            )
            .await
            .map_err(|error| {
                if error == "callback transport closed" || error == "stdout channel closed" {
                    ExperimentalLivePublicObservationDeliveryError::Closed
                } else {
                    ExperimentalLivePublicObservationDeliveryError::Rejected
                }
            })?;
        if result.get("accepted").and_then(Value::as_bool) == Some(true) {
            Ok(())
        } else {
            Err(ExperimentalLivePublicObservationDeliveryError::Rejected)
        }
    }
}

impl std::fmt::Display for GatewayStdoutLine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self)
    }
}

async fn write_gateway_stdout_line(line: &mut GatewayStdoutLine) -> bool {
    #[cfg(feature = "openai-live")]
    let _public_observation_custody = match line.acquire_public_observation_custody().await {
        Ok(custody) => custody,
        Err(()) => return false,
    };
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{line}")
        .and_then(|()| stdout.flush())
        .is_ok()
}

/// The `cb-<n>` id of a stdin line that failed to parse, when the line is a
/// callback response whose top-level `"id"` member is still readable. Only an
/// unescaped `"id"` key counts, so a `cb-` string inside escaped content
/// (a user message quoting JSON) does not match.
fn malformed_callback_response_id(line: &str) -> Option<String> {
    let bytes = line.as_bytes();
    let mut from = 0;
    while let Some(offset) = line[from..].find("\"id\"") {
        let key = from + offset;
        from = key + 4;
        if key > 0 && bytes[key - 1] == b'\\' {
            continue;
        }
        let rest = line[key + 4..].trim_start();
        let Some(rest) = rest.strip_prefix(':') else {
            continue;
        };
        let Some(rest) = rest.trim_start().strip_prefix("\"cb-") else {
            continue;
        };
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        if !digits.is_empty() && rest[digits.len()..].starts_with('"') {
            return Some(format!("cb-{digits}"));
        }
    }
    None
}

/// Shared handle for sending lines to stdout and receiving callback responses.
#[derive(Clone)]
struct StdioCallbackBridge {
    /// Send a line to stdout (the stdout writer task reads from this).
    stdout_tx: mpsc::Sender<GatewayStdoutLine>,
    /// Callback admission and pending responses share one lock so EOF cannot
    /// race a late insertion after pending callers have been cancelled.
    state: Arc<Mutex<StdioCallbackState>>,
    /// Counter for generating unique callback IDs.
    counter: Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Default)]
struct StdioCallbackState {
    closed: bool,
    pending: HashMap<String, oneshot::Sender<Value>>,
}

const GATEWAY_SHUTDOWN_METHOD: &str = "mobkit/shutdown";

// Shutdown is negotiated with SDK hosts because the callback bridge must stay
// open while identity-owned cleanup runs. The runtime budget deliberately
// covers two complete callback windows: one for an already-admitted identity
// operation which shutdown must join, and one for the final batched lease
// release. The 317-second runtime budget is exactly two 130-second provider
// callback windows, the runtime's 30-second event drain, its 10-second mob
// quiesce window, 10 seconds of scheduler overhead, the 2-second
// retired-supervisor join, and the 5-second mob terminal shutdown that
// produces the per-member shutdown report.
const PROVIDER_CALLBACK_TIMEOUT: Duration = Duration::from_secs(130);
const GATEWAY_RPC_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const GATEWAY_RUNTIME_EVENT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const GATEWAY_STDOUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

// The bounded gateway phases total at most 342 seconds (5 + 5 + 10 + 317 + 5).
// Advertise another 10 seconds for response delivery and process reaping so
// an SDK never races the gateway's own deadline and preempts a valid callback.
const GATEWAY_SHUTDOWN_HORIZON_MS: u64 = 352_000;

// -- mobkit/init: accepted, then settled (#550) ------------------------------
//
// An SDK that sends `init_protocol: "accepted_then_settled"` and an `init_id`
// gets an `accepted` response as soon as the pre-durable gates pass, then
// `mobkit/init_progress` notifications at each startup phase and exactly one
// `mobkit/init_settled` notification (`ready` with the init result fields, or
// `failed` with the error and `durable_effects`). An SDK that does not opt in
// gets the single legacy response written when init finishes; it never sees
// `accepted`.

const INIT_PROTOCOL_ACCEPTED_THEN_SETTLED: &str = "accepted_then_settled";
const INIT_PROGRESS_METHOD: &str = "mobkit/init_progress";
const INIT_SETTLED_METHOD: &str = "mobkit/init_settled";

/// A startup phase reported as `mobkit/init_progress`. Boundaries follow the
/// native init order: storage open, preliminary owner publication, runtime
/// prepare with module startup, the identity roster callback, owner
/// registration, persisted-authority prewarm, activation, roster restore,
/// schedules, serving setup, and failure cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum InitPhase {
    /// First touch of durable state (callback-job store, identity substrate,
    /// stores). Everything before this phase is positive evidence for
    /// `durable_effects: "none"`.
    Storage,
    /// Roster callback and persisted-owner publication before prepare.
    OwnerPublication,
    /// Runtime and storage prepare, including module startup.
    Prepare,
    /// Roster provider callback for identity bootstrap.
    Roster,
    RegisterOwners,
    /// Persisted-authority convergence; deliberately unbounded.
    Prewarm,
    Activate,
    Restore,
    /// Schedule driver and the remaining runtime wiring.
    Schedules,
    /// HTTP, live and console setup before readiness.
    Serve,
    /// Failure cleanup (runtime shutdown, which can await external
    /// lease-release callbacks) before `init_settled` reports the failure.
    Cleanup,
}

impl From<meerkat_mobkit::UnifiedRuntimeBootstrapPhase> for InitPhase {
    fn from(phase: meerkat_mobkit::UnifiedRuntimeBootstrapPhase) -> Self {
        use meerkat_mobkit::UnifiedRuntimeBootstrapPhase as P;
        match phase {
            P::RegisterPersistedOwners => Self::RegisterOwners,
            P::PrewarmPersistedAuthority => Self::Prewarm,
            P::Activate => Self::Activate,
            P::RestoreRoster => Self::Restore,
            P::FailureCleanup => Self::Cleanup,
            // A phase added upstream is still progress; report it under the
            // nearest gateway phase rather than dropping the transition.
            _ => Self::Activate,
        }
    }
}

/// How the SDK asked to run this init.
#[derive(Debug, Clone, PartialEq, Eq)]
enum InitProtocolRequest {
    Legacy,
    AcceptedThenSettled { init_id: String },
}

/// Read the opt-in from the init params. Only the exact protocol name with a
/// non-empty `init_id` opts in; anything else is the legacy protocol, so an
/// unknown future protocol name never gets a response shape it did not ask for.
fn init_protocol_request(params: &Value) -> InitProtocolRequest {
    if params.get("init_protocol").and_then(Value::as_str)
        != Some(INIT_PROTOCOL_ACCEPTED_THEN_SETTLED)
    {
        return InitProtocolRequest::Legacy;
    }
    match params.get("init_id").and_then(Value::as_str) {
        Some(init_id) if !init_id.trim().is_empty() => InitProtocolRequest::AcceptedThenSettled {
            init_id: init_id.to_string(),
        },
        _ => InitProtocolRequest::Legacy,
    }
}

/// The one init this process runs. Set once, right after the init request is
/// parsed; read by `fail_init` so every refusal site settles the same way.
struct InitSession {
    request_id: Value,
    protocol: InitProtocolRequest,
    accepted: AtomicBool,
    /// False until the first operation that may write durable state begins
    /// (`InitPhase::Storage`). `durable_effects` is "none" only while false.
    durable_phase_begun: AtomicBool,
    settled: AtomicBool,
}

static INIT_SESSION: std::sync::OnceLock<InitSession> = std::sync::OnceLock::new();

fn init_session() -> Option<&'static InitSession> {
    INIT_SESSION.get()
}

fn write_stdout_line_now(value: &Value) {
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(
        stdout,
        "{}",
        serde_json::to_string(value)
            .unwrap_or_else(|_| r#"{"error":"serialization failed"}"#.to_string())
    );
    let _ = stdout.flush();
}

impl InitSession {
    fn init_id(&self) -> Option<&str> {
        match &self.protocol {
            InitProtocolRequest::AcceptedThenSettled { init_id } => Some(init_id),
            InitProtocolRequest::Legacy => None,
        }
    }

    fn is_accepted(&self) -> bool {
        self.accepted.load(Ordering::Acquire)
    }

    fn durable_effects(&self) -> &'static str {
        if self.durable_phase_begun.load(Ordering::Acquire) {
            "possible"
        } else {
            "none"
        }
    }

    /// The `accepted` response. Written synchronously so it precedes every
    /// progress, callback and settlement line of this init.
    fn accepted_response(&self, init_id: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": self.request_id,
            "result": {
                "init_state": "accepted",
                "init_id": init_id,
                "provider_callback_timeout_ms": PROVIDER_CALLBACK_TIMEOUT.as_millis() as u64,
                "stdio_shutdown_handshake": true,
                "stdio_shutdown_horizon_ms": GATEWAY_SHUTDOWN_HORIZON_MS,
            }
        })
    }

    fn accept(&self) {
        let Some(init_id) = self.init_id() else {
            return;
        };
        if self.accepted.swap(true, Ordering::AcqRel) {
            return;
        }
        write_stdout_line_now(&self.accepted_response(init_id));
    }

    fn progress_notification(&self, phase: InitPhase) -> Option<Value> {
        let init_id = self.init_id()?;
        if !self.is_accepted() || self.settled.load(Ordering::Acquire) {
            return None;
        }
        Some(json!({
            "jsonrpc": "2.0",
            "method": INIT_PROGRESS_METHOD,
            "params": { "init_id": init_id, "phase": phase },
        }))
    }

    /// The init result as the legacy response or the `ready` settlement.
    fn ready_line(&self, result: Value) -> Value {
        match self.init_id() {
            Some(init_id) if self.is_accepted() => {
                let mut params = result;
                if let Some(object) = params.as_object_mut() {
                    object.insert("init_id".to_string(), json!(init_id));
                    object.insert("outcome".to_string(), json!("ready"));
                }
                json!({ "jsonrpc": "2.0", "method": INIT_SETTLED_METHOD, "params": params })
            }
            _ => json!({ "jsonrpc": "2.0", "id": self.request_id, "result": result }),
        }
    }

    /// The init failure as the legacy error response or the `failed`
    /// settlement. Once accepted, the request id has already been answered,
    /// so the error travels in the settlement with `durable_effects`.
    fn failed_line(&self, code: i64, message: &str, data: Option<Value>) -> Value {
        match self.init_id() {
            Some(init_id) if self.is_accepted() => {
                let mut params = json!({
                    "init_id": init_id,
                    "outcome": "failed",
                    "code": code,
                    "message": message,
                    "durable_effects": self.durable_effects(),
                });
                if let Some(data) = data {
                    params["data"] = data;
                }
                json!({ "jsonrpc": "2.0", "method": INIT_SETTLED_METHOD, "params": params })
            }
            _ => {
                let mut error = json!({ "code": code, "message": message });
                if let Some(data) = data {
                    error["data"] = data;
                }
                json!({ "jsonrpc": "2.0", "id": self.request_id, "error": error })
            }
        }
    }
}

/// `-32098`, shared with the shutdown-in-progress refusals: the init stopped
/// because the SDK asked the gateway to shut down before init settled.
const INIT_STOPPED_FOR_SHUTDOWN_CODE: i64 = -32098;

/// The `data` of a composition provenance init refusal
/// ([`COMPOSITION_PROVENANCE_CODE`]): the refusal's kind and the facts it
/// carries, such as the diverged fields. `None` only if serialization fails,
/// which leaves the typed code and the message intact.
fn composition_refusal_data(
    refusal: &meerkat_mobkit::mob_composition_manifest::MobCompositionProvenanceError,
) -> Option<Value> {
    match serde_json::to_value(refusal.refusal()) {
        Ok(data) => Some(data),
        Err(error) => {
            tracing::warn!(%error, "composition provenance refusal data did not serialize");
            None
        }
    }
}

/// `mobkit/init_progress` emission. Like `accepted` and the settlement, each
/// progress line is written synchronously by the task entering the phase, so
/// the three keep their order (a `cleanup` phase always precedes the failed
/// settlement it explains) and none is dropped under channel backpressure.
/// Callback lines travel through the stdout writer queue independently; their
/// order relative to progress carries no meaning.
#[derive(Clone, Copy)]
struct InitProgress;

impl InitProgress {
    fn enter(self, phase: InitPhase) {
        let Some(session) = init_session() else {
            return;
        };
        if phase == InitPhase::Storage {
            session.durable_phase_begun.store(true, Ordering::Release);
        }
        if let Some(line) = session.progress_notification(phase) {
            write_stdout_line_now(&line);
        }
    }
}

/// A `mobkit/shutdown` request arrived while init was still running. Stop at
/// this phase boundary: shut the runtime down when one exists (which can
/// await lease-release callbacks), settle the init `failed` only after that
/// cleanup settled, then answer the shutdown request and exit.
async fn abort_init_for_shutdown(
    runtime: Option<&meerkat_mobkit::UnifiedRuntime>,
    rpc_rx: &mut mpsc::Receiver<String>,
    progress: InitProgress,
) -> ! {
    progress.enter(InitPhase::Cleanup);
    let report = match runtime {
        Some(runtime) => Some(runtime.shutdown().await),
        None => None,
    };
    let cleanup_completed = report
        .as_ref()
        .is_none_or(UnifiedRuntimeShutdownReport::cleanup_completed);
    if let Some(session) = init_session() {
        if !cleanup_completed {
            // Cleanup that did not complete may have left durable state.
            session.durable_phase_begun.store(true, Ordering::Release);
        }
        session.settled.store(true, Ordering::Release);
        write_stdout_line_now(&session.failed_line(
            INIT_STOPPED_FOR_SHUTDOWN_CODE,
            "init stopped: a gateway shutdown was requested before init settled",
            Some(json!({
                "reason": "shutdown_requested",
                "cleanup_completed": cleanup_completed,
            })),
        ));
    }
    while let Some(line) = rpc_rx.recv().await {
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if let Some(request) = gateway_shutdown_request(&message) {
            let response = match report.as_ref() {
                Some(report) => gateway_shutdown_response(request.response_id, Some(report)),
                // No runtime was built yet, so there is nothing to clean up.
                None => json!({
                    "jsonrpc": "2.0",
                    "id": request.response_id,
                    "result": { "shutdown": true, "runtime_cleanup_completed": true },
                }),
            };
            write_stdout_line_now(&response);
            break;
        }
        if let Some(id) = message.get("id") {
            write_stdout_line_now(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": INIT_STOPPED_FOR_SHUTDOWN_CODE,
                    "message": "gateway shutdown in progress"
                },
            }));
        }
    }
    std::process::exit(0);
}

/// The immediate answer to an ordinary request that arrived before init
/// settled; `None` for a notification (no id), which gets no answer.
fn init_in_progress_refusal(message: &Value) -> Option<Value> {
    let id = message.get("id")?;
    let method = message.get("method").and_then(Value::as_str).unwrap_or("");
    Some(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": meerkat_mobkit::INIT_IN_PROGRESS_CODE,
            "message": format!(
                "{method} refused: mobkit/init has not settled yet; ordinary requests are \
                 served only after init settles (startup callbacks must not issue them)"
            ),
            "data": { "kind": "init_in_progress", "method": method },
        },
    }))
}

#[derive(Debug)]
struct GatewayShutdownRequest {
    response_id: Value,
}

fn gateway_shutdown_request(message: &Value) -> Option<GatewayShutdownRequest> {
    if message.get("method").and_then(Value::as_str) != Some(GATEWAY_SHUTDOWN_METHOD) {
        return None;
    }
    // The private SDK handshake is deliberately request/response, not a
    // notification: the host must keep stdin open until runtime cleanup
    // (including provider callbacks) is complete.
    Some(GatewayShutdownRequest {
        response_id: message.get("id")?.clone(),
    })
}

/// Name WHICH cleanup phase blocked, for a failed shutdown only.
///
/// `cleanup_completed` is a four-way conjunction (drain not timed out, mob stop
/// ok, identity authority released or unconfigured, zero orphan processes), and
/// the wire response used to carry only its result. So a caller learned that
/// cleanup failed and nothing about which of four unrelated phases failed,
/// which left a recurring CI shutdown cluster undiagnosed across four
/// occurrences: every test faithfully printed a response from which the cause
/// had already been erased.
///
/// Typed rather than an encoded string list, so counts stay counts and a new
/// phase can be added without re-parsing prose.
fn gateway_shutdown_diagnostics(runtime_shutdown: Option<&UnifiedRuntimeShutdownReport>) -> Value {
    let Some(report) = runtime_shutdown else {
        // Distinct from any phase failing: there was no report at all, so
        // nothing ran to completion or otherwise.
        return json!({ "runtime_shutdown_report": "absent" });
    };
    json!({
        "runtime_shutdown_report": "present",
        "drain": {
            "timed_out": report.drain.timed_out,
            "drained_count": report.drain.drained_count,
            "drain_duration_ms": report.drain.drain_duration_ms
        },
        // Typed phase status and numeric facts ONLY. No error Display and no
        // provider message: those are unstable across versions and can carry
        // paths or credentials, and this response crosses the SDK wire. The
        // point is to name WHICH phase blocked, which the operator can then
        // pair with the gateway's own logs.
        "mob_stop": match &report.mob_stop {
            Ok(()) => "ok",
            Err(_) => "failed",
        },
        "identity_authority_release": match &report.identity_authority_release {
            IdentityAuthorityReleaseOutcome::NotConfigured => {
                json!({ "outcome": "not_configured" })
            }
            IdentityAuthorityReleaseOutcome::Released { grant_count } => {
                json!({ "outcome": "released", "grant_count": grant_count })
            }
            IdentityAuthorityReleaseOutcome::Failed { .. } => {
                json!({ "outcome": "failed" })
            }
            IdentityAuthorityReleaseOutcome::SkippedResetCleanupFailed { .. } => {
                json!({ "outcome": "skipped_reset_cleanup_failed" })
            }
            IdentityAuthorityReleaseOutcome::SkippedMobStopFailed => {
                json!({ "outcome": "skipped_mob_stop_failed" })
            }
        },
        "module_shutdown": { "orphan_processes": report.module_shutdown.orphan_processes },
        // This phase gates cleanup_completed(), so it CAN be the sole reason
        // this response exists. That is why it must appear here: diagnostics
        // attach only when cleanup did not complete, so a gating phase absent
        // from them would be unreportable in exactly its own failure case.
        "retired_supervisor_cleanup": match &report.retired_supervisor_cleanup {
            RetiredSupervisorCleanupOutcome::NothingPending => {
                json!({ "outcome": "nothing_pending" })
            }
            RetiredSupervisorCleanupOutcome::Joined {
                lease_renewal,
                continuity_repair,
            } => json!({
                "outcome": "joined",
                "lease_renewal": lease_renewal,
                "continuity_repair": continuity_repair
            }),
            RetiredSupervisorCleanupOutcome::Incomplete {
                joined,
                join_failed,
                pending,
            } => json!({
                "outcome": "incomplete",
                "joined": joined,
                "join_failed": join_failed,
                "pending": pending
            }),
            _ => json!({ "outcome": "unclassified" }),
        }
    })
}

fn gateway_shutdown_response(
    response_id: Value,
    runtime_shutdown: Option<&UnifiedRuntimeShutdownReport>,
) -> Value {
    let runtime_cleanup_completed =
        runtime_shutdown.is_some_and(UnifiedRuntimeShutdownReport::cleanup_completed);
    let mut result = json!({
        // Future completion alone is not successful authority cleanup.
        // SDKs validate these fields and surface failure only after
        // reaping the gateway.
        "shutdown": runtime_cleanup_completed,
        "runtime_cleanup_completed": runtime_cleanup_completed
    });
    if !runtime_cleanup_completed {
        // Failure only. A successful shutdown keeps the response byte-identical
        // to what every existing SDK check already validates.
        result["runtime_cleanup_diagnostics"] = gateway_shutdown_diagnostics(runtime_shutdown);
    }
    json!({
        "jsonrpc": "2.0",
        "id": response_id,
        "result": result
    })
}

impl StdioCallbackBridge {
    fn new(stdout_tx: mpsc::Sender<GatewayStdoutLine>) -> Self {
        Self {
            stdout_tx,
            state: Arc::new(Mutex::new(StdioCallbackState::default())),
            counter: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Send a fire-and-forget notification to Python (no response expected).
    /// Uses `try_send` — may drop under backpressure.
    fn notify(&self, method: &str, params: Value) {
        let notification = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        if let Ok(line) = serde_json::to_string(&notification) {
            let _ = self.stdout_tx.try_send(GatewayStdoutLine::plain(line));
        }
    }

    async fn send_stdout(
        &self,
        line: GatewayStdoutLine,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        match tokio::time::timeout_at(deadline, self.stdout_tx.send(line)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err("stdout channel closed".to_string()),
            Err(_) => Err(format!(
                "callback stdout send timed out after {}s; delivery outcome unknown",
                PROVIDER_CALLBACK_TIMEOUT.as_secs()
            )),
        }
    }

    async fn send_callback(
        &self,
        line: GatewayStdoutLine,
        response: &mut oneshot::Receiver<Value>,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        tokio::select! {
            biased;
            // EOF must wake a callback even while stdout has no capacity.
            _ = response => Err("callback response channel dropped".to_string()),
            result = self.send_stdout(line, deadline) => result,
        }
    }

    /// Wait boundedly for notification admission; failures must be reported.
    /// Queue admission alone does not acknowledge delivery to the SDK.
    async fn notify_reliable(&self, method: &str, params: Value) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + PROVIDER_CALLBACK_TIMEOUT;
        let notification = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        let line = serde_json::to_string(&notification).map_err(|error| error.to_string())?;
        self.send_stdout(GatewayStdoutLine::plain(line), deadline)
            .await
    }

    /// Send a callback request to Python and wait for the response.
    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        // Admission and response share the existing provider callback window.
        let deadline = tokio::time::Instant::now() + PROVIDER_CALLBACK_TIMEOUT;
        let id = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id_str = format!("cb-{id}");

        let (tx, mut rx) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            if state.closed {
                return Err("callback transport closed".to_string());
            }
            state.pending.insert(id_str.clone(), tx);
        }

        let request = json!({
            "jsonrpc": "2.0",
            "id": id_str,
            "method": method,
            "params": params,
        });
        let line = match serde_json::to_string(&request) {
            Ok(l) => l,
            Err(e) => {
                self.state.lock().await.pending.remove(&id_str);
                return Err(e.to_string());
            }
        };
        if let Err(error) = self
            .send_callback(GatewayStdoutLine::plain(line), &mut rx, deadline)
            .await
        {
            self.state.lock().await.pending.remove(&id_str);
            return Err(error);
        }

        // Wait for Python to respond (routed by the stdin multiplexer)
        match tokio::time::timeout_at(deadline, rx).await {
            Ok(Ok(value)) => {
                if let Some(error) = value.get("error") {
                    Err(format!(
                        "callback error: {}",
                        error
                            .get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("unknown")
                    ))
                } else {
                    Ok(value.get("result").cloned().unwrap_or(Value::Null))
                }
            }
            Ok(Err(_)) => Err("callback response channel dropped".to_string()),
            Err(_) => {
                self.state.lock().await.pending.remove(&id_str);
                Err(format!(
                    "callback timed out after {}s; execution outcome unknown",
                    PROVIDER_CALLBACK_TIMEOUT.as_secs()
                ))
            }
        }
    }

    /// Publish one exact live output as an acknowledged SDK callback.
    ///
    /// The writer acquires opaque machine custody immediately before writing
    /// and holds it through flush. The callback must then explicitly accept
    /// the bounded SDK queue entry. A stale binding, writer failure, missing
    /// consumer, or queue overflow is returned to the provider pump as a
    /// delivery failure instead of silently losing playback authority.
    #[cfg(feature = "openai-live")]
    async fn call_live_public_observation(
        &self,
        machine: Arc<meerkat_runtime::MeerkatMachine>,
        binding: meerkat_live::ProviderWebrtcBinding,
        params: impl serde::Serialize,
    ) -> Result<Value, String> {
        let deadline = tokio::time::Instant::now() + PROVIDER_CALLBACK_TIMEOUT;
        let id = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id_str = format!("cb-{id}");

        let (tx, mut rx) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            if state.closed {
                return Err("callback transport closed".to_string());
            }
            state.pending.insert(id_str.clone(), tx);
        }

        let request = json!({
            "jsonrpc": "2.0",
            "id": id_str,
            "method": "mobkit/live/assistant_output_available",
            "params": params,
        });
        let line = match serde_json::to_string(&request) {
            Ok(line) => line,
            Err(error) => {
                self.state.lock().await.pending.remove(&id_str);
                return Err(error.to_string());
            }
        };
        let (line, written) = GatewayStdoutLine::public_observation(machine, binding, line);
        if let Err(error) = self.send_callback(line, &mut rx, deadline).await {
            self.state.lock().await.pending.remove(&id_str);
            return Err(error);
        }
        let mut early_response = None;
        let delivery = tokio::time::timeout_at(deadline, written);
        tokio::pin!(delivery);
        let delivery = tokio::select! {
            biased;
            delivery = &mut delivery => delivery,
            response = &mut rx => match response {
                Ok(value) => {
                    // The SDK can acknowledge before the writer task settles.
                    // Keep that response, but still require writer delivery.
                    early_response = Some(value);
                    delivery.await
                }
                Err(_) => {
                    self.state.lock().await.pending.remove(&id_str);
                    return Err("callback response channel dropped".to_string());
                }
            },
        };
        match delivery {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => {
                self.state.lock().await.pending.remove(&id_str);
                return Err("live output publication rejected before write".to_string());
            }
            Ok(Err(_)) => {
                self.state.lock().await.pending.remove(&id_str);
                return Err("stdout channel closed".to_string());
            }
            Err(_) => {
                self.state.lock().await.pending.remove(&id_str);
                return Err(format!(
                    "callback stdout write timed out after {}s; delivery outcome unknown",
                    PROVIDER_CALLBACK_TIMEOUT.as_secs()
                ));
            }
        }

        let response = match early_response {
            Some(value) => Ok(Ok(value)),
            None => tokio::time::timeout_at(deadline, rx).await,
        };
        match response {
            Ok(Ok(value)) => {
                if let Some(error) = value.get("error") {
                    Err(format!(
                        "callback error: {}",
                        error
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                    ))
                } else {
                    Ok(value.get("result").cloned().unwrap_or(Value::Null))
                }
            }
            Ok(Err(_)) => Err("callback response channel dropped".to_string()),
            Err(_) => {
                self.state.lock().await.pending.remove(&id_str);
                Err(format!(
                    "callback timed out after {}s; execution outcome unknown",
                    PROVIDER_CALLBACK_TIMEOUT.as_secs()
                ))
            }
        }
    }

    /// Route an incoming callback response (has "id" starting with "cb-").
    /// Fail one pending callback whose SDK reply could not be parsed (for
    /// example a `NaN` token), instead of letting it wait out its deadline.
    async fn fail_malformed_callback_response(&self, id: &str) {
        if let Some(tx) = self.state.lock().await.pending.remove(id) {
            let _ = tx.send(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32700,
                    "message": "malformed callback response: the SDK's reply was not valid JSON \
                                (for example a NaN or Infinity number)",
                },
            }));
        }
    }

    async fn route_callback_response(&self, msg: Value) {
        let id = msg
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if let Some(tx) = self.state.lock().await.pending.remove(&id) {
            let _ = tx.send(msg);
        }
    }

    /// Close callback admission and wake every pending caller. The pending
    /// senders are dropped after releasing the lock, resolving their oneshot
    /// receivers immediately without holding callback state across wakeups.
    async fn close(&self) {
        let pending = {
            let mut state = self.state.lock().await;
            state.closed = true;
            std::mem::take(&mut state.pending)
        };
        drop(pending);
    }
}

#[async_trait]
impl meerkat_mobkit::identity_first::gateway_bridges::CallbackBridge for StdioCallbackBridge {
    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        self.call(method, params).await
    }
}

/// One SDK-registered callback tool as it crosses the `callback/build_agent`
/// response wire: a bare name string (legacy shape) or
/// `{name, description?, input_schema?}` so the agent — and the live
/// projection's `live_visible_tool_defs` — see the real argument schema
/// instead of the permissive `{"type": "object"}` placeholder.
struct CallbackToolSpec {
    name: String,
    description: Option<String>,
    input_schema: Option<Value>,
    execution: ToolExecutionContract,
}

/// Typed outcome of the runtime-inbox backlog read behind the
/// `runtime_inbox_backlog` health dimension.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RuntimeInboxBacklog {
    /// The store enumerated its delivery authorities; this is the exact
    /// host-store total of committed-but-undrained deliveries.
    Counted(u64),
    /// The runtime store refused the cross-runtime delivery read with a typed
    /// `RuntimeStoreError::Unsupported`. That is a property of the store, not
    /// a fault of this tick: the dimension is unreadable (never a fabricated
    /// zero), and the condition is announced once rather than on every tick.
    /// A store that can answer but fails to (read error, corrupt authority)
    /// is NOT this variant; that stays an error the caller surfaces each time.
    Unsupported { operation: String },
}

impl RuntimeInboxBacklog {
    /// The rung this dimension contributes to meerkat's census reading.
    /// `Unreadable` for an unsupported store: the operator is told to look,
    /// not told a rung the store never established.
    fn reading(&self) -> meerkat::JobHealthReading {
        match self {
            Self::Counted(0) => meerkat::JobHealthReading::Ok,
            Self::Counted(_) => meerkat::JobHealthReading::Degraded,
            Self::Unsupported { .. } => meerkat::JobHealthReading::Unreadable,
        }
    }

    /// Wire value for `runtime_inbox_backlog`: the count, or `null` when the
    /// store cannot answer (a number here would claim a census that did not
    /// happen).
    fn count_value(&self) -> Value {
        match self {
            Self::Counted(total) => json!(total),
            Self::Unsupported { .. } => Value::Null,
        }
    }

    fn reading_label(&self) -> &'static str {
        match self {
            Self::Counted(_) => "counted",
            Self::Unsupported { .. } => "unsupported",
        }
    }
}

/// Classify the inbox census result. Only the typed `Unsupported` store
/// refusal becomes a non-error outcome; every other failure keeps its text and
/// stays an error so the delivery driver surfaces it on each tick.
fn classify_runtime_inbox_backlog(
    result: Result<u64, meerkat_runtime::RuntimeDeliveryError>,
) -> Result<RuntimeInboxBacklog, String> {
    match result {
        Ok(total) => Ok(RuntimeInboxBacklog::Counted(total)),
        Err(meerkat_runtime::RuntimeDeliveryError::Store(
            meerkat_runtime::store::RuntimeStoreError::Unsupported(operation),
        )) => Ok(RuntimeInboxBacklog::Unsupported { operation }),
        Err(error) => Err(error.to_string()),
    }
}

/// The gateway's one runtime delivery inbox over its runtime store.
///
/// `RuntimeDeliveryInbox`'s commit signal is per instance: clones share it,
/// separately constructed instances over the same store do not. The shell job
/// projector and the detached-job delivery runtime therefore both take clones
/// of this one inbox, so a delivery committed through either path wakes every
/// subscriber of the other. Build it once per gateway.
#[derive(Clone)]
struct GatewayRuntimeDelivery {
    composition: meerkat_mobkit::mob_handle_runtime::MobRuntimeDelivery,
}

impl GatewayRuntimeDelivery {
    fn new(
        runtime_store: Arc<dyn meerkat_runtime::RuntimeStore>,
        jobs: Arc<dyn meerkat::DetachedJobStore>,
    ) -> Self {
        Self {
            composition: meerkat_mobkit::mob_handle_runtime::MobRuntimeDelivery::new(
                runtime_store,
                jobs,
            ),
        }
    }

    /// A clone sharing the commit signal.
    fn inbox(&self) -> meerkat_runtime::RuntimeDeliveryInbox {
        self.composition.inbox()
    }

    /// The default shell job projector over this inbox. Per-session builds
    /// rebind its realm (`bound_to_realm`); the inbox stays this one.
    fn shell_job_projector(
        &self,
        job_store: Arc<dyn meerkat::DetachedJobStore>,
    ) -> meerkat::JobOutboxProjector {
        meerkat::JobOutboxProjector::new_for_realm(
            job_store,
            self.inbox(),
            meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
        )
    }
}

#[derive(Clone)]
struct DetachedCallbackJobRuntime {
    realm_id: String,
    store: Arc<dyn meerkat::DetachedJobStore>,
    service: meerkat::DetachedJobService,
    blob_store: Arc<dyn meerkat_core::BlobStore>,
    runtime_inbox: Option<meerkat_runtime::RuntimeDeliveryInbox>,
    /// Latched on the first typed `Unsupported` answer to the runtime-inbox
    /// backlog read; the WARN naming the operation fires on that transition
    /// only. See [`Self::runtime_inbox_backlog`].
    runtime_inbox_unsupported_announced: Arc<AtomicBool>,
    runtime_delivery: Option<meerkat_mobkit::mob_handle_runtime::MobRuntimeDelivery>,
    delivery_driver_started: Arc<AtomicBool>,
    monitor_recovery_completed: Arc<AtomicBool>,
    monitor_shell_config: Option<meerkat_tools::builtin::shell::ShellConfig>,
    monitor_managers: Arc<Mutex<HashMap<String, Arc<meerkat_tools::builtin::shell::JobManager>>>>,
    callback_runners: Arc<std::sync::RwLock<BTreeSet<(String, String, String)>>>,
}

impl DetachedCallbackJobRuntime {
    fn new(
        realm_id: impl Into<String>,
        store: Arc<dyn meerkat::DetachedJobStore>,
        blob_store: Arc<dyn meerkat_core::BlobStore>,
    ) -> Self {
        Self {
            realm_id: realm_id.into(),
            service: meerkat::DetachedJobService::new(Arc::clone(&store)),
            store,
            blob_store,
            runtime_inbox: None,
            runtime_inbox_unsupported_announced: Arc::new(AtomicBool::new(false)),
            runtime_delivery: None,
            delivery_driver_started: Arc::new(AtomicBool::new(false)),
            monitor_recovery_completed: Arc::new(AtomicBool::new(false)),
            monitor_shell_config: None,
            monitor_managers: Arc::new(Mutex::new(HashMap::new())),
            callback_runners: Arc::new(std::sync::RwLock::new(BTreeSet::new())),
        }
    }

    fn with_runtime_delivery(mut self, delivery: &GatewayRuntimeDelivery) -> Self {
        self.runtime_inbox = Some(delivery.inbox());
        self.runtime_delivery = Some(delivery.composition.clone());
        self
    }

    fn with_monitor_shell(mut self, project_root: PathBuf, enabled: bool) -> Self {
        if enabled {
            self.monitor_shell_config =
                Some(meerkat_tools::builtin::shell::ShellConfig::with_project_root(project_root));
        }
        self
    }

    fn shell_delivery_projector(
        &self,
    ) -> Result<Arc<dyn meerkat_tools::builtin::shell::ShellJobDeliveryProjector>, String> {
        let runtime_inbox = self.runtime_inbox.clone().ok_or_else(|| {
            "durable monitor execution requires the persistent runtime inbox".to_string()
        })?;
        Ok(Arc::new(meerkat::JobOutboxProjector::new_for_realm(
            Arc::clone(&self.store),
            runtime_inbox,
            self.realm_id.clone(),
        )))
    }

    async fn monitor_manager(
        &self,
        session_id: &meerkat_core::SessionId,
    ) -> Result<Arc<meerkat_tools::builtin::shell::JobManager>, String> {
        let key = session_id.to_string();
        let mut managers = self.monitor_managers.lock().await;
        if let Some(manager) = managers.get(&key) {
            return Ok(Arc::clone(manager));
        }
        let config = self.monitor_shell_config.clone().ok_or_else(|| {
            "monitors/start requires shell tooling to be enabled for this MobKit runtime"
                .to_string()
        })?;
        let durable = meerkat_tools::builtin::shell::DurableShellJobRuntime::new(
            self.realm_id.clone(),
            session_id.clone(),
            Arc::clone(&self.store),
            Arc::clone(&self.blob_store),
            self.shell_delivery_projector()?,
        )
        .map_err(|error| error.to_string())?;
        let manager = Arc::new(
            meerkat_tools::builtin::shell::JobManager::new(config)
                .with_durable_job_runtime(durable)
                .bind_canonical_async_ops(
                    session_id.clone(),
                    Arc::new(meerkat_runtime::RuntimeOpsLifecycleRegistry::new()),
                ),
        );
        managers.insert(key, Arc::clone(&manager));
        Ok(manager)
    }

    fn register_callback_catalog(&self, catalog: &[ToolCatalogEntry]) -> bool {
        let mut runners = self
            .callback_runners
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut registered_new_runner = false;
        for entry in catalog {
            if let Some(policy) = entry.execution.detached_policy() {
                registered_new_runner |= runners.insert((
                    entry.tool.name.to_string(),
                    policy.runner().name().to_string(),
                    policy.runner().version().to_string(),
                ));
            }
        }
        registered_new_runner
    }

    fn owns_callback_job(&self, spec: &meerkat::JobSpec) -> bool {
        self.callback_runners
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&(
                spec.tool.name().to_string(),
                spec.runner.name().to_string(),
                spec.runner.version().to_string(),
            ))
    }

    async fn recover_monitor_jobs(&self) -> Result<(), String> {
        if self.monitor_shell_config.is_none()
            || self.monitor_recovery_completed.load(Ordering::Acquire)
        {
            return Ok(());
        }
        let sessions = self
            .store
            .list_all(usize::MAX)
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|job| {
                job.spec.realm_id == self.realm_id
                    && matches!(
                        job.spec.runner.name(),
                        "meerkat.shell" | "meerkat.monitor_script"
                    )
            })
            .map(|job| {
                let session_id = job.spec.origin_session_id;
                (session_id.to_string(), session_id)
            })
            .collect::<BTreeMap<_, _>>();
        for session_id in sessions.into_values() {
            self.monitor_manager(&session_id)
                .await?
                .list_jobs()
                .await
                .map_err(|error| error.to_string())?;
        }
        self.monitor_recovery_completed
            .store(true, Ordering::Release);
        Ok(())
    }

    fn arm_delivery_driver(&self, unified_runtime: Arc<UnifiedRuntime>) {
        if self
            .delivery_driver_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let runtime = self.clone();
        let unified_runtime = Arc::downgrade(&unified_runtime);
        tokio::spawn(async move {
            loop {
                let Some(host) = unified_runtime.upgrade() else {
                    break;
                };
                if let Err(error) = runtime.recover_monitor_jobs().await {
                    tracing::warn!(%error, "durable monitor recovery failed");
                }
                match runtime.health_projection().await {
                    Ok(projection) => host.set_job_health_projection(Some(projection)),
                    Err(error) => {
                        tracing::warn!(%error, "durable callback health projection failed");
                        host.set_job_health_projection(Some(json!({
                            "status": "degraded",
                            "detached_jobs": {
                                "status": "degraded",
                                "reason": "job_health_projection_failed"
                            }
                        })));
                    }
                }
                drop(host);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    /// The exact host-store total behind `JobHealthSummary.runtime_inbox_backlog`.
    ///
    /// Deliberately NOT derived by walking job rows for origin sessions. The
    /// durable runtime store is one file per realm ROOT and serves every
    /// session this host built - including members built under `mob.<mob_id>`,
    /// and runtime ids carry no realm - so a job-derived sum silently misses
    /// any runtime whose job aged out or sits outside the logical realm, and
    /// publishes a false zero exactly when a backlog is what the operator is
    /// asking about. Over-inclusion is the safe direction here: every counted
    /// row is a real undrained delivery in this host.
    ///
    /// A runtime store that refuses the cross-runtime delivery read with a
    /// typed `Unsupported` yields [`RuntimeInboxBacklog::Unsupported`] rather
    /// than an error, announced at WARN once per runtime (then DEBUG): the
    /// 1 Hz delivery driver calls this on every tick, and through 0.8.32 the
    /// gateway's own runtime-store facade did not forward the verb, so every
    /// tick logged `durable callback health projection failed ...
    /// list_runtime_delivery_authorities` forever. Every other failure is a
    /// real fault of a store that CAN answer and stays an error the caller
    /// surfaces each time.
    async fn runtime_inbox_backlog(&self) -> Result<RuntimeInboxBacklog, String> {
        let Some(runtime_inbox) = self.runtime_inbox.clone() else {
            return Ok(RuntimeInboxBacklog::Counted(0));
        };
        let backlog = classify_runtime_inbox_backlog(runtime_inbox.pending_delivery_total().await)?;
        if let RuntimeInboxBacklog::Unsupported { operation } = &backlog {
            if self
                .runtime_inbox_unsupported_announced
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                tracing::warn!(
                    %operation,
                    "runtime store does not support the runtime inbox backlog read; health \
                     projects runtime_inbox_backlog as null with an unreadable reading until \
                     the store supports it (reported once)"
                );
            } else {
                tracing::debug!(
                    %operation,
                    "runtime inbox backlog read still unsupported on this runtime store"
                );
            }
        }
        Ok(backlog)
    }

    /// The count for the `jobs/health` wire contract, whose
    /// `runtime_inbox_backlog` is a plain `u64` with no "unreadable" shape. An
    /// unsupported store is a typed error here rather than a fabricated zero.
    async fn runtime_inbox_backlog_count(&self) -> Result<u64, String> {
        match self.runtime_inbox_backlog().await? {
            RuntimeInboxBacklog::Counted(total) => Ok(total),
            RuntimeInboxBacklog::Unsupported { operation } => Err(format!(
                "runtime store does not support {operation}; jobs/health cannot publish \
                 runtime_inbox_backlog"
            )),
        }
    }

    async fn health_projection(&self) -> Result<Value, String> {
        let now_ms = callback_unix_time_ms()?;
        let health = self
            .service
            .health_snapshot_for_realm(&self.realm_id, now_ms, usize::MAX)
            .await
            .map_err(|error| error.to_string())?;
        // NOT summed with `health.pending_outbox_jobs`. The two name different
        // wedges: a job whose delivery was never handed to a runtime, versus a
        // delivery a runtime accepted and never drained. 0.8.23 published their
        // sum under one name, which is a number that means neither.
        let runtime_inbox_backlog = self.runtime_inbox_backlog().await?;
        // Fold mobkit's own runtime dimension into meerkat's census rung rather
        // than collapsing to a boolean. `Unreadable` outranks `Degraded`: a
        // census that could not be read may be hiding the fault, so the
        // operator is told to look rather than told a rung. An unsupported
        // store lands on that rung too: it never established a count.
        let reading = health.reading().worst(runtime_inbox_backlog.reading());
        let delivery_pass = self
            .runtime_delivery
            .as_ref()
            .and_then(|delivery| delivery.last_pass());
        let owner_running = self
            .runtime_delivery
            .as_ref()
            .is_some_and(|delivery| delivery.is_running());
        let reading = match delivery_pass.as_ref() {
            Some(pass) if owner_running => {
                if !pass.blocked_sessions.is_empty()
                    || !pass.blocked_runtimes.is_empty()
                    || !pass.failures.is_empty()
                    || pass.cross_process_wake_unavailable.is_some()
                {
                    reading.worst(meerkat::JobHealthReading::Degraded)
                } else {
                    reading
                }
            }
            _ => reading.worst(meerkat::JobHealthReading::Unreadable),
        };
        let delivery_pass = delivery_pass.map(|pass| {
            json!({
                "generation": pass.generation,
                "projected": pass.projected,
                "applied": pass.applied,
                "refused": pass.refused,
                "blocked_sessions": pass.blocked_sessions,
                "blocked_runtimes": pass.blocked_runtimes,
                "failures": pass.failures,
                "awaiting_other_hosts": pass.awaiting_other_hosts,
                "applies_cold_deliveries": pass.applies_cold_deliveries,
                "cross_process_wake_unavailable": pass.cross_process_wake_unavailable,
            })
        });
        let status = match reading {
            meerkat::JobHealthReading::Ok => "ok",
            meerkat::JobHealthReading::Degraded => "degraded",
            meerkat::JobHealthReading::Unreadable => "unreadable",
        };
        let mut by_session = serde_json::Map::new();
        for job in self
            .store
            .list_all(usize::MAX)
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|job| job.spec.realm_id == self.realm_id)
        {
            let key = job.spec.origin_session_id.to_string();
            let entry = by_session.entry(key).or_insert_with(|| {
                json!({
                    "active": 0_u64,
                    "awaiting_detached": false,
                    "queued": 0_u64,
                    "running": 0_u64,
                    "needs_attention": 0_u64
                })
            });
            let active = matches!(
                job.machine_state.lifecycle_phase,
                meerkat::JobPhase::Unsubmitted
                    | meerkat::JobPhase::Queued
                    | meerkat::JobPhase::Claimed
                    | meerkat::JobPhase::Running
                    | meerkat::JobPhase::WaitingExternal
                    | meerkat::JobPhase::LossObserved
                    | meerkat::JobPhase::RetryScheduled
            );
            if active {
                entry["active"] = json!(
                    entry["active"]
                        .as_u64()
                        .unwrap_or_default()
                        .saturating_add(1)
                );
                entry["awaiting_detached"] = Value::Bool(true);
            }
            let field = match job.machine_state.lifecycle_phase {
                meerkat::JobPhase::Queued | meerkat::JobPhase::RetryScheduled => Some("queued"),
                meerkat::JobPhase::Running | meerkat::JobPhase::WaitingExternal => Some("running"),
                meerkat::JobPhase::NeedsAttention => Some("needs_attention"),
                _ => None,
            };
            if let Some(field) = field {
                entry[field] = json!(entry[field].as_u64().unwrap_or_default().saturating_add(1));
            }
        }
        Ok(json!({
            "status": status,
            "monitors_available": self.monitor_shell_config.is_some(),
            "detached_jobs": {
                "status": status,
                "queued": health.queued,
                "running": health.running,
                "awaiting_members": health.awaiting_members,
                "stale_leases": health.stale_leases,
                "needs_attention": health.needs_attention,
                "pending_outbox_jobs": health.pending_outbox_jobs,
                "runtime_inbox_backlog": runtime_inbox_backlog.count_value(),
                "runtime_inbox_backlog_reading": runtime_inbox_backlog.reading_label(),
                "blocked_deliveries": null,
                "blocked_deliveries_reading": "unavailable",
                "runtime_delivery_owner_running": owner_running,
                "runtime_delivery_pass": delivery_pass
            },
            "by_session": by_session
        }))
    }
}

impl CallbackToolSpec {
    fn parse(value: &Value) -> Result<Self, String> {
        if let Some(name) = value.as_str() {
            return Ok(Self {
                name: name.to_string(),
                description: None,
                input_schema: None,
                execution: ToolExecutionContract::default(),
            });
        }
        let Some(object) = value.as_object() else {
            return Err(format!(
                "tools entries must be strings or {{name, description?, input_schema?}} \
                 objects, got: {value}"
            ));
        };
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| format!("tool object requires a non-empty string name, got: {value}"))?
            .to_string();
        let description = match object.get("description") {
            None | Some(Value::Null) => None,
            Some(Value::String(text)) => Some(text.clone()),
            Some(other) => {
                return Err(format!(
                    "tool '{name}' description must be a string, got: {other}"
                ));
            }
        };
        let input_schema = match object.get("input_schema") {
            None | Some(Value::Null) => None,
            Some(schema @ Value::Object(_)) => Some(schema.clone()),
            Some(other) => {
                return Err(format!(
                    "tool '{name}' input_schema must be a JSON object, got: {other}"
                ));
            }
        };
        let execution = object
            .get("execution")
            .cloned()
            .map(serde_json::from_value::<meerkat_contracts::CallbackToolExecution>)
            .transpose()
            .map_err(|error| format!("tool '{name}' execution is invalid: {error}"))?
            .map_or_else(
                || Ok(ToolExecutionContract::default()),
                callback_execution_contract,
            )?;
        Ok(Self {
            name,
            description,
            input_schema,
            execution,
        })
    }
}

fn callback_execution_contract(
    execution: meerkat_contracts::CallbackToolExecution,
) -> Result<ToolExecutionContract, String> {
    match execution {
        meerkat_contracts::CallbackToolExecution::Fast => Ok(ToolExecutionContract::default()),
        meerkat_contracts::CallbackToolExecution::Detached {
            runner,
            restart_class,
            idempotency_scope,
            submission_timeout_ms,
            credential_scopes,
        } => {
            let runner = meerkat_core::RunnerIdentity::new(runner.name, runner.version)
                .map_err(|error| error.to_string())?;
            let restart_class = match restart_class {
                meerkat_contracts::JobRestartClass::Adoptable => {
                    meerkat_core::RestartClass::Adoptable
                }
                meerkat_contracts::JobRestartClass::CheckpointResumable => {
                    meerkat_core::RestartClass::CheckpointResumable
                }
                meerkat_contracts::JobRestartClass::Replayable => {
                    meerkat_core::RestartClass::Replayable
                }
                meerkat_contracts::JobRestartClass::NonResumable => {
                    meerkat_core::RestartClass::NonResumable
                }
            };
            let idempotency_scope = match idempotency_scope {
                meerkat_contracts::JobIdempotencyScope::ToolCall => {
                    meerkat_core::IdempotencyScope::ToolCall
                }
                meerkat_contracts::JobIdempotencyScope::InteractionAndArguments => {
                    meerkat_core::IdempotencyScope::InteractionAndArguments
                }
                meerkat_contracts::JobIdempotencyScope::HostSemanticKey => {
                    meerkat_core::IdempotencyScope::HostSemanticKey
                }
            };
            let policy = meerkat_core::DetachedToolExecutionPolicy::new(
                runner,
                restart_class,
                idempotency_scope,
                Duration::from_millis(submission_timeout_ms),
            )
            .map_err(|error| error.to_string())?
            .with_credential_scopes(credential_scopes);
            ToolExecutionContract::new(
                std::collections::BTreeSet::from([ToolExecutionMode::Detached]),
                ToolExecutionMode::Detached,
                None,
                Some(policy),
            )
            .map_err(|error| error.to_string())
        }
    }
}

/// Tool dispatcher that routes tool calls to Python via the callback bridge.
///
/// Created from the tool specs the SDK returned from `callback/build_agent`
/// (`add_tools()` names or `register_tool()` name + description + schema).
/// When the agent calls a tool, `dispatch()` sends `callback/call_tool` to
/// Python and returns the result.
#[derive(Clone)]
struct CallbackToolDispatcher {
    bridge: StdioCallbackBridge,
    scope_id: String,
    tool_defs: Arc<[Arc<ToolDef>]>,
    tool_catalog: Arc<[ToolCatalogEntry]>,
    detached_jobs: Option<DetachedCallbackJobRuntime>,
    reconcile_registered_catalog: bool,
}

impl CallbackToolDispatcher {
    fn new(
        bridge: StdioCallbackBridge,
        scope_id: String,
        tools: Vec<CallbackToolSpec>,
        detached_jobs: Option<DetachedCallbackJobRuntime>,
    ) -> Self {
        let provenance_source_id = format!("mobkit-callback:{scope_id}");
        let entries: Vec<(Arc<ToolDef>, ToolExecutionContract)> = tools
            .into_iter()
            .map(|tool| {
                (
                    Arc::new(ToolDef {
                        audience: Default::default(),
                        name: tool.name.into(),
                        description: tool
                            .description
                            .unwrap_or_else(|| "Python callback tool".to_string()),
                        input_schema: tool
                            .input_schema
                            .unwrap_or_else(|| json!({"type": "object"})),
                        provenance: Some(ToolProvenance {
                            kind: ToolSourceKind::Callback,
                            source_id: provenance_source_id.as_str().into(),
                        }),
                    }),
                    tool.execution,
                )
            })
            .collect();
        let tool_defs = entries
            .iter()
            .map(|(tool, _)| Arc::clone(tool))
            .collect::<Vec<_>>();
        let tool_catalog = entries
            .into_iter()
            .map(|(tool, execution)| {
                ToolCatalogEntry::session_inline(tool, true).with_execution_contract(execution)
            })
            .collect::<Vec<_>>();
        let reconcile_registered_catalog = detached_jobs
            .as_ref()
            .is_some_and(|runtime| runtime.register_callback_catalog(&tool_catalog));
        Self {
            bridge,
            scope_id,
            tool_defs: tool_defs.into(),
            tool_catalog: tool_catalog.into(),
            detached_jobs,
            reconcile_registered_catalog,
        }
    }

    async fn submit_detached(
        &self,
        call: ToolCallView<'_>,
        context: &meerkat_core::ToolDispatchContext,
        plan: &meerkat_core::ResolvedToolExecutionPlan,
    ) -> Result<ToolDispatchOutcome, ToolError> {
        let runtime = self.detached_jobs.clone().ok_or_else(|| {
            ToolError::unavailable(
                call.name,
                meerkat_core::ToolUnavailableReason::ExecutionModeOwnerUnavailable,
            )
        })?;
        let origin_session_id = context.origin_session_id().cloned().ok_or_else(|| {
            ToolError::execution_failed(
                "detached callback dispatch requires runtime-owned session identity".to_string(),
            )
        })?;
        let interaction_lineage = context.interaction_lineage_id().ok_or_else(|| {
            ToolError::execution_failed(
                "detached callback dispatch requires runtime-owned interaction lineage".to_string(),
            )
        })?;
        let arguments_sha256 = plan.canonical_arguments_sha256().ok_or_else(|| {
            ToolError::execution_failed(
                "detached callback dispatch requires a root-fenced canonical argument digest"
                    .to_string(),
            )
        })?;
        let policy = match plan.kind() {
            meerkat_core::ResolvedExecutionKind::Detached(policy) => policy,
            _ => {
                return Err(ToolError::execution_failed(
                    "detached callback owner received a non-detached execution plan".to_string(),
                ));
            }
        };
        let arguments: Value = serde_json::from_str(call.args.get())
            .map_err(|error| ToolError::invalid_arguments(call.name, error.to_string()))?;
        let arguments_hash = callback_sha256(arguments_sha256);
        let lineage = interaction_lineage.to_string();
        let submission_key = callback_submission_key(
            &runtime.realm_id,
            &origin_session_id,
            &lineage,
            call,
            policy,
            &arguments_hash,
            &arguments,
        )?;
        let specification = runtime
            .blob_store
            .put_artifact(
                "application/vnd.meerkat.callback-arguments+json",
                call.args.get(),
            )
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to persist detached callback specification: {error}"
                ))
            })?;
        let spec = meerkat::JobSpec::new(
            runtime.realm_id.clone(),
            origin_session_id,
            meerkat::ExecutionIntentId::from_string(format!(
                "intent:{lineage}:{}:{arguments_hash}",
                call.name
            ))
            .map_err(|error| ToolError::execution_failed(error.to_string()))?,
            meerkat::InteractionLineageId::from_string(lineage)
                .map_err(|error| ToolError::execution_failed(error.to_string()))?,
            meerkat::ToolIdentity::new(call.name, policy.runner().version())
                .map_err(|error| ToolError::execution_failed(error.to_string()))?,
            meerkat::RunnerIdentity::new(policy.runner().name(), policy.runner().version())
                .map_err(|error| ToolError::execution_failed(error.to_string()))?,
            callback_job_restart_class(policy.restart_class()),
            meerkat::CanonicalArgumentsHash::new(arguments_hash)
                .map_err(|error| ToolError::execution_failed(error.to_string()))?,
            meerkat::JobSubmissionKey::new(submission_key)
                .map_err(|error| ToolError::execution_failed(error.to_string()))?,
        )
        .with_runner_specification_ref(
            meerkat::RunnerSpecificationRef::new(specification.blob_id.to_string())
                .map_err(|error| ToolError::execution_failed(error.to_string()))?,
        )
        .with_credential_context_refs(match plan.credential_context_refs() {
            meerkat_core::ToolExecutionApplicability::Applicable(references) => references.clone(),
            meerkat_core::ToolExecutionApplicability::NotApplicable => Vec::new(),
        });
        let receipt = runtime
            .service
            .submit(spec)
            .await
            .map_err(|error| ToolError::execution_failed(error.to_string()))?;
        let projected = meerkat::project_job_receipt(receipt.clone());
        let bridge = self.bridge.clone();
        tokio::spawn(async move {
            if let Err(error) =
                start_detached_callback_attempt(runtime, bridge, receipt.job_id).await
            {
                tracing::warn!(%error, "detached callback attempt start did not complete");
            }
        });
        let content = serde_json::to_string(&projected).map_err(|error| {
            ToolError::execution_failed(format!("failed to encode detached job receipt: {error}"))
        })?;
        Ok(ToolResult::new(call.id.to_string(), content, false).into())
    }

    fn owns_detached_callback_spec(&self, spec: &meerkat::JobSpec) -> bool {
        self.tool_catalog.iter().any(|entry| {
            entry.tool.name == spec.tool.name()
                && entry.execution.detached_policy().is_some_and(|policy| {
                    policy.runner().name() == spec.runner.name()
                        && policy.runner().version() == spec.runner.version()
                })
        })
    }

    /// Rehydrate exact committed authority. Offering an attempt to the host
    /// never claims, retries, or advances its fence.
    async fn reconcile_detached_jobs(&self) -> Result<(), String> {
        let Some(runtime) = self.detached_jobs.clone() else {
            return Ok(());
        };
        let jobs = runtime.store.list_all(usize::MAX).await.map_err(|error| {
            format!("failed to enumerate detached callbacks for reconciliation: {error}")
        })?;
        let mut attempts = Vec::new();
        for job in jobs.into_iter().filter(|job| {
            job.spec.realm_id == runtime.realm_id && self.owns_detached_callback_spec(&job.spec)
        }) {
            if matches!(
                job.machine_state.lifecycle_phase,
                meerkat::JobPhase::Running | meerkat::JobPhase::WaitingExternal
            ) {
                let attempt_id =
                    job.machine_state
                        .current_attempt_id
                        .as_deref()
                        .ok_or_else(|| {
                            format!(
                                "active detached callback {} has no committed attempt id",
                                job.job_id
                            )
                        })?;
                let runner_handle =
                    job.machine_state.runner_handle.as_deref().ok_or_else(|| {
                        format!(
                            "active detached callback {} has no committed runner handle",
                            job.job_id
                        )
                    })?;
                let lease_expires_at_ms =
                    job.machine_state.lease_expires_at_ms.ok_or_else(|| {
                        format!(
                            "active detached callback {} has no committed lease",
                            job.job_id
                        )
                    })?;
                attempts.push(meerkat_contracts::CallbackJobReconcileAttempt {
                    authority: meerkat_contracts::JobAttemptAuthority {
                        job_id: job.job_id.to_string(),
                        attempt_id: attempt_id.to_string(),
                        fence: job.machine_state.current_fence,
                    },
                    runner: meerkat_contracts::JobRunner {
                        name: job.spec.runner.name().to_string(),
                        version: job.spec.runner.version().to_string(),
                    },
                    restart_class: callback_wire_restart_class(job.spec.restart_class),
                    runner_handle: runner_handle.to_string(),
                    checkpoint_ref: job
                        .machine_state
                        .checkpoint_ref
                        .as_ref()
                        .map(ToString::to_string),
                    lease_expires_at_ms,
                });
            }
        }
        if !attempts.is_empty() {
            let offered_attempts = attempts.clone();
            let offered = attempts
                .iter()
                .map(|attempt| attempt.authority.clone())
                .collect::<Vec<_>>();
            let result: meerkat_contracts::CallbackJobReconcileResult = serde_json::from_value(
                self.bridge
                    .call(
                        "callback/job/reconcile",
                        serde_json::to_value(meerkat_contracts::CallbackJobReconcileParams {
                            attempts,
                        })
                        .map_err(|error| error.to_string())?,
                    )
                    .await?,
            )
            .map_err(|error| error.to_string())?;
            if result
                .live_attempts
                .iter()
                .any(|authority| !offered.contains(authority))
            {
                return Err(
                    "callback/job/reconcile returned authority that was not offered".to_string(),
                );
            }
            let now_ms = callback_unix_time_ms()?;
            for attempt in offered_attempts.iter().filter(|attempt| {
                attempt.lease_expires_at_ms <= now_ms
                    && !result.live_attempts.contains(&attempt.authority)
            }) {
                let job_id =
                    meerkat::JobId::new(&attempt.authority.job_id).map_err(|e| e.to_string())?;
                let write = meerkat::AttemptWriteAuthority {
                    attempt_id: meerkat::AttemptId::new(&attempt.authority.attempt_id)
                        .map_err(|e| e.to_string())?,
                    fence: meerkat::FenceToken::new(attempt.authority.fence),
                };
                match runtime
                    .service
                    .observe_lease_expired(&job_id, write, now_ms)
                    .await
                {
                    Ok(_) => {}
                    Err(
                        meerkat::DetachedJobError::StaleRevision { .. }
                        | meerkat::DetachedJobError::InvalidTransition { .. },
                    ) => continue,
                    Err(error) => return Err(error.to_string()),
                }
                match attempt.restart_class {
                    meerkat_contracts::JobRestartClass::NonResumable => {
                        runtime
                            .service
                            .classify_worker_loss(&job_id, now_ms)
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    meerkat_contracts::JobRestartClass::CheckpointResumable
                        if attempt.checkpoint_ref.is_none() =>
                    {
                        runtime
                            .service
                            .mark_needs_attention(
                                &job_id,
                                now_ms,
                                meerkat::JobFailureCode::new(
                                    "checkpoint_resume_missing_checkpoint",
                                )
                                .map_err(|error| error.to_string())?,
                            )
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    meerkat_contracts::JobRestartClass::Adoptable
                    | meerkat_contracts::JobRestartClass::CheckpointResumable
                    | meerkat_contracts::JobRestartClass::Replayable => {
                        runtime
                            .service
                            .schedule_retry(&job_id, now_ms)
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                }
            }
            for attempt in offered_attempts.iter().filter(|attempt| {
                attempt.lease_expires_at_ms > now_ms
                    || result.live_attempts.contains(&attempt.authority)
            }) {
                let runtime = runtime.clone();
                let bridge = self.bridge.clone();
                let authority = attempt.authority.clone();
                spawn_callback_lease_tracker(runtime, bridge, authority);
            }
        }
        self.start_due_detached_jobs().await
    }

    async fn start_due_detached_jobs(&self) -> Result<(), String> {
        let Some(runtime) = self.detached_jobs.clone() else {
            return Ok(());
        };
        let now_ms = callback_unix_time_ms()?;
        let jobs = runtime.store.list_all(usize::MAX).await.map_err(|error| {
            format!("failed to enumerate detached callbacks for runnable work: {error}")
        })?;
        for job in jobs.into_iter().filter(|job| {
            job.spec.realm_id == runtime.realm_id
                && self.owns_detached_callback_spec(&job.spec)
                && (job.machine_state.lifecycle_phase == meerkat::JobPhase::Queued
                    || job.machine_state.lifecycle_phase == meerkat::JobPhase::RetryScheduled)
        }) {
            let runtime = runtime.clone();
            let bridge = self.bridge.clone();
            let delay_ms = job
                .machine_state
                .retry_due_at_ms
                .unwrap_or(now_ms)
                .saturating_sub(now_ms);
            tokio::spawn(async move {
                if delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                if let Err(error) =
                    start_detached_callback_attempt(runtime, bridge, job.job_id).await
                {
                    tracing::warn!(%error, "runnable detached callback start did not complete");
                }
            });
        }
        Ok(())
    }
}

fn spawn_callback_lease_tracker(
    runtime: DetachedCallbackJobRuntime,
    bridge: StdioCallbackBridge,
    authority: meerkat_contracts::JobAttemptAuthority,
) {
    tokio::spawn(async move {
        if let Err(error) = reconcile_missing_callback_after_lease(runtime, bridge, authority).await
        {
            tracing::warn!(%error, "detached callback lease tracking failed");
        }
    });
}

async fn reconcile_missing_callback_after_lease(
    runtime: DetachedCallbackJobRuntime,
    bridge: StdioCallbackBridge,
    authority: meerkat_contracts::JobAttemptAuthority,
) -> Result<(), String> {
    let job_id = meerkat::JobId::new(&authority.job_id).map_err(|error| error.to_string())?;
    loop {
        let stored = runtime
            .store
            .get(&job_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("detached callback {job_id} disappeared before lease expiry"))?;
        let exact_attempt = stored.machine_state.current_attempt_id.as_deref()
            == Some(authority.attempt_id.as_str())
            && stored.machine_state.current_fence == authority.fence
            && matches!(
                stored.machine_state.lifecycle_phase,
                meerkat::JobPhase::Running | meerkat::JobPhase::WaitingExternal
            );
        if !exact_attempt {
            return Ok(());
        }
        let lease_expires_at_ms = stored
            .machine_state
            .lease_expires_at_ms
            .ok_or_else(|| format!("active detached callback {job_id} has no committed lease"))?;
        let now_ms = callback_unix_time_ms()?;
        if lease_expires_at_ms > now_ms {
            tokio::time::sleep(Duration::from_millis(
                lease_expires_at_ms.saturating_sub(now_ms).saturating_add(1),
            ))
            .await;
            // Heartbeats may have extended the exact committed lease and
            // checkpoint while this task slept. Rehydrate again before
            // offering recovery so reopen never regresses durable authority.
            continue;
        }

        let offered = meerkat_contracts::CallbackJobReconcileAttempt {
            authority: authority.clone(),
            runner: meerkat_contracts::JobRunner {
                name: stored.spec.runner.name().to_string(),
                version: stored.spec.runner.version().to_string(),
            },
            restart_class: callback_wire_restart_class(stored.spec.restart_class),
            runner_handle: stored
                .machine_state
                .runner_handle
                .as_ref()
                .ok_or_else(|| {
                    format!("active detached callback {job_id} has no committed runner handle")
                })?
                .clone(),
            checkpoint_ref: stored
                .machine_state
                .checkpoint_ref
                .as_ref()
                .map(ToString::to_string),
            lease_expires_at_ms,
        };
        let _live_attempts = match bridge
            .call(
                "callback/job/reconcile",
                serde_json::to_value(meerkat_contracts::CallbackJobReconcileParams {
                    attempts: vec![offered],
                })
                .map_err(|error| error.to_string())?,
            )
            .await
        {
            Ok(value) => {
                let result: meerkat_contracts::CallbackJobReconcileResult =
                    serde_json::from_value(value).map_err(|error| error.to_string())?;
                if result
                    .live_attempts
                    .iter()
                    .any(|candidate| candidate != &authority)
                {
                    return Err(
                        "callback/job/reconcile returned authority that was not offered"
                            .to_string(),
                    );
                }
                result.live_attempts
            }
            // Once the committed lease has elapsed, an unavailable host is a
            // missing worker observation. The generated machine still decides
            // whether this becomes loss, retry, or needs-attention.
            Err(error) => {
                tracing::warn!(%error, %job_id, "host unavailable at detached callback lease boundary");
                Vec::new()
            }
        };
        let current = runtime
            .store
            .get(&job_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("detached callback {job_id} disappeared during reconcile"))?;
        let still_exact = current.machine_state.current_attempt_id.as_deref()
            == Some(authority.attempt_id.as_str())
            && current.machine_state.current_fence == authority.fence
            && matches!(
                current.machine_state.lifecycle_phase,
                meerkat::JobPhase::Running | meerkat::JobPhase::WaitingExternal
            );
        if !still_exact {
            return Ok(());
        }
        let current_lease = current
            .machine_state
            .lease_expires_at_ms
            .ok_or_else(|| format!("active detached callback {job_id} has no committed lease"))?;
        let now_ms = callback_unix_time_ms()?;
        if current_lease > now_ms {
            continue;
        }
        let write = meerkat::AttemptWriteAuthority {
            attempt_id: meerkat::AttemptId::new(&authority.attempt_id)
                .map_err(|error| error.to_string())?,
            fence: meerkat::FenceToken::new(authority.fence),
        };
        match runtime
            .service
            .observe_lease_expired(&job_id, write, now_ms)
            .await
        {
            Ok(_) => {}
            Err(
                meerkat::DetachedJobError::StaleRevision { .. }
                | meerkat::DetachedJobError::InvalidTransition { .. },
            ) => return Ok(()),
            Err(error) => return Err(error.to_string()),
        }
        let retry = match current.spec.restart_class {
            meerkat::RestartClass::NonResumable => {
                runtime
                    .service
                    .classify_worker_loss(&job_id, now_ms)
                    .await
                    .map_err(|error| error.to_string())?;
                false
            }
            meerkat::RestartClass::CheckpointResumable
                if current.machine_state.checkpoint_ref.is_none() =>
            {
                runtime
                    .service
                    .mark_needs_attention(
                        &job_id,
                        now_ms,
                        meerkat::JobFailureCode::new("checkpoint_resume_missing_checkpoint")
                            .map_err(|error| error.to_string())?,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                false
            }
            meerkat::RestartClass::Adoptable
            | meerkat::RestartClass::CheckpointResumable
            | meerkat::RestartClass::Replayable => {
                runtime
                    .service
                    .schedule_retry(&job_id, now_ms)
                    .await
                    .map_err(|error| error.to_string())?;
                true
            }
        };
        if retry {
            start_detached_callback_attempt(runtime, bridge, job_id).await?;
        }
        return Ok(());
    }
}

#[async_trait]
impl AgentToolDispatcher for CallbackToolDispatcher {
    fn tools(&self) -> Arc<[Arc<ToolDef>]> {
        Arc::clone(&self.tool_defs)
    }

    fn tool_catalog_capabilities(&self) -> ToolCatalogCapabilities {
        ToolCatalogCapabilities {
            exact_catalog: true,
            may_require_catalog_control_plane: false,
        }
    }

    fn tool_catalog(&self) -> Arc<[ToolCatalogEntry]> {
        Arc::clone(&self.tool_catalog)
    }

    fn resolve_execution_plan(
        &self,
        call: ToolCallView<'_>,
        _dispatch_context: &meerkat_core::ToolDispatchContext,
        resolution_context: &meerkat_core::ToolExecutionResolutionContext,
    ) -> Result<meerkat_core::ResolvedToolExecutionPlan, meerkat_core::ToolExecutionResolutionError>
    {
        let entry = self
            .tool_catalog
            .iter()
            .find(|entry| entry.tool.name == call.name)
            .ok_or_else(|| meerkat_core::ToolExecutionResolutionError::NotFound {
                tool_name: call.name.to_string(),
            })?;
        let resolution_context =
            resolution_context.with_deadline(ToolDeadlineContributor::finite(
                ToolDeadlineOwner::ToolInternal,
                Duration::from_mins(2),
            ))?;
        entry
            .execution
            .resolve_default(resolution_context.deadlines().clone())
            .map_err(Into::into)
    }

    async fn dispatch(&self, call: ToolCallView<'_>) -> Result<ToolDispatchOutcome, ToolError> {
        let args: Value =
            serde_json::from_str(call.args.get()).map_err(|e| ToolError::InvalidArguments {
                name: call.name.to_string(),
                reason: e.to_string(),
            })?;
        let params = json!({
            "scope_id": self.scope_id,
            "tool": call.name,
            "arguments": args,
        });
        match self.bridge.call("callback/call_tool", params).await {
            Ok(result) => Ok(ToolResult {
                host_metadata: Default::default(),
                tool_use_id: call.id.to_string(),
                content:
                    meerkat_mobkit::identity_first::gateway_bridges::callback_result_to_content(
                        &result,
                    ),
                is_error: false,
                settlement_failures: Vec::new(),
            }
            .into()),
            Err(err) => Ok(ToolResult {
                host_metadata: Default::default(),
                tool_use_id: call.id.to_string(),
                content: vec![ContentBlock::Text {
                    text: format!("Tool execution failed: {err}"),
                }],
                is_error: true,
                settlement_failures: Vec::new(),
            }
            .into()),
        }
    }

    async fn dispatch_resolved_with_context(
        &self,
        call: ToolCallView<'_>,
        context: &meerkat_core::ToolDispatchContext,
        plan: &meerkat_core::ResolvedToolExecutionPlan,
    ) -> Result<ToolDispatchOutcome, ToolError> {
        match plan.mode() {
            ToolExecutionMode::Fast => self.dispatch(call).await,
            ToolExecutionMode::Detached => self.submit_detached(call, context, plan).await,
            ToolExecutionMode::Streaming => Err(ToolError::unavailable(
                call.name,
                meerkat_core::ToolUnavailableReason::ExecutionModeOwnerUnavailable,
            )),
        }
    }
}

fn callback_sha256(digest: [u8; 32]) -> String {
    let mut encoded = String::with_capacity("sha256:".len() + digest.len() * 2);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn callback_submission_key(
    realm_id: &str,
    origin_session_id: &meerkat_core::SessionId,
    interaction_lineage: &str,
    call: ToolCallView<'_>,
    policy: &meerkat_core::DetachedToolExecutionPolicy,
    arguments_hash: &str,
    arguments: &Value,
) -> Result<String, ToolError> {
    let scope = match policy.idempotency_scope() {
        meerkat_core::IdempotencyScope::ToolCall => format!("tool-call:{}", call.id),
        meerkat_core::IdempotencyScope::InteractionAndArguments => {
            format!("interaction:{interaction_lineage}:arguments:{arguments_hash}")
        }
        meerkat_core::IdempotencyScope::HostSemanticKey => {
            let semantic_key = arguments
                .get("idempotency_key")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    ToolError::invalid_arguments(
                        call.name,
                        "host_semantic_key execution requires a non-empty string idempotency_key",
                    )
                })?;
            format!(
                "host-semantic:sha256:{:x}",
                Sha256::digest(semantic_key.as_bytes())
            )
        }
    };
    Ok(format!(
        "callback:{realm_id}:{origin_session_id}:{}:{}:{}:{scope}",
        call.name,
        policy.runner().name(),
        policy.runner().version(),
    ))
}

fn callback_job_restart_class(class: meerkat_core::RestartClass) -> meerkat::RestartClass {
    match class {
        meerkat_core::RestartClass::Adoptable => meerkat::RestartClass::Adoptable,
        meerkat_core::RestartClass::CheckpointResumable => {
            meerkat::RestartClass::CheckpointResumable
        }
        meerkat_core::RestartClass::Replayable => meerkat::RestartClass::Replayable,
        meerkat_core::RestartClass::NonResumable => meerkat::RestartClass::NonResumable,
    }
}

fn callback_wire_restart_class(class: meerkat::RestartClass) -> meerkat_contracts::JobRestartClass {
    match class {
        meerkat::RestartClass::Adoptable => meerkat_contracts::JobRestartClass::Adoptable,
        meerkat::RestartClass::CheckpointResumable => {
            meerkat_contracts::JobRestartClass::CheckpointResumable
        }
        meerkat::RestartClass::Replayable => meerkat_contracts::JobRestartClass::Replayable,
        meerkat::RestartClass::NonResumable => meerkat_contracts::JobRestartClass::NonResumable,
    }
}

async fn start_detached_callback_attempt(
    runtime: DetachedCallbackJobRuntime,
    bridge: StdioCallbackBridge,
    job_id: meerkat::JobId,
) -> Result<(), String> {
    let stored = runtime
        .store
        .get(&job_id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("detached callback job {job_id} disappeared after submission"))?;
    let claimed_at_ms = callback_unix_time_ms()?;
    let runnable = stored.machine_state.lifecycle_phase == meerkat::JobPhase::Queued
        || (stored.machine_state.lifecycle_phase == meerkat::JobPhase::RetryScheduled
            && stored
                .machine_state
                .retry_due_at_ms
                .is_some_and(|due_at_ms| claimed_at_ms >= due_at_ms));
    if !runnable {
        return Ok(());
    }
    let lease_expires_at_ms = claimed_at_ms
        .checked_add(120_000)
        .ok_or_else(|| "detached callback lease timestamp overflowed".to_string())?;
    let runner_handle = format!(
        "callback:{job_id}:attempt:{}",
        stored.machine_state.attempt_count.saturating_add(1)
    );
    let claim = match runtime
        .service
        .claim_attempt(
            &job_id,
            meerkat::AttemptClaim::new(
                meerkat::WorkerId::new(format!("mobkit-callback:{}", std::process::id()))
                    .map_err(|error| error.to_string())?,
                claimed_at_ms,
                lease_expires_at_ms,
                meerkat::RunnerHandleRef::new(runner_handle.clone())
                    .map_err(|error| error.to_string())?,
            ),
        )
        .await
    {
        Ok(claim) => claim,
        Err(
            meerkat::DetachedJobError::StaleRevision { .. }
            | meerkat::DetachedJobError::InvalidTransition { .. },
        ) => return Ok(()),
        Err(error) => return Err(error.to_string()),
    };
    let specification_ref = stored
        .spec
        .runner_specification_ref
        .as_ref()
        .ok_or_else(|| format!("detached callback job {job_id} has no runner specification"))?;
    let specification = runtime
        .blob_store
        .get(&meerkat_core::BlobId::new(specification_ref.as_str()))
        .await
        .map_err(|error| error.to_string())?;
    let arguments: Value =
        serde_json::from_str(&specification.data).map_err(|error| error.to_string())?;
    let credential_scopes = stored
        .spec
        .credential_context_refs
        .iter()
        .flat_map(|reference| match reference {
            meerkat_core::ToolCredentialContextRef::OwningProfile { required_scopes }
            | meerkat_core::ToolCredentialContextRef::AuthBinding {
                required_scopes, ..
            } => required_scopes.iter().cloned().collect::<Vec<_>>(),
        })
        .collect();
    let params = meerkat_contracts::CallbackJobStartParams {
        authority: meerkat_contracts::JobAttemptAuthority {
            job_id: job_id.to_string(),
            attempt_id: claim.attempt_id.to_string(),
            fence: claim.fence.get(),
        },
        runner: meerkat_contracts::JobRunner {
            name: stored.spec.runner.name().to_string(),
            version: stored.spec.runner.version().to_string(),
        },
        restart_class: callback_wire_restart_class(stored.spec.restart_class),
        runner_handle: runner_handle.clone(),
        runner_specification_ref: Some(specification_ref.to_string()),
        arguments,
        credential_scopes,
        resume_checkpoint: claim.resume_checkpoint.as_ref().map(ToString::to_string),
    };
    spawn_callback_lease_tracker(runtime.clone(), bridge.clone(), params.authority.clone());
    let result: meerkat_contracts::CallbackJobStartResult = serde_json::from_value(
        bridge
            .call(
                "callback/job/start",
                serde_json::to_value(params).map_err(|error| error.to_string())?,
            )
            .await?,
    )
    .map_err(|error| error.to_string())?;
    if !result.accepted || result.runner_handle != runner_handle {
        runtime
            .service
            .mark_needs_attention(
                &job_id,
                callback_unix_time_ms().unwrap_or(claimed_at_ms),
                meerkat::JobFailureCode::new(if result.accepted {
                    "callback_runner_handle_mismatch"
                } else {
                    "callback_start_rejected"
                })
                .map_err(|error| error.to_string())?,
            )
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn callback_unix_time_ms() -> Result<u64, String> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_millis();
    u64::try_from(millis).map_err(|_| "wall clock exceeds u64 milliseconds".to_string())
}

async fn callback_job_description(
    runtime: &DetachedCallbackJobRuntime,
    job_id: &meerkat::JobId,
) -> Result<meerkat::JobDescription, String> {
    runtime
        .service
        .describe_for_realm(&runtime.realm_id, job_id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("detached job {job_id} does not exist in this realm"))
}

fn callback_write_authority(
    authority: meerkat_contracts::JobAttemptAuthority,
) -> Result<(meerkat::JobId, meerkat::AttemptWriteAuthority), String> {
    Ok((
        meerkat::JobId::new(authority.job_id).map_err(|error| error.to_string())?,
        meerkat::AttemptWriteAuthority {
            attempt_id: meerkat::AttemptId::new(authority.attempt_id)
                .map_err(|error| error.to_string())?,
            fence: meerkat::FenceToken::new(authority.fence),
        },
    ))
}

fn callback_job_rpc_response(id: Value, result: Result<Value, String>) -> Result<String, String> {
    serde_json::to_string(&match result {
        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err(message) => {
            json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32602, "message": message}})
        }
    })
    .map_err(|error| error.to_string())
}

/// Handle the canonical Meerkat job surface over MobKit's inherited job
/// store. Returns `None` for methods owned by the ordinary MobKit router.
async fn handle_callback_job_rpc(
    request_line: &str,
    runtime: Option<&DetachedCallbackJobRuntime>,
    bridge: &StdioCallbackBridge,
) -> Option<String> {
    const METHODS: &[&str] = &[
        "jobs/get",
        "jobs/list",
        "jobs/cancel",
        "jobs/progress",
        "jobs/result",
        "jobs/artifacts",
        "jobs/retry",
        "jobs/health",
        "jobs/subscribe",
        "jobs/unsubscribe",
        "monitors/start",
        "mobkit/jobs/heartbeat",
        "mobkit/jobs/progress",
        "mobkit/jobs/checkpoint",
        "mobkit/jobs/complete",
        "mobkit/jobs/fail",
        "mobkit/jobs/cancel_ack",
    ];
    let request: Value = serde_json::from_str(request_line).ok()?;
    let method = request.get("method").and_then(Value::as_str)?;
    if !METHODS.contains(&method) {
        return None;
    }
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let Some(runtime) = runtime else {
        return Some(
            callback_job_rpc_response(
                id,
                Err(
                    "durable jobs require persistent_state; semantic detached admission is disabled"
                        .to_string(),
                ),
            )
            .unwrap_or_default(),
        );
    };
    let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
    let result: Result<Value, String> = async {
        match method {
            "jobs/get" => {
                let params: meerkat_contracts::JobsGetParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let job_id =
                    meerkat::JobId::new(params.job_id).map_err(|error| error.to_string())?;
                let job = callback_job_description(runtime, &job_id).await?;
                serde_json::to_value(meerkat_contracts::JobsGetResult {
                    job: meerkat::project_job_description(job),
                })
                .map_err(|error| error.to_string())
            }
            "jobs/list" => {
                let params: meerkat_contracts::JobsListParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let session = params
                    .session_id
                    .ok_or_else(|| "jobs/list requires session_id".to_string())
                    .and_then(|raw| {
                        meerkat_core::SessionId::parse(&raw).map_err(|error| error.to_string())
                    })?;
                let limit =
                    usize::try_from(params.limit.unwrap_or(100).min(1_000)).unwrap_or(1_000);
                let jobs = runtime
                    .service
                    .list_descriptions_for_origin(&runtime.realm_id, &session, limit)
                    .await
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .map(meerkat::project_job_description)
                    .collect();
                serde_json::to_value(meerkat_contracts::JobsListResult { jobs })
                    .map_err(|error| error.to_string())
            }
            "jobs/cancel" => {
                let params: meerkat_contracts::JobsCancelParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let job_id =
                    meerkat::JobId::new(params.job_id).map_err(|error| error.to_string())?;
                let stored = runtime
                    .store
                    .get(&job_id)
                    .await
                    .map_err(|error| error.to_string())?
                    .filter(|job| job.spec.realm_id == runtime.realm_id)
                    .ok_or_else(|| format!("detached job {job_id} does not exist in this realm"))?;
                if matches!(
                    stored.spec.runner.name(),
                    "meerkat.shell" | "meerkat.monitor_script"
                ) {
                    let manager = runtime
                        .monitor_manager(&stored.spec.origin_session_id)
                        .await?;
                    manager
                        .cancel_job(&meerkat_tools::builtin::shell::JobId::from_string(
                            job_id.to_string(),
                        ))
                        .await
                        .map_err(|error| error.to_string())?;
                } else {
                    let snapshot = runtime
                        .service
                        .request_cancel(&job_id)
                        .await
                        .map_err(|error| error.to_string())?;
                    if runtime.owns_callback_job(&stored.spec)
                        && let Some(attempt_id) = snapshot.current_attempt_id.as_ref()
                        && matches!(
                            snapshot.phase,
                            meerkat::JobPhase::Running | meerkat::JobPhase::WaitingExternal
                        )
                    {
                        let cancel: meerkat_contracts::CallbackJobCancelResult =
                            serde_json::from_value(
                                bridge
                                    .call(
                                        "callback/job/cancel",
                                        serde_json::to_value(
                                            meerkat_contracts::CallbackJobCancelParams {
                                                authority: meerkat_contracts::JobAttemptAuthority {
                                                    job_id: job_id.to_string(),
                                                    attempt_id: attempt_id.to_string(),
                                                    fence: snapshot.current_fence.get(),
                                                },
                                            },
                                        )
                                        .map_err(|error| error.to_string())?,
                                    )
                                    .await?,
                            )
                            .map_err(|error| error.to_string())?;
                        if !cancel.accepted {
                            return Err(
                                "callback/job/cancel rejected the committed active attempt"
                                    .to_string(),
                            );
                        }
                    }
                }
                let job = callback_job_description(runtime, &job_id).await?;
                serde_json::to_value(meerkat_contracts::JobsCancelResult {
                    job: meerkat::project_job_description(job),
                })
                .map_err(|error| error.to_string())
            }
            "jobs/progress" => {
                let params: meerkat_contracts::JobsProgressParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let job_id =
                    meerkat::JobId::new(params.job_id).map_err(|error| error.to_string())?;
                let job = meerkat::project_job_description(
                    callback_job_description(runtime, &job_id).await?,
                );
                serde_json::to_value(meerkat_contracts::JobsProgressResult {
                    job_id: job.job_id,
                    phase: job.phase,
                    progress: job.progress,
                })
                .map_err(|error| error.to_string())
            }
            "jobs/result" => {
                let params: meerkat_contracts::JobsResultParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let job_id =
                    meerkat::JobId::new(params.job_id).map_err(|error| error.to_string())?;
                let job = meerkat::project_job_description(
                    callback_job_description(runtime, &job_id).await?,
                );
                serde_json::to_value(meerkat_contracts::JobsResultResult {
                    job_id: job.job_id,
                    phase: job.phase,
                    result: job.terminal_result,
                })
                .map_err(|error| error.to_string())
            }
            "jobs/artifacts" => {
                let params: meerkat_contracts::JobsArtifactsParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let job_id =
                    meerkat::JobId::new(params.job_id).map_err(|error| error.to_string())?;
                let job = callback_job_description(runtime, &job_id).await?;
                let reference = match job.terminal_result {
                    Some(
                        meerkat::JobTerminalResult::Succeeded {
                            result_ref: Some(reference),
                        }
                        | meerkat::JobTerminalResult::Failed {
                            detail_ref: Some(reference),
                            ..
                        },
                    ) => Some(reference.to_string()),
                    _ => None,
                };
                serde_json::to_value(meerkat_contracts::JobsArtifactsResult {
                    job_id: job_id.to_string(),
                    artifacts: reference
                        .into_iter()
                        .map(|reference| meerkat_contracts::JobArtifactRef { reference })
                        .collect(),
                })
                .map_err(|error| error.to_string())
            }
            "jobs/retry" => {
                let params: meerkat_contracts::JobsRetryParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let job_id =
                    meerkat::JobId::new(params.job_id).map_err(|error| error.to_string())?;
                let stored = runtime
                    .store
                    .get(&job_id)
                    .await
                    .map_err(|error| error.to_string())?
                    .filter(|job| job.spec.realm_id == runtime.realm_id)
                    .ok_or_else(|| format!("detached job {job_id} does not exist in this realm"))?;
                let shell_owned = matches!(
                    stored.spec.runner.name(),
                    "meerkat.shell" | "meerkat.monitor_script"
                );
                runtime
                    .service
                    .schedule_retry(&job_id, params.retry_due_at_ms)
                    .await
                    .map_err(|error| error.to_string())?;
                let now = callback_unix_time_ms()?;
                let delay_ms = params.retry_due_at_ms.saturating_sub(now);
                if shell_owned {
                    let manager = runtime
                        .monitor_manager(&stored.spec.origin_session_id)
                        .await?;
                    let public_job_id =
                        meerkat_tools::builtin::shell::JobId::from_string(job_id.to_string());
                    tokio::spawn(async move {
                        if delay_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        }
                        if let Err(error) = manager.get_status(&public_job_id).await {
                            tracing::warn!(%error, "durable shell/monitor retry start failed");
                        }
                    });
                } else if runtime.owns_callback_job(&stored.spec) {
                    let runtime = (*runtime).clone();
                    let bridge = bridge.clone();
                    let retry_job_id = job_id.clone();
                    tokio::spawn(async move {
                        if delay_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        }
                        if let Err(error) =
                            start_detached_callback_attempt(runtime, bridge, retry_job_id).await
                        {
                            tracing::warn!(%error, "detached callback retry start failed");
                        }
                    });
                } else {
                    // Retry scheduling is machine authority. An unregistered
                    // runner may be claimed by another host; this gateway
                    // simply refrains from impersonating that execution owner.
                }
                let job = callback_job_description(runtime, &job_id).await?;
                serde_json::to_value(meerkat_contracts::JobsRetryResult {
                    job: meerkat::project_job_description(job),
                })
                .map_err(|error| error.to_string())
            }
            "jobs/health" => {
                let health = runtime
                    .service
                    .health_snapshot_for_realm(
                        &runtime.realm_id,
                        callback_unix_time_ms()?,
                        usize::MAX,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                // Separate wedges, deliberately not summed - see the health
                // projection above.
                let runtime_inbox_backlog = runtime.runtime_inbox_backlog_count().await?;
                // Carry meerkat's third rung through to the wire. Folding
                // `Unreadable` into either `Ok` or `Degraded` would publish a
                // verdict the census did not establish.
                let reading = health.reading().worst(if runtime_inbox_backlog > 0 {
                    meerkat::JobHealthReading::Degraded
                } else {
                    meerkat::JobHealthReading::Ok
                });
                serde_json::to_value(meerkat_contracts::JobsHealthResult {
                    detached_jobs: meerkat_contracts::JobHealthSummary {
                        status: match reading {
                            meerkat::JobHealthReading::Ok => meerkat_contracts::JobHealthStatus::Ok,
                            meerkat::JobHealthReading::Degraded => {
                                meerkat_contracts::JobHealthStatus::Degraded
                            }
                            meerkat::JobHealthReading::Unreadable => {
                                meerkat_contracts::JobHealthStatus::Unreadable
                            }
                        },
                        queued: health.queued,
                        running: health.running,
                        awaiting_members: health.awaiting_members,
                        stale_leases: health.stale_leases,
                        needs_attention: health.needs_attention,
                        pending_outbox_jobs: health.pending_outbox_jobs,
                        runtime_inbox_backlog,
                        coverage: match health.coverage {
                            meerkat::JobHealthCoverage::Complete => {
                                meerkat_contracts::JobHealthCoverage::Complete
                            }
                            meerkat::JobHealthCoverage::Truncated { scanned, limit } => {
                                meerkat_contracts::JobHealthCoverage::Truncated { scanned, limit }
                            }
                        },
                    },
                })
                .map_err(|error| error.to_string())
            }
            "jobs/subscribe" => {
                let params: meerkat_contracts::JobsSubscribeParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let job_id =
                    meerkat::JobId::new(params.job_id).map_err(|error| error.to_string())?;
                callback_job_description(runtime, &job_id).await?;
                let delivery = match params.delivery {
                    meerkat_contracts::JobDeliveryKind::Record => meerkat::JobDeliveryKind::Record,
                    meerkat_contracts::JobDeliveryKind::Notification => {
                        meerkat::JobDeliveryKind::Notification
                    }
                    meerkat_contracts::JobDeliveryKind::Event { handling_mode } => {
                        meerkat::JobDeliveryKind::Event {
                            handling_mode: handling_mode.into(),
                        }
                    }
                };
                runtime
                    .service
                    .subscribe(
                        &job_id,
                        meerkat::JobSubscription::new(
                            meerkat::JobSubscriptionId::new(params.subscription_id)
                                .map_err(|error| error.to_string())?,
                            meerkat_core::SessionId::parse(&params.session_id)
                                .map_err(|error| error.to_string())?,
                            delivery,
                        ),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                let job = callback_job_description(runtime, &job_id).await?;
                serde_json::to_value(meerkat_contracts::JobsSubscribeResult {
                    job: meerkat::project_job_description(job),
                })
                .map_err(|error| error.to_string())
            }
            "jobs/unsubscribe" => {
                let params: meerkat_contracts::JobsUnsubscribeParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let job_id =
                    meerkat::JobId::new(params.job_id).map_err(|error| error.to_string())?;
                callback_job_description(runtime, &job_id).await?;
                runtime
                    .service
                    .unsubscribe(
                        &job_id,
                        &meerkat::JobSubscriptionId::new(params.subscription_id)
                            .map_err(|error| error.to_string())?,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                let job = callback_job_description(runtime, &job_id).await?;
                serde_json::to_value(meerkat_contracts::JobsUnsubscribeResult {
                    job: meerkat::project_job_description(job),
                })
                .map_err(|error| error.to_string())
            }
            "monitors/start" => {
                let params: meerkat_contracts::MonitorsStartParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let session_id = meerkat_core::SessionId::parse(&params.session_id)
                    .map_err(|error| error.to_string())?;
                let mut limits = meerkat_tools::builtin::shell::MonitorProtocolLimits::default();
                if let Some(value) = params.max_line_bytes {
                    limits.max_line_bytes = usize::try_from(value)
                        .map_err(|_| "max_line_bytes exceeds this host's size limit".to_string())?;
                }
                if let Some(value) = params.max_notifications_per_window {
                    limits.max_notifications_per_window = usize::try_from(value).map_err(|_| {
                        "max_notifications_per_window exceeds this host's size limit".to_string()
                    })?;
                }
                if let Some(value) = params.notification_window_ms {
                    limits.notification_window_ms = value;
                }
                if let Some(value) = params.max_retained_diagnostic_bytes {
                    limits.max_retained_diagnostic_bytes =
                        usize::try_from(value).map_err(|_| {
                            "max_retained_diagnostic_bytes exceeds this host's size limit"
                                .to_string()
                        })?;
                }
                let protocol = match params.protocol {
                    meerkat_contracts::MonitorOutputProtocol::FramedJsonl => {
                        meerkat_tools::builtin::shell::MonitorOutputProtocol::FramedJsonl
                    }
                    meerkat_contracts::MonitorOutputProtocol::Lines => {
                        meerkat_tools::builtin::shell::MonitorOutputProtocol::Lines
                    }
                };
                let restart_class = match params.restart_class {
                    meerkat_contracts::JobRestartClass::Adoptable => {
                        meerkat::RestartClass::Adoptable
                    }
                    meerkat_contracts::JobRestartClass::CheckpointResumable => {
                        meerkat::RestartClass::CheckpointResumable
                    }
                    meerkat_contracts::JobRestartClass::Replayable => {
                        meerkat::RestartClass::Replayable
                    }
                    meerkat_contracts::JobRestartClass::NonResumable => {
                        meerkat::RestartClass::NonResumable
                    }
                };
                let delivery = match params.delivery {
                    meerkat_contracts::JobDeliveryKind::Record => meerkat::JobDeliveryKind::Record,
                    meerkat_contracts::JobDeliveryKind::Notification => {
                        meerkat::JobDeliveryKind::Notification
                    }
                    meerkat_contracts::JobDeliveryKind::Event { handling_mode } => {
                        meerkat::JobDeliveryKind::Event {
                            handling_mode: handling_mode.into(),
                        }
                    }
                };
                let manager = runtime.monitor_manager(&session_id).await?;
                let job_id = manager
                    .spawn_monitor_for_call(
                        &params.command,
                        params.working_dir.as_deref().map(std::path::Path::new),
                        params.timeout_secs,
                        &params.submission_key,
                        meerkat_tools::builtin::shell::MonitorStartOptions {
                            protocol,
                            restart_class,
                            limits,
                            delivery,
                        },
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                let job_id =
                    meerkat::JobId::new(job_id.to_string()).map_err(|error| error.to_string())?;
                let job = callback_job_description(runtime, &job_id).await?;
                serde_json::to_value(meerkat_contracts::MonitorsStartResult {
                    job: meerkat::project_job_description(job),
                })
                .map_err(|error| error.to_string())
            }
            "mobkit/jobs/heartbeat" => {
                let params: meerkat_contracts::MobkitJobHeartbeatParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let (job_id, write) = callback_write_authority(params.authority)?;
                callback_job_description(runtime, &job_id).await?;
                runtime
                    .service
                    .renew_lease(
                        &job_id,
                        write,
                        params.heartbeat_at_ms,
                        params.lease_expires_at_ms,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                callback_mutation_projection(runtime, &job_id).await
            }
            "mobkit/jobs/progress" => {
                let params: meerkat_contracts::MobkitJobProgressParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let (job_id, write) = callback_write_authority(params.authority)?;
                callback_job_description(runtime, &job_id).await?;
                runtime
                    .service
                    .report_progress(
                        &job_id,
                        write,
                        meerkat::JobProgress::new(params.cursor, params.detail)
                            .map_err(|error| error.to_string())?,
                        params.observed_at_ms,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                callback_mutation_projection(runtime, &job_id).await
            }
            "mobkit/jobs/checkpoint" => {
                let params: meerkat_contracts::MobkitJobCheckpointParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let (job_id, write) = callback_write_authority(params.authority)?;
                callback_job_description(runtime, &job_id).await?;
                runtime
                    .service
                    .record_checkpoint(
                        &job_id,
                        write,
                        meerkat::CheckpointRef::new(params.checkpoint_ref)
                            .map_err(|error| error.to_string())?,
                        params.observed_at_ms,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                callback_mutation_projection(runtime, &job_id).await
            }
            "mobkit/jobs/complete" => {
                let params: meerkat_contracts::MobkitJobCompleteParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let (job_id, write) = callback_write_authority(params.authority)?;
                callback_job_description(runtime, &job_id).await?;
                runtime
                    .service
                    .complete_attempt(
                        &job_id,
                        write,
                        params.completed_at_ms,
                        params
                            .result_ref
                            .map(meerkat::JobResultRef::new)
                            .transpose()
                            .map_err(|error| error.to_string())?,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                callback_mutation_projection(runtime, &job_id).await
            }
            "mobkit/jobs/fail" => {
                let params: meerkat_contracts::MobkitJobFailParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let (job_id, write) = callback_write_authority(params.authority)?;
                callback_job_description(runtime, &job_id).await?;
                runtime
                    .service
                    .fail_attempt(
                        &job_id,
                        write,
                        params.failed_at_ms,
                        meerkat::JobFailureCode::new(params.code)
                            .map_err(|error| error.to_string())?,
                        params
                            .detail_ref
                            .map(meerkat::JobResultRef::new)
                            .transpose()
                            .map_err(|error| error.to_string())?,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                callback_mutation_projection(runtime, &job_id).await
            }
            "mobkit/jobs/cancel_ack" => {
                let params: meerkat_contracts::MobkitJobCancelAckParams =
                    serde_json::from_value(params).map_err(|error| error.to_string())?;
                let (job_id, write) = callback_write_authority(params.authority)?;
                callback_job_description(runtime, &job_id).await?;
                runtime
                    .service
                    .acknowledge_cancel(&job_id, write, params.acknowledged_at_ms)
                    .await
                    .map_err(|error| error.to_string())?;
                callback_mutation_projection(runtime, &job_id).await
            }
            _ => unreachable!("method allowlist and match must stay in sync"),
        }
    }
    .await;
    Some(callback_job_rpc_response(id, result).unwrap_or_default())
}

async fn callback_mutation_projection(
    runtime: &DetachedCallbackJobRuntime,
    job_id: &meerkat::JobId,
) -> Result<Value, String> {
    let job = callback_job_description(runtime, job_id).await?;
    serde_json::to_value(meerkat_contracts::MobkitJobMutationResult {
        job: meerkat::project_job_description(job),
    })
    .map_err(|error| error.to_string())
}

/// Wraps FactoryAgentBuilder — sends callback/build_agent to Python before building.
struct StdioCallbackAgentBuilder {
    inner: FactoryAgentBuilder,
    bridge: StdioCallbackBridge,
    has_session_builder: bool,
    /// Session store for loading sessions by ID when the Python builder
    /// sets `resume_session_id`. Only populated in persistent mode.
    session_store: Option<Arc<dyn meerkat::SessionStore>>,
    detached_jobs: Option<DetachedCallbackJobRuntime>,
}

fn callback_build_agent_options(req: &CreateSessionRequest, scope_id: &str) -> Value {
    let request_labels = req.labels.as_ref();
    let build_labels = req
        .build
        .as_ref()
        .and_then(|build| build.peer_meta.as_ref())
        .map(|meta| &meta.labels);
    let mut labels = BTreeMap::new();
    if let Some(build_labels) = build_labels {
        labels.extend(
            build_labels
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
    }
    if let Some(request_labels) = request_labels {
        labels.extend(
            request_labels
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
    }
    let labels = (!labels.is_empty()).then_some(labels);
    let profile_name = build_labels
        .and_then(|labels| labels.get("profile_name").or_else(|| labels.get("role")))
        .or_else(|| {
            request_labels
                .and_then(|labels| labels.get("profile_name").or_else(|| labels.get("role")))
        });
    // Mint-vs-resume signal (task #62, a downstream app field ask 2026-08-06): a
    // spawn-level resume is known here via `build.resume_session`, so the
    // host can honor the System-message contract (append standing
    // instructions on MINT, inherit on RESUME) instead of inferring
    // platform state from its own continuity store. `session_id` keeps its
    // label-sourced semantics but falls back to the resumed session's id,
    // which is the identity the older restore-path complaint was missing.
    //
    // Meerkat types which one a carried session is (#1225): a spawn seats a
    // pre-assigned id as an empty Mint carrier, a resume carries the session
    // it continues. `resume_session_id` is sent on Resume only, so a host
    // never mistakes a fresh member's pre-assigned id for a resume and skips
    // its standing instructions; the carried id still rides as `session_id`.
    let carried_session_id = req
        .build
        .as_ref()
        .and_then(|b| b.resume_session.as_ref())
        .map(|s| s.id().to_string());
    let session_build_intent = req
        .build
        .as_ref()
        .map_or(meerkat_core::service::SessionBuildIntent::Mint, |b| {
            b.session_build_intent()
        });
    let resume_session_id = match session_build_intent {
        meerkat_core::service::SessionBuildIntent::Resume => carried_session_id.clone(),
        _ => None,
    };
    let session_build_intent = match session_build_intent {
        meerkat_core::service::SessionBuildIntent::Resume => "resume",
        _ => "mint",
    };
    // Fork lineage (`SessionBuildOptions::fork_source`): set by the mob
    // runtime alone, on the build that seats a durable fork with its source's
    // build inheritance and on every later rebuild of that member; absent for
    // every other build. Forwarded exactly as meerkat serializes it. Its
    // `source_member.member` is MobKit's encoded roster id (`mk--...`), while
    // hosts key grants by durable identity, so the source's identity rides
    // beside it, resolved through MobKit's own roster mapping. The child's
    // own labels and session identity above are untouched by either.
    let fork_source = req.build.as_ref().and_then(|b| b.fork_source.as_ref());
    let fork_source_identity = fork_source.map(|source| {
        meerkat_mobkit::member_comms_id::durable_identity_for_roster_member(
            &source.source_member.member,
        )
    });
    json!({
        "scope_id": scope_id,
        "session_id": labels
            .as_ref()
            .and_then(|l| l.get("session_id").cloned())
            .or_else(|| carried_session_id.clone()),
        "profile_name": profile_name,
        "model": &req.model,
        "prompt": &req.prompt,
        "labels": &labels,
        "app_context": req.build.as_ref()
            .and_then(|b| b.app_context.as_ref()),
        "resume_session_id": resume_session_id,
        "session_build_intent": session_build_intent,
        "fork_source": fork_source,
        "fork_source_identity": fork_source_identity,
    })
}

impl StdioCallbackAgentBuilder {
    /// Apply a `callback/build_agent` response to a copy of `req`.
    ///
    /// The response may add standing instructions (mint builds only), labels,
    /// a resume and callback tools. It can never set or alter
    /// `build.fork_source`: fork lineage is minted by the mob runtime alone
    /// and the host only ever receives it, so a response naming
    /// `fork_source` (or `fork_source_identity`) is ignored and the request's
    /// own lineage, or its absence, is what the build sees.
    /// `external_tools` is the request's external tool dispatcher, passed
    /// explicitly rather than read from `req`: the owned build path drains it
    /// from the caller's request first, so the transformed request is its
    /// only holder (see `build_agent_taking_tools`).
    async fn apply_build_agent_response(
        &self,
        req: &CreateSessionRequest,
        external_tools: Option<Arc<dyn meerkat_core::AgentToolDispatcher>>,
        result: &Value,
        scope_id: &str,
    ) -> Result<CreateSessionRequest, SessionError> {
        if result.get("fork_source").is_some() || result.get("fork_source_identity").is_some() {
            tracing::warn!(
                "callback/build_agent: fork_source in a build response is ignored; fork \
                 lineage is set only by the mob runtime"
            );
        }
        // Apply Python-returned options to a cloned request
        let mut modified_req = CreateSessionRequest {
            model: req.model.clone(),
            prompt: req.prompt.clone(),
            system_prompt: req.system_prompt.clone(),
            max_tokens: req.max_tokens,
            event_tx: req.event_tx.clone(),
            initial_turn: req.initial_turn.clone(),
            build: req.build.clone(),
            labels: req.labels.clone(),
            deferred_prompt_policy: req.deferred_prompt_policy,
            injected_context: req.injected_context.clone(),
        };
        match external_tools {
            Some(tools) => {
                modified_req
                    .build
                    .get_or_insert_with(meerkat_core::service::SessionBuildOptions::default)
                    .external_tools = Some(tools);
            }
            None => {
                if let Some(build) = modified_req.build.as_mut() {
                    build.external_tools = None;
                }
            }
        }
        // Resume-ness decides the instruction fold below, so resolve
        // it FIRST: a resume can arrive spawn-level
        // (build.resume_session already loaded) or be requested by
        // the Python response (resume_session_id, applied further
        // down) - both shapes must suppress the fold.
        // A spawn-level Mint carrier (the pre-assigned id of a fresh
        // member) is not a resume: meerkat types it (#1225).
        let spawn_resumed = modified_req.build.as_ref().is_some_and(|b| {
            b.session_build_intent() == meerkat_core::service::SessionBuildIntent::Resume
        });
        let resumed = spawn_resumed
            || result
                .get("resume_session_id")
                .and_then(|v| v.as_str())
                .is_some();
        // Apply additional_instructions through meerkat's NATIVE
        // standing-instructions carrier
        // (SessionBuildOptions.additional_instructions) - on MINT
        // builds only. The old translation folded them into an
        // explicit SystemPromptOverride::Set, a transcript-authoring
        // ruling this layer does not own: an explicit Set at resume
        // IS new transcript intent by contract, so the runtime
        // recorded one assembled System row per boot (the downstream app's
        // accretion: one member reached 1,294,962 tokens against a
        // 922,000 ceiling). Standing instructions are per-session
        // build state baked at mint; a deliberate mid-life change
        // must use the typed transcript admission.
        if let Some(instructions) = result.get("additional_instructions") {
            if let Some(arr) = instructions.as_array() {
                let combined: Vec<String> = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(ToString::to_string))
                    .collect();
                if !combined.is_empty() {
                    if resumed {
                        tracing::warn!(
                            "callback/build_agent: additional_instructions ignored on a \
                             RESUMED build (resume inherits persisted prompt state; \
                             append deliberate System content through the typed \
                             admission instead)"
                        );
                    } else {
                        let build = modified_req.build.get_or_insert_with(|| {
                            meerkat_core::service::SessionBuildOptions::default()
                        });
                        // Merge, preserving instructions another
                        // customizer already installed.
                        build
                            .additional_instructions
                            .get_or_insert_with(Vec::new)
                            .extend(combined);
                    }
                }
            }
        }
        // Apply labels
        if let Some(labels) = result.get("labels").and_then(|v| v.as_object()) {
            let label_map = modified_req.labels.get_or_insert_with(Default::default);
            for (k, v) in labels {
                if let Some(s) = v.as_str() {
                    label_map.insert(k.clone(), s.to_string());
                }
            }
        }
        // Apply resume_session_id: Python builder can request resuming
        // an existing session. The gateway loads the session from the
        // store and sets it on build.resume_session.
        if let Some(resume_id) = result.get("resume_session_id").and_then(|v| v.as_str()) {
            if let Some(ref store) = self.session_store {
                let sid = meerkat_core::types::SessionId::parse(resume_id).map_err(|_| {
                    SessionError::Agent(agent_tool_error(format!(
                        "callback/build_agent: invalid resume_session_id: {resume_id}"
                    )))
                })?;
                // Validate against any spawn-level RESUME already set. A Mint
                // carrier holds only a pre-assigned id; a host resume replaces
                // it with the loaded session.
                if let Some(existing) = modified_req
                    .build
                    .as_ref()
                    .filter(|b| {
                        b.session_build_intent()
                            == meerkat_core::service::SessionBuildIntent::Resume
                    })
                    .and_then(|b| b.resume_session.as_ref())
                {
                    if existing.id() != &sid {
                        return Err(SessionError::Agent(agent_tool_error(format!(
                            "callback/build_agent: resume_session_id conflict: \
                             spawn set {} but hook set {resume_id}",
                            existing.id()
                        ))));
                    }
                    // Same ID — already loaded, skip.
                } else {
                    let session = store.load(&sid).await.map_err(|e| {
                        SessionError::Agent(agent_tool_error(format!(
                            "callback/build_agent: failed to load resume session {resume_id}: {e}"
                        )))
                    })?;
                    let session = session.ok_or_else(|| {
                        SessionError::Agent(agent_tool_error(format!(
                            "callback/build_agent: resume session not found: {resume_id}"
                        )))
                    })?;
                    let build = modified_req.build.get_or_insert_with(|| {
                        meerkat_core::service::SessionBuildOptions::default()
                    });
                    build.resume_existing_session(session);
                }
            } else {
                return Err(SessionError::Agent(agent_tool_error(
                    "callback/build_agent: resume_session_id requires persistent mode \
                     (no session store available in ephemeral mode)"
                        .to_string(),
                )));
            }
        }
        // Callback tools: Python SDK provides tool names via add_tools()
        // or register_tool(). Create a CallbackToolDispatcher that routes
        // tool calls back to Python via callback/call_tool.
        if let Some(tools) = result.get("tools") {
            match tools.as_array() {
                Some(arr) => {
                    let mut tool_specs = Vec::with_capacity(arr.len());
                    for v in arr {
                        let spec = CallbackToolSpec::parse(v).map_err(|reason| {
                            SessionError::Agent(agent_tool_error(format!(
                                "callback/build_agent: {reason}"
                            )))
                        })?;
                        tool_specs.push(spec);
                    }
                    if !tool_specs.is_empty() {
                        let dispatcher = CallbackToolDispatcher::new(
                            self.bridge.clone(),
                            scope_id.to_string(),
                            tool_specs,
                            self.detached_jobs.clone(),
                        );
                        if dispatcher.reconcile_registered_catalog {
                            let reconciliation = dispatcher.clone();
                            tokio::spawn(async move {
                                if let Err(error) = reconciliation.reconcile_detached_jobs().await {
                                    tracing::warn!(
                                        %error,
                                        "detached callback reconciliation failed"
                                    );
                                }
                            });
                        }
                        let build = modified_req.build.get_or_insert_with(|| {
                            meerkat_core::service::SessionBuildOptions::default()
                        });
                        // COMPOSE over whatever an earlier installer
                        // put in the slot (downstream app Bug D: assigning
                        // wholesale silently discarded the agent-memory
                        // recorder's `memory` tool for every
                        // callback-built agent). Python-registered
                        // tools win name collisions; everything else
                        // falls through to the pre-installed
                        // dispatcher.
                        let pre_installed = build.external_tools.take();
                        build.external_tools =
                            Some(meerkat_mobkit::tool_compose::ComposedExternalTools::over(
                                Arc::new(dispatcher),
                                pre_installed,
                            ));
                    }
                }
                None => {
                    return Err(SessionError::Agent(agent_tool_error(format!(
                        "callback/build_agent: tools must be a JSON array, got: {tools}"
                    ))));
                }
            }
        }
        Ok(modified_req)
    }
}

#[async_trait]
impl SessionAgentBuilder for StdioCallbackAgentBuilder {
    type Agent = FactoryAgent;

    async fn abort_absent_session_compaction_stages(
        &self,
        session_id: &meerkat_core::SessionId,
    ) -> Result<(), SessionError> {
        // This wrapper only adds SDK callbacks around agent construction. The
        // inner factory still owns the canonical durable-memory backend and
        // therefore the pre-materialization compaction reconciliation seam.
        // Falling back to the trait default here fails every retire/respawn of
        // an already disposed session even though the factory can reconcile it.
        self.inner
            .abort_absent_session_compaction_stages(session_id)
            .await
    }

    async fn build_agent(
        &self,
        req: &CreateSessionRequest,
        event_tx: mpsc::Sender<AgentEvent>,
    ) -> Result<Self::Agent, SessionError> {
        if !self.has_session_builder {
            let normalized_req = CreateSessionRequest {
                model: req.model.clone(),
                prompt: req.prompt.clone(),
                system_prompt: req.system_prompt.clone(),
                max_tokens: req.max_tokens,
                event_tx: req.event_tx.clone(),
                initial_turn: req.initial_turn.clone(),
                build: req.build.clone(),
                labels: req.labels.clone(),
                deferred_prompt_policy: req.deferred_prompt_policy,
                injected_context: req.injected_context.clone(),
            };
            return self.inner.build_agent(&normalized_req, event_tx).await;
        }

        // Generate a unique scope ID for this build — used to isolate tool
        // handlers between concurrent sessions on the Python side.
        let scope_id = format!(
            "build-{}",
            self.bridge
                .counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );

        // Send callback to Python with full session context.
        // app_context flows from SpawnMemberSpec.context → build.app_context.
        let options = callback_build_agent_options(req, &scope_id);
        let params = json!({ "options": options });
        let callback_result = self.bridge.call("callback/build_agent", params).await;

        match callback_result {
            Ok(result) => {
                let shared_tools = req
                    .build
                    .as_ref()
                    .and_then(|build| build.external_tools.clone());
                let modified_req = self
                    .apply_build_agent_response(req, shared_tools, &result, &scope_id)
                    .await?;
                self.inner.build_agent(&modified_req, event_tx).await
            }
            Err(err) => {
                // Propagate the error — before_create failure aborts session creation.
                // This is an intentional breaking change from v0.5.x where failures
                // were silently swallowed with a fallback to default build.
                Err(SessionError::Agent(agent_tool_error(format!(
                    "callback/build_agent failed: {err}"
                ))))
            }
        }
    }

    /// The owned build: the request's external tool dispatcher moves into the
    /// agent's composed tool surface instead of staying shared with `req`, so
    /// session-time binding (owner session, ops registry) can rebind it.
    /// Building through the borrowed `build_agent` would keep a second holder
    /// alive and fail an ops-capable source with `SharedOwnership`.
    ///
    /// The original request's tool slot is drained FIRST, before any clone, so
    /// no copy of the request keeps the dispatcher. Everything else in `req`
    /// stays readable after the call, as the trait requires.
    async fn build_agent_taking_tools(
        &self,
        req: &mut CreateSessionRequest,
        event_tx: mpsc::Sender<AgentEvent>,
    ) -> Result<Self::Agent, SessionError> {
        if !self.has_session_builder {
            // The borrowed path's normalization copies every field unchanged;
            // forwarding the owned request directly is the same request with
            // its tools handed over.
            return self.inner.build_agent_taking_tools(req, event_tx).await;
        }

        let scope_id = format!(
            "build-{}",
            self.bridge
                .counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let owned_tools = req
            .build
            .as_mut()
            .and_then(|build| build.external_tools.take());
        let options = callback_build_agent_options(req, &scope_id);
        let params = json!({ "options": options });
        match self.bridge.call("callback/build_agent", params).await {
            Ok(result) => {
                let mut modified_req = self
                    .apply_build_agent_response(req, owned_tools, &result, &scope_id)
                    .await?;
                self.inner
                    .build_agent_taking_tools(&mut modified_req, event_tx)
                    .await
            }
            Err(err) => Err(SessionError::Agent(agent_tool_error(format!(
                "callback/build_agent failed: {err}"
            )))),
        }
    }
}

/// Meerkat 0.7 replaced `agent_tool_error(String)` with the typed
/// `AgentError::Tool { error: ToolError }` carrier.
fn agent_tool_error(message: String) -> AgentError {
    AgentError::Tool {
        error: ToolError::execution_failed(message),
    }
}

/// Persistent mode: reads JSON-RPC over stdin, bootstraps unified runtime, serves HTTP.
/// Why the persistent dispatch loop stopped admitting requests: the `reason=`
/// token of the "dispatch loop ended" log line. The three designed exits are
/// named so an operator can tell them apart from each other and from a
/// crash, which leaves no such line (only the panic hook's ERROR, or nothing
/// at all when the process was killed from outside).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DispatchLoopEnd {
    /// The stdin reader closed the request channel: the launching process
    /// closed our stdin (EOF) or the read failed. Exiting on EOF is by
    /// design; a gateway whose parent went away has nobody left to answer.
    StdinClosed,
    /// SIGINT or SIGTERM arrived while stdin was still open.
    Signal(ShutdownSignal),
    /// The SDK's private shutdown handshake asked for an orderly stop.
    SdkShutdownHandshake,
}

impl DispatchLoopEnd {
    /// The signal that ended the loop, when one did.
    fn signal(self) -> Option<ShutdownSignal> {
        match self {
            Self::Signal(signal) => Some(signal),
            Self::StdinClosed | Self::SdkShutdownHandshake => None,
        }
    }
}

impl std::fmt::Display for DispatchLoopEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::StdinClosed => "stdin_closed",
            Self::Signal(_) => "signal",
            Self::SdkShutdownHandshake => "sdk_shutdown_handshake",
        })
    }
}

fn run_persistent(control_listen: Option<ControlListenAddr>) {
    // Deep worker stacks for the generated machine-authority apply path; the
    // builder is shared with mobkit_gateway, the failure reporting is not
    // (this binary has no init handshake to answer at this point).
    match meerkat_mobkit::gateway_composition::gateway_tokio_runtime() {
        Ok(runtime) => runtime.block_on(run_persistent_inner(control_listen)),
        Err(error) => {
            eprintln!("failed to build tokio runtime: {error}");
            std::process::exit(1);
        }
    }
}

async fn run_persistent_inner(control_listen: Option<ControlListenAddr>) {
    use tokio::io::{AsyncBufReadExt, BufReader};

    // Initialize tracing subscriber so meerkat-mob/meerkat-runtime errors
    // are visible on stderr. Without this, all tracing events are silently
    // dropped and runtime failures (agent build, LLM calls, comms drain)
    // are invisible.
    //
    // Default: this crate's own targets at INFO, dependencies at WARN.
    // Operationally significant boot phases (the one-time head-canonical
    // conversion, continuity repair) report at INFO from meerkat_mobkit; at
    // the old blanket "warn" default they were invisible, and a 2026-07
    // production deploy was aborted because a supervisor read a
    // silent-but-working migration as a hang. RUST_LOG still overrides
    // everything. Shared with mobkit_gateway so the two gateways cannot
    // drift on observability posture.
    meerkat_mobkit::gateway_composition::init_gateway_tracing(GATEWAY_TRACING_TARGET);

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);

    // 1. Read first line — must be mobkit/init
    let mut init_line = String::new();
    if reader.read_line(&mut init_line).await.unwrap_or(0) == 0 {
        eprintln!("rpc_gateway: stdin closed before init request");
        std::process::exit(1);
    }

    let init_raw: Value = match serde_json::from_str(init_line.trim()) {
        Ok(v) => v,
        Err(e) => {
            let error_response = json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": { "code": -32700, "message": format!("Parse error: {e}") }
            });
            println!(
                "{}",
                serde_json::to_string(&error_response)
                    .unwrap_or_else(|_| r#"{"error":"serialization failed"}"#.to_string())
            );
            std::process::exit(1);
        }
    };

    let request_id = init_raw.get("id").cloned().unwrap_or(Value::Null);
    let method = init_raw
        .get("method")
        .and_then(|m| m.as_str())
        .unwrap_or("");
    if method != "mobkit/init" {
        let error_response = json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": { "code": -32600, "message": format!("Expected mobkit/init, got {method}") }
        });
        println!(
            "{}",
            serde_json::to_string(&error_response)
                .unwrap_or_else(|_| r#"{"error":"serialization failed"}"#.to_string())
        );
        std::process::exit(1);
    }

    let params = init_raw.get("params").cloned().unwrap_or_else(|| json!({}));
    let _ = INIT_SESSION.set(InitSession {
        request_id: request_id.clone(),
        protocol: init_protocol_request(&params),
        accepted: AtomicBool::new(false),
        durable_phase_begun: AtomicBool::new(false),
        settled: AtomicBool::new(false),
    });

    // 2. Parse init params
    let mob_config_param = params.get("mob_config").and_then(|v| v.as_str());
    let is_workspace_config = mob_config_param.is_some();
    let mob_config_toml = mob_config_param.unwrap_or(
        r#"
[mob]
id = "persistent-gateway"

[profiles.default]
model = "gpt-5.5"
external_addressable = true
"#,
    );

    let definition = MobDefinition::from_toml(mob_config_toml).unwrap_or_else(|e| {
        let error_response = json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": { "code": -32602, "message": format!("Invalid mob_config TOML: {e}") }
        });
        println!(
            "{}",
            serde_json::to_string(&error_response)
                .unwrap_or_else(|_| r#"{"error":"serialization failed"}"#.to_string())
        );
        std::process::exit(1);
    });
    let image_generation = mob_definition_may_use_image_generation(&definition);
    let shell = mob_definition_may_use_shell(&definition);

    // A `[self_hosted]` or `[realm]` table in mob.toml is dropped by the
    // definition parser and would only show up later as a member that cannot
    // build. Refuse it here, naming the option that carries host config.
    if let Err(table) =
        meerkat_mobkit::gateway_composition::refuse_host_config_tables_in_mob_toml(mob_config_toml)
    {
        fail_init(&request_id, -32602, table.to_string());
    }

    // Fail-fast model resolution by meerkat's own rule (catalogued, or
    // `[models.<id>]`-defined, or provider-annotated), with the catalog typo
    // hint kept for the dated-id case; see `unknown_model_init_error`.
    if let Some(message) = unknown_model_init_error(&definition) {
        fail_init(&request_id, -32602, message);
    }

    let (modules, pre_spawn) = parse_gateway_modules(&params);

    let discovery_modules: Vec<String> = modules.iter().map(|m| m.id.clone()).collect();
    let module_config = MobKitConfig {
        modules,
        discovery: DiscoverySpec {
            namespace: "persistent-gateway".to_string(),
            modules: discovery_modules,
        },
        pre_spawn,
    };

    let has_session_builder = params
        .get("has_session_builder")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let persistent_state = params
        .get("persistent_state")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from);

    let has_roster_provider = params
        .get("has_roster_provider")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let has_topology_provider = params
        .get("has_topology_provider")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let has_agent_customizer = params
        .get("has_agent_customizer")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let has_continuity_store = params
        .get("has_continuity_store")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let has_lease_provider = params
        .get("has_lease_provider")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let scratch_dir = params
        .get("scratch_dir")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from);

    // The path authority for this boot. Without persistent_state the layout
    // is an explicitly declared-ephemeral scratch root (per-process, under
    // the OS temp dir) — recorded in the layout summary, never a silent
    // call-site fallback.
    let storage_layout = match persistent_state {
        Some(ref state_path) => {
            meerkat_mobkit::MobKitStorageLayout::with_injected_roots(state_path.clone(), None)
        }
        None => meerkat_mobkit::MobKitStorageLayout::declared_ephemeral(
            meerkat_mobkit::storage_layout::default_ephemeral_scratch_root(),
        ),
    };

    // 3. Set up stdout writer channel for multiplexed output
    let (stdout_tx, mut stdout_rx) = mpsc::channel::<GatewayStdoutLine>(64);
    let (stdout_shutdown_tx, mut stdout_shutdown_rx) = oneshot::channel::<()>();
    let mut stdout_writer = tokio::spawn(async move {
        loop {
            let line = tokio::select! {
                shutdown = &mut stdout_shutdown_rx => {
                    let _ = shutdown;
                    while let Ok(mut line) = stdout_rx.try_recv() {
                        let delivered = write_gateway_stdout_line(&mut line).await;
                        line.settle_delivery(delivered).await;
                    }
                    break;
                }
                line = stdout_rx.recv() => match line {
                    Some(line) => line,
                    None => break,
                },
            };
            let mut line = line;
            let delivered = write_gateway_stdout_line(&mut line).await;
            line.settle_delivery(delivered).await;
        }
    });

    // 4. Build callback bridge and start stdin multiplexer BEFORE bootstrap.
    // This ensures callback responses (e.g. callback/build_agent during discovery
    // spawn) are routed even while UnifiedRuntime::bootstrap is running.
    let bridge = StdioCallbackBridge::new(stdout_tx.clone());
    let (rpc_tx, mut rpc_rx) = mpsc::channel::<String>(64);
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    // False until init settles. Before that the dispatch loop is not running,
    // so ordinary requests are refused at once (INIT_IN_PROGRESS_CODE) instead
    // of queueing behind startup, where a provider callback awaiting one would
    // wait on itself and a full queue would stall callback-response routing.
    let dispatch_open = Arc::new(AtomicBool::new(false));

    let stdin_reader = tokio::spawn({
        let bridge = bridge.clone();
        let rpc_tx = rpc_tx.clone();
        let stdout_tx = stdout_tx.clone();
        let shutdown_requested = shutdown_requested.clone();
        let dispatch_open = dispatch_open.clone();
        async move {
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => {
                        // EOF: the launching process closed the pipe. This
                        // ends the gateway by design (`DispatchLoopEnd`), so
                        // say so before the loop's own exit line.
                        tracing::info!(
                            "stdin reached EOF: the launching process closed the pipe; closing \
                             request admission"
                        );
                        break;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(%error, "stdin read failed; closing request admission");
                        break;
                    }
                }
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let msg: Value = match serde_json::from_str(trimmed) {
                    Ok(v) => v,
                    Err(error) => {
                        // Never skip silently: a reply the SDK meant for a
                        // callback would leave that callback waiting out its
                        // full deadline. Fail it now when its id is readable.
                        // The line itself is not logged (it may carry data).
                        let callback_id = malformed_callback_response_id(trimmed);
                        tracing::warn!(
                            %error,
                            bytes = trimmed.len(),
                            callback_id = callback_id.as_deref().unwrap_or("none"),
                            "skipping a stdin line that is not valid JSON"
                        );
                        if let Some(id) = callback_id {
                            bridge.fail_malformed_callback_response(&id).await;
                        }
                        continue;
                    }
                };
                // Callback responses: "id" starts with "cb-" and no "method"
                let is_callback_response = msg
                    .get("id")
                    .and_then(|v| v.as_str())
                    .is_some_and(|id| id.starts_with("cb-"))
                    && msg.get("method").is_none();

                if is_callback_response {
                    bridge.route_callback_response(msg).await;
                } else if gateway_shutdown_request(&msg).is_some() {
                    // Stop new RPC admission as soon as the handshake enters
                    // the reader, but keep routing callback responses. The
                    // dispatch loop will retain the bridge through
                    // `runtime.shutdown()` and answer this request only after
                    // provider-backed cleanup has completed.
                    if shutdown_requested.swap(true, Ordering::AcqRel) {
                        if let Some(id) = msg.get("id") {
                            let response = json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": {
                                    "code": -32098,
                                    "message": "gateway shutdown already in progress"
                                }
                            });
                            if let Ok(line) = serde_json::to_string(&response) {
                                let _ = stdout_tx.send(GatewayStdoutLine::plain(line)).await;
                            }
                        }
                    } else if rpc_tx.send(trimmed.to_string()).await.is_err() {
                        break;
                    }
                } else if shutdown_requested.load(Ordering::Acquire) {
                    // Requests admitted after shutdown cannot be serviced and
                    // must not be left pending in an SDK. Callback responses
                    // were routed above and remain admitted.
                    if let Some(id) = msg.get("id") {
                        let response = json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": {
                                "code": -32098,
                                "message": "gateway shutdown in progress"
                            }
                        });
                        if let Ok(line) = serde_json::to_string(&response) {
                            let _ = stdout_tx.send(GatewayStdoutLine::plain(line)).await;
                        }
                    }
                } else if !dispatch_open.load(Ordering::Acquire) {
                    // Init has not settled: refuse at once, never queue.
                    if let Some(response) = init_in_progress_refusal(&msg)
                        && let Ok(line) = serde_json::to_string(&response)
                    {
                        let _ = stdout_tx.send(GatewayStdoutLine::plain(line)).await;
                    }
                } else {
                    // Queue RPC request for the dispatch loop
                    if rpc_tx.send(trimmed.to_string()).await.is_err() {
                        break;
                    }
                }
            }
            bridge.close().await;
        }
    });
    // The reader task owns the sole producer. Dropping this setup copy is
    // what lets EOF close `rpc_rx`; retaining it would make the dispatch loop
    // wait forever and skip graceful runtime shutdown entirely.
    drop(rpc_tx);

    /// Helper: report the init failure and exit. Before acceptance (or for a
    /// legacy SDK) that is the JSON-RPC error response; after acceptance it is
    /// the `failed` `mobkit/init_settled` notification with `durable_effects`.
    /// Storage-refusal sites pass [`STORAGE_RESOLUTION_CODE`] so SDKs reify
    /// the fail-closed durability errors; everything else keeps the standard
    /// JSON-RPC codes.
    fn fail_init(request_id: &Value, code: i64, message: String) -> ! {
        fail_init_with_data(request_id, code, message, None)
    }

    /// [`fail_init`] with the error's `data`: the refusal as structured data
    /// for the SDK, in the error response or the `failed` settlement alike.
    fn fail_init_with_data(
        request_id: &Value,
        code: i64,
        message: String,
        data: Option<Value>,
    ) -> ! {
        let line = match init_session() {
            Some(session) => {
                session.settled.store(true, Ordering::Release);
                session.failed_line(code, &message, data)
            }
            None => {
                let mut error = json!({ "code": code, "message": message });
                if let Some(data) = data {
                    error["data"] = data;
                }
                json!({ "jsonrpc": "2.0", "id": request_id, "error": error })
            }
        };
        write_stdout_line_now(&line);
        std::process::exit(1);
    }

    // Startup progress for an SDK that opted in to accepted-then-settled.
    let init_progress = InitProgress;
    // The callback transport is up and the cheap pre-durable gates above
    // passed: answer an opted-in SDK now. Every later outcome, including the
    // refusals below, arrives as `mobkit/init_settled`.
    if let Some(session) = init_session() {
        session.accept();
    }

    if (has_continuity_store || has_lease_provider || scratch_dir.is_some())
        && !(has_continuity_store && has_lease_provider && scratch_dir.is_some())
    {
        let mut missing = Vec::new();
        if !has_continuity_store {
            missing.push("continuity_store");
        }
        if !has_lease_provider {
            missing.push("lease_provider");
        }
        if scratch_dir.is_none() {
            missing.push("scratch_dir");
        }
        fail_init(
            &request_id,
            -32602,
            format!(
                "external-authoritative path requires continuity_store + lease_provider + scratch_dir; missing: {}",
                missing.join(", ")
            ),
        );
    }

    // A top-level `http_listen` / `allow_remote` / `http_public_base_url` /
    // `meerkat_config_path` is mobkit_gateway's shape; here they live inside
    // runtime_options and a misplaced one must not be dropped.
    refuse_misplaced_runtime_options(&params)
        .unwrap_or_else(|error| fail_init(&request_id, -32602, error));
    let mut gateway_options = parse_gateway_runtime_options(&params, persistent_state.as_deref())
        .unwrap_or_else(|e| {
            fail_init(&request_id, -32602, e);
        });
    // HTTP listener exposure gate, before any bootstrap work: a refused
    // non-loopback `http_listen` costs the init reply and nothing else.
    http_bind_gate(&gateway_options)
        .unwrap_or_else(|error| fail_init(&request_id, -32602, error.to_string()));
    // Bind the HTTP listener right behind the gate, still before any
    // bootstrap work. `http_listen` makes a fixed port possible, so
    // EADDRINUSE is a realistic init failure, and failing here costs the init
    // reply and nothing else: no runtime build, no schedule executor lease
    // that a `std::process::exit` would leave held for its full duration.
    // The listener is served once the runtime is up (`http_binding.serve`).
    let http_binding =
        meerkat_mobkit::gateway_composition::GatewayHttpBinding::bind(gateway_options.http_listen)
            .await
            .unwrap_or_else(|error| {
                fail_init(
                    &request_id,
                    -32603,
                    format!(
                        "failed to bind HTTP listener {}: {error}",
                        gateway_options.http_listen
                    ),
                )
            })
            .with_advertised_base_url(gateway_options.http_public_base_url.clone());
    let http_reachable_addr = http_binding.reachable_addr();
    let http_base_url = http_binding.http_base_url();
    let http_public_base_url = http_binding.advertised_base_url().map(str::to_string);
    for origin in [
        Some(http_base_url.as_str()),
        http_public_base_url.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        gateway_options
            .console_ui
            .validate_mcp_apps_sandbox(Some(origin))
            .unwrap_or_else(|error| fail_init(&request_id, -32602, error.to_string()));
    }
    // Provider-annotated profiles pass meerkat's init rule whatever their
    // model string says; say so once per such profile, with the hint, so a
    // dated id does not first appear as a failed LLM call.
    for warning in uncatalogued_provider_annotated_model_warnings(
        &definition,
        gateway_options.host_config.as_ref(),
    ) {
        tracing::warn!("{warning}");
    }
    init_progress.enter(InitPhase::Storage);
    if shutdown_requested.load(Ordering::Acquire) {
        abort_init_for_shutdown(None, &mut rpc_rx, init_progress).await;
    }
    // The detached callback-job store is the first thing that touches the
    // persistent state directory; it opens AFTER the listener is bound so a
    // refused or failed bind leaves nothing on disk (the taken-port test
    // asserts the directory is absent).
    let callback_job_store: Option<Arc<dyn meerkat::DetachedJobStore>> =
        persistent_state.as_ref().map(|state_path| {
            let path = meerkat_store::realm_paths_in(
                state_path,
                meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
            )
            .jobs_sqlite_path;
            if let Some(parent) = path.parent()
                && let Err(error) = std::fs::create_dir_all(parent)
            {
                fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    format!(
                        "failed to create detached job store directory {}: {error}",
                        parent.display()
                    ),
                );
            }
            match meerkat::SqliteDetachedJobStore::open(path.clone()) {
                Ok(store) => Arc::new(store) as Arc<dyn meerkat::DetachedJobStore>,
                Err(error) => fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    meerkat_mobkit::storage_health::JobStoreResolutionError {
                        path,
                        message: error.to_string(),
                    }
                    .to_string(),
                ),
            }
        });
    validate_gateway_identity_bootstrap_intent(
        gateway_options.identity_bootstrap_mode.as_ref(),
        has_roster_provider,
    )
    .unwrap_or_else(|error| fail_init(&request_id, -32602, error));
    if gateway_options.agent_memory.is_some() && !has_roster_provider {
        fail_init(
            &request_id,
            -32602,
            "runtime_options.agent_memory requires an identity-first roster provider".to_string(),
        );
    }
    // Member role migrations this boot is authorized to perform, e.g.
    // `[{"identity": "domain:automation", "from_role": "domain"}]`.
    //
    // A malformed declaration refuses the boot rather than arming nothing. An
    // operator who wrote a declaration expects it to be in force, and silently
    // dropping it would resurface as the original
    // `MemberRoleMigrationRequired` refusal with nothing pointing at the
    // payload as the cause.
    let role_migration_declarations: Vec<meerkat_mobkit::identity_first::RoleMigrationDeclaration> =
        params
            .get("role_migrations")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()
            .unwrap_or_else(|error| {
                fail_init(
                    &request_id,
                    -32602,
                    format!("role_migrations is malformed: {error}"),
                )
            })
            .unwrap_or_default();
    if let Some((identity, first, second)) =
        meerkat_mobkit::identity_first::conflicting_role_migration_declaration(
            &role_migration_declarations,
        )
    {
        fail_init(
            &request_id,
            -32602,
            format!(
                "role_migrations declares '{identity}' twice with different predecessor roles \
                 ('{first}' and '{second}'); migration authority cannot be resolved by order"
            ),
        );
    }
    // Compiled application tool policies this boot serves, each the EXACT
    // canonical JSON bytes of one compiled policy:
    // `["{\"schema_version\":1,...}\n", ...]`.
    //
    // They arrive as strings rather than nested objects on purpose. Parsing
    // verifies the digest against the bytes it was given, so re-serialising a
    // nested object would check a digest against bytes MobKit just
    // manufactured instead of the ones the operator compiled. A malformed or
    // digest-mismatched payload refuses the boot, for the same reason
    // `role_migrations` does: an operator who supplied a policy expects it in
    // force, and arming nothing would resurface later as an unexplained
    // access denial.
    let compiled_tool_policies =
        meerkat_mobkit::member_tool_policy::compiled_policy_payloads_from_init_params(&params)
            .unwrap_or_else(|error| fail_init(&request_id, -32602, format!("{error}")));
    // The application tool policy for child mob members (agent `mob_create`,
    // delegate's implicit mob). With a registry installed, meerkat refuses
    // agent `mob_create` and delegate until one is chosen, possibly the
    // explicit `{"kind":"unmanaged"}` opt-out. A binding nothing here could
    // resolve refuses the boot below rather than every later delegate.
    let child_application_tool_policy =
        meerkat_mobkit::member_tool_policy::child_application_tool_policy_from_init_params(&params)
            .unwrap_or_else(|error| fail_init(&request_id, -32602, format!("{error}")));
    let tool_consequence_policy_registry = if compiled_tool_policies.is_empty() {
        if let Some(binding) = &child_application_tool_policy {
            meerkat_mobkit::member_tool_policy::validate_child_application_tool_policy(
                binding,
                &[],
            )
            .unwrap_or_else(|error| fail_init(&request_id, -32602, format!("{error}")));
        }
        None
    } else {
        // One provider per provider id CARRIED by the artifacts. The gateway
        // deliberately names none: see providers_from_canonical_payloads.
        let providers = meerkat_mobkit::member_tool_policy::providers_from_canonical_payloads(
            &compiled_tool_policies,
        )
        .unwrap_or_else(|error| {
            fail_init(
                &request_id,
                -32602,
                format!("application_tool_policies entry rejected: {error}"),
            )
        });
        let providers: Vec<std::sync::Arc<dyn meerkat_core::ToolConsequenceNarrowingPolicy>> =
            providers
                .into_iter()
                .map(|provider| {
                    provider as std::sync::Arc<dyn meerkat_core::ToolConsequenceNarrowingPolicy>
                })
                .collect();
        if let Some(binding) = &child_application_tool_policy {
            meerkat_mobkit::member_tool_policy::validate_child_application_tool_policy(
                binding, &providers,
            )
            .unwrap_or_else(|error| fail_init(&request_id, -32602, format!("{error}")));
        }
        let registry = meerkat_core::ToolConsequencePolicyRegistry::new(
            providers,
            meerkat_core::PolicyEvaluationSupervisorConfig::default(),
            None,
        )
        .unwrap_or_else(|error| {
            fail_init(
                &request_id,
                -32603,
                format!("application tool policy registry could not be built: {error}"),
            )
        });
        Some(std::sync::Arc::new(registry))
    };
    let default_llm_client: Option<Arc<dyn meerkat_client::LlmClient>> =
        match gateway_options.demo_llm {
            true => {
                let client: Arc<dyn meerkat_client::LlmClient> =
                    Arc::new(meerkat_client::TestClient::default());
                Some(client)
            }
            false => None,
        };
    // The bundled local lease provider's monotonic fencing counter must resume
    // ABOVE the persisted high-water on restart. Otherwise it resets to 1 and
    // restore presents a stale token that the store's compare-and-set rejects
    // ("stale fencing token: presented 1, current N"), aborting boot on every
    // restart with existing history. Capture the local store's high-water here
    // and seed the lease provider with it below.
    let mut local_default_lease_provider: Option<
        Arc<dyn meerkat_mobkit::identity_first::contracts::LeaseProvider>,
    > = None;
    let identity_continuity_store: Option<
        Arc<dyn meerkat_mobkit::identity_first::ContinuityStore>,
    > = if has_roster_provider {
        Some(if has_continuity_store {
            Arc::new(meerkat_mobkit::identity_first::GatewayContinuityStore::new(
                bridge.clone(),
            ))
        } else {
            let db_path = match storage_layout.continuity_db() {
                Ok(resolved) => resolved.path,
                Err(e) => fail_init(&request_id, STORAGE_RESOLUTION_CODE, e.to_string()),
            };
            if storage_layout.is_declared_ephemeral()
                && let Err(e) = std::fs::create_dir_all(storage_layout.state_dir())
            {
                fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    format!("failed to create the declared-ephemeral scratch root: {e}"),
                );
            }
            let substrate = meerkat_mobkit::gateway_wiring::open_identity_substrate(&db_path)
                .await
                .unwrap_or_else(|e| fail_init(&request_id, STORAGE_RESOLUTION_CODE, e));
            local_default_lease_provider = Some(substrate.lease_provider);
            substrate.continuity_store
        })
    } else {
        None
    };
    let identity_lease_provider: Option<
        Arc<dyn meerkat_mobkit::identity_first::contracts::LeaseProvider>,
    > = if has_roster_provider {
        Some(if has_lease_provider {
            Arc::new(meerkat_mobkit::identity_first::GatewayLeaseProvider::new(
                bridge.clone(),
            )) as Arc<dyn meerkat_mobkit::identity_first::contracts::LeaseProvider>
        } else {
            // From the shared substrate: fencing counter resumes above the
            // persisted high-water (see gateway_wiring::open_identity_substrate).
            local_default_lease_provider
                .clone()
                .expect("local substrate initialized with the continuity store")
        })
    } else {
        None
    };
    let identity_session_store_adapter = identity_continuity_store.as_ref().map(|store| {
        Arc::new(meerkat_mobkit::identity_first::ContinuitySessionStoreAdapter::new(store.clone()))
    });

    init_progress.enter(InitPhase::OwnerPublication);
    if shutdown_requested.load(Ordering::Acquire) {
        abort_init_for_shutdown(None, &mut rpc_rx, init_progress).await;
    }
    // Publish persisted owner authority BEFORE the bootstrap spec is built.
    //
    // `MobRuntime::prepare` runs durable-tail recovery and commits runtime
    // boundaries, and a head-canonical session whose owning identity has no
    // authoritative registration is refused there BY DESIGN ("a deliberate
    // refusal, not a store failure: no retry of the write will succeed").
    // `register_persisted_continuity_owners` runs AFTER prepare, so it cannot
    // reach exactly the stores that need it: a resume whose durable tail is
    // behind its committed authority. Confirmed against a production clone,
    // whose first boot dies precisely there.
    //
    // Best-effort and idempotent, deliberately: this only ever publishes a
    // registration that would otherwise happen later, and `register_session`
    // refuses an identity/generation change or a fencing regression, so the
    // later pass re-publishing the same facts is a no-op. A substrate that
    // cannot answer is therefore a WARN, not a boot failure - the same rule
    // `register_persisted_continuity_owners` already applies to itself.
    // Failing closed here would regress every boot that works today.
    if let (Some(store), Some(adapter)) = (
        identity_continuity_store.as_ref(),
        identity_session_store_adapter.as_ref(),
    ) {
        use meerkat_mobkit::identity_first::contracts::RosterProvider as _;
        use meerkat_mobkit::identity_first::{
            IdentityRuntime, RosterContext, gateway_bridges::GatewayRosterProvider,
        };
        let roster_provider = GatewayRosterProvider::new(bridge.clone());
        // `RosterContext` carries no mob state, so resolving the roster this
        // early is contract-preserving. Nothing is registered yet, so there are
        // no previous identities to report.
        let roster_context = RosterContext {
            mob_definition: Some(definition.clone()),
            previous_identities: Vec::new(),
        };
        match roster_provider.roster(&roster_context).await {
            Ok(roster) => {
                match IdentityRuntime::resolve_persisted_owner_states(store, &roster).await {
                    Ok(states) => {
                        if let Err(error) =
                            IdentityRuntime::publish_persisted_owner_states(adapter, &states).await
                        {
                            tracing::warn!(
                                %error,
                                resolved = states.len(),
                                "publishing persisted owner authority before prepare failed; \
                                 durable-tail recovery may refuse an unregistered owner"
                            );
                        }
                    }
                    Err(error) => tracing::warn!(
                        %error,
                        rostered = roster.len(),
                        "resolving persisted owner authority before prepare failed; \
                         durable-tail recovery may refuse an unregistered owner"
                    ),
                }
            }
            Err(error) => tracing::warn!(
                %error,
                "roster unavailable before prepare, so no persisted owner authority could be \
                 published; durable-tail recovery may refuse an unregistered owner"
            ),
        }
    }

    // 5. Build session service with callback bridge.
    // Stable owner id for the schedule driver, captured before `definition` is
    // moved into the bootstrap spec.
    let schedule_owner_id = definition.id.to_string();
    // Operator-verb seam: the CONCRETE persistent session service the mob
    // bootstraps with, reached through the transcript extension traits. The
    // erased Arc<dyn MobSessionService> cannot recover this (no trait
    // upcasting), and it must be THIS instance - a second service over the
    // same stores has its own live registry, so its Busy check would not see
    // this gateway's running members. Ephemeral lanes stay None.
    let mut gateway_transcript_edit_service: Option<
        Arc<dyn meerkat_mobkit::memory::hygienist::TranscriptEditSessionService>,
    > = None;
    let (
        mob_spec,
        _temp_dir,
        schedule_host_inputs,
        workgraph_service,
        live_inputs,
        gateway_detached_jobs,
    ) = if let Some(ref state_path) = persistent_state {
        if let Err(e) = std::fs::create_dir_all(state_path) {
            fail_init(
                &request_id,
                STORAGE_RESOLUTION_CODE,
                format!("failed to create persistent state directory: {e}"),
            );
        }
        let sqlite_path = match storage_layout.session_db() {
            Ok(resolved) => resolved.path,
            Err(e) => fail_init(&request_id, STORAGE_RESOLUTION_CODE, e.to_string()),
        };
        let session_store_kind = if identity_session_store_adapter.is_some() {
            "ContinuitySessionStoreAdapter"
        } else {
            "SqliteSessionStore"
        };
        let session_store: Arc<dyn meerkat::SessionStore> =
            if let Some(adapter) = identity_session_store_adapter.clone() {
                adapter
            } else {
                match meerkat_store::SqliteSessionStore::open(sqlite_path) {
                    Ok(s) => Arc::new(s),
                    Err(e) => fail_init(
                        &request_id,
                        STORAGE_RESOLUTION_CODE,
                        format!("failed to open SQLite session store: {e}"),
                    ),
                }
            };
        // H2: probe the incremental capability on the same Arc the session
        // service receives below — identity-first launches ride the
        // continuity adapter, which persists whole-blob only.
        let session_store_incremental =
            meerkat_mobkit::storage_health::probe_session_store_incremental(
                &session_store,
                session_store_kind,
            );
        // This is the persistent launch branch: the runtime store 25 lines
        // below is fail-closed persistent for exactly this reason ("must be
        // persistent so resume works across gateway restart"), and mob storage
        // was never given the same treatment. In-memory here means every
        // adopted identity declaration is gone on restart while sessions
        // survive.
        let (mob_storage, mob_storage_provenance) = if gateway_options.mob_storage_ephemeral {
            // Declared, not silent. This launch keeps mob state in memory even
            // though it has a persistent state dir, which is the only way to
            // keep editing mob_config across restarts: a persistent mob
            // storage pins the definition.
            if gateway_options.declare_spec_update.is_some() {
                // Nothing is pinned on in-memory mob storage, so there is no
                // spec to move. Silently ignoring the declaration would let an
                // activation log "spec updated" against a store that has none.
                fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    "runtime_options.declare_spec_update was supplied alongside an in-memory \
                     mob_storage declaration; in-memory mob storage pins no definition, so \
                     there is no persisted spec to declare an update against"
                        .to_string(),
                );
            }
            (
                MobStorage::in_memory(),
                meerkat_mobkit::mob_composition_manifest::MobStorageProvenance::declared_ephemeral(
                ),
            )
        } else {
            let pair = match meerkat_mobkit::mob_composition_manifest::persistent_mob_storage(
                storage_layout.event_log_db(),
            ) {
                Ok(pair) => pair,
                Err(err) => fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    format!(
                        "failed to open the persistent mob storage at {}: {err} (adopted \
                         identity declarations and mob events would not survive gateway \
                         restart)",
                        storage_layout.event_log_db().display()
                    ),
                ),
            };
            // The door through the pinned-mob_config refusal, taken only when
            // THIS activation declares it. Runs before the mob is built and
            // advances Meerkat's canonical definition epoch through the same
            // MobStorage instance that will resume below.
            if let Some(expected_revision) = gateway_options.declare_spec_update {
                match meerkat_mobkit::spec_update_ceremony::declare_spec_update(
                    &pair.0,
                    definition.id.as_str(),
                    &definition,
                    expected_revision,
                )
                .await
                {
                    Ok(receipt) => {
                        // Evidence, at warn: moving a pinned spec is a declared
                        // operator transition and should be legible in an
                        // activation log without raising the log level to find it.
                        tracing::warn!(
                            mob_id = %receipt.mob_id,
                            previous_revision = receipt.previous_revision,
                            committed_revision = receipt.committed_revision,
                            projection_revision = receipt.projection_revision,
                            event_cursor = receipt.event_cursor,
                            declared_fields = ?receipt.declared_fields,
                            "operator-declared mob spec update committed"
                        );
                    }
                    Err(err) => fail_init(
                        &request_id,
                        STORAGE_RESOLUTION_CODE,
                        format!("declared mob spec update refused: {err}"),
                    ),
                }
            }
            pair
        };
        let binary_blob_store: Arc<dyn BinaryBlobStore> =
            match ObjectStoreBlobStore::local(storage_layout.blob_root()) {
                Ok(store) => Arc::new(store),
                Err(e) => fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    format!("failed to open binary blob store: {e}"),
                ),
            };
        let blob_store: Arc<dyn meerkat_core::BlobStore> =
            Arc::new(Base64BlobStoreAdapter::new(binary_blob_store.clone()));
        // Persistent runtime store — must be Some() on the session service
        // so archive/retire can mutate the authoritative session, and must
        // be persistent so resume works across gateway restart.
        //
        // Fail-closed (M4): an open failure is an init error; the in-memory
        // form exists only as the explicit
        // `runtime_options.runtime_store = {"storage": "memory"}` declaration
        // (the former silent fallback left resume and archive broken long
        // after boot).
        let runtime_db_path = storage_layout.runtime_db();
        // The typed source of the repair commands in a session repair hold:
        // the exact database this launch opens, or none for the declared
        // in-memory store.
        let runtime_store_locator = (!gateway_options.runtime_store_ephemeral).then(|| {
            meerkat_mobkit::storage_health::RuntimeStoreLocator::SqliteFile {
                path: runtime_db_path.clone(),
            }
        });
        let (runtime_store, runtime_store_slot): (
            Arc<dyn meerkat_runtime::RuntimeStore>,
            meerkat_mobkit::storage_health::StorageSlotSummary,
        ) = if gateway_options.runtime_store_ephemeral {
            (
                Arc::new(meerkat_runtime::InMemoryRuntimeStore::new()),
                meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                    "runtime",
                    "InMemoryRuntimeStore",
                    "explicitly declared via runtime_options.runtime_store: sessions do not \
                     survive gateway restart",
                ),
            )
        } else {
            match meerkat_runtime::store::SqliteRuntimeStore::new(&runtime_db_path) {
                Ok(store) => (
                    Arc::new(store) as Arc<dyn meerkat_runtime::RuntimeStore>,
                    meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                        "runtime",
                        "SqliteRuntimeStore",
                    ),
                ),
                Err(err) => fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    meerkat_mobkit::storage_health::RuntimeStoreResolutionError {
                        path: runtime_db_path.clone(),
                        message: err.to_string(),
                    }
                    .to_string(),
                ),
            }
        };
        // Wrap the runtime store in the write-epoch facade BEFORE the machine
        // and session service capture it, and thread the witness into the
        // bootstrap spec below. Without it `MobBootstrapSpec::new` leaves the
        // witness absent and the console session-history epoch gate is
        // disabled — the 5s discovery loop then re-reads and re-validates
        // every member's whole session document forever (~0.3 core per idle
        // durable member at production document sizes; the 0.8.4 idle
        // driver on this externally-composed path).
        //
        // The facade also owns the durable session projection at meerkat
        // 0.8.11 (the session service no longer writes the SessionStore
        // itself) and every-boot runtime-authority re-minting from that
        // durable store - a reset/lost runtime store reseeds instead of
        // refusing resume.
        let (runtime_store, session_write_epochs) =
            meerkat_mobkit::mob_handle_runtime::epoch_tracking_runtime_store_with_durable_projection(
                runtime_store,
                session_store.clone(),
            );
        let adapter = Arc::new(
            meerkat_runtime::MeerkatMachine::persistent(
                Arc::clone(&runtime_store),
                Arc::clone(&blob_store),
            )
            .unwrap_or_else(|error| {
                fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    format!("failed to acquire the gateway runtime machine: {error}"),
                )
            }),
        );
        // Match the ephemeral path's capability mask — only comms is enabled
        // by default. Apps control additional capabilities via their mob
        // definition profiles, not the gateway factory.
        let mut factory = AgentFactory::new(state_path)
            .builtins(false)
            .shell(shell)
            .comms(true)
            .mcp_apps(gateway_options.console_ui.mcp_apps_sandbox_url.is_some());
        if image_generation {
            factory = factory.with_image_generation_machine(adapter.clone());
        }
        // Exclusive session-identity root, when the launch declares one. See
        // `GatewayRuntimeOptions::session_identity_config_root`.
        if let Some(root) = gateway_options.session_identity_config_root.as_ref() {
            factory = factory.user_config_root(root.clone());
        }
        // Live (realtime) needs the SAME factory shape for its per-open
        // credential-resolving realtime session factory; clone before
        // the builder consumes it.
        #[cfg(feature = "experimental-gpt-live")]
        let live_agent_factory =
            if let Some(experimental) = gateway_options.experimental_live.as_ref() {
                let operator =
                gateway_experimental_live_operator(experimental).unwrap_or_else(|error| {
                    fail_init(
                        &request_id,
                        -32602,
                        format!(
                            "runtime_options.experimental_live execution profile failed: {error}"
                        ),
                    )
                });
                factory
                    .clone()
                    .with_experimental_live_admission(operator, [experimental.realm.clone()])
            } else {
                factory.clone()
            };
        #[cfg(not(feature = "experimental-gpt-live"))]
        let live_agent_factory = factory.clone();
        let live_machine = Arc::clone(&adapter);
        let mut inner_builder =
            FactoryAgentBuilder::new(factory, gateway_agent_config(&gateway_options));
        inner_builder.default_session_store = Some(Arc::new(meerkat_store::StoreAdapter::new(
            session_store.clone(),
        )));
        inner_builder.default_blob_store = Some(blob_store.clone());
        inner_builder.default_detached_job_store = callback_job_store.clone();
        // One inbox for every delivery consumer in this gateway, so each sees
        // the others' commits ([`GatewayRuntimeDelivery`]).
        let runtime_delivery = GatewayRuntimeDelivery::new(
            Arc::clone(&runtime_store),
            callback_job_store.clone().unwrap_or_else(|| {
                fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    "persistent launch did not resolve its detached job store".to_string(),
                )
            }),
        );
        if let Some(job_store) = callback_job_store.as_ref() {
            // meerkat 0.8.22 (F4): the projector slot is a Clone value, not a
            // shared Arc - per-session builds rebind its realm authority via
            // bound_to_realm so mob members project under mob.<mob_id>, never
            // a service-lifetime realm frozen at gateway construction.
            inner_builder.default_shell_job_delivery_projector =
                Some(runtime_delivery.shell_job_projector(Arc::clone(job_store)));
        }
        // Attach meerkat's per-session schedule tools so SDK-hosted members whose
        // profile sets tools.schedule=true get the meerkat_schedule_* surface (the
        // slot lives on the inner FactoryAgentBuilder and propagates through the
        // callback wrapper). The returned service backs the firing host spawned
        // after the runtime boots — meerkat's runtime-backed host is now generic
        // over the session builder, so scheduled sessions materialize through the
        // SDK build callback and keep their identity-scoped tools.
        let (schedule_tools, schedule_slot) =
            match meerkat_mobkit::schedule_wiring::attach_schedule_tools_with_identity_targets_reporting(
                &inner_builder,
                storage_layout.state_dir(),
            ) {
                Ok(tools) => (
                    Some(tools),
                    meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                        "schedule",
                        "SqliteScheduleStore",
                    ),
                ),
                Err(error) => (
                    None,
                    // Sanctioned boot-without degradation: schedule tools are
                    // disabled and the fact is health-visible, not a warn
                    // line alone (M4).
                    meerkat_mobkit::storage_health::StorageSlotSummary::degraded(
                        "schedule",
                        format!("schedule store failed to open; schedule tools disabled: {error}"),
                    ),
                ),
            };
        // WorkGraph: durable store beside the schedule store (or in the
        // explicitly configured directory), realm scoped to the mob
        // definition id. Fills the member tool slot, threads to the
        // bootstrap spec (mob-executor attention overlays + child-mob
        // inheritance), the schedule host, and the RPC surface. The
        // state dir travels along for the cross-process admission
        // sidecar (a durable store is shareable across processes).
        let (workgraph, workgraph_slot) = match &gateway_options.workgraph {
            GatewayWorkgraphOption::Disabled => (
                None,
                meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                    "workgraph",
                    "disabled",
                    "explicitly disabled via runtime_options.workgraph = false",
                ),
            ),
            GatewayWorkgraphOption::Enabled => {
                match meerkat_mobkit::workgraph_wiring::attach_workgraph_tools_reporting(
                    &inner_builder,
                    storage_layout.state_dir(),
                    &schedule_owner_id,
                ) {
                    Ok((service, slot)) => (
                        Some((service, slot, storage_layout.state_dir().to_path_buf())),
                        meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                            "workgraph",
                            "SqliteWorkGraphStore",
                        ),
                    ),
                    Err(error) => (
                        None,
                        meerkat_mobkit::storage_health::StorageSlotSummary::degraded(
                            "workgraph",
                            format!("workgraph store failed to open; workgraph disabled: {error}"),
                        ),
                    ),
                }
            }
            GatewayWorkgraphOption::DurableDir(dir) => {
                // An explicit directory overrides the state-dir default.
                // Open failure keeps the boot-without-workgraph posture
                // (health-visible as a degraded slot).
                let _ = std::fs::create_dir_all(dir);
                match meerkat_mobkit::workgraph_wiring::attach_workgraph_tools_reporting(
                    &inner_builder,
                    dir,
                    &schedule_owner_id,
                ) {
                    Ok((service, slot)) => (
                        Some((service, slot, dir.clone())),
                        meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                            "workgraph",
                            "SqliteWorkGraphStore",
                        ),
                    ),
                    Err(error) => (
                        None,
                        meerkat_mobkit::storage_health::StorageSlotSummary::degraded(
                            "workgraph",
                            format!("workgraph store failed to open; workgraph disabled: {error}"),
                        ),
                    ),
                }
            }
        };
        let workgraph_service = workgraph.as_ref().map(|(service, _, _)| service.clone());
        let agent_mob_tools_slot = Arc::clone(&inner_builder.default_mob_tools);
        let detached_jobs = callback_job_store.as_ref().map(|store| {
            DetachedCallbackJobRuntime::new(
                meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
                Arc::clone(store),
                blob_store.clone(),
            )
            .with_runtime_delivery(&runtime_delivery)
            .with_monitor_shell(
                state_path
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or(state_path)
                    .to_path_buf(),
                shell,
            )
        });
        let callback_builder = StdioCallbackAgentBuilder {
            inner: inner_builder,
            bridge: bridge.clone(),
            has_session_builder,
            session_store: Some(session_store.clone()),
            detached_jobs: detached_jobs.clone(),
        };
        // Keep the CONCRETE typed service for the firing host (the runtime-backed
        // host needs PersistentSessionService<StdioCallbackAgentBuilder>, not the
        // erased Arc<dyn MobSessionService> the spec consumes).
        let concrete_service = Arc::new(meerkat_session::PersistentSessionService::new(
            callback_builder,
            gateway_options.max_sessions,
            session_store,
            Arc::clone(&runtime_store),
            blob_store,
        ));
        concrete_service
            .acquire_canonical_runtime_adapter(Some(Arc::clone(&adapter)))
            .unwrap_or_else(|error| {
                fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    format!(
                        "failed to acquire the persistent gateway session runtime owner: {error}"
                    ),
                )
            });
        let schedule_host_inputs = schedule_tools.map(|tools| {
            (
                tools.service,
                tools.mob_target_registry,
                Arc::clone(&concrete_service),
                adapter.clone(),
                storage_layout.schedule_db(),
                tools.firing_host_binding,
            )
        });
        // Live (realtime) transport inputs: the concrete service +
        // machine + factory, captured only on opt-in
        // (`runtime_options.live`). Ephemeral mode cannot serve live —
        // the projection sink and machine authorities are
        // persistent-service seams.
        let live_inputs = if matches!(gateway_options.live, GatewayLiveOption::Enabled { .. })
            || gateway_options.strict_live_registered()
        {
            Some((
                Arc::clone(&concrete_service),
                live_machine,
                live_agent_factory,
            ))
        } else {
            None
        };
        // Heal seam (2026-07-29 incident): the CONCRETE persistent service is
        // the committed-boundary recoverer for the identity repair supervisor.
        let committed_boundary_recoverer: Arc<
            dyn meerkat_mobkit::identity_first::CommittedBoundaryRecoverer,
        > = Arc::clone(&concrete_service) as _;
        gateway_transcript_edit_service = Some(Arc::clone(&concrete_service) as _);
        let session_service: Arc<dyn meerkat_mob::MobSessionService> = concrete_service;
        let mut spec = MobBootstrapSpec::new(definition, mob_storage, session_service)
            .with_mob_storage_provenance(mob_storage_provenance)
            // A parsed option that never reaches the spec is the same defect as
            // an unreachable API, one layer down.
            .with_composition_authority(gateway_options.composition_authority)
            .with_candidate_definition(gateway_options.candidate_definition)
            .with_optional_tool_consequence_policy_registry(
                tool_consequence_policy_registry.clone(),
            )
            .with_optional_child_application_tool_policy(child_application_tool_policy.clone())
            .with_session_write_epochs(&session_write_epochs)
            // The SAME facade handed to MeerkatMachine::persistent above.
            // The durable-authority convergence memo is per-facade, so a
            // different instance here would warm nothing.
            .with_runtime_authority_prewarm(&runtime_store)
            // Resume-seam reads must carry the runtime store's archived
            // terminal (at 0.8.11 archive stamps the catalog/lifecycle row,
            // never the session body).
            .with_runtime_archived_terminal_authority(Arc::clone(&runtime_store))
            .with_session_runtime_adapter(adapter.clone())
            .unwrap_or_else(|error| {
                fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    format!("failed to acquire the gateway session runtime owner: {error}"),
                )
            })
            // Order matters: workgraph before agent mob tools so child mobs
            // inherit the service at mob-state install time.
            .with_workgraph_service(workgraph_service.clone())
            // Agent mob tools + the schedule host's mob authority (a downstream app
            // 0.7.26 last-link fix): without this, agent-authored schedules
            // can't rewrite to mob-member targets or deliver them.
            .with_runtime_delivery(runtime_delivery.composition.clone())
            .with_agent_mob_tools(agent_mob_tools_slot)
            .unwrap_or_else(|error| {
                fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    format!("failed to install gateway agent mob tools: {error}"),
                )
            })
            .with_options(MobBootstrapOptions {
                allow_ephemeral_sessions: true,
                notify_orchestrator_on_resume: true,
                default_llm_client: default_llm_client.clone(),
            });
        spec.committed_boundary_recoverer = Some(committed_boundary_recoverer);
        if let Some((_, admission_slot, workgraph_state_dir)) = &workgraph {
            // Durable (cross-process shareable) store: register the
            // tool-plane admission slot and the sidecar lock beside it.
            spec = spec
                .with_workgraph_admission_slot(admission_slot.clone())
                .with_workgraph_admission_sidecar(workgraph_state_dir);
        }
        spec.runtime_adapter = Some(adapter);
        spec.binary_blob_store = Some(binary_blob_store);
        // Blob slot resolved fail-closed above (local disk under
        // <state_path>/blobs); record it plus the H2 probe result and the
        // per-slot census (M4) for the health surfaces.
        let mut slots = vec![
            if identity_session_store_adapter.is_some() {
                meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                    "sessions",
                    "ContinuitySessionStoreAdapter",
                )
                .with_detail("sessions ride the identity continuity store")
            } else {
                meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                    "sessions",
                    "SqliteSessionStore",
                )
            },
            runtime_store_slot,
            meerkat_mobkit::storage_health::blob_slot_summary(
                meerkat_mobkit::storage_health::BlobDurability::PersistentDisk,
            ),
            meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                "console",
                "SqliteConsoleLogStore",
            ),
            meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                "metadata",
                "SqliteMetadataStore",
            ),
            schedule_slot,
            workgraph_slot,
            meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                "jobs",
                "SqliteDetachedJobStore",
            ),
            gateway_event_log_slot(&gateway_options),
        ];
        // Configured agent memory is a durable disk-backed slot under the
        // state dir; the census must cover it like every other store.
        if let Some(agent_memory) = gateway_options.agent_memory.as_ref() {
            slots.push(agent_memory_census_slot(agent_memory));
        }
        if identity_continuity_store.is_some() {
            slots.push(if has_continuity_store {
                meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                    "continuity",
                    "GatewayContinuityStore (SDK-hosted)",
                )
                .with_detail("durability rides with the SDK-hosted continuity store")
            } else {
                meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                    "continuity",
                    "LocalContinuityStore",
                )
            });
        }
        slots.extend(meerkat_mobkit::storage_health::scratch_ring_buffer_slots());
        // The M4 census had no mob slot at all, which is why an in-memory mob
        // storage on this persistent launch was invisible to healthz and the
        // storage doctor while sessions correctly reported persistent. It now
        // reports whichever this launch actually composed.
        slots.push(if gateway_options.mob_storage_ephemeral {
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "mob",
                "MobStorage(in-memory)",
                "declared via runtime_options.mob_storage: mob events and adopted identity \
                 declarations do not survive gateway restart, in exchange for a mob_config \
                 that can still be edited across restarts",
            )
        } else {
            meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                "mob",
                "MobStorage(sqlite)",
            )
        });
        spec.resolved_storage = Some(
            meerkat_mobkit::storage_health::ResolvedStorageSummary::new(
                meerkat_mobkit::storage_health::BlobDurability::PersistentDisk,
                Some(session_store_incremental),
            )
            .with_state_dir(storage_layout.state_dir())
            .with_slots(slots)
            .with_runtime_store_locator(runtime_store_locator),
        );
        (
            spec,
            None,
            schedule_host_inputs,
            workgraph_service,
            live_inputs,
            detached_jobs,
        )
    } else {
        // Process-local mode keeps sessions and runtime state in memory.
        // The supported persistent service/machine pair does not require
        // durable session files on read-only container filesystems.
        let temp_dir = if scratch_dir.is_none() {
            Some(tempfile::tempdir().expect("create temp dir for agent working space"))
        } else {
            None
        };
        let agent_workspace = scratch_dir
            .as_deref()
            .or_else(|| temp_dir.as_ref().map(|dir| dir.path()))
            .expect("scratch dir or temp dir");
        if let Err(err) = std::fs::create_dir_all(agent_workspace) {
            fail_init(
                &request_id,
                STORAGE_RESOLUTION_CODE,
                format!("failed to create scratch directory: {err}"),
            );
        }
        let binary_blob_store: Arc<dyn BinaryBlobStore> = Arc::new(ObjectStoreBlobStore::memory());
        let blob_store: Arc<dyn meerkat_core::BlobStore> =
            Arc::new(Base64BlobStoreAdapter::new(binary_blob_store.clone()));
        let runtime_store: Arc<dyn meerkat_runtime::RuntimeStore> =
            Arc::new(meerkat_runtime::InMemoryRuntimeStore::new());
        // Same write-epoch facade as the persistent path: the console
        // discovery gate is composition-independent, and the witness is
        // sound for any store as long as every write goes through it.
        let (runtime_store, session_write_epochs) =
            meerkat_mobkit::mob_handle_runtime::epoch_tracking_runtime_store(runtime_store);
        let adapter = Arc::new(
            meerkat_runtime::MeerkatMachine::persistent(
                Arc::clone(&runtime_store),
                Arc::clone(&blob_store),
            )
            .unwrap_or_else(|error| {
                fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    format!("failed to acquire the gateway runtime machine: {error}"),
                )
            }),
        );
        let process_local_session_store: Arc<dyn meerkat::SessionStore> =
            Arc::new(meerkat::MemoryStore::new());
        let job_store: Arc<dyn meerkat::DetachedJobStore> =
            Arc::new(meerkat::MemoryDetachedJobStore::new());
        let runtime_delivery =
            GatewayRuntimeDelivery::new(Arc::clone(&runtime_store), Arc::clone(&job_store));
        let agent_mob_tools_slot = Arc::new(std::sync::RwLock::new(None));
        let mut factory = AgentFactory::new(agent_workspace)
            .builtins(false)
            .shell(shell)
            .comms(true)
            .mcp_apps(gateway_options.console_ui.mcp_apps_sandbox_url.is_some())
            .session_store(process_local_session_store.clone());
        if image_generation {
            factory = factory.with_image_generation_machine(adapter.clone());
        }
        // Exclusive session-identity root, when the launch declares one. See
        // `GatewayRuntimeOptions::session_identity_config_root`.
        if let Some(root) = gateway_options.session_identity_config_root.as_ref() {
            factory = factory.user_config_root(root.clone());
        }
        let mut inner_builder =
            FactoryAgentBuilder::new(factory, gateway_agent_config(&gateway_options));
        inner_builder.default_session_store = Some(Arc::new(meerkat_store::StoreAdapter::new(
            process_local_session_store.clone(),
        )));
        inner_builder.default_blob_store = Some(blob_store.clone());
        inner_builder.default_detached_job_store = Some(Arc::clone(&job_store));
        inner_builder.default_mob_tools = Arc::clone(&agent_mob_tools_slot);
        inner_builder.default_shell_job_delivery_projector =
            Some(runtime_delivery.shell_job_projector(Arc::clone(&job_store)));
        // No-persistent_state launches default to a memory-backed
        // workgraph (tools stay profile-gated, so nothing changes for
        // members that do not opt in); an explicit directory gets the
        // durable store instead — and, being cross-process shareable, a
        // cross-process admission sidecar beside it.
        let mut workgraph_sidecar_dir: Option<PathBuf> = None;
        let (workgraph_service, ephemeral_workgraph_slot) = match &gateway_options.workgraph {
            GatewayWorkgraphOption::Disabled => (
                None,
                meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                    "workgraph",
                    "disabled",
                    "explicitly disabled via runtime_options.workgraph = false",
                ),
            ),
            GatewayWorkgraphOption::DurableDir(dir) => {
                let _ = std::fs::create_dir_all(dir);
                workgraph_sidecar_dir = Some(dir.clone());
                match meerkat_mobkit::workgraph_wiring::open_workgraph_service_reporting(
                    dir,
                    &schedule_owner_id,
                ) {
                    Ok(service) => (
                        Some(service),
                        meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                            "workgraph",
                            "SqliteWorkGraphStore",
                        ),
                    ),
                    Err(error) => (
                        None,
                        meerkat_mobkit::storage_health::StorageSlotSummary::degraded(
                            "workgraph",
                            format!("workgraph store failed to open; workgraph disabled: {error}"),
                        ),
                    ),
                }
            }
            GatewayWorkgraphOption::Enabled => {
                if has_continuity_store {
                    // Identity-first launch persisting through the
                    // SDK-hosted continuity store: everything else about
                    // it is durable, but the continuity store is a stdio
                    // callback (no local db path to co-locate a default
                    // workgraph.sqlite3 with), so the workgraph silently
                    // rides memory unless a path is configured.
                    tracing::warn!(
                        "workgraph is MEMORY-BACKED for this launch: the continuity store \
                             is SDK-hosted (no local path) and no persistent_state dir is set, \
                             so goals, work items and attention bindings will NOT survive a \
                             gateway restart. Set runtime_options.workgraph to a directory \
                             path (or provide persistent_state) to place a durable \
                             workgraph.sqlite3.",
                    );
                }
                (
                    Some(
                        meerkat_mobkit::workgraph_wiring::ephemeral_workgraph_service(
                            &schedule_owner_id,
                        ),
                    ),
                    meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                        "workgraph",
                        "MemoryWorkGraphStore",
                        if has_continuity_store {
                            "memory-backed on an otherwise durable identity-first launch: set \
                             runtime_options.workgraph to a directory (or persistent_state) \
                             for a durable store"
                        } else {
                            "declared by the ephemeral launch mode"
                        },
                    ),
                )
            }
        };
        // One admission slot per builder that carries the tool surface;
        // the bootstrap spec registers them all so every dispatcher gets
        // the runtime-wide admission (round-3 R1).
        let mut workgraph_admission_slots = Vec::new();
        if let Some(service) = workgraph_service.as_ref() {
            workgraph_admission_slots.push(
                meerkat_mobkit::workgraph_wiring::install_workgraph_tools(&inner_builder, service),
            );
        }
        let callback_builder = StdioCallbackAgentBuilder {
            inner: inner_builder,
            bridge: bridge.clone(),
            has_session_builder,
            session_store: Some(process_local_session_store.clone()),
            detached_jobs: None,
        };
        // Heal seam (2026-07-29 incident): captured from the CONCRETE
        // persistent service below; the erased MobSessionService does not
        // carry the heal API.
        let mut committed_boundary_recoverer: Option<
            Arc<dyn meerkat_mobkit::identity_first::CommittedBoundaryRecoverer>,
        > = None;
        let mut session_store_incremental: Option<bool> = None;
        let session_service: Arc<dyn meerkat_mob::MobSessionService> =
            if let Some(session_adapter) = identity_session_store_adapter.clone() {
                let session_store: Arc<dyn meerkat::SessionStore> = session_adapter.clone();
                // H2: identity-first launches persist sessions through the
                // continuity adapter — whole-blob only; make it loud.
                session_store_incremental = Some(
                    meerkat_mobkit::storage_health::probe_session_store_incremental(
                        &session_store,
                        "ContinuitySessionStoreAdapter",
                    ),
                );
                let mut factory = AgentFactory::new(agent_workspace)
                    .builtins(false)
                    .shell(shell)
                    .comms(true)
                    .mcp_apps(gateway_options.console_ui.mcp_apps_sandbox_url.is_some())
                    .session_store(Arc::new(meerkat::MemoryStore::new()));
                if image_generation {
                    factory = factory.with_image_generation_machine(adapter.clone());
                }
                // Exclusive session-identity root, when declared. See
                // `GatewayRuntimeOptions::session_identity_config_root`.
                if let Some(root) = gateway_options.session_identity_config_root.as_ref() {
                    factory = factory.user_config_root(root.clone());
                }
                let mut inner_builder =
                    FactoryAgentBuilder::new(factory, gateway_agent_config(&gateway_options));
                inner_builder.default_session_store = Some(Arc::new(
                    meerkat_store::StoreAdapter::new(session_store.clone()),
                ));
                inner_builder.default_blob_store = Some(blob_store.clone());
                inner_builder.default_detached_job_store = Some(Arc::clone(&job_store));
                inner_builder.default_mob_tools = Arc::clone(&agent_mob_tools_slot);
                inner_builder.default_shell_job_delivery_projector =
                    Some(runtime_delivery.shell_job_projector(Arc::clone(&job_store)));
                if let Some(service) = workgraph_service.as_ref() {
                    workgraph_admission_slots.push(
                        meerkat_mobkit::workgraph_wiring::install_workgraph_tools(
                            &inner_builder,
                            service,
                        ),
                    );
                }
                let callback_builder = StdioCallbackAgentBuilder {
                    inner: inner_builder,
                    bridge: bridge.clone(),
                    has_session_builder,
                    session_store: Some(session_store.clone()),
                    detached_jobs: None,
                };
                let concrete = Arc::new(meerkat_session::PersistentSessionService::new(
                    callback_builder,
                    gateway_options.max_sessions,
                    session_store,
                    Arc::clone(&runtime_store),
                    blob_store.clone(),
                ));
                concrete
                    .acquire_canonical_runtime_adapter(Some(Arc::clone(&adapter)))
                    .unwrap_or_else(|error| {
                        fail_init(
                            &request_id,
                            STORAGE_RESOLUTION_CODE,
                            format!(
                                "failed to acquire the continuity-backed gateway \
                                 session runtime owner: {error}"
                            ),
                        )
                    });
                committed_boundary_recoverer = Some(Arc::clone(&concrete) as _);
                gateway_transcript_edit_service = Some(Arc::clone(&concrete) as _);
                concrete
            } else {
                let concrete = Arc::new(meerkat_session::PersistentSessionService::new(
                    callback_builder,
                    gateway_options.max_sessions,
                    process_local_session_store,
                    Arc::clone(&runtime_store),
                    blob_store.clone(),
                ));
                concrete
                    .acquire_canonical_runtime_adapter(Some(Arc::clone(&adapter)))
                    .unwrap_or_else(|error| {
                        fail_init(
                            &request_id,
                            STORAGE_RESOLUTION_CODE,
                            format!(
                                "failed to acquire the process-local gateway \
                                 session runtime owner: {error}"
                            ),
                        )
                    });
                concrete
            };

        let mut spec = MobBootstrapSpec::new(definition, MobStorage::in_memory(), session_service)
            .with_optional_tool_consequence_policy_registry(
                tool_consequence_policy_registry.clone(),
            )
            .with_optional_child_application_tool_policy(child_application_tool_policy.clone())
            .with_session_write_epochs(&session_write_epochs)
            // The SAME facade handed to MeerkatMachine::persistent above.
            // The durable-authority convergence memo is per-facade, so a
            // different instance here would warm nothing.
            .with_runtime_authority_prewarm(&runtime_store)
            .with_session_runtime_adapter(adapter.clone())
            .unwrap_or_else(|error| {
                fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    format!("failed to acquire the gateway session runtime owner: {error}"),
                )
            })
            .with_workgraph_service(workgraph_service.clone())
            .with_runtime_delivery(runtime_delivery.composition.clone())
            .with_agent_mob_tools(agent_mob_tools_slot)
            .unwrap_or_else(|error| {
                fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    format!("failed to install gateway agent mob tools: {error}"),
                )
            })
            .with_options(MobBootstrapOptions {
                allow_ephemeral_sessions: true,
                notify_orchestrator_on_resume: true,
                default_llm_client: default_llm_client.clone(),
            });
        spec.committed_boundary_recoverer = committed_boundary_recoverer;
        for slot in workgraph_admission_slots {
            spec = spec.with_workgraph_admission_slot(slot);
        }
        if let Some(dir) = workgraph_sidecar_dir.as_deref() {
            spec = spec.with_workgraph_admission_sidecar(dir);
        }
        spec.runtime_adapter = Some(adapter);
        spec.binary_blob_store = Some(binary_blob_store);
        // In-memory blobs are the declared choice of this launch mode; the
        // H2 flag is only set when the identity adapter backs a persistent
        // session service above.
        let mut slots = vec![
            if identity_session_store_adapter.is_some() {
                meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                    "sessions",
                    "ContinuitySessionStoreAdapter",
                )
                .with_detail("sessions ride the identity continuity store")
            } else {
                meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                    "sessions",
                    "MemoryStore",
                    "declared by the ephemeral launch mode",
                )
            },
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "runtime",
                "InMemoryRuntimeStore",
                "declared by the ephemeral launch mode",
            ),
            meerkat_mobkit::storage_health::blob_slot_summary(
                meerkat_mobkit::storage_health::BlobDurability::DeclaredEphemeral,
            ),
            // The ephemeral gateway's console timeline and metadata cursor
            // are in-memory by this surface's contract — a declared default,
            // documented and health-visible (M4), not an error.
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "console",
                "InMemoryConsoleLogStore",
                "declared default of the ephemeral launch mode",
            ),
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "metadata",
                "InMemoryMetadataStore",
                "declared default of the ephemeral launch mode",
            ),
            ephemeral_workgraph_slot,
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "jobs",
                "MemoryDetachedJobStore",
                "declared by the ephemeral launch mode",
            ),
            gateway_event_log_slot(&gateway_options),
        ];
        if identity_continuity_store.is_some() {
            slots.push(if has_continuity_store {
                meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                    "continuity",
                    "GatewayContinuityStore (SDK-hosted)",
                )
                .with_detail("durability rides with the SDK-hosted continuity store")
            } else {
                meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                    "continuity",
                    "LocalContinuityStore",
                    "backed by the declared-ephemeral scratch root",
                )
            });
        }
        slots.extend(meerkat_mobkit::storage_health::scratch_ring_buffer_slots());
        // Declared, not omitted: this launch has no persistent_state dir, so
        // mob state is in memory by design and the census now says so.
        slots.push(
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "mob",
                "MobStorage(in-memory)",
                "no persistent_state directory was supplied: mob events and adopted \
                 identity declarations do not survive gateway restart",
            ),
        );
        spec.resolved_storage = Some(
            meerkat_mobkit::storage_health::ResolvedStorageSummary::new(
                meerkat_mobkit::storage_health::BlobDurability::DeclaredEphemeral,
                session_store_incremental,
            )
            // The declared-ephemeral scratch root is still this runtime's
            // own state dir: a doctor request over it may see the census.
            .with_state_dir(storage_layout.state_dir())
            .with_slots(slots),
        );
        // Ephemeral sessions have no persistent service; firing is persistent-only.
        (spec, temp_dir, None, workgraph_service, None, None)
    };

    // `customize_build` tools follow each identity member across meerkat-side
    // rebuilds (restart restore, adoption, respawn, delivery-time repair)
    // through one stable dispatcher per identity (#563). The registry follows
    // whether a customizer is composed at all: the host's, or the agent-memory
    // customizer that wraps it (and runs without one). Gating it on the host
    // flag alone dropped the memory recorder from members of hosts that
    // enable memory without their own customizer.
    let composes_agent_customizer = has_agent_customizer || gateway_options.agent_memory.is_some();
    let customizer_tool_registry =
        composes_agent_customizer.then(meerkat_mobkit::identity_first::CustomizerToolRegistry::new);
    let mob_spec = match customizer_tool_registry.as_ref() {
        Some(registry) => mob_spec.with_spawn_member_customizer(Arc::new(
            meerkat_mobkit::identity_first::CustomizerToolsSpawnCustomizer::new(registry.clone()),
        )),
        None => mob_spec,
    };

    // Wire callback/after_create — notify Python/TS SDK after each session creation.
    // Uses notify_reliable to avoid silent drops under backpressure.
    let mob_spec = if has_session_builder {
        let after_bridge = bridge.clone();
        mob_spec.with_after_create_hook(Arc::new(
            move |session_id: meerkat_core::types::SessionId, ctx| {
                let b = after_bridge.clone();
                Box::pin(async move {
                    if let Err(error) = b
                        .notify_reliable(
                            "callback/after_create",
                            json!({
                                "session_id": session_id.to_string(),
                                "model": ctx.model,
                                "labels": ctx.labels,
                                "system_prompt": ctx.system_prompt,
                            }),
                        )
                        .await
                    {
                        eprintln!(
                            "[mobkit-gateway] failed to deliver callback/after_create: {error}"
                        );
                    }
                })
            },
        ))
    } else {
        mob_spec
    };

    // §10.1 dispatch-time taint join: keep the spec's late-bound slot so the
    // memory-stack region below can bind the tracker into every member build
    // (bootstrap members included - their decorators read the slot per call).
    let dispatch_taint_slot = mob_spec.dispatch_taint_slot();

    let timeout = GATEWAY_RUNTIME_EVENT_DRAIN_TIMEOUT;
    let persistent_metadata: Arc<dyn PersistentMetadataStore> = if persistent_state.is_some() {
        let metadata_path = match storage_layout.metadata_db() {
            Ok(resolved) => resolved.path,
            Err(e) => fail_init(&request_id, STORAGE_RESOLUTION_CODE, e.to_string()),
        };
        Arc::new(
            SqliteMetadataStore::open(&metadata_path).unwrap_or_else(|e| {
                fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    format!(
                        "failed to open the mobkit metadata store at {}: {e}",
                        metadata_path.display()
                    ),
                );
            }),
        )
    } else {
        Arc::new(InMemoryMetadataStore::new())
    };
    let bootstrap_plan =
        meerkat_mobkit::gateway_composition::GatewayRuntimeBootstrapPlan::stdio_rpc(
            mob_spec,
            module_config,
            Vec::new(),
            timeout,
            gateway_options.runtime_options.clone(),
            persistent_metadata,
        );
    init_progress.enter(InitPhase::Prepare);
    if shutdown_requested.load(Ordering::Acquire) {
        abort_init_for_shutdown(None, &mut rpc_rx, init_progress).await;
    }
    let mut composition = meerkat_mobkit::gateway_composition::GatewayComposition::prepare(
        meerkat_mobkit::gateway_composition::GatewayCompatibilityProfile::StdioRpc,
        bootstrap_plan,
    )
    .bootstrap()
    .await
    .unwrap_or_else(|e| {
        let message = format!("Runtime bootstrap failed: {e}");
        match e.composition_provenance() {
            // A deliberate composition refusal, not an internal error: its own
            // code, with the refusal's kind and diverged fields as data.
            Some(refusal) => fail_init_with_data(
                &request_id,
                COMPOSITION_PROVENANCE_CODE,
                message,
                composition_refusal_data(refusal),
            ),
            None => fail_init(&request_id, -32603, message),
        }
    });
    let runtime = composition.runtime_mut();
    runtime.set_bootstrap_phase_observer(Arc::new(move |phase| {
        init_progress.enter(InitPhase::from(phase));
    }));

    if persistent_state.is_some() {
        let console_log_path = match storage_layout.console_db() {
            Ok(resolved) => resolved.path,
            Err(e) => fail_init(&request_id, STORAGE_RESOLUTION_CODE, e.to_string()),
        };
        let console_log_store = Arc::new(
            SqliteConsoleLogStore::open(&console_log_path).unwrap_or_else(|e| {
                fail_init(
                    &request_id,
                    STORAGE_RESOLUTION_CODE,
                    format!(
                        "failed to open the mobkit console store at {}: {e}",
                        console_log_path.display()
                    ),
                );
            }),
        );
        runtime.set_console_log_store(console_log_store);
    }

    for route in gateway_options.routing_routes.iter().cloned() {
        if let Err(err) = runtime.add_runtime_route(route).await {
            fail_init(
                &request_id,
                -32602,
                format!("runtime_options.routing_config_path route failed validation: {err}"),
            );
        }
    }

    if let Some(event_log_config) = gateway_options.event_log.take() {
        runtime.start_event_log(event_log_config);
    }

    if let Some(access) = gateway_options.access.take() {
        runtime.set_access_controller(access);
    }

    // 5b. Wire error hook — forwards ErrorEvents to Python as JSON-RPC notifications
    let gateway_error_hook: meerkat_mobkit::ErrorHook = {
        let error_bridge = bridge.clone();
        Arc::new(move |event| {
            let b = error_bridge.clone();
            Box::pin(async move {
                if let Ok(params) = serde_json::to_value(&event) {
                    b.notify("mobkit/on_error", params);
                }
            })
        })
    };
    runtime.set_error_hook(gateway_error_hook.clone());

    // 5c. Build identity-first runtime if providers are configured
    // §8.5 steward late-bound seams: the gating/conflict bridges need the
    // Arc'd UnifiedRuntime, which exists only after identity-first init; a
    // OnceCell defers the binding without restructuring bootstrap. The
    // roster slot feeds mob-purpose context once restore_flow has run.
    let steward_late_runtime = StewardLateRuntime::default();
    let steward_roster_slot: Arc<
        std::sync::Mutex<Vec<meerkat_mobkit::identity_first::DurableAgentSpec>>,
    > = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut agent_memory_steward: Option<Arc<meerkat_mobkit::memory::steward::StewardEngine>> =
        None;

    let identity_ctx: Option<meerkat_mobkit::rpc::IdentityFirstContext> = if has_roster_provider {
        use meerkat_mobkit::identity_first::{
            AgentRuntimeServices, DurabilityPolicy, IdentityFirstRuntimeContext, IdentityRuntime,
            IdentityRuntimeConfig, RosterContext,
            gateway_bridges::{
                GatewayAgentCustomizer, GatewayRosterProvider, GatewayTopologyProvider,
            },
        };

        let continuity_store = identity_continuity_store
            .clone()
            .expect("identity continuity store initialized with roster provider");
        let lease_provider = identity_lease_provider
            .clone()
            .expect("identity lease provider initialized with roster provider");

        // Construct the session bridge from the mob handle. The gateway
        // uses the raw bootstrap path (not UnifiedRuntimeBuilder), so
        // session_bridge() won't be set — build it directly.
        let mob_handle = runtime.mob_handle();
        let mut identity_bridge = if let Some(adapter) = identity_session_store_adapter.clone() {
            meerkat_mobkit::identity_first::MobSessionBridge::with_continuity_session_store(
                mob_handle.clone(),
                adapter,
                runtime.mob_runtime().session_service().cloned(),
            )
        } else if let Some(session_service) = runtime.mob_runtime().session_service().cloned() {
            meerkat_mobkit::identity_first::MobSessionBridge::with_session_service(
                mob_handle.clone(),
                session_service,
            )
        } else {
            meerkat_mobkit::identity_first::MobSessionBridge::new(mob_handle.clone())
        };
        // Heal seam (2026-07-29 incident): the continuity repair supervisor
        // asks this recoverer to commit the durable head before declaring an
        // identity healed; without it, heal is a cosmetic entry reset that
        // the next materialization re-Breaks.
        if let Some(recoverer) = runtime.mob_runtime().committed_boundary_recoverer() {
            identity_bridge = identity_bridge.with_committed_boundary_recoverer(recoverer);
        }
        // A declared migration restamps a member's durable role, comms name
        // and binding, so the boot record must name every one the host armed.
        for declaration in &role_migration_declarations {
            tracing::info!(
                identity = %declaration.identity,
                from_role = %declaration.from_role,
                "activation declares a member role migration"
            );
        }
        identity_bridge =
            identity_bridge.with_role_migration_declarations(role_migration_declarations);
        // Operator-verb seam: share the bridge's compaction-floor registry
        // with the RPC context BEFORE the bridge is erased, so
        // mobkit/compact_member arms floors on the same registry the
        // materialization path reads.
        let compaction_floors = identity_bridge.compaction_floors();
        let bridge_arc: Arc<dyn meerkat_mobkit::identity_first::SessionBridge> =
            Arc::new(identity_bridge);

        let irt = Arc::new(
            IdentityRuntime::new(IdentityRuntimeConfig {
                continuity_store,
                lease_provider,
                runtime_instance_id: format!("gateway-{}", std::process::id()),
                has_runtime_store: identity_session_store_adapter.is_some()
                    || persistent_state.is_some(),
                durability_policy: DurabilityPolicy::SyncWriteThrough,
                bridge: Some(bridge_arc),
                default_timeout: None,
            })
            .with_runtime_services(AgentRuntimeServices::new(mob_handle)),
        );
        irt.set_error_hook(Some(gateway_error_hook.clone()));
        irt.set_customizer_tool_registry(customizer_tool_registry.clone())
            .await;
        // A member parked with a session repair hold names the exact
        // `rkat session repair-wholeblob` commands for the runtime store this
        // launch opened; an in-memory store has nothing to repair.
        irt.set_session_repair_scope(
            runtime.resolved_storage().as_ref().and_then(
                meerkat_mobkit::identity_first::SessionRepairScope::from_resolved_storage,
            ),
        );

        // Build provider bridges for callbacks to Python
        let roster: Arc<dyn meerkat_mobkit::identity_first::contracts::RosterProvider> =
            Arc::new(GatewayRosterProvider::new(bridge.clone()));
        let mob_definition = runtime.mob_handle().definition().clone();
        irt.set_reset_roster_provider_context(Some(roster.clone()), Some(mob_definition.clone()));
        let topology: Option<Arc<dyn meerkat_mobkit::identity_first::contracts::TopologyProvider>> =
            if has_topology_provider {
                Some(Arc::new(GatewayTopologyProvider::new(bridge.clone())))
            } else {
                None
            };
        let base_customizer: Option<
            Arc<dyn meerkat_mobkit::identity_first::contracts::AgentCustomizer>,
        > = if has_agent_customizer {
            Some(Arc::new(GatewayAgentCustomizer::new(bridge.clone())))
        } else {
            None
        };
        let mut agent_memory_taint: Option<meerkat_mobkit::SessionTaintTracker> = None;
        // Late-bound target for the always-on compaction reset sink: the
        // sink joins the observer before the injector exists, so it calls
        // through this slot (empty until wiring completes = no-op).
        let agent_memory_compaction_reset: Arc<
            std::sync::OnceLock<meerkat_mobkit::AgentMemoryRuntimeInjector>,
        > = Arc::new(std::sync::OnceLock::new());
        let mut agent_memory_distiller: Option<
            Arc<meerkat_mobkit::memory::distiller::DistillerEngine>,
        > = None;
        let agent_memory_provider: Option<Arc<dyn meerkat_mobkit::AgentMemoryProvider>> =
            if let Some(agent_memory) = gateway_options.agent_memory.as_ref() {
                // Concrete store construction is the only per-kind decision
                // left (M4 de-weld); everything downstream assembles on the
                // provider's advertised capabilities.
                // Agent memory is a durable storage slot: an open failure is
                // a storage-resolution refusal (-32014), same as the other
                // fail-closed store opens above.
                let provider: Arc<dyn meerkat_mobkit::AgentMemoryProvider> =
                    match agent_memory.store {
                        // The live markdown execution path is retired: refuse
                        // with the typed migration verdict instead of booting
                        // a store that silently has no firewall, no ledger
                        // and no judgment plane. The markdown READER is
                        // untouched and still runs, as the one-shot import
                        // inside SqliteAgentMemoryStore::open.
                        GatewayAgentMemoryStoreKind::Markdown => {
                            let migration = AgentMemoryStoreMigration::MarkdownIsImportOnly;
                            fail_init(&request_id, migration.code(), migration.message())
                        }
                        GatewayAgentMemoryStoreKind::Sqlite => Arc::new(
                            meerkat_mobkit::SqliteAgentMemoryStore::open(&agent_memory.path)
                                .unwrap_or_else(|e| {
                                    fail_init(
                                        &request_id,
                                        STORAGE_RESOLUTION_CODE,
                                        format!("failed to open agent memory store: {e}"),
                                    );
                                }),
                        ),
                    };
                if provider.as_taintable().is_some() {
                    // Shared read handle on the persistent session store
                    // for the Distiller's evidence windows and the
                    // steward's gather/usage/resolvability reads.
                    let memory_transcript_store: Option<Arc<dyn meerkat::SessionStore>> =
                        if agent_memory.distiller.enabled || agent_memory.steward.enabled {
                            if persistent_state.is_none() {
                                fail_init(
                                    &request_id,
                                    -32602,
                                    "agent memory distiller/steward require persistent_state"
                                        .to_string(),
                                );
                            }
                            Some(
                                if let Some(adapter) = identity_session_store_adapter.clone() {
                                    adapter
                                } else {
                                    // Second handle on the same session
                                    // database the mob bridge persists to;
                                    // WAL keeps the read-side safe.
                                    let session_db = match storage_layout.session_db() {
                                        Ok(resolved) => resolved.path,
                                        Err(e) => fail_init(
                                            &request_id,
                                            STORAGE_RESOLUTION_CODE,
                                            e.to_string(),
                                        ),
                                    };
                                    match meerkat_store::SqliteSessionStore::open(session_db) {
                                        Ok(store) => Arc::new(store),
                                        Err(e) => fail_init(
                                            &request_id,
                                            STORAGE_RESOLUTION_CODE,
                                            format!("agent memory session store: {e}"),
                                        ),
                                    }
                                },
                            )
                        } else {
                            None
                        };
                    // Converged assembly (memory_wiring): provider + §10.1
                    // firewall + Distiller + Steward, with the gateway's
                    // late-binding bridges passed as seams. Gateway-only
                    // extras (outbound taint declarer, panel registration,
                    // compaction reset, observer spawn, dream scheduling)
                    // follow below. Hygienist activation is parked and has
                    // no composition seam here.
                    let memory_events = runtime.memory_event_sink();
                    let engines = meerkat_mobkit::memory_wiring::MemoryEnginesConfig {
                        distiller: agent_memory.distiller.clone(),
                        steward: agent_memory.steward.clone(),
                    };
                    let stack = match meerkat_mobkit::memory_wiring::attach_memory_engines(
                        provider.clone(),
                        &agent_memory.config,
                        &engines,
                        meerkat_mobkit::memory_wiring::MemoryStackSeams {
                            persistent_state: persistent_state.clone(),
                            transcript_store: memory_transcript_store,
                            event_sink: Some(memory_events.clone()),
                            mob_purpose: Some(Arc::new(GatewayMobPurposeSource {
                                mob: mob_definition.id.to_string(),
                                roster: steward_roster_slot.clone(),
                            })),
                            steward_gating: Some(Arc::new(GatewayMemoryGatingBridge {
                                runtime: steward_late_runtime.clone(),
                            })),
                            steward_conflicts: Some(Arc::new(GatewayMemoryConflictBridge {
                                runtime: steward_late_runtime.clone(),
                                handle: tokio::runtime::Handle::current(),
                            })),
                        },
                    ) {
                        Ok(stack) => stack,
                        Err(e) => fail_init(&request_id, -32602, e),
                    };
                    let panel = stack.panel.clone();
                    let tracker = stack.taint;
                    let mut sinks = stack.sinks;
                    agent_memory_distiller = stack.distiller;
                    if let Some(engine) = agent_memory_distiller.as_ref() {
                        let _ = engine;
                        tracing::info!(
                            model = ?agent_memory.distiller.model,
                            "agent memory distiller installed"
                        );
                    }
                    if let Some(engine) = stack.steward.clone() {
                        // Dream cadence (§ ask 7 / P5): when this gateway
                        // runs a schedule host, the dream is driven as a
                        // durable host-runnable occurrence (registered at
                        // the schedule-host spawn). Only gateways WITHOUT
                        // a schedule host keep the in-process loop.
                        if schedule_host_inputs.is_none() {
                            std::mem::forget(engine.spawn_dream_loop());
                        }
                        agent_memory_steward = Some(engine);
                        tracing::info!(
                            model = ?agent_memory.steward.model,
                            cadence = %agent_memory.steward.cadence,
                            per_mob = agent_memory.steward.per_mob,
                            "agent memory steward installed"
                        );
                    }
                    // §10.1 ask 5 (outbound half): make the member's comms
                    // runtime the authenticated carrier of the host's taint
                    // fact. Fire-and-forget: the member may not be
                    // materialized, so a miss is logged, not fatal.
                    let taint_mob_handle = runtime.mob_handle();
                    tracker.set_outbound_taint_declarer(std::sync::Arc::new(
                            move |identity: &str,
                                  taint: Option<meerkat_core::comms::SenderContentTaint>| {
                                let handle = taint_mob_handle.clone();
                                let member =
                                    meerkat_mobkit::member_comms_id::mob_member_id(identity);
                                let identity_owned = identity.to_string();
                                tokio::spawn(async move {
                                    if let Err(err) = handle
                                        .declare_member_outbound_taint(member, taint)
                                        .await
                                    {
                                        tracing::debug!(
                                            identity = %identity_owned,
                                            ?taint,
                                            error = %err,
                                            "agent memory taint: outbound taint declaration \
                                             failed (member may not be materialized)"
                                        );
                                    }
                                });
                            },
                        ));
                    // §9.3 console Memory panel: must precede
                    // build_reference_app_router (the router builds after
                    // this region). Registered when — and only when —
                    // the provider advertises the panel read API.
                    if let Some(panel) = panel {
                        runtime.set_memory_panel_store(panel);
                    }
                    // §9.1 as-built: ALWAYS-ON compaction reset for the
                    // coordinator's session budgets — deliberately NOT
                    // gated on distiller.enabled (gate finding: budgeted
                    // injection without a distiller never reset).
                    let compaction_reset_slot = agent_memory_compaction_reset.clone();
                    sinks.push(Arc::new(meerkat_mobkit::CompactionResetSink::new(
                        Arc::new(move |session: &str| {
                            if let Some(injector) = compaction_reset_slot.get() {
                                injector.on_session_compacted(session);
                            }
                        }),
                    )));
                    // §10.1 dispatch-time taint join: bind the tracker into
                    // the member pre-build seam so every member's LLM client
                    // marks untrusted ingestion synchronously - ahead of the
                    // async observer spawned below (first-ingestion race).
                    dispatch_taint_slot.fill(tracker.clone());
                    // Observe-stream feed lives for the gateway process;
                    // forgetting the guard keeps the task running.
                    std::mem::forget(meerkat_mobkit::spawn_member_event_observer(
                        runtime.mob_handle(),
                        sinks,
                    ));
                    agent_memory_taint = Some(tracker);
                    Some(stack.provider)
                } else {
                    // Recall-only provider by its capability flags (M4
                    // de-weld): no firewall, no engines, no panel — and any
                    // engine explicitly configured against it refuses
                    // loudly instead of silently not existing.
                    if agent_memory.distiller.enabled || agent_memory.steward.enabled {
                        fail_init(
                            &request_id,
                            -32602,
                            "agent memory distiller/steward require a provider \
                             with judgment-plane capabilities (the bundled sqlite store)"
                                .to_string(),
                        );
                    }
                    Some(provider)
                }
            } else {
                None
            };
        // §7.2 / §16 Q1 operator-resolver seam. The PROVISIONAL keying is
        // the console auth principal; that resolver lives with the console
        // auth wiring and plugs in here as one line of glue. Until it is
        // installed, `operator_scope = "provisional"` composes nothing
        // (inert by design) while steward routing (proposal-keyed) is
        // already active.
        // §16 Q1 provisional keying (decided 2026-07-04): OperatorId = the
        // console auth principal. When the scope is provisional, install the
        // console-principal resolver and share it with the runtime so the
        // console send path can note authenticated interactions; recall
        // composition activates for identities an authenticated principal
        // has addressed (config AND resolver AND a real principal).
        let agent_memory_operator_resolver: Option<
            Arc<dyn meerkat_mobkit::memory::coordinator::OperatorResolver>,
        > = if gateway_options.agent_memory.as_ref().is_some_and(|memory| {
            memory.config.operator_scope == meerkat_mobkit::AgentMemoryOperatorScope::Provisional
        }) {
            let resolver = Arc::new(meerkat_mobkit::ConsolePrincipalOperatorResolver::new());
            runtime.set_console_operator_resolver(resolver.clone());
            tracing::info!(
                "agent memory operator scope active (provisional keying: console auth principal)"
            );
            Some(resolver)
        } else {
            None
        };
        // Degrade LOUD, not silent: recall composition of the Operator scope
        // needs BOTH the knob and a resolver (coordinator.rs scope_set), so a
        // resolver-less provisional deployment gets steward proposal-routing
        // only. Without this warning that half-activation is invisible.
        if operator_scope_recall_inert(
            gateway_options.agent_memory.as_ref(),
            agent_memory_operator_resolver.is_some(),
        ) {
            tracing::warn!(
                "agent_memory.operator_scope=\"provisional\" is configured but this gateway \
                 installs no operator resolver: operator-scope recall composition is INERT \
                 (records routed to the operator scope will not be recalled or injected); \
                 steward routing of operator-scope proposals remains active"
            );
        }
        // §7.2 mob-scope binding: every identity this gateway hosts runs
        // inside the one mob the runtime fronts, so a static binding closes
        // the mob-memory read path (write-only otherwise — gate blocker).
        let agent_memory_mob_resolver: Option<
            Arc<dyn meerkat_mobkit::memory::coordinator::MobScopeResolver>,
        > = gateway_options.agent_memory.as_ref().map(|memory| {
            Arc::new(meerkat_mobkit::memory::coordinator::StaticMobBinding {
                realm: memory.config.realm.clone(),
                mob: runtime.mob_handle().mob_id().to_string(),
            }) as Arc<dyn meerkat_mobkit::memory::coordinator::MobScopeResolver>
        });
        let customizer: Option<
            Arc<dyn meerkat_mobkit::identity_first::contracts::AgentCustomizer>,
        > = if let (Some(agent_memory), Some(provider)) = (
            gateway_options.agent_memory.as_ref(),
            agent_memory_provider.clone(),
        ) {
            Some(Arc::new(
                meerkat_mobkit::AgentMemoryCustomizer::wrap(
                    base_customizer,
                    provider,
                    agent_memory.config.clone(),
                )
                .with_operator_resolver(agent_memory_operator_resolver.clone())
                .with_mob_resolver(agent_memory_mob_resolver.clone()),
            ))
        } else {
            base_customizer
        };
        let agent_memory_injector = if let (Some(agent_memory), Some(provider)) = (
            gateway_options.agent_memory.as_ref(),
            agent_memory_provider.clone(),
        ) {
            let mut injector = meerkat_mobkit::AgentMemoryRuntimeInjector::new(
                provider,
                agent_memory.config.clone(),
            );
            if let Some(tracker) = agent_memory_taint.clone() {
                injector = injector.with_taint_tracker(tracker);
            }
            if let Some(distiller) = agent_memory_distiller.clone() {
                injector = injector.with_distiller(distiller);
            }
            if let Some(steward) = agent_memory_steward.clone() {
                injector = injector.with_steward(steward);
            }
            injector = injector.with_operator_resolver(agent_memory_operator_resolver.clone());
            injector = injector.with_mob_resolver(agent_memory_mob_resolver.clone());
            // Arm the always-on compaction reset sink (state is Arc-shared
            // across injector clones, so resetting through this clone
            // resets the delivery path's budgets too).
            let _ = agent_memory_compaction_reset.set(injector.clone());
            Some(injector)
        } else {
            None
        };
        irt.set_agent_memory(agent_memory_injector).await;
        // Install the composed customizer (host and/or agent memory) as the
        // runtime's own, as the library builder does: a member materialized
        // outside the roster bootstrap (lazy materialization on first
        // dispatch, delivery-time repair) carries no per-call override and
        // runs the installed customizer, so without this it ran none.
        irt.set_agent_customizer(customizer.clone()).await;

        init_progress.enter(InitPhase::Roster);
        if shutdown_requested.load(Ordering::Acquire) {
            abort_init_for_shutdown(Some(&*runtime), &mut rpc_rx, init_progress).await;
        }
        // Bootstrap identities from the roster provider using the explicit
        // gateway mode (eager remains the compatibility default).
        let roster_specs = roster
            .roster(&RosterContext {
                mob_definition: Some(mob_definition.clone()),
                previous_identities: Vec::new(),
            })
            .await
            .unwrap_or_else(|e| {
                fail_init(&request_id, -32603, format!("roster provider failed: {e}"));
            });

        // Mob-purpose context for the steward's promotion judgment: no
        // mob-level purpose field exists (verified — `MobDefinition` is
        // structural only), so the roster's labels are the source.
        *steward_roster_slot
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = roster_specs.clone();

        let identity_context = Arc::new(IdentityFirstRuntimeContext::new_with_bootstrap_mode(
            irt.clone(),
            roster.clone(),
            topology.clone(),
            customizer.clone(),
            Some(runtime.mob_handle().definition().clone()),
            gateway_options
                .identity_bootstrap_mode
                .clone()
                .unwrap_or_default(),
        ));
        if let Err(e) = runtime
            .install_and_bootstrap_identity_first_context(
                Arc::clone(&identity_context),
                &roster_specs,
            )
            .await
        {
            // `install_and_bootstrap_identity_first_context` has already run
            // the full UnifiedRuntime shutdown sequence. Keep the callback
            // bridge/stdin reader alive until that returns: external lease
            // release is itself a callback that must be acknowledged before
            // the process emits the init error and exits.
            fail_init(
                &request_id,
                -32603,
                format!("identity-first bootstrap failed: {e}"),
            );
        }

        Some(meerkat_mobkit::rpc::IdentityFirstContext {
            runtime: irt,
            roster_provider: roster,
            topology_provider: topology,
            customizer,
            agent_memory_provider,
            mob_definition: Some(mob_definition),
            transcript_edit_service: gateway_transcript_edit_service.clone(),
            compaction_floors: Some(compaction_floors),
        })
    } else {
        if let Err(error) = runtime.activate_without_identity_context().await {
            fail_init(
                &request_id,
                -32603,
                format!("activating runtime without an identity roster failed: {error}"),
            );
        }
        None
    };

    init_progress.enter(InitPhase::Schedules);
    if shutdown_requested.load(Ordering::Acquire) {
        abort_init_for_shutdown(Some(&*runtime), &mut rpc_rx, init_progress).await;
    }
    // Run the schedule driver after identity-first restore, so legacy
    // resumable-session repair can see live member bridge-session bindings and
    // due occurrences do not race identity materialization.
    let (schedule_host, _schedule_watchdog) = if let Some((
        schedule_service,
        mob_target_registry,
        service,
        adapter,
        schedule_store_path,
        schedule_firing_host_binding,
    )) = schedule_host_inputs
    {
        // Shared with mobkit_gateway. Kept a separate call from the host spawn
        // below on purpose: this gateway registers the steward dream BETWEEN
        // the two, and folding them into one helper would have silently
        // reordered a durable-store boot sequence.
        let mob_state = meerkat_mobkit::gateway_composition::adopt_schedule_mob_targets(
            runtime,
            &schedule_service,
            &mob_target_registry,
        )
        .await;
        // §8.5 / upstream ask 7 (P5): drive the memory steward's dream through
        // the durable schedule host instead of a bare interval loop. Register
        // the dream as a host runnable and find-or-create its cadence schedule
        // (idempotent across boots via the persistent store). The in-process
        // fallback loop is spawned only when there is no schedule host.
        // SDK-registered runnables (`runtime_options.host_runnables`) compose
        // into the same registry: their fires forward over the callback
        // bridge as `callback/schedule_fire`, so apps get deterministic
        // (non-LLM) schedule targets.
        let callback_runnables = (!gateway_options.host_runnables.is_empty()).then(|| {
            (
                Arc::new(bridge.clone())
                    as Arc<dyn meerkat_mobkit::identity_first::gateway_bridges::CallbackBridge>,
                gateway_options.host_runnables.clone(),
            )
        });
        let runnable_host = match meerkat_mobkit::schedule_wiring::gateway_runnable_host(
            agent_memory_steward.clone(),
            callback_runnables,
        ) {
            Ok(host) => host,
            Err(error) => {
                // Structurally unreachable: names are deduplicated and the
                // reserved steward name rejected at option parse time.
                fail_init(
                    &request_id,
                    -32602,
                    format!("failed to compose schedule host runnables: {error}"),
                );
            }
        };
        if let Some(steward) = agent_memory_steward.as_ref()
            && let Err(error) = meerkat_mobkit::schedule_wiring::ensure_steward_dream_schedule(
                &schedule_service,
                steward.dream_cadence(),
                chrono::Utc::now(),
            )
            .await
        {
            tracing::warn!(
                error = %error,
                "failed to ensure steward dream schedule; the steward will not dream on this gateway",
            );
        }
        // Shared with mobkit_gateway: the watchdog liveness contract log, the
        // host spawn, the firing-intent gate bind and the boot probe were
        // byte-identical in both binaries, every log string included. The one
        // real divergence, this gateway's runnable host (steward dream + the
        // SDK-declared `runtime_options.host_runnables` composed just above),
        // is now a named FIELD instead of a positional argument nobody could
        // see was a divergence.
        let (schedule_host, watchdog) =
            meerkat_mobkit::gateway_composition::spawn_gateway_schedule_host(
                runtime,
                mob_state,
                meerkat_mobkit::gateway_composition::GatewayScheduleHostInputs {
                    schedule_service,
                    session_service: service,
                    runtime_adapter: adapter,
                    schedule_store_path,
                    firing_host_binding: schedule_firing_host_binding,
                    runnable_host,
                    workgraph_service: workgraph_service.clone(),
                    owner_id: schedule_owner_id.clone(),
                },
            )
            .await;
        (schedule_host, Some(watchdog))
    } else {
        if !gateway_options.host_runnables.is_empty() {
            tracing::warn!(
                "runtime_options.host_runnables is configured but this gateway runs no \
                 schedule host (ephemeral mode, or schedule store unavailable): \
                 host-runnable schedule targets will never fire"
            );
        }
        (None, None)
    };

    // Cross-mob surfaces: install the inline contact directory (while the
    // runtime is still exclusively owned), the gateway signing identity,
    // and the control listener. The listener starts after identity-first
    // attachment above, but its handler re-reads the identity authority
    // per request either way.
    if let Some(directory) = gateway_options.contacts.clone() {
        runtime.set_contact_directory(directory);
    }
    // The gateway keypair signs cross-mob control responses (peers with
    // this gateway's pubkey pinned verify them) and backs mobkit/peer_pubkey.
    // Persistent boots keep it stable across restarts in the state dir;
    // ephemeral boots mint a per-process key.
    let gateway_peer_keys = match persistent_state.as_ref() {
        Some(state_path) => match meerkat_mobkit::GatewayPeerKeys::load_or_create(state_path) {
            Ok(keys) => keys,
            Err(error) => fail_init(
                &request_id,
                -32603,
                format!(
                    "failed to load or mint the gateway peer key under {}: {error}",
                    state_path.display()
                ),
            ),
        },
        None => meerkat_mobkit::GatewayPeerKeys::ephemeral(),
    };
    runtime.set_gateway_peer_keys(gateway_peer_keys);
    let control_listen_address = match control_listen.as_ref() {
        Some(addr) => {
            let authorizer = Arc::new(ControlAuthorizer::with_grants_for_audience(
                std::mem::take(&mut gateway_options.control_grants),
                runtime.mob_id(),
            ));
            match runtime
                .start_control_listener_with_authorizer(addr, authorizer)
                .await
            {
                Ok(advertised) => {
                    tracing::info!(%advertised, "cross-mob control listener bound");
                    Some(advertised)
                }
                Err(error) => fail_init(
                    &request_id,
                    -32603,
                    format!("--control-listen {addr}: {error}"),
                ),
            }
        }
        None => None,
    };

    let composition = composition.activate();
    let runtime = Arc::clone(composition.runtime());
    if let Some(detached_jobs) = gateway_detached_jobs.as_ref() {
        match detached_jobs.health_projection().await {
            Ok(projection) => runtime.set_job_health_projection(Some(projection)),
            Err(error) => {
                tracing::warn!(%error, "initial durable callback health projection failed");
                runtime.set_job_health_projection(Some(json!({
                    "status": "degraded",
                    "detached_jobs": {
                        "status": "degraded",
                        "reason": "job_health_projection_failed"
                    }
                })));
            }
        }
        detached_jobs.arm_delivery_driver(Arc::clone(&runtime));
    }
    // Bind the steward's late runtime seams and wire gating decisions back
    // to staged promotion commits (§10.2).
    steward_late_runtime.bind(runtime.clone());
    if let Some(steward) = agent_memory_steward.clone() {
        runtime
            .register_gating_resolution_observer(Arc::new(
                meerkat_mobkit::PromotionGateResolver::new(
                    steward,
                    tokio::runtime::Handle::current(),
                ),
            ))
            .await;
    }
    init_progress.enter(InitPhase::Serve);
    if shutdown_requested.load(Ordering::Acquire) {
        abort_init_for_shutdown(Some(runtime.as_ref()), &mut rpc_rx, init_progress).await;
    }
    // 6. The HTTP listener was bound at init, right behind the exposure gate
    // (see `http_binding` above); only serving it waits for the runtime.

    // 7. Start HTTP with graceful shutdown
    let mut decision_state = gateway_options
        .decisions
        .clone()
        .unwrap_or_else(minimal_decision_state);
    decision_state.console.ui = gateway_options.console_ui.clone();
    meerkat_mobkit::gateway_composition::warn_on_non_loopback_bind(
        meerkat_mobkit::gateway_composition::GatewaySurface::RpcGateway,
        http_binding.local_addr(),
        &decision_state,
    );
    if meerkat_mobkit::gateway_composition::ConsoleAuthPosture::of(&decision_state)
        == meerkat_mobkit::gateway_composition::ConsoleAuthPosture::ClosedToEveryCaller
    {
        tracing::warn!(
            "console requires app auth but trusts no signing key: every console request will be \
             refused with 401. Pass runtime_options.auth_config to trust an issuer, or set \
             runtime_options.console_require_app_auth = false for an explicitly open local console"
        );
    }
    let app = runtime.build_reference_app_router(decision_state);
    // Live (realtime) transport: ONE live context per gateway (one adapter
    // host, one WS state, one provider registration) serves every live door.
    // Console voice and the external `mobkit/live/*` surface share it and
    // take turns on its voice-path arbiter ("latest engaged wins"). The WS
    // router mounts on the SAME HTTP listener the console uses (no second
    // port; a LAN client or the host's reverse proxy reaches it at
    // {base}/live/ws), and the live/* RPC handler is erased over the
    // gateway's concrete session-builder type for the stdin dispatch loop.
    let ws_base_url = match &gateway_options.live {
        GatewayLiveOption::Enabled {
            public_base_url: Some(public),
            ..
        } => public.trim_end_matches('/').to_string(),
        _ => format!("ws://{http_reachable_addr}"),
    };
    let live_seed_max_chars = match &gateway_options.live {
        GatewayLiveOption::Enabled { seed_max_chars, .. } => *seed_max_chars,
        GatewayLiveOption::Disabled => None,
    };
    // The same host config every other agent-build door starts from:
    // `EnvRealtimeConfigSource` reads `[realm]` / `[self_hosted]` off it,
    // so `Config::default()` here would drop a declared host config on
    // the live door alone, with no diagnostic.
    let live_ctx = live_inputs.as_ref().map(|(service, machine, factory)| {
        Arc::new(meerkat_mobkit::live_wiring::attach_live(
            Arc::clone(service),
            Arc::clone(machine),
            factory,
            gateway_agent_config(&gateway_options),
            ws_base_url.clone(),
            live_seed_max_chars,
        ))
    });
    #[cfg(feature = "openai-live")]
    if let Some(live_ctx) = live_ctx.as_ref() {
        // Preempted owners learn immediately over stdio, not on their next
        // poll: `mobkit/live/superseded {owner, identity, channel_id, reason}`.
        let notifier_bridge = bridge.clone();
        live_ctx
            .arbiter
            .set_notifier(Arc::new(move |method, params| {
                notifier_bridge.notify(method, params);
            }));
    }
    #[cfg(feature = "openai-live")]
    let console_voice_controller = if let Some(registration) = gateway_options.console_voice.clone()
    {
        let (Some(live_ctx), Some((service, machine, factory))) =
            (live_ctx.as_ref(), live_inputs.as_ref())
        else {
            fail_init(
                &request_id,
                -32602,
                "console_voice requires persistent sessions".to_string(),
            );
        };
        meerkat_mobkit::console_voice::ConsoleVoiceController::with_shared_live_context(
            &runtime,
            Arc::clone(live_ctx),
            Arc::clone(service),
            Arc::clone(machine),
            factory.clone(),
            registration,
        )
        .unwrap_or_else(|error| {
            fail_init(
                &request_id,
                -32602,
                format!("console_voice composition failed: {error}"),
            )
        })
    } else {
        meerkat_mobkit::console_voice::ConsoleVoiceController::default()
    };
    #[cfg(not(feature = "openai-live"))]
    let console_voice_controller = meerkat_mobkit::console_voice::ConsoleVoiceController::default();
    // The external live door (WS router + stdio `mobkit/live/*`) mounts when
    // `live` is enabled, or when a strict registration other than console
    // voice asked for it. Console voice alone keeps its channels private to
    // the authenticated console, exactly as before.
    #[cfg(feature = "openai-live")]
    let console_voice_registered = gateway_options.console_voice.is_some();
    #[cfg(not(feature = "openai-live"))]
    let console_voice_registered = false;
    let external_live_door = live_inputs.is_some()
        && (matches!(gateway_options.live, GatewayLiveOption::Enabled { .. })
            || !console_voice_registered);
    let (app, live_rpc) =
        if let (true, Some(live_ctx), Some((live_service, live_machine, live_agent_factory))) =
            (external_live_door, live_ctx, live_inputs)
        {
            // Only the strict (openai-live) capability providers build from the
            // factory here; the ordinary door reads everything from `live_ctx`.
            #[cfg(not(feature = "openai-live"))]
            let _ = &live_agent_factory;
            let app = if matches!(gateway_options.live, GatewayLiveOption::Enabled { .. }) {
                app.merge(meerkat_live::live_ws_router(Arc::clone(&live_ctx.ws_state)))
            } else {
                app
            };
            #[cfg(feature = "openai-live")]
            let capability_provider = {
                // Shared strict-registration inputs: the owning Mob MCP state, the
                // durable-session binding authority scoped to the registered
                // principal and exact configured binding, the sealed WebRTC
                // transport, and the stdio public-observation publisher. Both
                // strict paths compose these identically; only the open authority
                // and its admission differ.
                let strict_live_inputs =
                    |option_name: &str, principal: &str, binding: &meerkat_core::AuthBindingRef| {
                        let mob_mcp_state = runtime
                    .mob_runtime()
                    .agent_mob_mcp_state()
                    .unwrap_or_else(|| {
                        fail_init(
                            &request_id,
                            -32602,
                            format!(
                                "runtime_options.{option_name} requires the owning Mob MCP state"
                            ),
                        )
                    });
                        let access = gateway_options
                            .access
                            .clone()
                            .unwrap_or_else(meerkat_mobkit::AccessController::disabled);
                        let binding_authority =
                            Arc::new(GatewayExperimentalLiveSessionBindingAuthority {
                                handle: runtime.mob_handle(),
                                machine: Arc::clone(&live_machine),
                                access,
                                principal: principal.to_string(),
                                allowed_binding: binding.clone(),
                            });
                        let transport = Arc::new(
                            meerkat::experimental_gpt_live::ExperimentalGptLiveWebrtcTransport::new(
                            ),
                        );
                        let publisher =
                            Arc::new(StdioExperimentalLivePublicObservationPublisher::new(
                                Arc::clone(&live_machine),
                                bridge.clone(),
                            ));
                        (mob_mcp_state, binding_authority, transport, publisher)
                    };
                let mut capability_provider: Option<
                    meerkat_mobkit::live_wiring::LiveCapabilityProvider,
                > = None;
                #[cfg(feature = "experimental-gpt-live")]
                if let Some(experimental) = gateway_options.experimental_live.as_ref() {
                    let (mob_mcp_state, binding_authority, transport, publisher) =
                        strict_live_inputs(
                            "experimental_live",
                            &experimental.principal,
                            &experimental.binding,
                        );
                    let open_authority = Arc::new(
                    meerkat::experimental_gpt_live::ExperimentalGptLiveOpenAuthority::new(
                        meerkat::experimental_gpt_live::ExperimentalGptLiveOpenAuthorityConfig {
                            agent_factory: live_agent_factory.clone(),
                            config_source: live_ctx.experimental_live_config_source(),
                            binding_authority,
                            execution_identity: meerkat_core::SessionLlmIdentity {
                                model: "gpt-live-1-codex".to_string(),
                                provider: meerkat_core::Provider::OpenAI,
                                self_hosted_server_id: None,
                                provider_params: None,
                                auth_binding: Some(experimental.binding.clone()),
                            },
                            realm: experimental.realm.clone(),
                            factory_identity: experimental.factory.clone(),
                            transport: Arc::clone(&transport),
                            voice: experimental.voice.clone(),
                        },
                    )
                    .unwrap_or_else(|error| {
                        fail_init(
                            &request_id,
                            -32602,
                            format!(
                                "runtime_options.experimental_live composition failed: {error}"
                            ),
                        )
                    }),
                );
                    capability_provider = Some(
                        meerkat_mobkit::live_wiring::LiveCapabilityProvider::experimental(
                            Arc::new(live_agent_factory.clone()),
                            experimental.realm.clone(),
                            experimental.factory.clone(),
                            open_authority,
                            transport,
                            mob_mcp_state,
                            publisher,
                        ),
                    );
                }
                // Parsing rejects both registrations at once, so this never
                // overrides an experimental provider.
                if let Some(public) = gateway_options.openai_live.as_ref() {
                    let (mob_mcp_state, binding_authority, transport, publisher) =
                        strict_live_inputs("openai_live", &public.principal, &public.binding);
                    let open_authority = Arc::new(
                    meerkat::experimental_gpt_live::ExperimentalGptLiveOpenAuthority::new_public(
                        meerkat::experimental_gpt_live::PublicGptLiveOpenAuthorityConfig {
                            agent_factory: live_agent_factory.clone(),
                            config_source: live_ctx.experimental_live_config_source(),
                            binding_authority,
                            execution_identity: meerkat_core::SessionLlmIdentity {
                                model: meerkat::GPT_LIVE_PUBLIC_MODEL.to_string(),
                                provider: meerkat_core::Provider::OpenAI,
                                self_hosted_server_id: None,
                                provider_params: None,
                                auth_binding: Some(public.binding.clone()),
                            },
                            realm: public.realm.clone(),
                            transport: Arc::clone(&transport),
                            voice: public.voice.clone(),
                            session_instructions: public.session_instructions.clone(),
                            // The external live channel keeps its configured
                            // instructions; the per-member capabilities preface
                            // is a console voice concern.
                            session_instructions_preface: None,
                        },
                    )
                    .unwrap_or_else(|error| {
                        fail_init(
                            &request_id,
                            -32602,
                            format!("runtime_options.openai_live composition failed: {error}"),
                        )
                    }),
                );
                    capability_provider =
                        Some(meerkat_mobkit::live_wiring::LiveCapabilityProvider::public(
                            Arc::new(live_agent_factory.clone()),
                            public.realm.clone(),
                            open_authority,
                            transport,
                            mob_mcp_state,
                            publisher,
                        ));
                }
                capability_provider
                    .unwrap_or_else(meerkat_mobkit::live_wiring::LiveCapabilityProvider::disabled)
            };
            #[cfg(feature = "openai-live")]
            let handler = meerkat_mobkit::live_wiring::live_rpc_handler_with_capabilities(
                live_ctx,
                live_service,
                live_machine,
                capability_provider,
            );
            #[cfg(not(feature = "openai-live"))]
            let handler =
                meerkat_mobkit::live_wiring::live_rpc_handler(live_ctx, live_service, live_machine);
            (app, Some(handler))
        } else {
            (app, None)
        };
    let http_server =
        http_binding.serve(app.layer(axum::Extension(console_voice_controller.clone())));

    // 8. Send init response via stdout channel
    let loaded_modules = runtime.loaded_modules().await;
    let runtime_origin = if is_workspace_config {
        "workspace_config"
    } else {
        "fallback_minimal"
    };
    let runtime_fingerprint = {
        let mut hasher = Sha256::new();
        hasher.update(mob_config_toml.as_bytes());
        hasher.update(
            serde_json::to_string(&loaded_modules)
                .unwrap_or_default()
                .as_bytes(),
        );
        format!("{:x}", hasher.finalize())
    };
    let identity_bootstrap = identity_ctx
        .as_ref()
        .map(|ctx| ctx.runtime.identity_bootstrap_status());
    let init_result = json!({
        "http_base_url": http_base_url,
        // Proxy-facing base the launch advertised through
        // runtime_options.http_public_base_url; null when none was
        // declared. Never bound. Wire-additive.
        "http_public_base_url": http_public_base_url,
        "loaded_modules": loaded_modules,
        "contract_version": MOBKIT_CONTRACT_VERSION,
        "runtime_origin": runtime_origin,
        "runtime_fingerprint": runtime_fingerprint,
        "identity_bootstrap": identity_bootstrap,
        // Private transport capability. SDKs use this to avoid sending
        // the shutdown control method to older/custom gateways that may
        // not implement normal JSON-RPC method-not-found semantics.
        "stdio_shutdown_handshake": true,
        // Complete, bounded host wait for the private shutdown request.
        // SDKs keep callback admission and stdin alive for this horizon.
        "stdio_shutdown_horizon_ms": GATEWAY_SHUTDOWN_HORIZON_MS,
        // Dialable address of the cross-mob control listener when the
        // gateway was launched with --control-listen (`tcp://ip:port`
        // with the real kernel-assigned port for host:0 binds, or
        // `uds:///path`); null otherwise. Peers put this address in
        // their contact directories. Wire-additive: older SDKs ignore
        // unknown result fields.
        "control_listen_address": control_listen_address,
    });
    if shutdown_requested.load(Ordering::Acquire) {
        abort_init_for_shutdown(Some(runtime.as_ref()), &mut rpc_rx, init_progress).await;
    }
    // Ordinary requests are admitted from here on: the dispatch loop below
    // drains them. Opened before readiness is reported, so a request the SDK
    // sends right after the settlement is queued, not refused.
    dispatch_open.store(true, Ordering::Release);
    // A legacy SDK gets its single response; an accepted init settles `ready`.
    let init_response = match init_session() {
        Some(session) => {
            session.settled.store(true, Ordering::Release);
            session.ready_line(init_result)
        }
        None => json!({ "jsonrpc": "2.0", "id": request_id, "result": init_result }),
    };
    let _ = stdout_tx
        .send(GatewayStdoutLine::plain(
            serde_json::to_string(&init_response)
                .unwrap_or_else(|_| r#"{"error":"serialization failed"}"#.to_string()),
        ))
        .await;

    // 9. RPC dispatch loop: each request runs on its own task. The loop must
    // never await a handler inline — a turn-running RPC can block on a
    // Python/TS callback round-trip, and the host may issue further RPCs
    // (e.g. mobkit/agent_memory/recall) from INSIDE that callback. With
    // sequential dispatch those reentrant requests starve behind the turn
    // until the callback times out (downstream app recall deadlock). Both SDK
    // transports match responses by id, and the HTTP surface already serves
    // the same methods concurrently, so completion order is free.
    let identity_ctx = identity_ctx.map(Arc::new);
    let http_base_url_shared: Arc<str> = http_base_url.clone().into();
    let mut gateway_shutdown = None;
    let dispatch_end = {
        let mut inflight = tokio::task::JoinSet::new();
        let dispatch_end = loop {
            let request_line = tokio::select! {
                line = rpc_rx.recv() => line,
                // SIGINT *and* SIGTERM: a container stop sends SIGTERM, and
                // waiting on ctrl_c alone meant the graceful path never ran
                // on an ordinary deploy, leaving the schedule executor lease
                // held for up to its duration. See
                // `meerkat_mobkit::shutdown_signal`.
                signal = meerkat_mobkit::shutdown_signal::shutdown_signal() => {
                    break DispatchLoopEnd::Signal(signal);
                },
            };
            let Some(request_line) = request_line else {
                // The stdin reader closed the request channel (EOF or a read
                // error; it logged which).
                break DispatchLoopEnd::StdinClosed;
            };
            if let Ok(message) = serde_json::from_str::<Value>(&request_line)
                && let Some(request) = gateway_shutdown_request(&message)
            {
                gateway_shutdown = Some(request);
                break DispatchLoopEnd::SdkShutdownHandshake;
            }
            let request_line =
                apply_gateway_runtime_config_to_request(&request_line, &gateway_options.gating);
            let runtime = runtime.clone();
            let stdout_tx = stdout_tx.clone();
            let identity_ctx = identity_ctx.clone();
            let http_base_url = http_base_url_shared.clone();
            let live_rpc = live_rpc.clone();
            let gateway_detached_jobs = gateway_detached_jobs.clone();
            let callback_bridge = bridge.clone();
            inflight.spawn(async move {
                let response = match handle_callback_job_rpc(
                    &request_line,
                    gateway_detached_jobs.as_ref(),
                    &callback_bridge,
                )
                .await
                {
                    Some(response) => GatewayStdoutLine::plain(response),
                    None => GatewayStdoutLine::delivery(
                        meerkat_mobkit::rpc::handle_unified_rpc_json_with_live_arc_delivery(
                            &runtime,
                            &request_line,
                            timeout,
                            Some(http_base_url.as_ref()),
                            identity_ctx.as_deref(),
                            live_rpc.as_ref(),
                        )
                        .await,
                    ),
                };
                if !response.is_empty() {
                    let _ = stdout_tx.send(response).await;
                }
            });
            // Reap completed handlers so the set does not grow unbounded.
            while inflight.try_join_next().is_some() {}
        };
        // Name the branch that ended admission BEFORE the drain and the
        // shutdown sequence, so a wedge in either still leaves the reason in
        // the log. `reason=` is one of `DispatchLoopEnd`'s tokens; `signal=`
        // is present only when the reason is `signal`.
        tracing::info!(
            reason = %dispatch_end,
            signal = dispatch_end.signal().map(tracing::field::display),
            "rpc_gateway dispatch loop ended; running graceful shutdown"
        );
        // EOF/Ctrl-C makes further callback responses impossible (or asks us
        // to stop immediately), so wake callback waiters. An explicit SDK
        // shutdown handshake is different: stdin stays open and callback
        // admission must survive until runtime-owned provider cleanup ends.
        if gateway_shutdown.is_none() {
            bridge.close().await;
        }
        // EOF closes request admission. Give ordinary handlers a bounded
        // response grace, then abort their outer waiters so runtime shutdown
        // can begin promptly. Identity materialization/delivery transactions
        // are independently owned by the runtime foreground supervisor and
        // are cancelled or joined below at their explicit cleanup boundary.
        let drain = async { while inflight.join_next().await.is_some() {} };
        if tokio::time::timeout(GATEWAY_RPC_DRAIN_TIMEOUT, drain)
            .await
            .is_err()
        {
            inflight.shutdown().await;
        }
        dispatch_end
    };
    let interrupted_with_open_stdin = matches!(dispatch_end, DispatchLoopEnd::Signal(_));

    // 10. Graceful shutdown: stop HTTP admission, bound the outer-handler
    // drain, then let the runtime cancel/join identity-owned transactions.
    // Dispatch can also end on Ctrl-C while stdin is still open. Stop the
    // reader now unless the SDK handshake must continue routing callback
    // responses through runtime shutdown. Aborting an already-completed EOF
    // reader is harmless.
    if gateway_shutdown.is_none() {
        stdin_reader.abort();
    }
    // Release the schedule executor lease on the graceful path.
    //
    // `ScheduleHostHandle::shutdown` is what calls
    // `driver.release_executor_lease()`. Dropping the handle only SIGNALS the
    // supervisor; tokio gives no guarantee that the woken supervisor is polled
    // between main's completion and runtime teardown, so the release may never
    // run. Both gateways previously bound `_schedule_host` and relied on Drop,
    // which left the lease row held: measured on a production store as all four
    // of owner_id / lease_token / acquired_at_ms / expires_at_ms still set 57s
    // past a CLEAN exit.
    //
    // Cost of leaving it held, from `shutdown_signal`: the replacement process
    // gets `AcquireScheduleExecutorLeaseOutcome::Busy`, its tick returns without
    // calling `claim_due_occurrences`, and SCHEDULES DO NOT FIRE for up to
    // `lease_duration` (60s) after every restart. The claim watchdog cannot see
    // it - its overdue threshold is longer than the window.
    //
    // This runs BEFORE `composition.shutdown()` because releasing the lease is a
    // store write, and the composition teardown is what closes the store.
    if let Some(schedule_host) = schedule_host {
        schedule_host.shutdown().await;
    }

    let shutdown = composition
        .shutdown(
            http_server,
            || async move {
                if let Err(error) = console_voice_controller.shutdown().await {
                    tracing::warn!(%error, "console voice shutdown did not attest complete cleanup");
                }
            },
            || async {},
        )
        .await;
    let runtime_shutdown = shutdown.runtime;
    bridge.close().await;
    if let Some(request) = gateway_shutdown {
        let response = gateway_shutdown_response(request.response_id, runtime_shutdown.as_ref());
        if let Ok(line) = serde_json::to_string(&response) {
            let _ = stdout_tx.send(GatewayStdoutLine::plain(line)).await;
        }
        // The SDK closes stdin after receiving the response. Abort our Tokio
        // reader task now; that close also releases Tokio's blocking stdin
        // helper before the runtime itself is dropped.
        stdin_reader.abort();
    }
    drop(stdout_tx);
    // Runtime and callback objects intentionally retain sender clones until
    // function exit. Signal the writer explicitly after all producers have
    // quiesced so it drains queued responses without waiting for channel
    // ownership to disappear.
    let _ = stdout_shutdown_tx.send(());
    let _ = stdin_reader.await;
    if tokio::time::timeout(GATEWAY_STDOUT_DRAIN_TIMEOUT, &mut stdout_writer)
        .await
        .is_err()
    {
        stdout_writer.abort();
        let _ = stdout_writer.await;
    }
    drop(_temp_dir);
    // The closing bookend to the "dispatch loop ended" line: if this one is
    // missing from a log that has the first, the shutdown sequence wedged.
    tracing::info!(
        reason = %dispatch_end,
        signal = dispatch_end.signal().map(tracing::field::display),
        "rpc_gateway shutdown complete; exiting"
    );
    if interrupted_with_open_stdin {
        // Tokio's stdin adapter performs a blocking read that cannot be
        // cancelled by aborting its task. All MobKit/runtime/stdout cleanup is
        // complete above; exit explicitly so the runtime destructor does not
        // wait forever for that helper thread while stdin remains open.
        std::process::exit(0);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().skip(1).any(|a| a == "--version" || a == "-V") {
        println!(
            "rpc_gateway {} (meerkat-mobkit SDK stdin-RPC gateway)",
            env!("CARGO_PKG_VERSION")
        );
        return;
    }
    // --control-listen <tcp://host:port | uds:///path>: bind the cross-mob
    // control listener so remote gateways can wire/unwire/inject/lookup
    // members of this runtime. Mirrors the mobkit_gateway flag; validated
    // here so a typo is a launch error, not a silently ignored flag. The
    // bound address is reported back in the mobkit/init response as
    // `control_listen_address` (important for tcp://host:0 ephemeral ports).
    let control_listen =
        match meerkat_mobkit::gateway_composition::parse_control_listen_arg(&args[1..]) {
            Ok(value) => value,
            Err(message) => {
                eprintln!("{message}");
                std::process::exit(2);
            }
        };
    if args.iter().any(|a| a == "--persistent") {
        run_persistent(control_listen);
    } else {
        if control_listen.is_some() {
            eprintln!(
                "--control-listen requires --persistent (the single-shot gateway hosts no long-lived runtime to serve control RPC)"
            );
            std::process::exit(2);
        }
        run_single_shot(&args[1..]);
    }
}

// ---------------------------------------------------------------------------
// §8.5 steward runtime bridges
// ---------------------------------------------------------------------------

/// Late-bound Arc'd runtime for the steward's gating/conflict bridges: the
/// engine is constructed during identity-first init, before
/// `Arc::new(runtime)` exists; the cell binds right after.
#[derive(Clone, Default)]
struct StewardLateRuntime(Arc<tokio::sync::OnceCell<Arc<UnifiedRuntime>>>);

impl StewardLateRuntime {
    fn bind(&self, runtime: Arc<UnifiedRuntime>) {
        let _ = self.0.set(runtime);
    }

    fn get(&self) -> Option<Arc<UnifiedRuntime>> {
        self.0.get().cloned()
    }
}

/// Mob-purpose context from the hosted mob's id plus the restored roster's
/// labels (no mob-level purpose field exists — verified; a `mob_purpose`
/// or `purpose` label on any member spec is adopted as the mob's purpose).
struct GatewayMobPurposeSource {
    mob: String,
    roster: Arc<std::sync::Mutex<Vec<meerkat_mobkit::identity_first::DurableAgentSpec>>>,
}

impl meerkat_mobkit::MobPurposeSource for GatewayMobPurposeSource {
    fn mob_contexts(&self) -> Vec<meerkat_mobkit::memory::steward::MobContext> {
        let roster = self.roster.lock().unwrap_or_else(|err| err.into_inner());
        let purpose = roster.iter().find_map(|spec| {
            spec.labels
                .get("mob_purpose")
                .or_else(|| spec.labels.get("purpose"))
                .cloned()
        });
        let member_labels = roster
            .iter()
            .map(|spec| (spec.identity.as_str().to_string(), spec.labels.clone()))
            .collect();
        vec![meerkat_mobkit::memory::steward::MobContext {
            mob: self.mob.clone(),
            purpose,
            member_labels,
        }]
    }
}

/// Quarantine-promotion gate enqueue over the runtime's gating engine
/// (§10.2): risk tier R3, so the evaluation mints a pending entry the
/// operator decides through the existing console/RPC gating flow.
struct GatewayMemoryGatingBridge {
    runtime: StewardLateRuntime,
}

#[async_trait]
impl meerkat_mobkit::MemoryGatingBridge for GatewayMemoryGatingBridge {
    async fn enqueue_promotion_gate(
        &self,
        realm: &str,
        description: &str,
        entity: &str,
        topic: &str,
    ) -> Result<String, String> {
        let Some(runtime) = self.runtime.get() else {
            return Err("runtime not yet bound".to_string());
        };
        let result = runtime
            .evaluate_gating_action(meerkat_mobkit::runtime::GatingEvaluateRequest {
                action: description.to_string(),
                actor_id: format!("memory-steward:{realm}"),
                risk_tier: meerkat_mobkit::runtime::GatingRiskTier::R3,
                rationale: Some(
                    "memory steward quarantine promotion (agent-memory §10.2)".to_string(),
                ),
                requested_approver: None,
                approval_recipient: None,
                approval_channel: None,
                approval_timeout_ms: None,
                entity: Some(entity.to_string()),
                topic: Some(topic.to_string()),
            })
            .await;
        result.pending_id.ok_or_else(|| {
            format!(
                "gating evaluation returned outcome {:?} without a pending entry{}",
                result.outcome,
                result
                    .fallback_reason
                    .as_deref()
                    .map(|reason| format!(" ({reason})"))
                    .unwrap_or_default()
            )
        })
    }
}

/// Contradiction bridge (§8.5): dream findings with operational
/// consequence land as `MemoryConflictSignal`s in the operational ledger,
/// where gating's R2/R3 conflict probe reads them. Fire-and-forget.
struct GatewayMemoryConflictBridge {
    runtime: StewardLateRuntime,
    handle: tokio::runtime::Handle,
}

impl meerkat_mobkit::MemoryConflictBridge for GatewayMemoryConflictBridge {
    fn emit_conflict(&self, entity: &str, topic: &str, reason: &str) {
        let Some(runtime) = self.runtime.get() else {
            tracing::warn!(
                entity,
                topic,
                "memory conflict bridge: runtime not yet bound"
            );
            return;
        };
        let entity = entity.to_string();
        let topic = topic.to_string();
        let reason = reason.to_string();
        self.handle.spawn(async move {
            let request = meerkat_mobkit::runtime::MemoryIndexRequest {
                entity: entity.clone(),
                topic: topic.clone(),
                store: None,
                fact: None,
                metadata: None,
                conflict: Some(true),
                conflict_reason: Some(reason),
            };
            if let Err(err) = runtime.memory_index(request).await {
                tracing::warn!(
                    entity,
                    topic,
                    error = ?err,
                    "memory conflict bridge: conflict signal write failed"
                );
            }
        });
    }
}
