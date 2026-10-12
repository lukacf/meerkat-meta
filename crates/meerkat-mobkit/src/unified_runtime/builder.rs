//! Builder for constructing a configured UnifiedRuntime instance.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use meerkat_client::LlmClient;
use meerkat_mob::{MobDefinition, MobStorage, SpawnMemberSpec};

use crate::console_aggregator::{ConsoleLogStore, InMemoryConsoleLogStore, SqliteConsoleLogStore};
use crate::contact_directory::ContactDirectory;
pub use crate::identity_first::IdentityBootstrapMode;
use crate::identity_first::{
    AgentCustomizer, AgentMemoryConfig, AgentMemoryCustomizer, AgentMemoryProvider,
    AgentMemoryRuntimeInjector, AgentRuntimeServices, ContinuitySessionStoreAdapter,
    DurabilityPolicy, IdentityFirstRuntimeContext, IdentityRuntime, IdentityRuntimeConfig,
    LocalContinuityStore, LocalLeaseProvider, RosterContext, RosterProvider, TopologyProvider,
};
use crate::mob_handle_runtime::{
    CapabilityFlags, MobBootstrapOptions, MobBootstrapSpec, SessionHook,
};
use crate::runtime::{
    InMemoryMetadataStore, PersistentMetadataStore, RuntimeOptions, SqliteMetadataStore,
};
use crate::storage_layout::MobKitStorageLayout;
use crate::types::{EventEnvelope, MobKitConfig, UnifiedEvent};

use super::edge_types::{Discovery, EdgeDiscovery, PreSpawnHook};
use super::types::{
    UnifiedRuntimeBootstrapError, UnifiedRuntimeBuilderError, UnifiedRuntimeBuilderField,
};
use super::{
    DEFAULT_DRAIN_TIMEOUT, ErrorHook, EventLogConfig, PostReconcileHook, PostSpawnHook,
    UnifiedRuntime, discovery_spec_to_spawn_spec,
};

/// How the mob definition is supplied to the builder.
pub(crate) enum DefinitionSource {
    Inline(Box<MobDefinition>),
    TomlPath(PathBuf),
}

/// Default max concurrent sessions for builder-created session services.
const DEFAULT_MAX_SESSIONS: usize = 64;

/// Default builder timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default)]
pub struct UnifiedRuntimeBuilder {
    // --- Legacy path (mob_spec directly) ---
    mob_spec: Option<MobBootstrapSpec>,

    // --- New convenience path ---
    definition_source: Option<DefinitionSource>,
    persistent_state_path: Option<PathBuf>,
    /// Host-declared `rkat` scope for `SessionRepairRequired` commands.
    session_repair_scope: Option<crate::identity_first::SessionRepairScope>,
    /// Root `continuity_from_state_dir` opened — pinned in CANONICAL form at
    /// open time — retained so `build()` can refuse a silent authority fork:
    /// session authority in one directory's continuity.sqlite3 with
    /// runtime/blob/workgraph stores in another is exactly the split-storage
    /// class the layout's twin detection refuses elsewhere. Canonical because
    /// the gate compares physical identity, not spelling: a relative path
    /// re-resolved under a later working directory, or a symlink retargeted
    /// between open and build, must not slip past as a lexical match.
    continuity_state_dir: Option<PathBuf>,
    session_hook: Option<Arc<dyn SessionHook>>,
    custom_session_store: Option<Arc<dyn meerkat::SessionStore>>,
    meerkat_config: Option<meerkat::Config>,
    /// Host-level compaction policy composed over `meerkat_config`'s
    /// compaction slot at spec-resolve time. Separate from `meerkat_config`
    /// so tuning compaction does not require an embedder to author a whole
    /// `meerkat::Config`, and so the one knob that actually bounds transcript
    /// growth is reachable by name.
    compaction_policy: Option<meerkat_core::config::CompactionRuntimeConfig>,
    default_llm_client: Option<Arc<dyn LlmClient>>,
    max_sessions: Option<usize>,
    capability_flags: CapabilityFlags,

    /// Refuse to build when the session store would persist whole session
    /// documents per turn (see
    /// [`require_incremental_session_persistence`](Self::require_incremental_session_persistence)).
    require_incremental_session_persistence: bool,

    // --- Identity-first external path ---
    continuity_store: Option<Arc<dyn crate::identity_first::contracts::ContinuityStore>>,
    lease_provider: Option<Arc<dyn crate::identity_first::contracts::LeaseProvider>>,
    roster_provider: Option<Arc<dyn RosterProvider>>,
    topology_provider: Option<Arc<dyn TopologyProvider>>,
    agent_customizer: Option<Arc<dyn AgentCustomizer>>,
    agent_memory_provider: Option<Arc<dyn AgentMemoryProvider>>,
    agent_memory_config: Option<AgentMemoryConfig>,
    agent_memory_profile_policy: BTreeMap<meerkat_mob::ProfileName, bool>,
    /// Named Rust tool bundles added to the resolved or supplied mob spec,
    /// each with its availability to child mobs.
    tool_bundles: BTreeMap<
        String,
        (
            Arc<dyn meerkat_core::AgentToolDispatcher>,
            meerkat_mob_mcp::ChildToolBundleAvailability,
        ),
    >,
    /// Child mob application tool policy added to the mob spec.
    child_application_tool_policy: Option<meerkat_core::ApplicationToolPolicyBinding>,
    #[cfg(feature = "extension-state")]
    tool_bundle_factories: BTreeMap<
        String,
        (
            crate::extension_state::ToolBundleRequirements,
            Arc<dyn crate::extension_state::ToolBundleFactory>,
        ),
    >,
    #[cfg(feature = "extension-state")]
    extension_store: Option<crate::storage_provider::OpenedExtensionState>,
    agent_memory_engines: Option<crate::memory_wiring::MemoryEnginesConfig>,
    identity_bootstrap_mode: IdentityBootstrapMode,
    identity_bootstrap_mode_configured: bool,
    identity_runtime_instance_id: Option<String>,
    scratch_dir: Option<PathBuf>,
    blob_store: Option<Arc<dyn meerkat_core::BlobStore>>,
    binary_blob_store: Option<Arc<dyn crate::blob_store::BinaryBlobStore>>,
    ephemeral_blobs: bool,
    ephemeral_runtime_store: bool,
    schedule_store: Option<Arc<dyn meerkat::ScheduleStore>>,
    workgraph_store: Option<Arc<dyn meerkat::WorkGraphStore>>,
    storage_provider: Option<Arc<dyn crate::storage_provider::MobKitStorageProvider>>,
    // Materialized from `storage_provider` (M4b): the meerkat-level bundle
    // opened through `meerkat_provider()` for non-disk backends, and the
    // provider's per-slot durability declarations retained for the census.
    provider_meerkat_stores: Option<crate::storage_provider::ProviderMeerkatStores>,
    provider_slot_census: Vec<crate::storage_health::StorageSlotSummary>,
    console_log_store: Option<Arc<dyn ConsoleLogStore>>,

    // --- Common fields ---
    module_config: Option<MobKitConfig>,
    module_agent_events: Vec<EventEnvelope<UnifiedEvent>>,
    timeout: Option<Duration>,
    options: RuntimeOptions,
    post_spawn_hook: Option<PostSpawnHook>,
    post_reconcile_hook: Option<PostReconcileHook>,
    error_hook: Option<ErrorHook>,
    event_log_config: Option<EventLogConfig>,
    drain_timeout: Option<Duration>,
    discovery: Option<Box<dyn Discovery>>,
    /// Behind a mutex only so the builder is `Sync`: the public
    /// [`PreSpawnHook`] is `Send` but not `Sync`, and `build` holds `&self`
    /// across awaits, so a plain field made `build()`'s future `!Send`. The
    /// hook is only ever moved in and out by value; the lock is never
    /// contended.
    pre_spawn_hook: std::sync::Mutex<Option<PreSpawnHook>>,
    edge_discovery: Option<Box<dyn EdgeDiscovery>>,
    contact_directory: Option<ContactDirectory>,
    control_listen: Option<String>,
    control_grants: Option<crate::runtime::cross_mob_control::ControlGrantTable>,
    persistent_metadata: Option<Arc<dyn PersistentMetadataStore>>,
    access_controller: Option<crate::access::AccessController>,
    topology_control_policy: crate::topology_control::TopologyControlPolicy,
    live: Option<super::live_compose::LiveOptions>,
    #[cfg(feature = "openai-live")]
    console_voice: Option<crate::public_live_config::PublicLiveRegistration>,
}

impl UnifiedRuntimeBuilder {
    /// Enable standard MCP Apps capabilities for a host with an isolated UI
    /// renderer. The stock Console also needs `mcp_apps_sandbox_url` configured.
    pub fn mcp_apps(mut self, enabled: bool) -> Self {
        self.capability_flags.mcp_apps = enabled;
        self
    }

    // -----------------------------------------------------------------------
    // New convenience API
    // -----------------------------------------------------------------------

    /// Set the mob definition from an inline `MobDefinition`.
    pub fn definition(mut self, def: MobDefinition) -> Self {
        self.definition_source = Some(DefinitionSource::Inline(Box::new(def)));
        self
    }

    /// Set the mob definition from a TOML file path.
    pub fn definition_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.definition_source = Some(DefinitionSource::TomlPath(path.into()));
        self
    }

    /// Enable persistent state at the given path. When set, the builder
    /// creates a `SqliteSessionStore`, runtime store, metadata store, console
    /// log store, and binary blob store under this directory. Mob storage stays
    /// in-memory. When not set, the builder uses an ephemeral session service
    /// with an auto-created temp directory.
    pub fn persistent_state(mut self, path: impl Into<PathBuf>) -> Self {
        self.persistent_state_path = Some(path.into());
        self
    }

    /// Override where the `rkat session repair-wholeblob` commands in a
    /// `SessionRepairRequired` hold point. By default the builder derives the
    /// scope from the runtime store it opened (`persistent_state` opens
    /// `<state_dir>/runtime.sqlite`, rendered as `--runtime-store`; a
    /// composite storage provider's meerkat-level realm renders as
    /// `--state-root <state_dir> --realm mobkit`); a build with no persistent
    /// runtime store declares no scope and the commands carry explicit
    /// `<state-root>` / `<realm>` placeholders. MobKit never guesses a path.
    pub fn session_repair_scope(
        mut self,
        scope: crate::identity_first::SessionRepairScope,
    ) -> Self {
        self.session_repair_scope = Some(scope);
        self
    }

    /// Set a session lifecycle hook.
    pub fn session_hook(mut self, hook: Arc<dyn SessionHook>) -> Self {
        self.session_hook = Some(hook);
        self
    }

    /// Set a custom session store. When set, the builder uses this store
    /// instead of creating a default one. Works with both `.persistent_state()`
    /// (overrides the auto-created SQLite store) and ephemeral builds
    /// (provides durable sessions without local mob storage).
    pub fn session_store(mut self, store: Arc<dyn meerkat::SessionStore>) -> Self {
        self.custom_session_store = Some(store);
        self
    }

    /// Set the Meerkat agent factory configuration used by builder-created
    /// session services.
    ///
    /// Applications with long-lived durable coordinator agents can use this to
    /// tune session compaction and other factory-level Meerkat behavior without
    /// constructing a full `MobBootstrapSpec` by hand.
    pub fn meerkat_config(mut self, config: meerkat::Config) -> Self {
        self.meerkat_config = Some(config);
        self
    }

    /// Declare the host-level session-compaction policy for every session
    /// this runtime builds.
    ///
    /// Meerkat always installs a compactor on the built session path; what
    /// this declares is *when it fires*. Without a declaration the threshold
    /// is meerkat's model-aware default — `context_window * 4 / 5`, i.e.
    /// `840_000` tokens on a million-token model, which in practice means
    /// "never". Declaring `auto_compact_threshold` pins the number instead
    /// (see [`crate::compaction_policy`] for the full precedence rules; a mob
    /// profile's own `auto_compact_threshold` still wins per member).
    ///
    /// Composes over [`meerkat_config`](Self::meerkat_config): this
    /// declaration replaces that config's compaction slot and leaves every
    /// other field alone. Invalid declarations (a zero threshold) surface as
    /// a build-time `ConflictingConfiguration` error, never as a silently
    /// ignored knob.
    pub fn compaction(mut self, policy: meerkat_core::config::CompactionRuntimeConfig) -> Self {
        self.compaction_policy = Some(policy);
        self
    }

    /// Set the default LLM client (used for test stubs).
    pub fn default_llm_client(mut self, client: Arc<dyn LlmClient>) -> Self {
        self.default_llm_client = Some(client);
        self
    }

    /// Set the maximum number of active sessions for builder-created session
    /// services.
    ///
    /// This only applies to the definition-based path. A legacy `.mob_spec()`
    /// supplies its own already-built session service and capacity.
    pub fn max_sessions(mut self, max_sessions: usize) -> Self {
        self.max_sessions = Some(max_sessions);
        self
    }

    /// Set an external `ContinuityStore` for the identity-first path.
    ///
    /// Supply [`lease_provider`](Self::lease_provider) with it — the pair is
    /// one substrate (fencing floors are store-coupled). May coexist with
    /// `persistent_state()` (M4): the external substrate stays the identity
    /// and session authority while the state directory supplies the
    /// meerkat-shared local stores.
    pub fn continuity_store(
        mut self,
        store: Arc<dyn crate::identity_first::contracts::ContinuityStore>,
    ) -> Self {
        self.continuity_store = Some(store);
        self
    }

    /// Opt in to strict session persistence: `build()` refuses with
    /// [`UnifiedRuntimeBuilderError::SessionStoreNotIncremental`] when the
    /// session store does not advertise meerkat's incremental persistence,
    /// instead of starting on the whole-blob fallback (one warning, then the
    /// whole session document written at every turn boundary).
    ///
    /// For an identity-first launch this means the continuity store must
    /// serve [`ContinuityStore::as_incremental_sessions`](crate::identity_first::contracts::ContinuityStore::as_incremental_sessions).
    /// Off by default.
    pub fn require_incremental_session_persistence(mut self, required: bool) -> Self {
        self.require_incremental_session_persistence = required;
        self
    }

    /// Set an external `LeaseProvider` for the identity-first path.
    ///
    /// Supply [`continuity_store`](Self::continuity_store) with it; see
    /// there for the `persistent_state()` coexistence semantics.
    pub fn lease_provider(
        mut self,
        provider: Arc<dyn crate::identity_first::contracts::LeaseProvider>,
    ) -> Self {
        self.lease_provider = Some(provider);
        self
    }

    /// Open the state directory's identity substrate and install BOTH halves,
    /// composing the SHIPPED GATEWAY's session topology on the builder.
    ///
    /// # Why this exists (doctrine D2, and a real coverage hole)
    ///
    /// `persistent_state()` + `roster_provider()` alone does NOT reproduce the
    /// gateway. The gateway opens the local identity substrate FIRST and makes
    /// its [`ContinuitySessionStoreAdapter`] meerkat's `SessionStore`, so ALL
    /// session I/O — heads, strand rows, resume reads — rides
    /// `continuity.sqlite3` (see `bin/rpc_gateway.rs`, the
    /// `identity_session_store_adapter` binding). The builder, by contrast,
    /// only installs that adapter when an EXTERNAL continuity store is
    /// supplied; on the plain `persistent_state()` arm it opens a
    /// `SqliteSessionStore` for sessions and opens the local continuity store
    /// later, for identity metadata only. A test written against that arm
    /// exercises neither the continuity adapter nor the head-canonical resume
    /// path, and will pass while proving nothing.
    ///
    /// This method closes that hole for embedders and tests that WANT the
    /// gateway shape, through the same [`crate::gateway_wiring`] seam the
    /// binaries use — including the fencing floor, which must resume above the
    /// persisted high-water or restore aborts every restart with a stale
    /// token.
    ///
    /// # Not a default
    ///
    /// It is deliberately opt-in. Flipping the plain `persistent_state()` arm
    /// over would move an existing deployment's session authority from
    /// `sessions.sqlite3` to an empty `continuity.sqlite3` — every durable
    /// member would resume onto nothing. That is a data migration, not a
    /// wiring default.
    ///
    /// Pair with [`persistent_state`](Self::persistent_state) pointing at the
    /// SAME directory and with [`roster_provider`](Self::roster_provider).
    ///
    /// # Errors
    ///
    /// Fails when the state directory cannot be created, when the continuity
    /// slot cannot be resolved (file-name twins), or when the substrate cannot
    /// be opened — never degrades to a zero fencing floor.
    pub async fn continuity_from_state_dir(
        mut self,
        state_dir: impl AsRef<std::path::Path>,
    ) -> Result<Self, UnifiedRuntimeBuilderError> {
        let state_dir = state_dir.as_ref();
        std::fs::create_dir_all(state_dir).map_err(|e| {
            UnifiedRuntimeBuilderError::Io(format!(
                "failed to create state directory at {}: {e}",
                state_dir.display()
            ))
        })?;
        // Pin the CANONICAL root at open time: the substrate is opened
        // against this physical directory NOW, and the same-root gate in
        // `build()` compares against what was actually opened. A relative
        // spelling re-resolved under a later working directory, or a
        // retargeted symlink, would otherwise fork session authority while
        // comparing equal lexically.
        let state_dir = std::fs::canonicalize(state_dir).map_err(|e| {
            UnifiedRuntimeBuilderError::Io(format!(
                "failed to canonicalize state directory at {}: {e}",
                state_dir.display()
            ))
        })?;
        let continuity_db = MobKitStorageLayout::with_injected_roots(state_dir.clone(), None)
            .continuity_db()?
            .path;
        let substrate = crate::gateway_wiring::open_identity_substrate(&continuity_db)
            .await
            .map_err(UnifiedRuntimeBuilderError::Io)?;
        self.continuity_store = Some(substrate.continuity_store);
        self.lease_provider = Some(substrate.lease_provider);
        self.continuity_state_dir = Some(state_dir);
        Ok(self)
    }

    /// Set the desired identity roster provider for identity-first bootstrap.
    pub fn roster_provider(mut self, provider: Arc<dyn RosterProvider>) -> Self {
        self.roster_provider = Some(provider);
        self
    }

    /// Set the managed topology provider for identity-first bootstrap and refresh.
    pub fn topology_provider(mut self, provider: Arc<dyn TopologyProvider>) -> Self {
        self.topology_provider = Some(provider);
        self
    }

    /// Register a named host-only Rust tool bundle for profiles'
    /// `tools.rust_bundles`. It is added to the mob spec this builder resolves
    /// or is given; see [`MobBootstrapSpec::register_tool_bundle`]. A name that
    /// the supplied spec already registers is refused at build.
    #[must_use]
    pub fn register_tool_bundle(
        self,
        name: impl Into<String>,
        dispatcher: Arc<dyn meerkat_core::AgentToolDispatcher>,
    ) -> Self {
        self.register_tool_bundle_with_availability(
            name,
            dispatcher,
            meerkat_mob_mcp::ChildToolBundleAvailability::HostOnly,
        )
    }

    /// Register a named Rust tool bundle with its availability to child
    /// mobs; see [`MobBootstrapSpec::register_tool_bundle_with_availability`].
    /// A name that the supplied spec already registers is refused at build.
    #[must_use]
    pub fn register_tool_bundle_with_availability(
        mut self,
        name: impl Into<String>,
        dispatcher: Arc<dyn meerkat_core::AgentToolDispatcher>,
        availability: meerkat_mob_mcp::ChildToolBundleAvailability,
    ) -> Self {
        self.tool_bundles
            .insert(name.into(), (dispatcher, availability));
        self
    }

    /// The application tool policy child mob members are built with; see
    /// [`MobBootstrapSpec::with_child_application_tool_policy`]. A supplied
    /// spec that already sets one is refused at build.
    #[must_use]
    pub fn child_application_tool_policy(
        mut self,
        binding: meerkat_core::ApplicationToolPolicyBinding,
    ) -> Self {
        self.child_application_tool_policy = Some(binding);
        self
    }

    /// Build an optional tool bundle from the configured provider's durable
    /// document capability. Profiles select its name through rust_bundles.
    #[cfg(feature = "extension-state")]
    pub fn register_tool_bundle_factory(
        mut self,
        name: impl Into<String>,
        requirements: crate::extension_state::ToolBundleRequirements,
        factory: Arc<dyn crate::extension_state::ToolBundleFactory>,
    ) -> Self {
        self.tool_bundle_factories
            .insert(name.into(), (requirements, factory));
        self
    }

    /// Set the identity-first build customizer.
    pub fn agent_customizer(mut self, customizer: Arc<dyn AgentCustomizer>) -> Self {
        self.agent_customizer = Some(customizer);
        self
    }

    /// Enable identity-first agent memory injection using the provided memory provider.
    pub fn agent_memory(
        mut self,
        provider: Arc<dyn AgentMemoryProvider>,
        config: AgentMemoryConfig,
    ) -> Self {
        self.agent_memory_provider = Some(provider);
        self.agent_memory_config = Some(config);
        self
    }

    /// Enable the FULL agent-memory stack (bundled SQLite store + the taint
    /// firewall + the enabled judgment-plane engines) — the same stack the
    /// rpc gateway assembles, reachable from the Rust builder (the operator
    /// deployment shape). Requires `persistent_state()`; the store lives
    /// under the layout's agent-memory root (canonical
    /// `<persistent_state>/agent-memory`, with a legacy
    /// `agent-memory-sqlite/` corpus honored where it lies).
    ///
    /// v1 boundaries (documented in `memory_wiring`): the Hygienist stays
    /// gateway-only; engines are driven by the member-event observe stream
    /// (their primary trigger path — the gateway's additional injector-side
    /// rotation hooks are not wired here yet); the steward dream runs on the
    /// in-process loop (no schedule host in library mode).
    pub fn persistent_agent_memory_stack(
        mut self,
        config: AgentMemoryConfig,
        engines: crate::memory_wiring::MemoryEnginesConfig,
    ) -> Self {
        self.agent_memory_config = Some(config);
        self.agent_memory_engines = Some(engines);
        self
    }

    /// Override identity-first memory tooling for a specific durable profile.
    ///
    /// Inline profiles inherit `profiles.<name>.tools.memory` automatically.
    /// Realm-referenced profiles are unresolved at the customizer boundary and
    /// therefore fail closed unless explicitly enabled with this method.
    pub fn agent_memory_for_profile(
        mut self,
        profile: impl Into<meerkat_mob::ProfileName>,
        enabled: bool,
    ) -> Self {
        self.agent_memory_profile_policy
            .insert(profile.into(), enabled);
        self
    }

    fn composed_agent_customizer(
        &self,
        memory_provider: Option<Arc<dyn AgentMemoryProvider>>,
    ) -> Option<Arc<dyn AgentCustomizer>> {
        match memory_provider {
            Some(provider) => Some(Arc::new(
                AgentMemoryCustomizer::wrap(
                    self.agent_customizer.clone(),
                    provider,
                    self.agent_memory_config.clone().unwrap_or_default(),
                )
                .with_profile_memory_policy(self.agent_memory_profile_policy.clone()),
            )),
            None => self.agent_customizer.clone(),
        }
    }

    /// Set how identity-first durable agents are materialized during build.
    pub fn identity_bootstrap_mode(mut self, mode: IdentityBootstrapMode) -> Self {
        self.identity_bootstrap_mode = mode;
        self.identity_bootstrap_mode_configured = true;
        self
    }

    /// Set the identity runtime instance id used when acquiring leases.
    pub fn identity_runtime_instance_id(mut self, id: impl Into<String>) -> Self {
        self.identity_runtime_instance_id = Some(id.into());
        self
    }

    /// Set a scratch directory for the external-authoritative path.
    ///
    /// Required when using `continuity_store()` + `lease_provider()`.
    pub fn scratch_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.scratch_dir = Some(path.into());
        self
    }

    /// Set an optional blob store for custom blob persistence.
    ///
    /// The same store is used for runtime image blobs and for MobKit's
    /// `/blobs/{id}` and `mobkit/blob/*` serving/upload paths.
    pub fn blob_store(mut self, store: Arc<dyn meerkat_core::BlobStore>) -> Self {
        self.blob_store = Some(store);
        self
    }

    /// Set an optional blob store in its raw-bytes [`crate::blob_store::BinaryBlobStore`] form
    /// (M4). Like [`blob_store`](Self::blob_store) but without the base64
    /// adapter round-trip on the HTTP `/blobs/{id}` and `mobkit/blob/*`
    /// byte-serving paths; the meerkat `BlobStore` face is adapted from it.
    /// Mutually exclusive with `blob_store()` — inject exactly one form.
    pub fn binary_blob_store(mut self, store: Arc<dyn crate::blob_store::BinaryBlobStore>) -> Self {
        self.binary_blob_store = Some(store);
        self
    }

    /// Declare that blobs are intentionally ephemeral (in-memory).
    ///
    /// Persistent-state builds fail closed when the blob slot resolves to a
    /// store that does not survive restart — a failed open of the local blob
    /// directory or an injected [`blob_store`](Self::blob_store) reporting
    /// `!is_persistent()`. This declaration makes in-memory blobs a
    /// configuration instead of an error (tests, demos), and is reported as
    /// `declared_ephemeral` on the health surfaces. Ephemeral launch modes
    /// (scratch/temp-dir) declare it implicitly.
    pub fn ephemeral_blobs(mut self, enabled: bool) -> Self {
        self.ephemeral_blobs = enabled;
        self
    }

    /// Declare that the runtime store is intentionally ephemeral
    /// (in-memory) on a persistent-state build.
    ///
    /// Persistent-state builds fail closed when `runtime.sqlite` cannot be
    /// opened — the former silent `InMemoryRuntimeStore` fallback (in which
    /// resume across restart and archive operations fail) is gone. This
    /// declaration makes the in-memory runtime store a configuration
    /// instead of an error (tests, demos), reported as `declared_ephemeral`
    /// in the per-slot storage census. Ephemeral launch modes declare it
    /// implicitly; without `persistent_state()` this is a no-op.
    pub fn ephemeral_runtime_store(mut self, enabled: bool) -> Self {
        self.ephemeral_runtime_store = enabled;
        self
    }

    /// Set an external [`ScheduleStore`](meerkat::ScheduleStore) (M4): the
    /// agent-facing schedule tools attach over the caller's store instead of
    /// the bundled SQLite file. Library mode wires no firing host — authored
    /// schedules become durable rows the embedder's own driver (or a gateway
    /// pointed at the same store) fires. Injection is a *foundation*: any
    /// shadow-scheduler deletion downstream requires a feature-parity audit
    /// first (multi-replica claims, timezone cron, jitter, delivery-policy
    /// handoffs).
    pub fn schedule_store(mut self, store: Arc<dyn meerkat::ScheduleStore>) -> Self {
        self.schedule_store = Some(store);
        self
    }

    /// Set an external [`WorkGraphStore`](meerkat::WorkGraphStore) (item 5):
    /// the agent-facing workgraph tools attach over the caller's store
    /// instead of the SQLite file beside `runtime.sqlite`. Injectable
    /// INDEPENDENTLY of continuity, schedule, lease, console and blob - that
    /// independence is the point of the item, replacing the previous
    /// all-or-nothing composition where a durable workgraph was reachable
    /// only by adopting a whole composite provider.
    ///
    /// Durability rides with the injector. Mobkit does NOT gate on
    /// [`WorkGraphStore::kind()`](meerkat::WorkGraphStore::kind), which is a
    /// backend-SHAPE tag and not a durability oracle: the path behind
    /// `Sqlite` is caller-supplied and says nothing about whether it
    /// survives a restart, and `Custom` says nothing either way.
    ///
    /// Two things an injected store does NOT get, both deliberate:
    /// - It is a MEERKAT-level slot, so it is absent from
    ///   `storage_provider::REQUIRED_MOBKIT_DURABILITY_DOMAINS` and from the
    ///   mobkit `RealmStoreSet`. On the provider path the workgraph rides
    ///   `provider_meerkat_stores` instead, and per-slot injection takes
    ///   precedence over it.
    /// - The cross-process admission sidecar is keyed on the STATE DIR, not
    ///   on the store. An injected backend can live anywhere, so the sidecar
    ///   serializes co-processes sharing a state dir rather than co-processes
    ///   sharing this store.
    pub fn workgraph_store(mut self, store: Arc<dyn meerkat::WorkGraphStore>) -> Self {
        self.workgraph_store = Some(store);
        self
    }

    /// Install a composite [`MobKitStorageProvider`] — the one-remote-bundle
    /// seam (M4). The provider's realm store set supplies the identity
    /// substrate (continuity + lease authority), console timeline, metadata,
    /// blobs, schedule store, and (when configured) the event log and agent
    /// memory, replacing the per-slot builder seams; supplying both the
    /// provider and an individual seam is a conflict. For a non-disk backend
    /// the composition also opens the provider's meerkat-level bundle
    /// through
    /// [`meerkat_provider`](crate::storage_provider::MobKitStorageProvider::meerkat_provider)
    /// (M4b), so runtime and workgraph authority land in the same backend
    /// instead of local files; the built-in disk backend keeps the flat
    /// local composition.
    ///
    /// Requires an identity-first configuration (`roster_provider()`): the
    /// provider's continuity store is the session authority. The realm root
    /// comes from `persistent_state()` or `scratch_dir()` (ephemeral local
    /// disk — the operator shape).
    ///
    /// [`MobKitStorageProvider`]: crate::storage_provider::MobKitStorageProvider
    pub fn storage_provider(
        mut self,
        provider: Arc<dyn crate::storage_provider::MobKitStorageProvider>,
    ) -> Self {
        self.storage_provider = Some(provider);
        self
    }

    /// Set a custom console log store.
    ///
    /// This lets applications pair a durable console timeline (for fast
    /// cursor-based history replay) with otherwise ephemeral mob state.
    pub fn with_console_log_store(mut self, store: Arc<dyn ConsoleLogStore>) -> Self {
        self.console_log_store = Some(store);
        self
    }

    /// Register the external live channel (`mobkit/live/*` over the live
    /// WebSocket router), the builder-path equivalent of the gateway's
    /// `runtime_options.live`.
    ///
    /// Requires a persistent session store ([`Self::session_store`] or
    /// [`Self::continuity_from_state_dir`]); `build()` fails closed with
    /// [`UnifiedRuntimeBuilderError::LiveCompose`] otherwise. The doors are
    /// composed by [`UnifiedRuntime::compose_live`] after the runtime is
    /// shared; [`UnifiedRuntime::build_reference_app_router`] does that and
    /// mounts the WebSocket router, so an embedder serving that router on
    /// its own listener gets `/live/ws` beside the console.
    pub fn live(mut self, options: super::live_compose::LiveOptions) -> Self {
        self.live = Some(options);
        self
    }

    /// Register console voice (gpt-live-1 through the authenticated console),
    /// the builder-path equivalent of the gateway's
    /// `runtime_options.console_voice`.
    ///
    /// Same persistent-store requirement as [`Self::live`]. The composed
    /// controller serves only authenticated console principals, so the
    /// decision state handed to [`UnifiedRuntime::build_reference_app_router`]
    /// must have `console.require_app_auth = true`; the router refuses to
    /// build otherwise. With [`Self::live`] registered as well, the two doors
    /// share one live context and take turns on its voice-path arbiter
    /// ("latest engaged wins"), exactly as in the gateway binaries.
    #[cfg(feature = "openai-live")]
    pub fn console_voice(
        mut self,
        registration: crate::public_live_config::PublicLiveRegistration,
    ) -> Self {
        self.console_voice = Some(registration);
        self
    }

    /// Enable or disable builtin tools (default: true).
    ///
    /// NOT a containment control for mob members. This sets the agent
    /// factory's runtime DEFAULT, and a default only applies where the
    /// per-session intent is `ToolCategoryOverride::Inherit`. A member built
    /// from an inline mob profile never inherits: `build_agent_config` maps
    /// every `profile.tools.*` boolean through
    /// `ToolCategoryOverride::from_effective`, which returns only
    /// `Enable`/`Disable` and never `Inherit`
    /// (`meerkat-core/src/session.rs:6656-6658`), and `resolve()` ignores the
    /// runtime default for those two (`:6626-6632`). So a profile declaring
    /// `tools.builtins = true` still gets builtins after `builtins(false)`.
    /// This flag governs sessions built through the same factory that are not
    /// profile-driven. For per-member containment set `profiles.<name>.tools`
    /// (or a `tool_access_policy`), not this.
    pub fn builtins(mut self, enabled: bool) -> Self {
        self.capability_flags.builtins = enabled;
        self
    }

    /// Enable or disable shell tool (default: true).
    ///
    /// A profile's `tools.shell` wins on the definition path; this is a
    /// factory default, not member containment. See [`Self::builtins`].
    pub fn shell(mut self, enabled: bool) -> Self {
        self.capability_flags.shell = enabled;
        self
    }

    /// Enable or disable mob tools (default: true).
    ///
    /// A profile's `tools.mob` wins on the definition path; see
    /// [`Self::builtins`]. Narrower still: the factory never reads its own
    /// mob default when composing, because `effective_mob` is derived from
    /// the build's mob operator authority context
    /// (`meerkat/src/factory.rs:5462-5467`).
    pub fn mob(mut self, enabled: bool) -> Self {
        self.capability_flags.mob = enabled;
        self
    }

    /// Enable or disable comms (default: true).
    ///
    /// A profile's `tools.comms` wins on the definition path; see
    /// [`Self::builtins`]. Note also that `tools.comms = false` is refused
    /// outright for mob members (`meerkat-mob/src/build.rs:170-173`), and the
    /// factory's own comms default is consulted only when the build carries
    /// no comms name, which the mob path always sets
    /// (`meerkat/src/factory.rs:4977-4981`, `meerkat-mob/src/build.rs:212`).
    pub fn comms(mut self, enabled: bool) -> Self {
        self.capability_flags.comms = enabled;
        self
    }

    /// Enable or disable memory tools (default: true).
    ///
    /// A profile's `tools.memory` wins on the definition path; this is a
    /// factory default, not member containment. See [`Self::builtins`].
    pub fn memory(mut self, enabled: bool) -> Self {
        self.capability_flags.memory = enabled;
        self
    }

    /// Force image-generation runtime substrate wiring.
    ///
    /// Definition-based builders also infer this from
    /// `profiles.<name>.tools.image_generation`; Meerkat owns the per-profile
    /// visibility decision.
    pub fn image_generation(mut self, enabled: bool) -> Self {
        self.capability_flags.image_generation = enabled;
        self
    }

    // -----------------------------------------------------------------------
    // Legacy API (preserved for backward compat)
    // -----------------------------------------------------------------------

    pub fn mob_spec(mut self, spec: MobBootstrapSpec) -> Self {
        self.mob_spec = Some(spec);
        self
    }

    pub fn module_config(mut self, config: MobKitConfig) -> Self {
        self.module_config = Some(config);
        self
    }

    pub fn module_agent_events(mut self, events: Vec<EventEnvelope<UnifiedEvent>>) -> Self {
        self.module_agent_events = events;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn runtime_options(mut self, options: RuntimeOptions) -> Self {
        self.options = options;
        self
    }

    pub fn post_spawn_hook(mut self, hook: PostSpawnHook) -> Self {
        self.post_spawn_hook = Some(hook);
        self
    }

    pub fn post_reconcile_hook(mut self, hook: PostReconcileHook) -> Self {
        self.post_reconcile_hook = Some(hook);
        self
    }

    /// Register the operational error hook.
    ///
    /// Failures the runtime detects after bootstrap (a spawn that failed, a
    /// checkpoint that did not persist, a stalled actor loop) have no caller
    /// to return to; they are emitted as typed [`crate::ErrorEvent`]s through
    /// this hook. The hook is fire-and-forget: it runs on a detached task, and
    /// a slow or failing hook cannot break the runtime. An attached
    /// identity-first runtime receives the same hook.
    ///
    /// Every event is also logged through `tracing` (ERROR, or INFO for
    /// `ErrorEvent::ActorLoopRecovered`) whether or not a hook is registered,
    /// and when this method was never called [`Self::build`] emits one WARN
    /// saying that nobody is listening. Both are `tracing` lines, so a library
    /// host must install a `tracing` subscriber before `build`; without one it
    /// sees neither the failures nor the notice. The gateway binaries install
    /// a stderr subscriber themselves; an embedded host does that itself.
    ///
    /// Hosts that construct the runtime through [`UnifiedRuntime::bootstrap`]
    /// instead of the builder register the hook with
    /// [`UnifiedRuntime::set_error_hook`].
    pub fn on_error(mut self, hook: ErrorHook) -> Self {
        self.error_hook = Some(hook);
        self
    }

    pub fn event_log(mut self, config: EventLogConfig) -> Self {
        self.event_log_config = Some(config);
        self
    }

    pub fn drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = Some(timeout);
        self
    }

    pub fn discovery(mut self, discovery: impl Discovery + 'static) -> Self {
        self.discovery = Some(Box::new(discovery));
        self
    }

    pub fn pre_spawn_hook(mut self, hook: PreSpawnHook) -> Self {
        *self
            .pre_spawn_hook
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(hook);
        self
    }

    pub fn edge_discovery(mut self, edge_discovery: impl EdgeDiscovery + 'static) -> Self {
        self.edge_discovery = Some(Box::new(edge_discovery));
        self
    }

    /// Set the contact directory for cross-mob address resolution.
    pub fn contact_directory(mut self, directory: ContactDirectory) -> Self {
        self.contact_directory = Some(directory);
        self
    }

    /// Bind a cross-mob control listener on startup so remote gateways can
    /// wire/unwire/inject/lookup members of this runtime.
    ///
    /// Accepts the contact-directory address spelling: `tcp://host:port`
    /// (port 0 binds an ephemeral port) or `uds:///path`. The concrete
    /// bound address is queryable after build via
    /// [`UnifiedRuntime::control_listener_advertised_address`].
    pub fn control_listen(mut self, addr: impl Into<String>) -> Self {
        self.control_listen = Some(addr.into());
        self
    }

    /// Authorize callers of the configured cross-mob control listener.
    ///
    /// Grants are bound to this runtime's mob id when the listener starts,
    /// so a signed request captured from one gateway cannot be replayed at
    /// another. Omitting this method is fail-closed: a configured listener
    /// still binds, but its empty grant table refuses every request.
    pub fn control_grants(
        mut self,
        grants: crate::runtime::cross_mob_control::ControlGrantTable,
    ) -> Self {
        self.control_grants = Some(grants);
        self
    }

    /// Install a persistent metadata store. Used for the structural-events
    /// subscription cursor — see `runtime::metadata::PersistentMetadataStore`.
    /// When unset, the builder defaults to an `InMemoryMetadataStore`, which
    /// is correct for in-memory mob deployments. Production gateways with a
    /// SQLite mob storage should pass `SqliteMetadataStore::open(path)`
    /// against the same database the mob uses, so the structural-events
    /// subscription can resume from the last-projected cursor on restart
    /// rather than jumping forward to "latest" and dropping events emitted
    /// between processes.
    pub fn persistent_metadata(mut self, store: Arc<dyn PersistentMetadataStore>) -> Self {
        self.persistent_metadata = Some(store);
        self
    }

    /// Install a pre-built access controller (ABAC enforcement for the
    /// console and SSE surfaces). Absent — the default — access control is
    /// off and every surface behaves exactly as before.
    pub fn access_controller(mut self, controller: crate::access::AccessController) -> Self {
        self.access_controller = Some(controller);
        self
    }

    /// Enable access control backed by a TOML file (conventionally
    /// `config/access.toml`). A missing file starts disabled; admin edits
    /// from the console persist back to the same path.
    pub fn access_control_file(
        mut self,
        path: impl Into<PathBuf>,
    ) -> Result<Self, crate::access::AccessConfigError> {
        self.access_controller = Some(crate::access::AccessController::load_or_default(path)?);
        Ok(self)
    }

    /// Configure the optional topology control plane. The default policy is
    /// disabled, single-operation, local-authority only.
    pub fn topology_control(
        mut self,
        policy: crate::topology_control::TopologyControlPolicy,
    ) -> Result<Self, crate::topology_control::TopologyControlError> {
        policy.validate()?;
        self.topology_control_policy = policy;
        Ok(self)
    }

    // -----------------------------------------------------------------------
    // Build
    // -----------------------------------------------------------------------

    pub async fn build(mut self) -> Result<UnifiedRuntime, UnifiedRuntimeBuilderError> {
        // --- Identity-first builder validation ---

        self.identity_bootstrap_mode
            .validate()
            .map_err(UnifiedRuntimeBuilderError::ConflictingConfiguration)?;

        // Validate the control-listen address up front so a typo fails the
        // build before any expensive bootstrap; the actual bind happens
        // after the runtime exists (near the end of build()).
        let control_listen = self
            .control_listen
            .as_deref()
            .map(crate::runtime::cross_mob_control::ControlListenAddr::parse)
            .transpose()
            .map_err(|error| {
                UnifiedRuntimeBuilderError::ConflictingConfiguration(format!(
                    "control_listen(): {error}"
                ))
            })?;

        let has_persistent_state = self.persistent_state_path.is_some();
        // Same-root pairing is a documented requirement of
        // `continuity_from_state_dir`; enforce it instead of letting two
        // different directories silently fork session authority from the
        // runtime/blob/workgraph stores.
        if let (Some(continuity_dir), Some(state_path)) = (
            self.continuity_state_dir.as_ref(),
            self.persistent_state_path.as_ref(),
        ) {
            verify_shared_state_root(continuity_dir, state_path)?;
        }
        // One path root per realm: the state directory and a scratch root
        // are competing path authorities.
        if has_persistent_state && self.scratch_dir.is_some() {
            return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                "persistent_state() and scratch_dir() are mutually exclusive — one path root \
                 per realm"
                    .to_string(),
            ));
        }
        if self.blob_store.is_some() && self.binary_blob_store.is_some() {
            return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                "blob_store() and binary_blob_store() are mutually exclusive — inject the one \
                 typed form you have"
                    .to_string(),
            ));
        }
        self.materialize_storage_provider().await?;

        let has_continuity_store = self.continuity_store.is_some();
        let has_lease_provider = self.lease_provider.is_some();
        let has_roster_provider = self.roster_provider.is_some();
        let has_topology_provider = self.topology_provider.is_some();
        // A2 decouple: agent memory is keyed by AgentIdentity, which every
        // mob member already has, so enabling it must NOT pull in the
        // identity-first orchestration layer (roster/continuity/leases).
        // Without a roster the BASIC memory surface (recorder tool +
        // build-time injection + panel store) rides the classic path via a
        // `MemorySpawnCustomizer`; with a roster, memory composes into the
        // IdentityRuntime customizer exactly as before (advanced lifecycle
        // features included).
        let has_agent_customizer = self.agent_customizer.is_some();
        let has_identity_runtime_instance_id = self.identity_runtime_instance_id.is_some();
        let has_scratch_dir = self.scratch_dir.is_some();
        let has_external_identity_storage =
            has_continuity_store || has_lease_provider || has_scratch_dir;
        let wants_identity_first = has_external_identity_storage
            || has_roster_provider
            || has_topology_provider
            || has_agent_customizer
            || has_identity_runtime_instance_id
            || self.identity_bootstrap_mode_configured;
        // REQ-23, lifted (M4): an external continuity/lease pair may coexist
        // with persistent_state() — the external substrate stays the
        // identity and session authority while the state directory supplies
        // the meerkat-shared local stores (runtime, workgraph, blobs, ...).
        // The genuinely contradictory combination keeps a typed error: half
        // an external substrate. The bundled store cannot split authority
        // with an external half — the lease provider's fencing floor is
        // coupled to its continuity store's persisted high-water.
        if has_persistent_state && (has_continuity_store != has_lease_provider) {
            return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                "continuity_store() and lease_provider() must be supplied together — the lease \
                 authority's fencing floor is coupled to its continuity store"
                    .to_string(),
            ));
        }

        if has_persistent_state && wants_identity_first && !has_roster_provider {
            return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                "persistent_state() identity-first path requires roster_provider()".to_string(),
            ));
        }

        // REQ-24: external path requires all three storage inputs plus a roster.
        if !has_persistent_state
            && wants_identity_first
            && !(has_continuity_store
                && has_lease_provider
                && has_roster_provider
                && has_scratch_dir)
        {
            let mut missing = Vec::new();
            if !has_continuity_store {
                missing.push("continuity_store");
            }
            if !has_lease_provider {
                missing.push("lease_provider");
            }
            if !has_roster_provider {
                missing.push("roster_provider");
            }
            if !has_scratch_dir {
                missing.push("scratch_dir");
            }
            return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                format!(
                    "identity-first path requires continuity_store() + lease_provider() + \
                     roster_provider() + scratch_dir(); missing: {}",
                    missing.join(", ")
                ),
            ));
        }

        #[cfg(feature = "extension-state")]
        if !self.tool_bundle_factories.is_empty()
            && wants_identity_first
            && self.continuity_store.is_none()
        {
            let path = self.persistent_state_path.clone().ok_or_else(|| {
                UnifiedRuntimeBuilderError::ConflictingConfiguration(
                    "extension documents require a configured persistent identity authority".into(),
                )
            })?;
            self = self.continuity_from_state_dir(path).await?;
        }

        let continuity_session_store = self
            .continuity_store
            .as_ref()
            .map(|store| Arc::new(ContinuitySessionStoreAdapter::new(store.clone())));
        if let Some(store) = continuity_session_store.as_ref() {
            self.custom_session_store = Some(store.clone());
        }
        // Strict mode: the default SQLite session store is incremental, so
        // only a custom or continuity-backed store can fail this.
        if self.require_incremental_session_persistence
            && let Some(store) = self.custom_session_store.as_ref()
            && Arc::clone(store).as_incremental().is_none()
        {
            return Err(UnifiedRuntimeBuilderError::SessionStoreNotIncremental {
                store_kind: self.custom_session_store_kind(),
            });
        }

        // Legacy mob_spec path takes precedence — must be consumed before
        // resolve_mob_spec (which borrows &self for the definition path).
        let mut mob_spec = match self.mob_spec.take() {
            Some(spec) => {
                // Legacy path: require module_config and timeout as before.
                if self.module_config.is_none() {
                    return Err(UnifiedRuntimeBuilderError::MissingRequiredField(
                        UnifiedRuntimeBuilderField::ModuleConfig,
                    ));
                }
                if self.timeout.is_none() {
                    return Err(UnifiedRuntimeBuilderError::MissingRequiredField(
                        UnifiedRuntimeBuilderField::Timeout,
                    ));
                }
                // A legacy spec arrives with its session service (and thus
                // its compactor) already built from the composer's own
                // `meerkat::Config`. Accepting a compaction declaration here
                // would produce exactly the dead knob this seam exists to
                // remove, so it refuses instead of silently doing nothing.
                if self.compaction_policy.is_some() {
                    return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                        "compaction() and mob_spec() are mutually exclusive — a pre-built \
                         MobBootstrapSpec already carries its session service's compaction \
                         policy; declare it on the spec's own meerkat::Config"
                            .to_string(),
                    ));
                }
                spec
            }
            None => self.resolve_mob_spec().await?,
        };

        #[cfg(feature = "extension-state")]
        let extension_authority = self.materialize_extension_factories(&mut mob_spec).await?;

        // Provider-declared durability flows to the census verbatim (M4b):
        // replace the locally-labeled slots for the domains the provider
        // declared and add the slots the local composition has no
        // equivalent record for (continuity, event_log, console, metadata,
        // agent_memory).
        if !self.provider_slot_census.is_empty()
            && let Some(summary) = mob_spec.resolved_storage.as_mut()
        {
            let provider_census = std::mem::take(&mut self.provider_slot_census);
            summary.slots.retain(|slot| {
                !provider_census.iter().any(|provider_slot| {
                    provider_slot.durability.domain() == slot.durability.domain()
                })
            });
            summary.slots.extend(provider_census);
        }

        for (name, (dispatcher, availability)) in std::mem::take(&mut self.tool_bundles) {
            if mob_spec.tool_bundles.contains_key(&name) {
                return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                    format!(
                        "tool bundle '{name}' is registered on both the builder and the supplied \
                     MobBootstrapSpec"
                    ),
                ));
            }
            mob_spec.tool_bundles.insert(name.clone(), dispatcher);
            mob_spec
                .child_tool_bundle_availability
                .insert(name, availability);
        }
        if let Some(binding) = self.child_application_tool_policy.take() {
            if mob_spec.child_application_tool_policy.is_some() {
                return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                    "a child application tool policy is set on both the builder and the \
                     supplied MobBootstrapSpec"
                        .to_string(),
                ));
            }
            mob_spec.child_application_tool_policy = Some(binding);
        }

        let module_config = self.module_config.take().unwrap_or_else(|| MobKitConfig {
            modules: Vec::new(),
            discovery: crate::types::DiscoverySpec {
                namespace: String::new(),
                modules: Vec::new(),
            },
            pre_spawn: Vec::new(),
        });
        let timeout = self.timeout.unwrap_or(DEFAULT_TIMEOUT);
        if let Some(state_path) = self.persistent_state_path.as_ref() {
            std::fs::create_dir_all(state_path).map_err(|e| {
                UnifiedRuntimeBuilderError::Io(format!(
                    "failed to create state directory at {}: {e}",
                    state_path.display()
                ))
            })?;
        }
        // The path authority for every state-dir file name below:
        // canonical-name-first probing keeps legacy spellings working where
        // they lie; twin spellings refuse loudly instead of forking history.
        let storage_layout = self
            .persistent_state_path
            .as_ref()
            .map(|path| MobKitStorageLayout::with_injected_roots(path.clone(), None));
        // Full-stack path: the stack provider exists pre-runtime so it can
        // serve as the provider (recorder + recall) from the first spawn;
        // the firewall + engines attach post-construction, when the memory
        // event sink and mob handle exist. An explicit `agent_memory()`
        // provider is used as-is — `attach_memory_engines` capability-checks
        // it; otherwise the bundled SQLite store opens under the layout root.
        let stack_provider: Option<Arc<dyn AgentMemoryProvider>> =
            if self.agent_memory_engines.is_some() {
                if let Some(provider) = self.agent_memory_provider.clone() {
                    Some(provider)
                } else {
                    let Some(layout) = storage_layout.as_ref() else {
                        return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                            "persistent_agent_memory_stack() requires persistent_state()"
                                .to_string(),
                        ));
                    };
                    // One slot for both store kinds: a legacy
                    // `agent-memory-sqlite/` corpus keeps being used where it
                    // lies; fresh deployments get the canonical
                    // `agent-memory/` root.
                    let memory_path = layout.agent_memory_root()?.path;
                    Some(Arc::new(
                        crate::memory::sqlite_store::SqliteAgentMemoryStore::open(&memory_path)
                            .map_err(|e| {
                                UnifiedRuntimeBuilderError::Io(format!(
                                    "failed to open agent memory store at {}: {e}",
                                    memory_path.display()
                                ))
                            })?,
                    ))
                }
            } else {
                None
            };
        let agent_memory_provider = self
            .agent_memory_provider
            .clone()
            .or_else(|| stack_provider.clone());
        let agent_memory_injector = agent_memory_provider.as_ref().map(|provider| {
            AgentMemoryRuntimeInjector::new(
                provider.clone(),
                self.agent_memory_config.clone().unwrap_or_default(),
            )
        });
        let agent_customizer = self.composed_agent_customizer(agent_memory_provider.clone());

        // Classic (roster-less) agent memory: register the per-spawn memory
        // customizer on the mob runtime itself, so every member spawn —
        // consumer, agent-tool, policy, respawn, resume — gets the recorder
        // tool and the build-time injection keyed on its AgentIdentity. The
        // identity-first path keeps its AgentCustomizer instead (composing
        // both would double-inject on identity-first materializations).
        let classic_agent_memory = if wants_identity_first {
            None
        } else {
            agent_memory_provider.clone()
        };
        if let Some(provider) = classic_agent_memory.as_ref() {
            mob_spec.spawn_member_customizer =
                Some(Arc::new(crate::memory::MemorySpawnCustomizer::new(
                    provider.clone(),
                    self.agent_memory_config.clone().unwrap_or_default(),
                )));
        }
        // Identity-first `customize_build` tools follow each member across
        // meerkat-side rebuilds through one stable dispatcher per identity
        // (#563). Composed into the single customizer slot, never replacing.
        let customizer_tool_registry = (wants_identity_first && agent_customizer.is_some())
            .then(crate::identity_first::CustomizerToolRegistry::new);
        if let Some(registry) = customizer_tool_registry.as_ref() {
            mob_spec.spawn_member_customizer =
                Some(crate::identity_first::ComposedSpawnMemberCustomizer::over(
                    mob_spec.spawn_member_customizer.take(),
                    Arc::new(crate::identity_first::CustomizerToolsSpawnCustomizer::new(
                        registry.clone(),
                    )),
                ));
        }

        // The structural-events subscription cursor lives in the
        // persistent metadata adapter. For ephemeral builds this can be
        // in-memory (the ledger itself isn't durable, so there's nothing
        // to resume from). For persistent_state builds we MUST default
        // to SQLite — otherwise after a gateway restart the subscriber
        // resumes from `latest_cursor` and silently skips every event
        // that was written to the durable mob ledger while MobKit was
        // down. Callers can still override via `.persistent_metadata()`.
        let persistent_metadata: Arc<dyn PersistentMetadataStore> =
            if let Some(store) = self.persistent_metadata.clone() {
                store
            } else if let Some(layout) = storage_layout.as_ref() {
                let metadata_path = layout.metadata_db()?.path;
                Arc::new(SqliteMetadataStore::open(&metadata_path).map_err(|e| {
                    UnifiedRuntimeBuilderError::Io(format!(
                        "failed to open the mobkit metadata store at {}: {e}",
                        metadata_path.display()
                    ))
                })?)
            } else {
                Arc::new(InMemoryMetadataStore::new())
            };
        let console_log_store: Arc<dyn ConsoleLogStore> =
            if let Some(store) = self.console_log_store.clone() {
                store
            } else if let Some(layout) = storage_layout.as_ref() {
                let console_log_path = layout.console_db()?.path;
                Arc::new(SqliteConsoleLogStore::open(&console_log_path).map_err(|e| {
                    UnifiedRuntimeBuilderError::Io(format!(
                        "failed to open the mobkit console store at {}: {e}",
                        console_log_path.display()
                    ))
                })?)
            } else {
                Arc::new(InMemoryConsoleLogStore::new())
            };
        // §10.1 dispatch-time taint join: keep the spec's late-bound slot so
        // the full-stack path below can bind the stack's tracker into every
        // member build (including bootstrap members built before the stack
        // attaches - their decorators read the slot per call).
        let dispatch_taint_slot = mob_spec.dispatch_taint_slot();
        let live_plan = self.live_plan(&mut mob_spec)?;
        // A persistent mob can be restored while `bootstrap` builds it (a log
        // that was left Running revives every member inside meerkat's own
        // resume), before any identity activation step. Publish each roster
        // identity's customizer tools first, so those builds carry them (#563).
        let early_customizer_tools_pending = match (
            customizer_tool_registry.as_ref(),
            self.roster_provider.as_ref(),
            agent_customizer.as_ref(),
            storage_layout.as_ref(),
        ) {
            (Some(registry), Some(roster_provider), Some(customizer), Some(_)) => {
                match roster_provider
                    .roster(&RosterContext {
                        mob_definition: Some(mob_spec.definition.clone()),
                        previous_identities: Vec::new(),
                    })
                    .await
                {
                    Ok(roster) => {
                        crate::identity_first::customizer_tools::prepublish(
                            registry,
                            &roster,
                            customizer.as_ref(),
                            AgentRuntimeServices::empty(),
                            &[],
                            &[],
                        )
                        .await
                    }
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "roster provider failed before the mob build; customizer tools are \
                             published by each identity's materialization instead"
                        );
                        BTreeMap::new()
                    }
                }
            }
            _ => BTreeMap::new(),
        };
        // A restored member without its tools must not start a run before its
        // materialization publishes them: meerkat holds its run starts from
        // registration until the identity runtime releases the hold.
        mob_spec.restored_members_awaiting_tools = early_customizer_tools_pending
            .keys()
            .map(|identity| crate::member_comms_id::mob_member_id(identity.as_str()))
            .collect();
        #[allow(unused_mut)]
        let mut runtime = Box::pin(UnifiedRuntime::bootstrap_with_options(
            mob_spec,
            module_config,
            self.module_agent_events,
            timeout,
            self.options,
            persistent_metadata,
        ))
        .await
        .map_err(UnifiedRuntimeBuilderError::Bootstrap)?;

        #[cfg(feature = "extension-state")]
        {
            runtime.extension_authority = extension_authority;
        }

        // Construct session bridge from the mob handle for identity-first wiring.
        // Available for BOTH persistent_state and external-authoritative paths —
        // the bridge connects the identity-first control plane to real sessions.
        let session_bridge: Option<Arc<dyn crate::identity_first::bridge::SessionBridge>> = {
            let handle = runtime.mob_runtime.handle();
            let session_service = runtime.mob_runtime.session_service().cloned();
            let session_store = self.custom_session_store.clone();
            let mut bridge: crate::identity_first::bridge::MobSessionBridge = if let Some(store) =
                continuity_session_store.clone()
            {
                crate::identity_first::bridge::MobSessionBridge::with_continuity_session_store(
                    handle,
                    store,
                    session_service,
                )
            } else if let (Some(store), Some(service)) =
                (session_store.clone(), session_service.clone())
            {
                crate::identity_first::bridge::MobSessionBridge::with_session_store_and_service(
                    handle, store, service,
                )
            } else if let Some(store) = session_store {
                crate::identity_first::bridge::MobSessionBridge::with_session_store(handle, store)
            } else if let Some(service) = session_service {
                crate::identity_first::bridge::MobSessionBridge::with_session_service(
                    handle, service,
                )
            } else {
                crate::identity_first::bridge::MobSessionBridge::new(handle)
            };
            #[cfg(feature = "extension-state")]
            if let Some(authority) = &runtime.extension_authority {
                bridge = bridge.with_extension_identity_publication(authority.publications.clone());
            }
            // Heal seam (2026-07-29 incident): without the recoverer, the
            // continuity repair supervisor can only reset entries — a
            // cosmetic heal that materialization re-Breaks when the durable
            // head is an intra-turn projection.
            if let Some(recoverer) = runtime.mob_runtime.committed_boundary_recoverer() {
                bridge = bridge.with_committed_boundary_recoverer(recoverer);
            }
            Some(Arc::new(bridge) as Arc<dyn crate::identity_first::bridge::SessionBridge>)
        };

        // Construct the durable control-plane authority before identity
        // restore. TopologyProvider output is only a declaration; the
        // IdentityRuntime composes this controller's additions/suppressions
        // under the same admission lock before it touches peer wiring.
        let topology_controller = if let Some(state_path) = self.persistent_state_path.as_ref() {
            crate::topology_control::TopologyController::load_or_default(
                self.topology_control_policy.clone(),
                state_path.join("topology-control.json"),
            )
            .map_err(|error| UnifiedRuntimeBuilderError::Io(error.to_string()))?
        } else {
            crate::topology_control::TopologyController::new(self.topology_control_policy.clone())
                .map_err(|error| UnifiedRuntimeBuilderError::Io(error.to_string()))?
        };
        topology_controller
            .bind_authority(runtime.mob_id())
            .await
            .map_err(|error| UnifiedRuntimeBuilderError::Io(error.to_string()))?;

        let (identity_first_context, identity_roster_specs) = if wants_identity_first {
            let (continuity_store, lease_provider): (
                Arc<dyn crate::identity_first::contracts::ContinuityStore>,
                Arc<dyn crate::identity_first::contracts::LeaseProvider>,
            ) = if let (Some(continuity_store), Some(lease_provider)) =
                (self.continuity_store.clone(), self.lease_provider.clone())
            {
                // An explicit external pair (or a storage provider's realm
                // set) is the identity authority even alongside
                // persistent_state() — the REQ-23 lift (M4).
                (continuity_store, lease_provider)
            } else if let Some(layout) = storage_layout.as_ref() {
                let continuity_path = layout.continuity_db()?.path;
                let (local_store, high_water) =
                    LocalContinuityStore::open_with_fencing_floor(continuity_path.clone())
                        .await
                        .map_err(|e| {
                            UnifiedRuntimeBuilderError::Io(format!(
                                "failed to open the identity continuity store and read its fencing high-water at {}: {e}",
                                continuity_path.display()
                            ))
                        })?;
                (
                    Arc::new(local_store),
                    Arc::new(LocalLeaseProvider::with_floor(high_water)),
                )
            } else {
                let Some(continuity_store) = self.continuity_store.clone() else {
                    return Err(UnifiedRuntimeBuilderError::Bootstrap(
                        UnifiedRuntimeBootstrapError::IdentityFirst(
                            "identity-first validation requires continuity_store".to_string(),
                        ),
                    ));
                };
                let Some(lease_provider) = self.lease_provider.clone() else {
                    return Err(UnifiedRuntimeBuilderError::Bootstrap(
                        UnifiedRuntimeBootstrapError::IdentityFirst(
                            "identity-first validation requires lease_provider".to_string(),
                        ),
                    ));
                };
                (continuity_store, lease_provider)
            };
            let Some(roster_provider) = self.roster_provider.clone() else {
                return Err(UnifiedRuntimeBuilderError::Bootstrap(
                    UnifiedRuntimeBootstrapError::IdentityFirst(
                        "identity-first validation requires roster_provider".to_string(),
                    ),
                ));
            };
            let bridge = session_bridge.clone();
            let identity_runtime = Arc::new(
                IdentityRuntime::new(IdentityRuntimeConfig {
                    continuity_store,
                    lease_provider,
                    runtime_instance_id: self
                        .identity_runtime_instance_id
                        .clone()
                        .unwrap_or_else(|| format!("mobkit-{}", std::process::id())),
                    has_runtime_store: true,
                    durability_policy: DurabilityPolicy::SyncWriteThrough,
                    bridge,
                    default_timeout: None,
                })
                .with_runtime_services(AgentRuntimeServices::new(runtime.mob_runtime.handle()))
                .with_reset_roster_provider_context(
                    roster_provider.clone(),
                    Some(runtime.mob_runtime.handle().definition().clone()),
                ),
            );
            identity_runtime
                .set_agent_customizer(agent_customizer.clone())
                .await;
            identity_runtime
                .set_customizer_tool_registry(customizer_tool_registry.clone())
                .await;
            identity_runtime.record_customizer_tools_pending(early_customizer_tools_pending);
            identity_runtime
                .set_agent_memory(agent_memory_injector.clone())
                .await;
            identity_runtime.set_error_hook(self.error_hook.clone());
            // The repair commands in a session repair hold name the runtime
            // store this build opened (recorded by the composition that
            // opened it); an explicit declaration wins, and a build without
            // a persistent runtime store declares no scope.
            identity_runtime.set_session_repair_scope(self.session_repair_scope.clone().or_else(
                || {
                    runtime
                        .mob_runtime
                        .resolved_storage()
                        .as_ref()
                        .and_then(crate::identity_first::SessionRepairScope::from_resolved_storage)
                },
            ));
            identity_runtime.set_topology_controller(topology_controller.clone());

            let roster_specs = roster_provider
                .roster(&RosterContext {
                    mob_definition: Some(runtime.mob_runtime.handle().definition().clone()),
                    previous_identities: Vec::new(),
                })
                .await
                .map_err(|err| {
                    UnifiedRuntimeBuilderError::Bootstrap(
                        UnifiedRuntimeBootstrapError::IdentityFirst(format!(
                            "roster provider failed: {err}"
                        )),
                    )
                })?;

            let identity_context = Arc::new(IdentityFirstRuntimeContext::new_with_bootstrap_mode(
                identity_runtime,
                roster_provider,
                self.topology_provider.clone(),
                agent_customizer.clone(),
                Some(runtime.mob_runtime.handle().definition().clone()),
                self.identity_bootstrap_mode.clone(),
            ));
            (Some(identity_context), Some(roster_specs))
        } else {
            (None, None)
        };
        // Set immutable outer fields by rebuilding the struct
        let mut runtime = UnifiedRuntime {
            access_controller: self.access_controller,
            topology_controller,
            post_spawn_hook: self.post_spawn_hook,
            post_reconcile_hook: self.post_reconcile_hook,
            drain_timeout: self.drain_timeout.unwrap_or(DEFAULT_DRAIN_TIMEOUT),
            discovery: self.discovery,
            // A custom embedder policy overrides the definition-derived
            // default `bootstrap_with_options` installed (a downstream app,
            // 2026-07-09); with none supplied the default is preserved.
            edge_discovery: self
                .edge_discovery
                .map(std::sync::Arc::<dyn EdgeDiscovery>::from)
                .or(runtime.edge_discovery),
            contact_directory: self.contact_directory,
            session_bridge,
            identity_first_context,
            identity_lease_renewal_task: tokio::sync::Mutex::new(None),
            identity_continuity_repair_task: tokio::sync::Mutex::new(None),
            retired_supervisor_cleanups: tokio::sync::Mutex::new(tokio::task::JoinSet::new()),
            console_log_store,
            ..runtime
        };
        runtime.set_live_plan(live_plan);
        // The error hook lives in a late-bound shared slot created at
        // construction (runtime-owned tasks already hold it), so a
        // builder-supplied hook installs into the slot rather than
        // replacing it.
        if let Some(hook) = self.error_hook {
            runtime.set_error_hook(hook);
        } else {
            // Say so at startup rather than at the first failure: a host that
            // never calls `on_error` gets operational events in logs only, and
            // that should be a decision it made rather than one it discovers
            // months later while reading a console table by hand. Checked here
            // rather than inside `bootstrap` because the hook installs AFTER
            // construction, so a bootstrap-time check would fire for every
            // correctly-wired host too.
            super::emit_error_hook_absent_notice();
        }

        // Run the host's last fallible pre-bootstrap hook before identity
        // hydration can start any runtime-owned background work. In
        // particular, LazyWithBackgroundWarm spawns materialization from
        // bootstrap_roster; a later hook failure must not detach that task or
        // leave its leases alive after build() returns an error.
        //
        // The mob is already bootstrapped here, so its supervisor in-proc
        // route is live; under meerkat 0.8.23's fail-closed registration an
        // ordinary-drop return would leave the name occupied and block any
        // same-process rebuild of this mob id. Failure must follow the same
        // cooperative shutdown path as the later bootstrap errors.
        let pre_spawn_hook = self
            .pre_spawn_hook
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pre_spawn_context = if let Some(hook) = pre_spawn_hook {
            match hook().await {
                Ok(context) => context,
                Err(err) => {
                    let build_error = UnifiedRuntimeBuilderError::Bootstrap(
                        UnifiedRuntimeBootstrapError::PreSpawnHook(err.to_string()),
                    );
                    runtime.shutdown().await;
                    return Err(build_error);
                }
            }
        } else {
            serde_json::Value::Null
        };

        // Classic-path capability wiring (M4 de-weld): the §10.1 posture
        // write gate and the §9.3 timeline sink install on any provider
        // advertising the firewall controls (only when the embedder did not
        // install a taint-tracking gate already), and the console Memory
        // panel (§9.3) registers for any provider advertising the panel
        // read API. Recall-only providers keep injection + recorder
        // without a panel — by their capability flags, not by type.
        if let Some(provider) = classic_agent_memory.as_ref() {
            if let Some(taintable) = provider.as_taintable() {
                let llm_writes = self
                    .agent_memory_config
                    .as_ref()
                    .map(|config| config.llm_writes)
                    .unwrap_or_default();
                taintable.set_llm_write_gate_if_absent(Arc::new(
                    crate::memory::taint::TaintLlmWriteGate::new(None, llm_writes),
                ));
                taintable.set_event_sink_if_absent(runtime.memory_event_sink());
            }
            if let Some(panel) = provider.as_memory_panel_store() {
                runtime.set_memory_panel_store(panel);
            }
        }

        // Full-stack path (persistent_agent_memory_stack): firewall + engines
        // + observer over the pre-opened stack provider.
        if let (Some(provider), Some(engines)) =
            (stack_provider, self.agent_memory_engines.as_ref())
        {
            let config = self.agent_memory_config.clone().unwrap_or_default();
            // Engine factory state and the HNSW discard source live under
            // the realm's path root — the state directory, or the scratch
            // root on the remote-authoritative (scratch_dir) shape.
            let persistent_state = self
                .persistent_state_path
                .clone()
                .or_else(|| self.scratch_dir.clone());
            let transcript_store: Option<Arc<dyn meerkat::SessionStore>> =
                if engines.distiller.enabled || engines.steward.enabled {
                    if let Some(store) = self.custom_session_store.clone() {
                        // The ACTIVE session authority — the provider/
                        // continuity adapter or a caller-injected store. The
                        // engines must read the transcripts the runtime
                        // actually persists; opening the local session
                        // database here would hand them an empty or stale
                        // parallel history.
                        Some(store)
                    } else {
                        let layout = storage_layout.as_ref().ok_or_else(|| {
                            UnifiedRuntimeBuilderError::ConflictingConfiguration(
                                "agent memory engines require persistent_state()".to_string(),
                            )
                        })?;
                        Some(Arc::new(
                            meerkat_store::SqliteSessionStore::open(layout.session_db()?.path)
                                .map_err(|e| {
                                    UnifiedRuntimeBuilderError::Io(format!(
                                        "agent memory session store: {e}"
                                    ))
                                })?,
                        ))
                    }
                } else {
                    None
                };
            let stack = crate::memory_wiring::attach_memory_engines(
                provider,
                &config,
                engines,
                crate::memory_wiring::MemoryStackSeams {
                    persistent_state,
                    transcript_store,
                    event_sink: Some(runtime.memory_event_sink()),
                    ..Default::default()
                },
            )
            .map_err(UnifiedRuntimeBuilderError::Io)?;
            if let Some(panel) = stack.panel.clone() {
                runtime.set_memory_panel_store(panel);
            }
            // §10.1 dispatch-time taint join: bind the stack's tracker into
            // the member pre-build seam so every member's LLM client marks
            // untrusted ingestion synchronously - ahead of the async
            // observer spawned below, closing the first-ingestion race.
            dispatch_taint_slot.fill(stack.taint.clone());
            // Runtime ownership makes both infinite supervisors visible to
            // normal shutdown and to the identity-bootstrap failure cleanup
            // path below. Leaking either handle here would leave ghost memory
            // work behind after build() returned Err.
            *runtime.agent_memory_observer_task.lock().await = Some(
                crate::spawn_member_event_observer(runtime.mob_handle(), stack.sinks),
            );
            if let Some(steward) = stack.steward.as_ref() {
                // Library mode has no schedule host; the guarded interval
                // loop drives dreams.
                *runtime.agent_memory_steward_task.lock().await = Some(steward.spawn_dream_loop());
            }
            tracing::info!(
                distiller = stack.distiller.is_some(),
                steward = stack.steward.is_some(),
                "agent memory stack installed (builder path)"
            );
        }

        // All fallible builder-only setup is complete. Install the circular
        // MobRuntime <-> IdentityRuntime authority only now: earlier
        // memory-stack errors can return by ordinary drop without retaining
        // the runtime and its persistent controller locks. (Ordinary drop
        // does NOT retire the live supervisor in-proc route under meerkat
        // 0.8.23; the pre-spawn hook path above already shuts down
        // cooperatively for that reason, and the memory-stack `?` returns
        // between runtime construction and this point share that latent
        // route leak for same-process same-id rebuilds.)
        if let (Some(context), Some(roster_specs)) = (
            runtime.identity_first_context.clone(),
            identity_roster_specs.as_ref(),
        ) {
            // The shared installer registers continuity owners before lifting
            // a persistent stopped mob and owns bootstrap failure cleanup.
            if let Err(err) = runtime
                .install_and_bootstrap_identity_first_context(context, roster_specs)
                .await
            {
                let build_error = UnifiedRuntimeBuilderError::Bootstrap(
                    UnifiedRuntimeBootstrapError::IdentityFirst(format!(
                        "identity bootstrap failed: {err}"
                    )),
                );
                return Err(build_error);
            }
        } else {
            runtime
                .activate_without_identity_context()
                .await
                .map_err(|error| {
                    UnifiedRuntimeBuilderError::Bootstrap(UnifiedRuntimeBootstrapError::Mob(error))
                })?;
        }

        if runtime.identity_first_context.is_none()
            && let Some(ref discovery) = runtime.discovery
        {
            let specs = discovery.discover(pre_spawn_context).await;
            let spawn_specs: Vec<SpawnMemberSpec> =
                specs.iter().map(discovery_spec_to_spawn_spec).collect();
            if let Err(err) = runtime.spawn_many(spawn_specs).await {
                let build_error =
                    UnifiedRuntimeBuilderError::Bootstrap(UnifiedRuntimeBootstrapError::Mob(err));
                // The full memory stack may already own infinite observer and
                // steward supervisors. Dropping their JoinHandles detaches
                // them, so failed classic discovery follows normal shutdown.
                runtime.shutdown().await;
                return Err(build_error);
            }
        }

        // Run initial edge reconciliation after spawn completes
        if runtime.edge_discovery.is_some()
            || runtime.topology_controller.revision().await > 0
            || runtime.topology_controller.has_pending().await
        {
            let report = runtime.reconcile_edges().await;
            *runtime.bootstrap_edges_report.write().await = Some(report);
        }

        // Bind the cross-mob control listener last among the fallible
        // steps: the fully assembled runtime owns the serve task, so a bind
        // failure follows the same cooperative shutdown path as the other
        // late bootstrap errors above.
        if let Some(addr) = control_listen.as_ref() {
            let authorizer = std::sync::Arc::new(
                crate::runtime::cross_mob_control::ControlAuthorizer::with_grants_for_audience(
                    self.control_grants.take().unwrap_or_default(),
                    runtime.mob_id(),
                ),
            );
            if let Err(error) = runtime
                .start_control_listener_with_authorizer(addr, authorizer)
                .await
            {
                let build_error =
                    UnifiedRuntimeBuilderError::Io(format!("control_listen(): {error}"));
                runtime.shutdown().await;
                return Err(build_error);
            }
        }

        // Start event log ingestion if configured
        let mut runtime = runtime;
        if let Some(event_log_config) = self.event_log_config {
            runtime.start_event_log(event_log_config);
        }

        Ok(runtime)
    }

    /// Health-surface label for the store behind `custom_session_store`; the
    /// H2 probe warning names the concrete store the builder composed
    /// (`build()` installs a `ContinuitySessionStoreAdapter` there when a
    /// continuity store is configured).
    fn custom_session_store_kind(&self) -> String {
        match self.continuity_store.as_ref() {
            Some(store) => format!(
                "ContinuitySessionStoreAdapter over {}",
                store.store_type_name()
            ),
            None => "custom session store".to_string(),
        }
    }

    /// The `meerkat::Config` builder-created session services are built with:
    /// the caller's [`meerkat_config`](Self::meerkat_config) with the
    /// host-level [`compaction`](Self::compaction) declaration composed over
    /// its compaction slot.
    ///
    /// Returns `None` when neither is configured, so the downstream
    /// `MobBootstrapSpec` constructors keep their existing
    /// `agent_config.unwrap_or_default()` behavior for un-configured hosts.
    fn effective_meerkat_config(
        &self,
    ) -> Result<Option<meerkat::Config>, UnifiedRuntimeBuilderError> {
        let Some(policy) = self.compaction_policy.as_ref() else {
            return Ok(self.meerkat_config.clone());
        };
        let mut config = self.meerkat_config.clone().unwrap_or_default();
        let applied = crate::compaction_policy::apply_compaction_policy(&mut config, policy);
        if let Err(error) = applied {
            return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                format!("compaction(): {error}"),
            ));
        }
        Ok(Some(config))
    }

    /// Resolve the caller's blob injection into its typed form. `build()`
    /// rejects supplying both forms up front; this keeps direct
    /// `resolve_mob_spec` users honest too.
    fn blob_injection(
        &self,
    ) -> Result<Option<crate::blob_store::BlobStoreInjection>, UnifiedRuntimeBuilderError> {
        match (self.blob_store.clone(), self.binary_blob_store.clone()) {
            (Some(_), Some(_)) => Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                "blob_store() and binary_blob_store() are mutually exclusive — inject the one \
                 typed form you have"
                    .to_string(),
            )),
            (Some(core), None) => Ok(Some(crate::blob_store::BlobStoreInjection::Core(core))),
            (None, Some(binary)) => Ok(Some(crate::blob_store::BlobStoreInjection::Binary(binary))),
            (None, None) => Ok(None),
        }
    }

    /// Validate retained identity authority before opening optional document
    /// storage or constructing a dispatcher. With no factory, leave every
    /// extension capability inactive.
    #[cfg(feature = "extension-state")]
    async fn materialize_extension_factories(
        &mut self,
        spec: &mut MobBootstrapSpec,
    ) -> Result<
        Option<Arc<crate::extension_state::NativeAuthorityRegistry>>,
        UnifiedRuntimeBuilderError,
    > {
        use crate::extension_state::{
            NativeAuthorityRegistry, NativeCallerResolver, ToolBundleContext,
        };
        use crate::storage_provider::MobKitStorageProvider;
        if self.tool_bundle_factories.is_empty() {
            return Ok(None);
        }
        if let Some(continuity) = &self.continuity_store {
            continuity
                .enable_identity_binding_history()
                .await
                .map_err(UnifiedRuntimeBuilderError::ExtensionAuthorityUnavailable)?;
        }
        let state_dir = self.persistent_state_path.clone().ok_or_else(|| {
            UnifiedRuntimeBuilderError::ConflictingConfiguration(
                "durable extension documents require persistent_state()".into(),
            )
        })?;
        let opened = match self.extension_store.take() {
            Some(opened) => opened,
            None => {
                crate::storage_provider::DiskMobKitStorageProvider
                    .open_extension_state(
                        &crate::storage_provider::MobKitRealmOpenContext::for_state_dir(state_dir),
                    )
                    .await?
            }
        };
        if opened.durability.domain != "extension-state"
            || opened.durability.class != meerkat_core::DurabilityClass::Durable
            || opened.durability.resolution != meerkat_core::DurabilityResolution::Persistent
        {
            return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                "extension provider must declare persistent extension-state storage".into(),
            ));
        }
        self.provider_slot_census
            .push(crate::storage_health::StorageSlotSummary {
                durability: crate::storage_health::StorageSlotDurability::Declared(
                    opened.durability,
                ),
                backend: "configured extension-state provider".into(),
                detail: None,
                degraded: false,
            });
        let registry = NativeAuthorityRegistry::new(
            crate::storage_provider::MEERKAT_LEVEL_REALM_ID.into(),
            spec.definition.id.to_string(),
            Arc::clone(&spec.session_service),
            self.continuity_store.clone(),
            self.access_controller.clone(),
        );
        let mut namespaces = std::collections::BTreeSet::new();
        for (name, (requirements, factory)) in std::mem::take(&mut self.tool_bundle_factories) {
            if spec.tool_bundles.contains_key(&name)
                || self.tool_bundles.contains_key(&name)
                || !namespaces.insert(requirements.namespace.clone())
            {
                return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                    format!("extension bundle or namespace registered more than once: {name}"),
                ));
            }
            let documents = mobkit_extension_state::DocumentService::new(
                crate::storage_provider::MEERKAT_LEVEL_REALM_ID,
                requirements.namespace.clone(),
                Arc::clone(&opened.store),
            )
            .map_err(|error| {
                UnifiedRuntimeBuilderError::ConflictingConfiguration(error.to_string())
            })?;
            let resolver = Arc::new(NativeCallerResolver {
                registry: Arc::downgrade(&registry),
                requirements: requirements.clone(),
            });
            let dispatcher = factory
                .build(ToolBundleContext {
                    documents,
                    caller_resolver: resolver,
                })
                .map_err(UnifiedRuntimeBuilderError::ConflictingConfiguration)?;
            let names = dispatcher.tools();
            if !names.iter().any(|tool| tool.name == requirements.read_tool)
                || !names.iter().any(|tool| tool.name == requirements.edit_tool)
                || dispatcher.tool_mutation_class(&requirements.read_tool)
                    != meerkat_core::ToolMutationClass::ReadOnly
                || dispatcher.tool_mutation_class(&requirements.edit_tool)
                    != meerkat_core::ToolMutationClass::Mutating
            {
                return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                    format!(
                        "extension bundle {name} must declare its read and edit tools accurately"
                    ),
                ));
            }
            spec.child_tool_bundle_availability.insert(
                name.clone(),
                meerkat_mob_mcp::ChildToolBundleAvailability::ChildAvailable,
            );
            spec.tool_bundles.insert(name, dispatcher);
        }
        // Bootstrap installs this on the final agent mob-tool state, after
        // composing its child policy and the bundles registered above.
        spec.before_activation = Some(registry.before_activation());
        Ok(Some(registry))
    }

    async fn materialize_storage_provider(&mut self) -> Result<(), UnifiedRuntimeBuilderError> {
        let Some(provider) = self.storage_provider.take() else {
            return Ok(());
        };
        let conflicting: Vec<&str> = [
            // A caller-supplied mob spec routes composition through the
            // legacy spec path, silently bypassing the provider's meerkat
            // bundle and the session-authority rewiring — both P1 classes
            // this seam exists to prevent.
            ("mob_spec()", self.mob_spec.is_some()),
            ("continuity_store()", self.continuity_store.is_some()),
            ("lease_provider()", self.lease_provider.is_some()),
            ("blob_store()", self.blob_store.is_some()),
            ("binary_blob_store()", self.binary_blob_store.is_some()),
            ("schedule_store()", self.schedule_store.is_some()),
            // The workgraph is a meerkat-level slot, so it is NOT part of the
            // mobkit `RealmStoreSet` this seam materializes - the provider's
            // workgraph rides `provider_meerkat_stores`. It still conflicts:
            // two independent channels would otherwise both claim the slot,
            // and per-slot injection silently winning is exactly the
            // ambiguity the typed error exists to prevent.
            ("workgraph_store()", self.workgraph_store.is_some()),
            ("with_console_log_store()", self.console_log_store.is_some()),
            ("persistent_metadata()", self.persistent_metadata.is_some()),
        ]
        .into_iter()
        .filter_map(|(name, set)| set.then_some(name))
        .collect();
        if !conflicting.is_empty() {
            return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                format!(
                    "storage_provider() supplies the realm store set; the per-slot seams it \
                 subsumes cannot also be set: {}",
                    conflicting.join(", ")
                ),
            ));
        }
        if self.roster_provider.is_none() {
            return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                "storage_provider() requires roster_provider(): the provider's continuity \
                 store is the identity-first session authority"
                    .to_string(),
            ));
        }
        let (state_dir, layout) = if let Some(state_path) = self.persistent_state_path.clone() {
            (
                state_path.clone(),
                MobKitStorageLayout::with_injected_roots(state_path, None),
            )
        } else if let Some(scratch) = self.scratch_dir.clone() {
            (
                scratch.clone(),
                MobKitStorageLayout::declared_ephemeral(scratch),
            )
        } else {
            return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                "storage_provider() requires persistent_state() or scratch_dir() as the \
                 realm's path root"
                    .to_string(),
            ));
        };
        let mut declared_ephemeral_domains = Vec::new();
        if self.ephemeral_blobs {
            declared_ephemeral_domains.push("blobs".to_string());
        }
        if self.ephemeral_runtime_store {
            declared_ephemeral_domains.push("runtime".to_string());
        }
        let ctx = crate::storage_provider::MobKitRealmOpenContext {
            layout,
            state_dir,
            declared_ephemeral_domains,
        };
        let set = provider.open_realm(&ctx).await?;
        // Providers are contracted to enforce this before returning; the
        // composition re-checks because the rule is the composition's.
        crate::storage_provider::enforce_fail_closed_store_set(&set, &ctx)?;
        #[cfg(feature = "extension-state")]
        if !self.tool_bundle_factories.is_empty() {
            set.continuity_store
                .enable_identity_binding_history()
                .await
                .map_err(UnifiedRuntimeBuilderError::ExtensionAuthorityUnavailable)?;
            self.extension_store = Some(provider.open_extension_state(&ctx).await?);
        }

        // M4b: the single-bundle contract covers the meerkat-shared level
        // too. The built-in disk backend IS the local composition the spec
        // opens (M2 layout compatibility — runtime.sqlite/workgraph.sqlite3
        // stay flat under the state dir), so only a non-disk backend routes
        // the meerkat-level slots through the provider's realm bundle;
        // runtime and workgraph authority then land in the same backend as
        // continuity, fail-closed at the seam instead of a silent local
        // split.
        let meerkat_provider = provider.meerkat_provider();
        self.provider_meerkat_stores = if meerkat_provider.name() == "disk" {
            None
        } else {
            Some(
                crate::storage_provider::open_provider_meerkat_stores(meerkat_provider, &ctx)
                    .await?,
            )
        };
        // Retain the provider's per-slot durability declarations for the
        // census: the health surfaces must report the provider-declared
        // resolutions verbatim, not the local defaults those declarations
        // replaced (an explicitly-ephemeral schedule must not read as
        // persistent).
        self.provider_slot_census = set
            .durability
            .iter()
            .map(|declaration| crate::storage_health::StorageSlotSummary {
                durability: crate::storage_health::StorageSlotDurability::Declared(
                    declaration.clone(),
                ),
                backend: format!("storage provider '{}'", provider.name()),
                detail: Some(
                    "provider-declared durability (composite realm store set)".to_string(),
                ),
                degraded: false,
            })
            .collect();

        self.continuity_store = Some(set.continuity_store);
        self.lease_provider = Some(match set.lease_authority {
            crate::storage_provider::MobKitLeaseAuthority::Provider(provider) => provider,
            crate::storage_provider::MobKitLeaseAuthority::FencingFloor(floor) => {
                Arc::new(LocalLeaseProvider::with_floor(floor))
            }
        });
        self.binary_blob_store = Some(set.blob_store);
        self.schedule_store = Some(set.schedule_store);
        self.console_log_store = Some(set.console_log_store);
        self.persistent_metadata = Some(set.metadata_store);
        if self.event_log_config.is_none()
            && let Some(store) = set.event_log_store
        {
            self.event_log_config = Some(EventLogConfig {
                store,
                ..EventLogConfig::default()
            });
        }
        if self.agent_memory_provider.is_none()
            && (self.agent_memory_config.is_some() || self.agent_memory_engines.is_some())
            && let Some(memory) = set.agent_memory_provider
        {
            self.agent_memory_provider = Some(memory);
        }
        Ok(())
    }

    /// Resolve the mob spec from the definition-based path.
    /// Called only when `mob_spec` is not set (legacy path handled in `build()`).
    #[allow(
        unknown_lints,
        clippy::unused_async_trait_impl,
        reason = "preserve the awaitable builder resolution seam used throughout the build pipeline"
    )]
    /// Turn the registered live doors into a plan, taking the typed inputs
    /// the spec retained. Fails closed when a door is registered but the
    /// session service is not persistent: live channels need the durable
    /// session owner, and silently composing nothing would leave the console
    /// reporting voice unavailable with no diagnostic.
    fn live_plan(
        &self,
        mob_spec: &mut MobBootstrapSpec,
    ) -> Result<Option<super::live_compose::LivePlan>, UnifiedRuntimeBuilderError> {
        #[cfg(feature = "openai-live")]
        let requested = self.live.is_some() || self.console_voice.is_some();
        #[cfg(not(feature = "openai-live"))]
        let requested = self.live.is_some();
        if !requested {
            return Ok(None);
        }
        if self
            .live
            .as_ref()
            .is_some_and(|options| options.public_base_url.trim().is_empty())
        {
            return Err(UnifiedRuntimeBuilderError::LiveCompose(
                super::live_compose::LiveComposeError::LiveRequiresPublicBaseUrl,
            ));
        }
        let inputs =
            mob_spec
                .live_compose_inputs
                .take()
                .ok_or(UnifiedRuntimeBuilderError::LiveCompose(
                    super::live_compose::LiveComposeError::LiveRequiresPersistentSessions,
                ))?;
        Ok(Some(super::live_compose::LivePlan {
            inputs,
            live: self.live.clone(),
            #[cfg(feature = "openai-live")]
            console_voice: self.console_voice.clone(),
            #[cfg(all(test, feature = "openai-live"))]
            summary_override: None,
            #[cfg(test)]
            session_factory_override: None,
        }))
    }

    async fn resolve_mob_spec(&self) -> Result<MobBootstrapSpec, UnifiedRuntimeBuilderError> {
        let mut caps = self.capability_flags;
        // Resolved once for all three storage arms: the agent config every
        // builder-created session service (and therefore every compactor)
        // is built from.
        let agent_config = self.effective_meerkat_config()?;
        let definition = match self.definition_source {
            Some(DefinitionSource::Inline(ref def)) => *def.clone(),
            Some(DefinitionSource::TomlPath(ref path)) => {
                let toml_content = std::fs::read_to_string(path).map_err(|e| {
                    UnifiedRuntimeBuilderError::Io(format!(
                        "failed to read definition TOML at {}: {e}",
                        path.display()
                    ))
                })?;
                MobDefinition::from_toml(&toml_content).map_err(|e| {
                    UnifiedRuntimeBuilderError::DefinitionLoad(format!(
                        "failed to parse definition TOML at {}: {e}",
                        path.display()
                    ))
                })?
            }
            None => {
                return Err(UnifiedRuntimeBuilderError::MissingRequiredField(
                    UnifiedRuntimeBuilderField::MobSpec,
                ));
            }
        };
        caps.image_generation |=
            crate::mob_handle_runtime::mob_definition_may_use_image_generation(&definition);
        // Declared-versus-resolved capability invariant. This is the ONLY
        // sound place for it: on the definition path `self.schedule_store`
        let max_sessions = self.max_sessions.unwrap_or(DEFAULT_MAX_SESSIONS);
        if max_sessions == 0 {
            return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
                "max_sessions() must be greater than 0".to_string(),
            ));
        }

        let hook = self
            .session_hook
            .as_ref()
            .map(|h| -> crate::mob_handle_runtime::PreBuildHook {
                let hook = h.clone();
                Arc::new(
                    move |req: &mut meerkat_core::service::CreateSessionRequest| {
                        let hook = hook.clone();
                        Box::pin(async move { hook.before_create(req).await })
                    },
                )
            });

        let after_hook: Option<crate::mob_handle_runtime::AfterCreateHook> = self
            .session_hook
            .as_ref()
            .map(|h| -> crate::mob_handle_runtime::AfterCreateHook {
                let hook = h.clone();
                Arc::new(move |session_id, ctx| {
                    let hook = hook.clone();
                    Box::pin(async move {
                        hook.after_create(&session_id, &ctx).await;
                    })
                })
            });

        // Note: blocking I/O (fs, SQLite) — acceptable at startup.
        let mut spec = if let Some(ref state_path) = self.persistent_state_path {
            std::fs::create_dir_all(state_path).map_err(|e| {
                UnifiedRuntimeBuilderError::Io(format!(
                    "failed to create state directory at {}: {e}",
                    state_path.display()
                ))
            })?;

            let session_store_kind = if self.custom_session_store.is_some() {
                self.custom_session_store_kind()
            } else {
                "SqliteSessionStore".to_string()
            };
            let session_store: Arc<dyn meerkat::SessionStore> =
                if let Some(ref store) = self.custom_session_store {
                    store.clone()
                } else {
                    let sqlite_path =
                        MobKitStorageLayout::with_injected_roots(state_path.clone(), None)
                            .session_db()?
                            .path;
                    Arc::new(
                        meerkat_store::SqliteSessionStore::open(sqlite_path).map_err(|e| {
                            UnifiedRuntimeBuilderError::Io(format!(
                                "failed to open SQLite session store: {e}"
                            ))
                        })?,
                    )
                };
            let mob_storage = MobStorage::in_memory();

            MobBootstrapSpec::persistent_inner_with_provider_stores(
                definition,
                mob_storage,
                state_path.clone(),
                max_sessions,
                session_store,
                &session_store_kind,
                self.blob_injection()?,
                self.ephemeral_blobs,
                self.ephemeral_runtime_store,
                self.schedule_store.clone(),
                self.workgraph_store.clone(),
                hook,
                caps,
                after_hook.clone(),
                agent_config,
                self.provider_meerkat_stores.clone(),
            )
            .map_err(storage_resolution_builder_error)?
        } else if let Some(ref scratch_dir) = self.scratch_dir {
            std::fs::create_dir_all(scratch_dir).map_err(|e| {
                UnifiedRuntimeBuilderError::Io(format!(
                    "failed to create scratch directory at {}: {e}",
                    scratch_dir.display()
                ))
            })?;

            MobBootstrapSpec::ephemeral_runtime_backed_with_provider_stores(
                definition,
                MobStorage::in_memory(),
                scratch_dir.clone(),
                max_sessions,
                self.custom_session_store.clone(),
                &self.custom_session_store_kind(),
                self.blob_injection()?,
                self.schedule_store.clone(),
                self.workgraph_store.clone(),
                hook,
                caps,
                after_hook,
                agent_config,
                self.provider_meerkat_stores.clone(),
            )
            .map_err(storage_resolution_builder_error)?
        } else {
            // Ephemeral: create a temp dir that lives as long as the runtime.
            let temp_dir = tempfile::tempdir().map_err(|e| {
                UnifiedRuntimeBuilderError::Io(format!("failed to create temp dir: {e}"))
            })?;
            let store_path = temp_dir.path().to_path_buf();

            let mut spec = MobBootstrapSpec::ephemeral_runtime_backed_inner(
                definition,
                MobStorage::in_memory(),
                store_path,
                max_sessions,
                self.custom_session_store.clone(),
                &self.custom_session_store_kind(),
                self.blob_injection()?,
                self.schedule_store.clone(),
                self.workgraph_store.clone(),
                hook,
                caps,
                after_hook,
                agent_config,
            )
            .map_err(storage_resolution_builder_error)?;
            spec._ephemeral_dir = Some(Arc::new(temp_dir));
            spec
        };

        spec.options = MobBootstrapOptions {
            allow_ephemeral_sessions: true,
            notify_orchestrator_on_resume: true,
            default_llm_client: self.default_llm_client.clone(),
        };

        Ok(spec)
    }
}

/// Enforce the documented same-root pairing of `continuity_from_state_dir`
/// and `persistent_state`: both must name ONE physical state directory, or
/// session authority forks from the runtime/blob/workgraph stores it must
/// pair with. The comparison is by canonical (physical) identity, not raw
/// spelling — a lexical compare both refuses a genuinely shared root spelled
/// two ways (symlink vs target, `..`-detour vs plain) and, worse, lets two
/// lexically equal spellings that resolve to different physical directories
/// fork silently. A path that cannot be canonicalized refuses outright with
/// both roots named; this gate never degrades to the lexical compare.
fn verify_shared_state_root(
    continuity_dir: &std::path::Path,
    state_path: &std::path::Path,
) -> Result<(), UnifiedRuntimeBuilderError> {
    let canonical_continuity = crate::storage_layout::canonicalize_storage_root(continuity_dir)
        .map_err(|error| {
            UnifiedRuntimeBuilderError::ConflictingConfiguration(format!(
                "cannot canonicalize continuity_from_state_dir root {} to verify it is the \
                 same state directory as persistent_state {}: {error}",
                continuity_dir.display(),
                state_path.display()
            ))
        })?;
    let canonical_state =
        crate::storage_layout::canonicalize_storage_root(state_path).map_err(|error| {
            UnifiedRuntimeBuilderError::ConflictingConfiguration(format!(
                "cannot canonicalize persistent_state root {} to verify it is the same state \
             directory as continuity_from_state_dir root {}: {error}",
                state_path.display(),
                continuity_dir.display()
            ))
        })?;
    if canonical_continuity != canonical_state {
        return Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(
            format!(
                "continuity_from_state_dir opened {} but persistent_state resolves to {} \
                 (configured as {}); the identity substrate and the runtime stores must \
                 share ONE state directory, or session authority forks from the stores it \
                 must pair with",
                canonical_continuity.display(),
                canonical_state.display(),
                state_path.display()
            ),
        ));
    }
    Ok(())
}

/// The builder-level class of a bootstrap storage refusal.
fn storage_resolution_builder_error(
    error: crate::storage_health::StorageResolutionError,
) -> UnifiedRuntimeBuilderError {
    match error {
        crate::storage_health::StorageResolutionError::Blob(
            crate::storage_health::BlobStoreResolutionError::NonPersistentUndeclared,
        ) => UnifiedRuntimeBuilderError::ConflictingConfiguration(error.to_string()),
        crate::storage_health::StorageResolutionError::Blob(
            crate::storage_health::BlobStoreResolutionError::OpenFailed { .. },
        )
        | crate::storage_health::StorageResolutionError::RuntimeStore(_)
        | crate::storage_health::StorageResolutionError::JobStore(_)
        // An unreadable council store is the same class: a durable slot
        // present on disk that refused at composition time.
        | crate::storage_health::StorageResolutionError::Council(_) => {
            UnifiedRuntimeBuilderError::Io(error.to_string())
        }
        crate::storage_health::StorageResolutionError::Runtime(error) => {
            UnifiedRuntimeBuilderError::Runtime(error)
        }
        crate::storage_health::StorageResolutionError::Delivery(error) => {
            UnifiedRuntimeBuilderError::Delivery(error)
        }
    }
}
#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use meerkat_core::service::{
        CreateSessionRequest, DeferredPromptPolicy, InitialTurnPolicy, SessionService,
    };

    #[cfg(feature = "extension-state")]
    #[tokio::test]
    async fn extension_gateway_authority_refuses_before_storage_or_factory_activation() {
        struct NoCallbacks;
        #[async_trait::async_trait]
        impl crate::identity_first::gateway_bridges::CallbackBridge for NoCallbacks {
            async fn call(
                &self,
                _: &str,
                _: serde_json::Value,
            ) -> Result<serde_json::Value, String> {
                panic!("extension activation must not invent SDK history callbacks")
            }
        }

        let definition = MobDefinition::from_toml("[mob]\nid = \"extension-activation\"\n")
            .expect("definition parses");
        let mut spec = UnifiedRuntimeBuilder::default()
            .definition(definition)
            .resolve_mob_spec()
            .await
            .expect("ephemeral spec resolves without execution");
        let continuity = Arc::new(
            crate::identity_first::gateway_bridges::GatewayContinuityStore::new(NoCallbacks),
        );
        let mut builder = UnifiedRuntimeBuilder::default().continuity_store(continuity);
        assert!(
            builder
                .materialize_extension_factories(&mut spec)
                .await
                .expect("no registered extension leaves the host unchanged")
                .is_none()
        );

        let dir = tempfile::tempdir().expect("temporary directory");
        let state = dir.path().join("unopened-extension-state");
        builder = builder.persistent_state(&state).register_tool_bundle_factory(
            "durable-documents-test",
            crate::extension_state::ToolBundleRequirements::durable_documents(
                "durable-documents-test",
                "documents_read",
                "documents_apply",
            ),
            Arc::new(
                |_: crate::extension_state::ToolBundleContext| -> Result<
                    Arc<dyn meerkat_core::AgentToolDispatcher>,
                    String,
                > {
                    panic!("unsupported authority must refuse before factory activation")
                },
            ),
        );
        assert!(matches!(
            builder.materialize_extension_factories(&mut spec).await,
            Err(UnifiedRuntimeBuilderError::ExtensionAuthorityUnavailable(
                crate::identity_first::ContinuityStoreError::UnsupportedAuthority {
                    capability: "SDK-hosted identity binding history"
                }
            ))
        ));
        assert!(
            !state.exists(),
            "refusal must precede opening document storage"
        );
        assert!(spec.before_activation.is_none());
    }

    /// H1: a persistent-state build with an injected blob store that reports
    /// `!is_persistent()` fails composition unless `ephemeral_blobs(true)`
    /// declares the choice — declared, it composes and reports a
    /// non-persistent custom blob slot.
    #[tokio::test]
    async fn persistent_state_gates_non_persistent_blob_store_on_declaration() {
        let dir = tempfile::tempdir().expect("temp dir");
        let memory_blobs: Arc<dyn meerkat_core::BlobStore> =
            Arc::new(crate::blob_store::Base64BlobStoreAdapter::new(Arc::new(
                crate::blob_store::ObjectStoreBlobStore::memory(),
            )));
        let definition = || {
            MobDefinition::from_toml("[mob]\nid = \"blob-declaration-test\"\n")
                .expect("parse test mob definition")
        };

        let undeclared = UnifiedRuntimeBuilder::default()
            .definition(definition())
            .persistent_state(dir.path())
            .blob_store(memory_blobs.clone());
        match undeclared.resolve_mob_spec().await {
            Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(message)) => {
                assert!(
                    message.contains("ephemeral_blobs"),
                    "the error must name the remediation, got: {message}"
                );
            }
            Err(other) => panic!("expected ConflictingConfiguration, got: {other}"),
            Ok(_) => panic!("undeclared non-persistent blob store must fail composition"),
        }

        let declared = UnifiedRuntimeBuilder::default()
            .definition(definition())
            .persistent_state(dir.path())
            .blob_store(memory_blobs)
            .ephemeral_blobs(true);
        let spec = declared
            .resolve_mob_spec()
            .await
            .unwrap_or_else(|e| panic!("declared ephemeral blobs must compose: {e}"));
        let summary = spec
            .resolved_storage
            .expect("persistent builds record their storage");
        assert_eq!(
            summary.blob_durability,
            crate::storage_health::BlobDurability::Custom { persistent: false }
        );
        // The repair commands of a session repair hold derive from the
        // runtime store this build opened: the default persistent layout's
        // single SQLite file, addressed directly.
        assert_eq!(
            summary.runtime_store_locator,
            Some(crate::storage_health::RuntimeStoreLocator::SqliteFile {
                path: dir.path().join(crate::storage_layout::RUNTIME_DB_FILE_NAME),
            })
        );
        let scope = crate::identity_first::SessionRepairScope::from_resolved_storage(&summary)
            .expect("a persistent runtime store derives a repair scope");
        assert_eq!(
            scope.runtime_store,
            Some(dir.path().join(crate::storage_layout::RUNTIME_DB_FILE_NAME))
        );
        assert_eq!(scope.state_root, None);
    }

    fn deferred_capacity_request(prompt: impl Into<String>) -> CreateSessionRequest {
        let build = meerkat_core::service::SessionBuildOptions {
            llm_client_override: Some(meerkat::encode_llm_client_override_for_service(Arc::new(
                meerkat_client::TestClient::default(),
            ))),
            ..Default::default()
        };

        CreateSessionRequest {
            model: "gpt-5.5".to_string(),
            prompt: meerkat_core::ContentInput::Text(prompt.into()),
            system_prompt: meerkat_core::config::SystemPromptOverride::Inherit,
            max_tokens: None,
            event_tx: None,
            initial_turn: InitialTurnPolicy::Defer,
            deferred_prompt_policy: DeferredPromptPolicy::Discard,
            build: Some(build),
            labels: None,
            injected_context: Vec::new(),
        }
    }

    #[tokio::test]
    async fn definition_based_ephemeral_spec_provides_runtime_adapter() {
        let definition = meerkat_mob::MobDefinition::from_toml(
            r#"
[mob]
id = "builder-ephemeral"

[profiles.worker]
model = "gpt-5.5"
runtime_mode = "autonomous_host"

[profiles.worker.tools]
comms = true
"#,
        )
        .expect("definition parses");

        let builder = UnifiedRuntimeBuilder::default().definition(definition);
        let spec = builder.resolve_mob_spec().await.expect("spec resolves");
        assert!(
            spec.runtime_adapter.is_some(),
            "definition-based ephemeral specs should expose runtime authority",
        );
    }

    #[tokio::test]
    async fn definition_based_ephemeral_spec_uses_configured_max_sessions() {
        let definition = meerkat_mob::MobDefinition::from_toml(
            r#"
[mob]
id = "builder-max-sessions"

[profiles.worker]
model = "gpt-5.5"
runtime_mode = "autonomous_host"
"#,
        )
        .expect("definition parses");

        let builder = UnifiedRuntimeBuilder::default()
            .definition(definition)
            .max_sessions(65);
        let spec = builder.resolve_mob_spec().await.expect("spec resolves");

        for index in 0..65 {
            SessionService::create_session(
                spec.session_service.as_ref(),
                deferred_capacity_request(format!("session {index}")),
            )
            .await
            .expect("configured capacity should admit session");
        }

        let blocked = SessionService::create_session(
            spec.session_service.as_ref(),
            deferred_capacity_request("one too many"),
        )
        .await
        .expect_err("configured capacity should block the next session");
        assert!(
            blocked.to_string().contains("Max sessions reached (65/65)"),
            "unexpected capacity error: {blocked}",
        );
    }

    #[tokio::test]
    async fn definition_based_spec_accepts_custom_meerkat_config() {
        let definition = meerkat_mob::MobDefinition::from_toml(
            r#"
[mob]
id = "builder-custom-config"

[profiles.worker]
model = "gpt-5.5"
runtime_mode = "autonomous_host"
"#,
        )
        .expect("definition parses");
        let mut config = meerkat::Config::default();
        config.compaction.auto_compact_threshold = 42_000;
        config.compaction.auto_compact_threshold_explicit = true;
        config.compaction.recent_turn_budget = 2;

        let builder = UnifiedRuntimeBuilder::default()
            .definition(definition)
            .meerkat_config(config)
            .max_sessions(1);
        let spec = builder.resolve_mob_spec().await.expect("spec resolves");

        SessionService::create_session(
            spec.session_service.as_ref(),
            deferred_capacity_request("custom config session"),
        )
        .await
        .expect("custom Meerkat config should still build a usable session service");
    }

    #[test]
    fn model_fallback_host_config_survives_compaction_composition() {
        let mut config = meerkat::Config::default();
        config.model_fallback.enabled = Some(false);
        config.model_fallback.policy.trigger_after_attempts = 7;
        let expected = config.model_fallback.clone();
        let builder = UnifiedRuntimeBuilder::default()
            .meerkat_config(config)
            .compaction(meerkat_core::config::CompactionRuntimeConfig {
                auto_compact_threshold: 120_000,
                auto_compact_threshold_explicit: true,
                ..Default::default()
            });
        let effective = builder
            .effective_meerkat_config()
            .expect("config")
            .expect("present");
        assert_eq!(effective.model_fallback, expected);
        assert_eq!(effective.compaction.auto_compact_threshold, 120_000);
        assert!(!meerkat::Config::default().model_fallback.is_enabled());
    }

    #[tokio::test]
    async fn definition_based_persistent_spec_uses_configured_max_sessions() {
        let definition = meerkat_mob::MobDefinition::from_toml(
            r#"
[mob]
id = "builder-persistent-max-sessions"

[profiles.worker]
model = "gpt-5.5"
runtime_mode = "autonomous_host"
"#,
        )
        .expect("definition parses");
        let tmp = tempfile::tempdir().expect("temp dir");
        let builder = UnifiedRuntimeBuilder::default()
            .definition(definition)
            .persistent_state(tmp.path().join("state"))
            .max_sessions(2);
        let spec = builder.resolve_mob_spec().await.expect("spec resolves");

        for index in 0..2 {
            SessionService::create_session(
                spec.session_service.as_ref(),
                deferred_capacity_request(format!("persistent session {index}")),
            )
            .await
            .expect("configured persistent capacity should admit session");
        }

        let blocked = SessionService::create_session(
            spec.session_service.as_ref(),
            deferred_capacity_request("persistent one too many"),
        )
        .await
        .expect_err("configured persistent capacity should block the next session");
        assert!(
            blocked.to_string().contains("Max sessions reached (2/2)"),
            "unexpected capacity error: {blocked}",
        );
    }

    #[tokio::test]
    async fn definition_based_spec_rejects_zero_max_sessions() {
        let definition = meerkat_mob::MobDefinition::from_toml(
            r#"
[mob]
id = "builder-zero-max-sessions"

[profiles.worker]
model = "gpt-5.5"
"#,
        )
        .expect("definition parses");

        let result = UnifiedRuntimeBuilder::default()
            .definition(definition)
            .max_sessions(0)
            .resolve_mob_spec()
            .await;
        assert!(result.is_err(), "zero max sessions should be rejected");
        let err = result.err().expect("zero max sessions error");

        assert!(
            err.to_string().contains("max_sessions"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn builder_accepts_custom_console_log_store_for_ephemeral_mob_state() {
        let store: Arc<dyn ConsoleLogStore> = Arc::new(InMemoryConsoleLogStore::new());
        let builder = UnifiedRuntimeBuilder::default().with_console_log_store(store.clone());

        assert!(
            Arc::ptr_eq(
                builder.console_log_store.as_ref().expect("custom store"),
                &store
            ),
            "builder should retain the exact console log store supplied by the app"
        );
    }

    /// The same-root gate compares PHYSICAL identity, not raw spelling: one
    /// root spelled two equivalent ways must pass, a not-yet-created root
    /// still compares through its deepest existing ancestor, and genuinely
    /// distinct directories must refuse. The lexical compare it replaces got
    /// all of this wrong — and let two lexically equal relative spellings
    /// that resolve under different working directories fork silently.
    #[test]
    fn same_root_gate_compares_physical_identity() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let root = tmp.path().join("state");
        std::fs::create_dir_all(&root).expect("create state root");

        // Equivalent spelling through an existing `..` detour: ONE root.
        let detour = tmp.path().join("detour");
        std::fs::create_dir_all(&detour).expect("create detour dir");
        let spelled_via_detour = detour.join("..").join("state");
        verify_shared_state_root(&root, &spelled_via_detour)
            .expect("equivalent spellings of one root must pass the gate");

        // A not-yet-created state root still compares by physical identity
        // (deepest existing ancestor canonicalized, remainder rejoined).
        let future_root = tmp.path().join("future");
        verify_shared_state_root(&future_root, &detour.join("..").join("future"))
            .expect("not-yet-created roots must still compare physically");

        // Genuinely distinct directories fork session authority: refuse.
        let other = tmp.path().join("other");
        std::fs::create_dir_all(&other).expect("create other root");
        let err = verify_shared_state_root(&root, &other).expect_err("distinct roots must refuse");
        match err {
            UnifiedRuntimeBuilderError::ConflictingConfiguration(message) => {
                assert!(
                    message.contains("share ONE state directory"),
                    "refusal must explain the pairing requirement: {message}"
                );
            }
            other => panic!("expected ConflictingConfiguration, got: {other}"),
        }
    }

    /// Symlinks resolve to their targets before the same-root comparison: a
    /// symlink and its target are the ONE directory they physically are, and
    /// a symlink to a DIFFERENT directory is exactly the silent
    /// session-authority fork the gate exists to refuse, whatever the
    /// spelling suggests.
    #[cfg(unix)]
    #[test]
    fn same_root_gate_resolves_symlinks() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let target = tmp.path().join("state");
        std::fs::create_dir_all(&target).expect("create state root");
        let link = tmp.path().join("state-link");
        std::os::unix::fs::symlink(&target, &link).expect("create symlink");

        verify_shared_state_root(&link, &target)
            .expect("a symlink and its target are ONE state directory");

        let elsewhere = tmp.path().join("elsewhere");
        std::fs::create_dir_all(&elsewhere).expect("create other root");
        let fork = tmp.path().join("fork-link");
        std::os::unix::fs::symlink(&elsewhere, &fork).expect("create fork symlink");
        assert!(
            verify_shared_state_root(&target, &fork).is_err(),
            "a symlink to another directory must refuse"
        );
    }

    /// End-to-end wiring: `build()` refuses a persistent_state root that is
    /// not the physical directory `continuity_from_state_dir` opened.
    #[tokio::test]
    async fn build_refuses_forked_continuity_and_state_roots() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let result = UnifiedRuntimeBuilder::default()
            .persistent_state(tmp.path().join("stores"))
            .continuity_from_state_dir(tmp.path().join("substrate"))
            .await
            .expect("open the substrate root")
            .build()
            .await;
        match result {
            Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(message)) => {
                assert!(
                    message.contains("share ONE state directory"),
                    "refusal must explain the pairing requirement: {message}"
                );
            }
            Err(other) => panic!("expected ConflictingConfiguration, got: {other}"),
            Ok(_) => panic!("forked continuity/state roots must refuse at build"),
        }
    }

    // ── Session-compaction wiring ───────────────────────────────────────
    //
    // Composition-level pins for the host compaction policy. The behavioral
    // proof that the built session path actually carries a compactor (and
    // that this policy is the trigger it uses) lives in
    // `tests/session_compaction_wiring.rs`, which drives real member turns.

    fn compaction_test_definition(id: &str) -> MobDefinition {
        MobDefinition::from_toml(&format!("[mob]\nid = \"{id}\"\n"))
            .expect("compaction fixture definition parses")
    }

    /// An invalid declaration is a composition error, never a silently
    /// ignored knob.
    #[tokio::test]
    async fn zero_compaction_threshold_refuses_composition() {
        let builder = UnifiedRuntimeBuilder::default()
            .definition(compaction_test_definition("compaction-policy-zero"))
            .compaction(meerkat_core::config::CompactionRuntimeConfig {
                auto_compact_threshold: 0,
                auto_compact_threshold_explicit: true,
                ..Default::default()
            });
        match builder.resolve_mob_spec().await {
            Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(message)) => {
                assert!(message.contains("greater than 0"), "{message}");
            }
            Err(other) => panic!("expected ConflictingConfiguration, got: {other}"),
            Ok(_) => panic!("a zero compaction threshold must refuse composition"),
        }
    }

    /// `compaction()` composes over `meerkat_config()` instead of replacing
    /// it: the rest of the caller's config survives.
    #[tokio::test]
    async fn compaction_policy_composes_over_meerkat_config() {
        let mut config = meerkat::Config::default();
        config.budget.max_tokens = Some(4_242);
        let builder = UnifiedRuntimeBuilder::default()
            .definition(compaction_test_definition("compaction-policy-compose"))
            .meerkat_config(config)
            .compaction(meerkat_core::config::CompactionRuntimeConfig {
                auto_compact_threshold: 77_000,
                auto_compact_threshold_explicit: true,
                ..Default::default()
            });
        let effective = builder
            .effective_meerkat_config()
            .expect("composition succeeds")
            .expect("a configured builder yields a config");
        assert_eq!(effective.compaction.auto_compact_threshold, 77_000);
        assert!(effective.compaction.auto_compact_threshold_explicit);
        assert_eq!(
            effective.budget.max_tokens,
            Some(4_242),
            "the rest of the caller's meerkat_config must survive",
        );
    }

    /// A pre-built spec already carries its own compactor, so accepting a
    /// declaration here would be exactly the dead knob this seam removes.
    #[tokio::test]
    async fn compaction_policy_refuses_the_legacy_mob_spec_path() {
        let temp = tempfile::tempdir().expect("temp dir");
        let spec = MobBootstrapSpec::ephemeral(
            compaction_test_definition("compaction-policy-legacy"),
            MobStorage::in_memory(),
            temp.path().to_path_buf(),
            4,
            None,
        )
        .expect("bootstrap spec");
        let result = UnifiedRuntimeBuilder::default()
            .mob_spec(spec)
            .module_config(MobKitConfig {
                modules: Vec::new(),
                discovery: crate::types::DiscoverySpec {
                    namespace: "compaction-policy-legacy".to_string(),
                    modules: Vec::new(),
                },
                pre_spawn: Vec::new(),
            })
            .timeout(Duration::from_secs(5))
            .compaction(meerkat_core::config::CompactionRuntimeConfig::default())
            .build()
            .await;
        match result {
            Err(UnifiedRuntimeBuilderError::ConflictingConfiguration(message)) => {
                assert!(message.contains("mutually exclusive"), "{message}");
            }
            Err(other) => panic!("expected ConflictingConfiguration, got: {other}"),
            Ok(_) => panic!("compaction() with a pre-built mob_spec must refuse"),
        }
    }
}
