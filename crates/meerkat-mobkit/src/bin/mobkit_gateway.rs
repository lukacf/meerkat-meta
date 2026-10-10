use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow};
use meerkat::{AgentFactory, Config, FactoryAgentBuilder, PersistentSessionService};
use meerkat_mob::ids::AgentIdentity;
use meerkat_mob::{MobDefinition, MobStorage, ProfileName, SpawnMemberSpec};
use meerkat_mobkit::contact_directory::ContactDirectory;
use meerkat_mobkit::runtime::cross_mob_control::{
    ControlAuthorizer, ControlGrantTable, ControlListenAddr,
};
use meerkat_mobkit::shutdown_signal::ShutdownSignal;
use meerkat_mobkit::{
    Base64BlobStoreAdapter, BigQueryNaming, BinaryBlobStore, ConsolePolicy, ConsoleUiConfig,
    ConventionalPaths, GatewayPeerKeys, MOBKIT_CONTRACT_VERSION, MobBootstrapOptions,
    MobBootstrapSpec, MobKitStorageLayout, ObjectStoreBlobStore, RuntimeDecisionState,
    load_console_ui_config_from_path_for_realm,
    mob_handle_runtime::mob_definition_may_use_image_generation,
};
use meerkat_store::SqliteSessionStore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, BufReader};

const FALLBACK_TEMPLATE_VERSION: &str = "tux-fallback-v2";

/// Durable schedule service + the concrete persistent session service it shares
/// with the firing host (the `dyn MobSessionService` returned for the spec can't
/// drive the runtime-backed schedule host, which needs the concrete type).
type ScheduleHostInputs = (
    meerkat::ScheduleService,
    meerkat_mobkit::schedule_wiring::ScheduleMobTargetRegistry,
    Arc<PersistentSessionService<FactoryAgentBuilder>>,
    PathBuf,
    meerkat_mobkit::schedule_wiring::ScheduleFiringHostBinding,
);
/// WorkGraph wiring from a durable attach: the realm-scoped service, the
/// tool-plane admission slot the bootstrap spec must register, and the state
/// dir for the cross-process admission sidecar (the store file is shareable
/// across processes).
type WorkGraphParts = (
    meerkat::WorkGraphService,
    meerkat_mobkit::workgraph_admission::WorkGraphAdmissionSlot,
    PathBuf,
);

/// This binary's own tracing target. Feeds
/// `gateway_composition::default_tracing_filter`, which builds the same
/// filter string this binary used to carry as its own constant: this crate's
/// own targets at INFO, dependencies at WARN. The one-time head-canonical
/// conversion and the storage maintenance verbs report progress at INFO from
/// `meerkat_mobkit`; a blanket "warn" default hid them.
const GATEWAY_TRACING_TARGET: &str = "mobkit_gateway";
type PersistentSessionServiceParts = (
    Arc<dyn meerkat_mob::MobSessionService>,
    Arc<meerkat_runtime::MeerkatMachine>,
    Arc<dyn BinaryBlobStore>,
    Option<ScheduleHostInputs>,
    Option<WorkGraphParts>,
    meerkat_mobkit::storage_health::ResolvedStorageSummary,
    meerkat_mobkit::mob_handle_runtime::SessionWriteEpochsHandle,
    Arc<dyn meerkat_mobkit::identity_first::CommittedBoundaryRecoverer>,
    Arc<dyn meerkat_runtime::RuntimeStore>,
    ConsoleLiveInputs,
    meerkat_mobkit::mob_handle_runtime::MobRuntimeDelivery,
);
type ConsoleLiveInputs = (
    Arc<PersistentSessionService<FactoryAgentBuilder>>,
    Arc<meerkat_runtime::MeerkatMachine>,
    AgentFactory,
    Config,
    AgentMobToolsSlot,
);
type AgentMobToolsSlot =
    Arc<std::sync::RwLock<Option<Arc<dyn meerkat_core::service::MobToolsFactory>>>>;

#[derive(Debug, Deserialize)]
struct InitParams {
    workspace_root: Option<PathBuf>,
    project_root: Option<PathBuf>,
    context_root: Option<PathBuf>,
    runtime_root: Option<PathBuf>,
    store_path: Option<PathBuf>,
    persistent_sessions: Option<bool>,
    /// Explicit authenticated HTTP voice registration, using the shared
    /// OpenAI profile shape. Requires persistent sessions and auth_config.
    console_voice: Option<Value>,
    /// Same explicit JWT console authentication contract as rpc_gateway.
    auth_config: Option<Value>,
    realm: Option<String>,
    isolated: Option<bool>,
    surface: Option<String>,
    runtime_profile: Option<String>,
    console_read_only: Option<bool>,
    /// Run this gateway identity-first (meerkat-studio ask K0): durable
    /// identities with continuity records, lease-fenced embodiment, and the
    /// tolerant identity lifecycle paths — instead of the session-owned
    /// mob-member surface (where the ask-20 retire/respawn class lives).
    identity_first: Option<bool>,
    /// Desired identity roster for `identity_first: true` — restored at boot
    /// and reconciled thereafter. `mobkit/ensure_member` adds to it at
    /// runtime.
    identity_roster: Option<Vec<meerkat_mobkit::identity_first::DurableAgentSpec>>,
    /// Member role migrations this activation is authorized to perform, e.g.
    /// `[{"identity": "domain:automation", "from_role": "domain"}]`.
    ///
    /// A durable member whose role changed refuses to resume
    /// (`MobError::MemberRoleMigrationRequired`) until the host names it here,
    /// which is deliberate: an unintended role edit must not silently restamp
    /// a member's durable role, comms name and binding. Declaring the
    /// migration is the host asserting "yes, this identity moved, and it moved
    /// FROM this role".
    ///
    /// Boot-scoped. It is read once into the session bridge, never persisted,
    /// and gone the moment it is absent from the next boot payload. Meerkat
    /// re-verifies the declared predecessor against durable state, so a
    /// mistyped `from_role` refuses rather than restamps, and it ignores the
    /// declaration entirely once the roles already agree - so leaving a
    /// completed migration in the payload is inert, not a repeat restamp.
    #[serde(default)]
    role_migrations: Option<Vec<meerkat_mobkit::identity_first::RoleMigrationDeclaration>>,
    /// Host-level session-compaction policy for every agent this gateway
    /// builds (`{"auto_compact_threshold": 120000, ...}`). Absent, the
    /// gateway inherits meerkat's model-aware default —
    /// `context_window * 4 / 5`, i.e. `840_000` tokens on a million-token
    /// model, which in practice never fires. Validated at init through
    /// [`meerkat_mobkit::parse_compaction_policy`].
    compaction: Option<Value>,
    /// Meerkat host `config.toml` for every agent this gateway builds: the
    /// `[self_hosted]`, `[realm]` and `[models]` tables meerkat keeps out of
    /// `mob.toml`. Absent, `<workspace>/.rkat/config.toml` is adopted when it
    /// exists; otherwise agents build from meerkat's default config as
    /// before. A named file that is missing or malformed refuses init. A
    /// relative path resolves against the launch directory.
    meerkat_config_path: Option<PathBuf>,
    /// HTTP listener bind address, `HOST:PORT` (default `127.0.0.1:0`:
    /// loopback, ephemeral port). Wins over `--http-listen` and
    /// `MOBKIT_HTTP_LISTEN_ADDR`. A non-loopback address is refused unless
    /// `allow_remote` acknowledges the exposure. Without auth_config the
    /// console is open, so loopback remains the default protection.
    http_listen: Option<String>,
    /// Explicit acknowledgement that `http_listen` exposes the listener
    /// beyond this host; also `--allow-remote` or `MOBKIT_HTTP_ALLOW_REMOTE=1`.
    /// Same word and rule as `--allow-remote` on `rkat-rpc --tcp`.
    allow_remote: Option<bool>,
    /// Base URL clients reach this gateway at through a proxy. Reported back
    /// as `http_public_base_url` (and remembered for a resumed launch); never
    /// used to bind.
    http_public_base_url: Option<String>,
    /// Present ONLY so it can be refused.
    ///
    /// `mobkit_gateway` does not build the application tool-policy registry;
    /// `rpc_gateway` does. `InitParams` carries no `deny_unknown_fields`, so
    /// without this field serde drops the key and a host that compiled,
    /// reviewed and deployed an artifact gets no registry, no error and no
    /// warning - the exact "looks deployed, does nothing" state the
    /// wrong-id-space refusal in `member_tool_policy` exists to prevent, one
    /// layer up and on the other binary. Refusing is not a narrower feature
    /// than implementing it here; it is the difference between a host knowing
    /// and not knowing.
    #[serde(default)]
    application_tool_policies: Option<Value>,
    /// Present ONLY so it can be refused, same rule as `application_tool_policies`.
    ///
    /// The whole `runtime_options` object (demo_llm, max_sessions,
    /// identity_bootstrap_mode, experimental_live, and #376's
    /// declare_spec_update) is an rpc_gateway protocol; this binary parses none
    /// of it. `InitParams` carries no `deny_unknown_fields`, so without this
    /// field serde dropped the object and every option inside it in silence -
    /// an operator who declared a spec update against this binary would boot into
    /// the exact pinned-definition refusal the declaration exists to clear, with
    /// no sign it was ever read. Both SDKs let the operator point at either
    /// binary (`gateway_bin` / `gatewayBin`), so the wrong-binary case is real.
    #[serde(default)]
    runtime_options: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct RuntimeRegistry {
    entries: Vec<RuntimeRegistryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RuntimeRegistryEntry {
    key: String,
    runtime_id: String,
    http_base_url: String,
    pid: u32,
    updated_at_ms: u64,
    /// Dialable cross-mob control-listener address of the live gateway, so
    /// a resumed launch can still report it. Serde-defaulted: registries
    /// written by older gateways simply resume with no control address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    control_listen_address: Option<String>,
    /// Proxy-facing base URL the live gateway was launched with, so a
    /// resumed launch reports the same one. Serde-defaulted for registries
    /// written by older gateways.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    http_public_base_url: Option<String>,
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn short_hash(value: &str) -> String {
    value.chars().take(8).collect()
}

fn load_registry(path: &Path) -> RuntimeRegistry {
    fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

fn save_registry(path: &Path, registry: &RuntimeRegistry) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let text = serde_json::to_string_pretty(registry)?;
    fs::write(path, text)?;
    Ok(())
}

async fn url_is_alive(url: &str) -> bool {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
    {
        Ok(client) => client,
        Err(_) => return false,
    };

    client
        .get(format!("{}/healthz", url.trim_end_matches('/')))
        .send()
        .await
        .map(|response| response.status().is_success())
        .unwrap_or(false)
}

fn conventional_paths(workspace_root: &Path) -> ConventionalPaths {
    ConventionalPaths::discover(
        workspace_root.join("config"),
        workspace_root.join("deployment"),
    )
    .with_meerkat_config_from_workspace(workspace_root)
}

fn collect_recursive_files(root: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_recursive_files(&path, files);
        } else if path.is_file() {
            files.push(path);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn config_fingerprint(
    workspace_root: &Path,
    realm: Option<&str>,
    isolated: bool,
    runtime_profile: &str,
    persistent_sessions: bool,
    console_read_only: bool,
    runtime_root: &Path,
    store_path: &Path,
    project_root: &Path,
    context_root: Option<&Path>,
    compaction: Option<&meerkat_core::config::CompactionRuntimeConfig>,
    control_listen: Option<&ControlListenAddr>,
    http_listen: SocketAddr,
    http_public_base_url: Option<&str>,
    paths: &ConventionalPaths,
) -> anyhow::Result<String> {
    let mut hasher = Sha256::new();
    let realpath = workspace_root
        .canonicalize()
        .unwrap_or_else(|_| workspace_root.to_path_buf());
    hasher.update(realpath.to_string_lossy().as_bytes());
    hasher.update(b"\n");
    hasher.update(realm.unwrap_or("").as_bytes());
    hasher.update(b"\n");
    hasher.update(if isolated { b"1" } else { b"0" });
    hasher.update(b"\n");
    hasher.update(runtime_profile.as_bytes());
    hasher.update(b"\n");
    hasher.update(if persistent_sessions { b"1" } else { b"0" });
    hasher.update(b"\n");
    hasher.update(if console_read_only { b"1" } else { b"0" });
    hasher.update(b"\n");
    hasher.update(runtime_root.to_string_lossy().as_bytes());
    hasher.update(b"\n");
    hasher.update(store_path.to_string_lossy().as_bytes());
    hasher.update(b"\n");
    hasher.update(project_root.to_string_lossy().as_bytes());
    hasher.update(b"\n");
    if let Some(ctx) = context_root {
        hasher.update(ctx.to_string_lossy().as_bytes());
    }
    hasher.update(b"\n");
    // The compaction policy is baked into every agent this runtime builds, so
    // a launch that changes it must NOT resume a runtime built with the old
    // one — that would be the dead-knob failure with extra steps.
    if let Some(policy) = compaction {
        hasher.update(policy.auto_compact_threshold.to_le_bytes());
        hasher.update(if policy.auto_compact_threshold_explicit {
            b"1"
        } else {
            b"0"
        });
        hasher.update(policy.recent_turn_budget.to_le_bytes());
        hasher.update(policy.max_summary_tokens.to_le_bytes());
        hasher.update(policy.min_turns_between_compactions.to_le_bytes());
    }
    hasher.update(b"\n");
    // The control listener binds at launch; resuming a live gateway that was
    // launched without (or with a different) --control-listen would silently
    // drop the flag, so the address participates in the resume key.
    if let Some(addr) = control_listen {
        hasher.update(addr.to_string().as_bytes());
    }
    hasher.update(b"\n");
    // The HTTP listener likewise binds at launch: a relaunch that asks for a
    // different address must not resume a live runtime bound somewhere else
    // and report that one as if it were the requested bind.
    hasher.update(http_listen.to_string().as_bytes());
    hasher.update(b"\n");
    // The advertised base is reported back and remembered in the registry; a
    // launch that declares a different one must not be answered with the
    // previous launch's value, so it joins the key beside the listen address.
    hasher.update(http_public_base_url.unwrap_or("").as_bytes());
    hasher.update(b"\n");
    hasher.update(env!("CARGO_PKG_VERSION").as_bytes());

    let definition_json = workspace_root.join("definition.json");
    if paths.mob_toml.is_some() {
        hasher.update(b"\nworkspace-config");
    } else if definition_json.exists() {
        hasher.update(b"\ndefinition-json");
    } else {
        // Version the generated fallback runtime separately so local TUX
        // launches do not resume older minimal runtimes after capability
        // changes such as new profiles, tools, or wiring defaults.
        hasher.update(b"\nfallback-template:");
        hasher.update(FALLBACK_TEMPLATE_VERSION.as_bytes());
    }

    let mut files = Vec::new();
    if let Some(path) = &paths.mob_toml {
        files.push(path.clone());
    }
    if let Some(path) = &paths.gating_toml {
        files.push(path.clone());
    }
    if let Some(path) = &paths.console_toml {
        files.push(path.clone());
    }
    if let Some(path) = &paths.routing_toml {
        files.push(path.clone());
    }
    files.extend(paths.schedule_files.clone());
    // The host config is baked into every agent (self-hosted aliases, realm
    // bindings, custom models), so a changed or re-pointed config.toml must
    // not resume a runtime built from the old one. `.rkat/` is outside the
    // scanned config roots below, so it rides here by name.
    if let Some(path) = &paths.meerkat_config_toml {
        files.push(path.clone());
    }
    if definition_json.exists() {
        files.push(definition_json);
    }
    let manifest_toml = workspace_root.join("manifest.toml");
    if manifest_toml.exists() {
        files.push(manifest_toml);
    }
    // Scan workspace root and any override roots for config files
    let mut scan_roots = vec![workspace_root.to_path_buf()];
    if project_root != workspace_root {
        scan_roots.push(project_root.to_path_buf());
    }
    if let Some(ctx) = context_root
        && ctx != workspace_root
    {
        scan_roots.push(ctx.to_path_buf());
    }
    for root in &scan_roots {
        for extra_dir in ["skills", "hooks", "mcp", "config"] {
            collect_recursive_files(&root.join(extra_dir), &mut files);
        }
    }
    files.sort();

    for path in files {
        hasher.update(b"\nfile:");
        hasher.update(path.to_string_lossy().as_bytes());
        if let Ok(bytes) = fs::read(&path) {
            hasher.update(b"\n");
            hasher.update(bytes);
        }
    }

    Ok(format!("{:x}", hasher.finalize()))
}

fn minimal_definition(runtime_id: &str) -> anyhow::Result<MobDefinition> {
    MobDefinition::from_toml(&format!(
        r#"
[mob]
id = "{runtime_id}"
orchestrator = "alpha"

[profiles.alpha]
model = "gpt-5.5"
skills = ["alpha-role"]
peer_description = "Runtime guide -- expands this runtime into a small mob and coordinates peers"
external_addressable = true

[profiles.alpha.tools]
builtins = true
comms = true
mob = true
mob_tasks = true

[profiles.worker]
model = "gpt-5.5"
skills = ["worker-role"]
peer_description = "General-purpose peer meerkat"
external_addressable = true

[profiles.worker.tools]
builtins = true
comms = true
mob_tasks = true

[wiring]
auto_wire_orchestrator = true

[skills.alpha-role]
source = "inline"
content = """
## Role
You are Alpha, the runtime guide for a lightweight Meerkat workspace.

## What You Can Do
- Answer directly when the job is simple.
- Grow the runtime into a small mob when parallel work helps.
- Spawn classic sub-agents for delegated background work.
- Spawn peer meerkats when a longer-lived collaborator should appear in the runtime.

## Preferred Growth Pattern
- For quick delegated work, use sub-agent tools.
- For visible collaborators inside this runtime, use mob tools to spawn worker peers.
- When you spawn worker peers, they should appear in the shared runtime UI.

## Coordination
- Use mob tools to spawn, list, wire, and retire meerkats.
- Use peers() and send() when peers are available.
- If asked to create a small team, prefer spawning `worker` peers unless the user clearly asks for classic sub-agents.

## Communication Style
Be explicit about whether you used a sub-agent or spawned a peer meerkat.
"""

[skills.worker-role]
source = "inline"
content = """
You are a general-purpose worker meerkat inside a lightweight runtime.
Complete assigned tasks concisely and report status back to Alpha.
If peer messaging is available, use it to report completion or blockers.
"""
"#
    ))
    .map_err(|error| anyhow!("invalid fallback mob definition: {error}"))
}

fn load_definition(
    workspace_root: &Path,
    fingerprint: &str,
    paths: &ConventionalPaths,
) -> anyhow::Result<(MobDefinition, bool)> {
    if let Some(path) = &paths.mob_toml {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let definition = MobDefinition::from_toml(&text)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        // The definition parser drops `[self_hosted]` and `[realm]` without a
        // diagnostic; refuse them by name (the typed message names
        // `meerkat_config_path` as the file that carries them).
        meerkat_mobkit::gateway_composition::refuse_host_config_tables_in_mob_toml(&text)
            .map_err(|table| anyhow!("{}: {table}", path.display()))?;
        return Ok((definition, true));
    }

    let definition_json_path = workspace_root.join("definition.json");
    if definition_json_path.exists() {
        let text = fs::read_to_string(&definition_json_path)
            .with_context(|| format!("failed to read {}", definition_json_path.display()))?;
        let definition = serde_json::from_str::<MobDefinition>(&text)
            .with_context(|| format!("failed to parse {}", definition_json_path.display()))?;
        return Ok((definition, true));
    }

    let runtime_id = format!("tux-{}", short_hash(fingerprint));
    Ok((minimal_definition(&runtime_id)?, false))
}

/// The `meerkat::Config` every agent this gateway builds starts from: the
/// host `config.toml` when one was resolved (the only carrier of
/// `[self_hosted]`, `[realm]` and `[models]`), else meerkat's default, with
/// the gateway's compaction declaration layered on top. One producer for both
/// launch arms, so the persistent and ephemeral paths cannot drift on which
/// tables reach the factory.
fn gateway_agent_config(
    host_config: Option<&Config>,
    compaction: Option<&meerkat_core::config::CompactionRuntimeConfig>,
) -> anyhow::Result<Config> {
    let mut config = host_config.cloned().unwrap_or_default();
    if let Some(policy) = compaction {
        // The compaction slot of this config is what meerkat's
        // `AgentFactory::build_agent` turns into the session compactor; an
        // absent declaration inherits the model-aware `context_window * 4 / 5`
        // trigger.
        meerkat_mobkit::apply_compaction_policy(&mut config, policy).map_err(|e| anyhow!("{e}"))?;
    }
    Ok(config)
}

/// Returns (session_service, runtime_adapter, binary_blob_store).
///
/// The session service and explicit runtime adapter share the same runtime
/// store. The service acquires that adapter before its schedule, voice, and
/// mob hosts are composed.
#[allow(clippy::too_many_arguments)]
fn build_persistent_session_service(
    layout: &MobKitStorageLayout,
    runtime_root: PathBuf,
    project_root: PathBuf,
    context_root: Option<PathBuf>,
    image_generation: bool,
    mcp_apps: bool,
    realm_id: &str,
    host_config: Option<&Config>,
    compaction: Option<&meerkat_core::config::CompactionRuntimeConfig>,
) -> anyhow::Result<PersistentSessionServiceParts> {
    let store_dir = layout.state_dir().to_path_buf();
    fs::create_dir_all(&store_dir)
        .with_context(|| format!("failed to create {}", store_dir.display()))?;
    let sqlite_path = layout.session_db().map_err(|e| anyhow!("{e}"))?.path;
    let session_store = Arc::new(
        SqliteSessionStore::open(sqlite_path.clone())
            .with_context(|| format!("failed to open {}", sqlite_path.display()))?,
    );
    // H2: probe the incremental capability on the same store the session
    // service receives below, so whole-blob degradation is loud and
    // health-visible.
    let session_store_incremental = meerkat_mobkit::storage_health::probe_session_store_incremental(
        &(session_store.clone() as Arc<dyn meerkat::SessionStore>),
        "SqliteSessionStore",
    );

    let binary_blob_store: Arc<dyn BinaryBlobStore> =
        Arc::new(ObjectStoreBlobStore::local(layout.blob_root())?);
    let blob_store: Arc<dyn meerkat_core::BlobStore> =
        Arc::new(Base64BlobStoreAdapter::new(binary_blob_store.clone()));
    // Persistent runtime store — same path chosen by
    // `MobBootstrapSpec::persistent_inner`, so a gateway and a library-mode
    // runtime pointed at the same dir share state.
    //
    // Fail-closed (M4): an open failure is a startup error — the former
    // silent `InMemoryRuntimeStore` fallback left resume and archive broken
    // long after boot. This surface has no ephemeral-runtime-store
    // declaration; its ephemeral launch mode (persistent_sessions = false)
    // is the declared in-memory path.
    let runtime_db_path = layout.runtime_db();
    let runtime_store: Arc<dyn meerkat_runtime::RuntimeStore> = Arc::new(
        meerkat_runtime::store::SqliteRuntimeStore::new(&runtime_db_path).map_err(|err| {
            anyhow!(
                "{}",
                meerkat_mobkit::storage_health::RuntimeStoreResolutionError {
                    path: runtime_db_path.clone(),
                    message: err.to_string(),
                }
            )
        })?,
    );
    // Wrap in the write-epoch facade BEFORE the machine and session service
    // capture the store; the witness threads to the bootstrap spec so the
    // console session-history epoch gate works on this externally-composed
    // path (mirrors rpc_gateway.rs — without it the 5s discovery loop
    // re-reads whole session documents forever). The facade also owns the
    // durable session projection at meerkat 0.8.11 (the session service no
    // longer writes the SessionStore itself) and every-boot authority
    // re-minting - a reset/lost runtime.sqlite reseeds from the durable
    // session rows instead of refusing resume.
    let (runtime_store, session_write_epochs) =
        meerkat_mobkit::mob_handle_runtime::epoch_tracking_runtime_store_with_durable_projection(
            runtime_store,
            session_store.clone() as Arc<dyn meerkat::SessionStore>,
        );
    let adapter = Arc::new(
        meerkat_runtime::MeerkatMachine::persistent(
            Arc::clone(&runtime_store),
            Arc::clone(&blob_store),
        )
        .context("failed to acquire the gateway runtime machine")?,
    );
    let mut factory = AgentFactory::new(store_dir)
        .session_store(session_store.clone())
        .runtime_root(runtime_root)
        .project_root(project_root)
        .builtins(true)
        .shell(true)
        .mob(true)
        .comms(true)
        .memory(true)
        .mcp_apps(mcp_apps);
    if image_generation {
        factory = factory.with_image_generation_machine(adapter.clone());
    }
    if let Some(context_root) = context_root {
        factory = factory.context_root(context_root);
    }

    let config = gateway_agent_config(host_config, compaction)?;
    let live_factory = factory.clone();
    let live_config = config.clone();
    let mut builder = FactoryAgentBuilder::new(factory, config);
    builder.default_blob_store = Some(blob_store.clone());
    let jobs_path = meerkat_store::realm_paths_in(
        layout.state_dir(),
        meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
    )
    .jobs_sqlite_path;
    let jobs: Arc<dyn meerkat::DetachedJobStore> = Arc::new(
        meerkat::SqliteDetachedJobStore::open(jobs_path.clone())
            .with_context(|| format!("failed to open {}", jobs_path.display()))?,
    );
    let runtime_delivery = meerkat_mobkit::mob_handle_runtime::MobRuntimeDelivery::new(
        Arc::clone(&runtime_store),
        Arc::clone(&jobs),
    );
    builder.default_detached_job_store = Some(Arc::clone(&jobs));
    builder.default_shell_job_delivery_projector =
        Some(meerkat::JobOutboxProjector::new_for_realm(
            jobs,
            runtime_delivery.inbox(),
            meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
        ));

    // Attach meerkat's per-session schedule tools so members whose profile sets
    // tools.schedule=true get the meerkat_schedule_* surface; the returned
    // service backs the firing host spawned once the runtime has booted.
    let (schedule_tools, schedule_slot) =
        match meerkat_mobkit::schedule_wiring::attach_schedule_tools_with_identity_targets_reporting(
            &builder,
            layout.state_dir(),
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
                meerkat_mobkit::storage_health::StorageSlotSummary::degraded(
                    "schedule",
                    format!("schedule store failed to open; schedule tools disabled: {error}"),
                ),
            ),
        };
    // WorkGraph: durable store beside the schedule store, realm scoped to
    // the mob definition id (member tools + overlays + console RPCs). The
    // state dir travels along so the bootstrap spec can place the
    // cross-process admission sidecar beside the store.
    let workgraph_state_dir = layout.state_dir().to_path_buf();
    let (workgraph, workgraph_slot) =
        match meerkat_mobkit::workgraph_wiring::attach_workgraph_tools_reporting(
            &builder,
            &workgraph_state_dir,
            realm_id,
        ) {
            Ok((service, admission_slot)) => (
                Some((service, admission_slot, workgraph_state_dir)),
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
        };
    let agent_mob_tools_slot = Arc::clone(&builder.default_mob_tools);
    let service = Arc::new(PersistentSessionService::new(
        builder,
        64,
        session_store,
        Arc::clone(&runtime_store),
        blob_store,
    ));
    service
        .acquire_canonical_runtime_adapter(Some(Arc::clone(&adapter)))
        .context("failed to acquire the persistent gateway session runtime owner")?;
    let schedule_host_inputs = schedule_tools.map(|tools| {
        (
            tools.service,
            tools.mob_target_registry,
            Arc::clone(&service),
            layout.schedule_db(),
            tools.firing_host_binding,
        )
    });
    // Blob slot resolved fail-closed above (local disk under
    // <store_dir>/blobs). The console timeline and metadata cursor of this
    // surface are in-memory by contract (`UnifiedRuntime::bootstrap` keeps
    // its defaults) — a declared default, documented and health-visible
    // (M4), not an error.
    let mut slots = vec![
        meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
            "sessions",
            "SqliteSessionStore",
        ),
        meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
            "runtime",
            "SqliteRuntimeStore",
        ),
        meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
            "jobs",
            "SqliteDetachedJobStore",
        ),
        meerkat_mobkit::storage_health::blob_slot_summary(
            meerkat_mobkit::storage_health::BlobDurability::PersistentDisk,
        ),
        meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
            "console",
            "InMemoryConsoleLogStore",
            "declared default of this surface (UnifiedRuntime::bootstrap keeps in-memory \
             console/metadata)",
        ),
        meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
            "metadata",
            "InMemoryMetadataStore",
            "declared default of this surface (UnifiedRuntime::bootstrap keeps in-memory \
             console/metadata)",
        ),
        schedule_slot,
        workgraph_slot,
        meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
            "jobs",
            "SqliteDetachedJobStore",
        ),
    ];
    slots.extend(meerkat_mobkit::storage_health::scratch_ring_buffer_slots());
    // Heal seam (2026-07-29 incident): the CONCRETE persistent service is the
    // committed-boundary recoverer; cast here, before the tuple erases it to
    // `dyn MobSessionService` (mirrors rpc_gateway.rs).
    let committed_boundary_recoverer: Arc<
        dyn meerkat_mobkit::identity_first::CommittedBoundaryRecoverer,
    > = service.clone();
    let voice_inputs = (
        Arc::clone(&service),
        Arc::clone(&adapter),
        live_factory,
        live_config,
        agent_mob_tools_slot,
    );
    Ok((
        service,
        adapter,
        binary_blob_store,
        schedule_host_inputs,
        workgraph,
        meerkat_mobkit::storage_health::ResolvedStorageSummary::new(
            meerkat_mobkit::storage_health::BlobDurability::PersistentDisk,
            Some(session_store_incremental),
        )
        .with_slots(slots)
        .with_runtime_store_locator(Some(
            meerkat_mobkit::storage_health::RuntimeStoreLocator::SqliteFile {
                path: layout.runtime_db(),
            },
        )),
        session_write_epochs,
        committed_boundary_recoverer,
        runtime_store,
        voice_inputs,
        runtime_delivery,
    ))
}

/// Session-store naming the standalone gateway projects into its session-store
/// descriptor. Read at serve time, so it is pinned here rather than inherited
/// from the shared constructor's SDK-gateway default.
const TUX_BIGQUERY_DATASET: &str = "tux_local";
const TUX_BIGQUERY_TABLE: &str = "runtime_events";

/// Default standalone console snapshot. Explicit auth_config replaces its
/// authentication fields at launch; otherwise the loopback host is the
/// protection. Shared non-auth defaults come from the keyless snapshot owner.
fn runtime_decision_state(
    console_ui: ConsoleUiConfig,
    console_read_only: bool,
) -> RuntimeDecisionState {
    RuntimeDecisionState::local_console(
        ConsolePolicy {
            require_app_auth: false,
            read_only: console_read_only,
            fetch_timeout_ms: None,
            ui: console_ui,
        },
        Some(BigQueryNaming {
            dataset: TUX_BIGQUERY_DATASET.to_string(),
            table: TUX_BIGQUERY_TABLE.to_string(),
        }),
    )
}

fn print_json_line(value: &Value) {
    let line = serde_json::to_string(value)
        .unwrap_or_else(|_| r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"serialization failed"}}"#.to_string());
    let mut stdout = io::stdout().lock();
    let _ = writeln!(stdout, "{line}");
    let _ = stdout.flush();
}

fn parse_init_request(line: &str) -> anyhow::Result<(Value, InitParams)> {
    let raw: Value = serde_json::from_str(line).context("failed to parse init request")?;
    let method = raw
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if method != "mobkit/init" {
        return Err(anyhow!("expected mobkit/init, got {method}"));
    }
    let params = raw.get("params").cloned().unwrap_or_else(|| json!({}));
    let parsed: InitParams = serde_json::from_value(params).context("invalid init params")?;
    if parsed.runtime_options.is_some() {
        return Err(anyhow!(
            "runtime_options is not implemented by mobkit_gateway; it takes effect only on the \
             rpc_gateway binary (demo_llm, max_sessions, identity_bootstrap_mode, \
             experimental_live, declare_spec_update). Refusing rather than ignoring: a silently \
             dropped option arms nothing while looking deployed, and a dropped \
             declare_spec_update leaves the pinned-definition refusal in place while the \
             operator believes they cleared it"
        ));
    }
    if parsed.application_tool_policies.is_some() {
        return Err(anyhow!(
            "application_tool_policies is not implemented by mobkit_gateway; it takes effect only \
             on the rpc_gateway binary. Refusing rather than ignoring: a silently dropped policy \
             arms nothing while looking deployed, and the only later evidence would be an absence \
             of denials, which is what an armed and fully granted policy also produces"
        ));
    }
    if let Some(auth) = parsed.auth_config.as_ref() {
        meerkat_mobkit::console_auth_config::parse_console_auth_config(auth)
            .map_err(|error| anyhow!("{error}"))?;
    }
    if let Some(voice) = parsed.console_voice.as_ref() {
        if !cfg!(feature = "openai-live") {
            return Err(anyhow!(
                "console_voice requires a gateway built with openai-live"
            ));
        }
        if !parsed.persistent_sessions.unwrap_or(false) || parsed.auth_config.is_none() {
            return Err(anyhow!(
                "console_voice requires persistent_sessions and auth_config"
            ));
        }
        meerkat_mobkit::public_live_config::PublicLiveRegistration::parse(voice)
            .map_err(|error| anyhow!("console_voice: {error}"))?;
    }
    Ok((raw.get("id").cloned().unwrap_or(Value::Null), parsed))
}

/// Resolve the HTTP listen address: init param, then `--http-listen`, then
/// `MOBKIT_HTTP_LISTEN_ADDR`, then loopback on an ephemeral port. Pure so the
/// precedence is testable without touching the process environment. An empty
/// environment value counts as unset, like `env_bool`.
fn resolve_http_listen(
    init_param: Option<&str>,
    flag: Option<SocketAddr>,
    env_value: Option<&str>,
) -> Result<SocketAddr, String> {
    if let Some(value) = init_param {
        return meerkat_mobkit::gateway_composition::parse_http_listen_addr(value)
            .map_err(|error| error.to_string());
    }
    if let Some(addr) = flag {
        return Ok(addr);
    }
    match env_value.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => meerkat_mobkit::gateway_composition::parse_http_listen_addr(value)
            .map_err(|error| format!("MOBKIT_HTTP_LISTEN_ADDR: {error}")),
        None => Ok(meerkat_mobkit::gateway_composition::DEFAULT_GATEWAY_HTTP_LISTEN),
    }
}

fn env_bool(name: &str) -> anyhow::Result<Option<bool>> {
    let Ok(value) = std::env::var(name) else {
        return Ok(None);
    };
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "" => Ok(None),
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => Err(anyhow!("{name} must be a boolean value")),
    }
}

fn init_response(
    request_id: Value,
    runtime_id: &str,
    http_base_url: &str,
    launch_state: &str,
    control_listen_address: Option<&str>,
    http_public_base_url: Option<&str>,
) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "contract_version": MOBKIT_CONTRACT_VERSION,
            "runtime_id": runtime_id,
            "http_base_url": http_base_url,
            // Proxy-facing base the launch advertised (http_public_base_url
            // init param); null when none was declared. Never bound.
            // Wire-additive optional field.
            "http_public_base_url": http_public_base_url,
            "launch_state": launch_state,
            // Dialable address of the cross-mob control listener when the
            // gateway was launched with --control-listen (real bound port
            // for tcp://host:0); null otherwise. Peers put this address in
            // their contact directories. Wire-additive optional field.
            "control_listen_address": control_listen_address,
        }
    })
}

/// Marker error: `run()` has already written the init reply for this failure
/// on the request id (`-32602` for a refused declaration, `-32603` for a
/// failed bind), so `main` must not write a second, id-less one.
#[derive(Debug)]
struct InitReplied;

impl std::fmt::Display for InitReplied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("mobkit/init was refused; the reply is already on stdout")
    }
}

impl std::error::Error for InitReplied {}

/// Write a typed init refusal on the request id and return the marker
/// `run()` propagates. Invalid declarations are `-32602` (the same code
/// `rpc_gateway` answers with), so an SDK gets a caller action rather than an
/// "internal error" with a null id.
fn refuse_init(request_id: &Value, code: i64, message: String) -> anyhow::Error {
    tracing::warn!(code, %message, "mobkit/init refused");
    print_json_line(&init_error(request_id.clone(), code, message));
    anyhow::Error::new(InitReplied)
}

fn init_error(request_id: Value, code: i64, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {
            "code": code,
            "message": message,
        }
    })
}

const STORAGE_MIGRATE_USAGE: &str = "usage: mobkit_gateway storage-migrate --state-dir <dir> \
     [--apply] [--adopt <path>] [--json]\n\
     Fenced offline migration of one MobKit state directory \
     (storage-unification M6): ledger baseline, legacy-spelling renames, \
     twin reconciliation, leftover census.\n\
     Dry-run by default; --apply mutates under the exclusive maintenance \
     fence. --adopt <path> resolves a divergent file-name twin by adopting \
     that copy and archiving the rest read-only (requires --apply).\n\
     --acknowledge-skipped <SESSION_ID> (repeatable) authorises the ledger \
     bump to proceed despite a blob row that cannot be parsed as a session. \
     Acknowledgement is BY ROW ID, never by count: a row you did not name \
     still blocks, so a later run cannot silently authorise a different set \
     than the one you read.\n\
     Exit codes: 0 clean, 1 refusals or fence/store failure, 2 usage error.";

/// Maintenance verb: `mobkit_gateway storage-migrate`. Runs the five-case
/// M6 migration pass and prints the report. Like the H3 verb, it is a
/// standalone argv verb bypassing the stdin init handshake — migration is
/// never an eager side effect of ordinary gateway startup.
fn run_storage_migrate(args: &[String]) -> i32 {
    let mut state_dir: Option<PathBuf> = None;
    let mut adopt: Option<PathBuf> = None;
    let mut apply = false;
    let mut json = false;
    let mut acknowledged_rows: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--state-dir" => match iter.next() {
                Some(value) => state_dir = Some(PathBuf::from(value)),
                None => {
                    eprintln!("--state-dir requires a directory\n{STORAGE_MIGRATE_USAGE}");
                    return 2;
                }
            },
            "--adopt" => match iter.next() {
                Some(value) => adopt = Some(PathBuf::from(value)),
                None => {
                    eprintln!("--adopt requires a path\n{STORAGE_MIGRATE_USAGE}");
                    return 2;
                }
            },
            "--acknowledge-skipped" => match iter.next() {
                Some(value) => {
                    acknowledged_rows.insert(value.clone());
                }
                None => {
                    eprintln!(
                        "--acknowledge-skipped requires a session id\n{STORAGE_MIGRATE_USAGE}"
                    );
                    return 2;
                }
            },
            "--apply" => apply = true,
            "--json" => json = true,
            other => {
                eprintln!("unknown argument {other:?}\n{STORAGE_MIGRATE_USAGE}");
                return 2;
            }
        }
    }
    let Some(state_dir) = state_dir else {
        eprintln!("--state-dir is required\n{STORAGE_MIGRATE_USAGE}");
        return 2;
    };
    if adopt.is_some() && !apply {
        eprintln!("--adopt requires --apply\n{STORAGE_MIGRATE_USAGE}");
        return 2;
    }
    let mode = if apply {
        meerkat_mobkit::MigrateMode::Apply
    } else {
        meerkat_mobkit::MigrateMode::DryRun
    };
    let report = meerkat_mobkit::migrate_state_dir_acknowledging_skipped(
        &state_dir,
        mode,
        adopt.as_deref(),
        &acknowledged_rows,
    );
    if json {
        match serde_json::to_string_pretty(&report) {
            Ok(text) => println!("{text}"),
            Err(error) => {
                eprintln!("failed to serialize migrate report: {error}");
                return 1;
            }
        }
    } else {
        print_migrate_report_text(&report);
    }
    i32::from(report.has_errors())
}

fn print_migrate_report_text(report: &meerkat_mobkit::MobKitMigrateReport) {
    let mode = match report.mode {
        meerkat_mobkit::MigrateMode::Apply => "apply",
        _ => "dry-run",
    };
    println!(
        "Storage migrate ({mode}) over {} ({} database(s) fenced):",
        report.state_dir.display(),
        report.fenced_databases.len()
    );
    if let Some(backfill) = &report.head_canonical_backfill {
        // The head-canonical crossing is the one irreversible thing this verb
        // does, so its outcome is printed before anything else and states
        // explicitly whether the ledger advanced.
        println!(
            "head-canonical backfill: {} converted of {} pending",
            backfill.converted.len(),
            backfill.examined
        );
        if !backfill.skipped_unparseable.is_empty() {
            println!(
                "  {} blob row(s) could not be parsed as sessions and are NOT converted; \
                 the ledger will not advance until each is acknowledged by id \
                 (--acknowledge-skipped <SESSION_ID>): {}",
                backfill.skipped_unparseable.len(),
                backfill.skipped_unparseable.join(", ")
            );
        }
        for (session_id, failure) in &backfill.failures {
            if session_id.is_empty() {
                println!("  REFUSED: {failure}");
            } else {
                println!("  FAILED {session_id}: {failure}");
            }
        }
        if backfill.ledger_stamped {
            println!(
                "  ledger STAMPED v2 - the corpus is wholly head-canonical; \
                 rollback to a pre-head-canonical release is no longer possible"
            );
        } else if backfill.applied {
            println!(
                "  ledger LEFT AT v1 - the crossing is incomplete, so rollback \
                 remains available; re-run to resume"
            );
        }
    }
    for twin in &report.twins {
        println!("twin [{}]:", twin.slot);
        for path in &twin.paths {
            println!("  copy: {}", path.display());
        }
        println!(
            "  rows: {} equal across all copies; byte-identical: {}",
            twin.rows_equal, twin.byte_identical
        );
        for row in &twin.rows {
            let status = match &row.status {
                meerkat_mobkit::DivergenceStatus::Equal => "equal".to_string(),
                meerkat_mobkit::DivergenceStatus::Divergent => "divergent".to_string(),
                meerkat_mobkit::DivergenceStatus::OnlyIn { location } => {
                    format!("only in {}", location.display())
                }
                _ => "unknown".to_string(),
            };
            println!("    {}: {status}", row.key);
        }
        match &twin.resolution {
            meerkat_mobkit::TwinResolution::Refused { reason } => {
                println!("  resolution: REFUSED — {reason}");
            }
            meerkat_mobkit::TwinResolution::Deduped { kept, archived } => {
                println!("  resolution: deduped, kept {}", kept.display());
                for archive in archived {
                    println!("    archived read-only: {}", archive.display());
                }
            }
            meerkat_mobkit::TwinResolution::Adopted { adopted, archived } => {
                println!("  resolution: adopted {}", adopted.display());
                for archive in archived {
                    println!("    archived read-only: {}", archive.display());
                }
            }
            _ => println!("  resolution: unknown"),
        }
        for note in &twin.notes {
            println!("  note: {note}");
        }
        for error in &twin.errors {
            println!("  error: {error}");
        }
    }
    for rename in &report.renames {
        let action = match rename.action {
            meerkat_mobkit::RenameAction::WouldRename => "would rename",
            meerkat_mobkit::RenameAction::Renamed => "renamed",
            meerkat_mobkit::RenameAction::Refused => "REFUSED",
            _ => "unknown",
        };
        println!(
            "rename [{}]: {} -> {} ({action}, {} sibling(s), wal checkpointed: {})",
            rename.slot,
            rename.from.display(),
            rename.to.display(),
            rename.siblings.len(),
            rename.wal_checkpointed
        );
    }
    for entry in &report.ledger {
        let action = match entry.action {
            meerkat_mobkit::LedgerBaselineAction::WouldStamp => "would-stamp",
            meerkat_mobkit::LedgerBaselineAction::Recorded => "recorded",
            meerkat_mobkit::LedgerBaselineAction::Stamped => "stamped",
            meerkat_mobkit::LedgerBaselineAction::AlreadyCurrent => "already-current",
            meerkat_mobkit::LedgerBaselineAction::ReportOnly => "report-only",
            meerkat_mobkit::LedgerBaselineAction::Exempt => "exempt",
            _ => "unknown",
        };
        let describe =
            |version: Option<i64>| version.map_or_else(|| "none".to_string(), |v| v.to_string());
        println!(
            "ledger {} [{}]: {} -> {} ({action})",
            entry.database.display(),
            entry.domain,
            describe(entry.before),
            describe(entry.after)
        );
    }
    for finding in &report.findings {
        let path = finding
            .path
            .as_ref()
            .map(|path| format!(" at {}", path.display()))
            .unwrap_or_default();
        println!("leftover [{}] {}{path}", finding.code, finding.message);
    }
    for note in &report.notes {
        println!("note: {note}");
    }
    for error in &report.errors {
        println!("error: {error}");
    }
    println!("storage migrate: {} error(s)", report.errors.len());
}

const STORAGE_PRUNE_USAGE: &str = "usage: mobkit_gateway storage-prune --state-dir <dir> \
     [--apply] [--older-than-days N] [--json]\n\
     Lifecycle of registered maintenance artifacts (`*.pre-*` backups, \
     `*.corrupt-*` quarantines) under one MobKit state directory. Never \
     touches anything outside those naming patterns.\n\
     Dry-run by default; --apply deletes artifacts at least \
     --older-than-days old (default 30; 0 = all).\n\
     Exit codes: 0 clean, 1 delete failures, 2 usage error.";

/// Maintenance verb: `mobkit_gateway storage-prune`.
fn run_storage_prune(args: &[String]) -> i32 {
    let mut state_dir: Option<PathBuf> = None;
    let mut older_than_days: u64 = 30;
    let mut apply = false;
    let mut json = false;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--state-dir" => match iter.next() {
                Some(value) => state_dir = Some(PathBuf::from(value)),
                None => {
                    eprintln!("--state-dir requires a directory\n{STORAGE_PRUNE_USAGE}");
                    return 2;
                }
            },
            "--older-than-days" => match iter.next().map(|value| value.parse::<u64>()) {
                Some(Ok(days)) => older_than_days = days,
                _ => {
                    eprintln!("--older-than-days requires a number\n{STORAGE_PRUNE_USAGE}");
                    return 2;
                }
            },
            "--apply" => apply = true,
            "--json" => json = true,
            other => {
                eprintln!("unknown argument {other:?}\n{STORAGE_PRUNE_USAGE}");
                return 2;
            }
        }
    }
    let Some(state_dir) = state_dir else {
        eprintln!("--state-dir is required\n{STORAGE_PRUNE_USAGE}");
        return 2;
    };
    let mode = if apply {
        meerkat_mobkit::MigrateMode::Apply
    } else {
        meerkat_mobkit::MigrateMode::DryRun
    };
    let report = meerkat_mobkit::prune_state_dir(&state_dir, older_than_days, mode);
    if json {
        match serde_json::to_string_pretty(&report) {
            Ok(text) => println!("{text}"),
            Err(error) => {
                eprintln!("failed to serialize prune report: {error}");
                return 1;
            }
        }
    } else {
        let mode = if apply { "apply" } else { "dry-run" };
        println!(
            "Storage prune ({mode}, older than {} day(s)) over {}:",
            report.older_than_days,
            report.state_dir.display()
        );
        if report.artifacts.is_empty() {
            println!("No registered maintenance artifacts found.");
        }
        for artifact in &report.artifacts {
            let action = match artifact.action {
                meerkat_mobkit::PruneAction::WouldDelete => "would delete",
                meerkat_mobkit::PruneAction::Deleted => "deleted",
                meerkat_mobkit::PruneAction::Kept => "kept (younger than threshold)",
                meerkat_mobkit::PruneAction::DeleteFailed => "DELETE FAILED",
                _ => "unknown",
            };
            println!(
                "  {}  {} bytes, {} day(s) old — {action}",
                artifact.path.display(),
                artifact.bytes,
                artifact.age_days
            );
        }
        for error in &report.errors {
            println!("error: {error}");
        }
        println!("storage prune: {} error(s)", report.errors.len());
    }
    i32::from(report.has_errors())
}

/// Launch-time (argv) inputs `run` resolves against the init params. An init
/// param wins where both name the same thing; the environment
/// (`MOBKIT_HTTP_LISTEN_ADDR`, `MOBKIT_HTTP_ALLOW_REMOTE`) sits below both.
struct GatewayLaunchArgs {
    control_listen: Option<ControlListenAddr>,
    /// `--http-listen HOST:PORT`.
    http_listen: Option<SocketAddr>,
    /// `--allow-remote`.
    allow_remote: bool,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--version" || a == "-V") {
        println!(
            "mobkit_gateway {} (meerkat-mobkit console/HTTP gateway)",
            env!("CARGO_PKG_VERSION")
        );
        return;
    }
    // Install the tracing subscriber FIRST — before the maintenance verbs,
    // not just before the gateway boot. The storage verbs drive the same
    // migration code whose progress reports through tracing; dispatching
    // them ahead of this init dropped every progress line, and a 2026-07
    // production deploy was aborted because a supervisor read the
    // silent-but-working migration as a hang. Also (meerkat-studio K1/K2):
    // without any init, runtime failures, console internal-error logs, and
    // the schedule claim watchdog's stall diagnosis all vanish on the child
    // gateways their app spawns. Stderr, never stdout: stdout carries the
    // init JSON handshake and the verbs' report output.
    //
    // Default: this crate's own targets at INFO, dependencies at WARN;
    // RUST_LOG overrides everything. Shared with rpc_gateway so the two
    // gateways cannot drift on observability posture.
    meerkat_mobkit::gateway_composition::init_gateway_tracing(GATEWAY_TRACING_TARGET);
    if args.first().map(String::as_str) == Some("storage-migrate") {
        std::process::exit(run_storage_migrate(&args[1..]));
    }
    if args.first().map(String::as_str) == Some("storage-prune") {
        std::process::exit(run_storage_prune(&args[1..]));
    }
    // --control-listen <tcp://host:port | uds:///path>: bind the cross-mob
    // control listener so remote gateways can wire/unwire/inject/lookup
    // members of this runtime. Validated here so a typo is a launch error,
    // not a silently ignored flag.
    let control_listen = match meerkat_mobkit::gateway_composition::parse_control_listen_arg(&args)
    {
        Ok(value) => value,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };
    // --http-listen HOST:PORT / --allow-remote: the HTTP listener's bind
    // address and the exposure acknowledgement a non-loopback bind needs.
    // Init params win over these; MOBKIT_HTTP_LISTEN_ADDR and
    // MOBKIT_HTTP_ALLOW_REMOTE sit below them. Validated here so a typo is a
    // launch error, not a silently ignored flag.
    let http_listen = match meerkat_mobkit::gateway_composition::parse_http_listen_arg(&args) {
        Ok(value) => value,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };
    let allow_remote = args.iter().any(|arg| arg == "--allow-remote");
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        "mobkit_gateway starting (console/HTTP gateway)"
    );
    // Deep worker stacks for the generated machine-authority apply path; the
    // builder is shared with rpc_gateway, the failure reporting is not (this
    // binary's parent reads a JSON-RPC handshake on stdout).
    let runtime = match meerkat_mobkit::gateway_composition::gateway_tokio_runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            let response = init_error(Value::Null, -32603, error.to_string());
            print_json_line(&response);
            std::process::exit(1);
        }
    };
    if let Err(error) = runtime.block_on(Box::pin(run(GatewayLaunchArgs {
        control_listen,
        http_listen,
        allow_remote,
    }))) {
        // A refusal `run()` already answered on the request id must not be
        // followed by a second, id-less reply.
        if error.downcast_ref::<InitReplied>().is_none() {
            tracing::error!(error = %format!("{error:#}"), "gateway startup failed");
            let response = init_error(Value::Null, -32603, error.to_string());
            print_json_line(&response);
        }
        std::process::exit(1);
    }
}

/// Which `select!` branch ended `run()`: the `reason=` token of the "run loop
/// ended" log line. Named so an operator can tell a supervisor's signal from
/// a server-task death from a crash, which leaves no such line (only the
/// panic hook's ERROR, or nothing at all when the process was killed from
/// outside).
enum RunEnd {
    /// The HTTP server task ended on its own; the outcome says whether
    /// cleanly. Abnormal outcomes are logged at ERROR and fail `run()`.
    HttpServer(meerkat_mobkit::gateway_composition::GatewayHttpDrainOutcome),
    /// The stdin guard returned. It parks forever after EOF, so this branch
    /// is not expected to fire; it is named rather than silently absorbed.
    StdinGuard,
    /// SIGINT or SIGTERM.
    Signal(ShutdownSignal),
}

impl RunEnd {
    /// The `reason=` token.
    fn reason(&self) -> &'static str {
        match self {
            Self::HttpServer(_) => "http_server_ended",
            Self::StdinGuard => "stdin_guard_ended",
            Self::Signal(_) => "signal",
        }
    }

    /// The signal that ended the loop, when one did.
    fn signal(&self) -> Option<ShutdownSignal> {
        match self {
            Self::Signal(signal) => Some(*signal),
            Self::HttpServer(_) | Self::StdinGuard => None,
        }
    }
}

async fn run(launch: GatewayLaunchArgs) -> anyhow::Result<()> {
    let GatewayLaunchArgs {
        control_listen,
        http_listen: http_listen_flag,
        allow_remote: allow_remote_flag,
    } = launch;
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut init_line = String::new();
    if reader.read_line(&mut init_line).await? == 0 {
        return Err(anyhow!("stdin closed before init request"));
    }

    let (request_id, params) = match parse_init_request(init_line.trim()) {
        Ok(value) => value,
        Err(error) => {
            return Err(refuse_init(&Value::Null, -32602, error.to_string()));
        }
    };

    let workspace_root = params
        .workspace_root
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let workspace_root = workspace_root.canonicalize().unwrap_or(workspace_root);
    let project_root = params
        .project_root
        .unwrap_or_else(|| workspace_root.clone());
    let project_root = project_root.canonicalize().unwrap_or(project_root);
    let context_root = params
        .context_root
        .map(|path| path.canonicalize().unwrap_or(path))
        .or_else(|| Some(project_root.clone()));
    let runtime_root = params
        .runtime_root
        .unwrap_or_else(|| workspace_root.clone());
    let runtime_root = runtime_root.canonicalize().unwrap_or(runtime_root);
    let store_path = params
        .store_path
        .unwrap_or_else(|| runtime_root.join("state"));
    let store_path = meerkat_mobkit::storage_layout::canonicalize_storage_root(&store_path)
        .map_err(|error| {
            refuse_init(
                &request_id,
                -32602,
                format!(
                    "cannot resolve store_path {}: {error}",
                    store_path.display()
                ),
            )
        })?;
    // The path authority for this boot: the state dir (a `store_path` with a
    // file extension is the explicit session-DB override escape hatch) plus
    // the XDG gateway home (runtime registry + peer key).
    let gateway_home = meerkat_mobkit::storage_layout::default_gateway_home()
        .context("resolve gateway state directory")?;
    let layout = MobKitStorageLayout::standalone_from_store_path(&store_path, gateway_home.clone());
    let persistent_sessions = params.persistent_sessions.unwrap_or(false);
    // Host-level compaction policy: validated once here so a typo or a zero
    // threshold is an init error, not a dead knob or a compaction storm.
    let compaction_policy = params
        .compaction
        .as_ref()
        .map(|value| {
            meerkat_mobkit::parse_compaction_policy(value).map_err(|error| {
                refuse_init(
                    &request_id,
                    -32602,
                    format!("init params: compaction {error}"),
                )
            })
        })
        .transpose()?;
    // Doctrine default: the identity substrate is ON. `identity_first: false`
    // remains as a one-release opt-out for deployments that need the pure
    // mob-plane console back (changelog-flagged).
    let identity_first = params.identity_first.unwrap_or(true);
    let identity_roster_seed = params.identity_roster.clone().unwrap_or_default();
    let role_migration_declarations = params.role_migrations.clone().unwrap_or_default();
    let realm = params.realm.as_deref();
    let isolated = params.isolated.unwrap_or(false);
    let _surface = params.surface.unwrap_or_else(|| "tux".to_string());
    let runtime_profile = params
        .runtime_profile
        .unwrap_or_else(|| "tux-auto".to_string());
    let console_read_only = match params.console_read_only {
        Some(value) => value,
        None => env_bool("MOBKIT_CONSOLE_READ_ONLY")?.unwrap_or(false),
    };
    // HTTP listener: init param > --http-listen > MOBKIT_HTTP_LISTEN_ADDR >
    // loopback on an ephemeral port. Resolved before the resume lookup so the
    // address joins the resume key and the exposure gate runs on every
    // launch, resumed or not.
    let http_listen = resolve_http_listen(
        params.http_listen.as_deref(),
        http_listen_flag,
        std::env::var("MOBKIT_HTTP_LISTEN_ADDR").ok().as_deref(),
    )
    .map_err(|error| {
        refuse_init(
            &request_id,
            -32602,
            format!("init params: http_listen: {error}"),
        )
    })?;
    let allow_remote = match params.allow_remote {
        Some(value) => value,
        None => allow_remote_flag || env_bool("MOBKIT_HTTP_ALLOW_REMOTE")?.unwrap_or(false),
    };
    let declared_public_base_url = params
        .http_public_base_url
        .as_deref()
        .map(meerkat_mobkit::gateway_composition::parse_http_public_base_url)
        .transpose()
        .map_err(|error| {
            refuse_init(
                &request_id,
                -32602,
                format!("init params: http_public_base_url: {error}"),
            )
        })?;
    // Exposure gate, before the registry lookup and before any bootstrap. The
    // console this binary serves is open (`runtime_decision_state` hardcodes
    // `require_app_auth: false`), so only `allow_remote` can open it; the gate
    // still reads the real decision state so the rule has one owner. The UI
    // config plays no part in the gate and is loaded later, as before.
    meerkat_mobkit::gateway_composition::validate_http_bind_policy(
        meerkat_mobkit::gateway_composition::GatewaySurface::MobkitGateway,
        http_listen,
        meerkat_mobkit::gateway_composition::HttpBindPolicy::for_gateway(
            allow_remote,
            &runtime_decision_state(ConsoleUiConfig::default(), console_read_only),
        ),
    )
    .map_err(|error| refuse_init(&request_id, -32602, format!("init params: {error}")))?;

    let mut paths = conventional_paths(&workspace_root);
    // An explicit init param wins over the workspace convention. The
    // effective path rides `paths` so the resume fingerprint and the loader
    // below read the same file.
    if let Some(explicit) = params.meerkat_config_path.clone() {
        paths.meerkat_config_toml = Some(explicit);
    }
    // Adoption is never invisible: the convention picks up a file the operator
    // may not have written for this gateway, and its content joins the resume
    // key, so the log names the file and which door produced it.
    if let Some(path) = paths.meerkat_config_toml.as_deref() {
        let source = if params.meerkat_config_path.is_some() {
            "meerkat_config_path init param"
        } else {
            "<workspace>/.rkat/config.toml convention"
        };
        tracing::info!(
            path = %path.display(),
            source,
            "meerkat host config adopted; every agent this gateway builds starts from it"
        );
    }
    // Loaded before the registry lookup so a bad file refuses init instead of
    // resuming or creating a runtime built from `Config::default()`.
    let host_config = paths
        .meerkat_config_toml
        .as_deref()
        .map(|path| {
            meerkat_mobkit::gateway_composition::load_gateway_host_config(path).map_err(|error| {
                refuse_init(
                    &request_id,
                    -32602,
                    format!("init params: meerkat_config_path: {error}"),
                )
            })
        })
        .transpose()?;
    let key = config_fingerprint(
        &workspace_root,
        realm,
        isolated,
        &runtime_profile,
        persistent_sessions,
        console_read_only,
        &runtime_root,
        &store_path,
        &project_root,
        context_root.as_deref(),
        compaction_policy.as_ref(),
        control_listen.as_ref(),
        http_listen,
        declared_public_base_url.as_deref(),
        &paths,
    )?;
    // A launch requesting authenticated voice must not resume an older open
    // console just because its workspace/listener settings happen to match.
    let key = if params.auth_config.is_some() || params.console_voice.is_some() {
        let auth_shape = serde_json::to_vec(&(
            &key,
            params.auth_config.as_ref(),
            params.console_voice.as_ref(),
        ))?;
        format!("{:x}", Sha256::digest(auth_shape))
    } else {
        key
    };
    let registry_file = layout
        .registry_file()
        .ok_or_else(|| anyhow!("gateway storage layout carries no gateway home"))?;
    let mut registry = load_registry(&registry_file);

    let mut live_entries = Vec::new();
    let mut resumed_entry = None;
    for entry in registry.entries.drain(..) {
        if url_is_alive(&entry.http_base_url).await {
            if entry.key == key {
                resumed_entry = Some(entry.clone());
            }
            live_entries.push(entry);
        }
    }
    registry.entries = live_entries;
    save_registry(&registry_file, &registry)?;

    if let Some(entry) = resumed_entry {
        print_json_line(&init_response(
            request_id,
            &entry.runtime_id,
            &entry.http_base_url,
            "resumed",
            entry.control_listen_address.as_deref(),
            entry.http_public_base_url.as_deref(),
        ));
        return Ok(());
    }

    // Bind the HTTP listener before any bootstrap work and right after the
    // resume lookup: a resumed launch reports the live runtime's listener and
    // must not contend for its port, while a fresh launch on a fixed
    // `http_listen` may find the port taken (EADDRINUSE). Failing here costs
    // the init reply and nothing else: no runtime build and no schedule
    // executor lease left held for its full duration by a process exit. The
    // listener is served once the runtime is up (`http_binding.serve`).
    let http_binding = meerkat_mobkit::gateway_composition::GatewayHttpBinding::bind(http_listen)
        .await
        .map_err(|error| {
            refuse_init(
                &request_id,
                -32603,
                format!("failed to bind gateway listener {http_listen}: {error}"),
            )
        })?
        .with_advertised_base_url(declared_public_base_url);
    let http_base_url = http_binding.http_base_url();
    let http_public_base_url = http_binding.advertised_base_url().map(str::to_string);

    std::env::set_current_dir(&workspace_root).ok();
    let (definition, used_workspace_config) = load_definition(&workspace_root, &key, &paths)
        .map_err(|error| refuse_init(&request_id, -32602, error.to_string()))?;
    let console_ui = match &paths.console_toml {
        Some(path) => load_console_ui_config_from_path_for_realm(path, realm)
            .with_context(|| format!("failed to load {}", path.display()))?,
        None => ConsoleUiConfig::default(),
    };
    for origin in [
        Some(http_base_url.as_str()),
        http_public_base_url.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        console_ui
            .validate_mcp_apps_sandbox(Some(origin))
            .map_err(|error| refuse_init(&request_id, -32602, error.to_string()))?;
    }
    let runtime_id = definition.id.to_string();
    let image_generation = mob_definition_may_use_image_generation(&definition);

    let session_bootstrap = if persistent_sessions {
        let (
            service,
            adapter,
            binary_blob_store,
            schedule_host_inputs,
            workgraph,
            resolved_storage,
            session_write_epochs,
            committed_boundary_recoverer,
            runtime_store,
            voice_inputs,
            runtime_delivery,
        ) = build_persistent_session_service(
            &layout,
            runtime_root.clone(),
            project_root.clone(),
            context_root.clone(),
            image_generation,
            console_ui.mcp_apps_sandbox_url.is_some(),
            &runtime_id,
            host_config.as_ref(),
            compaction_policy.as_ref(),
        )?;
        // Pair the schedule wiring with a clone of the runtime adapter so the
        // firing host can be spawned once the runtime has booted (below).
        let schedule_host_inputs =
            schedule_host_inputs.map(|(sched, registry, svc, path, binding)| {
                (sched, registry, svc, path, binding, adapter.clone())
            });
        let workgraph_service = workgraph.as_ref().map(|(service, _, _)| service.clone());
        // The explicit runtime adapter must share the session service's runtime
        // persistence authority or meerkat 0.7 fails the bootstrap closed.
        // `with_session_runtime_adapter` wires the SAME adapter into the session
        // service (mirrors rpc_gateway.rs). This branch already shared via the
        // runtime_store handed to PersistentSessionService, so this is defensive;
        // the default ephemeral branch below is the one that was actually broken.
        // Persistent sessions require persistent mob state. An in-memory mob
        // storage here drops every adopted identity declaration on restart
        // while the session store faithfully survives, which presents as a
        // healthy boot with the right member count and an empty roster of
        // adoptions - the failure announces nothing.
        let (mob_storage, mob_storage_provenance) =
            meerkat_mobkit::mob_composition_manifest::persistent_mob_storage(
                layout.event_log_db(),
            )?;
        let mut spec = MobBootstrapSpec::new(definition, mob_storage, service)
            .with_mob_storage_provenance(mob_storage_provenance)
            .with_session_write_epochs(&session_write_epochs)
            // The SAME facade handed to MeerkatMachine::persistent above.
            // The durable-authority convergence memo is per-facade, so a
            // different instance here would warm nothing.
            .with_runtime_authority_prewarm(&runtime_store)
            // Resume-seam reads must carry the runtime store's archived
            // terminal (at 0.8.11 archive stamps the catalog/lifecycle row,
            // never the session body).
            .with_runtime_archived_terminal_authority(runtime_store)
            .with_session_runtime_adapter(adapter.clone())
            .context("failed to acquire the gateway session runtime owner")?
            .with_workgraph_service(workgraph_service.clone())
            // This is the persistent session builder's shared agent-tool slot.
            // Identity activation needs it even when console voice is absent.
            .with_runtime_delivery(runtime_delivery)
            .with_agent_mob_tools(Arc::clone(&voice_inputs.4))
            .context("failed to install gateway agent mob tools")?;
        spec.committed_boundary_recoverer = Some(committed_boundary_recoverer);
        if let Some((_, admission_slot, state_dir)) = &workgraph {
            // Durable (cross-process shareable) store: register the tool-plane
            // admission slot and the sidecar lock beside the store.
            spec = spec
                .with_workgraph_admission_slot(admission_slot.clone())
                .with_workgraph_admission_sidecar(state_dir);
        }
        spec.runtime_adapter = Some(adapter);
        spec.binary_blob_store = Some(binary_blob_store);
        let mut resolved_storage = resolved_storage;
        // The M4 census covered nine slots and not this one, which is why an
        // in-memory mob storage on a persistent launch was invisible to
        // healthz and the storage doctor.
        resolved_storage.slots.push(
            meerkat_mobkit::storage_health::StorageSlotSummary::persistent(
                "mob",
                "MobStorage(sqlite)",
            ),
        );
        spec.resolved_storage = Some(resolved_storage);
        (
            spec,
            schedule_host_inputs,
            workgraph_service,
            Some(voice_inputs),
        )
    } else {
        // Build the ephemeral path manually to thread project/context roots
        // into AgentFactory (MobBootstrapSpec::ephemeral doesn't accept them).
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
            .context("failed to acquire the gateway runtime machine")?,
        );
        let session_store: Arc<dyn meerkat::SessionStore> = Arc::new(meerkat::MemoryStore::new());
        let mut factory = AgentFactory::new(&runtime_root)
            .session_store(session_store.clone())
            .runtime_root(runtime_root.clone())
            .project_root(project_root.clone())
            .builtins(true)
            .shell(true)
            .mob(true)
            .comms(true)
            .memory(true)
            .mcp_apps(console_ui.mcp_apps_sandbox_url.is_some());
        if image_generation {
            factory = factory.with_image_generation_machine(adapter.clone());
        }
        if let Some(ref ctx) = context_root {
            factory = factory.context_root(ctx.clone());
        }
        // Same producer as the persistent arm: host config first, the
        // compaction declaration on top.
        let config = gateway_agent_config(host_config.as_ref(), compaction_policy.as_ref())?;
        let mut builder = FactoryAgentBuilder::new(factory, config);
        builder.default_session_store = Some(Arc::new(meerkat_store::StoreAdapter::new(
            session_store.clone(),
        )));
        builder.default_blob_store = Some(blob_store.clone());
        let jobs: Arc<dyn meerkat::DetachedJobStore> =
            Arc::new(meerkat::MemoryDetachedJobStore::new());
        let runtime_delivery = meerkat_mobkit::mob_handle_runtime::MobRuntimeDelivery::new(
            Arc::clone(&runtime_store),
            Arc::clone(&jobs),
        );
        builder.default_detached_job_store = Some(Arc::clone(&jobs));
        builder.default_shell_job_delivery_projector =
            Some(meerkat::JobOutboxProjector::new_for_realm(
                jobs,
                runtime_delivery.inbox(),
                meerkat_mobkit::storage_provider::MEERKAT_LEVEL_REALM_ID,
            ));
        let agent_mob_tools_slot = Arc::clone(&builder.default_mob_tools);

        // The default TUX launch is ephemeral: a memory-backed workgraph
        // keeps the feature available (tools stay profile-gated). Memory
        // store = single process, so no admission sidecar.
        let (ephemeral_workgraph, workgraph_admission_slot) =
            meerkat_mobkit::workgraph_wiring::attach_workgraph_tools_ephemeral(
                &builder,
                &runtime_id,
            );
        let workgraph_service = Some(ephemeral_workgraph);
        let session_service = Arc::new(PersistentSessionService::new(
            builder,
            64,
            session_store,
            Arc::clone(&runtime_store),
            blob_store,
        ));
        session_service
            .acquire_canonical_runtime_adapter(Some(Arc::clone(&adapter)))
            .context("failed to acquire the process-local gateway session runtime owner")?;
        let mut spec = MobBootstrapSpec::new(definition, MobStorage::in_memory(), session_service)
            .with_session_write_epochs(&session_write_epochs)
            // The SAME facade handed to MeerkatMachine::persistent above.
            // The durable-authority convergence memo is per-facade, so a
            // different instance here would warm nothing.
            .with_runtime_authority_prewarm(&runtime_store)
            .with_session_runtime_adapter(adapter.clone())
            .context("failed to acquire the gateway session runtime owner")?
            .with_workgraph_service(workgraph_service.clone())
            .with_workgraph_admission_slot(workgraph_admission_slot)
            .with_runtime_delivery(runtime_delivery)
            .with_agent_mob_tools(agent_mob_tools_slot)
            .context("failed to install ephemeral gateway agent mob tools")?;
        spec.runtime_adapter = Some(adapter);
        spec.binary_blob_store = Some(binary_blob_store);
        // The service's session and blob stores are explicitly process-local;
        // no disk-backed session store is advertised by this launch.
        let mut slots = vec![
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "sessions",
                "MemoryStore",
                "declared by the default ephemeral launch (persistent_sessions = false)",
            ),
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "runtime",
                "InMemoryRuntimeStore",
                "declared by the default ephemeral launch",
            ),
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "jobs",
                "MemoryDetachedJobStore",
                "declared by the default ephemeral launch",
            ),
            meerkat_mobkit::storage_health::blob_slot_summary(
                meerkat_mobkit::storage_health::BlobDurability::DeclaredEphemeral,
            ),
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "console",
                "InMemoryConsoleLogStore",
                "declared default of this surface",
            ),
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "metadata",
                "InMemoryMetadataStore",
                "declared default of this surface",
            ),
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "workgraph",
                "MemoryWorkGraphStore",
                "declared by the default ephemeral launch",
            ),
        ];
        slots.push(
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "jobs",
                "MemoryDetachedJobStore",
                "declared by the ephemeral launch mode",
            ),
        );
        slots.extend(meerkat_mobkit::storage_health::scratch_ring_buffer_slots());
        // Declared, not silent: this launch keeps mob state in memory by
        // design, and the census now says so rather than omitting the slot.
        slots.push(
            meerkat_mobkit::storage_health::StorageSlotSummary::declared_ephemeral(
                "mob",
                "MobStorage(in-memory)",
                "declared by the ephemeral launch mode: mob events and adopted identity \
                 declarations do not survive gateway restart",
            ),
        );
        spec.resolved_storage = Some(
            meerkat_mobkit::storage_health::ResolvedStorageSummary::new(
                meerkat_mobkit::storage_health::BlobDurability::DeclaredEphemeral,
                None,
            )
            .with_slots(slots),
        );
        // This process-local launch keeps the schedule firing host and voice
        // host disabled; their gateway configuration is persistent-only.
        (spec, None, workgraph_service, None)
    };
    let (session_spec, schedule_host_inputs, workgraph_service, voice_inputs) = session_bootstrap;
    let mob_spec = session_spec.with_options(MobBootstrapOptions {
        allow_ephemeral_sessions: !persistent_sessions,
        notify_orchestrator_on_resume: true,
        default_llm_client: None,
    });

    let bootstrap_plan =
        meerkat_mobkit::gateway_composition::GatewayRuntimeBootstrapPlan::console_http(
            mob_spec,
            meerkat_mobkit::MobKitConfig {
                modules: Vec::new(),
                discovery: meerkat_mobkit::DiscoverySpec {
                    namespace: format!("tux.{}", short_hash(&key)),
                    modules: Vec::new(),
                },
                pre_spawn: Vec::new(),
            },
            Duration::from_secs(30),
        );
    let mut composition = meerkat_mobkit::gateway_composition::GatewayComposition::prepare(
        meerkat_mobkit::gateway_composition::GatewayCompatibilityProfile::ConsoleHttp,
        bootstrap_plan,
    )
    .bootstrap()
    .await
    .context("failed to bootstrap local runtime")?;
    let runtime = composition.runtime_mut();

    // Load contacts.toml if present. This enables mobkit/cross_mob/directory
    // (lookup of known mob addresses) without requiring peer mob handles.
    // High-level wire/unwire/send still need peer handles and are gated
    // separately by has_peer_mob_handles().
    let mut control_grants = ControlGrantTable::new();
    if let Some(ref contacts_path) = paths.contacts_toml {
        let contacts_text = fs::read_to_string(contacts_path)
            .with_context(|| format!("failed to read {}", contacts_path.display()))?;
        let directory = ContactDirectory::from_toml(&contacts_text)
            .with_context(|| format!("failed to parse {}", contacts_path.display()))?;
        if let Some(configured) =
            ControlGrantTable::from_toml(&contacts_text).with_context(|| {
                format!(
                    "failed to parse control grants in {}",
                    contacts_path.display()
                )
            })?
        {
            control_grants = configured;
        }
        runtime.set_contact_directory(directory);
    }

    // Opt-in ABAC: a `config/access.toml` wires the access controller into
    // every console and SSE surface. Console admin edits persist back to
    // the same file, so the deployment's access policy survives restarts.
    if let Some(ref access_path) = paths.access_toml {
        let controller = meerkat_mobkit::AccessController::load_or_default(access_path)
            .map_err(|error| anyhow!("failed to load {}: {error}", access_path.display()))?;
        runtime.set_access_controller(controller);
    }

    // Load (or mint and persist) the gateway's Ed25519 signing keypair.
    // Stored under the same state directory the registry lives in, which
    // already survives across runs. Cross-process peers fetch the
    // resulting pubkey via `mobkit/peer_pubkey`; inproc-only deployments
    // never use it but it's cheap to keep one ready.
    let peer_keys = GatewayPeerKeys::load_or_create(&gateway_home).with_context(|| {
        format!(
            "failed to load or mint gateway peer key under {}",
            gateway_home.display()
        )
    })?;
    runtime.set_gateway_peer_keys(peer_keys);

    // Identity-first mode (meerkat-studio ask K0): give this gateway the
    // durable-identity substrate — continuity records, lease-fenced
    // embodiment, resume-first restore with the Broken-identity repair task,
    // and the tolerant identity lifecycle paths. Default providers are
    // constructed from the existing store paths; the roster is seeded from
    // init params and extended at runtime by `mobkit/ensure_member`.
    let _identity_roster_provider: Option<
        Arc<meerkat_mobkit::identity_first::MutableRosterProvider>,
    > =
        if identity_first {
            use meerkat_mobkit::identity_first::{
                AgentRuntimeServices, DurabilityPolicy, IdentityFirstRuntimeContext,
                IdentityRuntime, IdentityRuntimeConfig, MobSessionBridge, MutableRosterProvider,
            };

            let store_dir = layout.state_dir();
            fs::create_dir_all(store_dir)
                .with_context(|| format!("failed to create {}", store_dir.display()))?;
            let continuity_db = layout.continuity_db().map_err(|e| anyhow!("{e}"))?.path;
            let substrate = meerkat_mobkit::gateway_wiring::open_identity_substrate(&continuity_db)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let mob_handle = runtime.mob_handle();
            let mut bridge =
                if let Some(session_service) = runtime.mob_runtime().session_service().cloned() {
                    MobSessionBridge::with_session_service(mob_handle.clone(), session_service)
                } else {
                    MobSessionBridge::new(mob_handle.clone())
                };
            // Heal seam (2026-07-29 incident): the continuity repair supervisor
            // asks this recoverer to commit the durable head before declaring an
            // identity healed; without it, heal is a cosmetic entry reset that
            // the next materialization re-Breaks.
            if let Some(recoverer) = runtime.mob_runtime().committed_boundary_recoverer() {
                bridge = bridge.with_committed_boundary_recoverer(recoverer);
            }
            if let Some((identity, first, second)) =
                meerkat_mobkit::identity_first::conflicting_role_migration_declaration(
                    &role_migration_declarations,
                )
            {
                anyhow::bail!(
                    "role_migrations declares '{identity}' twice with different predecessor roles \
                 ('{first}' and '{second}'); migration authority cannot be resolved by order"
                );
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
            bridge = bridge.with_role_migration_declarations(role_migration_declarations);
            let bridge: Arc<dyn meerkat_mobkit::identity_first::SessionBridge> = Arc::new(bridge);

            let irt = IdentityRuntime::new(IdentityRuntimeConfig {
                continuity_store: substrate.continuity_store,
                lease_provider: substrate.lease_provider,
                runtime_instance_id: format!("mobkit-gateway-{}", std::process::id()),
                has_runtime_store: persistent_sessions,
                durability_policy: DurabilityPolicy::SyncWriteThrough,
                bridge: Some(bridge),
                default_timeout: None,
            })
            .with_runtime_services(AgentRuntimeServices::new(mob_handle.clone()));
            // A member parked with a session repair hold names the exact
            // `rkat session repair-wholeblob` commands for the runtime store this
            // launch opened; the ephemeral launch has nothing to repair.
            irt.set_session_repair_scope(runtime.resolved_storage().as_ref().and_then(
                meerkat_mobkit::identity_first::SessionRepairScope::from_resolved_storage,
            ));

            let roster = Arc::new(MutableRosterProvider::new(identity_roster_seed));
            let mob_definition = mob_handle.definition().clone();
            let irt = Arc::new(irt);
            // Register continuity owners before a stopped mob resumes, then apply
            // the roster. The installer owns cleanup if either stage fails.
            runtime.set_console_identity_roster(roster.clone());
            let context = Arc::new(IdentityFirstRuntimeContext::new(
                irt,
                roster.clone(),
                None,
                None,
                Some(mob_definition),
            ));
            runtime
                .install_and_bootstrap_identity_first_context(context, &roster.snapshot())
                .await
                .context("identity-first bootstrap failed")?;
            tracing::info!(
                roster = roster.snapshot().len(),
                continuity_db = %continuity_db.display(),
                "identity-first gateway mode active"
            );
            Some(roster)
        } else {
            runtime
                .activate_without_identity_context()
                .await
                .context("failed to activate classic local runtime")?;
            None
        };

    if !used_workspace_config {
        let mut labels = BTreeMap::new();
        labels.insert("surface".to_string(), "tux".to_string());
        labels.insert("ui".to_string(), "meerkat-tux".to_string());
        if let Some(realm) = realm {
            labels.insert("realm".to_string(), realm.to_string());
        }
        runtime
            .mob_handle()
            .ensure_member(
                SpawnMemberSpec::new(ProfileName::from("alpha"), AgentIdentity::from("alpha"))
                    .with_labels(labels),
            )
            .await
            .map_err(|error| anyhow!("failed to spawn fallback alpha meerkat: {error}"))?;
    }

    // Bind the cross-mob control listener after identity-first attachment so
    // its startup log reflects the final authority posture (the handler
    // re-reads the identity slot per request either way).
    if let Some(addr) = control_listen.as_ref() {
        let authorizer = Arc::new(ControlAuthorizer::with_grants_for_audience(
            control_grants,
            runtime.mob_id(),
        ));
        let advertised = runtime
            .start_control_listener_with_authorizer(addr, authorizer)
            .await
            .map_err(|error| anyhow!("--control-listen {addr}: {error}"))?;
        tracing::info!(%advertised, "cross-mob control listener bound");
    }

    // Start schedule delivery only after identity-first attachment. The host
    // owns a snapshot of the authority Arc, so starting it before this point
    // would permanently reject every generated `rt:*` target.
    let (schedule_host, _schedule_watchdog) = if let Some((
        schedule_service,
        mob_target_registry,
        service,
        schedule_store_path,
        schedule_firing_host_binding,
        adapter,
    )) = schedule_host_inputs
    {
        let mob_state = meerkat_mobkit::gateway_composition::adopt_schedule_mob_targets(
            runtime,
            &schedule_service,
            &mob_target_registry,
        )
        .await;
        // Shared with rpc_gateway: the watchdog liveness contract log, the
        // host spawn, the firing-intent gate bind and the boot probe were
        // byte-identical in both binaries, every log string included. This
        // gateway declares no host runnables (it has no memory steward and no
        // SDK callback plane), which is now an explicit `None` FIELD rather
        // than a hard-coded argument nobody could see was a divergence.
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
                    runnable_host: None,
                    workgraph_service: workgraph_service.clone(),
                    owner_id: runtime_id.clone(),
                },
            )
            .await;
        (schedule_host, Some(watchdog))
    } else {
        (None, None)
    };

    let composition = composition.activate();
    let runtime = composition.runtime();
    // The HTTP listener was bound at init, right behind the exposure gate and
    // the resume lookup (`http_binding` above); only serving it waited.

    let control_listen_address = runtime.control_listener_advertised_address();
    registry.entries.retain(|entry| entry.key != key);
    registry.entries.push(RuntimeRegistryEntry {
        key: key.clone(),
        runtime_id: runtime_id.clone(),
        http_base_url: http_base_url.clone(),
        pid: std::process::id(),
        updated_at_ms: current_time_ms(),
        control_listen_address: control_listen_address.clone(),
        http_public_base_url: http_public_base_url.clone(),
    });
    save_registry(&registry_file, &registry)?;

    let mut decisions = runtime_decision_state(console_ui, console_read_only);
    if let Some(auth) = params.auth_config.as_ref() {
        let configured = meerkat_mobkit::console_auth_config::parse_console_auth_config(auth)
            .map_err(|error| anyhow!("{error}"))?;
        decisions.auth = configured.auth;
        decisions.trusted_oidc = configured.trusted_oidc;
        decisions.console.require_app_auth = true;
    }
    #[cfg(feature = "openai-live")]
    let controller = if let Some(registration) = params.console_voice.as_ref() {
        let (service, machine, factory, config, _) =
            voice_inputs.ok_or_else(|| anyhow!("console_voice requires persistent sessions"))?;
        meerkat_mobkit::console_voice::ConsoleVoiceController::with_live_host(
            runtime,
            service,
            machine,
            factory,
            config,
            meerkat_mobkit::public_live_config::PublicLiveRegistration::parse(registration)
                .map_err(|error| anyhow!("console_voice: {error}"))?,
        )
        .map_err(|error| anyhow!("console voice composition failed: {error}"))?
    } else {
        meerkat_mobkit::console_voice::ConsoleVoiceController::default()
    };
    #[cfg(not(feature = "openai-live"))]
    let controller = {
        let _ = voice_inputs;
        meerkat_mobkit::console_voice::ConsoleVoiceController::default()
    };
    meerkat_mobkit::gateway_composition::warn_on_non_loopback_bind(
        meerkat_mobkit::gateway_composition::GatewaySurface::MobkitGateway,
        http_binding.local_addr(),
        &decisions,
    );
    let app = runtime
        .build_reference_app_router(decisions)
        .layer(axum::Extension(controller.clone()));
    print_json_line(&init_response(
        request_id,
        &runtime_id,
        &http_base_url,
        "created",
        control_listen_address.as_deref(),
        http_public_base_url.as_deref(),
    ));
    let mut http_server = http_binding.serve(app);

    // `mobkit_gateway` serves the console/admin API over HTTP. It is NOT the
    // SDK's stdin JSON-RPC gateway — that is the separate `rpc_gateway` binary.
    // The SDK's PersistentTransport drives a gateway by sending JSON-RPC lines
    // (init, then reconcile_identity, send, ...) over stdin; we already consumed
    // the single init line above. If more JSON-RPC arrives on stdin, this binary
    // was misconfigured as the SDK gateway (a common mix-up, since the SDK env
    // var is named MOBKIT_RPC_GATEWAY_BIN and the release ships this binary).
    // Answer each such line with a clear JSON-RPC error instead of silently
    // ignoring it, so the SDK surfaces an actionable failure immediately rather
    // than blocking forever waiting for a response that never comes.
    let stdin_guard = async move {
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => {
                    // Unlike rpc_gateway, this binary outlives its stdin by
                    // design (see below); say so, or a later exit gets
                    // misattributed to the EOF.
                    tracing::info!(
                        "stdin closed by the launching process; mobkit_gateway keeps serving HTTP \
                         until SIGINT or SIGTERM"
                    );
                    break;
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "stdin read failed; mobkit_gateway keeps serving HTTP until SIGINT or SIGTERM"
                    );
                    break;
                }
                Ok(_) => {}
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let id = serde_json::from_str::<Value>(trimmed)
                .ok()
                .and_then(|value| value.get("id").cloned())
                .unwrap_or(Value::Null);
            print_json_line(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32601,
                    "message": "mobkit_gateway serves the console/admin API over HTTP and does not handle SDK stdin JSON-RPC after init. Point your SDK gateway path (MOBKIT_RPC_GATEWAY_BIN) at the 'rpc_gateway' binary instead."
                }
            }));
        }
        // stdin ended: keep serving HTTP until ctrl-c rather than tearing down
        // the console out from under any live HTTP clients.
        std::future::pending::<()>().await;
    };

    let run_end = tokio::select! {
        // SIGINT *and* SIGTERM: a container stop sends SIGTERM, and waiting
        // on ctrl_c alone meant the graceful path never ran on an ordinary
        // deploy. See `meerkat_mobkit::shutdown_signal` for why that became
        // load-bearing at 0.8.22 (unreleased schedule executor lease).
        outcome = http_server.wait() => RunEnd::HttpServer(outcome),
        () = stdin_guard => RunEnd::StdinGuard,
        signal = meerkat_mobkit::shutdown_signal::shutdown_signal() => RunEnd::Signal(signal),
    };
    // Name the branch that ended run() BEFORE the shutdown sequence, so a
    // wedge in it still leaves the reason in the log. `reason=` is one of
    // `RunEnd`'s tokens; `signal=` is present only when the reason is
    // `signal`.
    let exit_reason = run_end.reason();
    let exit_signal = run_end.signal();
    tracing::info!(
        reason = %exit_reason,
        signal = exit_signal.map(tracing::field::display),
        "mobkit_gateway run loop ended; running graceful shutdown"
    );
    if let RunEnd::HttpServer(outcome) = run_end {
        let failure = match outcome {
            meerkat_mobkit::gateway_composition::GatewayHttpDrainOutcome::Completed(Ok(())) => None,
            meerkat_mobkit::gateway_composition::GatewayHttpDrainOutcome::Completed(Err(error)) => {
                Some(anyhow::Error::new(error).context("gateway HTTP server failed"))
            }
            meerkat_mobkit::gateway_composition::GatewayHttpDrainOutcome::TimedOut => {
                Some(anyhow!("gateway HTTP server wait timed out"))
            }
            meerkat_mobkit::gateway_composition::GatewayHttpDrainOutcome::JoinFailed(error) => {
                Some(anyhow!("gateway HTTP server task failed: {error}"))
            }
        };
        if let Some(error) = failure {
            // main() reports this as a JSON-RPC error on stdout and exits 1;
            // the log stream is stderr, so name it there too.
            tracing::error!(
                error = %format!("{error:#}"),
                "mobkit_gateway exiting without graceful shutdown: the HTTP server task ended \
                 abnormally"
            );
            return Err(error);
        }
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
            || async {
                if let Err(error) = controller.shutdown().await {
                    tracing::warn!(%error, "console voice shutdown did not attest complete cleanup");
                }
            },
            || async {
                let mut registry = load_registry(&registry_file);
                registry.entries.retain(|entry| entry.key != key);
                save_registry(&registry_file, &registry)
            },
        )
        .await;
    if let Err(error) = shutdown.cleanup {
        tracing::error!(
            reason = %exit_reason,
            error = %error,
            "mobkit_gateway shutdown finished with a registry cleanup failure; exiting"
        );
        return Err(error);
    }
    // The closing bookend to the "run loop ended" line: if this one is
    // missing from a log that has the first, the shutdown sequence wedged.
    tracing::info!(
        reason = %exit_reason,
        signal = exit_signal.map(tracing::field::display),
        "mobkit_gateway shutdown complete; exiting"
    );
    if exit_signal.is_some() {
        // Tokio's stdin adapter performs a blocking read that cannot be
        // cancelled by dropping its future; on the signal path stdin may still
        // be open (a terminal, or a parent holding the pipe), so the runtime
        // destructor would wait forever for that helper thread and the process
        // would log "exiting" and then never exit. All MobKit/runtime/registry
        // cleanup is complete above; exit explicitly, as rpc_gateway does.
        std::process::exit(0);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The default tracing filter (RUST_LOG unset) must surface this crate's
    /// own INFO lines — storage-verb/migration progress was invisible at the
    /// old blanket "warn" default — while keeping dependencies at WARN.
    #[test]
    fn default_tracing_filter_surfaces_own_info_keeps_deps_at_warn() -> anyhow::Result<()> {
        use tracing_subscriber::layer::SubscriberExt;
        let filter = tracing_subscriber::EnvFilter::try_new(
            meerkat_mobkit::gateway_composition::default_tracing_filter(GATEWAY_TRACING_TARGET),
        )?;
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
                tracing::enabled!(target: "mobkit_gateway", tracing::Level::INFO),
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
        Ok(())
    }

    #[test]
    fn console_voice_init_requires_persistent_authenticated_host()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut params = json!({
            "persistent_sessions":true,
            "auth_config":{"provider":"jwt","shared_secret":"test-console-signing-key",
                "email_allowlist":["voice@example.com"]},
            "console_voice":{
                "principal":"voice@example.com","realm":"voice",
                "auth_binding":{"realm":"voice","binding":"openai"},"voice":"marin"
            }
        });
        let parse = |params: &Value| {
            parse_init_request(
                &json!({
                    "jsonrpc":"2.0","id":1,"method":"mobkit/init","params":params
                })
                .to_string(),
            )
        };
        assert_eq!(parse(&params).is_ok(), cfg!(feature = "openai-live"));
        params["persistent_sessions"] = json!(false);
        assert!(parse(&params).is_err());
        params["persistent_sessions"] = json!(true);
        params
            .as_object_mut()
            .ok_or("params must be an object")?
            .remove("auth_config");
        assert!(parse(&params).is_err());
        Ok(())
    }

    /// The other binary implements `application_tool_policies`; this one must
    /// not pretend to. `InitParams` has no `deny_unknown_fields`, so before
    /// this refusal the key was dropped in silence.
    /// `runtime_options` is an rpc_gateway protocol that this binary never
    /// parsed. #376 routed `declare_spec_update` through it, which turned the
    /// silent drop from "an option did nothing" into "a declaration the operator
    /// relied on did nothing". Refuse the object, naming the binary that reads it.
    #[test]
    fn init_refuses_runtime_options_rather_than_dropping_them() {
        let line = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "mobkit/init",
            "params": {
                "runtime_options": { "declare_spec_update": { "expected_revision": 1 } }
            }
        })
        .to_string();
        let error = parse_init_request(&line)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(
            !error.is_empty(),
            "a supplied runtime_options object must not be silently dropped"
        );
        assert!(
            error.contains("rpc_gateway") && error.contains("declare_spec_update"),
            "the refusal must name where the options work and the option that matters: {error}"
        );
    }

    #[test]
    fn init_refuses_application_tool_policies_rather_than_dropping_them() {
        let line = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "mobkit/init",
            "params": {
                "application_tool_policies": ["{}"]
            }
        })
        .to_string();

        let error = parse_init_request(&line)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(
            !error.is_empty(),
            "a supplied application tool policy must not be silently dropped"
        );
        // The refusal has to name the binary that DOES implement it, or the
        // host is told "no" with nowhere to go.
        assert!(
            error.contains("rpc_gateway"),
            "the refusal must name where the parameter works: {error}"
        );
    }

    /// The negative half: absence is still absence. A refusal that fired on
    /// every boot would be indistinguishable from one that works.
    #[test]
    fn an_absent_application_tool_policies_key_still_boots() -> anyhow::Result<()> {
        let line = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "mobkit/init",
            "params": { "console_read_only": true }
        })
        .to_string();
        let (_id, params) = parse_init_request(&line)?;
        assert!(params.application_tool_policies.is_none());
        Ok(())
    }

    #[test]
    fn init_params_parse_http_listen_allow_remote_and_public_base_url() -> anyhow::Result<()> {
        let line = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "mobkit/init",
            "params": {
                "http_listen": "0.0.0.0:8080",
                "allow_remote": true,
                "http_public_base_url": "https://mob.example.com"
            }
        })
        .to_string();

        let (_id, params) = parse_init_request(&line)?;

        assert_eq!(params.http_listen.as_deref(), Some("0.0.0.0:8080"));
        assert_eq!(params.allow_remote, Some(true));
        assert_eq!(
            params.http_public_base_url.as_deref(),
            Some("https://mob.example.com")
        );
        Ok(())
    }

    /// Precedence: init param, then `--http-listen`, then the environment,
    /// then loopback on an ephemeral port; an empty environment value is
    /// unset, a malformed one is a typed refusal naming the variable.
    #[test]
    fn resolve_http_listen_prefers_init_param_then_flag_then_env() -> Result<(), String> {
        let parse = |value: &str| -> Result<SocketAddr, String> {
            value
                .parse::<SocketAddr>()
                .map_err(|error| error.to_string())
        };
        let flag = parse("192.0.2.10:8080")?;
        assert_eq!(
            resolve_http_listen(Some("0.0.0.0:9090"), Some(flag), Some("[::]:7070"))?,
            parse("0.0.0.0:9090")?
        );
        assert_eq!(
            resolve_http_listen(None, Some(flag), Some("[::]:7070"))?,
            flag
        );
        assert_eq!(
            resolve_http_listen(None, None, Some("[::]:7070"))?,
            parse("[::]:7070")?
        );
        assert_eq!(
            resolve_http_listen(None, None, Some("  "))?,
            meerkat_mobkit::gateway_composition::DEFAULT_GATEWAY_HTTP_LISTEN
        );
        assert_eq!(
            resolve_http_listen(None, None, None)?,
            meerkat_mobkit::gateway_composition::DEFAULT_GATEWAY_HTTP_LISTEN
        );
        let error = resolve_http_listen(None, None, Some("localhost:1"))
            .err()
            .unwrap_or_default();
        assert!(error.contains("MOBKIT_HTTP_LISTEN_ADDR"), "{error}");
        let error = resolve_http_listen(Some("nope"), None, None)
            .err()
            .unwrap_or_default();
        assert!(error.contains("HOST:PORT"), "{error}");
        Ok(())
    }

    /// This binary serves an OPEN console (no auth ingress), so enforced
    /// console auth can never open the exposure gate here: a non-loopback
    /// bind always needs `allow_remote`, and loopback never does.
    #[test]
    fn non_loopback_http_listen_always_needs_allow_remote_on_the_open_console()
    -> Result<(), std::net::AddrParseError> {
        use meerkat_mobkit::gateway_composition::{
            GatewaySurface, HttpBindPolicy, validate_http_bind_policy,
        };

        let state = runtime_decision_state(ConsoleUiConfig::default(), false);
        let remote: SocketAddr = "0.0.0.0:8080".parse()?;
        let refused = validate_http_bind_policy(
            GatewaySurface::MobkitGateway,
            remote,
            HttpBindPolicy::for_gateway(false, &state),
        );
        let message = refused
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(message.contains("mobkit_gateway"), "{message}");
        assert!(message.contains("MOBKIT_HTTP_ALLOW_REMOTE"), "{message}");
        assert_eq!(
            validate_http_bind_policy(
                GatewaySurface::MobkitGateway,
                remote,
                HttpBindPolicy::for_gateway(true, &state),
            ),
            Ok(())
        );
        assert_eq!(
            validate_http_bind_policy(
                GatewaySurface::MobkitGateway,
                "127.0.0.1:0".parse()?,
                HttpBindPolicy::for_gateway(false, &state),
            ),
            Ok(())
        );
        Ok(())
    }

    /// The init reply carries the advertised base beside the same-host one,
    /// and `null` when none was declared, so an SDK can tell "not declared"
    /// from "declared".
    #[test]
    fn init_response_reports_the_advertised_public_base_url() {
        let with = init_response(
            serde_json::json!(1),
            "rt",
            "http://127.0.0.1:41",
            "created",
            None,
            Some("https://mob.example.com"),
        );
        assert_eq!(
            with["result"]["http_public_base_url"],
            serde_json::json!("https://mob.example.com")
        );
        assert_eq!(
            with["result"]["http_base_url"],
            serde_json::json!("http://127.0.0.1:41")
        );
        let without = init_response(
            serde_json::json!(1),
            "rt",
            "http://127.0.0.1:41",
            "resumed",
            None,
            None,
        );
        assert!(without["result"]["http_public_base_url"].is_null());
    }

    #[cfg(unix)]
    #[test]
    fn config_fingerprint_keeps_absent_store_identity_after_creation() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let workspace = temp.path().join("workspace");
        fs::create_dir(&workspace)?;
        let alias = temp.path().join("workspace-alias");
        std::os::unix::fs::symlink(&workspace, &alias)?;
        let workspace = workspace.canonicalize()?;
        let supplied = alias.join("future/nested/store");
        let paths = conventional_paths(&workspace);
        let fingerprint = |store: &Path| {
            config_fingerprint(
                &workspace,
                None,
                false,
                "tux-auto",
                true,
                false,
                &workspace,
                store,
                &workspace,
                Some(&workspace),
                None,
                None,
                meerkat_mobkit::gateway_composition::DEFAULT_GATEWAY_HTTP_LISTEN,
                None,
                &paths,
            )
        };
        let before = meerkat_mobkit::storage_layout::canonicalize_storage_root(&supplied)?;
        let first_key = fingerprint(&before)?;
        let (first_definition, _) = load_definition(&workspace, &first_key, &paths)?;
        assert!(!supplied.exists());
        fs::create_dir_all(&supplied)?;
        let after = meerkat_mobkit::storage_layout::canonicalize_storage_root(&supplied)?;
        let second_key = fingerprint(&after)?;
        let (second_definition, _) = load_definition(&workspace, &second_key, &paths)?;
        assert_eq!(
            first_key, second_key,
            "directory creation must not change the launch key"
        );
        assert_eq!(
            first_definition, second_definition,
            "the persisted mob definition must stay exact"
        );
        assert_ne!(
            first_key,
            fingerprint(&workspace.join("different-store"))?,
            "a genuinely different storage location must retain a distinct launch key"
        );
        Ok(())
    }

    /// Like `--control-listen`, the HTTP listen address is part of the resume
    /// key: a relaunch asking for a different bind must not resume a live
    /// runtime bound elsewhere.
    #[test]
    fn config_fingerprint_changes_with_http_listen() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let paths = conventional_paths(temp.path());
        let fingerprint = |listen: &str| -> anyhow::Result<String> {
            config_fingerprint(
                temp.path(),
                None,
                false,
                "tux-auto",
                false,
                false,
                temp.path(),
                temp.path(),
                temp.path(),
                None,
                None,
                None,
                listen.parse()?,
                None,
                &paths,
            )
        };
        assert_eq!(fingerprint("127.0.0.1:0")?, fingerprint("127.0.0.1:0")?);
        assert_ne!(fingerprint("127.0.0.1:0")?, fingerprint("0.0.0.0:8080")?);
        Ok(())
    }

    /// The advertised base is reported back from the registry on resume, so a
    /// launch that declares a different one (or drops it) must not resume a
    /// runtime recorded with the previous value: it joins the key beside the
    /// listen address.
    #[test]
    fn config_fingerprint_changes_with_http_public_base_url() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let paths = conventional_paths(temp.path());
        let fingerprint = |base: Option<&str>| -> anyhow::Result<String> {
            config_fingerprint(
                temp.path(),
                None,
                false,
                "tux-auto",
                false,
                false,
                temp.path(),
                temp.path(),
                temp.path(),
                None,
                None,
                None,
                meerkat_mobkit::gateway_composition::DEFAULT_GATEWAY_HTTP_LISTEN,
                base,
                &paths,
            )
        };
        assert_eq!(
            fingerprint(Some("https://mob.example.com"))?,
            fingerprint(Some("https://mob.example.com"))?
        );
        assert_ne!(
            fingerprint(Some("https://mob.example.com"))?,
            fingerprint(Some("https://other.example.com"))?
        );
        assert_ne!(
            fingerprint(Some("https://mob.example.com"))?,
            fingerprint(None)?
        );
        Ok(())
    }

    #[test]
    fn init_params_parse_console_read_only() -> anyhow::Result<()> {
        let line = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "mobkit/init",
            "params": {
                "console_read_only": true
            }
        })
        .to_string();

        let (_id, params) = parse_init_request(&line)?;

        assert_eq!(params.console_read_only, Some(true));
        Ok(())
    }

    #[test]
    fn runtime_decision_state_projects_console_read_only() {
        let state = runtime_decision_state(ConsoleUiConfig::default(), true);

        assert!(state.console.read_only);
    }

    #[test]
    fn runtime_decision_state_is_open_and_keeps_the_tux_session_store_naming() {
        let state = runtime_decision_state(ConsoleUiConfig::default(), false);

        assert!(!state.console.require_app_auth);
        assert_eq!(state.bigquery.dataset, "tux_local");
        assert_eq!(state.bigquery.table, "runtime_events");
        assert!(state.modules.is_empty());
    }

    #[test]
    fn init_params_parse_meerkat_config_path() -> anyhow::Result<()> {
        let line = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "mobkit/init",
            "params": { "meerkat_config_path": "/etc/example/config.toml" }
        })
        .to_string();

        let (_id, params) = parse_init_request(&line)?;

        assert_eq!(
            params.meerkat_config_path.as_deref(),
            Some(Path::new("/etc/example/config.toml"))
        );
        Ok(())
    }

    const HOST_CONFIG_FIXTURE: &str = "[self_hosted.servers.local]\n\
                                       transport = \"openai_compatible\"\n\
                                       base_url = \"http://127.0.0.1:11434\"\n\
                                       api_style = \"chat_completions\"\n\n\
                                       [self_hosted.models.gemma-4-31b]\n\
                                       server = \"local\"\n\
                                       remote_model = \"gemma4:31b\"\n";

    /// The host config is baked into every agent, so the resume key must
    /// change when its content changes or when it is dropped; otherwise a
    /// re-pointed config.toml resumes a runtime built from the old one (the
    /// dead-knob failure the compaction key already guards against).
    /// `.rkat/` is outside the scanned config roots, so this only holds
    /// because the path is pushed by name.
    #[test]
    fn config_fingerprint_changes_with_meerkat_host_config() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let rkat = temp.path().join(".rkat");
        fs::create_dir_all(&rkat)?;
        let config_toml = rkat.join("config.toml");
        fs::write(&config_toml, HOST_CONFIG_FIXTURE)?;
        let paths = conventional_paths(temp.path());
        assert_eq!(
            paths.meerkat_config_toml.as_deref(),
            Some(config_toml.as_path()),
            "the workspace .rkat/config.toml must be adopted by convention"
        );
        let fingerprint = |paths: &ConventionalPaths| {
            config_fingerprint(
                temp.path(),
                None,
                false,
                "tux-auto",
                false,
                false,
                temp.path(),
                temp.path(),
                temp.path(),
                None,
                None,
                None,
                meerkat_mobkit::gateway_composition::DEFAULT_GATEWAY_HTTP_LISTEN,
                None,
                paths,
            )
        };

        let first = fingerprint(&paths)?;
        fs::write(&config_toml, HOST_CONFIG_FIXTURE.replace("11434", "8000"))?;
        let second = fingerprint(&paths)?;
        assert_ne!(
            first, second,
            "a changed host config must not resume the previous runtime"
        );

        let mut without = paths;
        without.meerkat_config_toml = None;
        assert_ne!(
            fingerprint(&without)?,
            second,
            "dropping the host config must not resume a runtime built with it"
        );
        Ok(())
    }

    /// One producer for both launch arms: the host config's tables reach the
    /// factory config and the compaction declaration still pins on top.
    #[test]
    fn gateway_agent_config_layers_compaction_over_the_host_config() -> anyhow::Result<()> {
        let mut host = Config::default();
        host.merge_toml_str(HOST_CONFIG_FIXTURE)?;
        host.model_fallback.enabled = Some(false);
        host.model_fallback.policy.trigger_after_attempts = 7;
        let pinned = meerkat_mobkit::parse_compaction_policy(&json!({
            "auto_compact_threshold": 120_000
        }))
        .map_err(|e| anyhow!("{e}"))?;

        let config = gateway_agent_config(Some(&host), Some(&pinned))?;

        assert_eq!(
            config
                .self_hosted
                .models
                .get("gemma-4-31b")
                .map(|model| model.server.as_str()),
            Some("local")
        );
        assert_eq!(config.compaction.auto_compact_threshold, 120_000);
        assert!(config.compaction.auto_compact_threshold_explicit);
        assert_eq!(config.model_fallback, host.model_fallback);

        let defaulted = gateway_agent_config(None, None)?;
        assert!(defaulted.self_hosted.models.is_empty());
        assert!(!defaulted.model_fallback.is_enabled());
        Ok(())
    }

    /// `[self_hosted]` in the workspace mob.toml is refused at load with the
    /// typed message that names the file carrying host config, not dropped.
    #[test]
    fn load_definition_refuses_host_config_tables_in_mob_toml() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let config = temp.path().join("config");
        fs::create_dir_all(&config)?;
        fs::write(
            config.join("mob.toml"),
            "[mob]\nid = \"m\"\n\n[profiles.w]\nmodel = \"gpt-5.5\"\n\n\
             [self_hosted.servers.local]\nbase_url = \"http://127.0.0.1:11434\"\n",
        )?;
        let paths = conventional_paths(temp.path());

        let error = load_definition(temp.path(), "fingerprint", &paths)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();

        assert!(
            error.contains("[self_hosted]") && error.contains("meerkat_config_path"),
            "{error}"
        );
        Ok(())
    }

    #[test]
    fn config_fingerprint_changes_with_console_read_only() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let paths = conventional_paths(temp.path());

        let writable = config_fingerprint(
            temp.path(),
            None,
            false,
            "tux-auto",
            false,
            false,
            temp.path(),
            temp.path(),
            temp.path(),
            None,
            None,
            None,
            meerkat_mobkit::gateway_composition::DEFAULT_GATEWAY_HTTP_LISTEN,
            None,
            &paths,
        )?;
        let read_only = config_fingerprint(
            temp.path(),
            None,
            false,
            "tux-auto",
            false,
            true,
            temp.path(),
            temp.path(),
            temp.path(),
            None,
            None,
            None,
            meerkat_mobkit::gateway_composition::DEFAULT_GATEWAY_HTTP_LISTEN,
            None,
            &paths,
        )?;

        assert_ne!(writable, read_only);
        Ok(())
    }

    /// A launch that changes the compaction policy must not resume a runtime
    /// built with the previous one: the policy is baked into every agent the
    /// runtime constructs, so reuse would silently keep the old trigger.
    #[test]
    fn config_fingerprint_changes_with_compaction_policy() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let paths = conventional_paths(temp.path());
        let pinned = meerkat_mobkit::parse_compaction_policy(&json!({
            "auto_compact_threshold": 120_000
        }))
        .map_err(|e| anyhow!("{e}"))?;
        let lower = meerkat_mobkit::parse_compaction_policy(&json!({
            "auto_compact_threshold": 60_000
        }))
        .map_err(|e| anyhow!("{e}"))?;

        let inherited = config_fingerprint(
            temp.path(),
            None,
            false,
            "tux-auto",
            false,
            false,
            temp.path(),
            temp.path(),
            temp.path(),
            None,
            None,
            None,
            meerkat_mobkit::gateway_composition::DEFAULT_GATEWAY_HTTP_LISTEN,
            None,
            &paths,
        )?;
        let pinned_key = config_fingerprint(
            temp.path(),
            None,
            false,
            "tux-auto",
            false,
            false,
            temp.path(),
            temp.path(),
            temp.path(),
            None,
            Some(&pinned),
            None,
            meerkat_mobkit::gateway_composition::DEFAULT_GATEWAY_HTTP_LISTEN,
            None,
            &paths,
        )?;
        let lower_key = config_fingerprint(
            temp.path(),
            None,
            false,
            "tux-auto",
            false,
            false,
            temp.path(),
            temp.path(),
            temp.path(),
            None,
            Some(&lower),
            None,
            meerkat_mobkit::gateway_composition::DEFAULT_GATEWAY_HTTP_LISTEN,
            None,
            &paths,
        )?;

        assert_ne!(
            inherited, pinned_key,
            "pinning a threshold must not resume an inheriting runtime"
        );
        assert_ne!(
            pinned_key, lower_key,
            "two different thresholds must not share a runtime"
        );
        Ok(())
    }
}
