//! The MobKit composite storage-provider seam (Phase M4).
//!
//! Meerkat's [`RealmStorageProvider`] supplies the meerkat-shared stores
//! (sessions, runtime, schedule, workgraph, blobs, artifacts) and cannot
//! name MobKit-owned types without reversing the crate dependency. MobKit
//! therefore defines its own composite seam: a [`MobKitStorageProvider`]
//! *wraps or references* a [`RealmStorageProvider`] for the meerkat-shared
//! level and additionally opens the **realm-wide** MobKit store set —
//! continuity, lease-fencing authority, event log, console timeline,
//! metadata, blobs, agent memory, schedule — each slot paired with a
//! machine-readable [`DurabilityDeclaration`]. These are realm-wide stores
//! opened once at bootstrap, not per-mob objects.
//!
//! **One remote bundle, one seam**: a downstream backend (BigQuery,
//! Postgres, object stores) implements one `MobKitStorageProvider` and
//! covers both levels — sessions, continuity, events, blobs, and memory
//! flow through a single integration surface, with
//! `mobkit-store-conformance` (plus meerkat's conformance profiles via
//! [`MobKitStorageProvider::meerkat_provider`]) as the acceptance suite.
//!
//! **Fail-closed composition**: every durable slot in the returned set
//! resolves to provider-supplied storage, or an *explicitly declared*
//! ephemeral choice, or the open fails typed — never a silent in-memory
//! fallback ([`enforce_fail_closed_store_set`]).

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use meerkat::storage_provider::RealmStorageProvider;
use meerkat_core::{DurabilityDeclaration, DurabilityResolution};

use crate::blob_store::{BinaryBlobStore, ObjectStoreBlobStore};
use crate::console_aggregator::{ConsoleLogStore, SqliteConsoleLogStore};
use crate::identity_first::contracts::{ContinuityStore, LeaseProvider};
use crate::identity_first::{AgentMemoryProvider, LocalContinuityStore};
use crate::memory::sqlite_store::SqliteAgentMemoryStore;
use crate::runtime::{PersistentMetadataStore, SqliteMetadataStore};
use crate::storage_doctor::MobKitStorageMigrator;
use crate::storage_layout::MobKitStorageLayout;
use crate::unified_runtime::EventLogStore;

/// Everything a provider needs to open a realm's MobKit stores.
#[derive(Clone)]
pub struct MobKitRealmOpenContext {
    /// The path authority for the realm's state directory (canonical-name-
    /// first probing; disk providers derive every file location from it).
    pub layout: MobKitStorageLayout,
    /// The realm's state directory root (`layout.state_dir()`, owned).
    pub state_dir: PathBuf,
    /// Durable-class domains the embedder explicitly declared ephemeral
    /// (`"blobs"`, `"runtime"`, ...). A durable slot resolving non-persistent
    /// without its domain listed here fails composition.
    pub declared_ephemeral_domains: Vec<String>,
}

impl MobKitRealmOpenContext {
    /// Context over a state directory with no ephemeral declarations.
    pub fn for_state_dir(state_dir: impl Into<PathBuf>) -> Self {
        let state_dir = state_dir.into();
        Self {
            layout: MobKitStorageLayout::with_injected_roots(state_dir.clone(), None),
            state_dir,
            declared_ephemeral_domains: Vec::new(),
        }
    }

    /// Whether `domain` was explicitly declared ephemeral.
    pub fn is_declared_ephemeral(&self, domain: &str) -> bool {
        self.declared_ephemeral_domains
            .iter()
            .any(|declared| declared == domain)
    }
}

/// Live-ownership authority for the realm: either a full [`LeaseProvider`]
/// or the persisted fencing-token floor the composition seeds the bundled
/// `LocalLeaseProvider::with_floor` from (the disk provider's shape — its
/// lease state is process-local, only the floor is durable).
#[derive(Clone)]
pub enum MobKitLeaseAuthority {
    Provider(Arc<dyn LeaseProvider>),
    FencingFloor(u64),
}

/// The realm-wide MobKit stores a provider supplies, each slot covered by a
/// [`DurabilityDeclaration`] in [`Self::durability`].
///
/// `event_log_store` and `agent_memory_provider` are optional features:
/// `None` means the provider supplies no backend for them (the composition
/// keeps its declared defaults — no event ingestion / no agent memory) and
/// the corresponding declaration records that as an explicit choice.
pub struct MobKitRealmStoreSet {
    pub continuity_store: Arc<dyn ContinuityStore>,
    pub lease_authority: MobKitLeaseAuthority,
    pub event_log_store: Option<Box<dyn EventLogStore>>,
    pub console_log_store: Arc<dyn ConsoleLogStore>,
    pub metadata_store: Arc<dyn PersistentMetadataStore>,
    pub blob_store: Arc<dyn BinaryBlobStore>,
    pub agent_memory_provider: Option<Arc<dyn AgentMemoryProvider>>,
    pub schedule_store: Arc<dyn meerkat::ScheduleStore>,
    /// Per-slot durability declarations, machine-readable. Domain names:
    /// `continuity`, `event_log`, `console`, `metadata`, `blobs`,
    /// `agent_memory`, `schedule`.
    pub durability: Vec<DurabilityDeclaration>,
}

/// Typed failure opening or validating a realm's MobKit store set.
#[derive(Debug)]
pub enum MobKitStorageProviderError {
    #[cfg(feature = "extension-state")]
    Unsupported { capability: &'static str },
    /// A slot's backend failed to open.
    Open { slot: String, message: String },
    /// The fail-closed durability rule refused the set: a `Durable` slot
    /// resolved non-persistent without an explicit ephemeral declaration.
    DurabilityViolation { domain: String },
}

impl std::fmt::Display for MobKitStorageProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "extension-state")]
            Self::Unsupported { capability } => {
                write!(f, "storage provider does not support {capability}")
            }
            Self::Open { slot, message } => {
                write!(f, "failed to open the {slot} store: {message}")
            }
            Self::DurabilityViolation { domain } => write!(
                f,
                "durable storage slot '{domain}' resolved to a non-persistent \
                 backend without an explicit ephemeral declaration — declare \
                 the domain ephemeral or supply persistent storage \
                 (fail-closed durability, storage-unification principle 7)"
            ),
        }
    }
}

impl std::error::Error for MobKitStorageProviderError {}

/// One provider supplies all realm-wide MobKit stores plus (via
/// [`Self::meerkat_provider`]) the meerkat-shared stores — the single seam a
/// downstream backend implements.
#[async_trait]
pub trait MobKitStorageProvider: Send + Sync {
    /// Optional durable document capability. It is opened only when a native
    /// package explicitly declares this requirement; no local fallback exists.
    #[cfg(feature = "extension-state")]
    async fn open_extension_state(
        &self,
        _ctx: &MobKitRealmOpenContext,
    ) -> Result<OpenedExtensionState, MobKitStorageProviderError> {
        Err(MobKitStorageProviderError::Unsupported {
            capability: "extension-state",
        })
    }
    /// Stable provider name (`"disk"` for the built-in implementation).
    fn name(&self) -> &str;

    /// Open (or create) the realm's MobKit stores. Implementations must
    /// apply [`enforce_fail_closed_store_set`] (or an equivalent check)
    /// before returning, so an undeclared non-persistent durable slot is a
    /// typed startup error at the seam.
    async fn open_realm(
        &self,
        ctx: &MobKitRealmOpenContext,
    ) -> Result<MobKitRealmStoreSet, MobKitStorageProviderError>;

    /// The meerkat-level provider for the same backend: sessions, runtime,
    /// schedule, workgraph, blobs, artifacts. One downstream implements one
    /// `MobKitStorageProvider` and covers both levels.
    fn meerkat_provider(&self) -> &dyn RealmStorageProvider;

    /// The provider's storage-maintenance hook (diagnosis now; the mutation
    /// verbs land with the M6 migration framework). `None` = no migration
    /// story yet.
    fn migrator(&self) -> Option<&dyn meerkat_core::StorageMigrator> {
        None
    }
}

/// Every durability domain a [`MobKitRealmStoreSet`] must declare — one
/// declaration per slot, no omissions. Mirrors the meerkat-level
/// `REQUIRED_DURABILITY_DOMAINS` contract for the mobkit slots.
pub const REQUIRED_MOBKIT_DURABILITY_DOMAINS: [&str; 7] = [
    "continuity",
    "event_log",
    "console",
    "metadata",
    "blobs",
    "agent_memory",
    "schedule",
];

/// Enforce the fail-closed durability rule against the context's explicit
/// ephemeral declarations.
///
/// Completeness first: every slot in
/// [`REQUIRED_MOBKIT_DURABILITY_DOMAINS`] must carry exactly one
/// declaration — a provider cannot dodge the rule by omitting a domain or
/// returning an empty list. Then each declaration is checked: a `Durable`
/// slot resolving `NonPersistent` without its domain declared ephemeral
/// refuses composition.
pub fn enforce_fail_closed_store_set(
    set: &MobKitRealmStoreSet,
    ctx: &MobKitRealmOpenContext,
) -> Result<(), MobKitStorageProviderError> {
    for required in REQUIRED_MOBKIT_DURABILITY_DOMAINS {
        let count = set
            .durability
            .iter()
            .filter(|declaration| declaration.domain == required)
            .count();
        if count != 1 {
            return Err(MobKitStorageProviderError::DurabilityViolation {
                domain: format!(
                    "{required} (provider supplied {count} durability declarations for this                      slot; exactly one is required)"
                ),
            });
        }
    }
    for declaration in &set.durability {
        if declaration.is_undeclared_nonpersistent_durable()
            && !ctx.is_declared_ephemeral(&declaration.domain)
        {
            return Err(MobKitStorageProviderError::DurabilityViolation {
                domain: declaration.domain.clone(),
            });
        }
    }
    Ok(())
}

/// The meerkat-level realm id for a composite provider's shared store set.
/// The state directory is already deployment-scoped, so one constant realm
/// per state dir is stable; a non-disk backend's realm pins external under
/// the provider's name at `<state_dir>/mobkit/realm_manifest.json`.
pub const MEERKAT_LEVEL_REALM_ID: &str = "mobkit";

/// The meerkat-level stores (with their provider-declared durability) the
/// builder reroutes into the mob bootstrap composition when a composite
/// provider's backend is non-disk — the slots the spec would otherwise open
/// as local files, silently splitting the advertised single bundle across
/// backends.
#[derive(Clone)]
pub(crate) struct ProviderMeerkatStores {
    pub provider_name: String,
    pub runtime_store: Arc<dyn meerkat_runtime::RuntimeStore>,
    pub runtime_declaration: DurabilityDeclaration,
    pub workgraph_store: Arc<dyn meerkat::WorkGraphStore>,
    pub workgraph_declaration: DurabilityDeclaration,
    pub job_store: Arc<dyn meerkat::DetachedJobStore>,
    pub job_declaration: DurabilityDeclaration,
}

impl ProviderMeerkatStores {
    pub(crate) fn runtime_slot_summary(&self) -> crate::storage_health::StorageSlotSummary {
        self.slot_summary(&self.runtime_declaration)
    }

    pub(crate) fn workgraph_slot_summary(&self) -> crate::storage_health::StorageSlotSummary {
        self.slot_summary(&self.workgraph_declaration)
    }

    pub(crate) fn job_slot_summary(&self) -> crate::storage_health::StorageSlotSummary {
        self.slot_summary(&self.job_declaration)
    }

    fn slot_summary(
        &self,
        declaration: &DurabilityDeclaration,
    ) -> crate::storage_health::StorageSlotSummary {
        crate::storage_health::StorageSlotSummary {
            durability: crate::storage_health::StorageSlotDurability::Declared(declaration.clone()),
            backend: format!("storage provider '{}'", self.provider_name),
            detail: Some(
                "meerkat-level slot from the composite provider's realm bundle".to_string(),
            ),
            degraded: false,
        }
    }
}

/// Open the meerkat-level store set of a composite provider's backend
/// through the realm convergence the meerkat facade uses: ensure the
/// manifest pin (external under the provider's name for non-disk backends),
/// open through the [`RealmStorageProvider`] seam, and enforce fail-closed
/// durability before any slot is composed.
pub(crate) async fn open_provider_meerkat_stores(
    provider: &dyn RealmStorageProvider,
    ctx: &MobKitRealmOpenContext,
) -> Result<ProviderMeerkatStores, MobKitStorageProviderError> {
    let provider_name = provider.name().to_string();
    let pin_name = (provider_name != "disk").then_some(provider_name.as_str());
    let pin = meerkat_store::realm::ensure_realm_manifest_pin_with_candidates(
        &ctx.state_dir,
        &[],
        MEERKAT_LEVEL_REALM_ID,
        pin_name,
        None,
        None,
    )
    .await
    .map_err(|error| MobKitStorageProviderError::Open {
        slot: "meerkat-level realm manifest".to_string(),
        message: error.to_string(),
    })?;
    let realm = meerkat_core::RealmId::parse(MEERKAT_LEVEL_REALM_ID).map_err(|error| {
        MobKitStorageProviderError::Open {
            slot: "meerkat-level realm".to_string(),
            message: format!("invalid realm id '{MEERKAT_LEVEL_REALM_ID}': {error}"),
        }
    })?;
    let open_ctx = meerkat::storage_provider::RealmOpenContext {
        locator: meerkat_core::RealmLocator {
            state_root: ctx.state_dir.clone(),
            realm,
        },
        manifest: pin.clone(),
        paths: meerkat_store::realm_paths_in(&ctx.state_dir, MEERKAT_LEVEL_REALM_ID),
        layout: None,
    };
    let set = provider
        .open(&open_ctx)
        .await
        .map_err(|error| MobKitStorageProviderError::Open {
            slot: "meerkat-level realm store set".to_string(),
            message: error.to_string(),
        })?;
    // The embedder's explicit ephemeral declarations extend the pinned
    // manifest's, exactly as they gate the mobkit-level slots.
    let mut ephemeral_domains = pin.ephemeral_domains().to_vec();
    ephemeral_domains.extend(ctx.declared_ephemeral_domains.iter().cloned());
    meerkat::storage_provider::enforce_fail_closed_durability(&set, &ephemeral_domains).map_err(
        |error| match error {
            meerkat::PersistenceError::DurabilityViolation { domain } => {
                MobKitStorageProviderError::DurabilityViolation {
                    domain: format!("{domain} (meerkat level)"),
                }
            }
            other => MobKitStorageProviderError::Open {
                slot: "meerkat-level realm store set".to_string(),
                message: other.to_string(),
            },
        },
    )?;
    let declaration = |domain: &str| {
        set.durability
            .iter()
            .find(|declaration| declaration.domain == domain)
            .cloned()
            .ok_or_else(|| MobKitStorageProviderError::DurabilityViolation {
                domain: format!("{domain} (meerkat level: declaration missing)"),
            })
    };
    let runtime_declaration = declaration("runtime")?;
    let workgraph_declaration = declaration("workgraph")?;
    let job_declaration = declaration("jobs")?;
    Ok(ProviderMeerkatStores {
        provider_name,
        runtime_store: set.runtime_store,
        runtime_declaration,
        workgraph_store: set.workgraph_store,
        workgraph_declaration,
        job_store: set.job_store,
        job_declaration,
    })
}

/// The built-in disk provider: reproduces today's SQLite/object-store layout
/// via the M2 [`MobKitStorageLayout`] locators and the M3 ledgered openers.
#[derive(Debug, Clone, Copy, Default)]
pub struct DiskMobKitStorageProvider;

impl DiskMobKitStorageProvider {
    fn open_error(slot: &str, message: impl std::fmt::Display) -> MobKitStorageProviderError {
        MobKitStorageProviderError::Open {
            slot: slot.to_string(),
            message: message.to_string(),
        }
    }
}

#[async_trait]
impl MobKitStorageProvider for DiskMobKitStorageProvider {
    #[cfg(feature = "extension-state")]
    async fn open_extension_state(
        &self,
        ctx: &MobKitRealmOpenContext,
    ) -> Result<OpenedExtensionState, MobKitStorageProviderError> {
        if ctx.layout.is_declared_ephemeral() || ctx.is_declared_ephemeral("extension-state") {
            return Err(MobKitStorageProviderError::DurabilityViolation {
                domain: "extension-state (package requires persistent storage)".into(),
            });
        }
        std::fs::create_dir_all(&ctx.state_dir)
            .map_err(|e| Self::open_error("extension-state", e))?;
        let path = ctx
            .layout
            .extension_state_db()
            .map_err(|e| Self::open_error("extension-state", e))?
            .path;
        let store = crate::extension_state::open_disk_store(path)
            .map_err(|e| Self::open_error("extension-state", e))?;
        Ok(OpenedExtensionState {
            store: Arc::new(store),
            durability: DurabilityDeclaration::durable(
                "extension-state",
                DurabilityResolution::Persistent,
            ),
        })
    }
    fn name(&self) -> &'static str {
        "disk"
    }

    async fn open_realm(
        &self,
        ctx: &MobKitRealmOpenContext,
    ) -> Result<MobKitRealmStoreSet, MobKitStorageProviderError> {
        std::fs::create_dir_all(&ctx.state_dir)
            .map_err(|error| Self::open_error("state directory", error))?;
        let layout = &ctx.layout;

        let continuity_path = layout
            .continuity_db()
            .map_err(|error| Self::open_error("continuity", error))?
            .path;
        let (continuity_store, fencing_floor) =
            LocalContinuityStore::open_with_fencing_floor(continuity_path)
                .await
                .map_err(|error| Self::open_error("continuity", error))?;

        let console_path = layout
            .console_db()
            .map_err(|error| Self::open_error("console", error))?
            .path;
        let console_log_store = SqliteConsoleLogStore::open(&console_path)
            .map_err(|error| Self::open_error("console", error))?;

        let metadata_path = layout
            .metadata_db()
            .map_err(|error| Self::open_error("metadata", error))?
            .path;
        let metadata_store = SqliteMetadataStore::open(&metadata_path)
            .map_err(|error| Self::open_error("metadata", error))?;

        let (blob_store, blob_resolution): (Arc<dyn BinaryBlobStore>, DurabilityResolution) =
            if ctx.is_declared_ephemeral("blobs") {
                (
                    Arc::new(ObjectStoreBlobStore::memory()),
                    DurabilityResolution::DeclaredEphemeral,
                )
            } else {
                (
                    Arc::new(
                        ObjectStoreBlobStore::local(layout.blob_root())
                            .map_err(|error| Self::open_error("blobs", error))?,
                    ),
                    DurabilityResolution::Persistent,
                )
            };

        let schedule_store = meerkat::SqliteScheduleStore::open(layout.schedule_db())
            .map_err(|error| Self::open_error("schedule", error))?;

        let agent_memory_root = layout
            .agent_memory_root()
            .map_err(|error| Self::open_error("agent memory", error))?
            .path;
        let agent_memory_provider = SqliteAgentMemoryStore::open(&agent_memory_root)
            .map_err(|error| Self::open_error("agent memory", error))?;

        let set = MobKitRealmStoreSet {
            continuity_store: Arc::new(continuity_store),
            lease_authority: MobKitLeaseAuthority::FencingFloor(fencing_floor),
            // No durable event-log backend ships with the disk bundle; the
            // absence of ingestion is the declared default, overridable per
            // surface (builder `event_log()`, gateway
            // `runtime_options.event_log`).
            event_log_store: None,
            console_log_store: Arc::new(console_log_store),
            metadata_store: Arc::new(metadata_store),
            blob_store,
            agent_memory_provider: Some(Arc::new(agent_memory_provider)),
            schedule_store: Arc::new(schedule_store),
            durability: vec![
                DurabilityDeclaration::durable("continuity", DurabilityResolution::Persistent),
                DurabilityDeclaration::durable(
                    "event_log",
                    DurabilityResolution::DeclaredEphemeral,
                ),
                DurabilityDeclaration::durable("console", DurabilityResolution::Persistent),
                DurabilityDeclaration::durable("metadata", DurabilityResolution::Persistent),
                DurabilityDeclaration::durable("blobs", blob_resolution),
                DurabilityDeclaration::durable("agent_memory", DurabilityResolution::Persistent),
                DurabilityDeclaration::durable("schedule", DurabilityResolution::Persistent),
            ],
        };
        enforce_fail_closed_store_set(&set, ctx)?;
        Ok(set)
    }

    fn meerkat_provider(&self) -> &dyn RealmStorageProvider {
        static DISK: meerkat::storage_provider::DiskStorageProvider =
            meerkat::storage_provider::DiskStorageProvider;
        &DISK
    }

    fn migrator(&self) -> Option<&dyn meerkat_core::StorageMigrator> {
        static MIGRATOR: MobKitStorageMigrator = MobKitStorageMigrator;
        Some(&MIGRATOR)
    }
}

#[cfg(feature = "extension-state")]
pub struct OpenedExtensionState {
    pub store: Arc<dyn mobkit_extension_state::ExtensionDocumentStore>,
    pub durability: DurabilityDeclaration,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use meerkat_core::DurabilityClass;

    fn context(dir: &std::path::Path) -> MobKitRealmOpenContext {
        MobKitRealmOpenContext::for_state_dir(dir.join("state"))
    }

    /// The disk provider reproduces today's layout: every durable slot
    /// resolves persistent, the event log stays a declared default, and the
    /// lease authority is the persisted fencing floor.
    #[tokio::test]
    async fn disk_provider_opens_the_canonical_layout_fail_closed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let provider = DiskMobKitStorageProvider;
        assert_eq!(provider.name(), "disk");
        let set = provider
            .open_realm(&context(dir.path()))
            .await
            .expect("disk realm opens");

        assert!(matches!(
            set.lease_authority,
            MobKitLeaseAuthority::FencingFloor(0)
        ));
        assert!(set.event_log_store.is_none());
        assert!(set.agent_memory_provider.is_some());
        assert!(set.blob_store.is_persistent());
        for declaration in &set.durability {
            assert_eq!(declaration.class, DurabilityClass::Durable);
            match declaration.domain.as_str() {
                "event_log" => assert_eq!(
                    declaration.resolution,
                    DurabilityResolution::DeclaredEphemeral
                ),
                _ => assert_eq!(declaration.resolution, DurabilityResolution::Persistent),
            }
        }
        assert!(provider.migrator().is_some());
        assert_eq!(provider.meerkat_provider().name(), "disk");
    }

    /// Declared-ephemeral blobs resolve to the memory backend as an explicit
    /// choice, and the declaration records it.
    #[tokio::test]
    async fn disk_provider_honors_declared_ephemeral_blobs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut ctx = context(dir.path());
        ctx.declared_ephemeral_domains.push("blobs".to_string());
        let set = DiskMobKitStorageProvider
            .open_realm(&ctx)
            .await
            .expect("declared-ephemeral realm opens");
        assert!(!set.blob_store.is_persistent());
        let blob_declaration = set
            .durability
            .iter()
            .find(|declaration| declaration.domain == "blobs")
            .expect("blobs declaration");
        assert_eq!(
            blob_declaration.resolution,
            DurabilityResolution::DeclaredEphemeral
        );
    }

    /// The fail-closed rule: a durable slot resolving non-persistent without
    /// a declaration refuses composition with the typed error.
    #[tokio::test]
    async fn enforce_refuses_undeclared_nonpersistent_durable_slots() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ctx = context(dir.path());
        let mut set = DiskMobKitStorageProvider
            .open_realm(&ctx)
            .await
            .expect("disk realm opens");
        for declaration in &mut set.durability {
            if declaration.domain == "continuity" {
                declaration.resolution = DurabilityResolution::NonPersistent;
            }
        }
        let error = enforce_fail_closed_store_set(&set, &ctx)
            .expect_err("undeclared non-persistent durable slot must refuse");
        assert!(matches!(
            error,
            MobKitStorageProviderError::DurabilityViolation { ref domain } if domain == "continuity"
        ));
        assert!(error.to_string().contains("fail-closed"));

        // The same resolution with the domain declared ephemeral composes.
        let mut declared = ctx.clone();
        declared
            .declared_ephemeral_domains
            .push("continuity".to_string());
        enforce_fail_closed_store_set(&set, &declared)
            .expect("declared ephemeral domain must compose");
    }

    /// Completeness: a provider cannot dodge the fail-closed rule by
    /// omitting a domain's declaration (or declaring it twice).
    #[tokio::test]
    async fn enforce_refuses_omitted_and_duplicate_declarations() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ctx = context(dir.path());
        let set = DiskMobKitStorageProvider
            .open_realm(&ctx)
            .await
            .expect("disk realm opens");

        let mut omitted = set.durability.clone();
        omitted.retain(|declaration| declaration.domain != "metadata");
        let mut incomplete = MobKitRealmStoreSet {
            durability: omitted,
            ..clone_stores(&set)
        };
        let error = enforce_fail_closed_store_set(&incomplete, &ctx)
            .expect_err("an omitted durability declaration must refuse");
        assert!(matches!(
            error,
            MobKitStorageProviderError::DurabilityViolation { ref domain }
                if domain.starts_with("metadata") && domain.contains("0 durability declarations")
        ));

        incomplete.durability = set.durability;
        incomplete.durability.push(DurabilityDeclaration::durable(
            "metadata",
            DurabilityResolution::Persistent,
        ));
        let error = enforce_fail_closed_store_set(&incomplete, &ctx)
            .expect_err("a duplicated durability declaration must refuse");
        assert!(matches!(
            error,
            MobKitStorageProviderError::DurabilityViolation { ref domain }
                if domain.starts_with("metadata") && domain.contains("2 durability declarations")
        ));
    }

    fn clone_stores(set: &MobKitRealmStoreSet) -> MobKitRealmStoreSet {
        MobKitRealmStoreSet {
            continuity_store: Arc::clone(&set.continuity_store),
            lease_authority: set.lease_authority.clone(),
            event_log_store: None,
            console_log_store: Arc::clone(&set.console_log_store),
            metadata_store: Arc::clone(&set.metadata_store),
            blob_store: Arc::clone(&set.blob_store),
            agent_memory_provider: set.agent_memory_provider.clone(),
            schedule_store: Arc::clone(&set.schedule_store),
            durability: set.durability.clone(),
        }
    }

    /// Non-disk meerkat-level provider stub: in-memory stores with
    /// self-declared ephemerality, plus a configurable runtime resolution
    /// for the fail-closed refusal case.
    struct StubMeerkatProvider {
        opened: std::sync::atomic::AtomicBool,
        runtime_resolution: DurabilityResolution,
    }

    impl StubMeerkatProvider {
        fn declared_ephemeral() -> Self {
            Self {
                opened: std::sync::atomic::AtomicBool::new(false),
                runtime_resolution: DurabilityResolution::DeclaredEphemeral,
            }
        }
    }

    #[async_trait]
    impl RealmStorageProvider for StubMeerkatProvider {
        fn name(&self) -> &'static str {
            "stub-remote"
        }

        async fn open(
            &self,
            ctx: &meerkat::storage_provider::RealmOpenContext,
        ) -> Result<meerkat::storage_provider::RealmStoreSet, meerkat::PersistenceError> {
            self.opened.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(meerkat::storage_provider::RealmStoreSet {
                session_store: Arc::new(meerkat_store::MemoryStore::new()),
                runtime_store: Arc::new(meerkat_runtime::InMemoryRuntimeStore::new()),
                // Durable jobs (meerkat 0.8.8) added a job store to the set.
                job_store: Arc::new(meerkat::MemoryDetachedJobStore::new()),
                schedule_store: Arc::new(meerkat_schedule::MemoryScheduleStore::new()),
                workgraph_store: Arc::new(meerkat::MemoryWorkGraphStore::new()),
                blob_store: Arc::new(meerkat_store::MemoryBlobStore::new()),
                artifact_store: Arc::new(meerkat_store::MemoryArtifactStore::new()),
                store_path: ctx.paths.root.clone(),
                projection_root: None,
                durability: [
                    "sessions",
                    "jobs",
                    "schedule",
                    "workgraph",
                    "blobs",
                    "artifacts",
                ]
                .iter()
                .map(|domain| {
                    DurabilityDeclaration::durable(domain, DurabilityResolution::DeclaredEphemeral)
                })
                .chain(std::iter::once(DurabilityDeclaration::durable(
                    "runtime",
                    self.runtime_resolution,
                )))
                .collect(),
            })
        }
    }

    /// M4b: the meerkat-level open routes through the provider's realm seam
    /// — the provider is genuinely opened, its runtime/workgraph stores and
    /// their verbatim declarations come back, and the realm is pinned
    /// external under the provider's name.
    #[tokio::test]
    async fn meerkat_level_open_routes_the_provider_bundle_and_pins_external() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ctx = context(dir.path());
        let provider = StubMeerkatProvider::declared_ephemeral();

        let stores = open_provider_meerkat_stores(&provider, &ctx)
            .await
            .expect("stub meerkat bundle opens");

        assert!(provider.opened.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(stores.provider_name, "stub-remote");
        assert_eq!(
            stores.runtime_declaration.resolution,
            DurabilityResolution::DeclaredEphemeral
        );
        assert_eq!(
            stores.workgraph_declaration.resolution,
            DurabilityResolution::DeclaredEphemeral
        );
        let runtime_slot = stores.runtime_slot_summary();
        assert_eq!(runtime_slot.durability.domain(), "runtime");
        assert!(runtime_slot.backend.contains("stub-remote"));
        let manifest_path = ctx
            .state_dir
            .join(MEERKAT_LEVEL_REALM_ID)
            .join("realm_manifest.json");
        let manifest = std::fs::read_to_string(&manifest_path)
            .expect("meerkat-level realm manifest must be pinned");
        assert!(
            manifest.contains("stub-remote"),
            "the pin must carry the provider name, got: {manifest}"
        );
    }

    /// The meerkat-level fail-closed rule holds at the composite seam: an
    /// undeclared non-persistent durable slot refuses composition typed and
    /// names the level.
    #[tokio::test]
    async fn meerkat_level_open_refuses_undeclared_nonpersistent_durables() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ctx = context(dir.path());
        let provider = StubMeerkatProvider {
            opened: std::sync::atomic::AtomicBool::new(false),
            runtime_resolution: DurabilityResolution::NonPersistent,
        };

        let error = match open_provider_meerkat_stores(&provider, &ctx).await {
            Err(error) => error,
            Ok(_) => panic!("an undeclared non-persistent runtime slot must refuse"),
        };
        assert!(matches!(
            error,
            MobKitStorageProviderError::DurabilityViolation { ref domain }
                if domain.contains("runtime") && domain.contains("meerkat level")
        ));

        // The same resolution composes when the embedder declared the
        // domain ephemeral (the builder's ephemeral_runtime_store gate).
        let mut declared = ctx.clone();
        declared
            .declared_ephemeral_domains
            .push("runtime".to_string());
        let provider = StubMeerkatProvider {
            opened: std::sync::atomic::AtomicBool::new(false),
            runtime_resolution: DurabilityResolution::NonPersistent,
        };
        open_provider_meerkat_stores(&provider, &declared)
            .await
            .expect("a declared-ephemeral runtime domain must compose");
    }
}
