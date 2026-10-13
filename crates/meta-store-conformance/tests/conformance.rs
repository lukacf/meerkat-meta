//! In-repo instantiations: every chapter runs against the bundled Meta
//! implementations (or, where Meta bundles no production store for a
//! trait, against the in-crate reference implementation that proves the
//! chapter satisfiable).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use meerkat_meta::blob_store::{BinaryBlobStore, ObjectStoreBlobStore};
use meerkat_meta::console_aggregator::{
    ConsoleLogStore, InMemoryConsoleLogStore, SqliteConsoleLogStore,
};
use meerkat_meta::identity_first as contracts;
use meerkat_meta::identity_first::{AgentMemoryProvider, ContinuityStore, LocalContinuityStore};
use meerkat_meta::memory::SqliteAgentMemoryStore;
use meerkat_meta::unified_runtime::EventLogStore;
use meta_store_conformance::{
    AgentMemoryProviderFactory, BinaryBlobStoreFactory, CompatRollbackContinuityStore,
    ConformanceFailure, ConsoleLogStoreFactory, ContinuityStoreFactory, EventLogStoreFactory,
    ReferenceEventLogStore, ReferenceInMemoryAgentMemoryStore, chapters,
};

// ---------------------------------------------------------------------------
// Factories over the bundled implementations
// ---------------------------------------------------------------------------

/// File-backed `LocalContinuityStore`: each open is a NEW handle over the
/// same database file (models a process restart).
struct LocalFileContinuityFactory {
    path: PathBuf,
}

#[async_trait]
impl ContinuityStoreFactory for LocalFileContinuityFactory {
    async fn open(&self) -> Result<Arc<dyn ContinuityStore>, ConformanceFailure> {
        let store = LocalContinuityStore::open(&self.path)
            .map_err(|error| ConformanceFailure::new("factory", "open", error.to_string()))?;
        Ok(Arc::new(store))
    }
}

/// Shared-handle factory for deliberately non-persistent continuity stores.
struct SharedContinuityFactory {
    store: Arc<dyn ContinuityStore>,
}

#[async_trait]
impl ContinuityStoreFactory for SharedContinuityFactory {
    async fn open(&self) -> Result<Arc<dyn ContinuityStore>, ConformanceFailure> {
        Ok(Arc::clone(&self.store))
    }
}

struct SqliteConsoleFactory {
    path: PathBuf,
}

#[async_trait]
impl ConsoleLogStoreFactory for SqliteConsoleFactory {
    async fn open(&self) -> Result<Arc<dyn ConsoleLogStore>, ConformanceFailure> {
        let store = SqliteConsoleLogStore::open(&self.path)
            .map_err(|error| ConformanceFailure::new("factory", "open", error.to_string()))?;
        Ok(Arc::new(store))
    }
}

struct SharedConsoleFactory {
    store: Arc<dyn ConsoleLogStore>,
}

#[async_trait]
impl ConsoleLogStoreFactory for SharedConsoleFactory {
    async fn open(&self) -> Result<Arc<dyn ConsoleLogStore>, ConformanceFailure> {
        Ok(Arc::clone(&self.store))
    }
}

struct SharedEventLogFactory {
    store: Arc<dyn EventLogStore>,
}

#[async_trait]
impl EventLogStoreFactory for SharedEventLogFactory {
    async fn open(&self) -> Result<Arc<dyn EventLogStore>, ConformanceFailure> {
        Ok(Arc::clone(&self.store))
    }
}

struct LocalBlobFactory {
    root: PathBuf,
}

#[async_trait]
impl BinaryBlobStoreFactory for LocalBlobFactory {
    async fn open(&self) -> Result<Arc<dyn BinaryBlobStore>, ConformanceFailure> {
        let store = ObjectStoreBlobStore::local(self.root.clone())
            .map_err(|error| ConformanceFailure::new("factory", "open", error.to_string()))?;
        Ok(Arc::new(store))
    }
}

struct SharedBlobFactory {
    store: Arc<dyn BinaryBlobStore>,
}

#[async_trait]
impl BinaryBlobStoreFactory for SharedBlobFactory {
    async fn open(&self) -> Result<Arc<dyn BinaryBlobStore>, ConformanceFailure> {
        Ok(Arc::clone(&self.store))
    }
}

struct SqliteMemoryFactory {
    root: PathBuf,
}

#[async_trait]
impl AgentMemoryProviderFactory for SqliteMemoryFactory {
    async fn open(&self) -> Result<Arc<dyn AgentMemoryProvider>, ConformanceFailure> {
        let store = SqliteAgentMemoryStore::open(&self.root)
            .map_err(|error| ConformanceFailure::new("factory", "open", error.to_string()))?;
        Ok(Arc::new(store))
    }
}

struct ReferenceMemoryFactory;

#[async_trait]
impl AgentMemoryProviderFactory for ReferenceMemoryFactory {
    async fn open(&self) -> Result<Arc<dyn AgentMemoryProvider>, ConformanceFailure> {
        Ok(Arc::new(ReferenceInMemoryAgentMemoryStore::new()))
    }
}

// ---------------------------------------------------------------------------
// ContinuityStore profile
// ---------------------------------------------------------------------------

#[tokio::test]
async fn local_continuity_store_file_backed_passes_continuity_profile() {
    let dir = tempfile::tempdir().expect("tempdir");
    let factory = LocalFileContinuityFactory {
        path: dir.path().join("continuity.sqlite"),
    };
    chapters::continuity_store(&factory)
        .await
        .expect("LocalContinuityStore (file) must satisfy the continuity profile");
}

#[tokio::test]
async fn local_continuity_store_in_memory_passes_continuity_profile() {
    let store = LocalContinuityStore::in_memory().expect("in-memory store");
    let factory = SharedContinuityFactory {
        store: Arc::new(store),
    };
    chapters::continuity_store(&factory)
        .await
        .expect("LocalContinuityStore (memory) must satisfy the continuity profile");
}

#[tokio::test]
async fn compat_reference_store_passes_continuity_profile() {
    let factory = SharedContinuityFactory {
        store: Arc::new(CompatRollbackContinuityStore::new()),
    };
    chapters::continuity_store(&factory)
        .await
        .expect("the in-crate reference store must prove the continuity profile satisfiable");
}

#[tokio::test]
async fn local_continuity_store_rollback_atomic_override() {
    let dir = tempfile::tempdir().expect("tempdir");
    let factory = LocalFileContinuityFactory {
        path: dir.path().join("continuity.sqlite"),
    };
    chapters::continuity_rollback(&factory, chapters::RollbackPath::AtomicOverride)
        .await
        .expect("LocalContinuityStore must satisfy the atomic rollback profile");
}

/// Exercises the trait's NON-ATOMIC compatibility rollback (the reference
/// store deliberately does not override it). M4b fixed the default —
/// delete-then-reinsert restore a conforming generation-monotonic store CAN
/// satisfy — and this pins the fixed behavior, including the documented
/// caveat that the identity's snapshots (previous generation's included) do
/// not survive the compatibility path.
#[tokio::test]
async fn compat_reference_store_rollback_default_impl() {
    let factory = SharedContinuityFactory {
        store: Arc::new(CompatRollbackContinuityStore::new()),
    };
    chapters::continuity_rollback(&factory, chapters::RollbackPath::CompatibilityDefault)
        .await
        .expect("the compatibility rollback pins must hold on the default-impl store");
}

#[tokio::test]
async fn local_continuity_fencing_floor_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    chapters::local_continuity_fencing_floor(dir.path())
        .await
        .expect("the bundled LocalContinuityStore + LocalLeaseProvider pair must keep the floor");
}

// ---------------------------------------------------------------------------
// ContinuitySessionStoreAdapter
// ---------------------------------------------------------------------------

/// H2 pin, FLIPPED by M4b's incremental continuity channel: the bundled
/// `LocalContinuityStore` now makes head+rows the canonical durable session
/// representation, so the capability seam
/// (`ContinuityStore::as_incremental_sessions`) advertises and the adapter
/// forwards it. Identity-first deployments on the bundled store persist
/// O(delta) per save instead of a whole session document. If this pin ever
/// reads `false` again, the delta channel regressed.
#[tokio::test]
async fn continuity_session_adapter_over_local_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let factory = LocalFileContinuityFactory {
        path: dir.path().join("continuity.sqlite"),
    };
    chapters::continuity_session_adapter(&factory, true)
        .await
        .expect("ContinuitySessionStoreAdapter must satisfy the adapter chapter");
}

/// Canary (inverted with the flip): asserting the OLD whole-blob world must
/// now FAIL at the capability step — proving the pin is load-bearing in both
/// directions instead of passing vacuously.
#[tokio::test]
async fn continuity_session_adapter_whole_blob_expectation_fails_today() {
    let dir = tempfile::tempdir().expect("tempdir");
    let factory = LocalFileContinuityFactory {
        path: dir.path().join("continuity.sqlite"),
    };
    let failure = chapters::continuity_session_adapter(&factory, false)
        .await
        .expect_err("expecting the whole-blob degradation today must fail");
    assert_eq!(failure.chapter(), "continuity_session_adapter");
    assert_eq!(failure.step(), "as_incremental_capability");
}

/// The M4b delta channel itself: the upstream meerkat incremental profile
/// run over the real identity-first composition, plus the meta-specific
/// pins (pre-registration parking, per-mutation fence/version discipline,
/// byte-authority uniqueness).
#[tokio::test]
async fn local_continuity_store_passes_incremental_profile() {
    let dir = tempfile::tempdir().expect("tempdir");
    let factory = LocalFileContinuityFactory {
        path: dir.path().join("continuity.sqlite"),
    };
    chapters::continuity_incremental(&factory, 0)
        .await
        .expect("LocalContinuityStore must satisfy the incremental continuity profile");
}

/// A pass-through `ContinuityStore` decorator, the shape a metrics, auth or
/// tenancy layer takes. `forward_channel` decides whether it forwards the
/// inner store's session-delta channel.
struct DecoratorContinuityStore {
    inner: Arc<dyn ContinuityStore>,
    forward_channel: bool,
}

#[async_trait]
impl ContinuityStore for DecoratorContinuityStore {
    async fn resolve_many(
        &self,
        identities: &[contracts::AgentIdentity],
    ) -> Result<
        std::collections::BTreeMap<contracts::AgentIdentity, contracts::ContinuityResolveState>,
        contracts::ContinuityStoreError,
    > {
        self.inner.resolve_many(identities).await
    }

    async fn resolve_record_by_session(
        &self,
        session_id: &meerkat_core::types::SessionId,
    ) -> Result<
        Option<(
            contracts::ContinuityRecord,
            contracts::FencingToken,
            contracts::CheckpointVersion,
        )>,
        contracts::ContinuityStoreError,
    > {
        self.inner.resolve_record_by_session(session_id).await
    }

    async fn load_session_snapshot(
        &self,
        session_id: &meerkat_core::types::SessionId,
    ) -> Result<Option<contracts::SessionSnapshot>, contracts::ContinuityStoreError> {
        self.inner.load_session_snapshot(session_id).await
    }

    async fn session_snapshot_matches_current(
        &self,
        candidate: contracts::SessionSnapshotMatchCandidate,
    ) -> Result<bool, contracts::ContinuityStoreError> {
        self.inner.session_snapshot_matches_current(candidate).await
    }

    async fn delete_session_snapshot_if_current_revision(
        &self,
        session_id: &meerkat_core::types::SessionId,
        expected_current_revision: &str,
    ) -> Result<bool, contracts::ContinuityStoreError> {
        self.inner
            .delete_session_snapshot_if_current_revision(session_id, expected_current_revision)
            .await
    }

    async fn save_session_snapshot(
        &self,
        identity: &contracts::AgentIdentity,
        session_id: &meerkat_core::types::SessionId,
        generation: contracts::ContinuityGeneration,
        version: contracts::CheckpointVersion,
        fencing_token: contracts::FencingToken,
        snapshot: &contracts::SessionSnapshot,
    ) -> Result<(), contracts::ContinuityStoreError> {
        self.inner
            .save_session_snapshot(
                identity,
                session_id,
                generation,
                version,
                fencing_token,
                snapshot,
            )
            .await
    }

    async fn upsert_continuity_record(
        &self,
        record: &contracts::ContinuityRecord,
        fencing_token: contracts::FencingToken,
    ) -> Result<(), contracts::ContinuityStoreError> {
        self.inner
            .upsert_continuity_record(record, fencing_token)
            .await
    }

    async fn rollback_continuity_record(
        &self,
        expected_attempt: &contracts::ContinuityRecord,
        previous: Option<&contracts::ContinuityRecord>,
        fencing_token: contracts::FencingToken,
    ) -> Result<(), contracts::ContinuityStoreError> {
        self.inner
            .rollback_continuity_record(expected_attempt, previous, fencing_token)
            .await
    }

    async fn delete_continuity_record(
        &self,
        identity: &contracts::AgentIdentity,
        fencing_token: contracts::FencingToken,
    ) -> Result<(), contracts::ContinuityStoreError> {
        self.inner
            .delete_continuity_record(identity, fencing_token)
            .await
    }

    fn as_incremental_sessions(&self) -> Option<Arc<dyn contracts::ContinuityIncrementalSessions>> {
        if self.forward_channel {
            self.inner.as_incremental_sessions()
        } else {
            None
        }
    }
}

/// A decorator that forwards the inner store's channel keeps O(delta)
/// session persistence through the wrapper and the session adapter.
#[tokio::test]
async fn forwarding_decorator_preserves_incremental_channel() {
    let dir = tempfile::tempdir().expect("tempdir");
    let factory = LocalFileContinuityFactory {
        path: dir.path().join("continuity.sqlite"),
    };
    let wrap = |inner: Arc<dyn ContinuityStore>| -> Arc<dyn ContinuityStore> {
        Arc::new(DecoratorContinuityStore {
            inner,
            forward_channel: true,
        })
    };
    chapters::continuity_wrapper_preserves_incremental_channel(&factory, &wrap)
        .await
        .expect("a forwarding decorator must keep the session-delta channel");
}

/// A decorator that drops the channel fails the wrapper chapter at the
/// forwarding step: the chapter catches the silent whole-blob degradation.
#[tokio::test]
async fn channel_dropping_decorator_fails_the_wrapper_chapter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let factory = LocalFileContinuityFactory {
        path: dir.path().join("continuity.sqlite"),
    };
    let wrap = |inner: Arc<dyn ContinuityStore>| -> Arc<dyn ContinuityStore> {
        Arc::new(DecoratorContinuityStore {
            inner,
            forward_channel: false,
        })
    };
    let failure = chapters::continuity_wrapper_preserves_incremental_channel(&factory, &wrap)
        .await
        .expect_err("a decorator that drops the channel must fail");
    assert_eq!(failure.chapter(), "continuity_wrapper");
    assert_eq!(failure.step(), "wrapper_forwards_delta_channel");
}

// ---------------------------------------------------------------------------
// EventLogStore profile
// ---------------------------------------------------------------------------

/// Meta bundles no durable production `EventLogStore` (the built-in
/// default is a private null store that drops every event; the rpc_gateway's
/// in-memory store is binary-local). The in-crate reference store proves the
/// profile satisfiable, exactly like the upstream emulated-CAS reference.
#[tokio::test]
async fn reference_event_log_store_satisfies_event_log_profile() {
    let factory = SharedEventLogFactory {
        store: Arc::new(ReferenceEventLogStore::new()),
    };
    chapters::event_log(&factory)
        .await
        .expect("the reference event log store must satisfy the event log profile");
}

// ---------------------------------------------------------------------------
// ConsoleLogStore profile
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_console_store_passes_console_profile() {
    let dir = tempfile::tempdir().expect("tempdir");
    let factory = SqliteConsoleFactory {
        path: dir.path().join("meta_console.sqlite"),
    };
    chapters::console_log(&factory)
        .await
        .expect("SqliteConsoleLogStore must satisfy the console profile");
}

#[tokio::test(flavor = "multi_thread")]
async fn in_memory_console_store_passes_console_profile() {
    let factory = SharedConsoleFactory {
        store: Arc::new(InMemoryConsoleLogStore::new()),
    };
    chapters::console_log(&factory)
        .await
        .expect("InMemoryConsoleLogStore must satisfy the console profile");
}

// ---------------------------------------------------------------------------
// BinaryBlobStore profile
// ---------------------------------------------------------------------------

#[tokio::test]
async fn local_blob_store_passes_binary_blobs_profile() {
    let dir = tempfile::tempdir().expect("tempdir");
    let factory = LocalBlobFactory {
        root: dir.path().join("blobs"),
    };
    chapters::binary_blobs(&factory, Some(true))
        .await
        .expect("ObjectStoreBlobStore::local must satisfy the binary blob profile");
}

/// The memory backend must answer `is_persistent() == false` — a memory
/// store claiming persistence is the exact silent-loss hazard H1 deletes.
#[tokio::test]
async fn memory_blob_store_passes_binary_blobs_profile() {
    let factory = SharedBlobFactory {
        store: Arc::new(ObjectStoreBlobStore::memory()),
    };
    chapters::binary_blobs(&factory, Some(false))
        .await
        .expect("ObjectStoreBlobStore::memory must satisfy the binary blob profile");
}

#[tokio::test]
async fn local_blob_store_reads_legacy_fs_layout() {
    let dir = tempfile::tempdir().expect("tempdir");
    chapters::legacy_blob_layout(dir.path())
        .await
        .expect("the legacy filesystem blob layout must stay readable");
}

// ---------------------------------------------------------------------------
// AgentMemoryProvider capability matrix
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_agent_memory_store_passes_capability_matrix() {
    let dir = tempfile::tempdir().expect("tempdir");
    let factory = SqliteMemoryFactory {
        root: dir.path().to_path_buf(),
    };
    chapters::agent_memory(&factory)
        .await
        .expect("SqliteAgentMemoryStore (all-capable) must satisfy the capability matrix");
}

/// The basic recall/remember/forget shape remains a provider-trait contract,
/// independent of the retired markdown backend.
#[tokio::test]
async fn reference_agent_memory_store_passes_capability_matrix() {
    let factory = ReferenceMemoryFactory;
    chapters::agent_memory(&factory)
        .await
        .expect("the reference provider must satisfy the basic capability matrix");
}

// ---------------------------------------------------------------------------
// Legacy-data axis
// ---------------------------------------------------------------------------

#[tokio::test]
async fn legacy_continuity_database_opens_and_preserves_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    chapters::legacy_continuity_database(dir.path())
        .await
        .expect("a pre-ledger continuity database must open with bytes and CAS intact");
}

#[tokio::test]
async fn legacy_memory_database_upgrades_preserving_rows_and_sentinel() {
    let dir = tempfile::tempdir().expect("tempdir");
    chapters::legacy_memory_database(dir.path())
        .await
        .expect("a pre-column memory database must upgrade preserving rows and the sentinel");
}
