#![allow(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    unused_imports,
    redundant_semicolons,
    clippy::redundant_clone
)]
//! Tests for identity-first builder validation (Phase 1, Tasks 1.12–1.15).

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use meerkat_client::{LlmClient, LlmDoneOutcome, LlmError, LlmEvent, LlmRequest};
use meerkat_core::StopReason;
use meerkat_meta::identity_first::contracts::{
    AgentCustomizer, ContinuityStore, LeaseProvider, RosterProvider, TopologyProvider,
};
use meerkat_meta::identity_first::{
    AgentAddressability, AgentBuildContext, AgentBuildDraft, AgentIdentity, AgentRuntimeServices,
    BridgeDelivery, BridgeError, CheckpointVersion, ContinuityFailure, ContinuityFailureKind,
    ContinuityGeneration, ContinuityRecord, ContinuityResolveState, ContinuityStoreError,
    CustomizerError, DurabilityPolicy, DurableAgentSpec, FencingToken, IdentityBootstrapState,
    IdentityFirstRuntimeContext, IdentityLifecycleState, IdentityRuntime, IdentityRuntimeConfig,
    LeaseAcquireResult, LeaseError, LeaseGrant, LeaseRenewResult, LocalContinuityStore,
    LocalLeaseProvider, ManagedPeerEdge, MemberInspection, RestoreOutcome, ResumeSessionOutcome,
    RosterContext, RosterError, SessionBridge, SessionSnapshot, TopologyContext, TopologyError,
    restore_flow,
};
use meerkat_meta::unified_runtime::{IdentityBootstrapMode, UnifiedRuntimeBuilder};
use meerkat_meta::{
    AgentDiscoverySpec, AgentMemoryConfig, AllowAllConsoleVisibilityPolicy,
    ConsoleRuntimeRegistration, ConsoleVisibility, Discovery, JsonRpcResponse,
    MetaConsoleAggregator, NewAgentMemory, handle_unified_rpc_json,
};
use serde_json::json;

/// Per-test mob id counter: 0.8.23's fail-closed in-proc registration
/// means concurrently running tests must not share a supervisor route.
static NEXT_TEST_MOB_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

// ---------------------------------------------------------------------------
// Minimal mock implementations for builder testing
// ---------------------------------------------------------------------------

struct StubContinuityStore;

struct BrokenContinuityStore;

struct StaticDiscovery {
    specs: Vec<AgentDiscoverySpec>,
}

impl Discovery for StaticDiscovery {
    fn discover(
        &self,
        _context: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Vec<AgentDiscoverySpec>> + Send + '_>> {
        let specs = self.specs.clone();
        Box::pin(async move { specs })
    }
}

struct GatedCustomizer {
    entered: AtomicUsize,
    permits: Arc<tokio::sync::Semaphore>,
    failing_identity: Option<AgentIdentity>,
}

struct FailAfterPeerActiveCustomizer {
    runtime: Arc<IdentityRuntime>,
    peer: AgentIdentity,
    failing_identity: AgentIdentity,
}

#[derive(Default)]
struct FailOnceCustomizer {
    attempts: AtomicUsize,
}

#[async_trait]
impl AgentCustomizer for FailOnceCustomizer {
    async fn customize_build(
        &self,
        _context: &AgentBuildContext,
        _spec: &DurableAgentSpec,
        _draft: &mut AgentBuildDraft,
    ) -> Result<(), CustomizerError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(CustomizerError::BuildFailed(
                "synthetic first lazy materialization failure".to_string(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl AgentCustomizer for GatedCustomizer {
    async fn customize_build(
        &self,
        _context: &AgentBuildContext,
        spec: &DurableAgentSpec,
        _draft: &mut AgentBuildDraft,
    ) -> Result<(), CustomizerError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.permits
            .acquire()
            .await
            .expect("test semaphore remains open")
            .forget();
        if self.failing_identity.as_ref() == Some(&spec.identity) {
            return Err(CustomizerError::BuildFailed("injected warm failure".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl AgentCustomizer for FailAfterPeerActiveCustomizer {
    async fn customize_build(
        &self,
        _context: &AgentBuildContext,
        spec: &DurableAgentSpec,
        _draft: &mut AgentBuildDraft,
    ) -> Result<(), CustomizerError> {
        if spec.identity != self.failing_identity {
            return Ok(());
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.runtime.is_active(&self.peer).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            CustomizerError::BuildFailed(format!(
                "timed out waiting for {} to become active",
                self.peer
            ))
        })?;
        Err(CustomizerError::BuildFailed(
            "injected failure after peer activation".to_string(),
        ))
    }
}

#[async_trait]
impl ContinuityStore for StubContinuityStore {
    // The trait requires an explicit answer: a double that stayed
    // silent used to assert an authoritative "no record" while
    // meaning "I was never taught to answer", which is how a fix
    // goes inert with every test still green.
    async fn resolve_record_by_session(
        &self,
        _session_id: &meerkat_core::types::SessionId,
    ) -> Result<
        Option<(
            meerkat_meta::identity_first::ContinuityRecord,
            meerkat_meta::identity_first::FencingToken,
            meerkat_meta::identity_first::CheckpointVersion,
        )>,
        meerkat_meta::identity_first::ContinuityStoreError,
    > {
        Ok(None)
    }

    async fn resolve_many(
        &self,
        identities: &[AgentIdentity],
    ) -> Result<BTreeMap<AgentIdentity, ContinuityResolveState>, ContinuityStoreError> {
        Ok(identities
            .iter()
            .map(|id| (id.clone(), ContinuityResolveState::Uninitialized))
            .collect())
    }
    async fn load_session_snapshot(
        &self,
        _sid: &meerkat_core::types::SessionId,
    ) -> Result<Option<SessionSnapshot>, ContinuityStoreError> {
        Ok(None)
    }
    async fn save_session_snapshot(
        &self,
        _identity: &AgentIdentity,
        _sid: &meerkat_core::types::SessionId,
        _gen: ContinuityGeneration,
        _ver: CheckpointVersion,
        _ft: FencingToken,
        _snap: &SessionSnapshot,
    ) -> Result<(), ContinuityStoreError> {
        Ok(())
    }
    async fn upsert_continuity_record(
        &self,
        _record: &ContinuityRecord,
        _ft: FencingToken,
    ) -> Result<(), ContinuityStoreError> {
        Ok(())
    }
    async fn delete_continuity_record(
        &self,
        _identity: &AgentIdentity,
        _ft: FencingToken,
    ) -> Result<(), ContinuityStoreError> {
        Ok(())
    }
    /// Test double: deliberately whole-snapshot only.
    fn as_incremental_sessions(
        &self,
    ) -> Option<
        std::sync::Arc<dyn meerkat_meta::identity_first::contracts::ContinuityIncrementalSessions>,
    > {
        None
    }
}

#[async_trait]
impl ContinuityStore for BrokenContinuityStore {
    // The trait requires an explicit answer: a double that stayed
    // silent used to assert an authoritative "no record" while
    // meaning "I was never taught to answer", which is how a fix
    // goes inert with every test still green.
    async fn resolve_record_by_session(
        &self,
        _session_id: &meerkat_core::types::SessionId,
    ) -> Result<
        Option<(
            meerkat_meta::identity_first::ContinuityRecord,
            meerkat_meta::identity_first::FencingToken,
            meerkat_meta::identity_first::CheckpointVersion,
        )>,
        meerkat_meta::identity_first::ContinuityStoreError,
    > {
        Ok(None)
    }

    async fn resolve_many(
        &self,
        identities: &[AgentIdentity],
    ) -> Result<BTreeMap<AgentIdentity, ContinuityResolveState>, ContinuityStoreError> {
        Ok(identities
            .iter()
            .map(|identity| {
                (
                    identity.clone(),
                    ContinuityResolveState::Broken {
                        failure: ContinuityFailure {
                            identity: identity.clone(),
                            kind: ContinuityFailureKind::StoreUnavailable,
                            record: None,
                            detail: "injected broken continuity".to_string(),
                        },
                    },
                )
            })
            .collect())
    }

    async fn load_session_snapshot(
        &self,
        _sid: &meerkat_core::types::SessionId,
    ) -> Result<Option<SessionSnapshot>, ContinuityStoreError> {
        Ok(None)
    }

    async fn save_session_snapshot(
        &self,
        _identity: &AgentIdentity,
        _sid: &meerkat_core::types::SessionId,
        _gen: ContinuityGeneration,
        _ver: CheckpointVersion,
        _ft: FencingToken,
        _snap: &SessionSnapshot,
    ) -> Result<(), ContinuityStoreError> {
        Ok(())
    }

    async fn upsert_continuity_record(
        &self,
        _record: &ContinuityRecord,
        _ft: FencingToken,
    ) -> Result<(), ContinuityStoreError> {
        Ok(())
    }

    async fn delete_continuity_record(
        &self,
        _identity: &AgentIdentity,
        _ft: FencingToken,
    ) -> Result<(), ContinuityStoreError> {
        Ok(())
    }
    /// Test double: deliberately whole-snapshot only.
    fn as_incremental_sessions(
        &self,
    ) -> Option<
        std::sync::Arc<dyn meerkat_meta::identity_first::contracts::ContinuityIncrementalSessions>,
    > {
        None
    }
}

struct CountingReadyContinuityStore {
    records: BTreeMap<AgentIdentity, ContinuityRecord>,
    snapshot: Option<SessionSnapshot>,
    load_snapshot_calls: AtomicUsize,
    upsert_calls: AtomicUsize,
}

impl CountingReadyContinuityStore {
    fn new(records: BTreeMap<AgentIdentity, ContinuityRecord>) -> Self {
        Self {
            records,
            snapshot: None,
            load_snapshot_calls: AtomicUsize::new(0),
            upsert_calls: AtomicUsize::new(0),
        }
    }

    fn with_snapshot(mut self, snapshot: SessionSnapshot) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    fn load_snapshot_calls(&self) -> usize {
        self.load_snapshot_calls.load(Ordering::SeqCst)
    }

    fn upsert_calls(&self) -> usize {
        self.upsert_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ContinuityStore for CountingReadyContinuityStore {
    // The trait requires an explicit answer: a double that stayed
    // silent used to assert an authoritative "no record" while
    // meaning "I was never taught to answer", which is how a fix
    // goes inert with every test still green.
    async fn resolve_record_by_session(
        &self,
        _session_id: &meerkat_core::types::SessionId,
    ) -> Result<
        Option<(
            meerkat_meta::identity_first::ContinuityRecord,
            meerkat_meta::identity_first::FencingToken,
            meerkat_meta::identity_first::CheckpointVersion,
        )>,
        meerkat_meta::identity_first::ContinuityStoreError,
    > {
        Ok(None)
    }

    async fn resolve_many(
        &self,
        identities: &[AgentIdentity],
    ) -> Result<BTreeMap<AgentIdentity, ContinuityResolveState>, ContinuityStoreError> {
        Ok(identities
            .iter()
            .map(|id| {
                let state = self
                    .records
                    .get(id)
                    .cloned()
                    .map(|record| ContinuityResolveState::Ready { record })
                    .unwrap_or(ContinuityResolveState::Uninitialized);
                (id.clone(), state)
            })
            .collect())
    }

    async fn load_session_snapshot(
        &self,
        _sid: &meerkat_core::types::SessionId,
    ) -> Result<Option<SessionSnapshot>, ContinuityStoreError> {
        self.load_snapshot_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.snapshot.clone())
    }

    async fn save_session_snapshot(
        &self,
        _identity: &AgentIdentity,
        _sid: &meerkat_core::types::SessionId,
        _gen: ContinuityGeneration,
        _ver: CheckpointVersion,
        _ft: FencingToken,
        _snap: &SessionSnapshot,
    ) -> Result<(), ContinuityStoreError> {
        Ok(())
    }

    async fn upsert_continuity_record(
        &self,
        _record: &ContinuityRecord,
        _ft: FencingToken,
    ) -> Result<(), ContinuityStoreError> {
        self.upsert_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn delete_continuity_record(
        &self,
        _identity: &AgentIdentity,
        _ft: FencingToken,
    ) -> Result<(), ContinuityStoreError> {
        Ok(())
    }
    /// Test double: deliberately whole-snapshot only.
    fn as_incremental_sessions(
        &self,
    ) -> Option<
        std::sync::Arc<dyn meerkat_meta::identity_first::contracts::ContinuityIncrementalSessions>,
    > {
        None
    }
}

struct StubLeaseProvider;

#[derive(Default)]
struct ExactTrackingLeaseProvider {
    held: Mutex<BTreeMap<AgentIdentity, LeaseGrant>>,
    released: Mutex<Vec<LeaseGrant>>,
    renew_calls: AtomicUsize,
}

#[derive(Default)]
struct TrackingLeaseProvider {
    acquired: AtomicUsize,
    released: AtomicUsize,
}

struct FailOnceCleanupLeaseProvider {
    inner: LocalLeaseProvider,
    release_attempts: AtomicUsize,
}

impl FailOnceCleanupLeaseProvider {
    fn new() -> Self {
        Self {
            inner: LocalLeaseProvider::new(),
            release_attempts: AtomicUsize::new(0),
        }
    }

    fn release_attempts(&self) -> usize {
        self.release_attempts.load(Ordering::SeqCst)
    }
}

struct GatedCommittedRenewLeaseProvider {
    held: Mutex<BTreeMap<AgentIdentity, LeaseGrant>>,
    renew_entered: AtomicBool,
    renew_notify: tokio::sync::Notify,
    renew_permits: tokio::sync::Semaphore,
    released: Mutex<Vec<LeaseGrant>>,
}

impl ExactTrackingLeaseProvider {
    fn token_for(identity: &AgentIdentity) -> FencingToken {
        match identity.as_str() {
            "agent:alpha" => FencingToken::new(101),
            "agent:beta" => FencingToken::new(202),
            other => panic!("unexpected identity in exact lease test: {other}"),
        }
    }

    fn released(&self) -> Vec<LeaseGrant> {
        self.released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn held(&self) -> Vec<LeaseGrant> {
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }
}

impl GatedCommittedRenewLeaseProvider {
    fn new() -> Self {
        Self {
            held: Mutex::new(BTreeMap::new()),
            renew_entered: AtomicBool::new(false),
            renew_notify: tokio::sync::Notify::new(),
            renew_permits: tokio::sync::Semaphore::new(0),
            released: Mutex::new(Vec::new()),
        }
    }

    async fn wait_for_committed_renewal(&self) {
        while !self.renew_entered.load(Ordering::SeqCst) {
            self.renew_notify.notified().await;
        }
    }

    fn return_committed_renewal(&self) {
        self.renew_permits.add_permits(1);
    }

    fn released(&self) -> Vec<LeaseGrant> {
        self.released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn held(&self) -> Vec<LeaseGrant> {
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }
}

impl TrackingLeaseProvider {
    fn acquired(&self) -> usize {
        self.acquired.load(Ordering::SeqCst)
    }

    fn released(&self) -> usize {
        self.released.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl LeaseProvider for StubLeaseProvider {
    async fn acquire_leases(
        &self,
        identities: &[AgentIdentity],
        _instance: &str,
    ) -> Result<BTreeMap<AgentIdentity, LeaseAcquireResult>, LeaseError> {
        Ok(identities
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    LeaseAcquireResult::Acquired(LeaseGrant {
                        identity: id.clone(),
                        fencing_token: FencingToken::new(1),
                        ttl: Duration::from_secs(30),
                    }),
                )
            })
            .collect())
    }
    async fn renew_leases(
        &self,
        _grants: &[LeaseGrant],
    ) -> Result<BTreeMap<AgentIdentity, LeaseRenewResult>, LeaseError> {
        Ok(BTreeMap::new())
    }
    async fn release_leases(&self, _grants: &[LeaseGrant]) -> Result<(), LeaseError> {
        Ok(())
    }
}

#[async_trait]
impl LeaseProvider for ExactTrackingLeaseProvider {
    async fn acquire_leases(
        &self,
        identities: &[AgentIdentity],
        runtime_instance: &str,
    ) -> Result<BTreeMap<AgentIdentity, LeaseAcquireResult>, LeaseError> {
        let mut held = self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(identities
            .iter()
            .map(|identity| {
                let result = match held.get(identity) {
                    Some(_) => LeaseAcquireResult::AlreadyHeld {
                        identity: identity.clone(),
                        holder: runtime_instance.to_string(),
                    },
                    None => {
                        let grant = LeaseGrant {
                            identity: identity.clone(),
                            fencing_token: Self::token_for(identity),
                            ttl: Duration::from_mins(5),
                        };
                        held.insert(identity.clone(), grant.clone());
                        LeaseAcquireResult::Acquired(grant)
                    }
                };
                (identity.clone(), result)
            })
            .collect())
    }

    async fn renew_leases(
        &self,
        grants: &[LeaseGrant],
    ) -> Result<BTreeMap<AgentIdentity, LeaseRenewResult>, LeaseError> {
        self.renew_calls.fetch_add(1, Ordering::SeqCst);
        Ok(grants
            .iter()
            .map(|grant| {
                (
                    grant.identity.clone(),
                    LeaseRenewResult::Renewed(grant.clone()),
                )
            })
            .collect())
    }

    async fn release_leases(&self, grants: &[LeaseGrant]) -> Result<(), LeaseError> {
        self.released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(grants);
        let mut held = self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for grant in grants {
            if held
                .get(&grant.identity)
                .is_some_and(|current| current.fencing_token == grant.fencing_token)
            {
                held.remove(&grant.identity);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl LeaseProvider for FailOnceCleanupLeaseProvider {
    async fn acquire_leases(
        &self,
        identities: &[AgentIdentity],
        instance: &str,
    ) -> Result<BTreeMap<AgentIdentity, LeaseAcquireResult>, LeaseError> {
        self.inner.acquire_leases(identities, instance).await
    }

    async fn renew_leases(
        &self,
        grants: &[LeaseGrant],
    ) -> Result<BTreeMap<AgentIdentity, LeaseRenewResult>, LeaseError> {
        self.inner.renew_leases(grants).await
    }

    async fn release_leases(&self, grants: &[LeaseGrant]) -> Result<(), LeaseError> {
        if self.release_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(LeaseError::Io(
                "synthetic first cleanup release failure".to_string(),
            ));
        }
        self.inner.release_leases(grants).await
    }
}

#[async_trait]
impl LeaseProvider for TrackingLeaseProvider {
    async fn acquire_leases(
        &self,
        identities: &[AgentIdentity],
        _instance: &str,
    ) -> Result<BTreeMap<AgentIdentity, LeaseAcquireResult>, LeaseError> {
        self.acquired.fetch_add(identities.len(), Ordering::SeqCst);
        Ok(identities
            .iter()
            .map(|identity| {
                (
                    identity.clone(),
                    LeaseAcquireResult::Acquired(LeaseGrant {
                        identity: identity.clone(),
                        fencing_token: FencingToken::new(1),
                        ttl: Duration::from_secs(30),
                    }),
                )
            })
            .collect())
    }

    async fn renew_leases(
        &self,
        _grants: &[LeaseGrant],
    ) -> Result<BTreeMap<AgentIdentity, LeaseRenewResult>, LeaseError> {
        Ok(BTreeMap::new())
    }

    async fn release_leases(&self, grants: &[LeaseGrant]) -> Result<(), LeaseError> {
        self.released.fetch_add(grants.len(), Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl LeaseProvider for GatedCommittedRenewLeaseProvider {
    async fn acquire_leases(
        &self,
        identities: &[AgentIdentity],
        instance: &str,
    ) -> Result<BTreeMap<AgentIdentity, LeaseAcquireResult>, LeaseError> {
        let mut held = self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(identities
            .iter()
            .map(|identity| {
                let result = if held.contains_key(identity) {
                    LeaseAcquireResult::AlreadyHeld {
                        identity: identity.clone(),
                        holder: instance.to_string(),
                    }
                } else {
                    let grant = LeaseGrant {
                        identity: identity.clone(),
                        fencing_token: FencingToken::new(1),
                        ttl: Duration::from_millis(20),
                    };
                    held.insert(identity.clone(), grant.clone());
                    LeaseAcquireResult::Acquired(grant)
                };
                (identity.clone(), result)
            })
            .collect())
    }

    async fn renew_leases(
        &self,
        grants: &[LeaseGrant],
    ) -> Result<BTreeMap<AgentIdentity, LeaseRenewResult>, LeaseError> {
        let renewed = {
            let mut held = self
                .held
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            grants
                .iter()
                .map(|grant| {
                    let result = if held
                        .get(&grant.identity)
                        .is_some_and(|current| current.fencing_token == grant.fencing_token)
                    {
                        let next = LeaseGrant {
                            identity: grant.identity.clone(),
                            fencing_token: FencingToken::new(grant.fencing_token.get() + 1),
                            ttl: Duration::from_mins(5),
                        };
                        held.insert(grant.identity.clone(), next.clone());
                        LeaseRenewResult::Renewed(next)
                    } else {
                        LeaseRenewResult::Lost {
                            identity: grant.identity.clone(),
                        }
                    };
                    (grant.identity.clone(), result)
                })
                .collect::<BTreeMap<_, _>>()
        };
        self.renew_entered.store(true, Ordering::SeqCst);
        self.renew_notify.notify_waiters();
        self.renew_permits
            .acquire()
            .await
            .expect("test renewal gate remains open")
            .forget();
        Ok(renewed)
    }

    async fn release_leases(&self, grants: &[LeaseGrant]) -> Result<(), LeaseError> {
        self.released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(grants);
        let mut held = self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for grant in grants {
            if held
                .get(&grant.identity)
                .is_some_and(|current| current.fencing_token == grant.fencing_token)
            {
                held.remove(&grant.identity);
            }
        }
        Ok(())
    }
}

struct GatedUpsertContinuityStore {
    record: tokio::sync::Mutex<Option<ContinuityRecord>>,
    block_next_upsert: AtomicBool,
    upsert_entered: AtomicUsize,
    permits: Arc<tokio::sync::Semaphore>,
}

#[derive(Default)]
struct FailNextUpsertContinuityStore {
    fail_next_upsert: AtomicBool,
}

impl FailNextUpsertContinuityStore {
    fn fail_next_upsert(&self) {
        self.fail_next_upsert.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl ContinuityStore for FailNextUpsertContinuityStore {
    // The trait requires an explicit answer: a double that stayed
    // silent used to assert an authoritative "no record" while
    // meaning "I was never taught to answer", which is how a fix
    // goes inert with every test still green.
    async fn resolve_record_by_session(
        &self,
        _session_id: &meerkat_core::types::SessionId,
    ) -> Result<
        Option<(
            meerkat_meta::identity_first::ContinuityRecord,
            meerkat_meta::identity_first::FencingToken,
            meerkat_meta::identity_first::CheckpointVersion,
        )>,
        meerkat_meta::identity_first::ContinuityStoreError,
    > {
        Ok(None)
    }

    async fn resolve_many(
        &self,
        identities: &[AgentIdentity],
    ) -> Result<BTreeMap<AgentIdentity, ContinuityResolveState>, ContinuityStoreError> {
        Ok(identities
            .iter()
            .map(|identity| (identity.clone(), ContinuityResolveState::Uninitialized))
            .collect())
    }

    async fn load_session_snapshot(
        &self,
        _session_id: &meerkat_core::types::SessionId,
    ) -> Result<Option<SessionSnapshot>, ContinuityStoreError> {
        Ok(None)
    }

    async fn save_session_snapshot(
        &self,
        _identity: &AgentIdentity,
        _session_id: &meerkat_core::types::SessionId,
        _generation: ContinuityGeneration,
        _version: CheckpointVersion,
        _fencing_token: FencingToken,
        _snapshot: &SessionSnapshot,
    ) -> Result<(), ContinuityStoreError> {
        Ok(())
    }

    async fn upsert_continuity_record(
        &self,
        _record: &ContinuityRecord,
        _fencing_token: FencingToken,
    ) -> Result<(), ContinuityStoreError> {
        if self.fail_next_upsert.swap(false, Ordering::SeqCst) {
            return Err(ContinuityStoreError::Io(
                "synthetic renewal continuity publication failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn delete_continuity_record(
        &self,
        _identity: &AgentIdentity,
        _fencing_token: FencingToken,
    ) -> Result<(), ContinuityStoreError> {
        Ok(())
    }
    /// Test double: deliberately whole-snapshot only.
    fn as_incremental_sessions(
        &self,
    ) -> Option<
        std::sync::Arc<dyn meerkat_meta::identity_first::contracts::ContinuityIncrementalSessions>,
    > {
        None
    }
}

impl GatedUpsertContinuityStore {
    fn new(permits: Arc<tokio::sync::Semaphore>) -> Self {
        Self {
            record: tokio::sync::Mutex::new(None),
            block_next_upsert: AtomicBool::new(true),
            upsert_entered: AtomicUsize::new(0),
            permits,
        }
    }
}

#[async_trait]
impl ContinuityStore for GatedUpsertContinuityStore {
    // The trait requires an explicit answer: a double that stayed
    // silent used to assert an authoritative "no record" while
    // meaning "I was never taught to answer", which is how a fix
    // goes inert with every test still green.
    async fn resolve_record_by_session(
        &self,
        _session_id: &meerkat_core::types::SessionId,
    ) -> Result<
        Option<(
            meerkat_meta::identity_first::ContinuityRecord,
            meerkat_meta::identity_first::FencingToken,
            meerkat_meta::identity_first::CheckpointVersion,
        )>,
        meerkat_meta::identity_first::ContinuityStoreError,
    > {
        Ok(None)
    }

    async fn resolve_many(
        &self,
        identities: &[AgentIdentity],
    ) -> Result<BTreeMap<AgentIdentity, ContinuityResolveState>, ContinuityStoreError> {
        let record = self.record.lock().await.clone();
        Ok(identities
            .iter()
            .map(|identity| {
                let state = record
                    .as_ref()
                    .filter(|record| &record.identity == identity)
                    .cloned()
                    .map(|record| ContinuityResolveState::Ready { record })
                    .unwrap_or(ContinuityResolveState::Uninitialized);
                (identity.clone(), state)
            })
            .collect())
    }

    async fn load_session_snapshot(
        &self,
        _session_id: &meerkat_core::types::SessionId,
    ) -> Result<Option<SessionSnapshot>, ContinuityStoreError> {
        Ok(None)
    }

    async fn save_session_snapshot(
        &self,
        _identity: &AgentIdentity,
        _session_id: &meerkat_core::types::SessionId,
        _generation: ContinuityGeneration,
        _version: CheckpointVersion,
        _fencing_token: FencingToken,
        _snapshot: &SessionSnapshot,
    ) -> Result<(), ContinuityStoreError> {
        Ok(())
    }

    async fn upsert_continuity_record(
        &self,
        record: &ContinuityRecord,
        _fencing_token: FencingToken,
    ) -> Result<(), ContinuityStoreError> {
        self.upsert_entered.fetch_add(1, Ordering::SeqCst);
        if self.block_next_upsert.swap(false, Ordering::SeqCst) {
            self.permits
                .acquire()
                .await
                .expect("test gate remains open")
                .forget();
        }
        *self.record.lock().await = Some(record.clone());
        Ok(())
    }

    async fn delete_continuity_record(
        &self,
        _identity: &AgentIdentity,
        _fencing_token: FencingToken,
    ) -> Result<(), ContinuityStoreError> {
        *self.record.lock().await = None;
        Ok(())
    }
    /// Test double: deliberately whole-snapshot only.
    fn as_incremental_sessions(
        &self,
    ) -> Option<
        std::sync::Arc<dyn meerkat_meta::identity_first::contracts::ContinuityIncrementalSessions>,
    > {
        None
    }
}

/// Stub bridge whose snapshot appetite is the parameter under test.
///
/// `requires_resume_snapshot: false` models the bundled `MobSessionBridge`
/// (resume loads the durable session by id; the continuity payload is never
/// inspected). `true` models the conservative default a custom bridge gets,
/// which must still be served a real payload read.
struct ResumeSnapshotStubBridge {
    requires_resume_snapshot: bool,
    resume_snapshot: Mutex<Option<SessionSnapshot>>,
}

impl ResumeSnapshotStubBridge {
    fn new(requires_resume_snapshot: bool) -> Self {
        Self {
            requires_resume_snapshot,
            resume_snapshot: Mutex::new(None),
        }
    }

    fn resume_snapshot(&self) -> Option<SessionSnapshot> {
        self.resume_snapshot.lock().unwrap().clone()
    }
}

#[async_trait]
impl SessionBridge for ResumeSnapshotStubBridge {
    fn requires_resume_snapshot(&self) -> bool {
        self.requires_resume_snapshot
    }

    async fn create_session(
        &self,
        _identity: &AgentIdentity,
        _runtime_id: &meerkat_meta::identity_first::AgentRuntimeId,
        _spec: &DurableAgentSpec,
        _draft: &AgentBuildDraft,
        session_id: &meerkat_core::types::SessionId,
    ) -> Result<meerkat_core::types::SessionId, BridgeError> {
        Ok(session_id.clone())
    }

    async fn resume_session(
        &self,
        _identity: &AgentIdentity,
        _runtime_id: &meerkat_meta::identity_first::AgentRuntimeId,
        _spec: &DurableAgentSpec,
        _draft: &AgentBuildDraft,
        session_id: &meerkat_core::types::SessionId,
        snapshot: &SessionSnapshot,
    ) -> Result<ResumeSessionOutcome, BridgeError> {
        *self.resume_snapshot.lock().unwrap() = Some(snapshot.clone());
        Ok(ResumeSessionOutcome::Resumed {
            session_id: session_id.clone(),
        })
    }

    async fn deliver_admitted(
        &self,
        _runtime_id: &meerkat_meta::identity_first::AgentRuntimeId,
        _delivery: BridgeDelivery,
    ) -> Result<meerkat_core::types::SessionId, BridgeError> {
        Err(BridgeError::Mob("delivery not used in this test".into()))
    }

    async fn checkpoint_session(
        &self,
        _runtime_id: &meerkat_meta::identity_first::AgentRuntimeId,
        _session_id: &meerkat_core::types::SessionId,
    ) -> Result<SessionSnapshot, BridgeError> {
        Err(BridgeError::Mob("checkpoint not used in this test".into()))
    }

    async fn retire_member(
        &self,
        _runtime_id: &meerkat_meta::identity_first::AgentRuntimeId,
    ) -> Result<(), BridgeError> {
        Ok(())
    }

    async fn inspect_member(
        &self,
        _runtime_id: &meerkat_meta::identity_first::AgentRuntimeId,
    ) -> Result<MemberInspection, BridgeError> {
        Err(BridgeError::Mob("inspection not used in this test".into()))
    }
}

#[derive(Clone)]
struct StubRosterProvider {
    specs: Arc<tokio::sync::Mutex<Vec<DurableAgentSpec>>>,
}

impl StubRosterProvider {
    fn new(specs: Vec<DurableAgentSpec>) -> Self {
        Self {
            specs: Arc::new(tokio::sync::Mutex::new(specs)),
        }
    }

    async fn set(&self, specs: Vec<DurableAgentSpec>) {
        *self.specs.lock().await = specs;
    }
}

#[async_trait]
impl RosterProvider for StubRosterProvider {
    async fn roster(&self, _context: &RosterContext) -> Result<Vec<DurableAgentSpec>, RosterError> {
        Ok(self.specs.lock().await.clone())
    }
}

struct GatedRosterProvider {
    specs: Vec<DurableAgentSpec>,
    entered: AtomicUsize,
    permits: Arc<tokio::sync::Semaphore>,
}

struct FailAfterFirstRosterProvider {
    specs: Vec<DurableAgentSpec>,
    calls: AtomicUsize,
}

#[async_trait]
impl RosterProvider for FailAfterFirstRosterProvider {
    async fn roster(&self, _context: &RosterContext) -> Result<Vec<DurableAgentSpec>, RosterError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(self.specs.clone())
        } else {
            Err(RosterError::ProviderUnavailable(
                "injected reconcile failure".to_string(),
            ))
        }
    }
}

#[async_trait]
impl RosterProvider for GatedRosterProvider {
    async fn roster(&self, _context: &RosterContext) -> Result<Vec<DurableAgentSpec>, RosterError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.permits
            .acquire()
            .await
            .expect("test roster gate remains open")
            .forget();
        Ok(self.specs.clone())
    }
}

#[derive(Clone)]
struct MobDefinitionRequiredRosterProvider {
    specs: Arc<tokio::sync::Mutex<Vec<DurableAgentSpec>>>,
    missing_definition_calls: Arc<AtomicUsize>,
}

impl MobDefinitionRequiredRosterProvider {
    fn new(specs: Vec<DurableAgentSpec>) -> Self {
        Self {
            specs: Arc::new(tokio::sync::Mutex::new(specs)),
            missing_definition_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn set(&self, specs: Vec<DurableAgentSpec>) {
        *self.specs.lock().await = specs;
    }

    fn missing_definition_calls(&self) -> usize {
        self.missing_definition_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl RosterProvider for MobDefinitionRequiredRosterProvider {
    async fn roster(&self, context: &RosterContext) -> Result<Vec<DurableAgentSpec>, RosterError> {
        if context.mob_definition.is_none() {
            self.missing_definition_calls.fetch_add(1, Ordering::SeqCst);
            return Err(RosterError::ProviderUnavailable(
                "mob definition required".to_string(),
            ));
        }
        Ok(self.specs.lock().await.clone())
    }
}

struct StubTopologyProvider {
    edges: Arc<tokio::sync::Mutex<Vec<ManagedPeerEdge>>>,
}

struct GatedTopologyProvider {
    entered: AtomicUsize,
    permits: Arc<tokio::sync::Semaphore>,
}

struct SlowTurnClient {
    delay: Duration,
}

impl LlmClient for SlowTurnClient {
    fn stream<'a>(
        &'a self,
        _request: &'a LlmRequest,
    ) -> Pin<Box<dyn futures::Stream<Item = Result<LlmEvent, LlmError>> + Send + 'a>> {
        let delay = self.delay;
        Box::pin(async_stream::stream! {
            tokio::time::sleep(delay).await;
            yield Ok(LlmEvent::TextDelta {
                delta: "slow ok".to_string(),
                meta: None,
            });
            yield Ok(LlmEvent::Done {
                outcome: LlmDoneOutcome::Success {
                    stop_reason: StopReason::EndTurn,
                },
            });
        })
    }

    // meerkat 0.7: LlmClient::provider returns the typed Provider.
    fn provider(&self) -> meerkat_core::Provider {
        meerkat_core::Provider::OpenAI
    }

    fn health_check<'life0, 'async_trait>(
        &'life0 self,
    ) -> Pin<Box<dyn Future<Output = Result<(), LlmError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { Ok(()) })
    }
}

#[async_trait]
impl TopologyProvider for StubTopologyProvider {
    async fn compute_edges(
        &self,
        _target_identities: &[AgentIdentity],
        _context: &TopologyContext,
    ) -> Result<Vec<ManagedPeerEdge>, TopologyError> {
        Ok(self.edges.lock().await.clone())
    }
}

#[async_trait]
impl TopologyProvider for GatedTopologyProvider {
    async fn compute_edges(
        &self,
        _target_identities: &[AgentIdentity],
        _context: &TopologyContext,
    ) -> Result<Vec<ManagedPeerEdge>, TopologyError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.permits
            .acquire()
            .await
            .expect("test topology gate remains open")
            .forget();
        Ok(Vec::new())
    }
}

fn unique_builder_mob_id() -> String {
    format!(
        "identity-builder-test-{}",
        NEXT_TEST_MOB_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

/// Stable-id variant for tests that build the SAME mob twice (persistent
/// state reuse, continuity resume): the topology authority and continuity
/// records are keyed by mob id, so both builds must agree on it.
fn test_definition_with_id(mob_id: &str) -> meerkat_mob::MobDefinition {
    meerkat_mob::MobDefinition::from_toml(&format!(
        r#"
[mob]
id = "{mob_id}"

[profiles.default]
model = "gpt-5.5"
runtime_mode = "turn_driven"

[profiles.default.tools]
comms = true
"#
    ))
    .unwrap()
}

fn test_definition() -> meerkat_mob::MobDefinition {
    test_definition_with_id(&unique_builder_mob_id())
}

fn review_flow_definition() -> meerkat_mob::MobDefinition {
    meerkat_mob::MobDefinition::from_toml(&format!(
        r#"
[mob]
id = "identity-builder-flow-test-{}"

[profiles.default]
model = "gpt-5.5"
runtime_mode = "turn_driven"

[profiles.default.tools]
comms = true

[flows.review_cycle]
description = "Ops-shaped review flow"

[flows.review_cycle.steps.review]
role = "default"
message = "run review cycle"
"#,
        NEXT_TEST_MOB_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
    .unwrap()
}

fn domain_security_definition() -> meerkat_mob::MobDefinition {
    meerkat_mob::MobDefinition::from_toml(&format!(
        r#"
[mob]
id = "identity-builder-reset-reprofile-test-{}"

[profiles.domain]
model = "gpt-5.5"
runtime_mode = "turn_driven"

[profiles.domain.tools]
comms = true

[profiles.security]
model = "gpt-5.5"
runtime_mode = "turn_driven"

[profiles.security.tools]
comms = true
shell = true
"#,
        NEXT_TEST_MOB_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
    .unwrap()
}

fn durable_spec(identity: &str) -> DurableAgentSpec {
    durable_spec_with_profile(identity, "default")
}

fn durable_spec_with_profile(identity: &str, profile: &str) -> DurableAgentSpec {
    DurableAgentSpec {
        identity: AgentIdentity::parse(identity).unwrap(),
        profile: meerkat_mob::ProfileName::from(profile),
        addressability: AgentAddressability::Addressable,
        display_name: None,
        labels: BTreeMap::new(),
        context: None,
        additional_instructions: Vec::new(),
        initial_message: None,
        runtime_mode_override: Some(meerkat_mob::MobRuntimeMode::TurnDriven),
        backend: None,
        binding: None,
        placement: None,
    }
}

fn continuity_record(identity: &str) -> ContinuityRecord {
    ContinuityRecord {
        identity: AgentIdentity::parse(identity).unwrap(),
        agent_runtime_id: meerkat_meta::identity_first::AgentRuntimeId::parse(&format!(
            "rt:{identity}:0"
        ))
        .unwrap(),
        session_id: meerkat_core::types::SessionId::new(),
        generation: ContinuityGeneration::new(0),
        checkpoint_version: CheckpointVersion::new(1),
    }
}

/// Helper: assert builder.build() returns Err and the error message contains the given substring.
async fn assert_build_err_contains(builder: UnifiedRuntimeBuilder, expected: &str) {
    match Box::pin(builder.build()).await {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains(expected),
                "expected error containing {expected:?}, got: {msg}"
            );
        }
        Ok(_) => panic!("expected builder error containing {expected:?}, but build succeeded"),
    }
}

/// Helper: assert builder.build() returns Err and the error message does NOT contain either substring.
async fn assert_build_err_not_contains(builder: UnifiedRuntimeBuilder, not_a: &str, not_b: &str) {
    match Box::pin(builder.build()).await {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                !msg.contains(not_a) && !msg.contains(not_b),
                "expected error NOT containing {not_a:?} or {not_b:?}, got: {msg}"
            );
        }
        Ok(_) => {
            // If it somehow succeeded, that's fine for this assertion
            // (the point was that it shouldn't fail with conflicting config)
        }
    }
}

// ===========================================================================
// Task 1.12 (re-scoped by the M4 REQ-23 lift): persistent_state may coexist
// with a COMPLETE external continuity/lease pair (the external substrate
// stays the identity and session authority); the genuinely contradictory
// combos keep typed errors — half a substrate, or two path roots.
// ===========================================================================

#[tokio::test]
async fn identity_first_builder_persistent_state_rejects_half_an_external_substrate() {
    let builder = UnifiedRuntimeBuilder::default()
        .persistent_state("/tmp/test-state")
        .continuity_store(Arc::new(StubContinuityStore));
    Box::pin(assert_build_err_contains(
        builder,
        "must be supplied together",
    ))
    .await;

    let builder = UnifiedRuntimeBuilder::default()
        .persistent_state("/tmp/test-state")
        .lease_provider(Arc::new(StubLeaseProvider));
    Box::pin(assert_build_err_contains(
        builder,
        "must be supplied together",
    ))
    .await;
}

/// The REQ-23 lift: a complete external pair + persistent_state composes
/// (no conflict error); the build proceeds to the ordinary
/// missing-definition failure.
#[tokio::test]
async fn identity_first_builder_persistent_state_coexists_with_external_pair() {
    let tmp = tempfile::tempdir().unwrap();
    let builder = UnifiedRuntimeBuilder::default()
        .persistent_state(tmp.path())
        .continuity_store(Arc::new(StubContinuityStore))
        .lease_provider(Arc::new(StubLeaseProvider))
        .roster_provider(Arc::new(StubRosterProvider::new(vec![])));
    Box::pin(assert_build_err_not_contains(
        builder,
        "mutually exclusive",
        "must be supplied together",
    ))
    .await;
}

#[tokio::test]
async fn identity_first_builder_persistent_state_conflicts_with_scratch_dir() {
    let builder = UnifiedRuntimeBuilder::default()
        .persistent_state("/tmp/test-state")
        .scratch_dir("/tmp/test-scratch");
    Box::pin(assert_build_err_contains(builder, "mutually exclusive")).await;
}

// ===========================================================================
// Task 1.13: Builder external path requires all three (REQ-24)
// ===========================================================================

#[tokio::test]
async fn identity_first_builder_external_path_missing_lease_and_scratch() {
    let builder = UnifiedRuntimeBuilder::default().continuity_store(Arc::new(StubContinuityStore));
    Box::pin(assert_build_err_contains(builder, "lease_provider")).await;
}

#[tokio::test]
async fn identity_first_builder_external_path_missing_continuity_store() {
    let builder = UnifiedRuntimeBuilder::default()
        .lease_provider(Arc::new(StubLeaseProvider))
        .scratch_dir("/tmp/test-scratch");
    Box::pin(assert_build_err_contains(builder, "continuity_store")).await;
}

#[tokio::test]
async fn identity_first_builder_external_path_missing_scratch_dir() {
    let builder = UnifiedRuntimeBuilder::default()
        .continuity_store(Arc::new(StubContinuityStore))
        .lease_provider(Arc::new(StubLeaseProvider));
    Box::pin(assert_build_err_contains(builder, "scratch_dir")).await;
}

#[tokio::test]
async fn identity_first_builder_identity_first_optional_setters_require_core_providers() {
    let builder = UnifiedRuntimeBuilder::default()
        .topology_provider(Arc::new(StubTopologyProvider {
            edges: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }))
        .identity_runtime_instance_id("builder-test");
    Box::pin(assert_build_err_contains(builder, "roster_provider")).await;
}

#[tokio::test]
async fn identity_first_builder_blob_store_accepted_with_persistent_state() {
    // blob_store should be accepted alongside persistent_state without
    // triggering a conflict error. The build will fail later (missing definition).
    let tmp = tempfile::tempdir().unwrap();
    let builder = UnifiedRuntimeBuilder::default()
        .persistent_state(tmp.path())
        .blob_store(Arc::new(meerkat_store::FsBlobStore::new(
            tmp.path().join("blobs"),
        )));
    Box::pin(assert_build_err_not_contains(
        builder,
        "mutually exclusive",
        "conflicting",
    ))
    .await;
}

#[tokio::test]
async fn identity_first_builder_blob_store_accepted_with_external_path() {
    let tmp = tempfile::tempdir().unwrap();
    let builder = UnifiedRuntimeBuilder::default()
        .continuity_store(Arc::new(StubContinuityStore))
        .lease_provider(Arc::new(StubLeaseProvider))
        .roster_provider(Arc::new(StubRosterProvider::new(vec![])))
        .scratch_dir(tmp.path())
        .blob_store(Arc::new(meerkat_store::FsBlobStore::new(
            tmp.path().join("blobs"),
        )));
    Box::pin(assert_build_err_not_contains(
        builder,
        "mutually exclusive",
        "conflicting",
    ))
    .await;
}

/// Injected durable stores over an ephemeral scratch directory, with no
/// storage provider: the meerkat-level bundle (runtime, detached jobs,
/// outbox) lives in the scratch directory and is genuinely fresh, and no
/// injected store is keyed by the meerkat-level realm. The build needs no
/// `meerkat_level_realm` declaration.
#[tokio::test]
async fn injected_stores_over_scratch_boot_without_a_realm_declaration() {
    let tmp = tempfile::tempdir().unwrap();
    let scratch = tmp.path().join("identity-scratch");
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
                "agent:alpha",
            )])))
            .schedule_store(Arc::new(meerkat_schedule::MemoryScheduleStore::new()))
            .workgraph_store(Arc::new(meerkat::MemoryWorkGraphStore::new()))
            .binary_blob_store(Arc::new(
                meerkat_meta::blob_store::ObjectStoreBlobStore::memory(),
            ))
            .scratch_dir(&scratch)
            .identity_runtime_instance_id("injected-stores-scratch-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("injected stores over scratch must boot without a realm declaration");
    assert!(runtime.identity_runtime().is_some());
    assert!(
        !scratch
            .join(meerkat_meta::storage_predecessor::PREDECESSOR_REALM_ID)
            .exists(),
        "a fresh scratch installation never uses the predecessor realm"
    );
}

#[tokio::test]
async fn identity_first_builder_bootstraps_and_exposes_identity_runtime() {
    let tmp = tempfile::tempdir().unwrap();
    let roster = Arc::new(StubRosterProvider::new(vec![durable_spec("agent:alpha")]));

    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(roster)
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("builder should bootstrap identity-first runtime");

    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime should be exposed");
    let status = identity_runtime
        .status(&AgentIdentity::parse("agent:alpha").unwrap())
        .await
        .expect("identity should be active");
    assert_eq!(status.profile.unwrap().as_str(), "default");
    assert_eq!(
        status.runtime_mode,
        Some(meerkat_mob::MobRuntimeMode::TurnDriven)
    );
}

#[tokio::test]
async fn identity_first_builder_reset_reprofiles_from_current_roster_provider() {
    let tmp = tempfile::tempdir().unwrap();
    let identity = AgentIdentity::parse("domain:security").unwrap();
    let roster = Arc::new(StubRosterProvider::new(vec![durable_spec_with_profile(
        identity.as_str(),
        "domain",
    )]));

    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(domain_security_definition())
            .continuity_store(Arc::new(LocalContinuityStore::in_memory().unwrap()))
            .lease_provider(Arc::new(
                meerkat_meta::identity_first::LocalLeaseProvider::new(),
            ))
            .roster_provider(roster.clone())
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-reset-reprofile-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("builder should bootstrap identity-first runtime");

    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime should be exposed");
    let before = identity_runtime
        .status(&identity)
        .await
        .expect("identity should start active");
    assert_eq!(before.profile.unwrap().as_str(), "domain");

    roster
        .set(vec![durable_spec_with_profile(
            identity.as_str(),
            "security",
        )])
        .await;

    let reset_record = identity_runtime
        .reset(&identity)
        .await
        .expect("reset should use builder-installed roster provider");

    assert_eq!(reset_record.generation.get(), 1);
    let after = identity_runtime
        .status(&identity)
        .await
        .expect("identity should remain active after reset");
    assert_eq!(after.profile.unwrap().as_str(), "security");

    let _ = runtime.mob_handle().stop().await;
}

#[tokio::test]
async fn identity_first_builder_reset_reprofiles_with_roster_context() {
    let tmp = tempfile::tempdir().unwrap();
    let identity = AgentIdentity::parse("domain:security").unwrap();
    let roster = Arc::new(MobDefinitionRequiredRosterProvider::new(vec![
        durable_spec_with_profile(identity.as_str(), "domain"),
    ]));

    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(domain_security_definition())
            .continuity_store(Arc::new(LocalContinuityStore::in_memory().unwrap()))
            .lease_provider(Arc::new(
                meerkat_meta::identity_first::LocalLeaseProvider::new(),
            ))
            .roster_provider(roster.clone())
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-reset-reprofile-context-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("builder should bootstrap with context-sensitive roster provider");

    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime should be exposed");
    assert_eq!(
        identity_runtime
            .status(&identity)
            .await
            .expect("identity should start active")
            .profile
            .unwrap()
            .as_str(),
        "domain"
    );

    roster
        .set(vec![durable_spec_with_profile(
            identity.as_str(),
            "security",
        )])
        .await;

    identity_runtime
        .reset(&identity)
        .await
        .expect("reset should pass mob_definition to context-sensitive roster provider");

    assert_eq!(
        roster.missing_definition_calls(),
        0,
        "reset must preserve the builder/context mob_definition when reading the current roster"
    );
    assert_eq!(
        identity_runtime
            .status(&identity)
            .await
            .expect("identity should remain active after reset")
            .profile
            .unwrap()
            .as_str(),
        "security"
    );

    let _ = runtime.mob_handle().stop().await;
}

#[tokio::test]
async fn identity_first_send_does_not_park_mob_actor_until_turn_completion() {
    // The discriminator is the RATIO, not either number. A parked actor blocks
    // until the turn completes, so the test only has teeth while
    // INGRESS_BUDGET is meaningfully below TURN_DELAY. Named here, and asserted
    // below, so a later edit cannot quietly shrink the turn or grow the budget
    // until the test passes for everyone including a parked loop.
    const TURN_DELAY: Duration = Duration::from_secs(30);
    const INGRESS_BUDGET: Duration = Duration::from_secs(3);
    assert!(
        INGRESS_BUDGET.as_millis() * 5 <= TURN_DELAY.as_millis(),
        "this test discriminates only while the ingress budget is well under the \
         turn duration: a parked actor waits {TURN_DELAY:?}, so a budget of \
         {INGRESS_BUDGET:?} must stay far below it"
    );

    let tmp = tempfile::tempdir().unwrap();
    let roster = Arc::new(StubRosterProvider::new(vec![durable_spec("agent:alpha")]));

    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(roster)
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-slow-send-test")
            // The turn is deliberately far longer than the budgets below. This
            // test never waits for it to finish - it stops the mob while the
            // turn is still running - so a longer delay costs no wall clock and
            // buys margin. See the budget comment below.
            .default_llm_client(Arc::new(SlowTurnClient { delay: TURN_DELAY }))
            .build(),
    )
    .await
    .expect("builder should bootstrap identity-first runtime");

    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime should be exposed")
        .clone();
    // 3s against a 30s turn, not 250ms against a 2s turn.
    //
    // The property under test is "this acks at INGRESS rather than at turn
    // completion", and the discriminator is that it returns while the turn is
    // still running - not that it returns within any particular wall clock. A
    // parked actor would block for the full 30s, so 3s still fails it decisively
    // while tolerating an order of magnitude more scheduling noise.
    //
    // At 250ms against a 2s turn this hard-failed twice on loaded CI runners
    // (2026-08-30/31, PRs #367 and #372) with `Elapsed` - the missed-deadline
    // signature of a load flake, not of a parked loop. The binary has no retry
    // group, so each occurrence was a red build.
    tokio::time::timeout(
        INGRESS_BUDGET,
        identity_runtime.send(
            &AgentIdentity::parse("agent:alpha").unwrap(),
            &meerkat_core::ContentInput::Text("start slow turn".to_string()),
        ),
    )
    .await
    .expect("identity send should ack at ingress, not turn completion")
    .expect("identity send should be accepted");

    Box::pin(tokio::time::timeout(
        INGRESS_BUDGET,
        runtime
            .mob_handle()
            .spawn_spec(meerkat_mob::SpawnMemberSpec::from_wire(
                "default".to_string(),
                // meerkat 0.7: MemberCommsName is fail-closed; raw mob member
                // ids must be identifier-safe (no ":").
                "agent-beta".to_string(),
                Some("You are beta.".into()),
                None,
                None,
            )),
    ))
    .await
    .expect("mob actor should still process spawn while alpha turn runs")
    .expect("spawn should succeed");

    let _ = runtime.mob_handle().stop().await;
}

#[tokio::test]
async fn identity_first_builder_lazy_materialize_registers_large_ready_roster_without_hydration() {
    let tmp = tempfile::tempdir().unwrap();
    let specs = (0..1_000)
        .map(|index| durable_spec(&format!("agent:{index}")))
        .collect::<Vec<_>>();
    let records = specs
        .iter()
        .map(|spec| {
            (
                spec.identity.clone(),
                continuity_record(spec.identity.as_str()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let continuity_store = Arc::new(CountingReadyContinuityStore::new(records));

    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(continuity_store.clone())
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(Arc::new(StubRosterProvider::new(specs)))
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-lazy-test")
            .identity_bootstrap_mode(IdentityBootstrapMode::LazyMaterialize)
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("lazy builder should bootstrap identity metadata");

    assert_eq!(
        continuity_store.load_snapshot_calls(),
        0,
        "lazy build must not hydrate session snapshots"
    );
    assert_eq!(
        continuity_store.upsert_calls(),
        0,
        "lazy build must not rewrite continuity records"
    );
    assert!(
        runtime
            .mob_handle()
            .list_members_including_retiring()
            .await
            .is_empty(),
        "lazy build must not spawn/resume mob members"
    );
    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime should be exposed");
    let status = identity_runtime
        .status(&AgentIdentity::parse("agent:42").unwrap())
        .await
        .expect("dormant identity should be inspectable");
    assert_eq!(status.state, IdentityLifecycleState::Dormant);
    assert_eq!(status.profile.unwrap().as_str(), "default");
    let bootstrap = identity_runtime.identity_bootstrap_status();
    assert_eq!(bootstrap.mode, IdentityBootstrapMode::LazyMaterialize);
    assert!(bootstrap.complete);
    assert!(!bootstrap.ready);
    assert_eq!(bootstrap.counts.dormant, 1_000);
    assert_eq!(
        bootstrap.identities[&AgentIdentity::parse("agent:42").unwrap()].state,
        IdentityBootstrapState::Dormant
    );
}

/// Class 2 regression guard. The public refresh API (`refresh_desired_topology`
/// - the console add-member and reconcile path) used to force a full
/// continuity-payload read per Ready member even when the live bridge resumes
/// by session id and never looks at the payload: 14MB/60s to 94MB/180s per
/// one-word turn in production. Startup and refresh now share one restore
/// flow with one read-on-need rule, so a `requires_resume_snapshot() == false`
/// bridge costs ZERO snapshot reads on either path.
///
/// The outcome must still classify as `Resumed` - the verdict comes from the
/// bridge, not from snapshot presence - while the bridge receives the empty
/// internal argument promised by its opt-out contract.
#[tokio::test]
async fn identity_first_public_eager_refresh_elides_snapshot_load() {
    let specs = vec![durable_spec("agent:alpha")];
    let identity = specs[0].identity.clone();
    let records = BTreeMap::from([(identity.clone(), continuity_record("agent:alpha"))]);
    // Deliberately non-empty: the store HAS a payload, restore just declines
    // to read it for a bridge that declared it does not consume it.
    let unread_store_payload = SessionSnapshot {
        data: b"public-refresh-payload".to_vec(),
    };
    let continuity_store =
        Arc::new(CountingReadyContinuityStore::new(records).with_snapshot(unread_store_payload));
    let bridge = Arc::new(ResumeSnapshotStubBridge::new(false));
    let identity_runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: continuity_store.clone(),
        lease_provider: Arc::new(StubLeaseProvider),
        runtime_instance_id: "public-eager-refresh-snapshot-test".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(bridge.clone()),
        default_timeout: None,
    }));
    let context = IdentityFirstRuntimeContext::new(
        identity_runtime,
        Arc::new(StubRosterProvider::new(specs)),
        None,
        None,
        None,
    );

    let result = context
        .refresh_desired_topology()
        .await
        .expect("public eager refresh should succeed");
    assert_eq!(
        continuity_store.load_snapshot_calls(),
        0,
        "public refresh must not read the continuity payload for a bridge that \
         declared requires_resume_snapshot() == false"
    );
    assert!(
        matches!(&result.outcomes[&identity], RestoreOutcome::Resumed { .. }),
        "the bridge resume verdict must report Resumed"
    );
    assert_eq!(
        bridge.resume_snapshot(),
        Some(SessionSnapshot { data: Vec::new() }),
        "an opted-out bridge must receive an empty internal snapshot argument"
    );
}

/// The other side of the same ratchet. Eliding the payload read is keyed on
/// the bridge's declared appetite, NOT on "nobody reads it anyway": a bridge
/// that consumes the continuity payload must still be handed one on the
/// public refresh path.
#[tokio::test]
async fn identity_first_public_eager_refresh_still_loads_for_a_consuming_bridge() {
    let specs = vec![durable_spec("agent:alpha")];
    let identity = specs[0].identity.clone();
    let records = BTreeMap::from([(identity.clone(), continuity_record("agent:alpha"))]);
    let consumed_payload = SessionSnapshot {
        data: b"public-refresh-payload".to_vec(),
    };
    let continuity_store = Arc::new(
        CountingReadyContinuityStore::new(records).with_snapshot(consumed_payload.clone()),
    );
    let bridge = Arc::new(ResumeSnapshotStubBridge::new(true));
    let identity_runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: continuity_store.clone(),
        lease_provider: Arc::new(StubLeaseProvider),
        runtime_instance_id: "public-eager-refresh-consuming-bridge-test".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(bridge.clone()),
        default_timeout: None,
    }));
    let context = IdentityFirstRuntimeContext::new(
        identity_runtime,
        Arc::new(StubRosterProvider::new(specs)),
        None,
        None,
        None,
    );

    let result = context
        .refresh_desired_topology()
        .await
        .expect("public eager refresh should succeed");
    assert_eq!(
        continuity_store.load_snapshot_calls(),
        1,
        "a bridge that declared requires_resume_snapshot() == true must still be \
         served the continuity payload"
    );
    assert!(
        matches!(&result.outcomes[&identity], RestoreOutcome::Resumed { .. }),
        "the bridge resume verdict must report Resumed"
    );
    assert_eq!(
        bridge.resume_snapshot(),
        Some(consumed_payload),
        "the consuming bridge must receive the persisted snapshot bytes"
    );
}

#[tokio::test]
async fn identity_first_builder_lazy_console_lists_and_inspects_dormant_identities() {
    let tmp = tempfile::tempdir().unwrap();
    let specs = vec![durable_spec("agent:alpha"), durable_spec("agent:beta")];
    let alpha_record = continuity_record("agent:alpha");
    let expected_alpha_session_id = alpha_record.session_id.to_string();
    let beta_record = continuity_record("agent:beta");
    let records = BTreeMap::from([
        (AgentIdentity::parse("agent:alpha").unwrap(), alpha_record),
        (AgentIdentity::parse("agent:beta").unwrap(), beta_record),
    ]);
    let continuity_store = Arc::new(CountingReadyContinuityStore::new(records));

    let runtime = Arc::new(
        Box::pin(
            UnifiedRuntimeBuilder::default()
                .definition(test_definition())
                .continuity_store(continuity_store.clone())
                .lease_provider(Arc::new(StubLeaseProvider))
                .roster_provider(Arc::new(StubRosterProvider::new(specs)))
                .scratch_dir(tmp.path())
                .identity_runtime_instance_id("builder-lazy-console-test")
                .identity_bootstrap_mode(IdentityBootstrapMode::LazyMaterialize)
                .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
                .build(),
        )
        .await
        .expect("lazy builder should bootstrap identity metadata"),
    );
    let aggregator = MetaConsoleAggregator::in_memory();
    aggregator.register_runtime(ConsoleRuntimeRegistration {
        runtime_key: "runtime-a".to_string(),
        runtime: runtime.clone(),
        identity_namespace: "prod".to_string(),
        visibility_policy: Arc::new(AllowAllConsoleVisibilityPolicy),
    });

    let records = aggregator
        .list_identities()
        .await
        .expect("console identity list should work");
    let alpha = records
        .iter()
        .find(|record| record.identity == "prod/agent:alpha")
        .expect("dormant alpha should be visible");
    assert_eq!(alpha.health, "dormant");
    assert_eq!(alpha.visibility, ConsoleVisibility::Addressable);
    assert_eq!(alpha.session_id, Some(expected_alpha_session_id));
    assert!(
        runtime
            .mob_handle()
            .list_members_including_retiring()
            .await
            .is_empty(),
        "console listing must not materialize dormant identities"
    );
    assert_eq!(
        continuity_store.load_snapshot_calls(),
        0,
        "console listing must not load session snapshots"
    );

    let inspection = Box::pin(aggregator.inspect_identity("prod/agent:alpha"))
        .await
        .expect("console inspect should not fail")
        .expect("dormant alpha should be inspectable");
    assert_eq!(inspection.identity.health, "dormant");
    assert!(inspection.peers.is_empty());
    assert!(
        runtime
            .mob_handle()
            .list_members_including_retiring()
            .await
            .is_empty(),
        "console inspection must not materialize dormant identities"
    );
}

#[tokio::test]
async fn identity_first_builder_rejects_zero_background_warm_concurrency() {
    let tmp = tempfile::tempdir().unwrap();
    let builder = UnifiedRuntimeBuilder::default()
        .definition(test_definition())
        .continuity_store(Arc::new(StubContinuityStore))
        .lease_provider(Arc::new(StubLeaseProvider))
        .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
            "agent:alpha",
        )])))
        .scratch_dir(tmp.path())
        .identity_bootstrap_mode(IdentityBootstrapMode::LazyWithBackgroundWarm { concurrency: 0 })
        .default_llm_client(Arc::new(meerkat_client::TestClient::default()));

    Box::pin(assert_build_err_contains(builder, "concurrency")).await;
}

#[tokio::test]
async fn identity_first_builder_rejects_excessive_background_warm_concurrency() {
    let tmp = tempfile::tempdir().unwrap();
    let builder = UnifiedRuntimeBuilder::default()
        .definition(test_definition())
        .continuity_store(Arc::new(StubContinuityStore))
        .lease_provider(Arc::new(StubLeaseProvider))
        .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
            "agent:alpha",
        )])))
        .scratch_dir(tmp.path())
        .identity_bootstrap_mode(IdentityBootstrapMode::LazyWithBackgroundWarm { concurrency: 17 })
        .default_llm_client(Arc::new(meerkat_client::TestClient::default()));

    Box::pin(assert_build_err_contains(builder, "at most 16")).await;
}

#[tokio::test]
async fn identity_first_builder_every_explicit_mode_requires_roster_provider() {
    for mode in [
        IdentityBootstrapMode::EagerMaterialize,
        IdentityBootstrapMode::LazyMaterialize,
        IdentityBootstrapMode::LazyWithBackgroundWarm { concurrency: 2 },
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let builder = UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .persistent_state(tmp.path())
            .identity_bootstrap_mode(mode)
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()));

        Box::pin(assert_build_err_contains(builder, "roster_provider")).await;
    }
}

#[tokio::test]
async fn gateway_initializer_releases_exact_active_grant_after_partial_eager_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let mut runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .persistent_state(tmp.path().join("classic-state"))
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("classic runtime should build before gateway identity installation");
    assert!(runtime.identity_runtime().is_none());

    let specs = vec![durable_spec("agent:alpha"), durable_spec("agent:beta")];
    let alpha = specs[0].identity.clone();
    let beta = specs[1].identity.clone();
    let lease_provider = Arc::new(ExactTrackingLeaseProvider::default());
    let identity_runtime = Arc::new(
        IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(StubContinuityStore),
            lease_provider: lease_provider.clone(),
            runtime_instance_id: "gateway-partial-eager-cleanup-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: runtime.session_bridge().map(|bridge| {
                Arc::new(bridge) as Arc<dyn meerkat_meta::identity_first::bridge::SessionBridge>
            }),
            default_timeout: None,
        })
        .with_runtime_services(AgentRuntimeServices::new(runtime.mob_handle())),
    );
    let customizer = Arc::new(FailAfterPeerActiveCustomizer {
        runtime: identity_runtime.clone(),
        peer: alpha.clone(),
        failing_identity: beta.clone(),
    });
    let context = Arc::new(IdentityFirstRuntimeContext::new_with_bootstrap_mode(
        identity_runtime,
        Arc::new(StubRosterProvider::new(specs.clone())),
        None,
        Some(customizer),
        Some(runtime.mob_handle().definition().clone()),
        IdentityBootstrapMode::EagerMaterialize,
    ));

    // meerkat 0.8.22 / mobkit 0.8.16 pair (1ff4c8ca park-as-Broken): beta's
    // post-activation failure parks beta as typed Broken and can never abort
    // alpha's boot. This test now guards the per-member custody contract at
    // the park: beta's exact grant is released by the embodiment transaction
    // while alpha's authority stays held and active.
    tokio::time::timeout(
        Duration::from_secs(10),
        runtime.install_and_bootstrap_identity_first_context(context, &specs),
    )
    .await
    .expect("identity bootstrap must not hang")
    .expect("beta's failure parks Broken; it must not abort alpha's boot");

    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime installed");
    let (status, timed_out) = identity_runtime
        .wait_identity_bootstrap_terminal(Duration::from_secs(5))
        .await;
    assert!(!timed_out, "park-as-Broken bootstrap is terminal");
    assert!(status.complete);
    let alpha_entry = status.identities.get(&alpha).expect("alpha entry");
    assert_eq!(
        alpha_entry.state,
        IdentityBootstrapState::Active,
        "the sibling must stay active: {alpha_entry:?}"
    );
    let beta_entry = status.identities.get(&beta).expect("beta entry");
    assert_eq!(beta_entry.state, IdentityBootstrapState::Broken);
    assert!(
        beta_entry
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("injected failure after peer activation"),
        "beta must carry the injected failure verbatim: {beta_entry:?}"
    );

    let released = lease_provider
        .released()
        .into_iter()
        .map(|grant| (grant.identity, grant.fencing_token))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        released.get(&beta),
        Some(&FencingToken::new(202)),
        "the parked member's exact grant must be released at the park"
    );
    assert!(
        !released.contains_key(&alpha),
        "beta's park must not release the active sibling's grant"
    );
    let held = lease_provider.held();
    assert!(
        held.iter().any(|grant| grant.identity == alpha),
        "alpha's exact authority must remain held: {held:?}"
    );
    assert!(
        !held.iter().any(|grant| grant.identity == beta),
        "no ghost beta authority may remain: {held:?}"
    );

    runtime.shutdown().await;
}

#[tokio::test]
async fn identity_first_builder_eager_broken_continuity_is_terminal_without_timeout() {
    let tmp = tempfile::tempdir().unwrap();
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(BrokenContinuityStore))
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
                "agent:alpha",
            )])))
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-eager-broken-terminal-test")
            .identity_bootstrap_mode(IdentityBootstrapMode::EagerMaterialize)
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("typed Broken continuity is a terminal bootstrap result");

    let identity_runtime = runtime.identity_runtime().expect("identity runtime");
    let (status, timed_out) = identity_runtime
        .wait_identity_bootstrap_terminal(Duration::from_millis(50))
        .await;
    assert!(
        !timed_out,
        "Broken is terminal and must not wait as Dormant"
    );
    assert!(status.complete);
    assert!(!status.ready);
    assert_eq!(status.counts.broken, 1);
    let alpha = status
        .identities
        .get(&AgentIdentity::parse("agent:alpha").unwrap())
        .expect("alpha bootstrap entry");
    assert_eq!(alpha.state, IdentityBootstrapState::Broken);
    assert_eq!(alpha.error.as_deref(), Some("injected broken continuity"));

    runtime.shutdown().await;
}

#[tokio::test]
async fn identity_first_failed_builder_shutdown_retries_unpublished_lease() {
    let tmp = tempfile::tempdir().unwrap();
    let lease = Arc::new(FailOnceCleanupLeaseProvider::new());
    // Re-anchored at the meerkat 0.8.22 / mobkit 0.8.16 pair (1ff4c8ca
    // park-as-Broken): per-member failures no longer abort the build, so this
    // test now guards the FLEET-level fail-closed contract - a roster that
    // fails uniqueness validation aborts bootstrap BEFORE any exact authority
    // is acquired, and a failed build leaves no ghost lease behind. The
    // cleanup-RETRY contract this test used to carry lives at its true seam
    // in identity_first_lazy_reconcile_releases_orphan_grant_before_reacquire
    // (FailOnce release + exact release_attempts pinning).
    let result = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(lease.clone())
            .roster_provider(Arc::new(StubRosterProvider::new(vec![
                durable_spec("agent:alpha"),
                durable_spec("agent:alpha"),
            ])))
            .scratch_dir(tmp.path().join("scratch"))
            .identity_runtime_instance_id("builder-failed-memory-task-ownership-test")
            .identity_bootstrap_mode(IdentityBootstrapMode::EagerMaterialize)
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await;
    let error = match result {
        Ok(_) => panic!("a duplicate-identity roster must abort bootstrap fleet-level"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("agent:alpha"),
        "the fleet-level rejection must name the duplicated identity: {error}"
    );
    assert_eq!(
        lease.release_attempts(),
        0,
        "fleet validation fails closed before any exact authority exists to release"
    );

    let identity = AgentIdentity::parse("agent:alpha").unwrap();
    let acquired = lease
        .acquire_leases(std::slice::from_ref(&identity), "builder-failure-failover")
        .await
        .unwrap();
    assert!(
        matches!(
            acquired.get(&identity),
            Some(LeaseAcquireResult::Acquired(_))
        ),
        "failed construction must leave neither ghost tasks nor hidden lease authority: {acquired:?}"
    );
}

#[tokio::test]
async fn identity_first_failed_builder_terminates_runtime_owned_memory_supervisors() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let mob_id = unique_builder_mob_id();
    let identity = AgentIdentity::parse("agent:alpha").unwrap();
    // Re-anchored at the meerkat 0.8.22 / mobkit 0.8.16 pair (1ff4c8ca
    // park-as-Broken): a per-member customizer failure now parks Broken and
    // cannot fail the build, so the supervisor-teardown contract is exercised
    // by a FLEET-level roster-uniqueness failure that still aborts bootstrap
    // with the memory stack's runtime-owned supervisors already constructed.
    let engines = || meerkat_meta::memory_wiring::MemoryEnginesConfig {
        steward: meerkat_meta::memory::steward::StewardConfig {
            enabled: true,
            ..Default::default()
        },
        ..Default::default()
    };

    let failed = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition_with_id(&mob_id))
            .persistent_state(state.clone())
            .persistent_agent_memory_stack(AgentMemoryConfig::default(), engines())
            .roster_provider(Arc::new(StubRosterProvider::new(vec![
                durable_spec(identity.as_str()),
                durable_spec(identity.as_str()),
            ])))
            .identity_runtime_instance_id("builder-failed-memory-supervisor-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await;
    let error = match failed {
        Ok(_) => panic!("a duplicate-identity roster must abort bootstrap fleet-level"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains(identity.as_str()),
        "the fleet-level rejection must name the duplicated identity: {error}"
    );

    // The failed runtime must have aborted/joined its memory observer and
    // steward before returning Err, leaving the same durable stack reusable
    // immediately by a fresh runtime.
    let retry = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition_with_id(&mob_id))
            .persistent_state(state)
            .persistent_agent_memory_stack(AgentMemoryConfig::default(), engines())
            .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
                identity.as_str(),
            )])))
            .identity_runtime_instance_id("builder-memory-supervisor-retry-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("memory stack should be immediately reusable after failed bootstrap cleanup");
    retry.shutdown().await;
}

#[tokio::test]
async fn identity_first_builder_background_warm_is_tracked_to_active() {
    let tmp = tempfile::tempdir().unwrap();
    let specs = vec![durable_spec("agent:alpha"), durable_spec("agent:beta")];
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(Arc::new(StubRosterProvider::new(specs)))
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-background-warm-test")
            .identity_bootstrap_mode(IdentityBootstrapMode::LazyWithBackgroundWarm {
                concurrency: 2,
            })
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("background-warm builder should return after metadata registration");

    let identity_runtime = runtime.identity_runtime().expect("identity runtime");
    let (status, timed_out) = identity_runtime
        .wait_identity_bootstrap_terminal(Duration::from_secs(5))
        .await;
    assert!(!timed_out, "background warm should reach a terminal state");
    assert!(status.complete);
    assert!(
        status.ready,
        "all warm identities should be active: {status:?}"
    );
    assert_eq!(status.counts.active, 2);
    assert_eq!(status.counts.broken, 0);
    assert!(
        status
            .identities
            .values()
            .all(|entry| entry.state == IdentityBootstrapState::Active)
    );

    runtime.shutdown().await;
}

#[tokio::test]
async fn identity_first_builder_failed_pre_spawn_hook_never_starts_background_warm() {
    let tmp = tempfile::tempdir().unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let customizer = Arc::new(GatedCustomizer {
        entered: AtomicUsize::new(0),
        permits,
        failing_identity: None,
    });
    let hook: meerkat_meta::unified_runtime::PreSpawnHook = Box::new(|| {
        Box::pin(async {
            // Give an incorrectly early background warmer a deterministic
            // scheduling window before the builder fails.
            tokio::time::sleep(Duration::from_millis(50)).await;
            Err(
                Box::new(std::io::Error::other("injected pre-spawn failure"))
                    as Box<dyn std::error::Error + Send>,
            )
        })
    });

    let builder = UnifiedRuntimeBuilder::default()
        .definition(test_definition())
        .continuity_store(Arc::new(StubContinuityStore))
        .lease_provider(Arc::new(StubLeaseProvider))
        .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
            "agent:alpha",
        )])))
        .agent_customizer(customizer.clone())
        .scratch_dir(tmp.path())
        .identity_runtime_instance_id("builder-failed-hook-background-warm-test")
        .identity_bootstrap_mode(IdentityBootstrapMode::LazyWithBackgroundWarm { concurrency: 1 })
        .pre_spawn_hook(hook)
        .default_llm_client(Arc::new(meerkat_client::TestClient::default()));

    Box::pin(assert_build_err_contains(
        builder,
        "injected pre-spawn failure",
    ))
    .await;
    tokio::task::yield_now().await;
    assert_eq!(
        customizer.entered.load(Ordering::SeqCst),
        0,
        "a failed build must not leave background identity work behind"
    );
}

#[tokio::test]
async fn identity_first_failed_pre_spawn_releases_persistent_runtime_authority() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let mob_id = unique_builder_mob_id();
    let hook: meerkat_meta::unified_runtime::PreSpawnHook = Box::new(|| {
        Box::pin(async {
            Err(Box::new(std::io::Error::other(
                "injected persistent pre-spawn failure",
            )) as Box<dyn std::error::Error + Send>)
        })
    });
    let roster = Arc::new(StubRosterProvider::new(vec![durable_spec("agent:alpha")]));

    let failed = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition_with_id(&mob_id))
            .persistent_state(state.clone())
            .roster_provider(roster.clone())
            .identity_bootstrap_mode(IdentityBootstrapMode::LazyMaterialize)
            .pre_spawn_hook(hook)
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await;
    let error = match failed {
        Ok(_) => panic!("pre-spawn hook failure must fail the build"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("injected persistent pre-spawn failure")
    );

    let retry = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition_with_id(&mob_id))
            .persistent_state(state)
            .roster_provider(roster)
            .identity_bootstrap_mode(IdentityBootstrapMode::LazyMaterialize)
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("failed pre-spawn build must release persistent controller authority");
    retry.shutdown().await;
}

#[tokio::test]
async fn classic_discovery_failure_terminates_runtime_owned_memory_supervisors() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let mob_id = unique_builder_mob_id();
    let engines = || meerkat_meta::memory_wiring::MemoryEnginesConfig {
        steward: meerkat_meta::memory::steward::StewardConfig {
            enabled: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let discovery = StaticDiscovery {
        specs: vec![AgentDiscoverySpec {
            profile: "missing-profile".to_string(),
            meerkat_id: "invalid-discovery-member".to_string(),
            labels: None,
            context: None,
            additional_instructions: Vec::new(),
            resume_session_id: None,
        }],
    };

    let failed = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition_with_id(&mob_id))
            .persistent_state(state.clone())
            .persistent_agent_memory_stack(AgentMemoryConfig::default(), engines())
            .discovery(discovery)
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await;
    assert!(
        failed.is_err(),
        "invalid discovered profile must fail build"
    );

    let retry = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition_with_id(&mob_id))
            .persistent_state(state)
            .persistent_agent_memory_stack(AgentMemoryConfig::default(), engines())
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("classic discovery failure must join memory supervisors before returning");
    retry.shutdown().await;
}

#[tokio::test]
async fn durable_restore_customizers_for_different_aliases_run_concurrently() {
    let specs = vec![durable_spec("agent:alpha"), durable_spec("agent:beta")];
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let customizer = Arc::new(GatedCustomizer {
        entered: AtomicUsize::new(0),
        permits: permits.clone(),
        failing_identity: None,
    });
    let runtime = IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: Arc::new(StubContinuityStore),
        lease_provider: Arc::new(StubLeaseProvider),
        runtime_instance_id: "durable-restore-alias-concurrency-test".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: None,
        default_timeout: None,
    });

    let restore = restore_flow(&runtime, &specs, None, Some(customizer.as_ref()));
    let observe_concurrency = async {
        let observed = tokio::time::timeout(Duration::from_secs(2), async {
            while customizer.entered.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        permits.add_permits(2);
        observed
    };
    let (result, observed_concurrency) = tokio::join!(restore, observe_concurrency);

    assert!(
        observed_concurrency,
        "different durable aliases must not collapse restore customizers onto one namespace lock"
    );
    let result = result.expect("durable restore should complete");
    assert_eq!(result.outcomes.len(), 2);
}

#[tokio::test]
async fn identity_first_background_warm_bounds_concurrency_and_reports_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let specs = vec![
        durable_spec("agent:alpha"),
        durable_spec("agent:beta"),
        durable_spec("agent:gamma"),
    ];
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let customizer = Arc::new(GatedCustomizer {
        entered: AtomicUsize::new(0),
        permits: permits.clone(),
        failing_identity: Some(AgentIdentity::parse("agent:beta").unwrap()),
    });
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(Arc::new(StubRosterProvider::new(specs)))
            .agent_customizer(customizer.clone())
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-background-warm-bounded-test")
            .identity_bootstrap_mode(IdentityBootstrapMode::LazyWithBackgroundWarm {
                concurrency: 2,
            })
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("background-warm builder should return before customizers finish");

    tokio::time::timeout(Duration::from_secs(2), async {
        while customizer.entered.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("two warm operations should start");
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        customizer.entered.load(Ordering::SeqCst),
        2,
        "configured concurrency must cap simultaneous warm operations"
    );
    let warming = runtime
        .identity_runtime()
        .expect("identity runtime")
        .identity_bootstrap_status();
    assert_eq!(warming.counts.warming, 2);
    assert_eq!(warming.counts.dormant, 1);
    assert!(!warming.complete);

    permits.add_permits(3);
    let (terminal, timed_out) = runtime
        .identity_runtime()
        .unwrap()
        .wait_identity_bootstrap_terminal(Duration::from_secs(5))
        .await;
    assert!(!timed_out);
    assert!(terminal.complete);
    assert!(!terminal.ready);
    assert_eq!(terminal.counts.active, 2);
    assert_eq!(terminal.counts.broken, 1);
    let beta = &terminal.identities[&AgentIdentity::parse("agent:beta").unwrap()];
    assert_eq!(beta.state, IdentityBootstrapState::Broken);
    assert!(
        beta.error
            .as_deref()
            .is_some_and(|error| error.contains("injected warm failure"))
    );

    runtime.shutdown().await;
}

#[tokio::test]
async fn identity_first_shutdown_cancels_blocked_background_warm() {
    let tmp = tempfile::tempdir().unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let lease_provider = Arc::new(TrackingLeaseProvider::default());
    let customizer = Arc::new(GatedCustomizer {
        entered: AtomicUsize::new(0),
        permits,
        failing_identity: None,
    });
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(lease_provider.clone())
            .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
                "agent:alpha",
            )])))
            .agent_customizer(customizer.clone())
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-background-warm-shutdown-test")
            .identity_bootstrap_mode(IdentityBootstrapMode::LazyWithBackgroundWarm {
                concurrency: 1,
            })
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("background-warm builder should return while customization is blocked");

    tokio::time::timeout(Duration::from_secs(2), async {
        while customizer.entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("warm task should enter the customizer");

    tokio::time::timeout(Duration::from_secs(2), runtime.shutdown())
        .await
        .expect("shutdown must cancel and join blocked pre-session hydration");
    assert_eq!(
        lease_provider.acquired(),
        1,
        "single-embodiment ownership must precede host customization"
    );
    assert_eq!(
        lease_provider.released(),
        1,
        "a warm cancelled inside customization must release its uninstalled lease"
    );
}

#[tokio::test]
async fn identity_first_shutdown_joins_committed_renewal_before_releasing_authority() {
    let tmp = tempfile::tempdir().unwrap();
    let lease = Arc::new(GatedCommittedRenewLeaseProvider::new());
    let runtime = Arc::new(
        Box::pin(
            UnifiedRuntimeBuilder::default()
                .definition(test_definition())
                .continuity_store(Arc::new(StubContinuityStore))
                .lease_provider(lease.clone())
                .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
                    "agent:alpha",
                )])))
                .scratch_dir(tmp.path())
                .identity_runtime_instance_id("builder-renewal-shutdown-test")
                .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
                .build(),
        )
        .await
        .expect("eager builder should install the short initial lease"),
    );

    tokio::time::timeout(Duration::from_secs(2), lease.wait_for_committed_renewal())
        .await
        .expect("renewal provider should commit token 2 and gate its response");
    let shutdown = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move { runtime.shutdown().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !shutdown.is_finished(),
        "shutdown must join a provider-committed renewal instead of aborting it"
    );

    lease.return_committed_renewal();
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .expect("shutdown should finish after renewal publishes token 2")
        .expect("shutdown task should not panic");
    let released = lease.released();
    assert_eq!(released.len(), 1);
    assert_eq!(
        released[0].fencing_token,
        FencingToken::new(2),
        "final release must use the provider-committed renewed token"
    );
    assert!(
        lease.held().is_empty(),
        "exact token 2 release must clear provider authority"
    );
}

#[tokio::test]
async fn identity_first_shutdown_releases_rotated_grant_after_renewal_store_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let lease = Arc::new(GatedCommittedRenewLeaseProvider::new());
    let store = Arc::new(FailNextUpsertContinuityStore::default());
    let identity = AgentIdentity::parse("agent:alpha").unwrap();
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(store.clone())
            .lease_provider(lease.clone())
            .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
                identity.as_str(),
            )])))
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-renewal-store-failure-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("eager builder should install the short initial lease");

    tokio::time::timeout(Duration::from_secs(2), lease.wait_for_committed_renewal())
        .await
        .expect("renewal provider should commit token 2 and gate its response");
    store.fail_next_upsert();
    lease.return_committed_renewal();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if runtime
                .identity_runtime()
                .expect("identity runtime")
                .status(&identity)
                .await
                .is_ok_and(|status| status.state == IdentityLifecycleState::Broken)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failed post-renewal store publication must become Broken");

    runtime.shutdown().await;
    let released = lease.released();
    assert_eq!(released.len(), 1, "shutdown must release one exact grant");
    assert_eq!(
        released[0].fencing_token,
        FencingToken::new(2),
        "store failure must retain provider-committed token 2 for shutdown"
    );
    assert!(lease.held().is_empty());
}

#[tokio::test]
async fn identity_first_shutdown_joins_abandoned_foreground_materialization_cleanup() {
    let tmp = tempfile::tempdir().unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let lease_provider = Arc::new(TrackingLeaseProvider::default());
    let identity = AgentIdentity::parse("agent:alpha").unwrap();
    let customizer = Arc::new(GatedCustomizer {
        entered: AtomicUsize::new(0),
        permits: permits.clone(),
        failing_identity: Some(identity.clone()),
    });
    let runtime = Arc::new(
        Box::pin(
            UnifiedRuntimeBuilder::default()
                .definition(test_definition())
                .continuity_store(Arc::new(StubContinuityStore))
                .lease_provider(lease_provider.clone())
                .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
                    "agent:alpha",
                )])))
                .agent_customizer(customizer.clone())
                .scratch_dir(tmp.path())
                .identity_runtime_instance_id("builder-foreground-materialize-shutdown-test")
                .identity_bootstrap_mode(IdentityBootstrapMode::LazyMaterialize)
                .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
                .build(),
        )
        .await
        .expect("lazy builder should leave the identity dormant"),
    );

    let outer = tokio::spawn({
        let identity_runtime = runtime.identity_runtime().unwrap().clone();
        let identity = identity.clone();
        async move { identity_runtime.materialize_tracked(&identity).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while customizer.entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("foreground materialization should acquire its lease and enter customization");
    assert_eq!(lease_provider.acquired(), 1);

    // Simulate an RPC handler being dropped after the lease boundary. The
    // runtime-owned operation must outlive this result waiter.
    outer.abort();
    assert!(outer.await.unwrap_err().is_cancelled());

    tokio::time::timeout(Duration::from_secs(2), runtime.shutdown())
        .await
        .expect("shutdown must cancel and join the abandoned pre-session transaction");
    assert_eq!(
        lease_provider.released(),
        1,
        "shutdown cancellation must release the abandoned caller's uninstalled lease"
    );
}

#[tokio::test]
async fn identity_first_shutdown_joins_abandoned_reconcile_transaction() {
    let tmp = tempfile::tempdir().unwrap();
    let alpha_spec = durable_spec("agent:alpha");
    let roster = Arc::new(StubRosterProvider::new(vec![alpha_spec.clone()]));
    let permits = Arc::new(tokio::sync::Semaphore::new(1));
    let customizer = Arc::new(GatedCustomizer {
        entered: AtomicUsize::new(0),
        permits: permits.clone(),
        failing_identity: None,
    });
    let lease_provider = Arc::new(TrackingLeaseProvider::default());
    let runtime = Arc::new(
        Box::pin(
            UnifiedRuntimeBuilder::default()
                .definition(test_definition())
                .continuity_store(Arc::new(StubContinuityStore))
                .lease_provider(lease_provider.clone())
                .roster_provider(roster.clone())
                .agent_customizer(customizer.clone())
                .scratch_dir(tmp.path())
                .identity_runtime_instance_id("builder-abandoned-reconcile-shutdown-test")
                .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
                .build(),
        )
        .await
        .expect("initial eager bootstrap should consume the first customizer permit"),
    );

    // Active identities are now reconciled in place without rebuilding or
    // re-running their customizer. Add a new desired identity so this refresh
    // still exercises an abandoned runtime-owned materialization transaction.
    roster
        .set(vec![alpha_spec, durable_spec("agent:beta")])
        .await;

    let outer = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move { runtime.refresh_desired_topology().await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while customizer.entered.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reconcile should customize the new identity under its runtime-owned task");
    outer.abort();
    assert!(outer.await.unwrap_err().is_cancelled());

    let shutdown = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move { runtime.shutdown().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !shutdown.is_finished(),
        "shutdown must join, not abort, the reconcile transaction"
    );
    permits.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .expect("shutdown should finish after reconcile reaches its boundary")
        .expect("shutdown task should not panic");
    assert!(
        lease_provider.released() > 0,
        "shutdown must release the reconciled identity lease"
    );
    let status = runtime
        .identity_runtime()
        .unwrap()
        .status(&AgentIdentity::parse("agent:alpha").unwrap())
        .await
        .unwrap();
    assert_eq!(status.state, IdentityLifecycleState::Dormant);
    assert!(
        status.lease.is_none(),
        "post-shutdown status must not project an Active identity without authority"
    );
}

#[tokio::test]
async fn identity_first_concurrent_reconcile_waits_for_post_lease_warm_transaction() {
    let tmp = tempfile::tempdir().unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let continuity_store = Arc::new(GatedUpsertContinuityStore::new(permits.clone()));
    let runtime = Arc::new(
        Box::pin(
            UnifiedRuntimeBuilder::default()
                .definition(test_definition())
                .continuity_store(continuity_store.clone())
                .lease_provider(Arc::new(TrackingLeaseProvider::default()))
                .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
                    "agent:alpha",
                )])))
                .scratch_dir(tmp.path())
                .identity_runtime_instance_id("builder-background-warm-reconcile-test")
                .identity_bootstrap_mode(IdentityBootstrapMode::LazyWithBackgroundWarm {
                    concurrency: 1,
                })
                .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
                .build(),
        )
        .await
        .expect("background warm should start behind the gated continuity write"),
    );

    tokio::time::timeout(Duration::from_secs(2), async {
        while continuity_store.upsert_entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("warm transaction should cross the lease boundary");

    let first = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move { runtime.refresh_desired_topology().await }
    });
    let second = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move { runtime.refresh_desired_topology().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !first.is_finished() && !second.is_finished(),
        "reconciles must serialize behind, not abort, a post-lease materialization"
    );

    permits.add_permits(8);
    tokio::time::timeout(Duration::from_secs(5), async {
        first
            .await
            .expect("first reconcile task should not panic")
            .expect("first reconcile should succeed");
        second
            .await
            .expect("second reconcile task should not panic")
            .expect("second reconcile should succeed");
    })
    .await
    .expect("serialized reconciles should complete after the transaction gate opens");

    runtime.shutdown().await;
}

#[tokio::test]
async fn identity_first_reconcile_publishes_nonterminal_status_before_provider_wait() {
    let tmp = tempfile::tempdir().unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(1));
    let topology = Arc::new(GatedTopologyProvider {
        entered: AtomicUsize::new(0),
        permits: permits.clone(),
    });
    let runtime = Arc::new(
        Box::pin(
            UnifiedRuntimeBuilder::default()
                .definition(test_definition())
                .continuity_store(Arc::new(StubContinuityStore))
                .lease_provider(Arc::new(StubLeaseProvider))
                .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
                    "agent:alpha",
                )])))
                .topology_provider(topology.clone())
                .scratch_dir(tmp.path())
                .identity_runtime_instance_id("builder-reconcile-readiness-test")
                .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
                .build(),
        )
        .await
        .expect("initial eager bootstrap should consume the first topology permit"),
    );
    assert!(
        runtime
            .identity_runtime()
            .expect("identity runtime")
            .identity_bootstrap_status()
            .ready
    );

    let reconcile = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move { runtime.refresh_desired_topology().await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while topology.entered.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reconcile should enter the gated topology provider");

    let identity_runtime = runtime.identity_runtime().unwrap();
    let in_flight = identity_runtime.identity_bootstrap_status();
    assert!(!in_flight.complete);
    assert!(
        !in_flight.ready,
        "an in-flight pass must not retain the previous ready snapshot"
    );
    let (_, timed_out) = identity_runtime
        .wait_identity_bootstrap_terminal(Duration::from_millis(50))
        .await;
    assert!(timed_out, "the barrier must wait for the active reconcile");

    permits.add_permits(1);
    reconcile
        .await
        .expect("reconcile task should not panic")
        .expect("reconcile should succeed after the topology gate opens");
    let terminal = identity_runtime.identity_bootstrap_status();
    assert!(terminal.complete);
    assert!(terminal.ready);
    assert!(terminal.error.is_none());

    runtime.shutdown().await;
}

#[tokio::test]
async fn identity_first_superseded_warm_task_cannot_complete_new_reconcile_barrier() {
    let tmp = tempfile::tempdir().unwrap();
    let roster_permits = Arc::new(tokio::sync::Semaphore::new(1));
    let roster = Arc::new(GatedRosterProvider {
        specs: vec![durable_spec("agent:alpha")],
        entered: AtomicUsize::new(0),
        permits: roster_permits.clone(),
    });
    let customizer = Arc::new(GatedCustomizer {
        entered: AtomicUsize::new(0),
        permits: Arc::new(tokio::sync::Semaphore::new(0)),
        failing_identity: None,
    });
    let runtime = Arc::new(
        Box::pin(
            UnifiedRuntimeBuilder::default()
                .definition(test_definition())
                .continuity_store(Arc::new(StubContinuityStore))
                .lease_provider(Arc::new(TrackingLeaseProvider::default()))
                .roster_provider(roster.clone())
                .agent_customizer(customizer.clone())
                .scratch_dir(tmp.path())
                .identity_runtime_instance_id("builder-reconcile-generation-test")
                .identity_bootstrap_mode(IdentityBootstrapMode::LazyWithBackgroundWarm {
                    concurrency: 1,
                })
                .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
                .build(),
        )
        .await
        .expect("initial lazy bootstrap should start background warming"),
    );

    tokio::time::timeout(Duration::from_secs(2), async {
        while customizer.entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the original warm task should block inside customization");

    let reconcile = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move { runtime.refresh_desired_topology().await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while roster.entered.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reconcile should cancel the old warmer and reach roster discovery");

    let identity_runtime = runtime.identity_runtime().unwrap();
    let in_flight = identity_runtime.identity_bootstrap_status();
    assert!(!in_flight.complete);
    assert!(
        !in_flight.ready,
        "a superseded warm task must not complete the newer pass: {in_flight:?}"
    );
    let (_, timed_out) = identity_runtime
        .wait_identity_bootstrap_terminal(Duration::from_millis(50))
        .await;
    assert!(
        timed_out,
        "the new pass must retain ownership of its barrier"
    );

    roster_permits.add_permits(1);
    reconcile
        .await
        .expect("reconcile task should not panic")
        .expect("reconcile should succeed after roster discovery resumes");
    runtime.shutdown().await;
}

#[tokio::test]
async fn identity_first_lazy_reconcile_serializes_with_foreground_materialization() {
    let tmp = tempfile::tempdir().unwrap();
    let identity = AgentIdentity::parse("agent:alpha").unwrap();
    let original = durable_spec_with_profile(identity.as_str(), "domain");
    let mut replacement = original.clone();
    replacement.profile = meerkat_mob::ProfileName::from("security");
    replacement
        .labels
        .insert("roster_revision".to_string(), "v2".to_string());
    let roster = Arc::new(StubRosterProvider::new(vec![original]));
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let customizer = Arc::new(GatedCustomizer {
        entered: AtomicUsize::new(0),
        permits: permits.clone(),
        failing_identity: None,
    });
    let runtime = Arc::new(
        Box::pin(
            UnifiedRuntimeBuilder::default()
                .definition(domain_security_definition())
                .continuity_store(Arc::new(StubContinuityStore))
                .lease_provider(Arc::new(StubLeaseProvider))
                .roster_provider(roster.clone())
                .agent_customizer(customizer.clone())
                .scratch_dir(tmp.path())
                .identity_runtime_instance_id("builder-lazy-materialize-reconcile-race-test")
                .identity_bootstrap_mode(IdentityBootstrapMode::LazyMaterialize)
                .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
                .build(),
        )
        .await
        .expect("lazy builder should register the original spec"),
    );

    let materialize = tokio::spawn({
        let identity_runtime = runtime.identity_runtime().unwrap().clone();
        let identity = identity.clone();
        async move { identity_runtime.materialize_tracked(&identity).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while customizer.entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("foreground materialization should capture the original spec");

    roster.set(vec![replacement.clone()]).await;
    let reconcile = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move { runtime.refresh_desired_topology().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !reconcile.is_finished(),
        "reconcile must wait for the in-flight identity lifecycle transaction"
    );

    permits.add_permits(1);
    materialize
        .await
        .expect("materialize task should not panic")
        .expect("original materialization should reach its commit boundary");
    reconcile
        .await
        .expect("reconcile task should not panic")
        .expect("reconcile should retire the stale embodiment and install v2 metadata");

    let identity_runtime = runtime.identity_runtime().unwrap();
    let status = identity_runtime.status(&identity).await.unwrap();
    assert_eq!(status.state, IdentityLifecycleState::Dormant);
    assert_eq!(
        status.labels.get("roster_revision").map(String::as_str),
        Some("v2")
    );
    let bootstrap = identity_runtime.identity_bootstrap_status();
    assert!(bootstrap.complete);
    assert!(!bootstrap.ready);
    assert_eq!(
        bootstrap.identities[&identity].state,
        IdentityBootstrapState::Dormant
    );
    assert!(
        runtime
            .mob_handle()
            .list_members_including_retiring()
            .await
            .is_empty(),
        "the old physical member must be retired before v2 metadata is published"
    );

    runtime.shutdown().await;
}

/// #404 (a 2026-09-05 production incident): `retire_member` leaves the identity registered in
/// the retired terminal form (`Retiring`), and the next topology refresh no
/// longer lists it. Before the fix `apply_roster_controlled` refused with
/// `InvalidState { operation: "reconcile_roster_remove", state: Retiring }` on
/// every refresh, forever. The exact sequence must converge: the retired
/// identity is removed and the roster snapshot is ready again.
#[tokio::test]
async fn identity_first_retired_identity_is_removed_by_topology_refresh() {
    let tmp = tempfile::tempdir().unwrap();
    let alpha = AgentIdentity::parse("agent:alpha").unwrap();
    let beta = AgentIdentity::parse("agent:beta").unwrap();
    let roster = Arc::new(StubRosterProvider::new(vec![
        durable_spec(alpha.as_str()),
        durable_spec(beta.as_str()),
    ]));
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(roster.clone())
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-retired-identity-removal-test")
            .identity_bootstrap_mode(IdentityBootstrapMode::LazyWithBackgroundWarm {
                concurrency: 2,
            })
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("initial background warm should start");
    let identity_runtime = runtime.identity_runtime().unwrap();
    let (ready, timed_out) = identity_runtime
        .wait_identity_bootstrap_terminal(Duration::from_secs(5))
        .await;
    assert!(
        !timed_out && ready.ready,
        "initial roster must fully materialize"
    );

    // Step 1 (retire_member): retire beta through identity authority.
    identity_runtime
        .retire_tracked(&beta)
        .await
        .expect("retire beta");
    assert_eq!(
        identity_runtime.status(&beta).await.unwrap().state,
        IdentityLifecycleState::Retiring,
        "retire leaves the entry registered in the retired terminal form"
    );

    // Step 2 (topology refresh with the roster no longer listing beta): must
    // converge instead of failing on the Retiring entry.
    roster.set(vec![durable_spec(alpha.as_str())]).await;
    runtime
        .refresh_desired_topology()
        .await
        .expect("a retired identity must be removable by the roster reconciler (#404)");

    assert!(
        identity_runtime.status(&beta).await.is_err(),
        "the retired entry must be gone after the refresh"
    );
    let registered = identity_runtime
        .statuses()
        .await
        .into_iter()
        .map(|status| status.identity)
        .collect::<Vec<_>>();
    assert_eq!(registered, vec![alpha.clone()]);
    let bootstrap = identity_runtime.identity_bootstrap_status();
    assert!(
        bootstrap.complete && bootstrap.ready,
        "the reduced roster must report ready: {bootstrap:?}"
    );

    // A second refresh (refreshes repeat indefinitely) is a no-op, not an error.
    runtime
        .refresh_desired_topology()
        .await
        .expect("subsequent refreshes keep converging");

    runtime.shutdown().await;
}

#[tokio::test]
async fn identity_first_lazy_reconcile_retires_members_removed_from_roster() {
    let tmp = tempfile::tempdir().unwrap();
    let alpha = AgentIdentity::parse("agent:alpha").unwrap();
    let beta = AgentIdentity::parse("agent:beta").unwrap();
    let roster = Arc::new(StubRosterProvider::new(vec![
        durable_spec(alpha.as_str()),
        durable_spec(beta.as_str()),
    ]));
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(roster.clone())
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-lazy-roster-removal-test")
            .identity_bootstrap_mode(IdentityBootstrapMode::LazyWithBackgroundWarm {
                concurrency: 2,
            })
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("initial background warm should start");
    let identity_runtime = runtime.identity_runtime().unwrap();
    let (ready, timed_out) = identity_runtime
        .wait_identity_bootstrap_terminal(Duration::from_secs(5))
        .await;
    assert!(
        !timed_out && ready.ready,
        "initial roster must fully materialize"
    );

    roster.set(vec![durable_spec(alpha.as_str())]).await;
    runtime
        .refresh_desired_topology()
        .await
        .expect("reduced roster reconcile should succeed");

    let registered = identity_runtime
        .statuses()
        .await
        .into_iter()
        .map(|status| status.identity)
        .collect::<Vec<_>>();
    assert_eq!(registered, vec![alpha.clone()]);
    assert!(identity_runtime.status(&beta).await.is_err());
    let bootstrap = identity_runtime.identity_bootstrap_status();
    assert!(bootstrap.complete && bootstrap.ready);
    assert_eq!(
        bootstrap.identities.keys().cloned().collect::<Vec<_>>(),
        vec![alpha]
    );
    assert!(
        runtime
            .mob_handle()
            .list_members_including_retiring()
            .await
            .iter()
            .all(|member| !member.agent_identity.as_str().contains("beta")),
        "removed roster identity must not retain a lower-plane embodiment"
    );

    runtime.shutdown().await;
}

#[tokio::test]
async fn identity_first_lazy_reconcile_releases_orphan_grant_before_reacquire() {
    let tmp = tempfile::tempdir().unwrap();
    let identity = AgentIdentity::parse("agent:alpha").unwrap();
    let lease = Arc::new(FailOnceCleanupLeaseProvider::new());
    let customizer = Arc::new(FailOnceCustomizer::default());
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(lease.clone())
            .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
                identity.as_str(),
            )])))
            .agent_customizer(customizer.clone())
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-lazy-orphan-repair-test")
            .identity_bootstrap_mode(IdentityBootstrapMode::LazyWithBackgroundWarm {
                concurrency: 1,
            })
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("lazy builder returns before the injected warm failure");

    tokio::time::timeout(Duration::from_secs(2), async {
        while customizer.attempts.load(Ordering::SeqCst) < 1 || lease.release_attempts() < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("initial background warm must fail and park its grant");
    runtime
        .refresh_desired_topology()
        .await
        .expect("lazy reconcile must drain the orphan grant before re-registering");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let active = runtime
                .identity_runtime()
                .expect("identity runtime")
                .status(&identity)
                .await
                .is_ok_and(|status| status.state == IdentityLifecycleState::Active);
            if active
                && customizer.attempts.load(Ordering::SeqCst) >= 2
                && lease.release_attempts() >= 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("lazy reconcile must release the parked grant before reacquiring and warming");

    assert_eq!(
        lease.release_attempts(),
        2,
        "one failed cleanup plus one mode-independent reconcile retry"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn identity_bootstrap_status_tracks_reset_retire_materialize_and_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let identity = AgentIdentity::parse("agent:alpha").unwrap();
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(LocalContinuityStore::in_memory().unwrap()))
            .lease_provider(Arc::new(
                meerkat_meta::identity_first::LocalLeaseProvider::new(),
            ))
            .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
                identity.as_str(),
            )])))
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-bootstrap-lifecycle-status-test")
            .identity_bootstrap_mode(IdentityBootstrapMode::LazyWithBackgroundWarm {
                concurrency: 1,
            })
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("background-warm builder should start");
    let identity_runtime = runtime.identity_runtime().unwrap();
    let (ready, timed_out) = identity_runtime
        .wait_identity_bootstrap_terminal(Duration::from_secs(5))
        .await;
    assert!(!timed_out && ready.ready);

    identity_runtime
        .reset_tracked(&identity)
        .await
        .expect("reset should complete");
    assert_eq!(
        identity_runtime.identity_bootstrap_status().identities[&identity].state,
        IdentityBootstrapState::Active
    );
    assert!(identity_runtime.identity_bootstrap_status().ready);

    identity_runtime
        .retire_tracked(&identity)
        .await
        .expect("retire should complete");
    let retired = identity_runtime.identity_bootstrap_status();
    assert!(!retired.ready);
    assert_eq!(
        retired.identities[&identity].state,
        IdentityBootstrapState::Dormant
    );

    identity_runtime
        .respawn_tracked(&identity)
        .await
        .expect("respawn after retire should reactivate the identity");
    assert!(identity_runtime.identity_bootstrap_status().ready);

    runtime.shutdown().await;

    // Deletion has its own bootstrap-projection contract. Keep this half on a
    // never-embodied lazy identity so it does not depend on the upstream
    // MobMachine Running-authority recovery defect exercised by a rapid
    // retire -> respawn -> delete sequence.
    let delete_tmp = tempfile::tempdir().unwrap();
    let delete_identity = AgentIdentity::parse("agent:delete").unwrap();
    let delete_runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(LocalContinuityStore::in_memory().unwrap()))
            .lease_provider(Arc::new(
                meerkat_meta::identity_first::LocalLeaseProvider::new(),
            ))
            .roster_provider(Arc::new(StubRosterProvider::new(vec![durable_spec(
                delete_identity.as_str(),
            )])))
            .scratch_dir(delete_tmp.path())
            .identity_runtime_instance_id("builder-bootstrap-delete-status-test")
            .identity_bootstrap_mode(IdentityBootstrapMode::LazyMaterialize)
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("lazy builder should register the delete target without embodying it");
    let delete_identity_runtime = delete_runtime.identity_runtime().unwrap();

    delete_identity_runtime
        .delete_identity_tracked(&delete_identity)
        .await
        .expect("delete should complete");
    let deleted = delete_identity_runtime.identity_bootstrap_status();
    assert!(!deleted.ready);
    assert_eq!(
        deleted.identities[&delete_identity].state,
        IdentityBootstrapState::Dormant
    );
    assert!(
        delete_identity_runtime
            .status(&delete_identity)
            .await
            .is_err()
    );

    delete_runtime.shutdown().await;
}

#[tokio::test]
async fn identity_first_reconcile_provider_failure_is_terminal_and_not_ready() {
    let tmp = tempfile::tempdir().unwrap();
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(Arc::new(FailAfterFirstRosterProvider {
                specs: vec![durable_spec("agent:alpha")],
                calls: AtomicUsize::new(0),
            }))
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-reconcile-failure-status-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("initial bootstrap should succeed");

    let error = runtime
        .refresh_desired_topology()
        .await
        .expect_err("the second roster call should fail");
    assert!(error.to_string().contains("injected reconcile failure"));

    let status = runtime
        .identity_runtime()
        .expect("identity runtime")
        .identity_bootstrap_status();
    assert!(status.complete);
    assert!(!status.ready);
    assert!(
        status
            .error
            .as_deref()
            .is_some_and(|error| error.contains("injected reconcile failure")),
        "pass-level failure should be observable: {status:?}"
    );

    runtime.shutdown().await;
}

#[tokio::test]
async fn identity_first_builder_lazy_topology_refresh_stays_metadata_only() {
    let tmp = tempfile::tempdir().unwrap();
    let specs = vec![durable_spec("agent:a"), durable_spec("agent:b")];
    let records = specs
        .iter()
        .map(|spec| {
            (
                spec.identity.clone(),
                continuity_record(spec.identity.as_str()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let continuity_store = Arc::new(CountingReadyContinuityStore::new(records));
    let topology = Arc::new(StubTopologyProvider {
        edges: Arc::new(tokio::sync::Mutex::new(vec![
            ManagedPeerEdge::new(
                AgentIdentity::parse("agent:a").unwrap(),
                AgentIdentity::parse("agent:b").unwrap(),
            )
            .unwrap(),
        ])),
    });

    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(continuity_store.clone())
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(Arc::new(StubRosterProvider::new(specs)))
            .topology_provider(topology)
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-lazy-refresh-test")
            .identity_bootstrap_mode(IdentityBootstrapMode::LazyMaterialize)
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("lazy builder should bootstrap");

    runtime
        .refresh_desired_topology()
        .await
        .expect("lazy refresh should succeed");

    assert_eq!(
        continuity_store.load_snapshot_calls(),
        0,
        "lazy topology refresh must not hydrate snapshots"
    );
    assert!(
        runtime
            .mob_handle()
            .list_members_including_retiring()
            .await
            .is_empty(),
        "lazy topology refresh must not spawn members"
    );
    let status = runtime
        .identity_runtime()
        .unwrap()
        .status(&AgentIdentity::parse("agent:a").unwrap())
        .await
        .unwrap();
    assert_eq!(status.state, IdentityLifecycleState::Dormant);
}

#[tokio::test]
async fn identity_first_builder_lazy_run_flow_materializes_ops_shaped_roster_before_flow_start() {
    let tmp = tempfile::tempdir().unwrap();
    let specs = vec![
        durable_spec("review:singleton"),
        durable_spec("initiative:alpha"),
        durable_spec("initiative:beta"),
    ];
    let topology = Arc::new(StubTopologyProvider {
        edges: Arc::new(tokio::sync::Mutex::new(vec![
            ManagedPeerEdge::new(
                AgentIdentity::parse("review:singleton").unwrap(),
                AgentIdentity::parse("initiative:alpha").unwrap(),
            )
            .unwrap(),
            ManagedPeerEdge::new(
                AgentIdentity::parse("review:singleton").unwrap(),
                AgentIdentity::parse("initiative:beta").unwrap(),
            )
            .unwrap(),
        ])),
    });

    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(review_flow_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(Arc::new(StubRosterProvider::new(specs)))
            .topology_provider(topology)
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-lazy-flow-test")
            .identity_bootstrap_mode(IdentityBootstrapMode::LazyMaterialize)
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("lazy builder should bootstrap");

    assert!(
        runtime
            .mob_handle()
            .list_members_including_retiring()
            .await
            .is_empty(),
        "lazy build must still start without concrete members"
    );

    // Embedded applications hold the raw MobHandle. The
    // identity-first barrier must therefore be installed on that handle,
    // rather than living only in Meta's JSON-RPC wrapper.
    let run_id = runtime
        .mob_handle()
        .run_flow(
            meerkat_mob::FlowId::from("review_cycle"),
            json!({ "source": "ops" }),
        )
        .await
        .expect("direct MobHandle flow should hydrate lazy identities");

    // Returning a run id is not sufficient: the production failure returned
    // one and then remained Running forever with no admitted turn. Require the
    // flow kernel to actually record its first step transition.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let run = runtime
            .mob_handle()
            .flow_status(run_id.clone())
            .await
            .expect("flow status query")
            .expect("flow run should exist");
        if !run.step_ledger.is_empty() || !run.failure_ledger.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "flow {run_id} started but never admitted a turn: status={:?}",
            run.status
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let members = runtime.mob_handle().list_members_including_retiring().await;
    // meerkat 0.7: roster member ids are comms-safe encodings of the public
    // runtime-id aliases (MemberCommsName rejects ":"); the wire projection
    // (member_entry_to_json) decodes back to the alias space asserted below.
    let member_ids = members
        .iter()
        .map(|member| {
            meerkat_meta::mob_handle_runtime::member_entry_to_json(member)["agent_identity"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        member_ids.len(),
        3,
        "flow entrypoint must materialize the full review roster, got {member_ids:?}"
    );
    // The DURABLE identities, not the old rt:{identity}:{generation} aliases.
    // Since the stable-identity lowering the roster is keyed by the durable
    // identity and an AgentRuntimeId is per-incarnation binding detail, so a
    // roster keyed by the alias could not survive a respawn - which is the
    // defect this branch exists to fix. A fixture still naming the alias
    // asserts the contract that was replaced.
    for expected in ["review:singleton", "initiative:alpha", "initiative:beta"] {
        assert!(
            member_ids.iter().any(|member_id| member_id == expected),
            "expected materialized member {expected}, got {member_ids:?}"
        );
    }

    let identity_runtime = runtime.identity_runtime().unwrap();
    for identity in ["review:singleton", "initiative:alpha", "initiative:beta"] {
        assert_eq!(
            identity_runtime
                .status(&AgentIdentity::parse(identity).unwrap())
                .await
                .unwrap()
                .state,
            IdentityLifecycleState::Active
        );
    }
}

/// A checkpoint payload shaped like production's: the serialized session
/// document `MobSessionBridge::checkpoint_session` writes for the identity's
/// current session.
fn checkpoint_document(session_id: &meerkat_core::types::SessionId) -> SessionSnapshot {
    let mut session = meerkat_core::Session::with_id(session_id.clone());
    session.push(meerkat_core::Message::User(
        meerkat_core::UserMessage::text("explicit checkpoint".to_string()),
    ));
    SessionSnapshot {
        data: serde_json::to_vec(&session).expect("serialize checkpoint document"),
    }
}

#[tokio::test]
async fn identity_first_builder_runtime_checkpoint_follows_initial_session_save_version() {
    let tmp = tempfile::tempdir().unwrap();
    let roster = Arc::new(StubRosterProvider::new(vec![durable_spec("agent:alpha")]));
    let continuity_store = Arc::new(LocalContinuityStore::in_memory().unwrap());

    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(continuity_store)
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(roster)
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-checkpoint-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("builder should bootstrap identity-first runtime");

    let identity = AgentIdentity::parse("agent:alpha").unwrap();
    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime should be exposed");
    let status = identity_runtime
        .status(&identity)
        .await
        .expect("identity should be active");
    assert!(
        status
            .checkpoint_version
            .is_some_and(|version| version.get() >= 1),
        "initial session save should advance the live checkpoint version"
    );

    // The explicit checkpoint verb carries a session DOCUMENT, exactly as
    // `MobSessionBridge::checkpoint_session` does in production: once a
    // session is head-canonical, whole-document writes convert into delta
    // rows + a head, so opaque bytes are no longer expressible.
    let next_version = identity_runtime
        .checkpoint(
            &identity,
            &checkpoint_document(status.session_id.as_ref().expect("active session id")),
        )
        .await
        .expect("checkpoint after bootstrap should not be stale");
    assert!(next_version.get() >= 2);
}

/// WHY the Python e2e's reset checkpoint bound includes 1 (the value this
/// wiring reports since the meerkat 0.8.11 repin), traced to its writer and
/// pinned deterministically.
///
/// The writer is NOT reset's retire cleanup: post-abandon writes are
/// superseded no-ops against the OLD session key, and the identity's new
/// record has already committed by the time cleanup debt runs. The writer
/// is the CREATION-WINDOW FLUSH, whose shape at 0.8.11 is head-canonical
/// BIRTH through the adapter (the session service drives no store writes;
/// the created document reaches the continuity adapter as one committed-
/// boundary projection): the creation document carries ZERO messages at
/// its first boundary, so birth is `save_head` under a create-CAS alone -
/// the empty append is skipped - minting exactly ONE checkpoint version. A
/// freshly materialized member therefore reports checkpoint version
/// exactly 1: at first boot AND after every reset, whose replacement
/// session repeats the identical single flush with a fresh allocator.
///
/// BOUND HISTORY - this bound was re-derived twice on 2026-07-31, the next
/// reader needs the whole story:
/// - M4b era: 2. The service drove the delta channel; the initial document
///   rode as TWO parked mutations (`append_messages` = 1, adopting
///   `save_head` = 2) replayed at registration (parking mechanics remain
///   pinned unit-level in `adapters.rs`:
///   `incremental_mutations_park_before_registration_and_flush_on_register`).
/// - 2026-07-31 first re-derivation: 1, attributed to the facade's
///   whole-document projection landing as a single blob write. SUPERSEDED
///   the same day: the blob representation was itself the class-A
///   regression (heads never born), fixed by adapter-owned birth.
/// - 2026-07-31 final: 1, now via birth-with-empty-append - one `save_head`
///   under `SessionHeadCas::Create`. Same value as the superseded
///   derivation, DIFFERENT mechanism; the head-canonical representation is
///   restored and the single version is the honest creation-window count.
///
/// The e2e bound's other members stay legal for wirings where the creation
/// save lands after registration (None/0) and for the M4b-era delta flush
/// (2). If this test starts failing, the initial-document write shape
/// changed and that bound must be re-derived, not widened again.
#[tokio::test]
async fn reset_checkpoint_version_is_the_initial_document_flush_not_cleanup() {
    let tmp = tempfile::tempdir().unwrap();
    let roster = Arc::new(StubRosterProvider::new(vec![durable_spec("agent:alpha")]));
    let continuity_store = Arc::new(LocalContinuityStore::in_memory().unwrap());

    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(continuity_store.clone())
            .lease_provider(Arc::new(LocalLeaseProvider::new()))
            .roster_provider(roster)
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-reset-checkpoint-bound-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("builder should bootstrap identity-first runtime");

    let identity = AgentIdentity::parse("agent:alpha").unwrap();
    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime should be exposed");

    // First boot: the same writer with no reset anywhere near it.
    let boot = identity_runtime
        .status(&identity)
        .await
        .expect("identity should be active");
    assert_eq!(
        boot.checkpoint_version.map(CheckpointVersion::get),
        Some(1),
        "first boot must report the creation-window document at exactly 1 \
         (birth of an empty initial document = one create-CAS head write)"
    );
    let old_session = boot.session_id.clone().expect("active session id");

    let reset_record = identity_runtime
        .reset(&identity)
        .await
        .expect("reset should succeed");
    assert_eq!(reset_record.generation.get(), 1);
    assert_ne!(
        reset_record.session_id, old_session,
        "reset must mint a new session"
    );
    // The value the Python e2e observes right after reset: the replacement
    // session's own creation flush, committed by reset() before it returns
    // — cleanup debt has not even been recorded when this version is minted.
    assert_eq!(
        reset_record.checkpoint_version.get(),
        1,
        "reset must report the replacement document at exactly 1, \
         written by the creation-window birth flush"
    );
    let after = identity_runtime
        .status(&identity)
        .await
        .expect("identity should remain active after reset");
    assert_eq!(
        after.checkpoint_version.map(CheckpointVersion::get),
        Some(1),
        "status after reset must report the same creation-flush version"
    );

    // The durable record agrees with the in-memory entry, and the old
    // generation's cleanup added nothing on top.
    match ContinuityStore::resolve_many(continuity_store.as_ref(), std::slice::from_ref(&identity))
        .await
        .expect("resolve continuity record")
        .remove(&identity)
    {
        Some(ContinuityResolveState::Ready { record }) => {
            assert_eq!(record.generation.get(), 1);
            assert_eq!(
                record.checkpoint_version.get(),
                1,
                "the durable record must hold the creation-flush version, \
                 with no extra checkpoint from retire cleanup"
            );
        }
        other => panic!("expected ready continuity record after reset, got {other:?}"),
    }

    let _ = runtime.mob_handle().stop().await;
}

#[tokio::test]
async fn identity_first_builder_resume_checkpoint_follows_registered_session_save_version() {
    let continuity_store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let mob_id = unique_builder_mob_id();
    let identity = AgentIdentity::parse("agent:alpha").unwrap();

    {
        let tmp = tempfile::tempdir().unwrap();
        let roster = Arc::new(StubRosterProvider::new(vec![durable_spec("agent:alpha")]));
        let seed = Box::pin(
            UnifiedRuntimeBuilder::default()
                .definition(test_definition_with_id(&mob_id))
                .continuity_store(continuity_store.clone())
                .lease_provider(Arc::new(StubLeaseProvider))
                .roster_provider(roster)
                .scratch_dir(tmp.path())
                .identity_runtime_instance_id("builder-resume-seed")
                .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
                .build(),
        )
        .await
        .expect("seed runtime should create continuity");
        // Stand the seed down through the real lifecycle: 0.8.23's
        // fail-closed in-proc registration means the resumed runtime (same
        // mob id) cannot publish while the seed's route is still live.
        seed.shutdown().await;
    }

    let tmp = tempfile::tempdir().unwrap();
    let roster = Arc::new(StubRosterProvider::new(vec![durable_spec("agent:alpha")]));
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition_with_id(&mob_id))
            .continuity_store(continuity_store)
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(roster)
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-resume-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("builder should resume identity-first runtime");

    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime should be exposed");
    let status = identity_runtime
        .status(&identity)
        .await
        .expect("identity should be active after resume");
    let restored_version = status
        .checkpoint_version
        .expect("resume should expose a checkpoint version")
        .get();
    assert!(
        restored_version >= 1,
        "resume should inherit any registered session save version"
    );

    let next_version = identity_runtime
        .checkpoint(
            &identity,
            &checkpoint_document(status.session_id.as_ref().expect("active session id")),
        )
        .await
        .expect("checkpoint after resume should not be stale");
    assert!(next_version.get() > restored_version);
}

#[tokio::test]
async fn identity_first_builder_refreshes_desired_topology_from_providers() {
    let tmp = tempfile::tempdir().unwrap();
    let roster = Arc::new(StubRosterProvider::new(vec![
        durable_spec("agent:alpha"),
        durable_spec("agent:beta"),
    ]));
    let edges = Arc::new(tokio::sync::Mutex::new(vec![
        ManagedPeerEdge::new(
            AgentIdentity::parse("agent:alpha").unwrap(),
            AgentIdentity::parse("agent:beta").unwrap(),
        )
        .unwrap(),
    ]));

    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(roster)
            .topology_provider(Arc::new(StubTopologyProvider {
                edges: edges.clone(),
            }))
            .scratch_dir(tmp.path())
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("builder should bootstrap identity-first runtime");

    edges.lock().await.clear();
    let result = runtime
        .refresh_desired_topology()
        .await
        .expect("refresh should succeed")
        .expect("identity-first context should be present");
    assert!(result.managed_edges.is_empty());
}

// ===========================================================================
// Task 1.15: Legacy surface preservation (REQ-31)
// ===========================================================================

#[test]
fn identity_first_builder_legacy_types_still_compile() {
    // AgentDiscoverySpec is still usable
    let _spec = meerkat_meta::types::AgentDiscoverySpec {
        profile: "default".to_string(),
        meerkat_id: "agent-1".to_string(),
        labels: None,
        context: None,
        additional_instructions: vec![],
        resume_session_id: None,
    };

    // DiscoverySpec is still usable
    let _disc = meerkat_meta::types::DiscoverySpec {
        namespace: "test".to_string(),
        modules: vec![],
    };
}

#[test]
fn identity_first_builder_legacy_adapter_still_works() {
    let spec = meerkat_meta::types::AgentDiscoverySpec {
        profile: "default".to_string(),
        meerkat_id: "triage:main".to_string(),
        labels: None,
        context: None,
        additional_instructions: vec![],
        resume_session_id: Some("old-session-123".to_string()),
    };
    let durable = meerkat_meta::identity_first::agent_discovery_to_durable(&spec).unwrap();
    assert_eq!(durable.identity.as_str(), "triage:main");
}

// ===========================================================================
// Regression: external (peer-only) member restore must carry a generated
// owner binding on meerkat 0.7.1
// ===========================================================================

/// meerkat 0.7.1 `MultiBackendProvisioner::provision_member` fails external
/// peer-only members closed unless the spawn carries a generated owner
/// binding (owner bridge session + ops registry). The identity-first restore
/// path used plain `MobHandle::spawn_spec`, so real-target bootstrap (e.g.
/// the 004-mdm-console-pack real-target smoke) died with
/// "external peer-only member operation requires generated owner binding"
/// before touching the external backend at all.
///
/// This definition deliberately has no `[backend.external]`, so an
/// owner-bound spawn deterministically fails *later* — at the provisioner's
/// "external backend is not configured" check, which sits past the
/// owner-binding gate — without any network I/O.
#[tokio::test]
async fn identity_first_external_member_restore_supplies_generated_owner_binding() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spec = durable_spec("agent:target");
    spec.backend = Some(meerkat_mob::MobBackendKind::External);
    spec.binding = Some(
        serde_json::from_value(serde_json::json!({
            "kind": "external",
            "address": "tcp://127.0.0.1:4777",
            "bootstrap_token": "regression-test-token",
            "identity": {
                "kind": "ed25519_public_key",
                "public_key": "ed25519:BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc="
            }
        }))
        .expect("wire binding"),
    );
    let roster = Arc::new(StubRosterProvider::new(vec![spec]));

    // meerkat 0.8.22 / mobkit 0.8.16 pair (1ff4c8ca park-as-Broken): a
    // member-attributable restore failure parks only that identity as typed
    // Broken and can never abort the build. The invariant this test guards -
    // the spawn passes the owner-binding gate and reaches the external-backend
    // check - now surfaces in the Broken detail instead of the build error.
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(roster)
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-external-owner-binding-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("external misconfiguration is a per-member Broken park, not a build failure");

    let identity_runtime = runtime.identity_runtime().expect("identity runtime");
    let (status, timed_out) = identity_runtime
        .wait_identity_bootstrap_terminal(Duration::from_secs(5))
        .await;
    assert!(!timed_out, "Broken park is a terminal bootstrap outcome");
    assert!(status.complete);
    assert_eq!(status.counts.broken, 1);
    let target = status
        .identities
        .get(&AgentIdentity::parse("agent:target").unwrap())
        .expect("target bootstrap entry");
    assert_eq!(target.state, IdentityBootstrapState::Broken);
    let err = target.error.as_deref().unwrap_or_default();
    assert!(
        !err.contains("requires generated owner binding"),
        "identity-first restore must spawn external members with a generated owner context, got: {err}"
    );
    assert!(
        err.contains("external backend is not configured"),
        "expected the spawn to pass the owner-binding gate and reach the external-backend check, got: {err}"
    );

    runtime.shutdown().await;
}

// ===========================================================================
// M4: the composite storage-provider seam (one remote bundle)
// ===========================================================================

/// `storage_provider()` subsumes the per-slot seams: supplying both is a
/// typed conflict; it also requires an identity-first roster and a realm
/// path root.
#[tokio::test]
async fn builder_storage_provider_validation_matrix() {
    let tmp = tempfile::tempdir().unwrap();

    let builder = UnifiedRuntimeBuilder::default()
        .storage_provider(Arc::new(meerkat_meta::DiskMetaStorageProvider))
        .continuity_store(Arc::new(StubContinuityStore))
        .lease_provider(Arc::new(StubLeaseProvider))
        .roster_provider(Arc::new(StubRosterProvider::new(vec![])))
        .persistent_state(tmp.path());
    Box::pin(assert_build_err_contains(builder, "continuity_store()")).await;

    let builder = UnifiedRuntimeBuilder::default()
        .storage_provider(Arc::new(meerkat_meta::DiskMetaStorageProvider))
        .persistent_state(tmp.path());
    Box::pin(assert_build_err_contains(builder, "roster_provider")).await;

    let builder = UnifiedRuntimeBuilder::default()
        .storage_provider(Arc::new(meerkat_meta::DiskMetaStorageProvider))
        .roster_provider(Arc::new(StubRosterProvider::new(vec![])));
    Box::pin(assert_build_err_contains(builder, "path root")).await;
}

/// Injecting both typed blob forms is a conflict — one store, one form.
#[tokio::test]
async fn builder_rejects_both_blob_injection_forms() {
    let tmp = tempfile::tempdir().unwrap();
    let builder = UnifiedRuntimeBuilder::default()
        .blob_store(Arc::new(meerkat_store::FsBlobStore::new(
            tmp.path().join("blobs"),
        )))
        .binary_blob_store(Arc::new(
            meerkat_meta::blob_store::ObjectStoreBlobStore::memory(),
        ));
    Box::pin(assert_build_err_contains(builder, "binary_blob_store()")).await;
}

/// The full provider-backed build: the disk bundle supplies the identity
/// substrate and every Meta slot through the single seam, the runtime
/// boots, and the per-slot census reports the provider's stores.
#[tokio::test]
async fn builder_storage_provider_full_build_over_disk_bundle() {
    let tmp = tempfile::tempdir().unwrap();
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .storage_provider(Arc::new(meerkat_meta::DiskMetaStorageProvider))
            .persistent_state(tmp.path())
            .roster_provider(Arc::new(StubRosterProvider::new(vec![])))
            .identity_runtime_instance_id("builder-storage-provider-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("a provider-backed identity-first build must boot");

    let storage = runtime
        .resolved_storage()
        .expect("provider-backed build records the storage census");
    assert!(
        storage.slots.iter().any(|slot| {
            slot.durability.domain() == "schedule"
                && slot.backend.contains("storage provider 'disk'")
                && slot
                    .durability
                    .declaration()
                    .expect("declared slot")
                    .resolution
                    == meerkat_core::DurabilityResolution::Persistent
        }),
        "the provider's schedule declaration must be census-visible verbatim, got: {:?}",
        storage.slots
    );
    assert!(
        storage.slots.iter().any(|slot| {
            slot.durability.domain() == "sessions"
                && slot.backend.contains("ContinuitySessionStoreAdapter")
        }),
        "sessions must ride the provider's continuity store, got: {:?}",
        storage.slots
    );
    // The provider-only domains flow into the census instead of vanishing
    // after fail-closed validation.
    for domain in [
        "continuity",
        "console",
        "metadata",
        "event_log",
        "agent_memory",
    ] {
        assert!(
            storage
                .slots
                .iter()
                .any(|slot| slot.durability.domain() == domain),
            "provider-declared domain '{domain}' must be census-visible, got: {:?}",
            storage.slots
        );
    }
    // The disk provider's meerkat level IS the local composition: runtime
    // and workgraph stay on the flat local files, and no nested meerkat
    // realm is materialized under the state dir.
    assert!(
        storage.slots.iter().any(|slot| {
            slot.durability.domain() == "runtime" && slot.backend.contains("SqliteRuntimeStore")
        }),
        "the disk bundle keeps the local runtime store, got: {:?}",
        storage.slots
    );
    assert!(
        !tmp.path()
            .join(meerkat_meta::storage_provider::DEFAULT_MEERKAT_LEVEL_REALM_ID)
            .join("realm_manifest.json")
            .exists()
    );

    runtime.shutdown().await;
}

struct StubRemoteMeerkatProvider {
    opened: Arc<AtomicBool>,
    /// The realm every open was keyed by (the backend's actual selection).
    realms: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl meerkat::storage_provider::RealmStorageProvider for StubRemoteMeerkatProvider {
    fn name(&self) -> &'static str {
        "stub-bundle"
    }

    async fn open(
        &self,
        ctx: &meerkat::storage_provider::RealmOpenContext,
    ) -> Result<meerkat::storage_provider::RealmStoreSet, meerkat::PersistenceError> {
        self.opened.store(true, Ordering::SeqCst);
        self.realms
            .lock()
            .unwrap()
            .push(ctx.locator.realm.as_str().to_string());
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
                "runtime",
                "jobs",
                "schedule",
                "workgraph",
                "blobs",
                "artifacts",
            ]
            .iter()
            .map(|domain| {
                meerkat_core::DurabilityDeclaration::durable(
                    domain,
                    meerkat_core::DurabilityResolution::DeclaredEphemeral,
                )
            })
            .collect(),
        })
    }
}

struct StubBundleProvider {
    meerkat: StubRemoteMeerkatProvider,
}

#[async_trait]
impl meerkat_meta::MetaStorageProvider for StubBundleProvider {
    fn name(&self) -> &'static str {
        "stub-bundle"
    }

    async fn open_realm(
        &self,
        ctx: &meerkat_meta::MetaRealmOpenContext,
    ) -> Result<meerkat_meta::MetaRealmStoreSet, meerkat_meta::MetaStorageProviderError> {
        let set = meerkat_meta::MetaRealmStoreSet {
            continuity_store: Arc::new(StubContinuityStore),
            lease_authority: meerkat_meta::MetaLeaseAuthority::FencingFloor(0),
            event_log_store: None,
            console_log_store: Arc::new(meerkat_meta::InMemoryConsoleLogStore::new()),
            metadata_store: Arc::new(meerkat_meta::InMemoryMetadataStore::new()),
            blob_store: Arc::new(meerkat_meta::blob_store::ObjectStoreBlobStore::memory()),
            agent_memory_provider: None,
            schedule_store: Arc::new(meerkat_schedule::MemoryScheduleStore::new()),
            durability: meerkat_meta::REQUIRED_META_DURABILITY_DOMAINS
                .iter()
                .map(|domain| {
                    meerkat_core::DurabilityDeclaration::durable(
                        domain,
                        meerkat_core::DurabilityResolution::DeclaredEphemeral,
                    )
                })
                .collect(),
        };
        meerkat_meta::enforce_fail_closed_store_set(&set, ctx)?;
        Ok(set)
    }

    fn meerkat_provider(&self) -> &dyn meerkat::storage_provider::RealmStorageProvider {
        &self.meerkat
    }
}

/// M4b: a non-disk composite provider's meerkat-level bundle is genuinely
/// opened through `meerkat_provider()` — runtime and workgraph authority
/// land in the provider's backend instead of local files — and the
/// provider's durability declarations flow to the census verbatim (an
/// explicitly-ephemeral schedule slot must not be labeled persistent).
#[tokio::test]
async fn builder_storage_provider_routes_meerkat_level_bundle_and_census() {
    let tmp = tempfile::tempdir().unwrap();
    let opened = Arc::new(AtomicBool::new(false));
    let provider = Arc::new(StubBundleProvider {
        meerkat: StubRemoteMeerkatProvider {
            opened: opened.clone(),
            realms: Arc::new(Mutex::new(Vec::new())),
        },
    });
    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .storage_provider(provider)
            .meerkat_level_realm(meerkat_meta::MeerkatLevelRealmDeclaration::Fresh)
            .scratch_dir(tmp.path())
            .roster_provider(Arc::new(StubRosterProvider::new(vec![])))
            .identity_runtime_instance_id("builder-storage-provider-bundle-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("a non-disk provider-backed scratch build must boot");

    assert!(
        opened.load(Ordering::SeqCst),
        "the composition must open the provider's meerkat-level bundle"
    );
    assert!(
        tmp.path()
            .join(meerkat_meta::storage_provider::DEFAULT_MEERKAT_LEVEL_REALM_ID)
            .join("realm_manifest.json")
            .exists(),
        "the meerkat-level realm must be pinned under the provider's name"
    );

    let storage = runtime
        .resolved_storage()
        .expect("provider-backed build records the storage census");
    let slot = |domain: &str| {
        storage
            .slots
            .iter()
            .find(|slot| slot.durability.domain() == domain)
            .unwrap_or_else(|| panic!("{domain} slot missing: {:?}", storage.slots))
    };
    let schedule = slot("schedule");
    assert_eq!(
        schedule
            .durability
            .declaration()
            .expect("declared slot")
            .resolution,
        meerkat_core::DurabilityResolution::DeclaredEphemeral,
        "the provider's explicitly-ephemeral schedule declaration must flow verbatim"
    );
    assert!(schedule.backend.contains("stub-bundle"));
    assert!(
        slot("runtime").backend.contains("stub-bundle"),
        "runtime authority must ride the provider bundle, got: {:?}",
        storage.slots
    );
    assert!(
        slot("workgraph").backend.contains("stub-bundle"),
        "the workgraph must ride the provider bundle, got: {:?}",
        storage.slots
    );
    assert!(slot("continuity").backend.contains("stub-bundle"));

    runtime.shutdown().await;
}

/// One provider-backed build over `root` (scratch or persistent), with an
/// optional realm declaration. Returns the realms the backend was opened
/// under, or the build error text.
async fn build_over_bundle(
    root: &std::path::Path,
    mob_id: &str,
    persistent: bool,
    declaration: Option<meerkat_meta::MeerkatLevelRealmDeclaration>,
) -> Result<Vec<String>, String> {
    let realms = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(StubBundleProvider {
        meerkat: StubRemoteMeerkatProvider {
            opened: Arc::new(AtomicBool::new(false)),
            realms: realms.clone(),
        },
    });
    let mut builder = UnifiedRuntimeBuilder::default()
        .definition(test_definition_with_id(mob_id))
        .storage_provider(provider)
        .roster_provider(Arc::new(StubRosterProvider::new(vec![])))
        .identity_runtime_instance_id("builder-realm-declaration-test")
        .default_llm_client(Arc::new(meerkat_client::TestClient::default()));
    builder = if persistent {
        // The stub bundle's stores are in memory; the persistent composition
        // requires that choice to be explicit.
        builder.persistent_state(root).ephemeral_blobs(true)
    } else {
        builder.scratch_dir(root)
    };
    if let Some(declaration) = declaration {
        builder = builder.meerkat_level_realm(declaration);
    }
    match Box::pin(builder.build()).await {
        Ok(runtime) => {
            runtime.shutdown().await;
            Ok(realms.lock().unwrap().clone())
        }
        Err(error) => Err(error.to_string()),
    }
}

/// An adopted realm declared over unrecorded state is established (its
/// pin plus a recorded realm id) and then enforced: a later fresh
/// declaration refuses, and a later start without a declaration reopens
/// the adopted realm. Scratch and persistent roots behave alike.
#[tokio::test]
async fn an_adopted_realm_declaration_is_established_then_enforced_on_reopen() {
    use meerkat_meta::MeerkatLevelRealmDeclaration::{AdoptedPredecessor, Fresh};
    let predecessor = meerkat_meta::storage_predecessor::PREDECESSOR_REALM_ID;
    let fresh = meerkat_meta::storage_provider::DEFAULT_MEERKAT_LEVEL_REALM_ID;
    for persistent in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        // One installation keeps one mob across its restarts.
        let mob_id = unique_builder_mob_id();

        let first = build_over_bundle(&root, &mob_id, persistent, Some(AdoptedPredecessor))
            .await
            .unwrap_or_else(|error| {
                panic!("persistent={persistent}: adopted first start: {error}")
            });
        assert_eq!(
            first,
            vec![predecessor.to_string()],
            "persistent={persistent}"
        );
        assert!(root.join(predecessor).join("realm_manifest.json").exists());

        let conflicting = build_over_bundle(&root, &mob_id, persistent, Some(Fresh)).await;
        let error = conflicting.expect_err("a fresh declaration over an adopted realm refuses");
        assert!(
            error.contains("contradicts"),
            "persistent={persistent}: {error}"
        );
        assert!(!root.join(fresh).exists(), "persistent={persistent}");

        let reopened = build_over_bundle(&root, &mob_id, persistent, None)
            .await
            .unwrap_or_else(|error| panic!("persistent={persistent}: reopen: {error}"));
        assert_eq!(
            reopened,
            vec![predecessor.to_string()],
            "persistent={persistent}"
        );

        let redeclared = build_over_bundle(&root, &mob_id, persistent, Some(AdoptedPredecessor))
            .await
            .unwrap_or_else(|error| panic!("persistent={persistent}: redeclare: {error}"));
        assert_eq!(
            redeclared,
            vec![predecessor.to_string()],
            "persistent={persistent}"
        );
    }
}

/// Fresh-first: the default realm is pinned, and an adopted declaration
/// afterwards refuses.
#[tokio::test]
async fn a_fresh_realm_declaration_is_enforced_on_reopen() {
    use meerkat_meta::MeerkatLevelRealmDeclaration::{AdoptedPredecessor, Fresh};
    let fresh = meerkat_meta::storage_provider::DEFAULT_MEERKAT_LEVEL_REALM_ID;
    for persistent in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let mob_id = unique_builder_mob_id();
        let first = build_over_bundle(&root, &mob_id, persistent, Some(Fresh))
            .await
            .unwrap();
        assert_eq!(first, vec![fresh.to_string()]);
        let error = build_over_bundle(&root, &mob_id, persistent, Some(AdoptedPredecessor))
            .await
            .expect_err("an adopted declaration over a fresh pin refuses");
        assert!(error.contains("contradicts"), "{error}");
        let reopened = build_over_bundle(&root, &mob_id, persistent, None)
            .await
            .unwrap();
        assert_eq!(reopened, vec![fresh.to_string()]);
    }
}

fn activation_regression_spec(
    root: &std::path::Path,
    mob_id: &str,
) -> meerkat_meta::MobBootstrapSpec {
    std::fs::create_dir_all(root).expect("activation fixture root");
    let definition = meerkat_mob::MobDefinition::from_toml(&format!(
        r#"
[mob]
id = "{mob_id}"

[profiles.default]
model = "gpt-5.5"
runtime_mode = "turn_driven"
external_addressable = true

[profiles.default.tools]
comms = true
"#
    ))
    .expect("activation fixture definition");
    let (storage, provenance) =
        meerkat_meta::mob_composition_manifest::persistent_mob_storage(root.join("mob.sqlite3"))
            .expect("persistent mob ledger");
    let sessions = Arc::new(
        meerkat_store::SqliteSessionStore::open(root.join("sessions.sqlite3"))
            .expect("persistent sessions"),
    );
    // The persistent constructor installs the agent-tool identity slot even
    // for the classic composition, matching the gateway regression.
    meerkat_meta::MobBootstrapSpec::persistent(definition, storage, root.to_path_buf(), 8, sessions)
        .expect("persistent spec with agent mob tools")
        .with_mob_storage_provenance(provenance)
        // Creates no agent child mob. Explicit ProcessBound keeps its in-process
        // restart and complete-cleanup oracle, so this fixture does NOT test the
        // default-persistent shutdown (child-root retention until process exit).
        .with_child_mob_storage(meerkat_meta::ChildMobStorage::ProcessBound)
        .with_options(meerkat_meta::MobBootstrapOptions {
            allow_ephemeral_sessions: false,
            notify_orchestrator_on_resume: true,
            default_llm_client: Some(Arc::new(meerkat_client::TestClient::default())),
        })
}

fn activation_regression_builder(root: &std::path::Path, mob_id: &str) -> UnifiedRuntimeBuilder {
    UnifiedRuntimeBuilder::default()
        .mob_spec(activation_regression_spec(root, mob_id))
        .persistent_state(root)
        .module_config(meerkat_meta::MetaConfig {
            modules: Vec::new(),
            discovery: meerkat_meta::DiscoverySpec {
                namespace: mob_id.to_string(),
                modules: Vec::new(),
            },
            pre_spawn: Vec::new(),
        })
        .timeout(Duration::from_secs(2))
}

#[tokio::test]
async fn classic_persistent_builder_consumes_activation_before_returning_running() {
    let temp = tempfile::tempdir().expect("temp directory");
    let mob_id = unique_builder_mob_id();
    let member_id = meerkat_mob::AgentIdentity::from("activation-member");
    let first = Box::pin(activation_regression_builder(temp.path(), &mob_id).build())
        .await
        .expect("first classic bootstrap");
    assert!(first.identity_runtime().is_none());
    assert_eq!(
        first
            .mob_handle()
            .status()
            .await
            .expect("first owner state"),
        meerkat_mob::MobState::Running
    );
    first
        .mob_handle()
        .spawn_spec(meerkat_mob::SpawnMemberSpec::new(
            meerkat_mob::ProfileName::from("default"),
            member_id.clone(),
        ))
        .await
        .expect("seed persistent member");
    let session_before = first
        .mob_handle()
        .resolve_bridge_session_id(&member_id)
        .await
        .expect("seed member has a bridge session");
    let turn = first
        .mob_handle()
        .member(&member_id)
        .await
        .expect("seed member handle")
        .start_turn(
            "seed a committed classic turn",
            meerkat_core::types::HandlingMode::Queue,
            Default::default(),
            None,
        )
        .await
        .expect("first turn admission");
    tokio::time::timeout(Duration::from_secs(10), turn.wait())
        .await
        .expect("first turn finishes")
        .expect("first turn commits");
    let shutdown = first.shutdown().await;
    assert!(shutdown.cleanup_completed(), "{shutdown:?}");
    assert_eq!(
        first.mob_handle().status().await.expect("stopped owner"),
        meerkat_mob::MobState::Stopped
    );
    drop(first);

    let second = Box::pin(activation_regression_builder(temp.path(), &mob_id).build())
        .await
        .expect("second classic bootstrap");
    assert!(second.identity_runtime().is_none());
    assert_eq!(
        second
            .mob_handle()
            .status()
            .await
            .expect("second owner state"),
        meerkat_mob::MobState::Running,
        "classic composition must consume its pending activation before build returns"
    );
    assert_eq!(
        second
            .mob_handle()
            .resolve_bridge_session_id(&member_id)
            .await,
        Some(session_before.clone()),
        "cold activation must retain the persisted member-session binding"
    );
    let turn = second
        .mob_handle()
        .member(&member_id)
        .await
        .expect("restored member handle")
        .start_turn(
            "commit another classic turn after cold activation",
            meerkat_core::types::HandlingMode::Queue,
            Default::default(),
            None,
        )
        .await
        .expect("restored member admits a turn");
    assert_eq!(turn.session_id(), Some(&session_before));
    tokio::time::timeout(Duration::from_secs(10), turn.wait())
        .await
        .expect("restored turn finishes")
        .expect("restored turn commits");
    let shutdown = second.shutdown().await;
    assert!(shutdown.cleanup_completed(), "{shutdown:?}");
}

#[tokio::test]
async fn identity_persistent_builder_registers_owners_before_deferred_activation() {
    let temp = tempfile::tempdir().expect("temp directory");
    let mob_id = unique_builder_mob_id();
    let identity = AgentIdentity::parse("agent:activation").expect("identity");
    let roster = || {
        Arc::new(StubRosterProvider::new(vec![durable_spec(
            identity.as_str(),
        )]))
    };
    let first = Box::pin(
        activation_regression_builder(temp.path(), &mob_id)
            .roster_provider(roster())
            .build(),
    )
    .await
    .expect("first identity bootstrap");
    let identity_runtime = first.identity_runtime().expect("identity runtime");
    let bootstrap = identity_runtime.identity_bootstrap_status();
    assert!(
        bootstrap.ready,
        "first identity bootstrap must be ready before sending: {bootstrap:?}"
    );
    identity_runtime
        .send_awaiting_commit(
            &identity,
            &meerkat_core::ContentInput::Text("seed committed identity turn".to_string()),
        )
        .await
        .expect("first identity turn commits");
    let before = identity_runtime
        .status(&identity)
        .await
        .expect("seed identity");
    assert_eq!(before.state, IdentityLifecycleState::Active);
    assert!(before.session_id.is_some());
    let shutdown = first.shutdown().await;
    assert!(shutdown.cleanup_completed(), "{shutdown:?}");
    assert_eq!(
        first.mob_handle().status().await.expect("stopped owner"),
        meerkat_mob::MobState::Stopped
    );
    // The exported identity view is a public owner (D67): release it with
    // the runtime before a successor opens the same state.
    drop(identity_runtime);
    drop(first);

    let second = Box::pin(
        activation_regression_builder(temp.path(), &mob_id)
            .roster_provider(roster())
            .build(),
    )
    .await
    .expect("second identity bootstrap");
    assert_eq!(
        second
            .mob_handle()
            .status()
            .await
            .expect("second owner state"),
        meerkat_mob::MobState::Running
    );
    let identity_runtime = second
        .identity_runtime()
        .expect("restored identity runtime");
    let bootstrap = identity_runtime.identity_bootstrap_status();
    assert!(
        bootstrap.ready,
        "restored identity bootstrap must be ready before sending: {bootstrap:?}"
    );
    let after = identity_runtime
        .status(&identity)
        .await
        .expect("restored identity");
    assert_eq!(after.state, IdentityLifecycleState::Active);
    assert_eq!(after.session_id, before.session_id);
    assert_eq!(after.agent_runtime_id, before.agent_runtime_id);
    assert_eq!(after.generation, before.generation);
    identity_runtime
        .send_awaiting_commit(
            &identity,
            &meerkat_core::ContentInput::Text(
                "commit identity turn after cold activation".to_string(),
            ),
        )
        .await
        .expect("restored identity turn commits with its registered authority");
    let shutdown = second.shutdown().await;
    assert!(shutdown.cleanup_completed(), "{shutdown:?}");
}

/// Strict mode refuses a whole-snapshot continuity store at build time,
/// naming the concrete store, instead of starting on the whole-blob
/// fallback. Without the opt-in the same launch builds (see
/// `identity_first_builder_bootstraps_and_exposes_identity_runtime`).
#[tokio::test]
async fn strict_incremental_persistence_refuses_whole_snapshot_continuity_store() {
    let tmp = tempfile::tempdir().unwrap();
    let roster = Arc::new(StubRosterProvider::new(vec![durable_spec("agent:alpha")]));
    let result = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .continuity_store(Arc::new(StubContinuityStore))
            .lease_provider(Arc::new(StubLeaseProvider))
            .roster_provider(roster)
            .scratch_dir(tmp.path())
            .identity_runtime_instance_id("builder-test")
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .require_incremental_session_persistence(true)
            .build(),
    )
    .await;
    match result {
        Err(
            meerkat_meta::unified_runtime::UnifiedRuntimeBuilderError::SessionStoreNotIncremental {
                store_kind,
            },
        ) => {
            assert!(
                store_kind.contains("ContinuitySessionStoreAdapter over")
                    && store_kind.contains("StubContinuityStore"),
                "the refusal names the adapter and the concrete continuity store: {store_kind}"
            );
        }
        Err(other) => panic!("expected SessionStoreNotIncremental, got {other}"),
        Ok(_) => panic!("strict mode must refuse a whole-snapshot continuity store"),
    }
}
