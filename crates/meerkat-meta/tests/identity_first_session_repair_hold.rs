//! The typed "session needs repair" hold (a downstream app's 2026-09-22 wedge).
//!
//! meerkat 0.8.41 types the WholeBlob audited-endpoint divergence
//! (`SessionError::WholeBlobAuditedEndpointDivergence`, resume hold
//! `audited_endpoint_divergence`): the committed document keeps every message,
//! but every read refuses until the operator runs the sanctioned
//! `rkat session repair-wholeblob` repair. Before this hold, Meta classified
//! the refusal as a retryable resume rejection: the identity Broke, the
//! continuity repair supervisor re-ran recovery and the resume on a timer, and
//! every send reported reload-required with no operator path anywhere.
//!
//! These tests drive the Meta seam (`SessionBridge`) with a fake bridge that
//! reports the typed rejection exactly as `MobSessionBridge` classifies
//! meerkat-mob's typed error (pinned separately by the classifier unit tests
//! in `identity_first::bridge`), and assert the contract end to end:
//!
//! 1. the FIRST refusal parks the identity Broken with a
//!    [`SessionRepairRequired`] hold naming the session and the two commands;
//! 2. nothing retries against the hold: no heal call, no reconcile churn, no
//!    second resume, and reconcile keeps the Broken projection typed
//!    `RepairRequired`;
//! 3. the hold is visible on `status`, `member_health`, and the typed
//!    `EmbodimentRejected` error's structured data;
//! 4. `reload_member` is the way back: after the operator's repair it resumes
//!    the SAME session and generation with no extra flag and clears the hold;
//!    a document still refused re-parks the identity typed.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use meerkat_core::service::DurableResumeHold;
use meerkat_meta::identity_first::contracts::{ContinuityStore, LeaseProvider};
use meerkat_meta::identity_first::orchestrator::{RestoreOutcome, lazy_register_flow};
use meerkat_meta::identity_first::{
    AgentAddressability, AgentBuildDraft, AgentIdentity, AgentRuntimeId, BridgeDelivery,
    BridgeError, BridgeMemberReload, CheckpointVersion, CommittedBoundaryRepair,
    ContinuityFailureKind, ContinuityGeneration, ContinuityRecord, ContinuityRepairPolicy,
    DurabilityPolicy, DurableAgentSpec, FencingToken, IdentityFirstRuntimeContext,
    IdentityLifecycleState, IdentityRuntime, IdentityRuntimeConfig, IdentityRuntimeError,
    LeaseGrant, LocalContinuityStore, LocalLeaseProvider, MemberReloadDisposition,
    ReloadAttemptOutcome, ResumeRejectionKind, ResumeSessionOutcome, RosterContext, RosterError,
    RosterProvider, SessionBridge, SessionRepairRequired, SessionRepairScope, SessionSnapshot,
};

const IDENTITY: &str = "domain:security";

fn identity() -> AgentIdentity {
    AgentIdentity::parse(IDENTITY).unwrap()
}

fn spec() -> DurableAgentSpec {
    DurableAgentSpec {
        identity: identity(),
        profile: meerkat_mob::ProfileName::from("default"),
        addressability: AgentAddressability::Addressable,
        display_name: None,
        labels: BTreeMap::new(),
        context: None,
        additional_instructions: Vec::new(),
        initial_message: None,
        runtime_mode_override: None,
        backend: None,
        binding: None,
        placement: None,
    }
}

fn record(generation: u64) -> ContinuityRecord {
    ContinuityRecord {
        identity: identity(),
        agent_runtime_id: AgentRuntimeId::parse(&format!("rt:{IDENTITY}:{generation}")).unwrap(),
        session_id: meerkat_core::types::SessionId::new(),
        generation: ContinuityGeneration::new(generation),
        checkpoint_version: CheckpointVersion::new(0),
    }
}

fn grant(token: u64) -> LeaseGrant {
    LeaseGrant {
        identity: identity(),
        fencing_token: FencingToken::new(token),
        ttl: Duration::from_mins(5),
    }
}

fn divergence_detail(session_id: &meerkat_core::types::SessionId) -> String {
    format!(
        "resume spawn: session {session_id} has a committed WholeBlob document whose live \
         transcript does not preserve its graph-proved audited endpoint; it is preserved \
         intact and needs the sanctioned audited-endpoint repair before it can be resumed"
    )
}

/// A bridge that reports meerkat's typed audited-endpoint divergence the way
/// `MobSessionBridge` classifies it, for as many resumes as scripted, then
/// resumes normally (the operator repaired the document in between).
#[derive(Default)]
struct RepairBridge {
    resume_calls: AtomicUsize,
    retire_calls: AtomicUsize,
    recover_calls: AtomicUsize,
    reload_registration_calls: AtomicUsize,
    /// Remaining resumes to refuse with the typed divergence.
    refuse_resumes: AtomicUsize,
    /// While set, the registration reload reports the typed divergence.
    reload_registration_diverges: AtomicBool,
    /// While set, the heal authority reports the typed repair verdict.
    recover_diverges: AtomicBool,
}

#[async_trait]
impl SessionBridge for RepairBridge {
    async fn create_session(
        &self,
        _identity: &AgentIdentity,
        _runtime_id: &AgentRuntimeId,
        _spec: &DurableAgentSpec,
        _draft: &AgentBuildDraft,
        session_id: &meerkat_core::types::SessionId,
    ) -> Result<meerkat_core::types::SessionId, BridgeError> {
        Ok(session_id.clone())
    }

    async fn resume_session(
        &self,
        _identity: &AgentIdentity,
        _runtime_id: &AgentRuntimeId,
        _spec: &DurableAgentSpec,
        _draft: &AgentBuildDraft,
        session_id: &meerkat_core::types::SessionId,
        _snapshot: &SessionSnapshot,
    ) -> Result<ResumeSessionOutcome, BridgeError> {
        self.resume_calls.fetch_add(1, Ordering::SeqCst);
        if self
            .refuse_resumes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(BridgeError::ResumeRejected {
                kind: ResumeRejectionKind::AuditedEndpointDivergence,
                detail: divergence_detail(session_id),
            });
        }
        Ok(ResumeSessionOutcome::Resumed {
            session_id: session_id.clone(),
        })
    }

    async fn deliver_admitted(
        &self,
        _runtime_id: &AgentRuntimeId,
        _delivery: BridgeDelivery,
    ) -> Result<meerkat_core::types::SessionId, BridgeError> {
        Err(BridgeError::Mob(
            "delivery is out of scope here".to_string(),
        ))
    }

    async fn checkpoint_session(
        &self,
        _runtime_id: &AgentRuntimeId,
        _session_id: &meerkat_core::types::SessionId,
    ) -> Result<SessionSnapshot, BridgeError> {
        Ok(SessionSnapshot { data: Vec::new() })
    }

    async fn retire_member(&self, _runtime_id: &AgentRuntimeId) -> Result<(), BridgeError> {
        self.retire_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn reload_member_registration(
        &self,
        runtime_id: &AgentRuntimeId,
    ) -> Result<Option<BridgeMemberReload>, BridgeError> {
        self.reload_registration_calls
            .fetch_add(1, Ordering::SeqCst);
        if self.reload_registration_diverges.load(Ordering::SeqCst) {
            let session_id = meerkat_core::types::SessionId::new();
            return Err(BridgeError::SessionRepairRequired {
                identity: meerkat_mob::ids::AgentIdentity::from(runtime_id.as_str()),
                session_id: session_id.clone(),
                detail: divergence_detail(&session_id),
            });
        }
        Ok(None)
    }

    async fn recover_committed_boundary(
        &self,
        session_id: &meerkat_core::types::SessionId,
    ) -> Result<CommittedBoundaryRepair, BridgeError> {
        self.recover_calls.fetch_add(1, Ordering::SeqCst);
        if self.recover_diverges.load(Ordering::SeqCst) {
            return Ok(CommittedBoundaryRepair::RepairRequired {
                session_id: session_id.clone(),
                detail: divergence_detail(session_id),
            });
        }
        Ok(CommittedBoundaryRepair::Unsupported)
    }
}

struct StaticRoster {
    roster: Vec<DurableAgentSpec>,
    calls: AtomicUsize,
}

#[async_trait]
impl RosterProvider for StaticRoster {
    async fn roster(&self, _context: &RosterContext) -> Result<Vec<DurableAgentSpec>, RosterError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.roster.clone())
    }
}

fn runtime_with(bridge: Arc<RepairBridge>) -> (Arc<IdentityRuntime>, Arc<LocalContinuityStore>) {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease: Arc<dyn LeaseProvider> = Arc::new(LocalLeaseProvider::new());
    let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: store.clone() as Arc<dyn ContinuityStore>,
        lease_provider: lease,
        runtime_instance_id: "repair-hold-test".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(bridge),
        default_timeout: None,
    }));
    runtime.set_session_repair_scope(Some(SessionRepairScope {
        state_root: Some("/srv/example/state".into()),
        realm: Some("meta".to_string()),
        runtime_store: None,
    }));
    (runtime, store)
}

fn assert_hold_names_session(
    hold: &SessionRepairRequired,
    session_id: &meerkat_core::types::SessionId,
) {
    assert_eq!(hold.session_id, *session_id);
    assert_eq!(hold.hold, DurableResumeHold::AuditedEndpointDivergence);
    assert_eq!(
        hold.diagnose_command,
        format!(
            "rkat --state-root /srv/example/state --realm meta session repair-wholeblob \
             {session_id} --json"
        )
    );
    assert_eq!(
        hold.apply_command,
        format!(
            "rkat --state-root /srv/example/state --realm meta session repair-wholeblob \
             {session_id} --apply --json"
        )
    );
    assert!(
        hold.detail.contains("audited endpoint"),
        "the hold carries the producing refusal verbatim: {}",
        hold.detail
    );
}

/// Resume path: the first typed refusal parks the identity with the hold,
/// nothing retries against it, and `reload_member` resumes the same session
/// once the document reads again.
#[tokio::test]
async fn first_divergent_resume_parks_typed_and_reload_member_resumes_after_repair() {
    let bridge = Arc::new(RepairBridge::default());
    bridge.refuse_resumes.store(1, Ordering::SeqCst);
    let (runtime, store) = runtime_with(bridge.clone());

    let id = identity();
    let bound = record(3);
    let session_id = bound.session_id.clone();
    store
        .upsert_continuity_record(&bound, FencingToken::new(0))
        .await
        .unwrap();
    let roster = vec![spec()];
    let registered = lazy_register_flow(&runtime, &roster, None).await.unwrap();
    assert!(matches!(
        registered.outcomes.get(&id).unwrap(),
        RestoreOutcome::Dormant { .. }
    ));

    // 1. The FIRST refusal parks the identity typed.
    let error = runtime
        .materialize(&id)
        .await
        .expect_err("a divergent document must fail the materialization");
    match &error {
        IdentityRuntimeError::EmbodimentRejected(failure) => {
            assert_eq!(failure.kind, ContinuityFailureKind::RepairRequired);
            assert_eq!(
                failure.record.as_ref().map(|record| &record.session_id),
                Some(&session_id)
            );
        }
        other => panic!("expected the typed embodiment rejection, got {other:?}"),
    }
    let data = error
        .structured_data()
        .expect("the typed failure carries structured data");
    assert_eq!(data["kind"], "mob_member_session_repair_required");
    assert_eq!(
        meerkat_core::service::SessionError::durable_resume_hold_from_data(&data),
        Some(DurableResumeHold::AuditedEndpointDivergence),
        "the hold rides meerkat's own wire key: {data}"
    );
    assert_eq!(data["session_id"], session_id.to_string());
    assert_eq!(data["retryable"], false);

    let status = runtime.status(&id).await.unwrap();
    assert_eq!(status.state, IdentityLifecycleState::Broken);
    let hold = status
        .session_repair_required
        .as_ref()
        .expect("the FIRST refusal records the repair hold on the status");
    assert_hold_names_session(hold, &session_id);
    assert!(
        status.continuity_unrecoverable.is_none(),
        "a repairable hold is not the terminal heal verdict"
    );
    let health = runtime.member_health(&id).await.unwrap();
    assert_eq!(health.state, IdentityLifecycleState::Broken);
    assert_hold_names_session(
        health
            .session_repair_required
            .as_ref()
            .expect("member_health carries the hold and the commands"),
        &session_id,
    );
    assert_eq!(bridge.resume_calls.load(Ordering::SeqCst), 1);

    // 2. Nothing retries against the hold: no heal call, no reconcile churn,
    //    no second resume across many supervisor cycles.
    let provider = Arc::new(StaticRoster {
        roster: roster.clone(),
        calls: AtomicUsize::new(0),
    });
    let context = Arc::new(IdentityFirstRuntimeContext::new(
        runtime.clone(),
        provider.clone(),
        None,
        None,
        None,
    ));
    let repair = context
        .clone()
        .spawn_broken_identity_repair_task(ContinuityRepairPolicy::default());
    trigger_repairs_repeatedly(&context, Duration::from_millis(250)).await;
    repair.abort();
    assert_eq!(
        bridge.recover_calls.load(Ordering::SeqCst),
        0,
        "a repair-held identity must not reach the heal authority"
    );
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        0,
        "a repair-held identity must not trigger reconcile churn"
    );
    assert_eq!(
        bridge.resume_calls.load(Ordering::SeqCst),
        1,
        "the refused resume is never re-attempted on a timer"
    );

    // Reconcile keeps the Broken projection typed as the repair hold.
    let reconciled = lazy_register_flow(&runtime, &roster, None).await.unwrap();
    match reconciled.outcomes.get(&id).unwrap() {
        RestoreOutcome::Broken(failure) => {
            assert_eq!(failure.kind, ContinuityFailureKind::RepairRequired);
            assert!(
                failure.detail.contains("repair-wholeblob"),
                "{}",
                failure.detail
            );
        }
        other => panic!("a repair-held identity must stay Broken, got {other:?}"),
    }
    assert!(
        runtime
            .status(&id)
            .await
            .unwrap()
            .session_repair_required
            .is_some(),
        "reconcile must not forget the hold"
    );

    // 3. The operator repaired the document (the bridge now resumes). The
    //    same reload verb, no flag: same session, same generation, hold gone.
    let outcome = runtime
        .reload_member(&id)
        .await
        .expect("reload_member resumes a repaired session");
    assert!(outcome.reloaded);
    assert_eq!(outcome.disposition, MemberReloadDisposition::Discarded);
    assert_eq!(outcome.session_id.as_ref(), Some(&session_id));
    assert_eq!(outcome.generation, Some(bound.generation));
    let status = runtime.status(&id).await.unwrap();
    assert_eq!(status.state, IdentityLifecycleState::Active);
    assert!(
        status.session_repair_required.is_none(),
        "a successful resume clears the hold"
    );
    assert_eq!(
        bridge.resume_calls.load(Ordering::SeqCst),
        2,
        "exactly one operator-driven re-attempt"
    );
    assert_eq!(
        runtime
            .member_health(&id)
            .await
            .unwrap()
            .last_reload
            .unwrap()
            .outcome,
        ReloadAttemptOutcome::Discarded
    );
}

/// A document still refused after the operator's reload re-parks the
/// identity typed, records the attempt as `repair_required`, and keeps the
/// session bound: the verb never fresh-spawns or resets.
#[tokio::test]
async fn reload_member_on_a_still_refused_document_reparks_typed() {
    let bridge = Arc::new(RepairBridge::default());
    bridge.refuse_resumes.store(2, Ordering::SeqCst);
    let (runtime, store) = runtime_with(bridge.clone());
    let id = identity();
    let bound = record(1);
    let session_id = bound.session_id.clone();
    store
        .upsert_continuity_record(&bound, FencingToken::new(0))
        .await
        .unwrap();
    lazy_register_flow(&runtime, &[spec()], None).await.unwrap();
    runtime.materialize(&id).await.expect_err("first refusal");

    let error = runtime
        .reload_member(&id)
        .await
        .expect_err("a still-refused document fails the reload typed");
    match &error {
        IdentityRuntimeError::EmbodimentRejected(failure) => {
            assert_eq!(failure.kind, ContinuityFailureKind::RepairRequired);
        }
        other => panic!("expected the typed embodiment rejection, got {other:?}"),
    }
    let status = runtime.status(&id).await.unwrap();
    assert_eq!(status.state, IdentityLifecycleState::Broken);
    assert_eq!(
        status.session_id.as_ref(),
        Some(&session_id),
        "the durable session stays bound"
    );
    assert_hold_names_session(
        status.session_repair_required.as_ref().unwrap(),
        &session_id,
    );
    let health = runtime.member_health(&id).await.unwrap();
    let last_reload = health.last_reload.expect("the attempt is recorded");
    assert_eq!(last_reload.outcome, ReloadAttemptOutcome::RepairRequired);
    assert_eq!(
        last_reload.data.as_ref().map(|data| data["kind"].clone()),
        Some(serde_json::json!("mob_member_session_repair_required"))
    );
    assert_eq!(bridge.resume_calls.load(Ordering::SeqCst), 2);

    // Third time the document reads: back to Active on the same session.
    let outcome = runtime.reload_member(&id).await.expect("repaired");
    assert_eq!(outcome.session_id.as_ref(), Some(&session_id));
    assert_eq!(
        runtime.status(&id).await.unwrap().state,
        IdentityLifecycleState::Active
    );
    assert_eq!(bridge.resume_calls.load(Ordering::SeqCst), 3);
}

/// Registration reload path (the downstream-app shape: an Active member whose every
/// send reports reload-required): the typed divergence from the reload
/// primitive parks the member with the hold instead of the retryable
/// "store not healthy yet" refusal, and the next `reload_member` after the
/// repair resumes the same session.
#[tokio::test]
async fn registration_reload_divergence_parks_an_active_member_typed() {
    let bridge = Arc::new(RepairBridge::default());
    bridge
        .reload_registration_diverges
        .store(true, Ordering::SeqCst);
    let (runtime, _store) = runtime_with(bridge.clone());
    let id = identity();
    let bound = record(2);
    let session_id = bound.session_id.clone();
    runtime
        .register(
            spec(),
            IdentityLifecycleState::Active,
            Some(bound.clone()),
            Some(grant(1)),
        )
        .await;

    let error = runtime
        .reload_member(&id)
        .await
        .expect_err("a divergent registration reload parks typed");
    assert!(
        matches!(
            &error,
            IdentityRuntimeError::EmbodimentRejected(failure)
                if failure.kind == ContinuityFailureKind::RepairRequired
        ),
        "{error:?}"
    );
    assert_eq!(bridge.reload_registration_calls.load(Ordering::SeqCst), 1);
    let status = runtime.status(&id).await.unwrap();
    assert_eq!(status.state, IdentityLifecycleState::Broken);
    let hold = status
        .session_repair_required
        .as_ref()
        .expect("the registration reload records the hold");
    assert_eq!(hold.hold, DurableResumeHold::AuditedEndpointDivergence);
    assert!(
        hold.apply_command.ends_with("--apply --json"),
        "{}",
        hold.apply_command
    );
    assert_eq!(
        runtime
            .member_health(&id)
            .await
            .unwrap()
            .last_reload
            .unwrap()
            .outcome,
        ReloadAttemptOutcome::RepairRequired
    );
    assert_eq!(
        bridge.resume_calls.load(Ordering::SeqCst),
        0,
        "the registration reload never falls through to a resume that cannot read the document"
    );

    // Operator repaired the document: the same verb resumes the same session.
    bridge
        .reload_registration_diverges
        .store(false, Ordering::SeqCst);
    let outcome = runtime.reload_member(&id).await.expect("repaired");
    assert!(outcome.reloaded);
    assert_eq!(outcome.session_id.as_ref(), Some(&session_id));
    assert_eq!(outcome.generation, Some(bound.generation));
    assert_eq!(
        runtime.status(&id).await.unwrap().state,
        IdentityLifecycleState::Active
    );
    assert!(
        runtime
            .status(&id)
            .await
            .unwrap()
            .session_repair_required
            .is_none()
    );
    assert_eq!(bridge.resume_calls.load(Ordering::SeqCst), 1);
}

/// Heal path: the repair supervisor asks the heal authority about a Broken
/// identity; a typed repair verdict parks it with the hold after exactly one
/// recovery call, with no reconcile churn.
#[tokio::test]
async fn heal_verdict_divergence_parks_the_identity_after_one_recovery_call() {
    let bridge = Arc::new(RepairBridge::default());
    bridge.recover_diverges.store(true, Ordering::SeqCst);
    let (runtime, _store) = runtime_with(bridge.clone());
    let id = identity();
    let bound = record(5);
    let session_id = bound.session_id.clone();
    runtime
        .register(spec(), IdentityLifecycleState::Broken, Some(bound), None)
        .await;

    let provider = Arc::new(StaticRoster {
        roster: vec![spec()],
        calls: AtomicUsize::new(0),
    });
    let context = Arc::new(IdentityFirstRuntimeContext::new(
        runtime.clone(),
        provider.clone(),
        None,
        None,
        None,
    ));
    let repair = context
        .clone()
        .spawn_broken_identity_repair_task(ContinuityRepairPolicy::default());
    trigger_repairs_repeatedly(&context, Duration::from_millis(250)).await;
    repair.abort();

    assert_eq!(
        bridge.recover_calls.load(Ordering::SeqCst),
        1,
        "one typed verdict parks the identity; recovery is not retried on a timer"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(bridge.resume_calls.load(Ordering::SeqCst), 0);
    let status = runtime.status(&id).await.unwrap();
    assert_eq!(status.state, IdentityLifecycleState::Broken);
    assert_hold_names_session(
        status.session_repair_required.as_ref().unwrap(),
        &session_id,
    );
}

/// Keep triggering the continuity repair supervisor explicitly across
/// `window` (the `meta/request_continuity_repair` trigger). Used only by
/// NEGATIVE assertions: a parked identity must not be retried however often
/// the supervisor is triggered. Positive heals rely on typed triggers alone.
async fn trigger_repairs_repeatedly(
    context: &meerkat_meta::identity_first::IdentityFirstRuntimeContext,
    window: Duration,
) {
    let deadline = tokio::time::Instant::now() + window;
    while tokio::time::Instant::now() < deadline {
        context.request_continuity_repair().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
