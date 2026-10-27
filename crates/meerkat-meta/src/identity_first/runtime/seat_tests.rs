//! Rows for the seat step (design rows X3, R15, R64, R114 and the first-boot
//! carry-out of section 10): every roster identity Core does not hold is
//! seated Dormant in one Core call, with nothing materialized; a pending
//! first-boot row is carried out by its identity's seat; and a rotation a
//! crash interrupted is driven to its end by the next compile.

use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use meerkat_core::types::HandlingMode;

use super::*;
use crate::identity_first::bridge::{
    BridgeDelivery, BridgeError, DormantSeatRequest, DormantSeatResult, MemberRuntimeBinding,
    ResumeSessionOutcome, SessionBridge,
};
use crate::identity_first::core_test_bridge::bind_live;
use crate::identity_first::core_test_members::CoreTestMembers;
use crate::identity_first::first_boot_manifest::FirstBootManifest;
use crate::identity_first::local_lease::LocalLeaseProvider;
use crate::identity_first::local_store::LocalContinuityStore;
use crate::identity_first::{
    AgentAddressability, AgentRuntimeId, DurabilityPolicy, IdentityRuntimeConfig, SessionSnapshot,
};

fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn identity(name: &str) -> AgentIdentity {
    AgentIdentity::parse(name).expect("valid identity")
}

fn spec(identity: &AgentIdentity) -> DurableAgentSpec {
    DurableAgentSpec {
        identity: identity.clone(),
        profile: meerkat_mob::ProfileName::from("personal"),
        addressability: AgentAddressability::Addressable,
        display_name: None,
        labels: std::collections::BTreeMap::new(),
        context: None,
        additional_instructions: Vec::new(),
        initial_message: None,
        runtime_mode_override: None,
        backend: None,
        binding: None,
        placement: None,
    }
}

/// A Core that seats, rotates and releases members over a
/// [`CoreTestMembers`] history, and counts every other Core step a seat
/// must never take: a create, a resume, a wake.
#[derive(Default)]
struct SeatCore {
    core: CoreTestMembers,
    /// Core refuses this identity's seat, typed.
    refused: Option<AgentIdentity>,
    /// Core's read of its broken members fails.
    broken_read_fails: bool,
    /// Each seat call, as the identities and requested sessions it carried.
    seat_calls: StdMutex<Vec<Vec<(AgentIdentity, Option<SessionId>)>>>,
    rotations: StdMutex<Vec<AgentIdentity>>,
    releases: StdMutex<Vec<AgentIdentity>>,
    history_reads: AtomicUsize,
    creates: AtomicUsize,
    resumes: AtomicUsize,
    wakes: AtomicUsize,
}

impl SeatCore {
    fn session_of(&self, identity: &AgentIdentity) -> Option<SessionId> {
        self.core
            .last_member_binding(identity)
            .expect("Core's history reads")
            .and_then(|binding| binding.session_id().cloned())
    }

    fn generation_of(&self, identity: &AgentIdentity) -> Option<u64> {
        self.core
            .last_member_binding(identity)
            .expect("Core's history reads")
            .map(|binding| binding.generation().get())
    }

    fn next_generation(&self, identity: &AgentIdentity) -> Result<u64, BridgeError> {
        Ok(self
            .core
            .last_member_binding(identity)?
            .map_or(1, |binding| binding.generation().get() + 1))
    }
}

#[async_trait::async_trait]
impl SessionBridge for SeatCore {
    async fn create_session(
        &self,
        identity: &AgentIdentity,
        _spec: &DurableAgentSpec,
        _draft: &AgentBuildDraft,
    ) -> Result<MemberRuntimeBinding, BridgeError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        Err(BridgeError::Mob(format!(
            "{identity}: a seat creates no member"
        )))
    }

    async fn resume_session(
        &self,
        identity: &AgentIdentity,
        _spec: &DurableAgentSpec,
        _draft: &AgentBuildDraft,
        _session_id: &SessionId,
        _resume_from_role: Option<&meerkat_mob::ProfileName>,
    ) -> Result<ResumeSessionOutcome, BridgeError> {
        self.resumes.fetch_add(1, Ordering::SeqCst);
        Err(BridgeError::Mob(format!(
            "{identity}: a seat resumes no session"
        )))
    }

    async fn live_member(
        &self,
        identity: &AgentIdentity,
    ) -> Result<Option<MemberRuntimeBinding>, BridgeError> {
        self.core.live_member(identity)
    }

    async fn live_member_scope(
        &self,
        identity: &AgentIdentity,
    ) -> Result<Option<meerkat_mob::MemberDeliveryScope>, BridgeError> {
        self.core.live_member_scope(identity)
    }

    async fn ensure_member_live(
        &self,
        identity: &AgentIdentity,
    ) -> Result<Option<MemberRuntimeBinding>, BridgeError> {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        self.core.live_member(identity)
    }

    async fn member_binding_history(
        &self,
    ) -> Result<meerkat_mob::MemberBindingHistory, BridgeError> {
        self.history_reads.fetch_add(1, Ordering::SeqCst);
        self.core.history()
    }

    async fn last_member_binding(
        &self,
        identity: &AgentIdentity,
    ) -> Result<Option<meerkat_mob::MemberBinding>, BridgeError> {
        self.core.last_member_binding(identity)
    }

    async fn identity_management(
        &self,
        _identity: &AgentIdentity,
    ) -> Result<meerkat_mob::MemberIdentityManagement, BridgeError> {
        Ok(meerkat_mob::MemberIdentityManagement::CommandDriven)
    }

    async fn durable_identity_members(
        &self,
    ) -> Result<Vec<meerkat_mob::MemberBinding>, BridgeError> {
        self.core.durable_identity_members()
    }

    async fn broken_members(
        &self,
    ) -> Result<std::collections::BTreeMap<meerkat_mob::AgentIdentity, String>, BridgeError> {
        if self.broken_read_fails {
            return Err(BridgeError::Mob(
                "Core's member list did not read".to_string(),
            ));
        }
        Ok(std::collections::BTreeMap::new())
    }

    async fn seat_dormant_members(
        &self,
        requests: Vec<DormantSeatRequest>,
    ) -> Result<Vec<(AgentIdentity, DormantSeatResult)>, BridgeError> {
        let mut call = Vec::with_capacity(requests.len());
        let mut results = Vec::with_capacity(requests.len());
        for request in requests {
            call.push((request.identity.clone(), request.session.clone()));
            if self.refused.as_ref() == Some(&request.identity) {
                results.push((
                    request.identity.clone(),
                    DormantSeatResult::Refused(BridgeError::Mob(format!(
                        "Core refused the seat of {}",
                        request.identity
                    ))),
                ));
                continue;
            }
            if let Some(live) = self.core.live_member(&request.identity)?
                && let Some(session_id) = live.session_id
                && request.session.as_ref() == Some(&session_id)
            {
                results.push((
                    request.identity.clone(),
                    DormantSeatResult::AlreadySeated { session_id },
                ));
                continue;
            }
            let session_id = request.session.clone().unwrap_or_else(SessionId::new);
            let generation = self.next_generation(&request.identity)?;
            bind_live(
                &self.core,
                &request.identity,
                &request.spec.profile,
                generation,
                Some(session_id.clone()),
            )?;
            results.push((
                request.identity.clone(),
                DormantSeatResult::Seated { session_id },
            ));
        }
        lock(&self.seat_calls).push(call);
        Ok(results)
    }

    async fn rotate_member(
        &self,
        identity: &AgentIdentity,
        spec: &DurableAgentSpec,
        _draft: &AgentBuildDraft,
    ) -> Result<SessionId, BridgeError> {
        lock(&self.rotations).push(identity.clone());
        if let Some(live) = self.core.live_member(identity)? {
            self.core.retire(&live.agent_runtime_id);
        }
        let generation = self.next_generation(identity)?;
        let session_id = SessionId::new();
        bind_live(
            &self.core,
            identity,
            &spec.profile,
            generation,
            Some(session_id.clone()),
        )?;
        Ok(session_id)
    }

    async fn release_member(&self, identity: &AgentIdentity) -> Result<(), BridgeError> {
        lock(&self.releases).push(identity.clone());
        Ok(())
    }

    async fn deliver_admitted(
        &self,
        runtime_id: &AgentRuntimeId,
        _delivery: BridgeDelivery,
    ) -> Result<SessionId, BridgeError> {
        Err(BridgeError::Mob(format!(
            "{runtime_id}: a seat delivers nothing"
        )))
    }

    async fn checkpoint_session(
        &self,
        runtime_id: &AgentRuntimeId,
        _session_id: &SessionId,
    ) -> Result<SessionSnapshot, BridgeError> {
        Err(BridgeError::Mob(format!(
            "{runtime_id}: a seat checkpoints nothing"
        )))
    }

    async fn retire_member(&self, runtime_id: &AgentRuntimeId) -> Result<(), BridgeError> {
        self.core.retire(runtime_id);
        Ok(())
    }
}

/// A runtime whose Core is `core`, over the first-boot manifest in
/// `state_dir` when there is one.
fn runtime_over(state_dir: Option<&std::path::Path>, core: &Arc<SeatCore>) -> Arc<IdentityRuntime> {
    let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: Arc::new(LocalContinuityStore::in_memory().expect("store")),
        lease_provider: Arc::new(LocalLeaseProvider::new()),
        runtime_instance_id: "seat-rows".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(core.clone()),
        default_timeout: None,
    }));
    runtime.set_first_boot_state_dir(state_dir.map(std::path::Path::to_path_buf));
    runtime
}

async fn register_all(runtime: &IdentityRuntime, specs: &[DurableAgentSpec]) {
    for spec in specs {
        runtime.register(spec.clone()).await;
    }
}

/// X3 (G5) and R15: a roster compile seats every identity Core does not
/// hold, Dormant, in ONE Core call planned from ONE read of Core's member
/// history. Nothing is created, resumed or woken: each identity is a member
/// bound to a fresh session Core minted, and a delivery materializes it.
#[tokio::test]
async fn x3_r15_a_compile_seats_every_unheld_identity_in_one_core_call_and_wakes_none() {
    let core = Arc::new(SeatCore::default());
    let runtime = runtime_over(None, &core);
    let specs = ["personal:alice", "personal:bob", "personal:carol"]
        .map(|name| spec(&identity(name)))
        .to_vec();
    register_all(&runtime, &specs).await;

    let outcomes = runtime.seat_identities(&specs, None).await;

    for spec in &specs {
        let outcome = outcomes
            .get(&spec.identity)
            .expect("one outcome per identity")
            .as_ref()
            .expect("seated");
        assert!(!outcome.already_seated, "{}", spec.identity);
        assert!(outcome.minted, "{}: a fresh seat", spec.identity);
        assert_eq!(outcome.session_id, core.session_of(&spec.identity));
    }
    let calls = lock(&core.seat_calls).clone();
    assert_eq!(calls.len(), 1, "one Core call seats the whole compile");
    assert_eq!(
        calls[0],
        specs
            .iter()
            .map(|spec| (spec.identity.clone(), None))
            .collect::<Vec<_>>()
    );
    assert_eq!(core.history_reads.load(Ordering::SeqCst), 1);
    assert_eq!(core.creates.load(Ordering::SeqCst), 0);
    assert_eq!(core.resumes.load(Ordering::SeqCst), 0);
    assert_eq!(core.wakes.load(Ordering::SeqCst), 0, "a seat wakes nothing");
}

/// R15: a compile over identities Core already holds calls Core for
/// nothing: one history read plans them all, and every one answers
/// already seated.
#[tokio::test]
async fn r15_a_compile_over_seated_identities_calls_core_for_nothing() {
    let core = Arc::new(SeatCore::default());
    let runtime = runtime_over(None, &core);
    let specs = ["personal:alice", "personal:bob"]
        .map(|name| spec(&identity(name)))
        .to_vec();
    register_all(&runtime, &specs).await;
    runtime.seat_identities(&specs, None).await;

    let outcomes = runtime.seat_identities(&specs, None).await;

    assert!(
        outcomes
            .values()
            .all(|outcome| outcome.as_ref().is_ok_and(|seat| seat.already_seated)),
        "every identity is already seated"
    );
    assert_eq!(lock(&core.seat_calls).len(), 1, "no second Core call");
    assert_eq!(
        core.history_reads.load(Ordering::SeqCst),
        2,
        "one read per compile"
    );
}

/// The first-boot carry-out: each pending `Resume` row is its identity's
/// seat on the row's session, all of them in one Core call, and the
/// manifest is rewritten once for the whole step (here removed, every row
/// being done). Nothing is resumed or woken.
#[tokio::test]
async fn first_boot_resume_rows_are_seats_on_their_sessions_and_the_manifest_is_rewritten_once() {
    let rows = (0..6)
        .map(|index| FirstBootRow::Resume {
            identity: identity(&format!("personal:row-{index:02}")),
            session: SessionId::new(),
        })
        .collect::<Vec<_>>();
    let state_dir = tempfile::tempdir().expect("state dir");
    FirstBootManifest::write_new(state_dir.path(), rows.clone()).expect("the manifest writes");
    let core = Arc::new(SeatCore::default());
    let runtime = runtime_over(Some(state_dir.path()), &core);
    let specs = rows
        .iter()
        .map(|row| spec(row.identity()))
        .collect::<Vec<_>>();
    register_all(&runtime, &specs).await;

    let outcomes = runtime.seat_identities(&specs, None).await;

    for row in &rows {
        assert!(outcomes[row.identity()].is_ok(), "{}", row.identity());
        assert_eq!(
            core.session_of(row.identity()).as_ref(),
            Some(row.session()),
            "{} is seated on its row's session",
            row.identity()
        );
    }
    assert_eq!(lock(&core.seat_calls).len(), 1);
    assert_eq!(core.resumes.load(Ordering::SeqCst), 0);
    assert_eq!(core.wakes.load(Ordering::SeqCst), 0);
    assert_eq!(
        FirstBootManifest::read(state_dir.path()).expect("reads"),
        None,
        "every row is done"
    );
    assert_eq!(runtime.first_boot_rewrites.load(Ordering::SeqCst), 1);
    assert!(
        runtime
            .identity_bootstrap_status()
            .first_boot_pending
            .is_empty()
    );
}

/// A row whose seat Core refuses fails alone: its identity's outcome is the
/// typed refusal, its row is the only one the one rewritten manifest still
/// holds and the status still lists, and every other row is carried out.
#[tokio::test]
async fn a_refused_first_boot_seat_fails_alone_and_stays_in_the_rewritten_manifest() {
    let rows = (0..4)
        .map(|index| FirstBootRow::Resume {
            identity: identity(&format!("personal:row-{index:02}")),
            session: SessionId::new(),
        })
        .collect::<Vec<_>>();
    let refused = rows[2].identity().clone();
    let state_dir = tempfile::tempdir().expect("state dir");
    FirstBootManifest::write_new(state_dir.path(), rows.clone()).expect("the manifest writes");
    let core = Arc::new(SeatCore {
        refused: Some(refused.clone()),
        ..SeatCore::default()
    });
    let runtime = runtime_over(Some(state_dir.path()), &core);
    let specs = rows
        .iter()
        .map(|row| spec(row.identity()))
        .collect::<Vec<_>>();
    register_all(&runtime, &specs).await;

    let outcomes = runtime.seat_identities(&specs, None).await;

    assert!(outcomes[&refused].is_err(), "the refused row fails");
    assert_eq!(
        core.session_of(&refused),
        None,
        "the refused row binds nothing"
    );
    for row in rows.iter().filter(|row| row.identity() != &refused) {
        assert!(outcomes[row.identity()].is_ok(), "{}", row.identity());
        assert_eq!(
            core.session_of(row.identity()).as_ref(),
            Some(row.session())
        );
    }
    let left = FirstBootManifest::read(state_dir.path())
        .expect("reads")
        .expect("the refused row keeps the manifest")
        .into_rows();
    assert_eq!(
        left.iter().map(FirstBootRow::identity).collect::<Vec<_>>(),
        vec![&refused]
    );
    assert_eq!(runtime.first_boot_rewrites.load(Ordering::SeqCst), 1);
    assert_eq!(
        runtime
            .identity_bootstrap_status()
            .first_boot_pending
            .into_iter()
            .map(|row| row.identity)
            .collect::<Vec<_>>(),
        vec![refused]
    );
}

/// The status window: between a row's carry-out and the
/// rewrite of the manifest that drops it, the bootstrap status still lists
/// the row as pending. No reader sees "no pending row" while the row is
/// only settled in memory; Core's own host-preparation mark of the identity
/// ended in the seat's journaled step.
#[tokio::test]
async fn a_carried_out_row_reads_pending_until_the_manifest_rewrite_lands() {
    let alice = identity("personal:alice");
    let row = FirstBootRow::Resume {
        identity: alice.clone(),
        session: SessionId::new(),
    };
    let state_dir = tempfile::tempdir().expect("state dir");
    FirstBootManifest::write_new(state_dir.path(), vec![row]).expect("the manifest writes");
    let core = Arc::new(SeatCore::default());
    let runtime = runtime_over(Some(state_dir.path()), &core);
    let specs = vec![spec(&alice)];
    register_all(&runtime, &specs).await;
    runtime
        .load_first_boot_manifest()
        .await
        .expect("the manifest reads");

    let observed = Arc::new(StdMutex::new(None));
    let hook_runtime = Arc::clone(&runtime);
    let hook_observed = Arc::clone(&observed);
    runtime.set_first_boot_batch_hook(Box::new(move || {
        Box::pin(async move {
            let pending = hook_runtime
                .identity_bootstrap_status()
                .first_boot_pending
                .into_iter()
                .map(|row| row.identity)
                .collect::<Vec<_>>();
            *lock(&hook_observed) = Some(pending);
        })
    }));

    let outcomes = runtime.seat_identities(&specs, None).await;

    assert!(outcomes[&alice].is_ok());
    assert_eq!(
        lock(&observed).clone(),
        Some(vec![alice.clone()]),
        "before the rewrite lands the row still reads pending"
    );
    assert!(
        runtime
            .identity_bootstrap_status()
            .first_boot_pending
            .is_empty(),
        "after it lands, none does"
    );
}

/// R64: a pending first-boot `Fresh` row of an identity Core retired is
/// carried out by a Dormant seat on a fresh session in the seat call: no
/// spawn, no retire, no rotation.
#[tokio::test]
async fn r64_a_first_boot_fresh_row_is_a_dormant_seat_on_a_fresh_session() {
    let alice = identity("personal:alice");
    let retired_session = SessionId::new();
    let core = Arc::new(SeatCore::default());
    let retired = bind_live(
        &core.core,
        &alice,
        &meerkat_mob::ProfileName::from("personal"),
        1,
        Some(retired_session.clone()),
    )
    .expect("bound");
    core.core.retire(&retired);
    let state_dir = tempfile::tempdir().expect("state dir");
    FirstBootManifest::write_new(
        state_dir.path(),
        vec![FirstBootRow::Fresh {
            identity: alice.clone(),
            retired_session: retired_session.clone(),
        }],
    )
    .expect("the manifest writes");
    let runtime = runtime_over(Some(state_dir.path()), &core);
    let specs = vec![spec(&alice)];
    register_all(&runtime, &specs).await;

    let outcomes = runtime.seat_identities(&specs, None).await;

    let seat = outcomes[&alice].as_ref().expect("seated");
    assert!(seat.minted, "a fresh session");
    let session = core.session_of(&alice).expect("bound");
    assert_ne!(session, retired_session);
    assert_eq!(
        lock(&core.seat_calls).clone(),
        vec![vec![(alice.clone(), None)]]
    );
    assert!(lock(&core.rotations).is_empty(), "no rotation");
    assert_eq!(core.creates.load(Ordering::SeqCst), 0, "no spawn");
    assert_eq!(
        FirstBootManifest::read(state_dir.path()).expect("reads"),
        None
    );
}

/// R114: a rotation (`reset`, `delete_identity`) a crash interrupted is
/// recorded; the next compile drives it to its end through Core's rotation,
/// and removes the record once Core answered.
#[tokio::test]
async fn r114_an_interrupted_rotation_is_driven_to_its_end_by_the_next_compile() {
    let alice = identity("personal:alice");
    let core = Arc::new(SeatCore::default());
    let old_session = SessionId::new();
    bind_live(
        &core.core,
        &alice,
        &meerkat_mob::ProfileName::from("personal"),
        1,
        Some(old_session.clone()),
    )
    .expect("bound");
    let runtime = runtime_over(None, &core);
    let specs = vec![spec(&alice)];
    register_all(&runtime, &specs).await;
    runtime
        .record_rotation_intent(&alice, "reset", Some(1))
        .await
        .expect("the rotation is recorded");

    let outcomes = runtime.seat_identities(&specs, None).await;

    let seat = outcomes[&alice].as_ref().expect("rotated");
    assert!(seat.minted);
    assert_eq!(lock(&core.rotations).clone(), vec![alice.clone()]);
    assert_eq!(core.generation_of(&alice), Some(2));
    assert_ne!(core.session_of(&alice), Some(old_session));
    assert!(
        runtime
            .pending_rotation_intents()
            .await
            .expect("the records read")
            .is_empty(),
        "the finished rotation's record is removed"
    );
}

/// R114: when Core already finished the recorded rotation (the crash came
/// after its terminal step), the compile settles the record with a seat
/// step that writes nothing, and no second rotation runs.
#[tokio::test]
async fn r114_a_rotation_core_finished_settles_its_record_without_a_second_rotation() {
    let alice = identity("personal:alice");
    let core = Arc::new(SeatCore::default());
    let new_session = SessionId::new();
    bind_live(
        &core.core,
        &alice,
        &meerkat_mob::ProfileName::from("personal"),
        2,
        Some(new_session.clone()),
    )
    .expect("bound");
    let runtime = runtime_over(None, &core);
    let specs = vec![spec(&alice)];
    register_all(&runtime, &specs).await;
    runtime
        .record_rotation_intent(&alice, "delete_identity", Some(1))
        .await
        .expect("the rotation is recorded");

    let outcomes = runtime.seat_identities(&specs, None).await;

    let seat = outcomes[&alice].as_ref().expect("settled");
    assert!(seat.already_seated, "Core holds the rotated member already");
    assert!(lock(&core.rotations).is_empty(), "no second rotation");
    assert_eq!(core.session_of(&alice), Some(new_session));
    assert!(
        runtime
            .pending_rotation_intents()
            .await
            .expect("the records read")
            .is_empty()
    );
}

/// R65 and R66 (T10): Meta's retire releases the member: Core keeps it
/// seated on the same session at the same generation, so its saved delivery
/// scopes stay valid, and nothing is retired.
#[tokio::test]
async fn r66_retire_releases_the_member_and_keeps_its_session_and_generation() {
    let alice = identity("personal:alice");
    let core = Arc::new(SeatCore::default());
    let session = SessionId::new();
    let runtime = runtime_over(None, &core);
    crate::identity_first::core_test_bridge::register_live(
        &runtime,
        &core.core,
        spec(&alice),
        1,
        session.clone(),
    )
    .await
    .expect("registered live");

    runtime.retire(&alice).await.expect("retired");

    assert_eq!(lock(&core.releases).clone(), vec![alice.clone()]);
    assert!(
        core.core.live_member(&alice).expect("reads").is_some(),
        "still a member"
    );
    assert_eq!(core.session_of(&alice), Some(session));
    assert_eq!(core.generation_of(&alice), Some(1), "no generation advance");
}

/// R68 and F9, source guard: the identity runtime keeps no wake policy of
/// its own. No production path materializes a member's peers, keeps a
/// materialization backoff, holds run starts to keep a member dormant, or
/// decides a wake from a profile's `external_addressable` flag: Core decides
/// every materialization.
#[test]
fn r68_f9_the_identity_runtime_keeps_no_wake_policy_of_its_own() {
    let sources = [
        ("runtime.rs", include_str!("../runtime.rs")),
        ("runtime/seat.rs", include_str!("seat.rs")),
        ("runtime/records.rs", include_str!("records.rs")),
        (
            "runtime/build_preparation.rs",
            include_str!("build_preparation.rs"),
        ),
        ("orchestrator.rs", include_str!("../orchestrator.rs")),
        ("build_preparer.rs", include_str!("../build_preparer.rs")),
    ];
    for (file, source) in sources {
        // The production code only: a file's test modules follow it.
        let production = source
            .split("\n#[cfg(test)]\nmod ")
            .next()
            .unwrap_or(source);
        for needle in [
            "materialize_reachable_peers",
            "hydrate_peer_topology",
            "materialization_backoff",
            "hold_restored_member_run_starts",
            "external_addressable",
        ] {
            assert!(
                !production.contains(needle),
                "{file} names `{needle}`: Meta must not keep a wake policy of its own"
            );
        }
    }
}

/// R67: when Core's broken members cannot be read, an identity's status is
/// a typed refusal, never a guess: a member that may be Broken never reads
/// `active`.
#[tokio::test]
async fn r67_an_unreadable_broken_member_list_never_reads_active() {
    let alice = identity("personal:alice");
    let core = Arc::new(SeatCore {
        broken_read_fails: true,
        ..SeatCore::default()
    });
    let runtime = runtime_over(None, &core);
    crate::identity_first::core_test_bridge::register_live(
        &runtime,
        &core.core,
        spec(&alice),
        1,
        SessionId::new(),
    )
    .await
    .expect("registered live");

    match runtime.status(&alice).await {
        Err(IdentityRuntimeError::IdentityHistoryUnavailable { detail, .. }) => {
            assert!(detail.contains("broken members"), "{detail}");
        }
        other => panic!("expected a typed history refusal, got {other:?}"),
    }
}

/// The cross-mob door: an internal-only identity refuses a send through its
/// identity door exactly as it refuses an external `meta/send_message`
/// (`NotAddressable`), before any Core step, so a cross-mob send or inject
/// that names it delivers nothing.
#[tokio::test]
async fn an_internal_only_identity_refuses_a_cross_mob_send_as_an_external_send() {
    let alice = identity("personal:alice");
    let core = Arc::new(SeatCore::default());
    let runtime = runtime_over(None, &core);
    let mut internal = spec(&alice);
    internal.addressability = AgentAddressability::InternalOnly;
    crate::identity_first::core_test_bridge::register_live(
        &runtime,
        &core.core,
        internal,
        1,
        SessionId::new(),
    )
    .await
    .expect("registered live");
    let content = meerkat_core::ContentInput::Text("hello".to_string());

    let cross_mob = runtime
        .send_through_identity_door(&alice, alice.as_str(), &content)
        .await;
    let external = runtime
        .send_with_mode_and_interaction_tracked(&alice, &content, HandlingMode::Queue, None)
        .await;

    assert!(
        matches!(cross_mob, Err(IdentityRuntimeError::NotAddressable(_))),
        "{cross_mob:?}"
    );
    assert!(
        matches!(external, Err(IdentityRuntimeError::NotAddressable(_))),
        "{external:?}"
    );
    assert_eq!(core.wakes.load(Ordering::SeqCst), 0);
    assert!(lock(&core.seat_calls).is_empty(), "nothing reached Core");
}

/// A spec placed on a host no gateway has bound.
fn placed(identity: &AgentIdentity) -> DurableAgentSpec {
    DurableAgentSpec {
        placement: Some(meerkat_contracts::WireHostRef(
            "remote-member-host".to_string(),
        )),
        ..spec(identity)
    }
}

/// Placed and externally bound identities under the default pass
/// (`eager_materialize`): a placed identity Core does not hold has no
/// Dormant phase, so the pass creates it through its host, as 0.9.5's
/// default did, and a host that refuses it refuses it at the pass, typed.
/// The other identities are seated as ever.
#[tokio::test]
async fn the_default_pass_creates_a_placed_identity_through_its_host() {
    let core = Arc::new(SeatCore::default());
    let runtime = runtime_over(None, &core);
    let placed_identity = identity("personal:placed");
    let local = identity("personal:local");
    let specs = vec![placed(&placed_identity), spec(&local)];

    let result = crate::identity_first::orchestrator::restore_flow(&runtime, &specs, None, None)
        .await
        .expect("the pass runs");

    assert_eq!(
        core.creates.load(Ordering::SeqCst),
        1,
        "the pass creates the placed identity through its host"
    );
    assert!(
        matches!(
            result.outcomes.get(&placed_identity),
            Some(crate::identity_first::orchestrator::RestoreOutcome::Refused(_))
        ),
        "the host's refusal is the placed identity's own: {:?}",
        result.outcomes.get(&placed_identity)
    );
    let seated = lock(&core.seat_calls)
        .iter()
        .flatten()
        .map(|(identity, _)| identity.clone())
        .collect::<Vec<_>>();
    assert_eq!(seated, vec![local], "the local identity is seated");
}

/// Placed and externally bound identities under the explicit lazy modes: as
/// in 0.9.5, the pass leaves a placed identity Core does not hold for its
/// first use. Nothing is created at the pass, the identity reads dormant
/// (never broken while its host is not bound), and its first use creates it
/// through its host. The other identities are seated as under the default.
#[tokio::test]
async fn a_lazy_pass_leaves_a_placed_identity_for_its_first_use() {
    let core = Arc::new(SeatCore::default());
    let runtime = runtime_over(None, &core);
    let placed_identity = identity("personal:placed");
    let local = identity("personal:local");
    let specs = vec![placed(&placed_identity), spec(&local)];

    let result = crate::identity_first::orchestrator::lazy_register_flow(&runtime, &specs, None)
        .await
        .expect("the pass runs");

    assert_eq!(
        core.creates.load(Ordering::SeqCst),
        0,
        "the lazy pass creates nothing through a host"
    );
    assert!(
        matches!(
            result.outcomes.get(&placed_identity),
            Some(crate::identity_first::orchestrator::RestoreOutcome::Dormant { .. })
        ),
        "the placed identity waits for its first use: {:?}",
        result.outcomes.get(&placed_identity)
    );
    let seated = lock(&core.seat_calls)
        .iter()
        .flatten()
        .map(|(identity, _)| identity.clone())
        .collect::<Vec<_>>();
    assert_eq!(seated, vec![local], "the local identity is seated");
    assert_eq!(
        runtime
            .status(&placed_identity)
            .await
            .expect("the placed identity's status")
            .state,
        IdentityLifecycleState::Dormant,
        "the placed identity reads dormant, not broken"
    );

    // Its first use creates it through its host (which this Core refuses).
    let first_use = runtime.materialize(&placed_identity).await;
    assert!(
        first_use.is_err(),
        "this Core refuses the create, typed: {:?}",
        first_use.err()
    );
    assert_eq!(
        core.creates.load(Ordering::SeqCst),
        1,
        "the first use creates the placed identity through its host"
    );
}
