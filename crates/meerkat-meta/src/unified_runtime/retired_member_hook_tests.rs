//! The gateway host's retirement hook
//! (`GatewayHostComposition::identity_member_retired`, the identity
//! runtime's crate-private retired-member observer): which retires reach it,
//! with which incarnation and cause, and with no Meta lock held.
//!
//! Every cause ends exactly one incarnation. An explicit or scoped retire
//! leaves a still listed identity to return at the next generation on the
//! same session; only a roster pass that no longer lists the identity
//! retires it as `Unlisted`. A roster pass hands its retirements to the hook
//! after the bootstrap controller is released, also when a later step of
//! the pass fails, so a hook may refresh the roster itself. Replacements,
//! held aborts and shutdowns never reach it, and a hook error leaves the
//! retirement standing and reaches the error hook once.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::identity_first::core_test_bridge::{CoreReadsBridge, register_live};
use crate::identity_first::runtime::RetiredMemberObserver;
use crate::identity_first::{
    AgentIdentity, ContinuityStore, DurabilityPolicy, HeldBootstrapStart,
    IdentityFirstRuntimeContext, IdentityLifecycleState, IdentityRuntime, IdentityRuntimeConfig,
    LocalContinuityStore, LocalLeaseProvider, MutableRosterProvider, RetiredIdentityCause,
    RetiredIdentityMember, RosterProvider, ScopedRetireError,
};
use crate::unified_runtime::types::ErrorEvent;

use super::held_identity_test_support::{governed_runtime, identity_runtime, member};

/// The setup deadline of the fixture's held passes.
const HOLD: Duration = Duration::from_mins(1);

/// What the hook saw, in order.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<RetiredIdentityMember>>>);

impl Seen {
    fn calls(&self) -> Vec<RetiredIdentityMember> {
        self.0.lock().unwrap().clone()
    }
}

/// A hook that records each retired incarnation and answers `answer`.
fn recording(seen: &Seen, answer: Result<(), String>) -> RetiredMemberObserver {
    let seen = seen.clone();
    Arc::new(move |retired| {
        seen.0.lock().unwrap().push(retired);
        let answer = answer.clone();
        Box::pin(async move { answer })
    })
}

/// A running roster of two members, bootstrapped held, with the recording
/// hook installed before the bootstrap.
struct Fixture {
    runtime: crate::UnifiedRuntime,
    context: Arc<IdentityFirstRuntimeContext>,
    roster: Arc<MutableRosterProvider>,
    kept: AgentIdentity,
    other: AgentIdentity,
    seen: Seen,
}

impl Fixture {
    async fn start(dir: &tempfile::TempDir, prefix: &str) -> Self {
        let continuity = Arc::new(LocalContinuityStore::in_memory().expect("continuity store"));
        let kept = AgentIdentity::parse(&format!("{prefix}-kept")).unwrap();
        let other = AgentIdentity::parse(&format!("{prefix}-other")).unwrap();
        let mut runtime = governed_runtime(dir, prefix).await;
        let identities = identity_runtime(
            &runtime,
            Arc::clone(&continuity) as Arc<dyn ContinuityStore>,
            prefix,
        );
        let seen = Seen::default();
        identities.set_retired_member_observer(Some(recording(&seen, Ok(()))));
        let roster = Arc::new(MutableRosterProvider::new(vec![
            member(&kept),
            member(&other),
        ]));
        let context = Arc::new(IdentityFirstRuntimeContext::new(
            identities,
            Arc::clone(&roster) as Arc<dyn RosterProvider>,
            None,
            None,
            Some(runtime.mob_handle().definition().clone()),
        ));
        runtime.install_identity_first_context(Arc::clone(&context));
        runtime
            .bootstrap_installed_identity_first_context_held(
                &[member(&kept), member(&other)],
                tokio::time::Instant::now() + HOLD,
                Box::new(|_members, _completion| Box::pin(async { Ok(()) })),
            )
            .await
            .expect("bootstrap the roster");
        assert!(seen.calls().is_empty(), "bootstrapping retired nothing");
        Self {
            runtime,
            context,
            roster,
            kept,
            other,
            seen,
        }
    }

    fn identities(&self) -> &Arc<IdentityRuntime> {
        &self.context.runtime
    }

    async fn status(&self, identity: &AgentIdentity) -> crate::identity_first::IdentityStatus {
        self.identities()
            .status(identity)
            .await
            .expect("identity status")
    }
}

/// An explicit retire reaches the hook once, cause `Retire`, with the
/// incarnation it retired and with the identity's lifecycle lock released;
/// a second retire retires nothing and calls nothing.
#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_retire_reaches_the_hook_once() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Fixture::start(&dir, "retired-hook-explicit").await;
    let before = fixture.status(&fixture.kept).await;
    assert_eq!(before.state, IdentityLifecycleState::Active);

    let lock_free = Arc::new(Mutex::new(None));
    let observed = Arc::clone(&lock_free);
    let identities = Arc::downgrade(fixture.identities());
    let kept = fixture.kept.clone();
    let seen = fixture.seen.clone();
    let observer: RetiredMemberObserver = Arc::new(move |retired| {
        seen.0.lock().unwrap().push(retired);
        let observed = Arc::clone(&observed);
        let identities = identities.clone();
        let kept = kept.clone();
        Box::pin(async move {
            let identities = identities.upgrade().expect("the identity runtime");
            let free = identities
                .lifecycle_lock_for(&kept)
                .await
                .try_lock()
                .is_ok();
            *observed.lock().unwrap() = Some(free);
            Ok(())
        })
    });
    fixture
        .identities()
        .set_retired_member_observer(Some(observer));

    fixture
        .identities()
        .retire_tracked(&fixture.kept)
        .await
        .expect("the member retires");
    let calls = fixture.seen.calls();
    assert_eq!(calls.len(), 1, "{calls:?}");
    let retired = &calls[0];
    assert_eq!(retired.identity, fixture.kept);
    assert_eq!(retired.cause, RetiredIdentityCause::Retire);
    assert_eq!(Some(&retired.member), before.member_id.as_ref());
    assert_eq!(retired.role, meerkat_mob::ProfileName::from("worker"));
    assert_eq!(
        Some(&retired.agent_runtime_id),
        before.agent_runtime_id.as_ref()
    );
    assert_eq!(retired.session_id, before.session_id);
    assert_eq!(
        Some(retired.generation),
        before.generation.map(meerkat_mob::Generation::get)
    );
    assert_eq!(
        *lock_free.lock().unwrap(),
        Some(true),
        "the hook runs with the identity's lifecycle lock released"
    );

    fixture
        .identities()
        .retire_tracked(&fixture.kept)
        .await
        .expect_err("a dormant identity has no live member to retire");
    assert_eq!(
        fixture.seen.calls().len(),
        1,
        "a second retire calls nothing"
    );
    let _ = fixture.runtime.shutdown().await;
}

/// A retired identity the roster still lists returns at the next
/// generation on the same session at the next roster pass, and the hook saw
/// only the one `Retire`: the pass retires nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_retired_listed_identity_returns_at_the_next_generation() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Fixture::start(&dir, "retired-hook-return").await;
    let before = fixture.status(&fixture.kept).await;
    fixture
        .identities()
        .retire_tracked(&fixture.kept)
        .await
        .expect("the member retires");
    fixture
        .context
        .refresh_desired_topology_tracked()
        .await
        .expect("the roster pass runs");
    let after = fixture.status(&fixture.kept).await;
    assert_eq!(after.state, IdentityLifecycleState::Active, "{after:?}");
    assert_eq!(after.session_id, before.session_id, "the same session");
    assert_eq!(
        after.generation.map(meerkat_mob::Generation::get),
        before.generation.map(|g| g.get() + 1),
        "the next generation"
    );
    let calls = fixture.seen.calls();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0].cause, RetiredIdentityCause::Retire);
    let _ = fixture.runtime.shutdown().await;
}

/// A roster pass that no longer lists an identity retires it as
/// `Unlisted`, once; a repeat pass retires and calls nothing.
#[tokio::test(flavor = "multi_thread")]
async fn an_unlisted_identity_reaches_the_hook_once() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Fixture::start(&dir, "retired-hook-unlisted").await;
    let before = fixture.status(&fixture.other).await;
    assert!(fixture.roster.remove(&fixture.other));
    fixture
        .context
        .refresh_desired_topology_tracked()
        .await
        .expect("the roster pass runs");
    let calls = fixture.seen.calls();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0].identity, fixture.other);
    assert_eq!(calls[0].cause, RetiredIdentityCause::Unlisted);
    assert_eq!(calls[0].session_id, before.session_id);
    fixture
        .context
        .refresh_desired_topology_tracked()
        .await
        .expect("the repeat pass runs");
    assert_eq!(fixture.seen.calls().len(), 1, "a repeat pass calls nothing");
    assert_eq!(
        fixture.status(&fixture.kept).await.state,
        IdentityLifecycleState::Active,
        "the listed member is untouched"
    );
    let _ = fixture.runtime.shutdown().await;
}

/// A boot pass that finds a live member Meta spawned for an identity the
/// roster no longer lists, which this runtime never registered, retires it
/// as `Unlisted` and hands it to the hook once, after the pass's bootstrap
/// controller is released.
#[tokio::test(flavor = "multi_thread")]
async fn a_boot_pass_hands_its_unlisted_retirement_to_the_hook() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Fixture::start(&dir, "retired-hook-boot").await;
    let continuity = Arc::new(LocalContinuityStore::in_memory().expect("continuity store"));
    let rebooted = identity_runtime(
        &fixture.runtime,
        continuity as Arc<dyn ContinuityStore>,
        "retired-hook-boot-second",
    );
    let seen = Seen::default();
    let controller_free = Arc::new(Mutex::new(None));
    let observed = Arc::clone(&controller_free);
    let probe = Arc::downgrade(&rebooted);
    let recorded = seen.clone();
    let observer: RetiredMemberObserver = Arc::new(move |retired| {
        recorded.0.lock().unwrap().push(retired);
        let observed = Arc::clone(&observed);
        let probe = probe.clone();
        Box::pin(async move {
            let runtime = probe.upgrade().expect("the identity runtime");
            *observed.lock().unwrap() = Some(runtime.bootstrap_controller_is_free());
            Ok(())
        })
    });
    rebooted.set_retired_member_observer(Some(observer));
    let context = IdentityFirstRuntimeContext::new(
        Arc::clone(&rebooted),
        Arc::new(MutableRosterProvider::new(vec![member(&fixture.kept)])),
        None,
        None,
        Some(fixture.runtime.mob_handle().definition().clone()),
    );
    context
        .bootstrap_roster(&[member(&fixture.kept)])
        .await
        .expect("the boot pass runs");
    let calls = seen.calls();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0].identity, fixture.other);
    assert_eq!(calls[0].cause, RetiredIdentityCause::Unlisted);
    assert_eq!(
        *controller_free.lock().unwrap(),
        Some(true),
        "the hook runs after the bootstrap controller is released"
    );
    let _ = fixture.runtime.shutdown().await;
}

/// A hook that refreshes the roster from inside itself completes: no Meta
/// lock is held while it runs, and both passes finish.
#[tokio::test(flavor = "multi_thread")]
async fn a_hook_that_refreshes_the_roster_does_not_deadlock() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Fixture::start(&dir, "retired-hook-reentrant").await;
    let context = Arc::downgrade(&fixture.context);
    let inner = Arc::new(Mutex::new(None));
    let observed = Arc::clone(&inner);
    let seen = fixture.seen.clone();
    let observer: RetiredMemberObserver = Arc::new(move |retired| {
        seen.0.lock().unwrap().push(retired);
        let context = context.clone();
        let observed = Arc::clone(&observed);
        Box::pin(async move {
            let context = context.upgrade().expect("the identity context");
            let refreshed = context.refresh_desired_topology_tracked().await;
            *observed.lock().unwrap() = Some(refreshed.is_ok());
            Ok(())
        })
    });
    fixture
        .identities()
        .set_retired_member_observer(Some(observer));
    assert!(fixture.roster.remove(&fixture.other));
    tokio::time::timeout(
        Duration::from_mins(2),
        fixture.context.refresh_desired_topology_tracked(),
    )
    .await
    .expect("the outer pass does not deadlock")
    .expect("the outer pass runs");
    assert_eq!(*inner.lock().unwrap(), Some(true), "the inner pass ran");
    assert_eq!(fixture.seen.calls().len(), 1);
    let _ = fixture.runtime.shutdown().await;
}

/// Replacements, a pass that lists every live member and a shutdown never
/// reach the hook: each keeps or replaces the identity's member, and none
/// ends it.
#[tokio::test(flavor = "multi_thread")]
async fn replacements_listed_passes_and_shutdown_never_reach_the_hook() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Fixture::start(&dir, "retired-hook-never").await;
    fixture
        .identities()
        .reload_member(&fixture.kept)
        .await
        .expect("the member reloads");
    fixture
        .identities()
        .respawn_tracked(&fixture.kept)
        .await
        .expect("the member respawns");
    fixture
        .identities()
        .reset_tracked(&fixture.other)
        .await
        .expect("the member resets");
    fixture
        .context
        .refresh_desired_topology_tracked()
        .await
        .expect("a pass that lists every member runs");
    assert!(
        fixture.seen.calls().is_empty(),
        "{:?}",
        fixture.seen.calls()
    );
    let _ = fixture.runtime.shutdown().await;
    assert!(fixture.seen.calls().is_empty(), "a shutdown calls nothing");
}

/// A held add the host refuses aborts its member before publication: no
/// incarnation of it was ever retired, so the hook is never called.
#[tokio::test(flavor = "multi_thread")]
async fn an_aborted_held_add_never_reaches_the_hook() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Fixture::start(&dir, "retired-hook-held").await;
    let added = AgentIdentity::parse("retired-hook-held-added").unwrap();
    match fixture
        .context
        .add_member_held(
            member(&added),
            tokio::time::Instant::now() + HOLD,
            |_members, _completion| async { Err("the application refused".to_string()) },
        )
        .await
        .expect("the held add starts")
    {
        HeldBootstrapStart::Unsettled(settlement) => {
            settlement
                .settled()
                .await
                .expect_err("the host's refusal aborts the add");
        }
        HeldBootstrapStart::Activated(result) => {
            panic!("the refused add activated: {:?}", result.outcomes)
        }
    }
    assert!(
        fixture.seen.calls().is_empty(),
        "{:?}",
        fixture.seen.calls()
    );
    let _ = fixture.runtime.shutdown().await;
}

/// A hook error leaves the retirement standing: the retire answers `Ok`,
/// Core's member is retired, the error reaches the error hook once as
/// `RetiredMemberReleaseFailed`, and nothing is retried.
#[tokio::test(flavor = "multi_thread")]
async fn a_hook_error_is_reported_apart_from_the_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Fixture::start(&dir, "retired-hook-failure").await;
    let seen = Seen::default();
    fixture
        .identities()
        .set_retired_member_observer(Some(recording(&seen, Err("store busy".to_string()))));
    let reported = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&reported);
    let hook: crate::unified_runtime::ErrorHook = Arc::new(move |event| {
        sink.lock().unwrap().push(event);
        Box::pin(async {})
    });
    fixture.identities().set_error_hook(Some(hook));
    let before = fixture.status(&fixture.kept).await;
    fixture
        .identities()
        .retire_tracked(&fixture.kept)
        .await
        .expect("the retirement stands");
    assert_eq!(
        fixture.status(&fixture.kept).await.state,
        IdentityLifecycleState::Dormant
    );
    assert_eq!(seen.calls().len(), 1, "called once, never retried");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if !reported.lock().unwrap().is_empty() || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        *reported.lock().unwrap(),
        vec![ErrorEvent::RetiredMemberReleaseFailed {
            identity: fixture.kept.to_string(),
            agent_runtime_id: before
                .agent_runtime_id
                .as_ref()
                .expect("a live runtime id")
                .to_string(),
            session_id: before.session_id.as_ref().map(ToString::to_string),
            cause: "retire".to_string(),
            error: "store busy".to_string(),
        }]
    );
    let _ = fixture.runtime.shutdown().await;
}

/// A scoped retire of the live member reaches the hook once, cause
/// `Scoped`. The same scope again retires nothing (no incarnation is
/// current, Core answers `Ok`) and an old scope after the identity returned
/// is stale: neither calls the hook.
#[tokio::test(flavor = "multi_thread")]
async fn a_scoped_retire_reaches_the_hook_only_for_the_live_member() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Fixture::start(&dir, "retired-hook-scoped").await;
    let scope = fixture
        .identities()
        .capture_delivery_scope(&fixture.kept)
        .await
        .expect("the live scope");
    fixture
        .identities()
        .retire_at_scope_tracked(&fixture.kept, &scope)
        .await
        .expect("the scoped retire");
    let calls = fixture.seen.calls();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0].cause, RetiredIdentityCause::Scoped);

    fixture
        .identities()
        .retire_at_scope_tracked(&fixture.kept, &scope)
        .await
        .expect("no incarnation is current: Core answers Ok");
    assert_eq!(fixture.seen.calls().len(), 1, "nothing was retired");

    fixture
        .context
        .refresh_desired_topology_tracked()
        .await
        .expect("the identity returns");
    match fixture
        .identities()
        .retire_at_scope_tracked(&fixture.kept, &scope)
        .await
    {
        Err(ScopedRetireError::StaleScope { .. }) => {}
        other => panic!("an old scope is stale once the identity returned: {other:?}"),
    }
    assert_eq!(fixture.seen.calls().len(), 1, "a stale scope calls nothing");
    let _ = fixture.runtime.shutdown().await;
}

/// A retirement the roster pass made before a later step of the same pass
/// failed still reaches the hook, and the pass's error still reaches its
/// caller.
#[tokio::test]
async fn a_retirement_before_a_failed_step_still_reaches_the_hook() {
    let bridge = Arc::new(CoreReadsBridge::default());
    let identities = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: Arc::new(LocalContinuityStore::in_memory().expect("continuity store")),
        lease_provider: Arc::new(LocalLeaseProvider::new()),
        runtime_instance_id: "retired-hook-partial".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(bridge.clone()),
        default_timeout: None,
    }));
    let seen = Seen::default();
    identities.set_retired_member_observer(Some(recording(&seen, Ok(()))));
    let gone = AgentIdentity::parse("retired-hook-partial-gone").unwrap();
    register_live(
        &identities,
        &bridge.core,
        member(&gone),
        1,
        meerkat_core::types::SessionId::new(),
    )
    .await
    .expect("a live registered member");
    let context = IdentityFirstRuntimeContext::new(
        Arc::clone(&identities),
        Arc::new(MutableRosterProvider::new(Vec::new())),
        None,
        None,
        None,
    );
    // The pass retires the unlisted member, then fails reading Core's
    // durable members: this bridge does not answer that read.
    context
        .refresh_desired_topology()
        .await
        .expect_err("the pass fails after its first retire");
    let calls = seen.calls();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0].identity, gone);
    assert_eq!(calls[0].cause, RetiredIdentityCause::Unlisted);
    assert_eq!(calls[0].generation, 1);
}
