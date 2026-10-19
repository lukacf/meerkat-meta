//! The console experience's identity bindings: an identity-first row's
//! session is the identity runtime's answer, a walk recorded before the
//! identity runtime moved is never served for the moved binding, and a row
//! answered pending past the refresh deadline withholds its walked facts on
//! the wire while access and visibility still decide on them.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use meerkat_client::{LlmClient, LlmError, LlmRequest};
use meerkat_core::{Message, Provider};
use meerkat_mob::MobDefinition;
use serde_json::{Value, json};

use super::{
    CONSOLE_BINDING_REFRESH_DEADLINE, ConsoleSnapshotReadModel, IdentityBinding,
    IdentityBindingCut, build_live_snapshot, join_identity_bindings,
    withhold_pending_experience_rows,
};
use crate::access::{AccessControlConfig, AccessController, AccessEffect, AccessRule};
use crate::console_aggregator::{AllowAllConsoleVisibilityPolicy, MetaConsoleAggregator};
use crate::identity_first::{
    AgentAddressability, AgentIdentity, DurableAgentSpec, IdentityLifecycleState,
    LocalContinuityStore, LocalLeaseProvider, MutableRosterProvider,
};
use crate::runtime::{ConsoleMember, ConsoleModelCapabilities, ConsoleRestJsonRequest};
use crate::unified_runtime::UnifiedRuntime;

const KEEPER: &str = "voice-keeper";
const OTHER: &str = "other-keeper";

/// The members never take a turn; the client only has to exist.
struct NeverCalledClient;

#[async_trait::async_trait]
impl LlmClient for NeverCalledClient {
    fn project_replay_messages(&self, messages: &[Message]) -> Result<Vec<Message>, LlmError> {
        Ok(messages.to_vec())
    }

    fn stream<'a>(&'a self, _request: &'a LlmRequest) -> meerkat_client::types::LlmStream<'a> {
        Box::pin(futures::stream::empty())
    }

    fn provider(&self) -> Provider {
        Provider::OpenAI
    }

    async fn health_check(&self) -> Result<(), LlmError> {
        Ok(())
    }
}

fn row(roster_id: &str, identity_label: Option<&str>, session: Option<&str>) -> ConsoleMember {
    ConsoleMember {
        agent_identity: roster_id.to_string(),
        role: "worker".to_string(),
        state: "active".to_string(),
        model_capabilities: ConsoleModelCapabilities::default(),
        runtime_mode: None,
        session_id: session.map(str::to_string),
        wired_to: vec![],
        labels: identity_label
            .map(|identity| BTreeMap::from([("agent_identity".to_string(), identity.to_string())]))
            .unwrap_or_default(),
        progress: Some(json!({
            "run_state": "run_open",
            "in_flight_work": 1,
            "last_progress_at_ms": 7,
            "last_progress_event": "became_busy",
            "health": "healthy",
        })),
    }
}

fn cut(bindings: &[(&str, Option<&str>, IdentityLifecycleState)]) -> IdentityBindingCut {
    IdentityBindingCut {
        bindings: bindings
            .iter()
            .map(|(identity, session, state)| {
                (
                    (*identity).to_string(),
                    IdentityBinding {
                        session_id: session.map(str::to_string),
                        state: *state,
                    },
                )
            })
            .collect(),
        ..IdentityBindingCut::default()
    }
}

fn fallback_progress() -> Value {
    json!({
        "run_state": "unknown",
        "in_flight_work": 0,
        "last_progress_at_ms": 0,
        "last_progress_event": "unchanged",
        "health": "unknown",
    })
}

#[tokio::test]
async fn the_join_binds_identity_first_rows_to_the_owner_session_and_leaves_plain_members() {
    let walked = cut(&[(KEEPER, Some("s0"), IdentityLifecycleState::Active)]);
    let current = cut(&[(KEEPER, Some("s1"), IdentityLifecycleState::Active)]);
    let mut members = vec![
        row(KEEPER, Some(KEEPER), Some("s0")),
        // A plain mob member: the identity runtime never knew it.
        row("worker-1", None, Some("p0")),
        // A row whose label names the keeper but whose roster id is another
        // member is not the keeper's roster row.
        row("imposter", Some(KEEPER), Some("p1")),
    ];
    let untouched = members[1..].to_vec();

    let pending = join_identity_bindings(&mut members, &walked, &current, true, None).await;

    assert!(pending.is_empty(), "the walk answers: nothing is pending");
    assert_eq!(members[0].session_id.as_deref(), Some("s1"));
    assert_eq!(
        members[0].progress,
        Some(fallback_progress()),
        "progress observed for s0 is never shown beside s1"
    );
    assert_eq!(members[0].state, "active", "state stays the walk's");
    assert_eq!(&members[1..], untouched.as_slice());
}

#[tokio::test]
async fn the_join_names_a_moved_binding_pending_only_when_the_walk_does_not_answer() {
    let walked = cut(&[
        (KEEPER, Some("s0"), IdentityLifecycleState::Active),
        (OTHER, Some("o0"), IdentityLifecycleState::Active),
    ]);

    // A reset: the session moved.
    let mut members = vec![
        row(KEEPER, Some(KEEPER), Some("s0")),
        row(OTHER, Some(OTHER), Some("o0")),
    ];
    let reset = cut(&[
        (KEEPER, Some("s1"), IdentityLifecycleState::Active),
        (OTHER, Some("o0"), IdentityLifecycleState::Active),
    ]);
    let pending = join_identity_bindings(&mut members, &walked, &reset, false, None).await;
    assert_eq!(pending, BTreeSet::from([KEEPER.to_string()]));
    assert_eq!(members[0].session_id.as_deref(), Some("s1"));
    assert_eq!(members[0].progress, Some(fallback_progress()));
    assert_eq!(
        members[1],
        row(OTHER, Some(OTHER), Some("o0")),
        "an unchanged binding answers from the walk"
    );

    // A retire: the session stays, the lifecycle state moved.
    let mut members = vec![row(KEEPER, Some(KEEPER), Some("s0"))];
    let retire = cut(&[
        (KEEPER, Some("s0"), IdentityLifecycleState::Retiring),
        (OTHER, Some("o0"), IdentityLifecycleState::Active),
    ]);
    let pending = join_identity_bindings(&mut members, &walked, &retire, false, None).await;
    assert_eq!(pending, BTreeSet::from([KEEPER.to_string()]));
    assert_eq!(members[0].session_id.as_deref(), Some("s0"));
    assert_eq!(members[0].progress, Some(fallback_progress()));

    // A delete: the identity runtime no longer knows the identity.
    let mut members = vec![row(KEEPER, Some(KEEPER), Some("s0"))];
    let delete = cut(&[(OTHER, Some("o0"), IdentityLifecycleState::Active)]);
    let pending = join_identity_bindings(&mut members, &walked, &delete, false, None).await;
    assert_eq!(pending, BTreeSet::from([KEEPER.to_string()]));
    assert_eq!(members[0].session_id, None);

    // The same moves with a walk that answers are joined, never pending.
    for current in [&reset, &retire, &delete] {
        let mut members = vec![row(KEEPER, Some(KEEPER), Some("s0"))];
        let pending = join_identity_bindings(&mut members, &walked, current, true, None).await;
        assert!(pending.is_empty());
    }
}

fn cut_read_at(
    bindings: &[(&str, Option<&str>, IdentityLifecycleState)],
    started: tokio::time::Instant,
) -> IdentityBindingCut {
    IdentityBindingCut {
        read_started_at: started,
        read_completed_at: started + Duration::from_millis(1),
        ..cut(bindings)
    }
}

#[test]
fn a_walk_answers_for_the_same_bindings_or_a_cut_read_after_the_request() {
    let base = tokio::time::Instant::now();
    let successor = [(KEEPER, Some("s1"), IdentityLifecycleState::Active)];
    let predecessor = [(KEEPER, Some("s0"), IdentityLifecycleState::Active)];
    let request = cut_read_at(&successor, base + Duration::from_millis(10));

    let older_same = cut_read_at(&successor, base);
    assert!(older_same.answers_for(&request), "the same bindings answer");
    assert!(std::ptr::eq(older_same.newer(&request), &raw const request));

    let older_moved = cut_read_at(&predecessor, base);
    assert!(
        !older_moved.answers_for(&request),
        "a walk read before the binding moved never answers"
    );
    assert!(std::ptr::eq(
        older_moved.newer(&request),
        &raw const request
    ));

    let newer_moved = cut_read_at(&predecessor, base + Duration::from_millis(20));
    assert!(
        newer_moved.answers_for(&request),
        "a walk whose cut was read after the request's answers for it"
    );
    assert!(
        std::ptr::eq(newer_moved.newer(&request), &raw const newer_moved),
        "the request joins with the newer cut"
    );

    let overlapping = cut_read_at(&predecessor, base + Duration::from_millis(10));
    assert!(
        !overlapping.answers_for(&request),
        "a cut read while the request was reading is not after it"
    );
}

fn experience_with_rows() -> Value {
    let keeper_agent = json!({
        "agent_id": KEEPER,
        "member_id": KEEPER,
        "identity": KEEPER,
        "label": "Keeper",
        "kind": "mob_agent",
        "role": "worker",
        "state": "active",
        "progress": fallback_progress(),
        "model_capabilities": {"image_input": true},
        "session_id": "s1",
        "wired_to": [OTHER],
        "labels": {"agent_identity": KEEPER, "tier": "restricted", "display_name": "Keeper"},
        "group": "keepers",
        "addressable": true,
        "watched": true,
        "alertLevel": "critical",
        "degraded": false,
        "degradedReason": null,
        "affordances": {
            "addressable": true,
            "can_send_message": true,
            "can_retire": true,
            "can_respawn": true,
            "runtime_mode": "mob_agent",
        },
    });
    let mut other_agent = keeper_agent.clone();
    other_agent["agent_id"] = json!(OTHER);
    other_agent["member_id"] = json!(OTHER);
    other_agent["identity"] = json!(OTHER);
    other_agent["labels"] = json!({"agent_identity": OTHER});
    other_agent["wired_to"] = json!([]);
    json!({
        "runtime_capabilities": {
            "profile_capabilities": {
                "worker": {"instance_count": 2, "addressable": true, "has_wiring": true},
            },
        },
        "agent_sidebar": {"live_snapshot": {"agents": [keeper_agent, other_agent]}},
        "identity_status": {"rows": [
            {
                "identity": KEEPER,
                "state": "active",
                "addressability": "addressable",
                "labels": {"agent_identity": KEEPER, "tier": "restricted"},
                "display_name": "Keeper",
                "role": "worker",
                "progress": fallback_progress(),
                "model_capabilities": {"image_input": true},
                "session_id": "s1",
            },
            {
                "identity": OTHER,
                "state": "active",
                "addressability": "addressable",
                "labels": {"agent_identity": OTHER},
                "role": "worker",
                "session_id": "o0",
            },
        ]},
    })
}

#[test]
fn a_pending_row_withholds_its_walked_facts_on_the_wire_and_fails_affordances_closed() {
    let mut experience = experience_with_rows();
    let untouched_other = experience["agent_sidebar"]["live_snapshot"]["agents"][1].clone();
    let untouched_other_status = experience["identity_status"]["rows"][1].clone();

    withhold_pending_experience_rows(&mut experience, &BTreeSet::from([KEEPER.to_string()]));

    let keeper = &experience["agent_sidebar"]["live_snapshot"]["agents"][0];
    assert_eq!(keeper["session_id"], "s1", "the owner's session stays");
    assert_eq!(
        keeper["progress"],
        fallback_progress(),
        "the bounded run state stays"
    );
    assert_eq!(keeper["state"], "unknown");
    assert_eq!(keeper["label"], KEEPER);
    assert_eq!(keeper["labels"], json!({"agent_identity": KEEPER}));
    assert_eq!(keeper["wired_to"], json!([]));
    assert_eq!(keeper["addressable"], false);
    for withheld in [
        "role",
        "group",
        "model_capabilities",
        "watched",
        "alertLevel",
        "degraded",
        "degradedReason",
    ] {
        assert!(
            keeper.get(withheld).is_none(),
            "{withheld} is withheld: {keeper}"
        );
    }
    assert_eq!(keeper["affordances"]["addressable"], false);
    assert_eq!(keeper["affordances"]["can_send_message"], false);
    assert_eq!(keeper["affordances"]["can_retire"], false);
    assert_eq!(
        keeper["affordances"]["can_respawn"], true,
        "respawn resolves its target live"
    );
    assert_eq!(keeper["unobserved"]["reason"], "refresh_pending");
    assert!(
        keeper["unobserved"]["fields"]
            .as_array()
            .unwrap()
            .contains(&json!("labels"))
    );

    let keeper_status = &experience["identity_status"]["rows"][0];
    assert_eq!(keeper_status["session_id"], "s1");
    assert_eq!(keeper_status["state"], "unknown");
    assert_eq!(keeper_status["addressability"], "internal_only");
    assert_eq!(keeper_status["labels"], json!({"agent_identity": KEEPER}));
    for withheld in ["role", "display_name", "model_capabilities"] {
        assert!(
            keeper_status.get(withheld).is_none(),
            "{withheld} is withheld: {keeper_status}"
        );
    }
    assert_eq!(keeper_status["unobserved"]["reason"], "refresh_pending");

    assert_eq!(
        experience["agent_sidebar"]["live_snapshot"]["agents"][1],
        untouched_other
    );
    assert_eq!(
        experience["identity_status"]["rows"][1],
        untouched_other_status
    );
    assert_eq!(
        experience["runtime_capabilities"]["profile_capabilities"],
        json!({"worker": {"instance_count": 1, "addressable": true, "has_wiring": false}}),
        "the pending row's walked profile no longer counts"
    );

    // Nothing pending: the experience is untouched.
    let mut unchanged = experience_with_rows();
    withhold_pending_experience_rows(&mut unchanged, &BTreeSet::new());
    assert_eq!(unchanged, experience_with_rows());
}

fn spec(identity: &str, labels: BTreeMap<String, String>) -> DurableAgentSpec {
    DurableAgentSpec {
        identity: AgentIdentity::parse(identity).unwrap(),
        profile: "worker".into(),
        addressability: AgentAddressability::Addressable,
        display_name: None,
        labels,
        context: None,
        additional_instructions: Vec::new(),
        initial_message: None,
        runtime_mode_override: Some(meerkat_mob::MobRuntimeMode::AutonomousHost),
        backend: None,
        binding: None,
        placement: None,
    }
}

async fn identity_first_runtime(
    keeper_labels: BTreeMap<String, String>,
) -> (UnifiedRuntime, tempfile::TempDir) {
    let scratch = tempfile::tempdir().unwrap();
    let definition = MobDefinition::from_toml(&format!(
        r#"
[mob]
id = "console-binding-{}"
[profiles.worker]
model = "gpt-5.5"
runtime_mode = "autonomous_host"
external_addressable = true
[profiles.worker.tools]
comms = true
"#,
        uuid::Uuid::new_v4()
    ))
    .unwrap();
    let roster = Arc::new(MutableRosterProvider::new(vec![
        spec(KEEPER, keeper_labels),
        spec(OTHER, BTreeMap::new()),
    ]));
    let runtime = UnifiedRuntime::builder()
        .definition(definition)
        .continuity_store(Arc::new(LocalContinuityStore::in_memory().unwrap()))
        .lease_provider(Arc::new(LocalLeaseProvider::new()))
        .roster_provider(roster)
        .scratch_dir(scratch.path())
        .identity_runtime_instance_id("console-binding")
        .default_llm_client(Arc::new(NeverCalledClient))
        .build()
        .await
        .unwrap();
    (runtime, scratch)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_binding_change_past_the_refresh_deadline_answers_pending_without_predecessor_facts() {
    let (runtime, _scratch) = identity_first_runtime(BTreeMap::from([(
        "group".to_string(),
        "keepers".to_string(),
    )]))
    .await;
    let mob_runtime = runtime.mob_runtime().clone();
    let identity_runtime = runtime.identity_runtime().unwrap();
    let identity_runtime = identity_runtime.runtime();
    let read_model = ConsoleSnapshotReadModel::default();
    let policy = AllowAllConsoleVisibilityPolicy;

    // A primed read model, as a console that was open before the reset.
    let (snapshot, pending) = build_live_snapshot(
        &mob_runtime,
        &[],
        None,
        &policy,
        &read_model,
        Some(identity_runtime),
    )
    .await;
    assert!(pending.is_empty());
    let predecessor = member(&snapshot, KEEPER).unwrap().clone();
    let other_before = member(&snapshot, OTHER).unwrap().clone();

    // Every walk from here on waits for the gate.
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    read_model.set_walk_gate(Some(Arc::clone(&gate)));
    let successor = identity_runtime
        .reset(&AgentIdentity::parse(KEEPER).unwrap())
        .await
        .unwrap();

    let started = tokio::time::Instant::now();
    let (snapshot, pending) = build_live_snapshot(
        &mob_runtime,
        &[],
        None,
        &policy,
        &read_model,
        Some(identity_runtime),
    )
    .await;
    let waited = started.elapsed();
    assert!(
        waited >= CONSOLE_BINDING_REFRESH_DEADLINE
            && waited < CONSOLE_BINDING_REFRESH_DEADLINE + Duration::from_secs(2),
        "the read waits for the held walk until the deadline, then answers: {waited:?}"
    );
    assert_eq!(pending, BTreeSet::from([KEEPER.to_string()]));
    let keeper = member(&snapshot, KEEPER).unwrap();
    assert_eq!(
        keeper.session_id.as_deref(),
        Some(successor.session_id.to_string().as_str()),
        "the owner's session, never the walk's"
    );
    assert_ne!(keeper.session_id, predecessor.session_id);
    assert_ne!(
        keeper.progress, predecessor.progress,
        "the predecessor's progress is never served for the successor"
    );
    assert_eq!(
        keeper.progress.as_ref().unwrap()["last_progress_event"],
        "unchanged"
    );
    assert_eq!(
        member(&snapshot, OTHER).unwrap(),
        &other_before,
        "an unchanged binding answers from the walk"
    );

    // The held walk lands once released; the next read is the fresh walk.
    gate.add_permits(64);
    let (snapshot, pending) = build_live_snapshot(
        &mob_runtime,
        &[],
        None,
        &policy,
        &read_model,
        Some(identity_runtime),
    )
    .await;
    assert!(pending.is_empty());
    let keeper = member(&snapshot, KEEPER).unwrap();
    assert_eq!(keeper.state, "active");
    assert_eq!(
        keeper.session_id.as_deref(),
        Some(successor.session_id.to_string().as_str())
    );
    assert_eq!(
        keeper.labels.get("group").map(String::as_str),
        Some("keepers")
    );
    runtime.mob_handle().stop().await.unwrap();
}

fn member<'a>(
    snapshot: &'a crate::runtime::ConsoleLiveSnapshot,
    roster_id: &str,
) -> Option<&'a ConsoleMember> {
    snapshot
        .members
        .iter()
        .find(|member| member.agent_identity == roster_id)
}

fn open_console_decisions() -> crate::RuntimeDecisionState {
    crate::build_runtime_decision_state(crate::RuntimeDecisionInputs {
        bigquery: crate::BigQueryNaming {
            dataset: "console_binding".to_string(),
            table: "events".to_string(),
        },
        trusted_meta_toml: "modules = []\n".to_string(),
        auth: crate::AuthPolicy {
            default_provider: crate::AuthProvider::GoogleOAuth,
            email_allowlist: vec![],
        },
        trusted_oidc: crate::TrustedOidcRuntimeConfig {
            discovery_json:
                r#"{"issuer":"https://trusted.meta.local","jwks_uri":"https://trusted.meta.local/.well-known/jwks.json"}"#
                    .to_string(),
            jwks_json: r#"{"keys":[{"kid":"kid-current","kty":"oct","alg":"HS256","k":"cGhhc2U3LXRydXN0ZWQtY3VycmVudC1zZWNyZXQ"}]}"#
                .to_string(),
            audience: "meerkat-console".to_string(),
            require_verified_email: false,
        },
        console: crate::ConsolePolicy {
            require_app_auth: false,
            ..crate::ConsolePolicy::default()
        },
        ops: crate::RuntimeOpsPolicy::default(),
        release_metadata_json: include_str!("../assets/release-targets.json").to_string(),
    })
    .expect("decision state builds")
}

fn experience_identities(
    snapshot: &crate::runtime::ConsoleLiveSnapshot,
    controller: &AccessController,
) -> (Vec<String>, Vec<String>) {
    let response = crate::runtime::handle_console_rest_json_route_with_snapshot_and_access(
        &open_console_decisions(),
        &ConsoleRestJsonRequest {
            method: "GET".to_string(),
            path: "/console/experience".to_string(),
            auth: None,
        },
        Some(snapshot),
        Some(controller),
    );
    assert_eq!(response.status, 200, "{}", response.body);
    let names = |pointer: &str| {
        response
            .body
            .pointer(pointer)
            .and_then(Value::as_array)
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| row["identity"].as_str().map(str::to_string))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };
    (
        names("/agent_sidebar/live_snapshot/agents"),
        names("/identity_status/rows"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_label_scoped_deny_still_hides_a_member_answered_pending() {
    let (runtime, _scratch) = identity_first_runtime(BTreeMap::from([(
        "tier".to_string(),
        "restricted".to_string(),
    )]))
    .await;
    let mob_runtime = runtime.mob_runtime().clone();
    let identity_runtime = runtime.identity_runtime().unwrap();
    let identity_runtime = identity_runtime.runtime();
    let read_model = ConsoleSnapshotReadModel::default();
    let policy = AllowAllConsoleVisibilityPolicy;
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![
            AccessRule {
                id: "view-all".to_string(),
                actions: vec!["agent.view".to_string()],
                agents: vec!["*".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "deny-restricted".to_string(),
                effect: AccessEffect::Deny,
                actions: vec!["agent.view".to_string()],
                match_labels: BTreeMap::from([("tier".to_string(), "restricted".to_string())]),
                ..AccessRule::default()
            },
        ],
        ..AccessControlConfig::default()
    })
    .expect("access controller");

    let (snapshot, _) = build_live_snapshot(
        &mob_runtime,
        &[],
        None,
        &policy,
        &read_model,
        Some(identity_runtime),
    )
    .await;
    assert_eq!(
        experience_identities(&snapshot, &controller),
        (vec![OTHER.to_string()], vec![OTHER.to_string()]),
        "the deny hides the restricted keeper before the reset"
    );

    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    read_model.set_walk_gate(Some(Arc::clone(&gate)));
    identity_runtime
        .reset(&AgentIdentity::parse(KEEPER).unwrap())
        .await
        .unwrap();
    let (snapshot, pending) = build_live_snapshot(
        &mob_runtime,
        &[],
        None,
        &policy,
        &read_model,
        Some(identity_runtime),
    )
    .await;
    assert_eq!(pending, BTreeSet::from([KEEPER.to_string()]));
    assert_eq!(
        experience_identities(&snapshot, &controller),
        (vec![OTHER.to_string()], vec![OTHER.to_string()]),
        "access decides on the last observed labels while the row is pending"
    );
    gate.add_permits(64);
    runtime.mob_handle().stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_join_binds_the_runtimes_own_identities_under_a_namespaced_aggregator() {
    let (runtime, _scratch) = identity_first_runtime(BTreeMap::new()).await;
    let mob_runtime = runtime.mob_runtime().clone();
    let identity_runtime = runtime.identity_runtime().unwrap();
    let identity_runtime = identity_runtime.runtime();
    let aggregator = MetaConsoleAggregator::in_memory();
    aggregator.register_runtime_handles_with_policy(
        "default",
        "ns",
        mob_runtime.clone(),
        Some(Arc::clone(identity_runtime)),
        runtime.console_events(),
        Arc::new(AllowAllConsoleVisibilityPolicy),
    );
    let read_model = ConsoleSnapshotReadModel::default();
    let policy = AllowAllConsoleVisibilityPolicy;
    build_live_snapshot(
        &mob_runtime,
        &[],
        None,
        &policy,
        &read_model,
        Some(identity_runtime),
    )
    .await;

    let successor = identity_runtime
        .reset(&AgentIdentity::parse(KEEPER).unwrap())
        .await
        .unwrap();
    let records = aggregator.list_identities().await.unwrap();
    let record = records
        .iter()
        .find(|record| record.identity == format!("ns/{KEEPER}"))
        .unwrap_or_else(|| panic!("the aggregator lists the namespaced record: {records:?}"));
    assert_eq!(
        record.session_id.as_deref(),
        Some(successor.session_id.to_string().as_str())
    );
    let (snapshot, pending) = build_live_snapshot(
        &mob_runtime,
        &[],
        None,
        &policy,
        &read_model,
        Some(identity_runtime),
    )
    .await;
    assert!(pending.is_empty());
    assert_eq!(
        member(&snapshot, KEEPER).unwrap().session_id.as_deref(),
        Some(successor.session_id.to_string().as_str()),
        "the experience binds its bare row to the same session"
    );
    runtime.mob_handle().stop().await.unwrap();
}
