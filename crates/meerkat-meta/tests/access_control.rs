//! End-to-end tests for the optional ABAC access-control layer:
//! per-principal console experience filtering, RPC enforcement, the
//! live admin surface, and SSE route gating.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use futures::StreamExt;
use meerkat::{AgentFactory, Config, build_ephemeral_service};
use meerkat_client::TestClient;
use meerkat_core::types::HandlingMode;
// meerkat 0.7: the MeerkatId alias was deleted; member ids are AgentIdentity.
use meerkat_meta::runtime::ConsoleMember;
use meerkat_meta::{
    AccessControlConfig, AccessController, AccessGroup, AccessRule, AgentResourceAttributes,
    AuthPolicy, BigQueryNaming, ConsoleAccessRequest, ConsoleLiveSnapshot,
    ConsoleModelCapabilities, ConsolePolicy, ConsoleRestJsonRequest, ConsoleVisibilityPolicy,
    DiscoverySpec, MetaConfig, MobBootstrapOptions, MobBootstrapSpec, RuntimeDecisionInputs,
    RuntimeOpsPolicy, TopologyControlMode, TopologyControlPolicy, TrustedOidcRuntimeConfig,
    UnifiedRuntime, build_runtime_decision_state,
    handle_console_rest_json_route_with_snapshot_and_access,
};
use meerkat_meta::{StewardStore, TaintableStore};
use meerkat_mob::event::MobEventKind;
use meerkat_mob::ids::AgentIdentity as MeerkatId;
use meerkat_mob::{MobDefinition, MobStorage, SpawnMemberSpec};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

fn trusted_toml() -> String {
    r#"
[[modules]]
id = "router"
command = "router-bin"
args = []
restart_policy = "always"
"#
    .to_string()
}

fn trusted_oidc() -> TrustedOidcRuntimeConfig {
    TrustedOidcRuntimeConfig {
        discovery_json:
            r#"{"issuer":"https://trusted.meta.local","jwks_uri":"https://trusted.meta.local/.well-known/jwks.json"}"#
                .to_string(),
        jwks_json: r#"{"keys":[{"kid":"kid-current","kty":"oct","alg":"HS256","k":"cGhhc2U3LXRydXN0ZWQtY3VycmVudC1zZWNyZXQ"}]}"#
            .to_string(),
        audience: "meerkat-console".to_string(),
        require_verified_email: false,
    }
}

fn decision_state(require_app_auth: bool) -> meerkat_meta::RuntimeDecisionState {
    build_runtime_decision_state(RuntimeDecisionInputs {
        bigquery: BigQueryNaming {
            dataset: "access_dataset".to_string(),
            table: "access_table".to_string(),
        },
        trusted_meta_toml: trusted_toml(),
        auth: AuthPolicy {
            default_provider: meerkat_meta::AuthProvider::GoogleOAuth,
            email_allowlist: vec![
                "root@example.test".to_string(),
                "alice@example.test".to_string(),
                // Authenticated + allowlisted but granted nothing by the rules,
                // for the deny-by-default "outsider sees an empty console" case.
                "carol@example.test".to_string(),
            ],
        },
        trusted_oidc: trusted_oidc(),
        console: ConsolePolicy {
            require_app_auth,
            ..ConsolePolicy::default()
        },
        ops: RuntimeOpsPolicy::default(),
        release_metadata_json: include_str!("../assets/release-targets.json").to_string(),
    })
    .expect("decision state builds")
}

fn member(identity: &str, role: &str, labels: &[(&str, &str)]) -> ConsoleMember {
    ConsoleMember {
        agent_identity: identity.to_string(),
        role: role.to_string(),
        state: "active".to_string(),
        model_capabilities: ConsoleModelCapabilities::default(),
        runtime_mode: None,
        session_id: None,
        wired_to: Vec::new(),
        labels: labels
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
        progress: None,
    }
}

fn snapshot_with_members(members: Vec<ConsoleMember>) -> ConsoleLiveSnapshot {
    ConsoleLiveSnapshot::new(
        Some("access-test-runtime".to_string()),
        true,
        Vec::new(),
        Vec::new(),
        members,
        true,
    )
}

/// "Ops can see all agents but only interact with ops-lead" — the canonical
/// scenario, plus an admin who sees and can do everything.
fn ops_access_config() -> AccessControlConfig {
    AccessControlConfig {
        approvers: Default::default(),
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        groups: BTreeMap::from([(
            "ops".to_string(),
            AccessGroup {
                description: Some("Operations".to_string()),
                members: vec!["alice@example.test".to_string()],
            },
        )]),
        rules: vec![
            AccessRule {
                id: "ops-view-all".to_string(),
                groups: vec!["ops".to_string()],
                actions: vec!["agent.view".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "ops-send-lead".to_string(),
                groups: vec!["ops".to_string()],
                actions: vec!["agent.send".to_string()],
                agents: vec!["ops-lead".to_string()],
                ..AccessRule::default()
            },
        ],
    }
}

fn experience_for(
    controller: &AccessController,
    subject: &str,
    snapshot: &ConsoleLiveSnapshot,
) -> Value {
    let decisions = decision_state(true);
    let response = handle_console_rest_json_route_with_snapshot_and_access(
        &decisions,
        &ConsoleRestJsonRequest {
            method: "GET".to_string(),
            path: "/console/experience".to_string(),
            auth: Some(ConsoleAccessRequest {
                provider: meerkat_meta::AuthProvider::GoogleOAuth,
                email: subject.to_string(),
            }),
        },
        Some(snapshot),
        Some(controller),
    );
    assert_eq!(response.status, 200, "experience: {:?}", response.body);
    response.body
}

fn sidebar_identities(experience: &Value) -> Vec<String> {
    experience["agent_sidebar"]["live_snapshot"]["agents"]
        .as_array()
        .expect("sidebar agents")
        .iter()
        .map(|agent| {
            // Member rows carry `identity`; module-fallback rows carry only
            // `agent_id`. Use whichever identifies the row.
            agent["identity"]
                .as_str()
                .filter(|value| !value.is_empty())
                .or_else(|| agent["agent_id"].as_str())
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

#[test]
fn experience_is_filtered_per_principal() {
    let controller = AccessController::new(ops_access_config()).expect("controller");
    let snapshot = snapshot_with_members(vec![
        member("ops-lead", "lead", &[]),
        member("scout-1", "scout", &[]),
    ]);

    // Admin sees everything with full affordances.
    let admin_experience = experience_for(&controller, "root@example.test", &snapshot);
    assert_eq!(
        sidebar_identities(&admin_experience),
        ["ops-lead", "scout-1"]
    );
    assert_eq!(admin_experience["access"]["can_administer"], json!(true));
    assert_eq!(admin_experience["access"]["enabled"], json!(true));

    // Ops member sees both agents, can send only to ops-lead.
    let ops_experience = experience_for(&controller, "alice@example.test", &snapshot);
    assert_eq!(sidebar_identities(&ops_experience), ["ops-lead", "scout-1"]);
    let agents = ops_experience["agent_sidebar"]["live_snapshot"]["agents"]
        .as_array()
        .expect("agents");
    let affordance = |identity: &str, key: &str| -> bool {
        agents
            .iter()
            .find(|agent| agent["identity"] == identity)
            .and_then(|agent| agent["affordances"][key].as_bool())
            .unwrap_or(false)
    };
    assert!(affordance("ops-lead", "can_send_message"));
    assert!(!affordance("scout-1", "can_send_message"));
    assert!(!affordance("scout-1", "can_retire"));
    assert_eq!(ops_experience["access"]["can_administer"], json!(false));
    assert_eq!(
        ops_experience["access"]["groups"],
        json!(["ops"]),
        "groups surface in the access section"
    );
    assert_eq!(
        ops_experience["runtime_capabilities"]["can_send_messages"],
        json!(true)
    );
    assert_eq!(
        ops_experience["runtime_capabilities"]["can_spawn_members"],
        json!(false)
    );

    // An authenticated subject that no rule grants sees an empty console,
    // even though the config has allow rules for others (deny-by-default).
    let outsider_experience = experience_for(&controller, "carol@example.test", &snapshot);
    assert_eq!(
        sidebar_identities(&outsider_experience),
        Vec::<String>::new(),
        "an ungranted authenticated subject sees no agents"
    );
    assert_eq!(
        outsider_experience["access"]["can_administer"],
        json!(false)
    );
    let decisions = decision_state(true);
    let response = handle_console_rest_json_route_with_snapshot_and_access(
        &decisions,
        &ConsoleRestJsonRequest {
            method: "GET".to_string(),
            path: "/console/experience".to_string(),
            auth: Some(ConsoleAccessRequest {
                provider: meerkat_meta::AuthProvider::GoogleOAuth,
                email: "alice@example.test".to_string(),
            }),
        },
        Some(&snapshot_with_members(vec![member(
            "hidden-only",
            "lead",
            &[],
        )])),
        Some(
            &AccessController::new(AccessControlConfig {
                enabled: true,
                admins: vec!["root@example.test".to_string()],
                ..AccessControlConfig::default()
            })
            .expect("deny-all controller"),
        ),
    );
    assert_eq!(
        sidebar_identities(&response.body),
        Vec::<String>::new(),
        "deny-by-default hides every agent"
    );
}

#[test]
fn module_fallback_rows_are_filtered_for_denied_callers() {
    // When every roster member is filtered out, the sidebar falls back to
    // module-agent rows built from `loaded_modules`; those must be gated
    // by `agent.view` like any other agent row.
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        ..AccessControlConfig::default()
    })
    .expect("deny-all controller");
    let snapshot = ConsoleLiveSnapshot::new(
        Some("access-test-runtime".to_string()),
        true,
        vec!["router".to_string()],
        Vec::new(),
        Vec::new(),
        false,
    );
    let denied = experience_for(&controller, "alice@example.test", &snapshot);
    assert_eq!(
        sidebar_identities(&denied),
        Vec::<String>::new(),
        "module fallback rows must not leak to denied callers"
    );
    let admin = experience_for(&controller, "root@example.test", &snapshot);
    assert_eq!(sidebar_identities(&admin).len(), 1, "admin keeps modules");

    // Partial grant: with two module rows and a rule granting view of only
    // one, the sidebar must show exactly that one (not all-or-nothing).
    let partial = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![AccessRule {
            id: "carol-views-router".to_string(),
            subjects: vec!["carol@example.test".to_string()],
            actions: vec!["agent.view".to_string()],
            agents: vec!["router".to_string()],
            ..AccessRule::default()
        }],
        ..AccessControlConfig::default()
    })
    .expect("partial controller");
    let two_modules = ConsoleLiveSnapshot::new(
        Some("access-test-runtime".to_string()),
        true,
        vec!["router".to_string(), "delivery".to_string()],
        Vec::new(),
        Vec::new(),
        false,
    );
    let scoped = experience_for(&partial, "carol@example.test", &two_modules);
    assert_eq!(
        sidebar_identities(&scoped),
        ["router"],
        "only the granted module row is visible"
    );
}

#[test]
fn label_selector_rules_filter_experience() {
    let config = AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![AccessRule {
            id: "payments-only".to_string(),
            subjects: vec!["alice@example.test".to_string()],
            actions: vec!["agent.view".to_string()],
            match_labels: BTreeMap::from([("org".to_string(), "payments".to_string())]),
            ..AccessRule::default()
        }],
        ..AccessControlConfig::default()
    };
    let controller = AccessController::new(config).expect("controller");
    let snapshot = snapshot_with_members(vec![
        member("pay-analyst", "analyst", &[("org", "payments")]),
        member("hr-analyst", "analyst", &[("org", "people")]),
    ]);
    let experience = experience_for(&controller, "alice@example.test", &snapshot);
    assert_eq!(sidebar_identities(&experience), ["pay-analyst"]);
}

#[test]
fn identity_first_console_identity_drives_filtering() {
    // Identity-first: the console identity (labels.agent_identity) differs
    // from the runtime member id (member.agent_identity). Rules written in
    // console-identity terms must filter correctly, and the runtime member id
    // must resolve back via the agent_id fallback — neither direction may
    // leak the agent the caller is not granted.
    let config = AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![
            AccessRule {
                id: "alice-views-lead-by-identity".to_string(),
                subjects: vec!["alice@example.test".to_string()],
                actions: vec!["agent.view".to_string()],
                agents: vec!["identity:ops-lead".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "alice-sends-lead-by-runtime-id".to_string(),
                subjects: vec!["alice@example.test".to_string()],
                actions: vec!["agent.send".to_string()],
                // Written against the runtime member id, not the console identity.
                agents: vec!["member-runtime-7".to_string()],
                ..AccessRule::default()
            },
        ],
        ..AccessControlConfig::default()
    };
    let controller = AccessController::new(config).expect("controller");
    let snapshot = snapshot_with_members(vec![
        // Console identity (label) != runtime member id (agent_identity).
        member(
            "member-runtime-7",
            "lead",
            &[("agent_identity", "identity:ops-lead")],
        ),
        member(
            "member-runtime-8",
            "scout",
            &[("agent_identity", "identity:scout-9")],
        ),
    ]);
    let experience = experience_for(&controller, "alice@example.test", &snapshot);
    // Only the granted agent appears, keyed by its console identity.
    assert_eq!(sidebar_identities(&experience), ["identity:ops-lead"]);
    let agents = experience["agent_sidebar"]["live_snapshot"]["agents"]
        .as_array()
        .expect("agents");
    let lead = agents
        .iter()
        .find(|agent| agent["identity"] == "identity:ops-lead")
        .expect("lead row");
    // The send grant was written against the runtime member id; it must still
    // resolve via the agent_id fallback.
    assert_eq!(lead["affordances"]["can_send_message"], json!(true));
}

#[test]
fn disabled_controller_changes_nothing() {
    let controller = AccessController::disabled();
    let snapshot = snapshot_with_members(vec![
        member("ops-lead", "lead", &[]),
        member("scout-1", "scout", &[]),
    ]);
    let experience = experience_for(&controller, "alice@example.test", &snapshot);
    assert_eq!(sidebar_identities(&experience), ["ops-lead", "scout-1"]);
    assert_eq!(experience["access"]["enabled"], json!(false));
    // A disabled controller that names no admin grants administration to
    // nobody.
    assert_eq!(experience["access"]["can_administer"], json!(false));
}

// ---------------------------------------------------------------------------
// Full HTTP router enforcement (anonymous principal, require_app_auth=false)
// ---------------------------------------------------------------------------

async fn build_access_runtime_fixture() -> (tempfile::TempDir, UnifiedRuntime) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let session_path = temp_dir.path().join("sessions");
    std::fs::create_dir_all(&session_path).expect("session path");
    let factory = AgentFactory::new(&session_path).comms(true);
    let session_service = Arc::new(build_ephemeral_service(factory, Config::default(), 16));
    // Per-runtime mob id: 0.8.23's fail-closed in-proc registration means
    // concurrently running tests must not share a supervisor route.
    static NEXT_ACCESS_MOB: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let definition = MobDefinition::from_toml(&format!(
        r#"
[mob]
id = "access-control-mob-{}"

[profiles.lead]
model = "gpt-5.5"
external_addressable = true

[profiles.lead.tools]
comms = true
"#,
        NEXT_ACCESS_MOB.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
    .expect("definition");
    let mob_spec = MobBootstrapSpec::new(definition, MobStorage::in_memory(), session_service)
        .with_options(MobBootstrapOptions {
            allow_ephemeral_sessions: true,
            notify_orchestrator_on_resume: true,
            default_llm_client: Some(Arc::new(TestClient::default())),
        });
    let module_config = MetaConfig {
        modules: vec![],
        discovery: DiscoverySpec {
            namespace: "access-control".to_string(),
            modules: vec![],
        },
        pre_spawn: vec![],
    };
    let runtime = UnifiedRuntime::bootstrap(mob_spec, module_config, Duration::from_secs(2))
        .await
        .expect("bootstrap runtime");
    for member_id in ["router", "delivery"] {
        runtime
            .spawn(SpawnMemberSpec::from_wire(
                "lead".to_string(),
                MeerkatId::from(member_id).to_string(),
                Some(format!("You are {member_id}.").into()),
                None,
                None,
            ))
            .await
            .expect("spawn member");
    }
    (temp_dir, runtime)
}

async fn rpc(app: &axum::Router, method: &str, params: Value) -> Value {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": "test",
        "method": method,
        "params": params,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/console/rpc")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(payload.to_string()))
                .expect("rpc request"),
        )
        .await
        .expect("rpc response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("rpc body");
    serde_json::from_slice(&body).expect("rpc json")
}

async fn get_status(app: &axum::Router, uri: &str) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response")
        .status()
}

/// Everyone may view agents and nobody holds a memory grant. The config
/// names a memory action (for an unrelated agent), so it is taken literally:
/// the view rule does not also gain `agent.memory.read`, as it would in a
/// memory-naive config (see recall_read_action_migration_compat_rule_both_ways).
fn view_only_memory_aware_controller() -> AccessController {
    AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![
            AccessRule {
                id: "everyone-views".to_string(),
                actions: vec!["agent.view".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "unrelated-memory-rule".to_string(),
                actions: vec!["agent.memory.read".to_string()],
                agents: vec!["someone-else".to_string()],
                ..AccessRule::default()
            },
        ],
        ..AccessControlConfig::default()
    })
    .expect("controller")
}

/// The anonymous console caller under `controller` views `identity` but holds
/// neither memory read on it nor quarantine review, by the controller's own
/// decision and on the actual RPC path.
async fn assert_view_only_reader(
    app: &axum::Router,
    controller: &AccessController,
    identity: &str,
) {
    let reader = controller.view_for_subject(None);
    assert!(reader.can_view_agent(identity));
    assert!(!reader.allows_agent("agent.memory.read", identity));
    assert!(!reader.allows("memory.quarantine.review"));
    let recall = rpc(
        app,
        "meta/agent_memory/recall",
        json!({ "identity": identity, "selection": "always" }),
    )
    .await;
    assert_eq!(recall["error"]["code"], json!(-32030), "{recall:#?}");
    assert_eq!(
        recall["error"]["data"]["action"],
        json!("agent.memory.read")
    );
}

/// The first frame of `kind` on `/console/timeline/stream`, read from the
/// stream as it arrives. The deadline only bounds a broken run.
async fn timeline_stream_frame(app: &axum::Router, kind: &str) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/console/timeline/stream")
                .body(Body::empty())
                .expect("stream request"),
        )
        .await
        .expect("stream response");
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let mut buffered = String::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(chunk) = body.next().await {
            buffered.push_str(&String::from_utf8_lossy(&chunk.expect("stream chunk")));
            while let Some(end) = buffered.find("\n\n") {
                let event: String = buffered.drain(..end + 2).collect();
                let data = event
                    .lines()
                    .filter_map(|line| line.strip_prefix("data:"))
                    .map(str::trim_start)
                    .collect::<Vec<_>>()
                    .join("\n");
                if data.is_empty() {
                    continue;
                }
                let event: Value = serde_json::from_str(&data).expect("stream event json");
                if event["frame"]["kind"] == json!(kind) {
                    return event["frame"].clone();
                }
            }
        }
        panic!("the timeline stream ended before a {kind} frame");
    })
    .await
    .expect("a timeline frame within the deadline")
}

async fn retire_historical_secret_member(runtime: &UnifiedRuntime, identity: &str) -> u64 {
    let before_spawn = runtime
        .mob_handle()
        .events()
        .latest_cursor()
        .await
        .expect("latest cursor before historical member");
    let mut spec = SpawnMemberSpec::from_wire(
        "lead".to_string(),
        identity.to_string(),
        Some("historical secret member".into()),
        None,
        None,
    );
    spec.labels = Some(BTreeMap::from([("org".to_string(), "secret".to_string())]));
    runtime.spawn(spec).await.expect("spawn historical member");
    runtime
        .mob_handle()
        .retire(MeerkatId::from(identity))
        .await
        .expect("retire historical member");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let still_present = runtime
                .mob_handle()
                .list_members_including_retiring()
                .await
                .iter()
                .any(|member| member.agent_identity.as_str() == identity);
            if !still_present {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retired member should leave the operational roster");
    before_spawn
}

async fn assert_sse_replay_emits_no_data(response: axum::response::Response) {
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    match tokio::time::timeout(Duration::from_millis(200), stream.next()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(Err(error))) => panic!("unexpected SSE body error: {error}"),
        Ok(Some(Ok(bytes))) => panic!(
            "historical hidden member leaked through SSE replay: {}",
            String::from_utf8_lossy(&bytes)
        ),
    }
}

async fn assert_sse_emits_no_identity(response: axum::response::Response, identity: &str) {
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    let mut observed = String::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.next()).await {
            Err(_) | Ok(None) => break,
            Ok(Some(Err(error))) => panic!("unexpected SSE body error: {error}"),
            Ok(Some(Ok(bytes))) => {
                observed.push_str(&String::from_utf8_lossy(&bytes));
                assert!(
                    !observed.contains(identity),
                    "hidden identity {identity} leaked through SSE: {observed}"
                );
            }
        }
    }
}

async fn assert_structural_sse_excludes_and_includes_cursor(
    response: axum::response::Response,
    excluded_cursor: u64,
    included_cursor: u64,
) {
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    let excluded = format!("id: mob-evt-{excluded_cursor}");
    let included = format!("id: mob-evt-{included_cursor}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut observed = String::new();
    while !observed.contains(&included) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "expected current public cursor {included_cursor} in structural SSE: {observed}"
        );
        match tokio::time::timeout(remaining, stream.next()).await {
            Err(_) | Ok(None) => panic!(
                "expected current public cursor {included_cursor} in structural SSE: {observed}"
            ),
            Ok(Some(Err(error))) => panic!("unexpected SSE body error: {error}"),
            Ok(Some(Ok(bytes))) => {
                observed.push_str(&String::from_utf8_lossy(&bytes));
                assert!(
                    !observed.contains(&excluded),
                    "historical secret cursor {excluded_cursor} leaked through structural SSE: {observed}"
                );
            }
        }
    }
}

/// Anonymous callers (open console) only match rules with no subject
/// constraints: visible/sendable only what those rules grant.
fn anonymous_router_only_config() -> AccessControlConfig {
    AccessControlConfig {
        approvers: Default::default(),
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        groups: BTreeMap::new(),
        rules: vec![
            AccessRule {
                id: "everyone-views-router".to_string(),
                actions: vec!["agent.view".to_string()],
                agents: vec!["router".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "everyone-sends-router".to_string(),
                actions: vec!["agent.send".to_string()],
                agents: vec!["router".to_string()],
                ..AccessRule::default()
            },
        ],
    }
}

/// Gating views everyone may hold, with agent visibility limited to `router`.
fn gating_router_only_config() -> AccessControlConfig {
    let mut config = anonymous_router_only_config();
    config.rules.push(AccessRule {
        id: "everyone-views-gating".to_string(),
        actions: vec!["gating.view".to_string()],
        ..AccessRule::default()
    });
    config
}

async fn gate_with_origin(runtime: &UnifiedRuntime, identity: &str) {
    runtime
        .evaluate_gating_action_with_origin(
            serde_json::from_value::<meerkat_meta::GatingEvaluateRequest>(json!({
                "action": format!("release for {identity}"), "actor_id": "worker", "risk_tier": "r3",
            }))
            .expect("request"),
            Some(meerkat_meta::GatingOrigin {
                identity: identity.to_string(),
                conversation_id: None,
                interaction_id: None,
            }),
        )
        .await;
}

/// The cursor read applies the same origin audience as pending and audit:
/// hidden origins never appear, and the next cursor names only a visible
/// entry. A lost cursor reports a gap.
#[tokio::test]
async fn gating_audit_cursor_is_filtered_by_origin_audience() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let controller = AccessController::new(gating_router_only_config()).expect("controller");
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));

    gate_with_origin(&runtime, "router").await;
    let start = rpc(&app, "meta/gating/audit", json!({})).await;
    let cursor = start["result"]["next_cursor"].clone();
    assert!(cursor.is_string(), "{start:#}");

    gate_with_origin(&runtime, "delivery").await;
    gate_with_origin(&runtime, "router").await;
    let page = rpc(
        &app,
        "meta/gating/audit",
        json!({ "after_audit_id": cursor }),
    )
    .await;
    let entries = page["result"]["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), 1, "{page:#}");
    assert_eq!(entries[0]["detail"]["origin"]["identity"], json!("router"));
    assert_eq!(page["result"]["next_cursor"], entries[0]["audit_id"]);
    assert_eq!(page["result"]["gap"], json!(false));

    let lost = rpc(
        &app,
        "meta/gating/audit",
        json!({ "after_audit_id": "gate-audit-000009" }),
    )
    .await;
    assert_eq!(lost["result"]["gap"], json!(true));
    assert!(
        lost["result"]["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .all(|entry| entry["detail"]["origin"]["identity"] == json!("router"))
    );

    let malformed = rpc(&app, "meta/gating/audit", json!({ "after_audit_id": 7 })).await;
    assert_eq!(malformed["error"]["code"], json!(-32602), "{malformed:#}");
    let empty = rpc(&app, "meta/gating/audit", json!({ "after_audit_id": "" })).await;
    assert_eq!(empty["error"]["code"], json!(-32602), "{empty:#}");
}

async fn next_gating_frame(
    body: &mut (impl futures::Stream<Item = Result<axum::body::Bytes, axum::Error>> + Unpin),
    buffered: &mut String,
    wait: Duration,
) -> Option<String> {
    next_complete_gating_frame(body, buffered, wait)
        .await
        .and_then(|frame| {
            frame.lines().find_map(|line| {
                line.strip_prefix("event:")
                    .map(|event| event.trim().to_string())
            })
        })
}

async fn next_complete_gating_frame(
    body: &mut (impl futures::Stream<Item = Result<axum::body::Bytes, axum::Error>> + Unpin),
    buffered: &mut String,
    wait: Duration,
) -> Option<String> {
    tokio::time::timeout(wait, async {
        loop {
            if let Some(end) = buffered.find("\n\n") {
                let frame: String = buffered.drain(..end + 2).collect();
                if frame.lines().any(|line| line.starts_with("event:")) {
                    return Some(frame);
                }
                continue;
            }
            let chunk = body.next().await?;
            buffered.push_str(&String::from_utf8_lossy(&chunk.expect("stream chunk")));
        }
    })
    .await
    .ok()
    .flatten()
}

/// The live stream wakes only for entries the caller may see, carries no
/// gate data, and ends when `gating.view` is revoked.
#[tokio::test]
async fn gating_stream_wakes_for_visible_changes_and_ends_on_revocation() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let controller = AccessController::new(gating_router_only_config()).expect("controller");
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/console/gating/stream")
                .body(Body::empty())
                .expect("stream request"),
        )
        .await
        .expect("stream response");
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let mut buffered = String::new();
    let wait = Duration::from_secs(10);
    assert_eq!(
        next_gating_frame(&mut body, &mut buffered, wait)
            .await
            .as_deref(),
        Some("gating.resync_required")
    );

    gate_with_origin(&runtime, "delivery").await;
    assert_eq!(
        next_gating_frame(&mut body, &mut buffered, Duration::from_millis(500)).await,
        None,
        "a hidden origin must not wake the caller"
    );
    // A hidden burst larger than the wake channel lags the unread stream;
    // the lag itself must not produce a frame.
    for _ in 0..80 {
        gate_with_origin(&runtime, "delivery").await;
    }
    assert_eq!(
        next_gating_frame(&mut body, &mut buffered, Duration::from_millis(500)).await,
        None,
        "a lagged burst of hidden changes must not wake the caller"
    );
    gate_with_origin(&runtime, "router").await;
    assert_eq!(
        next_complete_gating_frame(&mut body, &mut buffered, wait)
            .await
            .as_deref(),
        Some("event: gating.changed\ndata: {}\n\n"),
        "a complete, data-free frame"
    );

    controller
        .replace_config(anonymous_router_only_config())
        .expect("revoke gating.view");
    gate_with_origin(&runtime, "router").await;
    assert_eq!(
        next_gating_frame(&mut body, &mut buffered, wait)
            .await
            .as_deref(),
        Some("gating.access_revoked")
    );

    let denied = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/console/gating/stream")
                .body(Body::empty())
                .expect("stream request"),
        )
        .await
        .expect("stream response");
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
}

/// A stream opened while the installed controller is disabled still follows
/// it: enabling a policy without gating.view closes the same stream.
#[tokio::test]
async fn gating_stream_follows_a_controller_enabled_after_it_opened() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let controller = AccessController::new(AccessControlConfig::default()).expect("controller");
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/console/gating/stream")
                .body(Body::empty())
                .expect("stream request"),
        )
        .await
        .expect("stream response");
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let mut buffered = String::new();
    let wait = Duration::from_secs(10);
    assert_eq!(
        next_gating_frame(&mut body, &mut buffered, wait)
            .await
            .as_deref(),
        Some("gating.resync_required")
    );

    controller
        .replace_config(anonymous_router_only_config())
        .expect("enable a policy without gating.view");
    gate_with_origin(&runtime, "router").await;
    let frame = next_gating_frame(&mut body, &mut buffered, wait).await;
    drop(body);
    runtime.shutdown().await;
    assert_eq!(
        frame.as_deref(),
        Some("gating.access_revoked"),
        "the stream must close, not report a change"
    );
}

/// A first-request gating stream must resolve live origin labels itself;
/// no experience or RPC request has primed the access controller.
#[tokio::test]
async fn gating_stream_resolves_label_deny_on_cold_cache() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    runtime
        .spawn(
            SpawnMemberSpec::from_wire(
                "lead".to_string(),
                MeerkatId::from("red-shadow").to_string(),
                Some("You are red-shadow.".into()),
                None,
                None,
            )
            .with_labels(BTreeMap::from([("team".to_string(), "red".to_string())])),
        )
        .await
        .expect("spawn labeled member");
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![
            AccessRule {
                id: "everyone-views".to_string(),
                actions: vec!["agent.view".to_string(), "gating.view".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "deny-red-team-view".to_string(),
                effect: meerkat_meta::AccessEffect::Deny,
                actions: vec!["agent.view".to_string()],
                match_labels: BTreeMap::from([("team".to_string(), "red".to_string())]),
                ..AccessRule::default()
            },
        ],
        ..AccessControlConfig::default()
    })
    .expect("controller");
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));
    assert!(
        !controller.view_for_subject(None).knows_agent("red-shadow"),
        "this first-request control requires an unprimed origin"
    );
    assert!(runtime.gating_state_snapshot().await.audit.is_empty());

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/console/gating/stream")
                .body(Body::empty())
                .expect("stream request"),
        )
        .await
        .expect("stream response");
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let mut buffered = String::new();
    let wait = Duration::from_secs(10);
    assert_eq!(
        next_complete_gating_frame(&mut body, &mut buffered, wait)
            .await
            .as_deref(),
        Some("event: gating.resync_required\ndata: {\"reason\":\"initial_sync\"}\n\n")
    );

    gate_with_origin(&runtime, "red-shadow").await;
    let hidden_audit = runtime.gating_state_snapshot().await.audit;
    assert_eq!(hidden_audit.len(), 1, "the hidden gate must actually exist");
    assert_eq!(hidden_audit[0].event_type, "pending_created");
    assert_eq!(
        hidden_audit[0].detail["origin"]["identity"],
        json!("red-shadow")
    );
    let hidden_frame =
        next_complete_gating_frame(&mut body, &mut buffered, Duration::from_millis(500)).await;

    // This gate is created only after polling the hidden wake. It proves the
    // same stream remains useful, rather than passing by closing or dropping all wakes.
    gate_with_origin(&runtime, "router").await;
    let visible_audit = runtime.gating_state_snapshot().await.audit;
    assert_eq!(visible_audit.len(), 2);
    assert_eq!(visible_audit[1].event_type, "pending_created");
    assert_eq!(
        visible_audit[1].detail["origin"]["identity"],
        json!("router")
    );
    let visible_frame = next_complete_gating_frame(&mut body, &mut buffered, wait).await;
    drop(body);
    runtime.shutdown().await;

    assert_eq!(
        hidden_frame, None,
        "a live label-hidden origin must not wake a cold gating-only connection"
    );
    assert_eq!(
        visible_frame.as_deref(),
        Some("event: gating.changed\ndata: {}\n\n"),
        "the visible sibling must yield a complete data-free frame"
    );
}

#[tokio::test]
async fn http_router_enforces_access_end_to_end() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let controller = AccessController::new(anonymous_router_only_config()).expect("controller");
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));

    // Experience: only the granted agent is projected.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/console/experience")
                .body(Body::empty())
                .expect("experience request"),
        )
        .await
        .expect("experience response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("experience body");
    let experience: Value = serde_json::from_slice(&body).expect("experience json");
    assert_eq!(sidebar_identities(&experience), ["router"]);
    assert_eq!(experience["access"]["enabled"], json!(true));

    // list_members is filtered to visible agents.
    let members = rpc(&app, "meta/list_members", json!({})).await;
    let member_rows = members["result"].as_array().expect("members array");
    assert_eq!(member_rows.len(), 1, "members: {member_rows:#?}");
    // Wire-contract pin: the published SDKs index `state` on member rows
    // (Python MemberSnapshot.from_dict does `data["state"]`), so the meerkat
    // 0.7 `status` projection must keep emitting the `state` key in the
    // console state vocabulary.
    assert_eq!(
        member_rows[0]["state"],
        json!("active"),
        "member rows must carry the `state` wire key: {member_rows:#?}"
    );

    // Sending to a hidden agent is denied with the typed access error.
    let denied = rpc(
        &app,
        "meta/console/send",
        json!({
            "identity": "delivery",
            "content": "hello",
            "origin": "test",
            "idempotency_key": "denied-send",
        }),
    )
    .await;
    assert_eq!(
        denied["error"]["code"],
        json!(-32030),
        "denied: {denied:#?}"
    );
    assert_eq!(denied["error"]["data"]["kind"], json!("access_denied"));

    // Sending to the granted agent passes the access gate.
    let allowed = rpc(
        &app,
        "meta/console/send",
        json!({
            "identity": "router",
            "content": "hello",
            "origin": "test",
            "idempotency_key": "allowed-send",
        }),
    )
    .await;
    assert_ne!(
        allowed["error"]["code"],
        json!(-32030),
        "allowed send must not be access-denied: {allowed:#?}"
    );

    // Lifecycle and admin-tier methods are deny-by-default.
    let retire = rpc(&app, "meta/retire", json!({ "identity": "router" })).await;
    assert_eq!(retire["error"]["code"], json!(-32030));
    let labels = rpc(
        &app,
        "meta/mob_labels/set",
        json!({ "labels": { "a": "b" } }),
    )
    .await;
    assert_eq!(labels["error"]["code"], json!(-32030));

    // Plumbing and flow-state reads that enumerate identities without
    // per-agent filtering are gated behind their operating tiers.
    for method in [
        "meta/routing/routes/list",
        "meta/delivery/history",
        "meta/cross_mob/directory",
        "meta/mob_labels/get",
        "meta/list_runs",
        "meta/list_flows",
    ] {
        let denied = rpc(&app, method, json!({})).await;
        assert_eq!(
            denied["error"]["code"],
            json!(-32030),
            "{method} must be access-gated: {denied:#?}"
        );
    }

    // Anonymous callers are not access admins while admins are configured.
    let status = rpc(&app, "meta/access/status", json!({})).await;
    assert_eq!(status["result"]["available"], json!(true));
    assert_eq!(status["result"]["enabled"], json!(true));
    assert_eq!(status["result"]["can_administer"], json!(false));
    let get_config = rpc(&app, "meta/access/get", json!({})).await;
    assert_eq!(get_config["error"]["code"], json!(-32030));

    // SSE gating: granted agent streams, hidden agent is 403, mob-wide
    // streams require the mob.observe grant.
    assert_eq!(
        get_status(&app, "/agents/router/events").await,
        StatusCode::OK
    );
    assert_eq!(
        get_status(&app, "/agents/delivery/events").await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(get_status(&app, "/mob/events").await, StatusCode::FORBIDDEN);
    assert_eq!(
        get_status(&app, "/meta/mob_events/stream").await,
        StatusCode::FORBIDDEN
    );

    // Live reconfiguration: granting view of "delivery" shows up on the
    // next request without any restart.
    controller
        .upsert_rule(AccessRule {
            id: "everyone-views-delivery".to_string(),
            actions: vec!["agent.view".to_string()],
            agents: vec!["delivery".to_string()],
            ..AccessRule::default()
        })
        .expect("live rule update");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/console/experience")
                .body(Body::empty())
                .expect("experience request"),
        )
        .await
        .expect("experience response");
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("experience body");
    let experience: Value = serde_json::from_slice(&body).expect("experience json");
    assert_eq!(sidebar_identities(&experience), ["delivery", "router"]);
    assert_eq!(
        get_status(&app, "/agents/delivery/events").await,
        StatusCode::OK
    );

    let _ = runtime.mob_handle().stop().await;
}

#[tokio::test]
async fn structural_sse_replay_fails_closed_when_historical_agent_attributes_are_unknown() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let identity = "historical-secret";
    let after_seq = retire_historical_secret_member(&runtime, identity).await;
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![
            AccessRule {
                id: "observe-mob".to_string(),
                actions: vec!["mob.observe".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "view-all".to_string(),
                actions: vec!["agent.view".to_string()],
                agents: vec!["*".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "deny-secret-label".to_string(),
                effect: meerkat_meta::AccessEffect::Deny,
                actions: vec!["agent.view".to_string()],
                match_labels: BTreeMap::from([("org".to_string(), "secret".to_string())]),
                ..AccessRule::default()
            },
        ],
        ..AccessControlConfig::default()
    })
    .expect("access controller");
    assert!(
        !controller.view_for_subject(None).knows_agent(identity),
        "the replay regression requires a genuinely cold historical identity"
    );
    runtime.set_access_controller(controller);
    let app = runtime.build_reference_app_router(decision_state(false));
    for method in ["meta/mob_events/query", "meta/mob_events/subscribe"] {
        let page = rpc(
            &app,
            method,
            json!({ "after_seq": after_seq, "identity": identity }),
        )
        .await;
        assert_eq!(page["error"], Value::Null, "{method}: {page:#?}");
        assert!(
            page["result"]["events"]
                .as_array()
                .is_some_and(Vec::is_empty),
            "cold historical attributes must fail closed in {method}: {page:#?}"
        );
    }
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/meta/mob_events/stream?after_seq={after_seq}&identity={identity}"
                ))
                .body(Body::empty())
                .expect("historical structural SSE request"),
        )
        .await
        .expect("historical structural SSE response");
    assert_sse_replay_emits_no_data(response).await;
    runtime.shutdown().await;
}

#[tokio::test]
async fn structural_sse_replay_fails_closed_without_live_visibility_projection() {
    #[derive(Debug)]
    struct HideHistoricalSecret;

    impl ConsoleVisibilityPolicy for HideHistoricalSecret {
        fn member_visible(&self, member: &ConsoleMember) -> bool {
            member.agent_identity != "historical-secret"
        }
    }

    let (_temp_dir, runtime) = build_access_runtime_fixture().await;
    let identity = "historical-secret";
    let after_seq = retire_historical_secret_member(&runtime, identity).await;
    let app = runtime.build_reference_app_router_with_console_visibility_policy(
        decision_state(false),
        Arc::new(HideHistoricalSecret),
    );
    for method in ["meta/mob_events/query", "meta/mob_events/subscribe"] {
        let page = rpc(
            &app,
            method,
            json!({ "after_seq": after_seq, "identity": identity }),
        )
        .await;
        assert_eq!(page["error"], Value::Null, "{method}: {page:#?}");
        assert!(
            page["result"]["events"]
                .as_array()
                .is_some_and(Vec::is_empty),
            "missing live visibility projection must fail closed in {method}: {page:#?}"
        );
    }
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/meta/mob_events/stream?after_seq={after_seq}&identity={identity}"
                ))
                .body(Body::empty())
                .expect("historical visibility SSE request"),
        )
        .await
        .expect("historical visibility SSE response");
    assert_sse_replay_emits_no_data(response).await;
    runtime.shutdown().await;
}

#[tokio::test]
async fn long_lived_sse_reauthorizes_stale_alias_when_new_generation_appears() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let identity = "label-transition";
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![
            AccessRule {
                id: "observe-mob".to_string(),
                actions: vec!["mob.observe".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "view-all".to_string(),
                actions: vec!["agent.view".to_string()],
                agents: vec!["*".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "deny-secret-label".to_string(),
                effect: meerkat_meta::AccessEffect::Deny,
                actions: vec!["agent.view".to_string()],
                match_labels: BTreeMap::from([("org".to_string(), "secret".to_string())]),
                ..AccessRule::default()
            },
        ],
        ..AccessControlConfig::default()
    })
    .expect("access controller");
    // Model the additive cache entry left by a prior embodiment of this alias.
    // The new live generation below has different labels; a long-lived stream
    // must not treat this known-but-stale entry as current authority.
    controller.record_agent_attributes(AgentResourceAttributes {
        identity: identity.to_string(),
        agent_id: Some(identity.to_string()),
        role: Some("lead".to_string()),
        labels: BTreeMap::from([("org".to_string(), "public".to_string())]),
    });
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));
    assert!(controller.view_for_subject(None).knows_agent(identity));
    let after_seq = runtime
        .mob_handle()
        .events()
        .latest_cursor()
        .await
        .expect("cursor before new generation");

    // Open both long-lived routes while the alias is absent. Their initial
    // prime therefore cannot overwrite the cached public labels.
    let mob_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mob/events")
                .body(Body::empty())
                .expect("mob SSE request"),
        )
        .await
        .expect("mob SSE response");
    let structural_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/meta/mob_events/stream?after_seq={after_seq}&identity={identity}"
                ))
                .body(Body::empty())
                .expect("structural SSE request"),
        )
        .await
        .expect("structural SSE response");

    let mut secret_spec = SpawnMemberSpec::from_wire(
        "lead".to_string(),
        identity.to_string(),
        Some("secret member".into()),
        None,
        None,
    );
    secret_spec.labels = Some(BTreeMap::from([("org".to_string(), "secret".to_string())]));
    runtime
        .spawn(secret_spec)
        .await
        .expect("spawn secret member");
    let member_id = MeerkatId::from(identity);
    let mut proof_stream = runtime
        .mob_handle()
        .subscribe_agent_events(&member_id)
        .await
        .expect("proof event stream");
    runtime
        .mob_handle()
        .member(&member_id)
        .await
        .expect("secret member handle")
        .send("emit a test event", HandlingMode::Queue)
        .await
        .expect("secret member turn");
    tokio::time::timeout(Duration::from_secs(2), proof_stream.next())
        .await
        .expect("secret member should emit an event")
        .expect("proof stream should remain open");

    assert_sse_emits_no_identity(mob_response, identity).await;
    assert_sse_emits_no_identity(structural_response, identity).await;
    runtime.shutdown().await;
}

#[tokio::test]
async fn event_surfaces_do_not_reauthorize_secret_generation_as_public_same_alias() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let identity = "secret-then-public";
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![
            AccessRule {
                id: "observe-mob".to_string(),
                actions: vec!["mob.observe".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "view-all".to_string(),
                actions: vec!["agent.view".to_string()],
                agents: vec!["*".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "deny-secret-label".to_string(),
                effect: meerkat_meta::AccessEffect::Deny,
                actions: vec!["agent.view".to_string()],
                match_labels: BTreeMap::from([("org".to_string(), "secret".to_string())]),
                ..AccessRule::default()
            },
        ],
        ..AccessControlConfig::default()
    })
    .expect("access controller");
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));
    let events_view = runtime.mob_handle().events();
    let before_secret = events_view
        .latest_cursor()
        .await
        .expect("cursor before secret generation");

    let mut secret_spec = SpawnMemberSpec::from_wire(
        "lead".to_string(),
        identity.to_string(),
        Some("secret generation".into()),
        None,
        None,
    );
    secret_spec.labels = Some(BTreeMap::from([("org".to_string(), "secret".to_string())]));
    runtime
        .spawn(secret_spec)
        .await
        .expect("spawn secret generation");
    let secret_spawn_cursor = events_view
        .poll_strict(before_secret, 128)
        .await
        .expect("secret spawn events")
        .into_iter()
        .find_map(|event| match event.kind {
            MobEventKind::MemberSpawned(spawned) if spawned.agent_identity.as_str() == identity => {
                Some(event.cursor)
            }
            _ => None,
        })
        .expect("secret member_spawned cursor");

    // Subscribe while the secret incarnation is current, then leave the body
    // unpolled so its attributed event remains queued until after the alias is
    // rebound to a public incarnation.
    let mob_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mob/events")
                .body(Body::empty())
                .expect("mob SSE request"),
        )
        .await
        .expect("mob SSE response");
    let member_id = MeerkatId::from(identity);
    let mut proof_stream = runtime
        .mob_handle()
        .subscribe_agent_events(&member_id)
        .await
        .expect("secret proof event stream");
    runtime
        .mob_handle()
        .member(&member_id)
        .await
        .expect("secret member handle")
        .send("emit a secret event", HandlingMode::Queue)
        .await
        .expect("secret member turn");
    tokio::time::timeout(Duration::from_secs(2), proof_stream.next())
        .await
        .expect("secret member should emit an event")
        .expect("proof stream should remain open");

    let before_public = events_view
        .latest_cursor()
        .await
        .expect("cursor before public alias projection");
    // A second exact member binding projects to the same durable console
    // alias. Its raw id sorts after the first in the roster projection, so the
    // legacy alias-keyed ABAC cache sees this public row as current. Protected
    // event surfaces must nevertheless authorize each event by runtime+fence.
    let public_runtime_identity = "zz-public-incarnation";
    let mut public_spec = SpawnMemberSpec::from_wire(
        "lead".to_string(),
        public_runtime_identity.to_string(),
        Some("public alias incarnation".into()),
        None,
        None,
    );
    public_spec.labels = Some(BTreeMap::from([
        ("agent_identity".to_string(), identity.to_string()),
        ("org".to_string(), "public".to_string()),
    ]));
    runtime
        .mob_handle()
        .spawn_spec(public_spec)
        .await
        .expect("spawn exact public binding for the reused alias");
    let public_spawn_cursor = events_view
        .poll_strict(before_public, 128)
        .await
        .expect("public alias events")
        .into_iter()
        .find_map(|event| match event.kind {
            MobEventKind::MemberSpawned(spawned)
                if spawned.agent_identity.as_str() == public_runtime_identity =>
            {
                Some(event.cursor)
            }
            _ => None,
        })
        .expect("public alias member_spawned cursor");

    for method in ["meta/mob_events/query", "meta/mob_events/subscribe"] {
        let page = rpc(
            &app,
            method,
            json!({
                "after_seq": before_secret,
                "event_types": ["member_spawned"],
                "limit": 1,
            }),
        )
        .await;
        assert_eq!(page["error"], Value::Null, "{method}: {page:#?}");
        let cursors = page["result"]["events"]
            .as_array()
            .expect("event array")
            .iter()
            .filter_map(|event| event["cursor"].as_u64())
            .collect::<Vec<_>>();
        assert!(
            !cursors.contains(&secret_spawn_cursor),
            "historical secret generation leaked through {method}: {page:#?}"
        );
        assert!(
            cursors.contains(&public_spawn_cursor),
            "current public binding should remain visible in {method}: {page:#?}"
        );
        assert_eq!(
            cursors.len(),
            1,
            "authorization-denied rows must not consume the visible page limit: {page:#?}"
        );
        assert_eq!(page["result"]["next_after_seq"], public_spawn_cursor);
    }

    let structural_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/meta/mob_events/stream?after_seq={before_secret}"))
                .body(Body::empty())
                .expect("structural SSE request"),
        )
        .await
        .expect("structural SSE response");
    assert_structural_sse_excludes_and_includes_cursor(
        structural_response,
        secret_spawn_cursor,
        public_spawn_cursor,
    )
    .await;

    // Reverse the starvation order as well: a newer hidden generation must
    // not consume a backwards snapshot's limit and hide the earlier public
    // generation. Empty forward pages still advance over the hidden raw row,
    // and subscribe resumes from that frontier without losing the next public
    // event.
    let before_latest_secret = events_view
        .latest_cursor()
        .await
        .expect("cursor before latest secret alias projection");
    let latest_secret_runtime_identity = "zzz-secret-incarnation";
    let mut latest_secret_spec = SpawnMemberSpec::from_wire(
        "lead".to_string(),
        latest_secret_runtime_identity.to_string(),
        Some("latest secret alias incarnation".into()),
        None,
        None,
    );
    latest_secret_spec.labels = Some(BTreeMap::from([
        ("agent_identity".to_string(), identity.to_string()),
        ("org".to_string(), "secret".to_string()),
    ]));
    runtime
        .mob_handle()
        .spawn_spec(latest_secret_spec)
        .await
        .expect("spawn latest exact secret binding");
    let latest_secret_cursor = events_view
        .poll_strict(before_latest_secret, 128)
        .await
        .expect("latest secret alias events")
        .into_iter()
        .find_map(|event| match event.kind {
            MobEventKind::MemberSpawned(spawned)
                if spawned.agent_identity.as_str() == latest_secret_runtime_identity =>
            {
                Some(event.cursor)
            }
            _ => None,
        })
        .expect("latest secret alias member_spawned cursor");
    let latest_secret_frontier = events_view
        .latest_cursor()
        .await
        .expect("raw frontier after latest secret alias projection");

    // The raw ledger keeps moving underneath these snapshots: every spawned
    // member's kickoff objective appends structural lifecycle rows on its own
    // schedule (observed live under suite load: `MemberKickoffUpdated
    // { phase: Started }` for "zz-public-incarnation" landing one row past
    // the captured frontier). Those rows are not `member_spawned` events, so
    // they can never enter the filtered pages below - but they DO advance
    // the raw frontier, which makes frontier EQUALITY a race against the
    // kickoff tasks. Assert the real properties instead: the reported
    // frontier advanced over the hidden raw row (>= the captured frontier)
    // and is an honest ledger position (<= the raw frontier read after the
    // call).
    let backward = rpc(
        &app,
        "meta/mob_events/query",
        json!({ "event_types": ["member_spawned"], "limit": 1 }),
    )
    .await;
    assert_eq!(backward["error"], Value::Null, "{backward:#?}");
    assert_eq!(
        backward["result"]["events"][0]["cursor"],
        public_spawn_cursor
    );
    assert_eq!(backward["result"]["events"].as_array().unwrap().len(), 1);
    let backward_next = backward["result"]["next_after_seq"]
        .as_u64()
        .expect("backward next_after_seq");
    let raw_frontier_after_backward = events_view
        .latest_cursor()
        .await
        .expect("raw frontier after backward query");
    assert!(
        backward_next >= latest_secret_frontier && backward_next <= raw_frontier_after_backward,
        "backwards snapshot must expose the raw ledger frontier (captured \
         {latest_secret_frontier}, reported {backward_next}, raw now \
         {raw_frontier_after_backward})"
    );

    let empty_subscribe = rpc(
        &app,
        "meta/mob_events/subscribe",
        json!({
            "after_seq": public_spawn_cursor,
            "event_types": ["member_spawned"],
            "limit": 1,
        }),
    )
    .await;
    assert_eq!(
        empty_subscribe["error"],
        Value::Null,
        "{empty_subscribe:#?}"
    );
    assert!(
        empty_subscribe["result"]["events"]
            .as_array()
            .is_some_and(Vec::is_empty),
        "hidden-only snapshot should remain empty: {empty_subscribe:#?}"
    );
    // Same bounded frontier contract as the backwards snapshot above: the
    // empty page must have advanced OVER the hidden latest-secret row, and
    // late kickoff-lifecycle rows (`MemberKickoffUpdated { phase: Started }`
    // from an earlier spawn's async kickoff) may already sit past the
    // captured frontier.
    let empty_next = empty_subscribe["result"]["next_after_seq"]
        .as_u64()
        .expect("empty subscribe next_after_seq");
    let raw_frontier_after_subscribe = events_view
        .latest_cursor()
        .await
        .expect("raw frontier after empty subscribe");
    assert!(
        empty_next >= latest_secret_frontier && empty_next <= raw_frontier_after_subscribe,
        "empty page must advance over the hidden raw row to a real ledger \
         frontier (captured {latest_secret_frontier}, reported {empty_next}, \
         raw now {raw_frontier_after_subscribe})"
    );

    let before_next_public = events_view
        .latest_cursor()
        .await
        .expect("cursor before next public alias projection");
    let next_public_runtime_identity = "zzzz-public-incarnation";
    let mut next_public_spec = SpawnMemberSpec::from_wire(
        "lead".to_string(),
        next_public_runtime_identity.to_string(),
        Some("next public alias incarnation".into()),
        None,
        None,
    );
    next_public_spec.labels = Some(BTreeMap::from([
        ("agent_identity".to_string(), identity.to_string()),
        ("org".to_string(), "public".to_string()),
    ]));
    runtime
        .mob_handle()
        .spawn_spec(next_public_spec)
        .await
        .expect("spawn next exact public binding");
    let next_public_cursor = events_view
        .poll_strict(before_next_public, 128)
        .await
        .expect("next public alias events")
        .into_iter()
        .find_map(|event| match event.kind {
            MobEventKind::MemberSpawned(spawned)
                if spawned.agent_identity.as_str() == next_public_runtime_identity =>
            {
                Some(event.cursor)
            }
            _ => None,
        })
        .expect("next public alias member_spawned cursor");
    let subscribe_url = empty_subscribe["result"]["subscribe_url"]
        .as_str()
        .expect("subscribe URL");
    let resumed_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(subscribe_url)
                .body(Body::empty())
                .expect("resumed structural SSE request"),
        )
        .await
        .expect("resumed structural SSE response");
    assert_structural_sse_excludes_and_includes_cursor(
        resumed_response,
        latest_secret_cursor,
        next_public_cursor,
    )
    .await;
    assert_sse_emits_no_identity(mob_response, identity).await;

    runtime.shutdown().await;
}

#[tokio::test]
async fn public_member_ingress_rejects_encoded_roster_ids_before_abac_and_resolution() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![
            AccessRule {
                id: "everyone-views".to_string(),
                actions: vec!["agent.view".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "deny-delivery-view".to_string(),
                effect: meerkat_meta::AccessEffect::Deny,
                actions: vec!["agent.view".to_string()],
                agents: vec!["delivery".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "everyone-spawns".to_string(),
                actions: vec!["agent.spawn".to_string()],
                ..AccessRule::default()
            },
        ],
        ..AccessControlConfig::default()
    })
    .expect("controller");
    runtime.set_access_controller(controller);
    let app = runtime.build_reference_app_router(decision_state(false));

    // Pre-fix, ABAC evaluated this raw spelling (matching the broad allow but
    // not the targeted deny), then member resolution decoded it to
    // `delivery`. Public ingress must reject before either step.
    let encoded_rpc = rpc(
        &app,
        "meta/get_member",
        json!({ "member_id": "mk--delivery" }),
    )
    .await;
    assert_eq!(encoded_rpc["error"]["code"], json!(-32602));
    assert!(
        encoded_rpc["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("encoded roster-id")),
        "{encoded_rpc:#?}"
    );

    assert_eq!(
        get_status(&app, "/agents/mk--delivery/events").await,
        StatusCode::BAD_REQUEST,
        "per-agent SSE must reject encoded roster ids before its agent.view decision"
    );
    assert_eq!(
        get_status(&app, "/agents/%20delivery%20/events").await,
        StatusCode::FORBIDDEN,
        "per-agent SSE must trim the public alias before its targeted agent.view decision"
    );

    let forged_label = rpc(
        &app,
        "meta/ensure_member",
        json!({
            "role": "lead",
            "agent_identity": "raw-imposter",
            "labels": { "agent_identity": "delivery" },
        }),
    )
    .await;
    assert_eq!(forged_label["error"]["code"], json!(-32602));
    assert!(
        forged_label["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("runtime-authoritative")),
        "{forged_label:#?}"
    );

    #[derive(Debug)]
    struct HideDelivery;

    impl ConsoleVisibilityPolicy for HideDelivery {
        fn member_visible(&self, member: &ConsoleMember) -> bool {
            member.agent_identity != "delivery"
        }
    }

    let hidden_app = runtime.build_reference_app_router_with_console_visibility_policy(
        decision_state(false),
        Arc::new(HideDelivery),
    );
    assert_eq!(
        get_status(&hidden_app, "/agents/delivery/events").await,
        StatusCode::NOT_FOUND,
        "a visibility-hidden agent must be rejected before SSE subscription"
    );

    let _ = runtime.mob_handle().stop().await;
}

#[tokio::test]
async fn topology_plan_and_audit_capabilities_follow_endpoint_scoped_grants() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    runtime
        .set_topology_control_policy(TopologyControlPolicy {
            mode: TopologyControlMode::ReadOnly,
            ..TopologyControlPolicy::default()
        })
        .expect("read-only topology policy");
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![AccessRule {
            id: "anonymous-topology-view".to_string(),
            actions: vec!["agent.view".to_string(), "topology.view".to_string()],
            agents: vec!["*".to_string()],
            ..AccessRule::default()
        }],
        ..AccessControlConfig::default()
    })
    .expect("topology controller");
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));

    let capabilities = rpc(&app, "meta/capabilities", json!({})).await;
    let methods = capabilities["result"]["methods"]
        .as_array()
        .expect("capability methods");
    assert!(methods.iter().any(|method| method == "meta/topology/query"));
    assert!(
        !methods.iter().any(|method| method == "meta/topology/plan"),
        "view-only callers must not be offered a planning oracle: {capabilities:#?}"
    );
    assert!(
        !methods
            .iter()
            .any(|method| method == "meta/topology/audit/query"),
        "audit is an independent sensitive permission: {capabilities:#?}"
    );

    let query = rpc(&app, "meta/topology/query", json!({})).await;
    let revision = query["result"]["revision"]
        .as_u64()
        .expect("topology revision");
    let connect = json!({
        "expected_revision": revision,
        "operations": [{
            "action": "connect",
            "edge": {
                "a": {"identity": "router"},
                "b": {"identity": "delivery"}
            }
        }]
    });
    let denied_without_action = rpc(&app, "meta/topology/plan", connect.clone()).await;
    assert_eq!(denied_without_action["error"]["code"], json!(-32030));
    assert_eq!(
        denied_without_action["error"]["data"]["action"],
        json!("topology.connect")
    );
    let denied_audit = rpc(&app, "meta/topology/audit/query", json!({})).await;
    assert_eq!(denied_audit["error"]["code"], json!(-32030));
    assert_eq!(
        denied_audit["error"]["data"]["action"],
        json!("topology.audit")
    );

    // A broad capability flag is not enough: granting connect only on one
    // endpoint advertises planning but the concrete pair still fails closed.
    controller
        .upsert_rule(AccessRule {
            id: "connect-router-only".to_string(),
            actions: vec!["topology.connect".to_string()],
            agents: vec!["router".to_string()],
            ..AccessRule::default()
        })
        .expect("live connect grant");
    let capabilities = rpc(&app, "meta/capabilities", json!({})).await;
    assert!(
        capabilities["result"]["methods"]
            .as_array()
            .expect("capability methods")
            .iter()
            .any(|method| method == "meta/topology/plan")
    );
    let denied_other_endpoint = rpc(&app, "meta/topology/plan", connect).await;
    assert_eq!(denied_other_endpoint["error"]["code"], json!(-32030));
    assert_eq!(
        denied_other_endpoint["error"]["data"]["resource"],
        json!("delivery")
    );

    let _ = runtime.mob_handle().stop().await;
}

#[tokio::test]
async fn multipart_send_denied_does_not_write_blob_before_access_gate() {
    // Regression: the multipart `meta/console/send` path must run the
    // `agent.send` ABAC gate on the target identity BEFORE externalizing /
    // persisting uploaded image bytes. Otherwise a caller denied send to an
    // identity can still write attacker-supplied bytes into that identity's
    // blob store (a pre-auth side effect / storage-amplification vector).
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let controller = AccessController::new(anonymous_router_only_config()).expect("controller");
    runtime.set_access_controller(controller);
    let app = runtime.build_reference_app_router(decision_state(false));

    // `delivery` is NOT granted agent.send (only `router` is).
    let image_bytes = b"denied-multipart-png-bytes";
    let boundary = "meta-access-boundary";
    let payload = json!({
        "jsonrpc": "2.0",
        "id": "denied-multipart-send",
        "method": "meta/console/send",
        "params": {
            "identity": "delivery",
            "origin": "test",
            "idempotency_key": "denied-multipart-send",
            "content": [
                { "type": "image_upload", "upload_id": "u1", "media_type": "image/png" }
            ]
        }
    });
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(b"Content-Disposition: form-data; name=\"payload\"\r\n");
    body.extend_from_slice(b"Content-Type: application/json\r\n\r\n");
    body.extend_from_slice(payload.to_string().as_bytes());
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"file:u1\"; filename=\"x.png\"\r\n",
    );
    body.extend_from_slice(b"Content-Type: image/png\r\n\r\n");
    body.extend_from_slice(image_bytes);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/console/rpc/multipart")
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .expect("multipart request"),
        )
        .await
        .expect("multipart response");
    assert_eq!(response.status(), StatusCode::OK);
    let resp_body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("multipart body");
    let resp_json: Value = serde_json::from_slice(&resp_body).expect("multipart json");
    assert_eq!(
        resp_json["error"]["code"],
        json!(-32030),
        "denied multipart send must be access-denied: {resp_json:#?}"
    );
    assert_eq!(resp_json["error"]["data"]["kind"], json!("access_denied"));

    // The uploaded bytes must NOT have been written: the content-addressed
    // blob is not retrievable (the gate fired before externalization). The
    // blob id hashes `media_type || 0x00 || bytes` (see compute_blob_id).
    let mut hasher = Sha256::new();
    hasher.update(b"image/png");
    hasher.update([0]);
    hasher.update(image_bytes);
    let blob_id = format!("sha256:{:x}", hasher.finalize());
    let blob_status = get_status(&app, &format!("/blobs/{blob_id}")).await;
    assert_eq!(
        blob_status,
        StatusCode::NOT_FOUND,
        "denied upload bytes must not be persisted before the access gate"
    );

    let _ = runtime.mob_handle().stop().await;
}

/// A new deployment's controller names no admin, so no console caller can
/// configure it: authentication is not administration. Trusted local setup
/// names the first admin (see the checked HTTP controls in the controller).
#[tokio::test]
async fn a_fresh_console_cannot_configure_access_without_a_named_admin() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    // Fresh deployment: controller present, disabled, no admins.
    let controller = AccessController::disabled();
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));

    let status = rpc(&app, "meta/access/status", json!({})).await;
    assert_eq!(status["result"]["enabled"], json!(false));
    assert_eq!(status["result"]["can_administer"], json!(false));

    // The console caller can neither read the configuration nor appoint an
    // admin.
    let set = rpc(
        &app,
        "meta/access/set",
        json!({ "config": {
            "enabled": true,
            "admins": ["root@example.test"],
            "rules": [
                { "id": "everyone-views-router", "actions": ["agent.view"], "agents": ["router"] }
            ],
        }}),
    )
    .await;
    assert_eq!(set["error"]["code"], json!(-32030), "set: {set:#?}");
    let get_config = rpc(&app, "meta/access/get", json!({})).await;
    assert_eq!(get_config["error"]["code"], json!(-32030));
    let (config, revision) = controller.snapshot();
    assert_eq!(revision, 0, "nothing was written");
    assert_eq!(*config, AccessControlConfig::default());

    // Enabling without admins is rejected (anti-lockout).
    assert!(
        controller
            .replace_config(AccessControlConfig {
                enabled: true,
                ..AccessControlConfig::default()
            })
            .is_err()
    );

    let _ = runtime.mob_handle().stop().await;
}

/// A label-keyed rule must resolve on the timeline read path even when the
/// caller never hit `/console/experience` first — the handler primes the
/// attribute cache from the roster itself. Guards the seam-priming fix
/// (shared by the windowed REST handler, the SSE timeline stream, and the
/// RPC/SSE event surfaces): without priming, the label rule would fail closed
/// and the labelled agent's frames would wrongly vanish.
#[tokio::test]
async fn timeline_role_rules_resolve_without_prior_experience() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    // Everyone may view + send agents whose role is "lead" (the fixture
    // members' profile). Role is an attribute that only resolves through the
    // primed cache — an identity-only surface can't see it otherwise.
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![AccessRule {
            id: "lead-role-only".to_string(),
            actions: vec!["agent.view".to_string(), "agent.send".to_string()],
            roles: vec!["lead".to_string()],
            ..AccessRule::default()
        }],
        ..AccessControlConfig::default()
    })
    .expect("controller");
    runtime.set_access_controller(controller);
    let app = runtime.build_reference_app_router(decision_state(false));

    // The send gate itself resolves the role only because the RPC seam primes
    // the cache — without priming this would fail closed (`-32030`) and no
    // frame would exist.
    let send = rpc(
        &app,
        "meta/console/send",
        json!({
            "identity": "router",
            "content": "ping",
            "origin": "test",
            "idempotency_key": "role-cold-send",
        }),
    )
    .await;
    assert_eq!(
        send["error"],
        Value::Null,
        "role-keyed send must resolve cold: {send:#?}"
    );

    // Query the timeline cold (no prior /console/experience). The per-frame
    // `agent.view` filter resolves the role only because the read path primes;
    // without priming the role allow would fail closed and the frame would
    // wrongly vanish.
    // Querying the timeline cold likewise resolves the role attribute through
    // the read-path priming (the response is access-denied only if priming is
    // missing).
    let page = rpc(
        &app,
        "meta/console/query_timeline",
        json!({ "mode": "recent", "limit": 200 }),
    )
    .await;
    assert_eq!(
        page["error"],
        Value::Null,
        "cold timeline query must succeed: {page:#?}"
    );
    // Any frames returned belong only to role-matched agents.
    if let Some(frames) = page["result"]["frames"].as_array() {
        assert!(
            frames
                .iter()
                .all(|frame| frame["identity"] == json!("router")),
            "only role-matched agents are visible: {frames:#?}"
        );
    }

    // The SSE timeline stream is reachable for the caller and primes the same
    // cache before its per-frame filter.
    assert_eq!(
        get_status(&app, "/console/timeline/stream").await,
        StatusCode::OK
    );

    let _ = runtime.mob_handle().stop().await;
}

/// Regression: REST `/console/send` evaluates the `agent.send` decision
/// before any cache-warming request (experience/timeline/RPC) has run, so
/// the handler must prime the attribute cache itself. Without priming, a
/// label-scoped deny rule does not match on a cold cache (the decision
/// degrades to a bare-identity resource) and a scripted caller whose FIRST
/// request is `/console/send` reaches a member the rule was meant to
/// exclude.
#[tokio::test]
async fn rest_console_send_resolves_label_deny_on_cold_cache() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    // A member the deny rule is scoped to by label.
    runtime
        .spawn(
            SpawnMemberSpec::from_wire(
                "lead".to_string(),
                MeerkatId::from("red-shadow").to_string(),
                Some("You are red-shadow.".into()),
                None,
                None,
            )
            .with_labels(BTreeMap::from([("team".to_string(), "red".to_string())])),
        )
        .await
        .expect("spawn labeled member");
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![
            AccessRule {
                id: "everyone-sends".to_string(),
                actions: vec!["agent.send".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "deny-red-team-send".to_string(),
                effect: meerkat_meta::AccessEffect::Deny,
                actions: vec!["agent.send".to_string()],
                match_labels: BTreeMap::from([("team".to_string(), "red".to_string())]),
                ..AccessRule::default()
            },
        ],
        ..AccessControlConfig::default()
    })
    .expect("controller");
    runtime.set_access_controller(controller);
    let app = runtime.build_reference_app_router(decision_state(false));

    let rest_send = |identity: &'static str, idempotency_key: &'static str| {
        let app = app.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/console/send")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "identity": identity,
                            "content": "hello",
                            "origin": "test",
                            "idempotency_key": idempotency_key,
                        })
                        .to_string(),
                    ))
                    .expect("send request"),
            )
            .await
            .expect("send response")
            .status()
        }
    };

    // FIRST request of the process: the label-scoped deny must already
    // resolve — without handler-level priming this read 200.
    assert_eq!(
        rest_send("red-shadow", "cold-deny-send").await,
        StatusCode::FORBIDDEN,
        "label-scoped deny must resolve on a cold cache"
    );

    // Members outside the deny scope still pass the same gate cold.
    assert_ne!(
        rest_send("router", "cold-allow-send").await,
        StatusCode::FORBIDDEN,
        "non-matching members must keep passing the send gate"
    );

    let _ = runtime.mob_handle().stop().await;
}

#[tokio::test]
async fn timeline_rpc_is_filtered_per_caller() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let controller = AccessController::new(anonymous_router_only_config()).expect("controller");
    runtime.set_access_controller(controller);
    let app = runtime.build_reference_app_router(decision_state(false));

    // Seed timeline frames for both identities through console sends.
    // The send to "delivery" is access-denied, so only "router" frames
    // can exist; query to prove filtering.
    let _ = rpc(
        &app,
        "meta/console/send",
        json!({
            "identity": "router",
            "content": "ping",
            "origin": "test",
            "idempotency_key": "timeline-send",
        }),
    )
    .await;

    let page = rpc(
        &app,
        "meta/console/query_timeline",
        json!({ "mode": "recent", "limit": 200 }),
    )
    .await;
    let frames = page["result"]["frames"].as_array().expect("frames");
    assert!(
        frames
            .iter()
            .all(|frame| frame["identity"] == json!("router")),
        "timeline must only contain visible identities: {frames:#?}"
    );

    let _ = runtime.mob_handle().stop().await;
}

/// §9.3 memory.* events with no affected identity land on the timeline
/// attributed to `_system`. The aggregator exempts that identity from the
/// per-roster visibility gate (it has no roster record), so the ABAC
/// boundary for those frames is the RPC layer's
/// `retain_visible_timeline_frames`: an unscoped `agent.view` grant sees
/// them, while a grant scoped to specific agents must not.
#[tokio::test]
async fn system_memory_frames_respect_timeline_access_filtering() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    runtime.memory_event_sink().emit(
        meerkat_meta::memory::events::MemoryTimelineEvent::DreamCompleted {
            realm: "default".to_string(),
            run_id: "run-acl-dream-1".to_string(),
            ops_committed: 1,
            detail: json!({ "phase": "test" }),
        },
    );

    let system_frames = |page: &Value| -> Vec<Value> {
        page["result"]["frames"]
            .as_array()
            .expect("frames")
            .iter()
            .filter(|frame| frame["identity"] == json!("_system"))
            .cloned()
            .collect()
    };

    // Unscoped viewer first: proves the `_system` frame actually reaches
    // the timeline (so the scoped assertion below cannot pass vacuously).
    // The sink append + aggregator projection are async — poll briefly.
    let open_controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![AccessRule {
            id: "everyone-views-everything".to_string(),
            actions: vec!["agent.view".to_string()],
            ..AccessRule::default()
        }],
        ..AccessControlConfig::default()
    })
    .expect("open controller");
    runtime.set_access_controller(open_controller);
    let open_app = runtime.build_reference_app_router(decision_state(false));
    let mut seen = Vec::new();
    for _ in 0..80 {
        let page = rpc(
            &open_app,
            "meta/console/query_timeline",
            json!({ "mode": "recent", "limit": 200 }),
        )
        .await;
        seen = system_frames(&page);
        if !seen.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        seen.iter()
            .any(|frame| frame["kind"] == json!("memory.dream.completed")),
        "unscoped agent.view must see the _system memory event: {seen:#?}"
    );

    // Scoped viewer: agent.view limited to "router" — `_system` frames are
    // filtered per-caller even though they are aggregator-visible.
    let scoped_controller =
        AccessController::new(anonymous_router_only_config()).expect("scoped controller");
    runtime.set_access_controller(scoped_controller);
    let scoped_app = runtime.build_reference_app_router(decision_state(false));
    let page = rpc(
        &scoped_app,
        "meta/console/query_timeline",
        json!({ "mode": "recent", "limit": 200 }),
    )
    .await;
    assert!(
        system_frames(&page).is_empty(),
        "agent-scoped viewers must not see _system frames: {page:#?}"
    );

    let _ = runtime.mob_handle().stop().await;
}

/// `mob.observe` opens the whole-mob event surface, but per-agent
/// `agent.view` still filters which agents' events flow through it — a
/// mob.observe grant must not reveal the events/lifecycle of an agent the
/// caller is denied `agent.view` on.
#[tokio::test]
async fn mob_observe_does_not_bypass_per_agent_view() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    // Anonymous callers may observe the mob and view "router" only.
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![
            AccessRule {
                id: "everyone-observes".to_string(),
                actions: vec!["mob.observe".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "everyone-views-router".to_string(),
                actions: vec!["agent.view".to_string()],
                agents: vec!["router".to_string()],
                ..AccessRule::default()
            },
        ],
        ..AccessControlConfig::default()
    })
    .expect("controller");
    runtime.set_access_controller(controller);
    let app = runtime.build_reference_app_router(decision_state(false));

    // The mob.observe gate opens the surface (not a -32030 denial)...
    let page = rpc(&app, "meta/mob_events/query", json!({})).await;
    assert_eq!(
        page["error"],
        Value::Null,
        "observe surface open: {page:#?}"
    );
    let events = page["result"]["events"].as_array().expect("events");
    // ...but every agent-attributed event belongs to the one viewable agent.
    // "delivery" was spawned in the fixture; its lifecycle must not leak.
    assert!(
        events.iter().all(|event| {
            event["agent_identity"].is_null() || event["agent_identity"] == json!("router")
        }),
        "mob.observe must not surface denied agents' events: {events:#?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event["agent_identity"] == json!("router")),
        "router's own ledger entries should still be visible: {events:#?}"
    );

    let _ = runtime.mob_handle().stop().await;
}

// ---------------------------------------------------------------------------
// §10.3 memory read actions + §9.3 console Memory panel
// ---------------------------------------------------------------------------

/// Seed a bundled store with one record per scope, a supersede chain, a
/// quarantined record, an injection-ledger row, one steward dream's audit
/// rows, and two gated promotions parked in the quarantine queue
/// (`gate-mob-promotion` targeting mob scope, `gate-delivery-promotion`
/// targeting the "delivery" identity). Returns (store, router_active_id,
/// router_quarantined_id, delivery_id, mob_record_id, operator_record_id).
async fn seeded_memory_store(
    root: &std::path::Path,
) -> (
    meerkat_meta::SqliteAgentMemoryStore,
    String,
    String,
    String,
    String,
    String,
) {
    use meerkat_meta::memory::records::{
        InjectionLogEntry, InjectionSurface, MemoryAuthor, MemoryKind,
    };
    use meerkat_meta::{
        AgentMemoryProvider, MemoryScope, NewMemoryRecord, SqliteAgentMemoryStore,
        StagedMemoryStore, StagedMutationBatch, StagedOp, TrustTier,
    };

    struct AlwaysQuarantine;
    impl meerkat_meta::memory::taint::LlmWriteGate for AlwaysQuarantine {
        fn quarantine_reason(
            &self,
            author: &MemoryAuthor,
            _kind: meerkat_meta::memory::staged::StagedBatchKind,
            _evidence: &[meerkat_meta::memory::records::EvidenceRef],
        ) -> Option<String> {
            author.is_llm().then(|| "test taint".to_string())
        }
    }

    let store = SqliteAgentMemoryStore::open(root).expect("open store");
    let record = |title: &str| NewMemoryRecord {
        kind: MemoryKind::Fact,
        title: title.to_string(),
        description: format!("{title} description"),
        body: format!("{title} body"),
        tags: Vec::new(),
        evidence: Vec::new(),
        verification: None,
    };
    let identity_scope = |identity: &str| MemoryScope::Identity {
        realm: "default".to_string(),
        identity: identity.to_string(),
    };

    let router_root = store
        .remember_authored(
            &identity_scope("router"),
            record("Router root"),
            MemoryAuthor::Operator,
        )
        .await
        .expect("router root");
    let router_tip = store
        .supersede_authored(
            &identity_scope("router"),
            &router_root.memory_id,
            record("Router tip"),
            MemoryAuthor::Operator,
        )
        .await
        .expect("router tip");
    let delivery = store
        .remember_authored(
            &identity_scope("delivery"),
            record("Delivery fact"),
            MemoryAuthor::Operator,
        )
        .await
        .expect("delivery record");
    let mob_record = store
        .remember_authored(
            &MemoryScope::Mob {
                realm: "default".to_string(),
                mob: "access-control-mob".to_string(),
            },
            record("Mob convention"),
            MemoryAuthor::Operator,
        )
        .await
        .expect("mob record");
    store
        .remember_authored(
            &MemoryScope::Realm {
                realm: "default".to_string(),
            },
            record("Realm fact"),
            MemoryAuthor::Operator,
        )
        .await
        .expect("realm record");
    // Operator scope is live (P4 provisional keying) and carries cross-mob
    // personal facts — the panel gates it on operator.memory.read.
    let operator_record = store
        .remember_authored(
            &MemoryScope::Operator {
                realm: "default".to_string(),
                operator: "op-luka".to_string(),
            },
            record("Operator preference"),
            MemoryAuthor::Operator,
        )
        .await
        .expect("operator record");

    // Quarantined write: LLM author through the installed write gate.
    store.set_llm_write_gate(Arc::new(AlwaysQuarantine));
    let quarantined = store
        .remember_authored(
            &identity_scope("router"),
            record("Router quarantined claim"),
            MemoryAuthor::Agent {
                identity: "router".to_string(),
            },
        )
        .await
        .expect("quarantined record");
    assert!(
        matches!(
            quarantined.status,
            meerkat_meta::memory::records::RecordStatus::Quarantined { .. }
        ),
        "seed record should land quarantined: {quarantined:?}"
    );

    // One injection-ledger row for the tip record.
    store
        .log_injections(
            "default",
            &[InjectionLogEntry {
                record_id: router_tip.memory_id.clone(),
                identity: "router".to_string(),
                session_key: Some("sess-1".to_string()),
                surface: InjectionSurface::Build,
                at_ms: 1,
            }],
        )
        .await
        .expect("injection row");

    // One steward dream commit → audit rows for the dreams surface.
    let token = store
        .stage(StagedMutationBatch {
            kind: meerkat_meta::memory::staged::StagedBatchKind::FreshWrite,
            realm: "default".to_string(),
            author: MemoryAuthor::Steward {
                run_id: "run-dream-1".to_string(),
            },
            ops: vec![StagedOp::Create {
                id: None,
                scope: MemoryScope::Mob {
                    realm: "default".to_string(),
                    mob: "access-control-mob".to_string(),
                },
                record: record("Dream consolidated"),
                trust: TrustTier::AgentObserved,
                derived_from: Vec::new(),
                rationale: Some("consolidated during dream".to_string()),
                created_at_ms: None,
                updated_at_ms: None,
            }],
        })
        .await
        .expect("stage dream batch");
    store.commit(token).await.expect("commit dream batch");

    // Two gated promotions parked in the queue (staged, never committed):
    // the quarantine panel gates each row on the target scope's read grant.
    let stage_promotion = |scope: MemoryScope, title: &str| {
        let store = store.clone();
        let record = record(title);
        async move {
            store
                .stage(StagedMutationBatch {
                    kind: meerkat_meta::memory::staged::StagedBatchKind::FreshWrite,
                    realm: "default".to_string(),
                    author: MemoryAuthor::Steward {
                        run_id: "run-gate-1".to_string(),
                    },
                    ops: vec![StagedOp::Create {
                        id: None,
                        scope,
                        record,
                        trust: TrustTier::AgentObserved,
                        derived_from: Vec::new(),
                        rationale: Some("gated promotion".to_string()),
                        created_at_ms: None,
                        updated_at_ms: None,
                    }],
                })
                .await
                .expect("stage promotion batch")
        }
    };
    let mob_stage = stage_promotion(
        MemoryScope::Mob {
            realm: "default".to_string(),
            mob: "access-control-mob".to_string(),
        },
        "Promoted mob claim",
    )
    .await;
    store
        .record_pending_promotion(
            "default",
            meerkat_meta::memory::PendingPromotion {
                pending_id: "gate-mob-promotion".to_string(),
                stage_token: mob_stage.token,
                record_id: quarantined.memory_id.clone(),
                scope_kind: "mob".to_string(),
                scope_key: "access-control-mob".to_string(),
                rationale: Some("steward: mob-wide convention".to_string()),
                status: "pending".to_string(),
                created_at_ms: 2,
            },
        )
        .await
        .expect("mob promotion row");
    let delivery_stage =
        stage_promotion(identity_scope("delivery"), "Promoted delivery claim").await;
    store
        .record_pending_promotion(
            "default",
            meerkat_meta::memory::PendingPromotion {
                pending_id: "gate-delivery-promotion".to_string(),
                stage_token: delivery_stage.token,
                record_id: quarantined.memory_id.clone(),
                scope_kind: "identity".to_string(),
                scope_key: "delivery".to_string(),
                rationale: Some("steward: delivery personal fact".to_string()),
                status: "pending".to_string(),
                created_at_ms: 3,
            },
        )
        .await
        .expect("delivery promotion row");

    (
        store,
        router_tip.memory_id,
        quarantined.memory_id,
        delivery.memory_id,
        mob_record.memory_id,
        operator_record.memory_id,
    )
}

#[tokio::test]
async fn memory_panel_reads_seeded_store_without_access_control() {
    let (_temp_dir, runtime) = build_access_runtime_fixture().await;
    let memory_dir = tempfile::tempdir().expect("memory dir");
    let (store, tip_id, quarantined_id, _delivery_id, _mob_id, _operator_id) =
        seeded_memory_store(memory_dir.path()).await;
    runtime.set_memory_panel_store(Arc::new(store.clone()));
    let app = runtime.build_reference_app_router(decision_state(false));

    // Capability advertisement follows the provider-dependent pattern.
    let capabilities = rpc(&app, "meta/capabilities", json!({})).await;
    let methods = capabilities["result"]["methods"]
        .as_array()
        .expect("methods");
    for method in [
        "meta/memory/panel/records",
        "meta/memory/panel/record",
        "meta/memory/panel/quarantine",
        "meta/memory/panel/dreams",
    ] {
        assert!(
            methods.iter().any(|value| value == method),
            "{method} must be advertised: {methods:#?}"
        );
    }

    // Records: every scope visible, list rows body-free.
    let records = rpc(&app, "meta/memory/panel/records", json!({})).await;
    assert_eq!(records["error"], Value::Null, "{records:#?}");
    let rows = records["result"]["records"].as_array().expect("records");
    assert!(rows.len() >= 5, "all seeded records: {rows:#?}");
    assert!(
        rows.iter().all(|row| row.get("body").is_none()),
        "list rows must be body-free: {rows:#?}"
    );
    assert!(
        rows.iter()
            .any(|row| row["status"]["status"] == json!("quarantined")),
        "quarantined row visible without enforcement: {rows:#?}"
    );

    // Identity filter narrows to that identity's scope.
    let router_rows = rpc(
        &app,
        "meta/memory/panel/records",
        json!({ "identity": "router" }),
    )
    .await;
    let router_rows = router_rows["result"]["records"]
        .as_array()
        .expect("router rows")
        .clone();
    assert!(!router_rows.is_empty());
    assert!(
        router_rows
            .iter()
            .all(|row| row["scope"]["identity"] == json!("router")),
        "identity filter leaked other scopes: {router_rows:#?}"
    );

    // Record detail: body + supersede chain + injection usage.
    let detail = rpc(
        &app,
        "meta/memory/panel/record",
        json!({ "memory_id": tip_id }),
    )
    .await;
    assert_eq!(detail["error"], Value::Null, "{detail:#?}");
    assert_eq!(detail["result"]["record"]["body"], json!("Router tip body"));
    let chain = detail["result"]["chain"].as_array().expect("chain");
    assert_eq!(chain.len(), 2, "root + tip: {chain:#?}");
    assert_eq!(chain[0]["status"]["status"], json!("superseded"));
    assert_eq!(chain[1]["id"], json!(tip_id));
    let injections = detail["result"]["injections"]
        .as_array()
        .expect("injections");
    assert_eq!(injections.len(), 1, "{injections:#?}");
    assert_eq!(injections[0]["surface"], json!("build"));

    // Quarantine queue: records plus both seeded promotions, tokenless.
    let quarantine = rpc(&app, "meta/memory/panel/quarantine", json!({})).await;
    assert_eq!(quarantine["error"], Value::Null, "{quarantine:#?}");
    let queue = quarantine["result"]["records"].as_array().expect("queue");
    assert!(
        queue.iter().any(|row| row["id"] == json!(quarantined_id)),
        "{queue:#?}"
    );
    let promotions = quarantine["result"]["pending_promotions"]
        .as_array()
        .expect("promotions");
    assert_eq!(promotions.len(), 2, "{promotions:#?}");
    assert!(
        promotions
            .iter()
            .all(|row| row.get("stage_token").is_none()),
        "stage_token is a commit capability and must never surface: {promotions:#?}"
    );

    // Dream history from steward audit rows.
    let dreams = rpc(&app, "meta/memory/panel/dreams", json!({})).await;
    assert_eq!(dreams["error"], Value::Null, "{dreams:#?}");
    let runs = dreams["result"]["runs"].as_array().expect("runs");
    assert_eq!(runs.len(), 1, "{runs:#?}");
    assert_eq!(runs[0]["run_id"], json!("run-dream-1"));
    assert_eq!(runs[0]["op_kinds"]["create"], json!(1));

    // Experience advertises the panel affordances.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/console/experience")
                .body(Body::empty())
                .expect("experience request"),
        )
        .await
        .expect("experience response");
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("experience body");
    let experience: Value = serde_json::from_slice(&body).expect("experience json");
    assert_eq!(experience["memory"]["available"], json!(true));
    assert_eq!(experience["memory"]["can_read"], json!(true));
    assert_eq!(experience["memory"]["can_review_quarantine"], json!(true));

    let _ = runtime.mob_handle().stop().await;
}

#[tokio::test]
async fn memory_panel_enforces_scope_actions_end_to_end() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let memory_dir = tempfile::tempdir().expect("memory dir");
    let (store, tip_id, quarantined_id, delivery_id, mob_id, operator_id) =
        seeded_memory_store(memory_dir.path()).await;
    runtime.set_memory_panel_store(Arc::new(store.clone()));

    // Anonymous callers: view + EXPLICIT memory read on "router" only. The
    // config mentions a memory action, so it is taken literally — no
    // compat rewrite, no mob/realm/quarantine grants.
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![
            AccessRule {
                id: "view-router".to_string(),
                actions: vec!["agent.view".to_string()],
                agents: vec!["router".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "read-router-memory".to_string(),
                actions: vec!["agent.memory.read".to_string()],
                agents: vec!["router".to_string()],
                ..AccessRule::default()
            },
        ],
        ..AccessControlConfig::default()
    })
    .expect("controller");
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));

    // Experience affordances: readable, not reviewer.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/console/experience")
                .body(Body::empty())
                .expect("experience request"),
        )
        .await
        .expect("experience response");
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("experience body");
    let experience: Value = serde_json::from_slice(&body).expect("experience json");
    assert_eq!(experience["memory"]["can_read"], json!(true));
    assert_eq!(experience["memory"]["can_review_quarantine"], json!(false));

    // Unscoped listing is row-filtered to the granted identity scope; the
    // quarantined router record needs the review grant on top.
    let records = rpc(&app, "meta/memory/panel/records", json!({})).await;
    assert_eq!(records["error"], Value::Null, "{records:#?}");
    let rows = records["result"]["records"].as_array().expect("records");
    assert!(
        rows.iter()
            .all(|row| row["scope"]["identity"] == json!("router")),
        "only router-scope rows may survive: {rows:#?}"
    );
    assert!(
        rows.iter().all(|row| row["id"] != json!(quarantined_id)),
        "quarantined rows need the review grant: {rows:#?}"
    );

    // Identity-keyed listing for a denied identity fails the entry gate.
    let denied = rpc(
        &app,
        "meta/memory/panel/records",
        json!({ "identity": "delivery" }),
    )
    .await;
    assert_eq!(denied["error"]["code"], json!(-32030), "{denied:#?}");

    // Record detail enforcement is post-load, per record scope.
    let allowed = rpc(
        &app,
        "meta/memory/panel/record",
        json!({ "memory_id": tip_id }),
    )
    .await;
    assert_eq!(allowed["error"], Value::Null, "{allowed:#?}");
    for (memory_id, action) in [
        (&delivery_id, "agent.memory.read"),
        (&mob_id, "mob.memory.read"),
        (&operator_id, "operator.memory.read"),
        (&quarantined_id, "memory.quarantine.review"),
    ] {
        let denied = rpc(
            &app,
            "meta/memory/panel/record",
            json!({ "memory_id": memory_id }),
        )
        .await;
        assert_eq!(denied["error"]["code"], json!(-32030), "{denied:#?}");
        assert_eq!(
            denied["error"]["data"]["action"],
            json!(action),
            "{denied:#?}"
        );
    }

    // Quarantine queue and dream history are gated.
    let quarantine = rpc(&app, "meta/memory/panel/quarantine", json!({})).await;
    assert_eq!(quarantine["error"]["code"], json!(-32030));
    let dreams = rpc(&app, "meta/memory/panel/dreams", json!({})).await;
    assert_eq!(dreams["error"]["code"], json!(-32030));

    // §10.3 migration: recall requires read AND view. This config grants
    // both on router (explicitly), neither on delivery.
    let recall_router = rpc(
        &app,
        "meta/agent_memory/recall",
        json!({ "identity": "router", "selection": "always" }),
    )
    .await;
    assert_ne!(
        recall_router["error"]["code"],
        json!(-32030),
        "router recall passes the access gate: {recall_router:#?}"
    );
    let recall_delivery = rpc(
        &app,
        "meta/agent_memory/recall",
        json!({ "identity": "delivery", "selection": "always" }),
    )
    .await;
    assert_eq!(recall_delivery["error"]["code"], json!(-32030));

    // Capabilities intersect: the resource-less panel reads the caller can
    // never use disappear; agent-scoped ones stay (enforced per call).
    let capabilities = rpc(&app, "meta/capabilities", json!({})).await;
    let methods = capabilities["result"]["methods"]
        .as_array()
        .expect("methods")
        .clone();
    assert!(
        methods
            .iter()
            .any(|value| value == "meta/memory/panel/records")
    );
    for hidden in ["meta/memory/panel/dreams", "meta/memory/panel/quarantine"] {
        assert!(
            methods.iter().all(|value| value != hidden),
            "{hidden} requires a grant this caller lacks: {methods:#?}"
        );
    }

    // Live grant of the reviewer + unscoped read opens the gated surfaces —
    // but NOT operator scope, which needs its own explicit grant.
    controller
        .upsert_rule(AccessRule {
            id: "reviewer".to_string(),
            actions: vec![
                "agent.memory.read".to_string(),
                "mob.memory.read".to_string(),
                "memory.quarantine.review".to_string(),
            ],
            ..AccessRule::default()
        })
        .expect("live reviewer grant");
    let dreams = rpc(&app, "meta/memory/panel/dreams", json!({})).await;
    assert_eq!(dreams["error"], Value::Null, "{dreams:#?}");
    let quarantine = rpc(&app, "meta/memory/panel/quarantine", json!({})).await;
    assert_eq!(quarantine["error"], Value::Null, "{quarantine:#?}");
    let queue = quarantine["result"]["records"].as_array().expect("queue");
    assert!(
        queue.iter().any(|row| row["id"] == json!(quarantined_id)),
        "{queue:#?}"
    );
    // Promotion rows ride the target scope's read grant: mob.memory.read
    // admits the mob-targeted promotion, while the delivery-identity one
    // stays hidden — the unscoped read grant lacks agent.view on delivery.
    let promotions = quarantine["result"]["pending_promotions"]
        .as_array()
        .expect("promotions");
    assert!(
        promotions
            .iter()
            .any(|row| row["pending_id"] == json!("gate-mob-promotion")),
        "{promotions:#?}"
    );
    assert!(
        promotions
            .iter()
            .all(|row| row["pending_id"] != json!("gate-delivery-promotion")),
        "identity promotions must not ride the unscoped read grant: {promotions:#?}"
    );

    // Operator-scope rows stay hidden behind operator.memory.read: absent
    // from unscoped listings, denied on detail, denied as a scope filter —
    // an unscoped agent.memory.read grant is deliberately not enough.
    let records = rpc(&app, "meta/memory/panel/records", json!({})).await;
    let rows = records["result"]["records"].as_array().expect("records");
    assert!(
        rows.iter().all(|row| row["id"] != json!(operator_id)),
        "operator rows must not ride the unscoped read grant: {rows:#?}"
    );
    let denied_operator = rpc(
        &app,
        "meta/memory/panel/record",
        json!({ "memory_id": operator_id }),
    )
    .await;
    assert_eq!(denied_operator["error"]["code"], json!(-32030));
    let denied_scope = rpc(
        &app,
        "meta/memory/panel/records",
        json!({ "scope": "operator" }),
    )
    .await;
    assert_eq!(denied_scope["error"]["code"], json!(-32030));

    controller
        .upsert_rule(AccessRule {
            id: "operator-reader".to_string(),
            actions: vec!["operator.memory.read".to_string()],
            ..AccessRule::default()
        })
        .expect("live operator grant");
    let operator_detail = rpc(
        &app,
        "meta/memory/panel/record",
        json!({ "memory_id": operator_id }),
    )
    .await;
    assert_eq!(
        operator_detail["error"],
        Value::Null,
        "{operator_detail:#?}"
    );

    let _ = runtime.mob_handle().stop().await;
}

/// §10.3 on the quarantine queue itself: `memory.quarantine.review` is the
/// entry gate but never sufficient per row — each queue record and each
/// pending promotion still needs the caller's read grant on its (target)
/// scope, so a reviewer-only principal sees an empty queue rather than
/// cross-scope titles, scope keys, and steward rationales.
#[tokio::test]
async fn memory_panel_quarantine_queue_filters_rows_per_scope() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let memory_dir = tempfile::tempdir().expect("memory dir");
    let (store, _tip_id, quarantined_id, _delivery_id, _mob_id, _operator_id) =
        seeded_memory_store(memory_dir.path()).await;
    runtime.set_memory_panel_store(Arc::new(store.clone()));

    // Enforcement on, zero rules: the anonymous caller holds no grants.
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: Vec::new(),
        ..AccessControlConfig::default()
    })
    .expect("controller");
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));

    // Non-reviewers fail the entry gate.
    let denied = rpc(&app, "meta/memory/panel/quarantine", json!({})).await;
    assert_eq!(denied["error"]["code"], json!(-32030), "{denied:#?}");

    // Reviewer-only principal: past the entry gate, but with no per-scope
    // read grant every row is filtered — records AND promotions come back
    // empty instead of leaking cross-scope metadata.
    controller
        .upsert_rule(AccessRule {
            id: "reviewer".to_string(),
            actions: vec!["memory.quarantine.review".to_string()],
            ..AccessRule::default()
        })
        .expect("reviewer grant");
    let quarantine = rpc(&app, "meta/memory/panel/quarantine", json!({})).await;
    assert_eq!(quarantine["error"], Value::Null, "{quarantine:#?}");
    let queue = quarantine["result"]["records"].as_array().expect("queue");
    assert!(
        queue.is_empty(),
        "reviewer-only must see no queue records: {queue:#?}"
    );
    let promotions = quarantine["result"]["pending_promotions"]
        .as_array()
        .expect("promotions");
    assert!(
        promotions.is_empty(),
        "reviewer-only must see no promotions: {promotions:#?}"
    );

    // Reviewer + read/view on "router": exactly router's quarantined record
    // appears; both promotions (mob-targeted, delivery-targeted) stay hidden.
    controller
        .upsert_rule(AccessRule {
            id: "router-reader".to_string(),
            actions: vec!["agent.memory.read".to_string(), "agent.view".to_string()],
            agents: vec!["router".to_string()],
            ..AccessRule::default()
        })
        .expect("router grant");
    let quarantine = rpc(&app, "meta/memory/panel/quarantine", json!({})).await;
    let queue = quarantine["result"]["records"].as_array().expect("queue");
    assert_eq!(queue.len(), 1, "{queue:#?}");
    assert_eq!(queue[0]["id"], json!(quarantined_id));
    let promotions = quarantine["result"]["pending_promotions"]
        .as_array()
        .expect("promotions");
    assert!(
        promotions.is_empty(),
        "router grants must not expose mob/delivery promotions: {promotions:#?}"
    );

    // Reviewer + mob.memory.read: the mob-targeted promotion appears; the
    // delivery-identity promotion still needs read+view on "delivery".
    controller
        .upsert_rule(AccessRule {
            id: "mob-reader".to_string(),
            actions: vec!["mob.memory.read".to_string()],
            ..AccessRule::default()
        })
        .expect("mob grant");
    let quarantine = rpc(&app, "meta/memory/panel/quarantine", json!({})).await;
    let promotions = quarantine["result"]["pending_promotions"]
        .as_array()
        .expect("promotions");
    assert_eq!(promotions.len(), 1, "{promotions:#?}");
    assert_eq!(promotions[0]["pending_id"], json!("gate-mob-promotion"));

    // Reviewer + read/view on "delivery": its promotion joins the queue.
    controller
        .upsert_rule(AccessRule {
            id: "delivery-reader".to_string(),
            actions: vec!["agent.memory.read".to_string(), "agent.view".to_string()],
            agents: vec!["delivery".to_string()],
            ..AccessRule::default()
        })
        .expect("delivery grant");
    let quarantine = rpc(&app, "meta/memory/panel/quarantine", json!({})).await;
    let promotions = quarantine["result"]["pending_promotions"]
        .as_array()
        .expect("promotions");
    assert_eq!(promotions.len(), 2, "{promotions:#?}");
    assert!(
        promotions
            .iter()
            .any(|row| row["pending_id"] == json!("gate-delivery-promotion")),
        "{promotions:#?}"
    );

    let _ = runtime.mob_handle().stop().await;
}

/// The operator half of quarantine review over the console: the decision
/// composes EXISTING grants. The reviewer grant alone decides nothing; it
/// must meet the per-row read grants and the identity's own write (release)
/// or delete (tombstone) grant. The store binds the decision to the named
/// identity's scope and to the reviewed content hash.
#[tokio::test]
async fn memory_quarantine_decide_composes_existing_grants_end_to_end() {
    use meerkat_meta::memory::records::{MemoryAuthor, MemoryKind, RecordStatus};
    use meerkat_meta::{AgentMemoryProvider, MemoryScope, NewMemoryRecord, StewardStore};

    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let memory_dir = tempfile::tempdir().expect("memory dir");
    let (store, _tip_id, gated_id, _delivery_id, _mob_id, _operator_id) =
        seeded_memory_store(memory_dir.path()).await;
    // A second quarantined router write with no gated promotion pending
    // (the seeded store still carries its always-quarantine write gate).
    let reviewable = store
        .remember_authored(
            &MemoryScope::Identity {
                realm: "default".to_string(),
                identity: "router".to_string(),
            },
            NewMemoryRecord {
                kind: MemoryKind::Preference,
                title: "Router reviewable claim".to_string(),
                description: "When routing late-night pages".to_string(),
                body: "Prefers paging the secondary after 22:00.".to_string(),
                tags: Vec::new(),
                evidence: Vec::new(),
                verification: None,
            },
            MemoryAuthor::Agent {
                identity: "router".to_string(),
            },
        )
        .await
        .expect("reviewable quarantined record");
    assert!(matches!(
        reviewable.status,
        RecordStatus::Quarantined { .. }
    ));
    let reviewable_id = reviewable.memory_id;
    runtime.set_memory_panel_store(Arc::new(store.clone()));

    // Enforcement on, zero rules: the anonymous caller holds no grants.
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: Vec::new(),
        ..AccessControlConfig::default()
    })
    .expect("controller");
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));
    let advertised = |capabilities: &Value| {
        capabilities["result"]["methods"]
            .as_array()
            .expect("methods")
            .iter()
            .any(|method| method == "meta/memory/quarantine/decide")
    };
    assert!(
        !advertised(&rpc(&app, "meta/capabilities", json!({})).await),
        "decide needs the reviewer grant before it is advertised"
    );

    controller
        .upsert_rule(AccessRule {
            id: "reviewer".to_string(),
            actions: vec!["memory.quarantine.review".to_string()],
            ..AccessRule::default()
        })
        .expect("reviewer grant");
    assert!(advertised(&rpc(&app, "meta/capabilities", json!({})).await));

    let decide = |verdict: &str, identity: &str, memory_id: &str, hash: &str| {
        json!({
            "identity": identity,
            "memory_id": memory_id,
            "verdict": verdict,
            "expected_content_hash": hash,
            "rationale": "confirmed with the on-call lead",
        })
    };
    let hash = meerkat_meta::memory::records::content_hash(
        "Router reviewable claim",
        "Prefers paging the secondary after 22:00.",
    );

    // Reviewer grant alone: denied at the identity's view grant.
    let denied = rpc(
        &app,
        "meta/memory/quarantine/decide",
        decide("release", "router", &reviewable_id, &hash),
    )
    .await;
    assert_eq!(denied["error"]["code"], json!(-32030), "{denied:#?}");
    assert_eq!(denied["error"]["data"]["action"], json!("agent.view"));

    // Reviewer + read/view on router: can read the body, still cannot
    // release without the identity's write grant.
    controller
        .upsert_rule(AccessRule {
            id: "router-reader".to_string(),
            actions: vec!["agent.memory.read".to_string(), "agent.view".to_string()],
            agents: vec!["router".to_string()],
            ..AccessRule::default()
        })
        .expect("router read grant");
    let detail = rpc(
        &app,
        "meta/memory/panel/record",
        json!({ "memory_id": reviewable_id }),
    )
    .await;
    assert_eq!(detail["error"], Value::Null, "{detail:#?}");
    assert_eq!(detail["result"]["record"]["content_hash"], json!(hash));
    let denied = rpc(
        &app,
        "meta/memory/quarantine/decide",
        decide("release", "router", &reviewable_id, &hash),
    )
    .await;
    assert_eq!(denied["error"]["code"], json!(-32030), "{denied:#?}");
    assert_eq!(
        denied["error"]["data"]["action"],
        json!("agent.memory.write")
    );

    // + write on router: the release commits, the reviewed hash bound.
    controller
        .upsert_rule(AccessRule {
            id: "router-writer".to_string(),
            actions: vec!["agent.memory.write".to_string()],
            agents: vec!["router".to_string()],
            ..AccessRule::default()
        })
        .expect("router write grant");
    let stale = rpc(
        &app,
        "meta/memory/quarantine/decide",
        decide("release", "router", &reviewable_id, "0000"),
    )
    .await;
    assert_eq!(stale["error"]["code"], json!(-32043), "{stale:#?}");
    assert_eq!(stale["error"]["data"]["reason"], json!("content_mismatch"));
    // The refusal never reveals the stored hash to bind against.
    assert!(!stale.to_string().contains(&hash), "{stale:#?}");
    let released = rpc(
        &app,
        "meta/memory/quarantine/decide",
        decide("release", "router", &reviewable_id, &hash),
    )
    .await;
    assert_eq!(released["error"], Value::Null, "{released:#?}");
    let result = &released["result"];
    assert_eq!(result["outcome"], json!("released"));
    assert_eq!(result["realm"], json!("default"));
    // The console runs without app auth here: the decision says no
    // principal was known instead of naming anyone.
    assert_eq!(
        result["decision"]["review"]["reviewer"],
        json!({ "kind": "operator", "principal": null })
    );
    assert_eq!(result["origin"]["memory_id"], json!(reviewable_id));
    assert_eq!(result["origin"]["status"]["status"], json!("tombstoned"));
    assert_eq!(result["origin"]["ever_quarantined"], json!(true));
    assert_eq!(
        result["successor"]["memory_id"],
        json!(format!("{reviewable_id}-released"))
    );
    assert_eq!(result["successor"]["status"]["status"], json!("active"));
    assert_eq!(result["successor"]["trust"], json!("agent_observed"));
    assert_eq!(result["successor"]["ever_quarantined"], json!(true));
    assert_eq!(result["successor"]["derived_from"], json!([reviewable_id]));
    assert_eq!(result["successor"]["content_hash"], json!(hash));
    assert_eq!(
        result["decision"]["review"]["origin_quarantine_reason"],
        json!("test taint"),
        "{result:#?}"
    );
    assert_eq!(
        result["decision"]["review"]["successor"],
        json!(format!("{reviewable_id}-released"))
    );
    assert!(
        result["decision"]["audit_token"]
            .as_str()
            .is_some_and(|token| token.starts_with("review-"))
    );
    // The successor is now ordinary active memory for the identity.
    let recalled = store
        .recall(meerkat_meta::AgentMemoryRecallRequest {
            identity: meerkat_meta::identity_first::AgentIdentity::parse("router")
                .expect("router identity"),
            realm: "default".to_string(),
            query_text: None,
            query_terms: Vec::new(),
            selection: meerkat_meta::AgentMemorySelection::Always,
            max_entries: 64,
        })
        .await
        .expect("recall");
    assert!(
        recalled
            .iter()
            .any(|record| record.memory_id == format!("{reviewable_id}-released")),
        "{recalled:#?}"
    );

    // Replays are idempotent; a cross-verdict replay names the successor.
    let replay = rpc(
        &app,
        "meta/memory/quarantine/decide",
        decide("release", "router", &reviewable_id, &hash),
    )
    .await;
    assert_eq!(replay["result"]["outcome"], json!("already_released"));
    assert_eq!(
        replay["result"]["successor"]["memory_id"],
        json!(format!("{reviewable_id}-released"))
    );
    assert_eq!(
        replay["result"]["decision"], result["decision"],
        "a replay returns the committed decision"
    );

    // Tombstone needs the identity's delete grant on top of the reviewer
    // and read grants; with it, a still-gated record is refused typed.
    let denied = rpc(
        &app,
        "meta/memory/quarantine/decide",
        decide("tombstone", "router", &gated_id, &hash),
    )
    .await;
    assert_eq!(denied["error"]["code"], json!(-32030), "{denied:#?}");
    assert_eq!(
        denied["error"]["data"]["action"],
        json!("agent.memory.delete")
    );
    controller
        .upsert_rule(AccessRule {
            id: "router-deleter".to_string(),
            actions: vec!["agent.memory.delete".to_string()],
            agents: vec!["router".to_string()],
            ..AccessRule::default()
        })
        .expect("router delete grant");
    let gated_hash = meerkat_meta::memory::records::content_hash(
        "Router quarantined claim",
        "Router quarantined claim body",
    );
    // A live gated promotion owns its source's publication: a release waits
    // for gating (or the expiry), while the operator's tombstone invalidates
    // the gate in the same transaction.
    let live_gated = store
        .remember_authored(
            &MemoryScope::Identity {
                realm: "default".to_string(),
                identity: "router".to_string(),
            },
            NewMemoryRecord {
                kind: MemoryKind::Fact,
                title: "Router gated claim".to_string(),
                description: String::new(),
                body: "Pages go to the mob channel.".to_string(),
                tags: Vec::new(),
                evidence: Vec::new(),
                verification: None,
            },
            MemoryAuthor::Agent {
                identity: "router".to_string(),
            },
        )
        .await
        .expect("live gated record")
        .memory_id;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64;
    store
        .record_pending_promotion(
            "default",
            meerkat_meta::memory::PendingPromotion {
                pending_id: "gate-live".to_string(),
                stage_token: "stage-live".to_string(),
                record_id: live_gated.clone(),
                scope_kind: "mob".to_string(),
                scope_key: "access-control-mob".to_string(),
                rationale: None,
                status: "pending".to_string(),
                created_at_ms: now_ms,
            },
        )
        .await
        .expect("live gate mapping");
    let live_hash = meerkat_meta::memory::records::content_hash(
        "Router gated claim",
        "Pages go to the mob channel.",
    );
    let gated = rpc(
        &app,
        "meta/memory/quarantine/decide",
        decide("release", "router", &live_gated, &live_hash),
    )
    .await;
    assert_eq!(gated["error"]["code"], json!(-32043), "{gated:#?}");
    assert_eq!(gated["error"]["data"]["reason"], json!("gate_pending"));
    assert_eq!(gated["error"]["data"]["pending_id"], json!("gate-live"));
    assert!(
        gated["error"]["data"]["expires_at_ms"]
            .as_u64()
            .is_some_and(|expires| expires > now_ms)
    );
    let invalidated = rpc(
        &app,
        "meta/memory/quarantine/decide",
        decide("tombstone", "router", &live_gated, &live_hash),
    )
    .await;
    assert_eq!(
        invalidated["result"]["outcome"],
        json!("tombstoned"),
        "{invalidated:#?}"
    );
    assert_eq!(
        invalidated["result"]["decision"]["review"]["invalidated_promotions"],
        json!(["gate-live"])
    );
    assert!(
        !store
            .pending_promotions("default")
            .await
            .expect("pending promotions")
            .iter()
            .any(|promotion| promotion.pending_id == "gate-live"),
        "the invalidated gate is no longer pending"
    );

    // The seeded promotions are long past the shared expiry: a decision
    // expires them (no steward needed) and records which ones it expired.
    let expired = rpc(
        &app,
        "meta/memory/quarantine/decide",
        decide("tombstone", "router", &gated_id, &gated_hash),
    )
    .await;
    assert_eq!(
        expired["result"]["outcome"],
        json!("tombstoned"),
        "{expired:#?}"
    );
    assert_eq!(
        expired["result"]["decision"]["review"]["expired_promotions"],
        json!(["gate-mob-promotion", "gate-delivery-promotion"])
    );

    // Another identity's scope never reaches a router record: denied while
    // ungranted, and reads as not_found once the grants exist.
    let denied = rpc(
        &app,
        "meta/memory/quarantine/decide",
        decide("release", "delivery", &gated_id, &gated_hash),
    )
    .await;
    assert_eq!(denied["error"]["code"], json!(-32030), "{denied:#?}");
    controller
        .upsert_rule(AccessRule {
            id: "delivery-writer".to_string(),
            actions: vec![
                "agent.view".to_string(),
                "agent.memory.read".to_string(),
                "agent.memory.write".to_string(),
            ],
            agents: vec!["delivery".to_string()],
            ..AccessRule::default()
        })
        .expect("delivery grants");
    let foreign = rpc(
        &app,
        "meta/memory/quarantine/decide",
        decide("release", "delivery", &gated_id, &gated_hash),
    )
    .await;
    assert_eq!(foreign["error"]["code"], json!(-32043), "{foreign:#?}");
    assert_eq!(foreign["error"]["data"]["reason"], json!("not_found"));

    let _ = runtime.mob_handle().stop().await;
}

/// A read-only console neither advertises nor executes the decision.
#[tokio::test]
async fn memory_quarantine_decide_is_refused_on_a_read_only_console() {
    use meerkat_meta::MemoryPanelStore;

    let (_temp_dir, runtime) = build_access_runtime_fixture().await;
    let memory_dir = tempfile::tempdir().expect("memory dir");
    let (store, _tip_id, quarantined_id, _delivery_id, _mob_id, _operator_id) =
        seeded_memory_store(memory_dir.path()).await;
    runtime.set_memory_panel_store(Arc::new(store.clone()));
    let mut decisions = decision_state(false);
    decisions.console.read_only = true;
    let app = runtime.build_reference_app_router(decisions);

    let capabilities = rpc(&app, "meta/capabilities", json!({})).await;
    let methods = capabilities["result"]["methods"]
        .as_array()
        .expect("methods");
    assert!(
        methods
            .iter()
            .any(|method| method == "meta/memory/panel/quarantine"),
        "the read surfaces stay: {methods:#?}"
    );
    assert!(
        methods
            .iter()
            .all(|method| method != "meta/memory/quarantine/decide"),
        "{methods:#?}"
    );
    let refused = rpc(
        &app,
        "meta/memory/quarantine/decide",
        json!({
            "identity": "router",
            "memory_id": quarantined_id,
            "verdict": "tombstone",
            "expected_content_hash": meerkat_meta::memory::records::content_hash(
                "Router quarantined claim",
                "Router quarantined claim body",
            ),
        }),
    )
    .await;
    assert_eq!(refused["error"]["code"], json!(-32010), "{refused:#?}");
    let record = store
        .record_by_id("default", &quarantined_id)
        .await
        .expect("read")
        .expect("record");
    assert!(matches!(
        record.status,
        meerkat_meta::memory::records::RecordStatus::Quarantined { .. }
    ));

    let _ = runtime.mob_handle().stop().await;
}

/// One quarantined `router` write through the seeded store's write gate.
async fn quarantined_router_record(
    store: &meerkat_meta::SqliteAgentMemoryStore,
    title: &str,
    body: &str,
) -> String {
    use meerkat_meta::AgentMemoryProvider;
    use meerkat_meta::memory::records::{MemoryAuthor, MemoryKind, RecordStatus};
    let receipt = store
        .remember_authored(
            &meerkat_meta::MemoryScope::Identity {
                realm: "default".to_string(),
                identity: "router".to_string(),
            },
            meerkat_meta::NewMemoryRecord {
                kind: MemoryKind::Fact,
                title: title.to_string(),
                description: String::new(),
                body: body.to_string(),
                tags: Vec::new(),
                evidence: Vec::new(),
                verification: None,
            },
            MemoryAuthor::Agent {
                identity: "router".to_string(),
            },
        )
        .await
        .expect("quarantined router write");
    assert!(matches!(receipt.status, RecordStatus::Quarantined { .. }));
    receipt.memory_id
}

fn operator_review(
    memory_id: &str,
    title: &str,
    body: &str,
    decision: meerkat_meta::QuarantineDecision,
    principal: Option<&str>,
    rationale: Option<&str>,
) -> meerkat_meta::QuarantineReviewRequest {
    meerkat_meta::QuarantineReviewRequest {
        scope: meerkat_meta::MemoryScope::Identity {
            realm: "default".to_string(),
            identity: "router".to_string(),
        },
        memory_id: memory_id.to_string(),
        decision,
        expected_content_hash: meerkat_meta::memory::records::content_hash(title, body),
        reviewer: meerkat_meta::QuarantineReviewer::Operator {
            principal: principal.map(str::to_string),
        },
        rationale: rationale.map(str::to_string),
    }
}

/// With access control on, an anonymous caller is a principal like any
/// other: no grants, no decision. There is no "no principal, so allow"
/// path.
#[tokio::test]
async fn memory_quarantine_decide_denies_an_anonymous_caller_under_enforced_access() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let memory_dir = tempfile::tempdir().expect("memory dir");
    let (store, _tip_id, _gated_id, _delivery_id, _mob_id, _operator_id) =
        seeded_memory_store(memory_dir.path()).await;
    let (title, body) = (
        "Router anonymous claim",
        "Anonymous callers decide nothing.",
    );
    let record = quarantined_router_record(&store, title, body).await;
    runtime.set_memory_panel_store(Arc::new(store.clone()));
    runtime.set_access_controller(
        AccessController::new(AccessControlConfig {
            enabled: true,
            admins: vec!["root@example.test".to_string()],
            rules: vec![AccessRule {
                id: "everyone-views".to_string(),
                actions: vec!["agent.view".to_string()],
                ..AccessRule::default()
            }],
            ..AccessControlConfig::default()
        })
        .expect("controller"),
    );
    let app = runtime.build_reference_app_router(decision_state(false));
    for verdict in ["release", "tombstone"] {
        let denied = rpc(
            &app,
            "meta/memory/quarantine/decide",
            json!({
                "identity": "router",
                "memory_id": record,
                "verdict": verdict,
                "expected_content_hash": meerkat_meta::memory::records::content_hash(title, body),
            }),
        )
        .await;
        assert_eq!(denied["error"]["code"], json!(-32030), "{denied:#?}");
        assert_eq!(
            denied["error"]["data"]["action"],
            json!("memory.quarantine.review")
        );
    }
    let still = meerkat_meta::MemoryPanelStore::record_by_id(&store, "default", &record)
        .await
        .expect("read")
        .expect("record");
    assert!(matches!(
        still.status,
        meerkat_meta::memory::records::RecordStatus::Quarantined { .. }
    ));

    let _ = runtime.mob_handle().stop().await;
}

/// A trusted host without app auth and without access control decides with
/// no principal, and the decision says so rather than naming anyone.
#[tokio::test]
async fn memory_quarantine_decide_on_a_trusted_host_records_no_principal() {
    let (_temp_dir, runtime) = build_access_runtime_fixture().await;
    let memory_dir = tempfile::tempdir().expect("memory dir");
    let (store, _tip_id, _gated_id, _delivery_id, _mob_id, _operator_id) =
        seeded_memory_store(memory_dir.path()).await;
    let (title, body) = ("Router trusted claim", "The host vouches for its console.");
    let record = quarantined_router_record(&store, title, body).await;
    runtime.set_memory_panel_store(Arc::new(store.clone()));
    let app = runtime.build_reference_app_router(decision_state(false));
    let released = rpc(
        &app,
        "meta/memory/quarantine/decide",
        json!({
            "identity": "router",
            "memory_id": record,
            "verdict": "release",
            "expected_content_hash": meerkat_meta::memory::records::content_hash(title, body),
        }),
    )
    .await;
    assert_eq!(released["error"], Value::Null, "{released:#?}");
    assert_eq!(released["result"]["outcome"], json!("released"));
    assert_eq!(
        released["result"]["decision"]["review"]["reviewer"],
        json!({ "kind": "operator", "principal": null })
    );

    let _ = runtime.mob_handle().stop().await;
}

/// Quarantined content stays reviewer-only after a review or forget
/// retires it: the released or discarded origin, and a forgotten
/// quarantined write, are never readable by an ordinary scope reader,
/// while the released successor is ordinary memory again.
#[tokio::test]
async fn quarantine_evidence_stays_reviewer_only_after_review_and_forget() {
    use meerkat_meta::{AgentMemoryProvider, QuarantineDecision, StewardStore};

    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let memory_dir = tempfile::tempdir().expect("memory dir");
    let (store, _tip_id, _gated_id, _delivery_id, _mob_id, _operator_id) =
        seeded_memory_store(memory_dir.path()).await;
    let released_title = "Router released claim";
    let released_body = "Escalate after two missed pages.";
    let released = quarantined_router_record(&store, released_title, released_body).await;
    let discarded_title = "Router discarded claim";
    let discarded_body = "Ignore the on-call rota.";
    let discarded = quarantined_router_record(&store, discarded_title, discarded_body).await;
    let forgotten = quarantined_router_record(&store, "Router forgotten claim", "Forgotten.").await;
    let successor = store
        .review_quarantined(operator_review(
            &released,
            released_title,
            released_body,
            QuarantineDecision::Release,
            Some("reviewer@example.test"),
            None,
        ))
        .await
        .expect("release")
        .successor()
        .expect("successor")
        .memory_id
        .clone();
    store
        .review_quarantined(operator_review(
            &discarded,
            discarded_title,
            discarded_body,
            QuarantineDecision::Tombstone,
            Some("reviewer@example.test"),
            None,
        ))
        .await
        .expect("tombstone");
    store
        .forget(
            "default",
            &meerkat_meta::identity_first::AgentIdentity::parse("router").expect("identity"),
            &forgotten,
        )
        .await
        .expect("forget");
    runtime.set_memory_panel_store(Arc::new(store.clone()));

    // An ordinary router reader: view + memory read, no review grant.
    let controller = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![AccessRule {
            id: "router-reader".to_string(),
            actions: vec!["agent.view".to_string(), "agent.memory.read".to_string()],
            agents: vec!["router".to_string()],
            ..AccessRule::default()
        }],
        ..AccessControlConfig::default()
    })
    .expect("controller");
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));

    for (origin, body) in [
        (&released, released_body),
        (&discarded, discarded_body),
        (&forgotten, "Forgotten."),
    ] {
        let denied = rpc(
            &app,
            "meta/memory/panel/record",
            json!({ "memory_id": origin }),
        )
        .await;
        assert_eq!(denied["error"]["code"], json!(-32030), "{denied:#?}");
        assert_eq!(
            denied["error"]["data"]["action"],
            json!("memory.quarantine.review")
        );
        assert!(!denied.to_string().contains(body), "{denied:#?}");
    }
    let rows = rpc(
        &app,
        "meta/memory/panel/records",
        json!({ "identity": "router", "limit": 200 }),
    )
    .await;
    let rows = rows["result"]["records"].as_array().expect("rows").clone();
    for origin in [&released, &discarded, &forgotten] {
        assert!(
            rows.iter().all(|row| row["id"] != json!(origin)),
            "retired quarantine evidence must not list for ordinary readers: {rows:#?}"
        );
    }

    // Positive control: the released successor is ordinary memory.
    let readable = rpc(
        &app,
        "meta/memory/panel/record",
        json!({ "memory_id": successor }),
    )
    .await;
    assert_eq!(readable["error"], Value::Null, "{readable:#?}");
    assert_eq!(readable["result"]["record"]["body"], json!(released_body));
    assert!(rows.iter().any(|row| row["id"] == json!(successor)));

    // A reviewer still reads the retained evidence.
    controller
        .upsert_rule(AccessRule {
            id: "reviewer".to_string(),
            actions: vec!["memory.quarantine.review".to_string()],
            ..AccessRule::default()
        })
        .expect("reviewer grant");
    let evidence = rpc(
        &app,
        "meta/memory/panel/record",
        json!({ "memory_id": discarded }),
    )
    .await;
    assert_eq!(evidence["result"]["record"]["body"], json!(discarded_body));

    let _ = runtime.mob_handle().stop().await;
}

/// The verdict a review emits onto the system-wide timeline carries no
/// reviewer identity and no rationale: a viewer with no memory grant sees
/// that a verdict happened, never who decided or why.
#[tokio::test]
async fn quarantine_verdict_frames_carry_no_reviewer_or_rationale() {
    use meerkat_meta::{QuarantineDecision, StewardStore};

    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let memory_dir = tempfile::tempdir().expect("memory dir");
    let (store, _tip_id, _gated_id, _delivery_id, _mob_id, _operator_id) =
        seeded_memory_store(memory_dir.path()).await;
    store.set_event_sink(runtime.memory_event_sink());
    let (title, body) = (
        "Router timeline claim",
        "Verdicts stay terse on the timeline.",
    );
    let record = quarantined_router_record(&store, title, body).await;
    let marker = "MARKER-rationale-7f3a";
    let reviewer = "reviewer-marker@example.test";
    store
        .review_quarantined(operator_review(
            &record,
            title,
            body,
            QuarantineDecision::Release,
            Some(reviewer),
            Some(marker),
        ))
        .await
        .expect("release");

    let controller = view_only_memory_aware_controller();
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));
    assert_view_only_reader(&app, &controller, "router").await;
    // The timeline stream subscribes before it snapshots, so the verdict
    // arrives either in the snapshot or live, whenever its append lands.
    let streamed = timeline_stream_frame(&app, "memory.quarantine.verdict")
        .await
        .to_string();
    assert!(streamed.contains(&record), "{streamed}");
    assert!(!streamed.contains(marker), "{streamed}");
    assert!(!streamed.contains(reviewer), "{streamed}");
    // The windowed timeline projects the same frame, as terse.
    let page = rpc(
        &app,
        "meta/console/query_timeline",
        json!({ "mode": "recent", "limit": 200 }),
    )
    .await;
    let verdicts = page["result"]["frames"]
        .as_array()
        .expect("frames")
        .iter()
        .filter(|frame| frame["kind"] == json!("memory.quarantine.verdict"))
        .count();
    assert_eq!(verdicts, 1, "the applied verdict reaches the timeline");
    let projected = page.to_string();
    assert!(!projected.contains(marker), "{projected}");
    assert!(!projected.contains(reviewer), "{projected}");

    let _ = runtime.mob_handle().stop().await;
}

/// A dream's open-loop escalation may name a quarantined record. Its nudge
/// reaches the system-wide timeline, which a reader with agent.view alone
/// sees; the dream's free-text rationale, which can quote the record, does
/// not.
#[tokio::test]
async fn open_loop_escalation_frames_carry_no_rationale() {
    use meerkat_client::{LlmClient, LlmDoneOutcome, LlmEvent};
    use meerkat_meta::memory::distiller::TranscriptSlice;
    use meerkat_meta::memory::{
        DistillerError, DreamOutcome, StewardClientHandle, StewardConfig, StewardEngine,
        StewardError, StewardProfile, TranscriptSource,
    };

    struct FixedReply(Arc<dyn LlmClient>);
    #[async_trait::async_trait]
    impl StewardClientHandle for FixedReply {
        async fn client(&self) -> Result<Arc<dyn LlmClient>, StewardError> {
            Ok(self.0.clone())
        }
        fn invalidate(&self) {}
    }
    struct NoTranscripts;
    #[async_trait::async_trait]
    impl TranscriptSource for NoTranscripts {
        async fn read(
            &self,
            _session_key: &str,
            _from_index: u64,
        ) -> Result<Option<TranscriptSlice>, DistillerError> {
            Ok(None)
        }
    }
    struct AgentWritesQuarantine;
    impl meerkat_meta::memory::taint::LlmWriteGate for AgentWritesQuarantine {
        fn quarantine_reason(
            &self,
            author: &meerkat_meta::memory::records::MemoryAuthor,
            _kind: meerkat_meta::memory::staged::StagedBatchKind,
            _evidence: &[meerkat_meta::memory::records::EvidenceRef],
        ) -> Option<String> {
            author.is_llm().then(|| "test taint".to_string())
        }
    }

    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;
    let memory_dir = tempfile::tempdir().expect("memory dir");
    let store = meerkat_meta::SqliteAgentMemoryStore::open(memory_dir.path()).expect("open store");
    store.set_llm_write_gate(Arc::new(AgentWritesQuarantine));
    store.set_event_sink(runtime.memory_event_sink());
    let record =
        quarantined_router_record(&store, "Router open loop", "Waiting on the vendor's reply.")
            .await;
    let canary = "CANARY-escalation-rationale-5d1c";
    // One reply serves every phase: an empty gather, then a consolidate that
    // escalates the quarantined record as a stale open loop.
    let reply = json!({
        "requests": [], "ops": [], "proposal_verdicts": [], "quarantine_verdicts": [],
        "open_loop_escalations": [{ "record_id": record, "rationale": canary }],
        "contradictions": [], "working_set": []
    })
    .to_string();
    let client: Arc<dyn LlmClient> = Arc::new(TestClient::new(vec![
        LlmEvent::TextDelta {
            delta: reply,
            meta: None,
        },
        LlmEvent::Done {
            outcome: LlmDoneOutcome::Success {
                stop_reason: meerkat_core::StopReason::EndTurn,
            },
        },
    ]));
    let engine = Arc::new(
        StewardEngine::new(
            StewardProfile::embedded_default(),
            StewardConfig {
                enabled: true,
                min_signals: 1,
                ..StewardConfig::default()
            },
            Arc::new(FixedReply(client)),
            Arc::new(store.clone()),
            Arc::new(NoTranscripts),
            "default",
        )
        .with_events(runtime.memory_event_sink()),
    );
    engine.note_session_completed();
    let DreamOutcome::Completed(run) = engine.dream_now().await else {
        panic!("the dream completes");
    };
    assert_eq!(run.verdicts.open_loops_escalated, 1, "{:?}", run.skips);

    let controller = view_only_memory_aware_controller();
    runtime.set_access_controller(controller.clone());
    let app = runtime.build_reference_app_router(decision_state(false));
    assert_view_only_reader(&app, &controller, "router").await;
    let nudge = timeline_stream_frame(&app, "memory.quarantine.verdict")
        .await
        .to_string();
    assert!(nudge.contains("open_loop_escalated"), "{nudge}");
    assert!(nudge.contains(&record), "{nudge}");
    assert!(!nudge.contains(canary), "{nudge}");
    let page = rpc(
        &app,
        "meta/console/query_timeline",
        json!({ "mode": "recent", "limit": 200 }),
    )
    .await
    .to_string();
    assert!(page.contains("open_loop_escalated"), "{page}");
    assert!(!page.contains(canary), "{page}");

    let _ = runtime.mob_handle().stop().await;
}

#[tokio::test]
async fn recall_read_action_migration_compat_rule_both_ways() {
    let (_temp_dir, mut runtime) = build_access_runtime_fixture().await;

    // Memory-naive config (no memory action anywhere): agent.memory.read is
    // implicitly granted wherever agent.view is granted, so pre-migration
    // recall behavior is preserved.
    let naive = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![AccessRule {
            id: "view-router".to_string(),
            actions: vec!["agent.view".to_string()],
            agents: vec!["router".to_string()],
            ..AccessRule::default()
        }],
        ..AccessControlConfig::default()
    })
    .expect("naive controller");
    let (config, _) = naive.snapshot();
    assert!(
        config.rules[0]
            .actions
            .contains(&"agent.memory.read".to_string()),
        "compat rewrite materializes the read grant: {config:#?}"
    );
    runtime.set_access_controller(naive);
    let app = runtime.build_reference_app_router(decision_state(false));
    let recall_router = rpc(
        &app,
        "meta/agent_memory/recall",
        json!({ "identity": "router", "selection": "always" }),
    )
    .await;
    assert_ne!(
        recall_router["error"]["code"],
        json!(-32030),
        "naive config keeps recall working on view grants: {recall_router:#?}"
    );
    let recall_delivery = rpc(
        &app,
        "meta/agent_memory/recall",
        json!({ "identity": "delivery", "selection": "always" }),
    )
    .await;
    assert_eq!(recall_delivery["error"]["code"], json!(-32030));

    // Explicit config (mentions a memory action anywhere): taken literally.
    // View on router without a read grant now denies recall on the read
    // action.
    let explicit = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["root@example.test".to_string()],
        rules: vec![
            AccessRule {
                id: "view-router".to_string(),
                actions: vec!["agent.view".to_string()],
                agents: vec!["router".to_string()],
                ..AccessRule::default()
            },
            AccessRule {
                id: "unrelated-memory-rule".to_string(),
                actions: vec!["agent.memory.read".to_string()],
                agents: vec!["someone-else".to_string()],
                ..AccessRule::default()
            },
        ],
        ..AccessControlConfig::default()
    })
    .expect("explicit controller");
    runtime.set_access_controller(explicit);
    let app = runtime.build_reference_app_router(decision_state(false));
    let denied = rpc(
        &app,
        "meta/agent_memory/recall",
        json!({ "identity": "router", "selection": "always" }),
    )
    .await;
    assert_eq!(denied["error"]["code"], json!(-32030), "{denied:#?}");
    assert_eq!(
        denied["error"]["data"]["action"],
        json!("agent.memory.read"),
        "explicit configs are taken literally: {denied:#?}"
    );

    let _ = runtime.mob_handle().stop().await;
}

// Candidate acceptance for the agreed checked_v1 wire contract. All Rust
// APIs exist at the baseline; absent advertisement/envelope support is not
// the historical stale-write behavioral RED.
mod checked_v1_acceptance {
    use super::*;

    const ADMIN_A: &str = "root@example.test";
    const ADMIN_B: &str = "alice@example.test";
    const NEW_ADMIN: &str = "carol@example.test";
    const ISSUER: &str = "https://trusted.meta.localhost";
    const PRIVATE_CONFIG: &str = "PRIVATE_CHECKED_SAVE_CONFIG_CANARY";

    struct Fixture {
        dir: tempfile::TempDir,
        controller: AccessController,
        app: axum::Router,
    }

    fn app_for(controller: &AccessController) -> axum::Router {
        let mut decisions = decision_state(true);
        // Development HS256 still exercises signature, issuer, audience and
        // allowlist verification through the real console HTTP route.
        decisions.trusted_oidc.discovery_json = json!({
            "issuer": ISSUER,
            "jwks_uri": format!("{ISSUER}/.well-known/jwks.json"),
        })
        .to_string();
        meerkat_meta::console_json_router_with_aggregator_and_access(
            decisions,
            meerkat_meta::MetaConsoleAggregator::new(Arc::new(
                meerkat_meta::InMemoryConsoleLogStore::default(),
            )),
            Some(controller.clone()),
        )
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().expect("access persistence directory");
        let config = AccessControlConfig {
            approvers: Default::default(),
            enabled: true,
            admins: vec![ADMIN_A.to_string(), ADMIN_B.to_string()],
            groups: BTreeMap::from([(
                "ops".to_string(),
                AccessGroup {
                    members: vec![NEW_ADMIN.to_string()],
                    ..AccessGroup::default()
                },
            )]),
            rules: vec![AccessRule {
                id: "existing-rule".to_string(),
                description: Some(PRIVATE_CONFIG.to_string()),
                subjects: vec![NEW_ADMIN.to_string()],
                actions: vec!["agent.send".to_string()],
                agents: vec!["router".to_string()],
                ..AccessRule::default()
            }],
        };
        let path = dir.path().join("access.toml");
        std::fs::write(&path, toml::to_string_pretty(&config).expect("seed TOML"))
            .expect("fixture TOML file");
        let controller = AccessController::load(path).expect("real stored owner");
        assert_eq!(*controller.snapshot().0, config);
        assert_eq!(controller.snapshot().1, 0);
        let app = app_for(&controller);
        Fixture {
            dir,
            controller,
            app,
        }
    }

    async fn authenticated_rpc(
        app: &axum::Router,
        subject: &str,
        method: &str,
        params: Value,
    ) -> Value {
        let mut jwt_header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
        jwt_header.kid = Some("kid-current".to_string());
        let token = jsonwebtoken::encode(
            &jwt_header,
            &json!({
                "iss": ISSUER,
                "aud": "meerkat-console",
                "sub": subject,
                "email": subject,
                "provider": "google_oauth",
                "exp": chrono::Utc::now().timestamp() + 300,
            }),
            &jsonwebtoken::EncodingKey::from_secret(b"phase7-trusted-current-secret"),
        )
        .expect("signed fixture token");
        let request = Request::builder()
            .method("POST")
            .uri("/console/rpc")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::from(
                json!({"jsonrpc": "2.0", "id": "checked-save", "method": method, "params": params})
                    .to_string(),
            ))
            .expect("authenticated RPC request");
        let response = tokio::time::timeout(Duration::from_secs(5), app.clone().oneshot(request))
            .await
            .expect("bounded RPC response")
            .expect("HTTP router response");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("RPC body");
        let value: Value = serde_json::from_slice(&bytes).expect("RPC JSON");
        assert_eq!(value["jsonrpc"], json!("2.0"));
        assert_eq!(value["id"], json!("checked-save"));
        value
    }

    /// Transport coverage through the signed HTTP router. The separate
    /// authenticator/handler fixture owns the in-flight revocation ordering.
    #[tokio::test]
    async fn authenticated_gating_rpc_preserves_principal_and_approver_authority() {
        const ROLE: &str = "role:release-manager";
        let (_dir, mut runtime) = build_access_runtime_fixture().await;
        let controller = AccessController::new(AccessControlConfig {
            enabled: true,
            admins: vec![ADMIN_A.to_string()],
            approvers: BTreeMap::from([(
                ROLE.to_string(),
                meerkat_meta::AccessApprover {
                    subjects: vec![ADMIN_B.to_string()],
                    ..meerkat_meta::AccessApprover::default()
                },
            )]),
            rules: vec![AccessRule {
                id: "gating-transport".to_string(),
                subjects: vec![ADMIN_B.to_string(), NEW_ADMIN.to_string()],
                actions: vec!["agent.view".to_string(), "gating.decide".to_string()],
                ..AccessRule::default()
            }],
            ..AccessControlConfig::default()
        })
        .expect("controller");
        let outsider = controller.view_for_subject(Some(NEW_ADMIN));
        assert!(outsider.allows("gating.decide"));
        assert!(outsider.held_approver_ids().is_empty());
        assert_eq!(
            controller
                .view_for_subject(Some(ADMIN_B))
                .held_approver_ids(),
            std::collections::BTreeSet::from([ROLE.to_string()])
        );
        runtime.set_access_controller(controller);
        let mut decisions = decision_state(true);
        decisions.trusted_oidc.discovery_json = json!({
            "issuer": ISSUER,
            "jwks_uri": format!("{ISSUER}/.well-known/jwks.json"),
        })
        .to_string();
        let app = runtime.build_reference_app_router(decisions);
        let evaluated = runtime
            .evaluate_gating_action_with_origin(
                serde_json::from_value(json!({
                    "action": "release the reviewed artifact",
                    "actor_id": "requester",
                    "risk_tier": "r3",
                    "requested_approver": ROLE,
                }))
                .expect("gating request"),
                Some(meerkat_meta::GatingOrigin {
                    identity: "router".to_string(),
                    conversation_id: None,
                    interaction_id: None,
                }),
            )
            .await;
        let pending_id = evaluated.pending_id.clone().expect("actual pending gate");
        let before = runtime.gating_state_snapshot().await;
        assert_eq!(before.pending.len(), 1);
        assert_eq!(before.pending[0].pending_id, pending_id);
        assert_eq!(before.audit.len(), 1);
        assert_eq!(before.audit[0].event_type, "pending_created");

        // Carol is authenticated and may invoke gating.decide, but does not
        // hold this role. Caller fields cannot lend her Alice's authority.
        let refused = authenticated_rpc(
            &app,
            NEW_ADMIN,
            "meta/gating/decide",
            json!({
                "pending_id": pending_id,
                "decision": "approve",
                "approver_id": ROLE,
                "deciding_principal": ADMIN_B,
                "held_approver_ids": [ROLE],
            }),
        )
        .await;
        let after_refusal = runtime.gating_state_snapshot().await;
        // Alice's actual owner-held role must arrive through the router.
        // These conflicting caller fields must neither redirect nor veto it.
        let permitted = authenticated_rpc(
            &app,
            ADMIN_B,
            "meta/gating/decide",
            json!({
                "pending_id": pending_id,
                "decision": "approve",
                "approver_id": NEW_ADMIN,
                "deciding_principal": NEW_ADMIN,
                "held_approver_ids": ["role:forged"],
            }),
        )
        .await;
        let after_permitted = runtime.gating_state_snapshot().await;
        drop(app);
        runtime.shutdown().await;

        assert!(refused["result"].is_null(), "{refused}");
        assert_eq!(refused["error"]["code"], json!(-32602), "{refused}");
        assert_eq!(
            after_refusal, before,
            "refusal must preserve exact owner state"
        );
        assert!(permitted["error"].is_null(), "{permitted}");
        let result: meerkat_meta::GatingDecisionResult =
            serde_json::from_value(permitted["result"].clone()).expect("actual decision result");
        assert_eq!(result.pending_id, pending_id);
        assert_eq!(result.action_id, evaluated.action_id);
        assert_eq!(result.approver_id, ROLE);
        assert_eq!(result.outcome, meerkat_meta::GatingOutcome::Allowed);
        assert!(after_permitted.pending.is_empty());
        assert_eq!(after_permitted.audit.len(), before.audit.len() + 1);
        assert_eq!(
            &after_permitted.audit[..before.audit.len()],
            before.audit.as_slice()
        );
        let audit = after_permitted.audit.last().expect("actual decision audit");
        assert_eq!(audit.event_type, "approval_decided");
        assert_eq!(audit.pending_id.as_deref(), Some(pending_id.as_str()));
        assert_eq!(audit.action_id, evaluated.action_id);
        assert_eq!(audit.actor_id, "requester");
        assert_eq!(audit.detail["deciding_principal"], json!(ADMIN_B));
        assert_eq!(audit.detail["approver_id"], json!(ROLE));
        assert_eq!(audit.detail["approver_binding"], json!("held"));
        assert_eq!(audit.detail["origin"]["identity"], json!("router"));
    }

    fn checkpoint(fixture: &Fixture) -> (AccessControlConfig, u64, Vec<u8>) {
        let (config, revision) = fixture.controller.snapshot();
        let bytes = std::fs::read(fixture.dir.path().join("access.toml")).expect("stored bytes");
        let disk: AccessControlConfig =
            toml::from_str(std::str::from_utf8(&bytes).expect("TOML UTF-8"))
                .expect("typed stored config");
        assert_eq!(disk, *config, "real store and owner agree");
        ((*config).clone(), revision, bytes)
    }

    fn checked(owner: &str, revision: u64, payload: Value) -> Value {
        let mut body = payload;
        body["owner_instance"] = json!(owner);
        body["expected_revision"] = json!(revision);
        json!({"checked_v1": body})
    }

    // Each required payload makes an observable edit when sent legally.
    // Groups are deliberately not referenced by a rule, so deletion is valid.
    fn mutations(config: &AccessControlConfig) -> Vec<(&'static str, Value, AccessControlConfig)> {
        let mut set = config.clone();
        set.admins.push(NEW_ADMIN.to_string());
        let rule = AccessRule {
            id: "new-rule".to_string(),
            subjects: vec![NEW_ADMIN.to_string()],
            actions: vec!["agent.send".to_string()],
            agents: vec!["worker".to_string()],
            ..AccessRule::default()
        };
        let mut upsert = config.clone();
        upsert.rules.push(rule.clone());
        let mut delete_rule = config.clone();
        delete_rule.rules.clear();
        let group = AccessGroup {
            members: vec![ADMIN_B.to_string()],
            ..AccessGroup::default()
        };
        let mut set_group = config.clone();
        let _ = set_group.groups.insert("ops".to_string(), group.clone());
        let mut delete_group = config.clone();
        let _ = delete_group.groups.remove("ops");
        let mut disable = config.clone();
        disable.enabled = false;
        vec![
            ("meta/access/set", json!({"config": set}), set),
            ("meta/access/rules/upsert", json!({"rule": rule}), upsert),
            (
                "meta/access/rules/delete",
                json!({"id": "existing-rule"}),
                delete_rule,
            ),
            (
                "meta/access/groups/set",
                json!({"name": "ops", "group": group}),
                set_group,
            ),
            (
                "meta/access/groups/delete",
                json!({"name": "ops"}),
                delete_group,
            ),
            ("meta/access/enable", json!({"enabled": false}), disable),
        ]
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct EditBase {
        owner: String,
        revision: u64,
        config: AccessControlConfig,
    }

    async fn read_edit_base(app: &axum::Router, subject: &str) -> EditBase {
        let status = authenticated_rpc(app, subject, "meta/access/status", json!({})).await;
        assert_eq!(status["error"], Value::Null, "{status:#?}");
        assert_eq!(status["result"]["subject"], json!(subject));
        assert_eq!(status["result"]["can_administer"], json!(true));
        assert_eq!(
            status["result"]["conditional_mutations"],
            json!("checked_v1"),
            "checked capability must describe the implemented owner: {status:#?}"
        );
        let read = authenticated_rpc(app, subject, "meta/access/get", json!({})).await;
        assert_eq!(read["error"], Value::Null, "{read:#?}");
        assert_eq!(read["result"]["conditional_mutations"], json!("checked_v1"));
        let owner = read["result"]["owner_instance"]
            .as_str()
            .expect("opaque owner instance");
        assert!(!owner.is_empty());
        EditBase {
            owner: owner.to_string(),
            revision: read["result"]["revision"]
                .as_u64()
                .expect("numeric revision"),
            config: serde_json::from_value(read["result"]["config"].clone()).expect("typed config"),
        }
    }

    fn safe_error(response: &Value, fixture: &Fixture) {
        assert_eq!(response["result"], Value::Null);
        let message = response["error"]["message"]
            .as_str()
            .expect("safe error message");
        assert!(!message.is_empty());
        let error = response["error"].to_string();
        for private in [
            PRIVATE_CONFIG,
            ADMIN_A,
            ADMIN_B,
            NEW_ADMIN,
            fixture.dir.path().to_str().expect("fixture path"),
        ] {
            assert!(
                !error.contains(private),
                "error leaked private detail: {error}"
            );
        }
    }

    async fn committed(
        fixture: &Fixture,
        response: &Value,
        before: &EditBase,
        expected: &AccessControlConfig,
    ) -> EditBase {
        assert_eq!(response["error"], Value::Null, "{response:#?}");
        assert_eq!(response["result"]["revision"], json!(before.revision + 1));
        let actual = checkpoint(fixture);
        assert_eq!(actual.0, *expected);
        assert_eq!(actual.1, before.revision + 1);
        let wire = read_edit_base(&fixture.app, ADMIN_B).await;
        assert_eq!(wire.owner, before.owner);
        assert_eq!(wire.revision, actual.1);
        assert_eq!(wire.config, actual.0);
        wire
    }

    #[tokio::test]
    async fn stale_save_preserves_newer_rule_then_fresh_reapply_and_current_admin_win() {
        let fixture = fixture();
        let initial = read_edit_base(&fixture.app, ADMIN_A).await;
        let cloned = fixture.controller.clone();
        let clone_app = app_for(&cloned);
        assert_eq!(
            read_edit_base(&clone_app, ADMIN_B).await,
            initial,
            "clones share edit identity"
        );
        let mut draft = initial.config.clone();
        draft.admins.push(NEW_ADMIN.to_string());
        let (_, rule_payload, newer_config) = mutations(&initial.config).remove(1);
        let b_save = authenticated_rpc(
            &fixture.app,
            ADMIN_B,
            "meta/access/rules/upsert",
            checked(&initial.owner, initial.revision, rule_payload),
        )
        .await;
        let newer = committed(&fixture, &b_save, &initial, &newer_config).await;
        assert_eq!(*cloned.snapshot().0, newer.config);
        assert_eq!(cloned.snapshot().1, newer.revision);
        let before_stale = checkpoint(&fixture);
        let stale = authenticated_rpc(
            &fixture.app,
            ADMIN_A,
            "meta/access/set",
            checked(&initial.owner, initial.revision, json!({"config": draft})),
        )
        .await;
        assert_eq!(stale["error"]["code"], json!(-32009), "{stale:#?}");
        assert_eq!(
            stale["error"]["data"],
            json!({
                "kind": "access_revision_conflict",
                "expected_revision": initial.revision,
                "actual_revision": newer.revision,
            })
        );
        safe_error(&stale, &fixture);
        assert_eq!(
            checkpoint(&fixture),
            before_stale,
            "conflict preserves exact disk/config/revision"
        );
        assert_eq!(read_edit_base(&fixture.app, ADMIN_B).await, newer);

        // Explicit review/reapply merges only A's admin edit with B's new rule.
        let mut reviewed = newer.config.clone();
        reviewed.admins = draft.admins;
        let fresh = authenticated_rpc(
            &fixture.app,
            ADMIN_A,
            "meta/access/set",
            checked(&newer.owner, newer.revision, json!({"config": reviewed})),
        )
        .await;
        let reapplied = committed(&fixture, &fresh, &newer, &reviewed).await;
        assert_eq!(reapplied.config.rules, newer.config.rules);

        let mut revoked = reapplied.config.clone();
        revoked.admins.retain(|subject| subject != ADMIN_A);
        let revoke = authenticated_rpc(
            &fixture.app,
            ADMIN_B,
            "meta/access/set",
            checked(
                &reapplied.owner,
                reapplied.revision,
                json!({"config": revoked}),
            ),
        )
        .await;
        let current = committed(&fixture, &revoke, &reapplied, &revoked).await;
        assert!(
            !fixture
                .controller
                .view_for_subject(Some(ADMIN_A))
                .can_administer()
        );
        let after_revoke = checkpoint(&fixture);
        // Known fresh identity is not permission; stale/wrong identities must
        // not disclose conflict details to the now-revoked administrator.
        for (owner, revision) in [
            (current.owner.as_str(), current.revision),
            (reapplied.owner.as_str(), reapplied.revision),
            ("different-owner", current.revision),
        ] {
            let denied = authenticated_rpc(
                &fixture.app,
                ADMIN_A,
                "meta/access/set",
                checked(owner, revision, json!({"config": reviewed})),
            )
            .await;
            assert_eq!(denied["error"]["code"], json!(-32030), "{denied:#?}");
            assert_eq!(denied["error"]["data"], json!({"kind": "access_denied"}));
            safe_error(&denied, &fixture);
            assert_eq!(checkpoint(&fixture), after_revoke);
        }
        let mut restored = current.config.clone();
        restored.admins.push(ADMIN_A.to_string());
        let healthy = authenticated_rpc(
            &fixture.app,
            ADMIN_B,
            "meta/access/set",
            checked(
                &current.owner,
                current.revision,
                json!({"config": restored}),
            ),
        )
        .await;
        committed(&fixture, &healthy, &current, &restored).await;
    }

    #[tokio::test]
    async fn replacement_owner_rejects_same_revision_token_then_accepts_fresh_read() {
        let mut fixture = fixture();
        let old = read_edit_base(&fixture.app, ADMIN_A).await;
        let disk_before = checkpoint(&fixture);
        // Load the same real file at the same route/logical runtime. Both
        // constructors start at revision zero; only the owner is replaced.
        let replacement = AccessController::load(fixture.dir.path().join("access.toml"))
            .expect("replacement real owner");
        fixture.app = app_for(&replacement);
        fixture.controller = replacement;
        let current = read_edit_base(&fixture.app, ADMIN_A).await;
        assert_eq!(current.revision, old.revision);
        assert_eq!(current.config, old.config);
        assert_ne!(
            current.owner, old.owner,
            "revision reuse must not reuse owner identity"
        );
        let mut draft = current.config.clone();
        draft.admins.push(NEW_ADMIN.to_string());
        let stale = authenticated_rpc(
            &fixture.app,
            ADMIN_A,
            "meta/access/set",
            checked(&old.owner, old.revision, json!({"config": draft})),
        )
        .await;
        assert_eq!(stale["error"]["code"], json!(-32009), "{stale:#?}");
        assert_eq!(
            stale["error"]["data"],
            json!({"kind": "access_owner_changed"})
        );
        safe_error(&stale, &fixture);
        assert_eq!(checkpoint(&fixture), disk_before);
        assert_eq!(read_edit_base(&fixture.app, ADMIN_B).await, current);
        let fresh = authenticated_rpc(
            &fixture.app,
            ADMIN_A,
            "meta/access/set",
            checked(&current.owner, current.revision, json!({"config": draft})),
        )
        .await;
        committed(&fixture, &fresh, &current, &draft).await;
    }

    #[tokio::test]
    async fn all_six_mutations_reject_malformed_or_mixed_envelopes_without_fallback() {
        let seed = fixture();
        let cases = mutations(&seed.controller.snapshot().0);
        for (method, payload, expected) in cases {
            let fixture = fixture();
            let base = read_edit_base(&fixture.app, ADMIN_A).await;
            let initial = checkpoint(&fixture);
            let valid = checked(&base.owner, base.revision, payload.clone());
            let mut missing_owner = valid.clone();
            let _ = missing_owner["checked_v1"]
                .as_object_mut()
                .expect("object")
                .remove("owner_instance");
            let mut missing_revision = valid.clone();
            let _ = missing_revision["checked_v1"]
                .as_object_mut()
                .expect("object")
                .remove("expected_revision");
            let mut null_revision = valid.clone();
            null_revision["checked_v1"]["expected_revision"] = Value::Null;
            let mut negative_revision = valid.clone();
            negative_revision["checked_v1"]["expected_revision"] = json!(-1);
            let mut overflow_revision = valid.clone();
            overflow_revision["checked_v1"]["expected_revision"] =
                serde_json::from_str("18446744073709551616").expect("valid JSON exceeding u64");
            let mut mixed_valid = payload.clone();
            mixed_valid["checked_v1"] = valid["checked_v1"].clone();
            let mut mixed_null = payload.clone();
            mixed_null["checked_v1"] = Value::Null;
            let mut mixed_unknown = payload.clone();
            mixed_unknown["checked_v2"] = valid["checked_v1"].clone();
            for (case, params) in [
                ("null envelope", json!({"checked_v1": null})),
                ("missing owner", missing_owner),
                ("missing revision", missing_revision),
                ("null revision", null_revision),
                ("negative revision", negative_revision),
                ("overflow revision", overflow_revision),
                (
                    "missing write payload",
                    checked(&base.owner, base.revision, json!({})),
                ),
                (
                    "unknown version",
                    json!({"checked_v2": valid["checked_v1"]}),
                ),
                ("mixed valid envelope", mixed_valid),
                ("mixed null envelope", mixed_null),
                ("unknown version with legacy payload", mixed_unknown),
            ] {
                let response = authenticated_rpc(&fixture.app, ADMIN_A, method, params).await;
                assert_eq!(
                    response["error"]["code"],
                    json!(-32602),
                    "{method}/{case}: {response:#?}"
                );
                safe_error(&response, &fixture);
                assert_eq!(
                    checkpoint(&fixture),
                    initial,
                    "{method}/{case}: no fallback/persist/revision"
                );
            }
            assert_eq!(read_edit_base(&fixture.app, ADMIN_B).await, base);
            let healthy = authenticated_rpc(&fixture.app, ADMIN_A, method, valid).await;
            committed(&fixture, &healthy, &base, &expected).await;
            assert_ne!(
                checkpoint(&fixture).2,
                initial.2,
                "{method}: real legal edit persisted"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_same_revision_commits_one_distinct_write_and_one_conflict() {
        let fixture = fixture();
        let cloned_owner = fixture.controller.clone();
        let second_app = app_for(&cloned_owner);
        let initial = read_edit_base(&fixture.app, ADMIN_A).await;
        assert_eq!(read_edit_base(&second_app, ADMIN_B).await, initial);
        let before = checkpoint(&fixture);
        assert_eq!(before.0, initial.config);
        assert_eq!(before.1, initial.revision);

        let mut a_config = initial.config.clone();
        let _ = a_config.groups.insert(
            "concurrent-a".to_string(),
            AccessGroup {
                members: vec![ADMIN_A.to_string()],
                ..AccessGroup::default()
            },
        );
        let mut b_config = initial.config.clone();
        let _ = b_config.groups.insert(
            "concurrent-b".to_string(),
            AccessGroup {
                members: vec![ADMIN_B.to_string()],
                ..AccessGroup::default()
            },
        );
        assert_ne!(a_config, b_config);
        assert_ne!(a_config, initial.config);
        assert_ne!(b_config, initial.config);
        let a_payload = checked(
            &initial.owner,
            initial.revision,
            json!({"config": a_config}),
        );
        let b_payload = checked(
            &initial.owner,
            initial.revision,
            json!({"config": b_config}),
        );

        // Both real HTTP calls share one owner and the same read precondition.
        // This start barrier does not claim an internal mutex-waiter schedule.
        let start = Arc::new(tokio::sync::Barrier::new(3));
        let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel(2);
        let mut tasks = tokio::task::JoinSet::new();
        for (label, app, subject, payload) in [
            ("a", fixture.app.clone(), ADMIN_A, a_payload),
            ("b", second_app.clone(), ADMIN_B, b_payload),
        ] {
            let start = Arc::clone(&start);
            let ready_tx = ready_tx.clone();
            tasks.spawn(async move {
                ready_tx
                    .send(label)
                    .await
                    .expect("parent owns readiness receiver");
                start.wait().await;
                let response = authenticated_rpc(&app, subject, "meta/access/set", payload).await;
                (label, response)
            });
        }
        drop(ready_tx);
        let run = tokio::time::timeout(Duration::from_secs(10), async {
            let ready = [ready_rx.recv().await, ready_rx.recv().await];
            let before_release = checkpoint(&fixture);
            start.wait().await;
            let mut joined = Vec::new();
            while let Some(result) = tasks.join_next().await {
                joined.push(result);
            }
            (ready, before_release, joined)
        })
        .await;
        // Always cancel/drain owned tasks before inspecting semantic results.
        // JoinSet also aborts its tasks if an earlier fixture assertion panics.
        tasks.abort_all();
        let drained = tokio::time::timeout(Duration::from_secs(2), async {
            while tasks.join_next().await.is_some() {}
        })
        .await;
        assert!(drained.is_ok(), "owned request tasks must quiesce");
        let (mut ready, before_release, joined) = run.expect("bounded concurrent saves");
        ready.sort();
        assert_eq!(ready, [Some("a"), Some("b")]);
        assert_eq!(
            before_release, before,
            "neither request enters before release"
        );
        assert_eq!(joined.len(), 2);
        let outcomes: Vec<_> = joined
            .into_iter()
            .map(|result| result.expect("request task must not panic"))
            .collect();
        let mut labels: Vec<_> = outcomes.iter().map(|(label, _)| *label).collect();
        labels.sort_unstable();
        assert_eq!(labels, vec!["a", "b"]);
        let successes: Vec<_> = outcomes
            .iter()
            .filter(|(_, response)| response["error"].is_null())
            .collect();
        let conflicts: Vec<_> = outcomes
            .iter()
            .filter(|(_, response)| response["error"]["code"] == json!(-32009))
            .collect();
        assert_eq!(
            successes.len(),
            1,
            "exactly one accepted write: {outcomes:#?}"
        );
        assert_eq!(conflicts.len(), 1, "exactly one stale write: {outcomes:#?}");
        let (winner, success) = successes[0];
        let (loser, conflict) = conflicts[0];
        assert_ne!(winner, loser);
        assert_eq!(
            conflict["error"]["data"],
            json!({
                "kind": "access_revision_conflict",
                "expected_revision": initial.revision,
                "actual_revision": initial.revision + 1,
            })
        );
        safe_error(conflict, &fixture);
        let (expected, rejected) = if *winner == "a" {
            (&a_config, &b_config)
        } else {
            (&b_config, &a_config)
        };
        let final_edit = committed(&fixture, success, &initial, expected).await;
        let after = checkpoint(&fixture);
        assert_eq!(after.1, initial.revision + 1, "one owner commit, not two");
        assert_eq!(after.0, *expected);
        assert_ne!(
            after.0, *rejected,
            "losing whole-config write is not published"
        );
        assert_eq!(*cloned_owner.snapshot().0, after.0);
        assert_eq!(cloned_owner.snapshot().1, after.1);
        let header = "# Meta access control. Managed by the console Access panel;\n# hand edits are preserved until the next console save.\n\n";
        let expected_bytes = format!(
            "{header}{}",
            toml::to_string_pretty(expected).expect("winning typed configuration serializes")
        )
        .into_bytes();
        assert_eq!(
            after.2, expected_bytes,
            "exact winning configuration is persisted"
        );
        assert_ne!(after.2, before.2);
        assert_eq!(read_edit_base(&fixture.app, ADMIN_A).await, final_edit);
        assert_eq!(read_edit_base(&second_app, ADMIN_B).await, final_edit);
        assert_eq!(
            checkpoint(&fixture),
            after,
            "readback causes no further mutation"
        );
    }
}
