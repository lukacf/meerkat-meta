#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::uninlined_format_args
)]
//! Integration test for `meta/list_flows` and `meta/run_flow`.
//!
//! Boots a `UnifiedRuntime` against a mob definition that declares a
//! single flow, then exercises the new RPC surface end-to-end:
//! list -> run -> status.

use std::sync::Arc;
use std::time::Duration;

use meerkat::{AgentFactory, Config, build_ephemeral_service};
use meerkat_client::TestClient;
use meerkat_meta::{
    DiscoverySpec, JsonRpcResponse, MetaConfig, MobBootstrapOptions, MobBootstrapSpec,
    UnifiedRuntime, handle_unified_rpc_json,
};
use meerkat_mob::{MobDefinition, MobStorage};
use serde_json::{Value, json};
use tempfile::TempDir;

/// Per-test mob id counter: 0.8.23's fail-closed in-proc registration
/// means concurrently running tests must not share a supervisor route.
static NEXT_TEST_MOB_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The one definition of the normalized-provider-accounting contract every
/// Meta LLM double must satisfy under meerkat 0.8.22. See the module docs.
#[path = "support/llm_usage.rs"]
mod llm_usage;

struct Fixture {
    _temp_dir: TempDir,
    runtime: UnifiedRuntime,
}

/// `TestClient::default()` DOES synthesize accounting under 0.8.22, but under
/// `Provider::Other` - and the flow profile is `gpt-5.5`, whose canonical owner
/// is `Provider::OpenAI`, so the turn would fail closed with
/// `normalized_provider_accounting_identity_mismatch`. See the rule in
/// `tests/support/llm_usage.rs`.
async fn build_unified_runtime_with_flow() -> Fixture {
    build_unified_runtime_with_flow_and_client(Arc::new(TestClient::for_provider(
        meerkat::Provider::OpenAI,
    )))
    .await
}

async fn build_unified_runtime_with_flow_and_client(
    default_llm_client: Arc<dyn meerkat::LlmClient>,
) -> Fixture {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let session_path = temp_dir.path().join("sessions");
    std::fs::create_dir_all(&session_path).expect("session path");

    let factory = AgentFactory::new(&session_path).comms(true);
    let session_service = Arc::new(build_ephemeral_service(factory, Config::default(), 16));

    // Mob definition with one flow ("demo") so list_flows has something to
    // return. The flow has a single trivial step targeting the lead role.
    let definition = MobDefinition::from_toml(&format!(
        r#"
[mob]
id = "flow-test-mob-{}"

[profiles.lead]
model = "gpt-5.5"
external_addressable = true

[profiles.lead.tools]
comms = true

[flows.demo]
description = "demo flow"

[flows.demo.steps.first]
role = "lead"
message = "first"
"#,
        NEXT_TEST_MOB_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
    .expect("parse mob definition");

    let mob_spec = MobBootstrapSpec::new(definition, MobStorage::in_memory(), session_service)
        .with_options(MobBootstrapOptions {
            allow_ephemeral_sessions: true,
            notify_orchestrator_on_resume: true,
            default_llm_client: Some(default_llm_client),
        });
    let module_config = MetaConfig {
        modules: vec![],
        discovery: DiscoverySpec {
            namespace: "list-flows-run-flow-test".to_string(),
            modules: vec![],
        },
        pre_spawn: vec![],
    };

    let runtime = UnifiedRuntime::bootstrap(mob_spec, module_config, Duration::from_secs(2))
        .await
        .expect("bootstrap unified runtime");

    Fixture {
        _temp_dir: temp_dir,
        runtime,
    }
}

fn parse_json_rpc(response: &str) -> JsonRpcResponse {
    serde_json::from_str(response).expect("json-rpc response")
}

#[tokio::test]
async fn list_flows_returns_configured_flow_ids() {
    let fixture = build_unified_runtime_with_flow().await;

    let response = parse_json_rpc(
        &handle_unified_rpc_json(
            &fixture.runtime,
            &json!({
                "jsonrpc": "2.0",
                "id": "list-1",
                "method": "meta/list_flows",
                "params": {}
            })
            .to_string(),
            Duration::from_secs(1),
            None,
            None,
        )
        .await,
    );

    assert!(
        response.error.is_none(),
        "list_flows should not error: {:?}",
        response.error
    );
    let result = response.result.expect("list_flows result");
    let flows = result
        .get("flows")
        .and_then(Value::as_array)
        .expect("flows array");
    let flow_ids: Vec<String> = flows
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    assert!(
        flow_ids.iter().any(|id| id == "demo"),
        "demo flow should be advertised, got {:?}",
        flow_ids
    );

    let shutdown = fixture.runtime.shutdown().await;
    assert!(
        shutdown.mob_stop.is_ok(),
        "mob stop failed at teardown: {:?}",
        shutdown.mob_stop
    );
}

#[tokio::test]
async fn run_flow_starts_run_and_status_observes_it() {
    let fixture = build_unified_runtime_with_flow().await;

    // Start the flow.
    let run_response = parse_json_rpc(
        &handle_unified_rpc_json(
            &fixture.runtime,
            &json!({
                "jsonrpc": "2.0",
                "id": "run-1",
                "method": "meta/run_flow",
                "params": {
                    "flow_id": "demo",
                    "params": { "hello": "world" }
                }
            })
            .to_string(),
            Duration::from_secs(2),
            None,
            None,
        )
        .await,
    );

    assert!(
        run_response.error.is_none(),
        "run_flow should not error: {:?}",
        run_response.error
    );
    let run_result = run_response.result.expect("run_flow result");
    let run_id = run_result
        .get("run_id")
        .and_then(Value::as_str)
        .expect("run_id string")
        .to_string();
    assert!(!run_id.is_empty(), "run_id must be non-empty");

    // Status should be observable for the freshly minted run id.
    let status_response = parse_json_rpc(
        &handle_unified_rpc_json(
            &fixture.runtime,
            &json!({
                "jsonrpc": "2.0",
                "id": "status-1",
                "method": "meta/flow_status",
                "params": { "run_id": run_id }
            })
            .to_string(),
            Duration::from_secs(2),
            None,
            None,
        )
        .await,
    );

    assert!(
        status_response.error.is_none(),
        "flow_status should not error: {:?}",
        status_response.error
    );
    // flow_status returns either the MobRun JSON or null. We require the
    // run is observable (not null) immediately after the run_flow call.
    let status = status_response.result.expect("flow_status result");
    assert!(
        !status.is_null(),
        "freshly started run should be observable in flow_status, got null"
    );

    let shutdown = fixture.runtime.shutdown().await;
    assert!(
        shutdown.mob_stop.is_ok(),
        "mob stop failed at teardown: {:?}",
        shutdown.mob_stop
    );
}

#[tokio::test]
async fn run_flow_rejects_unknown_flow_id_with_invalid_params() {
    let fixture = build_unified_runtime_with_flow().await;

    let response = parse_json_rpc(
        &handle_unified_rpc_json(
            &fixture.runtime,
            &json!({
                "jsonrpc": "2.0",
                "id": "run-bad",
                "method": "meta/run_flow",
                "params": {
                    "flow_id": "does-not-exist",
                    "params": null
                }
            })
            .to_string(),
            Duration::from_secs(1),
            None,
            None,
        )
        .await,
    );

    let error = response.error.expect("unknown flow should error");
    assert_eq!(
        error.code, -32602,
        "unknown flow id should map to invalid-params (-32602), got {}: {}",
        error.code, error.message
    );

    let shutdown = fixture.runtime.shutdown().await;
    assert!(
        shutdown.mob_stop.is_ok(),
        "mob stop failed at teardown: {:?}",
        shutdown.mob_stop
    );
}

/// LLM client whose turn never completes within the test window — the flow
/// step's member turn is guaranteed in flight when shutdown runs.
struct HangingTestClient;

impl meerkat::LlmClient for HangingTestClient {
    fn stream<'a>(
        &'a self,
        request: &'a meerkat::LlmRequest,
    ) -> std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<meerkat::LlmEvent, meerkat::LlmError>> + Send + 'a>,
    > {
        // The turn is meant to still be in flight when the test tears the
        // runtime down, so this tail is not expected to be reached. It carries
        // normalized accounting anyway: under 0.8.22 a reached `Done` without
        // it fails the turn closed, and "unreachable" is a property of the
        // test's timing, not of this client.
        let [usage, done] = llm_usage::usage_then_done(
            request,
            meerkat::Provider::OpenAI,
            meerkat::StopReason::EndTurn,
        );
        Box::pin(async_stream::stream! {
            tokio::time::sleep(Duration::from_mins(5)).await;
            yield Ok(usage);
            yield Ok(done);
        })
    }

    fn provider(&self) -> meerkat::Provider {
        meerkat::Provider::OpenAI
    }

    fn health_check<'life0, 'async_trait>(
        &'life0 self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), meerkat::LlmError>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { Ok(()) })
    }
}

/// meerkat 0.7.25: the mob machine refuses `Stop` while member work is in
/// flight (`InvalidTransition { from: Running, to: Stopped }`) instead of
/// stopping underneath it — the exact CI teardown failure this suite kept
/// hitting under load (the run was still executing when shutdown ran; fast
/// local boxes finished it first). Deterministic repro: run the flow against
/// a hanging LLM client so the step turn is GUARANTEED in flight, then shut
/// down. `UnifiedRuntime::shutdown` must quiesce (cancel member work) and
/// stop cleanly rather than reporting the machine's busy refusal.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_quiesces_an_in_flight_flow_run() {
    let fixture = build_unified_runtime_with_flow_and_client(Arc::new(HangingTestClient)).await;
    let run_response = parse_json_rpc(
        &handle_unified_rpc_json(
            &fixture.runtime,
            &json!({
                "jsonrpc": "2.0",
                "id": "run-hang",
                "method": "meta/run_flow",
                "params": { "flow_id": "demo", "params": {} }
            })
            .to_string(),
            Duration::from_secs(2),
            None,
            None,
        )
        .await,
    );
    assert!(
        run_response.error.is_none(),
        "run_flow should not error: {:?}",
        run_response.error
    );

    let shutdown = fixture.runtime.shutdown().await;
    assert!(
        shutdown.mob_stop.is_ok(),
        "shutdown must quiesce the in-flight run and stop: {:?}",
        shutdown.mob_stop
    );
}
