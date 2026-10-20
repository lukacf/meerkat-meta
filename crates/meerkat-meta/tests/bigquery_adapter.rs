#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::uninlined_format_args,
    clippy::collapsible_if,
    clippy::redundant_clone,
    clippy::needless_raw_string_hashes,
    clippy::single_match,
    clippy::redundant_closure_for_method_calls,
    clippy::redundant_pattern_matching,
    clippy::ignored_unit_patterns,
    clippy::clone_on_copy,
    clippy::manual_assert,
    clippy::unwrap_in_result,
    clippy::useless_vec
)]
use std::net::TcpListener;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use meerkat_meta::{
    BigQuerySessionStoreAdapter, BigQuerySessionStoreError, DiscoverySpec, MetaConfig,
    SessionPersistenceRow, handle_meta_rpc_json, start_meta_runtime,
};
use serde_json::{Value, json};

#[path = "support/bigquery_http_mock.rs"]
mod bigquery_http_mock;

use bigquery_http_mock::{MockHttpResponse, MockHttpServer};

const REAL_BQ_PROJECT: &str = "king-dnn-training-dev";
const DEFAULT_BQ_API_BASE_URL: &str = "https://bigquery.googleapis.com/bigquery/v2";

fn sample_write_rows() -> Vec<SessionPersistenceRow> {
    vec![SessionPersistenceRow {
        session_id: "sample-session".to_string(),
        updated_at_ms: 10_000,
        deleted: false,
        payload: json!({"step":"create"}),
        ..Default::default()
    }]
}

fn start_phase_f_rpc_runtime() -> meerkat_meta::MetaRuntimeHandle {
    start_meta_runtime(
        MetaConfig {
            modules: vec![],
            discovery: DiscoverySpec {
                namespace: "phase_f_rpc".to_string(),
                modules: vec![],
            },
            pre_spawn: vec![],
        },
        vec![],
        Duration::from_millis(250),
    )
    .expect("start runtime")
}

fn call_rpc(
    runtime: &mut meerkat_meta::MetaRuntimeHandle,
    request: Value,
    timeout: Duration,
) -> Value {
    let response_line = handle_meta_rpc_json(
        runtime,
        &serde_json::to_string(&request).expect("serialize rpc request"),
        timeout,
    );
    serde_json::from_str(&response_line).expect("parse rpc response")
}

#[tokio::test]
async fn phase_f_contract_native_bigquery_transport_request_shape_and_query_parsing() {
    let writes = vec![
        SessionPersistenceRow {
            session_id: "s1".to_string(),
            updated_at_ms: 1_000,
            deleted: false,
            payload: json!({"step":"create"}),
            ..Default::default()
        },
        SessionPersistenceRow {
            session_id: "s1".to_string(),
            updated_at_ms: 2_000,
            deleted: true,
            payload: json!({}),
            ..Default::default()
        },
        SessionPersistenceRow {
            session_id: "s2".to_string(),
            updated_at_ms: 1_500,
            deleted: false,
            payload: json!({"step":"create"}),
            ..Default::default()
        },
        SessionPersistenceRow {
            session_id: "s2".to_string(),
            updated_at_ms: 3_000,
            deleted: false,
            payload: json!({"step":"update","version":2}),
            ..Default::default()
        },
    ];
    // read_latest_rows now uses server-side QUALIFY dedup; mock returns already-deduped rows
    let latest_query_rows = json!({
        "jobComplete": true,
        "rows": [
            {"f":[{"v":"s1"},{"v":"2000"},{"v":"true"},{"v":"{}"},{"v":"{}"}]},
            {"f":[{"v":"s2"},{"v":"3000"},{"v":"false"},{"v":"{\"step\":\"update\",\"version\":2}"}]}
        ]
    });
    // read_live_rows uses server-side QUALIFY dedup + deleted=false filter
    let live_query_rows = json!({
        "jobComplete": true,
        "rows": [
            {"f":[{"v":"s2"},{"v":"3000"},{"v":"false"},{"v":"{\"step\":\"update\",\"version\":2}"}]}
        ]
    });

    let server = MockHttpServer::start(vec![
        MockHttpResponse::json(json!({})),
        MockHttpResponse::json(latest_query_rows),
        MockHttpResponse::json(live_query_rows),
    ]);
    let store = BigQuerySessionStoreAdapter::new_native("phase_f_dataset", "phase_f_table")
        .with_project_id("phase-f-project")
        .with_access_token("phase-f-token")
        .with_api_base_url(format!("{}/bigquery/v2", server.base_url()));

    store
        .stream_insert_rows(&writes)
        .await
        .expect("insertAll contract call should succeed");
    let latest = store
        .read_latest_rows()
        .await
        .expect("read latest via query API");
    let live = store
        .read_live_rows()
        .await
        .expect("read live via query API");

    assert_eq!(
        latest,
        vec![
            SessionPersistenceRow {
                session_id: "s1".to_string(),
                updated_at_ms: 2_000,
                deleted: true,
                payload: json!({}),
                ..Default::default()
            },
            SessionPersistenceRow {
                session_id: "s2".to_string(),
                updated_at_ms: 3_000,
                deleted: false,
                payload: json!({"step":"update","version":2}),
                ..Default::default()
            },
        ]
    );
    assert_eq!(
        live,
        vec![SessionPersistenceRow {
            session_id: "s2".to_string(),
            updated_at_ms: 3_000,
            deleted: false,
            payload: json!({"step":"update","version":2}),
            ..Default::default()
        }]
    );

    let requests = server.captured_requests();
    assert_eq!(requests.len(), 3);

    let insert_request = &requests[0];
    assert_eq!(insert_request.method, "POST");
    assert_eq!(
        insert_request.path,
        "/bigquery/v2/projects/phase-f-project/datasets/phase_f_dataset/tables/phase_f_table/insertAll"
    );
    assert_eq!(
        insert_request
            .headers
            .get("authorization")
            .expect("insert authorization header"),
        "Bearer phase-f-token"
    );
    let insert_body: Value =
        serde_json::from_str(&insert_request.body).expect("parse insert request body");
    let inserted_rows = insert_body["rows"]
        .as_array()
        .expect("insert request rows array");
    assert_eq!(inserted_rows.len(), writes.len());
    // MK-008: insertId must not be present
    for row in inserted_rows {
        assert!(
            row.get("insertId").is_none(),
            "insertId must be removed from BQ streaming inserts"
        );
    }
    assert_eq!(
        insert_body["rows"][0]["json"]["session_id"],
        Value::String("s1".to_string())
    );
    assert_eq!(
        insert_body["rows"][0]["json"]["payload"],
        Value::String("{\"step\":\"create\"}".to_string())
    );
    assert_eq!(insert_body["rows"][1]["json"]["deleted"], Value::Bool(true));

    let query_request = &requests[1];
    assert_eq!(query_request.method, "POST");
    assert_eq!(
        query_request.path,
        "/bigquery/v2/projects/phase-f-project/queries"
    );
    let query_body: Value =
        serde_json::from_str(&query_request.body).expect("parse query request body");
    let query_text = query_body["query"].as_str().expect("query body query text");
    assert!(query_text.contains("SELECT session_id, updated_at_ms, deleted, payload"));
    assert!(query_text.contains("phase-f-project.phase_f_dataset.phase_f_table"));
    // MK-009: read_latest_rows must use server-side QUALIFY dedup
    assert!(
        query_text.contains("QUALIFY ROW_NUMBER()"),
        "read_latest_rows query must use QUALIFY for server-side dedup"
    );
}

#[test]
fn phase_f_rpc_builtin_bigquery_path_uses_native_adapter() {
    let query_rows = json!({
        "rows": [
            {"f":[{"v":"rpc-session"},{"v":"1234"},{"v":"false"},{"v":"{\"kind\":\"rpc\"}"}]}
        ]
    });
    let server = MockHttpServer::start(vec![MockHttpResponse::json(query_rows)]);
    let mut runtime = start_phase_f_rpc_runtime();

    let request = json!({
        "jsonrpc":"2.0",
        "id":"phase-f-rpc-bq-1",
        "method":"meta/session_store/bigquery",
        "params":{
            "operation":"read_latest_rows",
            "dataset":"phase_f_dataset",
            "table":"phase_f_table",
            "project_id":"phase-f-project",
            "access_token":"phase-f-token",
            "api_base_url": format!("{}/bigquery/v2", server.base_url())
        }
    });
    let response = call_rpc(&mut runtime, request, Duration::from_secs(1));
    assert!(
        response.get("error").is_none(),
        "unexpected RPC error: {response}"
    );
    assert_eq!(response["result"]["operation"], "read_latest_rows");
    assert_eq!(
        response["result"]["rows"],
        json!([{
            "session_id":"rpc-session",
            "updated_at_ms":1234,
            "deleted":false,
            "payload":{"kind":"rpc"},"labels":{}
        }])
    );

    let requests = server.captured_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(
        requests[0].path,
        "/bigquery/v2/projects/phase-f-project/queries"
    );
}

#[test]
fn phase_f_rpc_capabilities_discover_bigquery_session_store_builtin() {
    let mut runtime = start_phase_f_rpc_runtime();
    let capabilities = call_rpc(
        &mut runtime,
        json!({
            "jsonrpc":"2.0",
            "id":"phase-f-rpc-caps-1",
            "method":"meta/capabilities",
            "params":{}
        }),
        Duration::from_secs(1),
    );
    let methods = capabilities["result"]["methods"]
        .as_array()
        .expect("capabilities methods array");
    assert!(
        methods
            .iter()
            .any(|method| method == "meta/session_store/bigquery"),
        "meta/capabilities must advertise meta/session_store/bigquery"
    );
}

#[test]
fn phase_f_rpc_bigquery_stream_insert_rows_issues_insert_all_request() {
    let server = MockHttpServer::start(vec![MockHttpResponse::json(json!({}))]);
    let mut runtime = start_phase_f_rpc_runtime();
    let rows = vec![
        SessionPersistenceRow {
            session_id: "rpc-s1".to_string(),
            updated_at_ms: 101,
            deleted: false,
            payload: json!({"step":"create"}),
            ..Default::default()
        },
        SessionPersistenceRow {
            session_id: "rpc-s1".to_string(),
            updated_at_ms: 202,
            deleted: true,
            payload: json!({}),
            ..Default::default()
        },
    ];

    let response = call_rpc(
        &mut runtime,
        json!({
            "jsonrpc":"2.0",
            "id":"phase-f-rpc-bq-insert",
            "method":"meta/session_store/bigquery",
            "params":{
                "operation":"stream_insert_rows",
                "dataset":"phase_f_dataset",
                "table":"phase_f_table",
                "project_id":"phase-f-project",
                "access_token":"phase-f-token",
                "api_base_url": format!("{}/bigquery/v2", server.base_url()),
                "rows": rows
            }
        }),
        Duration::from_secs(1),
    );
    assert!(
        response.get("error").is_none(),
        "unexpected RPC error: {response}"
    );
    assert_eq!(response["result"]["operation"], "stream_insert_rows");
    assert_eq!(response["result"]["accepted"], true);
    assert_eq!(response["result"]["inserted_rows"], 2);

    let requests = server.captured_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].path,
        "/bigquery/v2/projects/phase-f-project/datasets/phase_f_dataset/tables/phase_f_table/insertAll"
    );
    let insert_body: Value =
        serde_json::from_str(&requests[0].body).expect("parse insert request body");
    let rpc_inserted_rows = insert_body["rows"].as_array().expect("insert rows array");
    assert_eq!(rpc_inserted_rows.len(), 2);
    // MK-008: insertId must not be present
    for row in rpc_inserted_rows {
        assert!(
            row.get("insertId").is_none(),
            "insertId must be removed from BQ streaming inserts"
        );
    }
    assert_eq!(insert_body["rows"][0]["json"]["session_id"], "rpc-s1");
    assert_eq!(insert_body["rows"][0]["json"]["updated_at_ms"], "101");
    assert_eq!(
        insert_body["rows"][0]["json"]["payload"],
        "{\"step\":\"create\"}"
    );
    assert_eq!(insert_body["rows"][1]["json"]["deleted"], true);
}

#[test]
fn phase_f_rpc_bigquery_read_rows_and_read_live_rows_semantics() {
    // read_rows returns all raw rows (no server-side dedup)
    let all_query_rows = json!({
        "rows": [
            {"f":[{"v":"s1"},{"v":"1000"},{"v":"false"},{"v":"{\"step\":\"create\"}"},{"v":"{}"},{"v":"{}"}]},
            {"f":[{"v":"s1"},{"v":"2000"},{"v":"true"},{"v":"{}"},{"v":"{}"}]},
            {"f":[{"v":"s2"},{"v":"1500"},{"v":"false"},{"v":"{\"step\":\"create\"}"},{"v":"{}"},{"v":"{}"}]},
            {"f":[{"v":"s2"},{"v":"3000"},{"v":"false"},{"v":"{\"step\":\"update\",\"version\":2}"}]}
        ]
    });
    // read_live_rows uses server-side QUALIFY dedup + deleted=false filter
    let live_query_rows = json!({
        "rows": [
            {"f":[{"v":"s2"},{"v":"3000"},{"v":"false"},{"v":"{\"step\":\"update\",\"version\":2}"}]}
        ]
    });
    let server = MockHttpServer::start(vec![
        MockHttpResponse::json(all_query_rows),
        MockHttpResponse::json(live_query_rows),
    ]);
    let mut runtime = start_phase_f_rpc_runtime();

    let read_all = call_rpc(
        &mut runtime,
        json!({
            "jsonrpc":"2.0",
            "id":"phase-f-rpc-bq-read-all",
            "method":"meta/session_store/bigquery",
            "params":{
                "operation":"read_rows",
                "dataset":"phase_f_dataset",
                "table":"phase_f_table",
                "project_id":"phase-f-project",
                "access_token":"phase-f-token",
                "api_base_url": format!("{}/bigquery/v2", server.base_url())
            }
        }),
        Duration::from_secs(1),
    );
    let read_live = call_rpc(
        &mut runtime,
        json!({
            "jsonrpc":"2.0",
            "id":"phase-f-rpc-bq-read-live",
            "method":"meta/session_store/bigquery",
            "params":{
                "operation":"read_live_rows",
                "dataset":"phase_f_dataset",
                "table":"phase_f_table",
                "project_id":"phase-f-project",
                "access_token":"phase-f-token",
                "api_base_url": format!("{}/bigquery/v2", server.base_url())
            }
        }),
        Duration::from_secs(1),
    );

    assert!(
        read_all.get("error").is_none(),
        "unexpected read_rows error: {read_all}"
    );
    assert_eq!(read_all["result"]["operation"], "read_rows");
    assert_eq!(
        read_all["result"]["rows"],
        json!([
            {"session_id":"s1","updated_at_ms":1000,"deleted":false,"payload":{"step":"create"},"labels":{}},
            {"session_id":"s1","updated_at_ms":2000,"deleted":true,"payload":{},"labels":{}},
            {"session_id":"s2","updated_at_ms":1500,"deleted":false,"payload":{"step":"create"},"labels":{}},
            {"session_id":"s2","updated_at_ms":3000,"deleted":false,"payload":{"step":"update","version":2},"labels":{}}
        ])
    );

    assert!(
        read_live.get("error").is_none(),
        "unexpected read_live_rows error: {read_live}"
    );
    assert_eq!(read_live["result"]["operation"], "read_live_rows");
    assert_eq!(
        read_live["result"]["rows"],
        json!([
            {"session_id":"s2","updated_at_ms":3000,"deleted":false,"payload":{"step":"update","version":2},"labels":{}}
        ])
    );

    let requests = server.captured_requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.path == "/bigquery/v2/projects/phase-f-project/queries")
    );
}

#[test]
fn phase_f_rpc_bigquery_timeout_ms_propagates_to_store_http_timeout() {
    let server = MockHttpServer::start(vec![
        MockHttpResponse::json(json!({})).with_delay(Duration::from_millis(200)),
    ]);
    let mut runtime = start_phase_f_rpc_runtime();

    let started = Instant::now();
    let response = call_rpc(
        &mut runtime,
        json!({
            "jsonrpc":"2.0",
            "id":"phase-f-rpc-bq-timeout",
            "method":"meta/session_store/bigquery",
            "params":{
                "operation":"stream_insert_rows",
                "dataset":"phase_f_dataset",
                "table":"phase_f_table",
                "project_id":"phase-f-project",
                "access_token":"phase-f-token",
                "api_base_url": format!("{}/bigquery/v2", server.base_url()),
                "timeout_ms": 25,
                "rows": sample_write_rows()
            }
        }),
        Duration::from_secs(1),
    );
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(180),
        "rpc timeout must be enforced before delayed response (elapsed={elapsed:?})"
    );
    assert_eq!(response["error"]["code"], -32011);
    let message = response["error"]["message"]
        .as_str()
        .expect("error message string");
    assert!(
        message.contains("BigQuery session store request failed"),
        "expected store failure wrapper message, got: {message}"
    );
    let normalized = message.to_ascii_lowercase();
    assert!(
        normalized.contains("timed out")
            || normalized.contains("timedout")
            || normalized.contains("timeout")
            || normalized.contains("deadline has elapsed")
            || normalized.contains("error sending request for url"),
        "expected timeout-shaped RPC store error, got: {message}"
    );
}

#[tokio::test]
async fn phase_f_rpc_bigquery_invalid_params_return_minus_32602() {
    let mut runtime = start_phase_f_rpc_runtime();
    let missing_rows = call_rpc(
        &mut runtime,
        json!({
            "jsonrpc":"2.0",
            "id":"phase-f-rpc-bq-invalid-rows",
            "method":"meta/session_store/bigquery",
            "params":{
                "operation":"stream_insert_rows",
                "dataset":"phase_f_dataset",
                "table":"phase_f_table",
                "project_id":"phase-f-project",
                "access_token":"phase-f-token"
            }
        }),
        Duration::from_secs(1),
    );
    assert_eq!(missing_rows["error"]["code"], -32602);

    let invalid_timeout = call_rpc(
        &mut runtime,
        json!({
            "jsonrpc":"2.0",
            "id":"phase-f-rpc-bq-invalid-timeout",
            "method":"meta/session_store/bigquery",
            "params":{
                "operation":"read_rows",
                "dataset":"phase_f_dataset",
                "table":"phase_f_table",
                "timeout_ms": 0
            }
        }),
        Duration::from_secs(1),
    );
    assert_eq!(invalid_timeout["error"]["code"], -32602);
}

#[tokio::test]
async fn phase_f_rpc_bigquery_store_and_api_failures_return_minus_32011() {
    let mut runtime = start_phase_f_rpc_runtime();
    let missing_project = call_rpc(
        &mut runtime,
        json!({
            "jsonrpc":"2.0",
            "id":"phase-f-rpc-bq-store-config-error",
            "method":"meta/session_store/bigquery",
            "params":{
                "operation":"read_rows",
                "dataset":"phase_f_dataset",
                "table":"phase_f_table",
                "access_token":"phase-f-token"
            }
        }),
        Duration::from_secs(1),
    );
    assert_eq!(missing_project["error"]["code"], -32011);

    let api_server = MockHttpServer::start(vec![MockHttpResponse {
        status_code: 503,
        content_type: "application/json".to_string(),
        body: json!({"error":{"message":"service unavailable"}}).to_string(),
        response_delay: Duration::from_millis(0),
    }]);
    let api_failure = call_rpc(
        &mut runtime,
        json!({
            "jsonrpc":"2.0",
            "id":"phase-f-rpc-bq-store-api-error",
            "method":"meta/session_store/bigquery",
            "params":{
                "operation":"read_rows",
                "dataset":"phase_f_dataset",
                "table":"phase_f_table",
                "project_id":"phase-f-project",
                "access_token":"phase-f-token",
                "api_base_url": format!("{}/bigquery/v2", api_server.base_url())
            }
        }),
        Duration::from_secs(1),
    );
    assert_eq!(api_failure["error"]["code"], -32011);
    let api_message = api_failure["error"]["message"]
        .as_str()
        .expect("api failure message");
    assert!(
        api_message.contains("status 503"),
        "expected surfaced BigQuery API status in RPC error, got: {api_message}"
    );
}

#[tokio::test]
async fn phase_f_bigquery_missing_project_or_token_configuration_returns_configuration_error() {
    let writes = sample_write_rows();

    let missing_project =
        BigQuerySessionStoreAdapter::new_native("phase_f_dataset", "phase_f_table")
            .with_access_token("phase-f-token")
            .stream_insert_rows(&writes)
            .await
            .expect_err("missing project_id should fail");
    assert_configuration_error_contains(missing_project, "missing BigQuery project_id");

    let missing_token = BigQuerySessionStoreAdapter::new_native("phase_f_dataset", "phase_f_table")
        .with_project_id("phase-f-project")
        .stream_insert_rows(&writes)
        .await
        .expect_err("missing access token should fail");
    assert_configuration_error_contains(missing_token, "missing BigQuery access token");
}

#[tokio::test]
async fn phase_f_bigquery_insert_all_row_level_errors_are_not_silently_accepted() {
    let server = MockHttpServer::start(vec![MockHttpResponse::json(json!({
        "insertErrors": [
            {"index":0,"errors":[{"reason":"invalid","message":"row rejected"}]}
        ]
    }))]);
    let store = BigQuerySessionStoreAdapter::new_native("phase_f_dataset", "phase_f_table")
        .with_project_id("phase-f-project")
        .with_access_token("phase-f-token")
        .with_api_base_url(format!("{}/bigquery/v2", server.base_url()));

    let err = store
        .stream_insert_rows(&sample_write_rows())
        .await
        .expect_err("insertErrors should bubble as API failure");
    assert_api_error_contains(err, "insertAll returned row errors");
}

#[tokio::test]
async fn phase_f_bigquery_non_success_http_statuses_surface_api_error() {
    let server = MockHttpServer::start(vec![MockHttpResponse {
        status_code: 503,
        content_type: "application/json".to_string(),
        body: json!({"error":{"message":"service unavailable"}}).to_string(),
        response_delay: Duration::from_millis(0),
    }]);
    let store = BigQuerySessionStoreAdapter::new_native("phase_f_dataset", "phase_f_table")
        .with_project_id("phase-f-project")
        .with_access_token("phase-f-token")
        .with_api_base_url(format!("{}/bigquery/v2", server.base_url()));

    let err = store
        .stream_insert_rows(&sample_write_rows())
        .await
        .expect_err("non-2xx status should fail");
    assert_api_error_contains(err, "status 503");
}

#[tokio::test]
async fn phase_f_bigquery_malformed_query_rows_and_payloads_return_parse_failures() {
    let malformed_rows_server = MockHttpServer::start(vec![MockHttpResponse::json(json!({
        "rows": [
            {"bad":"shape"}
        ]
    }))]);
    let malformed_rows_store =
        BigQuerySessionStoreAdapter::new_native("phase_f_dataset", "phase_f_table")
            .with_project_id("phase-f-project")
            .with_access_token("phase-f-token")
            .with_api_base_url(format!("{}/bigquery/v2", malformed_rows_server.base_url()));
    let malformed_rows_err = malformed_rows_store
        .read_rows()
        .await
        .expect_err("query rows missing row.f should fail parsing");
    assert_invalid_query_error_contains(malformed_rows_err, "missing row.f cell array");

    let malformed_payload_server = MockHttpServer::start(vec![MockHttpResponse::json(json!({
        "rows": [
            {"f":[{"v":"s1"},{"v":"2000"},{"v":"false"},{"v":"{not-json}"}]}
        ]
    }))]);
    let malformed_payload_store =
        BigQuerySessionStoreAdapter::new_native("phase_f_dataset", "phase_f_table")
            .with_project_id("phase-f-project")
            .with_access_token("phase-f-token")
            .with_api_base_url(format!(
                "{}/bigquery/v2",
                malformed_payload_server.base_url()
            ));
    let malformed_payload_err = malformed_payload_store
        .read_rows()
        .await
        .expect_err("invalid payload JSON should fail parsing");
    assert_invalid_query_error_contains(malformed_payload_err, "payload JSON parse failed");
}

#[tokio::test]
async fn phase_f_bigquery_timeout_and_connection_failures_surface_http_errors() {
    let timeout_server = MockHttpServer::start(vec![
        MockHttpResponse::json(json!({})).with_delay(Duration::from_millis(200)),
    ]);
    let timeout_store = BigQuerySessionStoreAdapter::new_native("phase_f_dataset", "phase_f_table")
        .with_project_id("phase-f-project")
        .with_access_token("phase-f-token")
        .with_api_base_url(format!("{}/bigquery/v2", timeout_server.base_url()))
        .with_http_timeout(Duration::from_millis(25));
    let timeout_started = Instant::now();
    let timeout_err = timeout_store
        .stream_insert_rows(&sample_write_rows())
        .await
        .expect_err("delayed response should trigger HTTP timeout");
    let timeout_elapsed = timeout_started.elapsed();
    assert!(
        timeout_elapsed < Duration::from_millis(180),
        "timeout path should fail before delayed response writes back (elapsed={timeout_elapsed:?})"
    );
    assert_http_error_contains_any(
        timeout_err,
        &[
            "timed out",
            "timedout",
            "timeout",
            "deadline has elapsed",
            "error sending request for url",
        ],
    );

    let base_url = unused_bigquery_api_base_url();
    let connection_store =
        BigQuerySessionStoreAdapter::new_native("phase_f_dataset", "phase_f_table")
            .with_project_id("phase-f-project")
            .with_access_token("phase-f-token")
            .with_api_base_url(base_url)
            .with_http_timeout(Duration::from_millis(250));
    let connection_err = connection_store
        .stream_insert_rows(&sample_write_rows())
        .await
        .expect_err("connection failure should surface as HTTP error");
    assert_http_error_contains_any(
        connection_err,
        &[
            "connection refused",
            "failed to connect",
            "error trying to connect",
            "tcp connect error",
        ],
    );
}

#[test]
#[ignore] // requires real BigQuery credentials and network
fn phase_f_real_bigquery_integration_streaming_dedup_tombstone_semantics() {
    // Self-skip without credentials (same idiom as the env-gated tests in
    // gateway_external.rs): a contributor without GCP access sees SKIP, not
    // FAIL, when running the ignored lane.
    let Some(access_token) = bigquery_access_token() else {
        eprintln!(
            "skipping phase_f_real_bigquery_integration_streaming_dedup_tombstone_semantics \
             because no BigQuery credentials are available: set BIGQUERY_ACCESS_TOKEN (or \
             GOOGLE_OAUTH_ACCESS_TOKEN), or run `gcloud auth application-default login` for \
             project `{REAL_BQ_PROJECT}`"
        );
        return;
    };
    let api_base_url = std::env::var("BIGQUERY_API_BASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_BQ_API_BASE_URL.to_string());

    let dataset_id = format!("phase_f_{}_{}", std::process::id(), unix_ms());
    let table_id = "sessions_phase_f";
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build reqwest client");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime for BigQuery integration test");

    create_dataset(
        &client,
        &api_base_url,
        REAL_BQ_PROJECT,
        &dataset_id,
        &access_token,
    )
    .unwrap_or_else(|err| {
        panic!(
            "failed to create dataset `{}` in project `{}` via BigQuery API: {}",
            dataset_id, REAL_BQ_PROJECT, err
        )
    });
    let _cleanup = DatasetCleanupGuard::new(
        client.clone(),
        api_base_url.clone(),
        REAL_BQ_PROJECT.to_string(),
        dataset_id.clone(),
        access_token.clone(),
    );

    create_table(
        &client,
        &api_base_url,
        REAL_BQ_PROJECT,
        &dataset_id,
        table_id,
        &access_token,
    )
    .unwrap_or_else(|err| {
        panic!(
            "failed to create table `{}.{}` in project `{}` via BigQuery API: {}",
            dataset_id, table_id, REAL_BQ_PROJECT, err
        )
    });

    let store = BigQuerySessionStoreAdapter::new_native(dataset_id.clone(), table_id.to_string())
        .with_project_id(REAL_BQ_PROJECT)
        .with_access_token(access_token)
        .with_api_base_url(api_base_url);

    let writes = vec![
        SessionPersistenceRow {
            session_id: "s1".to_string(),
            updated_at_ms: 1_000,
            deleted: false,
            payload: json!({"step":"create"}),
            ..Default::default()
        },
        SessionPersistenceRow {
            session_id: "s1".to_string(),
            updated_at_ms: 2_000,
            deleted: true,
            payload: json!({}),
            ..Default::default()
        },
        SessionPersistenceRow {
            session_id: "s2".to_string(),
            updated_at_ms: 1_500,
            deleted: false,
            payload: json!({"step":"create"}),
            ..Default::default()
        },
        SessionPersistenceRow {
            session_id: "s2".to_string(),
            updated_at_ms: 3_000,
            deleted: false,
            payload: json!({"step":"update","version":2}),
            ..Default::default()
        },
    ];

    runtime
        .block_on(store.stream_insert_rows(&writes))
        .expect("stream insert rows into real BigQuery table");

    let expected_latest = vec![
        SessionPersistenceRow {
            session_id: "s1".to_string(),
            updated_at_ms: 2_000,
            deleted: true,
            payload: json!({}),
            ..Default::default()
        },
        SessionPersistenceRow {
            session_id: "s2".to_string(),
            updated_at_ms: 3_000,
            deleted: false,
            payload: json!({"step":"update","version":2}),
            ..Default::default()
        },
    ];
    let expected_live = vec![SessionPersistenceRow {
        session_id: "s2".to_string(),
        updated_at_ms: 3_000,
        deleted: false,
        payload: json!({"step":"update","version":2}),
        ..Default::default()
    }];

    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let latest = runtime
            .block_on(store.read_latest_rows())
            .expect("query latest rows from real BigQuery");
        let live = runtime
            .block_on(store.read_live_rows())
            .expect("query live rows from real BigQuery");
        if latest == expected_latest && live == expected_live {
            break;
        }
        if Instant::now() >= deadline {
            panic!(
                "BigQuery streaming rows did not converge to expected dedup+tombstone semantics within 60s.\nlatest={:?}\nlive={:?}",
                latest, live
            );
        }
        thread::sleep(Duration::from_secs(2));
    }
}

#[derive(Debug)]
struct DatasetCleanupGuard {
    client: reqwest::blocking::Client,
    api_base_url: String,
    project_id: String,
    dataset_id: String,
    access_token: String,
}

impl DatasetCleanupGuard {
    fn new(
        client: reqwest::blocking::Client,
        api_base_url: String,
        project_id: String,
        dataset_id: String,
        access_token: String,
    ) -> Self {
        Self {
            client,
            api_base_url,
            project_id,
            dataset_id,
            access_token,
        }
    }
}

impl Drop for DatasetCleanupGuard {
    fn drop(&mut self) {
        let endpoint = format!(
            "{}/projects/{}/datasets/{}?deleteContents=true",
            self.api_base_url.trim_end_matches('/'),
            self.project_id,
            self.dataset_id
        );
        let request = self
            .client
            .request(reqwest::Method::DELETE, &endpoint)
            .bearer_auth(&self.access_token);
        match request.send() {
            Ok(response) if response.status().is_success() => {}
            Ok(response) => {
                let status = response.status();
                let body = response
                    .text()
                    .unwrap_or_else(|_| "<unable to read response body>".to_string());
                eprintln!(
                    "phase_f cleanup warning: failed to delete dataset `{}` (status {}): {}",
                    self.dataset_id,
                    status.as_u16(),
                    body
                );
            }
            Err(err) => {
                eprintln!(
                    "phase_f cleanup warning: failed to delete dataset `{}`: {}",
                    self.dataset_id, err
                );
            }
        }
    }
}

fn create_dataset(
    client: &reqwest::blocking::Client,
    api_base_url: &str,
    project_id: &str,
    dataset_id: &str,
    access_token: &str,
) -> Result<(), String> {
    let endpoint = format!(
        "{}/projects/{}/datasets",
        api_base_url.trim_end_matches('/'),
        project_id
    );
    let body = json!({
        "datasetReference": {
            "projectId": project_id,
            "datasetId": dataset_id,
        },
        "location": "US",
        "description": "meerkat-meta phase_f integration test dataset",
    });
    send_bigquery_json_request(
        client,
        reqwest::Method::POST,
        &endpoint,
        access_token,
        Some(body),
    )
    .map(|_| ())
}

fn create_table(
    client: &reqwest::blocking::Client,
    api_base_url: &str,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
    access_token: &str,
) -> Result<(), String> {
    let endpoint = format!(
        "{}/projects/{}/datasets/{}/tables",
        api_base_url.trim_end_matches('/'),
        project_id,
        dataset_id
    );
    let body = json!({
        "tableReference": {
            "projectId": project_id,
            "datasetId": dataset_id,
            "tableId": table_id,
        },
        "schema": {
            "fields": [
                {"name":"session_id","type":"STRING","mode":"REQUIRED"},
                {"name":"updated_at_ms","type":"INT64","mode":"REQUIRED"},
                {"name":"deleted","type":"BOOL","mode":"REQUIRED"},
                {"name":"payload","type":"STRING","mode":"NULLABLE"},
                {"name":"labels_json","type":"STRING","mode":"NULLABLE"}
            ]
        }
    });
    send_bigquery_json_request(
        client,
        reqwest::Method::POST,
        &endpoint,
        access_token,
        Some(body),
    )
    .map(|_| ())
}

fn send_bigquery_json_request(
    client: &reqwest::blocking::Client,
    method: reqwest::Method,
    endpoint: &str,
    access_token: &str,
    body: Option<Value>,
) -> Result<Value, String> {
    let mut request = client
        .request(method.clone(), endpoint)
        .bearer_auth(access_token)
        .header("accept", "application/json");
    if let Some(body) = body {
        request = request
            .header("content-type", "application/json")
            .json(&body);
    }

    let response = request
        .send()
        .map_err(|err| format!("request {} {} failed: {}", method.as_str(), endpoint, err))?;
    let status = response.status();
    let text = response.text().map_err(|err| {
        format!(
            "read response {} {} failed: {}",
            method.as_str(),
            endpoint,
            err
        )
    })?;
    if !status.is_success() {
        return Err(format!(
            "{} {} failed with status {}: {}",
            method.as_str(),
            endpoint,
            status.as_u16(),
            text
        ));
    }

    if text.trim().is_empty() {
        Ok(json!({}))
    } else {
        serde_json::from_str::<Value>(&text).map_err(|err| {
            format!(
                "invalid JSON response for {} {}: {}",
                method.as_str(),
                endpoint,
                err
            )
        })
    }
}

fn assert_configuration_error_contains(error: BigQuerySessionStoreError, expected: &str) {
    match error {
        BigQuerySessionStoreError::Configuration(message) => {
            assert!(
                message.contains(expected),
                "expected configuration error to contain `{expected}`, got `{message}`"
            );
        }
        other => panic!("expected configuration error, got {other:?}"),
    }
}

fn assert_api_error_contains(error: BigQuerySessionStoreError, expected: &str) {
    match error {
        BigQuerySessionStoreError::Api(message) => {
            assert!(
                message.contains(expected),
                "expected API error to contain `{expected}`, got `{message}`"
            );
        }
        other => panic!("expected API error, got {other:?}"),
    }
}

fn assert_invalid_query_error_contains(error: BigQuerySessionStoreError, expected: &str) {
    match error {
        BigQuerySessionStoreError::InvalidQueryResponse(message) => {
            assert!(
                message.contains(expected),
                "expected invalid query error to contain `{expected}`, got `{message}`"
            );
        }
        other => panic!("expected invalid query response error, got {other:?}"),
    }
}

fn assert_http_error_contains_any(error: BigQuerySessionStoreError, expected_any: &[&str]) {
    match error {
        BigQuerySessionStoreError::Http(message) => {
            let normalized = message.to_ascii_lowercase();
            assert!(
                expected_any
                    .iter()
                    .any(|expected| normalized.contains(&expected.to_ascii_lowercase())),
                "expected HTTP error to contain one of {:?}, got `{}`",
                expected_any,
                message
            );
        }
        other => panic!("expected HTTP error, got {other:?}"),
    }
}

fn unused_bigquery_api_base_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral listener");
    let address = listener
        .local_addr()
        .expect("resolve ephemeral listener address");
    drop(listener);
    format!("http://{address}/bigquery/v2")
}

/// Resolve a BigQuery bearer token from the environment or gcloud ADC.
///
/// `None` means "no GCP credentials here" and the caller must SKIP, not
/// FAIL: contributors without access to project `king-dnn-training-dev`
/// (and CI runners) still run the ignored lane via `--ignored`, and a
/// missing-prerequisite panic reads as a red test rather than an
/// environment gap.
fn bigquery_access_token() -> Option<String> {
    for key in [
        "BIGQUERY_ACCESS_TOKEN",
        "GOOGLE_OAUTH_ACCESS_TOKEN",
        "GOOGLE_ACCESS_TOKEN",
    ] {
        if let Ok(token) = std::env::var(key) {
            let token = token.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    }

    if let Ok(output) = Command::new("gcloud")
        .args(["auth", "application-default", "print-access-token"])
        .output()
    {
        if output.status.success() {
            let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !token.is_empty() {
                return Some(token);
            }
        }
    }

    None
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn mirror_snapshot() -> meerkat_meta::unified_runtime::continuity_mirror::ContinuityMirrorSnapshot {
    use meerkat_meta::unified_runtime::continuity_mirror::{
        ContinuityMirrorRecord, ContinuityMirrorSession, ContinuityMirrorSnapshot,
    };
    ContinuityMirrorSnapshot {
        records: vec![
            ContinuityMirrorRecord {
                identity: "agent:alice".to_string(),
                agent_runtime_id: "rt:agent:alice:0".to_string(),
                session_id: "s-alice".to_string(),
                generation: 0,
                checkpoint_version: 0,
            },
            ContinuityMirrorRecord {
                identity: "agent:bob".to_string(),
                agent_runtime_id: "rt:agent:bob:2".to_string(),
                session_id: "s-bob".to_string(),
                generation: 2,
                checkpoint_version: 0,
            },
        ],
        sessions: vec![ContinuityMirrorSession {
            session_id: "s-alice".to_string(),
            data: "eyJ2IjoxfQ==".to_string(),
        }],
    }
}

fn mirror_sink(
    server: &MockHttpServer,
) -> meerkat_meta::unified_runtime::continuity_mirror_bigquery::BigQueryMirrorSink {
    meerkat_meta::unified_runtime::continuity_mirror_bigquery::BigQueryMirrorSink::new(
        BigQuerySessionStoreAdapter::new_native("mirror_dataset", "unused_table")
            .with_project_id("mirror-project")
            .with_access_token("mirror-token")
            .with_api_base_url(format!("{}/bigquery/v2", server.base_url())),
        "continuity",
    )
}

fn mirror_table_path(name: &str) -> String {
    format!("/bigquery/v2/projects/mirror-project/datasets/mirror_dataset/tables/{name}/insertAll")
}

fn mirror_request_paths(requests: &[bigquery_http_mock::CapturedHttpRequest]) -> Vec<String> {
    requests
        .iter()
        .map(|request| request.path.clone())
        .collect()
}

fn mirror_request_rows(request: &bigquery_http_mock::CapturedHttpRequest) -> Vec<Value> {
    let body: Value = serde_json::from_str(&request.body).expect("insert body");
    body["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row["json"].clone())
        .collect()
}

/// One `put` streams the sessions, then the records, then one snapshots row
/// naming the shared `snapshot_ms` and the row counts, each into its own
/// table of the dataset; a session row carries its data as part 0 of 1.
#[tokio::test]
async fn continuity_mirror_bigquery_sink_writes_sessions_records_then_the_snapshot_row() {
    use meerkat_meta::unified_runtime::continuity_mirror::ContinuityMirrorSink as _;
    let server = MockHttpServer::start(vec![
        MockHttpResponse::json(json!({})),
        MockHttpResponse::json(json!({})),
        MockHttpResponse::json(json!({})),
    ]);
    mirror_sink(&server)
        .put(&mirror_snapshot())
        .await
        .expect("the snapshot streams into BigQuery");

    let requests = server.captured_requests();
    assert_eq!(
        mirror_request_paths(&requests),
        vec![
            mirror_table_path("continuity_sessions"),
            mirror_table_path("continuity_records"),
            mirror_table_path("continuity_snapshots"),
        ]
    );
    let sessions = mirror_request_rows(&requests[0]);
    let records = mirror_request_rows(&requests[1]);
    let snapshot = mirror_request_rows(&requests[2]);
    let snapshot_ms = snapshot[0]["snapshot_ms"].clone();
    assert!(
        snapshot_ms
            .as_str()
            .is_some_and(|ms| ms.parse::<u64>().is_ok())
    );
    assert_eq!(
        snapshot,
        vec![json!({
            "snapshot_ms": snapshot_ms.clone(),
            "records": 2,
            "sessions": 1,
        })]
    );
    assert_eq!(
        sessions,
        vec![json!({
            "session_id": "s-alice",
            "data": "eyJ2IjoxfQ==",
            "part": 0,
            "parts": 1,
            "snapshot_ms": snapshot_ms.clone(),
        })]
    );
    assert_eq!(
        records,
        vec![
            json!({
                "identity": "agent:alice",
                "agent_runtime_id": "rt:agent:alice:0",
                "session_id": "s-alice",
                "generation": 0,
                "checkpoint_version": 0,
                "snapshot_ms": snapshot_ms.clone(),
            }),
            json!({
                "identity": "agent:bob",
                "agent_runtime_id": "rt:agent:bob:2",
                "session_id": "s-bob",
                "generation": 2,
                "checkpoint_version": 0,
                "snapshot_ms": snapshot_ms,
            }),
        ]
    );
    assert!(requests.iter().all(|request| {
        request.headers.get("authorization").map(String::as_str) == Some("Bearer mirror-token")
    }));
}

/// A table insert that BigQuery answers with row errors fails the `put`, and
/// no snapshots row is written, so a reader never takes the partial rows.
/// The next `put` writes everything again under a newer `snapshot_ms`.
#[tokio::test]
async fn continuity_mirror_bigquery_sink_writes_no_snapshot_row_when_an_insert_fails() {
    use meerkat_meta::unified_runtime::continuity_mirror::ContinuityMirrorSink as _;
    let mut responses = vec![
        MockHttpResponse::json(json!({})),
        MockHttpResponse::json(json!({
            "insertErrors": [{"index": 0, "errors": [{"reason": "invalid"}]}]
        })),
    ];
    responses.extend((0..3).map(|_| MockHttpResponse::json(json!({}))));
    let server = MockHttpServer::start(responses);
    let sink = mirror_sink(&server);
    let error = sink
        .put(&mirror_snapshot())
        .await
        .expect_err("row errors fail the put");
    assert!(error.contains("continuity_records"), "{error}");
    sink.put(&mirror_snapshot())
        .await
        .expect("the next put streams the snapshot again");

    let requests = server.captured_requests();
    assert_eq!(
        mirror_request_paths(&requests),
        vec![
            mirror_table_path("continuity_sessions"),
            mirror_table_path("continuity_records"),
            mirror_table_path("continuity_sessions"),
            mirror_table_path("continuity_records"),
            mirror_table_path("continuity_snapshots"),
        ]
    );
    let snapshot_ms = |index: usize| {
        mirror_request_rows(&requests[index])[0]["snapshot_ms"]
            .as_str()
            .expect("snapshot_ms")
            .parse::<u64>()
            .expect("decimal snapshot_ms")
    };
    assert!(snapshot_ms(0) < snapshot_ms(2));
    assert_eq!(snapshot_ms(2), snapshot_ms(4));
}

/// Session data longer than 4 MiB goes out as rows of at most 4 MiB, numbered
/// by `part` with the count in `parts`, that join back into the data.
#[tokio::test]
async fn continuity_mirror_bigquery_sink_splits_long_session_data_into_parts_that_rejoin() {
    use meerkat_meta::unified_runtime::continuity_mirror::ContinuityMirrorSink as _;
    const PART_BYTES: usize = 4 * 1024 * 1024;
    let server = MockHttpServer::start(vec![
        MockHttpResponse::json(json!({})),
        MockHttpResponse::json(json!({})),
        MockHttpResponse::json(json!({})),
    ]);
    let mut snapshot = mirror_snapshot();
    let data = format!("{}QUJD", "A".repeat(PART_BYTES));
    snapshot.sessions[0].data = data.clone();
    mirror_sink(&server)
        .put(&snapshot)
        .await
        .expect("the long session streams into BigQuery");

    let requests = server.captured_requests();
    assert_eq!(
        mirror_request_paths(&requests),
        vec![
            mirror_table_path("continuity_sessions"),
            mirror_table_path("continuity_records"),
            mirror_table_path("continuity_snapshots"),
        ]
    );
    let sessions = mirror_request_rows(&requests[0]);
    assert_eq!(sessions.len(), 2);
    for (index, row) in sessions.iter().enumerate() {
        assert_eq!(row["session_id"], json!("s-alice"));
        assert_eq!(row["part"], json!(index));
        assert_eq!(row["parts"], json!(2));
    }
    let pieces: Vec<&str> = sessions
        .iter()
        .map(|row| row["data"].as_str().expect("data"))
        .collect();
    assert_eq!(pieces[0].len(), PART_BYTES);
    assert_eq!(pieces.concat(), data);
}

/// A `put` that has not completed within its deadline fails, and writes no
/// snapshots row.
#[tokio::test]
async fn continuity_mirror_bigquery_sink_fails_a_put_past_its_deadline() {
    use meerkat_meta::unified_runtime::continuity_mirror::ContinuityMirrorSink as _;
    let server = MockHttpServer::start(vec![
        MockHttpResponse::json(json!({})).with_delay(Duration::from_secs(2)),
    ]);
    let started = Instant::now();
    let error = mirror_sink(&server)
        .with_put_deadline(Duration::from_millis(200))
        .put(&mirror_snapshot())
        .await
        .expect_err("the put misses its deadline");
    assert!(error.contains("did not complete within"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(
        mirror_request_paths(&server.captured_requests()),
        vec![mirror_table_path("continuity_sessions")]
    );
}
