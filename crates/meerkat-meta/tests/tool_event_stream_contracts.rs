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
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::StreamExt;
use meerkat_core::AgentEvent;
use meerkat_core::EventEnvelope;
use meerkat_core::comms::EventStream;
use meerkat_meta::{MobRuntimeError, agent_events_sse_router};
use serde_json::{Value, json};
use tower::ServiceExt;

#[tokio::test]
async fn phase0_contract_007_tool_events_stream_with_tool_call_id() {
    // meerkat 0.7 removed the flat `result` text from
    // `AgentEvent::ToolExecutionCompleted`; typed `content` blocks are the
    // sole owner. Meta's SSE edge derives the wire `result` field from the
    // text blocks (see console_agent_event_payload), keeping the SDK contract.
    let agent_event = AgentEvent::ToolExecutionCompleted {
        id: "tool-1".to_string(),
        name: "search".to_string(),
        content: vec![meerkat_core::types::ContentBlock::Text {
            text: "done".to_string(),
        }],
        is_error: false,
        duration_ms: 12,
    };
    let app = agent_events_sse_router(
        Arc::new(move |_agent_id| {
            let event = agent_event.clone();
            Box::pin(async move {
                Ok::<EventStream, MobRuntimeError>(Box::pin(futures::stream::iter(vec![
                    EventEnvelope::new("worker-1", 0, None, event),
                ])) as EventStream)
            })
        }),
        // No decisions: this test exercises the streaming shape, not
        // auth gating. Auth coverage lives in `sse_auth_gate.rs`.
        None,
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/agents/worker-1/events")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let mut stream = response.into_body().into_data_stream();
    let chunk = stream
        .next()
        .await
        .expect("initial sse frame")
        .expect("sse bytes");
    let text = String::from_utf8(chunk.to_vec()).expect("utf8 sse chunk");
    let data_line = text
        .lines()
        .find(|line| line.starts_with("data:"))
        .expect("sse data line");
    let payload: Value =
        serde_json::from_str(data_line.trim_start_matches("data:").trim()).expect("json payload");

    assert_eq!(payload["type"], json!("tool_execution_completed"));
    assert_eq!(payload["id"], json!("tool-1"));
    assert_eq!(payload["tool_call_id"], json!("tool-1"));
    assert_eq!(payload["name"], json!("search"));
    assert_eq!(payload["result"], json!("done"));
}
