#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use meerkat_core::{
    AgentError, AgentLlmClient, AgentLlmRequestAttempt, AssistantBlock, AssistantMessageId,
    BlockAssistantMessage, LlmStreamResult, Message, Provider, ProviderParamsOverride,
    ProviderRequestPressure, RequestAttemptAuthority, ServerToolKind, StopReason, ToolDef,
    ToolProvenance, ToolResult, ToolSourceKind, Usage,
};

use super::ReplaySanitizingAgentLlmClient;
use crate::memory::dispatch_taint::{DispatchTaintSlot, TaintObservingLlmClient};
use crate::memory::taint::{ContentTrustConfig, SessionTaintTracker};

const IDENTITY: &str = "prepared-attempt-agent";

struct PreparedArgs {
    messages: Arc<Vec<Message>>,
    tools: Arc<[Arc<ToolDef>]>,
    max_tokens: u32,
    temperature: Option<f32>,
    provider_params: Option<ProviderParamsOverride>,
}

struct ProbeAttempt {
    tracker: SessionTaintTracker,
    dispatches: Mutex<Vec<(AssistantMessageId, bool)>>,
    pressure: Mutex<VecDeque<Result<Option<ProviderRequestPressure>, &'static str>>>,
    fail_dispatch: AtomicBool,
    blocks: Vec<AssistantBlock>,
}

#[async_trait::async_trait]
impl AgentLlmRequestAttempt for ProbeAttempt {
    fn request_pressure(&self) -> Result<Option<ProviderRequestPressure>, AgentError> {
        self.pressure
            .lock()
            .unwrap()
            .pop_front()
            .expect("each pressure observation must reach the prepared attempt")
            .map_err(|message| AgentError::InternalError(message.to_string()))
    }

    async fn stream_response(
        &self,
        assistant_message_id: AssistantMessageId,
    ) -> Result<LlmStreamResult, AgentError> {
        self.dispatches.lock().unwrap().push((
            assistant_message_id,
            self.tracker.identity_taint(IDENTITY).is_some(),
        ));
        if self.fail_dispatch.load(Ordering::SeqCst) {
            return Err(AgentError::InternalError("prepared dispatch failed".into()));
        }
        Ok(LlmStreamResult::new(
            self.blocks.clone(),
            StopReason::EndTurn,
            Usage::default(),
        ))
    }
}

struct ProbeClient {
    prepared: Mutex<Vec<PreparedArgs>>,
    fail_prepare: AtomicBool,
    attempt: Arc<ProbeAttempt>,
}

#[async_trait::async_trait]
impl AgentLlmClient for ProbeClient {
    fn prepare_request_attempt(
        self: Arc<Self>,
        messages: Arc<Vec<Message>>,
        tools: Arc<[Arc<ToolDef>]>,
        max_tokens: u32,
        temperature: Option<f32>,
        provider_params: Option<ProviderParamsOverride>,
    ) -> Result<Arc<dyn AgentLlmRequestAttempt>, AgentError> {
        self.prepared.lock().unwrap().push(PreparedArgs {
            messages,
            tools,
            max_tokens,
            temperature,
            provider_params,
        });
        if self.fail_prepare.load(Ordering::SeqCst) {
            return Err(AgentError::InternalError("prepared route refused".into()));
        }
        Ok(self.attempt.clone())
    }

    fn request_attempt_authority(&self) -> RequestAttemptAuthority {
        RequestAttemptAuthority::Unified
    }

    fn provider(&self) -> Provider {
        Provider::OpenAI
    }

    fn model(&self) -> &'static str {
        "prepared-probe"
    }

    async fn stream_response(
        &self,
        _messages: &[Message],
        _tools: &[Arc<ToolDef>],
        _max_tokens: u32,
        _temperature: Option<f32>,
        _provider_params: Option<&ProviderParamsOverride>,
    ) -> Result<LlmStreamResult, AgentError> {
        panic!("a prepared request must not fall back to the identity-free client stream")
    }

    fn request_pressure(
        &self,
        _messages: &[Message],
        _tools: &[Arc<ToolDef>],
        _max_tokens: u32,
        _temperature: Option<f32>,
        _provider_params: Option<&ProviderParamsOverride>,
    ) -> Result<Option<ProviderRequestPressure>, AgentError> {
        panic!("pressure must use the bound prepared attempt, not another client route")
    }
}

fn probe(blocks: Vec<AssistantBlock>) -> Arc<ProbeClient> {
    Arc::new(ProbeClient {
        prepared: Mutex::new(Vec::new()),
        fail_prepare: AtomicBool::new(false),
        attempt: Arc::new(ProbeAttempt {
            tracker: SessionTaintTracker::new(ContentTrustConfig::default()),
            dispatches: Mutex::new(Vec::new()),
            pressure: Mutex::new(VecDeque::from([
                Ok(Some(ProviderRequestPressure::new(111, Some(4096)))),
                Ok(Some(ProviderRequestPressure::new(222, Some(8192)))),
                Ok(None),
                Err("prepared pressure failed"),
            ])),
            fail_dispatch: AtomicBool::new(false),
            blocks,
        }),
    })
}

fn sentinel() -> AssistantMessageId {
    serde_json::from_value(serde_json::json!("a02b58e1-bc62-4bea-84d1-356fca9f7d02"))
        .expect("serialized core-owned identity fixture")
}

fn unsafe_server_block() -> AssistantBlock {
    AssistantBlock::ServerToolContent {
        id: Some("provider-progress".into()),
        kind: ServerToolKind::WebSearch,
        content: serde_json::json!({"type": "response.web_search_call.searching"}),
        meta: None,
    }
}

fn untrusted_request() -> (Arc<Vec<Message>>, Arc<[Arc<ToolDef>]>) {
    let messages = vec![
        Message::BlockAssistant(BlockAssistantMessage::new(
            vec![AssistantBlock::ToolUse {
                id: "scrape-call".into(),
                name: "scrape_page".into(),
                args: serde_json::value::RawValue::from_string("{}".into()).unwrap(),
                meta: None,
            }],
            StopReason::ToolUse,
        )),
        Message::tool_results(vec![ToolResult::new(
            "scrape-call".into(),
            "untrusted result".into(),
            false,
        )]),
    ];
    let tools = vec![Arc::new(ToolDef {
        audience: Default::default(),
        name: "scrape_page".into(),
        description: "Fetch page".into(),
        input_schema: serde_json::json!({"type": "object"}),
        provenance: Some(ToolProvenance {
            kind: ToolSourceKind::Mcp,
            source_id: "scraper".into(),
        }),
    })];
    (Arc::new(messages), tools.into())
}

fn wrapped(kind: &str, inner: Arc<ProbeClient>) -> Arc<dyn AgentLlmClient> {
    match kind {
        "replay" => ReplaySanitizingAgentLlmClient::wrap(inner),
        "taint" => Arc::new(TaintObservingLlmClient::new(
            inner,
            IDENTITY.into(),
            DispatchTaintSlot::default(),
        )),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn prepared_attempt_replay_preserves_inner_attempt_identity_and_sanitized_owned_request() {
    let inner = probe(vec![]);
    let mut history = BlockAssistantMessage::new(
        vec![
            AssistantBlock::Text {
                text: "visible".into(),
                meta: None,
            },
            unsafe_server_block(),
            AssistantBlock::ServerToolContent {
                id: Some("provider-completed".into()),
                kind: ServerToolKind::ProviderNative {
                    name: "web_search_call".into(),
                },
                content: serde_json::json!({"type": "web_search_call", "status": "completed"}),
                meta: None,
            },
        ],
        StopReason::EndTurn,
    );
    history.assistant_message_id = Some(sentinel());
    let original = Arc::new(vec![Message::BlockAssistant(history)]);
    let original_bytes = serde_json::to_value(&original).unwrap();
    let (_, tools) = untrusted_request();
    let params = ProviderParamsOverride {
        top_p: Some(0.75),
        ..Default::default()
    };
    let attempt = wrapped("replay", inner.clone())
        .prepare_request_attempt(
            original.clone(),
            tools.clone(),
            731,
            Some(0.25),
            Some(params.clone()),
        )
        .unwrap();
    let expected_attempt: Arc<dyn AgentLlmRequestAttempt> = inner.attempt.clone();
    assert!(
        Arc::ptr_eq(&attempt, &expected_attempt),
        "replay returns the exact inner route witness"
    );
    {
        let prepared = inner.prepared.lock().unwrap();
        assert_eq!(prepared.len(), 1);
        let seen = &prepared[0];
        assert!(!Arc::ptr_eq(&seen.messages, &original));
        assert!(Arc::ptr_eq(&seen.tools, &tools));
        assert_eq!((seen.max_tokens, seen.temperature), (731, Some(0.25)));
        assert_eq!(seen.provider_params, Some(params));
        let Message::BlockAssistant(assistant) = &seen.messages[0] else {
            panic!("assistant row");
        };
        assert_eq!(assistant.assistant_message_id, Some(sentinel()));
        assert_eq!(assistant.blocks.len(), 2);
        assert!(
            matches!(&assistant.blocks[0], AssistantBlock::Text { text, .. } if text == "visible")
        );
        assert!(
            matches!(&assistant.blocks[1], AssistantBlock::ServerToolContent { id, .. } if id.as_deref() == Some("provider-completed"))
        );
    }
    assert_eq!(
        serde_json::to_value(&original).unwrap(),
        original_bytes,
        "canonical input is not mutated"
    );
    attempt.stream_response(sentinel()).await.unwrap();
    assert_eq!(
        *inner.attempt.dispatches.lock().unwrap(),
        [(sentinel(), false)]
    );
}

#[tokio::test]
async fn prepared_attempt_taint_marks_at_dispatch_with_late_slot_and_preserves_request() {
    let inner = probe(vec![]);
    let slot = DispatchTaintSlot::default();
    let client = Arc::new(TaintObservingLlmClient::new(
        inner.clone(),
        IDENTITY.into(),
        slot.clone(),
    ));
    let (messages, tools) = untrusted_request();
    let params = ProviderParamsOverride {
        max_output_tokens: Some(512),
        ..Default::default()
    };
    let attempt = client
        .prepare_request_attempt(
            messages.clone(),
            tools.clone(),
            512,
            Some(0.5),
            Some(params.clone()),
        )
        .unwrap();
    slot.fill(inner.attempt.tracker.clone());
    assert!(
        inner.attempt.tracker.identity_taint(IDENTITY).is_none(),
        "preparation does not ingest"
    );
    assert_eq!(
        attempt.request_pressure().unwrap(),
        Some(ProviderRequestPressure::new(111, Some(4096)))
    );
    assert!(
        inner.attempt.tracker.identity_taint(IDENTITY).is_none(),
        "pressure does not ingest"
    );
    {
        let prepared = inner.prepared.lock().unwrap();
        assert_eq!(prepared.len(), 1);
        assert!(Arc::ptr_eq(&prepared[0].messages, &messages));
        assert!(Arc::ptr_eq(&prepared[0].tools, &tools));
        assert_eq!(
            (prepared[0].max_tokens, prepared[0].temperature),
            (512, Some(0.5))
        );
        assert_eq!(prepared[0].provider_params, Some(params));
    }
    attempt.stream_response(sentinel()).await.unwrap();
    assert_eq!(
        *inner.attempt.dispatches.lock().unwrap(),
        [(sentinel(), true)],
        "mark precedes actual inner dispatch"
    );
    assert!(
        inner
            .attempt
            .tracker
            .identity_taint(IDENTITY)
            .unwrap()
            .source
            .contains("scraper")
    );
}

#[test]
fn prepared_attempt_wrappers_forward_dynamic_pressure_and_errors_without_repreparing() {
    for kind in ["replay", "taint"] {
        let inner = probe(vec![]);
        let attempt = wrapped(kind, inner.clone())
            .prepare_request_attempt(Arc::new(vec![]), Arc::new([]), 32, None, None)
            .unwrap();
        assert_eq!(
            attempt.request_pressure().unwrap(),
            Some(ProviderRequestPressure::new(111, Some(4096)))
        );
        assert_eq!(
            attempt.request_pressure().unwrap(),
            Some(ProviderRequestPressure::new(222, Some(8192)))
        );
        assert_eq!(attempt.request_pressure().unwrap(), None);
        assert!(
            matches!(attempt.request_pressure(), Err(AgentError::InternalError(message)) if message == "prepared pressure failed")
        );
        assert_eq!(inner.prepared.lock().unwrap().len(), 1);
        assert!(inner.attempt.dispatches.lock().unwrap().is_empty());
    }
}

#[test]
fn prepared_attempt_wrappers_propagate_preparation_failure_without_taint_or_dispatch() {
    for kind in ["replay", "taint"] {
        let inner = probe(vec![]);
        inner.fail_prepare.store(true, Ordering::SeqCst);
        let (messages, tools) = untrusted_request();
        let client: Arc<dyn AgentLlmClient> = if kind == "taint" {
            let slot = DispatchTaintSlot::default();
            slot.fill(inner.attempt.tracker.clone());
            Arc::new(TaintObservingLlmClient::new(
                inner.clone(),
                IDENTITY.into(),
                slot,
            ))
        } else {
            wrapped(kind, inner.clone())
        };
        assert!(
            matches!(client.prepare_request_attempt(messages, tools, 32, None, None),
            Err(AgentError::InternalError(message)) if message == "prepared route refused")
        );
        assert_eq!(inner.prepared.lock().unwrap().len(), 1);
        assert!(inner.attempt.tracker.identity_taint(IDENTITY).is_none());
        assert!(inner.attempt.dispatches.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn prepared_attempt_taint_marks_successful_server_result_before_returning() {
    for fail in [true, false] {
        let inner = probe(vec![unsafe_server_block()]);
        inner.attempt.fail_dispatch.store(fail, Ordering::SeqCst);
        let slot = DispatchTaintSlot::default();
        slot.fill(inner.attempt.tracker.clone());
        let client = Arc::new(TaintObservingLlmClient::new(
            inner.clone(),
            IDENTITY.into(),
            slot,
        ));
        let attempt = client
            .prepare_request_attempt(Arc::new(vec![]), Arc::new([]), 32, None, None)
            .unwrap();
        let result = attempt.stream_response(sentinel()).await;
        assert_eq!(
            *inner.attempt.dispatches.lock().unwrap(),
            [(sentinel(), false)]
        );
        if fail {
            assert!(
                matches!(result, Err(AgentError::InternalError(message)) if message == "prepared dispatch failed")
            );
            assert!(inner.attempt.tracker.identity_taint(IDENTITY).is_none());
        } else {
            assert_eq!(
                serde_json::to_value(result.unwrap().blocks()).unwrap(),
                serde_json::to_value(&inner.attempt.blocks).unwrap()
            );
            assert!(
                inner
                    .attempt
                    .tracker
                    .identity_taint(IDENTITY)
                    .unwrap()
                    .source
                    .contains("web_search")
            );
        }
    }
}

#[tokio::test]
async fn prepared_attempt_taint_retains_ingestion_on_failed_dispatch() {
    let inner = probe(vec![]);
    inner.attempt.fail_dispatch.store(true, Ordering::SeqCst);
    let slot = DispatchTaintSlot::default();
    slot.fill(inner.attempt.tracker.clone());
    let client = Arc::new(TaintObservingLlmClient::new(
        inner.clone(),
        IDENTITY.into(),
        slot,
    ));
    let (messages, tools) = untrusted_request();
    let attempt = client
        .prepare_request_attempt(messages, tools, 32, None, None)
        .unwrap();
    assert!(matches!(attempt.stream_response(sentinel()).await,
        Err(AgentError::InternalError(message)) if message == "prepared dispatch failed"));
    assert_eq!(
        *inner.attempt.dispatches.lock().unwrap(),
        [(sentinel(), true)]
    );
    assert!(inner.attempt.tracker.identity_taint(IDENTITY).is_some());
}

/// Raw provider events have no assistant identity. The real factory adapter
/// must receive the core-minted id through each installed decorator and stamp
/// the live event before the real session commits the same id to history.
struct IdentityChunks {
    requests: Mutex<Vec<Vec<Message>>>,
}

#[async_trait::async_trait]
impl meerkat_client::LlmClient for IdentityChunks {
    fn project_replay_messages(
        &self,
        messages: &[Message],
    ) -> Result<Vec<Message>, meerkat_client::LlmError> {
        Ok(messages.to_vec())
    }

    fn stream<'a>(
        &'a self,
        request: &'a meerkat_client::LlmRequest,
    ) -> meerkat_client::types::LlmStream<'a> {
        self.requests.lock().unwrap().push(request.messages.clone());
        let mut events = vec![
            meerkat_client::LlmEvent::TextDelta {
                delta: "decorated ".into(),
                meta: None,
            },
            meerkat_client::LlmEvent::TextDelta {
                delta: "reply".into(),
                meta: None,
            },
        ];
        events.extend(super::test_llm_usage::usage_then_done(
            request,
            Provider::OpenAI,
            StopReason::EndTurn,
        ));
        Box::pin(futures::stream::iter(events.into_iter().map(Ok)))
    }

    fn provider(&self) -> Provider {
        Provider::OpenAI
    }

    async fn health_check(&self) -> Result<(), meerkat_client::LlmError> {
        Ok(())
    }
}

#[tokio::test]
async fn prepared_attempt_decorated_real_session_joins_provider_deltas_to_committed_identity() {
    use std::time::Duration;

    use meerkat_core::service::{
        CreateSessionRequest, InitialTurnPolicy, SessionBuildOptions, SessionHistoryQuery,
        SessionService, SessionServiceHistoryExt,
    };

    for kind in ["replay", "taint", "taint-over-replay", "replay-over-taint"] {
        let temp = tempfile::tempdir().unwrap();
        let raw = Arc::new(IdentityChunks {
            requests: Mutex::new(Vec::new()),
        });
        let mut builder = meerkat::FactoryAgentBuilder::new(
            meerkat::AgentFactory::new(temp.path()).builtins(false),
            meerkat::Config::default(),
        );
        builder.default_llm_client = Some(raw.clone());
        let service = meerkat_session::EphemeralSessionService::new(builder, 4);
        let (events_tx, mut events_rx) = tokio::sync::mpsc::channel(128);
        let slot = DispatchTaintSlot::default();
        slot.fill(SessionTaintTracker::new(ContentTrustConfig::default()));
        let request = CreateSessionRequest {
            model: "gpt-5.5".into(),
            prompt: meerkat_core::ContentInput::Text("Check the decorated request path.".into()),
            injected_context: Vec::new(),
            system_prompt: meerkat_core::config::SystemPromptOverride::Inherit,
            max_tokens: Some(64),
            event_tx: Some(events_tx),
            initial_turn: InitialTurnPolicy::RunImmediately,
            deferred_prompt_policy: Default::default(),
            build: Some(SessionBuildOptions {
                agent_llm_client_decorator: Some(Arc::new(move |client| {
                    let taint = |inner| -> Arc<dyn AgentLlmClient> {
                        Arc::new(TaintObservingLlmClient::new(
                            inner,
                            IDENTITY.into(),
                            slot.clone(),
                        ))
                    };
                    match kind {
                        "replay" => ReplaySanitizingAgentLlmClient::wrap(client),
                        "taint" => taint(client),
                        "taint-over-replay" => taint(ReplaySanitizingAgentLlmClient::wrap(client)),
                        "replay-over-taint" => ReplaySanitizingAgentLlmClient::wrap(taint(client)),
                        _ => unreachable!(),
                    }
                })),
                ..Default::default()
            }),
            labels: None,
        };
        let completed =
            tokio::time::timeout(Duration::from_secs(30), service.create_session(request))
                .await
                .expect("decorated real session completes")
                .expect("real provider turn commits");
        let events = tokio::time::timeout(Duration::from_secs(5), async {
            let mut events = Vec::new();
            while let Some(envelope) = events_rx.recv().await {
                let done = matches!(
                    &envelope.payload,
                    meerkat_core::AgentEvent::RunCompleted { .. }
                );
                events.push(envelope.payload);
                if done {
                    break;
                }
            }
            events
        })
        .await
        .expect("actual session events arrive");
        let history = service
            .read_history(
                &completed.session_id,
                SessionHistoryQuery {
                    offset: 0,
                    limit: None,
                },
            )
            .await
            .expect("committed real session history");
        service.shutdown().await;
        assert_eq!(history.session_id, completed.session_id);
        assert!(!history.has_more);
        assert_eq!(
            raw.requests.lock().unwrap().len(),
            1,
            "one actual provider request for {kind}"
        );
        let assistants = history
            .messages
            .iter()
            .filter_map(|message| match message {
                Message::BlockAssistant(assistant) => Some(assistant),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            assistants.len(),
            1,
            "one canonical provider reply for {kind}"
        );
        let committed_id = assistants[0]
            .assistant_message_id
            .expect("core commits an occurrence identity");
        assert_eq!(
            assistants[0].text_blocks().collect::<String>(),
            "decorated reply"
        );
        let deltas = events
            .iter()
            .filter_map(|event| match event {
                meerkat_core::AgentEvent::TextDelta {
                    delta,
                    assistant_message_id,
                } => Some((delta, assistant_message_id)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            !deltas.is_empty(),
            "the real adapter emits live provider text for {kind}"
        );
        assert_eq!(
            deltas
                .iter()
                .map(|(delta, _)| delta.as_str())
                .collect::<String>(),
            "decorated reply"
        );
        for (_, live_id) in deltas {
            assert_eq!(
                *live_id,
                Some(committed_id),
                "{kind} forwards the core id through the real prepared adapter"
            );
        }
    }
}
