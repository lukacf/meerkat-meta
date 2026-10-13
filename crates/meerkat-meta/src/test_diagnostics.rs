//! Shared controls for nonterminal diagnostics beside physical results.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use meerkat_client::{LlmClient, LlmDoneOutcome, LlmError, LlmEvent, LlmRequest};

pub(crate) const PHYSICAL_TEXT: &str = "physical response, not diagnostic text";

#[derive(Clone, Copy, Debug)]
pub(crate) enum CompletionCase {
    Success,
    TerminalError,
    StreamError,
    Truncated,
}

impl CompletionCase {
    pub(crate) const ALL: [Self; 4] = [
        Self::Success,
        Self::TerminalError,
        Self::StreamError,
        Self::Truncated,
    ];
}

pub(crate) struct ObservationClient {
    events: Vec<Result<LlmEvent, LlmError>>,
    calls: AtomicUsize,
    operation_id: meerkat_core::OperationId,
}

impl ObservationClient {
    pub(crate) fn new(case: CompletionCase) -> Self {
        let operation_id = meerkat_core::OperationId::new();
        let events = vec![
            Ok(LlmEvent::TextDelta {
                delta: PHYSICAL_TEXT.into(),
                meta: None,
            }),
            Ok(LlmEvent::OperationObservationFailed {
                operation_id: operation_id.clone(),
                phase: meerkat_core::authorization::OperationObservationPhase::Outcome,
            }),
            match case {
                CompletionCase::Success => Ok(LlmEvent::Done {
                    outcome: LlmDoneOutcome::Success {
                        stop_reason: meerkat_core::StopReason::EndTurn,
                    },
                }),
                CompletionCase::TerminalError => Ok(LlmEvent::Done {
                    outcome: LlmDoneOutcome::Error {
                        error: LlmError::ConnectionReset,
                    },
                }),
                CompletionCase::StreamError => Err(LlmError::ConnectionReset),
                CompletionCase::Truncated => Ok(LlmEvent::Done {
                    outcome: LlmDoneOutcome::Success {
                        stop_reason: meerkat_core::StopReason::MaxTokens,
                    },
                }),
            },
        ];
        Self {
            events,
            calls: AtomicUsize::new(0),
            operation_id,
        }
    }

    pub(crate) fn assert_observed_once_without_retry(&self, log: &str) {
        assert_eq!(self.calls.load(Ordering::SeqCst), 1);
        assert_eq!(log.matches("LLM operation observation failed").count(), 1);
        assert!(log.contains(&self.operation_id.to_string()), "{log}");
        assert!(log.contains("phase=Outcome"), "{log}");
        assert!(!log.contains(PHYSICAL_TEXT), "response text leaked: {log}");
    }
}

#[async_trait]
impl LlmClient for ObservationClient {
    fn stream<'a>(&'a self, _request: &'a LlmRequest) -> meerkat_client::types::LlmStream<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::stream::iter(self.events.clone()))
    }

    fn provider(&self) -> meerkat_core::Provider {
        meerkat_core::Provider::Other
    }

    async fn health_check(&self) -> Result<(), LlmError> {
        Ok(())
    }
}

#[derive(Clone, Default)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CaptureWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

pub(crate) async fn capture_warnings<T>(future: impl Future<Output = T>) -> (T, String) {
    use tracing::instrument::WithSubscriber;

    let writer = CaptureWriter::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .without_time()
        .finish();
    let result = future.with_subscriber(subscriber).await;
    let log = String::from_utf8_lossy(
        &writer
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
    .into_owned();
    (result, log)
}

pub(crate) fn settlement_failure(
    is_error: bool,
) -> meerkat_core::ops::ToolDispatchSettlementFailure {
    meerkat_core::ops::ToolDispatchSettlementFailure {
        admission_source: meerkat_core::ops::ToolDispatchAdmissionSource::AuthorizationAudit,
        effect_kind: meerkat_core::LiveBridgeEffectKind::ToolDispatch,
        physical_outcome: if is_error {
            meerkat_core::LiveBridgeEffectOutcome::Failed
        } else {
            meerkat_core::LiveBridgeEffectOutcome::Committed
        },
        failure_kind: meerkat_core::ops::ToolDispatchTerminalErrorKind::Unavailable,
    }
}
