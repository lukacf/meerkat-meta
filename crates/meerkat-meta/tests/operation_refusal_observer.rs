//! A host's operation refusal observer, installed with
//! `UnifiedRuntimeBuilder::operation_refusal_observer`, reaches the member
//! agents of each builder-owned launch: persistent state, a scratch
//! directory (the identity-first composition) and the default temp-dir
//! ephemeral one. A member's tool owner refuses the one call its model
//! makes, and the observer receives exactly that refusal as the call's tool
//! result.
//!
//! Waiting for the observation is a typed watch on the observation count,
//! bounded only by a named failure timeout.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use meerkat_client::{LlmClient, LlmDoneOutcome, LlmError, LlmEvent, LlmRequest};
use meerkat_core::types::{ToolCallView, ToolDef};
use meerkat_core::{
    AgentToolDispatcher, Message, OperationRefusalDisposition, OperationRefusalKind,
    OperationRefusalObservation, OperationRefusalObserver, OperationRefusalOutcome,
    OperationRefusalSurface, OperationRefused, Provider, RefusalDispatch, StopReason,
    ToolDispatchOutcome, ToolError,
};
use meerkat_meta::UnifiedRuntime;
use meerkat_meta::identity_first::{
    AgentAddressability, AgentIdentity, DurableAgentSpec, LocalContinuityStore, LocalLeaseProvider,
    MutableRosterProvider,
};
use meerkat_mob::{MobDefinition, SpawnMemberSpec};

/// How long a member turn may take to reach the observer before the test
/// fails.
const OBSERVATION_FAILURE_BOUND: Duration = Duration::from_secs(30);
const REFUSED_TOOL: &str = "refused_probe";
const REFUSED_CALL: &str = "refused-call";
const MEMBER: &str = "helper";

fn definition(mob_id: &str) -> MobDefinition {
    MobDefinition::from_toml(&format!(
        r#"
[mob]
id = "{mob_id}"

[profiles.worker]
model = "gpt-5.5"
runtime_mode = "autonomous_host"
external_addressable = true

[profiles.worker.tools]
comms = true
rust_bundles = ["refusing"]
"#
    ))
    .expect("parse mob definition")
}

/// One tool whose owner refuses every call.
struct RefusingBundle;

#[async_trait::async_trait]
impl AgentToolDispatcher for RefusingBundle {
    fn tools(&self) -> Arc<[Arc<ToolDef>]> {
        vec![Arc::new(ToolDef::new(
            REFUSED_TOOL,
            "A tool whose owner refuses every call.",
            serde_json::json!({"type": "object", "properties": {}}),
        ))]
        .into()
    }

    async fn dispatch(&self, _call: ToolCallView<'_>) -> Result<ToolDispatchOutcome, ToolError> {
        Err(ToolError::AuthorizationRefused {
            refusal: OperationRefused::new(OperationRefusalKind::Denied),
        })
    }
}

/// Calls the refused tool, then ends the turn once the call has a result.
struct RefusedCallClient;

#[async_trait::async_trait]
impl LlmClient for RefusedCallClient {
    fn project_replay_messages(&self, messages: &[Message]) -> Result<Vec<Message>, LlmError> {
        Ok(messages.to_vec())
    }

    fn stream<'a>(&'a self, request: &'a LlmRequest) -> meerkat_client::types::LlmStream<'a> {
        let after_tool = matches!(request.messages.last(), Some(Message::ToolResults { .. }));
        let (event, stop_reason) = if after_tool {
            (
                LlmEvent::TextDelta {
                    delta: "done".to_string(),
                    meta: None,
                },
                StopReason::EndTurn,
            )
        } else {
            (
                LlmEvent::ToolCallComplete {
                    id: REFUSED_CALL.to_string(),
                    name: REFUSED_TOOL.to_string(),
                    args: serde_json::json!({}),
                    meta: None,
                },
                StopReason::ToolUse,
            )
        };
        Box::pin(futures::stream::iter(vec![
            Ok(event),
            Ok(LlmEvent::Done {
                outcome: LlmDoneOutcome::success(stop_reason),
            }),
        ]))
    }

    fn provider(&self) -> Provider {
        Provider::OpenAI
    }

    async fn health_check(&self) -> Result<(), LlmError> {
        Ok(())
    }
}

/// Records every observation, with a count to wait on.
struct RecordingObserver {
    observed: Mutex<Vec<OperationRefusalObservation>>,
    count: tokio::sync::watch::Sender<usize>,
}

impl RecordingObserver {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            observed: Mutex::new(Vec::new()),
            count: tokio::sync::watch::channel(0).0,
        })
    }

    /// Surface, outcome and disposition of every observation, once at least
    /// one exists.
    async fn observed(
        &self,
    ) -> Vec<(
        OperationRefusalSurface,
        OperationRefusalOutcome,
        OperationRefusalDisposition,
    )> {
        let mut count = self.count.subscribe();
        tokio::time::timeout(OBSERVATION_FAILURE_BOUND, count.wait_for(|seen| *seen >= 1))
            .await
            .expect("the member's refused call reached the observer within the failure bound")
            .expect("observer alive");
        self.observed
            .lock()
            .unwrap()
            .iter()
            .map(|observation| {
                (
                    observation.surface().clone(),
                    observation.outcome(),
                    observation.disposition(),
                )
            })
            .collect()
    }
}

impl OperationRefusalObserver for RecordingObserver {
    fn observe(&self, observation: &OperationRefusalObservation) {
        let seen = {
            let mut observed = self.observed.lock().unwrap();
            observed.push(observation.clone());
            observed.len()
        };
        self.count.send_replace(seen);
    }
}

/// The one observation every launch must produce: the refused call, returned
/// to the model as its tool result.
fn refused_call() -> Vec<(
    OperationRefusalSurface,
    OperationRefusalOutcome,
    OperationRefusalDisposition,
)> {
    vec![(
        OperationRefusalSurface::ToolCall {
            tool_use_id: REFUSED_CALL.to_string(),
        },
        OperationRefusalOutcome::Refused {
            kind: OperationRefusalKind::Denied,
            dispatch: RefusalDispatch::Unknown,
        },
        OperationRefusalDisposition::ToolResult,
    )]
}

/// Spawns the member and sends it one message; its turn calls the refused
/// tool.
async fn spawn_and_send(runtime: &UnifiedRuntime) {
    runtime
        .spawn(SpawnMemberSpec::from_wire(
            "worker".to_string(),
            MEMBER.to_string(),
            None,
            None,
            None,
        ))
        .await
        .expect("spawn");
    meerkat_meta::send_message_on_mob(&runtime.mob_handle(), MEMBER, "hello".to_string())
        .await
        .expect("send message");
}

/// The persistent launch (`persistent_state`).
#[tokio::test(flavor = "multi_thread")]
async fn a_persistent_member_reports_its_refused_call_to_the_host_observer() {
    let state = tempfile::tempdir().unwrap();
    let observer = RecordingObserver::new();
    let runtime = Box::pin(
        UnifiedRuntime::builder()
            .definition(definition("refusal-observer-persistent"))
            .persistent_state(state.path().join("state"))
            .default_llm_client(Arc::new(RefusedCallClient))
            .register_tool_bundle("refusing", Arc::new(RefusingBundle))
            .operation_refusal_observer(observer.clone())
            .build(),
    )
    .await
    .expect("runtime builds");
    spawn_and_send(&runtime).await;
    assert_eq!(observer.observed().await, refused_call());
    runtime.shutdown().await;
}

/// The default launch: neither persistent state nor a scratch directory,
/// so the builder owns a temp-dir ephemeral composition.
#[tokio::test(flavor = "multi_thread")]
async fn a_temp_dir_member_reports_its_refused_call_to_the_host_observer() {
    let observer = RecordingObserver::new();
    let runtime = Box::pin(
        UnifiedRuntime::builder()
            .definition(definition("refusal-observer-temp-dir"))
            .default_llm_client(Arc::new(RefusedCallClient))
            .register_tool_bundle("refusing", Arc::new(RefusingBundle))
            .operation_refusal_observer(observer.clone())
            .build(),
    )
    .await
    .expect("runtime builds");
    spawn_and_send(&runtime).await;
    assert_eq!(observer.observed().await, refused_call());
    runtime.shutdown().await;
}

/// The scratch launch: a scratch directory selects the identity-first
/// composition over the runtime-backed ephemeral spec. The roster member's
/// turn runs through the mob's host input.
#[tokio::test(flavor = "multi_thread")]
async fn a_scratch_member_reports_its_refused_call_to_the_host_observer() {
    let scratch = tempfile::tempdir().unwrap();
    let observer = RecordingObserver::new();
    let identity = AgentIdentity::parse(MEMBER).unwrap();
    let roster = Arc::new(MutableRosterProvider::new(vec![DurableAgentSpec {
        identity: identity.clone(),
        profile: "worker".into(),
        addressability: AgentAddressability::Addressable,
        display_name: None,
        labels: BTreeMap::new(),
        context: None,
        additional_instructions: Vec::new(),
        initial_message: None,
        runtime_mode_override: Some(meerkat_mob::MobRuntimeMode::AutonomousHost),
        backend: None,
        binding: None,
        placement: None,
    }]));
    let runtime = Box::pin(
        UnifiedRuntime::builder()
            .definition(definition("refusal-observer-scratch"))
            .continuity_store(Arc::new(LocalContinuityStore::in_memory().unwrap()))
            .lease_provider(Arc::new(LocalLeaseProvider::new()))
            .roster_provider(roster)
            .scratch_dir(scratch.path())
            .identity_runtime_instance_id("refusal-observer-scratch")
            .default_llm_client(Arc::new(RefusedCallClient))
            .register_tool_bundle("refusing", Arc::new(RefusingBundle))
            .operation_refusal_observer(observer.clone())
            .build(),
    )
    .await
    .expect("runtime builds");
    run_host_turn(&runtime, &identity).await;
    assert_eq!(observer.observed().await, refused_call());
    runtime.mob_handle().stop().await.unwrap();
}

/// One host input turn for the roster member, waited to its end.
async fn run_host_turn(runtime: &UnifiedRuntime, identity: &AgentIdentity) {
    let handle = runtime.mob_handle();
    let member = handle
        .get_member(&identity.as_str().into())
        .await
        .unwrap()
        .expect("the roster member is live");
    let interaction = uuid::Uuid::new_v4();
    let turn = handle
        .start_host_human_input_bounded(
            member.agent_runtime_id,
            member.fence_token,
            meerkat_mob::WorkSpec::new("Call the tool.", meerkat_mob::WorkOrigin::External)
                .with_interaction_id(meerkat_core::interaction::InteractionId(interaction)),
            meerkat_core::types::HandlingMode::Queue,
            meerkat_mob::MobDeliveryIdentity::new(interaction.to_string(), interaction.to_string())
                .unwrap(),
            std::time::Instant::now() + OBSERVATION_FAILURE_BOUND,
        )
        .await
        .unwrap();
    tokio::time::timeout(OBSERVATION_FAILURE_BOUND, turn.wait())
        .await
        .unwrap()
        .unwrap();
}
