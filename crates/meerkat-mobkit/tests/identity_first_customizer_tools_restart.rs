//! `customize_build` tools across a restart (#563), on the gateway-shaped
//! harness of `identity_first_head_canonical_resume` (state-dir continuity
//! substrate, persistent state, recording LLM double).
//!
//! - After a restart the restored member's tool scope already lists its
//!   customizer tools BEFORE any turn: MobKit publishes them before the mob is
//!   lifted, so meerkat's restore builds the member with them.
//! - The first turn after the restart (the shape of a kickoff or a queued
//!   input admitted at activation) carries them in its LLM request.
//! - A pre-activation `customize_build` that fails is recorded as
//!   `IdentityStatus::customizer_tools_pending` with the typed reason, and the
//!   materialization that publishes clears it. The mob build also asks meerkat
//!   to hold the member's run starts (`HostRunStartHoldReason::ToolsNotPublished`)
//!   until then; here the publication releases that hold before the member's
//!   registration, so the member runs unheld once materialized. (The hold
//!   takes effect when meerkat's own resume revives the member first, a mob
//!   log left Running; the release itself is unit-tested in the identity
//!   runtime.)
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use meerkat_client::{LlmError, LlmEvent, LlmRequest};
use meerkat_core::types::{StopReason, ToolCallView, ToolDef};
use meerkat_core::{AgentToolDispatcher, ToolDispatchOutcome, ToolError};
use meerkat_mob::{MobDefinition, ProfileName};
use meerkat_mobkit::UnifiedRuntimeBuilder;
use meerkat_mobkit::identity_first::contracts::{AgentCustomizer, RosterProvider};
use meerkat_mobkit::identity_first::{
    AgentAddressability, AgentBuildContext, AgentBuildDraft, AgentIdentity, CustomizerError,
    DurableAgentSpec, IdentityBootstrapMode, LocalExternalToolOverlay, RosterContext, RosterError,
};
use tokio::time::sleep;

#[path = "support/llm_usage.rs"]
mod llm_usage;

/// `{MOB_ID}` is replaced per test: tests in this binary run concurrently and
/// one process-wide comms namespace refuses two mobs with the same id.
const MOB_TOML: &str = r#"
[mob]
id = "{MOB_ID}"

[profiles.personal]
model = "gpt-5.5"
external_addressable = true
runtime_mode = "turn_driven"

[profiles.personal.tools]
comms = true
"#;

const MEMBER: &str = "personal:alice";
const TOOL: &str = "lookup_workspace";

fn id(name: &str) -> AgentIdentity {
    AgentIdentity::parse(name).expect("parse identity")
}

fn durable_spec(name: &str) -> DurableAgentSpec {
    DurableAgentSpec {
        identity: id(name),
        profile: ProfileName::from("personal"),
        addressability: AgentAddressability::Addressable,
        display_name: None,
        labels: BTreeMap::new(),
        context: None,
        additional_instructions: Vec::new(),
        initial_message: None,
        runtime_mode_override: None,
        backend: None,
        binding: None,
        placement: None,
    }
}

struct OneMemberRoster;

#[async_trait]
impl RosterProvider for OneMemberRoster {
    async fn roster(&self, _context: &RosterContext) -> Result<Vec<DurableAgentSpec>, RosterError> {
        Ok(vec![durable_spec(MEMBER)])
    }
}

/// The host's tools for one build.
struct HostTools;

#[async_trait]
impl AgentToolDispatcher for HostTools {
    fn tools(&self) -> Arc<[Arc<ToolDef>]> {
        vec![Arc::new(ToolDef {
            audience: Default::default(),
            name: TOOL.into(),
            description: "Look up the workspace record.".to_string(),
            input_schema: serde_json::json!({"type": "object"}),
            provenance: None,
        })]
        .into()
    }

    async fn dispatch(&self, call: ToolCallView<'_>) -> Result<ToolDispatchOutcome, ToolError> {
        Ok(meerkat_core::ToolResult {
            host_metadata: Default::default(),
            tool_use_id: call.id.to_string(),
            content: vec![],
            is_error: false,
            settlement_failures: Vec::new(),
        }
        .into())
    }
}

/// A host customizer that declares [`TOOL`] and refuses the calls whose
/// 1-based ordinal is in `fail_calls`.
#[derive(Default)]
struct ToolCustomizer {
    calls: AtomicUsize,
    fail_calls: BTreeSet<usize>,
}

#[async_trait]
impl AgentCustomizer for ToolCustomizer {
    async fn customize_build(
        &self,
        _context: &AgentBuildContext,
        _spec: &DurableAgentSpec,
        draft: &mut AgentBuildDraft,
    ) -> Result<(), CustomizerError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_calls.contains(&call) {
            return Err(CustomizerError::Io(format!("host refused build {call}")));
        }
        draft.local_external_tools = LocalExternalToolOverlay::new(Arc::new(HostTools));
        Ok(())
    }
}

#[derive(Clone, Default)]
struct CaptureClient {
    requests: Arc<std::sync::Mutex<Vec<String>>>,
}

impl CaptureClient {
    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
    fn last(&self) -> Option<String> {
        self.requests.lock().unwrap().last().cloned()
    }
}

impl meerkat_client::LlmClient for CaptureClient {
    fn project_replay_messages(
        &self,
        messages: &[meerkat_core::Message],
    ) -> Result<Vec<meerkat_core::Message>, LlmError> {
        Ok(messages.to_vec())
    }
    fn stream<'a>(
        &'a self,
        request: &'a LlmRequest,
    ) -> Pin<Box<dyn futures::Stream<Item = Result<LlmEvent, LlmError>> + Send + 'a>> {
        self.requests
            .lock()
            .unwrap()
            .push(serde_json::to_string(request).unwrap_or_default());
        let [usage, done] =
            llm_usage::usage_then_done(request, meerkat::Provider::OpenAI, StopReason::EndTurn);
        Box::pin(async_stream::stream! {
            yield Ok(LlmEvent::TextDelta { delta: "ok".to_string(), meta: None });
            yield Ok(usage);
            yield Ok(done);
        })
    }
    fn provider(&self) -> meerkat::Provider {
        meerkat::Provider::OpenAI
    }
    fn health_check<'life0, 'async_trait>(
        &'life0 self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), LlmError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { Ok(()) })
    }
}

async fn boot(
    mob_id: &str,
    state: &Path,
    capture: CaptureClient,
    customizer: Arc<ToolCustomizer>,
    mode: IdentityBootstrapMode,
) -> meerkat_mobkit::UnifiedRuntime {
    Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(
                MobDefinition::from_toml(&MOB_TOML.replace("{MOB_ID}", mob_id))
                    .expect("definition"),
            )
            .persistent_state(state)
            .continuity_from_state_dir(state)
            .await
            .expect("open the state-dir identity substrate")
            .roster_provider(Arc::new(OneMemberRoster))
            .agent_customizer(customizer)
            .identity_bootstrap_mode(mode)
            .identity_runtime_instance_id(mob_id)
            .comms(true)
            .default_llm_client(Arc::new(capture))
            .build(),
    )
    .await
    .expect("build the gateway-shaped UnifiedRuntime")
}

async fn resolved_tools(runtime: &meerkat_mobkit::UnifiedRuntime) -> Vec<String> {
    let identity_runtime = runtime.identity_runtime().expect("identity runtime");
    let status = identity_runtime.status(&id(MEMBER)).await.expect("status");
    let session_id = status.session_id.expect("member has a session");
    meerkat_mobkit::mob_handle_runtime::resolved_tools_for_session(
        runtime.mob_runtime().session_service(),
        MEMBER,
        session_id,
    )
    .await
    .expect("resolved tools")
    .tools
}

/// Whether meerkat holds the member's run starts, read from its runtime
/// (`None` while the member's session is not registered there).
async fn member_run_starts_held(runtime: &meerkat_mobkit::UnifiedRuntime) -> Option<bool> {
    let status = runtime.identity_runtime()?.status(&id(MEMBER)).await.ok()?;
    let session_id = status.session_id?;
    let machine = runtime
        .mob_runtime()
        .session_service()?
        .acquire_runtime_adapter(None)
        .expect("acquire the customizer restart fixture's runtime adapter")?;
    machine.run_starts_held_for_test(&session_id).await
}

async fn wait_for_turn(capture: &CaptureClient, want: usize, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while capture.count() < want {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(100)).await;
    }
}

/// Boot once (fresh materialization, tools present at spawn) and stop.
async fn first_boot(mob_id: &str, state: &Path) {
    let capture = CaptureClient::default();
    let runtime = boot(
        mob_id,
        state,
        capture.clone(),
        Arc::new(ToolCustomizer::default()),
        IdentityBootstrapMode::EagerMaterialize,
    )
    .await;
    assert!(resolved_tools(&runtime).await.contains(&TOOL.to_string()));
    runtime
        .identity_runtime()
        .expect("identity runtime")
        .send(
            &id(MEMBER),
            &meerkat_core::ContentInput::Text("hello".to_string()),
        )
        .await
        .expect("send boot-1 turn");
    wait_for_turn(&capture, 1, "boot 1's turn").await;
    sleep(Duration::from_millis(500)).await;
    runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn restored_member_lists_its_customizer_tools_before_the_first_turn() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    first_boot("customizer-tools-restart-a", &state).await;

    let capture = CaptureClient::default();
    let customizer = Arc::new(ToolCustomizer::default());
    let runtime = boot(
        "customizer-tools-restart-a",
        &state,
        capture.clone(),
        customizer.clone(),
        IdentityBootstrapMode::EagerMaterialize,
    )
    .await;
    // Published before the lift (once) and again by the materialization.
    assert!(customizer.calls.load(Ordering::SeqCst) >= 2);
    assert_eq!(capture.count(), 0, "no turn has run yet");
    assert!(
        resolved_tools(&runtime).await.contains(&TOOL.to_string()),
        "the restored member's tool scope lists its customizer tools before any turn"
    );
    let status = runtime
        .identity_runtime()
        .expect("identity runtime")
        .status(&id(MEMBER))
        .await
        .expect("status");
    assert!(status.customizer_tools_pending.is_none());

    // The first run after the restart, the shape of a kickoff admitted at
    // activation, offers the tool to the model.
    runtime
        .identity_runtime()
        .expect("identity runtime")
        .send(
            &id(MEMBER),
            &meerkat_core::ContentInput::Text("again".to_string()),
        )
        .await
        .expect("send boot-2 turn");
    wait_for_turn(&capture, 1, "boot 2's first turn").await;
    let request = capture.last().expect("captured request");
    assert!(
        request.contains(TOOL),
        "the first turn after the restart must offer the customizer tool"
    );
    runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_early_customize_is_pending_until_the_materialization_publishes() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    first_boot("customizer-tools-restart-b", &state).await;

    // Lazy bootstrap keeps the member unmaterialized after the restore, so
    // the pending state is observable; the early (first) call fails.
    let capture = CaptureClient::default();
    let customizer = Arc::new(ToolCustomizer {
        calls: AtomicUsize::new(0),
        fail_calls: BTreeSet::from([1]),
    });
    let runtime = boot(
        "customizer-tools-restart-b",
        &state,
        capture.clone(),
        customizer.clone(),
        IdentityBootstrapMode::LazyMaterialize,
    )
    .await;
    let identity_runtime = runtime.identity_runtime().expect("identity runtime");
    let status = identity_runtime.status(&id(MEMBER)).await.expect("status");
    let pending = status
        .customizer_tools_pending
        .expect("the failed early publication is recorded");
    assert!(
        pending
            .reason
            .contains("pre-activation customize_build failed")
            && pending.reason.contains("host refused build 1"),
        "{pending:?}"
    );
    // Lazy: nothing has registered the restored member's runtime yet, and
    // the materialization publishes (releasing the hold) before it registers.
    assert_eq!(member_run_starts_held(&runtime).await, None);

    // Materialization (here driven by a send) publishes and clears it.
    identity_runtime
        .send(
            &id(MEMBER),
            &meerkat_core::ContentInput::Text("wake".to_string()),
        )
        .await
        .expect("send materializes");
    wait_for_turn(&capture, 1, "the materializing turn").await;
    let status = identity_runtime.status(&id(MEMBER)).await.expect("status");
    assert!(status.customizer_tools_pending.is_none(), "{status:?}");
    assert_eq!(
        member_run_starts_held(&runtime).await,
        Some(false),
        "the released hold never reached the member's registration"
    );
    assert!(resolved_tools(&runtime).await.contains(&TOOL.to_string()));
    assert!(capture.last().expect("captured request").contains(TOOL));
    runtime.shutdown().await;
}
