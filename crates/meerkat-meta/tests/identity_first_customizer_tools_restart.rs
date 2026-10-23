//! `customize_build` tools across a restart (#563), on the gateway-shaped
//! harness of `identity_first_head_canonical_resume` (state-dir continuity
//! substrate, persistent state, recording LLM double).
//!
//! - After a restart the restored member's tool scope already lists its
//!   customizer tools BEFORE any turn: Meta publishes them before the mob is
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
//! - A member meerkat's resume revives whose identity the roster no longer
//!   lists starts no run before the roster pass retires it: nothing
//!   publishes its customizer tools, and the mob build holds its run starts.
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
use meerkat_meta::UnifiedRuntimeBuilder;
use meerkat_meta::identity_first::contracts::{AgentCustomizer, RosterProvider};
use meerkat_meta::identity_first::{
    AgentAddressability, AgentBuildContext, AgentBuildDraft, AgentIdentity, CustomizerError,
    DurableAgentSpec, IdentityBootstrapMode, LocalExternalToolOverlay, RosterContext, RosterError,
};
use meerkat_mob::{MobDefinition, ProfileName};
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

/// A roster that lists these identities.
struct ListedRoster(&'static [&'static str]);

#[async_trait]
impl RosterProvider for ListedRoster {
    async fn roster(&self, _context: &RosterContext) -> Result<Vec<DurableAgentSpec>, RosterError> {
        Ok(self.0.iter().map(|name| durable_spec(name)).collect())
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

/// A host customizer that declares [`TOOL`] only when its build context
/// carries the mob handle, as a host whose tools act on the mob does.
#[derive(Default)]
struct HandleToolCustomizer {
    calls: AtomicUsize,
}

#[async_trait]
impl AgentCustomizer for HandleToolCustomizer {
    async fn customize_build(
        &self,
        context: &AgentBuildContext,
        _spec: &DurableAgentSpec,
        draft: &mut AgentBuildDraft,
    ) -> Result<(), CustomizerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if context.runtime_services.mob_handle().is_none() {
            return Err(CustomizerError::Io(
                "mob handle missing for the host's tools".to_string(),
            ));
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
    /// Whether a model request carried `text`.
    fn saw(&self, text: &str) -> bool {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.contains(text))
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
    customizer: Arc<dyn AgentCustomizer>,
    mode: IdentityBootstrapMode,
) -> meerkat_meta::UnifiedRuntime {
    boot_listing(mob_id, state, capture, customizer, mode, &[MEMBER]).await
}

/// [`boot`] with a roster that lists `roster`.
async fn boot_listing(
    mob_id: &str,
    state: &Path,
    capture: CaptureClient,
    customizer: Arc<dyn AgentCustomizer>,
    mode: IdentityBootstrapMode,
    roster: &'static [&'static str],
) -> meerkat_meta::UnifiedRuntime {
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
            .roster_provider(Arc::new(ListedRoster(roster)))
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

async fn resolved_tools(runtime: &meerkat_meta::UnifiedRuntime) -> Vec<String> {
    let identity_runtime = runtime.identity_runtime().expect("identity runtime");
    let status = identity_runtime.status(&id(MEMBER)).await.expect("status");
    let session_id = status.session_id.expect("member has a session");
    meerkat_meta::mob_handle_runtime::resolved_tools_for_session(
        runtime.mob_runtime().session_service(),
        MEMBER,
        session_id,
    )
    .await
    .expect("resolved tools")
    .tools
}

/// The visible names of the member's tool scope, as its last model-call
/// boundary left them.
async fn scope_visible_tools(runtime: &meerkat_meta::UnifiedRuntime) -> Vec<String> {
    let identity_runtime = runtime.identity_runtime().expect("identity runtime");
    let status = identity_runtime.status(&id(MEMBER)).await.expect("status");
    let session_id = status.session_id.expect("member has a session");
    let mut names = runtime
        .mob_runtime()
        .session_service()
        .expect("session service")
        .tool_scope_snapshot(&session_id)
        .await
        .expect("read the tool scope")
        .expect("the member's tool scope")
        .visible_names
        .into_iter()
        .map(meerkat_core::types::ToolName::into_string)
        .collect::<Vec<_>>();
    names.sort();
    names
}

/// Whether meerkat holds the member's run starts, read from its runtime
/// (`None` while the member's session is not registered there).
async fn member_run_starts_held(runtime: &meerkat_meta::UnifiedRuntime) -> Option<bool> {
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

    // Both publications before the lift fail (the mob build's early one and
    // the bootstrap's), so the pending state is observable after the restore;
    // so does the first send's.
    let capture = CaptureClient::default();
    let customizer = Arc::new(ToolCustomizer {
        calls: AtomicUsize::new(0),
        fail_calls: BTreeSet::from([1, 2, 3]),
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
            && pending.reason.contains("host refused build 2"),
        "{pending:?}"
    );
    // Core restores the member live at activation (it ran at the last stop),
    // and it registers with its run starts held until its tools are
    // published.
    assert_eq!(member_run_starts_held(&runtime).await, Some(true));

    // A send whose publication fails (the third build) is refused as
    // retryable: nothing is queued behind the hold.
    let refused = identity_runtime
        .send(
            &id(MEMBER),
            &meerkat_core::ContentInput::Text("refused".to_string()),
        )
        .await;
    assert!(
        matches!(
            refused,
            Err(meerkat_meta::identity_first::IdentityRuntimeError::IdentityWakeRetryable { .. })
        ),
        "{refused:?}"
    );
    let status = identity_runtime.status(&id(MEMBER)).await.expect("status");
    let pending = status
        .customizer_tools_pending
        .expect("the failed publication stays recorded");
    assert!(
        pending.reason.contains("host refused build 3"),
        "{pending:?}"
    );
    assert_eq!(member_run_starts_held(&runtime).await, Some(true));
    assert_eq!(capture.count(), 0, "nothing ran behind the hold");

    // The next send's delivery publishes them (the fourth build), which
    // clears the pending state and releases the hold.
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

/// The mob build's early publication fails, so meerkat stages a run-start
/// hold for the restored member, and the bootstrap's own publication before
/// the lift succeeds. The hold is released once the mob is live: the member
/// runs with its tools, and nothing stays pending.
#[tokio::test(flavor = "multi_thread")]
async fn a_publication_before_the_lift_releases_the_hold_the_mob_build_staged() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    first_boot("customizer-tools-restart-c", &state).await;

    let capture = CaptureClient::default();
    let customizer = Arc::new(ToolCustomizer {
        calls: AtomicUsize::new(0),
        fail_calls: BTreeSet::from([1]),
    });
    let runtime = boot(
        "customizer-tools-restart-c",
        &state,
        capture.clone(),
        customizer.clone(),
        IdentityBootstrapMode::LazyMaterialize,
    )
    .await;
    assert_eq!(
        customizer.calls.load(Ordering::SeqCst),
        2,
        "the mob build's early publication and the bootstrap's"
    );
    let identity_runtime = runtime.identity_runtime().expect("identity runtime");
    let status = identity_runtime.status(&id(MEMBER)).await.expect("status");
    assert!(status.customizer_tools_pending.is_none(), "{status:?}");
    assert_eq!(
        member_run_starts_held(&runtime).await,
        Some(false),
        "the hold the mob build staged is released once the mob is live"
    );
    assert!(resolved_tools(&runtime).await.contains(&TOOL.to_string()));

    identity_runtime
        .send(
            &id(MEMBER),
            &meerkat_core::ContentInput::Text("after the restart".to_string()),
        )
        .await
        .expect("send boot-2 turn");
    wait_for_turn(&capture, 1, "boot 2's first turn").await;
    assert!(capture.last().expect("captured request").contains(TOOL));
    runtime.shutdown().await;
}

/// Set in the child process of
/// [`a_member_restored_after_an_unclean_stop_lists_its_customizer_tools_before_the_first_turn`]:
/// the state directory its first boot runs in.
const UNCLEAN_STOP_CHILD_STATE: &str = "META_CUSTOMIZER_TOOLS_UNCLEAN_STOP_CHILD_STATE";

/// After an unclean stop (a kill: no Stop is recorded, so the mob log stays
/// Running) meerkat's own resume revives the member while the mob is built,
/// and no lift is staged. The mob build's early publication runs before the
/// mob handle exists, so a customizer whose tools need it fails there and the
/// member is held; the bootstrap must still publish its tools, with the
/// runtime's services (which carry the handle), and release the hold, as it
/// does after a clean stop.
#[tokio::test(flavor = "multi_thread")]
async fn a_member_restored_after_an_unclean_stop_lists_its_customizer_tools_before_the_first_turn()
{
    const MOB_ID: &str = "customizer-tools-restart-d";
    if let Some(state) = std::env::var_os(UNCLEAN_STOP_CHILD_STATE) {
        // The child: the first boot, a turn, then an exit without shutdown.
        let capture = CaptureClient::default();
        let runtime = boot(
            MOB_ID,
            Path::new(&state),
            capture.clone(),
            Arc::new(ToolCustomizer::default()),
            IdentityBootstrapMode::EagerMaterialize,
        )
        .await;
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
        std::process::exit(0);
    }

    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let child = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "a_member_restored_after_an_unclean_stop_lists_its_customizer_tools_before_the_first_turn",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(UNCLEAN_STOP_CHILD_STATE, &state)
        .status()
        .expect("run the first boot in a child process");
    assert!(child.success(), "the child's first boot failed: {child}");

    let capture = CaptureClient::default();
    let customizer = Arc::new(HandleToolCustomizer::default());
    let runtime = boot(
        MOB_ID,
        &state,
        capture.clone(),
        customizer.clone(),
        IdentityBootstrapMode::LazyMaterialize,
    )
    .await;
    let identity_runtime = runtime.identity_runtime().expect("identity runtime");
    // Before any turn: the tools are published and the hold is released, so
    // a run Core starts on its own (a restored queued input, a keep-alive
    // turn, a peer message) is not held and carries them.
    let status = identity_runtime.status(&id(MEMBER)).await.expect("status");
    assert!(
        status.customizer_tools_pending.is_none(),
        "the restored member's customizer tools are published before any turn: {status:?}"
    );
    assert_eq!(
        member_run_starts_held(&runtime).await,
        Some(false),
        "the hold the mob build staged is released after the unclean stop too"
    );
    assert_eq!(
        customizer.calls.load(Ordering::SeqCst),
        2,
        "the mob build's early publication (no handle yet) and the bootstrap's"
    );
    assert_eq!(capture.count(), 0, "no turn has run yet");
    // Meerkat's resume built the member before the publication, so its tool
    // scope still holds the base set it was built with; the resolved tools
    // are those its next model request carries, which include the published
    // tools.
    assert!(
        resolved_tools(&runtime).await.contains(&TOOL.to_string()),
        "the restored member's resolved tools list its customizer tools before any turn"
    );

    identity_runtime
        .send(
            &id(MEMBER),
            &meerkat_core::ContentInput::Text("after the unclean stop".to_string()),
        )
        .await
        .expect("send boot-2 turn");
    wait_for_turn(&capture, 1, "boot 2's first turn").await;
    assert!(capture.last().expect("captured request").contains(TOOL));
    assert!(resolved_tools(&runtime).await.contains(&TOOL.to_string()));
    // Once the member has run, its tool scope holds the same tools.
    assert_eq!(
        resolved_tools(&runtime).await,
        scope_visible_tools(&runtime).await
    );
    runtime.shutdown().await;
}

/// Set in the child process of
/// [`a_restored_member_the_roster_no_longer_lists_starts_no_run_before_it_is_retired`]:
/// the state directory its first boot runs in.
const DROPPED_CHILD_STATE: &str = "META_CUSTOMIZER_TOOLS_DROPPED_CHILD_STATE";
/// A member the first boot runs and the restart's roster no longer lists.
const DROPPED: &str = "personal:bob";
/// The work [`DroppedMemberProbe`] submits to [`DROPPED`]'s member.
const PROBE: &str = "work for the member the roster no longer lists";
/// How long [`DroppedMemberProbe`] waits for that work to start a run.
const DROPPED_RUN_BOUND: Duration = Duration::from_secs(3);

/// A host customizer that declares [`TOOL`]. On its first build that carries
/// the mob handle (the identity bootstrap's publication for the restored
/// members it owes, which runs before the roster pass) it submits work to the
/// member of [`DROPPED`], which the build revived, and records how the submit
/// went and whether the work started a run within [`DROPPED_RUN_BOUND`].
struct DroppedMemberProbe {
    capture: CaptureClient,
    probe: std::sync::Mutex<Option<(String, bool)>>,
}

#[async_trait]
impl AgentCustomizer for DroppedMemberProbe {
    async fn customize_build(
        &self,
        context: &AgentBuildContext,
        _spec: &DurableAgentSpec,
        draft: &mut AgentBuildDraft,
    ) -> Result<(), CustomizerError> {
        if let Some(handle) = context.runtime_services.mob_handle()
            && self.probe.lock().unwrap().is_none()
        {
            let member_id = meerkat_meta::member_comms_id::mob_member_id(DROPPED);
            let submitted = match handle.member(&member_id).await {
                Ok(member) => match tokio::time::timeout(
                    Duration::from_secs(10),
                    member.send(
                        meerkat_core::ContentInput::Text(PROBE.to_string()),
                        meerkat_core::types::HandlingMode::Queue,
                    ),
                )
                .await
                {
                    Ok(Ok(_)) => "admitted".to_string(),
                    Ok(Err(error)) => format!("refused: {error}"),
                    Err(_) => "unanswered".to_string(),
                },
                Err(error) => format!("no member: {error}"),
            };
            let deadline = Instant::now() + DROPPED_RUN_BOUND;
            while !self.capture.saw(PROBE) && Instant::now() < deadline {
                sleep(Duration::from_millis(50)).await;
            }
            *self.probe.lock().unwrap() = Some((submitted, self.capture.saw(PROBE)));
        }
        draft.local_external_tools = LocalExternalToolOverlay::new(Arc::new(HostTools));
        Ok(())
    }
}

/// After an unclean stop meerkat's resume revives every member the first
/// boot ran while the mob is built, the one whose identity the restart's
/// roster no longer lists as well. Nothing publishes that member's customizer
/// tools: the build holds its run starts, so work submitted to it before the
/// identity bootstrap's roster pass retires it starts no run.
#[tokio::test(flavor = "multi_thread")]
async fn a_restored_member_the_roster_no_longer_lists_starts_no_run_before_it_is_retired() {
    const MOB_ID: &str = "customizer-tools-restart-f";
    if let Some(state) = std::env::var_os(DROPPED_CHILD_STATE) {
        // The child: the first boot runs both members, then exits without
        // shutdown.
        let runtime = boot_listing(
            MOB_ID,
            Path::new(&state),
            CaptureClient::default(),
            Arc::new(ToolCustomizer::default()),
            IdentityBootstrapMode::EagerMaterialize,
            &[MEMBER, DROPPED],
        )
        .await;
        let identity_runtime = runtime.identity_runtime().expect("identity runtime");
        for name in [MEMBER, DROPPED] {
            let status = identity_runtime.status(&id(name)).await.expect("status");
            assert!(status.session_id.is_some(), "{name} runs: {status:?}");
        }
        sleep(Duration::from_millis(500)).await;
        std::process::exit(0);
    }

    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let child = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "a_restored_member_the_roster_no_longer_lists_starts_no_run_before_it_is_retired",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(DROPPED_CHILD_STATE, &state)
        .status()
        .expect("run the first boot in a child process");
    assert!(child.success(), "the child's first boot failed: {child}");

    let capture = CaptureClient::default();
    let probe = Arc::new(DroppedMemberProbe {
        capture: capture.clone(),
        probe: std::sync::Mutex::new(None),
    });
    let runtime = boot_listing(
        MOB_ID,
        &state,
        capture.clone(),
        probe.clone(),
        IdentityBootstrapMode::LazyMaterialize,
        &[MEMBER],
    )
    .await;
    let (submitted, ran) = probe
        .probe
        .lock()
        .unwrap()
        .clone()
        .expect("the identity bootstrap's publication ran the probe before the roster pass");
    assert!(
        !ran,
        "the restored member the roster no longer lists started no run before the roster pass \
         retired it (the work submitted to it: {submitted})"
    );
    let dropped_member = meerkat_meta::member_comms_id::mob_member_id(DROPPED);
    let members = runtime
        .mob_runtime()
        .handle()
        .list_members()
        .await
        .into_iter()
        .map(|member| member.agent_identity.to_string())
        .collect::<Vec<_>>();
    assert!(
        !members.contains(&dropped_member.to_string()),
        "the roster pass retired the member the roster no longer lists: {members:?}"
    );
    sleep(Duration::from_millis(500)).await;
    assert!(
        !capture.saw(PROBE),
        "the work submitted to the retired member never ran"
    );
    runtime.shutdown().await;
}
