//! LazyMaterialize cross-boot recall guard (production field report, 2026-07-29:
//! "silent resume context loss").
//!
//! # The coverage hole this closes
//!
//! Two deterministic guards cover the EAGER bootstrap arm
//! (`identity_first_cold_restart_continuity`,
//! `identity_first_respawn_continuity`) and both are green. Nothing paired the
//! LAZY arm with a recall assertion, and the two arms diverge in
//! `IdentityFirstRuntimeContext::apply_roster_controlled`:
//! `EagerMaterialize` runs `orchestrator::restore_flow` (resume at bootstrap),
//! while `LazyMaterialize | LazyWithBackgroundWarm` run
//! `orchestrator::lazy_register_flow`, which registers every identity as
//! Dormant with its `ContinuityRecord` attached and materializes NOTHING.
//! Materialization is deferred to the first `send`/`dispatch`, which gates on
//! `Dormant | Uninitialized` and calls the shared `embody_identity` door.
//!
//! The field report: boot A takes a turn and the post-turn document lands in
//! the store with correctly-ordered messages; after a kill -9 and boot B every
//! resume signature is clean (0 conflicts, 0 held, 0 quarantined, 0 Broken),
//! yet the agent answers the next message with no record of the earlier
//! exchange. The store is right and the model's working context was empty —
//! completely silent, nothing in logs. So the property to pin is not a status
//! or a store row: it is the BYTES OF THE POST-RESTART LLM REQUEST.
//!
//! # Harness shape (and the trap it avoids)
//!
//! Same trap as `identity_first_head_canonical_resume`: plain
//! `persistent_state()` + `roster_provider()` gives sessions a
//! `SqliteSessionStore` and uses continuity for identity metadata only, which
//! exercises neither the continuity adapter nor the resume read path the
//! deployment runs. `continuity_from_state_dir` installs the substrate as BOTH
//! the identity authority and (via `ContinuitySessionStoreAdapter`) meerkat's
//! `SessionStore` - the gateway/production topology. Every boot therefore probes
//! `assert_durable_continuity_document` before asserting behavior, so a
//! mis-wired harness fails as MIS-WIRED instead of passing while proving
//! nothing.
//!
//! Scope note: a deployment may put a remote whole-blob store behind that same
//! adapter; this runs the local `LocalContinuityStore` behind it. What is held
//! constant is the bootstrap mode and the adapter seam, not the remote store
//! implementation.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::BTreeMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use meerkat_client::{LlmError, LlmEvent, LlmRequest};
use meerkat_core::types::StopReason;
use meerkat_meta::identity_first::contracts::{AgentCustomizer, RosterProvider};
use meerkat_meta::identity_first::{
    AgentAddressability, AgentBuildContext, AgentBuildDraft, AgentIdentity, ContinuityResolveState,
    ContinuitySessionStoreAdapter, ContinuityStore, CustomizerError, DurableAgentSpec,
    FencingToken, IdentityLifecycleState, LocalContinuityStore, RosterContext, RosterError,
};
use meerkat_meta::storage_layout::MetaStorageLayout;
use meerkat_meta::{IdentityBootstrapMode, UnifiedRuntimeBuilder};
use meerkat_mob::{MobDefinition, ProfileName};
use tokio::time::sleep;

/// The one definition of the normalized-provider-accounting contract every
/// Meta LLM double must satisfy under meerkat 0.8.22. See the module docs.
#[path = "support/llm_usage.rs"]
mod llm_usage;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const MOB_TOML: &str = r#"
[mob]
id = "lazy-recall-continuity"

[profiles.personal]
model = "gpt-5.5"
external_addressable = true
runtime_mode = "turn_driven"

[profiles.personal.tools]
comms = true
"#;

const MEMBER: &str = "personal:alice";
const RUNTIME_INSTANCE: &str = "lazy-recall-continuity";

/// Serializes this binary's tests: in the parent one memo-free child runs at a
/// time, and inside a child its single test owns the process.
static SERIAL_WINDOW: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Set on the re-exec'd child so it runs the test body instead of re-execing
/// again.
const MEMO_FREE_CHILD: &str = "MEERKAT_LAZY_RECALL_MEMO_FREE_CHILD";

/// Run this test in a child process with meerkat's PROCESS-GLOBAL memos
/// disabled; returns `true` in the parent (which has just proxied the whole
/// test) and `false` in the child (which must run the body).
///
/// # Why every test here needs it
///
/// Integration tests in one binary share one process, and meerkat keeps memos
/// that outlive a runtime: the transcript-graph decode memo, the digest
/// accumulator's memo, and the slim-materialization snapshot registry that
/// `SessionHead::from_session` seeds on the PRODUCER side of a save (see
/// `identity_first_subprocess_reboot.rs`, which exists because of them). Any
/// of those can hand a later same-process "boot" a decoded, digested or fully
/// materialized transcript that a real `execve` could never inherit —
/// precisely how a legacy-decode or crash-recovery defect hides behind a green
/// same-process test. `MEERKAT_DISABLE_GRAPH_DECODE_MEMO` is the established
/// kill switch for all of them.
///
/// It cannot be set in-process: `unsafe_code` is FORBIDDEN workspace-wide and
/// `std::env::set_var` is unsafe on edition 2024. So this follows the sibling
/// subprocess test's idiom — put it in a CHILD's environment — by re-execing
/// this test binary for the one named test. The child's output is forwarded, so
/// a failure reads exactly as it would have in-process.
fn proxied_to_memo_free_child(test_name: &str) -> bool {
    if std::env::var_os(MEMO_FREE_CHILD).is_some() {
        return false;
    }
    let exe = std::env::current_exe().expect("this test binary's own path");
    let output = std::process::Command::new(exe)
        // `--include-ignored` matters: without it a child running an
        // `#[ignore]`d test would filter it out and exit 0, and the parent
        // would report a PASS for a test that never ran.
        .args([
            test_name,
            "--exact",
            "--nocapture",
            "--test-threads=1",
            "--include-ignored",
        ])
        .env(MEMO_FREE_CHILD, "1")
        .env("MEERKAT_DISABLE_GRAPH_DECODE_MEMO", "1")
        .output()
        .expect("spawn the memo-free child");
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    assert!(
        output.status.success(),
        "the memo-free child running `{test_name}` failed ({}); its output is above",
        output.status
    );
    true
}

/// A system prompt only the customizer can install: the profile has none, so
/// its presence in a captured request means `customize_build` ran for that
/// member's build.
const CUSTOMIZER_PROMPT: &str = "You are alice. CUSTOMIZER-PROMPT-MARKER-4-QUEBEC.";

fn id(name: &str) -> AgentIdentity {
    AgentIdentity::parse(name).expect("parse identity")
}

fn definition() -> MobDefinition {
    MobDefinition::from_toml(MOB_TOML).expect("parse lazy-recall mob definition")
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

/// Installs a marker system prompt at build time.
///
/// On the lazy arm the customizer is not passed to `lazy_register_flow` at
/// all; `embody_identity` reads it from runtime state (installed by the
/// builder via `set_agent_customizer`) when the first send materializes the
/// member. A lazily materialized agent losing its host prompt would be a
/// second, independent defect, so both boots assert the marker.
struct MarkerPromptCustomizer;

#[async_trait]
impl AgentCustomizer for MarkerPromptCustomizer {
    async fn customize_build(
        &self,
        _context: &AgentBuildContext,
        _spec: &DurableAgentSpec,
        draft: &mut AgentBuildDraft,
    ) -> Result<(), CustomizerError> {
        draft.system_prompt = Some(CUSTOMIZER_PROMPT.to_string());
        Ok(())
    }
}

/// The raw token counts one [`CaptureClient`] turn reports.
///
/// meerkat 0.8.22 made a turn that reports NO usage fatal, and silently so at
/// compile time: the adapter seeds its local usage with `Usage::default()`
/// (no accounting) and `commit_calling_llm_response` then runs
/// `TurnUsage::try_from_usage` on whatever the stream left there, failing the
/// turn with `normalized_provider_accounting_unavailable`. Under 0.8.21 that
/// cost nothing here - emission was guarded on `usage_input_tokens > 0`,
/// which is never raised above its zero default in this file, and the
/// tool-call branch emitted no usage at all, so in practice NO turn here
/// reported usage. A type-only port would therefore have compiled and then
/// failed EVERY turn, surfacing as a provider error nowhere near anything
/// that mentions usage. Hence every completing turn now reports, whatever the
/// count. Declaring zero input tokens preserves the 0.8.21 value and stays
/// inert for compaction, which documents "no compaction when
/// `last_input_tokens` is zero".
fn capture_usage(input_tokens: u64) -> meerkat_core::types::Usage {
    meerkat_core::types::Usage {
        input_tokens,
        output_tokens: 1,
        ..Default::default()
    }
}

/// Records the serialized LLM request each turn and answers "ok" so turns
/// complete without a real provider.
///
/// `hang` turns it into a mid-turn stall: the request is still recorded, but
/// the stream then waits for [`CaptureClient::release`] instead of answering,
/// so the turn stays IN FLIGHT and no boundary save can land.
#[derive(Clone)]
struct CaptureClient {
    requests: Arc<std::sync::Mutex<Vec<String>>>,
    hang: Arc<AtomicBool>,
    /// Consumed by the first hanging turn, which spends it on a TOOL ROUND
    /// TRIP so the turn has in-flight work (and therefore durable intra-turn
    /// state) before it dies.
    tool_step: Arc<AtomicBool>,
    gate: Arc<tokio::sync::Semaphore>,
    /// Provider-reported input tokens. `last_input_tokens` (the compaction
    /// trigger, `compact.rs`: "compaction triggers when `last_input_tokens >=
    /// auto_compact_threshold`") comes from exactly this, so raising it would
    /// make compaction fire deterministically instead of depending on
    /// transcript size. No test in this file raises it: it stays at zero, which
    /// is the "never compact" arm of that same rule. The turn still REPORTS
    /// that zero - see [`capture_usage`] for why reporting nothing is no
    /// longer an option.
    usage_input_tokens: Arc<std::sync::atomic::AtomicU64>,
}

impl Default for CaptureClient {
    fn default() -> Self {
        Self {
            requests: Arc::new(std::sync::Mutex::new(Vec::new())),
            hang: Arc::new(AtomicBool::new(false)),
            tool_step: Arc::new(AtomicBool::new(false)),
            // Starts closed; only a hanging stream ever waits on it.
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
            usage_input_tokens: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

impl CaptureClient {
    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
    fn last(&self) -> Option<String> {
        self.requests.lock().unwrap().last().cloned()
    }
    /// Stall the next turn: it first spends one tool round trip (in-flight
    /// work), then hangs on its follow-up LLM call.
    fn hang_turns(&self) {
        self.tool_step.store(true, Ordering::SeqCst);
        self.hang.store(true, Ordering::SeqCst);
    }
    /// Let stalled turns (and any later one) complete, so the runtime can be
    /// shut down cleanly instead of being left wedged for the rest of the
    /// binary.
    fn release(&self) {
        self.hang.store(false, Ordering::SeqCst);
        self.gate
            .add_permits(tokio::sync::Semaphore::MAX_PERMITS >> 4);
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
        let hang = self.hang.load(Ordering::SeqCst);
        // One tool round trip before the stall: a turn that dies with NO
        // in-flight work has nothing durable to leave behind, and a mid-turn
        // kill would then be indistinguishable from a pre-turn one.
        let tool_step = hang && self.tool_step.swap(false, Ordering::SeqCst);
        let gate = self.gate.clone();
        let usage_input_tokens = self.usage_input_tokens.load(Ordering::SeqCst);
        // meerkat 0.8.22 rejects a turn whose stream carried no normalized
        // provider accounting, so the terminal `Done` never travels alone -
        // and that includes the tool-call turn, whose only outcome is a tool
        // call. Both pairs are built out here, against `request`, so the
        // stream body never borrows it.
        //
        // `Provider::OpenAI` is passed as the CLIENT's own declaration, not as
        // the answer: the helper resolves the accounting provider from the
        // MODEL through the same catalog authority `AgentFactory` used, and
        // falls back to the declaration only for an uncatalogued model. That
        // distinction is load-bearing here, because `provider()` below never
        // reaches the factory at all - Meta wraps the mob-wide default
        // client in `ProviderAgnosticLlmClient`, which reports
        // `Provider::Other` precisely so one stub can serve members across
        // providers. A hardcoded `OpenAI` would bind fine and then fail every
        // turn with `normalized_provider_accounting_identity_mismatch` the day
        // a profile in this file moves to a `claude-*` model.
        let [tool_usage, tool_done] = llm_usage::usage_then_done_with(
            request,
            meerkat::Provider::OpenAI,
            capture_usage(usage_input_tokens),
            StopReason::ToolUse,
        );
        let [turn_usage, turn_done] = llm_usage::usage_then_done_with(
            request,
            meerkat::Provider::OpenAI,
            capture_usage(usage_input_tokens),
            StopReason::EndTurn,
        );
        Box::pin(async_stream::stream! {
            if tool_step {
                yield Ok(LlmEvent::ToolCallComplete {
                    id: "midturn-peers-1".to_string(),
                    name: "peers".to_string(),
                    args: serde_json::json!({}),
                    meta: None,
                });
                yield Ok(tool_usage);
                yield Ok(tool_done);
                return;
            }
            if hang {
                // Mid-turn stall: the request is already recorded (so the
                // barrier can see the turn started) and the turn never reaches
                // its boundary save. Nothing at all is emitted while stalled -
                // usage included - because a stalled turn has no boundary to
                // account for; `release` then lets it finish through the tail
                // below like any other turn.
                let _permit = gate.acquire().await;
            }
            yield Ok(LlmEvent::TextDelta { delta: "ok".to_string(), meta: None });
            yield Ok(turn_usage);
            yield Ok(turn_done);
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

// ---------------------------------------------------------------------------
// Gateway-shaped harness
// ---------------------------------------------------------------------------

/// Build a runtime whose session I/O rides the continuity adapter (as
/// `rpc_gateway` composes it) under the requested bootstrap mode.
async fn boot(
    state: &Path,
    capture: CaptureClient,
    mode: IdentityBootstrapMode,
) -> meerkat_meta::UnifiedRuntime {
    boot_with_compaction(state, capture, mode, None).await
}

/// `boot`, optionally with a host compaction policy. Compaction is how a
/// document acquires a transcript graph at all (it is the rewrite the legacy
/// fixture below needs), so the axis that needs one declares it explicitly.
async fn boot_with_compaction(
    state: &Path,
    capture: CaptureClient,
    mode: IdentityBootstrapMode,
    compaction: Option<meerkat_core::config::CompactionRuntimeConfig>,
) -> meerkat_meta::UnifiedRuntime {
    let mut builder = UnifiedRuntimeBuilder::default()
        .definition(definition())
        .persistent_state(state)
        .continuity_from_state_dir(state)
        .await
        .expect("open the state-dir identity substrate")
        .roster_provider(Arc::new(OneMemberRoster))
        .agent_customizer(Arc::new(MarkerPromptCustomizer))
        .identity_bootstrap_mode(mode)
        .identity_runtime_instance_id(RUNTIME_INSTANCE)
        .comms(true)
        .default_llm_client(Arc::new(capture));
    if let Some(compaction) = compaction {
        builder = builder.compaction(compaction);
    }
    Box::pin(builder.build())
        .await
        .expect("build the gateway-shaped UnifiedRuntime")
}

fn continuity_db(state: &Path) -> std::path::PathBuf {
    MetaStorageLayout::with_injected_roots(state.to_path_buf(), None)
        .continuity_db()
        .expect("resolve continuity db")
        .path
}

/// The marker carried by [`PROFILE_PROMPT_MOB_TOML`]'s inline skill. Its
/// presence in a captured request proves the profile-level prompt carrier is
/// real, so the revival assertions rest on a prompt that exists.
const PROFILE_PROMPT_MARKER: &str = "PROFILE-PROMPT-MARKER-16-YANKEE";

/// The lazy-recall mob with a PROFILE-declared system prompt - the one
/// configuration shape proven (field contrast, 2026-08-06) to
/// re-author one assembled System row per automatic revival on the
/// 0.8.16-0.8.18 line: the assembled prompt lands in persisted spawn state
/// at original spawn and the mob-resume AppendExplicit branches re-lower it
/// as fresh explicit intent on every boot.
///
/// The prompt rides the only profile-level carrier meerkat reads: `skills`
/// resolved against an inline `[skills.<id>]`. Until 2026-09-03 this fixture
/// wrote a bare `system_prompt` key under `[profiles.personal]`, which is not
/// a `Profile` field and is dropped at `MobDefinition::from_toml`, so no
/// prompt was in play and the "authors no extra rows" assertions were
/// vacuous.
const PROFILE_PROMPT_MOB_TOML: &str = r#"
[mob]
id = "lazy-recall-continuity"

[profiles.personal]
model = "gpt-5.5"
external_addressable = true
runtime_mode = "turn_driven"
skills = ["personal_role"]

[profiles.personal.tools]
comms = true

[skills.personal_role]
source = "inline"
content = "PROFILE-PROMPT-MARKER-16-YANKEE: you are alice."
"#;

/// `boot` with an explicit definition and NO agent customizer, so a
/// profile-declared prompt is the only prompt source in play.
async fn boot_with_definition_no_customizer(
    state: &Path,
    capture: CaptureClient,
    mode: IdentityBootstrapMode,
    definition: MobDefinition,
) -> meerkat_meta::UnifiedRuntime {
    let builder = UnifiedRuntimeBuilder::default()
        .definition(definition)
        .persistent_state(state)
        .continuity_from_state_dir(state)
        .await
        .expect("open the state-dir identity substrate")
        .roster_provider(Arc::new(OneMemberRoster))
        .identity_bootstrap_mode(mode)
        .identity_runtime_instance_id(RUNTIME_INSTANCE)
        .comms(true)
        .default_llm_client(Arc::new(capture));
    Box::pin(builder.build())
        .await
        .expect("build the gateway-shaped UnifiedRuntime")
}

// ---------------------------------------------------------------------------
// Precondition probe
// ---------------------------------------------------------------------------

/// What the continuity file durably holds for one boot.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ContinuityCensus {
    /// `continuity_session_heads` rows: (session_id, message_count).
    heads: Vec<(String, i64)>,
    /// `continuity_strand_messages` row count. Rows can legally exist BEYOND
    /// the head's `message_count`: that is the crash window between
    /// `append_messages` and the head write that adopts them (named in
    /// `local_store.rs`'s schema comment), i.e. the mid-turn shape.
    strand_messages: i64,
    /// `continuity_session_rewrites` row count — ADOPTED transcript rewrite
    /// commits. Zero means the document has no transcript graph at all.
    rewrites: i64,
    /// `session_snapshots` rows (the whole-blob representation).
    snapshots: Vec<String>,
    /// `continuity_records` rows: (identity, session_id).
    records: Vec<(String, String)>,
}

fn table_exists(conn: &rusqlite::Connection, table: &str) -> bool {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
        rusqlite::params![table],
        |row| row.get::<_, i64>(0),
    )
    .unwrap_or(0)
        == 1
}

/// Read-only census of the continuity file. Opened as an ordinary read/write
/// path deliberately: `?immutable=1` ignores the `-wal` sidecar (and silently
/// manufactures a 0-byte file at a missing path), which is exactly how a probe
/// lies about durability.
fn census(db: &Path) -> ContinuityCensus {
    assert!(
        db.exists(),
        "continuity file {} does not exist — the runtime never opened it",
        db.display()
    );
    let conn = rusqlite::Connection::open(db).expect("open continuity db");
    // Polled while the runtime is LIVE; tolerate a briefly-locked writer
    // instead of panicking on SQLITE_BUSY.
    conn.busy_timeout(Duration::from_secs(5))
        .expect("set continuity census busy timeout");
    let mut out = ContinuityCensus::default();

    if table_exists(&conn, "continuity_session_heads") {
        let mut stmt = conn
            .prepare("SELECT session_id, message_count FROM continuity_session_heads ORDER BY 1")
            .expect("prepare heads");
        out.heads = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .expect("query heads")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect heads");
    }
    if table_exists(&conn, "continuity_strand_messages") {
        out.strand_messages = conn
            .query_row(
                "SELECT COUNT(*) FROM continuity_strand_messages",
                [],
                |row| row.get(0),
            )
            .expect("count strand messages");
    }
    if table_exists(&conn, "continuity_session_rewrites") {
        out.rewrites = conn
            .query_row(
                "SELECT COUNT(*) FROM continuity_session_rewrites",
                [],
                |row| row.get(0),
            )
            .expect("count rewrite commits");
    }
    if table_exists(&conn, "session_snapshots") {
        let mut stmt = conn
            .prepare("SELECT session_id FROM session_snapshots ORDER BY 1")
            .expect("prepare snapshots");
        out.snapshots = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query snapshots")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect snapshots");
    }
    if table_exists(&conn, "continuity_records") {
        let mut stmt = conn
            .prepare("SELECT identity, session_id FROM continuity_records ORDER BY 1")
            .expect("prepare records");
        out.records = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .expect("query records")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect records");
    }
    out
}

fn sibling_files(state: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(state)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .map(|entry| {
                    let len = entry.metadata().map(|m| m.len()).unwrap_or_default();
                    format!("{}({len}B)", entry.file_name().to_string_lossy())
                })
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// THE PRECONDITION. Fails the test as MIS-WIRED (not as a behavior
/// regression) unless the continuity file holds a durable session document —
/// a `continuity_session_heads` row (head-canonical) or a `session_snapshots`
/// row (whole-blob). Returns the census so callers can assert on shape.
fn assert_durable_continuity_document(state: &Path, phase: &str) -> ContinuityCensus {
    let db = continuity_db(state);
    let census = census(&db);
    assert!(
        !census.heads.is_empty() || !census.snapshots.is_empty(),
        "PRECONDITION FAILED ({phase}): {} holds NO durable session document \
         (continuity_session_heads: 0 rows, session_snapshots: 0 rows). The harness is \
         MIS-WIRED — session I/O is not going through the continuity adapter, so nothing \
         asserted after this point is meaningful. Census: {census:?}. Sibling files: {:?}",
        db.display(),
        sibling_files(state),
    );
    census
}

async fn wait_for_turn(capture: &CaptureClient, want: usize, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while capture.count() < want {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what} to reach the LLM"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

/// Deterministic turn-boundary barrier: wait until the continuity file holds
/// exactly ONE durable session document with at least `floor` messages,
/// returning `(session_id, observed_count)`. The row is written by the same
/// boundary save that persists the transcript, so once it shows the growth the
/// turn is durable and the restart below cannot race it.
///
/// Both per-session canonical representations count (`local_store.rs`'s
/// canonical-representation rule): a `continuity_session_heads` row carries
/// its count directly (registered sessions birth head-canonical on their
/// first committed-boundary projection); a whole-blob `session_snapshots`
/// row - unregistered sessions and substrates without the incremental
/// channel - is decoded to count its messages.
async fn wait_for_durable_document_at_least(state: &Path, floor: i64, what: &str) -> (String, i64) {
    let db = continuity_db(state);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let census = census(&db);
        if let [(session, count)] = census.heads.as_slice()
            && *count >= floor
        {
            return (session.clone(), *count);
        }
        if census.heads.is_empty() {
            let mut ids = census.snapshots.clone();
            ids.dedup(); // census orders by session_id
            if let [session] = ids.as_slice() {
                let conn = rusqlite::Connection::open(&db).expect("open continuity db");
                conn.busy_timeout(Duration::from_secs(5))
                    .expect("set snapshot decode busy timeout");
                let data: Vec<u8> = conn
                    .query_row(
                        "SELECT data FROM session_snapshots WHERE session_id = ?1 \
                         ORDER BY generation DESC, checkpoint_version DESC LIMIT 1",
                        rusqlite::params![session],
                        |row| row.get(0),
                    )
                    .expect("read snapshot row");
                if let Ok(decoded) = meerkat_core::Session::from_persisted_bytes(&data) {
                    let count = i64::try_from(decoded.messages().len()).unwrap_or(0);
                    if count >= floor {
                        return (session.clone(), count);
                    }
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}: want one durable session document with >= {floor} \
             messages, census: {census:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

// ---------------------------------------------------------------------------
// Axis helpers: crash images and pre-0.8.10 document encoding
// ---------------------------------------------------------------------------

/// Copy a whole state directory (recursively, every SQLite sidecar included).
///
/// Taken mid-turn this is a CRASH IMAGE: the same on-disk state a `kill -9`
/// leaves behind, `-wal`/`-shm` and all, with no clean-shutdown flush applied.
/// SQLite recovers it on the next open exactly as it would after a kill.
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create image dir");
    let mut copied = 0_usize;
    for entry in std::fs::read_dir(from).expect("read state dir") {
        let entry = entry.expect("state dir entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy state file");
        }
        copied += 1;
    }
    assert!(
        copied > 0,
        "state dir {} was empty — the crash image would be vacuous",
        from.display()
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The production report, deterministically: with `LazyMaterialize`, a restart that
/// materializes the member ON FIRST SEND must replay the persisted transcript
/// into the LLM request.
///
/// Both boots prove the lazy arm was genuinely taken rather than silently
/// falling back to eager restore: the bootstrap pass reports the member
/// Dormant (`ready == false`, `counts.dormant == 1`) with no session yet, and
/// the identity only becomes Active AFTER the send. Without those assertions a
/// green run could be the eager path wearing a lazy label, proving nothing.
#[tokio::test(flavor = "multi_thread")]
async fn lazy_materialize_cross_boot_recall_replays_the_transcript() {
    const TOKEN: &str = "MARKER-LAZY-RECALL-3-XRAY";
    if proxied_to_memo_free_child("lazy_materialize_cross_boot_recall_replays_the_transcript") {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let member = id(MEMBER);

    // --- Boot 1: lazy register, then materialize on demand from turn 1 ---
    let session_id;
    {
        let capture = CaptureClient::default();
        let runtime = boot(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();

        let bootstrap = identity_runtime.identity_bootstrap_status();
        eprintln!("PROBE boot 1 bootstrap: {bootstrap:?}");
        assert_eq!(
            bootstrap.mode,
            IdentityBootstrapMode::LazyMaterialize,
            "the runtime must be running the lazy arm; bootstrap: {bootstrap:?}"
        );
        assert!(
            bootstrap.complete,
            "the lazy bootstrap pass must have finished; bootstrap: {bootstrap:?}"
        );
        assert!(
            !bootstrap.ready,
            "lazy bootstrap must leave the roster UNmaterialized (ready == false); \
             bootstrap: {bootstrap:?}"
        );
        assert_eq!(
            (bootstrap.counts.dormant, bootstrap.counts.active),
            (1, 0),
            "lazy bootstrap must register the member dormant and materialize nothing; \
             bootstrap: {bootstrap:?}"
        );
        let before = identity_runtime
            .status(&member)
            .await
            .expect("dormant identity is inspectable");
        assert_eq!(
            before.state,
            IdentityLifecycleState::Dormant,
            "boot 1 must start Dormant, not eagerly restored; status: {before:?}"
        );

        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text(format!("Please note this token: {TOKEN}")),
            )
            .await
            .expect("send turn 1");
        wait_for_turn(&capture, 1, "boot 1's turn").await;

        let after = identity_runtime
            .status(&member)
            .await
            .expect("status after the first send");
        assert_eq!(
            after.state,
            IdentityLifecycleState::Active,
            "the FIRST SEND is what must have materialized the member (on-demand \
             materialization from Dormant); status: {after:?}"
        );
        session_id = after
            .session_id
            .as_ref()
            .expect("a materialized identity owns a durable session")
            .to_string();

        // The customizer must reach a freshly materialized member, otherwise
        // the post-restart prompt assertion below would be vacuous.
        let first = capture.last().expect("a boot-1 request was captured");
        assert!(
            first.contains(CUSTOMIZER_PROMPT),
            "the customizer's system prompt must reach the lazily materialized member's \
             first request; request: {first}"
        );

        let (head, count) =
            wait_for_durable_document_at_least(&state, 2, "boot 1's turn to commit").await;
        eprintln!("PROBE boot 1 durable head: {head} at {count} messages");
        assert_eq!(
            head, session_id,
            "the durable document must belong to the materialized session"
        );
        runtime.shutdown().await;
    }

    let census = assert_durable_continuity_document(&state, "boot 1 + one turn");
    eprintln!("PROBE census after boot 1: {census:?}");
    assert_eq!(
        census.records.len(),
        1,
        "one identity must own one continuity record; census: {census:?}"
    );
    assert_eq!(
        census.records[0].1, session_id,
        "the continuity record must name the session the turn was written to; \
         census: {census:?}"
    );

    // --- Boot 2: lazy again. First send must materialize AND recall. ---
    {
        let capture = CaptureClient::default(); // fresh: only boot-2 requests
        let runtime = boot(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();

        let pre = assert_durable_continuity_document(&state, "boot 2 (pre-turn)");
        assert_eq!(
            pre.records.first().map(|(_, session)| session.clone()),
            Some(session_id.clone()),
            "boot 2 must still bind the identity to the boot-1 session; census: {pre:?}"
        );

        let bootstrap = identity_runtime.identity_bootstrap_status();
        eprintln!("PROBE boot 2 bootstrap: {bootstrap:?}");
        assert_eq!(
            (bootstrap.counts.dormant, bootstrap.counts.active),
            (1, 0),
            "boot 2 must ALSO take the lazy arm (dormant registration, nothing \
             materialized) — otherwise this test silently exercises eager restore; \
             bootstrap: {bootstrap:?}"
        );
        let before = identity_runtime
            .status(&member)
            .await
            .expect("dormant identity is inspectable after restart");
        assert_eq!(
            before.state,
            IdentityLifecycleState::Dormant,
            "boot 2 must start Dormant with its continuity record attached; status: {before:?}"
        );

        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("What token did I give you earlier?".to_string()),
            )
            .await
            .expect("send turn 2");
        wait_for_turn(&capture, 1, "the post-restart turn").await;

        let last = capture.last().expect("a post-restart request was captured");
        // THE PROPERTY THE FIELD REPORT FOUND VIOLATED. The store document is
        // correct in the field; what was empty was the model's working
        // context, so this asserts the REQUEST BYTES, not a status or a row.
        assert!(
            last.contains(TOKEN),
            "post-restart LLM request must replay the persisted transcript (token {TOKEN}); \
             on-demand materialization served an empty or stale document. Durable census: \
             {pre:?}. Request: {last}"
        );
        // Independent second property: resume AUTHORS NOTHING (the bridge
        // clears the prompt override; meerkat preserves every ordered System
        // byte-for-byte) — so this pins that the host prompt survives a lazy
        // restart purely as preserved durable transcript, not by any
        // boot-time re-application.
        assert!(
            last.contains(CUSTOMIZER_PROMPT),
            "post-restart LLM request must still carry the host system prompt; \
             request: {last}"
        );

        let after = identity_runtime
            .status(&member)
            .await
            .expect("status after the post-restart send");
        assert_eq!(
            after.state,
            IdentityLifecycleState::Active,
            "the post-restart send must have materialized the member; status: {after:?}"
        );
        assert_eq!(
            after.session_id.as_ref().map(ToString::to_string),
            Some(session_id.clone()),
            "on-demand materialization must RESUME the durable session, not create a \
             fresh one; status: {after:?}"
        );
        runtime.shutdown().await;
    }
}

/// Same property for `LazyWithBackgroundWarm`, which shares the
/// `lazy_register_flow` match arm and then materializes from the background
/// warmer instead of from the first send. Cheap to cover and a different
/// materialization trigger over the same dormant registration.
#[tokio::test(flavor = "multi_thread")]
async fn lazy_with_background_warm_cross_boot_recall_replays_the_transcript() {
    const TOKEN: &str = "MARKER-LAZY-WARM-8-SIERRA";
    if proxied_to_memo_free_child(
        "lazy_with_background_warm_cross_boot_recall_replays_the_transcript",
    ) {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    let mode = IdentityBootstrapMode::LazyWithBackgroundWarm { concurrency: 1 };
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let member = id(MEMBER);

    // --- Boot 1: background warm materializes, then turn 1 ---
    let session_id;
    {
        let capture = CaptureClient::default();
        let runtime = boot(&state, capture.clone(), mode.clone()).await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();

        let (bootstrap, timed_out) = identity_runtime
            .wait_identity_bootstrap_terminal(Duration::from_secs(30))
            .await;
        eprintln!("PROBE warm boot 1 bootstrap: {bootstrap:?}");
        assert!(
            !timed_out,
            "background warm must reach a terminal state; bootstrap: {bootstrap:?}"
        );
        assert_eq!(bootstrap.mode, mode, "bootstrap: {bootstrap:?}");
        assert_eq!(
            (bootstrap.counts.active, bootstrap.counts.broken),
            (1, 0),
            "the warmer must materialize the roster it registered dormant; \
             bootstrap: {bootstrap:?}"
        );

        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text(format!("Please note this token: {TOKEN}")),
            )
            .await
            .expect("send turn 1");
        wait_for_turn(&capture, 1, "warm boot 1's turn").await;

        let first = capture.last().expect("a warm boot-1 request was captured");
        assert!(
            first.contains(CUSTOMIZER_PROMPT),
            "the customizer's system prompt must reach the background-warmed member; \
             request: {first}"
        );

        let (head, count) =
            wait_for_durable_document_at_least(&state, 2, "warm boot 1's turn to commit").await;
        eprintln!("PROBE warm boot 1 durable head: {head} at {count} messages");
        session_id = head;
        runtime.shutdown().await;
    }

    // --- Boot 2: warm again, then a turn that must recall turn 1 ---
    {
        let capture = CaptureClient::default(); // fresh: only boot-2 requests
        let runtime = boot(&state, capture.clone(), mode.clone()).await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();

        let pre = assert_durable_continuity_document(&state, "warm boot 2 (pre-turn)");
        assert_eq!(
            pre.records.first().map(|(_, session)| session.clone()),
            Some(session_id.clone()),
            "warm boot 2 must still bind the identity to the boot-1 session; census: {pre:?}"
        );

        let (bootstrap, timed_out) = identity_runtime
            .wait_identity_bootstrap_terminal(Duration::from_secs(30))
            .await;
        eprintln!("PROBE warm boot 2 bootstrap: {bootstrap:?}");
        assert!(
            !timed_out,
            "background warm must reach a terminal state after a restart; \
             bootstrap: {bootstrap:?}"
        );
        assert_eq!(
            (bootstrap.counts.active, bootstrap.counts.broken),
            (1, 0),
            "the warmer must re-materialize the persisted member; bootstrap: {bootstrap:?}"
        );

        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("What token did I give you earlier?".to_string()),
            )
            .await
            .expect("send turn 2");
        wait_for_turn(&capture, 1, "the post-restart warm turn").await;

        let last = capture
            .last()
            .expect("a post-restart warm request was captured");
        assert!(
            last.contains(TOKEN),
            "post-restart LLM request must replay the persisted transcript (token {TOKEN}); \
             background-warm materialization served an empty or stale document. Durable \
             census: {pre:?}. Request: {last}"
        );
        assert!(
            last.contains(CUSTOMIZER_PROMPT),
            "post-restart warm request must still carry the host system prompt; \
             request: {last}"
        );

        let after = identity_runtime
            .status(&member)
            .await
            .expect("status after the post-restart warm send");
        assert_eq!(
            after.session_id.as_ref().map(ToString::to_string),
            Some(session_id.clone()),
            "background-warm materialization must RESUME the durable session; \
             status: {after:?}"
        );
        runtime.shutdown().await;
    }
}

/// The durable mid-turn shape a crash image was taken over.
///
/// Read through `Debug` in the PROBE line and in the silent-amnesia failure
/// message; the fields exist to make that report specific.
#[derive(Debug)]
#[allow(dead_code)]
struct MidTurnState {
    head_messages: i64,
    strand_messages: i64,
    shape: &'static str,
}

/// Barrier: wait until the in-flight turn has left DURABLE state behind, so
/// the crash image below is genuinely mid-turn.
///
/// Two legal shapes, both mid-turn: strand rows past the head (the crash
/// window between `append_messages` and the head write that adopts them,
/// named in `local_store.rs`'s schema comment), or a head that advanced past
/// the last committed boundary. If NEITHER appears, a mid-turn kill is
/// indistinguishable from a pre-turn kill on this path and the axis cannot
/// arm — which this fails loudly rather than passing vacuously.
async fn wait_for_mid_turn_durable_state(state: &Path, committed: i64, what: &str) -> MidTurnState {
    let db = continuity_db(state);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let census = census(&db);
        let head_messages = census.heads.first().map(|(_, count)| *count).unwrap_or(0);
        if census.strand_messages > head_messages {
            return MidTurnState {
                head_messages,
                strand_messages: census.strand_messages,
                shape: "uncommitted strand rows beyond the head",
            };
        }
        if head_messages > committed {
            return MidTurnState {
                head_messages,
                strand_messages: census.strand_messages,
                shape: "head advanced mid-turn (user message committed, reply not)",
            };
        }
        assert!(
            Instant::now() < deadline,
            "MID-TURN FIXTURE DID NOT ARM waiting for {what}: no durable mid-turn state appeared \
             (committed head was {committed}, census: {census:?}). A mid-turn kill would be \
             indistinguishable from a pre-turn one, so this axis cannot be asserted honestly."
        );
        sleep(Duration::from_millis(100)).await;
    }
}

/// AXIS 2 — a kill that lands MID-TURN, with in-flight work.
///
/// The field's other failing case. Turn 2 is stalled inside the LLM call, so
/// its boundary save never lands, and the state directory is copied in that
/// window: `-wal`/`-shm` included, no clean-shutdown flush — the on-disk state
/// a `kill -9` leaves. Boot 2 runs against that image.
///
/// The contract asserted is deliberately two-sided, because recovery has two
/// legitimate answers and one illegitimate one:
///
/// - LOUD is fine: a typed rejection / degraded identity ("the only durable
///   checkpoint is an intra-turn projection" — forbidden as an authority base,
///   `checkpoint.rs`) tells the operator recovery needs attention. Two of the
///   field's three identities did exactly this.
/// - RECALL is fine: the send is accepted and replays the pre-crash
///   transcript.
/// - SILENT AMNESIA is not: an accepted turn whose request carries none of the
///   prior transcript, which then saves cleanly over the durable head. That is
///   the shape the deployment observed mid-cycle, and the store looks perfect
///   afterwards precisely because the save re-merges — so only the request
///   bytes can tell the two apart.
// The fixture cannot yet be armed, and the reason is a finding in itself.
//
// MEASURED: with the turn stalled inside the LLM call — and stalled AFTER a
// completed `peers` tool round trip, so the turn had in-flight work behind it
// — the continuity file did not move for 20s: head 3 messages, 3 strand rows,
// 0 rewrite commits, i.e. exactly the pre-turn boundary. On this substrate
// NOTHING between a turn's admission and its boundary save is durable: not the
// user message, not the assistant tool-call message, not the tool result. A
// mid-turn kill is therefore byte-identical to a pre-turn kill, so a resume
// after one reads an intact committed boundary and this axis cannot be
// asserted honestly (the barrier fails loudly rather than certify that).
//
// WHERE THE FIELD'S SHAPE MUST COME FROM INSTEAD: the intra-turn projection
// stamp ("the only durable checkpoint is an intra-turn projection", forbidden
// as an authority base by `checkpoint.rs`) is minted in exactly one place —
// `meerkat-session/src/persistent.rs:1899` and `:1932`, the head bridging
// inside a transcript-REWRITE sequence. So the field state implicates a crash
// during a COMPACTION rewrite, not a crash during an ordinary turn. Arming
// this axis needs a kill inside that multi-step rewrite persist (or a
// whole-blob substrate that checkpoints mid-turn), which the in-process LLM
// seam cannot reach; the subprocess harness
// (identity_first_subprocess_reboot.rs) can land a real kill -9 mid-persist.
// Un-ignore once armed; the loud-or-recalls assertion is ready.
#[ignore = "fixture arming: no durable mid-turn write exists to crash over (see the note above)"]
#[tokio::test(flavor = "multi_thread")]
async fn lazy_resume_after_a_mid_turn_kill_is_loud_or_recalls_never_silently_empty() {
    const TOKEN: &str = "MARKER-MIDTURN-KILL-6-WHISKEY";
    if proxied_to_memo_free_child(
        "lazy_resume_after_a_mid_turn_kill_is_loud_or_recalls_never_silently_empty",
    ) {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let live = temp.path().join("live");
    let image = temp.path().join("crash-image");
    let member = id(MEMBER);

    // --- Boot 1: one COMPLETE turn — the history a mid-turn crash can lose ---
    let capture = CaptureClient::default();
    let runtime = boot(
        &live,
        capture.clone(),
        IdentityBootstrapMode::LazyMaterialize,
    )
    .await;
    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime")
        .clone();
    identity_runtime
        .send(
            &member,
            &meerkat_core::ContentInput::Text(format!("Please note this token: {TOKEN}")),
        )
        .await
        .expect("send turn 1");
    wait_for_turn(&capture, 1, "turn 1").await;
    let (session_id, committed) =
        wait_for_durable_document_at_least(&live, 2, "turn 1 to commit").await;
    eprintln!("PROBE committed head before the mid-turn turn: {session_id} at {committed}");

    // --- Turn 2 stalls IN FLIGHT: its boundary save can never land ---
    capture.hang_turns();
    identity_runtime
        .send(
            &member,
            &meerkat_core::ContentInput::Text("This turn dies in flight.".to_string()),
        )
        .await
        .expect("send the mid-turn turn");
    // Request 2 is the tool call, request 3 is the follow-up that hangs — so
    // the turn dies AFTER a completed tool round trip, i.e. with in-flight
    // work behind it.
    wait_for_turn(&capture, 3, "the mid-turn turn's post-tool LLM call").await;
    let mid = wait_for_mid_turn_durable_state(&live, committed, "the mid-turn durable write").await;
    eprintln!("PROBE mid-turn durable state: {mid:?}");

    // --- The crash image, taken with the turn still in flight ---
    copy_tree(&live, &image);

    // Release the stalled turn so boot 1 can shut down cleanly. The image is
    // already captured and `live` is never read again.
    capture.release();
    runtime.shutdown().await;

    // --- Boot 2 over the crash image ---
    let capture = CaptureClient::default();
    let runtime = boot(
        &image,
        capture.clone(),
        IdentityBootstrapMode::LazyMaterialize,
    )
    .await;
    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime")
        .clone();
    let recovered = census(&continuity_db(&image));
    eprintln!("PROBE crash-image census after recovery open: {recovered:?}");

    let outcome = identity_runtime
        .send(
            &member,
            &meerkat_core::ContentInput::Text("What token did I give you earlier?".to_string()),
        )
        .await;
    match outcome {
        Err(error) => {
            let status = identity_runtime.status(&member).await;
            eprintln!(
                "PROBE loud path: the post-crash send was REJECTED: {error} (status: {status:?})"
            );
        }
        Ok(_) => {
            wait_for_turn(&capture, 1, "the post-crash turn").await;
            let last = capture.last().expect("a post-crash request was captured");
            assert!(
                last.contains(TOKEN),
                "SILENT AMNESIA after a mid-turn kill: the post-crash turn was ACCEPTED (no \
                 error, no degraded identity) but its request does not replay the pre-crash \
                 transcript (token {TOKEN}). Mid-turn state at image time: {mid:?}. Recovered \
                 census: {recovered:?}. Request: {last}"
            );
        }
    }

    runtime.shutdown().await;
}

/// Production release-critical regression (2026-07-31), the v2-row variant of the
/// every-boot runtime-authority mint: an identity whose durable truth is a
/// RELEASED 0.8.10 whole-blob envelope must, on one cold activation, (1) mint
/// store-issued runtime authority (the ephemeral RuntimeStore has no record
/// of the runtime), (2) import the released envelope through the adapter
/// load inside that same activation, (3) resume and run a REAL turn whose
/// request replays the released transcript, and (4) commit that turn's
/// boundary - the first post-mint boundary CAS chains off the minted seed,
/// and the facade write-through advances the durable row to a document the
/// CURRENT decoder accepts. A doc note is not this proof; only the turn is.
///
/// The fixture is a frozen 0.8.10-written document
/// (tests/fixtures/README.md); current code cannot and must not mint it.
///
/// Harness note: unlike the lazy tests above, this boots with the runtime
/// store DECLARED ephemeral (`ephemeral_runtime_store(true)`) - the production
/// pod-scratch shape the mint exists for. Durable truth lives in the
/// continuity store; runtime authority reconstructs on every boot. With a
/// persistent (SQLite) runtime store the facade correctly refuses to mint,
/// and this released-row activation would stay refused by design.
async fn boot_scratch_runtime(
    state: &Path,
    capture: CaptureClient,
) -> meerkat_meta::UnifiedRuntime {
    let builder = UnifiedRuntimeBuilder::default()
        .definition(definition())
        .persistent_state(state)
        .continuity_from_state_dir(state)
        .await
        .expect("open the state-dir identity substrate")
        .roster_provider(Arc::new(OneMemberRoster))
        .agent_customizer(Arc::new(MarkerPromptCustomizer))
        .identity_bootstrap_mode(IdentityBootstrapMode::LazyMaterialize)
        .identity_runtime_instance_id(RUNTIME_INSTANCE)
        .comms(true)
        .ephemeral_runtime_store(true)
        .default_llm_client(Arc::new(capture));
    Box::pin(builder.build())
        .await
        .expect("build the pod-scratch-shaped UnifiedRuntime")
}

#[tokio::test(flavor = "multi_thread")]
async fn released_v2_document_mints_authority_imports_and_takes_a_turn() {
    if proxied_to_memo_free_child("released_v2_document_mints_authority_imports_and_takes_a_turn") {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    const RELEASED: &[u8] = include_bytes!("fixtures/v0_8_10_released_session.json");
    // A marker only the released transcript carries: the fixture's system
    // prompt. Resume authors nothing (the bridge clears the prompt
    // override), so its presence in post-mint request bytes proves the
    // imported document reached the model's working context.
    const RELEASED_MARKER: &str = "Example Review Agent";
    let raw: serde_json::Value = serde_json::from_slice(RELEASED).expect("fixture JSON");
    let released_session_id =
        meerkat_core::types::SessionId::parse(raw["id"].as_str().expect("fixture id"))
            .expect("fixture session id");
    let released_message_count = raw["messages"].as_array().expect("fixture messages").len();
    // The released envelope is exactly what the current decoder refuses -
    // nothing below can go green without the import + mint path.
    assert!(meerkat_core::Session::from_persisted_bytes(RELEASED).is_err());

    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let member = id(MEMBER);

    // --- Boot 1: one real turn through the PUBLIC flow, so the continuity
    // record carries a production-shaped runtime id (the forging idiom of
    // `never_persisted_continuity_head_fresh_spawns_instead_of_wedging`). ---
    {
        let capture = CaptureClient::default();
        let runtime = boot_scratch_runtime(&state, capture.clone()).await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text(
                    "seed a production-shaped continuity record".to_string(),
                ),
            )
            .await
            .expect("send boot 1's record-seeding turn");
        wait_for_turn(&capture, 1, "boot 1's record-seeding turn").await;
        wait_for_durable_document_at_least(&state, 2, "boot 1's turn to commit").await;
        runtime.shutdown().await;
    }

    // --- Forge the released field shape offline: rebind the record to the
    // fixture session and seed the raw 0.8.10 row exactly as that release
    // left it - a durable row never routed through current encoders. ---
    let db = continuity_db(&state);
    {
        let store = LocalContinuityStore::open(&db).expect("open continuity store for the forge");
        let resolved = store
            .resolve_many(std::slice::from_ref(&member))
            .await
            .expect("resolve the member after boot 1");
        let record = match resolved.get(&member).expect("member state") {
            ContinuityResolveState::Ready { record } => record.clone(),
            other => panic!("expected Ready after boot 1, got {other:?}"),
        };
        let boot1_session_id = record.session_id.clone();
        let mut forged = record;
        forged.session_id = released_session_id.clone();
        store
            .upsert_continuity_record(&forged, FencingToken::new(1))
            .await
            .expect("rebind the record to the released session");
        let conn = rusqlite::Connection::open(&db).expect("seed connection");
        // The released document was written by ANOTHER deployment (a different
        // member), and resume validates the persisted comms identity
        // against the current member. The seed therefore adopts this
        // harness's own persisted comms_name from boot 1's durable
        // document. The envelope ENCODING - the property under test - is
        // untouched, and the decode-refusal guard below re-proves it on
        // the exact seeded bytes.
        let boot1_doc: Vec<u8> = conn
            .query_row(
                "SELECT data FROM session_snapshots WHERE session_id = ?1 \
                 ORDER BY generation DESC, checkpoint_version DESC LIMIT 1",
                rusqlite::params![boot1_session_id.to_string()],
                |row| row.get(0),
            )
            .expect("read boot 1's durable document");
        let boot1_json: serde_json::Value =
            serde_json::from_slice(&boot1_doc).expect("boot 1 document JSON");
        let harness_comms_name = boot1_json["metadata"]["session_metadata"]["comms_name"]
            .as_str()
            .expect("boot 1's document carries this harness's comms_name")
            .to_string();
        let mut seeded = raw.clone();
        seeded["metadata"]["session_metadata"]["comms_name"] =
            serde_json::Value::String(harness_comms_name);
        let seeded_bytes =
            serde_json::to_vec(&seeded).expect("encode the seeded released document");
        assert!(
            meerkat_core::Session::from_persisted_bytes(&seeded_bytes).is_err(),
            "the seeded released envelope must still be refused by the current decoder"
        );
        conn.execute(
            "INSERT INTO session_snapshots \
             (session_id, identity, generation, checkpoint_version, fencing_token, data) \
             VALUES (?1, ?2, 0, 1, 1, ?3)",
            rusqlite::params![
                released_session_id.to_string(),
                member.to_string(),
                seeded_bytes
            ],
        )
        .expect("seed the released snapshot row");
    }

    // --- Boot 2: a cold ephemeral RuntimeStore over the released row. The
    // first send must mint, import, resume, and run a REAL turn. ---
    {
        let capture = CaptureClient::default();
        let runtime = boot_scratch_runtime(&state, capture.clone()).await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        let before = identity_runtime
            .status(&member)
            .await
            .expect("dormant identity is inspectable after the forge");
        assert_eq!(
            before.state,
            IdentityLifecycleState::Dormant,
            "boot 2 must take the lazy arm so the SEND is what activates the released row; \
             status: {before:?}"
        );

        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("What is your job?".to_string()),
            )
            .await
            .expect(
                "the released v2 document must mint runtime authority and resume; a refusal \
                 here is the production cold-activation wall",
            );
        wait_for_turn(&capture, 1, "the post-mint turn").await;
        let last = capture.last().expect("a post-mint request was captured");
        assert!(
            last.contains(RELEASED_MARKER),
            "the post-mint LLM request must replay the imported released transcript (marker \
             {RELEASED_MARKER}); request: {last}"
        );
        let after = identity_runtime
            .status(&member)
            .await
            .expect("status after the post-mint send");
        assert_eq!(
            after.session_id.as_ref().map(ToString::to_string),
            Some(released_session_id.to_string()),
            "the mint must RESUME the released session, not create a fresh one; status: {after:?}"
        );
        runtime.shutdown().await;
    }

    // --- The boundary commit: the turn's first post-mint boundary chained
    // off the minted store-issued seed, and the write-through projection
    // advanced the durable row to a CURRENT-decodable document. ---
    {
        let store = Arc::new(LocalContinuityStore::open(&db).expect("reopen continuity store"));
        let adapter = ContinuitySessionStoreAdapter::new(store);
        let resumed = meerkat::SessionStore::load(&adapter, &released_session_id)
            .await
            .expect("the post-turn durable row must load under the current decoder")
            .expect("the post-turn durable row must exist");
        assert!(
            resumed.messages().len() >= released_message_count + 2,
            "the post-mint turn's boundary commit must extend the released transcript \
             durably (have {}, want >= {})",
            resumed.messages().len(),
            released_message_count + 2
        );
    }
}

/// The class-3 binding leg on a byte-lossless continuity closure: one member
/// session (019fae11-4dd7-7301-9754-67b646603fb3) carrying a max-depth
/// 26-rewrite chain, 57-message head, 191 strand rows.
///
/// The class-3 shape: a released HEAD ROW EXISTS (created by a later delta
/// write) while the compact graph/rewrite-prefix strata do not - so adoption
/// must key on the RELEASED HEAD's inability to authorize a mutation, never
/// on head-absence. Before the adoption lane, the first projected boundary
/// at boot refused for every identity: "rewrite rejected: rewritten current
/// head has no compact graph-prefix authority" (all identities degraded
/// pending retry; the fail-closed side held).
///
/// BYTE-LOSSLESS PRINCIPLE: the bundle is reconstituted verbatim - every row
/// of every table, through the bundle's own DDL - and the HARNESS adopts the
/// bundle's identity space instead (the mob id read from the released head's
/// comms name, profile `domain`, and the member identity carried by the
/// bundle), so the persisted `mob_member_binding` and `comms_name` match the
/// booting mob without a single byte of document surgery. A patched-metadata
/// variant of this leg proved why: the identity-binding guard refuses a
/// document minted under another identity space BEFORE adoption is reached.
///
/// This leg boots that composition over the reconstitution with NO runtime
/// store: the mint reads through the head-lane importer, resume spawns, the
/// boundary projection ADOPTS under the import receipt, the identity is
/// ACTIVE, the closure transcript replays, and a real turn extends the
/// adopted (current-format) head durably.
///
/// FIXTURE: fixtures/ledger_v1_closure/ (sha256 128aee2d...) - the released
/// layout and rewrite topology with SYNTHETIC content, and every strand id,
/// commit digest and head revision computed with the released 0.8.10 digest
/// (tests/fixtures/README.md). It goes red without the adoption fix, with
/// the class-3 refusal.
#[tokio::test(flavor = "multi_thread")]
async fn downstream_rewrite_carrying_closure_adopts_resumes_and_takes_a_turn() {
    if proxied_to_memo_free_child(
        "downstream_rewrite_carrying_closure_adopts_resumes_and_takes_a_turn",
    ) {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    const CLOSURE: &[u8] =
        include_bytes!("fixtures/ledger_v1_closure/calendar-continuity-closure.json");
    const CLOSURE_DDL: &str = include_str!("fixtures/ledger_v1_closure/continuity-schema.sql");
    /// A phrase only the closure transcript carries (its synthetic domain
    /// system role, fixed by the checksummed bundle bytes).
    const CLOSURE_MARKER: &str = "household domain specialist";
    /// The member identity the checksummed bundle carries.
    const CLOSURE_MEMBER: &str = "domain:calendar";

    fn closure_bytes(value: &serde_json::Value) -> Vec<u8> {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD
            .decode(value["b64"].as_str().expect("closure {b64} value"))
            .expect("closure base64 payload")
    }

    /// Reconstitute the ENTIRE closure verbatim: the bundle's own DDL, then
    /// every row of every table. TEXT and BLOB columns are distinguished so
    /// SQLite storage classes match the source file (a TEXT value inserted
    /// as a blob would change the storage class the store reads back).
    fn reconstitute_closure(db: &std::path::Path, closure: &serde_json::Value) {
        std::fs::create_dir_all(db.parent().expect("continuity db parent"))
            .expect("create state dir");
        let conn = rusqlite::Connection::open(db).expect("reconstitution connection");
        conn.execute_batch(CLOSURE_DDL).expect("closure DDL");
        const BLOB_COLUMNS: [&str; 4] = ["head_json", "message_json", "commit_json", "data"];
        for (table, spec) in closure["tables"].as_object().expect("closure tables") {
            let columns: Vec<&str> = spec["columns"]
                .as_array()
                .expect("closure columns")
                .iter()
                .map(|c| c.as_str().expect("closure column name"))
                .collect();
            let placeholders = (1..=columns.len())
                .map(|i| format!("?{i}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "INSERT INTO {table} ({}) VALUES ({placeholders})",
                columns.join(", ")
            );
            for row in spec["rows"].as_array().expect("closure rows") {
                let params: Vec<Box<dyn rusqlite::ToSql>> = columns
                    .iter()
                    .zip(row.as_array().expect("closure row"))
                    .map(|(column, value)| -> Box<dyn rusqlite::ToSql> {
                        if value.is_null() {
                            Box::new(rusqlite::types::Null)
                        } else if let Some(number) = value.as_i64() {
                            Box::new(number)
                        } else {
                            let bytes = closure_bytes(value);
                            if BLOB_COLUMNS.contains(column) {
                                Box::new(bytes)
                            } else {
                                Box::new(
                                    String::from_utf8(bytes).expect("closure TEXT value UTF-8"),
                                )
                            }
                        }
                    })
                    .collect();
                conn.execute(
                    &sql,
                    rusqlite::params_from_iter(
                        params.iter().map(|p| p.as_ref() as &dyn rusqlite::ToSql),
                    ),
                )
                .unwrap_or_else(|error| panic!("reconstitute {table} row: {error}"));
            }
        }
    }

    let closure: serde_json::Value = serde_json::from_slice(CLOSURE).expect("closure JSON");
    let released_session_id = meerkat_core::types::SessionId::parse(
        closure["meta"]["session_id"]
            .as_str()
            .expect("meta session id"),
    )
    .expect("closure session id");
    let heads = &closure["tables"]["continuity_session_heads"];
    let head_columns: Vec<&str> = heads["columns"]
        .as_array()
        .expect("head columns")
        .iter()
        .map(|c| c.as_str().expect("head column"))
        .collect();
    let head_row = heads["rows"][0].as_array().expect("head row");
    let head_value = |name: &str| {
        &head_row[head_columns
            .iter()
            .position(|c| *c == name)
            .expect("head column present")]
    };
    let released_message_count = head_value("message_count")
        .as_i64()
        .expect("head message count");
    let head_json: serde_json::Value =
        serde_json::from_slice(&closure_bytes(head_value("head_json"))).expect("head_json");
    // The class-3 property, pinned on the exact closure bytes: a RELEASED
    // envelope with retained rewrites and NONE of the current authority
    // carriers.
    assert_eq!(
        head_json["version"].as_u64(),
        Some(2),
        "released head envelope version"
    );
    assert_eq!(
        head_value("rewrite_count").as_i64(),
        Some(26),
        "the closure's max-depth rewrite chain"
    );
    for absent in ["graph_prefix", "rewrite_prefix", "message_row_prefix"] {
        assert!(
            head_json.get(absent).is_none(),
            "the released head must NOT carry the current authority field {absent}"
        );
    }

    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    reconstitute_closure(&continuity_db(&state), &closure);
    // The closure is predecessor (pre-0.9) production state: its schema
    // ledger row names the predecessor domain (`mobkit-continuity`), so the
    // 0.9 store refuses it as unversioned until the explicit, offline
    // predecessor adoption has renamed that row, exactly as an operator runs
    // `meta_gateway storage-adopt-predecessor --apply`. The schema gate
    // stays strict; nothing here stamps the ledger by hand.
    let adoption = meerkat_meta::storage_predecessor::adopt_predecessor_state(
        &meerkat_meta::storage_predecessor::PredecessorAdoptRequest {
            state_dir: state.clone(),
            predecessor_home: None,
            gateway_home: None,
            homes_explicit: false,
            mode: meerkat_meta::storage_migrate::MigrateMode::Apply,
        },
    )
    .await;
    assert!(
        adoption.complete && !adoption.has_errors(),
        "the predecessor closure must adopt cleanly: {adoption:?}"
    );

    // The bundle's identity space, adopted by the harness.
    let closure_mob_id = head_json["metadata"]["session_metadata"]["comms_name"]
        .as_str()
        .and_then(|comms_name| comms_name.split('/').next())
        .expect("the released head carries its mob-scoped comms name");
    let closure_definition_toml = format!(
        r#"
[mob]
id = "{closure_mob_id}"

[profiles.domain]
model = "gpt-5.5"
external_addressable = true
runtime_mode = "turn_driven"

[profiles.domain.tools]
comms = true
"#
    );
    let closure_definition = MobDefinition::from_toml(&closure_definition_toml)
        .expect("parse the closure-shaped mob definition");
    struct ClosureRoster;
    #[async_trait]
    impl RosterProvider for ClosureRoster {
        async fn roster(
            &self,
            _context: &RosterContext,
        ) -> Result<Vec<DurableAgentSpec>, RosterError> {
            Ok(vec![DurableAgentSpec {
                identity: id(CLOSURE_MEMBER),
                profile: ProfileName::from("domain"),
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
            }])
        }
    }

    let member = id(CLOSURE_MEMBER);
    // Boot 1 in its own scope: every runtime handle (including the topology
    // control flock) must drop before boot 2 opens the same state dir.
    {
        let capture = CaptureClient::default();
        let runtime = {
            let builder = UnifiedRuntimeBuilder::default()
                .definition(closure_definition)
                .persistent_state(&state)
                .continuity_from_state_dir(&state)
                .await
                .expect("open the reconstituted identity substrate")
                .roster_provider(Arc::new(ClosureRoster))
                .identity_bootstrap_mode(IdentityBootstrapMode::LazyMaterialize)
                .identity_runtime_instance_id("closure-runtime")
                .comms(true)
                .ephemeral_runtime_store(true)
                .default_llm_client(Arc::new(capture.clone()));
            Box::pin(builder.build())
                .await
                .expect("build the closure-shaped UnifiedRuntime over the closure")
        };
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("What did we schedule?".to_string()),
            )
            .await
            .expect(
                "the closure must import, mint, resume, and ADOPT; a refusal here is the \
             class-3 boot dead end",
            );
        wait_for_turn(&capture, 1, "the post-adoption turn").await;
        let last = capture
            .last()
            .expect("a post-adoption request was captured");
        assert!(
            last.contains(CLOSURE_MARKER),
            "the post-adoption LLM request must replay the closure transcript (marker \
         {CLOSURE_MARKER:?})"
        );
        let after = identity_runtime
            .status(&member)
            .await
            .expect("status after the post-adoption send");
        assert_eq!(
            after.state,
            IdentityLifecycleState::Active,
            "the closure identity must come back ACTIVE, not degraded; status: {after:?}"
        );
        assert_eq!(
            after.session_id.as_ref().map(ToString::to_string),
            Some(released_session_id.to_string()),
            "the mint must RESUME the closure session, not rotate; status: {after:?}"
        );
        runtime.shutdown().await;
    }

    // The adoption landed durably: the head is CURRENT (no longer the
    // released envelope), the transcript extended, and it loads under the
    // current decoder without the importer.
    let db = continuity_db(&state);
    let (adopted_head, live_after_boot_1) = {
        let store = Arc::new(LocalContinuityStore::open(&db).expect("reopen continuity store"));
        let adopted_head =
            meerkat_meta::identity_first::contracts::ContinuityIncrementalSessions::load_canonical_head(
                store.as_ref(),
                &released_session_id,
            )
            .await
            .expect("adopted head read")
            .expect("adopted head present");
        assert_ne!(
            adopted_head.version, 2,
            "the boundary projection must ADOPT the released head into a current one"
        );
        let adapter = ContinuitySessionStoreAdapter::new(store);
        // The turn's durable projection can lag its completion signal under
        // full-suite load; poll with the file's standard 30s bound instead of
        // asserting on a single immediate read (a longer wait can only reduce
        // flakes - the floor assertion below still gates the outcome).
        let turn_floor = (released_message_count as usize) + 2;
        let deadline = Instant::now() + Duration::from_secs(30);
        let resumed = loop {
            let loaded = meerkat::SessionStore::load(&adapter, &released_session_id)
                .await
                .expect("the adopted durable document must load under the current decoder")
                .expect("the adopted durable document must exist");
            if loaded.messages().len() >= turn_floor || Instant::now() >= deadline {
                break loaded;
            }
            sleep(Duration::from_millis(100)).await;
        };
        assert!(
            resumed.messages().len() >= turn_floor,
            "the post-adoption turn must extend the closure transcript durably \
             (have {}, want >= {})",
            resumed.messages().len(),
            released_message_count + 2
        );
        (adopted_head, resumed)
    };

    // --- SQUASH INVARIANTS (0.8.11 one-time transcript-history squash) -----
    //
    // The adoption deliberately consumes the released rewrite graph: retired
    // revision bodies are the point of the transcript-history redesign, so
    // the pinned expectations are the POST-SQUASH values, never the
    // pre-upgrade ones. What must hold regardless is the LIVE transcript:
    // every released head-strand row survives content-faithful at its exact
    // position (typed comparison; the typed decode of both sides drops the
    // released `mutation_kind` provenance annotation identically).
    {
        // Live content, row by row, against the closure's exact bytes.
        let head_strand = head_json["strand"].as_str().expect("released head strand");
        let strands = &closure["tables"]["continuity_strand_messages"];
        let strand_columns: Vec<&str> = strands["columns"]
            .as_array()
            .expect("strand columns")
            .iter()
            .map(|c| c.as_str().expect("strand column"))
            .collect();
        let strand_idx = |name: &str| {
            strand_columns
                .iter()
                .position(|c| *c == name)
                .expect("strand column present")
        };
        let mut released_rows: Vec<(i64, Vec<u8>)> = strands["rows"]
            .as_array()
            .expect("strand rows")
            .iter()
            .map(|r| r.as_array().expect("strand row"))
            .filter(|r| {
                String::from_utf8(closure_bytes(&r[strand_idx("strand")])).expect("strand id")
                    == head_strand
            })
            .map(|r| {
                (
                    r[strand_idx("seq")].as_i64().expect("strand seq"),
                    closure_bytes(&r[strand_idx("message_json")]),
                )
            })
            .collect();
        released_rows.sort_by_key(|(seq, _)| *seq);
        assert_eq!(
            released_rows.len(),
            released_message_count as usize,
            "the closure's head strand must carry exactly the released live transcript"
        );
        for (index, (_, released_bytes)) in released_rows.iter().enumerate() {
            let released_message: meerkat_core::types::Message =
                serde_json::from_slice(released_bytes)
                    .expect("released row decodes as a current Message");
            assert_eq!(
                &live_after_boot_1.messages()[index],
                &released_message,
                "post-adoption live transcript row {index} must be content-faithful to the \
                 released row"
            );
        }
        // Post-squash representation counts: the released 26-rewrite graph is
        // consumed whole (no history state key in this head's metadata, so
        // the imported reading retains no history), and the adopted document
        // lives on one strand.
        let conn = rusqlite::Connection::open(&db).expect("squash census connection");
        let rewrite_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM continuity_session_rewrites WHERE session_id = ?1",
                rusqlite::params![released_session_id.to_string()],
                |row| row.get(0),
            )
            .expect("count rewrite rows");
        assert_eq!(
            rewrite_rows, 0,
            "the one-time squash must consume the released rewrite graph whole (26 -> 0)"
        );
        assert_eq!(
            adopted_head.rewrite_count, 0,
            "the adopted head starts a current lineage at generation 0"
        );
        let live_strands: i64 = conn
            .query_row(
                "SELECT COUNT(DISTINCT strand) FROM continuity_strand_messages \
                 WHERE session_id = ?1",
                rusqlite::params![released_session_id.to_string()],
                |row| row.get(0),
            )
            .expect("count distinct strands");
        assert_eq!(
            live_strands, 1,
            "the adoption purges superseded strands; the adopted document lives on ONE strand"
        );
    }

    // --- Boot 2: ZERO-TURN, EAGER materialization - a reconcile-only
    // boot shape (resume-spawn + control-snapshot boundary + projection,
    // no turn). EXACTLY-ONCE adoption demands ZERO head writes here: the
    // adopted head is current (the released-adoption arm keys on envelope
    // version 2), and a zero-semantic-change projection must land on the
    // exact-resave noop - including the per-boot updated_at touch and the
    // set-order re-stamp of any tool-visibility filter (the
    // domain:security violation: a byte-different head every boot). ---
    let head_row_snapshot = |phase: &str| -> (String, i64, Vec<u8>) {
        let conn = rusqlite::Connection::open(&db).expect("head snapshot connection");
        conn.query_row(
            "SELECT cas_token, checkpoint_version, head_json FROM continuity_session_heads \
             WHERE session_id = ?1",
            rusqlite::params![released_session_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap_or_else(|error| panic!("head row snapshot ({phase}): {error}"))
    };
    let before_zero_turn_boot = head_row_snapshot("before the zero-turn boot");
    {
        let capture = CaptureClient::default();
        let runtime = {
            let builder = UnifiedRuntimeBuilder::default()
                .definition(
                    MobDefinition::from_toml(&closure_definition_toml)
                        .expect("parse the closure-shaped mob definition (zero-turn boot)"),
                )
                .persistent_state(&state)
                .continuity_from_state_dir(&state)
                .await
                .expect("reopen the adopted identity substrate (zero-turn boot)")
                .roster_provider(Arc::new(ClosureRoster))
                .identity_bootstrap_mode(IdentityBootstrapMode::EagerMaterialize)
                .identity_runtime_instance_id("closure-runtime")
                .comms(true)
                .ephemeral_runtime_store(true)
                .default_llm_client(Arc::new(capture.clone()));
            Box::pin(builder.build())
                .await
                .expect("build the zero-turn eager boot over the adopted state")
        };
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        let status = identity_runtime
            .status(&member)
            .await
            .expect("status after the eager zero-turn boot");
        assert_eq!(
            status.state,
            IdentityLifecycleState::Active,
            "the eager boot must resume the adopted session; status: {status:?}"
        );
        runtime.shutdown().await;
    }
    let after_zero_turn_boot = head_row_snapshot("after the zero-turn boot");
    assert_eq!(
        before_zero_turn_boot, after_zero_turn_boot,
        "EXACTLY-ONCE violated: a zero-turn boot performed a head write \
         (cas_token/checkpoint/bytes changed) - the domain:security per-boot \
         rewrite class"
    );

    // --- Boot 3: a real turn extends through the ORDINARY arms - same
    // strand, same envelope version, no rebase, plain appends only. ---
    {
        let capture = CaptureClient::default();
        let runtime = {
            let builder = UnifiedRuntimeBuilder::default()
                .definition(
                    MobDefinition::from_toml(&closure_definition_toml)
                        .expect("parse the closure-shaped mob definition (boot 2)"),
                )
                .persistent_state(&state)
                .continuity_from_state_dir(&state)
                .await
                .expect("reopen the adopted identity substrate")
                .roster_provider(Arc::new(ClosureRoster))
                .identity_bootstrap_mode(IdentityBootstrapMode::LazyMaterialize)
                .identity_runtime_instance_id("closure-runtime")
                .comms(true)
                .ephemeral_runtime_store(true)
                .default_llm_client(Arc::new(capture.clone()));
            Box::pin(builder.build())
                .await
                .expect("build boot 2 over the adopted state")
        };
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("And the week after?".to_string()),
            )
            .await
            .expect("boot 3 must resume the ADOPTED document through the ordinary arms");
        wait_for_turn(&capture, 1, "boot 3's turn").await;
        runtime.shutdown().await;

        let store = Arc::new(LocalContinuityStore::open(&db).expect("reopen after boot 2"));
        let boot2_head =
            meerkat_meta::identity_first::contracts::ContinuityIncrementalSessions::load_canonical_head(
                store.as_ref(),
                &released_session_id,
            )
            .await
            .expect("boot 2 head read")
            .expect("boot 2 head present");
        assert_eq!(
            boot2_head.strand, adopted_head.strand,
            "EXACTLY-ONCE adoption violated: boot 2 switched the head strand again \
             (the domain:security double-adoption class)"
        );
        assert_eq!(
            boot2_head.version, adopted_head.version,
            "boot 2 must not change the envelope version again"
        );
        assert_eq!(
            boot2_head.rewrite_count, adopted_head.rewrite_count,
            "boot 2 must not mint or consume rewrites"
        );
        assert!(
            boot2_head.message_count >= adopted_head.message_count + 2,
            "boot 2's turn must extend the adopted strand by plain appends \
             (have {}, want >= {})",
            boot2_head.message_count,
            adopted_head.message_count + 2
        );
        let adapter = ContinuitySessionStoreAdapter::new(store);
        let after_boot_2 = meerkat::SessionStore::load(&adapter, &released_session_id)
            .await
            .expect("boot 2 durable document loads")
            .expect("boot 2 durable document exists");
        for (index, message) in live_after_boot_1.messages().iter().enumerate() {
            assert_eq!(
                &after_boot_2.messages()[index],
                message,
                "boot 2 changed already-durable transcript row {index}"
            );
        }
    }
}

/// The PR #304 CI wedge, mirrored at the gateway shape (the Python
/// `test_real_gateway_reset_reprofile_materializes_shell_tools` sequence):
/// an identity with an ACTIVE session takes a LIVE reset (the continuity
/// record is replaced while the superseded runtime session is still live in
/// this process; its retirement is deferred cleanup debt BY DESIGN), a real
/// turn runs on the fresh session, and shutdown completes within a bounded
/// horizon. Before the supersede-absorbing projection, the superseded
/// session's next boundary commit (its shutdown checkpoint at the latest)
/// failed the durable write-through with the store's cursor refusal, the
/// runtime escalated to repair-blocked retention, the deferred retire
/// wedged behind it, and gateway shutdown blew its 310s bounded horizon.
#[tokio::test(flavor = "multi_thread")]
async fn live_reset_takes_a_turn_and_shuts_down_within_horizon() {
    if proxied_to_memo_free_child("live_reset_takes_a_turn_and_shuts_down_within_horizon") {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let member = id(MEMBER);
    let capture = CaptureClient::default();
    let runtime = boot_scratch_runtime(&state, capture.clone()).await;
    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime")
        .clone();
    identity_runtime
        .send(
            &member,
            &meerkat_core::ContentInput::Text("turn on the ORIGINAL session".to_string()),
        )
        .await
        .expect("the pre-reset turn");
    wait_for_turn(&capture, 1, "the pre-reset turn").await;
    let before = identity_runtime
        .status(&member)
        .await
        .expect("status before reset");
    let old_session = before
        .session_id
        .as_ref()
        .map(ToString::to_string)
        .expect("a bound session before reset");

    let record = identity_runtime
        .reset(&member)
        .await
        .expect("the LIVE reset");
    assert_ne!(
        record.session_id.to_string(),
        old_session,
        "reset must mint a fresh session"
    );

    identity_runtime
        .send(
            &member,
            &meerkat_core::ContentInput::Text("turn on the FRESH session".to_string()),
        )
        .await
        .expect("the post-reset turn on the fresh session");
    wait_for_turn(&capture, 2, "the post-reset turn").await;

    // The horizon pin. In the field the superseded session's failed
    // projection held teardown in retain-for-retry past the gateway's 310s
    // bounded horizon; a healthy teardown completes in seconds locally but a
    // loaded 2-vCPU CI runner needs minutes for the full lifecycle - 5 minutes
    // still cleanly separates slow-but-done from the 310s+ retention wedge.
    tokio::time::timeout(std::time::Duration::from_mins(5), runtime.shutdown())
        .await
        .expect(
            "shutdown must complete within the bounded horizon; a timeout here is the \
             superseded-session repair-blocked retention wedge",
        );
}

/// Regression (a) of the every-boot mint acceptance: a durable
/// CURRENT-encoding session row and an EMPTY (pod-scratch) runtime store -
/// the first send mints store-issued authority from the durable row,
/// resumes the transcript, and runs a REAL turn whose boundary commit
/// chains off the minted seed. Isolates mint-path failures from
/// import-path failures; the released-envelope variant of the same chain is
/// `released_v2_document_mints_authority_imports_and_takes_a_turn` above.
#[tokio::test(flavor = "multi_thread")]
async fn current_row_mints_authority_resumes_and_takes_a_turn() {
    const TOKEN: &str = "MARKER-PLAIN-MINT-5-VICTOR";
    if proxied_to_memo_free_child("current_row_mints_authority_resumes_and_takes_a_turn") {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let member = id(MEMBER);

    // --- Boot 1 (scratch runtime): one turn carrying the marker. ---
    let session_id;
    let boot1_count;
    {
        let capture = CaptureClient::default();
        let runtime = boot_scratch_runtime(&state, capture.clone()).await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text(format!("Please note this token: {TOKEN}")),
            )
            .await
            .expect("send boot 1's turn");
        wait_for_turn(&capture, 1, "boot 1's turn").await;
        let (head, count) =
            wait_for_durable_document_at_least(&state, 2, "boot 1's turn to commit").await;
        session_id = head;
        boot1_count = count;
        runtime.shutdown().await;
    }

    // --- Boot 2: a COLD pod (empty runtime store) over the durable
    // current-encoding row. The first send must mint, resume, and run a
    // REAL turn whose boundary commit lands durably. ---
    {
        let capture = CaptureClient::default();
        let runtime = boot_scratch_runtime(&state, capture.clone()).await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("What token did I give you earlier?".to_string()),
            )
            .await
            .expect("the cold send must mint runtime authority and resume the durable row");
        wait_for_turn(&capture, 1, "the post-mint turn").await;
        let last = capture.last().expect("a post-mint request was captured");
        assert!(
            last.contains(TOKEN),
            "the post-mint LLM request must replay the durable transcript (token {TOKEN}); \
             request: {last}"
        );
        let after = identity_runtime
            .status(&member)
            .await
            .expect("status after the post-mint send");
        assert_eq!(
            after.session_id.as_ref().map(ToString::to_string),
            Some(session_id.clone()),
            "the mint must RESUME the durable session, not create a fresh one; status: {after:?}"
        );
        // The turn's boundary commit chained off the minted seed and the
        // write-through projection advanced the durable row.
        wait_for_durable_document_at_least(
            &state,
            boot1_count + 2,
            "the post-mint turn's boundary commit",
        )
        .await;
        runtime.shutdown().await;
    }
}

/// The sanctioned runtime-store-reset recovery path in miniature (a
/// 0.8.11 upgrade shape): continuity intact, runtime.sqlite
/// DELETED. The next boot must reseed runtime authority from the durable
/// continuity rows - the member resumes with the exact preserved
/// transcript and takes a real turn whose boundary commits - instead of
/// refusing with "missing durable session snapshot (<no runtime record>)".
/// Unlike the pod-scratch tests above this runs the PERSISTENT (SQLite
/// runtime store) composition: the mint arms for durable inner stores too.
#[tokio::test(flavor = "multi_thread")]
async fn reset_runtime_store_reseeds_from_continuity_and_resumes() {
    const TOKEN: &str = "MARKER-RUNTIME-RESET-9-OSCAR";
    if proxied_to_memo_free_child("reset_runtime_store_reseeds_from_continuity_and_resumes") {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let member = id(MEMBER);

    // --- Boot 1 (persistent runtime store): one turn with the marker. ---
    let session_id;
    let boot1_count;
    {
        let capture = CaptureClient::default();
        let runtime = boot(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text(format!("Please note this token: {TOKEN}")),
            )
            .await
            .expect("send boot 1's turn");
        wait_for_turn(&capture, 1, "boot 1's turn").await;
        let (head, count) =
            wait_for_durable_document_at_least(&state, 2, "boot 1's turn to commit").await;
        session_id = head;
        boot1_count = count;
        runtime.shutdown().await;
    }

    // --- The RESET: delete the runtime.sqlite file SET (sidecars and
    // maintenance-fence marker included - a full reset removes
    // them all, and half-deleted sidecar combinations behave differently);
    // the continuity store is untouched. ---
    let mut removed = 0;
    for suffix in ["", "-wal", "-shm", ".mfence"] {
        let path = state.join(format!(
            "{}{suffix}",
            meerkat_meta::storage_layout::RUNTIME_DB_FILE_NAME
        ));
        if path.exists() {
            std::fs::remove_file(&path).expect("reset runtime store");
            removed += 1;
        }
    }
    assert!(
        removed > 0,
        "the persistent shape must have written runtime.sqlite"
    );
    assert!(
        continuity_db(&state).exists(),
        "the reset must leave the continuity store intact"
    );

    // --- Boot 2 over the reset store: resume + recall + a real turn. ---
    {
        let capture = CaptureClient::default();
        let runtime = boot(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("What token did I give you earlier?".to_string()),
            )
            .await
            .expect("the post-reset send must reseed runtime authority and resume");
        wait_for_turn(&capture, 1, "the post-reset turn").await;
        let last = capture.last().expect("a post-reset request was captured");
        assert!(
            last.contains(TOKEN),
            "the post-reset LLM request must replay the preserved transcript (token {TOKEN}); \
             request: {last}"
        );
        let after = identity_runtime
            .status(&member)
            .await
            .expect("status after the post-reset send");
        assert_eq!(
            after.session_id.as_ref().map(ToString::to_string),
            Some(session_id.clone()),
            "the reseed must RESUME the durable session, not create a fresh one; \
             status: {after:?}"
        );
        wait_for_durable_document_at_least(
            &state,
            boot1_count + 2,
            "the post-reset turn's boundary commit",
        )
        .await;
        runtime.shutdown().await;
    }
}

/// COLD-MINT COUNT FAITHFULNESS (2026-08-05 boot blocker, released-0.8.16
/// lineage): a mob that had accumulated customizer-prompt boot appends took
/// the sanctioned runtime-store reset and came back with every member
/// broken - the machine-authorized revival's
/// first projection save was refused with "new message count N is shorter
/// than previously persisted M without transcript-continuity proof"
/// (deltas were the per-member accumulated boot appends). This
/// pins the count contract hop by hop: durable load -> runtime mint ->
/// resume materialization -> first projection must be row-count-faithful;
/// no hop may manufacture or silently admit a shrink. Boot append deltas
/// are MEASURED per boot rather than assumed, so the test holds both on
/// lineages that append a prompt copy per boot and on lineages that
/// preserve persisted prompts without appending.
#[tokio::test(flavor = "multi_thread")]
async fn reset_after_repeated_customizer_boots_preserves_exact_counts_and_projects() {
    const TOKEN: &str = "MARKER-MULTIBOOT-RESET-11-VICTOR";
    if proxied_to_memo_free_child(
        "reset_after_repeated_customizer_boots_preserves_exact_counts_and_projects",
    ) {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let member = id(MEMBER);

    // --- Boot 1: one turn carrying the recall marker. ---
    let session_id;
    let mut durable_count;
    {
        let capture = CaptureClient::default();
        let runtime = boot(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text(format!("Please note this token: {TOKEN}")),
            )
            .await
            .expect("send boot 1's turn");
        wait_for_turn(&capture, 1, "boot 1's turn").await;
        runtime.shutdown().await;
        let (head, count) =
            wait_for_durable_document_at_least(&state, 2, "boot 1's turn to commit").await;
        session_id = head;
        durable_count = count;
    }

    // --- Boots 2..=4: accumulate whatever this lineage's resume policy
    // appends per customizer boot (measured after quiescent shutdown, not
    // assumed). Each boot takes one real turn so its boundary commits. ---
    let mut boot_appends: i64 = 0;
    for boot_no in 2i64..=4 {
        let capture = CaptureClient::default();
        let runtime = boot(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text(format!("boot {boot_no} touch")),
            )
            .await
            .unwrap_or_else(|e| panic!("boot {boot_no}'s turn must not be refused: {e}"));
        wait_for_turn(&capture, 1, "an accumulation boot's turn").await;
        wait_for_durable_document_at_least(
            &state,
            durable_count + 2,
            "an accumulation boot's boundary commit",
        )
        .await;
        runtime.shutdown().await;
        let (head, count) =
            wait_for_durable_document_at_least(&state, 0, "the quiescent post-boot document").await;
        assert_eq!(
            head, session_id,
            "accumulation boots must extend the same durable session"
        );
        let appended = count - durable_count - 2;
        assert!(
            appended >= 0,
            "boot {boot_no} SHRANK the durable document: {durable_count} -> {count}"
        );
        boot_appends += appended;
        durable_count = count;
    }

    // --- Direct typed seed (the field accumulation): this harness's customizer resume authors nothing per
    // boot (measured boot_appends stays 0 here), but the field gateway's
    // callback path appended one System copy per boot for a day - so seed
    // the duplicate System rows through the SAME adapter representation
    // the gateway writes through, and assert the accumulation is real
    // (a zero-duplicate run must not pass as a field repro). ---
    const SEEDED_DUPLICATES: i64 = 12;
    {
        let continuity: std::sync::Arc<dyn meerkat_meta::identity_first::ContinuityStore> =
            std::sync::Arc::new(
                meerkat_meta::identity_first::LocalContinuityStore::open(continuity_db(&state))
                    .expect("open the continuity store for the duplicate seed"),
            );
        let adapter = std::sync::Arc::new(
            meerkat_meta::identity_first::ContinuitySessionStoreAdapter::new(
                std::sync::Arc::clone(&continuity),
            ),
        );
        let sid = meerkat_core::types::SessionId::parse(&session_id).expect("session id");
        let (record, fencing_token, fence_current) = continuity
            .resolve_record_by_session(&sid)
            .await
            .expect("resolve the continuity record")
            .expect("a continuity record binds the session");
        adapter
            .register_session(
                &sid,
                meerkat_meta::identity_first::SessionRuntimeState {
                    identity: record.identity.clone(),
                    generation: record.generation,
                    fencing_token,
                    checkpoint_version: fence_current,
                },
            )
            .await
            .expect("register the seed writer");
        let mut seeded = meerkat::SessionStore::load(adapter.as_ref(), &sid)
            .await
            .expect("load for the duplicate seed")
            .expect("the durable session exists");
        for _ in 0..SEEDED_DUPLICATES {
            seeded.push(meerkat_core::Message::System(
                meerkat_core::types::SystemMessage::new(CUSTOMIZER_PROMPT),
            ));
        }
        meerkat::SessionStore::save(adapter.as_ref(), &seeded)
            .await
            .expect("seed the duplicate System rows through the typed door");
    }
    durable_count += SEEDED_DUPLICATES;
    let (_, seeded_count) =
        wait_for_durable_document_at_least(&state, durable_count, "the seeded duplicates").await;
    assert_eq!(
        seeded_count, durable_count,
        "all {SEEDED_DUPLICATES} duplicate System rows must be durable before the reset"
    );

    // --- The sanctioned reset: the runtime.sqlite file set deleted,
    // continuity untouched (same file set as the single-boot test above). ---
    let mut removed = 0;
    for suffix in ["", "-wal", "-shm", ".mfence"] {
        let path = state.join(format!(
            "{}{suffix}",
            meerkat_meta::storage_layout::RUNTIME_DB_FILE_NAME
        ));
        if path.exists() {
            std::fs::remove_file(&path).expect("reset runtime store");
            removed += 1;
        }
    }
    assert!(
        removed > 0,
        "the persistent shape must have written runtime.sqlite"
    );
    assert!(
        continuity_db(&state).exists(),
        "the reset must leave the continuity store intact"
    );

    // Durable-load hop: the reset must not move the durable document.
    let (head, pre_mint_count) =
        wait_for_durable_document_at_least(&state, 0, "the durable document after the reset").await;
    assert_eq!(head, session_id);
    assert_eq!(
        pre_mint_count, durable_count,
        "the reset deleted only runtime.sqlite, so the durable count must be unchanged"
    );

    // --- Boot 5 over the reset store: cold mint -> materialize -> turn ->
    // first projection. This send is exactly where the field failure hit. ---
    {
        let capture = CaptureClient::default();
        let runtime = boot(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("What token did I give you earlier?".to_string()),
            )
            .await
            .expect(
                "the cold-mint resume turn must not be refused \
                 (field failure: MonotonicityViolation on the first projection save)",
            );
        wait_for_turn(&capture, 1, "the cold-mint turn").await;
        let last = capture.last().expect("the cold-mint request was captured");
        assert!(
            last.contains(TOKEN),
            "the cold-mint materialization must replay boot 1's turn (token {TOKEN}); \
             request: {last}"
        );
        let after = identity_runtime
            .status(&member)
            .await
            .expect("status after the cold-mint send");
        assert_eq!(
            after.session_id.as_ref().map(ToString::to_string),
            Some(session_id.clone()),
            "the cold mint must RESUME the durable session, not create a fresh one; \
             status: {after:?}"
        );
        wait_for_durable_document_at_least(
            &state,
            pre_mint_count + 2,
            "the cold-mint turn's boundary commit",
        )
        .await;
        runtime.shutdown().await;

        // First-projection hop: exact count against this boot's own
        // measured append delta - growth only, never a shrink.
        let (_, post_count) =
            wait_for_durable_document_at_least(&state, 0, "the quiescent post-cold-mint document")
                .await;
        let boot5_appends = post_count - pre_mint_count - 2;
        assert!(
            boot5_appends >= 0,
            "the cold-mint boot SHRANK the durable document: {pre_mint_count} -> {post_count}"
        );

        // Resume-materialization hop: every accumulated customizer copy
        // reaches the LLM request - the live prompt, the seeded field-shape
        // duplicates, and each measured boot append (prior boots' plus this
        // boot's own).
        let expected_copies = usize::try_from(1 + SEEDED_DUPLICATES + boot_appends + boot5_appends)
            .expect("customizer copy count");
        let seen_copies = last.matches(CUSTOMIZER_PROMPT).count();
        eprintln!(
            "COUNT-TRACE: seeded={SEEDED_DUPLICATES} boot_appends={boot_appends} \
             boot5_appends={boot5_appends} pre_mint={pre_mint_count} post={post_count} \
             seen_copies={seen_copies}"
        );
        assert_eq!(
            seen_copies, expected_copies,
            "cold-mint materialization dropped or invented customizer copies \
             (live prompt 1 + seeded {SEEDED_DUPLICATES} + accumulated {boot_appends} \
             + this boot's {boot5_appends}); request: {last}"
        );
    }
}

/// Operator-rewrite mirror (a drill-proven boot blocker): an operator
/// dedup rewrite through the typed door, then the sanctioned runtime-store
/// reset, then a cold boot. The cold mint materializes the rewritten
/// durable head SLIM (the compact graph stays out-of-line), so the first
/// turn's boundary projection composed rewrite-shaped state with no graph
/// authority and the store refused fail-closed ("rewritten session has no
/// validated compact graph authority") - every member wedged in a
/// refuse-retry loop (reproduced byte-exact offline on both released 0.8.16
/// and the 0.8.17 candidate). The mint must hydrate the graph from the store's own
/// adopted rewrite records so the seed carries the authority its durable
/// row already proves.
#[tokio::test(flavor = "multi_thread")]
async fn reset_after_operator_rewrite_cold_mints_with_graph_authority() {
    const TOKEN: &str = "MARKER-REWRITE-RESET-13-WHISKEY";
    if proxied_to_memo_free_child("reset_after_operator_rewrite_cold_mints_with_graph_authority") {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let member = id(MEMBER);

    // --- Boot 1: one turn carrying the recall marker. ---
    let session_id;
    {
        let capture = CaptureClient::default();
        let runtime = boot(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text(format!("Please note this token: {TOKEN}")),
            )
            .await
            .expect("send boot 1's turn");
        wait_for_turn(&capture, 1, "boot 1's turn").await;
        runtime.shutdown().await;
        let (head, _) =
            wait_for_durable_document_at_least(&state, 2, "boot 1's turn to commit").await;
        session_id = head;
    }

    // --- Field shape: accumulated duplicate System rows, then the operator
    // dedup rewrite through the typed door (the surgery in miniature,
    // same sequence as examples/dedup_system_rows.rs). ---
    let expected_after_rewrite;
    {
        let continuity: Arc<dyn ContinuityStore> = Arc::new(
            LocalContinuityStore::open(continuity_db(&state))
                .expect("open the continuity store for the operator rewrite"),
        );
        let adapter = Arc::new(ContinuitySessionStoreAdapter::new(Arc::clone(&continuity)));
        let sid = meerkat_core::types::SessionId::parse(&session_id).expect("session id");
        let (record, fencing_token, fence_current) = continuity
            .resolve_record_by_session(&sid)
            .await
            .expect("resolve the continuity record")
            .expect("a continuity record binds the session");
        adapter
            .register_session(
                &sid,
                meerkat_meta::identity_first::SessionRuntimeState {
                    identity: record.identity.clone(),
                    generation: record.generation,
                    fencing_token,
                    checkpoint_version: fence_current,
                },
            )
            .await
            .expect("register the operator writer");
        let mut with_dups = meerkat::SessionStore::load(adapter.as_ref(), &sid)
            .await
            .expect("load for the duplicate seed")
            .expect("the durable session exists");
        for _ in 0..12 {
            with_dups.push(meerkat_core::Message::System(
                meerkat_core::types::SystemMessage::new(CUSTOMIZER_PROMPT),
            ));
        }
        meerkat::SessionStore::save(adapter.as_ref(), &with_dups)
            .await
            .expect("seed the duplicate System rows");

        let mut cleaned: Vec<meerkat_core::Message> = Vec::new();
        let mut kept_first_system = false;
        for message in with_dups.messages() {
            if matches!(message, meerkat_core::Message::System(_)) {
                if kept_first_system {
                    continue;
                }
                kept_first_system = true;
            }
            cleaned.push(message.clone());
        }
        assert!(
            with_dups.messages().len() - cleaned.len() >= 11,
            "the dedup must drop real duplicate rows"
        );
        expected_after_rewrite = i64::try_from(cleaned.len()).expect("cleaned count");
        let parent_revision = with_dups
            .transcript_revision()
            .expect("parent revision before the rewrite");
        let selection_end = with_dups.messages().len();
        let mut rewritten = with_dups.clone();
        let commit = rewritten
            .commit_transcript_rewrite(
                meerkat_core::TranscriptRewriteSelection::MessageRange {
                    start: 0,
                    end: selection_end,
                },
                cleaned,
                meerkat_core::TranscriptRewriteReason::new(
                    "test operator dedup of duplicate System copies",
                ),
                Some("test-operator/dedup".to_string()),
                Some(parent_revision),
            )
            .expect("compose the dedup rewrite");
        meerkat::SessionStore::save_transcript_rewrite(adapter.as_ref(), &rewritten, &commit)
            .await
            .expect("commit the dedup rewrite through the typed door");
        meerkat::SessionStore::save_authoritative_projection(adapter.as_ref(), &rewritten)
            .await
            .expect("project the rewritten head");
    }
    let (_, post_rewrite_count) =
        wait_for_durable_document_at_least(&state, 0, "the rewritten durable head").await;
    assert_eq!(
        post_rewrite_count, expected_after_rewrite,
        "the dedup rewrite must land on the durable head"
    );

    // --- The sanctioned reset: runtime.sqlite file set deleted, continuity
    // untouched. ---
    let mut removed = 0;
    for suffix in ["", "-wal", "-shm", ".mfence"] {
        let path = state.join(format!(
            "{}{suffix}",
            meerkat_meta::storage_layout::RUNTIME_DB_FILE_NAME
        ));
        if path.exists() {
            std::fs::remove_file(&path).expect("reset runtime store");
            removed += 1;
        }
    }
    assert!(
        removed > 0,
        "the persistent shape must have written runtime.sqlite"
    );
    assert!(
        continuity_db(&state).exists(),
        "the reset must leave the continuity store intact"
    );

    // --- Cold boot over the rewritten head: mint, materialize, one real
    // turn whose boundary projection must carry graph authority. Pre-fix
    // this send refused fail-closed and the member wedged. ---
    {
        let capture = CaptureClient::default();
        let runtime = boot(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("What token did I give you earlier?".to_string()),
            )
            .await
            .expect(
                "the cold-mint turn over a rewritten head must not be refused \
                 (field failure: 'rewritten session has no validated compact graph authority')",
            );
        wait_for_turn(&capture, 1, "the cold-mint turn").await;
        let last = capture.last().expect("the cold-mint request was captured");
        assert!(
            last.contains(TOKEN),
            "the cold mint must replay the preserved transcript (token {TOKEN})"
        );
        // Exactly the LIVE prompt. The dedup kept the transcript's first
        // System row - the boot-1 creation row, which does not carry the
        // customizer marker (the multi-boot regression's 13-not-14 count
        // pins that) - so every seeded marker copy is dead and the only
        // marker in the request is the live system prompt itself.
        assert_eq!(
            last.matches(CUSTOMIZER_PROMPT).count(),
            1,
            "the deduped marker copies must stay dead after the cold mint"
        );
        runtime.shutdown().await;
        let (_, post) =
            wait_for_durable_document_at_least(&state, 0, "the post-cold-mint document").await;
        assert_eq!(
            post,
            expected_after_rewrite + 2,
            "the first projection must extend the rewritten head exactly"
        );
    }
}

/// STEER INTO A NON-RESIDENT MEMBER (production 0.8.18 field-acceptance shape,
/// 2026-08-06): a console steer whose arrival TRIGGERS the member build
/// persisted its interaction id into the first run's input row with an
/// EMPTY body - identity survived the whole chain, content did not, and
/// the marker never appeared in the session. This pins meta's half of
/// that chain: a Steer-mode tracked send into a not-yet-materialized
/// member must deliver its body into the first run. Green here means the
/// console hand-off and the identity-first materialization lane carry
/// steer content intact and the residual field loss lives beyond the
/// bridge admission.
#[tokio::test(flavor = "multi_thread")]
async fn steer_into_non_resident_member_delivers_content() {
    const MARKER: &str = "STEER-CONTENT-MARKER-15-XRAY";
    if proxied_to_memo_free_child("steer_into_non_resident_member_delivers_content") {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let member = id(MEMBER);

    // Boot 1: one turn so the member has a durable session, then shut down
    // so the next boot starts with the member NON-RESIDENT.
    {
        let capture = CaptureClient::default();
        let runtime = boot(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("seed turn".to_string()),
            )
            .await
            .expect("seed turn");
        wait_for_turn(&capture, 1, "the seed turn").await;
        runtime.shutdown().await;
    }

    // Boot 2: the very first contact is a STEER-mode tracked send - the
    // steer's own arrival drives materialization (the production shape).
    {
        let capture = CaptureClient::default();
        let runtime = boot(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send_with_mode_and_interaction_tracked(
                &member,
                &meerkat_core::ContentInput::Text(format!(
                    "{MARKER}: if you can read this line, reply with the marker."
                )),
                meerkat_core::types::HandlingMode::Steer,
                Some("0818aaaa-1247-5ce3-85c8-17ec20e09a77"),
            )
            .await
            .expect("the steer into a non-resident member must be admitted");
        wait_for_turn(&capture, 1, "the steer-driven first turn").await;
        let last = capture.last().expect("a request was captured");
        assert!(
            last.contains(MARKER),
            "the steer body must reach the first run's request - the field shape lost it \
             (identity persisted, content empty); request: {last}"
        );
        runtime.shutdown().await;
    }
}

/// THE SHIPPED REPAIR BINARY end to end: seed field-shape
/// duplicate System rows, run `meta-repair` dry-run then `--apply`
/// against the stopped store, reset the runtime scratch, and prove the
/// member resumes with the healed head and its conversation intact - the
/// operator repair procedure, driven through the supported binary
/// instead of the example.
#[tokio::test(flavor = "multi_thread")]
async fn meta_repair_binary_prunes_duplicates_and_member_resumes() {
    const TOKEN: &str = "MARKER-REPAIR-BIN-17-ZULU";
    if proxied_to_memo_free_child("meta_repair_binary_prunes_duplicates_and_member_resumes") {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let member = id(MEMBER);

    // Boot 1: one turn with the recall marker, then shut down.
    let session_id;
    {
        let capture = CaptureClient::default();
        let runtime = boot(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text(format!("Please note this token: {TOKEN}")),
            )
            .await
            .expect("boot 1's turn");
        wait_for_turn(&capture, 1, "boot 1's turn").await;
        runtime.shutdown().await;
        let (head, _) = wait_for_durable_document_at_least(&state, 2, "boot 1's commit").await;
        session_id = head;
    }

    // Seed 12 field-shape duplicate System rows through the typed door.
    {
        let continuity: Arc<dyn ContinuityStore> =
            Arc::new(LocalContinuityStore::open(continuity_db(&state)).expect("open continuity"));
        let adapter = Arc::new(ContinuitySessionStoreAdapter::new(Arc::clone(&continuity)));
        let sid = meerkat_core::types::SessionId::parse(&session_id).expect("session id");
        let (record, fencing_token, fence_current) = continuity
            .resolve_record_by_session(&sid)
            .await
            .expect("resolve record")
            .expect("record binds session");
        adapter
            .register_session(
                &sid,
                meerkat_meta::identity_first::SessionRuntimeState {
                    identity: record.identity.clone(),
                    generation: record.generation,
                    fencing_token,
                    checkpoint_version: fence_current,
                },
            )
            .await
            .expect("register seed writer");
        let mut with_dups = meerkat::SessionStore::load(adapter.as_ref(), &sid)
            .await
            .expect("load")
            .expect("durable session");
        for _ in 0..12 {
            with_dups.push(meerkat_core::Message::System(
                meerkat_core::types::SystemMessage::new(CUSTOMIZER_PROMPT),
            ));
        }
        meerkat::SessionStore::save(adapter.as_ref(), &with_dups)
            .await
            .expect("seed duplicates");
    }
    let (_, seeded_count) =
        wait_for_durable_document_at_least(&state, 0, "the seeded document").await;

    // Dry run through the SHIPPED binary: plan only, durable untouched.
    let bin = env!("CARGO_BIN_EXE_meta_repair");
    let db = continuity_db(&state);
    let dry = std::process::Command::new(bin)
        .args(["--db", db.to_str().expect("db path"), "--all-sessions"])
        .output()
        .expect("run meta-repair dry-run");
    assert!(dry.status.success(), "dry-run must exit 0: {dry:?}");
    let dry_report: serde_json::Value =
        serde_json::from_slice(&dry.stdout).expect("dry-run emits JSON");
    assert_eq!(dry_report["mode"], "content_dedup");
    assert_eq!(dry_report["applied"], false);
    assert_eq!(dry_report["sessions"][0]["outcome"], "dry_run");
    assert_eq!(
        dry_report["sessions"][0]["system_rows_after"],
        dry_report["sessions"][0]["system_rows_before"]
            .as_u64()
            .expect("count")
            - 11,
        "12 identical copies collapse to one kept occurrence"
    );
    let (_, after_dry) =
        wait_for_durable_document_at_least(&state, 0, "the document after dry-run").await;
    assert_eq!(
        after_dry, seeded_count,
        "a dry run must not touch the durable row"
    );

    // Apply through the binary, then the reset-reseed lane.
    let applied = std::process::Command::new(bin)
        .args([
            "--db",
            db.to_str().expect("db path"),
            "--all-sessions",
            "--apply",
        ])
        .output()
        .expect("run meta-repair apply");
    assert!(applied.status.success(), "apply must exit 0: {applied:?}");
    let applied_report: serde_json::Value =
        serde_json::from_slice(&applied.stdout).expect("apply emits JSON");
    assert_eq!(applied_report["sessions"][0]["outcome"], "applied");
    assert_eq!(applied_report["refused"], 0);
    let expected_after = seeded_count - 11;
    let (_, healed) = wait_for_durable_document_at_least(&state, 0, "the healed document").await;
    assert_eq!(
        healed, expected_after,
        "the durable head reflects the applied plan"
    );

    let mut removed = 0;
    for suffix in ["", "-wal", "-shm", ".mfence"] {
        let path = state.join(format!(
            "{}{suffix}",
            meerkat_meta::storage_layout::RUNTIME_DB_FILE_NAME
        ));
        if path.exists() {
            std::fs::remove_file(&path).expect("reset runtime store");
            removed += 1;
        }
    }
    assert!(
        removed > 0,
        "the persistent shape must have written runtime.sqlite"
    );

    // Boot 2: cold mint over the healed rewritten head, one real turn.
    {
        let capture = CaptureClient::default();
        let runtime = boot(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("What token did I give you earlier?".to_string()),
            )
            .await
            .expect("the post-repair cold-mint turn must not be refused");
        wait_for_turn(&capture, 1, "the post-repair turn").await;
        let last = capture.last().expect("request captured");
        assert!(
            last.contains(TOKEN),
            "the conversation survives the repair (token {TOKEN})"
        );
        runtime.shutdown().await;
        let (_, post) =
            wait_for_durable_document_at_least(&state, 0, "the post-repair document").await;
        assert_eq!(
            post,
            expected_after + 2,
            "the turn extends the healed head exactly"
        );
    }
}

/// Profile-prompt revival hygiene (0.8.19 pairing prep, 2026-08-06): a
/// member whose PROFILE declares a prompt through an inline skill, booted
/// repeatedly with a turn each - automatic rematerialization must author
/// NOTHING beyond each turn's own messages, and every boot's request must
/// carry the prompt exactly once (zero copies is a lost prompt, more is
/// revival re-authoring; the durable row count below is the same fact seen
/// from the store). MEASURED FINDING: this is ALREADY GREEN on the
/// 0.8.18 pairing - the identity-first lazy revival and the cold-mint
/// recovery lane both take PreservePersisted paths (host_materialize) and
/// do not mint. The field defect (one assembled row per
/// boot, hundreds of copies) therefore lives in resume paths this harness does not
/// traverse (the mob actor resume AppendExplicit branches, reached through
/// the gateway's bridge session lanes). This test pins the clean lanes as
/// STAYING clean across the 0.8.19 contract change (resumed builds force
/// Inherit); the minting lane needs its own regression at the call site
/// an inspection of the minting lane confirms.
#[tokio::test(flavor = "multi_thread")]
async fn profile_prompt_revival_authors_no_system_rows() {
    if proxied_to_memo_free_child("profile_prompt_revival_authors_no_system_rows") {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let member = id(MEMBER);
    let definition = || {
        MobDefinition::from_toml(PROFILE_PROMPT_MOB_TOML)
            .expect("parse profile-prompt mob definition")
    };

    // Boot 1: original spawn (the assembled prompt lands in persisted
    // spawn state here) plus one turn.
    let session_id;
    let mut durable_count;
    {
        let capture = CaptureClient::default();
        let runtime = boot_with_definition_no_customizer(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
            definition(),
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("boot 1 turn".to_string()),
            )
            .await
            .expect("boot 1's turn");
        wait_for_turn(&capture, 1, "boot 1's turn").await;
        // The carrier must be real: the profile's inline-skill prompt reaches
        // the freshly spawned member's first request. Without this, the
        // revival assertions below pass for a member with no prompt to
        // re-author.
        assert!(
            PROFILE_PROMPT_MOB_TOML.contains(PROFILE_PROMPT_MARKER),
            "fixture and marker drifted apart"
        );
        let first = capture.last().expect("a boot-1 request was captured");
        assert_eq!(
            first.matches(PROFILE_PROMPT_MARKER).count(),
            1,
            "the profile's inline-skill prompt must reach the spawned member's first \
             request exactly once; request: {first}"
        );
        runtime.shutdown().await;
        let (head, count) = wait_for_durable_document_at_least(&state, 2, "boot 1's commit").await;
        session_id = head;
        durable_count = count;
    }

    // Boots 2 and 3: pure revivals. Any durable growth beyond the turn's
    // own two messages is revival-authored configuration - the defect.
    for boot_no in 2i64..=3 {
        let capture = CaptureClient::default();
        let runtime = boot_with_definition_no_customizer(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
            definition(),
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text(format!("boot {boot_no} turn")),
            )
            .await
            .unwrap_or_else(|e| panic!("boot {boot_no}'s turn must not be refused: {e}"));
        wait_for_turn(&capture, 1, "a revival boot's turn").await;
        let revived = capture.last().expect("a revival boot request was captured");
        assert_eq!(
            revived.matches(PROFILE_PROMPT_MARKER).count(),
            1,
            "boot {boot_no} must carry the profile prompt exactly once; request: {revived}"
        );
        wait_for_durable_document_at_least(
            &state,
            durable_count + 2,
            "a revival boot's boundary commit",
        )
        .await;
        runtime.shutdown().await;
        let (head, count) =
            wait_for_durable_document_at_least(&state, 0, "the quiescent post-boot document").await;
        assert_eq!(head, session_id, "revivals must extend the same session");
        assert_eq!(
            count,
            durable_count + 2,
            "boot {boot_no} authored {} extra durable row(s): automatic rematerialization \
             re-authored persisted prompt configuration as fresh System input",
            count - durable_count - 2
        );
        durable_count = count;
    }

    // Sanctioned recovery lane under the same contract: runtime-store
    // reset, cold mint, one more turn.
    let mut removed = 0;
    for suffix in ["", "-wal", "-shm", ".mfence"] {
        let path = state.join(format!(
            "{}{suffix}",
            meerkat_meta::storage_layout::RUNTIME_DB_FILE_NAME
        ));
        if path.exists() {
            std::fs::remove_file(&path).expect("reset runtime store");
            removed += 1;
        }
    }
    assert!(
        removed > 0,
        "the persistent shape must have written runtime.sqlite"
    );
    {
        let capture = CaptureClient::default();
        let runtime = boot_with_definition_no_customizer(
            &state,
            capture.clone(),
            IdentityBootstrapMode::LazyMaterialize,
            definition(),
        )
        .await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("post-reset turn".to_string()),
            )
            .await
            .expect("the cold-mint turn must not be refused");
        wait_for_turn(&capture, 1, "the cold-mint turn").await;
        let minted = capture.last().expect("a cold-mint request was captured");
        assert_eq!(
            minted.matches(PROFILE_PROMPT_MARKER).count(),
            1,
            "the cold-mint boot must carry the profile prompt exactly once; request: {minted}"
        );
        runtime.shutdown().await;
        let (_, count) =
            wait_for_durable_document_at_least(&state, 0, "the post-reset document").await;
        assert_eq!(
            count,
            durable_count + 2,
            "the cold-mint boot must extend the head exactly, authoring nothing"
        );
    }
}

/// Destroy-deprojection regression (2026-07-31 verdict): deleting an
/// identity must remove its durable session row along with the continuity
/// record, and a COLD pod after the delete must not mint, resume, or
/// otherwise resurrect the deleted transcript - on this pod-scratch shape a
/// leftover external body is exactly what the activation mint would
/// faithfully re-seed, so the delete is where the row must die.
#[tokio::test(flavor = "multi_thread")]
async fn deleted_identity_leaves_no_durable_row_and_a_cold_boot_does_not_resurrect() {
    const TOKEN: &str = "MARKER-DELETED-RESURRECTION-7-TANGO";
    if proxied_to_memo_free_child(
        "deleted_identity_leaves_no_durable_row_and_a_cold_boot_does_not_resurrect",
    ) {
        return;
    }
    let _serial = SERIAL_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let member = id(MEMBER);

    // --- Boot 1 (scratch runtime): one turn carrying the marker, then
    // DELETE the identity through the public flow. ---
    let deleted_session_id;
    {
        let capture = CaptureClient::default();
        let runtime = boot_scratch_runtime(&state, capture.clone()).await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text(format!("Please note this token: {TOKEN}")),
            )
            .await
            .expect("send boot 1's turn");
        wait_for_turn(&capture, 1, "boot 1's turn").await;
        wait_for_durable_document_at_least(&state, 2, "boot 1's turn to commit").await;
        deleted_session_id = identity_runtime
            .status(&member)
            .await
            .expect("status after the turn")
            .session_id
            .expect("an active identity owns a durable session");
        identity_runtime
            .delete_identity(&member)
            .await
            .expect("delete the identity");
        runtime.shutdown().await;
    }

    // --- The durable row died with the record. ---
    let db = continuity_db(&state);
    {
        let store = LocalContinuityStore::open(&db).expect("reopen continuity store");
        let resolved = store
            .resolve_many(std::slice::from_ref(&member))
            .await
            .expect("resolve after delete");
        assert!(
            matches!(
                resolved.get(&member),
                Some(ContinuityResolveState::Uninitialized)
            ),
            "the deleted identity must resolve Uninitialized: {resolved:?}"
        );
        assert!(
            store
                .load_session_snapshot(&deleted_session_id)
                .await
                .expect("post-delete snapshot load")
                .is_none(),
            "the deleted identity's durable session row must not survive the delete"
        );
    }

    // --- Cold pod: nothing to mint or resume. The first send fresh-spawns
    // under a NEW session id and its request must NOT replay the deleted
    // transcript. ---
    {
        let capture = CaptureClient::default();
        let runtime = boot_scratch_runtime(&state, capture.clone()).await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &member,
                &meerkat_core::ContentInput::Text("What token did I give you earlier?".to_string()),
            )
            .await
            .expect("the post-delete cold send must fresh-spawn, not wedge or resurrect");
        wait_for_turn(&capture, 1, "the post-delete turn").await;
        let last = capture.last().expect("a post-delete request was captured");
        assert!(
            !last.contains(TOKEN),
            "a deleted transcript must not resurrect into the model's context; request: {last}"
        );
        let after = identity_runtime
            .status(&member)
            .await
            .expect("status after the post-delete send");
        assert_ne!(
            after.session_id.as_ref().map(ToString::to_string),
            Some(deleted_session_id.to_string()),
            "the cold boot must fresh-spawn a NEW session, not resume the deleted id; \
             status: {after:?}"
        );
        runtime.shutdown().await;
    }
}
