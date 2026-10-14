//! Head-canonical resume regression, with a PRECONDITION PROBE.
//!
//! # The trap this file exists to avoid
//!
//! The obvious harness — `UnifiedRuntimeBuilder::default().persistent_state(..)
//! .roster_provider(..)` — does NOT reproduce the shipped gateway. On that arm
//! the builder opens a `SqliteSessionStore` for sessions and opens the local
//! continuity store later, for identity metadata only. The gateway does the
//! opposite: it opens the identity substrate FIRST and installs its
//! `ContinuitySessionStoreAdapter` as meerkat's `SessionStore`, so heads,
//! strand rows and resume reads all ride `continuity.sqlite3` (see
//! `bin/rpc_gateway.rs`, the `identity_session_store_adapter` binding — the
//! reason the real corpus has an EMPTY meerkat `sessions` table).
//!
//! A regression test written against the wrong arm exercises neither the
//! continuity adapter nor the head-canonical path, and passes while proving
//! nothing. That already happened once. So every test here calls
//! [`assert_durable_continuity_document`] BEFORE asserting behavior: if no
//! durable document exists in the continuity file, the test fails as
//! MIS-WIRED rather than passing as green.
//!
//! `UnifiedRuntimeBuilder::continuity_from_state_dir` is the seam that makes
//! the gateway shape reachable; it opens the substrate through the same
//! `gateway_wiring::open_identity_substrate` the binaries use.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::BTreeMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use meerkat_client::{LlmError, LlmEvent, LlmRequest};
use meerkat_core::types::StopReason;
use meerkat_meta::UnifiedRuntimeBuilder;
use meerkat_meta::identity_first::contracts::RosterProvider;
use meerkat_meta::identity_first::{
    AgentAddressability, AgentIdentity, DurableAgentSpec, RosterContext, RosterError,
};
use meerkat_meta::storage_layout::MetaStorageLayout;
use meerkat_mob::{MobDefinition, ProfileName, ResumeSessionLoad};
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
id = "head-canonical-resume"

[profiles.personal]
model = "gpt-5.5"
external_addressable = true
runtime_mode = "turn_driven"

[profiles.personal.tools]
comms = true
"#;

const MEMBER: &str = "personal:alice";
const RUNTIME_INSTANCE: &str = "head-canonical-resume";

/// Serializes every test in this binary.
///
/// `steady_state_head_canonical_turns_stay_inside_the_boundary_encode_envelope`
/// reads `meerkat_core::global_session_encode_bytes()`, a
/// PROCESS-GLOBAL counter of whole-session boundary serializations.
/// Integration tests in one binary share one process, so a sibling test
/// running concurrently would bleed its own boundary encodes into the
/// measured window and turn the envelope assertion into noise. Every test in
/// this file takes this lock first; other test binaries are unaffected.
static PROCESS_COUNTER_WINDOW: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn id(name: &str) -> AgentIdentity {
    AgentIdentity::parse(name).expect("parse identity")
}

fn definition() -> MobDefinition {
    MobDefinition::from_toml(MOB_TOML).expect("parse head-canonical mob definition")
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

/// Records the serialized LLM request each turn and answers "ok" so turns
/// complete without a real provider.
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
        // meerkat 0.8.22 rejects a turn whose stream carried no normalized
        // provider accounting, so the terminal `Done` never travels alone.
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

// ---------------------------------------------------------------------------
// Gateway-shaped harness
// ---------------------------------------------------------------------------

/// Build a runtime whose session I/O rides the continuity adapter, exactly as
/// `rpc_gateway` composes it: the state directory's identity substrate is
/// opened through `gateway_wiring::open_identity_substrate` and installed as
/// BOTH the identity authority and (via the builder's
/// `ContinuitySessionStoreAdapter`) meerkat's `SessionStore`.
async fn boot(state: &Path, capture: CaptureClient) -> meerkat_meta::UnifiedRuntime {
    Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(definition())
            .persistent_state(state)
            .continuity_from_state_dir(state)
            .await
            .expect("open the state-dir identity substrate")
            .roster_provider(Arc::new(OneMemberRoster))
            .identity_runtime_instance_id(RUNTIME_INSTANCE)
            .comms(true)
            .default_llm_client(Arc::new(capture))
            .build(),
    )
    .await
    .expect("build the gateway-shaped UnifiedRuntime")
}

fn continuity_db(state: &Path) -> std::path::PathBuf {
    MetaStorageLayout::with_injected_roots(state.to_path_buf(), None)
        .continuity_db()
        .expect("resolve continuity db")
        .path
}

/// The layout-authoritative runtime store path (`runtime.sqlite` today, but
/// derived rather than hardcoded so a layout rename cannot silently turn the
/// snapshot/restore harness into a no-op).
fn runtime_db(state: &Path) -> std::path::PathBuf {
    MetaStorageLayout::with_injected_roots(state.to_path_buf(), None).runtime_db()
}

/// Every file the persistent runtime store owns: the db plus whichever
/// -wal/-shm/-journal sidecars exist right now. The runtime snapshot is the
/// OTHER document a resume can be handed, so a test that wants to prove the
/// head-canonical read path must be able to move this set around.
fn runtime_store_files(state: &Path) -> Vec<std::path::PathBuf> {
    let db = runtime_db(state);
    ["", "-wal", "-shm", "-journal"]
        .iter()
        .map(|suffix| std::path::PathBuf::from(format!("{}{suffix}", db.display())))
        .filter(|path| path.exists())
        .collect()
}

/// Copy the runtime store aside (a point-in-time runtime snapshot). Fails
/// loudly when nothing exists to copy: a silent no-op here would leave every
/// divergence assertion downstream vacuously green.
fn save_runtime_store(state: &Path, into: &Path) {
    let files = runtime_store_files(state);
    assert!(
        !files.is_empty(),
        "no runtime store files exist at {} — the snapshot harness is watching the wrong \
         location and the manufactured divergence would never arm",
        runtime_db(state).display()
    );
    std::fs::create_dir_all(into).expect("snapshot dir");
    for path in files {
        let name = path.file_name().expect("file name");
        std::fs::copy(&path, into.join(name)).expect("copy runtime store file");
    }
}

/// Restore a previously saved runtime store, replacing whatever is there now.
/// This manufactures the field shape the advisory describes: a runtime
/// snapshot that lags the durable continuity head. Fails loudly when the
/// stash restores nothing.
fn restore_runtime_store(state: &Path, from: &Path) {
    for path in runtime_store_files(state) {
        std::fs::remove_file(&path).expect("clear current runtime store file");
    }
    let store_dir = runtime_db(state)
        .parent()
        .expect("runtime db has a parent directory")
        .to_path_buf();
    let mut restored = 0_usize;
    for entry in std::fs::read_dir(from).expect("read snapshot dir") {
        let entry = entry.expect("snapshot entry");
        std::fs::copy(entry.path(), store_dir.join(entry.file_name()))
            .expect("restore runtime file");
        restored += 1;
    }
    assert!(
        restored > 0,
        "runtime store stash at {} was empty — nothing was restored, the divergence was \
         never armed",
        from.display()
    );
}

/// Message count of the runtime store's persisted session snapshot for
/// `session_id`, read from `runtime_session_snapshots` and parsed from the
/// persisted envelope (`{id, messages, ...}` — plain UTF-8 JSON per the
/// store's `JsonColumnBytes` contract). `None` when no snapshot names the
/// session.
fn runtime_snapshot_message_count(state: &Path, session_id: &str) -> Option<i64> {
    let db = runtime_db(state);
    assert!(
        db.exists(),
        "runtime store {} does not exist — cannot verify the manufactured divergence",
        db.display()
    );
    let conn = rusqlite::Connection::open(&db).expect("open runtime store");
    conn.busy_timeout(Duration::from_secs(5))
        .expect("set runtime store busy timeout");
    // meerkat 0.8.11: whole-blob boundary commits land in the
    // runtime_whole_blob_authority + runtime_whole_blob_bodies pair; the
    // runtime_session_snapshots table is the pre-0.8.11 legacy
    // representation and current commits never write it.
    assert!(
        table_exists(&conn, "runtime_whole_blob_authority"),
        "runtime store {} has no runtime_whole_blob_authority table — the divergence probe \
         is reading the wrong store",
        db.display()
    );
    let mut stmt = conn
        .prepare(
            "SELECT bodies.session_snapshot \
             FROM runtime_whole_blob_authority AS authority \
             JOIN runtime_whole_blob_bodies AS bodies \
               ON bodies.blob_sha256 = authority.blob_sha256",
        )
        .expect("prepare runtime snapshots");
    let blobs = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .expect("query runtime snapshots")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect runtime snapshots");
    for blob in blobs {
        let value: serde_json::Value =
            serde_json::from_slice(&blob).expect("parse persisted session envelope");
        if value.get("id").and_then(serde_json::Value::as_str) == Some(session_id) {
            let count = value
                .get("messages")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .expect("persisted session envelope has a messages array");
            return Some(i64::try_from(count).expect("message count fits"));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Precondition probe
// ---------------------------------------------------------------------------

/// What the continuity file durably holds for one boot.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ContinuityCensus {
    /// `continuity_session_heads` rows: (session_id, message_count).
    heads: Vec<(String, i64)>,
    /// `continuity_strand_messages` row count.
    strand_messages: i64,
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
    // The turn-boundary barrier polls this census while the runtime is LIVE;
    // tolerate a briefly-locked writer instead of panicking on SQLITE_BUSY.
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

/// THE PRECONDITION. Fails the test as MIS-WIRED (not as a behavior
/// regression) unless the continuity file holds a durable session document —
/// a `continuity_session_heads` row (head-canonical) or a `session_snapshots`
/// row (whole-blob). Returns the census so callers can assert on shape.
///
/// Without this, a harness whose session I/O silently went to
/// `sessions.sqlite3` produces a green test that proves nothing.
fn assert_durable_continuity_document(state: &Path, phase: &str) -> ContinuityCensus {
    let db = continuity_db(state);
    let census = census(&db);
    assert!(
        !census.heads.is_empty() || !census.snapshots.is_empty(),
        "PRECONDITION FAILED ({phase}): {} holds NO durable session document \
         (continuity_session_heads: 0 rows, session_snapshots: 0 rows). The harness is \
         MIS-WIRED — session I/O is not going through the continuity adapter, so nothing \
         asserted after this point is meaningful. Census: {census:?}. \
         Sibling files: {:?}",
        db.display(),
        sibling_files(state),
    );
    census
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

/// Wait until the continuity file holds exactly ONE durable head with at
/// least `floor` messages, returning `(session_id, observed_count)`.
///
/// This is the deterministic turn-boundary barrier: the durable head row is
/// written by the same boundary save that serializes the session, so once the
/// row shows the growth, that boundary's encode work has already been counted.
/// Callers thread the OBSERVED count back in as the next floor (+2 for one
/// user/assistant pair), which keeps the barrier correct even when a turn
/// durably records more than two messages.
async fn wait_for_single_head_at_least(state: &Path, floor: i64, what: &str) -> (String, i64) {
    let db = continuity_db(state);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let census = census(&db);
        if let [(session, count)] = census.heads.as_slice()
            && *count >= floor
        {
            return (session.clone(), *count);
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}: want one durable head with >= {floor} messages, \
             census: {census:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

/// On-disk byte total of one SQLite database family: the db file plus its
/// -wal/-shm/-journal sidecars.
fn sqlite_family_bytes(db: &Path) -> u64 {
    ["", "-wal", "-shm", "-journal"]
        .iter()
        .map(|suffix| std::path::PathBuf::from(format!("{}{suffix}", db.display())))
        .filter_map(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .sum()
}

/// On-disk byte total of the continuity substrate.
///
/// The continuity store runs `journal_mode=WAL`
/// (`src/identity_first/local_store.rs`), where every committed transaction
/// APPENDS its touched pages to the -wal file until a checkpoint folds them
/// into the db. Between checkpoints the growth of this total is therefore a
/// gross-durable-write-volume witness that NO writer into the store can
/// bypass: the continuity adapter, a meta checkpointer loop, and a raw
/// `serde_json::to_vec` whole-blob path all land their pages here.
fn continuity_substrate_bytes(state: &Path) -> u64 {
    sqlite_family_bytes(&continuity_db(state))
}

/// On-disk byte total of the runtime store family — the state dir's OTHER
/// per-turn writer. Meerkat's boundary contract persists a whole-session
/// runtime snapshot at run boundaries (`runtime_session_snapshots`), so this
/// one is EXPECTED to scale with document size; the envelope test prints it
/// for visibility next to the continuity numbers and never asserts on it.
fn runtime_substrate_bytes(state: &Path) -> u64 {
    sqlite_family_bytes(&runtime_db(state))
}

/// Best-effort `wal_checkpoint(TRUNCATE)` so a measured window starts from an
/// (almost) empty log. Failure is tolerated and printed: the window math
/// measures deltas, so an untruncated baseline only leaves slack — and a
/// mid-window auto-checkpoint shows up as non-positive growth, which the
/// caller's anti-vacuity guard turns into a loud failure instead of a pass.
fn truncate_continuity_wal(state: &Path) {
    let conn = rusqlite::Connection::open(continuity_db(state)).expect("open continuity db");
    conn.busy_timeout(Duration::from_secs(5))
        .expect("set continuity checkpoint busy timeout");
    let result: Result<(i64, i64, i64), rusqlite::Error> =
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        });
    eprintln!("PROBE wal_checkpoint(TRUNCATE) before the measured window: {result:?}");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Wiring proof, run first and standalone so a mis-wire is diagnosed as a
/// harness fault rather than as a resume regression: one boot + one turn
/// through the gateway-shaped harness must leave a HEAD-CANONICAL document in
/// the continuity file (`continuity_session_heads` + `continuity_strand_messages`),
/// and must NOT have written meerkat's own session database.
#[tokio::test(flavor = "multi_thread")]
async fn gateway_shaped_harness_writes_a_head_canonical_document() {
    let _counter_window = PROCESS_COUNTER_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");

    let capture = CaptureClient::default();
    let runtime = boot(&state, capture.clone()).await;
    runtime
        .identity_runtime()
        .expect("identity runtime")
        .send(
            &id(MEMBER),
            &meerkat_core::ContentInput::Text("hello".to_string()),
        )
        .await
        .expect("send turn 1");
    wait_for_turn(&capture, 1, "turn 1").await;
    sleep(Duration::from_millis(500)).await;
    runtime.shutdown().await;

    let census = assert_durable_continuity_document(&state, "boot 1 + one turn");
    eprintln!("PROBE census after boot 1 + one turn: {census:?}");
    eprintln!("PROBE state dir: {:?}", sibling_files(&state));

    assert_eq!(
        census.heads.len(),
        1,
        "the member's session must be head-canonical (one continuity_session_heads row); \
         census: {census:?}"
    );
    assert!(
        census.strand_messages > 0,
        "head-canonical persistence must have written strand rows; census: {census:?}"
    );
    assert_eq!(
        census.records.len(),
        1,
        "one identity must own one continuity record; census: {census:?}"
    );
    let (head_session, _) = &census.heads[0];
    assert_eq!(
        &census.records[0].1, head_session,
        "the identity's continuity record must name the head-canonical session; \
         census: {census:?}"
    );
    // The gateway's session authority is the continuity file. If meerkat's own
    // session database exists with content, session I/O forked in two.
    let sessions_db = state.join("sessions.sqlite3");
    assert!(
        !sessions_db.exists(),
        "session I/O must ride the continuity adapter only, but {} was created; \
         state dir: {:?}",
        sessions_db.display(),
        sibling_files(&state),
    );
}

/// The regression this harness was built for (advisory:
/// "head-canonical sessions become unresumable after reboot"): with the
/// head-canonical document as the ONLY session authority, four
/// boot/turn/reboot cycles must keep the identity ACTIVE, keep resuming the
/// same session, keep replaying the transcript, and keep GROWING the durable
/// head.
///
/// The field shape degrades across restarts rather than failing on the first
/// one (2 ACTIVE -> 1 ACTIVE/1 BROKEN -> 2 BROKEN), because each turn advances
/// the durable head while the stale document the reader is handed stays
/// pinned. One restart is therefore not enough coverage; this walks four.
#[tokio::test(flavor = "multi_thread")]
async fn head_canonical_document_survives_repeated_reboots() {
    const TOKEN: &str = "MARKER-HEAD-CANONICAL-7-ZULU";
    const CYCLES: usize = 4;
    let _counter_window = PROCESS_COUNTER_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");

    let mut session_id: Option<String> = None;
    let mut message_count: i64 = 0;

    for cycle in 1..=CYCLES {
        let capture = CaptureClient::default();
        let runtime = boot(&state, capture.clone()).await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();

        // Every reboot after the first must find the durable document already
        // there — probed BEFORE any behavior assertion, so a mis-wired harness
        // reports as mis-wired.
        if cycle > 1 {
            let census =
                assert_durable_continuity_document(&state, &format!("boot {cycle} (pre-turn)"));
            assert_eq!(
                census.heads.first().map(|(id, _)| id.clone()),
                session_id,
                "boot {cycle} must resume the SAME head-canonical session; census: {census:?}"
            );
        }

        // Form 2 of the advisory surfaces here: a resume that cannot read its
        // own durable head leaves the identity Broken instead of Active.
        let status = identity_runtime
            .status(&id(MEMBER))
            .await
            .unwrap_or_else(|e| panic!("boot {cycle}: identity status: {e}"));
        assert_eq!(
            status.state,
            meerkat_meta::identity_first::IdentityLifecycleState::Active,
            "boot {cycle}: identity must be Active after restore, got {:?} (continuity_health: \
             {:?})",
            status.state,
            status.continuity_health,
        );
        if let (Some(previous), Some(current)) = (session_id.as_ref(), status.session_id.as_ref()) {
            assert_eq!(
                &current.to_string(),
                previous,
                "boot {cycle}: restore must reuse the durable session, not create a new one"
            );
        }

        let prompt = if cycle == 1 {
            format!("Please note this token: {TOKEN}")
        } else {
            format!("Reboot {cycle}: which token did I give you?")
        };
        identity_runtime
            .send(&id(MEMBER), &meerkat_core::ContentInput::Text(prompt))
            .await
            .unwrap_or_else(|e| panic!("boot {cycle}: send turn: {e}"));
        wait_for_turn(&capture, 1, &format!("boot {cycle}'s turn")).await;

        if cycle > 1 {
            // Form 1 of the advisory surfaces here: the reader is handed a
            // stale document, so the replayed transcript is short or empty.
            let replayed = capture
                .last()
                .unwrap_or_else(|| panic!("boot {cycle}: no request captured"));
            assert!(
                replayed.contains(TOKEN),
                "boot {cycle}: the post-restart request must replay the head-canonical \
                 transcript (token {TOKEN}); resume served a stale or empty document"
            );
        }

        sleep(Duration::from_millis(500)).await;
        runtime.shutdown().await;

        let census =
            assert_durable_continuity_document(&state, &format!("boot {cycle} (post-turn)"));
        eprintln!("PROBE census after boot {cycle}: {census:?}");
        assert_eq!(
            census.heads.len(),
            1,
            "boot {cycle}: exactly one head-canonical document must exist; census: {census:?}"
        );
        let (head_session, head_messages) = census.heads[0].clone();
        match session_id.as_ref() {
            None => session_id = Some(head_session),
            Some(previous) => assert_eq!(
                &head_session, previous,
                "boot {cycle}: the durable head must stay on the same session"
            ),
        }
        assert!(
            head_messages > message_count,
            "boot {cycle}: the turn must EXTEND the durable head (message_count \
             {message_count} -> {head_messages}); a rejected save or a re-created session \
             would not grow it. Census: {census:?}"
        );
        assert!(
            census.strand_messages >= head_messages,
            "boot {cycle}: the head must be backed by at least as many strand rows as it \
             claims messages; census: {census:?}"
        );
        message_count = head_messages;
    }

    assert!(
        message_count >= (CYCLES * 2) as i64,
        "after {CYCLES} boot/turn cycles the durable head should hold at least one \
         user+assistant pair per cycle, got {message_count}"
    );
}

/// The test that actually REACHES the head-canonical read path.
///
/// The sibling four-cycle test above is necessary but NOT sufficient: with a
/// clean shutdown the persistent runtime snapshot (`runtime.sqlite`) is as
/// fresh as the durable head, so resume is served from the runtime snapshot
/// and never consults `continuity_session_heads`. Proven by construction —
/// forcing `ContinuitySessionStoreAdapter::load_head_canonical_session` to
/// decline every call leaves that test fully green.
///
/// This test manufactures the corpus shape instead: a runtime snapshot pinned
/// at an older message count while the durable head has moved on (the
/// advisory's "reader at 101 messages, head at 103"). Resume must then serve
/// the COMMITTED STORE HEAD — full transcript, save accepted, identity Active,
/// head keeps growing. On a reader that prefers the stale runtime snapshot the
/// save is refused and the identity goes Broken.
#[tokio::test(flavor = "multi_thread")]
async fn stale_runtime_snapshot_resumes_from_the_committed_head() {
    const TOKEN: &str = "MARKER-STALE-SNAPSHOT-4-XRAY";
    let _counter_window = PROCESS_COUNTER_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let stashed_runtime = temp.path().join("runtime-snapshot-at-boot-1");

    // --- Boot 1: seed the token; stash the runtime snapshot beside it -----
    let head_after_boot_1 = {
        let capture = CaptureClient::default();
        let runtime = boot(&state, capture.clone()).await;
        runtime
            .identity_runtime()
            .expect("identity runtime")
            .send(
                &id(MEMBER),
                &meerkat_core::ContentInput::Text(format!("Please note this token: {TOKEN}")),
            )
            .await
            .expect("send turn 1");
        wait_for_turn(&capture, 1, "turn 1").await;
        sleep(Duration::from_millis(500)).await;
        runtime.shutdown().await;

        let census = assert_durable_continuity_document(&state, "boot 1");
        eprintln!("PROBE census after boot 1: {census:?}");
        save_runtime_store(&state, &stashed_runtime);
        census.heads[0].1
    };

    // --- Boot 2: take another turn so the durable head moves ahead --------
    let head_after_boot_2 = {
        let capture = CaptureClient::default();
        let runtime = boot(&state, capture.clone()).await;
        runtime
            .identity_runtime()
            .expect("identity runtime")
            .send(
                &id(MEMBER),
                &meerkat_core::ContentInput::Text("Second turn, extending the head.".to_string()),
            )
            .await
            .expect("send turn 2");
        wait_for_turn(&capture, 1, "turn 2").await;
        sleep(Duration::from_millis(500)).await;
        runtime.shutdown().await;

        let census = assert_durable_continuity_document(&state, "boot 2");
        eprintln!("PROBE census after boot 2: {census:?}");
        census.heads[0].1
    };

    // --- Manufacture the divergence --------------------------------------
    let head_session =
        assert_durable_continuity_document(&state, "before runtime-snapshot rollback").heads[0]
            .0
            .clone();
    let fresh_runtime_messages = runtime_snapshot_message_count(&state, &head_session)
        .expect("boot 2's clean shutdown must leave a runtime snapshot for the session");
    restore_runtime_store(&state, &stashed_runtime);
    let census = assert_durable_continuity_document(&state, "after runtime-snapshot rollback");
    assert!(
        head_after_boot_2 > head_after_boot_1,
        "the durable head must have advanced between the two boots \
         ({head_after_boot_1} -> {head_after_boot_2}); without that there is no divergence to \
         test"
    );
    assert_eq!(
        census.heads[0].1, head_after_boot_2,
        "rolling the RUNTIME store back must not touch the continuity head; census: {census:?}"
    );
    // POSITIVE proof the divergence is armed, not assumed: the restored
    // runtime store must hold a snapshot for this session that is strictly
    // BEHIND both the snapshot it replaced and the durable continuity head.
    // Without this read, a renamed or relocated runtime store would make
    // save/restore silent no-ops and everything below would pass vacuously
    // (boot 3 would resume from boot 2's perfectly fresh snapshot).
    let stale_runtime_messages = runtime_snapshot_message_count(&state, &head_session)
        .expect("the restored runtime store must hold the boot-1 snapshot for the session");
    assert!(
        stale_runtime_messages < fresh_runtime_messages,
        "divergence NOT armed: the rollback left the runtime snapshot at \
         {stale_runtime_messages} messages, not behind the boot-2 snapshot it was supposed to \
         replace ({fresh_runtime_messages}); the restore replaced nothing"
    );
    assert!(
        stale_runtime_messages < head_after_boot_2,
        "divergence NOT armed: the restored runtime snapshot ({stale_runtime_messages} \
         messages) is not behind the durable continuity head ({head_after_boot_2})"
    );
    eprintln!(
        "PROBE divergence armed: runtime snapshot rolled back to {stale_runtime_messages} \
         messages (boot-2 snapshot held {fresh_runtime_messages}), durable continuity head at \
         {head_after_boot_2} messages"
    );

    // --- Boot 3: resume must serve the committed head, not the stale one --
    {
        let capture = CaptureClient::default();
        let runtime = boot(&state, capture.clone()).await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();

        let status = identity_runtime
            .status(&id(MEMBER))
            .await
            .expect("identity status");
        assert_eq!(
            status.state,
            meerkat_meta::identity_first::IdentityLifecycleState::Active,
            "a stale runtime snapshot must not brick restore; state {:?}, continuity_health {:?}",
            status.state,
            status.continuity_health,
        );

        identity_runtime
            .send(
                &id(MEMBER),
                &meerkat_core::ContentInput::Text("Which token did I give you?".to_string()),
            )
            .await
            .expect("send turn 3");
        wait_for_turn(&capture, 1, "the post-rollback turn").await;

        let replayed = capture.last().expect("a request was captured");
        assert!(
            replayed.contains(TOKEN),
            "resume must replay the transcript from the committed head (token {TOKEN})"
        );
        // The stale runtime snapshot predates turn 2. If resume were served
        // from it, the request would carry the boot-1 transcript only.
        assert!(
            replayed.contains("Second turn, extending the head."),
            "resume served the STALE RUNTIME SNAPSHOT: the request is missing turn 2, which is \
             durably recorded in the continuity head. This is the advisory's Form 1."
        );

        sleep(Duration::from_millis(500)).await;
        runtime.shutdown().await;

        let final_census =
            assert_durable_continuity_document(&state, "boot 3 (post-turn, post-rollback)");
        eprintln!("PROBE census after boot 3: {final_census:?}");
        assert!(
            final_census.heads[0].1 > head_after_boot_2,
            "the post-rollback turn must EXTEND the committed head ({head_after_boot_2} -> {}); \
             a save refused as 'behind the persisted head' would leave it unchanged",
            final_census.heads[0].1
        );
    }
}

/// The second 0.8.6 field failure, previously with ZERO coverage: an ARCHIVED
/// durable session must surface as archived through the typed resume seam —
/// NEVER as missing.
///
/// In the field, hosts asking for a retired member's session were told
/// "missing durable session snapshot", concluded the transcript was gone, and
/// rotated the identity onto a fresh session — silently orphaning an intact,
/// durably preserved document. The typed seam
/// (`MobSessionService::load_session_for_resume`, meerkat 0.8.9) exists so
/// "archived" can never again collapse into "absent".
///
/// The archive here runs through the same path the shipped gateway uses:
/// `IdentityRuntime::retire` -> session-bridge `retire_member` -> mob machine
/// disposal -> `archive_with_mob_lifecycle_authority_under_runtime_turn_boundary`.
/// Three levels are then pinned, in order:
///   1. service: the resume-seam read is `Revivable`/`ArchivedNotRevivable`,
///      never `Absent`, and a `Revivable` document carries the archived
///      terminal;
///   2. reconcile: the production revival shape is identity-runtime
///      reconciliation over a reboot — the roster still lists the identity,
///      so startup re-registers it (advancing generation/fencing) BEFORE any
///      member spawn. After the reboot, EITHER the identity comes back
///      Active bound to the SAME archived session, whose next turn replays
///      and extends the original head, OR the revival is refused typed and
///      the identity parks non-Active with its continuity binding preserved.
///      Rotation to a fresh session, or the session reading as absent, fails
///      the pin either way. Two shapes that do NOT work were confirmed
///      against this suite and are recorded here deliberately: a resume
///      spawned under a foreign member identity is refused by the comms
///      binding ("persisted comms_name ... does not match current mob
///      identity"), and a raw `mob_handle().spawn_spec(..)` resume under the
///      correct identity is refused by the continuity adapter's registration
///      guard ("session ... was unregistered from identity runtime state") —
///      a durable-identity revival cannot bypass identity registration;
///   3. durable: after shutdown the continuity file still holds the same
///      single document — archival preserves, it neither deletes nor re-keys.
#[tokio::test(flavor = "multi_thread")]
async fn archived_session_reads_as_archived_never_as_missing() {
    let _counter_window = PROCESS_COUNTER_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");

    // --- Boot 1: one turn, archive through retire, service-level pin --------
    let (head_session, head_messages) = {
        let capture = CaptureClient::default();
        let runtime = boot(&state, capture.clone()).await;
        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime")
            .clone();
        identity_runtime
            .send(
                &id(MEMBER),
                &meerkat_core::ContentInput::Text("hello before archive".to_string()),
            )
            .await
            .expect("send turn 1");
        wait_for_turn(&capture, 1, "turn 1").await;
        let (head_session, head_messages) =
            wait_for_single_head_at_least(&state, 2, "turn 1's boundary save").await;

        // Probed BEFORE any behavior assertion, per this file's contract: a
        // mis-wired harness must report as mis-wired, not as an archive bug.
        let pre_archive_census = assert_durable_continuity_document(&state, "pre-archive");
        eprintln!("PROBE census before archive: {pre_archive_census:?}");
        let session_id = meerkat_core::types::SessionId::parse(&head_session)
            .expect("durable head session id parses");

        identity_runtime
            .retire(&id(MEMBER))
            .await
            .unwrap_or_else(|e| panic!("retire the member (archives its durable session): {e}"));

        // --- 1. Service level ------------------------------------------------
        let service = runtime
            .mob_runtime()
            .session_service()
            .cloned()
            .expect("the gateway-shape runtime composes a mob session service");
        let load = service
            .load_session_for_resume(&session_id)
            .await
            .expect("the resume-seam read must not fault");
        match &load {
            ResumeSessionLoad::Revivable(session) => {
                assert_eq!(
                    session.id(),
                    &session_id,
                    "a revivable read must return the archived session"
                );
                assert!(
                    session
                        .lifecycle_terminal()
                        .is_some_and(meerkat_core::SessionLifecycleTerminal::is_archived),
                    "retire must stamp the archived terminal on the durable document; a \
                     revivable read without it means the mob archive authority never archived \
                     (host-owned disposal branch?)"
                );
            }
            ResumeSessionLoad::ArchivedNotRevivable { runtime_state } => {
                eprintln!(
                    "PROBE archived document not revivable from runtime state {runtime_state:?}"
                );
            }
            ResumeSessionLoad::Active(_) => panic!(
                "retire left the durable session readable as ACTIVE: the archive protocol \
                 never stamped the terminal"
            ),
            ResumeSessionLoad::Absent => panic!(
                "archived session must never read as missing: load_session_for_resume returned \
                 Absent for session {session_id}, which was just archived with {head_messages} \
                 durably recorded messages (the 0.8.6 field failure — hosts treated the \
                 preserved transcript as lost)"
            ),
            // `ResumeSessionLoad` is #[non_exhaustive]: a future variant must
            // fail this pin loudly for reclassification, never pass silently.
            other => panic!(
                "load_session_for_resume returned an unclassified shape for the archived \
                 session {session_id}: {other:?} — extend this pin deliberately"
            ),
        }

        runtime.shutdown().await;
        (head_session, head_messages)
    };

    // --- 2. Reconcile level (the production revival shape) -------------------
    // Reboot over the same state dir: the roster still lists alice, so
    // startup reconciliation re-registers the identity and drives whatever
    // revival the machine permits. Both legal outcomes pin the field
    // failure; a rotated fresh session or an absent read fails.
    let capture = CaptureClient::default();
    let runtime = boot(&state, capture.clone()).await;
    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime")
        .clone();
    let status = identity_runtime
        .status(&id(MEMBER))
        .await
        .unwrap_or_else(|e| panic!("post-reboot identity status: {e}"));
    eprintln!(
        "PROBE post-reboot identity state {:?} (session {:?}, continuity_health {:?})",
        status.state, status.session_id, status.continuity_health
    );
    if status.state == meerkat_meta::identity_first::IdentityLifecycleState::Active {
        // (a) Reconcile revived the archived document. The binding must name
        // the ORIGINAL session, and the next turn must replay + extend that
        // head — a fresh rotated session would satisfy neither.
        assert_eq!(
            status.session_id.as_ref().map(ToString::to_string),
            Some(head_session.clone()),
            "reconcile revival must bind the identity to the archived session; a DIFFERENT \
             session here is rotation over an archived-but-intact document — the 0.8.6 field \
             failure"
        );
        identity_runtime
            .send(
                &id(MEMBER),
                &meerkat_core::ContentInput::Text("which session is this?".to_string()),
            )
            .await
            .unwrap_or_else(|e| panic!("post-revival turn: send: {e}"));
        wait_for_turn(&capture, 1, "the post-revival turn").await;
        let (revived_head, revived_messages) = wait_for_single_head_at_least(
            &state,
            head_messages + 2,
            "the post-revival boundary save",
        )
        .await;
        assert_eq!(
            revived_head, head_session,
            "the post-revival turn must extend the ORIGINAL durable head"
        );
        let replayed = capture.last().expect("post-revival request captured");
        assert!(
            replayed.contains("hello before archive"),
            "the revived session must replay the archived transcript"
        );
        eprintln!("PROBE revived head extended to {revived_messages} messages");
    } else {
        // (b) Reconcile refused the revival (typed). The identity parks
        // non-Active; what it must NOT do is rotate — the continuity binding
        // has to keep naming the archived session.
        if let Some(bound) = status.session_id.as_ref() {
            assert_eq!(
                bound.to_string(),
                head_session,
                "a refused revival must preserve the archived continuity binding, not rotate it"
            );
        }
        let census_after_refusal = census(&continuity_db(&state));
        assert!(
            census_after_refusal
                .records
                .iter()
                .all(|(_, session)| session == &head_session),
            "a refused revival must leave every continuity record on the archived session; \
             census: {census_after_refusal:?}"
        );
    }

    runtime.shutdown().await;

    // --- 3. Durable level ---------------------------------------------------
    let after = census(&continuity_db(&state));
    eprintln!("PROBE census after archive + reconcile: {after:?}");
    let mut durable_ids: Vec<String> = after
        .heads
        .iter()
        .map(|(session, _)| session.clone())
        .chain(after.snapshots.iter().cloned())
        .collect();
    durable_ids.sort();
    durable_ids.dedup();
    assert!(
        durable_ids.iter().all(|id| id == &head_session),
        "the archive disposal or the reconcile manufactured a fresh durable session; \
         only {head_session} may exist. Census: {after:?}"
    );
    assert!(
        after.strand_messages > 0 || !after.snapshots.is_empty(),
        "the archived transcript must remain durably represented in the continuity file \
         ('the transcript is intact and preserved'); census: {after:?}"
    );
}

/// Flat-curve acceptance at the meta boundary (the Class-2 field curve:
/// 14 MB documents cost ~60 s and 94 MB ~180 s PER one-word turn, because the
/// boundary re-serialized whole documents every turn).
///
/// PRIMARY signal: growth of the continuity substrate files themselves
/// (db + WAL/journal sidecars, via [`continuity_substrate_bytes`]) across the
/// measured turns, with the WAL truncated at the window's start. In WAL mode
/// every committed transaction appends its touched pages to the log, so this
/// growth is gross durable write volume that NO writer into the store can
/// bypass — the continuity adapter, a meta checkpointer loop, and a raw
/// `serde_json::to_vec` whole-blob path all land their pages there. This is
/// the acceptance signal.
///
/// SECONDARY signal: `meerkat_core::global_session_encode_bytes()`
/// — whole-session serializations minted by core's own seams only
/// (`BoundSessionCommit::sealed` in `meerkat-core/src/lifecycle/core_executor.rs`
/// and the stamped-snapshot install in `meerkat-session/src/persistent.rs`).
/// It CANNOT see adapter- or meta-side serializations, so it must never be
/// the acceptance signal; it is kept to isolate core's own contribution when
/// the primary envelope trips.
///
/// SCALING window (the proof the envelope measures overhead, not document
/// size): after the small-document window, the transcript is inflated past
/// the envelope itself (two stuffing turns of 160 KiB each, > 256 KiB of
/// document) and two more small turns are measured. O(1) transactional
/// overhead passes unchanged; an O(document) writer cannot stay under an
/// envelope the document itself now exceeds — a compile-time assertion pins
/// that geometry. The runtime store's growth is printed beside it because
/// meerkat's boundary contract DOES persist a whole-session runtime snapshot
/// per run boundary — expected to scale, deliberately not asserted, and the
/// reason the continuity substrate is measured separately at all. The
/// core-seam counter is likewise not asserted in this window: one whole-
/// document encode per boundary is core's stated contract, so it scales with
/// the document by design.
///
/// Calibration (first honest run, 2026-07-28, small-document window):
/// 72,116 B/turn of continuity substrate growth = 17-18 WAL pages per turn,
/// against an 18 KiB document and 34 KiB request proxy. That is FIXED
/// page-granularity transaction overhead, not document bytes — a turn
/// commits several continuity transactions (strand append, head save,
/// continuity-record/checkpoint update), and every commit re-logs each
/// touched B-tree page plus the db header page as whole 4 KiB WAL frames.
/// The scaling window proves the O(1) claim; the 256 KiB envelope (~60
/// pages) keeps ~3.5x headroom over that floor while still tripping any
/// O(document) writer once a document exceeds it. Follow-up candidate (not
/// a test concern): batching the per-turn continuity mutations into fewer
/// transactions would shave the repeated header-page frames. A zero-growth
/// window fails loudly (anti-vacuity guard) instead of passing, and the
/// PROBE lines print every per-turn number next to a serialized-transcript
/// proxy for future recalibration.
#[tokio::test(flavor = "multi_thread")]
async fn steady_state_head_canonical_turns_stay_inside_the_boundary_encode_envelope() {
    const WARMUP_TURNS: usize = 2;
    const MEASURED_TURNS: usize = 2;
    const STUFFING_TURNS: usize = 2;
    const STUFFING_BYTES_PER_TURN: usize = 160 * 1024;
    const PER_TURN_DURABLE_ENVELOPE_BYTES: i64 = 256 * 1024;
    const PER_TURN_ENCODE_ENVELOPE_BYTES: u64 = 256 * 1024;
    // The scaling window is only a proof if the inflated document exceeds
    // the envelope: below that, an O(document) writer would pass it.
    const _: () = assert!(
        STUFFING_TURNS * STUFFING_BYTES_PER_TURN > PER_TURN_DURABLE_ENVELOPE_BYTES as usize
    );

    let _counter_window = PROCESS_COUNTER_WINDOW.lock().await;
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");

    let capture = CaptureClient::default();
    let runtime = boot(&state, capture.clone()).await;
    let identity_runtime = runtime
        .identity_runtime()
        .expect("identity runtime")
        .clone();

    let mut head_session: Option<String> = None;
    let mut head_floor: i64 = 0;
    for turn in 1..=WARMUP_TURNS {
        identity_runtime
            .send(
                &id(MEMBER),
                &meerkat_core::ContentInput::Text(format!("warm-up turn {turn}")),
            )
            .await
            .unwrap_or_else(|e| panic!("warm-up turn {turn}: send: {e}"));
        wait_for_turn(&capture, turn, &format!("warm-up turn {turn}")).await;
        let (session, observed) = wait_for_single_head_at_least(
            &state,
            head_floor + 2,
            &format!("warm-up turn {turn}'s boundary save"),
        )
        .await;
        match head_session.as_ref() {
            None => head_session = Some(session),
            Some(previous) => assert_eq!(
                &session, previous,
                "steady-state turns must stay on one durable head"
            ),
        }
        head_floor = observed;
    }

    // Probed BEFORE the measured window, per this file's contract: if session
    // I/O is not riding the continuity adapter, the measurement below would
    // be of the wrong composition entirely.
    let census = assert_durable_continuity_document(&state, "after warm-up");
    eprintln!("PROBE census after warm-up: {census:?}");

    // Start the measured window from a truncated WAL so substrate growth over
    // the window is the gross write volume of exactly these turns.
    truncate_continuity_wal(&state);
    let substrate_baseline = continuity_substrate_bytes(&state);
    let encode_baseline = meerkat_core::global_session_encode_bytes();
    for offset in 1..=MEASURED_TURNS {
        let turn = WARMUP_TURNS + offset;
        identity_runtime
            .send(
                &id(MEMBER),
                &meerkat_core::ContentInput::Text(format!("measured turn {turn}")),
            )
            .await
            .unwrap_or_else(|e| panic!("measured turn {turn}: send: {e}"));
        wait_for_turn(&capture, turn, &format!("measured turn {turn}")).await;
        let (session, observed) = wait_for_single_head_at_least(
            &state,
            head_floor + 2,
            &format!("measured turn {turn}'s boundary save"),
        )
        .await;
        assert_eq!(
            Some(&session),
            head_session.as_ref(),
            "measured turns must stay on the warm-up's durable head"
        );
        head_floor = observed;
    }
    let substrate_grown = i64::try_from(continuity_substrate_bytes(&state))
        .expect("substrate size fits")
        - i64::try_from(substrate_baseline).expect("substrate baseline fits");
    let encoded = meerkat_core::global_session_encode_bytes() - encode_baseline;
    let measured_turns = i64::try_from(MEASURED_TURNS).expect("measured turn count fits");
    let per_turn_durable = substrate_grown / measured_turns;
    let per_turn_encoded =
        encoded / u64::try_from(MEASURED_TURNS).expect("measured turn count fits");
    let transcript_proxy_bytes = capture.last().map_or(0, |request| request.len());
    eprintln!(
        "PROBE durable substrate bytes: {substrate_baseline}B + {substrate_grown}B over \
         {MEASURED_TURNS} measured turns ({per_turn_durable}B/turn); core-seam encode bytes: \
         {encoded}B ({per_turn_encoded}B/turn); serialized LLM-request proxy for the whole \
         transcript: {transcript_proxy_bytes}B"
    );

    // Anti-vacuity guard: two committed turns MUST have written durable
    // bytes. Zero or negative growth means this probe is watching the wrong
    // files, or a mid-window checkpoint reset the log — either way the
    // envelope below would be meaningless, so fail instead of passing.
    assert!(
        substrate_grown > 0,
        "the two measured turns grew the continuity substrate by {substrate_grown}B — the \
         measurement is vacuous (wrong files, or a mid-window WAL checkpoint); fix the probe \
         before trusting the envelope"
    );
    // PRIMARY: the envelope on evidence no writer into the store can bypass.
    assert!(
        per_turn_durable < PER_TURN_DURABLE_ENVELOPE_BYTES,
        "steady-state head-canonical turns wrote {per_turn_durable}B/turn into the continuity \
         substrate (grew {substrate_grown}B over {MEASURED_TURNS} turns; whole-transcript \
         proxy {transcript_proxy_bytes}B) — over the {PER_TURN_DURABLE_ENVELOPE_BYTES}B \
         acceptance envelope. That is the field regression's shape: something is durably \
         re-writing whole documents per turn."
    );
    // SECONDARY: core's own encode seams only (see the doc comment for what
    // this counter cannot see). Kept to attribute a primary trip, never as
    // the acceptance signal.
    assert!(
        per_turn_encoded < PER_TURN_ENCODE_ENVELOPE_BYTES,
        "meerkat-core's own boundary seams serialized {per_turn_encoded}B/turn of whole-session \
         encodes (measured {encoded}B over {MEASURED_TURNS} turns) — over the \
         {PER_TURN_ENCODE_ENVELOPE_BYTES}B envelope on the core seams alone"
    );

    // --- Scaling window: overhead must be O(1) in document size -------------
    // Inflate the transcript past the envelope, then re-measure two small
    // turns. See the doc comment: an O(document) writer cannot stay under an
    // envelope the document itself now exceeds.
    let mut turn = WARMUP_TURNS + MEASURED_TURNS;
    for stuffing in 1..=STUFFING_TURNS {
        turn += 1;
        identity_runtime
            .send(
                &id(MEMBER),
                &meerkat_core::ContentInput::Text(format!(
                    "stuffing turn {stuffing}: {}",
                    "x".repeat(STUFFING_BYTES_PER_TURN)
                )),
            )
            .await
            .unwrap_or_else(|e| panic!("stuffing turn {stuffing}: send: {e}"));
        wait_for_turn(&capture, turn, &format!("stuffing turn {stuffing}")).await;
        let (session, observed) = wait_for_single_head_at_least(
            &state,
            head_floor + 2,
            &format!("stuffing turn {stuffing}'s boundary save"),
        )
        .await;
        assert_eq!(
            Some(&session),
            head_session.as_ref(),
            "stuffing turns must stay on the durable head"
        );
        head_floor = observed;
    }

    truncate_continuity_wal(&state);
    let inflated_baseline = continuity_substrate_bytes(&state);
    let runtime_store_baseline = runtime_substrate_bytes(&state);
    let inflated_encode_baseline = meerkat_core::global_session_encode_bytes();
    for _ in 1..=MEASURED_TURNS {
        turn += 1;
        identity_runtime
            .send(
                &id(MEMBER),
                &meerkat_core::ContentInput::Text(format!("post-stuff measured turn {turn}")),
            )
            .await
            .unwrap_or_else(|e| panic!("post-stuff measured turn {turn}: send: {e}"));
        wait_for_turn(&capture, turn, &format!("post-stuff measured turn {turn}")).await;
        let (session, observed) = wait_for_single_head_at_least(
            &state,
            head_floor + 2,
            &format!("post-stuff measured turn {turn}'s boundary save"),
        )
        .await;
        assert_eq!(
            Some(&session),
            head_session.as_ref(),
            "post-stuff measured turns must stay on the durable head"
        );
        head_floor = observed;
    }
    let inflated_grown = i64::try_from(continuity_substrate_bytes(&state))
        .expect("inflated substrate size fits")
        - i64::try_from(inflated_baseline).expect("inflated baseline fits");
    let runtime_store_grown = i64::try_from(runtime_substrate_bytes(&state))
        .expect("runtime store size fits")
        - i64::try_from(runtime_store_baseline).expect("runtime store baseline fits");
    let inflated_encoded = meerkat_core::global_session_encode_bytes() - inflated_encode_baseline;
    let inflated_per_turn = inflated_grown / measured_turns;
    let inflated_proxy_bytes = capture.last().map_or(0, |request| request.len());
    eprintln!(
        "PROBE inflated-document window: continuity substrate grew {inflated_grown}B over \
         {MEASURED_TURNS} turns ({inflated_per_turn}B/turn; small-document window was \
         {per_turn_durable}B/turn); runtime store grew {runtime_store_grown}B (whole-snapshot \
         boundary contract, expected to scale); core-seam encode bytes {inflated_encoded}B \
         (contractual whole-document encode, expected to scale); transcript proxy now \
         {inflated_proxy_bytes}B"
    );

    runtime.shutdown().await;

    // The stuffing must actually have inflated the replayed document, or the
    // scaling proof below proves nothing.
    assert!(
        inflated_proxy_bytes > STUFFING_TURNS * STUFFING_BYTES_PER_TURN,
        "the stuffing turns did not inflate the replayed transcript (proxy \
         {inflated_proxy_bytes}B for {STUFFING_TURNS} x {STUFFING_BYTES_PER_TURN}B of \
         stuffing) — the scaling window is vacuous"
    );
    assert!(
        inflated_grown > 0,
        "the post-stuff measured turns grew the continuity substrate by {inflated_grown}B — \
         the scaling measurement is vacuous (wrong files, or a mid-window WAL checkpoint)"
    );
    // The O(1) claim itself: per-turn durable growth on a >256 KiB document
    // must stay inside the same envelope the small document used.
    assert!(
        inflated_per_turn < PER_TURN_DURABLE_ENVELOPE_BYTES,
        "per-turn durable growth SCALED with document size: {inflated_per_turn}B/turn against \
         a ~{}KiB document (small-document window: {per_turn_durable}B/turn). The continuity \
         boundary is durably re-writing whole documents per turn — the field regression's \
         shape, hidden behind a small fixture until now.",
        (STUFFING_TURNS * STUFFING_BYTES_PER_TURN) / 1024
    );
}
