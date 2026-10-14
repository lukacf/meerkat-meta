#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
//! meta-repair tool-result bounding contract (motivating case, 2026-08-14:
//! a durable member strand of 820 messages / 3.75 MB was 72.4%
//! tool_results - the System-row modes could recover almost none of it).
//!
//! The bounding mode edits ToolResults rows IN PLACE through the same typed
//! rewrite door as the System-row modes: rows are never removed (removing one
//! would orphan the paired assistant tool_use block), content over the bound
//! is replaced by a typed truncation/elision marker that preserves the
//! tool_use_id and is_error provenance, and System/User rows are never
//! touched. Dry-run is the default; the durable strand mutates only under
//! `--apply`. Exit codes: 0 clean, 1 refusals, 2 usage errors.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use meerkat_meta::identity_first::{
    AgentIdentity, AgentRuntimeId, CheckpointVersion, ContinuityGeneration, ContinuityRecord,
    ContinuitySessionStoreAdapter, ContinuityStore, FencingToken, LocalContinuityStore,
    SessionRuntimeState,
};

const MARKER_PREFIX: &str = "[meta-repair:tool-result-";

fn continuity_db(dir: &Path) -> PathBuf {
    dir.join("continuity.sqlite3")
}

/// Seed one bound durable session with the given transcript and return its id.
async fn seed_session(db: &Path, messages: Vec<meerkat_core::Message>) -> String {
    let store = Arc::new(LocalContinuityStore::open(db).expect("open continuity store"));
    let mut session = meerkat_core::Session::new();
    for message in messages {
        session.push(message);
    }
    let identity = AgentIdentity::parse("test:bulky").expect("identity");
    let token = FencingToken::new(1);
    store
        .upsert_continuity_record(
            &ContinuityRecord {
                identity: identity.clone(),
                agent_runtime_id: AgentRuntimeId::parse("rt:test:bulky:0").expect("runtime id"),
                session_id: session.id().clone(),
                generation: ContinuityGeneration::new(0),
                checkpoint_version: CheckpointVersion::new(0),
            },
            token,
        )
        .await
        .expect("seed continuity record");
    let adapter = ContinuitySessionStoreAdapter::new(store);
    adapter
        .register_session(
            session.id(),
            SessionRuntimeState {
                identity,
                generation: ContinuityGeneration::new(0),
                fencing_token: token,
                checkpoint_version: CheckpointVersion::new(0),
            },
        )
        .await
        .expect("register seed writer");
    meerkat::SessionStore::save(&adapter, &session)
        .await
        .expect("save seeded session");
    session.id().to_string()
}

/// Reload the durable HEAD exactly as a resume would (register + load); the
/// successful load IS the post-repair "session loads cleanly" validation.
async fn load_head(db: &Path, sid: &str) -> Vec<meerkat_core::Message> {
    let store: Arc<dyn ContinuityStore> =
        Arc::new(LocalContinuityStore::open(db).expect("reopen continuity store"));
    let session_id = meerkat_core::types::SessionId::parse(sid).expect("session id");
    let (record, fencing_token, fence_current) = store
        .resolve_record_by_session(&session_id)
        .await
        .expect("resolve record")
        .expect("record binds session");
    let adapter = ContinuitySessionStoreAdapter::new(Arc::clone(&store));
    adapter
        .register_session(
            &session_id,
            SessionRuntimeState {
                identity: record.identity.clone(),
                generation: record.generation,
                fencing_token,
                checkpoint_version: fence_current,
            },
        )
        .await
        .expect("register reader");
    let session = meerkat::SessionStore::load(&adapter, &session_id)
        .await
        .expect("post-repair load must succeed")
        .expect("durable session exists");
    session.messages().to_vec()
}

struct RepairRun {
    code: i32,
    report: serde_json::Value,
    stderr: String,
}

fn run_repair(db: &Path, sid: &str, extra: &[&str]) -> RepairRun {
    let output = Command::new(env!("CARGO_BIN_EXE_meta_repair"))
        .args(["--db", db.to_str().expect("db path"), "--session", sid])
        .args(extra)
        .output()
        .expect("spawn meta-repair");
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let report = if output.stdout.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "stdout is not a JSON report ({error}): {}",
                String::from_utf8_lossy(&output.stdout)
            )
        })
    };
    RepairRun {
        code: output.status.code().unwrap_or(-1),
        report,
        stderr,
    }
}

fn tool_results(message: &meerkat_core::Message) -> &[meerkat_core::types::ToolResult] {
    match message {
        meerkat_core::Message::ToolResults { results, .. } => results,
        other => panic!("expected a ToolResults row, got {other:?}"),
    }
}

fn as_json(messages: &[meerkat_core::Message]) -> Vec<serde_json::Value> {
    messages
        .iter()
        .map(|m| serde_json::to_value(m).expect("serialize message"))
        .collect()
}

/// A multibyte payload whose byte bound never falls on a char boundary,
/// so truncation must floor to a valid UTF-8 cut instead of panicking.
fn big_multibyte(bytes: usize) -> String {
    "µ".repeat(bytes / 2)
}

/// The assistant tool-use row a faithful transcript pairs with each
/// ToolResults row (the rewrite door's shape validator enforces the pairing).
fn assistant_tool_use(ids: &[&str]) -> meerkat_core::Message {
    let blocks = ids
        .iter()
        .map(|id| meerkat_core::types::AssistantBlock::ToolUse {
            id: (*id).to_string(),
            name: "bulky_tool".to_string(),
            args: serde_json::value::to_raw_value(&serde_json::json!({})).expect("args"),
            meta: None,
        })
        .collect();
    meerkat_core::Message::BlockAssistant(meerkat_core::types::BlockAssistantMessage::new(
        blocks,
        meerkat_core::types::StopReason::ToolUse,
    ))
}

fn bulky_fixture() -> Vec<meerkat_core::Message> {
    vec![
        meerkat_core::Message::System(meerkat_core::types::SystemMessage::new(
            "you are a bulky test member",
        )),
        meerkat_core::Message::User(meerkat_core::types::UserMessage::text(
            "please do the thing",
        )),
        assistant_tool_use(&["tool-big", "tool-small"]),
        meerkat_core::Message::tool_results(vec![
            meerkat_core::types::ToolResult::new(
                "tool-big".to_string(),
                big_multibyte(64 * 1024),
                false,
            ),
            meerkat_core::types::ToolResult::new("tool-small".to_string(), "small ok".into(), true),
        ]),
        meerkat_core::Message::User(meerkat_core::types::UserMessage::text("tail user row")),
    ]
}

#[tokio::test]
async fn truncate_dry_run_plans_and_apply_bounds_oversized_tool_results() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = continuity_db(dir.path());
    let sid = seed_session(&db, bulky_fixture()).await;
    let seeded = as_json(&load_head(&db, &sid).await);

    // Dry run is the default: full accounting, durable strand untouched.
    let dry = run_repair(&db, &sid, &["--truncate-tool-results", "1023"]);
    assert_eq!(dry.code, 0, "dry-run must exit 0; stderr: {}", dry.stderr);
    assert_eq!(dry.report["mode"], "truncate_tool_results_1023");
    assert_eq!(dry.report["applied"], false);
    let session = &dry.report["sessions"][0];
    assert_eq!(session["outcome"], "dry_run");
    assert_eq!(session["rows_before"], 5);
    assert_eq!(
        session["rows_after"], 5,
        "bounding edits rows in place; it never removes one (tool_use pairing)"
    );
    assert_eq!(session["system_rows_before"], 1);
    assert_eq!(
        session["system_rows_after"], 1,
        "System rows are out of scope for the tool-result mode"
    );
    assert_eq!(session["tool_result_messages"], 1);
    assert_eq!(
        session["tool_results_bounded"], 1,
        "only the oversized entry is planned; the small one stays"
    );
    let tool_bytes_before = session["tool_result_bytes_before"]
        .as_u64()
        .expect("tool bytes before");
    let tool_bytes_after = session["tool_result_bytes_after"]
        .as_u64()
        .expect("tool bytes after");
    assert!(
        tool_bytes_before > 64 * 1024,
        "accounting must see the oversized entry ({tool_bytes_before})"
    );
    assert!(
        tool_bytes_after < 4096,
        "the plan bounds the bulk ({tool_bytes_after})"
    );
    assert_eq!(
        as_json(&load_head(&db, &sid).await),
        seeded,
        "a dry run must not touch the durable strand"
    );

    // Apply through the same lever.
    let applied = run_repair(&db, &sid, &["--truncate-tool-results", "1023", "--apply"]);
    assert_eq!(
        applied.code, 0,
        "apply must exit 0; stderr: {}",
        applied.stderr
    );
    assert_eq!(applied.report["sessions"][0]["outcome"], "applied");
    assert_eq!(applied.report["refused"], 0);

    let healed = load_head(&db, &sid).await;
    assert_eq!(healed.len(), 5, "row count survives the repair");
    let healed_json = as_json(&healed);
    assert_eq!(
        healed_json[0], seeded[0],
        "the System row must be byte-identical after a tool-result repair"
    );
    assert_eq!(healed_json[1], seeded[1], "User rows are never touched");
    assert_eq!(
        healed_json[2], seeded[2],
        "the paired assistant tool-use row is never touched"
    );
    assert_eq!(healed_json[4], seeded[4], "User rows are never touched");

    let results = tool_results(&healed[3]);
    assert_eq!(results.len(), 2, "entries survive; only content is bounded");
    assert_eq!(results[0].tool_use_id, "tool-big", "provenance survives");
    assert!(!results[0].is_error);
    let truncated = results[0].text_content();
    assert!(
        truncated.starts_with(MARKER_PREFIX),
        "the truncated entry leads with the typed marker: {truncated:.120}"
    );
    assert!(
        truncated.contains("truncated") && truncated.contains("original_bytes="),
        "the marker states what happened and how much was there: {truncated:.120}"
    );
    assert!(
        truncated.contains('µ'),
        "a kept prefix of the original content survives under the bound"
    );
    assert!(
        truncated.len() < 2048,
        "the entry is actually bounded ({} bytes)",
        truncated.len()
    );
    assert_eq!(results[1].tool_use_id, "tool-small");
    assert_eq!(
        results[1].text_content(),
        "small ok",
        "entries under the bound are untouched"
    );
    assert!(results[1].is_error, "is_error provenance is preserved");

    // Idempotency: a second apply finds nothing left to bound.
    let again = run_repair(&db, &sid, &["--truncate-tool-results", "1023", "--apply"]);
    assert_eq!(again.code, 0);
    assert_eq!(
        again.report["sessions"][0]["outcome"], "nothing_to_do",
        "already-bounded entries are never re-ground"
    );
}

#[tokio::test]
async fn drop_older_than_elides_only_rows_beyond_the_tail_window() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = continuity_db(dir.path());
    // Positions: 0 System, 1 Assistant(tool-old), 2 ToolResults(old), 3 User,
    // 4 Assistant(tool-recent), 5 ToolResults(recent), 6 User. Distance from
    // tail: old = 5, recent = 2; bound 2 keeps recent and elides old.
    let sid = seed_session(
        &db,
        vec![
            meerkat_core::Message::System(meerkat_core::types::SystemMessage::new("prompt")),
            assistant_tool_use(&["tool-old"]),
            meerkat_core::Message::tool_results(vec![meerkat_core::types::ToolResult::new(
                "tool-old".to_string(),
                "old bulky output ".repeat(64),
                false,
            )]),
            meerkat_core::Message::User(meerkat_core::types::UserMessage::text("middle")),
            assistant_tool_use(&["tool-recent"]),
            meerkat_core::Message::tool_results(vec![meerkat_core::types::ToolResult::new(
                "tool-recent".to_string(),
                "recent output".to_string(),
                false,
            )]),
            meerkat_core::Message::User(meerkat_core::types::UserMessage::text("tail")),
        ],
    )
    .await;
    let seeded = as_json(&load_head(&db, &sid).await);

    let applied = run_repair(
        &db,
        &sid,
        &["--drop-tool-results-older-than", "2", "--apply"],
    );
    assert_eq!(applied.code, 0, "stderr: {}", applied.stderr);
    assert_eq!(applied.report["mode"], "drop_tool_results_older_than_2");
    let session = &applied.report["sessions"][0];
    assert_eq!(session["outcome"], "applied");
    assert_eq!(
        session["rows_after"], 7,
        "rows are elided in place, not removed"
    );
    assert_eq!(session["tool_results_bounded"], 1);

    let healed = load_head(&db, &sid).await;
    let healed_json = as_json(&healed);
    assert_eq!(healed_json[0], seeded[0], "System row untouched");
    assert_eq!(healed_json[1], seeded[1], "assistant rows untouched");
    assert_eq!(healed_json[3], seeded[3], "User rows untouched");
    assert_eq!(healed_json[4], seeded[4], "assistant rows untouched");
    assert_eq!(healed_json[6], seeded[6], "User rows untouched");
    assert_eq!(
        healed_json[5], seeded[5],
        "the recent ToolResults row inside the window is untouched"
    );

    let old = tool_results(&healed[2]);
    assert_eq!(
        old[0].tool_use_id, "tool-old",
        "provenance survives elision"
    );
    let elided = old[0].text_content();
    assert!(
        elided.starts_with(MARKER_PREFIX) && elided.contains("elided"),
        "the elided entry is a typed marker: {elided}"
    );
    assert!(
        elided.contains("original_bytes="),
        "the marker accounts for what was dropped: {elided}"
    );
}

#[tokio::test]
async fn session_without_tool_results_is_an_honest_noop() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = continuity_db(dir.path());
    let sid = seed_session(
        &db,
        vec![
            meerkat_core::Message::System(meerkat_core::types::SystemMessage::new("prompt")),
            meerkat_core::Message::User(meerkat_core::types::UserMessage::text("hello")),
        ],
    )
    .await;
    let seeded = as_json(&load_head(&db, &sid).await);

    for extra in [
        &["--truncate-tool-results", "1024"][..],
        &["--truncate-tool-results", "1024", "--apply"][..],
    ] {
        let run = run_repair(&db, &sid, extra);
        assert_eq!(
            run.code, 0,
            "a no-op is not a refusal; stderr: {}",
            run.stderr
        );
        let session = &run.report["sessions"][0];
        assert_eq!(session["outcome"], "nothing_to_do");
        assert_eq!(session["rows_before"], session["rows_after"]);
        assert_eq!(session["bytes_before"], session["bytes_after"]);
        assert_eq!(session["tool_result_messages"], 0);
        assert_eq!(session["tool_results_bounded"], 0);
        assert_eq!(
            session["tool_result_bytes_before"],
            session["tool_result_bytes_after"]
        );
    }
    assert_eq!(
        as_json(&load_head(&db, &sid).await),
        seeded,
        "no-op runs (even with --apply) must not touch the durable strand"
    );
}

/// Rows the predecessor (pre-0.9) repair tool bounded carry its marker; a
/// pass over them must not shave them again and overwrite the recorded
/// original size.
#[tokio::test]
async fn rows_bounded_by_the_predecessor_tool_are_not_bounded_again() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = continuity_db(dir.path());
    let predecessor_bounded = format!(
        "{}truncated original_bytes=65536]\n{}",
        meerkat_meta::storage_predecessor::PREDECESSOR_REPAIR_MARKER_PREFIX,
        big_multibyte(4096)
    );
    let sid = seed_session(
        &db,
        vec![
            meerkat_core::Message::System(meerkat_core::types::SystemMessage::new("prompt")),
            meerkat_core::Message::User(meerkat_core::types::UserMessage::text("hello")),
            assistant_tool_use(&["tool-big"]),
            meerkat_core::Message::tool_results(vec![meerkat_core::types::ToolResult::new(
                "tool-big".to_string(),
                predecessor_bounded,
                false,
            )]),
        ],
    )
    .await;
    let seeded = as_json(&load_head(&db, &sid).await);

    let run = run_repair(&db, &sid, &["--truncate-tool-results", "1023", "--apply"]);
    assert_eq!(run.code, 0, "stderr: {}", run.stderr);
    assert_eq!(
        as_json(&load_head(&db, &sid).await),
        seeded,
        "a predecessor-bounded row must keep its bytes and recorded original size"
    );
}

#[test]
fn usage_errors_exit_2() {
    let bin = env!("CARGO_BIN_EXE_meta_repair");
    let cases: &[&[&str]] = &[
        &[
            "--db",
            "/nonexistent",
            "--all-sessions",
            "--truncate-tool-results",
            "0",
        ],
        &[
            "--db",
            "/nonexistent",
            "--all-sessions",
            "--truncate-tool-results",
            "abc",
        ],
        &[
            "--db",
            "/nonexistent",
            "--all-sessions",
            "--drop-tool-results-older-than",
            "0",
        ],
        &[
            "--db",
            "/nonexistent",
            "--all-sessions",
            "--drop-tool-results-older-than",
            "x",
        ],
        // System-row selection and tool-result bounding are different levers;
        // combining them is a usage error, not a silent priority pick.
        &[
            "--db",
            "/nonexistent",
            "--all-sessions",
            "--keep-newest",
            "2",
            "--truncate-tool-results",
            "1024",
        ],
    ];
    for args in cases {
        let output = Command::new(bin).args(*args).output().expect("spawn");
        assert_eq!(
            output.status.code(),
            Some(2),
            "usage error must exit 2 for {args:?}; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
