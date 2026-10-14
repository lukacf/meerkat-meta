//! COLD-PROCESS recovery: the reboot proof that a same-process harness
//! cannot give.
//!
//! # Why this file exists
//!
//! `identity_first_head_canonical_resume.rs` walks four "reboots" by
//! constructing and shutting down `UnifiedRuntime`s inside ONE test process.
//! That establishes durable-store behavior, but it does NOT establish cold
//! recovery, because meerkat keeps PROCESS-GLOBAL memos that outlive a
//! runtime:
//!
//! - the transcript-graph decode memo (`MEERKAT_DISABLE_GRAPH_DECODE_MEMO`
//!   is its kill switch),
//! - the digest accumulator's memo, and
//! - the slim-materialization snapshot registry that `SessionHead::from_session`
//!   seeds on the PRODUCER side of a save.
//!
//! Every one of those can hand a later same-process "boot" a decoded, digested
//! or fully materialized transcript that a real `execve` could never inherit.
//! A resume defect that only shows up when the bytes on disk are the sole
//! input therefore stays invisible in that file, by construction.
//!
//! # What this file does instead
//!
//! Each boot is a REAL OS PROCESS: the shipped `rpc_gateway --persistent`
//! binary, spawned over the same state directory, driven with the established
//! stdin-JSONL host protocol (the same idiom as
//! `gateway_concurrent_dispatch.rs` and `storage_layout_boot.rs`). The test
//! process is the SDK host: it answers `callback/roster_provider/roster` and
//! nothing else, so the gateway opens its OWN continuity substrate in the
//! state dir and installs `ContinuitySessionStoreAdapter` as meerkat's
//! session store — the shipped identity-first composition, not a builder arm
//! assembled by the test.
//!
//! The LLM is `runtime_options.demo_llm` (meerkat's `TestClient`): no network,
//! no API key, deterministic single-turn replies.
//!
//! At least one boot runs with `MEERKAT_DISABLE_GRAPH_DECODE_MEMO=1` in the
//! CHILD's environment so the decode memo cannot mask a decode defect; the
//! other boots explicitly `env_remove` it, so "memo enabled" boots are
//! genuinely memo-enabled even when a developer exports it in their shell.
//!
//! # What is asserted
//!
//! Only DURABLE state, read back out of `continuity.sqlite3` with plain
//! SQL after the child has exited — never an in-process handle:
//!
//! - exactly ONE `continuity_session_heads` row survives each reboot, and its
//!   session id never changes (session rotation is a failure, not a recovery),
//! - the head's strand rows form a complete, gap-free `0..message_count`
//!   prefix (the store's own read contract, `strand_messages_in_txn(.., 0..
//!   head.message_count)`),
//! - the transcript a reboot inherits is EXACTLY the transcript the previous
//!   process left (element-wise JSON equality, same order, same length —
//!   restore may not mutate the durable transcript),
//! - each turn EXTENDS that transcript: the previous messages stay an exact
//!   prefix and every earlier boot's marker token is still present,
//! - the identity is `active` with a reachable continuity store, and the
//!   gateway's shutdown handshake attests completed runtime cleanup (a wedged
//!   runtime reports `shutdown: false`).
//!
//! The second test additionally reproduces the field shape the same-process
//! sibling documents as its own blind spot — a runtime snapshot pinned behind
//! the durable head — but ACROSS a process boundary and with the decode memo
//! off, which is where the committed-head read path must carry recovery
//! entirely on its own.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::uninlined_format_args
)]

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use meerkat_meta::storage_layout::MetaStorageLayout;
use serde_json::{Value, json};

#[path = "support/gateway_shutdown.rs"]
mod gateway_shutdown;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// `turn_driven` so one `meta/send` is exactly one turn (the autonomous-host
/// default would keep looping and make durable message accounting
/// nondeterministic). Model and tool shape mirror the identity-first arm of
/// `gateway_concurrent_dispatch.rs`.
const MOB_CONFIG: &str = r#"
[mob]
id = "identity-first-subprocess-reboot"

[profiles.default]
model = "gpt-5.5"
external_addressable = true
runtime_mode = "turn_driven"

[profiles.default.tools]
comms = true
"#;

const IDENTITY: &str = "agent:alice";

/// Meerkat's kill switch for the process-global transcript-graph decode memo.
const DECODE_MEMO_KILL_SWITCH: &str = "MEERKAT_DISABLE_GRAPH_DECODE_MEMO";

/// Messages one `turn_driven` turn must durably add: the user prompt and the
/// assistant reply.
const MESSAGES_PER_TURN: i64 = 2;

const INIT_TIMEOUT: Duration = Duration::from_mins(3);
const RPC_TIMEOUT: Duration = Duration::from_mins(1);
const TURN_TIMEOUT: Duration = Duration::from_mins(3);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_mins(3);
const EXIT_TIMEOUT: Duration = Duration::from_mins(1);

fn roster_spec() -> Value {
    json!({
        "identity": IDENTITY,
        "profile": "default",
        "addressability": "addressable",
        "display_name": null,
        "labels": {},
        "context": null,
        "additional_instructions": []
    })
}

// ---------------------------------------------------------------------------
// Child gateway process
// ---------------------------------------------------------------------------

/// One booted `rpc_gateway --persistent` child process, plus the reader
/// threads that pump its stdout (JSONL protocol) and stderr (diagnostics).
struct Gateway {
    label: String,
    child: Child,
    stdin: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
    stderr: Arc<Mutex<Vec<String>>>,
}

impl Gateway {
    /// Spawn the real binary. `disable_decode_memo` controls ONLY this
    /// child's environment; when false the variable is explicitly removed so
    /// an ambient export in the developer's shell cannot silently turn a
    /// memo-enabled boot into a memo-disabled one.
    fn start(label: &str, disable_decode_memo: bool) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_rpc_gateway"));
        command
            .arg("--persistent")
            // This oracle observes the default INFO-level boot declaration.
            // An ambient RUST_LOG must not suppress the event under test.
            .env_remove("RUST_LOG")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if disable_decode_memo {
            command.env(DECODE_MEMO_KILL_SWITCH, "1");
        } else {
            command.env_remove(DECODE_MEMO_KILL_SWITCH);
        }
        let mut child = command.spawn().expect("spawn rpc_gateway --persistent");

        let stdin = child.stdin.take().expect("gateway stdin");
        let stdout = child.stdout.take().expect("gateway stdout");
        let child_stderr = child.stderr.take().expect("gateway stderr");

        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });

        let stderr = Arc::new(Mutex::new(Vec::new()));
        let stderr_sink = stderr.clone();
        thread::spawn(move || {
            for line in BufReader::new(child_stderr).lines() {
                let Ok(line) = line else { break };
                let Ok(mut sink) = stderr_sink.lock() else {
                    break;
                };
                if sink.len() < 500 {
                    sink.push(line);
                }
            }
        });

        Self {
            label: label.to_string(),
            child,
            stdin: Some(stdin),
            lines: rx,
            stderr,
        }
    }

    fn send(&mut self, value: &Value) {
        let stdin = self.stdin.as_mut().expect("gateway stdin remains open");
        writeln!(
            stdin,
            "{}",
            serde_json::to_string(value).expect("serialize request")
        )
        .expect("write request to gateway stdin");
        stdin.flush().expect("flush gateway stdin");
    }

    fn close_stdin(&mut self) {
        drop(self.stdin.take());
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Last lines the child wrote to stderr, for failure diagnosis.
    fn stderr_tail(&self) -> String {
        let Ok(sink) = self.stderr.lock() else {
            return "[gateway stderr unavailable: poisoned lock]".to_string();
        };
        let start = sink.len().saturating_sub(40);
        format!(
            "--- [{}] gateway stderr (last {} of {} lines) ---\n{}",
            self.label,
            sink.len() - start,
            sink.len(),
            sink[start..].join("\n")
        )
    }

    /// Read protocol messages until `predicate` matches; every other message
    /// is handed to `on_other` so host callbacks stay answered.
    fn wait_for(
        &mut self,
        deadline: Duration,
        mut on_other: impl FnMut(&mut Self, &Value),
        predicate: impl Fn(&Value) -> bool,
    ) -> Option<Value> {
        let start = Instant::now();
        loop {
            let remaining = deadline.checked_sub(start.elapsed())?;
            let Ok(line) = self.lines.recv_timeout(remaining) else {
                return None;
            };
            let Ok(message) = serde_json::from_str::<Value>(line.trim()) else {
                continue;
            };
            if predicate(&message) {
                return Some(message);
            }
            on_other(self, &message);
        }
    }

    /// Send one request and return its response, answering host callbacks
    /// while waiting. Panics with the child's stderr tail on timeout.
    fn call(&mut self, id: &str, method: &str, params: Value, deadline: Duration) -> Value {
        self.send(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }));
        let matched = self.wait_for(deadline, answer_host_callback, |message| {
            is_response_with_id(message, id)
        });
        match matched {
            Some(response) => {
                assert!(
                    response.get("error").is_none(),
                    "[{}] {method} failed: {response}\n{}",
                    self.label,
                    self.stderr_tail()
                );
                response
            }
            None => panic!(
                "[{}] no response to {method} within {:?}\n{}",
                self.label,
                deadline,
                self.stderr_tail()
            ),
        }
    }

    /// The SDK shutdown handshake, then EOF, then reaping. The result is the
    /// runtime's cleanup attestation: complete, with the durable child root
    /// released and every other phase settled; a wedged runtime (drain
    /// timeout, mob stop failure, unreleased identity authority, orphaned
    /// module processes, a held child root) fails it.
    fn shutdown_and_reap(&mut self) {
        let response = self.call("shutdown", "meta/shutdown", json!({}), SHUTDOWN_TIMEOUT);
        // A persistent stock gateway stands its durable child mobs down and
        // releases their root: cleanup is complete.
        gateway_shutdown::assert_persistent_shutdown_completed(
            &response,
            &format!("[{}] shutdown\n{}", self.label, self.stderr_tail()),
        );
        self.close_stdin();
        let status = self.wait_for_exit(EXIT_TIMEOUT);
        assert!(
            status.success(),
            "[{}] rpc_gateway exited with {status}\n{}",
            self.label,
            self.stderr_tail()
        );
    }

    fn wait_for_exit(&mut self, deadline: Duration) -> ExitStatus {
        let start = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("poll gateway exit") {
                return status;
            }
            if start.elapsed() >= deadline {
                let tail = self.stderr_tail();
                let _ = self.child.kill();
                let _ = self.child.wait();
                panic!(
                    "[{}] rpc_gateway did not exit within {:?}\n{}",
                    self.label, deadline, tail
                );
            }
            thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn is_response_with_id(message: &Value, id: &str) -> bool {
    message.get("method").is_none() && message.get("id").and_then(Value::as_str) == Some(id)
}

/// The host side of the gateway's callback bridge. This harness declares ONE
/// provider (`has_roster_provider`), so the roster callback is the only one
/// that may arrive; anything else means the init params installed a bridge
/// this test does not implement, and answering it with a stub would make the
/// run meaningless.
fn answer_host_callback(gateway: &mut Gateway, message: &Value) {
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return;
    };
    // Notifications carry no id and expect no reply.
    let Some(id) = message.get("id").cloned() else {
        return;
    };
    match method {
        "callback/roster_provider/roster" => {
            gateway.send(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": [roster_spec()],
            }));
        }
        other if other.starts_with("callback/") => {
            panic!(
                "[{}] unexpected host callback {other}: this harness only provides a roster \
                 provider, so a stub answer would invalidate the run",
                gateway.label
            );
        }
        _ => {}
    }
}

/// Boot a gateway over `state`, complete the identity-first init handshake,
/// and return the live child.
fn boot(label: &str, state: &Path, disable_decode_memo: bool) -> Gateway {
    boot_with_role_migrations(label, state, disable_decode_memo, None)
}

/// `boot`, plus the boot-scoped member role migrations this activation is
/// authorized to perform. `None` is the ordinary case: no identity may migrate
/// its durable role on resume.
fn boot_with_role_migrations(
    label: &str,
    state: &Path,
    disable_decode_memo: bool,
    role_migrations: Option<Value>,
) -> Gateway {
    let mut gateway = Gateway::start(label, disable_decode_memo);
    let mut init_params = json!({
            "persistent_state": state,
            "mob_config": MOB_CONFIG,
            // Identity-first WITHOUT has_continuity_store/has_lease_provider:
            // the gateway opens its own continuity substrate in the state dir
            // and installs it as meerkat's session store. That is the shape
            // whose cold recovery is under test.
            "has_roster_provider": true,
            "runtime_options": {
                "demo_llm": true,
                "identity_bootstrap_mode": { "mode": "eager_materialize" }
            }
    });
    if let Some(declarations) = role_migrations {
        init_params["role_migrations"] = declarations;
    }
    let response = gateway.call("init", "meta/init", init_params, INIT_TIMEOUT);
    assert!(
        response["result"]["contract_version"].is_string(),
        "[{label}] identity-first init returned no contract version: {response}"
    );
    gateway
}

/// Queue one turn. `turn_driven` members process it as exactly one turn.
///
/// `content` is a bare JSON string: `ContentInput` is `#[serde(untagged)]`,
/// so its `Text` variant is the string itself (this is also exactly what the
/// Python/TypeScript SDKs put on the wire).
fn send_turn(gateway: &mut Gateway, prompt: &str) {
    let response = gateway.call(
        "send",
        "meta/send",
        json!({ "identity": IDENTITY, "content": prompt }),
        RPC_TIMEOUT,
    );
    assert!(
        response["result"]["fencing_token"].is_number(),
        "[{}] meta/send did not return a fencing token: {response}",
        gateway.label
    );
}

/// Assert the restored identity is healthy and bound to `session_id`.
fn assert_identity_bound(gateway: &mut Gateway, session_id: &str) {
    let response = gateway.call(
        "status",
        "meta/status_identity",
        json!({ "identity": IDENTITY }),
        RPC_TIMEOUT,
    );
    let result = &response["result"];
    assert_eq!(
        result["state"],
        json!("active"),
        "[{}] restore left the identity non-active: {response}",
        gateway.label
    );
    assert_eq!(
        result["session_id"],
        json!(session_id),
        "[{}] restore rebound the identity to a different session: {response}",
        gateway.label
    );
    assert_eq!(
        result["continuity_health"]["store_reachable"],
        json!(true),
        "[{}] continuity store unreachable after restore: {response}",
        gateway.label
    );
}

// ---------------------------------------------------------------------------
// Durable state (read from disk, never from a handle)
// ---------------------------------------------------------------------------

fn continuity_db(state: &Path) -> PathBuf {
    MetaStorageLayout::with_injected_roots(state.to_path_buf(), None)
        .continuity_db()
        .expect("resolve the continuity db path")
        .path
}

/// The layout-authoritative runtime store path, derived rather than
/// hardcoded so a layout rename cannot turn the snapshot harness into a
/// silent no-op.
fn runtime_db(state: &Path) -> PathBuf {
    MetaStorageLayout::with_injected_roots(state.to_path_buf(), None).runtime_db()
}

/// Open the durable file the way a reader must: an ordinary read/write path.
/// `?immutable=1` ignores the `-wal` sidecar and manufactures a 0-byte file at
/// a missing path, which is exactly how a probe lies about durability.
fn open_durable(db: &Path) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(db).expect("open durable sqlite file");
    conn.busy_timeout(Duration::from_secs(10))
        .expect("set busy timeout");
    conn
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

/// `(session_id, message_count)` for every durable head. Cheap enough to poll
/// while the child process is still writing.
fn head_summaries(db: &Path) -> Vec<(String, i64)> {
    if !db.exists() {
        return Vec::new();
    }
    let conn = open_durable(db);
    if !table_exists(&conn, "continuity_session_heads") {
        return Vec::new();
    }
    let mut stmt = conn
        .prepare("SELECT session_id, message_count FROM continuity_session_heads ORDER BY 1")
        .expect("prepare head summary");
    stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })
    .expect("query head summaries")
    .collect::<Result<Vec<_>, _>>()
    .expect("collect head summaries")
}

/// The durable transcript, reconstructed exactly the way the store's own
/// reader does it: the single head row names a strand and a message count,
/// and the transcript is that strand's `0..message_count` prefix.
#[derive(Debug, Clone)]
struct DurableTranscript {
    session_id: String,
    strand: String,
    message_count: i64,
    messages: Vec<Value>,
}

impl DurableTranscript {
    /// Raw JSON of every durable message, for marker-token containment.
    fn joined_json(&self) -> String {
        self.messages
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Read the durable transcript, failing loudly on every shape that would make
/// a downstream assertion vacuous.
fn read_durable_transcript(state: &Path, phase: &str) -> DurableTranscript {
    let db = continuity_db(state);
    assert!(
        db.exists(),
        "PRECONDITION FAILED ({phase}): {} does not exist — no gateway process ever opened the \
         continuity substrate. State dir: {:?}",
        db.display(),
        state_dir_listing(state)
    );
    let conn = open_durable(&db);
    assert!(
        table_exists(&conn, "continuity_session_heads"),
        "PRECONDITION FAILED ({phase}): {} has no continuity_session_heads table — session I/O \
         is MIS-WIRED (it never rode the continuity adapter), so nothing asserted after this \
         point is meaningful. State dir: {:?}",
        db.display(),
        state_dir_listing(state)
    );

    let mut stmt = conn
        .prepare(
            "SELECT session_id, message_count, head_json FROM continuity_session_heads ORDER BY 1",
        )
        .expect("prepare head read");
    let heads = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })
        .expect("query heads")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect heads");

    assert!(
        !heads.is_empty(),
        "PRECONDITION FAILED ({phase}): {} holds NO durable session head. Session I/O is \
         MIS-WIRED. State dir: {:?}",
        db.display(),
        state_dir_listing(state)
    );
    assert_eq!(
        heads.len(),
        1,
        "({phase}) exactly one head-canonical document must exist; found {}: {:?}",
        heads.len(),
        heads
            .iter()
            .map(|(id, count, _)| (id.clone(), *count))
            .collect::<Vec<_>>()
    );

    let (session_id, message_count, head_json) = heads.into_iter().next().expect("one head");
    let head: Value = serde_json::from_slice(&head_json).expect("decode head_json");
    let strand = head["strand"]
        .as_str()
        .unwrap_or_else(|| panic!("({phase}) head document names no strand: {head}"))
        .to_string();
    let head_count = head["message_count"]
        .as_i64()
        .unwrap_or_else(|| panic!("({phase}) head document has no message_count: {head}"));
    assert_eq!(
        head_count, message_count,
        "({phase}) the head row's indexed message_count disagrees with the head document"
    );

    let mut strand_stmt = conn
        .prepare(
            "SELECT seq, message_json FROM continuity_strand_messages \
             WHERE session_id = ?1 AND strand = ?2 AND seq >= 0 AND seq < ?3 ORDER BY seq ASC",
        )
        .expect("prepare strand read");
    let strand_rows = strand_stmt
        .query_map(
            rusqlite::params![session_id.as_str(), strand.as_str(), message_count],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .expect("query strand rows")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect strand rows");

    let observed_seqs = strand_rows.iter().map(|(seq, _)| *seq).collect::<Vec<_>>();
    let expected_seqs = (0..message_count).collect::<Vec<_>>();
    assert_eq!(
        observed_seqs, expected_seqs,
        "({phase}) session {session_id} strand {strand} does not cover a gap-free 0..\
         {message_count} prefix — the head claims messages its strand rows cannot serve"
    );

    let messages = strand_rows
        .into_iter()
        .map(|(seq, bytes)| {
            serde_json::from_slice::<Value>(&bytes)
                .unwrap_or_else(|e| panic!("({phase}) strand row seq {seq} is not valid JSON: {e}"))
        })
        .collect::<Vec<_>>();

    DurableTranscript {
        session_id,
        strand,
        message_count,
        messages,
    }
}

fn state_dir_listing(state: &Path) -> Vec<String> {
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

/// Turn-boundary barrier: block until the durable head reaches `floor`
/// messages. The head row is written by the same boundary save that persists
/// the strand rows, so once the count moves, that turn is durable.
fn wait_for_durable_messages(state: &Path, floor: i64, what: &str) {
    let db = continuity_db(state);
    let start = Instant::now();
    loop {
        let heads = head_summaries(&db);
        if let [(_, count)] = heads.as_slice()
            && *count >= floor
        {
            return;
        }
        assert!(
            start.elapsed() < TURN_TIMEOUT,
            "timed out waiting for {what}: want one durable head with >= {floor} messages, \
             observed {heads:?}"
        );
        thread::sleep(Duration::from_millis(150));
    }
}

/// The exact-survival assertion: `previous` must still be there, unchanged,
/// in order, as a prefix of `current`.
fn assert_exact_prefix(previous: &DurableTranscript, current: &DurableTranscript, phase: &str) {
    assert_eq!(
        previous.session_id, current.session_id,
        "({phase}) the durable session was ROTATED across the process boundary \
         ({} -> {}); a new session is data loss, not recovery",
        previous.session_id, current.session_id
    );
    assert!(
        current.messages.len() >= previous.messages.len(),
        "({phase}) the durable transcript SHRANK: {} -> {} messages",
        previous.messages.len(),
        current.messages.len()
    );
    for (index, before) in previous.messages.iter().enumerate() {
        assert_eq!(
            &current.messages[index], before,
            "({phase}) durable message {index} changed across the boundary \
             (session {}, strand {} -> {})",
            current.session_id, previous.strand, current.strand
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Three real `rpc_gateway` processes over one state directory. Every boot
/// after the first starts with an empty address space: no decode memo, no
/// digest memo, no producer-seeded materialization snapshot. The transcript
/// and the identity binding must survive on the strength of the bytes on disk
/// alone.
#[test]
fn head_canonical_document_survives_real_process_restarts() {
    // `(label, disable the decode memo in this child)`. Boot 2 — the first
    // resume, and the one that must decode a transcript it did not write —
    // runs with the memo off. Boot 3 runs with it on, so the shipped
    // configuration is covered too.
    const BOOTS: [(&str, bool); 3] = [
        ("boot-1/memo-on", false),
        ("boot-2/memo-off", true),
        ("boot-3/memo-on", false),
    ];
    let tokens = [
        "MARKER-COLD-BOOT-1-ALFA",
        "MARKER-COLD-BOOT-2-BRAVO",
        "MARKER-COLD-BOOT-3-CHARLIE",
    ];

    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&state).expect("create state dir");

    let mut previous: Option<DurableTranscript> = None;

    for (index, (label, disable_memo)) in BOOTS.iter().enumerate() {
        let mut gateway = boot(label, &state, *disable_memo);
        // Each boot really is an `execve`, not a fresh runtime inside this
        // address space — which is the whole point of the file.
        assert_ne!(
            gateway.pid(),
            std::process::id(),
            "({label}) the gateway must run as a separate OS process"
        );

        // Pre-turn: what this cold process INHERITED must be exactly what the
        // previous process left behind.
        if let Some(before) = previous.as_ref() {
            let inherited = read_durable_transcript(&state, &format!("{label} pre-turn"));
            assert_eq!(
                inherited.message_count, before.message_count,
                "({label}) restore mutated the durable transcript length before any turn: \
                 {} -> {}",
                before.message_count, inherited.message_count
            );
            assert_exact_prefix(before, &inherited, &format!("{label} pre-turn"));
            assert_eq!(
                inherited.messages.len(),
                before.messages.len(),
                "({label}) restore appended messages before any turn"
            );
            assert_identity_bound(&mut gateway, &before.session_id);
        }

        let floor = previous
            .as_ref()
            .map_or(MESSAGES_PER_TURN, |t| t.message_count + MESSAGES_PER_TURN);
        send_turn(
            &mut gateway,
            &format!("Please note this token: {}", tokens[index]),
        );
        wait_for_durable_messages(&state, floor, &format!("{label}'s turn"));
        gateway.shutdown_and_reap();

        // Post-exit: read the durable file with the writer gone.
        let after = read_durable_transcript(&state, &format!("{label} post-turn"));
        eprintln!(
            "PROBE {label}: session={} strand={} messages={}",
            after.session_id, after.strand, after.message_count
        );
        assert!(
            after.message_count >= floor,
            "({label}) the turn did not extend the durable head: {} < {floor}",
            after.message_count
        );
        let joined = after.joined_json();
        for token in tokens.iter().take(index + 1).copied() {
            assert!(
                joined.contains(token),
                "({label}) the durable transcript lost marker {token} — a reboot dropped or \
                 replaced earlier turns. Durable messages: {}",
                after.message_count
            );
        }
        if let Some(before) = previous.as_ref() {
            assert_exact_prefix(before, &after, &format!("{label} post-turn"));
        }
        previous = Some(after);
    }

    let final_transcript = previous.expect("three boots produced a transcript");
    assert!(
        final_transcript.message_count >= BOOTS.len() as i64 * MESSAGES_PER_TURN,
        "after {} process restarts the durable head should hold one user+assistant pair per \
         boot, got {}",
        BOOTS.len(),
        final_transcript.message_count
    );

    // One identity, one continuity record, naming the surviving session.
    let conn = open_durable(&continuity_db(&state));
    let records = {
        let mut stmt = conn
            .prepare("SELECT identity, session_id FROM continuity_records ORDER BY 1")
            .expect("prepare continuity records");
        stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .expect("query continuity records")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect continuity records")
    };
    assert_eq!(
        records,
        vec![(IDENTITY.to_string(), final_transcript.session_id)],
        "the identity binding must survive every restart pointing at the same session"
    );
}

/// The same-process sibling documents its own blind spot: after a clean
/// shutdown the persistent runtime snapshot is as fresh as the durable head,
/// so resume never has to consult `continuity_session_heads`.
///
/// This test manufactures the advisory's field shape — a runtime snapshot
/// pinned behind the committed head — and then reboots into it AS A NEW
/// PROCESS with the decode memo disabled. Recovery must be carried entirely
/// by the committed continuity head, read from bytes, with no in-process
/// residue of the producer that wrote them.
#[test]
fn cold_process_with_a_stale_runtime_snapshot_resumes_from_the_committed_head() {
    const TOKEN_A: &str = "MARKER-STALE-COLD-1-DELTA";
    const TOKEN_B: &str = "MARKER-STALE-COLD-2-ECHO";
    const TOKEN_C: &str = "MARKER-STALE-COLD-3-FOXTROT";

    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    let stash = temp.path().join("runtime-snapshot-after-boot-1");
    std::fs::create_dir_all(&state).expect("create state dir");

    // --- Boot 1: seed the transcript, then stash the runtime snapshot ------
    let mut gateway = boot("stale/boot-1", &state, false);
    send_turn(&mut gateway, &format!("Please note this token: {TOKEN_A}"));
    wait_for_durable_messages(&state, MESSAGES_PER_TURN, "stale/boot-1's turn");
    gateway.shutdown_and_reap();
    let after_boot_1 = read_durable_transcript(&state, "stale/boot-1");
    save_runtime_store(&state, &stash);

    // --- Boot 2: advance the durable head past the stashed snapshot --------
    let mut gateway = boot("stale/boot-2", &state, false);
    assert_identity_bound(&mut gateway, &after_boot_1.session_id);
    send_turn(&mut gateway, &format!("Second token: {TOKEN_B}"));
    wait_for_durable_messages(
        &state,
        after_boot_1.message_count + MESSAGES_PER_TURN,
        "stale/boot-2's turn",
    );
    gateway.shutdown_and_reap();
    let after_boot_2 = read_durable_transcript(&state, "stale/boot-2");
    assert_exact_prefix(&after_boot_1, &after_boot_2, "stale/boot-2");
    assert!(
        after_boot_2.message_count > after_boot_1.message_count,
        "boot 2 must advance the durable head past the stashed runtime snapshot ({} -> {})",
        after_boot_1.message_count,
        after_boot_2.message_count
    );

    // --- Arm the divergence: runtime snapshot back at the boot-1 state -----
    //
    // POSITIVE proof the divergence is armed, not assumed: a renamed or
    // relocated runtime store would make save/restore silent no-ops, and boot
    // 3 would then resume from a perfectly fresh snapshot while every
    // assertion below passed vacuously.
    let fresh_runtime_messages = runtime_snapshot_message_count(&state, &after_boot_2.session_id)
        .expect("boot 2's clean shutdown must leave a runtime snapshot for the session");
    restore_runtime_store(&state, &stash);
    let stale_runtime_messages = runtime_snapshot_message_count(&state, &after_boot_2.session_id)
        .expect("the restored runtime store must hold the boot-1 snapshot for the session");
    assert!(
        stale_runtime_messages < fresh_runtime_messages,
        "divergence NOT armed: the rollback left the runtime snapshot at \
         {stale_runtime_messages} messages, not behind the boot-2 snapshot it replaced \
         ({fresh_runtime_messages}); the restore replaced nothing"
    );
    assert!(
        stale_runtime_messages < after_boot_2.message_count,
        "divergence NOT armed: the restored runtime snapshot ({stale_runtime_messages} messages) \
         is not behind the committed continuity head ({})",
        after_boot_2.message_count
    );
    eprintln!(
        "PROBE divergence armed: runtime snapshot rolled back to {stale_runtime_messages} \
         messages (boot-2 snapshot held {fresh_runtime_messages}), committed head at {} messages",
        after_boot_2.message_count
    );
    // The durable head is untouched by the runtime-store swap.
    let before_boot_3 = read_durable_transcript(&state, "stale/pre-boot-3");
    assert_exact_prefix(&after_boot_2, &before_boot_3, "stale/pre-boot-3");
    assert_eq!(
        before_boot_3.message_count, after_boot_2.message_count,
        "swapping the runtime store must not change the committed continuity head"
    );

    // --- Boot 3: cold process, memo off, stale runtime snapshot ------------
    let mut gateway = boot("stale/boot-3/memo-off", &state, true);
    assert_identity_bound(&mut gateway, &after_boot_2.session_id);
    let inherited = read_durable_transcript(&state, "stale/boot-3 pre-turn");
    assert_exact_prefix(&after_boot_2, &inherited, "stale/boot-3 pre-turn");
    assert_eq!(
        inherited.message_count, after_boot_2.message_count,
        "restore from a stale runtime snapshot must not rewind or mutate the committed head"
    );

    send_turn(&mut gateway, &format!("Third token: {TOKEN_C}"));
    wait_for_durable_messages(
        &state,
        after_boot_2.message_count + MESSAGES_PER_TURN,
        "stale/boot-3's turn (a reader that preferred the stale runtime snapshot would have its \
         save refused here)",
    );
    gateway.shutdown_and_reap();

    let after_boot_3 = read_durable_transcript(&state, "stale/boot-3");
    assert_exact_prefix(&after_boot_2, &after_boot_3, "stale/boot-3");
    assert!(
        after_boot_3.message_count >= after_boot_2.message_count + MESSAGES_PER_TURN,
        "the turn served from the committed head must extend it ({} -> {})",
        after_boot_2.message_count,
        after_boot_3.message_count
    );
    let joined = after_boot_3.joined_json();
    for token in [TOKEN_A, TOKEN_B, TOKEN_C] {
        assert!(
            joined.contains(token),
            "the recovered transcript lost marker {token}; durable messages: {}",
            after_boot_3.message_count
        );
    }
}

// ---------------------------------------------------------------------------
// Runtime-store snapshot harness (second test)
// ---------------------------------------------------------------------------

/// Every file the persistent runtime store owns right now.
fn runtime_store_files(state: &Path) -> Vec<PathBuf> {
    let db = runtime_db(state);
    ["", "-wal", "-shm", "-journal"]
        .iter()
        .map(|suffix| PathBuf::from(format!("{}{suffix}", db.display())))
        .filter(|path| path.exists())
        .collect()
}

/// Copy the runtime store aside. Fails loudly when there is nothing to copy:
/// a silent no-op would leave the divergence assertions vacuously green.
fn save_runtime_store(state: &Path, into: &Path) {
    let files = runtime_store_files(state);
    assert!(
        !files.is_empty(),
        "no runtime store files exist at {} — the snapshot harness is watching the wrong \
         location and the manufactured divergence would never arm. State dir: {:?}",
        runtime_db(state).display(),
        state_dir_listing(state)
    );
    std::fs::create_dir_all(into).expect("create snapshot dir");
    for path in files {
        let name = path.file_name().expect("runtime store file name");
        std::fs::copy(&path, into.join(name)).expect("copy runtime store file");
    }
}

/// Put the stashed runtime store back, replacing whatever is there now.
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
            .expect("restore runtime store file");
        restored += 1;
    }
    assert!(
        restored > 0,
        "runtime store stash at {} was empty — nothing was restored, the divergence was never \
         armed",
        from.display()
    );
}

/// Message count of the runtime store's persisted snapshot for `session_id`,
/// parsed from the persisted envelope (`{id, messages, ...}` — plain UTF-8
/// JSON per the store's `JsonColumnBytes` contract).
fn runtime_snapshot_message_count(state: &Path, session_id: &str) -> Option<i64> {
    let db = runtime_db(state);
    assert!(
        db.exists(),
        "runtime store {} does not exist — cannot verify the manufactured divergence",
        db.display()
    );
    let conn = open_durable(&db);
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

/// Wait for a stderr line: the reader thread pumps asynchronously, so a bare
/// read races the boot that produced the line.
fn wait_for_stderr(gateway: &Gateway, needle: &str, deadline: Duration) -> bool {
    let started = std::time::Instant::now();
    while started.elapsed() < deadline {
        if gateway.stderr_tail().contains(needle) {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    false
}

/// A role migration declaration is BOOT-scoped: the next boot that declares
/// nothing has no migration authority.
///
/// This is the property that makes a leftover declaration safe, and it can only
/// fail on a LATER boot, so it is proved across two real OS processes sharing
/// one durable state dir rather than by inspecting a map in-process.
///
/// Boot 1 also covers the inert case end to end: `from_role` names a
/// predecessor this identity never had, and because the identity's stored role
/// still equals its current role, Meerkat returns before the declaration is
/// ever read. Materialization therefore proceeds exactly as it does without
/// one, which is why boot 1 is expected to come up healthy.
///
/// The two assertions cannot both pass vacuously: if the gateway stopped
/// recording declarations altogether, boot 1 fails. So a green boot-2 means
/// absence, not silence.
#[test]
fn a_role_migration_declaration_does_not_survive_into_the_next_boot() {
    const DECLARED: &str = "activation declares a member role migration";

    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&state).expect("create state dir");

    let mut declaring = boot_with_role_migrations(
        "role-migration/boot-1",
        &state,
        false,
        Some(json!([{ "identity": IDENTITY, "from_role": "predecessor" }])),
    );
    assert!(
        wait_for_stderr(&declaring, DECLARED, Duration::from_secs(30)),
        "[boot-1] the gateway must record the declaration it was armed with: {}",
        declaring.stderr_tail()
    );
    declaring.shutdown_and_reap();

    let mut fresh = boot("role-migration/boot-2", &state, false);
    // Boot 2 has completed its init handshake, which is the same phase that
    // emitted the line on boot 1, so the line would already be here if it were
    // coming. The settle window covers only the stderr reader thread.
    thread::sleep(Duration::from_millis(500));
    assert!(
        !fresh.stderr_tail().contains(DECLARED),
        "[boot-2] a boot that declared nothing must arm nothing, but the \
         declaration came back from durable state: {}",
        fresh.stderr_tail()
    );
    fresh.shutdown_and_reap();
}

// ---------------------------------------------------------------------------
// Roster callback context at the SDK boundary
// ---------------------------------------------------------------------------

const ROSTER_CALLBACK: &str = "callback/roster_provider/roster";

/// `Gateway::call`, but every `callback/roster_provider/roster` request the
/// gateway makes while the call is in flight is appended to `recorded`, params
/// exactly as they crossed the wire. This is the host's view: what the SDK
/// dispatchers hand to the user's roster callback is `params.context`.
fn call_recording_roster_callbacks(
    gateway: &mut Gateway,
    id: &str,
    method: &str,
    params: Value,
    deadline: Duration,
    recorded: &mut Vec<Value>,
) -> Value {
    gateway.send(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    }));
    let matched = gateway.wait_for(
        deadline,
        |gateway, message| {
            if message.get("method").and_then(Value::as_str) == Some(ROSTER_CALLBACK) {
                recorded.push(message.get("params").cloned().unwrap_or(Value::Null));
            }
            answer_host_callback(gateway, message);
        },
        |message| is_response_with_id(message, id),
    );
    let Some(response) = matched else {
        panic!(
            "[{}] no response to {method} within {:?}\n{}",
            gateway.label,
            deadline,
            gateway.stderr_tail()
        );
    };
    assert!(
        response.get("error").is_none(),
        "[{}] {method} failed: {response}\n{}",
        gateway.label,
        gateway.stderr_tail()
    );
    response
}

/// The `RosterContext` the SDK callback receives, or a panic naming the
/// defect this test guards: the gateway used to serialize the context as the
/// params root, so `params.context` (what Python and TypeScript read) was `{}`.
fn roster_callback_context(params: &Value) -> &Value {
    assert!(
        params.get("mob_definition").is_none() && params.get("previous_identities").is_none(),
        "roster context was flattened into the params root; the SDK dispatchers read \
         `params.context` and would receive an empty context: {params}"
    );
    params
        .get("context")
        .unwrap_or_else(|| panic!("roster callback params carry no `context` envelope: {params}"))
}

/// Regression for the SDK-reported defect that the roster callback hosted by
/// Python/TypeScript received an empty context. Drives the real binary and
/// asserts on the wire, so it proves what an SDK host sees, not what the Rust
/// bridge intends: the bootstrap roster calls carry the mob definition (with
/// nothing registered yet, no previous identities), and a later topology query
/// re-asks the roster with the identity the runtime has since registered.
#[test]
fn the_roster_callback_receives_the_mob_definition_and_previous_identities() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&state).expect("create state dir");

    let mut gateway = Gateway::start("roster-context", false);
    let mut recorded = Vec::new();
    let response = call_recording_roster_callbacks(
        &mut gateway,
        "init",
        "meta/init",
        json!({
            "persistent_state": state,
            "mob_config": MOB_CONFIG,
            "has_roster_provider": true,
            "runtime_options": {
                "demo_llm": true,
                "identity_bootstrap_mode": { "mode": "eager_materialize" }
            }
        }),
        INIT_TIMEOUT,
        &mut recorded,
    );
    assert!(
        response["result"]["contract_version"].is_string(),
        "[roster-context] identity-first init returned no contract version: {response}"
    );
    assert!(
        !recorded.is_empty(),
        "[roster-context] init never resolved the roster through the host callback"
    );
    for params in &recorded {
        let context = roster_callback_context(params);
        assert_eq!(
            context["mob_definition"]["id"],
            json!("identity-first-subprocess-reboot"),
            "[roster-context] bootstrap roster context lacks the mob definition: {params}"
        );
        assert!(
            context["mob_definition"]["profiles"]["default"].is_object(),
            "[roster-context] mob definition profiles did not reach the host: {params}"
        );
        assert!(
            context["previous_identities"].is_array(),
            "[roster-context] previous_identities must be a list, even when empty: {params}"
        );
    }
    // The first resolve runs before anything is registered, so the runtime has
    // no previous identities to report there; the field is present but empty.
    assert_eq!(
        recorded[0]["context"]["previous_identities"],
        json!([]),
        "[roster-context] the pre-registration roster call must report no previous identities"
    );

    // A topology query re-derives the desired roster with the identities the
    // runtime has registered since bootstrap, so this is where the SDK
    // callback must see the previously known identity.
    recorded.clear();
    call_recording_roster_callbacks(
        &mut gateway,
        "topology",
        "meta/topology/query",
        json!({}),
        RPC_TIMEOUT,
        &mut recorded,
    );
    let params = recorded.last().unwrap_or_else(|| {
        panic!(
            "[roster-context] the identity-first topology query did not consult the roster\n{}",
            gateway.stderr_tail()
        )
    });
    let context = roster_callback_context(params);
    assert_eq!(
        context["mob_definition"]["id"],
        json!("identity-first-subprocess-reboot"),
        "[roster-context] topology roster context lacks the mob definition: {params}"
    );
    assert_eq!(
        context["previous_identities"],
        json!([IDENTITY]),
        "[roster-context] the registered identity did not reach the roster callback: {params}"
    );

    gateway.shutdown_and_reap();
}
