//! R1 version-boundary drive lane: realms MINTED BY THE RELEASED 0.8.8
//! ARTIFACT, driven through the build under test.
//!
//! # The release rule this file enforces
//!
//! N-1 state must be minted by the released binary, never by the build under
//! test. A fixture re-synthesized by current code silently passes exactly the
//! writer-drift bugs this lane exists to catch (the 0.8.11 fleet-import
//! regression shipped past a synthetic 26-chain test for that reason - see
//! tests/fixtures/README.md). The four realms under
//! `tests/fixtures/released_0_8_8_realms/` were therefore captured by driving
//! the PUBLIC-RELEASE 0.8.8 `rpc_gateway` (embedding meerkat SDK
//! 0.8.10) as a cold subprocess with the stdin-JSONL host protocol - the same
//! handshake this file uses - and copied here byte-exact, WAL/SHM/.mfence
//! sidecars included. `blobs/` ships empty in the released shape; git cannot
//! track an empty directory, so the staging helper restores it.
//!
//! The realms (all: continuity ledger v2 under its predecessor domain name,
//! runtime-store ledger v1, single identity `agent:alice`, strand `root`, no
//! realm_manifest.json). They carry the predecessor (pre-0.9) durable names,
//! so every drive first runs the explicit one-shot adoption verb
//! (`meta_gateway storage-adopt-predecessor --apply`) over the staged copy:
//! that verb, a process of the build under test, is the first contact with
//! the released bytes, and the gateway boots over what it adopted.
//!
//! - `baseline/`      multi-boot clean-shutdown history, 5 turns
//!   (head: 11 messages, tokens ALPHA..ECHO)
//! - `burst_drain/`   a pipelined 6-send burst raced against shutdown; the
//!   released gateway drained the whole queue before
//!   stopping (head: 21 messages, tokens ALPHA..DELTA +
//!   ECHO-0..ECHO-5)
//! - `crash_sigkill/` SIGKILLed after the burst drained. Inspected truth,
//!   pinned below: all 10 durable inputs are `consumed`, the
//!   legacy runtime snapshot is IN SYNC with the continuity
//!   head (21 messages), the persisted runtime state is
//!   idle - the kill landed between turns. The crash residue
//!   is therefore the BYTE SHAPE, not queue state:
//!   un-checkpointed WALs (the 4 KiB continuity main file
//!   carries nothing; all 21 messages live only in the
//!   828 KiB WAL), live -shm sidecars, and no clean-shutdown
//!   attestation. Recovery must ride WAL replay of released
//!   bytes, and the pinned truth says full re-activation
//!   (not typed parking) is the correct outcome.
//! - `deploy_cycles/` 4 deploy-boot cycles (boot, turn, clean shutdown).
//!   Head: 9 messages, tokens ALPHA..DELTA. Pinned truth:
//!   `rewrite_count` is 0 - the released binary minted NO
//!   resume rewrites for an unchanged system prompt, so this
//!   realm covers repeated released resume cycles, not
//!   rewrite chains.
//!
//! # What one drive proves, per realm
//!
//! 1. The CURRENT gateway is the first process to open a copy of the released
//!    bytes (`--persistent`, demo_llm, the shipped identity-first
//!    composition): boot succeeds, the pre-existing identity resumes ACTIVE
//!    and bound to the released session (rotation is data loss, not
//!    recovery), and no member is Broken.
//! 2. One real turn per pre-existing identity extends the released transcript
//!    durably; clean shutdown attests runtime cleanup.
//! 3. The released transcript survives element-for-element as an exact prefix
//!    (upgrade may not rewrite history), every released marker token is still
//!    present, and the upgraded runtime store now serves the session from the
//!    whole-blob authority representation (the 0.8.11 runtime-store schema).
//! 4. A SECOND cold boot resumes from state written by the NEW build: the
//!    upgraded bytes read back unchanged, and one more turn extends them.
//!
//! Durable assertions are read from disk with plain SQL after the child
//! exits, never from an in-process handle - the same discipline as
//! `identity_first_subprocess_reboot.rs`, whose subprocess harness this file
//! reuses. Fixture files themselves are NEVER opened with SQLite (a
//! read/write open checkpoints and truncates the WAL, destroying the released
//! byte shape); the size pins below catch that mistake loudly.

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

/// The EXACT mob config the released 0.8.8 capture driver used to mint the
/// realms. `turn_driven` so one `meta/send` is exactly one turn; changing
/// anything here would make the drive resume a different mob than the one the
/// released binary persisted.
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

/// The single identity every released realm persisted.
const IDENTITY: &str = "agent:alice";

/// Meerkat's kill switch for the process-global transcript-graph decode memo.
/// Every boot here is a cold `execve`, so the memo is empty regardless; the
/// variable is removed so an ambient developer export cannot flip the lane
/// away from the shipped configuration.
const DECODE_MEMO_KILL_SWITCH: &str = "MEERKAT_DISABLE_GRAPH_DECODE_MEMO";

/// Messages one `turn_driven` turn must durably add: the user prompt and the
/// assistant reply.
const MESSAGES_PER_TURN: i64 = 2;

const INIT_TIMEOUT: Duration = Duration::from_mins(3);
const RPC_TIMEOUT: Duration = Duration::from_mins(1);
const TURN_TIMEOUT: Duration = Duration::from_mins(3);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_mins(3);
const EXIT_TIMEOUT: Duration = Duration::from_mins(1);

/// Everything that is TRUE about one committed released realm, pinned from
/// read-only inspection of the capture (job 6b689d90). The drive asserts the
/// fixture still matches before trusting anything built on top of it.
struct RealmSpec {
    /// Directory name under `tests/fixtures/released_0_8_8_realms/`.
    name: &'static str,
    /// The session the released binary bound `agent:alice` to.
    session_id: &'static str,
    /// The released head's durable message count.
    released_message_count: i64,
    /// Marker tokens the released turns embedded; each must survive the
    /// upgrade verbatim.
    released_tokens: &'static [&'static str],
    /// Byte length of the committed `continuity.sqlite3` MAIN file. 4096 for
    /// every realm: the released substrate never checkpointed, so page 1 is
    /// all the main file holds.
    continuity_main_bytes: u64,
    /// Byte length of the committed `continuity.sqlite3-wal`. The entire
    /// released transcript lives here; a read/write SQLite open of the
    /// fixture would checkpoint and truncate it, which is exactly the rot
    /// this pin catches.
    continuity_wal_bytes: u64,
}

const BASELINE: RealmSpec = RealmSpec {
    name: "baseline",
    session_id: "019fb02c-0ebf-77a0-a9e3-5b2a99e29e42",
    released_message_count: 11,
    released_tokens: &[
        "FIXTURE-TOKEN-ALPHA",
        "FIXTURE-TOKEN-BRAVO",
        "FIXTURE-TOKEN-CHARLIE",
        "FIXTURE-TOKEN-DELTA",
        "FIXTURE-TOKEN-ECHO",
    ],
    continuity_main_bytes: 4096,
    continuity_wal_bytes: 498_552,
};

const BURST_DRAIN: RealmSpec = RealmSpec {
    name: "burst_drain",
    session_id: "019fb02e-5cd4-7ae2-b2ff-cf4400c205fa",
    released_message_count: 21,
    released_tokens: &[
        "FIXTURE-TOKEN-ALPHA",
        "FIXTURE-TOKEN-BRAVO",
        "FIXTURE-TOKEN-CHARLIE",
        "FIXTURE-TOKEN-DELTA",
        "FIXTURE-TOKEN-ECHO-0",
        "FIXTURE-TOKEN-ECHO-1",
        "FIXTURE-TOKEN-ECHO-2",
        "FIXTURE-TOKEN-ECHO-3",
        "FIXTURE-TOKEN-ECHO-4",
        "FIXTURE-TOKEN-ECHO-5",
    ],
    continuity_main_bytes: 4096,
    continuity_wal_bytes: 828_152,
};

const CRASH_SIGKILL: RealmSpec = RealmSpec {
    name: "crash_sigkill",
    session_id: "019fb039-69a7-71e2-849c-cd04a4254830",
    released_message_count: 21,
    released_tokens: &[
        "FIXTURE-TOKEN-ALPHA",
        "FIXTURE-TOKEN-BRAVO",
        "FIXTURE-TOKEN-CHARLIE",
        "FIXTURE-TOKEN-DELTA",
        "FIXTURE-TOKEN-ECHO-0",
        "FIXTURE-TOKEN-ECHO-1",
        "FIXTURE-TOKEN-ECHO-2",
        "FIXTURE-TOKEN-ECHO-3",
        "FIXTURE-TOKEN-ECHO-4",
        "FIXTURE-TOKEN-ECHO-5",
    ],
    continuity_main_bytes: 4096,
    continuity_wal_bytes: 828_152,
};

const DEPLOY_CYCLES: RealmSpec = RealmSpec {
    name: "deploy_cycles",
    session_id: "019fb03c-0cd5-7d71-adf8-de941fdefc3e",
    released_message_count: 9,
    released_tokens: &[
        "FIXTURE-TOKEN-ALPHA",
        "FIXTURE-TOKEN-BRAVO",
        "FIXTURE-TOKEN-CHARLIE",
        "FIXTURE-TOKEN-DELTA",
    ],
    continuity_main_bytes: 4096,
    continuity_wal_bytes: 444_992,
};

/// Durable input rows the crash realm froze; every one of them had already
/// been consumed when SIGKILL landed (the released gateway drained the burst
/// faster than the kill raced it). Pinned so nobody "fixes" an assertion into
/// expecting accepted-but-unconsumed inputs this realm does not contain.
const CRASH_INPUT_ROWS: usize = 10;

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

fn fixture_realm_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/released_0_8_8_realms")
        .join(name)
}

/// Stage one pristine copy of a committed realm. The fixture itself is never
/// mutated and never opened; the copy is what a process may touch.
fn stage_realm_copy(fixture: &Path, into: &Path) {
    copy_dir_recursive(fixture, into);
    // The released realm shape includes an EMPTY `blobs/` directory; git
    // cannot track it, so restore it here instead of polluting the
    // byte-exact fixture with a placeholder file.
    std::fs::create_dir_all(into.join("blobs")).expect("restore empty blobs dir");
}

/// Run the explicit predecessor adoption over a staged realm copy and pin
/// that it renamed the predecessor stores and left none behind.
fn adopt_predecessor_names(realm: &str, state: &Path) {
    // An unadopted realm refuses to resolve at all.
    assert!(
        MetaStorageLayout::with_injected_roots(state.to_path_buf(), None)
            .continuity_db()
            .is_err(),
        "({realm}) the staged released realm must refuse before adoption"
    );
    let output = Command::new(env!("CARGO_BIN_EXE_meta_gateway"))
        .args(["storage-adopt-predecessor", "--state-dir"])
        .arg(state)
        .args(["--gateway-home"])
        .arg(state.join("gateway-home"))
        .args(["--apply", "--json"])
        .output()
        .expect("run meta_gateway storage-adopt-predecessor");
    assert!(
        output.status.success(),
        "({realm}) predecessor adoption failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).expect("adoption report json");
    let applied = |item: &str| {
        report["actions"]
            .as_array()
            .expect("actions")
            .iter()
            .filter(|action| action["item"] == item && action["outcome"] == "applied")
            .count()
    };
    assert_eq!(applied("database_file"), 2, "({realm}) {report}");
    assert!(applied("ledger_domain") >= 3, "({realm}) {report}");
    assert_eq!(applied("metadata_table"), 1, "({realm}) {report}");
    assert!(
        meerkat_meta::storage_predecessor::predecessor_database_files(state).is_empty(),
        "({realm}) adoption left predecessor stores behind"
    );
}

fn copy_dir_recursive(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create realm copy dir");
    for entry in std::fs::read_dir(from).expect("read fixture realm dir") {
        let entry = entry.expect("fixture realm dir entry");
        let target = to.join(entry.file_name());
        let file_type = entry.file_type().expect("fixture entry file type");
        if file_type.is_dir() {
            copy_dir_recursive(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy fixture realm file");
        }
    }
}

// ---------------------------------------------------------------------------
// Child gateway process (the established stdin-JSONL harness, as in
// identity_first_subprocess_reboot.rs)
// ---------------------------------------------------------------------------

struct Gateway {
    label: String,
    child: Child,
    stdin: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
    stderr: Arc<Mutex<Vec<String>>>,
}

impl Gateway {
    fn start(label: &str) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_rpc_gateway"));
        command
            .arg("--persistent")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // Shipped configuration: memo enabled. Removed explicitly so an
        // ambient developer export cannot silently change what this lane
        // exercises. Every boot is a cold process either way.
        command.env_remove(DECODE_MEMO_KILL_SWITCH);
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

    /// Task #21 pin: a boot over healthy (or once-healed) durable state must
    /// classify CLEAN — zero Broken classifications, zero continuity-repair
    /// activity. A heal that does not persist a clean boundary re-Breaks on
    /// the NEXT boot (the 2026-07-29 heal/re-Break production loop), and this
    /// is where that would surface.
    fn assert_no_repair_activity(&self) {
        let Ok(sink) = self.stderr.lock() else {
            panic!("[{}] gateway stderr unavailable: poisoned lock", self.label);
        };
        for marker in [
            "marking identity Broken",
            "continuity repair",
            "continuity heal",
        ] {
            if let Some(line) = sink.iter().find(|line| line.contains(marker)) {
                panic!(
                    "({}) boot must classify clean, but the gateway logged repair \
                     activity ({marker}): {line}",
                    self.label
                );
            }
        }
    }

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
/// provider (`has_roster_provider`); any other callback means the init params
/// installed a bridge this test does not implement, and a stub answer would
/// invalidate the run.
fn answer_host_callback(gateway: &mut Gateway, message: &Value) {
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return;
    };
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

/// Boot the CURRENT gateway over `state` with the same identity-first init
/// the released capture driver used: the gateway opens its own continuity
/// substrate in the state dir - the shipped composition whose released-realm
/// upgrade is under test.
fn boot(label: &str, state: &Path) -> Gateway {
    let mut gateway = Gateway::start(label);
    let response = gateway.call(
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
    );
    assert!(
        response["result"]["contract_version"].is_string(),
        "[{label}] identity-first init returned no contract version: {response}"
    );
    gateway
}

/// Queue one turn for `identity`. `turn_driven` members process it as exactly
/// one turn. `content` is a bare JSON string (`ContentInput` is untagged).
fn send_turn(gateway: &mut Gateway, identity: &str, prompt: &str) {
    let response = gateway.call(
        "send",
        "meta/send",
        json!({ "identity": identity, "content": prompt }),
        RPC_TIMEOUT,
    );
    assert!(
        response["result"]["fencing_token"].is_number(),
        "[{}] meta/send did not return a fencing token: {response}",
        gateway.label
    );
}

/// Assert the resumed identity is healthy and still bound to the RELEASED
/// session: rotation across the version boundary is data loss, not recovery.
fn assert_identity_bound(gateway: &mut Gateway, identity: &str, session_id: &str) {
    let response = gateway.call(
        "status",
        "meta/status_identity",
        json!({ "identity": identity }),
        RPC_TIMEOUT,
    );
    let result = &response["result"];
    // A non-active identity here is almost always a typed restore failure, and
    // `meta/status_identity` does not carry the reason. Fold the bootstrap
    // status into the assertion message so the failure names itself, rather
    // than leaving the next reader to guess from a bare "broken".
    let boot_diagnostic = if result["state"] == json!("active") {
        String::new()
    } else {
        format!(
            " bootstrap status: {}",
            gateway.call(
                "bootdiag",
                "meta/status_identity_bootstrap",
                json!({}),
                RPC_TIMEOUT,
            )
        )
    };
    assert_eq!(
        result["state"],
        json!("active"),
        "[{}] released-realm resume left the identity non-active: {response}{boot_diagnostic}",
        gateway.label
    );
    assert_eq!(
        result["session_id"],
        json!(session_id),
        "[{}] resume rebound the identity away from the released session: {response}",
        gateway.label
    );
    assert_eq!(
        result["continuity_health"]["store_reachable"],
        json!(true),
        "[{}] continuity store unreachable after released-realm resume: {response}",
        gateway.label
    );
}

/// The clean-resume sweep: the roster materialized at least one member and
/// NONE of them is Broken.
fn assert_no_broken_members(gateway: &mut Gateway) {
    let response = gateway.call("members", "meta/list_members", json!({}), RPC_TIMEOUT);
    let members = response["result"].as_array().unwrap_or_else(|| {
        panic!(
            "[{}] meta/list_members did not return an array: {response}",
            gateway.label
        )
    });
    assert!(
        !members.is_empty(),
        "[{}] released-realm resume produced an empty member roster",
        gateway.label
    );
    for member in members {
        let state = member["state"].as_str().unwrap_or("<missing>");
        assert_ne!(
            state, "broken",
            "[{}] a member came back Broken from the released realm: {member}",
            gateway.label
        );
    }
}

// ---------------------------------------------------------------------------
// Durable state (read from disk, never from a handle)
// ---------------------------------------------------------------------------

/// The continuity file. Its name is the same before and after predecessor
/// adoption, and the released inspection copies are read before adoption
/// (when the layout refuses to resolve anything), so it is joined directly.
fn continuity_db(state: &Path) -> PathBuf {
    state.join(meerkat_meta::storage_layout::CANONICAL_CONTINUITY_DB_FILE_NAME)
}

fn runtime_db(state: &Path) -> PathBuf {
    MetaStorageLayout::with_injected_roots(state.to_path_buf(), None).runtime_db()
}

/// Open the durable file the way a reader must: an ordinary read/write path.
/// `?immutable=1` ignores the `-wal` sidecar and manufactures a 0-byte file
/// at a missing path, which is exactly how a probe lies about durability.
/// NEVER call this on a committed fixture path - only on staged copies.
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
    fn joined_json(&self) -> String {
        self.messages
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn read_durable_transcript(state: &Path, phase: &str) -> DurableTranscript {
    let db = continuity_db(state);
    assert!(
        db.exists(),
        "PRECONDITION FAILED ({phase}): {} does not exist. State dir: {:?}",
        db.display(),
        state_dir_listing(state)
    );
    let conn = open_durable(&db);
    assert!(
        table_exists(&conn, "continuity_session_heads"),
        "PRECONDITION FAILED ({phase}): {} has no continuity_session_heads table - session I/O \
         never rode the continuity adapter, so nothing asserted after this point is meaningful. \
         State dir: {:?}",
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
         {message_count} prefix - the head claims messages its strand rows cannot serve"
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

fn continuity_identity_records(state: &Path) -> Vec<(String, String)> {
    let conn = open_durable(&continuity_db(state));
    let mut stmt = conn
        .prepare("SELECT identity, session_id FROM continuity_records ORDER BY 1")
        .expect("prepare continuity records");
    stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })
    .expect("query continuity records")
    .collect::<Result<Vec<_>, _>>()
    .expect("collect continuity records")
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
/// messages.
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
/// The ONE sanctioned released-provenance drop, normalized before row
/// comparison: the released 0.8.10 System wire carried a `mutation_kind`
/// provenance field that the 0.8.11 importer deliberately does not carry
/// forward (the current `SystemMessage` cannot represent it; its successor is
/// the `identity` carrier, which ordinary System rows omit). The first
/// write-path adoption re-encodes the strand without it, so a driven realm's
/// prefix diverges from the released bytes by EXACTLY this key on System
/// rows. Every other field of every row must remain identical - this is a
/// targeted allowance, never a typed re-decode that would launder broader
/// drift.
fn strip_released_system_provenance(row: &Value) -> Value {
    let mut row = row.clone();
    if row.get("role").and_then(Value::as_str) == Some("system")
        && let Some(map) = row.as_object_mut()
    {
        map.remove("mutation_kind");
    }
    row
}

fn assert_exact_prefix(previous: &DurableTranscript, current: &DurableTranscript, phase: &str) {
    assert_eq!(
        previous.session_id, current.session_id,
        "({phase}) the durable session was ROTATED across the version boundary \
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
            strip_released_system_provenance(&current.messages[index]),
            strip_released_system_provenance(before),
            "({phase}) durable message {index} changed across the boundary \
             (session {}, strand {} -> {})",
            current.session_id,
            previous.strand,
            current.strand
        );
    }
}

/// Message count of the runtime store's whole-blob snapshot for `session_id`.
/// meerkat 0.8.11 serves boundary commits from the
/// `runtime_whole_blob_authority` + `runtime_whole_blob_bodies` pair; the
/// `runtime_session_snapshots` table the RELEASED binary wrote is the legacy
/// representation the upgrade must migrate away from.
fn whole_blob_snapshot_message_count(state: &Path, session_id: &str) -> Option<i64> {
    let db = runtime_db(state);
    assert!(
        db.exists(),
        "runtime store {} does not exist - cannot verify the upgraded representation",
        db.display()
    );
    let conn = open_durable(&db);
    assert!(
        table_exists(&conn, "runtime_whole_blob_authority"),
        "runtime store {} has no runtime_whole_blob_authority table - the released realm was \
         never migrated to the current runtime-store schema",
        db.display()
    );
    let mut stmt = conn
        .prepare(
            "SELECT bodies.session_snapshot \
             FROM runtime_whole_blob_authority AS authority \
             JOIN runtime_whole_blob_bodies AS bodies \
               ON bodies.blob_sha256 = authority.blob_sha256",
        )
        .expect("prepare whole-blob snapshots");
    let blobs = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .expect("query whole-blob snapshots")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect whole-blob snapshots");
    for blob in blobs {
        let value: Value = serde_json::from_slice(&blob).expect("parse persisted session envelope");
        if value.get("id").and_then(Value::as_str) == Some(session_id) {
            let count = value
                .get("messages")
                .and_then(Value::as_array)
                .map(Vec::len)
                .expect("persisted session envelope has a messages array");
            return Some(i64::try_from(count).expect("message count fits"));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// The drive
// ---------------------------------------------------------------------------

/// Boot the CURRENT build over a copy of the released realm, drive one turn
/// per pre-existing identity, shut down cleanly, verify the upgrade on disk;
/// then reboot once more and prove the state the NEW build wrote reads back.
fn drive_released_realm(spec: &RealmSpec) {
    let fixture = fixture_realm_dir(spec.name);
    assert!(
        fixture.is_dir(),
        "committed released realm missing at {} - the R1 fixtures were minted by the RELEASED \
         0.8.8 rpc_gateway and can only be restored from git history, never regenerated with \
         current code",
        fixture.display()
    );

    // --- Fixture byte pins (metadata only; never open a fixture) -----------
    let main_len = std::fs::metadata(fixture.join("continuity.sqlite3"))
        .expect("stat committed continuity main file")
        .len();
    assert_eq!(
        main_len, spec.continuity_main_bytes,
        "({}) the committed continuity.sqlite3 main file changed size - the released byte shape \
         was altered in-repo",
        spec.name
    );
    let wal_len = std::fs::metadata(fixture.join("continuity.sqlite3-wal"))
        .expect("stat committed continuity WAL")
        .len();
    assert_eq!(
        wal_len, spec.continuity_wal_bytes,
        "({}) the committed continuity.sqlite3-wal changed size. The released transcript lives \
         ONLY in this WAL; any read/write sqlite open of the fixture checkpoints and truncates \
         it. Restore the fixture from git and never open fixture files in place",
        spec.name
    );

    let temp = tempfile::TempDir::new().expect("temp dir");

    // --- Released truth, read from a dedicated inspection copy -------------
    //
    // A separate copy so the DRIVE copy's first opener is the gateway under
    // test: even a read-only-intent rusqlite open would replay the WAL into
    // the -shm and checkpoint on close, and the whole point of this lane is
    // that the NEW build performs first contact with the released bytes.
    let inspect = temp.path().join("released-inspect");
    stage_realm_copy(&fixture, &inspect);
    let released = read_durable_transcript(&inspect, &format!("{} released fixture", spec.name));
    assert_eq!(
        released.session_id, spec.session_id,
        "({}) the committed realm no longer holds the released session",
        spec.name
    );
    assert_eq!(
        released.message_count, spec.released_message_count,
        "({}) the committed realm's released head count drifted",
        spec.name
    );
    let released_joined = released.joined_json();
    for token in spec.released_tokens {
        assert!(
            released_joined.contains(token),
            "({}) released marker {token} missing from the committed realm",
            spec.name
        );
    }
    let identities: Vec<String> = continuity_identity_records(&inspect)
        .into_iter()
        .map(|(identity, session)| {
            assert_eq!(
                session, spec.session_id,
                "({}) released identity record names an unexpected session",
                spec.name
            );
            identity
        })
        .collect();
    assert_eq!(
        identities,
        vec![IDENTITY.to_string()],
        "({}) the released realm's pre-existing identity set drifted",
        spec.name
    );

    // --- Adoption: the NEW build's first contact with released bytes -------
    let state = temp.path().join("state");
    stage_realm_copy(&fixture, &state);
    adopt_predecessor_names(spec.name, &state);

    // --- Boot A: the first gateway boot over the adopted realm -------------

    let label_a = format!("{}/boot-A", spec.name);
    let mut gateway = boot(&label_a, &state);
    assert_ne!(
        gateway.pid(),
        std::process::id(),
        "({label_a}) the gateway must run as a separate OS process"
    );
    assert_identity_bound(&mut gateway, IDENTITY, spec.session_id);
    assert_no_broken_members(&mut gateway);

    let marker_a = format!("MARKER-UPGRADE-{}-BOOT-A", spec.name);
    let mut floor = spec.released_message_count;
    for identity in &identities {
        send_turn(
            &mut gateway,
            identity,
            &format!("Please note this token: {marker_a}"),
        );
        floor += MESSAGES_PER_TURN;
        wait_for_durable_messages(&state, floor, &format!("{label_a} turn for {identity}"));
    }
    gateway.assert_no_repair_activity();
    gateway.shutdown_and_reap();

    // --- Post-exit: the upgrade, read from bytes ----------------------------
    let after_a = read_durable_transcript(&state, &format!("{label_a} post-turn"));
    eprintln!(
        "PROBE {label_a}: session={} strand={} messages={}",
        after_a.session_id, after_a.strand, after_a.message_count
    );
    assert_exact_prefix(&released, &after_a, &format!("{label_a} post-turn"));
    assert!(
        after_a.message_count >= floor,
        "({label_a}) the driven turn did not extend the released head: {} < {floor}",
        after_a.message_count
    );
    let joined_a = after_a.joined_json();
    assert!(
        joined_a.contains(&marker_a),
        "({label_a}) the driven turn's marker never became durable"
    );
    for token in spec.released_tokens {
        assert!(
            joined_a.contains(token),
            "({label_a}) released marker {token} was lost by the upgrade"
        );
    }
    let blob_count =
        whole_blob_snapshot_message_count(&state, spec.session_id).unwrap_or_else(|| {
            panic!(
                "({label_a}) the upgraded runtime store holds no whole-blob snapshot for the \
                 released session {}",
                spec.session_id
            )
        });
    assert!(
        blob_count >= floor,
        "({label_a}) the whole-blob snapshot lags the driven boundary: {blob_count} < {floor}"
    );

    // --- Boot B: reboot into state written by the NEW build ----------------
    let label_b = format!("{}/boot-B", spec.name);
    let mut gateway = boot(&label_b, &state);
    assert_identity_bound(&mut gateway, IDENTITY, spec.session_id);
    assert_no_broken_members(&mut gateway);

    // What this cold process inherited must be exactly what the new build's
    // previous process left behind: upgrade once, then continue.
    let inherited = read_durable_transcript(&state, &format!("{label_b} pre-turn"));
    assert_eq!(
        inherited.message_count, after_a.message_count,
        "({label_b}) resuming the upgraded realm mutated the durable transcript length before \
         any turn: {} -> {}",
        after_a.message_count, inherited.message_count
    );
    assert_exact_prefix(&after_a, &inherited, &format!("{label_b} pre-turn"));

    let marker_b = format!("MARKER-UPGRADE-{}-BOOT-B", spec.name);
    for identity in &identities {
        send_turn(&mut gateway, identity, &format!("Second token: {marker_b}"));
        floor += MESSAGES_PER_TURN;
        wait_for_durable_messages(&state, floor, &format!("{label_b} turn for {identity}"));
    }
    // Task #21 two-boot property on real released bytes: boot B inherits
    // state the NEW build wrote (boot A upgraded, drove, and shut down
    // clean) and must classify CLEAN — zero repairs. On the crash realm this
    // additionally pins heal-then-clean over SIGKILL residue.
    gateway.assert_no_repair_activity();
    gateway.shutdown_and_reap();

    let after_b = read_durable_transcript(&state, &format!("{label_b} post-turn"));
    eprintln!(
        "PROBE {label_b}: session={} strand={} messages={}",
        after_b.session_id, after_b.strand, after_b.message_count
    );
    assert_exact_prefix(&after_a, &after_b, &format!("{label_b} post-turn"));
    assert!(
        after_b.message_count >= floor,
        "({label_b}) the post-upgrade turn did not extend the durable head: {} < {floor}",
        after_b.message_count
    );
    let joined_b = after_b.joined_json();
    for token in spec
        .released_tokens
        .iter()
        .copied()
        .chain([marker_a.as_str(), marker_b.as_str()])
    {
        assert!(
            joined_b.contains(token),
            "({label_b}) the durable transcript lost marker {token} after the second reboot"
        );
    }

    // One identity, one continuity record, still naming the released session.
    assert_eq!(
        continuity_identity_records(&state),
        vec![(IDENTITY.to_string(), spec.session_id.to_string())],
        "({}) the identity binding must survive the upgrade pointing at the released session",
        spec.name
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn released_0_8_8_baseline_realm_upgrades_and_drives() {
    drive_released_realm(&BASELINE);
}

#[test]
fn released_0_8_8_burst_drain_realm_upgrades_and_drives() {
    drive_released_realm(&BURST_DRAIN);
}

/// The SIGKILL realm. Pinned truth first (see the module docs): the kill
/// landed AT IDLE after the burst drained, so the realm holds consumed
/// inputs and an in-sync legacy snapshot - the crash residue is the
/// un-checkpointed WAL byte shape and the missing shutdown attestation.
/// Recovery must therefore be FULL re-activation; a member parked Broken or
/// a rotated session over these bytes is a regression, not caution.
#[test]
fn released_0_8_8_sigkill_crash_realm_recovers_upgrades_and_drives() {
    // Pin what the SIGKILL actually froze, on a dedicated inspection copy,
    // so the recovery expectation below stays anchored to evidence.
    let temp = tempfile::TempDir::new().expect("temp dir");
    let inspect = temp.path().join("crash-runtime-inspect");
    stage_realm_copy(&fixture_realm_dir(CRASH_SIGKILL.name), &inspect);
    let conn = open_durable(&runtime_db(&inspect));

    let mut stmt = conn
        .prepare("SELECT state_json FROM runtime_input_states")
        .expect("prepare crash input states");
    let states = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .expect("query crash input states")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect crash input states")
        .into_iter()
        .map(|bytes| {
            serde_json::from_slice::<Value>(&bytes)
                .expect("parse stored input state")
                .get("current_state")
                .and_then(Value::as_str)
                .expect("stored input state names current_state")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        states.len(),
        CRASH_INPUT_ROWS,
        "the crash realm's durable input ledger drifted from the released capture"
    );
    assert!(
        states.iter().all(|state| state == "consumed"),
        "the crash realm froze non-consumed inputs; the pinned recovery expectation (full \
         re-activation, no typed parking) no longer holds. Observed states: {states:?}"
    );

    // The legacy (released-representation) snapshot is in sync with the
    // continuity head: SIGKILL landed between turns, not mid-save.
    let snapshot_rows = conn
        .query_row(
            "SELECT COUNT(*) FROM runtime_session_snapshots",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect("count legacy runtime snapshots");
    assert_eq!(
        snapshot_rows, 1,
        "the crash realm must hold exactly one legacy runtime snapshot"
    );
    let snapshot = conn
        .query_row(
            "SELECT session_snapshot FROM runtime_session_snapshots",
            [],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .expect("read the legacy runtime snapshot");
    let envelope: Value = serde_json::from_slice(&snapshot).expect("parse legacy snapshot");
    assert_eq!(
        envelope.get("id").and_then(Value::as_str),
        Some(CRASH_SIGKILL.session_id),
        "the legacy runtime snapshot names an unexpected session"
    );
    assert_eq!(
        envelope
            .get("messages")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(usize::try_from(CRASH_SIGKILL.released_message_count).expect("count fits")),
        "the legacy runtime snapshot is not in sync with the released continuity head; the \
         crash-shape truth this test pins has drifted"
    );
    drop(stmt);
    drop(conn);

    drive_released_realm(&CRASH_SIGKILL);
}

#[test]
fn released_0_8_8_deploy_cycles_realm_upgrades_and_drives() {
    drive_released_realm(&DEPLOY_CYCLES);
}

/// The downstream app's binding leg, byte-for-byte: a RESET runtime store over a
/// RELEASED-minted head-canonical continuity store.
///
/// The runtime store is deleted whole (the sanctioned reset/loss shape the
/// facade mint reseeds), so the mint's durable read is the ONLY path back to
/// the session - and on a released realm that read materializes from HEAD
/// ROWS carrying v2 session envelopes. Before the head-lane importer, this
/// leg failed uniformly in the field (a downstream app, 17/17 identities): "durable
/// session read for runtime-authority mint: failed to restore session from
/// head row: generated session persistence version authority rejected
/// SessionEnvelope: expected current 3, got 2" - the blob-lane importer
/// covered whole-blob rows (the production shape) while head-canonical fleets
/// refused. The fail-closed side worked as designed (durable rows preserved,
/// identities degraded pending retry); the lane was missing its importer.
///
/// The earlier reset regression
/// (`reset_runtime_store_reseeds_from_continuity_and_resumes`) resets a
/// NEW-BUILD-written realm, whose continuity rows are already current (v3);
/// this test is the missing case: reset over RELEASED v2 continuity.
#[test]
fn released_0_8_8_continuity_with_reset_runtime_store_mints_from_head_rows() {
    let spec = &BASELINE;
    let fixture = fixture_realm_dir(spec.name);
    assert!(
        fixture.is_dir(),
        "committed released realm missing at {} - restore from git history, never regenerate",
        fixture.display()
    );
    let temp = tempfile::TempDir::new().expect("temp dir");

    // Released truth from a dedicated inspection copy, same discipline as
    // `drive_released_realm`: the DRIVE copy's first opener must be the
    // gateway under test.
    let inspect = temp.path().join("released-inspect");
    stage_realm_copy(&fixture, &inspect);
    let released = read_durable_transcript(&inspect, "reset-mint released fixture");
    assert_eq!(
        released.session_id, spec.session_id,
        "the committed realm no longer holds the released session"
    );
    assert_eq!(
        released.message_count, spec.released_message_count,
        "the committed realm's released head count drifted"
    );

    // Stage the drive copy, then RESET the runtime store: delete
    // runtime.sqlite and every sidecar while the released continuity bytes
    // stay untouched (the downstream app's reset/purge shape).
    let state = temp.path().join("state");
    stage_realm_copy(&fixture, &state);
    let mut removed = Vec::new();
    for name in [
        "runtime.sqlite",
        "runtime.sqlite-wal",
        "runtime.sqlite-shm",
        "runtime.sqlite-journal",
        "runtime.sqlite.mfence",
    ] {
        let path = state.join(name);
        if path.exists() {
            std::fs::remove_file(&path)
                .unwrap_or_else(|error| panic!("reset runtime store file {name}: {error}"));
            removed.push(name);
        }
    }
    assert!(
        removed.contains(&"runtime.sqlite"),
        "the released realm carries no runtime store; the reset under test never armed"
    );

    adopt_predecessor_names(spec.name, &state);

    // Boot over the reset: identity Active, bound to the released session,
    // nobody Broken. This is exactly where the field failure surfaced.
    let label = "baseline/reset-mint";
    let mut gateway = boot(label, &state);
    assert_identity_bound(&mut gateway, IDENTITY, spec.session_id);
    assert_no_broken_members(&mut gateway);

    let marker = "MARKER-RESET-MINT-BASELINE";
    send_turn(
        &mut gateway,
        IDENTITY,
        &format!("Please note this token: {marker}"),
    );
    let floor = spec.released_message_count + MESSAGES_PER_TURN;
    wait_for_durable_messages(&state, floor, &format!("{label} turn"));
    gateway.shutdown_and_reap();

    // v2 -> v3 from head rows, prefix byte-exact, real turn appended.
    let after = read_durable_transcript(&state, &format!("{label} post-turn"));
    eprintln!(
        "PROBE {label}: session={} strand={} messages={}",
        after.session_id, after.strand, after.message_count
    );
    assert_exact_prefix(&released, &after, &format!("{label} post-turn"));
    assert!(
        after.message_count >= floor,
        "({label}) the post-reset turn did not extend the released head: {} < {floor}",
        after.message_count
    );
    let joined = after.joined_json();
    assert!(
        joined.contains(marker),
        "({label}) the post-reset turn's marker never became durable"
    );
    for token in spec.released_tokens {
        assert!(
            joined.contains(token),
            "({label}) released marker {token} was lost by the reset mint"
        );
    }
    // The mint's seed must be store-issued runtime authority: the reset
    // runtime store holds a whole-blob snapshot at (at least) the driven
    // boundary.
    let blob_count =
        whole_blob_snapshot_message_count(&state, spec.session_id).unwrap_or_else(|| {
            panic!(
                "({label}) the reseeded runtime store holds no whole-blob snapshot for the \
                 released session {}",
                spec.session_id
            )
        });
    assert!(
        blob_count >= floor,
        "({label}) the whole-blob snapshot lags the driven boundary: {blob_count} < {floor}"
    );

    // Boot B over the minted state: continue, never re-import or rotate.
    let label_b = "baseline/reset-mint-boot-B";
    let mut gateway = boot(label_b, &state);
    assert_identity_bound(&mut gateway, IDENTITY, spec.session_id);
    assert_no_broken_members(&mut gateway);
    gateway.shutdown_and_reap();
    assert_eq!(
        continuity_identity_records(&state),
        vec![(IDENTITY.to_string(), spec.session_id.to_string())],
        "the identity binding must survive the reset mint pointing at the released session"
    );
}
