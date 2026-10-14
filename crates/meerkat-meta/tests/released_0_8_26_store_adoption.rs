//! Acceptance lane for a persistent store MINTED BY THE RELEASED MobKit
//! 0.8.26 `rpc_gateway`, adopted and resumed by the build under test.
//!
//! The store under `tests/fixtures/released_0_8_26_store/state/` was written
//! by the public-release 0.8.26 gateway, never by current code (see
//! `mint_released_0_8_26_store.py` beside it, and tests/fixtures/README.md):
//! one mob created on persistent SQLite mob storage, so its event log
//! `event_log.sqlite3` carries the mob's events and MobKit's composition
//! manifest `event_log.sqlite3.composition.json` sits beside it with the
//! creator field spelled `created_by_mobkit`; one durable identity with its
//! member session and one driven turn; two agent memories; one memory
//! ledger assertion. The fixture is byte-lossless (`checksums.sha256` pins
//! every file) and is never opened in place: every test works on a staged
//! copy.
//!
//! The upgrade a 0.8.x installation performs, end to end:
//!
//! 1. `meta_gateway storage-adopt-predecessor --state-dir <state>` (dry run)
//!    lists every step, the composition manifest rewrite included, and
//!    changes nothing;
//! 2. `--apply` completes, and the manifest differs from the released bytes
//!    in the creator field's name only;
//! 3. the build under test boots over the adopted store with the definition
//!    the store was created with, byte for byte (and again with the
//!    `tools.mob_tasks` key 0.9 ignores removed): composition verification
//!    passes, the identity resumes bound to the released session, memory
//!    recall returns the released memories, and the event log, the ledger
//!    and the transcript read back.
//!
//! A store adopted before the manifest step existed (as Meta 0.9.0 left it:
//! adoption complete, manifest still predecessor-spelled) resumes too, on
//! the exact path 0.9.0 refused with -32019 `malformed` (persistent mob
//! storage, non-empty event log, authoritative launch) and as a candidate
//! launch, whose released-representation allowance reads the manifest; and
//! `meta_repair --composition-manifests` detects and repairs its manifest.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::uninlined_format_args
)]

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use meerkat_meta::storage_layout::MetaStorageLayout;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[path = "support/gateway_shutdown.rs"]
mod gateway_shutdown;

// ---------------------------------------------------------------------------
// What the released capture holds (pinned from the capture's own summary)
// ---------------------------------------------------------------------------

/// The exact mob definition the released gateway created the store for.
/// 0.8.26 accepted `tools.mob_tasks` (its profile tool config ignored the
/// unknown key, so the knob never reached the stored definition); 0.9 warns
/// that it ignores it.
const MOB_CONFIG: &str = r#"[mob]
id = "adoption-fixture-planners"

[profiles.planner]
model = "gpt-5.5"
external_addressable = true
runtime_mode = "turn_driven"

[profiles.planner.tools]
comms = true
mob_tasks = true
"#;

/// The durable identity the released roster declared.
const IDENTITY: &str = "learner:fixture-0001";
/// Its member id in the released event log.
const MEMBER_ID: &str = "mk--learner_cfixture-0001";
/// The member session the released gateway bound the identity to.
const SESSION_ID: &str = "01a13aaa-4918-71e2-b44c-366e9b2d4de3";
/// The two agent memories the released gateway stored for the identity.
const MEMORIES: [(&str, &str); 2] = [
    (
        "mem-1791985601402882094-222c50-0-18395066da7f0be3",
        "FIXTURE-0826-MEMORY-ONE: the synthetic learner prefers short lessons.",
    ),
    (
        "mem-1791985601407345168-222c50-1-6e92bf82a622ec38",
        "FIXTURE-0826-MEMORY-TWO: the synthetic learner finished unit one.",
    ),
];
/// The marker of the one turn the released gateway drove.
const TURN_TOKEN: &str = "FIXTURE-0826-TOKEN-ALPHA";
/// The memory ledger assertion the released gateway indexed.
const LEDGER_ENTITY: &str = "fixture-curriculum";
const LEDGER_ASSERTION_ID: &str = "memory-assert-000000";

const MANIFEST_FILE: &str = "event_log.sqlite3.composition.json";
const PREDECESSOR_CREATOR_KEY: &str = "\"created_by_mobkit\"";
const CREATOR_KEY: &str = "\"created_by_meta\"";

const INIT_TIMEOUT: Duration = Duration::from_mins(3);
const RPC_TIMEOUT: Duration = Duration::from_mins(1);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_mins(3);
const EXIT_TIMEOUT: Duration = Duration::from_mins(1);

fn roster_spec() -> Value {
    json!({
        "identity": IDENTITY,
        "profile": "planner",
        "addressability": "addressable",
        "labels": { "cohort": "fixture" },
        "additional_instructions": ["Answer in one sentence."]
    })
}

/// The released definition without the key 0.9 ignores.
fn mob_config_without_ignored_key() -> String {
    let stripped = MOB_CONFIG.replace("mob_tasks = true\n", "");
    assert_ne!(
        stripped, MOB_CONFIG,
        "the released definition names mob_tasks"
    );
    stripped
}

// ---------------------------------------------------------------------------
// Fixture staging
// ---------------------------------------------------------------------------

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/released_0_8_26_store")
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn files_under(root: &Path, dir: &Path, out: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).expect("read fixture dir") {
        let path = entry.expect("fixture dir entry").path();
        if path.is_dir() {
            files_under(root, &path, out);
        } else {
            out.push(
                path.strip_prefix(root)
                    .expect("fixture file under the fixture root")
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/"),
            );
        }
    }
}

/// The committed store is exactly the released capture: every file listed in
/// `checksums.sha256` with its digest, and nothing else. Hashing reads the
/// files; it never opens them as databases.
fn assert_fixture_is_the_released_capture() {
    let root = fixture_dir();
    let listing =
        std::fs::read_to_string(root.join("checksums.sha256")).expect("read fixture checksums");
    let mut pinned = Vec::new();
    for line in listing.lines() {
        let (digest, file) = line
            .split_once("  ")
            .unwrap_or_else(|| panic!("malformed checksum line {line:?}"));
        let bytes = std::fs::read(root.join(file))
            .unwrap_or_else(|error| panic!("read fixture file {file}: {error}"));
        assert_eq!(
            sha256_hex(&bytes),
            digest,
            "fixture file {file} no longer holds the released bytes; restore it from git and \
             never open fixture files in place"
        );
        pinned.push(file.to_string());
    }
    let mut present = Vec::new();
    files_under(&root, &root.join("state"), &mut present);
    pinned.sort();
    present.sort();
    assert_eq!(
        present, pinned,
        "the fixture state holds files outside its checksums"
    );
}

fn copy_dir_recursive(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create staged dir");
    for entry in std::fs::read_dir(from).expect("read fixture dir") {
        let entry = entry.expect("fixture dir entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("fixture entry type").is_dir() {
            copy_dir_recursive(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy fixture file");
        }
    }
}

/// Stage one pristine copy of the released store into `<temp>/state`.
fn stage_store(temp: &Path) -> PathBuf {
    assert_fixture_is_the_released_capture();
    let state = temp.join("state");
    copy_dir_recursive(&fixture_dir().join("state"), &state);
    // The released state holds an EMPTY `blobs/` directory, which git cannot
    // track; restore it rather than committing a placeholder.
    std::fs::create_dir_all(state.join("blobs")).expect("restore empty blobs dir");
    state
}

fn released_manifest() -> String {
    std::fs::read_to_string(fixture_dir().join("state").join(MANIFEST_FILE))
        .expect("read the released composition manifest")
}

/// The released manifest with the creator field's name changed and every
/// other byte kept.
fn adopted_manifest() -> String {
    let released = released_manifest();
    assert_eq!(
        released.matches(PREDECESSOR_CREATOR_KEY).count(),
        1,
        "the released manifest spells its creator field once: {released}"
    );
    assert!(!released.contains(CREATOR_KEY), "{released}");
    released.replacen(PREDECESSOR_CREATOR_KEY, CREATOR_KEY, 1)
}

fn manifest_text(state: &Path) -> String {
    std::fs::read_to_string(state.join(MANIFEST_FILE)).expect("read the staged manifest")
}

// ---------------------------------------------------------------------------
// Maintenance verbs (each a process of the build under test)
// ---------------------------------------------------------------------------

struct VerbRun {
    code: i32,
    stdout: String,
    stderr: String,
}

impl VerbRun {
    fn json(&self) -> Value {
        serde_json::from_str(&self.stdout).unwrap_or_else(|error| {
            panic!(
                "stdout is not a JSON report ({error})\nstdout: {}\nstderr: {}",
                self.stdout, self.stderr
            )
        })
    }
}

fn run_binary(binary: &str, args: &[&str], home: &Path) -> VerbRun {
    let output = Command::new(binary)
        .args(args)
        .env("HOME", home)
        .env("XDG_STATE_HOME", home.join(".local").join("state"))
        .output()
        .unwrap_or_else(|error| panic!("spawn {binary}: {error}"));
    VerbRun {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

fn adopt(state: &Path, home: &Path, apply: bool) -> Value {
    let state_arg = state.to_str().expect("utf-8 state path");
    let mut args = vec![
        "storage-adopt-predecessor",
        "--state-dir",
        state_arg,
        "--json",
    ];
    if apply {
        args.push("--apply");
    }
    let run = run_binary(env!("CARGO_BIN_EXE_meta_gateway"), &args, home);
    assert_eq!(
        run.code, 0,
        "storage-adopt-predecessor (apply={apply}) failed\nstdout: {}\nstderr: {}",
        run.stdout, run.stderr
    );
    run.json()
}

fn actions_of<'a>(report: &'a Value, item: &str) -> Vec<&'a Value> {
    report["actions"]
        .as_array()
        .expect("report actions")
        .iter()
        .filter(|action| action["item"] == item)
        .collect()
}

/// The report names exactly one composition manifest action, for the
/// store's manifest, renaming the creator field, with `outcome`.
fn assert_manifest_action(report: &Value, state: &Path, outcome: &str) {
    let actions = actions_of(report, "composition_manifest");
    assert_eq!(actions.len(), 1, "one manifest action expected: {report:#}");
    let action = actions[0];
    assert_eq!(action["outcome"], outcome, "{report:#}");
    assert_eq!(
        action["subject"],
        json!(state.join(MANIFEST_FILE).display().to_string()),
        "{report:#}"
    );
    assert_eq!(action["from"], "created_by_mobkit", "{report:#}");
    assert_eq!(action["to"], "created_by_meta", "{report:#}");
}

// ---------------------------------------------------------------------------
// Child gateway process (stdin-JSONL, as the SDKs drive it)
// ---------------------------------------------------------------------------

struct Gateway {
    label: String,
    child: Child,
    stdin: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
    stderr: Arc<Mutex<Vec<String>>>,
}

impl Gateway {
    /// A cold `rpc_gateway --persistent` with a private HOME, so nothing it
    /// derives ambiently (session comms identities, gateway home) touches the
    /// invoking user's state.
    fn start(label: &str, home: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_rpc_gateway"))
            .arg("--persistent")
            .env("HOME", home)
            .env("XDG_STATE_HOME", home.join(".local").join("state"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn rpc_gateway --persistent");
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
        let sink = Arc::clone(&stderr);
        thread::spawn(move || {
            for line in BufReader::new(child_stderr).lines() {
                let Ok(line) = line else { break };
                let Ok(mut lines) = sink.lock() else { break };
                if lines.len() < 2000 {
                    lines.push(line);
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

    fn stderr_tail(&self) -> String {
        let Ok(lines) = self.stderr.lock() else {
            return "[gateway stderr unavailable]".to_string();
        };
        let start = lines.len().saturating_sub(40);
        format!(
            "--- [{}] gateway stderr (last {} lines) ---\n{}",
            self.label,
            lines.len() - start,
            lines[start..].join("\n")
        )
    }

    fn send(&mut self, value: &Value) {
        let stdin = self.stdin.as_mut().expect("gateway stdin open");
        writeln!(stdin, "{value}").expect("write to gateway stdin");
        stdin.flush().expect("flush gateway stdin");
    }

    /// One request; the response, error or not. Host callbacks are answered
    /// while waiting: this host provides the roster and nothing else.
    fn request(&mut self, id: &str, method: &str, params: Value, deadline: Duration) -> Value {
        self.send(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        let start = Instant::now();
        loop {
            let remaining = deadline.checked_sub(start.elapsed()).unwrap_or_else(|| {
                panic!(
                    "[{}] no response to {method} within {deadline:?}\n{}",
                    self.label,
                    self.stderr_tail()
                )
            });
            let Ok(line) = self.lines.recv_timeout(remaining) else {
                panic!(
                    "[{}] the gateway closed or stalled before answering {method}\n{}",
                    self.label,
                    self.stderr_tail()
                );
            };
            let Ok(message) = serde_json::from_str::<Value>(line.trim()) else {
                continue;
            };
            if let Some(callback) = message.get("method").and_then(Value::as_str) {
                let callback = callback.to_string();
                self.answer_callback(&callback, &message);
                continue;
            }
            if message.get("id").and_then(Value::as_str) == Some(id) {
                return message;
            }
        }
    }

    /// One request that must succeed; its result.
    fn call(&mut self, id: &str, method: &str, params: Value) -> Value {
        let response = self.request(id, method, params, RPC_TIMEOUT);
        assert!(
            response.get("error").is_none(),
            "[{}] {method} failed: {response}\n{}",
            self.label,
            self.stderr_tail()
        );
        response["result"].clone()
    }

    fn answer_callback(&mut self, method: &str, message: &Value) {
        let Some(id) = message.get("id").cloned() else {
            return;
        };
        if method == "callback/roster_provider/roster" {
            self.send(&json!({ "jsonrpc": "2.0", "id": id, "result": [roster_spec()] }));
            return;
        }
        assert!(
            !method.starts_with("callback/"),
            "[{}] unexpected host callback {method}: this host provides only the roster",
            self.label
        );
    }

    fn shutdown_and_reap(mut self) {
        let response = self.request("shutdown", "meta/shutdown", json!({}), SHUTDOWN_TIMEOUT);
        gateway_shutdown::assert_persistent_shutdown_completed(
            &response,
            &format!("[{}] shutdown\n{}", self.label, self.stderr_tail()),
        );
        drop(self.stdin.take());
        let start = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("poll gateway exit") {
                assert!(
                    status.success(),
                    "[{}] rpc_gateway exited with {status}\n{}",
                    self.label,
                    self.stderr_tail()
                );
                return;
            }
            assert!(
                start.elapsed() < EXIT_TIMEOUT,
                "[{}] rpc_gateway did not exit\n{}",
                self.label,
                self.stderr_tail()
            );
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

/// Which composition authority a boot declares.
#[derive(Clone, Copy)]
enum Launch {
    /// The default: the launch speaks for the durable composition, so a
    /// resume over the persistent, non-empty event log verifies the supplied
    /// definition against the store through its composition manifest.
    Authoritative,
    /// `runtime_options.mob_composition.authority = "candidate"`: the resume
    /// boots the stored definition and refuses a supplied one that differs,
    /// after the released-representation allowance the manifest enables.
    Candidate,
}

/// Boot the build under test over `state` the way the released deployment
/// ran: same persistent state (so persistent mob storage over the released,
/// non-empty event log), same roster, same agent-memory and memory ledger
/// configuration, and the supplied definition.
fn boot(label: &str, state: &Path, home: &Path, mob_config: &str, launch: Launch) -> Gateway {
    let mut runtime_options = json!({
        "demo_llm": true,
        "identity_bootstrap_mode": { "mode": "eager_materialize" },
        "agent_memory": { "selection": "contextual", "max_entries": 4 },
        "memory_config": { "backend": "local_json" }
    });
    if matches!(launch, Launch::Candidate) {
        runtime_options["mob_composition"] = json!({ "authority": "candidate" });
    }
    let mut gateway = Gateway::start(label, home);
    let response = gateway.request(
        "init",
        "meta/init",
        json!({
            "persistent_state": state,
            "mob_config": mob_config,
            "has_roster_provider": true,
            "runtime_options": runtime_options
        }),
        INIT_TIMEOUT,
    );
    assert!(
        response.get("error").is_none(),
        "[{label}] meta/init over the adopted released store was refused: {response}\n{}",
        gateway.stderr_tail()
    );
    assert!(
        response["result"]["contract_version"].is_string(),
        "[{label}] init returned no contract version: {response}"
    );
    gateway
}

/// The identity resumed active and bound to the released session, its
/// released memories recall, and the event log and memory ledger read back.
fn assert_released_state_resumed(gateway: &mut Gateway) {
    let label = gateway.label.clone();
    let status = gateway.call(
        "status",
        "meta/status_identity",
        json!({ "identity": IDENTITY }),
    );
    assert_eq!(status["state"], "active", "[{label}] {status}");
    assert_eq!(
        status["session_id"], SESSION_ID,
        "[{label}] the identity was rebound away from its released session: {status}"
    );

    let recalled = gateway.call(
        "recall",
        "meta/agent_memory/recall",
        json!({ "identity": IDENTITY, "selection": "always", "max_entries": 8 }),
    );
    let mut records: Vec<(String, String)> = recalled["records"]
        .as_array()
        .unwrap_or_else(|| panic!("[{label}] recall returned no records array: {recalled}"))
        .iter()
        .map(|record| {
            (
                record["memory_id"].as_str().unwrap_or_default().to_string(),
                record["body"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    records.sort();
    let expected: Vec<(String, String)> = MEMORIES
        .iter()
        .map(|(id, body)| ((*id).to_string(), (*body).to_string()))
        .collect();
    assert_eq!(
        records, expected,
        "[{label}] recall must return exactly the released memories"
    );

    let events = gateway.call("events", "meta/mob_events/query", json!({}));
    let events_list = events["events"]
        .as_array()
        .unwrap_or_else(|| panic!("[{label}] no events array: {events}"));
    assert!(
        !events_list.is_empty(),
        "[{label}] the event log reads back empty"
    );
    assert!(
        events.to_string().contains(MEMBER_ID),
        "[{label}] the released member's events are missing from the event log: {events}"
    );

    let ledger = gateway.call(
        "ledger",
        "meta/memory/query",
        json!({ "entity": LEDGER_ENTITY }),
    );
    let assertion_ids: Vec<&str> = ledger["assertions"]
        .as_array()
        .unwrap_or_else(|| panic!("[{label}] no assertions array: {ledger}"))
        .iter()
        .filter_map(|assertion| assertion["assertion_id"].as_str())
        .collect();
    assert_eq!(
        assertion_ids,
        vec![LEDGER_ASSERTION_ID],
        "[{label}] the released memory ledger assertion must read back: {ledger}"
    );
}

/// Durable truth after the gateway exited: the identity's one continuity
/// record still names the released session, and its transcript still holds
/// the released turn.
fn assert_released_transcript_survived(state: &Path) {
    let conn =
        rusqlite::Connection::open(state.join("continuity.sqlite3")).expect("open continuity");
    let records: Vec<(String, String)> = conn
        .prepare("SELECT identity, session_id FROM continuity_records ORDER BY 1")
        .expect("prepare records")
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .expect("query records")
        .collect::<Result<_, _>>()
        .expect("collect records");
    assert_eq!(
        records,
        vec![(IDENTITY.to_string(), SESSION_ID.to_string())],
        "one identity, still bound to its released session"
    );
    let transcript: Vec<Vec<u8>> = conn
        .prepare("SELECT message_json FROM continuity_strand_messages WHERE session_id = ?1")
        .expect("prepare transcript")
        .query_map([SESSION_ID], |row| row.get::<_, Vec<u8>>(0))
        .expect("query transcript")
        .collect::<Result<_, _>>()
        .expect("collect transcript");
    assert!(
        transcript
            .iter()
            .any(|row| String::from_utf8_lossy(row).contains(TURN_TOKEN)),
        "the released turn is missing from the session's durable transcript"
    );
}

fn journal(state: &Path) -> Value {
    serde_json::from_slice(
        &std::fs::read(state.join("meta-adoption.json")).expect("read the adoption journal"),
    )
    .expect("decode the adoption journal")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The acceptance flow: dry run, apply, then resume on 0.9 with the released
/// definition byte for byte, and again with the ignored `tools.mob_tasks`
/// key removed.
#[test]
fn released_0_8_26_store_adopts_and_resumes_with_its_definition() {
    let temp = tempfile::tempdir().expect("tempdir");
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home).expect("home");
    let state = stage_store(temp.path());
    let released = released_manifest();

    assert!(
        MetaStorageLayout::with_injected_roots(state.clone(), None)
            .continuity_db()
            .is_err(),
        "the unadopted released store must refuse to open"
    );

    // 1. Dry run: every step listed, the manifest rewrite among them, and
    // only the given state directory in scope.
    let dry = adopt(&state, &home, false);
    assert_eq!(dry["errors"], json!([]), "{dry:#}");
    assert_eq!(dry["changed_state"], false, "{dry:#}");
    assert_eq!(dry["complete"], false, "{dry:#}");
    assert_manifest_action(&dry, &state, "planned");
    for item in [
        "database_file",
        "ledger_domain",
        "metadata_table",
        "realm_id",
    ] {
        assert!(
            !actions_of(&dry, item).is_empty(),
            "{item} missing: {dry:#}"
        );
    }
    assert!(
        actions_of(&dry, "gateway_home").is_empty(),
        "a state-directory adoption must not plan the user's gateway home: {dry:#}"
    );
    assert_eq!(
        manifest_text(&state),
        released,
        "the dry run changed the manifest"
    );

    // 2. Apply: complete, journaled, and the manifest differs in the creator
    // field's name only.
    let applied = adopt(&state, &home, true);
    assert_eq!(applied["errors"], json!([]), "{applied:#}");
    assert_eq!(applied["complete"], true, "{applied:#}");
    assert_manifest_action(&applied, &state, "applied");
    assert_eq!(manifest_text(&state), adopted_manifest());
    let journal = journal(&state);
    assert_eq!(journal["status"], "complete", "{journal:#}");
    // Every step finished, recorded only under step names every 0.9 release
    // reads (the manifest rename rides the `ledger` step), so a binary
    // rollback to 0.9.0 can still read this journal.
    assert_eq!(
        journal["completed"],
        json!([
            "database_files",
            "memory_ledger_file",
            "ledger",
            "steward_schedule",
            "gateway_home",
            "draft_store"
        ]),
        "{journal:#}"
    );
    assert_eq!(journal["realm_id"], "mobkit", "{journal:#}");

    // Idempotent: a rerun finds nothing left to adopt.
    let again = adopt(&state, &home, false);
    assert_eq!(again["actions"], json!([]), "{again:#}");
    assert_eq!(again["complete"], true, "{again:#}");

    // 3a. The released definition, byte for byte: the exact failing path
    // (persistent mob storage, non-empty event log, authoritative launch).
    let mut gateway = boot(
        "0.8.26/same-definition",
        &state,
        &home,
        MOB_CONFIG,
        Launch::Authoritative,
    );
    assert_released_state_resumed(&mut gateway);
    gateway.shutdown_and_reap();
    assert_released_transcript_survived(&state);

    // 3b. The same definition without the key 0.9 ignores.
    let mut gateway = boot(
        "0.8.26/ignored-key-removed",
        &state,
        &home,
        &mob_config_without_ignored_key(),
        Launch::Authoritative,
    );
    assert_released_state_resumed(&mut gateway);
    gateway.shutdown_and_reap();
    assert_released_transcript_survived(&state);
}

/// The released store adopted the way an adoption without the manifest step
/// (Meta 0.9.0) leaves it: journal complete, manifest still
/// predecessor-spelled.
fn stage_store_adopted_without_the_manifest_step(temp: &Path, home: &Path) -> PathBuf {
    let state = stage_store(temp);
    let applied = adopt(&state, home, true);
    assert_eq!(applied["complete"], true, "{applied:#}");
    std::fs::write(state.join(MANIFEST_FILE), released_manifest()).expect("restore manifest");
    state
}

/// A store an earlier 0.9 adoption completed without the manifest step opens
/// as it lies, on the exact path 0.9.0 refused (-32019 malformed: persistent
/// mob storage, non-empty event log, authoritative launch).
#[test]
fn released_0_8_26_store_adopted_before_the_manifest_step_resumes() {
    let temp = tempfile::tempdir().expect("tempdir");
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home).expect("home");
    let state = stage_store_adopted_without_the_manifest_step(temp.path(), &home);

    let mut gateway = boot(
        "0.8.26/predecessor-manifest",
        &state,
        &home,
        MOB_CONFIG,
        Launch::Authoritative,
    );
    assert_released_state_resumed(&mut gateway);
    gateway.shutdown_and_reap();
    assert_eq!(
        manifest_text(&state),
        released_manifest(),
        "resuming never rewrites the manifest; adoption and repair do"
    );
}

/// A candidate launch over the same store with the unchanged definition
/// boots: the 0.8.9-0.8.28 released-representation allowance reads the
/// predecessor manifest's creator version, so the stored definition's
/// synthesized profile fields (provider, resume_overrides) are not a
/// candidate divergence.
#[test]
fn released_0_8_26_store_resumes_as_a_candidate_with_its_definition() {
    let temp = tempfile::tempdir().expect("tempdir");
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home).expect("home");
    let state = stage_store_adopted_without_the_manifest_step(temp.path(), &home);

    let mut gateway = boot(
        "0.8.26/candidate",
        &state,
        &home,
        MOB_CONFIG,
        Launch::Candidate,
    );
    assert_released_state_resumed(&mut gateway);
    gateway.shutdown_and_reap();
    assert_released_transcript_survived(&state);
}

/// `meta_repair --composition-manifests` detects a predecessor manifest,
/// repairs it to exactly the adopted bytes, and finds nothing left to do.
#[test]
fn meta_repair_detects_and_repairs_a_predecessor_composition_manifest() {
    let temp = tempfile::tempdir().expect("tempdir");
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home).expect("home");
    let state = stage_store(temp.path());
    let state_arg = state.to_str().expect("utf-8 state path");
    let manifest = state.join(MANIFEST_FILE).display().to_string();
    let repair = |args: &[&str]| {
        let run = run_binary(env!("CARGO_BIN_EXE_meta_repair"), args, &home);
        assert_eq!(
            run.code, 0,
            "meta_repair {args:?} failed\nstdout: {}\nstderr: {}",
            run.stdout, run.stderr
        );
        run.json()
    };

    let detected = repair(&["--composition-manifests", state_arg]);
    assert_eq!(
        detected,
        json!({
            "mode": "composition_manifests",
            "applied": false,
            "manifests": [{ "path": manifest, "outcome": "predecessor" }],
            "refused": 0,
            "failed": 0
        })
    );
    assert_eq!(
        manifest_text(&state),
        released_manifest(),
        "detection is read-only"
    );

    let repaired = repair(&["--composition-manifests", state_arg, "--apply"]);
    assert_eq!(
        repaired,
        json!({
            "mode": "composition_manifests",
            "applied": true,
            "manifests": [{ "path": manifest, "outcome": "rewritten" }],
            "refused": 0,
            "failed": 0
        })
    );
    assert_eq!(manifest_text(&state), adopted_manifest());

    let current = repair(&["--composition-manifests", state_arg, "--apply"]);
    assert_eq!(
        current["manifests"],
        json!([{ "path": manifest, "outcome": "current" }])
    );
    assert_eq!(manifest_text(&state), adopted_manifest());
}
