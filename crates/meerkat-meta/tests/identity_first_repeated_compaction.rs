#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::uninlined_format_args
)]
//! Repeated auto-compaction of identity-first members through the shipped
//! `rpc_gateway --persistent` composition (#488).
//!
//! The composition under test is the one production runs: identity-first
//! members over a WholeBlob `runtime.sqlite`, Meta's projecting runtime
//! store, and the head-canonical continuity store. Every committed turn
//! boundary replays its rewrite commits into the continuity row.
//!
//! The defect: record-only rewrites named the adopted strand by the commit's
//! revision digest alone. The demo LLM summarises every compaction to the
//! same text, so the second compaction of a member rewrites to the same
//! transcript revision as the first. The replay then re-targeted the strand
//! it was rewriting, whose immutable post-head rows refused the new base
//! ("rewrite replay save at generation 2 ... not a continuation of persisted
//! revision"), the member went repair-blocked, and every later send was
//! refused.
//!
//! Every wait here is typed: a tracked delivery is awaited through
//! `meta/wait_for_turn` on its own ticket, and the durable facts are read
//! only after a graceful shutdown has quiesced the gateway. The only clocks
//! are failure deadlines.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use meerkat_core::session_store::SessionHead;
use meerkat_meta::storage_layout::MetaStorageLayout;
use serde_json::{Value, json};

/// A low threshold so the synthesized usage of each 8 KiB prompt pushes the
/// member over it every turn or two.
const MOB_CONFIG: &str = r#"
[mob]
id = "repeated-compaction"

[profiles.default]
model = "gpt-5.5"
external_addressable = true
runtime_mode = "turn_driven"
auto_compact_threshold = 6000

[profiles.default.tools]
comms = true
"#;

const IDENTITIES: [&str; 2] = ["agent:alice", "agent:bob"];
/// Adopted rewrites every member must reach before the reboot.
const MIN_COMPACTIONS: u64 = 5;
const TURNS: usize = 14;
const PROMPT_BYTES: usize = 8 * 1024;

fn roster() -> Value {
    Value::Array(
        IDENTITIES
            .iter()
            .map(|identity| {
                json!({
                    "identity": identity,
                    "profile": "default",
                    "addressability": "addressable",
                    "display_name": null,
                    "labels": {},
                    "context": null,
                    "additional_instructions": []
                })
            })
            .collect(),
    )
}

struct Gateway {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
    stderr: Arc<Mutex<Vec<String>>>,
    next_id: u64,
}

impl Gateway {
    fn boot(state: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_rpc_gateway"))
            .arg("--persistent")
            .env_remove("RUST_LOG")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn rpc_gateway --persistent");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("stdout");
        let child_stderr = child.stderr.take().expect("stderr");
        let (tx, lines) = mpsc::channel();
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
                sink.lock().unwrap().push(line);
            }
        });
        let mut gateway = Self {
            child,
            stdin,
            lines,
            stderr,
            next_id: 0,
        };
        gateway.call(
            "meta/init",
            json!({
                "persistent_state": state,
                "mob_config": MOB_CONFIG,
                "has_roster_provider": true,
                "runtime_options": { "demo_llm": true }
            }),
            Duration::from_mins(10),
        );
        gateway
    }

    fn write(&mut self, value: &Value) {
        let stdin = self.stdin.as_mut().expect("gateway stdin");
        writeln!(stdin, "{}", serde_json::to_string(value).unwrap()).unwrap();
        stdin.flush().unwrap();
    }

    /// One JSON-RPC call; answers roster callbacks while it waits.
    fn call(&mut self, method: &str, params: Value, deadline: Duration) -> Value {
        self.next_id += 1;
        let id = format!("req-{}", self.next_id);
        self.write(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        let start = Instant::now();
        loop {
            let remaining = deadline
                .checked_sub(start.elapsed())
                .unwrap_or_else(|| panic!("no response to {method}\n{}", self.stderr_tail()));
            let line = self
                .lines
                .recv_timeout(remaining)
                .unwrap_or_else(|_| panic!("no response to {method}\n{}", self.stderr_tail()));
            let Ok(message) = serde_json::from_str::<Value>(line.trim()) else {
                continue;
            };
            if message.get("method").is_none()
                && message.get("id").and_then(Value::as_str) == Some(id.as_str())
            {
                assert!(
                    message.get("error").is_none(),
                    "{method} failed: {message}\n{}",
                    self.stderr_tail()
                );
                return message["result"].clone();
            }
            if message.get("method").and_then(Value::as_str)
                == Some("callback/roster_provider/roster")
                && let Some(callback_id) = message.get("id").cloned()
            {
                self.write(&json!({"jsonrpc": "2.0", "id": callback_id, "result": roster()}));
            }
        }
    }

    /// Deliver one tracked prompt and await THAT turn's typed outcome.
    fn send_and_await(&mut self, identity: &str, content: &str) {
        let admitted = self.call(
            "meta/send",
            json!({"identity": identity, "content": content, "track_turn": true}),
            Duration::from_mins(2),
        );
        let ticket = admitted["turn"]["ticket"]
            .as_str()
            .unwrap_or_else(|| panic!("send to {identity} was not tracked: {admitted}"))
            .to_string();
        let outcome = self.call(
            "meta/wait_for_turn",
            json!({"identity": identity, "ticket": ticket, "timeout_ms": 120_000}),
            Duration::from_mins(3),
        );
        assert_eq!(
            outcome["state"],
            "completed",
            "turn for {identity} did not complete: {outcome}\n{}",
            self.stderr_tail()
        );
    }

    fn stderr_tail(&self) -> String {
        let sink = self.stderr.lock().unwrap();
        let start = sink.len().saturating_sub(40);
        sink[start..].join("\n")
    }

    fn shutdown(mut self) {
        self.call("meta/shutdown", json!({}), Duration::from_mins(3));
        drop(self.stdin.take());
        self.child.wait().expect("gateway exit");
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn prompt(identity: &str, turn: usize) -> String {
    format!("{identity} turn {turn} ")
        .repeat(PROMPT_BYTES / 16)
        .chars()
        .take(PROMPT_BYTES)
        .collect()
}

fn continuity_db(state: &Path) -> PathBuf {
    MetaStorageLayout::with_injected_roots(state.to_path_buf(), None)
        .continuity_db()
        .expect("continuity db path")
        .path
}

/// One durable continuity head (the typed `SessionHead` the store persists)
/// and its rewrite rows `(rewrite_idx, parent_strand, strand)` in order.
type DurableChain = (SessionHead, Vec<(i64, String, String)>);

/// Durable heads and rewrite rows keyed by identity. Read only while no
/// gateway holds the store.
fn durable_heads(state: &Path) -> BTreeMap<String, DurableChain> {
    let conn = rusqlite::Connection::open(continuity_db(state)).expect("open continuity db");
    let mut heads = conn
        .prepare("SELECT identity, session_id, head_json FROM continuity_session_heads")
        .unwrap();
    let heads = heads
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut rewrites = conn
        .prepare(
            "SELECT rewrite_idx, parent_strand, strand FROM continuity_session_rewrites
             WHERE session_id = ?1 ORDER BY rewrite_idx",
        )
        .unwrap();
    heads
        .into_iter()
        .map(|(identity, session_id, head_json)| {
            let head: SessionHead =
                serde_json::from_slice(&head_json).expect("typed durable session head");
            let chain = rewrites
                .query_map([&session_id], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            (identity, (head, chain))
        })
        .collect()
}

#[test]
fn identity_first_members_survive_repeated_auto_compaction() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    std::fs::create_dir_all(&state).unwrap();

    let mut gateway = Gateway::boot(&state);
    for turn in 0..TURNS {
        for identity in IDENTITIES {
            gateway.send_and_await(identity, &prompt(identity, turn));
        }
    }
    gateway.shutdown();

    let heads = durable_heads(&state);
    for identity in IDENTITIES {
        let (head, chain) = heads
            .get(identity)
            .unwrap_or_else(|| panic!("no durable head for {identity}: {heads:#?}"));
        assert!(
            head.rewrite_count >= MIN_COMPACTIONS,
            "{identity} must reach {MIN_COMPACTIONS} adopted compactions: {head:#?} {chain:#?}"
        );
        // The persisted revision chain continues: one contiguous rewrite row
        // per adopted generation, the latest on the head's own strand.
        let adopted = usize::try_from(head.rewrite_count).unwrap();
        assert!(chain.len() >= adopted, "{identity}: {chain:#?}");
        for (index, row) in chain[..adopted].iter().enumerate() {
            assert_eq!(
                row.0,
                i64::try_from(index).unwrap(),
                "{identity}: {chain:#?}"
            );
        }
        assert_eq!(
            chain[adopted - 1].2,
            head.strand.as_str(),
            "{identity}: head is not on its latest rewrite strand: {chain:#?}"
        );
        // A recurring revision still gets its own strand.
        let strands: std::collections::BTreeSet<_> =
            chain[..adopted].iter().map(|row| row.2.as_str()).collect();
        assert_eq!(
            strands.len(),
            adopted,
            "{identity}: strand reused: {chain:#?}"
        );
    }

    // The chain restores and keeps accepting work after a cold reboot.
    let mut rebooted = Gateway::boot(&state);
    for identity in IDENTITIES {
        rebooted.send_and_await(identity, &prompt(identity, TURNS));
    }
    rebooted.shutdown();
    let after = durable_heads(&state);
    for identity in IDENTITIES {
        let (head, _) = &heads[identity];
        let (rebooted_head, _) = after
            .get(identity)
            .unwrap_or_else(|| panic!("{identity} lost across reboot: {after:#?}"));
        assert_eq!(rebooted_head.id, head.id, "{identity} changed session");
        assert!(
            rebooted_head.rewrite_count >= head.rewrite_count,
            "{identity}: rewrite generation regressed across reboot: {} -> {}",
            head.rewrite_count,
            rebooted_head.rewrite_count
        );
    }
}
