//! M5 wire acceptance for the storage durability vocabulary on the real
//! `rpc_gateway` binary: the `runtime_options.runtime_store` declaration,
//! the `event_log.storage = "null"` form, the per-slot storage census on
//! `meta/status`, and the typed `STORAGE_RESOLUTION_CODE` (-32014) init
//! refusals the SDKs reify as `StorageResolutionError`.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    clippy::uninlined_format_args
)]

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};

use serde_json::{Value, json};
use tempfile::TempDir;

const MOB_CONFIG: &str = r#"
[mob]
id = "storage-wire-test"

[profiles.default]
model = "gpt-5.5"
external_addressable = true
"#;

struct GatewayProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
}

impl GatewayProcess {
    fn start() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_rpc_gateway"))
            .arg("--persistent")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn rpc_gateway");
        let stdin = child.stdin.take().expect("gateway stdin");
        let stdout = BufReader::new(child.stdout.take().expect("gateway stdout"));
        Self {
            child,
            stdin,
            stdout,
        }
    }

    fn rpc(&mut self, request: Value) -> Value {
        writeln!(
            self.stdin,
            "{}",
            serde_json::to_string(&request).expect("request json")
        )
        .expect("write request");
        self.stdin.flush().expect("flush request");

        let mut line = String::new();
        self.stdout.read_line(&mut line).expect("read response");
        assert!(!line.is_empty(), "gateway closed stdout before response");
        serde_json::from_str(line.trim()).expect("response json")
    }

    fn init(&mut self, state_dir: &TempDir, runtime_options: Value) -> Value {
        self.rpc(json!({
            "jsonrpc": "2.0",
            "id": "init",
            "method": "meta/init",
            "params": {
                "persistent_state": state_dir.path(),
                "mob_config": MOB_CONFIG,
                "runtime_options": runtime_options
            }
        }))
    }

    fn status(&mut self) -> Value {
        self.rpc(json!({
            "jsonrpc": "2.0",
            "id": "status",
            "method": "meta/status",
            "params": {}
        }))
    }
}

impl Drop for GatewayProcess {
    fn drop(&mut self) {
        let _ = self.stdin.flush();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn slot<'a>(status: &'a Value, domain: &str) -> &'a Value {
    status["result"]["storage"]["slots"]
        .as_array()
        .expect("storage.slots array")
        .iter()
        .find(|slot| slot["domain"] == json!(domain))
        .unwrap_or_else(|| panic!("no {domain} slot in {status}"))
}

/// Default persistent launch: the runtime slot resolves persistent and the
/// census rides `meta/status` (M4 wire shape the SDK census model parses).
#[test]
fn status_census_reports_persistent_runtime_store_by_default() {
    let state_dir = tempfile::tempdir().expect("state dir");
    let mut gateway = GatewayProcess::start();
    let init = gateway.init(&state_dir, json!({}));
    assert!(init["result"]["contract_version"].is_string(), "{init}");

    let status = gateway.status();
    let runtime_slot = slot(&status, "runtime");
    assert_eq!(runtime_slot["class"], json!("durable"));
    assert_eq!(runtime_slot["resolution"], json!("persistent"));
    assert_eq!(runtime_slot["backend"], json!("SqliteRuntimeStore"));
    assert_eq!(runtime_slot["degraded"], json!(false));
    assert_eq!(
        status["result"]["storage"]["blob_durability"],
        json!("persistent_disk")
    );
}

/// The explicit `runtime_store = {"storage": "memory"}` declaration is
/// accepted on the wire and shows up as a declared-ephemeral slot.
#[test]
fn runtime_store_memory_declaration_is_census_visible() {
    let state_dir = tempfile::tempdir().expect("state dir");
    let mut gateway = GatewayProcess::start();
    let init = gateway.init(
        &state_dir,
        json!({ "runtime_store": { "storage": "memory" } }),
    );
    assert!(init["result"]["contract_version"].is_string(), "{init}");

    let status = gateway.status();
    let runtime_slot = slot(&status, "runtime");
    assert_eq!(runtime_slot["resolution"], json!("declared_ephemeral"));
    assert_eq!(runtime_slot["backend"], json!("InMemoryRuntimeStore"));
}

/// `event_log.storage = "null"` (M4) parses and boots; events are declared
/// dropped, and the census records the declared in-process store.
#[test]
fn event_log_null_declaration_boots_and_is_census_visible() {
    let state_dir = tempfile::tempdir().expect("state dir");
    let mut gateway = GatewayProcess::start();
    let init = gateway.init(&state_dir, json!({ "event_log": { "storage": "null" } }));
    assert!(init["result"]["contract_version"].is_string(), "{init}");

    let status = gateway.status();
    let event_slot = slot(&status, "event_log");
    assert_eq!(event_slot["resolution"], json!("declared_ephemeral"));

    let queried = gateway.rpc(json!({
        "jsonrpc": "2.0",
        "id": "events",
        "method": "meta/query_events",
        "params": {}
    }));
    assert_eq!(queried["result"], json!([]));
}

/// File-name twins refuse at init with the typed storage-resolution code
/// (-32014) and a message that points at the storage doctor — the error the
/// SDKs reify as `StorageResolutionError` instead of a transport failure.
#[test]
fn session_store_twins_refuse_init_with_storage_resolution_code() {
    let state_dir = tempfile::tempdir().expect("state dir");
    std::fs::write(state_dir.path().join("sessions.sqlite3"), b"").expect("canonical twin");
    std::fs::write(state_dir.path().join("sessions.db"), b"").expect("legacy twin");

    let mut gateway = GatewayProcess::start();
    let init = gateway.init(&state_dir, json!({}));
    assert_eq!(
        init["error"]["code"],
        json!(meerkat_meta::STORAGE_RESOLUTION_CODE),
        "{init}"
    );
    let message = init["error"]["message"].as_str().expect("error message");
    assert!(message.contains("meta/storage/doctor"), "{message}");
}

/// A runtime store that cannot open is a typed startup refusal (M4's
/// fail-closed posture), and the message names the explicit ephemeral
/// declaration as the remediation.
#[test]
fn runtime_store_open_failure_refuses_init_with_storage_resolution_code() {
    let state_dir = tempfile::tempdir().expect("state dir");
    // A directory where the runtime database file must live forces the
    // SQLite open to fail without touching permissions (CI-safe).
    std::fs::create_dir(state_dir.path().join("runtime.sqlite")).expect("block runtime db");

    let mut gateway = GatewayProcess::start();
    let init = gateway.init(&state_dir, json!({}));
    assert_eq!(
        init["error"]["code"],
        json!(meerkat_meta::STORAGE_RESOLUTION_CODE),
        "{init}"
    );
    let message = init["error"]["message"].as_str().expect("error message");
    assert!(
        message.contains("runtime_options.runtime_store"),
        "{message}"
    );
}

/// Undeclared event-log storage kinds still reject as invalid params
/// (-32602): a parse error is not a storage-resolution refusal.
#[test]
fn undeclared_event_log_storage_rejects_as_invalid_params() {
    let state_dir = tempfile::tempdir().expect("state dir");
    let mut gateway = GatewayProcess::start();
    let init = gateway.init(&state_dir, json!({ "event_log": { "storage": "sqlite" } }));
    assert_eq!(init["error"]["code"], json!(-32602), "{init}");
    let message = init["error"]["message"].as_str().expect("error message");
    assert!(
        message.contains("unsupported runtime_options.event_log.storage"),
        "{message}"
    );
}

/// A zero flush interval would panic the ingestion task
/// (`tokio::time::interval` requires a non-zero period); the gateway
/// rejects it as invalid params instead of booting a runtime whose event
/// log dies silently.
#[test]
fn zero_event_log_flush_interval_rejects_as_invalid_params() {
    let state_dir = tempfile::tempdir().expect("state dir");
    let mut gateway = GatewayProcess::start();
    let init = gateway.init(
        &state_dir,
        json!({ "event_log": { "storage": "memory", "flush_interval_ms": 0 } }),
    );
    assert_eq!(init["error"]["code"], json!(-32602), "{init}");
    let message = init["error"]["message"].as_str().expect("error message");
    assert!(
        message.contains("runtime_options.event_log.flush_interval_ms"),
        "{message}"
    );
}

/// Non-integer flush intervals are typed invalid params, not a silent
/// fallback to the default interval.
#[test]
fn non_integer_event_log_flush_interval_rejects_as_invalid_params() {
    let state_dir = tempfile::tempdir().expect("state dir");
    let mut gateway = GatewayProcess::start();
    let init = gateway.init(
        &state_dir,
        json!({ "event_log": { "storage": "memory", "flush_interval_ms": "fast" } }),
    );
    assert_eq!(init["error"]["code"], json!(-32602), "{init}");
    let message = init["error"]["message"].as_str().expect("error message");
    assert!(
        message.contains("runtime_options.event_log.flush_interval_ms"),
        "{message}"
    );
}
