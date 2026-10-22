//! A graceful shutdown with `shutdown_in_flight_drain_ms` lets an in-flight
//! turn finish.
//!
//! The shipped `rpc_gateway --persistent` binary runs over a fresh state
//! directory with one roster identity and a loopback chat-completions model
//! that holds its answer until the test releases it. The identity's tracked
//! turn reaches the model and is held there. The gateway is then asked to
//! stop, by the SDK shutdown handshake or by SIGTERM, and the model is
//! released only once the drain has stopped the mob and waits for its runs.
//! The turn completes, its waiter gets the result before the gateway exits,
//! and the handshake's answer reports the drain. Without a window the init
//! advertises the plain horizon and the answer carries no drain report.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::uninlined_format_args
)]

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

#[path = "support/gateway_shutdown.rs"]
mod gateway_shutdown;

const MOB_ID: &str = "in-flight-drain";
const MODEL: &str = "in-flight-drain-model";
const IDENTITY: &str = "agent:drained";
const TURN_TEXT: &str = "IN-FLIGHT finish the report";
const WINDOW_MS: u64 = 60_000;

/// The stderr line the drain logs once the mob is stopped with its runs left
/// running, right before it waits for them.
const DRAIN_WAITING: &str = "in-flight drain: the mob is stopped";
/// The stderr line the gateway logs with the drain's report.
const DRAIN_FINISHED: &str = "in-flight drain finished";

const INIT_TIMEOUT: Duration = Duration::from_mins(3);
const RPC_TIMEOUT: Duration = Duration::from_mins(1);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_mins(3);
const EXIT_TIMEOUT: Duration = Duration::from_mins(1);

fn mob_config() -> String {
    format!(
        r#"
[mob]
id = "{MOB_ID}"

[profiles.specialist]
model = "{MODEL}"
provider = "self_hosted"
external_addressable = true
runtime_mode = "turn_driven"

[profiles.specialist.tools]
comms = true
"#
    )
}

fn realm_binding(realm: &str) -> String {
    format!(
        r#"
[realm."{realm}"]
default_binding = "loop"

[realm."{realm}".backend.loop]
provider = "self_hosted"
backend_kind = "self_hosted"

[realm."{realm}".auth.loop]
provider = "self_hosted"
auth_method = "none"
source = {{ kind = "platform_default" }}

[realm."{realm}".binding.loop]
backend_profile = "loop"
auth_profile = "loop"
default_model = "{MODEL}"
provider_default = true
"#
    )
}

fn host_config_text(base_url: &str) -> String {
    let mut config = format!(
        r#"
[self_hosted]
default_model = "{MODEL}"

[self_hosted.servers.loop]
base_url = "{base_url}"

[self_hosted.models."{MODEL}"]
server = "loop"
remote_model = "{MODEL}"
display_name = "In-flight drain test model"
family = "{MODEL}"
"#
    );
    config.push_str(&realm_binding("global"));
    config.push_str(&realm_binding(&format!("mob.{MOB_ID}")));
    config
}

fn roster_spec() -> Value {
    json!({
        "identity": IDENTITY,
        "profile": "specialist",
        "addressability": "addressable",
        "display_name": null,
        "labels": {},
        "context": null,
        "additional_instructions": []
    })
}

// ---------------------------------------------------------------------------
// Loopback model that answers only once released
// ---------------------------------------------------------------------------

struct Model {
    base_url: String,
    requests: Arc<Mutex<usize>>,
    release: tokio::sync::watch::Sender<bool>,
}

#[derive(Clone)]
struct ModelState {
    requests: Arc<Mutex<usize>>,
    released: tokio::sync::watch::Receiver<bool>,
}

async fn chat_completions(
    axum::extract::State(state): axum::extract::State<ModelState>,
    axum::Json(_request): axum::Json<Value>,
) -> impl axum::response::IntoResponse {
    *state.requests.lock().unwrap() += 1;
    let mut released = state.released.clone();
    let _ = released.wait_for(|released| *released).await;
    let payload = "data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\n\
                   data: {\"choices\":[{\"finish_reason\":\"stop\"}],\
                   \"usage\":{\"prompt_tokens\":10,\"completion_tokens\":1}}\n\n\
                   data: [DONE]\n\n";
    ([("content-type", "text/event-stream")], payload)
}

async fn models() -> impl axum::response::IntoResponse {
    axum::Json(json!({ "data": [] }))
}

impl Model {
    fn start() -> Self {
        let requests = Arc::new(Mutex::new(0));
        let (release, released) = tokio::sync::watch::channel(false);
        let state = ModelState {
            requests: Arc::clone(&requests),
            released,
        };
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("model runtime");
            runtime.block_on(async move {
                let app = axum::Router::new()
                    .route(
                        "/v1/chat/completions",
                        axum::routing::post(chat_completions),
                    )
                    .route("/chat/completions", axum::routing::post(chat_completions))
                    .route("/v1/models", axum::routing::get(models))
                    .route("/models", axum::routing::get(models))
                    .with_state(state);
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind loopback model");
                tx.send(listener.local_addr().expect("model address"))
                    .expect("report model address");
                axum::serve(listener, app)
                    .await
                    .expect("serve loopback model");
            });
        });
        let address = rx
            .recv_timeout(Duration::from_secs(30))
            .expect("loopback model address");
        Self {
            base_url: format!("http://{address}"),
            requests,
            release,
        }
    }

    /// Wait until the model holds one request: the turn is in flight.
    fn wait_for_request(&self) {
        let start = Instant::now();
        while *self.requests.lock().unwrap() == 0 {
            assert!(
                start.elapsed() < RPC_TIMEOUT,
                "the turn never reached the model"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn release(&self) {
        self.release.send(true).expect("release the model");
    }
}

// ---------------------------------------------------------------------------
// Child gateway process
// ---------------------------------------------------------------------------

struct Gateway {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
    stderr: Arc<Mutex<Vec<String>>>,
    init: Value,
}

impl Gateway {
    fn boot(state: &Path, host_config: &Path, window_ms: Option<u64>) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_rpc_gateway"));
        command
            .arg("--persistent")
            .env_remove("RUST_LOG")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for name in [
            "RKAT_ANTHROPIC_API_KEY",
            "ANTHROPIC_API_KEY",
            "RKAT_OPENAI_API_KEY",
            "OPENAI_API_KEY",
            "RKAT_GEMINI_API_KEY",
            "GEMINI_API_KEY",
            "RKAT_SELF_HOSTED_API_KEY",
        ] {
            command.env_remove(name);
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
        let sink = Arc::clone(&stderr);
        thread::spawn(move || {
            for line in BufReader::new(child_stderr).lines() {
                let Ok(line) = line else { break };
                let Ok(mut sink) = sink.lock() else { break };
                if sink.len() < 20_000 {
                    sink.push(line);
                }
            }
        });
        let mut gateway = Self {
            child,
            stdin: Some(stdin),
            lines: rx,
            stderr,
            init: Value::Null,
        };
        let mut params = json!({
            "persistent_state": state,
            "mob_config": mob_config(),
            "has_roster_provider": true,
            "runtime_options": {
                "meerkat_config_path": host_config,
                "identity_bootstrap_mode": { "mode": "eager_materialize" }
            }
        });
        if let Some(window_ms) = window_ms {
            params["shutdown_in_flight_drain_ms"] = json!(window_ms);
        }
        gateway.send_request("init", "meta/init", params);
        let response = gateway.response("init", INIT_TIMEOUT);
        assert!(
            response.get("error").is_none(),
            "init failed: {response}\n{}",
            gateway.stderr_tail()
        );
        gateway.init = response["result"].clone();
        gateway
    }

    fn send_request(&mut self, id: &str, method: &str, params: Value) {
        let stdin = self.stdin.as_mut().expect("gateway stdin remains open");
        let request = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        writeln!(stdin, "{request}").expect("write request");
        stdin.flush().expect("flush request");
    }

    fn stderr_tail(&self) -> String {
        let Ok(sink) = self.stderr.lock() else {
            return "[stderr unavailable]".to_string();
        };
        let start = sink.len().saturating_sub(80);
        format!("--- gateway stderr ---\n{}", sink[start..].join("\n"))
    }

    /// The next response to any of `ids`, answering host callbacks on the
    /// way.
    fn next_response(&mut self, ids: &[&str], deadline: Duration) -> (String, Value) {
        let start = Instant::now();
        loop {
            let remaining = deadline.checked_sub(start.elapsed()).unwrap_or_else(|| {
                panic!(
                    "no response to {ids:?} within {deadline:?}\n{}",
                    self.stderr_tail()
                )
            });
            let line = self.lines.recv_timeout(remaining).unwrap_or_else(|_| {
                panic!(
                    "no response to {ids:?} within {deadline:?}\n{}",
                    self.stderr_tail()
                )
            });
            let Ok(message) = serde_json::from_str::<Value>(line.trim()) else {
                continue;
            };
            if message.get("method").is_none()
                && let Some(id) = message.get("id").and_then(Value::as_str)
                && ids.contains(&id)
            {
                return (id.to_string(), message);
            }
            self.answer_callback(&message);
        }
    }

    fn response(&mut self, id: &str, deadline: Duration) -> Value {
        self.next_response(&[id], deadline).1
    }

    fn answer_callback(&mut self, message: &Value) {
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            return;
        };
        let Some(id) = message.get("id").cloned() else {
            return;
        };
        match method {
            "callback/roster_provider/roster" => {
                let stdin = self.stdin.as_mut().expect("gateway stdin remains open");
                let answer = json!({ "jsonrpc": "2.0", "id": id, "result": [roster_spec()] });
                writeln!(stdin, "{answer}").expect("write callback answer");
                stdin.flush().expect("flush callback answer");
            }
            other if other.starts_with("callback/") => {
                panic!("unexpected host callback {other}: this harness provides only a roster")
            }
            _ => {}
        }
    }

    /// Send the identity's tracked turn and return its ticket.
    fn send_turn(&mut self) -> String {
        self.send_request(
            "send",
            "meta/send",
            json!({ "identity": IDENTITY, "content": TURN_TEXT, "track_turn": true }),
        );
        let sent = self.response("send", RPC_TIMEOUT);
        assert!(sent.get("error").is_none(), "meta/send failed: {sent}");
        sent["result"]["turn"]["ticket"]
            .as_str()
            .unwrap_or_else(|| panic!("untracked turn: {sent}"))
            .to_string()
    }

    fn wait_for_turn(&mut self, ticket: &str) {
        self.send_request(
            "wait",
            "meta/wait_for_turn",
            json!({ "identity": IDENTITY, "ticket": ticket, "timeout_ms": 120_000 }),
        );
    }

    fn wait_for_stderr(&self, needle: &str, deadline: Duration) -> String {
        let start = Instant::now();
        loop {
            if let Some(line) = self
                .stderr
                .lock()
                .unwrap()
                .iter()
                .find(|line| line.contains(needle))
            {
                return line.clone();
            }
            assert!(
                start.elapsed() < deadline,
                "no stderr line with {needle:?} within {deadline:?}\n{}",
                self.stderr_tail()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[cfg(unix)]
    fn signal(&self, name: &str) {
        let status = Command::new("kill")
            .args([format!("-{name}"), self.child.id().to_string()])
            .status()
            .expect("run kill");
        assert!(status.success(), "kill -{name} failed: {status}");
    }

    fn wait_for_exit(&mut self, deadline: Duration) -> ExitStatus {
        let start = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("poll gateway exit") {
                return status;
            }
            assert!(
                start.elapsed() < deadline,
                "gateway did not exit within {deadline:?}\n{}",
                self.stderr_tail()
            );
            while let Ok(line) = self.lines.try_recv() {
                if let Ok(message) = serde_json::from_str::<Value>(line.trim()) {
                    self.answer_callback(&message);
                }
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

struct Scratch {
    _dir: tempfile::TempDir,
    state: std::path::PathBuf,
    host_config: std::path::PathBuf,
}

fn scratch(model: &Model) -> Scratch {
    let dir = tempfile::tempdir().expect("scratch dir");
    let state = dir.path().join("state");
    let host_config = dir.path().join("config.toml");
    std::fs::write(&host_config, host_config_text(&model.base_url)).expect("write host config");
    Scratch {
        _dir: dir,
        state,
        host_config,
    }
}

fn assert_turn_completed(wait: &Value, gateway: &Gateway) {
    assert_eq!(
        wait["result"]["state"],
        json!("completed"),
        "the drained turn completes: {wait}\n{}",
        gateway.stderr_tail()
    );
}

/// The handshake with a window: the stop waits for the held turn, which
/// completes once released. Its waiter is answered before the shutdown, and
/// the shutdown's answer reports the member's run ended, completed.
#[test]
fn a_handshake_shutdown_lets_an_in_flight_turn_complete() {
    let model = Model::start();
    let scratch = scratch(&model);
    let mut gateway = Gateway::boot(&scratch.state, &scratch.host_config, Some(WINDOW_MS));
    assert_eq!(
        gateway.init["stdio_shutdown_horizon_ms"],
        json!(432_000 + 10_000 + WINDOW_MS),
        "the horizon counts the drain phase: {}",
        gateway.init
    );

    let ticket = gateway.send_turn();
    model.wait_for_request();
    gateway.wait_for_turn(&ticket);
    gateway.send_request("shutdown", "meta/shutdown", json!({}));
    gateway.wait_for_stderr(DRAIN_WAITING, SHUTDOWN_TIMEOUT);
    model.release();

    let (first, wait) = gateway.next_response(&["wait", "shutdown"], SHUTDOWN_TIMEOUT);
    assert_eq!(first, "wait", "the waiter is answered first: {wait}");
    assert_turn_completed(&wait, &gateway);
    let shutdown = gateway.response("shutdown", SHUTDOWN_TIMEOUT);
    gateway_shutdown::assert_persistent_shutdown_completed(
        &shutdown,
        &format!("shutdown\n{}", gateway.stderr_tail()),
    );
    let drain = &shutdown["result"]["in_flight_drain"];
    assert_eq!(drain["outcome"], json!("drained"), "{shutdown}");
    assert_eq!(
        drain["window_ms"].as_u64().map(|ms| ms <= WINDOW_MS),
        Some(true)
    );
    assert!(
        drain["waited_ms"].as_u64().expect("waited_ms") < WINDOW_MS,
        "the drain ends with the turn, not at its bound: {drain}"
    );
    let ended = drain["ended"].as_array().expect("ended");
    assert_eq!(ended.len(), 1, "{drain}");
    assert_eq!(ended[0]["terminal"], json!("completed"), "{drain}");
    assert_eq!(drain["still_running_at_bound"], json!([]), "{drain}");

    drop(gateway.stdin.take());
    let status = gateway.wait_for_exit(EXIT_TIMEOUT);
    assert!(
        status.success(),
        "exit status {status}\n{}",
        gateway.stderr_tail()
    );
}

/// SIGTERM with a window drains the same way. Its turn completes and its
/// waiter is answered before the gateway exits, and the drain's report is
/// logged.
#[cfg(unix)]
#[test]
fn a_sigterm_shutdown_lets_an_in_flight_turn_complete() {
    let model = Model::start();
    let scratch = scratch(&model);
    let mut gateway = Gateway::boot(&scratch.state, &scratch.host_config, Some(WINDOW_MS));

    let ticket = gateway.send_turn();
    model.wait_for_request();
    gateway.wait_for_turn(&ticket);
    gateway.signal("TERM");
    gateway.wait_for_stderr(DRAIN_WAITING, SHUTDOWN_TIMEOUT);
    model.release();

    let wait = gateway.response("wait", SHUTDOWN_TIMEOUT);
    assert_turn_completed(&wait, &gateway);
    let status = gateway.wait_for_exit(SHUTDOWN_TIMEOUT);
    assert!(
        status.success(),
        "exit status {status}\n{}",
        gateway.stderr_tail()
    );
    let finished = gateway.wait_for_stderr(DRAIN_FINISHED, EXIT_TIMEOUT);
    assert!(
        finished.contains("\"outcome\":\"drained\"") && finished.contains("\"completed\""),
        "the drain's report: {finished}"
    );
}

/// Without a window the init advertises the plain horizon, and the shutdown
/// answer carries no drain report.
#[test]
fn without_a_window_the_shutdown_is_unchanged() {
    let model = Model::start();
    let scratch = scratch(&model);
    let mut gateway = Gateway::boot(&scratch.state, &scratch.host_config, None);
    assert_eq!(
        gateway.init["stdio_shutdown_horizon_ms"],
        json!(432_000),
        "{}",
        gateway.init
    );
    gateway.send_request("shutdown", "meta/shutdown", json!({}));
    let shutdown = gateway.response("shutdown", SHUTDOWN_TIMEOUT);
    gateway_shutdown::assert_persistent_shutdown_completed(
        &shutdown,
        &format!("shutdown\n{}", gateway.stderr_tail()),
    );
    assert!(
        shutdown["result"].get("in_flight_drain").is_none(),
        "{shutdown}"
    );
    drop(gateway.stdin.take());
    let status = gateway.wait_for_exit(EXIT_TIMEOUT);
    assert!(
        status.success(),
        "exit status {status}\n{}",
        gateway.stderr_tail()
    );
    assert!(
        !gateway
            .stderr
            .lock()
            .unwrap()
            .iter()
            .any(|line| line.contains("in-flight drain")),
        "no drain phase ran\n{}",
        gateway.stderr_tail()
    );
}
