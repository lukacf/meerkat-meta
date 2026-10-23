//! Mob announcements across a real process crash: a shared specialist
//! receives a burst while its subscription is paused, catches up with one
//! batch, loses the acknowledgment of that batch to a crash, and continues
//! its original work after the restart.
//!
//! Each boot is the shipped `rpc_gateway --persistent` binary over one state
//! directory, built with the `announcement-test-faults` feature so the first
//! boot can abort at an exact point: after Meerkat's continuation owner
//! committed the batch into the specialist's runtime inbox and before the
//! announcement owner recorded the receipt. The model is a loopback
//! chat-completions server run by this test. It holds the specialist's first
//! turn open, so the crash also lands in the middle of that turn, and it
//! records every request, so model calls are counted exactly instead of
//! inferred.
//!
//! A second test boots the same gateway over its own state with a member
//! placed on a host in the roster, and shows that a subscription for that
//! member is refused typed.
//!
//! A third has a member whose profile opts into the member tools announce
//! from its own model turn: the loopback model calls `mob_announce`, and
//! another member's subscription receives the announcement as that member's.
//!
//! Two more kill a gateway while an announcement source's collector advances
//! it: inside the advance's transaction, which leaves neither its items nor
//! its cursor, and after its commit, which delivers its items exactly once
//! and answers the collector's retry with the original result.

#![cfg(feature = "announcement-test-faults")]
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

const MOB_ID: &str = "mob-announcements-delivery-process";
const MODEL: &str = "announcement-test";
const IDENTITY: &str = "agent:specialist";
/// A member of the same mob whose durable spec places it on a host.
const PLACED_IDENTITY: &str = "agent:placed-specialist";
/// The host the placed member names. No host is bound to this gateway.
const PLACED_HOST: &str = "remote-member-host";
const TOPIC: &str = "payments.ios";
const BURST: usize = 200;
const HOLD_MARKER: &str = "ORIGINAL-WORK";
const BATCH_MARKER: &str = "Mob announcements batch-";
const FINAL_MARKER: &str = "FINAL-CHECK";
/// A member of the same mob whose profile opts into the member tools.
const ANNOUNCER: &str = "agent:announcer";
/// The prompt the loopback model answers with one `mob_announce` call.
const ANNOUNCE_MARKER: &str = "ANNOUNCE-REFUND-DELAYS";
const ANNOUNCED_BODY: &str = "refunds to every card issuer are delayed by one day";
const ANNOUNCED_KEY: &str = "refund-delays";
/// The member tools, as the model is offered them.
const MEMBER_TOOLS: [&str; 2] = ["mob_announce", "mob_subscription"];
/// A member of the gateway's mob whose profile may create mobs.
const LEAD: &str = "agent:lead";
/// The mob the lead creates, and the member it spawns there.
const CHILD_MOB: &str = "announcement-child-mob";
const CHILD_MEMBER: &str = "worker-1";
/// The prompt the loopback model answers by creating the child mob and
/// spawning its member.
const CREATE_MARKER: &str = "CREATE-CHILD-MOB";
/// Like [`CREATE_MARKER`], with a child profile that opts in to the member
/// tools in its `tools.bundles`.
const CREATE_OPTED_MARKER: &str = "CREATE-OPTED-CHILD-MOB";
/// The child member's first prompt: offered the member tools, it subscribes
/// itself to the lead's mob.
const SUBSCRIBE_MARKER: &str = "SUBSCRIBE-TO-THE-ROOT-MOB";
/// The prompt the loopback model answers by destroying the child mob.
const DESTROY_MARKER: &str = "DESTROY-CHILD-MOB";
/// The prompt the loopback model answers by retiring the child member.
const RETIRE_MARKER: &str = "RETIRE-CHILD-MEMBER";
/// The subscription the child member makes for itself.
const CHILD_SUBSCRIPTION: &str = "from-the-root";

const INIT_TIMEOUT: Duration = Duration::from_mins(3);
const RPC_TIMEOUT: Duration = Duration::from_mins(1);
const SETTLE_TIMEOUT: Duration = Duration::from_mins(3);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_mins(3);
const EXIT_TIMEOUT: Duration = Duration::from_mins(1);
/// How long an owed announcement waits to be sealed in the tests that
/// seal a batch only by resuming its subscription.
const HELD_DELAY_MS: u64 = 60_000;

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

[profiles.announcer]
model = "{MODEL}"
provider = "self_hosted"
external_addressable = true
runtime_mode = "turn_driven"

[profiles.announcer.tools]
comms = true
rust_bundles = ["mob_announcements"]

[profiles.lead]
model = "{MODEL}"
provider = "self_hosted"
external_addressable = true
runtime_mode = "turn_driven"

[profiles.lead.tools]
comms = true
mob = true
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

/// The host config naming the loopback server for the specialist's model.
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
display_name = "Announcement test model"
family = "{MODEL}"
"#
    );
    config.push_str(&realm_binding("global"));
    config.push_str(&realm_binding(&format!("mob.{MOB_ID}")));
    config.push_str(&realm_binding(&format!("mob.{CHILD_MOB}")));
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

/// A roster row for a member of the same mob placed on a host: registered
/// with this gateway, but its session is not this gateway's runtime to serve.
fn placed_roster_spec() -> Value {
    json!({
        "identity": PLACED_IDENTITY,
        "profile": "specialist",
        "addressability": "addressable",
        "display_name": null,
        "labels": {},
        "context": null,
        "additional_instructions": [],
        "placement": PLACED_HOST
    })
}

/// A roster row for the member whose profile opts into the member tools.
fn announcer_roster_spec() -> Value {
    json!({
        "identity": ANNOUNCER,
        "profile": "announcer",
        "addressability": "addressable",
        "display_name": null,
        "labels": {},
        "context": null,
        "additional_instructions": []
    })
}

/// A member of this gateway's mob, in the form every method takes.
fn member(identity: &str) -> Value {
    json!({ "mob_id": MOB_ID, "identity": identity })
}

/// A roster row for the member whose profile may create mobs.
fn lead_roster_spec() -> Value {
    json!({
        "identity": LEAD,
        "profile": "lead",
        "addressability": "addressable",
        "display_name": null,
        "labels": {},
        "context": null,
        "additional_instructions": []
    })
}

/// The member of the child mob, by its plain roster id.
fn child_member() -> Value {
    json!({ "mob_id": CHILD_MOB, "identity": CHILD_MEMBER })
}

/// The stderr line a fault point prints right before it aborts the process.
fn fault_line(point: &str) -> String {
    format!("announcement fault point {point}: aborting the process")
}

// ---------------------------------------------------------------------------
// Loopback model
// ---------------------------------------------------------------------------

/// What the loopback model saw: every request body, in arrival order.
#[derive(Default)]
struct ModelLog {
    requests: Vec<Value>,
    held: bool,
}

/// The text of the last message in a chat-completions request: the new
/// input a turn answers.
fn last_message_text(request: &Value) -> String {
    let Some(message) = request["messages"].as_array().and_then(|all| all.last()) else {
        return String::new();
    };
    match &message["content"] {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| part["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn message_texts(request: &Value) -> Vec<(String, String)> {
    request["messages"]
        .as_array()
        .map(|messages| {
            messages
                .iter()
                .map(|message| {
                    let text = match &message["content"] {
                        Value::String(text) => text.clone(),
                        Value::Array(parts) => parts
                            .iter()
                            .filter_map(|part| part["text"].as_str())
                            .collect::<Vec<_>>()
                            .join("\n"),
                        _ => String::new(),
                    };
                    (
                        message["role"].as_str().unwrap_or_default().to_string(),
                        text,
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The role of the last message in a chat-completions request: `tool` when
/// the request carries a tool's answer back to the model.
fn last_message_role(request: &Value) -> String {
    request["messages"]
        .as_array()
        .and_then(|all| all.last())
        .and_then(|message| message["role"].as_str())
        .unwrap_or_default()
        .to_string()
}

/// The names of the tools a chat-completions request offers the model.
fn offered_tools(request: &Value) -> Vec<String> {
    request["tools"]
        .as_array()
        .map(|tools| {
            tools
                .iter()
                .filter_map(|tool| tool["function"]["name"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// The name of the last tool the model called in a chat-completions
/// request's history.
fn last_tool_called(request: &Value) -> Option<String> {
    request["messages"]
        .as_array()?
        .iter()
        .rev()
        .find_map(|message| {
            message["tool_calls"].as_array()?.first()?["function"]["name"]
                .as_str()
                .map(str::to_owned)
        })
}

/// A streamed reply that is one call of `name` with `arguments`.
fn tool_call(id: &str, name: &str, arguments: &Value) -> String {
    let call = json!({
        "choices": [{
            "delta": {
                "tool_calls": [{
                    "index": 0,
                    "id": id,
                    "type": "function",
                    "function": { "name": name, "arguments": arguments.to_string() }
                }]
            }
        }]
    });
    let done = json!({
        "choices": [{ "finish_reason": "tool_calls" }],
        "usage": { "prompt_tokens": 1, "completion_tokens": 1 }
    });
    format!("data: {call}\n\ndata: {done}\n\ndata: [DONE]\n\n")
}

/// The lead's calls: create the child mob, then spawn its member with the
/// prompt that subscribes it. `opt_in` has the child profile name the
/// member tool bundle in its `tools.bundles`.
fn create_child_call(opt_in: bool) -> String {
    let mut tools = json!({ "comms": true });
    if opt_in {
        tools["bundles"] = json!(["mob_announcements"]);
    }
    tool_call(
        "call-create-1",
        "mob_create",
        &json!({
            "definition": {
                "id": CHILD_MOB,
                "profiles": {
                    "worker": {
                        "model": MODEL,
                        "provider": "self_hosted",
                        "runtime_mode": "turn_driven",
                        "tools": tools
                    }
                }
            }
        }),
    )
}

fn spawn_child_call() -> String {
    tool_call(
        "call-spawn-1",
        "mob_spawn_member",
        &json!({
            "mob_id": CHILD_MOB,
            "profile": "worker",
            "member_id": CHILD_MEMBER,
            "runtime_mode": "turn_driven",
            "initial_message": format!("{SUBSCRIBE_MARKER}: follow the root mob")
        }),
    )
}

fn subscribe_call() -> String {
    tool_call(
        "call-subscribe-1",
        "mob_subscription",
        &json!({
            "action": "subscribe",
            "subscription": CHILD_SUBSCRIPTION,
            "topics": [TOPIC],
            "mobs": [MOB_ID]
        }),
    )
}

fn retire_child_member_call() -> String {
    tool_call(
        "call-retire-1",
        "mob_retire_member",
        &json!({ "mob_id": CHILD_MOB, "member_id": CHILD_MEMBER }),
    )
}

fn destroy_child_call() -> String {
    tool_call(
        "call-destroy-1",
        "mob_destroy",
        &json!({ "mob_id": CHILD_MOB }),
    )
}

/// A streamed reply that is one `mob_announce` call and nothing else.
fn announce_call() -> String {
    let arguments = json!({
        "topics": [TOPIC],
        "body": ANNOUNCED_BODY,
        "key": ANNOUNCED_KEY
    })
    .to_string();
    let call = json!({
        "choices": [{
            "delta": {
                "tool_calls": [{
                    "index": 0,
                    "id": "call-announce-1",
                    "type": "function",
                    "function": { "name": "mob_announce", "arguments": arguments }
                }]
            }
        }]
    });
    let done = json!({
        "choices": [{ "finish_reason": "tool_calls" }],
        "usage": { "prompt_tokens": 1, "completion_tokens": 1 }
    });
    format!("data: {call}\n\ndata: {done}\n\ndata: [DONE]\n\n")
}

async fn chat_completions(
    axum::extract::State(log): axum::extract::State<Arc<Mutex<ModelLog>>>,
    axum::Json(request): axum::Json<Value>,
) -> impl axum::response::IntoResponse {
    let last = last_message_text(&request);
    let answers_a_tool = last_message_role(&request) == "tool";
    let request_seen = request.clone();
    let hold = {
        let mut log = log.lock().unwrap();
        log.requests.push(request);
        let hold = last.contains(HOLD_MARKER) && !log.held;
        if hold {
            log.held = true;
        }
        hold
    };
    if hold {
        // The first attempt at the original work never finishes: the
        // gateway is killed while it waits.
        std::future::pending::<()>().await;
    }
    if last.contains(ANNOUNCE_MARKER) && !answers_a_tool {
        return ([("content-type", "text/event-stream")], announce_call());
    }
    if answers_a_tool && last_tool_called(&request_seen) == Some("mob_create".to_string()) {
        return ([("content-type", "text/event-stream")], spawn_child_call());
    }
    if !answers_a_tool {
        if last.contains(CREATE_OPTED_MARKER) {
            return (
                [("content-type", "text/event-stream")],
                create_child_call(true),
            );
        }
        if last.contains(CREATE_MARKER) {
            return (
                [("content-type", "text/event-stream")],
                create_child_call(false),
            );
        }
        if last.contains(DESTROY_MARKER) {
            return (
                [("content-type", "text/event-stream")],
                destroy_child_call(),
            );
        }
        if last.contains(RETIRE_MARKER) {
            return (
                [("content-type", "text/event-stream")],
                retire_child_member_call(),
            );
        }
        // Only a member offered the member tools can subscribe itself.
        if last.contains(SUBSCRIBE_MARKER)
            && offered_tools(&request_seen)
                .iter()
                .any(|tool| tool == "mob_subscription")
        {
            return ([("content-type", "text/event-stream")], subscribe_call());
        }
    }
    let reply = if answers_a_tool {
        "announced the refund delays"
    } else if last.contains(HOLD_MARKER) {
        "original work complete"
    } else if last.contains(BATCH_MARKER) {
        "noted the payment failures"
    } else {
        "ok"
    };
    let payload = format!(
        concat!(
            "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{}\"}}}}]}}\n\n",
            "data: {{\"choices\":[{{\"finish_reason\":\"stop\"}}],",
            "\"usage\":{{\"prompt_tokens\":1,\"completion_tokens\":1}}}}\n\n",
            "data: [DONE]\n\n"
        ),
        reply
    );
    ([("content-type", "text/event-stream")], payload)
}

async fn models() -> impl axum::response::IntoResponse {
    axum::Json(json!({ "data": [] }))
}

/// Start the loopback model on its own runtime; returns its base URL.
fn start_model(log: Arc<Mutex<ModelLog>>) -> String {
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
                .with_state(log);
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
    format!("http://{address}")
}

fn wait_until(what: &str, deadline: Duration, mut condition: impl FnMut() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(start.elapsed() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(50));
    }
}

// ---------------------------------------------------------------------------
// Child gateway process
// ---------------------------------------------------------------------------

struct Gateway {
    label: String,
    child: Child,
    stdin: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
    stderr: Arc<Mutex<Vec<String>>>,
    next_id: u64,
    roster: Vec<Value>,
    /// Every `callback/schedule_fire` the gateway sent, in order.
    fires: Vec<Value>,
    /// The params of every `meta/on_error` notification, in order.
    on_error: Vec<Value>,
}

impl Gateway {
    fn start(label: &str, fault: Option<&str>) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_rpc_gateway"));
        command
            .arg("--persistent")
            .env_remove("RUST_LOG")
            .env_remove("META_ANNOUNCEMENT_FAULT")
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
        if let Some(point) = fault {
            command.env("META_ANNOUNCEMENT_FAULT", point);
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
                if sink.len() < 2_000 {
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
            next_id: 0,
            roster: Vec::new(),
            fires: Vec::new(),
            on_error: Vec::new(),
        }
    }

    fn send(&mut self, value: &Value) {
        let stdin = self.stdin.as_mut().expect("gateway stdin remains open");
        writeln!(stdin, "{}", serde_json::to_string(value).expect("encode"))
            .expect("write request");
        stdin.flush().expect("flush request");
    }

    fn stderr_tail(&self) -> String {
        let Ok(sink) = self.stderr.lock() else {
            return "[stderr unavailable]".to_string();
        };
        let start = sink.len().saturating_sub(60);
        format!(
            "--- [{}] gateway stderr (last {} lines) ---\n{}",
            self.label,
            sink.len() - start,
            sink[start..].join("\n")
        )
    }

    /// Send one request; answer host callbacks while waiting for its
    /// response. Returns the whole response, error or not.
    fn request(&mut self, method: &str, params: Value, deadline: Duration) -> Value {
        self.next_id += 1;
        let id = format!("{}-{}", self.label, self.next_id);
        self.send(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        let start = Instant::now();
        loop {
            let Some(remaining) = deadline.checked_sub(start.elapsed()) else {
                panic!(
                    "[{}] no response to {method} within {deadline:?}\n{}",
                    self.label,
                    self.stderr_tail()
                );
            };
            let Ok(line) = self.lines.recv_timeout(remaining) else {
                panic!(
                    "[{}] no response to {method} within {deadline:?}\n{}",
                    self.label,
                    self.stderr_tail()
                );
            };
            let Ok(message) = serde_json::from_str::<Value>(line.trim()) else {
                continue;
            };
            if message.get("method").is_none()
                && message.get("id").and_then(Value::as_str) == Some(id.as_str())
            {
                return message;
            }
            self.answer_callback(&message);
        }
    }

    /// `request`, asserting success; returns `result`.
    fn call(&mut self, method: &str, params: Value) -> Value {
        let response = self.request(method, params, RPC_TIMEOUT);
        assert!(
            response.get("error").is_none(),
            "[{}] {method} failed: {response}\n{}",
            self.label,
            self.stderr_tail()
        );
        response["result"].clone()
    }

    fn answer_callback(&mut self, message: &Value) {
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            return;
        };
        if method == "meta/on_error" {
            self.on_error
                .push(message.get("params").cloned().unwrap_or(Value::Null));
        }
        let Some(id) = message.get("id").cloned() else {
            return;
        };
        match method {
            "callback/roster_provider/roster" => {
                let roster = Value::Array(self.roster.clone());
                self.send(&json!({ "jsonrpc": "2.0", "id": id, "result": roster }));
            }
            // A host runnable this harness registered fired: it collects
            // nothing, and the fire completes.
            "callback/schedule_fire" => {
                self.fires
                    .push(message.get("params").cloned().unwrap_or(Value::Null));
                self.send(&json!({ "jsonrpc": "2.0", "id": id, "result": {} }));
            }
            other if other.starts_with("callback/") => panic!(
                "[{}] unexpected host callback {other}: this harness provides a roster and \
                 schedule fires",
                self.label
            ),
            _ => {}
        }
    }

    /// The first `meta/on_error` notification of `category` after the first
    /// `skip` of them, answering callbacks while it waits.
    fn wait_for_on_error(&mut self, category: &str, skip: usize, deadline: Duration) -> Value {
        let start = Instant::now();
        loop {
            self.pump();
            if let Some(event) = self
                .on_error
                .iter()
                .filter(|event| event["category"] == json!(category))
                .nth(skip)
            {
                return event.clone();
            }
            assert!(
                start.elapsed() < deadline,
                "[{}] no meta/on_error {category} within {deadline:?}: {:?}\n{}",
                self.label,
                self.on_error,
                self.stderr_tail()
            );
            thread::sleep(Duration::from_millis(25));
        }
    }

    /// Drain stdout without blocking, answering callbacks.
    fn pump(&mut self) {
        while let Ok(line) = self.lines.try_recv() {
            if let Ok(message) = serde_json::from_str::<Value>(line.trim()) {
                self.answer_callback(&message);
            }
        }
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
                    "[{}] gateway did not exit within {deadline:?}\n{tail}",
                    self.label
                );
            }
            self.pump();
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn shutdown_and_reap(&mut self) {
        let response = self.request("meta/shutdown", json!({}), SHUTDOWN_TIMEOUT);
        gateway_shutdown::assert_persistent_shutdown_completed(
            &response,
            &format!("[{}] shutdown\n{}", self.label, self.stderr_tail()),
        );
        drop(self.stdin.take());
        let status = self.wait_for_exit(EXIT_TIMEOUT);
        assert!(
            status.success(),
            "[{}] gateway exited with {status}\n{}",
            self.label,
            self.stderr_tail()
        );
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Boot a gateway whose roster is the specialist alone, materialized at
/// startup.
fn boot(label: &str, state: &Path, host_config: &Path, fault: Option<&str>) -> Gateway {
    boot_with(
        label,
        state,
        host_config,
        fault,
        vec![roster_spec()],
        "eager_materialize",
        HELD_DELAY_MS,
    )
}

/// Boot a gateway with mob announcements enabled over `state`, answering
/// its roster callback with `roster`, bootstrapping identities in
/// `bootstrap_mode`, and sealing an owed announcement once it has waited
/// `max_delay_ms`.
fn boot_with(
    label: &str,
    state: &Path,
    host_config: &Path,
    fault: Option<&str>,
    roster: Vec<Value>,
    bootstrap_mode: &str,
    max_delay_ms: u64,
) -> Gateway {
    boot_with_runnables(
        label,
        state,
        host_config,
        fault,
        roster,
        bootstrap_mode,
        max_delay_ms,
        &[],
    )
}

/// [`boot_with`], registering `runnables` as the host's schedule runnables
/// (`runtime_options.host_runnables`): their fires arrive as
/// `callback/schedule_fire` and are recorded in [`Gateway::fires`].
#[allow(clippy::too_many_arguments)]
fn boot_with_runnables(
    label: &str,
    state: &Path,
    host_config: &Path,
    fault: Option<&str>,
    roster: Vec<Value>,
    bootstrap_mode: &str,
    max_delay_ms: u64,
    runnables: &[&str],
) -> Gateway {
    let mut gateway = Gateway::start(label, fault);
    gateway.roster = roster;
    let mut params = json!({
        "persistent_state": state,
        "mob_config": mob_config(),
        "has_roster_provider": true,
        "runtime_options": {
            "meerkat_config_path": host_config,
            "identity_bootstrap_mode": { "mode": bootstrap_mode },
            "mob_announcements": {
                "enabled": true,
                "batch": {
                    "max_count": 256,
                    "max_bytes": 1_048_576,
                    "max_delay_ms": max_delay_ms,
                    "min_wake_interval_ms": 0
                }
            }
        }
    });
    if !runnables.is_empty() {
        params["runtime_options"]["host_runnables"] = json!(runnables);
    }
    let response = gateway.request("meta/init", params, INIT_TIMEOUT);
    assert!(
        response.get("error").is_none(),
        "[{label}] init failed: {response}\n{}",
        gateway.stderr_tail()
    );
    gateway
}

fn deliveries(gateway: &mut Gateway) -> Vec<Value> {
    gateway.call(
        "meta/mob_announcements/deliveries/list",
        json!({ "member": member(IDENTITY) }),
    )["deliveries"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

#[test]
fn a_burst_lost_acknowledgment_and_crash_mid_turn_deliver_exactly_once() {
    let state = tempfile::tempdir().expect("state dir");
    let config_dir = tempfile::tempdir().expect("config dir");
    let log = Arc::new(Mutex::new(ModelLog::default()));
    let base_url = start_model(Arc::clone(&log));
    let host_config = config_dir.path().join("config.toml");
    std::fs::write(&host_config, host_config_text(&base_url)).expect("write host config");

    // Boot 1 aborts right after the native commit of the first batch.
    let mut first = boot(
        "boot-1",
        state.path(),
        &host_config,
        Some("after_native_commit"),
    );
    // The subscription takes this gateway's own mob, the default.
    let subscription = first.call(
        "meta/mob_announcements/subscriptions/put",
        json!({
            "operation_id": "subscribe-1",
            "subscription_id": "ios-payments",
            "member": member(IDENTITY),
            "topics": [TOPIC]
        }),
    )["subscription"]
        .clone();
    assert_eq!(subscription["state"], json!("active"));
    let paused = first.call(
        "meta/mob_announcements/subscriptions/pause",
        json!({
            "operation_id": "pause-1",
            "subscription_id": "ios-payments",
            "expected_revision": subscription["revision"]
        }),
    );
    let revision = paused["subscription"]["revision"].clone();

    // The specialist's original work: its first attempt is held open.
    first.call(
        "meta/send",
        json!({ "identity": IDENTITY, "content": format!("{HOLD_MARKER}: investigate the checkout regression") }),
    );
    wait_until(
        "the original work to reach the model",
        SETTLE_TIMEOUT,
        || log.lock().unwrap().held,
    );

    // The burst is announced into this gateway's own mob (the default)
    // while the subscription is paused.
    for index in 0..BURST {
        let announced = first.call(
            "meta/mob_announcements/announce",
            json!({
                "source_key": format!("failure-{index}"),
                "topics": [TOPIC],
                "body": format!("card declined during checkout for order {index}")
            }),
        );
        assert_eq!(announced["captured"], json!(1));
    }
    assert!(
        deliveries(&mut first).is_empty(),
        "nothing is sealed while paused"
    );

    // Resume: one batch is sealed and committed natively, and the process
    // aborts before the announcement owner records the receipt.
    let resume = json!({
        "operation_id": "resume-1",
        "subscription_id": "ios-payments",
        "expected_revision": revision
    });
    first.send(&json!({
        "jsonrpc": "2.0",
        "id": "resume",
        "method": "meta/mob_announcements/subscriptions/resume",
        "params": resume
    }));
    let status = first.wait_for_exit(SETTLE_TIMEOUT);
    assert!(
        !status.success(),
        "boot 1 must die at the fault point: {status}\n{}",
        first.stderr_tail()
    );
    let first_tail = first.stderr_tail();
    assert!(
        first_tail.contains(&fault_line("after_native_commit")),
        "boot 1 died somewhere else\n{first_tail}"
    );
    drop(first);

    // Boot 2: the same state, no fault.
    let mut second = boot("boot-2", state.path(), &host_config, None);
    let batch_id = {
        let mut found = None;
        let start = Instant::now();
        while found.is_none() {
            assert!(
                start.elapsed() < SETTLE_TIMEOUT,
                "no delivery after restart"
            );
            let all = deliveries(&mut second);
            assert!(all.len() <= 1, "exactly one batch, never a second: {all:?}");
            if let Some(batch) = all.first()
                && batch["state"]["state"] == json!("delivered")
            {
                found = batch["id"].as_str().map(str::to_owned);
            }
            thread::sleep(Duration::from_millis(100));
        }
        found.unwrap()
    };
    let observed = second.call(
        "meta/mob_announcements/deliveries/observe",
        json!({ "batch_id": batch_id }),
    );
    assert_eq!(observed["native"]["status"], json!("applied"), "{observed}");
    assert_eq!(
        observed["batch"]["items"].as_array().map(Vec::len),
        Some(BURST),
        "the whole burst is one batch"
    );
    assert_eq!(
        observed["batch"]["state"]["state"],
        json!("delivered"),
        "the lost acknowledgment was reconciled from native evidence"
    );

    // Both the original work and the batch complete, then one more turn
    // exposes the whole transcript the specialist carries.
    wait_until("the batch turn to reach the model", SETTLE_TIMEOUT, || {
        log.lock()
            .unwrap()
            .requests
            .iter()
            .any(|request| last_message_text(request).contains(BATCH_MARKER))
    });
    wait_until("the batch input to complete", SETTLE_TIMEOUT, || {
        let observed = second.call(
            "meta/mob_announcements/deliveries/observe",
            json!({ "batch_id": batch_id }),
        );
        observed["input"]["completion"] == json!("completed")
    });
    second.call(
        "meta/send",
        json!({ "identity": IDENTITY, "content": format!("{FINAL_MARKER}: summarize") }),
    );
    wait_until("the final turn to reach the model", SETTLE_TIMEOUT, || {
        log.lock()
            .unwrap()
            .requests
            .iter()
            .any(|request| last_message_text(request).contains(FINAL_MARKER))
    });

    let log = log.lock().unwrap();
    let batch_turns = log
        .requests
        .iter()
        .filter(|request| last_message_text(request).contains(BATCH_MARKER))
        .count();
    assert_eq!(batch_turns, 1, "exactly one model call for the whole burst");
    let final_request = log
        .requests
        .iter()
        .rev()
        .find(|request| last_message_text(request).contains(FINAL_MARKER))
        .expect("final request");
    let transcript = message_texts(final_request);
    let originals = transcript
        .iter()
        .filter(|(role, text)| role == "user" && text.contains(HOLD_MARKER))
        .count();
    assert_eq!(
        originals, 1,
        "the original work is one input: {transcript:?}"
    );
    let batches = transcript
        .iter()
        .filter(|(_, text)| text.contains(BATCH_MARKER))
        .count();
    assert_eq!(batches, 1, "the batch was admitted once: {transcript:?}");
    assert!(
        transcript
            .iter()
            .any(|(role, text)| role == "assistant" && text.contains("original work complete")),
        "the original work completed after the restart: {transcript:?}"
    );
    assert!(
        transcript
            .iter()
            .any(|(role, text)| role == "assistant" && text.contains("noted the payment failures")),
        "the batch was processed: {transcript:?}"
    );
    // The evidence this run rests on, for the log.
    eprintln!(
        "mob announcement process evidence: batch={batch_id} items={} native={} input={} \
         model_requests={} batch_turns={batch_turns} original_inputs={originals} \
         batch_inputs={batches}",
        observed["batch"]["items"].as_array().map_or(0, Vec::len),
        observed["native"]["status"],
        observed["input"],
        log.requests.len(),
    );
    eprintln!(
        "mob announcement process evidence: boot 1 ended with {status}: {}",
        first_tail
            .lines()
            .find(|line| line.contains("announcement fault point"))
            .unwrap_or_default()
    );
    drop(log);
    second.shutdown_and_reap();
}

/// A member placed on a host is registered with this gateway, but this
/// gateway's runtime does not serve its session, so no delivery to it could
/// ever be admitted: a subscription for it is refused typed, and nothing is
/// created. A member the gateway serves subscribes on the same gateway.
///
/// The roster is registered without materializing anyone: the refusal reads
/// the registered roster row alone, and no host is bound here for the placed
/// member to run on.
#[test]
fn a_subscription_for_a_member_placed_on_a_host_is_refused() {
    let state = tempfile::tempdir().expect("state dir");
    let config_dir = tempfile::tempdir().expect("config dir");
    let base_url = start_model(Arc::new(Mutex::new(ModelLog::default())));
    let host_config = config_dir.path().join("config.toml");
    std::fs::write(&host_config, host_config_text(&base_url)).expect("write host config");

    let mut gateway = boot_with(
        "placed",
        state.path(),
        &host_config,
        None,
        vec![roster_spec(), placed_roster_spec()],
        "lazy_materialize",
        HELD_DELAY_MS,
    );
    let refused = gateway.request(
        "meta/mob_announcements/subscriptions/put",
        json!({
            "operation_id": "subscribe-placed",
            "subscription_id": "placed-payments",
            "member": member(PLACED_IDENTITY),
            "topics": [TOPIC]
        }),
        RPC_TIMEOUT,
    );
    assert_eq!(refused["error"]["code"], json!(-32047), "{refused}");
    assert_eq!(
        refused["error"]["data"]["kind"],
        json!("unusable_member"),
        "{refused}"
    );
    let absent = gateway.call(
        "meta/mob_announcements/subscriptions/get",
        json!({ "subscription_id": "placed-payments" }),
    );
    assert_eq!(
        absent["subscription"],
        Value::Null,
        "a refused subscription is not created: {absent}"
    );

    // The same gateway subscribes a member its runtime serves.
    let served = gateway.call(
        "meta/mob_announcements/subscriptions/put",
        json!({
            "operation_id": "subscribe-served",
            "subscription_id": "served-payments",
            "member": member(IDENTITY),
            "topics": [TOPIC]
        }),
    )["subscription"]
        .clone();
    assert_eq!(served["state"], json!("active"), "{served}");
    gateway.shutdown_and_reap();
}

/// A member whose profile opts into the member tools announces from its own
/// model turn. The call is attributed to it from this gateway's mob roster
/// and identity runtime, nothing else: the tool answers with a count, and
/// another member's subscription receives the announcement introduced as
/// the announcer's. The subscriber's profile does not opt in, so its model
/// is never offered the tools.
#[test]
fn a_member_announces_from_its_own_turn_as_itself() {
    let state = tempfile::tempdir().expect("state dir");
    let config_dir = tempfile::tempdir().expect("config dir");
    let log = Arc::new(Mutex::new(ModelLog::default()));
    let base_url = start_model(Arc::clone(&log));
    let host_config = config_dir.path().join("config.toml");
    std::fs::write(&host_config, host_config_text(&base_url)).expect("write host config");

    let mut gateway = boot_with(
        "member-tools",
        state.path(),
        &host_config,
        None,
        vec![roster_spec(), announcer_roster_spec()],
        "eager_materialize",
        // An owed announcement is sealed at once.
        0,
    );
    gateway.call(
        "meta/mob_announcements/subscriptions/put",
        json!({
            "operation_id": "subscribe-refunds",
            "subscription_id": "refunds",
            "member": member(IDENTITY),
            "topics": [TOPIC]
        }),
    );

    // The announcer's turn: its model calls mob_announce, and the tool's
    // answer comes back in the next request.
    gateway.call(
        "meta/send",
        json!({ "identity": ANNOUNCER, "content": format!("{ANNOUNCE_MARKER}: tell the mob") }),
    );
    wait_until(
        "the tool's answer to reach the model",
        SETTLE_TIMEOUT,
        || {
            log.lock()
                .unwrap()
                .requests
                .iter()
                .any(|request| last_message_role(request) == "tool")
        },
    );
    let (offered, answer) = {
        let log = log.lock().unwrap();
        let turn = log
            .requests
            .iter()
            .find(|request| {
                last_message_role(request) != "tool"
                    && last_message_text(request).contains(ANNOUNCE_MARKER)
            })
            .expect("the announcer's turn");
        let answered = log
            .requests
            .iter()
            .find(|request| last_message_role(request) == "tool")
            .expect("the tool's answer");
        (offered_tools(turn), last_message_text(answered))
    };
    for tool in MEMBER_TOOLS {
        assert!(
            offered.iter().any(|name| name == tool),
            "the announcer's profile opts into {tool}: {offered:?}"
        );
    }
    let answer: Value = serde_json::from_str(&answer).unwrap_or_else(|error| {
        panic!("the tool refused or did not answer JSON ({error}): {answer}")
    });
    assert_eq!(answer["mob_id"], json!(MOB_ID), "{answer}");
    assert_eq!(answer["key"], json!(ANNOUNCED_KEY), "{answer}");
    assert_eq!(answer["replayed"], json!(false), "{answer}");
    assert_eq!(
        answer["captured"],
        json!(1),
        "a count of the subscriptions that captured it: {answer}"
    );

    // The subscriber's batch carries it, as the announcer's.
    let batch_id = {
        let mut found = None;
        let start = Instant::now();
        while found.is_none() {
            assert!(
                start.elapsed() < SETTLE_TIMEOUT,
                "the announcement was never delivered\n{}",
                gateway.stderr_tail()
            );
            if let Some(batch) = deliveries(&mut gateway).first()
                && batch["state"]["state"] == json!("delivered")
            {
                found = batch["id"].as_str().map(str::to_owned);
            }
            thread::sleep(Duration::from_millis(100));
        }
        found.unwrap()
    };
    let observed = gateway.call(
        "meta/mob_announcements/deliveries/observe",
        json!({ "batch_id": batch_id }),
    );
    let items = observed["batch"]["items"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(items.len(), 1, "{observed}");
    let announcement = gateway.call(
        "meta/mob_announcements/get",
        json!({ "mob_id": items[0]["mob_id"], "source_key": items[0]["source_key"] }),
    )["announcement"]
        .clone();
    assert_eq!(
        announcement["announced_by"],
        json!({ "kind": "member", "identity": ANNOUNCER }),
        "{announcement}"
    );
    assert_eq!(
        announcement["body"],
        json!(ANNOUNCED_BODY),
        "{announcement}"
    );
    assert_eq!(announcement["mob_id"], json!(MOB_ID), "{announcement}");

    wait_until("the batch turn to reach the model", SETTLE_TIMEOUT, || {
        log.lock()
            .unwrap()
            .requests
            .iter()
            .any(|request| last_message_text(request).contains(BATCH_MARKER))
    });
    let log = log.lock().unwrap();
    let batch_turn = log
        .requests
        .iter()
        .find(|request| last_message_text(request).contains(BATCH_MARKER))
        .expect("the batch turn");
    let text = last_message_text(batch_turn);
    assert!(
        text.contains(&format!("by member \"{ANNOUNCER}\"")),
        "the batch introduces the announcement as the announcer's: {text}"
    );
    assert!(
        text.contains(&format!("> {ANNOUNCED_BODY}")),
        "the body is quoted: {text}"
    );
    let offered = offered_tools(batch_turn);
    assert!(
        !offered
            .iter()
            .any(|name| MEMBER_TOOLS.contains(&name.as_str())),
        "a profile that does not opt in is not offered the member tools: {offered:?}"
    );
    eprintln!(
        "mob announcement member tool evidence: answer={answer} batch={batch_id} \
         announced_by={} model_requests={}",
        announcement["announced_by"],
        log.requests.len()
    );
    drop(log);
    gateway.shutdown_and_reap();
}

// ---------------------------------------------------------------------------
// Members of a mob a member created
// ---------------------------------------------------------------------------

/// Boot a gateway whose roster is the lead alone, sealing an owed
/// announcement at once, with or without the member tools in the mobs its
/// members create.
fn boot_lead(label: &str, state: &Path, host_config: &Path, child_tools: bool) -> Gateway {
    let mut gateway = Gateway::start(label, None);
    gateway.roster = vec![lead_roster_spec()];
    let response = gateway.request(
        "meta/init",
        json!({
            "persistent_state": state,
            "mob_config": mob_config(),
            "has_roster_provider": true,
            "runtime_options": {
                "meerkat_config_path": host_config,
                "identity_bootstrap_mode": { "mode": "eager_materialize" },
                "mob_announcements": {
                    "enabled": true,
                    "batch": {
                        "max_count": 256,
                        "max_bytes": 1_048_576,
                        "max_delay_ms": 0,
                        "min_wake_interval_ms": 0
                    },
                    "member_tools_in_child_mobs": child_tools
                }
            }
        }),
        INIT_TIMEOUT,
    );
    assert!(
        response.get("error").is_none(),
        "[{label}] init failed: {response}\n{}",
        gateway.stderr_tail()
    );
    gateway
}

/// The lead creates the child mob and spawns its member, whose first turn
/// answers the subscribe prompt. Returns once that turn reached the model.
/// `opt_in` has the child profile opt in to the member tools.
fn create_child(gateway: &mut Gateway, log: &Arc<Mutex<ModelLog>>, opt_in: bool) {
    let marker = if opt_in {
        CREATE_OPTED_MARKER
    } else {
        CREATE_MARKER
    };
    gateway.call(
        "meta/send",
        json!({ "identity": LEAD, "content": format!("{marker}: start a team") }),
    );
    let start = Instant::now();
    loop {
        gateway.pump();
        let log = log.lock().unwrap();
        if log.requests.iter().any(|request| {
            last_message_role(request) != "tool"
                && last_message_text(request).contains(SUBSCRIBE_MARKER)
        }) {
            return;
        }
        if start.elapsed() > SETTLE_TIMEOUT {
            let answered = log
                .requests
                .iter()
                .filter(|request| last_message_role(request) == "tool")
                .map(|request| (last_tool_called(request), last_message_text(request)))
                .collect::<Vec<_>>();
            panic!(
                "the child member's first turn never reached the model; the lead's \
                 tools answered {answered:?}\n{}",
                gateway.stderr_tail()
            );
        }
        drop(log);
        thread::sleep(Duration::from_millis(50));
    }
}

/// The tools the child member's first turn was offered.
fn child_turn_tools(log: &Arc<Mutex<ModelLog>>) -> Vec<String> {
    let log = log.lock().unwrap();
    let turn = log
        .requests
        .iter()
        .find(|request| {
            last_message_role(request) != "tool"
                && last_message_text(request).contains(SUBSCRIBE_MARKER)
        })
        .expect("the child member's first turn");
    offered_tools(turn)
}

/// Announce into the lead's mob and wait until the child member's batch is
/// delivered; returns its id.
fn announce_and_deliver_to_child(gateway: &mut Gateway, key: &str) -> String {
    gateway.call(
        "meta/mob_announcements/announce",
        json!({
            "mob_id": MOB_ID,
            "source_key": key,
            "topics": [TOPIC],
            "body": format!("{key}: the payment provider is degraded")
        }),
    );
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() < SETTLE_TIMEOUT,
            "the child member never received {key}\n{}",
            gateway.stderr_tail()
        );
        let delivered = gateway.call(
            "meta/mob_announcements/deliveries/list",
            json!({ "member": child_member() }),
        )["deliveries"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .find(|batch| {
                batch["state"]["state"] == json!("delivered")
                    && batch["body"]
                        .as_str()
                        .is_some_and(|body| body.contains(key))
            });
        if let Some(batch) = delivered {
            return batch["id"].as_str().expect("a batch id").to_owned();
        }
        thread::sleep(Duration::from_millis(100));
    }
}

/// Prompt the lead, and wait until the `tool` call its turn makes has
/// returned to the model; returns what the tool answered.
fn lead_calls(
    gateway: &mut Gateway,
    log: &Arc<Mutex<ModelLog>>,
    prompt: &str,
    tool: &str,
) -> String {
    gateway.call("meta/send", json!({ "identity": LEAD, "content": prompt }));
    let mut answer = None;
    wait_until(
        &format!("the lead's {tool} call to return"),
        SETTLE_TIMEOUT,
        || {
            gateway.pump();
            answer = log
                .lock()
                .unwrap()
                .requests
                .iter()
                .find(|request| {
                    last_message_role(request) == "tool"
                        && last_tool_called(request).as_deref() == Some(tool)
                })
                .map(last_message_text);
            answer.is_some()
        },
    );
    answer.unwrap_or_default()
}

/// The child member's subscription named `id`, as the host reads it now.
fn child_subscription(gateway: &mut Gateway, id: &str) -> Value {
    gateway.call(
        "meta/mob_announcements/subscriptions/get",
        json!({ "subscription_id": id }),
    )["subscription"]
        .clone()
}

/// Wait until the subscription named `id` has ended; returns it.
fn wait_until_ended(gateway: &mut Gateway, id: &str) -> Value {
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() < SETTLE_TIMEOUT,
            "the child member's subscription never ended\n{}",
            gateway.stderr_tail()
        );
        let current = child_subscription(gateway, id);
        if current["state"] == json!("ended") {
            return current;
        }
        thread::sleep(Duration::from_millis(100));
    }
}

/// P1: under the `member_tools_in_child_mobs` ceiling, a member of a mob
/// the lead created, whose profile opts in to the member tools in its
/// `tools.bundles`, is offered them, subscribes itself to the lead's mob
/// from its own turn, and receives what is announced there.
#[test]
fn a_member_of_a_mob_a_member_created_subscribes_itself_to_the_mob_above() {
    let state = tempfile::tempdir().expect("state dir");
    let config_dir = tempfile::tempdir().expect("config dir");
    let log = Arc::new(Mutex::new(ModelLog::default()));
    let base_url = start_model(Arc::clone(&log));
    let host_config = config_dir.path().join("config.toml");
    std::fs::write(&host_config, host_config_text(&base_url)).expect("write host config");
    let mut gateway = boot_lead("child-tools", state.path(), &host_config, true);

    create_child(&mut gateway, &log, true);
    let offered = child_turn_tools(&log);
    for tool in MEMBER_TOOLS {
        assert!(
            offered.iter().any(|name| name == tool),
            "the child's profile is given {tool}: {offered:?}"
        );
    }
    // Its own subscription, attributed to it, names the lead's mob.
    let start = Instant::now();
    let subscription = loop {
        assert!(
            start.elapsed() < SETTLE_TIMEOUT,
            "the child member never subscribed itself\n{}",
            gateway.stderr_tail()
        );
        let listed = gateway.call(
            "meta/mob_announcements/subscriptions/list",
            json!({ "member": child_member() }),
        )["subscriptions"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        if let Some(subscription) = listed.into_iter().next() {
            break subscription;
        }
        thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(subscription["member"], child_member(), "{subscription}");
    assert_eq!(
        subscription["selector"]["mobs"],
        json!([MOB_ID]),
        "{subscription}"
    );
    assert_eq!(subscription["state"], json!("active"), "{subscription}");

    announce_and_deliver_to_child(&mut gateway, "outage-1");
    gateway.shutdown_and_reap();
}

/// Under the ceiling, a child profile that does not opt in gets no member
/// tools: the bundle is offered to child mobs, never supplied to every
/// child profile.
#[test]
fn under_the_ceiling_a_child_profile_without_the_opt_in_gets_no_member_tools() {
    let state = tempfile::tempdir().expect("state dir");
    let config_dir = tempfile::tempdir().expect("config dir");
    let log = Arc::new(Mutex::new(ModelLog::default()));
    let base_url = start_model(Arc::clone(&log));
    let host_config = config_dir.path().join("config.toml");
    std::fs::write(&host_config, host_config_text(&base_url)).expect("write host config");
    let mut gateway = boot_lead("child-no-opt-in", state.path(), &host_config, true);

    create_child(&mut gateway, &log, false);
    let offered = child_turn_tools(&log);
    for tool in MEMBER_TOOLS {
        assert!(
            !offered.iter().any(|name| name == tool),
            "a child profile without the opt-in is not given {tool}: {offered:?}"
        );
    }
    gateway.shutdown_and_reap();
}

/// Boot a gateway whose lead creates a mob and spawns its member, and
/// subscribe that member to the lead's mob through the host RPC. Without
/// the option the member is not offered the member tools.
fn host_subscribed_child(
    label: &str,
    state: &Path,
    host_config: &Path,
    log: &Arc<Mutex<ModelLog>>,
) -> (Gateway, Value) {
    let mut gateway = boot_lead(label, state, host_config, false);
    create_child(&mut gateway, log, false);
    let offered = child_turn_tools(log);
    for tool in MEMBER_TOOLS {
        assert!(
            !offered.iter().any(|name| name == tool),
            "the member tools are host-only by default: {offered:?}"
        );
    }
    let subscription = gateway.call(
        "meta/mob_announcements/subscriptions/put",
        json!({
            "operation_id": "subscribe-child",
            "subscription_id": "child-from-root",
            "member": child_member(),
            "topics": [TOPIC],
            "mobs": [MOB_ID]
        }),
    )["subscription"]
        .clone();
    assert_eq!(subscription["state"], json!("active"), "{subscription}");
    (gateway, subscription)
}

/// P2: the host RPC subscribes a member of a mob the lead created to the
/// lead's mob, and it receives what is announced there. A restart restores
/// the child mob with every other: its owner's session is durable, so it is
/// not held. The subscription stays active and the member is delivered to
/// again.
#[test]
fn the_host_subscribes_a_member_of_a_mob_a_member_created_across_a_restart() {
    let state = tempfile::tempdir().expect("state dir");
    let config_dir = tempfile::tempdir().expect("config dir");
    let log = Arc::new(Mutex::new(ModelLog::default()));
    let base_url = start_model(Arc::clone(&log));
    let host_config = config_dir.path().join("config.toml");
    std::fs::write(&host_config, host_config_text(&base_url)).expect("write host config");
    let (mut gateway, subscription) =
        host_subscribed_child("child-host-1", state.path(), &host_config, &log);
    announce_and_deliver_to_child(&mut gateway, "outage-1");
    gateway.shutdown_and_reap();
    drop(gateway);

    // The same state again: the child mob is restored, running.
    let mut gateway = boot_lead("child-host-2", state.path(), &host_config, false);
    announce_and_deliver_to_child(&mut gateway, "outage-2");
    let current = child_subscription(&mut gateway, "child-from-root");
    assert_eq!(current["state"], json!("active"), "{current}");
    assert_eq!(current["revision"], subscription["revision"], "{current}");
    gateway.shutdown_and_reap();
}

/// P3: a member retired from its mob's roster has no live incarnation to
/// deliver to, so its batch waits and nothing ends. Destroying its mob then
/// ends the subscription as `member_mob_ended`.
#[test]
fn a_retired_member_waits_until_its_destroyed_mob_ends_the_subscription() {
    let state = tempfile::tempdir().expect("state dir");
    let config_dir = tempfile::tempdir().expect("config dir");
    let log = Arc::new(Mutex::new(ModelLog::default()));
    let base_url = start_model(Arc::clone(&log));
    let host_config = config_dir.path().join("config.toml");
    std::fs::write(&host_config, host_config_text(&base_url)).expect("write host config");
    let (mut gateway, _) = host_subscribed_child("child-retire", state.path(), &host_config, &log);
    announce_and_deliver_to_child(&mut gateway, "outage-1");

    // The lead retires the member: off its mob's roster, it has no live
    // incarnation to deliver to, so its batch waits and nothing ends.
    let answer = lead_calls(
        &mut gateway,
        &log,
        &format!("{RETIRE_MARKER}: the worker is done"),
        "mob_retire_member",
    );
    gateway.call(
        "meta/mob_announcements/announce",
        json!({
            "mob_id": MOB_ID,
            "source_key": "outage-2",
            "topics": [TOPIC],
            "body": "outage-2: the payment provider is degraded"
        }),
    );
    // An attempt for it commits nothing: the batch is owed again, and the
    // member's marker says why. Waiting ends nothing.
    let start = Instant::now();
    let waiting = loop {
        if start.elapsed() > SETTLE_TIMEOUT {
            let listed = gateway.call(
                "meta/mob_announcements/deliveries/list",
                json!({ "member": child_member() }),
            );
            panic!(
                "the retired member's batch never waited on member_not_live; retire \
                 answered {answer}; deliveries {listed}\n{}",
                gateway.stderr_tail()
            );
        }
        if child_subscription(&mut gateway, "child-from-root")["state"] == json!("ended") {
            break None;
        }
        let owed = gateway.call(
            "meta/mob_announcements/deliveries/list",
            json!({ "member": child_member() }),
        )["deliveries"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .find(|batch| {
                batch["body"]
                    .as_str()
                    .is_some_and(|body| body.contains("outage-2"))
            });
        if let Some(batch) = owed {
            let observed = gateway.call(
                "meta/mob_announcements/deliveries/observe",
                json!({ "batch_id": batch["id"] }),
            );
            if observed["batch"]["state"]["state"] == json!("sealed")
                && observed["marker"]["cause"]["kind"] == json!("member_not_live")
                && observed["marker"]["evidence"] == json!("local")
            {
                break Some(batch);
            }
        }
        thread::sleep(Duration::from_millis(100));
    };
    if let Some(batch) = &waiting {
        // One more attempt, made and answered before this returns.
        gateway.call(
            "meta/mob_announcements/deliveries/reconcile",
            json!({ "batch_id": batch["id"] }),
        );
    }
    let current = child_subscription(&mut gateway, "child-from-root");
    assert_eq!(
        current["state"],
        json!("active"),
        "{current}\nretire answered: {answer}"
    );
    assert!(waiting.is_some(), "the batch waits for the member");

    // The lead destroys the child mob: the subscription ends typed.
    gateway.call(
        "meta/send",
        json!({ "identity": LEAD, "content": format!("{DESTROY_MARKER}: the team is done") }),
    );
    let ended = wait_until_ended(&mut gateway, "child-from-root");
    assert_eq!(
        ended["ended"],
        json!({ "kind": "member_mob_ended", "why": "destroyed" }),
        "{ended}"
    );
    gateway.shutdown_and_reap();
}

// ---------------------------------------------------------------------------
// Announcement sources across a crash
// ---------------------------------------------------------------------------

const SOURCE_TOPIC: &str = "mail.received";
const SOURCE_ITEM_MARKER: &str = "SOURCE-ITEM-INVOICE-7";

/// The verified account the source "inbox" reads.
fn inbox_principal() -> Value {
    json!({
        "kind": "connector_verified_account",
        "issuer": "https://accounts.example",
        "strategy_id": "oauth-code",
        "subject": "subject-1"
    })
}

/// Boot a gateway over `state` whose roster is the specialist, sealing an
/// owed announcement at once.
fn boot_collecting(label: &str, state: &Path, host_config: &Path, fault: Option<&str>) -> Gateway {
    boot_with(
        label,
        state,
        host_config,
        fault,
        vec![roster_spec()],
        "eager_materialize",
        0,
    )
}

/// Declare the source "inbox" in this gateway's mob, take its baseline, and
/// subscribe the specialist to its topic.
fn declare_live_inbox(gateway: &mut Gateway) {
    gateway.call(
        "meta/mob_announcements/sources/put",
        json!({
            "operation_id": "put-inbox",
            "source_id": "inbox",
            "identity": {
                "provider": "google.gmail",
                "principal": inbox_principal(),
                "scope": { "kind": "mailbox_label", "id": "INBOX" }
            },
            "connection": { "kind": "connect_setup", "id": "setup-1" },
            "observation": { "baseline": "delta", "deletions": "explicit" },
            "topics": [SOURCE_TOPIC],
            "audience": [{ "kind": "member", "member": member(IDENTITY) }]
        }),
    );
    let baseline = gateway.call(
        "meta/mob_announcements/sources/advance",
        json!({
            "operation_id": "baseline",
            "source_id": "inbox",
            "expected_revision": 1,
            "expected_version": 0,
            "principal": inbox_principal(),
            "next_cursor": "c1",
            "coverage": "complete"
        }),
    );
    assert_eq!(
        baseline["source"]["collection"],
        json!("live"),
        "{baseline}"
    );
    gateway.call(
        "meta/mob_announcements/subscriptions/put",
        json!({
            "operation_id": "subscribe-inbox",
            "subscription_id": "inbox-mail",
            "member": member(IDENTITY),
            "topics": [SOURCE_TOPIC]
        }),
    );
}

/// The collector's step that brings one new invoice mail.
fn invoice_step() -> Value {
    json!({
        "operation_id": "schedule:collector:occurrence:1:v1",
        "source_id": "inbox",
        "expected_revision": 1,
        "expected_version": 1,
        "principal": inbox_principal(),
        "next_cursor": "c2",
        "items": [{
            "item_id": "msg-7",
            "version": "h-1",
            "change": "upsert",
            "topics": [SOURCE_TOPIC],
            "body": format!("{SOURCE_ITEM_MARKER}: an invoice arrived")
        }]
    })
}

/// Send the invoice step to a gateway booted at `point` and wait for it to
/// die there.
fn advance_into_fault(gateway: &mut Gateway, point: &str) {
    gateway.send(&json!({
        "jsonrpc": "2.0",
        "id": "advance",
        "method": "meta/mob_announcements/sources/advance",
        "params": invoice_step()
    }));
    let status = gateway.wait_for_exit(SETTLE_TIMEOUT);
    assert!(
        !status.success(),
        "the gateway must die at the fault point: {status}\n{}",
        gateway.stderr_tail()
    );
    let tail = gateway.stderr_tail();
    assert!(
        tail.contains(&fault_line(point)),
        "the gateway died somewhere else\n{tail}"
    );
}

/// The specialist's one delivered batch, waiting until it is delivered and
/// checking at every read that there is never a second one.
fn the_one_delivered_batch(gateway: &mut Gateway) -> Value {
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() < SETTLE_TIMEOUT,
            "the item was never delivered\n{}",
            gateway.stderr_tail()
        );
        let all = deliveries(gateway);
        assert!(all.len() <= 1, "exactly one batch, never a second: {all:?}");
        if let Some(batch) = all.first()
            && batch["state"]["state"] == json!("delivered")
        {
            return batch.clone();
        }
        thread::sleep(Duration::from_millis(100));
    }
}

/// How many model requests carried the invoice as their new input.
fn invoice_turns(log: &Arc<Mutex<ModelLog>>) -> usize {
    log.lock()
        .unwrap()
        .requests
        .iter()
        .filter(|request| last_message_text(request).contains(SOURCE_ITEM_MARKER))
        .count()
}

#[test]
fn a_gateway_killed_inside_an_advance_keeps_neither_items_nor_cursor() {
    let state = tempfile::tempdir().expect("state dir");
    let config_dir = tempfile::tempdir().expect("config dir");
    let log = Arc::new(Mutex::new(ModelLog::default()));
    let base_url = start_model(Arc::clone(&log));
    let host_config = config_dir.path().join("config.toml");
    std::fs::write(&host_config, host_config_text(&base_url)).expect("write host config");

    let mut first = boot_collecting("inside-1", state.path(), &host_config, None);
    declare_live_inbox(&mut first);
    first.shutdown_and_reap();
    drop(first);

    // Boot 2 dies inside the advance's transaction, after its item and its
    // cursor were written and before the commit.
    let mut second = boot_collecting(
        "inside-2",
        state.path(),
        &host_config,
        Some("inside_source_advance"),
    );
    advance_into_fault(&mut second, "inside_source_advance");
    drop(second);

    // Boot 3: neither the item nor the cursor survived.
    let mut third = boot_collecting("inside-3", state.path(), &host_config, None);
    let source = third.call(
        "meta/mob_announcements/sources/get",
        json!({ "source_id": "inbox" }),
    )["source"]
        .clone();
    assert_eq!(source["cursor"]["version"], json!(1), "{source}");
    assert_eq!(source["cursor"]["token"], json!("c1"), "{source}");
    assert!(deliveries(&mut third).is_empty(), "nothing was captured");
    // The same step is new to the store: it commits now, and is delivered.
    let advanced = third.call("meta/mob_announcements/sources/advance", invoice_step());
    assert_eq!(advanced["replayed"], json!(false), "{advanced}");
    assert_eq!(
        advanced["items"][0]["outcome"],
        json!("announced"),
        "{advanced}"
    );
    assert_eq!(
        advanced["source"]["cursor"]["version"],
        json!(2),
        "{advanced}"
    );
    let batch = the_one_delivered_batch(&mut third);
    wait_until(
        "the invoice turn to reach the model",
        SETTLE_TIMEOUT,
        || invoice_turns(&log) > 0,
    );
    assert_eq!(invoice_turns(&log), 1, "{batch}");
    third.shutdown_and_reap();
}

#[test]
fn a_gateway_killed_after_an_advance_delivers_its_items_exactly_once() {
    let state = tempfile::tempdir().expect("state dir");
    let config_dir = tempfile::tempdir().expect("config dir");
    let log = Arc::new(Mutex::new(ModelLog::default()));
    let base_url = start_model(Arc::clone(&log));
    let host_config = config_dir.path().join("config.toml");
    std::fs::write(&host_config, host_config_text(&base_url)).expect("write host config");

    let mut first = boot_collecting("after-1", state.path(), &host_config, None);
    declare_live_inbox(&mut first);
    first.shutdown_and_reap();
    drop(first);

    // Boot 2 dies after the advance committed, before the collector hears
    // its answer.
    let mut second = boot_collecting(
        "after-2",
        state.path(),
        &host_config,
        Some("after_source_advance"),
    );
    advance_into_fault(&mut second, "after_source_advance");
    drop(second);

    // Boot 3: the collector retries its step and is answered the original;
    // the specialist gets the invoice in exactly one batch and one turn.
    let mut third = boot_collecting("after-3", state.path(), &host_config, None);
    let retried = third.call("meta/mob_announcements/sources/advance", invoice_step());
    assert_eq!(retried["replayed"], json!(true), "{retried}");
    assert_eq!(
        retried["items"][0]["outcome"],
        json!("announced"),
        "the original answer: {retried}"
    );
    assert_eq!(
        retried["source"]["cursor"]["version"],
        json!(2),
        "{retried}"
    );
    let batch = the_one_delivered_batch(&mut third);
    let observed = third.call(
        "meta/mob_announcements/deliveries/observe",
        json!({ "batch_id": batch["id"] }),
    );
    assert_eq!(
        observed["batch"]["items"].as_array().map(Vec::len),
        Some(1),
        "{observed}"
    );
    wait_until(
        "the invoice turn to reach the model",
        SETTLE_TIMEOUT,
        || invoice_turns(&log) > 0,
    );
    assert_eq!(invoice_turns(&log), 1, "{observed}");
    third.shutdown_and_reap();
}

/// A source's runner first fires at its declaration, within the schedule
/// host's own pickup of due occurrences.
const RUNNER_FIRE_TIMEOUT: Duration = Duration::from_secs(90);

/// A source declared with a runner gets a schedule that fires the host's
/// runnable (here an SDK one, `callback/schedule_fire`) with the source it
/// collects, without waiting a whole interval.
#[test]
fn a_source_runner_fires_the_hosts_runnable_with_its_source() {
    let state = tempfile::tempdir().expect("state dir");
    let config_dir = tempfile::tempdir().expect("config dir");
    let log = Arc::new(Mutex::new(ModelLog::default()));
    let base_url = start_model(Arc::clone(&log));
    let host_config = config_dir.path().join("config.toml");
    std::fs::write(&host_config, host_config_text(&base_url)).expect("write host config");

    let mut gateway = boot_with_runnables(
        "runner",
        state.path(),
        &host_config,
        None,
        vec![roster_spec()],
        "eager_materialize",
        0,
        &["inbox-collector"],
    );
    let declared = gateway.call(
        "meta/mob_announcements/sources/put",
        json!({
            "operation_id": "put-inbox",
            "source_id": "inbox",
            "identity": {
                "provider": "google.gmail",
                "principal": inbox_principal(),
                "scope": { "kind": "mailbox_label", "id": "INBOX" }
            },
            "connection": { "kind": "connect_setup", "id": "setup-1" },
            "observation": { "baseline": "delta", "deletions": "explicit" },
            "topics": [SOURCE_TOPIC],
            "audience": [{ "kind": "member", "member": member(IDENTITY) }],
            "runner": { "host_runnable": "inbox-collector", "every_seconds": 60 }
        }),
    );
    assert_eq!(
        declared["source"]["runner"],
        json!({ "host_runnable": "inbox-collector", "every_seconds": 60 })
    );
    // The schedule Meta keeps for the runner fires the host's runnable with
    // the source it collects.
    let start = Instant::now();
    while gateway.fires.is_empty() {
        assert!(
            start.elapsed() < RUNNER_FIRE_TIMEOUT,
            "the runner never fired\n{}",
            gateway.stderr_tail()
        );
        gateway.pump();
        thread::sleep(Duration::from_millis(200));
    }
    let fire = gateway.fires[0].clone();
    assert_eq!(fire["runnable"], json!("inbox-collector"), "{fire}");
    assert_eq!(
        fire["occurrence"]["payload"],
        json!({ "mob_id": MOB_ID, "source_id": "inbox" }),
        "{fire}"
    );
    gateway.shutdown_and_reap();
}

/// A source whose collections keep failing escalates: the host hears it as a
/// `meta/on_error` notification carrying the escalation and its event
/// cursor, and hears the same escalation id resolve once a collection
/// succeeds.
#[test]
fn an_escalation_reaches_the_host_as_meta_on_error_and_resolves_with_the_same_id() {
    let state = tempfile::tempdir().expect("state dir");
    let config_dir = tempfile::tempdir().expect("config dir");
    let log = Arc::new(Mutex::new(ModelLog::default()));
    let base_url = start_model(Arc::clone(&log));
    let host_config = config_dir.path().join("config.toml");
    std::fs::write(&host_config, host_config_text(&base_url)).expect("write host config");
    let mut gateway = boot_collecting("escalation", state.path(), &host_config, None);
    declare_live_inbox(&mut gateway);
    for attempt in 1..=3 {
        gateway.call(
            "meta/mob_announcements/sources/record_failure",
            json!({
                "operation_id": format!("failure-{attempt}"),
                "source_id": "inbox",
                "expected_revision": 1,
                "expected_version": 1,
                "principal": inbox_principal(),
                "failure": { "class": "provider_unavailable" }
            }),
        );
    }
    let raised = gateway.wait_for_on_error("mob_announcement_escalated", 0, SETTLE_TIMEOUT);
    assert_eq!(
        raised["escalation"]["cause"],
        json!("failure_streak"),
        "{raised}"
    );
    assert_eq!(raised["event"], json!("raised"), "{raised}");
    assert_eq!(
        raised["escalation"]["subject"],
        json!({ "kind": "source", "mob_id": MOB_ID, "source_id": "inbox" }),
        "{raised}"
    );
    gateway.call(
        "meta/mob_announcements/sources/advance",
        json!({
            "operation_id": "recovered",
            "source_id": "inbox",
            "expected_revision": 1,
            "expected_version": 1,
            "principal": inbox_principal(),
            "next_cursor": "c2"
        }),
    );
    let resolved =
        gateway.wait_for_on_error("mob_announcement_escalation_resolved", 0, SETTLE_TIMEOUT);
    assert_eq!(
        resolved["escalation_id"], raised["escalation"]["escalation_id"],
        "{resolved}"
    );
    assert!(
        resolved["cursor"].as_u64() > raised["cursor"].as_u64(),
        "{raised} then {resolved}"
    );
    let read = gateway.call(
        "meta/mob_announcements/escalations/list",
        json!({ "after": raised["cursor"].as_u64().unwrap() - 1 }),
    );
    assert_eq!(read["events"].as_array().map(Vec::len), Some(2), "{read}");
    gateway.shutdown_and_reap();
}
