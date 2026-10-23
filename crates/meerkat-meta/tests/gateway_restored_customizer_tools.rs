//! A persistent gateway's mob can be restored while the gateway's bootstrap
//! builds it: a mob log left Running revives every member inside meerkat's
//! own resume, before the identity bootstrap publishes any member's
//! customizer tools. As on the library builder, each such member holds its
//! run starts (`HostRunStartHoldReason::ToolsNotPublished`) until the identity
//! runtime publishes its tools and releases the hold, so it starts no run (a
//! restored queued input, a kickoff, a peer message) without them. A
//! process-local gateway restores nothing at its bootstrap: its member is
//! built with its tools and nothing is held. Each boot is a separate gateway
//! process over real stdio, answering the SDK callbacks itself; the first
//! persistent boot ends with a kill, so its mob log stays Running.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use meerkat_meta::UnifiedRuntime;
use meerkat_meta::rpc_gateway::{
    GatewayBootstrapReady, GatewayHostCloseReason, GatewayHostComposition, GatewayHostError,
};
use meerkat_mob::MobSessionService;
use meerkat_runtime::meerkat_machine::NativeGrantWorkConfiguration;
use serde_json::{Value, json};

/// Set only in the gateway host process; `RECORD` names its record file.
const HOST_PROCESS: &str = "META_GATEWAY_RESTORED_TOOLS_HOST";
const RECORD: &str = "META_GATEWAY_RESTORED_TOOLS_RECORD";
const PERSISTENT_TEST: &str =
    "a_member_restored_at_a_persistent_gateway_bootstrap_holds_its_runs_until_its_tools_publish";
const PROCESS_LOCAL_TEST: &str = "a_process_local_gateway_bootstrap_holds_no_member";
const BACKSTOP: Duration = Duration::from_mins(3);
/// Mob members need comms; `turn_driven` keeps the member from starting
/// turns of its own.
const MOB_CONFIG: &str = r#"
[mob]
id = "gateway-restored-customizer-tools"

[profiles.worker]
model = "gpt-5.5"
runtime_mode = "turn_driven"

[profiles.worker.tools]
comms = true
"#;
/// The one roster member, and the tool the SDK customizer gives it.
const IDENTITY: &str = "keeper";
const TOOL: &str = "lookup";

fn append(record: &Path, line: &str) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(record)
        .expect("record");
    writeln!(file, "{line}").expect("append");
}

/// Whether meerkat holds each session's run starts, in session order
/// (`None` for a session its runtime does not register).
async fn run_starts_held(service: &Arc<dyn MobSessionService>) -> Vec<Option<bool>> {
    let Some(machine) = service
        .acquire_runtime_adapter(None)
        .expect("acquire the gateway's runtime adapter")
    else {
        return Vec::new();
    };
    let mut sessions = service
        .list(meerkat_core::service::SessionQuery::default())
        .await
        .expect("sessions")
        .into_iter()
        .map(|summary| summary.session_id)
        .collect::<Vec<_>>();
    sessions.sort_by_key(ToString::to_string);
    let mut held = Vec::new();
    for session in &sessions {
        held.push(machine.run_starts_held_for_test(session).await);
    }
    held
}

/// Records whether meerkat holds the run starts of the members it restored,
/// once when the gateway binds the runtime (after the mob build, before the
/// identity bootstrap) and once after the Ready publication (after the
/// identity bootstrap).
struct HoldProbeHost {
    record: PathBuf,
    service: Mutex<Option<Arc<dyn MobSessionService>>>,
}

#[async_trait::async_trait]
impl GatewayHostComposition for HoldProbeHost {
    async fn prepare_bootstrap(
        &self,
        spec: meerkat_meta::MobBootstrapSpec,
    ) -> Result<meerkat_meta::MobBootstrapSpec, GatewayHostError> {
        Ok(spec)
    }

    fn native_grant_work_configuration(
        &self,
    ) -> Result<Option<NativeGrantWorkConfiguration>, GatewayHostError> {
        Ok(None)
    }

    async fn bind_runtime(&self, runtime: &mut UnifiedRuntime) -> Result<(), GatewayHostError> {
        let service = runtime
            .mob_runtime()
            .session_service()
            .cloned()
            .expect("the gateway's session service");
        let held = run_starts_held(&service).await;
        append(&self.record, &format!("bind held={held:?}"));
        *self.service.lock().unwrap() = Some(service);
        Ok(())
    }

    fn publish_bootstrap_ready(
        &self,
        publication: GatewayBootstrapReady<'_>,
    ) -> Result<(), GatewayHostError> {
        publication.publish();
        let service = self.service.lock().unwrap().clone();
        let record = self.record.clone();
        tokio::spawn(async move {
            let held = match service {
                Some(service) => run_starts_held(&service).await,
                None => Vec::new(),
            };
            append(&record, &format!("ready held={held:?}"));
        });
        Ok(())
    }

    fn begin_close(&self, _reason: GatewayHostCloseReason) -> Result<(), GatewayHostError> {
        Ok(())
    }

    async fn join_close(&self) -> Result<(), GatewayHostError> {
        Ok(())
    }

    async fn wait_closed(&self) {
        std::future::pending::<()>().await;
    }
}

/// In the gateway host process, run the gateway with the probing host and
/// exit.
fn serve_if_host_process() {
    if std::env::var_os(HOST_PROCESS).is_none() {
        return;
    }
    let host = Arc::new(HoldProbeHost {
        record: std::env::var_os(RECORD).expect("record path").into(),
        service: Mutex::new(None),
    });
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let _ = runtime.block_on(meerkat_meta::rpc_gateway::run_persistent_with_host(
        None, host,
    ));
    std::process::exit(0);
}

/// The SDK side of the gateway's callback bridge: the roster holds one
/// member, and the customizer gives it one callback tool.
fn answer_callback(method: &str, params: &Value) -> Result<Value, String> {
    match method {
        "callback/roster_provider/roster" => {
            Ok(json!([{ "identity": IDENTITY, "profile": "worker" }]))
        }
        "callback/agent_customizer/customize_build" => {
            let mut draft = params["draft"].clone();
            draft["external_tools"] = json!([{
                "name": TOOL,
                "description": "Look a value up",
                "input_schema": { "type": "object" },
            }]);
            Ok(draft)
        }
        "callback/agent_customizer/after_create" => Ok(Value::Null),
        other => Err(format!("no handler for {other}")),
    }
}

/// How a boot's gateway process ends once its init settled ready.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Ending {
    /// Close its stdin: the gateway shuts down and stops its mob.
    Close,
    /// Kill it: its mob log stays Running.
    Kill,
}

/// One boot: its record lines, its settled outcome, and the SDK customizer
/// builds it asked for.
struct Boot {
    lines: Vec<String>,
    outcome: String,
    customizer_builds: usize,
}

impl Boot {
    fn line(&self, prefix: &str) -> &str {
        self.lines
            .iter()
            .map(String::as_str)
            .find(|line| line.starts_with(prefix))
            .unwrap_or_else(|| panic!("a {prefix:?} line is recorded: {:?}", self.lines))
    }
}

/// Run one gateway host process of `test` in `dir` over `state` (none: a
/// process-local launch) through its init, wait for the host's Ready probe,
/// and end it as `ending` says.
fn boot(test: &str, dir: &Path, state: Option<&Path>, ending: Ending) -> Boot {
    std::fs::create_dir_all(dir).expect("boot dir");
    let record = dir.join("record");
    let stderr_path = dir.join("stderr");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env(HOST_PROCESS, "1")
        .env(RECORD, &record)
        .env("OPENAI_API_KEY", "sk-regression-test")
        .env("XDG_STATE_HOME", dir.join("xdg-state"))
        .current_dir(dir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::fs::File::create(&stderr_path).expect("stderr file"))
        .spawn()
        .expect("gateway host process");
    let stdout = child.stdout.take().expect("stdout");
    let (tx, lines) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let mut stdin = child.stdin.take().expect("stdin");
    let mut send = |value: Value| {
        writeln!(stdin, "{value}").expect("write");
        stdin.flush().expect("flush");
    };
    let mut params = json!({
        "init_protocol": "accepted_then_settled",
        "init_id": "restored-customizer-tools",
        "mob_config": MOB_CONFIG,
        "has_roster_provider": true,
        "has_agent_customizer": true,
        "runtime_options": { "demo_llm": true },
    });
    if let Some(state) = state {
        params["persistent_state"] = json!(state);
    }
    send(json!({"jsonrpc": "2.0", "id": "init", "method": "meta/init", "params": params}));

    let started = Instant::now();
    let mut seen = Vec::new();
    let mut customizer_builds = 0;
    let outcome = loop {
        let line = match lines.recv_timeout(BACKSTOP.saturating_sub(started.elapsed())) {
            Ok(line) => line,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                eprint!(
                    "{}",
                    std::fs::read_to_string(&stderr_path).unwrap_or_default()
                );
                panic!("the init did not settle ({error:?}); seen {seen:?}");
            }
        };
        // The test harness of the host process may print before the first
        // message on the same line.
        let Some(message) = line
            .find('{')
            .and_then(|start| serde_json::from_str::<Value>(line[start..].trim()).ok())
        else {
            continue;
        };
        if let (Some(method), Some(id)) = (
            message.get("method").and_then(Value::as_str),
            message.get("id").cloned(),
        ) {
            if method == "callback/agent_customizer/customize_build" {
                customizer_builds += 1;
            }
            send(match answer_callback(method, &message["params"]) {
                Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
                Err(error) => json!({ "jsonrpc": "2.0", "id": id,
                    "error": { "code": -32601, "message": error } }),
            });
        }
        if message.get("method").and_then(Value::as_str) == Some("meta/init_settled") {
            break message["params"]["outcome"]
                .as_str()
                .unwrap_or_default()
                .to_string();
        }
        seen.push(message);
    };
    // The host's Ready probe runs after the publication.
    let deadline = Instant::now() + BACKSTOP;
    while outcome == "ready"
        && !std::fs::read_to_string(&record)
            .unwrap_or_default()
            .contains("ready held=")
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(20));
    }
    match ending {
        Ending::Kill => {
            let _ = child.kill();
            let _ = child.wait();
        }
        Ending::Close => {
            drop(stdin);
            let deadline = Instant::now() + BACKSTOP;
            while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(50));
            }
            if child.try_wait().unwrap().is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
    }
    // The host process's log rides this test's own output.
    eprint!(
        "{}",
        std::fs::read_to_string(&stderr_path).unwrap_or_default()
    );
    let recorded = std::fs::read_to_string(&record).unwrap_or_default();
    Boot {
        lines: recorded.lines().map(str::to_string).collect(),
        outcome,
        customizer_builds,
    }
}

/// A persistent gateway killed while its mob ran left the mob's log Running,
/// so the next boot's mob build revives the member inside meerkat's own
/// resume, before the identity bootstrap publishes its customizer tools.
/// Meerkat holds the revived member's run starts when the gateway binds the
/// runtime; the identity bootstrap publishes the tools (one SDK customizer
/// build) and releases the hold before Ready.
#[test]
fn a_member_restored_at_a_persistent_gateway_bootstrap_holds_its_runs_until_its_tools_publish() {
    serve_if_host_process();
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");

    let first = boot(
        PERSISTENT_TEST,
        &dir.path().join("boot-1"),
        Some(&state),
        Ending::Kill,
    );
    assert_eq!(first.outcome, "ready", "the first boot settles ready");
    assert!(
        first.customizer_builds >= 1,
        "the first boot builds the roster member through the SDK customizer"
    );
    assert_eq!(
        first.line("ready held="),
        "ready held=[Some(false)]",
        "the first boot's member runs unheld"
    );

    let second = boot(
        PERSISTENT_TEST,
        &dir.path().join("boot-2"),
        Some(&state),
        Ending::Close,
    );
    assert_eq!(second.outcome, "ready", "the restart settles ready");
    assert_eq!(
        second.line("bind held="),
        "bind held=[Some(true)]",
        "the member meerkat revived during the mob build holds its run starts until its \
         customizer tools publish"
    );
    assert!(
        second.customizer_builds >= 1,
        "the identity bootstrap publishes the member's tools through the SDK customizer"
    );
    assert_eq!(
        second.line("ready held="),
        "ready held=[Some(false)]",
        "the publication released the hold before Ready"
    );
}

/// A process-local gateway keeps its mob in memory, so its bootstrap
/// restores no member: nothing is held when it binds the runtime, and its
/// member, built by the identity bootstrap with its tools, runs unheld.
#[test]
fn a_process_local_gateway_bootstrap_holds_no_member() {
    serve_if_host_process();
    let dir = tempfile::tempdir().unwrap();
    let run = boot(
        PROCESS_LOCAL_TEST,
        &dir.path().join("boot"),
        None,
        Ending::Close,
    );
    assert_eq!(run.outcome, "ready", "the boot settles ready");
    assert_eq!(
        run.line("bind held="),
        "bind held=[]",
        "the bootstrap restored no member"
    );
    assert!(
        run.customizer_builds >= 1,
        "the identity bootstrap builds the member through the SDK customizer"
    );
    assert_eq!(
        run.line("ready held="),
        "ready held=[Some(false)]",
        "the member runs unheld"
    );
}
