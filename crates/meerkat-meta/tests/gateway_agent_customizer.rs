//! The composed gateway offers its host the agent customizer it composed, once,
//! on the identity-first path, before the identity context is installed. The
//! host's answer is the one customizer the gateway installs: the identity-first
//! context's, which builds the roster bootstrap's members, and the identity
//! runtime's, which builds a reset's fresh life, are the same `Arc`, and a host
//! wrapper sees the dispatcher the SDK customizer's tools were built into. A
//! host without the hook keeps the gateway's own composition. A refusal, or an
//! answer that adds or drops a customizer, fails init before the identity
//! bootstrap, and a gateway without an identity roster never asks. Each case is
//! a separate gateway process over real stdio, answering the SDK callbacks
//! itself.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use meerkat_meta::UnifiedRuntime;
use meerkat_meta::identity_first::{
    AgentBuildContext, AgentBuildDraft, AgentCustomizer, AgentIdentity, CustomizerError,
    DurableAgentSpec,
};
use meerkat_meta::mob_handle_runtime::SessionCreatedContext;
use meerkat_meta::rpc_gateway::{
    GatewayBootstrapReady, GatewayHostCloseReason, GatewayHostComposition, GatewayHostError,
};
use meerkat_runtime::meerkat_machine::NativeGrantWorkConfiguration;
use serde_json::{Value, json};

/// Set (to the case's mode) only in the gateway host process; `RECORD` names
/// its record file.
const HOST_PROCESS: &str = "META_GATEWAY_AGENT_CUSTOMIZER_HOST";
const RECORD: &str = "META_GATEWAY_AGENT_CUSTOMIZER_RECORD";
const BACKSTOP: Duration = Duration::from_mins(3);
const MOB_CONFIG: &str =
    "[mob]\nid = \"gateway-agent-customizer\"\n\n[profiles.worker]\nmodel = \"gpt-5.5\"\n";
/// The one roster member, and the tool the SDK customizer gives it.
const IDENTITY: &str = "keeper";
const TOOL: &str = "lookup";
/// The record line the driver writes once the init settled ready, before it
/// resets the member.
const SETTLED: &str = "driver settled ready";
/// Init phases of the identity roster and its bootstrap; a refused
/// composition reaches none of them.
const IDENTITY_PHASES: [&str; 4] = ["roster", "register_owners", "activate", "restore"];
const WRAP_TEST: &str = "a_host_wrapper_is_the_one_customizer_on_both_build_paths";
const DEFAULT_TEST: &str = "a_host_without_the_hook_keeps_the_composed_customizer";
const REFUSE_TEST: &str = "a_refused_customizer_composition_fails_init_closed";
const SHAPE_TEST: &str = "a_host_can_neither_add_nor_drop_the_agent_customizer";

fn append(record: &Path, line: &str) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(record)
        .expect("record");
    writeln!(file, "{line}").expect("append");
}

/// A host wrapper around the gateway's composed customizer. It records each
/// build with its own address and the tools of the dispatcher the wrapped
/// customizer built, and changes nothing.
struct Wrapper {
    inner: Arc<dyn AgentCustomizer>,
    record: PathBuf,
}

#[async_trait::async_trait]
impl AgentCustomizer for Wrapper {
    async fn customize_build(
        &self,
        context: &AgentBuildContext,
        spec: &DurableAgentSpec,
        draft: &mut AgentBuildDraft,
    ) -> Result<(), CustomizerError> {
        let built = self.inner.customize_build(context, spec, draft).await;
        let tools = draft
            .local_external_tools
            .dispatcher()
            .map(|dispatcher| {
                dispatcher
                    .tools()
                    .iter()
                    .map(|tool| tool.name.as_str().to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        append(
            &self.record,
            &format!(
                "customize identity={} wrapper={:p} tools={tools}",
                context.identity, self
            ),
        );
        built
    }

    async fn after_create(
        &self,
        identity: &AgentIdentity,
        session_id: &meerkat_core::types::SessionId,
        context: &SessionCreatedContext,
    ) -> Result<(), CustomizerError> {
        self.inner.after_create(identity, session_id, context).await
    }
}

/// A customizer of the host's own.
struct HostCustomizer;

#[async_trait::async_trait]
impl AgentCustomizer for HostCustomizer {
    async fn customize_build(
        &self,
        _context: &AgentBuildContext,
        _spec: &DurableAgentSpec,
        _draft: &mut AgentBuildDraft,
    ) -> Result<(), CustomizerError> {
        Ok(())
    }
}

/// What a composing host answers when the gateway offers it the customizer.
#[derive(Clone, Copy)]
enum Answer {
    Wrap,
    Refuse,
    Add,
    Drop,
}

/// A host that answers the composition hook and records it, and what the
/// installed identity context holds at the identity bind.
struct ComposingHost {
    answer: Answer,
    record: PathBuf,
    composed: Mutex<Option<Arc<dyn AgentCustomizer>>>,
}

#[async_trait::async_trait]
impl GatewayHostComposition for ComposingHost {
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

    async fn bind_runtime(&self, _runtime: &mut UnifiedRuntime) -> Result<(), GatewayHostError> {
        Ok(())
    }

    fn compose_agent_customizer(
        &self,
        customizer: Option<Arc<dyn AgentCustomizer>>,
    ) -> Result<Option<Arc<dyn AgentCustomizer>>, GatewayHostError> {
        let given = if customizer.is_some() { "some" } else { "none" };
        match (self.answer, customizer) {
            (Answer::Wrap, Some(inner)) => {
                let wrapper = Arc::new(Wrapper {
                    inner,
                    record: self.record.clone(),
                });
                append(
                    &self.record,
                    &format!("compose given={given} wrapper={:p}", Arc::as_ptr(&wrapper)),
                );
                let wrapper: Arc<dyn AgentCustomizer> = wrapper;
                *self.composed.lock().unwrap() = Some(Arc::clone(&wrapper));
                Ok(Some(wrapper))
            }
            (Answer::Wrap, None) => {
                append(&self.record, &format!("compose given={given}"));
                Err(GatewayHostError::Unavailable)
            }
            (Answer::Refuse, _) => {
                append(&self.record, &format!("compose given={given}"));
                Err(GatewayHostError::Denied)
            }
            (Answer::Add, _) => {
                append(&self.record, &format!("compose given={given}"));
                Ok(Some(Arc::new(HostCustomizer)))
            }
            (Answer::Drop, _) => {
                append(&self.record, &format!("compose given={given}"));
                Ok(None)
            }
        }
    }

    async fn bind_identity_runtime(
        &self,
        runtime: &mut UnifiedRuntime,
    ) -> Result<(), GatewayHostError> {
        let installed = runtime
            .identity_first_context()
            .and_then(|context| context.customizer.clone());
        let composed = self.composed.lock().unwrap().clone();
        let held = match (installed, composed) {
            (Some(installed), Some(composed)) if Arc::ptr_eq(&installed, &composed) => "composed",
            (Some(_), _) => "other",
            (None, _) => "none",
        };
        append(&self.record, &format!("bind_identity customizer={held}"));
        Ok(())
    }

    fn publish_bootstrap_ready(
        &self,
        publication: GatewayBootstrapReady<'_>,
    ) -> Result<(), GatewayHostError> {
        publication.publish();
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

/// A host without the composition hook: the trait's default answers it.
struct PlainHost;

#[async_trait::async_trait]
impl GatewayHostComposition for PlainHost {
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

    async fn bind_runtime(&self, _runtime: &mut UnifiedRuntime) -> Result<(), GatewayHostError> {
        Ok(())
    }

    fn publish_bootstrap_ready(
        &self,
        publication: GatewayBootstrapReady<'_>,
    ) -> Result<(), GatewayHostError> {
        publication.publish();
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

/// In the gateway host process, run the gateway with the case's host, record
/// how the run returned, and exit.
fn serve_if_host_process() {
    let Some(mode) = std::env::var_os(HOST_PROCESS) else {
        return;
    };
    let record: PathBuf = std::env::var_os(RECORD).expect("record path").into();
    let composing = |answer| -> Arc<dyn GatewayHostComposition> {
        Arc::new(ComposingHost {
            answer,
            record: record.clone(),
            composed: Mutex::new(None),
        })
    };
    let host = match mode.to_str().expect("mode") {
        "plain" => Arc::new(PlainHost) as Arc<dyn GatewayHostComposition>,
        "wrap" => composing(Answer::Wrap),
        "refuse" => composing(Answer::Refuse),
        "add" => composing(Answer::Add),
        "drop" => composing(Answer::Drop),
        other => panic!("unknown host mode {other}"),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let returned = runtime.block_on(meerkat_meta::rpc_gateway::run_persistent_with_host(
        None, host,
    ));
    append(&record, &format!("returned ok={}", returned.is_ok()));
    std::process::exit(0);
}

/// One gateway host process, driven through its init and, when that settles
/// ready with an identity roster, a reset of the roster member.
struct Run {
    /// The record: the host's lines and the driver's [`SETTLED`] marker.
    lines: Vec<String>,
    /// Every message the gateway wrote.
    seen: Vec<Value>,
    /// The SDK customizer builds the gateway asked for before the init
    /// settled, and after it.
    bootstrap_builds: usize,
    later_builds: usize,
    /// The reset's response.
    reset: Option<Value>,
}

impl Run {
    fn settled(&self) -> &Value {
        let settled = self
            .seen
            .iter()
            .find(|message| {
                message.get("method").and_then(Value::as_str) == Some("meta/init_settled")
            })
            .unwrap_or_else(|| panic!("the init settles: {:?}", self.seen));
        &settled["params"]
    }

    fn outcome(&self) -> &str {
        self.settled()["outcome"].as_str().unwrap_or_default()
    }

    fn phases(&self) -> Vec<&str> {
        self.seen
            .iter()
            .filter(|m| m.get("method").and_then(Value::as_str) == Some("meta/init_progress"))
            .map(|m| m["params"]["phase"].as_str().unwrap_or_default())
            .collect()
    }

    fn starting(&self, prefix: &str) -> Vec<&str> {
        self.lines
            .iter()
            .map(String::as_str)
            .filter(|line| line.starts_with(prefix))
            .collect()
    }
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

/// Drive the `mode` host as a child process of `test`, with an identity roster
/// (`roster`) and an SDK customizer (`customizer`) registered at init.
fn drive(test: &str, mode: &str, roster: bool, customizer: bool) -> Run {
    let dir = tempfile::tempdir().unwrap();
    let record = dir.path().join("record");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env(HOST_PROCESS, mode)
        .env(RECORD, &record)
        .env("OPENAI_API_KEY", "sk-regression-test")
        .env("XDG_STATE_HOME", dir.path().join("xdg-state"))
        .current_dir(dir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
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
        "init_id": format!("agent-customizer-{mode}"),
        "mob_config": MOB_CONFIG,
        "persistent_state": dir.path().join("state"),
        "runtime_options": { "demo_llm": true },
    });
    if roster {
        params["has_roster_provider"] = json!(true);
    }
    if customizer {
        params["has_agent_customizer"] = json!(true);
    }
    send(json!({"jsonrpc": "2.0", "id": "init", "method": "meta/init", "params": params}));

    let started = Instant::now();
    let mut seen = Vec::new();
    let mut settled = false;
    let mut bootstrap_builds = 0;
    let mut later_builds = 0;
    let mut reset = None;
    loop {
        let remaining = BACKSTOP.saturating_sub(started.elapsed());
        let line = match lines.recv_timeout(remaining) {
            Ok(line) => line,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("{mode}: the gateway ended before the run did; seen {seen:?}")
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("{mode}: the run did not finish; seen {seen:?}")
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
                if settled {
                    later_builds += 1;
                } else {
                    bootstrap_builds += 1;
                }
            }
            send(match answer_callback(method, &message["params"]) {
                Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
                Err(error) => json!({ "jsonrpc": "2.0", "id": id,
                    "error": { "code": -32601, "message": error } }),
            });
        }
        if message.get("method").and_then(Value::as_str) == Some("meta/init_settled") {
            settled = true;
            let ready = message["params"]["outcome"] == "ready";
            seen.push(message);
            if ready && roster {
                append(&record, SETTLED);
                send(json!({
                    "jsonrpc": "2.0",
                    "id": "reset",
                    "method": "meta/reset",
                    "params": { "identity": IDENTITY },
                }));
                continue;
            }
            break;
        }
        if message.get("method").is_none() && message["id"] == "reset" {
            reset = Some(message.clone());
            seen.push(message);
            break;
        }
        seen.push(message);
    }
    drop(stdin);
    let deadline = Instant::now() + BACKSTOP;
    while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    let exited = child.try_wait().unwrap().is_some();
    if !exited {
        let _ = child.kill();
    }
    let _ = child.wait();
    assert!(exited, "{mode}: the gateway ends after its run");
    let recorded = std::fs::read_to_string(&record).unwrap_or_default();
    Run {
        lines: recorded.lines().map(str::to_string).collect(),
        seen,
        bootstrap_builds,
        later_builds,
        reset,
    }
}

/// The reset answered with the member's fresh session.
fn assert_reset(run: &Run) {
    let reset = run.reset.as_ref().expect("the member was reset");
    assert!(
        reset["result"]["session_id"].is_string(),
        "the reset succeeds: {reset}"
    );
}

/// The wrapper the host returned is the customizer of both build paths: the
/// identity context holds that very `Arc` at the identity bind, the roster
/// bootstrap (the context's customizer) builds the member through it, and so
/// does the reset after Ready (the identity runtime's customizer). Every SDK
/// customizer build ran inside it, and it saw the dispatcher the SDK
/// customizer's tool was built into.
#[test]
fn a_host_wrapper_is_the_one_customizer_on_both_build_paths() {
    serve_if_host_process();
    let run = drive(WRAP_TEST, "wrap", true, true);
    assert_eq!(run.outcome(), "ready", "{:?}", run.seen);
    let composes = run.starting("compose ");
    assert_eq!(composes.len(), 1, "the host is asked once: {:?}", run.lines);
    let wrapper = composes[0]
        .strip_prefix("compose given=some wrapper=")
        .unwrap_or_else(|| panic!("the host wrapped the composed customizer: {:?}", run.lines));
    assert!(
        run.lines
            .iter()
            .any(|line| line == "bind_identity customizer=composed"),
        "the identity context holds the host's Arc: {:?}",
        run.lines
    );
    let marker = run
        .lines
        .iter()
        .position(|line| line == SETTLED)
        .unwrap_or_else(|| panic!("the init settled ready: {:?}", run.lines));
    let build = format!("customize identity={IDENTITY} wrapper={wrapper} tools={TOOL}");
    let builds: Vec<usize> = run
        .lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line.starts_with("customize "))
        .map(|(at, line)| {
            assert_eq!(line, &build, "{:?}", run.lines);
            at
        })
        .collect();
    assert!(
        builds.iter().any(|at| *at < marker),
        "the roster bootstrap built through the wrapper: {:?}",
        run.lines
    );
    assert!(
        builds.iter().any(|at| *at > marker),
        "the reset built through the wrapper: {:?}",
        run.lines
    );
    assert!(
        run.bootstrap_builds > 0 && run.later_builds > 0,
        "the SDK customizer built at the bootstrap and at the reset: {:?}",
        run.seen
    );
    assert_eq!(
        builds.len(),
        run.bootstrap_builds + run.later_builds,
        "every SDK customizer build ran inside the wrapper: {:?}",
        run.lines
    );
    assert_reset(&run);
}

/// The trait's default answers with the given customizer itself, and a host
/// without the hook keeps the gateway's composition: the SDK customizer builds
/// the member at the roster bootstrap and at the reset.
#[test]
fn a_host_without_the_hook_keeps_the_composed_customizer() {
    serve_if_host_process();
    let given: Arc<dyn AgentCustomizer> = Arc::new(HostCustomizer);
    let answer = PlainHost
        .compose_agent_customizer(Some(Arc::clone(&given)))
        .expect("the default accepts")
        .expect("the default keeps the customizer");
    assert!(Arc::ptr_eq(&given, &answer), "the very same Arc");
    assert!(
        PlainHost
            .compose_agent_customizer(None)
            .expect("the default accepts")
            .is_none()
    );

    let run = drive(DEFAULT_TEST, "plain", true, true);
    assert_eq!(run.outcome(), "ready", "{:?}", run.seen);
    assert!(run.bootstrap_builds > 0, "{:?}", run.seen);
    assert!(run.later_builds > 0, "{:?}", run.seen);
    assert_reset(&run);
}

/// A host error fails init closed, with the host's own error, before the
/// identity context is installed or bootstrapped: no identity bind, no
/// identity phase, no member build. A gateway without an identity roster
/// never asks, so the same host starts it.
#[test]
fn a_refused_customizer_composition_fails_init_closed() {
    serve_if_host_process();
    let run = drive(REFUSE_TEST, "refuse", true, true);
    assert_eq!(run.outcome(), "failed", "{:?}", run.seen);
    assert!(
        run.settled()
            .to_string()
            .contains(&GatewayHostError::Denied.to_string()),
        "the init failure is the host's own error: {}",
        run.settled()
    );
    assert_eq!(run.lines, ["compose given=some", "returned ok=false"]);
    let reached: Vec<&str> = run
        .phases()
        .into_iter()
        .filter(|phase| IDENTITY_PHASES.contains(phase))
        .collect();
    assert!(reached.is_empty(), "no identity phase: {reached:?}");
    assert_eq!(run.bootstrap_builds, 0, "no member build");

    let run = drive(REFUSE_TEST, "refuse", false, true);
    assert_eq!(run.outcome(), "ready", "{:?}", run.seen);
    assert!(run.starting("compose ").is_empty(), "{:?}", run.lines);
}

/// Whether a customizer runs is settled before the host is asked: a host that
/// answers a customizer where the gateway composed none, or none where it
/// composed one, is refused like a host error.
#[test]
fn a_host_can_neither_add_nor_drop_the_agent_customizer() {
    serve_if_host_process();
    for (mode, customizer, given, refusal) in [
        (
            "add",
            false,
            "compose given=none",
            "added an agent customizer",
        ),
        (
            "drop",
            true,
            "compose given=some",
            "dropped the gateway's agent customizer",
        ),
    ] {
        let run = drive(SHAPE_TEST, mode, true, customizer);
        assert_eq!(run.outcome(), "failed", "{mode}: {:?}", run.seen);
        assert!(
            run.settled().to_string().contains(refusal),
            "{mode}: {}",
            run.settled()
        );
        assert_eq!(run.lines, [given, "returned ok=false"], "{mode}");
        assert_eq!(run.bootstrap_builds, 0, "{mode}: no member build");
    }
}
