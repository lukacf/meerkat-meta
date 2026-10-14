#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::uninlined_format_args
)]

//! M2 `MetaStorageLayout` boot coverage: the dual-name fixture matrix per
//! surface. Every surface (builder, `rpc_gateway`, `meta_gateway`) must
//! keep booting against a legacy-named state directory (files used where
//! they lie — the resolver never creates a twin), converge fresh
//! directories on the canonical spellings, and refuse loudly when both
//! spellings of one store exist.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use async_trait::async_trait;
use meerkat_meta::identity_first::contracts::RosterProvider;
use meerkat_meta::identity_first::{DurableAgentSpec, RosterContext, RosterError};
use meerkat_meta::unified_runtime::UnifiedRuntimeBuilder;
use serde_json::{Value, json};
use tempfile::TempDir;

/// Per-test mob id counter: 0.8.23's fail-closed in-proc registration
/// means concurrently running tests must not share a supervisor route.
static NEXT_TEST_MOB_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn test_definition() -> meerkat_mob::MobDefinition {
    meerkat_mob::MobDefinition::from_toml(&format!(
        r#"
[mob]
id = "storage-layout-boot-test-{}"

[profiles.default]
model = "gpt-5.5"
"#,
        NEXT_TEST_MOB_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
    .expect("parse test mob definition")
}

fn touch(path: &Path) {
    std::fs::write(path, b"").expect("seed fixture file");
}

/// An empty desired roster: enough to drive the identity-first arm (and its
/// continuity-store open) without materializing any member.
struct EmptyRosterProvider;

#[async_trait]
impl RosterProvider for EmptyRosterProvider {
    async fn roster(&self, _context: &RosterContext) -> Result<Vec<DurableAgentSpec>, RosterError> {
        Ok(Vec::new())
    }
}

// ---------------------------------------------------------------------------
// Builder (library surface)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn builder_fresh_state_dir_converges_on_canonical_names() {
    let tmp = TempDir::new().expect("temp dir");
    let state = tmp.path().join("state");

    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .persistent_state(&state)
            .roster_provider(Arc::new(EmptyRosterProvider))
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("fresh persistent build");

    for canonical in [
        "sessions.sqlite3",
        "runtime.sqlite",
        "continuity.sqlite3",
        "meta_metadata.sqlite3",
        "meta_console.sqlite3",
    ] {
        assert!(
            state.join(canonical).exists(),
            "fresh boot must create canonical {canonical}"
        );
    }
    for legacy in [
        "sessions.db",
        "sessions.sqlite",
        "continuity.db",
        "identity_continuity.sqlite",
    ] {
        assert!(
            !state.join(legacy).exists(),
            "fresh boot must not create legacy {legacy}"
        );
    }

    runtime.mob_handle().stop().await.expect("stop");
}

#[tokio::test]
async fn builder_keeps_using_a_legacy_named_state_dir_where_it_lies() {
    let tmp = TempDir::new().expect("temp dir");
    let state = tmp.path().join("state");
    std::fs::create_dir_all(&state).expect("state dir");
    // The builder's own historical spellings (recon census): SQLite opens
    // the zero-byte seeds as empty databases and initializes them in place.
    touch(&state.join("sessions.db"));
    touch(&state.join("identity_continuity.sqlite"));

    let runtime = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .persistent_state(&state)
            .roster_provider(Arc::new(EmptyRosterProvider))
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .expect("legacy-named persistent build");

    for (legacy, canonical) in [
        ("sessions.db", "sessions.sqlite3"),
        ("identity_continuity.sqlite", "continuity.sqlite3"),
    ] {
        assert!(
            state.join(legacy).exists(),
            "legacy {legacy} must keep being used where it lies"
        );
        assert!(
            !state.join(canonical).exists(),
            "boot must not create a canonical twin {canonical} beside {legacy}"
        );
        assert!(
            std::fs::metadata(state.join(legacy))
                .expect("metadata")
                .len()
                > 0,
            "legacy {legacy} must have been opened and initialized in place"
        );
    }

    runtime.mob_handle().stop().await.expect("stop");
}

#[tokio::test]
async fn builder_refuses_session_file_name_twins() {
    let tmp = TempDir::new().expect("temp dir");
    let state = tmp.path().join("state");
    std::fs::create_dir_all(&state).expect("state dir");
    touch(&state.join("sessions.db"));
    touch(&state.join("sessions.sqlite3"));

    let error = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .persistent_state(&state)
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .err()
    .expect("twin spellings must refuse to boot");
    let message = error.to_string();
    assert!(
        message.contains("file-name twins") && message.contains("sessions"),
        "twins refusal must name the slot and point at the doctor: {message}"
    );
}

/// The predecessor (pre-0.9) metadata and console file names are not
/// legacy spellings: the builder refuses typed instead of starting renamed
/// stores empty beside them, and names the one-shot adoption verb.
#[tokio::test]
async fn builder_refuses_unadopted_predecessor_state() {
    let tmp = TempDir::new().expect("temp dir");
    let state = tmp.path().join("state");
    std::fs::create_dir_all(&state).expect("state dir");
    let predecessor =
        state.join(meerkat_meta::storage_predecessor::PREDECESSOR_METADATA_DB_FILE_NAMES[0]);
    touch(&predecessor);

    let error = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .persistent_state(&state)
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .err()
    .expect("predecessor state must refuse to boot");
    let message = error.to_string();
    assert!(
        message.contains("predecessor") && message.contains("storage-adopt-predecessor"),
        "predecessor refusal must name the adoption verb: {message}"
    );
    assert!(
        message.contains(&format!("--state-dir {} --apply", state.display())),
        "the refusal must name the exact state directory to adopt: {message}"
    );
    assert!(
        !state.join("meta_metadata.sqlite3").exists(),
        "a refused boot must not create the renamed store beside the predecessor"
    );
}

/// The predecessor memory ledger file alone refuses the builder too, with
/// no memory backend configured: detection never depends on memory being
/// enabled, and a refused boot leaves the file as it was.
#[tokio::test]
async fn builder_refuses_an_unadopted_predecessor_memory_ledger() {
    let tmp = TempDir::new().expect("temp dir");
    let state = tmp.path().join("state");
    std::fs::create_dir_all(&state).expect("state dir");
    let ledger = state.join(meerkat_meta::storage_predecessor::PREDECESSOR_MEMORY_LEDGER_FILE_NAME);
    let bytes = b"{\"assertions\": [], \"conflicts\": []}";
    std::fs::write(&ledger, bytes).expect("seed the predecessor ledger");

    let error = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .persistent_state(&state)
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .err()
    .expect("a predecessor ledger must refuse to boot");
    let message = error.to_string();
    assert!(
        message.contains(&ledger.display().to_string())
            && message.contains(&format!("--state-dir {} --apply", state.display())),
        "the refusal must name the ledger and the exact adoption command: {message}"
    );
    assert_eq!(std::fs::read(&ledger).expect("predecessor ledger"), bytes);
    assert!(
        !state
            .join(meerkat_meta::storage_layout::MEMORY_LEDGER_FILE_NAME)
            .exists()
    );
    assert!(!state.join("meta_metadata.sqlite3").exists());
}

// ---------------------------------------------------------------------------
// rpc_gateway (SDK stdio surface)
// ---------------------------------------------------------------------------

/// Per-call mob id: 0.8.23's fail-closed in-proc registration means
/// concurrently running tests must not share a supervisor route.
fn rpc_mob_config() -> String {
    format!(
        r#"
[mob]
id = "storage-layout-rpc-test-{}"

[profiles.default]
model = "gpt-5.5"
"#,
        NEXT_TEST_MOB_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

/// Spawn the real `rpc_gateway --persistent`, send one `meta/init` with
/// `persistent_state`, and return the init response.
fn rpc_gateway_init(state_dir: &Path) -> Value {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rpc_gateway"))
        .arg("--persistent")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn rpc_gateway");
    let mut stdin = child.stdin.take().expect("gateway stdin");
    let init = json!({
        "jsonrpc": "2.0",
        "id": "init",
        "method": "meta/init",
        "params": {
            "persistent_state": state_dir,
            "mob_config": &rpc_mob_config(),
        }
    });
    writeln!(
        stdin,
        "{}",
        serde_json::to_string(&init).expect("init json")
    )
    .expect("write init");
    stdin.flush().expect("flush init");

    let stdout = child.stdout.take().expect("gateway stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let _ = reader.read_line(&mut line);
        let _ = tx.send(line);
    });
    let line = rx
        .recv_timeout(Duration::from_secs(90))
        .expect("rpc_gateway did not answer meta/init");
    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
    serde_json::from_str(line.trim()).expect("init response json")
}

#[test]
fn rpc_gateway_fresh_state_dir_converges_on_canonical_names() {
    let state = TempDir::new().expect("state dir");
    let response = rpc_gateway_init(state.path());
    assert!(
        response.get("error").is_none(),
        "fresh init must succeed: {response}"
    );
    for canonical in [
        "sessions.sqlite3",
        "runtime.sqlite",
        "meta_metadata.sqlite3",
        "meta_console.sqlite3",
    ] {
        assert!(
            state.path().join(canonical).exists(),
            "fresh boot must create canonical {canonical}"
        );
    }
    for legacy in ["sessions.db", "sessions.sqlite"] {
        assert!(
            !state.path().join(legacy).exists(),
            "fresh boot must not create legacy {legacy}"
        );
    }
}

#[test]
fn rpc_gateway_keeps_using_a_legacy_named_state_dir_where_it_lies() {
    let state = TempDir::new().expect("state dir");
    // rpc_gateway's own historical spellings (recon census).
    touch(&state.path().join("sessions.db"));

    let response = rpc_gateway_init(state.path());
    assert!(
        response.get("error").is_none(),
        "legacy-named init must succeed: {response}"
    );
    assert!(
        state.path().join("sessions.db").exists(),
        "legacy sessions.db must keep being used where it lies"
    );
    assert!(
        !state.path().join("sessions.sqlite3").exists(),
        "boot must not create a canonical twin sessions.sqlite3 beside sessions.db"
    );
}

#[test]
fn rpc_gateway_refuses_session_file_name_twins() {
    let state = TempDir::new().expect("state dir");
    touch(&state.path().join("sessions.db"));
    touch(&state.path().join("sessions.sqlite3"));

    let response = rpc_gateway_init(state.path());
    let message = response["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("file-name twins") && message.contains("sessions"),
        "twin spellings must fail init with the twins refusal: {response}"
    );
}

#[test]
fn rpc_gateway_refuses_unadopted_predecessor_state() {
    let state = TempDir::new().expect("state dir");
    touch(
        &state
            .path()
            .join(meerkat_meta::storage_predecessor::PREDECESSOR_CONSOLE_DB_FILE_NAMES[0]),
    );

    let response = rpc_gateway_init(state.path());
    let message = response["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("predecessor") && message.contains("storage-adopt-predecessor"),
        "predecessor state must fail init with the adoption pointer: {response}"
    );
    assert!(
        message.contains(&format!("--state-dir {} --apply", state.path().display())),
        "the pointer must name the gateway's persistent_state: {response}"
    );
    assert!(!state.path().join("meta_console.sqlite3").exists());
}

// ---------------------------------------------------------------------------
// meta_gateway (console/HTTP surface)
// ---------------------------------------------------------------------------

/// Spawn the real `meta_gateway`, send one `meta/init` pointed at
/// `store_dir` with `persistent_sessions: true`, and return the init
/// response. XDG state is isolated per test (shared-home CI flake class).
fn meta_gateway_init(workspace: &Path, store_dir: &Path) -> Value {
    let init = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "meta/init",
        "params": {
            "workspace_root": workspace,
            "store_path": store_dir,
            "persistent_sessions": true,
        },
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_meta_gateway"))
        .current_dir(workspace)
        // Dummy provider secrets: the fallback mob's LLM client is created
        // at bootstrap but never called (mirrors meta_gateway_bootstrap).
        .env("ANTHROPIC_API_KEY", "sk-ant-regression-test")
        .env("OPENAI_API_KEY", "sk-regression-test")
        .env("XDG_STATE_HOME", workspace.join("xdg-state"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn meta_gateway");

    let mut stdin = child.stdin.take().expect("stdin");
    writeln!(
        stdin,
        "{}",
        serde_json::to_string(&init).expect("init json")
    )
    .expect("write init");
    stdin.flush().expect("flush");

    let stdout = child.stdout.take().expect("stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let _ = reader.read_line(&mut line);
        let _ = tx.send(line);
    });
    let line = rx
        .recv_timeout(Duration::from_secs(90))
        .expect("meta_gateway did not answer meta/init");
    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
    serde_json::from_str(line.trim()).expect("init response json")
}

#[test]
fn meta_gateway_fresh_store_dir_converges_on_canonical_names() {
    let workspace = TempDir::new().expect("workspace");
    let store = TempDir::new().expect("store");
    let store_dir = store.path().join("store");

    let response = meta_gateway_init(workspace.path(), &store_dir);
    assert!(
        response.get("error").is_none(),
        "fresh init must succeed: {response}"
    );
    for canonical in ["sessions.sqlite3", "runtime.sqlite", "continuity.sqlite3"] {
        assert!(
            store_dir.join(canonical).exists(),
            "fresh boot must create canonical {canonical}"
        );
    }
    for legacy in ["sessions.sqlite", "sessions.db", "continuity.db"] {
        assert!(
            !store_dir.join(legacy).exists(),
            "fresh boot must not create legacy {legacy}"
        );
    }
}

#[test]
fn meta_gateway_keeps_using_a_legacy_named_store_dir_where_it_lies() {
    let workspace = TempDir::new().expect("workspace");
    let store = TempDir::new().expect("store");
    let store_dir = store.path().join("store");
    std::fs::create_dir_all(&store_dir).expect("store dir");
    // meta_gateway's own historical spellings (recon census).
    touch(&store_dir.join("sessions.sqlite"));
    touch(&store_dir.join("continuity.db"));

    let response = meta_gateway_init(workspace.path(), &store_dir);
    assert!(
        response.get("error").is_none(),
        "legacy-named init must succeed: {response}"
    );
    for (legacy, canonical) in [
        ("sessions.sqlite", "sessions.sqlite3"),
        ("continuity.db", "continuity.sqlite3"),
    ] {
        assert!(
            store_dir.join(legacy).exists(),
            "legacy {legacy} must keep being used where it lies"
        );
        assert!(
            !store_dir.join(canonical).exists(),
            "boot must not create a canonical twin {canonical} beside {legacy}"
        );
    }
}

/// A gateway home that only exists under its predecessor (pre-0.9) name
/// refuses init instead of minting a new peer identity.
#[test]
fn meta_gateway_refuses_unadopted_predecessor_gateway_home() {
    let workspace = TempDir::new().expect("workspace");
    let store = TempDir::new().expect("store");
    let predecessor_home = workspace
        .path()
        .join("xdg-state")
        .join(meerkat_meta::storage_predecessor::PREDECESSOR_GATEWAY_HOME_DIR_NAME);
    std::fs::create_dir_all(&predecessor_home).expect("predecessor home");
    touch(&predecessor_home.join("peer_key.ed25519"));

    let response = meta_gateway_init(workspace.path(), &store.path().join("store"));
    let message = response["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("predecessor") && message.contains("storage-adopt-predecessor"),
        "predecessor home must fail init with the adoption pointer: {response}"
    );
    assert!(
        !workspace
            .path()
            .join("xdg-state")
            .join("meerkat-meta")
            .exists(),
        "a refused boot must not mint the renamed gateway home"
    );
}

/// A predecessor environment variable is configuration the renamed binary
/// would silently ignore; the gateway refuses to start instead.
#[test]
fn rpc_gateway_refuses_predecessor_environment() {
    let variable = meerkat_meta::storage_predecessor::PREDECESSOR_ENV_VARS
        .iter()
        .find(|name| name.ends_with("HTTP_LISTEN_ADDR"))
        .expect("the predecessor listen variable")
        .to_string();
    let output = Command::new(env!("CARGO_BIN_EXE_rpc_gateway"))
        .arg("--persistent")
        .env(&variable, "127.0.0.1:0")
        .stdin(Stdio::null())
        .output()
        .expect("run rpc_gateway");
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(&variable), "{stderr}");
}
