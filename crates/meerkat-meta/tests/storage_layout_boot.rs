#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::uninlined_format_args
)]

//! M2 `MetaStorageLayout` boot coverage: the dual-name fixture matrix per
//! surface. Every surface (builder, `rpc_gateway`, `meta_gateway`) must
//! keep booting against a legacy-named state directory (files used where
//! they lie, the resolver never creates a twin) and converge fresh
//! directories on the canonical spellings.
//!
//! Sessions live in Core's pair on `runtime.sqlite`. No surface creates a
//! sessions database, so a directory a fresh boot leaves starts again. A
//! sessions database in any spelling, file-name twins included, is the
//! 0.9.3 session projection: every surface refuses it typed before any
//! store opens, names `meta_gateway storage-migrate-native`, and leaves it
//! where it lies.
//!
//! The stdio gateway's boot-time member route is covered here too: the
//! native realm a launch's members read their configuration from
//! (`runtime_options.native_realm`) and the auth binding they build with
//! (`runtime_options.member_auth_binding`), resolved at boot or refused
//! typed.

use std::io::{BufRead, BufReader, Read, Write};
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

/// No surface creates a sessions database in any spelling: sessions live in
/// Core's pair on `runtime.sqlite`.
fn assert_no_sessions_database(state_dir: &Path) {
    let slot = meerkat_meta::storage_layout::DatabaseSlot::Sessions;
    for spelling in
        std::iter::once(slot.canonical_name()).chain(slot.legacy_names().iter().copied())
    {
        assert!(
            !state_dir.join(spelling).exists(),
            "no surface may create a separate sessions database, found {spelling}"
        );
    }
}

/// The refusal a sessions database gets: the directory must be migrated,
/// the message names the native migration verb and every file it found.
fn assert_native_migration_refusal(message: &str, files: &[&str]) {
    assert!(
        message.contains("must be migrated before this version opens it")
            && message.contains(meerkat_meta::storage_layout::NATIVE_STORAGE_MIGRATION_VERB),
        "a sessions database must refuse with the native migration: {message}"
    );
    for file in files {
        assert!(
            message.contains(file),
            "the refusal must name {file}: {message}"
        );
    }
}

/// A refused boot opens no store: every seeded file is still empty and
/// Core's `runtime.sqlite` was never created.
fn assert_refused_boot_left_the_directory(state_dir: &Path, seeded: &[&str]) {
    for file in seeded {
        assert_eq!(
            std::fs::metadata(state_dir.join(file))
                .expect("seeded file")
                .len(),
            0,
            "a refused boot must leave {file} as it was"
        );
    }
    assert!(
        !state_dir.join("runtime.sqlite").exists(),
        "a refused boot must not open the runtime store"
    );
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
    assert_no_sessions_database(&state);
    for legacy in ["continuity.db", "identity_continuity.sqlite"] {
        assert!(
            !state.join(legacy).exists(),
            "fresh boot must not create legacy {legacy}"
        );
    }

    runtime.mob_handle().stop().await.expect("stop");
}

/// The second start of a directory a fresh boot left: it holds no sessions
/// database, so after a clean shutdown it reads as native and the same mob
/// builds on it again.
#[tokio::test]
async fn builder_starts_a_fresh_state_dir_again_after_a_clean_shutdown() {
    let tmp = TempDir::new().expect("temp dir");
    let state = tmp.path().join("state");
    // One mob throughout: the directory's topology state belongs to it.
    let definition = test_definition();
    let build = || {
        Box::pin(
            UnifiedRuntimeBuilder::default()
                .definition(definition.clone())
                .persistent_state(&state)
                .roster_provider(Arc::new(EmptyRosterProvider))
                .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
                .build(),
        )
    };

    let first = build()
        .await
        .expect("first boot of a fresh state directory");
    first.shutdown().await;
    drop(first);
    assert_no_sessions_database(&state);
    assert_eq!(
        meerkat_meta::storage_migrate_native::native_layout_status(&state),
        meerkat_meta::storage_migrate_native::NativeLayoutStatus::Native,
        "the directory a fresh boot leaves must read as native"
    );

    let again = build()
        .await
        .expect("a directory this version created must start again");
    assert_no_sessions_database(&state);
    again.shutdown().await;
}

/// One Meta runtime per state directory: a second builder on a directory a
/// live runtime holds is refused typed before it opens any store. A clean
/// shutdown releases the directory, and once the first runtime is gone the
/// same mob builds on it again.
#[tokio::test]
async fn builder_refuses_a_state_directory_another_runtime_holds() {
    let tmp = TempDir::new().expect("temp dir");
    let state = tmp.path().join("state");
    // One mob throughout: the directory's topology state belongs to it.
    let definition = test_definition();
    let build = || {
        Box::pin(
            UnifiedRuntimeBuilder::default()
                .definition(definition.clone())
                .persistent_state(&state)
                .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
                .build(),
        )
    };
    let first = build().await.expect("first runtime");
    let Err(refused) = build().await else {
        panic!("a second runtime on a held state directory must be refused");
    };
    assert!(
        matches!(
            refused,
            meerkat_meta::UnifiedRuntimeBuilderError::StorageLayout(
                meerkat_meta::storage_layout::StorageLayoutError::StateDirInUse { .. }
            )
        ),
        "{refused}"
    );
    assert!(
        refused
            .to_string()
            .ends_with("is in use by another Meta runtime"),
        "{refused}"
    );
    first.shutdown().await;
    drop(
        meerkat_meta::state_dir_claim::StateDirClaim::acquire(&state)
            .expect("a clean shutdown releases the state directory"),
    );
    drop(first);
    let again = build()
        .await
        .expect("the same mob builds on the directory once the first runtime is gone");
    again.shutdown().await;
}

/// The builder claims its state directory only once the configuration is
/// coherent: a configuration it refuses leaves the directory untouched.
#[tokio::test]
async fn a_refused_builder_configuration_leaves_the_state_directory_untouched() {
    let tmp = TempDir::new().expect("temp dir");
    let state = tmp.path().join("state");
    let Err(refused) = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .persistent_state(&state)
            .scratch_dir(tmp.path().join("scratch"))
            .build(),
    )
    .await
    else {
        panic!("persistent_state() with scratch_dir() must be refused");
    };
    assert!(
        matches!(
            refused,
            meerkat_meta::UnifiedRuntimeBuilderError::ConflictingConfiguration(_)
        ),
        "{refused}"
    );
    assert!(
        !state.exists(),
        "a refused configuration created {}",
        state.display()
    );
}

#[tokio::test]
async fn builder_keeps_using_a_legacy_named_state_dir_where_it_lies() {
    let tmp = TempDir::new().expect("temp dir");
    let state = tmp.path().join("state");
    std::fs::create_dir_all(&state).expect("state dir");
    // The builder's own historical continuity spelling (recon census):
    // SQLite opens the zero-byte seed as an empty database and initializes
    // it in place. Its historical sessions spelling refuses instead
    // (`builder_refuses_a_legacy_sessions_database_until_it_is_migrated`).
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

    let (legacy, canonical) = ("identity_continuity.sqlite", "continuity.sqlite3");
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
    assert_no_sessions_database(&state);

    runtime.mob_handle().stop().await.expect("stop");
}

/// A legacy sessions database is the 0.9.3 session projection: the builder
/// refuses it typed before any store opens and leaves it where it lies for
/// the native migration to move.
#[tokio::test]
async fn builder_refuses_a_legacy_sessions_database_until_it_is_migrated() {
    let tmp = TempDir::new().expect("temp dir");
    let state = tmp.path().join("state");
    std::fs::create_dir_all(&state).expect("state dir");
    touch(&state.join("sessions.db"));

    let error = Box::pin(
        UnifiedRuntimeBuilder::default()
            .definition(test_definition())
            .persistent_state(&state)
            .roster_provider(Arc::new(EmptyRosterProvider))
            .default_llm_client(Arc::new(meerkat_client::TestClient::default()))
            .build(),
    )
    .await
    .err()
    .expect("a legacy sessions database must refuse to boot");
    assert!(
        matches!(
            error,
            meerkat_meta::UnifiedRuntimeBuilderError::StorageLayout(
                meerkat_meta::storage_layout::StorageLayoutError::NativeStorageMigrationRequired { .. }
            )
        ),
        "{error}"
    );
    assert_native_migration_refusal(&error.to_string(), &["sessions.db"]);
    assert_refused_boot_left_the_directory(&state, &["sessions.db"]);
}

/// Session file-name twins are two copies of the 0.9.3 session projection:
/// the builder refuses them like a single one, naming both.
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
    assert!(
        matches!(
            error,
            meerkat_meta::UnifiedRuntimeBuilderError::StorageLayout(
                meerkat_meta::storage_layout::StorageLayoutError::NativeStorageMigrationRequired { .. }
            )
        ),
        "{error}"
    );
    assert_native_migration_refusal(&error.to_string(), &["sessions.db", "sessions.sqlite3"]);
    assert_refused_boot_left_the_directory(&state, &["sessions.db", "sessions.sqlite3"]);
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
/// `persistent_state` and `mob_config`, and return the init response.
fn rpc_gateway_init(state_dir: &Path, mob_config: &str) -> Value {
    rpc_gateway_init_with(
        &json!({
            "persistent_state": state_dir,
            "mob_config": mob_config,
        }),
        &[],
    )
}

/// [`rpc_gateway_init`] with the init `params` given, and `env` set in the
/// gateway's environment.
fn rpc_gateway_init_with(params: &Value, env: &[(&str, &Path)]) -> Value {
    rpc_gateway_session(params, env, &[])
        .into_iter()
        .next()
        .expect("the init response")
}

/// Spawn the real `rpc_gateway --persistent` with `env` set, send `meta/init`
/// with `params`, then each of `requests` once init has answered, and return
/// the init response followed by each request's response, each matched by
/// its id. A refused init ends the session.
fn rpc_gateway_session(params: &Value, env: &[(&str, &Path)], requests: &[Value]) -> Vec<Value> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rpc_gateway"))
        .arg("--persistent")
        .envs(env.iter().copied())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn rpc_gateway");
    let mut stdin = child.stdin.take().expect("gateway stdin");
    let stdout = child.stdout.take().expect("gateway stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let init = json!({
        "jsonrpc": "2.0",
        "id": "init",
        "method": "meta/init",
        "params": params,
    });
    let mut responses = Vec::new();
    for request in std::iter::once(&init).chain(requests) {
        writeln!(
            stdin,
            "{}",
            serde_json::to_string(request).expect("request json")
        )
        .expect("write request");
        stdin.flush().expect("flush request");
        let response = loop {
            let line = rx
                .recv_timeout(Duration::from_secs(90))
                .unwrap_or_else(|_| panic!("rpc_gateway did not answer {}", request["method"]));
            let Ok(message) = serde_json::from_str::<Value>(line.trim()) else {
                continue;
            };
            // Notifications carry no id; a refusal of an unreadable line
            // carries a null one.
            if message["id"] == request["id"]
                || (message["id"].is_null() && message.get("error").is_some())
            {
                break message;
            }
        };
        let init_refused = request["method"] == "meta/init" && response.get("error").is_some();
        responses.push(response);
        if init_refused {
            break;
        }
    }
    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
    responses
}

/// `meta/mobpacks/deploy` with `execute`: a deploy that runs `rkat`, which a
/// runtime holding a named provider credential store refuses before reading
/// any document.
fn executing_deploy() -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": "deploy",
        "method": "meta/mobpacks/deploy",
        "params": { "execute": true },
    })
}

/// The deploy reply is the typed refusal of a runtime holding a named
/// provider credential store.
fn assert_named_store_refusal(launch: &str, deploy: &Value) {
    assert_eq!(
        (
            deploy["error"]["code"].clone(),
            deploy["error"]["data"]["kind"].clone()
        ),
        (
            json!(meerkat_meta::CAPABILITY_UNAVAILABLE_CODE),
            json!("provider_credentials_not_carried")
        ),
        "{launch}: the runtime must refuse an executing deploy: {deploy}"
    );
}

/// POST `body` as JSON to `path` on a gateway's loopback HTTP listener and
/// return the JSON reply.
fn post_json(base_url: &str, path: &str, body: &Value) -> Value {
    let authority = base_url
        .strip_prefix("http://")
        .unwrap_or_else(|| panic!("a plain http base url: {base_url}"));
    let mut stream =
        std::net::TcpStream::connect(authority).expect("connect to the gateway listener");
    stream
        .set_read_timeout(Some(Duration::from_secs(90)))
        .expect("read timeout");
    let body = body.to_string();
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("write the request");
    let mut reply = String::new();
    stream.read_to_string(&mut reply).expect("read the reply");
    let (_head, json) = reply
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("an HTTP reply: {reply}"));
    serde_json::from_str(json).unwrap_or_else(|error| panic!("a JSON reply ({error}): {reply}"))
}

/// A launch that names a provider credentials root opens it at boot, for a
/// persistent and a process-local launch alike, and records it: the
/// runtime then refuses an executing deploy, whose `rkat` process could not
/// carry the store. An unusable root refuses init with the storage
/// resolution code. Nothing reaches the process default credential store,
/// which lies in a scratch directory.
#[test]
fn rpc_gateway_opens_the_named_credentials_root_at_boot() {
    let scratch = TempDir::new().expect("scratch dir");
    let config_home = scratch.path().join("config");
    let home = scratch.path().join("home");
    let env = [
        ("XDG_CONFIG_HOME", config_home.as_path()),
        ("HOME", home.as_path()),
    ];
    let state = TempDir::new().expect("state dir");
    let mob_config = rpc_mob_config();
    for (launch, persistent) in [("persistent", true), ("process-local", false)] {
        let root = scratch.path().join(launch).join("credentials");
        let mut params = json!({
            "mob_config": mob_config,
            "runtime_options": { "provider_credentials_root": root },
        });
        if persistent {
            params["persistent_state"] = json!(state.path());
        }
        let responses = rpc_gateway_session(&params, &env, &[executing_deploy()]);
        let response = &responses[0];
        assert!(
            response["result"]["contract_version"].is_string(),
            "{launch}: init must succeed: {response}"
        );
        assert!(root.is_dir(), "{launch}: the named root is opened at boot");
        assert_named_store_refusal(launch, &responses[1]);
    }

    let unusable = scratch.path().join("not-a-directory");
    std::fs::write(&unusable, b"").expect("a regular file");
    let refused = rpc_gateway_init_with(
        &json!({
            "mob_config": mob_config,
            "runtime_options": { "provider_credentials_root": unusable },
        }),
        &env,
    );
    assert_eq!(
        refused["error"]["code"],
        json!(meerkat_meta::STORAGE_RESOLUTION_CODE),
        "{refused}"
    );

    for default_store in [
        config_home.join("meerkat").join("credentials"),
        home.join(".config").join("meerkat").join("credentials"),
        home.join("Library")
            .join("Application Support")
            .join("meerkat")
            .join("credentials"),
    ] {
        assert!(
            !default_store.exists(),
            "nothing may reach the process default store {}",
            default_store.display()
        );
    }
}

#[test]
fn rpc_gateway_fresh_state_dir_converges_on_canonical_names() {
    let state = TempDir::new().expect("state dir");
    // One mob throughout: the directory's composition record belongs to it.
    let mob_config = rpc_mob_config();
    let response = rpc_gateway_init(state.path(), &mob_config);
    assert!(
        response["result"]["contract_version"].is_string(),
        "fresh init must succeed: {response}"
    );
    for canonical in [
        "runtime.sqlite",
        "meta_metadata.sqlite3",
        "meta_console.sqlite3",
    ] {
        assert!(
            state.path().join(canonical).exists(),
            "fresh boot must create canonical {canonical}"
        );
    }
    assert_no_sessions_database(state.path());

    // The second start of the directory the first one left (killed after
    // init, as a crash would) reads it as native and starts.
    let again = rpc_gateway_init(state.path(), &mob_config);
    assert!(
        again["result"]["contract_version"].is_string(),
        "a directory this version created must start again: {again}"
    );
    assert_no_sessions_database(state.path());
}

/// `rpc_gateway`'s historical sessions spelling is the 0.9.3 session
/// projection: init refuses it with the storage-resolution code before any
/// store opens and leaves it where it lies.
#[test]
fn rpc_gateway_refuses_a_legacy_sessions_database_until_it_is_migrated() {
    let state = TempDir::new().expect("state dir");
    touch(&state.path().join("sessions.db"));

    let response = rpc_gateway_init(state.path(), &rpc_mob_config());
    assert_eq!(
        response["error"]["code"],
        json!(meerkat_meta::STORAGE_RESOLUTION_CODE),
        "{response}"
    );
    let message = response["error"]["message"].as_str().unwrap_or_default();
    assert_native_migration_refusal(message, &["sessions.db"]);
    assert_refused_boot_left_the_directory(state.path(), &["sessions.db"]);
}

#[test]
fn rpc_gateway_refuses_session_file_name_twins() {
    let state = TempDir::new().expect("state dir");
    touch(&state.path().join("sessions.db"));
    touch(&state.path().join("sessions.sqlite3"));

    let response = rpc_gateway_init(state.path(), &rpc_mob_config());
    assert_eq!(
        response["error"]["code"],
        json!(meerkat_meta::STORAGE_RESOLUTION_CODE),
        "{response}"
    );
    let message = response["error"]["message"].as_str().unwrap_or_default();
    assert_native_migration_refusal(message, &["sessions.db", "sessions.sqlite3"]);
    assert_refused_boot_left_the_directory(state.path(), &["sessions.db", "sessions.sqlite3"]);
}

#[test]
fn rpc_gateway_refuses_unadopted_predecessor_state() {
    let state = TempDir::new().expect("state dir");
    touch(
        &state
            .path()
            .join(meerkat_meta::storage_predecessor::PREDECESSOR_CONSOLE_DB_FILE_NAMES[0]),
    );

    let response = rpc_gateway_init(state.path(), &rpc_mob_config());
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
    with_meta_gateway(
        workspace,
        &json!({
            "workspace_root": workspace,
            "store_path": store_dir,
            "persistent_sessions": true,
        }),
        &[],
        |_| (),
    )
    .0
}

/// Spawn the real `meta_gateway` in `workspace` with `env` set, send
/// `meta/init` with `params`, run `while_up` with the init response while the
/// gateway still serves, then stop it.
fn with_meta_gateway<T>(
    workspace: &Path,
    params: &Value,
    env: &[(&str, &Path)],
    while_up: impl FnOnce(&Value) -> T,
) -> (Value, T) {
    let init = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "meta/init",
        "params": params,
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_meta_gateway"))
        .current_dir(workspace)
        // Dummy provider secrets: the fallback mob's LLM client is created
        // at bootstrap but never called (mirrors meta_gateway_bootstrap).
        .env("ANTHROPIC_API_KEY", "sk-ant-regression-test")
        .env("OPENAI_API_KEY", "sk-regression-test")
        .env("XDG_STATE_HOME", workspace.join("xdg-state"))
        .envs(env.iter().copied())
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
    let response: Value = serde_json::from_str(line.trim()).expect("init response json");
    let outcome = while_up(&response);
    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
    (response, outcome)
}

/// meta_gateway's top-level `provider_credentials_root` is opened at boot and
/// recorded: its Flow Editor refuses an executing deploy, whose `rkat`
/// process could not carry the store. An unusable root refuses init with
/// the storage resolution code. Nothing reaches the process default
/// credential store, which lies in a scratch directory.
#[test]
fn meta_gateway_opens_the_named_credentials_root_at_boot() {
    let workspace = TempDir::new().expect("workspace");
    let store = TempDir::new().expect("store");
    let scratch = TempDir::new().expect("scratch dir");
    let config_home = scratch.path().join("config");
    let home = scratch.path().join("home");
    let env = [
        ("XDG_CONFIG_HOME", config_home.as_path()),
        ("HOME", home.as_path()),
    ];
    let root = scratch.path().join("credentials");
    let (response, deploy) = with_meta_gateway(
        workspace.path(),
        &json!({
            "workspace_root": workspace.path(),
            "store_path": store.path().join("store"),
            "persistent_sessions": true,
            "provider_credentials_root": root,
        }),
        &env,
        |response| {
            response["result"]["http_base_url"]
                .as_str()
                .map(|base_url| post_json(base_url, "/flow-editor/rpc", &executing_deploy()))
        },
    );
    assert!(
        response.get("error").is_none(),
        "init must succeed: {response}"
    );
    assert!(root.is_dir(), "the named root is opened at boot");
    assert_named_store_refusal(
        "meta_gateway",
        &deploy.expect("init reports its http_base_url"),
    );

    let unusable = scratch.path().join("not-a-directory");
    std::fs::write(&unusable, b"").expect("a regular file");
    let refusing = TempDir::new().expect("workspace");
    let (refused, ()) = with_meta_gateway(
        refusing.path(),
        &json!({
            "workspace_root": refusing.path(),
            "store_path": store.path().join("refused-store"),
            "persistent_sessions": true,
            "provider_credentials_root": unusable,
        }),
        &env,
        |_| (),
    );
    assert_eq!(
        refused["error"]["code"],
        json!(meerkat_meta::STORAGE_RESOLUTION_CODE),
        "{refused}"
    );

    for default_store in [
        config_home.join("meerkat").join("credentials"),
        home.join(".config").join("meerkat").join("credentials"),
        home.join("Library")
            .join("Application Support")
            .join("meerkat")
            .join("credentials"),
    ] {
        assert!(
            !default_store.exists(),
            "nothing may reach the process default store {}",
            default_store.display()
        );
    }
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
    for canonical in ["runtime.sqlite", "continuity.sqlite3"] {
        assert!(
            store_dir.join(canonical).exists(),
            "fresh boot must create canonical {canonical}"
        );
    }
    assert_no_sessions_database(&store_dir);
    assert!(
        !store_dir.join("continuity.db").exists(),
        "fresh boot must not create legacy continuity.db"
    );

    // The second start of the store directory the first one left (killed
    // after init, as a crash would) reads it as native and creates a new
    // runtime on it.
    let again = meta_gateway_init(workspace.path(), &store_dir);
    assert_eq!(
        again["result"]["launch_state"],
        json!("created"),
        "a directory this version created must start again: {again}"
    );
    assert_no_sessions_database(&store_dir);
}

#[test]
fn meta_gateway_keeps_using_a_legacy_named_store_dir_where_it_lies() {
    let workspace = TempDir::new().expect("workspace");
    let store = TempDir::new().expect("store");
    let store_dir = store.path().join("store");
    std::fs::create_dir_all(&store_dir).expect("store dir");
    // meta_gateway's own historical continuity spelling (recon census). Its
    // historical sessions spelling refuses instead
    // (`meta_gateway_refuses_a_legacy_sessions_database_until_it_is_migrated`).
    touch(&store_dir.join("continuity.db"));

    let response = meta_gateway_init(workspace.path(), &store_dir);
    assert!(
        response.get("error").is_none(),
        "legacy-named init must succeed: {response}"
    );
    let (legacy, canonical) = ("continuity.db", "continuity.sqlite3");
    assert!(
        store_dir.join(legacy).exists(),
        "legacy {legacy} must keep being used where it lies"
    );
    assert!(
        !store_dir.join(canonical).exists(),
        "boot must not create a canonical twin {canonical} beside {legacy}"
    );
    assert_no_sessions_database(&store_dir);
}

/// `meta_gateway`'s historical sessions spelling is the 0.9.3 session
/// projection: init refuses it with the storage-resolution code before any
/// store opens and leaves it where it lies.
#[test]
fn meta_gateway_refuses_a_legacy_sessions_database_until_it_is_migrated() {
    let workspace = TempDir::new().expect("workspace");
    let store = TempDir::new().expect("store");
    let store_dir = store.path().join("store");
    std::fs::create_dir_all(&store_dir).expect("store dir");
    touch(&store_dir.join("sessions.sqlite"));

    let response = meta_gateway_init(workspace.path(), &store_dir);
    assert_eq!(
        response["error"]["code"],
        json!(meerkat_meta::STORAGE_RESOLUTION_CODE),
        "{response}"
    );
    let message = response["error"]["message"].as_str().unwrap_or_default();
    assert_native_migration_refusal(message, &["sessions.sqlite"]);
    assert_refused_boot_left_the_directory(&store_dir, &["sessions.sqlite"]);
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

// ---------------------------------------------------------------------------
// rpc_gateway: members built from a native realm with a member auth binding
// ---------------------------------------------------------------------------

/// The native realm the member route rows launch against.
const NATIVE_REALM: &str = "native-team";

/// The realm's own document, as a native host keeps it: three OpenAI API key
/// bindings, `a` the realm's default and `b` and `c` the ones the launches
/// bind their members to, each reading its key from its own environment
/// variable. The backend points at a closed local port, so nothing in these
/// rows can reach a provider.
const NATIVE_REALM_DOCUMENT: &str = r#"
[realm.native-team]
default_binding = "a"

[realm.native-team.backend.openai_main]
provider = "openai"
backend_kind = "openai_api"
base_url = "http://127.0.0.1:9/v1"

[realm.native-team.auth.key_a]
provider = "openai"
auth_method = "api_key"
source = { kind = "env", env = "NATIVE_REALM_TEST_KEY_A" }

[realm.native-team.auth.key_b]
provider = "openai"
auth_method = "api_key"
source = { kind = "env", env = "NATIVE_REALM_TEST_KEY_B" }

[realm.native-team.auth.key_c]
provider = "openai"
auth_method = "api_key"
source = { kind = "env", env = "NATIVE_REALM_TEST_KEY_C" }

[realm.native-team.binding.a]
backend_profile = "openai_main"
auth_profile = "key_a"

[realm.native-team.binding.b]
backend_profile = "openai_main"
auth_profile = "key_b"

[realm.native-team.binding.c]
backend_profile = "openai_main"
auth_profile = "key_c"
"#;

/// The environment variables of the realm's three keys.
const NATIVE_REALM_KEYS: [&str; 3] = [
    "NATIVE_REALM_TEST_KEY_A",
    "NATIVE_REALM_TEST_KEY_B",
    "NATIVE_REALM_TEST_KEY_C",
];

/// Variables a launch must not inherit: an ambient provider key, with which
/// a member built without a binding could still find a credential; the
/// `RKAT_`-prefixed overrides of the realm's own keys, which win over the
/// keys a launch sets; and the debug-build test client switch, which skips
/// credential resolution altogether.
const AMBIENT_PROVIDER_KEYS: [&str; 10] = [
    "RKAT_OPENAI_API_KEY",
    "OPENAI_API_KEY",
    "RKAT_ANTHROPIC_API_KEY",
    "ANTHROPIC_API_KEY",
    "RKAT_GEMINI_API_KEY",
    "GEMINI_API_KEY",
    "RKAT_NATIVE_REALM_TEST_KEY_A",
    "RKAT_NATIVE_REALM_TEST_KEY_B",
    "RKAT_NATIVE_REALM_TEST_KEY_C",
    "RKAT_TEST_CLIENT",
];

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a test runtime")
}

/// A native realm on disk under `root`, as a native host keeps one: its
/// manifest written with the store's own realm API and its document beside
/// it, with an empty user configuration root. Returns the
/// `runtime_options.native_realm` naming it.
fn write_native_realm(root: &Path) -> Value {
    let realms = root.join("realms");
    current_thread_runtime()
        .block_on(meerkat_store::ensure_realm_manifest_in(
            &realms,
            NATIVE_REALM,
            None,
            None,
        ))
        .expect("the native realm's manifest");
    std::fs::write(
        meerkat_store::realm_paths_in(&realms, NATIVE_REALM).config_path,
        NATIVE_REALM_DOCUMENT,
    )
    .expect("the native realm's document");
    std::fs::create_dir_all(root.join("native-home")).expect("the user configuration root");
    std::fs::create_dir_all(root.join("context")).expect("the context root");
    json!({
        "realm_id": NATIVE_REALM,
        "context_root": root.join("context"),
        "state_root": realms,
        "user_config_root": root.join("native-home"),
    })
}

/// The members' definition: one OpenAI profile, turn-driven so no member
/// starts a turn of its own. Per call, like `rpc_mob_config`.
fn member_route_mob_config() -> String {
    format!(
        r#"
[mob]
id = "native-realm-members-{}"

[profiles.worker]
model = "gpt-5.5"
provider = "openai"
runtime_mode = "turn_driven"

[profiles.worker.tools]
comms = true
"#,
        NEXT_TEST_MOB_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

/// The member auth binding naming `binding` in the native realm.
fn native_binding(binding: &str) -> Value {
    json!({ "realm": NATIVE_REALM, "binding": binding })
}

/// The init params of a member route launch, with an identity roster when
/// `roster` is set.
fn member_route_init(
    mob_config: &str,
    state: Option<&Path>,
    runtime_options: Value,
    roster: bool,
) -> Value {
    let mut params = json!({ "mob_config": mob_config, "runtime_options": runtime_options });
    if let Some(state) = state {
        params["persistent_state"] = json!(state);
    }
    if roster {
        params["has_roster_provider"] = json!(true);
        params["runtime_options"]["identity_bootstrap_mode"] =
            json!({ "mode": "eager_materialize" });
    }
    params
}

/// One `rpc_gateway --persistent` launch over real stdio, playing the SDK:
/// it answers the roster callback with the one identity `keeper` (profile
/// `worker`) whenever the gateway asks. Its HOME and XDG directories lie in
/// the scratch directory, no ambient provider key reaches it, and of the
/// realm's keys only those `keys` names are set.
struct MemberRouteGateway {
    child: std::process::Child,
    stdin: Option<std::process::ChildStdin>,
    lines: mpsc::Receiver<String>,
    log: std::path::PathBuf,
}

impl MemberRouteGateway {
    fn launch(scratch: &Path, name: &str, keys: &[&str]) -> Self {
        let log = scratch.join(format!("{name}.log"));
        let mut command = Command::new(env!("CARGO_BIN_EXE_rpc_gateway"));
        command
            .arg("--persistent")
            .env("HOME", scratch.join("process-home"))
            .env("XDG_CONFIG_HOME", scratch.join("process-config"))
            .env("XDG_STATE_HOME", scratch.join("process-state"))
            .env("XDG_DATA_HOME", scratch.join("process-data"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(std::fs::File::create(&log).expect("the gateway log"));
        for key in AMBIENT_PROVIDER_KEYS {
            command.env_remove(key);
        }
        for key in NATIVE_REALM_KEYS {
            if keys.contains(&key) {
                command.env(key, format!("sk-test-{key}"));
            } else {
                command.env_remove(key);
            }
        }
        let mut child = command.spawn().expect("spawn rpc_gateway");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("the gateway stdout");
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            stdin,
            lines,
            log,
        }
    }

    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn send(&mut self, message: &Value) {
        let stdin = self.stdin.as_mut().expect("the gateway stdin is open");
        writeln!(stdin, "{message}").expect("write to the gateway");
        stdin.flush().expect("flush to the gateway");
    }

    /// Send `method` with `params` and read until its response, answering
    /// every callback on the way.
    fn call(&mut self, method: &str, params: Value) -> Value {
        let id = json!(format!("{method}-request"));
        self.send(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        loop {
            let Ok(line) = self.lines.recv_timeout(Duration::from_mins(3)) else {
                panic!(
                    "rpc_gateway did not answer {method}; its log:\n{}",
                    self.log()
                );
            };
            let Some(message) = line
                .find('{')
                .and_then(|start| serde_json::from_str::<Value>(&line[start..]).ok())
            else {
                continue;
            };
            if let (Some(callback), Some(callback_id)) = (
                message.get("method").and_then(Value::as_str),
                message.get("id").cloned(),
            ) {
                let reply = if callback == "callback/roster_provider/roster" {
                    json!({
                        "jsonrpc": "2.0",
                        "id": callback_id,
                        "result": [{ "identity": "keeper", "profile": "worker" }],
                    })
                } else {
                    json!({
                        "jsonrpc": "2.0",
                        "id": callback_id,
                        "error": { "code": -32601, "message": format!("no handler for {callback}") },
                    })
                };
                self.send(&reply);
                continue;
            }
            if message.get("method").is_none() && message["id"] == id {
                return message;
            }
        }
    }

    /// [`Self::call`], asserting the request succeeded; returns its result.
    fn call_ok(&mut self, method: &str, params: Value) -> Value {
        let response = self.call(method, params);
        assert!(
            response.get("error").is_none(),
            "{method} must succeed: {response}; the gateway log:\n{}",
            self.log()
        );
        response["result"].clone()
    }

    /// The keeper's identity status, asserting it is live, and its session.
    fn live_keeper_session(&mut self, step: &str) -> Value {
        let status = self.call_ok("meta/status_identity", json!({ "identity": "keeper" }));
        assert_eq!(
            status["state"],
            json!("active"),
            "{step}: the keeper is live: {status}; the gateway log:\n{}",
            self.log()
        );
        assert!(status["session_id"].is_string(), "{step}: {status}");
        status["session_id"].clone()
    }

    /// A raw member's state, asserting it is live: Core restores or spawns a
    /// member it cannot build as broken, never live.
    fn live_member(&mut self, step: &str, member: &str) {
        let entry = self.call_ok("meta/get_member", json!({ "member_id": member }));
        assert_eq!(
            entry["state"],
            json!("active"),
            "{step}: {member} is live: {entry}; the gateway log:\n{}",
            self.log()
        );
    }

    /// One tracked turn of the keeper, waited until it completes.
    fn keeper_turn(&mut self) {
        let sent = self.call_ok(
            "meta/send",
            json!({ "identity": "keeper", "content": "Answer with one word.", "track_turn": true }),
        );
        let ticket = sent["turn"]["ticket"].clone();
        assert!(ticket.is_string(), "a tracked turn: {sent}");
        let waited = self.call_ok(
            "meta/wait_for_turn",
            json!({ "identity": "keeper", "ticket": ticket, "timeout_ms": 120_000 }),
        );
        assert_eq!(
            waited["state"],
            json!("completed"),
            "the turn completes: {waited}"
        );
    }

    /// Close stdin and wait for the gateway's graceful exit.
    fn finish(mut self) {
        drop(self.stdin.take());
        let deadline = std::time::Instant::now() + Duration::from_mins(3);
        while self.child.try_wait().expect("poll the gateway").is_none()
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(50));
        }
        let exited = self.child.try_wait().expect("poll the gateway").is_some();
        if !exited {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        assert!(
            exited,
            "the gateway ends once its stdin closes; its log:\n{}",
            self.log()
        );
    }
}

/// Every member session in `state`'s session store that records how its
/// member was built: `(roster member, auth binding)`, read from the member's
/// own session record, the metadata Core writes for the member's build,
/// never from the options a launch sent.
fn member_session_bindings(state: &Path) -> Vec<(String, Value)> {
    current_thread_runtime().block_on(async {
        use meerkat::SessionStore as _;
        let store = meerkat_store::SqliteSessionStore::open(state.join("runtime.sqlite"))
            .expect("open the session store");
        let mut members = Vec::new();
        for meta in store
            .list(meerkat::SessionFilter::default())
            .await
            .expect("list the sessions")
        {
            let Some(session) = store.load(&meta.id).await.expect("load a session") else {
                continue;
            };
            let Some(metadata) = session
                .try_session_metadata()
                .expect("decode the session metadata")
            else {
                continue;
            };
            let Some(member) = metadata.mob_member_binding.as_ref() else {
                continue;
            };
            members.push((
                member.member.clone(),
                serde_json::to_value(&metadata.auth_binding).expect("the binding as JSON"),
            ));
        }
        members
    })
}

/// A launch whose members build from a native realm with a member auth
/// binding resolves every member it constructs through that binding in the
/// realm's own configuration, on each construction path that runs no model
/// turn: the initial roster member, a `meta/spawn_member` member, the
/// successor of an identity reset, an identity respawn (which rebuilds the
/// member on its own session), and the restore of every member by a new
/// launch, the non-roster member included (Core's restore alone rebuilds
/// it).
///
/// Each build resolves the member's provider key for real. The binding and
/// its key exist only in the realm's own document and the variable the
/// launch sets, the realm's default binding `a` has no key, and no ambient
/// provider key, key override or test client switch reaches the gateway, so
/// a member builds only with the launch's binding resolved in the realm's
/// configuration. The second launch names binding `c` and sets only `c`'s
/// key, so the restore and the respawn, which rebuild the first launch's
/// members on their own sessions, build only by taking the second launch's
/// binding in place of the durable `b`. Every member session of the first
/// launch (the keeper's two and the spawned member's) records the first
/// launch's binding.
#[test]
fn rpc_gateway_resolves_every_member_through_the_native_realm_binding() {
    let scratch = TempDir::new().expect("scratch dir");
    let state = scratch.path().join("state");
    let native_realm = write_native_realm(scratch.path());
    let mob_config = member_route_mob_config();

    let mut first =
        MemberRouteGateway::launch(scratch.path(), "first", &["NATIVE_REALM_TEST_KEY_B"]);
    let init = first.call(
        "meta/init",
        member_route_init(
            &mob_config,
            Some(&state),
            json!({
                "native_realm": native_realm,
                "member_auth_binding": {
                    "realm": NATIVE_REALM,
                    "binding": "b",
                    "origin": "configured",
                },
            }),
            true,
        ),
    );
    assert!(
        init["result"]["contract_version"].is_string(),
        "the first launch starts: {init}; its log:\n{}",
        first.log()
    );
    first.live_keeper_session("the initial roster member");
    let spawned = first.call_ok(
        "meta/spawn_member",
        json!({ "profile": "worker", "meerkat_id": "spawned" }),
    );
    assert_eq!(spawned["accepted"], json!(true), "{spawned}");
    first.live_member("the spawned member", "spawned");
    first.call_ok("meta/reset", json!({ "identity": "keeper" }));
    let keeper_session = first.live_keeper_session("the reset successor");
    first.call_ok("meta/respawn", json!({ "identity": "keeper" }));
    assert_eq!(
        first.live_keeper_session("the respawned member"),
        keeper_session,
        "a respawn rebuilds the member on its own session"
    );
    first.finish();

    let b = native_binding("b");
    let recorded = member_session_bindings(&state);
    let sessions_of = |member: &str| {
        recorded
            .iter()
            .filter(|(recorded_member, _)| recorded_member == member)
            .count()
    };
    assert!(
        sessions_of("keeper") >= 2,
        "the keeper records its first session and its reset successor: {recorded:?}"
    );
    assert!(
        sessions_of("spawned") >= 1,
        "the spawned member records its session: {recorded:?}"
    );
    assert!(
        recorded.iter().all(|(_, binding)| *binding == b),
        "every member session the first launch recorded names binding b: {recorded:?}"
    );

    let mut second =
        MemberRouteGateway::launch(scratch.path(), "second", &["NATIVE_REALM_TEST_KEY_C"]);
    let init = second.call(
        "meta/init",
        member_route_init(
            &mob_config,
            Some(&state),
            json!({
                "native_realm": native_realm,
                "member_auth_binding": native_binding("c"),
            }),
            true,
        ),
    );
    assert!(
        init["result"]["contract_version"].is_string(),
        "the second launch restores its members with binding c: {init}; its log:\n{}",
        second.log()
    );
    assert_eq!(
        second.live_keeper_session("the restored member"),
        keeper_session,
        "the restore rebuilds the keeper on its own session"
    );
    second.live_member("the restored non-roster member", "spawned");
    second.call_ok("meta/respawn", json!({ "identity": "keeper" }));
    assert_eq!(
        second.live_keeper_session("the member respawned by the second launch"),
        keeper_session,
        "a respawn rebuilds the member on its own session"
    );
    second.finish();
}

/// Each member session records the binding its member was built with, read
/// from the session itself: the initial roster member and its reset
/// successor after a turn each, a helper and a fork helper, which answer only
/// once their turn has run. The deterministic demo client serves the turns.
#[test]
fn rpc_gateway_records_the_member_binding_on_every_member_session() {
    let scratch = TempDir::new().expect("scratch dir");
    let state = scratch.path().join("state");
    let native_realm = write_native_realm(scratch.path());

    let mut gateway =
        MemberRouteGateway::launch(scratch.path(), "recorded", &["NATIVE_REALM_TEST_KEY_B"]);
    let init = gateway.call(
        "meta/init",
        member_route_init(
            &member_route_mob_config(),
            Some(&state),
            json!({
                "native_realm": native_realm,
                "member_auth_binding": native_binding("b"),
                "demo_llm": true,
            }),
            true,
        ),
    );
    assert!(
        init["result"]["contract_version"].is_string(),
        "the launch starts: {init}; its log:\n{}",
        gateway.log()
    );
    gateway.keeper_turn();
    let helper_options = json!({ "role": "worker", "runtime_mode": "turn_driven" });
    gateway.call_ok(
        "meta/spawn_helper",
        json!({
            "agent_identity": "helper",
            "task": "Answer with one word.",
            "result_label": "answer",
            "max_text_bytes": 4096,
            "options": helper_options,
        }),
    );
    gateway.call_ok(
        "meta/fork_helper",
        json!({
            "source_member_id": "keeper",
            "agent_identity": "forked",
            "task": "Answer with one word.",
            "result_label": "answer",
            "max_text_bytes": 4096,
            "options": helper_options,
        }),
    );
    gateway.call_ok("meta/reset", json!({ "identity": "keeper" }));
    gateway.keeper_turn();
    gateway.finish();

    let b = native_binding("b");
    let recorded = member_session_bindings(&state);
    for (member, sessions) in [("keeper", 2_usize), ("helper", 1), ("forked", 1)] {
        assert!(
            recorded
                .iter()
                .filter(|(recorded_member, _)| recorded_member == member)
                .count()
                >= sessions,
            "{member} records {sessions} session(s): {recorded:?}"
        );
    }
    assert!(
        recorded.iter().all(|(_, binding)| *binding == b),
        "every member session records binding b: {recorded:?}"
    );
}

/// A member route the gateway cannot carry refuses init, typed, before any
/// factory is built: the native realm beside the host config file, a realm
/// the state root does not hold, a binding the realm's configuration does
/// not define, an origin other than `configured`, and a profile the binding
/// does not serve.
#[test]
fn rpc_gateway_refuses_a_member_route_it_cannot_carry() {
    let scratch = TempDir::new().expect("scratch dir");
    let native_realm = write_native_realm(scratch.path());
    let mut absent_realm = native_realm.clone();
    absent_realm["realm_id"] = json!("absent-team");
    let mob_config = member_route_mob_config();
    let anthropic_mob_config = format!(
        "[mob]\nid = \"native-realm-refusal-{}\"\n\n[profiles.lead]\n\
         model = \"claude-sonnet-4-5\"\nprovider = \"anthropic\"\n",
        NEXT_TEST_MOB_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let cases = [
        (
            "both-config-sources",
            mob_config.as_str(),
            json!({
                "native_realm": native_realm,
                "member_auth_binding": native_binding("b"),
                "meerkat_config_path": scratch.path().join("config.toml"),
            }),
            -32602,
            "native_realm_config_conflict",
        ),
        (
            "unknown-realm",
            mob_config.as_str(),
            json!({ "native_realm": absent_realm, "member_auth_binding": native_binding("b") }),
            meerkat_meta::STORAGE_RESOLUTION_CODE,
            "native_realm_unknown",
        ),
        (
            "missing-binding",
            mob_config.as_str(),
            json!({
                "native_realm": native_realm,
                "member_auth_binding": native_binding("absent"),
            }),
            -32602,
            "member_auth_binding_unknown",
        ),
        (
            "origin",
            mob_config.as_str(),
            json!({
                "native_realm": native_realm,
                "member_auth_binding": {
                    "realm": NATIVE_REALM,
                    "binding": "b",
                    "origin": "synthetic_env_default",
                },
            }),
            -32602,
            "member_auth_binding_origin",
        ),
        (
            "provider-mismatch",
            anthropic_mob_config.as_str(),
            json!({ "native_realm": native_realm, "member_auth_binding": native_binding("b") }),
            -32602,
            "member_auth_binding_mismatch",
        ),
    ];
    for (case, mob_config, runtime_options, code, kind) in cases {
        let mut gateway = MemberRouteGateway::launch(scratch.path(), case, &[]);
        let init = gateway.call(
            "meta/init",
            member_route_init(mob_config, None, runtime_options, false),
        );
        gateway.finish();
        assert_eq!(
            (
                init["error"]["code"].clone(),
                init["error"]["data"]["kind"].clone()
            ),
            (json!(code), json!(kind)),
            "{case}: {init}"
        );
    }
}
