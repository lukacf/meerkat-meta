//! Gateway maintenance subcommand contract (storage-unification M6):
//! `meta_gateway storage-migrate --state-dir <dir> [--apply] [--adopt
//! <path>] [--json]` and `meta_gateway storage-prune --state-dir <dir>
//! [--apply] [--older-than-days N] [--json]`.
//!
//! Both verbs bypass the stdin init handshake entirely and are dry-run by
//! default; the state directory is mutated only with `--apply`. Exit codes:
//! 0 clean, 1 refusals or fence/store failures, 2 usage errors.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::path::Path;
use std::process::Command;

use sha2::{Digest, Sha256};

/// The 0.7.x-era continuity DDL (no `meerkat_schema` ledger): what pre-M3
/// binaries actually wrote — exactly the population the verb converges.
const LEGACY_DDL: &str = "CREATE TABLE continuity_records (
        identity       TEXT PRIMARY KEY,
        agent_runtime_id TEXT NOT NULL,
        session_id     TEXT NOT NULL,
        generation     INTEGER NOT NULL,
        checkpoint_version INTEGER NOT NULL,
        fencing_token  INTEGER NOT NULL
    );
    CREATE TABLE session_snapshots (
        session_id     TEXT PRIMARY KEY,
        identity       TEXT NOT NULL,
        generation     INTEGER NOT NULL,
        checkpoint_version INTEGER NOT NULL,
        fencing_token  INTEGER NOT NULL,
        data           BLOB NOT NULL
    );";

/// Create a legacy-named continuity fixture with one adoptable
/// legacy-unverified snapshot (matching record at generation 3 / checkpoint
/// version 4). Returns the session id.
fn seed_legacy_continuity(path: &Path) -> String {
    let conn = rusqlite::Connection::open(path).expect("create fixture db");
    conn.execute_batch(LEGACY_DDL).expect("apply legacy ddl");
    // Ledgered at the v1 floor: pre-ledger fixtures are below the meta
    // 0.8.8 floor and refuse typed since the 0.8.11 reset.
    conn.execute_batch(
        "CREATE TABLE meerkat_schema (domain TEXT PRIMARY KEY, version INTEGER NOT NULL);
         INSERT INTO meerkat_schema (domain, version) VALUES ('meta-continuity', 1);",
    )
    .expect("v1 ledger row");
    let session = meerkat_core::Session::new();
    let sid = session.id().to_string();
    let legacy = serde_json::to_vec(&session).expect("serialize legacy session");
    conn.execute(
        "INSERT INTO continuity_records \
         (identity, agent_runtime_id, session_id, generation, checkpoint_version, fencing_token) \
         VALUES ('test:alice', 'rt-1', ?1, 3, 4, 7)",
        [&sid],
    )
    .expect("insert record");
    conn.execute(
        "INSERT INTO session_snapshots \
         (session_id, identity, generation, checkpoint_version, fencing_token, data) \
         VALUES (?1, 'test:alice', 3, 4, 7, ?2)",
        rusqlite::params![sid, legacy],
    )
    .expect("insert snapshot");
    sid
}

fn file_digest(path: &Path) -> String {
    let bytes = std::fs::read(path).expect("read db file");
    format!("{:x}", Sha256::digest(&bytes))
}

struct VerbOutput {
    code: i32,
    stdout: String,
    stderr: String,
}

fn run_verb(verb: &str, args: &[&str]) -> VerbOutput {
    let output = Command::new(env!("CARGO_BIN_EXE_meta_gateway"))
        .arg(verb)
        .args(args)
        .output()
        .expect("spawn meta_gateway");
    VerbOutput {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

fn report_json(stdout: &str) -> serde_json::Value {
    serde_json::from_str(stdout).unwrap_or_else(|error| {
        panic!("stdout is not a JSON report ({error}): {stdout}");
    })
}

#[test]
fn migrate_dry_run_is_the_default_and_never_mutates() {
    let dir = tempfile::tempdir().expect("tempdir");
    let legacy = dir.path().join("continuity.db");
    seed_legacy_continuity(&legacy);
    let before = file_digest(&legacy);

    let run = run_verb(
        "storage-migrate",
        &["--state-dir", dir.path().to_str().unwrap(), "--json"],
    );
    assert_eq!(
        run.code, 0,
        "clean dry-run must exit 0; stderr: {}",
        run.stderr
    );
    let report = report_json(&run.stdout);
    assert_eq!(report["mode"], "dry_run");
    assert_eq!(report["renames"][0]["action"], "would-rename");
    // The fixture carries its v1 floor ledger row (pre-ledger files refuse
    // typed since the 0.8.11 reset), so dry-run RECORDS it rather than
    // promising a stamp.
    assert_eq!(
        report["ledger"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["domain"] == "meta-continuity")
            .expect("continuity ledger entry")["action"],
        "recorded"
    );

    assert!(legacy.is_file(), "dry-run must not rename");
    assert_eq!(
        file_digest(&legacy),
        before,
        "without --apply the database must stay byte-identical"
    );
}

#[test]
fn migrate_apply_renames_stamps_adopts_and_is_idempotent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let legacy = dir.path().join("continuity.db");
    let sid = seed_legacy_continuity(&legacy);

    let first = run_verb(
        "storage-migrate",
        &[
            "--state-dir",
            dir.path().to_str().unwrap(),
            "--apply",
            "--json",
        ],
    );
    assert_eq!(first.code, 0, "stderr: {}", first.stderr);
    let report = report_json(&first.stdout);
    assert_eq!(report["renames"][0]["action"], "renamed");

    let canonical = dir.path().join("continuity.sqlite3");
    assert!(canonical.is_file(), "renamed to the canonical spelling");
    assert!(!legacy.exists());
    // The snapshot payload survives byte-for-byte: the retired adoption
    // walk no longer rewrites documents (0.8.11 reset).
    {
        let conn = rusqlite::Connection::open(&canonical).expect("reopen");
        let data: Vec<u8> = conn
            .query_row(
                "SELECT data FROM session_snapshots WHERE session_id = ?1",
                [&sid],
                |row| row.get(0),
            )
            .expect("snapshot row");
        let _session: meerkat_core::Session =
            serde_json::from_slice(&data).expect("decode current-format document");
    }
    // The rename marker is a registered backup artifact prune can see.
    let marker = report["renames"][0]["marker"]
        .as_str()
        .expect("rename marker recorded");
    assert!(marker.contains(".pre-"), "{marker}");
    assert!(Path::new(marker).is_file());

    let after_first = file_digest(&canonical);
    let second = run_verb(
        "storage-migrate",
        &[
            "--state-dir",
            dir.path().to_str().unwrap(),
            "--apply",
            "--json",
        ],
    );
    assert_eq!(second.code, 0, "stderr: {}", second.stderr);
    let report = report_json(&second.stdout);
    assert!(report["renames"].as_array().unwrap().is_empty());
    assert_eq!(
        file_digest(&canonical),
        after_first,
        "a second apply must be a byte-identical no-op"
    );
}

#[test]
fn migrate_twins_refuse_then_adopt_resolves() {
    let dir = tempfile::tempdir().expect("tempdir");
    let legacy = dir.path().join("continuity.db");
    let canonical = dir.path().join("continuity.sqlite3");
    seed_legacy_continuity(&legacy);
    seed_legacy_continuity(&canonical);

    // Twins fail closed: exit 1, divergence report, nothing moved.
    let refused = run_verb(
        "storage-migrate",
        &["--state-dir", dir.path().to_str().unwrap(), "--json"],
    );
    assert_eq!(
        refused.code, 1,
        "twins must exit 1; stdout: {}",
        refused.stdout
    );
    let report = report_json(&refused.stdout);
    assert_eq!(report["twins"][0]["slot"], "continuity");
    assert_eq!(report["twins"][0]["resolution"]["kind"], "refused");
    assert!(legacy.is_file());
    assert!(canonical.is_file());

    // --adopt (with --apply) adopts one copy and archives the other.
    let resolved = run_verb(
        "storage-migrate",
        &[
            "--state-dir",
            dir.path().to_str().unwrap(),
            "--apply",
            "--adopt",
            canonical.to_str().unwrap(),
            "--json",
        ],
    );
    assert_eq!(resolved.code, 0, "stderr: {}", resolved.stderr);
    let report = report_json(&resolved.stdout);
    assert_eq!(report["twins"][0]["resolution"]["kind"], "adopted");
    assert!(!legacy.exists(), "the non-adopted copy is archived");
    assert!(canonical.is_file());
}

#[test]
fn usage_errors_exit_2() {
    let run = run_verb("storage-migrate", &[]);
    assert_eq!(run.code, 2, "missing --state-dir must exit 2");
    assert!(run.stderr.contains("usage:"), "stderr: {}", run.stderr);

    let run = run_verb("storage-migrate", &["--state-dir", "/tmp", "--frobnicate"]);
    assert_eq!(run.code, 2, "unknown flags must exit 2");

    let run = run_verb("storage-migrate", &["--state-dir", "/tmp", "--adopt", "/x"]);
    assert_eq!(run.code, 2, "--adopt without --apply must exit 2");
    assert!(run.stderr.contains("--adopt requires --apply"));

    let run = run_verb("storage-prune", &[]);
    assert_eq!(run.code, 2, "missing --state-dir must exit 2");
    let run = run_verb(
        "storage-prune",
        &["--state-dir", "/tmp", "--older-than-days", "x"],
    );
    assert_eq!(run.code, 2, "non-numeric threshold must exit 2");
}

#[test]
fn help_prints_usage_and_exits_0() {
    for verb in [
        "storage-migrate",
        "storage-prune",
        "storage-adopt-predecessor",
    ] {
        for flag in ["--help", "-h"] {
            let run = run_verb(verb, &[flag]);
            assert_eq!(run.code, 0, "{verb} {flag} must exit 0: {}", run.stderr);
            assert!(
                run.stdout.contains(&format!("usage: meta_gateway {verb}")),
                "{verb} {flag} stdout: {}",
                run.stdout
            );
        }
    }
}

#[test]
fn prune_dry_run_lists_and_apply_deletes_registered_artifacts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let artifact = dir.path().join("continuity.db.pre-0.0.1-1700000000.twin");
    std::fs::write(&artifact, b"backup").expect("artifact");
    let distractor = dir.path().join("continuity.sqlite3");
    std::fs::write(&distractor, b"live").expect("distractor");

    let dry = run_verb(
        "storage-prune",
        &[
            "--state-dir",
            dir.path().to_str().unwrap(),
            "--older-than-days",
            "0",
            "--json",
        ],
    );
    assert_eq!(dry.code, 0, "stderr: {}", dry.stderr);
    let report = report_json(&dry.stdout);
    assert_eq!(report["artifacts"].as_array().map(Vec::len), Some(1));
    assert_eq!(report["artifacts"][0]["action"], "would-delete");
    assert!(artifact.is_file(), "dry-run deletes nothing");

    let applied = run_verb(
        "storage-prune",
        &[
            "--state-dir",
            dir.path().to_str().unwrap(),
            "--apply",
            "--older-than-days",
            "0",
            "--json",
        ],
    );
    assert_eq!(applied.code, 0, "stderr: {}", applied.stderr);
    let report = report_json(&applied.stdout);
    assert_eq!(report["artifacts"][0]["action"], "deleted");
    assert!(!artifact.exists());
    assert!(distractor.is_file(), "unregistered names are never touched");
}
