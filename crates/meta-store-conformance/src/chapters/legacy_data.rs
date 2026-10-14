//! Legacy-data axis: synthetic fixtures with genuine schema scar tissue.
//!
//! Release-day incidents have been legacy-shape issues, not fresh-store
//! bugs. These chapters fabricate the exact on-disk shapes older Meta
//! lines persisted and pin the 0.8.11 contract for them: a pre-ledger
//! database is BELOW the supported floor (mobkit 0.8.8 stamped a schema
//! ledger row at every open, so every supported corpus is ledgered), and
//! today's bundled stores refuse it typed at open — an unledgered file
//! whose owned tables already exist is never silently converged the way
//! every earlier line converged it — while leaving its logical content
//! untouched for the operator. Both chapters are scoped to the bundled
//! implementations by construction — they fabricate those stores' schemas.

use std::path::Path;

use meerkat_meta::identity_first::{
    AgentIdentity, AgentMemoryProvider, LocalContinuityStore, NewAgentMemory,
};
use meerkat_meta::memory::SqliteAgentMemoryStore;
use meerkat_store_conformance::ConformanceFailure;

use crate::fixtures;
use crate::steps::Steps;

/// The pre-ledger continuity schema exactly as `LocalContinuityStore` has
/// always created it (`identity_first/local_store.rs` DDL): two tables, no
/// schema ledger, no secondary indexes.
const LEGACY_CONTINUITY_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS continuity_records (
        identity       TEXT PRIMARY KEY,
        agent_runtime_id TEXT NOT NULL,
        session_id     TEXT NOT NULL,
        generation     INTEGER NOT NULL,
        checkpoint_version INTEGER NOT NULL,
        fencing_token  INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS session_snapshots (
        session_id     TEXT PRIMARY KEY,
        identity       TEXT NOT NULL,
        generation     INTEGER NOT NULL,
        checkpoint_version INTEGER NOT NULL,
        fencing_token  INTEGER NOT NULL,
        data           BLOB NOT NULL
    );";

/// A pre-ledger continuity database (the exact file shape a pre-floor fleet
/// persisted) must be refused typed at open: the file carries the domain's
/// owned tables but no `meerkat_schema` row, which is below the meta
/// 0.8.8 floor. The refusal must leave the fabricated rows untouched —
/// fail-closed means the operator still owns byte-exact evidence.
///
/// This chapter pinned silent pre-ledger convergence (plus LegacyUnverified
/// snapshot service) until the 0.8.11 reset deleted both the embedded
/// checkpoint vocabulary and pre-floor convergence; it now pins the typed
/// refusal that replaced them.
pub async fn legacy_continuity_database(dir: &Path) -> Result<(), ConformanceFailure> {
    let steps = Steps::chapter("legacy_continuity_database");
    let db_path = dir.join("continuity.db");

    // --- fabricate the legacy database ---------------------------------------
    let step = "fabricate_legacy_database";
    let session = fixtures::session_with_texts(&["legacy turn one", "legacy turn two"])?;
    let legacy_blob = steps.wrap(step, serde_json::to_vec(&session))?;
    let session_key = session.id().to_string();
    {
        let connection = steps.wrap(step, rusqlite::Connection::open(&db_path))?;
        steps.wrap(step, connection.execute_batch(LEGACY_CONTINUITY_SCHEMA))?;
        steps.wrap(
            step,
            connection.execute(
                "INSERT INTO continuity_records \
                 (identity, agent_runtime_id, session_id, generation, checkpoint_version, \
                  fencing_token) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params!["legacy:main", "rt-legacy", session_key, 3_u64, 7_u64, 9_u64],
            ),
        )?;
        steps.wrap(
            step,
            connection.execute(
                "INSERT INTO session_snapshots \
                 (session_id, identity, generation, checkpoint_version, fencing_token, data) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![session_key, "legacy:main", 3_u64, 7_u64, 9_u64, legacy_blob],
            ),
        )?;
    }

    // --- open is refused typed -------------------------------------------------
    let step = "pre_ledger_open_is_refused";
    if LocalContinuityStore::open_with_fencing_floor(db_path.clone())
        .await
        .is_ok()
    {
        return Err(steps.fail(
            step,
            "opening a pre-ledger continuity database must refuse typed: unledgered owned \
             tables are below the mobkit 0.8.8 floor and must never be silently converged",
        ));
    }

    // --- refusal left the logical content untouched ----------------------------
    let step = "refusal_preserves_rows";
    let connection = steps.wrap(step, rusqlite::Connection::open(&db_path))?;
    let (identity, generation, version, fence): (String, u64, u64, u64) = steps.wrap(
        step,
        connection.query_row(
            "SELECT identity, generation, checkpoint_version, fencing_token \
             FROM continuity_records WHERE session_id = ?1",
            rusqlite::params![session_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ),
    )?;
    steps.ensure(
        step,
        identity == "legacy:main" && generation == 3 && version == 7 && fence == 9,
        "the refused open must leave the continuity record intact",
    )?;
    let stored_blob: Vec<u8> = steps.wrap(
        step,
        connection.query_row(
            "SELECT data FROM session_snapshots WHERE session_id = ?1",
            rusqlite::params![session_key],
            |row| row.get(0),
        ),
    )?;
    steps.ensure(
        step,
        stored_blob == steps.wrap(step, serde_json::to_vec(&session))?,
        "the refused open must leave the snapshot payload bytes intact",
    )?;
    let ledgered: bool = steps.wrap(
        step,
        connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'meerkat_schema')",
            [],
            |row| row.get(0),
        ),
    )?;
    steps.ensure(
        step,
        !ledgered,
        "the refusal must not stamp a schema ledger onto a file it refused to own",
    )?;
    Ok(())
}

/// The pre-`ever_quarantined` / pre-`proposals.taint` agent-memory schema:
/// `SCHEMA_SQL` as it existed before those columns, and before the schema
/// ledger.
const LEGACY_MEMORY_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS records (
    memory_id       TEXT PRIMARY KEY,
    scope_kind      TEXT NOT NULL,
    scope_key       TEXT NOT NULL,
    kind            TEXT NOT NULL,
    title           TEXT NOT NULL,
    description     TEXT NOT NULL DEFAULT '',
    body            TEXT NOT NULL,
    tags            TEXT NOT NULL DEFAULT '[]',
    provenance      TEXT NOT NULL,
    trust           TEXT NOT NULL,
    status_kind     TEXT NOT NULL,
    status_detail   TEXT,
    supersedes      TEXT,
    derived_from    TEXT NOT NULL DEFAULT '[]',
    working_set_rank INTEGER,
    rank_set_at_ms  INTEGER,
    content_hash    TEXT NOT NULL,
    created_at_ms   INTEGER NOT NULL,
    updated_at_ms   INTEGER NOT NULL,
    usage_stats     TEXT NOT NULL DEFAULT '{}',
    tombstoned_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS records_scope_idx
    ON records(scope_kind, scope_key, status_kind);
CREATE INDEX IF NOT EXISTS records_scope_hash_idx
    ON records(scope_kind, scope_key, content_hash);

CREATE TABLE IF NOT EXISTS proposals (
    proposal_id   TEXT PRIMARY KEY,
    scope_kind    TEXT NOT NULL,
    scope_key     TEXT NOT NULL,
    record        TEXT NOT NULL,
    author        TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS audit (
    audit_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    stage_token   TEXT NOT NULL,
    op_index      INTEGER NOT NULL,
    op_kind       TEXT NOT NULL,
    memory_id     TEXT,
    detail        TEXT NOT NULL,
    applied_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS stage (
    token         TEXT PRIMARY KEY,
    batch         TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
";

/// A memory-realm database with pre-column, pre-ledger scar tissue must be
/// refused typed at the realm connection: the file carries the domain's
/// owned tables but no `meerkat_schema` row, which is below the meta
/// 0.8.8 floor. The refusal must leave the fabricated rows untouched.
///
/// This chapter pinned the open-time `ensure_column` upgrade (column adds,
/// quarantine/taint backfills) until the 0.8.11 reset retired pre-floor
/// convergence; it now pins the typed refusal that replaced it.
pub async fn legacy_memory_database(root: &Path) -> Result<(), ConformanceFailure> {
    let steps = Steps::chapter("legacy_memory_database");
    // The bundled store keys one database per realm:
    // `<root>/<encoded-realm>.sqlite3`; "default" encodes to itself.
    let db_path = root.join("default.sqlite3");

    // --- fabricate the legacy realm database -----------------------------------
    let step = "fabricate_legacy_database";
    {
        let connection = steps.wrap(step, rusqlite::Connection::open(&db_path))?;
        steps.wrap(step, connection.execute_batch(LEGACY_MEMORY_SCHEMA))?;
        steps.wrap(
            step,
            connection.execute(
                "INSERT INTO records \
                 (memory_id, scope_kind, scope_key, kind, title, description, body, tags, \
                  provenance, trust, status_kind, content_hash, created_at_ms, \
                  updated_at_ms, usage_stats) \
                 VALUES ('legacy-mem-active', 'identity', 'conformance:memory', 'fact', \
                         'Legacy active title', '', 'legacy active body', '[]', \
                         '{\"source\":\"legacy-fixture\"}', 'application', 'active', \
                         'legacy-hash-active', 1000, 1000, '{}')",
                [],
            ),
        )?;
    }

    // --- realm connection is refused typed --------------------------------------
    let step = "pre_ledger_realm_is_refused";
    let store = steps.wrap(step, SqliteAgentMemoryStore::open(root))?;
    let identity = steps.wrap(step, AgentIdentity::parse("conformance:upgrade"))?;
    if store
        .remember(
            "default",
            &identity,
            NewAgentMemory {
                title: "Upgrade trigger".to_string(),
                body: "Written to force the realm connection open.".to_string(),
                tags: Vec::new(),
            },
        )
        .await
        .is_ok()
    {
        return Err(steps.fail(
            step,
            "a pre-ledger memory realm must refuse typed at the realm connection: \
             unledgered owned tables are below the mobkit 0.8.8 floor",
        ));
    }

    // --- refusal left the legacy rows untouched ----------------------------------
    let step = "refusal_preserves_rows";
    let connection = steps.wrap(step, rusqlite::Connection::open(&db_path))?;
    let (title, body): (String, String) = steps.wrap(
        step,
        connection.query_row(
            "SELECT title, body FROM records WHERE memory_id = 'legacy-mem-active'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ),
    )?;
    steps.ensure(
        step,
        title == "Legacy active title" && body == "legacy active body",
        "the refused realm connection must leave legacy record content unchanged",
    )?;
    let has_quarantine_column: bool = steps.wrap(
        step,
        connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('records') \
             WHERE name = 'ever_quarantined')",
            [],
            |row| row.get(0),
        ),
    )?;
    steps.ensure(
        step,
        !has_quarantine_column,
        "the refusal must not half-apply the retired ensure_column upgrade",
    )?;
    Ok(())
}
