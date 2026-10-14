//! Supported operator repair for mob-member durable transcripts (task #63):
//! drop superseded System rows from a session's durable transcript through
//! the typed rewrite door - one full-range audited rewrite commit composed
//! onto the retained graph, never a hand edit of store rows.
//!
//! Selection modes:
//! - DEFAULT: content-equality dedup - group System rows by
//!   (content, identity), keep the FIRST occurrence of each distinct group,
//!   drop the rest. Compares the system CONTENT, never the serialized
//!   envelope: each materialized copy carries its own `created_at`
//!   (verified in the field: 12 differing bytes, all inside the timestamp),
//!   so envelope hashing sees every copy as distinct.
//! - `--keep-newest <N>`: recency pruning - keep only the LAST N System
//!   rows by position, drop every earlier one. Strictly more destructive
//!   (discards genuinely different superseded prompt versions); an explicit
//!   operator lever, never the default.
//! - `--truncate-tool-results <max_bytes>`: tool-result bounding - replace
//!   the content of every ToolResult entry whose serialized bytes exceed
//!   the bound with a typed truncation marker plus a UTF-8-safe kept prefix
//!   of the original text projection (one production member, 2026-08-14: 72.4%
//!   of a 3.75 MB member strand was tool_results the System modes could not
//!   touch).
//! - `--drop-tool-results-older-than <N>`: tail-window elision - replace
//!   the content of every ToolResult entry more than N messages from the
//!   transcript tail with a typed elision marker.
//!
//! The tool-result modes edit ToolResults rows IN PLACE and never remove a
//! row: removing one would orphan the paired assistant tool_use block and
//! break provider replay. `tool_use_id` and `is_error` always survive, and
//! System/User rows are never touched (the selector matches only
//! `Message::ToolResults`). Marker-led entries are never re-ground, so a
//! second pass over an already-bounded strand reports nothing_to_do. The two
//! tool-result flags may combine (elision wins where both select an entry);
//! combining either with `--keep-newest` is a usage error.
//!
//! Procedure (single-writer discipline, unchanged from the field-proven
//! window-4/5 runs):
//!   1. STOP the gateway.
//!   2. Back up continuity.db (byte copy).
//!   3. Dry run:  meta-repair --db /path/to/continuity.db --all-sessions
//!   4. Apply:    ... --apply
//!   5. Remove the runtime scratch store (runtime.db file set) so the next
//!      boot mints runtime authority from the healed durable rows (the
//!      fleet-proven reset-reseed lane).
//!   6. Start the gateway.
//!
//! Fleet passes commit PER SESSION atomically: a typed refusal on one
//! session is recorded in the report and the pass continues, so a refusal
//! on member 7 never strands members 8-15 in an unknown state.
//!
//! Output: one JSON report document on stdout (and to `--report <path>` if
//! given); human progress goes to stderr. Exit code is 0 when every
//! selected session ended in a non-refused outcome, 1 otherwise.
//!
//! Byte semantics (field-misread twice, so stated here and in the report):
//! `bytes_before`/`bytes_after` measure the live HEAD transcript - the
//! strand a resume actually serves and the provider actually reads - NOT
//! the on-disk file. Audited rewrites RETAIN prior strands by design, so
//! the store file does not shrink after a successful apply; the head does.
//! This is a head-size repair (provider-input pressure), not a disk-space
//! repair.
//!
//! Composition manifests (`--composition-manifests <path> [--apply]`): a
//! separate mode for a store created by the predecessor (pre-0.9) release,
//! whose mob composition manifest (`*.composition.json` beside the mob
//! storage) still spells its creator field `created_by_mobkit`. `<path>` is
//! one manifest, or a state directory whose top-level manifests are all
//! inspected. Dry run by default: each manifest is reported `current`,
//! `predecessor` or `refused` (with the reason; nothing changed). `--apply`
//! renames the field of each `predecessor` manifest in place (reported
//! `rewritten`), keeping every other byte, the recorded definition included;
//! a manifest whose rename changed something before failing (replaced but
//! not yet durable, or a stage left behind) is reported `failed`, with the
//! reason. Predecessor adoption (`meta_gateway storage-adopt-predecessor`)
//! does the same as one of its steps; this mode serves a store adopted
//! before that step existed, and a manifest outside a state directory. Stop
//! every process using the store first. It cannot combine with `--db` or the
//! session modes. Exit code 0 when no manifest was refused or failed, 1
//! otherwise.

use std::sync::Arc;

use meerkat_meta::identity_first::{
    ContinuitySessionStoreAdapter, ContinuityStore, LocalContinuityStore, SessionRuntimeState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectionMode {
    ContentDedup,
    KeepNewest(usize),
}

/// Byte/window bounds for the tool-result modes. At least one is `Some`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ToolResultBounds {
    truncate_max_bytes: Option<usize>,
    drop_older_than: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepairMode {
    System(SelectionMode),
    ToolResults(ToolResultBounds),
}

/// Grep-stable lead-in shared by both tool-result markers. Doubles as the
/// idempotency guard: a single-Text-block entry starting with this prefix
/// was bounded by an earlier pass and is never re-ground (re-grinding would
/// shave more bytes on every pass and overwrite the recorded provenance).
const TOOL_RESULT_MARKER_PREFIX: &str = "[meta-repair:tool-result-";

#[derive(Debug, serde::Serialize)]
struct SessionReport {
    session_id: String,
    identity: Option<String>,
    outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    rows_before: Option<usize>,
    rows_after: Option<usize>,
    bytes_before: Option<usize>,
    bytes_after: Option<usize>,
    system_rows_before: Option<usize>,
    system_rows_after: Option<usize>,
    /// Tool-result bounding accounting, present only for the tool-result
    /// modes so System-mode reports keep their existing shape.
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_result_messages: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_results_bounded: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_result_bytes_before: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_result_bytes_after: Option<usize>,
}

/// Per-entry accounting for one session's tool-result bounding plan.
#[derive(Debug, Clone, Copy)]
struct ToolResultAccounting {
    messages: usize,
    bounded: usize,
    bytes_before: usize,
    bytes_after: usize,
}

#[derive(Debug, serde::Serialize)]
struct RepairReport {
    mode: String,
    applied: bool,
    sessions: Vec<SessionReport>,
    refused: usize,
    /// Constant clarifier shipped IN the report because it was misread twice
    /// in the field: bytes measure the healed HEAD strand, not the file.
    bytes_semantics: &'static str,
}

const BYTES_SEMANTICS: &str = "bytes_before/bytes_after measure the live HEAD transcript (what \
     a resume serves and the provider reads), not the on-disk file: audited rewrites retain \
     prior strands by design, so the store file does not shrink after apply";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    match run().await {
        Ok(exit) => std::process::exit(exit),
        Err(error) => {
            eprintln!("meta-repair: {error}");
            std::process::exit(2);
        }
    }
}

async fn run() -> Result<i32, Box<dyn std::error::Error>> {
    let mut db: Option<String> = None;
    let mut sessions: Vec<String> = Vec::new();
    let mut all_sessions = false;
    let mut keep_newest: Option<usize> = None;
    let mut truncate_tool_results: Option<usize> = None;
    let mut drop_tool_results_older_than: Option<usize> = None;
    let mut composition_manifests: Option<String> = None;
    let mut apply = false;
    let mut report_path: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => db = args.next(),
            "--composition-manifests" => {
                composition_manifests = Some(
                    args.next()
                        .ok_or("--composition-manifests requires a manifest or a directory")?,
                );
            }
            "--session" => {
                sessions.push(args.next().ok_or("--session requires a value")?);
            }
            "--all-sessions" => all_sessions = true,
            "--keep-newest" => {
                let value = args.next().ok_or("--keep-newest requires a value")?;
                let n: usize = value
                    .parse()
                    .map_err(|_| format!("--keep-newest must be a number: {value}"))?;
                if n == 0 {
                    return Err("--keep-newest must be >= 1 (a session keeps at least \
                                its newest System row)"
                        .into());
                }
                keep_newest = Some(n);
            }
            "--truncate-tool-results" => {
                let value = args
                    .next()
                    .ok_or("--truncate-tool-results requires a value")?;
                let n: usize = value.parse().map_err(|_| {
                    format!("--truncate-tool-results must be a byte count: {value}")
                })?;
                if n == 0 {
                    return Err("--truncate-tool-results must be >= 1 byte".into());
                }
                truncate_tool_results = Some(n);
            }
            "--drop-tool-results-older-than" => {
                let value = args
                    .next()
                    .ok_or("--drop-tool-results-older-than requires a value")?;
                let n: usize = value.parse().map_err(|_| {
                    format!("--drop-tool-results-older-than must be a message count: {value}")
                })?;
                if n == 0 {
                    return Err("--drop-tool-results-older-than must be >= 1 (the \
                                tail row's own results always stay)"
                        .into());
                }
                drop_tool_results_older_than = Some(n);
            }
            "--apply" => apply = true,
            "--report" => report_path = args.next(),
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    if let Some(target) = composition_manifests {
        if db.is_some()
            || all_sessions
            || !sessions.is_empty()
            || keep_newest.is_some()
            || truncate_tool_results.is_some()
            || drop_tool_results_older_than.is_some()
        {
            return Err(
                "--composition-manifests repairs manifests only; it cannot combine \
                        with --db, the session selectors or a transcript mode"
                    .into(),
            );
        }
        return repair_composition_manifests(&target, apply, report_path.as_deref());
    }
    let db = db.ok_or("--db <path to the continuity store> is required")?;
    if all_sessions && !sessions.is_empty() {
        return Err("--all-sessions and --session are mutually exclusive".into());
    }
    if !all_sessions && sessions.is_empty() {
        return Err("select sessions with --session <uuid> (repeatable) or --all-sessions".into());
    }
    let tool_result_bounds = match (truncate_tool_results, drop_tool_results_older_than) {
        (None, None) => None,
        (truncate_max_bytes, drop_older_than) => Some(ToolResultBounds {
            truncate_max_bytes,
            drop_older_than,
        }),
    };
    if keep_newest.is_some() && tool_result_bounds.is_some() {
        return Err("--keep-newest selects System rows; it cannot combine with \
                    the tool-result bounding flags"
            .into());
    }
    let mode = match tool_result_bounds {
        Some(bounds) => RepairMode::ToolResults(bounds),
        None => RepairMode::System(
            keep_newest.map_or(SelectionMode::ContentDedup, SelectionMode::KeepNewest),
        ),
    };

    let local = Arc::new(LocalContinuityStore::open(&db)?);
    let targets: Vec<(Option<String>, meerkat_core::types::SessionId)> = if all_sessions {
        local
            .list_session_bindings()
            .await?
            .into_iter()
            .map(|(identity, session_id)| (Some(identity.to_string()), session_id))
            .collect()
    } else {
        let mut targets = Vec::new();
        for raw in &sessions {
            let session_id = meerkat_core::types::SessionId::parse(raw)
                .map_err(|e| format!("--session must be a session UUID: {e}"))?;
            targets.push((None, session_id));
        }
        targets
    };
    if targets.is_empty() {
        return Err("the continuity store holds no session bindings".into());
    }

    let continuity: Arc<dyn ContinuityStore> = local;
    let mut reports = Vec::with_capacity(targets.len());
    let mut refused = 0usize;
    for (identity_hint, session_id) in targets {
        eprintln!("meta-repair: session {session_id} ...");
        match repair_session(&continuity, &session_id, mode, apply).await {
            Ok(report) => reports.push(report),
            Err(error) => {
                refused += 1;
                eprintln!("meta-repair: session {session_id} REFUSED: {error}");
                reports.push(SessionReport {
                    session_id: session_id.to_string(),
                    identity: identity_hint,
                    outcome: "refused".to_string(),
                    detail: Some(error.to_string()),
                    rows_before: None,
                    rows_after: None,
                    bytes_before: None,
                    bytes_after: None,
                    system_rows_before: None,
                    system_rows_after: None,
                    tool_result_messages: None,
                    tool_results_bounded: None,
                    tool_result_bytes_before: None,
                    tool_result_bytes_after: None,
                });
            }
        }
    }

    let report = RepairReport {
        mode: match mode {
            RepairMode::System(SelectionMode::ContentDedup) => "content_dedup".to_string(),
            RepairMode::System(SelectionMode::KeepNewest(n)) => format!("keep_newest_{n}"),
            RepairMode::ToolResults(bounds) => {
                let mut parts = Vec::new();
                if let Some(n) = bounds.truncate_max_bytes {
                    parts.push(format!("truncate_tool_results_{n}"));
                }
                if let Some(n) = bounds.drop_older_than {
                    parts.push(format!("drop_tool_results_older_than_{n}"));
                }
                parts.join("+")
            }
        },
        applied: apply,
        sessions: reports,
        refused,
        bytes_semantics: BYTES_SEMANTICS,
    };
    let rendered = serde_json::to_string_pretty(&report)?;
    println!("{rendered}");
    if let Some(path) = report_path {
        std::fs::write(&path, &rendered)?;
        eprintln!("meta-repair: report written to {path}");
    }
    if apply
        && report
            .sessions
            .iter()
            .any(|session| session.outcome == "applied")
    {
        // Step 5 of the procedure is LOAD-BEARING and easy to skip (called
        // out by two production operators): the runtime scratch store still
        // projects the PRE-repair transcript, and a boot that reads it will
        // serve the bloated head as if the repair never happened. Say it
        // loudly at the exact moment the operator is about to restart.
        eprintln!();
        eprintln!("meta-repair: APPLY COMPLETE - ONE STEP REMAINS BEFORE RESTARTING THE GATEWAY:");
        eprintln!(
            "meta-repair:   remove the runtime scratch store (the file set next to this \
             continuity store; `rkat storage doctor` names it)."
        );
        eprintln!(
            "meta-repair:   It still projects the PRE-repair transcript; the next boot must \
             mint runtime authority from the healed durable rows (the reset-reseed lane). \
             Skipping this step makes the repair appear to have not happened."
        );
        eprintln!(
            "meta-repair:   Note: the store FILE does not shrink - audited rewrites retain \
             prior strands; the healed HEAD is what got smaller (see bytes_semantics in the \
             report)."
        );
    }
    Ok(i32::from(refused != 0))
}

/// One manifest's line in the composition manifest report.
#[derive(Debug, serde::Serialize)]
struct ManifestReport {
    path: String,
    /// `current`, `predecessor`, `rewritten`, `refused` (nothing changed) or
    /// `failed` (something changed: see the detail).
    outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct ManifestRepairReport {
    mode: &'static str,
    applied: bool,
    manifests: Vec<ManifestReport>,
    refused: usize,
    failed: usize,
}

/// The `--composition-manifests` mode: inspect, and with `apply` rename, the
/// predecessor creator field of one manifest or of every top-level manifest
/// in a directory.
fn repair_composition_manifests(
    target: &str,
    apply: bool,
    report_path: Option<&str>,
) -> Result<i32, Box<dyn std::error::Error>> {
    let target = std::path::Path::new(target);
    let manifests = if target.is_dir() {
        meerkat_meta::storage_predecessor::composition_manifests_in(target)?
    } else {
        vec![target.to_path_buf()]
    };
    if manifests.is_empty() {
        eprintln!(
            "meta-repair: no composition manifest (*.composition.json) in {}",
            target.display()
        );
    }
    let mode = if apply {
        meerkat_meta::MigrateMode::Apply
    } else {
        meerkat_meta::MigrateMode::DryRun
    };
    let mut refused = 0usize;
    let mut failed = 0usize;
    let mut reports = Vec::with_capacity(manifests.len());
    for manifest in manifests {
        let path = manifest.display().to_string();
        match meerkat_meta::storage_predecessor::repair_composition_manifest(&manifest, mode) {
            Ok(outcome) => {
                let outcome = serde_json::to_value(outcome)?
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                eprintln!("meta-repair: {path}: {outcome}");
                reports.push(ManifestReport {
                    path,
                    outcome,
                    detail: None,
                });
            }
            Err(error) => {
                let counter = match error {
                    meerkat_meta::storage_predecessor::ManifestRepairError::Refused(_) => {
                        &mut refused
                    }
                    meerkat_meta::storage_predecessor::ManifestRepairError::Failed(_) => {
                        &mut failed
                    }
                };
                *counter += 1;
                let outcome = error.outcome();
                eprintln!(
                    "meta-repair: {path} {}: {error}",
                    outcome.to_ascii_uppercase()
                );
                reports.push(ManifestReport {
                    path,
                    outcome: outcome.to_string(),
                    detail: Some(error.detail().to_string()),
                });
            }
        }
    }
    let rendered = serde_json::to_string_pretty(&ManifestRepairReport {
        mode: "composition_manifests",
        applied: apply,
        manifests: reports,
        refused,
        failed,
    })?;
    println!("{rendered}");
    if let Some(path) = report_path {
        std::fs::write(path, &rendered)?;
        eprintln!("meta-repair: report written to {path}");
    }
    Ok(i32::from(refused + failed != 0))
}

/// Repair one session through the typed rewrite door. Every failure is a
/// typed refusal returned to the fleet loop; the durable row is preserved
/// by the door's own guards on every refused path.
async fn repair_session(
    continuity: &Arc<dyn ContinuityStore>,
    session_id: &meerkat_core::types::SessionId,
    mode: RepairMode,
    apply: bool,
) -> Result<SessionReport, Box<dyn std::error::Error>> {
    let adapter = Arc::new(ContinuitySessionStoreAdapter::new(Arc::clone(continuity)));

    // Write authority = the durable continuity record, exactly the facts a
    // registered resume would carry (the parked-repair contract).
    let (record, fencing_token, fence_current) = continuity
        .resolve_record_by_session(session_id)
        .await?
        .ok_or("no continuity record binds this session")?;
    adapter
        .register_session(
            session_id,
            SessionRuntimeState {
                identity: record.identity.clone(),
                generation: record.generation,
                fencing_token,
                checkpoint_version: fence_current,
            },
        )
        .await?;

    // Load the durable session (slim: content only), then HYDRATE the
    // out-of-line rewrite graph from the store's own rewrite rows onto it.
    // Slim materializations drop the compact graph by design - a rewrite
    // committed on one composes at generation 1 and the store refuses it
    // against the retained chain. With the store-proved graph installed the
    // rewrite composes at the correct next generation.
    let mut session = meerkat::SessionStore::load(adapter.as_ref(), session_id)
        .await?
        .ok_or("no durable row for this session")?;
    let channel = continuity
        .as_incremental_sessions()
        .ok_or("the continuity store provides no incremental channel")?;
    let rewrite_records = channel.load_rewrites(session_id).await?;
    if let Some(validated) =
        meerkat_core::ValidatedTranscriptHistory::from_rewrite_records_with_proved(
            rewrite_records,
            None,
        )?
    {
        session.install_validated_audited_transcript_history_preserving_live(validated)?;
    }

    let messages = session.messages();
    let serialized: Vec<Vec<u8>> = messages
        .iter()
        .map(serde_json::to_vec)
        .collect::<Result<_, _>>()?;
    let values: Vec<serde_json::Value> = serialized
        .iter()
        .map(|bytes| serde_json::from_slice(bytes))
        .collect::<Result<_, _>>()?;
    let is_system = |value: &serde_json::Value| -> bool {
        value
            .get("role")
            .and_then(|r| r.as_str())
            .map(|r| r == "system")
            .unwrap_or(false)
    };
    let system_positions: Vec<usize> = values
        .iter()
        .enumerate()
        .filter(|(_, value)| is_system(value))
        .map(|(index, _)| index)
        .collect();

    let rows_before = messages.len();
    let bytes_before: usize = serialized.iter().map(Vec::len).sum();
    let system_before = system_positions.len();

    // Per-mode plan: the transcript to commit, the resulting System count,
    // and (tool-result modes only) the per-entry accounting.
    let (cleaned, system_after, tool_accounting) = match mode {
        RepairMode::System(selection) => {
            let duplicate_indices: Vec<usize> = match selection {
                SelectionMode::ContentDedup => {
                    // Group System rows by (content, identity), ignoring only the
                    // envelope timestamp; keep the FIRST occurrence per group.
                    let mut seen_groups: std::collections::HashMap<String, usize> =
                        std::collections::HashMap::new();
                    let mut duplicates = Vec::new();
                    for &index in &system_positions {
                        let value = &values[index];
                        let key = serde_json::to_string(&serde_json::json!({
                            "content": value.get("content"),
                            "identity": value.get("identity"),
                        }))?;
                        match seen_groups.entry(key) {
                            std::collections::hash_map::Entry::Vacant(slot) => {
                                slot.insert(index);
                            }
                            std::collections::hash_map::Entry::Occupied(_) => {
                                duplicates.push(index);
                            }
                        }
                    }
                    duplicates
                }
                SelectionMode::KeepNewest(n) => {
                    let cut = system_positions.len().saturating_sub(n);
                    system_positions[..cut].to_vec()
                }
            };
            if duplicate_indices.is_empty() {
                eprintln!(
                    "meta-repair: session {session_id}: {system_before} System row(s), \
                     nothing to drop"
                );
                return Ok(SessionReport {
                    session_id: session_id.to_string(),
                    identity: Some(record.identity.to_string()),
                    outcome: "nothing_to_do".to_string(),
                    detail: None,
                    rows_before: Some(rows_before),
                    rows_after: Some(rows_before),
                    bytes_before: Some(bytes_before),
                    bytes_after: Some(bytes_before),
                    system_rows_before: Some(system_before),
                    system_rows_after: Some(system_before),
                    tool_result_messages: None,
                    tool_results_bounded: None,
                    tool_result_bytes_before: None,
                    tool_result_bytes_after: None,
                });
            }
            let cleaned: Vec<meerkat_core::Message> = messages
                .iter()
                .enumerate()
                .filter(|(index, _)| !duplicate_indices.contains(index))
                .map(|(_, message)| message.clone())
                .collect();
            let system_after = system_before - duplicate_indices.len();
            (cleaned, system_after, None)
        }
        RepairMode::ToolResults(bounds) => {
            let plan = plan_tool_result_bounds(messages, bounds)?;
            let accounting = ToolResultAccounting {
                messages: plan.tool_result_messages,
                bounded: plan.bounded,
                bytes_before: plan.tool_bytes_before,
                bytes_after: plan.tool_bytes_after,
            };
            if plan.bounded == 0 {
                eprintln!(
                    "meta-repair: session {session_id}: {} tool-result row(s), \
                     nothing to bound",
                    accounting.messages
                );
                return Ok(SessionReport {
                    session_id: session_id.to_string(),
                    identity: Some(record.identity.to_string()),
                    outcome: "nothing_to_do".to_string(),
                    detail: None,
                    rows_before: Some(rows_before),
                    rows_after: Some(rows_before),
                    bytes_before: Some(bytes_before),
                    bytes_after: Some(bytes_before),
                    system_rows_before: Some(system_before),
                    system_rows_after: Some(system_before),
                    tool_result_messages: Some(accounting.messages),
                    tool_results_bounded: Some(0),
                    tool_result_bytes_before: Some(accounting.bytes_before),
                    tool_result_bytes_after: Some(accounting.bytes_after),
                });
            }
            // System rows are out of scope for this mode by construction:
            // the planner rewrites only `Message::ToolResults` rows.
            (plan.cleaned, system_before, Some(accounting))
        }
    };

    let rows_after = cleaned.len();
    let bytes_after: usize = cleaned
        .iter()
        .map(|m| serde_json::to_vec(m).map(|v| v.len()).unwrap_or(0))
        .sum();
    eprintln!(
        "meta-repair: session {session_id}: plan {rows_before} -> {rows_after} rows, \
         ~{bytes_before} -> ~{bytes_after} bytes, System {system_before} -> {system_after}"
    );
    if let Some(tool) = tool_accounting {
        eprintln!(
            "meta-repair: session {session_id}: bounding {} tool-result entry(ies) \
             across {} tool-result row(s) in place, ~{} -> ~{} tool-result bytes",
            tool.bounded, tool.messages, tool.bytes_before, tool.bytes_after
        );
    }

    if !apply {
        return Ok(SessionReport {
            session_id: session_id.to_string(),
            identity: Some(record.identity.to_string()),
            outcome: "dry_run".to_string(),
            detail: None,
            rows_before: Some(rows_before),
            rows_after: Some(rows_after),
            bytes_before: Some(bytes_before),
            bytes_after: Some(bytes_after),
            system_rows_before: Some(system_before),
            system_rows_after: Some(system_after),
            tool_result_messages: tool_accounting.map(|t| t.messages),
            tool_results_bounded: tool_accounting.map(|t| t.bounded),
            tool_result_bytes_before: tool_accounting.map(|t| t.bytes_before),
            tool_result_bytes_after: tool_accounting.map(|t| t.bytes_after),
        });
    }

    let (rewrite_reason, rewrite_provenance) = match mode {
        RepairMode::System(_) => (
            "operator prune of superseded System rows (meta-repair)",
            "meta-repair/system-rows",
        ),
        RepairMode::ToolResults(_) => (
            "operator bound of tool-result bulk (meta-repair)",
            "meta-repair/tool-results",
        ),
    };
    let parent_revision = session.transcript_revision()?;
    let selection_end = messages.len();
    let mut rewritten = session.clone();
    let commit = rewritten.commit_transcript_rewrite(
        meerkat_core::TranscriptRewriteSelection::MessageRange {
            start: 0,
            end: selection_end,
        },
        cleaned,
        meerkat_core::TranscriptRewriteReason::new(rewrite_reason),
        Some(rewrite_provenance.to_string()),
        Some(parent_revision),
    )?;
    meerkat::SessionStore::save_transcript_rewrite(adapter.as_ref(), &rewritten, &commit).await?;
    meerkat::SessionStore::save_authoritative_projection(adapter.as_ref(), &rewritten).await?;
    eprintln!(
        "meta-repair: session {session_id}: APPLIED at rewrite generation {}",
        commit.rewrite_generation
    );
    Ok(SessionReport {
        session_id: session_id.to_string(),
        identity: Some(record.identity.to_string()),
        outcome: "applied".to_string(),
        detail: None,
        rows_before: Some(rows_before),
        rows_after: Some(rows_after),
        bytes_before: Some(bytes_before),
        bytes_after: Some(bytes_after),
        system_rows_before: Some(system_before),
        system_rows_after: Some(system_after),
        tool_result_messages: tool_accounting.map(|t| t.messages),
        tool_results_bounded: tool_accounting.map(|t| t.bounded),
        tool_result_bytes_before: tool_accounting.map(|t| t.bytes_before),
        tool_result_bytes_after: tool_accounting.map(|t| t.bytes_after),
    })
}

/// One session's computed tool-result bounding plan.
struct ToolResultBoundsPlan {
    cleaned: Vec<meerkat_core::Message>,
    tool_result_messages: usize,
    bounded: usize,
    tool_bytes_before: usize,
    tool_bytes_after: usize,
}

/// A single-Text-block entry led by the tool's own marker was bounded by an
/// earlier pass; re-grinding it would shave more bytes on every run and
/// overwrite the recorded original_bytes provenance.
fn is_already_bounded(result: &meerkat_core::types::ToolResult) -> bool {
    match result.content.as_slice() {
        [meerkat_core::types::ContentBlock::Text { text }] => {
            text.starts_with(TOOL_RESULT_MARKER_PREFIX)
                || text.starts_with(
                    meerkat_meta::storage_predecessor::PREDECESSOR_REPAIR_MARKER_PREFIX,
                )
        }
        _ => false,
    }
}

/// Compute the bounded transcript for the tool-result modes.
///
/// Only `Message::ToolResults` rows are rewritten - every other row is cloned
/// through untouched, so System and User rows cannot be affected. Entries are
/// bounded IN PLACE (`tool_use_id` and `is_error` survive) because removing a
/// row would orphan the paired assistant tool_use block and break provider
/// replay. Elision (the tail-window lever) wins over truncation where both
/// select an entry. Byte counts measure each entry's serialized JSON; the
/// truncation bound therefore triggers on envelope size but keeps at most
/// `max_bytes` of the original text projection (UTF-8-safe cut) under the
/// marker, so multimodal bulk (image/video blocks) is bounded too.
fn plan_tool_result_bounds(
    messages: &[meerkat_core::Message],
    bounds: ToolResultBounds,
) -> Result<ToolResultBoundsPlan, serde_json::Error> {
    let total = messages.len();
    let mut cleaned = Vec::with_capacity(total);
    let mut tool_result_messages = 0usize;
    let mut bounded = 0usize;
    let mut tool_bytes_before = 0usize;
    let mut tool_bytes_after = 0usize;
    for (index, message) in messages.iter().enumerate() {
        let meerkat_core::Message::ToolResults {
            results,
            created_at,
        } = message
        else {
            cleaned.push(message.clone());
            continue;
        };
        tool_result_messages += 1;
        // Distance 1 is the tail row itself: `--drop-tool-results-older-than N`
        // elides strictly beyond the newest N transcript positions.
        let distance_from_tail = total - index;
        let elide = bounds
            .drop_older_than
            .is_some_and(|n| distance_from_tail > n);
        let mut bounded_results = Vec::with_capacity(results.len());
        for result in results {
            let entry_bytes = serde_json::to_vec(result)?.len();
            tool_bytes_before += entry_bytes;
            let replacement = if is_already_bounded(result) {
                None
            } else if elide {
                Some(format!(
                    "{TOOL_RESULT_MARKER_PREFIX}elided original_bytes={entry_bytes}]"
                ))
            } else {
                match bounds.truncate_max_bytes {
                    Some(max_bytes) if entry_bytes > max_bytes => {
                        let text = result.text_content();
                        let mut cut = max_bytes.min(text.len());
                        while cut > 0 && !text.is_char_boundary(cut) {
                            cut -= 1;
                        }
                        Some(format!(
                            "{TOOL_RESULT_MARKER_PREFIX}truncated \
                             original_bytes={entry_bytes} kept_bytes={cut}]\n{}",
                            &text[..cut]
                        ))
                    }
                    _ => None,
                }
            };
            let entry = match replacement {
                Some(marker_text) => {
                    bounded += 1;
                    let mut bounded_entry = result.clone();
                    bounded_entry.set_text_content(marker_text);
                    bounded_entry
                }
                None => result.clone(),
            };
            tool_bytes_after += serde_json::to_vec(&entry)?.len();
            bounded_results.push(entry);
        }
        cleaned.push(meerkat_core::Message::ToolResults {
            results: bounded_results,
            created_at: *created_at,
        });
    }
    Ok(ToolResultBoundsPlan {
        cleaned,
        tool_result_messages,
        bounded,
        tool_bytes_before,
        tool_bytes_after,
    })
}
