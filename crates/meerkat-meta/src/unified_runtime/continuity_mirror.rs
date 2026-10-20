//! A live copy of each durable identity's current continuity record and
//! committed session, in the JSON Lines shape the continuity export's
//! `jsonl_provider` reads (`tools/continuity-export`).
//!
//! It is a restore source for a lost state directory and is never read at
//! boot. Restore runs the latest rows through `continuity_export.py --provider
//! jsonl_provider:from_env` and `meta_gateway storage-migrate-native` into a
//! fresh state directory.
//!
//! The runtime stays the only authority, and owns the mirror when the builder
//! installs one ([`crate::UnifiedRuntimeBuilder::continuity_mirror`]).
//! - The mirror reads Core's durable member bindings and each bound session
//!   through the runtime's non-owning bridge and its session service.
//! - It keeps, per identity, the record, the session's resume authority and
//!   the document's digest, never the document.
//! - A round re-reads a session only when its authority changed, and hands
//!   the sink the records plus the changed documents.
//! - A failure on one identity is counted and logged, and that identity keeps
//!   its last rows; the others go on.
//! - A put that fails, panics or outlives its deadline is counted, and the
//!   next round retries.
//! - Shutdown writes a last round within a deadline; dropping the runtime
//!   aborts the mirror without one.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use base64::Engine as _;
use futures::FutureExt as _;
use meerkat_mob::{MobSessionService, ResumeSessionLoad, SessionResumeAuthority};
use sha2::{Digest as _, Sha256};

use crate::identity_first::bridge::SessionBridge;

/// How often the mirror runs a round, and how long a put and the last round
/// at shutdown may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinuityMirrorTimings {
    pub interval: Duration,
    /// Past it a put counts as failed and the next round retries.
    pub put_deadline: Duration,
    /// Past it shutdown aborts the last round and goes on.
    pub stop_deadline: Duration,
}

impl Default for ContinuityMirrorTimings {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            put_deadline: Duration::from_mins(1),
            stop_deadline: Duration::from_secs(30),
        }
    }
}

/// One identity's row in the records file: the five fields of the export's
/// record. The import reads `identity` and `session_id`; it reads the other
/// three only for its report, so they carry the binding's generation, a
/// runtime id derived from it, and checkpoint version 0.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContinuityMirrorRecord {
    pub identity: String,
    pub agent_runtime_id: String,
    pub session_id: String,
    pub generation: u64,
    pub checkpoint_version: u64,
}

/// One row in the sessions file: the session id and the persisted session
/// document's exact bytes in standard base64.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContinuityMirrorSession {
    pub session_id: String,
    pub data: String,
}

/// What one put hands the sink.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContinuityMirrorUpdate {
    /// Every mirrored identity's record, sorted by identity.
    pub records: Vec<ContinuityMirrorRecord>,
    /// The sessions whose document changed since the last accepted put, sorted
    /// by id; every session on the first put after a start.
    pub changed_sessions: Vec<ContinuityMirrorSession>,
    /// Every session a record names. A sink keeps no other session.
    pub session_ids: BTreeSet<String>,
}

/// Where the mirror writes.
#[async_trait::async_trait]
pub trait ContinuityMirrorSink: Send + Sync {
    async fn put(&self, update: &ContinuityMirrorUpdate) -> Result<(), String>;
}

/// Writes `records.jsonl` and `sessions.jsonl` into a directory. Each file is
/// written to a `.partial` file, synced and renamed, then the directory is
/// synced. The sessions file first takes the union of its old rows and the
/// changed ones, then the records file is replaced, then the sessions file is
/// trimmed to the sessions the records name: a reader never finds a record
/// whose session is missing, even after a crash between the steps.
pub struct JsonlFileMirrorSink {
    dir: PathBuf,
}

impl JsonlFileMirrorSink {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn put_blocking(&self, update: &ContinuityMirrorUpdate) -> Result<(), String> {
        std::fs::create_dir_all(&self.dir)
            .map_err(|error| format!("{}: {error}", self.dir.display()))?;
        let mut sessions = read_sessions(&self.dir.join("sessions.jsonl"))?;
        for session in &update.changed_sessions {
            sessions.insert(session.session_id.clone(), session.clone());
        }
        write_jsonl(&self.dir, "sessions.jsonl", sessions.values())?;
        write_jsonl(&self.dir, "records.jsonl", update.records.iter())?;
        sessions.retain(|session_id, _| update.session_ids.contains(session_id));
        write_jsonl(&self.dir, "sessions.jsonl", sessions.values())
    }
}

#[async_trait::async_trait]
impl ContinuityMirrorSink for JsonlFileMirrorSink {
    async fn put(&self, update: &ContinuityMirrorUpdate) -> Result<(), String> {
        let sink = Self {
            dir: self.dir.clone(),
        };
        let update = update.clone();
        tokio::task::spawn_blocking(move || sink.put_blocking(&update))
            .await
            .map_err(|error| format!("the file sink's writer ended: {error}"))?
    }
}

fn read_sessions(path: &Path) -> Result<BTreeMap<String, ContinuityMirrorSession>, String> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    let mut sessions = BTreeMap::new();
    for (index, line) in text.lines().enumerate() {
        let session: ContinuityMirrorSession = serde_json::from_str(line)
            .map_err(|error| format!("{} line {}: {error}", path.display(), index + 1))?;
        sessions.insert(session.session_id.clone(), session);
    }
    Ok(sessions)
}

fn write_jsonl<'a, T: serde::Serialize + 'a>(
    dir: &Path,
    name: &str,
    rows: impl Iterator<Item = &'a T>,
) -> Result<(), String> {
    use std::io::Write as _;
    let partial = dir.join(format!(".{name}.partial"));
    let target = dir.join(name);
    let fail = |path: &Path, error: std::io::Error| format!("{}: {error}", path.display());
    let mut file = std::fs::File::create(&partial).map_err(|error| fail(&partial, error))?;
    for row in rows {
        let line = serde_json::to_string(row).map_err(|error| error.to_string())?;
        file.write_all(line.as_bytes())
            .and_then(|()| file.write_all(b"\n"))
            .map_err(|error| fail(&partial, error))?;
    }
    file.sync_all().map_err(|error| fail(&partial, error))?;
    drop(file);
    std::fs::rename(&partial, &target).map_err(|error| fail(&target, error))?;
    std::fs::File::open(dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| fail(dir, error))
}

/// What the mirror reads from: the runtime's non-owning session bridge and
/// its session service. Holding them keeps nothing of the runtime alive.
#[derive(Clone)]
pub struct ContinuityMirrorSource {
    bridge: Arc<dyn SessionBridge>,
    service: Arc<dyn MobSessionService>,
}

/// One mirrored identity, as the mirror keeps it between rounds.
#[derive(Debug, Clone)]
struct Mirrored {
    record: ContinuityMirrorRecord,
    authority: SessionResumeAuthority,
    document_sha256: [u8; 32],
}

/// One identity's outcome in a round.
enum IdentityRead {
    /// The session is unchanged since the last round.
    Unchanged,
    /// A new or changed session, with its row.
    Changed(Mirrored, ContinuityMirrorSession),
    /// No durable session to copy (`Absent`), or an archived one the lifecycle
    /// cannot revive (`ArchivedNotRevivable`): the identity is not mirrored.
    NotMirrored,
}

impl ContinuityMirrorSource {
    pub(crate) fn new(bridge: Arc<dyn SessionBridge>, service: Arc<dyn MobSessionService>) -> Self {
        Self { bridge, service }
    }

    async fn read_identity(
        &self,
        identity: &str,
        binding: &meerkat_mob::MemberBinding,
        session_id: &meerkat_core::types::SessionId,
        previous: Option<&Mirrored>,
    ) -> Result<IdentityRead, String> {
        let generation = binding.generation().get();
        let authority = self
            .service
            .observe_session_resume_authority(session_id)
            .await
            .map_err(|error| format!("session {session_id}: {error}"))?;
        // A service that keeps no store authority (an ephemeral one) answers
        // the empty bundle: then only the document digest below can tell.
        let same_binding = previous.is_some_and(|previous| {
            previous.record.session_id == session_id.to_string()
                && previous.record.generation == generation
        });
        if same_binding
            && authority != SessionResumeAuthority::default()
            && previous.is_some_and(|previous| previous.authority == authority)
        {
            return Ok(IdentityRead::Unchanged);
        }
        let session = match self
            .service
            .load_session_for_resume(session_id)
            .await
            .map_err(|error| format!("session {session_id}: {error}"))?
        {
            ResumeSessionLoad::Active(session) | ResumeSessionLoad::Revivable(session) => session,
            ResumeSessionLoad::ArchivedNotRevivable { .. } | ResumeSessionLoad::Absent => {
                return Ok(IdentityRead::NotMirrored);
            }
            // A load this build does not know: the identity keeps its last rows.
            _ => return Err(format!("session {session_id}: an unknown resume load")),
        };
        let document = session
            .to_persisted_bytes()
            .map_err(|error| format!("session {session_id}: {error}"))?;
        let document_sha256: [u8; 32] = Sha256::digest(&document).into();
        if same_binding
            && previous.is_some_and(|previous| previous.document_sha256 == document_sha256)
        {
            return Ok(IdentityRead::Unchanged);
        }
        let record = ContinuityMirrorRecord {
            identity: identity.to_owned(),
            agent_runtime_id: format!("rt:{identity}:{generation}"),
            session_id: session_id.to_string(),
            generation,
            checkpoint_version: 0,
        };
        let row = ContinuityMirrorSession {
            session_id: session_id.to_string(),
            data: base64::engine::general_purpose::STANDARD.encode(&document),
        };
        Ok(IdentityRead::Changed(
            Mirrored {
                record,
                authority,
                document_sha256,
            },
            row,
        ))
    }
}

/// Counts a mirror keeps. Shutdown and the health surfaces read them.
#[derive(Debug, Default)]
pub struct ContinuityMirrorCounts {
    /// Puts the sink accepted.
    pub writes: AtomicU64,
    /// Rounds or identities that failed: a binding read, a session read, a
    /// put that failed, panicked or passed its deadline.
    pub failures: AtomicU64,
    /// Identities seen with no session to copy: `Absent`, or archived and not
    /// revivable. They keep no rows.
    pub not_mirrored: AtomicU64,
}

/// The mirror's state between rounds.
#[derive(Default)]
struct MirrorState {
    identities: BTreeMap<String, Mirrored>,
    /// Session rows changed since the last accepted put, kept until one is
    /// accepted.
    pending: BTreeMap<String, ContinuityMirrorSession>,
    /// The update the sink last accepted, compared without its documents.
    accepted: Option<(Vec<ContinuityMirrorRecord>, BTreeSet<String>)>,
}

async fn round(
    source: &ContinuityMirrorSource,
    sink: &dyn ContinuityMirrorSink,
    put_deadline: Duration,
    state: &mut MirrorState,
    counts: &ContinuityMirrorCounts,
) {
    let history = match source.bridge.member_binding_history().await {
        Ok(history) => history,
        Err(error) => {
            counts.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(error = %error, "continuity mirror: reading the member bindings failed");
            return;
        }
    };
    let mut identities = BTreeMap::new();
    for binding in history.durable_identity_bindings() {
        let Some(session_id) = binding.session_id() else {
            continue;
        };
        let identity =
            crate::member_comms_id::durable_identity_for_roster_member(binding.identity().as_ref());
        let previous = state.identities.get(&identity);
        match source
            .read_identity(&identity, binding, session_id, previous)
            .await
        {
            Ok(IdentityRead::Unchanged) => {
                if let Some(previous) = previous {
                    identities.insert(identity, previous.clone());
                }
            }
            Ok(IdentityRead::Changed(mirrored, row)) => {
                state.pending.insert(row.session_id.clone(), row);
                identities.insert(identity, mirrored);
            }
            Ok(IdentityRead::NotMirrored) => {
                counts.not_mirrored.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                counts.failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    identity = %identity,
                    error = %error,
                    "continuity mirror: reading an identity failed; it keeps its last rows"
                );
                if let Some(previous) = previous {
                    identities.insert(identity, previous.clone());
                }
            }
        }
    }
    state.identities = identities;
    let records: Vec<_> = state
        .identities
        .values()
        .map(|mirrored| mirrored.record.clone())
        .collect();
    let session_ids: BTreeSet<String> = records
        .iter()
        .map(|record| record.session_id.clone())
        .collect();
    state
        .pending
        .retain(|session_id, _| session_ids.contains(session_id));
    if state.pending.is_empty()
        && state.accepted.as_ref() == Some(&(records.clone(), session_ids.clone()))
    {
        return;
    }
    let update = ContinuityMirrorUpdate {
        records,
        changed_sessions: state.pending.values().cloned().collect(),
        session_ids,
    };
    let put =
        AssertUnwindSafe(tokio::time::timeout(put_deadline, sink.put(&update))).catch_unwind();
    match put.await {
        Ok(Ok(Ok(()))) => {
            counts.writes.fetch_add(1, Ordering::Relaxed);
            state.pending.clear();
            state.accepted = Some((update.records, update.session_ids));
        }
        Ok(Ok(Err(error))) => {
            counts.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(error = %error, "continuity mirror: the sink refused the update");
        }
        Ok(Err(_elapsed)) => {
            counts.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                deadline_ms = put_deadline.as_millis() as u64,
                "continuity mirror: the sink did not answer before its deadline"
            );
        }
        Err(_panic) => {
            counts.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("continuity mirror: the sink panicked");
        }
    }
}

/// A running mirror. The runtime that owns it stops it at shutdown; dropping
/// it aborts the task without a last round.
pub struct ContinuityMirror {
    stop: tokio::sync::watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
    counts: Arc<ContinuityMirrorCounts>,
    stop_deadline: Duration,
}

impl ContinuityMirror {
    /// Run a round now and then every `timings.interval`.
    pub fn spawn(
        source: ContinuityMirrorSource,
        sink: Arc<dyn ContinuityMirrorSink>,
        timings: ContinuityMirrorTimings,
    ) -> Self {
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let counts = Arc::new(ContinuityMirrorCounts::default());
        let task_counts = Arc::clone(&counts);
        let task = tokio::spawn(async move {
            let mut state = MirrorState::default();
            loop {
                round(
                    &source,
                    sink.as_ref(),
                    timings.put_deadline,
                    &mut state,
                    &task_counts,
                )
                .await;
                if *stopped.borrow() {
                    return;
                }
                tokio::select! {
                    _ = tokio::time::sleep(timings.interval) => {}
                    // A stop runs one last round above, then returns.
                    _ = stopped.changed() => {}
                }
            }
        });
        Self {
            stop,
            task: Some(task),
            counts,
            stop_deadline: timings.stop_deadline,
        }
    }

    pub fn counts(&self) -> Arc<ContinuityMirrorCounts> {
        Arc::clone(&self.counts)
    }

    /// Run a last round and end the task, within the stop deadline; past it
    /// the task is aborted and the stop counts as a failure.
    pub async fn stop(mut self) -> Arc<ContinuityMirrorCounts> {
        let _ = self.stop.send(true);
        if let Some(mut task) = self.task.take()
            && tokio::time::timeout(self.stop_deadline, &mut task)
                .await
                .is_err()
        {
            task.abort();
            self.counts.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                deadline_ms = self.stop_deadline.as_millis() as u64,
                "continuity mirror: the last round did not end before its deadline"
            );
        }
        Arc::clone(&self.counts)
    }
}

impl Drop for ContinuityMirror {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl super::UnifiedRuntime {
    /// Start the continuity mirror the builder asked for, owned by this
    /// runtime. It reads through the runtime's own non-owning bridge.
    pub(crate) async fn install_continuity_mirror(
        &self,
        sink: Arc<dyn ContinuityMirrorSink>,
        timings: ContinuityMirrorTimings,
    ) -> Result<(), String> {
        let bridge = self
            .session_bridge
            .clone()
            .ok_or_else(|| "it needs an identity-first runtime (no session bridge)".to_string())?;
        let service = self
            .mob_runtime
            .session_service()
            .cloned()
            .ok_or_else(|| "it needs a session service".to_string())?;
        let mirror =
            ContinuityMirror::spawn(ContinuityMirrorSource::new(bridge, service), sink, timings);
        *self.continuity_mirror.lock().await = Some(mirror);
        Ok(())
    }

    /// The installed continuity mirror's counts, or `None` without one (or
    /// once shutdown has stopped it).
    pub async fn continuity_mirror_counts(&self) -> Option<Arc<ContinuityMirrorCounts>> {
        self.continuity_mirror
            .lock()
            .await
            .as_ref()
            .map(ContinuityMirror::counts)
    }
}
