//! A live copy of each durable identity's current continuity record and
//! committed session, in the JSON Lines shape the continuity export's
//! `jsonl_provider` reads (`tools/continuity-export`).
//!
//! It is a restore source for a lost state directory, never read at boot.
//! Restore runs the latest rows through `continuity_export.py --provider
//! jsonl_provider:from_env` and `meta_gateway storage-migrate-native` into a
//! fresh state directory.
//!
//! The runtime stays the only authority. The mirror reads Core's durable
//! member bindings and each bound session's persisted snapshot through the
//! runtime, and hands the sink a whole snapshot when it differs from the last
//! one written. It never blocks a turn: a failed read or write is counted and
//! logged, and the next round tries again. Stop the mirror (which writes a last
//! snapshot) before shutting the runtime down: it holds the runtime's session
//! bridge.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use base64::Engine as _;
use meerkat_mob::MobSessionService;

use super::UnifiedRuntime;

/// One identity's row in the records file: the five fields of the export's
/// record. The import reads `identity` and `session_id`; it reads the other
/// three only for its report, so they carry the binding's generation, a
/// runtime id derived from it, and checkpoint version 0.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ContinuityMirrorRecord {
    pub identity: String,
    pub agent_runtime_id: String,
    pub session_id: String,
    pub generation: u64,
    pub checkpoint_version: u64,
}

/// One row in the sessions file: the session id and the persisted session
/// document's exact bytes in standard base64.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ContinuityMirrorSession {
    pub session_id: String,
    pub data: String,
}

/// Both row sets, each sorted by its key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContinuityMirrorSnapshot {
    pub records: Vec<ContinuityMirrorRecord>,
    pub sessions: Vec<ContinuityMirrorSession>,
}

/// Where the mirror writes. A sink replaces its previous copy with each
/// snapshot it is given.
#[async_trait::async_trait]
pub trait ContinuityMirrorSink: Send + Sync {
    async fn put(&self, snapshot: &ContinuityMirrorSnapshot) -> Result<(), String>;
}

/// Writes `records.jsonl` and `sessions.jsonl` into a directory, each through
/// a temporary file and a rename, so a reader sees whole files.
pub struct JsonlFileMirrorSink {
    dir: PathBuf,
}

impl JsonlFileMirrorSink {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }
}

fn write_jsonl<T: serde::Serialize>(dir: &Path, name: &str, rows: &[T]) -> Result<(), String> {
    let mut text = String::new();
    for row in rows {
        text.push_str(&serde_json::to_string(row).map_err(|error| error.to_string())?);
        text.push('\n');
    }
    let partial = dir.join(format!(".{name}.partial"));
    std::fs::write(&partial, text).map_err(|error| format!("{}: {error}", partial.display()))?;
    let target = dir.join(name);
    std::fs::rename(&partial, &target).map_err(|error| format!("{}: {error}", target.display()))
}

#[async_trait::async_trait]
impl ContinuityMirrorSink for JsonlFileMirrorSink {
    async fn put(&self, snapshot: &ContinuityMirrorSnapshot) -> Result<(), String> {
        std::fs::create_dir_all(&self.dir)
            .map_err(|error| format!("{}: {error}", self.dir.display()))?;
        write_jsonl(&self.dir, "sessions.jsonl", &snapshot.sessions)?;
        write_jsonl(&self.dir, "records.jsonl", &snapshot.records)
    }
}

/// What the mirror reads from: the runtime's session bridge and session
/// service.
#[derive(Clone)]
pub struct ContinuityMirrorSource {
    bridge: crate::identity_first::PublicSessionBridge,
    service: Arc<dyn MobSessionService>,
}

impl ContinuityMirrorSource {
    /// The rows now: one record per durable identity whose latest binding
    /// names a session with a persisted snapshot, and that session's document.
    pub async fn snapshot(&self) -> Result<ContinuityMirrorSnapshot, String> {
        let history = self
            .bridge
            .member_binding_history()
            .await
            .map_err(|error| format!("member binding history: {error}"))?;
        let mut records = BTreeMap::new();
        let mut sessions = BTreeMap::new();
        for binding in history.durable_identity_bindings() {
            let Some(session_id) = binding.session_id() else {
                continue;
            };
            let Some(session) = self
                .service
                .load_persisted_session(session_id)
                .await
                .map_err(|error| format!("session {session_id}: {error}"))?
            else {
                continue;
            };
            let document = serde_json::to_vec(&session)
                .map_err(|error| format!("session {session_id}: {error}"))?;
            let identity = crate::member_comms_id::durable_identity_for_roster_member(
                binding.identity().as_ref(),
            );
            let generation = binding.generation().get();
            records.insert(
                identity.clone(),
                ContinuityMirrorRecord {
                    agent_runtime_id: format!("rt:{identity}:{generation}"),
                    identity,
                    session_id: session_id.to_string(),
                    generation,
                    checkpoint_version: 0,
                },
            );
            sessions.insert(
                session_id.to_string(),
                ContinuityMirrorSession {
                    session_id: session_id.to_string(),
                    data: base64::engine::general_purpose::STANDARD.encode(document),
                },
            );
        }
        Ok(ContinuityMirrorSnapshot {
            records: records.into_values().collect(),
            sessions: sessions.into_values().collect(),
        })
    }
}

impl UnifiedRuntime {
    /// The source a continuity mirror reads, or why this runtime has none (no
    /// identity-first session bridge, or no session service).
    pub fn continuity_mirror_source(&self) -> Result<ContinuityMirrorSource, String> {
        let bridge = self
            .session_bridge()
            .ok_or_else(|| "the runtime has no identity-first session bridge".to_string())?;
        let service = self
            .mob_runtime
            .session_service()
            .cloned()
            .ok_or_else(|| "the runtime has no session service".to_string())?;
        Ok(ContinuityMirrorSource { bridge, service })
    }
}

/// Counts a running mirror keeps.
#[derive(Debug, Default)]
pub struct ContinuityMirrorCounts {
    pub writes: AtomicU64,
    pub failures: AtomicU64,
}

/// A running mirror. [`ContinuityMirror::stop`] writes a last snapshot.
pub struct ContinuityMirror {
    stop: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
    counts: Arc<ContinuityMirrorCounts>,
}

impl ContinuityMirror {
    /// Write a snapshot every `interval` when it differs from the last one
    /// written.
    pub fn spawn(
        source: ContinuityMirrorSource,
        sink: Arc<dyn ContinuityMirrorSink>,
        interval: Duration,
    ) -> Self {
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let counts = Arc::new(ContinuityMirrorCounts::default());
        let task_counts = Arc::clone(&counts);
        let task = tokio::spawn(async move {
            let mut written: Option<ContinuityMirrorSnapshot> = None;
            loop {
                round(&source, sink.as_ref(), &mut written, &task_counts).await;
                if *stopped.borrow() {
                    return;
                }
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = stopped.changed() => {
                        // One last round below, then return.
                    }
                }
            }
        });
        Self { stop, task, counts }
    }

    pub fn counts(&self) -> &ContinuityMirrorCounts {
        &self.counts
    }

    /// Write a last snapshot and end the task.
    pub async fn stop(self) -> Arc<ContinuityMirrorCounts> {
        let _ = self.stop.send(true);
        let _ = self.task.await;
        self.counts
    }
}

async fn round(
    source: &ContinuityMirrorSource,
    sink: &dyn ContinuityMirrorSink,
    written: &mut Option<ContinuityMirrorSnapshot>,
    counts: &ContinuityMirrorCounts,
) {
    let snapshot = match source.snapshot().await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            counts.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(error = %error, "continuity mirror: reading the runtime failed");
            return;
        }
    };
    if written.as_ref() == Some(&snapshot) {
        return;
    }
    match sink.put(&snapshot).await {
        Ok(()) => {
            counts.writes.fetch_add(1, Ordering::Relaxed);
            *written = Some(snapshot);
        }
        Err(error) => {
            counts.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(error = %error, "continuity mirror: writing the sink failed");
        }
    }
}
