//! Escalations: a source or member condition a person should know about,
//! kept as a durable row and reported as events with a monotonic cursor.
//!
//! One escalation is open per cause and subject. Raising an open one keeps
//! it as it is; it is notified again every `renotify_after_ms` under the
//! same id with a higher count until it resolves. Every raise, notification
//! and resolution is also an event row: a host reads them from its last
//! cursor after a reconnect and misses nothing, and the owner pushes each to
//! the host's `meta/on_error` hook as it is written. Nothing here decides a
//! delivery: escalations only report.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::availability::SuspendMarker;
use super::sources::AudienceGrant;
use super::types::{Member, MobId, SourceId, Topic};

/// The reserved topic agents receive health announcements on.
pub const HEALTH_TOPIC: &str = "mob_announcements.health";

/// The key prefix of health announcements.
pub const HEALTH_KEY_PREFIX: &str = "health:";

/// Why an escalation is open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EscalationCause {
    /// A source's collections keep failing.
    FailureStreak,
    /// An active source has not advanced for its stale window: its
    /// collector is not running.
    Stale,
    /// A live source keeps succeeding without an item new to it.
    Quiet,
    /// A source's snapshots keep returning zero records.
    EmptyCoverage,
    /// A source rebaselined: changes may be missing.
    SourceGap,
    /// A source offered other content under a key it already announced.
    Conflicts,
    /// A member's deliveries have waited for an hour.
    Suspended,
    /// A member's deliveries report missing announcements.
    MemberGap,
    /// A member has items held because its coverage changed.
    AudienceHold,
    /// A declared record or role expansion waits on the runtime.
    DeclarationPending,
}

impl EscalationCause {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FailureStreak => "failure_streak",
            Self::Stale => "stale",
            Self::Quiet => "quiet",
            Self::EmptyCoverage => "empty_coverage",
            Self::SourceGap => "source_gap",
            Self::Conflicts => "conflicts",
            Self::Suspended => "suspended",
            Self::MemberGap => "member_gap",
            Self::AudienceHold => "audience_hold",
            Self::DeclarationPending => "declaration_pending",
        }
    }
}

impl std::fmt::Display for EscalationCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The kind of declared record a declaration escalation names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeclarationRecordKind {
    Source,
    Subscription,
    RoleChild,
}

impl DeclarationRecordKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Subscription => "subscription",
            Self::RoleChild => "role_child",
        }
    }
}

/// What an escalation is about.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum EscalationSubject {
    Source {
        mob_id: MobId,
        source_id: SourceId,
    },
    Member {
        member: Member,
    },
    Declaration {
        record_kind: DeclarationRecordKind,
        id: String,
    },
}

impl std::fmt::Display for EscalationSubject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source { mob_id, source_id } => write!(f, "source {source_id} of mob {mob_id}"),
            Self::Member { member } => {
                write!(f, "member {} of mob {}", member.identity, member.mob_id)
            }
            Self::Declaration { record_kind, id } => {
                write!(f, "declared {} {id}", record_kind.as_str())
            }
        }
    }
}

impl EscalationSubject {
    /// The key one open escalation per cause and subject is unique on.
    #[must_use]
    pub fn key(&self) -> String {
        match self {
            Self::Source { mob_id, source_id } => format!("source:{mob_id}:{source_id}"),
            Self::Member { member } => format!("member:{}", member.key()),
            Self::Declaration { record_kind, id } => {
                format!("declaration:{}:{id}", record_kind.as_str())
            }
        }
    }

    /// The mob the subject belongs to; a declaration names none.
    #[must_use]
    pub fn mob_id(&self) -> Option<&MobId> {
        match self {
            Self::Source { mob_id, .. } => Some(mob_id),
            Self::Member { member } => Some(&member.mob_id),
            Self::Declaration { .. } => None,
        }
    }

    /// A short digest of the subject, for health announcement keys.
    #[must_use]
    pub fn digest(&self) -> String {
        use std::fmt::Write as _;
        let digest = Sha256::digest(self.key().as_bytes());
        let mut out = String::with_capacity(16);
        for byte in digest.iter().take(8) {
            let _ = write!(out, "{byte:02x}");
        }
        out
    }
}

/// What the latest advance of a quiet source's window observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum QuietEvidence {
    /// The provider answered a delta: likely genuinely idle.
    DeltaConfirmed,
    /// A snapshot returned zero records: suspicious.
    SnapshotEmpty,
    /// A snapshot returned records, none new.
    SnapshotNonempty,
    /// The latest advance did not say what it observed.
    Unreported,
}

/// A durable escalation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Escalation {
    pub escalation_id: String,
    pub cause: EscalationCause,
    pub subject: EscalationSubject,
    /// When the condition began.
    pub since_ms: u64,
    pub raised_at_ms: u64,
    /// How many times it was notified again after its raise.
    pub renotify_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at_ms: Option<u64>,
    /// For a quiet source: what the window's latest advance observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<QuietEvidence>,
}

/// What an escalation event records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EscalationEventKind {
    Raised,
    Renotified,
    Resolved,
}

impl EscalationEventKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Raised => "raised",
            Self::Renotified => "renotified",
            Self::Resolved => "resolved",
        }
    }
}

impl std::fmt::Display for EscalationEventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One raise, notification or resolution, at its cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct EscalationEvent {
    pub cursor: u64,
    pub escalation_id: String,
    pub event: EscalationEventKind,
    pub cause: EscalationCause,
    pub subject: EscalationSubject,
    pub since_ms: u64,
    pub at_ms: u64,
    pub renotify_count: u32,
}

/// An escalation event and the escalation as it stands after it: what the
/// owner pushes to the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EscalationNotice {
    pub event: EscalationEvent,
    pub escalation: Escalation,
}

/// The typed body of a health announcement: never adapter text.
#[must_use]
pub(crate) fn health_body(escalation: &Escalation, event: EscalationEventKind) -> String {
    serde_json::json!({
        "event": event,
        "escalation_id": escalation.escalation_id,
        "cause": escalation.cause,
        "subject": escalation.subject,
        "since_ms": escalation.since_ms,
        "renotify_count": escalation.renotify_count,
        "evidence": escalation.evidence,
    })
    .to_string()
}

/// The key of the health announcement for `escalation` at its current count.
#[must_use]
pub(crate) fn health_key(escalation: &Escalation) -> String {
    format!(
        "{HEALTH_KEY_PREFIX}{}:{}:{}:{}",
        escalation.cause,
        escalation.subject.digest(),
        escalation.since_ms,
        escalation.renotify_count
    )
}

/// The reserved health topic.
pub(crate) fn health_topic() -> Result<Topic, super::types::AnnouncementStoreError> {
    Topic::new(HEALTH_TOPIC)
}

/// What one escalation pass needs from the owner's configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EscalationPass {
    /// How long an open escalation waits before it is notified again.
    pub renotify_after_ms: u64,
    /// Who receives member escalations on the health topic.
    pub health_audience: Vec<AudienceGrant>,
    /// How long events are kept.
    pub retention_ms: u64,
}

/// A member's backlog as members/get reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct MemberBacklogRead {
    pub eligible_count: u64,
    pub eligible_bytes: u64,
    pub oldest_captured_at_ms: Option<u64>,
    pub last_sealed_at_ms: Option<u64>,
    pub outstanding: bool,
    pub held_audience: u64,
    pub held_unhandled: u64,
    pub held_refused: u64,
    pub withheld_audience: u64,
}

/// What members/get reads about one member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct MemberRead {
    pub member: Member,
    pub backlog: MemberBacklogRead,
    /// The persisted suspend marker.
    pub marker: Option<SuspendMarker>,
    pub escalations: Vec<Escalation>,
}

/// meta/status's mob announcement counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct StatusCounts {
    pub sources_active: u64,
    pub sources_paused: u64,
    pub sources_failing: u64,
    pub sources_stale: u64,
    pub members_suspended: u64,
    pub open_escalations: u64,
    pub last_event_cursor: u64,
}

/// How long a member's deliveries wait before they escalate as suspended.
pub(crate) const SUSPENDED_ESCALATION_MS: u64 = 60 * 60 * 1_000;

/// How long the escalation of a condition that happens, rather than holds,
/// stays open after it last happened: a source's conflict or gap, a batch
/// that reported gaps, a capture withheld because coverage changed. A pass
/// reads back further when the last pass before it is older: what happened
/// while no round ran is raised by the next one.
pub(crate) const EVENT_WINDOW_MS: u64 = 24 * 60 * 60 * 1_000;
