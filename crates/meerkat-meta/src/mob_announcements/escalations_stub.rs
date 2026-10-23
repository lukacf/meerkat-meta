//! The escalation calls a declaration makes, with the signatures the
//! escalation module will have. Until that module is part of this build,
//! raising and resolving record nothing: the pending status in
//! `declarations/get` is the whole report.

use rusqlite::Connection;

use super::types::{Member, MobId, SourceId};

pub(super) type Result<T> = std::result::Result<T, super::types::AnnouncementStoreError>;

/// Why an escalation is raised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EscalationCause {
    /// A declared record or a role expansion is pending with a refusal that
    /// depends on the runtime.
    DeclarationPending,
}

/// What an escalation is about. Only declarations raise here; the
/// escalation module raises the source and member ones.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
#[allow(dead_code)]
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

/// The kind of a declared record an escalation is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeclarationRecordKind {
    Source,
    Subscription,
    RoleChild,
}

/// Open an escalation of (cause, subject) inside a store transaction, or
/// keep the open one.
pub(super) fn raise_escalation_in(
    db: &Connection,
    cause: EscalationCause,
    subject: &EscalationSubject,
    since_ms: u64,
    now_ms: u64,
) -> Result<()> {
    let _ = (db, cause, subject, since_ms, now_ms);
    Ok(())
}

/// Close the open escalation of (cause, subject), if any, inside a store
/// transaction.
pub(super) fn resolve_escalation_in(
    db: &Connection,
    cause: EscalationCause,
    subject: &EscalationSubject,
    now_ms: u64,
) -> Result<()> {
    let _ = (db, cause, subject, now_ms);
    Ok(())
}
