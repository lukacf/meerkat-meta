//! SQLite implementation of [`AnnouncementStore`].
//!
//! One connection behind a mutex; every method runs as one `IMMEDIATE`
//! transaction on the blocking pool. Records are typed JSON; the columns
//! beside them exist for indexed reads only and are written in the same
//! statement as the record they index.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::sources::{
    AdvanceSource, AnnouncementSource, AuditAction, AuditQuery, AuditReason, AuditRow,
    CollectionPhase, Coverage, CoverageGap, DeletionCapability, ItemChange, ItemOutcome,
    ItemOutcomeKind, MAX_SOURCES_PER_GATEWAY, MAX_SOURCES_PER_MOB, MAX_STREAMS_PER_SOURCE,
    MemberFacts, OnboardingMode, Principal, PutSource, RecordSourceFailure, RosterFacts,
    SourceAdvance, SourceChange, SourceConfig, SourceConflictRecord, SourceFailureRecord,
    SourceQuery, SourceState, SourceStream, StreamPhase, TopicOwnership, covers,
};
use super::store::{AnnouncementStore, SubmissionCheck};
use super::types::{
    AnnounceOutcome, AnnounceRequest, Announcement, AnnouncementStoreError, Announcer, Batch,
    BatchEvent, BatchGap, BatchId, BatchItem, BatchPosition, BatchQuery, BatchState, BatchStateTag,
    ExpiryReport, GapReason, LateOutcome, MAX_BACKFILL, MAX_ITEM_WIRE_BYTES, MAX_PAGE, Member,
    MemberBacklog, MemberRoute, MemberRouteRecord, MobId, MobLineage, OperationId, PruneReport,
    PutSubscription, Result, SOURCE_KEY_PREFIX, SealingPolicy, Selector, SourceId, SourceKey,
    StartFrontier, Subscription, SubscriptionChange, SubscriptionChangeOutcome, SubscriptionEnd,
    SubscriptionId, SubscriptionState, Topic, content_digest, header_wire_bound, merge_gaps,
    quoted, render_batch, render_gap, render_item, request_fingerprint, wire_budget, wire_len,
};

/// The mob announcement store's version 1 tables, frozen with the release
/// that shipped them (0.9.4). A provider applies this and then [`SCHEMA_V2`]
/// through its own migration ledger before handing a connection to
/// [`SqliteAnnouncementStore::from_connection`].
pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS announcement_logs (
 mob_id TEXT PRIMARY KEY,
 head_sequence INTEGER NOT NULL,
 earliest_retained INTEGER NOT NULL,
 ended_at_ms INTEGER);
CREATE TABLE IF NOT EXISTS announcements (
 mob_id TEXT NOT NULL, sequence INTEGER NOT NULL, source_key TEXT NOT NULL,
 announced_at_ms INTEGER NOT NULL, rendered_bytes INTEGER NOT NULL, record TEXT NOT NULL,
 PRIMARY KEY (mob_id, sequence), UNIQUE (mob_id, source_key));
CREATE TABLE IF NOT EXISTS announcement_topics (
 topic TEXT NOT NULL, mob_id TEXT NOT NULL, sequence INTEGER NOT NULL,
 PRIMARY KEY (topic, mob_id, sequence));
CREATE TABLE IF NOT EXISTS member_subscriptions (
 id TEXT PRIMARY KEY, member TEXT NOT NULL, state TEXT NOT NULL,
 updated_at_ms INTEGER NOT NULL, record TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS member_subscriptions_by_member
 ON member_subscriptions (member, id);
CREATE INDEX IF NOT EXISTS member_subscriptions_by_state
 ON member_subscriptions (state, updated_at_ms, id);
CREATE TABLE IF NOT EXISTS member_subscription_topics (
 topic TEXT NOT NULL, subscription_id TEXT NOT NULL,
 PRIMARY KEY (topic, subscription_id));
CREATE TABLE IF NOT EXISTS member_subscription_mobs (
 mob_id TEXT NOT NULL, subscription_id TEXT NOT NULL,
 PRIMARY KEY (mob_id, subscription_id));
CREATE TABLE IF NOT EXISTS owed_announcements (
 subscription_id TEXT NOT NULL, mob_id TEXT NOT NULL, sequence INTEGER NOT NULL,
 subscription_revision INTEGER NOT NULL, member TEXT NOT NULL,
 captured_at_ms INTEGER NOT NULL, rendered_bytes INTEGER NOT NULL,
 batch_id TEXT, settlement TEXT,
 PRIMARY KEY (subscription_id, mob_id, sequence));
CREATE INDEX IF NOT EXISTS owed_announcements_open
 ON owed_announcements (member, settlement, batch_id, captured_at_ms);
CREATE INDEX IF NOT EXISTS owed_announcements_by_announcement
 ON owed_announcements (mob_id, sequence);
CREATE INDEX IF NOT EXISTS owed_announcements_by_batch ON owed_announcements (batch_id);
CREATE TABLE IF NOT EXISTS announcement_members (
 member TEXT PRIMARY KEY, last_sealed_at_ms INTEGER, flush_requested_at_ms INTEGER);
CREATE TABLE IF NOT EXISTS announcement_batches (
 id TEXT PRIMARY KEY, member TEXT NOT NULL, state TEXT NOT NULL,
 sealed_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL, record TEXT NOT NULL,
 late_read_at_ms INTEGER);
CREATE INDEX IF NOT EXISTS announcement_batches_by_state
 ON announcement_batches (state, sealed_at_ms, id);
CREATE INDEX IF NOT EXISTS announcement_batches_by_member
 ON announcement_batches (member, sealed_at_ms, id);
CREATE INDEX IF NOT EXISTS announcement_batches_late_reads
 ON announcement_batches (late_read_at_ms, id) WHERE late_read_at_ms IS NOT NULL;
CREATE TABLE IF NOT EXISTS announcement_batch_subscriptions (
 subscription_id TEXT NOT NULL, batch_id TEXT NOT NULL,
 PRIMARY KEY (subscription_id, batch_id));
CREATE TABLE IF NOT EXISTS announcement_operations (
 operation_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
 created_at_ms INTEGER NOT NULL, result TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS mob_lineage (
 row_id INTEGER PRIMARY KEY AUTOINCREMENT, mob_id TEXT NOT NULL, parent_mob_id TEXT,
 ended_at_ms INTEGER, record TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS mob_lineage_live
 ON mob_lineage (mob_id) WHERE ended_at_ms IS NULL;
CREATE INDEX IF NOT EXISTS mob_lineage_by_parent ON mob_lineage (parent_mob_id);
CREATE TABLE IF NOT EXISTS announcement_gaps (
 member TEXT NOT NULL, mob_id TEXT NOT NULL, reason TEXT NOT NULL, count INTEGER NOT NULL,
 PRIMARY KEY (member, mob_id, reason));
CREATE TABLE IF NOT EXISTS member_routes (
 member TEXT PRIMARY KEY, route TEXT NOT NULL, record TEXT NOT NULL,
 checked_through INTEGER);
CREATE INDEX IF NOT EXISTS member_routes_by_route ON member_routes (route, member);";

/// The tables version 2 adds: announcement sources, one row per
/// `(mob_id, source_id)` holding its current generation, the topics the
/// sources own, the streams each generation onboarded, the counted audit
/// of what sources' audiences and routes refused, withheld, held, released
/// or let through, one entry per source, generation, member, action and
/// reason, and the owed items held while their member does not pass. A
/// provider applies this after [`SCHEMA`].
pub const SCHEMA_V2: &str = "
CREATE TABLE IF NOT EXISTS announcement_sources (
 mob_id TEXT NOT NULL, source_id TEXT NOT NULL, generation INTEGER NOT NULL,
 state TEXT NOT NULL, identity_key TEXT NOT NULL, updated_at_ms INTEGER NOT NULL,
 record TEXT NOT NULL, PRIMARY KEY (mob_id, source_id));
CREATE INDEX IF NOT EXISTS announcement_sources_by_identity
 ON announcement_sources (mob_id, identity_key, state);
CREATE TABLE IF NOT EXISTS announcement_source_topics (
 mob_id TEXT NOT NULL, topic TEXT NOT NULL, source_id TEXT NOT NULL, kind TEXT NOT NULL,
 PRIMARY KEY (mob_id, topic));
CREATE INDEX IF NOT EXISTS announcement_source_topics_by_source
 ON announcement_source_topics (mob_id, source_id);
CREATE TABLE IF NOT EXISTS announcement_source_streams (
 mob_id TEXT NOT NULL, source_id TEXT NOT NULL, stream TEXT NOT NULL,
 generation INTEGER NOT NULL, phase TEXT NOT NULL, mode TEXT NOT NULL,
 max_items INTEGER, backfilled INTEGER NOT NULL, since_ms INTEGER NOT NULL,
 PRIMARY KEY (mob_id, source_id, stream));
CREATE TABLE IF NOT EXISTS announcement_audit (
 id INTEGER PRIMARY KEY AUTOINCREMENT, mob_id TEXT NOT NULL, source_id TEXT NOT NULL,
 generation INTEGER NOT NULL, member TEXT NOT NULL, action TEXT NOT NULL,
 reason TEXT NOT NULL, count INTEGER NOT NULL, first_at_ms INTEGER NOT NULL,
 last_at_ms INTEGER NOT NULL, record TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS announcement_audit_counted
 ON announcement_audit (mob_id, source_id, generation, member, action, reason);
CREATE INDEX IF NOT EXISTS announcement_audit_by_source
 ON announcement_audit (mob_id, source_id, id);
CREATE INDEX IF NOT EXISTS announcement_audit_by_member
 ON announcement_audit (member, id);
CREATE TABLE IF NOT EXISTS announcement_holds (
 subscription_id TEXT NOT NULL, mob_id TEXT NOT NULL, sequence INTEGER NOT NULL,
 member TEXT NOT NULL, source_id TEXT NOT NULL, generation INTEGER NOT NULL,
 held_at_ms INTEGER NOT NULL, checked_at_ms INTEGER NOT NULL, record TEXT NOT NULL,
 PRIMARY KEY (subscription_id, mob_id, sequence));
CREATE INDEX IF NOT EXISTS announcement_holds_by_member
 ON announcement_holds (member, checked_at_ms);";

fn storage(error: impl std::fmt::Display) -> AnnouncementStoreError {
    AnnouncementStoreError::Storage(error.to_string())
}

fn encode(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(storage)
}

fn decode<T: DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value).map_err(storage)
}

fn int(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        AnnouncementStoreError::Invalid(format!("{value} exceeds the store's integer range"))
    })
}

fn uint(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| storage(format!("negative stored integer {value}")))
}

fn page(limit: usize) -> Result<i64> {
    int(limit.clamp(1, MAX_PAGE) as u64)
}

/// Taken around every operation, so a provider can fence maintenance (for
/// example an offline migration) against live use of the database.
pub trait ConnectionGuard: Send + Sync {
    fn acquire(&self) -> Result<Box<dyn Send>>;
}

#[derive(Clone)]
pub struct SqliteAnnouncementStore {
    connection: Arc<Mutex<Connection>>,
    guard: Option<Arc<dyn ConnectionGuard>>,
}

impl SqliteAnnouncementStore {
    /// Standalone opener for tests and embedders without a migration
    /// ledger. Production composition supplies its ledgered connection
    /// through [`Self::from_connection`].
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path).map_err(storage)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(storage)?;
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(storage)?;
        connection.execute_batch(SCHEMA).map_err(storage)?;
        connection.execute_batch(SCHEMA_V2).map_err(storage)?;
        Ok(Self::from_connection(connection, None))
    }

    /// The caller must have applied [`SCHEMA`] and [`SCHEMA_V2`] under its
    /// migration contract.
    pub fn from_connection(
        connection: Connection,
        guard: Option<Arc<dyn ConnectionGuard>>,
    ) -> Self {
        Self {
            connection: Arc::new(Mutex::new(connection)),
            guard,
        }
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let connection = Arc::clone(&self.connection);
        let guard = self.guard.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = connection
                .lock()
                .map_err(|_| storage("announcement store connection poisoned"))?;
            let _operation_guard = guard.map(|guard| guard.acquire()).transpose()?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(storage)?;
            let value = operation(&transaction)?;
            transaction.commit().map_err(storage)?;
            Ok(value)
        })
        .await
        .map_err(storage)?
    }
}

// ---------------------------------------------------------------------------
// Row helpers
// ---------------------------------------------------------------------------

fn operation_result(
    db: &Connection,
    operation: &OperationId,
    fingerprint: &str,
) -> Result<Option<String>> {
    let row: Option<(String, String)> = db
        .query_row(
            "SELECT fingerprint, result FROM announcement_operations WHERE operation_id = ?1",
            params![operation.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(storage)?;
    match row {
        Some((stored, result)) if stored == fingerprint => Ok(Some(result)),
        Some(_) => Err(AnnouncementStoreError::OperationReused),
        None => Ok(None),
    }
}

fn record_operation(
    db: &Connection,
    operation: &OperationId,
    fingerprint: &str,
    now_ms: u64,
    result: &str,
) -> Result<()> {
    db.execute(
        "INSERT INTO announcement_operations (operation_id, fingerprint, created_at_ms, result)
         VALUES (?1, ?2, ?3, ?4)",
        params![operation.as_str(), fingerprint, int(now_ms)?, result],
    )
    .map_err(storage)?;
    Ok(())
}

fn announcement_by_key(
    db: &Connection,
    mob_id: &MobId,
    source_key: &SourceKey,
) -> Result<Option<Announcement>> {
    let record: Option<String> = db
        .query_row(
            "SELECT record FROM announcements WHERE mob_id = ?1 AND source_key = ?2",
            params![mob_id.as_str(), source_key.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    record.as_deref().map(decode).transpose()
}

fn announcement_at(db: &Connection, mob_id: &MobId, sequence: u64) -> Result<Announcement> {
    let record: Option<String> = db
        .query_row(
            "SELECT record FROM announcements WHERE mob_id = ?1 AND sequence = ?2",
            params![mob_id.as_str(), int(sequence)?],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    match record {
        Some(record) => decode(&record),
        None => Err(storage(format!(
            "an owed announcement names {mob_id} #{sequence}, which is not retained"
        ))),
    }
}

/// Every mob's head sequence and earliest retained sequence.
fn log_heads(db: &Connection) -> Result<BTreeMap<MobId, (u64, u64)>> {
    let mut statement = db
        .prepare("SELECT mob_id, head_sequence, earliest_retained FROM announcement_logs")
        .map_err(storage)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(storage)?;
    let mut heads = BTreeMap::new();
    for row in rows {
        let (mob_id, head, earliest) = row.map_err(storage)?;
        heads.insert(MobId::new(mob_id)?, (uint(head)?, uint(earliest)?));
    }
    Ok(heads)
}

fn load_subscription(db: &Connection, id: &SubscriptionId) -> Result<Option<Subscription>> {
    let record: Option<String> = db
        .query_row(
            "SELECT record FROM member_subscriptions WHERE id = ?1",
            params![id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    record.as_deref().map(decode).transpose()
}

/// Write a subscription with its topic and mob index rows. Only a live
/// subscription is indexed: a terminal one captures nothing and names no
/// mob that retention must keep.
fn write_subscription(db: &Connection, subscription: &Subscription) -> Result<()> {
    db.execute(
        "INSERT INTO member_subscriptions (id, member, state, updated_at_ms, record)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(id) DO UPDATE SET member = excluded.member,
           state = excluded.state, updated_at_ms = excluded.updated_at_ms,
           record = excluded.record",
        params![
            subscription.id.as_str(),
            subscription.member.key(),
            subscription.state.as_str(),
            int(subscription.updated_at_ms)?,
            encode(subscription)?,
        ],
    )
    .map_err(storage)?;
    for table in ["member_subscription_topics", "member_subscription_mobs"] {
        db.execute(
            &format!("DELETE FROM {table} WHERE subscription_id = ?1"),
            params![subscription.id.as_str()],
        )
        .map_err(storage)?;
    }
    if subscription.state.is_live() {
        for topic in &subscription.selector.topics {
            db.execute(
                "INSERT INTO member_subscription_topics (topic, subscription_id) VALUES (?1, ?2)",
                params![topic.as_str(), subscription.id.as_str()],
            )
            .map_err(storage)?;
        }
        for mob_id in &subscription.selector.mobs {
            db.execute(
                "INSERT INTO member_subscription_mobs (mob_id, subscription_id) VALUES (?1, ?2)",
                params![mob_id.as_str(), subscription.id.as_str()],
            )
            .map_err(storage)?;
        }
    }
    Ok(())
}

/// A mob a caller names again is hosted (the owner checked it before this
/// transaction), so its log is no longer marked ended.
fn reopen_logs<'a>(db: &Connection, mobs: impl IntoIterator<Item = &'a MobId>) -> Result<()> {
    for mob_id in mobs {
        db.execute(
            "UPDATE announcement_logs SET ended_at_ms = NULL
             WHERE mob_id = ?1 AND ended_at_ms IS NOT NULL",
            params![mob_id.as_str()],
        )
        .map_err(storage)?;
    }
    Ok(())
}

/// Live subscriptions selecting any of these topics.
fn subscriptions_for_topics(
    db: &Connection,
    topics: &BTreeSet<Topic>,
) -> Result<BTreeMap<SubscriptionId, Subscription>> {
    let mut statement = db
        .prepare_cached(
            "SELECT s.record FROM member_subscription_topics t
             JOIN member_subscriptions s ON s.id = t.subscription_id
             WHERE t.topic = ?1 AND s.state IN ('active', 'paused')",
        )
        .map_err(storage)?;
    let mut found = BTreeMap::new();
    for topic in topics {
        let rows = statement
            .query_map(params![topic.as_str()], |row| row.get::<_, String>(0))
            .map_err(storage)?;
        for record in rows {
            let subscription: Subscription = decode(&record.map_err(storage)?)?;
            found.insert(subscription.id.clone(), subscription);
        }
    }
    Ok(found)
}

fn insert_owed(
    db: &Connection,
    subscription: &Subscription,
    mob_id: &MobId,
    sequence: u64,
    captured_at_ms: u64,
    rendered_bytes: u64,
) -> Result<()> {
    db.execute(
        "INSERT OR IGNORE INTO owed_announcements
         (subscription_id, mob_id, sequence, subscription_revision, member,
          captured_at_ms, rendered_bytes, batch_id, settlement)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL)",
        params![
            subscription.id.as_str(),
            mob_id.as_str(),
            int(sequence)?,
            int(subscription.revision)?,
            subscription.member.key(),
            int(captured_at_ms)?,
            int(rendered_bytes)?,
        ],
    )
    .map_err(storage)?;
    Ok(())
}

fn captured_for(db: &Connection, mob_id: &MobId, sequence: u64) -> Result<Vec<SubscriptionId>> {
    let mut statement = db
        .prepare(
            "SELECT subscription_id FROM owed_announcements
             WHERE mob_id = ?1 AND sequence = ?2 ORDER BY subscription_id",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(params![mob_id.as_str(), int(sequence)?], |row| {
            row.get::<_, String>(0)
        })
        .map_err(storage)?;
    rows.map(|row| SubscriptionId::new(row.map_err(storage)?))
        .collect()
}

fn load_batch(db: &Connection, id: &BatchId) -> Result<Option<Batch>> {
    let record: Option<String> = db
        .query_row(
            "SELECT record FROM announcement_batches WHERE id = ?1",
            params![id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    record.as_deref().map(decode).transpose()
}

fn insert_batch(db: &Connection, batch: &Batch) -> Result<()> {
    db.execute(
        "INSERT INTO announcement_batches (id, member, state, sealed_at_ms, updated_at_ms, record)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            batch.id.as_str(),
            batch.member.key(),
            batch.state.tag().as_str(),
            int(batch.sealed_at_ms)?,
            int(batch.updated_at_ms)?,
            encode(batch)?,
        ],
    )
    .map_err(storage)?;
    let subscriptions = batch
        .items
        .iter()
        .map(|item| item.subscription_id.as_str())
        .collect::<BTreeSet<_>>();
    for subscription in subscriptions {
        db.execute(
            "INSERT OR IGNORE INTO announcement_batch_subscriptions (subscription_id, batch_id)
             VALUES (?1, ?2)",
            params![subscription, batch.id.as_str()],
        )
        .map_err(storage)?;
    }
    Ok(())
}

fn update_batch(db: &Connection, batch: &Batch) -> Result<()> {
    db.execute(
        "UPDATE announcement_batches SET state = ?2, updated_at_ms = ?3, record = ?4 WHERE id = ?1",
        params![
            batch.id.as_str(),
            batch.state.tag().as_str(),
            int(batch.updated_at_ms)?,
            encode(batch)?,
        ],
    )
    .map_err(storage)?;
    Ok(())
}

fn batches_from(
    statement: &mut rusqlite::CachedStatement<'_>,
    params: impl rusqlite::Params,
) -> Result<Vec<Batch>> {
    let rows = statement
        .query_map(params, |row| row.get::<_, String>(0))
        .map_err(storage)?;
    rows.map(|record| decode(&record.map_err(storage)?))
        .collect()
}

fn settle_owed(db: &Connection, batch: &BatchId, settlement: &str) -> Result<()> {
    db.execute(
        "UPDATE owed_announcements SET settlement = ?2
         WHERE batch_id = ?1 AND settlement IS NULL",
        params![batch.as_str(), settlement],
    )
    .map_err(storage)?;
    Ok(())
}

/// Add `gaps` to the member's pending gaps, which its next batch reports.
fn add_gaps(db: &Connection, member: &Member, gaps: &[BatchGap]) -> Result<()> {
    for gap in gaps {
        db.execute(
            "INSERT INTO announcement_gaps (member, mob_id, reason, count) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(member, mob_id, reason) DO UPDATE SET count = count + excluded.count",
            params![
                member.key(),
                gap.mob_id.as_str(),
                gap.reason.as_str(),
                int(gap.count)?
            ],
        )
        .map_err(storage)?;
    }
    Ok(())
}

/// The member's pending gaps, in a stable order.
fn pending_gaps(db: &Connection, member: &Member) -> Result<Vec<BatchGap>> {
    let mut statement = db
        .prepare(
            "SELECT mob_id, reason, count FROM announcement_gaps WHERE member = ?1
             ORDER BY mob_id, reason",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(params![member.key()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(storage)?;
    let mut gaps = Vec::new();
    for row in rows {
        let (mob_id, reason, count) = row.map_err(storage)?;
        gaps.push(BatchGap {
            mob_id: MobId::new(mob_id)?,
            count: uint(count)?,
            reason: GapReason::parse(&reason)?,
        });
    }
    Ok(gaps)
}

/// What the unsettled owed announcements of `batch` amount to as gaps for
/// `reason`: distinct announcements per mob, so one held for two of the
/// member's subscriptions counts once.
fn batch_gaps(db: &Connection, batch: &BatchId, reason: GapReason) -> Result<Vec<BatchGap>> {
    let mut statement = db
        .prepare(
            "SELECT mob_id, COUNT(DISTINCT sequence) FROM owed_announcements
             WHERE batch_id = ?1 AND settlement IS NULL GROUP BY mob_id ORDER BY mob_id",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(params![batch.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(storage)?;
    let mut gaps = Vec::new();
    for row in rows {
        let (mob_id, count) = row.map_err(storage)?;
        gaps.push(BatchGap {
            mob_id: MobId::new(mob_id)?,
            count: uint(count)?,
            reason,
        });
    }
    Ok(gaps)
}

/// When the late outcome of an unconfirmed batch is read next, or never.
fn set_late_read(db: &Connection, id: &BatchId, at_ms: Option<u64>) -> Result<()> {
    db.execute(
        "UPDATE announcement_batches SET late_read_at_ms = ?2 WHERE id = ?1",
        params![id.as_str(), at_ms.map(int).transpose()?],
    )
    .map_err(storage)?;
    Ok(())
}

fn record_late_read_in(
    db: &Connection,
    id: &BatchId,
    late: Option<LateOutcome>,
    next_read_at_ms: Option<u64>,
) -> Result<Batch> {
    let mut batch = load_batch(db, id)?.ok_or(AnnouncementStoreError::UnknownBatch)?;
    if !BatchStateTag::UNCONFIRMED.contains(&batch.state.tag()) {
        return Err(AnnouncementStoreError::Invalid(format!(
            "a {} batch keeps no late outcome",
            batch.state.tag()
        )));
    }
    if let Some(late) = late {
        batch.state = batch.state.with_late(late)?;
        // The batch stays settled when it settled: retention counts from
        // then, not from a late read.
        db.execute(
            "UPDATE announcement_batches SET record = ?2 WHERE id = ?1",
            params![id.as_str(), encode(&batch)?],
        )
        .map_err(storage)?;
    }
    set_late_read(db, id, next_read_at_ms)?;
    Ok(batch)
}

fn mark_sealed(db: &Connection, member: &Member, now_ms: u64) -> Result<()> {
    db.execute(
        "INSERT INTO announcement_members (member, last_sealed_at_ms, flush_requested_at_ms)
         VALUES (?1, ?2, NULL)
         ON CONFLICT(member) DO UPDATE SET last_sealed_at_ms = excluded.last_sealed_at_ms,
           flush_requested_at_ms = NULL",
        params![member.key(), int(now_ms)?],
    )
    .map_err(storage)?;
    Ok(())
}

fn request_flush(db: &Connection, member: &Member, now_ms: u64) -> Result<()> {
    db.execute(
        "INSERT INTO announcement_members (member, last_sealed_at_ms, flush_requested_at_ms)
         VALUES (?1, NULL, ?2)
         ON CONFLICT(member) DO UPDATE SET flush_requested_at_ms = excluded.flush_requested_at_ms",
        params![member.key(), int(now_ms)?],
    )
    .map_err(storage)?;
    Ok(())
}

fn backlog_for(db: &Connection, member: &Member) -> Result<Option<MemberBacklog>> {
    let row: Option<(i64, i64, Option<i64>)> = db
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(o.rendered_bytes), 0), MIN(o.captured_at_ms)
             FROM owed_announcements o JOIN member_subscriptions s ON s.id = o.subscription_id
             WHERE o.member = ?1 AND o.settlement IS NULL AND o.batch_id IS NULL
               AND s.state = 'active'
               AND NOT EXISTS (
                 SELECT 1 FROM announcement_holds h WHERE h.subscription_id = o.subscription_id
                   AND h.mob_id = o.mob_id AND h.sequence = o.sequence)",
            params![member.key()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(storage)?;
    let Some((count, bytes, Some(oldest))) = row else {
        return Ok(None);
    };
    if count == 0 {
        return Ok(None);
    }
    complete_backlog(
        db,
        member.clone(),
        uint(count)?,
        uint(bytes)?,
        uint(oldest)?,
    )
    .map(Some)
}

fn complete_backlog(
    db: &Connection,
    member: Member,
    eligible_count: u64,
    eligible_bytes: u64,
    oldest_captured_at_ms: u64,
) -> Result<MemberBacklog> {
    let marks: Option<(Option<i64>, Option<i64>)> = db
        .query_row(
            "SELECT last_sealed_at_ms, flush_requested_at_ms FROM announcement_members
             WHERE member = ?1",
            params![member.key()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(storage)?;
    let (last_sealed, flush) = marks.unwrap_or((None, None));
    let outstanding: bool = db
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM announcement_batches WHERE member = ?1
               AND state IN ('sealed', 'submitting', 'committed'))",
            params![member.key()],
            |row| row.get(0),
        )
        .map_err(storage)?;
    Ok(MemberBacklog {
        member,
        eligible_count,
        eligible_bytes,
        oldest_captured_at_ms,
        last_sealed_at_ms: last_sealed.map(uint).transpose()?,
        flush_requested_at_ms: flush.map(uint).transpose()?,
        outstanding,
    })
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

fn announce_in(
    db: &Connection,
    request: &AnnounceRequest,
    facts: &RosterFacts,
    now_ms: u64,
) -> Result<AnnounceOutcome> {
    request.validate()?;
    let digest = request.digest();
    if let Some(existing) = announcement_by_key(db, &request.mob_id, &request.source_key)? {
        if existing.digest != digest {
            return Err(AnnouncementStoreError::AnnouncementConflict {
                existing: Box::new(existing),
            });
        }
        let captured = captured_for(db, &existing.mob_id, existing.sequence)?;
        return Ok(AnnounceOutcome {
            announcement: existing,
            replayed: true,
            captured_for: captured,
        });
    }
    // The key prefix of source items and a topic a source owns are reached
    // only by that source's items and, on a forward topic, by its triage
    // route's own member. A replay above meets its original; this guards
    // only what would be announced now.
    request.ensure_key_open()?;
    ensure_topics_open(db, request)?;
    let head: Option<i64> = db
        .query_row(
            "SELECT head_sequence FROM announcement_logs WHERE mob_id = ?1",
            params![request.mob_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    let sequence = head
        .map(uint)
        .transpose()?
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| storage("mob sequence exhausted"))?;
    let announcement = Announcement {
        mob_id: request.mob_id.clone(),
        sequence,
        announced_by: request.announced_by.clone(),
        source_key: request.source_key.clone(),
        topics: request.topics.clone(),
        body: request.body.clone(),
        correlation_id: request.correlation_id.clone(),
        causes: request.causes.clone(),
        digest,
        announced_at_ms: now_ms,
    };
    let item = render_item(&announcement);
    let item_wire = wire_len(&item);
    if item_wire > MAX_ITEM_WIRE_BYTES {
        return Err(AnnouncementStoreError::TooLarge(format!(
            "the announcement renders to {item_wire} bytes inside a delivery; at most \
             {MAX_ITEM_WIRE_BYTES}"
        )));
    }
    let rendered_bytes = item.len() as u64;
    // The owner checked that the mob is hosted, so a log marked ended
    // belongs to an earlier mob under the same id and is open again.
    db.execute(
        "INSERT INTO announcement_logs (mob_id, head_sequence, earliest_retained)
         VALUES (?1, ?2, ?2)
         ON CONFLICT(mob_id) DO UPDATE SET head_sequence = excluded.head_sequence,
           ended_at_ms = NULL",
        params![announcement.mob_id.as_str(), int(sequence)?],
    )
    .map_err(storage)?;
    db.execute(
        "INSERT INTO announcements
         (mob_id, sequence, source_key, announced_at_ms, rendered_bytes, record)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            announcement.mob_id.as_str(),
            int(sequence)?,
            announcement.source_key.as_str(),
            int(now_ms)?,
            int(rendered_bytes)?,
            encode(&announcement)?,
        ],
    )
    .map_err(storage)?;
    for topic in &announcement.topics {
        db.execute(
            "INSERT INTO announcement_topics (topic, mob_id, sequence) VALUES (?1, ?2, ?3)",
            params![topic.as_str(), announcement.mob_id.as_str(), int(sequence)?],
        )
        .map_err(storage)?;
    }
    let owners = topic_sources(db, &announcement.mob_id, &announcement.topics)?;
    let mut captured = Vec::new();
    for subscription in subscriptions_for_topics(db, &announcement.topics)?.into_values() {
        if !subscription
            .selector
            .matches(&announcement.mob_id, &announcement.topics)
        {
            continue;
        }
        let after = subscription
            .frontier
            .get(&announcement.mob_id)
            .copied()
            .unwrap_or(0);
        if sequence <= after {
            continue;
        }
        // A topic a source owns admits only the members its audience covers
        // and its routes reach, and none that owns a live conversation on
        // its scope.
        if let Some((reason, owned)) =
            withholding(&owners, &subscription, &announcement.topics, facts)
        {
            count_audit(
                db,
                &AuditKey {
                    mob_id: announcement.mob_id.clone(),
                    source_id: owned.source_id.clone(),
                    generation: owned.generation,
                    member: subscription.member.clone(),
                    action: AuditAction::CaptureWithheld,
                    reason,
                },
                now_ms,
            )?;
            continue;
        }
        insert_owed(
            db,
            &subscription,
            &announcement.mob_id,
            sequence,
            now_ms,
            rendered_bytes,
        )?;
        captured.push(subscription.id);
    }
    Ok(AnnounceOutcome {
        announcement,
        replayed: false,
        captured_for: captured,
    })
}

fn create_subscription(
    db: &Connection,
    request: &PutSubscription,
    now_ms: u64,
) -> Result<Subscription> {
    let heads = log_heads(db)?;
    // A mob that has announced nothing yet is at head 0.
    let head_of = |mob_id: &MobId| heads.get(mob_id).copied().unwrap_or((0, 1));
    let mut frontier: BTreeMap<MobId, u64> = request
        .selector
        .mobs
        .iter()
        .map(|mob_id| (mob_id.clone(), head_of(mob_id).0))
        .collect();
    let replay = match &request.start {
        StartFrontier::Now => BTreeMap::new(),
        StartFrontier::After { frontier: named } => {
            for (mob_id, after) in named {
                if !request.selector.mobs.contains(mob_id) {
                    return Err(AnnouncementStoreError::Invalid(format!(
                        "the start frontier names mob {mob_id}, which the subscription does not"
                    )));
                }
                let (head, earliest) = head_of(mob_id);
                if *after > head {
                    return Err(AnnouncementStoreError::FrontierBeyondHead {
                        mob_id: mob_id.clone(),
                        head,
                    });
                }
                if after.saturating_add(1) < earliest {
                    return Err(AnnouncementStoreError::Gap {
                        mob_id: mob_id.clone(),
                        earliest_retained: earliest,
                    });
                }
                frontier.insert(mob_id.clone(), *after);
            }
            named.clone()
        }
    };
    let subscription = Subscription {
        id: request.id.clone(),
        member: request.member.clone(),
        selector: request.selector.clone(),
        state: SubscriptionState::Active,
        revision: 1,
        selector_revision: 1,
        frontier,
        ended_mobs: BTreeSet::new(),
        ended: None,
        member_mob_cursor: request.member_mob_cursor,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    };
    write_subscription(db, &subscription)?;
    reopen_logs(db, &subscription.selector.mobs)?;
    let mut statement = db
        .prepare_cached(
            "SELECT p.sequence, p.rendered_bytes FROM announcement_topics t
             JOIN announcements p ON p.mob_id = t.mob_id AND p.sequence = t.sequence
             WHERE t.topic = ?1 AND t.mob_id = ?2 AND t.sequence > ?3",
        )
        .map_err(storage)?;
    let mut replayed = 0_usize;
    for (mob_id, after) in &replay {
        let mut selected = BTreeMap::new();
        for topic in &subscription.selector.topics {
            let rows = statement
                .query_map(
                    params![topic.as_str(), mob_id.as_str(), int(*after)?],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .map_err(storage)?;
            for row in rows {
                let (sequence, bytes) = row.map_err(storage)?;
                selected.insert(uint(sequence)?, uint(bytes)?);
            }
        }
        for (sequence, bytes) in selected {
            replayed += 1;
            if replayed > MAX_BACKFILL {
                return Err(AnnouncementStoreError::TooLarge(format!(
                    "a starting frontier may replay at most {MAX_BACKFILL} announcements"
                )));
            }
            insert_owed(db, &subscription, mob_id, sequence, now_ms, bytes)?;
        }
    }
    Ok(subscription)
}

fn put_subscription_in(
    db: &Connection,
    operation: &OperationId,
    request: &PutSubscription,
    now_ms: u64,
) -> Result<Subscription> {
    let fingerprint = request_fingerprint("put_subscription", request)?;
    if let Some(result) = operation_result(db, operation, &fingerprint)? {
        return decode(&result);
    }
    request.selector.validate()?;
    let route = request
        .route
        .as_ref()
        .ok_or_else(|| AnnouncementStoreError::RouteRequired {
            member: request.member.clone(),
        })?;
    keep_member_route_in(db, &request.member, route)?;
    if route.route == MemberRoute::Child {
        within_child_limits(db, request)?;
    }
    let subscription = match (
        load_subscription(db, &request.id)?,
        request.expected_revision,
    ) {
        (None, None) => create_subscription(db, request, now_ms)?,
        (None, Some(_)) => return Err(AnnouncementStoreError::UnknownSubscription),
        (Some(existing), None) => {
            return Err(AnnouncementStoreError::StaleRevision {
                current: existing.revision,
            });
        }
        (Some(existing), Some(expected)) => {
            if let Some(refusal) = existing.terminal_refusal() {
                return Err(refusal);
            }
            if existing.revision != expected {
                return Err(AnnouncementStoreError::StaleRevision {
                    current: existing.revision,
                });
            }
            if existing.member != request.member {
                return Err(AnnouncementStoreError::MemberMismatch);
            }
            // The new selector applies from the current head of every mob it
            // names; what the old one captured stays owed.
            let revision = existing.revision + 1;
            let heads = log_heads(db)?;
            let updated = Subscription {
                selector: request.selector.clone(),
                revision,
                selector_revision: revision,
                frontier: request
                    .selector
                    .mobs
                    .iter()
                    .map(|mob_id| {
                        let head = heads.get(mob_id).map_or(0, |(head, _)| *head);
                        (mob_id.clone(), head)
                    })
                    .collect(),
                updated_at_ms: now_ms,
                ..existing
            };
            write_subscription(db, &updated)?;
            reopen_logs(db, &updated.selector.mobs)?;
            updated
        }
    };
    record_operation(db, operation, &fingerprint, now_ms, &encode(&subscription)?)?;
    Ok(subscription)
}

#[derive(Serialize)]
struct ChangeRequest<'a> {
    id: &'a SubscriptionId,
    expected_revision: u64,
    change: SubscriptionChange,
}

fn unresolved_batches_of(db: &Connection, id: &SubscriptionId) -> Result<Vec<BatchId>> {
    let mut statement = db
        .prepare(
            "SELECT b.id FROM announcement_batch_subscriptions m
             JOIN announcement_batches b ON b.id = m.batch_id
             WHERE m.subscription_id = ?1 AND b.state IN ('sealed', 'submitting', 'committed')
             ORDER BY b.sealed_at_ms, b.id",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(params![id.as_str()], |row| row.get::<_, String>(0))
        .map_err(storage)?;
    rows.map(|row| BatchId::new(row.map_err(storage)?))
        .collect()
}

/// Cancel the sealed batches no live subscription wants any more. A batch
/// that may have reached the native owner is never cancelled here.
fn cancel_unwanted_sealed(db: &Connection, id: &SubscriptionId, now_ms: u64) -> Result<()> {
    for batch_id in unresolved_batches_of(db, id)? {
        let Some(mut batch) = load_batch(db, &batch_id)? else {
            continue;
        };
        if batch.state.tag() != BatchStateTag::Sealed {
            continue;
        }
        let mut wanted = false;
        let member_subscriptions = batch
            .items
            .iter()
            .map(|item| item.subscription_id.clone())
            .collect::<BTreeSet<_>>();
        for subscription in &member_subscriptions {
            if load_subscription(db, subscription)?.is_some_and(|found| found.state.is_live()) {
                wanted = true;
                break;
            }
        }
        if wanted {
            continue;
        }
        batch.state = batch.state.apply(&BatchEvent::Cancel)?;
        batch.updated_at_ms = now_ms;
        update_batch(db, &batch)?;
        settle_owed(db, &batch.id, "cancelled")?;
        // Its gap lines reach the member's next batch instead.
        add_gaps(db, &batch.member, &batch.gaps)?;
    }
    Ok(())
}

fn change_subscription_in(
    db: &Connection,
    operation: &OperationId,
    id: &SubscriptionId,
    expected_revision: u64,
    change: SubscriptionChange,
    now_ms: u64,
) -> Result<SubscriptionChangeOutcome> {
    let fingerprint = request_fingerprint(
        "change_subscription",
        &ChangeRequest {
            id,
            expected_revision,
            change,
        },
    )?;
    if let Some(result) = operation_result(db, operation, &fingerprint)? {
        return decode(&result);
    }
    let mut subscription =
        load_subscription(db, id)?.ok_or(AnnouncementStoreError::UnknownSubscription)?;
    if let Some(refusal) = subscription.terminal_refusal() {
        return Err(refusal);
    }
    if subscription.revision != expected_revision {
        return Err(AnnouncementStoreError::StaleRevision {
            current: subscription.revision,
        });
    }
    let next = match change {
        SubscriptionChange::Pause => SubscriptionState::Paused,
        SubscriptionChange::Resume => SubscriptionState::Active,
        SubscriptionChange::Cancel => SubscriptionState::Cancelled,
    };
    if next != subscription.state {
        subscription.state = next;
        subscription.revision += 1;
        subscription.updated_at_ms = now_ms;
        write_subscription(db, &subscription)?;
        match change {
            SubscriptionChange::Pause => {}
            SubscriptionChange::Resume => request_flush(db, &subscription.member, now_ms)?,
            SubscriptionChange::Cancel => {
                db.execute(
                    "UPDATE owed_announcements SET settlement = 'cancelled'
                     WHERE subscription_id = ?1 AND batch_id IS NULL AND settlement IS NULL",
                    params![id.as_str()],
                )
                .map_err(storage)?;
                cancel_unwanted_sealed(db, id, now_ms)?;
            }
        }
    }
    let outcome = SubscriptionChangeOutcome {
        unresolved_batches: unresolved_batches_of(db, id)?,
        subscription,
    };
    record_operation(db, operation, &fingerprint, now_ms, &encode(&outcome)?)?;
    Ok(outcome)
}

/// End a live subscription: mark it ended at a new revision, settle what it
/// captured that no batch holds, and cancel its sealed batches nothing else
/// wants.
fn end_subscription(
    db: &Connection,
    subscription: &mut Subscription,
    end: SubscriptionEnd,
    now_ms: u64,
) -> Result<()> {
    subscription.state = SubscriptionState::Ended;
    subscription.ended = Some(end);
    subscription.revision += 1;
    subscription.updated_at_ms = now_ms;
    write_subscription(db, subscription)?;
    db.execute(
        "UPDATE owed_announcements SET settlement = 'ended'
         WHERE subscription_id = ?1 AND batch_id IS NULL AND settlement IS NULL",
        params![subscription.id.as_str()],
    )
    .map_err(storage)?;
    cancel_unwanted_sealed(db, &subscription.id, now_ms)
}

fn subscription_ids(db: &Connection, sql: &str, key: &str) -> Result<Vec<SubscriptionId>> {
    let mut statement = db.prepare_cached(sql).map_err(storage)?;
    let rows = statement
        .query_map(params![key], |row| row.get::<_, String>(0))
        .map_err(storage)?;
    rows.map(|row| SubscriptionId::new(row.map_err(storage)?))
        .collect()
}

fn end_member_in(
    db: &Connection,
    member: &Member,
    end: &SubscriptionEnd,
    now_ms: u64,
) -> Result<Vec<SubscriptionId>> {
    let ids = subscription_ids(
        db,
        "SELECT id FROM member_subscriptions
         WHERE member = ?1 AND state IN ('active', 'paused') ORDER BY id",
        &member.key(),
    )?;
    let mut ended = Vec::with_capacity(ids.len());
    for id in ids {
        let Some(mut subscription) = load_subscription(db, &id)? else {
            continue;
        };
        end_subscription(db, &mut subscription, end.clone(), now_ms)?;
        ended.push(id);
    }
    Ok(ended)
}

/// End the member's live subscriptions made before `cursor` of its own mob's
/// event log: a completion, a reset or a new incarnation at `cursor`
/// ended the mob they were made in.
fn end_member_before_in(
    db: &Connection,
    member: &Member,
    cursor: u64,
    end: &SubscriptionEnd,
    now_ms: u64,
) -> Result<Vec<SubscriptionId>> {
    let ids = subscription_ids(
        db,
        "SELECT id FROM member_subscriptions
         WHERE member = ?1 AND state IN ('active', 'paused') ORDER BY id",
        &member.key(),
    )?;
    let mut ended = Vec::new();
    for id in ids {
        let Some(mut subscription) = load_subscription(db, &id)? else {
            continue;
        };
        if subscription
            .member_mob_cursor
            .is_none_or(|made_at| made_at < cursor)
        {
            end_subscription(db, &mut subscription, end.clone(), now_ms)?;
            ended.push(id);
        }
    }
    Ok(ended)
}

fn member_route_in(db: &Connection, member: &Member) -> Result<Option<MemberRouteRecord>> {
    db.query_row(
        "SELECT record FROM member_routes WHERE member = ?1",
        params![member.key()],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(storage)?
    .map(|record| decode(&record))
    .transpose()
}

/// Keep `route` for the member with its first subscription. A member kept on
/// another route is refused: it is never rerouted.
fn keep_member_route_in(db: &Connection, member: &Member, route: &MemberRouteRecord) -> Result<()> {
    match member_route_in(db, member)? {
        Some(kept) if kept == *route => Ok(()),
        Some(kept) => Err(AnnouncementStoreError::RouteKept { kept: kept.route }),
        None => {
            db.execute(
                "INSERT INTO member_routes (member, route, record) VALUES (?1, ?2, ?3)",
                params![member.key(), route.route.as_str(), encode(route)?],
            )
            .map_err(storage)?;
            Ok(())
        }
    }
}

/// Refuse a subscription for a member of a mob hosted here that would pass
/// a bound on such subscriptions. The subscription it replaces, if any, is
/// not counted.
fn within_child_limits(db: &Connection, request: &PutSubscription) -> Result<()> {
    let limits = request.child_limits;
    let live: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM member_subscriptions s
             JOIN member_routes r ON r.member = s.member
             WHERE r.route = ?1 AND s.state IN ('active', 'paused') AND s.id != ?2",
            params![MemberRoute::Child.as_str(), request.id.as_str()],
            |row| row.get(0),
        )
        .map_err(storage)?;
    if uint(live)? >= u64::from(limits.subscriptions) {
        return Err(AnnouncementStoreError::ChildMemberLimit {
            limit: limits.subscriptions,
            mob_id: None,
        });
    }
    let mut statement = db
        .prepare_cached(
            "SELECT COUNT(*) FROM member_subscriptions s
             JOIN member_routes r ON r.member = s.member
             JOIN member_subscription_mobs m ON m.subscription_id = s.id
             WHERE r.route = ?1 AND s.state IN ('active', 'paused') AND s.id != ?2
               AND m.mob_id = ?3",
        )
        .map_err(storage)?;
    for mob_id in &request.selector.mobs {
        let naming: i64 = statement
            .query_row(
                params![
                    MemberRoute::Child.as_str(),
                    request.id.as_str(),
                    mob_id.as_str()
                ],
                |row| row.get(0),
            )
            .map_err(storage)?;
        if uint(naming)? >= u64::from(limits.per_mob) {
            return Err(AnnouncementStoreError::ChildMemberLimit {
                limit: limits.per_mob,
                mob_id: Some(mob_id.clone()),
            });
        }
    }
    Ok(())
}

/// End the member's live subscriptions and drop its kept route.
fn unbind_member_route_in(
    db: &Connection,
    member: &Member,
    end: &SubscriptionEnd,
    now_ms: u64,
) -> Result<Vec<SubscriptionId>> {
    let ended = end_member_in(db, member, end, now_ms)?;
    db.execute(
        "DELETE FROM member_routes WHERE member = ?1",
        params![member.key()],
    )
    .map_err(storage)?;
    Ok(ended)
}

fn child_member_routes_in(db: &Connection) -> Result<Vec<(Member, MemberRouteRecord)>> {
    let mut statement = db
        .prepare_cached("SELECT member, record FROM member_routes WHERE route = ?1 ORDER BY member")
        .map_err(storage)?;
    let rows = statement
        .query_map(params![MemberRoute::Child.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(storage)?;
    rows.map(|row| {
        let (member, record) = row.map_err(storage)?;
        Ok((Member::from_key(&member)?, decode(&record)?))
    })
    .collect()
}

fn member_mob_check_from_in(db: &Connection, member: &Member) -> Result<Option<u64>> {
    let ids = subscription_ids(
        db,
        "SELECT id FROM member_subscriptions
         WHERE member = ?1 AND state IN ('active', 'paused') ORDER BY id",
        &member.key(),
    )?;
    let mut earliest: Option<u64> = None;
    for id in ids {
        if let Some(made_at) = load_subscription(db, &id)?.and_then(|s| s.member_mob_cursor) {
            earliest = Some(earliest.map_or(made_at, |earliest| earliest.min(made_at)));
        }
    }
    let Some(earliest) = earliest else {
        return Ok(None);
    };
    let checked = db
        .query_row(
            "SELECT checked_through FROM member_routes WHERE member = ?1",
            params![member.key()],
            |row| row.get::<_, Option<i64>>(0),
        )
        .optional()
        .map_err(storage)?
        .flatten()
        .map(uint)
        .transpose()?;
    Ok(Some(
        checked.map_or(earliest, |checked| checked.max(earliest)),
    ))
}

fn record_member_mob_check_in(db: &Connection, member: &Member, through: u64) -> Result<()> {
    db.execute(
        "UPDATE member_routes SET checked_through = ?2
         WHERE member = ?1 AND (checked_through IS NULL OR checked_through < ?2)",
        params![member.key(), int(through)?],
    )
    .map_err(storage)?;
    Ok(())
}

/// Mobs that live subscriptions name, and mobs whose log is not marked
/// ended: every mob a destroy could leave behind here.
fn known_mobs_in(db: &Connection) -> Result<BTreeSet<MobId>> {
    let mut statement = db
        .prepare(
            "SELECT mob_id FROM member_subscription_mobs
             UNION SELECT mob_id FROM announcement_logs WHERE ended_at_ms IS NULL",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(storage)?;
    rows.map(|row| MobId::new(row.map_err(storage)?)).collect()
}

fn lineage_in(db: &Connection) -> Result<Vec<MobLineage>> {
    let mut statement = db
        .prepare("SELECT record FROM mob_lineage WHERE ended_at_ms IS NULL ORDER BY mob_id")
        .map_err(storage)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(storage)?;
    rows.map(|row| decode(&row.map_err(storage)?)).collect()
}

fn record_lineage_in(db: &Connection, lineage: &MobLineage, now_ms: u64) -> Result<bool> {
    let live = db
        .query_row(
            "SELECT record FROM mob_lineage WHERE mob_id = ?1 AND ended_at_ms IS NULL",
            params![lineage.mob_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(storage)?;
    if let Some(live) = live {
        let live: MobLineage = decode(&live)?;
        // Derived once: the same incarnation keeps its first record.
        if live.incarnation == lineage.incarnation {
            return Ok(false);
        }
        db.execute(
            "UPDATE mob_lineage SET ended_at_ms = ?2 WHERE mob_id = ?1 AND ended_at_ms IS NULL",
            params![lineage.mob_id.as_str(), int(now_ms)?],
        )
        .map_err(storage)?;
    }
    db.execute(
        "INSERT INTO mob_lineage (mob_id, parent_mob_id, ended_at_ms, record)
         VALUES (?1, ?2, NULL, ?3)",
        params![
            lineage.mob_id.as_str(),
            lineage.parent.as_ref().map(MobId::as_str),
            encode(lineage)?
        ],
    )
    .map_err(storage)?;
    Ok(true)
}

fn end_lineage_in(db: &Connection, mobs: &BTreeSet<MobId>, now_ms: u64) -> Result<()> {
    for mob_id in mobs {
        db.execute(
            "UPDATE mob_lineage SET ended_at_ms = ?2 WHERE mob_id = ?1 AND ended_at_ms IS NULL",
            params![mob_id.as_str(), int(now_ms)?],
        )
        .map_err(storage)?;
    }
    Ok(())
}

fn drop_ended_mobs_in(
    db: &Connection,
    ended: &BTreeSet<MobId>,
    now_ms: u64,
) -> Result<Vec<SubscriptionId>> {
    let mut naming = BTreeSet::new();
    for mob_id in ended {
        db.execute(
            "UPDATE announcement_logs SET ended_at_ms = ?2
             WHERE mob_id = ?1 AND ended_at_ms IS NULL",
            params![mob_id.as_str(), int(now_ms)?],
        )
        .map_err(storage)?;
        naming.extend(subscription_ids(
            db,
            "SELECT subscription_id FROM member_subscription_mobs WHERE mob_id = ?1",
            mob_id.as_str(),
        )?);
    }
    let mut changed = Vec::with_capacity(naming.len());
    for id in naming {
        let Some(mut subscription) = load_subscription(db, &id)? else {
            continue;
        };
        if !subscription.state.is_live() {
            continue;
        }
        let dropped = subscription
            .selector
            .mobs
            .intersection(ended)
            .cloned()
            .collect::<BTreeSet<_>>();
        if dropped.is_empty() {
            continue;
        }
        if dropped.len() == subscription.selector.mobs.len() {
            // The record keeps the selector it ended with.
            subscription.ended_mobs.extend(dropped);
            end_subscription(db, &mut subscription, SubscriptionEnd::MobsEnded, now_ms)?;
        } else {
            for mob_id in &dropped {
                subscription.selector.mobs.remove(mob_id);
                subscription.frontier.remove(mob_id);
            }
            subscription.ended_mobs.extend(dropped);
            subscription.revision += 1;
            subscription.selector_revision = subscription.revision;
            subscription.updated_at_ms = now_ms;
            write_subscription(db, &subscription)?;
        }
        changed.push(id);
    }
    Ok(changed)
}

fn backlogs_in(db: &Connection, limit: usize) -> Result<Vec<MemberBacklog>> {
    let mut statement = db
        .prepare(
            "SELECT o.member, COUNT(*), COALESCE(SUM(o.rendered_bytes), 0),
                    MIN(o.captured_at_ms)
             FROM owed_announcements o JOIN member_subscriptions s ON s.id = o.subscription_id
             WHERE o.settlement IS NULL AND o.batch_id IS NULL AND s.state = 'active'
               AND NOT EXISTS (
                 SELECT 1 FROM announcement_batches b WHERE b.member = o.member
                   AND b.state IN ('sealed', 'submitting', 'committed'))
               AND NOT EXISTS (
                 SELECT 1 FROM announcement_holds h WHERE h.subscription_id = o.subscription_id
                   AND h.mob_id = o.mob_id AND h.sequence = o.sequence)
             GROUP BY o.member
             ORDER BY MIN(o.captured_at_ms), o.member
             LIMIT ?1",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(params![page(limit)?], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .map_err(storage)?;
    let mut summaries = Vec::new();
    for row in rows {
        summaries.push(row.map_err(storage)?);
    }
    drop(statement);
    summaries
        .into_iter()
        .map(|(member, count, bytes, oldest)| {
            complete_backlog(
                db,
                Member::from_key(&member)?,
                uint(count)?,
                uint(bytes)?,
                uint(oldest)?,
            )
        })
        .collect()
}

fn seal_batch_in(
    db: &Connection,
    member: &Member,
    batch_id: &BatchId,
    policy: &SealingPolicy,
    facts: &RosterFacts,
    now_ms: u64,
) -> Result<Option<Batch>> {
    policy.validate()?;
    // What the member holds is checked afresh first: what passes now is
    // released and seals with the rest.
    release_holds(db, member, policy.max_count, facts, now_ms)?;
    let Some(backlog) = backlog_for(db, member)? else {
        return Ok(None);
    };
    if policy.due_at(&backlog).is_none_or(|due| due > now_ms) {
        return Ok(None);
    }
    if load_batch(db, batch_id)?.is_some() {
        return Err(AnnouncementStoreError::Invalid(format!(
            "batch id {batch_id} is already used"
        )));
    }
    // Every subscription keeps its member's route, so a member with owed
    // captures has one; none is assumed when it is gone. The batch is sealed
    // on this route, and its wire budget comes from it, in this transaction.
    let route = member_route_in(db, member)?
        .ok_or_else(|| AnnouncementStoreError::RouteNotKept {
            member: member.clone(),
        })?
        .route;
    let wire_budget = wire_budget(route);
    let mut statement = db
        .prepare(
            "SELECT o.subscription_id, o.subscription_revision, o.mob_id, o.sequence,
                    o.rendered_bytes
             FROM owed_announcements o JOIN member_subscriptions s ON s.id = o.subscription_id
             WHERE o.member = ?1 AND o.settlement IS NULL AND o.batch_id IS NULL
               AND s.state = 'active'
               AND NOT EXISTS (
                 SELECT 1 FROM announcement_holds h WHERE h.subscription_id = o.subscription_id
                   AND h.mob_id = o.mob_id AND h.sequence = o.sequence)
             ORDER BY o.captured_at_ms, o.mob_id, o.sequence, o.subscription_id
             LIMIT ?2",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(params![member.key(), i64::from(policy.max_count)], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .map_err(storage)?;
    let mut candidates = Vec::new();
    for row in rows {
        candidates.push(row.map_err(storage)?);
    }
    drop(statement);
    let mut owed = Vec::new();
    let mut announcements = BTreeMap::new();
    let mut items = Vec::new();
    let mut bytes = 0_u64;
    // The encoded body is its header, its gap lines and each item's
    // encoding without the quotes: JSON escapes character by character.
    let mut wire = wire_budget.map_or(0, |_| header_wire_bound(batch_id));
    let mut subscriptions: BTreeMap<SubscriptionId, Option<Subscription>> = BTreeMap::new();
    for (subscription, revision, mob_id, sequence, rendered) in candidates {
        let key = (MobId::new(mob_id)?, uint(sequence)?);
        let rendered = uint(rendered)?;
        let subscription_id = SubscriptionId::new(subscription)?;
        // Each item is read once, by the first subscription that names it.
        let fresh = if announcements.contains_key(&key) {
            None
        } else {
            Some(announcement_at(db, &key.0, key.1)?)
        };
        // An item its member no longer passes is held, not sealed.
        if !subscriptions.contains_key(&subscription_id) {
            let loaded = load_subscription(db, &subscription_id)?;
            subscriptions.insert(subscription_id.clone(), loaded);
        }
        let announcement = fresh.as_ref().or_else(|| announcements.get(&key));
        if let (Some(Some(owner)), Some(announcement)) =
            (subscriptions.get(&subscription_id), announcement)
            && let Some(refusal) = owed_refusal(db, owner, announcement, facts)?
        {
            hold_owed(db, &subscription_id, &key, member, &refusal, now_ms)?;
            continue;
        }
        if let Some(announcement) = fresh {
            let item = render_item(&announcement);
            let item_wire = wire_len(&item).saturating_sub(2);
            let over_bytes = bytes.saturating_add(rendered) > policy.max_bytes;
            let over_wire =
                wire_budget.is_some_and(|budget| wire.saturating_add(item_wire) > budget);
            if !items.is_empty() && (over_bytes || over_wire) {
                break;
            }
            bytes = bytes.saturating_add(rendered);
            wire = wire.saturating_add(item_wire);
            announcements.insert(key.clone(), announcement);
            items.push(item);
        }
        owed.push((subscription_id, uint(revision)?, key));
    }
    if owed.is_empty() {
        return Ok(None);
    }
    // What the member was owed and never got rides on its next batch, as
    // many gap lines as fit what the items left of the budget; the rest
    // wait for a later batch.
    let mut gaps = Vec::new();
    for gap in pending_gaps(db, member)? {
        let line = wire_len(&render_gap(&gap)).saturating_sub(2);
        if wire_budget.is_some_and(|budget| wire.saturating_add(line) > budget) {
            break;
        }
        wire = wire.saturating_add(line);
        gaps.push(gap);
    }
    let body = render_batch(batch_id, None, &gaps, &items);
    for gap in &gaps {
        db.execute(
            "DELETE FROM announcement_gaps WHERE member = ?1 AND mob_id = ?2 AND reason = ?3",
            params![member.key(), gap.mob_id.as_str(), gap.reason.as_str()],
        )
        .map_err(storage)?;
    }
    let mut batch_items = Vec::with_capacity(owed.len());
    for (subscription_id, subscription_revision, key) in owed {
        let source_key = announcements
            .get(&key)
            .map(|announcement| announcement.source_key.clone())
            .ok_or_else(|| storage("a sealed item lost its announcement"))?;
        db.execute(
            "UPDATE owed_announcements SET batch_id = ?4
             WHERE subscription_id = ?1 AND mob_id = ?2 AND sequence = ?3",
            params![
                subscription_id.as_str(),
                key.0.as_str(),
                int(key.1)?,
                batch_id.as_str(),
            ],
        )
        .map_err(storage)?;
        batch_items.push(BatchItem {
            subscription_id,
            subscription_revision,
            mob_id: key.0,
            sequence: key.1,
            source_key,
        });
    }
    let batch = Batch {
        id: batch_id.clone(),
        member: member.clone(),
        items: batch_items,
        gaps,
        route,
        content_digest: content_digest(&body),
        body,
        predecessor: None,
        successor: None,
        state: BatchState::Sealed,
        sealed_at_ms: now_ms,
        updated_at_ms: now_ms,
    };
    insert_batch(db, &batch)?;
    mark_sealed(db, member, now_ms)?;
    Ok(Some(batch))
}

fn transition_batch_in(
    db: &Connection,
    id: &BatchId,
    expected: BatchStateTag,
    event: &BatchEvent,
    now_ms: u64,
) -> Result<Batch> {
    let mut batch = load_batch(db, id)?.ok_or(AnnouncementStoreError::UnknownBatch)?;
    let current = batch.state.tag();
    if current != expected {
        return Err(AnnouncementStoreError::BatchMoved { current });
    }
    let next = batch.state.apply(event)?;
    if let BatchEvent::NativeStranded {
        successor: Some(successor),
        ..
    } = event
    {
        if load_batch(db, &successor.id)?.is_some() {
            return Err(AnnouncementStoreError::Invalid(format!(
                "batch id {} is already used",
                successor.id
            )));
        }
        let successor_batch = Batch {
            id: successor.id.clone(),
            member: batch.member.clone(),
            items: batch.items.clone(),
            gaps: successor.gaps.clone(),
            route: batch.route,
            body: successor.body.clone(),
            content_digest: content_digest(&successor.body),
            predecessor: Some(batch.id.clone()),
            successor: None,
            state: BatchState::Sealed,
            sealed_at_ms: now_ms,
            updated_at_ms: now_ms,
        };
        insert_batch(db, &successor_batch)?;
        db.execute(
            "UPDATE owed_announcements SET batch_id = ?2
             WHERE batch_id = ?1 AND settlement IS NULL",
            params![batch.id.as_str(), successor.id.as_str()],
        )
        .map_err(storage)?;
        mark_sealed(db, &batch.member, now_ms)?;
        // What the successor had no room for reaches the member's next batch.
        add_gaps(db, &batch.member, &successor.deferred)?;
        batch.successor = Some(successor.id.clone());
    }
    let held_as = match event {
        BatchEvent::Expire | BatchEvent::ExpireNotCommitted => Some(GapReason::Expired),
        BatchEvent::ExpireUnverified => Some(GapReason::DeliveryNotConfirmed),
        BatchEvent::ExpireUnconfirmed => Some(GapReason::AdmissionNotConfirmed),
        _ => None,
    };
    if let Some(reason) = held_as {
        // The member learns of what this batch held, and of the gaps it was
        // to report, from its next batch: a batch whose delivery was not
        // confirmed may still have reached the member, so those lines may
        // reach it twice, never not at all.
        let held = batch_gaps(db, &batch.id, reason)?;
        add_gaps(db, &batch.member, &merge_gaps(&batch.gaps, &held))?;
    } else if matches!(
        next.tag(),
        BatchStateTag::Cancelled
            | BatchStateTag::Refused
            | BatchStateTag::Blocked
            | BatchStateTag::Stranded
    ) && batch.successor.is_none()
    {
        // A batch that reports nothing hands its gap lines to the member's
        // next batch; a successor already carries them.
        add_gaps(db, &batch.member, &batch.gaps)?;
    }
    if let Some(settlement) = event.settlement() {
        settle_owed(db, &batch.id, settlement.as_str())?;
    }
    batch.state = next;
    batch.updated_at_ms = now_ms;
    update_batch(db, &batch)?;
    if BatchStateTag::UNCONFIRMED.contains(&batch.state.tag()) {
        // What its delivery came to is read again, first at the end of the
        // round that settled it.
        set_late_read(db, &batch.id, Some(now_ms))?;
    }
    Ok(batch)
}

/// A mob's oldest retained announcement: (announced time, mob id,
/// sequence).
fn oldest_announcement(db: &Connection, mob_id: &str) -> Result<Option<(i64, String, i64)>> {
    db.query_row(
        "SELECT sequence, announced_at_ms FROM announcements
         WHERE mob_id = ?1 ORDER BY sequence LIMIT 1",
        params![mob_id],
        |row| {
            Ok((
                row.get::<_, i64>(1)?,
                mob_id.to_string(),
                row.get::<_, i64>(0)?,
            ))
        },
    )
    .optional()
    .map_err(storage)
}

fn expire_in(
    db: &Connection,
    now_ms: u64,
    retention_ms: u64,
    quiet_ms: u64,
    limit: usize,
) -> Result<ExpiryReport> {
    let cutoff = int(now_ms.saturating_sub(retention_ms))?;
    let quiet_cutoff = int(now_ms.saturating_sub(quiet_ms))?;
    let budget = u64::try_from(limit.max(1)).unwrap_or(u64::MAX);
    let mut report = ExpiryReport::default();

    // Held items past the window expire with their own audit entry, and no
    // gap line tells their member of them: it may not learn of what it may
    // not receive.
    let mut statement = db
        .prepare(
            "SELECT h.subscription_id, h.mob_id, h.sequence, h.member, h.record
             FROM announcement_holds h JOIN owed_announcements o
               ON o.subscription_id = h.subscription_id AND o.mob_id = h.mob_id
              AND o.sequence = h.sequence
             WHERE o.batch_id IS NULL AND o.settlement IS NULL AND o.captured_at_ms <= ?1
             ORDER BY o.captured_at_ms, h.member, h.mob_id, h.sequence LIMIT ?2",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(params![cutoff, int(budget.saturating_add(1))?], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .map_err(storage)?;
    let mut held = Vec::new();
    for row in rows {
        held.push(row.map_err(storage)?);
    }
    drop(statement);
    if held.len() as u64 > budget {
        held.truncate(usize::try_from(budget).unwrap_or(usize::MAX));
        report.more = true;
    }
    for (subscription_id, mob_id, sequence, member, record) in held {
        let refusal: OwedRefusal = decode(&record)?;
        db.execute(
            "UPDATE owed_announcements SET settlement = 'expired'
             WHERE subscription_id = ?1 AND mob_id = ?2 AND sequence = ?3
               AND batch_id IS NULL AND settlement IS NULL",
            params![subscription_id, mob_id, sequence],
        )
        .map_err(storage)?;
        db.execute(
            "DELETE FROM announcement_holds
             WHERE subscription_id = ?1 AND mob_id = ?2 AND sequence = ?3",
            params![subscription_id, mob_id, sequence],
        )
        .map_err(storage)?;
        count_audit(
            db,
            &AuditKey {
                mob_id: MobId::new(mob_id)?,
                source_id: refusal.source_id,
                generation: refusal.generation,
                member: Member::from_key(&member)?,
                action: AuditAction::ExpiredWhileHeld,
                reason: refusal.reason,
            },
            now_ms,
        )?;
        report.captures += 1;
    }

    // Captures no batch holds, owed longer than the window: each
    // announcement once per member, oldest first.
    let mut statement = db
        .prepare(
            "SELECT member, mob_id, sequence, MIN(captured_at_ms) AS at FROM owed_announcements o
             WHERE batch_id IS NULL AND settlement IS NULL AND captured_at_ms <= ?1
               AND NOT EXISTS (
                 SELECT 1 FROM announcement_holds h WHERE h.subscription_id = o.subscription_id
                   AND h.mob_id = o.mob_id AND h.sequence = o.sequence)
             GROUP BY member, mob_id, sequence ORDER BY at, member, mob_id, sequence LIMIT ?2",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(params![cutoff, int(budget.saturating_add(1))?], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(storage)?;
    let mut expired = Vec::new();
    for row in rows {
        expired.push(row.map_err(storage)?);
    }
    drop(statement);
    if expired.len() as u64 > budget {
        expired.truncate(usize::try_from(budget).unwrap_or(usize::MAX));
        report.more = true;
    }
    let mut gaps = BTreeMap::<String, Vec<BatchGap>>::new();
    for (member, mob_id, sequence) in expired {
        db.execute(
            "UPDATE owed_announcements SET settlement = 'expired'
             WHERE member = ?1 AND mob_id = ?2 AND sequence = ?3
               AND batch_id IS NULL AND settlement IS NULL
               AND NOT EXISTS (
                 SELECT 1 FROM announcement_holds h
                 WHERE h.subscription_id = owed_announcements.subscription_id
                   AND h.mob_id = owed_announcements.mob_id
                   AND h.sequence = owed_announcements.sequence)",
            params![member, mob_id, sequence],
        )
        .map_err(storage)?;
        let gap = BatchGap {
            mob_id: MobId::new(mob_id)?,
            count: 1,
            reason: GapReason::Expired,
        };
        let member_gaps = gaps.entry(member).or_default();
        *member_gaps = merge_gaps(member_gaps, &[gap]);
        report.captures += 1;
    }
    for (member, member_gaps) in gaps {
        add_gaps(db, &Member::from_key(&member)?, &member_gaps)?;
    }

    // Sealed batches nothing native holds, sealed before the window: every
    // item they hold is at least that old. A batch owed again after a
    // refused submission was submitted before, and an earlier request for
    // it may still commit until the quiet window after its last request
    // has passed, so it waits that out first.
    let mut statement = db
        .prepare(
            "SELECT id FROM announcement_batches
             WHERE state = 'sealed' AND sealed_at_ms <= ?1 AND updated_at_ms <= ?3
             ORDER BY sealed_at_ms, id LIMIT ?2",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(
            params![cutoff, int(budget.saturating_add(1))?, quiet_cutoff],
            |row| row.get::<_, String>(0),
        )
        .map_err(storage)?;
    let mut sealed = Vec::new();
    for row in rows {
        sealed.push(BatchId::new(row.map_err(storage)?)?);
    }
    drop(statement);
    if sealed.len() as u64 > budget {
        sealed.truncate(usize::try_from(budget).unwrap_or(usize::MAX));
        report.more = true;
    }
    for id in sealed {
        transition_batch_in(db, &id, BatchStateTag::Sealed, &BatchEvent::Expire, now_ms)?;
        report.batches += 1;
    }
    Ok(report)
}

fn prune_in(db: &Connection, now_ms: u64, retention_ms: u64, limit: usize) -> Result<PruneReport> {
    let cutoff = int(now_ms.saturating_sub(retention_ms))?;
    let budget = u64::try_from(limit.max(1)).unwrap_or(u64::MAX);
    let mut report = PruneReport::default();

    // Announcements, oldest first across mobs. A mob stops at its first
    // announcement that is still owed or inside the window, so its retained
    // history never has holes. A mob's oldest retained announcement is the
    // one at its earliest retained sequence, so one read finds every mob's
    // that is old enough, and a quiet mob costs one index lookup.
    let mut candidates = std::collections::BinaryHeap::new();
    {
        let mut statement = db
            .prepare(
                "SELECT a.announced_at_ms, a.mob_id, a.sequence FROM announcement_logs l
                 JOIN announcements a ON a.mob_id = l.mob_id AND a.sequence = l.earliest_retained
                 WHERE a.announced_at_ms <= ?1",
            )
            .map_err(storage)?;
        let rows = statement
            .query_map(params![cutoff], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(storage)?;
        for row in rows {
            candidates.push(std::cmp::Reverse(row.map_err(storage)?));
        }
    }
    let mut touched = BTreeSet::new();
    while let Some(std::cmp::Reverse((announced_at, mob_id, sequence))) = candidates.pop() {
        if announced_at > cutoff {
            continue;
        }
        // Still owed by a capture no batch holds, or by a batch nothing
        // native holds yet: kept until it is delivered or expires. A batch
        // the native owner may hold already carries its own body, so it
        // keeps only its owed rows, never the announcement.
        let owed: bool = db
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM owed_announcements o
                   LEFT JOIN announcement_batches b ON b.id = o.batch_id
                   WHERE o.mob_id = ?1 AND o.sequence = ?2 AND o.settlement IS NULL
                     AND (o.batch_id IS NULL OR b.state = 'sealed'))",
                params![mob_id, sequence],
                |row| row.get(0),
            )
            .map_err(storage)?;
        if owed {
            continue;
        }
        if report.announcements >= budget {
            report.more = true;
            break;
        }
        db.execute(
            "DELETE FROM owed_announcements
             WHERE mob_id = ?1 AND sequence = ?2 AND settlement IS NOT NULL",
            params![mob_id, sequence],
        )
        .map_err(storage)?;
        for table in ["announcement_topics", "announcements"] {
            db.execute(
                &format!("DELETE FROM {table} WHERE mob_id = ?1 AND sequence = ?2"),
                params![mob_id, sequence],
            )
            .map_err(storage)?;
        }
        report.announcements += 1;
        if let Some(next) = oldest_announcement(db, &mob_id)? {
            candidates.push(std::cmp::Reverse(next));
        }
        touched.insert(mob_id);
    }
    // Only a mob that lost an announcement has a new earliest retained
    // sequence.
    for mob_id in &touched {
        let (head, earliest): (i64, Option<i64>) = db
            .query_row(
                "SELECT head_sequence,
                        (SELECT MIN(sequence) FROM announcements WHERE mob_id = ?1)
                 FROM announcement_logs WHERE mob_id = ?1",
                params![mob_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(storage)?;
        let earliest = match earliest {
            Some(sequence) => uint(sequence)?,
            None => uint(head)?.saturating_add(1),
        };
        db.execute(
            "UPDATE announcement_logs SET earliest_retained = ?2 WHERE mob_id = ?1",
            params![mob_id, int(earliest)?],
        )
        .map_err(storage)?;
    }

    // Owed rows a native-held batch kept after its announcement was removed,
    // once that batch settled them.
    let orphaned = db
        .execute(
            "DELETE FROM owed_announcements WHERE rowid IN (
               SELECT o.rowid FROM owed_announcements o
               WHERE o.settlement IS NOT NULL AND NOT EXISTS (SELECT 1 FROM announcements a
                 WHERE a.mob_id = o.mob_id AND a.sequence = o.sequence)
               LIMIT ?1)",
            params![int(budget)?],
        )
        .map_err(storage)? as u64;
    report.more |= orphaned >= budget;

    // Settled batches no owed announcement still names, oldest first, with their
    // own budget.
    let batch_ids = {
        let mut statement = db
            .prepare(
                "SELECT b.id FROM announcement_batches b
                 WHERE b.state NOT IN ('sealed', 'submitting', 'committed')
                   AND b.updated_at_ms <= ?1
                   AND NOT EXISTS (SELECT 1 FROM owed_announcements o WHERE o.batch_id = b.id)
                 ORDER BY b.updated_at_ms LIMIT ?2",
            )
            .map_err(storage)?;
        let rows = statement
            .query_map(params![cutoff, int(budget.saturating_add(1))?], |row| {
                row.get::<_, String>(0)
            })
            .map_err(storage)?;
        let mut ids = Vec::new();
        for row in rows {
            ids.push(row.map_err(storage)?);
        }
        ids
    };
    for id in &batch_ids {
        if report.batches >= budget {
            report.more = true;
            break;
        }
        db.execute(
            "DELETE FROM announcement_batches WHERE id = ?1",
            params![id],
        )
        .map_err(storage)?;
        db.execute(
            "DELETE FROM announcement_batch_subscriptions WHERE batch_id = ?1",
            params![id],
        )
        .map_err(storage)?;
        report.batches += 1;
    }

    // Operation receipts, oldest first, with their own budget.
    report.operation_receipts = db
        .execute(
            "DELETE FROM announcement_operations WHERE operation_id IN (
               SELECT operation_id FROM announcement_operations WHERE created_at_ms <= ?1
               ORDER BY created_at_ms LIMIT ?2)",
            params![cutoff, int(budget)?],
        )
        .map_err(storage)? as u64;
    if report.operation_receipts >= budget {
        let left: bool = db
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM announcement_operations WHERE created_at_ms <= ?1)",
                params![cutoff],
                |row| row.get(0),
            )
            .map_err(storage)?;
        report.more |= left;
    }

    // Cancelled and ended subscriptions that changed before the window and
    // hold nothing unsettled, oldest first, with their own budget; then the
    // member rows and the logs of ended mobs nothing refers to any more.
    let ended_ids = {
        let mut statement = db
            .prepare(
                "SELECT s.id FROM member_subscriptions s
                 WHERE s.state IN ('cancelled', 'ended') AND s.updated_at_ms <= ?1
                   AND NOT EXISTS (SELECT 1 FROM owed_announcements o
                     WHERE o.subscription_id = s.id AND o.settlement IS NULL)
                   AND NOT EXISTS (SELECT 1 FROM announcement_batch_subscriptions m
                     JOIN announcement_batches b ON b.id = m.batch_id
                     WHERE m.subscription_id = s.id
                       AND b.state IN ('sealed', 'submitting', 'committed'))
                 ORDER BY s.updated_at_ms, s.id LIMIT ?2",
            )
            .map_err(storage)?;
        let rows = statement
            .query_map(params![cutoff, int(budget.saturating_add(1))?], |row| {
                row.get::<_, String>(0)
            })
            .map_err(storage)?;
        let mut ids = Vec::new();
        for row in rows {
            ids.push(row.map_err(storage)?);
        }
        ids
    };
    for id in &ended_ids {
        if report.subscriptions >= budget {
            report.more = true;
            break;
        }
        for table in [
            "member_subscription_topics",
            "member_subscription_mobs",
            "owed_announcements",
            "announcement_batch_subscriptions",
        ] {
            db.execute(
                &format!("DELETE FROM {table} WHERE subscription_id = ?1"),
                params![id],
            )
            .map_err(storage)?;
        }
        db.execute(
            "DELETE FROM member_subscriptions WHERE id = ?1",
            params![id],
        )
        .map_err(storage)?;
        report.subscriptions += 1;
    }
    let members = db
        .execute(
            "DELETE FROM announcement_members WHERE member IN (
               SELECT m.member FROM announcement_members m
               WHERE NOT EXISTS (SELECT 1 FROM member_subscriptions s WHERE s.member = m.member)
                 AND NOT EXISTS (SELECT 1 FROM announcement_batches b WHERE b.member = m.member)
               LIMIT ?1)",
            params![int(budget)?],
        )
        .map_err(storage)? as u64;
    let logs = db
        .execute(
            "DELETE FROM announcement_logs WHERE mob_id IN (
               SELECT l.mob_id FROM announcement_logs l
               WHERE l.ended_at_ms IS NOT NULL AND l.earliest_retained > l.head_sequence
                 AND NOT EXISTS (SELECT 1 FROM member_subscription_mobs m
                   WHERE m.mob_id = l.mob_id)
               LIMIT ?1)",
            params![int(budget)?],
        )
        .map_err(storage)? as u64;
    report.more |= members >= budget || logs >= budget;
    // A kept route goes with the last subscription and batch of its member.
    db.execute(
        "DELETE FROM member_routes WHERE NOT EXISTS (SELECT 1 FROM member_subscriptions s
           WHERE s.member = member_routes.member)
           AND NOT EXISTS (SELECT 1 FROM announcement_batches b
           WHERE b.member = member_routes.member)",
        [],
    )
    .map_err(storage)?;
    // Gaps of a member with no live subscription have no one to reach.
    db.execute(
        "DELETE FROM announcement_gaps WHERE NOT EXISTS (SELECT 1 FROM member_subscriptions s
           WHERE s.member = announcement_gaps.member AND s.state IN ('active', 'paused'))",
        [],
    )
    .map_err(storage)?;
    report.lineage = db
        .execute(
            "DELETE FROM mob_lineage WHERE row_id IN (
               SELECT l.row_id FROM mob_lineage l
               WHERE l.ended_at_ms IS NOT NULL AND l.ended_at_ms <= ?1
                 AND NOT EXISTS (SELECT 1 FROM mob_lineage c
                   WHERE c.parent_mob_id = l.mob_id AND c.ended_at_ms IS NULL)
               ORDER BY l.row_id LIMIT ?2)",
            params![cutoff, int(budget)?],
        )
        .map_err(storage)? as u64;
    report.more |= report.lineage >= budget;

    // Audit entries are kept for the retention window from their last count.
    let audited = db
        .execute(
            "DELETE FROM announcement_audit WHERE id IN (
               SELECT id FROM announcement_audit WHERE last_at_ms <= ?1 ORDER BY id LIMIT ?2)",
            params![cutoff, int(budget)?],
        )
        .map_err(storage)? as u64;
    report.more |= audited >= budget;

    // A hold whose item is no longer open (its subscription ended, or the
    // item settled otherwise) holds nothing.
    let orphans = db
        .execute(
            "DELETE FROM announcement_holds WHERE rowid IN (
               SELECT h.rowid FROM announcement_holds h WHERE NOT EXISTS (
                 SELECT 1 FROM owed_announcements o
                 WHERE o.subscription_id = h.subscription_id AND o.mob_id = h.mob_id
                   AND o.sequence = h.sequence AND o.settlement IS NULL
                   AND o.batch_id IS NULL)
               LIMIT ?1)",
            params![int(budget)?],
        )
        .map_err(storage)? as u64;
    report.more |= orphans >= budget;
    Ok(report)
}

// ---------------------------------------------------------------------------
// Announcement sources
// ---------------------------------------------------------------------------

fn load_source(
    db: &Connection,
    mob_id: &MobId,
    source_id: &SourceId,
) -> Result<Option<AnnouncementSource>> {
    let record: Option<String> = db
        .query_row(
            "SELECT record FROM announcement_sources WHERE mob_id = ?1 AND source_id = ?2",
            params![mob_id.as_str(), source_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    record.as_deref().map(decode).transpose()
}

fn write_source(db: &Connection, source: &AnnouncementSource) -> Result<()> {
    db.execute(
        "INSERT INTO announcement_sources
         (mob_id, source_id, generation, state, identity_key, updated_at_ms, record)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(mob_id, source_id) DO UPDATE SET generation = excluded.generation,
           state = excluded.state, identity_key = excluded.identity_key,
           updated_at_ms = excluded.updated_at_ms, record = excluded.record",
        params![
            source.mob_id.as_str(),
            source.source_id.as_str(),
            int(source.generation)?,
            source.state.as_str(),
            source.identity.key()?,
            int(source.updated_at_ms)?,
            encode(source)?,
        ],
    )
    .map_err(storage)?;
    Ok(())
}

/// The source that owns `topic` in `mob_id`, with the kind of ownership.
fn topic_owner(
    db: &Connection,
    mob_id: &MobId,
    topic: &Topic,
) -> Result<Option<(SourceId, TopicOwnership)>> {
    let row: Option<(String, String)> = db
        .query_row(
            "SELECT source_id, kind FROM announcement_source_topics
             WHERE mob_id = ?1 AND topic = ?2",
            params![mob_id.as_str(), topic.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(storage)?;
    row.map(|(source_id, kind)| Ok((SourceId::new(source_id)?, TopicOwnership::parse(&kind)?)))
        .transpose()
}

/// Refuse an announcement on a topic a source owns, unless it is that
/// source's own item on one of its topics, or the triage member of the
/// source's route announcing on that route's forward topic while the source
/// is not ended.
fn ensure_topics_open(db: &Connection, request: &AnnounceRequest) -> Result<()> {
    for topic in &request.topics {
        let Some((source_id, kind)) = topic_owner(db, &request.mob_id, topic)? else {
            continue;
        };
        let allowed = match (&request.announced_by, kind) {
            (Announcer::Source { source_id: own }, TopicOwnership::Source) => *own == source_id,
            (Announcer::Member { identity }, TopicOwnership::Forward) => {
                load_source(db, &request.mob_id, &source_id)?.is_some_and(|source| {
                    source.state != SourceState::Ended
                        && source
                            .config()
                            .forwarding_member(topic)
                            .is_some_and(|member| {
                                member.mob_id == request.mob_id && member.identity == *identity
                            })
                })
            }
            _ => false,
        };
        if !allowed {
            return Err(AnnouncementStoreError::TopicOwnedBySource {
                topic: topic.clone(),
                source_id,
            });
        }
    }
    Ok(())
}

/// Claim every topic `source` owns, refusing one another source owns. A
/// topic the source no longer names stays its own: ownership is never
/// released here.
fn claim_topics(db: &Connection, source: &AnnouncementSource) -> Result<()> {
    for (topic, kind) in source.config().owned_topics() {
        if let Some((owner, _)) = topic_owner(db, &source.mob_id, &topic)?
            && owner != source.source_id
        {
            return Err(AnnouncementStoreError::TopicOwned {
                topic,
                source_id: owner,
            });
        }
        db.execute(
            "INSERT INTO announcement_source_topics (mob_id, topic, source_id, kind)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(mob_id, topic) DO UPDATE SET kind = excluded.kind",
            params![
                source.mob_id.as_str(),
                topic.as_str(),
                source.source_id.as_str(),
                kind.as_str(),
            ],
        )
        .map_err(storage)?;
    }
    Ok(())
}

/// The refusal of a change at a revision the source has moved past.
fn source_stale(source: &AnnouncementSource) -> AnnouncementStoreError {
    AnnouncementStoreError::SourceStaleRevision {
        revision: source.revision,
        generation: source.generation,
        cursor: source.cursor.clone(),
    }
}

/// Refuse a source of a principal's scope another live source of the mob
/// already reads, and a source past the live-source bounds.
fn ensure_source_admitted(db: &Connection, source: &AnnouncementSource) -> Result<()> {
    let taken: Option<String> = db
        .query_row(
            "SELECT source_id FROM announcement_sources
             WHERE mob_id = ?1 AND identity_key = ?2 AND state != 'ended' AND source_id != ?3
             ORDER BY source_id LIMIT 1",
            params![
                source.mob_id.as_str(),
                source.identity.key()?,
                source.source_id.as_str(),
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    if let Some(taken) = taken {
        return Err(AnnouncementStoreError::SourceIdentityTaken {
            source_id: SourceId::new(taken)?,
        });
    }
    let (in_mob, in_gateway): (i64, i64) = db
        .query_row(
            "SELECT COUNT(*) FILTER (WHERE mob_id = ?1), COUNT(*) FROM announcement_sources
             WHERE state != 'ended' AND NOT (mob_id = ?1 AND source_id = ?2)",
            params![source.mob_id.as_str(), source.source_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(storage)?;
    if uint(in_mob)? >= u64::from(MAX_SOURCES_PER_MOB) {
        return Err(AnnouncementStoreError::SourceLimit {
            limit: MAX_SOURCES_PER_MOB,
            mob_id: Some(source.mob_id.clone()),
        });
    }
    if uint(in_gateway)? >= u64::from(MAX_SOURCES_PER_GATEWAY) {
        return Err(AnnouncementStoreError::SourceLimit {
            limit: MAX_SOURCES_PER_GATEWAY,
            mob_id: None,
        });
    }
    Ok(())
}

fn put_source_in(
    db: &Connection,
    operation: &OperationId,
    request: &PutSource,
    now_ms: u64,
) -> Result<AnnouncementSource> {
    let fingerprint = request_fingerprint("put_source", request)?;
    if let Some(result) = operation_result(db, operation, &fingerprint)? {
        return decode(&result);
    }
    request.config.validate()?;
    request.config.validate_for(&request.mob_id)?;
    let config = request.config.clone();
    let existing = load_source(db, &request.mob_id, &request.source_id)?;
    let source = match (existing, request.expected_revision) {
        (None, None) => AnnouncementSource::begin(
            request.mob_id.clone(),
            request.source_id.clone(),
            1,
            1,
            config,
            now_ms,
        ),
        (None, Some(_)) => return Err(AnnouncementStoreError::UnknownSource),
        // Declaring an ended source again starts its next generation; the
        // revision keeps rising across generations.
        (Some(existing), None) if existing.state == SourceState::Ended => {
            AnnouncementSource::begin(
                request.mob_id.clone(),
                request.source_id.clone(),
                existing.generation + 1,
                existing.revision + 1,
                config,
                now_ms,
            )
        }
        (Some(existing), None) => return Err(source_stale(&existing)),
        (Some(existing), Some(_)) if existing.state == SourceState::Ended => {
            return Err(AnnouncementStoreError::SourceEnded);
        }
        (Some(existing), Some(expected)) if existing.revision != expected => {
            return Err(source_stale(&existing));
        }
        (Some(existing), Some(_)) if request.new_generation => AnnouncementSource::begin(
            request.mob_id.clone(),
            request.source_id.clone(),
            existing.generation + 1,
            existing.revision + 1,
            config,
            now_ms,
        ),
        (Some(mut existing), Some(_)) => {
            if existing.identity != config.identity {
                return Err(AnnouncementStoreError::IdentityImmutable);
            }
            existing.reconfigure(config);
            existing.revision += 1;
            existing.updated_at_ms = now_ms;
            existing.refresh_status();
            existing
        }
    };
    ensure_source_admitted(db, &source)?;
    ensure_routes_admitted(db, &source)?;
    claim_topics(db, &source)?;
    // A new generation onboards its streams again.
    db.execute(
        "DELETE FROM announcement_source_streams
         WHERE mob_id = ?1 AND source_id = ?2 AND generation != ?3",
        params![
            source.mob_id.as_str(),
            source.source_id.as_str(),
            int(source.generation)?,
        ],
    )
    .map_err(storage)?;
    write_source(db, &source)?;
    record_operation(db, operation, &fingerprint, now_ms, &encode(&source)?)?;
    Ok(source)
}

#[derive(Serialize)]
struct SourceChangeRequest<'a> {
    mob_id: &'a MobId,
    source_id: &'a SourceId,
    expected_revision: u64,
    change: SourceChange,
}

fn change_source_in(
    db: &Connection,
    operation: &OperationId,
    mob_id: &MobId,
    source_id: &SourceId,
    expected_revision: u64,
    change: SourceChange,
    now_ms: u64,
) -> Result<AnnouncementSource> {
    let fingerprint = request_fingerprint(
        "change_source",
        &SourceChangeRequest {
            mob_id,
            source_id,
            expected_revision,
            change,
        },
    )?;
    if let Some(result) = operation_result(db, operation, &fingerprint)? {
        return decode(&result);
    }
    let mut source =
        load_source(db, mob_id, source_id)?.ok_or(AnnouncementStoreError::UnknownSource)?;
    if source.state == SourceState::Ended {
        return Err(AnnouncementStoreError::SourceEnded);
    }
    if source.revision != expected_revision {
        return Err(source_stale(&source));
    }
    let changed = match change {
        SourceChange::Pause => {
            let changed = source.state != SourceState::Paused;
            source.state = SourceState::Paused;
            changed
        }
        SourceChange::Resume { rebaseline } => {
            let changed = source.state != SourceState::Active || rebaseline;
            source.state = SourceState::Active;
            if rebaseline {
                source.rebaseline();
            }
            changed
        }
        SourceChange::End => {
            source.state = SourceState::Ended;
            true
        }
    };
    if changed {
        source.revision += 1;
        source.updated_at_ms = now_ms;
        source.refresh_status();
        write_source(db, &source)?;
    }
    record_operation(db, operation, &fingerprint, now_ms, &encode(&source)?)?;
    Ok(source)
}

fn sources_in(db: &Connection, query: &SourceQuery) -> Result<Vec<AnnouncementSource>> {
    let (after_mob, after_source) = query
        .after
        .as_ref()
        .map_or((None, None), |(mob_id, source_id)| {
            (Some(mob_id.as_str()), Some(source_id.as_str()))
        });
    let mut statement = db
        .prepare(
            "SELECT record FROM announcement_sources
             WHERE (?1 IS NULL OR mob_id = ?1)
               AND (?2 IS NULL OR (mob_id, source_id) > (?2, ?3))
               AND (?4 IS NULL OR state = ?4)
             ORDER BY mob_id, source_id LIMIT ?5",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(
            params![
                query.mob_id.as_ref().map(MobId::as_str),
                after_mob,
                after_source,
                query.state.map(SourceState::as_str),
                page(query.limit)?,
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(storage)?;
    rows.map(|record| decode(&record.map_err(storage)?))
        .collect()
}

// ---------------------------------------------------------------------------
// Advancing a source
// ---------------------------------------------------------------------------

/// One stream of a source's current generation.
struct StreamRow {
    phase: CollectionPhase,
    mode: OnboardingMode,
    max_items: Option<u32>,
    backfilled: u32,
    since_ms: u64,
}

fn small(value: i64) -> Result<u32> {
    u32::try_from(value).map_err(|_| storage(format!("{value} is out of range")))
}

fn load_stream(
    db: &Connection,
    source: &AnnouncementSource,
    stream: &str,
) -> Result<Option<StreamRow>> {
    let row: Option<(String, String, Option<i64>, i64, i64)> = db
        .query_row(
            "SELECT phase, mode, max_items, backfilled, since_ms FROM announcement_source_streams
             WHERE mob_id = ?1 AND source_id = ?2 AND stream = ?3 AND generation = ?4",
            params![
                source.mob_id.as_str(),
                source.source_id.as_str(),
                stream,
                int(source.generation)?,
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(storage)?;
    row.map(|(phase, mode, max_items, backfilled, since_ms)| {
        Ok(StreamRow {
            phase: CollectionPhase::parse(&phase)?,
            mode: OnboardingMode::parse(&mode)?,
            max_items: max_items.map(small).transpose()?,
            backfilled: small(backfilled)?,
            since_ms: uint(since_ms)?,
        })
    })
    .transpose()
}

fn write_stream(
    db: &Connection,
    source: &AnnouncementSource,
    stream: &str,
    row: &StreamRow,
) -> Result<()> {
    db.execute(
        "INSERT INTO announcement_source_streams
         (mob_id, source_id, stream, generation, phase, mode, max_items, backfilled, since_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(mob_id, source_id, stream) DO UPDATE SET generation = excluded.generation,
           phase = excluded.phase, mode = excluded.mode, max_items = excluded.max_items,
           backfilled = excluded.backfilled, since_ms = excluded.since_ms",
        params![
            source.mob_id.as_str(),
            source.source_id.as_str(),
            stream,
            int(source.generation)?,
            row.phase.as_str(),
            row.mode.as_str(),
            row.max_items.map(i64::from),
            i64::from(row.backfilled),
            int(row.since_ms)?,
        ],
    )
    .map_err(storage)?;
    Ok(())
}

/// How many of the generation's streams are live, and how many onboard.
fn count_streams(db: &Connection, source: &AnnouncementSource) -> Result<(u32, u32)> {
    let (live, onboarding): (i64, i64) = db
        .query_row(
            "SELECT COUNT(*) FILTER (WHERE phase = 'live'), COUNT(*) FILTER (WHERE phase != 'live')
             FROM announcement_source_streams
             WHERE mob_id = ?1 AND source_id = ?2 AND generation = ?3",
            params![
                source.mob_id.as_str(),
                source.source_id.as_str(),
                int(source.generation)?,
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(storage)?;
    Ok((small(live)?, small(onboarding)?))
}

fn source_streams_in(
    db: &Connection,
    mob_id: &MobId,
    source_id: &SourceId,
) -> Result<Vec<SourceStream>> {
    let Some(source) = load_source(db, mob_id, source_id)? else {
        return Ok(Vec::new());
    };
    let mut statement = db
        .prepare(
            "SELECT stream, phase, mode, max_items, backfilled, since_ms
             FROM announcement_source_streams
             WHERE mob_id = ?1 AND source_id = ?2 AND generation = ?3
             ORDER BY stream",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(
            params![
                source.mob_id.as_str(),
                source.source_id.as_str(),
                int(source.generation)?,
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .map_err(storage)?;
    rows.map(|row| {
        let (stream, phase, mode, max_items, backfilled, since_ms) = row.map_err(storage)?;
        Ok(SourceStream {
            stream,
            phase: CollectionPhase::parse(&phase)?,
            mode: OnboardingMode::parse(&mode)?,
            max_items: max_items.map(small).transpose()?,
            backfilled: small(backfilled)?,
            since_ms: uint(since_ms)?,
        })
    })
    .collect()
}

/// Refuse a collector's step that is not the source's current one: a
/// paused or ended source, another revision, another principal, or another
/// cursor version, in that order.
fn ensure_current_step(
    source: &AnnouncementSource,
    expected_revision: u64,
    principal: &Principal,
    expected_version: u64,
) -> Result<()> {
    match source.state {
        SourceState::Paused => return Err(AnnouncementStoreError::SourcePaused),
        SourceState::Ended => return Err(AnnouncementStoreError::SourceEnded),
        _ => {}
    }
    if source.revision != expected_revision {
        return Err(source_stale(source));
    }
    if source.identity.principal.canonical()? != principal.canonical()? {
        return Err(AnnouncementStoreError::PrincipalChanged {
            pinned_digest: source.identity.principal.digest()?,
            offered_digest: principal.digest()?,
        });
    }
    if source.cursor.version != expected_version {
        return Err(AnnouncementStoreError::SourceVersionConflict {
            revision: source.revision,
            state: source.state,
            cursor: source.cursor.clone(),
        });
    }
    Ok(())
}

/// A time a gap notice names, in UTC to the second.
fn utc(ms: u64) -> String {
    i64::try_from(ms)
        .ok()
        .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
        .map_or_else(
            || format!("{ms} ms"),
            |time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        )
}

/// Announce the one notice a coverage gap or a rebaseline owes the
/// source's subscribers: on its topics, under its own key, in typed words
/// that name no adapter text but the reason's class. Returns whether it
/// captured anything.
/// The one notice a rebaseline or a coverage gap owes.
struct GapNotice<'a> {
    /// The cursor version the step commits.
    version: u64,
    what: &'a str,
    reason: &'a str,
    /// The end of the window changes may be missing from.
    until_ms: u64,
}

fn announce_gap_notice(
    db: &Connection,
    source: &AnnouncementSource,
    gap: &GapNotice<'_>,
    facts: &RosterFacts,
    now_ms: u64,
) -> Result<bool> {
    let GapNotice {
        version,
        what,
        reason,
        until_ms,
    } = *gap;
    let since_ms = source
        .health
        .last_success_at_ms
        .unwrap_or(source.created_at_ms);
    let notice = AnnounceRequest {
        mob_id: source.mob_id.clone(),
        announced_by: Announcer::Source {
            source_id: source.source_id.clone(),
        },
        source_key: SourceKey::new(format!(
            "{SOURCE_KEY_PREFIX}{}:gap:{}:{version}",
            source.source_id, source.generation
        ))?,
        topics: source.topics.clone(),
        body: format!(
            "Source {} {what} ({reason}): changes between {} and {} may be missing.",
            quoted(source.source_id.as_str()),
            utc(since_ms),
            utc(until_ms)
        ),
        correlation_id: None,
        causes: Vec::new(),
    };
    Ok(!announce_in(db, &notice, facts, now_ms)?
        .captured_for
        .is_empty())
}

fn advance_source_in(
    db: &Connection,
    operation: &OperationId,
    request: &AdvanceSource,
    facts: &RosterFacts,
    now_ms: u64,
) -> Result<SourceAdvance> {
    request.validate()?;
    let fingerprint = request_fingerprint("advance_source", request)?;
    if let Some(result) = operation_result(db, operation, &fingerprint)? {
        let mut answer: SourceAdvance = decode(&result)?;
        answer.replayed = true;
        return Ok(answer);
    }
    let mut source = load_source(db, &request.mob_id, &request.source_id)?
        .ok_or(AnnouncementStoreError::UnknownSource)?;
    ensure_current_step(
        &source,
        request.expected_revision,
        &request.principal,
        request.expected_version,
    )?;

    // What the declaration allows.
    for item in &request.items {
        if let Some(topic) = item
            .topics
            .iter()
            .find(|topic| !source.topics.contains(*topic))
        {
            return Err(AnnouncementStoreError::Invalid(format!(
                "topic {topic} is not one of the source's own topics"
            )));
        }
    }
    if source.observation.deletions == DeletionCapability::Unobservable
        && request
            .items
            .iter()
            .any(|item| item.change == ItemChange::Delete)
    {
        return Err(AnnouncementStoreError::DeletionsUnobservable);
    }

    // Onboarding: the source's own phase for this step, then its streams.
    let phase = if request.rebaseline.is_some() {
        CollectionPhase::Baselining
    } else {
        source.collection
    };
    if phase == CollectionPhase::Baselining && !request.items.is_empty() {
        return Err(AnnouncementStoreError::BaselineRequired);
    }
    let mut streams: BTreeMap<String, StreamRow> = BTreeMap::new();
    let mut new_streams = 0usize;
    for entry in &request.streams {
        let row = match load_stream(db, &source, &entry.stream)? {
            Some(row) if row.phase == CollectionPhase::Live => {
                return Err(AnnouncementStoreError::Invalid(
                    "an onboarded stream is live already".to_string(),
                ));
            }
            Some(row) if row.mode != entry.mode || row.max_items != entry.max_items => {
                return Err(AnnouncementStoreError::Invalid(
                    "a stream onboards in one mode, with one bound".to_string(),
                ));
            }
            Some(row) => row,
            None => {
                new_streams += 1;
                StreamRow {
                    phase: match entry.mode {
                        OnboardingMode::Backfill => CollectionPhase::Backfilling,
                        _ => CollectionPhase::Baselining,
                    },
                    mode: entry.mode,
                    max_items: entry.max_items,
                    backfilled: 0,
                    since_ms: now_ms,
                }
            }
        };
        streams.insert(entry.stream.clone(), row);
    }
    if new_streams > 0 {
        let (live, onboarding) = count_streams(db, &source)?;
        if live as usize + onboarding as usize + new_streams > MAX_STREAMS_PER_SOURCE {
            return Err(AnnouncementStoreError::TooLarge(format!(
                "a source onboards at most {MAX_STREAMS_PER_SOURCE} streams"
            )));
        }
    }
    let mut per_stream: BTreeMap<String, u32> = BTreeMap::new();
    for item in &request.items {
        *per_stream.entry(item.stream.clone()).or_default() += 1;
    }
    let mut unbaselined = Vec::new();
    for stream in per_stream.keys().filter(|stream| !stream.is_empty()) {
        if !streams.contains_key(stream)
            && let Some(row) = load_stream(db, &source, stream)?
        {
            streams.insert(stream.clone(), row);
        }
        if streams
            .get(stream)
            .is_none_or(|row| row.phase == CollectionPhase::Baselining)
        {
            unbaselined.push(stream.clone());
        }
    }
    if !unbaselined.is_empty() {
        return Err(AnnouncementStoreError::StreamNotBaselined {
            streams: unbaselined,
        });
    }
    let default_items = per_stream.get("").copied().unwrap_or(0);
    if phase == CollectionPhase::Backfilling && default_items > 0 {
        let remaining = source
            .backfill_bound()
            .unwrap_or(0)
            .saturating_sub(source.backfilled);
        if default_items > remaining {
            return Err(AnnouncementStoreError::BackfillBoundExceeded {
                remaining,
                stream: None,
            });
        }
    }
    for (stream, count) in per_stream.iter().filter(|(stream, _)| !stream.is_empty()) {
        if let Some(row) = streams.get(stream)
            && row.phase == CollectionPhase::Backfilling
        {
            let remaining = row.max_items.unwrap_or(0).saturating_sub(row.backfilled);
            if *count > remaining {
                return Err(AnnouncementStoreError::BackfillBoundExceeded {
                    remaining,
                    stream: Some(stream.clone()),
                });
            }
        }
    }
    let settles = request.settles();
    let source_onboarded = settles && phase != CollectionPhase::Live;
    if (source_onboarded || request.streams.iter().any(|entry| entry.is_final))
        && request.coverage.is_none()
    {
        return Err(AnnouncementStoreError::Invalid(
            "the final page of a baseline, a backfill or a stream onboarding says what it \
             covered"
                .to_string(),
        ));
    }

    // The items, in order: announced, replayed or a recorded conflict.
    let announcer = Announcer::Source {
        source_id: source.source_id.clone(),
    };
    let mut outcomes = Vec::with_capacity(request.items.len());
    let mut captured = false;
    let (mut announced, mut replayed, mut conflicts) = (0u64, 0u64, 0u64);
    for item in &request.items {
        let key = item.key(&source.source_id, &source.identity)?;
        let announcement = AnnounceRequest {
            mob_id: source.mob_id.clone(),
            announced_by: announcer.clone(),
            source_key: key.clone(),
            topics: item.topics.clone(),
            body: item.body.clone(),
            correlation_id: item.correlation_id.clone(),
            causes: Vec::new(),
        };
        let mut outcome = ItemOutcome {
            item_id: item.item_id.clone(),
            version: item.version.clone(),
            source_key: key.clone(),
            outcome: ItemOutcomeKind::Announced,
            sequence: None,
            captured: None,
            existing_digest: None,
            offered_digest: None,
        };
        match announce_in(db, &announcement, facts, now_ms) {
            Ok(committed) if committed.replayed => {
                replayed += 1;
                outcome.outcome = ItemOutcomeKind::Replayed;
                outcome.sequence = Some(committed.announcement.sequence);
            }
            Ok(committed) => {
                announced += 1;
                captured |= !committed.captured_for.is_empty();
                outcome.sequence = Some(committed.announcement.sequence);
                outcome.captured =
                    Some(u32::try_from(committed.captured_for.len()).unwrap_or(u32::MAX));
            }
            Err(AnnouncementStoreError::AnnouncementConflict { existing }) => {
                conflicts += 1;
                let offered = announcement.digest();
                source.health.last_conflict = Some(SourceConflictRecord {
                    source_key: key,
                    existing_sequence: existing.sequence,
                    existing_digest: existing.digest.clone(),
                    offered_digest: offered.clone(),
                    at_ms: now_ms,
                });
                outcome.outcome = ItemOutcomeKind::Conflict;
                outcome.sequence = Some(existing.sequence);
                outcome.existing_digest = Some(existing.digest);
                outcome.offered_digest = Some(offered);
            }
            Err(error) => return Err(error),
        }
        outcomes.push(outcome);
    }

    // One gap notice for a rebaseline or a final page that cannot prove
    // complete coverage.
    let version = request.expected_version + 1;
    let gap = match (&request.rebaseline, &request.coverage) {
        (Some(rebaseline), _) => Some((
            "is taking a fresh baseline",
            rebaseline.reason.as_str().to_string(),
        )),
        (None, Some(Coverage::Gap(CoverageGap { reason }))) => {
            Some(("could not prove complete coverage", reason.clone()))
        }
        _ => None,
    };
    if let Some((what, reason)) = gap {
        let until_ms = request.collected_at_ms.unwrap_or(now_ms);
        let notice = GapNotice {
            version,
            what,
            reason: &reason,
            until_ms,
        };
        captured |= announce_gap_notice(db, &source, &notice, facts, now_ms)?;
        source.health.totals.gaps += 1;
    }

    // The cursor, the phases and the health.
    source.cursor.version = version;
    source.cursor.token.clone_from(&request.next_cursor);
    source.cursor.advanced_at_ms = Some(now_ms);
    if request.rebaseline.is_some() {
        source.collection = CollectionPhase::Baselining;
    }
    if phase == CollectionPhase::Backfilling {
        source.backfilled = source.backfilled.saturating_add(default_items);
    }
    if source_onboarded {
        source.collection = CollectionPhase::Live;
    }
    for (stream, count) in &per_stream {
        if let Some(row) = streams.get_mut(stream)
            && row.phase == CollectionPhase::Backfilling
        {
            row.backfilled = row.backfilled.saturating_add(*count);
        }
    }
    let mut phases = Vec::with_capacity(request.streams.len());
    for entry in &request.streams {
        if let Some(row) = streams.get_mut(&entry.stream) {
            if entry.is_final && row.phase != CollectionPhase::Live {
                row.phase = CollectionPhase::Live;
                row.since_ms = now_ms;
            }
            phases.push(StreamPhase {
                stream: entry.stream.clone(),
                phase: row.phase,
            });
        }
    }
    for (stream, row) in &streams {
        write_stream(db, &source, stream, row)?;
    }
    let (live, onboarding) = count_streams(db, &source)?;
    source.streams.live = live;
    source.streams.onboarding = onboarding;
    let health = &mut source.health;
    health.consecutive_failures = 0;
    if settles {
        health.last_success_at_ms = Some(now_ms);
    }
    health.totals.announced += announced;
    health.totals.replayed += replayed;
    health.totals.conflicts += conflicts;
    if announced > 0 {
        health.last_new_item_at_ms = Some(now_ms);
    }
    health.observe(request.observed, now_ms);
    source.updated_at_ms = now_ms;
    source.refresh_status();
    write_source(db, &source)?;
    let answer = SourceAdvance {
        source,
        replayed: false,
        items: outcomes,
        streams: phases,
        captured,
    };
    record_operation(db, operation, &fingerprint, now_ms, &encode(&answer)?)?;
    super::faults::inside_source_advance();
    Ok(answer)
}

/// Count an onboarding refusal of an advance toward its source's failure
/// streak, in a transaction of its own, unless the source moved since.
fn count_refusal_in(
    db: &Connection,
    request: &AdvanceSource,
    kind: &str,
    now_ms: u64,
) -> Result<()> {
    let Some(mut source) = load_source(db, &request.mob_id, &request.source_id)? else {
        return Ok(());
    };
    if source.state != SourceState::Active
        || source.revision != request.expected_revision
        || source.cursor.version != request.expected_version
    {
        return Ok(());
    }
    let health = &mut source.health;
    health.consecutive_failures = health.consecutive_failures.saturating_add(1);
    health.totals.refusals += 1;
    health.last_failure = Some(SourceFailureRecord {
        class: format!("refused:{kind}"),
        at_ms: now_ms,
        retry_after_ms: None,
        detail: None,
    });
    source.refresh_status();
    write_source(db, &source)
}

fn record_failure_in(
    db: &Connection,
    operation: &OperationId,
    request: &RecordSourceFailure,
    now_ms: u64,
) -> Result<AnnouncementSource> {
    request.validate()?;
    let fingerprint = request_fingerprint("record_source_failure", request)?;
    if let Some(result) = operation_result(db, operation, &fingerprint)? {
        return decode(&result);
    }
    let mut source = load_source(db, &request.mob_id, &request.source_id)?
        .ok_or(AnnouncementStoreError::UnknownSource)?;
    ensure_current_step(
        &source,
        request.expected_revision,
        &request.principal,
        request.expected_version,
    )?;
    let health = &mut source.health;
    health.consecutive_failures = health.consecutive_failures.saturating_add(1);
    health.last_failure = Some(SourceFailureRecord {
        class: request.failure.class.as_str().to_string(),
        at_ms: now_ms,
        retry_after_ms: request.failure.retry_after_ms,
        detail: request.failure.detail.clone(),
    });
    source.refresh_status();
    write_source(db, &source)?;
    record_operation(db, operation, &fingerprint, now_ms, &encode(&source)?)?;
    Ok(source)
}

// ---------------------------------------------------------------------------
// Who may receive a source's items
// ---------------------------------------------------------------------------

/// The source owning a topic: its id, its current generation and its
/// declaration.
struct OwnedTopic {
    source_id: SourceId,
    generation: u64,
    config: SourceConfig,
}

/// The sources owning any of `topics` in `mob_id`, by topic.
fn topic_sources(
    db: &Connection,
    mob_id: &MobId,
    topics: &BTreeSet<Topic>,
) -> Result<BTreeMap<Topic, OwnedTopic>> {
    let mut owners = BTreeMap::new();
    let mut loaded: BTreeMap<SourceId, (u64, SourceConfig)> = BTreeMap::new();
    for topic in topics {
        let Some((source_id, _)) = topic_owner(db, mob_id, topic)? else {
            continue;
        };
        if !loaded.contains_key(&source_id) {
            let Some(source) = load_source(db, mob_id, &source_id)? else {
                return Err(storage(format!(
                    "topic {topic} is owned by a source the store does not hold"
                )));
            };
            loaded.insert(source_id.clone(), (source.generation, source.config()));
        }
        if let Some((generation, config)) = loaded.get(&source_id) {
            owners.insert(
                topic.clone(),
                OwnedTopic {
                    source_id,
                    generation: *generation,
                    config: config.clone(),
                },
            );
        }
    }
    Ok(owners)
}

/// Why `subscription` may not capture an announcement on `topics`, if it
/// may not: the reason, and the source that withholds it. A topic no source
/// owns lets it capture. A topic a source owns lets it capture only when the
/// member owns no live conversation on the source's scope, the topic's
/// audience covers it as the roster facts the owner read say (a member those
/// facts do not name is covered by a member grant alone), and, for one of
/// the source's own topics, a direct route names the topic or the member
/// triages it. Routes bind every subscriber, one from before a route change
/// too.
fn withholding<'a>(
    owners: &'a BTreeMap<Topic, OwnedTopic>,
    subscription: &Subscription,
    topics: &BTreeSet<Topic>,
    facts: &RosterFacts,
) -> Option<(AuditReason, &'a OwnedTopic)> {
    let member = &subscription.member;
    let mut refusal = None;
    for topic in subscription.selector.topics.intersection(topics) {
        // A topic no source owns lets the subscription capture.
        let owned = owners.get(topic)?;
        let config = &owned.config;
        let reason = if config.conversation_members.contains(member) {
            AuditReason::RouteConflict
        } else if !config
            .audience_of(topic)
            .is_some_and(|audience| covers(audience, member, facts.get(member)))
        {
            AuditReason::AudienceNotCovered
        } else if config.topics.contains(topic)
            && !config.directly_routes(topic)
            && !config.triages(member, topic)
        {
            AuditReason::NoDirectRoute
        } else {
            return None;
        };
        refusal.get_or_insert((reason, owned));
    }
    refusal
}

/// What one counted audit entry is about. Every refusal or withholding with
/// the same key counts on the same entry.
#[derive(Serialize, serde::Deserialize)]
struct AuditKey {
    mob_id: MobId,
    source_id: SourceId,
    generation: u64,
    member: Member,
    action: AuditAction,
    reason: AuditReason,
}

/// Count one refusal or withholding on its entry: the first makes the entry,
/// and each later one adds to its count and moves its last time.
fn count_audit(db: &Connection, key: &AuditKey, now_ms: u64) -> Result<()> {
    db.execute(
        "INSERT INTO announcement_audit (mob_id, source_id, generation, member, action,
           reason, count, first_at_ms, last_at_ms, record)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?7, ?8)
         ON CONFLICT (mob_id, source_id, generation, member, action, reason) DO UPDATE SET
           count = count + 1, last_at_ms = MAX(last_at_ms, excluded.last_at_ms)",
        params![
            key.mob_id.as_str(),
            key.source_id.as_str(),
            int(key.generation)?,
            key.member.key(),
            key.action.as_str(),
            key.reason.as_str(),
            int(now_ms)?,
            encode(key)?,
        ],
    )
    .map_err(storage)?;
    Ok(())
}

fn audit_in(db: &Connection, query: &AuditQuery) -> Result<Vec<AuditRow>> {
    let member = query.member.as_ref().map(Member::key);
    let mut statement = db
        .prepare(
            "SELECT id, count, first_at_ms, last_at_ms, record FROM announcement_audit
             WHERE (?1 IS NULL OR mob_id = ?1) AND (?2 IS NULL OR source_id = ?2)
               AND (?3 IS NULL OR member = ?3) AND (?4 IS NULL OR action = ?4) AND id > ?5
             ORDER BY id LIMIT ?6",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(
            params![
                query.mob_id.as_ref().map(MobId::as_str),
                query.source_id.as_ref().map(SourceId::as_str),
                member,
                query.action.map(AuditAction::as_str),
                int(query.after.unwrap_or(0))?,
                page(query.limit)?,
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .map_err(storage)?;
    rows.map(|row| {
        let (id, count, first_at_ms, last_at_ms, record) = row.map_err(storage)?;
        let key: AuditKey = decode(&record)?;
        let last_at_ms = uint(last_at_ms)?;
        Ok(AuditRow {
            id: uint(id)?,
            at_ms: last_at_ms,
            action: key.action,
            mob_id: key.mob_id,
            source_id: key.source_id,
            generation: key.generation,
            member: key.member,
            reason: key.reason,
            count: uint(count)?,
            first_at_ms: uint(first_at_ms)?,
            last_at_ms,
        })
    })
    .collect()
}

/// Why an owed item may not reach its member now: the reason, and the
/// source and generation that refuse it.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
struct OwedRefusal {
    reason: AuditReason,
    source_id: SourceId,
    generation: u64,
}

/// The check of capture, made again for an owed item: whether
/// `subscription`'s member may still receive `announcement`, as the roster
/// facts the owner read say. An item on no source's topic always may.
fn owed_refusal(
    db: &Connection,
    subscription: &Subscription,
    announcement: &Announcement,
    facts: &RosterFacts,
) -> Result<Option<OwedRefusal>> {
    let owners = topic_sources(db, &announcement.mob_id, &announcement.topics)?;
    Ok(
        withholding(&owners, subscription, &announcement.topics, facts).map(|(reason, owned)| {
            OwedRefusal {
                reason,
                source_id: owned.source_id.clone(),
                generation: owned.generation,
            }
        }),
    )
}

/// Hold an owed item: it stays owed, no batch takes it, and the hold is
/// counted in the audit.
fn hold_owed(
    db: &Connection,
    subscription_id: &SubscriptionId,
    key: &(MobId, u64),
    member: &Member,
    refusal: &OwedRefusal,
    now_ms: u64,
) -> Result<()> {
    db.execute(
        "INSERT OR REPLACE INTO announcement_holds
           (subscription_id, mob_id, sequence, member, source_id, generation, held_at_ms,
            checked_at_ms, record)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?8)",
        params![
            subscription_id.as_str(),
            key.0.as_str(),
            int(key.1)?,
            member.key(),
            refusal.source_id.as_str(),
            int(refusal.generation)?,
            int(now_ms)?,
            encode(refusal)?,
        ],
    )
    .map_err(storage)?;
    count_audit(
        db,
        &AuditKey {
            mob_id: key.0.clone(),
            source_id: refusal.source_id.clone(),
            generation: refusal.generation,
            member: member.clone(),
            action: AuditAction::ItemsHeld,
            reason: refusal.reason,
        },
        now_ms,
    )
}

/// The held items the next check reads: open, on an active subscription,
/// and held by their source's current generation (a new generation
/// inherits nothing held, so an earlier generation's holds wait out
/// retention and never stand in front of these).
const CHECKABLE_HOLDS: &str = "FROM announcement_holds h
     JOIN owed_announcements o ON o.subscription_id = h.subscription_id
      AND o.mob_id = h.mob_id AND o.sequence = h.sequence
     JOIN member_subscriptions s ON s.id = h.subscription_id
     JOIN announcement_sources src ON src.mob_id = h.mob_id AND src.source_id = h.source_id
      AND src.generation = h.generation
     WHERE o.settlement IS NULL AND o.batch_id IS NULL AND s.state = 'active'";

/// Check the member's held items afresh, the ones checked longest ago first
/// and at most `limit`: one that now passes is released and counted; one
/// that still fails is stamped checked, so every hold is reached within a
/// few checks however many stand in front of it.
fn release_holds(
    db: &Connection,
    member: &Member,
    limit: u32,
    facts: &RosterFacts,
    now_ms: u64,
) -> Result<()> {
    let mut statement = db
        .prepare(&format!(
            "SELECT h.subscription_id, h.mob_id, h.sequence, h.record {CHECKABLE_HOLDS}
               AND h.member = ?1
             ORDER BY h.checked_at_ms, h.held_at_ms, h.mob_id, h.sequence, h.subscription_id
             LIMIT ?2"
        ))
        .map_err(storage)?;
    let rows = statement
        .query_map(params![member.key(), i64::from(limit)], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(storage)?;
    let mut held = Vec::new();
    for row in rows {
        held.push(row.map_err(storage)?);
    }
    drop(statement);
    for (subscription_id, mob_id, sequence, record) in held {
        let subscription_id = SubscriptionId::new(subscription_id)?;
        let key = (MobId::new(mob_id)?, uint(sequence)?);
        let refusal: OwedRefusal = decode(&record)?;
        let Some(subscription) = load_subscription(db, &subscription_id)? else {
            continue;
        };
        let announcement = announcement_at(db, &key.0, key.1)?;
        if owed_refusal(db, &subscription, &announcement, facts)?.is_some() {
            db.execute(
                "UPDATE announcement_holds SET checked_at_ms = ?4
                 WHERE subscription_id = ?1 AND mob_id = ?2 AND sequence = ?3",
                params![
                    subscription_id.as_str(),
                    key.0.as_str(),
                    int(key.1)?,
                    int(now_ms)?
                ],
            )
            .map_err(storage)?;
            continue;
        }
        db.execute(
            "DELETE FROM announcement_holds
             WHERE subscription_id = ?1 AND mob_id = ?2 AND sequence = ?3",
            params![subscription_id.as_str(), key.0.as_str(), int(key.1)?],
        )
        .map_err(storage)?;
        count_audit(
            db,
            &AuditKey {
                mob_id: key.0,
                source_id: refusal.source_id,
                generation: refusal.generation,
                member: member.clone(),
                action: AuditAction::ItemsReleased,
                reason: refusal.reason,
            },
            now_ms,
        )?;
    }
    Ok(())
}

/// Members whose held items are due a fresh check (see [`CHECKABLE_HOLDS`]):
/// one last checked at or before `checked_before_ms`.
fn members_with_holds_due_in(
    db: &Connection,
    checked_before_ms: u64,
    limit: usize,
) -> Result<Vec<Member>> {
    let mut statement = db
        .prepare(&format!(
            "SELECT DISTINCT h.member {CHECKABLE_HOLDS} AND h.checked_at_ms <= ?1
             ORDER BY h.member LIMIT ?2"
        ))
        .map_err(storage)?;
    let rows = statement
        .query_map(params![int(checked_before_ms)?, page(limit)?], |row| {
            row.get::<_, String>(0)
        })
        .map_err(storage)?;
    rows.map(|member| Member::from_key(&member.map_err(storage)?))
        .collect()
}

/// The last check before a submission of batch `id`, in state `expected`
/// (sealed, or submitting after an answer that nothing was committed):
/// every item is checked again. When one fails and `may_cancel`, the batch
/// is cancelled, its failing items are held, the rest reopen for the
/// member's next batch, and its gap lines go to that batch. Without
/// `may_cancel` nothing changes.
fn check_before_submission_in(
    db: &Connection,
    id: &BatchId,
    expected: BatchStateTag,
    facts: &RosterFacts,
    may_cancel: bool,
    now_ms: u64,
) -> Result<SubmissionCheck> {
    let mut batch = load_batch(db, id)?.ok_or(AnnouncementStoreError::UnknownBatch)?;
    let current = batch.state.tag();
    if current != expected {
        return Err(AnnouncementStoreError::BatchMoved { current });
    }
    let event = match expected {
        BatchStateTag::Sealed => BatchEvent::Cancel,
        BatchStateTag::Submitting => BatchEvent::CancelNotCommitted,
        other => {
            return Err(AnnouncementStoreError::Invalid(format!(
                "a {} batch has no check before a submission",
                other.as_str()
            )));
        }
    };
    let mut failing = Vec::new();
    for item in &batch.items {
        let Some(subscription) = load_subscription(db, &item.subscription_id)? else {
            continue;
        };
        let announcement = announcement_at(db, &item.mob_id, item.sequence)?;
        if let Some(refusal) = owed_refusal(db, &subscription, &announcement, facts)? {
            failing.push((
                item.subscription_id.clone(),
                (item.mob_id.clone(), item.sequence),
                refusal,
            ));
        }
    }
    if failing.is_empty() {
        return Ok(SubmissionCheck::Passed);
    }
    if !may_cancel {
        return Ok(SubmissionCheck::Failed);
    }
    let next = batch.state.apply(&event)?;
    // The batch reports nothing: its gap lines go to the member's next one.
    add_gaps(db, &batch.member, &batch.gaps)?;
    db.execute(
        "UPDATE owed_announcements SET batch_id = NULL
         WHERE batch_id = ?1 AND settlement IS NULL",
        params![batch.id.as_str()],
    )
    .map_err(storage)?;
    for (subscription_id, key, refusal) in &failing {
        hold_owed(db, subscription_id, key, &batch.member, refusal, now_ms)?;
    }
    batch.state = next;
    batch.updated_at_ms = now_ms;
    update_batch(db, &batch)?;
    Ok(SubmissionCheck::Cancelled(Box::new(batch)))
}

/// Count each item of the admitted batch `id` its member no longer passes:
/// it reached the member across a revocation.
fn audit_delivered_across_in(
    db: &Connection,
    id: &BatchId,
    facts: &RosterFacts,
    now_ms: u64,
) -> Result<()> {
    let batch = load_batch(db, id)?.ok_or(AnnouncementStoreError::UnknownBatch)?;
    for item in &batch.items {
        let Some(subscription) = load_subscription(db, &item.subscription_id)? else {
            continue;
        };
        let announcement = announcement_at(db, &item.mob_id, item.sequence)?;
        if let Some(refusal) = owed_refusal(db, &subscription, &announcement, facts)? {
            count_audit(
                db,
                &AuditKey {
                    mob_id: item.mob_id.clone(),
                    source_id: refusal.source_id,
                    generation: refusal.generation,
                    member: batch.member.clone(),
                    action: AuditAction::DeliveredAcrossRevocation,
                    reason: refusal.reason,
                },
                now_ms,
            )?;
        }
    }
    Ok(())
}

/// The roster facts of one member, as a store check reads them.
fn facts_of(member: &Member, facts: Option<MemberFacts>) -> RosterFacts {
    let mut roster = RosterFacts::default();
    if let Some(found) = facts {
        roster.insert(member.clone(), found);
    }
    roster
}

/// A subscription a source's audience or routes refused: the refusal, and
/// the source, generation and reason its audit entry counts under.
struct SubscriptionRefusal {
    error: AnnouncementStoreError,
    mob_id: MobId,
    source_id: SourceId,
    generation: u64,
    reason: AuditReason,
}

/// The member's live subscriptions.
fn member_live_subscriptions(db: &Connection, member: &Member) -> Result<Vec<Subscription>> {
    let mut statement = db
        .prepare(
            "SELECT record FROM member_subscriptions
             WHERE member = ?1 AND state IN ('active', 'paused')",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(params![member.key()], |row| row.get::<_, String>(0))
        .map_err(storage)?;
    rows.map(|record| decode(&record.map_err(storage)?))
        .collect()
}

/// Whether subscriptions with these selectors reach `config`'s source in
/// `mob_id` directly (one of its own topics) and through a forward (one of
/// its forward topics).
fn reaches<'a>(
    config: &SourceConfig,
    mob_id: &MobId,
    selectors: impl Iterator<Item = &'a Selector>,
) -> (bool, bool) {
    let forwards: BTreeSet<Topic> = config
        .owned_topics()
        .into_iter()
        .filter(|(_, kind)| *kind == TopicOwnership::Forward)
        .map(|(topic, _)| topic)
        .collect();
    let (mut direct, mut forward) = (false, false);
    for selector in selectors.filter(|selector| selector.mobs.contains(mob_id)) {
        direct |= !selector.topics.is_disjoint(&config.topics);
        forward |= !selector.topics.is_disjoint(&forwards);
    }
    (direct, forward)
}

/// The refusal of a subscription a source's audience or routes do not
/// admit, if they do not: for every topic it selects that a source owns, the
/// member must own no live conversation on the source's scope, the topic's
/// audience must cover it, a source topic must have a direct route unless
/// the member triages it, and with its other live subscriptions the member
/// must not reach the source both directly and through a forward.
fn ensure_subscription_admitted(
    db: &Connection,
    request: &PutSubscription,
    facts: Option<&MemberFacts>,
) -> Result<Option<SubscriptionRefusal>> {
    let member = &request.member;
    let mut sources: BTreeMap<(MobId, SourceId), OwnedTopic> = BTreeMap::new();
    for mob_id in &request.selector.mobs {
        for (topic, owned) in topic_sources(db, mob_id, &request.selector.topics)? {
            let config = &owned.config;
            let refused = if config.conversation_members.contains(member) {
                Some((
                    AuditReason::RouteConflict,
                    AnnouncementStoreError::ConversationMember {
                        source_id: owned.source_id.clone(),
                    },
                ))
            } else if !config
                .audience_of(&topic)
                .is_some_and(|audience| covers(audience, member, facts))
            {
                Some((
                    AuditReason::AudienceNotCovered,
                    AnnouncementStoreError::AudienceNotCovered {
                        source_id: owned.source_id.clone(),
                        topic: topic.clone(),
                    },
                ))
            } else if config.topics.contains(&topic)
                && !config.directly_routes(&topic)
                && !config.triages(member, &topic)
            {
                Some((
                    AuditReason::NoDirectRoute,
                    AnnouncementStoreError::NoDirectRoute {
                        source_id: owned.source_id.clone(),
                        topic: topic.clone(),
                    },
                ))
            } else {
                None
            };
            if let Some((reason, error)) = refused {
                return Ok(Some(SubscriptionRefusal {
                    error,
                    mob_id: mob_id.clone(),
                    source_id: owned.source_id,
                    generation: owned.generation,
                    reason,
                }));
            }
            sources.insert((mob_id.clone(), owned.source_id.clone()), owned);
        }
    }
    if sources.is_empty() {
        return Ok(None);
    }
    let others = member_live_subscriptions(db, member)?;
    for ((mob_id, source_id), owned) in sources {
        let selectors = std::iter::once(&request.selector).chain(
            others
                .iter()
                .filter(|other| other.id != request.id)
                .map(|other| &other.selector),
        );
        if reaches(&owned.config, &mob_id, selectors) == (true, true) {
            return Ok(Some(SubscriptionRefusal {
                error: AnnouncementStoreError::RouteOverlap {
                    source_id: source_id.clone(),
                    member: member.clone(),
                },
                mob_id,
                source_id,
                generation: owned.generation,
                reason: AuditReason::RouteOverlap,
            }));
        }
    }
    Ok(None)
}

/// Put a subscription the sources' audiences and routes admit. A refusal
/// comes back inside, with what its audit entry counts under, and leaves
/// the store unchanged.
fn put_subscription_admitted_in(
    db: &Connection,
    operation: &OperationId,
    request: &PutSubscription,
    facts: Option<&MemberFacts>,
    now_ms: u64,
) -> Result<std::result::Result<Subscription, SubscriptionRefusal>> {
    // A replay of a committed put answers its original.
    let fingerprint = request_fingerprint("put_subscription", request)?;
    if let Some(result) = operation_result(db, operation, &fingerprint)? {
        return Ok(Ok(decode(&result)?));
    }
    if let Some(refusal) = ensure_subscription_admitted(db, request, facts)? {
        return Ok(Err(refusal));
    }
    Ok(Ok(put_subscription_in(db, operation, request, now_ms)?))
}

/// Refuse a declaration under which an existing live subscriber would reach
/// the source both directly and through a forward.
fn ensure_routes_admitted(db: &Connection, source: &AnnouncementSource) -> Result<()> {
    let config = source.config();
    let owned: BTreeSet<Topic> = config
        .owned_topics()
        .into_iter()
        .map(|(topic, _)| topic)
        .collect();
    let mut by_member: BTreeMap<Member, Vec<Subscription>> = BTreeMap::new();
    for subscription in subscriptions_for_topics(db, &owned)?.into_values() {
        if subscription.selector.mobs.contains(&source.mob_id) {
            by_member
                .entry(subscription.member.clone())
                .or_default()
                .push(subscription);
        }
    }
    for (member, subscriptions) in by_member {
        let selectors = subscriptions
            .iter()
            .map(|subscription| &subscription.selector);
        if reaches(&config, &source.mob_id, selectors) == (true, true) {
            return Err(AnnouncementStoreError::RouteOverlap {
                source_id: source.source_id.clone(),
                member,
            });
        }
    }
    Ok(())
}

/// The topics `source_id` owns in `mob_id`.
fn source_topics(db: &Connection, mob_id: &MobId, source_id: &SourceId) -> Result<BTreeSet<Topic>> {
    let mut statement = db
        .prepare(
            "SELECT topic FROM announcement_source_topics WHERE mob_id = ?1 AND source_id = ?2",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(params![mob_id.as_str(), source_id.as_str()], |row| {
            row.get::<_, String>(0)
        })
        .map_err(storage)?;
    rows.map(|topic| Topic::new(topic.map_err(storage)?))
        .collect()
}

/// The members whose live subscriptions take any of `topics` a source owns
/// in `mob_id`: the members whose roster facts a capture there needs.
fn owned_topic_subscribers_in(
    db: &Connection,
    mob_id: &MobId,
    topics: &BTreeSet<Topic>,
) -> Result<BTreeSet<Member>> {
    let mut owned = BTreeSet::new();
    for topic in topics {
        if topic_owner(db, mob_id, topic)?.is_some() {
            owned.insert(topic.clone());
        }
    }
    if owned.is_empty() {
        return Ok(BTreeSet::new());
    }
    Ok(subscriptions_for_topics(db, &owned)?
        .into_values()
        .filter(|subscription| subscription.selector.mobs.contains(mob_id))
        .map(|subscription| subscription.member)
        .collect())
}

#[async_trait]
impl AnnouncementStore for SqliteAnnouncementStore {
    async fn announce(&self, request: AnnounceRequest, now_ms: u64) -> Result<AnnounceOutcome> {
        self.announce_with_facts(request, &RosterFacts::default(), now_ms)
            .await
    }

    async fn announce_with_facts(
        &self,
        request: AnnounceRequest,
        facts: &RosterFacts,
        now_ms: u64,
    ) -> Result<AnnounceOutcome> {
        let facts = facts.clone();
        self.run(move |db| announce_in(db, &request, &facts, now_ms))
            .await
    }

    async fn announcement(
        &self,
        mob_id: &MobId,
        source_key: &SourceKey,
    ) -> Result<Option<Announcement>> {
        let (mob_id, source_key) = (mob_id.clone(), source_key.clone());
        self.run(move |db| announcement_by_key(db, &mob_id, &source_key))
            .await
    }

    async fn put_subscription(
        &self,
        operation: &OperationId,
        request: PutSubscription,
        now_ms: u64,
    ) -> Result<Subscription> {
        self.put_subscription_with_facts(operation, request, None, now_ms)
            .await
    }

    async fn put_subscription_with_facts(
        &self,
        operation: &OperationId,
        request: PutSubscription,
        facts: Option<MemberFacts>,
        now_ms: u64,
    ) -> Result<Subscription> {
        let request = Arc::new(request);
        let put = {
            let (operation, request) = (operation.clone(), Arc::clone(&request));
            move |db: &Connection| {
                put_subscription_admitted_in(db, &operation, &request, facts.as_ref(), now_ms)
            }
        };
        let refusal = match self.run(put).await? {
            Ok(subscription) => return Ok(subscription),
            Err(refusal) => refusal,
        };
        // A refusal leaves the store unchanged, then is counted on its own.
        let key = AuditKey {
            mob_id: refusal.mob_id,
            source_id: refusal.source_id,
            generation: refusal.generation,
            member: request.member.clone(),
            action: AuditAction::SubscriptionRefused,
            reason: refusal.reason,
        };
        if let Err(audit_error) = self.run(move |db| count_audit(db, &key, now_ms)).await {
            tracing::warn!(
                error = %audit_error,
                "a refused subscription could not be audited"
            );
        }
        Err(refusal.error)
    }

    async fn change_subscription(
        &self,
        operation: &OperationId,
        id: &SubscriptionId,
        expected_revision: u64,
        change: SubscriptionChange,
        now_ms: u64,
    ) -> Result<SubscriptionChangeOutcome> {
        let (operation, id) = (operation.clone(), id.clone());
        self.run(move |db| {
            change_subscription_in(db, &operation, &id, expected_revision, change, now_ms)
        })
        .await
    }

    async fn subscription(&self, id: &SubscriptionId) -> Result<Option<Subscription>> {
        let id = id.clone();
        self.run(move |db| load_subscription(db, &id)).await
    }

    async fn end_member(
        &self,
        member: &Member,
        end: SubscriptionEnd,
        now_ms: u64,
    ) -> Result<Vec<SubscriptionId>> {
        let member = member.clone();
        self.run(move |db| end_member_in(db, &member, &end, now_ms))
            .await
    }

    async fn known_mobs(&self) -> Result<BTreeSet<MobId>> {
        self.run(known_mobs_in).await
    }

    async fn drop_ended_mobs(
        &self,
        ended: &BTreeSet<MobId>,
        now_ms: u64,
    ) -> Result<Vec<SubscriptionId>> {
        let ended = ended.clone();
        self.run(move |db| drop_ended_mobs_in(db, &ended, now_ms))
            .await
    }

    async fn lineage(&self) -> Result<Vec<MobLineage>> {
        self.run(lineage_in).await
    }

    async fn member_route(&self, member: &Member) -> Result<Option<MemberRouteRecord>> {
        let member = member.clone();
        self.run(move |db| member_route_in(db, &member)).await
    }

    async fn unbind_member_route(
        &self,
        member: &Member,
        end: SubscriptionEnd,
        now_ms: u64,
    ) -> Result<Vec<SubscriptionId>> {
        let member = member.clone();
        self.run(move |db| unbind_member_route_in(db, &member, &end, now_ms))
            .await
    }

    async fn child_member_routes(&self) -> Result<Vec<(Member, MemberRouteRecord)>> {
        self.run(child_member_routes_in).await
    }

    async fn member_mob_check_from(&self, member: &Member) -> Result<Option<u64>> {
        let member = member.clone();
        self.run(move |db| member_mob_check_from_in(db, &member))
            .await
    }

    async fn record_member_mob_check(&self, member: &Member, through: u64) -> Result<()> {
        let member = member.clone();
        self.run(move |db| record_member_mob_check_in(db, &member, through))
            .await
    }

    async fn end_member_before(
        &self,
        member: &Member,
        cursor: u64,
        end: SubscriptionEnd,
        now_ms: u64,
    ) -> Result<Vec<SubscriptionId>> {
        let member = member.clone();
        self.run(move |db| end_member_before_in(db, &member, cursor, &end, now_ms))
            .await
    }

    async fn record_lineage(&self, lineage: &MobLineage, now_ms: u64) -> Result<bool> {
        let lineage = lineage.clone();
        self.run(move |db| record_lineage_in(db, &lineage, now_ms))
            .await
    }

    async fn end_lineage(&self, mobs: &BTreeSet<MobId>, now_ms: u64) -> Result<()> {
        let mobs = mobs.clone();
        self.run(move |db| end_lineage_in(db, &mobs, now_ms)).await
    }

    async fn subscriptions(
        &self,
        member: Option<&Member>,
        after: Option<&SubscriptionId>,
        limit: usize,
    ) -> Result<Vec<Subscription>> {
        let member = member.map(Member::key);
        let after = after.map(|value| value.as_str().to_owned());
        self.run(move |db| {
            let mut statement = db
                .prepare(
                    "SELECT record FROM member_subscriptions
                     WHERE (?1 IS NULL OR member = ?1) AND (?2 IS NULL OR id > ?2)
                     ORDER BY id LIMIT ?3",
                )
                .map_err(storage)?;
            let rows = statement
                .query_map(params![member, after, page(limit)?], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(storage)?;
            rows.map(|record| decode(&record.map_err(storage)?))
                .collect()
        })
        .await
    }

    async fn backlogs(&self, limit: usize) -> Result<Vec<MemberBacklog>> {
        self.run(move |db| backlogs_in(db, limit)).await
    }

    async fn seal_batch(
        &self,
        member: &Member,
        batch_id: &BatchId,
        policy: &SealingPolicy,
        now_ms: u64,
    ) -> Result<Option<Batch>> {
        self.seal_batch_with_facts(member, batch_id, policy, None, now_ms)
            .await
    }

    async fn seal_batch_with_facts(
        &self,
        member: &Member,
        batch_id: &BatchId,
        policy: &SealingPolicy,
        facts: Option<MemberFacts>,
        now_ms: u64,
    ) -> Result<Option<Batch>> {
        let (member, batch_id, policy) = (member.clone(), batch_id.clone(), *policy);
        let facts = facts_of(&member, facts);
        self.run(move |db| seal_batch_in(db, &member, &batch_id, &policy, &facts, now_ms))
            .await
    }

    async fn check_before_submission(
        &self,
        id: &BatchId,
        expected: BatchStateTag,
        facts: Option<MemberFacts>,
        may_cancel: bool,
        now_ms: u64,
    ) -> Result<SubmissionCheck> {
        let id = id.clone();
        self.run(move |db| {
            let member = load_batch(db, &id)?
                .ok_or(AnnouncementStoreError::UnknownBatch)?
                .member;
            let facts = facts_of(&member, facts);
            check_before_submission_in(db, &id, expected, &facts, may_cancel, now_ms)
        })
        .await
    }

    async fn members_with_holds_due(
        &self,
        checked_before_ms: u64,
        limit: usize,
    ) -> Result<Vec<Member>> {
        self.run(move |db| members_with_holds_due_in(db, checked_before_ms, limit))
            .await
    }

    async fn release_holds(
        &self,
        member: &Member,
        facts: Option<MemberFacts>,
        limit: u32,
        now_ms: u64,
    ) -> Result<()> {
        let member = member.clone();
        let facts = facts_of(&member, facts);
        self.run(move |db| release_holds(db, &member, limit, &facts, now_ms))
            .await
    }

    async fn audit_delivered_across_revocation(
        &self,
        id: &BatchId,
        facts: Option<MemberFacts>,
        now_ms: u64,
    ) -> Result<()> {
        let id = id.clone();
        self.run(move |db| {
            let member = load_batch(db, &id)?
                .ok_or(AnnouncementStoreError::UnknownBatch)?
                .member;
            let facts = facts_of(&member, facts);
            audit_delivered_across_in(db, &id, &facts, now_ms)
        })
        .await
    }

    async fn batch(&self, id: &BatchId) -> Result<Option<Batch>> {
        let id = id.clone();
        self.run(move |db| load_batch(db, &id)).await
    }

    async fn batches(&self, query: &BatchQuery) -> Result<Vec<Batch>> {
        let query = query.clone();
        self.run(move |db| {
            let after = match &query.after {
                Some(after) => {
                    let sealed_at: Option<i64> = db
                        .query_row(
                            "SELECT sealed_at_ms FROM announcement_batches WHERE id = ?1",
                            params![after.as_str()],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(storage)?;
                    Some((
                        sealed_at.ok_or(AnnouncementStoreError::UnknownBatch)?,
                        after.as_str().to_owned(),
                    ))
                }
                None => None,
            };
            let (after_at, after_id) = after.unzip();
            let limit = page(query.limit)?;
            if let Some(subscription) = &query.subscription {
                let mut statement = db
                    .prepare_cached(
                        "SELECT b.record FROM announcement_batch_subscriptions m
                         JOIN announcement_batches b ON b.id = m.batch_id
                         WHERE m.subscription_id = ?1
                           AND (?2 IS NULL OR b.sealed_at_ms > ?2
                                OR (b.sealed_at_ms = ?2 AND b.id > ?3))
                         ORDER BY b.sealed_at_ms, b.id LIMIT ?4",
                    )
                    .map_err(storage)?;
                return batches_from(
                    &mut statement,
                    params![subscription.as_str(), after_at, after_id, limit],
                );
            }
            let member = query.member.as_ref().map(Member::key);
            let mut statement = db
                .prepare_cached(
                    "SELECT record FROM announcement_batches
                     WHERE (?1 IS NULL OR member = ?1)
                       AND (?2 IS NULL OR sealed_at_ms > ?2
                            OR (sealed_at_ms = ?2 AND id > ?3))
                     ORDER BY sealed_at_ms, id LIMIT ?4",
                )
                .map_err(storage)?;
            batches_from(&mut statement, params![member, after_at, after_id, limit])
        })
        .await
    }

    async fn unsettled_batches(
        &self,
        after: Option<&BatchPosition>,
        limit: usize,
    ) -> Result<Vec<Batch>> {
        let after = after.cloned();
        self.run(move |db| match &after {
            None => {
                let mut statement = db
                    .prepare_cached(
                        "SELECT record FROM announcement_batches
                         WHERE state IN ('sealed', 'submitting', 'committed')
                         ORDER BY sealed_at_ms, id LIMIT ?1",
                    )
                    .map_err(storage)?;
                batches_from(&mut statement, params![page(limit)?])
            }
            Some(after) => {
                let mut statement = db
                    .prepare_cached(
                        "SELECT record FROM announcement_batches
                         WHERE state IN ('sealed', 'submitting', 'committed')
                           AND (sealed_at_ms > ?1 OR (sealed_at_ms = ?1 AND id > ?2))
                         ORDER BY sealed_at_ms, id LIMIT ?3",
                    )
                    .map_err(storage)?;
                batches_from(
                    &mut statement,
                    params![int(after.sealed_at_ms)?, after.id.as_str(), page(limit)?],
                )
            }
        })
        .await
    }

    async fn newest_unsettled_batch(&self) -> Result<Option<BatchPosition>> {
        self.run(move |db| {
            let newest: Option<(i64, String)> = db
                .query_row(
                    "SELECT sealed_at_ms, id FROM announcement_batches
                     WHERE state IN ('sealed', 'submitting', 'committed')
                     ORDER BY sealed_at_ms DESC, id DESC LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(storage)?;
            newest
                .map(|(sealed_at, id)| {
                    Ok(BatchPosition {
                        sealed_at_ms: uint(sealed_at)?,
                        id: BatchId::new(id)?,
                    })
                })
                .transpose()
        })
        .await
    }

    async fn transition_batch(
        &self,
        id: &BatchId,
        expected: BatchStateTag,
        event: BatchEvent,
        now_ms: u64,
    ) -> Result<Batch> {
        let id = id.clone();
        self.run(move |db| transition_batch_in(db, &id, expected, &event, now_ms))
            .await
    }

    async fn late_reads_due(&self, now_ms: u64, limit: usize) -> Result<Vec<Batch>> {
        self.run(move |db| {
            let mut statement = db
                .prepare_cached(
                    "SELECT record FROM announcement_batches
                     WHERE late_read_at_ms IS NOT NULL AND late_read_at_ms <= ?1
                     ORDER BY late_read_at_ms, id LIMIT ?2",
                )
                .map_err(storage)?;
            batches_from(&mut statement, params![int(now_ms)?, page(limit)?])
        })
        .await
    }

    async fn record_late_read(
        &self,
        id: &BatchId,
        late: Option<LateOutcome>,
        next_read_at_ms: Option<u64>,
    ) -> Result<Batch> {
        let id = id.clone();
        self.run(move |db| record_late_read_in(db, &id, late, next_read_at_ms))
            .await
    }

    async fn expire(
        &self,
        now_ms: u64,
        retention_ms: u64,
        quiet_ms: u64,
        limit: usize,
    ) -> Result<ExpiryReport> {
        self.run(move |db| expire_in(db, now_ms, retention_ms, quiet_ms, limit))
            .await
    }

    async fn prune(&self, now_ms: u64, retention_ms: u64, limit: usize) -> Result<PruneReport> {
        self.run(move |db| prune_in(db, now_ms, retention_ms, limit))
            .await
    }

    async fn put_source(
        &self,
        operation: &OperationId,
        request: PutSource,
        now_ms: u64,
    ) -> Result<AnnouncementSource> {
        let operation = operation.clone();
        self.run(move |db| put_source_in(db, &operation, &request, now_ms))
            .await
    }

    async fn change_source(
        &self,
        operation: &OperationId,
        mob_id: &MobId,
        source_id: &SourceId,
        expected_revision: u64,
        change: SourceChange,
        now_ms: u64,
    ) -> Result<AnnouncementSource> {
        let (operation, mob_id, source_id) = (operation.clone(), mob_id.clone(), source_id.clone());
        self.run(move |db| {
            change_source_in(
                db,
                &operation,
                &mob_id,
                &source_id,
                expected_revision,
                change,
                now_ms,
            )
        })
        .await
    }

    async fn advance_source(
        &self,
        operation: &OperationId,
        request: AdvanceSource,
        now_ms: u64,
    ) -> Result<SourceAdvance> {
        self.advance_source_with_facts(operation, request, &RosterFacts::default(), now_ms)
            .await
    }

    async fn advance_source_with_facts(
        &self,
        operation: &OperationId,
        request: AdvanceSource,
        facts: &RosterFacts,
        now_ms: u64,
    ) -> Result<SourceAdvance> {
        let request = Arc::new(request);
        let step = {
            let (operation, request, facts) =
                (operation.clone(), Arc::clone(&request), facts.clone());
            move |db: &Connection| advance_source_in(db, &operation, &request, &facts, now_ms)
        };
        let result = self.run(step).await;
        // An onboarding refusal leaves the source unchanged, then counts
        // toward its failure streak on its own. A failure to count it never
        // replaces the refusal the caller is owed.
        if let Err(error) = &result
            && let Some(kind) = error.onboarding_refusal()
            && let Err(count_error) = self
                .run(move |db| count_refusal_in(db, &request, kind, now_ms))
                .await
        {
            tracing::warn!(
                refusal = kind,
                error = %count_error,
                "an onboarding refusal of a source advance could not be counted"
            );
        }
        result
    }

    async fn owned_topic_subscribers(
        &self,
        mob_id: &MobId,
        topics: &BTreeSet<Topic>,
    ) -> Result<BTreeSet<Member>> {
        let (mob_id, topics) = (mob_id.clone(), topics.clone());
        self.run(move |db| owned_topic_subscribers_in(db, &mob_id, &topics))
            .await
    }

    async fn source_subscribers(
        &self,
        mob_id: &MobId,
        source_id: &SourceId,
    ) -> Result<BTreeSet<Member>> {
        let (mob_id, source_id) = (mob_id.clone(), source_id.clone());
        self.run(move |db| {
            let topics = source_topics(db, &mob_id, &source_id)?;
            owned_topic_subscribers_in(db, &mob_id, &topics)
        })
        .await
    }

    async fn audit(&self, query: &AuditQuery) -> Result<Vec<AuditRow>> {
        let query = query.clone();
        self.run(move |db| audit_in(db, &query)).await
    }

    async fn record_source_failure(
        &self,
        operation: &OperationId,
        request: RecordSourceFailure,
        now_ms: u64,
    ) -> Result<AnnouncementSource> {
        let operation = operation.clone();
        self.run(move |db| record_failure_in(db, &operation, &request, now_ms))
            .await
    }

    async fn source_streams(
        &self,
        mob_id: &MobId,
        source_id: &SourceId,
    ) -> Result<Vec<SourceStream>> {
        let (mob_id, source_id) = (mob_id.clone(), source_id.clone());
        self.run(move |db| source_streams_in(db, &mob_id, &source_id))
            .await
    }

    async fn source(
        &self,
        mob_id: &MobId,
        source_id: &SourceId,
    ) -> Result<Option<AnnouncementSource>> {
        let (mob_id, source_id) = (mob_id.clone(), source_id.clone());
        self.run(move |db| load_source(db, &mob_id, &source_id))
            .await
    }

    async fn sources(&self, query: &SourceQuery) -> Result<Vec<AnnouncementSource>> {
        let query = query.clone();
        self.run(move |db| sources_in(db, &query)).await
    }
}
