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

use super::store::AnnouncementStore;
use super::types::{
    AnnounceOutcome, AnnounceRequest, Announcement, AnnouncementStoreError, Batch, BatchEvent,
    BatchGap, BatchId, BatchItem, BatchPosition, BatchQuery, BatchState, BatchStateTag,
    ExpiryReport, GapReason, MAX_BACKFILL, MAX_ITEM_WIRE_BYTES, MAX_PAGE, Member, MemberBacklog,
    MobId, MobLineage, OperationId, PruneReport, PutSubscription, Result, SealingPolicy, SourceKey,
    StartFrontier, Subscription, SubscriptionChange, SubscriptionChangeOutcome, SubscriptionEnd,
    SubscriptionId, SubscriptionState, Topic, content_digest, header_wire_bound, merge_gaps,
    render_batch, render_gap, render_item, request_fingerprint, wire_len,
};

/// The mob announcement store's tables. A provider applies this through its own
/// migration ledger before handing a connection to
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
 sealed_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL, record TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS announcement_batches_by_state
 ON announcement_batches (state, sealed_at_ms, id);
CREATE INDEX IF NOT EXISTS announcement_batches_by_member
 ON announcement_batches (member, sealed_at_ms, id);
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
 PRIMARY KEY (member, mob_id, reason));";

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
        Ok(Self::from_connection(connection, None))
    }

    /// The caller must have applied [`SCHEMA`] under its migration contract.
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

/// What the unsettled owed announcements of `batch` amount to as expired
/// gaps: distinct announcements per mob, so one held for two of the
/// member's subscriptions counts once.
fn batch_expired_gaps(db: &Connection, batch: &BatchId) -> Result<Vec<BatchGap>> {
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
            reason: GapReason::Expired,
        });
    }
    Ok(gaps)
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
               AND s.state = 'active'",
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

fn announce_in(db: &Connection, request: &AnnounceRequest, now_ms: u64) -> Result<AnnounceOutcome> {
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
    wire_budget: Option<u64>,
    now_ms: u64,
) -> Result<Option<Batch>> {
    policy.validate()?;
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
    let mut statement = db
        .prepare(
            "SELECT o.subscription_id, o.subscription_revision, o.mob_id, o.sequence,
                    o.rendered_bytes
             FROM owed_announcements o JOIN member_subscriptions s ON s.id = o.subscription_id
             WHERE o.member = ?1 AND o.settlement IS NULL AND o.batch_id IS NULL
               AND s.state = 'active'
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
    // What the member was owed and never got rides on its next batch.
    let gaps = pending_gaps(db, member)?;
    // The encoded body is its header, its gap lines and each item's
    // encoding without the quotes: JSON escapes character by character.
    let mut wire = wire_budget.map_or(0, |_| {
        gaps.iter().fold(header_wire_bound(batch_id), |wire, gap| {
            wire.saturating_add(wire_len(&render_gap(gap)).saturating_sub(2))
        })
    });
    for (subscription, revision, mob_id, sequence, rendered) in candidates {
        let key = (MobId::new(mob_id)?, uint(sequence)?);
        let rendered = uint(rendered)?;
        if !announcements.contains_key(&key) {
            let announcement = announcement_at(db, &key.0, key.1)?;
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
        owed.push((SubscriptionId::new(subscription)?, uint(revision)?, key));
    }
    if owed.is_empty() {
        return Ok(None);
    }
    let body = render_batch(batch_id, None, &gaps, &items);
    db.execute(
        "DELETE FROM announcement_gaps WHERE member = ?1",
        params![member.key()],
    )
    .map_err(storage)?;
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
        batch.successor = Some(successor.id.clone());
    }
    if matches!(event, BatchEvent::Expire | BatchEvent::ExpireNotCommitted) {
        // The member learns of what expired, and of the gaps this batch was
        // to report, from its next batch.
        let expired = batch_expired_gaps(db, &batch.id)?;
        add_gaps(db, &batch.member, &merge_gaps(&batch.gaps, &expired))?;
    }
    if let Some(settlement) = event.settlement() {
        settle_owed(db, &batch.id, settlement.as_str())?;
    }
    batch.state = next;
    batch.updated_at_ms = now_ms;
    update_batch(db, &batch)?;
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
    limit: usize,
) -> Result<ExpiryReport> {
    let cutoff = int(now_ms.saturating_sub(retention_ms))?;
    let budget = u64::try_from(limit.max(1)).unwrap_or(u64::MAX);
    let mut report = ExpiryReport::default();

    // Captures no batch holds, owed longer than the window: each
    // announcement once per member, oldest first.
    let mut statement = db
        .prepare(
            "SELECT member, mob_id, sequence, MIN(captured_at_ms) AS at FROM owed_announcements
             WHERE batch_id IS NULL AND settlement IS NULL AND captured_at_ms <= ?1
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
               AND batch_id IS NULL AND settlement IS NULL",
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

    // Sealed batches nothing native ever started for, sealed before the
    // window: every item they hold is at least that old.
    let mut statement = db
        .prepare(
            "SELECT id FROM announcement_batches WHERE state = 'sealed' AND sealed_at_ms <= ?1
             ORDER BY sealed_at_ms, id LIMIT ?2",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(params![cutoff, int(budget.saturating_add(1))?], |row| {
            row.get::<_, String>(0)
        })
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
    Ok(report)
}

#[async_trait]
impl AnnouncementStore for SqliteAnnouncementStore {
    async fn announce(&self, request: AnnounceRequest, now_ms: u64) -> Result<AnnounceOutcome> {
        self.run(move |db| announce_in(db, &request, now_ms)).await
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
        let operation = operation.clone();
        self.run(move |db| put_subscription_in(db, &operation, &request, now_ms))
            .await
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
        wire_budget: Option<u64>,
        now_ms: u64,
    ) -> Result<Option<Batch>> {
        let (member, batch_id, policy) = (member.clone(), batch_id.clone(), *policy);
        self.run(move |db| seal_batch_in(db, &member, &batch_id, &policy, wire_budget, now_ms))
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

    async fn expire(&self, now_ms: u64, retention_ms: u64, limit: usize) -> Result<ExpiryReport> {
        self.run(move |db| expire_in(db, now_ms, retention_ms, limit))
            .await
    }

    async fn prune(&self, now_ms: u64, retention_ms: u64, limit: usize) -> Result<PruneReport> {
        self.run(move |db| prune_in(db, now_ms, retention_ms, limit))
            .await
    }
}
