//! The mob announcement store contract.

use std::collections::BTreeSet;

use async_trait::async_trait;

use super::types::{
    AnnounceOutcome, AnnounceRequest, Announcement, Batch, BatchEvent, BatchId, BatchPosition,
    BatchQuery, BatchStateTag, Member, MemberBacklog, MobId, MobLineage, OperationId, PruneReport,
    PutSubscription, Result, SealingPolicy, SourceKey, Subscription, SubscriptionChange,
    SubscriptionChangeOutcome, SubscriptionEnd, SubscriptionId,
};

/// Durable mob announcements, member subscriptions and delivery batches.
///
/// Every method is one atomic transaction. In particular:
///
/// - `announce` commits the announcement and its capture for every matching
///   subscription together, so a committed announcement is never missing an
///   owed announcement it should have produced, and nothing is captured for
///   an announcement that did not commit.
/// - `seal_batch` re-checks the sealing policy and binds the batch's exact
///   items and immutable body in the same transaction.
/// - `announce` refuses an announcement whose rendering, JSON-encoded,
///   exceeds [`super::types::MAX_ITEM_WIRE_BYTES`], so any one announcement
///   fits a batch for a member on a peer gateway.
/// - `transition_batch` applies [`super::types::BatchState::apply`] against
///   the state the caller observed, and settles or moves the batch's owed
///   announcements in the same transaction.
///
/// Mutations of subscriptions carry an operation id: a replay with the same
/// request returns the original result, and the same id with another request
/// is refused. Implementations must keep those receipts at least as long as
/// the retention window.
#[async_trait]
pub trait AnnouncementStore: Send + Sync {
    async fn announce(&self, request: AnnounceRequest, now_ms: u64) -> Result<AnnounceOutcome>;

    async fn announcement(
        &self,
        mob_id: &MobId,
        source_key: &SourceKey,
    ) -> Result<Option<Announcement>>;

    async fn put_subscription(
        &self,
        operation: &OperationId,
        request: PutSubscription,
        now_ms: u64,
    ) -> Result<Subscription>;

    async fn change_subscription(
        &self,
        operation: &OperationId,
        id: &SubscriptionId,
        expected_revision: u64,
        change: SubscriptionChange,
        now_ms: u64,
    ) -> Result<SubscriptionChangeOutcome>;

    async fn subscription(&self, id: &SubscriptionId) -> Result<Option<Subscription>>;

    /// End every live subscription of `member` for `end`, a final fact the
    /// caller read from the member's owner: each is marked ended at a new
    /// revision, what it captured and no batch holds settles, and its
    /// sealed batches nothing else wants are cancelled. Returns the ended
    /// subscriptions.
    async fn end_member(
        &self,
        member: &Member,
        end: SubscriptionEnd,
        now_ms: u64,
    ) -> Result<Vec<SubscriptionId>>;

    /// The mobs live subscriptions name, and the mobs whose announcement
    /// log is not marked ended: every mob whose destruction matters here.
    async fn known_mobs(&self) -> Result<BTreeSet<MobId>>;

    /// Drop destroyed mobs from every live subscription that names them, at
    /// a new revision. What was captured from them stays owed. A
    /// subscription left with no mob ends ([`SubscriptionEnd::MobsEnded`]).
    /// Their announcement logs are marked ended, so retention can remove
    /// them once nothing refers to them. Returns the changed subscriptions.
    async fn drop_ended_mobs(
        &self,
        ended: &BTreeSet<MobId>,
        now_ms: u64,
    ) -> Result<Vec<SubscriptionId>>;

    async fn subscriptions(
        &self,
        member: Option<&Member>,
        after: Option<&SubscriptionId>,
        limit: usize,
    ) -> Result<Vec<Subscription>>;

    /// The live lineage records: at most one per mob, for the incarnation
    /// hosted when it was derived.
    async fn lineage(&self) -> Result<Vec<MobLineage>>;

    /// Record a derived lineage. A live record of the same mob and
    /// incarnation stays as it is: a relation is derived once. A live
    /// record of another incarnation ends first. Returns whether a record
    /// was written.
    async fn record_lineage(&self, lineage: &MobLineage, now_ms: u64) -> Result<bool>;

    /// End the live lineage records of `mobs`: they are no longer hosted.
    async fn end_lineage(&self, mobs: &BTreeSet<MobId>, now_ms: u64) -> Result<()>;

    /// Members with deliverable announcements and no unsettled batch,
    /// oldest first. A member whose batch is outstanding cannot be sealed
    /// again until that batch settles, so it takes no place in the page.
    async fn backlogs(&self, limit: usize) -> Result<Vec<MemberBacklog>>;

    /// Seal the member's deliverable backlog into one batch if the policy
    /// says it is due at `now_ms`; `None` when it is not. With a wire budget
    /// the batch also stops where its JSON-encoded body, counted with
    /// [`super::types::header_wire_bound`], would exceed the budget.
    async fn seal_batch(
        &self,
        member: &Member,
        batch_id: &BatchId,
        policy: &SealingPolicy,
        wire_budget: Option<u64>,
        now_ms: u64,
    ) -> Result<Option<Batch>>;

    async fn batch(&self, id: &BatchId) -> Result<Option<Batch>>;

    async fn batches(&self, query: &BatchQuery) -> Result<Vec<Batch>>;

    /// Batches that are sealed, submitting or committed, in (sealed time,
    /// id) order, starting after `after` when it is given.
    async fn unsettled_batches(
        &self,
        after: Option<&BatchPosition>,
        limit: usize,
    ) -> Result<Vec<Batch>>;

    /// The position of the last unsettled batch in that order, if any.
    async fn newest_unsettled_batch(&self) -> Result<Option<BatchPosition>>;

    async fn transition_batch(
        &self,
        id: &BatchId,
        expected: BatchStateTag,
        event: BatchEvent,
        now_ms: u64,
    ) -> Result<Batch>;

    /// Remove what retention allows, oldest first and at most `limit` of
    /// each kind: announcements that no subscription is still owed and that
    /// are older than the window, across mobs, each mob stopping at its
    /// first announcement that is not (so its retained history never has
    /// holes); settled batches no owed announcement still names; old
    /// operation receipts; and cancelled or ended subscriptions that
    /// changed before the window and hold no unsettled batch, with the
    /// member rows nothing refers to and the logs of ended mobs that retain
    /// nothing and that no subscription names; and lineage records that
    /// ended before the window and that no live record names as parent.
    /// [`PruneReport::more`] says whether another pass has work.
    async fn prune(&self, now_ms: u64, retention_ms: u64, limit: usize) -> Result<PruneReport>;
}
