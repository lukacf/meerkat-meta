//! Store conformance and owner behaviour for mob announcements.
//!
//! The conformance cases run against any [`AnnouncementStore`]; here they run
//! against the SQLite store, both fresh and reopened from disk. The owner
//! cases drive [`AnnouncementService`] against a scripted native owner that
//! models Meerkat's continuation contract: one binding per key, identical
//! replays answered with the original receipt, other content refused, and
//! status read from the binding only.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::native::{
    MemberProblem, MemberRoute, MobOrigin, MobOriginFacts, MobPresence, NativeDelivery,
    NativeStatus, NativeSubmitError, NativeUnavailable, PrepareError, continuation_for,
};
use super::owner::{
    AnnouncementConfig, AnnouncementError, AnnouncementService, NativeObservation, RoundReport,
};
use super::sqlite::SqliteAnnouncementStore;
use super::store::AnnouncementStore;
use super::types::*;

fn sid(value: &str) -> SubscriptionId {
    SubscriptionId::new(value).unwrap()
}

fn op(value: &str) -> OperationId {
    OperationId::new(value).unwrap()
}

/// The mob of the gateway these tests run as: its members are local.
const OWN_MOB: &str = "payments";

fn member(value: &str) -> Member {
    Member::new(mob(OWN_MOB), MemberIdentity::new(value).unwrap())
}

fn mob(value: &str) -> MobId {
    MobId::new(value).unwrap()
}

fn topics(values: &[&str]) -> BTreeSet<Topic> {
    values
        .iter()
        .map(|value| Topic::new(*value).unwrap())
        .collect()
}

fn announce_request(mob_id: &str, key: &str, topic: &[&str], body: &str) -> AnnounceRequest {
    AnnounceRequest {
        mob_id: mob(mob_id),
        announced_by: Announcer::Host,
        source_key: SourceKey::new(key).unwrap(),
        topics: topics(topic),
        body: body.to_string(),
        correlation_id: None,
        causes: Vec::new(),
    }
}

/// Every mob these tests announce into. A subscription made with [`put`]
/// names them all, so only a test that narrows one sees a mob left out.
const TEST_MOBS: &[&str] = &[
    "payments", "events", "builds", "refunds", "a-newer", "z-older", "a-mob", "b-mob",
];

fn put(id: &str, to: &str, topic: &[&str], start: StartFrontier) -> PutSubscription {
    PutSubscription {
        id: sid(id),
        member: member(to),
        selector: Selector {
            topics: topics(topic),
            mobs: TEST_MOBS.iter().map(|name| mob(name)).collect(),
        },
        start,
        expected_revision: None,
    }
}

fn policy() -> SealingPolicy {
    SealingPolicy {
        max_count: 256,
        max_bytes: 1_048_576,
        max_delay_ms: 1_000,
        min_wake_interval_ms: 0,
    }
}

fn batch_id(value: &str) -> BatchId {
    BatchId::new(value).unwrap()
}

fn receipt(id: &str) -> NativeReceipt {
    NativeReceipt {
        delivery_id: format!("continuation:{id}"),
        address: "rt:member:test:agent:1".into(),
        submission_digest: format!("digest-{id}"),
        committed_at_ms: 1,
    }
}

fn temp_store() -> (tempfile::TempDir, SqliteAnnouncementStore) {
    let dir = tempfile::tempdir().unwrap();
    let store =
        SqliteAnnouncementStore::open(dir.path().join("mob_announcements.sqlite3")).unwrap();
    (dir, store)
}

// ---------------------------------------------------------------------------
// Store conformance
// ---------------------------------------------------------------------------

async fn announcements_are_sequenced_per_mob_and_bound_to_content(store: &dyn AnnouncementStore) {
    let first = store
        .announce(announce_request("payments", "k1", &["ios"], "one"), 10)
        .await
        .unwrap();
    assert_eq!(first.announcement.sequence, 1);
    assert!(!first.replayed);
    let second = store
        .announce(announce_request("payments", "k2", &["ios"], "two"), 11)
        .await
        .unwrap();
    assert_eq!(second.announcement.sequence, 2);
    let other = store
        .announce(announce_request("builds", "k1", &["ios"], "one"), 12)
        .await
        .unwrap();
    assert_eq!(
        other.announcement.sequence, 1,
        "sequences of independent mobs are never shared"
    );
    let replay = store
        .announce(announce_request("payments", "k1", &["ios"], "one"), 99)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(
        replay.announcement, first.announcement,
        "a replay returns the original"
    );
    let conflict = store
        .announce(announce_request("payments", "k1", &["ios"], "changed"), 99)
        .await;
    assert_eq!(
        conflict,
        Err(AnnouncementStoreError::AnnouncementConflict {
            existing: Box::new(first.announcement.clone()),
        })
    );
    assert_eq!(
        store
            .announcement(&mob("payments"), &SourceKey::new("k1").unwrap())
            .await
            .unwrap(),
        Some(first.announcement)
    );
}

async fn capture_commits_with_the_announcement(store: &dyn AnnouncementStore) {
    store
        .announce(announce_request("payments", "before", &["ios"], "old"), 1)
        .await
        .unwrap();
    store
        .put_subscription(
            &op("o1"),
            put("s1", "agent:a", &["ios"], StartFrontier::Now),
            2,
        )
        .await
        .unwrap();
    let mut scoped = put("s2", "agent:b", &["ios"], StartFrontier::Now);
    scoped.selector.mobs = [mob("builds")].into();
    store.put_subscription(&op("o2"), scoped, 2).await.unwrap();
    let matched = store
        .announce(
            announce_request("payments", "after", &["ios", "billing"], "new"),
            3,
        )
        .await
        .unwrap();
    assert_eq!(matched.captured_for, vec![sid("s1")]);
    let unmatched = store
        .announce(announce_request("payments", "other", &["android"], "x"), 4)
        .await
        .unwrap();
    assert!(unmatched.captured_for.is_empty());
    let backlogs = store.backlogs(10).await.unwrap();
    assert_eq!(backlogs.len(), 1, "only s1's member owes anything");
    assert_eq!(backlogs[0].member, member("agent:a"));
    assert_eq!(
        backlogs[0].eligible_count, 1,
        "a subscription starting now captures nothing announced before it"
    );
}

async fn explicit_frontiers_replay_or_report_a_gap(store: &dyn AnnouncementStore) {
    for key in ["a", "b", "c"] {
        store
            .announce(announce_request("payments", key, &["ios"], key), 1)
            .await
            .unwrap();
    }
    let replaying = store
        .put_subscription(
            &op("o1"),
            put(
                "s1",
                "agent:a",
                &["ios"],
                StartFrontier::After {
                    frontier: [(mob("payments"), 1)].into(),
                },
            ),
            5,
        )
        .await
        .unwrap();
    assert_eq!(replaying.frontier.get(&mob("payments")), Some(&1));
    assert_eq!(
        store.backlogs(10).await.unwrap()[0].eligible_count,
        2,
        "sequences 2 and 3 are replayed"
    );
    store
        .change_subscription(&op("o2"), &sid("s1"), 1, SubscriptionChange::Cancel, 6)
        .await
        .unwrap();
    // Nothing is owed any more, so retention removes the whole prefix.
    store.prune(10_000, 0, 100).await.unwrap();
    let gap = store
        .put_subscription(
            &op("o3"),
            put(
                "s2",
                "agent:a",
                &["ios"],
                StartFrontier::After {
                    frontier: [(mob("payments"), 0)].into(),
                },
            ),
            10_001,
        )
        .await;
    assert_eq!(
        gap,
        Err(AnnouncementStoreError::Gap {
            mob_id: mob("payments"),
            earliest_retained: 4,
        }),
        "a frontier older than retained history is a typed gap, never a jump"
    );
}

async fn frontiers_outside_a_mob_are_refused(store: &dyn AnnouncementStore) {
    for key in ["k1", "k2"] {
        store
            .announce(announce_request("payments", key, &["ios"], key), 1)
            .await
            .unwrap();
    }
    let after = |frontier: &[(&str, u64)]| StartFrontier::After {
        frontier: frontier
            .iter()
            .map(|(name, sequence)| (mob(name), *sequence))
            .collect(),
    };
    assert_eq!(
        store
            .put_subscription(
                &op("o1"),
                put("s1", "agent:a", &["ios"], after(&[("payments", 3)])),
                2,
            )
            .await,
        Err(AnnouncementStoreError::FrontierBeyondHead {
            mob_id: mob("payments"),
            head: 2,
        }),
        "a frontier beyond the head would skip announcements not made yet; it is refused, \
         never clamped"
    );
    assert_eq!(
        store
            .put_subscription(
                &op("o2"),
                put("s2", "agent:a", &["ios"], after(&[("refunds", 1)])),
                2,
            )
            .await,
        Err(AnnouncementStoreError::FrontierBeyondHead {
            mob_id: mob("refunds"),
            head: 0,
        }),
        "a mob that has announced nothing yet is at head 0"
    );
    let mut narrow = put("s3", "agent:a", &["ios"], after(&[("refunds", 0)]));
    narrow.selector.mobs = [mob("payments")].into();
    assert!(
        matches!(
            store.put_subscription(&op("o3"), narrow, 2).await,
            Err(AnnouncementStoreError::Invalid(_))
        ),
        "a frontier may name only the mobs the subscription names"
    );
    // A frontier at the head, or at 0 for a mob that has announced nothing
    // yet, captures from the next announcement.
    let subscription = store
        .put_subscription(
            &op("o4"),
            put(
                "s4",
                "agent:a",
                &["ios"],
                after(&[("payments", 2), ("refunds", 0)]),
            ),
            2,
        )
        .await
        .unwrap();
    assert_eq!(subscription.frontier.get(&mob("payments")), Some(&2));
    let refund = store
        .announce(announce_request("refunds", "r1", &["ios"], "r1"), 3)
        .await
        .unwrap();
    assert_eq!(refund.captured_for, vec![sid("s4")]);
}

async fn subscription_changes_are_revisioned_and_idempotent(store: &dyn AnnouncementStore) {
    let created = store
        .put_subscription(
            &op("o1"),
            put("s1", "agent:a", &["ios"], StartFrontier::Now),
            1,
        )
        .await
        .unwrap();
    assert_eq!(created.revision, 1);
    assert_eq!(
        store
            .put_subscription(
                &op("o1"),
                put("s1", "agent:a", &["ios"], StartFrontier::Now),
                2
            )
            .await
            .unwrap(),
        created,
        "the same operation replays its original result"
    );
    assert_eq!(
        store
            .put_subscription(
                &op("o1"),
                put("s1", "agent:a", &["web"], StartFrontier::Now),
                2
            )
            .await,
        Err(AnnouncementStoreError::OperationReused)
    );
    assert_eq!(
        store
            .put_subscription(
                &op("o2"),
                put("s1", "agent:a", &["ios"], StartFrontier::Now),
                2
            )
            .await,
        Err(AnnouncementStoreError::StaleRevision { current: 1 }),
        "creating an existing subscription names no revision"
    );
    let mut update = put("s1", "agent:a", &["web"], StartFrontier::Now);
    update.expected_revision = Some(7);
    assert_eq!(
        store.put_subscription(&op("o3"), update.clone(), 3).await,
        Err(AnnouncementStoreError::StaleRevision { current: 1 })
    );
    update.expected_revision = Some(1);
    update.member = member("agent:b");
    assert_eq!(
        store.put_subscription(&op("o4"), update.clone(), 3).await,
        Err(AnnouncementStoreError::MemberMismatch)
    );
    update.member = member("agent:a");
    let updated = store.put_subscription(&op("o5"), update, 3).await.unwrap();
    assert_eq!(updated.revision, 2);
    assert_eq!(updated.selector_revision, 2);
    let paused = store
        .change_subscription(&op("o6"), &sid("s1"), 2, SubscriptionChange::Pause, 4)
        .await
        .unwrap();
    assert_eq!(paused.subscription.state, SubscriptionState::Paused);
    assert_eq!(
        store
            .change_subscription(&op("o6"), &sid("s1"), 2, SubscriptionChange::Pause, 5)
            .await
            .unwrap(),
        paused
    );
    let cancelled = store
        .change_subscription(&op("o7"), &sid("s1"), 3, SubscriptionChange::Cancel, 6)
        .await
        .unwrap();
    assert_eq!(cancelled.subscription.state, SubscriptionState::Cancelled);
    assert_eq!(
        store
            .change_subscription(&op("o8"), &sid("s1"), 4, SubscriptionChange::Resume, 7)
            .await,
        Err(AnnouncementStoreError::SubscriptionCancelled)
    );
}

async fn paused_subscriptions_capture_and_hold(store: &dyn AnnouncementStore) {
    store
        .put_subscription(
            &op("o1"),
            put("s1", "agent:a", &["ios"], StartFrontier::Now),
            1,
        )
        .await
        .unwrap();
    store
        .change_subscription(&op("o2"), &sid("s1"), 1, SubscriptionChange::Pause, 2)
        .await
        .unwrap();
    for index in 0..5 {
        let outcome = store
            .announce(
                announce_request("payments", &format!("k{index}"), &["ios"], "x"),
                3,
            )
            .await
            .unwrap();
        assert_eq!(
            outcome.captured_for,
            vec![sid("s1")],
            "a paused subscription captures"
        );
    }
    assert!(
        store.backlogs(10).await.unwrap().is_empty(),
        "a paused subscription's announcements are not deliverable"
    );
    assert_eq!(
        store
            .seal_batch(
                &member("agent:a"),
                &batch_id("b1"),
                &policy(),
                None,
                1_000_000
            )
            .await
            .unwrap(),
        None
    );
    store
        .change_subscription(&op("o3"), &sid("s1"), 2, SubscriptionChange::Resume, 4)
        .await
        .unwrap();
    let backlog = store.backlogs(10).await.unwrap().remove(0);
    assert_eq!(backlog.eligible_count, 5);
    assert_eq!(backlog.flush_requested_at_ms, Some(4), "a resume flushes");
    assert_eq!(policy().due_at(&backlog), Some(0));
}

async fn sealing_binds_exact_members_once(store: &dyn AnnouncementStore) {
    for id in ["s1", "s2"] {
        store
            .put_subscription(&op(id), put(id, "agent:a", &["ios"], StartFrontier::Now), 1)
            .await
            .unwrap();
    }
    store
        .announce(announce_request("payments", "k1", &["ios"], "shared"), 2)
        .await
        .unwrap();
    assert_eq!(
        store
            .seal_batch(&member("agent:a"), &batch_id("b0"), &policy(), None, 500)
            .await
            .unwrap(),
        None,
        "not due before the delay"
    );
    let batch = store
        .seal_batch(&member("agent:a"), &batch_id("b1"), &policy(), None, 1_002)
        .await
        .unwrap()
        .expect("due after the delay");
    assert_eq!(
        batch.items.len(),
        2,
        "each subscription's capture is an item"
    );
    assert_eq!(
        batch
            .body
            .matches("Announcement #1 in mob \"payments\"")
            .count(),
        1,
        "one announcement is rendered once"
    );
    assert_eq!(batch.content_digest, content_digest(&batch.body));
    assert_eq!(batch.state, BatchState::Sealed);
    let backlog_after = store.backlogs(10).await.unwrap();
    assert!(backlog_after.is_empty(), "everything owed is in the batch");
    store
        .announce(announce_request("payments", "k2", &["ios"], "later"), 1_003)
        .await
        .unwrap();
    assert!(
        store.backlogs(10).await.unwrap().is_empty(),
        "a member with a batch outstanding takes no place in the backlog page"
    );
    assert_eq!(
        store
            .seal_batch(
                &member("agent:a"),
                &batch_id("b2"),
                &policy(),
                None,
                9_999_999
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(store.batch(&batch_id("b1")).await.unwrap(), Some(batch));
}

async fn transitions_settle_owed_announcements(store: &dyn AnnouncementStore) {
    store
        .put_subscription(
            &op("o1"),
            put("s1", "agent:a", &["ios"], StartFrontier::Now),
            1,
        )
        .await
        .unwrap();
    store
        .announce(announce_request("payments", "k1", &["ios"], "x"), 2)
        .await
        .unwrap();
    store
        .seal_batch(&member("agent:a"), &batch_id("b1"), &policy(), None, 2_000)
        .await
        .unwrap()
        .unwrap();
    let id = batch_id("b1");
    assert_eq!(
        store
            .transition_batch(&id, BatchStateTag::Committed, BatchEvent::BeginSubmit, 3)
            .await,
        Err(AnnouncementStoreError::BatchMoved {
            current: BatchStateTag::Sealed,
        })
    );
    store
        .transition_batch(&id, BatchStateTag::Sealed, BatchEvent::BeginSubmit, 3)
        .await
        .unwrap();
    store
        .transition_batch(
            &id,
            BatchStateTag::Submitting,
            BatchEvent::NativeCommitted {
                receipt: receipt("b1"),
            },
            4,
        )
        .await
        .unwrap();
    assert_eq!(store.unsettled_batches(None, 10).await.unwrap().len(), 1);
    let delivered = store
        .transition_batch(
            &id,
            BatchStateTag::Committed,
            BatchEvent::NativeApplied {
                receipt: receipt("b1"),
                session_id: "session".into(),
                input_id: "input".into(),
            },
            5,
        )
        .await
        .unwrap();
    assert_eq!(delivered.state.tag(), BatchStateTag::Delivered);
    assert!(store.unsettled_batches(None, 10).await.unwrap().is_empty());
    let report = store.prune(10_000_000, 0, 100).await.unwrap();
    assert_eq!(
        report.announcements, 1,
        "a settled announcement no longer holds retention"
    );
    assert_eq!(report.batches, 1);
}

async fn stranded_batches_hand_over_to_a_successor(store: &dyn AnnouncementStore) {
    store
        .put_subscription(
            &op("o1"),
            put("s1", "agent:a", &["ios"], StartFrontier::Now),
            1,
        )
        .await
        .unwrap();
    store
        .announce(announce_request("payments", "k1", &["ios"], "x"), 2)
        .await
        .unwrap();
    let id = batch_id("b1");
    store
        .seal_batch(&member("agent:a"), &id, &policy(), None, 2_000)
        .await
        .unwrap()
        .unwrap();
    store
        .transition_batch(&id, BatchStateTag::Sealed, BatchEvent::BeginSubmit, 3)
        .await
        .unwrap();
    store
        .transition_batch(
            &id,
            BatchStateTag::Submitting,
            BatchEvent::NativeCommitted {
                receipt: receipt("b1"),
            },
            4,
        )
        .await
        .unwrap();
    let stranded = store
        .transition_batch(
            &id,
            BatchStateTag::Committed,
            BatchEvent::NativeStranded {
                receipt: receipt("b1"),
                successor: Some(SuccessorBatch {
                    id: batch_id("b2"),
                    body: "redelivery".into(),
                    gaps: Vec::new(),
                }),
            },
            5,
        )
        .await
        .unwrap();
    assert_eq!(stranded.successor, Some(batch_id("b2")));
    let successor = store.batch(&batch_id("b2")).await.unwrap().unwrap();
    assert_eq!(successor.predecessor, Some(id));
    assert_eq!(
        successor.items, stranded.items,
        "the same announcements move on"
    );
    assert_eq!(successor.state, BatchState::Sealed);
    let report = store.prune(10_000_000, 0, 100).await.unwrap();
    assert_eq!(
        report.announcements, 0,
        "announcements still owed through the successor keep their announcement"
    );
}

async fn cancellation_settles_what_was_never_dispatched(store: &dyn AnnouncementStore) {
    store
        .put_subscription(
            &op("o1"),
            put("s1", "agent:a", &["ios"], StartFrontier::Now),
            1,
        )
        .await
        .unwrap();
    store
        .announce(announce_request("payments", "k1", &["ios"], "x"), 2)
        .await
        .unwrap();
    store
        .seal_batch(
            &member("agent:a"),
            &batch_id("sealed"),
            &policy(),
            None,
            2_000,
        )
        .await
        .unwrap()
        .unwrap();
    store
        .announce(announce_request("payments", "k2", &["ios"], "y"), 3)
        .await
        .unwrap();
    let outcome = store
        .change_subscription(&op("o2"), &sid("s1"), 1, SubscriptionChange::Cancel, 4)
        .await
        .unwrap();
    assert!(
        outcome.unresolved_batches.is_empty(),
        "a sealed batch nothing wants any more is cancelled, not left owed"
    );
    assert_eq!(
        store
            .batch(&batch_id("sealed"))
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::Cancelled
    );
    assert!(store.backlogs(10).await.unwrap().is_empty());
    let report = store.prune(10_000_000, 0, 100).await.unwrap();
    assert_eq!(report.announcements, 2);

    store
        .put_subscription(
            &op("o3"),
            put("s2", "agent:a", &["ios"], StartFrontier::Now),
            5,
        )
        .await
        .unwrap();
    store
        .announce(announce_request("payments", "k3", &["ios"], "z"), 6)
        .await
        .unwrap();
    let id = batch_id("dispatching");
    store
        .seal_batch(&member("agent:a"), &id, &policy(), None, 2_000)
        .await
        .unwrap()
        .unwrap();
    store
        .transition_batch(&id, BatchStateTag::Sealed, BatchEvent::BeginSubmit, 7)
        .await
        .unwrap();
    let outcome = store
        .change_subscription(&op("o4"), &sid("s2"), 1, SubscriptionChange::Cancel, 8)
        .await
        .unwrap();
    assert_eq!(
        outcome.unresolved_batches,
        vec![id.clone()],
        "a batch that may have reached the native owner is reported, never erased"
    );
    assert_eq!(
        store.batch(&id).await.unwrap().unwrap().state.tag(),
        BatchStateTag::Submitting
    );
}

async fn retention_keeps_a_contiguous_history(store: &dyn AnnouncementStore) {
    store
        .put_subscription(
            &op("o1"),
            put("s1", "agent:a", &["ios"], StartFrontier::Now),
            1,
        )
        .await
        .unwrap();
    store
        .announce(announce_request("payments", "k1", &["ios"], "owed"), 2)
        .await
        .unwrap();
    store
        .announce(announce_request("payments", "k2", &["web"], "free"), 3)
        .await
        .unwrap();
    let report = store.prune(1_000_000, 0, 100).await.unwrap();
    assert_eq!(
        report.announcements, 0,
        "an owed announcement blocks retention behind it, so history has no holes"
    );
    assert!(
        store
            .announcement(&mob("payments"), &SourceKey::new("k2").unwrap())
            .await
            .unwrap()
            .is_some()
    );
}

async fn retention_prunes_the_oldest_first_across_mobs(store: &dyn AnnouncementStore) {
    store
        .announce(announce_request("z-older", "k1", &["ios"], "first"), 1)
        .await
        .unwrap();
    store
        .announce(announce_request("a-newer", "k1", &["ios"], "second"), 2)
        .await
        .unwrap();
    let report = store.prune(1_000_000, 0, 1).await.unwrap();
    assert_eq!(report.announcements, 1);
    assert!(
        store
            .announcement(&mob("z-older"), &SourceKey::new("k1").unwrap())
            .await
            .unwrap()
            .is_none(),
        "the oldest announcement goes first, whatever its mob_id is named"
    );
    assert!(
        store
            .announcement(&mob("a-newer"), &SourceKey::new("k1").unwrap())
            .await
            .unwrap()
            .is_some()
    );
}

async fn retention_passes_report_when_more_work_is_left(store: &dyn AnnouncementStore) {
    // Three members owe the first announcement; each one's batch of it is
    // cancelled, so all three batches can go once the announcement has gone.
    for (index, agent) in ["agent:a", "agent:b", "agent:c"].iter().enumerate() {
        store
            .put_subscription(
                &op(&format!("o{index}")),
                put(&format!("s{index}"), agent, &["ios"], StartFrontier::Now),
                1,
            )
            .await
            .unwrap();
    }
    for index in 0..3 {
        let topic = if index == 0 { "ios" } else { "web" };
        store
            .announce(
                announce_request("payments", &format!("k{index}"), &[topic], "old"),
                1,
            )
            .await
            .unwrap();
    }
    for (index, agent) in ["agent:a", "agent:b", "agent:c"].iter().enumerate() {
        let id = batch_id(&format!("b{index}"));
        store
            .seal_batch(&member(agent), &id, &policy(), None, 2_000)
            .await
            .unwrap()
            .unwrap();
        store
            .transition_batch(&id, BatchStateTag::Sealed, BatchEvent::Cancel, 3_000)
            .await
            .unwrap();
    }
    let first = store.prune(1_000_000, 0, 2).await.unwrap();
    assert_eq!(
        (first.announcements, first.batches, first.operation_receipts),
        (2, 2, 2),
        "each kind has its own limit"
    );
    assert!(first.more, "a pass that stopped at its limit says so");
    let second = store.prune(1_000_000, 0, 2).await.unwrap();
    assert_eq!(
        (
            second.announcements,
            second.batches,
            second.operation_receipts
        ),
        (1, 1, 1)
    );
    assert!(!second.more, "nothing is left for another pass");
}

fn peer_member(identity: &str, peer: &str) -> Member {
    Member::new(mob(peer), MemberIdentity::new(identity).unwrap())
}

fn put_for(id: &str, to: Member, topic: &[&str]) -> PutSubscription {
    PutSubscription {
        member: to,
        ..put(id, "agent:a", topic, StartFrontier::Now)
    }
}

async fn remote_batches_fit_one_delivery_request(store: &dyn AnnouncementStore) {
    let remote = peer_member("agent:a", "mob-b");
    store
        .put_subscription(&op("o1"), put_for("s1", remote.clone(), &["ios"]), 1)
        .await
        .unwrap();
    store
        .put_subscription(
            &op("o2"),
            put("s2", "agent:c", &["ios"], StartFrontier::Now),
            1,
        )
        .await
        .unwrap();
    // Forty announcements of about 2 KiB: more than one delivery request
    // carries, far less than the byte cap.
    let body = "x".repeat(2_000);
    for index in 0..40_u64 {
        store
            .announce(
                announce_request("payments", &format!("k{index}"), &["ios"], &body),
                2 + index,
            )
            .await
            .unwrap();
    }
    let batch = store
        .seal_batch(
            &remote,
            &batch_id("b1"),
            &policy(),
            Some(MAX_REMOTE_BODY_WIRE_BYTES),
            1_000_000,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        !batch.items.is_empty() && batch.items.len() < 40,
        "the wire budget bounds the batch: {}",
        batch.items.len()
    );
    assert_eq!(batch.member, remote);
    assert!(wire_len(&batch.body) <= MAX_REMOTE_BODY_WIRE_BYTES);
    // A successor carrying the same announcements fits too.
    let mut items = Vec::new();
    for item in &batch.items {
        let announcement = store
            .announcement(&item.mob_id, &item.source_key)
            .await
            .unwrap()
            .unwrap();
        items.push(render_item(&announcement));
    }
    let successor_id = BatchId::new(format!("batch-{}", uuid::Uuid::new_v4())).unwrap();
    let successor = render_batch(&successor_id, Some(&batch.id), &[], &items);
    assert!(wire_len(&successor) <= MAX_REMOTE_BODY_WIRE_BYTES);
    // A local member's batch is bounded by its byte cap alone.
    let local = store
        .seal_batch(
            &member("agent:c"),
            &batch_id("b2"),
            &policy(),
            None,
            1_000_000,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(local.items.len(), 40);
}

async fn oversized_renderings_are_refused(store: &dyn AnnouncementStore) {
    // Line breaks render far larger than their byte count.
    let body = "\n".repeat(MAX_BODY_BYTES);
    assert!(matches!(
        store
            .announce(announce_request("payments", "k1", &["ios"], &body), 1)
            .await,
        Err(AnnouncementStoreError::TooLarge(_))
    ));
    let body = "x".repeat(MAX_BODY_BYTES);
    store
        .announce(announce_request("payments", "k2", &["ios"], &body), 2)
        .await
        .unwrap();
}

async fn the_unsettled_walk_resumes_after_a_position(store: &dyn AnnouncementStore) {
    for (index, agent) in ["agent:a", "agent:b", "agent:c"].iter().enumerate() {
        let id = format!("s{index}");
        let topic = format!("t{index}");
        store
            .put_subscription(
                &op(&format!("o{index}")),
                put(&id, agent, &[topic.as_str()], StartFrontier::Now),
                1,
            )
            .await
            .unwrap();
        store
            .announce(
                announce_request("events", &format!("k{index}"), &[topic.as_str()], "x"),
                2,
            )
            .await
            .unwrap();
        store
            .seal_batch(
                &member(agent),
                &batch_id(&format!("b{index}")),
                &policy(),
                None,
                2_000 + index as u64,
            )
            .await
            .unwrap()
            .expect("due once the delay passed");
    }
    let all = store.unsettled_batches(None, 10).await.unwrap();
    let ids = |batches: &[Batch]| {
        batches
            .iter()
            .map(|batch| batch.id.to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&all), ["b0", "b1", "b2"], "(sealed time, id) order");
    let after = BatchPosition::of(&all[0]);
    assert_eq!(
        ids(&store.unsettled_batches(Some(&after), 10).await.unwrap()),
        ["b1", "b2"],
        "the walk resumes after the position"
    );
    let end = BatchPosition::of(&all[2]);
    assert!(
        store
            .unsettled_batches(Some(&end), 10)
            .await
            .unwrap()
            .is_empty()
    );
    store
        .announce(announce_request("events", "k0-more", &["t0"], "y"), 2_010)
        .await
        .unwrap();
    assert!(
        store.backlogs(10).await.unwrap().is_empty(),
        "agent:a's new announcement waits behind its outstanding batch and takes no place"
    );
}

async fn lifecycle_ends_subscriptions_on_final_facts(store: &dyn AnnouncementStore) {
    store
        .put_subscription(
            &op("o1"),
            put("s1", "agent:a", &["ios"], StartFrontier::Now),
            1,
        )
        .await
        .unwrap();
    store
        .put_subscription(
            &op("o2"),
            put("s2", "agent:b", &["ios"], StartFrontier::Now),
            1,
        )
        .await
        .unwrap();
    store
        .announce(announce_request("payments", "k1", &["ios"], "x"), 2)
        .await
        .unwrap();
    store
        .seal_batch(
            &member("agent:a"),
            &batch_id("sealed"),
            &policy(),
            None,
            2_000,
        )
        .await
        .unwrap()
        .unwrap();
    store
        .announce(announce_request("payments", "k2", &["ios"], "y"), 3)
        .await
        .unwrap();
    let ended = store
        .end_member(&member("agent:a"), SubscriptionEnd::MemberLeft, 4)
        .await
        .unwrap();
    assert_eq!(ended, vec![sid("s1")]);
    let s1 = store.subscription(&sid("s1")).await.unwrap().unwrap();
    assert_eq!(s1.state, SubscriptionState::Ended);
    assert_eq!(s1.ended, Some(SubscriptionEnd::MemberLeft));
    assert_eq!(s1.revision, 2, "ending is a change at a new revision");
    assert_eq!(
        store
            .batch(&batch_id("sealed"))
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::Cancelled,
        "a sealed batch nothing wants any more is cancelled"
    );
    let owing = store
        .backlogs(10)
        .await
        .unwrap()
        .into_iter()
        .map(|backlog| backlog.member)
        .collect::<Vec<_>>();
    assert_eq!(
        owing,
        vec![member("agent:b")],
        "what the ended subscription captured is settled"
    );
    let later = store
        .announce(announce_request("payments", "k3", &["ios"], "z"), 5)
        .await
        .unwrap();
    assert_eq!(
        later.captured_for,
        vec![sid("s2")],
        "an ended subscription captures nothing"
    );
    let ended_refusal = AnnouncementStoreError::SubscriptionEnded {
        end: SubscriptionEnd::MemberLeft,
    };
    assert_eq!(
        store
            .change_subscription(&op("o3"), &sid("s1"), 2, SubscriptionChange::Resume, 6)
            .await
            .unwrap_err(),
        ended_refusal
    );
    let mut replace = put("s1", "agent:a", &["ios"], StartFrontier::Now);
    replace.expected_revision = Some(2);
    assert_eq!(
        store
            .put_subscription(&op("o4"), replace, 6)
            .await
            .unwrap_err(),
        ended_refusal
    );
    assert!(
        store
            .end_member(&member("agent:a"), SubscriptionEnd::MemberLeft, 7)
            .await
            .unwrap()
            .is_empty(),
        "nothing live is left to end"
    );
}

async fn destroyed_mobs_leave_the_subscriptions_that_name_them(store: &dyn AnnouncementStore) {
    let mut both = put("s1", "agent:a", &["ios"], StartFrontier::Now);
    both.selector.mobs = [mob("payments"), mob("builds")].into();
    store.put_subscription(&op("o1"), both, 1).await.unwrap();
    let mut only = put("s2", "agent:b", &["ios"], StartFrontier::Now);
    only.selector.mobs = [mob("builds")].into();
    store.put_subscription(&op("o2"), only, 1).await.unwrap();
    store
        .announce(announce_request("builds", "k1", &["ios"], "owed"), 2)
        .await
        .unwrap();
    assert_eq!(
        store.known_mobs().await.unwrap(),
        [mob("payments"), mob("builds")].into()
    );
    let changed = store
        .drop_ended_mobs(&[mob("builds")].into(), 3)
        .await
        .unwrap();
    assert_eq!(changed, vec![sid("s1"), sid("s2")]);
    let s1 = store.subscription(&sid("s1")).await.unwrap().unwrap();
    assert_eq!(s1.state, SubscriptionState::Active);
    assert_eq!(s1.selector.mobs, [mob("payments")].into());
    assert_eq!(s1.ended_mobs, [mob("builds")].into());
    assert_eq!(s1.revision, 2);
    assert!(!s1.frontier.contains_key(&mob("builds")));
    let s2 = store.subscription(&sid("s2")).await.unwrap().unwrap();
    assert_eq!(s2.state, SubscriptionState::Ended);
    assert_eq!(s2.ended, Some(SubscriptionEnd::MobsEnded));
    assert_eq!(s2.ended_mobs, [mob("builds")].into());
    let backlogs = store.backlogs(10).await.unwrap();
    assert_eq!(backlogs.len(), 1);
    assert_eq!(backlogs[0].member, member("agent:a"));
    assert_eq!(
        backlogs[0].eligible_count, 1,
        "what was captured from a destroyed mob stays owed"
    );
    assert_eq!(
        store.known_mobs().await.unwrap(),
        [mob("payments")].into(),
        "the destroyed mob's log is marked ended"
    );
    assert!(
        store
            .drop_ended_mobs(&[mob("builds")].into(), 4)
            .await
            .unwrap()
            .is_empty(),
        "dropping again changes nothing"
    );
    let again = store
        .announce(announce_request("builds", "k2", &["ios"], "a new mob"), 5)
        .await
        .unwrap();
    assert!(
        again.captured_for.is_empty(),
        "a mob created later under the same id is another mob"
    );
    assert_eq!(
        store.known_mobs().await.unwrap(),
        [mob("payments"), mob("builds")].into(),
        "announcing into a hosted mob opens its log again"
    );
}

async fn retention_removes_what_ended_subscriptions_and_destroyed_mobs_leave(
    store: &dyn AnnouncementStore,
) {
    let mut only = put("s1", "agent:a", &["ios"], StartFrontier::Now);
    only.selector.mobs = [mob("builds")].into();
    store.put_subscription(&op("o1"), only, 1).await.unwrap();
    store
        .announce(announce_request("builds", "k1", &["ios"], "x"), 2)
        .await
        .unwrap();
    store
        .drop_ended_mobs(&[mob("builds")].into(), 3)
        .await
        .unwrap();
    let early = store.prune(4, 1_000, 100).await.unwrap();
    assert_eq!(
        (early.announcements, early.subscriptions),
        (0, 0),
        "nothing goes before the window has passed"
    );
    assert!(store.subscription(&sid("s1")).await.unwrap().is_some());
    let report = store.prune(1_000_000, 0, 100).await.unwrap();
    assert_eq!(report.announcements, 1);
    assert_eq!(report.subscriptions, 1);
    assert!(!report.more);
    assert!(store.subscription(&sid("s1")).await.unwrap().is_none());
    assert!(
        store.known_mobs().await.unwrap().is_empty(),
        "the destroyed mob's log is removed once it retains nothing"
    );
    let fresh = store
        .announce(announce_request("builds", "k1", &["ios"], "y"), 5)
        .await
        .unwrap();
    assert_eq!(
        (fresh.announcement.sequence, fresh.replayed),
        (1, false),
        "a mob created later under the same id starts a log of its own"
    );
}

async fn retention_reads_only_mobs_with_old_announcements(store: &dyn AnnouncementStore) {
    // Fifty quiet mobs with recent announcements, and one with a backlog of
    // old ones that takes two passes.
    for index in 0..50 {
        store
            .announce(
                announce_request(&format!("quiet-{index:02}"), "k", &["ios"], "recent"),
                900_000,
            )
            .await
            .unwrap();
    }
    for index in 0..5_u64 {
        store
            .announce(
                announce_request("busy", &format!("k{index}"), &["ios"], "old"),
                10 + index,
            )
            .await
            .unwrap();
    }
    let first = store.prune(1_000_000, 500_000, 3).await.unwrap();
    assert_eq!((first.announcements, first.more), (3, true));
    let second = store.prune(1_000_000, 500_000, 3).await.unwrap();
    assert_eq!((second.announcements, second.more), (2, false));
    for index in 0..50 {
        assert!(
            store
                .announcement(
                    &mob(&format!("quiet-{index:02}")),
                    &SourceKey::new("k").unwrap()
                )
                .await
                .unwrap()
                .is_some(),
            "a quiet mob keeps its history"
        );
    }
    let mut at_head = put(
        "s1",
        "agent:a",
        &["ios"],
        StartFrontier::After {
            frontier: [(mob("busy"), 5)].into(),
        },
    );
    at_head.selector.mobs = [mob("busy"), mob("quiet-07")].into();
    store
        .put_subscription(&op("o1"), at_head, 2_000_000)
        .await
        .unwrap();
    let mut behind = put(
        "s2",
        "agent:a",
        &["ios"],
        StartFrontier::After {
            frontier: [(mob("busy"), 4)].into(),
        },
    );
    behind.selector.mobs = [mob("busy")].into();
    assert_eq!(
        store.put_subscription(&op("o2"), behind, 2_000_000).await,
        Err(AnnouncementStoreError::Gap {
            mob_id: mob("busy"),
            earliest_retained: 6,
        }),
        "the busy mob's earliest retained sequence moved past what was pruned"
    );
    let mut quiet = put(
        "s3",
        "agent:a",
        &["ios"],
        StartFrontier::After {
            frontier: [(mob("quiet-07"), 0)].into(),
        },
    );
    quiet.selector.mobs = [mob("quiet-07")].into();
    store
        .put_subscription(&op("o3"), quiet, 2_000_000)
        .await
        .expect("a quiet mob still retains its first announcement");
}

async fn run_conformance<F, Fut>(name: &str, case: F)
where
    F: Fn(Arc<dyn AnnouncementStore>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let (_dir, store) = temp_store();
    eprintln!("announcement store conformance: {name}");
    case(Arc::new(store)).await;
}

#[tokio::test]
async fn sqlite_store_conforms() {
    run_conformance("announcements", |store| async move {
        announcements_are_sequenced_per_mob_and_bound_to_content(store.as_ref()).await;
    })
    .await;
    run_conformance("capture", |store| async move {
        capture_commits_with_the_announcement(store.as_ref()).await;
    })
    .await;
    run_conformance("frontiers", |store| async move {
        explicit_frontiers_replay_or_report_a_gap(store.as_ref()).await;
    })
    .await;
    run_conformance("frontiers outside a mob", |store| async move {
        frontiers_outside_a_mob_are_refused(store.as_ref()).await;
    })
    .await;
    run_conformance("revisions", |store| async move {
        subscription_changes_are_revisioned_and_idempotent(store.as_ref()).await;
    })
    .await;
    run_conformance("pause", |store| async move {
        paused_subscriptions_capture_and_hold(store.as_ref()).await;
    })
    .await;
    run_conformance("sealing", |store| async move {
        sealing_binds_exact_members_once(store.as_ref()).await;
    })
    .await;
    run_conformance("transitions", |store| async move {
        transitions_settle_owed_announcements(store.as_ref()).await;
    })
    .await;
    run_conformance("stranded", |store| async move {
        stranded_batches_hand_over_to_a_successor(store.as_ref()).await;
    })
    .await;
    run_conformance("cancellation", |store| async move {
        cancellation_settles_what_was_never_dispatched(store.as_ref()).await;
    })
    .await;
    run_conformance("retention", |store| async move {
        retention_keeps_a_contiguous_history(store.as_ref()).await;
    })
    .await;
    run_conformance("wire budget", |store| async move {
        remote_batches_fit_one_delivery_request(store.as_ref()).await;
    })
    .await;
    run_conformance("item size", |store| async move {
        oversized_renderings_are_refused(store.as_ref()).await;
    })
    .await;
    run_conformance("unsettled walk", |store| async move {
        the_unsettled_walk_resumes_after_a_position(store.as_ref()).await;
    })
    .await;
    run_conformance("retention order", |store| async move {
        retention_prunes_the_oldest_first_across_mobs(store.as_ref()).await;
    })
    .await;
    run_conformance("retention budgets", |store| async move {
        retention_passes_report_when_more_work_is_left(store.as_ref()).await;
    })
    .await;
    run_conformance("lifecycle", |store| async move {
        lifecycle_ends_subscriptions_on_final_facts(store.as_ref()).await;
    })
    .await;
    run_conformance("destroyed mobs", |store| async move {
        destroyed_mobs_leave_the_subscriptions_that_name_them(store.as_ref()).await;
    })
    .await;
    run_conformance("retention of ended rows", |store| async move {
        retention_removes_what_ended_subscriptions_and_destroyed_mobs_leave(store.as_ref()).await;
    })
    .await;
    run_conformance("retention reads", |store| async move {
        retention_reads_only_mobs_with_old_announcements(store.as_ref()).await;
    })
    .await;
    run_conformance("lineage", |store| async move {
        lineage_is_recorded_once_per_incarnation(store.as_ref()).await;
    })
    .await;
    run_conformance("expiry", |store| async move {
        expiry_settles_what_waited_past_the_window(store.as_ref()).await;
    })
    .await;
    run_conformance("retention of native-held bodies", |store| async move {
        retention_drops_bodies_a_native_held_batch_carries(store.as_ref()).await;
    })
    .await;
}

fn incarnation(owner: &str, first_event_at_ms: u64) -> MobIncarnation {
    MobIncarnation {
        owner_session: Some(owner.to_string()),
        first_event_at_ms,
    }
}

fn lineage_record(
    mob_id: &str,
    parent: Option<&str>,
    incarnation: MobIncarnation,
    at: u64,
) -> MobLineage {
    MobLineage {
        mob_id: mob(mob_id),
        incarnation,
        parent: parent.map(mob),
        parent_incarnation: None,
        creator: parent.map(|_| "creator".to_string()),
        recorded_at_ms: at,
    }
}

async fn lineage_is_recorded_once_per_incarnation(store: &dyn AnnouncementStore) {
    let first = lineage_record("events", Some("payments"), incarnation("s1", 10), 100);
    assert!(store.record_lineage(&first, 100).await.unwrap());
    // Derived once: the same incarnation keeps its first record.
    let again = lineage_record("events", Some("builds"), incarnation("s1", 10), 200);
    assert!(!store.record_lineage(&again, 200).await.unwrap());
    assert_eq!(store.lineage().await.unwrap(), vec![first.clone()]);
    // Another incarnation (same id, created again) replaces it.
    let second = lineage_record("events", Some("refunds"), incarnation("s1", 20), 300);
    assert!(store.record_lineage(&second, 300).await.unwrap());
    assert_eq!(store.lineage().await.unwrap(), vec![second.clone()]);
    let child = lineage_record("builds", Some("events"), incarnation("s2", 30), 300);
    let unrelated = lineage_record("refunds", None, incarnation("s3", 40), 300);
    store.record_lineage(&child, 300).await.unwrap();
    store.record_lineage(&unrelated, 300).await.unwrap();
    store
        .end_lineage(&BTreeSet::from([mob("events"), mob("refunds")]), 400)
        .await
        .unwrap();
    assert_eq!(
        store.lineage().await.unwrap(),
        vec![child.clone()],
        "ended records are not live"
    );
    // Retention removes ended records before the window, except the
    // records of a mob a live record still names as parent.
    let report = store.prune(10_000, 1_000, 100).await.unwrap();
    assert_eq!(
        report.lineage, 1,
        "refunds; events is still named by builds"
    );
    assert_eq!(store.lineage().await.unwrap(), vec![child.clone()]);
    store
        .end_lineage(&BTreeSet::from([mob("builds")]), 500)
        .await
        .unwrap();
    let report = store.prune(10_000, 1_000, 100).await.unwrap();
    assert_eq!(report.lineage, 3, "both events records and builds");
    assert!(store.lineage().await.unwrap().is_empty());
    let report = store.prune(10_000, 1_000, 100).await.unwrap();
    assert_eq!(report.lineage, 0);
}

#[tokio::test]
async fn the_sqlite_store_survives_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mob_announcements.sqlite3");
    {
        let store = SqliteAnnouncementStore::open(&path).unwrap();
        store
            .put_subscription(
                &op("o1"),
                put("s1", "agent:a", &["ios"], StartFrontier::Now),
                1,
            )
            .await
            .unwrap();
        store
            .announce(announce_request("payments", "k1", &["ios"], "x"), 2)
            .await
            .unwrap();
        store
            .seal_batch(&member("agent:a"), &batch_id("b1"), &policy(), None, 2_000)
            .await
            .unwrap()
            .unwrap();
        store
            .transition_batch(
                &batch_id("b1"),
                BatchStateTag::Sealed,
                BatchEvent::BeginSubmit,
                3,
            )
            .await
            .unwrap();
    }
    let reopened = SqliteAnnouncementStore::open(&path).unwrap();
    let unsettled = reopened.unsettled_batches(None, 10).await.unwrap();
    assert_eq!(unsettled.len(), 1);
    assert_eq!(unsettled[0].state, BatchState::Submitting { attempt: 1 });
    assert_eq!(
        reopened
            .put_subscription(
                &op("o1"),
                put("s1", "agent:a", &["ios"], StartFrontier::Now),
                9
            )
            .await
            .unwrap()
            .revision,
        1,
        "operation receipts survive the restart"
    );
}

#[tokio::test]
async fn the_ledgered_disk_store_opens_twice() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mob_announcements.sqlite3");
    {
        let store = super::storage::open_disk_store(path.clone()).unwrap();
        store
            .announce(announce_request("payments", "k1", &["ios"], "x"), 1)
            .await
            .unwrap();
    }
    let store = super::storage::open_disk_store(path).unwrap();
    assert!(
        store
            .announcement(&mob("payments"), &SourceKey::new("k1").unwrap())
            .await
            .unwrap()
            .is_some()
    );
}

// ---------------------------------------------------------------------------
// Owner behaviour against a scripted native owner
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Ledger {
    /// key -> (exact delivery, receipt, applied)
    bindings: BTreeMap<String, (String, NativeReceipt, bool)>,
    submits: u64,
    lose_next_ack: bool,
    refuse_owner_retired: bool,
    /// Every submission meets no live incarnation until this is cleared.
    retired: bool,
    strand: BTreeSet<String>,
    /// Native status reads, for pacing checks.
    status_reads: u64,
}

struct ScriptedNative {
    ledger: Mutex<Ledger>,
    governed: bool,
    known: BTreeSet<String>,
    /// Registered identities this gateway's runtime does not serve.
    unusable: BTreeSet<String>,
    /// Peers this gateway can call authenticated.
    peers: BTreeSet<String>,
    /// Mobs the mob owner records as destroyed.
    destroyed: Mutex<BTreeSet<String>>,
    /// Members whose owner reports them gone for good, and why.
    gone: Mutex<BTreeMap<String, SubscriptionEnd>>,
    /// The next submission's final answer that its member is gone.
    submit_gone: Mutex<Option<SubscriptionEnd>>,
    apply_on_commit: bool,
    /// What Meerkat's records say about the mobs members created, by mob.
    origins: Mutex<BTreeMap<String, MobOriginFacts>>,
    /// Mobs whose origin cannot be read now.
    unreadable_origins: Mutex<BTreeSet<String>>,
    /// Whether every hosted mob has been restored.
    restored: std::sync::atomic::AtomicBool,
}

impl ScriptedNative {
    fn new() -> Self {
        Self {
            ledger: Mutex::new(Ledger::default()),
            governed: false,
            known: ["agent:a".to_string()].into(),
            unusable: BTreeSet::new(),
            peers: ["mob-b".to_string()].into(),
            destroyed: Mutex::new(BTreeSet::new()),
            gone: Mutex::new(BTreeMap::new()),
            submit_gone: Mutex::new(None),
            apply_on_commit: true,
            origins: Mutex::new(BTreeMap::new()),
            unreadable_origins: Mutex::new(BTreeSet::new()),
            restored: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Script the origin Meerkat's records give the mob `mob_id`.
    fn set_origin(&self, mob_id: &str, incarnation: MobIncarnation, origin: MobOrigin) {
        self.origins.lock().unwrap().insert(
            mob_id.to_string(),
            MobOriginFacts {
                incarnation,
                origin,
            },
        );
    }

    /// That a member of `parent` created a mob, under the parent's scripted
    /// incarnation, or of this gateway's own mob, which has none.
    fn created_by(&self, parent: &str, creator: &str) -> MobOrigin {
        let parent_incarnation = (parent != OWN_MOB).then(|| {
            self.origins
                .lock()
                .unwrap()
                .get(parent)
                .map(|facts| facts.incarnation.clone())
                .expect("the parent's origin is scripted first")
        });
        MobOrigin::Member {
            parent: mob(parent),
            creator: creator.to_string(),
            parent_incarnation,
        }
    }

    fn hosted(&self, mob_id: &MobId) -> bool {
        TEST_MOBS.contains(&mob_id.as_str())
            && !self.destroyed.lock().unwrap().contains(mob_id.as_str())
    }

    fn submits(&self) -> u64 {
        self.ledger.lock().unwrap().submits
    }

    fn bound(&self, key: &str) -> Option<String> {
        self.ledger
            .lock()
            .unwrap()
            .bindings
            .get(key)
            .map(|(delivery, _, _)| delivery.clone())
    }
}

#[async_trait]
impl NativeDelivery for ScriptedNative {
    fn governs_work_authority(&self) -> std::result::Result<bool, NativeUnavailable> {
        Ok(self.governed)
    }

    fn route(&self, member: &Member) -> MemberRoute {
        if member.mob_id.as_str() == OWN_MOB {
            MemberRoute::Local
        } else {
            MemberRoute::Peer
        }
    }

    async fn mob_presence(
        &self,
        mobs: &BTreeSet<MobId>,
    ) -> std::result::Result<BTreeMap<MobId, MobPresence>, NativeUnavailable> {
        Ok(mobs
            .iter()
            .map(|mob_id| {
                let presence = if self.hosted(mob_id) {
                    MobPresence::Hosted
                } else {
                    MobPresence::Gone
                };
                (mob_id.clone(), presence)
            })
            .collect())
    }

    async fn hosted_mob_ids(&self) -> std::result::Result<BTreeSet<MobId>, NativeUnavailable> {
        Ok(TEST_MOBS
            .iter()
            .filter(|mob_id| **mob_id != OWN_MOB)
            .map(|mob_id| mob(mob_id))
            .filter(|mob_id| self.hosted(mob_id))
            .collect())
    }

    fn mobs_restored(&self) -> bool {
        self.restored.load(Ordering::SeqCst)
    }

    fn own_mob(&self) -> Option<MobId> {
        Some(mob(OWN_MOB))
    }

    async fn mob_origin(
        &self,
        mob_id: &MobId,
    ) -> std::result::Result<Option<MobOriginFacts>, NativeUnavailable> {
        if self
            .unreadable_origins
            .lock()
            .unwrap()
            .contains(mob_id.as_str())
        {
            return Err(NativeUnavailable(
                "the session store is not reachable".into(),
            ));
        }
        if !self.hosted(mob_id) {
            return Ok(None);
        }
        Ok(self.origins.lock().unwrap().get(mob_id.as_str()).cloned())
    }

    async fn check_member(&self, member: &Member) -> std::result::Result<(), MemberProblem> {
        let identity = member.identity.as_str();
        match self.route(member) {
            MemberRoute::Local if self.unusable.contains(identity) => Err(MemberProblem::Unusable(
                format!("{identity} is placed on a host"),
            )),
            MemberRoute::Local if self.known.contains(identity) => Ok(()),
            MemberRoute::Local => Err(MemberProblem::Unknown),
            MemberRoute::Peer if self.hosted(&member.mob_id) => Err(MemberProblem::Unsupported(
                format!("{} is hosted here", member.mob_id),
            )),
            MemberRoute::Peer if self.peers.contains(member.mob_id.as_str()) => Ok(()),
            MemberRoute::Peer => Err(MemberProblem::UnusablePeer(format!(
                "no pinned contact for {}",
                member.mob_id
            ))),
        }
    }

    async fn prepare_member(&self, member: &Member) -> std::result::Result<(), PrepareError> {
        match self.gone.lock().unwrap().get(member.identity.as_str()) {
            Some(end) => Err(PrepareError::Gone(end.clone())),
            None => Ok(()),
        }
    }

    async fn submit(
        &self,
        batch: &Batch,
        committed_at_ms: u64,
    ) -> std::result::Result<NativeReceipt, NativeSubmitError> {
        let mut ledger = self.ledger.lock().unwrap();
        ledger.submits += 1;
        if let Some(end) = self.submit_gone.lock().unwrap().take() {
            return Err(NativeSubmitError::MemberGone(end));
        }
        if ledger.refuse_owner_retired || ledger.retired {
            ledger.refuse_owner_retired = false;
            return Err(NativeSubmitError::OwnerRetired);
        }
        let exact = serde_json::to_string(&continuation_for(batch).unwrap()).unwrap();
        let key = batch.id.to_string();
        if let Some((bound, receipt, _)) = ledger.bindings.get(&key) {
            if *bound == exact {
                return Ok(receipt.clone());
            }
            return Err(NativeSubmitError::Conflict(format!(
                "the native key {key} is bound to other content"
            )));
        }
        let receipt = NativeReceipt {
            delivery_id: format!("continuation:{key}"),
            address: "rt:member:test:agent:1".into(),
            submission_digest: format!("digest:{key}"),
            committed_at_ms,
        };
        ledger
            .bindings
            .insert(key, (exact, receipt.clone(), self.apply_on_commit));
        if ledger.lose_next_ack {
            ledger.lose_next_ack = false;
            return Err(NativeSubmitError::Unavailable("the answer was lost".into()));
        }
        Ok(receipt)
    }

    async fn status(&self, batch: &Batch) -> std::result::Result<NativeStatus, NativeUnavailable> {
        let mut ledger = self.ledger.lock().unwrap();
        ledger.status_reads += 1;
        let Some((_, receipt, applied)) = ledger.bindings.get(batch.id.as_str()) else {
            return Ok(NativeStatus::NotCommitted);
        };
        if ledger.strand.contains(batch.id.as_str()) {
            return Ok(NativeStatus::Stranded {
                receipt: receipt.clone(),
            });
        }
        Ok(if *applied {
            NativeStatus::Applied {
                receipt: receipt.clone(),
                session_id: "session-1".into(),
                input_id: "input-1".into(),
            }
        } else {
            NativeStatus::Pending {
                receipt: receipt.clone(),
                admitted: None,
            }
        })
    }

    async fn input(
        &self,
        _: &Batch,
        _: &str,
        _: &str,
    ) -> std::result::Result<Option<serde_json::Value>, NativeUnavailable> {
        Ok(Some(serde_json::json!({ "phase": "consumed" })))
    }
}

struct Harness {
    _dir: tempfile::TempDir,
    store: Arc<SqliteAnnouncementStore>,
    native: Arc<ScriptedNative>,
    clock: Arc<AtomicU64>,
    service: AnnouncementService,
}

fn harness_with(native: ScriptedNative) -> Harness {
    let (dir, store) = temp_store();
    let store = Arc::new(store);
    let native = Arc::new(native);
    let clock = Arc::new(AtomicU64::new(1_000));
    let reader = Arc::clone(&clock);
    let config = AnnouncementConfig {
        policy: SealingPolicy {
            max_count: 256,
            max_bytes: 1_048_576,
            max_delay_ms: 60_000,
            min_wake_interval_ms: 60_000,
        },
        ..AnnouncementConfig::default()
    };
    let service = AnnouncementService::with_clock(
        Arc::clone(&store) as Arc<dyn AnnouncementStore>,
        Arc::clone(&native) as Arc<dyn NativeDelivery>,
        config,
        Arc::new(move || reader.load(Ordering::SeqCst)),
    )
    .unwrap();
    Harness {
        _dir: dir,
        store,
        native,
        clock,
        service,
    }
}

impl Harness {
    /// A round once every batch the last one read is due to be read again.
    async fn round_after_pause(&self) -> RoundReport {
        self.clock.fetch_add(
            AnnouncementConfig::default().settle_check_max_ms + 1,
            Ordering::SeqCst,
        );
        self.service.run_round().await.unwrap()
    }

    async fn subscribe(&self, id: &str) -> Subscription {
        self.service
            .put_subscription(&op(id), put(id, "agent:a", &["ios"], StartFrontier::Now))
            .await
            .unwrap()
    }

    async fn announce_burst(&self, count: usize) {
        for index in 0..count {
            self.service
                .announce(announce_request(
                    "payments",
                    &format!("failure-{index}"),
                    &["ios"],
                    &format!("checkout failed for order {index}"),
                ))
                .await
                .unwrap();
        }
    }

    async fn only_batch(&self) -> Batch {
        let mut batches = self
            .service
            .deliveries(&BatchQuery {
                member: Some(member("agent:a")),
                limit: 10,
                ..BatchQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(batches.len(), 1, "exactly one batch: {batches:?}");
        batches.remove(0)
    }
}

#[tokio::test]
async fn a_burst_while_paused_is_delivered_once_on_resume() {
    let harness = harness_with(ScriptedNative::new());
    let subscription = harness.subscribe("s1").await;
    let paused = harness
        .service
        .change_subscription(
            &op("p"),
            &sid("s1"),
            subscription.revision,
            SubscriptionChange::Pause,
        )
        .await
        .unwrap();
    harness.announce_burst(200).await;
    harness.clock.store(10_000_000, Ordering::SeqCst);
    let report = harness.service.run_round().await.unwrap();
    assert_eq!(report.sealed, 0, "nothing is delivered while paused");
    assert_eq!(harness.native.submits(), 0);

    harness
        .service
        .change_subscription(
            &op("r"),
            &sid("s1"),
            paused.subscription.revision,
            SubscriptionChange::Resume,
        )
        .await
        .unwrap();
    let report = harness.service.run_round().await.unwrap();
    assert_eq!(report.sealed, 1, "the whole burst is one batch");
    assert_eq!(harness.native.submits(), 1, "one native submission");
    let batch = harness.only_batch().await;
    assert_eq!(batch.items.len(), 200);
    assert_eq!(batch.state.tag(), BatchStateTag::Delivered);
    let observed = harness.service.observe_delivery(&batch.id).await.unwrap();
    assert!(matches!(observed.native, NativeObservation::Applied { .. }));
    assert!(
        observed.input.is_some(),
        "the admitted input's own state is reported"
    );

    let again = harness.service.run_round().await.unwrap();
    assert_eq!(again.sealed, 0);
    assert_eq!(harness.native.submits(), 1, "nothing is delivered twice");
}

#[tokio::test]
async fn a_lost_acknowledgment_is_reconciled_without_a_second_submission() {
    let mut native = ScriptedNative::new();
    native.apply_on_commit = false;
    native.ledger.get_mut().unwrap().lose_next_ack = true;
    let harness = harness_with(native);
    harness.subscribe("s1").await;
    harness.announce_burst(3).await;
    harness.clock.store(10_000_000, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let batch = harness.only_batch().await;
    assert_eq!(
        batch.state,
        BatchState::Submitting { attempt: 1 },
        "an unknown outcome is never recorded as success or failure"
    );
    assert_eq!(harness.native.submits(), 1);

    // The next round reads the native status instead of submitting again.
    harness.round_after_pause().await;
    let batch = harness.only_batch().await;
    assert_eq!(batch.state.tag(), BatchStateTag::Committed);
    assert_eq!(
        harness.native.submits(),
        1,
        "the native commit was found, not repeated"
    );
    let observed = harness.service.observe_delivery(&batch.id).await.unwrap();
    assert!(
        matches!(observed.native, NativeObservation::Pending { .. }),
        "committed but not yet admitted is reported as such: {:?}",
        observed.native
    );
}

#[tokio::test]
async fn an_attempt_that_never_reached_the_native_owner_is_resubmitted_identically() {
    let harness = harness_with(ScriptedNative::new());
    harness.subscribe("s1").await;
    harness.announce_burst(2).await;
    let id = batch_id("b-crash");
    harness
        .store
        .seal_batch(
            &member("agent:a"),
            &id,
            &harness.service.config().policy,
            None,
            10_000_000,
        )
        .await
        .unwrap()
        .unwrap();
    // The intent was durable when the process died; nothing was submitted.
    harness
        .store
        .transition_batch(
            &id,
            BatchStateTag::Sealed,
            BatchEvent::BeginSubmit,
            10_000_001,
        )
        .await
        .unwrap();
    harness.clock.store(10_000_002, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let batch = harness.store.batch(&id).await.unwrap().unwrap();
    assert_eq!(batch.state.tag(), BatchStateTag::Delivered);
    assert_eq!(harness.native.submits(), 1);
    let expected =
        serde_json::to_string(&super::native::continuation_for(&batch).unwrap()).unwrap();
    assert_eq!(harness.native.bound(id.as_str()), Some(expected));
}

#[tokio::test]
async fn a_stranded_delivery_moves_to_a_successor_with_lineage() {
    let mut native = ScriptedNative::new();
    native.apply_on_commit = false;
    let harness = harness_with(native);
    harness.subscribe("s1").await;
    harness.announce_burst(1).await;
    harness.clock.store(10_000_000, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let first = harness.only_batch().await;
    assert_eq!(first.state.tag(), BatchStateTag::Committed);
    harness
        .native
        .ledger
        .lock()
        .unwrap()
        .strand
        .insert(first.id.to_string());
    harness.round_after_pause().await;
    let stranded = harness.store.batch(&first.id).await.unwrap().unwrap();
    assert_eq!(stranded.state.tag(), BatchStateTag::Stranded);
    let successor_id = stranded
        .successor
        .clone()
        .expect("a successor carries the announcements");
    let successor = harness.store.batch(&successor_id).await.unwrap().unwrap();
    assert_eq!(successor.predecessor, Some(first.id.clone()));
    assert_eq!(successor.items, first.items);
    assert!(
        successor.body.contains(first.id.as_str()),
        "the redelivery names its lineage"
    );
    assert_eq!(successor.state.tag(), BatchStateTag::Committed);
    assert_eq!(
        harness.native.submits(),
        2,
        "the successor is a new native key, never a re-key"
    );
}

#[tokio::test]
async fn a_member_without_an_incarnation_keeps_its_batch_owed() {
    let native = ScriptedNative::new();
    native.ledger.lock().unwrap().refuse_owner_retired = true;
    let harness = harness_with(native);
    harness.subscribe("s1").await;
    harness.announce_burst(1).await;
    harness.clock.store(10_000_000, Ordering::SeqCst);
    let report = harness.service.run_round().await.unwrap();
    assert!(
        report.next_due_ms.is_some(),
        "a refused attempt is looked at again"
    );
    let batch = harness.only_batch().await;
    assert_eq!(batch.state, BatchState::Submitting { attempt: 1 });
    harness.clock.store(10_100_000, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let batch = harness.only_batch().await;
    assert_eq!(batch.state.tag(), BatchStateTag::Delivered);
    assert_eq!(harness.native.submits(), 2);
}

#[tokio::test]
async fn contradicted_native_evidence_blocks_a_committed_batch() {
    let mut native = ScriptedNative::new();
    native.apply_on_commit = false;
    let harness = harness_with(native);
    harness.subscribe("s1").await;
    harness.announce_burst(1).await;
    harness.clock.store(10_000_000, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let batch = harness.only_batch().await;
    assert_eq!(batch.state.tag(), BatchStateTag::Committed);
    // The native owner no longer holds the commit the receipt names.
    harness
        .native
        .ledger
        .lock()
        .unwrap()
        .bindings
        .remove(batch.id.as_str());
    harness.round_after_pause().await;
    let batch = harness.only_batch().await;
    assert_eq!(
        batch.state.tag(),
        BatchStateTag::Blocked,
        "contradicted evidence settles the batch for operators, never an error loop"
    );
    assert_eq!(harness.native.submits(), 1, "nothing is resubmitted blind");
}

#[tokio::test]
async fn a_committed_batch_is_blocked_by_evidence_about_another_delivery() {
    for applied in [false, true] {
        let mut native = ScriptedNative::new();
        native.apply_on_commit = false;
        let harness = harness_with(native);
        harness.subscribe("s1").await;
        harness.announce_burst(1).await;
        harness.clock.store(10_000_000, Ordering::SeqCst);
        harness.service.run_round().await.unwrap();
        let batch = harness.only_batch().await;
        assert_eq!(batch.state.tag(), BatchStateTag::Committed);
        // The native owner now reports a delivery other than the one this
        // batch's receipt names, pending or applied.
        {
            let mut ledger = harness.native.ledger.lock().unwrap();
            let binding = ledger.bindings.get_mut(batch.id.as_str()).unwrap();
            binding.1.delivery_id = "continuation:another-delivery".into();
            binding.1.submission_digest = "digest:another-delivery".into();
            binding.2 = applied;
        }
        harness.round_after_pause().await;
        let batch = harness.only_batch().await;
        assert_eq!(
            batch.state.tag(),
            BatchStateTag::Blocked,
            "applied={applied}: another delivery never settles this one"
        );
        assert_eq!(harness.native.submits(), 1, "nothing is resubmitted blind");
    }
}

#[test]
fn a_committed_batch_settles_only_from_its_own_receipt() {
    let committed = NativeReceipt {
        delivery_id: "continuation:mine".into(),
        address: "rt:member:test:agent:1".into(),
        submission_digest: "digest:mine".into(),
        committed_at_ms: 1,
    };
    let state = BatchState::Committed {
        receipt: committed.clone(),
    };
    let other_id = NativeReceipt {
        delivery_id: "continuation:theirs".into(),
        ..committed.clone()
    };
    let other_digest = NativeReceipt {
        submission_digest: "digest:theirs".into(),
        ..committed.clone()
    };
    let events = |receipt: &NativeReceipt| {
        vec![
            BatchEvent::NativeApplied {
                receipt: receipt.clone(),
                session_id: "session-1".into(),
                input_id: "input-1".into(),
            },
            BatchEvent::NativeStranded {
                receipt: receipt.clone(),
                successor: None,
            },
            BatchEvent::NativeRefused {
                reason: "refused".into(),
                receipt: Some(receipt.clone()),
            },
        ]
    };
    for other in [&other_id, &other_digest] {
        assert!(state.other_delivery(other).is_some());
        for event in events(other) {
            assert_eq!(
                state.apply(&event).unwrap().tag(),
                BatchStateTag::Blocked,
                "{event:?}"
            );
        }
    }
    assert!(state.other_delivery(&committed).is_none());
    let settled: Vec<_> = events(&committed)
        .iter()
        .map(|event| state.apply(event).unwrap().tag())
        .collect();
    assert_eq!(
        settled,
        [
            BatchStateTag::Delivered,
            BatchStateTag::Stranded,
            BatchStateTag::Refused
        ]
    );
    // A submitting batch has no receipt yet: native evidence settles it.
    assert!(
        BatchState::Submitting { attempt: 1 }
            .other_delivery(&other_id)
            .is_none()
    );
}

/// A round's worth of members whose batches the native owner commits but
/// does not admit (a long turn), each with newer announcements waiting
/// behind its batch, fill a whole round. One more member's announcement is
/// still sealed, and the dispatcher still comes back to its batch.
#[tokio::test]
async fn a_round_full_of_held_members_does_not_starve_the_next_one() {
    const HELD: usize = super::owner::ROUND_BATCHES;
    // One round seals every held member.
    const { assert!(HELD <= super::owner::ROUND_MEMBERS) };
    let mut native = ScriptedNative::new();
    native.apply_on_commit = false;
    native.known = (0..=HELD)
        .map(|index| format!("agent:r{index:02}"))
        .collect();
    let harness = harness_with(native);
    for index in 0..=HELD {
        let topic = format!("t{index:02}");
        harness
            .service
            .put_subscription(
                &op(&format!("o{index:02}")),
                put(
                    &format!("s{index:02}"),
                    &format!("agent:r{index:02}"),
                    &[topic.as_str()],
                    StartFrontier::Now,
                ),
            )
            .await
            .unwrap();
    }
    for index in 0..HELD {
        let topic = format!("t{index:02}");
        harness
            .service
            .announce(announce_request(
                "events",
                &format!("first-{index:02}"),
                &[topic.as_str()],
                "first",
            ))
            .await
            .unwrap();
    }
    harness.clock.store(100_000, Ordering::SeqCst);
    let report = harness.service.run_round().await.unwrap();
    assert_eq!(
        report.sealed as usize, HELD,
        "each held member has its batch"
    );
    for index in 0..HELD {
        let topic = format!("t{index:02}");
        harness
            .service
            .announce(announce_request(
                "events",
                &format!("second-{index:02}"),
                &[topic.as_str()],
                "second",
            ))
            .await
            .unwrap();
    }
    harness.clock.store(200_000, Ordering::SeqCst);
    harness
        .service
        .announce(announce_request(
            "events",
            "last",
            &[format!("t{HELD:02}").as_str()],
            "for the last member",
        ))
        .await
        .unwrap();
    harness.clock.store(300_000, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let last = BatchQuery {
        member: Some(member(&format!("agent:r{HELD:02}"))),
        limit: 10,
        ..BatchQuery::default()
    };
    let sealed = harness.service.deliveries(&last).await.unwrap();
    assert_eq!(
        sealed.len(),
        1,
        "the last member's announcement is sealed although a full round of held members is \
         ahead of it"
    );

    // The native owner admits the 65th member's batch; a later round
    // reads that evidence.
    harness
        .native
        .ledger
        .lock()
        .unwrap()
        .bindings
        .get_mut(sealed[0].id.as_str())
        .expect("the batch was committed")
        .2 = true;
    for round in 1..=3_u64 {
        harness
            .clock
            .store(300_000 + round * 100_000, Ordering::SeqCst);
        harness.service.run_round().await.unwrap();
    }
    let settled = harness.service.deliveries(&last).await.unwrap();
    assert_eq!(
        settled[0].state.tag(),
        BatchStateTag::Delivered,
        "the dispatcher comes back to the last member's batch"
    );
    assert_eq!(
        harness.native.submits() as usize,
        HELD + 1,
        "each batch is submitted once; the held ones are only read"
    );
}

/// The SQLite store with injected faults: batch transitions fail for one
/// member, as a storage fault would, and the first retention passes
/// report more work, as a long backlog would. It counts what it is asked.
struct FaultyStore {
    inner: SqliteAnnouncementStore,
    failing: Option<Member>,
    failures: AtomicU64,
    /// Retention passes left that report more work.
    more_passes: AtomicU64,
    prune_passes: AtomicU64,
    /// Backlog reads: one per dispatcher round.
    backlog_reads: AtomicU64,
    /// When each retention pass started.
    pass_times: Mutex<Vec<std::time::Instant>>,
    /// Panics the next backlog read, as a fault inside a round would.
    panic_next_round: std::sync::atomic::AtomicBool,
}

impl FaultyStore {
    fn new(inner: SqliteAnnouncementStore, failing: Option<&str>) -> Self {
        Self {
            inner,
            failing: failing.map(member),
            failures: AtomicU64::new(0),
            more_passes: AtomicU64::new(0),
            prune_passes: AtomicU64::new(0),
            backlog_reads: AtomicU64::new(0),
            pass_times: Mutex::new(Vec::new()),
            panic_next_round: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl AnnouncementStore for FaultyStore {
    async fn announce(&self, request: AnnounceRequest, now_ms: u64) -> Result<AnnounceOutcome> {
        self.inner.announce(request, now_ms).await
    }

    async fn announcement(
        &self,
        mob_id: &MobId,
        source_key: &SourceKey,
    ) -> Result<Option<Announcement>> {
        self.inner.announcement(mob_id, source_key).await
    }

    async fn put_subscription(
        &self,
        operation: &OperationId,
        request: PutSubscription,
        now_ms: u64,
    ) -> Result<Subscription> {
        self.inner
            .put_subscription(operation, request, now_ms)
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
        self.inner
            .change_subscription(operation, id, expected_revision, change, now_ms)
            .await
    }

    async fn subscription(&self, id: &SubscriptionId) -> Result<Option<Subscription>> {
        self.inner.subscription(id).await
    }

    async fn subscriptions(
        &self,
        member: Option<&Member>,
        after: Option<&SubscriptionId>,
        limit: usize,
    ) -> Result<Vec<Subscription>> {
        self.inner.subscriptions(member, after, limit).await
    }

    async fn end_member(
        &self,
        member: &Member,
        end: SubscriptionEnd,
        now_ms: u64,
    ) -> Result<Vec<SubscriptionId>> {
        self.inner.end_member(member, end, now_ms).await
    }

    async fn known_mobs(&self) -> Result<BTreeSet<MobId>> {
        self.inner.known_mobs().await
    }

    async fn drop_ended_mobs(
        &self,
        ended: &BTreeSet<MobId>,
        now_ms: u64,
    ) -> Result<Vec<SubscriptionId>> {
        self.inner.drop_ended_mobs(ended, now_ms).await
    }

    async fn lineage(&self) -> Result<Vec<MobLineage>> {
        self.inner.lineage().await
    }

    async fn record_lineage(&self, lineage: &MobLineage, now_ms: u64) -> Result<bool> {
        self.inner.record_lineage(lineage, now_ms).await
    }

    async fn end_lineage(&self, mobs: &BTreeSet<MobId>, now_ms: u64) -> Result<()> {
        self.inner.end_lineage(mobs, now_ms).await
    }

    async fn backlogs(&self, limit: usize) -> Result<Vec<MemberBacklog>> {
        self.backlog_reads.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.panic_next_round.swap(false, Ordering::SeqCst),
            "injected fault inside a dispatcher round"
        );
        self.inner.backlogs(limit).await
    }

    async fn seal_batch(
        &self,
        member: &Member,
        batch_id: &BatchId,
        policy: &SealingPolicy,
        wire_budget: Option<u64>,
        now_ms: u64,
    ) -> Result<Option<Batch>> {
        self.inner
            .seal_batch(member, batch_id, policy, wire_budget, now_ms)
            .await
    }

    async fn batch(&self, id: &BatchId) -> Result<Option<Batch>> {
        self.inner.batch(id).await
    }

    async fn batches(&self, query: &BatchQuery) -> Result<Vec<Batch>> {
        self.inner.batches(query).await
    }

    async fn unsettled_batches(
        &self,
        after: Option<&BatchPosition>,
        limit: usize,
    ) -> Result<Vec<Batch>> {
        self.inner.unsettled_batches(after, limit).await
    }

    async fn newest_unsettled_batch(&self) -> Result<Option<BatchPosition>> {
        self.inner.newest_unsettled_batch().await
    }

    async fn transition_batch(
        &self,
        id: &BatchId,
        expected: BatchStateTag,
        event: BatchEvent,
        now_ms: u64,
    ) -> Result<Batch> {
        if self
            .inner
            .batch(id)
            .await?
            .is_some_and(|batch| self.failing.as_ref() == Some(&batch.member))
        {
            self.failures.fetch_add(1, Ordering::SeqCst);
            return Err(AnnouncementStoreError::Storage(
                "injected transition failure".into(),
            ));
        }
        self.inner
            .transition_batch(id, expected, event, now_ms)
            .await
    }

    async fn expire(&self, now_ms: u64, retention_ms: u64, limit: usize) -> Result<ExpiryReport> {
        self.inner.expire(now_ms, retention_ms, limit).await
    }

    async fn prune(&self, now_ms: u64, retention_ms: u64, limit: usize) -> Result<PruneReport> {
        self.pass_times
            .lock()
            .unwrap()
            .push(std::time::Instant::now());
        self.prune_passes.fetch_add(1, Ordering::SeqCst);
        let mut report = self.inner.prune(now_ms, retention_ms, limit).await?;
        if self
            .more_passes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            report.more = true;
        }
        Ok(report)
    }
}

/// A batch the round cannot advance (here, its transitions fail in the
/// store) is reported and skipped; it does not end the round for every
/// other member.
#[tokio::test]
async fn one_failing_batch_does_not_end_the_round() {
    let (dir, inner) = temp_store();
    let store = Arc::new(FaultyStore::new(inner, Some("agent:a")));
    let mut native = ScriptedNative::new();
    native.known = ["agent:a".to_string(), "agent:b".to_string()].into();
    let native = Arc::new(native);
    let clock = Arc::new(AtomicU64::new(1_000));
    let reader = Arc::clone(&clock);
    let service = AnnouncementService::with_clock(
        Arc::clone(&store) as Arc<dyn AnnouncementStore>,
        Arc::clone(&native) as Arc<dyn NativeDelivery>,
        AnnouncementConfig {
            policy: policy(),
            ..AnnouncementConfig::default()
        },
        Arc::new(move || reader.load(Ordering::SeqCst)),
    )
    .unwrap();
    for (index, agent) in ["agent:a", "agent:b"].iter().enumerate() {
        let topic = format!("t{index}");
        service
            .put_subscription(
                &op(&format!("o{index}")),
                put(
                    &format!("s{index}"),
                    agent,
                    &[topic.as_str()],
                    StartFrontier::Now,
                ),
            )
            .await
            .unwrap();
        service
            .announce(announce_request(
                "events",
                &format!("k{index}"),
                &[topic.as_str()],
                "x",
            ))
            .await
            .unwrap();
    }
    clock.store(100_000, Ordering::SeqCst);
    let report = service
        .run_round()
        .await
        .expect("a batch that cannot be advanced does not end the round");
    assert_eq!(report.sealed, 2, "both backlogs are sealed");
    let delivered = service
        .deliveries(&BatchQuery {
            member: Some(member("agent:b")),
            limit: 10,
            ..BatchQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(
        delivered[0].state.tag(),
        BatchStateTag::Delivered,
        "the other member's batch is delivered in the same round"
    );

    // The next round walks the failing batch first; it still seals and
    // delivers the other member's new announcement.
    service
        .announce(announce_request("events", "k1-more", &["t1"], "y"))
        .await
        .unwrap();
    clock.store(200_000, Ordering::SeqCst);
    let report = service
        .run_round()
        .await
        .expect("a batch the walk cannot advance does not end the round");
    assert_eq!(report.sealed, 1, "the other member's new backlog is sealed");
    let delivered = service
        .deliveries(&BatchQuery {
            member: Some(member("agent:b")),
            limit: 10,
            ..BatchQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(delivered.len(), 2);
    assert!(
        delivered
            .iter()
            .all(|batch| batch.state.tag() == BatchStateTag::Delivered),
        "{delivered:?}"
    );
    drop(dir);
}

/// More than one pass of retention work, most of it in the mob_id that
/// sorts first: the dispatcher keeps passing until everything the window
/// allows is gone, in both mobs, without waiting for the next interval.
#[tokio::test]
async fn retention_keeps_passing_while_a_pass_hits_its_limit() {
    let harness = harness_with(ScriptedNative::new());
    for index in 0..1_200 {
        harness
            .service
            .announce(announce_request(
                "a-mob",
                &format!("a-{index}"),
                &["ios"],
                "old",
            ))
            .await
            .unwrap();
    }
    for index in 0..5 {
        harness
            .service
            .announce(announce_request(
                "b-mob",
                &format!("b-{index}"),
                &["ios"],
                "old",
            ))
            .await
            .unwrap();
    }
    harness.clock.store(
        1_000 + super::owner::DEFAULT_RETENTION_MS + 1,
        Ordering::SeqCst,
    );
    let gone = |mob_id: &'static str, key: &'static str| {
        let store = Arc::clone(&harness.store);
        async move {
            store
                .announcement(&mob(mob_id), &SourceKey::new(key).unwrap())
                .await
                .unwrap()
                .is_none()
        }
    };
    let dispatcher = harness.service.start();
    let caught_up = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while !(gone("a-mob", "a-1199").await && gone("b-mob", "b-4").await) {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    harness.service.stop();
    dispatcher.await.unwrap();
    assert!(
        caught_up.is_ok(),
        "liveness: retention passes repeat while a pass hits its limit, so the \
         backlog in both mobs is gone without waiting an interval per pass"
    );
}

#[test]
fn a_retention_window_below_the_floor_is_refused() {
    let options =
        |retention_ms: u64| serde_json::json!({ "enabled": true, "retention_ms": retention_ms });
    assert!(super::parse_runtime_options(&options(0)).is_err());
    assert!(super::parse_runtime_options(&options(3_599_999)).is_err());
    assert!(super::parse_runtime_options(&options(3_600_000)).is_ok());
    let (_dir, store) = temp_store();
    let refused = AnnouncementService::new(
        Arc::new(store) as Arc<dyn AnnouncementStore>,
        Arc::new(ScriptedNative::new()) as Arc<dyn NativeDelivery>,
        AnnouncementConfig {
            retention_ms: 0,
            ..AnnouncementConfig::default()
        },
    );
    assert!(
        refused.is_err(),
        "a window that prunes receipts a retry still needs is refused"
    );
}

/// A batch the native owner has committed but not admitted is read again
/// only once its retry time has passed, not in every round.
#[tokio::test]
async fn a_batch_is_not_read_again_before_its_retry_time() {
    let mut native = ScriptedNative::new();
    native.apply_on_commit = false;
    let harness = harness_with(native);
    harness.subscribe("s1").await;
    harness.announce_burst(1).await;
    harness.clock.store(100_000, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let reads = || harness.native.ledger.lock().unwrap().status_reads;
    assert_eq!(reads(), 1, "the batch is read once after it commits");
    harness.service.run_round().await.unwrap();
    assert_eq!(
        reads(),
        1,
        "a round before the batch's retry time does not read it again"
    );
    harness.clock.store(
        100_000 + AnnouncementConfig::default().settle_check_max_ms + 1,
        Ordering::SeqCst,
    );
    harness.service.run_round().await.unwrap();
    assert_eq!(reads(), 2, "after its retry time it is read again");
}

/// A batch whose transitions keep failing is retried at the slowest settle
/// pace, not in every round.
#[tokio::test]
async fn a_failing_batch_is_retried_at_the_slow_pace() {
    let (dir, inner) = temp_store();
    let store = Arc::new(FaultyStore::new(inner, Some("agent:a")));
    let clock = Arc::new(AtomicU64::new(1_000));
    let reader = Arc::clone(&clock);
    let config = AnnouncementConfig {
        policy: policy(),
        ..AnnouncementConfig::default()
    };
    let service = AnnouncementService::with_clock(
        Arc::clone(&store) as Arc<dyn AnnouncementStore>,
        Arc::new(ScriptedNative::new()) as Arc<dyn NativeDelivery>,
        config,
        Arc::new(move || reader.load(Ordering::SeqCst)),
    )
    .unwrap();
    service
        .put_subscription(
            &op("o1"),
            put("s1", "agent:a", &["ios"], StartFrontier::Now),
        )
        .await
        .unwrap();
    service
        .announce(announce_request("payments", "k1", &["ios"], "x"))
        .await
        .unwrap();
    clock.store(100_000, Ordering::SeqCst);
    let first = service.run_round().await.unwrap();
    assert_eq!(store.failures.load(Ordering::SeqCst), 1);
    assert!(
        first
            .next_due_ms
            .is_some_and(|due| due >= 100_000 + config.settle_check_max_ms),
        "a failure is retried at the slowest pace: {first:?}"
    );
    clock.store(100_000 + config.settle_check_min_ms, Ordering::SeqCst);
    service.run_round().await.unwrap();
    assert_eq!(
        store.failures.load(Ordering::SeqCst),
        1,
        "a round before the retry time leaves the failing batch alone"
    );
    clock.store(100_000 + config.settle_check_max_ms + 1, Ordering::SeqCst);
    service.run_round().await.unwrap();
    assert_eq!(store.failures.load(Ordering::SeqCst), 2);
    drop(dir);
}

/// Every round seals 64 new batches that stay unsettled, so the walk's
/// windows stay full and never wrap on their own. A lap is still bounded:
/// the walk comes back to an older batch the native owner has since
/// admitted.
#[tokio::test]
async fn a_lap_of_full_windows_still_comes_back_to_an_older_batch() {
    const ROUNDS: usize = 5;
    const FRESH: usize = 64;
    let mut native = ScriptedNative::new();
    native.apply_on_commit = false;
    native.known =
        std::iter::once("agent:held".to_string())
            .chain((1..=ROUNDS).flat_map(|round| {
                (0..FRESH).map(move |index| format!("agent:f{round}-{index:02}"))
            }))
            .collect();
    let harness = harness_with(native);
    let subscribe_and_announce = |agent: String| {
        let service = harness.service.clone();
        async move {
            let topic = agent.replace(':', "-");
            service
                .put_subscription(
                    &op(&format!("o-{topic}")),
                    put(
                        &format!("s-{topic}"),
                        &agent,
                        &[topic.as_str()],
                        StartFrontier::Now,
                    ),
                )
                .await
                .unwrap();
            service
                .announce(announce_request("events", &topic, &[topic.as_str()], "x"))
                .await
                .unwrap();
        }
    };
    // The held member's batch is sealed first, alone, so it is the oldest
    // unsettled batch: the native owner commits it and does not admit it.
    subscribe_and_announce("agent:held".to_string()).await;
    let mut now = 62_000_u64;
    harness.clock.store(now, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let mut held_batch = None;
    for round in 1..=ROUNDS {
        for index in 0..FRESH {
            subscribe_and_announce(format!("agent:f{round}-{index:02}")).await;
        }
        now += 61_000;
        harness.clock.store(now, Ordering::SeqCst);
        harness.service.run_round().await.unwrap();
        if round == 2 {
            // The native owner admits the held member's batch now.
            let held = harness
                .service
                .deliveries(&BatchQuery {
                    member: Some(member("agent:held")),
                    limit: 10,
                    ..BatchQuery::default()
                })
                .await
                .unwrap()
                .remove(0);
            harness
                .native
                .ledger
                .lock()
                .unwrap()
                .bindings
                .get_mut(held.id.as_str())
                .expect("committed")
                .2 = true;
            held_batch = Some(held.id);
        }
    }
    let held = harness
        .service
        .observe_delivery(&held_batch.expect("round 2 ran"))
        .await
        .unwrap();
    assert_eq!(
        held.batch.state.tag(),
        BatchStateTag::Delivered,
        "a lap is bounded, so the walk comes back to the older batch"
    );
}

/// The walk keeps when to read a batch again, and when it last reported a
/// failing one, only while the batch is unsettled. A batch a reconcile
/// settles leaves at once; one the store settles (its last subscription
/// went) leaves at the next window over it. Churn cannot grow the walk.
#[tokio::test]
async fn the_walk_drops_batches_once_they_settle() {
    let (dir, inner) = temp_store();
    let store = Arc::new(FaultyStore::new(inner, Some("agent:b")));
    let mut native = ScriptedNative::new();
    native.apply_on_commit = false;
    native.known = ["agent:a", "agent:b", "agent:c"].map(String::from).into();
    let native = Arc::new(native);
    let clock = Arc::new(AtomicU64::new(1_000));
    let reader = Arc::clone(&clock);
    let service = AnnouncementService::with_clock(
        Arc::clone(&store) as Arc<dyn AnnouncementStore>,
        Arc::clone(&native) as Arc<dyn NativeDelivery>,
        AnnouncementConfig {
            policy: policy(),
            ..AnnouncementConfig::default()
        },
        Arc::new(move || reader.load(Ordering::SeqCst)),
    )
    .unwrap();
    for name in ["a", "b", "c"] {
        service
            .put_subscription(
                &op(&format!("o-{name}")),
                put(
                    &format!("s-{name}"),
                    &format!("agent:{name}"),
                    &[name],
                    StartFrontier::Now,
                ),
            )
            .await
            .unwrap();
        service
            .announce(announce_request("events", name, &[name], "x"))
            .await
            .unwrap();
    }
    clock.store(100_000, Ordering::SeqCst);
    let report = service.run_round().await.unwrap();
    assert_eq!(report.sealed, 3);
    assert_eq!(
        service.walk_entries().await,
        (3, 1),
        "a and c wait for their retry time; b failed, is retried later and was reported"
    );
    let batch_of = |agent: &'static str| {
        let service = service.clone();
        async move {
            service
                .deliveries(&BatchQuery {
                    member: Some(member(agent)),
                    limit: 10,
                    ..BatchQuery::default()
                })
                .await
                .unwrap()
                .remove(0)
        }
    };

    // The native owner admits a's batch and a reconcile settles it.
    let a = batch_of("agent:a").await;
    native
        .ledger
        .lock()
        .unwrap()
        .bindings
        .get_mut(a.id.as_str())
        .expect("committed")
        .2 = true;
    let reconciled = service.reconcile_delivery(&a.id).await.unwrap();
    assert_eq!(reconciled.batch.state.tag(), BatchStateTag::Delivered);
    assert_eq!(
        service.walk_entries().await,
        (2, 1),
        "a batch a reconcile settles leaves the walk at once"
    );

    // b's only subscription goes, and the store cancels its sealed batch.
    service
        .change_subscription(
            &op("o-b-cancel"),
            &sid("s-b"),
            1,
            SubscriptionChange::Cancel,
        )
        .await
        .unwrap();
    assert_eq!(
        batch_of("agent:b").await.state.tag(),
        BatchStateTag::Cancelled
    );
    // Nothing is due yet; the round's window over the unsettled batches no
    // longer holds b's.
    service.run_round().await.unwrap();
    assert_eq!(
        service.walk_entries().await,
        (1, 0),
        "a batch the store settled leaves the walk at the next window over it"
    );
    drop(dir);
}

/// A long retention backlog runs pass after pass with no pause, but the
/// rounds keep their own pace: a pass that has more work behind it does not
/// bring a round with it.
#[tokio::test]
async fn retention_catch_up_keeps_the_round_pace() {
    const PASSES: u64 = 40;
    let (dir, inner) = temp_store();
    let store = Arc::new(FaultyStore::new(inner, None));
    store.more_passes.store(PASSES, Ordering::SeqCst);
    let service = AnnouncementService::with_clock(
        Arc::clone(&store) as Arc<dyn AnnouncementStore>,
        Arc::new(ScriptedNative::new()) as Arc<dyn NativeDelivery>,
        AnnouncementConfig::default(),
        // Past the first retention interval, so the dispatcher's first
        // iteration runs a pass.
        Arc::new(|| 10_000_000),
    )
    .unwrap();
    let dispatcher = service.start();
    let caught_up = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while store.prune_passes.load(Ordering::SeqCst) <= PASSES {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    service.stop();
    dispatcher.await.unwrap();
    assert!(caught_up.is_ok(), "the passes run back to back");
    assert_eq!(
        store.backlog_reads.load(Ordering::SeqCst),
        1,
        "cadence: one round over {} catch-up passes, since none came due",
        PASSES + 1
    );
    drop(dir);
}

#[tokio::test]
async fn governed_runtimes_and_unknown_members_are_refused_typed() {
    let mut native = ScriptedNative::new();
    native.governed = true;
    let harness = harness_with(native);
    assert_eq!(
        harness
            .service
            .put_subscription(
                &op("o1"),
                put("s1", "agent:a", &["ios"], StartFrontier::Now)
            )
            .await,
        Err(AnnouncementError::GovernedRuntime)
    );
    let harness = harness_with(ScriptedNative::new());
    assert_eq!(
        harness
            .service
            .put_subscription(
                &op("o1"),
                put("s1", "agent:z", &["ios"], StartFrontier::Now)
            )
            .await,
        Err(AnnouncementError::UnknownMember(member("agent:z")))
    );
}

#[tokio::test]
async fn a_peer_member_needs_a_usable_peer_not_a_local_identity() {
    let mut native = ScriptedNative::new();
    // This gateway's own runtime refuses host continuations; the peer
    // answers for its own.
    native.governed = true;
    let harness = harness_with(native);
    let remote = peer_member("agent:x", "mob-b");
    let subscription = harness
        .service
        .put_subscription(&op("o1"), put_for("s1", remote.clone(), &["ios"]))
        .await
        .unwrap();
    assert_eq!(subscription.member, remote);
    assert!(matches!(
        harness
            .service
            .put_subscription(
                &op("o2"),
                put_for("s2", peer_member("agent:x", "mob-z"), &["ios"])
            )
            .await,
        Err(AnnouncementError::UnusablePeer(_))
    ));
}

#[tokio::test]
async fn a_remote_burst_is_delivered_in_batches_that_fit_the_wire() {
    let harness = harness_with(ScriptedNative::new());
    let remote = peer_member("agent:x", "mob-b");
    harness
        .service
        .put_subscription(&op("o1"), put_for("s1", remote.clone(), &["ios"]))
        .await
        .unwrap();
    let body = "x".repeat(2_000);
    for index in 0..40 {
        harness
            .service
            .announce(announce_request(
                "payments",
                &format!("k{index}"),
                &["ios"],
                &body,
            ))
            .await
            .unwrap();
    }
    let mut delivered = 0;
    let mut batches = 0;
    for round in 0..10_u64 {
        harness
            .clock
            .store(10_000_000 + round * 60_000, Ordering::SeqCst);
        harness.service.run_round().await.unwrap();
        let all = harness
            .service
            .deliveries(&BatchQuery {
                member: Some(remote.clone()),
                limit: 100,
                ..BatchQuery::default()
            })
            .await
            .unwrap();
        delivered = all.iter().map(|batch| batch.items.len()).sum::<usize>();
        batches = all.len();
        for batch in &all {
            assert!(wire_len(&batch.body) <= MAX_REMOTE_BODY_WIRE_BYTES);
            assert_eq!(batch.state.tag(), BatchStateTag::Delivered);
        }
        if delivered == 40 {
            break;
        }
    }
    assert_eq!(
        delivered, 40,
        "every announcement is delivered exactly once"
    );
    assert!(batches >= 2, "one request cannot carry the whole burst");
}

#[tokio::test]
async fn the_rpc_surface_is_strict_and_typed() {
    use super::rpc::{
        ANNOUNCEMENTS_CONFLICT_CODE, ANNOUNCEMENTS_FRONTIER_CODE, ANNOUNCEMENTS_NOT_FOUND_CODE,
        ANNOUNCEMENTS_REFUSED_CODE, ANNOUNCEMENTS_UNAVAILABLE_CODE, handle_announcement_method,
    };
    let own = mob("payments");
    let unavailable = handle_announcement_method(
        None,
        &own,
        "meta/mob_announcements/announce",
        &serde_json::json!({}),
    )
    .await
    .unwrap_err();
    assert_eq!(unavailable.code, ANNOUNCEMENTS_UNAVAILABLE_CODE);

    let harness = harness_with(ScriptedNative::new());
    let service = Some(&harness.service);
    let call = |method: &'static str, params: serde_json::Value| {
        let own = own.clone();
        async move { handle_announcement_method(service, &own, method, &params).await }
    };
    let unknown_field = call(
        "meta/mob_announcements/announce",
        serde_json::json!({ "source_key": "k", "topics": ["t"], "body": "b", "extra": 1 }),
    )
    .await
    .unwrap_err();
    assert_eq!(unknown_field.code, -32602);
    let as_member = call(
        "meta/mob_announcements/announce",
        serde_json::json!({
            "source_key": "k", "topics": ["t"], "body": "b",
            "announced_by": { "kind": "member", "identity": "agent:a" }
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(
        as_member.code, -32602,
        "a host call cannot announce as a member"
    );
    let announced = call(
        "meta/mob_announcements/announce",
        serde_json::json!({ "source_key": "k", "topics": ["t"], "body": "b" }),
    )
    .await
    .unwrap();
    assert_eq!(announced["announcement"]["sequence"], 1);
    assert_eq!(
        announced["announcement"]["mob_id"], "payments",
        "a call that names no mob announces into this host's mob"
    );
    assert_eq!(
        announced["announcement"]["announced_by"],
        serde_json::json!({ "kind": "host" })
    );
    assert_eq!(announced["captured"], 0);
    assert!(
        announced.get("captured_for").is_none(),
        "an announce result counts the subscriptions that captured it, never names them"
    );
    let elsewhere = call(
        "meta/mob_announcements/announce",
        serde_json::json!({ "mob_id": "nowhere", "source_key": "k", "topics": ["t"], "body": "b" }),
    )
    .await
    .unwrap_err();
    assert_eq!(elsewhere.code, ANNOUNCEMENTS_NOT_FOUND_CODE);
    assert_eq!(
        elsewhere.data.unwrap(),
        serde_json::json!({ "kind": "unknown_mob", "mobs": ["nowhere"] })
    );
    let conflict = call(
        "meta/mob_announcements/announce",
        serde_json::json!({ "source_key": "k", "topics": ["t"], "body": "c" }),
    )
    .await
    .unwrap_err();
    assert_eq!(conflict.code, ANNOUNCEMENTS_CONFLICT_CODE);
    assert_eq!(conflict.data.unwrap()["kind"], "announcement_conflict");
    let subscribed = call(
        "meta/mob_announcements/subscriptions/put",
        serde_json::json!({
            "operation_id": "op-1",
            "subscription_id": "sub-1",
            "member": { "mob_id": "payments", "identity": "agent:a" },
            "topics": ["t"]
        }),
    )
    .await
    .unwrap();
    assert_eq!(subscribed["subscription"]["revision"], 1);
    assert_eq!(subscribed["subscription"]["state"], "active");
    assert_eq!(
        subscribed["subscription"]["selector"]["mobs"],
        serde_json::json!(["payments"]),
        "a subscription that names no mob takes this host's mob"
    );
    let remote = serde_json::json!({ "mob_id": "mob-b", "identity": "agent:x" });
    let subscribed = call(
        "meta/mob_announcements/subscriptions/put",
        serde_json::json!({
            "operation_id": "op-2",
            "subscription_id": "sub-2",
            "member": remote,
            "topics": ["t"],
            "mobs": ["payments", "builds"]
        }),
    )
    .await
    .unwrap();
    assert_eq!(subscribed["subscription"]["member"], remote);
    let listed = call(
        "meta/mob_announcements/subscriptions/list",
        serde_json::json!({ "member": remote }),
    )
    .await
    .unwrap();
    assert_eq!(listed["subscriptions"].as_array().map(Vec::len), Some(1));
    let unusable = call(
        "meta/mob_announcements/subscriptions/put",
        serde_json::json!({
            "operation_id": "op-3",
            "subscription_id": "sub-3",
            "member": { "mob_id": "mob-z", "identity": "agent:x" },
            "topics": ["t"]
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(unusable.code, ANNOUNCEMENTS_REFUSED_CODE);
    assert_eq!(unusable.data.unwrap()["kind"], "unusable_peer");
    let child = call(
        "meta/mob_announcements/subscriptions/put",
        serde_json::json!({
            "operation_id": "op-3b",
            "subscription_id": "sub-3b",
            "member": { "mob_id": "builds", "identity": "worker-1" },
            "topics": ["t"]
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(child.code, ANNOUNCEMENTS_REFUSED_CODE);
    assert_eq!(
        child.data.unwrap()["kind"],
        "unsupported_member",
        "a member of another mob hosted here cannot subscribe yet"
    );
    let malformed = call(
        "meta/mob_announcements/subscriptions/put",
        serde_json::json!({
            "operation_id": "op-4",
            "subscription_id": "sub-4",
            "member": { "mob_id": "mob-b", "identity": "agent:x", "host": "mob-b" },
            "topics": ["t"]
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(malformed.code, -32602, "unknown member fields are refused");
    let bare = call(
        "meta/mob_announcements/subscriptions/put",
        serde_json::json!({
            "operation_id": "op-4b",
            "subscription_id": "sub-4b",
            "member": "agent:a",
            "topics": ["t"]
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(bare.code, -32602, "a member names its mob");
    let beyond = call(
        "meta/mob_announcements/subscriptions/put",
        serde_json::json!({
            "operation_id": "op-5",
            "subscription_id": "sub-5",
            "member": { "mob_id": "payments", "identity": "agent:a" },
            "topics": ["t"],
            "start": { "kind": "after", "frontier": { "payments": 2 } }
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(beyond.code, ANNOUNCEMENTS_FRONTIER_CODE);
    assert_eq!(
        beyond.data.unwrap(),
        serde_json::json!({ "kind": "frontier_beyond_head", "mob_id": "payments", "head": 1 })
    );
}

#[tokio::test]
async fn a_member_gone_for_good_ends_its_subscriptions_instead_of_retrying() {
    for end in [
        SubscriptionEnd::MemberLeft,
        SubscriptionEnd::MemberUnusable {
            detail: "agent:a is placed on a host".into(),
        },
    ] {
        let harness = harness_with(ScriptedNative::new());
        harness.subscribe("s1").await;
        harness.announce_burst(3).await;
        harness
            .native
            .gone
            .lock()
            .unwrap()
            .insert("agent:a".into(), end.clone());
        harness.clock.store(10_000_000, Ordering::SeqCst);
        let report = harness.service.run_round().await.unwrap();
        assert_eq!(report.sealed, 1);
        assert_eq!(
            harness.only_batch().await.state,
            BatchState::Cancelled,
            "nothing native existed, so the batch is cancelled"
        );
        assert_eq!(harness.native.submits(), 0);
        assert_eq!(
            harness.service.walk_entries().await,
            (0, 0),
            "nothing is left to retry"
        );
        let subscription = harness
            .service
            .subscription(&sid("s1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(subscription.state, SubscriptionState::Ended);
        assert_eq!(subscription.ended, Some(end));
        let later = harness
            .service
            .announce(announce_request("payments", "later", &["ios"], "x"))
            .await
            .unwrap();
        assert!(later.captured_for.is_empty());
        harness.round_after_pause().await;
        assert_eq!(harness.native.submits(), 0);
    }
}

#[tokio::test]
async fn a_final_refusal_from_the_member_owner_ends_its_subscriptions() {
    let harness = harness_with(ScriptedNative::new());
    let remote = peer_member("agent:x", "mob-b");
    harness
        .service
        .put_subscription(&op("o1"), put_for("s1", remote.clone(), &["ios"]))
        .await
        .unwrap();
    harness.announce_burst(2).await;
    *harness.native.submit_gone.lock().unwrap() = Some(SubscriptionEnd::PeerUnsupported);
    harness.clock.store(10_000_000, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let batches = harness
        .service
        .deliveries(&BatchQuery {
            member: Some(remote),
            limit: 10,
            ..BatchQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert!(
        matches!(&batches[0].state, BatchState::Refused { receipt: None, .. }),
        "nothing was committed: {:?}",
        batches[0].state
    );
    let subscription = harness
        .service
        .subscription(&sid("s1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(subscription.state, SubscriptionState::Ended);
    assert_eq!(subscription.ended, Some(SubscriptionEnd::PeerUnsupported));
    harness.round_after_pause().await;
    assert_eq!(harness.native.submits(), 1, "a final answer is not retried");
}

#[tokio::test]
async fn destroyed_mobs_are_reconciled_from_the_mob_owner() {
    let harness = harness_with(ScriptedNative::new());
    let mut both = put("s1", "agent:a", &["ios"], StartFrontier::Now);
    both.selector.mobs = [mob("payments"), mob("builds")].into();
    harness
        .service
        .put_subscription(&op("o1"), both)
        .await
        .unwrap();
    assert!(
        harness.service.reconcile_mobs().await.unwrap().is_empty(),
        "nothing is destroyed yet"
    );
    harness
        .native
        .destroyed
        .lock()
        .unwrap()
        .insert("builds".into());
    assert_eq!(
        harness.service.reconcile_mobs().await.unwrap(),
        vec![sid("s1")]
    );
    let subscription = harness
        .service
        .subscription(&sid("s1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(subscription.selector.mobs, [mob("payments")].into());
    assert_eq!(subscription.ended_mobs, [mob("builds")].into());
    assert!(
        harness.service.reconcile_mobs().await.unwrap().is_empty(),
        "a reconcile changes nothing twice"
    );
    assert_eq!(
        harness
            .service
            .announce(announce_request("builds", "k", &["ios"], "x"))
            .await,
        Err(AnnouncementError::UnknownMobs(vec![mob("builds")]))
    );
    let mut gone = put("s2", "agent:a", &["ios"], StartFrontier::Now);
    gone.selector.mobs = [mob("builds")].into();
    assert_eq!(
        harness.service.put_subscription(&op("o2"), gone).await,
        Err(AnnouncementError::UnknownMobs(vec![mob("builds")]))
    );
}

#[tokio::test]
async fn members_this_runtime_cannot_deliver_to_are_refused_at_put() {
    let mut native = ScriptedNative::new();
    native.known.insert("agent:placed".into());
    native.unusable.insert("agent:placed".into());
    let harness = harness_with(native);
    assert!(matches!(
        harness
            .service
            .put_subscription(
                &op("o1"),
                put("s1", "agent:placed", &["ios"], StartFrontier::Now)
            )
            .await,
        Err(AnnouncementError::UnusableMember(_))
    ));
    assert!(
        matches!(
            harness
                .service
                .put_subscription(
                    &op("o2"),
                    put_for("s2", peer_member("worker-1", "builds"), &["ios"])
                )
                .await,
            Err(AnnouncementError::UnsupportedMember(_))
        ),
        "a member of another mob hosted here cannot subscribe yet"
    );
}

#[test]
fn storage_maintenance_fences_the_announcement_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join(crate::storage_layout::MOB_ANNOUNCEMENTS_DB_FILE_NAME);
    std::fs::write(&path, b"").unwrap();
    assert!(
        crate::storage_migrate::enumerate_state_dir_databases(dir.path()).contains(&path),
        "a database only some gateways open is still fenced in every build"
    );
}

fn admit(harness: &Harness, id: &BatchId) {
    harness
        .native
        .ledger
        .lock()
        .unwrap()
        .bindings
        .get_mut(id.as_str())
        .expect("committed")
        .2 = true;
}

/// Two windows of unsettled batches. The first round seals a full window
/// of batches (one round's member limit); the second reads them and seals
/// one more, the newest. The round at 184 s starts a lap that ends at the
/// newest batch: it reads the full window of older batches (due again at
/// 196.2 s) and resumes after them. Returns the harness and the newest
/// batch.
async fn two_window_lap() -> (Harness, BatchId) {
    const MEMBERS: usize = super::owner::ROUND_BATCHES + 1;
    assert_eq!(
        super::owner::ROUND_MEMBERS,
        super::owner::ROUND_BATCHES,
        "the first round seals exactly one window"
    );
    let mut native = ScriptedNative::new();
    native.apply_on_commit = false;
    native.known = (0..MEMBERS)
        .map(|index| format!("agent:r{index:02}"))
        .collect();
    let harness = harness_with(native);
    for index in 0..MEMBERS {
        let agent = format!("agent:r{index:02}");
        let topic = agent.replace(':', "-");
        harness
            .service
            .put_subscription(
                &op(&format!("o-{topic}")),
                put(
                    &format!("s-{topic}"),
                    &agent,
                    &[topic.as_str()],
                    StartFrontier::Now,
                ),
            )
            .await
            .unwrap();
        harness
            .service
            .announce(announce_request("events", &topic, &[topic.as_str()], "x"))
            .await
            .unwrap();
    }
    for now in [62_000_u64, 123_000] {
        harness.clock.store(now, Ordering::SeqCst);
        harness.service.run_round().await.unwrap();
    }
    let unsettled = harness.store.unsettled_batches(None, 100).await.unwrap();
    assert_eq!(unsettled.len(), MEMBERS);
    let newest = unsettled.last().unwrap().clone();
    assert_eq!(newest.sealed_at_ms, 123_000);
    harness.clock.store(184_000, Ordering::SeqCst);
    let report = harness.service.run_round().await.unwrap();
    assert_eq!(
        report.next_due_ms,
        Some(185_000),
        "the lap continues after the full window of older batches: {report:?}"
    );
    (harness, newest.id)
}

/// A reconcile settles the lap's last batch between rounds, so the next
/// round's window over the rest of the lap is empty. The round still names a
/// next look no later than when the batches the walk read at 184 s are due
/// again (196.2 s); with none, the dispatcher would idle five minutes.
#[tokio::test]
async fn a_lap_ending_on_an_empty_window_names_the_next_look() {
    let (harness, newest) = two_window_lap().await;
    admit(&harness, &newest);
    let reconciled = harness.service.reconcile_delivery(&newest).await.unwrap();
    assert_eq!(reconciled.batch.state.tag(), BatchStateTag::Delivered);
    harness.clock.store(185_000, Ordering::SeqCst);
    let report = harness.service.run_round().await.unwrap();
    assert!(
        report.next_due_ms.is_some_and(|due| due <= 196_200),
        "a window of unsettled batches is due again at 196200; the round names no look \
         by then: {report:?}"
    );
}

/// The lap's last window settles everything it reads (the newest batch was
/// admitted meanwhile). Same expectation.
#[tokio::test]
async fn a_lap_ending_on_a_settled_window_names_the_next_look() {
    let (harness, newest) = two_window_lap().await;
    admit(&harness, &newest);
    harness.clock.store(185_000, Ordering::SeqCst);
    let report = harness.service.run_round().await.unwrap();
    assert_eq!(
        harness
            .store
            .batch(&newest)
            .await
            .unwrap()
            .unwrap()
            .state
            .tag(),
        BatchStateTag::Delivered,
        "the round read and settled the newest batch"
    );
    assert!(
        report.next_due_ms.is_some_and(|due| due <= 196_200),
        "a window of unsettled batches is due again at 196200; the round names no look \
         by then: {report:?}"
    );
}

/// The wall clock steps back an hour after a committed batch was read; the
/// native owner then admits it. Rounds over the next 155 s of clock time read
/// it again once a settle pause (at most 30 s) has passed.
#[tokio::test]
async fn a_clock_step_back_does_not_hide_a_remembered_batch() {
    let mut native = ScriptedNative::new();
    native.apply_on_commit = false;
    let harness = harness_with(native);
    harness.subscribe("s1").await;
    harness.announce_burst(1).await;
    harness.clock.store(10_000_000, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let batch = harness.only_batch().await;
    assert_eq!(batch.state.tag(), BatchStateTag::Committed);
    admit(&harness, &batch.id);
    let mut now = 10_000_000 - 3_600_000;
    for _ in 0..5 {
        now += 31_000;
        harness.clock.store(now, Ordering::SeqCst);
        harness.service.run_round().await.unwrap();
    }
    assert_eq!(
        harness.only_batch().await.state.tag(),
        BatchStateTag::Delivered,
        "155 s of clock time after a one-hour step back, the admitted batch is still unread"
    );
}

/// The wall clock steps back an hour while retention catches up. The round
/// it had scheduled now lies more than the longest idle ahead; the
/// dispatcher runs a round anyway instead of waiting for the clock.
#[tokio::test]
async fn a_clock_step_back_during_catch_up_still_runs_rounds() {
    const PASSES: u64 = 300;
    let (dir, inner) = temp_store();
    let store = Arc::new(FaultyStore::new(inner, None));
    store.more_passes.store(PASSES, Ordering::SeqCst);
    let clock = Arc::new(AtomicU64::new(10_000_000));
    let reader = Arc::clone(&clock);
    let service = AnnouncementService::with_clock(
        Arc::clone(&store) as Arc<dyn AnnouncementStore>,
        Arc::new(ScriptedNative::new()) as Arc<dyn NativeDelivery>,
        AnnouncementConfig::default(),
        Arc::new(move || reader.load(Ordering::SeqCst)),
    )
    .unwrap();
    let dispatcher = service.start();
    let first_round = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while store.backlog_reads.load(Ordering::SeqCst) < 1
            || store.prune_passes.load(Ordering::SeqCst) < 2
        {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(first_round.is_ok(), "the dispatcher started");
    clock.store(10_000_000 - 3_600_000, Ordering::SeqCst);
    let caught_up = tokio::time::timeout(std::time::Duration::from_mins(1), async {
        while store.prune_passes.load(Ordering::SeqCst) <= PASSES {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    service.stop();
    dispatcher.await.unwrap();
    assert!(caught_up.is_ok(), "the passes ran");
    assert!(
        store.backlog_reads.load(Ordering::SeqCst) >= 2,
        "after the clock stepped back an hour, no round ran for the rest of the catch-up"
    );
    drop(dir);
}

/// Wakes arrive every millisecond while retention catches up. Each wake may
/// bring a round, but the passes keep their own spacing, so commands get the
/// store between them.
#[tokio::test]
async fn catch_up_passes_keep_their_spacing_under_wakes() {
    const PASSES: u64 = 30;
    let (dir, inner) = temp_store();
    let store = Arc::new(FaultyStore::new(inner, None));
    store.more_passes.store(PASSES, Ordering::SeqCst);
    let service = AnnouncementService::with_clock(
        Arc::clone(&store) as Arc<dyn AnnouncementStore>,
        Arc::new(ScriptedNative::new()) as Arc<dyn NativeDelivery>,
        AnnouncementConfig::default(),
        // Past the first retention interval, so the first iteration passes.
        Arc::new(|| 10_000_000),
    )
    .unwrap();
    let dispatcher = service.start();
    let waker = {
        let service = service.clone();
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            while store.prune_passes.load(Ordering::SeqCst) <= PASSES {
                service.wake();
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
    };
    let caught_up = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while store.prune_passes.load(Ordering::SeqCst) <= PASSES {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    waker.await.unwrap();
    service.stop();
    dispatcher.await.unwrap();
    assert!(caught_up.is_ok(), "the passes ran");
    let times = store.pass_times.lock().unwrap().clone();
    let shortest = times
        .windows(2)
        .map(|pair| pair[1].duration_since(pair[0]))
        .min()
        .unwrap();
    assert!(
        shortest >= std::time::Duration::from_millis(10),
        "wakes brought passes {shortest:?} apart"
    );
    drop(dir);
}

/// A fault inside a round ends the dispatcher task. It is reported and
/// started again, so the owed announcement is still delivered.
#[tokio::test]
async fn a_dispatcher_that_panics_is_started_again() {
    let (dir, inner) = temp_store();
    let store = Arc::new(FaultyStore::new(inner, None));
    let native = Arc::new(ScriptedNative::new());
    let clock = Arc::new(AtomicU64::new(1_000));
    let reader = Arc::clone(&clock);
    let service = AnnouncementService::with_clock(
        Arc::clone(&store) as Arc<dyn AnnouncementStore>,
        Arc::clone(&native) as Arc<dyn NativeDelivery>,
        AnnouncementConfig {
            policy: policy(),
            ..AnnouncementConfig::default()
        },
        Arc::new(move || reader.load(Ordering::SeqCst)),
    )
    .unwrap();
    service
        .put_subscription(
            &op("o1"),
            put("s1", "agent:a", &["ios"], StartFrontier::Now),
        )
        .await
        .unwrap();
    service
        .announce(announce_request("payments", "k1", &["ios"], "x"))
        .await
        .unwrap();
    clock.store(100_000, Ordering::SeqCst);
    store.panic_next_round.store(true, Ordering::SeqCst);
    let dispatcher = service.start();
    let delivered = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let batches = service
                .deliveries(&BatchQuery {
                    member: Some(member("agent:a")),
                    limit: 10,
                    ..BatchQuery::default()
                })
                .await
                .unwrap();
            if batches
                .first()
                .is_some_and(|batch| batch.state.tag() == BatchStateTag::Delivered)
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    service.stop();
    dispatcher.await.unwrap();
    assert!(
        delivered.is_ok(),
        "after a fault inside a round, no dispatcher delivers the owed announcement"
    );
    drop(dir);
}

/// A batch that keeps failing is reported once per interval. After the
/// clock steps back an hour, its next failure is reported at once, not an
/// hour and an interval later.
#[tokio::test]
async fn a_clock_step_back_does_not_mute_failure_reports() {
    let (dir, inner) = temp_store();
    let store = Arc::new(FaultyStore::new(inner, Some("agent:a")));
    let native = Arc::new(ScriptedNative::new());
    let clock = Arc::new(AtomicU64::new(1_000));
    let reader = Arc::clone(&clock);
    let service = AnnouncementService::with_clock(
        Arc::clone(&store) as Arc<dyn AnnouncementStore>,
        Arc::clone(&native) as Arc<dyn NativeDelivery>,
        AnnouncementConfig {
            policy: policy(),
            ..AnnouncementConfig::default()
        },
        Arc::new(move || reader.load(Ordering::SeqCst)),
    )
    .unwrap();
    service
        .put_subscription(
            &op("o1"),
            put("s1", "agent:a", &["ios"], StartFrontier::Now),
        )
        .await
        .unwrap();
    service
        .announce(announce_request("events", "k1", &["ios"], "x"))
        .await
        .unwrap();
    clock.store(10_000_000, Ordering::SeqCst);
    service.run_round().await.unwrap();
    assert_eq!(service.walk_reported_at().await, vec![10_000_000]);
    let stepped_back = 10_000_000 - 3_600_000;
    clock.store(stepped_back, Ordering::SeqCst);
    service.run_round().await.unwrap();
    assert_eq!(
        service.walk_reported_at().await,
        vec![stepped_back],
        "a report dated after the stepped-back clock is dropped, so the failure is reported \
         again now"
    );
    drop(dir);
}

// ---------------------------------------------------------------------------
// Member tools
// ---------------------------------------------------------------------------

/// Sessions the scripted resolver attributes to members.
struct ScriptedCallers {
    sessions: BTreeMap<String, Member>,
}

#[async_trait]
impl super::tools::CallerResolver for ScriptedCallers {
    async fn caller(
        &self,
        session: &meerkat_core::types::SessionId,
    ) -> std::result::Result<Member, String> {
        self.sessions
            .get(&session.to_string())
            .cloned()
            .ok_or_else(|| "the calling session is not a member of this gateway's mob".to_string())
    }
}

struct ToolHarness {
    harness: Harness,
    tools: super::tools::MobAnnouncementTools,
    sessions: BTreeMap<&'static str, meerkat_core::types::SessionId>,
}

fn tool_harness(limits: super::tools::MemberLimits) -> ToolHarness {
    let mut native = ScriptedNative::new();
    native.known.insert("agent:b".into());
    let harness = harness_with(native);
    let sessions = BTreeMap::from([
        ("agent:a", meerkat_core::types::SessionId::new()),
        ("agent:b", meerkat_core::types::SessionId::new()),
    ]);
    let callers = ScriptedCallers {
        sessions: sessions
            .iter()
            .map(|(identity, session)| (session.to_string(), member(identity)))
            .collect(),
    };
    let slot: super::tools::ToolSlot = Arc::new(std::sync::OnceLock::new());
    let bound = slot
        .set(super::tools::ToolBackend {
            service: harness.service.clone(),
            callers: Arc::new(callers),
            limits,
        })
        .is_ok();
    assert!(bound);
    let clock = Arc::clone(&harness.clock);
    let tools = super::tools::MobAnnouncementTools::with_clock(
        slot,
        Arc::new(move || clock.load(Ordering::SeqCst)),
    );
    ToolHarness {
        harness,
        tools,
        sessions,
    }
}

impl ToolHarness {
    /// Call a tool as `who` (`None`: a call with no session at all), in a
    /// turn of its own.
    async fn call(
        &self,
        who: Option<&str>,
        tool: &str,
        args: serde_json::Value,
    ) -> (bool, serde_json::Value) {
        use meerkat_core::AgentToolDispatcher as _;
        let raw = serde_json::value::RawValue::from_string(args.to_string()).unwrap();
        let call = meerkat_core::types::ToolCallView {
            id: "call-1",
            name: tool,
            args: &raw,
        };
        let mut context = meerkat_core::ToolDispatchContext::default();
        if let Some(who) = who {
            let session = self
                .sessions
                .get(who)
                .cloned()
                .unwrap_or_else(meerkat_core::types::SessionId::new);
            context = context.with_runtime_identity(
                session,
                Some(meerkat_core::interaction::InteractionId::new()),
            );
        }
        let outcome = self
            .tools
            .dispatch_with_context(call, &context)
            .await
            .unwrap();
        let text = meerkat_core::types::text_content(&outcome.result.content);
        let value = serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
        (outcome.result.is_error, value)
    }
}

#[tokio::test]
async fn a_member_announces_as_itself_into_its_own_mob() {
    let tools = tool_harness(super::tools::MemberLimits::default());
    tools
        .harness
        .service
        .put_subscription(
            &op("o1"),
            put("s1", "agent:b", &["ios"], StartFrontier::Now),
        )
        .await
        .unwrap();
    let (failed, result) = tools
        .call(
            Some("agent:a"),
            super::tools::ANNOUNCE_TOOL,
            serde_json::json!({ "topics": ["ios"], "body": "checkout fixed", "key": "fix-1" }),
        )
        .await;
    assert!(!failed, "{result}");
    assert_eq!(result["mob_id"], OWN_MOB);
    assert_eq!(result["captured"], 1);
    assert!(
        result.get("captured_for").is_none(),
        "who subscribes is not the announcer's to learn: {result}"
    );
    let key = format!("{}-fix-1", super::tools::member_prefix(&member("agent:a")));
    let stored = tools
        .harness
        .store
        .announcement(&mob(OWN_MOB), &SourceKey::new(key).unwrap())
        .await
        .unwrap()
        .expect("the key lives in the calling member's namespace");
    assert_eq!(
        stored.announced_by,
        Announcer::Member {
            identity: MemberIdentity::new("agent:a").unwrap(),
        },
        "the announcement is attributed to the calling member"
    );
}

#[tokio::test]
async fn a_call_that_cannot_be_attributed_changes_nothing() {
    let tools = tool_harness(super::tools::MemberLimits::default());
    for who in [None, Some("a session of no member")] {
        let (failed, result) = tools
            .call(
                who,
                super::tools::ANNOUNCE_TOOL,
                serde_json::json!({ "topics": ["ios"], "body": "x" }),
            )
            .await;
        assert!(failed, "{who:?}: {result}");
    }
    assert!(
        tools.harness.store.known_mobs().await.unwrap().is_empty(),
        "nothing was announced"
    );
}

/// What attributing a call from `session` decides when the roster binds
/// `bound` to it and the identity runtime records `record` for `agent:a`
/// and knows no other identity.
async fn attributed(
    session: &meerkat_core::types::SessionId,
    bound: &[&str],
    record: super::tools::RecordedBinding,
) -> std::result::Result<Member, String> {
    let bound = bound.iter().map(|id| (*id).to_string()).collect::<Vec<_>>();
    super::tools::attribute(&mob(OWN_MOB), session, &bound, |agent| {
        std::future::ready(if agent.as_str() == "agent:a" {
            Ok(record)
        } else {
            Err(format!(
                "the calling member is not registered here: unknown identity {agent}"
            ))
        })
    })
    .await
}

#[tokio::test]
async fn a_call_is_attributed_only_to_the_one_member_both_records_agree_on() {
    let session = meerkat_core::types::SessionId::new();
    let current = super::tools::RecordedBinding {
        session_id: Some(session.to_string()),
        runtime_id: Some("rt:agent:a:1".to_string()),
    };
    let encoded = crate::member_comms_id::mob_member_id_str("agent:a").into_owned();
    // The roster row in either spelling, bound to the session the identity
    // runtime records: the call is that member's.
    for roster_id in ["agent:a", encoded.as_str()] {
        assert_eq!(
            attributed(&session, &[roster_id], current.clone()).await,
            Ok(member("agent:a")),
            "{roster_id}"
        );
    }

    let another_session = super::tools::RecordedBinding {
        session_id: Some(meerkat_core::types::SessionId::new().to_string()),
        ..current.clone()
    };
    let no_session = super::tools::RecordedBinding {
        session_id: None,
        ..current.clone()
    };
    let cases: [(&str, &[&str], super::tools::RecordedBinding, &str); 7] = [
        (
            "no roster entry is bound to the session",
            &[],
            current.clone(),
            "not a member of this gateway's mob",
        ),
        (
            "two roster entries are bound to the session",
            &["agent:a", "agent:b"],
            current.clone(),
            "more than one member",
        ),
        (
            "the roster id is no durable identity",
            &["not durable"],
            current.clone(),
            "not a durable identity",
        ),
        (
            "the identity runtime does not know the identity",
            &["agent:b"],
            current.clone(),
            "not registered here",
        ),
        (
            "the identity runtime records another session",
            &["agent:a"],
            another_session,
            "does not record the calling session",
        ),
        (
            "the identity runtime records no session",
            &["agent:a"],
            no_session,
            "does not record the calling session",
        ),
        // A generated runtime alias is never decoded to the durable
        // identity, even when it names the current incarnation.
        (
            "the roster id is a runtime alias",
            &["rt:agent:a:1"],
            current.clone(),
            "not registered here",
        ),
    ];
    for (what, bound, record, reason) in cases {
        let refused = attributed(&session, bound, record).await.expect_err(what);
        assert!(refused.contains(reason), "{what}: {refused}");
    }
}

#[tokio::test]
async fn member_limits_bound_announcements_and_subscriptions() {
    let tools = tool_harness(super::tools::MemberLimits {
        announcements_per_minute: 2,
        announcement_bytes_per_minute: 1_000,
        subscriptions: 1,
        topics_per_subscription: 2,
    });
    for (index, expected_failure) in [(0, false), (1, false), (2, true)] {
        let (failed, result) = tools
            .call(
                Some("agent:a"),
                super::tools::ANNOUNCE_TOOL,
                serde_json::json!({ "topics": ["ios"], "body": "x", "key": format!("k{index}") }),
            )
            .await;
        assert_eq!(failed, expected_failure, "announcement {index}: {result}");
    }
    let (failed, result) = tools
        .call(
            Some("agent:b"),
            super::tools::ANNOUNCE_TOOL,
            serde_json::json!({ "topics": ["ios"], "body": "x", "key": "k0" }),
        )
        .await;
    assert!(!failed, "another member's minute is its own: {result}");
    tools.harness.clock.fetch_add(60_000, Ordering::SeqCst);
    let (failed, result) = tools
        .call(
            Some("agent:a"),
            super::tools::ANNOUNCE_TOOL,
            serde_json::json!({ "topics": ["ios"], "body": "x".repeat(1_001) }),
        )
        .await;
    assert!(
        failed,
        "the byte limit holds within a fresh minute: {result}"
    );
    let subscribe = |name: &str, topics: serde_json::Value| serde_json::json!({ "action": "subscribe", "subscription": name, "topics": topics });
    let (failed, result) = tools
        .call(
            Some("agent:a"),
            super::tools::SUBSCRIPTION_TOOL,
            subscribe("wide", serde_json::json!(["a", "b", "c"])),
        )
        .await;
    assert!(failed, "too many topics: {result}");
    let (failed, result) = tools
        .call(
            Some("agent:a"),
            super::tools::SUBSCRIPTION_TOOL,
            subscribe("first", serde_json::json!(["ios"])),
        )
        .await;
    assert!(!failed, "{result}");
    let (failed, result) = tools
        .call(
            Some("agent:a"),
            super::tools::SUBSCRIPTION_TOOL,
            subscribe("second", serde_json::json!(["web"])),
        )
        .await;
    assert!(failed, "one subscription at most: {result}");
}

#[tokio::test]
async fn members_see_and_change_only_their_own_subscriptions() {
    let tools = tool_harness(super::tools::MemberLimits::default());
    let (failed, result) = tools
        .call(
            Some("agent:a"),
            super::tools::SUBSCRIPTION_TOOL,
            serde_json::json!({ "action": "subscribe", "subscription": "news", "topics": ["ios"] }),
        )
        .await;
    assert!(!failed, "{result}");
    assert_eq!(result["subscription"], "news");
    assert_eq!(result["revision"], 1);
    let (failed, result) = tools
        .call(
            Some("agent:b"),
            super::tools::SUBSCRIPTION_TOOL,
            serde_json::json!({ "action": "cancel", "subscription": "news", "expected_revision": 1 }),
        )
        .await;
    assert!(
        failed,
        "another member's subscription is out of reach: {result}"
    );
    let (_, listed) = tools
        .call(
            Some("agent:b"),
            super::tools::SUBSCRIPTION_TOOL,
            serde_json::json!({ "action": "list" }),
        )
        .await;
    assert_eq!(listed["subscriptions"], serde_json::json!([]));
    let (_, listed) = tools
        .call(
            Some("agent:a"),
            super::tools::SUBSCRIPTION_TOOL,
            serde_json::json!({ "action": "list" }),
        )
        .await;
    assert_eq!(listed["subscriptions"][0]["subscription"], "news");
    assert_eq!(listed["subscriptions"][0]["state"], "active", "{listed}");
}

/// Two members naming the same key or the same subscription never meet:
/// each name lives in its member's own namespace.
#[tokio::test]
async fn two_members_never_meet_under_one_name() {
    let tools = tool_harness(super::tools::MemberLimits::default());
    for who in ["agent:a", "agent:b"] {
        let (failed, result) = tools
            .call(
                Some(who),
                super::tools::ANNOUNCE_TOOL,
                serde_json::json!({ "topics": ["ios"], "body": "checkout fixed", "key": "fix-1" }),
            )
            .await;
        assert!(!failed, "{who}: {result}");
        assert_eq!(result["replayed"], false, "{who}: {result}");
        let (failed, result) = tools
            .call(
                Some(who),
                super::tools::SUBSCRIPTION_TOOL,
                serde_json::json!({ "action": "subscribe", "subscription": "news", "topics": ["ios"] }),
            )
            .await;
        assert!(!failed, "{who}: {result}");
        assert_eq!(result["revision"], 1, "{who}: {result}");
    }
    for who in ["agent:a", "agent:b"] {
        let key = format!("{}-fix-1", super::tools::member_prefix(&member(who)));
        let stored = tools
            .harness
            .store
            .announcement(&mob(OWN_MOB), &SourceKey::new(key).unwrap())
            .await
            .unwrap()
            .expect("each member's key is its own");
        assert_eq!(
            stored.announced_by,
            Announcer::Member {
                identity: MemberIdentity::new(who).unwrap(),
            },
            "{who}"
        );
        let (_, listed) = tools
            .call(
                Some(who),
                super::tools::SUBSCRIPTION_TOOL,
                serde_json::json!({ "action": "list" }),
            )
            .await;
        assert_eq!(
            listed["subscriptions"].as_array().map(Vec::len),
            Some(1),
            "{who}: {listed}"
        );
    }
}

// ---------------------------------------------------------------------------
// Lineage and reach
// ---------------------------------------------------------------------------

/// Records for the reach walk: `(mob, parent)`.
/// Live records for `(mob, parent)` pairs, each edge derived under its
/// parent's incarnation; a parent without a record is the root.
fn records(pairs: &[(&str, Option<&str>)]) -> Vec<MobLineage> {
    let incarnation_of = |mob_id: &str| {
        pairs
            .iter()
            .position(|(other, _)| *other == mob_id)
            .map(|index| incarnation("s", index as u64))
    };
    pairs
        .iter()
        .enumerate()
        .map(|(index, (mob_id, parent))| MobLineage {
            parent_incarnation: parent.and_then(incarnation_of),
            ..lineage_record(mob_id, *parent, incarnation("s", index as u64), 1)
        })
        .collect()
}

fn names(reach: &BTreeSet<MobId>) -> Vec<&str> {
    reach.iter().map(MobId::as_str).collect()
}

#[test]
fn reach_is_the_own_mob_its_ancestors_and_its_descendants() {
    use super::owner::reach_from;
    // root
    //  +- team (+- task, +- sibling-task)
    //  +- other-team
    let lineage = records(&[
        ("team", Some("root")),
        ("task", Some("team")),
        ("sibling-task", Some("team")),
        ("deep", Some("task")),
        ("other-team", Some("root")),
        ("unrelated", None),
    ]);
    assert_eq!(
        names(&reach_from(&lineage, &mob("task"))),
        ["deep", "root", "task", "team"],
        "never sideways: sibling-task, other-team and unrelated are out"
    );
    assert_eq!(
        names(&reach_from(&lineage, &mob("root"))),
        ["deep", "other-team", "root", "sibling-task", "task", "team"],
        "the root reaches every mob below it, not one without a member origin"
    );
    assert_eq!(
        names(&reach_from(&lineage, &mob("unrelated"))),
        ["unrelated"]
    );

    // Records that loop (an id reused under a new parent) end the walk.
    let looping = records(&[("x", Some("y")), ("y", Some("x"))]);
    assert_eq!(names(&reach_from(&looping, &mob("x"))), ["x", "y"]);

    // A chain longer than the walk's bound stops at the bound.
    let mut chain = vec![("m0".to_string(), Some("root".to_string()))];
    for level in 1..100 {
        chain.push((format!("m{level}"), Some(format!("m{}", level - 1))));
    }
    let chain = chain
        .iter()
        .map(|(mob_id, parent)| (mob_id.as_str(), parent.as_deref()))
        .collect::<Vec<_>>();
    assert_eq!(reach_from(&records(&chain), &mob("root")).len(), 65);
    assert_eq!(reach_from(&records(&chain), &mob("m99")).len(), 65);

    // A parent created again under the same id adopts no child of its old
    // incarnation: the edge holds only to the incarnation it was derived
    // under.
    let mut adopted = records(&[("team", Some("root")), ("task", Some("team"))]);
    adopted[0].incarnation = incarnation("again", 99);
    assert_eq!(names(&reach_from(&adopted, &mob("root"))), ["root", "team"]);
    assert_eq!(names(&reach_from(&adopted, &mob("task"))), ["task"]);
    // An edge to the root never names an incarnation.
    let mut rooted = records(&[("team", Some("root"))]);
    rooted[0].parent_incarnation = Some(incarnation("root", 1));
    assert_eq!(names(&reach_from(&rooted, &mob("team"))), ["team"]);
}

#[tokio::test]
async fn lineage_is_derived_once_and_only_from_proofs() {
    let native = ScriptedNative::new();
    native.set_origin(
        "events",
        incarnation("s1", 10),
        native.created_by(OWN_MOB, "agent:a"),
    );
    native.set_origin(
        "builds",
        incarnation("s2", 20),
        native.created_by("events", "worker"),
    );
    native.set_origin(
        "refunds",
        incarnation("s3", 30),
        MobOrigin::Unproven("its owner session is bound to no member".into()),
    );
    native.set_origin(
        "a-mob",
        incarnation("s4", 40),
        MobOrigin::ParentNotHosted(mob("gone-mob")),
    );
    native
        .unreadable_origins
        .lock()
        .unwrap()
        .insert("b-mob".into());
    native.set_origin(
        "b-mob",
        incarnation("s5", 50),
        native.created_by(OWN_MOB, "agent:a"),
    );
    native.restored.store(false, Ordering::SeqCst);
    let harness = harness_with(native);
    let lineage = |harness: &Harness| {
        let store = Arc::clone(&harness.store);
        async move {
            store
                .lineage()
                .await
                .unwrap()
                .into_iter()
                .map(|record| {
                    (
                        record.mob_id.to_string(),
                        record.parent.map(|parent| parent.to_string()),
                    )
                })
                .collect::<Vec<_>>()
        }
    };

    // A read that fails writes nothing and is reported; the others are
    // recorded. A missing parent before the restore point waits.
    assert!(harness.service.reconcile_lineage().await.is_err());
    assert_eq!(
        lineage(&harness).await,
        [
            ("builds".to_string(), Some("events".to_string())),
            ("events".to_string(), Some(OWN_MOB.to_string())),
            ("refunds".to_string(), None),
        ]
    );

    // After the restore point a missing parent is final: an orphan.
    harness.native.restored.store(true, Ordering::SeqCst);
    harness.native.unreadable_origins.lock().unwrap().clear();
    harness.service.reconcile_lineage().await.unwrap();
    assert_eq!(
        lineage(&harness).await,
        [
            ("a-mob".to_string(), None),
            ("b-mob".to_string(), Some(OWN_MOB.to_string())),
            ("builds".to_string(), Some("events".to_string())),
            ("events".to_string(), Some(OWN_MOB.to_string())),
            ("refunds".to_string(), None),
        ]
    );

    // Derived once: the same incarnation is never rewritten from what the
    // records say later.
    harness.native.set_origin(
        "events",
        incarnation("s1", 10),
        harness.native.created_by("refunds", "someone-else"),
    );
    harness.service.reconcile_lineage().await.unwrap();
    assert!(
        lineage(&harness)
            .await
            .contains(&("events".to_string(), Some(OWN_MOB.to_string())))
    );

    // A new incarnation (destroyed and created again, even by the same
    // owner session) is derived again.
    harness.native.set_origin(
        "events",
        incarnation("s1", 11),
        harness.native.created_by("refunds", "someone-else"),
    );
    harness.service.reconcile_lineage().await.unwrap();
    assert!(
        lineage(&harness)
            .await
            .contains(&("events".to_string(), Some("refunds".to_string())))
    );

    // A mob no longer hosted leaves the live records.
    harness
        .native
        .destroyed
        .lock()
        .unwrap()
        .insert("builds".into());
    harness.service.reconcile_lineage().await.unwrap();
    assert!(
        !lineage(&harness)
            .await
            .iter()
            .any(|(mob_id, _)| mob_id == "builds")
    );
}

#[tokio::test]
async fn members_subscribe_within_their_reach_only() {
    let tools = tool_harness(super::tools::MemberLimits::default());
    let native = &tools.harness.native;
    native.set_origin(
        "events",
        incarnation("s1", 10),
        native.created_by(OWN_MOB, "agent:a"),
    );
    native.set_origin(
        "builds",
        incarnation("s2", 20),
        native.created_by("events", "worker"),
    );
    native.set_origin(
        "refunds",
        incarnation("s3", 30),
        MobOrigin::Unproven("no member created it".into()),
    );
    let subscribe = |name: &str, mobs: serde_json::Value| {
        serde_json::json!({
            "action": "subscribe",
            "subscription": name,
            "topics": ["ios"],
            "mobs": mobs,
        })
    };
    // Below the caller's mob, derived on first use.
    let (failed, result) = tools
        .call(
            Some("agent:a"),
            super::tools::SUBSCRIPTION_TOOL,
            subscribe("below", serde_json::json!(["events", "builds"])),
        )
        .await;
    assert!(!failed, "{result}");
    assert_eq!(result["mobs"], serde_json::json!(["builds", "events"]));
    // Its own mob by default.
    let (failed, result) = tools
        .call(
            Some("agent:a"),
            super::tools::SUBSCRIPTION_TOOL,
            serde_json::json!({ "action": "subscribe", "subscription": "own", "topics": ["ios"] }),
        )
        .await;
    assert!(!failed, "{result}");
    assert_eq!(result["mobs"], serde_json::json!([OWN_MOB]));
    // A mob no member created is outside every member's reach.
    let (failed, result) = tools
        .call(
            Some("agent:a"),
            super::tools::SUBSCRIPTION_TOOL,
            subscribe("unrelated", serde_json::json!(["refunds"])),
        )
        .await;
    assert!(failed, "{result}");
    assert!(
        result
            .to_string()
            .contains("mob refunds is not the member's own mob"),
        "{result}"
    );
    let (failed, result) = tools
        .call(
            Some("agent:a"),
            super::tools::SUBSCRIPTION_TOOL,
            subscribe("none", serde_json::json!([])),
        )
        .await;
    assert!(failed, "{result}");
    let listed = tools
        .harness
        .store
        .subscriptions(Some(&member("agent:a")), None, 10)
        .await
        .unwrap();
    assert_eq!(listed.len(), 2, "refusals change nothing: {listed:?}");
}

#[test]
fn a_member_created_a_mob_only_when_all_three_records_agree() {
    use super::native::{BindingRecord, OriginRecords, OwnerMetadata, ParentRecord, origin_from};
    let binding = |mob_id: &str, role: &str, member: &str| BindingRecord {
        mob_id: mob_id.to_string(),
        role: role.to_string(),
        member: member.to_string(),
    };
    let owned = |journal: Option<(&str, BindingRecord)>| OriginRecords {
        owner_session: Some("session-1".to_string()),
        owner_metadata: Some(OwnerMetadata::Bound(binding("payments", "lead", "agent:a"))),
        parent: Some(ParentRecord::Running {
            journal: journal.map(|(session, recorded)| (session.to_string(), recorded)),
            incarnation: Some(incarnation("parent", 7)),
        }),
    };
    assert_eq!(
        origin_from(&owned(Some((
            "session-1",
            binding("payments", "lead", "agent:a")
        )))),
        Ok(MobOrigin::Member {
            parent: mob("payments"),
            creator: "agent:a".to_string(),
            parent_incarnation: Some(incarnation("parent", 7)),
        })
    );
    let unproven = [
        ("no owner binding", OriginRecords::default()),
        (
            "no persisted metadata",
            OriginRecords {
                owner_session: Some("session-1".to_string()),
                owner_metadata: Some(OwnerMetadata::Absent),
                parent: None,
            },
        ),
        (
            "no member binding",
            OriginRecords {
                owner_session: Some("session-1".to_string()),
                owner_metadata: Some(OwnerMetadata::Unbound),
                parent: None,
            },
        ),
        ("no journal entry", owned(None)),
        (
            "another session in the journal",
            owned(Some(("session-2", binding("payments", "lead", "agent:a")))),
        ),
        (
            "another mob in the journal",
            owned(Some(("session-1", binding("events", "lead", "agent:a")))),
        ),
        (
            "another role in the journal",
            owned(Some((
                "session-1",
                binding("payments", "worker", "agent:a"),
            ))),
        ),
        (
            "another member in the journal",
            owned(Some(("session-1", binding("payments", "lead", "agent:b")))),
        ),
        (
            "a binding that names no valid mob",
            OriginRecords {
                owner_metadata: Some(OwnerMetadata::Bound(binding("", "lead", "agent:a"))),
                ..owned(None)
            },
        ),
    ];
    for (case, records) in unproven {
        assert!(
            matches!(origin_from(&records), Ok(MobOrigin::Unproven(_))),
            "{case}: {:?}",
            origin_from(&records)
        );
    }
    let parent_at = |parent: ParentRecord| OriginRecords {
        parent: Some(parent),
        ..owned(None)
    };
    assert_eq!(
        origin_from(&parent_at(ParentRecord::NotHosted)),
        Ok(MobOrigin::ParentNotHosted(mob("payments")))
    );
    assert!(
        origin_from(&parent_at(ParentRecord::Held)).is_err(),
        "a held parent may run again: nothing is final yet"
    );
}

// ---------------------------------------------------------------------------
// Expiry, gaps and the retention of native-held bodies
// ---------------------------------------------------------------------------

const WINDOW: u64 = 3_600_000;

fn applied(id: &str) -> BatchEvent {
    BatchEvent::NativeApplied {
        receipt: receipt(id),
        session_id: "session-1".into(),
        input_id: "input-1".into(),
    }
}

fn expired_gap(count: u64) -> BatchGap {
    BatchGap {
        mob_id: mob("payments"),
        count,
        reason: GapReason::Expired,
    }
}

async fn expiry_settles_what_waited_past_the_window(store: &dyn AnnouncementStore) {
    store
        .put_subscription(
            &op("o1"),
            put("s1", "agent:a", &["ios"], StartFrontier::Now),
            1_000,
        )
        .await
        .unwrap();
    // A second subscription of the same member captures the same
    // announcement: it still expires once.
    store
        .put_subscription(
            &op("o2"),
            put("s2", "agent:a", &["ios"], StartFrontier::Now),
            1_000,
        )
        .await
        .unwrap();
    store
        .announce(announce_request("payments", "k1", &["ios"], "old"), 1_000)
        .await
        .unwrap();
    assert_eq!(
        store.expire(1_000 + WINDOW - 1, WINDOW, 100).await.unwrap(),
        ExpiryReport::default(),
        "inside the window nothing expires"
    );
    let report = store.expire(1_000 + WINDOW, WINDOW, 100).await.unwrap();
    assert_eq!(report.captures, 1, "once per member: {report:?}");

    let now = 1_000 + WINDOW + 10;
    store
        .announce(announce_request("payments", "k2", &["ios"], "new"), now)
        .await
        .unwrap();
    let batch = store
        .seal_batch(
            &member("agent:a"),
            &batch_id("b1"),
            &policy(),
            None,
            now + 1_000,
        )
        .await
        .unwrap()
        .expect("the new announcement is due");
    assert_eq!(batch.gaps, vec![expired_gap(1)]);
    assert_eq!(
        batch
            .items
            .iter()
            .map(|item| item.source_key.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["k2"]),
        "only what is still owed"
    );
    assert!(
        batch
            .body
            .contains("Not delivered: 1 announcement in mob \"payments\""),
        "{}",
        batch.body
    );
    for (expected, event) in [
        (BatchStateTag::Sealed, BatchEvent::BeginSubmit),
        (
            BatchStateTag::Submitting,
            BatchEvent::NativeCommitted {
                receipt: receipt("b1"),
            },
        ),
        (BatchStateTag::Committed, applied("b1")),
    ] {
        store
            .transition_batch(&batch.id, expected, event, now + 2_000)
            .await
            .unwrap();
    }
    store
        .announce(
            announce_request("payments", "k3", &["ios"], "later"),
            now + 3_000,
        )
        .await
        .unwrap();
    let next = store
        .seal_batch(
            &member("agent:a"),
            &batch_id("b2"),
            &policy(),
            None,
            now + 5_000,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(next.gaps.is_empty(), "a gap is reported once");

    // A sealed batch nothing native ever started for expires once it was
    // sealed before the window, and the next batch reports what it held.
    let later = now + 5_000 + WINDOW;
    let report = store.expire(later - 1, WINDOW, 100).await.unwrap();
    assert_eq!(report.batches, 0);
    let report = store.expire(later, WINDOW, 100).await.unwrap();
    assert_eq!(report.batches, 1);
    assert_eq!(
        store.batch(&next.id).await.unwrap().unwrap().state,
        BatchState::Expired
    );
    store
        .announce(announce_request("payments", "k4", &["ios"], "after"), later)
        .await
        .unwrap();
    let after = store
        .seal_batch(
            &member("agent:a"),
            &batch_id("b3"),
            &policy(),
            None,
            later + 1_000,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.gaps, vec![expired_gap(1)]);
}

async fn retention_drops_bodies_a_native_held_batch_carries(store: &dyn AnnouncementStore) {
    store
        .put_subscription(
            &op("o1"),
            put("s1", "agent:a", &["ios"], StartFrontier::Now),
            1_000,
        )
        .await
        .unwrap();
    store
        .put_subscription(
            &op("o2"),
            put("s2", "agent:b", &["web"], StartFrontier::Now),
            1_000,
        )
        .await
        .unwrap();
    store
        .announce(announce_request("payments", "k1", &["ios"], "held"), 1_000)
        .await
        .unwrap();
    store
        .announce(
            announce_request("payments", "k2", &["web"], "sealed"),
            1_500,
        )
        .await
        .unwrap();
    let held = store
        .seal_batch(&member("agent:a"), &batch_id("b1"), &policy(), None, 3_000)
        .await
        .unwrap()
        .unwrap();
    store
        .transition_batch(
            &held.id,
            BatchStateTag::Sealed,
            BatchEvent::BeginSubmit,
            3_000,
        )
        .await
        .unwrap();
    store
        .transition_batch(
            &held.id,
            BatchStateTag::Submitting,
            BatchEvent::NativeCommitted {
                receipt: receipt("b1"),
            },
            3_000,
        )
        .await
        .unwrap();
    store
        .seal_batch(&member("agent:b"), &batch_id("b2"), &policy(), None, 3_000)
        .await
        .unwrap()
        .unwrap();

    let report = store.prune(3_000 + WINDOW, WINDOW, 100).await.unwrap();
    assert_eq!(
        report.announcements, 1,
        "a batch the native owner holds carries its own body"
    );
    let k = |key: &str| SourceKey::new(key).unwrap();
    assert!(
        store
            .announcement(&mob("payments"), &k("k1"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .announcement(&mob("payments"), &k("k2"))
            .await
            .unwrap()
            .is_some(),
        "a sealed batch nothing native holds still needs its announcement"
    );
    assert_eq!(
        store.batch(&held.id).await.unwrap().unwrap().items,
        held.items,
        "the held batch keeps what it owes"
    );

    // Once it settles, what it owed and then the batch itself go.
    store
        .transition_batch(
            &held.id,
            BatchStateTag::Committed,
            applied("b1"),
            4_000 + WINDOW,
        )
        .await
        .unwrap();
    let report = store.prune(4_000 + 2 * WINDOW, WINDOW, 100).await.unwrap();
    assert_eq!(report.batches, 1, "{report:?}");
}

#[tokio::test]
async fn a_member_without_an_incarnation_does_not_hold_retention_past_the_window() {
    let native = ScriptedNative::new();
    native.ledger.lock().unwrap().retired = true;
    let harness = harness_with(native);
    harness.subscribe("s1").await;
    harness.announce_burst(1).await;
    harness.clock.store(10_000_000, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let batch = harness.only_batch().await;
    assert_eq!(batch.state.tag(), BatchStateTag::Submitting);
    let retention = AnnouncementConfig::default().retention_ms;

    // Still inside the window from its sealing: it waits.
    harness
        .clock
        .store(batch.sealed_at_ms + retention - 1, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    assert_eq!(
        harness.only_batch().await.state.tag(),
        BatchStateTag::Submitting
    );

    // Past it (and past its next look), nothing native holds it: it expires
    // and the announcement can be pruned.
    let settle = AnnouncementConfig::default().settle_check_max_ms;
    harness
        .clock
        .store(batch.sealed_at_ms + retention + settle, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    assert_eq!(harness.only_batch().await.state, BatchState::Expired);
    let now = harness.clock.load(Ordering::SeqCst);
    let report = harness.store.prune(now, retention, 100).await.unwrap();
    assert_eq!(report.announcements, 1, "{report:?}");

    // Back again: its next batch reports the gap.
    harness.native.ledger.lock().unwrap().retired = false;
    harness
        .service
        .announce(announce_request(
            "payments",
            "back",
            &["ios"],
            "after the gap",
        ))
        .await
        .unwrap();
    harness.clock.fetch_add(60_001, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let batches = harness
        .service
        .deliveries(&BatchQuery {
            member: Some(member("agent:a")),
            limit: 10,
            ..BatchQuery::default()
        })
        .await
        .unwrap();
    let delivered = batches
        .iter()
        .find(|batch| batch.state.tag() == BatchStateTag::Delivered)
        .expect("the member got its next batch");
    assert_eq!(delivered.gaps, vec![expired_gap(1)]);
    assert!(
        delivered.body.contains("Not delivered"),
        "{}",
        delivered.body
    );
}

#[tokio::test]
async fn a_peer_batch_expires_only_once_its_last_request_is_stale() {
    let native = ScriptedNative::new();
    native.ledger.lock().unwrap().retired = true;
    let harness = harness_with(native);
    let peer = Member::new(mob("mob-b"), MemberIdentity::new("agent:p").unwrap());
    harness
        .service
        .put_subscription(
            &op("p1"),
            PutSubscription {
                member: peer.clone(),
                ..put("p1", "agent:a", &["ios"], StartFrontier::Now)
            },
        )
        .await
        .unwrap();
    harness.announce_burst(1).await;
    harness.clock.store(10_000_000, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let query = BatchQuery {
        member: Some(peer.clone()),
        limit: 10,
        ..BatchQuery::default()
    };
    let batch = harness.service.deliveries(&query).await.unwrap().remove(0);
    let retention = AnnouncementConfig::default().retention_ms;
    let window = AnnouncementConfig::default().peer_request_window_ms;

    // A retry just before the window ends: a request may be in flight.
    harness
        .clock
        .store(batch.sealed_at_ms + retention - 1_000, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let submits = harness.native.submits();
    let retried = harness.service.deliveries(&query).await.unwrap().remove(0);
    // Past the window (and past its next look) it neither expires nor is
    // submitted again while that request could still land.
    let settle = AnnouncementConfig::default().settle_check_max_ms;
    assert!(retried.updated_at_ms + settle + 1 > batch.sealed_at_ms + retention);
    harness
        .clock
        .store(retried.updated_at_ms + settle + 1, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let waiting = harness.service.deliveries(&query).await.unwrap().remove(0);
    assert_eq!(waiting.state.tag(), BatchStateTag::Submitting);
    assert_eq!(harness.native.submits(), submits, "no further attempt");
    // Once the peer would refuse that request as stale, it expires.
    harness
        .clock
        .store(retried.updated_at_ms + window + 1, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let expired = harness.service.deliveries(&query).await.unwrap().remove(0);
    assert_eq!(expired.state, BatchState::Expired);
    assert_eq!(harness.native.submits(), submits);
}

#[tokio::test]
async fn a_stranded_batch_whose_announcements_were_pruned_hands_on_a_gap() {
    let mut native = ScriptedNative::new();
    native.apply_on_commit = false;
    let harness = harness_with(native);
    harness.subscribe("s1").await;
    harness.announce_burst(2).await;
    harness.clock.store(10_000_000, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let first = harness.only_batch().await;
    assert_eq!(first.state.tag(), BatchStateTag::Committed);
    let retention = AnnouncementConfig::default().retention_ms;
    let report = harness
        .store
        .prune(10_000_000 + retention, retention, 100)
        .await
        .unwrap();
    assert_eq!(report.announcements, 2, "{report:?}");
    harness
        .native
        .ledger
        .lock()
        .unwrap()
        .strand
        .insert(first.id.to_string());
    harness.round_after_pause().await;
    let stranded = harness.store.batch(&first.id).await.unwrap().unwrap();
    assert_eq!(stranded.state.tag(), BatchStateTag::Stranded);
    let successor = harness
        .store
        .batch(&stranded.successor.expect("a successor"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        successor.gaps,
        vec![BatchGap {
            mob_id: mob("payments"),
            count: 2,
            reason: GapReason::NotRetained,
        }]
    );
    assert!(
        successor.body.contains("no longer retained"),
        "{}",
        successor.body
    );
}

#[tokio::test]
async fn a_backlog_paused_past_the_window_expires_and_the_resume_reports_it() {
    let harness = harness_with(ScriptedNative::new());
    let subscription = harness.subscribe("s1").await;
    harness
        .service
        .change_subscription(
            &op("pause"),
            &sid("s1"),
            subscription.revision,
            SubscriptionChange::Pause,
        )
        .await
        .unwrap();
    harness.announce_burst(3).await;
    let retention = AnnouncementConfig::default().retention_ms;
    let report = harness
        .store
        .expire(1_000 + retention, retention, 100)
        .await
        .unwrap();
    assert_eq!(report.captures, 3);
    harness
        .service
        .change_subscription(
            &op("resume"),
            &sid("s1"),
            subscription.revision + 1,
            SubscriptionChange::Resume,
        )
        .await
        .unwrap();
    harness
        .service
        .announce(announce_request(
            "payments",
            "after",
            &["ios"],
            "after the pause",
        ))
        .await
        .unwrap();
    harness.clock.store(1_000 + retention + 1, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
    let batch = harness.only_batch().await;
    assert_eq!(batch.gaps, vec![expired_gap(3)]);
    assert_eq!(batch.items.len(), 1);
}

#[tokio::test]
async fn this_gateways_own_mob_never_gets_a_lineage_record() {
    let native = ScriptedNative::new();
    native.set_origin(
        OWN_MOB,
        incarnation("own", 1),
        MobOrigin::Unproven("no member created it".into()),
    );
    native.set_origin(
        "refunds",
        incarnation("s3", 30),
        MobOrigin::Unproven("no member created it".into()),
    );
    let harness = harness_with(native);
    // A member of this gateway's own mob names a mob outside its reach: the
    // check derives what it names and the member's own mob, and refuses.
    let refused = harness
        .service
        .put_member_subscription(
            &op("r1"),
            PutSubscription {
                selector: Selector {
                    topics: topics(&["ios"]),
                    mobs: BTreeSet::from([mob("refunds")]),
                },
                ..put("r1", "agent:a", &["ios"], StartFrontier::Now)
            },
        )
        .await;
    assert!(
        matches!(refused, Err(AnnouncementError::UnrelatedMob(_))),
        "{refused:?}"
    );
    harness.service.reconcile_lineage().await.unwrap();
    let recorded = harness
        .store
        .lineage()
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.mob_id.to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        recorded,
        ["refunds"],
        "the root has no record to end or leak"
    );
}
