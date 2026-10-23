//! Rows for escalations, persisted suspend markers, member reads, the status
//! object and the capability seam: the source health a host and its agents
//! can act on.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::Ordering;

use serde_json::{Value, json};

use super::owner::{AnnouncementConfig, AnnouncementService};
use super::store::AnnouncementStore;
use super::tests::{
    Harness, OWN_MOB, SOURCES_ADVANCE, SOURCES_GET, SOURCES_PUT, SOURCES_RECORD_FAILURE,
    ScriptedNative, advance_params, harness_with, member, owed, principal_of, source_params,
    subscribe_to_inbox, try_host_call, upsert, with, with_items,
};
use super::{NativeDelivery, SuspendCause};

const MINUTE: u64 = 60_000;
const HOUR: u64 = 60 * MINUTE;
const DAY: u64 = 24 * HOUR;

const ESCALATIONS_LIST: &str = "meta/mob_announcements/escalations/list";
const MEMBERS_GET: &str = "meta/mob_announcements/members/get";
const MEMBERS_LIST: &str = "meta/mob_announcements/members/list";

/// Source "inbox" as its collector reads it: declared with `policy` as its
/// health policy (or the default when `None`) and live after its baseline,
/// at revision 1, version 1, at the harness clock.
async fn inbox(harness: &Harness, policy: Option<Value>) {
    let mut params = source_params("put-inbox", "inbox", "subject-1", &["mail.received"]);
    if let Some(policy) = policy {
        params = with(params, "health_policy", policy);
    }
    try_host_call(&harness.service, SOURCES_PUT, params)
        .await
        .unwrap();
    let baseline = with(
        advance_params("baseline", 1, 0, "c1"),
        "coverage",
        json!("complete"),
    );
    try_host_call(&harness.service, SOURCES_ADVANCE, baseline)
        .await
        .unwrap();
}

/// The collector of "inbox" at `version`: one successful advance at `at_ms`,
/// observing `observed` and carrying `items`. Returns the next version.
async fn advance(
    harness: &Harness,
    version: u64,
    at_ms: u64,
    observed: Option<Value>,
    items: &[Value],
) -> u64 {
    harness.clock.store(at_ms, Ordering::SeqCst);
    let mut params = advance_params(
        &format!("advance-{version}"),
        1,
        version,
        &format!("c{}", version + 1),
    );
    if let Some(observed) = observed {
        params = with(params, "observed", observed);
    }
    if !items.is_empty() {
        params = with_items(params, items);
    }
    try_host_call(&harness.service, SOURCES_ADVANCE, params)
        .await
        .unwrap();
    version + 1
}

/// One failed collection of "inbox" at `version`, at `at_ms`.
async fn fail(harness: &Harness, version: u64, at_ms: u64, attempt: u32) {
    harness.clock.store(at_ms, Ordering::SeqCst);
    try_host_call(
        &harness.service,
        SOURCES_RECORD_FAILURE,
        json!({
            "operation_id": format!("failure-{version}-{attempt}"),
            "source_id": "inbox",
            "expected_revision": 1,
            "expected_version": version,
            "principal": principal_of("subject-1"),
            "failure": { "class": "provider_unavailable" },
        }),
    )
    .await
    .unwrap();
}

async fn round_at(harness: &Harness, at_ms: u64) {
    harness.clock.store(at_ms, Ordering::SeqCst);
    harness.service.run_round().await.unwrap();
}

/// The open escalations, as `escalations/list` reads them.
async fn open_escalations(harness: &Harness) -> Vec<Value> {
    let listed = try_host_call(&harness.service, ESCALATIONS_LIST, json!({ "open": true }))
        .await
        .unwrap();
    listed["escalations"].as_array().unwrap().clone()
}

/// The open escalations of `cause`.
async fn open_of(harness: &Harness, cause: &str) -> Vec<Value> {
    open_escalations(harness)
        .await
        .into_iter()
        .filter(|escalation| escalation["cause"] == json!(cause))
        .collect()
}

/// The escalation events after `cursor`, and the cursor to read on from.
async fn events_after(harness: &Harness, cursor: u64) -> (Vec<Value>, u64) {
    let listed = try_host_call(
        &harness.service,
        ESCALATIONS_LIST,
        json!({ "after": cursor }),
    )
    .await
    .unwrap();
    (
        listed["events"].as_array().unwrap().clone(),
        listed["next"].as_u64().unwrap(),
    )
}

fn inbox_subject() -> Value {
    json!({ "kind": "source", "mob_id": OWN_MOB, "source_id": "inbox" })
}

async fn health(harness: &Harness) -> Value {
    try_host_call(
        &harness.service,
        SOURCES_GET,
        json!({ "source_id": "inbox" }),
    )
    .await
    .unwrap()["source"]["health"]
        .clone()
}

/// A live source whose advances keep succeeding but carry no item new to it
/// for its quiet window escalates "quiet", dated from the start of the
/// window, and names the evidence class of the window's latest advance.
#[tokio::test]
async fn a_source_succeeding_empty_past_its_quiet_window_escalates() {
    let harness = harness_with(ScriptedNative::new());
    inbox(&harness, Some(json!({ "quiet_after_ms": HOUR }))).await;
    let delta = json!({ "kind": "delta" });
    let mut version = 1;
    for step in 1..=6 {
        version = advance(
            &harness,
            version,
            1_000 + step * 10 * MINUTE,
            Some(delta.clone()),
            &[],
        )
        .await;
    }
    round_at(&harness, 1_000 + 59 * MINUTE).await;
    assert_eq!(
        open_of(&harness, "quiet").await,
        Vec::<Value>::new(),
        "inside the window"
    );
    version = advance(&harness, version, 1_000 + HOUR + MINUTE, Some(delta), &[]).await;
    let _ = version;
    round_at(&harness, 1_000 + HOUR + MINUTE).await;
    let quiet = open_of(&harness, "quiet").await;
    assert_eq!(quiet.len(), 1, "{quiet:?}");
    assert_eq!(quiet[0]["subject"], inbox_subject());
    assert_eq!(quiet[0]["since_ms"], json!(1_000));
    assert_eq!(quiet[0]["evidence"], json!("delta_confirmed"));
    assert_eq!(quiet[0]["renotify_count"], json!(0));
}

/// The first advance that carries an item new to the source resolves its
/// quiet escalation, under the same id.
#[tokio::test]
async fn the_first_new_item_resolves_a_quiet_escalation() {
    let harness = harness_with(ScriptedNative::new());
    inbox(&harness, Some(json!({ "quiet_after_ms": HOUR }))).await;
    let mut version = advance(
        &harness,
        1,
        1_000 + HOUR + MINUTE,
        Some(json!({ "kind": "delta" })),
        &[],
    )
    .await;
    round_at(&harness, 1_000 + HOUR + MINUTE).await;
    let quiet = open_of(&harness, "quiet").await;
    assert_eq!(quiet.len(), 1, "{quiet:?}");
    let id = quiet[0]["escalation_id"].clone();
    version = advance(
        &harness,
        version,
        1_000 + HOUR + 2 * MINUTE,
        Some(json!({ "kind": "delta" })),
        &[upsert("m-1", "v1", "a new message")],
    )
    .await;
    let _ = version;
    round_at(&harness, 1_000 + HOUR + 2 * MINUTE).await;
    assert_eq!(open_of(&harness, "quiet").await, Vec::<Value>::new());
    let (events, _) = events_after(&harness, 0).await;
    let resolved: Vec<&Value> = events
        .iter()
        .filter(|event| event["event"] == json!("resolved"))
        .collect();
    assert_eq!(resolved.len(), 1, "{events:?}");
    assert_eq!(resolved[0]["escalation_id"], id);
    assert_eq!(resolved[0]["cause"], json!("quiet"));
}

/// Replays never count as new: a collector that keeps sending the page it
/// already sent still goes quiet, dated from the last item that was new.
#[tokio::test]
async fn a_collector_replaying_the_same_page_still_escalates_quiet() {
    let harness = harness_with(ScriptedNative::new());
    inbox(&harness, Some(json!({ "quiet_after_ms": HOUR }))).await;
    let page = [upsert("m-1", "v1", "the only message")];
    let first = 1_000 + MINUTE;
    let mut version = advance(&harness, 1, first, None, &page).await;
    for step in 1..=7 {
        version = advance(&harness, version, first + step * 10 * MINUTE, None, &page).await;
    }
    round_at(&harness, first + 70 * MINUTE).await;
    let quiet = open_of(&harness, "quiet").await;
    assert_eq!(quiet.len(), 1, "{quiet:?}");
    assert_eq!(quiet[0]["since_ms"], json!(first));
    assert_eq!(quiet[0]["evidence"], json!("unreported"));
}

/// Without a quiet policy the source reads "quiet detection not configured"
/// and never escalates quiet, however long it stays empty.
#[tokio::test]
async fn a_source_without_quiet_policy_reports_quiet_detection_not_configured() {
    let harness = harness_with(ScriptedNative::new());
    inbox(&harness, None).await;
    let monitoring = health(&harness).await["quiet_monitoring"].clone();
    assert_eq!(
        monitoring,
        json!({ "configured": false, "quiet_after_ms": null, "last_new_item_at_ms": null })
    );
    let mut version = 1;
    for step in 1..=6 {
        version = advance(
            &harness,
            version,
            1_000 + step * 8 * HOUR,
            Some(json!({ "kind": "delta" })),
            &[],
        )
        .await;
        round_at(&harness, 1_000 + step * 8 * HOUR).await;
    }
    assert_eq!(open_of(&harness, "quiet").await, Vec::<Value>::new());
}

/// Without an empty coverage policy the source reads "empty coverage
/// detection not configured" and never escalates it.
#[tokio::test]
async fn a_source_without_empty_policy_reports_empty_coverage_detection_not_configured() {
    let harness = harness_with(ScriptedNative::new());
    inbox(&harness, None).await;
    let empty = json!({ "kind": "snapshot", "records": 0 });
    let mut version = 1;
    for step in 1..=6 {
        version = advance(
            &harness,
            version,
            1_000 + step * 8 * HOUR,
            Some(empty.clone()),
            &[],
        )
        .await;
        round_at(&harness, 1_000 + step * 8 * HOUR).await;
    }
    let monitoring = health(&harness).await["empty_coverage_monitoring"].clone();
    assert_eq!(
        monitoring,
        json!({
            "configured": false,
            "empty_after_ms": null,
            "empty_snapshot_since_ms": 1_000 + 8 * HOUR,
        })
    );
    assert_eq!(
        open_of(&harness, "empty_coverage").await,
        Vec::<Value>::new()
    );
}

/// A spell of empty snapshots escalates "empty_coverage" after its window,
/// inside a longer quiet window that has not passed.
#[tokio::test]
async fn an_empty_snapshot_spell_escalates_inside_the_quiet_window() {
    let harness = harness_with(ScriptedNative::new());
    inbox(
        &harness,
        Some(json!({ "quiet_after_ms": 4 * HOUR, "empty_after_ms": HOUR })),
    )
    .await;
    let empty = json!({ "kind": "snapshot", "records": 0 });
    let first = 1_000 + 10 * MINUTE;
    let mut version = advance(&harness, 1, first, Some(empty.clone()), &[]).await;
    for step in 1..=7 {
        version = advance(
            &harness,
            version,
            first + step * 10 * MINUTE,
            Some(empty.clone()),
            &[],
        )
        .await;
    }
    round_at(&harness, first + 70 * MINUTE).await;
    let empty_coverage = open_of(&harness, "empty_coverage").await;
    assert_eq!(empty_coverage.len(), 1, "{empty_coverage:?}");
    assert_eq!(empty_coverage[0]["since_ms"], json!(first));
    assert_eq!(empty_coverage[0]["subject"], inbox_subject());
    assert_eq!(open_of(&harness, "quiet").await, Vec::<Value>::new());
    let monitoring = health(&harness).await["empty_coverage_monitoring"].clone();
    assert_eq!(
        monitoring,
        json!({ "configured": true, "empty_after_ms": HOUR, "empty_snapshot_since_ms": first })
    );
}

/// A delta that confirms nothing changed is a real "no change": it never
/// raises empty coverage, however long it lasts.
#[tokio::test]
async fn a_confirmed_empty_delta_never_raises_empty_coverage() {
    let harness = harness_with(ScriptedNative::new());
    inbox(&harness, Some(json!({ "empty_after_ms": HOUR }))).await;
    let mut version = 1;
    for step in 1..=12 {
        version = advance(
            &harness,
            version,
            1_000 + step * 20 * MINUTE,
            Some(json!({ "kind": "delta" })),
            &[],
        )
        .await;
        round_at(&harness, 1_000 + step * 20 * MINUTE).await;
    }
    assert_eq!(
        open_of(&harness, "empty_coverage").await,
        Vec::<Value>::new()
    );
}

/// Three failed collections escalate "failure_streak" in the next round,
/// and the next success resolves it under the same id.
#[tokio::test]
async fn a_failure_streak_escalates_within_its_bound_and_resolves_on_success() {
    let harness = harness_with(ScriptedNative::new());
    inbox(&harness, None).await;
    fail(&harness, 1, 1_000 + MINUTE, 1).await;
    fail(&harness, 1, 1_000 + 2 * MINUTE, 2).await;
    round_at(&harness, 1_000 + 2 * MINUTE).await;
    assert_eq!(
        open_of(&harness, "failure_streak").await,
        Vec::<Value>::new(),
        "two failures"
    );
    fail(&harness, 1, 1_000 + 3 * MINUTE, 3).await;
    round_at(&harness, 1_000 + 3 * MINUTE).await;
    let streak = open_of(&harness, "failure_streak").await;
    assert_eq!(streak.len(), 1, "{streak:?}");
    assert_eq!(streak[0]["subject"], inbox_subject());
    assert_eq!(streak[0]["since_ms"], json!(1_000));
    let id = streak[0]["escalation_id"].clone();
    advance(&harness, 1, 1_000 + 4 * MINUTE, None, &[]).await;
    round_at(&harness, 1_000 + 4 * MINUTE).await;
    assert_eq!(
        open_of(&harness, "failure_streak").await,
        Vec::<Value>::new()
    );
    let (events, _) = events_after(&harness, 0).await;
    let kinds: Vec<(Value, Value)> = events
        .iter()
        .filter(|event| event["escalation_id"] == id)
        .map(|event| (event["event"].clone(), event["cause"].clone()))
        .collect();
    assert_eq!(
        kinds,
        vec![
            (json!("raised"), json!("failure_streak")),
            (json!("resolved"), json!("failure_streak")),
        ]
    );
}

/// An active source with no advance for a day escalates "stale": its
/// collector is not running.
#[tokio::test]
async fn a_stale_source_escalates_when_no_collector_runs() {
    let harness = harness_with(ScriptedNative::new());
    inbox(&harness, None).await;
    round_at(&harness, 1_000 + DAY - MINUTE).await;
    assert_eq!(open_of(&harness, "stale").await, Vec::<Value>::new());
    round_at(&harness, 1_000 + DAY + MINUTE).await;
    let stale = open_of(&harness, "stale").await;
    assert_eq!(stale.len(), 1, "{stale:?}");
    assert_eq!(stale[0]["subject"], inbox_subject());
    assert_eq!(stale[0]["since_ms"], json!(1_000));
}

/// A member's suspend marker is persisted: after a restart the same wait
/// keeps the time it was first seen, and members/get reads it.
#[tokio::test]
async fn persisted_suspend_markers_keep_their_since_across_a_restart() {
    let harness = harness_with(ScriptedNative::new());
    harness.subscribe("s1").await;
    harness.announce_burst(2).await;
    harness
        .native
        .waiting
        .lock()
        .unwrap()
        .insert("agent:a".into(), SuspendCause::MemberUnknown);
    round_at(&harness, 10_000_000).await;
    let first = harness
        .service
        .suspend_marker(&member("agent:a"))
        .expect("the wait is marked");
    assert_eq!(first.since_ms, 10_000_000);

    // A restart: a new owner over the same store and native.
    let clock = Arc::clone(&harness.clock);
    clock.store(20_000_000, Ordering::SeqCst);
    let restarted = AnnouncementService::with_clock(
        Arc::clone(&harness.store) as Arc<dyn AnnouncementStore>,
        Arc::clone(&harness.native) as Arc<dyn NativeDelivery>,
        harness.service.config().clone(),
        Arc::new(move || clock.load(Ordering::SeqCst)),
    )
    .unwrap();
    let read = try_host_call(
        &restarted,
        MEMBERS_GET,
        json!({ "member": { "mob_id": OWN_MOB, "identity": "agent:a" } }),
    )
    .await
    .unwrap();
    assert_eq!(read["marker"]["since_ms"], json!(10_000_000), "{read}");
    restarted.run_round().await.unwrap();
    let marker = restarted
        .suspend_marker(&member("agent:a"))
        .expect("the same wait is marked again");
    assert_eq!(marker.since_ms, 10_000_000, "since survives the restart");
}

/// An open escalation is notified again every day, under the same id with a
/// higher count, until it resolves; then it is notified no more.
#[tokio::test]
async fn an_unresolved_escalation_renotifies_every_day_until_it_resolves() {
    let harness = harness_with(ScriptedNative::new());
    inbox(&harness, None).await;
    for attempt in 1..=3 {
        fail(&harness, 1, 1_000 + u64::from(attempt) * MINUTE, attempt).await;
    }
    round_at(&harness, 1_000 + 3 * MINUTE).await;
    let id = open_of(&harness, "failure_streak").await[0]["escalation_id"].clone();
    round_at(&harness, 1_000 + 3 * MINUTE + DAY + MINUTE).await;
    round_at(&harness, 1_000 + 3 * MINUTE + 2 * DAY + 2 * MINUTE).await;
    let open = open_of(&harness, "failure_streak").await;
    assert_eq!(open[0]["escalation_id"], id);
    assert_eq!(open[0]["renotify_count"], json!(2));
    advance(
        &harness,
        1,
        1_000 + 3 * MINUTE + 2 * DAY + 3 * MINUTE,
        None,
        &[],
    )
    .await;
    round_at(&harness, 1_000 + 3 * MINUTE + 2 * DAY + 3 * MINUTE).await;
    round_at(&harness, 1_000 + 3 * MINUTE + 4 * DAY).await;
    let (events, _) = events_after(&harness, 0).await;
    let of_id: Vec<(Value, Value)> = events
        .iter()
        .filter(|event| event["escalation_id"] == id)
        .map(|event| (event["event"].clone(), event["renotify_count"].clone()))
        .collect();
    assert_eq!(
        of_id,
        vec![
            (json!("raised"), json!(0)),
            (json!("renotified"), json!(1)),
            (json!("renotified"), json!(2)),
            (json!("resolved"), json!(2)),
        ]
    );
}

/// A host that reconnects reads every escalation event after its last
/// cursor, and nothing it read before; a host away longer reads the open
/// set.
#[tokio::test]
async fn escalation_events_are_read_back_from_a_cursor_after_a_reconnect() {
    let harness = harness_with(ScriptedNative::new());
    inbox(&harness, None).await;
    for attempt in 1..=3 {
        fail(&harness, 1, 1_000 + u64::from(attempt) * MINUTE, attempt).await;
    }
    round_at(&harness, 1_000 + 3 * MINUTE).await;
    let (first, cursor) = events_after(&harness, 0).await;
    assert_eq!(first.len(), 1, "{first:?}");
    assert_eq!(first[0]["event"], json!("raised"));
    assert_eq!(first[0]["cursor"].as_u64(), Some(cursor));
    assert_eq!(open_escalations(&harness).await.len(), 1);

    // The host is away while the escalation resolves.
    advance(&harness, 1, 1_000 + 4 * MINUTE, None, &[]).await;
    round_at(&harness, 1_000 + 4 * MINUTE).await;
    let (missed, next) = events_after(&harness, cursor).await;
    assert_eq!(missed.len(), 1, "{missed:?}");
    assert_eq!(missed[0]["event"], json!("resolved"));
    assert_eq!(missed[0]["escalation_id"], first[0]["escalation_id"]);
    assert!(next > cursor);
    let (none, same) = events_after(&harness, next).await;
    assert_eq!(none, Vec::<Value>::new());
    assert_eq!(same, next);
    assert_eq!(open_escalations(&harness).await, Vec::<Value>::new());
}

/// members/get reads a member's backlog, its persisted marker and its
/// escalations; a marker older than an hour escalates "suspended", and
/// members/list finds the member, also among the escalated ones.
#[tokio::test]
async fn members_get_and_list_read_backlog_marker_and_escalations() {
    let harness = harness_with(ScriptedNative::new());
    harness.subscribe("s1").await;
    harness.announce_burst(3).await;
    harness
        .native
        .waiting
        .lock()
        .unwrap()
        .insert("agent:a".into(), SuspendCause::MemberUnknown);
    round_at(&harness, 10_000_000).await;
    let wire_member = json!({ "mob_id": OWN_MOB, "identity": "agent:a" });
    let read = try_host_call(
        &harness.service,
        MEMBERS_GET,
        json!({ "member": wire_member }),
    )
    .await
    .unwrap();
    assert_eq!(read["member"], wire_member);
    assert_eq!(
        read["marker"]["cause"]["kind"],
        json!("member_unknown"),
        "{read}"
    );
    assert_eq!(read["marker"]["since_ms"], json!(10_000_000));
    assert!(read["backlog"].is_object(), "{read}");
    assert_eq!(read["escalations"], json!([]));

    round_at(&harness, 10_000_000 + HOUR + MINUTE).await;
    let read = try_host_call(
        &harness.service,
        MEMBERS_GET,
        json!({ "member": wire_member }),
    )
    .await
    .unwrap();
    let escalations = read["escalations"].as_array().unwrap();
    assert_eq!(escalations.len(), 1, "{read}");
    assert_eq!(escalations[0]["cause"], json!("suspended"));
    assert_eq!(
        escalations[0]["subject"],
        json!({ "kind": "member", "member": wire_member })
    );
    assert_eq!(escalations[0]["since_ms"], json!(10_000_000));
    for params in [json!({}), json!({ "escalated": true })] {
        let listed = try_host_call(&harness.service, MEMBERS_LIST, params.clone())
            .await
            .unwrap();
        let members: Vec<Value> = listed["members"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["member"].clone())
            .collect();
        assert_eq!(members, vec![wire_member.clone()], "{params}: {listed}");
    }
}

/// meta/status's mob announcement object counts sources by state, members
/// suspended, and open escalations with the last event cursor.
#[tokio::test]
async fn the_status_object_counts_sources_members_and_escalations() {
    let harness = harness_with(ScriptedNative::new());
    inbox(&harness, None).await;
    for attempt in 1..=3 {
        fail(&harness, 1, 1_000 + u64::from(attempt) * MINUTE, attempt).await;
    }
    round_at(&harness, 1_000 + 3 * MINUTE).await;
    let (_, cursor) = events_after(&harness, 0).await;
    let status = super::rpc::status_projection(&harness.service).await;
    assert_eq!(
        status,
        json!({
            "sources": { "active": 1, "paused": 0, "failing": 1, "stale": 0 },
            "members": { "suspended": 0, "unhandled_held": 0 },
            "escalations": { "open": 1, "last_event_cursor": cursor },
        })
    );
}

/// meta/capabilities names source support and reads local delivery from the
/// same fact the subscription check reads.
#[tokio::test]
async fn the_capability_seam_names_sources_v1_and_local_delivery() {
    assert!(
        super::rpc::announcement_feature_capabilities()
            .contains(&super::rpc::SOURCES_FEATURE_CAPABILITY)
    );
    assert_eq!(
        super::rpc::SOURCES_FEATURE_CAPABILITY,
        "mob_announcements.sources.v1"
    );
    assert_eq!(super::rpc::capabilities_projection(None).await, Value::Null);
    let ready = harness_with(ScriptedNative::new());
    assert_eq!(
        super::rpc::capabilities_projection(Some(&ready.service)).await,
        json!({
            "sources": "v1",
            "local_delivery": { "kind": "ready" },
            "governed_admission": false,
        })
    );
    let mut governed = ScriptedNative::new();
    governed.governed = true;
    let governed = harness_with(governed);
    assert_eq!(
        super::rpc::capabilities_projection(Some(&governed.service)).await["local_delivery"],
        json!({ "kind": "refused", "cause": "governed_runtime" })
    );
}

/// sources/put answers how many live subscriptions the source's audience
/// and routes now bind, and whether this gateway can deliver locally.
#[tokio::test]
async fn sources_put_reports_affected_subscriptions_and_delivery_readiness() {
    let harness = harness_with(ScriptedNative::new());
    let created = try_host_call(
        &harness.service,
        SOURCES_PUT,
        source_params("put-1", "inbox", "subject-1", &["mail.received"]),
    )
    .await
    .unwrap();
    assert_eq!(created["affected_subscriptions"], json!(0), "{created}");
    assert_eq!(created["delivery_readiness"], json!({ "kind": "ready" }));
    subscribe_to_inbox(&harness.service, "sub-a", "agent:a").await;
    let replaced = try_host_call(
        &harness.service,
        SOURCES_PUT,
        with(
            source_params("put-2", "inbox", "subject-1", &["mail.received"]),
            "expected_revision",
            json!(1),
        ),
    )
    .await
    .unwrap();
    assert_eq!(replaced["affected_subscriptions"], json!(1), "{replaced}");
}

/// A source whose runner this gateway does not schedule (no schedule service
/// installed its runners) shows it in its health, so an operator does not
/// wait on a collector that never runs here.
#[tokio::test]
async fn a_runner_this_gateway_does_not_schedule_shows_in_the_source_health() {
    let harness = harness_with(ScriptedNative::new());
    try_host_call(
        &harness.service,
        SOURCES_PUT,
        with(
            source_params("put-1", "inbox", "subject-1", &["mail.received"]),
            "runner",
            json!({ "host_runnable": "collect-inbox", "every_seconds": 300 }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        health(&harness).await["runner_scheduled_here"],
        json!(false)
    );
    let other = harness_with(ScriptedNative::new());
    inbox(&other, None).await;
    assert_eq!(
        health(&other).await["runner_scheduled_here"],
        Value::Null,
        "no runner, nothing to schedule"
    );
}

/// A source escalation is announced to the agents its audience covers, on
/// the reserved health topic, with a typed body; a member outside the
/// audience subscribed to the same topic captures nothing.
#[tokio::test]
async fn a_source_escalation_is_announced_on_the_health_topic_to_its_audience() {
    let mut native = ScriptedNative::new();
    native.known.insert("agent:b".into());
    let harness = harness_with(native);
    inbox(&harness, None).await;
    for (id, identity) in [("health-a", "agent:a"), ("health-b", "agent:b")] {
        try_host_call(
            &harness.service,
            super::tests::SUBSCRIPTIONS_PUT,
            json!({
                "operation_id": format!("subscribe-{id}"),
                "subscription_id": id,
                "member": { "mob_id": OWN_MOB, "identity": identity },
                "topics": ["mob_announcements.health"],
            }),
        )
        .await
        .unwrap();
    }
    for attempt in 1..=3 {
        fail(&harness, 1, 1_000 + u64::from(attempt) * MINUTE, attempt).await;
    }
    round_at(&harness, 1_000 + 3 * MINUTE).await;
    let owed = owed(&harness).await;
    assert_eq!(owed.get("agent:a"), Some(&1), "{owed:?}");
    assert_eq!(owed.get("agent:b"), None, "{owed:?}");
}

/// Every escalation event reaches the host's error sink as it is written:
/// raised as MobAnnouncementEscalated, resolved as
/// MobAnnouncementEscalationResolved, both under one id and with the event
/// cursor.
#[tokio::test]
async fn escalation_events_reach_the_error_sink_with_their_cursor() {
    let harness = harness_with(ScriptedNative::new());
    let seen: Arc<std::sync::Mutex<Vec<Value>>> = Arc::default();
    let sink = Arc::clone(&seen);
    harness
        .service
        .set_error_sink(Arc::new(move |event: crate::ErrorEvent| {
            sink.lock()
                .unwrap()
                .push(serde_json::to_value(&event).unwrap());
        }));
    inbox(&harness, None).await;
    for attempt in 1..=3 {
        fail(&harness, 1, 1_000 + u64::from(attempt) * MINUTE, attempt).await;
    }
    round_at(&harness, 1_000 + 3 * MINUTE).await;
    advance(&harness, 1, 1_000 + 4 * MINUTE, None, &[]).await;
    round_at(&harness, 1_000 + 4 * MINUTE).await;
    let seen = seen.lock().unwrap().clone();
    let categories: Vec<Value> = seen.iter().map(|event| event["category"].clone()).collect();
    assert_eq!(
        categories,
        vec![
            json!("mob_announcement_escalated"),
            json!("mob_announcement_escalation_resolved"),
        ],
        "{seen:?}"
    );
    assert_eq!(
        seen[0]["escalation"]["escalation_id"],
        seen[1]["escalation_id"]
    );
    assert_eq!(seen[0]["escalation"]["cause"], json!("failure_streak"));
    assert!(seen[1]["cursor"].as_u64() > seen[0]["cursor"].as_u64());
}

/// The defaults the owner runs with: a day between notifications of an open
/// escalation, and no health audience for member escalations.
#[test]
fn escalation_options_default_to_a_day_and_no_health_audience() {
    let config = AnnouncementConfig::default();
    assert_eq!(config.renotify_after_ms, DAY);
    assert!(config.health_audience.is_empty());
    let parsed = super::parse_options(
        &json!({
            "enabled": true,
            "renotify_after_ms": 2 * DAY,
            "health_audience": [
                { "kind": "member", "member": { "mob_id": OWN_MOB, "identity": "agent:a" } }
            ],
        }),
        "mob_announcements",
    )
    .unwrap()
    .unwrap();
    assert_eq!(parsed.renotify_after_ms, 2 * DAY);
    assert_eq!(parsed.health_audience.len(), 1);
}
