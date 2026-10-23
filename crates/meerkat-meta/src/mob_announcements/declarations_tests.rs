//! Declarations and role-selected subscriptions: what a host declares is
//! reconciled into the records it manages and nothing else, and a role
//! template captures for the members that hold its role when an
//! announcement is made.

use super::*;

const DECLARATIONS_APPLY: &str = "meta/mob_announcements/declarations/apply";
const DECLARATIONS_GET: &str = "meta/mob_announcements/declarations/get";
const SUBSCRIPTIONS_GET: &str = "meta/mob_announcements/subscriptions/get";

/// Apply `declarations` at `revision`, keeping a refusal.
async fn try_apply(
    service: &AnnouncementService,
    revision: u64,
    sources: serde_json::Value,
    subscriptions: serde_json::Value,
) -> std::result::Result<serde_json::Value, crate::rpc::JsonRpcError> {
    try_host_call(
        service,
        DECLARATIONS_APPLY,
        serde_json::json!({
            "revision": revision,
            "declarations": {
                "revision": revision,
                "sources": sources,
                "subscriptions": subscriptions,
            },
        }),
    )
    .await
}

/// What `declarations/get` reports.
async fn declared(service: &AnnouncementService) -> serde_json::Value {
    try_host_call(service, DECLARATIONS_GET, serde_json::json!({}))
        .await
        .unwrap()
}

/// The record of `kind` and `id` in a `declarations/get` result.
fn record<'a>(view: &'a serde_json::Value, kind: &str, id: &str) -> Option<&'a serde_json::Value> {
    view["records"]
        .as_array()?
        .iter()
        .find(|record| record["kind"] == kind && record["id"] == id)
}

/// The role children of template `template` a `declarations/get` result
/// lists.
fn role_children(view: &serde_json::Value, template: &str) -> Vec<serde_json::Value> {
    let prefix = format!("{template}/");
    view["records"]
        .as_array()
        .map(|records| {
            records
                .iter()
                .filter(|record| {
                    record["kind"] == "role_child"
                        && record["id"]
                            .as_str()
                            .is_some_and(|id| id.starts_with(&prefix))
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

/// A subscription's state, or `None` when no subscription has the id.
async fn subscription_state(service: &AnnouncementService, id: &str) -> Option<String> {
    let found = try_host_call(
        service,
        SUBSCRIPTIONS_GET,
        serde_json::json!({ "subscription_id": id }),
    )
    .await
    .unwrap();
    found["subscription"]["state"].as_str().map(str::to_string)
}

/// A declared source: "inbox" on "mail.received" with this audience and a
/// direct route.
fn declared_inbox(audience: serde_json::Value) -> serde_json::Value {
    let mut source = with(
        with(
            source_params("unused", "inbox", "subject-1", &["mail.received"]),
            "audience",
            audience,
        ),
        "routes",
        serde_json::json!([{ "kind": "direct" }]),
    );
    source.as_object_mut().unwrap().remove("operation_id");
    source
}

/// A declared subscription of this mob's member `identity` to `topics`.
fn declared_subscription(id: &str, identity: &str, topics: &[&str]) -> serde_json::Value {
    serde_json::json!({
        "subscription_id": id,
        "member": { "mob_id": OWN_MOB, "identity": identity },
        "topics": topics,
        "start": { "kind": "now" },
    })
}

/// A declared template: every member of this mob with `role`, on `topics`.
fn declared_template(id: &str, role: &str, topics: &[&str]) -> serde_json::Value {
    serde_json::json!({
        "subscription_id": id,
        "members": { "mob_id": OWN_MOB, "role": role },
        "topics": topics,
        "start": { "kind": "now" },
    })
}

/// The host announces `key` on `topics` into this gateway's mob.
async fn host_announces(service: &AnnouncementService, key: &str, topics: &[&str]) {
    try_host_call(
        service,
        ANNOUNCE,
        serde_json::json!({ "source_key": key, "topics": topics, "body": key }),
    )
    .await
    .unwrap();
}

/// A declared subscription whose member its source's audience does not
/// cover is never made: the record is pending with the audience's refusal,
/// and nothing captures for the member.
#[tokio::test]
async fn a_declared_subscription_outside_its_source_audience_is_refused() {
    let harness = harness_with(native_serving(&["agent:b"]));
    let service = &harness.service;
    try_apply(
        service,
        1,
        serde_json::json!([declared_inbox(serde_json::json!([member_grant("agent:a")]))]),
        serde_json::json!([declared_subscription(
            "b-mail",
            "agent:b",
            &["mail.received"]
        )]),
    )
    .await
    .unwrap();
    let view = declared(service).await;
    assert_eq!(
        record(&view, "source", "inbox").map(|record| &record["status"]),
        Some(&serde_json::json!("applied")),
        "{view}"
    );
    let refused = record(&view, "subscription", "b-mail")
        .unwrap_or_else(|| panic!("the declared subscription is listed: {view}"));
    assert_eq!(refused["status"], "pending", "{view}");
    assert_eq!(refused["refusal"]["kind"], "audience_not_covered", "{view}");
    assert_eq!(subscription_state(service, "b-mail").await, None);
}

/// A role template takes a member that gets the role after the template
/// was declared: its first matching announcement is captured for it, under
/// a child named for the template and the member, and a member without
/// the role captures nothing.
#[tokio::test]
async fn a_role_template_captures_for_a_member_spawned_after_it() {
    let harness = harness_with(native_serving(&["agent:b", "agent:c"]));
    let service = &harness.service;
    try_apply(
        service,
        1,
        serde_json::json!([]),
        serde_json::json!([declared_template("reviewers", "reviewer", &["deploy.done"])]),
    )
    .await
    .unwrap();
    // The member is spawned with the role after the template.
    harness.native.set_roster_facts("agent:b", "reviewer", &[]);
    harness.native.set_roster_facts("agent:c", "author", &[]);
    host_announces(service, "deploy-1", &["deploy.done"]).await;
    let owed_now = owed(&harness).await;
    assert_eq!(owed_now.get("agent:b"), Some(&1), "{owed_now:?}");
    assert_eq!(owed_now.get("agent:c"), None, "{owed_now:?}");
    let view = declared(service).await;
    assert_eq!(
        record(&view, "subscription", "reviewers").map(|record| &record["status"]),
        Some(&serde_json::json!("applied")),
        "{view}"
    );
    let children = role_children(&view, "reviewers");
    assert_eq!(children.len(), 1, "{view}");
    let child = children[0]["id"].as_str().unwrap().to_string();
    let digest = child.strip_prefix("reviewers/").unwrap();
    assert!(
        digest.len() == 16 && digest.chars().all(|c| c.is_ascii_hexdigit()),
        "{child}"
    );
    assert_eq!(children[0]["status"], "applied", "{view}");
    assert_eq!(
        subscription_state(service, &child).await.as_deref(),
        Some("active")
    );
}

/// A member that loses the role stops capturing at once, keeps what it
/// already owes, and its child is cancelled at the next reconcile.
#[tokio::test]
async fn a_member_that_leaves_the_role_stops_capturing() {
    let harness = harness_with(native_serving(&["agent:b"]));
    let service = &harness.service;
    harness.native.set_roster_facts("agent:b", "reviewer", &[]);
    try_apply(
        service,
        1,
        serde_json::json!([]),
        serde_json::json!([declared_template("reviewers", "reviewer", &["deploy.done"])]),
    )
    .await
    .unwrap();
    host_announces(service, "deploy-1", &["deploy.done"]).await;
    assert_eq!(owed(&harness).await.get("agent:b"), Some(&1));
    let child = role_children(&declared(service).await, "reviewers")[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    // The member is respawned under another role.
    harness.native.set_roster_facts("agent:b", "author", &[]);
    host_announces(service, "deploy-2", &["deploy.done"]).await;
    assert_eq!(
        owed(&harness).await.get("agent:b"),
        Some(&1),
        "the second announcement is not captured; the first is still owed"
    );
    service.reconcile_mobs().await.unwrap();
    assert_eq!(
        subscription_state(service, &child).await.as_deref(),
        Some("cancelled")
    );
    assert!(role_children(&declared(service).await, "reviewers").is_empty());
    assert_eq!(owed(&harness).await.get("agent:b"), Some(&1));
}

/// The reconciler changes only the records a declaration made: a record
/// dropped from the declaration is cancelled, and a subscription made at
/// runtime is never touched.
#[tokio::test]
async fn declarations_reconcile_only_what_they_manage() {
    let harness = harness_with(native_serving(&["agent:b"]));
    let service = &harness.service;
    try_subscribe(service, "made-at-runtime", "agent:a", &["ops.runtime"])
        .await
        .unwrap();
    try_apply(
        service,
        1,
        serde_json::json!([]),
        serde_json::json!([
            declared_subscription("declared-a", "agent:a", &["ops.declared"]),
            declared_subscription("declared-b", "agent:b", &["ops.declared"]),
        ]),
    )
    .await
    .unwrap();
    assert_eq!(
        subscription_state(service, "declared-a").await.as_deref(),
        Some("active")
    );
    try_apply(
        service,
        2,
        serde_json::json!([]),
        serde_json::json!([declared_subscription(
            "declared-b",
            "agent:b",
            &["ops.declared"]
        )]),
    )
    .await
    .unwrap();
    assert_eq!(
        subscription_state(service, "declared-a").await.as_deref(),
        Some("cancelled"),
        "a record dropped from the declaration is cancelled"
    );
    assert_eq!(
        subscription_state(service, "declared-b").await.as_deref(),
        Some("active")
    );
    assert_eq!(
        subscription_state(service, "made-at-runtime")
            .await
            .as_deref(),
        Some("active"),
        "a runtime record is never touched"
    );
    let view = declared(service).await;
    assert!(
        record(&view, "subscription", "declared-a").is_none(),
        "{view}"
    );
    assert!(
        record(&view, "subscription", "made-at-runtime").is_none(),
        "{view}"
    );
    assert_eq!(view["revision"], 2, "{view}");
}

/// A lower revision is refused as a rollback, the applied revision with
/// other declarations as a digest conflict, and the applied revision with
/// the same declarations is a no-op.
#[tokio::test]
async fn a_declaration_rollback_or_digest_conflict_is_refused() {
    let harness = harness_with(native_serving(&["agent:b"]));
    let service = &harness.service;
    let first = serde_json::json!([declared_subscription("declared-a", "agent:a", &["ops.one"])]);
    try_apply(service, 2, serde_json::json!([]), first.clone())
        .await
        .unwrap();
    let digest = declared(service).await["digest"].clone();
    assert!(
        digest.as_str().is_some_and(|digest| !digest.is_empty()),
        "{digest}"
    );
    let rollback = try_apply(service, 1, serde_json::json!([]), first.clone())
        .await
        .unwrap_err();
    assert_eq!(
        rollback.data,
        Some(serde_json::json!({ "kind": "declaration_rollback", "applied": 2 }))
    );
    let conflict = try_apply(
        service,
        2,
        serde_json::json!([]),
        serde_json::json!([declared_subscription("declared-a", "agent:a", &["ops.two"])]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        conflict.data,
        Some(serde_json::json!({ "kind": "declaration_digest_conflict", "revision": 2 }))
    );
    try_apply(service, 2, serde_json::json!([]), first)
        .await
        .unwrap();
    let view = declared(service).await;
    assert_eq!(view["revision"], 2, "{view}");
    assert_eq!(view["digest"], digest, "{view}");
}

/// A managed subscription whose declaration changed is replaced in place
/// at its revision: what it captured before stays owed, the new selector
/// captures from now, and the old one captures no more.
#[tokio::test]
async fn a_replaced_managed_subscription_keeps_what_it_owes() {
    let harness = harness_with(native_serving(&[]));
    let service = &harness.service;
    try_apply(
        service,
        1,
        serde_json::json!([]),
        serde_json::json!([declared_subscription("declared-a", "agent:a", &["ops.one"])]),
    )
    .await
    .unwrap();
    host_announces(service, "one-1", &["ops.one"]).await;
    assert_eq!(owed(&harness).await.get("agent:a"), Some(&1));
    try_apply(
        service,
        2,
        serde_json::json!([]),
        serde_json::json!([declared_subscription("declared-a", "agent:a", &["ops.two"])]),
    )
    .await
    .unwrap();
    let found = try_host_call(
        service,
        SUBSCRIPTIONS_GET,
        serde_json::json!({ "subscription_id": "declared-a" }),
    )
    .await
    .unwrap();
    assert_eq!(found["subscription"]["revision"], 2, "{found}");
    assert_eq!(found["subscription"]["state"], "active", "{found}");
    assert_eq!(owed(&harness).await.get("agent:a"), Some(&1), "still owed");
    host_announces(service, "two-1", &["ops.two"]).await;
    host_announces(service, "one-2", &["ops.one"]).await;
    assert_eq!(
        owed(&harness).await.get("agent:a"),
        Some(&2),
        "the new selector captures; the old one no more"
    );
}

/// A declared subscription refused for a runtime reason is pending and is
/// retried at the next trigger: once its member is on the roster with the
/// role the audience grants, it is made.
#[tokio::test]
async fn a_pending_declared_subscription_is_retried_when_its_member_registers() {
    let harness = harness_with(native_serving(&["agent:b"]));
    let service = &harness.service;
    let audience = serde_json::json!([{ "kind": "role", "mob_id": OWN_MOB, "role": "triager" }]);
    try_apply(
        service,
        1,
        serde_json::json!([declared_inbox(audience)]),
        serde_json::json!([declared_subscription(
            "b-mail",
            "agent:b",
            &["mail.received"]
        )]),
    )
    .await
    .unwrap();
    let view = declared(service).await;
    let pending = record(&view, "subscription", "b-mail")
        .unwrap_or_else(|| panic!("the declared subscription is listed: {view}"));
    assert_eq!(pending["status"], "pending", "{view}");
    assert_eq!(subscription_state(service, "b-mail").await, None);
    // The member registers on the roster with the role.
    harness.native.set_roster_facts("agent:b", "triager", &[]);
    service.reconcile_mobs().await.unwrap();
    let view = declared(service).await;
    assert_eq!(
        record(&view, "subscription", "b-mail").map(|record| &record["status"]),
        Some(&serde_json::json!("applied")),
        "{view}"
    );
    assert_eq!(
        subscription_state(service, "b-mail").await.as_deref(),
        Some("active")
    );
}
