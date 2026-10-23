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

/// A member that loses the role stops capturing at once and keeps what it
/// already owes, which is delivered: its child stays active, capturing
/// nothing, and leaving the role never cancels it. A member given the role
/// again captures again, under the same child.
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
    assert_eq!(owed(&harness).await.get("agent:b"), Some(&1));
    // What it owes is delivered.
    harness.clock.store(10_000_000, Ordering::SeqCst);
    service.run_round().await.unwrap();
    let delivered = service
        .deliveries(&BatchQuery {
            member: Some(member("agent:b")),
            limit: 10,
            ..BatchQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(delivered.len(), 1, "{delivered:?}");
    assert_eq!(delivered[0].items.len(), 1, "{delivered:?}");
    assert_eq!(
        delivered[0].state.tag(),
        BatchStateTag::Delivered,
        "{delivered:?}"
    );
    service.reconcile_mobs().await.unwrap();
    assert_eq!(
        subscription_state(service, &child).await.as_deref(),
        Some("active"),
        "leaving the role never cancels the child"
    );
    let children = role_children(&declared(service).await, "reviewers");
    assert_eq!(children.len(), 1, "{children:?}");
    assert_eq!(children[0]["status"], "left", "{children:?}");
    host_announces(service, "deploy-3", &["deploy.done"]).await;
    assert_eq!(
        owed(&harness).await.get("agent:b"),
        None,
        "a member without the role captures nothing"
    );
    // The member is respawned with the role again.
    harness.native.set_roster_facts("agent:b", "reviewer", &[]);
    host_announces(service, "deploy-4", &["deploy.done"]).await;
    assert_eq!(owed(&harness).await.get("agent:b"), Some(&1));
    let children = role_children(&declared(service).await, "reviewers");
    assert_eq!(children.len(), 1, "{children:?}");
    assert_eq!(children[0]["id"], child.as_str());
    assert_eq!(children[0]["status"], "applied", "{children:?}");
    assert_eq!(
        subscription_state(service, &child).await.as_deref(),
        Some("active")
    );
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

/// An apply whose declarations the static checks refuse is refused whole:
/// nothing it declares is made, nothing it drops is cancelled, and the
/// applied revision stands.
#[tokio::test]
async fn an_apply_the_static_checks_refuse_changes_nothing() {
    let harness = harness_with(native_serving(&["agent:b"]));
    let service = &harness.service;
    try_apply(
        service,
        1,
        serde_json::json!([]),
        serde_json::json!([declared_subscription("declared-a", "agent:a", &["ops.one"])]),
    )
    .await
    .unwrap();
    let before = declared(service).await;
    // Revision 2 drops declared-a and names declared-b twice.
    let refused = try_apply(
        service,
        2,
        serde_json::json!([]),
        serde_json::json!([
            declared_subscription("declared-b", "agent:b", &["ops.two"]),
            declared_subscription("declared-b", "agent:b", &["ops.three"]),
        ]),
    )
    .await
    .unwrap_err();
    assert_eq!(refused.code, -32602, "{refused:?}");
    assert_eq!(declared(service).await, before);
    assert_eq!(
        subscription_state(service, "declared-a").await.as_deref(),
        Some("active")
    );
    assert_eq!(subscription_state(service, "declared-b").await, None);
}

/// A declared template whose replace is refused is pending with the
/// refusal, and the children it made before are still listed: they still
/// capture.
#[tokio::test]
async fn a_pending_template_replace_keeps_its_children_listed() {
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
    assert_eq!(
        role_children(&declared(service).await, "reviewers").len(),
        1
    );
    // Revision 2 names a mob this gateway does not host: the replace is
    // refused.
    let moved = with(
        declared_template("reviewers", "reviewer", &["deploy.done"]),
        "mobs",
        serde_json::json!(["mob-not-hosted"]),
    );
    try_apply(
        service,
        2,
        serde_json::json!([]),
        serde_json::json!([moved]),
    )
    .await
    .unwrap();
    let view = declared(service).await;
    assert_eq!(
        record(&view, "subscription", "reviewers").map(|record| &record["status"]),
        Some(&serde_json::json!("pending")),
        "{view}"
    );
    let children = role_children(&view, "reviewers");
    assert_eq!(children.len(), 1, "{view}");
    assert_eq!(children[0]["status"], "applied", "{view}");
}

/// Change the subscription `id` at its revision as an operator does:
/// `verb` is "pause", "resume" or "cancel".
async fn operator_changes(service: &AnnouncementService, verb: &str, id: &str) {
    let found = try_host_call(
        service,
        SUBSCRIPTIONS_GET,
        serde_json::json!({ "subscription_id": id }),
    )
    .await
    .unwrap();
    try_host_call(
        service,
        &format!("meta/mob_announcements/subscriptions/{verb}"),
        serde_json::json!({
            "operation_id": format!("operator-{verb}-{id}"),
            "subscription_id": id,
            "expected_revision": found["subscription"]["revision"],
        }),
    )
    .await
    .unwrap();
}

/// An operator's pause of a declared subscription holds. An operator's
/// cancel of it, or end of a declared source, holds as suppressed while the
/// declaration of that record is unchanged. A changed declaration starts
/// the source's next generation; the cancelled subscription stays
/// cancelled and its declaration waits as pending until retention removes
/// the cancelled record.
#[tokio::test]
async fn an_operators_pause_and_cancel_of_a_declared_record_hold() {
    let harness = harness_with(native_serving(&["agent:b"]));
    let service = &harness.service;
    let first_inbox = declared_inbox(serde_json::json!([member_grant("agent:a")]));
    let first = serde_json::json!([declared_subscription("declared-a", "agent:a", &["ops.one"])]);
    try_apply(service, 1, serde_json::json!([first_inbox]), first.clone())
        .await
        .unwrap();

    // A pause holds across the same declarations applied again and a
    // reconcile.
    operator_changes(service, "pause", "declared-a").await;
    try_apply(service, 1, serde_json::json!([first_inbox]), first)
        .await
        .unwrap();
    service.reconcile_mobs().await.unwrap();
    assert_eq!(
        subscription_state(service, "declared-a").await.as_deref(),
        Some("paused")
    );

    // A cancel and an end hold as suppressed.
    operator_changes(service, "cancel", "declared-a").await;
    let revision = inbox(service).await["revision"].clone();
    try_host_call(
        service,
        SOURCES_END,
        serde_json::json!({
            "operation_id": "operator-end-inbox",
            "source_id": "inbox",
            "expected_revision": revision,
        }),
    )
    .await
    .unwrap();
    service.reconcile_mobs().await.unwrap();
    let view = declared(service).await;
    for (kind, id) in [("source", "inbox"), ("subscription", "declared-a")] {
        assert_eq!(
            record(&view, kind, id).map(|record| &record["status"]),
            Some(&serde_json::json!("suppressed")),
            "{view}"
        );
    }
    assert_eq!(
        subscription_state(service, "declared-a").await.as_deref(),
        Some("cancelled")
    );
    assert_eq!(inbox(service).await["state"], "ended");

    // Revision 2 changes both declarations.
    try_apply(
        service,
        2,
        serde_json::json!([declared_inbox(serde_json::json!([
            member_grant("agent:a"),
            member_grant("agent:b"),
        ]))]),
        serde_json::json!([declared_subscription("declared-a", "agent:a", &["ops.two"])]),
    )
    .await
    .unwrap();
    let view = declared(service).await;
    assert_eq!(
        record(&view, "source", "inbox").map(|record| &record["status"]),
        Some(&serde_json::json!("applied")),
        "{view}"
    );
    let source = inbox(service).await;
    assert_eq!(source["generation"], 2, "{source}");
    assert_eq!(source["state"], "active", "{source}");
    let waiting = record(&view, "subscription", "declared-a")
        .unwrap_or_else(|| panic!("the declared subscription is listed: {view}"));
    assert_eq!(waiting["status"], "pending", "{view}");
    assert_eq!(
        waiting["refusal"]["kind"], "subscription_cancelled",
        "{view}"
    );
    assert_eq!(
        subscription_state(service, "declared-a").await.as_deref(),
        Some("cancelled")
    );
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

/// The options take declarations inline or from a sidecar file the host
/// names, read once at init, never both; a sidecar that cannot be read or
/// parsed refuses the configuration.
#[test]
fn the_options_take_declarations_inline_or_from_a_sidecar() {
    let declarations = serde_json::json!({
        "revision": 3,
        "subscriptions": [declared_subscription("declared-a", "agent:a", &["ops.one"])],
    });
    let inline = super::super::parse_runtime_options(&serde_json::json!({
        "enabled": true,
        "declarations": declarations,
    }))
    .unwrap()
    .unwrap();
    let source = inline
        .declarations
        .as_deref()
        .expect("inline declarations are kept");
    assert_eq!(source.declarations().revision, 3);
    assert!(source.sidecar().is_none());

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("declarations.json");
    std::fs::write(&path, declarations.to_string()).unwrap();
    let sidecar = super::super::parse_runtime_options(&serde_json::json!({
        "enabled": true,
        "declarations_path": path,
    }))
    .unwrap()
    .unwrap();
    let source = sidecar
        .declarations
        .as_deref()
        .expect("the sidecar is read at init");
    assert_eq!(source.declarations().revision, 3);
    assert_eq!(source.sidecar(), Some(path.as_path()));

    let both = super::super::parse_runtime_options(&serde_json::json!({
        "enabled": true,
        "declarations": declarations,
        "declarations_path": path,
    }));
    assert!(both.is_err(), "one source of declarations per gateway");
    std::fs::write(&path, "{ not declarations").unwrap();
    let unparsed = super::super::parse_runtime_options(&serde_json::json!({
        "enabled": true,
        "declarations_path": path,
    }));
    assert!(
        unparsed.is_err(),
        "a sidecar that cannot be parsed fails closed"
    );
    let missing = super::super::parse_runtime_options(&serde_json::json!({
        "enabled": true,
        "declarations_path": dir.path().join("absent.json"),
    }));
    assert!(
        missing.is_err(),
        "a sidecar that cannot be read fails closed"
    );
}

/// The static checks refuse a whole declaration, whatever the runtime
/// holds: a stream route selector, a triage member its source's audience
/// leaves out, a topic two declared sources own (a forward topic too), a
/// member whose declared subscriptions together reach a declared source
/// both directly and through its forward, and a conversation member
/// subscribed to its own scope.
#[test]
fn a_declaration_the_static_checks_refuse_is_refused_whole() {
    let own = mob(OWN_MOB);
    let check = |value: serde_json::Value| {
        let declarations: super::super::declarations::Declarations =
            serde_json::from_value(value).unwrap();
        super::super::declarations::validate(&declarations, &own)
    };
    let streams = with(
        declared_inbox(serde_json::json!([member_grant("agent:a")])),
        "routes",
        serde_json::json!([{ "kind": "direct", "streams": ["INBOX"] }]),
    );
    let triage_outside = with(
        declared_inbox(serde_json::json!([member_grant("agent:a")])),
        "routes",
        serde_json::json!([
            { "kind": "direct" },
            triage_by("agent:t", serde_json::json!([member_grant("agent:a")])),
        ]),
    );
    // "agent:t" triages "mail.received" and forwards on "mail.triaged".
    let forwarding = with(
        declared_inbox(serde_json::json!([
            member_grant("agent:a"),
            member_grant("agent:t")
        ])),
        "routes",
        serde_json::json!([
            { "kind": "direct" },
            triage_by("agent:t", serde_json::json!([member_grant("agent:a")])),
        ]),
    );
    let mut triaged = with(
        source_params("unused", "triaged", "subject-2", &["mail.triaged"]),
        "routes",
        serde_json::json!([{ "kind": "direct" }]),
    );
    triaged.as_object_mut().unwrap().remove("operation_id");
    let conversation = with(
        declared_inbox(serde_json::json!([member_grant("agent:a")])),
        "conversation_members",
        serde_json::json!([{ "mob_id": OWN_MOB, "identity": "agent:a" }]),
    );
    let refusals = [
        (
            "a stream route selector",
            matches!(
                check(serde_json::json!({ "revision": 1, "sources": [streams] })),
                Err(AnnouncementStoreError::RouteSelectorUnsupported { .. })
            ),
        ),
        (
            "a triage member outside its source's audience",
            matches!(
                check(serde_json::json!({ "revision": 1, "sources": [triage_outside] })),
                Err(AnnouncementStoreError::AudienceNotCovered { .. })
            ),
        ),
        (
            "a forward topic another declared source owns",
            matches!(
                check(serde_json::json!({
                    "revision": 1,
                    "sources": [forwarding, triaged],
                })),
                Err(AnnouncementStoreError::TopicOwned { .. })
            ),
        ),
        (
            "a member reaching a declared source directly and through its forward",
            matches!(
                check(serde_json::json!({
                    "revision": 1,
                    "sources": [forwarding],
                    "subscriptions": [
                        declared_subscription("direct", "agent:a", &["mail.received"]),
                        declared_subscription("forwarded", "agent:a", &["mail.triaged"]),
                    ],
                })),
                Err(AnnouncementStoreError::RouteOverlap { .. })
            ),
        ),
        (
            "a conversation member subscribed to its own scope",
            matches!(
                check(serde_json::json!({
                    "revision": 1,
                    "sources": [conversation],
                    "subscriptions": [
                        declared_subscription("own-scope", "agent:a", &["mail.received"]),
                    ],
                })),
                Err(AnnouncementStoreError::ConversationMember { .. })
            ),
        ),
    ];
    let missed: Vec<&str> = refusals
        .iter()
        .filter(|(_, refused)| !refused)
        .map(|(what, _)| *what)
        .collect();
    assert!(missed.is_empty(), "the static checks admit: {missed:?}");
    let admitted = check(serde_json::json!({
        "revision": 1,
        "sources": [forwarding],
        "subscriptions": [
            declared_subscription("forwarded", "agent:a", &["mail.triaged"]),
            declared_subscription("triage", "agent:t", &["mail.received"]),
        ],
    }));
    assert!(
        admitted.is_ok(),
        "a declaration the checks admit: {admitted:?}"
    );
}

/// A declared subscription that names no mobs takes this gateway's own, as
/// `subscriptions/put` does, whichever mob its member or role is of.
#[test]
fn a_declared_subscription_names_this_gateways_mob_by_default() {
    let own = mob(OWN_MOB);
    let decode = |value: serde_json::Value| -> super::super::declarations::DeclaredSubscription {
        serde_json::from_value(value).unwrap()
    };
    let one = decode(serde_json::json!({
        "subscription_id": "child-member",
        "member": { "mob_id": "payments-child", "identity": "agent:c" },
        "topics": ["ops.one"],
    }));
    assert_eq!(one.mob_set(&own), BTreeSet::from([own.clone()]));
    let template = decode(serde_json::json!({
        "subscription_id": "child-role",
        "members": { "mob_id": "payments-child", "role": "reviewer" },
        "topics": ["ops.one"],
    }));
    assert_eq!(template.mob_set(&own), BTreeSet::from([own.clone()]));
}

/// A harness whose gateway takes its declarations from `source`, composed
/// as `compose` composes it: the declarations are reconciled before
/// anything runs.
async fn harness_declaring(
    native: ScriptedNative,
    source: super::super::declarations::DeclarationsSource,
) -> Harness {
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
        declarations: Some(Arc::new(source)),
        ..AnnouncementConfig::default()
    };
    let service = AnnouncementService::with_clock(
        Arc::clone(&store) as Arc<dyn AnnouncementStore>,
        Arc::clone(&native) as Arc<dyn NativeDelivery>,
        config,
        Arc::new(move || reader.load(Ordering::SeqCst)),
    )
    .unwrap();
    service.reconcile_declarations().await.unwrap();
    Harness {
        _dir: dir,
        store,
        native,
        clock,
        service,
    }
}

/// An apply follows the gateway's source of declarations. A gateway that
/// declares none, or declares them inline, applies what the call carries,
/// a revision and declarations both; one with a sidecar takes no
/// declarations in the call and reads the file again at each apply.
#[tokio::test]
async fn an_apply_follows_the_gateways_source_of_declarations() {
    let first = serde_json::json!({
        "revision": 1,
        "subscriptions": [declared_subscription("declared-a", "agent:a", &["ops.one"])],
    });
    let none = harness_with(native_serving(&[]));
    let empty = try_host_call(&none.service, DECLARATIONS_APPLY, serde_json::json!({}))
        .await
        .unwrap_err();
    assert_eq!(empty.code, -32602, "nothing to apply: {empty:?}");

    let inline = harness_declaring(
        native_serving(&[]),
        super::super::declarations::DeclarationsSource::Inline(
            serde_json::from_value(first.clone()).unwrap(),
        ),
    )
    .await;
    assert_eq!(
        subscription_state(&inline.service, "declared-a")
            .await
            .as_deref(),
        Some("active"),
        "inline declarations are reconciled at compose"
    );
    let empty = try_host_call(&inline.service, DECLARATIONS_APPLY, serde_json::json!({}))
        .await
        .unwrap_err();
    assert_eq!(
        empty.code, -32602,
        "an inline gateway's apply carries its declarations: {empty:?}"
    );

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("declarations.json");
    std::fs::write(&path, first.to_string()).unwrap();
    let harness = harness_declaring(
        native_serving(&[]),
        super::super::declarations::DeclarationsSource::Sidecar {
            path: path.clone(),
            declarations: serde_json::from_value(first).unwrap(),
        },
    )
    .await;
    let service = &harness.service;
    assert_eq!(
        subscription_state(service, "declared-a").await.as_deref(),
        Some("active"),
        "the sidecar read at init is reconciled at compose"
    );
    let carried = try_apply(service, 2, serde_json::json!([]), serde_json::json!([]))
        .await
        .unwrap_err();
    assert_eq!(
        carried.code, -32602,
        "a sidecar gateway's apply carries no declarations: {carried:?}"
    );
    assert_eq!(declared(service).await["revision"], 1);
    std::fs::write(
        &path,
        serde_json::json!({
            "revision": 2,
            "subscriptions": [declared_subscription("declared-b", "agent:a", &["ops.two"])],
        })
        .to_string(),
    )
    .unwrap();
    try_host_call(service, DECLARATIONS_APPLY, serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(declared(service).await["revision"], 2);
    assert_eq!(
        subscription_state(service, "declared-a").await.as_deref(),
        Some("cancelled"),
        "the file read again drops what it no longer declares"
    );
    assert_eq!(
        subscription_state(service, "declared-b").await.as_deref(),
        Some("active")
    );
}
