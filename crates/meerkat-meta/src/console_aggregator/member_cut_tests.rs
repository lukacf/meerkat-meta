//! Rows for one member cut per console read (see `member_cut`): a read
//! decides every identity record and frame exactly as the per-call reads
//! decided them, from one listing of each runtime's members; an identities
//! read and a timeline page list each runtime once; and an identities read
//! asks for a session-history backfill of a session only once.
#![allow(clippy::expect_used)]

use meerkat_core::SessionId;
use meerkat_mob::SpawnMemberSpec;

use super::tests::{
    CountingConsoleLogStore, build_empty_runtime, build_stress_runtime, runtime_entry_for_test,
    spawn_trusted_identity_projected_member, unique_console_mob_id,
};
use super::*;
use crate::identity_first::core_test_bridge::{CoreReadsBridge, register_live};

const RUNTIME: &str = "runtime-a";

/// Hides members labelled `org=secret` and the identity `concealed`, and
/// lists implicit delegates' members only when asked to.
struct FixturePolicy {
    include_delegates: bool,
}

impl ConsoleVisibilityPolicy for FixturePolicy {
    fn include_implicit_delegate_members(&self) -> bool {
        self.include_delegates
    }

    fn member_visible(&self, member: &ConsoleMember) -> bool {
        member.labels.get("org").map(String::as_str) != Some("secret")
    }

    fn identity_visible(&self, record: &ConsoleIdentityRecord) -> bool {
        record.identity != "concealed"
    }
}

fn spec(member: &str, labels: &[(&str, &str)]) -> SpawnMemberSpec {
    SpawnMemberSpec::from_wire(
        "worker".to_string(),
        member.to_string(),
        Some(format!("You are {member}.").into()),
        None,
        None,
    )
    .with_labels(
        labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
    )
}

fn durable_spec(identity: &str) -> crate::identity_first::DurableAgentSpec {
    crate::identity_first::DurableAgentSpec {
        identity: crate::identity_first::AgentIdentity::parse(identity).expect("identity"),
        profile: meerkat_mob::ProfileName::from("worker"),
        addressability: crate::identity_first::AgentAddressability::Addressable,
        display_name: None,
        labels: BTreeMap::new(),
        context: None,
        additional_instructions: Vec::new(),
        initial_message: None,
        runtime_mode_override: None,
        backend: None,
        binding: None,
        placement: None,
    }
}

/// A realm with every kind of member a console read decides:
/// - a visible member, and one hidden by its `org=secret` label;
/// - a durable identity's stale incarnation row (`rt:review:singleton:0`)
///   beside its own row, hidden, which shadows it;
/// - a durable identity named by its own row (`builder`);
/// - an implicit delegate's member, and a delegate row that projects the
///   identity `builder` from another source;
/// - durable identities with no live row: one concealed by the policy, one
///   dormant.
struct Realm {
    primary: UnifiedRuntime,
    delegate: UnifiedRuntime,
    entry: RuntimeEntry,
}

impl Realm {
    async fn build(include_delegates: bool) -> Self {
        let primary_id = unique_console_mob_id("member-cut-primary");
        let primary = build_empty_runtime(&primary_id).await;
        let members: [(&str, &[(&str, &str)]); 5] = [
            ("visible", &[]),
            ("secret", &[("org", "secret")]),
            (
                "rt:review:singleton:0",
                &[("agent_identity", "review:singleton")],
            ),
            (
                "review:singleton",
                &[("agent_identity", "review:singleton"), ("org", "secret")],
            ),
            ("builder", &[("agent_identity", "builder")]),
        ];
        for (member, labels) in members {
            spawn_trusted_identity_projected_member(&primary, spec(member, labels)).await;
        }
        let delegate_id = unique_console_mob_id("member-cut-delegate");
        let delegate = build_empty_runtime(&delegate_id).await;
        spawn_trusted_identity_projected_member(&delegate, spec("delegate", &[])).await;
        spawn_trusted_identity_projected_member(
            &delegate,
            spec("builder", &[("agent_identity", "other")]),
        )
        .await;
        primary
            .mob_runtime()
            .agent_mob_mcp_state()
            .expect("the primary runtime exposes its mob state")
            .mob_insert_handle(
                meerkat_mob::ids::MobId::from(delegate_id.as_str()),
                delegate.mob_handle().clone(),
            )
            .await;

        let core_bridge = Arc::new(CoreReadsBridge::default());
        let identity_runtime = Arc::new(crate::identity_first::IdentityRuntime::new(
            crate::identity_first::IdentityRuntimeConfig {
                continuity_store: Arc::new(
                    crate::identity_first::LocalContinuityStore::in_memory()
                        .expect("continuity store"),
                ),
                lease_provider: Arc::new(crate::identity_first::LocalLeaseProvider::new()),
                runtime_instance_id: "member-cut-equivalence".to_string(),
                has_runtime_store: true,
                durability_policy: crate::identity_first::DurabilityPolicy::SyncWriteThrough,
                bridge: Some(core_bridge.clone()),
                default_timeout: None,
            },
        ));
        for identity in ["review:singleton", "builder", "concealed"] {
            register_live(
                &identity_runtime,
                &core_bridge.core,
                durable_spec(identity),
                0,
                SessionId::new(),
            )
            .await
            .expect("a live durable identity registers");
        }
        identity_runtime.register(durable_spec("dormant")).await;

        let entry = RuntimeEntry {
            registration_id: uuid::Uuid::new_v4(),
            runtime_key: RUNTIME.to_string(),
            identity_namespace: String::new(),
            runtime: primary.mob_runtime().clone(),
            identity_runtime: Some(identity_runtime),
            identity_owner: None,
            memory_panel: None,
            console_events: primary.console_events(),
            visibility_policy: Arc::new(FixturePolicy { include_delegates }),
        };
        Self {
            primary,
            delegate,
            entry,
        }
    }

    async fn stop(self) {
        let _ = self.delegate.mob_handle().stop().await;
        let _ = self.primary.mob_handle().stop().await;
    }
}

fn record(
    identity: &str,
    runtime_member_id: &str,
    session_id: Option<&str>,
) -> ConsoleIdentityRecord {
    ConsoleIdentityRecord {
        identity: identity.to_string(),
        display_name: identity.to_string(),
        runtime_key: RUNTIME.to_string(),
        runtime_member_id: runtime_member_id.to_string(),
        session_id: session_id.map(ToString::to_string),
        member_id: None,
        visibility: ConsoleVisibility::Addressable,
        addressable: true,
        health: "ready".to_string(),
        topology_peers: Vec::new(),
        labels: BTreeMap::new(),
    }
}

fn frame(identity: &str, session_id: Option<&str>) -> ConsoleFrame {
    ConsoleFrame {
        id: format!("frame-{identity}-{session_id:?}"),
        cursor: ConsoleCursor::from_seq(1),
        dedupe_key: format!("frame-{identity}-{session_id:?}"),
        timestamp_ms: 1,
        runtime_key: RUNTIME.to_string(),
        identity: identity.to_string(),
        conversation_id: Some(identity.to_string()),
        session_id: session_id.map(ToString::to_string),
        kind: "text_complete".to_string(),
        status: ConsoleFrameStatus::Completed,
        frame_version: 1,
        updated_at_ms: None,
        payload: json!({ "text": identity }),
        source: ConsoleFrameSource {
            member_provenance: None,
            kind: ConsoleFrameSourceKind::ConsoleEvent,
            source_cursor: None,
        },
        source_event_id: None,
        interaction_id: None,
        turn_id: None,
        run_id: None,
        parent_frame_id: None,
        caused_by_frame_id: None,
    }
}

/// Every identity record a read of the realm decides: each durable
/// identity's status record, each listed member's record, durable records
/// spelled by their binding, and records with no live member at all.
async fn records_to_decide(realm: &Realm) -> Vec<ConsoleIdentityRecord> {
    let identity_runtime = realm
        .entry
        .identity_runtime
        .as_ref()
        .expect("identity runtime");
    let mut records: Vec<_> = identity_runtime
        .statuses()
        .await
        .expect("statuses")
        .iter()
        .map(|status| identity_record_for_status(&realm.entry, status))
        .collect();
    for resolved in member_sources_for_entry_including_hidden(&realm.entry).await {
        records.extend(identity_record_for_resolved_member(&resolved).await);
    }
    records.extend([
        record("builder", "rt:builder:0", None),
        record("review:singleton", "rt:review:singleton:0", None),
        record("retired", "retired", Some("retired-session")),
        record("ghost", "ghost", None),
    ]);
    records
}

/// Every frame a read of the realm decides: each record's identity with its
/// session, with none and with another one, under its runtime member id, and
/// frames of identities no member names.
fn frames_to_decide(records: &[ConsoleIdentityRecord]) -> Vec<ConsoleFrame> {
    let mut frames = Vec::new();
    for record in records {
        frames.push(frame(&record.identity, record.session_id.as_deref()));
        frames.push(frame(&record.identity, None));
        frames.push(frame(&record.identity, Some("another-session")));
        frames.push(frame(&record.runtime_member_id, None));
    }
    frames.push(frame("retired", Some("retired-session")));
    frames.push(frame("ghost", None));
    frames
}

/// A record naming `parent` as its spawner, bound (or not) to `session`.
fn child_of(parent: &str, session: Option<&str>) -> ConsoleIdentityRecord {
    let mut child = record("child", "child", Some("child-session"));
    child.labels.insert(
        crate::console_spawn::SPAWNED_BY_LABEL.to_string(),
        parent.to_string(),
    );
    if let Some(session) = session {
        child.labels.insert(
            crate::console_spawn::SPAWNED_BY_SESSION_LABEL.to_string(),
            session.to_string(),
        );
    }
    child
}

/// The per-call decisions a read made before the cut, kept verbatim (only
/// renamed) from the integration base: each call lists the members itself.
mod per_call {
    use super::*;

    pub(super) async fn console_identity_record_visible(
        entry: &RuntimeEntry,
        record: &ConsoleIdentityRecord,
    ) -> bool {
        if !entry.visibility_policy.identity_visible(record) {
            return false;
        }
        let mut bound_live_member_seen = false;
        let mut wrong_live_projection_seen = false;
        for resolved in Box::pin(member_sources_for_entry_including_hidden(entry)).await {
            if !crate::member_comms_id::live_member_names_identity_or_incarnation(
                &resolved.runtime_identity,
                &record.identity,
                Some(record.runtime_member_id.as_str()),
            ) {
                continue;
            }
            bound_live_member_seen = true;
            let Some(live_record) = identity_record_for_resolved_member(&resolved).await else {
                continue;
            };
            if live_record.identity != record.identity {
                wrong_live_projection_seen = true;
                continue;
            }
            if raw_resolved_member_visible(&resolved, &live_record) {
                return true;
            }
        }
        if wrong_live_projection_seen {
            return true;
        }
        !bound_live_member_seen
    }

    async fn live_record_shadowed_by_hidden_durable_binding(
        resolved: &ResolvedConsoleMember,
        record: &ConsoleIdentityRecord,
    ) -> bool {
        let Some(identity_runtime) = resolved.entry.identity_runtime.as_ref() else {
            return false;
        };
        let Ok(identity) = crate::identity_first::AgentIdentity::parse(&record.identity) else {
            return false;
        };
        if crate::member_comms_id::live_member_is_identity(
            &resolved.runtime_identity,
            identity.as_str(),
        ) {
            return false;
        }
        let status = match identity_runtime.status(&identity).await {
            Ok(status) => status,
            Err(crate::identity_first::IdentityRuntimeError::UnknownIdentity(_)) => return false,
            Err(_) => return true,
        };
        if status.agent_runtime_id.is_none() {
            return false;
        }
        for candidate in Box::pin(member_sources_for_entry(&resolved.entry)).await {
            if !crate::member_comms_id::live_member_is_identity(
                &candidate.runtime_identity,
                status.identity.as_str(),
            ) {
                continue;
            }
            let Some(bound_record) = identity_record_for_resolved_member(&candidate).await else {
                continue;
            };
            if bound_record.identity == record.identity
                && !raw_resolved_member_visible(&candidate, &bound_record)
            {
                return true;
            }
        }
        false
    }

    pub(super) async fn resolved_member_visible(
        resolved: &ResolvedConsoleMember,
        record: &ConsoleIdentityRecord,
    ) -> bool {
        if !raw_resolved_member_visible(resolved, record) {
            return false;
        }
        !Box::pin(live_record_shadowed_by_hidden_durable_binding(
            resolved, record,
        ))
        .await
    }

    pub(super) async fn frame_matches_hidden_member(
        entry: &RuntimeEntry,
        frame: &ConsoleFrame,
    ) -> bool {
        let runtime_member_id = strip_namespace(&frame.identity, &entry.identity_namespace)
            .unwrap_or_else(|| frame.identity.clone());
        let frame_session_id = frame.session_id.as_deref();
        let mut saw_hidden = false;
        let mut saw_visible = false;
        for resolved in Box::pin(member_sources_for_entry_including_hidden(entry)).await {
            let Some(record) = identity_record_for_resolved_member(&resolved).await else {
                continue;
            };
            let session_matches =
                frame_session_id.is_some() && record.session_id.as_deref() == frame_session_id;
            let runtime_matches = record.runtime_member_id == runtime_member_id;
            let identity_matches = record.identity == frame.identity;
            if !session_matches && !runtime_matches && !identity_matches {
                continue;
            }
            if session_matches && !raw_resolved_member_visible(&resolved, &record) {
                return true;
            }
            if Box::pin(resolved_member_visible(&resolved, &record)).await {
                saw_visible = true;
            } else {
                saw_hidden = true;
            }
        }
        saw_hidden && !saw_visible
    }

    pub(super) async fn capture_member_lineage(
        resolved: &ResolvedConsoleMember,
        record: &ConsoleIdentityRecord,
    ) -> Vec<ConsoleFrameLineageLink> {
        let namespace = resolved.entry.identity_namespace.as_str();
        let mut lineage = vec![lineage_link_for_record(
            record,
            namespace,
            resolved.member.role.as_str(),
        )];
        let mut visited = BTreeSet::from([record.identity.clone()]);
        let mut child_labels = record.labels.clone();
        let mut sources: Option<Vec<ResolvedConsoleMember>> = None;
        while lineage.len() < CAPTURED_LINEAGE_DEPTH {
            let Some(parent) = child_labels
                .get(crate::console_spawn::SPAWNED_BY_LABEL)
                .map(|parent| parent.trim().to_string())
                .filter(|parent| !parent.is_empty())
            else {
                break;
            };
            let parent_identity = apply_namespace(&parent, namespace);
            if !visited.insert(parent_identity.clone()) {
                break;
            }
            let bound_session = child_labels
                .get(crate::console_spawn::SPAWNED_BY_SESSION_LABEL)
                .map(|session| session.trim().to_string())
                .filter(|session| !session.is_empty());
            let mut confirmed = None;
            if let Some(bound_session) = bound_session.as_deref() {
                if sources.is_none() {
                    sources = Some(
                        Box::pin(member_sources_for_entry_including_hidden(&resolved.entry)).await,
                    );
                }
                for candidate in sources.iter().flatten() {
                    let roster_id = candidate.member.agent_identity.as_str();
                    let alias_name = crate::member_comms_id::runtime_alias_str(roster_id);
                    let alias: &str = alias_name.as_ref();
                    let durable =
                        crate::member_comms_id::durable_identity_label(&candidate.member.labels)
                            .unwrap_or(alias);
                    if roster_id != parent && alias != parent && durable != parent {
                        continue;
                    }
                    let Some(candidate_record) =
                        Box::pin(identity_record_for_resolved_member(candidate)).await
                    else {
                        continue;
                    };
                    if candidate_record.session_id.as_deref() == Some(bound_session) {
                        confirmed = Some((candidate_record, candidate.member.role.to_string()));
                        break;
                    }
                }
            }
            match confirmed {
                Some((parent_record, role)) => {
                    lineage.push(lineage_link_for_record(&parent_record, namespace, &role));
                    child_labels = parent_record.labels;
                }
                None => {
                    lineage.push(ConsoleFrameLineageLink {
                        identity: parent_identity,
                        agent_id: None,
                        role: None,
                        labels: None,
                    });
                    break;
                }
            }
        }
        lineage
    }
}

/// One cut per read decides every record, frame, member and lineage exactly
/// as the per-call reads did, over a realm with visible, hidden, shadowed,
/// delegate, labelled, dormant, retired and unknown identities, with the
/// policy listing implicit delegates and without. It is an equivalence
/// guard, green on the base by construction: a cut that kept a listing the
/// base would not have read (a delegate the policy leaves out of the binding
/// check), or reused a decision across members, turns it red.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_cut_decides_every_record_and_frame_as_the_per_call_reads() {
    for include_delegates in [true, false] {
        let realm = Realm::build(include_delegates).await;
        let records = records_to_decide(&realm).await;
        let frames = frames_to_decide(&records);
        let cut = MemberCut::new(&realm.entry);

        for record in &records {
            assert_eq!(
                console_identity_record_visible_in(&cut, record).await,
                per_call::console_identity_record_visible(&realm.entry, record).await,
                "record {} ({}) with delegates {include_delegates}",
                record.identity,
                record.runtime_member_id,
            );
        }
        for frame in &frames {
            assert_eq!(
                frame_matches_hidden_member_in(&cut, frame).await,
                per_call::frame_matches_hidden_member(&realm.entry, frame).await,
                "frame of {} in session {:?} with delegates {include_delegates}",
                frame.identity,
                frame.session_id,
            );
        }
        for member in cut.members().await {
            let resolved = &member.resolved;
            let Some(own) = member.record().await else {
                continue;
            };
            assert_eq!(
                resolved_member_visible_in(&cut, resolved, own).await,
                per_call::resolved_member_visible(resolved, own).await,
                "member {} with delegates {include_delegates}",
                resolved.runtime_identity,
            );
            let session = own.session_id.as_deref();
            for child in [
                child_of(&resolved.runtime_identity, session),
                child_of(&resolved.runtime_identity, Some("an-earlier-parent")),
                child_of(&resolved.runtime_identity, None),
            ] {
                assert_eq!(
                    capture_member_lineage_in(&cut, resolved, &child).await,
                    per_call::capture_member_lineage(resolved, &child).await,
                    "lineage through {} with delegates {include_delegates}",
                    resolved.runtime_identity,
                );
            }
        }
        // The realm is decided both ways: something is hidden and something
        // is visible, so the comparison is not over a uniform answer.
        let mut outcomes = BTreeSet::new();
        for record in &records {
            outcomes.insert(console_identity_record_visible_in(&cut, record).await);
        }
        assert_eq!(outcomes, BTreeSet::from([false, true]));
        realm.stop().await;
    }
}

/// Every frame decision of a read through `frame_is_visible_cached`, one
/// read for all frames, equals the frame decided as a read of its own, with
/// the identities a collect of the realm lists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_decides_each_frame_as_a_read_of_that_frame_alone() {
    for include_delegates in [true, false] {
        let realm = Realm::build(include_delegates).await;
        let aggregator = MetaConsoleAggregator::in_memory();
        aggregator
            .inner
            .runtimes
            .write()
            .expect("runtime registry")
            .insert(RUNTIME.to_string(), realm.entry.clone());
        let identity_records = collect_identity_records(
            &aggregator.inner,
            IdentityCollectionMode::IncludeLiveMembers,
        )
        .await
        .expect("identity records");
        let records = records_to_decide(&realm).await;
        let mut read = FrameVisibilityRead::default();
        for frame in frames_to_decide(&records) {
            for allow_historical_identity in [false, true] {
                assert_eq!(
                    frame_is_visible_cached(
                        &aggregator.inner,
                        &frame,
                        allow_historical_identity,
                        &mut read,
                        &identity_records,
                    )
                    .await
                    .expect("decided in the read"),
                    frame_is_visible(
                        &aggregator.inner,
                        &frame,
                        allow_historical_identity,
                        &identity_records,
                    )
                    .await
                    .expect("decided alone"),
                    "frame of {} in session {:?} (historical {allow_historical_identity}, \
                     delegates {include_delegates})",
                    frame.identity,
                    frame.session_id,
                );
            }
        }
        realm.stop().await;
    }
}

/// An identity-first realm of `count` live durable identities, each named by
/// its own row, under the default policy.
async fn identity_first_realm(count: usize) -> (UnifiedRuntime, RuntimeEntry) {
    let runtime = build_empty_runtime(&unique_console_mob_id("member-cut-listings")).await;
    let core_bridge = Arc::new(CoreReadsBridge::default());
    let identity_runtime = Arc::new(crate::identity_first::IdentityRuntime::new(
        crate::identity_first::IdentityRuntimeConfig {
            continuity_store: Arc::new(
                crate::identity_first::LocalContinuityStore::in_memory().expect("continuity store"),
            ),
            lease_provider: Arc::new(crate::identity_first::LocalLeaseProvider::new()),
            runtime_instance_id: "member-cut-listings".to_string(),
            has_runtime_store: true,
            durability_policy: crate::identity_first::DurabilityPolicy::SyncWriteThrough,
            bridge: Some(core_bridge.clone()),
            default_timeout: None,
        },
    ));
    for index in 0..count {
        let identity = format!("member-{index}");
        spawn_trusted_identity_projected_member(
            &runtime,
            spec(&identity, &[("agent_identity", identity.as_str())]),
        )
        .await;
        register_live(
            &identity_runtime,
            &core_bridge.core,
            durable_spec(&identity),
            0,
            SessionId::new(),
        )
        .await
        .expect("a live durable identity registers");
    }
    let entry = RuntimeEntry {
        registration_id: uuid::Uuid::new_v4(),
        runtime_key: RUNTIME.to_string(),
        identity_namespace: String::new(),
        runtime: runtime.mob_runtime().clone(),
        identity_runtime: Some(identity_runtime),
        identity_owner: None,
        memory_panel: None,
        console_events: runtime.console_events(),
        visibility_policy: Arc::new(AllowAllConsoleVisibilityPolicy),
    };
    (runtime, entry)
}

fn aggregator_over(entry: &RuntimeEntry) -> MetaConsoleAggregator {
    let aggregator = MetaConsoleAggregator::new_with_options(
        Arc::new(InMemoryConsoleLogStore::new()),
        ConsoleAggregatorOptions {
            session_history_backfill_enabled: false,
            ..ConsoleAggregatorOptions::default()
        },
    );
    aggregator
        .inner
        .runtimes
        .write()
        .expect("runtime registry")
        .insert(RUNTIME.to_string(), entry.clone());
    aggregator
}

/// An identities read lists each runtime's members once. RED on the base:
/// every durable identity's record listed the members again (N listings)
/// and the live members once more, N + 1 in all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_identities_read_lists_each_runtime_once() {
    const IDENTITIES: usize = 6;
    let (runtime, entry) = identity_first_realm(IDENTITIES).await;
    let aggregator = aggregator_over(&entry);

    let before = member_cut::listings::count(entry.registration_id);
    let identities = aggregator.list_identities().await.expect("identities");
    assert_eq!(
        member_cut::listings::count(entry.registration_id) - before,
        1,
        "an identities read lists the members once"
    );
    let listed: BTreeSet<_> = identities
        .iter()
        .map(|record| record.identity.clone())
        .collect();
    assert_eq!(
        listed,
        (0..IDENTITIES)
            .map(|index| format!("member-{index}"))
            .collect::<BTreeSet<_>>(),
    );
    let _ = runtime.mob_handle().stop().await;
}

/// A timeline page lists each runtime's members once, however many frames
/// and identities it decides. RED on the base: every frame of a visible or
/// unknown identity listed the members again, and every identity once more.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timeline_page_lists_each_runtime_once() {
    const IDENTITIES: usize = 4;
    const FRAMES_EACH: usize = 3;
    let (runtime, entry) = identity_first_realm(IDENTITIES).await;
    let aggregator = aggregator_over(&entry);
    aggregator
        .list_identities()
        .await
        .expect("the identity read model is primed");
    let mut sequence = 0_u64;
    let mut append = |identity: String| {
        sequence += 1;
        NewConsoleFrame {
            id: None,
            dedupe_key: format!("listing-frame-{sequence}"),
            timestamp_ms: sequence,
            runtime_key: RUNTIME.to_string(),
            identity: identity.clone(),
            conversation_id: Some(identity),
            session_id: None,
            kind: "text_complete".to_string(),
            status: ConsoleFrameStatus::Completed,
            payload: json!({ "text": sequence }),
            source: ConsoleFrameSource {
                member_provenance: None,
                kind: ConsoleFrameSourceKind::ConsoleEvent,
                source_cursor: None,
            },
            source_event_id: None,
            interaction_id: None,
            turn_id: None,
            run_id: None,
            parent_frame_id: None,
            caused_by_frame_id: None,
        }
    };
    let mut frames = Vec::new();
    for _ in 0..FRAMES_EACH {
        for index in 0..IDENTITIES {
            frames.push(append(format!("member-{index}")));
        }
        frames.push(append("retired".to_string()));
    }
    for frame in frames {
        aggregator
            .store()
            .append_if_absent(frame)
            .await
            .expect("append");
    }

    let before = member_cut::listings::count(entry.registration_id);
    let page = aggregator
        .query_timeline(ConsoleTimelineQuery {
            limit: 1_000,
            ..ConsoleTimelineQuery::default()
        })
        .await
        .expect("timeline page");
    assert_eq!(
        member_cut::listings::count(entry.registration_id) - before,
        1,
        "a timeline page lists the members once"
    );
    // Every live identity's frames, none of the retired identity's.
    assert_eq!(page.frames.len(), IDENTITIES * FRAMES_EACH);
    assert!(page.frames.iter().all(|frame| frame.identity != "retired"));
    let _ = runtime.mob_handle().stop().await;
}

/// A second identities read asks for no session-history backfill of a
/// session the first one asked for: the discovery loop rereads a session
/// whose boundary moves. RED on the base: every identities read asked for
/// every listed live session again, one more watermark read each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_identities_read_asks_no_backfill_for_a_session_already_asked_for() {
    const MEMBERS: usize = 4;
    let store = Arc::new(CountingConsoleLogStore::new());
    let aggregator = MetaConsoleAggregator::new(store.clone());
    let (_temp, runtime, _service) = build_stress_runtime(MEMBERS, Duration::ZERO).await;
    aggregator
        .inner
        .runtimes
        .write()
        .expect("runtime registry")
        .insert(
            RUNTIME.to_string(),
            runtime_entry_for_test(RUNTIME, runtime.as_ref()),
        );

    let identities = aggregator.list_identities().await.expect("identities");
    let live_sessions = identities
        .iter()
        .filter(|record| record.session_id.is_some())
        .count();
    assert_eq!(live_sessions, MEMBERS, "{identities:#?}");
    aggregator.history_backfill_converged().await;
    let first = store.source_watermark_calls();
    assert!(
        first >= MEMBERS,
        "the first read asks for each live session: {first} watermark reads"
    );

    aggregator.list_identities().await.expect("identities");
    aggregator.history_backfill_converged().await;
    assert_eq!(
        store.source_watermark_calls(),
        first,
        "the second read asked for no session again"
    );
    let _ = runtime.mob_handle().stop().await;
}
