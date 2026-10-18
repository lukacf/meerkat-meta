//! Keyed inject between gateways, over a real control listener.
//!
//! The serving side here is the same keyed inject dispatch the gateway's control
//! handler runs, in front of a scripted continuation owner; the calling side
//! is the production [`RemoteMemberDelivery`] and [`RemoteMobProxy`]. What
//! these pin: only a signed answer the caller's pinned key verifies is
//! evidence, requests are bound to their audience, a grant must name the
//! verbs, the freshness window holds exactly at its edges, and a replay can
//! never add a second commit.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use base64::Engine as _;
use ed25519_dalek::Signer as _;

use super::native::{NativeStatus, NativeSubmitError};
use super::remote::{Clock, RemoteMemberDelivery, validate_delivery};
use super::types::*;
use crate::auth::peer_keys::GatewayPeerKeys;
use crate::contact_directory::ContactDirectory;
use crate::runtime::cross_mob_control::AuthorizedCaller;
use crate::runtime::cross_mob_control::{
    ControlAuthorizer, ControlGrant, ControlGrantTable, ControlHandler, ControlMemberScope,
    ControlRequest, ControlResponse, ControlSignerSlot, ControlVerb,
    control_response_signing_payload, serve_tcp_control_with_authorizer,
    sign_control_request_as_caller, verify_control_response,
};
use crate::runtime::keyed_inject::{
    KeyedInject, KeyedInjectCallerScope, KeyedInjectFuture, KeyedInjectReceiver,
    KeyedInjectRefusal, KeyedInjectRefusalCode, KeyedInjectSlot, KeyedInjectStatusAnswer,
    NativeReceipt, answer_keyed_inject_request,
};

const PEER: &str = "mob-b";
const MEMBER: &str = "agent:a";
const WINDOW_MS: u64 = 300_000;
const NOW_MS: u64 = 1_800_000_000_000;

/// A continuation owner in miniature, keyed exactly as the gateway's
/// receiver keys it (the batch id under the caller's scope): one binding per
/// key, identical replays answered with the original receipt, other content
/// refused.
#[derive(Default)]
struct Bindings {
    by_key: BTreeMap<String, (String, NativeReceipt)>,
    submits: usize,
}

struct ScriptedReceiver {
    now: AtomicU64,
    started_at: AtomicU64,
    bindings: Mutex<Bindings>,
    refuse_next: Mutex<Option<KeyedInjectRefusal>>,
}

impl ScriptedReceiver {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            now: AtomicU64::new(NOW_MS),
            started_at: AtomicU64::new(0),
            bindings: Mutex::new(Bindings::default()),
            refuse_next: Mutex::new(None),
        })
    }

    fn commits(&self) -> usize {
        self.bindings.lock().unwrap().by_key.len()
    }

    fn submits(&self) -> usize {
        self.bindings.lock().unwrap().submits
    }
}

impl KeyedInjectReceiver for ScriptedReceiver {
    fn window_ms(&self) -> u64 {
        WINDOW_MS
    }

    fn now_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }

    fn started_at_ms(&self) -> u64 {
        self.started_at.load(Ordering::SeqCst)
    }

    fn deliver<'a>(
        &'a self,
        scope: &'a KeyedInjectCallerScope,
        delivery: KeyedInject<'a>,
    ) -> KeyedInjectFuture<'a, std::result::Result<NativeReceipt, KeyedInjectRefusal>> {
        Box::pin(async move {
            if let Some(refusal) = self.refuse_next.lock().unwrap().take() {
                return Err(refusal);
            }
            validate_delivery(&delivery)?;
            let key = scope.native_key(delivery.delivery_key);
            let exact = format!("{}|{}", delivery.content_digest, delivery.summary);
            let mut bindings = self.bindings.lock().unwrap();
            bindings.submits += 1;
            if let Some((bound, receipt)) = bindings.by_key.get(&key) {
                if *bound == exact {
                    return Ok(receipt.clone());
                }
                return Err(KeyedInjectRefusal::new(
                    KeyedInjectRefusalCode::Conflict,
                    "the key is bound to other content",
                ));
            }
            let receipt = NativeReceipt {
                delivery_id: format!("delivery:{key}"),
                address: format!("rt:{}:1", delivery.member),
                submission_digest: format!("submission:{key}:{exact}"),
                committed_at_ms: self.now_ms(),
            };
            bindings.by_key.insert(key, (exact, receipt.clone()));
            Ok(receipt)
        })
    }

    fn status<'a>(
        &'a self,
        scope: &'a KeyedInjectCallerScope,
        _member: &'a str,
        delivery_key: &'a str,
        with_input: bool,
    ) -> KeyedInjectFuture<'a, std::result::Result<KeyedInjectStatusAnswer, KeyedInjectRefusal>>
    {
        Box::pin(async move {
            let bindings = self.bindings.lock().unwrap();
            let status = match bindings.by_key.get(&scope.native_key(delivery_key)) {
                None => NativeStatus::NotCommitted,
                Some((_, receipt)) => NativeStatus::Applied {
                    receipt: receipt.clone(),
                    session_id: "session-1".into(),
                    input_id: "input-1".into(),
                },
            };
            let input = (with_input && matches!(status, NativeStatus::Applied { .. }))
                .then(|| serde_json::json!({ "phase": "consumed" }));
            Ok(KeyedInjectStatusAnswer { status, input })
        })
    }
}

/// The keyed inject dispatch the gateway's handler runs; nothing else.
struct KeyedInjectHandler {
    slot: KeyedInjectSlot,
}

impl ControlHandler for KeyedInjectHandler {
    fn handle(
        &self,
        request: ControlRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ControlResponse> + Send + '_>> {
        Box::pin(async move { answer_keyed_inject_request(&self.slot, request, None).await })
    }

    fn handle_admitted(
        &self,
        request: ControlRequest,
        caller: Option<AuthorizedCaller>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ControlResponse> + Send + '_>> {
        Box::pin(
            async move { answer_keyed_inject_request(&self.slot, request, caller.as_ref()).await },
        )
    }
}

fn signer(keys: &Arc<GatewayPeerKeys>) -> ControlSignerSlot {
    Arc::new(RwLock::new(Some(Arc::clone(keys))))
}

fn keyed_inject_grant(caller: &GatewayPeerKeys, verbs: &[ControlVerb]) -> ControlGrantTable {
    let mut table = ControlGrantTable::new();
    file_keyed_inject_grant(&mut table, "calling-gateway", caller, verbs);
    table
}

fn file_keyed_inject_grant(
    table: &mut ControlGrantTable,
    label: &str,
    caller: &GatewayPeerKeys,
    verbs: &[ControlVerb],
) {
    table.insert(
        caller.pubkey_bytes(),
        ControlGrant::new(
            label,
            verbs.iter().copied(),
            ControlMemberScope::members([MEMBER]),
        ),
    );
}

struct Peer {
    address: String,
    receiver: Arc<ScriptedReceiver>,
}

/// Serve the keyed inject plane on a loopback port, signing answers with
/// `answer_keys` and admitting callers through `authorizer`.
async fn serve(answer_keys: &Arc<GatewayPeerKeys>, authorizer: ControlAuthorizer) -> Peer {
    let receiver = ScriptedReceiver::new();
    let slot: KeyedInjectSlot = Arc::new(RwLock::new(Some(
        Arc::clone(&receiver) as Arc<dyn KeyedInjectReceiver>
    )));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("tcp://{}", listener.local_addr().unwrap());
    tokio::spawn(serve_tcp_control_with_authorizer(
        listener,
        Arc::new(KeyedInjectHandler { slot }),
        signer(answer_keys),
        Arc::new(authorizer),
    ));
    Peer { address, receiver }
}

fn contacts(mob_id: &str, address: &str, pinned: &GatewayPeerKeys) -> ContactDirectory {
    ContactDirectory::from_toml(&format!(
        "[mobs]\n\"{mob_id}\" = {{ transport = \"{address}\", pubkey = \"{}\" }}\n",
        pinned.pubkey_b64()
    ))
    .unwrap()
}

fn caller(
    directory: ContactDirectory,
    keys: &Arc<GatewayPeerKeys>,
    clock_ms: Arc<AtomicU64>,
) -> RemoteMemberDelivery {
    let clock: Clock = Arc::new(move || clock_ms.load(Ordering::SeqCst));
    RemoteMemberDelivery::with_clock(directory, signer(keys), clock)
}

fn remote_batch(id: &str, body_text: &str) -> Batch {
    let id = BatchId::new(id).unwrap();
    let body = render_batch(
        &id,
        None,
        &[],
        &[format!("- [ios] payments #1 key=k\n  {body_text}\n")],
    );
    Batch {
        member: Member::new(
            MobId::new(PEER).unwrap(),
            MemberIdentity::new(MEMBER).unwrap(),
        ),
        items: vec![BatchItem {
            subscription_id: SubscriptionId::new("s1").unwrap(),
            subscription_revision: 1,
            mob_id: MobId::new("payments").unwrap(),
            sequence: 1,
            source_key: SourceKey::new("k").unwrap(),
        }],
        gaps: Vec::new(),
        content_digest: content_digest(&body),
        body,
        id,
        predecessor: None,
        successor: None,
        state: BatchState::Submitting { attempt: 1 },
        sealed_at_ms: NOW_MS,
        updated_at_ms: NOW_MS,
    }
}

/// A granted, pinned, fresh pair: the baseline every other case perturbs.
async fn granted_pair() -> (Peer, RemoteMemberDelivery, Arc<AtomicU64>) {
    let peer_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let caller_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let peer = serve(
        &peer_keys,
        ControlAuthorizer::with_grants_for_audience(
            keyed_inject_grant(
                &caller_keys,
                &[ControlVerb::InjectKeyed, ControlVerb::InjectStatus],
            ),
            PEER,
        ),
    )
    .await;
    let clock = Arc::new(AtomicU64::new(NOW_MS));
    let delivery = caller(
        contacts(PEER, &peer.address, &peer_keys),
        &caller_keys,
        Arc::clone(&clock),
    );
    (peer, delivery, clock)
}

#[tokio::test]
async fn a_remote_batch_commits_once_and_every_replay_returns_the_original_receipt() {
    let (peer, delivery, clock) = granted_pair().await;
    let batch = remote_batch("batch-1", "card declined");
    let first = delivery.submit(&batch).await.unwrap();
    // A later attempt is a new request (new nonce, new issue time) for the
    // same immutable batch: the same commit answers it.
    clock.fetch_add(1_000, Ordering::SeqCst);
    let second = delivery.submit(&batch).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(peer.receiver.commits(), 1);
    assert_eq!(peer.receiver.submits(), 2);
    match delivery.status(&batch).await.unwrap() {
        NativeStatus::Applied { receipt, .. } => assert_eq!(receipt, first),
        other => panic!("expected applied, got {other:?}"),
    }
    assert_eq!(
        delivery.input(&batch).await.unwrap(),
        Some(serde_json::json!({ "phase": "consumed" }))
    );
    // Other content under the same batch id is a signed conflict, which
    // settles the batch rather than retrying it forever.
    let mut changed = remote_batch("batch-1", "a different announcement");
    changed.state = BatchState::Submitting { attempt: 2 };
    assert!(matches!(
        delivery.submit(&changed).await,
        Err(NativeSubmitError::Conflict(_))
    ));
    assert_eq!(peer.receiver.commits(), 1);
}

#[tokio::test]
async fn an_answer_signed_by_any_key_but_the_pinned_one_is_not_evidence() {
    let real_peer_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let impostor_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let caller_keys = Arc::new(GatewayPeerKeys::ephemeral());
    // The listener answers with a key other than the one the caller pinned.
    let peer = serve(
        &impostor_keys,
        ControlAuthorizer::with_grants_for_audience(
            keyed_inject_grant(
                &caller_keys,
                &[ControlVerb::InjectKeyed, ControlVerb::InjectStatus],
            ),
            PEER,
        ),
    )
    .await;
    let delivery = caller(
        contacts(PEER, &peer.address, &real_peer_keys),
        &caller_keys,
        Arc::new(AtomicU64::new(NOW_MS)),
    );
    let batch = remote_batch("batch-1", "card declined");
    match delivery.submit(&batch).await {
        Err(NativeSubmitError::Unavailable(detail)) => {
            assert!(detail.contains("not authenticated"), "{detail}");
        }
        other => panic!("an unverifiable receipt must leave the outcome unknown: {other:?}"),
    }
    assert_eq!(
        peer.receiver.commits(),
        1,
        "the peer did commit; the caller just cannot know it from this answer"
    );
    assert!(
        delivery.status(&batch).await.is_err(),
        "an unverifiable status is no evidence either"
    );
}

#[test]
fn a_signature_over_other_request_bytes_does_not_verify() {
    let keys = GatewayPeerKeys::ephemeral();
    let receipt = NativeReceipt {
        delivery_id: "d".into(),
        address: "a".into(),
        submission_digest: "s".into(),
        committed_at_ms: 1,
    };
    let mut response = ControlResponse::InjectReceipt {
        receipt,
        sig_b64: None,
    };
    let answered = br#"{"op":"inject_keyed","nonce":"first"}"#;
    let replayed = br#"{"op":"inject_keyed","nonce":"second"}"#;
    let payload = control_response_signing_payload(answered, &response).unwrap();
    let signature = keys.signing_key().sign(payload.as_bytes());
    if let ControlResponse::InjectReceipt { sig_b64, .. } = &mut response {
        *sig_b64 = Some(base64::engine::general_purpose::STANDARD.encode(signature.to_bytes()));
    }
    assert_eq!(
        verify_control_response(&keys.pubkey_bytes(), answered, &response),
        Ok(()),
        "the positive control: the answer verifies for the request it answered"
    );
    assert!(
        verify_control_response(&keys.pubkey_bytes(), replayed, &response).is_err(),
        "the same signed answer cannot settle another request"
    );
    let mut unsigned = response;
    if let ControlResponse::InjectReceipt { sig_b64, .. } = &mut unsigned {
        *sig_b64 = None;
    }
    assert!(
        verify_control_response(&keys.pubkey_bytes(), answered, &unsigned).is_err(),
        "an unsigned keyed inject answer never verifies"
    );
}

#[tokio::test]
async fn a_request_minted_for_another_audience_is_refused() {
    let peer_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let caller_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let peer = serve(
        &peer_keys,
        ControlAuthorizer::with_grants_for_audience(
            keyed_inject_grant(
                &caller_keys,
                &[ControlVerb::InjectKeyed, ControlVerb::InjectStatus],
            ),
            PEER,
        ),
    )
    .await;
    // The caller files the same listener under another mob id, so it signs
    // its requests for that audience.
    let delivery = caller(
        contacts("mob-c", &peer.address, &peer_keys),
        &caller_keys,
        Arc::new(AtomicU64::new(NOW_MS)),
    );
    let mut batch = remote_batch("batch-1", "card declined");
    batch.member = Member::new(
        MobId::new("mob-c").unwrap(),
        MemberIdentity::new(MEMBER).unwrap(),
    );
    match delivery.submit(&batch).await {
        Err(NativeSubmitError::Unavailable(detail)) => {
            assert!(detail.contains("audience_mismatch"), "{detail}");
        }
        other => panic!("expected an audience refusal: {other:?}"),
    }
    assert_eq!(peer.receiver.submits(), 0, "nothing reached the owner");
}

#[tokio::test]
async fn keyed_inject_verbs_need_a_grant_that_names_them() {
    let peer_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let caller_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let batch = remote_batch("batch-1", "card declined");

    // `*` expands to the member plane only.
    let star = ControlGrantTable::from_toml(&format!(
        "[control_grants.calling-gateway]\npubkey = \"{}\"\nverbs = [\"*\"]\n",
        caller_keys.pubkey_b64()
    ))
    .unwrap()
    .unwrap();
    let peer = serve(
        &peer_keys,
        ControlAuthorizer::with_grants_for_audience(star, PEER),
    )
    .await;
    let delivery = caller(
        contacts(PEER, &peer.address, &peer_keys),
        &caller_keys,
        Arc::new(AtomicU64::new(NOW_MS)),
    );
    match delivery.submit(&batch).await {
        Err(NativeSubmitError::Unavailable(detail)) => {
            assert!(detail.contains("verb_not_granted"), "{detail}");
        }
        other => panic!("a * grant must not cover keyed inject verbs: {other:?}"),
    }
    assert_eq!(peer.receiver.submits(), 0);

    // An open listener authorizes nothing, so it refuses them outright.
    let open = serve(&peer_keys, ControlAuthorizer::open()).await;
    let delivery = caller(
        contacts(PEER, &open.address, &peer_keys),
        &caller_keys,
        Arc::new(AtomicU64::new(NOW_MS)),
    );
    match delivery.submit(&batch).await {
        Err(NativeSubmitError::Unavailable(detail)) => {
            assert!(detail.contains("grants_required"), "{detail}");
        }
        other => panic!("an open listener must refuse keyed inject verbs: {other:?}"),
    }
    assert_eq!(open.receiver.submits(), 0);

    // Member scope applies: a grant for another member does not reach
    // this one.
    let mut other_member = ControlGrantTable::new();
    other_member.insert(
        caller_keys.pubkey_bytes(),
        ControlGrant::new(
            "calling-gateway",
            [ControlVerb::InjectKeyed],
            ControlMemberScope::members(["agent:other"]),
        ),
    );
    let scoped = serve(
        &peer_keys,
        ControlAuthorizer::with_grants_for_audience(other_member, PEER),
    )
    .await;
    let delivery = caller(
        contacts(PEER, &scoped.address, &peer_keys),
        &caller_keys,
        Arc::new(AtomicU64::new(NOW_MS)),
    );
    match delivery.submit(&batch).await {
        Err(NativeSubmitError::Unavailable(detail)) => {
            assert!(detail.contains("member_not_granted"), "{detail}");
        }
        other => panic!("expected a member-scope refusal: {other:?}"),
    }
}

#[tokio::test]
async fn the_freshness_window_admits_its_edges_and_nothing_beyond() {
    let (peer, delivery, clock) = granted_pair().await;
    let cases = [
        ("batch-old-edge", NOW_MS - WINDOW_MS, true),
        ("batch-new-edge", NOW_MS + WINDOW_MS, true),
        ("batch-too-old", NOW_MS - WINDOW_MS - 1, false),
        ("batch-too-new", NOW_MS + WINDOW_MS + 1, false),
    ];
    for (id, issued_at, fresh) in cases {
        clock.store(issued_at, Ordering::SeqCst);
        let batch = remote_batch(id, "card declined");
        let outcome = delivery.submit(&batch).await;
        if fresh {
            assert!(outcome.is_ok(), "{id}: {outcome:?}");
        } else {
            match outcome {
                Err(NativeSubmitError::Unavailable(detail)) => {
                    assert!(detail.starts_with("stale_request"), "{id}: {detail}");
                }
                other => panic!("{id}: a stale request must be refused: {other:?}"),
            }
        }
    }
    assert_eq!(
        peer.receiver.commits(),
        2,
        "only the two requests inside the window committed"
    );
}

#[tokio::test]
async fn only_signed_refusals_settle_a_batch_or_end_its_member() {
    let (peer, delivery, _clock) = granted_pair().await;
    let batch = remote_batch("batch-1", "card declined");
    let cases = [
        (KeyedInjectRefusalCode::Conflict, "conflict"),
        (KeyedInjectRefusalCode::InvalidRequest, "invalid"),
        (KeyedInjectRefusalCode::Governed, "governed"),
        (KeyedInjectRefusalCode::OwnerRetired, "owner_retired"),
        (KeyedInjectRefusalCode::UnknownMember, "member_left"),
        (KeyedInjectRefusalCode::UnusableMember, "member_unusable"),
        (KeyedInjectRefusalCode::NativeUnavailable, "unavailable"),
        (KeyedInjectRefusalCode::Unavailable, "unavailable"),
        (KeyedInjectRefusalCode::StaleRequest, "unavailable"),
    ];
    for (code, expected) in cases {
        *peer.receiver.refuse_next.lock().unwrap() =
            Some(KeyedInjectRefusal::new(code, "scripted"));
        let outcome = delivery.submit(&batch).await;
        let kind = match &outcome {
            Err(NativeSubmitError::Conflict(_)) => "conflict",
            Err(NativeSubmitError::Invalid(_)) => "invalid",
            Err(NativeSubmitError::Governed) => "governed",
            Err(NativeSubmitError::OwnerRetired) => "owner_retired",
            Err(NativeSubmitError::MemberGone(SubscriptionEnd::MemberLeft)) => "member_left",
            Err(NativeSubmitError::MemberGone(SubscriptionEnd::MemberUnusable { .. })) => {
                "member_unusable"
            }
            Err(NativeSubmitError::MemberGone(_)) => "other_end",
            Err(NativeSubmitError::Unavailable(_)) => "unavailable",
            Ok(_) => "receipt",
        };
        assert_eq!(kind, expected, "{code}: {outcome:?}");
    }
    assert_eq!(peer.receiver.commits(), 0);
}

#[tokio::test]
async fn an_unusable_contact_is_refused_before_anything_is_sent() {
    let peer_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let caller_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let member = Member::new(
        MobId::new(PEER).unwrap(),
        MemberIdentity::new(MEMBER).unwrap(),
    );
    let unpinned = ContactDirectory::from_toml(&format!(
        "[mobs]\n\"{PEER}\" = {{ transport = \"tcp://127.0.0.1:9\" }}\n"
    ))
    .unwrap();
    let opted_out = ContactDirectory::from_toml(&format!(
        "[mobs]\n\"{PEER}\" = {{ transport = \"tcp://127.0.0.1:9\", pubkey = \"{}\", \
         require_signed_control = false }}\n",
        peer_keys.pubkey_b64()
    ))
    .unwrap();
    let absent = ContactDirectory::from_toml("[mobs]\n").unwrap();
    for directory in [unpinned, opted_out, absent] {
        let delivery = caller(directory, &caller_keys, Arc::new(AtomicU64::new(NOW_MS)));
        assert!(
            matches!(
                delivery.check_member(&member),
                Err(super::native::MemberProblem::UnusablePeer(_))
            ),
            "a contact that cannot authenticate the peer is refused"
        );
    }
    // No signing key on this gateway.
    let keyless = RemoteMemberDelivery::with_clock(
        contacts(PEER, "tcp://127.0.0.1:9", &peer_keys),
        Arc::new(RwLock::new(None)),
        Arc::new(|| NOW_MS),
    );
    assert!(matches!(
        keyless.check_member(&member),
        Err(super::native::MemberProblem::UnusablePeer(_))
    ));
    // A member must be a durable identity in its public form.
    let delivery = caller(
        contacts(PEER, "tcp://127.0.0.1:9", &peer_keys),
        &caller_keys,
        Arc::new(AtomicU64::new(NOW_MS)),
    );
    let encoded = Member::new(
        MobId::new(PEER).unwrap(),
        MemberIdentity::new("mk--agent_ca").unwrap(),
    );
    assert!(matches!(
        delivery.check_member(&encoded),
        Err(super::native::MemberProblem::Invalid(_))
    ));
    assert_eq!(delivery.check_member(&member), Ok(()));
}

#[test]
fn a_tampered_request_fails_its_caller_signature() {
    let caller_keys = GatewayPeerKeys::ephemeral();
    let authorizer = ControlAuthorizer::with_grants_for_audience(
        keyed_inject_grant(&caller_keys, &[ControlVerb::InjectKeyed]),
        PEER,
    );
    let body = "Mob announcements batch-1: 1 new announcement for your subscriptions.\n";
    let mut request = ControlRequest::InjectKeyed {
        remote_member: MEMBER.into(),
        delivery_key: "batch-1".into(),
        content_digest: content_digest(body),
        summary: "1 announcement".into(),
        body: body.into(),
        issued_at_ms: NOW_MS,
        nonce: "n1".into(),
        caller: None,
    };
    sign_control_request_as_caller(&caller_keys, Some(PEER), &mut request);
    assert!(
        authorizer.authorize(&request).is_ok(),
        "the positive control"
    );
    for tamper in 0..4 {
        let mut tampered = request.clone();
        if let ControlRequest::InjectKeyed {
            body,
            issued_at_ms,
            delivery_key,
            remote_member,
            ..
        } = &mut tampered
        {
            match tamper {
                0 => body.push_str("an injected line\n"),
                1 => *issued_at_ms += 1,
                2 => delivery_key.push('x'),
                _ => *remote_member = "agent:other".into(),
            }
        }
        assert!(
            authorizer.authorize(&tampered).is_err(),
            "tamper case {tamper} must not authorize"
        );
    }
}

#[test]
fn a_delivery_whose_body_does_not_match_its_digest_is_invalid() {
    let body = "Mob announcements batch-1: 1 new announcement for your subscriptions.\n";
    let digest = content_digest(body);
    let valid = KeyedInject {
        member: MEMBER,
        delivery_key: "batch-1",
        content_digest: &digest,
        summary: "1 announcement",
        body,
    };
    assert!(validate_delivery(&valid).is_ok());
    let mismatched = KeyedInject {
        body: "something else",
        ..valid
    };
    assert_eq!(
        validate_delivery(&mismatched).unwrap_err().code,
        KeyedInjectRefusalCode::InvalidRequest
    );
    let long_summary = "s".repeat(MAX_ID_BYTES + 1);
    let oversized = KeyedInject {
        summary: &long_summary,
        ..valid
    };
    assert_eq!(
        validate_delivery(&oversized).unwrap_err().code,
        KeyedInjectRefusalCode::InvalidRequest
    );
}

#[tokio::test]
async fn an_unreachable_peer_fails_fast_for_a_moment_and_is_tried_again() {
    let peer_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let caller_keys = Arc::new(GatewayPeerKeys::ephemeral());
    // A port nothing listens on: the connection is refused at once.
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("tcp://{}", closed.local_addr().unwrap());
    drop(closed);
    let clock = Arc::new(AtomicU64::new(NOW_MS));
    let delivery = caller(
        contacts(PEER, &address, &peer_keys),
        &caller_keys,
        Arc::clone(&clock),
    );
    let batch = remote_batch("batch-1", "card declined");
    let first = delivery.submit(&batch).await;
    assert!(
        matches!(&first, Err(NativeSubmitError::Unavailable(detail)) if !detail.contains("moments ago")),
        "{first:?}"
    );
    let second = delivery.submit(&batch).await;
    assert!(
        matches!(&second, Err(NativeSubmitError::Unavailable(detail)) if detail.contains("moments ago")),
        "a call inside the window fails without dialing: {second:?}"
    );
    assert!(delivery.status(&batch).await.is_err());
    clock.fetch_add(super::remote::PEER_BACKOFF_MS, Ordering::SeqCst);
    let third = delivery.submit(&batch).await;
    assert!(
        matches!(&third, Err(NativeSubmitError::Unavailable(detail)) if !detail.contains("moments ago")),
        "after the window the peer is dialed again: {third:?}"
    );
}

#[tokio::test]
async fn a_gateway_that_serves_no_keyed_inject_answers_a_signed_unavailable() {
    // An empty slot: a build without the feature, or a gateway that did
    // not enable it. The refusal is still signed, and it settles nothing.
    let peer_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let caller_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("tcp://{}", listener.local_addr().unwrap());
    tokio::spawn(serve_tcp_control_with_authorizer(
        listener,
        Arc::new(KeyedInjectHandler {
            slot: crate::runtime::keyed_inject::empty_keyed_inject_slot(),
        }),
        signer(&peer_keys),
        Arc::new(ControlAuthorizer::with_grants_for_audience(
            keyed_inject_grant(
                &caller_keys,
                &[ControlVerb::InjectKeyed, ControlVerb::InjectStatus],
            ),
            PEER,
        )),
    ));
    let delivery = caller(
        contacts(PEER, &address, &peer_keys),
        &caller_keys,
        Arc::new(AtomicU64::new(NOW_MS)),
    );
    let batch = remote_batch("batch-1", "card declined");
    match delivery.submit(&batch).await {
        Err(NativeSubmitError::Unavailable(detail)) => {
            assert!(detail.starts_with("keyed_inject_unavailable"), "{detail}");
        }
        other => panic!("expected a signed unavailable refusal: {other:?}"),
    }
}

/// Two gateways granted on one member: neither can commit under the other's
/// batch id, nor be answered about the other's batch.
#[tokio::test]
async fn a_second_granted_caller_neither_takes_over_nor_answers_for_anothers_batch() {
    let peer_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let first_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let second_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let verbs = [ControlVerb::InjectKeyed, ControlVerb::InjectStatus];
    let mut table = ControlGrantTable::new();
    file_keyed_inject_grant(&mut table, "first-gateway", &first_keys, &verbs);
    file_keyed_inject_grant(&mut table, "second-gateway", &second_keys, &verbs);
    let peer = serve(
        &peer_keys,
        ControlAuthorizer::with_grants_for_audience(table, PEER),
    )
    .await;
    let clock = Arc::new(AtomicU64::new(NOW_MS));
    let first = caller(
        contacts(PEER, &peer.address, &peer_keys),
        &first_keys,
        Arc::clone(&clock),
    );
    let second = caller(
        contacts(PEER, &peer.address, &peer_keys),
        &second_keys,
        Arc::clone(&clock),
    );
    let mine = remote_batch("batch-shared", "the first caller's announcement");
    let borrowed = remote_batch("batch-shared", "the second caller's content");

    // The second caller commits its own body under the first caller's id.
    let theirs = second.submit(&borrowed).await.unwrap();
    // The first caller is told nothing about it.
    assert_eq!(
        first.status(&mine).await.unwrap(),
        NativeStatus::NotCommitted
    );
    let own = first.submit(&mine).await.unwrap();
    assert_ne!(own.delivery_id, theirs.delivery_id);
    assert_ne!(own.submission_digest, theirs.submission_digest);
    assert_eq!(peer.receiver.commits(), 2);
    match first.status(&mine).await.unwrap() {
        NativeStatus::Applied { receipt, .. } => assert_eq!(receipt, own),
        other => panic!("expected the first caller's own delivery, got {other:?}"),
    }
    match second.status(&borrowed).await.unwrap() {
        NativeStatus::Applied { receipt, .. } => assert_eq!(receipt, theirs),
        other => panic!("expected the second caller's own delivery, got {other:?}"),
    }
    // A caller's replay of its own key is still its original receipt.
    clock.fetch_add(1_000, Ordering::SeqCst);
    assert_eq!(first.submit(&mine).await.unwrap(), own);
    assert_eq!(peer.receiver.commits(), 2);
}

/// One grant label is one caller: a key rotation under the label keeps its
/// deliveries.
#[tokio::test]
async fn a_key_rotation_under_one_grant_label_keeps_the_callers_deliveries() {
    let peer_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let old_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let new_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let verbs = [ControlVerb::InjectKeyed, ControlVerb::InjectStatus];
    let mut table = ControlGrantTable::new();
    file_keyed_inject_grant(&mut table, "rotating-gateway", &old_keys, &verbs);
    file_keyed_inject_grant(&mut table, "rotating-gateway", &new_keys, &verbs);
    let peer = serve(
        &peer_keys,
        ControlAuthorizer::with_grants_for_audience(table, PEER),
    )
    .await;
    let clock = Arc::new(AtomicU64::new(NOW_MS));
    let before = caller(
        contacts(PEER, &peer.address, &peer_keys),
        &old_keys,
        Arc::clone(&clock),
    );
    let after = caller(
        contacts(PEER, &peer.address, &peer_keys),
        &new_keys,
        Arc::clone(&clock),
    );
    let batch = remote_batch("batch-rotated", "an announcement");
    let receipt = before.submit(&batch).await.unwrap();
    match after.status(&batch).await.unwrap() {
        NativeStatus::Applied { receipt: seen, .. } => assert_eq!(seen, receipt),
        other => panic!("expected the delivery made before the rotation, got {other:?}"),
    }
    assert_eq!(after.submit(&batch).await.unwrap(), receipt);
    assert_eq!(peer.receiver.commits(), 1);
}

/// Nothing issued before the serving process started is served after it,
/// even inside the freshness window.
#[tokio::test]
async fn a_request_issued_before_the_serving_process_started_is_stale() {
    let (peer, delivery, clock) = granted_pair().await;
    peer.receiver.started_at.store(NOW_MS, Ordering::SeqCst);
    clock.store(NOW_MS - 1, Ordering::SeqCst);
    let batch = remote_batch("batch-before-start", "an announcement");
    match delivery.submit(&batch).await {
        Err(NativeSubmitError::Unavailable(detail)) => {
            assert!(detail.contains("stale_request"), "{detail}");
        }
        other => panic!("expected a stale refusal, got {other:?}"),
    }
    assert_eq!(peer.receiver.submits(), 0);
    clock.store(NOW_MS, Ordering::SeqCst);
    delivery.submit(&batch).await.unwrap();
    assert_eq!(peer.receiver.commits(), 1);
}

/// A keyed inject verb reaching the dispatch without an admitting caller is never
/// served.
#[tokio::test]
async fn a_keyed_inject_verb_without_an_admitting_caller_is_refused() {
    let receiver = ScriptedReceiver::new();
    let slot: KeyedInjectSlot = Arc::new(RwLock::new(Some(
        Arc::clone(&receiver) as Arc<dyn KeyedInjectReceiver>
    )));
    let body = "a body".to_string();
    let request = ControlRequest::InjectKeyed {
        remote_member: MEMBER.to_string(),
        delivery_key: "batch-unadmitted".to_string(),
        content_digest: content_digest(&body),
        summary: "1 announcement".to_string(),
        body,
        issued_at_ms: NOW_MS,
        nonce: "n".to_string(),
        caller: None,
    };
    match answer_keyed_inject_request(&slot, request, None).await {
        ControlResponse::InjectRefused { code, .. } => {
            assert_eq!(code, KeyedInjectRefusalCode::Unavailable.as_str());
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(receiver.submits(), 0);
}

/// A gateway that predates keyed inject cannot decode the request, and its
/// listener says so unsigned. That gateway never will serve it, so the
/// member's subscriptions end typed instead of retrying forever.
#[tokio::test]
async fn a_gateway_that_predates_keyed_inject_ends_the_subscription_typed() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let peer_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let caller_keys = Arc::new(GatewayPeerKeys::ephemeral());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("tcp://{}", listener.local_addr().unwrap());
    let old_peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).await.unwrap();
        let mut request = vec![0u8; u32::from_be_bytes(header) as usize];
        stream.read_exact(&mut request).await.unwrap();
        let answer = serde_json::to_vec(&ControlResponse::Err {
            code: "decode".to_string(),
            message: "unknown variant `inject_keyed`".to_string(),
        })
        .unwrap();
        stream
            .write_all(&u32::try_from(answer.len()).unwrap().to_be_bytes())
            .await
            .unwrap();
        stream.write_all(&answer).await.unwrap();
        stream.flush().await.unwrap();
    });
    let delivery = caller(
        contacts(PEER, &address, &peer_keys),
        &caller_keys,
        Arc::new(AtomicU64::new(NOW_MS)),
    );
    assert_eq!(
        delivery
            .submit(&remote_batch("batch-1", "card declined"))
            .await,
        Err(NativeSubmitError::MemberGone(
            SubscriptionEnd::PeerUnsupported
        ))
    );
    old_peer.await.unwrap();
}
