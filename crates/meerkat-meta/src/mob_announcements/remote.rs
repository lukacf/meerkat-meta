//! Mob announcement delivery between gateways, over keyed inject.
//!
//! [`RemoteMemberDelivery`] is the calling side: it delivers a batch whose
//! member lives on a peer gateway with `inject_keyed`, and reads that
//! batch's native status there with `inject_status`, over the signed control
//! channel. [`KeyedInjectServer`] is the serving side: it submits what a peer
//! delivers to this gateway's own continuation owner. See
//! [`crate::runtime::keyed_inject`] for the signing, freshness and evidence
//! rules both sides follow.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use super::native::{
    IdentityService, MemberProblem, MobNativeDelivery, NativeReceipt, NativeStatus,
    NativeSubmitError, NativeUnavailable, PrepareError, batch_summary, continuation_delivery,
};
use super::types::{
    Batch, BatchId, MAX_ID_BYTES, Member, MemberIdentity, MobId, SubscriptionEnd, content_digest,
};
use crate::runtime::cross_mob_control::ControlSignerSlot;
use crate::runtime::cross_mob_remote::RemoteMobProxy;
use crate::runtime::keyed_inject::{
    KeyedInject, KeyedInjectCallError, KeyedInjectCallOutcome, KeyedInjectCallerScope,
    KeyedInjectFuture, KeyedInjectReceiver, KeyedInjectRefusal, KeyedInjectRefusalCode,
    KeyedInjectStatusAnswer,
};

pub(crate) type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

pub(crate) fn system_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// After a peer could not be reached, calls to it fail fast for this long.
/// This keeps one unreachable peer from stalling a dispatcher round on
/// connection timeouts; it decides nothing about any delivery, which stays
/// owed and is asked about again.
pub(crate) const PEER_BACKOFF_MS: u64 = 5_000;

/// The calling side: delivers to members on peer gateways named in this
/// gateway's contact directory, authenticated as this gateway's own key.
pub struct RemoteMemberDelivery {
    contacts: crate::contact_directory::ContactDirectory,
    keys: ControlSignerSlot,
    clock: Clock,
    unreachable_until: Mutex<BTreeMap<MobId, u64>>,
}

impl RemoteMemberDelivery {
    pub(crate) fn new(
        contacts: crate::contact_directory::ContactDirectory,
        keys: ControlSignerSlot,
    ) -> Self {
        Self::with_clock(contacts, keys, Arc::new(system_clock))
    }

    /// `clock` stamps each request's issue time: this gateway's wall clock,
    /// which the peer compares with its own.
    pub(crate) fn with_clock(
        contacts: crate::contact_directory::ContactDirectory,
        keys: ControlSignerSlot,
        clock: Clock,
    ) -> Self {
        Self {
            contacts,
            keys,
            clock,
            unreachable_until: Mutex::new(BTreeMap::new()),
        }
    }

    fn reachable(&self, peer: &MobId) -> Result<(), KeyedInjectCallError> {
        let now = (self.clock)();
        let unreachable = self
            .unreachable_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match unreachable.get(peer) {
            Some(until) if now < *until => Err(KeyedInjectCallError::Transport(format!(
                "'{peer}' could not be reached moments ago; the next attempt is at {until} ms"
            ))),
            _ => Ok(()),
        }
    }

    /// Remember whether the peer answered at all. Any answer, a refusal
    /// included, means it is reachable.
    fn note<T>(&self, peer: &MobId, outcome: &Result<T, KeyedInjectCallError>) {
        let mut unreachable = self
            .unreachable_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match outcome {
            Err(KeyedInjectCallError::Transport(_)) => {
                unreachable.insert(peer.clone(), (self.clock)().saturating_add(PEER_BACKOFF_MS));
            }
            _ => {
                unreachable.remove(peer);
            }
        }
    }

    fn proxy(&self, peer: &MobId) -> Result<RemoteMobProxy, KeyedInjectCallError> {
        let entry = self.contacts.get(peer.as_str()).ok_or_else(|| {
            KeyedInjectCallError::NotAuthenticatable(format!(
                "the contact directory has no entry for '{peer}'"
            ))
        })?;
        let keys = self
            .keys
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let proxy = RemoteMobProxy::from_entry_with_caller(entry, keys)
            .map_err(|error| KeyedInjectCallError::NotAuthenticatable(error.to_string()))?
            .ok_or_else(|| {
                KeyedInjectCallError::NotAuthenticatable(format!(
                    "the contact for '{peer}' is in-process, not a peer gateway"
                ))
            })?;
        proxy.keyed_inject_ready()?;
        Ok(proxy)
    }

    /// The peer gateway a member's mob is filed under in the contact
    /// directory: the member's own mob id.
    fn peer_of(member: &Member) -> &MobId {
        &member.mob_id
    }

    /// Whether the contact directory files a peer under `mob_id`.
    pub(crate) fn files(&self, mob_id: &MobId) -> bool {
        self.contacts.get(mob_id.as_str()).is_some()
    }

    /// A member on a peer is deliverable when its identity is in public
    /// form and the peer can be called authenticated. Nothing is sent.
    pub(crate) fn check_member(&self, member: &Member) -> Result<(), MemberProblem> {
        let identity = member.identity.as_str();
        if crate::member_comms_id::runtime_alias_str(identity) != identity
            || crate::member_comms_id::is_reserved_generated_alias(identity)
        {
            return Err(MemberProblem::Invalid(format!(
                "'{identity}' is not a durable identity in its public form"
            )));
        }
        let peer = Self::peer_of(member);
        self.proxy(peer)
            .map(|_| ())
            .map_err(|error| MemberProblem::UnusablePeer(error.to_string()))
    }

    /// Deliver the batch to its peer. Only the peer's signed answer is
    /// evidence; everything else leaves the outcome unknown.
    pub(crate) async fn submit(&self, batch: &Batch) -> Result<NativeReceipt, NativeSubmitError> {
        let unavailable =
            |error: KeyedInjectCallError| NativeSubmitError::Unavailable(error.to_string());
        let peer = Self::peer_of(&batch.member);
        self.reachable(peer).map_err(unavailable)?;
        let proxy = self.proxy(peer).map_err(unavailable)?;
        let summary = batch_summary(batch);
        let outcome = proxy
            .inject_keyed(
                KeyedInject {
                    member: batch.member.identity.as_str(),
                    delivery_key: batch.id.as_str(),
                    content_digest: &batch.content_digest,
                    summary: &summary,
                    body: &batch.body,
                },
                (self.clock)(),
            )
            .await;
        self.note(peer, &outcome);
        match outcome {
            Ok(KeyedInjectCallOutcome::Answered(receipt)) => Ok(receipt),
            // A signed refusal is the peer's own word about its member.
            Ok(KeyedInjectCallOutcome::Refused { code, message }) => {
                Err(match KeyedInjectRefusalCode::parse(&code) {
                    Some(KeyedInjectRefusalCode::Conflict) => NativeSubmitError::Conflict(message),
                    Some(KeyedInjectRefusalCode::InvalidRequest) => {
                        NativeSubmitError::Invalid(message)
                    }
                    Some(KeyedInjectRefusalCode::Governed) => NativeSubmitError::Governed,
                    Some(KeyedInjectRefusalCode::OwnerRetired) => NativeSubmitError::OwnerRetired,
                    Some(KeyedInjectRefusalCode::UnknownMember) => {
                        NativeSubmitError::MemberGone(SubscriptionEnd::MemberLeft)
                    }
                    Some(KeyedInjectRefusalCode::UnusableMember) => {
                        NativeSubmitError::MemberGone(SubscriptionEnd::MemberUnusable {
                            detail: message,
                        })
                    }
                    _ => NativeSubmitError::Unavailable(format!("{code}: {message}")),
                })
            }
            // A gateway that predates keyed inject cannot decode the request
            // and says so, unsigned. It never will serve it: the
            // subscription ends instead of retrying forever. Being
            // unsigned, this decides availability only, which an on-path
            // party could deny anyway.
            Err(KeyedInjectCallError::Rejected { code, .. }) if code == "decode" => Err(
                NativeSubmitError::MemberGone(SubscriptionEnd::PeerUnsupported),
            ),
            // A request this gateway built too large can never be sent; it
            // is the one failure the batch itself causes.
            Err(KeyedInjectCallError::Oversize { bytes }) => Err(NativeSubmitError::Invalid(
                format!("the delivery request is {bytes} bytes, more than one control frame"),
            )),
            Err(error) => Err(unavailable(error)),
        }
    }

    async fn read(
        &self,
        batch: &Batch,
        with_input: bool,
    ) -> Result<KeyedInjectStatusAnswer, String> {
        let peer = Self::peer_of(&batch.member);
        self.reachable(peer).map_err(|error| error.to_string())?;
        let proxy = self.proxy(peer).map_err(|error| error.to_string())?;
        let outcome = proxy
            .inject_status(
                batch.member.identity.as_str(),
                batch.id.as_str(),
                with_input,
                (self.clock)(),
            )
            .await;
        self.note(peer, &outcome);
        match outcome {
            Ok(KeyedInjectCallOutcome::Answered(answer)) => Ok(answer),
            Ok(KeyedInjectCallOutcome::Refused { code, message }) => {
                Err(format!("{code}: {message}"))
            }
            Err(error) => Err(error.to_string()),
        }
    }

    /// The batch's native status on its peer.
    pub(crate) async fn status(&self, batch: &Batch) -> Result<NativeStatus, NativeUnavailable> {
        self.read(batch, false)
            .await
            .map(|answer| answer.status)
            .map_err(NativeUnavailable)
    }

    /// The admitted input's state on the batch's peer.
    pub(crate) async fn input(&self, batch: &Batch) -> Result<Option<Value>, NativeUnavailable> {
        self.read(batch, true)
            .await
            .map(|answer| answer.input)
            .map_err(NativeUnavailable)
    }
}

/// The serving side: submits what a peer delivers to this gateway's own
/// continuation owner, as the continuation the peer's batch is, keyed under
/// the scope of the caller the grant admitted.
pub struct KeyedInjectServer {
    native: Arc<MobNativeDelivery>,
    window_ms: u64,
    clock: Clock,
    /// When this receiver was built: no request issued earlier is served.
    started_at_ms: u64,
}

impl KeyedInjectServer {
    pub(crate) fn new(native: Arc<MobNativeDelivery>, window_ms: u64) -> Self {
        Self::with_clock(native, window_ms, Arc::new(system_clock))
    }

    pub(crate) fn with_clock(native: Arc<MobNativeDelivery>, window_ms: u64, clock: Clock) -> Self {
        let started_at_ms = clock();
        Self {
            native,
            window_ms,
            clock,
            started_at_ms,
        }
    }
}

fn invalid(detail: impl std::fmt::Display) -> KeyedInjectRefusal {
    KeyedInjectRefusal::new(KeyedInjectRefusalCode::InvalidRequest, detail.to_string())
}

fn native_unavailable(error: NativeUnavailable) -> KeyedInjectRefusal {
    KeyedInjectRefusal::new(KeyedInjectRefusalCode::NativeUnavailable, error.0)
}

fn hosted_identity(member: &str) -> Result<MemberIdentity, KeyedInjectRefusal> {
    let identity = MemberIdentity::new(member).map_err(invalid)?;
    crate::identity_first::AgentIdentity::parse(identity.as_str()).map_err(invalid)?;
    Ok(identity)
}

/// The checks a delivery passes before anything native is touched: valid
/// ids, a bounded summary, and a body that matches its digest.
pub(crate) fn validate_delivery(
    delivery: &KeyedInject<'_>,
) -> Result<MemberIdentity, KeyedInjectRefusal> {
    let identity = hosted_identity(delivery.member)?;
    BatchId::new(delivery.delivery_key).map_err(invalid)?;
    if delivery.summary.len() > MAX_ID_BYTES {
        return Err(invalid(format!(
            "the summary is longer than {MAX_ID_BYTES} bytes"
        )));
    }
    if content_digest(delivery.body) != delivery.content_digest {
        return Err(invalid("the body does not match its content digest"));
    }
    Ok(identity)
}

impl KeyedInjectReceiver for KeyedInjectServer {
    fn window_ms(&self) -> u64 {
        self.window_ms
    }

    fn now_ms(&self) -> u64 {
        (self.clock)()
    }

    fn started_at_ms(&self) -> u64 {
        self.started_at_ms
    }

    fn deliver<'a>(
        &'a self,
        scope: &'a KeyedInjectCallerScope,
        delivery: KeyedInject<'a>,
    ) -> KeyedInjectFuture<'a, Result<NativeReceipt, KeyedInjectRefusal>> {
        Box::pin(async move {
            let identity = validate_delivery(&delivery)?;
            match self.native.governs_work_authority() {
                Ok(false) => {}
                Ok(true) => {
                    return Err(KeyedInjectRefusal::new(
                        KeyedInjectRefusalCode::Governed,
                        "this gateway's runtime admits work only through a native work \
                         authorization host, which refuses host continuations",
                    ));
                }
                Err(error) => return Err(native_unavailable(error)),
            }
            match self
                .native
                .identity_service(&identity)
                .await
                .map_err(native_unavailable)?
            {
                IdentityService::Local => {}
                IdentityService::Unknown => {
                    return Err(KeyedInjectRefusal::new(
                        KeyedInjectRefusalCode::UnknownMember,
                        format!("{identity} is not an identity registered with this gateway"),
                    ));
                }
                IdentityService::Elsewhere(detail) => {
                    return Err(KeyedInjectRefusal::new(
                        KeyedInjectRefusalCode::UnusableMember,
                        detail,
                    ));
                }
            }
            self.native
                .prepare_identity(&identity)
                .await
                .map_err(|error| match error {
                    PrepareError::Gone(SubscriptionEnd::MemberUnusable { detail }) => {
                        KeyedInjectRefusal::new(KeyedInjectRefusalCode::UnusableMember, detail)
                    }
                    PrepareError::Gone(end) => KeyedInjectRefusal::new(
                        KeyedInjectRefusalCode::UnknownMember,
                        end.to_string(),
                    ),
                    PrepareError::Unavailable(detail) => {
                        KeyedInjectRefusal::new(KeyedInjectRefusalCode::NativeUnavailable, detail)
                    }
                })?;
            let continuation = continuation_delivery(
                &scope.native_key(delivery.delivery_key),
                delivery.delivery_key,
                delivery.content_digest,
                delivery.summary,
                delivery.body,
            )
            .map_err(invalid)?;
            match self
                .native
                .submit_delivery(&identity, continuation, self.now_ms())
                .await
            {
                Ok(receipt) => {
                    super::faults::after_remote_commit();
                    Ok(receipt)
                }
                Err(NativeSubmitError::OwnerRetired) => Err(KeyedInjectRefusal::new(
                    KeyedInjectRefusalCode::OwnerRetired,
                    format!("{identity} has no live incarnation; nothing was committed"),
                )),
                Err(NativeSubmitError::Governed) => Err(KeyedInjectRefusal::new(
                    KeyedInjectRefusalCode::Governed,
                    "the member's runtime refuses host continuations",
                )),
                Err(NativeSubmitError::Conflict(detail)) => Err(KeyedInjectRefusal::new(
                    KeyedInjectRefusalCode::Conflict,
                    detail,
                )),
                Err(NativeSubmitError::Invalid(detail)) => Err(invalid(detail)),
                Err(NativeSubmitError::Unavailable(detail)) => Err(KeyedInjectRefusal::new(
                    KeyedInjectRefusalCode::NativeUnavailable,
                    detail,
                )),
                Err(NativeSubmitError::MemberGone(end)) => Err(KeyedInjectRefusal::new(
                    KeyedInjectRefusalCode::UnknownMember,
                    end.to_string(),
                )),
            }
        })
    }

    fn status<'a>(
        &'a self,
        scope: &'a KeyedInjectCallerScope,
        member: &'a str,
        delivery_key: &'a str,
        with_input: bool,
    ) -> KeyedInjectFuture<'a, Result<KeyedInjectStatusAnswer, KeyedInjectRefusal>> {
        Box::pin(async move {
            let identity = hosted_identity(member)?;
            BatchId::new(delivery_key).map_err(invalid)?;
            let status = self
                .native
                .status_of(&identity, &scope.native_key(delivery_key))
                .await
                .map_err(native_unavailable)?;
            let input = match (&status, with_input) {
                (
                    NativeStatus::Applied {
                        session_id,
                        input_id,
                        ..
                    },
                    true,
                ) => self
                    .native
                    .input_state(session_id, input_id)
                    .await
                    .unwrap_or_else(|error| Some(serde_json::json!({ "unavailable": error.0 }))),
                _ => None,
            };
            Ok(KeyedInjectStatusAnswer { status, input })
        })
    }
}
