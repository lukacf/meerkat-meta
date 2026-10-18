//! The native delivery seam: how the announcement owner reaches Meerkat's
//! continuation owner for a member.
//!
//! Batches are delivered as host continuations in the
//! [`ANNOUNCEMENT_CONTINUATION_NAMESPACE`] namespace, keyed by batch id.
//! Meerkat's continuation owner commits each into the member's durable
//! runtime inbox, binds it to the incarnation current at the first
//! submission, replays an identical submission with the original receipt,
//! refuses other content under the same key, and admits it as one input when
//! the member is served. The announcement owner never decides any of that; it
//! reads it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use super::remote::RemoteMemberDelivery;
use super::types::{Batch, Member, MemberIdentity, MobId, SubscriptionEnd};
pub use crate::runtime::keyed_inject::{AdmittedInput, NativeReceipt, NativeStatus};

/// The continuation namespace announcement batches are delivered under.
pub const ANNOUNCEMENT_CONTINUATION_NAMESPACE: &str = "meta.mob_announcements";

/// A native read or preparation that could not be made now. Never evidence
/// that something did or did not happen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeUnavailable(pub String);

impl std::fmt::Display for NativeUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a submission returned no receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeSubmitError {
    /// The member has no live incarnation now; nothing was committed. Not
    /// final: a member being respawned has none for a while.
    OwnerRetired,
    /// A final answer that nothing was committed and the member will never
    /// take a delivery: its subscriptions end for this reason.
    MemberGone(SubscriptionEnd),
    /// The member's runtime admits work only through a native work
    /// authorization host, which refuses host continuations.
    Governed,
    /// The batch id is bound to other content.
    Conflict(String),
    /// The delivery can never be submitted as it is.
    Invalid(String),
    /// The submission could not be made or its answer was lost. The commit
    /// may or may not have happened: only a status read settles it.
    Unavailable(String),
}

/// Why a member cannot be subscribed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberProblem {
    /// Not an identity registered with this gateway.
    Unknown,
    /// Not a durable identity in its public form.
    Invalid(String),
    /// A member of another mob hosted on this gateway. Deliveries reach the
    /// members of this gateway's own mob and of peer gateways' mobs.
    Unsupported(String),
    /// A member this gateway's runtime does not serve (placed on a host, or
    /// bound to an external runtime), so a delivery could never be admitted.
    Unusable(String),
    /// The peer gateway cannot be called authenticated.
    UnusablePeer(String),
    /// A read the check needs could not be made now.
    Unavailable(String),
}

/// Why a member could not be made ready for a delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareError {
    /// A final fact about the member: its subscriptions end for this reason.
    Gone(SubscriptionEnd),
    /// Not now. Never evidence of anything.
    Unavailable(String),
}

impl std::fmt::Display for PrepareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Gone(end) => write!(f, "the member is gone: {end}"),
            Self::Unavailable(detail) => f.write_str(detail),
        }
    }
}

/// How a member is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberRoute {
    /// A member of this gateway's mob, through this gateway's runtime.
    Local,
    /// A member of a mob on a peer gateway, through that gateway.
    Peer,
}

/// Whether a mob named by an announcement or a subscription is hosted here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MobPresence {
    /// Managed by this gateway and not destroyed, or restored and held
    /// until its owner resolves.
    Hosted,
    /// Not hosted here: destroyed, or never created here. A mob created
    /// later under the same id is another mob.
    Gone,
}

/// What the announcement owner needs from the hosts of its members.
#[async_trait]
pub trait NativeDelivery: Send + Sync {
    /// Whether this gateway's runtime admits work only through a native work
    /// authorization host, which refuses host continuations. Asked for
    /// members on this gateway only; a peer answers for its own runtime
    /// when a batch reaches it.
    fn governs_work_authority(&self) -> Result<bool, NativeUnavailable>;

    /// How the member is reached, from this gateway's own mob id alone.
    fn route(&self, member: &Member) -> MemberRoute;

    /// Whether each named mob is hosted here, from the mob owner's own
    /// records.
    async fn mob_presence(
        &self,
        mobs: &BTreeSet<MobId>,
    ) -> Result<BTreeMap<MobId, MobPresence>, NativeUnavailable>;

    /// Changes whenever the set of mobs this gateway manages changes: a mob
    /// was created, restored or destroyed. `None` when nothing signals it.
    fn mob_set_changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        None
    }

    /// Whether the member can be subscribed: an identity registered with
    /// this gateway and served by its runtime, or one on a peer gateway this
    /// gateway can call authenticated. A peer's roster is not read here;
    /// the peer refuses an unknown member when a batch reaches it.
    async fn check_member(&self, member: &Member) -> Result<(), MemberProblem>;

    /// Make the member's current incarnation resolvable before a
    /// submission binds to it, or report the final fact that it never will
    /// be. Submits nothing. A peer prepares its own member when a batch
    /// reaches it.
    async fn prepare_member(&self, member: &Member) -> Result<(), PrepareError>;

    /// Submit the batch's exact continuation for its member.
    async fn submit(
        &self,
        batch: &Batch,
        committed_at_ms: u64,
    ) -> Result<NativeReceipt, NativeSubmitError>;

    /// The authoritative status of the batch's key, read from its
    /// member's continuation owner. Serves nothing.
    async fn status(&self, batch: &Batch) -> Result<NativeStatus, NativeUnavailable>;

    /// The native state of an input admitted for the batch's member, as
    /// that member's runtime reports it.
    async fn input(
        &self,
        batch: &Batch,
        session_id: &str,
        input_id: &str,
    ) -> Result<Option<Value>, NativeUnavailable>;
}

/// The summary a batch's continuation carries: how many distinct
/// announcements it holds.
pub fn batch_summary(batch: &Batch) -> String {
    let distinct = batch
        .items
        .iter()
        .map(|item| (item.mob_id.as_str(), item.sequence))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    format!(
        "{distinct} announcement{}",
        if distinct == 1 { "" } else { "s" }
    )
}

/// The exact continuation a delivery is submitted as under the native `key`,
/// from the batch's immutable fields alone, so every attempt submits
/// identical content. A local delivery's key is its batch id.
pub fn continuation_delivery(
    key: &str,
    batch_id: &str,
    content_digest: &str,
    summary: &str,
    body: &str,
) -> Result<meerkat::ContinuationDelivery, String> {
    let key = meerkat::ContinuationKey::new(key).map_err(|error| error.to_string())?;
    Ok(meerkat::ContinuationDelivery {
        key,
        result: meerkat::ContinuationResultRef {
            producer: meerkat::ContinuationProducer::Host {
                namespace: ANNOUNCEMENT_CONTINUATION_NAMESPACE.to_string(),
            },
            producer_id: batch_id.to_string(),
            result_digest: content_digest.to_string(),
            summary: Some(summary.to_string()),
        },
        body: meerkat::ContinuationBody::from(body.to_string()),
        handling: meerkat::ContinuationHandling::Queue,
    })
}

/// The exact continuation a batch is delivered as.
pub fn continuation_for(batch: &Batch) -> Result<meerkat::ContinuationDelivery, String> {
    continuation_delivery(
        batch.id.as_str(),
        batch.id.as_str(),
        &batch.content_digest,
        &batch_summary(batch),
        &batch.body,
    )
}

fn native_receipt(receipt: &meerkat::ContinuationReceipt) -> NativeReceipt {
    NativeReceipt {
        delivery_id: receipt.delivery_id.clone(),
        address: receipt.address.clone(),
        submission_digest: receipt.submission_digest.clone(),
        committed_at_ms: receipt.committed_at_ms,
    }
}

fn reason_text(reason: meerkat_runtime::RuntimeDeliveryRefusalReason) -> String {
    serde_json::to_value(reason)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{reason:?}"))
}

fn native_status(status: meerkat::ContinuationStatus) -> NativeStatus {
    match status {
        meerkat::ContinuationStatus::NotCommitted => NativeStatus::NotCommitted,
        meerkat::ContinuationStatus::Pending {
            receipt, admitted, ..
        } => NativeStatus::Pending {
            receipt: native_receipt(&receipt),
            admitted: admitted.map(|(session, input)| AdmittedInput {
                session_id: session.to_string(),
                input_id: input.to_string(),
            }),
        },
        meerkat::ContinuationStatus::Applied {
            receipt,
            session,
            input,
        } => NativeStatus::Applied {
            receipt: native_receipt(&receipt),
            session_id: session.to_string(),
            input_id: input.to_string(),
        },
        meerkat::ContinuationStatus::Stranded { receipt, .. } => NativeStatus::Stranded {
            receipt: native_receipt(&receipt),
        },
        meerkat::ContinuationStatus::Refused {
            receipt, reason, ..
        } => NativeStatus::Refused {
            receipt: native_receipt(&receipt),
            reason: reason_text(reason),
        },
    }
}

/// The production seam over this gateway's mob runtime.
///
/// It holds the composition's non-owning identity runtime, the agent mob
/// tool state (whose continuation service is resolved on every call, so a
/// rebound state is followed) and the runtime machine. None of them keeps
/// the mob publicly owned.
pub struct MobNativeDelivery {
    mob_id: MobId,
    identities: Arc<crate::identity_first::IdentityRuntime>,
    mob_tools: Arc<meerkat_mob_mcp::MobMcpState>,
    runtime: Arc<meerkat_runtime::MeerkatMachine>,
}

impl MobNativeDelivery {
    pub(crate) fn new(
        mob_id: MobId,
        identities: Arc<crate::identity_first::IdentityRuntime>,
        mob_tools: Arc<meerkat_mob_mcp::MobMcpState>,
        runtime: Arc<meerkat_runtime::MeerkatMachine>,
    ) -> Self {
        Self {
            mob_id,
            identities,
            mob_tools,
            runtime,
        }
    }

    fn service(&self) -> Result<Arc<meerkat::ContinuationOwnerService>, NativeUnavailable> {
        self.mob_tools.continuation_service().ok_or_else(|| {
            NativeUnavailable("the mob's continuation service is not bound".to_string())
        })
    }

    fn owner(&self, identity: &MemberIdentity) -> meerkat::ContinuationOwner {
        meerkat::ContinuationOwner::Member {
            mob_id: self.mob_id.to_string(),
            identity: crate::member_comms_id::roster_member_id_for_identity(identity.as_str())
                .to_string(),
        }
    }

    fn agent_identity(
        identity: &MemberIdentity,
    ) -> Result<crate::identity_first::AgentIdentity, NativeUnavailable> {
        crate::identity_first::AgentIdentity::parse(identity.as_str())
            .map_err(|error| NativeUnavailable(error.to_string()))
    }

    /// Whether this gateway's runtime refuses host continuations.
    pub fn governs_work_authority(&self) -> Result<bool, NativeUnavailable> {
        Ok(self.service()?.governs_work_authority())
    }

    /// Whether the identity is registered with this gateway, and if so
    /// whether this gateway's runtime serves it.
    pub async fn identity_service(
        &self,
        identity: &MemberIdentity,
    ) -> Result<IdentityService, NativeUnavailable> {
        let identity = Self::agent_identity(identity)?;
        match self.identities.not_served_here(&identity).await {
            Ok(None) => Ok(IdentityService::Local),
            Ok(Some(detail)) => Ok(IdentityService::Elsewhere(detail)),
            Err(crate::identity_first::IdentityRuntimeError::UnknownIdentity(_)) => {
                Ok(IdentityService::Unknown)
            }
            Err(error) => Err(NativeUnavailable(error.to_string())),
        }
    }

    /// Make the identity's current incarnation resolvable, or report that
    /// it is gone for good: no longer registered, or not served here.
    pub async fn prepare_identity(&self, identity: &MemberIdentity) -> Result<(), PrepareError> {
        match self
            .identity_service(identity)
            .await
            .map_err(|error| PrepareError::Unavailable(error.0))?
        {
            IdentityService::Unknown => {
                return Err(PrepareError::Gone(SubscriptionEnd::MemberLeft));
            }
            IdentityService::Elsewhere(detail) => {
                return Err(PrepareError::Gone(SubscriptionEnd::MemberUnusable {
                    detail,
                }));
            }
            IdentityService::Local => {}
        }
        let identity =
            Self::agent_identity(identity).map_err(|error| PrepareError::Unavailable(error.0))?;
        self.identities
            .materialize(&identity)
            .await
            .map(|_| ())
            .map_err(|error| PrepareError::Unavailable(error.to_string()))
    }

    /// Whether each named mob is hosted here: managed and not destroyed, or
    /// restored and held until its owner resolves.
    pub async fn hosted_mobs(
        &self,
        mobs: &BTreeSet<MobId>,
    ) -> Result<BTreeMap<MobId, MobPresence>, NativeUnavailable> {
        let managed = self
            .mob_tools
            .mob_list()
            .await
            .map_err(|error| NativeUnavailable(error.to_string()))?
            .into_iter()
            .filter(|(_, state)| *state != meerkat_mob::MobState::Destroyed)
            .map(|(mob_id, _)| mob_id.as_str().to_string())
            .collect::<BTreeSet<_>>();
        let held = self
            .mob_tools
            .held_child_mobs()
            .await
            .into_iter()
            .map(|(mob_id, _)| mob_id.as_str().to_string())
            .collect::<BTreeSet<_>>();
        Ok(mobs
            .iter()
            .map(|mob_id| {
                let hosted = managed.contains(mob_id.as_str()) || held.contains(mob_id.as_str());
                let presence = if hosted {
                    MobPresence::Hosted
                } else {
                    MobPresence::Gone
                };
                (mob_id.clone(), presence)
            })
            .collect())
    }

    /// Submit one continuation for the identity.
    pub async fn submit_delivery(
        &self,
        identity: &MemberIdentity,
        delivery: meerkat::ContinuationDelivery,
        committed_at_ms: u64,
    ) -> Result<NativeReceipt, NativeSubmitError> {
        let service = self
            .service()
            .map_err(|error| NativeSubmitError::Unavailable(error.0))?;
        service
            .submit(&self.owner(identity), delivery, committed_at_ms)
            .await
            .map(|receipt| native_receipt(&receipt))
            .map_err(|error| match error {
                meerkat::ContinuationSubmitError::Unavailable(detail) => {
                    NativeSubmitError::Unavailable(detail)
                }
                meerkat::ContinuationSubmitError::OwnerRetired => NativeSubmitError::OwnerRetired,
                meerkat::ContinuationSubmitError::NoAdmissibleWorkBinding => {
                    NativeSubmitError::Governed
                }
                meerkat::ContinuationSubmitError::Conflict { existing } => {
                    NativeSubmitError::Conflict(format!(
                        "the native key {} is bound to other content",
                        existing.key.as_str()
                    ))
                }
                meerkat::ContinuationSubmitError::Invalid(detail) => {
                    NativeSubmitError::Invalid(detail)
                }
            })
    }

    /// The continuation owner's status of a native key for the identity.
    pub async fn status_of(
        &self,
        identity: &MemberIdentity,
        key: &str,
    ) -> Result<NativeStatus, NativeUnavailable> {
        let key = meerkat::ContinuationKey::new(key)
            .map_err(|error| NativeUnavailable(error.to_string()))?;
        self.service()?
            .continuation_status(&self.owner(identity), &key)
            .await
            .map(native_status)
            .map_err(|error| NativeUnavailable(error.to_string()))
    }

    /// The runtime's own state of an admitted input.
    pub async fn input_state(
        &self,
        session_id: &str,
        input_id: &str,
    ) -> Result<Option<Value>, NativeUnavailable> {
        use meerkat_runtime::SessionServiceRuntimeExt as _;
        let session = meerkat_core::types::SessionId::parse(session_id)
            .map_err(|error| NativeUnavailable(error.to_string()))?;
        let input = uuid::Uuid::parse_str(input_id)
            .map(meerkat_core::lifecycle::InputId)
            .map_err(|error| NativeUnavailable(error.to_string()))?;
        let Some(state) = self
            .runtime
            .input_state(&session, &input)
            .await
            .map_err(|error| NativeUnavailable(error.to_string()))?
        else {
            return Ok(None);
        };
        let completion = match self
            .runtime
            .input_terminal_completion(&session, &input)
            .await
        {
            Ok(Some(outcome)) => serde_json::to_value(outcome)
                .ok()
                .and_then(|value| value.get("completion_type").cloned())
                .unwrap_or(Value::Null),
            Ok(None) => Value::Null,
            Err(error) => json!({ "unavailable": error.to_string() }),
        };
        Ok(Some(json!({
            "phase": serde_json::to_value(state.seed.phase).unwrap_or(Value::Null),
            "terminal_outcome": serde_json::to_value(state.seed.terminal_outcome)
                .unwrap_or(Value::Null),
            "completion": completion,
        })))
    }
}

/// Whether an identity of this gateway's mob is registered, and whether this
/// gateway's runtime serves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityService {
    /// Not registered with this gateway.
    Unknown,
    /// Served by this gateway's runtime: a local session-backed member.
    Local,
    /// Registered, but its session is not this runtime's to serve (placed
    /// on a host, or bound to an external runtime).
    Elsewhere(String),
}

/// Members of this gateway's mob are delivered to through its own runtime. A
/// member on a peer gateway has no path from this seam: it is refused at
/// subscription time.
#[async_trait]
impl NativeDelivery for MobNativeDelivery {
    fn governs_work_authority(&self) -> Result<bool, NativeUnavailable> {
        MobNativeDelivery::governs_work_authority(self)
    }

    fn route(&self, member: &Member) -> MemberRoute {
        if member.mob_id == self.mob_id {
            MemberRoute::Local
        } else {
            MemberRoute::Peer
        }
    }

    async fn mob_presence(
        &self,
        mobs: &BTreeSet<MobId>,
    ) -> Result<BTreeMap<MobId, MobPresence>, NativeUnavailable> {
        self.hosted_mobs(mobs).await
    }

    fn mob_set_changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.mob_tools.mob_set_changes())
    }

    async fn check_member(&self, member: &Member) -> Result<(), MemberProblem> {
        if self.route(member) == MemberRoute::Peer {
            let named = BTreeSet::from([member.mob_id.clone()]);
            let presence = self
                .hosted_mobs(&named)
                .await
                .map_err(|error| MemberProblem::Unavailable(error.0))?;
            if presence.get(&member.mob_id) == Some(&MobPresence::Hosted) {
                return Err(MemberProblem::Unsupported(format!(
                    "{} is another mob hosted on this gateway; deliveries reach members of \
                     this gateway's own mob and of peer gateways' mobs",
                    member.mob_id
                )));
            }
            return Err(MemberProblem::UnusablePeer(format!(
                "this gateway has no delivery path to members of {}",
                member.mob_id
            )));
        }
        match self.identity_service(&member.identity).await {
            Ok(IdentityService::Local) => Ok(()),
            Ok(IdentityService::Unknown) => Err(MemberProblem::Unknown),
            Ok(IdentityService::Elsewhere(detail)) => Err(MemberProblem::Unusable(detail)),
            Err(error) => Err(MemberProblem::Unavailable(error.0)),
        }
    }

    async fn prepare_member(&self, member: &Member) -> Result<(), PrepareError> {
        if self.route(member) == MemberRoute::Peer {
            return Err(PrepareError::Unavailable(
                "this gateway has no delivery path to peer members".to_string(),
            ));
        }
        self.prepare_identity(&member.identity).await
    }

    async fn submit(
        &self,
        batch: &Batch,
        committed_at_ms: u64,
    ) -> Result<NativeReceipt, NativeSubmitError> {
        if self.route(&batch.member) == MemberRoute::Peer {
            return Err(NativeSubmitError::Unavailable(
                "this gateway has no delivery path to peer members".to_string(),
            ));
        }
        let delivery = continuation_for(batch).map_err(NativeSubmitError::Invalid)?;
        let receipt = self
            .submit_delivery(&batch.member.identity, delivery, committed_at_ms)
            .await?;
        super::faults::after_native_commit();
        Ok(receipt)
    }

    async fn status(&self, batch: &Batch) -> Result<NativeStatus, NativeUnavailable> {
        if self.route(&batch.member) == MemberRoute::Peer {
            return Err(NativeUnavailable(
                "this gateway has no delivery path to peer members".to_string(),
            ));
        }
        self.status_of(&batch.member.identity, batch.id.as_str())
            .await
    }

    async fn input(
        &self,
        _batch: &Batch,
        session_id: &str,
        input_id: &str,
    ) -> Result<Option<Value>, NativeUnavailable> {
        self.input_state(session_id, input_id).await
    }
}

/// The owner's seam: this gateway's runtime for members of its own mob, and
/// keyed inject over the signed control channel for members of mobs on peer
/// gateways.
pub struct AnnouncementNativeDelivery {
    local: Arc<MobNativeDelivery>,
    remote: Option<Arc<RemoteMemberDelivery>>,
}

impl AnnouncementNativeDelivery {
    pub(crate) fn new(
        local: Arc<MobNativeDelivery>,
        remote: Option<Arc<RemoteMemberDelivery>>,
    ) -> Self {
        Self { local, remote }
    }

    fn remote(&self) -> Result<&RemoteMemberDelivery, String> {
        self.remote
            .as_deref()
            .ok_or_else(|| "this gateway has no contact directory for peer members".to_string())
    }
}

#[async_trait]
impl NativeDelivery for AnnouncementNativeDelivery {
    fn governs_work_authority(&self) -> Result<bool, NativeUnavailable> {
        self.local.governs_work_authority()
    }

    fn route(&self, member: &Member) -> MemberRoute {
        NativeDelivery::route(self.local.as_ref(), member)
    }

    async fn mob_presence(
        &self,
        mobs: &BTreeSet<MobId>,
    ) -> Result<BTreeMap<MobId, MobPresence>, NativeUnavailable> {
        self.local.hosted_mobs(mobs).await
    }

    fn mob_set_changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        NativeDelivery::mob_set_changes(self.local.as_ref())
    }

    async fn check_member(&self, member: &Member) -> Result<(), MemberProblem> {
        match self.route(member) {
            MemberRoute::Local => NativeDelivery::check_member(self.local.as_ref(), member).await,
            MemberRoute::Peer => {
                // A mob hosted here is never reached as a peer: the local
                // seam refuses its members typed.
                let named = BTreeSet::from([member.mob_id.clone()]);
                let presence = self
                    .local
                    .hosted_mobs(&named)
                    .await
                    .map_err(|error| MemberProblem::Unavailable(error.0))?;
                if presence.get(&member.mob_id) == Some(&MobPresence::Hosted) {
                    return NativeDelivery::check_member(self.local.as_ref(), member).await;
                }
                self.remote()
                    .map_err(MemberProblem::UnusablePeer)?
                    .check_member(member)
            }
        }
    }

    async fn prepare_member(&self, member: &Member) -> Result<(), PrepareError> {
        match self.route(member) {
            MemberRoute::Local => self.local.prepare_identity(&member.identity).await,
            // The peer prepares its own member when the batch reaches it.
            MemberRoute::Peer => Ok(()),
        }
    }

    async fn submit(
        &self,
        batch: &Batch,
        committed_at_ms: u64,
    ) -> Result<NativeReceipt, NativeSubmitError> {
        match self.route(&batch.member) {
            MemberRoute::Local => {
                NativeDelivery::submit(self.local.as_ref(), batch, committed_at_ms).await
            }
            MemberRoute::Peer => {
                self.remote()
                    .map_err(NativeSubmitError::Unavailable)?
                    .submit(batch)
                    .await
            }
        }
    }

    async fn status(&self, batch: &Batch) -> Result<NativeStatus, NativeUnavailable> {
        match self.route(&batch.member) {
            MemberRoute::Local => NativeDelivery::status(self.local.as_ref(), batch).await,
            MemberRoute::Peer => {
                self.remote()
                    .map_err(NativeUnavailable)?
                    .status(batch)
                    .await
            }
        }
    }

    async fn input(
        &self,
        batch: &Batch,
        session_id: &str,
        input_id: &str,
    ) -> Result<Option<Value>, NativeUnavailable> {
        match self.route(&batch.member) {
            MemberRoute::Local => self.local.input_state(session_id, input_id).await,
            MemberRoute::Peer => self.remote().map_err(NativeUnavailable)?.input(batch).await,
        }
    }
}
