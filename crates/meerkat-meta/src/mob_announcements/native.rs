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
use super::types::{Batch, Member, MemberIdentity, MobId, MobIncarnation, SubscriptionEnd};
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

/// Who created a hosted mob, as Meerkat's durable records say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MobOrigin {
    /// A member of `parent`, roster id `creator`, created it: the mob's
    /// owner binding names a session whose own persisted member binding
    /// names that member, and the parent's member-creation journal records
    /// the same session for the same binding.
    Member { parent: MobId, creator: String },
    /// No member created it, or the records do not prove one: a final
    /// fact for this incarnation. The detail says which.
    Unproven(String),
    /// The parent the owner session names is not hosted here. Final only
    /// once every mob this gateway hosts has been restored.
    ParentNotHosted(MobId),
}

/// One hosted mob's incarnation and origin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MobOriginFacts {
    pub incarnation: MobIncarnation,
    pub origin: MobOrigin,
}

/// A member binding as Meerkat persists it: the mob, the member's role and
/// its roster id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BindingRecord {
    pub(crate) mob_id: String,
    pub(crate) role: String,
    pub(crate) member: String,
}

/// The parent a member binding names, as this gateway hosts it now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParentRecord {
    /// Running: its member-creation journal entry for the owner session,
    /// as the session it records and the binding it records.
    Running {
        journal: Option<(String, BindingRecord)>,
    },
    /// Restored and held until its owner resolves: it may run again.
    Held,
    /// Neither running nor held.
    NotHosted,
}

/// What the owner session's persisted metadata says about its member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OwnerMetadata {
    /// No persisted metadata: an archived session reads so.
    Absent,
    /// Persisted, and bound to no member.
    Unbound,
    /// Persisted, and bound to this member.
    Bound(BindingRecord),
}

/// What Meerkat's durable records say about who created a hosted mob, each
/// read only when the one before it names something.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OriginRecords {
    /// The mob's set-once owner binding: the session of the member that
    /// created it.
    pub(crate) owner_session: Option<String>,
    /// That session's persisted metadata, read when there is one.
    pub(crate) owner_metadata: Option<OwnerMetadata>,
    /// Where the parent that binding names is.
    pub(crate) parent: Option<ParentRecord>,
}

/// Who created a mob, from its records, or `Err` while that cannot be told
/// yet (its parent is held until its owner resolves). A member created it
/// only when the owner session's binding names it and the parent's journal
/// records that same session for the same mob, role and member.
pub(crate) fn origin_from(records: &OriginRecords) -> Result<MobOrigin, String> {
    let Some(owner) = &records.owner_session else {
        return Ok(MobOrigin::Unproven("no member created it".to_string()));
    };
    let binding = match &records.owner_metadata {
        // An archived owner session reads as absent: the window a mob first
        // seen after its creator respawned falls into. It fails closed.
        None | Some(OwnerMetadata::Absent) => {
            return Ok(MobOrigin::Unproven(
                "its owner session has no persisted metadata".to_string(),
            ));
        }
        Some(OwnerMetadata::Unbound) => {
            return Ok(MobOrigin::Unproven(
                "its owner session is bound to no member".to_string(),
            ));
        }
        Some(OwnerMetadata::Bound(binding)) => binding,
    };
    let parent = MobId::new(binding.mob_id.clone()).map_err(|error| error.to_string())?;
    match &records.parent {
        Some(ParentRecord::Running { journal }) => {
            let agrees = journal
                .as_ref()
                .is_some_and(|(session, recorded)| session == owner && recorded == binding);
            if agrees {
                Ok(MobOrigin::Member {
                    parent,
                    creator: binding.member.clone(),
                })
            } else {
                Ok(MobOrigin::Unproven(
                    "the parent's member-creation journal does not record its owner session \
                     for that member"
                        .to_string(),
                ))
            }
        }
        Some(ParentRecord::Held) => Err(format!(
            "its parent {parent} is held until its owner resolves"
        )),
        Some(ParentRecord::NotHosted) | None => Ok(MobOrigin::ParentNotHosted(parent)),
    }
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

    /// The mobs hosted here other than this gateway's own: managed and not
    /// destroyed, or restored and held until their owner resolves.
    async fn hosted_mob_ids(&self) -> Result<BTreeSet<MobId>, NativeUnavailable> {
        Ok(BTreeSet::new())
    }

    /// Whether every mob this gateway hosts across a restart has been
    /// restored: from then on, a mob it does not host is gone for good.
    fn mobs_restored(&self) -> bool {
        true
    }

    /// The incarnation of the hosted mob `mob_id` and who created it, from
    /// Meerkat's durable records. `Ok(None)` when it is not running here
    /// now: gone, or held until its owner resolves.
    async fn mob_origin(
        &self,
        _mob_id: &MobId,
    ) -> Result<Option<MobOriginFacts>, NativeUnavailable> {
        Ok(None)
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
    sessions: Arc<dyn meerkat_mob::MobSessionService>,
}

impl MobNativeDelivery {
    pub(crate) fn new(
        mob_id: MobId,
        identities: Arc<crate::identity_first::IdentityRuntime>,
        mob_tools: Arc<meerkat_mob_mcp::MobMcpState>,
        runtime: Arc<meerkat_runtime::MeerkatMachine>,
        sessions: Arc<dyn meerkat_mob::MobSessionService>,
    ) -> Self {
        Self {
            mob_id,
            identities,
            mob_tools,
            runtime,
            sessions,
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

    /// The mobs hosted here other than this gateway's own.
    ///
    /// Read through the agent mob tool state's own listings, which finish
    /// its restore of persisted child mobs before they answer, so a mob
    /// missing here is not one still being restored.
    pub async fn hosted_mob_ids(&self) -> Result<BTreeSet<MobId>, NativeUnavailable> {
        let managed = self
            .mob_tools
            .mob_list()
            .await
            .map_err(|error| NativeUnavailable(error.to_string()))?
            .into_iter()
            .filter(|(_, state)| *state != meerkat_mob::MobState::Destroyed)
            .map(|(mob_id, _)| mob_id.as_str().to_string());
        let held = self
            .mob_tools
            .held_child_mobs()
            .await
            .into_iter()
            .map(|(mob_id, _)| mob_id.as_str().to_string());
        managed
            .chain(held)
            .filter(|mob_id| mob_id != self.mob_id.as_str())
            .map(|mob_id| MobId::new(mob_id).map_err(|error| NativeUnavailable(error.to_string())))
            .collect()
    }

    /// The incarnation of a running hosted mob and who created it, from
    /// three durable records: the mob's set-once owner binding, that
    /// session's persisted member binding, and the parent mob's
    /// member-creation journal for that session. The live roster is not
    /// read: the relation must hold across a repoint of the creator.
    pub async fn mob_origin(
        &self,
        mob_id: &MobId,
    ) -> Result<Option<MobOriginFacts>, NativeUnavailable> {
        let unavailable = |error: &dyn std::fmt::Display| NativeUnavailable(error.to_string());
        // Held mobs are read before running ones: activation moves a held
        // mob to the running ones, never back, so a parent in neither is
        // not one about to run.
        let held = self
            .mob_tools
            .held_child_mobs()
            .await
            .into_iter()
            .map(|(id, _)| id.as_str().to_string())
            .collect::<BTreeSet<_>>();
        let handles = self
            .mob_tools
            .mob_handles_snapshot()
            .await
            .map_err(|error| unavailable(&error))?;
        let Some(handle) = handles
            .iter()
            .find(|(id, _)| id.as_str() == mob_id.as_str())
            .map(|(_, handle)| handle.clone())
        else {
            return Ok(None);
        };
        let first_event_at_ms = handle
            .events()
            .poll(0, 1)
            .await
            .map_err(|error| unavailable(&error))?
            .first()
            .map(|event| u64::try_from(event.timestamp.timestamp_millis()).unwrap_or(0))
            .ok_or_else(|| NativeUnavailable(format!("mob {mob_id} has no events yet")))?;
        let owner = handle
            .owner_bridge_session_lifecycle_authority()
            .map(|authority| authority.bridge_session_id);
        let incarnation = MobIncarnation {
            owner_session: owner.as_ref().map(ToString::to_string),
            first_event_at_ms,
        };
        let mut records = OriginRecords {
            owner_session: owner.as_ref().map(ToString::to_string),
            ..OriginRecords::default()
        };
        if let Some(owner) = &owner {
            let metadata = match self
                .sessions
                .load_persisted_session_metadata(owner)
                .await
                .map_err(|error| unavailable(&error))?
                .filter(|view| view.session_id == *owner)
            {
                None => OwnerMetadata::Absent,
                Some(view) => match view
                    .session_metadata
                    .and_then(|metadata| metadata.mob_member_binding)
                {
                    None => OwnerMetadata::Unbound,
                    Some(binding) => OwnerMetadata::Bound(BindingRecord {
                        mob_id: binding.mob_id,
                        role: binding.role,
                        member: binding.member,
                    }),
                },
            };
            if let OwnerMetadata::Bound(binding) = &metadata {
                let parent = handles
                    .iter()
                    .find(|(id, _)| id.as_str() == binding.mob_id)
                    .map(|(_, handle)| handle.clone());
                records.parent = Some(match parent {
                    Some(parent) => ParentRecord::Running {
                        journal: parent
                            .read_handle()
                            .member_creation_for_session(owner)
                            .await
                            .map_err(|error| unavailable(&error))?
                            .map(|snapshot| {
                                (
                                    snapshot.session_id.to_string(),
                                    BindingRecord {
                                        mob_id: snapshot.member_binding.mob_id,
                                        role: snapshot.member_binding.role,
                                        member: snapshot.member_binding.member,
                                    },
                                )
                            }),
                    },
                    None if held.contains(&binding.mob_id) => ParentRecord::Held,
                    None => ParentRecord::NotHosted,
                });
            }
            records.owner_metadata = Some(metadata);
        }
        let origin = origin_from(&records).map_err(NativeUnavailable)?;
        Ok(Some(MobOriginFacts {
            incarnation,
            origin,
        }))
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

    async fn hosted_mob_ids(&self) -> Result<BTreeSet<MobId>, NativeUnavailable> {
        MobNativeDelivery::hosted_mob_ids(self).await
    }

    async fn mob_origin(
        &self,
        mob_id: &MobId,
    ) -> Result<Option<MobOriginFacts>, NativeUnavailable> {
        MobNativeDelivery::mob_origin(self, mob_id).await
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

    async fn hosted_mob_ids(&self) -> Result<BTreeSet<MobId>, NativeUnavailable> {
        self.local.hosted_mob_ids().await
    }

    async fn mob_origin(
        &self,
        mob_id: &MobId,
    ) -> Result<Option<MobOriginFacts>, NativeUnavailable> {
        self.local.mob_origin(mob_id).await
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
