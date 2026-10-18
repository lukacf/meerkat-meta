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
pub use super::types::MemberRoute;
use super::types::{
    Batch, Member, MemberIdentity, MemberRouteRecord, MobId, MobIncarnation, SubscriptionEnd,
};
pub use crate::runtime::keyed_inject::{AdmittedInput, NativeReceipt, NativeStatus};

/// Events read per page when a member's mob is checked for its end.
const MEMBER_MOB_EVENT_PAGE: usize = 256;

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
    /// A member this gateway's runtime does not serve (placed on a host, or
    /// bound to an external runtime), so a delivery could never be admitted.
    Unusable(String),
    /// The peer gateway cannot be called authenticated.
    UnusablePeer(String),
    /// A read the check needs could not be made now.
    Unavailable(String),
    /// The member's mob id names a mob hosted here and a peer in the
    /// contact directory alike.
    Ambiguous(String),
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

/// A mob hosted here other than this gateway's own.
enum ChildMob {
    Running(Box<meerkat_mob::MobHandle>),
    /// Restored and held until its owner resolves: it may run again.
    Held,
    /// Neither running nor held: destroyed, or never created here.
    Gone,
}

/// That a member's own mob, a mob a member created, ended after a cursor of
/// its event log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberMobEnd {
    /// Subscriptions made before this cursor end. `u64::MAX` when the mob
    /// is gone or another incarnation: every subscription made in it ends.
    pub cursor: u64,
    pub why: String,
}

impl MemberMobEnd {
    fn gone(why: &str) -> Self {
        Self {
            cursor: u64::MAX,
            why: why.to_string(),
        }
    }
}

/// What a member's own mob, a mob a member created, did after a cursor of its
/// event log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberMobScan {
    /// It ended.
    Ended(MemberMobEnd),
    /// It runs, and neither completed nor reset after the cursor through
    /// `through`.
    Running { through: u64 },
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
    /// the same session for the same binding. `parent_incarnation` is the
    /// parent's incarnation then; `None` when the parent is this gateway's
    /// own mob.
    Member {
        parent: MobId,
        creator: String,
        parent_incarnation: Option<MobIncarnation>,
    },
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
        /// The parent's incarnation; `None` for this gateway's own mob.
        incarnation: Option<MobIncarnation>,
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
    let parent = match MobId::new(binding.mob_id.clone()) {
        Ok(parent) => parent,
        Err(error) => {
            return Ok(MobOrigin::Unproven(format!(
                "its owner session names no valid mob: {error}"
            )));
        }
    };
    match &records.parent {
        Some(ParentRecord::Running {
            journal,
            incarnation,
        }) => {
            let agrees = journal
                .as_ref()
                .is_some_and(|(session, recorded)| session == owner && recorded == binding);
            if agrees {
                Ok(MobOrigin::Member {
                    parent,
                    creator: binding.member.clone(),
                    parent_incarnation: incarnation.clone(),
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

    /// How the member would be reached if it were subscribed now: a member
    /// of this gateway's mob, of a mob a member created here (with that
    /// mob's incarnation), or of a mob on a peer gateway. The route a member
    /// was first subscribed with is kept by the store; this answers only for
    /// a member without one.
    async fn route_for(&self, member: &Member) -> Result<MemberRouteRecord, MemberProblem>;

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

    /// This gateway's own mob: the root of the lineage, with no record.
    fn own_mob(&self) -> Option<MobId> {
        None
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
    async fn check_member(&self, member: &Member, route: MemberRoute) -> Result<(), MemberProblem>;

    /// Make the member's current incarnation resolvable before a
    /// submission binds to it, or report the final fact that it never will
    /// be. Submits nothing. A peer prepares its own member when a batch
    /// reaches it.
    async fn prepare_member(&self, member: &Member, route: MemberRoute)
    -> Result<(), PrepareError>;

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

    /// The current cursor of the event log of a mob a member created: a
    /// subscription made now is made at it.
    async fn member_mob_cursor(&self, mob_id: &MobId) -> Result<u64, NativeUnavailable> {
        Err(NativeUnavailable(format!(
            "{mob_id} keeps no event log here"
        )))
    }

    /// Whether the mob a member was subscribed in (`incarnation` of
    /// `mob_id`) is gone for good: destroyed, or created again under the
    /// same id. Read once every mob hosted across a restart is restored.
    async fn member_mob_gone(
        &self,
        _mob_id: &MobId,
        _incarnation: Option<&MobIncarnation>,
    ) -> Result<Option<MemberMobEnd>, NativeUnavailable> {
        Ok(None)
    }

    /// What the mob a member was subscribed in did after `after`: gone (as
    /// [`Self::member_mob_gone`] reads it), or completed or reset per its
    /// own durable event log, or neither through the cursor read.
    async fn member_mob_end(
        &self,
        mob_id: &MobId,
        incarnation: Option<&MobIncarnation>,
        after: u64,
    ) -> Result<MemberMobScan, NativeUnavailable> {
        Ok(match self.member_mob_gone(mob_id, incarnation).await? {
            Some(end) => MemberMobScan::Ended(end),
            None => MemberMobScan::Running { through: after },
        })
    }
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

/// Why this gateway's runtime names no continuation owner for a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OwnerRefusal {
    /// The member is on a peer gateway: its batches go through that peer.
    NoPeerPath,
    /// The batch's route is local, but its member names another mob than
    /// this gateway's. The record contradicts itself, and no member of this
    /// gateway's mob is delivered to in that member's place.
    LocalRouteNamesAnotherMob { member: Member, own: MobId },
}

impl std::fmt::Display for OwnerRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoPeerPath => f.write_str("this gateway has no delivery path to peer members"),
            Self::LocalRouteNamesAnotherMob { member, own } => write!(
                f,
                "the batch for {member} is routed to this gateway's mob {own}, \
                 which is not the member's"
            ),
        }
    }
}

/// The continuation owner of `member` by its kept route, on the gateway
/// whose own mob is `own`. A member of `own` is named by its roster id, a
/// member of a mob a member created by its plain roster id, and a local
/// route always names the member's own mob.
pub(crate) fn continuation_owner(
    own: &MobId,
    member: &Member,
    route: MemberRoute,
) -> Result<meerkat::ContinuationOwner, OwnerRefusal> {
    match route {
        MemberRoute::Local if member.mob_id != *own => {
            Err(OwnerRefusal::LocalRouteNamesAnotherMob {
                member: member.clone(),
                own: own.clone(),
            })
        }
        MemberRoute::Local => Ok(meerkat::ContinuationOwner::Member {
            mob_id: member.mob_id.to_string(),
            identity: crate::member_comms_id::roster_member_id_for_identity(
                member.identity.as_str(),
            )
            .to_string(),
        }),
        MemberRoute::Child => Ok(meerkat::ContinuationOwner::Member {
            mob_id: member.mob_id.to_string(),
            identity: member.identity.to_string(),
        }),
        MemberRoute::Peer => Err(OwnerRefusal::NoPeerPath),
    }
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

/// The agent mob tool state's listings of the mobs hosted here. A seam:
/// which mobs a reader finds depends on the order it reads them in.
#[async_trait]
pub(crate) trait HostedListings: Send + Sync {
    /// A running mob's handle.
    type Handle: Send;

    /// Every managed mob with its state. Fails until the restore of
    /// persisted mobs is done.
    async fn mob_list(
        &self,
    ) -> Result<Vec<(meerkat_mob::MobId, meerkat_mob::MobState)>, meerkat_mob::MobError>;

    /// The restored mobs held until their owner resolves. Empty when the
    /// restore fails.
    async fn held_child_mobs(&self) -> Vec<(meerkat_mob::MobId, meerkat_core::types::SessionId)>;

    /// The handles of the managed mobs not destroyed. Fails until the
    /// restore is done.
    async fn mob_handles_snapshot(
        &self,
    ) -> Result<Vec<(meerkat_mob::MobId, Self::Handle)>, meerkat_mob::MobError>;
}

#[async_trait]
impl HostedListings for meerkat_mob_mcp::MobMcpState {
    type Handle = meerkat_mob::MobHandle;

    async fn mob_list(
        &self,
    ) -> Result<Vec<(meerkat_mob::MobId, meerkat_mob::MobState)>, meerkat_mob::MobError> {
        meerkat_mob_mcp::MobMcpState::mob_list(self).await
    }

    async fn held_child_mobs(&self) -> Vec<(meerkat_mob::MobId, meerkat_core::types::SessionId)> {
        meerkat_mob_mcp::MobMcpState::held_child_mobs(self).await
    }

    async fn mob_handles_snapshot(
        &self,
    ) -> Result<Vec<(meerkat_mob::MobId, meerkat_mob::MobHandle)>, meerkat_mob::MobError> {
        meerkat_mob_mcp::MobMcpState::mob_handles_snapshot(self).await
    }
}

/// Every mob hosted here, this gateway's own included: managed and not
/// destroyed, or restored and held until its owner resolves.
///
/// The managed listing is read first only to finish the restore of
/// persisted mobs: it fails rather than answer before the restore, where
/// the held listing answers empty. Held mobs are then read before managed
/// ones, because activation adds a held mob to the managed ones before it
/// drops the hold: a mob that activates between the two reads is in one of
/// them.
pub(crate) async fn hosted_set<L: HostedListings>(
    listings: &L,
) -> Result<BTreeSet<String>, NativeUnavailable> {
    managed_listing(listings).await?;
    let held = held_listing(listings).await;
    let managed = managed_listing(listings)
        .await?
        .into_iter()
        .filter(|(_, state)| *state != meerkat_mob::MobState::Destroyed)
        .map(|(mob_id, _)| mob_id.as_str().to_string());
    Ok(held.into_iter().chain(managed).collect())
}

/// The held mobs and the running mobs' handles, read in the order
/// [`hosted_set`] reads them in: the restore finished, then held, then
/// running. A mob in neither is not one still being restored, nor one
/// about to run.
pub(crate) async fn hosted_handles<L: HostedListings>(
    listings: &L,
) -> Result<(BTreeSet<String>, Vec<(meerkat_mob::MobId, L::Handle)>), NativeUnavailable> {
    managed_listing(listings).await?;
    let held = held_listing(listings).await;
    let running = listings
        .mob_handles_snapshot()
        .await
        .map_err(|error| NativeUnavailable(error.to_string()))?;
    Ok((held, running))
}

/// The mobs hosted here other than `own`. An id this store cannot hold is
/// skipped, so the others stay readable, and warned about once: `warned`
/// keeps the ids already reported.
pub(crate) async fn hosted_mob_ids_in<L: HostedListings>(
    listings: &L,
    own: &MobId,
    warned: &std::sync::Mutex<BTreeSet<String>>,
) -> Result<BTreeSet<MobId>, NativeUnavailable> {
    Ok(hosted_set(listings)
        .await?
        .into_iter()
        .filter(|mob_id| mob_id != own.as_str())
        .filter_map(|mob_id| match MobId::new(mob_id.clone()) {
            Ok(mob_id) => Some(mob_id),
            Err(error) => {
                let first = warned
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(mob_id.clone());
                if first {
                    tracing::warn!(mob = %mob_id, %error, "hosted mob id not usable here");
                }
                None
            }
        })
        .collect())
}

/// The managed mobs, once the restore of persisted mobs is finished.
async fn managed_listing<L: HostedListings>(
    listings: &L,
) -> Result<Vec<(meerkat_mob::MobId, meerkat_mob::MobState)>, NativeUnavailable> {
    listings
        .mob_list()
        .await
        .map_err(|error| NativeUnavailable(error.to_string()))
}

/// The ids of the restored mobs held until their owner resolves.
async fn held_listing<L: HostedListings>(listings: &L) -> BTreeSet<String> {
    listings
        .held_child_mobs()
        .await
        .into_iter()
        .map(|(mob_id, _)| mob_id.as_str().to_string())
        .collect()
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
    /// Hosted mob ids this store cannot hold, warned about once each.
    unusable_ids: std::sync::Mutex<BTreeSet<String>>,
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
            unusable_ids: std::sync::Mutex::new(BTreeSet::new()),
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
        let hosted = hosted_set(&*self.mob_tools).await?;
        Ok(mobs
            .iter()
            .map(|mob_id| {
                let presence = if hosted.contains(mob_id.as_str()) {
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
        hosted_mob_ids_in(&*self.mob_tools, &self.mob_id, &self.unusable_ids).await
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
        // A parent in neither listing is not one still being restored, nor
        // one about to run.
        let (held, handles) = hosted_handles(&*self.mob_tools).await?;
        let Some(handle) = handles
            .iter()
            .find(|(id, _)| id.as_str() == mob_id.as_str())
            .map(|(_, handle)| handle.clone())
        else {
            return Ok(None);
        };
        let incarnation = Self::incarnation_of(&handle, mob_id).await?;
        let owner = handle
            .owner_bridge_session_lifecycle_authority()
            .map(|authority| authority.bridge_session_id);
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
                        // The root has no incarnation on its edges, and an id
                        // this store cannot hold makes a record with no
                        // parent (origin_from), so neither reads one.
                        incarnation: match MobId::new(binding.mob_id.clone()) {
                            Ok(parent_id) if parent_id != self.mob_id => {
                                Some(Self::incarnation_of(&parent, &parent_id).await?)
                            }
                            _ => None,
                        },
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

    /// One running mob's incarnation: its owner binding and the time of the
    /// first event in its own log.
    async fn incarnation_of(
        handle: &meerkat_mob::MobHandle,
        mob_id: &MobId,
    ) -> Result<MobIncarnation, NativeUnavailable> {
        let first_event_at_ms = handle
            .events()
            .poll(0, 1)
            .await
            .map_err(|error| NativeUnavailable(error.to_string()))?
            .first()
            .map(|event| u64::try_from(event.timestamp.timestamp_millis()).unwrap_or(0))
            .ok_or_else(|| NativeUnavailable(format!("mob {mob_id} has no events yet")))?;
        Ok(MobIncarnation {
            owner_session: handle
                .owner_bridge_session_lifecycle_authority()
                .map(|authority| authority.bridge_session_id.to_string()),
            first_event_at_ms,
        })
    }

    /// A mob hosted here other than this gateway's own: running, held until
    /// its owner resolves, or gone.
    ///
    /// Read through [`hosted_handles`]: a mob that is neither held nor
    /// running is not one still being restored, nor one about to run.
    async fn child_mob(&self, mob_id: &MobId) -> Result<ChildMob, NativeUnavailable> {
        let (held, running) = hosted_handles(&*self.mob_tools).await?;
        let handle = running
            .into_iter()
            .find(|(id, _)| *mob_id != self.mob_id && id.as_str() == mob_id.as_str())
            .map(|(_, handle)| handle);
        Ok(match handle {
            Some(handle) => ChildMob::Running(Box::new(handle)),
            None if held.contains(mob_id.as_str()) => ChildMob::Held,
            None => ChildMob::Gone,
        })
    }

    /// The running handle of a mob hosted here other than this gateway's
    /// own; `None` when it is gone; unavailable while it is held.
    async fn running_child(
        &self,
        mob_id: &MobId,
    ) -> Result<Option<meerkat_mob::MobHandle>, NativeUnavailable> {
        match self.child_mob(mob_id).await? {
            ChildMob::Running(handle) => Ok(Some(*handle)),
            ChildMob::Held => Err(NativeUnavailable(format!(
                "{mob_id} is held until its owner resolves"
            ))),
            ChildMob::Gone => Ok(None),
        }
    }

    /// The incarnation of a running mob hosted here other than this
    /// gateway's own, or `None` when it is gone. Unavailable while the mob
    /// is held until its owner resolves.
    pub async fn child_incarnation(
        &self,
        mob_id: &MobId,
    ) -> Result<Option<MobIncarnation>, NativeUnavailable> {
        match self.running_child(mob_id).await? {
            Some(handle) => Ok(Some(Self::incarnation_of(&handle, mob_id).await?)),
            None => Ok(None),
        }
    }

    /// What Meerkat records for a call from `session` that is not on this
    /// gateway's own roster: the member binding persisted with the session,
    /// and that member's roster entry in its mob now. Read only from durable
    /// session metadata and the mob's own roster.
    pub(crate) async fn child_caller_records(
        &self,
        session: &meerkat_core::types::SessionId,
    ) -> Result<super::tools::ChildCallerRecords, String> {
        let binding = self
            .sessions
            .load_persisted_session_metadata(session)
            .await
            .map_err(|error| format!("the calling session's records cannot be read: {error}"))?
            .filter(|view| view.session_id == *session)
            .and_then(|view| view.session_metadata)
            .and_then(|metadata| metadata.mob_member_binding);
        let Some(binding) = binding else {
            return Ok(super::tools::ChildCallerRecords::default());
        };
        let entry = match MobId::new(binding.mob_id.clone()) {
            Ok(mob_id) if mob_id != self.mob_id => {
                match self.running_child(&mob_id).await.map_err(|error| error.0)? {
                    Some(handle) => handle
                        .get_member(&meerkat_mob::AgentIdentity::from(binding.member.as_str()))
                        .await
                        .map_err(|error| error.to_string())?
                        .map(|entry| {
                            (
                                entry.bridge_session_id().map(ToString::to_string),
                                entry.role.as_str().to_string(),
                            )
                        }),
                    None => None,
                }
            }
            _ => None,
        };
        Ok(super::tools::ChildCallerRecords {
            binding: Some((binding.mob_id, binding.role, binding.member)),
            entry,
        })
    }

    /// A member of a mob a member created can be subscribed while that mob
    /// runs and has not completed, and the member is on its roster now. Its
    /// absence later ends nothing: a respawn takes it off for a while.
    async fn check_child_member(&self, member: &Member) -> Result<(), MemberProblem> {
        let Some(handle) = self
            .running_child(&member.mob_id)
            .await
            .map_err(|error| MemberProblem::Unavailable(error.0))?
        else {
            return Err(MemberProblem::Unusable(format!(
                "{} does not run here",
                member.mob_id
            )));
        };
        if matches!(
            handle.status_observation_snapshot(),
            meerkat_mob::MobState::Completed | meerkat_mob::MobState::Destroyed
        ) {
            return Err(MemberProblem::Unusable(format!(
                "{} has completed",
                member.mob_id
            )));
        }
        match handle
            .get_member(&meerkat_mob::AgentIdentity::from(member.identity.as_str()))
            .await
        {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(MemberProblem::Unknown),
            Err(error) => Err(MemberProblem::Unavailable(error.to_string())),
        }
    }

    /// A member of a mob a member created is ready while that mob runs here;
    /// once it is gone for good the member's subscriptions end. A held mob
    /// may run again: its members wait.
    async fn prepare_child_member(&self, member: &Member) -> Result<(), PrepareError> {
        match self.running_child(&member.mob_id).await {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(PrepareError::Gone(SubscriptionEnd::MemberMobEnded {
                why: "destroyed".to_string(),
            })),
            Err(error) => Err(PrepareError::Unavailable(error.0)),
        }
    }

    /// The continuation owner of a member by its kept route.
    fn owner_for(
        &self,
        member: &Member,
        route: MemberRoute,
    ) -> Result<meerkat::ContinuationOwner, OwnerRefusal> {
        continuation_owner(&self.mob_id, member, route)
    }

    /// The current cursor of a running child mob's event log.
    pub async fn member_mob_cursor(&self, mob_id: &MobId) -> Result<u64, NativeUnavailable> {
        let handle = self
            .running_child(mob_id)
            .await?
            .ok_or_else(|| NativeUnavailable(format!("{mob_id} does not run here")))?;
        handle
            .events()
            .latest_cursor()
            .await
            .map_err(|error| NativeUnavailable(error.to_string()))
    }

    /// The running handle of the mob a member was subscribed in, or the
    /// final fact that it is gone: destroyed (neither running nor held once
    /// every mob is restored), or created again (another incarnation under
    /// the same id). Unavailable while it is held until its owner resolves.
    async fn member_mob_handle(
        &self,
        mob_id: &MobId,
        incarnation: Option<&MobIncarnation>,
    ) -> Result<Result<meerkat_mob::MobHandle, MemberMobEnd>, NativeUnavailable> {
        let Some(handle) = self.running_child(mob_id).await? else {
            return Ok(Err(MemberMobEnd::gone("destroyed")));
        };
        if let Some(subscribed_in) = incarnation
            && Self::incarnation_of(&handle, mob_id).await? != *subscribed_in
        {
            return Ok(Err(MemberMobEnd::gone("created again")));
        }
        Ok(Ok(handle))
    }

    /// Whether the mob a member was subscribed in is gone for good.
    pub async fn member_mob_gone(
        &self,
        mob_id: &MobId,
        incarnation: Option<&MobIncarnation>,
    ) -> Result<Option<MemberMobEnd>, NativeUnavailable> {
        Ok(self.member_mob_handle(mob_id, incarnation).await?.err())
    }

    /// What the mob a member was subscribed in did after `after`, from its
    /// own durable records: gone, or the first completion or reset in its
    /// event log after `after`, or neither through the last event read.
    pub async fn member_mob_end(
        &self,
        mob_id: &MobId,
        incarnation: Option<&MobIncarnation>,
        after: u64,
    ) -> Result<MemberMobScan, NativeUnavailable> {
        let handle = match self.member_mob_handle(mob_id, incarnation).await? {
            Ok(handle) => handle,
            Err(end) => return Ok(MemberMobScan::Ended(end)),
        };
        let view = handle.events();
        let mut through = after;
        loop {
            let page = view
                .poll(through, MEMBER_MOB_EVENT_PAGE)
                .await
                .map_err(|error| NativeUnavailable(error.to_string()))?;
            for event in &page {
                let why = match event.kind {
                    meerkat_mob::event::MobEventKind::MobCompleted => "completed",
                    meerkat_mob::event::MobEventKind::MobReset => "reset",
                    _ => continue,
                };
                return Ok(MemberMobScan::Ended(MemberMobEnd {
                    cursor: event.cursor,
                    why: why.to_string(),
                }));
            }
            match page.last() {
                Some(last) if last.cursor > through => through = last.cursor,
                _ => return Ok(MemberMobScan::Running { through }),
            }
            if page.len() < MEMBER_MOB_EVENT_PAGE {
                return Ok(MemberMobScan::Running { through });
            }
        }
    }

    /// Submit one continuation for the identity.
    pub async fn submit_delivery(
        &self,
        identity: &MemberIdentity,
        delivery: meerkat::ContinuationDelivery,
        committed_at_ms: u64,
    ) -> Result<NativeReceipt, NativeSubmitError> {
        self.submit_delivery_to(&self.owner(identity), delivery, committed_at_ms)
            .await
    }

    /// Submit one continuation for a continuation owner.
    async fn submit_delivery_to(
        &self,
        owner: &meerkat::ContinuationOwner,
        delivery: meerkat::ContinuationDelivery,
        committed_at_ms: u64,
    ) -> Result<NativeReceipt, NativeSubmitError> {
        let service = self
            .service()
            .map_err(|error| NativeSubmitError::Unavailable(error.0))?;
        service
            .submit(owner, delivery, committed_at_ms)
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
        self.status_at(&self.owner(identity), key).await
    }

    /// The continuation owner's status of a native key for an owner.
    async fn status_at(
        &self,
        owner: &meerkat::ContinuationOwner,
        key: &str,
    ) -> Result<NativeStatus, NativeUnavailable> {
        let key = meerkat::ContinuationKey::new(key)
            .map_err(|error| NativeUnavailable(error.to_string()))?;
        self.service()?
            .continuation_status(owner, &key)
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

/// Members of this gateway's mob, and of mobs its members created, are
/// delivered to through its own runtime. A member on a peer gateway has no
/// path from this seam.
#[async_trait]
impl NativeDelivery for MobNativeDelivery {
    fn governs_work_authority(&self) -> Result<bool, NativeUnavailable> {
        MobNativeDelivery::governs_work_authority(self)
    }

    async fn route_for(&self, member: &Member) -> Result<MemberRouteRecord, MemberProblem> {
        if member.mob_id == self.mob_id {
            return Ok(MemberRouteRecord {
                route: MemberRoute::Local,
                incarnation: None,
            });
        }
        match self
            .child_incarnation(&member.mob_id)
            .await
            .map_err(|error| MemberProblem::Unavailable(error.0))?
        {
            Some(incarnation) => Ok(MemberRouteRecord {
                route: MemberRoute::Child,
                incarnation: Some(incarnation),
            }),
            None => Ok(MemberRouteRecord {
                route: MemberRoute::Peer,
                incarnation: None,
            }),
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

    fn own_mob(&self) -> Option<MobId> {
        Some(self.mob_id.clone())
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

    async fn check_member(&self, member: &Member, route: MemberRoute) -> Result<(), MemberProblem> {
        match route {
            MemberRoute::Local => match self.identity_service(&member.identity).await {
                Ok(IdentityService::Local) => Ok(()),
                Ok(IdentityService::Unknown) => Err(MemberProblem::Unknown),
                Ok(IdentityService::Elsewhere(detail)) => Err(MemberProblem::Unusable(detail)),
                Err(error) => Err(MemberProblem::Unavailable(error.0)),
            },
            MemberRoute::Child => self.check_child_member(member).await,
            MemberRoute::Peer => Err(MemberProblem::UnusablePeer(format!(
                "this gateway has no delivery path to members of {}",
                member.mob_id
            ))),
        }
    }

    async fn prepare_member(
        &self,
        member: &Member,
        route: MemberRoute,
    ) -> Result<(), PrepareError> {
        match route {
            MemberRoute::Local => self.prepare_identity(&member.identity).await,
            MemberRoute::Child => self.prepare_child_member(member).await,
            MemberRoute::Peer => Err(PrepareError::Unavailable(
                "this gateway has no delivery path to peer members".to_string(),
            )),
        }
    }

    async fn submit(
        &self,
        batch: &Batch,
        committed_at_ms: u64,
    ) -> Result<NativeReceipt, NativeSubmitError> {
        let owner =
            self.owner_for(&batch.member, batch.route)
                .map_err(|refusal| match refusal {
                    OwnerRefusal::NoPeerPath => NativeSubmitError::Unavailable(refusal.to_string()),
                    OwnerRefusal::LocalRouteNamesAnotherMob { .. } => {
                        NativeSubmitError::Invalid(refusal.to_string())
                    }
                })?;
        let delivery = continuation_for(batch).map_err(NativeSubmitError::Invalid)?;
        let receipt = self
            .submit_delivery_to(&owner, delivery, committed_at_ms)
            .await?;
        super::faults::after_native_commit();
        Ok(receipt)
    }

    async fn status(&self, batch: &Batch) -> Result<NativeStatus, NativeUnavailable> {
        let owner = self
            .owner_for(&batch.member, batch.route)
            .map_err(|refusal| NativeUnavailable(refusal.to_string()))?;
        self.status_at(&owner, batch.id.as_str()).await
    }

    async fn input(
        &self,
        _batch: &Batch,
        session_id: &str,
        input_id: &str,
    ) -> Result<Option<Value>, NativeUnavailable> {
        self.input_state(session_id, input_id).await
    }

    async fn member_mob_cursor(&self, mob_id: &MobId) -> Result<u64, NativeUnavailable> {
        MobNativeDelivery::member_mob_cursor(self, mob_id).await
    }

    async fn member_mob_gone(
        &self,
        mob_id: &MobId,
        incarnation: Option<&MobIncarnation>,
    ) -> Result<Option<MemberMobEnd>, NativeUnavailable> {
        MobNativeDelivery::member_mob_gone(self, mob_id, incarnation).await
    }

    async fn member_mob_end(
        &self,
        mob_id: &MobId,
        incarnation: Option<&MobIncarnation>,
        after: u64,
    ) -> Result<MemberMobScan, NativeUnavailable> {
        MobNativeDelivery::member_mob_end(self, mob_id, incarnation, after).await
    }
}

/// The owner's seam: this gateway's runtime for members of its own mob and
/// of mobs its members created, and keyed inject over the signed control
/// channel for members of mobs on peer gateways.
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

    /// A mob id hosted here as a mob a member created and also filed in the
    /// contact directory names two mobs: it is refused, never guessed.
    async fn route_for(&self, member: &Member) -> Result<MemberRouteRecord, MemberProblem> {
        let route = NativeDelivery::route_for(self.local.as_ref(), member).await?;
        if route.route == MemberRoute::Child
            && self
                .remote
                .as_deref()
                .is_some_and(|remote| remote.files(&member.mob_id))
        {
            return Err(MemberProblem::Ambiguous(format!(
                "{} is a mob hosted here and also a peer in the contact directory",
                member.mob_id
            )));
        }
        Ok(route)
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

    fn own_mob(&self) -> Option<MobId> {
        NativeDelivery::own_mob(self.local.as_ref())
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

    async fn check_member(&self, member: &Member, route: MemberRoute) -> Result<(), MemberProblem> {
        match route {
            MemberRoute::Local | MemberRoute::Child => {
                NativeDelivery::check_member(self.local.as_ref(), member, route).await
            }
            MemberRoute::Peer => self
                .remote()
                .map_err(MemberProblem::UnusablePeer)?
                .check_member(member),
        }
    }

    async fn prepare_member(
        &self,
        member: &Member,
        route: MemberRoute,
    ) -> Result<(), PrepareError> {
        match route {
            MemberRoute::Local | MemberRoute::Child => {
                NativeDelivery::prepare_member(self.local.as_ref(), member, route).await
            }
            // The peer prepares its own member when the batch reaches it.
            MemberRoute::Peer => Ok(()),
        }
    }

    async fn submit(
        &self,
        batch: &Batch,
        committed_at_ms: u64,
    ) -> Result<NativeReceipt, NativeSubmitError> {
        match batch.route {
            MemberRoute::Local | MemberRoute::Child => {
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
        match batch.route {
            MemberRoute::Local | MemberRoute::Child => {
                NativeDelivery::status(self.local.as_ref(), batch).await
            }
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
        match batch.route {
            MemberRoute::Local | MemberRoute::Child => {
                self.local.input_state(session_id, input_id).await
            }
            MemberRoute::Peer => self.remote().map_err(NativeUnavailable)?.input(batch).await,
        }
    }

    async fn member_mob_cursor(&self, mob_id: &MobId) -> Result<u64, NativeUnavailable> {
        self.local.member_mob_cursor(mob_id).await
    }

    async fn member_mob_gone(
        &self,
        mob_id: &MobId,
        incarnation: Option<&MobIncarnation>,
    ) -> Result<Option<MemberMobEnd>, NativeUnavailable> {
        self.local.member_mob_gone(mob_id, incarnation).await
    }

    async fn member_mob_end(
        &self,
        mob_id: &MobId,
        incarnation: Option<&MobIncarnation>,
        after: u64,
    ) -> Result<MemberMobScan, NativeUnavailable> {
        self.local.member_mob_end(mob_id, incarnation, after).await
    }
}
