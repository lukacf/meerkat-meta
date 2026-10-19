//! The announcement owner: announcement and subscription commands, delivery
//! observation, and the dispatcher that seals and delivers batches.
//!
//! The owner decides nothing about native delivery. It persists its intent
//! before every native effect, reads the native owner's evidence after it,
//! and moves a batch only through [`BatchState::apply`] as the store applies
//! it. The dispatcher runs when something changed (an announcement was
//! captured for a subscription, a subscription resumed, a batch is due) or
//! when a computed deadline passes. Its timer is a hint: no batch moves
//! without evidence.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use tokio::sync::{Mutex, Notify};

pub use super::native::AdmittedInput;
use super::native::{
    MemberMobScan, MemberProblem, MemberRoute, MobOrigin, MobPresence, NativeDelivery,
    NativeStatus, NativeSubmitError, OwnerRefusal, PrepareError, route_contradiction,
};
use super::store::AnnouncementStore;
use super::types::{
    AnnounceOutcome, AnnounceRequest, Announcement, AnnouncementStoreError, Batch, BatchEvent,
    BatchGap, BatchId, BatchPosition, BatchQuery, BatchState, BatchStateTag, ChildMemberLimits,
    GapReason, MAX_REMOTE_BODY_WIRE_BYTES, Member, MemberRouteRecord, MobId, MobIncarnation,
    MobLineage, NativeReceipt, OperationId, PutSubscription, SealingPolicy, SourceKey,
    Subscription, SubscriptionChange, SubscriptionChangeOutcome, SubscriptionEnd, SubscriptionId,
    SubscriptionState, SuccessorBatch, merge_gaps, render_item, successor_batch,
};

/// The owner's error for a member a put cannot subscribe.
fn member_problem(member: &Member, problem: MemberProblem) -> AnnouncementError {
    match problem {
        MemberProblem::Unknown => AnnouncementError::UnknownMember(member.clone()),
        MemberProblem::Invalid(detail) => AnnouncementError::InvalidMember(detail),
        MemberProblem::Unusable(detail) => AnnouncementError::UnusableMember(detail),
        MemberProblem::UnusablePeer(detail) => AnnouncementError::UnusablePeer(detail),
        MemberProblem::Unavailable(detail) => AnnouncementError::NativeUnavailable(detail),
        MemberProblem::Ambiguous(_) => AnnouncementError::AmbiguousMob(member.mob_id.clone()),
    }
}

/// Announcements and settled batches are kept this long after they settle.
pub const DEFAULT_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
/// The shortest retention window: a retry of an announcement or a subscription
/// change inside it still meets its original result.
pub const MIN_RETENTION_MS: u64 = 60 * 60 * 1_000;
/// Unsettled batches the dispatcher advances per round.
pub(super) const ROUND_BATCHES: usize = 64;
/// Members the dispatcher considers for sealing per round.
pub(super) const ROUND_MEMBERS: usize = 64;
/// Longest the dispatcher sleeps without a signal, in ms.
const MAX_IDLE_MS: u64 = 5 * 60 * 1_000;
/// How often retention runs.
const PRUNE_INTERVAL_MS: u64 = 60 * 60 * 1_000;
/// The pause between retention passes while retention is behind, so
/// commands get the store in between.
const CATCH_UP_PAUSE: Duration = Duration::from_millis(10);
/// How often a batch that keeps failing is reported again.
const FAILURE_REPORT_INTERVAL_MS: u64 = 10 * 60 * 1_000;
/// The first pause before a dispatcher that ended without a stop request is
/// started again; it doubles up to [`RESTART_PAUSE_MAX`].
const RESTART_PAUSE_MIN: Duration = Duration::from_secs(1);
const RESTART_PAUSE_MAX: Duration = Duration::from_mins(1);
/// One retention pass removes at most this many announcements, this many
/// batches and this many receipts; passes repeat while one stops at a limit.
const PRUNE_LIMIT: usize = 1_000;

/// Configurable announcement policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnnouncementConfig {
    pub policy: SealingPolicy,
    pub retention_ms: u64,
    /// Shortest pause before a batch the native owner has not settled yet
    /// is read again. The pause grows with the batch's age.
    pub settle_check_min_ms: u64,
    /// Longest such pause.
    pub settle_check_max_ms: u64,
    /// Per-member limits on the member tools.
    pub member_limits: super::tools::MemberLimits,
    /// How far a keyed inject request's issue time may be from a peer's
    /// clock: a request older than this is refused as stale, so a batch
    /// for a peer member whose last attempt is older may expire.
    pub peer_request_window_ms: u64,
    /// Bounds on subscriptions for members of mobs hosted here other than
    /// this gateway's own.
    pub child_limits: ChildMemberLimits,
    /// Whether every inline profile of every mob a member creates gets the
    /// member tools, so the members of those mobs subscribe themselves.
    pub member_tools_in_child_mobs: bool,
}

impl Default for AnnouncementConfig {
    fn default() -> Self {
        Self {
            policy: SealingPolicy::default(),
            retention_ms: DEFAULT_RETENTION_MS,
            settle_check_min_ms: 1_000,
            settle_check_max_ms: 30_000,
            member_limits: super::tools::MemberLimits::default(),
            peer_request_window_ms: crate::runtime::keyed_inject::DEFAULT_KEYED_INJECT_WINDOW_MS,
            child_limits: ChildMemberLimits::default(),
            member_tools_in_child_mobs: false,
        }
    }
}

/// Announcement refusals and failures, as callers see them.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AnnouncementError {
    Store(AnnouncementStoreError),
    /// This host's runtime admits work only through a native work
    /// authorization host, which refuses host continuations.
    GovernedRuntime,
    /// The member is not an identity registered with this host.
    UnknownMember(Member),
    /// The member is not a durable identity in its public form.
    InvalidMember(String),
    /// This gateway's runtime does not serve the member (it is placed on a
    /// host, or bound to an external runtime), so a delivery could never be
    /// admitted.
    UnusableMember(String),
    /// The named mobs are not hosted on this gateway.
    UnknownMobs(Vec<MobId>),
    /// The member's peer gateway cannot be called authenticated: no
    /// contact, no pinned gateway key, an opt-out of signed control
    /// responses, or no signing key on this gateway.
    UnusablePeer(String),
    /// A native read the request needs could not be made now.
    NativeUnavailable(String),
    /// A member asked for a mob that is not its own, above it or below it
    /// in the lineage of mobs members created.
    UnrelatedMob(MobId),
    /// The member's mob id names a mob hosted here and a peer in the
    /// contact directory alike, or would now route differently from how the
    /// member was first subscribed.
    AmbiguousMob(MobId),
}

impl std::fmt::Display for AnnouncementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(error) => error.fmt(f),
            Self::GovernedRuntime => f.write_str(
                "this host's runtime admits work only through a native work authorization \
                 host, which refuses host continuations",
            ),
            Self::UnknownMember(member) => {
                write!(f, "{member} is not an identity registered with this host")
            }
            Self::InvalidMember(detail) => write!(f, "invalid member: {detail}"),
            Self::UnusableMember(detail) => {
                write!(f, "the member cannot take a delivery: {detail}")
            }
            Self::UnknownMobs(mobs) => {
                let names = mobs
                    .iter()
                    .map(MobId::as_str)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "not hosted on this gateway: {names}")
            }
            Self::UnusablePeer(detail) => {
                write!(f, "the member's peer gateway is not usable: {detail}")
            }
            Self::NativeUnavailable(detail) => write!(f, "native state unavailable: {detail}"),
            Self::UnrelatedMob(mob_id) => write!(
                f,
                "mob {mob_id} is not the member's own mob, above it or below it"
            ),
            Self::AmbiguousMob(mob_id) => write!(
                f,
                "mob {mob_id} names a mob hosted here and a peer gateway alike"
            ),
        }
    }
}

impl std::error::Error for AnnouncementError {}

impl From<AnnouncementStoreError> for AnnouncementError {
    fn from(error: AnnouncementStoreError) -> Self {
        Self::Store(error)
    }
}

/// A live read of a batch's native delivery. Each variant is the native
/// owner's own status; none implies that the member processed anything.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[non_exhaustive]
pub enum NativeObservation {
    /// No native submission has started for this batch.
    NotSubmitted,
    /// The native owner holds no commit for the batch's key.
    NotCommitted,
    /// Committed to the member's inbox, not yet admitted.
    Pending {
        receipt: NativeReceipt,
        #[serde(skip_serializing_if = "Option::is_none")]
        admitted: Option<AdmittedInput>,
    },
    /// Admitted as one input of the member's session.
    Applied {
        receipt: NativeReceipt,
        session_id: String,
        input_id: String,
    },
    /// Committed to an incarnation that retired before admitting it.
    Stranded { receipt: NativeReceipt },
    /// Refused by the native owner.
    Refused {
        receipt: NativeReceipt,
        reason: String,
    },
    /// The native status could not be read now; nothing is implied.
    Unavailable { detail: String },
}

/// Everything known about one delivery: the immutable batch with its owner
/// state, the native owner's live status, and, once admitted, the native
/// input's own state.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeliveryObservation {
    pub batch: Batch,
    pub native: NativeObservation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<Value>,
}

/// What one dispatcher round did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoundReport {
    pub sealed: u32,
    pub advanced: u32,
    /// The earliest time something is due again, if anything is.
    pub next_due_ms: Option<u64>,
}

type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// When a batch nothing native holds expires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expiry {
    NotYet,
    Now,
    /// Once the peer's request window has passed since the last attempt.
    AfterQuietWindow(u64),
}

/// Which subscriptions still want a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Interest {
    Active,
    Paused,
    None,
}

fn system_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// The dispatcher's walk over unsettled batches, in (sealed time, id)
/// order. Batches that stay unsettled for long (a long turn, a paused
/// subscription, an unresolvable member, an unreadable status) cannot take
/// every place of every round, and none is read faster than its own pace.
#[derive(Debug, Default)]
struct Walk {
    /// Where the next round resumes while a lap is under way.
    position: Option<BatchPosition>,
    /// The newest unsettled batch when the lap started: the lap ends there,
    /// so batches sealed meanwhile wait for the next lap and full windows
    /// of newer batches cannot leave an older one behind.
    lap_end: Option<BatchPosition>,
    /// When each recently read batch is read again: its native retry time,
    /// or, after a failure, the longest settle pause. Only future times are
    /// kept, and only for batches still unsettled when last seen.
    due: BTreeMap<BatchPosition, u64>,
    /// When a batch that keeps failing was last reported.
    reported: BTreeMap<BatchPosition, u64>,
    /// Members whose backlog failed to seal since it last sealed: reported
    /// once until it seals again or is gone.
    unsealed: BTreeSet<Member>,
}

impl Walk {
    /// Record a batch just read: when to read it again. A batch that needs
    /// no timer (settled, or held by a paused subscription) keeps no entry.
    fn read(&mut self, position: BatchPosition, due: Option<u64>) {
        self.reported.remove(&position);
        match due {
            Some(at) => {
                self.due.insert(position, at);
            }
            None => {
                self.due.remove(&position);
            }
        }
    }

    /// A window holds every unsettled batch in the range it covers, from
    /// the walk's position through its last batch, or to the end when it is
    /// not full. A batch the walk remembers in that range and the window
    /// lacks has settled or was pruned since: it leaves the walk.
    fn drop_settled(&mut self, window: &[Batch], full: bool) {
        let held = window
            .iter()
            .map(BatchPosition::of)
            .collect::<BTreeSet<_>>();
        let after = self.position.clone();
        let through = if full { held.last().cloned() } else { None };
        let kept = |position: &BatchPosition| {
            held.contains(position)
                || after.as_ref().is_some_and(|after| position <= after)
                || through.as_ref().is_some_and(|through| position > through)
        };
        self.due.retain(|position, _| kept(position));
        self.reported.retain(|position, _| kept(position));
    }
}

struct Inner {
    store: Arc<dyn AnnouncementStore>,
    native: Arc<dyn NativeDelivery>,
    config: AnnouncementConfig,
    wake: Arc<Notify>,
    /// Held for a whole round; it owns the walk over unsettled batches.
    round: Mutex<Walk>,
    clock: Clock,
    stopped: AtomicBool,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Let a waiting dispatcher notice the service is gone.
        self.wake.notify_one();
    }
}

/// The announcement owner. Cheap to clone; clones share one owner.
#[derive(Clone)]
pub struct AnnouncementService {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for AnnouncementService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnnouncementService")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

fn min_due(current: Option<u64>, candidate: Option<u64>) -> Option<u64> {
    match (current, candidate) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// Members on a peer gateway get batches whose encoded body fits one
/// delivery request.
fn wire_budget(route: MemberRoute) -> Option<u64> {
    match route {
        MemberRoute::Local | MemberRoute::Child => None,
        MemberRoute::Peer => Some(MAX_REMOTE_BODY_WIRE_BYTES),
    }
}

/// The longest chain of mobs a lineage walk follows, up or down.
const MAX_LINEAGE_DEPTH: usize = 64;

/// The mobs a member of `own` reaches by the lineage `records`: its own
/// mob, the chain of mobs above it, and every mob below it. Siblings and
/// cousins are not reached. Each mob is visited once, so records that loop
/// (an id reused under a new parent) end the walk instead of repeating it.
pub(crate) fn reach_from(records: &[MobLineage], own: &MobId) -> BTreeSet<MobId> {
    let incarnation_of = records
        .iter()
        .map(|record| (&record.mob_id, &record.incarnation))
        .collect::<BTreeMap<_, _>>();
    // An edge holds only to the parent incarnation it was derived under: a
    // parent created again under the same id adopts no child of the old
    // one. The root, this gateway's own mob, has no record.
    let parent_of = records
        .iter()
        .filter_map(|record| {
            let parent = record.parent.as_ref()?;
            let holds = match (&record.parent_incarnation, incarnation_of.get(parent)) {
                (None, None) => true,
                (Some(derived_under), Some(live)) => derived_under == *live,
                _ => false,
            };
            holds.then_some((&record.mob_id, parent))
        })
        .collect::<BTreeMap<_, _>>();
    let mut children = BTreeMap::<&MobId, Vec<&MobId>>::new();
    for (child, parent) in &parent_of {
        children.entry(*parent).or_default().push(*child);
    }
    let mut reach = BTreeSet::from([own.clone()]);
    let mut at = own;
    for _ in 0..MAX_LINEAGE_DEPTH {
        let Some(parent) = parent_of.get(at) else {
            break;
        };
        if !reach.insert((*parent).clone()) {
            break;
        }
        at = parent;
    }
    let mut frontier = vec![own];
    for _ in 0..MAX_LINEAGE_DEPTH {
        let mut next = Vec::new();
        for mob_id in frontier {
            for child in children.get(mob_id).into_iter().flatten() {
                if reach.insert((*child).clone()) {
                    next.push(*child);
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    reach
}

fn new_batch_id() -> Result<BatchId, AnnouncementError> {
    Ok(BatchId::new(format!("batch-{}", uuid::Uuid::new_v4()))?)
}

impl AnnouncementService {
    pub fn new(
        store: Arc<dyn AnnouncementStore>,
        native: Arc<dyn NativeDelivery>,
        config: AnnouncementConfig,
    ) -> Result<Self, AnnouncementError> {
        Self::with_clock(store, native, config, Arc::new(system_clock))
    }

    pub(crate) fn with_clock(
        store: Arc<dyn AnnouncementStore>,
        native: Arc<dyn NativeDelivery>,
        config: AnnouncementConfig,
        clock: Clock,
    ) -> Result<Self, AnnouncementError> {
        config.policy.validate()?;
        if config.retention_ms < MIN_RETENTION_MS {
            return Err(AnnouncementStoreError::Invalid(format!(
                "retention must be at least {MIN_RETENTION_MS} ms"
            ))
            .into());
        }
        if config.settle_check_min_ms == 0
            || config.settle_check_min_ms > config.settle_check_max_ms
        {
            return Err(AnnouncementStoreError::Invalid(
                "settle checks need 0 < min <= max".into(),
            )
            .into());
        }
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                native,
                config,
                wake: Arc::new(Notify::new()),
                round: Mutex::new(Walk::default()),
                clock,
                stopped: AtomicBool::new(false),
            }),
        })
    }

    pub fn config(&self) -> &AnnouncementConfig {
        &self.inner.config
    }

    fn now(&self) -> u64 {
        (self.inner.clock)()
    }

    /// Ask the dispatcher for a round.
    pub fn wake(&self) {
        self.inner.wake.notify_one();
    }

    /// Stop the dispatcher. Commands still work; nothing is delivered
    /// until a new dispatcher starts.
    pub fn stop(&self) {
        self.inner.stopped.store(true, Ordering::SeqCst);
        self.inner.wake.notify_one();
    }

    fn ensure_deliverable(&self) -> Result<(), AnnouncementError> {
        match self.inner.native.governs_work_authority() {
            Ok(false) => Ok(()),
            Ok(true) => Err(AnnouncementError::GovernedRuntime),
            Err(error) => Err(AnnouncementError::NativeUnavailable(error.0)),
        }
    }

    /// Refuse mobs that are not hosted here, from the mob owner's own
    /// records.
    async fn ensure_hosted(
        &self,
        mobs: &BTreeSet<MobId>,
    ) -> Result<BTreeSet<MobId>, AnnouncementError> {
        let presence = self
            .inner
            .native
            .mob_presence(mobs)
            .await
            .map_err(|error| AnnouncementError::NativeUnavailable(error.0))?;
        let gone = mobs
            .iter()
            .filter(|mob_id| presence.get(*mob_id) != Some(&MobPresence::Hosted))
            .cloned()
            .collect::<Vec<_>>();
        if gone.is_empty() {
            Ok(mobs.clone())
        } else {
            Err(AnnouncementError::UnknownMobs(gone))
        }
    }

    /// Announce into a mob hosted here.
    pub async fn announce(
        &self,
        request: AnnounceRequest,
    ) -> Result<AnnounceOutcome, AnnouncementError> {
        self.ensure_hosted(&BTreeSet::from([request.mob_id.clone()]))
            .await?;
        let outcome = self.inner.store.announce(request, self.now()).await?;
        if !outcome.captured_for.is_empty() {
            self.wake();
        }
        Ok(outcome)
    }

    pub async fn announcement(
        &self,
        mob_id: &MobId,
        source_key: &SourceKey,
    ) -> Result<Option<Announcement>, AnnouncementError> {
        Ok(self.inner.store.announcement(mob_id, source_key).await?)
    }

    pub async fn put_subscription(
        &self,
        operation: &OperationId,
        mut request: PutSubscription,
    ) -> Result<Subscription, AnnouncementError> {
        let route = self.member_route(&request.member).await?;
        // A peer answers for its own runtime when a batch reaches it.
        if route.route != MemberRoute::Peer {
            self.ensure_deliverable()?;
        }
        // A member of a mob a member created is subscribed at that mob's
        // current event cursor, read before the member is checked: a
        // completion or reset the check does not see is in the log after it
        // and ends the subscription.
        request.member_mob_cursor = match route.route {
            MemberRoute::Child => Some(
                self.inner
                    .native
                    .member_mob_cursor(&request.member.mob_id)
                    .await
                    .map_err(|error| AnnouncementError::NativeUnavailable(error.0))?,
            ),
            MemberRoute::Local | MemberRoute::Peer => None,
        };
        if let Err(problem) = self
            .inner
            .native
            .check_member(&request.member, route.route)
            .await
        {
            return Err(member_problem(&request.member, problem));
        }
        request.route = Some(route);
        request.child_limits = self.inner.config.child_limits;
        let mobs = self.ensure_hosted(&request.selector.mobs).await?;
        let member_mob = request.member.mob_id.clone();
        let subscription = self
            .inner
            .store
            .put_subscription(operation, request, self.now())
            .await
            .map_err(|error| match error {
                // Another put kept the member on another route meanwhile.
                AnnouncementStoreError::RouteKept { .. } => {
                    AnnouncementError::AmbiguousMob(member_mob)
                }
                other => AnnouncementError::Store(other),
            })?;
        // A mob destroyed between the check and the commit would otherwise
        // stay named until the mob set changes again.
        if let Err(error) = self.drop_ended(&mobs).await {
            tracing::warn!(%error, "destroyed mobs not reconciled after a subscription change");
        }
        self.wake();
        Ok(subscription)
    }

    /// The route a put subscribes the member on: the route it is kept on,
    /// or the route it takes now when it has none. A kept route never
    /// changes. A member kept for a mob a member created that is now gone or
    /// created again first ends there and loses its kept route; a member
    /// whose mob id would now route another way is refused as ambiguous.
    async fn member_route(&self, member: &Member) -> Result<MemberRouteRecord, AnnouncementError> {
        let current = self
            .inner
            .native
            .route_for(member)
            .await
            .map_err(|problem| member_problem(member, problem))?;
        let Some(kept) = self.inner.store.member_route(member).await? else {
            return Ok(current);
        };
        if kept == current {
            return Ok(kept);
        }
        if kept.route != MemberRoute::Child {
            return Err(AnnouncementError::AmbiguousMob(member.mob_id.clone()));
        }
        let why = if current.route == MemberRoute::Child {
            "created again"
        } else {
            "destroyed"
        };
        let ended = self
            .inner
            .store
            .unbind_member_route(
                member,
                SubscriptionEnd::MemberMobEnded {
                    why: why.to_string(),
                },
                self.now(),
            )
            .await?;
        tracing::info!(
            member = %member,
            why,
            subscriptions = ended.len(),
            "member's mob ended; its subscriptions ended there"
        );
        Ok(current)
    }

    /// Create or replace a subscription a member asks for itself: every mob
    /// it names must be its own mob, above it, or below it in the lineage
    /// of mobs members created. A named mob without a live lineage record
    /// is derived first. A mob still outside that reach is refused with
    /// [`AnnouncementError::UnrelatedMob`], and nothing changes.
    pub async fn put_member_subscription(
        &self,
        operation: &OperationId,
        request: PutSubscription,
    ) -> Result<Subscription, AnnouncementError> {
        self.ensure_within_reach(&request.member.mob_id, &request.selector.mobs)
            .await?;
        self.put_subscription(operation, request).await
    }

    /// The mobs a member of `own` may name in a subscription it makes
    /// itself, by the live lineage records.
    pub async fn reach(&self, own: &MobId) -> Result<BTreeSet<MobId>, AnnouncementError> {
        Ok(reach_from(&self.inner.store.lineage().await?, own))
    }

    async fn ensure_within_reach(
        &self,
        own: &MobId,
        mobs: &BTreeSet<MobId>,
    ) -> Result<(), AnnouncementError> {
        let records = self.inner.store.lineage().await?;
        if mobs.is_subset(&reach_from(&records, own)) {
            return Ok(());
        }
        // A mob seen for the first time gets its record before the check.
        let recorded = records
            .into_iter()
            .map(|record| (record.mob_id, record.incarnation))
            .collect::<BTreeMap<_, _>>();
        for mob_id in mobs.iter().chain([own]) {
            if !recorded.contains_key(mob_id) {
                self.derive_lineage(mob_id, None).await?;
            }
        }
        let reach = self.reach(own).await?;
        match mobs.iter().find(|mob_id| !reach.contains(*mob_id)) {
            Some(mob_id) => Err(AnnouncementError::UnrelatedMob(mob_id.clone())),
            None => Ok(()),
        }
    }

    /// Record the lineage of the hosted mob `mob_id` unless its live record
    /// is already for the incarnation hosted now: a relation is derived
    /// once. A missing parent is final only once every mob this gateway
    /// hosts has been restored; before that nothing is written. A read
    /// that fails writes nothing.
    async fn derive_lineage(
        &self,
        mob_id: &MobId,
        recorded: Option<&MobIncarnation>,
    ) -> Result<(), AnnouncementError> {
        // This gateway's own mob is the root: it has no record.
        if self.inner.native.own_mob().as_ref() == Some(mob_id) {
            return Ok(());
        }
        let Some(facts) = self
            .inner
            .native
            .mob_origin(mob_id)
            .await
            .map_err(|error| AnnouncementError::NativeUnavailable(error.0))?
        else {
            return Ok(());
        };
        if recorded == Some(&facts.incarnation) {
            return Ok(());
        }
        let (parent, parent_incarnation, creator) = match facts.origin {
            MobOrigin::Member {
                parent,
                creator,
                parent_incarnation,
            } => (Some(parent), parent_incarnation, Some(creator)),
            MobOrigin::Unproven(detail) => {
                tracing::info!(mob = %mob_id, %detail, "mob has no member origin");
                (None, None, None)
            }
            MobOrigin::ParentNotHosted(parent) => {
                if !self.inner.native.mobs_restored() {
                    return Ok(());
                }
                tracing::info!(mob = %mob_id, parent = %parent, "the mob's parent is gone");
                (None, None, None)
            }
        };
        let now = self.now();
        self.inner
            .store
            .record_lineage(
                &MobLineage {
                    mob_id: mob_id.clone(),
                    incarnation: facts.incarnation,
                    parent,
                    parent_incarnation,
                    creator,
                    recorded_at_ms: now,
                },
                now,
            )
            .await?;
        Ok(())
    }

    /// Derive the lineage of every hosted mob whose current incarnation has
    /// no live record, and end the records of mobs no longer hosted. A mob
    /// whose read fails is tried again at the next reconcile; the others
    /// are recorded meanwhile.
    pub async fn reconcile_lineage(&self) -> Result<(), AnnouncementError> {
        let hosted = self
            .inner
            .native
            .hosted_mob_ids()
            .await
            .map_err(|error| AnnouncementError::NativeUnavailable(error.0))?;
        let records = self.inner.store.lineage().await?;
        let gone = records
            .iter()
            .filter(|record| !hosted.contains(&record.mob_id))
            .map(|record| record.mob_id.clone())
            .collect::<BTreeSet<_>>();
        if !gone.is_empty() {
            self.inner.store.end_lineage(&gone, self.now()).await?;
        }
        let recorded = records
            .into_iter()
            .map(|record| (record.mob_id, record.incarnation))
            .collect::<BTreeMap<_, _>>();
        let mut failed = None;
        for mob_id in &hosted {
            if let Err(error) = self.derive_lineage(mob_id, recorded.get(mob_id)).await {
                tracing::warn!(mob = %mob_id, %error, "mob lineage not derived yet");
                failed = Some(error);
            }
        }
        failed.map_or(Ok(()), Err)
    }

    /// Drop the mobs among `mobs` that are no longer hosted here from every
    /// live subscription that names them; a subscription left with none
    /// ends. Returns the subscriptions that changed.
    async fn drop_ended(
        &self,
        mobs: &BTreeSet<MobId>,
    ) -> Result<Vec<SubscriptionId>, AnnouncementError> {
        if mobs.is_empty() {
            return Ok(Vec::new());
        }
        let presence = self
            .inner
            .native
            .mob_presence(mobs)
            .await
            .map_err(|error| AnnouncementError::NativeUnavailable(error.0))?;
        let gone = mobs
            .iter()
            .filter(|mob_id| presence.get(*mob_id) == Some(&MobPresence::Gone))
            .cloned()
            .collect::<BTreeSet<_>>();
        if gone.is_empty() {
            return Ok(Vec::new());
        }
        let changed = self.inner.store.drop_ended_mobs(&gone, self.now()).await?;
        if !changed.is_empty() {
            self.wake();
        }
        Ok(changed)
    }

    /// Reconcile every mob the store knows against the mob owner's records:
    /// destroyed mobs leave the subscriptions that name them, and their logs
    /// are marked ended for retention. Then reconcile the lineage records
    /// ([`Self::reconcile_lineage`]).
    pub async fn reconcile_mobs(&self) -> Result<Vec<SubscriptionId>, AnnouncementError> {
        let known = self.inner.store.known_mobs().await?;
        let changed = self.drop_ended(&known).await?;
        self.reconcile_child_members().await?;
        self.reconcile_lineage().await?;
        Ok(changed)
    }

    /// End the subscriptions of members kept for a mob a member created
    /// that is now gone or was created again. A mob that cannot be read now
    /// (held until its owner resolves, or unavailable) is left to the next
    /// reconcile or delivery.
    async fn reconcile_child_members(&self) -> Result<(), AnnouncementError> {
        let routes = self.inner.store.child_member_routes().await?;
        let mut gone: BTreeMap<(MobId, Option<MobIncarnation>), Option<String>> = BTreeMap::new();
        let now = self.now();
        for (member, kept) in routes {
            let key = (member.mob_id.clone(), kept.incarnation.clone());
            let why = match gone.get(&key) {
                Some(why) => why.clone(),
                None => {
                    match self
                        .inner
                        .native
                        .member_mob_gone(&member.mob_id, kept.incarnation.as_ref())
                        .await
                    {
                        Ok(end) => {
                            let why = end.map(|end| end.why);
                            gone.insert(key, why.clone());
                            why
                        }
                        Err(error) => {
                            tracing::warn!(
                                mob = %member.mob_id,
                                error = %error,
                                "member's mob not read; its members are reconciled later"
                            );
                            gone.insert(key, None);
                            None
                        }
                    }
                }
            };
            if let Some(why) = why {
                self.end_member(&member, SubscriptionEnd::MemberMobEnded { why }, now)
                    .await;
            }
        }
        Ok(())
    }

    /// Follow the mob owner's set of mobs: reconcile once now, then on every
    /// change. A reconcile that cannot read the mob owner is tried again
    /// after the slowest settle pause. Holds the service weakly and ends
    /// with it; `None` when nothing signals mob set changes.
    pub fn watch_mobs(&self) -> Option<tokio::task::JoinHandle<()>> {
        let mut changes = self.inner.native.mob_set_changes()?;
        let weak = Arc::downgrade(&self.inner);
        Some(tokio::spawn(async move {
            loop {
                let retry = {
                    let Some(inner) = weak.upgrade() else {
                        return;
                    };
                    if inner.stopped.load(Ordering::SeqCst) {
                        return;
                    }
                    let service = AnnouncementService { inner };
                    match service.reconcile_mobs().await {
                        Ok(_) => None,
                        Err(error) => {
                            tracing::warn!(%error, "destroyed mobs not reconciled; retrying");
                            Some(Duration::from_millis(
                                service.inner.config.settle_check_max_ms,
                            ))
                        }
                    }
                };
                let changed = match retry {
                    None => changes.changed().await.is_ok(),
                    Some(pause) => tokio::select! {
                        changed = changes.changed() => changed.is_ok(),
                        () = tokio::time::sleep(pause) => true,
                    },
                };
                if !changed {
                    return;
                }
            }
        }))
    }

    /// End every live subscription of a member that is gone for good.
    async fn end_member(&self, member: &Member, end: SubscriptionEnd, now: u64) {
        match self.inner.store.end_member(member, end.clone(), now).await {
            Ok(ended) if !ended.is_empty() => {
                tracing::info!(
                    member = %member,
                    reason = %end,
                    subscriptions = ended.len(),
                    "member subscriptions ended"
                );
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(member = %member, %error, "member subscriptions not ended");
            }
        }
    }

    pub async fn change_subscription(
        &self,
        operation: &OperationId,
        id: &SubscriptionId,
        expected_revision: u64,
        change: SubscriptionChange,
    ) -> Result<SubscriptionChangeOutcome, AnnouncementError> {
        let outcome = self
            .inner
            .store
            .change_subscription(operation, id, expected_revision, change, self.now())
            .await?;
        self.wake();
        Ok(outcome)
    }

    pub async fn subscription(
        &self,
        id: &SubscriptionId,
    ) -> Result<Option<Subscription>, AnnouncementError> {
        Ok(self.inner.store.subscription(id).await?)
    }

    pub async fn subscriptions(
        &self,
        member: Option<&Member>,
        after: Option<&SubscriptionId>,
        limit: usize,
    ) -> Result<Vec<Subscription>, AnnouncementError> {
        Ok(self.inner.store.subscriptions(member, after, limit).await?)
    }

    pub async fn deliveries(&self, query: &BatchQuery) -> Result<Vec<Batch>, AnnouncementError> {
        Ok(self.inner.store.batches(query).await?)
    }

    /// The delivery's immutable batch, its owner state, and the native
    /// owner's live status. Reads only; serves and changes nothing.
    pub async fn observe_delivery(
        &self,
        id: &BatchId,
    ) -> Result<DeliveryObservation, AnnouncementError> {
        let batch = self
            .inner
            .store
            .batch(id)
            .await?
            .ok_or(AnnouncementStoreError::UnknownBatch)?;
        self.observe(batch).await
    }

    /// Advance one delivery as far as native evidence allows now, then
    /// observe it.
    pub async fn reconcile_delivery(
        &self,
        id: &BatchId,
    ) -> Result<DeliveryObservation, AnnouncementError> {
        let batch = {
            let mut walk = self.inner.round.lock().await;
            let batch = self
                .inner
                .store
                .batch(id)
                .await?
                .ok_or(AnnouncementStoreError::UnknownBatch)?;
            let position = BatchPosition::of(&batch);
            let now = self.now();
            let due = self.drive(batch, now).await?;
            // The walk paces its next look at the batch from this read; a
            // batch this settled leaves the walk.
            walk.read(position, due);
            self.inner
                .store
                .batch(id)
                .await?
                .ok_or(AnnouncementStoreError::UnknownBatch)?
        };
        // A batch this settled frees its member for the next one now,
        // not at the dispatcher's next timer.
        self.wake();
        self.observe(batch).await
    }

    async fn observe(&self, batch: Batch) -> Result<DeliveryObservation, AnnouncementError> {
        if batch.state.tag() == BatchStateTag::Sealed {
            return Ok(DeliveryObservation {
                batch,
                native: NativeObservation::NotSubmitted,
                input: None,
            });
        }
        // Nothing is read for a batch whose route contradicts its member,
        // here as on every drive path: the one guard answers instead.
        if let Some(refusal) = self.route_refusal(&batch) {
            return Ok(DeliveryObservation {
                batch,
                native: NativeObservation::Unavailable {
                    detail: refusal.to_string(),
                },
                input: None,
            });
        }
        let native = match self.inner.native.status(&batch).await {
            Err(error) => NativeObservation::Unavailable { detail: error.0 },
            Ok(status) => Self::native_observation(status),
        };
        let input = match &native {
            NativeObservation::Applied {
                session_id,
                input_id,
                ..
            } => match self.inner.native.input(&batch, session_id, input_id).await {
                Ok(input) => input,
                Err(error) => Some(serde_json::json!({ "unavailable": error.0 })),
            },
            _ => None,
        };
        Ok(DeliveryObservation {
            batch,
            native,
            input,
        })
    }

    fn native_observation(status: NativeStatus) -> NativeObservation {
        match status {
            NativeStatus::NotCommitted => NativeObservation::NotCommitted,
            NativeStatus::Pending { receipt, admitted } => {
                NativeObservation::Pending { receipt, admitted }
            }
            NativeStatus::Applied {
                receipt,
                session_id,
                input_id,
            } => NativeObservation::Applied {
                receipt,
                session_id,
                input_id,
            },
            NativeStatus::Stranded { receipt } => NativeObservation::Stranded { receipt },
            NativeStatus::Refused { receipt, reason } => {
                NativeObservation::Refused { receipt, reason }
            }
        }
    }

    /// One dispatcher round: advance a window of unsettled batches from
    /// native evidence, then seal and deliver due backlogs.
    pub async fn run_round(&self) -> Result<RoundReport, AnnouncementError> {
        let mut guard = self.inner.round.lock().await;
        let walk = &mut *guard;
        let now = self.now();
        let config = self.inner.config;
        let mut report = RoundReport::default();
        // No next look is ever written further ahead than the slowest settle
        // pause, and no report is in the future; one that is predates a step
        // back of the clock.
        walk.due
            .retain(|_, at| *at > now && *at - now <= config.settle_check_max_ms);
        walk.reported
            .retain(|_, at| *at <= now && now - *at < FAILURE_REPORT_INTERVAL_MS);
        if walk.position.is_none() {
            walk.lap_end = self.inner.store.newest_unsettled_batch().await?;
        }
        let mut window = self
            .inner
            .store
            .unsettled_batches(walk.position.as_ref(), ROUND_BATCHES)
            .await?;
        let full = window.len() == ROUND_BATCHES;
        walk.drop_settled(&window, full);
        let lap_end = walk.lap_end.clone();
        window.retain(|batch| {
            lap_end
                .as_ref()
                .is_some_and(|end| BatchPosition::of(batch) <= *end)
        });
        // A full window short of the lap's end resumes after it in the next
        // round, soon. Anything else ends the lap, which starts over.
        walk.position = match window.last().map(BatchPosition::of) {
            Some(last) if full && lap_end.as_ref().is_some_and(|end| last < *end) => {
                report.next_due_ms = Some(now.saturating_add(config.settle_check_min_ms));
                Some(last)
            }
            _ => {
                // The lap ends: the next one starts by the time the first
                // batch it remembers is due again.
                report.next_due_ms = min_due(report.next_due_ms, walk.due.values().min().copied());
                None
            }
        };
        // A batch or backlog the round cannot advance is reported and
        // retried at the slowest pace; it never ends the round for the
        // others.
        for batch in window {
            let position = BatchPosition::of(&batch);
            if let Some(&at) = walk.due.get(&position) {
                report.next_due_ms = min_due(report.next_due_ms, Some(at));
                continue;
            }
            match self.advance(batch, now).await {
                Ok((moved, due)) => {
                    report.advanced += u32::from(moved);
                    self.read_again(walk, &mut report, position, due);
                }
                Err(error) => self.retry_later(walk, &mut report, position, now, &error),
            }
        }
        let policy = config.policy;
        let backlogs = self.inner.store.backlogs(ROUND_MEMBERS).await?;
        walk.unsealed
            .retain(|member| backlogs.iter().any(|backlog| backlog.member == *member));
        for backlog in backlogs {
            match policy.due_at(&backlog) {
                Some(due) if due <= now => {
                    let id = new_batch_id()?;
                    let sealed = self.seal_backlog(&backlog.member, &id, &policy, now).await;
                    if sealed.is_ok() {
                        walk.unsealed.remove(&backlog.member);
                    }
                    match sealed {
                        Ok(Some(batch)) => {
                            report.sealed += 1;
                            let position = BatchPosition::of(&batch);
                            match self.drive(batch, now).await {
                                Ok(due) => self.read_again(walk, &mut report, position, due),
                                Err(error) => {
                                    self.retry_later(walk, &mut report, position, now, &error);
                                }
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            // Reported once while it keeps failing: a mob
                            // held until its owner resolves fails every
                            // round until it runs again.
                            if walk.unsealed.insert(backlog.member.clone()) {
                                tracing::warn!(
                                    member = %backlog.member,
                                    %error,
                                    "announcement backlog not sealed this round"
                                );
                            } else {
                                tracing::debug!(
                                    member = %backlog.member,
                                    %error,
                                    "announcement backlog still not sealed"
                                );
                            }
                            report.next_due_ms = min_due(
                                report.next_due_ms,
                                Some(now.saturating_add(config.settle_check_max_ms)),
                            );
                        }
                    }
                }
                Some(due) => report.next_due_ms = min_due(report.next_due_ms, Some(due)),
                None => {}
            }
        }
        Ok(report)
    }

    /// Seal a member's due backlog into one batch on its kept route. The
    /// log of a member's own mob, for a member of a mob a member created,
    /// is read first: a subscription that mob's end ended carries nothing
    /// into the batch, so a batch never mixes what was captured before a
    /// reset with what a subscription made after it wants.
    async fn seal_backlog(
        &self,
        member: &Member,
        id: &BatchId,
        policy: &SealingPolicy,
        now: u64,
    ) -> Result<Option<Batch>, AnnouncementError> {
        // Every subscription keeps its member's route, so a member with
        // owed captures has one. Without it nothing is sealed: no route is
        // assumed.
        let route = self
            .inner
            .store
            .member_route(member)
            .await?
            .map(|kept| kept.route)
            .ok_or_else(|| AnnouncementStoreError::RouteNotKept {
                member: member.clone(),
            })?;
        if route == MemberRoute::Child {
            self.member_mob_ended(member, now).await?;
        }
        Ok(self
            .inner
            .store
            .seal_batch(member, id, policy, wire_budget(route), now)
            .await?)
    }

    /// Record when a batch the round just read is read again.
    fn read_again(
        &self,
        walk: &mut Walk,
        report: &mut RoundReport,
        position: BatchPosition,
        due: Option<u64>,
    ) {
        report.next_due_ms = min_due(report.next_due_ms, due);
        walk.read(position, due);
    }

    /// A batch the round could not advance is retried at the slowest settle
    /// pace and reported at most once per interval.
    fn retry_later(
        &self,
        walk: &mut Walk,
        report: &mut RoundReport,
        position: BatchPosition,
        now: u64,
        error: &AnnouncementError,
    ) {
        let at = now.saturating_add(self.inner.config.settle_check_max_ms);
        report.next_due_ms = min_due(report.next_due_ms, Some(at));
        walk.due.insert(position.clone(), at);
        match walk.reported.entry(position) {
            std::collections::btree_map::Entry::Occupied(entry) => {
                tracing::debug!(batch = %entry.key().id, %error, "announcement batch still not advanced");
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                tracing::warn!(
                    batch = %entry.key().id,
                    %error,
                    "announcement batch not advanced; retried at the slowest settle pace"
                );
                entry.insert(now);
            }
        }
    }

    /// How many batches the walk keeps a next look for, and how many a
    /// failure report for.
    #[cfg(test)]
    pub(crate) async fn walk_entries(&self) -> (usize, usize) {
        let walk = self.inner.round.lock().await;
        (walk.due.len(), walk.reported.len())
    }

    /// Seal one member's due backlog now, as a round does.
    #[cfg(test)]
    pub(crate) async fn seal_member(
        &self,
        member: &Member,
    ) -> Result<Option<Batch>, AnnouncementError> {
        let id = new_batch_id()?;
        let policy = self.inner.config.policy;
        self.seal_backlog(member, &id, &policy, self.now()).await
    }

    /// When each batch the walk reports as failing was last reported.
    #[cfg(test)]
    pub(crate) async fn walk_reported_at(&self) -> Vec<u64> {
        let walk = self.inner.round.lock().await;
        walk.reported.values().copied().collect()
    }

    /// Drive one unsettled batch: whether its state moved, and when it
    /// should be looked at again.
    async fn advance(
        &self,
        batch: Batch,
        now: u64,
    ) -> Result<(bool, Option<u64>), AnnouncementError> {
        let before = batch.state.tag();
        let id = batch.id.clone();
        let due = self.drive(batch, now).await?;
        let moved = self
            .inner
            .store
            .batch(&id)
            .await?
            .is_some_and(|after| after.state.tag() != before);
        Ok((moved, due))
    }

    /// Start the dispatcher under a supervisor. Both hold the service weakly
    /// and end when it is dropped or stopped. A dispatcher that ends any
    /// other way (a panic) is reported and started again after a pause that
    /// doubles up to a minute.
    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        self.inner.stopped.store(false, Ordering::SeqCst);
        let first = self.spawn_dispatcher();
        let weak = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            let mut dispatcher = first;
            let mut pause = RESTART_PAUSE_MIN;
            loop {
                let Err(error) = dispatcher.await else {
                    return;
                };
                tracing::error!(
                    %error,
                    "mob announcement dispatcher ended unexpectedly; it starts again"
                );
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(RESTART_PAUSE_MAX);
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                if inner.stopped.load(Ordering::SeqCst) {
                    return;
                }
                dispatcher = AnnouncementService { inner }.spawn_dispatcher();
            }
        })
    }

    /// One dispatcher task. It never clears a stop request, so a restart
    /// racing a stop ends at once.
    fn spawn_dispatcher(&self) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(&self.inner);
        let wake = Arc::clone(&self.inner.wake);
        tokio::spawn(async move {
            let mut last_prune = 0_u64;
            // Set while the last retention pass stopped at its limit: the
            // next pass follows after a short pause instead of an interval.
            let mut retention_behind = false;
            // When the next catch-up pass may run. Wakes bring rounds, never
            // an early pass, so commands get the store between passes.
            let mut next_pass_at = tokio::time::Instant::now();
            // When the next round is due. A wake or the round's own timer
            // runs it; retention passes in between do not.
            let mut round_due = 0_u64;
            let mut round_wanted = true;
            loop {
                let pause = {
                    let Some(inner) = weak.upgrade() else {
                        return;
                    };
                    if inner.stopped.load(Ordering::SeqCst) {
                        return;
                    }
                    let service = AnnouncementService { inner };
                    let now = service.now();
                    let pass_due = if retention_behind {
                        tokio::time::Instant::now() >= next_pass_at
                    } else {
                        now.saturating_sub(last_prune) >= PRUNE_INTERVAL_MS
                    };
                    if pass_due {
                        let retention_ms = service.inner.config.retention_ms;
                        // What waited longer than the window for a delivery
                        // that never started expires first, so the prune
                        // after it is never held back by it.
                        let store = &service.inner.store;
                        match store.expire(now, retention_ms, PRUNE_LIMIT).await {
                            Ok(expired) => {
                                match store.prune(now, retention_ms, PRUNE_LIMIT).await {
                                    Ok(report) => retention_behind = expired.more || report.more,
                                    Err(error) => {
                                        tracing::warn!(%error, "announcement retention pass failed");
                                        retention_behind = false;
                                    }
                                }
                            }
                            Err(error) => {
                                tracing::warn!(%error, "announcement expiry pass failed");
                                retention_behind = false;
                            }
                        }
                        if retention_behind {
                            next_pass_at = tokio::time::Instant::now() + CATCH_UP_PAUSE;
                        } else {
                            last_prune = now;
                        }
                    }
                    let now = service.now();
                    // A round is never further off than the longest idle: one
                    // that is was scheduled before the clock stepped back.
                    if round_wanted
                        || now >= round_due
                        || round_due.saturating_sub(now) > MAX_IDLE_MS
                    {
                        let latest = now.saturating_add(MAX_IDLE_MS);
                        round_due = match service.run_round().await {
                            Ok(report) => report.next_due_ms.map_or(latest, |due| due.min(latest)),
                            Err(error) => {
                                tracing::warn!(%error, "announcement dispatcher round failed");
                                now.saturating_add(service.inner.config.settle_check_max_ms)
                            }
                        };
                    }
                    let round_pause = Duration::from_millis(
                        round_due.saturating_sub(service.now()).min(MAX_IDLE_MS),
                    );
                    // A retention backlog continues after a short pause, not
                    // an interval.
                    if retention_behind {
                        round_pause.min(
                            next_pass_at.saturating_duration_since(tokio::time::Instant::now()),
                        )
                    } else {
                        round_pause
                    }
                };
                round_wanted = tokio::select! {
                    biased;
                    () = wake.notified() => true,
                    // The round's own timer, or the pause between retention
                    // passes.
                    () = tokio::time::sleep(pause) => !retention_behind,
                };
            }
        })
    }

    /// Which subscriptions still want this batch's announcements: delivery
    /// needs one active subscription; paused ones hold the batch until they
    /// resume; when every one is cancelled the batch is no longer wanted.
    async fn interest(&self, batch: &Batch) -> Result<Interest, AnnouncementError> {
        let subscriptions = batch
            .items
            .iter()
            .map(|item| item.subscription_id.clone())
            .collect::<BTreeSet<_>>();
        let mut interest = Interest::None;
        for id in subscriptions {
            match self
                .inner
                .store
                .subscription(&id)
                .await?
                .map(|subscription| subscription.state)
            {
                Some(SubscriptionState::Active) => return Ok(Interest::Active),
                Some(SubscriptionState::Paused) => interest = Interest::Paused,
                Some(SubscriptionState::Cancelled | SubscriptionState::Ended) | None => {}
            }
        }
        Ok(interest)
    }

    async fn successor_for(
        &self,
        batch: &Batch,
    ) -> Result<Option<SuccessorBatch>, AnnouncementError> {
        if self.interest(batch).await? == Interest::None {
            return Ok(None);
        }
        let id = new_batch_id()?;
        let mut seen = BTreeSet::new();
        let mut items = Vec::new();
        let mut not_retained = Vec::new();
        for item in &batch.items {
            if !seen.insert((item.mob_id.clone(), item.sequence)) {
                continue;
            }
            // Retention may have removed an announcement while the native
            // owner held this batch: the successor reports it as a gap.
            match self
                .inner
                .store
                .announcement(&item.mob_id, &item.source_key)
                .await?
            {
                Some(announcement) => items.push(render_item(&announcement)),
                None => not_retained.push(BatchGap {
                    mob_id: item.mob_id.clone(),
                    count: 1,
                    reason: GapReason::NotRetained,
                }),
            }
        }
        Ok(Some(successor_batch(
            id,
            &batch.id,
            merge_gaps(&batch.gaps, &not_retained),
            &items,
            wire_budget(batch.route),
        )))
    }

    /// For a member kept for a mob a member created: end its subscriptions
    /// made before that mob ended (destroyed, created again, completed or
    /// reset), read from the mob's own durable records; otherwise record how
    /// far its event log was read, so the next check reads on from there.
    /// Returns whether any subscription ended.
    async fn member_mob_ended(&self, member: &Member, now: u64) -> Result<bool, AnnouncementError> {
        let Some(kept) = self.inner.store.member_route(member).await? else {
            return Ok(false);
        };
        if kept.route != MemberRoute::Child {
            return Ok(false);
        }
        let Some(from) = self.inner.store.member_mob_check_from(member).await? else {
            return Ok(false);
        };
        let scan = self
            .inner
            .native
            .member_mob_end(&member.mob_id, kept.incarnation.as_ref(), from)
            .await
            .map_err(|error| AnnouncementError::NativeUnavailable(error.0))?;
        match scan {
            MemberMobScan::Running { through } => {
                if through > from {
                    self.inner
                        .store
                        .record_member_mob_check(member, through)
                        .await?;
                }
                Ok(false)
            }
            MemberMobScan::Ended(end) => {
                let ended = self
                    .inner
                    .store
                    .end_member_before(
                        member,
                        end.cursor,
                        SubscriptionEnd::MemberMobEnded {
                            why: end.why.clone(),
                        },
                        now,
                    )
                    .await?;
                if !ended.is_empty() {
                    tracing::info!(
                        member = %member,
                        why = %end.why,
                        subscriptions = ended.len(),
                        "member's mob ended; its subscriptions ended there"
                    );
                }
                Ok(!ended.is_empty())
            }
        }
    }

    /// Whether the mob a member was kept for is destroyed: not hosted here
    /// once every mob is restored.
    async fn member_mob_destroyed(&self, member: &Member) -> Result<bool, AnnouncementError> {
        let named = BTreeSet::from([member.mob_id.clone()]);
        let presence = self
            .inner
            .native
            .mob_presence(&named)
            .await
            .map_err(|error| AnnouncementError::NativeUnavailable(error.0))?;
        Ok(presence.get(&member.mob_id) == Some(&MobPresence::Gone))
    }

    /// Whether a batch the native owner reads as not committed expires:
    /// once it was sealed before the retention window, every item it holds
    /// is at least that old. For a member on a peer gateway its last
    /// submission must also be older than the peer's request window, so a
    /// request still in flight is refused as stale instead of landing after
    /// the expiry; until then nothing is submitted again.
    fn expiry(&self, batch: &Batch, now: u64) -> Expiry {
        let config = &self.inner.config;
        if now.saturating_sub(batch.sealed_at_ms) < config.retention_ms {
            return Expiry::NotYet;
        }
        // A peer accepts a request while its issue time is within the
        // window of its own clock: the wait covers this gateway's window and
        // the slowest settle pause past it, for a request issued late in a
        // round or read by a clock a little behind.
        let quiet = config
            .peer_request_window_ms
            .saturating_add(config.settle_check_max_ms);
        if batch.route != MemberRoute::Peer || now.saturating_sub(batch.updated_at_ms) > quiet {
            return Expiry::Now;
        }
        Expiry::AfterQuietWindow(batch.updated_at_ms.saturating_add(quiet).saturating_add(1))
    }

    fn settle_pause_ms(&self, batch: &Batch, now: u64) -> u64 {
        let age = now.saturating_sub(batch.sealed_at_ms);
        (age / 10).clamp(
            self.inner.config.settle_check_min_ms,
            self.inner.config.settle_check_max_ms,
        )
    }

    async fn apply(
        &self,
        batch: &Batch,
        event: BatchEvent,
        now: u64,
    ) -> Result<Batch, AnnouncementError> {
        Ok(self
            .inner
            .store
            .transition_batch(&batch.id, batch.state.tag(), event, now)
            .await?)
    }

    /// Advance one batch as far as native evidence allows now. Returns when
    /// it should be looked at again, or `None` once it is settled.
    async fn drive(&self, mut batch: Batch, now: u64) -> Result<Option<u64>, AnnouncementError> {
        // A route that contradicts its member blocks the batch before
        // anything is prepared, ended, submitted or read for it: no member
        // of this gateway's mob is touched in that member's place.
        if BatchStateTag::UNSETTLED.contains(&batch.state.tag())
            && let Some(refusal) = self.route_refusal(&batch)
        {
            self.apply(
                &batch,
                BatchEvent::Block {
                    cause: refusal.to_string(),
                },
                now,
            )
            .await?;
            return Ok(None);
        }
        let mut submitted_this_round = false;
        for _ in 0..8 {
            let retry_at = Some(now.saturating_add(self.settle_pause_ms(&batch, now)));
            match batch.state.clone() {
                BatchState::Sealed => {
                    match self.interest(&batch).await? {
                        Interest::None => {
                            batch = self.apply(&batch, BatchEvent::Cancel, now).await?;
                            continue;
                        }
                        // A resume wakes the dispatcher; no timer needed.
                        Interest::Paused => return Ok(None),
                        Interest::Active => {}
                    }
                    if batch.route == MemberRoute::Child
                        && self.member_mob_ended(&batch.member, now).await?
                    {
                        batch = self.reload(&batch).await?;
                        continue;
                    }
                    match self
                        .inner
                        .native
                        .prepare_member(&batch.member, batch.route)
                        .await
                    {
                        Ok(()) => {}
                        // Nothing native exists yet: the member's
                        // subscriptions end and this batch is cancelled.
                        Err(PrepareError::Gone(end)) => {
                            self.end_member(&batch.member, end, now).await;
                            batch = self.reload(&batch).await?;
                            continue;
                        }
                        Err(PrepareError::Unavailable(error)) => {
                            tracing::warn!(
                                batch = %batch.id,
                                member = %batch.member,
                                error = %error,
                                "announcement member is not resolvable yet; the batch stays sealed"
                            );
                            return Ok(retry_at);
                        }
                    }
                    // Stamped when the request is issued, not when the
                    // round began: a peer's window runs from the issue.
                    batch = self
                        .apply(&batch, BatchEvent::BeginSubmit, self.now())
                        .await?;
                    super::faults::after_begin_submit();
                    submitted_this_round = true;
                    match self.submit(&batch, now).await? {
                        Some(next) => batch = next,
                        None => return Ok(retry_at),
                    }
                }
                BatchState::Submitting { .. } => {
                    let Some(status) = self.status(&batch).await else {
                        return Ok(retry_at);
                    };
                    match status {
                        NativeStatus::NotCommitted => {
                            let interest = self.interest(&batch).await?;
                            if interest == Interest::None {
                                batch = self
                                    .apply(&batch, BatchEvent::CancelNotCommitted, now)
                                    .await?;
                                continue;
                            }
                            // Nothing native holds it and it waited longer than
                            // the retention window: its announcements expire,
                            // paused or not, and the next batch reports them.
                            match self.expiry(&batch, now) {
                                Expiry::NotYet => {}
                                Expiry::Now => {
                                    batch = self
                                        .apply(&batch, BatchEvent::ExpireNotCommitted, now)
                                        .await?;
                                    continue;
                                }
                                // A peer's request may still be in flight: no
                                // further attempt, and the expiry waits until
                                // the peer would refuse it as stale.
                                Expiry::AfterQuietWindow(due) => return Ok(Some(due)),
                            }
                            if interest == Interest::Paused {
                                return Ok(None);
                            }
                            if submitted_this_round {
                                return Ok(retry_at);
                            }
                            if batch.route == MemberRoute::Child
                                && self.member_mob_ended(&batch.member, now).await?
                            {
                                batch = self.reload(&batch).await?;
                                continue;
                            }
                            match self
                                .inner
                                .native
                                .prepare_member(&batch.member, batch.route)
                                .await
                            {
                                Ok(()) => {}
                                // Read "not committed": the member's
                                // subscriptions end and the batch is
                                // cancelled.
                                Err(PrepareError::Gone(end)) => {
                                    self.end_member(&batch.member, end, now).await;
                                    batch = self.reload(&batch).await?;
                                    continue;
                                }
                                Err(PrepareError::Unavailable(error)) => {
                                    tracing::warn!(
                                        batch = %batch.id,
                                        error = %error,
                                        "announcement member is not resolvable yet"
                                    );
                                    return Ok(retry_at);
                                }
                            }
                            batch = self
                                .apply(&batch, BatchEvent::RetryNotCommitted, self.now())
                                .await?;
                            submitted_this_round = true;
                            match self.submit(&batch, now).await? {
                                Some(next) => batch = next,
                                None => return Ok(retry_at),
                            }
                        }
                        status => batch = self.settle_from(&batch, status, now).await?,
                    }
                }
                BatchState::Committed { .. } => {
                    let Some(status) = self.status(&batch).await else {
                        return Ok(retry_at);
                    };
                    match status {
                        // Evidence about another delivery never settles this
                        // one, nor leaves it waiting on that delivery.
                        NativeStatus::Pending { receipt, .. } => {
                            match batch.state.other_delivery(&receipt) {
                                Some(cause) => {
                                    batch = self
                                        .apply(&batch, BatchEvent::Block { cause }, now)
                                        .await?;
                                }
                                None => {
                                    // Meerkat keeps a delivery to a member of a mob it
                                    // does not host waiting, in case that mob is
                                    // restored. A destroyed mob a member created never
                                    // is: the delivery strands, after the member's
                                    // subscriptions there end.
                                    if batch.route != MemberRoute::Child
                                        || !self.member_mob_destroyed(&batch.member).await?
                                    {
                                        return Ok(retry_at);
                                    }
                                    self.inner
                                        .store
                                        .end_member(
                                            &batch.member,
                                            SubscriptionEnd::MemberMobEnded {
                                                why: "destroyed".to_string(),
                                            },
                                            now,
                                        )
                                        .await?;
                                    let successor = self.successor_for(&batch).await?;
                                    batch = self
                                        .apply(
                                            &batch,
                                            BatchEvent::NativeStranded { receipt, successor },
                                            now,
                                        )
                                        .await?;
                                }
                            }
                        }
                        NativeStatus::NotCommitted => {
                            batch = self
                                .apply(
                                    &batch,
                                    BatchEvent::Block {
                                        cause: "the native owner holds no commit for a batch \
                                                it issued a receipt for"
                                            .into(),
                                    },
                                    now,
                                )
                                .await?;
                        }
                        status => batch = self.settle_from(&batch, status, now).await?,
                    }
                }
                BatchState::Stranded { .. } => match batch.successor.clone() {
                    Some(successor) => {
                        let Some(next) = self.inner.store.batch(&successor).await? else {
                            return Ok(None);
                        };
                        batch = next;
                    }
                    None => return Ok(None),
                },
                BatchState::Delivered { .. }
                | BatchState::Refused { .. }
                | BatchState::Cancelled
                | BatchState::Expired
                | BatchState::Blocked { .. } => return Ok(None),
            }
        }
        Ok(Some(
            now.saturating_add(self.inner.config.settle_check_min_ms),
        ))
    }

    /// Whether the batch's kept route contradicts its member, by the one
    /// guard the native seam runs too.
    fn route_refusal(&self, batch: &Batch) -> Option<OwnerRefusal> {
        let own = self.inner.native.own_mob()?;
        route_contradiction(&own, &batch.member, batch.route)
    }

    /// The batch as the store holds it now.
    async fn reload(&self, batch: &Batch) -> Result<Batch, AnnouncementError> {
        Ok(self
            .inner
            .store
            .batch(&batch.id)
            .await?
            .ok_or(AnnouncementStoreError::UnknownBatch)?)
    }

    async fn status(&self, batch: &Batch) -> Option<NativeStatus> {
        match self.inner.native.status(batch).await {
            Ok(status) => Some(status),
            Err(error) => {
                tracing::warn!(batch = %batch.id, error = %error, "announcement native status unavailable");
                None
            }
        }
    }

    /// Settle or advance a submitting or committed batch from a native
    /// status other than "not committed".
    async fn settle_from(
        &self,
        batch: &Batch,
        status: NativeStatus,
        now: u64,
    ) -> Result<Batch, AnnouncementError> {
        let event = match status {
            NativeStatus::NotCommitted => return Ok(batch.clone()),
            NativeStatus::Pending { receipt, .. } => {
                if batch.state.tag() == BatchStateTag::Committed {
                    return Ok(batch.clone());
                }
                BatchEvent::NativeCommitted { receipt }
            }
            NativeStatus::Applied {
                receipt,
                session_id,
                input_id,
            } => BatchEvent::NativeApplied {
                receipt,
                session_id,
                input_id,
            },
            NativeStatus::Refused { receipt, reason } => BatchEvent::NativeRefused {
                reason,
                receipt: Some(receipt),
            },
            NativeStatus::Stranded { receipt } => BatchEvent::NativeStranded {
                receipt,
                successor: self.successor_for(batch).await?,
            },
        };
        self.apply(batch, event, now).await
    }

    /// Submit the batch's exact continuation. Returns the advanced batch, or
    /// `None` when the outcome is unknown or nothing was committed and a
    /// later round must read the native status first.
    async fn submit(&self, batch: &Batch, now: u64) -> Result<Option<Batch>, AnnouncementError> {
        let event = match self.inner.native.submit(batch, now).await {
            Ok(receipt) => BatchEvent::NativeCommitted { receipt },
            Err(NativeSubmitError::Unavailable(detail)) => {
                tracing::warn!(
                    batch = %batch.id,
                    detail = %detail,
                    "announcement submission outcome unknown; the native status will settle it"
                );
                return Ok(None);
            }
            Err(NativeSubmitError::OwnerRetired) => {
                tracing::warn!(
                    batch = %batch.id,
                    member = %batch.member,
                    "announcement member has no live incarnation; nothing was committed"
                );
                return Ok(None);
            }
            Err(NativeSubmitError::Governed) => BatchEvent::NativeRefused {
                reason: "governed_runtime".into(),
                receipt: None,
            },
            // A final answer that nothing was committed: the batch settles
            // as refused and the member's subscriptions end.
            Err(NativeSubmitError::MemberGone(end)) => {
                let refused = self
                    .apply(
                        batch,
                        BatchEvent::NativeRefused {
                            reason: end.to_string(),
                            receipt: None,
                        },
                        now,
                    )
                    .await?;
                self.end_member(&batch.member, end, now).await;
                return Ok(Some(refused));
            }
            Err(NativeSubmitError::Conflict(cause) | NativeSubmitError::Invalid(cause)) => {
                BatchEvent::Block { cause }
            }
        };
        self.apply(batch, event, now).await.map(Some)
    }
}
