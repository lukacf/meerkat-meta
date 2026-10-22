//! Announcement sources: the durable record of one feed a collector reads
//! into a mob.
//!
//! A source is a mob announcement source, not a concept of its own. What it
//! carries are ordinary announcements under their own announcer kind
//! ([`Announcer::Source`](super::types::Announcer::Source)). The store keeps
//! one record per `(mob_id, source_id)`, in generations: its identity (the
//! provider, the principal whose account it reads and the scope), the topics
//! it owns, who may receive its items, how they route, how its adapter
//! observes, its progress and its health.
//!
//! A source owns its topics and its triage routes' forward topics in its mob,
//! and keeps them reserved to its source id after it ends: no other source
//! and no plain announcement ever reaches them.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::types::{
    AnnouncementStoreError, MAX_ID_BYTES, Member, MobId, Result, SourceId, Topic, sha256_hex,
};

/// Most topics a source owns for its items.
pub const MAX_SOURCE_TOPICS: usize = 16;
/// Most forward topics one triage route owns.
pub const MAX_FORWARD_TOPICS: usize = 16;
/// Most routes a source declares.
pub const MAX_ROUTES: usize = 16;
/// Most grants an audience names.
pub const MAX_AUDIENCE_GRANTS: usize = 64;
/// Most members a source names as owning a live conversation on its scope.
pub const MAX_CONVERSATION_MEMBERS: usize = 64;
/// Most items a source's first collection may backfill, across its pages.
pub const MAX_BACKFILL_ITEMS: u32 = 1_000;
/// The largest per-source cap on one batch.
pub const MAX_PER_BATCH: u32 = 1_000;
/// The per-source cap on one batch when a source names none.
pub const DEFAULT_MAX_PER_BATCH: u32 = 20;
/// Longest provider name, in bytes.
pub const MAX_PROVIDER_BYTES: usize = 128;
/// Longest canonical principal, in bytes.
pub const MAX_PRINCIPAL_BYTES: usize = 1_024;
/// Most live sources one mob holds.
pub const MAX_SOURCES_PER_MOB: u32 = 256;
/// Most live sources one gateway holds.
pub const MAX_SOURCES_PER_GATEWAY: u32 = 1_024;

const MINUTE_MS: u64 = 60 * 1_000;
const HOUR_MS: u64 = 60 * MINUTE_MS;
const DAY_MS: u64 = 24 * HOUR_MS;
/// The longest window a health policy names.
const MAX_HEALTH_WINDOW_MS: u64 = 30 * DAY_MS;

/// A bounded, control-free text field of a source declaration.
fn text(value: &str, max: usize, what: &str) -> Result<()> {
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        return Err(AnnouncementStoreError::Invalid(format!(
            "{what} must be 1..={max} bytes without control characters"
        )));
    }
    Ok(())
}

/// Whose account a source reads. Meta keeps it as given and compares it by
/// its canonical encoding; it never branches on its fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum Principal {
    /// The exact verified tuple of a connector login: the account the
    /// provider proved. A credential slot or a label is never one.
    ConnectorVerifiedAccount {
        issuer: String,
        strategy_id: String,
        subject: String,
    },
    /// The host vouching for an account it reads with its own connection.
    /// Accepted only with a host connection, and never taken for a verified
    /// account.
    HostAttested { attester: String, account: String },
}

impl Principal {
    /// The kind, as the wire spells it.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ConnectorVerifiedAccount { .. } => "connector_verified_account",
            Self::HostAttested { .. } => "host_attested",
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::ConnectorVerifiedAccount {
                issuer,
                strategy_id,
                subject,
            } => {
                text(issuer, MAX_ID_BYTES, "a principal issuer")?;
                text(strategy_id, MAX_ID_BYTES, "a principal strategy id")?;
                text(subject, MAX_ID_BYTES, "a principal subject")?;
            }
            Self::HostAttested { attester, account } => {
                text(attester, MAX_ID_BYTES, "a principal attester")?;
                text(account, MAX_ID_BYTES, "a principal account")?;
            }
        }
        if self.canonical()?.len() > MAX_PRINCIPAL_BYTES {
            return Err(AnnouncementStoreError::TooLarge(format!(
                "a principal is at most {MAX_PRINCIPAL_BYTES} bytes in canonical form"
            )));
        }
        Ok(())
    }

    /// The canonical encoding: Meta's own serialization of the typed value,
    /// fields in their declared order.
    pub fn canonical(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|error| AnnouncementStoreError::Invalid(error.to_string()))
    }

    /// The digest identity uniqueness and source keys use.
    pub fn digest(&self) -> Result<String> {
        Ok(sha256_hex(&[
            b"mob-announcement-principal-v1",
            self.canonical()?.as_bytes(),
        ]))
    }
}

/// A provider scope: what of the principal's account the source reads.
/// Opaque to Meta; a tenant or resource is part of it when the provider's
/// item ids are local to one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceScope {
    pub kind: String,
    pub id: String,
}

/// A source's identity: fixed within a generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceIdentity {
    pub provider: String,
    pub principal: Principal,
    pub scope: SourceScope,
}

impl SourceIdentity {
    pub fn validate(&self) -> Result<()> {
        text(&self.provider, MAX_PROVIDER_BYTES, "a provider")?;
        self.principal.validate()?;
        text(&self.scope.kind, MAX_ID_BYTES, "a scope kind")?;
        text(&self.scope.id, MAX_ID_BYTES, "a scope id")
    }

    /// The key one live source per mob is kept on: the provider, the
    /// principal's digest and the scope.
    pub fn key(&self) -> Result<String> {
        Ok(sha256_hex(&[
            b"mob-announcement-source-identity-v1",
            self.provider.as_bytes(),
            self.principal.digest()?.as_bytes(),
            self.scope.kind.as_bytes(),
            self.scope.id.as_bytes(),
        ]))
    }
}

/// What a source's connection reference names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ConnectionKind {
    /// A Connect integration setup record.
    ConnectSetup,
    /// A connection the host owns.
    Host,
}

/// A reference to the connection a source's collector reads through. Meta
/// keeps it and never dereferences it; it is not the principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConnection {
    pub kind: ConnectionKind,
    pub id: String,
}

/// How a source's adapter takes a baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BaselineCapability {
    /// A provider checkpoint taken without replaying history.
    Delta,
    /// Bounded pages that emit no history.
    Snapshot,
}

/// Whether a source's provider reports deletions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeletionCapability {
    /// Deletions arrive as explicit tombstones.
    Explicit,
    /// The provider has no observable deletions; the source emits none.
    Unobservable,
}

/// How a source's adapter observes. Every source declares it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationCapability {
    pub baseline: BaselineCapability,
    pub deletions: DeletionCapability,
}

/// One grant of an audience, over the facts a mob's roster records. Exact
/// values only; a grant never matches another mob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum AudienceGrant {
    /// One member.
    Member { member: Member },
    /// Every member of the mob spawned from this profile.
    Role { mob_id: MobId, role: String },
    /// Every member of the mob carrying this label.
    Label {
        mob_id: MobId,
        key: String,
        value: String,
    },
}

impl AudienceGrant {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Member { .. } => Ok(()),
            Self::Role { role, .. } => text(role, MAX_ID_BYTES, "a role"),
            Self::Label { key, value, .. } => {
                text(key, MAX_ID_BYTES, "a label key")?;
                text(value, MAX_ID_BYTES, "a label value")
            }
        }
    }
}

fn validate_audience(audience: &[AudienceGrant], what: &str) -> Result<()> {
    if audience.is_empty() || audience.len() > MAX_AUDIENCE_GRANTS {
        return Err(AnnouncementStoreError::Invalid(format!(
            "{what} names 1..={MAX_AUDIENCE_GRANTS} grants"
        )));
    }
    audience.iter().try_for_each(AudienceGrant::validate)
}

/// How a source's items reach members.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum SourceRoute {
    /// Members within the audience subscribe to these topics of the source
    /// directly (all of them when absent).
    Direct {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        topics: Option<BTreeSet<Topic>>,
        /// Selecting by stream is not supported in this release; a route
        /// that names streams is refused typed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        streams: Option<Vec<String>>,
    },
    /// The triage member takes these topics of the source (all of them
    /// when absent) and forwards on its forward topics, which the source
    /// owns, to the forward audience (the source's audience when absent).
    Triage {
        member: Member,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        topics: Option<BTreeSet<Topic>>,
        forward_topics: BTreeSet<Topic>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        forward_audience: Option<Vec<AudienceGrant>>,
        /// As for a direct route: refused typed when named.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        streams: Option<Vec<String>>,
    },
}

/// Where a new generation of a source starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum SourceStart {
    /// Take the provider's position and announce no history.
    #[default]
    Baseline,
    /// Announce at most this many items of history across the first
    /// collection's pages, then go live.
    Backfill { max_items: u32 },
}

/// The order a member's batches take a source's items in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SourcePriority {
    Low,
    #[default]
    Normal,
    Urgent,
}

/// What happens to items whose admitted turn ended without completing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum UnhandledPolicy {
    /// Owed again, with notice, up to the redelivery bound.
    Redeliver,
    /// Held for an operator.
    Hold,
    /// Recorded and counted only.
    Report,
}

fn default_max_per_batch() -> Option<u32> {
    Some(DEFAULT_MAX_PER_BATCH)
}

fn default_unhandled() -> UnhandledPolicy {
    UnhandledPolicy::Redeliver
}

/// How a source's items are delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceDelivery {
    #[serde(default)]
    pub priority: SourcePriority,
    /// The most items of this source one batch carries; `null` for no cap.
    #[serde(default = "default_max_per_batch")]
    pub max_per_batch: Option<u32>,
    #[serde(default = "default_unhandled")]
    pub on_unhandled: UnhandledPolicy,
}

impl Default for SourceDelivery {
    fn default() -> Self {
        Self {
            priority: SourcePriority::default(),
            max_per_batch: default_max_per_batch(),
            on_unhandled: default_unhandled(),
        }
    }
}

fn default_escalate_after_failures() -> u32 {
    3
}

fn default_escalate_after_ms() -> u64 {
    6 * HOUR_MS
}

fn default_stale_after_ms() -> u64 {
    DAY_MS
}

/// When a source's health escalates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceHealthPolicy {
    /// Consecutive failed collections before an escalation.
    #[serde(default = "default_escalate_after_failures")]
    pub escalate_after_failures: u32,
    /// Time without a successful collection before an escalation.
    #[serde(default = "default_escalate_after_ms")]
    pub escalate_after_ms: u64,
    /// Time without any advance before the source counts as stale.
    #[serde(default = "default_stale_after_ms")]
    pub stale_after_ms: u64,
    /// Time a live source may keep succeeding without carrying a single item
    /// before it escalates as quiet; `null` (the default) never does.
    #[serde(default)]
    pub quiet_after_ms: Option<u64>,
    /// Time a source's snapshots may keep returning zero records before it
    /// escalates its coverage as empty; `null` (the default) never does. A
    /// delta source never raises it.
    #[serde(default)]
    pub empty_after_ms: Option<u64>,
}

impl Default for SourceHealthPolicy {
    fn default() -> Self {
        Self {
            escalate_after_failures: default_escalate_after_failures(),
            escalate_after_ms: default_escalate_after_ms(),
            stale_after_ms: default_stale_after_ms(),
            quiet_after_ms: None,
            empty_after_ms: None,
        }
    }
}

impl SourceHealthPolicy {
    pub fn validate(&self) -> Result<()> {
        if !(1..=1_000).contains(&self.escalate_after_failures) {
            return Err(AnnouncementStoreError::Invalid(
                "health_policy.escalate_after_failures must be 1..=1000".to_string(),
            ));
        }
        let windows = [
            ("escalate_after_ms", Some(self.escalate_after_ms)),
            ("stale_after_ms", Some(self.stale_after_ms)),
            ("quiet_after_ms", self.quiet_after_ms),
            ("empty_after_ms", self.empty_after_ms),
        ];
        for (name, window) in windows {
            if window.is_some_and(|ms| !(MINUTE_MS..=MAX_HEALTH_WINDOW_MS).contains(&ms)) {
                return Err(AnnouncementStoreError::Invalid(format!(
                    "health_policy.{name} must be one minute to 30 days"
                )));
            }
        }
        Ok(())
    }
}

/// What runs a source's collector: the host runnable whose code the host
/// owns, and how often it runs. For a declared source Meta schedules it
/// through Core's schedule service; Meta runs none of its code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRunner {
    pub host_runnable: String,
    pub every_seconds: u64,
}

/// The shortest and longest cadence a runner takes.
const MIN_RUNNER_SECONDS: u64 = 60;
const MAX_RUNNER_SECONDS: u64 = 30 * 24 * 60 * 60;

/// Everything a declaration of a source says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub identity: SourceIdentity,
    #[serde(default)]
    pub connection: Option<SourceConnection>,
    pub observation: ObservationCapability,
    pub topics: BTreeSet<Topic>,
    pub audience: Vec<AudienceGrant>,
    pub routes: Vec<SourceRoute>,
    #[serde(default)]
    pub conversation_members: Vec<Member>,
    #[serde(default)]
    pub start: SourceStart,
    #[serde(default)]
    pub delivery: SourceDelivery,
    #[serde(default)]
    pub health_policy: SourceHealthPolicy,
    #[serde(default)]
    pub runner: Option<SourceRunner>,
}

/// The kind of a topic a source owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TopicOwnership {
    /// One of the source's own topics: only its items reach it.
    Source,
    /// A triage route's forward topic: only that route's member announces
    /// on it.
    Forward,
}

impl TopicOwnership {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Forward => "forward",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "source" => Ok(Self::Source),
            "forward" => Ok(Self::Forward),
            other => Err(AnnouncementStoreError::Storage(format!(
                "unknown topic ownership {other}"
            ))),
        }
    }
}

impl SourceConfig {
    /// Refuse a declaration no store keeps: bounds, a principal its
    /// connection cannot carry, routes over topics the source does not own,
    /// and route selectors this release does not support.
    pub fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        if let Some(connection) = &self.connection {
            text(&connection.id, MAX_ID_BYTES, "a connection id")?;
        }
        if matches!(self.identity.principal, Principal::HostAttested { .. })
            && !matches!(
                self.connection,
                Some(SourceConnection {
                    kind: ConnectionKind::Host,
                    ..
                })
            )
        {
            return Err(AnnouncementStoreError::PrincipalNotAllowed {
                kind: self.identity.principal.kind().to_string(),
            });
        }
        if self.topics.is_empty() || self.topics.len() > MAX_SOURCE_TOPICS {
            return Err(AnnouncementStoreError::Invalid(format!(
                "a source owns 1..={MAX_SOURCE_TOPICS} topics"
            )));
        }
        validate_audience(&self.audience, "an audience")?;
        if self.routes.is_empty() || self.routes.len() > MAX_ROUTES {
            return Err(AnnouncementStoreError::Invalid(format!(
                "a source declares 1..={MAX_ROUTES} routes"
            )));
        }
        let mut forwards = BTreeSet::new();
        for route in &self.routes {
            let (topics, streams) = match route {
                SourceRoute::Direct { topics, streams } => (topics, streams),
                SourceRoute::Triage {
                    topics,
                    forward_topics,
                    forward_audience,
                    streams,
                    ..
                } => {
                    if forward_topics.is_empty() || forward_topics.len() > MAX_FORWARD_TOPICS {
                        return Err(AnnouncementStoreError::Invalid(format!(
                            "a triage route forwards on 1..={MAX_FORWARD_TOPICS} topics"
                        )));
                    }
                    for topic in forward_topics {
                        if self.topics.contains(topic) {
                            return Err(AnnouncementStoreError::Invalid(format!(
                                "forward topic {topic} is one of the source's own topics"
                            )));
                        }
                        if !forwards.insert(topic.clone()) {
                            return Err(AnnouncementStoreError::Invalid(format!(
                                "forward topic {topic} is named by more than one route"
                            )));
                        }
                    }
                    if let Some(audience) = forward_audience {
                        validate_audience(audience, "a forward audience")?;
                    }
                    (topics, streams)
                }
            };
            if streams.is_some() {
                return Err(AnnouncementStoreError::RouteSelectorUnsupported {
                    selector: "streams".to_string(),
                });
            }
            if let Some(topics) = topics
                && (topics.is_empty() || !topics.is_subset(&self.topics))
            {
                return Err(AnnouncementStoreError::Invalid(
                    "a route names 1 or more of its source's own topics".to_string(),
                ));
            }
        }
        if self.conversation_members.len() > MAX_CONVERSATION_MEMBERS {
            return Err(AnnouncementStoreError::Invalid(format!(
                "a source names at most {MAX_CONVERSATION_MEMBERS} conversation members"
            )));
        }
        if let SourceStart::Backfill { max_items } = self.start
            && !(1..=MAX_BACKFILL_ITEMS).contains(&max_items)
        {
            return Err(AnnouncementStoreError::Invalid(format!(
                "a backfill start takes 1..={MAX_BACKFILL_ITEMS} items"
            )));
        }
        if let Some(cap) = self.delivery.max_per_batch
            && !(1..=MAX_PER_BATCH).contains(&cap)
        {
            return Err(AnnouncementStoreError::Invalid(format!(
                "delivery.max_per_batch must be 1..={MAX_PER_BATCH} or null"
            )));
        }
        self.health_policy.validate()?;
        if let Some(runner) = &self.runner {
            text(
                &runner.host_runnable,
                MAX_ID_BYTES,
                "a runner host runnable",
            )?;
            if !(MIN_RUNNER_SECONDS..=MAX_RUNNER_SECONDS).contains(&runner.every_seconds) {
                return Err(AnnouncementStoreError::Invalid(
                    "runner.every_seconds must be one minute to 30 days".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Every topic the source owns, with its kind.
    pub fn owned_topics(&self) -> Vec<(Topic, TopicOwnership)> {
        let own = self
            .topics
            .iter()
            .map(|topic| (topic.clone(), TopicOwnership::Source));
        let forwards = self.routes.iter().flat_map(|route| match route {
            SourceRoute::Triage { forward_topics, .. } => forward_topics
                .iter()
                .map(|topic| (topic.clone(), TopicOwnership::Forward))
                .collect::<Vec<_>>(),
            SourceRoute::Direct { .. } => Vec::new(),
        });
        own.chain(forwards).collect()
    }

    /// The triage member whose route forwards on `topic`, if one does.
    pub fn forwarding_member(&self, topic: &Topic) -> Option<&Member> {
        self.routes.iter().find_map(|route| match route {
            SourceRoute::Triage {
                member,
                forward_topics,
                ..
            } if forward_topics.contains(topic) => Some(member),
            _ => None,
        })
    }
}

/// A source's operator state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SourceState {
    /// Collects.
    Active,
    /// Takes no advance until resumed; keeps its cursor.
    Paused,
    /// Its generation is over. A declaration starts the next one.
    Ended,
}

impl SourceState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Ended => "ended",
        }
    }
}

/// Where a source's collection stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CollectionPhase {
    /// Taking the provider's position: no items until the final page.
    Baselining,
    /// Announcing a bounded history until the final page.
    Backfilling,
    /// Collecting changes.
    Live,
}

impl CollectionPhase {
    fn at_start(start: SourceStart) -> Self {
        match start {
            SourceStart::Baseline => Self::Baselining,
            SourceStart::Backfill { .. } => Self::Backfilling,
        }
    }
}

/// A source's progress: an opaque provider position and the version every
/// advance compares and sets.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SourceCursor {
    /// `0` until the first advance of the generation commits.
    pub version: u64,
    /// The adapter's opaque token, stored as given and never parsed.
    pub token: Option<String>,
    pub advanced_at_ms: Option<u64>,
}

/// How many of a source's streams are live and how many are onboarding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SourceStreamCounts {
    pub live: u32,
    pub onboarding: u32,
}

/// A source's health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SourceHealthStatus {
    Baselining,
    Backfilling,
    Ok,
    Failing,
    Stale,
    Paused,
    Ended,
}

/// What an advance observed, as its adapter reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum Observed {
    /// The provider answered a delta request from the stored cursor: a page
    /// without items is a real "no change".
    Delta,
    /// A full or windowed listing returned this many records, changed or
    /// not. Zero is suspicious.
    Snapshot { records: u64 },
}

/// What a source's collections have done.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SourceTotals {
    pub announced: u64,
    pub replayed: u64,
    pub conflicts: u64,
    pub own_writes: u64,
    pub refusals: u64,
    pub gaps: u64,
}

/// A source's health.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SourceHealth {
    pub status: SourceHealthStatus,
    pub last_success_at_ms: Option<u64>,
    /// When an advance last carried an item new to the source (outcome
    /// "announced", or an own write): the clock a quiet source is measured
    /// by. Replays and conflicts never move it, so a collector replaying
    /// the same page still goes quiet.
    pub last_new_item_at_ms: Option<u64>,
    /// What the last advance that said so observed, and when.
    pub last_observed: Option<Observed>,
    pub last_observed_at_ms: Option<u64>,
    /// When the current spell of zero-record snapshots began; `None` outside
    /// one.
    pub empty_snapshot_since_ms: Option<u64>,
    pub consecutive_failures: u32,
    pub totals: SourceTotals,
}

impl SourceHealth {
    fn fresh(status: SourceHealthStatus) -> Self {
        Self {
            status,
            last_success_at_ms: None,
            last_new_item_at_ms: None,
            last_observed: None,
            last_observed_at_ms: None,
            empty_snapshot_since_ms: None,
            consecutive_failures: 0,
            totals: SourceTotals::default(),
        }
    }
}

/// A durable announcement source, one generation of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AnnouncementSource {
    pub mob_id: MobId,
    pub source_id: SourceId,
    pub generation: u64,
    /// Rises on every change, across generations: an advance names it.
    pub revision: u64,
    pub identity: SourceIdentity,
    pub connection: Option<SourceConnection>,
    pub observation: ObservationCapability,
    pub topics: BTreeSet<Topic>,
    pub audience: Vec<AudienceGrant>,
    pub routes: Vec<SourceRoute>,
    pub conversation_members: Vec<Member>,
    pub start: SourceStart,
    pub delivery: SourceDelivery,
    pub health_policy: SourceHealthPolicy,
    pub runner: Option<SourceRunner>,
    pub state: SourceState,
    pub collection: CollectionPhase,
    pub cursor: SourceCursor,
    pub streams: SourceStreamCounts,
    pub health: SourceHealth,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl AnnouncementSource {
    /// A new generation at `generation` and `revision`: active, from its
    /// start, with no cursor.
    pub(crate) fn begin(
        mob_id: MobId,
        source_id: SourceId,
        generation: u64,
        revision: u64,
        config: SourceConfig,
        now_ms: u64,
    ) -> Self {
        let collection = CollectionPhase::at_start(config.start);
        let mut source = Self {
            mob_id,
            source_id,
            generation,
            revision,
            identity: config.identity,
            connection: config.connection,
            observation: config.observation,
            topics: config.topics,
            audience: config.audience,
            routes: config.routes,
            conversation_members: config.conversation_members,
            start: config.start,
            delivery: config.delivery,
            health_policy: config.health_policy,
            runner: config.runner,
            state: SourceState::Active,
            collection,
            cursor: SourceCursor::default(),
            streams: SourceStreamCounts::default(),
            health: SourceHealth::fresh(SourceHealthStatus::Baselining),
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        };
        source.refresh_status();
        source
    }

    /// The declaration this generation keeps.
    pub fn config(&self) -> SourceConfig {
        SourceConfig {
            identity: self.identity.clone(),
            connection: self.connection.clone(),
            observation: self.observation,
            topics: self.topics.clone(),
            audience: self.audience.clone(),
            routes: self.routes.clone(),
            conversation_members: self.conversation_members.clone(),
            start: self.start,
            delivery: self.delivery,
            health_policy: self.health_policy,
            runner: self.runner.clone(),
        }
    }

    /// Replace the declaration within the generation; the identity, the
    /// progress and the health stay.
    pub(crate) fn reconfigure(&mut self, config: SourceConfig) {
        self.connection = config.connection;
        self.observation = config.observation;
        self.topics = config.topics;
        self.audience = config.audience;
        self.routes = config.routes;
        self.conversation_members = config.conversation_members;
        self.start = config.start;
        self.delivery = config.delivery;
        self.health_policy = config.health_policy;
        self.runner = config.runner;
    }

    /// Take a fresh baseline: no token, collection from the start. The
    /// version is kept, so it never repeats, and the change that asks for
    /// this moves the revision, so an advance read before it loses at the
    /// fence.
    pub(crate) fn rebaseline(&mut self) {
        self.cursor.token = None;
        self.collection = CollectionPhase::Baselining;
    }

    /// The health status the state and the collection phase give.
    pub(crate) fn refresh_status(&mut self) {
        self.health.status = match (self.state, self.collection) {
            (SourceState::Ended, _) => SourceHealthStatus::Ended,
            (SourceState::Paused, _) => SourceHealthStatus::Paused,
            (_, CollectionPhase::Baselining) => SourceHealthStatus::Baselining,
            (_, CollectionPhase::Backfilling) => SourceHealthStatus::Backfilling,
            (_, CollectionPhase::Live) if self.health.consecutive_failures > 0 => {
                SourceHealthStatus::Failing
            }
            (_, CollectionPhase::Live) => SourceHealthStatus::Ok,
        };
    }
}

/// Create a source, replace its declaration at a revision, or start its
/// next generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutSource {
    pub mob_id: MobId,
    pub source_id: SourceId,
    /// `None` declares the source: a first generation, or the next one after
    /// an end. `Some` replaces the declaration at this revision.
    pub expected_revision: Option<u64>,
    /// With an expected revision: end the current generation and start the
    /// next one from a fresh baseline, with this identity.
    pub new_generation: bool,
    pub config: SourceConfig,
}

/// A change to a source's operator state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SourceChange {
    Pause,
    /// Resume collecting, from the kept cursor or from a fresh baseline.
    Resume {
        rebaseline: bool,
    },
    /// End the generation; the source's topics stay reserved to it.
    End,
}

/// A page of sources.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SourceQuery {
    pub mob_id: Option<MobId>,
    /// Start after this source, in `(mob_id, source_id)` order.
    pub after: Option<(MobId, SourceId)>,
    pub state: Option<SourceState>,
    pub limit: usize,
}
