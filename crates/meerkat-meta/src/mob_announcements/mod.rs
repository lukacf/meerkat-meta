//! Mob announcements: members of a mob, and the host, announce developments
//! to the mob; members subscribe to topics of the mobs they name; the
//! announcements each subscriber is owed reach it in bounded batches,
//! durably and reconcilably.
//!
//! Active only when a host enables it at startup
//! (`runtime_options.mob_announcements`). When it is off nothing here runs.
//!
//! # Ownership
//!
//! This module owns announcements, member subscriptions, capture, batch
//! sealing and its own delivery progress. Meerkat owns everything native: a
//! sealed batch is delivered as one host continuation
//! ([`native::ANNOUNCEMENT_CONTINUATION_NAMESPACE`], keyed by batch id)
//! through Meerkat's continuation owner, which commits it into the member's
//! durable runtime inbox, binds it to the member incarnation current at the
//! first submission, and admits it as one input when the member is served.
//! The announcement owner reads those facts back; it never mirrors or decides
//! them.
//!
//! # Facts a caller can observe
//!
//! For each delivery, `meta/mob_announcements/deliveries/observe` reports
//! separately: the announcements it carries, the sealed batch (what the
//! member is owed), the native commit receipt, native admission as an input,
//! and that input's own runtime state. There is no single "processed" flag:
//! admission says nothing about what the agent did.
//!
//! # Attention policy
//!
//! An announcement is captured for every matching subscription in the
//! transaction that commits it. A member's owed announcements are sealed
//! into at most one outstanding batch at a time, at most one batch per wake
//! interval, when the batch is full, the oldest owed announcement has waited
//! `max_delay_ms`, or a paused subscription resumes. No model call happens
//! per announcement.

pub mod availability;
mod faults;
pub mod native;
pub mod owner;
pub mod remote;
pub mod rpc;
pub mod sqlite;
pub(crate) mod storage;
pub mod store;
pub mod tools;
pub mod types;

#[cfg(test)]
mod remote_tests;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use serde::Deserialize;

pub use availability::{
    BlockCause, DepartureEvidence, EvidenceClass, MemberDeparture, NotServedHereCause,
    SuspendCause, SuspendMarker,
};
pub use native::{
    ANNOUNCEMENT_CONTINUATION_NAMESPACE, AnnouncementNativeDelivery, IdentityService, MemberMobEnd,
    MemberMobScan, MemberProblem, MemberRoute, MobNativeDelivery, MobPresence, NativeDelivery,
    NativeStatus, NativeStatusError, NativeSubmitError, NativeUnavailable, PrepareError,
    batch_summary, continuation_delivery, continuation_for,
};
pub use owner::{
    AdmittedInput, AnnouncementConfig, AnnouncementError, AnnouncementService,
    DEFAULT_RETENTION_MS, DeliveryObservation, MIN_RETENTION_MS, NativeObservation, RoundReport,
};
pub use remote::{KeyedInjectServer, RemoteMemberDelivery};
pub use sqlite::{ConnectionGuard, SCHEMA, SqliteAnnouncementStore};
pub use store::AnnouncementStore;
pub use types::{
    AnnounceOutcome, AnnounceRequest, Announcement, AnnouncementRef, AnnouncementStoreError,
    Announcer, Batch, BatchEvent, BatchEventTag, BatchId, BatchItem, BatchQuery, BatchState,
    BatchStateTag, ChildMemberLimits, LateOutcome, MAX_ITEM_WIRE_BYTES, MAX_REMOTE_BODY_WIRE_BYTES,
    Member, MemberBacklog, MemberIdentity, MemberRouteRecord, MobId, NativeReceipt, OperationId,
    PruneReport, PutSubscription, SealingPolicy, Selector, Settlement, SourceKey, StartFrontier,
    Subscription, SubscriptionChange, SubscriptionChangeOutcome, SubscriptionEnd, SubscriptionId,
    SubscriptionState, SuccessorBatch, Topic,
};

/// `runtime_options.mob_announcements`, as a host passes it at startup.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct AnnouncementOptions {
    enabled: bool,
    #[serde(default)]
    batch: Option<BatchOptions>,
    #[serde(default)]
    retention_ms: Option<u64>,
    #[serde(default)]
    member_limits: Option<MemberLimitOptions>,
    #[serde(default)]
    member_tools_in_child_mobs: bool,
    #[serde(default)]
    child_member_limits: Option<ChildMemberLimitOptions>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildMemberLimitOptions {
    #[serde(default)]
    subscriptions: Option<u32>,
    #[serde(default)]
    per_mob: Option<u32>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemberLimitOptions {
    #[serde(default)]
    announcements_per_minute: Option<u32>,
    #[serde(default)]
    announcement_bytes_per_minute: Option<u64>,
    #[serde(default)]
    subscriptions: Option<u32>,
    #[serde(default)]
    topics_per_subscription: Option<u32>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchOptions {
    #[serde(default)]
    max_count: Option<u32>,
    #[serde(default)]
    max_bytes: Option<u64>,
    #[serde(default)]
    max_delay_ms: Option<u64>,
    #[serde(default)]
    min_wake_interval_ms: Option<u64>,
}

/// Why a mob announcement configuration is refused before anything is
/// composed, on every surface that takes one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AnnouncementOptionsError {
    /// An option is malformed or out of range; the message names it.
    Invalid(String),
    /// `member_tools_in_child_mobs` would hand the member tools to every
    /// inline profile of every mob a member creates. Each child mob profile
    /// has to opt in itself, and this build cannot offer that opt-in yet.
    ChildMemberToolsNeedProfileOptIn,
}

impl From<String> for AnnouncementOptionsError {
    fn from(message: String) -> Self {
        Self::Invalid(message)
    }
}

impl std::fmt::Display for AnnouncementOptionsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => f.write_str(message),
            Self::ChildMemberToolsNeedProfileOptIn => f.write_str(
                "member_tools_in_child_mobs is refused: each child mob profile must opt in to the \
                 member tools itself, and this build cannot offer that per-profile opt-in yet",
            ),
        }
    }
}

/// Parse `runtime_options.mob_announcements`. `Ok(None)` when it is
/// disabled.
pub(crate) fn parse_runtime_options(
    value: &serde_json::Value,
) -> Result<Option<AnnouncementConfig>, AnnouncementOptionsError> {
    parse_options(value, "runtime_options.mob_announcements")
}

/// Parse a mob announcement options object (`{enabled, batch?,
/// retention_ms?, member_limits?, member_tools_in_child_mobs?,
/// child_member_limits?}`), as every gateway takes it; `path` is where the
/// surface takes it, for the messages. `Ok(None)` when it is disabled. The
/// result has passed [`AnnouncementConfig::validate`].
pub fn parse_options(
    value: &serde_json::Value,
    path: &str,
) -> Result<Option<AnnouncementConfig>, AnnouncementOptionsError> {
    let options: AnnouncementOptions =
        serde_json::from_value(value.clone()).map_err(|error| format!("{path}: {error}"))?;
    // Refused whether or not announcements are enabled: an operator who sets
    // it would believe it does something.
    if options.member_tools_in_child_mobs {
        return Err(AnnouncementOptionsError::ChildMemberToolsNeedProfileOptIn);
    }
    if !options.enabled {
        return Ok(None);
    }
    let mut config = AnnouncementConfig::default();
    if let Some(batch) = options.batch {
        let policy = &mut config.policy;
        policy.max_count = batch.max_count.unwrap_or(policy.max_count);
        policy.max_bytes = batch.max_bytes.unwrap_or(policy.max_bytes);
        policy.max_delay_ms = batch.max_delay_ms.unwrap_or(policy.max_delay_ms);
        policy.min_wake_interval_ms = batch
            .min_wake_interval_ms
            .unwrap_or(policy.min_wake_interval_ms);
    }
    if let Some(retention_ms) = options.retention_ms {
        config.retention_ms = retention_ms;
    }
    if let Some(limits) = options.member_limits {
        let current = &mut config.member_limits;
        current.announcements_per_minute = limits
            .announcements_per_minute
            .unwrap_or(current.announcements_per_minute);
        current.announcement_bytes_per_minute = limits
            .announcement_bytes_per_minute
            .unwrap_or(current.announcement_bytes_per_minute);
        current.subscriptions = limits.subscriptions.unwrap_or(current.subscriptions);
        current.topics_per_subscription = limits
            .topics_per_subscription
            .unwrap_or(current.topics_per_subscription);
    }
    if let Some(limits) = options.child_member_limits {
        let current = &mut config.child_limits;
        current.subscriptions = limits.subscriptions.unwrap_or(current.subscriptions);
        current.per_mob = limits.per_mob.unwrap_or(current.per_mob);
    }
    config.validate().map_err(|error| match error {
        AnnouncementOptionsError::Invalid(message) => {
            AnnouncementOptionsError::Invalid(format!("{path}.{message}"))
        }
        other => other,
    })?;
    Ok(Some(config))
}

/// Where a runtime keeps its mob announcement owner once one attaches. A
/// surface built before the owner attaches (the console) reads it at each
/// request.
pub type AnnouncementSlot = Arc<std::sync::OnceLock<AnnouncementService>>;

/// Mob announcements as a host enables them, composed the same way on every
/// surface: `rpc_gateway` from `runtime_options.mob_announcements`,
/// `meta_gateway` from its init's `mob_announcements`, and a runtime the
/// builder composes from
/// [`UnifiedRuntimeBuilder::mob_announcements`](crate::UnifiedRuntimeBuilder::mob_announcements).
///
/// Two steps, in this order: [`Self::register_member_tools`] puts the member
/// tool bundle on the mob spec before the runtime is built, and
/// [`Self::compose`] attaches the owner once the runtime's identity runtime
/// is installed, before the keyed inject plane of its control listener is
/// sealed.
pub struct AnnouncementComposition {
    config: AnnouncementConfig,
    member_tools: tools::ToolSlot,
}

impl AnnouncementComposition {
    /// Refuses a configuration [`AnnouncementConfig::validate`] refuses.
    pub fn new(config: AnnouncementConfig) -> Result<Self, AnnouncementOptionsError> {
        config.validate()?;
        Ok(Self {
            config,
            member_tools: Arc::new(std::sync::OnceLock::new()),
        })
    }

    /// The window a peer applies to this gateway's keyed inject requests: a
    /// batch for a member on a peer expires only once it has passed. Each
    /// surface sets the window its own control listener serves with.
    #[must_use]
    pub fn with_peer_request_window(mut self, window_ms: u64) -> Self {
        self.config.peer_request_window_ms = window_ms;
        self
    }

    /// Put the member tool bundle ([`tools::TOOL_BUNDLE`]) on `spec`. The
    /// host's own profiles that name it in `tools.rust_bundles` get the
    /// member tools; no child mob profile does, since that takes a
    /// per-profile opt-in this build cannot offer yet.
    pub fn register_member_tools(
        &self,
        spec: crate::mob_handle_runtime::MobBootstrapSpec,
    ) -> crate::mob_handle_runtime::MobBootstrapSpec {
        let (name, dispatcher) = self.member_tool_bundle();
        spec.register_tool_bundle_with_availability(
            name,
            dispatcher,
            meerkat_mob_mcp::ChildToolBundleAvailability::HostOnly,
        )
    }

    /// The member tool bundle's name and dispatcher, for a composer that
    /// collects its bundles before it resolves a spec. Host only.
    pub(crate) fn member_tool_bundle(
        &self,
    ) -> (&'static str, Arc<dyn meerkat_core::AgentToolDispatcher>) {
        (
            tools::TOOL_BUNDLE,
            Arc::new(tools::MobAnnouncementTools::new(Arc::clone(
                &self.member_tools,
            ))),
        )
    }

    /// Attach the owner to `runtime`: open its database under `layout`,
    /// bind the member tools, start the dispatcher and follow the mob set,
    /// so destroyed mobs leave the subscriptions that name them. The runtime
    /// must be identity-first; its shutdown stops the dispatcher.
    pub fn compose(
        self,
        runtime: &crate::unified_runtime::UnifiedRuntime,
        layout: &crate::storage_layout::MetaStorageLayout,
    ) -> Result<AnnouncementService, String> {
        let Self {
            config,
            member_tools,
        } = self;
        let local = Arc::new(local_delivery(runtime)?);
        let identities = runtime
            .identity_runtime_ref()
            .cloned()
            .ok_or("mob announcements require an identity-first gateway")?;
        let own_mob = MobId::new(runtime.mob_handle().mob_id().to_string())
            .map_err(|error| format!("this gateway's mob id: {error}"))?;
        let path = layout
            .mob_announcements_db()
            .map_err(|error| format!("mob announcement database: {error}"))?;
        let store = storage::open_disk_store(path)
            .map_err(|error| format!("mob announcement database: {error}"))?;
        let window_ms = config.peer_request_window_ms;
        let remote = runtime
            .peer_delivery_access()
            .map(|(contacts, keys)| Arc::new(RemoteMemberDelivery::new(contacts, keys, window_ms)));
        // Callers in mobs members created are attributed through this
        // gateway's runtime only when they can be offered the tools.
        let children = config
            .member_tools_in_child_mobs
            .then(|| Arc::clone(&local));
        let native = AnnouncementNativeDelivery::new(local, remote);
        let service = AnnouncementService::new(Arc::new(store), Arc::new(native), config)
            .map_err(|error| error.to_string())?;
        if !runtime.attach_mob_announcements(service.clone()) {
            return Err("mob announcements are already attached to this runtime".to_string());
        }
        // A non-owning handle: the member tools never keep the mob open.
        let callers = tools::RosterCallerResolver::new(
            own_mob,
            runtime.mob_handle().observer(),
            identities,
            children,
        );
        let bound = member_tools
            .set(tools::ToolBackend {
                service: service.clone(),
                callers: Arc::new(callers),
                limits: config.member_limits,
            })
            .is_ok();
        if !bound {
            return Err("the member tools are already bound".to_string());
        }
        drop(service.start());
        drop(service.watch_mobs());
        Ok(service)
    }
}

/// The seam to this gateway's own runtime: its identity runtime, its agent
/// mob tool state and its runtime machine.
fn local_delivery(
    runtime: &crate::unified_runtime::UnifiedRuntime,
) -> Result<MobNativeDelivery, String> {
    let identities = runtime
        .identity_runtime_ref()
        .cloned()
        .ok_or("mob announcements require an identity-first gateway")?;
    let mob_runtime = runtime.mob_runtime();
    let mob_tools = mob_runtime
        .agent_mob_mcp_state()
        .ok_or("mob announcements require the gateway's agent mob tool state")?;
    let sessions = mob_runtime
        .session_service()
        .cloned()
        .ok_or("mob announcements require the gateway's session service")?;
    let machine = crate::mob_handle_runtime::observed_runtime_adapter(sessions.as_ref())
        .ok_or("mob announcements require the gateway's runtime machine")?;
    let mob_id = MobId::new(runtime.mob_handle().mob_id().to_string())
        .map_err(|error| format!("this gateway's mob id: {error}"))?;
    Ok(MobNativeDelivery::new(
        mob_id, identities, mob_tools, machine, sessions,
    ))
}

/// How a gateway's configuration has its control channel serve the keyed
/// inject plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeyedInjectPlane {
    /// Serve peers' keyed injects, refusing a request issued further than
    /// `window_ms` from this gateway's clock.
    Serve { window_ms: u64 },
    /// Serve none: both verbs answer `not_served` with the cause
    /// `runtime_disabled`.
    Disabled,
}

impl Default for KeyedInjectPlane {
    fn default() -> Self {
        Self::Serve {
            window_ms: crate::runtime::keyed_inject::DEFAULT_KEYED_INJECT_WINDOW_MS,
        }
    }
}

/// Seal the keyed inject plane of a gateway whose control listener is bound,
/// once its runtime is composed: every surface (`rpc_gateway`,
/// `meta_gateway` and the runtime builder) composes it here and nowhere
/// else.
///
/// An identity-first gateway that serves keyed injects submits what a peer
/// delivers to one of its members to its own continuation owner, whether or
/// not it enables mob announcements itself. Any other gateway answers
/// `not_served` with its typed cause. Until this runs, both verbs answer
/// `keyed_inject_unavailable`: the gateway is starting.
pub fn compose_keyed_inject_plane(
    runtime: &crate::unified_runtime::UnifiedRuntime,
    plane: KeyedInjectPlane,
) -> Result<(), String> {
    use crate::runtime::keyed_inject::{KeyedInjectServing, NotServedCause};
    let serving = match plane {
        KeyedInjectPlane::Disabled => {
            KeyedInjectServing::NotServed(NotServedCause::RuntimeDisabled)
        }
        KeyedInjectPlane::Serve { .. } if runtime.identity_runtime_ref().is_none() => {
            KeyedInjectServing::NotServed(NotServedCause::NotIdentityFirst)
        }
        KeyedInjectPlane::Serve { window_ms } => {
            let local = Arc::new(local_delivery(runtime)?);
            KeyedInjectServing::Serving(Arc::new(KeyedInjectServer::new(local, window_ms)))
        }
    };
    if !runtime.seal_keyed_inject(serving) {
        return Err("the keyed inject plane of this runtime is already sealed".to_string());
    }
    Ok(())
}
