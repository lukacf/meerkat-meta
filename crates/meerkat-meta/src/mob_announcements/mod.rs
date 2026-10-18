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

mod faults;
pub mod native;
pub mod owner;
pub mod rpc;
pub mod sqlite;
pub(crate) mod storage;
pub mod store;
pub mod types;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use serde::Deserialize;

pub use native::{
    ANNOUNCEMENT_CONTINUATION_NAMESPACE, IdentityService, MemberProblem, MemberRoute,
    MobNativeDelivery, MobPresence, NativeDelivery, NativeStatus, NativeSubmitError,
    NativeUnavailable, PrepareError, batch_summary, continuation_delivery, continuation_for,
};
pub use owner::{
    AdmittedInput, AnnouncementConfig, AnnouncementError, AnnouncementService,
    DEFAULT_RETENTION_MS, DeliveryObservation, MIN_RETENTION_MS, NativeObservation, RoundReport,
};
pub use sqlite::{ConnectionGuard, SCHEMA, SqliteAnnouncementStore};
pub use store::AnnouncementStore;
pub use types::{
    AnnounceOutcome, AnnounceRequest, Announcement, AnnouncementRef, AnnouncementStoreError,
    Announcer, Batch, BatchEvent, BatchEventTag, BatchId, BatchItem, BatchQuery, BatchState,
    BatchStateTag, MAX_ITEM_WIRE_BYTES, MAX_REMOTE_BODY_WIRE_BYTES, Member, MemberBacklog,
    MemberIdentity, MobId, NativeReceipt, OperationId, PruneReport, PutSubscription, SealingPolicy,
    Selector, Settlement, SourceKey, StartFrontier, Subscription, SubscriptionChange,
    SubscriptionChangeOutcome, SubscriptionEnd, SubscriptionId, SubscriptionState, SuccessorBatch,
    Topic,
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

/// Parse `runtime_options.mob_announcements`. `Ok(None)` when it is
/// disabled.
pub(crate) fn parse_runtime_options(
    value: &serde_json::Value,
) -> Result<Option<AnnouncementConfig>, String> {
    let options: AnnouncementOptions = serde_json::from_value(value.clone())
        .map_err(|error| format!("runtime_options.mob_announcements: {error}"))?;
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
        if retention_ms < owner::MIN_RETENTION_MS {
            return Err(format!(
                "runtime_options.mob_announcements.retention_ms must be at least {}",
                owner::MIN_RETENTION_MS
            ));
        }
        config.retention_ms = retention_ms;
    }
    config
        .policy
        .validate()
        .map_err(|error| format!("runtime_options.mob_announcements.batch: {error}"))?;
    Ok(Some(config))
}

/// Compose mob announcements for a persistent identity-first gateway: open
/// its database, attach the owner to the runtime, start the dispatcher and
/// follow the mob set, so destroyed mobs leave the subscriptions that name
/// them.
pub(crate) fn compose_for_runtime(
    runtime: &crate::unified_runtime::UnifiedRuntime,
    layout: &crate::storage_layout::MetaStorageLayout,
    config: AnnouncementConfig,
) -> Result<AnnouncementService, String> {
    let identities = runtime
        .identity_runtime_ref()
        .cloned()
        .ok_or("mob announcements require an identity-first gateway")?;
    let mob_runtime = runtime.mob_runtime();
    let mob_tools = mob_runtime
        .agent_mob_mcp_state()
        .ok_or("mob announcements require the gateway's agent mob tool state")?;
    let machine = mob_runtime
        .session_service()
        .and_then(|service| crate::mob_handle_runtime::observed_runtime_adapter(service.as_ref()))
        .ok_or("mob announcements require the gateway's runtime machine")?;
    let mob_id = MobId::new(runtime.mob_handle().mob_id().to_string())
        .map_err(|error| format!("this gateway's mob id: {error}"))?;
    let path = layout
        .mob_announcements_db()
        .map_err(|error| format!("mob announcement database: {error}"))?;
    let store = storage::open_disk_store(path)
        .map_err(|error| format!("mob announcement database: {error}"))?;
    let native = MobNativeDelivery::new(mob_id, identities, mob_tools, machine);
    let service = AnnouncementService::new(Arc::new(store), Arc::new(native), config)
        .map_err(|error| error.to_string())?;
    if !runtime.attach_mob_announcements(service.clone()) {
        return Err("mob announcements are already attached to this runtime".to_string());
    }
    drop(service.start());
    drop(service.watch_mobs());
    Ok(service)
}
