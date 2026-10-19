//! The `meta/mob_announcements/*` JSON-RPC methods.
//!
//! They answer only when the host enabled mob announcements at startup.
//! Parameters are strict: unknown fields are refused. A host call announces
//! as the host; members announce through their own tools.

use std::collections::BTreeSet;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::owner::{AnnouncementError, AnnouncementService};
use super::types::{
    AnnounceRequest, AnnouncementRef, AnnouncementStoreError, Announcer, BatchId, BatchQuery,
    ChildMemberLimits, Member, MobId, OperationId, PutSubscription, Selector, SourceKey,
    StartFrontier, SubscriptionChange, SubscriptionId, Topic,
};
use crate::rpc::JsonRpcError;

/// Mob announcements are not enabled on this host.
pub const ANNOUNCEMENTS_UNAVAILABLE_CODE: i64 = -32044;
/// A conflicting or stale request: other content under a source key, a
/// stale revision, a reused operation id, a batch that moved.
pub const ANNOUNCEMENTS_CONFLICT_CODE: i64 = -32045;
/// A requested start frontier lies outside a mob's announcements: before
/// what is retained, or beyond its head.
pub const ANNOUNCEMENTS_FRONTIER_CODE: i64 = -32046;
/// A typed refusal: governed runtime, unknown, unsupported or unusable
/// member, unusable peer gateway, cancelled or ended subscription, member
/// change, a native read unavailable now.
pub const ANNOUNCEMENTS_REFUSED_CODE: i64 = -32047;
/// The named subscription, delivery or mob does not exist here.
pub const ANNOUNCEMENTS_NOT_FOUND_CODE: i64 = -32048;

pub const ANNOUNCEMENT_METHODS: &[&str] = &[
    "meta/mob_announcements/announce",
    "meta/mob_announcements/get",
    "meta/mob_announcements/subscriptions/put",
    "meta/mob_announcements/subscriptions/pause",
    "meta/mob_announcements/subscriptions/resume",
    "meta/mob_announcements/subscriptions/cancel",
    "meta/mob_announcements/subscriptions/get",
    "meta/mob_announcements/subscriptions/list",
    "meta/mob_announcements/deliveries/list",
    "meta/mob_announcements/deliveries/observe",
    "meta/mob_announcements/deliveries/reconcile",
];

pub fn is_announcement_method(method: &str) -> bool {
    ANNOUNCEMENT_METHODS.contains(&method)
}

fn invalid_params(message: impl std::fmt::Display) -> JsonRpcError {
    JsonRpcError::new(-32602, format!("Invalid params: {message}"))
}

fn typed(code: i64, kind: &str, message: impl std::fmt::Display, extra: Value) -> JsonRpcError {
    let mut data = json!({ "kind": kind });
    if let (Some(data), Value::Object(extra)) = (data.as_object_mut(), extra) {
        data.extend(extra);
    }
    JsonRpcError::new(code, message.to_string()).with_data(data)
}

pub fn announcements_unavailable_error() -> JsonRpcError {
    typed(
        ANNOUNCEMENTS_UNAVAILABLE_CODE,
        "mob_announcements_unavailable",
        "mob announcements are not enabled on this host",
        Value::Null,
    )
}

fn rpc_error(error: AnnouncementError) -> JsonRpcError {
    let message = error.to_string();
    match error {
        AnnouncementError::GovernedRuntime => typed(
            ANNOUNCEMENTS_REFUSED_CODE,
            "governed_runtime",
            message,
            Value::Null,
        ),
        AnnouncementError::UnknownMember(member) => typed(
            ANNOUNCEMENTS_REFUSED_CODE,
            "unknown_member",
            message,
            json!({ "member": member }),
        ),
        AnnouncementError::InvalidMember(_) => invalid_params(message),
        AnnouncementError::UnusableMember(_) => typed(
            ANNOUNCEMENTS_REFUSED_CODE,
            "unusable_member",
            message,
            Value::Null,
        ),
        AnnouncementError::UnknownMobs(mobs) => typed(
            ANNOUNCEMENTS_NOT_FOUND_CODE,
            "unknown_mob",
            message,
            json!({ "mobs": mobs }),
        ),
        AnnouncementError::UnusablePeer(_) => typed(
            ANNOUNCEMENTS_REFUSED_CODE,
            "unusable_peer",
            message,
            Value::Null,
        ),
        AnnouncementError::NativeUnavailable(_) => typed(
            ANNOUNCEMENTS_REFUSED_CODE,
            "native_unavailable",
            message,
            Value::Null,
        ),
        AnnouncementError::UnrelatedMob(mob_id) => typed(
            ANNOUNCEMENTS_REFUSED_CODE,
            "unrelated_mob",
            message,
            json!({ "mob_id": mob_id }),
        ),
        AnnouncementError::AmbiguousMob(mob_id) => typed(
            ANNOUNCEMENTS_REFUSED_CODE,
            "ambiguous_mob",
            message,
            json!({ "mob_id": mob_id }),
        ),
        AnnouncementError::Store(error) => match error {
            AnnouncementStoreError::AnnouncementConflict { existing } => typed(
                ANNOUNCEMENTS_CONFLICT_CODE,
                "announcement_conflict",
                message,
                json!({ "existing": *existing }),
            ),
            AnnouncementStoreError::StaleRevision { current } => typed(
                ANNOUNCEMENTS_CONFLICT_CODE,
                "stale_revision",
                message,
                json!({ "current_revision": current }),
            ),
            AnnouncementStoreError::OperationReused => typed(
                ANNOUNCEMENTS_CONFLICT_CODE,
                "operation_reused",
                message,
                Value::Null,
            ),
            AnnouncementStoreError::BatchMoved { current } => typed(
                ANNOUNCEMENTS_CONFLICT_CODE,
                "batch_moved",
                message,
                json!({ "current": current }),
            ),
            AnnouncementStoreError::Gap {
                mob_id,
                earliest_retained,
            } => typed(
                ANNOUNCEMENTS_FRONTIER_CODE,
                "gap",
                message,
                json!({ "mob_id": mob_id, "earliest_retained": earliest_retained }),
            ),
            AnnouncementStoreError::FrontierBeyondHead { mob_id, head } => typed(
                ANNOUNCEMENTS_FRONTIER_CODE,
                "frontier_beyond_head",
                message,
                json!({ "mob_id": mob_id, "head": head }),
            ),
            AnnouncementStoreError::UnknownSubscription => typed(
                ANNOUNCEMENTS_NOT_FOUND_CODE,
                "unknown_subscription",
                message,
                Value::Null,
            ),
            AnnouncementStoreError::UnknownBatch => typed(
                ANNOUNCEMENTS_NOT_FOUND_CODE,
                "unknown_delivery",
                message,
                Value::Null,
            ),
            AnnouncementStoreError::SubscriptionCancelled => typed(
                ANNOUNCEMENTS_REFUSED_CODE,
                "subscription_cancelled",
                message,
                Value::Null,
            ),
            AnnouncementStoreError::SubscriptionEnded { end } => typed(
                ANNOUNCEMENTS_REFUSED_CODE,
                "subscription_ended",
                message,
                json!({ "end": end }),
            ),
            AnnouncementStoreError::MemberMismatch => typed(
                ANNOUNCEMENTS_REFUSED_CODE,
                "member_mismatch",
                message,
                Value::Null,
            ),
            AnnouncementStoreError::RouteKept { kept } => typed(
                ANNOUNCEMENTS_REFUSED_CODE,
                "ambiguous_mob",
                message,
                json!({ "kept_route": kept }),
            ),
            AnnouncementStoreError::ChildMemberLimit { limit, mob_id } => typed(
                ANNOUNCEMENTS_REFUSED_CODE,
                "child_member_limit",
                message,
                json!({ "limit": limit, "mob_id": mob_id }),
            ),
            AnnouncementStoreError::IllegalTransition { .. } => JsonRpcError::new(-32000, message),
            AnnouncementStoreError::TooLarge(_) | AnnouncementStoreError::Invalid(_) => {
                invalid_params(message)
            }
            AnnouncementStoreError::Storage(_)
            | AnnouncementStoreError::RouteRequired { .. }
            | AnnouncementStoreError::RouteNotKept { .. } => JsonRpcError::new(-32000, message),
        },
    }
}

fn params<T: DeserializeOwned>(params: &Value) -> Result<T, JsonRpcError> {
    let value = if params.is_null() {
        Value::Object(serde_json::Map::new())
    } else {
        params.clone()
    };
    serde_json::from_value(value).map_err(invalid_params)
}

fn to_value(value: impl serde::Serialize) -> Result<Value, JsonRpcError> {
    serde_json::to_value(value).map_err(|error| JsonRpcError::new(-32000, error.to_string()))
}

/// `meta/mob_announcements/announce`: the host announces into a mob hosted
/// here, this gateway's own by default.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AnnounceParams {
    #[serde(default)]
    mob_id: Option<MobId>,
    source_key: SourceKey,
    topics: BTreeSet<Topic>,
    body: String,
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    causes: Vec<AnnouncementRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GetAnnouncementParams {
    mob_id: MobId,
    source_key: SourceKey,
}

fn default_start() -> StartFrontier {
    StartFrontier::Now
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PutSubscriptionParams {
    operation_id: OperationId,
    subscription_id: SubscriptionId,
    member: Member,
    topics: BTreeSet<Topic>,
    /// The mobs hosted here whose announcements the subscription takes;
    /// this gateway's own mob when absent.
    #[serde(default)]
    mobs: Option<BTreeSet<MobId>>,
    #[serde(default = "default_start")]
    start: StartFrontier,
    #[serde(default)]
    expected_revision: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeSubscriptionParams {
    operation_id: OperationId,
    subscription_id: SubscriptionId,
    expected_revision: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionParams {
    subscription_id: SubscriptionId,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListSubscriptionsParams {
    #[serde(default)]
    member: Option<Member>,
    #[serde(default)]
    after: Option<SubscriptionId>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListDeliveriesParams {
    #[serde(default)]
    member: Option<Member>,
    #[serde(default)]
    subscription_id: Option<SubscriptionId>,
    #[serde(default)]
    after: Option<BatchId>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveryParams {
    batch_id: BatchId,
}

const DEFAULT_LIMIT: usize = 100;

/// The result of an announce call. It counts the subscriptions that
/// captured the announcement and never names them: who subscribes is not
/// the announcer's to learn.
#[derive(serde::Serialize)]
struct AnnounceResult {
    announcement: super::types::Announcement,
    replayed: bool,
    captured: usize,
}

/// Answer one mob announcement method. `service` is `None` when the host did
/// not enable mob announcements; `own_mob` is this host's mob, the default
/// for a call that names none.
pub async fn handle_announcement_method(
    service: Option<&AnnouncementService>,
    own_mob: &MobId,
    method: &str,
    raw: &Value,
) -> Result<Value, JsonRpcError> {
    if !is_announcement_method(method) {
        return Err(JsonRpcError::new(-32601, "Method not found"));
    }
    let Some(service) = service else {
        return Err(announcements_unavailable_error());
    };
    match method {
        "meta/mob_announcements/announce" => {
            let request: AnnounceParams = params(raw)?;
            let outcome = service
                .announce(AnnounceRequest {
                    mob_id: request.mob_id.unwrap_or_else(|| own_mob.clone()),
                    announced_by: Announcer::Host,
                    source_key: request.source_key,
                    topics: request.topics,
                    body: request.body,
                    correlation_id: request.correlation_id,
                    causes: request.causes,
                })
                .await
                .map_err(rpc_error)?;
            to_value(AnnounceResult {
                captured: outcome.captured_for.len(),
                announcement: outcome.announcement,
                replayed: outcome.replayed,
            })
        }
        "meta/mob_announcements/get" => {
            let request: GetAnnouncementParams = params(raw)?;
            let announcement = service
                .announcement(&request.mob_id, &request.source_key)
                .await
                .map_err(rpc_error)?;
            Ok(json!({ "announcement": to_value(announcement)? }))
        }
        "meta/mob_announcements/subscriptions/put" => {
            let request: PutSubscriptionParams = params(raw)?;
            let mobs = request
                .mobs
                .unwrap_or_else(|| BTreeSet::from([own_mob.clone()]));
            let subscription = service
                .put_subscription(
                    &request.operation_id,
                    PutSubscription {
                        id: request.subscription_id,
                        member: request.member,
                        selector: Selector {
                            topics: request.topics,
                            mobs,
                        },
                        start: request.start,
                        expected_revision: request.expected_revision,
                        route: None,
                        member_mob_cursor: None,
                        child_limits: ChildMemberLimits::default(),
                    },
                )
                .await
                .map_err(rpc_error)?;
            Ok(json!({ "subscription": to_value(subscription)? }))
        }
        "meta/mob_announcements/subscriptions/pause"
        | "meta/mob_announcements/subscriptions/resume"
        | "meta/mob_announcements/subscriptions/cancel" => {
            let request: ChangeSubscriptionParams = params(raw)?;
            let change = match method {
                "meta/mob_announcements/subscriptions/pause" => SubscriptionChange::Pause,
                "meta/mob_announcements/subscriptions/resume" => SubscriptionChange::Resume,
                _ => SubscriptionChange::Cancel,
            };
            let outcome = service
                .change_subscription(
                    &request.operation_id,
                    &request.subscription_id,
                    request.expected_revision,
                    change,
                )
                .await
                .map_err(rpc_error)?;
            to_value(outcome)
        }
        "meta/mob_announcements/subscriptions/get" => {
            let request: SubscriptionParams = params(raw)?;
            let subscription = service
                .subscription(&request.subscription_id)
                .await
                .map_err(rpc_error)?;
            Ok(json!({ "subscription": to_value(subscription)? }))
        }
        "meta/mob_announcements/subscriptions/list" => {
            let request: ListSubscriptionsParams = params(raw)?;
            let subscriptions = service
                .subscriptions(
                    request.member.as_ref(),
                    request.after.as_ref(),
                    request.limit.unwrap_or(DEFAULT_LIMIT),
                )
                .await
                .map_err(rpc_error)?;
            Ok(json!({ "subscriptions": to_value(subscriptions)? }))
        }
        "meta/mob_announcements/deliveries/list" => {
            let request: ListDeliveriesParams = params(raw)?;
            let batches = service
                .deliveries(&BatchQuery {
                    member: request.member,
                    subscription: request.subscription_id,
                    after: request.after,
                    limit: request.limit.unwrap_or(DEFAULT_LIMIT),
                })
                .await
                .map_err(rpc_error)?;
            Ok(json!({ "deliveries": to_value(batches)? }))
        }
        "meta/mob_announcements/deliveries/observe" => {
            let request: DeliveryParams = params(raw)?;
            let observation = service
                .observe_delivery(&request.batch_id)
                .await
                .map_err(rpc_error)?;
            to_value(observation)
        }
        "meta/mob_announcements/deliveries/reconcile" => {
            let request: DeliveryParams = params(raw)?;
            let observation = service
                .reconcile_delivery(&request.batch_id)
                .await
                .map_err(rpc_error)?;
            to_value(observation)
        }
        _ => Err(JsonRpcError::new(-32601, "Method not found")),
    }
}
