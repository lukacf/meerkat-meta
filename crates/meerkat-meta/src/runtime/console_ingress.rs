//! Console ingress types and JSON request/response structures.

use super::*;
use crate::access::{AccessController, AccessResource, AccessView, AgentResourceAttributes};
use crate::rpc::META_CONTRACT_VERSION;

/// Console-facing view of a single mob member.
///
/// Narrow projection of meerkat's roster enriched with cached diagnostics
/// such as the current bridge session id when the console read model has one.
/// Wire field names match the console JSON — `agent_identity`, `role`,
/// `state` — so the admin UI reads them unchanged from the live snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleMember {
    pub agent_identity: String,
    pub role: String,
    pub state: String,
    #[serde(default)]
    pub model_capabilities: ConsoleModelCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub wired_to: Vec<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub labels: std::collections::BTreeMap<String, String>,
    /// Machine-owned liveness projection (meerkat 0.7.29+, ask 14):
    /// `{run_state, in_flight_work, last_progress_at_ms, last_progress_event,
    /// health}`. Kept as raw wire JSON — the console reads it, meta does
    /// not interpret it. `None` when the member has no snapshot (final
    /// members, or rosters over the progress projection cap).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleModelCapabilities {
    #[serde(default)]
    pub image_input: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleRestJsonRequest {
    pub method: String,
    pub path: String,
    pub auth: Option<ConsoleAccessRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleRestJsonResponse {
    pub status: u16,
    pub body: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleAgentLiveSnapshot {
    pub agent_id: String,
    pub member_id: String,
    pub label: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default)]
    pub model_capabilities: ConsoleModelCapabilities,
    /// Missing means activity is unavailable; explicit null means known quiet.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_response_phase"
    )]
    pub response_phase: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watched: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "alertLevel"
    )]
    pub alert_level: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub degraded: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "degradedReason"
    )]
    pub degraded_reason: Option<String>,
}

// The wire contract distinguishes missing (unknown), null (quiet), and a phase.
#[allow(clippy::option_option)]
fn deserialize_response_phase<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleLiveSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_id: Option<String>,
    pub running: bool,
    pub loaded_modules: Vec<String>,
    #[serde(default)]
    pub agents: Vec<ConsoleAgentLiveSnapshot>,
    pub members: Vec<ConsoleMember>,
    pub has_mob_runtime: bool,
}

fn console_member_console_identity(member: &ConsoleMember) -> &str {
    crate::member_comms_id::durable_identity_label(&member.labels)
        .unwrap_or(member.agent_identity.as_str())
}

impl ConsoleLiveSnapshot {
    pub fn new(
        runtime_id: Option<String>,
        running: bool,
        loaded_modules: Vec<String>,
        agents: Vec<ConsoleAgentLiveSnapshot>,
        members: Vec<ConsoleMember>,
        has_mob_runtime: bool,
    ) -> Self {
        let mut seen = BTreeSet::new();
        let mut deduped_modules = Vec::new();
        for module_id in loaded_modules {
            if seen.insert(module_id.clone()) {
                deduped_modules.push(module_id);
            }
        }
        let mut seen_agents = BTreeSet::new();
        let mut deduped_agents = Vec::new();
        for agent in agents {
            if seen_agents.insert(agent.agent_id.clone()) {
                deduped_agents.push(agent);
            }
        }
        Self {
            runtime_id,
            running,
            loaded_modules: deduped_modules,
            agents: deduped_agents,
            members,
            has_mob_runtime,
        }
    }
}

pub fn handle_console_rest_json_route(
    decisions: &RuntimeDecisionState,
    request: &ConsoleRestJsonRequest,
) -> ConsoleRestJsonResponse {
    handle_console_rest_json_route_with_snapshot(decisions, request, None)
}

pub fn handle_console_rest_json_route_with_snapshot(
    decisions: &RuntimeDecisionState,
    request: &ConsoleRestJsonRequest,
    live_snapshot: Option<&ConsoleLiveSnapshot>,
) -> ConsoleRestJsonResponse {
    handle_console_rest_json_route_with_snapshot_and_access(decisions, request, live_snapshot, None)
}

/// Access-aware variant of [`handle_console_rest_json_route_with_snapshot`].
///
/// When an [`AccessController`] is supplied and enabled, the experience
/// projection is filtered per authenticated principal: agents the caller
/// may not view disappear from every section, per-agent affordances are
/// intersected with the caller's grants, and an `access` section describing
/// the caller's standing is appended for the console UI.
pub fn handle_console_rest_json_route_with_snapshot_and_access(
    decisions: &RuntimeDecisionState,
    request: &ConsoleRestJsonRequest,
    live_snapshot: Option<&ConsoleLiveSnapshot>,
    access: Option<&AccessController>,
) -> ConsoleRestJsonResponse {
    handle_console_rest_json_route_with_snapshot_access_and_memory(
        decisions,
        request,
        live_snapshot,
        access,
        false,
    )
}

/// Memory-panel-aware variant: `memory_panel_available` reports whether the
/// runtime wired a Memory-panel store, so the experience can project the
/// `memory` affordance section (§9.3) the console nav gate consumes.
pub fn handle_console_rest_json_route_with_snapshot_access_and_memory(
    decisions: &RuntimeDecisionState,
    request: &ConsoleRestJsonRequest,
    live_snapshot: Option<&ConsoleLiveSnapshot>,
    access: Option<&AccessController>,
    memory_panel_available: bool,
) -> ConsoleRestJsonResponse {
    handle_console_rest_json_route_with_snapshot_access_memory_and_workgraph(
        decisions,
        request,
        live_snapshot,
        access,
        memory_panel_available,
        false,
    )
}

/// Workgraph-aware variant: `workgraph_available` reports whether the runtime
/// wired a WorkGraph service, so the experience can project the `workgraph`
/// affordance section the console consumes (`available` + ABAC-intersected
/// `can_view`/`can_manage`).
pub fn handle_console_rest_json_route_with_snapshot_access_memory_and_workgraph(
    decisions: &RuntimeDecisionState,
    request: &ConsoleRestJsonRequest,
    live_snapshot: Option<&ConsoleLiveSnapshot>,
    access: Option<&AccessController>,
    memory_panel_available: bool,
    workgraph_available: bool,
) -> ConsoleRestJsonResponse {
    let (base_path, query_params) = split_path_and_query(&request.path);
    if request.method != "GET"
        || (base_path != CONSOLE_MODULES_ROUTE && base_path != CONSOLE_EXPERIENCE_ROUTE)
    {
        return ConsoleRestJsonResponse {
            status: 404,
            body: serde_json::json!({"error":"not_found"}),
        };
    }

    let resolved_auth = match resolve_console_auth(decisions, request.auth.as_ref(), &query_params)
    {
        Ok(auth) => auth,
        Err(error) => {
            return ConsoleRestJsonResponse {
                status: 401,
                body: serde_json::json!({
                    "error":"unauthorized",
                    "reason": console_auth_error_reason(&error),
                }),
            };
        }
    };

    match &resolved_auth {
        Some(auth) => {
            if let Err(error) =
                enforce_console_route_access(&decisions.auth, &decisions.console, auth)
            {
                return ConsoleRestJsonResponse {
                    status: 401,
                    body: serde_json::json!({
                        "error":"unauthorized",
                        "reason": auth_error_reason(&error),
                    }),
                };
            }
        }
        None if decisions.console.require_app_auth => {
            return ConsoleRestJsonResponse {
                status: 401,
                body: serde_json::json!({
                    "error":"unauthorized",
                    "reason":"missing_credentials",
                }),
            };
        }
        None => {}
    }

    // Subject for ABAC. When app auth is not required the auth resolver
    // returns None without looking at credentials; still identify callers
    // that *did* present a valid token so per-user grants apply on
    // auth-optional consoles. Invalid tokens fall back to anonymous.
    let mut access_subject = resolved_auth.as_ref().map(|auth| auth.email.clone());
    if access_subject.is_none()
        && access.is_some()
        && !decisions.console.require_app_auth
        && let Some(token) = query_params.get("auth_token")
    {
        access_subject =
            resolve_authorized_console_auth_from_token(decisions, token).map(|auth| auth.email);
    }
    let access_view =
        access.map(|controller| controller.view_for_subject(access_subject.as_deref()));

    let modules: Vec<String> = decisions
        .modules
        .iter()
        .map(|module| module.id.clone())
        .collect();
    let mut live_snapshot = live_snapshot
        .cloned()
        .unwrap_or_else(|| default_console_live_snapshot(decisions));
    if let Some(controller) = access {
        prime_access_attributes(controller, &live_snapshot);
    }
    if let Some(view) = &access_view {
        filter_snapshot_for_access(&mut live_snapshot, view);
    }
    let mut body = if base_path == CONSOLE_EXPERIENCE_ROUTE {
        build_console_experience_contract(&modules, &live_snapshot, &decisions.console)
    } else {
        serde_json::json!({
            "contract_version": META_CONTRACT_VERSION,
            "modules": modules
        })
    };
    if base_path == CONSOLE_EXPERIENCE_ROUTE
        && let Some(view) = &access_view
    {
        apply_access_to_experience(&mut body, view);
    }
    if base_path == CONSOLE_EXPERIENCE_ROUTE {
        apply_memory_to_experience(&mut body, access_view.as_ref(), memory_panel_available);
        apply_workgraph_to_experience(&mut body, access_view.as_ref(), workgraph_available);
    }
    ConsoleRestJsonResponse { status: 200, body }
}

/// Append the `workgraph` affordance section. `can_view`/`can_manage` are
/// intersected with the caller's grants when access control is enforced and
/// mirror availability otherwise; per-method enforcement still applies to
/// every `meta/workgraph/*` RPC.
fn apply_workgraph_to_experience(
    body: &mut Value,
    view: Option<&AccessView>,
    workgraph_available: bool,
) {
    let (can_view, can_manage) = match view.filter(|view| view.enforced()) {
        Some(view) => (
            workgraph_available && view.allows(crate::access::ACTION_WORKGRAPH_VIEW),
            workgraph_available && view.allows(crate::access::ACTION_WORKGRAPH_MANAGE),
        ),
        None => (workgraph_available, workgraph_available),
    };
    if let Some(object) = body.as_object_mut() {
        object.insert(
            "workgraph".to_string(),
            serde_json::json!({
                "available": workgraph_available,
                "can_view": can_view,
                "can_manage": can_manage,
            }),
        );
    }
}

/// Append the `memory` affordance section (§9.3) the console nav gate
/// consumes. Coarse by design — `can_read` means "some memory read could
/// succeed for this caller", mirroring the capabilities intersection;
/// per-scope enforcement still applies to every panel RPC.
fn apply_memory_to_experience(
    body: &mut Value,
    view: Option<&AccessView>,
    memory_panel_available: bool,
) {
    let (can_read, can_review_quarantine) = match view.filter(|view| view.enforced()) {
        Some(view) => (
            memory_panel_available
                && (view.may_perform_anywhere(crate::access::ACTION_AGENT_MEMORY_READ)
                    || view.may_perform_anywhere(crate::access::ACTION_MOB_MEMORY_READ)
                    || view.may_perform_anywhere(crate::access::ACTION_OPERATOR_MEMORY_READ)),
            memory_panel_available && view.allows(crate::access::ACTION_MEMORY_QUARANTINE_REVIEW),
        ),
        None => (memory_panel_available, memory_panel_available),
    };
    if let Some(object) = body.as_object_mut() {
        object.insert(
            "memory".to_string(),
            serde_json::json!({
                "available": memory_panel_available,
                "can_read": can_read,
                "can_review_quarantine": can_review_quarantine,
            }),
        );
    }
}

/// Feed the controller's attribute cache from a roster snapshot so
/// label/role selectors resolve on surfaces that only carry an identity
/// string (timeline frames, send requests, SSE streams).
fn prime_access_attributes(controller: &AccessController, snapshot: &ConsoleLiveSnapshot) {
    for agent in &snapshot.agents {
        let identity = agent
            .identity
            .clone()
            .unwrap_or_else(|| agent.agent_id.clone());
        controller.record_agent_attributes(AgentResourceAttributes {
            identity,
            agent_id: Some(agent.agent_id.clone()),
            role: agent.role.clone(),
            labels: std::collections::BTreeMap::new(),
        });
    }
    // Members carry the richer attribute set; record them last so they win.
    for member in &snapshot.members {
        controller.record_agent_attributes(AgentResourceAttributes {
            identity: console_member_console_identity(member).to_string(),
            agent_id: Some(member.agent_identity.clone()),
            role: Some(member.role.clone()),
            labels: member.labels.clone(),
        });
    }
}

fn member_access_resource(member: &ConsoleMember) -> AccessResource<'_> {
    AccessResource {
        identity: Some(console_member_console_identity(member)),
        agent_id: Some(member.agent_identity.as_str()),
        role: Some(member.role.as_str()),
        labels: Some(&member.labels),
    }
}

fn filter_snapshot_for_access(snapshot: &mut ConsoleLiveSnapshot, view: &AccessView) {
    if !view.enforced() {
        return;
    }
    snapshot.members.retain(|member| {
        view.decide(
            crate::access::ACTION_AGENT_VIEW,
            &member_access_resource(member),
        )
        .is_allow()
    });
    snapshot.agents.retain(|agent| {
        let identity = agent.identity.as_deref().unwrap_or(agent.agent_id.as_str());
        view.allows_agent(crate::access::ACTION_AGENT_VIEW, identity)
    });
    // Module IDs double as module-agent sidebar rows when no roster
    // member survives filtering; gate them like any other agent so a
    // fully-denied caller is not shown the module fallback.
    snapshot
        .loaded_modules
        .retain(|module_id| view.allows_agent(crate::access::ACTION_AGENT_VIEW, module_id));
}

/// Intersect the projected experience with the caller's grants and append
/// the `access` section the console UI consumes.
fn apply_access_to_experience(body: &mut Value, view: &AccessView) {
    if view.enforced() {
        let mut any_can_send = false;
        let mut any_can_retire = false;
        if let Some(agents) = body
            .pointer_mut("/agent_sidebar/live_snapshot/agents")
            .and_then(Value::as_array_mut)
        {
            for agent in agents {
                let identity = agent
                    .get("identity")
                    .or_else(|| agent.get("member_id"))
                    .or_else(|| agent.get("agent_id"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let can_send =
                    view.allows_agent(crate::access::ACTION_AGENT_SEND, identity.as_str());
                let can_retire =
                    view.allows_agent(crate::access::ACTION_AGENT_RETIRE, identity.as_str());
                let can_respawn =
                    view.allows_agent(crate::access::ACTION_AGENT_RESPAWN, identity.as_str());
                if let Some(affordances) =
                    agent.get_mut("affordances").and_then(Value::as_object_mut)
                {
                    intersect_bool(affordances, "can_send_message", can_send);
                    intersect_bool(affordances, "can_retire", can_retire);
                    intersect_bool(affordances, "can_respawn", can_respawn);
                    any_can_send |= affordances
                        .get("can_send_message")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    any_can_retire |= affordances
                        .get("can_retire")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                } else {
                    any_can_send |= can_send;
                    any_can_retire |= can_retire;
                }
            }
        }
        if let Some(capabilities) = body
            .get_mut("runtime_capabilities")
            .and_then(Value::as_object_mut)
        {
            intersect_bool(capabilities, "can_send_messages", any_can_send);
            intersect_bool(capabilities, "can_retire_members", any_can_retire);
            intersect_bool(
                capabilities,
                "can_spawn_members",
                view.allows(crate::access::ACTION_AGENT_SPAWN),
            );
            intersect_bool(
                capabilities,
                "can_wire_members",
                view.allows(crate::access::ACTION_RUNTIME_ADMIN),
            );
        }
    }
    if let Some(object) = body.as_object_mut() {
        object.insert(
            "access".to_string(),
            serde_json::json!({
                "available": true,
                "enabled": view.enforced(),
                "subject": view.subject(),
                "groups": view.groups().iter().collect::<Vec<_>>(),
                "can_administer": view.can_administer(),
            }),
        );
    }
}

fn intersect_bool(object: &mut serde_json::Map<String, Value>, key: &str, allowed: bool) {
    let current = object.get(key).and_then(Value::as_bool).unwrap_or(false);
    object.insert(key.to_string(), Value::Bool(current && allowed));
}

fn default_console_live_snapshot(decisions: &RuntimeDecisionState) -> ConsoleLiveSnapshot {
    let loaded_modules = decisions
        .modules
        .iter()
        .map(|module| module.id.clone())
        .collect::<Vec<_>>();
    let agents = loaded_modules
        .iter()
        .map(|module_id| ConsoleAgentLiveSnapshot {
            agent_id: module_id.clone(),
            member_id: module_id.clone(),
            label: module_id.clone(),
            kind: "module_agent".to_string(),
            identity: None,
            role: None,
            state: Some("idle".to_string()),
            session_id: None,
            model_capabilities: ConsoleModelCapabilities::default(),
            response_phase: None,
            watched: None,
            alert_level: None,
            degraded: None,
            degraded_reason: None,
        })
        .collect::<Vec<_>>();
    ConsoleLiveSnapshot::new(
        None,
        !decisions.modules.is_empty(),
        loaded_modules,
        agents,
        Vec::new(),
        false,
    )
}

fn build_console_experience_contract(
    modules: &[String],
    live_snapshot: &ConsoleLiveSnapshot,
    console_policy: &ConsolePolicy,
) -> Value {
    let console_config = &console_policy.ui;
    let is_read_only = console_policy.read_only;
    let is_aggregate_console = live_snapshot.runtime_id.as_deref() == Some("console-aggregator");
    fn has_extended_agent_contract(agent: &ConsoleAgentLiveSnapshot) -> bool {
        agent.role.is_some()
            || agent.session_id.is_some()
            || agent.model_capabilities != ConsoleModelCapabilities::default()
            || agent.response_phase.is_some()
            || agent.watched.is_some()
            || agent.alert_level.is_some()
            || agent.degraded.is_some()
            || agent.degraded_reason.is_some()
            || agent.kind != "module_agent"
            || agent.member_id != agent.agent_id
            || agent.label != agent.agent_id
    }

    let module_panels = modules
        .iter()
        .map(|module_id| {
            serde_json::json!({
                "panel_id": format!("module.{module_id}"),
                "module_id": module_id,
                "title": format!("{module_id} module"),
                "route": format!("/console/modules/{module_id}"),
                "capabilities": {
                    "can_render": true,
                    "can_subscribe_activity": true,
                }
            })
        })
        .collect::<Vec<_>>();

    // P0 fix: Build sidebar from the full mob roster (members) when a mob
    // runtime is present, so multi-instance profiles (e.g. 5 profiles → 15
    // agents) enumerate every individual agent, not just profile-level IDs.
    // Fall back to loaded_modules for module-only runtimes.
    let has_roster_members = live_snapshot.has_mob_runtime && !live_snapshot.members.is_empty();
    let response_phases: BTreeMap<&str, &Option<String>> = live_snapshot
        .agents
        .iter()
        .filter_map(|agent| {
            agent.response_phase.as_ref().map(|phase| {
                (
                    agent
                        .identity
                        .as_deref()
                        .unwrap_or(agent.member_id.as_str()),
                    phase,
                )
            })
        })
        .collect();
    let sidebar_agents: Vec<Value> = if has_roster_members {
        let mut sorted_members: Vec<&ConsoleMember> = live_snapshot.members.iter().collect();
        sorted_members.sort_by(|a, b| a.agent_identity.cmp(&b.agent_identity));
        sorted_members
                .iter()
                .map(|member| {
                    let console_identity = console_member_console_identity(member);
                    let label = member
                        .labels
                        .get("display_name")
                        .cloned()
                        .unwrap_or_else(|| member.agent_identity.clone());
                    let is_active = member.state == "active";
                    let addressable = is_active
                        && member
                        .labels
                        .get("addressable")
                        .map(|v| v != "false")
                        .unwrap_or(true);
                    let can_send_message = addressable && !is_read_only;
                    let watched = member
                        .labels
                        .get("console_watched")
                        .map(|value| value == "true");
                    let alert_level = member
                        .labels
                        .get("console_alert_level")
                        .filter(|value| matches!(value.as_str(), "elevated" | "critical"))
                        .cloned();
                    let degraded = member
                        .labels
                        .get("console_degraded")
                        .map(|value| value == "true");
                    let degraded_reason = member.labels.get("console_degraded_reason").cloned();
                    let singleton = member
                        .labels
                        .get("singleton")
                        .map(|v| v == "true")
                        .unwrap_or(false);
                    let group = member
                        .labels
                        .get("group")
                        .cloned()
                        .unwrap_or_else(|| member.role.clone());
                    let mut record = serde_json::json!({
                        "agent_id": member.agent_identity,
                        "member_id": member.agent_identity,
                        "identity": console_identity,
                        "label": label,
                        "kind": "mob_agent",
                        "role": member.role,
                        "state": member.state,
                        "progress": member.progress,
                        "model_capabilities": member.model_capabilities,
                        "session_id": member.session_id,
                        "wired_to": member.wired_to,
                        "labels": member.labels,
                        "group": group,
                        "addressable": addressable,
                        "watched": watched,
                        "alertLevel": alert_level,
                        "degraded": degraded,
                        "degradedReason": degraded_reason,
                        "affordances": {
                            "addressable": addressable,
                            "can_send_message": can_send_message,
                            "can_retire": is_active && !is_read_only && !is_aggregate_console && !singleton,
                            "can_respawn": !is_read_only && !is_aggregate_console,
                            "runtime_mode": if is_aggregate_console { "console_aggregator" } else { "mob_agent" },
                        },
                    });
                    if let Some(phase) = response_phases.get(console_identity) {
                        record["response_phase"] = serde_json::json!(phase);
                    }
                    record
                })
                .collect()
    } else {
        live_snapshot
            .loaded_modules
            .iter()
            .map(|module_id| {
                serde_json::json!({
                    "agent_id": module_id,
                    "member_id": module_id,
                    "label": module_id,
                    "kind": "module_agent",
                })
            })
            .collect()
    };

    let sidebar_agents: Vec<Value> = if has_roster_members {
        sidebar_agents
    } else if live_snapshot.agents.iter().any(has_extended_agent_contract) {
        live_snapshot
            .agents
            .iter()
            .map(|agent| {
                let mut record = serde_json::Map::new();
                record.insert(
                    "agent_id".to_string(),
                    Value::String(agent.agent_id.clone()),
                );
                record.insert(
                    "member_id".to_string(),
                    Value::String(agent.member_id.clone()),
                );
                record.insert("label".to_string(), Value::String(agent.label.clone()));
                record.insert("kind".to_string(), Value::String(agent.kind.clone()));
                if let Some(role) = &agent.role {
                    record.insert("role".to_string(), Value::String(role.clone()));
                }
                if let Some(state) = &agent.state {
                    record.insert("state".to_string(), Value::String(state.clone()));
                }
                if let Some(identity) = &agent.identity {
                    record.insert("identity".to_string(), Value::String(identity.clone()));
                }
                if let Some(session_id) = &agent.session_id {
                    record.insert("session_id".to_string(), Value::String(session_id.clone()));
                }
                record.insert(
                    "model_capabilities".to_string(),
                    serde_json::to_value(&agent.model_capabilities).unwrap_or(Value::Null),
                );
                if let Some(response_phase) = &agent.response_phase {
                    record.insert(
                        "response_phase".to_string(),
                        serde_json::json!(response_phase),
                    );
                }
                if is_aggregate_console {
                    record.insert("addressable".to_string(), Value::Bool(true));
                    record.insert(
                        "affordances".to_string(),
                        serde_json::json!({
                            "addressable": true,
                            "can_send_message": !is_read_only,
                            "can_retire": false,
                            "can_respawn": false,
                            "runtime_mode": "console_aggregator",
                        }),
                    );
                }
                if let Some(watched) = agent.watched {
                    record.insert("watched".to_string(), Value::Bool(watched));
                }
                if let Some(alert_level) = &agent.alert_level {
                    record.insert("alertLevel".to_string(), Value::String(alert_level.clone()));
                }
                if let Some(degraded) = agent.degraded {
                    record.insert("degraded".to_string(), Value::Bool(degraded));
                }
                if let Some(degraded_reason) = &agent.degraded_reason {
                    record.insert(
                        "degradedReason".to_string(),
                        Value::String(degraded_reason.clone()),
                    );
                }
                Value::Object(record)
            })
            .collect()
    } else {
        sidebar_agents
    };

    let has_mob = live_snapshot.has_mob_runtime && !is_aggregate_console;
    let can_send_messages = !is_read_only && (has_mob || is_aggregate_console);
    let can_spawn_members = !is_read_only && has_mob && !is_aggregate_console;
    let can_wire_members = !is_read_only && has_mob && !is_aggregate_console;
    let can_retire_members = !is_read_only && has_mob && !is_aggregate_console;
    let identity_status_rows = build_identity_status_rows(&sidebar_agents);

    // P3: Build per-profile capability hints from roster data.
    let profile_capabilities: BTreeMap<String, Value> = {
        let mut profiles: BTreeMap<String, (usize, bool, bool)> = BTreeMap::new();
        for member in &live_snapshot.members {
            let entry = profiles
                .entry(member.role.clone())
                .or_insert((0, true, false));
            entry.0 += 1; // instance_count
            // addressable = all instances addressable
            let member_addressable = member
                .labels
                .get("addressable")
                .map(|v| v != "false")
                .unwrap_or(true);
            entry.1 = entry.1 && member_addressable;
            // has_wiring = any instance wired
            entry.2 = entry.2 || !member.wired_to.is_empty();
        }
        profiles
            .into_iter()
            .map(|(profile, (count, addressable, has_wiring))| {
                (
                    profile,
                    serde_json::json!({
                        "instance_count": count,
                        "addressable": addressable,
                        "has_wiring": has_wiring,
                    }),
                )
            })
            .collect()
    };

    let console_title = console_config.title.as_deref().unwrap_or("Mob Console");
    let mut body = serde_json::json!({
        "contract_version": META_CONTRACT_VERSION,
        "runtime_id": live_snapshot.runtime_id,
        "console_config": console_config,
        // HTTP has no authenticated, summary-seeded voice host yet. Generic
        // live/model capabilities must never authorize microphone acquisition.
        "voice": { "available": false },
        "runtime_capabilities": {
            "can_spawn_members": can_spawn_members,
            "can_send_messages": can_send_messages,
            "can_wire_members": can_wire_members,
            "can_retire_members": can_retire_members,
            "available_spawn_modes": if can_spawn_members {
                vec!["module", "role"]
            } else {
                vec!["module"]
            },
            "profile_capabilities": profile_capabilities,
        },
        "base_panel": {
            "panel_id": "console.home",
            "title": console_title,
            "route": CONSOLE_EXPERIENCE_ROUTE,
            "capabilities": {
                "can_render": true,
                "surface": "console",
            }
        },
        "module_panels": module_panels,
        "agent_sidebar": {
            "panel_id": "console.agent_sidebar",
            "title": "Agents",
            "schema_version": "1",
            "refresh": {
                "mode": "poll",
                "interval_ms": 5000,
            },
            "source_method": if is_aggregate_console { "meta/console/list_identities" } else if has_mob { "meta/list_members" } else { "meta/status" },
            "refresh_policy": {
                "mode": "pull",
                "poll_interval_ms": 5000,
            },
            "selection_contract": {
                "selected_agent_id_field": "agent_id",
                "selected_member_id_field": "member_id",
                "emits_scope": "agent",
                "supported_scopes": ["mob", "agent"],
            },
            "list_item_contract": {
                "fields": ["agent_id", "member_id", "identity", "label", "kind", "role", "state", "progress", "model_capabilities", "response_phase", "wired_to", "labels", "group", "addressable", "affordances", "watched", "alertLevel", "degraded", "degradedReason"],
                "agent_id_field": "agent_id",
                "member_id_field": "member_id",
                "group_by_field": "group",
                "well_known_labels": {
                    "display_name": "human-readable label; overrides member_id in sidebar",
                    "addressable": "set \"false\" to hide send-message actions for internal agents",
                    "singleton": "set \"true\" to prevent retire (e.g. review, summarizer agents)",
                    "group": "sidebar group name; overrides profile-based grouping",
                },
                "refresh_projection": "source_method returns ConsoleMember rows (agent_identity, role, state, model_capabilities, wired_to, labels). Clients must project: agent_id=agent_identity, member_id=agent_identity, identity=labels.agent_identity||agent_identity, label=labels.display_name||agent_identity, group=labels.group||role, addressable=labels.addressable!='false', model_capabilities.image_input default false, affordances derived from labels.singleton and addressable.",
            },
            "live_snapshot": {
                "agents": sidebar_agents,
            }
        },
        "identity_status": {
            "panel_id": "console.identity_status",
            "title": "Identity Status",
            "schema_version": "1",
            "refresh": {
                "mode": "poll",
                "interval_ms": 5000,
            },
            "source_method": if is_aggregate_console { "meta/console/list_identities" } else if has_mob { "meta/list_members" } else { "meta/status" },
            "rows": identity_status_rows,
        },
        "activity_feed": {
            "panel_id": "console.activity_feed",
            "title": "Activity",
            "schema_version": "1",
            "refresh": {
                "mode": "stream",
                "topic": "all_events",
                "update_semantics": "append",
            },
            "transport": "sse",
            "source_route": "/console/timeline/stream",
            "request_contract": {
                "last_event_id_header": "optional Last-Event-ID checkpoint from prior event_id",
            },
            "event_contract": {
                "envelope_fields": ["event_id", "interaction_id", "identity", "event_type", "timestamp_ms", "data"],
                "event_type_path": "event_type",
                "frame_format": "id: <event_id>\\nevent: <event_type>\\ndata: <event_json>\\n\\n",
            },
            "keep_alive": {
                "interval_ms": SSE_KEEP_ALIVE_INTERVAL_MS,
                "event": SSE_KEEP_ALIVE_EVENT_NAME,
                "comment_frame": SSE_KEEP_ALIVE_COMMENT_FRAME,
            },
            "filter_presets": [
                { "id": "all", "label": "All" },
                { "id": "watched-only", "label": "Watched only", "watchedOnly": true },
                { "id": "critical", "label": "Critical", "alertLevels": ["critical"] }
            ],
            "active_preset_id": "all"
        },
        "chat_inspector": {
            "panel_id": "console.chat_inspector",
            "title": "Chat Inspector",
            "schema_version": "1",
            "refresh": {
                "mode": "stream",
                "topic": "selected_identity",
                "update_semantics": "append",
            },
            "send_method": "meta/console/send",
            "observe_route": "/console/timeline/stream",
            "transport": "rpc+sse",
            "request_contract": {
                "identity": "required target identity",
                "content": "required user text to send",
                "origin": "required caller identifier for audit and routing",
            },
            "response_contract": {
                "interaction_id": "per-turn console correlation token",
                "identity": "echoed target identity",
            },
            "event_contract": {
                "envelope_fields": ["event_id", "interaction_id", "identity", "event_type", "timestamp_ms", "data"],
                "event_type_path": "event_type",
                "interaction_id_field": "interaction_id",
            }
        },
        "topology": {
            "panel_id": "console.topology",
            "title": "Topology",
            "schema_version": "1",
            "refresh": {
                "mode": "poll",
                "interval_ms": 5000,
            },
            "source_method": if is_aggregate_console { "meta/console/list_identities" } else { "meta/status" },
            "route_method": if is_aggregate_console { Value::Null } else { serde_json::json!("meta/routing/routes/list") },
            "refresh_policy": {
                "mode": "pull",
                "poll_interval_ms": 5000,
            },
            "graph_contract": {
                "node_id_field": "identity",
                "edge_fields": ["wired_to"],
            },
            "live_snapshot": if live_snapshot.members.is_empty() {
                // Module-only runtime: topology nodes are loaded module IDs.
                serde_json::json!({
                    "nodes": &live_snapshot.loaded_modules,
                    "node_count": live_snapshot.loaded_modules.len(),
                })
            } else {
                // Mob runtime: identity-native topology from members.
                serde_json::json!({
                    "nodes": live_snapshot.members.iter().map(|member| {
                        let addressable = member.state == "active" && member
                            .labels
                            .get("addressable")
                            .map(|value| value != "false")
                            .unwrap_or(true);
                        serde_json::json!({
                            "identity": member.agent_identity,
                            "label": member.labels.get("display_name").cloned().unwrap_or_else(|| member.agent_identity.clone()),
                            "role": member.role,
                            "state": member.state,
                            "wired_to": member.wired_to,
                            "addressable": addressable,
                        })
                    }).collect::<Vec<_>>(),
                    "node_count": live_snapshot.members.len(),
                })
            }
        },
        "health_overview": {
            "panel_id": "console.health_overview",
            "title": "Health",
            "schema_version": "1",
            "refresh": {
                "mode": "poll",
                "interval_ms": 5000,
            },
            "source_method": if is_aggregate_console { "meta/console/list_identities" } else { "meta/status" },
            "activity_source_method": if is_aggregate_console { Value::Null } else { serde_json::json!(EVENTS_SUBSCRIBE_METHOD) },
            "activity_source_route": if is_aggregate_console { serde_json::json!("/console/timeline/stream") } else { Value::Null },
            "refresh_policy": {
                "mode": "pull_and_stream",
                "poll_interval_ms": 5000,
            },
            "status_contract": {
                "running_field": "running",
                "loaded_modules_field": "loaded_modules",
            },
            "live_snapshot": {
                "running": live_snapshot.running,
                "loaded_modules": &live_snapshot.loaded_modules,
                "loaded_module_count": &live_snapshot.loaded_modules.len(),
                "identities": identity_status_rows,
            }
        },
        "flows": serde_json::json!({
            "panel_id": "console.flows",
            "title": "Flows",
            "schema_version": "1",
            "available": false,
            "reason": "static flow scheduling is not exposed; use the durable schedule service and schedule tools",
        }),
        "session_history": if has_mob {
            serde_json::json!({
                "panel_id": "console.session_history",
                "title": "Session History",
                "schema_version": "1",
                "refresh": {
                    "mode": "poll",
                    "interval_ms": 5000,
                },
                "source_method": "meta/console/query_timeline",
                "transport": "rpc",
                "available": true,
                "request_contract": {
                    "session_id": "required current session id for the identity/member",
                    "offset": "optional message offset from the start of the transcript",
                    "limit": "optional max messages to return",
                },
                "response_contract": {
                    "success": "result is SessionHistoryPage {session_id, message_count, offset, limit, has_more, messages}",
                }
            })
        } else {
            serde_json::json!({
                "panel_id": "console.session_history",
                "title": "Session History",
                "schema_version": "1",
                "refresh": {
                    "mode": "poll",
                    "interval_ms": 5000,
                },
                "available": false,
                "reason": "session history is projected through the console timeline",
            })
        }
    });
    if let Some(fetch_timeout_ms) = console_policy.fetch_timeout_ms {
        body["console_policy"] = serde_json::json!({
            "fetch_timeout_ms": fetch_timeout_ms,
        });
    }
    if is_read_only && let Some(object) = body.as_object_mut() {
        let policy = object
            .entry("console_policy")
            .or_insert_with(|| serde_json::json!({}));
        policy["read_only"] = Value::Bool(true);
    }
    body
}

fn build_identity_status_rows(sidebar_agents: &[Value]) -> Vec<Value> {
    sidebar_agents
        .iter()
        .map(|agent| {
            let identity = agent
                .get("identity")
                .and_then(Value::as_str)
                .or_else(|| agent.get("member_id").and_then(Value::as_str))
                .unwrap_or_default();
            let display_name = agent
                .get("label")
                .and_then(Value::as_str)
                .filter(|label| *label != identity);
            let role = agent.get("role").and_then(Value::as_str);
            let state = agent
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let labels = agent
                .get("labels")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let addressability = if agent
                .get("addressable")
                .and_then(Value::as_bool)
                .unwrap_or(true)
            {
                "addressable"
            } else {
                "internal_only"
            };
            let mut row = serde_json::json!({
                "identity": identity,
                "state": state,
                "addressability": addressability,
                "labels": labels,
            });
            if let Some(display_name) = display_name {
                row["display_name"] = Value::String(display_name.to_string());
            }
            if let Some(role) = role {
                row["role"] = Value::String(role.to_string());
            }
            if let Some(generation) = agent.get("generation").and_then(Value::as_u64) {
                row["generation"] = Value::from(generation);
            }
            if let Some(checkpoint_version) =
                agent.get("checkpoint_version").and_then(Value::as_u64)
            {
                row["checkpoint_version"] = Value::from(checkpoint_version);
            }
            if let Some(lease_healthy) = agent.get("lease_healthy").and_then(Value::as_bool) {
                row["lease_healthy"] = Value::from(lease_healthy);
            }
            // Machine-owned liveness projection (meerkat 0.7.29+, ask 14).
            if let Some(progress) = agent.get("progress").filter(|value| !value.is_null()) {
                row["progress"] = progress.clone();
            }
            if let Some(response_phase) = agent.get("response_phase") {
                row["response_phase"] = response_phase.clone();
            }
            if let Some(model_capabilities) = agent.get("model_capabilities") {
                row["model_capabilities"] = model_capabilities.clone();
            }
            if let Some(session_id) = agent.get("session_id").and_then(Value::as_str) {
                row["session_id"] = Value::String(session_id.to_string());
            }
            row
        })
        .collect()
}

fn split_path_and_query(path: &str) -> (&str, BTreeMap<String, String>) {
    let (base, query) = path.split_once('?').unwrap_or((path, ""));
    let mut params = BTreeMap::new();
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        if !k.is_empty() {
            params.insert(k.to_string(), v.to_string());
        }
    }
    (base, params)
}

fn resolve_console_auth(
    decisions: &RuntimeDecisionState,
    explicit_auth: Option<&ConsoleAccessRequest>,
    query_params: &BTreeMap<String, String>,
) -> Result<Option<ConsoleAccessRequest>, ConsoleAuthResolutionError> {
    if let Some(auth) = explicit_auth {
        return Ok(Some(auth.clone()));
    }

    if !decisions.console.require_app_auth {
        return Ok(None);
    }

    // Check query-param auth_token (also used as the bearer-header injection
    // point by the HTTP handler — see console_json_handler).
    match query_params.get("auth_token") {
        Some(token) => resolve_console_auth_from_token(decisions, token).map(Some),
        None => Ok(None),
    }
}

/// Extract a bearer token from an `Authorization: Bearer <token>` header value.
pub fn extract_bearer_token_from_header(header_value: &str) -> Option<&str> {
    let token = header_value.strip_prefix("Bearer ")?;
    if token.is_empty() { None } else { Some(token) }
}

/// Validate a bearer token against the console's trusted OIDC config AND
/// the email allowlist / provider policy (via `enforce_console_route_access`).
/// Returns `true` only if the token is valid AND the caller is authorized.
pub fn validate_console_token(decisions: &RuntimeDecisionState, token: &str) -> bool {
    resolve_authorized_console_auth_from_token(decisions, token).is_some()
}

pub(crate) fn resolve_authorized_console_auth_from_token(
    decisions: &RuntimeDecisionState,
    token: &str,
) -> Option<ConsoleAccessRequest> {
    resolve_authorized_console_human_from_token(decisions, token).map(|(auth, _)| auth)
}

/// [`resolve_authorized_console_auth_from_token`] that also returns the
/// verified human behind the token, when there is one.
pub(crate) fn resolve_authorized_console_human_from_token(
    decisions: &RuntimeDecisionState,
    token: &str,
) -> Option<(ConsoleAccessRequest, Option<AuthenticatedConsoleHuman>)> {
    resolve_authorized_console_caller_from_token(decisions, token)
        .map(|caller| (caller.auth, caller.human))
}

/// A caller the Console resolved from a token it verified and authorized.
pub(crate) struct ResolvedConsoleCaller {
    /// The access principal and provider classification the Console's
    /// policy checked.
    pub(crate) auth: ConsoleAccessRequest,
    /// The issuer the token verified against: the Console's trusted OIDC
    /// issuer, as the verification used it.
    pub(crate) issuer: String,
    /// The verified human behind the token, when there is one.
    pub(crate) human: Option<AuthenticatedConsoleHuman>,
}

/// [`resolve_authorized_console_human_from_token`] that also carries the
/// issuer the token verified against out of that verification.
pub(crate) fn resolve_authorized_console_caller_from_token(
    decisions: &RuntimeDecisionState,
    token: &str,
) -> Option<ResolvedConsoleCaller> {
    let caller = resolve_console_caller_from_token(decisions, token).ok()?;
    crate::decisions::enforce_console_route_access(
        &decisions.auth,
        &decisions.console,
        &caller.auth,
    )
    .ok()?;
    Some(caller)
}

/// The trust-domain prefix of a Console human's qualified principal: the
/// domain is this prefix followed by the exact verified OIDC issuer.
pub const CONSOLE_HUMAN_TRUST_DOMAIN_PREFIX: &str = "oidc-issuer:";

/// A human the Console authenticated from a verified OIDC token: the
/// verified issuer and `sub` (the OIDC stable identifier), the provider
/// classification and the token's expiry and digest.
///
/// Built only from a token the Console verified (signature, issuer,
/// audience, time) and authorized (allowlist and provider policy); there is
/// no other constructor and no serde. A service identity, or a token without
/// a `sub`, yields none. It identifies the caller; it grants nothing.
#[derive(Clone)]
pub struct AuthenticatedConsoleHuman {
    provider: AuthProvider,
    issuer: String,
    subject: String,
    expires_at_epoch_seconds: Option<u64>,
    token_digest: [u8; 32],
    /// The token's signed `sid`, exactly as signed: the issuer's opaque login
    /// association, when it asserts one.
    login_scope: Option<String>,
    /// The verifier that accepted the token, retained so the human can be
    /// re-verified after an await ([`Self::current`]); never serialized.
    verification: Option<Arc<ConsoleHumanVerification>>,
}

/// The accepted token and the Console decisions that verified it.
struct ConsoleHumanVerification {
    decisions: RuntimeDecisionState,
    token: String,
}

/// The verified Console human is no longer current: its retained token no
/// longer verifies under the verifier decisions it was accepted with (for
/// example it expired), or no longer names the same issuer and subject, or
/// the human was not built from a retained verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsoleHumanNotCurrent;

impl std::fmt::Display for ConsoleHumanNotCurrent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the Console human's authentication is not current")
    }
}

impl std::error::Error for ConsoleHumanNotCurrent {}

impl PartialEq for AuthenticatedConsoleHuman {
    fn eq(&self, other: &Self) -> bool {
        self.provider == other.provider
            && self.issuer == other.issuer
            && self.subject == other.subject
            && self.expires_at_epoch_seconds == other.expires_at_epoch_seconds
            && self.token_digest == other.token_digest
            && self.login_scope == other.login_scope
    }
}

impl Eq for AuthenticatedConsoleHuman {}

impl AuthenticatedConsoleHuman {
    /// The provider classification of the verified token.
    pub fn provider(&self) -> &AuthProvider {
        &self.provider
    }

    /// The verified OIDC issuer.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// The verified OIDC `sub`.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// The verified token's `exp`, when it has one.
    pub fn expires_at_epoch_seconds(&self) -> Option<u64> {
        self.expires_at_epoch_seconds
    }

    /// SHA-256 of the exact verified compact token, so a host can record
    /// which authentication observation it bound without retaining the
    /// token.
    pub fn token_digest(&self) -> &[u8; 32] {
        &self.token_digest
    }

    /// The opaque login association the verified issuer signed into the
    /// token (`sid`), exactly as signed; `None` when the token carries none
    /// or a malformed one (not a string, or blank). It is an issuer
    /// assertion within that issuer's namespace: not identity (see
    /// [`Self::principal`]), not a permission, and not a verdict that the
    /// login is current. A host that requires the association resolves it
    /// within its own issuer and subject, and refuses its absence; a token
    /// without one can still be a valid authenticated human.
    pub fn login_scope(&self) -> Option<&str> {
        self.login_scope.as_deref()
    }

    /// The human's qualified principal: a `Human` whose id is the verified
    /// `sub`, in the trust domain of the verified issuer
    /// ([`CONSOLE_HUMAN_TRUST_DOMAIN_PREFIX`] + issuer). This is the one
    /// qualification of a Console human, for the original it sends and the
    /// approvals it gives; it is never an alias of another domain's
    /// principal.
    pub fn principal(
        &self,
    ) -> Result<meerkat_core::PrincipalRef, meerkat_core::PrincipalContractError> {
        let domain = meerkat_core::TrustDomainId::new(format!(
            "{CONSOLE_HUMAN_TRUST_DOMAIN_PREFIX}{}",
            self.issuer
        ))?;
        meerkat_core::PrincipalRef::in_domain(
            meerkat_core::PrincipalKind::Human,
            self.subject.clone(),
            domain,
        )
    }

    /// Re-verify this human now, after any await: the retained token must
    /// still verify under the verifier decisions it was accepted with
    /// (signature, issuer, audience, expiry, and that snapshot's allowlist
    /// and provider policy) and still name the same issuer and subject.
    /// Returns the access principal (the email the Console's access policy
    /// keys) for a separate check against the live access policy. It does
    /// not observe a replaced login, nor replacement verifier, JWKS,
    /// allowlist or provider decisions made after the token was accepted.
    /// The digest and expiry alone are attribution, not current
    /// authorization.
    pub fn current(&self) -> Result<String, ConsoleHumanNotCurrent> {
        let verification = self.verification.as_ref().ok_or(ConsoleHumanNotCurrent)?;
        let (auth, human) = resolve_authorized_console_human_from_token(
            &verification.decisions,
            &verification.token,
        )
        .ok_or(ConsoleHumanNotCurrent)?;
        match human {
            Some(human)
                if human.issuer == self.issuer
                    && human.subject == self.subject
                    && human.login_scope == self.login_scope =>
            {
                Ok(auth.email)
            }
            _ => Err(ConsoleHumanNotCurrent),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(issuer: &str, subject: &str) -> Self {
        Self {
            provider: AuthProvider::GenericOidc,
            issuer: issuer.to_string(),
            subject: subject.to_string(),
            expires_at_epoch_seconds: None,
            token_digest: [0; 32],
            login_scope: None,
            verification: None,
        }
    }
}

impl std::fmt::Debug for AuthenticatedConsoleHuman {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthenticatedConsoleHuman")
            .field("provider", &self.provider)
            .field("issuer", &self.issuer)
            .field("login_scope", &self.login_scope.is_some())
            .finish_non_exhaustive()
    }
}

fn resolve_console_auth_from_token(
    decisions: &RuntimeDecisionState,
    token: &str,
) -> Result<ConsoleAccessRequest, ConsoleAuthResolutionError> {
    resolve_console_caller_from_token(decisions, token).map(|caller| caller.auth)
}

fn resolve_console_caller_from_token(
    decisions: &RuntimeDecisionState,
    token: &str,
) -> Result<ResolvedConsoleCaller, ConsoleAuthResolutionError> {
    if decisions.trusted_oidc.audience.trim().is_empty() {
        return Err(ConsoleAuthResolutionError::InvalidTrustedOidcConfig);
    }

    let discovery = parse_oidc_discovery_json(&decisions.trusted_oidc.discovery_json)
        .map_err(|_| ConsoleAuthResolutionError::InvalidTrustedOidcConfig)?;
    let jwks = parse_jwks_json(&decisions.trusted_oidc.jwks_json)
        .map_err(|_| ConsoleAuthResolutionError::InvalidTrustedOidcConfig)?;
    let header =
        inspect_jwt_header(token).map_err(|_| ConsoleAuthResolutionError::InvalidTokenHeader)?;

    if header.alg == "HS256"
        && !hs256_allowed_for_development_issuer(&discovery.issuer, &discovery.jwks_uri)
    {
        return Err(ConsoleAuthResolutionError::Hs256NotAllowed);
    }

    let key = select_jwk_for_token(&jwks, header.kid.as_deref(), &header.alg)
        .map_err(|_| ConsoleAuthResolutionError::JwksKeyNotFound)?;
    let verification_key = build_jwt_verification_key(key, &header.alg)
        .map_err(|_| ConsoleAuthResolutionError::InvalidJwksKeyMaterial)?;

    let now_epoch_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let claims = validate_jwt_with_verification_key(
        token,
        &verification_key,
        &JwtClaimsValidationConfig {
            issuer: Some(discovery.issuer.clone()),
            audience: Some(decisions.trusted_oidc.audience.clone()),
            now_epoch_seconds,
            leeway_seconds: 30,
        },
    )
    .map_err(|_| ConsoleAuthResolutionError::InvalidToken)?;

    let principal = if decisions.trusted_oidc.require_verified_email {
        // A public issuer mints tokens for unverified addresses too, and the
        // allowlist compares addresses: require the claim and never let a
        // `sub` stand in for one.
        let email = claims
            .email
            .ok_or(ConsoleAuthResolutionError::MissingTokenIdentity)?;
        if claims.email_verified != Some(true) {
            return Err(ConsoleAuthResolutionError::EmailNotVerified);
        }
        email
    } else {
        claims
            .email
            .clone()
            .or(claims.subject.clone())
            .ok_or(ConsoleAuthResolutionError::MissingTokenIdentity)?
    };
    let provider =
        if claims.actor_type.as_deref() == Some("service") || principal.starts_with("svc:") {
            AuthProvider::ServiceIdentity
        } else {
            match claims.provider.as_deref() {
                Some("google_oauth") => AuthProvider::GoogleOAuth,
                Some("github_oauth") => AuthProvider::GitHubOAuth,
                Some("generic_oidc") => AuthProvider::GenericOidc,
                _ => AuthProvider::GenericOidc,
            }
        };
    // Only a verified human `sub`, taken from the accepted claims (never
    // from the email-first access principal above), under a nonempty
    // verified issuer, is a Console human; a service identity never is,
    // whatever claims it carries.
    let human = match (&provider, claims.subject) {
        (AuthProvider::ServiceIdentity, _) | (_, None) => None,
        (_, Some(subject)) if subject.trim().is_empty() => None,
        _ if discovery.issuer.trim().is_empty() => None,
        (_, Some(subject)) => Some(AuthenticatedConsoleHuman {
            provider: provider.clone(),
            issuer: discovery.issuer.clone(),
            subject,
            expires_at_epoch_seconds: claims.expires_at_epoch_seconds,
            token_digest: {
                use sha2::Digest as _;
                sha2::Sha256::digest(token.as_bytes()).into()
            },
            // Exactly as signed; a blank one is no usable association.
            login_scope: claims.session_id.filter(|scope| !scope.trim().is_empty()),
            verification: Some(Arc::new(ConsoleHumanVerification {
                decisions: decisions.clone(),
                token: token.to_string(),
            })),
        }),
    };

    Ok(ResolvedConsoleCaller {
        auth: ConsoleAccessRequest {
            provider,
            email: principal,
        },
        issuer: discovery.issuer,
        human,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConsoleAuthResolutionError {
    InvalidTrustedOidcConfig,
    InvalidTokenHeader,
    JwksKeyNotFound,
    InvalidJwksKeyMaterial,
    InvalidToken,
    MissingTokenIdentity,
    Hs256NotAllowed,
    EmailNotVerified,
}

fn console_auth_error_reason(error: &ConsoleAuthResolutionError) -> &'static str {
    match error {
        ConsoleAuthResolutionError::InvalidTrustedOidcConfig => "invalid_trusted_oidc_config",
        ConsoleAuthResolutionError::InvalidTokenHeader => "invalid_token_header",
        ConsoleAuthResolutionError::JwksKeyNotFound => "jwks_key_not_found",
        ConsoleAuthResolutionError::InvalidJwksKeyMaterial => "invalid_jwks_key_material",
        ConsoleAuthResolutionError::InvalidToken => "invalid_token",
        ConsoleAuthResolutionError::MissingTokenIdentity => "missing_token_identity",
        ConsoleAuthResolutionError::Hs256NotAllowed => "hs256_not_allowed",
        ConsoleAuthResolutionError::EmailNotVerified => "email_not_verified",
    }
}

fn hs256_allowed_for_development_issuer(issuer: &str, jwks_uri: &str) -> bool {
    match (extract_uri_host(issuer), extract_uri_host(jwks_uri)) {
        (Some(issuer_host), Some(jwks_host)) => {
            is_development_host(issuer_host) && is_development_host(jwks_host)
        }
        _ => false,
    }
}

fn extract_uri_host(uri: &str) -> Option<&str> {
    let after_scheme = uri.split_once("://").map_or(uri, |(_, rest)| rest);
    let authority_with_path = after_scheme.split('/').next()?;
    let authority = authority_with_path
        .rsplit('@')
        .next()
        .unwrap_or(authority_with_path);
    if authority.is_empty() {
        return None;
    }

    if let Some(stripped) = authority.strip_prefix('[') {
        let (ipv6_host, _) = stripped.split_once(']')?;
        return if ipv6_host.is_empty() {
            None
        } else {
            Some(ipv6_host)
        };
    }

    let host = authority
        .split_once(':')
        .map_or(authority, |(hostname, _)| hostname);
    if host.is_empty() { None } else { Some(host) }
}

fn is_development_host(host: &str) -> bool {
    let lowercase = host.to_ascii_lowercase();
    lowercase == "localhost"
        || lowercase == "127.0.0.1"
        || lowercase == "::1"
        || lowercase.ends_with(".localhost")
}

fn auth_error_reason(error: &DecisionPolicyError) -> &'static str {
    match error {
        DecisionPolicyError::AuthProviderMismatch => "provider_mismatch",
        DecisionPolicyError::AuthProviderNotSupported => "provider_not_supported",
        DecisionPolicyError::EmailNotAllowlisted => "email_not_allowlisted",
        DecisionPolicyError::InvalidServiceIdentity => "invalid_service_identity",
        DecisionPolicyError::ServiceIdentityNotAllowlisted => "service_identity_not_allowlisted",
        _ => "policy_denied",
    }
}
