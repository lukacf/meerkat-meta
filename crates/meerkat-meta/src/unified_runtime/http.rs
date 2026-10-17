//! HTTP server and route assembly for the unified runtime.

use std::sync::Arc;
use std::time::Duration;

use axum::http::{HeaderMap, header};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use futures::StreamExt;
use meerkat_core::comms::EventStream;

use crate::console_aggregator::{
    ConsoleVisibilityPolicy, HideImplicitDelegateMembersConsoleVisibilityPolicy,
    MetaConsoleAggregator,
};
use crate::http_console::{
    console_frontend_router, console_json_router_with_runtime_events_and_policy,
};
use crate::http_flow_editor::protected_flow_editor_router_with_runtime_catalog;
use crate::http_sse::{
    agent_events_sse_router_with_access_and_priming, mob_events_sse_router_with_access_and_priming,
    mob_structural_events_sse_router_with_access_and_priming, workgraph_facts_sse_router,
};
use crate::runtime::RuntimeDecisionState;
use tower::limit::ConcurrencyLimitLayer;

use super::UnifiedRuntime;

async fn healthz_response(
    headers: HeaderMap,
    job_health_projection: Arc<std::sync::RwLock<Option<serde_json::Value>>>,
) -> axum::response::Response {
    let wants_json = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|item| item.trim().starts_with("application/json"))
        });
    if !wants_json {
        return "ok".into_response();
    }
    let projection = job_health_projection
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let public_projection = projection.map_or_else(
        || {
            serde_json::json!({
                "status": "ok",
                "detached_jobs": null
            })
        },
        |projection| {
            serde_json::json!({
                "status": projection
                    .get("status")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!("ok")),
                "detached_jobs": projection
                    .get("detached_jobs")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            })
        },
    );
    Json(public_projection).into_response()
}

/// Default cap for the stock reference app router.
///
/// SSE routes keep HTTP requests open for the lifetime of the subscription, so
/// a demo-scale cap such as 20 can make the console look frozen while streams
/// occupy all slots. Hosts that wrap Meta in their own axum service may still
/// choose a different outer limit, but the reference router itself defaults to
/// a ceiling high enough for real console usage.
pub const DEFAULT_REFERENCE_APP_MAX_CONCURRENT_REQUESTS: usize = 1024;

/// A runtime alias that is stale, or names no identity, has no member
/// stream to open: it answers as a missing member, not an internal error.
fn agent_event_target_error(
    agent_id: &str,
    error: crate::identity_first::IdentityRuntimeError,
) -> crate::MobRuntimeError {
    use crate::identity_first::IdentityRuntimeError;
    let missing = matches!(
        error,
        IdentityRuntimeError::StaleRuntimeAlias { .. } | IdentityRuntimeError::UnknownIdentity(_)
    ) || (crate::member_comms_id::is_reserved_generated_alias(agent_id)
        && matches!(error, IdentityRuntimeError::Internal(_)));
    if missing {
        crate::MobRuntimeError::Mob(meerkat_mob::MobError::MemberNotFound(
            meerkat_mob::AgentIdentity::from(agent_id),
        ))
    } else {
        crate::MobRuntimeError::InvalidConfig(error.to_string())
    }
}

async fn resolve_agent_event_member_id(
    handle: &meerkat_mob::MobHandle,
    agent_id: &str,
) -> meerkat_mob::ids::AgentIdentity {
    let direct = crate::member_comms_id::mob_member_id(agent_id);
    if handle.get_member(&direct).await.ok().flatten().is_some() {
        return direct;
    }
    handle
        .list_members_including_retiring()
        .await
        .into_iter()
        .find(|entry| {
            crate::member_comms_id::durable_identity_label(&entry.labels)
                .is_some_and(|identity| identity == agent_id)
        })
        .map(|entry| entry.agent_identity)
        .unwrap_or(direct)
}

async fn identity_event_alias_is_current(
    identity_runtime: &crate::identity_first::IdentityRuntime,
    identity: &crate::identity_first::AgentIdentity,
    expected_alias: &str,
) -> bool {
    identity_runtime.status(identity).await.is_ok_and(|status| {
        status.state == crate::identity_first::IdentityLifecycleState::Active
            && status
                .agent_runtime_id
                .as_ref()
                .is_some_and(|runtime_id| runtime_id.as_str() == expected_alias)
    })
}

/// Keep a per-agent event subscription bound to the exact identity generation
/// that was current when the HTTP request was admitted. A reset replaces that
/// generation while the lower-plane event stream can remain alive long enough
/// to emit late output; forwarding it through the durable/current alias would
/// make a stale owner appear authoritative.
fn generation_authoritative_agent_event_stream(
    mut event_stream: EventStream,
    mut identity_events: tokio::sync::broadcast::Receiver<
        crate::identity_first::runtime::IdentityEvent,
    >,
    // The stream is handed to the HTTP client: it holds the owning view.
    identity_runtime: crate::identity_first::PublicIdentityRuntime,
    identity: crate::identity_first::AgentIdentity,
    expected_alias: String,
) -> EventStream {
    Box::pin(async_stream::stream! {
        // Reset currently updates the continuity entry without guaranteeing a
        // state-change notification on the pre-reset identity channel. Poll
        // the cheap in-memory status snapshot as a liveness backstop so an
        // otherwise-idle SSE connection still closes promptly.
        let mut authority_check = tokio::time::interval(Duration::from_millis(250));
        authority_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                // Prefer lifecycle invalidation when both it and a stale
                // lower-plane event are ready in the same scheduler turn.
                biased;
                lifecycle = identity_events.recv() => {
                    match lifecycle {
                        Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            if !identity_event_alias_is_current(
                                &identity_runtime,
                                &identity,
                                &expected_alias,
                            )
                            .await
                            {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = authority_check.tick() => {
                    if !identity_event_alias_is_current(
                        &identity_runtime,
                        &identity,
                        &expected_alias,
                    )
                    .await
                    {
                        break;
                    }
                }
                event = event_stream.next() => {
                    let Some(event) = event else {
                        break;
                    };
                    // Reset marks the identity non-Active before publishing
                    // the replacement generation. Recheck on every payload so
                    // output racing that transition is dropped immediately,
                    // even before the eventual Active lifecycle event arrives.
                    if !identity_event_alias_is_current(
                        &identity_runtime,
                        &identity,
                        &expected_alias,
                    )
                    .await
                    {
                        break;
                    }
                    yield event;
                }
            }
        }
    })
}

impl UnifiedRuntime {
    /// Establish the host-wide console payload policy before building views.
    /// Redaction applies before persistence and broadcast. Identity and member
    /// visibility remain per-view, so a restricted view cannot erase another
    /// authorized view's history. A later call cannot replace this owner policy.
    pub fn initialize_console_projection_with_policy(
        &self,
        policy: Arc<dyn ConsoleVisibilityPolicy>,
    ) -> Result<(), &'static str> {
        let mut initialized = false;
        self.console_projection.get_or_init(|| {
            initialized = true;
            self.create_console_projection(policy)
        });
        if initialized {
            Ok(())
        } else {
            Err("console projection policy is already initialized")
        }
    }

    fn create_console_projection(
        &self,
        policy: Arc<dyn ConsoleVisibilityPolicy>,
    ) -> MetaConsoleAggregator {
        struct HostPayloadPolicy(Arc<dyn ConsoleVisibilityPolicy>);
        impl ConsoleVisibilityPolicy for HostPayloadPolicy {
            fn redact_payload(
                &self,
                frame: &crate::console_aggregator::NewConsoleFrame,
            ) -> Option<serde_json::Value> {
                self.0.redact_payload(frame)
            }
        }
        // The runtime's own projection is a back-edge: it holds the observer
        // mob runtime and the non-owning identity runtime.
        let aggregator = MetaConsoleAggregator::new(self.console_log_store());
        aggregator.register_runtime_handles_with_policy(
            "default",
            "",
            self.mob_runtime.clone(),
            self.identity_first_context
                .as_ref()
                .map(|context| Arc::clone(&context.runtime)),
            self.console_events(),
            Arc::new(HostPayloadPolicy(policy)),
        );
        aggregator
    }

    pub fn build_console_json_router(&self, decisions: RuntimeDecisionState) -> Router {
        self.build_console_json_router_with_policy(
            decisions,
            Arc::new(HideImplicitDelegateMembersConsoleVisibilityPolicy),
        )
    }

    pub fn build_console_json_router_with_policy(
        &self,
        decisions: RuntimeDecisionState,
        visibility_policy: Arc<dyn ConsoleVisibilityPolicy>,
    ) -> Router {
        // Existing single-policy hosts keep ingestion redaction. Hosts serving
        // different views can initialize an explicit host policy first.
        let projection = self
            .console_projection
            .get_or_init(|| self.create_console_projection(visibility_policy.clone()));
        let view = projection.policy_view(visibility_policy.clone());
        // The router is returned to the host: it holds the public owners.
        console_json_router_with_runtime_events_and_policy(
            decisions,
            self.mob_runtime().clone(),
            Some(self.module_runtime_handle()),
            self.contact_directory.clone(),
            self.event_log_store(),
            self.gateway_peer_keys().map(|keys| (*keys).clone()),
            Some(self.console_events()),
            Some(self.console_log_store()),
            Some(self.mob_events_store()),
            Some(Arc::clone(self.metadata_table())),
            self.identity_runtime(),
            visibility_policy,
            self.access_controller().cloned(),
            self.memory_panel_store(),
            self.console_operator_resolver(),
            self.console_identity_roster(),
            self.workgraph_service(),
            Some(self.topology_runtime_handle()),
            Some(Arc::clone(&self.job_health_projection)),
            Some(view),
            self.console_governed_ingress.clone(),
        )
    }

    /// Resolves Console viewers for host routes served beside the console,
    /// with the same decisions and access controller as the console router
    /// built from `decisions`.
    pub fn console_viewer_resolver(
        &self,
        decisions: &RuntimeDecisionState,
    ) -> crate::http_console::ConsoleViewerResolver {
        crate::http_console::ConsoleViewerResolver::new(
            decisions.clone(),
            self.access_controller().cloned(),
        )
    }

    /// The Console's stock send gate for a host route that prepares a
    /// Console send (see [`crate::http_console::ConsoleSendPrecheck`]): the
    /// same decisions, live roster and access controller as the console
    /// router built from `decisions`.
    pub fn console_send_precheck(
        &self,
        decisions: &RuntimeDecisionState,
    ) -> crate::http_console::ConsoleSendPrecheck {
        crate::http_console::ConsoleSendPrecheck::new(
            decisions.clone(),
            Some(self.mob_runtime().clone()),
            self.access_controller().cloned(),
        )
    }

    pub fn build_console_frontend_router(&self) -> Router {
        console_frontend_router()
    }

    pub fn build_reference_app_router(&self, decisions: RuntimeDecisionState) -> Router {
        self.build_reference_app_router_with_console_visibility_policy(
            decisions,
            Arc::new(HideImplicitDelegateMembersConsoleVisibilityPolicy),
        )
    }

    /// The reference router plus the live doors the builder registered.
    ///
    /// Composes the live doors once ([`UnifiedRuntime::compose_live`]), then
    /// mounts them beside the console: the console voice controller as the
    /// request extension the console RPC reads, and the live WebSocket
    /// router (`/live/ws`) when the external channel is registered. An
    /// embedder that serves this router on its own listener gets the same
    /// surface the gateway binaries expose.
    ///
    /// Fails closed when console voice is composed but
    /// `decisions.console.require_app_auth` is false: the controller serves
    /// only authenticated principals, and an open console would report voice
    /// as unavailable with no diagnostic. Without a registered live door the
    /// result equals [`Self::build_reference_app_router`].
    pub async fn build_reference_app_router_with_live(
        self: &Arc<Self>,
        decisions: RuntimeDecisionState,
    ) -> Result<Router, super::live_compose::LiveComposeError> {
        let composition = self.compose_live().await?;
        #[cfg(feature = "openai-live")]
        if composition
            .as_ref()
            .is_some_and(|composition| composition.console_voice().is_some())
            && !decisions.console.require_app_auth
        {
            return Err(super::live_compose::LiveComposeError::ConsoleVoiceRequiresAppAuth);
        }
        let router = self.build_reference_app_router(decisions);
        let Some(composition) = composition else {
            return Ok(router);
        };
        let router = if composition.external_handler().is_some() {
            router.merge(meerkat_live::live_ws_router(Arc::clone(
                &composition.context().ws_state,
            )))
        } else {
            router
        };
        #[cfg(feature = "openai-live")]
        let router = match composition.console_voice() {
            Some(controller) => router.layer(axum::Extension(controller.clone())),
            None => router,
        };
        Ok(router)
    }

    pub fn build_reference_app_router_with_console_visibility_policy(
        &self,
        decisions: RuntimeDecisionState,
        visibility_policy: Arc<dyn ConsoleVisibilityPolicy>,
    ) -> Router {
        // The router is returned to the host: it holds the public owners.
        let agent_runtime = self.mob_runtime().clone();
        let agent_identity_runtime = self.identity_runtime();
        let mob_runtime = self.mob_runtime().clone();
        // Every SSE route shares the same `RuntimeDecisionState` the
        // console RPC route uses: when `require_app_auth` is on, requests
        // must carry a valid bearer / auth_token. Pre-fix, only the
        // structural-events route gated; tier-2 (`/agents/{id}/events`)
        // and tier-3 (`/mob/events`) shipped unauthenticated.
        let flow_editor_decisions = decisions.clone();
        let flow_editor_runtime_catalog = self.mobpack_runtime_catalog_state_snapshot();
        let flow_editor_host_deploy = self.flow_editor_host_deploy;
        let sse_decisions_a = decisions.clone();
        let sse_decisions_b = decisions.clone();
        let sse_decisions_c = decisions.clone();
        let sse_decisions_d = decisions.clone();
        let access = self.access_controller().cloned();
        let agent_sse_visibility_policy = visibility_policy.clone();
        let mob_sse_visibility_policy = visibility_policy.clone();
        let structural_sse_visibility_policy = visibility_policy.clone();
        let job_health_projection = Arc::clone(&self.job_health_projection);
        Router::new()
            .route(
                "/healthz",
                get(move |headers: HeaderMap| {
                    let job_health_projection = Arc::clone(&job_health_projection);
                    healthz_response(headers, job_health_projection)
                }),
            )
            .merge(self.build_console_frontend_router())
            .merge(protected_flow_editor_router_with_runtime_catalog(
                flow_editor_decisions,
                flow_editor_runtime_catalog,
                access.clone(),
                flow_editor_host_deploy,
            ))
            .merge(self.build_console_json_router_with_policy(decisions, visibility_policy))
            .merge(agent_events_sse_router_with_access_and_priming(
                Arc::new(move |agent_id, cursor| {
                    let runtime = agent_runtime.clone();
                    let identity_runtime = agent_identity_runtime.clone();
                    Box::pin(async move {
                        let agent_id =
                            crate::member_comms_id::runtime_alias_str(&agent_id).into_owned();
                        let handle = runtime.handle();
                        let target = if let Some(identity_runtime) = identity_runtime.as_ref() {
                            identity_runtime
                                .runtime()
                                .member_alias_lifecycle_target(&agent_id)
                                .await
                                .map_err(|error| agent_event_target_error(&agent_id, error))?
                        } else {
                            None
                        };
                        if let Some(target) = target {
                            let Some(authority_runtime) = identity_runtime.clone() else {
                                return Err(crate::MobRuntimeError::InvalidConfig(
                                    "identity event target resolved without identity runtime"
                                        .to_string(),
                                ));
                            };
                            let authority_identity = authority_runtime
                                .identity_for_member_mutation(&agent_id)
                                .await
                                .ok_or_else(|| {
                                    crate::MobRuntimeError::InvalidConfig(format!(
                                        "identity event target lost authority before subscription: {agent_id}"
                                    ))
                                })?;
                            return crate::identity_first::IdentityRuntime::run_member_alias_targets_operation_tracked(
                                vec![target],
                                move || async move {
                                    // The lifecycle guard pins both the authorized identity
                                    // and its current alias. The core roster identity is not
                                    // the runtime alias used to fence this subscription.
                                    let expected_alias = authority_runtime
                                        .status(&authority_identity)
                                        .await
                                        .map_err(|error| error.to_string())?
                                        .agent_runtime_id
                                        .ok_or_else(|| {
                                            "identity event target has no current runtime alias"
                                                .to_string()
                                        })?
                                        .to_string();
                                    let identity_events = authority_runtime
                                        .subscribe(&authority_identity)
                                        .await
                                        .map_err(|error| error.to_string())?;
                                    let member_id = resolve_agent_event_member_id(
                                        &handle,
                                        authority_identity.as_str(),
                                    )
                                    .await;
                                    let subscription =
                                        subscribe_agent_events_resuming(&handle, &member_id, cursor)
                                            .await
                                            .map_err(|error| error.to_string())?;
                                    Ok(crate::http_sse::AgentEventReplay {
                                        epoch: subscription.epoch,
                                        stream: generation_authoritative_agent_event_stream(
                                            subscription.stream,
                                            identity_events,
                                            authority_runtime,
                                            authority_identity,
                                            expected_alias,
                                        ),
                                    })
                                },
                            )
                            .await
                            .map_err(|error| {
                                crate::MobRuntimeError::InvalidConfig(error.to_string())
                            });
                        }
                        let member_id = resolve_agent_event_member_id(&handle, &agent_id).await;
                        subscribe_agent_events_resuming(&handle, &member_id, cursor)
                            .await
                            .map(|subscription| crate::http_sse::AgentEventReplay {
                                epoch: subscription.epoch,
                                stream: subscription.stream,
                            })
                            .map_err(Into::into)
                    })
                }),
                Some(sse_decisions_a),
                access.clone(),
                Some(self.mob_runtime().clone()),
                self.identity_runtime_ref().cloned(),
                agent_sse_visibility_policy,
            ))
            .merge(mob_events_sse_router_with_access_and_priming(
                Arc::new(move || {
                    let mob_runtime = mob_runtime.clone();
                    Box::pin(async move { mob_runtime.handle().subscribe_mob_events().await })
                }),
                Some(sse_decisions_b),
                access.clone(),
                Some(self.mob_runtime().clone()),
                mob_sse_visibility_policy,
            ))
            .merge(mob_structural_events_sse_router_with_access_and_priming(
                self.mob_handle(),
                self.mob_events_store(),
                Some(sse_decisions_c),
                access.clone(),
                Some(self.mob_runtime().clone()),
                structural_sse_visibility_policy,
            ))
            .merge(self.workgraph_fact_hub().map_or_else(Router::new, |hub| {
                workgraph_facts_sse_router(hub, Some(sse_decisions_d), access)
            }))
            .layer(ConcurrencyLimitLayer::new(
                DEFAULT_REFERENCE_APP_MAX_CONCURRENT_REQUESTS,
            ))
    }
}

/// Subscribe to a member's agent events for an SSE connection: from the
/// cursor its `Last-Event-ID` names, or from the actor's earliest retained
/// event (meerkat #1236), so a client that connects after a run started
/// still receives it from `run_started` and a reconnect resumes after the
/// last event it received. A cursor from another sequence space (a restart
/// since) or ahead of it replays the current space from its earliest event.
async fn subscribe_agent_events_resuming(
    handle: &meerkat_mob::MobHandle,
    member_id: &meerkat_mob::AgentIdentity,
    cursor: meerkat_core::comms::SessionEventCursor,
) -> Result<meerkat_mob::AgentEventSubscription, meerkat_mob::MobError> {
    match handle.subscribe_agent_events_from(member_id, cursor).await {
        Err(meerkat_mob::MobError::AgentEventCursorRejected { .. })
            if cursor != meerkat_core::comms::SessionEventCursor::Earliest =>
        {
            handle
                .subscribe_agent_events_from(
                    member_id,
                    meerkat_core::comms::SessionEventCursor::Earliest,
                )
                .await
        }
        result => result,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use axum::http::{HeaderMap, header};
    use futures::StreamExt;
    use meerkat_client::TestClient;
    use meerkat_core::comms::EventStream;
    use meerkat_mob::{MobDefinition, MobRuntimeMode, ProfileName};

    use super::{
        DEFAULT_REFERENCE_APP_MAX_CONCURRENT_REQUESTS, generation_authoritative_agent_event_stream,
        healthz_response,
    };
    use crate::identity_first::{
        AgentAddressability, AgentIdentity, DurableAgentSpec, LocalContinuityStore,
        LocalLeaseProvider, MutableRosterProvider,
    };
    use crate::unified_runtime::UnifiedRuntimeBuilder;

    #[tokio::test]
    async fn host_console_payload_policy_is_explicit_and_cannot_be_replaced_by_later_views() {
        struct RedactHost;
        impl crate::console_aggregator::ConsoleVisibilityPolicy for RedactHost {
            fn redact_payload(
                &self,
                _: &crate::console_aggregator::NewConsoleFrame,
            ) -> Option<serde_json::Value> {
                Some(serde_json::json!({"host":"redacted"}))
            }
        }
        let definition = MobDefinition::from_toml(
            "[mob]\nid = \"host-policy-owner\"\n[profiles.worker]\nmodel = \"gpt-5.5\"\n",
        )
        .expect("definition");
        let runtime = UnifiedRuntimeBuilder::default()
            .definition(definition)
            .default_llm_client(Arc::new(TestClient::default()))
            .build()
            .await
            .expect("runtime");
        runtime
            .initialize_console_projection_with_policy(Arc::new(RedactHost))
            .expect("initial policy");
        assert!(
            runtime
                .initialize_console_projection_with_policy(Arc::new(
                    crate::console_aggregator::AllowAllConsoleVisibilityPolicy,
                ))
                .is_err(),
            "a later view must not replace the ingestion policy"
        );
        let first = runtime
            .console_projection
            .get()
            .expect("projection")
            .store();
        let decisions = crate::RuntimeDecisionState::local_console(
            crate::ConsolePolicy {
                require_app_auth: false,
                ..Default::default()
            },
            None,
        );
        drop(runtime.build_console_json_router_with_policy(
            decisions,
            Arc::new(crate::console_aggregator::AllowAllConsoleVisibilityPolicy),
        ));
        assert!(Arc::ptr_eq(
            &first,
            &runtime
                .console_projection
                .get()
                .expect("same owner")
                .store()
        ));
        let mut stream = runtime.console_projection.get().expect("owner").subscribe();
        let source = runtime
            .console_events()
            .append(
                "_system",
                None,
                "text_delta",
                serde_json::json!({"delta":"host-private-token"}),
            )
            .await;
        let projected = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let crate::console_aggregator::ConsoleTimelineEvent::ConsoleFrame { frame } =
                    stream.recv().await.expect("owner stream")
                    && frame.source_event_id.as_deref() == Some(source.event_id.as_str())
                {
                    break frame;
                }
            }
        })
        .await
        .expect("source reaches canonical owner");
        assert_eq!(projected.payload, serde_json::json!({"host":"redacted"}));
        assert_eq!(projected.status, crate::ConsoleFrameStatus::Redacted);
        let stored = first
            .frame_by_dedupe_key(&projected.dedupe_key)
            .await
            .expect("raw store")
            .expect("persisted");
        assert_eq!(
            stored.payload, projected.payload,
            "host redaction precedes both persistence and raw broadcast"
        );
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn multiple_console_routers_reuse_one_runtime_projection() {
        let definition = MobDefinition::from_toml(
            r#"
[mob]
id = "shared-console-router-owner"
[profiles.worker]
model = "gpt-5.5"
"#,
        )
        .expect("definition");
        let runtime = UnifiedRuntimeBuilder::default()
            .definition(definition)
            .default_llm_client(Arc::new(TestClient::default()))
            .build()
            .await
            .expect("runtime");
        let decisions = crate::runtime::RuntimeDecisionState::local_console(
            crate::ConsolePolicy {
                require_app_auth: false,
                ..Default::default()
            },
            None,
        );
        let _first = runtime.build_console_json_router(decisions.clone());
        let first = runtime
            .console_projection
            .get()
            .expect("projection")
            .store();
        let _second = runtime.build_console_json_router(decisions);
        let second = runtime
            .console_projection
            .get()
            .expect("same projection")
            .store();
        assert!(Arc::ptr_eq(&first, &second));
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn console_projection_tasks_release_replaced_and_shutdown_stores() {
        async fn assert_released(
            store: std::sync::Weak<crate::console_aggregator::InMemoryConsoleLogStore>,
        ) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while store.upgrade().is_some() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("terminated console tasks must release their old store");
        }

        let definition = MobDefinition::from_toml(
            r#"
[mob]
id = "console-projector-lifecycle"
[profiles.worker]
model = "gpt-5.5"
"#,
        )
        .expect("definition");
        let mut runtime = UnifiedRuntimeBuilder::default()
            .definition(definition)
            .default_llm_client(Arc::new(TestClient::default()))
            .build()
            .await
            .expect("runtime");
        let decisions = crate::runtime::RuntimeDecisionState::local_console(
            crate::ConsolePolicy {
                require_app_auth: false,
                ..Default::default()
            },
            None,
        );
        let old_store = Arc::new(crate::console_aggregator::InMemoryConsoleLogStore::new());
        let old_weak = Arc::downgrade(&old_store);
        runtime.set_console_log_store(old_store.clone());
        drop(runtime.build_console_json_router(decisions.clone()));
        drop(old_store);

        let new_store = Arc::new(crate::console_aggregator::InMemoryConsoleLogStore::new());
        let new_weak = Arc::downgrade(&new_store);
        runtime.set_console_log_store(new_store.clone());
        assert!(runtime.console_projection.get().is_none());
        assert_released(old_weak).await;
        drop(runtime.build_console_json_router(decisions.clone()));
        drop(runtime.build_console_json_router(decisions));
        drop(new_store);
        runtime.shutdown().await;
        drop(runtime);
        assert_released(new_weak).await;
    }

    #[test]
    fn reference_router_default_concurrency_allows_sse_fanout() {
        assert_eq!(DEFAULT_REFERENCE_APP_MAX_CONCURRENT_REQUESTS, 1024);
    }

    #[tokio::test]
    async fn healthz_preserves_text_probe_and_negotiates_structured_jobs() {
        let projection = Arc::new(std::sync::RwLock::new(Some(serde_json::json!({
            "status": "ok",
            "detached_jobs": {
                "queued": 1,
                "running": 2,
                "awaiting_members": 1,
                "stale_leases": 0,
                "needs_attention": 0,
                "pending_outbox_jobs": 0,
                "runtime_inbox_backlog": 0
            }
        }))));
        let text = healthz_response(HeaderMap::new(), Arc::clone(&projection)).await;
        assert_eq!(text.status(), axum::http::StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(text.into_body(), usize::MAX)
                .await
                .expect("text body"),
            "ok"
        );

        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, "application/json".parse().expect("accept"));
        let json = healthz_response(headers, projection).await;
        let body = axum::body::to_bytes(json.into_body(), usize::MAX)
            .await
            .expect("json body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(body["detached_jobs"]["running"], 2);
    }

    #[tokio::test]
    async fn durable_agent_event_stream_terminates_when_reset_replaces_generation() {
        let definition = MobDefinition::from_toml(
            r#"
[mob]
id = "generation-authoritative-agent-events"

[profiles.worker]
model = "gpt-5.5"
runtime_mode = "turn_driven"

[profiles.worker.tools]
comms = true
"#,
        )
        .expect("definition parses");
        let identity = AgentIdentity::parse("lead").expect("identity parses");
        let roster = Arc::new(MutableRosterProvider::new(vec![DurableAgentSpec {
            identity: identity.clone(),
            profile: ProfileName::from("worker"),
            addressability: AgentAddressability::Addressable,
            display_name: None,
            labels: BTreeMap::new(),
            context: None,
            additional_instructions: Vec::new(),
            initial_message: None,
            runtime_mode_override: Some(MobRuntimeMode::TurnDriven),
            backend: None,
            binding: None,
            placement: None,
        }]));
        let scratch = tempfile::tempdir().expect("scratch dir");
        let runtime = UnifiedRuntimeBuilder::default()
            .definition(definition)
            .continuity_store(Arc::new(
                LocalContinuityStore::in_memory().expect("continuity store"),
            ))
            .lease_provider(Arc::new(LocalLeaseProvider::new()))
            .roster_provider(roster)
            .scratch_dir(scratch.path())
            .identity_runtime_instance_id("generation-authoritative-agent-events")
            .default_llm_client(Arc::new(TestClient::default()))
            .build()
            .await
            .expect("identity-first runtime builds");

        let identity_runtime = runtime
            .identity_runtime()
            .expect("identity runtime installed");
        let expected_alias = identity_runtime
            .status(&identity)
            .await
            .expect("identity active")
            .agent_runtime_id
            .expect("active runtime alias")
            .as_str()
            .to_string();
        let identity_events = identity_runtime
            .subscribe(&identity)
            .await
            .expect("identity event subscription");
        // A pending lower-plane stream proves termination comes from the
        // identity generation gate, not from old-member channel teardown.
        let lower_plane: EventStream = Box::pin(futures::stream::pending());
        let mut guarded = generation_authoritative_agent_event_stream(
            lower_plane,
            identity_events,
            identity_runtime.clone(),
            identity.clone(),
            expected_alias,
        );

        let replacement = identity_runtime
            .reset(&identity)
            .await
            .expect("identity reset succeeds");
        assert_eq!(replacement.generation.get(), 1);
        let next = tokio::time::timeout(Duration::from_secs(5), guarded.next())
            .await
            .expect("generation gate terminates promptly");
        assert!(
            next.is_none(),
            "stream opened on the prior generation must terminate after reset"
        );

        let _ = runtime.mob_handle().stop().await;
    }

    /// The per-agent event stream answers member_not_found for a runtime alias
    /// that is not the identity's current incarnation, as for any member it
    /// cannot find, rather than an internal error.
    #[tokio::test]
    async fn a_stale_runtime_alias_has_no_member_event_stream() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let definition = MobDefinition::from_toml(
            r#"
[mob]
id = "stale-alias-agent-events"

[profiles.worker]
model = "gpt-5.5"
runtime_mode = "turn_driven"

[profiles.worker.tools]
comms = true
"#,
        )
        .expect("definition parses");
        let identity = AgentIdentity::parse("lead").expect("identity parses");
        let roster = Arc::new(MutableRosterProvider::new(vec![DurableAgentSpec {
            identity: identity.clone(),
            profile: ProfileName::from("worker"),
            addressability: AgentAddressability::Addressable,
            display_name: None,
            labels: BTreeMap::new(),
            context: None,
            additional_instructions: Vec::new(),
            initial_message: None,
            runtime_mode_override: Some(MobRuntimeMode::TurnDriven),
            backend: None,
            binding: None,
            placement: None,
        }]));
        let scratch = tempfile::tempdir().expect("scratch dir");
        let runtime = UnifiedRuntimeBuilder::default()
            .definition(definition)
            .continuity_store(Arc::new(
                LocalContinuityStore::in_memory().expect("continuity store"),
            ))
            .lease_provider(Arc::new(LocalLeaseProvider::new()))
            .roster_provider(roster)
            .scratch_dir(scratch.path())
            .identity_runtime_instance_id("stale-alias-agent-events")
            .default_llm_client(Arc::new(TestClient::default()))
            .build()
            .await
            .expect("identity-first runtime builds");
        let current = runtime
            .identity_runtime()
            .expect("identity runtime installed")
            .status(&identity)
            .await
            .expect("identity active")
            .agent_runtime_id
            .expect("active runtime alias")
            .as_str()
            .to_string();
        let decisions = crate::runtime::RuntimeDecisionState::local_console(
            crate::ConsolePolicy {
                require_app_auth: false,
                ..Default::default()
            },
            None,
        );
        let app = runtime.build_reference_app_router(decisions);
        let status = async |agent_id: &str| {
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/agents/{agent_id}/events"))
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response")
                .status()
        };

        assert_eq!(
            (
                status(&current).await,
                status("rt:lead:7").await,
                status("rt:nobody:0").await,
            ),
            (StatusCode::OK, StatusCode::NOT_FOUND, StatusCode::NOT_FOUND),
            "(current alias {current}, stale alias, alias of no identity)"
        );
        let _ = runtime.mob_handle().stop().await;
    }
}
