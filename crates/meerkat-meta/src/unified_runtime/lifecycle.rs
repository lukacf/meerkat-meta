//! Runtime lifecycle management — startup, shutdown, rediscovery, and periodic maintenance.

use std::future::Future;
use std::future::IntoFuture;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Bound on the mob actor's terminal teardown in [`UnifiedRuntime::shutdown`].
/// It runs after the mob stop quiesced members, inside the mob quiesce window
/// the published shutdown horizon already counts.
pub const MOB_TERMINAL_SHUTDOWN_BUDGET: Duration = Duration::from_secs(5);

/// Budget for joining supervisor cleanups retired by replacement. See
/// [`UnifiedRuntime::join_retired_supervisor_cleanups`] for why this is its own
/// value rather than a second spend of `drain_timeout`.
///
/// Public because it is a bounded phase INSIDE `UnifiedRuntime::shutdown`, and
/// the gateway advertises a shutdown horizon that must cover every such phase.
/// A budget the horizon gate cannot see is a budget that silently overruns it.
pub const RETIRED_SUPERVISOR_JOIN_BUDGET: Duration = Duration::from_secs(2);

use meerkat_mob::SpawnMemberSpec;
use tokio::sync::mpsc::error::TryRecvError;

use crate::mob_handle_runtime::MobRuntimeError;
use crate::runtime::RuntimeDecisionState;

use super::types::{
    IdentityAuthorityReleaseOutcome, MemberMcpSettlement, MobStopOutcome,
    MobTerminalShutdownOutcome, RediscoverReport, RetiredSupervisorCleanupOutcome,
    RetiredSupervisorKind, ShutdownDrainReport, UnifiedRuntimeError, UnifiedRuntimeRunReport,
    UnifiedRuntimeShutdownReport,
};
use super::{MobEventIngress, UnifiedRuntime, discovery_spec_to_spawn_spec};

/// The `HostLoopCrash` detail for a `run_failed` agent event.
///
/// meerkat's `RunFailed` carries its failure truth as the typed
/// `error_report { class, reason, message }` and the projected payload keeps
/// it, so name it here: for a non-retryable provider failure (a 401 on a bad
/// key) meerkat itself logs nothing at warn or above, which makes this ERROR
/// line the only place an operator reading logs learns WHY the run failed.
/// A payload without a report (a producer that predates it, or a
/// payload-less envelope) falls back to the bare event id the line always
/// carried.
fn run_failed_crash_error(event_id: &str, payload: Option<&serde_json::Value>) -> String {
    fn string_field(value: Option<&serde_json::Value>) -> Option<&str> {
        value.and_then(serde_json::Value::as_str)
    }
    let mut detail = format!("agent run failed (event_id: {event_id})");
    let Some(report) = payload.and_then(|payload| payload.get("error_report")) else {
        return detail;
    };
    if let Some(class) = string_field(report.get("class")) {
        detail.push_str(&format!(" class={class}"));
    }
    if let Some(reason_type) = string_field(
        report
            .get("reason")
            .and_then(|reason| reason.get("reason_type")),
    ) {
        detail.push_str(&format!(" reason={reason_type}"));
    }
    if let Some(message) = string_field(report.get("message")) {
        detail.push_str(&format!(": {message}"));
    }
    detail
}

impl UnifiedRuntime {
    /// Reset the mob and re-run discovery + edge reconciliation.
    ///
    /// Sequence:
    /// 1. `MobHandle::reset()` — retires all members, clears projections,
    ///    restarts MCP servers, returns mob to Running state
    /// 2. Re-runs the stored `Discovery` (with `Value::Null` context since
    ///    `PreSpawnHook` is consumed at boot and cannot be replayed)
    /// 3. Spawns discovered members via `spawn_many`
    /// 4. Clears managed dynamic edges (stale after reset)
    /// 5. Runs edge reconciliation if `EdgeDiscovery` is configured
    ///
    /// Returns `None` if no `Discovery` is configured (nothing to rediscover).
    pub async fn rediscover(&self) -> Result<Option<RediscoverReport>, MobRuntimeError> {
        match self.rediscover_inner().await {
            Ok(report) => Ok(report),
            Err(err) => {
                self.fire_error(super::types::ErrorEvent::RediscoverFailure {
                    error: format!("{err}"),
                });
                Err(err)
            }
        }
    }

    async fn rediscover_inner(&self) -> Result<Option<RediscoverReport>, MobRuntimeError> {
        let discovery = match &self.discovery {
            Some(d) => d,
            None => return Ok(None),
        };
        if self.identity_runtime_ref().is_some() {
            return Err(MobRuntimeError::InvalidConfig(
                "rediscover resets the whole mob and is unavailable with identity-first authority; use refresh_desired_topology"
                    .to_string(),
            ));
        }

        // 1. Reset the mob — retires all, clears state, returns to Running
        self.mob_runtime
            .handle()
            .reset()
            .await
            .map_err(MobRuntimeError::Mob)?;

        // 2. Re-run discovery (no pre-spawn context — PreSpawnHook is FnOnce)
        let specs = discovery.discover(serde_json::Value::Null).await;
        let spawn_specs: Vec<SpawnMemberSpec> =
            specs.iter().map(discovery_spec_to_spawn_spec).collect();
        let spawned: Vec<String> = spawn_specs.iter().map(|s| s.identity.to_string()).collect();

        // 3. Spawn discovered members (hook-aware variant fires post_spawn_hook)
        self.spawn_many(spawn_specs).await?;

        // 4. Clear stale managed edges (old topology is gone after reset)
        self.managed_dynamic_edges.write().await.clear();

        // 5. Reconcile edges
        let edges = self.reconcile_edges().await;

        Ok(Some(RediscoverReport { spawned, edges }))
    }

    pub async fn run<F>(
        &self,
        listener: tokio::net::TcpListener,
        decisions: RuntimeDecisionState,
        shutdown_signal: F,
    ) -> UnifiedRuntimeRunReport
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let app = self.build_reference_app_router(decisions);
        let serve = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal)
            .into_future();
        tokio::pin!(serve);
        let serve_result = loop {
            tokio::select! {
                result = &mut serve => break result,
                () = tokio::time::sleep(Duration::from_millis(25)) => {
                    let _ = self.drain_mob_agent_events().await;
                }
            }
        };
        let shutdown = self.shutdown().await;
        UnifiedRuntimeRunReport {
            serve_result,
            shutdown,
        }
    }

    pub async fn serve(
        &self,
        listener: tokio::net::TcpListener,
        decisions: RuntimeDecisionState,
    ) -> std::io::Result<()> {
        let app = self.build_reference_app_router(decisions);
        let serve = axum::serve(listener, app).into_future();
        tokio::pin!(serve);
        loop {
            tokio::select! {
                result = &mut serve => break result,
                () = tokio::time::sleep(Duration::from_millis(25)) => {
                    let _ = self.drain_mob_agent_events().await;
                }
            }
        }
    }

    /// Spawn a detached task that periodically drains mob agent events and
    /// projects them onto the ConsoleEventStore. Returns a [`tokio::task::JoinHandle`] -
    /// callers that manage graceful shutdown should abort it before stopping
    /// the runtime.
    ///
    /// Use this when embedding [`UnifiedRuntime`] inside a host-owned axum
    /// server (so [`Self::serve`]'s built-in drain loop isn't running).
    /// Without this task the mob event router fills up, agent turns never
    /// reach the console SSE stream, and event-log consumers miss events.
    pub fn spawn_event_drain_task(self: std::sync::Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(25)).await;
                if self.shutting_down.load(Ordering::SeqCst) {
                    break;
                }
                if let Err(err) = self.drain_mob_agent_events().await {
                    if matches!(err, UnifiedRuntimeError::RuntimeShuttingDown) {
                        break;
                    }
                    // Transient drain failures are logged but don't stop the
                    // task — the next tick will try again.
                    tracing::warn!(error = %err, "mob agent event drain tick failed");
                }
            }
        })
    }

    /// How long [`Self::shutdown`] may legitimately take.
    ///
    /// **Bound your wait on this, not on a generic stage timeout.** Shutdown
    /// covers two provider-callback windows, runtime event and mob drains,
    /// bounded RPC/HTTP/stdout phases, and process-reap margin. A caller that
    /// gives it less is not detecting a hang - it is interrupting work the
    /// runtime is bounded to finish, and abandoning the wait does not stop it:
    /// the shutdown keeps running while the caller proceeds to exit, which is
    /// how cleanup gets killed mid-flight.
    ///
    /// This exists because the number was already published, documented and
    /// arithmetically asserted - and two independent consumers still bounded it
    /// 4x and 11x too tightly on the same night (2026-08-31), each by applying
    /// an ordinary host stage timeout. A `pub const` in a module they had no
    /// reason to open was not discoverable at the point of use. This method is
    /// reachable from the value they already hold and are about to shut down.
    ///
    /// The stdio surface has always advertised its horizon over the wire via
    /// `stdio_shutdown_horizon_ms`; this is the embedder's equivalent.
    ///
    /// ```no_run
    /// # async fn example(runtime: meerkat_meta::UnifiedRuntime) {
    /// tokio::time::timeout(runtime.shutdown_horizon(), runtime.shutdown())
    ///     .await
    ///     .expect("shutdown exceeded its own published horizon");
    /// # }
    /// ```
    #[must_use]
    pub fn shutdown_horizon(&self) -> Duration {
        crate::gateway_composition::GATEWAY_RUNTIME_SHUTDOWN_TIMEOUT
    }

    /// Retained module cleanup as its process owners see it. `None` while the
    /// module runtime is busy (for example inside its own shutdown): an
    /// absent observation, never a settled one.
    pub fn try_module_cleanup_observation(
        &self,
    ) -> Option<crate::runtime::ModuleCleanupObservation> {
        self.module_runtime
            .try_lock()
            .ok()
            .map(|runtime| runtime.module_cleanup_observation())
    }

    pub async fn shutdown(&self) -> UnifiedRuntimeShutdownReport {
        self.shutting_down.store(true, Ordering::SeqCst);
        // Nothing is delivered once teardown starts: stop the mob
        // announcement dispatcher first, whichever surface composed it.
        // Stopping is idempotent; a gateway may already have stopped it.
        if let Some(announcements) = self.mob_announcements() {
            announcements.stop();
        }
        // The liveness probe is observation only. Abort it before anything
        // can quiesce the mob actor so an intentional shutdown stall cannot
        // page a false ActorLoopStalled.
        if let Some(task) = self.actor_loop_probe_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        // The fact tail owns only a lossy wake projection, so shutdown does
        // not drain it. Abort and join the sole runtime-owned task before
        // taking down the authoritative WorkGraph-bearing mob runtime.
        if let Some(task) = self.workgraph_fact_tail_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(observer) = self.agent_memory_observer_task.lock().await.take() {
            observer.abort_and_join().await;
        }
        if let Some(task) = self.agent_memory_steward_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        // Reconnect probes are observation accelerants only, but they own
        // sockets and may be mid-authentication. Stop and join the sole task
        // before closing the listener or quiescing member/session authority.
        if let Some(task) = self.remote_host_reconnect_task.lock().await.take() {
            abort_and_join_remote_host_reconnect(task).await;
        }
        // Stop accepting cross-mob control RPC before the mob quiesces so a
        // late inbound wire/inject cannot race member teardown.
        if let Some(task) = self.cross_mob_control_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        let identity_runtime = self.identity_runtime_ref().cloned();
        if let Some(identity_runtime) = identity_runtime.as_ref() {
            // Close request admission before any supervisor is drained. A
            // caller may disappear while its lazy materialization owns an
            // uninstalled lease; the runtime, not the caller, owns that task
            // through its explicit commit/rollback boundary.
            identity_runtime.close_foreground_operations();
        }
        // A continuity repair pass owns the same serialized bootstrap
        // controller as explicit reconcile. Cancel it while idle, or join an
        // in-flight pass to its explicit lease/bridge commit boundary, before
        // waiting for background hydration.
        if let Some(task) = self.identity_continuity_repair_task.lock().await.take() {
            task.cancel_and_join().await;
        }
        if let Some(identity_runtime) = identity_runtime.as_ref() {
            // Background hydration owns concrete member creation/resume work;
            // stop and join it before quiescing the mob actor.
            identity_runtime.cancel_identity_bootstrap().await;
            // Foreground request tasks can share the same materialization and
            // lifecycle locks. Join them after the warmer has stopped and
            // before lease renewal or the mob actor is taken down.
            identity_runtime.join_foreground_operations().await;
        }
        if let Some(task) = self.implicit_delegate_retirement_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }

        // Reset commits the replacement continuity generation before the old
        // physical member can finish its archive protocol. Those exact
        // post-commit obligations live in a dedicated runtime-owned task set
        // and debt ledger: join them after foreground lifecycle admission is
        // closed, then synchronously retry every remaining pair before Mob
        // stop can observe a stale Retiring anchor.
        let mut reset_bridge_cleanup_error = None;
        if let Some(identity_runtime) = identity_runtime.as_ref() {
            identity_runtime.join_reset_bridge_cleanup_tasks().await;
            if let Err(error) = identity_runtime.drain_pending_reset_bridge_cleanups().await {
                tracing::warn!(
                    %error,
                    "reset-superseded bridge cleanup remains before mob shutdown"
                );
                reset_bridge_cleanup_error = Some(error.to_string());
            }
        }

        // Phase 1: Drain in-flight events
        let drain_start = std::time::Instant::now();
        // Phase 1 and the post-stop drain before Phase 3 share this window:
        // the published shutdown horizon counts one event drain.
        let drain_deadline = tokio::time::Instant::from_std(drain_start) + self.drain_timeout;
        // The console events this shutdown's drains project follow this
        // position; the post-stop drain makes each of them durable.
        let console_drain_position = self.console_events.latest_event_id().await;
        let mut drained_count = 0_usize;
        let drain_result = tokio::time::timeout(self.drain_timeout, async {
            loop {
                if self.drain_mob_agent_events().await.is_err() {
                    break;
                }
                let ingress = self.mob_event_ingress.lock().await;
                if ingress.is_none() {
                    break;
                }
                drop(ingress);
                drained_count += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
                if drained_count > 1 {
                    break;
                }
            }
        })
        .await;
        let drain = ShutdownDrainReport {
            drained_count,
            timed_out: drain_result.is_err(),
            drain_duration_ms: drain_start.elapsed().as_millis() as u64,
        };

        // Phase 2: Stop the mob actor while its router/module dependencies
        // are still alive. Closing them first can race Stop against an
        // already-dropped actor reply channel under teardown pressure.
        // A refusal stays Err: the gates below (grant release, terminal
        // teardown) are conservative on a mob that did not quiesce, because
        // releasing identity authority while a member is still live parks
        // Active identities Broken. Phases 3 and 4 continue either way.
        let mut mob_stop = self.stop_mob().await;
        // Shutdown is terminal: the delivery owner stops with the mob rather
        // than when the last runtime clone happens to drop. Undelivered rows
        // stay pending in the durable inbox.
        self.mob_runtime.stop_runtime_delivery();
        // Terminal as well: no member is built or admitted after Stop, and
        // the builder's tool slots otherwise keep the whole composition alive.
        self.mob_runtime.release_composition_slots();

        // A first cleanup attempt can fail while the Mob stop itself finishes
        // quiescing the old runtime. Retry the retained exact debt once more;
        // if it converges after a failed stop, retry Stop so cleanup attestation
        // reflects the final structural state rather than the first refusal.
        if reset_bridge_cleanup_error.is_some()
            && let Some(identity_runtime) = identity_runtime.as_ref()
        {
            match identity_runtime.drain_pending_reset_bridge_cleanups().await {
                Ok(_) => {
                    reset_bridge_cleanup_error = None;
                    if mob_stop.is_err() {
                        mob_stop = self.stop_mob().await;
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "reset-superseded bridge cleanup remains after mob shutdown retry"
                    );
                    reset_bridge_cleanup_error = Some(error.to_string());
                }
            }
        }

        // Fencing authority must outlive the physical members it protects.
        // Keep renewal running through mob quiescence, then stop it before the
        // final provider release so no renewal can race the release boundary.
        if let Some(task) = self.identity_lease_renewal_task.lock().await.take() {
            task.cancel_and_join().await;
        }
        let identity_authority_release = match identity_runtime.as_ref() {
            None => IdentityAuthorityReleaseOutcome::NotConfigured,
            Some(identity_runtime) if mob_stop.is_ok() && reset_bridge_cleanup_error.is_none() => {
                match identity_runtime.release_all_leases_for_shutdown().await {
                    Ok(grant_count) => IdentityAuthorityReleaseOutcome::Released { grant_count },
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "failed to release identity authority after mob shutdown"
                        );
                        IdentityAuthorityReleaseOutcome::Failed {
                            error: error.to_string(),
                        }
                    }
                }
            }
            Some(_) if mob_stop.is_ok() => {
                let error = reset_bridge_cleanup_error.unwrap_or_else(|| {
                    "reset bridge cleanup remained without an error detail".to_string()
                });
                tracing::warn!(
                    %error,
                    "retaining identity grants because reset bridge cleanup did not converge"
                );
                IdentityAuthorityReleaseOutcome::SkippedResetCleanupFailed { error }
            }
            Some(_) => {
                tracing::warn!(
                    "mob shutdown did not quiesce physical members; retaining identity grants"
                );
                IdentityAuthorityReleaseOutcome::SkippedMobStopFailed
            }
        };
        if mob_stop.is_ok() {
            // Break the MobRuntime <-> IdentityRuntime authority cycle only
            // after physical members are gone. This is required for failed
            // builders to release persistent topology/store locks before
            // returning Err; on a failed mob stop the authority and grants
            // deliberately remain intact.
            self.mob_runtime.clear_identity_runtime_authority();
            *self
                .implicit_delegate_identity_runtime
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        }

        // Durable child mobs share the root's terminal teardown window, so the
        // advertised shutdown horizon gains no new phase.
        let terminal_window_deadline = std::time::Instant::now() + MOB_TERMINAL_SHUTDOWN_BUDGET;

        // `stop` quiesces members but keeps the mob actor - and its
        // `{mob_id}/__mob_supervisor__` in-proc route - alive for resume.
        // UnifiedRuntime shutdown is terminal, and meerkat 0.8.23 refuses
        // route displacement, so a same-process cold replacement for this mob
        // id must observe the name actually freed. Drive the mob's own
        // terminal teardown (which retires the supervisor route) only AFTER
        // identity authority is released and detached: terminal member
        // teardown observed by a still-attached IdentityRuntime would park
        // Active identities Broken instead of leaving them Dormant.
        // Best-effort: a refusal leaves the route registered and is reported
        // loudly rather than failing the report.
        let mob_terminal_shutdown = if mob_stop.is_ok() {
            self.shutdown_mob_actor().await
        } else {
            MobTerminalShutdownOutcome::SkippedMobStopFailed
        };

        // Durable child mobs stand down after the root mob, which a failed
        // root stop skips because children may still be running. Their root
        // claim is released only after a proven stand-down.
        let child_mob_stand_down = if mob_stop.is_ok() {
            self.mob_runtime
                .stand_down_child_mobs(terminal_window_deadline)
                .await
        } else if self.mob_runtime.owns_child_mob_root() {
            crate::child_mob_storage::ChildMobStandDownOutcome::SkippedMobStopFailed
        } else {
            crate::child_mob_storage::ChildMobStandDownOutcome::NotConfigured
        };

        // The member teardown above is bounded below a stdio close's own
        // bound, and an agent's release of its MCP router is not a cleanup
        // receipt. Start every member MCP close in custody and wait for each
        // native outcome before any cleanup is reported: only a close that
        // reported settled counts as complete.
        let member_mcp_settlement = self.settle_member_mcp().await;

        // The mob stop and terminal teardown above end a member's in-flight
        // run after Phase 1 drained, so the run's terminal reaches the router
        // only now. Drain once more and make the console frames durable
        // before the router closes and the live projection detaches, or a
        // turn the shutdown interrupted never gets its terminal frame.
        self.drain_terminal_events_after_stop(drain_deadline, console_drain_position)
            .await;

        // Phase 3: Close event router
        self.close_event_router().await;

        // The runtime owns the console projector. Detach its live source after
        // draining runtime events so closed runtimes cannot retain discovery tasks.
        if let Some(projection) = self.console_projection.get() {
            projection.unregister_runtime("default");
        }

        // Phase 4: Shutdown modules
        let module_shutdown = self.module_runtime.lock().await.shutdown();
        let retired_supervisor_cleanup = self.join_retired_supervisor_cleanups().await;
        // The shutdown ran to its end: a later drop has nothing to hand over.
        // Entering shutdown alone never disarms the drop path.
        self.teardown_settled.store(true, Ordering::SeqCst);
        UnifiedRuntimeShutdownReport {
            drain,
            module_shutdown,
            mob_stop,
            identity_authority_release,
            retired_supervisor_cleanup,
            mob_terminal_shutdown,
            child_mob_stand_down,
            member_mcp_settlement,
        }
    }

    /// Shutdown's post-stop drain: at most two drain passes 50 ms apart, as
    /// in Phase 1, then the console projection of every event this
    /// shutdown's drains appended after `console_drain_position`, all
    /// within `deadline`. Projection dedupes on the source event id, so
    /// frames the live projection already wrote are not written twice.
    async fn drain_terminal_events_after_stop(
        &self,
        deadline: tokio::time::Instant,
        console_drain_position: Option<String>,
    ) {
        let drained = tokio::time::timeout_at(deadline, async {
            for pass in 0..2 {
                if pass > 0 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                if self.drain_mob_agent_events().await.is_err()
                    || self.mob_event_ingress.lock().await.is_none()
                {
                    break;
                }
            }
            let Some(projection) = self.console_projection.get() else {
                return;
            };
            let events = match self
                .console_events
                .replay_all(console_drain_position.as_deref())
                .await
            {
                Ok(events) => events,
                // The position aged out of the replay buffer: project all
                // the buffer still holds.
                Err(_) => self
                    .console_events
                    .replay_all(None)
                    .await
                    .unwrap_or_default(),
            };
            if let Err(error) = projection.project_runtime_events("default", events).await {
                tracing::warn!(
                    %error,
                    "shutdown could not persist every console frame of its final drain"
                );
            }
        })
        .await;
        if drained.is_err() {
            tracing::warn!(
                "shutdown's post-stop event drain reached the end of the event drain window"
            );
        }
    }

    /// The members' MCP servers, closed through the custody the gateway
    /// composed them under, with each close's native outcome. Each wait ends
    /// at the deadline Meerkat derives from the closes it started, within
    /// [`MEMBER_MCP_SETTLEMENT_BUDGET`](crate::member_mcp_custody::MEMBER_MCP_SETTLEMENT_BUDGET).
    /// Safe to repeat: a gateway's executor-exit boundary runs
    /// [`Self::settle_member_mcp_to_exit`] after it.
    pub(crate) async fn settle_member_mcp(&self) -> MemberMcpSettlement {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = self.mob_runtime.member_mcp_custody() {
            return custody.settle().await;
        }
        MemberMcpSettlement::NothingOwned
    }

    /// The member MCP settlement at a gateway's executor-exit boundary: as
    /// [`Self::settle_member_mcp`], except that a close still pending at its
    /// deadline is waited for until its native outcome reports. Nothing
    /// settles these closes after it, so no deadline ends its wait; a close
    /// that needed the wait still counts under `deadline`.
    pub(crate) async fn settle_member_mcp_to_exit(&self) -> MemberMcpSettlement {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = self.mob_runtime.member_mcp_custody() {
            return custody.settle_to_exit().await;
        }
        MemberMcpSettlement::NothingOwned
    }

    /// The mob actor's terminal teardown, once: meerkat 0.8.51's
    /// `shutdown_with_report` owns its convergence (stuck retirements, held
    /// unregisters and in-flight runs are settled or reported, not retried),
    /// bounded by [`MOB_TERMINAL_SHUTDOWN_BUDGET`].
    async fn shutdown_mob_actor(&self) -> MobTerminalShutdownOutcome {
        let deadline = std::time::Instant::now() + MOB_TERMINAL_SHUTDOWN_BUDGET;
        match self
            .mob_handle()
            .shutdown_with_report(meerkat_mob::ShutdownOptions::default().with_deadline(deadline))
            .await
        {
            Ok(report) => {
                if !report.is_clean() {
                    tracing::warn!(
                        members = ?report.members,
                        "mob terminal shutdown completed with members it could not settle"
                    );
                }
                MobTerminalShutdownOutcome::Completed(report)
            }
            // A mob shutting down answers every caller request with a closed
            // command channel: the actor is already gone, not failing.
            Err(
                meerkat_mob::MobError::ActorCommandChannelClosed
                | meerkat_mob::MobError::ActorReplyChannelClosed,
            ) => MobTerminalShutdownOutcome::AlreadyShutDown,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "mob terminal shutdown after stop was refused; the supervisor in-proc \
                     route may remain registered until process exit"
                );
                MobTerminalShutdownOutcome::Refused {
                    error: error.to_string(),
                }
            }
        }
    }

    /// Join the supervisor cleanups that replacement retired.
    ///
    /// These are joined, never aborted. `TrackedLeaseRenewalTask` and
    /// `TrackedContinuityRepairTask` cancel cooperatively and document why:
    /// a renewal is joined through publication of the provider's fencing token,
    /// and a repair pass through its explicit commit/rollback boundary, so
    /// raw-aborting is a correctness error rather than a faster shutdown.
    /// Aborting the wrapper future would also drop the inner `JoinHandle` and
    /// re-detach the supervisor, which is the leak this exists to close.
    ///
    /// Because the join is therefore unbounded in the bad case, it gets its own
    /// small explicit budget. Deliberately NOT `self.drain_timeout`: that value
    /// is already spent by the event drain above, so reusing it would silently
    /// double shutdown's worst case, and hosts that SIGKILL at a fixed grace
    /// period would pay for that without being told. Every task here was
    /// cancelled when it was replaced, so this is a fail-fast confirmation
    /// rather than a wait, and expiry is reported instead of absorbed.
    async fn join_retired_supervisor_cleanups(&self) -> RetiredSupervisorCleanupOutcome {
        let mut retired = self.retired_supervisor_cleanups.lock().await;
        if retired.is_empty() {
            return RetiredSupervisorCleanupOutcome::NothingPending;
        }
        let mut lease_renewal = 0usize;
        let mut continuity_repair = 0usize;
        let mut join_failed = 0usize;
        let joined_all = tokio::time::timeout(RETIRED_SUPERVISOR_JOIN_BUDGET, async {
            while let Some(joined) = retired.join_next().await {
                match joined {
                    // Matched exhaustively on purpose: `RetiredSupervisorKind`
                    // is `#[non_exhaustive]` only to downstream crates, so a new
                    // supervisor added here fails to compile until it is counted
                    // rather than silently landing in a catch-all.
                    Ok(RetiredSupervisorKind::LeaseRenewal) => lease_renewal += 1,
                    Ok(RetiredSupervisorKind::ContinuityRepair) => continuity_repair += 1,
                    // A cleanup that did not return normally leaves no
                    // attestation that it reached its release boundary. Not
                    // claimed to prove the fencing token was never published:
                    // `JoinError` also covers cancellation, and a panic can
                    // land after the side effect but before the return. The
                    // absent attestation is the fact, and it is enough to
                    // withhold one. Nothing is aborted to learn this; the task
                    // is already over.
                    Err(_) => join_failed += 1,
                }
            }
        })
        .await
        .is_ok();
        // `len()` is the tasks still running, so it is 0 whenever the budget did
        // not expire.
        let pending = retired.len();
        if joined_all && join_failed == 0 {
            RetiredSupervisorCleanupOutcome::Joined {
                lease_renewal,
                continuity_repair,
            }
        } else {
            RetiredSupervisorCleanupOutcome::Incomplete {
                joined: lease_renewal + continuity_repair,
                join_failed,
                pending,
            }
        }
    }

    /// Stop the mob for teardown: [`MobStopOutcome::Stopped`], or the stop's
    /// refusal as [`MobStopOutcome::Failed`].
    ///
    /// Since MobKit 0.8.46 this never returns
    /// [`MobStopOutcome::ProceededWithoutInterrupt`]: that degraded a
    /// `Runtime not ready: attached` refusal from meerkat's old stop
    /// interrupt path, which meerkat 0.8.51's Stop no longer takes. The
    /// variant (and [`super::types::ErrorEvent::MobStopProceededWithoutInterrupt`]) stays
    /// for wire and SDK compatibility.
    pub async fn stop_mob_for_teardown(&self) -> MobStopOutcome {
        let outcome = match self.stop_mob().await {
            Ok(()) => MobStopOutcome::Stopped,
            Err(error) => MobStopOutcome::Failed(error),
        };
        // Teardown: no delivery may be applied to the stopped mob.
        self.mob_runtime.stop_runtime_delivery();
        outcome
    }

    /// Stop the mob, settling its active flow runs first when they hold the
    /// Stop (see [`super::mob_stop`]). meerkat 0.8.51's Stop settles member
    /// work itself, so nothing here cancels member work or re-sends the stop
    /// on a timer; any other refusal is returned as is.
    async fn stop_mob(&self) -> Result<(), MobRuntimeError> {
        super::mob_stop::settle_flow_runs_then_stop(
            &super::mob_stop::HandleStopTarget(&self.mob_handle()),
            super::mob_stop::MOB_STOP_FLOW_SETTLE_BUDGET,
        )
        .await
    }

    /// Drain pending agent/module events from the mob event router and
    /// project them onto the ConsoleEventStore + event log. Callers that
    /// embed `UnifiedRuntime` inside their own axum server (rather than
    /// using `.serve()`) must poll this periodically — typically via
    /// [`UnifiedRuntime::spawn_event_drain_task`] — or console/event-log
    /// consumers will never see agent responses.
    pub async fn drain_mob_agent_events(&self) -> Result<(), UnifiedRuntimeError> {
        let mut disconnected = false;
        let mut ingress_guard = match self.mob_event_ingress.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                // A previous drain tick may still be projecting a burst of
                // events. Skip this tick instead of killing the host-owned
                // background drain task.
                return Ok(());
            }
        };
        let ingress = match ingress_guard.as_mut() {
            Some(i) => i,
            None => return Ok(()),
        };

        loop {
            match Self::try_recv_ingress_event(ingress) {
                Some(Ok(forwarded)) => {
                    // Fire the typed alert the forwarder extracted at ingest
                    // (e.g. a member compaction persistence rejection). The
                    // hook is read here, at drain time, so hooks installed
                    // via `set_error_hook` after construction still fire.
                    if let Some(alert) = forwarded.alert {
                        self.fire_error(alert);
                    }
                    let unified_event = forwarded.envelope;
                    // Detect agent run failures and fire HostLoopCrash, naming
                    // the typed failure (class, reason, message) carried in
                    // the payload's `error_report`.
                    if let crate::types::UnifiedEvent::Agent {
                        ref agent_id,
                        ref event_type,
                        ref payload,
                    } = unified_event.event
                        && event_type == "run_failed"
                    {
                        self.fire_error(super::types::ErrorEvent::HostLoopCrash {
                            member_id: agent_id.clone(),
                            error: run_failed_crash_error(
                                &unified_event.event_id,
                                payload.as_ref(),
                            ),
                        });
                    }
                    // Ingest into event log (non-blocking, buffered)
                    self.ingest_event(&unified_event);
                    self.project_console_event_from_unified(&unified_event)
                        .await;
                    self.module_runtime
                        .lock()
                        .await
                        .append_normalized_event(unified_event)?;
                }
                Some(Err(TryRecvError::Empty)) => break,
                Some(Err(TryRecvError::Disconnected)) => {
                    disconnected = true;
                    break;
                }
                None => break,
            }
        }

        if disconnected {
            *ingress_guard = None;
        }

        Ok(())
    }

    pub(super) async fn close_event_router(&self) {
        let ingress = self.mob_event_ingress.lock().await.take();
        match ingress {
            Some(MobEventIngress::Forwarder(forwarder)) => {
                let task = forwarder.task;
                task.abort();
                let _ = task.await;
                let health_task = forwarder.identity_stream_health_task;
                health_task.abort();
                let _ = health_task.await;
            }
            None => {}
        }

        // Stop the structural mob-events subscription task as well.
        if let Some(task) = self.mob_events_subscriber_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
    }

    fn try_recv_ingress_event(
        ingress: &mut MobEventIngress,
    ) -> Option<Result<super::ForwardedMemberEvent, TryRecvError>> {
        Some(match ingress {
            MobEventIngress::Forwarder(forwarder) => forwarder.event_rx.try_recv(),
        })
    }
}

async fn abort_and_join_remote_host_reconnect(task: tokio::task::JoinHandle<()>) {
    task.abort();
    let _ = task.await;
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod remote_host_task_tests {
    use super::abort_and_join_remote_host_reconnect;

    struct DropNotify(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropNotify {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn reconnect_task_abort_is_joined_before_shutdown_continues() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _notify = DropNotify(Some(sender));
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;

        abort_and_join_remote_host_reconnect(task).await;
        receiver
            .await
            .expect("joined task must drop all owned reconnect state");
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
pub(crate) mod stop_degrade_tests {
    use super::*;
    use std::sync::Arc;

    pub(crate) async fn empty_runtime(mob_id: &str) -> UnifiedRuntime {
        let definition = meerkat_mob::MobDefinition::from_toml(&format!(
            r#"
[mob]
id = "{mob_id}"

[profiles.worker]
model = "gpt-5.5"

[profiles.worker.tools]
comms = true
"#
        ))
        .expect("definition parses");
        UnifiedRuntime::builder()
            .definition(definition)
            .default_llm_client(Arc::new(meerkat_client::TestClient::for_provider(
                meerkat_core::Provider::OpenAI,
            )))
            .build()
            .await
            .expect("runtime builds")
    }

    /// D67: entering `shutdown()` does not disarm the drop path. An explicit
    /// shutdown held in a real phase (module teardown, behind its lock) and
    /// abandoned by its caller has not settled; dropping the runtime then
    /// hands its state to the retained teardown, which runs the shutdown to
    /// its end and only then releases the state.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_abandoned_explicit_shutdown_hands_over_on_drop() {
        let runtime = empty_runtime("abandoned-shutdown").await;
        let modules = Arc::clone(&runtime.module_runtime);
        let state = Arc::downgrade(&modules);
        let settlement = Arc::clone(&runtime.retained_teardown_report);
        let held = modules.lock().await;
        let attempt =
            tokio::time::timeout(std::time::Duration::from_millis(500), runtime.shutdown()).await;
        assert!(attempt.is_err(), "the shutdown is held at module teardown");
        assert!(
            !runtime.teardown_settled.load(Ordering::SeqCst),
            "an entered but abandoned shutdown has not settled"
        );
        drop(held);
        drop(modules);
        drop(runtime);
        let settled = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while settlement.get().is_none() || state.strong_count() > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(
            settled.is_ok(),
            "the retained teardown resumes and finishes"
        );
        // The cleanup resumed rather than the state merely dropping: the
        // retained teardown ran the shutdown through module teardown (the
        // phase the abandoned call never reached) and reported its outcome.
        let report = settlement.get().expect("the retained teardown reported");
        assert_eq!(
            report.module_shutdown.physical_cleanup.phase,
            crate::ModuleCleanupPhase::Settled,
            "module teardown ran to settlement"
        );
        // The abandoned call had already stopped the mob, so the resumed
        // shutdown reports that phase as it found it; module teardown above
        // is the phase that proves the cleanup resumed.
        tracing::debug!(terminal = ?report.mob_terminal_shutdown, "resumed teardown");
    }

    fn injected_refusal(detail: &str) -> MobRuntimeError {
        MobRuntimeError::Mob(meerkat_mob::MobError::Internal(detail.to_string()))
    }

    /// Teardown sends the mob's Stop once and returns the machine's refusal
    /// as is. A completed mob refuses Stop with `InvalidTransition`; the old
    /// quiescing loop answered that class by cancelling member work and
    /// re-sending the stop every 250 ms for 10 s. With the clock paused, any
    /// such wait advances virtual time, so the teardown must take none.
    #[tokio::test(flavor = "current_thread")]
    async fn teardown_sends_stop_once_and_returns_the_refusal() {
        let runtime = empty_runtime("stop-once-refusal").await;
        runtime
            .mob_handle()
            .complete()
            .await
            .expect("the mob completes");

        tokio::time::pause();
        let started = tokio::time::Instant::now();
        let outcome = runtime.stop_mob_for_teardown().await;
        let waited = started.elapsed();
        tokio::time::resume();

        match outcome {
            MobStopOutcome::Failed(MobRuntimeError::Mob(
                meerkat_mob::MobError::InvalidTransition { .. },
            )) => {}
            other => panic!("a completed mob refuses Stop with InvalidTransition: {other:?}"),
        }
        assert_eq!(
            waited,
            Duration::ZERO,
            "teardown re-sent or waited on the stop instead of returning the refusal"
        );
    }

    /// The outcome vocabulary is what callers branch on, and the two
    /// non-failure outcomes must stay TELLABLE APART: teardown may continue
    /// after proceeding without an interrupt, but that is not a clean stop.
    /// Collapsing them is the false-success shape this item refuses.
    #[test]
    fn proceeding_without_an_interrupt_is_never_reported_as_a_clean_stop() {
        let proceeded = MobStopOutcome::ProceededWithoutInterrupt {
            waited_ms: 10_000,
            member: Some("019e3c52-0f1b-73d3-a5c7-4b21c2bbf131".to_string()),
            error: "Runtime not ready: attached".to_string(),
        };

        assert!(MobStopOutcome::Stopped.teardown_may_proceed());
        assert!(MobStopOutcome::Stopped.stopped_cleanly());

        assert!(
            proceeded.teardown_may_proceed(),
            "teardown must not be blocked by a readiness state"
        );
        assert!(
            !proceeded.stopped_cleanly(),
            "proceeding without an interrupt must never read as a clean stop"
        );

        let failed = MobStopOutcome::Failed(injected_refusal("actor task dropped"));
        assert!(!failed.teardown_may_proceed());
        assert!(!failed.stopped_cleanly());
    }
}

#[cfg(test)]
// A cleanup that dies before its release boundary is one of the two failure
// modes under test, and the only way to produce a real `JoinError` is to let a
// task actually panic.
#[allow(clippy::panic)]
mod retired_supervisor_cleanup_tests {
    use super::stop_degrade_tests::empty_runtime;
    use super::{RETIRED_SUPERVISOR_JOIN_BUDGET, RetiredSupervisorCleanupOutcome};
    use crate::unified_runtime::types::RetiredSupervisorKind;

    /// Replacement used to `tokio::spawn(previous.cancel_and_join())` and drop
    /// the handle. A dropped `JoinHandle` detaches, so the cleanup could outlive
    /// shutdown while still holding the authority it was releasing.
    #[tokio::test]
    async fn a_retired_cleanup_is_joined_at_shutdown_rather_than_detached() {
        let mut runtime = empty_runtime("retired-cleanup-joined").await;
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        runtime.retain_retired_supervisor_cleanup(
            RetiredSupervisorKind::LeaseRenewal,
            async move {
                let _ = rx.await;
            },
        );

        // Still outstanding: the drain must actually wait for it, so releasing
        // it only after the drain has begun proves the join is real.
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let _ = tx.send(());
        });

        assert_eq!(
            runtime.join_retired_supervisor_cleanups().await,
            RetiredSupervisorCleanupOutcome::Joined {
                lease_renewal: 1,
                continuity_repair: 0
            }
        );
    }

    #[tokio::test]
    async fn each_retired_supervisor_is_counted_as_the_one_it_was() {
        let mut runtime = empty_runtime("retired-cleanup-kinds").await;
        runtime.retain_retired_supervisor_cleanup(RetiredSupervisorKind::LeaseRenewal, async {});
        runtime
            .retain_retired_supervisor_cleanup(RetiredSupervisorKind::ContinuityRepair, async {});
        runtime
            .retain_retired_supervisor_cleanup(RetiredSupervisorKind::ContinuityRepair, async {});

        // Not a single total: a lease renewal is joined through fencing-token
        // publication and a repair pass through its commit/rollback boundary,
        // so which one failed to finish is the actionable fact.
        assert_eq!(
            runtime.join_retired_supervisor_cleanups().await,
            RetiredSupervisorCleanupOutcome::Joined {
                lease_renewal: 1,
                continuity_repair: 2
            }
        );
    }

    /// The budget must be reported, not absorbed. An unbounded join here would
    /// be the exact silent shutdown stall this reporting exists to surface.
    #[tokio::test]
    async fn an_unfinished_cleanup_is_reported_instead_of_hanging_shutdown() {
        let mut runtime = empty_runtime("retired-cleanup-timeout").await;
        runtime.retain_retired_supervisor_cleanup(
            RetiredSupervisorKind::LeaseRenewal,
            std::future::pending::<()>(),
        );

        let started = std::time::Instant::now();
        let outcome = runtime.join_retired_supervisor_cleanups().await;
        let waited = started.elapsed();

        assert_eq!(
            outcome,
            RetiredSupervisorCleanupOutcome::Incomplete {
                joined: 0,
                join_failed: 0,
                pending: 1
            }
        );
        // Bounded by its own budget, and NOT by a second spend of drain_timeout:
        // reusing that value would silently double shutdown's worst case for
        // hosts that SIGKILL at a fixed grace period.
        assert!(
            waited < RETIRED_SUPERVISOR_JOIN_BUDGET * 3,
            "the join must be bounded by its own budget, waited {waited:?}"
        );
    }

    /// A `JoinSet` does not reap on its own. Without the non-blocking reap in
    /// `retain_retired_supervisor_cleanup`, a process that re-attaches
    /// identity-first repeatedly accumulates finished entries for its whole
    /// lifetime, and the shutdown count would measure total replacements rather
    /// than outstanding work.
    #[tokio::test]
    async fn finished_cleanups_are_reaped_so_the_count_tracks_outstanding_work() {
        let mut runtime = empty_runtime("retired-cleanup-reaped").await;
        runtime.retain_retired_supervisor_cleanup(RetiredSupervisorKind::LeaseRenewal, async {});
        // Let the first cleanup actually finish before the next replacement.
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        runtime
            .retain_retired_supervisor_cleanup(RetiredSupervisorKind::ContinuityRepair, async {});

        assert_eq!(
            runtime.join_retired_supervisor_cleanups().await,
            RetiredSupervisorCleanupOutcome::Joined {
                lease_renewal: 0,
                continuity_repair: 1
            },
            "the finished lease cleanup should have been reaped at the next retain"
        );
    }

    /// An empty set reports `NothingPending`, and the name has to stay that
    /// modest: because finished cleanups are reaped at the next replacement, a
    /// runtime that retired several cleanups which all completed is
    /// indistinguishable here from one that never replaced a supervisor. The
    /// earlier name `NothingRetired` asserted the second, which this value
    /// cannot support.
    #[tokio::test]
    async fn an_empty_set_reports_nothing_pending_and_claims_no_history() {
        let runtime = empty_runtime("retired-cleanup-none").await;
        assert_eq!(
            runtime.join_retired_supervisor_cleanups().await,
            RetiredSupervisorCleanupOutcome::NothingPending
        );

        // Same value after a real replacement whose cleanup finished and was
        // reaped, which is exactly why the variant cannot claim history.
        let mut reaped = empty_runtime("retired-cleanup-reaped-then-empty").await;
        reaped.retain_retired_supervisor_cleanup(RetiredSupervisorKind::LeaseRenewal, async {});
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        reaped.retain_retired_supervisor_cleanup(RetiredSupervisorKind::LeaseRenewal, async {});
        // The second retain reaped the first; joining the second empties the set.
        let _ = reaped.join_retired_supervisor_cleanups().await;
        assert_eq!(
            reaped.join_retired_supervisor_cleanups().await,
            RetiredSupervisorCleanupOutcome::NothingPending
        );
    }

    /// A cleanup that does not return normally leaves no attestation that it
    /// reached its release boundary. Reporting it as `Joined` would attest a
    /// release the process cannot evidence.
    #[tokio::test]
    async fn a_cleanup_that_did_not_return_is_not_reported_as_released() {
        let mut runtime = empty_runtime("retired-cleanup-panic").await;
        runtime.retain_retired_supervisor_cleanup(RetiredSupervisorKind::LeaseRenewal, async {
            panic!("cleanup did not return normally");
        });
        runtime
            .retain_retired_supervisor_cleanup(RetiredSupervisorKind::ContinuityRepair, async {});

        assert_eq!(
            runtime.join_retired_supervisor_cleanups().await,
            RetiredSupervisorCleanupOutcome::Incomplete {
                joined: 1,
                join_failed: 1,
                pending: 0
            }
        );
    }
}

#[cfg(test)]
mod shutdown_horizon_tests {
    /// The published shutdown horizon must not silently collapse.
    ///
    /// `shutdown_horizon()` returns
    /// `gateway_composition::GATEWAY_RUNTIME_SHUTDOWN_TIMEOUT` directly - one
    /// expression, so accessor-vs-constant divergence is visible in the diff
    /// rather than needing a test. What a diff does NOT make obvious is the
    /// constant shrinking, and that is what this guards.
    ///
    /// It matters because the failure is asymmetric. Two independent consumers
    /// bounded this budget 4x and 11x too tightly on the same night
    /// (2026-08-31) while it was merely a documented `pub const`. Now that the
    /// runtime advertises it, an embedder who sizes their wait on the
    /// advertised figure is stranded if it drops beneath what shutdown
    /// actually needs - and they would be MORE confident while being wrong,
    /// because they asked.
    #[test]
    fn the_published_shutdown_horizon_does_not_silently_collapse() {
        const ADVERTISED: std::time::Duration =
            crate::gateway_composition::GATEWAY_RUNTIME_SHUTDOWN_TIMEOUT;
        assert!(
            ADVERTISED.as_secs() >= 300,
            "the published shutdown horizon collapsed to {ADVERTISED:?}. It \
             covers two provider-callback windows plus drains and reap margin; \
             shrinking it below that strands every embedder that sized their \
             wait on the advertised figure"
        );
    }
}

#[cfg(test)]
mod run_failed_crash_error_tests {
    use super::run_failed_crash_error;

    /// The ERROR line names the typed failure: class, the stable
    /// `reason_type`, and meerkat's message, after the event id it always
    /// carried.
    #[test]
    fn names_class_reason_and_message_from_the_error_report() {
        let payload = serde_json::json!({
            "type": "run_failed",
            "session_id": "01a0-session",
            "error_report": {
                "class": "llm",
                "reason": { "reason_type": "llm_auth_error" },
                "message": "LLM error: authentication failed (401)"
            }
        });
        assert_eq!(
            run_failed_crash_error("evt-agent-7", Some(&payload)),
            "agent run failed (event_id: evt-agent-7) class=llm reason=llm_auth_error: \
             LLM error: authentication failed (401)"
        );
    }

    /// A report without a typed reason still names class and message.
    #[test]
    fn omits_the_reason_when_the_report_carries_none() {
        let payload = serde_json::json!({
            "error_report": { "class": "internal", "message": "runner gave up" }
        });
        assert_eq!(
            run_failed_crash_error("evt-1", Some(&payload)),
            "agent run failed (event_id: evt-1) class=internal: runner gave up"
        );
    }

    /// Without a report (a payload-less envelope, or a producer that
    /// predates it) the line is exactly what it was before.
    #[test]
    fn falls_back_to_the_bare_event_id_without_a_report() {
        assert_eq!(
            run_failed_crash_error("evt-2", None),
            "agent run failed (event_id: evt-2)"
        );
        let unrelated = serde_json::json!({ "type": "run_failed", "session_id": "s" });
        assert_eq!(
            run_failed_crash_error("evt-3", Some(&unrelated)),
            "agent run failed (event_id: evt-3)"
        );
    }
}
