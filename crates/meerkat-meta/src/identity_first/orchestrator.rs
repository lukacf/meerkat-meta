//! Orchestrator: restore flow, reconciliation, and topology wiring.
//!
//! Implements REQ-12 (restore flow), REQ-13 (broken continuity), REQ-19/REQ-21
//! (topology reconciliation), REQ-20 (static wiring preserved), REQ-22 (topology
//! not continuity truth), and REQ-33 (identity-keyed reconciliation).

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use futures::{StreamExt, stream};

use super::contracts::{AgentCustomizer, TopologyProvider};
use super::runtime::{EmbodimentOverrides, IdentityRuntime, IdentityRuntimeError};
use super::types::{
    AgentBuildDraft, AgentIdentity, ContinuityFailure, ContinuityFailureKind, ContinuityRecord,
    ContinuityResolveState, DurableAgentSpec, IdentityLifecycleState, IdentityRestoreProgress,
    ManagedPeerEdge, TopologyContext,
};

pub(crate) const IDENTITY_RESTORE_CONCURRENCY: usize = 4;

/// Effective restore concurrency: `META_IDENTITY_RESTORE_CONCURRENCY`
/// overrides the default, clamped to [1, 16]. `1` serializes restores — the
/// field mitigation for Bug I, where concurrent multi-hundred-MB session
/// resumes against one SQLite store exceed the 5s writer busy_timeout and the
/// resulting mid-spawn store failures feed meerkat's destructive resume
/// rollback (upstream ask 31).
pub(crate) fn identity_restore_concurrency() -> usize {
    parse_identity_restore_concurrency(
        std::env::var("META_IDENTITY_RESTORE_CONCURRENCY")
            .ok()
            .as_deref(),
    )
}

fn parse_identity_restore_concurrency(raw: Option<&str>) -> usize {
    raw.and_then(|value| value.trim().parse::<usize>().ok())
        .map(|value| value.clamp(1, 16))
        .unwrap_or(IDENTITY_RESTORE_CONCURRENCY)
}

#[cfg(test)]
mod restore_concurrency_tests {
    use super::{IDENTITY_RESTORE_CONCURRENCY, parse_identity_restore_concurrency};

    #[test]
    fn defaults_when_unset_or_invalid() {
        assert_eq!(
            parse_identity_restore_concurrency(None),
            IDENTITY_RESTORE_CONCURRENCY
        );
        assert_eq!(
            parse_identity_restore_concurrency(Some("")),
            IDENTITY_RESTORE_CONCURRENCY
        );
        assert_eq!(
            parse_identity_restore_concurrency(Some("not-a-number")),
            IDENTITY_RESTORE_CONCURRENCY
        );
    }

    #[test]
    fn parses_and_clamps() {
        assert_eq!(parse_identity_restore_concurrency(Some("1")), 1);
        assert_eq!(parse_identity_restore_concurrency(Some(" 8 ")), 8);
        assert_eq!(parse_identity_restore_concurrency(Some("0")), 1);
        assert_eq!(parse_identity_restore_concurrency(Some("64")), 16);
    }

    // The former `should_load_resume_snapshot` / `RestoreSnapshotPolicy` unit
    // tests are gone with the policy twin they described. The surviving rule
    // ("load the continuity payload unless a live bridge declares it resumes
    // by session id") is guarded end to end where it can actually regress:
    // `identity_first_public_eager_refresh_elides_snapshot_load` counts real
    // store calls, and the bridge-less `restore_flow` tests in
    // identity_first_runtime / identity_first_choke / identity_first_e2e pin
    // the classification path that still depends on the payload.
}

fn trace_identity_restore_completed(identity: &AgentIdentity, started_at: Instant) {
    let elapsed = started_at.elapsed();
    if elapsed >= Duration::from_secs(1) {
        tracing::info!(
            %identity,
            elapsed_ms = elapsed.as_millis(),
            "identity restore completed"
        );
    } else {
        tracing::debug!(
            %identity,
            elapsed_ms = elapsed.as_millis(),
            "identity restore completed"
        );
    }
}

// ---------------------------------------------------------------------------
// Restore flow result
// ---------------------------------------------------------------------------

/// Result of the restore flow for a single identity.
#[derive(Debug, Clone)]
pub enum RestoreOutcome {
    /// Lazy bootstrap registered identity metadata without materializing a
    /// concrete mob member/session.
    Dormant {
        record: Option<ContinuityRecord>,
        draft: AgentBuildDraft,
    },
    /// Fresh-created: new AgentRuntimeId, new SessionId.
    Created {
        record: ContinuityRecord,
        draft: AgentBuildDraft,
    },
    /// Resumed: the identity kept its AgentRuntimeId and SessionId.
    ///
    /// The durable record is the public resume authority. Snapshot bytes are
    /// internal bridge input and are deliberately not copied into this result.
    Resumed {
        record: ContinuityRecord,
        draft: AgentBuildDraft,
    },
    /// Broken continuity or a member-scoped embodiment failure, surfaced with
    /// its exact typed cause while unrelated members continue restoring.
    Broken(ContinuityFailure),
}

impl RestoreOutcome {
    /// The typed restore progress this settled outcome publishes.
    pub fn restore_progress(&self) -> IdentityRestoreProgress {
        match self {
            Self::Dormant { .. } => IdentityRestoreProgress::Registered,
            Self::Created { .. } => IdentityRestoreProgress::Minted,
            Self::Resumed { .. } => IdentityRestoreProgress::Resumed,
            Self::Broken(failure) => IdentityRestoreProgress::Broken {
                kind: failure.kind.clone(),
            },
        }
    }
}

/// Result of the full restore flow.
#[derive(Debug)]
pub struct RestoreFlowResult {
    pub outcomes: BTreeMap<AgentIdentity, RestoreOutcome>,
    pub managed_edges: Vec<ManagedPeerEdge>,
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

/// Actions produced by reconciliation (REQ-33).
#[derive(Debug, Clone)]
pub enum ReconcileAction {
    /// New identity: resolve + lease + activate.
    Activate(DurableAgentSpec),
    /// Removed identity: retire.
    Retire(AgentIdentity),
    /// Changed spec: hot-reload metadata fields.
    HotReload {
        identity: AgentIdentity,
        new_spec: DurableAgentSpec,
    },
    /// Profile changed: requires respawn.
    Respawn {
        identity: AgentIdentity,
        new_spec: DurableAgentSpec,
    },
}

/// Compute reconciliation actions by comparing desired roster to current active set.
pub fn compute_reconcile_actions(
    desired: &[DurableAgentSpec],
    current: &BTreeMap<AgentIdentity, DurableAgentSpec>,
) -> Vec<ReconcileAction> {
    let mut actions = Vec::new();

    let desired_map: BTreeMap<&AgentIdentity, &DurableAgentSpec> =
        desired.iter().map(|s| (&s.identity, s)).collect();

    // New identities: in desired, not in current
    for (id, spec) in &desired_map {
        if !current.contains_key(*id) {
            actions.push(ReconcileAction::Activate((*spec).clone()));
        }
    }

    // Removed identities: in current, not in desired
    for id in current.keys() {
        if !desired_map.contains_key(id) {
            actions.push(ReconcileAction::Retire(id.clone()));
        }
    }

    // Changed identities: in both, spec differs
    for (id, new_spec) in &desired_map {
        if let Some(old_spec) = current.get(*id)
            && old_spec != *new_spec
        {
            // Profile change → respawn (REQ-33)
            if old_spec.profile == new_spec.profile {
                // labels, display_name, addressability → hot-reload
                // context, additional_instructions → registry update
                actions.push(ReconcileAction::HotReload {
                    identity: (*id).clone(),
                    new_spec: (*new_spec).clone(),
                });
            } else {
                actions.push(ReconcileAction::Respawn {
                    identity: (*id).clone(),
                    new_spec: (*new_spec).clone(),
                });
            }
        }
    }

    actions
}

type RosterAuthorityGuards = (
    tokio::sync::OwnedMutexGuard<()>,
    tokio::sync::OwnedMutexGuard<()>,
);

/// Reserve every identity and its public alias in one stable order before a
/// roster-wide continuity/lease decision. Keeping the guards owned lets the
/// restore work remain concurrent after the all-or-nothing authority gate,
/// while reset, delete, materialize, and raw-member creation cannot invalidate
/// any member's freshly resolved generation.
async fn acquire_roster_authority_guards(
    runtime: &IdentityRuntime,
    identities: &[AgentIdentity],
) -> Result<BTreeMap<AgentIdentity, RosterAuthorityGuards>, IdentityRuntimeError> {
    let ordered = identities.iter().cloned().collect::<BTreeSet<_>>();
    let mut lifecycle_guards = BTreeMap::new();
    for identity in &ordered {
        lifecycle_guards.insert(
            identity.clone(),
            runtime
                .lifecycle_lock_for(identity)
                .await
                .lock_owned()
                .await,
        );
    }

    let mut alias_guards = BTreeMap::new();
    for identity in &ordered {
        alias_guards.insert(
            identity.clone(),
            runtime
                .raw_member_alias_lock(identity.as_str())
                .await
                .lock_owned()
                .await,
        );
    }
    for identity in &ordered {
        runtime.ensure_raw_member_alias_available(identity).await?;
    }

    ordered
        .into_iter()
        .map(|identity| {
            let lifecycle = lifecycle_guards.remove(&identity).ok_or_else(|| {
                IdentityRuntimeError::Internal(format!(
                    "authority gate lost lifecycle reservation for {identity}"
                ))
            })?;
            let alias = alias_guards.remove(&identity).ok_or_else(|| {
                IdentityRuntimeError::Internal(format!(
                    "authority gate lost alias reservation for {identity}"
                ))
            })?;
            Ok((identity, (lifecycle, alias)))
        })
        .collect::<Result<_, IdentityRuntimeError>>()
}

// ---------------------------------------------------------------------------
// Restore flow orchestration — REQ-12
// ---------------------------------------------------------------------------

/// Execute the full restore flow per REQ-12 sequencing.
///
/// Steps:
/// 1. Validate the roster and compute topology.
/// 2. Resolve continuity for the complete roster and publish metadata.
/// 3. For each non-Broken identity, independently acquire its lease, build,
///    and create or resume through `IdentityRuntime::embody_identity`.
/// 4. Park a member-attributable failure as a typed Broken outcome and keep
///    restoring the remaining identities.
///
/// When `bridge` is `Some`, Uninitialized identities call `bridge.create_session()`
/// and Ready identities call `bridge.resume_session()` to actually spawn/resume
/// mob members. When `None`, the flow runs validation-only (for tests).
///
/// # Snapshot payloads are read on need, not on principle
///
/// There is ONE restore flow and ONE snapshot rule (there used to be two of
/// each - `RestoreSnapshotPolicy` and a bootstrap-only twin - which is why
/// console add-member and every reconcile pass paid a full session-blob read
/// per Ready member: 14MB/60s to 94MB/180s in production). Restore now reads
/// the continuity payload only where something consumes it:
///
/// - already-converged identity: never (metadata convergence, no embodiment);
/// - live bridge that declares `requires_resume_snapshot() == false`: never
///   (it resumes by session id and must not inspect the argument);
/// - otherwise (no bridge, or a bridge that consumes the payload): always.
///
/// The no-bridge case is load-bearing, not conservatism: with no bridge
/// verdict to key on, snapshot presence is the only signal that distinguishes
/// a resumed identity from a fresh-created one.
///
/// Snapshot bytes remain internal to the embodiment transaction. The public
/// `Resumed` outcome reports the durable identity/session record instead of a
/// second, non-authoritative copy of bridge input.
pub async fn restore_flow(
    runtime: &IdentityRuntime,
    roster: &[DurableAgentSpec],
    topology_provider: Option<&dyn TopologyProvider>,
    customizer: Option<&dyn AgentCustomizer>,
) -> Result<RestoreFlowResult, IdentityRuntimeError> {
    // The pass is one embodiment burst: the repair supervisor defers until
    // all of it settled, then runs one pass for every settle it raised.
    let _burst = runtime.enter_embodiment_burst();
    // Fleet-level declaration and continuity resolution remain fail-closed:
    // without a valid unique roster, topology, or complete batch-store answer
    // there is no truthful set of identities to materialize. The lazy
    // registration phase publishes that validated metadata without creating a
    // member or taking an embodiment lease.
    let registered = register_roster_metadata(runtime, roster, topology_provider, false).await?;
    let managed_edges = registered.managed_edges;
    let registered_outcomes = registered.outcomes;
    // Typed per-member progress on the bootstrap status, so a host follows
    // each member instead of waiting on the whole pass.
    for (identity, outcome) in &registered_outcomes {
        let error = match outcome {
            RestoreOutcome::Broken(failure) => Some(failure.detail.clone()),
            _ => None,
        };
        let progress = match outcome {
            RestoreOutcome::Broken(_) => outcome.restore_progress(),
            _ => IdentityRestoreProgress::Registered,
        };
        runtime.publish_restore_progress(identity, progress, error);
    }

    // Concrete embodiment is deliberately per identity. Eager restore, lazy
    // foreground materialization, and background warming now use the same
    // lifecycle-locked transaction in IdentityRuntime::embody_identity. A
    // member-attributable failure parks only that identity as a typed Broken
    // outcome; it can never abort an unrelated member's boot.
    // A held pass decides its members only once every one of them is held
    // or settled. Its members wait on the pass's construction slots (the
    // ordinary restore concurrency) and give a slot back when they report
    // held, so a held member never blocks a later member's construction.
    let held_pass = runtime.held_create_pass();
    let restore_concurrency = if held_pass.is_some() {
        roster.len().max(1)
    } else {
        identity_restore_concurrency()
    };
    tracing::info!(
        member_count = roster.len(),
        concurrency = restore_concurrency,
        "starting identity restore"
    );
    let restore_started_at = Instant::now();
    let mut restored = stream::iter(roster.iter().cloned().enumerate())
        .map(|(index, spec)| {
            let initial = registered_outcomes.get(&spec.identity).cloned();
            let held_pass = held_pass.clone();
            async move {
                let identity = spec.identity.clone();
                // A held member waits for a construction slot only within
                // the setup's bound: once the pass's terminal decision is
                // `Abort`, it is never constructed.
                let construction_refused = match held_pass.as_ref() {
                    Some(pass) => !pass.take_construction_slot(&identity).await,
                    None => false,
                };
                let member_started_at = Instant::now();

                let outcome = match initial {
                    _ if construction_refused => {
                        let error = IdentityRuntimeError::Internal(format!(
                            "the held identity bootstrap ended before {identity}'s construction started"
                        ));
                        RestoreOutcome::Broken(
                            runtime.park_embodiment_failure(&identity, &error).await,
                        )
                    }
                    Some(RestoreOutcome::Broken(failure)) => {
                        RestoreOutcome::Broken(failure)
                    }
                    Some(
                        RestoreOutcome::Dormant { .. }
                        | RestoreOutcome::Created { .. }
                        | RestoreOutcome::Resumed { .. },
                    ) => {
                        runtime.publish_restore_progress(
                            &identity,
                            IdentityRestoreProgress::Resuming,
                            None,
                        );
                        let mut bound_bootstrap_generation = None;
                        match runtime
                            .embody_identity(
                                &identity,
                                None,
                                None,
                                None,
                                &mut bound_bootstrap_generation,
                                EmbodimentOverrides {
                                    spec: Some(&spec),
                                    customizer,
                                    expected_spec_digest: None,
                                },
                            )
                            .await
                        {
                            Ok(embodiment) if embodiment.resumed => RestoreOutcome::Resumed {
                                record: embodiment.record,
                                draft: embodiment.draft,
                            },
                            Ok(embodiment) => RestoreOutcome::Created {
                                record: embodiment.record,
                                draft: embodiment.draft,
                            },
                            Err(error) => {
                                let failure = runtime
                                    .park_embodiment_failure(&identity, &error)
                                    .await;
                                tracing::warn!(
                                    %identity,
                                    kind = ?failure.kind,
                                    detail = %failure.detail,
                                    "identity embodiment failed; parked Broken while fleet restore continues"
                                );
                                RestoreOutcome::Broken(failure)
                            }
                        }
                    }
                    None => {
                        // lazy_register_flow proves one result for every roster
                        // identity. Treat a violated internal correspondence as
                        // this member's visible failure rather than borrowing
                        // the cause for the rest of the fleet.
                        let error = IdentityRuntimeError::Internal(format!(
                            "validated registration outcome disappeared for {identity}"
                        ));
                        let failure = runtime
                            .park_embodiment_failure(&identity, &error)
                            .await;
                        RestoreOutcome::Broken(failure)
                    }
                };

                // A held pass settles what its host's completion covers: a
                // published member owes nothing, and a Broken one owes its
                // host a fresh held completion before it is created again.
                if held_pass.is_some() {
                    match &outcome {
                        RestoreOutcome::Created { .. } => {
                            runtime.record_held_completion(&identity, true);
                        }
                        RestoreOutcome::Broken(_) => {
                            runtime.record_held_completion(&identity, false);
                        }
                        RestoreOutcome::Dormant { .. } | RestoreOutcome::Resumed { .. } => {}
                    }
                }
                let error = match &outcome {
                    RestoreOutcome::Broken(failure) => Some(failure.detail.clone()),
                    _ => None,
                };
                runtime.publish_restore_progress(&identity, outcome.restore_progress(), error);
                trace_identity_restore_completed(&identity, member_started_at);
                if let Some(pass) = held_pass.as_ref() {
                    pass.settle(&identity);
                }
                (index, identity, outcome)
            }
        })
        .buffer_unordered(restore_concurrency)
        .collect::<Vec<_>>()
        .await;
    restored.sort_by_key(|(index, _, _)| *index);

    let outcomes = restored
        .into_iter()
        .map(|(_, identity, outcome)| (identity, outcome))
        .collect();
    tracing::info!(
        member_count = roster.len(),
        elapsed_ms = restore_started_at.elapsed().as_millis(),
        "identity restore completed"
    );

    // Topology is a fleet-level projection over the successfully embodied
    // set. Member failures have already become typed outcomes, but an invalid
    // fleet projection remains a pass failure rather than being attributed to
    // any one identity.
    runtime.set_desired_peer_edges(managed_edges.clone()).await;
    runtime.reconcile_managed_peer_edges(&managed_edges).await?;

    Ok(RestoreFlowResult {
        outcomes,
        managed_edges,
    })
}
/// Register the roster/topology metadata without materializing members.
///
/// This is the identity-first lazy bootstrap path: it validates the roster,
/// resolves cheap continuity metadata, records desired topology, and exposes
/// status/inspection surfaces. It deliberately does not acquire leases, call
/// customizers, load session snapshots, create sessions, or resume sessions.
pub async fn lazy_register_flow(
    runtime: &IdentityRuntime,
    roster: &[DurableAgentSpec],
    topology_provider: Option<&dyn TopologyProvider>,
) -> Result<RestoreFlowResult, IdentityRuntimeError> {
    register_roster_metadata(runtime, roster, topology_provider, true).await
}

/// Validate and publish roster metadata without opening a concrete embodiment
/// transaction. Eager restore defers an Active member's spec mutation until
/// the shared embodiment door has revalidated its external lease; a genuinely
/// lazy reconcile has no embodiment transaction and publishes the metadata in
/// this phase.
async fn register_roster_metadata(
    runtime: &IdentityRuntime,
    roster: &[DurableAgentSpec],
    topology_provider: Option<&dyn TopologyProvider>,
    update_active_specs: bool,
) -> Result<RestoreFlowResult, IdentityRuntimeError> {
    IdentityRuntime::validate_roster_uniqueness(roster)?;

    let identities: Vec<AgentIdentity> = roster.iter().map(|s| s.identity.clone()).collect();

    let topology_context = TopologyContext {
        roster: roster.to_vec(),
    };
    let managed_edges = if let Some(tp) = topology_provider {
        tp.compute_edges(&identities, &topology_context)
            .await
            .map_err(|e| IdentityRuntimeError::Internal(format!("topology: {e}")))?
    } else {
        Vec::new()
    };
    runtime.set_identity_edge_ownership(
        super::runtime::IdentityEdgeOwnership::for_topology_provider(topology_provider.is_some()),
    );
    runtime.set_desired_peer_edges(managed_edges.clone()).await;

    // Reserve the full roster before the one fleet-wide metadata read. Each
    // identity's guard remains held until its dormant/active projection is
    // published, so a queued reset/delete cannot invalidate the batch result
    // while preserving O(1) store round trips for large lazy rosters.
    let mut authority_guards = acquire_roster_authority_guards(runtime, &identities).await?;
    // Restore/materialization can fail before an IdentityEntry exists. Exact
    // grants from that phase live in the runtime-owned orphan ledger; both
    // eager and lazy reconcile must drain it before publishing a repair or
    // allowing later on-demand materialization to reacquire authority.
    runtime.release_parked_unactivated_leases().await?;
    let resolved = runtime
        .continuity_store()
        .resolve_many(&identities)
        .await
        .map_err(IdentityRuntimeError::Store)?;
    for identity in &identities {
        if !resolved.contains_key(identity) {
            return Err(IdentityRuntimeError::Internal(format!(
                "resolve_many did not return state for {identity}"
            )));
        }
    }

    let mut outcomes = BTreeMap::new();
    for spec in roster {
        let identity = &spec.identity;
        let _authority_guards = authority_guards.remove(identity).ok_or_else(|| {
            IdentityRuntimeError::Internal(format!(
                "validated lazy authority reservation disappeared for {identity}"
            ))
        })?;
        let resolve_state = resolved.get(identity).cloned().ok_or_else(|| {
            IdentityRuntimeError::Internal(format!(
                "validated lazy resolve state disappeared for {identity}"
            ))
        })?;
        let current_state = runtime
            .status(identity)
            .await
            .ok()
            .map(|status| status.state);
        let currently_active = current_state == Some(IdentityLifecycleState::Active);
        // A terminal continuity verdict owns this Broken projection. It must
        // preserve the durable lower-plane binding for operator repair rather
        // than entering ordinary failed-embodiment cleanup.
        // Two typed projections own a Broken entry against reconcile: the
        // heal authority's unprovable verdict (CheckpointUnrecoverable) and
        // the audited-endpoint repair hold (RepairRequired), whose exit is the
        // operator's `rkat session repair-wholeblob` plus `reload_member`.
        // A refused declared role migration owns it too: the refusal kept the
        // restored member, and retiring that member for the ordinary cleanup
        // is the destructive step the refusal exists to prevent. It is checked
        // against the spec this pass restores, so a changed spec never
        // inherits it.
        let terminal_verdict: Option<(ContinuityFailureKind, String)> = if currently_active {
            None
        } else if let Some(verdict) = runtime.continuity_unrecoverable(identity).await {
            Some((
                ContinuityFailureKind::CheckpointUnrecoverable,
                verdict.reason,
            ))
        } else if let Some(reason) = runtime.role_migration_refusal(identity, Some(spec)).await {
            Some((ContinuityFailureKind::RoleMigrationNotApplied, reason))
        } else {
            runtime.session_repair_required(identity).await.map(|hold| {
                (
                    ContinuityFailureKind::RepairRequired,
                    hold.operator_reason(),
                )
            })
        };
        if current_state == Some(IdentityLifecycleState::Broken)
            && terminal_verdict.is_none()
            && let Err(error) = runtime
                .prepare_broken_identity_for_registration(identity)
                .await
        {
            let failure = runtime.park_embodiment_failure(identity, &error).await;
            tracing::warn!(
                %identity,
                kind = ?failure.kind,
                detail = %failure.detail,
                "Broken identity cleanup failed; retained exact authority and continued roster registration"
            );
            outcomes.insert(identity.clone(), RestoreOutcome::Broken(failure));
            continue;
        }
        // Terminal heal verdict (2026-07-29 incident): while it stands, lazy
        // reconcile must not soften this identity's Broken projection to
        // Dormant - that cosmetic "heal" is what materialization re-Breaks.
        let draft = AgentBuildDraft {
            model: None,
            system_prompt: None,
            additional_instructions: spec.additional_instructions.clone(),
            labels: spec.labels.clone(),
            app_context: spec.context.clone(),
            external_tools: Vec::new(),
            local_external_tools: Default::default(),
            provider_params: None,
            compaction_curator: Default::default(),
        };
        match resolve_state {
            ContinuityResolveState::Uninitialized => {
                if currently_active {
                    if update_active_specs {
                        runtime.update_spec_locked(spec.clone()).await?;
                    }
                } else {
                    runtime
                        .register_locked(spec.clone(), IdentityLifecycleState::Dormant, None, None)
                        .await;
                }
                outcomes.insert(
                    identity.clone(),
                    RestoreOutcome::Dormant {
                        record: None,
                        draft,
                    },
                );
            }
            ContinuityResolveState::Ready { record } => {
                if currently_active {
                    if update_active_specs {
                        runtime.update_spec_locked(spec.clone()).await?;
                    }
                } else if let Some((kind, reason)) = terminal_verdict {
                    // 2026-07-29 incident: re-registering a heal-unprovable
                    // identity as Dormant is the cosmetic "heal" that the
                    // next on-demand materialization immediately re-Breaks.
                    // Keep the Broken projection and its typed terminal
                    // reason; delivery keeps refusing loudly (REQ-13) until
                    // an operator intervenes. The repair hold is the same
                    // shape with the operator's repair as its exit.
                    runtime.update_spec_locked(spec.clone()).await?;
                    outcomes.insert(
                        identity.clone(),
                        RestoreOutcome::Broken(ContinuityFailure {
                            identity: identity.clone(),
                            kind,
                            record: Some(record),
                            detail: reason,
                        }),
                    );
                    continue;
                } else {
                    runtime
                        .register_locked(
                            spec.clone(),
                            IdentityLifecycleState::Dormant,
                            Some(record.clone()),
                            None,
                        )
                        .await;
                }
                outcomes.insert(
                    identity.clone(),
                    RestoreOutcome::Dormant {
                        record: Some(record),
                        draft,
                    },
                );
            }
            ContinuityResolveState::Broken { failure } => {
                if currently_active {
                    // A store projection cannot revoke a live embodiment's
                    // exact local authority. Preserve the Active entry and
                    // let the shared embodiment door validate its lease
                    // before applying an eager desired spec. Lazy metadata
                    // refreshes may update the spec directly because they do
                    // not embody in this pass.
                    if update_active_specs {
                        runtime.update_spec_locked(spec.clone()).await?;
                    }
                    outcomes.insert(
                        identity.clone(),
                        RestoreOutcome::Dormant {
                            record: failure.record,
                            draft,
                        },
                    );
                } else if let Some((kind, reason)) = terminal_verdict
                    && matches!(failure.kind, ContinuityFailureKind::SnapshotMissing)
                    && failure.record.is_some()
                {
                    // Same guard as the Ready arm above: a store-visible
                    // Broken shape must not be softened to Dormant while the
                    // heal authority's terminal verdict stands.
                    runtime.update_spec_locked(spec.clone()).await?;
                    outcomes.insert(
                        identity.clone(),
                        RestoreOutcome::Broken(ContinuityFailure {
                            identity: identity.clone(),
                            kind,
                            record: failure.record.clone(),
                            detail: reason,
                        }),
                    );
                } else if matches!(failure.kind, ContinuityFailureKind::SnapshotMissing)
                    && let Some(record) = failure.record.clone()
                {
                    if currently_active {
                        if update_active_specs {
                            runtime.update_spec_locked(spec.clone()).await?;
                        }
                    } else {
                        runtime
                            .register_locked(
                                spec.clone(),
                                IdentityLifecycleState::Dormant,
                                Some(record.clone()),
                                None,
                            )
                            .await;
                    }
                    outcomes.insert(
                        identity.clone(),
                        RestoreOutcome::Dormant {
                            record: Some(record),
                            draft,
                        },
                    );
                } else {
                    // Same warn-baseline visibility rule as the resolve-time
                    // Broken registration above: the typed reason must not
                    // ride only the roster.
                    tracing::warn!(
                        %identity,
                        kind = ?failure.kind,
                        detail = %failure.detail,
                        "restore failure registers the identity Broken pending reconcile"
                    );
                    runtime
                        .register_locked(
                            spec.clone(),
                            IdentityLifecycleState::Broken,
                            failure.record.clone(),
                            None,
                        )
                        .await;
                    outcomes.insert(identity.clone(), RestoreOutcome::Broken(failure));
                }
            }
        }
    }

    if update_active_specs
        && let Err(err) = runtime.reconcile_managed_peer_edges(&managed_edges).await
    {
        tracing::warn!(
            error = %err,
            "identity lazy register flow completed with topology reconcile warning"
        );
    }

    Ok(RestoreFlowResult {
        outcomes,
        managed_edges,
    })
}
