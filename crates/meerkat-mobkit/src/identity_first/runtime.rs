//! Identity-first runtime: delivery, status, lifecycle, and ownership enforcement.
//!
//! This module implements the behavioral core of identity-first continuity:
//! - Delivery: `send()` and `dispatch()` with addressability and lease enforcement
//! - Status: `status()` returning `IdentityStatus`
//! - Lifecycle: `retire()`, `respawn()`, `reset()`, `delete_identity()`
//! - Ownership: lease tracking, fencing, and invariant enforcement

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock, Weak};
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use meerkat_core::types::{HandlingMode, SessionId};
use tokio::sync::{Mutex, Notify, RwLock, broadcast, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

use super::agent_memory::{
    AgentMemoryError, AgentMemoryForgetResult, AgentMemoryRecallRequest, AgentMemoryRecord,
    AgentMemoryRuntimeInjector, NewAgentMemory,
};
use super::bridge::{
    BridgeAdmissionError, BridgeError, BridgeTurnError, CommittedBoundaryRepair,
    ResumeRejectionKind, SessionBridge, archived_not_revivable_park_reason,
};
use super::contracts::{
    AgentCustomizer, ContinuityStore, LeaseProvider, RosterProvider, TopologyProvider,
};
use super::types::{
    AgentAddressability, AgentBuildContext, AgentBuildDraft, AgentIdentity, AgentRuntimeId,
    AgentRuntimeServices, CheckpointVersion, CompletionCursor, CompletionProgress, CompletionWait,
    ContinuityFailure, ContinuityFailureKind, ContinuityGeneration, ContinuityHealth,
    ContinuityRecord, ContinuityStoreError, ContinuityUnrecoverable, CustomizerToolsPending,
    DeliveryErrorClass, DeliveryErrorRecord, DispatchAdmission, DispatchInput, DurabilityPolicy,
    DurableAgentSpec, FencingToken, HostRejectedBuildPark, IdentityBootstrapEntry,
    IdentityBootstrapMode, IdentityBootstrapState, IdentityBootstrapStatus, IdentityLifecycleState,
    IdentityRestoreProgress, IdentityStatus, LeaseGrant, LeaseInfo, ManagedPeerEdge,
    MemberHealthReport, MemberReloadDisposition, MemberReloadOutcome, NotAddressable,
    ReloadAttemptOutcome, ReloadAttemptRecord, RosterContext, SendAdmission, SessionRepairRequired,
    SessionRepairScope, SessionSnapshot, Ticketed, TopologyContext, TurnOutcome, TurnOutput,
    TurnTicket, TurnTracking, TurnUntrackable, TurnWait,
};
use crate::actor_loop_health::{ActorLoopHealth, ActorLoopHealthKind, ActorLoopHealthReport};
use crate::memory::records::{
    ManifestTier, MemoryId, MemoryKind, MemoryScope, NewMemoryRecord, RecordMeta, UsageEvent,
};

const MANAGED_PEER_RECONCILE_CONCURRENCY: usize = 64;
const MATERIALIZATION_FAILURE_BACKOFF: Duration = Duration::from_secs(30);
const RAW_MEMBER_ALIAS_LOCK_SWEEP_MIN: usize = 256;
const BACKGROUND_WARM_CANCELLED: &str =
    "identity background warm cancelled before session installation";
fn durable_spec_uses_external_binding(spec: &DurableAgentSpec) -> bool {
    matches!(spec.backend, Some(meerkat_mob::MobBackendKind::External))
        || matches!(
            spec.binding.as_ref(),
            Some(meerkat_contracts::WireRuntimeBinding::External { .. })
        )
}

fn interaction_id_for_delivery<'a>(
    spec: &DurableAgentSpec,
    interaction_id: Option<&'a str>,
) -> Option<&'a str> {
    // Meerkat 0.8.2 deliberately rejects transcript interaction ids on
    // remotely hosted / peer-only member turns: that metadata carrier is not
    // representable on the wire path. MobKit still correlates the response
    // through its pending-interaction ledger and the peer terminal event.
    (!durable_spec_uses_external_binding(spec))
        .then_some(interaction_id)
        .flatten()
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// The identity embodiment a completion-bearing delivery was admitted onto.
///
/// Compared by value across the unlocked window. Any inequality means the
/// identity was reset, retired or rebound while its turn ran, and the turn's
/// result belongs to an embodiment that no longer exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapturedIncarnation {
    runtime_id: Option<AgentRuntimeId>,
    generation: Option<u64>,
    fencing_token: Option<FencingToken>,
}

/// Which bridge verb the shared send core delivers through.
///
/// A typed mode rather than a bool, and one shared body rather than two, for
/// the same reason the bridge's own submit path is shaped this way: the parts
/// that differ between the lanes are trivial, and the parts that must NOT
/// differ - lease acquisition, alias pinning, defanging, memory injection,
/// session reconciliation - are the ones a second copy would silently drift on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendCommitMode {
    /// Return once the work is admitted. The historical behaviour of `send`.
    Ingress,
    /// Return only after this exact turn has committed its terminal boundary.
    /// Fails closed when completion cannot be observed.
    AwaitCommit,
    /// Return once the work is admitted, like `Ingress`, with the admitted
    /// turn's own completion handle kept under a [`TurnTicket`] minted for
    /// this admission; the turn's own output is recorded when it completes.
    /// The caller's interaction id rides unchanged. Falls back to `Ingress`
    /// (reported as [`TurnTracking::Unavailable`]) when the turn cannot be
    /// tracked.
    TrackTurn,
}

/// Which bridge verb a send's delivery step calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryLane {
    Ingress,
    AwaitCommit,
    Ticketed,
}

/// A send's delivery step once the tracked admission, if the lane called for
/// one, has been attempted.
enum DeliveryStep {
    Ingress,
    AwaitCommit,
    /// Admitted on the tracked lane; the receipt carries the admitted turn.
    Tracked(super::bridge::BridgeTurnReceipt),
}

/// Why a tracked delivery's turn cannot be tracked, or `None` to attempt the
/// tracked lane. Decided from typed state before anything is delivered, so
/// these untrackable turns go to the ingress lane without a tracked attempt.
///
/// The member's runtime mode is deliberately NOT decided here: MobKit's spec
/// is the desired mode, but meerkat checks the live roster entry's mode
/// (fixed at spawn), and the two can disagree. The bridge reads the live
/// entry right before admission and refuses an `autonomous_host` member typed
/// ([`BridgeAdmissionError::UnsupportedForMode`]), before anything is
/// submitted; the tracked lanes then fall back to the ingress lane.
fn turn_untrackable(
    delivered: bool,
    external_binding: bool,
    local_human: bool,
    bridge_tracks_turn_output: bool,
) -> Option<TurnUntrackable> {
    if !delivered {
        Some(TurnUntrackable::NotDelivered)
    } else if local_human {
        Some(TurnUntrackable::HostHumanInput)
    } else if external_binding {
        Some(TurnUntrackable::ExternallyBound)
    } else if !bridge_tracks_turn_output {
        Some(TurnUntrackable::BridgeCannotReportOutput)
    } else {
        None
    }
}

/// The tracked admission was refused for the member's LIVE runtime mode
/// (by the bridge's pre-submit check of the live roster entry, or by meerkat
/// itself), before anything was delivered: the delivery falls back to the
/// ingress lane, reported untracked with the typed reason. An
/// `autonomous_host` member is [`TurnUntrackable::AutonomousHost`]; a refusal
/// in any other mode is [`TurnUntrackable::RefusedByRuntime`].
fn runtime_refused_tracking(
    identity: &AgentIdentity,
    mode: meerkat_mob::MobRuntimeMode,
    detail: &str,
) -> TurnTracking {
    tracing::debug!(
        identity = %identity,
        runtime_mode = %mode,
        detail,
        "tracked admission refused for the member's live runtime mode; delivering untracked \
         on the ingress lane"
    );
    TurnTracking::Unavailable(match mode {
        meerkat_mob::MobRuntimeMode::AutonomousHost => TurnUntrackable::AutonomousHost,
        mode => TurnUntrackable::RefusedByRuntime { mode },
    })
}

/// Tracking reported for a ticketed delivery whose lane recorded none (the
/// identity has no session bridge or no bound runtime, so nothing was
/// delivered to track).
fn untracked_delivery() -> TurnTracking {
    TurnTracking::Unavailable(TurnUntrackable::NotDelivered)
}

/// What [`IdentityRuntime::send_core`] settled on.
struct SendCoreOutcome {
    token: FencingToken,
    completion_baseline: CompletionCursor,
    /// `Some` exactly when the send asked for [`SendCommitMode::TrackTurn`].
    turn: Option<TurnTracking>,
}

/// One invocation of the shared identity delivery state machine.
///
/// Keeping the lane-specific carriers in one value makes the common send body
/// explicit without growing a positional parameter list that is easy to wire
/// incorrectly when a new carrier is added.
struct SendRequest<'a> {
    expected_alias: Option<&'a str>,
    content: &'a meerkat_core::ContentInput,
    system_prompt: Option<&'a str>,
    handling_mode: HandlingMode,
    interaction_id: Option<&'a str>,
    commit_mode: SendCommitMode,
    console_human: Option<ConsoleHumanSend<'a>>,
}

/// Budget for ONE managed-topology wiring call to the session bridge (a wire,
/// unwire or wire inspection; each is a mob-actor round trip that also awaits
/// comms trust installs and peer notices). The reconcile or topology mutation
/// settles with [`IdentityRuntimeError::PeerWiringTimedOut`] instead of
/// parking behind a wedged call. No admission waits on these calls at all.
const PEER_WIRING_CALL_BUDGET: Duration = Duration::from_secs(30);

/// End-to-end budget for the wake path's target-only topology reconcile
/// (lock acquisition plus every wiring call), so a woken member's admission
/// is delayed by at most this much. A failure is logged and retried after
/// admission; it never refuses the input.
const WAKE_TOPOLOGY_BUDGET: Duration = Duration::from_secs(30);

/// How many times a managed-topology plan is made before a reconcile or
/// topology mutation gives up with
/// [`IdentityRuntimeError::PeerTopologySuperseded`] because an endpoint's
/// runtime binding kept changing while the plan's wiring ran.
const PEER_TOPOLOGY_MAX_PLAN_ATTEMPTS: u32 = 3;

/// Run one managed-topology bridge wiring call under
/// [`PEER_WIRING_CALL_BUDGET`].
async fn bounded_peer_wiring<T>(
    operation: &'static str,
    call: impl Future<Output = T>,
) -> Result<T, IdentityRuntimeError> {
    bounded_peer_wiring_by(operation, None, call).await
}

/// [`bounded_peer_wiring`], additionally capped by an optional end-to-end
/// `deadline` (the wake path's single budget for its whole target
/// reconcile). Whichever comes first ends the call as `PeerWiringTimedOut`,
/// through the same unsettled-edge bookkeeping as any other timeout.
async fn bounded_peer_wiring_by<T>(
    operation: &'static str,
    deadline: Option<tokio::time::Instant>,
    call: impl Future<Output = T>,
) -> Result<T, IdentityRuntimeError> {
    let start = tokio::time::Instant::now();
    let per_call = start + PEER_WIRING_CALL_BUDGET;
    let until = deadline.map_or(per_call, |deadline| deadline.min(per_call));
    tokio::time::timeout_at(until, call).await.map_err(|_| {
        IdentityRuntimeError::PeerWiringTimedOut {
            operation,
            waited: until.saturating_duration_since(start),
        }
    })
}

/// The runtime bindings a managed-topology plan was made against.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PeerBindingEpoch {
    /// Every identity with a continuity record, to its runtime id.
    known: BTreeMap<AgentIdentity, AgentRuntimeId>,
    /// The Active subset of `known`.
    active: BTreeMap<AgentIdentity, AgentRuntimeId>,
}

impl PeerBindingEpoch {
    /// Whether none of `identities` changed binding (runtime id or activity)
    /// between `self` and `current`.
    fn unchanged_for(&self, current: &Self, identities: &BTreeSet<AgentIdentity>) -> bool {
        identities.iter().all(|identity| {
            self.known.get(identity) == current.known.get(identity)
                && self.active.get(identity) == current.active.get(identity)
        })
    }
}

/// The binding epoch of an entries table (see [`PeerBindingEpoch`]).
fn peer_binding_epoch_of(entries: &BTreeMap<AgentIdentity, IdentityEntry>) -> PeerBindingEpoch {
    let mut epoch = PeerBindingEpoch::default();
    for (identity, entry) in entries {
        let Some(record) = entry.continuity.as_ref() else {
            continue;
        };
        epoch
            .known
            .insert(identity.clone(), record.agent_runtime_id.clone());
        if entry.state == IdentityLifecycleState::Active {
            epoch
                .active
                .insert(identity.clone(), record.agent_runtime_id.clone());
        }
    }
    epoch
}

/// Outcome of one plan / wire / revalidate pass over the managed topology.
enum PeerTopologyAttempt {
    Committed,
    /// An involved endpoint's binding changed during the wiring; nothing was
    /// committed and the plan must be remade.
    Stale,
}

/// Whether a send or dispatch result means meerkat admitted the input, so the
/// post-admission peer hydration it deferred is still owed. A completion or
/// post-admission failure is a turn that WAS admitted.
///
/// `LeaseLost` and `CompletionIncarnationChanged` are deliberately absent:
/// on the send and dispatch lanes a lease loss is only raised by the lease
/// checks that run BEFORE the bridge admission (the first one, and the
/// re-check after a reload-for-delivery), so nothing was admitted; and
/// `CompletionIncarnationChanged` is raised only by `wait_for_completion`,
/// which is outside these lanes. Neither is a turn that ran.
fn input_was_admitted<T>(result: &Result<T, IdentityRuntimeError>) -> bool {
    matches!(
        result,
        Ok(_)
            | Err(IdentityRuntimeError::CompletionFailed { .. }
                | IdentityRuntimeError::PostAdmissionResolutionFailed { .. }
                | IdentityRuntimeError::PostAdmissionSuperseded { .. })
    )
}

/// One target's in-flight post-admission hydration: the claim that owns it
/// and whether another pass was requested while it ran.
#[derive(Debug, Clone, Copy)]
struct PeerHydrationClaim {
    token: u64,
    rerun: bool,
}

/// In-flight post-admission hydrations, one per target, plus the token
/// counter that makes every claim distinguishable.
#[derive(Debug, Default)]
struct PeerHydrations {
    next_token: u64,
    in_flight: BTreeMap<AgentIdentity, PeerHydrationClaim>,
}

impl PeerHydrations {
    /// Claim `identity`'s slot, returning the claim token. `None` when a
    /// hydration is already in flight; that one is asked to run once more.
    fn claim(&mut self, identity: &AgentIdentity) -> Option<u64> {
        if let Some(claim) = self.in_flight.get_mut(identity) {
            claim.rerun = true;
            return None;
        }
        self.next_token = self.next_token.wrapping_add(1);
        let token = self.next_token;
        self.in_flight.insert(
            identity.clone(),
            PeerHydrationClaim {
                token,
                rerun: false,
            },
        );
        Some(token)
    }

    /// Release `identity`'s slot only if `token` still owns it, so a stale
    /// release (a second removal after a panic, or a refused spawn racing a
    /// newer claim) can never drop someone else's claim.
    fn release(&mut self, identity: &AgentIdentity, token: u64) {
        if self
            .in_flight
            .get(identity)
            .is_some_and(|claim| claim.token == token)
        {
            self.in_flight.remove(identity);
        }
    }
}

/// A claimed post-admission hydration slot; releasing it (also on panic or
/// cancellation) lets the next admission to that target hydrate again.
struct PeerHydrationSlot<'a> {
    hydrations: &'a StdMutex<PeerHydrations>,
    identity: AgentIdentity,
    token: u64,
}

impl PeerHydrationSlot<'_> {
    /// Finish one pass: under one lock, either consume a rerun request (keep
    /// the slot, return true) or release the slot (return false). Atomic, so
    /// a request that arrives as the pass ends is never lost.
    fn finish_pass(&self) -> bool {
        let mut hydrations = self
            .hydrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match hydrations.in_flight.get_mut(&self.identity) {
            Some(claim) if claim.token == self.token && claim.rerun => {
                claim.rerun = false;
                true
            }
            _ => {
                hydrations.release(&self.identity, self.token);
                false
            }
        }
    }

    /// Run hydration passes until no rerun is pending, then release.
    async fn run(self, runtime: &IdentityRuntime) {
        loop {
            #[cfg(test)]
            runtime
                .peer_hydration_passes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            runtime.hydrate_peer_topology_logged(&self.identity).await;
            if !self.finish_pass() {
                return;
            }
        }
    }
}

impl Drop for PeerHydrationSlot<'_> {
    /// Release this claim (a no-op once `finish_pass` released it, or when a
    /// newer claim owns the slot): covers a pass that panicked or was
    /// cancelled mid-flight, so the next admission can hydrate again.
    fn drop(&mut self) {
        self.hydrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .release(&self.identity, self.token);
    }
}

/// Whether a foreground materialization reconciles the whole managed peer
/// topology after the embodiment commits, or leaves that to the admission
/// path (which reconciles only the woken target's own edges before admission
/// and hydrates the rest after it; see
/// [`IdentityRuntime::materialize_for_admission`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopologyAfterEmbody {
    Reconcile,
    DeferToPostAdmission,
}

struct ConsoleHumanSend<'a> {
    delivery_identity: &'a meerkat_mob::MobDeliveryIdentity,
    expected_session: Option<&'a str>,
    /// Selected skills for this exact console turn; carried only on the
    /// local host-human lane.
    skill_references: &'a [meerkat_core::skills::SkillKey],
}

enum DeliveryPreparation {
    Work(meerkat_mob::MobRuntimeMode),
    ConsoleHuman,
}

/// Map errors observed before a completion-bearing receipt is available.
/// A deadline without a receipt does not establish whether admission ran.
fn admission_phase_error(
    identity: &AgentIdentity,
    err: BridgeAdmissionError,
) -> IdentityRuntimeError {
    let identity = identity.clone();
    match err {
        BridgeAdmissionError::HostHumanInput(error) => IdentityRuntimeError::HostHumanInput(error),
        BridgeAdmissionError::CompletionUnsupported(reason) => {
            IdentityRuntimeError::CompletionUnavailable { identity, reason }
        }
        BridgeAdmissionError::Mob(detail)
        | BridgeAdmissionError::UnsupportedForMode { detail, .. }
        | BridgeAdmissionError::InvalidInput(detail)
        | BridgeAdmissionError::InvariantViolation(detail) => {
            IdentityRuntimeError::AdmissionFailed { identity, detail }
        }
        BridgeAdmissionError::ActorLoopStalled {
            operation,
            stall_id,
            stalled_for,
            observation,
            ..
        } => IdentityRuntimeError::ActorLoopStalled {
            identity,
            operation,
            stall_id,
            stalled_for,
            observation,
        },
        BridgeAdmissionError::ActorTerminated {
            operation,
            detail,
            observation,
            ..
        } => IdentityRuntimeError::ActorTerminated {
            identity,
            operation,
            detail,
            observation,
        },
        BridgeAdmissionError::ReloadRequired { reason, .. } => {
            IdentityRuntimeError::ReloadRequired { identity, reason }
        }
        BridgeAdmissionError::AdmissionBacklogFull { depth, .. } => {
            IdentityRuntimeError::AdmissionBacklogFull { identity, depth }
        }
        BridgeAdmissionError::ReloadRefused {
            session_id, reason, ..
        } => IdentityRuntimeError::ReloadRefused {
            identity,
            session_id,
            reason,
        },
        BridgeAdmissionError::ReloadTimedOut {
            session_id, stage, ..
        } => IdentityRuntimeError::ReloadTimedOut {
            identity,
            session_id,
            stage,
        },
        BridgeAdmissionError::ActorAdmissionTimeout {
            operation,
            waited,
            command,
            ..
        } => IdentityRuntimeError::ActorAdmissionTimeout {
            identity,
            operation,
            waited,
            command,
        },
        other @ (BridgeAdmissionError::ResumeRejected { .. }
        | BridgeAdmissionError::ProviderAuthRejected { .. }) => {
            IdentityRuntimeError::AdmissionFailed {
                identity,
                detail: other.to_string(),
            }
        }
    }
}

/// Map the ingress-only lane's bridge error. The typed fast-fail refusals
/// (open stall, terminated loop, reload required) keep their type so callers
/// and the automatic reload can act on them; everything else keeps the
/// historical `Internal("bridge deliver: ..")` form.
fn ingress_phase_error(identity: &AgentIdentity, err: BridgeError) -> IdentityRuntimeError {
    let identity = identity.clone();
    match err {
        BridgeError::HostHumanInput(error) => IdentityRuntimeError::HostHumanInput(error),
        BridgeError::ActorAdmissionTimeout {
            operation,
            waited,
            command,
            ..
        } => IdentityRuntimeError::ActorAdmissionTimeout {
            identity,
            operation,
            waited,
            command,
        },
        BridgeError::ActorLoopStalled {
            operation,
            stall_id,
            stalled_for,
            observation,
            ..
        } => IdentityRuntimeError::ActorLoopStalled {
            identity,
            operation,
            stall_id,
            stalled_for,
            observation,
        },
        BridgeError::ActorTerminated {
            operation,
            detail,
            observation,
            ..
        } => IdentityRuntimeError::ActorTerminated {
            identity,
            operation,
            detail,
            observation,
        },
        BridgeError::ReloadRequired { reason, .. } => {
            IdentityRuntimeError::ReloadRequired { identity, reason }
        }
        BridgeError::AdmissionBacklogFull { depth, .. } => {
            IdentityRuntimeError::AdmissionBacklogFull { identity, depth }
        }
        BridgeError::ReloadRefused {
            session_id, reason, ..
        } => IdentityRuntimeError::ReloadRefused {
            identity,
            session_id,
            reason,
        },
        BridgeError::ReloadTimedOut {
            session_id, stage, ..
        } => IdentityRuntimeError::ReloadTimedOut {
            identity,
            session_id,
            stage,
        },
        other => IdentityRuntimeError::Internal(format!("bridge deliver: {other}")),
    }
}

/// Map the bridge's reload-primitive error. `Refused` and `TimedOut` keep
/// their class; everything else is an internal bridge failure.
fn reload_phase_error(identity: &AgentIdentity, err: BridgeError) -> IdentityRuntimeError {
    let identity = identity.clone();
    match err {
        typed @ (BridgeError::ActorAdmissionTimeout { .. }
        | BridgeError::ActorLoopStalled { .. }
        | BridgeError::ActorTerminated { .. }) => ingress_phase_error(&identity, typed),
        BridgeError::ReloadRefused {
            session_id, reason, ..
        } => IdentityRuntimeError::ReloadRefused {
            identity,
            session_id,
            reason,
        },
        BridgeError::ReloadTimedOut {
            session_id, stage, ..
        } => IdentityRuntimeError::ReloadTimedOut {
            identity,
            session_id,
            stage,
        },
        other => {
            IdentityRuntimeError::Internal(format!("bridge reload_member_registration: {other}"))
        }
    }
}

/// Why a member is being retired on the mob plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleRetireIntent {
    /// The identity leaves the fleet: memory harvest hooks run (session tail
    /// distillation, exit interview).
    Retire,
    /// Non-destructive reload: the same durable session is re-materialized
    /// immediately under the same lifecycle lock, so nothing rotates and the
    /// memory hooks must NOT run (an exit interview for a member that stays
    /// would be a lie).
    Reload,
}

/// Map the terminal edge of an admitted turn. Pre-admission outcomes are
/// unrepresentable in this input type.
fn turn_phase_error(identity: &AgentIdentity, err: BridgeTurnError) -> IdentityRuntimeError {
    let identity = identity.clone();
    match err {
        BridgeTurnError::CompletionFailed(detail) => {
            IdentityRuntimeError::CompletionFailed { identity, detail }
        }
        BridgeTurnError::PostAdmissionResolutionFailed(detail) => {
            IdentityRuntimeError::PostAdmissionResolutionFailed { identity, detail }
        }
    }
}

/// Errors from identity-first runtime operations.
#[derive(Debug)]
pub enum IdentityRuntimeError {
    HostHumanInput(super::bridge::HostHumanInputError),
    /// Target identity is not registered/active.
    UnknownIdentity(AgentIdentity),
    /// send() rejected: target is InternalOnly.
    NotAddressable(NotAddressable),
    /// PHASE 1 of 4. A completion-bearing send could not be honoured AT ALL.
    ///
    /// Nothing was submitted: either no bridge is installed, the identity has
    /// no bound runtime id, or the bridge is ingress-only. The completion lane
    /// fails closed here rather than degrading to the admission-only path -
    /// degrading would report success for a turn that was never awaited.
    ///
    /// Safe to fall back or fail; nothing ran.
    CompletionUnavailable {
        identity: AgentIdentity,
        reason: String,
    },
    /// PHASE 2 of 4. No completion-bearing receipt was obtained. This does
    /// not by itself establish execution fate or authorize a retry.
    AdmissionFailed {
        identity: AgentIdentity,
        detail: String,
    },
    /// An actor round trip was not observed within budget. Neither a missing
    /// reply nor a missing upstream execution flag proves nonexecution.
    ActorAdmissionTimeout {
        identity: AgentIdentity,
        operation: &'static str,
        waited: Duration,
        command: Option<super::bridge::ActorCommandTimeout>,
    },
    /// The actor-loop probe has an open stall. `observation` distinguishes
    /// refusal before this call from an in-flight observation that ended.
    /// A correlated recovery does not settle an in-flight call's fate.
    ActorLoopStalled {
        identity: AgentIdentity,
        operation: &'static str,
        stall_id: u64,
        stalled_for: Duration,
        observation: super::bridge::ActorCallObservation,
    },
    /// The actor-loop probe observed closed channels. An in-flight call may
    /// have progressed; only `BeforeCall` proves this call was not started.
    ActorTerminated {
        identity: AgentIdentity,
        operation: &'static str,
        detail: String,
        observation: super::bridge::ActorCallObservation,
    },
    /// meerkat refused admission because the member's runtime registration is
    /// durability-degraded and needs a registration-authorized cold reload.
    /// The runtime already ran ONE automatic non-destructive reload
    /// (`reload_member`) and retried; this is the typed failure after that
    /// single attempt (or the reload itself failing, with both reasons in
    /// `reason`). Nothing was delivered. Operator path: `mobkit/member_health`
    /// then `mobkit/reload_member`; never `respawn_member` (destructive).
    ReloadRequired {
        identity: AgentIdentity,
        reason: String,
    },
    /// Refused BEFORE admission: the member's per-member admission lane is
    /// full (meerkat 0.8.34 `MemberAdmissionBacklogFull`). Per-member
    /// backpressure, not a loop or durability verdict: RETRYABLE once the
    /// lane drains. Nothing was delivered.
    AdmissionBacklogFull {
        identity: AgentIdentity,
        depth: usize,
    },
    /// meerkat refused the member reload because the durable resume authority
    /// is still unreadable: the store is not healthy yet. The degraded
    /// registration is retained and the next send stays reload-required.
    /// RETRYABLE once the store is healthy; nothing was delivered.
    ReloadRefused {
        identity: AgentIdentity,
        session_id: String,
        reason: String,
    },
    /// meerkat's bounded member reload (`MEMBER_RELOAD_TOTAL_TIMEOUT`) timed
    /// out at `stage`. NOT retryable without inspection: the reload may have
    /// partially progressed. Nothing was delivered.
    ReloadTimedOut {
        identity: AgentIdentity,
        session_id: String,
        stage: String,
    },
    /// PHASE 3 of 4. The work WAS admitted and its turn reached a FAILED
    /// terminal.
    ///
    /// The turn RAN. Never retry - a retry would run the identity's turn a
    /// second time.
    CompletionFailed {
        identity: AgentIdentity,
        detail: String,
    },
    /// PHASE 4 of 4. The turn was admitted, reached a SUCCESSFUL terminal, and
    /// only then did projecting its session id fail.
    ///
    /// The member did the work. Non-retryable: repairing cannot undo a turn
    /// that already ran, and resubmitting would run it twice.
    PostAdmissionResolutionFailed {
        identity: AgentIdentity,
        detail: String,
    },
    /// The turn was admitted and ran, and the identity was RESET, RETIRED or
    /// REBOUND while it ran.
    ///
    /// Distinct from [`Self::PostAdmissionResolutionFailed`]: there the
    /// embodiment is intact and only the projection failed; here the embodiment
    /// the turn belonged to no longer exists. Non-retryable, and deliberately
    /// NOT reconciled - binding the live incarnation to a dead turn's session
    /// is worse than losing the attribution.
    PostAdmissionSuperseded {
        identity: AgentIdentity,
        detail: String,
    },
    /// Operation rejected: no active lease for this identity.
    NoActiveLease(AgentIdentity),
    /// Fail-closed single-embodiment guard: the identity's durable lease is
    /// held by another live runtime instance — a second live embodiment is
    /// refused, loudly and with the holder named. This is a POLICY point,
    /// not a structural assumption: multi-bind / forked-session semantics
    /// (future work) relax exactly this arm.
    AlreadyEmbodied {
        identity: AgentIdentity,
        holder: String,
    },
    /// Operation rejected: lease was lost.
    LeaseLost(AgentIdentity),
    /// Operation rejected: identity is not in a state that permits this operation.
    InvalidState {
        identity: AgentIdentity,
        state: IdentityLifecycleState,
        operation: &'static str,
    },
    /// Dispatch rejected BEFORE bridge admission: the delivery identity is
    /// half-formed (idempotency key without a correlation id, or vice
    /// versa) or fails upstream canonical validation (non-nil canonical
    /// UUID correlation). Fail-closed on purpose: a degraded delivery under
    /// a broken identity is a silent dedup hole - the caller gets dedup or
    /// this refusal, never at-least-once. NO delivery occurs.
    InvalidDeliveryIdentity {
        identity: AgentIdentity,
        detail: String,
    },
    /// Continuity store error.
    Store(ContinuityStoreError),
    /// Lease provider error.
    Lease(super::types::LeaseError),
    /// Duplicate identities in roster.
    DuplicateIdentity(AgentIdentity),
    /// Stale fencing token on checkpoint.
    StaleFencingToken {
        identity: AgentIdentity,
        presented: FencingToken,
        current: FencingToken,
    },
    /// Stale checkpoint version.
    StaleCheckpointVersion {
        identity: AgentIdentity,
        presented: CheckpointVersion,
        current: CheckpointVersion,
    },
    StaleContinuityGeneration {
        identity: AgentIdentity,
        presented: ContinuityGeneration,
        current: ContinuityGeneration,
    },
    /// A lifecycle request named an old generated runtime alias after the
    /// durable identity had already advanced to another generation.
    StaleRuntimeAlias {
        identity: AgentIdentity,
        requested: String,
        current: Option<AgentRuntimeId>,
    },
    /// A completion wait presented a baseline from a superseded runtime
    /// incarnation. Turn counts do not carry across incarnations, so this is
    /// reported rather than resolved either way — the caller must capture a
    /// fresh baseline instead of inferring that its turn did or did not run.
    CompletionIncarnationChanged {
        identity: AgentIdentity,
        baseline: CompletionCursor,
        observed: CompletionCursor,
    },
    /// The identity is parked because its build was DETERMINISTICALLY
    /// rejected: either the host answered the app-side `callback/build_agent`
    /// round trip with an error (the candidate-mode effect gate class), or
    /// provider authentication refused the build with a credential kind no
    /// unattended retry can change (a missing secret). No automatic retry: a
    /// roster/policy (spec) change clears the park; `reason` carries the
    /// rejection for operators.
    HostRejectedBuild {
        identity: AgentIdentity,
        reason: String,
    },
    /// A concrete member resume reached a typed terminal refusal. The
    /// continuity record is preserved and the failure remains attributable to
    /// this identity when an eager fleet restore converts it into a visible
    /// `RestoreOutcome::Broken`.
    EmbodimentRejected(Box<ContinuityFailure>),
    /// A managed-topology wiring call to the session bridge (a wire, unwire or
    /// wire inspection, each one a mob-actor round trip) was not observed
    /// within its budget. The reconcile or topology mutation that issued it
    /// settles with this instead of parking. The call's own effect is
    /// unknown; nothing was recorded as managed for it, and the next
    /// reconcile observes the live graph and converges.
    PeerWiringTimedOut {
        operation: &'static str,
        waited: Duration,
    },
    /// A managed-topology plan kept going stale: an endpoint's runtime binding
    /// changed (reset, respawn, retire) while its wiring calls ran, on every
    /// one of `attempts` replans. Nothing stale was committed; the next
    /// reconcile replans from the live bindings.
    PeerTopologySuperseded {
        attempts: u32,
    },
    /// Generic I/O or internal error.
    Internal(String),
}

impl std::fmt::Display for IdentityRuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HostHumanInput(error) => write!(f, "{error}"),
            Self::UnknownIdentity(id) => write!(f, "unknown identity: {id}"),
            Self::NotAddressable(err) => write!(f, "{err}"),
            Self::CompletionUnavailable { identity, reason } => write!(
                f,
                "completion-bearing send to {identity} cannot be honoured: {reason}; \
                 NOTHING was submitted"
            ),
            Self::AdmissionFailed { identity, detail } => write!(
                f,
                "completion-bearing send to {identity} did not obtain an admission receipt: {detail}"
            ),
            Self::ActorAdmissionTimeout {
                identity,
                operation,
                waited,
                command,
            } => super::bridge::fmt_actor_timeout(f, operation, identity, *waited, *command),
            Self::ActorLoopStalled {
                identity,
                operation,
                stall_id,
                stalled_for,
                observation,
            } => write!(
                f,
                "send to {identity}: the mob actor loop has an open stall (stall_id \
                 {stall_id}, open for {stalled_for:?}) during `{operation}`; {observation}; \
                 consult runtime fate before retrying an in-flight call"
            ),
            Self::ActorTerminated {
                identity,
                operation,
                detail,
                observation,
            } => write!(
                f,
                "send to {identity}: the mob actor loop TERMINATED ({detail}) so \
                 `{operation}` cannot continue to be observed; {observation}; \
                 this will not recover, the process must restart"
            ),
            Self::ReloadRequired { identity, reason } => write!(
                f,
                "send to {identity} refused: the member's runtime registration requires a \
                 registration-authorized cold reload and one automatic reload attempt did not \
                 clear it; nothing was delivered; run mobkit/reload_member (non-destructive), \
                 never respawn_member: {reason}"
            ),
            Self::AdmissionBacklogFull { identity, depth } => write!(
                f,
                "send to {identity} refused: the member's admission lane is full ({depth} \
                 deliveries parked); nothing was delivered; retryable once the lane drains"
            ),
            Self::ReloadRefused {
                identity,
                session_id,
                reason,
            } => write!(
                f,
                "reload of {identity} (session {session_id}) refused: store not healthy, reload \
                 refused, retry later; the registration was retained and sends stay \
                 reload-required until the store is healthy: {reason}"
            ),
            Self::ReloadTimedOut {
                identity,
                session_id,
                stage,
            } => write!(
                f,
                "reload of {identity} (session {session_id}) timed out at stage `{stage}` \
                 (MEMBER_RELOAD_TOTAL_TIMEOUT); not retryable without inspection: the reload may \
                 have partially progressed"
            ),
            Self::CompletionFailed { identity, detail } => write!(
                f,
                "completion-bearing send to {identity} was admitted and then failed: {detail}; \
                 the turn RAN - do not retry"
            ),
            Self::PostAdmissionResolutionFailed { identity, detail } => write!(
                f,
                "completion-bearing send to {identity} COMPLETED and then its session \
                 projection failed: {detail}; the turn RAN - do not retry"
            ),
            Self::PostAdmissionSuperseded { identity, detail } => write!(
                f,
                "completion-bearing send to {identity} COMPLETED, but the identity was \
                 superseded while it ran: {detail}; the turn RAN against an embodiment \
                 that no longer exists - do not retry, and its session was NOT rebound"
            ),
            Self::NoActiveLease(id) => write!(f, "no active lease for {id}"),
            Self::AlreadyEmbodied { identity, holder } => write!(
                f,
                "identity {identity} is already embodied by runtime instance '{holder}' \
                 (single-embodiment guard: refusing a second live bind)"
            ),
            Self::LeaseLost(id) => write!(f, "lease lost for {id}"),
            Self::InvalidState {
                identity,
                state,
                operation,
            } => write!(
                f,
                "cannot {operation} identity {identity} in state {state:?}"
            ),
            Self::InvalidDeliveryIdentity { identity, detail } => write!(
                f,
                "dispatch to {identity} refused before bridge admission: invalid delivery \
                 identity ({detail}); dedup or typed refusal, never a degraded delivery"
            ),
            Self::Store(err) => write!(f, "continuity store: {err}"),
            Self::Lease(err) => write!(f, "lease provider: {err}"),
            Self::DuplicateIdentity(id) => write!(f, "duplicate identity in roster: {id}"),
            Self::StaleFencingToken {
                identity,
                presented,
                current,
            } => write!(
                f,
                "stale fencing token for {identity}: presented {presented}, current {current}"
            ),
            Self::StaleCheckpointVersion {
                identity,
                presented,
                current,
            } => write!(
                f,
                "stale checkpoint version for {identity}: presented {presented}, current {current}"
            ),
            Self::StaleContinuityGeneration {
                identity,
                presented,
                current,
            } => write!(
                f,
                "stale continuity generation for {identity}: presented {presented}, current {current}"
            ),
            Self::StaleRuntimeAlias {
                identity,
                requested,
                current,
            } => write!(
                f,
                "stale runtime alias for {identity}: requested {requested}, current {}",
                current
                    .as_ref()
                    .map(AgentRuntimeId::as_str)
                    .unwrap_or("<none>")
            ),
            Self::CompletionIncarnationChanged {
                identity,
                baseline,
                observed,
            } => write!(
                f,
                "completion baseline {baseline} for {identity} belongs to a superseded runtime \
                 incarnation (now {observed}); capture a fresh baseline"
            ),
            Self::HostRejectedBuild { identity, reason } => write!(
                f,
                "identity {identity} is parked: its build was deterministically rejected \
                 ({reason}); a roster/policy (spec) change clears the park"
            ),
            Self::EmbodimentRejected(failure) => write!(
                f,
                "identity {} embodiment was rejected: {}",
                failure.identity, failure.detail
            ),
            Self::PeerWiringTimedOut { operation, waited } => write!(
                f,
                "managed topology wiring call {operation} was not observed within {waited:?}; \
                 its effect is unknown and nothing was recorded as managed for it"
            ),
            Self::PeerTopologySuperseded { attempts } => write!(
                f,
                "managed topology plan went stale on all {attempts} attempts: an endpoint's \
                 runtime binding changed while its wiring ran; nothing stale was committed"
            ),
            Self::Internal(msg) => write!(f, "internal: {msg}"),
        }
    }
}

/// Digest of the exact roster spec, for [`HostRejectedBuildPark`] scoping.
/// Process-local (std hasher over the canonical JSON form): the park itself
/// is in-memory, so cross-process stability is not required.
pub(crate) fn durable_spec_digest(spec: &DurableAgentSpec) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    match serde_json::to_string(spec) {
        Ok(json) => json.hash(&mut hasher),
        // A roster spec is plain data; serialization cannot realistically
        // fail. Degrade to "spec never changes" rather than panicking.
        Err(_) => spec.identity.as_str().hash(&mut hasher),
    }
    hasher.finish()
}

/// A build failure whose root cause is the HOST's own answer: the
/// `callback/build_agent` round trip COMPLETED and the app returned an error
/// (rpc_gateway mints `callback/build_agent failed: callback error: <host
/// message>` for exactly this case — see `StdioCallbackBridge::call`).
/// Transport-tier callback failures ("callback transport closed", timeouts,
/// dropped channels) deliberately do NOT match: those are retryable and stay
/// on the existing reconcile/repair lanes (their backoff is the upstream
/// meerkat fix, not this park).
pub(crate) fn is_host_rejected_build_error(detail: &str) -> bool {
    detail.contains("callback/build_agent failed: callback error:")
}

/// Whether a typed provider-auth kind is a DETERMINISTIC build wall: a fact
/// about configuration that no unattended retry can change, so the identity
/// parks with the reason instead of heal-looping through the continuity
/// repair supervisor (a missing key used to burn a full build per attempt,
/// forever, with the cause visible nowhere). The remaining kinds describe
/// credential STATE that can change out of band (a lease refreshes, a login
/// completes, an I/O fault clears), so they stay on the existing repair
/// lanes. Exhaustive on purpose: a new kind must be classified here.
pub(crate) const fn is_deterministic_provider_auth_kind(kind: meerkat_core::AuthErrorKind) -> bool {
    use meerkat_core::AuthErrorKind;
    match kind {
        AuthErrorKind::MissingSecret
        | AuthErrorKind::UnsupportedCombination
        | AuthErrorKind::MissingRequiredMetadata
        | AuthErrorKind::WorkspaceMismatch
        | AuthErrorKind::HostOwnedUnavailable => true,
        AuthErrorKind::StaleCredential
        | AuthErrorKind::RefreshRequired
        | AuthErrorKind::LeaseAbsent
        | AuthErrorKind::UserReauthRequired
        | AuthErrorKind::Expired
        | AuthErrorKind::RefreshFailed
        | AuthErrorKind::ResolveRequired
        | AuthErrorKind::InteractiveLoginRequired
        | AuthErrorKind::Io
        | AuthErrorKind::Other => false,
    }
}

/// The park reason for a build the bridge refused DETERMINISTICALLY, or
/// `None` when the failure may resolve on its own and belongs on the repair
/// lanes. Two walls are recognised: the typed provider-auth refusal
/// ([`BridgeError::ProviderAuthRejected`] with a kind
/// [`is_deterministic_provider_auth_kind`] accepts) and the host's own
/// `callback/build_agent` answer ([`is_host_rejected_build_error`], the one
/// place a string probe remains because `rpc_gateway` mints that text).
pub(crate) fn deterministic_build_rejection_reason(error: &BridgeError) -> Option<String> {
    match error {
        BridgeError::ProviderAuthRejected { failure, detail } => {
            is_deterministic_provider_auth_kind(failure.kind)
                .then(|| provider_auth_park_reason(failure, detail))
        }
        other => {
            let detail = other.to_string();
            is_host_rejected_build_error(&detail).then_some(detail)
        }
    }
}

/// One producer text for the provider-auth park so operators see a stable,
/// greppable reason with the operator path inline.
fn provider_auth_park_reason(
    failure: &meerkat_core::service::SessionProviderAuthFailure,
    detail: &str,
) -> String {
    format!(
        "{} (typed AuthErrorKind `{}`): a credential/binding fact no unattended retry can \
         change, so this identity is parked instead of heal-looping. Operator path: supply \
         the credential or fix the binding for provider `{}`, then restart the gateway (the \
         park is process-local) or change the roster spec. Refusal: {detail}",
        failure.public_message(),
        failure.kind.as_str(),
        failure.provider.as_str()
    )
}

impl std::error::Error for IdentityRuntimeError {}

impl IdentityRuntimeError {
    /// Secret-free owner observations for RPC errors and member health.
    /// Omitted execution/retry flags are not a nonexecution verdict.
    pub fn structured_data(&self) -> Option<serde_json::Value> {
        match self {
            Self::HostHumanInput(error) => Some(error.structured_data()),
            Self::ActorAdmissionTimeout {
                operation,
                waited,
                command,
                ..
            } => Some(super::bridge::actor_timeout_data(
                operation, *waited, *command,
            )),
            Self::ActorLoopStalled {
                operation,
                stall_id,
                observation,
                ..
            } => Some(serde_json::json!({
                "kind": "actor_loop_stalled", "operation": operation,
                "stall_id": stall_id, "observation": observation,
            })),
            Self::ActorTerminated {
                operation,
                observation,
                ..
            } => Some(serde_json::json!({
                "kind": "actor_terminated", "operation": operation,
                "observation": observation,
            })),
            Self::AdmissionBacklogFull { depth, .. } => Some(serde_json::json!({
                "kind": "mob_member_admission_backlog_full", "depth": depth,
            })),
            Self::ReloadRefused { .. } => Some(serde_json::json!({
                "kind": "mob_member_reload_refused",
            })),
            Self::ReloadTimedOut { stage, .. } => Some(serde_json::json!({
                "kind": "mob_member_reload_timed_out", "stage": stage,
            })),
            // The typed repair hold rides the same `durable_resume_hold` key
            // meerkat uses on every surface, so a caller classifies this
            // exactly as it would classify meerkat's own -32013 refusal.
            Self::EmbodimentRejected(failure)
                if failure.kind == ContinuityFailureKind::RepairRequired =>
            {
                Some(serde_json::json!({
                    "kind": "mob_member_session_repair_required",
                    meerkat_core::service::SessionError::DURABLE_RESUME_HOLD_KEY:
                        meerkat_core::service::DurableResumeHold::AuditedEndpointDivergence.as_str(),
                    "session_id": failure
                        .record
                        .as_ref()
                        .map(|record| record.session_id.to_string()),
                    "content_retained": true,
                    "retryable": false,
                    "operator_path": [
                        SessionRepairRequired::REPAIR_SUBCOMMAND,
                        "mobkit/reload_member",
                    ],
                }))
            }
            Self::PeerWiringTimedOut { operation, waited } => Some(serde_json::json!({
                "kind": "peer_wiring_timed_out", "operation": operation,
                "waited_ms": u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
            })),
            Self::PeerTopologySuperseded { attempts } => Some(serde_json::json!({
                "kind": "peer_topology_superseded", "attempts": attempts,
            })),
            _ => None,
        }
    }
}

impl From<ContinuityStoreError> for IdentityRuntimeError {
    fn from(err: ContinuityStoreError) -> Self {
        match err {
            ContinuityStoreError::StaleFencingToken {
                identity,
                presented,
                current,
            } => Self::StaleFencingToken {
                identity,
                presented,
                current,
            },
            ContinuityStoreError::StaleCheckpointVersion {
                identity,
                presented,
                current,
            } => Self::StaleCheckpointVersion {
                identity,
                presented,
                current,
            },
            ContinuityStoreError::StaleContinuityGeneration {
                identity,
                presented,
                current,
            } => Self::StaleContinuityGeneration {
                identity,
                presented,
                current,
            },
            other => Self::Store(other),
        }
    }
}

// ---------------------------------------------------------------------------
// Per-identity runtime state
// ---------------------------------------------------------------------------

/// Tracks the live state for a single identity within the runtime.
#[derive(Debug, Clone)]
pub(crate) struct IdentityEntry {
    pub spec: DurableAgentSpec,
    /// Bootstrap pass that last accepted `spec` as this entry's desired
    /// roster projection. Foreground materialization binds readiness writes
    /// to this entry-owned generation only after acquiring the lifecycle
    /// lock, so a pass that wins that lock cannot be mistaken for either its
    /// predecessor or successor merely because the global status epoch moved.
    pub bootstrap_generation: u64,
    pub state: IdentityLifecycleState,
    pub continuity: Option<ContinuityRecord>,
    pub lease: Option<LeaseEntry>,
    /// Exact authority that a completed lower-plane transition tried and
    /// failed to release. This is deliberately separate from `lease`: Broken
    /// identities must not advertise or renew active authority, but repair
    /// still needs the exact fencing token to retry the provider release
    /// before any reacquire attempt.
    pub pending_lease_release: Option<LeaseGrant>,
    pub checkpoint_version: CheckpointVersion,
    /// Whether a durable runtime_store is available (affects dispatch ack semantics).
    pub has_runtime_store: bool,
    /// Terminal heal verdict from the bridge's heal authority. While set,
    /// the continuity repair supervisor must not re-attempt this identity and
    /// reconcile must not cosmetically reset it to Dormant — either would
    /// restart the 2026-07-29 heal/re-Break loop. Cleared by any non-Broken
    /// lifecycle projection (a real recovery or an operator reset).
    pub continuity_unrecoverable: Option<ContinuityUnrecoverable>,
    /// Typed park for a build the host deterministically rejected (the
    /// candidate-mode effect gate class). While set AND the recorded spec
    /// digest still matches `spec`, materialization fails fast typed (no
    /// bridge/callback churn) and the repair supervisor skips the identity.
    /// A changed spec clears it — the retry is then permitted.
    pub host_rejected_build_park: Option<HostRejectedBuildPark>,
    /// Typed "session needs repair" hold: meerkat's audited-endpoint
    /// divergence reported by a resume, a registration reload, or the heal
    /// authority. While set, the repair supervisor skips the identity and
    /// reconcile keeps its Broken projection; the reload verb is the one
    /// door that re-attempts the resume (after the operator's repair).
    /// Cleared by any non-Broken lifecycle projection.
    pub session_repair_required: Option<SessionRepairRequired>,
    /// The most recent delivery failure for this identity, cleared by the
    /// next successful delivery. Read by `mobkit/member_health` so an
    /// operator learns WHY sends fail without spending an admission budget.
    pub last_delivery_error: Option<DeliveryErrorRecord>,
    /// The most recent reload attempt and how it ended (see `member_health`).
    pub last_reload: Option<ReloadAttemptRecord>,
}

/// Tracks a held lease for an identity.
#[derive(Debug, Clone)]
pub(crate) struct LeaseEntry {
    pub fencing_token: FencingToken,
    pub ttl: Duration,
    pub acquired_at: Instant,
}

impl LeaseEntry {
    pub fn is_expired(&self) -> bool {
        self.acquired_at.elapsed() > self.ttl
    }

    pub fn ttl_remaining(&self) -> Duration {
        self.ttl.saturating_sub(self.acquired_at.elapsed())
    }

    pub fn is_healthy(&self) -> bool {
        // Healthy if more than 20% TTL remains
        let remaining = self.ttl_remaining();
        remaining > self.ttl / 5
    }
}

// ---------------------------------------------------------------------------
// Identity-scoped events
// ---------------------------------------------------------------------------

/// Events emitted for a specific identity, used by `subscribe()`.
#[derive(Debug, Clone)]
pub enum IdentityEvent {
    /// Lifecycle state changed.
    StateChanged {
        identity: AgentIdentity,
        new_state: IdentityLifecycleState,
    },
    /// Lease acquired or renewed.
    LeaseUpdated {
        identity: AgentIdentity,
        fencing_token: FencingToken,
    },
    /// Lease lost.
    LeaseLost { identity: AgentIdentity },
    /// Checkpoint completed.
    CheckpointCompleted {
        identity: AgentIdentity,
        version: CheckpointVersion,
    },
    /// Resume could not reuse a persisted runtime binding and materialization
    /// fresh-spawned a member instead.
    ResumeFallback {
        identity: AgentIdentity,
        reason: super::bridge::ResumeFallbackReason,
    },
}

/// Per-identity event channel capacity.
const IDENTITY_EVENT_CHANNEL_CAPACITY: usize = 64;
const DEFAULT_LEASE_RENEWAL_MAX_POLL_INTERVAL: Duration = Duration::from_mins(1);
const DEFAULT_LEASE_RENEWAL_MIN_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// Base delay for the lease-renewal failure backoff. The renewal tick's
/// normal cadence is TTL-derived (down to a 10ms floor), so a lease provider
/// that errors persistently would otherwise retry — and warn — at that floor
/// rate. On failure we back off from this base, doubling toward the max poll
/// interval, so a backend outage can't spin the renewal task.
const LEASE_RENEWAL_FAILURE_BACKOFF_BASE: Duration = Duration::from_secs(1);

fn lease_renewal_failure_backoff(
    consecutive_failures: u32,
    max_poll_interval: Duration,
) -> Duration {
    LEASE_RENEWAL_FAILURE_BACKOFF_BASE
        .saturating_mul(1u32 << consecutive_failures.min(6))
        .min(max_poll_interval)
}

// ---------------------------------------------------------------------------
// IdentityRuntime
// ---------------------------------------------------------------------------

/// Configuration for the identity-first runtime.
pub struct IdentityRuntimeConfig {
    pub continuity_store: Arc<dyn ContinuityStore>,
    pub lease_provider: Arc<dyn LeaseProvider>,
    pub runtime_instance_id: String,
    pub has_runtime_store: bool,
    pub durability_policy: DurabilityPolicy,
    /// Optional session bridge for real session delivery. When `None`,
    /// delivery operations validate invariants but do not forward to
    /// the Meerkat session pipeline (useful for tests).
    pub bridge: Option<Arc<dyn SessionBridge>>,
    /// Default timeout for wait_for_output / wait_for_output_containing.
    /// Defaults to 90 seconds if not set.
    pub default_timeout: Option<Duration>,
}

#[derive(Clone)]
pub struct IdentityFirstRuntimeContext {
    pub runtime: Arc<IdentityRuntime>,
    pub roster_provider: Arc<dyn RosterProvider>,
    pub topology_provider: Option<Arc<dyn TopologyProvider>>,
    pub customizer: Option<Arc<dyn AgentCustomizer>>,
    mob_definition: Option<meerkat_mob::MobDefinition>,
    bootstrap_mode: IdentityBootstrapMode,
}

impl IdentityFirstRuntimeContext {
    pub(crate) async fn topology_snapshot_inputs(
        &self,
    ) -> Result<(Vec<DurableAgentSpec>, Vec<ManagedPeerEdge>), IdentityRuntimeError> {
        let previous_identities = self.runtime.registered_identities().await;
        let roster = self
            .roster_provider
            .roster(&RosterContext {
                mob_definition: self.mob_definition.clone(),
                previous_identities,
            })
            .await
            .map_err(|error| IdentityRuntimeError::Internal(format!("roster provider: {error}")))?;
        let identities = roster
            .iter()
            .map(|spec| spec.identity.clone())
            .collect::<Vec<_>>();
        let declared = match self.topology_provider.as_deref() {
            Some(provider) => provider
                .compute_edges(
                    &identities,
                    &TopologyContext {
                        roster: roster.clone(),
                    },
                )
                .await
                .map_err(|error| {
                    IdentityRuntimeError::Internal(format!("topology provider: {error}"))
                })?,
            None => self.runtime.desired_peer_edges.read().await.clone(),
        };
        Ok((roster, declared))
    }

    pub fn new(
        runtime: Arc<IdentityRuntime>,
        roster_provider: Arc<dyn RosterProvider>,
        topology_provider: Option<Arc<dyn TopologyProvider>>,
        customizer: Option<Arc<dyn AgentCustomizer>>,
        mob_definition: Option<meerkat_mob::MobDefinition>,
    ) -> Self {
        Self::new_with_lazy_materialization(
            runtime,
            roster_provider,
            topology_provider,
            customizer,
            mob_definition,
            false,
        )
    }

    pub fn new_with_lazy_materialization(
        runtime: Arc<IdentityRuntime>,
        roster_provider: Arc<dyn RosterProvider>,
        topology_provider: Option<Arc<dyn TopologyProvider>>,
        customizer: Option<Arc<dyn AgentCustomizer>>,
        mob_definition: Option<meerkat_mob::MobDefinition>,
        lazy_materialization: bool,
    ) -> Self {
        Self::new_with_bootstrap_mode(
            runtime,
            roster_provider,
            topology_provider,
            customizer,
            mob_definition,
            if lazy_materialization {
                IdentityBootstrapMode::LazyMaterialize
            } else {
                IdentityBootstrapMode::EagerMaterialize
            },
        )
    }

    /// Construct a context that preserves the complete startup policy across
    /// later roster reconciliation.
    pub fn new_with_bootstrap_mode(
        runtime: Arc<IdentityRuntime>,
        roster_provider: Arc<dyn RosterProvider>,
        topology_provider: Option<Arc<dyn TopologyProvider>>,
        customizer: Option<Arc<dyn AgentCustomizer>>,
        mob_definition: Option<meerkat_mob::MobDefinition>,
        bootstrap_mode: IdentityBootstrapMode,
    ) -> Self {
        runtime.set_reset_roster_provider_context(
            Some(roster_provider.clone()),
            mob_definition.clone(),
        );
        runtime.set_identity_edge_ownership(IdentityEdgeOwnership::for_topology_provider(
            topology_provider.is_some(),
        ));
        Self {
            runtime,
            roster_provider,
            topology_provider,
            customizer,
            mob_definition,
            bootstrap_mode,
        }
    }

    pub fn bootstrap_mode(&self) -> &IdentityBootstrapMode {
        &self.bootstrap_mode
    }

    /// Apply the configured bootstrap policy to an already-resolved roster.
    /// The same helper is used at startup and during reconcile so a lazy
    /// deployment can never accidentally hydrate the full fleet.
    pub async fn bootstrap_roster(
        &self,
        roster: &[DurableAgentSpec],
    ) -> Result<super::orchestrator::RestoreFlowResult, IdentityRuntimeError> {
        let _controller = self.runtime.bootstrap_controller.lock().await;
        let generation = self
            .runtime
            .begin_identity_bootstrap_pending(self.bootstrap_mode.clone());
        if let Err(error) = self.prepare_controlled_bootstrap().await {
            self.runtime.fail_identity_bootstrap(generation, &error);
            return Err(error);
        }
        self.apply_roster_controlled(generation, roster, RestorePassOrigin::Host)
            .await
    }

    /// Apply a roster under the runtime's single bootstrap controller.
    ///
    /// Startup and the public refresh API now share ONE restore path. The
    /// former split (bootstrap skipped snapshot payloads for opted-out
    /// bridges, public refresh always loaded them) meant console add-member
    /// and every reconcile pass paid a full session-blob read per Ready
    /// member for a payload nothing reads. `restore_flow` owns the single
    /// read-on-need rule; see its docs.
    async fn prepare_controlled_bootstrap(&self) -> Result<(), IdentityRuntimeError> {
        if self.runtime.bootstrap_shutdown.load(Ordering::Acquire) {
            return Err(IdentityRuntimeError::Internal(
                "identity bootstrap is shutting down".to_string(),
            ));
        }
        // Stop admitting new warm items and let any materialization already in
        // flight reach a transaction boundary before applying the next roster.
        self.runtime.request_identity_bootstrap_stop();
        self.runtime.join_identity_bootstrap_task().await;
        if self.runtime.bootstrap_shutdown.load(Ordering::Acquire) {
            return Err(IdentityRuntimeError::Internal(
                "identity bootstrap is shutting down".to_string(),
            ));
        }
        Ok(())
    }

    async fn apply_roster_controlled(
        &self,
        generation: u64,
        roster: &[DurableAgentSpec],
        origin: RestorePassOrigin,
    ) -> Result<super::orchestrator::RestoreFlowResult, IdentityRuntimeError> {
        // The whole pass is one embodiment burst, held until its settle
        // trigger below is sent: a deferred repair pass waiting for zero in
        // flight then consumes every trigger the pass raised, instead of
        // starting in a gap between members (or before `RestorePassSettled`)
        // and running once more on the late trigger.
        let _burst = self.runtime.enter_embodiment_burst();
        let result = self.apply_roster_pass(generation, roster).await;
        // A settled pass released every store writer it held, so a Broken
        // identity's retry can now succeed. The supervisor's own passes do not
        // re-trigger it (that would loop on a persistent failure); the
        // embodiments they settle do, through `EmbodimentSettled`.
        if origin == RestorePassOrigin::Host {
            self.runtime
                .trigger_continuity_repair(ContinuityRepairTrigger::RestorePassSettled);
        }
        result
    }

    async fn apply_roster_pass(
        &self,
        generation: u64,
        roster: &[DurableAgentSpec],
    ) -> Result<super::orchestrator::RestoreFlowResult, IdentityRuntimeError> {
        if let Err(message) = self.bootstrap_mode.validate() {
            let error = IdentityRuntimeError::Internal(message);
            self.runtime
                .begin_identity_bootstrap(generation, self.bootstrap_mode.clone(), roster)
                .await;
            self.runtime.fail_identity_bootstrap(generation, &error);
            return Err(error);
        }
        // Publish the new pass before any provider/restore await. RPC dispatch
        // is concurrent, so retaining the previous complete/ready snapshot here
        // would let a readiness waiter falsely pass while reconcile is active.
        self.runtime
            .begin_identity_bootstrap(generation, self.bootstrap_mode.clone(), roster)
            .await;
        // Converge the physical runtime before either restore policy publishes
        // the new roster. The restore flows only iterate desired identities;
        // without this boundary a removed member can remain live (and leased)
        // while the reduced bootstrap snapshot reports ready. Likewise, an
        // already-active member must not keep running an old build after its
        // desired spec changes.
        if let Err(error) = self
            .runtime
            .reconcile_roster_members(roster, generation)
            .await
        {
            self.runtime.fail_identity_bootstrap(generation, &error);
            return Err(error);
        }
        // One eager path for both startup and refresh: the snapshot-policy
        // twin is gone, so there is nothing left for the caller to select.
        let result = match &self.bootstrap_mode {
            IdentityBootstrapMode::EagerMaterialize => {
                super::orchestrator::restore_flow(
                    &self.runtime,
                    roster,
                    self.topology_provider.as_deref(),
                    self.customizer.as_deref(),
                )
                .await
            }
            IdentityBootstrapMode::LazyMaterialize
            | IdentityBootstrapMode::LazyWithBackgroundWarm { .. } => {
                super::orchestrator::lazy_register_flow(
                    &self.runtime,
                    roster,
                    self.topology_provider.as_deref(),
                )
                .await
            }
        };
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                self.runtime.fail_identity_bootstrap(generation, &error);
                return Err(error);
            }
        };
        if self.runtime.bootstrap_shutdown.load(Ordering::Acquire) {
            let error =
                IdentityRuntimeError::Internal("identity bootstrap is shutting down".to_string());
            self.runtime.fail_identity_bootstrap(generation, &error);
            return Err(error);
        }
        self.runtime
            .install_identity_bootstrap(generation, self.bootstrap_mode.clone(), roster, &result)
            .await;
        Ok(result)
    }

    pub async fn refresh_desired_topology(
        &self,
    ) -> Result<super::orchestrator::RestoreFlowResult, IdentityRuntimeError> {
        self.refresh_desired_topology_from(RestorePassOrigin::Host)
            .await
    }

    /// Ask the continuity repair supervisor for a pass now. The supervisor
    /// retries Broken identities only on typed triggers; this is the host's
    /// explicit one.
    pub async fn request_continuity_repair(&self) -> ContinuityRepairRequestOutcome {
        self.runtime
            .trigger_continuity_repair(ContinuityRepairTrigger::Requested);
        self.runtime.continuity_repair_request_outcome().await
    }

    async fn refresh_desired_topology_from(
        &self,
        origin: RestorePassOrigin,
    ) -> Result<super::orchestrator::RestoreFlowResult, IdentityRuntimeError> {
        // One runtime owns one controller from roster discovery through task
        // installation. Provider calls are user code and can be slow; publish
        // the in-flight pass before awaiting them so concurrent readiness RPCs
        // cannot observe the previous terminal snapshot.
        let _controller = self.runtime.bootstrap_controller.lock().await;
        let generation = self
            .runtime
            .begin_identity_bootstrap_pending(self.bootstrap_mode.clone());
        if let Err(error) = self.prepare_controlled_bootstrap().await {
            self.runtime.fail_identity_bootstrap(generation, &error);
            return Err(error);
        }
        let roster = match self
            .roster_provider
            .roster(&RosterContext {
                mob_definition: self.mob_definition.clone(),
                previous_identities: Vec::new(),
            })
            .await
        {
            Ok(roster) => roster,
            Err(err) => {
                let error = IdentityRuntimeError::Internal(format!("roster provider: {err}"));
                self.runtime.fail_identity_bootstrap(generation, &error);
                return Err(error);
            }
        };

        self.apply_roster_controlled(generation, &roster, origin)
            .await
    }

    /// Cancellation-safe reconcile for RPC/host request boundaries.
    /// Dropping the caller only drops its result receiver; the runtime owns
    /// the pass until every acquired lease and bridge mutation reaches an
    /// explicit commit or rollback boundary.
    pub async fn refresh_desired_topology_tracked(
        self: &Arc<Self>,
    ) -> Result<super::orchestrator::RestoreFlowResult, IdentityRuntimeError> {
        let context = Arc::clone(self);
        let runtime = Arc::clone(&self.runtime);
        runtime
            .run_tracked_foreground(async move { context.refresh_desired_topology().await })
            .await
    }

    /// Background repair for Broken identities. A rejected resume degrades the
    /// identity to Broken while preserving the durable session; the documented
    /// contract is "the next reconcile retries the resume" — but without this
    /// task, nothing runs that reconcile: delivery refuses Broken identities
    /// (REQ-13 fails loudly), `materialize` refuses the Broken state, and the
    /// only retries were a manual `mobkit/reconcile_identity` RPC or a process
    /// restart. A downstream app on 0.7.23 sat with 14 preserved-but-parked identities
    /// because of exactly that gap.
    ///
    /// The supervisor has no timer. It runs one pass when it starts (the boot
    /// pass has just settled) and then waits for a typed
    /// [`ContinuityRepairTrigger`]: another identity's embodiment settled (a
    /// resume or a fresh durable mint committed, releasing the store writer
    /// it held), a host restore or reconcile pass settled (boot, a roster
    /// refresh, `mobkit/reconcile_identity`), or a host asked
    /// ([`Self::request_continuity_repair`]). Only when at least one identity
    /// is Broken does a pass re-run [`Self::refresh_desired_topology`], the
    /// same idempotent flow the reconcile RPC runs (eager: `restore_flow`
    /// retries the resume; lazy: `lazy_register_flow` re-registers a
    /// store-Ready identity as Dormant so on-demand materialization retries).
    /// The supervisor's own passes never re-trigger it, so a persistent cause
    /// costs one pass per real event, and a transient one (store writer
    /// contention during a reseed boot) heals as soon as the contending
    /// embodiment settles instead of after a doubling sleep.
    pub fn spawn_broken_identity_repair_task(
        self: Arc<Self>,
        policy: ContinuityRepairPolicy,
    ) -> JoinHandle<()> {
        tokio::spawn(self.run_broken_identity_repair_loop(policy, None))
    }

    pub(crate) fn spawn_tracked_broken_identity_repair_task(
        self: Arc<Self>,
        policy: ContinuityRepairPolicy,
    ) -> TrackedContinuityRepairTask {
        let (cancel, receiver) = watch::channel(false);
        let join = tokio::spawn(self.run_broken_identity_repair_loop(policy, Some(receiver)));
        TrackedContinuityRepairTask { cancel, join }
    }

    async fn run_broken_identity_repair_loop(
        self: Arc<Self>,
        _policy: ContinuityRepairPolicy,
        mut cancellation: Option<watch::Receiver<bool>>,
    ) {
        let _supervising = ContinuityRepairSupervising::enter(&self.runtime);
        // Subscribe before the first pass so a trigger landing during it
        // still wakes the next wait.
        let mut triggers = self.runtime.subscribe_continuity_repair_triggers();
        // Bounded non-identical retries (production 0.8.12-era evidence): a repair
        // pass whose failure comes back byte-identical N times in a row is a
        // deterministic wall, and each blind retry re-executes the pass's
        // DESTRUCTIVE dispose steps against the same blocking precondition.
        // Track consecutive per-identity failure signatures and park typed
        // after [`REPAIR_IDENTICAL_FAILURE_PARK_ATTEMPTS`].
        let mut identical_failure_streaks: HashMap<AgentIdentity, (String, u32)> = HashMap::new();
        // The last-resort wake for transient causes with no typed signal
        // ([`crate::fallback_wake`]); disarmed while nothing transient stands.
        let mut fallback = crate::fallback_wake::FallbackWake::default();
        // The supervisor starts once the boot pass settled, which is itself a
        // trigger: one pass up front, then only on typed triggers.
        let mut wake = Some(RepairWake::Trigger(
            ContinuityRepairTrigger::RestorePassSettled,
        ));
        loop {
            let current = match wake.take() {
                Some(current) => current,
                None => tokio::select! {
                    changed = triggers.changed() => match changed {
                        Ok(()) => match triggers.borrow_and_update().last {
                            Some(trigger) => RepairWake::Trigger(trigger),
                            None => continue,
                        },
                        Err(_) => return,
                    },
                    () = crate::fallback_wake::sleep_until(fallback.next_wake()),
                        if fallback.next_wake().is_some() => RepairWake::Fallback,
                    () = repair_cancelled(&mut cancellation) => return,
                },
            };
            // Another embodiment in flight contends for the same store
            // writers, and its settling is itself a trigger: defer the pass
            // until none is in flight (the typed in-flight count reaching
            // zero), so a warm-up burst of settles runs one pass, not one per
            // member, and cannot trip the identical-failure park in seconds.
            if !self.runtime.broken_identities().await.is_empty()
                && !self
                    .runtime
                    .embodiments_settled_or_cancelled(&mut cancellation)
                    .await
            {
                return;
            }
            // Triggers that landed before this pass are covered by it.
            triggers.borrow_and_update();
            match self
                .continuity_repair_pass(&mut identical_failure_streaks, current)
                .await
            {
                RepairPassVerdict::Settled => fallback.reset(),
                RepairPassVerdict::Transient => {
                    fallback.record_failure(tokio::time::Instant::now());
                }
            }
            if cancellation
                .as_ref()
                .is_some_and(|cancellation| *cancellation.borrow())
            {
                return;
            }
        }
    }

    /// One repair pass over the Broken identities; reports whether a
    /// transient cause with no typed signal still stands.
    async fn continuity_repair_pass(
        &self,
        identical_failure_streaks: &mut HashMap<AgentIdentity, (String, u32)>,
        wake: RepairWake,
    ) -> RepairPassVerdict {
        let broken = self.runtime.broken_identities().await;
        if broken.is_empty() {
            return RepairPassVerdict::Settled;
        }
        // Heal must be REAL before reconcile runs: reconcile alone only
        // resets the runtime entry (lazy mode re-registers Dormant) while
        // the durable head can stay an intra-turn projection that the
        // next materialization re-Breaks — the measured 2026-07-29
        // production heal/re-Break loop. Drive the bridge's heal
        // authority FIRST; only identities whose durable head is (now)
        // committed — or whose bridge has no heal seam — proceed.
        let mut repairable = Vec::new();
        let mut recovery_failures = 0usize;
        for identity in &broken {
            if self
                .runtime
                .continuity_unrecoverable(identity)
                .await
                .is_some()
            {
                // Terminal typed verdict already recorded: stable across
                // calls, so re-healing every cycle is exactly the loop
                // this replaces. Operators act on the surfaced reason.
                continue;
            }
            if self
                .runtime
                .session_repair_required(identity)
                .await
                .is_some()
            {
                // The durable document needs the operator's sanctioned
                // repair; the heal authority cannot even read it. Only
                // `reload_member` after the repair re-attempts the resume.
                continue;
            }
            if self
                .runtime
                .host_rejected_build_park(identity)
                .await
                .is_some()
            {
                // The host's gate rejects this exact spec
                // deterministically: reattempting re-asks the same
                // question and burns a build + callback round trip per
                // cycle. A spec change clears the park (checked inside
                // the accessor); until then this identity is parked, not
                // repaired.
                continue;
            }
            match self.attempt_committed_boundary_recovery(identity).await {
                BrokenRepairDisposition::Repairable => repairable.push(identity.clone()),
                BrokenRepairDisposition::Unprovable | BrokenRepairDisposition::RepairRequired => {}
                BrokenRepairDisposition::RetryLater => recovery_failures += 1,
            }
        }
        if repairable.is_empty() {
            // Nothing eligible this pass: every Broken identity carries a
            // terminal verdict (no reconcile churn), or the heal authority
            // itself failed transiently, which has no typed "reachable again"
            // signal: arm the fallback wake.
            if recovery_failures > 0 {
                tracing::debug!(
                    recovery_failures,
                    "continuity repair: heal authority unavailable; arming the fallback wake"
                );
                return RepairPassVerdict::Transient;
            }
            return RepairPassVerdict::Settled;
        }
        tracing::info!(
            broken = repairable.len(),
            wake = wake.label(),
            "continuity repair: retrying restore for Broken identities"
        );
        let pass = match self
            .refresh_desired_topology_from(RestorePassOrigin::ContinuityRepair)
            .await
        {
            Ok(pass) => pass,
            Err(err) => {
                // A roster provider or store blip: no typed signal says it
                // cleared, so the fallback wake retries it.
                tracing::warn!(
                    error = %err,
                    "continuity repair reconcile failed; arming the fallback wake"
                );
                return RepairPassVerdict::Transient;
            }
        };
        let still_broken = self.runtime.repairable_broken_identities().await;
        // A member still Broken on an unreachable store has no typed "store
        // healthy" signal either; every other cause is retried by a typed
        // trigger (a settled embodiment, a settled pass, a caller's demand).
        let store_unavailable = still_broken.iter().any(|identity| {
            matches!(
                pass.outcomes.get(identity),
                Some(super::orchestrator::RestoreOutcome::Broken(failure))
                    if failure.kind == ContinuityFailureKind::StoreUnavailable
            )
        });
        let healed = repairable
            .iter()
            .filter(|id| !still_broken.contains(id))
            .count();
        if healed > 0 {
            tracing::info!(
                healed,
                still_broken = still_broken.len(),
                "continuity repair healed identities"
            );
        }
        for identity in &repairable {
            if !still_broken.contains(identity) {
                identical_failure_streaks.remove(identity);
                continue;
            }
            let Some(super::orchestrator::RestoreOutcome::Broken(failure)) =
                pass.outcomes.get(identity)
            else {
                // No comparable typed failure for this pass (the identity
                // broke through a different door); a streak cannot be
                // byte-compared across shapes.
                identical_failure_streaks.remove(identity);
                continue;
            };
            let signature = format!("{:?}: {}", failure.kind, failure.detail);
            let streak = identical_failure_streaks
                .entry(identity.clone())
                .or_insert_with(|| (signature.clone(), 0));
            if streak.0 == signature {
                streak.1 += 1;
            } else {
                *streak = (signature.clone(), 1);
            }
            if streak.1 >= REPAIR_IDENTICAL_FAILURE_PARK_ATTEMPTS {
                identical_failure_streaks.remove(identity);
                tracing::error!(
                    %identity,
                    attempts = REPAIR_IDENTICAL_FAILURE_PARK_ATTEMPTS,
                    blocking_failure = %signature,
                    "continuity repair failed byte-identically on every attempt; \
                     parking the identity typed instead of re-executing destructive \
                     repair steps on a timer. Operator path back after fixing the \
                     blocking failure: restart the gateway (the park is \
                     process-local; boot re-attempts repair once) or reset the \
                     identity via `mobkit/reset` (deliberate fresh start)"
                );
                if !self
                    .runtime
                    .mark_continuity_unrecoverable(
                        identity,
                        format!(
                            "continuity repair parked after \
                             {REPAIR_IDENTICAL_FAILURE_PARK_ATTEMPTS} consecutive \
                             byte-identical repair failures; blocking failure: \
                             {signature}. After fixing it, restart the gateway \
                             (process-local park; boot re-attempts repair) or reset \
                             the identity via `mobkit/reset`"
                        ),
                    )
                    .await
                {
                    tracing::debug!(
                        %identity,
                        "identity left Broken before the repair park could be recorded"
                    );
                }
            }
        }
        if store_unavailable || recovery_failures > 0 {
            RepairPassVerdict::Transient
        } else {
            RepairPassVerdict::Settled
        }
    }

    /// Ask the bridge's heal authority to drive this Broken identity's
    /// durable session head to a strict-resume-acceptable committed boundary,
    /// and translate the verdict into what the repair pass may do next.
    async fn attempt_committed_boundary_recovery(
        &self,
        identity: &AgentIdentity,
    ) -> BrokenRepairDisposition {
        let Some(bridge) = self.runtime.bridge() else {
            // Metadata-only runtime (tests): reconcile owns the retry.
            return BrokenRepairDisposition::Repairable;
        };
        let Some(session_id) = self.runtime.continuity_session_id(identity).await else {
            // No durable session bound (e.g. the store failed before a record
            // existed): there is no head to heal; reconcile retries as before.
            return BrokenRepairDisposition::Repairable;
        };
        match bridge.recover_committed_boundary(&session_id).await {
            Ok(
                CommittedBoundaryRepair::AlreadyCommitted | CommittedBoundaryRepair::Unsupported,
            ) => BrokenRepairDisposition::Repairable,
            Ok(CommittedBoundaryRepair::Recovered) => {
                tracing::info!(
                    %identity,
                    %session_id,
                    "continuity heal: recovery persisted a committed durable head; \
                     proceeding to reconcile"
                );
                BrokenRepairDisposition::Repairable
            }
            Ok(CommittedBoundaryRepair::Unprovable { reason }) => {
                tracing::error!(
                    %identity,
                    %session_id,
                    reason = %reason,
                    "continuity heal verdict: durable head unprovable; parking the \
                     identity as Broken until an operator intervenes"
                );
                if !self
                    .runtime
                    .mark_continuity_unrecoverable(identity, reason)
                    .await
                {
                    // The entry left Broken between the read and the mark
                    // (an operator reset raced us); nothing to park.
                    tracing::debug!(
                        %identity,
                        "unprovable verdict arrived after the identity left Broken"
                    );
                }
                BrokenRepairDisposition::Unprovable
            }
            Ok(CommittedBoundaryRepair::RepairRequired { session_id, detail }) => {
                tracing::error!(
                    %identity,
                    %session_id,
                    detail = %detail,
                    "continuity heal verdict: the durable document needs the sanctioned \
                     audited-endpoint repair; parking the identity typed until the operator \
                     repairs it and runs reload_member"
                );
                if !self
                    .runtime
                    .mark_session_repair_required(identity, session_id, detail)
                    .await
                {
                    tracing::debug!(
                        %identity,
                        "repair-required verdict arrived after the identity left Broken"
                    );
                }
                BrokenRepairDisposition::RepairRequired
            }
            Err(error) => {
                // Only the error tier is retryable per the heal contract
                // (Busy mid-turn, store I/O, CAS races).
                tracing::warn!(
                    %identity,
                    %session_id,
                    error = %error,
                    "committed-boundary recovery failed; retrying next repair pass"
                );
                BrokenRepairDisposition::RetryLater
            }
        }
    }
}

/// What the repair pass may do with one Broken identity after consulting the
/// bridge's heal authority.
enum BrokenRepairDisposition {
    /// Reconcile may retry this identity now (head committed, recovered, or
    /// no heal seam to consult).
    Repairable,
    /// Terminal typed verdict recorded; excluded until an operator clears it.
    Unprovable,
    /// Typed repair hold recorded: the durable document needs the operator's
    /// sanctioned audited-endpoint repair; excluded until `reload_member`
    /// resumes it after that repair.
    RepairRequired,
    /// The recovery attempt itself failed transiently; retry next pass.
    RetryLater,
}

/// Runtime-owned repair supervisor with cooperative idle cancellation.
///
/// Cancellation is observed while sleeping or between passes. An active
/// restore pass is joined to its explicit commit/rollback boundary instead of
/// being raw-aborted after lease acquisition.
pub(crate) struct TrackedContinuityRepairTask {
    cancel: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl TrackedContinuityRepairTask {
    pub(crate) fn cancel(&self) {
        let _ = self.cancel.send(true);
    }

    pub(crate) async fn cancel_and_join(self) {
        self.cancel();
        let _ = self.join.await;
    }
}

/// Runtime-owned lease-renewal supervisor with cooperative cancellation.
///
/// Cancellation is observed while the supervisor is idle or between ticks.
/// An in-flight renewal is always joined through publication of the provider's
/// returned fencing token so final shutdown releases current authority.
pub(crate) struct TrackedLeaseRenewalTask {
    cancel: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl TrackedLeaseRenewalTask {
    pub(crate) fn cancel(&self) {
        let _ = self.cancel.send(true);
    }

    pub(crate) async fn cancel_and_join(self) {
        self.cancel();
        let _ = self.join.await;
    }
}

/// Consecutive byte-identical repair failures tolerated for one identity
/// before the repair supervisor parks it typed
/// ([`IdentityRuntime::mark_continuity_unrecoverable`]) instead of
/// re-executing the pass's destructive dispose steps on a timer.
const REPAIR_IDENTICAL_FAILURE_PARK_ATTEMPTS: u32 = 3;

/// Configuration for [`IdentityFirstRuntimeContext::spawn_broken_identity_repair_task`].
///
/// The supervisor has no timer: it retries Broken identities when a typed
/// [`ContinuityRepairTrigger`] fires, so there is no cadence to configure.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ContinuityRepairPolicy {}

/// What woke the continuity repair supervisor. Each is an event after which
/// a Broken identity's retry can succeed where the last attempt failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuityRepairTrigger {
    /// Another identity's embodiment settled: a resume, or a fresh durable
    /// mint, committed, releasing the store writer it held.
    EmbodimentSettled,
    /// A restore or reconcile pass the supervisor did not run itself (boot, a
    /// roster refresh, `mobkit/reconcile_identity`) finished, releasing every
    /// writer it held.
    RestorePassSettled,
    /// A host asked for a repair pass
    /// ([`IdentityFirstRuntimeContext::request_continuity_repair`],
    /// `mobkit/request_continuity_repair`).
    Requested,
    /// A caller's delivery or materialization was refused because the
    /// identity is Broken: real demand for the identity.
    DemandOnBroken,
}

/// Who ran a restore pass, so the repair supervisor's own passes do not
/// trigger it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestorePassOrigin {
    /// Boot, a roster refresh, or `mobkit/reconcile_identity`.
    Host,
    /// The continuity repair supervisor.
    ContinuityRepair,
}

/// What woke one continuity repair pass (for the operator log).
#[derive(Debug, Clone, Copy)]
enum RepairWake {
    Trigger(ContinuityRepairTrigger),
    /// The last-resort wake armed by a transient cause with no typed signal.
    Fallback,
}

impl RepairWake {
    fn label(self) -> &'static str {
        match self {
            Self::Trigger(ContinuityRepairTrigger::EmbodimentSettled) => "embodiment_settled",
            Self::Trigger(ContinuityRepairTrigger::RestorePassSettled) => "restore_pass_settled",
            Self::Trigger(ContinuityRepairTrigger::Requested) => "requested",
            Self::Trigger(ContinuityRepairTrigger::DemandOnBroken) => "demand_on_broken",
            Self::Fallback => "fallback_wake",
        }
    }
}

/// Whether a repair pass left a transient cause that has no typed signal
/// (an unreachable heal authority, a failed pass, an unavailable store).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepairPassVerdict {
    Settled,
    Transient,
}

/// Resolves when the supervisor is cancelled or its owner dropped the
/// cancellation sender; never for an uncancellable supervisor.
async fn repair_cancelled(cancellation: &mut Option<watch::Receiver<bool>>) {
    let Some(cancellation) = cancellation.as_mut() else {
        return std::future::pending().await;
    };
    loop {
        if *cancellation.borrow_and_update() {
            return;
        }
        if cancellation.changed().await.is_err() {
            return;
        }
    }
}

/// Marks a continuity repair supervisor as running for its lifetime, so a
/// repair request can report whether anything will act on it.
struct ContinuityRepairSupervising(Arc<IdentityRuntime>);

impl ContinuityRepairSupervising {
    fn enter(runtime: &Arc<IdentityRuntime>) -> Self {
        runtime
            .continuity_repair_supervisors
            .fetch_add(1, Ordering::AcqRel);
        Self(Arc::clone(runtime))
    }
}

impl Drop for ContinuityRepairSupervising {
    fn drop(&mut self) {
        self.0
            .continuity_repair_supervisors
            .fetch_sub(1, Ordering::AcqRel);
    }
}

/// Counts one embodiment, or one embodiment burst (a restore pass, a
/// background warm), in flight for its lifetime (including a cancelled one),
/// publishing the count so the repair supervisor can wait for zero.
pub(super) struct EmbodimentInFlight<'a>(&'a watch::Sender<usize>);

impl<'a> EmbodimentInFlight<'a> {
    fn enter(count: &'a watch::Sender<usize>) -> Self {
        count.send_modify(|count| *count += 1);
        Self(count)
    }
}

impl Drop for EmbodimentInFlight<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|count| *count = count.saturating_sub(1));
    }
}

/// What a continuity repair request reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityRepairRequestOutcome {
    /// A running supervisor will run a pass for the repairable Broken
    /// identities.
    Scheduled,
    /// No identity is Broken in a repairable way; there is nothing to repair.
    NothingToRepair,
    /// Repairable identities are Broken but no repair supervisor runs in this
    /// runtime (an embedding that never started one).
    NoSupervisor,
}

/// The supervisor's wake signal: a monotonic epoch and the latest cause.
#[derive(Debug, Clone, Copy, Default)]
struct ContinuityRepairWake {
    epoch: u64,
    last: Option<ContinuityRepairTrigger>,
}

/// Weak keyed locks serialize claims on the shared raw/durable alias
/// namespace without retaining every caller-chosen alias for the lifetime of
/// the runtime. Sweeps grow geometrically while many aliases are live (as in
/// fleet bootstrap), avoiding a full-map scan for every new roster member.
struct RawMemberAliasLockTable {
    entries: BTreeMap<String, Weak<Mutex<()>>>,
    next_sweep_len: usize,
    #[cfg(test)]
    sweep_count: usize,
}

impl Default for RawMemberAliasLockTable {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            next_sweep_len: RAW_MEMBER_ALIAS_LOCK_SWEEP_MIN,
            #[cfg(test)]
            sweep_count: 0,
        }
    }
}

impl RawMemberAliasLockTable {
    fn sweep_if_needed(&mut self) {
        if self.entries.len() < self.next_sweep_len {
            return;
        }
        self.entries.retain(|_, lock| lock.upgrade().is_some());
        self.next_sweep_len = self
            .entries
            .len()
            .saturating_mul(2)
            .max(RAW_MEMBER_ALIAS_LOCK_SWEEP_MIN);
        #[cfg(test)]
        {
            self.sweep_count += 1;
        }
    }
}

/// The identity-first runtime tracks active identities and enforces delivery,
/// ownership, and lifecycle invariants.
/// Observes a live member moving from one session to another while it stays
/// the same member incarnation: the continuity rebind after a respawn or
/// after a delivery repair that rotated the member's session.
#[async_trait::async_trait]
pub(crate) trait MemberSessionRotationObserver: Send + Sync {
    /// `member` is the member's mob roster identity.
    async fn member_session_rotated(
        &self,
        member: &meerkat_mob::ids::AgentIdentity,
        previous: &SessionId,
        current: &SessionId,
    );
}

/// Run completions of a member's actor that already happened but are not
/// counted yet: the session's events that meerkat retains past the point the
/// identity health monitor credited. [`IdentityRuntime::completion_cursor`]
/// credits them before every read.
#[async_trait::async_trait]
pub(crate) trait PendingCompletionDrain: Send + Sync {
    /// Consume the uncounted events of `session_id` published so far and
    /// return the run terminals among them.
    async fn drain(&self, session_id: &SessionId) -> PendingRunTerminals;
}

/// Run terminals a [`PendingCompletionDrain`] consumed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PendingRunTerminals {
    /// Completed runs.
    pub(crate) completed: u64,
    /// Failed (or cancelled) runs.
    pub(crate) failed: u64,
}

/// The identity table, with a change signal: releasing a write lock wakes
/// the completion waiters ([`IdentityRuntime::await_completion`]), which
/// re-derive their cursor from the table (a lease incarnation change is an
/// entry write). The signal carries no state and is never read as an event;
/// a wake only means "re-read now".
struct IdentityEntries {
    table: RwLock<BTreeMap<AgentIdentity, IdentityEntry>>,
    changed: watch::Sender<()>,
    /// Each identity's current lease fencing token, as of the last write.
    /// A write that moves it (a lease acquired, rotated or released) wakes
    /// the [`Self::lease_observer`].
    lease_tokens: StdMutex<BTreeMap<AgentIdentity, u64>>,
    /// Told (an epoch bump) whenever any identity's lease fencing token
    /// changes. The identity health monitor attaches a member's stream only
    /// once the member's lease exists, so this is its typed wake for that
    /// moment ([`IdentityRuntime::install_lease_observer`]).
    lease_observer: StdRwLock<Option<watch::Sender<u64>>>,
}

impl IdentityEntries {
    fn new() -> Self {
        Self {
            table: RwLock::new(BTreeMap::new()),
            changed: watch::channel(()).0,
            lease_tokens: StdMutex::new(BTreeMap::new()),
            lease_observer: StdRwLock::new(None),
        }
    }

    async fn read(
        &self,
    ) -> tokio::sync::RwLockReadGuard<'_, BTreeMap<AgentIdentity, IdentityEntry>> {
        self.table.read().await
    }

    async fn write(&self) -> IdentityEntriesWriteGuard<'_> {
        IdentityEntriesWriteGuard {
            guard: self.table.write().await,
            entries: self,
        }
    }

    /// Wake every waiter subscribed through [`Self::subscribe`].
    fn notify(&self) {
        self.changed.send_replace(());
    }

    fn subscribe(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    /// Compare the table's lease fencing tokens with the last published ones
    /// and wake the lease observer when any moved.
    fn publish_lease_tokens(&self, table: &BTreeMap<AgentIdentity, IdentityEntry>) {
        let leased = || {
            table.iter().filter_map(|(identity, entry)| {
                entry
                    .lease
                    .as_ref()
                    .map(|lease| (identity, lease.fencing_token.get()))
            })
        };
        let moved = {
            let mut published = self
                .lease_tokens
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Compared in place (both sides are identity-ordered); the map is
            // rebuilt only when a lease actually moved.
            let unchanged = leased().count() == published.len()
                && leased().zip(published.iter()).all(
                    |((identity, token), (published_identity, published_token))| {
                        identity == published_identity && token == *published_token
                    },
                );
            if !unchanged {
                *published = leased()
                    .map(|(identity, token)| (identity.clone(), token))
                    .collect();
            }
            !unchanged
        };
        if moved {
            self.wake_lease_observer();
        }
    }

    fn wake_lease_observer(&self) {
        if let Some(observer) = self
            .lease_observer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            observer.send_modify(|epoch| *epoch = epoch.wrapping_add(1));
        }
    }
}

struct IdentityEntriesWriteGuard<'a> {
    guard: tokio::sync::RwLockWriteGuard<'a, BTreeMap<AgentIdentity, IdentityEntry>>,
    entries: &'a IdentityEntries,
}

impl std::ops::Deref for IdentityEntriesWriteGuard<'_> {
    type Target = BTreeMap<AgentIdentity, IdentityEntry>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl std::ops::DerefMut for IdentityEntriesWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for IdentityEntriesWriteGuard<'_> {
    fn drop(&mut self) {
        self.entries.publish_lease_tokens(&self.guard);
        self.entries.changed.send_replace(());
    }
}

/// Who owns the live peer edges between a runtime's identities.
///
/// Its identities' wiring custody is external-managed: meerkat's identity
/// reconciler abstains and the topology layer is the desired-state owner.
/// What that layer can claim depends on whether a topology provider declares
/// the topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdentityEdgeOwnership {
    /// No topology provider: the desired set covers only what was declared
    /// to this runtime, and other edges (for example the mob definition's
    /// spawn-time wiring) are not its to remove. A reconcile unwires only the
    /// edges this process wired.
    WiredByThisProcess,
    /// A topology provider declares the whole desired topology: every live
    /// edge between two identities is the topology's, including one wired
    /// before a restart (the mob's wiring is durable and replays, while the
    /// managed-edge set is this process's memory). A reconcile unwires every
    /// live identity edge that is no longer desired.
    TopologyProvider,
}

impl IdentityEdgeOwnership {
    pub(crate) fn for_topology_provider(present: bool) -> Self {
        if present {
            Self::TopologyProvider
        } else {
            Self::WiredByThisProcess
        }
    }
}

pub struct IdentityRuntime {
    entries: IdentityEntries,
    event_channels: RwLock<BTreeMap<AgentIdentity, broadcast::Sender<IdentityEvent>>>,
    continuity_store: Arc<dyn ContinuityStore>,
    lease_provider: Arc<dyn LeaseProvider>,
    runtime_instance_id: String,
    has_runtime_store: bool,
    durability_policy: DurabilityPolicy,
    bridge: Option<Arc<dyn SessionBridge>>,
    reset_roster_source: StdRwLock<Option<ResetRosterSource>>,
    runtime_services: AgentRuntimeServices,
    managed_peer_edges: RwLock<BTreeSet<(AgentIdentity, AgentIdentity)>>,
    /// Who owns the live edges between this runtime's identities; see
    /// [`IdentityEdgeOwnership`].
    identity_edge_ownership: StdRwLock<IdentityEdgeOwnership>,
    managed_peer_reconcile_lock: Mutex<()>,
    /// Managed edges whose wire or unwire call timed out: the reply was
    /// dropped, but the mob command may still be queued in the actor, and
    /// inspection (which reads roster and machine state directly) cannot see
    /// a queued command, so the edge's fate is unknown. A reconcile settles
    /// each one only with an ORDERED command that succeeded after it: an
    /// (idempotent) wire if still desired, a tolerant unwire if not. An edge
    /// stays here until such a command succeeded, or until an endpoint has no
    /// member left for the pending command to land on.
    unsettled_peer_wiring: RwLock<BTreeSet<(AgentIdentity, AgentIdentity)>>,
    /// Post-admission peer hydrations in flight, per target, with whether
    /// another admission asked for a rerun meanwhile. Concurrent admissions
    /// to one target merge into the running hydration instead of stacking.
    peer_hydrations_in_flight: StdMutex<PeerHydrations>,
    /// Post-admission hydration passes run (test observability only).
    #[cfg(test)]
    peer_hydration_passes: std::sync::atomic::AtomicUsize,
    /// A weak handle to the owning `Arc`, remembered by the first
    /// `Arc`-receiving entry point (tracked foreground work, the lease
    /// supervisor). Lets `&self` admission paths detach their post-admission
    /// peer hydration instead of running it inline before they return.
    self_handle: std::sync::OnceLock<std::sync::Weak<IdentityRuntime>>,
    desired_peer_edges: RwLock<Vec<ManagedPeerEdge>>,
    topology_controller: StdRwLock<Option<crate::topology_control::TopologyController>>,
    materialization_locks: RwLock<BTreeMap<AgentIdentity, Arc<Mutex<()>>>>,
    best_effort_materialization_locks: RwLock<BTreeMap<AgentIdentity, Arc<Mutex<()>>>>,
    lifecycle_locks: RwLock<BTreeMap<AgentIdentity, Arc<Mutex<()>>>>,
    raw_member_alias_locks: RwLock<RawMemberAliasLockTable>,
    customizer: RwLock<Option<Arc<dyn AgentCustomizer>>>,
    /// Stable per-identity dispatchers for `customize_build` tools (#563).
    /// Set whenever an agent customizer exists; the same registry backs the
    /// mob's `CustomizerToolsSpawnCustomizer`.
    customizer_tools: RwLock<Option<Arc<super::customizer_tools::CustomizerToolRegistry>>>,
    /// Identities whose pre-activation `customize_build` failed: restored
    /// without their customizer tools until materialization publishes.
    customizer_tools_pending: std::sync::RwLock<BTreeMap<AgentIdentity, CustomizerToolsPending>>,
    agent_memory: RwLock<Option<AgentMemoryRuntimeInjector>>,
    lease_renewal_notify: Notify,
    /// Exact grants whose restore task failed before an IdentityEntry existed.
    /// These must outlive the failed task so reconcile or shutdown can retry
    /// provider release instead of leaving invisible, non-expiring authority.
    pending_unactivated_lease_releases: RwLock<Vec<LeaseGrant>>,
    pending_unactivated_lease_release_gate: Mutex<()>,
    default_timeout: Duration,
    materialization_failure_backoff: RwLock<BTreeMap<AgentIdentity, MaterializationFailureBackoff>>,
    /// Host-declared `rkat` scope rendered into [`SessionRepairRequired`]
    /// commands; absent fields render as explicit placeholders.
    session_repair_scope: StdRwLock<Option<SessionRepairScope>>,
    error_hook: StdRwLock<Option<crate::unified_runtime::ErrorHook>>,
    /// The unified runtime's actor-loop probe verdict, installed late (the
    /// probe starts with the base runtime, identity-first attaches after).
    /// Forwarded to the session bridge so admission fails fast while a stall
    /// is open; read by `member_health`.
    actor_loop_health: StdRwLock<Option<Arc<ActorLoopHealth>>>,
    /// Told when a live member moves to a new session under the same
    /// incarnation (a respawn rebind or a delivery-repair rotation).
    session_rotation_observer: StdRwLock<Option<Arc<dyn MemberSessionRotationObserver>>>,
    bootstrap_status: watch::Sender<IdentityBootstrapStatus>,
    bootstrap_generation: StdMutex<u64>,
    bootstrap_controller: Mutex<()>,
    bootstrap_task: Mutex<Option<JoinHandle<()>>>,
    bootstrap_cancel: StdRwLock<Option<watch::Sender<bool>>>,
    bootstrap_shutdown: AtomicBool,
    foreground_operations: Mutex<JoinSet<()>>,
    foreground_cancel: watch::Sender<bool>,
    foreground_shutdown: AtomicBool,
    /// Post-commit cleanup for reset-superseded bridge generations. The debt
    /// is recorded before task spawn, survives task failure/timeout, and is
    /// retried synchronously before shutdown can attest cleanup or release
    /// identity fencing authority.
    pending_reset_bridge_cleanups: Arc<RwLock<BTreeMap<String, PendingResetBridgeCleanup>>>,
    reset_bridge_cleanup_tasks: Mutex<JoinSet<()>>,
    /// Per-identity completed-turn counters ([`CompletionCursor`]).
    ///
    /// RETAINED for the process lifetime — never pruned on retire, reset, or
    /// delete. Dropping an entry would let a re-registered identity republish
    /// a cursor it already published, which is the one thing a completion
    /// cursor must never do. The map is bounded by the identities this process
    /// has seen, i.e. by roster size.
    completion_cursors: StdMutex<BTreeMap<AgentIdentity, CompletionCursor>>,
    /// Outcomes of turns admitted under a [`TurnTicket`]
    /// ([`Self::send_with_turn_ticket`], [`Self::dispatch_with_turn_ticket`]).
    /// Shared with the waiter tasks that settle them.
    turn_outcomes: Arc<StdMutex<TurnOutcomes>>,
    /// Failed (or cancelled) runs observed per identity, so a completion
    /// wait can end typed instead of running to its deadline. Retained for
    /// the process lifetime, like the completion cursors.
    run_failures: StdMutex<BTreeMap<AgentIdentity, u64>>,
    /// Completions that happened but are not counted yet
    /// ([`Self::install_pending_completion_drain`]).
    pending_completion_drain: StdRwLock<Option<Arc<dyn PendingCompletionDrain>>>,
    /// Per-identity serialization of completion credit: an observed run
    /// terminal is claimed and credited under it, and a cursor read drains
    /// and reads under it, so a read never sees a terminal claimed but not
    /// yet credited.
    completion_credit_locks:
        StdMutex<BTreeMap<AgentIdentity, std::sync::Weak<tokio::sync::Mutex<()>>>>,
    /// Whether a completion drain was installed when the lease observer was
    /// (the first lease-woken attach may replay only with one).
    #[cfg(test)]
    lease_observer_saw_drain: std::sync::atomic::AtomicBool,
    /// Fired whenever a ticketed turn is admitted (which may evict another)
    /// or settles, so [`Self::wait_for_turn`] waits on the change instead of
    /// polling the registry. Carries no state.
    turn_outcome_changes: Arc<watch::Sender<()>>,
    /// Typed wake-ups for the continuity repair supervisor: bumped whenever
    /// something happened that can let a Broken identity's retry succeed
    /// ([`ContinuityRepairTrigger`]). The supervisor has no timer.
    continuity_repair_triggers: watch::Sender<ContinuityRepairWake>,
    /// Running continuity repair supervisors ([`ContinuityRepairSupervising`]).
    continuity_repair_supervisors: AtomicUsize,
    /// Embodiments (resumes and fresh mints) and embodiment bursts (restore
    /// passes, background warms) in flight, published so the repair
    /// supervisor waits for zero instead of contending with them.
    embodiments_in_flight: watch::Sender<usize>,
    /// Member inspections shared per runtime incarnation ([`Self::inspect`]).
    inspections: StdMutex<BTreeMap<AgentRuntimeId, SharedInspection>>,
}

/// How many ticketed turns are remembered at once.
const TRACKED_TURNS_CAP: usize = 4096;

/// How long a settled ticketed turn stays readable.
const SETTLED_TURN_RETENTION: Duration = Duration::from_mins(10);

/// Ticketed turns, bounded by [`TRACKED_TURNS_CAP`] and
/// [`SETTLED_TURN_RETENTION`].
///
/// Keyed by a ticket this registry mints once per admission, so two admissions
/// can never share a record: interaction and delivery correlation ids repeat
/// across admissions by design. A ticket read for any identity other than the
/// one it was admitted for is `Unknown`, and only the waiter that owns a
/// pending record settles it.
#[derive(Default)]
struct TurnOutcomes {
    turns: BTreeMap<TurnTicket, TrackedTurn>,
    /// Tracked dispatches by the scope meerkat deduplicates in (the member
    /// incarnation's session runtime ledger) plus the dispatch's idempotency
    /// key, so a re-dispatch of one key names the original admission's ticket.
    by_delivery: HashMap<DeliveryKey, TurnTicket>,
    admitted: u64,
}

/// A dispatch's dedup identity as meerkat sees it: the member incarnation and
/// the session whose runtime ledger admitted it, plus its idempotency key.
/// meerkat deduplicates only within one session's runtime ledger, and a
/// session rotation keeps the incarnation's `AgentRuntimeId`, so the session
/// is part of the key: after a rotation (or a reset, which mints a new
/// incarnation) a re-dispatch is tracked as an admission of its own.
type DeliveryKey = (AgentRuntimeId, SessionId, String);

struct TrackedTurn {
    identity: AgentIdentity,
    delivery_key: Option<DeliveryKey>,
    /// Admission order: evicts the oldest pending turn when the cap is full
    /// of pending turns.
    admitted: u64,
    state: TrackedTurnState,
}

enum TrackedTurnState {
    Pending,
    Settled {
        at: Instant,
        outcome: Result<TurnOutput, String>,
    },
}

impl TurnOutcomes {
    /// Record a newly admitted turn as pending under a freshly minted ticket.
    fn admit(&mut self, identity: &AgentIdentity, delivery_key: Option<DeliveryKey>) -> TurnTicket {
        self.prune();
        let ticket = loop {
            let ticket = TurnTicket::mint();
            if !self.turns.contains_key(&ticket) {
                break ticket;
            }
        };
        self.admitted += 1;
        if let Some(key) = &delivery_key {
            // A later admission under the same key (the earlier one failed)
            // takes over the index; the earlier record stays readable under
            // its own ticket.
            self.by_delivery.insert(key.clone(), ticket);
        }
        self.turns.insert(
            ticket,
            TrackedTurn {
                identity: identity.clone(),
                delivery_key,
                admitted: self.admitted,
                state: TrackedTurnState::Pending,
            },
        );
        ticket
    }

    /// The ticket of the tracked admission a re-dispatch under `key` is
    /// deduplicated onto, while that admission is still pending or completed.
    /// A settled failure is not reused: whether the runtime replays a failed
    /// key is its decision, so the retry is tracked as an admission of its own
    /// (and reads [`TurnOutput::NoOwnResult`] if it is deduplicated after all).
    fn reusable_delivery_ticket(
        &mut self,
        identity: &AgentIdentity,
        key: &DeliveryKey,
    ) -> Option<TurnTicket> {
        self.prune();
        let ticket = *self.by_delivery.get(key)?;
        let turn = self.turns.get(&ticket)?;
        let reusable = &turn.identity == identity
            && matches!(
                turn.state,
                TrackedTurnState::Pending | TrackedTurnState::Settled { outcome: Ok(_), .. }
            );
        reusable.then_some(ticket)
    }

    /// Settle the pending record `ticket` names for `identity`. Called once,
    /// by the waiter that owns the record; a record that is gone, already
    /// settled, or another identity's is left untouched.
    fn settle(
        &mut self,
        identity: &AgentIdentity,
        ticket: TurnTicket,
        outcome: Result<TurnOutput, String>,
    ) {
        if let Some(turn) = self.turns.get_mut(&ticket)
            && &turn.identity == identity
            && matches!(turn.state, TrackedTurnState::Pending)
        {
            turn.state = TrackedTurnState::Settled {
                at: Instant::now(),
                outcome,
            };
        }
    }

    fn outcome(&self, identity: &AgentIdentity, ticket: TurnTicket) -> TurnOutcome {
        match self.turns.get(&ticket) {
            Some(turn) if &turn.identity == identity => match &turn.state {
                TrackedTurnState::Pending => TurnOutcome::Pending,
                TrackedTurnState::Settled {
                    outcome: Ok(output),
                    ..
                } => TurnOutcome::Completed {
                    output: output.clone(),
                },
                TrackedTurnState::Settled {
                    outcome: Err(reason),
                    ..
                } => TurnOutcome::Failed {
                    reason: reason.clone(),
                },
            },
            _ => TurnOutcome::Unknown,
        }
    }

    fn remove(&mut self, ticket: TurnTicket) {
        if let Some(turn) = self.turns.remove(&ticket)
            && let Some(key) = turn.delivery_key
            && self.by_delivery.get(&key) == Some(&ticket)
        {
            self.by_delivery.remove(&key);
        }
    }

    /// Drop settled turns past their retention; then, while at the cap, the
    /// turn that settled LONGEST AGO (settle time, not admission order, so a
    /// long turn that just settled keeps its full retention). Only a cap full
    /// of pending turns evicts a pending one, the oldest admitted; its waiter
    /// then reads `Unknown`, never a wrong output.
    fn prune(&mut self) {
        let turns = &mut self.turns;
        turns.retain(|_, turn| match turn.state {
            TrackedTurnState::Pending => true,
            TrackedTurnState::Settled { at, .. } => at.elapsed() < SETTLED_TURN_RETENTION,
        });
        let turns = &self.turns;
        self.by_delivery
            .retain(|_, ticket| turns.contains_key(ticket));
        while self.turns.len() >= TRACKED_TURNS_CAP {
            let settled_longest_ago = self
                .turns
                .iter()
                .filter_map(|(ticket, turn)| match turn.state {
                    TrackedTurnState::Settled { at, .. } => Some((at, *ticket)),
                    TrackedTurnState::Pending => None,
                })
                .min_by_key(|(at, _)| *at)
                .map(|(_, ticket)| ticket);
            let evicted = settled_longest_ago.or_else(|| {
                self.turns
                    .iter()
                    .min_by_key(|(_, turn)| turn.admitted)
                    .map(|(ticket, _)| *ticket)
            });
            let Some(evicted) = evicted else {
                break;
            };
            if settled_longest_ago.is_none() {
                tracing::warn!(
                    ticket = %evicted,
                    cap = TRACKED_TURNS_CAP,
                    "ticketed turns at capacity with every one still pending; the oldest is \
                     forgotten and reads as unknown"
                );
            }
            self.remove(evicted);
        }
    }
}

/// One bridge inspection of a member incarnation in flight, shared by the
/// callers that join it (see [`IdentityRuntime::inspect`]). A settled read
/// is never reused.
#[derive(Clone)]
struct SharedInspection {
    /// The identity's completion cursor when this read started. The read
    /// reflects at least every turn counted there.
    started_at: CompletionCursor,
    read: Arc<tokio::sync::OnceCell<Result<super::bridge::MemberInspection, String>>>,
}

impl SharedInspection {
    fn start(started_at: CompletionCursor) -> Self {
        Self {
            started_at,
            read: Arc::default(),
        }
    }

    fn in_flight(&self) -> bool {
        self.read.get().is_none()
    }

    /// A caller that has seen completion cursor `seen` may join only a read
    /// still in flight that started at or after it. A read started before a
    /// turn the caller already counts could report the previous turn's
    /// output next to the new cursor.
    fn joinable_at(&self, seen: CompletionCursor) -> bool {
        self.in_flight() && self.started_at >= seen
    }
}

/// One generated member alias plus the lifecycle lock owned by its durable
/// identity runtime. Cross-runtime topology code resolves all endpoints first,
/// then acquires these targets in one global order before inspecting or
/// mutating either side.
pub(crate) struct MemberAliasLifecycleTarget {
    runtime: Arc<IdentityRuntime>,
    identity: AgentIdentity,
    alias: String,
    lock: Arc<Mutex<()>>,
}

impl MemberAliasLifecycleTarget {
    /// Canonical durable identity whose lifecycle lock owns this generated
    /// member alias. Surfaces may project this label, but the target itself
    /// remains the authority carried into the tracked operation.
    pub(crate) fn durable_identity(&self) -> &AgentIdentity {
        &self.identity
    }
}

struct MultiRuntimeForegroundCompletion(watch::Sender<bool>);

impl Drop for MultiRuntimeForegroundCompletion {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

/// Internal result of one dispatch attempt: the caller-facing admission plus
/// the concrete bridge session that accepted the work (scheduler delivery
/// needs the latter; RPC callers do not).
pub(crate) struct DispatchOutcome {
    admission: DispatchAdmission,
    pub(crate) session_id: Option<SessionId>,
    pub(crate) incarnation: CapturedIncarnation,
    /// `Some` exactly when the dispatch asked to track its turn.
    turn: Option<TurnTracking>,
}

/// Exact result of the one concrete embodiment door shared by eager restore,
/// lazy foreground materialization, and background warming.
pub(crate) struct EmbodimentOutcome {
    pub(crate) record: ContinuityRecord,
    pub(crate) resumed: bool,
    pub(crate) draft: AgentBuildDraft,
    /// The identity was already Active when the embodiment door ran (a
    /// converged re-validation, not a wake).
    pub(crate) already_active: bool,
}

/// Eager-only inputs to the shared embodiment transaction. Lazy foreground
/// and background callers use the registered entry's spec and customizer.
#[derive(Default)]
pub(crate) struct EmbodimentOverrides<'a> {
    pub(crate) spec: Option<&'a DurableAgentSpec>,
    pub(crate) customizer: Option<&'a dyn AgentCustomizer>,
}

#[derive(Clone)]
struct ResetRosterSource {
    provider: Arc<dyn RosterProvider>,
    mob_definition: Option<meerkat_mob::MobDefinition>,
}

#[derive(Debug, Clone)]
struct MaterializationFailureBackoff {
    suppress_until: Instant,
    error: String,
}

#[derive(Clone)]
struct PendingResetMemoryCapture {
    injector: AgentMemoryRuntimeInjector,
    identity: AgentIdentity,
    session_key: String,
    generation: u64,
}

impl PendingResetMemoryCapture {
    async fn run(&self) {
        // Repeat the synchronous marks in the owned task so a raw reset
        // future dropped after debt publication cannot let the distiller read
        // evidence before the reset quarantine boundary exists.
        self.injector.note_reset_boundary(&self.session_key);
        self.injector
            .note_session_generation(&self.identity, &self.session_key, self.generation);
        self.injector
            .distill_before_rotation(
                &self.identity,
                &self.session_key,
                crate::memory::distiller::DistillCause::Reset,
            )
            .await;
    }
}

#[derive(Clone)]
struct PendingResetBridgeCleanup {
    runtime_id: Option<AgentRuntimeId>,
    session_id: Option<SessionId>,
    memory_capture: Option<PendingResetMemoryCapture>,
}

impl PartialEq for PendingResetBridgeCleanup {
    fn eq(&self, other: &Self) -> bool {
        self.runtime_id == other.runtime_id && self.session_id == other.session_id
    }
}

impl Eq for PendingResetBridgeCleanup {}

impl PendingResetBridgeCleanup {
    fn key(&self) -> String {
        format!(
            "{}|{}",
            self.runtime_id
                .as_ref()
                .map(AgentRuntimeId::as_str)
                .unwrap_or("-"),
            self.session_id
                .as_ref()
                .map(std::string::ToString::to_string)
                .as_deref()
                .unwrap_or("-")
        )
    }
}

impl IdentityRuntime {
    /// Create a new identity runtime with the given configuration.
    pub fn new(config: IdentityRuntimeConfig) -> Self {
        let (bootstrap_status, _) = watch::channel(IdentityBootstrapStatus::empty(
            IdentityBootstrapMode::EagerMaterialize,
        ));
        let (foreground_cancel, _) = watch::channel(false);
        Self {
            entries: IdentityEntries::new(),
            event_channels: RwLock::new(BTreeMap::new()),
            continuity_store: config.continuity_store,
            lease_provider: config.lease_provider,
            runtime_instance_id: config.runtime_instance_id,
            has_runtime_store: config.has_runtime_store,
            durability_policy: config.durability_policy,
            bridge: config.bridge,
            reset_roster_source: StdRwLock::new(None),
            runtime_services: AgentRuntimeServices::empty(),
            managed_peer_edges: RwLock::new(BTreeSet::new()),
            identity_edge_ownership: StdRwLock::new(IdentityEdgeOwnership::WiredByThisProcess),
            managed_peer_reconcile_lock: Mutex::new(()),
            unsettled_peer_wiring: RwLock::new(BTreeSet::new()),
            peer_hydrations_in_flight: StdMutex::new(PeerHydrations::default()),
            #[cfg(test)]
            peer_hydration_passes: std::sync::atomic::AtomicUsize::new(0),
            self_handle: std::sync::OnceLock::new(),
            desired_peer_edges: RwLock::new(Vec::new()),
            topology_controller: StdRwLock::new(None),
            materialization_locks: RwLock::new(BTreeMap::new()),
            best_effort_materialization_locks: RwLock::new(BTreeMap::new()),
            lifecycle_locks: RwLock::new(BTreeMap::new()),
            raw_member_alias_locks: RwLock::new(RawMemberAliasLockTable::default()),
            customizer: RwLock::new(None),
            customizer_tools: RwLock::new(None),
            customizer_tools_pending: std::sync::RwLock::new(BTreeMap::new()),
            agent_memory: RwLock::new(None),
            lease_renewal_notify: Notify::new(),
            pending_unactivated_lease_releases: RwLock::new(Vec::new()),
            pending_unactivated_lease_release_gate: Mutex::new(()),
            default_timeout: config.default_timeout.unwrap_or(Duration::from_secs(90)),
            materialization_failure_backoff: RwLock::new(BTreeMap::new()),
            session_repair_scope: StdRwLock::new(None),
            error_hook: StdRwLock::new(None),
            actor_loop_health: StdRwLock::new(None),
            session_rotation_observer: StdRwLock::new(None),
            bootstrap_status,
            bootstrap_generation: StdMutex::new(0),
            bootstrap_controller: Mutex::new(()),
            bootstrap_task: Mutex::new(None),
            bootstrap_cancel: StdRwLock::new(None),
            bootstrap_shutdown: AtomicBool::new(false),
            foreground_operations: Mutex::new(JoinSet::new()),
            foreground_cancel,
            foreground_shutdown: AtomicBool::new(false),
            pending_reset_bridge_cleanups: Arc::new(RwLock::new(BTreeMap::new())),
            reset_bridge_cleanup_tasks: Mutex::new(JoinSet::new()),
            completion_cursors: StdMutex::new(BTreeMap::new()),
            turn_outcomes: Arc::default(),
            turn_outcome_changes: Arc::new(watch::channel(()).0),
            run_failures: StdMutex::new(BTreeMap::new()),
            pending_completion_drain: StdRwLock::new(None),
            completion_credit_locks: StdMutex::new(BTreeMap::new()),
            #[cfg(test)]
            lease_observer_saw_drain: std::sync::atomic::AtomicBool::new(false),
            continuity_repair_triggers: watch::channel(ContinuityRepairWake::default()).0,
            continuity_repair_supervisors: AtomicUsize::new(0),
            embodiments_in_flight: watch::channel(0).0,
            inspections: StdMutex::new(BTreeMap::new()),
        }
    }

    pub fn with_runtime_services(mut self, runtime_services: AgentRuntimeServices) -> Self {
        self.runtime_services = runtime_services;
        self
    }

    pub fn with_reset_roster_provider(self, provider: Arc<dyn RosterProvider>) -> Self {
        self.set_reset_roster_provider(Some(provider));
        self
    }

    pub fn with_reset_roster_provider_context(
        self,
        provider: Arc<dyn RosterProvider>,
        mob_definition: Option<meerkat_mob::MobDefinition>,
    ) -> Self {
        self.set_reset_roster_provider_context(Some(provider), mob_definition);
        self
    }

    pub(crate) fn runtime_services(&self) -> AgentRuntimeServices {
        self.runtime_services.clone()
    }

    /// Current typed bootstrap snapshot. Reading it never waits on an
    /// in-flight materialization.
    pub fn identity_bootstrap_status(&self) -> IdentityBootstrapStatus {
        self.identity_bootstrap_status_with_generation().1
    }

    pub(crate) fn identity_bootstrap_status_with_generation(
        &self,
    ) -> (u64, IdentityBootstrapStatus) {
        let generation = self
            .bootstrap_generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (*generation, self.bootstrap_status.borrow().clone())
    }

    pub(crate) fn subscribe_identity_bootstrap_status(
        &self,
    ) -> watch::Receiver<IdentityBootstrapStatus> {
        self.bootstrap_status.subscribe()
    }

    /// Change stream of the typed bootstrap snapshot, including each
    /// identity's eager-restore progress, so an in-process host can wait on
    /// typed per-member progress instead of a timeout.
    pub fn watch_identity_bootstrap_status(&self) -> watch::Receiver<IdentityBootstrapStatus> {
        self.bootstrap_status.subscribe()
    }

    /// Wait until every tracked identity has reached Active or Broken.
    /// Broken is terminal (and `ready == false`), so callers receive a useful
    /// failure snapshot instead of hanging forever.
    pub async fn wait_identity_bootstrap_terminal(
        &self,
        timeout: Duration,
    ) -> (IdentityBootstrapStatus, bool) {
        let (status, timed_out, _) = self
            .wait_identity_bootstrap_terminal_with_generation(timeout)
            .await;
        (status, timed_out)
    }

    pub(crate) async fn wait_identity_bootstrap_terminal_with_generation(
        &self,
        timeout: Duration,
    ) -> (IdentityBootstrapStatus, bool, u64) {
        let mut receiver = self.subscribe_identity_bootstrap_status();
        let wait = async {
            loop {
                let (generation, snapshot) = self.identity_bootstrap_status_with_generation();
                if snapshot.complete && snapshot.materialization_terminal() {
                    return (snapshot, generation);
                }
                if receiver.changed().await.is_err() {
                    let (generation, snapshot) = self.identity_bootstrap_status_with_generation();
                    return (snapshot, generation);
                }
            }
        };
        match tokio::time::timeout(timeout, wait).await {
            Ok((snapshot, generation)) => (snapshot, false, generation),
            Err(_) => {
                let (generation, snapshot) = self.identity_bootstrap_status_with_generation();
                (snapshot, true, generation)
            }
        }
    }

    fn request_identity_bootstrap_stop(&self) {
        if let Some(cancel) = self
            .bootstrap_cancel
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            let _ = cancel.send(true);
        }
    }

    async fn join_identity_bootstrap_task(&self) {
        if let Some(task) = self.bootstrap_task.lock().await.take() {
            let _ = task.await;
        }
        *self
            .bootstrap_cancel
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    /// Serialize status mutation with pass supersession. The generation lock
    /// closes the check-then-write race that an atomic epoch alone would leave
    /// between a retiring warm task and a newly-published reconcile barrier.
    fn modify_bootstrap_status<F>(&self, expected_generation: Option<u64>, modify: F) -> bool
    where
        F: FnOnce(&mut IdentityBootstrapStatus),
    {
        let current_generation = self
            .bootstrap_generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if expected_generation.is_some_and(|expected| expected != *current_generation) {
            return false;
        }
        self.bootstrap_status.send_modify(modify);
        true
    }

    fn replace_bootstrap_status(
        &self,
        expected_generation: u64,
        status: IdentityBootstrapStatus,
    ) -> bool {
        let current_generation = self
            .bootstrap_generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if expected_generation != *current_generation {
            return false;
        }
        self.bootstrap_status.send_replace(status);
        true
    }

    /// Close the controller, cooperatively cancel warm operations before
    /// bridge/member installation, and join the tracked task. An acquired but
    /// uninstalled lease is released explicitly; operations past that boundary
    /// reach their explicit commit/rollback path so no external lease or
    /// bridge session can be leaked by a raw task abort.
    pub(crate) async fn cancel_identity_bootstrap(&self) {
        self.bootstrap_shutdown.store(true, Ordering::Release);
        self.request_identity_bootstrap_stop();
        let _controller = self.bootstrap_controller.lock().await;
        self.request_identity_bootstrap_stop();
        self.join_identity_bootstrap_task().await;
        self.modify_bootstrap_status(None, |snapshot| {
            for entry in snapshot.identities.values_mut() {
                if entry.state == IdentityBootstrapState::Warming {
                    entry.state = IdentityBootstrapState::Dormant;
                }
            }
            snapshot.complete = true;
            snapshot.refresh_aggregates();
        });
    }

    async fn begin_identity_bootstrap(
        &self,
        generation: u64,
        mode: IdentityBootstrapMode,
        roster: &[DurableAgentSpec],
    ) {
        let entries = self.entries.read().await;
        let identities = roster
            .iter()
            .map(|spec| {
                let lifecycle = entries.get(&spec.identity).map(|entry| entry.state);
                let state = match lifecycle {
                    Some(IdentityLifecycleState::Active) => IdentityBootstrapState::Active,
                    _ if matches!(&mode, IdentityBootstrapMode::EagerMaterialize) => {
                        IdentityBootstrapState::Warming
                    }
                    _ => IdentityBootstrapState::Dormant,
                };
                (
                    spec.identity.clone(),
                    IdentityBootstrapEntry {
                        state,
                        error: None,
                        restore: None,
                    },
                )
            })
            .collect();
        drop(entries);
        let mut status = IdentityBootstrapStatus {
            mode,
            complete: false,
            ready: false,
            error: None,
            counts: Default::default(),
            identities,
        };
        status.refresh_aggregates();
        // A roster consisting only of already-active identities still has a
        // reconcile pass in flight. `complete` is the barrier guard even when
        // the aggregate states themselves happen to look ready.
        status.complete = false;
        status.ready = false;
        self.replace_bootstrap_status(generation, status);
    }

    fn begin_identity_bootstrap_pending(&self, mode: IdentityBootstrapMode) -> u64 {
        let mut generation = self
            .bootstrap_generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *generation = generation.wrapping_add(1);
        if *generation == 0 {
            *generation = 1;
        }
        let current_generation = *generation;
        self.bootstrap_status.send_modify(|snapshot| {
            snapshot.mode = mode;
            snapshot.complete = false;
            snapshot.ready = false;
            snapshot.error = None;
            // Per-identity causes are pass-scoped. `begin_identity_bootstrap`
            // already rebuilds every entry with `error: None`, but it runs only
            // AFTER the prepare/provider await, and `fail_identity_bootstrap`
            // is reachable before that (prepare failure in `bootstrap_roster`,
            // roster-provider failure in `refresh_desired_topology`). Without
            // this reset, its `entry.error.is_none()` guard cannot tell "this
            // identity produced its own cause during THIS pass" from "left
            // over from the last pass", and an early failure would report a
            // previous pass's cause beside the current pass's `error`.
            // Only causes are cleared: `refresh_aggregates` reads `entry.state`
            // exclusively, so no count, `ready`, or `materialization_terminal`
            // value moves here.
            for entry in snapshot.identities.values_mut() {
                entry.error = None;
            }
        });
        current_generation
    }

    /// Record a PASS-level bootstrap failure.
    ///
    /// Terminality is preserved deliberately: the barrier rule is
    /// `complete && dormant == 0 && warming == 0`
    /// ([`IdentityBootstrapStatus::materialization_terminal`]), and an eager
    /// pass seeds the whole roster `Warming`, so leaving those entries alone
    /// would hang `wait_identity_bootstrap_terminal` until its timeout
    /// instead of returning a truthful failure snapshot.
    ///
    /// What must NOT happen is attributing the pass cause to each identity as
    /// if it were that identity's own. Before embodiment became per identity,
    /// one member's `bridge create_session:` failure was copied onto 16 peers
    /// (downstream app activation-33 shape). Member failures now bypass this method,
    /// while a genuine pass failure stamps only what is known: the pass died
    /// before this identity's own outcome was recorded. A cause the identity actually produced
    /// during the pass (`mark_bootstrap_from_lifecycle`,
    /// `mark_bootstrap_materialization_finished`) is authoritative and is
    /// never overwritten. That equivalence between "the entry already has a
    /// cause" and "this pass produced it" is not free: it holds because
    /// `begin_identity_bootstrap_pending` resets per-entry causes when the
    /// pass opens, which is the only reason this `is_none()` guard cannot
    /// resurrect a previous pass's cause. Member embodiment failures no longer
    /// enter this path; they park only their own identity. The pass cause still rides
    /// `snapshot.error`, which the type documents as the pass-level slot and
    /// which every reader (RPC status, `wait_identity_bootstrap`, the Python
    /// SDK model) already parses.
    fn fail_identity_bootstrap(&self, generation: u64, error: &IdentityRuntimeError) {
        self.modify_bootstrap_status(Some(generation), |snapshot| {
            let detail = error.to_string();
            let unattributed = format!(
                "identity bootstrap pass failed before this identity reached its own \
                 outcome: {detail}"
            );
            snapshot.complete = true;
            snapshot.error = Some(detail);
            for entry in snapshot.identities.values_mut() {
                if entry.state != IdentityBootstrapState::Active {
                    entry.state = IdentityBootstrapState::Broken;
                    if entry.error.is_none() {
                        entry.error = Some(unattributed.clone());
                    }
                }
            }
            snapshot.refresh_aggregates();
        });
    }

    #[cfg(test)]
    pub(crate) fn test_supersede_identity_bootstrap_ready(&self) {
        let generation =
            self.begin_identity_bootstrap_pending(IdentityBootstrapMode::EagerMaterialize);
        self.modify_bootstrap_status(Some(generation), |snapshot| {
            snapshot.identities.clear();
            snapshot.complete = true;
            snapshot.error = None;
            snapshot.refresh_aggregates();
        });
    }

    #[cfg(test)]
    pub(crate) fn test_fail_identity_bootstrap(&self, detail: &str) {
        let generation =
            self.begin_identity_bootstrap_pending(IdentityBootstrapMode::EagerMaterialize);
        self.modify_bootstrap_status(Some(generation), |snapshot| {
            snapshot.identities.clear();
            if let Ok(identity) = AgentIdentity::parse("agent:test-bootstrap-failure") {
                snapshot.identities.insert(
                    identity,
                    IdentityBootstrapEntry {
                        state: IdentityBootstrapState::Dormant,
                        error: None,
                        restore: None,
                    },
                );
            }
            snapshot.refresh_aggregates();
        });
        self.fail_identity_bootstrap(
            generation,
            &IdentityRuntimeError::Internal(detail.to_string()),
        );
    }

    async fn install_identity_bootstrap(
        self: &Arc<Self>,
        generation: u64,
        mode: IdentityBootstrapMode,
        roster: &[DurableAgentSpec],
        result: &super::orchestrator::RestoreFlowResult,
    ) {
        let background_concurrency = match mode {
            IdentityBootstrapMode::LazyWithBackgroundWarm { concurrency } => Some(concurrency),
            _ => None,
        };
        let mut status = IdentityBootstrapStatus {
            mode: mode.clone(),
            complete: background_concurrency.is_none(),
            ready: false,
            error: None,
            counts: Default::default(),
            identities: BTreeMap::new(),
        };
        for spec in roster {
            let broken_outcome = result.outcomes.get(&spec.identity).and_then(|outcome| {
                if let super::orchestrator::RestoreOutcome::Broken(failure) = outcome {
                    Some(failure.detail.clone())
                } else {
                    None
                }
            });
            let lifecycle = self
                .status(&spec.identity)
                .await
                .ok()
                .map(|item| item.state);
            let (state, error) = match (broken_outcome, lifecycle) {
                // A store-projected Broken result is itself authoritative.
                // restore_flow intentionally does not fabricate a lifecycle
                // entry for it, so consulting lifecycle alone would default
                // to Dormant and make a terminal wait barrier time out.
                (Some(detail), _) => (IdentityBootstrapState::Broken, Some(detail)),
                (None, Some(IdentityLifecycleState::Active)) => {
                    (IdentityBootstrapState::Active, None)
                }
                (None, Some(IdentityLifecycleState::Broken)) => {
                    (IdentityBootstrapState::Broken, None)
                }
                (None, _) => (IdentityBootstrapState::Dormant, None),
            };
            // The eager pass's typed per-member progress survives into the
            // settled snapshot.
            let restore = match mode {
                IdentityBootstrapMode::EagerMaterialize => result
                    .outcomes
                    .get(&spec.identity)
                    .map(super::orchestrator::RestoreOutcome::restore_progress),
                _ => None,
            };
            status.identities.insert(
                spec.identity.clone(),
                IdentityBootstrapEntry {
                    state,
                    error,
                    restore,
                },
            );
        }
        status.refresh_aggregates();
        if !self.replace_bootstrap_status(generation, status.clone()) {
            return;
        }

        let Some(concurrency) = background_concurrency else {
            return;
        };
        let identities = status
            .identities
            .iter()
            .filter(|(_, entry)| entry.state == IdentityBootstrapState::Dormant)
            .map(|(identity, _)| identity.clone())
            .collect::<Vec<_>>();
        if identities.is_empty() {
            self.modify_bootstrap_status(Some(generation), |snapshot| {
                snapshot.complete = true;
                snapshot.refresh_aggregates();
            });
            return;
        }

        let runtime = Arc::clone(self);
        let (cancel, task_cancel) = watch::channel(false);
        let task = tokio::spawn(async move {
            // The warm is one embodiment burst: the repair supervisor defers
            // until all of it settled and then runs one pass.
            let _burst = runtime.enter_embodiment_burst();
            stream::iter(identities.into_iter().map(|identity| {
                let runtime = Arc::clone(&runtime);
                let mut cancel = task_cancel.clone();
                async move {
                    if *cancel.borrow() {
                        return;
                    }
                    let Some(result) = runtime
                        .materialize_for_background(&identity, &mut cancel, generation)
                        .await
                    else {
                        return;
                    };
                    if let Err(error) = result {
                        tracing::warn!(
                            %identity,
                            error = %error,
                            "identity background warm failed"
                        );
                        runtime
                            .record_best_effort_materialization_failure(
                                &identity,
                                None,
                                "background_warm",
                                &error,
                            )
                            .await;
                    }
                }
            }))
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
            if !*task_cancel.borrow() {
                runtime.modify_bootstrap_status(Some(generation), |snapshot| {
                    snapshot.complete = true;
                    snapshot.refresh_aggregates();
                });
            }
        });
        *self
            .bootstrap_cancel
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cancel);
        *self.bootstrap_task.lock().await = Some(task);
    }

    fn mark_bootstrap_materialization_started(
        &self,
        identity: &AgentIdentity,
        generation: Option<u64>,
    ) {
        self.modify_bootstrap_status(generation, |snapshot| {
            let Some(entry) = snapshot.identities.get_mut(identity) else {
                return;
            };
            entry.state = IdentityBootstrapState::Warming;
            entry.error = None;
            snapshot.refresh_aggregates();
        });
    }

    fn mark_bootstrap_from_lifecycle(
        &self,
        identity: &AgentIdentity,
        lifecycle: IdentityLifecycleState,
        error: Option<String>,
    ) {
        self.modify_bootstrap_status(None, |snapshot| {
            let Some(entry) = snapshot.identities.get_mut(identity) else {
                return;
            };
            entry.state = match lifecycle {
                IdentityLifecycleState::Active => IdentityBootstrapState::Active,
                IdentityLifecycleState::Broken => IdentityBootstrapState::Broken,
                IdentityLifecycleState::Dormant
                | IdentityLifecycleState::Retiring
                | IdentityLifecycleState::Suspended
                | IdentityLifecycleState::Uninitialized => IdentityBootstrapState::Dormant,
            };
            entry.error = if entry.state == IdentityBootstrapState::Broken {
                error
            } else {
                None
            };
            snapshot.refresh_aggregates();
        });
    }

    fn mark_bootstrap_materialization_finished(
        &self,
        identity: &AgentIdentity,
        result: &Result<ContinuityRecord, IdentityRuntimeError>,
        generation: Option<u64>,
    ) {
        self.modify_bootstrap_status(generation, |snapshot| {
            let Some(entry) = snapshot.identities.get_mut(identity) else {
                return;
            };
            match result {
                Ok(_) => {
                    entry.state = IdentityBootstrapState::Active;
                    entry.error = None;
                }
                Err(error) => {
                    entry.state = IdentityBootstrapState::Broken;
                    entry.error = Some(error.to_string());
                }
            }
            snapshot.refresh_aggregates();
        });
    }

    fn mark_bootstrap_materialization_cancelled(
        &self,
        identity: &AgentIdentity,
        generation: Option<u64>,
    ) {
        self.modify_bootstrap_status(generation, |snapshot| {
            if let Some(entry) = snapshot.identities.get_mut(identity) {
                entry.state = IdentityBootstrapState::Dormant;
                entry.error = None;
                snapshot.refresh_aggregates();
            }
        });
    }

    /// Publish one identity's typed eager-restore progress on the bootstrap
    /// status as the restore pass reaches it. A settled stage also settles
    /// the identity's bootstrap state, so per-member barriers can pass before
    /// the whole pass ends.
    pub(crate) fn publish_restore_progress(
        &self,
        identity: &AgentIdentity,
        progress: IdentityRestoreProgress,
        error: Option<String>,
    ) {
        self.modify_bootstrap_status(None, |snapshot| {
            let Some(entry) = snapshot.identities.get_mut(identity) else {
                return;
            };
            match &progress {
                IdentityRestoreProgress::Minted | IdentityRestoreProgress::Resumed => {
                    entry.state = IdentityBootstrapState::Active;
                    entry.error = None;
                }
                IdentityRestoreProgress::Broken { .. } => {
                    entry.state = IdentityBootstrapState::Broken;
                    entry.error = error;
                }
                IdentityRestoreProgress::Registered | IdentityRestoreProgress::Resuming => {}
            }
            entry.restore = Some(progress);
            snapshot.refresh_aggregates();
        });
    }

    /// Exact concrete member ids represented by the tracked bootstrap roster.
    /// Used only after `ready == true`, preventing a false-ready snapshot of a
    /// partially warmed mob.
    pub async fn identity_bootstrap_member_ids(&self) -> Vec<meerkat_mob::ids::AgentIdentity> {
        let tracked = self.identity_bootstrap_status();
        self.identity_bootstrap_member_ids_for_status(&tracked)
            .await
    }

    pub(crate) async fn identity_bootstrap_member_ids_for_status(
        &self,
        tracked: &IdentityBootstrapStatus,
    ) -> Vec<meerkat_mob::ids::AgentIdentity> {
        let entries = self.entries.read().await;
        tracked
            .identities
            .keys()
            .filter_map(|identity| {
                // The roster is keyed by the encoded DURABLE identity for every
                // binding. This used to mirror the old spawn-side lowering
                // (identity for external, runtime id for local); keeping that
                // split would report member ids no roster row answers to, and
                // the bootstrap status would look healthy while naming nothing.
                let entry = entries.get(identity)?;
                // A continuity row still gates inclusion: an identity with no
                // binding is not a bootstrapped member.
                entry.continuity.as_ref()?;
                Some(crate::member_comms_id::roster_member_id_for_identity(
                    identity.as_str(),
                ))
            })
            .collect()
    }

    pub async fn set_agent_customizer(&self, customizer: Option<Arc<dyn AgentCustomizer>>) {
        *self.customizer.write().await = customizer;
    }

    /// Install the registry whose per-identity dispatchers carry
    /// `customize_build` tools across meerkat-side rebuilds (#563). The same
    /// registry must back the mob's `CustomizerToolsSpawnCustomizer`.
    pub async fn set_customizer_tool_registry(
        &self,
        registry: Option<Arc<super::customizer_tools::CustomizerToolRegistry>>,
    ) {
        *self.customizer_tools.write().await = registry;
    }

    /// Publish one successful `customize_build` result for `identity` and
    /// point the draft at the identity's stable dispatcher, so the spawn (or
    /// the adopted occupant, which already holds it) serves the new tools.
    async fn publish_customizer_tools(
        &self,
        identity: &AgentIdentity,
        draft: &mut AgentBuildDraft,
    ) {
        let Some(registry) = self.customizer_tools.read().await.clone() else {
            return;
        };
        let entry = registry.publish(identity, draft.local_external_tools.dispatcher());
        draft.local_external_tools = super::types::LocalExternalToolOverlay::new(entry);
        let was_pending = self
            .customizer_tools_pending
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(identity)
            .is_some();
        // The mob build held this restored member's run starts while its
        // tools were unpublished; they are published now, so release it.
        if was_pending && let Some(bridge) = self.bridge.as_ref() {
            let member_id = crate::member_comms_id::mob_member_id(identity.as_str());
            if let Err(error) = bridge
                .release_member_run_starts_for_published_tools(&member_id)
                .await
            {
                let reason = format!("customizer tools published, but {error}");
                tracing::error!(
                    %identity,
                    %reason,
                    "restored member stays held: it starts no run until the hold is released"
                );
                self.customizer_tools_pending
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(identity.clone(), CustomizerToolsPending { reason });
            }
        }
    }

    fn customizer_tools_pending_for(
        &self,
        identity: &AgentIdentity,
    ) -> Option<CustomizerToolsPending> {
        self.customizer_tools_pending
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(identity)
            .cloned()
    }

    /// Publish each roster identity's customizer tools BEFORE the mob is
    /// lifted (#563). meerkat's restore at activation then builds every
    /// restored member with its tools already present: its tool scope is
    /// right from the first build, and a run that starts at activation (a
    /// queued input, a kickoff, a peer message) sees them. The materialization
    /// that follows runs `customize_build` again and republishes through the
    /// same atomic swap. A failure here is logged per identity and recorded as
    /// `IdentityStatus::customizer_tools_pending` until the materialization
    /// publishes.
    pub async fn prepublish_customizer_tools(
        &self,
        roster: &[DurableAgentSpec],
        customizer: Option<&dyn AgentCustomizer>,
    ) {
        let Some(registry) = self.customizer_tools.read().await.clone() else {
            return;
        };
        let installed = self.customizer.read().await.clone();
        let Some(customizer) = customizer.or(installed.as_deref()) else {
            return;
        };
        let active_peers: Vec<AgentIdentity> = self.entries.read().await.keys().cloned().collect();
        let managed_edges = self.desired_peer_edges.read().await.clone();
        let failed = super::customizer_tools::prepublish(
            &registry,
            roster,
            customizer,
            self.runtime_services(),
            &active_peers,
            &managed_edges,
        )
        .await;
        let mut pending = self
            .customizer_tools_pending
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for spec in roster {
            pending.remove(&spec.identity);
        }
        pending.extend(failed);
    }

    /// Record identities whose customizer tools a pre-build publication could
    /// not publish (see [`super::customizer_tools::prepublish`]).
    pub fn record_customizer_tools_pending(
        &self,
        failed: BTreeMap<AgentIdentity, CustomizerToolsPending>,
    ) {
        self.customizer_tools_pending
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(failed);
    }

    pub async fn set_agent_memory(&self, injector: Option<AgentMemoryRuntimeInjector>) {
        *self.agent_memory.write().await = injector;
    }

    pub async fn agent_memory_supports_recall(&self) -> bool {
        self.agent_memory.read().await.is_some()
    }

    pub async fn agent_memory_supports_remember(&self) -> bool {
        self.agent_memory
            .read()
            .await
            .as_ref()
            .is_some_and(|injector| injector.provider().supports_remember())
    }

    pub async fn agent_memory_supports_forget(&self) -> bool {
        self.agent_memory
            .read()
            .await
            .as_ref()
            .is_some_and(|injector| injector.provider().supports_forget())
    }

    pub async fn agent_memory_supports_update(&self) -> bool {
        self.agent_memory
            .read()
            .await
            .as_ref()
            .is_some_and(|injector| injector.provider().supports_supersede())
    }

    pub async fn agent_memory_supports_manifest(&self) -> bool {
        self.agent_memory
            .read()
            .await
            .as_ref()
            .is_some_and(|injector| injector.provider().supports_manifest())
    }

    pub async fn remember_agent_memory(
        &self,
        realm: &str,
        identity: &AgentIdentity,
        memory: NewAgentMemory,
    ) -> Result<AgentMemoryRecord, AgentMemoryError> {
        self.status(identity)
            .await
            .map_err(|err| AgentMemoryError::InvalidConfig(err.to_string()))?;
        let provider = self
            .agent_memory
            .read()
            .await
            .as_ref()
            .map(AgentMemoryRuntimeInjector::provider)
            .ok_or_else(|| {
                AgentMemoryError::InvalidConfig("agent memory is not configured".to_string())
            })?;
        provider.remember(realm, identity, memory).await
    }

    pub async fn forget_agent_memory(
        &self,
        realm: &str,
        identity: &AgentIdentity,
        memory_id: &str,
    ) -> Result<AgentMemoryForgetResult, AgentMemoryError> {
        self.status(identity)
            .await
            .map_err(|err| AgentMemoryError::InvalidConfig(err.to_string()))?;
        let provider = self
            .agent_memory
            .read()
            .await
            .as_ref()
            .map(AgentMemoryRuntimeInjector::provider)
            .ok_or_else(|| {
                AgentMemoryError::InvalidConfig("agent memory is not configured".to_string())
            })?;
        provider.forget(realm, identity, memory_id).await
    }

    /// Supersede `memory_id` within its lineage (the D4 fix): the new
    /// title/body/tags become the active record; the prior stays
    /// retrievable with provenance.
    pub async fn update_agent_memory(
        &self,
        realm: &str,
        identity: &AgentIdentity,
        memory_id: &str,
        memory: NewAgentMemory,
    ) -> Result<MemoryId, AgentMemoryError> {
        self.status(identity)
            .await
            .map_err(|err| AgentMemoryError::InvalidConfig(err.to_string()))?;
        let provider = self
            .agent_memory
            .read()
            .await
            .as_ref()
            .map(AgentMemoryRuntimeInjector::provider)
            .ok_or_else(|| {
                AgentMemoryError::InvalidConfig("agent memory is not configured".to_string())
            })?;
        let scope = MemoryScope::Identity {
            realm: realm.to_string(),
            identity: identity.as_str().to_string(),
        };
        let record = NewMemoryRecord {
            kind: MemoryKind::Fact,
            title: memory.title,
            description: String::new(),
            body: memory.body,
            tags: memory.tags,
            evidence: Vec::new(),
            verification: None,
        };
        provider.supersede(&scope, memory_id, record).await
    }

    /// Tiered metadata manifest for the identity's own scope (§8.3).
    pub async fn manifest_agent_memory(
        &self,
        realm: &str,
        identity: &AgentIdentity,
        tier: ManifestTier,
    ) -> Result<Vec<RecordMeta>, AgentMemoryError> {
        self.status(identity)
            .await
            .map_err(|err| AgentMemoryError::InvalidConfig(err.to_string()))?;
        let provider = self
            .agent_memory
            .read()
            .await
            .as_ref()
            .map(AgentMemoryRuntimeInjector::provider)
            .ok_or_else(|| {
                AgentMemoryError::InvalidConfig("agent memory is not configured".to_string())
            })?;
        let scope = MemoryScope::Identity {
            realm: realm.to_string(),
            identity: identity.as_str().to_string(),
        };
        provider.manifest(&[scope], tier).await
    }

    pub async fn recall_agent_memory(
        &self,
        request: AgentMemoryRecallRequest,
    ) -> Result<Vec<AgentMemoryRecord>, AgentMemoryError> {
        self.status(&request.identity)
            .await
            .map_err(|err| AgentMemoryError::InvalidConfig(err.to_string()))?;
        let provider = self
            .agent_memory
            .read()
            .await
            .as_ref()
            .map(AgentMemoryRuntimeInjector::provider)
            .ok_or_else(|| {
                AgentMemoryError::InvalidConfig("agent memory is not configured".to_string())
            })?;
        let records = provider.recall(request).await?;
        // §9.2: explicit recall reads mark usage mechanically. Telemetry
        // never fails the read — providers without usage support
        // (markdown) return Unsupported, which is downgraded here.
        if !records.is_empty() {
            let ids: Vec<MemoryId> = records
                .iter()
                .map(|record| record.memory_id.clone())
                .collect();
            if let Err(err) = provider.mark_usage(&ids, UsageEvent::ExplicitRecall).await {
                tracing::debug!(error = %err, "agent memory explicit-recall usage marking skipped");
            }
        }
        Ok(records)
    }

    /// Attach the roster provider reset should consult for current specs.
    pub fn set_reset_roster_provider(&self, provider: Option<Arc<dyn RosterProvider>>) {
        self.set_reset_roster_provider_context(provider, None);
    }

    /// Attach the roster provider and context reset should consult for current specs.
    pub fn set_reset_roster_provider_context(
        &self,
        provider: Option<Arc<dyn RosterProvider>>,
        mob_definition: Option<meerkat_mob::MobDefinition>,
    ) {
        let source = provider.map(|provider| ResetRosterSource {
            provider,
            mob_definition,
        });
        match self.reset_roster_source.write() {
            Ok(mut stored_source) => *stored_source = source,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "identity runtime reset roster source lock poisoned; dropping provider update"
                );
            }
        }
    }

    async fn adopt_current_roster_spec_for_reset(&self, identity: &AgentIdentity) {
        let source = match self.reset_roster_source.read() {
            Ok(stored_source) => stored_source.clone(),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "reset: roster source lock poisoned; rebuilding on stored spec"
                );
                None
            }
        };
        if let Some(source) = source {
            self.adopt_roster_spec_with_context(
                &source.provider,
                identity,
                source.mob_definition.clone(),
            )
            .await;
        }
    }

    /// Attach a best-effort operational error hook used for alerting.
    /// Declare where the operator's `rkat --state-root .. --realm ..` scope
    /// for [`SessionRepairRequired`] commands points. Hosts that embed MobKit
    /// in a meerkat realm know both; a field left `None` renders as an
    /// explicit placeholder rather than a guessed path.
    pub fn set_session_repair_scope(&self, scope: Option<SessionRepairScope>) {
        *self
            .session_repair_scope
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = scope;
    }

    pub fn session_repair_scope(&self) -> Option<SessionRepairScope> {
        self.session_repair_scope
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn set_error_hook(&self, hook: Option<crate::unified_runtime::ErrorHook>) {
        match self.error_hook.write() {
            Ok(mut stored_hook) => *stored_hook = hook,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "identity runtime error hook lock poisoned; dropping hook update"
                );
            }
        }
    }

    /// Spawn a background supervisor that renews active identity leases before
    /// they reach their TTL deadline.
    pub fn spawn_lease_renewal_task(self: Arc<Self>) -> JoinHandle<()> {
        self.remember_self_handle();
        self.spawn_lease_renewal_task_with_poll_interval(DEFAULT_LEASE_RENEWAL_MAX_POLL_INTERVAL)
    }

    /// Spawn the runtime-owned renewal supervisor. Unlike the public
    /// fire-and-forget helper, this handle cooperatively cancels while idle
    /// and joins any in-flight renewal through provider, continuity-store,
    /// bridge, and local-publication commit boundaries.
    pub(crate) fn spawn_tracked_lease_renewal_task(self: Arc<Self>) -> TrackedLeaseRenewalTask {
        self.spawn_tracked_lease_renewal_task_with_poll_interval(
            DEFAULT_LEASE_RENEWAL_MAX_POLL_INTERVAL,
        )
    }

    pub(crate) fn spawn_tracked_lease_renewal_task_with_poll_interval(
        self: Arc<Self>,
        max_poll_interval: Duration,
    ) -> TrackedLeaseRenewalTask {
        self.remember_self_handle();
        let (cancel, receiver) = watch::channel(false);
        let join = tokio::spawn(self.run_lease_renewal_loop(max_poll_interval, Some(receiver)));
        TrackedLeaseRenewalTask { cancel, join }
    }

    /// Spawn a lease renewal supervisor with a caller-provided maximum poll
    /// interval. Embedders can use this for shorter external lease TTLs; tests
    /// use it to exercise renewal without waiting on wall-clock TTLs.
    pub fn spawn_lease_renewal_task_with_poll_interval(
        self: Arc<Self>,
        max_poll_interval: Duration,
    ) -> JoinHandle<()> {
        self.remember_self_handle();
        tokio::spawn(self.run_lease_renewal_loop(max_poll_interval, None))
    }

    /// Remember the owning `Arc` (weakly) for detached post-admission work.
    fn remember_self_handle(self: &Arc<Self>) {
        let _ = self.self_handle.set(Arc::downgrade(self));
    }

    /// The owning `Arc`, when an `Arc`-receiving entry point has run.
    fn shared_handle(&self) -> Option<Arc<Self>> {
        self.self_handle.get().and_then(std::sync::Weak::upgrade)
    }

    async fn run_lease_renewal_loop(
        self: Arc<Self>,
        max_poll_interval: Duration,
        mut cancellation: Option<watch::Receiver<bool>>,
    ) {
        let max_poll_interval = max_poll_interval.max(DEFAULT_LEASE_RENEWAL_MIN_POLL_INTERVAL);
        let mut consecutive_failures: u32 = 0;
        loop {
            if cancellation
                .as_ref()
                .is_some_and(|cancellation| *cancellation.borrow())
            {
                return;
            }
            let base = self.lease_renewal_sleep_interval(max_poll_interval).await;
            // While the provider is failing, hold off at least the backoff
            // delay so a persistent outage retries at a bounded rate instead
            // of the TTL-derived floor (down to 10ms).
            let sleep = if consecutive_failures > 0 {
                base.max(lease_renewal_failure_backoff(
                    consecutive_failures,
                    max_poll_interval,
                ))
            } else {
                base
            };
            if let Some(cancellation) = cancellation.as_mut() {
                tokio::select! {
                    () = tokio::time::sleep(sleep) => {}
                    () = self.lease_renewal_notify.notified() => {}
                    changed = cancellation.changed() => {
                        match changed {
                            Ok(()) if *cancellation.borrow() => return,
                            Ok(()) => continue,
                            Err(_) => return,
                        }
                    }
                }
            } else {
                tokio::select! {
                    () = tokio::time::sleep(sleep) => {}
                    () = self.lease_renewal_notify.notified() => {}
                }
            }

            // Do not select cancellation against this future. Once renewal
            // has entered the provider, the returned token may already be
            // authoritative; the runtime must publish that exact grant before
            // shutdown performs its final release.
            match self.renew_due_leases_once().await {
                Ok(_) => consecutive_failures = 0,
                Err(err) => {
                    // Warn once, then debounce to debug so a backend outage
                    // can't flood the log at the renewal cadence.
                    if consecutive_failures == 0 {
                        tracing::warn!(
                            error = %err,
                            "identity-first proactive lease renewal tick failed; backing off"
                        );
                    } else {
                        tracing::debug!(
                            error = %err,
                            consecutive_failures,
                            "identity-first lease renewal still failing; backing off"
                        );
                    }
                    consecutive_failures = consecutive_failures.saturating_add(1);
                }
            }
            if cancellation
                .as_ref()
                .is_some_and(|cancellation| *cancellation.borrow())
            {
                return;
            }
        }
    }

    async fn lease_renewal_sleep_interval(&self, max_poll_interval: Duration) -> Duration {
        let entries = self.entries.read().await;
        entries
            .values()
            .filter(|entry| entry.state == IdentityLifecycleState::Active)
            .filter_map(|entry| entry.lease.as_ref())
            .map(|lease| (lease.ttl / 10).max(DEFAULT_LEASE_RENEWAL_MIN_POLL_INTERVAL))
            .min()
            .unwrap_or(max_poll_interval)
            .min(max_poll_interval)
    }

    /// Renew every active lease that has entered the runtime's renewal window.
    pub async fn renew_due_leases_once(&self) -> Result<usize, IdentityRuntimeError> {
        let due = {
            let entries = self.entries.read().await;
            entries
                .iter()
                .filter(|(_, entry)| entry.state == IdentityLifecycleState::Active)
                .filter_map(|(identity, entry)| {
                    entry
                        .lease
                        .as_ref()
                        .filter(|lease| !lease.is_healthy())
                        .map(|_| identity.clone())
                })
                .collect::<Vec<_>>()
        };

        let mut renewed = 0;
        let mut first_error = None;
        for identity in due {
            let lifecycle_lock = self.lifecycle_lock_for(&identity).await;
            let _lifecycle_guard = lifecycle_lock.lock().await;
            match self.ensure_active_lease(&identity).await {
                Ok(_) => renewed += 1,
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }
        if let Some(err) = first_error {
            Err(err)
        } else {
            Ok(renewed)
        }
    }

    async fn release_uninstalled_materialize_lease(&self, grant: &LeaseGrant) -> Option<String> {
        self.release_or_park_untracked_leases(std::slice::from_ref(grant))
            .await
            .err()
            .map(|err| err.to_string())
    }

    async fn cancel_uninstalled_background_materialization(
        &self,
        grant: &LeaseGrant,
    ) -> IdentityRuntimeError {
        let cleanup_error = self.release_uninstalled_materialize_lease(grant).await;
        IdentityRuntimeError::Internal(
            cleanup_error
                .map(|error| format!("{BACKGROUND_WARM_CANCELLED}; lease cleanup failed: {error}"))
                .unwrap_or_else(|| BACKGROUND_WARM_CANCELLED.to_string()),
        )
    }

    pub async fn set_desired_peer_edges(&self, edges: Vec<ManagedPeerEdge>) {
        *self.desired_peer_edges.write().await = edges;
    }

    pub(crate) fn set_topology_controller(
        &self,
        controller: crate::topology_control::TopologyController,
    ) {
        *self
            .topology_controller
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(controller);
    }

    fn topology_controller(&self) -> Option<crate::topology_control::TopologyController> {
        self.topology_controller
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub async fn desired_peer_edges(&self) -> Vec<ManagedPeerEdge> {
        let declared = self.desired_peer_edges.read().await.clone();
        match self.topology_controller() {
            Some(controller) => {
                // Materialization/console reads must not observe the target
                // intent while a topology transaction is between WAL and
                // terminal commit/rollback.
                let _admission = controller.mutation_guard().await;
                match controller.compose_managed_peer_edges(&declared).await {
                    Ok(edges) => edges,
                    Err(error) => {
                        tracing::error!(error = %error, "failed to compose identity topology overlay");
                        Vec::new()
                    }
                }
            }
            None => declared,
        }
    }

    async fn registered_identities(&self) -> Vec<AgentIdentity> {
        self.entries.read().await.keys().cloned().collect()
    }

    async fn reachable_peer_identities(&self, identity: &AgentIdentity) -> Vec<AgentIdentity> {
        self.desired_peer_edges()
            .await
            .iter()
            .filter_map(|edge| {
                if edge.a() == identity {
                    Some(edge.b().clone())
                } else if edge.b() == identity {
                    Some(edge.a().clone())
                } else {
                    None
                }
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    #[must_use]
    pub fn has_session_bridge(&self) -> bool {
        self.bridge.is_some()
    }

    pub(crate) async fn logical_peer_edges(
        &self,
    ) -> Result<Vec<ManagedPeerEdge>, IdentityRuntimeError> {
        let Some(bridge) = self.bridge.as_ref() else {
            return Ok(Vec::new());
        };
        let runtime_identities: BTreeMap<AgentRuntimeId, AgentIdentity> = self
            .entries
            .read()
            .await
            .iter()
            .filter_map(|(identity, entry)| {
                entry
                    .continuity
                    .as_ref()
                    .map(|record| (record.agent_runtime_id.clone(), identity.clone()))
            })
            .collect();
        let runtime_edges = bridge.current_member_wires().await.map_err(|error| {
            IdentityRuntimeError::Internal(format!("bridge current_member_wires: {error}"))
        })?;
        Ok(runtime_edges
            .into_iter()
            .filter_map(|(runtime_a, runtime_b)| {
                let a = runtime_identities.get(&runtime_a)?.clone();
                let b = runtime_identities.get(&runtime_b)?.clone();
                ManagedPeerEdge::new(a, b).ok()
            })
            .collect())
    }

    pub(crate) async fn logical_peer_edges_any_half(
        &self,
    ) -> Result<Vec<ManagedPeerEdge>, IdentityRuntimeError> {
        let Some(bridge) = self.bridge.as_ref() else {
            return Ok(Vec::new());
        };
        let runtime_identities: BTreeMap<AgentRuntimeId, AgentIdentity> = self
            .entries
            .read()
            .await
            .iter()
            .filter_map(|(identity, entry)| {
                entry
                    .continuity
                    .as_ref()
                    .map(|record| (record.agent_runtime_id.clone(), identity.clone()))
            })
            .collect();
        let runtime_edges = bridge
            .current_member_wires_any_half()
            .await
            .map_err(|error| {
                IdentityRuntimeError::Internal(format!(
                    "bridge current_member_wires_any_half: {error}"
                ))
            })?;
        Ok(runtime_edges
            .into_iter()
            .filter_map(|(runtime_a, runtime_b)| {
                let a = runtime_identities.get(&runtime_a)?.clone();
                let b = runtime_identities.get(&runtime_b)?.clone();
                ManagedPeerEdge::new(a, b).ok()
            })
            .collect())
    }

    pub(crate) fn identity_edge_ownership(&self) -> IdentityEdgeOwnership {
        *self
            .identity_edge_ownership
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Identity edges the mob definition's wiring declares over this
    /// runtime's identities (`auto_wire_orchestrator` / `role_wiring`, an
    /// identity's profile as its role). They are the definition's: its
    /// spawn-time wiring and edge reconciler own them, so the topology never
    /// prunes them.
    pub(crate) async fn definition_declared_identity_edges(
        &self,
    ) -> BTreeSet<(AgentIdentity, AgentIdentity)> {
        let definition = self
            .reset_roster_source
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|source| source.mob_definition.clone());
        let Some(policy) = definition.as_ref().and_then(
            crate::unified_runtime::edge_reconcile::DefinitionWiringEdgeDiscovery::from_definition,
        ) else {
            return BTreeSet::new();
        };
        let members: Vec<crate::unified_runtime::edge_types::EdgeMemberView> = self
            .entries
            .read()
            .await
            .iter()
            .map(
                |(identity, entry)| crate::unified_runtime::edge_types::EdgeMemberView {
                    agent_identity: identity.as_str().to_string(),
                    role: entry.spec.profile.to_string(),
                    wired_to: BTreeSet::new(),
                    labels: BTreeMap::new(),
                },
            )
            .collect();
        policy
            .desired_edges(&members)
            .into_iter()
            .filter_map(|edge| {
                let (a, b) = edge.endpoints();
                let a = AgentIdentity::parse(a).ok()?;
                let b = AgentIdentity::parse(b).ok()?;
                Some(if a <= b { (a, b) } else { (b, a) })
            })
            .collect()
    }

    pub(crate) fn set_identity_edge_ownership(&self, ownership: IdentityEdgeOwnership) {
        *self
            .identity_edge_ownership
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = ownership;
    }

    pub(crate) async fn managed_peer_edges_snapshot(
        &self,
    ) -> BTreeSet<(AgentIdentity, AgentIdentity)> {
        self.managed_peer_edges.read().await.clone()
    }

    pub(crate) async fn retain_managed_peer_edges(
        &self,
        edges: &BTreeSet<(AgentIdentity, AgentIdentity)>,
    ) {
        self.managed_peer_edges
            .write()
            .await
            .extend(edges.iter().cloned());
    }

    /// Logical identity actuator used only while the shared topology
    /// controller's admission lock is already held by TopologyRuntimeHandle.
    ///
    /// Same plan / wire / revalidate contract as
    /// [`Self::reconcile_managed_peer_edges_admitted`]: the endpoints'
    /// runtime bindings are resolved from one snapshot, the bounded bridge
    /// calls run with no lifecycle lock held, and the bookkeeping commits
    /// (under the entries read lock, never a lifecycle lock) only if neither
    /// binding moved; otherwise the mutation is replanned against the live
    /// bindings. A wiring error while an endpoint's binding moved (e.g. it
    /// was just retired) is a replan too, not a failure. A wiring call that
    /// timed out leaves the edge in the unsettled set for the next reconcile.
    pub(crate) async fn mutate_managed_peer_edge_admitted(
        &self,
        action: crate::topology_control::TopologyAction,
        edge: &ManagedPeerEdge,
    ) -> Result<(), IdentityRuntimeError> {
        let _guard = self.managed_peer_reconcile_lock.lock().await;
        let Some(bridge) = self.bridge.clone() else {
            return Err(IdentityRuntimeError::Internal(
                "topology mutation requires a session bridge".to_string(),
            ));
        };
        let endpoints: BTreeSet<AgentIdentity> =
            [edge.a().clone(), edge.b().clone()].into_iter().collect();
        let key = (edge.a().clone(), edge.b().clone());
        for _attempt in 0..PEER_TOPOLOGY_MAX_PLAN_ATTEMPTS {
            let epoch = self.peer_binding_epoch().await;
            match self
                .mutate_managed_peer_edge_attempt(&bridge, action, edge, &key, &endpoints, &epoch)
                .await
            {
                Ok(PeerTopologyAttempt::Committed) => return Ok(()),
                Ok(PeerTopologyAttempt::Stale) => {}
                Err(error) => {
                    if matches!(error, IdentityRuntimeError::PeerWiringTimedOut { .. }) {
                        self.record_unsettled_peer_wiring([key.clone()]).await;
                        return Err(error);
                    }
                    if self.peer_bindings_moved(&epoch, &endpoints).await {
                        tracing::debug!(
                            edge = ?key,
                            %error,
                            "identity-first topology mutation failed while an endpoint's binding \
                             moved; replanning"
                        );
                    } else {
                        return Err(error);
                    }
                }
            }
            tracing::debug!(
                edge = ?key,
                "identity-first topology mutation went stale during wiring; replanning"
            );
        }
        Err(IdentityRuntimeError::PeerTopologySuperseded {
            attempts: PEER_TOPOLOGY_MAX_PLAN_ATTEMPTS,
        })
    }

    /// One plan / wire / revalidate pass of
    /// [`Self::mutate_managed_peer_edge_admitted`] against `epoch`.
    async fn mutate_managed_peer_edge_attempt(
        &self,
        bridge: &Arc<dyn SessionBridge>,
        action: crate::topology_control::TopologyAction,
        edge: &ManagedPeerEdge,
        key: &(AgentIdentity, AgentIdentity),
        endpoints: &BTreeSet<AgentIdentity>,
        epoch: &PeerBindingEpoch,
    ) -> Result<PeerTopologyAttempt, IdentityRuntimeError> {
        let resolve = |identity: &AgentIdentity| {
            epoch.active.get(identity).cloned().ok_or_else(|| {
                IdentityRuntimeError::Internal(format!(
                    "topology endpoint is not active: {identity}"
                ))
            })
        };
        let (runtime_a, runtime_b) = (resolve(edge.a())?, resolve(edge.b())?);
        let current = bounded_peer_wiring("current_member_wires", bridge.current_member_wires())
            .await?
            .map_err(|error| {
                IdentityRuntimeError::Internal(format!("bridge current_member_wires: {error}"))
            })?;
        let actual = current.iter().any(|(a, b)| {
            (a == &runtime_a && b == &runtime_b) || (a == &runtime_b && b == &runtime_a)
        });
        let any_half = bounded_peer_wiring(
            "current_member_wires_any_half",
            bridge.current_member_wires_any_half(),
        )
        .await?
        .map_err(|error| {
            IdentityRuntimeError::Internal(format!("bridge current_member_wires_any_half: {error}"))
        })?
        .iter()
        .any(|(a, b)| (a == &runtime_a && b == &runtime_b) || (a == &runtime_b && b == &runtime_a));
        let wire = || async {
            bounded_peer_wiring(
                "wire_peers_batch",
                bridge.wire_peers_batch(&[(runtime_a.clone(), runtime_b.clone())]),
            )
            .await?
            .map_err(|error| {
                IdentityRuntimeError::Internal(format!("bridge wire_peers_batch: {error}"))
            })
        };
        let unwire = || async {
            bounded_peer_wiring("unwire_peer", bridge.unwire_peer(&runtime_a, &runtime_b))
                .await?
                .map_err(|error| {
                    IdentityRuntimeError::Internal(format!("bridge unwire_peer: {error}"))
                })
        };
        match action {
            crate::topology_control::TopologyAction::Connect if !actual => {
                wire().await?;
            }
            crate::topology_control::TopologyAction::Reconnect => {
                if any_half {
                    unwire().await?;
                }
                wire().await?;
            }
            crate::topology_control::TopologyAction::Disconnect if any_half => {
                unwire().await?;
            }
            _ => {}
        }
        let committed = self
            .commit_managed_edges_if_current(epoch, endpoints, |managed| {
                if matches!(action, crate::topology_control::TopologyAction::Disconnect) {
                    managed.remove(key);
                } else {
                    managed.insert(key.clone());
                }
            })
            .await;
        Ok(if committed.is_some() {
            PeerTopologyAttempt::Committed
        } else {
            PeerTopologyAttempt::Stale
        })
    }

    /// Apply identity-first managed topology to the concrete mob graph.
    ///
    /// Topology providers return stable logical identities. The mob comms graph
    /// is keyed by active runtime member IDs, so this resolves each endpoint
    /// through continuity records before calling the same-mob bridge wire APIs.
    pub async fn reconcile_managed_peer_edges(
        &self,
        desired_edges: &[ManagedPeerEdge],
    ) -> Result<(), IdentityRuntimeError> {
        let topology_controller = self.topology_controller();
        let _topology_guard = match topology_controller.as_ref() {
            Some(controller) => Some(controller.mutation_guard().await),
            None => None,
        };
        if let Some(controller) = topology_controller.as_ref() {
            controller
                .prepare_pending_recovery()
                .await
                .map_err(|error| {
                    IdentityRuntimeError::Internal(format!("topology recovery journal: {error}"))
                })?;
        }
        let pending_recovery_edges = if let Some(controller) = topology_controller.as_ref() {
            controller
                .pending_local_recovery_edges()
                .await
                .map_err(|error| {
                    IdentityRuntimeError::Internal(format!("topology recovery ownership: {error}"))
                })?
        } else {
            BTreeSet::new()
        };
        if !pending_recovery_edges.is_empty() {
            self.retain_managed_peer_edges(&pending_recovery_edges)
                .await;
        }
        let composed_edges;
        let desired_edges = if let Some(controller) = topology_controller.as_ref() {
            composed_edges = controller
                .compose_managed_peer_edges(desired_edges)
                .await
                .map_err(|error| {
                    IdentityRuntimeError::Internal(format!("topology overlay: {error}"))
                })?;
            composed_edges.as_slice()
        } else {
            desired_edges
        };
        let result = self
            .reconcile_managed_peer_edges_admitted(desired_edges)
            .await;
        let mut recovery_inspection_error = None;
        if let Some(controller) = topology_controller.as_ref() {
            let recovery_complete = if result.is_ok() && !pending_recovery_edges.is_empty() {
                match self
                    .pending_recovery_is_physically_complete(desired_edges, &pending_recovery_edges)
                    .await
                {
                    Ok(complete) => complete,
                    Err(error) => {
                        recovery_inspection_error = Some(error);
                        false
                    }
                }
            } else {
                false
            };
            controller
                .finalize_recovered_pending(result.is_ok() && recovery_complete)
                .await
                .map_err(|error| {
                    IdentityRuntimeError::Internal(format!("topology recovery receipt: {error}"))
                })?;
        }
        result?;
        if let Some(error) = recovery_inspection_error {
            return Err(error);
        }
        Ok(())
    }

    pub(crate) async fn pending_recovery_is_physically_complete(
        &self,
        desired_edges: &[ManagedPeerEdge],
        pending_recovery_edges: &BTreeSet<(AgentIdentity, AgentIdentity)>,
    ) -> Result<bool, IdentityRuntimeError> {
        if self.bridge.is_none() {
            return Ok(false);
        }
        let active_identities = self
            .entries
            .read()
            .await
            .iter()
            .filter_map(|(identity, entry)| {
                (entry.state == IdentityLifecycleState::Active && entry.continuity.is_some())
                    .then_some(identity.clone())
            })
            .collect::<BTreeSet<_>>();
        if pending_recovery_edges
            .iter()
            .any(|(a, b)| !active_identities.contains(a) || !active_identities.contains(b))
        {
            return Ok(false);
        }
        let desired = desired_edges
            .iter()
            .map(|edge| (edge.a().clone(), edge.b().clone()))
            .collect::<BTreeSet<_>>();
        let actual = self
            .logical_peer_edges()
            .await?
            .into_iter()
            .map(|edge| (edge.a().clone(), edge.b().clone()))
            .collect::<BTreeSet<_>>();
        let actual_any_half = self
            .logical_peer_edges_any_half()
            .await?
            .into_iter()
            .map(|edge| (edge.a().clone(), edge.b().clone()))
            .collect::<BTreeSet<_>>();

        Ok(desired.is_subset(&actual)
            && pending_recovery_edges
                .difference(&desired)
                .all(|edge| !actual_any_half.contains(edge)))
    }

    /// Reconcile an already-composed desired topology while the caller holds
    /// the shared topology-controller admission guard.
    ///
    /// Plan / wire / revalidate. The plan is computed from one snapshot of the
    /// endpoints' runtime bindings (its epoch) with no lifecycle lock held;
    /// the bridge wiring calls (mob-actor round trips, each bounded by
    /// [`PEER_WIRING_CALL_BUDGET`]) run with no lifecycle lock held either.
    /// The commit takes no lifecycle lock at all: it revalidates the involved
    /// endpoints' bindings and writes the managed-edge bookkeeping while
    /// holding the entries read lock (see
    /// [`Self::commit_managed_edges_if_current`]), so no admission path ever
    /// waits behind a topology commit. A binding that changed meanwhile
    /// (reset, respawn, retire) makes the plan Stale: nothing is committed
    /// and the reconcile replans from the live bindings, at most
    /// [`PEER_TOPOLOGY_MAX_PLAN_ATTEMPTS`] times.
    pub(crate) async fn reconcile_managed_peer_edges_admitted(
        &self,
        desired_edges: &[ManagedPeerEdge],
    ) -> Result<(), IdentityRuntimeError> {
        let _guard = self.managed_peer_reconcile_lock.lock().await;
        let Some(bridge) = self.bridge.clone() else {
            return Ok(());
        };
        let desired: BTreeSet<(AgentIdentity, AgentIdentity)> = desired_edges
            .iter()
            .map(|edge| (edge.a().clone(), edge.b().clone()))
            .collect();
        self.reconcile_managed_peer_edges_planned(&bridge, &desired, None, None)
            .await
    }

    /// The replan loop shared by the full reconcile and the wake path's
    /// target-scoped reconcile. The caller holds the managed-peer reconcile
    /// lock (and the topology admission guard when a controller exists).
    async fn reconcile_managed_peer_edges_planned(
        &self,
        bridge: &Arc<dyn SessionBridge>,
        desired: &BTreeSet<(AgentIdentity, AgentIdentity)>,
        scope: Option<&AgentIdentity>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), IdentityRuntimeError> {
        for _attempt in 0..PEER_TOPOLOGY_MAX_PLAN_ATTEMPTS {
            match self
                .reconcile_managed_peer_edges_attempt(bridge, desired, scope, deadline)
                .await?
            {
                PeerTopologyAttempt::Committed => return Ok(()),
                PeerTopologyAttempt::Stale => {
                    tracing::debug!(
                        "identity-first topology plan went stale during wiring; replanning"
                    );
                }
            }
        }
        Err(IdentityRuntimeError::PeerTopologySuperseded {
            attempts: PEER_TOPOLOGY_MAX_PLAN_ATTEMPTS,
        })
    }

    /// One plan / wire / revalidate pass of
    /// [`Self::reconcile_managed_peer_edges_planned`]. With a `scope`, only
    /// edges touching that identity are planned, wired or cleaned up (the
    /// wake path's target-only reconcile); every other edge is left alone.
    async fn reconcile_managed_peer_edges_attempt(
        &self,
        bridge: &Arc<dyn SessionBridge>,
        desired: &BTreeSet<(AgentIdentity, AgentIdentity)>,
        scope: Option<&AgentIdentity>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<PeerTopologyAttempt, IdentityRuntimeError> {
        let in_scope = |edge: &(AgentIdentity, AgentIdentity)| {
            scope.is_none_or(|target| &edge.0 == target || &edge.1 == target)
        };
        // Edges whose earlier wiring call timed out. Their command may still
        // be queued in the mob actor, and inspection reads roster and machine
        // state directly (not through the actor queue), so what inspection
        // says about them is not their fate. Each is settled only by an
        // ORDERED command that succeeded after it: a wire if it is still
        // desired, a tolerant unwire (not-wired is Ok) if not. Until then it
        // stays unsettled and inspection is never trusted for it.
        let unsettled: BTreeSet<(AgentIdentity, AgentIdentity)> = self
            .unsettled_peer_wiring
            .read()
            .await
            .iter()
            .filter(|edge| in_scope(edge))
            .cloned()
            .collect();
        let managed_snapshot: BTreeSet<(AgentIdentity, AgentIdentity)> = self
            .managed_peer_edges
            .read()
            .await
            .iter()
            .filter(|edge| in_scope(edge))
            .cloned()
            .collect();
        let desired: BTreeSet<(AgentIdentity, AgentIdentity)> = desired
            .iter()
            .filter(|edge| in_scope(edge))
            .cloned()
            .collect();
        let epoch = self.peer_binding_epoch().await;
        let runtime_identities: BTreeMap<AgentRuntimeId, AgentIdentity> = epoch
            .known
            .iter()
            .map(|(identity, runtime_id)| (runtime_id.clone(), identity.clone()))
            .collect();
        let logical = |runtime_edges: &[(AgentRuntimeId, AgentRuntimeId)]| {
            runtime_edges
                .iter()
                .filter_map(|(runtime_a, runtime_b)| {
                    let a = runtime_identities.get(runtime_a)?;
                    let b = runtime_identities.get(runtime_b)?;
                    if a <= b {
                        Some((a.clone(), b.clone()))
                    } else {
                        Some((b.clone(), a.clone()))
                    }
                })
                .collect::<BTreeSet<_>>()
        };
        let current_logical_edges: Option<BTreeSet<(AgentIdentity, AgentIdentity)>> =
            match bounded_peer_wiring_by(
                "current_member_wires",
                deadline,
                bridge.current_member_wires(),
            )
            .await?
            {
                Ok(current_runtime_edges) => Some(logical(&current_runtime_edges)),
                Err(err) => {
                    tracing::debug!(
                        error = %err,
                        "identity-first topology reconcile could not inspect current member wires"
                    );
                    None
                }
            };
        let current_any_half_edges: Option<BTreeSet<(AgentIdentity, AgentIdentity)>> =
            match bounded_peer_wiring_by(
                "current_member_wires_any_half",
                deadline,
                bridge.current_member_wires_any_half(),
            )
            .await?
            {
                Ok(current_runtime_edges) => Some(logical(&current_runtime_edges)),
                Err(error) => {
                    tracing::debug!(
                        %error,
                        "identity-first topology reconcile could not inspect orphan wire halves"
                    );
                    None
                }
            };
        let active_runtimes = &epoch.active;

        let edge_is_managed_and_live = |edge: &(AgentIdentity, AgentIdentity)| {
            // Managed-but-missing live edges are retried deliberately so tolerant topology restores self-heal.
            managed_snapshot.contains(edge)
                && current_logical_edges
                    .as_ref()
                    .is_none_or(|edges| edges.contains(edge))
        };
        let retained_logical_edges: Vec<(AgentIdentity, AgentIdentity)> = desired
            .iter()
            .filter(|edge| !unsettled.contains(*edge))
            .filter(|edge| !edge_is_managed_and_live(edge))
            .filter(|edge| {
                current_logical_edges
                    .as_ref()
                    .is_some_and(|edges| edges.contains(*edge))
            })
            .filter(|(a, b)| active_runtimes.contains_key(a) && active_runtimes.contains_key(b))
            .cloned()
            .collect();
        let to_wire: Vec<(AgentIdentity, AgentIdentity, AgentRuntimeId, AgentRuntimeId)> = desired
            .iter()
            .filter(|edge| {
                // A still-desired unsettled edge gets an ordered (idempotent)
                // wire whatever inspection reports.
                unsettled.contains(*edge)
                    || (!edge_is_managed_and_live(edge)
                        && current_logical_edges
                            .as_ref()
                            .is_none_or(|edges| !edges.contains(*edge)))
            })
            .filter_map(|(a, b)| {
                let runtime_a = active_runtimes.get(a)?;
                let runtime_b = active_runtimes.get(b)?;
                Some((a.clone(), b.clone(), runtime_a.clone(), runtime_b.clone()))
            })
            .collect();

        // Stale: a managed edge that is no longer desired and, when a
        // topology provider owns the topology, every live edge between two of
        // this runtime's identities that no declared owner wants (neither the
        // desired set nor the mob definition's wiring). The managed
        // set is this process's memory and starts empty, while the mob's
        // wiring is durable and replays on recovery, so an edge wired before
        // a restart is live but unmanaged (see `IdentityEdgeOwnership`). An
        // edge to a member that is not one of this runtime's identities never
        // appears here.
        // A live edge the mob definition's wiring declares has its own
        // owner; it is never the topology's to prune.
        let live_owned_edges: BTreeSet<(AgentIdentity, AgentIdentity)> = match (
            self.identity_edge_ownership(),
            current_logical_edges.as_ref(),
        ) {
            (IdentityEdgeOwnership::TopologyProvider, Some(live)) => {
                let definition_declared = self.definition_declared_identity_edges().await;
                live.iter()
                    .filter(|edge| in_scope(edge))
                    .filter(|edge| !definition_declared.contains(*edge))
                    .cloned()
                    .collect()
            }
            (IdentityEdgeOwnership::TopologyProvider, None)
            | (IdentityEdgeOwnership::WiredByThisProcess, _) => BTreeSet::new(),
        };
        let stale: Vec<(AgentIdentity, AgentIdentity)> = managed_snapshot
            .iter()
            .chain(live_owned_edges.iter())
            .filter(|edge| !desired.contains(*edge))
            .filter(|edge| !unsettled.contains(*edge))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        // No longer desired and unsettled: an ordered, tolerant unwire,
        // issued whatever inspection says (it queues behind a pending wire).
        // One whose endpoint is no longer Active has no member left for a
        // late wire to land on (retire removes the roster entry and its
        // wiring), so it settles without a command.
        let (forced_unwire, unsettled_without_members): (Vec<_>, Vec<_>) = unsettled
            .iter()
            .filter(|edge| !desired.contains(*edge))
            .map(
                |(a, b)| match (active_runtimes.get(a), active_runtimes.get(b)) {
                    (Some(runtime_a), Some(runtime_b)) => {
                        Ok((a.clone(), b.clone(), runtime_a.clone(), runtime_b.clone()))
                    }
                    _ => Err((a.clone(), b.clone())),
                },
            )
            .partition(Result::is_ok);
        let forced_unwire: Vec<(AgentIdentity, AgentIdentity, AgentRuntimeId, AgentRuntimeId)> =
            forced_unwire.into_iter().filter_map(Result::ok).collect();
        let unsettled_without_members: Vec<(AgentIdentity, AgentIdentity)> =
            unsettled_without_members
                .into_iter()
                .filter_map(Result::err)
                .collect();
        let to_unwire: Vec<(AgentIdentity, AgentIdentity, AgentRuntimeId, AgentRuntimeId)> = stale
            .iter()
            .filter_map(|(a, b)| {
                let runtime_a = active_runtimes.get(a)?;
                let runtime_b = active_runtimes.get(b)?;
                if current_any_half_edges
                    .as_ref()
                    .is_some_and(|edges| !edges.contains(&(a.clone(), b.clone())))
                {
                    return None;
                }
                Some((a.clone(), b.clone(), runtime_a.clone(), runtime_b.clone()))
            })
            .collect();

        // Every endpoint whose binding this plan relied on.
        let involved: BTreeSet<AgentIdentity> = retained_logical_edges
            .iter()
            .chain(stale.iter())
            .flat_map(|(a, b)| [a.clone(), b.clone()])
            .chain(
                to_wire
                    .iter()
                    .chain(forced_unwire.iter())
                    .flat_map(|(a, b, _, _)| [a.clone(), b.clone()]),
            )
            .collect();

        let wire_logical_edges = to_wire
            .iter()
            .map(|(a, b, _, _)| (a.clone(), b.clone()))
            .collect::<Vec<_>>();
        let wire_runtime_edges = to_wire
            .iter()
            .map(|(_, _, runtime_a, runtime_b)| (runtime_a.clone(), runtime_b.clone()))
            .collect::<Vec<_>>();
        if !wire_runtime_edges.is_empty() {
            match bounded_peer_wiring_by(
                "wire_peers_batch",
                deadline,
                bridge.wire_peers_batch(&wire_runtime_edges),
            )
            .await
            {
                Ok(Ok(())) => {
                    // An ordered wire succeeded after any pending command for
                    // these edges: their fate is known again.
                    self.settle_unsettled_peer_wiring(wire_logical_edges.iter().cloned())
                        .await;
                }
                Ok(Err(error)) => {
                    // A refusal from an endpoint that was retired or respawned
                    // under the plan is the plan going stale, not a failure.
                    if self.peer_bindings_moved(&epoch, &involved).await {
                        return Ok(PeerTopologyAttempt::Stale);
                    }
                    return Err(IdentityRuntimeError::Internal(format!(
                        "bridge wire_peers_batch: {error}"
                    )));
                }
                Err(timed_out) => {
                    // The batch may still land in the actor after its reply
                    // was dropped: its edges' fate is unknown.
                    self.record_unsettled_peer_wiring(wire_logical_edges.iter().cloned())
                        .await;
                    return Err(timed_out);
                }
            }
        }

        let unwire_results = stream::iter(to_unwire.into_iter().chain(forced_unwire).map(
            |(a, b, runtime_a, runtime_b)| {
                let bridge = bridge.clone();
                async move {
                    // The bridge's unwire is tolerant: an edge that is not
                    // wired (or a peer that is gone) is Ok.
                    let result = bounded_peer_wiring_by(
                        "unwire_peer",
                        deadline,
                        bridge.unwire_peer(&runtime_a, &runtime_b),
                    )
                    .await
                    .map(|outcome| outcome.map_err(|e| format!("{e}")));
                    (a, b, result)
                }
            },
        ))
        .buffer_unordered(MANAGED_PEER_RECONCILE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        let timed_out_unwires: Vec<(AgentIdentity, AgentIdentity)> = unwire_results
            .iter()
            .filter(|(_, _, result)| result.is_err())
            .map(|(a, b, _)| (a.clone(), b.clone()))
            .collect();
        if !timed_out_unwires.is_empty() {
            self.record_unsettled_peer_wiring(timed_out_unwires).await;
        }
        // Ordered unwires that succeeded, and unsettled edges with no member
        // left, are settled; a refused or timed-out one stays unsettled.
        self.settle_unsettled_peer_wiring(
            unwire_results
                .iter()
                .filter(|(_, _, result)| matches!(result, Ok(Ok(()))))
                .map(|(a, b, _)| (a.clone(), b.clone()))
                .filter(|edge| unsettled.contains(edge))
                .chain(unsettled_without_members.iter().cloned()),
        )
        .await;

        // Commit only if none of the involved bindings moved since the plan
        // was made; no lifecycle lock is taken.
        let committed = self
            .commit_managed_edges_if_current(&epoch, &involved, |managed| {
                for edge in retained_logical_edges {
                    managed.insert(edge);
                }
                for edge in wire_logical_edges {
                    managed.insert(edge);
                }
                for (a, b) in stale {
                    let key = (a.clone(), b.clone());
                    if current_any_half_edges
                        .as_ref()
                        .is_some_and(|edges| !edges.contains(&key))
                    {
                        managed.remove(&key);
                    }
                }
                for edge in unsettled_without_members {
                    managed.remove(&edge);
                }
                for (a, b, result) in unwire_results {
                    result?.map_err(|e| {
                        IdentityRuntimeError::Internal(format!("bridge unwire_peer: {e}"))
                    })?;
                    managed.remove(&(a, b));
                }
                Ok::<(), IdentityRuntimeError>(())
            })
            .await;
        match committed {
            Some(result) => result.map(|()| PeerTopologyAttempt::Committed),
            None => Ok(PeerTopologyAttempt::Stale),
        }
    }

    /// Commit managed-edge bookkeeping if none of `involved` changed binding
    /// since `epoch`. The entries read lock is held across the revalidation
    /// and the commit, so no binding change (every one is an entries write)
    /// can interleave; this is what the endpoints' lifecycle locks used to
    /// guarantee, without making any admission wait on a topology commit.
    /// `managed_peer_edges` is only ever taken after `entries`, never while
    /// awaiting it. Returns `None` (nothing committed) when the plan is stale.
    async fn commit_managed_edges_if_current<R>(
        &self,
        epoch: &PeerBindingEpoch,
        involved: &BTreeSet<AgentIdentity>,
        commit: impl FnOnce(&mut BTreeSet<(AgentIdentity, AgentIdentity)>) -> R,
    ) -> Option<R> {
        let entries = self.entries.read().await;
        if !epoch.unchanged_for(&peer_binding_epoch_of(&entries), involved) {
            return None;
        }
        let mut managed = self.managed_peer_edges.write().await;
        Some(commit(&mut managed))
    }

    /// Whether any of `involved` changed binding since `epoch`.
    async fn peer_bindings_moved(
        &self,
        epoch: &PeerBindingEpoch,
        involved: &BTreeSet<AgentIdentity>,
    ) -> bool {
        !epoch.unchanged_for(&self.peer_binding_epoch().await, involved)
    }

    /// Record edges whose wiring call timed out (see
    /// [`IdentityRuntime::unsettled_peer_wiring`]).
    async fn record_unsettled_peer_wiring(
        &self,
        edges: impl IntoIterator<Item = (AgentIdentity, AgentIdentity)>,
    ) {
        self.unsettled_peer_wiring.write().await.extend(edges);
    }

    /// Settle unsettled edges: an ordered command for each succeeded after
    /// its timed-out one (or no member is left for it to land on).
    async fn settle_unsettled_peer_wiring(
        &self,
        edges: impl IntoIterator<Item = (AgentIdentity, AgentIdentity)>,
    ) {
        let mut unsettled = self.unsettled_peer_wiring.write().await;
        for edge in edges {
            unsettled.remove(&edge);
        }
    }

    /// Edges whose last wiring call timed out and that no ordered command has
    /// settled yet.
    #[cfg(test)]
    pub(crate) async fn unsettled_peer_wiring_snapshot(
        &self,
    ) -> BTreeSet<(AgentIdentity, AgentIdentity)> {
        self.unsettled_peer_wiring.read().await.clone()
    }

    /// One consistent snapshot of every registered identity's runtime binding:
    /// the epoch a managed-topology plan is made against and revalidated with.
    async fn peer_binding_epoch(&self) -> PeerBindingEpoch {
        peer_binding_epoch_of(&*self.entries.read().await)
    }

    /// Emit an event for the given identity. Best-effort — no error if no subscribers.
    async fn emit_event(&self, identity: &AgentIdentity, event: IdentityEvent) {
        let channels = self.event_channels.read().await;
        if let Some(tx) = channels.get(identity) {
            let _ = tx.send(event);
        }
    }

    fn emit_error(&self, event: crate::unified_runtime::types::ErrorEvent) {
        let hook = match self.error_hook.read() {
            Ok(stored_hook) => stored_hook.clone(),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "identity runtime error hook lock poisoned; dropping error event"
                );
                None
            }
        };
        // The identity runtime owns a hook slot of its own, so it is a second
        // place an ErrorEvent can vanish when no host wired one. Same default
        // sink as the unified-runtime fire point.
        crate::unified_runtime::log_error_event(&event, hook.is_some());
        if let Some(hook) = hook {
            tokio::spawn(async move {
                let () = hook(event).await;
            });
        }
    }

    async fn materialization_backoff_error(&self, identity: &AgentIdentity) -> Option<String> {
        let backoffs = self.materialization_failure_backoff.read().await;
        let backoff = backoffs.get(identity)?;
        if Instant::now() < backoff.suppress_until {
            Some(backoff.error.clone())
        } else {
            None
        }
    }

    async fn clear_materialization_backoff(&self, identity: &AgentIdentity) {
        self.materialization_failure_backoff
            .write()
            .await
            .remove(identity);
    }

    async fn record_best_effort_materialization_failure(
        &self,
        identity: &AgentIdentity,
        initiator: Option<&AgentIdentity>,
        operation: &'static str,
        err: &IdentityRuntimeError,
    ) {
        let error = err.to_string();
        let suppress_until = Instant::now() + MATERIALIZATION_FAILURE_BACKOFF;
        self.materialization_failure_backoff.write().await.insert(
            identity.clone(),
            MaterializationFailureBackoff {
                suppress_until,
                error: error.clone(),
            },
        );
        self.emit_error(
            crate::unified_runtime::types::ErrorEvent::IdentityMaterializationFailure {
                identity: identity.to_string(),
                initiator: initiator.map(ToString::to_string),
                operation: operation.to_string(),
                error,
            },
        );
    }

    // -----------------------------------------------------------------------
    // Registration / activation
    // -----------------------------------------------------------------------

    /// Register an identity entry in the runtime (called during restore flow).
    pub async fn register(
        &self,
        spec: DurableAgentSpec,
        state: IdentityLifecycleState,
        continuity: Option<ContinuityRecord>,
        lease: Option<LeaseGrant>,
    ) {
        let identity = spec.identity.clone();
        let bootstrap_generation = self.identity_bootstrap_status_with_generation().0;
        let cpv = continuity
            .as_ref()
            .map(|r| r.checkpoint_version)
            .unwrap_or(CheckpointVersion::new(0));
        let lease_entry = lease.map(|g| LeaseEntry {
            fencing_token: g.fencing_token,
            ttl: g.ttl,
            acquired_at: Instant::now(),
        });
        let has_active_lease = state == IdentityLifecycleState::Active && lease_entry.is_some();
        {
            let mut entries = self.entries.write().await;
            // A terminal heal verdict is about the durable head, not this
            // entry instance: re-projecting Broken (a repair retry, an eager
            // reconcile) must not silently forget it, while any non-Broken
            // projection is a real lifecycle transition that supersedes it
            // (2026-07-29 heal/re-Break incident).
            let continuity_unrecoverable = if state == IdentityLifecycleState::Broken {
                entries
                    .get(&identity)
                    .and_then(|existing| existing.continuity_unrecoverable.clone())
            } else {
                None
            };
            // Same rule for the typed repair hold: it is about the durable
            // document, so a re-projected Broken entry keeps it and any
            // non-Broken projection (a successful resume) clears it.
            let session_repair_required = if state == IdentityLifecycleState::Broken {
                entries
                    .get(&identity)
                    .and_then(|existing| existing.session_repair_required.clone())
            } else {
                None
            };
            // A host-rejected-build park is about the SPEC, not this entry
            // instance: re-registration (a reconcile pass, a repair retry)
            // with the same spec must not forget it — retrying an unchanged
            // spec against a deterministic gate re-burns a build + callback
            // round trip for the same answer. A changed spec clears it.
            let host_rejected_build_park = entries.get(&identity).and_then(|existing| {
                existing
                    .host_rejected_build_park
                    .clone()
                    .filter(|park| park.spec_digest == durable_spec_digest(&spec))
            });
            let entry = IdentityEntry {
                spec,
                bootstrap_generation,
                state,
                continuity,
                lease: lease_entry,
                pending_lease_release: None,
                checkpoint_version: cpv,
                has_runtime_store: self.has_runtime_store,
                continuity_unrecoverable,
                host_rejected_build_park,
                session_repair_required,
                last_delivery_error: None,
                last_reload: None,
            };
            entries.insert(identity.clone(), entry);
        }

        // Create event channel for this identity
        let (tx, _) = broadcast::channel(IDENTITY_EVENT_CHANNEL_CAPACITY);
        self.event_channels
            .write()
            .await
            .insert(identity.clone(), tx);
        if has_active_lease {
            self.lease_renewal_notify.notify_one();
        }
        self.mark_bootstrap_from_lifecycle(&identity, state, None);
    }

    async fn materialization_lock_for(&self, identity: &AgentIdentity) -> Arc<Mutex<()>> {
        if let Some(lock) = self.materialization_locks.read().await.get(identity) {
            return lock.clone();
        }
        let mut locks = self.materialization_locks.write().await;
        locks
            .entry(identity.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn best_effort_materialization_lock_for(
        &self,
        identity: &AgentIdentity,
    ) -> Arc<Mutex<()>> {
        if let Some(lock) = self
            .best_effort_materialization_locks
            .read()
            .await
            .get(identity)
        {
            return lock.clone();
        }
        let mut locks = self.best_effort_materialization_locks.write().await;
        locks
            .entry(identity.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub(crate) async fn lifecycle_lock_for(&self, identity: &AgentIdentity) -> Arc<Mutex<()>> {
        if let Some(lock) = self.lifecycle_locks.read().await.get(identity) {
            return lock.clone();
        }
        let mut locks = self.lifecycle_locks.write().await;
        locks
            .entry(identity.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub(crate) async fn raw_member_alias_lock(&self, alias: &str) -> Arc<Mutex<()>> {
        let alias = crate::member_comms_id::runtime_alias_str(alias.trim()).into_owned();
        if let Some(lock) = self
            .raw_member_alias_locks
            .read()
            .await
            .entries
            .get(&alias)
            .and_then(Weak::upgrade)
        {
            return lock;
        }
        let mut table = self.raw_member_alias_locks.write().await;
        table.sweep_if_needed();
        // Another caller may have installed the key after our read miss. The
        // write-side upgrade is the authority boundary that prevents two live
        // mutexes from serializing the same alias independently.
        if let Some(lock) = table.entries.get(&alias).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        table.entries.insert(alias, Arc::downgrade(&lock));
        lock
    }

    #[cfg(test)]
    pub(crate) async fn raw_member_alias_lock_metrics(&self) -> (usize, usize, usize) {
        let table = self.raw_member_alias_locks.read().await;
        (table.entries.len(), table.next_sweep_len, table.sweep_count)
    }

    pub(crate) async fn ensure_raw_member_alias_available(
        &self,
        identity: &AgentIdentity,
    ) -> Result<(), IdentityRuntimeError> {
        if let Some(bridge) = self.bridge.as_ref()
            && bridge
                .raw_member_alias_exists(identity.as_str())
                .await
                .map_err(|error| {
                    IdentityRuntimeError::Internal(format!(
                        "raw member namespace inspection for {identity}: {error}"
                    ))
                })?
        {
            return Err(IdentityRuntimeError::Internal(format!(
                "durable identity '{identity}' collides with an existing raw member alias"
            )));
        }
        Ok(())
    }

    /// Resolve an alias into a lifecycle-lock target. Classic members return
    /// `None`; generated aliases always resolve to an authority target even
    /// after deletion so validation under the lock fails closed.
    pub(crate) async fn member_alias_lifecycle_target(
        self: &Arc<Self>,
        alias: &str,
    ) -> Result<Option<MemberAliasLifecycleTarget>, IdentityRuntimeError> {
        let alias = crate::member_comms_id::runtime_alias_str(alias).into_owned();
        match self.identity_for_member_mutation(&alias).await {
            Some(identity) => {
                // Fail stale/deleted generated aliases before unrelated
                // operation prerequisites (for example cross-mob directory
                // lookup). The tracked operation validates again after taking
                // the lifecycle lock, so this preflight does not become the
                // authority boundary or introduce a TOCTOU gap.
                self.ensure_expected_member_alias_current(&identity, &alias)
                    .await?;
                Ok(Some(MemberAliasLifecycleTarget {
                    runtime: Arc::clone(self),
                    lock: self.lifecycle_lock_for(&identity).await,
                    identity,
                    alias,
                }))
            }
            None if crate::member_comms_id::is_reserved_generated_alias(&alias) => {
                Err(IdentityRuntimeError::Internal(format!(
                    "generated member alias requires identity authority: {alias}"
                )))
            }
            None => Ok(None),
        }
    }

    /// Acquire member-alias lifecycle targets across one or more identity
    /// runtimes in a deterministic process-global order, then validate every
    /// alias while its owning lock is held. Duplicate identities acquire one
    /// lock but still validate every spelling/generation.
    pub(crate) async fn acquire_member_alias_lifecycle_targets(
        mut targets: Vec<MemberAliasLifecycleTarget>,
    ) -> Result<Vec<tokio::sync::OwnedMutexGuard<()>>, IdentityRuntimeError> {
        targets.sort_by(|a, b| {
            a.runtime
                .runtime_instance_id
                .cmp(&b.runtime.runtime_instance_id)
                .then_with(|| {
                    (Arc::as_ptr(&a.runtime) as usize).cmp(&(Arc::as_ptr(&b.runtime) as usize))
                })
                .then_with(|| a.identity.cmp(&b.identity))
                .then_with(|| a.alias.cmp(&b.alias))
        });

        let mut guards = Vec::with_capacity(targets.len());
        let mut held: Option<(usize, AgentIdentity)> = None;
        for target in targets {
            let key = (
                Arc::as_ptr(&target.runtime) as usize,
                target.identity.clone(),
            );
            if held.as_ref() != Some(&key) {
                guards.push(target.lock.lock_owned().await);
                held = Some(key);
            }
            target
                .runtime
                .ensure_expected_member_alias_current(&target.identity, &target.alias)
                .await?;
            let state = target
                .runtime
                .entries
                .read()
                .await
                .get(&target.identity)
                .map(|entry| entry.state)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(target.identity.clone()))?;
            if state != IdentityLifecycleState::Active {
                return Err(IdentityRuntimeError::InvalidState {
                    identity: target.identity,
                    state,
                    operation: "mutate member alias",
                });
            }
        }
        Ok(guards)
    }

    /// Cancellation-safe operation spanning one or more alias targets. The
    /// first target in global order supervises the transaction to its explicit
    /// commit/rollback boundary if the request future is dropped.
    pub(crate) async fn run_member_alias_targets_operation_tracked<T, F, Fut>(
        targets: Vec<MemberAliasLifecycleTarget>,
        operation: F,
    ) -> Result<T, IdentityRuntimeError>
    where
        T: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, String>> + Send + 'static,
    {
        let mut runtimes = targets
            .iter()
            .map(|target| Arc::clone(&target.runtime))
            .collect::<Vec<_>>();
        runtimes.sort_by(|a, b| {
            a.runtime_instance_id
                .cmp(&b.runtime_instance_id)
                .then_with(|| (Arc::as_ptr(a) as usize).cmp(&(Arc::as_ptr(b) as usize)))
        });
        runtimes.dedup_by(|a, b| Arc::ptr_eq(a, b));
        let transaction = async move {
            let _guards = Self::acquire_member_alias_lifecycle_targets(targets).await?;
            operation().await.map_err(IdentityRuntimeError::Internal)
        };
        if !runtimes.is_empty() {
            return Self::run_tracked_foreground_multi(runtimes, transaction).await;
        }
        transaction.await
    }

    /// Register one compound transaction with every participating runtime.
    /// Non-owner shutdowns wait on a completion task in their own JoinSet,
    /// while the globally first runtime owns the actual operation and result.
    async fn run_tracked_foreground_multi<T, F>(
        runtimes: Vec<Arc<Self>>,
        operation: F,
    ) -> Result<T, IdentityRuntimeError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, IdentityRuntimeError>> + Send + 'static,
    {
        let Some((supervisor, participants)) = runtimes.split_first() else {
            return operation.await;
        };
        if runtimes
            .iter()
            .any(|runtime| runtime.foreground_shutdown.load(Ordering::Acquire))
        {
            return Err(IdentityRuntimeError::Internal(
                "identity runtime is shutting down".to_string(),
            ));
        }

        let (completion, _) = watch::channel(false);
        for runtime in participants {
            let mut operations = runtime.foreground_operations.lock().await;
            if runtime.foreground_shutdown.load(Ordering::Acquire) {
                completion.send_replace(true);
                return Err(IdentityRuntimeError::Internal(
                    "identity runtime is shutting down".to_string(),
                ));
            }
            while let Some(result) = operations.try_join_next() {
                if let Err(error) = result {
                    tracing::error!(
                        error = %error,
                        "tracked foreground identity operation panicked"
                    );
                }
            }
            let mut completed = completion.subscribe();
            operations.spawn(async move {
                while !*completed.borrow() && completed.changed().await.is_ok() {}
            });
        }

        let (sender, receiver) = oneshot::channel();
        {
            let mut operations = supervisor.foreground_operations.lock().await;
            if supervisor.foreground_shutdown.load(Ordering::Acquire) {
                completion.send_replace(true);
                return Err(IdentityRuntimeError::Internal(
                    "identity runtime is shutting down".to_string(),
                ));
            }
            while let Some(result) = operations.try_join_next() {
                if let Err(error) = result {
                    tracing::error!(
                        error = %error,
                        "tracked foreground identity operation panicked"
                    );
                }
            }
            operations.spawn(async move {
                let _completion = MultiRuntimeForegroundCompletion(completion);
                let _ = sender.send(operation.await);
            });
        }
        receiver.await.map_err(|_| {
            IdentityRuntimeError::Internal(
                "tracked foreground identity operation terminated without a result".to_string(),
            )
        })?
    }

    /// Run an externally-cancellable operation under runtime ownership.
    ///
    /// Dropping the caller only drops the result receiver; the transaction
    /// remains in the runtime's join set and reaches its explicit
    /// commit/rollback boundary. Graceful shutdown closes admission and joins
    /// every such task before lease renewal or the mob actor is stopped.
    pub(crate) async fn run_tracked_foreground<T, F>(
        self: &Arc<Self>,
        operation: F,
    ) -> Result<T, IdentityRuntimeError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, IdentityRuntimeError>> + Send + 'static,
    {
        self.remember_self_handle();
        if self.foreground_shutdown.load(Ordering::Acquire) {
            return Err(IdentityRuntimeError::Internal(
                "identity runtime is shutting down".to_string(),
            ));
        }
        let (sender, receiver) = oneshot::channel();
        {
            let mut operations = self.foreground_operations.lock().await;
            if self.foreground_shutdown.load(Ordering::Acquire) {
                return Err(IdentityRuntimeError::Internal(
                    "identity runtime is shutting down".to_string(),
                ));
            }
            while let Some(result) = operations.try_join_next() {
                if let Err(error) = result {
                    tracing::error!(
                        error = %error,
                        "tracked foreground identity operation panicked"
                    );
                }
            }
            operations.spawn(async move {
                let _ = sender.send(operation.await);
            });
        }
        receiver.await.map_err(|_| {
            IdentityRuntimeError::Internal(
                "tracked foreground identity operation terminated without a result".to_string(),
            )
        })?
    }

    /// Install the observer told (an epoch bump) whenever any identity's
    /// lease fencing token changes, and bump it once so the observer
    /// re-reads leases that predate the installation. The unified runtime
    /// installs its identity health monitor's wake here.
    pub(crate) fn install_lease_observer(&self, observer: watch::Sender<u64>) {
        #[cfg(test)]
        self.lease_observer_saw_drain
            .store(self.has_pending_completion_drain(), Ordering::Release);
        *self
            .entries
            .lease_observer
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(observer);
        self.entries.wake_lease_observer();
    }

    pub(crate) fn close_foreground_operations(&self) {
        self.foreground_shutdown.store(true, Ordering::Release);
        // `watch::Sender::send` discards the value when no receiver exists.
        // A just-admitted JoinSet task may not have subscribed yet, so retain
        // shutdown truth for future receivers explicitly.
        self.foreground_cancel.send_replace(true);
    }

    pub(crate) async fn join_foreground_operations(&self) {
        let mut operations = self.foreground_operations.lock().await;
        while let Some(result) = operations.join_next().await {
            if let Err(error) = result {
                tracing::error!(
                    error = %error,
                    "tracked foreground identity operation panicked during shutdown"
                );
            }
        }
    }

    /// Preserve exact grants acquired by a restore task that failed before it
    /// could publish an [`IdentityEntry`]. Entry-scoped pending-release state
    /// cannot represent this phase, so the runtime owns a small orphan-grant
    /// ledger until reconcile or shutdown successfully releases it.
    pub(crate) async fn park_unactivated_lease_releases(&self, grants: &[LeaseGrant]) {
        let mut pending = self.pending_unactivated_lease_releases.write().await;
        for grant in grants {
            if !pending.iter().any(|parked| {
                parked.identity == grant.identity && parked.fencing_token == grant.fencing_token
            }) {
                pending.push(grant.clone());
            }
        }
    }

    /// Release grants that do not yet have an IdentityEntry capable of
    /// retaining retry state. A provider failure atomically transfers their
    /// exact fencing tokens into the runtime-owned orphan ledger.
    pub(crate) async fn release_or_park_untracked_leases(
        &self,
        grants: &[LeaseGrant],
    ) -> Result<(), super::types::LeaseError> {
        let _release_guard = self.pending_unactivated_lease_release_gate.lock().await;
        match self.lease_provider.release_leases(grants).await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.park_unactivated_lease_releases(grants).await;
                Err(error)
            }
        }
    }

    /// Retry every grant parked before lifecycle publication. This runs under
    /// the restore controller before any new batch acquisition, and again from
    /// shutdown after all identity work has joined.
    pub(crate) async fn release_parked_unactivated_leases(
        &self,
    ) -> Result<usize, IdentityRuntimeError> {
        self.release_parked_unactivated_leases_matching(None).await
    }

    /// Retry parked pre-publication grants for one identity before a direct
    /// lazy materialization attempts to acquire fresh authority. The caller
    /// holds that identity's lifecycle lock, so another same-identity path
    /// cannot park a replacement grant between this drain and acquisition.
    async fn release_parked_unactivated_leases_for_identity(
        &self,
        identity: &AgentIdentity,
    ) -> Result<usize, IdentityRuntimeError> {
        self.release_parked_unactivated_leases_matching(Some(identity))
            .await
    }

    async fn release_parked_unactivated_leases_matching(
        &self,
        identity: Option<&AgentIdentity>,
    ) -> Result<usize, IdentityRuntimeError> {
        let _release_guard = self.pending_unactivated_lease_release_gate.lock().await;
        let grants = self
            .pending_unactivated_lease_releases
            .read()
            .await
            .iter()
            .filter(|grant| identity.is_none_or(|identity| grant.identity == *identity))
            .cloned()
            .collect::<Vec<_>>();
        if grants.is_empty() {
            return Ok(0);
        }
        self.lease_provider
            .release_leases(&grants)
            .await
            .map_err(IdentityRuntimeError::Lease)?;
        let mut pending = self.pending_unactivated_lease_releases.write().await;
        pending.retain(|parked| {
            !grants.iter().any(|released| {
                released.identity == parked.identity
                    && released.fencing_token == parked.fencing_token
            })
        });
        Ok(grants.len())
    }

    /// Release every durable identity grant after the lower mob plane has
    /// quiesced. Successful release clears the in-memory authority so the
    /// method is idempotent; failures retain the exact grants for inspection
    /// or a subsequent retry.
    pub(crate) async fn release_all_leases_for_shutdown(
        &self,
    ) -> Result<usize, IdentityRuntimeError> {
        let _parked_release_guard = self.pending_unactivated_lease_release_gate.lock().await;
        let mut grants = self
            .entries
            .read()
            .await
            .iter()
            .filter_map(|(identity, entry)| {
                entry.pending_lease_release.clone().or_else(|| {
                    entry.lease.as_ref().map(|lease| LeaseGrant {
                        identity: identity.clone(),
                        fencing_token: lease.fencing_token,
                        ttl: lease.ttl,
                    })
                })
            })
            .collect::<Vec<_>>();
        let parked_grants = self.pending_unactivated_lease_releases.read().await.clone();
        for grant in parked_grants {
            if !grants.iter().any(|existing| {
                existing.identity == grant.identity && existing.fencing_token == grant.fencing_token
            }) {
                grants.push(grant);
            }
        }
        if grants.is_empty() {
            return Ok(0);
        }

        self.lease_provider
            .release_leases(&grants)
            .await
            .map_err(IdentityRuntimeError::Lease)?;

        let mut entries = self.entries.write().await;
        let mut lifecycle_updates = Vec::new();
        for grant in &grants {
            if let Some(entry) = entries.get_mut(&grant.identity) {
                let released_active_lease = entry
                    .lease
                    .as_ref()
                    .is_some_and(|lease| lease.fencing_token == grant.fencing_token);
                if released_active_lease {
                    entry.lease = None;
                }
                if entry
                    .pending_lease_release
                    .as_ref()
                    .is_some_and(|pending| pending.fencing_token == grant.fencing_token)
                {
                    entry.pending_lease_release = None;
                }
                if released_active_lease
                    && matches!(
                        entry.state,
                        IdentityLifecycleState::Active | IdentityLifecycleState::Suspended
                    )
                {
                    entry.state = IdentityLifecycleState::Dormant;
                    lifecycle_updates.push(grant.identity.clone());
                }
            }
        }
        drop(entries);
        let mut pending = self.pending_unactivated_lease_releases.write().await;
        pending.retain(|parked| {
            !grants.iter().any(|released| {
                released.identity == parked.identity
                    && released.fencing_token == parked.fencing_token
            })
        });
        drop(pending);
        for identity in lifecycle_updates {
            self.mark_bootstrap_from_lifecycle(&identity, IdentityLifecycleState::Dormant, None);
        }
        Ok(grants.len())
    }

    /// Materialize a dormant identity into a concrete mob member/session.
    ///
    /// This is the lazy counterpart to eager `restore_flow`: it performs the
    /// expensive bridge create/resume and snapshot load only when an identity is
    /// actually touched. Parallel calls for one identity coalesce on a
    /// per-identity lock and re-check state after acquiring it.
    pub async fn materialize(
        &self,
        identity: &AgentIdentity,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        self.materialize_with_expected_member_alias(identity, None)
            .await
    }

    async fn materialize_with_expected_member_alias(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        self.materialize_with_expected_member_alias_after_inner(
            identity,
            expected_alias,
            std::future::ready(()),
            TopologyAfterEmbody::Reconcile,
            &mut false,
        )
        .await
    }

    /// Materialize a Dormant/retired send or dispatch target on the admission
    /// path (the wake path).
    ///
    /// The target's own embodiment is required for admission, and so are its
    /// own managed edges: the input wakes the member, and its first turn must
    /// be able to reach the peers it is wired to (a peer message sent before
    /// the edge exists fails as "peer not found" with no wait or retry). So
    /// after the embodiment commits, the TARGET's edges (only edges touching
    /// it) are reconciled before admission, bounded (see
    /// [`Self::reconcile_target_topology_for_wake`]). The fleet-wide
    /// reconcile and the peer builds an ordinary materialization would run
    /// are not needed for admission and would serialize it behind every
    /// topology identity; the sender hydrates those after admission (see
    /// [`Self::hydrate_peer_topology_after_admission`]). Already-Active
    /// targets never come here: they are admitted first (the mid-turn case).
    ///
    /// A failed target reconcile (typed: `PeerWiringTimedOut`,
    /// `PeerTopologySuperseded`, a bridge refusal) is logged and does not
    /// refuse the input. That is the contract topology reconcile has always
    /// had on this path (a reconcile warning never failed a send), and the
    /// post-admission hydration retries it; refusing the operator's message
    /// because one edge could not be wired would trade a degraded first turn
    /// for a lost one.
    async fn materialize_for_admission(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        let mut already_active = false;
        let record = self
            .materialize_with_expected_member_alias_after_inner(
                identity,
                expected_alias,
                std::future::ready(()),
                TopologyAfterEmbody::DeferToPostAdmission,
                &mut already_active,
            )
            .await?;
        // A concurrent materialization won: the target was already Active
        // when the embodiment door ran, so this send woke nothing and is
        // admitted first like any send to an Active target.
        if already_active {
            return Ok(record);
        }
        if let Err(error) = self.reconcile_target_topology_for_wake(identity).await {
            tracing::warn!(
                identity = %identity,
                %error,
                "woken target's managed edges were not reconciled before admission; the \
                 post-admission hydration retries them"
            );
        }
        Ok(record)
    }

    /// Reconcile only `target`'s own managed edges, for the wake path.
    ///
    /// Bounded end to end by ONE deadline ([`WAKE_TOPOLOGY_BUDGET`] from the
    /// start): acquiring the topology admission guard and the managed-peer
    /// reconcile lock settles as
    /// `PeerWiringTimedOut { operation: "wake_topology_locks" }` when the
    /// deadline passes first, and every wiring call inside is capped by the
    /// remaining time (a timed-out wire is recorded unsettled like any
    /// other); a plan that keeps going stale settles as
    /// `PeerTopologySuperseded`. Only edges whose
    /// other endpoint is already Active can be wired here; a Dormant peer is
    /// built and wired by the post-admission hydration.
    async fn reconcile_target_topology_for_wake(
        &self,
        target: &AgentIdentity,
    ) -> Result<(), IdentityRuntimeError> {
        let Some(bridge) = self.bridge.clone() else {
            return Ok(());
        };
        let touches = |a: &AgentIdentity, b: &AgentIdentity| a == target || b == target;
        let declared = self.desired_peer_edges.read().await.clone();
        let controller = self.topology_controller();
        if controller.is_none()
            && !declared.iter().any(|edge| touches(edge.a(), edge.b()))
            && !self
                .managed_peer_edges
                .read()
                .await
                .iter()
                .any(|(a, b)| touches(a, b))
        {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + WAKE_TOPOLOGY_BUDGET;
        let (_topology_guard, _reconcile_guard) =
            bounded_peer_wiring_by("wake_topology_locks", Some(deadline), async {
                let topology_guard = match controller.as_ref() {
                    Some(controller) => Some(controller.mutation_guard().await),
                    None => None,
                };
                (
                    topology_guard,
                    self.managed_peer_reconcile_lock.lock().await,
                )
            })
            .await?;
        let desired_edges = match controller.as_ref() {
            Some(controller) => controller
                .compose_managed_peer_edges(&declared)
                .await
                .map_err(|error| {
                    IdentityRuntimeError::Internal(format!("topology overlay: {error}"))
                })?,
            None => declared,
        };
        let desired: BTreeSet<(AgentIdentity, AgentIdentity)> = desired_edges
            .iter()
            .filter(|edge| touches(edge.a(), edge.b()))
            .map(|edge| (edge.a().clone(), edge.b().clone()))
            .collect();
        self.reconcile_managed_peer_edges_planned(&bridge, &desired, Some(target), Some(deadline))
            .await
    }

    /// Complete a foreground materialization with an internal seam after the
    /// lifecycle transaction commits. Production callers use an immediately
    /// ready future; deterministic concurrency tests pause here to exercise
    /// superseding roster passes without adding runtime-visible hooks.
    async fn materialize_with_expected_member_alias_after_inner<F>(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        after_inner: F,
        topology: TopologyAfterEmbody,
        already_active: &mut bool,
    ) -> Result<ContinuityRecord, IdentityRuntimeError>
    where
        F: Future<Output = ()>,
    {
        // `embody_identity` binds this attempt to the generation stored on
        // the IdentityEntry after it acquires the lifecycle lock. Sampling the
        // global generation here is insufficient: a newer reconcile can
        // publish G+1 and win that lock before this future reaches the entry,
        // in which case the materialized spec belongs to G+1, not G.
        let mut bootstrap_generation = None;
        let mut shutdown = self.foreground_cancel.subscribe();
        let result = self
            .embody_identity(
                identity,
                expected_alias,
                Some(&mut shutdown),
                None,
                &mut bootstrap_generation,
                EmbodimentOverrides::default(),
            )
            .await
            .map(|outcome| {
                *already_active = outcome.already_active;
                outcome.record
            });
        after_inner.await;
        // A caller wanted this identity and it is parked Broken: that demand
        // is a typed repair trigger (it would otherwise wait for an unrelated
        // event in a steady deployment).
        if matches!(
            &result,
            Err(IdentityRuntimeError::InvalidState {
                state: IdentityLifecycleState::Broken,
                ..
            })
        ) {
            self.trigger_continuity_repair(ContinuityRepairTrigger::DemandOnBroken);
        }
        if matches!(
            &result,
            Err(IdentityRuntimeError::Internal(message)) if message == BACKGROUND_WARM_CANCELLED
        ) {
            if let Some(generation) = bootstrap_generation {
                self.mark_bootstrap_materialization_cancelled(identity, Some(generation));
            }
            return result;
        }
        // Alias validation happens under the lifecycle lock before
        // `embody_identity` marks bootstrap work as started. A stale alias
        // therefore must leave the replacement generation's exact readiness
        // state untouched.
        if matches!(&result, Err(IdentityRuntimeError::StaleRuntimeAlias { .. })) {
            return result;
        }
        if result.is_ok() && topology == TopologyAfterEmbody::Reconcile {
            let desired_edges = self.desired_peer_edges.read().await.clone();
            let has_pending_topology_recovery = match self.topology_controller() {
                Some(controller) => controller.has_pending().await,
                None => false,
            };
            if (!desired_edges.is_empty() || has_pending_topology_recovery)
                && let Err(error) = self.reconcile_managed_peer_edges(&desired_edges).await
            {
                tracing::warn!(
                    identity = %identity,
                    %error,
                    "identity materialized with topology reconcile warning"
                );
            }
        }
        if let Some(generation) = bootstrap_generation {
            self.mark_bootstrap_materialization_finished(identity, &result, Some(generation));
        }
        result
    }

    /// Cancellation-safe materialization for request/host boundaries.
    pub async fn materialize_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        self.run_tracked_foreground(async move { runtime.materialize(&identity).await })
            .await
    }

    /// Resolve the owner state for each rostered identity from durable
    /// continuity, with NO write target required.
    ///
    /// Layer 1 of the pre-prepare registration path. Separated from publication
    /// on purpose: `register_persisted_continuity_owners` publishes through
    /// `SessionBridge`, which is a LATE indirection - it arrives with the
    /// identity context, after `MobRuntime::prepare` has already run. Meerkat's
    /// durable-tail recovery executes DURING prepare, so anything needing the
    /// bridge is structurally too late for it, and the bridge's absence makes
    /// that method return `Ok(0)`: a silent nothing that logs as a boot with
    /// nothing to register. This function needs only the continuity store, which
    /// the caller holds long before prepare.
    ///
    /// # Strictness depends on the store, deliberately
    ///
    /// On a store with an INCREMENTAL (head-canonical) channel, an unreadable
    /// fact is TERMINAL. A Ready record followed by an error or an empty answer
    /// from the exact session lookup returns `Err`, and the caller must fail this
    /// boot attempt and retry rather than proceed. Skipping there reproduces the
    /// exact defect this path exists to remove: publish nothing, run prepare,
    /// let inner recovery commit, and let the projection hit the backstop - and
    /// that boot is NOT byte-neutral, because the inner commit has already
    /// landed by the time the refusal fires.
    ///
    /// On a blob-only store the older best-effort semantics stand, because there
    /// is no head-canonical projection requiring the cursor.
    ///
    /// Only an EXPLICIT `Uninitialized` skips, on either store shape: a newly
    /// added roster identity legitimately has no prior session and the ordinary
    /// create path owns it. That is the creation path, not a degraded read.
    ///
    /// The other two non-Ready outcomes are NOT the creation path. `Broken` is
    /// terminal on a head-canonical store, because the identity HAS durable
    /// state. A MISSING map entry is terminal on both shapes, because the
    /// `ContinuityStore` contract requires an entry per requested identity and
    /// says a missing one is a provider error rather than an implicit
    /// `Uninitialized`.
    ///
    /// Identity and generation come from the Ready record; the fencing token and
    /// fence-current come from `resolve_record_by_session`. Both sources must
    /// agree on identity AND session AND generation - a mismatch is always
    /// `Err`, on any store, because publishing an authority the two sources
    /// dispute is worse than refusing to boot.
    pub async fn resolve_persisted_owner_states(
        continuity_store: &Arc<dyn super::ContinuityStore>,
        roster: &[DurableAgentSpec],
    ) -> Result<
        Vec<(
            meerkat_core::types::SessionId,
            super::adapters::SessionRuntimeState,
        )>,
        IdentityRuntimeError,
    > {
        // INV-06 FIRST, before any store read or publication. Roster uniqueness
        // used to be validated in the later roster-registration path, which was
        // downstream of everything; hoisting owner registration ahead of prepare
        // also hoisted it ahead of that guard.
        //
        // What actually happens without it is WORSE than a misleading error, and
        // I only learned that by mutating it out: the duplicated roster resolves
        // to Ok([]) - no error at all. The resolver publishes nothing and lets
        // prepare proceed, which is the original defect shape. My first comment
        // here claimed it would surface later as an ownership conflict from a
        // double register_session; the mutation disproved that, so the claim is
        // corrected rather than left as a plausible-sounding rationale.
        //
        // Moving work earlier moves it before the checks that used to protect
        // it. That is the same class as everything else this hoist touched.
        Self::validate_roster_uniqueness(roster)?;
        // The discriminator for strictness. A head-canonical store projects
        // through the cursor, so an owner it cannot report is an owner whose
        // recovery will refuse; a blob-only store has no such projection.
        let head_canonical = continuity_store.as_incremental_sessions().is_some();
        let mut states = Vec::new();
        for spec in roster {
            let identity = &spec.identity;
            let resolved = match continuity_store
                .resolve_many(std::slice::from_ref(identity))
                .await
            {
                Ok(resolved) => resolved,
                Err(error) if head_canonical => {
                    return Err(IdentityRuntimeError::Internal(format!(
                        "continuity resolution for {identity} failed before prepare on a \
                         head-canonical store, so no owner state can be published and recovery \
                         would refuse after an inner commit that is not byte-neutral; failing \
                         this boot attempt instead: {error}"
                    )));
                }
                Err(error) => {
                    tracing::info!(
                        %identity,
                        %error,
                        "continuity resolution unavailable before prepare on a blob-only store; \
                         no owner state for this identity"
                    );
                    continue;
                }
            };
            // THREE distinct outcomes, not two. Collapsing them into one skip
            // treats a Broken identity as one that never existed, which is how a
            // broken continuity head gets silently recreated instead of refused.
            let record = match resolved.get(identity) {
                // The ONLY creation path. resolve_many answered, and its answer
                // is that this identity has never persisted a session.
                Some(super::types::ContinuityResolveState::Uninitialized) => {
                    tracing::info!(
                        %identity,
                        head_canonical,
                        "continuity reports Uninitialized before prepare; ordinary create path \
                         owns this identity"
                    );
                    continue;
                }
                Some(super::types::ContinuityResolveState::Ready { record }) => record,
                // Broken is NOT a creation path: the identity HAS durable state
                // and the store is telling us it cannot be used. Publishing
                // nothing and letting prepare proceed means recovery meets an
                // empty owner map for a session that exists.
                Some(super::types::ContinuityResolveState::Broken { failure }) => {
                    if head_canonical {
                        return Err(IdentityRuntimeError::Internal(format!(
                            "continuity for {identity} is Broken before prepare ({failure:?}), so \
                             no owner state can be published and recovery would meet an empty \
                             owner map for a session that exists; failing this boot attempt"
                        )));
                    }
                    tracing::info!(
                        %identity,
                        ?failure,
                        "continuity Broken before prepare on a blob-only store; no owner state"
                    );
                    continue;
                }
                // A PROVIDER ERROR on every store shape, not a capability
                // difference. The ContinuityStore trait doc is explicit:
                // "resolve_many MUST return an entry for every requested
                // identity. Missing entries are treated as a provider error, not
                // implicit Uninitialized." So this is terminal regardless of
                // head_canonical - a store that violates its own contract has
                // told us nothing about this identity, and inferring
                // Uninitialized from silence is precisely what the contract
                // forbids.
                None => {
                    return Err(IdentityRuntimeError::Internal(format!(
                        "continuity resolution returned no entry for {identity} before prepare, \
                         which its contract forbids: resolve_many must answer for every requested \
                         identity and a missing entry is a provider error, not an implicit \
                         Uninitialized"
                    )));
                }
            };
            let session_id = record.session_id.clone();
            let bound = match continuity_store
                .resolve_record_by_session(&session_id)
                .await
            {
                Ok(bound) => bound,
                Err(error) if head_canonical => {
                    return Err(IdentityRuntimeError::Internal(format!(
                        "continuity fence lookup for {identity} session {session_id} failed \
                         before prepare on a head-canonical store after a Ready record; failing \
                         this boot attempt rather than letting recovery commit and refuse: \
                         {error}"
                    )));
                }
                Err(error) => {
                    tracing::info!(
                        %identity,
                        %session_id,
                        %error,
                        "continuity fence lookup unavailable before prepare on a blob-only \
                         store; no owner state for this session"
                    );
                    continue;
                }
            };
            let Some((bound_record, fencing_token, fence_current)) = bound else {
                if head_canonical {
                    // A Ready record naming a session the substrate will not
                    // bind is not a creation path - the record asserts the
                    // session exists. On a head-canonical store that is the
                    // unreadable-fact case, and it is terminal.
                    return Err(IdentityRuntimeError::Internal(format!(
                        "continuity reports a Ready record for {identity} at session \
                         {session_id} but the substrate binds no fence for it before prepare, \
                         so no owner state can be published on a head-canonical store"
                    )));
                }
                tracing::info!(
                    %identity,
                    %session_id,
                    "no substrate fence binds this session before prepare on a blob-only store; \
                     no owner state"
                );
                continue;
            };
            // Exact agreement on all three, on EITHER store shape. The
            // pre-activation pass checked identity and session; generation is
            // added because the published state CARRIES a generation, and a
            // state whose generation came from one source while its fence came
            // from another is an authority neither source asserted.
            if bound_record.session_id != session_id
                || bound_record.identity != *identity
                || bound_record.generation != record.generation
            {
                return Err(IdentityRuntimeError::Internal(format!(
                    "continuity for {identity} resolves to session {session_id} generation {:?} \
                     but the substrate binds that session to {}/{}/{:?}, so no owner state can \
                     be resolved before prepare",
                    record.generation,
                    bound_record.identity,
                    bound_record.session_id,
                    bound_record.generation
                )));
            }
            states.push((
                session_id,
                super::adapters::SessionRuntimeState {
                    identity: identity.clone(),
                    generation: bound_record.generation,
                    fencing_token,
                    checkpoint_version: fence_current,
                },
            ));
        }
        Ok(states)
    }

    /// Publish resolved persisted owner authority into the session store
    /// BEFORE the mob is prepared.
    ///
    /// This exists for an ordering problem that `register_persisted_continuity_owners`
    /// cannot solve, because it runs too late. Durable-tail recovery happens
    /// INSIDE `MobRuntime::prepare` and commits runtime boundaries; a
    /// head-canonical session whose owning identity has no authoritative
    /// registration is refused there BY DESIGN ("this is a deliberate refusal,
    /// not a store failure: no retry of the write will succeed"). Registration
    /// after prepare is therefore unreachable for exactly the stores that need
    /// it: a resume whose durable tail is behind its committed authority.
    /// Verified against a production clone, whose first boot dies precisely
    /// there.
    ///
    /// Idempotent by construction, which is why publishing here and registering
    /// again later is safe: `register_session` refuses an identity or generation
    /// change and a fencing regression, so a re-publish of the same facts is a
    /// no-op rather than a second authority.
    ///
    /// Returns how many owners were published, so a caller can log a boot that
    /// published none rather than leaving an absent effect indistinguishable
    /// from an unconfigured one.
    pub async fn publish_persisted_owner_states(
        adapter: &Arc<super::adapters::ContinuitySessionStoreAdapter>,
        states: &[(
            meerkat_core::types::SessionId,
            super::adapters::SessionRuntimeState,
        )],
    ) -> Result<usize, IdentityRuntimeError> {
        let mut published = 0usize;
        for (session_id, state) in states {
            adapter
                .register_session(session_id, state.clone())
                .await
                .map_err(|error| {
                    IdentityRuntimeError::Internal(format!(
                        "publishing the persisted owner of {} session {session_id} before \
                         prepare: {error}",
                        state.identity
                    ))
                })?;
            published += 1;
        }
        tracing::info!(
            published,
            resolved = states.len(),
            "published persisted continuity owners before mob prepare"
        );
        Ok(published)
    }

    /// Register the owning identity of every persisted session on the roster,
    /// without materializing anything.
    ///
    /// This exists for exactly one ordering problem. Lifting a resumed mob from
    /// `Stopped` to `Running` makes MobMachine revive its persisted sessions,
    /// and a revived session whose owner has no authoritative registration is
    /// correctly refused by the continuity adapter - so every restored member
    /// comes back Broken. `embody_identity` already registers from the
    /// authoritative record, but it registers and then SPAWNS, and spawning
    /// needs a mob that is already Running. Hence a registration-only pass that
    /// can run while the mob is still Stopped.
    ///
    /// The facts come from the continuity substrate's own
    /// `resolve_record_by_session`, whose contract is that it returns "the same
    /// facts registration would carry": the record, its fencing token, and the
    /// substrate's fence-current version. Nothing is minted here, and this runs
    /// inside the lifecycle owner rather than seeding durable identity state
    /// from outside it.
    ///
    /// Best-effort per identity BY DESIGN. An identity with no durable record
    /// has nothing to register and is not an error - it has never persisted a
    /// session, so the ordinary create path owns it.
    ///
    /// Returns the sessions whose owners were registered, in roster order, so a
    /// caller can log a boot that registered none AND reuse this exact set
    /// without resolving continuity a second time. The set is deliberately the
    /// registration's own output rather than a re-read: a second resolve could
    /// disagree with what was actually published here, and a caller acting on
    /// the disagreeing half would be operating on an authority nothing
    /// registered.
    pub async fn register_persisted_continuity_owners(
        &self,
        roster: &[DurableAgentSpec],
    ) -> Result<Vec<meerkat_core::types::SessionId>, IdentityRuntimeError> {
        let Some(bridge) = self.bridge.as_ref() else {
            return Ok(Vec::new());
        };
        let mut registered: Vec<meerkat_core::types::SessionId> = Vec::new();
        // Instrumented per identity: on 2026-09-03 a production fleet's registration
        // went from 49s to 170s for 17 identities with this function byte-identical,
        // and this loop logged nothing, so nobody could say which of its three awaits
        // had slowed. One INFO per identity with the three step durations, and one
        // summary, so the next such regression names its step on the first launch.
        let loop_started = std::time::Instant::now();
        // Register every roster identity's customizer-tool dispatcher before
        // the mob is lifted, so meerkat's restart restore attaches it (#563).
        if let Some(registry) = self.customizer_tools.read().await.clone() {
            for spec in roster {
                registry.ensure(&spec.identity);
            }
        }
        for spec in roster {
            let identity = &spec.identity;
            let identity_started = std::time::Instant::now();
            // A substrate that cannot answer is not a boot failure. This pass
            // only ever ADDS a registration that would otherwise happen later;
            // when the facts are unavailable the pre-existing behaviour stands,
            // including the downstream registration-required refusal. Failing
            // the boot here would make an optional capability mandatory.
            let resolve_started = std::time::Instant::now();
            let resolved = match self
                .continuity_store
                .resolve_many(std::slice::from_ref(identity))
                .await
            {
                Ok(resolved) => resolved,
                Err(error) => {
                    tracing::warn!(
                        %identity,
                        %error,
                        "continuity resolution unavailable before activation; skipping owner \
                         pre-registration for this identity"
                    );
                    continue;
                }
            };
            let Some(super::types::ContinuityResolveState::Ready { record }) =
                resolved.get(identity)
            else {
                continue;
            };
            let resolve_ms = resolve_started.elapsed().as_millis() as u64;
            let session_id = record.session_id.clone();
            // The substrate is the authority for the fence, not the record: the
            // record's own checkpoint version is a checkpoint-time stamp and may
            // trail the substrate's write counter, and registering a trailing
            // version makes the next write present a stale one.
            // Explicitly optional: the trait documents `None` both for "no
            // record binds this session" AND for "the substrate does not
            // support the lookup", with callers told to keep their
            // registration-required refusal. A substrate without the fence
            // table returns an ERROR rather than None, which is the same
            // condition, so treat it the same way instead of failing the boot.
            let continuity_started = std::time::Instant::now();
            let bound = match self
                .continuity_store
                .resolve_record_by_session(&session_id)
                .await
            {
                Ok(bound) => bound,
                Err(error) => {
                    tracing::warn!(
                        %identity,
                        %session_id,
                        %error,
                        "continuity fence lookup unavailable before activation; skipping owner \
                         pre-registration for this session"
                    );
                    continue;
                }
            };
            let Some((bound_record, fencing_token, fence_current)) = bound else {
                continue;
            };
            if bound_record.session_id != session_id || bound_record.identity != *identity {
                // The substrate binds this session to something other than what
                // the roster resolution said. Registering either spelling would
                // publish an authority neither source agrees on.
                return Err(IdentityRuntimeError::Internal(format!(
                    "continuity for {identity} resolves to session {session_id} but the substrate \
                     binds that session to {}/{}, so no owner registration can be published \
                     before activation",
                    bound_record.identity, bound_record.session_id
                )));
            }
            let continuity_ms = continuity_started.elapsed().as_millis() as u64;
            let publish_started = std::time::Instant::now();
            bridge
                .register_session_runtime_state(
                    &session_id,
                    identity,
                    bound_record.generation,
                    fence_current,
                    fencing_token,
                )
                .await
                .map_err(|error| {
                    IdentityRuntimeError::Internal(format!(
                        "registering the persisted owner of {identity} session {session_id} \
                         before activation: {error}"
                    ))
                })?;
            tracing::info!(
                identity = %identity,
                session_id = %session_id,
                resolve_ms,
                continuity_ms,
                publish_ms = publish_started.elapsed().as_millis() as u64,
                total_ms = identity_started.elapsed().as_millis() as u64,
                "persisted continuity owner registered"
            );
            registered.push(session_id);
        }
        tracing::info!(
            registered = registered.len(),
            roster = roster.len(),
            elapsed_ms = loop_started.elapsed().as_millis() as u64,
            "persisted continuity owners registered"
        );
        Ok(registered)
    }

    /// Discharge the attach postcondition, converting an attached occupant into
    /// a genuinely resumed one.
    ///
    /// A successful ATTACH is not yet a resume. The occupant must carry an
    /// authoritative owning-identity registration before anything can commit a
    /// runtime boundary, and only the continuity record holds the generation,
    /// checkpoint version and fencing token that registration needs - which is
    /// why the bridge cannot discharge this itself and reports
    /// `AttachedPendingRegistration` instead of guessing.
    ///
    /// This is the ONE place that converts that variant into `Resumed`, because
    /// forgetting the postcondition does not fail here: it surfaces far away, as
    /// a head-canonical session refused for an unregistered owner during an
    /// unrelated boundary commit. A caller that reimplements this can omit the
    /// registration and still compile, still pass, and still break a later boot.
    ///
    /// Registration failure is retryable by construction: the occupant is
    /// untouched, nothing is retired or replaced, and no `Resumed` is reported.
    async fn complete_attach_postcondition(
        &self,
        outcome: super::bridge::ResumeSessionOutcome,
        identity: &AgentIdentity,
        record: &ContinuityRecord,
        fencing_token: FencingToken,
    ) -> Result<super::bridge::ResumeSessionOutcome, IdentityRuntimeError> {
        let super::bridge::ResumeSessionOutcome::AttachedPendingRegistration { session_id } =
            outcome
        else {
            return Ok(outcome);
        };
        // The record's generation, checkpoint version and fence are being
        // published as the authority for THIS session. If the bridge attached a
        // different one, registering would bind this identity's fence to a
        // session its record does not name, so refuse instead.
        if session_id != record.session_id {
            return Err(IdentityRuntimeError::Internal(format!(
                "attach reported session {session_id} but the continuity record for {identity} \
                 binds {}, so registering this record's authority against it would publish a \
                 binding the record does not name",
                record.session_id
            )));
        }
        if let Some(bridge) = self.bridge.as_ref() {
            bridge
                .register_session_runtime_state(
                    &session_id,
                    identity,
                    record.generation,
                    record.checkpoint_version,
                    fencing_token,
                )
                .await
                .map_err(|err| {
                    IdentityRuntimeError::Internal(format!(
                        "owner registration after attach of {session_id} failed, so the session \
                         is not resumable yet (retryable: the occupant is untouched and retry is \
                         idempotent): {err}"
                    ))
                })?;
        }
        Ok(super::bridge::ResumeSessionOutcome::Resumed { session_id })
    }

    /// Cancellation-safe compatibility restore used by embedders that pass
    /// an RPC identity context without attaching an
    /// [`IdentityFirstRuntimeContext`] to the unified runtime.
    pub(crate) async fn restore_flow_tracked(
        self: &Arc<Self>,
        roster: Vec<DurableAgentSpec>,
        topology_provider: Option<Arc<dyn TopologyProvider>>,
        customizer: Option<Arc<dyn AgentCustomizer>>,
    ) -> Result<super::orchestrator::RestoreFlowResult, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        self.run_tracked_foreground(async move {
            super::orchestrator::restore_flow(
                runtime.as_ref(),
                &roster,
                topology_provider.as_deref(),
                customizer.as_deref(),
            )
            .await
        })
        .await
    }

    /// Cancellation-safe retirement for RPC/host request boundaries.
    pub async fn retire_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        self.run_tracked_foreground(async move { runtime.retire(&identity).await })
            .await
    }

    /// Cancellation-safe retirement that atomically rejects an old generated
    /// runtime alias under the same lifecycle lock as the mutation.
    pub async fn retire_member_alias_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: &str,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.to_string();
        self.run_tracked_foreground(async move {
            runtime
                .retire_with_expected_member_alias(&identity, Some(&expected_alias))
                .await
        })
        .await
    }

    /// Retire identity authority and its captured lower-plane generations
    /// under one lifecycle lock. Enumeration performed by `cleanup` therefore
    /// observes the generation actually retired, even when this request had
    /// waited behind a concurrent reset.
    pub(crate) async fn retire_and_cleanup_live_members_tracked<T, F, Fut>(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        cleanup: F,
    ) -> Result<(FencingToken, T), IdentityRuntimeError>
    where
        T: Send + 'static,
        F: FnOnce(Option<AgentRuntimeId>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.map(str::to_owned);
        self.run_tracked_foreground(async move {
            let lifecycle_lock = runtime.lifecycle_lock_for(&identity).await;
            let _lifecycle_guard = lifecycle_lock.lock().await;
            if let Some(expected_alias) = expected_alias.as_deref() {
                runtime
                    .ensure_expected_member_alias_current(&identity, expected_alias)
                    .await?;
            }
            let retired_alias = runtime
                .entries
                .read()
                .await
                .get(&identity)
                .and_then(|entry| entry.continuity.as_ref())
                .map(|record| record.agent_runtime_id.clone());
            let token = runtime.retire_locked(&identity).await?;
            let metadata = cleanup(retired_alias).await;
            Ok((token, metadata))
        })
        .await
    }

    /// Cancellation-safe respawn for RPC/host request boundaries.
    pub async fn respawn_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        self.run_tracked_foreground(async move { runtime.respawn(&identity).await })
            .await
    }

    /// Cancellation-safe respawn that atomically rejects an old generated
    /// runtime alias under the same lifecycle lock as the mutation.
    pub async fn respawn_member_alias_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: &str,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.to_string();
        self.run_tracked_foreground(async move {
            runtime
                .respawn_with_expected_member_alias(&identity, Some(&expected_alias))
                .await
        })
        .await
    }

    /// Execute a lower member-plane mutation while pinning a generated alias
    /// to the durable identity generation that owns it. This is the common
    /// authority boundary for legacy member RPCs (for example force-cancel)
    /// that cannot otherwise express identity continuity.
    pub(crate) async fn run_member_alias_operation_tracked<T, F, Fut>(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: &str,
        operation: F,
    ) -> Result<T, IdentityRuntimeError>
    where
        T: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, String>> + Send + 'static,
    {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.to_string();
        self.run_tracked_foreground(async move {
            let lifecycle_lock = runtime.lifecycle_lock_for(&identity).await;
            let _lifecycle_guard = lifecycle_lock.lock().await;
            runtime
                .ensure_expected_member_alias_current(&identity, &expected_alias)
                .await?;
            operation().await.map_err(IdentityRuntimeError::Internal)
        })
        .await
    }

    /// Cancellation-safe identity-first respawn for RPC and console boundaries.
    ///
    /// A durable identity already owns an authoritative Meerkat session. Its
    /// respawn operation therefore fences the old owner, refreshes the persisted
    /// runtime state, and reactivates that exact continuity record in place. It
    /// must not call the raw member-plane respawn convenience: that operation
    /// creates a new session and would silently turn a non-destructive identity
    /// recovery into a destructive reset.
    pub(crate) async fn respawn_identity_in_place_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        match expected_alias {
            Some(expected_alias) => {
                self.respawn_member_alias_tracked(identity, expected_alias)
                    .await
            }
            None => self.respawn_tracked(identity).await,
        }
    }

    /// Cancellation-safe live-session rebind for RPC/host boundaries.
    pub async fn rebind_session_after_live_respawn_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        session_id: SessionId,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        self.run_tracked_foreground(async move {
            runtime
                .rebind_session_after_live_respawn(&identity, session_id)
                .await
        })
        .await
    }

    /// Cancellation-safe live-session rebind pinned to the generation that
    /// initiated the lower-level member respawn.
    pub async fn rebind_session_after_live_respawn_member_alias_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: &str,
        session_id: SessionId,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.to_string();
        self.run_tracked_foreground(async move {
            runtime
                .rebind_session_after_live_respawn_with_expected_member_alias(
                    &identity,
                    Some(&expected_alias),
                    session_id,
                )
                .await
        })
        .await
    }

    /// Cancellation-safe destructive reset for RPC/host boundaries.
    pub async fn reset_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        self.run_tracked_foreground(async move { runtime.reset(&identity).await })
            .await
    }

    /// Cancellation-safe reset that atomically rejects an old generated
    /// runtime alias under the same lifecycle lock as the mutation.
    pub async fn reset_member_alias_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: &str,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.to_string();
        self.run_tracked_foreground(async move {
            runtime
                .reset_with_expected_member_alias(&identity, Some(&expected_alias))
                .await
        })
        .await
    }

    /// Cancellation-safe identity deletion for RPC/host boundaries.
    pub async fn delete_identity_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
    ) -> Result<(), IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        self.run_tracked_foreground(async move { runtime.delete_identity(&identity).await })
            .await
    }

    /// Cancellation-safe deletion that atomically rejects an old generated
    /// runtime alias under the same lifecycle lock as the mutation.
    pub async fn delete_identity_member_alias_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: &str,
    ) -> Result<(), IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.to_string();
        self.run_tracked_foreground(async move {
            runtime
                .delete_identity_with_expected_member_alias(&identity, Some(&expected_alias))
                .await
        })
        .await
    }

    /// Delete identity authority and clean its captured concrete generations
    /// in the same lifecycle transaction.
    pub(crate) async fn delete_identity_and_cleanup_live_members_tracked<T, F, Fut>(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        cleanup: F,
    ) -> Result<T, IdentityRuntimeError>
    where
        T: Send + 'static,
        F: FnOnce(Option<AgentRuntimeId>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.map(str::to_owned);
        self.run_tracked_foreground(async move {
            let lifecycle_lock = runtime.lifecycle_lock_for(&identity).await;
            let _lifecycle_guard = lifecycle_lock.lock().await;
            if let Some(expected_alias) = expected_alias.as_deref() {
                runtime
                    .ensure_expected_member_alias_current(&identity, expected_alias)
                    .await?;
            }
            let deleted_alias = runtime
                .entries
                .read()
                .await
                .get(&identity)
                .and_then(|entry| entry.continuity.as_ref())
                .map(|record| record.agent_runtime_id.clone());
            runtime.delete_identity_locked(&identity).await?;
            Ok(cleanup(deleted_alias).await)
        })
        .await
    }

    /// Background warming observes controller cancellation after lease
    /// acquisition bookkeeping but before bridge/member installation. A
    /// cancelled grant is explicitly released; after customization completes,
    /// the operation is joined to its explicit commit/rollback boundary.
    async fn materialize_for_background(
        &self,
        identity: &AgentIdentity,
        cancellation: &mut watch::Receiver<bool>,
        generation: u64,
    ) -> Option<Result<ContinuityRecord, IdentityRuntimeError>> {
        // Background warm is indirect hydration: a retired identity stays
        // retired until something addressed to it revives it.
        if let Some(err) = self.retired_terminal_refusal(identity, "materialize").await {
            return Some(Err(err));
        }
        let mut bound_generation = None;
        let result = self
            .embody_identity(
                identity,
                None,
                Some(cancellation),
                Some(generation),
                &mut bound_generation,
                EmbodimentOverrides::default(),
            )
            .await
            .map(|outcome| outcome.record);
        if matches!(
            &result,
            Err(IdentityRuntimeError::Internal(message)) if message == BACKGROUND_WARM_CANCELLED
        ) {
            self.mark_bootstrap_materialization_cancelled(identity, Some(generation));
            return None;
        }
        if result.is_ok() {
            let desired_edges = self.desired_peer_edges.read().await.clone();
            if !desired_edges.is_empty()
                && let Err(error) = self.reconcile_managed_peer_edges(&desired_edges).await
            {
                tracing::warn!(
                    identity = %identity,
                    %error,
                    "background identity materialized with topology reconcile warning"
                );
            }
        }
        self.mark_bootstrap_materialization_finished(identity, &result, Some(generation));
        Some(result)
    }

    pub(crate) async fn embody_identity(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        cancellation: Option<&mut watch::Receiver<bool>>,
        expected_bootstrap_generation: Option<u64>,
        bound_bootstrap_generation: &mut Option<u64>,
        overrides: EmbodimentOverrides<'_>,
    ) -> Result<EmbodimentOutcome, IdentityRuntimeError> {
        let lifecycle_lock = self.lifecycle_lock_for(identity).await;
        let _lifecycle_guard = lifecycle_lock.lock().await;
        self.embody_identity_locked(
            identity,
            expected_alias,
            cancellation,
            expected_bootstrap_generation,
            bound_bootstrap_generation,
            overrides,
        )
        .await
    }

    /// [`Self::embody_identity`] for a caller that ALREADY holds this
    /// identity's lifecycle lock (the reload verb runs retire and
    /// re-materialize under one lock so no send can interleave). The
    /// per-identity tokio mutex is not reentrant, so the body is split rather
    /// than re-acquired.
    async fn embody_identity_locked(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        cancellation: Option<&mut watch::Receiver<bool>>,
        expected_bootstrap_generation: Option<u64>,
        bound_bootstrap_generation: &mut Option<u64>,
        overrides: EmbodimentOverrides<'_>,
    ) -> Result<EmbodimentOutcome, IdentityRuntimeError> {
        let was_active = self
            .entries
            .read()
            .await
            .get(identity)
            .is_some_and(|entry| entry.state == IdentityLifecycleState::Active);
        let _in_flight = EmbodimentInFlight::enter(&self.embodiments_in_flight);
        let result = self
            .embody_identity_transaction(
                identity,
                expected_alias,
                cancellation,
                expected_bootstrap_generation,
                bound_bootstrap_generation,
                overrides,
            )
            .await;
        // A genuine embodiment (a resume, or a fresh mint) committed and
        // released the store writer it held: a typed repair trigger. A
        // converged Active identity re-validated by a pass is not one, so a
        // repair pass cannot wake itself through its healthy members.
        if result.is_ok() && !was_active {
            self.trigger_continuity_repair(ContinuityRepairTrigger::EmbodimentSettled);
        }
        result
    }

    async fn embody_identity_transaction(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        mut cancellation: Option<&mut watch::Receiver<bool>>,
        expected_bootstrap_generation: Option<u64>,
        bound_bootstrap_generation: &mut Option<u64>,
        overrides: EmbodimentOverrides<'_>,
    ) -> Result<EmbodimentOutcome, IdentityRuntimeError> {
        let bootstrap_generation = {
            let entries = self.entries.read().await;
            entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?
                .bootstrap_generation
        };
        *bound_bootstrap_generation = Some(bootstrap_generation);
        // A background item belongs to one controller generation. If a newer
        // roster pass converged the entry before this item acquired lifecycle
        // authority, treat the old work as cancelled instead of materializing
        // the newer spec on behalf of a superseded pass.
        if expected_bootstrap_generation.is_some_and(|expected| expected != bootstrap_generation) {
            return Err(IdentityRuntimeError::Internal(
                BACKGROUND_WARM_CANCELLED.to_string(),
            ));
        }
        let raw_alias_lock = self.raw_member_alias_lock(identity.as_str()).await;
        let _raw_alias_guard = raw_alias_lock.lock().await;
        self.ensure_raw_member_alias_available(identity).await?;
        if let Some(expected_alias) = expected_alias {
            self.ensure_expected_member_alias_current(identity, expected_alias)
                .await?;
        }
        let lock = self.materialization_lock_for(identity).await;
        let _guard = lock.lock().await;

        let (spec, continuity, state) = {
            let entries = self.entries.read().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            (
                overrides
                    .spec
                    .cloned()
                    .unwrap_or_else(|| entry.spec.clone()),
                entry.continuity.clone(),
                entry.state,
            )
        };
        if state == IdentityLifecycleState::Active {
            // Converged eager restore still validates the time-sensitive
            // external lease. This is part of the shared embodiment door, not
            // a second restore implementation: healthy authority is reused,
            // due authority is renewed, and lost authority parks this member.
            let record = self.reuse_active_restore_state(&spec).await?;
            self.clear_materialization_backoff(identity).await;
            return Ok(EmbodimentOutcome {
                already_active: true,
                record,
                resumed: true,
                draft: AgentBuildDraft {
                    model: None,
                    system_prompt: None,
                    additional_instructions: spec.additional_instructions.clone(),
                    labels: spec.labels.clone(),
                    app_context: spec.context.clone(),
                    external_tools: Vec::new(),
                    local_external_tools: Default::default(),
                    provider_params: None,
                    compaction_curator: Default::default(),
                },
            });
        }
        // Host-rejected-build park: the app-side gate answered this exact
        // spec with a deterministic rejection. Fail fast typed — every
        // attempt would otherwise burn a full member build plus a callback
        // round trip for the same answer. A spec change clears the park
        // (checked inside the accessor).
        if let Some(park) = self.host_rejected_build_park(identity).await {
            return Err(IdentityRuntimeError::HostRejectedBuild {
                identity: identity.clone(),
                reason: park.reason,
            });
        }
        let original_continuity = continuity.clone();
        let continuity = if durable_spec_uses_external_binding(&spec) {
            None
        } else {
            continuity
        };

        match state {
            // `Retiring` is the retired terminal form (see `retire_locked`): no
            // live member, lease released, continuity record intact. The
            // lifecycle lock held here means the in-transaction form cannot be
            // observed, so re-materializing it is exactly the Dormant path and
            // resumes the SAME durable session and generation.
            IdentityLifecycleState::Dormant
            | IdentityLifecycleState::Uninitialized
            | IdentityLifecycleState::Retiring => {}
            IdentityLifecycleState::Broken | IdentityLifecycleState::Suspended => {
                return Err(IdentityRuntimeError::InvalidState {
                    identity: identity.clone(),
                    state,
                    operation: "materialize",
                });
            }
            IdentityLifecycleState::Active => unreachable!("active handled above"),
        }
        // Begin readiness bookkeeping only after ownership validation and
        // only for an identity that will actually materialize. An old alias
        // can now fail without a transient or terminal mutation of the
        // replacement generation's bootstrap entry.
        self.mark_bootstrap_materialization_started(identity, Some(bootstrap_generation));

        // A previous customization/installation failure can leave its exact
        // unactivated grant parked when provider cleanup itself fails. Strict
        // providers reject reacquisition while that grant still exists, so a
        // direct lazy retry must drain this identity's orphan before acquiring
        // fresh authority. Other identities' cleanup failures remain isolated.
        self.release_parked_unactivated_leases_for_identity(identity)
            .await?;

        // Establish single-embodiment ownership before invoking arbitrary
        // host customizer code, preserving the public ordering contract. No
        // bridge/member state exists yet, so every exit below can still
        // release this uninstalled grant explicitly.
        let lease_results = self
            .lease_provider
            .acquire_leases(std::slice::from_ref(identity), &self.runtime_instance_id)
            .await
            .map_err(IdentityRuntimeError::Lease)?;
        let grant = match lease_results.get(identity) {
            Some(super::types::LeaseAcquireResult::Acquired(grant)) => grant.clone(),
            Some(super::types::LeaseAcquireResult::AlreadyHeld { holder, .. }) => {
                tracing::error!(
                    %identity,
                    holder = %holder,
                    "single-embodiment guard: refusing to materialize an identity whose \
                     durable lease is held by another live runtime instance"
                );
                return Err(IdentityRuntimeError::AlreadyEmbodied {
                    identity: identity.clone(),
                    holder: holder.clone(),
                });
            }
            None => return Err(IdentityRuntimeError::NoActiveLease(identity.clone())),
        };
        if let Some(record) = continuity.as_ref()
            && let Err(err) = self
                .continuity_store
                .upsert_continuity_record(record, grant.fencing_token)
                .await
        {
            let cleanup_error = self.release_uninstalled_materialize_lease(&grant).await;
            return Err(IdentityRuntimeError::Internal(format!(
                "continuity upsert before materialize: {err}{}",
                cleanup_error
                    .as_ref()
                    .map(|e| format!("; lease cleanup failed: {e}"))
                    .unwrap_or_default(),
            )));
        }

        if cancellation
            .as_ref()
            .is_some_and(|cancellation| *cancellation.borrow())
        {
            return Err(self
                .cancel_uninstalled_background_materialization(&grant)
                .await);
        }

        let active_peers = self.entries.read().await.keys().cloned().collect();
        let managed_edges = self.desired_peer_edges.read().await.clone();
        let build_context = AgentBuildContext {
            identity: identity.clone(),
            active_peers,
            managed_edges,
            runtime_services: self.runtime_services(),
        };
        let mut draft = AgentBuildDraft {
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
        let installed_customizer = self.customizer.read().await.clone();
        let mut customized = false;
        if let Some(customizer) = overrides.customizer.or(installed_customizer.as_deref()) {
            let customize = customizer.customize_build(&build_context, &spec, &mut draft);
            tokio::pin!(customize);
            let customize_result = if let Some(cancellation) = cancellation.as_mut() {
                let cancellation = &mut **cancellation;
                tokio::select! {
                    result = &mut customize => result,
                    changed = cancellation.changed() => {
                        if changed.is_ok() && *cancellation.borrow() {
                            return Err(
                                self.cancel_uninstalled_background_materialization(&grant)
                                    .await,
                            );
                        }
                        customize.await
                    }
                }
            } else {
                customize.await
            };
            if let Err(err) = customize_result {
                let cleanup_error = self.release_uninstalled_materialize_lease(&grant).await;
                return Err(IdentityRuntimeError::Internal(format!(
                    "customizer: {err}{}",
                    cleanup_error
                        .as_ref()
                        .map(|e| format!("; lease cleanup failed: {e}"))
                        .unwrap_or_default(),
                )));
            }
            customized = true;
        }
        if customized {
            self.publish_customizer_tools(identity, &mut draft).await;
        }

        #[cfg(feature = "extension-state")]
        let extension_publication = match self
            .bridge
            .as_ref()
            .map(|bridge| bridge.begin_extension_identity_publication(identity))
            .transpose()
        {
            Ok(publication) => publication.flatten(),
            Err(error) => {
                let cleanup = self.release_uninstalled_materialize_lease(&grant).await;
                return Err(IdentityRuntimeError::Internal(format!(
                    "identity publication preparation: {error}{}",
                    cleanup
                        .map(|error| format!("; lease cleanup failed: {error}"))
                        .unwrap_or_default()
                )));
            }
        };

        let mut abandoned_session_registrations: Vec<SessionId> = Vec::new();
        let mut resumed = false;
        let mut record = if let Some(mut record) = continuity {
            let resume_snapshot = if self
                .bridge
                .as_ref()
                .is_none_or(|bridge| bridge.requires_resume_snapshot())
            {
                match self
                    .continuity_store
                    .load_session_snapshot(&record.session_id)
                    .await
                {
                    Ok(snapshot) => snapshot,
                    Err(err) => {
                        let cleanup_error =
                            self.release_uninstalled_materialize_lease(&grant).await;
                        return Err(IdentityRuntimeError::Internal(format!(
                            "load session snapshot before materialize: {err}{}",
                            cleanup_error
                                .as_ref()
                                .map(|e| format!("; lease cleanup failed: {e}"))
                                .unwrap_or_default(),
                        )));
                    }
                }
            } else {
                None
            };

            if let Some(bridge) = self.bridge.as_ref() {
                if let Err(err) = bridge
                    .register_session_runtime_state(
                        &record.session_id,
                        identity,
                        record.generation,
                        record.checkpoint_version,
                        grant.fencing_token,
                    )
                    .await
                {
                    let unregister_error = Self::unregister_bridge_session_runtime_states(
                        bridge.as_ref(),
                        std::slice::from_ref(&record.session_id),
                    )
                    .await;
                    let cleanup_error = self.release_uninstalled_materialize_lease(&grant).await;
                    return Err(IdentityRuntimeError::Internal(format!(
                        "bridge register_session_runtime_state: {err}{}{}",
                        unregister_error
                            .as_ref()
                            .map(|e| format!("; unregister session failed: {e}"))
                            .unwrap_or_default(),
                        cleanup_error
                            .as_ref()
                            .map(|e| format!("; lease cleanup failed: {e}"))
                            .unwrap_or_default(),
                    )));
                }
                let registered_session_id = record.session_id.clone();
                let empty_snapshot = SessionSnapshot { data: Vec::new() };
                let snapshot = resume_snapshot.as_ref().unwrap_or(&empty_snapshot);
                let outcome = bridge
                    .resume_session(
                        identity,
                        &record.agent_runtime_id,
                        &spec,
                        &draft,
                        &record.session_id,
                        snapshot,
                    )
                    .await;
                // An attach that reconciled custody is NOT yet resumed. Register
                // the owning identity from THIS record - the only authoritative
                // source of the generation, checkpoint version and fencing token
                // - before anything can commit a runtime boundary. Without it a
                // head-canonical session reaches the continuity adapter with an
                // unregistered owner and is correctly refused, far from here.
                //
                // Registration failure leaves a retryable pending state: no
                // retire, no replacement, and never a Resumed report.
                let outcome = match outcome {
                    Ok(
                        outcome
                        @ super::bridge::ResumeSessionOutcome::AttachedPendingRegistration {
                            ..
                        },
                    ) => {
                        self.complete_attach_postcondition(
                            outcome,
                            identity,
                            &record,
                            grant.fencing_token,
                        )
                        .await?
                    }
                    Ok(outcome) => outcome,
                    Err(err) => {
                        // A rejected resume must NEVER abandon the durable
                        // session (the transcript is the only copy). Keep the
                        // identity → session binding intact — no retire, no
                        // continuity rebind — mark the identity Broken with
                        // the error attached so this send fails loudly, and
                        // let the next reconcile retry the resume. Unregister
                        // only the session-runtime-state bookkeeping (it is
                        // re-registered on retry).
                        tracing::error!(
                            %identity,
                            session_id = %registered_session_id,
                            error = %err,
                            "materialize resume rejected; marking identity Broken and preserving \
                             the durable session for reconcile retry"
                        );
                        let unregister_error = Self::unregister_bridge_session_runtime_states(
                            bridge.as_ref(),
                            std::slice::from_ref(&registered_session_id),
                        )
                        .await;
                        let lease_cleanup_error =
                            self.release_uninstalled_materialize_lease(&grant).await;
                        {
                            let mut entries = self.entries.write().await;
                            if let Some(entry) = entries.get_mut(identity) {
                                entry.state = IdentityLifecycleState::Broken;
                                entry.lease = None;
                            }
                        }
                        self.emit_event(
                            identity,
                            IdentityEvent::StateChanged {
                                identity: identity.clone(),
                                new_state: IdentityLifecycleState::Broken,
                            },
                        )
                        .await;
                        if let Some(reason) = deterministic_build_rejection_reason(&err) {
                            self.mark_host_rejected_build_park(identity, reason).await;
                        }
                        // Repair honesty (production rehearsal): the typed
                        // ArchivedNotRevivable refusal is a stable,
                        // deterministic wall - record the terminal verdict on
                        // this FIRST refusal so the repair supervisor parks
                        // instead of heal-looping (the roster heal succeeds,
                        // this materialize precondition never does).
                        if let BridgeError::ResumeRejected {
                            kind: ResumeRejectionKind::ArchivedNotRevivable,
                            detail,
                        } = &err
                            && !self
                                .mark_continuity_unrecoverable(
                                    identity,
                                    archived_not_revivable_park_reason(
                                        &registered_session_id,
                                        detail,
                                    ),
                                )
                                .await
                        {
                            tracing::debug!(
                                %identity,
                                "identity left Broken before the archived-not-revivable \
                                 park could be recorded"
                            );
                        }
                        // The audited-endpoint divergence is the same shape
                        // of wall with a different exit: the operator's
                        // sanctioned repair, then reload_member. Record the
                        // typed hold with the exact commands on this FIRST
                        // refusal so nothing retries against it.
                        if let BridgeError::ResumeRejected {
                            kind: ResumeRejectionKind::AuditedEndpointDivergence,
                            detail,
                        } = &err
                            && !self
                                .mark_session_repair_required(
                                    identity,
                                    registered_session_id.clone(),
                                    detail.clone(),
                                )
                                .await
                        {
                            tracing::debug!(
                                %identity,
                                "identity left Broken before the session repair hold could \
                                 be recorded"
                            );
                        }
                        let detail = format!(
                            "bridge resume_session rejected (identity degraded, durable session \
                             preserved for reconcile retry): {err}{}{}",
                            unregister_error
                                .as_ref()
                                .map(|e| format!("; unregister session failed: {e}"))
                                .unwrap_or_default(),
                            lease_cleanup_error
                                .as_ref()
                                .map(|e| format!("; lease cleanup failed: {e}"))
                                .unwrap_or_default(),
                        );
                        let kind = match &err {
                            BridgeError::ResumeRejected {
                                kind: ResumeRejectionKind::ArchivedNotRevivable,
                                ..
                            } => ContinuityFailureKind::CheckpointUnrecoverable,
                            BridgeError::ResumeRejected {
                                kind: ResumeRejectionKind::AuditedEndpointDivergence,
                                ..
                            } => ContinuityFailureKind::RepairRequired,
                            BridgeError::ResumeRejected {
                                kind: ResumeRejectionKind::RoleMigrationNotApplied,
                                ..
                            } => ContinuityFailureKind::RoleMigrationNotApplied,
                            _ => ContinuityFailureKind::ResumeRejected,
                        };
                        return Err(IdentityRuntimeError::EmbodimentRejected(Box::new(
                            ContinuityFailure {
                                identity: identity.clone(),
                                kind,
                                record: Some(record.clone()),
                                detail,
                            },
                        )));
                    }
                };
                resumed = outcome.fallback_reason().is_none();
                if let Some(reason) = outcome.fallback_reason().cloned() {
                    tracing::warn!(
                        %identity,
                        reason = ?reason,
                        "lazy identity materialization fresh-spawned after typed resume fallback"
                    );
                    self.emit_event(
                        identity,
                        IdentityEvent::ResumeFallback {
                            identity: identity.clone(),
                            reason,
                        },
                    )
                    .await;
                }
                let effective_session_id = outcome.session_id().clone();
                if effective_session_id != registered_session_id {
                    // §8.4 trigger (b): the resume fallback abandoned the
                    // registered session — harvest it detached (materialize
                    // is a hot path; the session store read stays valid).
                    if let Some(injector) = self.agent_memory.read().await.as_ref() {
                        let abandoned_key = registered_session_id.to_string();
                        injector.note_session_generation(
                            identity,
                            &abandoned_key,
                            record.generation.get(),
                        );
                        injector.spawn_rotation_distillation(
                            identity,
                            &abandoned_key,
                            crate::memory::distiller::DistillCause::ResumeFallback,
                        );
                    }
                    abandoned_session_registrations.push(registered_session_id);
                }
                record.session_id = effective_session_id;
            } else {
                resumed = resume_snapshot.is_some();
            }
            record
        } else {
            let new_runtime_id =
                AgentRuntimeId::parse(&format!("rt:{identity}:0")).map_err(|err| {
                    IdentityRuntimeError::Internal(format!("failed to mint runtime id: {err}"))
                })?;
            let mut record = ContinuityRecord {
                identity: identity.clone(),
                agent_runtime_id: new_runtime_id,
                session_id: meerkat_core::types::SessionId::new(),
                generation: ContinuityGeneration::new(0),
                checkpoint_version: CheckpointVersion::new(0),
            };
            if let Err(err) = self
                .continuity_store
                .upsert_continuity_record(&record, grant.fencing_token)
                .await
            {
                let cleanup_error = self.release_uninstalled_materialize_lease(&grant).await;
                return Err(IdentityRuntimeError::Internal(format!(
                    "continuity upsert before materialize create: {err}{}",
                    cleanup_error
                        .as_ref()
                        .map(|e| format!("; lease cleanup failed: {e}"))
                        .unwrap_or_default(),
                )));
            }
            if let Some(bridge) = self.bridge.as_ref() {
                let provisional_session_id = record.session_id.clone();
                if let Err(err) = bridge
                    .register_session_runtime_state(
                        &record.session_id,
                        identity,
                        record.generation,
                        record.checkpoint_version,
                        grant.fencing_token,
                    )
                    .await
                    .map_err(|err| {
                        IdentityRuntimeError::Internal(format!(
                            "bridge register_session_runtime_state: {err}"
                        ))
                    })
                {
                    let unregister_error = Self::unregister_bridge_session_runtime_states(
                        bridge.as_ref(),
                        std::slice::from_ref(&provisional_session_id),
                    )
                    .await;
                    let delete_error = self
                        .continuity_store
                        .delete_continuity_record(identity, grant.fencing_token)
                        .await
                        .err();
                    let cleanup_error = self.release_uninstalled_materialize_lease(&grant).await;
                    if let Some(delete_error) = delete_error {
                        return Err(IdentityRuntimeError::Internal(format!(
                            "{err}{}; tentative continuity cleanup failed: {delete_error}{}",
                            unregister_error
                                .as_ref()
                                .map(|e| format!("; unregister session failed: {e}"))
                                .unwrap_or_default(),
                            cleanup_error
                                .as_ref()
                                .map(|e| format!("; lease cleanup failed: {e}"))
                                .unwrap_or_default(),
                        )));
                    }
                    if let Some(cleanup_error) = cleanup_error {
                        return Err(IdentityRuntimeError::Internal(format!(
                            "{err}{}; lease cleanup failed: {cleanup_error}",
                            unregister_error
                                .as_ref()
                                .map(|e| format!("; unregister session failed: {e}"))
                                .unwrap_or_default(),
                        )));
                    }
                    if let Some(unregister_error) = unregister_error {
                        return Err(IdentityRuntimeError::Internal(format!(
                            "{err}; unregister session failed: {unregister_error}"
                        )));
                    }
                    return Err(err);
                }
                let created_session_id = bridge
                    .create_session(
                        identity,
                        &record.agent_runtime_id,
                        &spec,
                        &draft,
                        &record.session_id,
                    )
                    .await;
                let created_session_id = match created_session_id {
                    Ok(session_id) => Ok(session_id),
                    // Typed on the FIRST attempt: the park is recorded and the
                    // caller receives the same `HostRejectedBuild` a later
                    // attempt fails fast with, so an in-band materialize (a
                    // console send to a Dormant identity) surfaces the reason,
                    // not `Internal` prose that happens to contain it.
                    Err(err) => match deterministic_build_rejection_reason(&err) {
                        Some(reason) => {
                            self.mark_host_rejected_build_park(identity, reason.clone())
                                .await;
                            Err(IdentityRuntimeError::HostRejectedBuild {
                                identity: identity.clone(),
                                reason,
                            })
                        }
                        None => Err(IdentityRuntimeError::Internal(format!(
                            "bridge create_session: {err}"
                        ))),
                    },
                };
                match created_session_id {
                    Ok(session_id) => {
                        if session_id != provisional_session_id {
                            abandoned_session_registrations.push(provisional_session_id);
                        }
                        record.session_id = session_id;
                    }
                    Err(err) => {
                        let unregister_error = Self::unregister_bridge_session_runtime_states(
                            bridge.as_ref(),
                            std::slice::from_ref(&provisional_session_id),
                        )
                        .await;
                        let cleanup_error =
                            bridge.retire_member(&record.agent_runtime_id).await.err();
                        let delete_error = self
                            .continuity_store
                            .delete_continuity_record(identity, grant.fencing_token)
                            .await
                            .err();
                        let lease_cleanup_error =
                            self.release_uninstalled_materialize_lease(&grant).await;
                        if unregister_error.is_some()
                            || cleanup_error.is_some()
                            || delete_error.is_some()
                            || lease_cleanup_error.is_some()
                        {
                            return Err(IdentityRuntimeError::Internal(format!(
                                "{err}{}{}{}{}",
                                unregister_error
                                    .as_ref()
                                    .map(|e| format!("; unregister session failed: {e}"))
                                    .unwrap_or_default(),
                                cleanup_error
                                    .as_ref()
                                    .map(|e| format!("; cleanup retire failed: {e}"))
                                    .unwrap_or_default(),
                                delete_error
                                    .as_ref()
                                    .map(|e| format!("; tentative continuity cleanup failed: {e}"))
                                    .unwrap_or_default(),
                                lease_cleanup_error
                                    .as_ref()
                                    .map(|e| format!("; lease cleanup failed: {e}"))
                                    .unwrap_or_default(),
                            )));
                        }
                        return Err(err);
                    }
                }
            }
            record
        };

        if let Err(err) = self
            .continuity_store
            .upsert_continuity_record(&record, grant.fencing_token)
            .await
        {
            let unregister_error = if let Some(bridge) = self.bridge.as_ref() {
                let mut sessions_to_unregister = abandoned_session_registrations.clone();
                sessions_to_unregister.push(record.session_id.clone());
                Self::unregister_bridge_session_runtime_states(
                    bridge.as_ref(),
                    &sessions_to_unregister,
                )
                .await
            } else {
                None
            };
            let cleanup_error = if let Some(bridge) = self.bridge.as_ref() {
                bridge.retire_member(&record.agent_runtime_id).await.err()
            } else {
                None
            };
            let restore_error = self
                .restore_continuity_after_materialize_failure(
                    identity,
                    original_continuity.as_ref(),
                    &grant,
                )
                .await;
            let lease_cleanup_error = self.release_uninstalled_materialize_lease(&grant).await;
            if unregister_error.is_some()
                || cleanup_error.is_some()
                || restore_error.is_some()
                || lease_cleanup_error.is_some()
            {
                return Err(IdentityRuntimeError::Internal(format!(
                    "continuity upsert after materialize: {err}{}{}{}{}",
                    unregister_error
                        .as_ref()
                        .map(|e| format!("; unregister session failed: {e}"))
                        .unwrap_or_default(),
                    cleanup_error
                        .as_ref()
                        .map(|e| format!("; cleanup retire failed: {e}"))
                        .unwrap_or_default(),
                    restore_error
                        .as_ref()
                        .map(|e| format!("; continuity rollback failed: {e}"))
                        .unwrap_or_default(),
                    lease_cleanup_error
                        .as_ref()
                        .map(|e| format!("; lease cleanup failed: {e}"))
                        .unwrap_or_default(),
                )));
            }
            return Err(IdentityRuntimeError::Internal(format!(
                "continuity upsert after materialize: {err}"
            )));
        }
        if let Some(bridge) = self.bridge.as_ref() {
            let register_result = bridge
                .register_session_runtime_state(
                    &record.session_id,
                    identity,
                    record.generation,
                    record.checkpoint_version,
                    grant.fencing_token,
                )
                .await;
            let effective_checkpoint_version = match register_result {
                Ok(version) => version,
                Err(err) => {
                    let mut sessions_to_unregister = abandoned_session_registrations.clone();
                    sessions_to_unregister.push(record.session_id.clone());
                    let unregister_error = Self::unregister_bridge_session_runtime_states(
                        bridge.as_ref(),
                        &sessions_to_unregister,
                    )
                    .await;
                    let cleanup_error = bridge.retire_member(&record.agent_runtime_id).await.err();
                    let restore_error = self
                        .restore_continuity_after_materialize_failure(
                            identity,
                            original_continuity.as_ref(),
                            &grant,
                        )
                        .await;
                    let lease_cleanup_error =
                        self.release_uninstalled_materialize_lease(&grant).await;
                    return Err(IdentityRuntimeError::Internal(format!(
                        "bridge register actual session runtime state: {err}{}{}{}{}",
                        unregister_error
                            .as_ref()
                            .map(|e| format!("; unregister session failed: {e}"))
                            .unwrap_or_default(),
                        cleanup_error
                            .as_ref()
                            .map(|e| format!("; cleanup retire failed: {e}"))
                            .unwrap_or_default(),
                        restore_error
                            .as_ref()
                            .map(|e| format!("; continuity rollback failed: {e}"))
                            .unwrap_or_default(),
                        lease_cleanup_error
                            .as_ref()
                            .map(|e| format!("; lease cleanup failed: {e}"))
                            .unwrap_or_default(),
                    )));
                }
            };
            record.checkpoint_version = effective_checkpoint_version;
            if let Some(err) = Self::unregister_bridge_session_runtime_states(
                bridge.as_ref(),
                &abandoned_session_registrations,
            )
            .await
            {
                let actual_unregister_error = Self::unregister_bridge_session_runtime_states(
                    bridge.as_ref(),
                    std::slice::from_ref(&record.session_id),
                )
                .await;
                let unregister_error = actual_unregister_error
                    .map(|actual_err| format!("{err}; actual session: {actual_err}"))
                    .unwrap_or(err);
                let cleanup_error = bridge.retire_member(&record.agent_runtime_id).await.err();
                let restore_error = self
                    .restore_continuity_after_materialize_failure(
                        identity,
                        original_continuity.as_ref(),
                        &grant,
                    )
                    .await;
                let lease_cleanup_error = self.release_uninstalled_materialize_lease(&grant).await;
                return Err(IdentityRuntimeError::Internal(format!(
                    "bridge unregister abandoned session runtime state: {unregister_error}{}{}{}",
                    cleanup_error
                        .as_ref()
                        .map(|e| format!("; cleanup retire failed: {e}"))
                        .unwrap_or_default(),
                    restore_error
                        .as_ref()
                        .map(|e| format!("; continuity rollback failed: {e}"))
                        .unwrap_or_default(),
                    lease_cleanup_error
                        .as_ref()
                        .map(|e| format!("; lease cleanup failed: {e}"))
                        .unwrap_or_default(),
                )));
            }
        }

        {
            let mut entries = self.entries.write().await;
            let entry = entries
                .get_mut(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            entry.continuity = Some(record.clone());
            entry.lease = Some(Self::lease_entry_from_grant(&grant));
            entry.state = IdentityLifecycleState::Active;
            entry.checkpoint_version = record.checkpoint_version;
        }
        self.emit_event(
            identity,
            IdentityEvent::StateChanged {
                identity: identity.clone(),
                new_state: IdentityLifecycleState::Active,
            },
        )
        .await;
        self.clear_materialization_backoff(identity).await;
        #[cfg(feature = "extension-state")]
        if let Some(publication) = extension_publication {
            publication.publish();
        }
        Ok(EmbodimentOutcome {
            already_active: false,
            record,
            resumed,
            draft,
        })
    }

    /// Project one member-attributable embodiment failure into the durable
    /// identity runtime without turning it into a fleet-level bootstrap
    /// failure. The returned payload is the exact typed outcome installed by
    /// eager restore. Pass-level roster, topology, and batch-store failures do
    /// not enter this door.
    pub(crate) async fn park_embodiment_failure(
        &self,
        identity: &AgentIdentity,
        error: &IdentityRuntimeError,
    ) -> ContinuityFailure {
        let explicit_failure = match error {
            IdentityRuntimeError::EmbodimentRejected(failure) => Some((**failure).clone()),
            _ => None,
        };
        let (record, transitioned) = {
            let mut entries = self.entries.write().await;
            let record = entries
                .get(identity)
                .and_then(|entry| entry.continuity.clone());
            let mut transitioned = false;
            if let Some(entry) = entries.get_mut(identity) {
                transitioned = entry.state != IdentityLifecycleState::Broken;
                entry.state = IdentityLifecycleState::Broken;
            }
            (record, transitioned)
        };
        if transitioned {
            self.emit_event(
                identity,
                IdentityEvent::StateChanged {
                    identity: identity.clone(),
                    new_state: IdentityLifecycleState::Broken,
                },
            )
            .await;
        }

        explicit_failure.unwrap_or_else(|| ContinuityFailure {
            identity: identity.clone(),
            kind: match error {
                IdentityRuntimeError::Store(_) => ContinuityFailureKind::StoreUnavailable,
                _ => ContinuityFailureKind::EmbodimentFailed,
            },
            record,
            detail: error.to_string(),
        })
    }

    /// A retired identity (`Retiring`, the terminal form left by
    /// `retire_locked`) is revived ONLY by an operation addressed to it
    /// (`send`, `dispatch`, `materialize`, `reload_member`). Indirect hydration
    /// (a peer hand-off's `materialize_reachable_peers`, fleet
    /// `materialize_all`, the background warm) must not silently bring a
    /// retired member back; it is refused typed exactly as before #404 so the
    /// initiator's cycle completes without the retired peer.
    async fn retired_terminal_refusal(
        &self,
        identity: &AgentIdentity,
        operation: &'static str,
    ) -> Option<IdentityRuntimeError> {
        let state = self
            .entries
            .read()
            .await
            .get(identity)
            .map(|entry| entry.state)?;
        (state == IdentityLifecycleState::Retiring).then(|| IdentityRuntimeError::InvalidState {
            identity: identity.clone(),
            state,
            operation,
        })
    }

    async fn best_effort_materialize_identity(
        &self,
        identity: AgentIdentity,
        initiator: Option<&AgentIdentity>,
        operation: &'static str,
    ) -> Option<ContinuityRecord> {
        let attempt_lock = self.best_effort_materialization_lock_for(&identity).await;
        let _attempt_guard = attempt_lock.lock().await;

        if let Some(err) = self
            .retired_terminal_refusal(&identity, "materialize")
            .await
        {
            tracing::warn!(
                identity = %identity,
                initiator = initiator.map(ToString::to_string).as_deref(),
                error = %err,
                "identity best-effort materialization refused: the identity is retired; only a \
                 send addressed to it re-materializes it"
            );
            self.record_best_effort_materialization_failure(&identity, initiator, operation, &err)
                .await;
            return None;
        }

        if let Some(error) = self.materialization_backoff_error(&identity).await {
            tracing::debug!(
                identity = %identity,
                initiator = initiator.map(ToString::to_string).as_deref(),
                error = %error,
                "identity best-effort materialization skipped due to materialization backoff"
            );
            return None;
        }

        match self.materialize(&identity).await {
            Ok(record) => {
                self.clear_materialization_backoff(&identity).await;
                Some(record)
            }
            Err(err) => {
                tracing::warn!(
                    identity = %identity,
                    initiator = initiator.map(ToString::to_string).as_deref(),
                    error = %err,
                    "identity best-effort materialization skipped identity after materialization failure"
                );
                self.record_best_effort_materialization_failure(
                    &identity, initiator, operation, &err,
                )
                .await;
                None
            }
        }
    }

    async fn materialize_all_records(
        &self,
    ) -> Vec<(
        AgentIdentity,
        Result<ContinuityRecord, IdentityRuntimeError>,
    )> {
        // One embodiment burst (see [`Self::enter_embodiment_burst`]).
        let _burst = self.enter_embodiment_burst();
        let identities = self.registered_identities().await;
        stream::iter(identities.into_iter().map(|identity| async move {
            let result = self.materialize(&identity).await;
            (identity, result)
        }))
        .buffer_unordered(MANAGED_PEER_RECONCILE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
    }

    /// Materialize all identities currently registered with the runtime.
    ///
    /// Fleet hydration is best-effort: one member that cannot build is skipped
    /// and surfaced through logs/error hooks rather than aborting unrelated
    /// members.
    pub async fn materialize_all(&self) -> Result<Vec<ContinuityRecord>, IdentityRuntimeError> {
        // One embodiment burst (see [`Self::enter_embodiment_burst`]).
        let _burst = self.enter_embodiment_burst();
        let identities = self.registered_identities().await;
        let records = stream::iter(identities.into_iter().map(|identity| async move {
            self.best_effort_materialize_identity(identity, None, "materialize_all")
                .await
        }))
        .buffer_unordered(MANAGED_PEER_RECONCILE_CONCURRENCY)
        .filter_map(async move |record| record)
        .collect::<Vec<_>>()
        .await;

        let desired_edges = self.desired_peer_edges.read().await.clone();
        if !desired_edges.is_empty()
            && let Err(err) = self.reconcile_managed_peer_edges(&desired_edges).await
        {
            tracing::warn!(
                error = %err,
                "identity materialize_all completed with topology reconcile warning"
            );
        }

        Ok(records)
    }

    /// Materialize all identities and fail if any registered identity cannot
    /// hydrate. Flow admission uses this strict variant so a run is not accepted
    /// with a partially materialized identity-first fleet.
    pub async fn materialize_all_required(
        &self,
    ) -> Result<Vec<ContinuityRecord>, IdentityRuntimeError> {
        let results = self.materialize_all_records().await;
        let mut records = Vec::with_capacity(results.len());
        let mut failures = Vec::new();

        for (identity, result) in results {
            match result {
                Ok(record) => records.push(record),
                Err(err) => failures.push(format!("{identity}: {err}")),
            }
        }

        if !failures.is_empty() {
            return Err(IdentityRuntimeError::Internal(format!(
                "identity-first required materialization failed for {} identities: {}",
                failures.len(),
                failures.join("; ")
            )));
        }

        let desired_edges = self.desired_peer_edges.read().await.clone();
        if !desired_edges.is_empty() {
            self.reconcile_managed_peer_edges(&desired_edges).await?;
        }

        Ok(records)
    }

    /// Cancellation-safe strict fleet hydration for flow/request boundaries.
    pub async fn materialize_all_required_tracked(
        self: &Arc<Self>,
    ) -> Result<Vec<ContinuityRecord>, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        self.run_tracked_foreground(async move { runtime.materialize_all_required().await })
            .await
    }

    /// Ensure an active identity's desired peer neighborhood exists in the
    /// concrete mob graph before ordinary communication starts.
    pub async fn materialize_reachable_peers(
        &self,
        identity: &AgentIdentity,
    ) -> Result<Vec<ContinuityRecord>, IdentityRuntimeError> {
        let peers = self.reachable_peer_identities(identity).await;
        let records = stream::iter(peers.into_iter().map(|peer| async move {
            self.best_effort_materialize_identity(
                peer,
                Some(identity),
                "materialize_reachable_peers",
            )
            .await
        }))
        .buffer_unordered(MANAGED_PEER_RECONCILE_CONCURRENCY)
        .filter_map(async move |record| record)
        .collect::<Vec<_>>()
        .await;

        let desired_edges = self.desired_peer_edges.read().await.clone();
        if !desired_edges.is_empty()
            && let Err(err) = self.reconcile_managed_peer_edges(&desired_edges).await
        {
            tracing::warn!(
                identity = %identity,
                error = %err,
                "identity peer materialization completed with topology reconcile warning"
            );
        }

        Ok(records)
    }

    // -----------------------------------------------------------------------
    // Subscribe — REQ-06
    // -----------------------------------------------------------------------

    /// Subscribe to identity-scoped events.
    ///
    /// Returns a broadcast receiver that yields `IdentityEvent` items for
    /// state changes, lease updates, lease loss, and checkpoint completions.
    pub async fn subscribe(
        &self,
        identity: &AgentIdentity,
    ) -> Result<broadcast::Receiver<IdentityEvent>, IdentityRuntimeError> {
        let channels = self.event_channels.read().await;
        let tx = channels
            .get(identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
        Ok(tx.subscribe())
    }

    /// Update the spec for an existing identity (used during reconciliation).
    pub async fn update_spec(&self, spec: DurableAgentSpec) -> Result<(), IdentityRuntimeError> {
        let mut entries = self.entries.write().await;
        let entry = entries
            .get_mut(&spec.identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(spec.identity.clone()))?;
        entry.spec = spec;
        Ok(())
    }

    /// Release every exact provider grant retained by a Broken entry. The
    /// caller owns this identity's lifecycle lock.
    ///
    /// Most failed lower-plane transitions already park authority in
    /// `pending_lease_release`, but rollback of an originally Active entry can
    /// deliberately retain its current grant in `lease` while projecting
    /// Broken. Move that live grant into pending state before the provider await
    /// so cancellation, a provider failure, and shutdown all retain the exact
    /// fencing token. A failed release aborts reconcile before lazy registration
    /// can overwrite the entry or restore can reacquire authority.
    async fn release_broken_lease_locked(
        &self,
        identity: &AgentIdentity,
    ) -> Result<bool, IdentityRuntimeError> {
        let pending = {
            let mut entries = self.entries.write().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            if entry.state != IdentityLifecycleState::Broken {
                return Ok(false);
            }

            let existing_pending = entry.pending_lease_release.clone();
            let retained_lease = entry.lease.as_ref().map(|lease| LeaseGrant {
                identity: identity.clone(),
                fencing_token: lease.fencing_token,
                ttl: lease.ttl,
            });
            if let (Some(pending), Some(retained)) =
                (existing_pending.as_ref(), retained_lease.as_ref())
                && pending.fencing_token != retained.fencing_token
            {
                return Err(IdentityRuntimeError::Internal(format!(
                    "Broken identity {identity} retained conflicting fencing tokens {} and {}",
                    pending.fencing_token, retained.fencing_token
                )));
            }

            let grant = existing_pending.or(retained_lease);
            if let Some(grant) = grant.as_ref() {
                let entry = entries
                    .get_mut(identity)
                    .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
                entry.lease = None;
                entry.pending_lease_release = Some(grant.clone());
            }
            grant
        };
        let Some(grant) = pending else {
            return Ok(false);
        };

        if let Err(error) = self
            .lease_provider
            .release_leases(std::slice::from_ref(&grant))
            .await
        {
            self.mark_bootstrap_from_lifecycle(
                identity,
                IdentityLifecycleState::Broken,
                Some(format!("pending lease release retry: {error}")),
            );
            return Err(IdentityRuntimeError::Lease(error));
        }

        let mut entries = self.entries.write().await;
        let entry = entries
            .get_mut(identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
        match entry.pending_lease_release.as_ref() {
            Some(current) if current.fencing_token == grant.fencing_token => {
                entry.pending_lease_release = None;
            }
            Some(current) => {
                return Err(IdentityRuntimeError::Internal(format!(
                    "pending lease release for {identity} changed from fencing token {} to {} under lifecycle authority",
                    grant.fencing_token, current.fencing_token
                )));
            }
            None => {
                return Err(IdentityRuntimeError::Internal(format!(
                    "pending lease release for {identity} disappeared under lifecycle authority"
                )));
            }
        }
        Ok(true)
    }

    /// Prepare an existing Broken entry for metadata replacement while the
    /// caller owns its lifecycle lock. The concrete member is disposed before
    /// the exact retained provider grant is moved through the pending-release
    /// ledger. A failure leaves the entry Broken and the authority visible for
    /// retry or shutdown.
    pub(crate) async fn prepare_broken_identity_for_registration(
        &self,
        identity: &AgentIdentity,
    ) -> Result<(), IdentityRuntimeError> {
        self.cleanup_broken_lower_plane_locked(identity).await?;
        self.release_broken_lease_locked(identity).await?;
        Ok(())
    }

    /// Dispose the concrete member and its session-store authority retained by
    /// a continuity-backed Broken entry. Lease loss is fail-closed at the
    /// identity plane, but it cannot synchronously remove the lower member;
    /// every roster reconcile that will reproject the Broken entry must do so
    /// before abandoning or reusing the runtime alias. The real bridge treats
    /// an already-retired member as success, and session unregister is
    /// idempotent, making retries safe when a later exact-grant cleanup step
    /// fails.
    ///
    /// This deliberately does not mutate `lease` or `pending_lease_release`:
    /// provider authority is an independent exact-token cleanup obligation.
    async fn cleanup_broken_lower_plane_locked(
        &self,
        identity: &AgentIdentity,
    ) -> Result<(), IdentityRuntimeError> {
        let continuity = {
            let entries = self.entries.read().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            (entry.state == IdentityLifecycleState::Broken)
                .then(|| entry.continuity.clone())
                .flatten()
        };
        let (Some(bridge), Some(record)) = (self.bridge.as_ref(), continuity) else {
            return Ok(());
        };

        let retire_error = bridge.retire_member(&record.agent_runtime_id).await.err();
        let unregister_error = bridge
            .unregister_session_runtime_state(&record.session_id)
            .await
            .err();
        match (retire_error, unregister_error) {
            (None, None) => Ok(()),
            (retire_error, unregister_error) => Err(IdentityRuntimeError::Internal(format!(
                "cleanup Broken lower-plane state for {identity}{}{}",
                retire_error
                    .map(|error| format!("; retire member: {error}"))
                    .unwrap_or_default(),
                unregister_error
                    .map(|error| format!("; unregister session: {error}"))
                    .unwrap_or_default()
            ))),
        }
    }

    async fn accept_bootstrap_spec_locked(
        &self,
        spec: DurableAgentSpec,
        generation: u64,
    ) -> Result<(), IdentityRuntimeError> {
        let mut entries = self.entries.write().await;
        let entry = entries
            .get_mut(&spec.identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(spec.identity.clone()))?;
        entry.spec = spec;
        entry.bootstrap_generation = generation;
        Ok(())
    }

    /// Converge currently registered embodiments with a newly desired roster.
    ///
    /// Restore flows only walk the desired roster, so removal and replacement
    /// must happen first. Each transition shares the same per-identity lock as
    /// foreground materialization and lifecycle RPCs. Same-profile changes are
    /// REQ-33 hot reloads: publish the new registry metadata while preserving
    /// the exact live session and grant. Profile changes retire to Dormant
    /// before the new spec is installed; the selected eager/background/lazy
    /// policy then decides when to rebuild the physical member.
    async fn reconcile_roster_members(
        &self,
        roster: &[DurableAgentSpec],
        generation: u64,
    ) -> Result<(), IdentityRuntimeError> {
        self.reconcile_roster_members_after_lifecycle_lock(roster, generation, |_| {
            std::future::ready(())
        })
        .await
    }

    /// Internal deterministic seam used by concurrency regressions. The hook
    /// runs with each identity's lifecycle lock held and before the entry is
    /// inspected or mutated.
    async fn reconcile_roster_members_after_lifecycle_lock<H, F>(
        &self,
        roster: &[DurableAgentSpec],
        generation: u64,
        mut after_lifecycle_lock: H,
    ) -> Result<(), IdentityRuntimeError>
    where
        H: FnMut(&AgentIdentity) -> F,
        F: Future<Output = ()>,
    {
        Self::validate_roster_uniqueness(roster)?;
        let desired = roster
            .iter()
            .map(|spec| (spec.identity.clone(), spec.clone()))
            .collect::<BTreeMap<_, _>>();
        let registered = self.registered_identities().await;

        for identity in registered
            .iter()
            .filter(|identity| !desired.contains_key(*identity))
        {
            let lifecycle_lock = self.lifecycle_lock_for(identity).await;
            let _lifecycle_guard = lifecycle_lock.lock().await;
            after_lifecycle_lock(identity).await;
            self.cleanup_broken_lower_plane_locked(identity).await?;
            self.release_broken_lease_locked(identity).await?;
            let state = self
                .entries
                .read()
                .await
                .get(identity)
                .map(|entry| entry.state);
            let Some(state) = state else {
                continue;
            };
            match state {
                IdentityLifecycleState::Active => {
                    self.retire_locked(identity).await?;
                }
                // `Retiring` here is the RETIRED terminal form: `retire_locked`
                // leaves the entry in it after the mob member is gone and the
                // lease released, and every lifecycle mutation (including this
                // one) runs under the identity's lifecycle lock, so the
                // in-transaction form is never observable from this arm. There
                // is no live member left to retire; removal is the same
                // bookkeeping as for a Dormant entry. Refusing it is how a
                // retired identity got stuck for every later roster reconcile
                // (meerkat-mobkit #404, a 2026-09-05 production incident).
                IdentityLifecycleState::Dormant
                | IdentityLifecycleState::Broken
                | IdentityLifecycleState::Uninitialized
                | IdentityLifecycleState::Retiring => {
                    let lease = self
                        .entries
                        .read()
                        .await
                        .get(identity)
                        .and_then(|entry| entry.lease.as_ref())
                        .map(|lease| LeaseGrant {
                            identity: identity.clone(),
                            fencing_token: lease.fencing_token,
                            ttl: lease.ttl,
                        });
                    if let Some(lease) = lease {
                        self.lease_provider
                            .release_leases(std::slice::from_ref(&lease))
                            .await
                            .map_err(IdentityRuntimeError::Lease)?;
                    }
                }
                IdentityLifecycleState::Suspended => {
                    return Err(IdentityRuntimeError::InvalidState {
                        identity: identity.clone(),
                        state,
                        operation: "reconcile_roster_remove",
                    });
                }
            }
            self.event_channels.write().await.remove(identity);
            self.entries.write().await.remove(identity);
        }

        for (identity, desired_spec) in desired {
            let lifecycle_lock = self.lifecycle_lock_for(&identity).await;
            let _lifecycle_guard = lifecycle_lock.lock().await;
            after_lifecycle_lock(&identity).await;
            // Initial bootstrap reaches this loop before lazy/eager restore has
            // registered the desired identities. Pending-release recovery only
            // applies to an existing lifecycle entry.
            if !self.entries.read().await.contains_key(&identity) {
                continue;
            }
            let reconciling_broken_entry = {
                let entries = self.entries.read().await;
                entries
                    .get(&identity)
                    .is_some_and(|entry| entry.state == IdentityLifecycleState::Broken)
            };
            if reconciling_broken_entry {
                self.cleanup_broken_lower_plane_locked(&identity).await?;
            }
            // This runs before the same-spec fast path. A failed retire may
            // leave the desired metadata unchanged while the physical member
            // is already gone and its exact provider grant is still held.
            self.release_broken_lease_locked(&identity).await?;
            let current = self
                .entries
                .read()
                .await
                .get(&identity)
                .map(|entry| (entry.spec.clone(), entry.state));
            let Some((current_spec, state)) = current else {
                continue;
            };
            if current_spec == desired_spec {
                self.accept_bootstrap_spec_locked(desired_spec, generation)
                    .await?;
                continue;
            }
            // A retired (`Retiring`) entry has no live member: a profile change
            // is adopted like on a Dormant entry and takes effect on the next
            // materialization. Only a mid-transaction `Suspended` refuses.
            if state == IdentityLifecycleState::Suspended {
                return Err(IdentityRuntimeError::InvalidState {
                    identity,
                    state,
                    operation: "reconcile_roster_replace",
                });
            }
            // REQ-33 and `compute_reconcile_actions` classify every
            // same-profile change (addressability, labels, display name,
            // context, and instructions) as a metadata hot reload. Retiring
            // an Active member here needlessly rotates its exact grant and can
            // collide with a still-draining session during eager restore.
            if current_spec.profile == desired_spec.profile {
                self.accept_bootstrap_spec_locked(desired_spec, generation)
                    .await?;
                continue;
            }
            match state {
                IdentityLifecycleState::Active => {
                    self.retire_locked(&identity).await?;
                }
                IdentityLifecycleState::Dormant
                | IdentityLifecycleState::Broken
                | IdentityLifecycleState::Uninitialized
                | IdentityLifecycleState::Retiring => {}
                IdentityLifecycleState::Suspended => {
                    return Err(IdentityRuntimeError::InvalidState {
                        identity,
                        state,
                        operation: "reconcile_roster_replace",
                    });
                }
            }
            self.accept_bootstrap_spec_locked(desired_spec, generation)
                .await?;
        }

        Ok(())
    }

    /// Adopt the roster's CURRENT spec for `identity` into the in-memory entry,
    /// so a subsequent [`reset`](Self::reset) rebuilds the regenerated session
    /// on the current profile instead of carrying the stored one forward.
    ///
    /// Best-effort: logs and leaves the stored spec in place if the roster
    /// can't be resolved or no longer lists the identity (reset's primary job —
    /// the destructive continuity reset — must not fail because the roster
    /// provider hiccuped). The runtime stays roster-agnostic; the provider is
    /// supplied by the caller (the reset RPC handler), which owns it.
    pub async fn adopt_roster_spec(
        &self,
        roster_provider: &Arc<dyn RosterProvider>,
        identity: &AgentIdentity,
    ) {
        self.adopt_roster_spec_with_context(roster_provider, identity, None)
            .await;
    }

    async fn adopt_roster_spec_with_context(
        &self,
        roster_provider: &Arc<dyn RosterProvider>,
        identity: &AgentIdentity,
        mob_definition: Option<meerkat_mob::MobDefinition>,
    ) {
        match roster_provider
            .roster(&RosterContext {
                mob_definition,
                previous_identities: Vec::new(),
            })
            .await
        {
            Ok(specs) => {
                if let Some(spec) = specs.into_iter().find(|s| &s.identity == identity)
                    && let Err(err) = self.update_spec(spec).await
                {
                    tracing::warn!(
                        identity = %identity,
                        error = %err,
                        "reset: failed to adopt current roster spec; rebuilding on stored spec",
                    );
                }
            }
            Err(err) => {
                tracing::warn!(
                    identity = %identity,
                    error = %err,
                    "reset: roster provider failed; rebuilding on stored spec",
                );
            }
        }
    }

    /// Update the lease for an identity.
    pub async fn update_lease(
        &self,
        identity: &AgentIdentity,
        grant: LeaseGrant,
    ) -> Result<(), IdentityRuntimeError> {
        let fencing_token = grant.fencing_token;
        let mut entries = self.entries.write().await;
        let entry = entries
            .get_mut(identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
        entry.lease = Some(LeaseEntry {
            fencing_token,
            ttl: grant.ttl,
            acquired_at: Instant::now(),
        });
        drop(entries);
        self.lease_renewal_notify.notify_one();
        self.emit_event(
            identity,
            IdentityEvent::LeaseUpdated {
                identity: identity.clone(),
                fencing_token,
            },
        )
        .await;
        Ok(())
    }

    /// Mark a lease as lost for an identity (INV-02).
    pub async fn mark_lease_lost(
        &self,
        identity: &AgentIdentity,
    ) -> Result<(), IdentityRuntimeError> {
        let mut entries = self.entries.write().await;
        let entry = entries
            .get_mut(identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
        entry.state = IdentityLifecycleState::Broken;
        entry.lease = None;
        drop(entries);
        self.mark_bootstrap_from_lifecycle(
            identity,
            IdentityLifecycleState::Broken,
            Some("external lease authority was lost".to_string()),
        );
        self.emit_event(
            identity,
            IdentityEvent::LeaseLost {
                identity: identity.clone(),
            },
        )
        .await;
        Ok(())
    }

    /// Remove an identity from the runtime.
    #[allow(dead_code)]
    pub(crate) async fn remove(&self, identity: &AgentIdentity) -> Option<IdentityEntry> {
        self.event_channels.write().await.remove(identity);
        self.entries.write().await.remove(identity)
    }

    /// Set the lifecycle state for an identity.
    pub async fn set_state(
        &self,
        identity: &AgentIdentity,
        state: IdentityLifecycleState,
    ) -> Result<(), IdentityRuntimeError> {
        let mut entries = self.entries.write().await;
        let entry = entries
            .get_mut(identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
        entry.state = state;
        drop(entries);
        if state == IdentityLifecycleState::Active {
            self.lease_renewal_notify.notify_one();
        }
        self.emit_event(
            identity,
            IdentityEvent::StateChanged {
                identity: identity.clone(),
                new_state: state,
            },
        )
        .await;
        Ok(())
    }

    /// Fail the exact active embodiment when its live session has been torn
    /// down underneath the identity layer. A stale close notification from a
    /// replaced generation is ignored, while the current embodiment becomes
    /// `Broken` so the existing continuity repair supervisor can rebuild it.
    pub(crate) async fn mark_active_runtime_broken(
        &self,
        identity: &AgentIdentity,
        expected_runtime_id: &str,
        expected_fencing_token: u64,
        detail: &str,
    ) -> Result<bool, IdentityRuntimeError> {
        let lifecycle_lock = self.lifecycle_lock_for(identity).await;
        let _lifecycle_guard = lifecycle_lock.lock().await;
        let changed = {
            let mut entries = self.entries.write().await;
            let entry = entries
                .get_mut(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            let is_current_active = entry.state == IdentityLifecycleState::Active
                && entry
                    .continuity
                    .as_ref()
                    .is_some_and(|record| record.agent_runtime_id.as_str() == expected_runtime_id)
                && entry
                    .lease
                    .as_ref()
                    .is_some_and(|lease| lease.fencing_token.get() == expected_fencing_token);
            if is_current_active {
                // Retain the exact live lease. Broken repair owns lower-plane
                // cleanup and releases that grant before rematerializing.
                entry.state = IdentityLifecycleState::Broken;
            }
            is_current_active
        };
        if changed {
            self.mark_bootstrap_from_lifecycle(
                identity,
                IdentityLifecycleState::Broken,
                Some(detail.to_string()),
            );
            self.emit_event(
                identity,
                IdentityEvent::StateChanged {
                    identity: identity.clone(),
                    new_state: IdentityLifecycleState::Broken,
                },
            )
            .await;
        }
        Ok(changed)
    }

    // -----------------------------------------------------------------------
    // Lease checking (INV-01, INV-02)
    // -----------------------------------------------------------------------

    /// Check that the identity has an active, non-expired lease.
    /// Returns the fencing token if valid.
    fn check_lease(entry: &IdentityEntry) -> Result<FencingToken, IdentityRuntimeError> {
        match &entry.lease {
            Some(lease) if !lease.is_expired() => Ok(lease.fencing_token),
            Some(_) => Err(IdentityRuntimeError::LeaseLost(entry.spec.identity.clone())),
            None => Err(IdentityRuntimeError::NoActiveLease(
                entry.spec.identity.clone(),
            )),
        }
    }

    fn lease_entry_from_grant(grant: &LeaseGrant) -> LeaseEntry {
        LeaseEntry {
            fencing_token: grant.fencing_token,
            ttl: grant.ttl,
            acquired_at: Instant::now(),
        }
    }

    /// Transfer a provider-returned active grant into runtime ownership before
    /// any fallible continuity or bridge projection. The provider call is the
    /// authority commit: once it returns, retaining only the previous token is
    /// unsafe because exact-token release would become a no-op.
    async fn stage_active_grant(
        &self,
        identity: &AgentIdentity,
        expected_previous: Option<FencingToken>,
        grant: &LeaseGrant,
    ) -> Result<Option<ContinuityRecord>, IdentityRuntimeError> {
        let staged = {
            let mut entries = self.entries.write().await;
            (|| {
                if grant.identity != *identity {
                    return Err(IdentityRuntimeError::Internal(format!(
                        "provider returned grant for {} while publishing active authority for {identity}",
                        grant.identity
                    )));
                }
                let entry = entries
                    .get_mut(identity)
                    .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
                if entry.state != IdentityLifecycleState::Active {
                    return Err(IdentityRuntimeError::InvalidState {
                        identity: identity.clone(),
                        state: entry.state,
                        operation: "stage_active_grant",
                    });
                }
                let current = entry
                    .lease
                    .as_ref()
                    .ok_or_else(|| IdentityRuntimeError::NoActiveLease(identity.clone()))?;
                if let Some(expected) = expected_previous
                    && current.fencing_token != expected
                {
                    return Err(IdentityRuntimeError::Internal(format!(
                        "active lease for {identity} changed from fencing token {expected} to {} before renewed authority could be staged",
                        current.fencing_token
                    )));
                }
                if let Some(pending) = entry.pending_lease_release.as_ref() {
                    return Err(IdentityRuntimeError::Internal(format!(
                        "active identity {identity} already has pending fencing token {} while staging {}",
                        pending.fencing_token, grant.fencing_token
                    )));
                }

                let continuity = entry.continuity.clone();
                // Broken + pending is the fail-closed intermediate state.
                // The lifecycle lock prevents another operation from
                // observing it as a completed transition, while a dropped
                // caller still leaves repair/shutdown an exact token.
                entry.state = IdentityLifecycleState::Broken;
                entry.lease = None;
                entry.pending_lease_release = Some(grant.clone());
                Ok(continuity)
            })()
        };

        match staged {
            Ok(continuity) => Ok(continuity),
            Err(error) => {
                if let Err(cleanup_error) = self
                    .release_or_park_untracked_leases(std::slice::from_ref(grant))
                    .await
                {
                    return Err(IdentityRuntimeError::Internal(format!(
                        "{error}; exact grant cleanup failed: {cleanup_error}"
                    )));
                }
                Err(error)
            }
        }
    }

    async fn fail_staged_active_grant(
        &self,
        identity: &AgentIdentity,
        grant: &LeaseGrant,
        detail: String,
    ) {
        let retained_by_entry = {
            let mut entries = self.entries.write().await;
            entries.get_mut(identity).is_some_and(|entry| {
                if entry
                    .pending_lease_release
                    .as_ref()
                    .is_some_and(|pending| pending.fencing_token == grant.fencing_token)
                {
                    entry.state = IdentityLifecycleState::Broken;
                    entry.lease = None;
                    true
                } else {
                    false
                }
            })
        };
        if !retained_by_entry {
            self.park_unactivated_lease_releases(std::slice::from_ref(grant))
                .await;
        }
        self.mark_bootstrap_from_lifecycle(identity, IdentityLifecycleState::Broken, Some(detail));
        self.emit_event(
            identity,
            IdentityEvent::StateChanged {
                identity: identity.clone(),
                new_state: IdentityLifecycleState::Broken,
            },
        )
        .await;
    }

    async fn suspend_bridge_continuity(
        &self,
        continuity: Option<&ContinuityRecord>,
    ) -> Result<(), IdentityRuntimeError> {
        let (Some(bridge), Some(record)) = (self.bridge.as_ref(), continuity) else {
            return Ok(());
        };
        bridge
            .suspend_session_runtime_state(&record.session_id)
            .await
            .map_err(|error| {
                IdentityRuntimeError::Internal(format!(
                    "suspend bridge session runtime state before authority rotation: {error}"
                ))
            })
    }

    async fn resume_bridge_continuity(
        &self,
        identity: &AgentIdentity,
        continuity: Option<&ContinuityRecord>,
        grant: &LeaseGrant,
    ) -> Result<(), IdentityRuntimeError> {
        let (Some(bridge), Some(record)) = (self.bridge.as_ref(), continuity) else {
            return Ok(());
        };
        bridge
            .register_session_runtime_state(
                &record.session_id,
                identity,
                record.generation,
                record.checkpoint_version,
                grant.fencing_token,
            )
            .await
            .map(|_| ())
            .map_err(|error| {
                IdentityRuntimeError::Internal(format!(
                    "resume bridge session runtime state after pre-commit renewal failure: {error}"
                ))
            })
    }

    async fn break_existing_active_grant(
        &self,
        identity: &AgentIdentity,
        grant: &LeaseGrant,
        detail: String,
    ) {
        let retained = {
            let mut entries = self.entries.write().await;
            entries.get_mut(identity).is_some_and(|entry| {
                if entry
                    .lease
                    .as_ref()
                    .is_some_and(|lease| lease.fencing_token == grant.fencing_token)
                {
                    entry.state = IdentityLifecycleState::Broken;
                    entry.lease = None;
                    entry.pending_lease_release = Some(grant.clone());
                    true
                } else {
                    false
                }
            })
        };
        if !retained {
            self.park_unactivated_lease_releases(std::slice::from_ref(grant))
                .await;
        }
        self.mark_bootstrap_from_lifecycle(identity, IdentityLifecycleState::Broken, Some(detail));
        self.emit_event(
            identity,
            IdentityEvent::StateChanged {
                identity: identity.clone(),
                new_state: IdentityLifecycleState::Broken,
            },
        )
        .await;
    }

    async fn commit_staged_active_grant(
        &self,
        identity: &AgentIdentity,
        grant: &LeaseGrant,
        continuity: Option<ContinuityRecord>,
    ) -> Result<(), IdentityRuntimeError> {
        let committed = {
            let mut entries = self.entries.write().await;
            match entries.get_mut(identity) {
                Some(entry)
                    if entry
                        .pending_lease_release
                        .as_ref()
                        .is_some_and(|pending| pending.fencing_token == grant.fencing_token) =>
                {
                    if let Some(record) = continuity {
                        entry.checkpoint_version = record.checkpoint_version;
                        entry.continuity = Some(record);
                    }
                    entry.pending_lease_release = None;
                    entry.lease = Some(Self::lease_entry_from_grant(grant));
                    entry.state = IdentityLifecycleState::Active;
                    Ok(())
                }
                Some(entry) => Err(IdentityRuntimeError::Internal(format!(
                    "staged fencing token {} for {identity} was replaced before commit (state {:?})",
                    grant.fencing_token, entry.state
                ))),
                None => Err(IdentityRuntimeError::UnknownIdentity(identity.clone())),
            }
        };
        if let Err(error) = committed {
            self.park_unactivated_lease_releases(std::slice::from_ref(grant))
                .await;
            return Err(error);
        }
        Ok(())
    }

    async fn publish_active_grant(
        &self,
        identity: &AgentIdentity,
        expected_previous: Option<FencingToken>,
        grant: &LeaseGrant,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        let mut continuity = self
            .stage_active_grant(identity, expected_previous, grant)
            .await?;

        // `ensure_active_lease` suspends before invoking the provider so no
        // stale-token save can overlap the authority rotation. Keep this
        // idempotent suspension here as a defensive boundary for callers that
        // already hold a provider-returned active grant (restore compatibility).
        if let Err(error) = self.suspend_bridge_continuity(continuity.as_ref()).await {
            self.fail_staged_active_grant(identity, grant, error.to_string())
                .await;
            return Err(error);
        }

        if let Some(record) = continuity.as_mut() {
            if let Err(error) = self
                .continuity_store
                .upsert_continuity_record(record, grant.fencing_token)
                .await
            {
                let runtime_error = IdentityRuntimeError::Store(error);
                self.fail_staged_active_grant(
                    identity,
                    grant,
                    format!("active grant continuity publication failed: {runtime_error}"),
                )
                .await;
                return Err(runtime_error);
            }
            if let Some(bridge) = self.bridge.as_ref() {
                match bridge
                    .register_session_runtime_state(
                        &record.session_id,
                        identity,
                        record.generation,
                        record.checkpoint_version,
                        grant.fencing_token,
                    )
                    .await
                {
                    Ok(version) => {
                        record.checkpoint_version = CheckpointVersion::new(
                            record.checkpoint_version.get().max(version.get()),
                        );
                    }
                    Err(error) => {
                        let runtime_error = IdentityRuntimeError::Internal(format!(
                            "bridge refresh session runtime state after active grant publication: {error}"
                        ));
                        let _ = self.suspend_bridge_continuity(Some(record)).await;
                        self.fail_staged_active_grant(identity, grant, runtime_error.to_string())
                            .await;
                        return Err(runtime_error);
                    }
                }
            }
        }

        let session_to_reference = continuity.as_ref().map(|record| record.session_id.clone());
        if let Err(error) = self
            .commit_staged_active_grant(identity, grant, continuity)
            .await
        {
            if let (Some(bridge), Some(session_id)) =
                (self.bridge.as_ref(), session_to_reference.as_ref())
            {
                let _ = bridge.suspend_session_runtime_state(session_id).await;
            }
            return Err(error);
        }
        self.lease_renewal_notify.notify_one();
        self.emit_event(
            identity,
            IdentityEvent::LeaseUpdated {
                identity: identity.clone(),
                fencing_token: grant.fencing_token,
            },
        )
        .await;
        Ok(grant.fencing_token)
    }

    async fn ensure_active_lease(
        &self,
        identity: &AgentIdentity,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        let (grant, continuity) = {
            let entries = self.entries.read().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            let lease = match &entry.lease {
                Some(lease) if lease.is_healthy() => return Ok(lease.fencing_token),
                Some(lease) => lease,
                None => return Err(IdentityRuntimeError::NoActiveLease(identity.clone())),
            };
            (
                LeaseGrant {
                    identity: identity.clone(),
                    fencing_token: lease.fencing_token,
                    ttl: lease.ttl,
                },
                entry.continuity.clone(),
            )
        };

        // Drain every already-admitted persistence mutation before asking the
        // provider to rotate N to N+1. Once this returns, all session-store
        // mutations fail closed until register_session_runtime_state publishes
        // the exact replacement token.
        if let Err(suspend_error) = self.suspend_bridge_continuity(continuity.as_ref()).await {
            if let Err(resume_error) = self
                .resume_bridge_continuity(identity, continuity.as_ref(), &grant)
                .await
            {
                self.break_existing_active_grant(
                    identity,
                    &grant,
                    format!("{suspend_error}; bridge rollback failed: {resume_error}"),
                )
                .await;
                return Err(IdentityRuntimeError::Internal(format!(
                    "{suspend_error}; bridge rollback failed: {resume_error}"
                )));
            }
            return Err(suspend_error);
        }

        let renewed = match self
            .lease_provider
            .renew_leases(std::slice::from_ref(&grant))
            .await
        {
            Ok(renewed) => renewed,
            Err(error) => {
                let runtime_error = IdentityRuntimeError::Lease(error);
                if let Err(resume_error) = self
                    .resume_bridge_continuity(identity, continuity.as_ref(), &grant)
                    .await
                {
                    self.break_existing_active_grant(
                        identity,
                        &grant,
                        format!("{runtime_error}; bridge rollback failed: {resume_error}"),
                    )
                    .await;
                    return Err(IdentityRuntimeError::Internal(format!(
                        "{runtime_error}; bridge rollback failed: {resume_error}"
                    )));
                }
                return Err(runtime_error);
            }
        };
        let renewed_grant = match renewed.get(identity) {
            Some(super::types::LeaseRenewResult::Renewed(grant)) => grant.clone(),
            Some(super::types::LeaseRenewResult::Lost { .. }) | None => {
                self.mark_lease_lost(identity).await?;
                return Err(IdentityRuntimeError::LeaseLost(identity.clone()));
            }
        };

        self.publish_active_grant(identity, Some(grant.fencing_token), &renewed_grant)
            .await
    }

    async fn mark_lifecycle_in_progress(
        &self,
        identity: &AgentIdentity,
        state: IdentityLifecycleState,
    ) -> Result<IdentityEntry, IdentityRuntimeError> {
        let mut entries = self.entries.write().await;
        let entry = entries
            .get_mut(identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
        let snapshot = entry.clone();
        entry.state = state;
        entry.lease = None;
        drop(entries);
        self.mark_bootstrap_materialization_started(identity, None);
        Ok(snapshot)
    }

    async fn restore_entry(&self, identity: &AgentIdentity, entry: IdentityEntry) {
        let state = entry.state;
        self.entries.write().await.insert(identity.clone(), entry);
        self.mark_bootstrap_from_lifecycle(identity, state, None);
    }

    async fn restore_entry_with_grant(
        &self,
        identity: &AgentIdentity,
        mut entry: IdentityEntry,
        grant: &LeaseGrant,
    ) {
        let restore_live_lease = entry.state == IdentityLifecycleState::Active;
        if let Some(record) = entry.continuity.as_ref() {
            if let Err(err) = self
                .continuity_store
                .upsert_continuity_record(record, grant.fencing_token)
                .await
            {
                tracing::warn!(
                    %identity,
                    error = %err,
                    "failed to advance restored continuity fencing token after lifecycle failure"
                );
                entry.state = IdentityLifecycleState::Broken;
            } else if let Some(bridge) = self.bridge.as_ref()
                && let Err(err) = bridge
                    .register_session_runtime_state(
                        &record.session_id,
                        identity,
                        record.generation,
                        record.checkpoint_version,
                        grant.fencing_token,
                    )
                    .await
            {
                tracing::warn!(
                    %identity,
                    error = %err,
                    "failed to refresh restored session runtime state after lifecycle failure"
                );
                entry.state = IdentityLifecycleState::Broken;
            }
        }
        // Preserve ownership for an originally Active entry even if bridge
        // repair marks it Broken; repair still needs the current fenced
        // grant. Originally non-Active entries must not retain a lease that
        // their restored local state does not expose.
        let retain_grant = restore_live_lease;
        entry.lease = retain_grant.then(|| Self::lease_entry_from_grant(grant));
        if !retain_grant {
            entry.pending_lease_release = Some(grant.clone());
            match self
                .lease_provider
                .release_leases(std::slice::from_ref(grant))
                .await
            {
                Ok(()) => entry.pending_lease_release = None,
                Err(err) => {
                    tracing::warn!(
                        %identity,
                        error = %err,
                        "failed to release lifecycle rollback lease for non-active identity; exact grant parked for retry"
                    );
                    entry.state = IdentityLifecycleState::Broken;
                }
            }
        }
        self.restore_entry(identity, entry).await;
        if retain_grant {
            self.lease_renewal_notify.notify_one();
        }
    }

    /// Reuse an already-active embodiment during roster reconciliation.
    ///
    /// The lifecycle reservation held by the restore controller makes this
    /// snapshot stable. Crucially, this path preserves the exact live lease
    /// grant instead of reacquiring/rotating authority for a member that is
    /// already running.
    pub(crate) async fn reuse_active_restore_state(
        &self,
        spec: &DurableAgentSpec,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        // The restore controller still owns this identity's lifecycle guard,
        // so validate time-sensitive external authority before snapshotting or
        // mutating the local projection. Healthy grants return unchanged;
        // due grants publish their exact renewal, and Lost authority marks the
        // identity Broken instead of reporting a successful active reconcile.
        self.ensure_active_lease(&spec.identity).await?;

        let mut entries = self.entries.write().await;
        let entry = entries
            .get_mut(&spec.identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(spec.identity.clone()))?;
        if entry.state != IdentityLifecycleState::Active {
            return Err(IdentityRuntimeError::InvalidState {
                identity: spec.identity.clone(),
                state: entry.state,
                operation: "reuse active restore state",
            });
        }
        if entry.lease.is_none() {
            return Err(IdentityRuntimeError::NoActiveLease(spec.identity.clone()));
        }
        let record = entry.continuity.clone().ok_or_else(|| {
            IdentityRuntimeError::Internal(format!(
                "active identity {} has no continuity record",
                spec.identity
            ))
        })?;
        entry.spec = spec.clone();
        Ok(record)
    }

    /// Publish a fail-closed local state and relinquish the fresh external
    /// grant acquired for the failed lifecycle transaction.
    ///
    /// These ambiguous failure paths deliberately publish Broken with no local
    /// lease. Keeping the matching provider grant alive would make that
    /// projection a lie and could block another runtime from repairing the
    /// identity until the provider TTL expires (forever for the bundled
    /// single-process provider).
    async fn restore_broken_entry_and_release_grant(
        &self,
        identity: &AgentIdentity,
        mut entry: IdentityEntry,
        grant: &LeaseGrant,
    ) {
        entry.state = IdentityLifecycleState::Broken;
        entry.lease = None;
        // Park the exact grant before the provider call. If release fails (or
        // this future is abandoned after publication), reconcile/shutdown can
        // retry the same fencing token instead of leaving an invisible owner.
        entry.pending_lease_release = Some(grant.clone());
        self.restore_entry(identity, entry).await;
        match self
            .lease_provider
            .release_leases(std::slice::from_ref(grant))
            .await
        {
            Ok(()) => {
                let mut entries = self.entries.write().await;
                if let Some(entry) = entries.get_mut(identity)
                    && entry
                        .pending_lease_release
                        .as_ref()
                        .is_some_and(|pending| pending.fencing_token == grant.fencing_token)
                {
                    entry.pending_lease_release = None;
                }
            }
            Err(err) => {
                self.mark_bootstrap_from_lifecycle(
                    identity,
                    IdentityLifecycleState::Broken,
                    Some(format!("pending lease release: {err}")),
                );
                tracing::warn!(
                    %identity,
                    error = %err,
                    "failed to release lease after lifecycle transaction became Broken; exact grant parked for retry"
                );
            }
        }
    }

    async fn restore_broken_entry_with_fenced_store(
        &self,
        identity: &AgentIdentity,
        mut entry: IdentityEntry,
        grant: &LeaseGrant,
    ) {
        entry.state = IdentityLifecycleState::Broken;
        entry.lease = None;
        if let Some(record) = entry.continuity.as_ref()
            && let Err(err) = self
                .continuity_store
                .upsert_continuity_record(record, grant.fencing_token)
                .await
        {
            tracing::warn!(
                %identity,
                error = %err,
                "failed to preserve fenced continuity record for broken identity"
            );
        }
        self.restore_broken_entry_and_release_grant(identity, entry, grant)
            .await;
    }

    /// A failed reset rollback means the caller's pre-reset entry is no
    /// longer authoritative. Re-resolve under the lifecycle transaction and
    /// publish only the store's actual record; if the store itself cannot be
    /// read, fail closed without continuity rather than claiming the old
    /// generation still owns the durable row.
    async fn restore_broken_entry_from_authoritative_continuity(
        &self,
        identity: &AgentIdentity,
        mut entry: IdentityEntry,
        grant: &LeaseGrant,
    ) {
        let authoritative = self
            .continuity_store
            .resolve_many(std::slice::from_ref(identity))
            .await;
        entry.continuity = match authoritative {
            Ok(resolved) => match resolved.get(identity) {
                Some(super::types::ContinuityResolveState::Ready { record }) => {
                    Some(record.clone())
                }
                Some(super::types::ContinuityResolveState::Broken { failure }) => {
                    failure.record.clone()
                }
                Some(super::types::ContinuityResolveState::Uninitialized) | None => None,
            },
            Err(error) => {
                tracing::warn!(
                    %identity,
                    error = %error,
                    "failed to resolve authoritative continuity after reset rollback failure"
                );
                None
            }
        };
        entry.checkpoint_version = entry
            .continuity
            .as_ref()
            .map(|record| record.checkpoint_version)
            .unwrap_or(CheckpointVersion::new(0));
        self.restore_broken_entry_and_release_grant(identity, entry, grant)
            .await;
    }

    async fn mark_rebind_failure_broken(
        &self,
        identity: &AgentIdentity,
        mut entry: IdentityEntry,
        grant: &LeaseGrant,
        rebound_record: &ContinuityRecord,
    ) {
        entry.state = IdentityLifecycleState::Broken;
        entry.lease = None;
        entry.checkpoint_version = rebound_record.checkpoint_version;
        entry.continuity = Some(rebound_record.clone());
        if let Err(err) = self
            .continuity_store
            .upsert_continuity_record(rebound_record, grant.fencing_token)
            .await
        {
            tracing::warn!(
                %identity,
                session_id = %rebound_record.session_id,
                error = %err,
                "failed to preserve rebound continuity after live respawn rebind failure"
            );
        }
        self.restore_broken_entry_and_release_grant(identity, entry, grant)
            .await;
    }

    async fn restore_entry_after_reset_bridge_failure(
        &self,
        identity: &AgentIdentity,
        expected_attempt: &ContinuityRecord,
        entry: IdentityEntry,
        grant: &LeaseGrant,
        force_broken: bool,
    ) -> Option<ContinuityStoreError> {
        let rollback_error = self
            .continuity_store
            .rollback_continuity_record(
                expected_attempt,
                entry.continuity.as_ref(),
                grant.fencing_token,
            )
            .await
            .err();
        if rollback_error.is_some() {
            self.restore_broken_entry_from_authoritative_continuity(identity, entry, grant)
                .await;
        } else if force_broken {
            self.restore_broken_entry_and_release_grant(identity, entry, grant)
                .await;
        } else {
            self.restore_entry_with_grant(identity, entry, grant).await;
        }
        rollback_error
    }

    async fn restore_continuity_after_materialize_failure(
        &self,
        identity: &AgentIdentity,
        previous: Option<&ContinuityRecord>,
        grant: &LeaseGrant,
    ) -> Option<ContinuityStoreError> {
        match previous {
            Some(record) => self
                .continuity_store
                .upsert_continuity_record(record, grant.fencing_token)
                .await
                .err(),
            None => self
                .continuity_store
                .delete_continuity_record(identity, grant.fencing_token)
                .await
                .err(),
        }
    }

    async fn unregister_bridge_session_runtime_states(
        bridge: &dyn SessionBridge,
        session_ids: &[SessionId],
    ) -> Option<String> {
        let mut errors = Vec::new();
        let mut seen = BTreeSet::new();
        for session_id in session_ids {
            if !seen.insert(session_id.to_string()) {
                continue;
            }
            if let Err(err) = bridge.unregister_session_runtime_state(session_id).await {
                errors.push(format!("{session_id}: {err}"));
            }
        }
        (!errors.is_empty()).then(|| errors.join("; "))
    }

    async fn advance_existing_continuity_fence(
        &self,
        identity: &AgentIdentity,
        entry: &IdentityEntry,
        grant: &LeaseGrant,
    ) -> Result<(), IdentityRuntimeError> {
        if let Some(record) = entry.continuity.as_ref() {
            self.continuity_store
                .upsert_continuity_record(record, grant.fencing_token)
                .await
                .map_err(IdentityRuntimeError::Store)?;
        }
        let _ = identity;
        Ok(())
    }

    async fn refresh_existing_session_runtime_state(
        &self,
        identity: &AgentIdentity,
        record: &ContinuityRecord,
        grant: &LeaseGrant,
    ) -> Result<CheckpointVersion, IdentityRuntimeError> {
        let Some(bridge) = self.bridge.as_ref() else {
            return Ok(record.checkpoint_version);
        };
        bridge
            .register_session_runtime_state(
                &record.session_id,
                identity,
                record.generation,
                record.checkpoint_version,
                grant.fencing_token,
            )
            .await
            .map_err(|err| {
                IdentityRuntimeError::Internal(format!(
                    "bridge refresh session runtime state: {err}"
                ))
            })
    }

    // -----------------------------------------------------------------------
    // Delivery: send() — REQ-01, REQ-03
    // -----------------------------------------------------------------------

    /// Send conversational content and return only after the runtime has
    /// COMMITTED this exact turn's terminal boundary.
    ///
    /// Same body, same enforcement, same memory/defang/alias handling as
    /// [`Self::send`]; the single difference is the bridge verb. Use this when
    /// the caller needs PROOF the turn finished rather than proof it was
    /// admitted.
    ///
    /// Why this exists rather than "send, then watch the event stream": a
    /// session-wide `RunCompleted`/`RunFailed` cannot authorize a specific
    /// turn, because queued turns share a `session_id`. Waiting on one means
    /// some OTHER turn's terminal can satisfy the wait, so a test built that
    /// way can pass while the behaviour it claims to prove is broken. A timer
    /// is worse still: it elapses whether the turn succeeded, failed closed, or
    /// never ran at all.
    ///
    /// FAILS CLOSED. If no bridge is installed, or the identity has no bound
    /// runtime id, or the bridge is ingress-only, this returns a typed error.
    /// It never silently degrades to the admission-only path - that would
    /// report success for a turn whose outcome is unknown.
    pub async fn send_awaiting_commit(
        &self,
        identity: &AgentIdentity,
        content: &meerkat_core::ContentInput,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.send_awaiting_commit_with_mode(identity, content, HandlingMode::Queue)
            .await
    }

    /// [`Self::send_awaiting_commit`] with an explicit turn handling mode.
    pub async fn send_awaiting_commit_with_mode(
        &self,
        identity: &AgentIdentity,
        content: &meerkat_core::ContentInput,
        handling_mode: HandlingMode,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.send_awaiting_commit_with_mode_and_interaction(identity, content, handling_mode, None)
            .await
    }

    /// [`Self::send_awaiting_commit`] carrying a host-minted interaction id, so
    /// the completed turn can be looked up EXACTLY afterwards by the identity
    /// the caller stamped on it - rather than by "the last request", by a
    /// request count, or by polling content, none of which name one turn.
    pub async fn send_awaiting_commit_with_mode_and_interaction(
        &self,
        identity: &AgentIdentity,
        content: &meerkat_core::ContentInput,
        handling_mode: HandlingMode,
        interaction_id: Option<&str>,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.send_core(
            identity,
            SendRequest {
                expected_alias: None,
                content,
                system_prompt: None,
                handling_mode,
                interaction_id,
                commit_mode: SendCommitMode::AwaitCommit,
                console_human: None,
            },
        )
        .await
        .map(|outcome| outcome.token)
    }

    /// [`Self::send_awaiting_commit`] carrying one ordinary System message
    /// authored for THIS exact turn (meerkat 0.8.11 `WorkSpec::system_prompt`,
    /// appended at the turn's admitted transcript boundary).
    ///
    /// Per-turn content, never member or session configuration - the same
    /// carrier the ingress authored path uses, on the lane that can prove the
    /// turn finished.
    pub async fn send_awaiting_commit_with_system_prompt(
        &self,
        identity: &AgentIdentity,
        content: &meerkat_core::ContentInput,
        system_prompt: Option<&str>,
        handling_mode: HandlingMode,
        interaction_id: Option<&str>,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.send_core(
            identity,
            SendRequest {
                expected_alias: None,
                content,
                system_prompt,
                handling_mode,
                interaction_id,
                commit_mode: SendCommitMode::AwaitCommit,
                console_human: None,
            },
        )
        .await
        .map(|outcome| outcome.token)
    }

    /// Send conversational content to an addressable identity.
    ///
    /// Enforces:
    /// - Identity must be registered and active
    /// - Identity must be Addressable (REQ-03)
    /// - Lease must be held (INV-01)
    /// - Lease must not be lost (INV-02)
    ///
    /// Returns the fencing token for the delivery (caller uses it for checkpoint).
    pub async fn send(
        &self,
        identity: &AgentIdentity,
        content: &meerkat_core::ContentInput,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.send_with_mode(identity, content, HandlingMode::Queue)
            .await
    }

    /// Cancellation-safe queue send for RPC/host request boundaries.
    pub async fn send_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        content: &meerkat_core::ContentInput,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.send_with_mode_tracked(identity, content, HandlingMode::Queue)
            .await
    }

    /// Send conversational content using an explicit turn handling mode.
    ///
    /// This is the identity-first counterpart to the mob member send path used
    /// by the console. Ordinary API callers can keep using [`Self::send`],
    /// which preserves queue semantics.
    pub async fn send_with_mode(
        &self,
        identity: &AgentIdentity,
        content: &meerkat_core::ContentInput,
        handling_mode: HandlingMode,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.send_with_mode_and_interaction(identity, content, handling_mode, None)
            .await
    }

    /// Cancellation-safe send that also returns the completion baseline to
    /// wait past.
    ///
    /// This is the delivery entry point every surface should use when the
    /// caller intends to wait for the answer: it is the only one that hands
    /// back a [`CompletionCursor`] captured before delivery, which is what
    /// makes "wait for MY turn" expressible without comparing output text.
    /// `expected_alias` pins the delivery to a generated runtime alias the
    /// caller already resolved; pass `None` for the durable identity.
    pub async fn send_admission_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        content: &meerkat_core::ContentInput,
        handling_mode: HandlingMode,
        interaction_id: Option<&str>,
    ) -> Result<SendAdmission, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.map(ToString::to_string);
        let content = content.clone();
        let interaction_id = interaction_id.map(ToString::to_string);
        self.run_tracked_foreground(async move {
            runtime
                .send_with_mode_and_interaction_with_expected_member_alias(
                    &identity,
                    expected_alias.as_deref(),
                    &content,
                    handling_mode,
                    interaction_id.as_deref(),
                )
                .await
                .map(|(fencing_token, completion_baseline)| SendAdmission {
                    fencing_token,
                    completion_baseline,
                })
        })
        .await
    }

    /// Cancellation-safe send whose admitted turn is tracked by ticket.
    ///
    /// The same delivery as [`Self::send_admission_tracked`] (same admission,
    /// same interaction id, same session reconciliation, same completion
    /// baseline), returning once the work is admitted. The runtime keeps the
    /// admitted turn's own completion handle under a [`TurnTicket`] minted for
    /// this admission alone; [`Self::turn_outcome`] and
    /// [`Self::wait_for_turn`] then report THAT turn and its own output. No
    /// other delivery to the identity (a peer message, a scheduled turn, a
    /// fork completion wake, another send reusing the interaction id) can
    /// satisfy them.
    ///
    /// When the turn cannot be tracked ([`TurnUntrackable`]: a member whose
    /// LIVE runtime mode is `autonomous_host`, an externally bound member, a
    /// bridge that cannot report per-turn output), it is delivered exactly
    /// once, exactly like [`Self::send_admission_tracked`], and
    /// [`Ticketed::turn`] says why; only [`TurnUntrackable::NotDelivered`]
    /// (no bridge or no bound runtime) delivers nothing.
    pub async fn send_with_turn_ticket(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        content: &meerkat_core::ContentInput,
        handling_mode: HandlingMode,
        interaction_id: Option<&str>,
    ) -> Result<Ticketed<SendAdmission>, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.map(ToString::to_string);
        let content = content.clone();
        let interaction_id = interaction_id.map(ToString::to_string);
        self.run_tracked_foreground(async move {
            let outcome = runtime
                .send_core(
                    &identity,
                    SendRequest {
                        expected_alias: expected_alias.as_deref(),
                        content: &content,
                        system_prompt: None,
                        handling_mode,
                        interaction_id: interaction_id.as_deref(),
                        commit_mode: SendCommitMode::TrackTurn,
                        console_human: None,
                    },
                )
                .await?;
            Ok(Ticketed {
                admission: SendAdmission {
                    fencing_token: outcome.token,
                    completion_baseline: outcome.completion_baseline,
                },
                turn: outcome.turn.unwrap_or_else(untracked_delivery),
            })
        })
        .await
    }

    /// Where the turn admitted under `ticket` for `identity` stands. A ticket
    /// of another identity, or one this runtime never admitted (or has
    /// forgotten), is [`TurnOutcome::Unknown`].
    pub fn turn_outcome(&self, identity: &AgentIdentity, ticket: TurnTicket) -> TurnOutcome {
        self.turn_outcomes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .outcome(identity, ticket)
    }

    /// Wait until the turn admitted under `ticket` settles, then return its
    /// outcome ([`TurnOutcome::Completed`] carries the turn's own output).
    /// Returns [`TurnOutcome::Unknown`] at once for an unknown ticket, and
    /// [`TurnOutcome::Pending`] if `timeout` elapses first or the runtime
    /// shuts down. See [`Self::wait_for_turn_outcome`] for why it ended.
    pub async fn wait_for_turn(
        &self,
        identity: &AgentIdentity,
        ticket: TurnTicket,
        timeout: Duration,
    ) -> TurnOutcome {
        match self.wait_for_turn_outcome(identity, ticket, timeout).await {
            TurnWait::Settled(outcome)
            | TurnWait::TimedOut(outcome)
            | TurnWait::ShuttingDown(outcome) => outcome,
        }
    }

    /// [`Self::wait_for_turn`], saying how the wait ended.
    ///
    /// Event-driven: the wait sleeps until the ticket registry changes (an
    /// admission or a settlement), or the runtime starts shutting down, and
    /// re-reads it then. `timeout` is only the caller's overall deadline.
    pub async fn wait_for_turn_outcome(
        &self,
        identity: &AgentIdentity,
        ticket: TurnTicket,
        timeout: Duration,
    ) -> TurnWait {
        let deadline = tokio::time::Instant::now() + timeout;
        // Subscribe before the first read so a settlement landing between the
        // read and the wait still wakes it.
        let mut changes = self.turn_outcome_changes.subscribe();
        let mut shutdown = self.foreground_cancel.subscribe();
        loop {
            let outcome = self.turn_outcome(identity, ticket);
            if outcome != TurnOutcome::Pending {
                return TurnWait::Settled(outcome);
            }
            if *shutdown.borrow_and_update() {
                return TurnWait::ShuttingDown(outcome);
            }
            tokio::select! {
                changed = changes.changed() => {
                    if changed.is_err() {
                        return TurnWait::ShuttingDown(self.turn_outcome(identity, ticket));
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        return TurnWait::ShuttingDown(self.turn_outcome(identity, ticket));
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    return TurnWait::TimedOut(self.turn_outcome(identity, ticket));
                }
            }
        }
    }

    /// Record the admitted turn as pending under a freshly minted ticket and
    /// hand `receipt` to a waiter that settles THAT record with the turn's own
    /// outcome. The waiter holds only the outcome registry, never a lock or
    /// the runtime, and ends when the turn reaches its terminal (or its
    /// completion channel closes).
    ///
    /// The outcome is the turn's own, whatever post-admission session
    /// resolution did: a resolution failure is reported on the delivery-error
    /// channel by the caller ([`Self::report_ticketed_resolution_failure`]),
    /// never as a failure of a turn that ran and succeeded.
    fn track_turn(
        &self,
        identity: &AgentIdentity,
        delivery_key: Option<DeliveryKey>,
        receipt: super::bridge::BridgeTurnReceipt,
    ) -> TurnTicket {
        let ticket = self
            .turn_outcomes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .admit(identity, delivery_key);
        // An admission can evict the oldest pending turn at the cap.
        self.turn_outcome_changes.send_replace(());
        let outcomes = Arc::clone(&self.turn_outcomes);
        let changes = Arc::clone(&self.turn_outcome_changes);
        let owner = identity.clone();
        tokio::spawn(async move {
            let outcome = receipt.wait_turn_output().await;
            outcomes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .settle(&owner, ticket, outcome);
            changes.send_replace(());
        });
        ticket
    }

    /// The tracked admission a re-dispatch under `key` is deduplicated onto,
    /// if it is still pending or completed; see
    /// [`TurnOutcomes::reusable_delivery_ticket`].
    fn reusable_delivery_ticket(
        &self,
        identity: &AgentIdentity,
        key: &DeliveryKey,
    ) -> Option<TurnTicket> {
        self.turn_outcomes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reusable_delivery_ticket(identity, key)
    }

    /// A ticketed turn was admitted and is tracked, but projecting its session
    /// failed, so its session is not reconciled. The turn's own outcome is
    /// unaffected (see [`Self::track_turn`]); the resolution failure is
    /// recorded typed on the identity's delivery-error channel, where an
    /// operator reads it, instead of failing the admission or the turn.
    async fn report_ticketed_resolution_failure(
        &self,
        identity: &AgentIdentity,
        ticket: TurnTicket,
        detail: &str,
    ) {
        tracing::warn!(
            identity = %identity,
            ticket = %ticket,
            detail,
            "ticketed turn admitted but its session could not be resolved; the turn is \
             tracked, its session is not reconciled"
        );
        self.record_delivery_error(
            identity,
            &IdentityRuntimeError::PostAdmissionResolutionFailed {
                identity: identity.clone(),
                detail: format!(
                    "ticketed turn {ticket} was admitted and is tracked, but its session could \
                     not be resolved, so it was not reconciled: {detail}"
                ),
            },
        )
        .await;
    }

    /// Cancellation-safe explicit-mode send for RPC/host request boundaries.
    pub async fn send_with_mode_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        content: &meerkat_core::ContentInput,
        handling_mode: HandlingMode,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.send_with_mode_and_interaction_tracked(identity, content, handling_mode, None)
            .await
    }

    /// Cancellation-safe interaction send for RPC/host request boundaries.
    pub async fn send_with_mode_and_interaction_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        content: &meerkat_core::ContentInput,
        handling_mode: HandlingMode,
        interaction_id: Option<&str>,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.send_admission_tracked(identity, None, content, handling_mode, interaction_id)
            .await
            .map(|admission| admission.fencing_token)
    }

    /// Cancellation-safe interaction send pinned to the generated runtime
    /// alias that the caller resolved. Validation and delivery share the
    /// identity lifecycle lock, so a concurrent reset cannot retarget the
    /// request onto the replacement generation.
    pub async fn send_with_mode_and_interaction_member_alias_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: &str,
        content: &meerkat_core::ContentInput,
        handling_mode: HandlingMode,
        interaction_id: Option<&str>,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.send_admission_tracked(
            identity,
            Some(expected_alias),
            content,
            handling_mode,
            interaction_id,
        )
        .await
        .map(|admission| admission.fencing_token)
    }

    /// Console-only dispatch, called ONCE by the owner of a fresh persisted
    /// console reservation. HTTP replays return that original frame/status or
    /// typed unknown fate after failure; they never re-prepare changed memory.
    /// No replay cache (or eviction which could make an old key executable).
    /// Whether a console send to `identity` can carry a native skill
    /// selection: only a local session-backed member (no external binding,
    /// no remote placement) has meerkat's host-human skill carrier. Read
    /// from the durable spec, so a console send can refuse a selection
    /// before it reserves an interaction.
    pub(crate) async fn carries_console_skill_selection(&self, identity: &AgentIdentity) -> bool {
        self.entries
            .read()
            .await
            .get(identity)
            .is_some_and(|entry| {
                !durable_spec_uses_external_binding(&entry.spec) && entry.spec.placement.is_none()
            })
    }

    pub(crate) async fn send_console_human_input_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        content: &meerkat_core::ContentInput,
        handling_mode: HandlingMode,
        skill_references: &[meerkat_core::skills::SkillKey],
        accepted: &crate::console_aggregator::ConsoleInteractionAccepted,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        let delivery_identity = meerkat_mob::MobDeliveryIdentity::new(
            accepted.input_frame_id.clone(),
            accepted.interaction_id.clone(),
        )
        .map_err(|error| {
            IdentityRuntimeError::HostHumanInput(super::bridge::HostHumanInputError::Mob(Box::new(
                error.into(),
            )))
        })?;
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.map(ToString::to_string);
        let content = content.clone();
        let skill_references = skill_references.to_vec();
        let interaction_id = accepted.interaction_id.clone();
        let expected_session = accepted.session_id.clone();
        self.run_tracked_foreground(async move {
            runtime
                .send_core(
                    &identity,
                    SendRequest {
                        expected_alias: expected_alias.as_deref(),
                        content: &content,
                        system_prompt: None,
                        handling_mode,
                        interaction_id: Some(&interaction_id),
                        commit_mode: SendCommitMode::Ingress,
                        console_human: Some(ConsoleHumanSend {
                            delivery_identity: &delivery_identity,
                            expected_session: expected_session.as_deref(),
                            skill_references: &skill_references,
                        }),
                    },
                )
                .await
                .map(|outcome| outcome.token)
        })
        .await
    }

    /// Converge an identity's reachable peer neighbourhood AFTER a queue-mode
    /// send or dispatch to it was admitted.
    ///
    /// Admission never waits for this: hydration materializes peers (member
    /// builds, host callbacks) and reconciles the managed topology (the
    /// topology guard, the reconcile lock and mob wiring calls), none of which
    /// the target's admission needs. meerkat already holds the admitted input
    /// durably and queues it behind any running turn. When the runtime's
    /// owning `Arc` is known the hydration is detached and runtime-tracked
    /// (shutdown joins it at its own commit/rollback boundary); otherwise it
    /// runs inline here, after admission and with no admission lock held.
    /// Best effort either way: a failure is logged, like the materialization
    /// warnings it wraps.
    ///
    /// Concurrent admissions to one target merge: while a hydration for it is
    /// in flight, a new admission only asks it to run once more when it
    /// finishes (so the latest topology is still converged), instead of
    /// stacking another hydration behind the same locks.
    async fn hydrate_peer_topology_after_admission(&self, identity: &AgentIdentity) {
        let Some(token) = self.claim_peer_hydration(identity) else {
            return;
        };
        if let Some(runtime) = self.shared_handle() {
            runtime.spawn_post_admission_peer_hydration(identity.clone(), token);
            return;
        }
        PeerHydrationSlot {
            hydrations: &self.peer_hydrations_in_flight,
            identity: identity.clone(),
            token,
        }
        .run(self)
        .await;
    }

    fn spawn_post_admission_peer_hydration(self: &Arc<Self>, identity: AgentIdentity, token: u64) {
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            let hydrator = Arc::clone(&runtime);
            let owner = identity.clone();
            let _ = runtime
                .run_tracked_foreground(async move {
                    PeerHydrationSlot {
                        hydrations: &hydrator.peer_hydrations_in_flight,
                        identity: owner,
                        token,
                    }
                    .run(&hydrator)
                    .await;
                    Ok::<(), IdentityRuntimeError>(())
                })
                .await
                .inspect_err(|error| {
                    // Refused at shutdown: release this claim (only this
                    // one) so nothing stays marked in flight.
                    runtime
                        .peer_hydrations_in_flight
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .release(&identity, token);
                    tracing::warn!(
                        identity = %identity,
                        %error,
                        "input was admitted; post-admission peer hydration was not run"
                    );
                });
        });
    }

    async fn hydrate_peer_topology_logged(&self, identity: &AgentIdentity) {
        if let Err(error) = self.hydrate_peer_topology(identity).await {
            tracing::warn!(
                identity = %identity,
                %error,
                "input was admitted; post-admission peer hydration failed"
            );
        }
    }

    /// Claim `identity`'s hydration slot (see [`PeerHydrations::claim`]).
    fn claim_peer_hydration(&self, identity: &AgentIdentity) -> Option<u64> {
        self.peer_hydrations_in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .claim(identity)
    }

    /// The topology work an ordinary send used to run before admission:
    /// materialize the reachable peers and reconcile the managed edges, plus
    /// the pending-recovery reconcile that a wake-path materialization
    /// ([`Self::materialize_for_admission`], which reconciles only the
    /// target's own edges) left to it.
    async fn hydrate_peer_topology(
        &self,
        identity: &AgentIdentity,
    ) -> Result<(), IdentityRuntimeError> {
        self.materialize_reachable_peers(identity).await?;
        let desired_edges = self.desired_peer_edges.read().await.clone();
        if desired_edges.is_empty()
            && let Some(controller) = self.topology_controller()
            && controller.has_pending().await
        {
            self.reconcile_managed_peer_edges(&desired_edges).await?;
        }
        Ok(())
    }

    /// [`Self::send_with_mode`] with a host-minted interaction id (meerkat
    /// 0.7.25 ask 15 addendum). The id rides `WorkSpec` into runtime
    /// admission, so the turn's live events and its committed transcript
    /// messages carry the same identity the console stamped on its frames —
    /// the exact live↔history join the console dedup needs. Only UUID-form
    /// ids thread; others are delivered without one.
    pub async fn send_with_mode_and_interaction(
        &self,
        identity: &AgentIdentity,
        content: &meerkat_core::ContentInput,
        handling_mode: HandlingMode,
        interaction_id: Option<&str>,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.send_with_mode_and_interaction_with_expected_member_alias(
            identity,
            None,
            content,
            handling_mode,
            interaction_id,
        )
        .await
        .map(|(token, _)| token)
    }

    async fn send_with_mode_and_interaction_with_expected_member_alias(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        content: &meerkat_core::ContentInput,
        handling_mode: HandlingMode,
        interaction_id: Option<&str>,
    ) -> Result<(FencingToken, CompletionCursor), IdentityRuntimeError> {
        self.send_core(
            identity,
            SendRequest {
                expected_alias,
                content,
                system_prompt: None,
                handling_mode,
                interaction_id,
                commit_mode: SendCommitMode::Ingress,
                console_human: None,
            },
        )
        .await
        .map(|outcome| (outcome.token, outcome.completion_baseline))
    }

    /// The ONE send body. Every lane (ingress, await-commit, ticketed) runs
    /// it; the only difference is which bridge verb the delivery step calls,
    /// which is exactly the part that must not drift. A second copy would let
    /// the completion lanes diverge from the ingress lane on lease
    /// acquisition, alias pinning, memory injection, defanging or session
    /// reconciliation - every one of which is load-bearing and none of which
    /// is visible from a test.
    async fn send_core(
        &self,
        identity: &AgentIdentity,
        request: SendRequest<'_>,
    ) -> Result<SendCoreOutcome, IdentityRuntimeError> {
        // Steers keep their established contract: no topology work at all
        // (background and full-fleet materialization own the peers).
        let hydrate = request.handling_mode != HandlingMode::Steer;
        let result = self.send_core_admit(identity, request).await;
        if hydrate && input_was_admitted(&result) {
            self.hydrate_peer_topology_after_admission(identity).await;
        }
        result
    }

    /// Admission for [`Self::send_core`]. For an already-Active target it
    /// waits only on the target's lifecycle lock, alias validation, its
    /// lease, the bounded memory recall and the bounded bridge admission: it
    /// never hydrates peers, reconciles topology or takes another identity's
    /// lifecycle lock, so a queued input gets meerkat's durable admission
    /// receipt while the member is mid-turn (meerkat queues it behind the
    /// running turn). A Dormant or retired target is woken first, and the
    /// wake reconciles that target's own managed edges, bounded, so its first
    /// turn can reach its peers (see [`Self::materialize_for_admission`]).
    async fn send_core_admit(
        &self,
        identity: &AgentIdentity,
        request: SendRequest<'_>,
    ) -> Result<SendCoreOutcome, IdentityRuntimeError> {
        let SendRequest {
            expected_alias,
            content,
            system_prompt,
            handling_mode,
            interaction_id,
            commit_mode,
            console_human,
        } = request;
        let should_materialize = {
            let entries = self.entries.read().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;

            // REQ-03: reject send to InternalOnly
            if entry.spec.addressability == AgentAddressability::InternalOnly {
                return Err(IdentityRuntimeError::NotAddressable(NotAddressable {
                    identity: identity.clone(),
                    addressability: entry.spec.addressability,
                }));
            }
            // A retired identity (`Retiring`, the terminal form) is
            // re-materialized onto its recorded session exactly like a Dormant
            // one; this is the operator's "retire then send" recovery path.
            matches!(
                entry.state,
                IdentityLifecycleState::Dormant
                    | IdentityLifecycleState::Uninitialized
                    | IdentityLifecycleState::Retiring
            )
        };
        if should_materialize {
            self.materialize_for_admission(identity, expected_alias)
                .await?;
        }

        let lifecycle_lock = self.lifecycle_lock_for(identity).await;
        let _lifecycle_guard = lifecycle_lock.lock().await;
        if let Some(expected_alias) = expected_alias {
            self.ensure_expected_member_alias_current(identity, expected_alias)
                .await?;
        }
        {
            let entries = self.entries.read().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            if entry.state != IdentityLifecycleState::Active {
                return Err(self.refuse_inactive_delivery(identity, entry.state, "send"));
            }
        }

        let mut token = self.ensure_active_lease(identity).await?;
        // Read the completion baseline BEFORE delivery is attempted. The turn
        // this send starts can only complete after this point, so a caller
        // waiting past the baseline cannot miss it. The converse ambiguity is
        // deliberate and documented: on an identity receiving concurrent
        // traffic, another delivery's completion can also satisfy the wait.
        // Waiting too little beats the failure this replaces (waiting forever).
        let completion_baseline = self.credited_completion_cursor(identity, token).await;
        let (
            runtime_id,
            memory_session_key,
            memory_generation,
            bridge_interaction_id,
            memory_runtime_mode,
            local_human,
            human_session,
            external_binding,
        ) = {
            let entries = self.entries.read().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            (
                entry
                    .continuity
                    .as_ref()
                    .map(|c| c.agent_runtime_id.clone()),
                // Scopes the injector's cross-turn dedup + cumulative budget.
                entry.continuity.as_ref().map(|c| c.session_id.to_string()),
                entry.continuity.as_ref().map(|c| c.generation.get()),
                interaction_id_for_delivery(&entry.spec, interaction_id),
                // Resolved as meerkat resolves it at spawn (override, profile,
                // default). Generic autonomous work has no user context slot;
                // the explicit human boundary does.
                self.effective_runtime_mode(&entry.spec),
                console_human.is_some() && !durable_spec_uses_external_binding(&entry.spec),
                entry.continuity.as_ref().map(|c| c.session_id.clone()),
                durable_spec_uses_external_binding(&entry.spec),
            )
        };
        if !local_human
            && console_human
                .as_ref()
                .is_some_and(|human| !human.skill_references.is_empty())
        {
            // An externally bound identity has no native member-turn skill
            // carrier; refuse before delivery instead of running the turn
            // without the selected skills.
            return Err(IdentityRuntimeError::HostHumanInput(
                super::bridge::HostHumanInputError::Unsupported(
                    "selected skills require a local session-backed member".to_string(),
                ),
            ));
        }
        if local_human
            && let Some(expected_session) = console_human
                .as_ref()
                .and_then(|human| human.expected_session)
            && human_session.as_ref().map(ToString::to_string).as_deref() != Some(expected_session)
        {
            return Err(IdentityRuntimeError::HostHumanInput(
                super::bridge::HostHumanInputError::BindingChanged,
            ));
        }
        let (content_to_deliver, injected_context) = self
            .prepare_member_delivery(
                identity,
                content,
                memory_session_key.as_deref(),
                memory_generation,
                handling_mode == HandlingMode::Steer,
                if local_human {
                    DeliveryPreparation::ConsoleHuman
                } else {
                    DeliveryPreparation::Work(memory_runtime_mode)
                },
            )
            .await?;

        // Deliver through the session bridge when available.
        //
        // AwaitCommit FAILS CLOSED here and nowhere else: if there is no bridge
        // or no runtime id, the ingress lane legitimately no-ops, but the
        // completion lane must NOT - silently taking the ingress path would
        // return success for a turn that was never awaited, which is the exact
        // false-success this API exists to remove.
        if (commit_mode == SendCommitMode::AwaitCommit || local_human)
            && (self.bridge.is_none() || runtime_id.is_none())
        {
            if local_human {
                return Err(IdentityRuntimeError::HostHumanInput(
                    super::bridge::HostHumanInputError::BindingChanged,
                ));
            }
            return Err(IdentityRuntimeError::CompletionUnavailable {
                identity: identity.clone(),
                reason: if self.bridge.is_none() {
                    "no session bridge is installed on this runtime".to_string()
                } else {
                    "the identity has no bound agent runtime id".to_string()
                },
            });
        }
        // Whether a tracked send can be tracked is decided here, from typed
        // state, before anything is delivered: a turn that cannot be tracked
        // is delivered on the ingress lane and reported as such, never
        // attempted on one lane and retried on the other.
        let mut turn_tracking = None;
        let lane = match commit_mode {
            SendCommitMode::Ingress => DeliveryLane::Ingress,
            SendCommitMode::AwaitCommit => DeliveryLane::AwaitCommit,
            SendCommitMode::TrackTurn => match turn_untrackable(
                self.bridge.is_some() && runtime_id.is_some(),
                external_binding,
                local_human,
                self.bridge
                    .as_ref()
                    .is_some_and(|bridge| bridge.tracks_turn_output()),
            ) {
                None => DeliveryLane::Ticketed,
                Some(reason) => {
                    turn_tracking = Some(TurnTracking::Unavailable(reason));
                    DeliveryLane::Ingress
                }
            },
        };
        if let (Some(bridge), Some(rid)) = (&self.bridge, &runtime_id) {
            // Validated -> Admitted(receipt). A tracked send is admitted
            // before the lane dispatch below, under the lock like every lane,
            // so meerkat's typed pre-admission refusal (the member's ACTUAL
            // runtime mode refuses per-turn completion: a profile whose
            // runtime_mode changed without a respawn) can still take the
            // ingress lane. Nothing was delivered by the refused attempt, so
            // the send is still delivered exactly once.
            let step = match lane {
                DeliveryLane::Ingress => DeliveryStep::Ingress,
                DeliveryLane::AwaitCommit => DeliveryStep::AwaitCommit,
                DeliveryLane::Ticketed => {
                    let mut delivery = super::bridge::BridgeDelivery::new(
                        content_to_deliver.clone(),
                        handling_mode,
                    );
                    delivery.system_prompt = system_prompt.map(ToString::to_string);
                    delivery.injected_context = injected_context.clone();
                    delivery.interaction_id = bridge_interaction_id.map(ToString::to_string);
                    // One automatic non-destructive reload on a typed
                    // reload-required refusal, then the typed failure (same
                    // rule as the other lanes).
                    let mut reload_attempted = false;
                    loop {
                        let attempt = match bridge
                            .begin_delivery_with_output(rid, delivery.clone())
                            .await
                        {
                            Err(BridgeAdmissionError::UnsupportedForMode {
                                mode, detail, ..
                            }) => {
                                turn_tracking =
                                    Some(runtime_refused_tracking(identity, mode, &detail));
                                break DeliveryStep::Ingress;
                            }
                            attempt => attempt.map_err(|err| admission_phase_error(identity, err)),
                        };
                        match attempt {
                            Ok(receipt) => {
                                self.record_delivery_success(identity).await;
                                break DeliveryStep::Tracked(receipt);
                            }
                            Err(err) => {
                                self.record_delivery_error(identity, &err).await;
                                if !reload_attempted
                                    && matches!(err, IdentityRuntimeError::ReloadRequired { .. })
                                {
                                    reload_attempted = true;
                                    if let Err(reload_error) =
                                        self.reload_for_delivery_locked(identity, &err).await
                                    {
                                        self.record_delivery_error(identity, &reload_error).await;
                                        return Err(reload_error);
                                    }
                                    token = self.ensure_active_lease(identity).await?;
                                    continue;
                                }
                                return Err(err);
                            }
                        }
                    }
                }
            };
            let delivered_session_id = match step {
                // Validated -> Delivered. One await, all of it bounded, all of
                // it under the lock. Unchanged.
                DeliveryStep::Ingress => {
                    // One automatic non-destructive reload on a typed
                    // reload-required refusal, then the typed failure. Never a
                    // loop: a second refusal after a reload is an operator
                    // matter.
                    let mut reload_attempted = false;
                    loop {
                        let attempt = if local_human {
                            let session = human_session.as_ref().ok_or(
                                IdentityRuntimeError::HostHumanInput(
                                    super::bridge::HostHumanInputError::BindingChanged,
                                ),
                            )?;
                            let mut delivery = super::bridge::BridgeDelivery::new(
                                content_to_deliver.clone(),
                                handling_mode,
                            );
                            delivery.system_prompt = system_prompt.map(ToString::to_string);
                            delivery.injected_context = injected_context.clone();
                            delivery.interaction_id =
                                bridge_interaction_id.map(ToString::to_string);
                            delivery.delivery_identity = console_human
                                .as_ref()
                                .map(|human| human.delivery_identity.clone());
                            delivery.skill_references = console_human
                                .as_ref()
                                .map(|human| human.skill_references.to_vec())
                                .unwrap_or_default();
                            bridge
                                .deliver_host_human_input(rid, session, delivery)
                                .await
                        } else {
                            // External delivery keeps its established wire semantics.
                            // It is not a claim of local canonical human authorship.
                            bridge
                                .deliver_with_mode_context_and_system_prompt(
                                    rid,
                                    &content_to_deliver,
                                    system_prompt,
                                    &injected_context,
                                    handling_mode,
                                    bridge_interaction_id,
                                )
                                .await
                        }
                        .map_err(|e| ingress_phase_error(identity, e));
                        match attempt {
                            Ok(delivered) => {
                                self.record_delivery_success(identity).await;
                                break delivered;
                            }
                            Err(err) => {
                                self.record_delivery_error(identity, &err).await;
                                if !reload_attempted
                                    && matches!(err, IdentityRuntimeError::ReloadRequired { .. })
                                {
                                    reload_attempted = true;
                                    if let Err(reload_error) =
                                        self.reload_for_delivery_locked(identity, &err).await
                                    {
                                        // The reload's own typed verdict (refused,
                                        // timed out, failed) is what the operator
                                        // sees, so it is the recorded class too.
                                        self.record_delivery_error(identity, &reload_error).await;
                                        return Err(reload_error);
                                    }
                                    token = self.ensure_active_lease(identity).await?;
                                    continue;
                                }
                                return Err(err);
                            }
                        }
                    }
                }

                // Admitted(receipt) -> Tracked -> Delivered. The admission and
                // its bounded session resolution ran under the lock exactly
                // like the ingress lane, and the send returns at the same
                // point. The turn is NOT awaited here: its receipt moves to a
                // waiter that records the turn's own output under the ticket.
                DeliveryStep::Tracked(receipt) => {
                    let resolved_session = receipt.resolved_session().cloned();
                    let resolution_error =
                        receipt.session_resolution_error().map(ToString::to_string);
                    // Recorded before the send returns, so the caller's first
                    // look at the ticket never reads `Unknown`.
                    let ticket = self.track_turn(identity, None, receipt);
                    turn_tracking = Some(TurnTracking::Tracked(ticket));
                    match resolved_session {
                        Some(session_id) => session_id,
                        // The turn is admitted and tracked, and its outcome is
                        // its own; the resolution failure is reported on its
                        // own channel. There is no session to reconcile onto.
                        None => {
                            self.report_ticketed_resolution_failure(
                                identity,
                                ticket,
                                resolution_error
                                    .as_deref()
                                    .unwrap_or("no session was resolved"),
                            )
                            .await;
                            return Ok(SendCoreOutcome {
                                token,
                                completion_baseline,
                                turn: turn_tracking,
                            });
                        }
                    }
                }

                // Validated -> Admitted(receipt) -> [UNLOCK] -> Terminal ->
                // [RELOCK] -> Revalidated -> Reconciled | Superseded.
                //
                // The lock covers admission and bounded session resolution and
                // NOTHING else. Awaiting an LLM turn under it would serialise
                // same-identity sends behind a model call and block every
                // lifecycle operation - reset, retire, alias rebind - for the
                // turn's whole duration.
                DeliveryStep::AwaitCommit => {
                    // ADMITTED. Bounded; still under the lock. One automatic
                    // non-destructive reload on a typed reload-required
                    // refusal, then the typed failure (same rule as the
                    // ingress lane).
                    let mut reload_attempted = false;
                    let receipt = loop {
                        let attempt = bridge
                            .begin_awaiting_commit(
                                rid,
                                &content_to_deliver,
                                system_prompt,
                                &injected_context,
                                handling_mode,
                                bridge_interaction_id,
                            )
                            .await
                            .map_err(|err| admission_phase_error(identity, err));
                        match attempt {
                            Ok(receipt) => {
                                self.record_delivery_success(identity).await;
                                break receipt;
                            }
                            Err(err) => {
                                self.record_delivery_error(identity, &err).await;
                                if !reload_attempted
                                    && matches!(err, IdentityRuntimeError::ReloadRequired { .. })
                                {
                                    reload_attempted = true;
                                    if let Err(reload_error) =
                                        self.reload_for_delivery_locked(identity, &err).await
                                    {
                                        // The reload's own typed verdict (refused,
                                        // timed out, failed) is what the operator
                                        // sees, so it is the recorded class too.
                                        self.record_delivery_error(identity, &reload_error).await;
                                        return Err(reload_error);
                                    }
                                    token = self.ensure_active_lease(identity).await?;
                                    continue;
                                }
                                return Err(err);
                            }
                        }
                    };

                    // CAPTURE the incarnation we admitted onto, so a reset,
                    // retire or alias rebind during the unlocked window is
                    // detectable rather than silently reconciled onto.
                    let captured = self.capture_incarnation(identity).await?;

                    // UNLOCK. Nothing is held from here.
                    drop(_lifecycle_guard);

                    // TERMINAL. The LLM turn. The only long step.
                    let terminal = receipt.wait().await;

                    // RELOCK.
                    let _relock_guard = lifecycle_lock.lock().await;

                    // DETECT supersede WITHOUT returning on it. What the turn
                    // did and whether its embodiment still exists are
                    // INDEPENDENT axes, so neither may swallow the other. A
                    // post-lock read failure IS a supersede - the identity was
                    // retired or deleted while the turn ran - and must never
                    // surface as a pre-admission-shaped UnknownIdentity, which
                    // would say nothing was delivered.
                    let supersede: Option<String> = match self.capture_incarnation(identity).await {
                        Err(IdentityRuntimeError::UnknownIdentity(_)) => {
                            Some("the identity no longer exists".to_string())
                        }
                        Err(other) => Some(format!("the identity could not be re-read: {other}")),
                        Ok(current) if current != captured => {
                            Some(format!("admitted onto {captured:?}, now {current:?}"))
                        }
                        // Re-run the ORIGINAL expected alias through the
                        // authoritative check rather than comparing a copy
                        // of it. Any failure is a POST-admission supersede.
                        Ok(_) => match expected_alias {
                            Some(expected) => match self
                                .ensure_expected_member_alias_current(identity, expected)
                                .await
                            {
                                Ok(()) => None,
                                Err(err) => Some(format!(
                                    "the expected member alias {expected} is no longer \
                                         current: {err}"
                                )),
                            },
                            None => None,
                        },
                    };

                    // REVALIDATE BEFORE RECONCILE is load-bearing rather than
                    // defensive: `reconcile_delivered_session_locked` treats a
                    // session mismatch as "the bridge rotated the session" and
                    // calls `rebind_session_after_live_respawn_locked`, which
                    // SUSPENDS the identity and RE-ACQUIRES its leases. After a
                    // reset during the unlocked window, current belongs to the
                    // NEW incarnation and delivered to the dead OLD turn, so
                    // reconciling first would bind the live incarnation to a
                    // dead turn's session - an active corruption path with a
                    // lifecycle mutation in it.
                    return match (terminal, supersede) {
                        // The turn did not reach a clean success. Nothing was
                        // superseded, so this is the ordinary phase mapping.
                        (Err(err), None) => Err(turn_phase_error(identity, err)),
                        // Both axes fired. Which one leads depends on what the
                        // turn actually did, and the two phases mean opposite
                        // things about that.
                        (
                            Err(BridgeTurnError::PostAdmissionResolutionFailed(detail)),
                            Some(sup),
                        ) => Err(IdentityRuntimeError::PostAdmissionSuperseded {
                            identity: identity.clone(),
                            detail: format!(
                                "{sup}; the turn also failed to project its session: \
                                     {detail}"
                            ),
                        }),
                        // The turn RAN AND FAILED. That is what an operator
                        // acts on, so it leads; the supersede is secondary.
                        (Err(BridgeTurnError::CompletionFailed(detail)), Some(sup)) => {
                            Err(IdentityRuntimeError::CompletionFailed {
                                identity: identity.clone(),
                                detail: format!(
                                    "{detail}; additionally, the identity was superseded \
                                     while the turn ran: {sup}"
                                ),
                            })
                        }
                        // The turn SUCCEEDED against an embodiment that is
                        // gone. Do NOT reconcile: losing the attribution beats
                        // binding the live incarnation to a dead turn's session.
                        (Ok(_), Some(sup)) => Err(IdentityRuntimeError::PostAdmissionSuperseded {
                            identity: identity.clone(),
                            detail: sup,
                        }),
                        // RECONCILED. Same relocked guard the ingress lane
                        // would have held throughout.
                        (Ok(delivered), None) => {
                            if let Some(rebound_token) = self
                                .reconcile_delivered_session_locked(identity, delivered)
                                .await?
                            {
                                token = rebound_token;
                            }
                            Ok(SendCoreOutcome {
                                token,
                                completion_baseline,
                                turn: None,
                            })
                        }
                    };
                }
            };
            if let Some(rebound_token) = self
                .reconcile_delivered_session_locked(identity, delivered_session_id)
                .await?
            {
                token = rebound_token;
            }
        }

        Ok(SendCoreOutcome {
            token,
            completion_baseline,
            turn: turn_tracking,
        })
    }

    /// The exact incarnation a delivery was admitted onto.
    ///
    /// Captured under the lifecycle lock at admission and compared under it
    /// again after the turn, so any reset, retire or alias rebind during the
    /// unlocked window is DETECTED rather than silently reconciled onto.
    ///
    /// THREE fields, all read from authoritative state: a fresh spawn moves
    /// `runtime_id`, a resume moves `generation`, a lease re-acquisition moves
    /// `fencing_token`.
    ///
    /// The member alias is deliberately NOT captured here. Deriving it from
    /// `agent_runtime_id` would have made it a duplicate of a field already in
    /// this struct rather than an independent signal, so it could never have
    /// detected the alias divergence it was there for. The alias is instead
    /// revalidated after relock by re-running the ORIGINAL expected alias
    /// through `ensure_expected_member_alias_current`, which is the authority.
    pub(crate) async fn capture_incarnation(
        &self,
        identity: &AgentIdentity,
    ) -> Result<CapturedIncarnation, IdentityRuntimeError> {
        let entries = self.entries.read().await;
        let entry = entries
            .get(identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
        Ok(CapturedIncarnation {
            runtime_id: entry
                .continuity
                .as_ref()
                .map(|c| c.agent_runtime_id.clone()),
            generation: entry.continuity.as_ref().map(|c| c.generation.get()),
            fencing_token: entry.lease.as_ref().map(|lease| lease.fencing_token),
        })
    }

    // -----------------------------------------------------------------------
    // Delivery: dispatch() — REQ-02
    // -----------------------------------------------------------------------

    /// Dispatch internal content to any identity (Addressable or InternalOnly).
    ///
    /// Enforces:
    /// - Identity must be registered and active
    /// - Lease must be held (INV-01)
    /// - Lease must not be lost (INV-02)
    ///
    /// Returns (fencing_token, is_durable) where is_durable indicates whether
    /// the dispatch is backed by a runtime_store (REQ-04).
    pub async fn dispatch(
        &self,
        identity: &AgentIdentity,
        input: &DispatchInput,
    ) -> Result<(FencingToken, bool), IdentityRuntimeError> {
        self.dispatch_with_expected_member_alias(identity, None, input)
            .await
            .map(|outcome| (outcome.admission.fencing_token, outcome.admission.durable))
    }

    /// The runtime mode a member actually runs in, resolved exactly as
    /// meerkat-mob resolves it at spawn: the roster's explicit override wins,
    /// else the profile's `runtime_mode`, else meerkat's default
    /// (`AutonomousHost`). Reading only the override - as the first cut of the
    /// autonomous-host skip did - treated every member whose roster provider
    /// set nothing as autonomous, and skipped ambient memory on `turn_driven`
    /// profiles fleet-wide. Two sources of one fact; this is the one meerkat
    /// uses.
    ///
    /// A profile bound by realm reference (not inline) has no mode MobKit can
    /// read here and resolves to the default; that is a documented limitation,
    /// not a guess dressed as knowledge.
    fn effective_runtime_mode(&self, spec: &DurableAgentSpec) -> meerkat_mob::MobRuntimeMode {
        // The mob definition reaches this runtime through the reset-roster
        // context the gateway installs at boot; a host that installs none has
        // given MobKit no profile table to read, and resolves to the default.
        let source = self
            .reset_roster_source
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        effective_runtime_mode(
            source
                .as_ref()
                .and_then(|source| source.mob_definition.as_ref()),
            spec,
        )
    }

    /// The ONE delivery preparation both member doors run (task #54): the
    /// send door always had it; the dispatch door previously delivered raw,
    /// so internal dispatches (schedules foremost) skipped defanging, taint
    /// session attribution, and ambient memory injection for the member's
    /// whole lifetime (a downstream app: zero surface=Turn injection-ledger rows).
    ///
    /// Steer is latency-sensitive live operator input: it bypasses both
    /// memory injection and inbound defanging by design. Every other
    /// delivery is defanged first (§9.1 anti-spoofing - even with injection
    /// off, forged memory envelopes are an inbound threat) and only then
    /// considered for ambient injection. Ask 1: the user content and the
    /// ambient recall travel as SEPARATE bodies - the (defanged) message and
    /// the recall as its own typed injected-context body, never fused.
    /// §10.1 taint hook: `note_current_session` keeps session attribution
    /// authoritative ahead of the async observe stream; the generation bind
    /// feeds the Distiller's EvidenceRefs (§8.4).
    async fn prepare_member_delivery(
        &self,
        identity: &AgentIdentity,
        content: &meerkat_core::ContentInput,
        memory_session_key: Option<&str>,
        memory_generation: Option<u64>,
        steer: bool,
        preparation: DeliveryPreparation,
    ) -> Result<(meerkat_core::ContentInput, Vec<meerkat_core::ContentInput>), IdentityRuntimeError>
    {
        if steer {
            return Ok((content.clone(), Vec::new()));
        }
        match self.agent_memory.read().await.clone() {
            Some(injector) => {
                if let Some(session_key) = memory_session_key {
                    injector.note_current_session(identity, session_key);
                    if let Some(generation) = memory_generation {
                        injector.note_session_generation(identity, session_key, generation);
                    }
                }
                let defanged = injector.defang_inbound(identity, content);
                // meerkat refuses injected context on generic autonomous work
                // ("autonomous inbox delivery carries no user-channel work
                // boundary"), and a refusal there is the WHOLE turn, not just the
                // memory. Defang still runs - it is an inbound threat regardless of
                // mode - but recall is skipped, typed, so the zero names itself.
                if matches!(
                    preparation,
                    DeliveryPreparation::Work(meerkat_mob::MobRuntimeMode::AutonomousHost)
                ) {
                    injector.note_turn_skip(
                        identity,
                        crate::memory::coordinator::TurnInjectionSkip::RuntimeModeAutonomousHost,
                    );
                    return Ok((defanged, Vec::new()));
                }
                let injected_context = injector
                    .inject_for_turn(identity, memory_session_key, &defanged)
                    .await
                    .map_err(|err| {
                        IdentityRuntimeError::Internal(format!("agent memory recall: {err}"))
                    })?;
                Ok((defanged, injected_context))
            }
            None => Ok((content.clone(), Vec::new())),
        }
    }

    async fn dispatch_with_expected_member_alias(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        input: &DispatchInput,
    ) -> Result<DispatchOutcome, IdentityRuntimeError> {
        self.dispatch_with_expected_incarnation(identity, expected_alias, None, input)
            .await
    }

    pub(crate) async fn dispatch_with_expected_incarnation(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        expected: Option<&CapturedIncarnation>,
        input: &DispatchInput,
    ) -> Result<DispatchOutcome, IdentityRuntimeError> {
        self.dispatch_core(identity, expected_alias, expected, input, false)
            .await
    }

    /// The ONE dispatch body. `track_turn` admits the work under a
    /// [`TurnTicket`] (see [`Self::dispatch_with_turn_ticket`]); everything
    /// else is identical.
    async fn dispatch_core(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        expected: Option<&CapturedIncarnation>,
        input: &DispatchInput,
        track_turn: bool,
    ) -> Result<DispatchOutcome, IdentityRuntimeError> {
        let result = self
            .dispatch_core_admit(identity, expected_alias, expected, input, track_turn)
            .await;
        // A maintenance dispatch (`expected` incarnation) never touched the
        // topology; an ordinary one hydrates it, after admission.
        if expected.is_none() && input_was_admitted(&result) {
            self.hydrate_peer_topology_after_admission(identity).await;
        }
        result
    }

    /// Admission for [`Self::dispatch_core`]; the same admission-first
    /// contract as [`Self::send_core_admit`].
    async fn dispatch_core_admit(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        expected: Option<&CapturedIncarnation>,
        input: &DispatchInput,
        track_turn: bool,
    ) -> Result<DispatchOutcome, IdentityRuntimeError> {
        let should_materialize = {
            let entries = self.entries.read().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            // A retired identity (`Retiring`, the terminal form) is
            // re-materialized onto its recorded session exactly like a Dormant
            // one; this is the operator's "retire then send" recovery path.
            matches!(
                entry.state,
                IdentityLifecycleState::Dormant
                    | IdentityLifecycleState::Uninitialized
                    | IdentityLifecycleState::Retiring
            )
        };
        if should_materialize && expected.is_none() {
            self.materialize_for_admission(identity, expected_alias)
                .await?;
        }

        let lifecycle_lock = self.lifecycle_lock_for(identity).await;
        let _lifecycle_guard = lifecycle_lock.lock().await;
        if let Some(expected) = expected
            && self.capture_incarnation(identity).await? != *expected
        {
            return Err(IdentityRuntimeError::PostAdmissionSuperseded {
                identity: identity.clone(),
                detail: "maintenance incarnation changed before dispatch".to_string(),
            });
        }
        if let Some(expected_alias) = expected_alias {
            self.ensure_expected_member_alias_current(identity, expected_alias)
                .await?;
        }
        {
            let entries = self.entries.read().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            if entry.state != IdentityLifecycleState::Active {
                return Err(self.refuse_inactive_delivery(identity, entry.state, "dispatch"));
            }
        }

        let mut token = self.ensure_active_lease(identity).await?;
        // Same pre-delivery baseline contract as the send path — see
        // `send_with_mode_and_interaction_with_expected_member_alias`.
        let completion_baseline = self.credited_completion_cursor(identity, token).await;
        let (
            is_durable,
            runtime_id,
            current_session,
            memory_session_key,
            memory_generation,
            memory_runtime_mode,
            external_binding,
        ) = {
            let entries = self.entries.read().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            // REQ-04: durability depends on runtime_store
            let is_durable = entry.has_runtime_store;

            let runtime_id = entry
                .continuity
                .as_ref()
                .map(|c| c.agent_runtime_id.clone());

            (
                is_durable,
                runtime_id,
                entry.continuity.as_ref().map(|c| c.session_id.clone()),
                entry.continuity.as_ref().map(|c| c.session_id.to_string()),
                entry.continuity.as_ref().map(|c| c.generation.get()),
                self.effective_runtime_mode(&entry.spec),
                durable_spec_uses_external_binding(&entry.spec),
            )
        };

        // Task #54: the dispatch door runs the SAME delivery preparation as
        // the send door (defang, taint session attribution, ambient memory
        // injection). Dispatch has no Steer mode; `DispatchOrigin` rides the
        // input untouched.
        let (content_to_deliver, injected_context) = self
            .prepare_member_delivery(
                identity,
                &input.content,
                memory_session_key.as_deref(),
                memory_generation,
                false,
                DeliveryPreparation::Work(memory_runtime_mode),
            )
            .await?;
        // Task #50 fail-closed matrix, validated BEFORE bridge admission:
        // (None, None) is an ordinary delivery; a full pair validates
        // upstream (canonical non-nil UUID correlation) and carries; EVERY
        // other combination - half-pair, invalid UUID - is a typed refusal
        // and NO delivery occurs. Never warn-and-degrade: a degraded
        // delivery under a broken identity is a silent dedup hole.
        let delivery_identity = match (&input.idempotency_key, &input.correlation_id) {
            (None, None) => None,
            (Some(idempotency_key), Some(correlation_id)) => {
                // App-supplied correlations canonicalize deterministically
                // instead of refusing on value shape (downstream app admission
                // break: source-string correlations were tolerated through
                // the 0.8.15 pair and refused by 0.8.16 identity threading).
                // Half-pairs below stay typed refusals - structure is still
                // fail closed; only the VALUE domain is widened, and ONLY
                // for the app-reachable dispatch origins (Connector, plus
                // the RPC surface's System default). Code-owned lanes -
                // Scheduler, Policy, Flow - mint their own canonical UUIDs,
                // so a non-canonical value there is a defect to refuse
                // typed (the task #50 matrix), never to mask.
                let app_lane = matches!(
                    input.origin,
                    super::types::DispatchOrigin::Connector | super::types::DispatchOrigin::System
                );
                let canonical = if app_lane {
                    crate::member_comms_id::canonical_correlation_id(correlation_id.as_str())
                } else {
                    std::borrow::Cow::Borrowed(correlation_id.as_str())
                };
                if canonical.as_ref() != correlation_id.as_str() {
                    tracing::info!(
                        identity = %identity,
                        canonical_correlation = %canonical,
                        source_len = correlation_id.as_str().len(),
                        "app-supplied delivery correlation canonicalized to UUIDv5 \
                         for bridge admission"
                    );
                }
                Some(
                    meerkat_mob::MobDeliveryIdentity::new(
                        idempotency_key.as_str(),
                        canonical.as_ref(),
                    )
                    .map_err(|error| {
                        IdentityRuntimeError::InvalidDeliveryIdentity {
                            identity: identity.clone(),
                            detail: error.to_string(),
                        }
                    })?,
                )
            }
            (Some(_), None) => {
                return Err(IdentityRuntimeError::InvalidDeliveryIdentity {
                    identity: identity.clone(),
                    detail: "idempotency key without a correlation id (half-pair)".to_string(),
                });
            }
            (None, Some(_)) => {
                return Err(IdentityRuntimeError::InvalidDeliveryIdentity {
                    identity: identity.clone(),
                    detail: "correlation id without an idempotency key (half-pair)".to_string(),
                });
            }
        };

        // Deliver through the session bridge when available. When the dedup
        // carrier is present its correlation id also rides as the
        // interaction id (meerkat requires the two to agree), tracked or not.
        // A tracked dispatch's ticket is minted per admission and never
        // derived from the correlation, which several admissions may share
        // (a fan-out to several identities, several keys under one
        // correlation).
        let mut dispatched_session_id = None;
        let mut turn = track_turn.then(untracked_delivery);
        if let (Some(bridge), Some(rid)) = (&self.bridge, &runtime_id) {
            // The dedup scope a re-dispatch would land in: this incarnation's
            // current session ledger.
            let current_delivery_key = delivery_identity
                .as_ref()
                .zip(current_session.as_ref())
                .map(|(carrier, session)| {
                    (
                        rid.clone(),
                        session.clone(),
                        carrier.idempotency_key.clone(),
                    )
                });
            let ticketed = track_turn
                && match turn_untrackable(
                    true,
                    external_binding,
                    false,
                    bridge.tracks_turn_output(),
                ) {
                    Some(reason) => {
                        turn = Some(TurnTracking::Unavailable(reason));
                        false
                    }
                    // A re-dispatch of a key whose tracked admission is still
                    // pending or completed is deduplicated by the runtime onto
                    // that admission: it names the original's ticket and is
                    // delivered on the ingress lane, so no second waiter can
                    // ever report the runtime's "deduplicated, no result of
                    // its own" terminal over the original's output.
                    None => match current_delivery_key
                        .as_ref()
                        .and_then(|key| self.reusable_delivery_ticket(identity, key))
                    {
                        Some(original) => {
                            turn = Some(TurnTracking::Tracked(original));
                            false
                        }
                        None => true,
                    },
                };
            let mut delivery =
                super::bridge::BridgeDelivery::new(content_to_deliver.clone(), HandlingMode::Queue);
            delivery.injected_context = injected_context.clone();
            delivery.interaction_id = delivery_identity
                .as_ref()
                .map(|carrier| carrier.correlation_id.clone());
            delivery.delivery_identity = delivery_identity.clone();
            // meerkat's typed pre-admission refusal of the tracked admission
            // (the member's ACTUAL runtime mode) sends the dispatch down the
            // ingress lane below: nothing was delivered by the refused attempt.
            let tracked_attempt = if ticketed {
                match bridge
                    .begin_delivery_with_output(rid, delivery.clone())
                    .await
                {
                    Err(BridgeAdmissionError::UnsupportedForMode { mode, detail, .. }) => {
                        turn = Some(runtime_refused_tracking(identity, mode, &detail));
                        None
                    }
                    attempt => Some(attempt),
                }
            } else {
                None
            };
            let delivered_session_id = if let Some(attempt) = tracked_attempt {
                // Same admission and reconciliation as the ingress arm; the
                // receipt moves to a waiter that records the turn's own output.
                let attempt = attempt.map_err(|error| admission_phase_error(identity, error));
                let receipt = match attempt {
                    Ok(receipt) => {
                        self.record_delivery_success(identity).await;
                        receipt
                    }
                    Err(error) => {
                        self.record_delivery_error(identity, &error).await;
                        return Err(error);
                    }
                };
                let resolved_session = receipt.resolved_session().cloned();
                let resolution_error = receipt.session_resolution_error().map(ToString::to_string);
                // Indexed under the session the turn was actually admitted onto;
                // an unresolved session cannot name a dedup scope, so it is not
                // indexed.
                let admitted_delivery_key = delivery_identity
                    .as_ref()
                    .zip(resolved_session.as_ref())
                    .map(|(carrier, session)| {
                        (
                            rid.clone(),
                            session.clone(),
                            carrier.idempotency_key.clone(),
                        )
                    });
                let ticket = self.track_turn(identity, admitted_delivery_key, receipt);
                turn = Some(TurnTracking::Tracked(ticket));
                match resolved_session {
                    Some(session_id) => session_id,
                    None => {
                        self.report_ticketed_resolution_failure(
                            identity,
                            ticket,
                            resolution_error
                                .as_deref()
                                .unwrap_or("no session was resolved"),
                        )
                        .await;
                        return Ok(DispatchOutcome {
                            admission: DispatchAdmission {
                                fencing_token: token,
                                durable: is_durable,
                                completion_baseline,
                            },
                            session_id: None,
                            incarnation: self.capture_incarnation(identity).await?,
                            turn,
                        });
                    }
                }
            } else {
                let attempt = bridge
                    .deliver_admitted(rid, delivery)
                    .await
                    .map_err(|error| ingress_phase_error(identity, error));
                match attempt {
                    Ok(delivered) => {
                        self.record_delivery_success(identity).await;
                        delivered
                    }
                    Err(error) => {
                        self.record_delivery_error(identity, &error).await;
                        return Err(error);
                    }
                }
            };
            // A reused ticket claims the runtime deduplicated this delivery
            // onto the original. That holds only if it landed on the session
            // whose ledger admitted the original: delivery repair or a respawn
            // can move the member onto a new session under the same runtime
            // id, and that ledger has never seen the key, so the delivery ran
            // as a turn of its own. Then the original's ticket would name the
            // wrong turn: report it untracked instead.
            if let (Some(TurnTracking::Tracked(_)), Some((_, reused_session, _)), false) =
                (&turn, current_delivery_key.as_ref(), ticketed)
                && reused_session != &delivered_session_id
            {
                tracing::warn!(
                    identity = %identity,
                    reused_session = %reused_session,
                    delivered_session = %delivered_session_id,
                    "re-dispatch landed on a different session than the admission it was to be \
                     deduplicated onto; it ran as its own turn and is reported untracked"
                );
                turn = Some(TurnTracking::Unavailable(TurnUntrackable::SessionRotated));
            }
            dispatched_session_id = Some(delivered_session_id.clone());
            if let Some(rebound_token) = self
                .reconcile_delivered_session_locked(identity, delivered_session_id)
                .await?
            {
                token = rebound_token;
            }
        }

        Ok(DispatchOutcome {
            admission: DispatchAdmission {
                fencing_token: token,
                durable: is_durable,
                completion_baseline,
            },
            session_id: dispatched_session_id,
            incarnation: self.capture_incarnation(identity).await?,
            turn,
        })
    }

    /// Cancellation-safe dispatch for RPC/host request boundaries.
    pub async fn dispatch_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        input: &DispatchInput,
    ) -> Result<(FencingToken, bool), IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let input = input.clone();
        self.run_tracked_foreground(async move { runtime.dispatch(&identity, &input).await })
            .await
    }

    /// Cancellation-safe dispatch pinned to the generated runtime alias that
    /// the caller resolved.
    pub async fn dispatch_member_alias_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: &str,
        input: &DispatchInput,
    ) -> Result<(FencingToken, bool), IdentityRuntimeError> {
        self.dispatch_admission_tracked(identity, Some(expected_alias), input)
            .await
            .map(|admission| (admission.fencing_token, admission.durable))
    }

    /// Cancellation-safe dispatch that also returns the completion baseline to
    /// wait past. Dispatch counterpart of [`Self::send_admission_tracked`];
    /// pass `expected_alias = None` for the durable identity.
    pub async fn dispatch_admission_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        input: &DispatchInput,
    ) -> Result<DispatchAdmission, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.map(ToString::to_string);
        let input = input.clone();
        self.run_tracked_foreground(async move {
            runtime
                .dispatch_with_expected_member_alias(&identity, expected_alias.as_deref(), &input)
                .await
                .map(|outcome| outcome.admission)
        })
        .await
    }

    /// Cancellation-safe dispatch whose admitted turn is tracked by ticket.
    /// Dispatch counterpart of [`Self::send_with_turn_ticket`]: the same
    /// delivery as [`Self::dispatch_admission_tracked`] (the correlation id
    /// still rides as the interaction id), with the admitted turn tracked
    /// under a [`TurnTicket`] minted for this admission, never derived from
    /// the correlation, which a fan-out or several keys may share.
    ///
    /// A re-dispatch of an idempotency key whose tracked admission to this
    /// incarnation's current session is still pending or completed is
    /// deduplicated by the runtime onto that admission, so it names the
    /// original's ticket (and the original's output). Any other re-dispatch
    /// the runtime deduplicates (the original was untracked, failed, aged out
    /// of the registry, or ran before a session rotation this key does not
    /// span) is tracked on its own and settles with what the runtime reports
    /// for the deduplicated input: the ORIGINAL input's own result while that
    /// input is still running (the runtime shares the one input's
    /// completion), and [`TurnOutput::NoOwnResult`] once it has ended.
    pub async fn dispatch_with_turn_ticket(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        input: &DispatchInput,
    ) -> Result<Ticketed<DispatchAdmission>, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.map(ToString::to_string);
        let input = input.clone();
        self.run_tracked_foreground(async move {
            runtime
                .dispatch_core(&identity, expected_alias.as_deref(), None, &input, true)
                .await
                .map(|outcome| Ticketed {
                    admission: outcome.admission,
                    turn: outcome.turn.unwrap_or_else(untracked_delivery),
                })
        })
        .await
    }

    /// Scheduler delivery pinned to a generated alias, returning the exact
    /// bridge session that accepted the work while the lifecycle lock held.
    pub(crate) async fn dispatch_member_alias_with_session_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: &str,
        input: &DispatchInput,
    ) -> Result<Option<SessionId>, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.to_string();
        let input = input.clone();
        self.run_tracked_foreground(async move {
            runtime
                .dispatch_with_expected_member_alias(&identity, Some(&expected_alias), &input)
                .await
                .map(|outcome| outcome.session_id)
        })
        .await
    }

    // -----------------------------------------------------------------------
    // Status: status() — REQ-07
    // -----------------------------------------------------------------------

    /// Return the full identity status for the given identity.
    pub async fn status(
        &self,
        identity: &AgentIdentity,
    ) -> Result<IdentityStatus, IdentityRuntimeError> {
        let entries = self.entries.read().await;
        let entry = entries
            .get(identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;

        let lease_info = entry.lease.as_ref().map(|l| LeaseInfo {
            fencing_token: l.fencing_token,
            ttl_remaining: l.ttl_remaining(),
            healthy: l.is_healthy(),
        });

        let continuity_health = Some(ContinuityHealth {
            store_reachable: true, // tracked per-store in production
            durability_policy: self.durability_policy.clone(),
            last_checkpoint_version: if entry.checkpoint_version.get() > 0 {
                Some(entry.checkpoint_version)
            } else {
                None
            },
        });

        Ok(IdentityStatus {
            identity: identity.clone(),
            state: entry.state,
            agent_runtime_id: entry
                .continuity
                .as_ref()
                .map(|c| c.agent_runtime_id.clone()),
            session_id: entry.continuity.as_ref().map(|c| c.session_id.clone()),
            profile: Some(entry.spec.profile.clone()),
            runtime_mode: entry.spec.runtime_mode_override,
            addressability: entry.spec.addressability,
            display_name: entry.spec.display_name.clone(),
            labels: entry.spec.labels.clone(),
            generation: entry.continuity.as_ref().map(|c| c.generation),
            checkpoint_version: if entry.checkpoint_version.get() > 0 {
                Some(entry.checkpoint_version)
            } else {
                None
            },
            lease: lease_info,
            continuity_health,
            continuity_unrecoverable: entry.continuity_unrecoverable.clone(),
            session_repair_required: entry.session_repair_required.clone(),
            customizer_tools_pending: self.customizer_tools_pending_for(identity),
        })
    }

    /// Return statuses for every registered identity without materializing
    /// dormant members.
    pub async fn statuses(&self) -> Vec<IdentityStatus> {
        let identities = self
            .entries
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut statuses = Vec::with_capacity(identities.len());
        for identity in identities {
            if let Ok(status) = self.status(&identity).await {
                statuses.push(status);
            }
        }
        statuses
    }

    // -----------------------------------------------------------------------
    // Lifecycle: retire() — REQ-08
    // -----------------------------------------------------------------------

    /// Retire an identity. Validates lease ownership and retires the mob member.
    pub async fn retire(
        &self,
        identity: &AgentIdentity,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.retire_with_expected_member_alias(identity, None).await
    }

    async fn retire_with_expected_member_alias(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        let lifecycle_lock = self.lifecycle_lock_for(identity).await;
        let _lifecycle_guard = lifecycle_lock.lock().await;
        if let Some(expected_alias) = expected_alias {
            self.ensure_expected_member_alias_current(identity, expected_alias)
                .await?;
        }
        self.retire_locked(identity).await
    }

    async fn retire_locked(
        &self,
        identity: &AgentIdentity,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.retire_locked_with_intent(identity, LifecycleRetireIntent::Retire)
            .await
    }

    /// Change a maintenance build input and rebuild only the captured owner.
    /// The existing retire and embodiment doors share one lifecycle lock.
    pub(crate) async fn rebuild_compaction_owner(
        &self,
        identity: &AgentIdentity,
        expected: &CapturedIncarnation,
        expected_alias: Option<&str>,
        change: super::bridge::CompactionFloorChange<'_>,
    ) -> Result<(ContinuityRecord, CapturedIncarnation), IdentityRuntimeError> {
        let lock = self.lifecycle_lock_for(identity).await;
        let _guard = lock.lock().await;
        if self.capture_incarnation(identity).await? != *expected {
            return Err(IdentityRuntimeError::PostAdmissionSuperseded {
                identity: identity.clone(),
                detail: "maintenance owner changed before profile rebuild".to_string(),
            });
        }
        if let Some(alias) = expected_alias {
            self.ensure_expected_member_alias_current(identity, alias)
                .await?;
        }
        let before = self.status(identity).await?;
        if before.state != IdentityLifecycleState::Active {
            return Err(IdentityRuntimeError::InvalidState {
                identity: identity.clone(),
                state: before.state,
                operation: "compact_member",
            });
        }
        self.retire_locked(identity).await?;
        match change.floor {
            Some(floor) => {
                change
                    .registry
                    .set_owned(identity, change.operation_id, floor);
            }
            None => {
                change.registry.clear_owned(identity, change.operation_id);
            }
        }
        let mut generation = None;
        let result = self
            .embody_identity_locked(
                identity,
                None,
                None,
                None,
                &mut generation,
                EmbodimentOverrides::default(),
            )
            .await
            .map(|outcome| outcome.record);
        self.mark_bootstrap_materialization_finished(identity, &result, generation);
        change
            .registry
            .finish_owned_build(identity, change.operation_id);
        if result.is_err() && change.floor.is_some() {
            change.registry.clear_owned(identity, change.operation_id);
        }
        let record = result?;
        if before.session_id.as_ref() != Some(&record.session_id) {
            if change.floor.is_some() {
                change.registry.clear_owned(identity, change.operation_id);
            }
            return Err(IdentityRuntimeError::PostAdmissionSuperseded {
                identity: identity.clone(),
                detail: "maintenance rebuild did not retain the captured durable session"
                    .to_string(),
            });
        }
        Ok((record, self.capture_incarnation(identity).await?))
    }

    pub(crate) async fn invalidate_superseded_compaction_floor(
        &self,
        identity: &AgentIdentity,
        expected: &CapturedIncarnation,
        floors: &super::bridge::CompactionFloorRegistry,
        operation_id: &str,
    ) {
        let lock = self.lifecycle_lock_for(identity).await;
        let _guard = lock.lock().await;
        if !matches!(self.capture_incarnation(identity).await, Ok(current) if current == *expected)
        {
            floors.clear_owned(identity, operation_id);
        }
    }

    /// Retire the identity's mob member while the caller holds the lifecycle
    /// lock. `Retire` is the ordinary path; `Reload` is the first half of a
    /// non-destructive reload and skips the memory harvest hooks because the
    /// same session is re-materialized right after (nothing rotates, no exit).
    async fn retire_locked_with_intent(
        &self,
        identity: &AgentIdentity,
        intent: LifecycleRetireIntent,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.ensure_active_lease(identity).await?;
        let registered_entry = self
            .mark_lifecycle_in_progress(identity, IdentityLifecycleState::Retiring)
            .await?;
        let _previous_token = match Self::check_lease(&registered_entry) {
            Ok(token) => token,
            Err(err) => {
                self.restore_entry(identity, registered_entry).await;
                return Err(err);
            }
        };
        let runtime_id = registered_entry
            .continuity
            .as_ref()
            .map(|c| c.agent_runtime_id.clone());
        let session_id = registered_entry
            .continuity
            .as_ref()
            .map(|c| c.session_id.clone());

        let acquire_result = match self
            .lease_provider
            .acquire_leases(std::slice::from_ref(identity), &self.runtime_instance_id)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                self.restore_entry(identity, registered_entry).await;
                return Err(IdentityRuntimeError::Lease(err));
            }
        };

        let grant = match acquire_result.get(identity) {
            Some(super::types::LeaseAcquireResult::Acquired(g)) => g.clone(),
            _ => {
                self.restore_entry(identity, registered_entry).await;
                return Err(IdentityRuntimeError::NoActiveLease(identity.clone()));
            }
        };
        if let Err(err) = self
            .advance_existing_continuity_fence(identity, &registered_entry, &grant)
            .await
        {
            self.restore_broken_entry_and_release_grant(identity, registered_entry, &grant)
                .await;
            return Err(err);
        }
        // The durable fence now belongs to the retire transaction. Refresh
        // the bridge adapter before retirement so Meerkat's terminal archive
        // projection carries that same token. Leaving the live session on the
        // prior token makes ArchiveSession fail closed with a stale fence and
        // strands the member in Retiring.
        if let Some(record) = registered_entry.continuity.as_ref()
            && let Err(error) = self
                .refresh_existing_session_runtime_state(identity, record, &grant)
                .await
        {
            self.restore_broken_entry_with_fenced_store(identity, registered_entry, &grant)
                .await;
            return Err(IdentityRuntimeError::Internal(format!(
                "bridge refresh session authority before retire: {error}"
            )));
        }

        // §8.4 trigger (b): distill the outgoing session's tail BEFORE the
        // member retires. Best-effort and bounded — retirement proceeds at
        // the distiller's pre-rotation timeout. A reload keeps the session,
        // so neither the distillation nor the exit interview applies.
        if intent == LifecycleRetireIntent::Retire
            && let Some(injector) = self.agent_memory.read().await.clone()
        {
            if let Some(session_id) = session_id.as_ref() {
                injector
                    .distill_before_rotation(
                        identity,
                        &session_id.to_string(),
                        crate::memory::distiller::DistillCause::Retire,
                    )
                    .await;
            }
            // §8.5 exit interview: queue the retired identity's store for
            // the next dream's harvest sub-phase.
            injector
                .note_identity_retired(
                    identity,
                    session_id
                        .as_ref()
                        .map(std::string::ToString::to_string)
                        .as_deref(),
                    "retire",
                )
                .await;
        }

        // Retire the mob member through the session bridge when available.
        if let (Some(bridge), Some(rid)) = (&self.bridge, &runtime_id)
            && let Err(err) = bridge.retire_member(rid).await
        {
            self.restore_entry_with_grant(identity, registered_entry, &grant)
                .await;
            return Err(IdentityRuntimeError::Internal(format!(
                "bridge retire: {err}"
            )));
        }
        if let (Some(bridge), Some(session_id)) = (&self.bridge, &session_id)
            && let Err(err) = bridge.unregister_session_runtime_state(session_id).await
        {
            self.restore_broken_entry_and_release_grant(identity, registered_entry, &grant)
                .await;
            return Err(IdentityRuntimeError::Internal(format!(
                "bridge unregister retired session: {err}"
            )));
        }

        let release_result = self
            .lease_provider
            .release_leases(std::slice::from_ref(&grant))
            .await;
        let (state, bootstrap_error) = match &release_result {
            Ok(()) => (IdentityLifecycleState::Retiring, None),
            Err(error) => (
                IdentityLifecycleState::Broken,
                Some(format!("lease release after retire: {error}")),
            ),
        };
        {
            let mut entries = self.entries.write().await;
            let entry = entries
                .get_mut(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            entry.state = state;
            entry.lease = None;
            entry.pending_lease_release = release_result.as_ref().err().map(|_| grant.clone());
        }
        self.mark_bootstrap_from_lifecycle(identity, state, bootstrap_error);
        release_result.map_err(IdentityRuntimeError::Lease)?;
        Ok(grant.fencing_token)
    }

    // -----------------------------------------------------------------------
    // Lifecycle: reload_member (non-destructive cold reload, a 2026-09-04 production incident)
    // -----------------------------------------------------------------------

    /// Record every completed reload attempt, including early no-ops/refusals.
    async fn reload_locked(
        &self,
        identity: &AgentIdentity,
    ) -> Result<MemberReloadOutcome, IdentityRuntimeError> {
        let result = self.reload_locked_inner(identity).await;
        match &result {
            Ok(outcome) => self.record_reload_outcome(identity, outcome).await,
            Err(error) => self.record_reload_failure(identity, error).await,
        }
        result
    }

    /// Non-destructive cold reload of the identity's live member.
    ///
    /// Same identity alias, same durable session id, same continuity
    /// generation. Implemented on the meerkat 0.8.33 API as retire-then-
    /// re-materialize under the identity's lifecycle lock:
    ///
    /// 1. the mob member is retired (`bridge.retire_member`, which runs
    ///    meerkat-mob's retire path and its reload-required discard) and the
    ///    session runtime state is unregistered; the continuity record is NOT
    ///    touched (retire only re-upserts it under the new fence token);
    /// 2. the entry stays in the retired terminal form (`Retiring`), which the
    ///    embodiment door accepts exactly like `Dormant` (#404);
    /// 3. the same continuity record is embodied again: `resume_session` on
    ///    the recorded `session_id`, whose executor registration is where
    ///    meerkat mints the cold reload from durable truth.
    ///
    /// Contrast `respawn_member` (identity reset): fence owner, ADVANCE the
    /// generation, fresh continuity. That is a destructive continuity reset
    /// and this verb never calls it.
    ///
    /// Prefer the upstream registration primitive. Only a bridge that does
    /// not expose it uses the legacy retire/embody path. Dormant,
    /// uninitialized and retired identities return `not_current`; reload
    /// does not revive them. Broken/Suspended are refused typed.
    async fn reload_locked_inner(
        &self,
        identity: &AgentIdentity,
    ) -> Result<MemberReloadOutcome, IdentityRuntimeError> {
        let (state, before, repair_held) = {
            let entries = self.entries.read().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            (
                entry.state,
                entry.continuity.clone(),
                entry.session_repair_required.is_some(),
            )
        };
        match state {
            IdentityLifecycleState::Active => {}
            // The one Broken shape the reload verb IS the repair path for:
            // the operator has (presumably) run the sanctioned WholeBlob
            // repair and asks for the same durable session back. No flag, no
            // reset: re-embody the recorded continuity through the ordinary
            // resume door, which re-records the hold if the document is
            // still refused.
            IdentityLifecycleState::Broken if repair_held => {
                return self.reload_repaired_session_locked(identity, before).await;
            }
            // Dormant, uninitialized, or retired: no live registration to
            // reload; the next send materializes lazily.
            IdentityLifecycleState::Dormant
            | IdentityLifecycleState::Uninitialized
            | IdentityLifecycleState::Retiring => {
                return Ok(MemberReloadOutcome {
                    reloaded: false,
                    disposition: MemberReloadDisposition::NotCurrent,
                    session_id: before.as_ref().map(|record| record.session_id.clone()),
                    generation: before.as_ref().map(|record| record.generation),
                });
            }
            IdentityLifecycleState::Broken | IdentityLifecycleState::Suspended => {
                return Err(IdentityRuntimeError::InvalidState {
                    identity: identity.clone(),
                    state,
                    operation: "reload_member",
                });
            }
        }
        let Some(before) = before else {
            return Ok(MemberReloadOutcome {
                reloaded: false,
                disposition: MemberReloadDisposition::NotCurrent,
                session_id: None,
                generation: None,
            });
        };

        self.ensure_active_lease(identity).await?;

        // meerkat 0.8.34: the mob primitive quiesces, discards, and re-registers
        // the executor for the SAME session off the actor loop, and can tell a
        // healthy registration apart (`not_degraded`). Preferred when the
        // bridge exposes it. A `not_current` verdict is a no-op, not authority
        // to retire or replace the member.
        let registration_reload = match self.bridge.as_ref() {
            Some(bridge) => {
                match bridge
                    .reload_member_registration(&before.agent_runtime_id)
                    .await
                {
                    Ok(reload) => reload,
                    // Typed audited-endpoint divergence: the registration is
                    // retained, but no reload can read the document. Park
                    // the member with the exact repair commands instead of
                    // reporting a store-not-healthy refusal that invites the
                    // retry storm.
                    Err(BridgeError::SessionRepairRequired {
                        session_id, detail, ..
                    }) => {
                        return Err(self
                            .park_session_repair_required(identity, session_id, detail)
                            .await);
                    }
                    Err(error) => return Err(reload_phase_error(identity, error)),
                }
            }
            None => None,
        };
        if let Some(reload) = registration_reload {
            if reload.session_id != before.session_id {
                return Err(IdentityRuntimeError::Internal(format!(
                    "reload_member registration session {} differs from continuity session {}",
                    reload.session_id, before.session_id,
                )));
            }
            tracing::info!(
                %identity,
                session_id = %before.session_id,
                generation = before.generation.get(),
                disposition = ?reload.disposition,
                registration_generation = reload.registration_generation,
                "reload_member: meerkat member registration reload completed \
                 (non-destructive; continuity generation does not advance)"
            );
            return Ok(MemberReloadOutcome {
                reloaded: matches!(
                    reload.disposition,
                    MemberReloadDisposition::Discarded | MemberReloadDisposition::Reattached
                ),
                disposition: reload.disposition,
                session_id: Some(before.session_id),
                generation: Some(before.generation),
            });
        }

        tracing::info!(
            %identity,
            session_id = %before.session_id,
            generation = before.generation.get(),
            "reload_member: discarding the live member registration and re-materializing the \
             same durable session (non-destructive; generation does not advance)"
        );
        // Leaves the entry in the retired terminal form (`Retiring`, lease
        // released, continuity intact), which `embody_identity_locked` accepts
        // exactly like Dormant: the same door the plain retire-then-send
        // recovery takes (#404).
        self.retire_locked_with_intent(identity, LifecycleRetireIntent::Reload)
            .await?;

        let mut bound_generation = None;
        let result = self
            .embody_identity_locked(
                identity,
                None,
                None,
                None,
                &mut bound_generation,
                EmbodimentOverrides::default(),
            )
            .await
            .map(|outcome| outcome.record);
        self.mark_bootstrap_materialization_finished(identity, &result, bound_generation);
        let record = result?;
        if record.session_id != before.session_id {
            tracing::warn!(
                %identity,
                before = %before.session_id,
                after = %record.session_id,
                "reload_member: the resume fell back to a fresh session (meerkat reported the \
                 durable snapshot typed-absent); the identity is bound to the new session"
            );
        }
        Ok(MemberReloadOutcome {
            reloaded: true,
            disposition: MemberReloadDisposition::Discarded,
            session_id: Some(record.session_id),
            generation: Some(record.generation),
        })
    }

    /// `reload_member` for a Broken identity parked with a
    /// [`SessionRepairRequired`] hold: release the Broken entry's lower-plane
    /// remnants and lease exactly as the repair pass would, re-project it
    /// Dormant with its continuity record intact, and resume the SAME durable
    /// session through the shared embodiment door. A successful resume clears
    /// the hold (non-Broken projection); a document still refused re-parks
    /// the identity through the resume door with a fresh hold, and the typed
    /// `EmbodimentRejected(RepairRequired)` failure is what the operator sees.
    async fn reload_repaired_session_locked(
        &self,
        identity: &AgentIdentity,
        before: Option<ContinuityRecord>,
    ) -> Result<MemberReloadOutcome, IdentityRuntimeError> {
        let Some(before) = before else {
            return Err(IdentityRuntimeError::InvalidState {
                identity: identity.clone(),
                state: IdentityLifecycleState::Broken,
                operation: "reload_member",
            });
        };
        tracing::info!(
            %identity,
            session_id = %before.session_id,
            generation = before.generation.get(),
            "reload_member: re-attempting the resume of a session parked for the sanctioned \
             audited-endpoint repair (same session, same generation)"
        );
        self.prepare_broken_identity_for_registration(identity)
            .await?;
        {
            let mut entries = self.entries.write().await;
            if let Some(entry) = entries.get_mut(identity) {
                entry.state = IdentityLifecycleState::Dormant;
                entry.lease = None;
            }
        }
        self.emit_event(
            identity,
            IdentityEvent::StateChanged {
                identity: identity.clone(),
                new_state: IdentityLifecycleState::Dormant,
            },
        )
        .await;
        self.clear_materialization_backoff(identity).await;

        let mut bound_generation = None;
        let result = self
            .embody_identity_locked(
                identity,
                None,
                None,
                None,
                &mut bound_generation,
                EmbodimentOverrides::default(),
            )
            .await
            .map(|outcome| outcome.record);
        self.mark_bootstrap_materialization_finished(identity, &result, bound_generation);
        let record = result?;
        {
            let mut entries = self.entries.write().await;
            if let Some(entry) = entries.get_mut(identity)
                && entry.state != IdentityLifecycleState::Broken
            {
                entry.session_repair_required = None;
            }
        }
        if record.session_id != before.session_id {
            tracing::warn!(
                %identity,
                before = %before.session_id,
                after = %record.session_id,
                "reload_member: the repaired-session resume fell back to a fresh session; the \
                 identity is bound to the new session"
            );
        }
        Ok(MemberReloadOutcome {
            reloaded: true,
            disposition: MemberReloadDisposition::Discarded,
            session_id: Some(record.session_id),
            generation: Some(record.generation),
        })
    }

    fn unix_ms_now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }

    async fn record_reload_outcome(&self, identity: &AgentIdentity, outcome: &MemberReloadOutcome) {
        let record = ReloadAttemptRecord {
            outcome: match outcome.disposition {
                MemberReloadDisposition::Discarded => ReloadAttemptOutcome::Discarded,
                MemberReloadDisposition::NotDegraded => ReloadAttemptOutcome::NotDegraded,
                MemberReloadDisposition::NotCurrent => ReloadAttemptOutcome::NotCurrent,
                MemberReloadDisposition::Reattached => ReloadAttemptOutcome::Reattached,
            },
            detail: None,
            data: None,
            at_unix_ms: Self::unix_ms_now(),
        };
        if let Some(entry) = self.entries.write().await.get_mut(identity) {
            entry.last_reload = Some(record);
        }
    }

    async fn record_reload_failure(&self, identity: &AgentIdentity, error: &IdentityRuntimeError) {
        let (outcome, detail) = match error {
            IdentityRuntimeError::ReloadRefused { reason, .. } => {
                (ReloadAttemptOutcome::Refused, reason.clone())
            }
            IdentityRuntimeError::ReloadTimedOut { stage, .. } => {
                (ReloadAttemptOutcome::TimedOut, stage.clone())
            }
            IdentityRuntimeError::ActorAdmissionTimeout { .. } => {
                (ReloadAttemptOutcome::TimedOut, error.to_string())
            }
            IdentityRuntimeError::EmbodimentRejected(failure)
                if failure.kind == ContinuityFailureKind::RepairRequired =>
            {
                (ReloadAttemptOutcome::RepairRequired, failure.detail.clone())
            }
            other => (ReloadAttemptOutcome::Failed, other.to_string()),
        };
        let record = ReloadAttemptRecord {
            outcome,
            detail: Some(detail),
            data: error.structured_data(),
            at_unix_ms: Self::unix_ms_now(),
        };
        if let Some(entry) = self.entries.write().await.get_mut(identity) {
            entry.last_reload = Some(record);
        }
    }

    /// The delivery path's single automatic reload after a typed
    /// reload-required refusal. Runs under the caller's lifecycle lock. Any
    /// failure becomes the typed `ReloadRequired` carrying both reasons.
    async fn reload_for_delivery_locked(
        &self,
        identity: &AgentIdentity,
        refusal: &IdentityRuntimeError,
    ) -> Result<(), IdentityRuntimeError> {
        tracing::warn!(
            %identity,
            refusal = %refusal,
            "delivery refused: member requires a cold reload; running ONE automatic \
             non-destructive reload_member before retrying"
        );
        match self.reload_locked(identity).await {
            Ok(outcome) => {
                tracing::info!(
                    %identity,
                    disposition = ?outcome.disposition,
                    "automatic reload_member completed; retrying the delivery once"
                );
                Ok(())
            }
            // The store is not healthy yet, or the bounded reload timed out:
            // keep the class (retry-later vs inspect) instead of folding it
            // into a generic reload-required failure.
            Err(
                typed @ (IdentityRuntimeError::ReloadRefused { .. }
                | IdentityRuntimeError::ReloadTimedOut { .. }
                | IdentityRuntimeError::ActorAdmissionTimeout { .. }
                | IdentityRuntimeError::ActorLoopStalled { .. }
                | IdentityRuntimeError::ActorTerminated { .. }),
            ) => Err(typed),
            Err(reload_error) => Err(IdentityRuntimeError::ReloadRequired {
                identity: identity.clone(),
                reason: format!("{refusal}; automatic reload_member failed: {reload_error}"),
            }),
        }
    }

    /// Public entry for the reload verb (in-process callers and tests).
    pub async fn reload_member(
        &self,
        identity: &AgentIdentity,
    ) -> Result<MemberReloadOutcome, IdentityRuntimeError> {
        let lifecycle_lock = self.lifecycle_lock_for(identity).await;
        let _lifecycle_guard = lifecycle_lock.lock().await;
        self.reload_locked(identity).await
    }

    /// Cancellation-safe reload for RPC/console boundaries that atomically
    /// rejects a stale generated runtime alias under the same lifecycle lock
    /// as the mutation.
    pub async fn reload_member_alias_tracked(
        self: &Arc<Self>,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
    ) -> Result<MemberReloadOutcome, IdentityRuntimeError> {
        let runtime = Arc::clone(self);
        let identity = identity.clone();
        let expected_alias = expected_alias.map(str::to_owned);
        self.run_tracked_foreground(async move {
            let lifecycle_lock = runtime.lifecycle_lock_for(&identity).await;
            let _lifecycle_guard = lifecycle_lock.lock().await;
            if let Some(expected_alias) = expected_alias.as_deref()
                && let Err(error) = runtime
                    .ensure_expected_member_alias_current(&identity, expected_alias)
                    .await
            {
                runtime.record_reload_failure(&identity, &error).await;
                return Err(error);
            }
            runtime.reload_locked(&identity).await
        })
        .await
    }

    // -----------------------------------------------------------------------
    // Health: member_health (in-process reads only; answers during a stall)
    // -----------------------------------------------------------------------

    /// Install the unified runtime's actor-loop probe verdict and forward it
    /// to the session bridge, whose admission path fails fast while a stall
    /// is open.
    /// Install the observer told when a live member's session is rebound
    /// to a new session id (see [`MemberSessionRotationObserver`]).
    pub(crate) fn install_session_rotation_observer(
        &self,
        observer: Arc<dyn MemberSessionRotationObserver>,
    ) {
        *self
            .session_rotation_observer
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(observer);
    }

    pub fn install_actor_loop_health(&self, health: Arc<ActorLoopHealth>) {
        if let Some(bridge) = self.bridge.as_ref() {
            bridge.observe_actor_loop_health(Arc::clone(&health));
        }
        *self
            .actor_loop_health
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(health);
    }

    /// The installed probe verdict, if any.
    pub fn actor_loop_health(&self) -> Option<Arc<ActorLoopHealth>> {
        self.actor_loop_health
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The identity's lifecycle and delivery health. Every field is an
    /// in-process read: this never touches the mob actor, so it answers while
    /// the loop is stalled, which is exactly when an operator needs it.
    pub async fn member_health(
        &self,
        identity: &AgentIdentity,
    ) -> Result<MemberHealthReport, IdentityRuntimeError> {
        let entries = self.entries.read().await;
        let entry = entries
            .get(identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
        let bootstrap_state = self
            .identity_bootstrap_status()
            .identities
            .get(identity)
            .map(|entry| entry.state);
        let actor_loop = self
            .actor_loop_health()
            .map(|health| health.report())
            .unwrap_or_else(ActorLoopHealthReport::unobserved);
        let open_stall_id = (actor_loop.state == ActorLoopHealthKind::Stalled)
            .then_some(actor_loop.stall_id)
            .flatten();
        let mut report = MemberHealthReport {
            identity: identity.clone(),
            member_id: entry
                .continuity
                .as_ref()
                .map(|record| record.agent_runtime_id.clone()),
            state: entry.state,
            bootstrap_state,
            materialization_in_flight: bootstrap_state == Some(IdentityBootstrapState::Warming),
            session_id: entry
                .continuity
                .as_ref()
                .map(|record| record.session_id.clone()),
            generation: entry.continuity.as_ref().map(|record| record.generation),
            last_delivery_error: entry.last_delivery_error.clone(),
            last_reload: entry.last_reload.clone(),
            actor_loop,
            open_stall_id,
            durability: None,
            continuity_unrecoverable: entry.continuity_unrecoverable.clone(),
            session_repair_required: entry.session_repair_required.clone(),
        };
        // Custom bridges may await I/O. Never hold the fleet entries lock
        // across that extension point.
        drop(entries);
        if let (Some(bridge), Some(session_id)) = (self.bridge.as_ref(), report.session_id.as_ref())
        {
            report.durability = bridge.member_durability(session_id).await;
        }
        Ok(report)
    }

    fn delivery_error_class(err: &IdentityRuntimeError) -> DeliveryErrorClass {
        match err {
            IdentityRuntimeError::ReloadRequired { .. } => DeliveryErrorClass::ReloadRequired,
            IdentityRuntimeError::ActorLoopStalled { .. } => DeliveryErrorClass::ActorLoopStalled,
            IdentityRuntimeError::ActorTerminated { .. } => DeliveryErrorClass::ActorTerminated,
            IdentityRuntimeError::AdmissionBacklogFull { .. } => {
                DeliveryErrorClass::AdmissionBacklogFull
            }
            IdentityRuntimeError::ReloadRefused { .. } => DeliveryErrorClass::ReloadRefused,
            IdentityRuntimeError::ReloadTimedOut { .. } => DeliveryErrorClass::ReloadTimedOut,
            IdentityRuntimeError::ActorAdmissionTimeout { .. } => {
                DeliveryErrorClass::AdmissionTimeout
            }
            IdentityRuntimeError::CompletionFailed { .. }
            | IdentityRuntimeError::PostAdmissionResolutionFailed { .. }
            | IdentityRuntimeError::PostAdmissionSuperseded { .. } => {
                DeliveryErrorClass::Completion
            }
            _ => DeliveryErrorClass::AdmissionFailed,
        }
    }

    async fn record_delivery_error(&self, identity: &AgentIdentity, err: &IdentityRuntimeError) {
        let record = DeliveryErrorRecord {
            class: Self::delivery_error_class(err),
            detail: err.to_string(),
            data: err.structured_data(),
            at_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
                .unwrap_or(0),
        };
        if let Some(entry) = self.entries.write().await.get_mut(identity) {
            entry.last_delivery_error = Some(record);
        }
    }

    async fn record_delivery_success(&self, identity: &AgentIdentity) {
        if let Some(entry) = self.entries.write().await.get_mut(identity)
            && entry.last_delivery_error.is_some()
        {
            entry.last_delivery_error = None;
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle: respawn() — REQ-09
    // -----------------------------------------------------------------------

    /// Respawn: non-destructive recovery.
    ///
    /// 1. Fence the current owner
    /// 2. Attempt final checkpoint
    /// 3. Reactivate from authoritative continuity with same record + runtime ID
    /// 4. ContinuityGeneration does NOT advance
    pub async fn respawn(
        &self,
        identity: &AgentIdentity,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        self.respawn_with_expected_member_alias(identity, None)
            .await
    }

    async fn respawn_with_expected_member_alias(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        let lifecycle_lock = self.lifecycle_lock_for(identity).await;
        let _lifecycle_guard = lifecycle_lock.lock().await;
        if let Some(expected_alias) = expected_alias {
            self.ensure_expected_member_alias_current(identity, expected_alias)
                .await?;
        }
        self.respawn_locked(identity).await
    }

    /// Perform the identity-side half of respawn while the caller holds the
    /// per-identity lifecycle lock.
    async fn respawn_locked(
        &self,
        identity: &AgentIdentity,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        let registered_entry = self
            .mark_lifecycle_in_progress(identity, IdentityLifecycleState::Suspended)
            .await?;

        // Fence the old owner by re-acquiring the lease
        let acquire_result = match self
            .lease_provider
            .acquire_leases(std::slice::from_ref(identity), &self.runtime_instance_id)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                self.restore_entry(identity, registered_entry).await;
                return Err(IdentityRuntimeError::Lease(err));
            }
        };

        let grant = match acquire_result.get(identity) {
            Some(super::types::LeaseAcquireResult::Acquired(g)) => g.clone(),
            _ => {
                self.restore_entry(identity, registered_entry).await;
                return Err(IdentityRuntimeError::NoActiveLease(identity.clone()));
            }
        };

        if let Err(err) = self
            .advance_existing_continuity_fence(identity, &registered_entry, &grant)
            .await
        {
            self.restore_broken_entry_and_release_grant(identity, registered_entry, &grant)
                .await;
            return Err(err);
        }

        // Resolve current continuity state
        let resolved = match self
            .continuity_store
            .resolve_many(std::slice::from_ref(identity))
            .await
        {
            Ok(resolved) => resolved,
            Err(err) => {
                self.restore_entry_with_grant(identity, registered_entry.clone(), &grant)
                    .await;
                return Err(IdentityRuntimeError::Store(err));
            }
        };

        let record = match resolved.get(identity) {
            Some(super::types::ContinuityResolveState::Ready { record }) => record.clone(),
            Some(super::types::ContinuityResolveState::Broken { failure }) => {
                self.restore_entry_with_grant(identity, registered_entry, &grant)
                    .await;
                return Err(IdentityRuntimeError::Internal(format!(
                    "broken continuity for {identity}: {}",
                    failure.detail
                )));
            }
            Some(super::types::ContinuityResolveState::Uninitialized) => {
                self.restore_entry_with_grant(identity, registered_entry, &grant)
                    .await;
                return Err(IdentityRuntimeError::Internal(format!(
                    "cannot respawn uninitialized identity {identity}"
                )));
            }
            None => {
                self.restore_entry_with_grant(identity, registered_entry, &grant)
                    .await;
                return Err(IdentityRuntimeError::Store(
                    ContinuityStoreError::NotFound {
                        identity: identity.clone(),
                    },
                ));
            }
        };

        // §8.4 trigger (b): respawn is a recovery boundary — harvest the
        // session's window before the runtime refreshes (the SessionId does
        // not rotate here; the cursor stays valid). Bounded; respawn
        // proceeds at the pre-rotation timeout.
        if let Some(injector) = self.agent_memory.read().await.clone() {
            let session_key = record.session_id.to_string();
            injector.note_session_generation(identity, &session_key, record.generation.get());
            injector
                .distill_before_rotation(
                    identity,
                    &session_key,
                    crate::memory::distiller::DistillCause::Respawn,
                )
                .await;
        }

        let effective_checkpoint_version = match self
            .refresh_existing_session_runtime_state(identity, &record, &grant)
            .await
        {
            Ok(version) => version,
            Err(err) => {
                self.restore_entry_with_grant(identity, registered_entry, &grant)
                    .await;
                return Err(err);
            }
        };
        let mut record = record;
        record.checkpoint_version = effective_checkpoint_version;

        // Update runtime state: same record, new lease, back to Active
        let mut entries = self.entries.write().await;
        let Some(entry) = entries.get_mut(identity) else {
            drop(entries);
            if let Err(err) = self
                .release_or_park_untracked_leases(std::slice::from_ref(&grant))
                .await
            {
                tracing::warn!(
                    %identity,
                    error = %err,
                    "failed to release lease after respawn entry disappeared; exact grant parked for retry"
                );
            }
            return Err(IdentityRuntimeError::UnknownIdentity(identity.clone()));
        };
        entry.continuity = Some(record.clone());
        entry.lease = Some(LeaseEntry {
            fencing_token: grant.fencing_token,
            ttl: grant.ttl,
            acquired_at: Instant::now(),
        });
        entry.state = IdentityLifecycleState::Active;
        entry.checkpoint_version = record.checkpoint_version;
        drop(entries);
        self.mark_bootstrap_from_lifecycle(identity, IdentityLifecycleState::Active, None);

        Ok(record)
    }

    /// Rebind continuity to the concrete session created by a lower-level
    /// member respawn. This keeps identity-first status aligned when a control
    /// surface refreshes the mob member outside the identity runtime bridge.
    pub async fn rebind_session_after_live_respawn(
        &self,
        identity: &AgentIdentity,
        session_id: SessionId,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        self.rebind_session_after_live_respawn_with_expected_member_alias(
            identity, None, session_id,
        )
        .await
    }

    async fn rebind_session_after_live_respawn_with_expected_member_alias(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
        session_id: SessionId,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        let lifecycle_lock = self.lifecycle_lock_for(identity).await;
        let _lifecycle_guard = lifecycle_lock.lock().await;
        if let Some(expected_alias) = expected_alias {
            self.ensure_expected_member_alias_current(identity, expected_alias)
                .await?;
        }
        self.rebind_session_after_live_respawn_locked(identity, session_id)
            .await
    }

    async fn reconcile_delivered_session_locked(
        &self,
        identity: &AgentIdentity,
        delivered_session_id: SessionId,
    ) -> Result<Option<FencingToken>, IdentityRuntimeError> {
        let current_session_id = {
            let entries = self.entries.read().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            entry
                .continuity
                .as_ref()
                .map(|record| record.session_id.clone())
        };

        let Some(current_session_id) = current_session_id else {
            return Ok(None);
        };
        if current_session_id == delivered_session_id {
            return Ok(None);
        }

        tracing::warn!(
            %identity,
            old_session_id = %current_session_id,
            new_session_id = %delivered_session_id,
            "identity bridge delivery returned a rotated session; rebinding continuity"
        );
        self.rebind_session_after_live_respawn_locked(identity, delivered_session_id)
            .await?;

        let entries = self.entries.read().await;
        let entry = entries
            .get(identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
        Ok(entry.lease.as_ref().map(|lease| lease.fencing_token))
    }

    async fn rebind_session_after_live_respawn_locked(
        &self,
        identity: &AgentIdentity,
        session_id: SessionId,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        let registered_entry = self
            .mark_lifecycle_in_progress(identity, IdentityLifecycleState::Suspended)
            .await?;

        let acquire_result = match self
            .lease_provider
            .acquire_leases(std::slice::from_ref(identity), &self.runtime_instance_id)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                self.restore_entry(identity, registered_entry).await;
                return Err(IdentityRuntimeError::Lease(err));
            }
        };

        let grant = match acquire_result.get(identity) {
            Some(super::types::LeaseAcquireResult::Acquired(g)) => g.clone(),
            _ => {
                self.restore_entry(identity, registered_entry).await;
                return Err(IdentityRuntimeError::NoActiveLease(identity.clone()));
            }
        };

        let mut record = match registered_entry.continuity.as_ref() {
            Some(record) => record.clone(),
            None => {
                self.restore_entry_with_grant(identity, registered_entry, &grant)
                    .await;
                return Err(IdentityRuntimeError::UnknownIdentity(identity.clone()));
            }
        };
        if let Err(err) = self
            .advance_existing_continuity_fence(identity, &registered_entry, &grant)
            .await
        {
            if let Some(bridge) = self.bridge.as_ref()
                && let Err(unregister_err) =
                    bridge.unregister_session_runtime_state(&session_id).await
            {
                tracing::warn!(
                    %identity,
                    session_id = %session_id,
                    error = %unregister_err,
                    "failed to unregister rebound session after continuity fence failure"
                );
            }
            self.restore_broken_entry_and_release_grant(identity, registered_entry, &grant)
                .await;
            return Err(err);
        }
        let previous_session_id = record.session_id.clone();
        record.session_id = session_id;

        if let Err(err) = self
            .continuity_store
            .upsert_continuity_record(&record, grant.fencing_token)
            .await
        {
            if let Some(bridge) = self.bridge.as_ref()
                && let Err(unregister_err) = bridge
                    .unregister_session_runtime_state(&record.session_id)
                    .await
            {
                tracing::warn!(
                    %identity,
                    session_id = %record.session_id,
                    error = %unregister_err,
                    "failed to unregister rebound session after continuity upsert failure"
                );
            }
            self.mark_rebind_failure_broken(identity, registered_entry, &grant, &record)
                .await;
            return Err(IdentityRuntimeError::Store(err));
        }

        // A lower session-store save can advance the durable checkpoint head
        // while a respawn is rebinding to a different session id. The local
        // store preserves that same-generation maximum; read the effective
        // row back before seeding bridge runtime state so neither the in-memory
        // entry nor the new session counter rewinds to the stale captured
        // version.
        let effective = match self
            .continuity_store
            .resolve_many(std::slice::from_ref(identity))
            .await
        {
            Ok(effective) => effective,
            Err(error) => {
                self.mark_rebind_failure_broken(identity, registered_entry, &grant, &record)
                    .await;
                return Err(IdentityRuntimeError::Store(error));
            }
        };
        match effective.get(identity) {
            Some(super::types::ContinuityResolveState::Ready {
                record: effective_record,
            }) if effective_record.session_id == record.session_id
                && effective_record.generation == record.generation =>
            {
                record = effective_record.clone();
            }
            other => {
                self.mark_rebind_failure_broken(identity, registered_entry, &grant, &record)
                    .await;
                return Err(IdentityRuntimeError::Internal(format!(
                    "rebind continuity read-back did not return the committed session/generation: {other:?}"
                )));
            }
        }

        if let Some(bridge) = self.bridge.as_ref() {
            match bridge
                .register_session_runtime_state(
                    &record.session_id,
                    identity,
                    record.generation,
                    record.checkpoint_version,
                    grant.fencing_token,
                )
                .await
            {
                Ok(version) => {
                    record.checkpoint_version =
                        CheckpointVersion::new(record.checkpoint_version.get().max(version.get()));
                }
                Err(err) => {
                    if let Err(unregister_err) = bridge
                        .unregister_session_runtime_state(&record.session_id)
                        .await
                    {
                        tracing::warn!(
                            %identity,
                            session_id = %record.session_id,
                            error = %unregister_err,
                            "failed to unregister rebound session after bridge register failure"
                        );
                    }
                    self.mark_rebind_failure_broken(identity, registered_entry, &grant, &record)
                        .await;
                    return Err(IdentityRuntimeError::Internal(format!(
                        "bridge rebind respawned session runtime state: {err}"
                    )));
                }
            }
            if previous_session_id != record.session_id
                && let Err(err) = bridge
                    .unregister_session_runtime_state(&previous_session_id)
                    .await
            {
                tracing::warn!(
                    %identity,
                    session_id = %previous_session_id,
                    error = %err,
                    "failed to unregister previous session after live respawn rebind"
                );
            }
        }

        if let Err(err) = self
            .continuity_store
            .upsert_continuity_record(&record, grant.fencing_token)
            .await
        {
            if let Some(bridge) = self.bridge.as_ref()
                && let Err(unregister_err) = bridge
                    .unregister_session_runtime_state(&record.session_id)
                    .await
            {
                tracing::warn!(
                    %identity,
                    session_id = %record.session_id,
                    error = %unregister_err,
                    "failed to unregister rebound session after final continuity upsert failure"
                );
            }
            self.mark_rebind_failure_broken(identity, registered_entry, &grant, &record)
                .await;
            return Err(IdentityRuntimeError::Store(err));
        }

        self.register(
            registered_entry.spec,
            IdentityLifecycleState::Active,
            Some(record.clone()),
            Some(grant),
        )
        .await;
        if previous_session_id != record.session_id {
            let observer = self
                .session_rotation_observer
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(observer) = observer {
                observer
                    .member_session_rotated(
                        &crate::member_comms_id::mob_member_id(record.agent_runtime_id.as_str()),
                        &previous_session_id,
                        &record.session_id,
                    )
                    .await;
            }
        }
        Ok(record)
    }

    // -----------------------------------------------------------------------
    // Lifecycle: reset() — REQ-10
    // -----------------------------------------------------------------------

    /// Reset: destructive continuity reset.
    ///
    /// 1. Fence old owner
    /// 2. Advance ContinuityGeneration
    /// 3. Create fresh continuity under the same AgentIdentity
    /// 4. Old-owner late writes rejected by stale fencing token
    pub async fn reset(
        &self,
        identity: &AgentIdentity,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        self.reset_with_expected_member_alias(identity, None).await
    }

    async fn reset_with_expected_member_alias(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
    ) -> Result<ContinuityRecord, IdentityRuntimeError> {
        let mut foreground_shutdown = self.foreground_cancel.subscribe();
        let lifecycle_lock = self.lifecycle_lock_for(identity).await;
        let _lifecycle_guard = lifecycle_lock.lock().await;
        if let Some(expected_alias) = expected_alias {
            self.ensure_expected_member_alias_current(identity, expected_alias)
                .await?;
        }
        // Re-profile before snapshotting the lifecycle entry so the rebuilt
        // session uses the current roster spec, not the old checkpoint spec.
        self.adopt_current_roster_spec_for_reset(identity).await;
        let registered_entry = self
            .mark_lifecycle_in_progress(identity, IdentityLifecycleState::Suspended)
            .await?;

        // INV-05: fence the old owner first
        let acquire_result = match self
            .lease_provider
            .acquire_leases(std::slice::from_ref(identity), &self.runtime_instance_id)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                self.restore_entry(identity, registered_entry).await;
                return Err(IdentityRuntimeError::Lease(err));
            }
        };

        let grant = match acquire_result.get(identity) {
            Some(super::types::LeaseAcquireResult::Acquired(g)) => g.clone(),
            _ => {
                self.restore_entry(identity, registered_entry).await;
                return Err(IdentityRuntimeError::NoActiveLease(identity.clone()));
            }
        };
        if let Err(err) = self
            .advance_existing_continuity_fence(identity, &registered_entry, &grant)
            .await
        {
            self.restore_broken_entry_and_release_grant(identity, registered_entry, &grant)
                .await;
            return Err(err);
        }

        // Resolve to get current generation
        let resolved = match self
            .continuity_store
            .resolve_many(std::slice::from_ref(identity))
            .await
        {
            Ok(resolved) => resolved,
            Err(err) => {
                self.restore_entry_with_grant(identity, registered_entry.clone(), &grant)
                    .await;
                return Err(IdentityRuntimeError::Store(err));
            }
        };

        let current_gen = match resolved.get(identity) {
            Some(super::types::ContinuityResolveState::Ready { record }) => record.generation,
            Some(super::types::ContinuityResolveState::Uninitialized) => {
                ContinuityGeneration::new(0)
            }
            _ => ContinuityGeneration::new(0),
        };

        // Advance generation
        let new_gen = ContinuityGeneration::new(current_gen.get() + 1);
        let new_session_id = meerkat_core::types::SessionId::new();
        let new_runtime_id = AgentRuntimeId::parse(&format!("rt:{identity}:{}", new_gen.get()))
            .map_err(|e| {
                IdentityRuntimeError::Internal(format!("failed to mint runtime id: {e}"))
            })?;

        let new_record = ContinuityRecord {
            identity: identity.clone(),
            agent_runtime_id: new_runtime_id,
            session_id: new_session_id,
            generation: new_gen,
            checkpoint_version: CheckpointVersion::new(0),
        };
        let spec = registered_entry.spec.clone();
        let mut draft = super::types::AgentBuildDraft {
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
        if self.bridge.is_some() {
            let active_peers = self.entries.read().await.keys().cloned().collect();
            let managed_edges = self.desired_peer_edges.read().await.clone();
            let build_context = AgentBuildContext {
                identity: identity.clone(),
                active_peers,
                managed_edges,
                runtime_services: self.runtime_services(),
            };
            let mut customized = false;
            if let Some(customizer) = self.customizer.read().await.clone() {
                let customize = customizer.customize_build(&build_context, &spec, &mut draft);
                tokio::pin!(customize);
                let customize_result = if *foreground_shutdown.borrow() {
                    None
                } else {
                    tokio::select! {
                        result = &mut customize => Some(result),
                        changed = foreground_shutdown.changed() => {
                            if changed.is_ok() && *foreground_shutdown.borrow() {
                                None
                            } else {
                                Some(customize.await)
                            }
                        }
                    }
                };
                let Some(customize_result) = customize_result else {
                    self.restore_entry_with_grant(identity, registered_entry, &grant)
                        .await;
                    return Err(IdentityRuntimeError::Internal(
                        "identity reset cancelled during shutdown before session installation"
                            .to_string(),
                    ));
                };
                if let Err(err) = customize_result {
                    self.restore_entry_with_grant(identity, registered_entry, &grant)
                        .await;
                    return Err(IdentityRuntimeError::Internal(format!(
                        "customizer after reset: {err}"
                    )));
                }
                customized = true;
            }
            if customized {
                self.publish_customizer_tools(identity, &mut draft).await;
            }
        }

        #[cfg(feature = "extension-state")]
        let extension_publication = match self
            .bridge
            .as_ref()
            .map(|bridge| bridge.begin_extension_identity_publication(identity))
            .transpose()
        {
            Ok(publication) => publication.flatten(),
            Err(error) => {
                self.restore_entry_with_grant(identity, registered_entry, &grant)
                    .await;
                return Err(IdentityRuntimeError::Internal(format!(
                    "identity publication preparation: {error}"
                )));
            }
        };

        // Bridge: ONE authoritative successor transition. This used to retire the
        // old mob member and then create a fresh one, which the durable-roster
        // contract makes impossible - the successor occupies the same roster row,
        // so the create collided with its own predecessor. Respawn is the single
        // primitive that retires terminally and spawns the exact successor, and
        // splitting it back into retire + create would reopen the compensation
        // races it exists to close.
        if let Some(bridge) = &self.bridge {
            if let Err(err) = self
                .continuity_store
                .upsert_continuity_record(&new_record, grant.fencing_token)
                .await
            {
                self.restore_entry_with_grant(identity, registered_entry, &grant)
                    .await;
                return Err(IdentityRuntimeError::Store(err));
            }
            let provisional_record = new_record.clone();

            let old_runtime_id = registered_entry
                .continuity
                .as_ref()
                .map(|c| c.agent_runtime_id.clone());
            let old_session_id = registered_entry
                .continuity
                .as_ref()
                .map(|c| c.session_id.clone());

            let successor = bridge
                .reset_member_to_successor(identity, &spec, &draft)
                .await
                .map_err(|e| {
                    IdentityRuntimeError::Internal(format!("bridge reset successor: {e}"))
                });
            let successor = match successor {
                Ok(successor) => successor,
                Err(err) => {
                    // No cleanup retire here. The provisional runtime id was
                    // never created as a roster row - respawn either committed
                    // its own successor or did not - so retiring it would report
                    // a spurious cleanup failure on top of the real error.
                    let cleanup_error: Option<BridgeError> = None;
                    let delete_error = self
                        .restore_entry_after_reset_bridge_failure(
                            identity,
                            &provisional_record,
                            registered_entry.clone(),
                            &grant,
                            cleanup_error.is_some(),
                        )
                        .await;
                    if cleanup_error.is_some() || delete_error.is_some() {
                        return Err(IdentityRuntimeError::Internal(format!(
                            "{err}{}{}",
                            cleanup_error
                                .as_ref()
                                .map(|e| format!("; cleanup retire failed: {e}"))
                                .unwrap_or_default(),
                            delete_error
                                .as_ref()
                                .map(|e| format!("; tentative continuity cleanup failed: {e}"))
                                .unwrap_or_default()
                        )));
                    }
                    return Err(err);
                }
            };
            // Commit continuity from what the machine actually committed. Both
            // atoms come from the successor binding: the generation is
            // MobMachine's to mint, so a pre-computed runtime id would be a
            // guess that happened to be right until it was not.
            let mut new_record = new_record;
            new_record.agent_runtime_id = successor.agent_runtime_id.clone();
            let session_id = successor.session_id;
            new_record.session_id = session_id;
            tracing::debug!(
                identity = %identity,
                runtime_id = %new_record.agent_runtime_id,
                session_id = %new_record.session_id,
                "reset bridge create_session completed",
            );

            if let Err(err) = self
                .continuity_store
                .upsert_continuity_record(&new_record, grant.fencing_token)
                .await
            {
                let unregister_error = Self::unregister_bridge_session_runtime_states(
                    bridge.as_ref(),
                    std::slice::from_ref(&new_record.session_id),
                )
                .await;
                let cleanup_error = bridge
                    .retire_member(&new_record.agent_runtime_id)
                    .await
                    .err();
                let delete_error = self
                    .restore_entry_after_reset_bridge_failure(
                        identity,
                        &provisional_record,
                        registered_entry.clone(),
                        &grant,
                        unregister_error.is_some() || cleanup_error.is_some(),
                    )
                    .await;
                if unregister_error.is_some() || cleanup_error.is_some() || delete_error.is_some() {
                    return Err(IdentityRuntimeError::Internal(format!(
                        "continuity upsert actual session after reset: {err}{}{}{}",
                        unregister_error
                            .as_ref()
                            .map(|e| format!("; unregister session failed: {e}"))
                            .unwrap_or_default(),
                        cleanup_error
                            .as_ref()
                            .map(|e| format!("; cleanup retire failed: {e}"))
                            .unwrap_or_default(),
                        delete_error
                            .as_ref()
                            .map(|e| format!("; tentative continuity cleanup failed: {e}"))
                            .unwrap_or_default(),
                    )));
                }
                return Err(IdentityRuntimeError::Store(err));
            }

            let register_result = bridge
                .register_session_runtime_state(
                    &new_record.session_id,
                    identity,
                    new_record.generation,
                    new_record.checkpoint_version,
                    grant.fencing_token,
                )
                .await;
            let effective_checkpoint_version = match register_result {
                Ok(version) => version,
                Err(err) => {
                    let unregister_error = Self::unregister_bridge_session_runtime_states(
                        bridge.as_ref(),
                        std::slice::from_ref(&new_record.session_id),
                    )
                    .await;
                    let cleanup_error = bridge
                        .retire_member(&new_record.agent_runtime_id)
                        .await
                        .err();
                    let mut detail =
                        format!("bridge register actual session runtime state after reset: {err}");
                    if let Some(unregister_error) = unregister_error.as_ref() {
                        detail
                            .push_str(&format!("; unregister session failed: {unregister_error}"));
                    }
                    if let Some(cleanup_error) = cleanup_error.as_ref() {
                        detail.push_str(&format!("; cleanup retire failed: {cleanup_error}"));
                    }
                    let delete_error = self
                        .restore_entry_after_reset_bridge_failure(
                            identity,
                            &new_record,
                            registered_entry.clone(),
                            &grant,
                            unregister_error.is_some() || cleanup_error.is_some(),
                        )
                        .await;
                    if let Some(delete_error) = delete_error {
                        return Err(IdentityRuntimeError::Internal(format!(
                            "{detail}; tentative continuity cleanup failed: {delete_error}"
                        )));
                    }
                    return Err(IdentityRuntimeError::Internal(detail));
                }
            };
            new_record.checkpoint_version = effective_checkpoint_version;
            tracing::debug!(
                identity = %identity,
                runtime_id = %new_record.agent_runtime_id,
                session_id = %new_record.session_id,
                checkpoint_version = new_record.checkpoint_version.get(),
                "reset bridge session runtime state registered",
            );

            if let Err(err) = self
                .continuity_store
                .upsert_continuity_record(&new_record, grant.fencing_token)
                .await
            {
                tracing::warn!(
                    identity = %identity,
                    runtime_id = %new_record.agent_runtime_id,
                    session_id = %new_record.session_id,
                    error = %err,
                    "reset final continuity upsert failed after bridge materialization; rolling back new generation",
                );
                let unregister_error = bridge
                    .unregister_session_runtime_state(&new_record.session_id)
                    .await
                    .err();
                let cleanup_error = bridge
                    .retire_member(&new_record.agent_runtime_id)
                    .await
                    .err();
                let rollback_error = self
                    .continuity_store
                    .rollback_continuity_record(
                        &new_record,
                        registered_entry.continuity.as_ref(),
                        grant.fencing_token,
                    )
                    .await
                    .err();
                if rollback_error.is_some() {
                    self.restore_broken_entry_from_authoritative_continuity(
                        identity,
                        registered_entry,
                        &grant,
                    )
                    .await;
                } else {
                    self.restore_broken_entry_and_release_grant(identity, registered_entry, &grant)
                        .await;
                }
                if unregister_error.is_some() || cleanup_error.is_some() || rollback_error.is_some()
                {
                    return Err(IdentityRuntimeError::Internal(format!(
                        "continuity upsert after reset: {err}{}{}{}",
                        unregister_error
                            .as_ref()
                            .map(|e| format!("; unregister session failed: {e}"))
                            .unwrap_or_default(),
                        cleanup_error
                            .as_ref()
                            .map(|e| format!("; cleanup retire failed: {e}"))
                            .unwrap_or_default(),
                        rollback_error
                            .as_ref()
                            .map(|e| format!("; continuity rollback failed: {e}"))
                            .unwrap_or_default()
                    )));
                }
                return Err(IdentityRuntimeError::Store(err));
            }

            // Update runtime state
            let mut entries = self.entries.write().await;
            let Some(entry) = entries.get_mut(identity) else {
                drop(entries);
                tracing::warn!(
                    identity = %identity,
                    runtime_id = %new_record.agent_runtime_id,
                    session_id = %new_record.session_id,
                    "reset entry disappeared after bridge materialization; rolling back new generation",
                );
                let _ = bridge
                    .unregister_session_runtime_state(&new_record.session_id)
                    .await;
                let _ = bridge.retire_member(&new_record.agent_runtime_id).await;
                if let Err(err) = self
                    .continuity_store
                    .rollback_continuity_record(
                        &new_record,
                        registered_entry.continuity.as_ref(),
                        grant.fencing_token,
                    )
                    .await
                {
                    tracing::warn!(
                        %identity,
                        error = %err,
                        "failed to roll back continuity after reset entry disappeared"
                    );
                }
                if let Err(err) = self
                    .release_or_park_untracked_leases(std::slice::from_ref(&grant))
                    .await
                {
                    tracing::warn!(
                        %identity,
                        error = %err,
                        "failed to release lease after reset entry disappeared; exact grant parked for retry"
                    );
                }
                return Err(IdentityRuntimeError::UnknownIdentity(identity.clone()));
            };
            entry.continuity = Some(new_record.clone());
            entry.lease = Some(Self::lease_entry_from_grant(&grant));
            entry.state = IdentityLifecycleState::Active;
            entry.checkpoint_version = new_record.checkpoint_version;
            tracing::debug!(
                identity = %identity,
                runtime_id = %new_record.agent_runtime_id,
                session_id = %new_record.session_id,
                "reset completed",
            );
            drop(entries);

            // The prior bridge projection remains rollback authority until
            // the final continuity row and in-memory entry both commit. In
            // particular, blocking continuity stores introduce scheduler
            // yield points here, so scheduling cleanup before the final
            // upsert can unregister the old session while rollback is still
            // possible.
            // NO old-member retire debt. The authoritative successor transition
            // (respawn) terminally retired the predecessor row inside the same
            // operation, so scheduling a retire here would manufacture debt for
            // work another authority already committed - and because respawn
            // always advances the generation, the old/new filter below would
            // never skip it. Reset would return success while the runtime
            // accumulated retryable cleanup it could never discharge.
            //
            // `retire_reset_superseded_member` is deliberately NOT made tolerant
            // of an absent member: that would hide wrong ownership here and mask
            // genuinely missing members everywhere else it is used.
            let cleanup_old_runtime_id: Option<AgentRuntimeId> = None;
            let _ = &old_runtime_id;
            // The old SESSION is still MobKit's to unregister: it is a
            // continuity concern rather than a roster row, so respawn does not
            // touch it. If that unregistration fails it stays exact and
            // retryable, unchanged.
            let cleanup_old_session_id = old_session_id
                .as_ref()
                .filter(|old_session_id| *old_session_id != &new_record.session_id)
                .cloned();
            let reset_memory_injector = self.agent_memory.read().await.clone();
            let reset_memory_capture = reset_memory_injector.as_ref().and_then(|injector| {
                registered_entry.continuity.as_ref().map(|old_continuity| {
                    PendingResetMemoryCapture {
                        injector: injector.clone(),
                        identity: identity.clone(),
                        session_key: old_continuity.session_id.to_string(),
                        generation: old_continuity.generation.get(),
                    }
                })
            });
            // Record exact cleanup debt immediately after the durable and
            // in-memory replacement commit. The owned debt carries its memory
            // capture prerequisite, so reset remains latency-neutral while
            // graceful shutdown still joins/retries the complete sequence.
            let reset_bridge_cleanup = self
                .record_old_bridge_cleanup_after_reset(
                    cleanup_old_runtime_id,
                    cleanup_old_session_id,
                    reset_memory_capture.clone(),
                )
                .await;
            // §10.1: reset is the deliberate clean-slate boundary — clear
            // session taint explicitly (rotation clears implicitly; this
            // also drops pending pre-attribution taint). Mark the outgoing
            // boundary synchronously; the runtime-owned cleanup task performs
            // bounded distillation before it may CAS-delete the superseded
            // session projection. This preserves detached reset latency
            // without racing the evidence source.
            if let Some(injector) = reset_memory_injector.as_ref() {
                injector.clear_taint_for_identity(identity);
                injector.note_session_generation(
                    identity,
                    &new_record.session_id.to_string(),
                    new_record.generation.get(),
                );
                if let Some(old_continuity) = registered_entry.continuity.as_ref() {
                    let old_session_key = old_continuity.session_id.to_string();
                    injector.note_reset_boundary(&old_session_key);
                    injector.note_session_generation(
                        identity,
                        &old_session_key,
                        old_continuity.generation.get(),
                    );
                }
            }

            if let Some(cleanup) = reset_bridge_cleanup {
                self.spawn_old_bridge_cleanup_after_reset(bridge.clone(), cleanup)
                    .await;
            }
            tracing::debug!(
                identity = %identity,
                runtime_id = %new_record.agent_runtime_id,
                session_id = %new_record.session_id,
                "reset old bridge cleanup debt recorded after continuity commit",
            );
            self.mark_bootstrap_from_lifecycle(identity, IdentityLifecycleState::Active, None);
            #[cfg(feature = "extension-state")]
            if let Some(publication) = extension_publication {
                publication.publish();
            }
            return Ok(new_record);
        }

        // Persist the new record (fencing token from new lease protects against old writes)
        if let Err(err) = self
            .continuity_store
            .upsert_continuity_record(&new_record, grant.fencing_token)
            .await
        {
            self.restore_entry_with_grant(identity, registered_entry, &grant)
                .await;
            return Err(IdentityRuntimeError::Store(err));
        }

        // No bridge — update runtime state only (validation mode)
        let mut entries = self.entries.write().await;
        let Some(entry) = entries.get_mut(identity) else {
            drop(entries);
            if let Err(err) = self
                .continuity_store
                .rollback_continuity_record(
                    &new_record,
                    registered_entry.continuity.as_ref(),
                    grant.fencing_token,
                )
                .await
            {
                tracing::warn!(
                    %identity,
                    error = %err,
                    "failed to roll back continuity after reset validation entry disappeared"
                );
            }
            if let Err(err) = self
                .release_or_park_untracked_leases(std::slice::from_ref(&grant))
                .await
            {
                tracing::warn!(
                    %identity,
                    error = %err,
                    "failed to release lease after reset validation entry disappeared; exact grant parked for retry"
                );
            }
            return Err(IdentityRuntimeError::UnknownIdentity(identity.clone()));
        };
        entry.continuity = Some(new_record.clone());
        entry.lease = Some(Self::lease_entry_from_grant(&grant));
        entry.state = IdentityLifecycleState::Active;
        entry.checkpoint_version = CheckpointVersion::new(0);
        drop(entries);
        if let Some(injector) = self.agent_memory.read().await.as_ref() {
            injector.clear_taint_for_identity(identity);
            // No-bridge (validation) reset: same §8.4 boundary semantics.
            if let Some(old_continuity) = registered_entry.continuity.as_ref() {
                let old_session_key = old_continuity.session_id.to_string();
                injector.note_reset_boundary(&old_session_key);
                injector.spawn_rotation_distillation(
                    identity,
                    &old_session_key,
                    crate::memory::distiller::DistillCause::Reset,
                );
            }
        }

        self.mark_bootstrap_from_lifecycle(identity, IdentityLifecycleState::Active, None);
        Ok(new_record)
    }

    // -----------------------------------------------------------------------
    // Lifecycle: delete_identity() — REQ-11
    // -----------------------------------------------------------------------

    /// Delete an identity: removes continuity record.
    ///
    /// 1. Fence old owner
    /// 2. Remove ContinuityRecord
    /// 3. Future bootstrap treats identity as Uninitialized
    pub async fn delete_identity(
        &self,
        identity: &AgentIdentity,
    ) -> Result<(), IdentityRuntimeError> {
        self.delete_identity_with_expected_member_alias(identity, None)
            .await
    }

    async fn delete_identity_with_expected_member_alias(
        &self,
        identity: &AgentIdentity,
        expected_alias: Option<&str>,
    ) -> Result<(), IdentityRuntimeError> {
        let lifecycle_lock = self.lifecycle_lock_for(identity).await;
        let _lifecycle_guard = lifecycle_lock.lock().await;
        if let Some(expected_alias) = expected_alias {
            self.ensure_expected_member_alias_current(identity, expected_alias)
                .await?;
        }
        self.delete_identity_locked(identity).await
    }

    async fn delete_identity_locked(
        &self,
        identity: &AgentIdentity,
    ) -> Result<(), IdentityRuntimeError> {
        let registered_entry = self
            .mark_lifecycle_in_progress(identity, IdentityLifecycleState::Retiring)
            .await?;
        let runtime_id = registered_entry
            .continuity
            .as_ref()
            .map(|c| c.agent_runtime_id.clone());
        let session_id = registered_entry
            .continuity
            .as_ref()
            .map(|c| c.session_id.clone());

        // INV-05: fence the old owner first
        let acquire_result = match self
            .lease_provider
            .acquire_leases(std::slice::from_ref(identity), &self.runtime_instance_id)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                self.restore_entry(identity, registered_entry).await;
                return Err(IdentityRuntimeError::Lease(err));
            }
        };

        let grant = match acquire_result.get(identity) {
            Some(super::types::LeaseAcquireResult::Acquired(g)) => g.clone(),
            _ => {
                self.restore_entry(identity, registered_entry).await;
                return Err(IdentityRuntimeError::NoActiveLease(identity.clone()));
            }
        };
        if let Err(err) = self
            .advance_existing_continuity_fence(identity, &registered_entry, &grant)
            .await
        {
            self.restore_broken_entry_and_release_grant(identity, registered_entry, &grant)
                .await;
            return Err(err);
        }
        // The durable fence now belongs to the delete transaction. Refresh
        // the bridge adapter before retirement so Meerkat's terminal archive
        // projection carries that same token. Leaving the live session on the
        // prior token makes ArchiveSession fail closed, retains a Retiring Mob
        // anchor, and later prevents strict shutdown attestation.
        if let Some(record) = registered_entry.continuity.as_ref()
            && let Err(error) = self
                .refresh_existing_session_runtime_state(identity, record, &grant)
                .await
        {
            self.restore_broken_entry_with_fenced_store(identity, registered_entry, &grant)
                .await;
            return Err(IdentityRuntimeError::Internal(format!(
                "bridge refresh session authority before delete: {error}"
            )));
        }

        // §8.4 trigger (b): delete is the identity's LAST boundary — harvest
        // the outgoing session before teardown (its exit-interview analog,
        // §8.5, is the steward's; the distillate is what it will read).
        // Bounded; deletion proceeds at the pre-rotation timeout.
        if let Some(injector) = self.agent_memory.read().await.clone() {
            if let Some(session_id) = session_id.as_ref() {
                let session_key = session_id.to_string();
                injector
                    .distill_before_rotation(
                        identity,
                        &session_key,
                        crate::memory::distiller::DistillCause::Delete,
                    )
                    .await;
                // Ask 2 GC: delete permanently abandons this session id, and
                // its knowledge has just been distilled into the identity-keyed
                // MobKit store (which the exit-interview harvest below reads —
                // a DIFFERENT store from the meerkat session-memory scope). So
                // reclaiming the orphaned meerkat scope here frees dead
                // re-embed weight without starving any downstream read.
                injector
                    .drop_orphaned_session_scope(
                        &session_key,
                        crate::memory::distiller::DistillCause::Delete,
                    )
                    .await;
            }
            // §8.5 exit interview (delete is the identity's LAST boundary).
            injector
                .note_identity_retired(
                    identity,
                    session_id
                        .as_ref()
                        .map(std::string::ToString::to_string)
                        .as_deref(),
                    "delete",
                )
                .await;
        }

        // Retire the mob member through the session bridge before removing
        // the continuity record. This ensures the mob actor is cleaned up.
        if let (Some(bridge), Some(rid)) = (&self.bridge, &runtime_id)
            && let Err(err) = bridge.retire_member(rid).await
        {
            self.restore_entry_with_grant(identity, registered_entry, &grant)
                .await;
            return Err(IdentityRuntimeError::Internal(format!(
                "bridge retire before delete: {err}"
            )));
        }

        if let (Some(bridge), Some(session_id)) = (&self.bridge, &session_id)
            && let Some(err) = Self::unregister_bridge_session_runtime_states(
                bridge.as_ref(),
                std::slice::from_ref(session_id),
            )
            .await
        {
            self.restore_broken_entry_with_fenced_store(identity, registered_entry, &grant)
                .await;
            return Err(IdentityRuntimeError::Internal(format!(
                "bridge unregister session before delete: {err}"
            )));
        }

        // Destroy-deprojection (2026-07-31 verdict): the durable session row
        // must not outlive the identity - a leftover external body is
        // exactly what the ephemeral-runtime-store activation mint would
        // faithfully resurrect on the next cold pod. Projection writes are
        // already quiesced (member retired, session unregistered) and this
        // delete transaction owns the ADVANCED identity fence, so the
        // revision-CAS delete cannot race a live writer. `delete_continuity_record` below removes any
        // remainder atomically for conforming stores; a store that cannot
        // support session-scoped deletion (`Ok(false)` default) or a row the
        // current decoder cannot token (an unimported released envelope)
        // keeps the record-scoped contract and is surfaced loudly instead of
        // silently retained.
        if let Some(session_id) = session_id.as_ref() {
            match self
                .continuity_store
                .load_session_snapshot(session_id)
                .await
            {
                Ok(Some(snapshot)) => {
                    let cas_token = serde_json::from_slice::<meerkat_core::Session>(&snapshot.data)
                        .ok()
                        .and_then(|current| {
                            meerkat_core::session_store::session_projection_cas_token(&current).ok()
                        });
                    match cas_token {
                        Some(token) => match self
                            .continuity_store
                            .delete_session_snapshot_if_current_revision(session_id, &token)
                            .await
                        {
                            Ok(true) => {}
                            Ok(false) => {
                                tracing::warn!(
                                    identity = %identity,
                                    session_id = %session_id,
                                    "session-scoped snapshot delete unsupported or superseded; \
                                     relying on delete_continuity_record's record-scoped \
                                     deletion - a non-conforming external store may retain a \
                                     resurrectable session body"
                                );
                            }
                            Err(err) => {
                                self.restore_broken_entry_and_release_grant(
                                    identity,
                                    registered_entry,
                                    &grant,
                                )
                                .await;
                                return Err(IdentityRuntimeError::Store(err));
                            }
                        },
                        None => {
                            tracing::warn!(
                                identity = %identity,
                                session_id = %session_id,
                                "durable session row cannot be revision-tokened for CAS \
                                 delete; relying on delete_continuity_record's record-scoped \
                                 deletion"
                            );
                        }
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    self.restore_broken_entry_and_release_grant(identity, registered_entry, &grant)
                        .await;
                    return Err(IdentityRuntimeError::Store(err));
                }
            }
        }

        // Remove authoritative continuity record from the store
        if let Err(err) = self
            .continuity_store
            .delete_continuity_record(identity, grant.fencing_token)
            .await
        {
            self.restore_broken_entry_and_release_grant(identity, registered_entry, &grant)
                .await;
            return Err(IdentityRuntimeError::Store(err));
        }

        // Authoritative deletion is committed before ownership is released,
        // so another holder can never race the continuity delete. Surface a
        // provider release failure explicitly; the still-held external grant
        // then remains the fail-closed ownership fence.
        let release_result = self
            .lease_provider
            .release_leases(std::slice::from_ref(&grant))
            .await;

        match release_result {
            Ok(()) => {
                self.event_channels.write().await.remove(identity);
                self.entries.write().await.remove(identity);
                // The identity remains part of the desired bootstrap roster
                // until a roster reconcile removes it. Successful deletion
                // therefore makes readiness Dormant.
                self.mark_bootstrap_from_lifecycle(identity, IdentityLifecycleState::Dormant, None);
                Ok(())
            }
            Err(error) => {
                // The physical member and continuity row are already gone,
                // but the provider still owns this exact fencing grant. Keep
                // an explicit Broken tombstone so reconcile and shutdown can
                // retry it; removing the entry here would make non-expiring
                // provider authority permanently unreachable.
                let mut entries = self.entries.write().await;
                let entry = entries
                    .get_mut(identity)
                    .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
                entry.state = IdentityLifecycleState::Broken;
                entry.continuity = None;
                entry.lease = None;
                entry.pending_lease_release = Some(grant);
                drop(entries);
                self.mark_bootstrap_from_lifecycle(
                    identity,
                    IdentityLifecycleState::Broken,
                    Some(format!("lease release after delete: {error}")),
                );
                Err(IdentityRuntimeError::Lease(error))
            }
        }
    }

    // -----------------------------------------------------------------------
    // Checkpoint — REQ-14, REQ-15, REQ-16, REQ-17
    // -----------------------------------------------------------------------

    /// Save a checkpoint snapshot. Enforces version ordering and fencing.
    pub async fn checkpoint(
        &self,
        identity: &AgentIdentity,
        snapshot: &SessionSnapshot,
    ) -> Result<CheckpointVersion, IdentityRuntimeError> {
        let lifecycle_lock = self.lifecycle_lock_for(identity).await;
        let _lifecycle_guard = lifecycle_lock.lock().await;
        {
            let entries = self.entries.read().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            if entry.state != IdentityLifecycleState::Active {
                return Err(IdentityRuntimeError::InvalidState {
                    identity: identity.clone(),
                    state: entry.state,
                    operation: "checkpoint",
                });
            }
        }

        let token = self.ensure_active_lease(identity).await?;
        let (record, new_version) = {
            let entries = self.entries.read().await;
            let entry = entries
                .get(identity)
                .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
            let record = entry
                .continuity
                .as_ref()
                .ok_or_else(|| {
                    IdentityRuntimeError::Internal(format!("no continuity record for {identity}"))
                })?
                .clone();

            let new_version = CheckpointVersion::new(entry.checkpoint_version.get() + 1);
            (record, new_version)
        };

        // REQ-15 + REQ-16: store enforces version ordering and fencing
        self.continuity_store
            .save_session_snapshot(
                identity,
                &record.session_id,
                record.generation,
                new_version,
                token,
                snapshot,
            )
            .await?;

        // Update local checkpoint version
        {
            let mut entries = self.entries.write().await;
            if let Some(entry) = entries.get_mut(identity) {
                entry.checkpoint_version = new_version;
            }
        }

        self.emit_event(
            identity,
            IdentityEvent::CheckpointCompleted {
                identity: identity.clone(),
                version: new_version,
            },
        )
        .await;

        Ok(new_version)
    }

    // -----------------------------------------------------------------------
    // Roster inspection — REQ-32
    // -----------------------------------------------------------------------

    /// Return all active identities with their specs and status.
    pub async fn roster_inspect(
        &self,
    ) -> BTreeMap<AgentIdentity, (DurableAgentSpec, IdentityStatus)> {
        let entries = self.entries.read().await;
        let mut result = BTreeMap::new();
        for (identity, entry) in entries.iter() {
            let lease_info = entry.lease.as_ref().map(|l| LeaseInfo {
                fencing_token: l.fencing_token,
                ttl_remaining: l.ttl_remaining(),
                healthy: l.is_healthy(),
            });
            let continuity_health = Some(ContinuityHealth {
                store_reachable: true,
                durability_policy: self.durability_policy.clone(),
                last_checkpoint_version: if entry.checkpoint_version.get() > 0 {
                    Some(entry.checkpoint_version)
                } else {
                    None
                },
            });
            let status = IdentityStatus {
                identity: identity.clone(),
                state: entry.state,
                agent_runtime_id: entry
                    .continuity
                    .as_ref()
                    .map(|c| c.agent_runtime_id.clone()),
                session_id: entry.continuity.as_ref().map(|c| c.session_id.clone()),
                profile: Some(entry.spec.profile.clone()),
                runtime_mode: entry.spec.runtime_mode_override,
                addressability: entry.spec.addressability,
                display_name: entry.spec.display_name.clone(),
                labels: entry.spec.labels.clone(),
                generation: entry.continuity.as_ref().map(|c| c.generation),
                checkpoint_version: if entry.checkpoint_version.get() > 0 {
                    Some(entry.checkpoint_version)
                } else {
                    None
                },
                lease: lease_info,
                continuity_health,
                continuity_unrecoverable: entry.continuity_unrecoverable.clone(),
                session_repair_required: entry.session_repair_required.clone(),
                customizer_tools_pending: self.customizer_tools_pending_for(identity),
            };
            result.insert(identity.clone(), (entry.spec.clone(), status));
        }
        result
    }

    // -----------------------------------------------------------------------
    // Roster uniqueness validation — INV-06
    // -----------------------------------------------------------------------

    /// Validate that a roster contains no duplicate identities.
    pub fn validate_roster_uniqueness(
        specs: &[DurableAgentSpec],
    ) -> Result<(), IdentityRuntimeError> {
        let mut seen = std::collections::BTreeSet::new();
        for spec in specs {
            if !seen.insert(&spec.identity) {
                return Err(IdentityRuntimeError::DuplicateIdentity(
                    spec.identity.clone(),
                ));
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Accessors for internal state
    // -----------------------------------------------------------------------

    /// Get the current entries (read-only snapshot).
    #[allow(dead_code)]
    pub(crate) async fn entries(&self) -> BTreeMap<AgentIdentity, IdentityEntry> {
        self.entries.read().await.clone()
    }

    /// Check if an identity is registered.
    pub async fn contains(&self, identity: &AgentIdentity) -> bool {
        self.entries.read().await.contains_key(identity)
    }

    /// Check if an identity is registered AND in Active state.
    pub async fn is_active(&self, identity: &AgentIdentity) -> bool {
        self.entries
            .read()
            .await
            .get(identity)
            .is_some_and(|e| e.state == IdentityLifecycleState::Active)
    }

    fn identity_from_member_alias(alias: &str) -> Option<(AgentIdentity, bool)> {
        let alias = crate::member_comms_id::runtime_alias_str(alias);
        let alias = alias.as_ref();
        alias
            .strip_prefix("rt:")
            .and_then(|rest| rest.rsplit_once(':'))
            .filter(|(identity, generation)| {
                !identity.is_empty()
                    && !generation.is_empty()
                    && generation.chars().all(|ch| ch.is_ascii_digit())
            })
            .and_then(|(identity, _)| AgentIdentity::parse(identity).ok())
            .map(|identity| (identity, true))
            .or_else(|| {
                AgentIdentity::parse(alias)
                    .ok()
                    .map(|identity| (identity, false))
            })
    }

    /// Parse the durable owner encoded in the reserved generated-alias
    /// namespace, whether or not that identity is still registered. Mutating
    /// member surfaces use this to fail closed after a concurrent delete
    /// instead of treating an orphaned `rt:*` alias as an ordinary mob member.
    pub(crate) fn identity_for_generated_member_alias(alias: &str) -> Option<AgentIdentity> {
        Self::identity_from_member_alias(alias).and_then(|(identity, generated)| {
            if generated { Some(identity) } else { None }
        })
    }

    /// Resolve a mutating member request without allowing a generated alias
    /// to fall back to the raw mob plane after concurrent deletion. Plain
    /// durable identities retain their historical registered-only behavior.
    pub(crate) async fn identity_for_member_mutation(&self, alias: &str) -> Option<AgentIdentity> {
        match Self::identity_from_member_alias(alias) {
            Some((identity, true)) => Some(identity),
            Some((identity, false)) => self.contains(&identity).await.then_some(identity),
            None => None,
        }
    }

    async fn ensure_expected_member_alias_current(
        &self,
        identity: &AgentIdentity,
        expected_alias: &str,
    ) -> Result<(), IdentityRuntimeError> {
        let canonical_alias = crate::member_comms_id::runtime_alias_str(expected_alias);
        let Some((alias_identity, generated_runtime_alias)) =
            Self::identity_from_member_alias(canonical_alias.as_ref())
        else {
            return Err(IdentityRuntimeError::UnknownIdentity(identity.clone()));
        };
        if alias_identity != *identity {
            return Err(IdentityRuntimeError::UnknownIdentity(alias_identity));
        }
        let entries = self.entries.read().await;
        let entry = entries
            .get(identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
        if generated_runtime_alias {
            let current = entry
                .continuity
                .as_ref()
                .map(|record| record.agent_runtime_id.clone());
            if current.as_ref().map(AgentRuntimeId::as_str) != Some(canonical_alias.as_ref()) {
                return Err(IdentityRuntimeError::StaleRuntimeAlias {
                    identity: identity.clone(),
                    requested: canonical_alias.into_owned(),
                    current,
                });
            }
        }
        Ok(())
    }

    /// Resolve a member alias to the durable identity that OWNS it, if any.
    ///
    /// Accepts both a generated runtime alias (`rt:<identity>:<generation>`)
    /// and a plain identity string; returns the identity only when it is
    /// actually registered in this runtime. Member-scoped RPCs use this to
    /// route lifecycle mutations of identity-owned members through the
    /// identity authority — a classic `handle.retire()`/`respawn()` on such
    /// a member would mutate it behind the IdentityRuntime's back (stale
    /// continuity binding, generation drift), which is the doctrine's
    /// "mob plane must not mangle durables" rule.
    pub async fn owned_identity_for_member_alias(&self, alias: &str) -> Option<AgentIdentity> {
        let (identity, _) = Self::identity_from_member_alias(alias)?;
        self.contains(&identity).await.then_some(identity)
    }

    /// Identities currently in the Broken lifecycle state.
    pub async fn broken_identities(&self) -> Vec<AgentIdentity> {
        self.entries
            .read()
            .await
            .iter()
            .filter(|(_, entry)| entry.state == IdentityLifecycleState::Broken)
            .map(|(identity, _)| identity.clone())
            .collect()
    }

    /// Broken identities that do NOT carry a terminal heal verdict — the set
    /// the continuity repair supervisor is allowed to keep retrying.
    pub async fn repairable_broken_identities(&self) -> Vec<AgentIdentity> {
        self.entries
            .read()
            .await
            .iter()
            .filter(|(_, entry)| {
                entry.state == IdentityLifecycleState::Broken
                    && entry.continuity_unrecoverable.is_none()
                    && entry.session_repair_required.is_none()
                    && entry
                        .host_rejected_build_park
                        .as_ref()
                        .is_none_or(|park| park.spec_digest != durable_spec_digest(&entry.spec))
            })
            .map(|(identity, _)| identity.clone())
            .collect()
    }

    /// The terminal heal verdict recorded for an identity, if any.
    pub async fn continuity_unrecoverable(
        &self,
        identity: &AgentIdentity,
    ) -> Option<ContinuityUnrecoverable> {
        self.entries
            .read()
            .await
            .get(identity)
            .and_then(|entry| entry.continuity_unrecoverable.clone())
    }

    /// The typed "session needs repair" hold recorded for an identity, if any.
    pub async fn session_repair_required(
        &self,
        identity: &AgentIdentity,
    ) -> Option<SessionRepairRequired> {
        self.entries
            .read()
            .await
            .get(identity)
            .and_then(|entry| entry.session_repair_required.clone())
    }

    /// Record meerkat's typed audited-endpoint divergence against a Broken
    /// identity as a [`SessionRepairRequired`] hold carrying the exact repair
    /// commands.
    ///
    /// Returns `false` (without writing) when the identity is unknown or not
    /// Broken: the hold only ever parks an already-Broken entry. While
    /// recorded, the repair supervisor skips the identity and reconcile keeps
    /// its Broken projection; `reload_member` re-attempts the resume once the
    /// operator has repaired the document.
    pub async fn mark_session_repair_required(
        &self,
        identity: &AgentIdentity,
        session_id: meerkat_core::types::SessionId,
        detail: impl Into<String>,
    ) -> bool {
        let hold = SessionRepairRequired::audited_endpoint_divergence(
            session_id,
            self.session_repair_scope().as_ref(),
            detail,
        );
        let reason = hold.operator_reason();
        let marked = {
            let mut entries = self.entries.write().await;
            match entries.get_mut(identity) {
                Some(entry) if entry.state == IdentityLifecycleState::Broken => {
                    entry.session_repair_required = Some(hold);
                    true
                }
                _ => false,
            }
        };
        if marked {
            tracing::error!(
                %identity,
                reason = %reason,
                "identity parked: durable session needs the sanctioned audited-endpoint repair"
            );
            self.mark_bootstrap_from_lifecycle(
                identity,
                IdentityLifecycleState::Broken,
                Some(reason),
            );
        }
        marked
    }

    /// Project a live or Dormant identity Broken with the typed repair hold
    /// and hand back the typed failure the caller returns. Used by the doors
    /// that learn about the divergence outside the resume path (the
    /// registration reload of an Active member).
    async fn park_session_repair_required(
        &self,
        identity: &AgentIdentity,
        session_id: meerkat_core::types::SessionId,
        detail: String,
    ) -> IdentityRuntimeError {
        let (record, transitioned) = {
            let mut entries = self.entries.write().await;
            let record = entries
                .get(identity)
                .and_then(|entry| entry.continuity.clone());
            let mut transitioned = false;
            if let Some(entry) = entries.get_mut(identity) {
                transitioned = entry.state != IdentityLifecycleState::Broken;
                entry.state = IdentityLifecycleState::Broken;
            }
            (record, transitioned)
        };
        if transitioned {
            self.emit_event(
                identity,
                IdentityEvent::StateChanged {
                    identity: identity.clone(),
                    new_state: IdentityLifecycleState::Broken,
                },
            )
            .await;
        }
        if !self
            .mark_session_repair_required(identity, session_id, detail.clone())
            .await
        {
            tracing::debug!(
                %identity,
                "identity left Broken before the session repair hold could be recorded"
            );
        }
        IdentityRuntimeError::EmbodimentRejected(Box::new(ContinuityFailure {
            identity: identity.clone(),
            kind: ContinuityFailureKind::RepairRequired,
            record,
            detail,
        }))
    }

    /// Record a terminal heal verdict against a Broken identity.
    ///
    /// Returns `false` (without writing) when the identity is unknown or no
    /// longer Broken — the verdict only ever parks an already-Broken entry;
    /// it never degrades a live one. While recorded, the repair supervisor
    /// skips the identity and reconcile keeps its Broken projection instead
    /// of cosmetically resetting it (2026-07-29 heal/re-Break incident).
    pub async fn mark_continuity_unrecoverable(
        &self,
        identity: &AgentIdentity,
        reason: String,
    ) -> bool {
        let marked = {
            let mut entries = self.entries.write().await;
            match entries.get_mut(identity) {
                Some(entry) if entry.state == IdentityLifecycleState::Broken => {
                    entry.continuity_unrecoverable = Some(ContinuityUnrecoverable {
                        reason: reason.clone(),
                    });
                    true
                }
                _ => false,
            }
        };
        if marked {
            // Keep the bootstrap status surface honest about WHY the
            // identity stays broken (operators read this, not the log).
            self.mark_bootstrap_from_lifecycle(
                identity,
                IdentityLifecycleState::Broken,
                Some(reason),
            );
        }
        marked
    }

    /// Clear a previously recorded terminal heal verdict (operator retry).
    pub async fn clear_continuity_unrecoverable(&self, identity: &AgentIdentity) -> bool {
        let mut entries = self.entries.write().await;
        match entries.get_mut(identity) {
            Some(entry) => entry.continuity_unrecoverable.take().is_some(),
            None => false,
        }
    }

    /// The ACTIVE host-rejected-build park for an identity: `Some` only
    /// while the identity's current spec still digests to the parked value.
    /// A mismatch (the roster/policy changed) clears the park in place and
    /// returns `None` — the retry is permitted.
    pub async fn host_rejected_build_park(
        &self,
        identity: &AgentIdentity,
    ) -> Option<HostRejectedBuildPark> {
        let mut entries = self.entries.write().await;
        let entry = entries.get_mut(identity)?;
        let park = entry.host_rejected_build_park.clone()?;
        if park.spec_digest == durable_spec_digest(&entry.spec) {
            Some(park)
        } else {
            entry.host_rejected_build_park = None;
            None
        }
    }

    /// Park an identity whose build was deterministically rejected (the host
    /// gate or a deterministic provider-auth kind, see
    /// [`deterministic_build_rejection_reason`]), scoped to the identity's
    /// CURRENT spec. Operator-visible through the
    /// bootstrap status surface and the warn line; no automatic retry until
    /// the spec changes.
    pub(crate) async fn mark_host_rejected_build_park(
        &self,
        identity: &AgentIdentity,
        reason: String,
    ) -> bool {
        let marked_state = {
            let mut entries = self.entries.write().await;
            match entries.get_mut(identity) {
                Some(entry) => {
                    entry.host_rejected_build_park = Some(HostRejectedBuildPark {
                        reason: reason.clone(),
                        spec_digest: durable_spec_digest(&entry.spec),
                    });
                    Some(entry.state)
                }
                None => None,
            }
        };
        let Some(state) = marked_state else {
            return false;
        };
        tracing::warn!(
            identity = %identity,
            reason = %reason,
            "this identity's build was deterministically rejected; parking (no automatic \
             retry) until the roster spec changes"
        );
        // Keep the bootstrap status surface honest about WHY the identity is
        // stuck (operators read this, not the log). The detail renders on
        // Broken projections; for a Dormant park the typed materialize error
        // carries the reason to every send instead.
        self.mark_bootstrap_from_lifecycle(identity, state, Some(reason));
        true
    }

    /// Clear a host-rejected-build park (operator retry with an unchanged
    /// spec — e.g. after fixing the app-side gate's policy out of band).
    pub async fn clear_host_rejected_build_park(&self, identity: &AgentIdentity) -> bool {
        let mut entries = self.entries.write().await;
        match entries.get_mut(identity) {
            Some(entry) => entry.host_rejected_build_park.take().is_some(),
            None => false,
        }
    }

    /// The durable session currently bound to an identity, if any.
    pub(crate) async fn continuity_session_id(
        &self,
        identity: &AgentIdentity,
    ) -> Option<SessionId> {
        self.entries
            .read()
            .await
            .get(identity)
            .and_then(|entry| entry.continuity.as_ref().map(|c| c.session_id.clone()))
    }

    /// Get the continuity store reference.
    pub fn continuity_store(&self) -> &Arc<dyn ContinuityStore> {
        &self.continuity_store
    }

    /// Get the lease provider reference.
    pub fn lease_provider(&self) -> &Arc<dyn LeaseProvider> {
        &self.lease_provider
    }

    /// Get the runtime instance ID.
    pub fn runtime_instance_id(&self) -> &str {
        &self.runtime_instance_id
    }

    /// Get the durability policy.
    pub fn durability_policy(&self) -> &DurabilityPolicy {
        &self.durability_policy
    }

    /// Get whether a runtime store is configured.
    pub fn has_runtime_store(&self) -> bool {
        self.has_runtime_store
    }

    /// Get the session bridge reference, if configured.
    pub fn bridge(&self) -> Option<&Arc<dyn SessionBridge>> {
        self.bridge.as_ref()
    }

    // -----------------------------------------------------------------------
    // Convenience methods
    // -----------------------------------------------------------------------

    /// Send plain text to an addressable identity.
    pub async fn send_text(
        &self,
        identity: &AgentIdentity,
        text: impl Into<String>,
    ) -> Result<FencingToken, IdentityRuntimeError> {
        self.send(identity, &meerkat_core::ContentInput::Text(text.into()))
            .await
    }

    /// Dispatch plain text with system origin.
    pub async fn dispatch_text(
        &self,
        identity: &AgentIdentity,
        text: impl Into<String>,
    ) -> Result<(FencingToken, bool), IdentityRuntimeError> {
        self.dispatch(identity, &DispatchInput::system(text)).await
    }

    /// Execute the restore flow for the given roster.
    pub async fn restore_flow(
        &self,
        roster: &[DurableAgentSpec],
        topology_provider: Option<&dyn super::contracts::TopologyProvider>,
        customizer: Option<&dyn super::contracts::AgentCustomizer>,
    ) -> Result<super::orchestrator::RestoreFlowResult, IdentityRuntimeError> {
        super::orchestrator::restore_flow(self, roster, topology_provider, customizer).await
    }

    /// Resolve the AgentRuntimeId for a registered identity.
    /// # NOT a roster key
    ///
    /// This returns the identity's CURRENT INCARNATION - `rt:{identity}:{gen}` -
    /// which is binding detail. Since the stable-identity lowering the mob roster
    /// is keyed by the bare DURABLE identity, so encoding this result into a
    /// member id yields a valid-looking id for a member that does not exist, and
    /// operations on it fail with "mob member not found" about a healthy member.
    ///
    /// For "which roster row is this identity", use
    /// [`crate::member_comms_id::roster_member_id_for_identity`].
    ///
    /// This distinction cost a downstream fleet a zeroed board: our resolver
    /// handed out this spelling, the consumer encoded it as documented, and wire
    /// rejected it - two halves of one public API disagreeing about what a roster
    /// key is.
    pub async fn runtime_id_for(
        &self,
        identity: &AgentIdentity,
    ) -> Result<AgentRuntimeId, IdentityRuntimeError> {
        let entries = self.entries.read().await;
        let entry = entries
            .get(identity)
            .ok_or_else(|| IdentityRuntimeError::UnknownIdentity(identity.clone()))?;
        entry
            .continuity
            .as_ref()
            .map(|c| c.agent_runtime_id.clone())
            .ok_or_else(|| {
                IdentityRuntimeError::Internal(format!("no continuity record for {identity}"))
            })
    }

    /// Inspect the current execution state of an identity via the bridge.
    ///
    /// The bridge's inspection is a full meerkat member-status read, which
    /// the mob admits one at a time. So concurrent callers asking about the
    /// same member incarnation join one read in flight instead of each
    /// starting their own. A settled read is never reused: the next caller
    /// reads afresh. A caller joins only a read that started at or after the
    /// identity's completion cursor as the caller sees it, so an inspection
    /// never reports an older turn's output than the cursor read before it
    /// (`mobkit/inspect_identity` returns both, and SDK completion waits take
    /// the output once the cursor moves). A new incarnation (respawn, reset)
    /// has a new runtime id and is always read afresh.
    pub async fn inspect(
        &self,
        identity: &AgentIdentity,
    ) -> Result<super::bridge::MemberInspection, IdentityRuntimeError> {
        let runtime_id = self.runtime_id_for(identity).await?;
        let bridge = self
            .bridge
            .as_ref()
            .ok_or_else(|| IdentityRuntimeError::Internal("no bridge configured".to_string()))?;
        let seen = self.completion_cursor(identity).await;
        let shared = {
            let mut inspections = self
                .inspections
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            inspections.retain(|_, shared| shared.in_flight());
            match inspections.get(&runtime_id) {
                Some(shared) if shared.joinable_at(seen) => shared.clone(),
                _ => {
                    let shared = SharedInspection::start(seen);
                    inspections.insert(runtime_id.clone(), shared.clone());
                    shared
                }
            }
        };
        shared
            .read
            .get_or_init(|| async {
                bridge
                    .inspect_member(&runtime_id)
                    .await
                    .map_err(|e| format!("inspect: {e}"))
            })
            .await
            .clone()
            .map_err(IdentityRuntimeError::Internal)
    }

    // -----------------------------------------------------------------------
    // Continuity repair triggers
    // -----------------------------------------------------------------------

    /// Wake the continuity repair supervisor with a typed cause.
    pub(crate) fn trigger_continuity_repair(&self, trigger: ContinuityRepairTrigger) {
        self.continuity_repair_triggers.send_modify(|wake| {
            wake.epoch = wake.epoch.wrapping_add(1);
            wake.last = Some(trigger);
        });
    }

    /// The refusal of a delivery to an identity that is not Active. A Broken
    /// identity's refused delivery is a caller's demand for it, and fires the
    /// typed repair trigger.
    fn refuse_inactive_delivery(
        &self,
        identity: &AgentIdentity,
        state: IdentityLifecycleState,
        operation: &'static str,
    ) -> IdentityRuntimeError {
        if state == IdentityLifecycleState::Broken {
            self.trigger_continuity_repair(ContinuityRepairTrigger::DemandOnBroken);
        }
        IdentityRuntimeError::InvalidState {
            identity: identity.clone(),
            state,
            operation,
        }
    }

    fn subscribe_continuity_repair_triggers(&self) -> watch::Receiver<ContinuityRepairWake> {
        self.continuity_repair_triggers.subscribe()
    }

    /// Hold one embodiment burst in flight (a restore pass, a background
    /// warm). Between two members of a burst no single embodiment may be in
    /// flight; the burst keeps the count above zero until it settled.
    pub(super) fn enter_embodiment_burst(&self) -> EmbodimentInFlight<'_> {
        EmbodimentInFlight::enter(&self.embodiments_in_flight)
    }

    /// Wait until no embodiment is in flight; `false` when the supervisor
    /// was cancelled first.
    async fn embodiments_settled_or_cancelled(
        &self,
        cancellation: &mut Option<watch::Receiver<bool>>,
    ) -> bool {
        let mut in_flight = self.embodiments_in_flight.subscribe();
        loop {
            if *in_flight.borrow_and_update() == 0 {
                return true;
            }
            tokio::select! {
                changed = in_flight.changed() => {
                    if changed.is_err() {
                        return true;
                    }
                }
                () = repair_cancelled(cancellation) => return false,
            }
        }
    }

    /// What a repair request reaches right now: the repairable Broken
    /// identities, and whether a supervisor runs to act on them.
    pub(crate) async fn continuity_repair_request_outcome(&self) -> ContinuityRepairRequestOutcome {
        if self.repairable_broken_identities().await.is_empty() {
            ContinuityRepairRequestOutcome::NothingToRepair
        } else if self.continuity_repair_supervisors.load(Ordering::Acquire) == 0 {
            ContinuityRepairRequestOutcome::NoSupervisor
        } else {
            ContinuityRepairRequestOutcome::Scheduled
        }
    }

    // -----------------------------------------------------------------------
    // Turn-completion cursor
    // -----------------------------------------------------------------------

    /// Completion epoch of a REGISTERED identity: its live lease token, or
    /// token 0 when it holds no grant (dormant, or lease lost). `None` when
    /// the identity is not registered at all.
    async fn registered_completion_epoch(&self, identity: &AgentIdentity) -> Option<FencingToken> {
        let entries = self.entries.read().await;
        entries.get(identity).map(|entry| {
            entry
                .lease
                .as_ref()
                .map_or_else(|| FencingToken::new(0), |lease| lease.fencing_token)
        })
    }

    /// Move the stored cursor onto `epoch` if the lease incarnation advanced,
    /// and return it. Never rewinds: a stale epoch leaves the cursor alone.
    fn rebase_completion_cursor(
        &self,
        identity: &AgentIdentity,
        epoch: FencingToken,
    ) -> CompletionCursor {
        let mut cursors = self
            .completion_cursors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cursor = cursors
            .entry(identity.clone())
            .or_insert_with(|| CompletionCursor::start(epoch));
        *cursor = cursor.rebased(epoch);
        *cursor
    }

    /// Whatever was last published for `identity`, without minting a ledger
    /// entry. Reads for unregistered names go through here so probing
    /// arbitrary strings cannot grow the map.
    fn retained_completion_cursor(&self, identity: &AgentIdentity) -> CompletionCursor {
        self.completion_cursors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(identity)
            .copied()
            .unwrap_or_default()
    }

    /// Current [`CompletionCursor`] for `identity`.
    ///
    /// A registered identity's cursor is rebased onto its live lease
    /// incarnation first, so a poller observes an incarnation change
    /// immediately rather than comparing against a turn count that no longer
    /// means anything. An identity that is gone (retired, deleted, or never
    /// registered) reports its last published cursor — retained precisely so
    /// this read cannot rewind.
    ///
    /// Credit before read: completions that already happened but are still
    /// uncounted in the session's retained events are credited first, so the
    /// read never precedes the credit of a completion that happened before it
    /// (a baseline taken now can then never be satisfied by an older turn).
    pub async fn completion_cursor(&self, identity: &AgentIdentity) -> CompletionCursor {
        match self.registered_completion_epoch(identity).await {
            Some(epoch) => {
                if epoch.get() != 0 {
                    self.credited_completion_cursor(identity, epoch).await
                } else {
                    self.rebase_completion_cursor(identity, epoch)
                }
            }
            None => self.retained_completion_cursor(identity),
        }
    }

    /// Credit the run completions of `identity`'s member that happened but
    /// are not counted yet ([`PendingCompletionDrain`]). Only under a live
    /// lease: a completion credited before any lease would be discarded by
    /// the rebase onto the lease, while one credited under the lease is part
    /// of every baseline taken from then on.
    async fn credit_pending_completions(&self, identity: &AgentIdentity) {
        let drain = self
            .pending_completion_drain
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(drain) = drain else {
            return;
        };
        let session_id = self.entries.read().await.get(identity).and_then(|entry| {
            entry
                .continuity
                .as_ref()
                .map(|record| record.session_id.clone())
        });
        let Some(session_id) = session_id else {
            return;
        };
        let terminals = drain.drain(&session_id).await;
        for _ in 0..terminals.completed {
            self.record_turn_completed(identity).await;
        }
        for _ in 0..terminals.failed {
            self.record_turn_failed(identity).await;
        }
    }

    fn completion_credit_lock(&self, identity: &AgentIdentity) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .completion_credit_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(identity).and_then(std::sync::Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(identity.clone(), Arc::downgrade(&lock));
        lock
    }

    /// Credit what already happened, then read the cursor under `epoch`, both
    /// under the identity's completion-credit lock: no terminal is claimed
    /// but not yet credited while the read happens.
    async fn credited_completion_cursor(
        &self,
        identity: &AgentIdentity,
        epoch: FencingToken,
    ) -> CompletionCursor {
        let lock = self.completion_credit_lock(identity);
        let _credit = lock.lock().await;
        self.credit_pending_completions(identity).await;
        self.rebase_completion_cursor(identity, epoch)
    }

    /// Credit one observed run terminal of `identity` if `claim` admits it,
    /// claiming and crediting under the identity's completion-credit lock so
    /// a concurrent cursor read sees either neither or both. Returns whether
    /// a completion was credited.
    pub(crate) async fn credit_observed_terminal(
        &self,
        identity: &AgentIdentity,
        failed: bool,
        claim: impl FnOnce() -> bool,
    ) -> bool {
        let lock = self.completion_credit_lock(identity);
        let _credit = lock.lock().await;
        if !claim() {
            return false;
        }
        if failed {
            self.record_turn_failed(identity).await;
            false
        } else {
            self.record_turn_completed(identity).await;
            true
        }
    }

    /// Whether a completion drain was installed before the lease observer.
    #[cfg(test)]
    pub(crate) fn lease_observer_installed_after_drain(&self) -> bool {
        self.lease_observer_saw_drain.load(Ordering::Acquire)
    }

    /// Whether a completion-cursor drain is installed
    /// ([`Self::install_pending_completion_drain`]).
    pub(crate) fn has_pending_completion_drain(&self) -> bool {
        self.pending_completion_drain
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    /// Install the source of completions that happened but are not counted
    /// yet: meerkat's retained session events past the health monitor's
    /// credited position.
    pub(crate) fn install_pending_completion_drain(&self, drain: Arc<dyn PendingCompletionDrain>) {
        *self
            .pending_completion_drain
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(drain);
    }

    /// Record that a turn completed for `identity`, advancing its cursor by
    /// one within the current lease incarnation.
    ///
    /// Production drives this from `AgentEvent::RunCompleted` on the always-on
    /// identity agent-event monitor. It is deliberately event-driven rather
    /// than derived from a polled projection: a poll cannot distinguish "new
    /// turn, identical text" from "no new turn", which is the entire defect
    /// this cursor exists to close.
    ///
    /// Not idempotent by design — one observed completion advances the cursor
    /// once, which is exactly what a poller comparing against a pre-delivery
    /// baseline needs.
    pub async fn record_turn_completed(&self, identity: &AgentIdentity) -> CompletionCursor {
        // A completion racing deregistration still counts under the last
        // known epoch rather than being dropped.
        let epoch = self
            .registered_completion_epoch(identity)
            .await
            .unwrap_or_else(|| self.retained_completion_cursor(identity).epoch);
        let mut cursors = self
            .completion_cursors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cursor = cursors
            .entry(identity.clone())
            .or_insert_with(|| CompletionCursor::start(epoch));
        *cursor = cursor.rebased(epoch).advanced();
        let cursor = *cursor;
        drop(cursors);
        self.entries.notify();
        cursor
    }

    /// Wait until a turn completes past `baseline`, or the timeout expires.
    ///
    /// This is the correct completion barrier: it compares cursors, never
    /// output text, so two consecutive turns emitting byte-identical text are
    /// still two distinct completions. `baseline` is the
    /// `completion_baseline` returned by [`Self::dispatch_admission_tracked`]
    /// or [`Self::send_admission_tracked`].
    ///
    /// A genuinely stalled turn still times out rather than hanging forever,
    /// and an incarnation change is reported as its own error rather than
    /// being read as either completion or continued waiting. Event-driven, as
    /// [`Self::await_completion`].
    pub async fn wait_for_completion(
        &self,
        identity: &AgentIdentity,
        baseline: CompletionCursor,
        timeout: Duration,
    ) -> Result<CompletionCursor, IdentityRuntimeError> {
        match self
            .await_completion(identity, Some(baseline), timeout)
            .await
        {
            CompletionWait::Completed(cursor) => Ok(cursor),
            CompletionWait::IncarnationChanged(observed) => {
                Err(IdentityRuntimeError::CompletionIncarnationChanged {
                    identity: identity.clone(),
                    baseline,
                    observed,
                })
            }
            CompletionWait::TimedOut(_) => Err(IdentityRuntimeError::Internal(format!(
                "timed out after {}s waiting for a turn past {baseline} on {identity}",
                timeout.as_secs_f64()
            ))),
            other => Err(IdentityRuntimeError::Internal(format!(
                "waiting for a turn past {baseline} on {identity} ended: {}",
                other.wire_str()
            ))),
        }
    }

    /// Wait until the identity's completion cursor moves past `after`, or,
    /// with no `after`, until it counts at least one completed turn (a
    /// startup readiness barrier).
    ///
    /// Event-driven: the wait sleeps until a completion or a failed run is
    /// recorded, the identity table changes (a lease incarnation change,
    /// Broken, retire and delete are table writes), or the runtime starts
    /// shutting down, and re-derives the outcome then. Every wake re-reads
    /// the identity's entry, so a wait never runs to its deadline on an
    /// identity that went Broken, retired or was deleted. `timeout` is only
    /// the caller's overall deadline; reaching it is
    /// [`CompletionWait::TimedOut`], a typed outcome rather than an error.
    ///
    /// An identity that is not registered yet reads its retained cursor, so a
    /// readiness wait started before the identity materializes simply waits
    /// for its first completion.
    ///
    /// The wake signal is shared by every identity (one watch over the
    /// identity table), so a write to one identity wakes every waiter, which
    /// re-reads its own entry and cursor and sleeps again. That costs one
    /// cheap in-memory read per waiter per write; waiters are few (one per
    /// parked SDK wait), so a per-identity signal is not worth the bookkeeping.
    pub async fn await_completion(
        &self,
        identity: &AgentIdentity,
        after: Option<CompletionCursor>,
        timeout: Duration,
    ) -> CompletionWait {
        let deadline = tokio::time::Instant::now() + timeout;
        // Subscribe before the first read so a completion landing between the
        // read and the wait still wakes it.
        let mut changes = self.entries.subscribe();
        let mut shutdown = self.foreground_cancel.subscribe();
        // Credit what already happened before taking the failure baseline: a
        // failure that preceded this wait but is credited by its first read
        // must not end the wait as its own failure.
        let _ = self.completion_cursor(identity).await;
        let failures_at_start = self.run_failure_count(identity);
        let registered_at_start = self.entries.read().await.contains_key(identity);
        loop {
            let cursor = self.completion_cursor(identity).await;
            match after {
                Some(baseline) => match cursor.progress_since(baseline) {
                    CompletionProgress::Completed => return CompletionWait::Completed(cursor),
                    CompletionProgress::IncarnationChanged => {
                        return CompletionWait::IncarnationChanged(cursor);
                    }
                    CompletionProgress::Pending => {}
                },
                None if cursor.turns > 0 => return CompletionWait::Completed(cursor),
                None => {}
            }
            if self.run_failure_count(identity) > failures_at_start {
                return CompletionWait::RunFailed(cursor);
            }
            let state = self
                .entries
                .read()
                .await
                .get(identity)
                .map(|entry| entry.state);
            match state {
                None if registered_at_start => return CompletionWait::IdentityGone(cursor),
                Some(IdentityLifecycleState::Broken) => return CompletionWait::Broken(cursor),
                Some(IdentityLifecycleState::Retiring) => {
                    return CompletionWait::Retiring(cursor);
                }
                _ => {}
            }
            if *shutdown.borrow_and_update() {
                return CompletionWait::ShuttingDown(cursor);
            }
            tokio::select! {
                changed = changes.changed() => {
                    if changed.is_err() {
                        return CompletionWait::ShuttingDown(cursor);
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        return CompletionWait::ShuttingDown(cursor);
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    return CompletionWait::TimedOut(cursor);
                }
            }
        }
    }

    /// Record that a run on `identity` failed (or was cancelled), waking the
    /// completion waiters. Driven from `AgentEvent::RunFailed` on the
    /// identity agent-event monitor, beside [`Self::record_turn_completed`].
    pub async fn record_turn_failed(&self, identity: &AgentIdentity) {
        *self
            .run_failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(identity.clone())
            .or_default() += 1;
        self.entries.notify();
    }

    fn run_failure_count(&self, identity: &AgentIdentity) -> u64 {
        self.run_failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(identity)
            .copied()
            .unwrap_or(0)
    }

    /// The configured default timeout for wait operations.
    pub fn default_timeout(&self) -> Duration {
        self.default_timeout
    }

    async fn execute_reset_bridge_cleanup(
        bridge: &Arc<dyn SessionBridge>,
        cleanup: &PendingResetBridgeCleanup,
    ) -> Result<(), BridgeError> {
        match (&cleanup.runtime_id, &cleanup.session_id) {
            (Some(runtime_id), Some(session_id)) => {
                bridge
                    .retire_reset_superseded_member(runtime_id, session_id)
                    .await
            }
            (Some(runtime_id), None) => bridge.retire_member(runtime_id).await,
            (None, Some(session_id)) => bridge.unregister_session_runtime_state(session_id).await,
            (None, None) => Ok(()),
        }
    }

    async fn clear_reset_bridge_cleanup_if_current(
        pending: &RwLock<BTreeMap<String, PendingResetBridgeCleanup>>,
        key: &str,
        cleanup: &PendingResetBridgeCleanup,
    ) {
        let mut pending = pending.write().await;
        if pending.get(key) == Some(cleanup) {
            pending.remove(key);
        }
    }

    async fn run_reset_memory_capture_if_pending(
        pending: &RwLock<BTreeMap<String, PendingResetBridgeCleanup>>,
        key: &str,
        cleanup: &mut PendingResetBridgeCleanup,
    ) {
        let Some(capture) = cleanup.memory_capture.take() else {
            return;
        };
        capture.run().await;

        // Publish the state-machine advance only after the bounded capture
        // returns. Cancellation before this write leaves the capture on the
        // authoritative debt and a shutdown retry runs it again.
        let mut pending = pending.write().await;
        if let Some(current) = pending.get_mut(key)
            && current == cleanup
        {
            current.memory_capture = None;
        }
    }

    async fn record_old_bridge_cleanup_after_reset(
        &self,
        old_runtime_id: Option<AgentRuntimeId>,
        old_session_id: Option<SessionId>,
        memory_capture: Option<PendingResetMemoryCapture>,
    ) -> Option<PendingResetBridgeCleanup> {
        if old_runtime_id.is_none() && old_session_id.is_none() {
            return None;
        }
        let cleanup = PendingResetBridgeCleanup {
            runtime_id: old_runtime_id,
            session_id: old_session_id,
            memory_capture,
        };
        let key = cleanup.key();
        self.pending_reset_bridge_cleanups
            .write()
            .await
            .insert(key, cleanup.clone());
        Some(cleanup)
    }

    async fn spawn_old_bridge_cleanup_after_reset(
        &self,
        bridge: Arc<dyn SessionBridge>,
        mut cleanup: PendingResetBridgeCleanup,
    ) {
        let key = cleanup.key();
        let runtime_instance_id = self.runtime_instance_id.clone();
        let pending = self.pending_reset_bridge_cleanups.clone();
        let mut tasks = self.reset_bridge_cleanup_tasks.lock().await;
        while let Some(result) = tasks.try_join_next() {
            if let Err(error) = result {
                tracing::error!(
                    runtime_instance_id = %self.runtime_instance_id,
                    %error,
                    "reset bridge cleanup task panicked"
                );
            }
        }
        tasks.spawn(async move {
            Self::run_reset_memory_capture_if_pending(&pending, &key, &mut cleanup).await;
            match Self::execute_reset_bridge_cleanup(&bridge, &cleanup).await {
                Ok(()) => {
                    Self::clear_reset_bridge_cleanup_if_current(&pending, &key, &cleanup).await;
                }
                Err(error) => tracing::warn!(
                    runtime_instance_id = %runtime_instance_id,
                    cleanup_key = %key,
                    %error,
                    "reset-superseded bridge cleanup failed; retaining exact shutdown debt",
                ),
            }
        });
    }

    /// Join every post-commit reset cleanup task after foreground lifecycle
    /// admission is closed. The exact debt remains in the ledger on failure
    /// and is retried synchronously before physical mob shutdown.
    pub(crate) async fn join_reset_bridge_cleanup_tasks(&self) {
        let mut tasks = self.reset_bridge_cleanup_tasks.lock().await;
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                tracing::error!(
                    runtime_instance_id = %self.runtime_instance_id,
                    %error,
                    "reset bridge cleanup task panicked"
                );
            }
        }
    }

    /// Retry all exact reset cleanup debt. Success is the only path that
    /// removes an obligation; callers must retain identity grants when this
    /// returns an error.
    pub(crate) async fn drain_pending_reset_bridge_cleanups(
        &self,
    ) -> Result<usize, IdentityRuntimeError> {
        let Some(bridge) = self.bridge.as_ref() else {
            return Ok(0);
        };
        let pending = self
            .pending_reset_bridge_cleanups
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut completed = 0_usize;
        let mut errors = Vec::new();
        for mut cleanup in pending {
            let key = cleanup.key();
            Self::run_reset_memory_capture_if_pending(
                &self.pending_reset_bridge_cleanups,
                &key,
                &mut cleanup,
            )
            .await;
            match Self::execute_reset_bridge_cleanup(bridge, &cleanup).await {
                Ok(()) => {
                    Self::clear_reset_bridge_cleanup_if_current(
                        &self.pending_reset_bridge_cleanups,
                        &key,
                        &cleanup,
                    )
                    .await;
                    completed += 1;
                }
                Err(error) => errors.push(format!("{key}: {error}")),
            }
        }
        if errors.is_empty() {
            Ok(completed)
        } else {
            Err(IdentityRuntimeError::Internal(format!(
                "reset bridge cleanup debt remains: {}",
                errors.join("; ")
            )))
        }
    }

    /// Wait until the identity produces an output_preview, or timeout.
    ///
    /// **Unsound as a completion barrier.** `output_preview` is the last
    /// committed assistant text, so this returns immediately when a PREVIOUS
    /// turn already left a preview, and it cannot tell "new turn, identical
    /// text" from "no new turn" at all. Use
    /// [`Self::wait_for_completion`] with the `completion_baseline` from
    /// [`Self::send_admission_tracked`] / [`Self::dispatch_admission_tracked`]
    /// when you need to wait for a specific turn. Retained for callers that
    /// only need "has this identity ever spoken".
    ///
    /// Event-driven, as [`Self::wait_for_output_where`].
    pub async fn wait_for_output(
        &self,
        identity: &AgentIdentity,
        timeout: Duration,
    ) -> Result<String, IdentityRuntimeError> {
        self.wait_for_output_where(identity, timeout, |_| true)
            .await
            .map_err(|ended| ended.into_error(identity, "output"))
    }

    /// Wait until the identity's output_preview contains `needle`, or
    /// timeout. Event-driven, as [`Self::wait_for_output_where`].
    pub async fn wait_for_output_containing(
        &self,
        identity: &AgentIdentity,
        needle: &str,
        timeout: Duration,
    ) -> Result<String, IdentityRuntimeError> {
        self.wait_for_output_where(identity, timeout, |preview| preview.contains(needle))
            .await
            .map_err(|ended| ended.into_error(identity, &format!("output containing '{needle}'")))
    }

    /// Read the identity's output_preview whenever it can have changed, until
    /// `accept` takes it or `timeout` elapses.
    ///
    /// No sleep or poll: the preview is the last committed assistant text,
    /// and a commit is recorded as a completion, which wakes the identity
    /// table's change signal ([`Self::await_completion`] waits on the same
    /// one). Registration, lease changes, Broken, retire and delete are table
    /// writes too, so a wait started before the identity materializes reads
    /// again once it does. The subscription is taken before each read, so a
    /// completion landing during the read still wakes the next one. A read
    /// that fails (the identity is not active yet, say) is kept as the
    /// timeout's detail and retried on the next wake, never on a timer.
    async fn wait_for_output_where(
        &self,
        identity: &AgentIdentity,
        timeout: Duration,
        accept: impl Fn(&str) -> bool,
    ) -> Result<String, OutputWaitEnded> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut changes = self.entries.subscribe();
        let mut shutdown = self.foreground_cancel.subscribe();
        let mut last_error = None;
        loop {
            changes.borrow_and_update();
            match self.inspect(identity).await {
                Ok(inspection) => {
                    if let Some(preview) = inspection.output_preview
                        && accept(&preview)
                    {
                        return Ok(preview);
                    }
                }
                Err(error) => last_error = Some(error.to_string()),
            }
            if *shutdown.borrow_and_update() {
                return Err(OutputWaitEnded::ShuttingDown);
            }
            tokio::select! {
                changed = changes.changed() => {
                    if changed.is_err() {
                        return Err(OutputWaitEnded::ShuttingDown);
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        return Err(OutputWaitEnded::ShuttingDown);
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    return Err(OutputWaitEnded::TimedOut { last_error });
                }
            }
        }
    }
}

/// Why [`IdentityRuntime::wait_for_output_where`] ended without an accepted
/// preview.
enum OutputWaitEnded {
    TimedOut { last_error: Option<String> },
    ShuttingDown,
}

impl OutputWaitEnded {
    fn into_error(self, identity: &AgentIdentity, what: &str) -> IdentityRuntimeError {
        match self {
            Self::TimedOut { last_error: None } => IdentityRuntimeError::Internal(format!(
                "timed out waiting for {what} from {identity}"
            )),
            Self::TimedOut {
                last_error: Some(error),
            } => IdentityRuntimeError::Internal(format!(
                "timed out waiting for {what} from {identity} (last read failed: {error})"
            )),
            Self::ShuttingDown => IdentityRuntimeError::Internal(format!(
                "runtime shutting down while waiting for {what} from {identity}"
            )),
        }
    }
}

/// Wire two identities across mobs, resolving runtime IDs from both IdentityRuntimes.
///
/// This is a convenience function for same-process cross-mob scenarios where both
/// IdentityRuntimes are available. It resolves the AgentRuntimeId for each identity
/// and delegates to `UnifiedRuntime::wire_cross_mob()`.
pub async fn wire_cross_mob_by_identity(
    local_irt: &IdentityRuntime,
    local_identity: &AgentIdentity,
    remote_irt: &IdentityRuntime,
    remote_identity: &AgentIdentity,
    local_unified: &crate::UnifiedRuntime,
    remote_mob_id: &str,
) -> Result<(), IdentityRuntimeError> {
    let local_rt = local_irt.runtime_id_for(local_identity).await?;
    let remote_rt = remote_irt.runtime_id_for(remote_identity).await?;
    Box::pin(local_unified.wire_cross_mob(local_rt.as_str(), remote_rt.as_str(), remote_mob_id))
        .await
        .map_err(|e| IdentityRuntimeError::Internal(format!("wire_cross_mob: {e}")))
}

#[cfg(test)]
mod reset_reprofile_tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::{Mutex as AsyncMutex, RwLock as AsyncRwLock};

    use super::super::bridge::{
        BridgeDelivery, BridgeError, MemberInspection, ResumeSessionOutcome,
    };
    use super::super::contracts::RosterProvider;
    use super::super::local_lease::LocalLeaseProvider;
    use super::super::local_store::LocalContinuityStore;
    use super::super::types::{
        AgentBuildDraft, ContinuityResolveState, CustomizerError, LeaseAcquireResult, LeaseError,
        LeaseRenewResult, RosterError, SessionSnapshot,
    };

    struct MutableRoster {
        specs: AsyncRwLock<Vec<DurableAgentSpec>>,
    }

    struct LostRenewLeaseProvider {
        inner: LocalLeaseProvider,
        lose_renewals: AtomicBool,
    }

    impl Default for LostRenewLeaseProvider {
        fn default() -> Self {
            Self {
                inner: LocalLeaseProvider::new(),
                lose_renewals: AtomicBool::new(false),
            }
        }
    }

    #[async_trait::async_trait]
    impl LeaseProvider for LostRenewLeaseProvider {
        async fn acquire_leases(
            &self,
            identities: &[AgentIdentity],
            runtime_instance: &str,
        ) -> Result<BTreeMap<AgentIdentity, LeaseAcquireResult>, LeaseError> {
            self.inner
                .acquire_leases(identities, runtime_instance)
                .await
        }

        async fn renew_leases(
            &self,
            grants: &[LeaseGrant],
        ) -> Result<BTreeMap<AgentIdentity, LeaseRenewResult>, LeaseError> {
            if self.lose_renewals.load(Ordering::SeqCst) {
                return Ok(grants
                    .iter()
                    .map(|grant| {
                        (
                            grant.identity.clone(),
                            LeaseRenewResult::Lost {
                                identity: grant.identity.clone(),
                            },
                        )
                    })
                    .collect());
            }
            self.inner.renew_leases(grants).await
        }

        async fn release_leases(&self, grants: &[LeaseGrant]) -> Result<(), LeaseError> {
            self.inner.release_leases(grants).await
        }
    }

    struct RecordingReleaseLeaseProvider {
        inner: LocalLeaseProvider,
        fail_next_release: AtomicBool,
        release_attempts: AsyncMutex<Vec<LeaseGrant>>,
    }

    impl Default for RecordingReleaseLeaseProvider {
        fn default() -> Self {
            Self {
                inner: LocalLeaseProvider::new(),
                fail_next_release: AtomicBool::new(false),
                release_attempts: AsyncMutex::new(Vec::new()),
            }
        }
    }

    impl RecordingReleaseLeaseProvider {
        fn fail_next_release(&self) {
            self.fail_next_release.store(true, Ordering::SeqCst);
        }

        async fn release_attempts(&self) -> Vec<LeaseGrant> {
            self.release_attempts.lock().await.clone()
        }
    }

    #[async_trait::async_trait]
    impl LeaseProvider for RecordingReleaseLeaseProvider {
        async fn acquire_leases(
            &self,
            identities: &[AgentIdentity],
            runtime_instance: &str,
        ) -> Result<BTreeMap<AgentIdentity, LeaseAcquireResult>, LeaseError> {
            self.inner
                .acquire_leases(identities, runtime_instance)
                .await
        }

        async fn renew_leases(
            &self,
            grants: &[LeaseGrant],
        ) -> Result<BTreeMap<AgentIdentity, LeaseRenewResult>, LeaseError> {
            self.inner.renew_leases(grants).await
        }

        async fn release_leases(&self, grants: &[LeaseGrant]) -> Result<(), LeaseError> {
            self.release_attempts.lock().await.extend_from_slice(grants);
            if self.fail_next_release.swap(false, Ordering::SeqCst) {
                return Err(LeaseError::ProviderUnavailable(
                    "synthetic retained Broken lease release failure".to_string(),
                ));
            }
            self.inner.release_leases(grants).await
        }
    }

    struct GatedResetContinuityStore {
        inner: Arc<LocalContinuityStore>,
        upsert_calls: AtomicUsize,
        fail_on_call: AtomicUsize,
        failure_started: Notify,
        release_failure: Notify,
    }

    #[derive(Default)]
    struct GatedResetCustomizer {
        entered: Notify,
    }

    impl GatedResetCustomizer {
        async fn wait_for_entry(&self) {
            self.entered.notified().await;
        }
    }

    #[async_trait::async_trait]
    impl AgentCustomizer for GatedResetCustomizer {
        async fn customize_build(
            &self,
            _context: &AgentBuildContext,
            _spec: &DurableAgentSpec,
            _draft: &mut AgentBuildDraft,
        ) -> Result<(), CustomizerError> {
            self.entered.notify_one();
            futures::future::pending::<()>().await;
            Ok(())
        }
    }

    impl GatedResetContinuityStore {
        fn new() -> Result<Self, ContinuityStoreError> {
            Ok(Self {
                inner: Arc::new(LocalContinuityStore::in_memory()?),
                upsert_calls: AtomicUsize::new(0),
                fail_on_call: AtomicUsize::new(usize::MAX),
                failure_started: Notify::new(),
                release_failure: Notify::new(),
            })
        }

        fn fail_after_successful_upserts(&self, successful_upserts: usize) {
            let current = self.upsert_calls.load(Ordering::SeqCst);
            self.fail_on_call
                .store(current + successful_upserts + 1, Ordering::SeqCst);
        }

        async fn wait_for_failure(&self) {
            self.failure_started.notified().await;
        }

        fn release_failure(&self) {
            self.release_failure.notify_one();
        }
    }

    #[async_trait::async_trait]
    impl ContinuityStore for GatedResetContinuityStore {
        // The trait requires an explicit answer: a double that stayed
        // silent used to assert an authoritative "no record" while
        // meaning "I was never taught to answer", which is how a fix
        // goes inert with every test still green.
        async fn resolve_record_by_session(
            &self,
            _session_id: &meerkat_core::types::SessionId,
        ) -> Result<
            Option<(
                crate::identity_first::ContinuityRecord,
                crate::identity_first::FencingToken,
                crate::identity_first::CheckpointVersion,
            )>,
            crate::identity_first::ContinuityStoreError,
        > {
            Ok(None)
        }

        async fn resolve_many(
            &self,
            identities: &[AgentIdentity],
        ) -> Result<BTreeMap<AgentIdentity, ContinuityResolveState>, ContinuityStoreError> {
            self.inner.resolve_many(identities).await
        }

        async fn load_session_snapshot(
            &self,
            session_id: &SessionId,
        ) -> Result<Option<SessionSnapshot>, ContinuityStoreError> {
            self.inner.load_session_snapshot(session_id).await
        }

        async fn save_session_snapshot(
            &self,
            identity: &AgentIdentity,
            session_id: &SessionId,
            generation: ContinuityGeneration,
            version: CheckpointVersion,
            fencing_token: FencingToken,
            snapshot: &SessionSnapshot,
        ) -> Result<(), ContinuityStoreError> {
            self.inner
                .save_session_snapshot(
                    identity,
                    session_id,
                    generation,
                    version,
                    fencing_token,
                    snapshot,
                )
                .await
        }

        async fn upsert_continuity_record(
            &self,
            record: &ContinuityRecord,
            fencing_token: FencingToken,
        ) -> Result<(), ContinuityStoreError> {
            let call = self.upsert_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self
                .fail_on_call
                .compare_exchange(call, usize::MAX, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.failure_started.notify_one();
                self.release_failure.notified().await;
                return Err(ContinuityStoreError::Io(
                    "gated final reset upsert failure".to_string(),
                ));
            }
            self.inner
                .upsert_continuity_record(record, fencing_token)
                .await
        }

        async fn rollback_continuity_record(
            &self,
            expected_attempt: &ContinuityRecord,
            previous: Option<&ContinuityRecord>,
            fencing_token: FencingToken,
        ) -> Result<(), ContinuityStoreError> {
            self.inner
                .rollback_continuity_record(expected_attempt, previous, fencing_token)
                .await
        }

        async fn delete_continuity_record(
            &self,
            identity: &AgentIdentity,
            fencing_token: FencingToken,
        ) -> Result<(), ContinuityStoreError> {
            self.inner
                .delete_continuity_record(identity, fencing_token)
                .await
        }
        /// Test double: deliberately whole-snapshot only.
        fn as_incremental_sessions(
            &self,
        ) -> Option<
            std::sync::Arc<dyn crate::identity_first::contracts::ContinuityIncrementalSessions>,
        > {
            None
        }
    }

    impl MutableRoster {
        fn new(specs: Vec<DurableAgentSpec>) -> Self {
            Self {
                specs: AsyncRwLock::new(specs),
            }
        }

        async fn set(&self, specs: Vec<DurableAgentSpec>) {
            *self.specs.write().await = specs;
        }
    }

    #[async_trait::async_trait]
    impl RosterProvider for MutableRoster {
        async fn roster(
            &self,
            _context: &RosterContext,
        ) -> Result<Vec<DurableAgentSpec>, RosterError> {
            Ok(self.specs.read().await.clone())
        }
    }

    #[derive(Default)]
    struct RecordingBridge {
        create_profiles: AsyncMutex<Vec<String>>,
        create_delay: Duration,
        creates_in_flight: AtomicUsize,
        max_creates_in_flight: AtomicUsize,
        retired_runtime_ids: AsyncMutex<Vec<String>>,
        hanging_retire_runtime_ids: AsyncMutex<BTreeSet<String>>,
        failing_register_session_ids: AsyncMutex<BTreeSet<String>>,
        failing_unregister_session_ids: AsyncMutex<BTreeSet<String>>,
        registered_fencing_tokens: AsyncMutex<Vec<FencingToken>>,
        authority_transitions: AsyncMutex<Vec<String>>,
        /// Successor generations minted by `reset_member_to_successor`, so a
        /// test can observe that the transition happened and under which
        /// incarnation.
        successor_generations: AsyncMutex<Vec<String>>,
    }

    impl RecordingBridge {
        async fn create_profiles(&self) -> Vec<String> {
            self.create_profiles.lock().await.clone()
        }

        async fn successor_generations(&self) -> Vec<String> {
            self.successor_generations.lock().await.clone()
        }

        fn max_creates_in_flight(&self) -> usize {
            self.max_creates_in_flight.load(Ordering::SeqCst)
        }

        async fn retired_runtime_ids(&self) -> Vec<String> {
            self.retired_runtime_ids.lock().await.clone()
        }

        async fn hang_retire_for(&self, runtime_id: &AgentRuntimeId) {
            self.hanging_retire_runtime_ids
                .lock()
                .await
                .insert(runtime_id.to_string());
        }

        async fn fail_unregister_for(&self, session_id: &SessionId) {
            self.failing_unregister_session_ids
                .lock()
                .await
                .insert(session_id.to_string());
        }

        async fn allow_unregister_for(&self, session_id: &SessionId) {
            self.failing_unregister_session_ids
                .lock()
                .await
                .remove(&session_id.to_string());
        }

        async fn fail_register_for(&self, session_id: &SessionId) {
            self.failing_register_session_ids
                .lock()
                .await
                .insert(session_id.to_string());
        }

        async fn registered_fencing_tokens(&self) -> Vec<FencingToken> {
            self.registered_fencing_tokens.lock().await.clone()
        }

        async fn authority_transitions(&self) -> Vec<String> {
            self.authority_transitions.lock().await.clone()
        }
    }

    #[async_trait::async_trait]
    impl SessionBridge for RecordingBridge {
        /// Mirrors respawn, including the profile the successor is built on.
        ///
        /// This used to deliberately DROP `spec.profile`, because respawn
        /// preserved the predecessor's profile and a double that applied the
        /// requested one could reprofile where Meerkat could not - the capability
        /// tests would then have gone green against a capability that did not
        /// exist.
        ///
        /// It exists now. `respawn_with_successor_spec` applies the successor
        /// spec atomically, proven end to end on the real mob handle by
        /// identity_first_builder_reset_reprofiles_{from_current_roster_provider,
        /// with_roster_context}, which assert status().profile through no double
        /// at all. So recording the profile now mirrors production rather than
        /// overstating it - and the ORDER mattered: the production path was
        /// proven first, and only then was this unlocked.
        async fn reset_member_to_successor(
            &self,
            identity: &AgentIdentity,
            spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
        ) -> Result<crate::identity_first::bridge::ResetSuccessorBinding, BridgeError> {
            // The successor IS built on the requested spec now, so the double
            // publishes the profile the same way create does.
            self.create_profiles
                .lock()
                .await
                .push(spec.profile.as_str().to_string());
            let generation = self.successor_generations.lock().await.len() as u64 + 1;
            let alias = format!("rt:{}:{generation}", identity.as_str());
            self.successor_generations.lock().await.push(alias.clone());
            let agent_runtime_id = AgentRuntimeId::parse(&alias).map_err(|error| {
                BridgeError::Mob(format!("test double minted an unusable successor: {error}"))
            })?;
            Ok(crate::identity_first::bridge::ResetSuccessorBinding {
                agent_runtime_id,
                session_id: SessionId::new(),
            })
        }

        async fn create_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            session_id: &SessionId,
        ) -> Result<SessionId, BridgeError> {
            let in_flight = self.creates_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_creates_in_flight
                .fetch_max(in_flight, Ordering::SeqCst);
            tokio::time::sleep(self.create_delay).await;
            self.creates_in_flight.fetch_sub(1, Ordering::SeqCst);
            self.create_profiles
                .lock()
                .await
                .push(spec.profile.to_string());
            Ok(session_id.clone())
        }

        async fn resume_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            _session_id: &SessionId,
            _snapshot: &SessionSnapshot,
        ) -> Result<ResumeSessionOutcome, BridgeError> {
            Err(BridgeError::Mob(
                "resume not used in reset test".to_string(),
            ))
        }

        async fn deliver_admitted(
            &self,
            _runtime_id: &AgentRuntimeId,
            _delivery: BridgeDelivery,
        ) -> Result<SessionId, BridgeError> {
            Err(BridgeError::Mob(
                "deliver not used in reset test".to_string(),
            ))
        }

        async fn checkpoint_session(
            &self,
            _runtime_id: &AgentRuntimeId,
            _session_id: &SessionId,
        ) -> Result<SessionSnapshot, BridgeError> {
            Err(BridgeError::Mob(
                "checkpoint not used in reset test".to_string(),
            ))
        }

        async fn retire_member(&self, runtime_id: &AgentRuntimeId) -> Result<(), BridgeError> {
            self.retired_runtime_ids
                .lock()
                .await
                .push(runtime_id.to_string());
            if self
                .hanging_retire_runtime_ids
                .lock()
                .await
                .contains(runtime_id.as_str())
            {
                futures::future::pending::<()>().await;
            }
            Ok(())
        }

        async fn inspect_member(
            &self,
            _runtime_id: &AgentRuntimeId,
        ) -> Result<MemberInspection, BridgeError> {
            Err(BridgeError::Mob(
                "inspect not used in reset test".to_string(),
            ))
        }

        async fn register_session_runtime_state(
            &self,
            session_id: &SessionId,
            _identity: &AgentIdentity,
            _generation: ContinuityGeneration,
            checkpoint_version: CheckpointVersion,
            fencing_token: FencingToken,
        ) -> Result<CheckpointVersion, BridgeError> {
            self.registered_fencing_tokens
                .lock()
                .await
                .push(fencing_token);
            self.authority_transitions
                .lock()
                .await
                .push(format!("register:{}", fencing_token.get()));
            if self
                .failing_register_session_ids
                .lock()
                .await
                .contains(&session_id.to_string())
            {
                return Err(BridgeError::Mob(
                    "synthetic live-session rebind failure".to_string(),
                ));
            }
            Ok(checkpoint_version)
        }

        async fn suspend_session_runtime_state(
            &self,
            session_id: &SessionId,
        ) -> Result<(), BridgeError> {
            self.authority_transitions
                .lock()
                .await
                .push(format!("suspend:{session_id}"));
            Ok(())
        }

        async fn unregister_session_runtime_state(
            &self,
            session_id: &SessionId,
        ) -> Result<(), BridgeError> {
            if self
                .failing_unregister_session_ids
                .lock()
                .await
                .contains(&session_id.to_string())
            {
                return Err(BridgeError::Mob("old session still draining".to_string()));
            }
            Ok(())
        }
    }

    /// Bridge used to prove REQ-33 metadata hot reloads never cross the
    /// retire/resume boundary. Initial creation and ordinary dispatch work;
    /// any attempted resume fails loudly, reproducing the real gateway's
    /// still-draining-member collision without an LLM or network dependency.
    #[derive(Default)]
    struct HotReloadBridge {
        sessions: AsyncMutex<BTreeMap<String, SessionId>>,
        retire_calls: AtomicUsize,
        resume_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl SessionBridge for HotReloadBridge {
        async fn create_session(
            &self,
            _identity: &AgentIdentity,
            runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            session_id: &SessionId,
        ) -> Result<SessionId, BridgeError> {
            self.sessions
                .lock()
                .await
                .insert(runtime_id.to_string(), session_id.clone());
            Ok(session_id.clone())
        }

        async fn resume_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            _session_id: &SessionId,
            _snapshot: &SessionSnapshot,
        ) -> Result<ResumeSessionOutcome, BridgeError> {
            self.resume_calls.fetch_add(1, Ordering::SeqCst);
            Err(BridgeError::Mob(
                "same-profile metadata hot reload attempted session resume".to_string(),
            ))
        }

        async fn deliver_admitted(
            &self,
            runtime_id: &AgentRuntimeId,
            _delivery: BridgeDelivery,
        ) -> Result<SessionId, BridgeError> {
            self.sessions
                .lock()
                .await
                .get(runtime_id.as_str())
                .cloned()
                .ok_or_else(|| BridgeError::Mob(format!("missing session for {runtime_id}")))
        }

        async fn checkpoint_session(
            &self,
            _runtime_id: &AgentRuntimeId,
            _session_id: &SessionId,
        ) -> Result<SessionSnapshot, BridgeError> {
            Err(BridgeError::Mob(
                "checkpoint not used in hot-reload test".to_string(),
            ))
        }

        async fn retire_member(&self, runtime_id: &AgentRuntimeId) -> Result<(), BridgeError> {
            self.retire_calls.fetch_add(1, Ordering::SeqCst);
            self.sessions.lock().await.remove(runtime_id.as_str());
            Ok(())
        }

        async fn inspect_member(
            &self,
            _runtime_id: &AgentRuntimeId,
        ) -> Result<MemberInspection, BridgeError> {
            Err(BridgeError::Mob(
                "inspect not used in hot-reload test".to_string(),
            ))
        }
    }

    /// Lower-plane model for lease-loss reconciliation. Resume rejects a
    /// duplicate concrete alias, so the tests prove Broken cleanup happens
    /// before profile replacement rather than merely observing call counts.
    #[derive(Default)]
    struct LostCleanupBridge {
        members: AsyncMutex<BTreeSet<String>>,
        session_runtime_states: AsyncMutex<BTreeSet<String>>,
        retire_calls: AtomicUsize,
        unregister_calls: AtomicUsize,
        unregistered_sessions: AsyncMutex<Vec<String>>,
        resume_collisions: AtomicUsize,
    }

    impl LostCleanupBridge {
        async fn member_count(&self) -> usize {
            self.members.lock().await.len()
        }

        async fn session_runtime_state_count(&self) -> usize {
            self.session_runtime_states.lock().await.len()
        }
    }

    async fn force_renewal_lost(
        runtime: &IdentityRuntime,
        lease_provider: &LostRenewLeaseProvider,
        identity: &AgentIdentity,
    ) -> Result<(), Box<dyn std::error::Error>> {
        lease_provider.lose_renewals.store(true, Ordering::SeqCst);
        {
            let mut entries = runtime.entries.write().await;
            let entry = entries
                .get_mut(identity)
                .ok_or("identity disappeared before forced renewal")?;
            let lease = entry
                .lease
                .as_mut()
                .ok_or("active identity has no lease before forced renewal")?;
            lease.ttl = Duration::ZERO;
        }
        assert!(matches!(
            runtime.renew_due_leases_once().await,
            Err(IdentityRuntimeError::LeaseLost(lost)) if lost == *identity
        ));
        Ok(())
    }

    #[async_trait::async_trait]
    impl SessionBridge for LostCleanupBridge {
        async fn create_session(
            &self,
            _identity: &AgentIdentity,
            runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            session_id: &SessionId,
        ) -> Result<SessionId, BridgeError> {
            if !self.members.lock().await.insert(runtime_id.to_string()) {
                return Err(BridgeError::Mob(format!(
                    "member collision for {runtime_id}"
                )));
            }
            Ok(session_id.clone())
        }

        async fn resume_session(
            &self,
            _identity: &AgentIdentity,
            runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            session_id: &SessionId,
            _snapshot: &SessionSnapshot,
        ) -> Result<ResumeSessionOutcome, BridgeError> {
            if !self.members.lock().await.insert(runtime_id.to_string()) {
                self.resume_collisions.fetch_add(1, Ordering::SeqCst);
                return Err(BridgeError::Mob(format!(
                    "member collision for {runtime_id}"
                )));
            }
            Ok(ResumeSessionOutcome::Resumed {
                session_id: session_id.clone(),
            })
        }

        async fn deliver_admitted(
            &self,
            _runtime_id: &AgentRuntimeId,
            _delivery: BridgeDelivery,
        ) -> Result<SessionId, BridgeError> {
            Err(BridgeError::Mob(
                "deliver not used in lost cleanup test".to_string(),
            ))
        }

        async fn checkpoint_session(
            &self,
            _runtime_id: &AgentRuntimeId,
            _session_id: &SessionId,
        ) -> Result<SessionSnapshot, BridgeError> {
            Err(BridgeError::Mob(
                "checkpoint not used in lost cleanup test".to_string(),
            ))
        }

        async fn retire_member(&self, runtime_id: &AgentRuntimeId) -> Result<(), BridgeError> {
            self.retire_calls.fetch_add(1, Ordering::SeqCst);
            self.members.lock().await.remove(runtime_id.as_str());
            Ok(())
        }

        async fn inspect_member(
            &self,
            _runtime_id: &AgentRuntimeId,
        ) -> Result<MemberInspection, BridgeError> {
            Err(BridgeError::Mob(
                "inspect not used in lost cleanup test".to_string(),
            ))
        }

        async fn register_session_runtime_state(
            &self,
            session_id: &SessionId,
            _identity: &AgentIdentity,
            _generation: ContinuityGeneration,
            checkpoint_version: CheckpointVersion,
            _fencing_token: FencingToken,
        ) -> Result<CheckpointVersion, BridgeError> {
            self.session_runtime_states
                .lock()
                .await
                .insert(session_id.to_string());
            Ok(checkpoint_version)
        }

        async fn unregister_session_runtime_state(
            &self,
            session_id: &SessionId,
        ) -> Result<(), BridgeError> {
            self.unregister_calls.fetch_add(1, Ordering::SeqCst);
            self.unregistered_sessions
                .lock()
                .await
                .push(session_id.to_string());
            self.session_runtime_states
                .lock()
                .await
                .remove(&session_id.to_string());
            Ok(())
        }
    }

    fn durable_spec(identity: AgentIdentity, profile: &str) -> DurableAgentSpec {
        DurableAgentSpec {
            identity,
            profile: meerkat_mob::ProfileName::from(profile),
            addressability: AgentAddressability::Addressable,
            display_name: None,
            labels: BTreeMap::new(),
            context: None,
            additional_instructions: Vec::new(),
            initial_message: None,
            runtime_mode_override: None,
            backend: None,
            binding: None,
            placement: None,
        }
    }

    /// Always answers create with the host's deterministic rejection — the
    /// exact string shape rpc_gateway mints when the app-side
    /// `callback/build_agent` round trip COMPLETES with an error.
    #[derive(Default)]
    struct HostRejectingBridge {
        create_attempts: std::sync::atomic::AtomicUsize,
    }

    impl HostRejectingBridge {
        fn attempts(&self) -> usize {
            self.create_attempts
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl SessionBridge for HostRejectingBridge {
        async fn create_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            _session_id: &SessionId,
        ) -> Result<SessionId, BridgeError> {
            self.create_attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(BridgeError::Mob(
                "spawn_member: callback/build_agent failed: callback error: candidate-mode \
                 effect gate refused this build"
                    .to_string(),
            ))
        }

        async fn resume_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            _session_id: &SessionId,
            _snapshot: &SessionSnapshot,
        ) -> Result<ResumeSessionOutcome, BridgeError> {
            Err(BridgeError::Mob(
                "resume not used in host-reject test".to_string(),
            ))
        }

        async fn deliver_admitted(
            &self,
            _runtime_id: &AgentRuntimeId,
            _delivery: BridgeDelivery,
        ) -> Result<SessionId, BridgeError> {
            Err(BridgeError::Mob(
                "deliver not used in host-reject test".to_string(),
            ))
        }

        async fn checkpoint_session(
            &self,
            _runtime_id: &AgentRuntimeId,
            _session_id: &SessionId,
        ) -> Result<SessionSnapshot, BridgeError> {
            Err(BridgeError::Mob(
                "checkpoint not used in host-reject test".to_string(),
            ))
        }

        async fn retire_member(&self, _runtime_id: &AgentRuntimeId) -> Result<(), BridgeError> {
            Ok(())
        }
    }

    /// Records the run-start hold releases the identity runtime asks for;
    /// `fail` makes each release fail like a refused mob command.
    #[derive(Default)]
    struct RunStartReleaseBridge {
        fail: bool,
        released: std::sync::Mutex<Vec<String>>,
    }

    impl RunStartReleaseBridge {
        fn released(&self) -> Vec<String> {
            self.released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl SessionBridge for RunStartReleaseBridge {
        async fn create_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            _session_id: &SessionId,
        ) -> Result<SessionId, BridgeError> {
            Err(BridgeError::Mob(
                "create not used in release test".to_string(),
            ))
        }

        async fn resume_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            _session_id: &SessionId,
            _snapshot: &SessionSnapshot,
        ) -> Result<ResumeSessionOutcome, BridgeError> {
            Err(BridgeError::Mob(
                "resume not used in release test".to_string(),
            ))
        }

        async fn deliver_admitted(
            &self,
            _runtime_id: &AgentRuntimeId,
            _delivery: BridgeDelivery,
        ) -> Result<SessionId, BridgeError> {
            Err(BridgeError::Mob(
                "deliver not used in release test".to_string(),
            ))
        }

        async fn checkpoint_session(
            &self,
            _runtime_id: &AgentRuntimeId,
            _session_id: &SessionId,
        ) -> Result<SessionSnapshot, BridgeError> {
            Err(BridgeError::Mob(
                "checkpoint not used in release test".to_string(),
            ))
        }

        async fn retire_member(&self, _runtime_id: &AgentRuntimeId) -> Result<(), BridgeError> {
            Ok(())
        }

        async fn release_member_run_starts_for_published_tools(
            &self,
            member_id: &meerkat_mob::AgentIdentity,
        ) -> Result<(), BridgeError> {
            self.released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(member_id.to_string());
            if self.fail {
                return Err(BridgeError::Mob("mob refused the release".to_string()));
            }
            Ok(())
        }
    }

    /// #563 follow-up: a restored member whose early `customize_build` failed
    /// is held by the mob build (`ToolsNotPublished`). The publication that
    /// clears its pending record releases that hold exactly once; publishing
    /// for an identity that was not pending releases nothing; a failed
    /// release keeps the identity pending with the error, so a member left
    /// held is visible in its status.
    #[tokio::test]
    async fn publishing_a_pending_identitys_tools_releases_its_run_start_hold()
    -> Result<(), Box<dyn std::error::Error>> {
        for fail in [false, true] {
            let identity = AgentIdentity::parse("personal:alice")?;
            let spec = durable_spec(identity.clone(), "personal");
            let bridge = Arc::new(RunStartReleaseBridge {
                fail,
                ..Default::default()
            });
            let runtime = IdentityRuntime::new(IdentityRuntimeConfig {
                continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
                lease_provider: Arc::new(LocalLeaseProvider::new()),
                runtime_instance_id: "run-start-release-test".to_string(),
                has_runtime_store: true,
                durability_policy: DurabilityPolicy::SyncWriteThrough,
                bridge: Some(bridge.clone()),
                default_timeout: None,
            });
            runtime
                .set_customizer_tool_registry(Some(
                    super::super::customizer_tools::CustomizerToolRegistry::new(),
                ))
                .await;
            runtime.record_customizer_tools_pending(BTreeMap::from([(
                identity.clone(),
                CustomizerToolsPending {
                    reason: "pre-activation customize_build failed: host down".to_string(),
                },
            )]));
            let draft = || AgentBuildDraft {
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
            runtime
                .publish_customizer_tools(&identity, &mut draft())
                .await;
            let member = crate::member_comms_id::mob_member_id(identity.as_str()).to_string();
            assert_eq!(bridge.released(), vec![member.clone()]);
            let pending = runtime.customizer_tools_pending_for(&identity);
            if fail {
                let pending = pending.ok_or("a failed release keeps the identity pending")?;
                assert!(
                    pending.reason.contains("customizer tools published, but")
                        && pending.reason.contains("mob refused the release"),
                    "{pending:?}"
                );
            } else {
                assert!(pending.is_none(), "{pending:?}");
                // Not pending any more: a later publication releases nothing.
                runtime
                    .publish_customizer_tools(&identity, &mut draft())
                    .await;
                assert_eq!(bridge.released(), vec![member]);
            }
        }
        Ok(())
    }

    /// Always answers create with meerkat's typed provider-auth refusal for
    /// the configured `AuthErrorKind`: the shape the mob bridge produces when
    /// the factory fails credential resolution at build time.
    struct ProviderAuthRejectingBridge {
        kind: meerkat_core::AuthErrorKind,
        create_attempts: std::sync::atomic::AtomicUsize,
    }

    impl ProviderAuthRejectingBridge {
        fn new(kind: meerkat_core::AuthErrorKind) -> Self {
            Self {
                kind,
                create_attempts: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn attempts(&self) -> usize {
            self.create_attempts
                .load(std::sync::atomic::Ordering::SeqCst)
        }

        fn failure(&self) -> meerkat_core::service::SessionProviderAuthFailure {
            meerkat_core::service::SessionProviderAuthFailure {
                kind: self.kind,
                provider: meerkat_core::Provider::Anthropic,
                realm_id: None,
                binding_id: None,
            }
        }
    }

    #[async_trait::async_trait]
    impl SessionBridge for ProviderAuthRejectingBridge {
        async fn create_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            _session_id: &SessionId,
        ) -> Result<SessionId, BridgeError> {
            self.create_attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let failure = self.failure();
            Err(BridgeError::ProviderAuthRejected {
                detail: format!("create_session: {}", failure.public_message()),
                failure,
            })
        }

        async fn resume_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            _session_id: &SessionId,
            _snapshot: &SessionSnapshot,
        ) -> Result<ResumeSessionOutcome, BridgeError> {
            Err(BridgeError::Mob(
                "resume not used in provider-auth test".to_string(),
            ))
        }

        async fn deliver_admitted(
            &self,
            _runtime_id: &AgentRuntimeId,
            _delivery: BridgeDelivery,
        ) -> Result<SessionId, BridgeError> {
            Err(BridgeError::Mob(
                "deliver not used in provider-auth test".to_string(),
            ))
        }

        async fn checkpoint_session(
            &self,
            _runtime_id: &AgentRuntimeId,
            _session_id: &SessionId,
        ) -> Result<SessionSnapshot, BridgeError> {
            Err(BridgeError::Mob(
                "checkpoint not used in provider-auth test".to_string(),
            ))
        }

        async fn retire_member(&self, _runtime_id: &AgentRuntimeId) -> Result<(), BridgeError> {
            Ok(())
        }
    }

    /// A head-canonical store that answers Ready and then fails the exact
    /// session lookup must make pre-prepare resolution TERMINAL, not skip.
    ///
    /// Skipping is the defect: publish nothing, let prepare run, let inner
    /// recovery commit, and let the projection hit the unregistered-owner
    /// backstop - a boot that is NOT byte-neutral, because the inner commit has
    /// already landed when the refusal fires. Failing here lets the caller retry
    /// a boot that has changed nothing.
    #[tokio::test]
    async fn strict_preflight_refuses_rather_than_skipping_on_a_head_canonical_store()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::identity_first::types::{
            CheckpointVersion, ContinuityGeneration, ContinuityRecord,
        };

        #[derive(Clone, Copy, PartialEq, Eq)]
        enum FenceAnswer {
            Error,
            Absent,
            Matching,
        }

        struct ScriptedFenceStore {
            inner: Arc<crate::identity_first::local_store::LocalContinuityStore>,
            record: ContinuityRecord,
            answer: FenceAnswer,
        }

        #[async_trait::async_trait]
        impl crate::identity_first::ContinuityStore for ScriptedFenceStore {
            async fn resolve_many(
                &self,
                identities: &[AgentIdentity],
            ) -> Result<
                std::collections::BTreeMap<
                    AgentIdentity,
                    crate::identity_first::types::ContinuityResolveState,
                >,
                crate::identity_first::types::ContinuityStoreError,
            > {
                let mut out = std::collections::BTreeMap::new();
                for identity in identities {
                    out.insert(
                        identity.clone(),
                        crate::identity_first::types::ContinuityResolveState::Ready {
                            record: self.record.clone(),
                        },
                    );
                }
                Ok(out)
            }

            async fn resolve_record_by_session(
                &self,
                _session_id: &meerkat_core::types::SessionId,
            ) -> Result<
                Option<(
                    ContinuityRecord,
                    crate::identity_first::types::FencingToken,
                    CheckpointVersion,
                )>,
                crate::identity_first::types::ContinuityStoreError,
            > {
                match self.answer {
                    FenceAnswer::Error => Err(
                        crate::identity_first::types::ContinuityStoreError::Transient(
                            "scripted fence failure".to_string(),
                        ),
                    ),
                    FenceAnswer::Absent => Ok(None),
                    FenceAnswer::Matching => Ok(Some((
                        self.record.clone(),
                        crate::identity_first::types::FencingToken::new(1),
                        CheckpointVersion::new(7),
                    ))),
                }
            }

            async fn load_session_snapshot(
                &self,
                session_id: &meerkat_core::types::SessionId,
            ) -> Result<
                Option<crate::identity_first::types::SessionSnapshot>,
                crate::identity_first::types::ContinuityStoreError,
            > {
                self.inner.load_session_snapshot(session_id).await
            }

            async fn save_session_snapshot(
                &self,
                identity: &AgentIdentity,
                session_id: &meerkat_core::types::SessionId,
                generation: crate::identity_first::types::ContinuityGeneration,
                version: CheckpointVersion,
                fencing_token: crate::identity_first::types::FencingToken,
                snapshot: &crate::identity_first::types::SessionSnapshot,
            ) -> Result<(), crate::identity_first::types::ContinuityStoreError> {
                self.inner
                    .save_session_snapshot(
                        identity,
                        session_id,
                        generation,
                        version,
                        fencing_token,
                        snapshot,
                    )
                    .await
            }

            async fn upsert_continuity_record(
                &self,
                record: &ContinuityRecord,
                fencing_token: crate::identity_first::types::FencingToken,
            ) -> Result<(), crate::identity_first::types::ContinuityStoreError> {
                self.inner
                    .upsert_continuity_record(record, fencing_token)
                    .await
            }

            async fn delete_continuity_record(
                &self,
                identity: &AgentIdentity,
                fencing_token: crate::identity_first::types::FencingToken,
            ) -> Result<(), crate::identity_first::types::ContinuityStoreError> {
                self.inner
                    .delete_continuity_record(identity, fencing_token)
                    .await
            }

            // Delegated so this double IS head-canonical, which is the whole
            // point: the strict branch is selected by as_incremental_sessions()
            // being Some, so a double that returned None would silently test the
            // blob-only path and pass while proving nothing.
            fn as_incremental_sessions(
                &self,
            ) -> Option<Arc<dyn crate::identity_first::contracts::ContinuityIncrementalSessions>>
            {
                self.inner.as_incremental_sessions()
            }
        }

        let identity = AgentIdentity::parse("domain:calendar")?;
        let record = ContinuityRecord {
            identity: identity.clone(),
            agent_runtime_id: AgentRuntimeId::parse("rt:domain:calendar:0")?,
            session_id: meerkat_core::types::SessionId::new(),
            generation: ContinuityGeneration::new(0),
            checkpoint_version: CheckpointVersion::new(0),
        };
        let roster = vec![durable_spec(identity.clone(), "calendar")];

        for answer in [FenceAnswer::Error, FenceAnswer::Absent] {
            let store: Arc<dyn crate::identity_first::ContinuityStore> =
                Arc::new(ScriptedFenceStore {
                    inner: Arc::new(
                        crate::identity_first::local_store::LocalContinuityStore::in_memory()?,
                    ),
                    record: record.clone(),
                    answer,
                });
            let outcome = IdentityRuntime::resolve_persisted_owner_states(&store, &roster).await;
            assert!(
                outcome.is_err(),
                "a Ready record followed by an unreadable fence must be TERMINAL on a \
                 head-canonical store, not a skip that lets prepare proceed"
            );
        }

        // Control: the same double with a matching fence resolves one state, so
        // the refusals above are about the unreadable fence and not about the
        // double being unusable.
        let store: Arc<dyn crate::identity_first::ContinuityStore> = Arc::new(ScriptedFenceStore {
            inner: Arc::new(crate::identity_first::local_store::LocalContinuityStore::in_memory()?),
            record: record.clone(),
            answer: FenceAnswer::Matching,
        });
        let states = IdentityRuntime::resolve_persisted_owner_states(&store, &roster).await?;
        assert_eq!(
            states.len(),
            1,
            "a matching fence must resolve exactly one owner state"
        );
        assert_eq!(states[0].1.identity, identity);
        assert_eq!(states[0].1.checkpoint_version, CheckpointVersion::new(7));

        // The three resolve-state outcomes, which must NOT collapse into one
        // skip. Only Uninitialized is the creation path.
        struct ScriptedResolveStore {
            inner: Arc<crate::identity_first::local_store::LocalContinuityStore>,
            state: Option<crate::identity_first::types::ContinuityResolveState>,
            // false => as_incremental_sessions returns None, i.e. BLOB-ONLY.
            // Without this the "terminal on ANY store" claim was only ever
            // exercised head-canonical, because delegating to a real store makes
            // the double head-canonical by construction - the assertion passed
            // while testing the opposite branch of the one it named.
            head_canonical: bool,
            // Counts store reads so INV-06 can be shown to reject BEFORE any
            // read rather than merely returning some error eventually.
            reads: Arc<std::sync::atomic::AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl crate::identity_first::ContinuityStore for ScriptedResolveStore {
            async fn resolve_many(
                &self,
                identities: &[AgentIdentity],
            ) -> Result<
                std::collections::BTreeMap<
                    AgentIdentity,
                    crate::identity_first::types::ContinuityResolveState,
                >,
                crate::identity_first::types::ContinuityStoreError,
            > {
                self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let mut out = std::collections::BTreeMap::new();
                if let Some(state) = self.state.clone() {
                    for identity in identities {
                        out.insert(identity.clone(), state.clone());
                    }
                }
                // state == None means we deliberately answer with NO entry,
                // which the trait contract forbids.
                Ok(out)
            }

            async fn resolve_record_by_session(
                &self,
                session_id: &meerkat_core::types::SessionId,
            ) -> Result<
                Option<(
                    ContinuityRecord,
                    crate::identity_first::types::FencingToken,
                    CheckpointVersion,
                )>,
                crate::identity_first::types::ContinuityStoreError,
            > {
                self.inner.resolve_record_by_session(session_id).await
            }

            async fn load_session_snapshot(
                &self,
                session_id: &meerkat_core::types::SessionId,
            ) -> Result<
                Option<crate::identity_first::types::SessionSnapshot>,
                crate::identity_first::types::ContinuityStoreError,
            > {
                self.inner.load_session_snapshot(session_id).await
            }

            async fn save_session_snapshot(
                &self,
                identity: &AgentIdentity,
                session_id: &meerkat_core::types::SessionId,
                generation: crate::identity_first::types::ContinuityGeneration,
                version: CheckpointVersion,
                fencing_token: crate::identity_first::types::FencingToken,
                snapshot: &crate::identity_first::types::SessionSnapshot,
            ) -> Result<(), crate::identity_first::types::ContinuityStoreError> {
                self.inner
                    .save_session_snapshot(
                        identity,
                        session_id,
                        generation,
                        version,
                        fencing_token,
                        snapshot,
                    )
                    .await
            }

            async fn upsert_continuity_record(
                &self,
                record: &ContinuityRecord,
                fencing_token: crate::identity_first::types::FencingToken,
            ) -> Result<(), crate::identity_first::types::ContinuityStoreError> {
                self.inner
                    .upsert_continuity_record(record, fencing_token)
                    .await
            }

            async fn delete_continuity_record(
                &self,
                identity: &AgentIdentity,
                fencing_token: crate::identity_first::types::FencingToken,
            ) -> Result<(), crate::identity_first::types::ContinuityStoreError> {
                self.inner
                    .delete_continuity_record(identity, fencing_token)
                    .await
            }

            fn as_incremental_sessions(
                &self,
            ) -> Option<Arc<dyn crate::identity_first::contracts::ContinuityIncrementalSessions>>
            {
                if self.head_canonical {
                    self.inner.as_incremental_sessions()
                } else {
                    None
                }
            }
        }

        // Built once with `?` rather than `.expect()` inside the closure: this
        // crate denies clippy::expect_used, and --lib alone does not compile the
        // test targets, so only --all-targets surfaces it.
        let scripted_inner =
            Arc::new(crate::identity_first::local_store::LocalContinuityStore::in_memory()?);
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let scripted = |state: Option<crate::identity_first::types::ContinuityResolveState>,
                        head_canonical: bool| {
            let store: Arc<dyn crate::identity_first::ContinuityStore> =
                Arc::new(ScriptedResolveStore {
                    inner: Arc::clone(&scripted_inner),
                    state,
                    head_canonical,
                    reads: Arc::clone(&reads),
                });
            store
        };

        // Uninitialized: the ONLY creation path. Zero states, no error, on BOTH
        // store shapes.
        for head_canonical in [true, false] {
            let uninit = scripted(
                Some(crate::identity_first::types::ContinuityResolveState::Uninitialized),
                head_canonical,
            );
            let states = IdentityRuntime::resolve_persisted_owner_states(&uninit, &roster).await?;
            assert!(
                states.is_empty(),
                "Uninitialized is the creation path on either store shape: no owner state, and \
                 NOT an error (head_canonical={head_canonical})"
            );
        }

        // Missing entry: a provider-contract violation, terminal on ANY store -
        // and now actually EXERCISED on a blob-only double. Previously this claim
        // was only ever run head-canonical, because delegating
        // as_incremental_sessions to a real store makes the double
        // head-canonical, so the assertion named one branch and tested another.
        for head_canonical in [true, false] {
            let missing = scripted(None, head_canonical);
            assert!(
                IdentityRuntime::resolve_persisted_owner_states(&missing, &roster)
                    .await
                    .is_err(),
                "resolve_many must answer for every requested identity; a missing entry is a \
                 provider error on EITHER store shape, not an implicit Uninitialized \
                 (head_canonical={head_canonical})"
            );
        }

        // Broken: the identity HAS durable state and the store says it is
        // unusable. Terminal on a head-canonical store - publishing nothing and
        // letting prepare proceed means recovery meets an empty owner map for a
        // session that exists, which is the defect, not a creation.
        let broken = scripted(
            Some(
                crate::identity_first::types::ContinuityResolveState::Broken {
                    failure: crate::identity_first::types::ContinuityFailure {
                        identity: identity.clone(),
                        kind:
                            crate::identity_first::types::ContinuityFailureKind::SnapshotCorrupted,
                        record: None,
                        detail: "scripted broken head".to_string(),
                    },
                },
            ),
            true,
        );
        assert!(
            IdentityRuntime::resolve_persisted_owner_states(&broken, &roster)
                .await
                .is_err(),
            "Broken is not the creation path: an identity with unusable durable state must be \
             terminal on a head-canonical store rather than silently recreated"
        );

        // BLOB-ONLY Broken control, proving the accepted legacy disposition
        // rather than assuming it: no head-canonical projection needs the cursor
        // there, so Broken degrades to best-effort - ZERO states, and NOT an
        // error. Without this the head-canonical assertion above would be
        // consistent with Broken being terminal everywhere, which is not what
        // was accepted.
        let broken_blob = scripted(
            Some(
                crate::identity_first::types::ContinuityResolveState::Broken {
                    failure: crate::identity_first::types::ContinuityFailure {
                        identity: identity.clone(),
                        kind:
                            crate::identity_first::types::ContinuityFailureKind::SnapshotCorrupted,
                        record: None,
                        detail: "scripted broken head, blob-only".to_string(),
                    },
                },
            ),
            false,
        );
        let blob_states =
            IdentityRuntime::resolve_persisted_owner_states(&broken_blob, &roster).await?;
        assert!(
            blob_states.is_empty(),
            "on a blob-only store Broken keeps the legacy best-effort disposition: zero states, \
             not an error"
        );

        // INV-06 before any store read: a duplicated roster identity must be
        // rejected as a malformed roster, NOT surface later as an ownership
        // conflict from register_session being called twice.
        let duplicated = vec![
            durable_spec(identity.clone(), "calendar"),
            durable_spec(identity.clone(), "calendar"),
        ];
        let ok_store = scripted(
            Some(crate::identity_first::types::ContinuityResolveState::Uninitialized),
            true,
        );
        let reads_before = reads.load(std::sync::atomic::Ordering::SeqCst);
        let inv06 = IdentityRuntime::resolve_persisted_owner_states(&ok_store, &duplicated).await;
        // The TYPED variant, not merely is_err(): an is_err() assertion passes on
        // any failure at all, including one from a later store read, which is
        // exactly the outcome this test exists to rule out.
        assert!(
            matches!(inv06, Err(IdentityRuntimeError::DuplicateIdentity(_))),
            "a duplicated roster identity must fail as DuplicateIdentity, not as some later \
             ownership conflict: {inv06:?}"
        );
        // And ZERO store reads: INV-06 runs before the first read, so the counter
        // must not have moved.
        assert_eq!(
            reads.load(std::sync::atomic::Ordering::SeqCst),
            reads_before,
            "INV-06 must reject a malformed roster BEFORE any store read"
        );
        Ok(())
    }

    /// Herd-investigation park: a build the HOST deterministically rejects
    /// (the candidate-mode effect gate) parks the identity typed on the
    /// FIRST attempt — no repair-loop churn — and a roster spec change
    /// re-admits exactly one new attempt.
    #[tokio::test]
    async fn host_rejected_build_parks_identity_until_spec_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:gated")?;
        let bridge = Arc::new(HostRejectingBridge::default());
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "host-reject-park-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        }));
        runtime
            .register(
                durable_spec(identity.clone(), "domain"),
                IdentityLifecycleState::Dormant,
                None,
                None,
            )
            .await;

        // The first attempt reaches the host exactly once; the deterministic
        // rejection parks the identity typed.
        assert!(
            runtime.materialize(&identity).await.is_err(),
            "gated build must fail"
        );
        assert_eq!(bridge.attempts(), 1);
        let park = runtime
            .host_rejected_build_park(&identity)
            .await
            .ok_or("the first host rejection must park the identity")?;
        assert!(park.reason.contains("callback error"));

        // Parked: the next attempt fails fast typed WITHOUT reaching the
        // host, and the repair supervisor's Broken selection excludes the
        // identity even when a reconcile re-registers it Broken with the
        // same spec.
        match runtime.materialize(&identity).await {
            Err(IdentityRuntimeError::HostRejectedBuild { reason, .. }) => {
                assert!(reason.contains("callback error"));
            }
            other => return Err(format!("expected the typed park, got {other:?}").into()),
        }
        assert_eq!(
            bridge.attempts(),
            1,
            "a parked identity must not re-ask the host"
        );
        runtime
            .register(
                durable_spec(identity.clone(), "domain"),
                IdentityLifecycleState::Broken,
                None,
                None,
            )
            .await;
        assert!(
            runtime.repairable_broken_identities().await.is_empty(),
            "the repair supervisor must skip a parked identity"
        );

        // A spec change clears the park: the retry is permitted and reaches
        // the host exactly once more.
        let mut changed = durable_spec(identity.clone(), "domain");
        changed
            .labels
            .insert("policy_epoch".to_string(), "2".to_string());
        runtime
            .register(changed, IdentityLifecycleState::Dormant, None, None)
            .await;
        assert!(
            runtime.host_rejected_build_park(&identity).await.is_none(),
            "a spec change must clear the park"
        );
        assert!(runtime.materialize(&identity).await.is_err());
        assert_eq!(
            bridge.attempts(),
            2,
            "a changed spec re-admits exactly one new build attempt"
        );
        Ok(())
    }

    /// A missing provider secret is a deterministic wall: the first typed
    /// refusal parks the identity with an operator-readable reason, the next
    /// attempt fails fast without a bridge call, and the repair supervisor
    /// skips it. Before this the refusal was flattened to text, matched
    /// nothing, and the identity heal-looped with the cause visible nowhere.
    #[tokio::test]
    async fn missing_secret_build_refusal_parks_identity_typed_on_first_attempt()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:keyless")?;
        let bridge = Arc::new(ProviderAuthRejectingBridge::new(
            meerkat_core::AuthErrorKind::MissingSecret,
        ));
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "provider-auth-park-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        }));
        runtime
            .register(
                durable_spec(identity.clone(), "domain"),
                IdentityLifecycleState::Dormant,
                None,
                None,
            )
            .await;

        // The FIRST attempt is already the typed park, not `Internal` prose:
        // a console send that materializes a Dormant keyless identity in-band
        // surfaces the reason on this very call.
        match runtime.materialize(&identity).await {
            Err(IdentityRuntimeError::HostRejectedBuild { reason, .. }) => {
                assert!(reason.contains("kind=missing_secret"), "{reason}");
            }
            other => {
                return Err(format!(
                    "the first keyless build must fail with the typed park, got {other:?}"
                )
                .into());
            }
        }
        assert_eq!(bridge.attempts(), 1);
        let park = runtime
            .host_rejected_build_park(&identity)
            .await
            .ok_or("the first typed provider-auth refusal must park the identity")?;
        assert!(
            park.reason.contains("kind=missing_secret"),
            "{}",
            park.reason
        );
        assert!(
            park.reason.contains("provider=anthropic"),
            "{}",
            park.reason
        );

        // Parked: the next attempt fails fast typed WITHOUT re-running the
        // build, and the repair supervisor's Broken selection excludes it.
        match runtime.materialize(&identity).await {
            Err(IdentityRuntimeError::HostRejectedBuild { reason, .. }) => {
                assert!(reason.contains("kind=missing_secret"), "{reason}");
            }
            other => return Err(format!("expected the typed park, got {other:?}").into()),
        }
        assert_eq!(
            bridge.attempts(),
            1,
            "a parked identity must not re-run the build"
        );
        runtime
            .register(
                durable_spec(identity.clone(), "domain"),
                IdentityLifecycleState::Broken,
                None,
                None,
            )
            .await;
        assert!(
            runtime.repairable_broken_identities().await.is_empty(),
            "the repair supervisor must skip a parked identity"
        );
        Ok(())
    }

    /// Credential STATE that can change out of band (here an expired lease)
    /// is not a wall: no park, and the next attempt reaches the bridge again.
    #[tokio::test]
    async fn transient_provider_auth_failure_stays_on_the_repair_lanes()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:expired")?;
        let bridge = Arc::new(ProviderAuthRejectingBridge::new(
            meerkat_core::AuthErrorKind::Expired,
        ));
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "provider-auth-transient-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        }));
        runtime
            .register(
                durable_spec(identity.clone(), "domain"),
                IdentityLifecycleState::Dormant,
                None,
                None,
            )
            .await;

        assert!(runtime.materialize(&identity).await.is_err());
        assert!(
            runtime.host_rejected_build_park(&identity).await.is_none(),
            "an expired credential must not park"
        );
        assert!(runtime.materialize(&identity).await.is_err());
        assert_eq!(
            bridge.attempts(),
            2,
            "an unparked identity re-attempts the build"
        );
        Ok(())
    }

    /// The classifier reads the typed refusal (its kind) and the host gate's
    /// minted text; everything else is not a deterministic wall.
    #[test]
    fn deterministic_build_rejection_reason_reads_the_typed_kind()
    -> Result<(), Box<dyn std::error::Error>> {
        let refusal = |kind| BridgeError::ProviderAuthRejected {
            failure: meerkat_core::service::SessionProviderAuthFailure {
                kind,
                provider: meerkat_core::Provider::OpenAI,
                realm_id: None,
                binding_id: None,
            },
            detail: "create_session: refused".to_string(),
        };
        let reason = deterministic_build_rejection_reason(&refusal(
            meerkat_core::AuthErrorKind::MissingSecret,
        ))
        .ok_or("a missing secret is a deterministic wall")?;
        assert!(
            reason.contains("provider=openai kind=missing_secret"),
            "{reason}"
        );
        assert!(
            reason.contains("Refusal: create_session: refused"),
            "{reason}"
        );
        for transient in [
            meerkat_core::AuthErrorKind::Expired,
            meerkat_core::AuthErrorKind::RefreshFailed,
            meerkat_core::AuthErrorKind::InteractiveLoginRequired,
            meerkat_core::AuthErrorKind::Io,
        ] {
            assert!(
                deterministic_build_rejection_reason(&refusal(transient)).is_none(),
                "{transient:?} can change out of band and must not park"
            );
        }
        let host_gate = BridgeError::Mob(
            "spawn_member: callback/build_agent failed: callback error: gate refused".to_string(),
        );
        assert!(deterministic_build_rejection_reason(&host_gate).is_some());
        let transport = BridgeError::Mob("callback transport closed".to_string());
        assert!(deterministic_build_rejection_reason(&transport).is_none());
        Ok(())
    }

    #[test]
    fn external_delivery_omits_unrepresentable_interaction_id_carrier()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("target-smoke")?;
        let mut external = durable_spec(identity.clone(), "target");
        external.backend = Some(meerkat_mob::MobBackendKind::External);
        assert_eq!(
            interaction_id_for_delivery(&external, Some("interaction-1")),
            None
        );

        let local = durable_spec(identity, "target");
        assert_eq!(
            interaction_id_for_delivery(&local, Some("interaction-1")),
            Some("interaction-1")
        );
        Ok(())
    }

    async fn lazy_context_with_broken_retained_lease(
        identity: AgentIdentity,
        spec: DurableAgentSpec,
        roster: Arc<MutableRoster>,
        lease_provider: Arc<RecordingReleaseLeaseProvider>,
        runtime_instance_id: &str,
    ) -> Result<
        (
            Arc<IdentityRuntime>,
            IdentityFirstRuntimeContext,
            LeaseGrant,
        ),
        Box<dyn std::error::Error>,
    > {
        let continuity_store = Arc::new(LocalContinuityStore::in_memory()?);
        let acquired = lease_provider
            .acquire_leases(std::slice::from_ref(&identity), runtime_instance_id)
            .await?;
        let grant = match acquired.get(&identity) {
            Some(LeaseAcquireResult::Acquired(grant)) => grant.clone(),
            other => return Err(format!("expected retained lease grant, got {other:?}").into()),
        };
        let record = ContinuityRecord {
            identity: identity.clone(),
            agent_runtime_id: AgentRuntimeId::parse(&format!("rt:{identity}:0"))?,
            session_id: SessionId::new(),
            generation: ContinuityGeneration::new(0),
            checkpoint_version: CheckpointVersion::new(0),
        };
        continuity_store
            .upsert_continuity_record(&record, grant.fencing_token)
            .await?;
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store,
            lease_provider,
            runtime_instance_id: runtime_instance_id.to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: None,
            default_timeout: None,
        }));
        runtime
            .register(
                spec,
                IdentityLifecycleState::Active,
                Some(record),
                Some(grant.clone()),
            )
            .await;
        // Model the public rollback state exercised by
        // `identity_first_runtime_reset_register_failure_cleans_new_member_and_preserves_old_continuity`:
        // the identity is fail-closed Broken while its exact current grant is
        // retained in `entry.lease` for repair.
        runtime
            .entries
            .write()
            .await
            .get_mut(&identity)
            .ok_or("seeded identity disappeared")?
            .state = IdentityLifecycleState::Broken;
        let context = IdentityFirstRuntimeContext::new_with_bootstrap_mode(
            runtime.clone(),
            roster,
            None,
            None,
            None,
            IdentityBootstrapMode::LazyMaterialize,
        );
        Ok((runtime, context, grant))
    }

    async fn active_alias_runtime(
        runtime_instance_id: &str,
        identity: &str,
    ) -> Result<(Arc<IdentityRuntime>, String), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse(identity)?;
        let alias = format!("rt:{identity}:0");
        let record = ContinuityRecord {
            identity: identity.clone(),
            agent_runtime_id: AgentRuntimeId::parse(&alias)?,
            session_id: SessionId::new(),
            generation: ContinuityGeneration::new(0),
            checkpoint_version: CheckpointVersion::new(0),
        };
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: runtime_instance_id.to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: None,
            default_timeout: None,
        }));
        runtime
            .register(
                durable_spec(identity, "domain"),
                IdentityLifecycleState::Active,
                Some(record),
                None,
            )
            .await;
        Ok((runtime, alias))
    }

    async fn alias_target(
        runtime: &Arc<IdentityRuntime>,
        alias: &str,
    ) -> Result<MemberAliasLifecycleTarget, Box<dyn std::error::Error>> {
        runtime
            .member_alias_lifecycle_target(alias)
            .await?
            .ok_or_else(|| {
                format!("generated alias did not resolve to lifecycle target: {alias}").into()
            })
    }

    #[tokio::test]
    async fn permanent_stream_loss_breaks_only_the_current_active_embodiment()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:stream-loss")?;
        let (runtime, alias) =
            active_alias_runtime("stream-loss-runtime", identity.as_str()).await?;
        runtime
            .update_lease(
                &identity,
                LeaseGrant {
                    identity: identity.clone(),
                    fencing_token: FencingToken::new(7),
                    ttl: Duration::from_mins(1),
                },
            )
            .await?;

        assert!(
            runtime
                .mark_active_runtime_broken(&identity, &alias, 7, "event stream closed")
                .await?
        );
        assert_eq!(
            runtime.status(&identity).await?.state,
            IdentityLifecycleState::Broken
        );
        assert!(
            runtime
                .entries
                .read()
                .await
                .get(&identity)
                .and_then(|entry| entry.lease.as_ref())
                .is_some(),
            "repair must retain the exact live lease for fenced cleanup"
        );
        Ok(())
    }

    #[tokio::test]
    async fn stale_stream_loss_does_not_break_replacement_embodiment()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:replacement")?;
        let (runtime, _) = active_alias_runtime("replacement-runtime", identity.as_str()).await?;

        assert!(
            !runtime
                .mark_active_runtime_broken(
                    &identity,
                    "rt:domain:replacement:stale",
                    0,
                    "stale event stream closed",
                )
                .await?
        );
        assert_eq!(
            runtime.status(&identity).await?.state,
            IdentityLifecycleState::Active
        );
        Ok(())
    }

    #[tokio::test]
    async fn stale_fence_stream_loss_does_not_break_same_alias_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:replacement-fence")?;
        let (runtime, alias) =
            active_alias_runtime("replacement-fence-runtime", identity.as_str()).await?;
        runtime
            .update_lease(
                &identity,
                LeaseGrant {
                    identity: identity.clone(),
                    fencing_token: FencingToken::new(8),
                    ttl: Duration::from_mins(1),
                },
            )
            .await?;

        assert!(
            !runtime
                .mark_active_runtime_broken(&identity, &alias, 7, "old-fence event stream closed",)
                .await?
        );
        assert_eq!(
            runtime.status(&identity).await?.state,
            IdentityLifecycleState::Active
        );
        Ok(())
    }

    #[tokio::test]
    async fn compound_alias_targets_sort_opposite_orders_without_deadlock()
    -> Result<(), Box<dyn std::error::Error>> {
        let (runtime_a, alias_a) = active_alias_runtime("00-alias-runtime", "domain:alpha").await?;
        let (runtime_b, alias_b) = active_alias_runtime("01-alias-runtime", "domain:beta").await?;

        // Hold the globally first lock while both transactions are admitted.
        // Correct ordering makes both wait on A without touching B. An input-
        // ordered implementation lets the reverse request take B first and
        // then deadlocks once the forward request receives A.
        let held_a = alias_target(&runtime_a, &alias_a)
            .await?
            .lock
            .clone()
            .lock_owned()
            .await;
        let completed = Arc::new(AtomicUsize::new(0));

        let forward = tokio::spawn({
            let targets = vec![
                alias_target(&runtime_a, &alias_a).await?,
                alias_target(&runtime_b, &alias_b).await?,
            ];
            let completed = Arc::clone(&completed);
            async move {
                IdentityRuntime::run_member_alias_targets_operation_tracked(targets, move || {
                    let completed = Arc::clone(&completed);
                    async move {
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !runtime_a.foreground_operations.lock().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;

        let reverse = tokio::spawn({
            let targets = vec![
                alias_target(&runtime_b, &alias_b).await?,
                alias_target(&runtime_a, &alias_a).await?,
            ];
            let completed = Arc::clone(&completed);
            async move {
                IdentityRuntime::run_member_alias_targets_operation_tracked(targets, move || {
                    let completed = Arc::clone(&completed);
                    async move {
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if runtime_a.foreground_operations.lock().await.len() >= 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        tokio::task::yield_now().await;

        let b_probe = alias_target(&runtime_b, &alias_b)
            .await?
            .lock
            .try_lock_owned()
            .map_err(|_| "reverse-order request acquired B before globally-first A")?;
        drop(b_probe);
        drop(held_a);

        tokio::time::timeout(Duration::from_secs(2), forward).await???;
        tokio::time::timeout(Duration::from_secs(2), reverse).await???;
        assert_eq!(completed.load(Ordering::SeqCst), 2);

        runtime_a.close_foreground_operations();
        runtime_b.close_foreground_operations();
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                runtime_a.join_foreground_operations(),
                runtime_b.join_foreground_operations()
            );
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn dropped_compound_alias_caller_still_completes_operation_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let (runtime_a, alias_a) = active_alias_runtime("00-drop-runtime", "domain:alpha").await?;
        let (runtime_b, alias_b) = active_alias_runtime("01-drop-runtime", "domain:beta").await?;
        let targets = vec![
            alias_target(&runtime_a, &alias_a).await?,
            alias_target(&runtime_b, &alias_b).await?,
        ];
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let caller = tokio::spawn({
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let completed = Arc::clone(&completed);
            async move {
                IdentityRuntime::run_member_alias_targets_operation_tracked(targets, move || {
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    let completed = Arc::clone(&completed);
                    async move {
                        entered.add_permits(1);
                        release
                            .acquire()
                            .await
                            .map_err(|error| error.to_string())?
                            .forget();
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), entered.acquire())
            .await??
            .forget();

        caller.abort();
        match caller.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(result) => {
                return Err(format!("aborted caller unexpectedly returned: {result:?}").into());
            }
        }
        release.add_permits(1);

        runtime_a.close_foreground_operations();
        runtime_b.close_foreground_operations();
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                runtime_a.join_foreground_operations(),
                runtime_b.join_foreground_operations()
            );
        })
        .await?;
        assert_eq!(
            completed.load(Ordering::SeqCst),
            1,
            "runtime-owned compound transaction must reach its boundary"
        );
        Ok(())
    }

    #[tokio::test]
    async fn either_compound_alias_runtime_shutdown_waits_for_operation_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        for shutdown_first_runtime in [true, false] {
            let suffix = if shutdown_first_runtime {
                "first"
            } else {
                "second"
            };
            let (runtime_a, alias_a) =
                active_alias_runtime(&format!("00-shutdown-{suffix}"), "domain:alpha").await?;
            let (runtime_b, alias_b) =
                active_alias_runtime(&format!("01-shutdown-{suffix}"), "domain:beta").await?;
            let targets = vec![
                alias_target(&runtime_a, &alias_a).await?,
                alias_target(&runtime_b, &alias_b).await?,
            ];
            let entered = Arc::new(tokio::sync::Semaphore::new(0));
            let release = Arc::new(tokio::sync::Semaphore::new(0));
            let operation = tokio::spawn({
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                async move {
                    IdentityRuntime::run_member_alias_targets_operation_tracked(
                        targets,
                        move || {
                            let entered = Arc::clone(&entered);
                            let release = Arc::clone(&release);
                            async move {
                                entered.add_permits(1);
                                release
                                    .acquire()
                                    .await
                                    .map_err(|error| error.to_string())?
                                    .forget();
                                Ok(())
                            }
                        },
                    )
                    .await
                }
            });
            tokio::time::timeout(Duration::from_secs(2), entered.acquire())
                .await??
                .forget();

            let shutting_down = if shutdown_first_runtime {
                Arc::clone(&runtime_a)
            } else {
                Arc::clone(&runtime_b)
            };
            shutting_down.close_foreground_operations();
            let mut shutdown = tokio::spawn(async move {
                shutting_down.join_foreground_operations().await;
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut shutdown)
                    .await
                    .is_err(),
                "shutdown of {suffix} participating runtime returned before compound boundary"
            );

            release.add_permits(1);
            tokio::time::timeout(Duration::from_secs(2), &mut shutdown).await??;
            tokio::time::timeout(Duration::from_secs(2), operation).await???;

            runtime_a.close_foreground_operations();
            runtime_b.close_foreground_operations();
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::join!(
                    runtime_a.join_foreground_operations(),
                    runtime_b.join_foreground_operations()
                );
            })
            .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn restore_flow_bounds_parallel_member_creation() -> Result<(), Box<dyn std::error::Error>>
    {
        let specs = (0..8)
            .map(|index| {
                Ok(durable_spec(
                    AgentIdentity::parse(&format!("domain:restore-{index}"))?,
                    "domain",
                ))
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        let bridge = Arc::new(RecordingBridge {
            create_delay: Duration::from_millis(25),
            ..RecordingBridge::default()
        });
        let runtime = IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "parallel-restore-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        });

        let result = super::super::orchestrator::restore_flow(&runtime, &specs, None, None).await?;

        assert_eq!(result.outcomes.len(), specs.len());
        assert!(
            bridge.max_creates_in_flight() > 1,
            "member creation should no longer be serial"
        );
        assert!(
            bridge.max_creates_in_flight()
                <= super::super::orchestrator::IDENTITY_RESTORE_CONCURRENCY,
            "restore concurrency must remain bounded"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reset_reprofiles_session_from_runtime_configured_roster_provider()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:security")?;
        let roster = Arc::new(MutableRoster::new(vec![durable_spec(
            identity.clone(),
            "domain",
        )]));
        let bridge = Arc::new(RecordingBridge::default());
        let runtime = Arc::new(
            IdentityRuntime::new(IdentityRuntimeConfig {
                continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
                lease_provider: Arc::new(LocalLeaseProvider::new()),
                runtime_instance_id: "reset-reprofile-test".to_string(),
                has_runtime_store: true,
                durability_policy: DurabilityPolicy::SyncWriteThrough,
                bridge: Some(bridge.clone()),
                default_timeout: None,
            })
            .with_reset_roster_provider(roster.clone()),
        );

        super::super::orchestrator::restore_flow(
            &runtime,
            &roster
                .roster(&RosterContext {
                    mob_definition: None,
                    previous_identities: Vec::new(),
                })
                .await?,
            None,
            None,
        )
        .await?;
        roster
            .set(vec![durable_spec(identity.clone(), "security")])
            .await;

        let record = runtime.reset(&identity).await?;

        assert_eq!(record.generation.get(), 1);
        assert_eq!(
            bridge.create_profiles().await,
            vec!["domain".to_string(), "security".to_string()]
        );
        let status = runtime.status(&identity).await?;
        assert_eq!(
            status.profile.map(|profile| profile.to_string()).as_deref(),
            Some("security")
        );
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_joins_reset_after_outer_abort_at_final_upsert()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:security")?;
        let roster = Arc::new(MutableRoster::new(vec![durable_spec(
            identity.clone(),
            "domain",
        )]));
        let bridge = Arc::new(RecordingBridge::default());
        let store = Arc::new(GatedResetContinuityStore::new()?);
        let runtime = Arc::new(
            IdentityRuntime::new(IdentityRuntimeConfig {
                continuity_store: store.clone(),
                lease_provider: Arc::new(LocalLeaseProvider::new()),
                runtime_instance_id: "tracked-reset-shutdown-test".to_string(),
                has_runtime_store: true,
                durability_policy: DurabilityPolicy::SyncWriteThrough,
                bridge: Some(bridge.clone()),
                default_timeout: None,
            })
            .with_reset_roster_provider(roster.clone()),
        );

        super::super::orchestrator::restore_flow(
            &runtime,
            &roster
                .roster(&RosterContext {
                    mob_definition: None,
                    previous_identities: Vec::new(),
                })
                .await?,
            None,
            None,
        )
        .await?;
        let old_record = match store
            .resolve_many(std::slice::from_ref(&identity))
            .await?
            .remove(&identity)
        {
            Some(ContinuityResolveState::Ready { record }) => record,
            other => return Err(format!("expected initial continuity, got {other:?}").into()),
        };
        store.fail_after_successful_upserts(3);

        let outer = tokio::spawn({
            let runtime = Arc::clone(&runtime);
            let identity = identity.clone();
            async move { runtime.reset_tracked(&identity).await }
        });
        // Bounded. This waits for an injected failure at the FINAL continuity
        // upsert, which is unreachable whenever reset refuses earlier - for
        // example the reprofile guard refusing a drifted spec before the
        // destructive step. Unbounded, that turns into a suite-blocking hang
        // that reports nothing; bounded, it names itself.
        tokio::time::timeout(Duration::from_secs(10), store.wait_for_failure())
            .await
            .map_err(|_| {
                "reset never reached the final continuity upsert, so the injected failure could \
                 not fire; reset refused earlier in the flow"
            })?;
        outer.abort();
        match outer.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(result) => {
                return Err(
                    format!("outer reset waiter unexpectedly completed: {result:?}").into(),
                );
            }
        }

        runtime.close_foreground_operations();
        let mut join = tokio::spawn({
            let runtime = Arc::clone(&runtime);
            async move { runtime.join_foreground_operations().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut join)
                .await
                .is_err(),
            "shutdown must wait for the runtime-owned reset transaction"
        );
        assert!(
            bridge.retired_runtime_ids().await.is_empty(),
            "rollback cleanup cannot run before the final upsert resolves"
        );

        store.release_failure();
        tokio::time::timeout(Duration::from_secs(2), &mut join)
            .await
            .map_err(|_| "shutdown did not join reset rollback")??;

        let resolved = store.resolve_many(std::slice::from_ref(&identity)).await?;
        assert_eq!(
            resolved.get(&identity),
            Some(&ContinuityResolveState::Ready {
                record: old_record.clone(),
            }),
            "abandoned reset must roll durable continuity back before shutdown completes"
        );
        let status = runtime.status(&identity).await?;
        assert_eq!(
            status.agent_runtime_id.as_ref(),
            Some(&old_record.agent_runtime_id)
        );
        assert_eq!(status.state, IdentityLifecycleState::Broken);
        assert_eq!(
            bridge.retired_runtime_ids().await,
            vec!["rt:domain:security:1".to_string()],
            "rollback must retire only the tentative reset generation"
        );
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_cancels_reset_customizer_and_restores_old_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:security")?;
        let spec = durable_spec(identity.clone(), "domain");
        let bridge = Arc::new(RecordingBridge::default());
        let store = Arc::new(LocalContinuityStore::in_memory()?);
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: store.clone(),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "reset-customizer-shutdown-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        }));

        super::super::orchestrator::restore_flow(&runtime, std::slice::from_ref(&spec), None, None)
            .await?;
        let old_record = match store
            .resolve_many(std::slice::from_ref(&identity))
            .await?
            .remove(&identity)
        {
            Some(ContinuityResolveState::Ready { record }) => record,
            other => return Err(format!("expected initial continuity, got {other:?}").into()),
        };

        let customizer = Arc::new(GatedResetCustomizer::default());
        runtime.set_agent_customizer(Some(customizer.clone())).await;
        let outer = tokio::spawn({
            let runtime = Arc::clone(&runtime);
            let identity = identity.clone();
            async move { runtime.reset_tracked(&identity).await }
        });
        tokio::time::timeout(Duration::from_secs(2), customizer.wait_for_entry())
            .await
            .map_err(|_| "reset did not enter the gated customizer")?;
        outer.abort();
        match outer.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(result) => {
                return Err(
                    format!("outer reset waiter unexpectedly completed: {result:?}").into(),
                );
            }
        }

        runtime.close_foreground_operations();
        tokio::time::timeout(Duration::from_secs(2), runtime.join_foreground_operations())
            .await
            .map_err(|_| "shutdown hung on the reset customizer")?;

        let resolved = store.resolve_many(std::slice::from_ref(&identity)).await?;
        assert_eq!(
            resolved.get(&identity),
            Some(&ContinuityResolveState::Ready {
                record: old_record.clone(),
            }),
            "shutdown cancellation must roll durable continuity back to the old generation"
        );
        let status = runtime.status(&identity).await?;
        assert_eq!(status.state, IdentityLifecycleState::Active);
        assert_eq!(
            status.agent_runtime_id.as_ref(),
            Some(&old_record.agent_runtime_id)
        );
        assert_eq!(status.session_id.as_ref(), Some(&old_record.session_id));
        assert_eq!(bridge.create_profiles().await, vec!["domain".to_string()]);
        assert!(
            bridge.retired_runtime_ids().await.is_empty(),
            "cancellation before session installation must not touch either bridge generation"
        );
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_cancelled_dormant_reset_releases_temporary_lease()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:dormant")?;
        let bridge = Arc::new(RecordingBridge::default());
        let store = Arc::new(LocalContinuityStore::in_memory()?);
        let lease_provider = Arc::new(LocalLeaseProvider::new());
        let runtime_instance_id = "dormant-reset-customizer-shutdown-test";
        let acquired = lease_provider
            .acquire_leases(std::slice::from_ref(&identity), runtime_instance_id)
            .await?;
        let initial_grant = match acquired.get(&identity) {
            Some(super::super::types::LeaseAcquireResult::Acquired(grant)) => grant.clone(),
            other => return Err(format!("expected initial lease, got {other:?}").into()),
        };
        let old_record = ContinuityRecord {
            identity: identity.clone(),
            agent_runtime_id: AgentRuntimeId::parse("rt:domain:dormant:0")?,
            session_id: SessionId::new(),
            generation: ContinuityGeneration::new(0),
            checkpoint_version: CheckpointVersion::new(0),
        };
        store
            .upsert_continuity_record(&old_record, initial_grant.fencing_token)
            .await?;
        lease_provider
            .release_leases(std::slice::from_ref(&initial_grant))
            .await?;
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: store.clone(),
            lease_provider: lease_provider.clone(),
            runtime_instance_id: runtime_instance_id.to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        }));
        runtime
            .register(
                durable_spec(identity.clone(), "domain"),
                IdentityLifecycleState::Dormant,
                Some(old_record.clone()),
                None,
            )
            .await;

        let customizer = Arc::new(GatedResetCustomizer::default());
        runtime.set_agent_customizer(Some(customizer.clone())).await;
        let outer = tokio::spawn({
            let runtime = Arc::clone(&runtime);
            let identity = identity.clone();
            async move { runtime.reset_tracked(&identity).await }
        });
        tokio::time::timeout(Duration::from_secs(2), customizer.wait_for_entry())
            .await
            .map_err(|_| "dormant reset did not enter the gated customizer")?;
        outer.abort();
        match outer.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(result) => {
                return Err(
                    format!("outer dormant reset unexpectedly completed: {result:?}").into(),
                );
            }
        }
        runtime.close_foreground_operations();
        tokio::time::timeout(Duration::from_secs(2), runtime.join_foreground_operations())
            .await
            .map_err(|_| "shutdown hung on the dormant reset customizer")?;

        let status = runtime.status(&identity).await?;
        assert_eq!(status.state, IdentityLifecycleState::Dormant);
        assert!(status.lease.is_none());
        let resolved = store.resolve_many(std::slice::from_ref(&identity)).await?;
        assert_eq!(
            resolved.get(&identity),
            Some(&ContinuityResolveState::Ready {
                record: old_record.clone(),
            })
        );
        let failover = lease_provider
            .acquire_leases(std::slice::from_ref(&identity), "dormant-reset-failover")
            .await?;
        assert!(
            matches!(
                failover.get(&identity),
                Some(super::super::types::LeaseAcquireResult::Acquired(_))
            ),
            "shutdown rollback must release the dormant reset lease: {failover:?}"
        );
        assert!(bridge.create_profiles().await.is_empty());
        assert!(bridge.retired_runtime_ids().await.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn tracked_identity_respawn_preserves_authoritative_continuity()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:durable-respawn")?;
        let spec = durable_spec(identity.clone(), "domain");
        let bridge = Arc::new(RecordingBridge::default());
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "durable-respawn".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        }));
        super::super::orchestrator::restore_flow(&runtime, std::slice::from_ref(&spec), None, None)
            .await?;

        let before = runtime.status(&identity).await?;
        let runtime_alias = before
            .agent_runtime_id
            .clone()
            .ok_or("missing durable runtime id")?;
        let respawned = runtime
            .respawn_identity_in_place_tracked(&identity, Some(runtime_alias.as_str()))
            .await?;

        assert_eq!(Some(respawned.session_id), before.session_id);
        assert_eq!(Some(respawned.agent_runtime_id), before.agent_runtime_id);
        assert_eq!(Some(respawned.generation), before.generation);
        assert_eq!(
            runtime.status(&identity).await?.state,
            IdentityLifecycleState::Active
        );
        assert!(
            bridge.retired_runtime_ids().await.is_empty(),
            "identity respawn must not retire and recreate the authoritative session"
        );
        Ok(())
    }

    #[tokio::test]
    async fn stale_alias_materialization_preserves_active_and_dormant_bootstrap_status()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:bootstrap-alias")?;
        let spec = durable_spec(identity.clone(), "domain");
        let current_alias = AgentRuntimeId::parse("rt:domain:bootstrap-alias:1")?;
        let stale_alias = "rt:domain:bootstrap-alias:0";
        let store = Arc::new(LocalContinuityStore::in_memory()?);
        let lease_provider = Arc::new(LocalLeaseProvider::new());
        let grants = lease_provider
            .acquire_leases(std::slice::from_ref(&identity), "bootstrap-alias-status")
            .await?;
        let grant = match grants.get(&identity) {
            Some(super::super::types::LeaseAcquireResult::Acquired(grant)) => grant.clone(),
            other => return Err(format!("expected acquired lease, got {other:?}").into()),
        };
        let record = ContinuityRecord {
            identity: identity.clone(),
            agent_runtime_id: current_alias,
            session_id: SessionId::new(),
            generation: ContinuityGeneration::new(1),
            checkpoint_version: CheckpointVersion::new(0),
        };
        store
            .upsert_continuity_record(&record, grant.fencing_token)
            .await?;
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: store,
            lease_provider,
            runtime_instance_id: "bootstrap-alias-status".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: None,
            default_timeout: None,
        }));
        runtime
            .register(
                spec.clone(),
                IdentityLifecycleState::Active,
                Some(record),
                Some(grant),
            )
            .await;

        let active_generation =
            runtime.begin_identity_bootstrap_pending(IdentityBootstrapMode::LazyMaterialize);
        runtime
            .begin_identity_bootstrap(
                active_generation,
                IdentityBootstrapMode::LazyMaterialize,
                std::slice::from_ref(&spec),
            )
            .await;
        runtime.modify_bootstrap_status(Some(active_generation), |status| {
            status.complete = true;
            status.refresh_aggregates();
        });
        let active_before = runtime.identity_bootstrap_status();
        assert_eq!(
            active_before
                .identities
                .get(&identity)
                .map(|entry| entry.state),
            Some(IdentityBootstrapState::Active)
        );
        assert!(matches!(
            runtime
                .materialize_with_expected_member_alias(&identity, Some(stale_alias))
                .await,
            Err(IdentityRuntimeError::StaleRuntimeAlias { .. })
        ));
        assert_eq!(runtime.identity_bootstrap_status(), active_before);

        runtime.retire(&identity).await?;
        let dormant_generation =
            runtime.begin_identity_bootstrap_pending(IdentityBootstrapMode::LazyMaterialize);
        runtime
            .begin_identity_bootstrap(
                dormant_generation,
                IdentityBootstrapMode::LazyMaterialize,
                std::slice::from_ref(&spec),
            )
            .await;
        runtime.modify_bootstrap_status(Some(dormant_generation), |status| {
            status.complete = true;
            status.refresh_aggregates();
        });
        let dormant_before = runtime.identity_bootstrap_status();
        assert_eq!(
            dormant_before
                .identities
                .get(&identity)
                .map(|entry| entry.state),
            Some(IdentityBootstrapState::Dormant)
        );
        assert!(matches!(
            runtime
                .materialize_with_expected_member_alias(&identity, Some(stale_alias))
                .await,
            Err(IdentityRuntimeError::StaleRuntimeAlias { .. })
        ));
        assert_eq!(runtime.identity_bootstrap_status(), dormant_before);
        Ok(())
    }

    #[tokio::test]
    async fn eager_reconcile_hot_reloads_active_metadata_without_retire_or_resume()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("identity:luka")?;
        let mut initial_spec = durable_spec(identity.clone(), "personal");
        initial_spec
            .labels
            .insert("revision".to_string(), "v1".to_string());
        let roster = Arc::new(MutableRoster::new(vec![initial_spec.clone()]));
        let bridge = Arc::new(HotReloadBridge::default());
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "eager-hot-reload-contract".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        }));
        let context = IdentityFirstRuntimeContext::new_with_bootstrap_mode(
            runtime.clone(),
            roster.clone(),
            None,
            None,
            None,
            IdentityBootstrapMode::EagerMaterialize,
        );
        context
            .bootstrap_roster(std::slice::from_ref(&initial_spec))
            .await?;

        let before = runtime.status(&identity).await?;
        assert_eq!(before.state, IdentityLifecycleState::Active);
        let before_session = before.session_id.clone();
        let before_runtime_id = before.agent_runtime_id.clone();
        let before_generation = before.generation;
        let before_token = before
            .lease
            .as_ref()
            .ok_or("initial active identity must have a lease")?
            .fencing_token;

        let mut updated_spec = initial_spec;
        updated_spec.addressability = AgentAddressability::InternalOnly;
        updated_spec
            .labels
            .insert("timezone".to_string(), "Europe/Stockholm".to_string());
        updated_spec
            .labels
            .insert("revision".to_string(), "v2".to_string());
        roster.set(vec![updated_spec]).await;

        let result = context.refresh_desired_topology().await?;
        assert!(matches!(
            result.outcomes.get(&identity),
            Some(super::super::orchestrator::RestoreOutcome::Resumed { .. })
        ));
        let after = runtime.status(&identity).await?;
        assert_eq!(after.state, IdentityLifecycleState::Active);
        assert_eq!(after.addressability, AgentAddressability::InternalOnly);
        assert_eq!(
            after.labels.get("timezone").map(String::as_str),
            Some("Europe/Stockholm")
        );
        assert_eq!(after.session_id, before_session);
        assert_eq!(after.agent_runtime_id, before_runtime_id);
        assert_eq!(after.generation, before_generation);
        assert_eq!(
            after
                .lease
                .as_ref()
                .ok_or("hot-reloaded identity must retain its lease")?
                .fencing_token,
            before_token,
            "same-profile reconcile must preserve exact live authority"
        );
        assert_eq!(bridge.retire_calls.load(Ordering::SeqCst), 0);
        assert_eq!(bridge.resume_calls.load(Ordering::SeqCst), 0);

        assert!(matches!(
            runtime
                .send(
                    &identity,
                    &meerkat_core::ContentInput::Text("external".to_string()),
                )
                .await,
            Err(IdentityRuntimeError::NotAddressable(_))
        ));
        let (dispatch_token, durable) = runtime
            .dispatch(&identity, &DispatchInput::system("system notice"))
            .await?;
        assert_eq!(dispatch_token, before_token);
        assert!(durable);
        Ok(())
    }

    #[tokio::test]
    async fn lazy_unchanged_reconcile_retained_broken_lease_failure_stays_shutdown_visible()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("identity:broken-retained-unchanged")?;
        let spec = durable_spec(identity.clone(), "personal");
        let roster = Arc::new(MutableRoster::new(vec![spec.clone()]));
        let lease_provider = Arc::new(RecordingReleaseLeaseProvider::default());
        let (runtime, context, retained_grant) = lazy_context_with_broken_retained_lease(
            identity.clone(),
            spec,
            roster,
            lease_provider.clone(),
            "broken-retained-unchanged",
        )
        .await?;

        lease_provider.fail_next_release();
        let error = match context.refresh_desired_topology().await {
            Err(error) => error,
            Ok(_) => return Err("failed exact release allowed lazy registration".into()),
        };
        assert!(
            error
                .to_string()
                .contains("synthetic retained Broken lease release failure")
        );
        let status = runtime.status(&identity).await?;
        assert_eq!(status.state, IdentityLifecycleState::Broken);
        assert!(
            status.lease.is_none(),
            "failed release authority must be pending, never advertised as active"
        );
        assert_eq!(
            runtime
                .entries
                .read()
                .await
                .get(&identity)
                .and_then(|entry| entry.pending_lease_release.as_ref())
                .map(|grant| grant.fencing_token),
            Some(retained_grant.fencing_token),
            "the exact retained token must survive provider failure"
        );
        assert_eq!(
            lease_provider
                .release_attempts()
                .await
                .iter()
                .map(|grant| grant.fencing_token)
                .collect::<Vec<_>>(),
            vec![retained_grant.fencing_token]
        );

        assert_eq!(
            runtime.release_all_leases_for_shutdown().await?,
            1,
            "shutdown must retain visibility of the staged exact grant"
        );
        assert_eq!(
            lease_provider
                .release_attempts()
                .await
                .iter()
                .map(|grant| grant.fencing_token)
                .collect::<Vec<_>>(),
            vec![retained_grant.fencing_token, retained_grant.fencing_token],
            "shutdown must retry the same exact fencing token"
        );
        assert!(
            runtime
                .entries
                .read()
                .await
                .get(&identity)
                .is_some_and(|entry| {
                    entry.lease.is_none() && entry.pending_lease_release.is_none()
                })
        );

        context.refresh_desired_topology().await?;
        let recovered = runtime.status(&identity).await?;
        assert_eq!(recovered.state, IdentityLifecycleState::Dormant);
        assert!(recovered.lease.is_none());
        let failover = lease_provider
            .acquire_leases(std::slice::from_ref(&identity), "other-runtime")
            .await?;
        assert!(matches!(
            failover.get(&identity),
            Some(LeaseAcquireResult::Acquired(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn lazy_profile_replace_releases_retained_broken_lease_before_overwrite()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("identity:broken-retained-replace")?;
        let original = durable_spec(identity.clone(), "personal-v1");
        let roster = Arc::new(MutableRoster::new(vec![original.clone()]));
        let lease_provider = Arc::new(RecordingReleaseLeaseProvider::default());
        let (runtime, context, retained_grant) = lazy_context_with_broken_retained_lease(
            identity.clone(),
            original,
            roster.clone(),
            lease_provider.clone(),
            "broken-retained-replace",
        )
        .await?;

        roster
            .set(vec![durable_spec(identity.clone(), "personal-v2")])
            .await;
        context.refresh_desired_topology().await?;

        let status = runtime.status(&identity).await?;
        assert_eq!(status.state, IdentityLifecycleState::Dormant);
        assert_eq!(
            status.profile.as_ref().map(ToString::to_string).as_deref(),
            Some("personal-v2")
        );
        assert!(status.lease.is_none());
        assert_eq!(
            lease_provider
                .release_attempts()
                .await
                .iter()
                .map(|grant| grant.fencing_token)
                .collect::<Vec<_>>(),
            vec![retained_grant.fencing_token],
            "profile replacement must release the retained exact token before lazy registration"
        );
        assert!(
            runtime
                .entries
                .read()
                .await
                .get(&identity)
                .is_some_and(|entry| entry.pending_lease_release.is_none())
        );
        let failover = lease_provider
            .acquire_leases(std::slice::from_ref(&identity), "replacement-failover")
            .await?;
        assert!(matches!(
            failover.get(&identity),
            Some(LeaseAcquireResult::Acquired(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn lease_lost_then_roster_remove_cleans_lower_plane_before_dropping_entry()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("identity:lost-remove")?;
        let spec = durable_spec(identity.clone(), "personal");
        let roster = Arc::new(MutableRoster::new(vec![spec.clone()]));
        let bridge = Arc::new(LostCleanupBridge::default());
        let lease_provider = Arc::new(LostRenewLeaseProvider::default());
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: lease_provider.clone(),
            runtime_instance_id: "lost-remove-cleanup".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        }));
        let context = IdentityFirstRuntimeContext::new_with_bootstrap_mode(
            runtime.clone(),
            roster.clone(),
            None,
            None,
            None,
            IdentityBootstrapMode::EagerMaterialize,
        );
        context
            .bootstrap_roster(std::slice::from_ref(&spec))
            .await?;
        assert_eq!(bridge.member_count().await, 1);
        assert_eq!(bridge.session_runtime_state_count().await, 1);

        force_renewal_lost(&runtime, &lease_provider, &identity).await?;
        assert_eq!(
            runtime.status(&identity).await?.state,
            IdentityLifecycleState::Broken
        );
        assert_eq!(bridge.member_count().await, 1);
        let unregisters_after_lost = bridge.unregister_calls.load(Ordering::SeqCst);

        roster.set(Vec::new()).await;
        context.refresh_desired_topology().await?;
        assert!(!runtime.contains(&identity).await);
        assert_eq!(
            bridge.member_count().await,
            0,
            "stale member survived removal"
        );
        assert_eq!(
            bridge.session_runtime_state_count().await,
            0,
            "stale session authority survived removal"
        );
        assert_eq!(bridge.retire_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            bridge.unregister_calls.load(Ordering::SeqCst),
            unregisters_after_lost + 1,
            "roster removal must idempotently unregister after Lost"
        );
        Ok(())
    }

    #[tokio::test]
    async fn lease_lost_then_profile_replace_cleans_old_member_before_resume()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("identity:lost-replace")?;
        let original = durable_spec(identity.clone(), "personal-v1");
        let roster = Arc::new(MutableRoster::new(vec![original.clone()]));
        let bridge = Arc::new(LostCleanupBridge::default());
        let lease_provider = Arc::new(LostRenewLeaseProvider::default());
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: lease_provider.clone(),
            runtime_instance_id: "lost-replace-cleanup".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        }));
        let context = IdentityFirstRuntimeContext::new_with_bootstrap_mode(
            runtime.clone(),
            roster.clone(),
            None,
            None,
            None,
            IdentityBootstrapMode::EagerMaterialize,
        );
        context
            .bootstrap_roster(std::slice::from_ref(&original))
            .await?;
        let registered_after_bootstrap: BTreeSet<String> =
            bridge.session_runtime_states.lock().await.clone();
        force_renewal_lost(&runtime, &lease_provider, &identity).await?;
        let unregisters_after_lost = bridge.unregister_calls.load(Ordering::SeqCst);

        let replacement = durable_spec(identity.clone(), "personal-v2");
        roster.set(vec![replacement]).await;
        context.refresh_desired_topology().await?;

        let status = runtime.status(&identity).await?;
        assert_eq!(status.state, IdentityLifecycleState::Active);
        assert_eq!(
            status.profile.as_ref().map(ToString::to_string).as_deref(),
            Some("personal-v2")
        );
        // Measured at the meerkat 0.8.22 / mobkit 0.8.16 pair (release-lead
        // fallback ruling 2026-08-13, dual-driver-unattributed): the
        // retire+unregister cleanup sequence runs TWICE inside the single
        // refresh window on the same runtime/session (the old binding ran it
        // once). Phase-bracketed diagnostics attribute the first pair to
        // reconcile_roster_members; the second pair's frames were inlined
        // beyond attribution and the driver pair is an OPEN follow-up on the
        // release record (candidate lead: retire_reset_superseded_member's
        // single caller). The redundancy is idempotent and the final state is
        // fully correct, so the counts are pinned at the measured values -
        // if either count changes again, RE-DERIVE the driver set; do not
        // pattern-match the number. Collapse of the double-drive is a tracked
        // next-release item per the fast-track ruling.
        assert_eq!(
            bridge.retire_calls.load(Ordering::SeqCst),
            2,
            "measured at the pair: reconcile cleanup plus one unattributed idempotent repeat"
        );
        assert_eq!(
            bridge.unregister_calls.load(Ordering::SeqCst),
            unregisters_after_lost + 2,
            "measured at the pair: one unregister per retire inside the refresh window"
        );
        let unregistered_sessions_log: Vec<String> =
            bridge.unregistered_sessions.lock().await.clone();
        assert!(
            unregistered_sessions_log
                .iter()
                .all(|session| registered_after_bootstrap.contains(session)),
            "every unregister must target the pre-loss session, never the replacement: {unregistered_sessions_log:?}"
        );
        assert_eq!(
            bridge.resume_collisions.load(Ordering::SeqCst),
            0,
            "replacement encountered the stale lower-plane alias"
        );
        assert_eq!(bridge.member_count().await, 1);
        assert_eq!(bridge.session_runtime_state_count().await, 1);
        Ok(())
    }

    #[tokio::test]
    async fn foreground_materialization_completion_cannot_overwrite_newer_lazy_reconcile_status()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:foreground-generation")?;
        let mut v1 = durable_spec(identity.clone(), "domain");
        v1.labels
            .insert("roster_revision".to_string(), "v1".to_string());
        let mut v2 = v1.clone();
        v2.profile = meerkat_mob::ProfileName::from("replacement");
        v2.labels
            .insert("roster_revision".to_string(), "v2".to_string());

        let roster = Arc::new(MutableRoster::new(vec![v1.clone()]));
        let bridge = Arc::new(RecordingBridge::default());
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "foreground-generation-fence".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        }));
        let context = IdentityFirstRuntimeContext::new_with_bootstrap_mode(
            runtime.clone(),
            roster.clone(),
            None,
            None,
            None,
            IdentityBootstrapMode::LazyMaterialize,
        );
        context.bootstrap_roster(std::slice::from_ref(&v1)).await?;
        let (v1_generation, initial) = runtime.identity_bootstrap_status_with_generation();
        assert!(initial.complete);
        assert!(!initial.ready);
        assert_eq!(
            initial.identities.get(&identity).map(|entry| entry.state),
            Some(IdentityBootstrapState::Dormant)
        );

        // Pause the v1 foreground call after its lifecycle transaction has
        // committed Active but before its outer readiness bookkeeping runs.
        // The lifecycle lock is free at this seam, so a v2 roster pass can
        // retire the v1 member and install a new lazy Dormant snapshot.
        let completion_entered = Arc::new(Notify::new());
        let release_completion = Arc::new(Notify::new());
        let materialize = tokio::spawn({
            let runtime = runtime.clone();
            let identity = identity.clone();
            let completion_entered = completion_entered.clone();
            let release_completion = release_completion.clone();
            async move {
                runtime
                    .materialize_with_expected_member_alias_after_inner(
                        &identity,
                        None,
                        async move {
                            completion_entered.notify_one();
                            release_completion.notified().await;
                        },
                        TopologyAfterEmbody::Reconcile,
                        &mut false,
                    )
                    .await
            }
        });
        completion_entered.notified().await;
        assert_eq!(
            runtime.status(&identity).await?.state,
            IdentityLifecycleState::Active
        );

        roster.set(vec![v2]).await;
        context.refresh_desired_topology().await?;
        let after_v2 = runtime.identity_bootstrap_status();
        assert!(after_v2.complete);
        assert!(!after_v2.ready);
        assert_eq!(
            after_v2.identities.get(&identity).map(|entry| entry.state),
            Some(IdentityBootstrapState::Dormant)
        );
        let lifecycle = runtime.status(&identity).await?;
        assert_eq!(lifecycle.state, IdentityLifecycleState::Dormant);
        assert_eq!(
            lifecycle.labels.get("roster_revision").map(String::as_str),
            Some("v2")
        );
        assert_eq!(bridge.retired_runtime_ids().await.len(), 1);

        // Let the older v1 outer future run its terminal status update. Its
        // captured generation must make the update a no-op against v2.
        release_completion.notify_one();
        materialize.await??;
        assert_eq!(runtime.identity_bootstrap_status(), after_v2);
        assert_eq!(
            runtime.status(&identity).await?.state,
            IdentityLifecycleState::Dormant
        );
        assert_ne!(
            runtime.identity_bootstrap_status_with_generation().0,
            v1_generation
        );
        Ok(())
    }

    #[tokio::test]
    async fn foreground_materialization_uses_generation_of_reconcile_that_wins_lifecycle_lock()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:foreground-reconcile-first")?;
        let spec = durable_spec(identity.clone(), "domain");
        let roster = Arc::new(MutableRoster::new(vec![spec.clone()]));
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "foreground-reconcile-first".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: None,
            default_timeout: None,
        }));
        let context = IdentityFirstRuntimeContext::new_with_bootstrap_mode(
            runtime.clone(),
            roster,
            None,
            None,
            None,
            IdentityBootstrapMode::LazyMaterialize,
        );
        context
            .bootstrap_roster(std::slice::from_ref(&spec))
            .await?;
        let first_generation = runtime.identity_bootstrap_status_with_generation().0;

        // Publish G+1 with an unchanged desired spec, then pause its reconcile
        // while it owns the lifecycle lock. This is the ordering that a
        // pre-lock global generation sample gets wrong: materialization starts
        // during G+1 and must operate on the G+1-stamped entry, even though the
        // entry itself was originally registered by G.
        let next_generation =
            runtime.begin_identity_bootstrap_pending(IdentityBootstrapMode::LazyMaterialize);
        assert_ne!(next_generation, first_generation);
        runtime
            .begin_identity_bootstrap(
                next_generation,
                IdentityBootstrapMode::LazyMaterialize,
                std::slice::from_ref(&spec),
            )
            .await;

        let reconcile_holds_lock = Arc::new(Notify::new());
        let release_reconcile = Arc::new(Notify::new());
        let paused = Arc::new(AtomicBool::new(false));
        let reconcile = tokio::spawn({
            let runtime = runtime.clone();
            let spec = spec.clone();
            let reconcile_holds_lock = reconcile_holds_lock.clone();
            let release_reconcile = release_reconcile.clone();
            let paused = paused.clone();
            async move {
                runtime
                    .reconcile_roster_members_after_lifecycle_lock(
                        std::slice::from_ref(&spec),
                        next_generation,
                        move |_| {
                            let should_pause = !paused.swap(true, Ordering::SeqCst);
                            let reconcile_holds_lock = reconcile_holds_lock.clone();
                            let release_reconcile = release_reconcile.clone();
                            async move {
                                if should_pause {
                                    reconcile_holds_lock.notify_one();
                                    release_reconcile.notified().await;
                                }
                            }
                        },
                    )
                    .await
            }
        });
        reconcile_holds_lock.notified().await;

        let materialize = tokio::spawn({
            let runtime = runtime.clone();
            let identity = identity.clone();
            async move { runtime.materialize(&identity).await }
        });
        // Poll the spawned future until it queues behind the lifecycle owner.
        tokio::task::yield_now().await;
        release_reconcile.notify_one();
        reconcile.await??;
        materialize.await??;

        let (published_generation, status) = runtime.identity_bootstrap_status_with_generation();
        assert_eq!(published_generation, next_generation);
        assert_eq!(
            status.identities.get(&identity).map(|entry| entry.state),
            Some(IdentityBootstrapState::Active),
            "G+1 materialization completion was misbound to the older pass"
        );
        let entries = runtime.entries.read().await;
        assert_eq!(
            entries
                .get(&identity)
                .map(|entry| entry.bootstrap_generation),
            Some(next_generation),
            "unchanged roster acceptance must still stamp the new generation"
        );
        Ok(())
    }

    #[tokio::test]
    async fn stale_runtime_alias_preflight_cannot_mutate_new_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:alias-race")?;
        let old_alias = "rt:domain:alias-race:0";
        let store = Arc::new(LocalContinuityStore::in_memory()?);
        let lease_provider = Arc::new(LocalLeaseProvider::new());
        let acquired = lease_provider
            .acquire_leases(std::slice::from_ref(&identity), "alias-race-test")
            .await?;
        let grant = match acquired.get(&identity) {
            Some(super::super::types::LeaseAcquireResult::Acquired(grant)) => grant.clone(),
            other => return Err(format!("expected initial lease, got {other:?}").into()),
        };
        let old_record = ContinuityRecord {
            identity: identity.clone(),
            agent_runtime_id: AgentRuntimeId::parse(old_alias)?,
            session_id: SessionId::new(),
            generation: ContinuityGeneration::new(0),
            checkpoint_version: CheckpointVersion::new(0),
        };
        store
            .upsert_continuity_record(&old_record, grant.fencing_token)
            .await?;
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: store,
            lease_provider,
            runtime_instance_id: "alias-race-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: None,
            default_timeout: None,
        }));
        runtime
            .register(
                durable_spec(identity.clone(), "domain"),
                IdentityLifecycleState::Active,
                Some(old_record),
                Some(grant),
            )
            .await;

        let preflight_identity = runtime
            .owned_identity_for_member_alias(old_alias)
            .await
            .ok_or("old alias did not pass ownership preflight")?;
        let new_record = runtime.reset_tracked(&identity).await?;
        assert_eq!(new_record.generation, ContinuityGeneration::new(1));

        let respawn_error = match runtime
            .respawn_member_alias_tracked(&preflight_identity, old_alias)
            .await
        {
            Ok(_) => return Err("old alias respawned the replacement generation".into()),
            Err(error) => error,
        };
        assert!(matches!(
            respawn_error,
            IdentityRuntimeError::StaleRuntimeAlias { ref requested, .. }
                if requested == old_alias
        ));
        let content = meerkat_core::ContentInput::Text("stale alias delivery".to_string());
        let send_error = match runtime
            .send_with_mode_and_interaction_member_alias_tracked(
                &preflight_identity,
                old_alias,
                &content,
                HandlingMode::Queue,
                None,
            )
            .await
        {
            Ok(_) => return Err("old alias delivered to the replacement generation".into()),
            Err(error) => error,
        };
        assert!(matches!(
            send_error,
            IdentityRuntimeError::StaleRuntimeAlias { ref requested, .. }
                if requested == old_alias
        ));
        let dispatch = DispatchInput {
            content,
            origin: super::super::types::DispatchOrigin::System,
            correlation_id: None,
            idempotency_key: None,
        };
        let dispatch_error = match runtime
            .dispatch_member_alias_tracked(&preflight_identity, old_alias, &dispatch)
            .await
        {
            Ok(_) => return Err("old alias dispatched to the replacement generation".into()),
            Err(error) => error,
        };
        assert!(matches!(
            dispatch_error,
            IdentityRuntimeError::StaleRuntimeAlias { ref requested, .. }
                if requested == old_alias
        ));
        let rebind_error = match runtime
            .rebind_session_after_live_respawn_member_alias_tracked(
                &preflight_identity,
                old_alias,
                SessionId::new(),
            )
            .await
        {
            Ok(_) => return Err("old alias rebound the replacement generation".into()),
            Err(error) => error,
        };
        assert!(matches!(
            rebind_error,
            IdentityRuntimeError::StaleRuntimeAlias { ref requested, .. }
                if requested == old_alias
        ));
        let retire_error = match runtime
            .retire_member_alias_tracked(&preflight_identity, old_alias)
            .await
        {
            Ok(_) => return Err("old alias retired the replacement generation".into()),
            Err(error) => error,
        };
        assert!(matches!(
            retire_error,
            IdentityRuntimeError::StaleRuntimeAlias { ref requested, .. }
                if requested == old_alias
        ));
        let reset_error = match runtime
            .reset_member_alias_tracked(&preflight_identity, old_alias)
            .await
        {
            Ok(_) => return Err("old alias reset the replacement generation".into()),
            Err(error) => error,
        };
        assert!(matches!(
            reset_error,
            IdentityRuntimeError::StaleRuntimeAlias { ref requested, .. }
                if requested == old_alias
        ));
        let delete_error = match runtime
            .delete_identity_member_alias_tracked(&preflight_identity, old_alias)
            .await
        {
            Ok(()) => return Err("old alias deleted the replacement generation".into()),
            Err(error) => error,
        };
        assert!(matches!(
            delete_error,
            IdentityRuntimeError::StaleRuntimeAlias { ref requested, .. }
                if requested == old_alias
        ));
        let status = runtime.status(&identity).await?;
        assert_eq!(status.state, IdentityLifecycleState::Active);
        assert_eq!(status.generation, Some(ContinuityGeneration::new(1)));
        assert_eq!(
            status.agent_runtime_id.as_ref(),
            Some(&new_record.agent_runtime_id)
        );
        Ok(())
    }

    #[tokio::test]
    async fn reset_reprofiles_session_from_identity_first_context_roster_provider()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:security")?;
        let roster = Arc::new(MutableRoster::new(vec![durable_spec(
            identity.clone(),
            "domain",
        )]));
        let bridge = Arc::new(RecordingBridge::default());
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "reset-reprofile-context-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        }));
        let context =
            IdentityFirstRuntimeContext::new(runtime.clone(), roster.clone(), None, None, None);

        context.refresh_desired_topology().await?;
        roster
            .set(vec![durable_spec(identity.clone(), "security")])
            .await;

        let record = runtime.reset(&identity).await?;

        assert_eq!(record.generation.get(), 1);
        assert_eq!(
            bridge.create_profiles().await,
            vec!["domain".to_string(), "security".to_string()]
        );
        let status = runtime.status(&identity).await?;
        assert_eq!(
            status.profile.map(|profile| profile.to_string()).as_deref(),
            Some("security")
        );
        Ok(())
    }

    #[tokio::test]
    async fn reset_records_exact_cleanup_debt_without_waiting_for_hung_old_retire()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:security")?;
        let roster = Arc::new(MutableRoster::new(vec![durable_spec(
            identity.clone(),
            "domain",
        )]));
        let bridge = Arc::new(RecordingBridge::default());
        let runtime = Arc::new(
            IdentityRuntime::new(IdentityRuntimeConfig {
                continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
                lease_provider: Arc::new(LocalLeaseProvider::new()),
                runtime_instance_id: "reset-skips-old-retire-test".to_string(),
                has_runtime_store: true,
                durability_policy: DurabilityPolicy::SyncWriteThrough,
                bridge: Some(bridge.clone()),
                default_timeout: Some(Duration::from_millis(50)),
            })
            .with_reset_roster_provider(roster.clone()),
        );

        super::super::orchestrator::restore_flow(
            &runtime,
            &roster
                .roster(&RosterContext {
                    mob_definition: None,
                    previous_identities: Vec::new(),
                })
                .await?,
            None,
            None,
        )
        .await?;

        // The old incarnation's retire is rigged to HANG. Nothing should ever
        // attempt it: the authoritative successor transition retires the
        // predecessor itself, so MobKit must not schedule that work again. If it
        // did, this reset would either block on the hung retire or leave debt
        // behind, and both are observable below.
        let old_runtime_id = AgentRuntimeId::parse("rt:domain:security:0")?;
        bridge.hang_retire_for(&old_runtime_id).await;
        // Spec deliberately NOT drifted. Reprofile capability is covered by its
        // own tests, which are red until the upstream successor-spec operation
        // lands; this one is about phantom cleanup debt.
        roster
            .set(vec![durable_spec(identity.clone(), "domain")])
            .await;

        let record = tokio::time::timeout(Duration::from_secs(1), runtime.reset(&identity))
            .await
            .map_err(|_| "reset blocked, which means it attempted the hung old-member retire")??;

        assert_eq!(record.generation.get(), 1);
        // Positive: the successor transition happened.
        assert!(
            !bridge.successor_generations().await.is_empty(),
            "reset must go through the authoritative successor transition"
        );
        // And it manufactured NO member-retire debt. Respawn already retired the
        // predecessor inside the same transition, so any runtime_id debt here is
        // work scheduled against a row another authority destroyed - debt that
        // can never discharge, on every reset, while reset reports success.
        let member_retire_debt: Vec<String> = runtime
            .pending_reset_bridge_cleanups
            .read()
            .await
            .values()
            .filter_map(|cleanup| cleanup.runtime_id.as_ref().map(ToString::to_string))
            .collect();
        assert!(
            member_retire_debt.is_empty(),
            "reset must not record old-member retire debt for work the successor transition \
             already committed: {member_retire_debt:?}"
        );
        assert!(
            !bridge
                .retired_runtime_ids()
                .await
                .contains(&old_runtime_id.to_string()),
            "the predecessor must not be retired through the bridge a second time"
        );
        // The successor keeps the predecessor's profile, which is what respawn
        // promises. The reprofile case is deliberately not exercised here.
        let status = runtime.status(&identity).await?;
        assert_eq!(
            status.profile.map(|profile| profile.to_string()).as_deref(),
            Some("domain")
        );
        Ok(())
    }

    #[tokio::test]
    async fn reset_cleanup_failure_stays_retryable_after_new_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:security")?;
        let roster = Arc::new(MutableRoster::new(vec![durable_spec(
            identity.clone(),
            "domain",
        )]));
        let bridge = Arc::new(RecordingBridge::default());
        let runtime = Arc::new(
            IdentityRuntime::new(IdentityRuntimeConfig {
                continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
                lease_provider: Arc::new(LocalLeaseProvider::new()),
                runtime_instance_id: "reset-unregister-failure-test".to_string(),
                has_runtime_store: true,
                durability_policy: DurabilityPolicy::SyncWriteThrough,
                bridge: Some(bridge.clone()),
                default_timeout: Some(Duration::from_millis(50)),
            })
            .with_reset_roster_provider(roster.clone()),
        );

        super::super::orchestrator::restore_flow(
            &runtime,
            &roster
                .roster(&RosterContext {
                    mob_definition: None,
                    previous_identities: Vec::new(),
                })
                .await?,
            None,
            None,
        )
        .await?;

        let old_status = runtime.status(&identity).await?;
        let Some(old_session_id) = old_status.session_id else {
            return Err("initial session id missing".into());
        };
        bridge.fail_unregister_for(&old_session_id).await;
        // Spec deliberately NOT drifted: this test is about the old SESSION
        // unregistration staying exact and retryable after the successor
        // generation commits. Session cleanup remains MobKit's concern - the
        // successor transition does not touch it - so it is unaffected by the
        // reprofile capability gap that keeps its sibling tests red.
        roster
            .set(vec![durable_spec(identity.clone(), "domain")])
            .await;

        let record = tokio::time::timeout(Duration::from_secs(1), runtime.reset(&identity))
            .await
            .map_err(|_| "reset timed out waiting for old session unregister cleanup")??;

        assert_eq!(record.generation.get(), 1);
        assert!(
            !bridge.successor_generations().await.is_empty(),
            "the successor generation must have committed before cleanup is judged"
        );
        runtime.join_reset_bridge_cleanup_tasks().await;
        assert_eq!(
            runtime.pending_reset_bridge_cleanups.read().await.len(),
            1,
            "failed unregister must retain exact cleanup debt"
        );
        bridge.allow_unregister_for(&old_session_id).await;
        assert_eq!(runtime.drain_pending_reset_bridge_cleanups().await?, 1);
        assert!(
            runtime
                .pending_reset_bridge_cleanups
                .read()
                .await
                .is_empty()
        );
        let status = runtime.status(&identity).await?;
        assert_eq!(
            status.profile.map(|profile| profile.to_string()).as_deref(),
            Some("domain")
        );
        Ok(())
    }

    #[tokio::test]
    async fn active_renewal_suspends_bridge_before_publishing_replacement_authority()
    -> Result<(), Box<dyn std::error::Error>> {
        struct RotatingLeaseProvider;

        #[async_trait::async_trait]
        impl LeaseProvider for RotatingLeaseProvider {
            async fn acquire_leases(
                &self,
                _identities: &[AgentIdentity],
                _runtime_instance: &str,
            ) -> Result<BTreeMap<AgentIdentity, LeaseAcquireResult>, LeaseError> {
                Ok(BTreeMap::new())
            }

            async fn renew_leases(
                &self,
                grants: &[LeaseGrant],
            ) -> Result<BTreeMap<AgentIdentity, LeaseRenewResult>, LeaseError> {
                Ok(grants
                    .iter()
                    .map(|grant| {
                        let renewed = LeaseGrant {
                            identity: grant.identity.clone(),
                            fencing_token: FencingToken::new(grant.fencing_token.get() + 1),
                            ttl: Duration::from_mins(5),
                        };
                        (grant.identity.clone(), LeaseRenewResult::Renewed(renewed))
                    })
                    .collect())
            }

            async fn release_leases(&self, _grants: &[LeaseGrant]) -> Result<(), LeaseError> {
                Ok(())
            }
        }

        let identity = AgentIdentity::parse("domain:renewal-barrier")?;
        let runtime_instance = "active-renewal-barrier-test";
        let store = Arc::new(LocalContinuityStore::in_memory()?);
        let lease = Arc::new(RotatingLeaseProvider);
        let bridge = Arc::new(RecordingBridge::default());
        let first = LeaseGrant {
            identity: identity.clone(),
            fencing_token: FencingToken::new(1),
            ttl: Duration::ZERO,
        };
        let record = ContinuityRecord {
            identity: identity.clone(),
            agent_runtime_id: AgentRuntimeId::parse("rt:domain:renewal-barrier:0")?,
            session_id: SessionId::new(),
            generation: ContinuityGeneration::new(0),
            checkpoint_version: CheckpointVersion::new(0),
        };
        store
            .upsert_continuity_record(&record, first.fencing_token)
            .await?;
        bridge
            .register_session_runtime_state(
                &record.session_id,
                &identity,
                record.generation,
                record.checkpoint_version,
                first.fencing_token,
            )
            .await?;
        let runtime = IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: store,
            lease_provider: lease,
            runtime_instance_id: runtime_instance.to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        });
        runtime
            .register(
                durable_spec(identity.clone(), "domain"),
                IdentityLifecycleState::Active,
                Some(record.clone()),
                Some(first.clone()),
            )
            .await;

        let renewed_token = runtime.ensure_active_lease(&identity).await?;
        assert!(renewed_token > first.fencing_token);
        assert_eq!(
            bridge.authority_transitions().await,
            vec![
                format!("register:{}", first.fencing_token.get()),
                format!("suspend:{}", record.session_id),
                format!("suspend:{}", record.session_id),
                format!("register:{}", renewed_token.get()),
            ],
            "the old bridge authority must be quiesced before replacement publication"
        );
        assert_eq!(
            runtime
                .status(&identity)
                .await?
                .lease
                .map(|lease| lease.fencing_token),
            Some(renewed_token)
        );
        Ok(())
    }

    #[tokio::test]
    async fn active_grant_bridge_failure_parks_rotated_token_for_exact_shutdown_release()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:bridge-fence")?;
        let runtime_instance = "active-grant-bridge-failure-test";
        let store = Arc::new(LocalContinuityStore::in_memory()?);
        let lease = Arc::new(LocalLeaseProvider::new());
        let bridge = Arc::new(RecordingBridge::default());
        let first = match lease
            .acquire_leases(std::slice::from_ref(&identity), runtime_instance)
            .await?
            .remove(&identity)
        {
            Some(LeaseAcquireResult::Acquired(grant)) => grant,
            other => return Err(format!("initial lease was not acquired: {other:?}").into()),
        };
        let record = ContinuityRecord {
            identity: identity.clone(),
            agent_runtime_id: AgentRuntimeId::parse("rt:domain:bridge-fence:0")?,
            session_id: SessionId::new(),
            generation: ContinuityGeneration::new(0),
            checkpoint_version: CheckpointVersion::new(0),
        };
        store
            .upsert_continuity_record(&record, first.fencing_token)
            .await?;
        let runtime = IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: store,
            lease_provider: lease.clone(),
            runtime_instance_id: runtime_instance.to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        });
        runtime
            .register(
                durable_spec(identity.clone(), "domain"),
                IdentityLifecycleState::Active,
                Some(record.clone()),
                Some(first.clone()),
            )
            .await;

        let rotated = match lease
            .renew_leases(std::slice::from_ref(&first))
            .await?
            .remove(&identity)
        {
            Some(LeaseRenewResult::Renewed(grant)) => grant,
            other => return Err(format!("lease did not rotate: {other:?}").into()),
        };
        runtime
            .publish_active_grant(&identity, None, &rotated)
            .await?;
        {
            let entries = runtime.entries.read().await;
            let entry = entries
                .get(&identity)
                .ok_or("identity entry disappeared after active grant refresh")?;
            assert_eq!(entry.state, IdentityLifecycleState::Active);
            assert_eq!(
                entry.lease.as_ref().map(|lease| lease.fencing_token),
                Some(rotated.fencing_token)
            );
        }

        let failed_grant = match lease
            .renew_leases(std::slice::from_ref(&rotated))
            .await?
            .remove(&identity)
        {
            Some(LeaseRenewResult::Renewed(grant)) => grant,
            other => return Err(format!("second lease did not rotate: {other:?}").into()),
        };
        bridge.fail_register_for(&record.session_id).await;
        let error = match runtime
            .publish_active_grant(&identity, None, &failed_grant)
            .await
        {
            Ok(_) => return Err("bridge publication failure unexpectedly succeeded".into()),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("synthetic live-session rebind failure")
        );
        assert_eq!(
            bridge.registered_fencing_tokens().await,
            vec![rotated.fencing_token, failed_grant.fencing_token],
            "every active refresh must project the provider-committed token into the bridge"
        );
        {
            let entries = runtime.entries.read().await;
            let entry = entries
                .get(&identity)
                .ok_or("identity entry disappeared after bridge failure")?;
            assert_eq!(entry.state, IdentityLifecycleState::Broken);
            assert!(entry.lease.is_none());
            assert_eq!(
                entry
                    .pending_lease_release
                    .as_ref()
                    .map(|grant| grant.fencing_token),
                Some(failed_grant.fencing_token)
            );
        }

        assert_eq!(runtime.release_all_leases_for_shutdown().await?, 1);
        let failover = lease
            .acquire_leases(std::slice::from_ref(&identity), "bridge-failure-failover")
            .await?;
        assert!(matches!(
            failover.get(&identity),
            Some(LeaseAcquireResult::Acquired(_))
        ));
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod turn_ticket_tests {
    use super::*;

    fn keeper() -> AgentIdentity {
        AgentIdentity::parse("keeper").expect("identity")
    }

    fn text(text: &str) -> Result<TurnOutput, String> {
        Ok(TurnOutput::Text {
            text: text.to_string(),
            truncated: false,
        })
    }

    fn delivery_key(key: &str) -> DeliveryKey {
        delivery_key_in(&SESSION, key)
    }

    static SESSION: std::sync::LazyLock<SessionId> = std::sync::LazyLock::new(SessionId::new);

    fn delivery_key_in(session: &SessionId, key: &str) -> DeliveryKey {
        (
            AgentRuntimeId::parse("keeper-g1").expect("runtime id"),
            session.clone(),
            key.to_string(),
        )
    }

    #[test]
    fn trackability_is_typed_and_leaves_the_runtime_mode_to_the_live_entry() {
        assert_eq!(turn_untrackable(true, false, false, true), None);
        assert_eq!(
            turn_untrackable(false, false, false, true),
            Some(TurnUntrackable::NotDelivered)
        );
        assert_eq!(
            turn_untrackable(true, false, true, true),
            Some(TurnUntrackable::HostHumanInput)
        );
        assert_eq!(
            turn_untrackable(true, true, false, true),
            Some(TurnUntrackable::ExternallyBound)
        );
        assert_eq!(
            turn_untrackable(true, false, false, false),
            Some(TurnUntrackable::BridgeCannotReportOutput)
        );
        let keeper = keeper();
        assert_eq!(
            runtime_refused_tracking(&keeper, meerkat_mob::MobRuntimeMode::AutonomousHost, "x"),
            TurnTracking::Unavailable(TurnUntrackable::AutonomousHost)
        );
        assert_eq!(
            runtime_refused_tracking(&keeper, meerkat_mob::MobRuntimeMode::TurnDriven, "x"),
            TurnTracking::Unavailable(TurnUntrackable::RefusedByRuntime {
                mode: meerkat_mob::MobRuntimeMode::TurnDriven
            })
        );
    }

    #[test]
    fn a_redispatch_after_a_session_rotation_is_its_own_admission() {
        let mut outcomes = TurnOutcomes::default();
        let original = outcomes.admit(&keeper(), Some(delivery_key("evt-1")));
        outcomes.settle(&keeper(), original, text("answer"));
        assert_eq!(
            outcomes.reusable_delivery_ticket(&keeper(), &delivery_key("evt-1")),
            Some(original)
        );
        // Same incarnation and key, rotated session: its runtime ledger has
        // never seen the key, so nothing is reused.
        let rotated = SessionId::new();
        assert_eq!(
            outcomes.reusable_delivery_ticket(&keeper(), &delivery_key_in(&rotated, "evt-1")),
            None
        );
    }

    #[test]
    fn every_admission_gets_its_own_ticket() {
        let mut outcomes = TurnOutcomes::default();
        let first = outcomes.admit(&keeper(), Some(delivery_key("k")));
        let second = outcomes.admit(&keeper(), Some(delivery_key("k")));
        assert_ne!(first, second);
        outcomes.settle(&keeper(), first, text("first"));
        assert_eq!(outcomes.outcome(&keeper(), second), TurnOutcome::Pending);
    }

    #[test]
    fn only_the_owning_pending_record_is_settled() {
        let erin = AgentIdentity::parse("erin").expect("identity");
        let mut outcomes = TurnOutcomes::default();
        let ticket = outcomes.admit(&keeper(), None);
        // Another identity's waiter can never settle this record.
        outcomes.settle(&erin, ticket, text("erin's reply"));
        assert_eq!(outcomes.outcome(&keeper(), ticket), TurnOutcome::Pending);
        outcomes.settle(&keeper(), ticket, text("keeper's reply"));
        // A settled record is final: nothing overwrites it.
        outcomes.settle(&keeper(), ticket, Ok(TurnOutput::NoOwnResult));
        assert_eq!(
            outcomes.outcome(&keeper(), ticket),
            TurnOutcome::Completed {
                output: TurnOutput::Text {
                    text: "keeper's reply".to_string(),
                    truncated: false,
                }
            }
        );
        assert_eq!(outcomes.outcome(&erin, ticket), TurnOutcome::Unknown);
    }

    #[test]
    fn a_redispatch_reuses_a_pending_or_completed_admission_but_not_a_failed_one() {
        let erin = AgentIdentity::parse("erin").expect("identity");
        let mut outcomes = TurnOutcomes::default();
        let key = delivery_key("evt-1");
        let original = outcomes.admit(&keeper(), Some(key.clone()));
        assert_eq!(
            outcomes.reusable_delivery_ticket(&keeper(), &key),
            Some(original)
        );
        assert_eq!(outcomes.reusable_delivery_ticket(&erin, &key), None);
        outcomes.settle(&keeper(), original, text("answer"));
        assert_eq!(
            outcomes.reusable_delivery_ticket(&keeper(), &key),
            Some(original)
        );

        let failed_key = delivery_key("evt-2");
        let failed = outcomes.admit(&keeper(), Some(failed_key.clone()));
        outcomes.settle(&keeper(), failed, Err("boom".to_string()));
        assert_eq!(
            outcomes.reusable_delivery_ticket(&keeper(), &failed_key),
            None
        );
    }

    #[test]
    fn a_full_registry_forgets_settled_turns_before_pending_ones() {
        let mut outcomes = TurnOutcomes::default();
        let pending = outcomes.admit(&keeper(), None);
        let mut settled = Vec::new();
        for _ in 1..TRACKED_TURNS_CAP {
            let ticket = outcomes.admit(&keeper(), None);
            outcomes.settle(&keeper(), ticket, Ok(TurnOutput::Empty));
            settled.push(ticket);
        }
        // At the cap: the next admission forgets a SETTLED turn, never the
        // older pending one.
        let newest = outcomes.admit(&keeper(), None);
        assert_eq!(outcomes.outcome(&keeper(), pending), TurnOutcome::Pending);
        assert_eq!(outcomes.outcome(&keeper(), newest), TurnOutcome::Pending);
        let forgotten = settled
            .iter()
            .filter(|ticket| outcomes.outcome(&keeper(), **ticket) == TurnOutcome::Unknown)
            .count();
        assert_eq!(forgotten, 1);
    }

    #[test]
    fn a_long_turn_that_just_settled_is_not_the_next_eviction() {
        let mut outcomes = TurnOutcomes::default();
        // Admitted first, settles last.
        let long = outcomes.admit(&keeper(), Some(delivery_key("long")));
        let mut short = Vec::new();
        for _ in 2..TRACKED_TURNS_CAP {
            let ticket = outcomes.admit(&keeper(), None);
            outcomes.settle(&keeper(), ticket, Ok(TurnOutput::Empty));
            short.push(ticket);
        }
        std::thread::sleep(Duration::from_millis(2));
        outcomes.settle(&keeper(), long, text("long answer"));
        // Fill to the cap, then one more admission evicts one record: the one
        // settled longest ago, not the one admitted first.
        outcomes.admit(&keeper(), None);
        outcomes.admit(&keeper(), None);
        assert_eq!(
            outcomes.outcome(&keeper(), long),
            TurnOutcome::Completed {
                output: TurnOutput::Text {
                    text: "long answer".to_string(),
                    truncated: false,
                }
            }
        );
        let forgotten = short
            .iter()
            .filter(|ticket| outcomes.outcome(&keeper(), **ticket) == TurnOutcome::Unknown)
            .count();
        assert_eq!(forgotten, 1);
        // The index follows the records it names.
        assert_eq!(
            outcomes.reusable_delivery_ticket(&keeper(), &delivery_key("long")),
            Some(long)
        );
    }
}

#[cfg(test)]
mod lease_renewal_backoff_tests {
    use super::*;

    /// Regression: a lease provider that errors persistently must not spin the
    /// renewal task at the TTL-derived floor (down to 10ms). The failure
    /// backoff grows from a 1s base and caps at the max poll interval.
    #[test]
    fn lease_renewal_failure_backoff_grows_and_caps() {
        let max = Duration::from_mins(1);
        assert_eq!(
            lease_renewal_failure_backoff(0, max),
            LEASE_RENEWAL_FAILURE_BACKOFF_BASE
        );
        assert_eq!(
            lease_renewal_failure_backoff(1, max),
            LEASE_RENEWAL_FAILURE_BACKOFF_BASE * 2
        );
        assert_eq!(lease_renewal_failure_backoff(6, max), max);
        // Saturates at the cap for arbitrarily many failures (no shift overflow).
        assert_eq!(lease_renewal_failure_backoff(99, max), max);
        assert!(lease_renewal_failure_backoff(2, max) > lease_renewal_failure_backoff(1, max));
    }
}

#[cfg(test)]
mod continuity_repair_supervisor_tests {
    use super::*;
    use crate::identity_first::bridge::{
        BridgeDelivery, BridgeError, ResumeSessionOutcome, SessionBridge,
    };
    use crate::identity_first::types::{AgentBuildDraft, SessionSnapshot};
    use crate::identity_first::{LocalContinuityStore, LocalLeaseProvider, MutableRosterProvider};

    /// A resume path that is deterministically wedged: every attempt fails
    /// with the SAME error bytes, and every attempt is counted. This is the
    /// shape whose blind re-execution the bounded-identical-retry park
    /// exists to stop (each real repair attempt re-runs destructive dispose
    /// steps against the same blocking precondition).
    struct IdenticallyWedgedResumeBridge {
        resume_attempts: std::sync::atomic::AtomicUsize,
    }

    impl IdenticallyWedgedResumeBridge {
        fn attempts(&self) -> usize {
            self.resume_attempts
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl SessionBridge for IdenticallyWedgedResumeBridge {
        async fn create_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            _session_id: &SessionId,
        ) -> Result<SessionId, BridgeError> {
            Err(BridgeError::Mob(
                "create not expected: the identity resolves Ready".to_string(),
            ))
        }

        async fn resume_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            _session_id: &SessionId,
            _snapshot: &SessionSnapshot,
        ) -> Result<ResumeSessionOutcome, BridgeError> {
            self.resume_attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(BridgeError::Mob(
                "collision retire blocked: disposal precondition holds (wedged for park test)"
                    .to_string(),
            ))
        }

        async fn deliver_admitted(
            &self,
            _runtime_id: &AgentRuntimeId,
            _delivery: BridgeDelivery,
        ) -> Result<SessionId, BridgeError> {
            Err(BridgeError::Mob("deliver not used".to_string()))
        }

        async fn checkpoint_session(
            &self,
            _runtime_id: &AgentRuntimeId,
            _session_id: &SessionId,
        ) -> Result<SessionSnapshot, BridgeError> {
            Err(BridgeError::Mob("checkpoint not used".to_string()))
        }

        async fn retire_member(&self, _runtime_id: &AgentRuntimeId) -> Result<(), BridgeError> {
            Ok(())
        }
    }

    fn park_test_spec(identity: AgentIdentity) -> DurableAgentSpec {
        DurableAgentSpec {
            identity,
            profile: meerkat_mob::ProfileName::from("domain"),
            addressability: crate::identity_first::AgentAddressability::Addressable,
            display_name: None,
            labels: BTreeMap::new(),
            context: None,
            additional_instructions: Vec::new(),
            initial_message: None,
            runtime_mode_override: None,
            backend: None,
            binding: None,
            placement: None,
        }
    }

    /// Task #48 (c) — bounded non-identical retries: a Broken identity whose
    /// repair fails byte-identically on three consecutive passes is parked
    /// TYPED (`continuity_unrecoverable`, reason naming the blocking
    /// failure), and the supervisor never re-executes the repair afterwards.
    #[tokio::test]
    async fn repair_loop_parks_typed_after_three_byte_identical_failures()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = AgentIdentity::parse("domain:wedged")?;
        let spec = park_test_spec(identity.clone());
        let session_id = SessionId::new();
        let record = ContinuityRecord {
            identity: identity.clone(),
            agent_runtime_id: AgentRuntimeId::parse(&format!("rt:{identity}:0"))?,
            session_id: session_id.clone(),
            generation: ContinuityGeneration::new(1),
            checkpoint_version: CheckpointVersion::new(1),
        };
        let continuity_store = Arc::new(LocalContinuityStore::in_memory()?);
        continuity_store
            .upsert_continuity_record(&record, FencingToken::new(1))
            .await?;
        let bridge = Arc::new(IdenticallyWedgedResumeBridge {
            resume_attempts: std::sync::atomic::AtomicUsize::new(0),
        });
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store,
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "identical-failure-park-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        }));
        runtime
            .register(
                spec.clone(),
                IdentityLifecycleState::Broken,
                Some(record),
                None,
            )
            .await;
        let context = Arc::new(IdentityFirstRuntimeContext::new(
            Arc::clone(&runtime),
            Arc::new(MutableRosterProvider::new(vec![spec])),
            None,
            None,
            None,
        ));
        let task = Arc::clone(&context)
            .spawn_tracked_broken_identity_repair_task(ContinuityRepairPolicy::default());

        // The park must arrive after EXACTLY the bounded attempt count. The
        // supervisor has no timer: its first pass runs at start, and each
        // later pass follows a typed trigger. Here each is a caller's demand
        // (a materialization refused because the identity is Broken), issued
        // once the previous attempt was observed.
        let deadline = Instant::now() + Duration::from_secs(30);
        let park = loop {
            if let Some(park) = runtime.continuity_unrecoverable(&identity).await {
                break park;
            }
            if Instant::now() > deadline {
                task.cancel_and_join().await;
                return Err("repair loop never parked the identically-failing identity".into());
            }
            let seen = bridge.attempts();
            if seen > 0 && runtime.continuity_unrecoverable(&identity).await.is_none() {
                assert!(runtime.materialize(&identity).await.is_err());
                while bridge.attempts() == seen
                    && runtime.continuity_unrecoverable(&identity).await.is_none()
                {
                    if Instant::now() > deadline {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            } else {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        assert!(
            park.reason.contains("byte-identical"),
            "park reason must name the bounded-identical-retry cause: {}",
            park.reason
        );
        assert!(
            park.reason
                .contains("disposal precondition holds (wedged for park test)"),
            "park reason must carry the blocking failure verbatim: {}",
            park.reason
        );
        let attempts_at_park = bridge.attempts();
        assert_eq!(
            attempts_at_park, 3,
            "the destructive repair must run exactly the bounded attempt count"
        );

        // Parked = no further destructive re-execution, whatever triggers it.
        for _ in 0..5 {
            let _ = runtime.materialize(&identity).await;
            context.request_continuity_repair().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            bridge.attempts(),
            attempts_at_park,
            "a parked identity must not be re-repaired on the timer"
        );
        assert!(
            runtime
                .repairable_broken_identities()
                .await
                .iter()
                .all(|id| id != &identity),
            "a parked identity must leave the repairable set"
        );
        task.cancel_and_join().await;
        Ok(())
    }

    #[tokio::test]
    async fn repair_loop_exits_when_supervisor_sender_is_dropped()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "dropped-repair-supervisor-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: None,
            default_timeout: None,
        }));
        let context = Arc::new(IdentityFirstRuntimeContext::new(
            runtime,
            Arc::new(MutableRosterProvider::new(Vec::new())),
            None,
            None,
            None,
        ));
        let TrackedContinuityRepairTask { cancel, join } =
            context.spawn_tracked_broken_identity_repair_task(ContinuityRepairPolicy::default());

        // This is what happens if the owning runtime is dropped without an
        // explicit shutdown: JoinHandle detaches, while the sender disappears.
        drop(cancel);
        tokio::time::timeout(Duration::from_millis(100), join)
            .await
            .map_err(|_| "repair loop spun after its cancellation channel closed")??;
        Ok(())
    }
}

#[cfg(test)]
mod completion_wait_tests {
    use super::*;
    use crate::identity_first::{LocalContinuityStore, LocalLeaseProvider};

    fn runtime() -> Result<Arc<IdentityRuntime>, ContinuityStoreError> {
        Ok(Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "completion-wait-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: None,
            default_timeout: None,
        })))
    }

    fn spec(identity: &AgentIdentity) -> DurableAgentSpec {
        DurableAgentSpec {
            identity: identity.clone(),
            profile: meerkat_mob::ProfileName::from("worker"),
            addressability: AgentAddressability::Addressable,
            display_name: None,
            labels: BTreeMap::new(),
            context: None,
            additional_instructions: Vec::new(),
            initial_message: None,
            runtime_mode_override: None,
            backend: None,
            binding: None,
            placement: None,
        }
    }

    /// A registered identity whose entry leaves the table (without passing
    /// through Retiring) ends a baseline wait as `IdentityGone`.
    #[tokio::test]
    async fn a_wait_ends_identity_gone_when_the_entry_leaves_the_table()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = runtime()?;
        let identity = AgentIdentity::parse("agent:gone")?;
        runtime
            .register(spec(&identity), IdentityLifecycleState::Dormant, None, None)
            .await;
        let baseline = runtime.completion_cursor(&identity).await;
        let wait = runtime.await_completion(&identity, Some(baseline), Duration::from_mins(10));
        let remove = async {
            tokio::task::yield_now().await;
            runtime.entries.write().await.remove(&identity);
        };
        let (outcome, ()) = tokio::join!(wait, remove);
        assert_eq!(outcome, CompletionWait::IdentityGone(baseline));
        Ok(())
    }

    /// A readiness wait (no baseline) on an identity that is not registered
    /// yet keeps waiting; it is not "gone".
    #[tokio::test(start_paused = true)]
    async fn a_readiness_wait_on_an_unregistered_identity_keeps_waiting()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = runtime()?;
        let identity = AgentIdentity::parse("agent:later")?;
        let outcome = runtime
            .await_completion(&identity, None, Duration::from_secs(5))
            .await;
        assert!(
            matches!(outcome, CompletionWait::TimedOut(_)),
            "{outcome:?}"
        );
        Ok(())
    }

    /// Shutdown ends both waits, typed.
    #[tokio::test]
    async fn waits_end_shutting_down_when_the_runtime_closes()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = runtime()?;
        let identity = AgentIdentity::parse("agent:closing")?;
        runtime
            .register(spec(&identity), IdentityLifecycleState::Dormant, None, None)
            .await;
        let baseline = runtime.completion_cursor(&identity).await;
        let completion =
            runtime.await_completion(&identity, Some(baseline), Duration::from_mins(10));
        let ticket = TurnTicket::parse(&uuid::Uuid::new_v4().to_string())?;
        let turn = runtime.wait_for_turn_outcome(&identity, ticket, Duration::from_mins(10));
        let close = async {
            tokio::task::yield_now().await;
            runtime.close_foreground_operations();
        };
        let (completion, turn, ()) = tokio::join!(completion, turn, close);
        assert!(matches!(completion, CompletionWait::ShuttingDown(_)));
        // An unknown ticket settles as Unknown at once, shutdown or not.
        assert!(matches!(turn, TurnWait::Settled(TurnOutcome::Unknown)));
        Ok(())
    }
}

#[cfg(test)]
mod lease_observer_tests {
    use super::*;
    use crate::identity_first::{LocalContinuityStore, LocalLeaseProvider};

    /// The identity health monitor's typed wake: the installed observer is
    /// told when a lease lands or moves, and not for writes that leave every
    /// lease where it was.
    #[tokio::test]
    async fn the_lease_observer_is_told_when_a_lease_lands_and_only_then()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "lease-observer-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: None,
            default_timeout: None,
        });
        let (observer, mut wake) = watch::channel(0_u64);
        runtime.install_lease_observer(observer);
        assert!(wake.has_changed()?, "installation wakes once");
        wake.borrow_and_update();

        let identity = AgentIdentity::parse("agent:leased")?;
        let spec = DurableAgentSpec {
            identity: identity.clone(),
            profile: meerkat_mob::ProfileName::from("worker"),
            addressability: AgentAddressability::Addressable,
            display_name: None,
            labels: BTreeMap::new(),
            context: None,
            additional_instructions: Vec::new(),
            initial_message: None,
            runtime_mode_override: None,
            backend: None,
            binding: None,
            placement: None,
        };
        runtime
            .register(spec.clone(), IdentityLifecycleState::Dormant, None, None)
            .await;
        assert!(!wake.has_changed()?, "no lease, no wake");

        runtime
            .register(
                spec.clone(),
                IdentityLifecycleState::Active,
                None,
                Some(LeaseGrant {
                    identity: identity.clone(),
                    fencing_token: FencingToken::new(3),
                    ttl: Duration::from_mins(5),
                }),
            )
            .await;
        assert!(wake.has_changed()?, "the lease landing wakes the observer");
        wake.borrow_and_update();

        runtime.record_turn_completed(&identity).await;
        runtime.entries.notify();
        // A table write that leaves every lease where it was.
        runtime
            .entries
            .write()
            .await
            .get_mut(&identity)
            .ok_or("registered")?
            .checkpoint_version = CheckpointVersion::new(7);
        assert!(
            !wake.has_changed()?,
            "a write that moves no lease does not wake"
        );

        runtime
            .register(
                spec,
                IdentityLifecycleState::Active,
                None,
                Some(LeaseGrant {
                    identity: identity.clone(),
                    fencing_token: FencingToken::new(4),
                    ttl: Duration::from_mins(5),
                }),
            )
            .await;
        assert!(wake.has_changed()?, "a rotated lease wakes the observer");
        Ok(())
    }
}

#[cfg(test)]
mod foreground_shutdown_tests {
    use super::*;
    use crate::identity_first::{LocalContinuityStore, LocalLeaseProvider};

    #[test]
    fn foreground_shutdown_value_is_retained_for_late_subscribers()
    -> Result<(), ContinuityStoreError> {
        let runtime = IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "late-foreground-cancel-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: None,
            default_timeout: None,
        });

        assert_eq!(runtime.foreground_cancel.receiver_count(), 0);
        runtime.close_foreground_operations();
        let receiver = runtime.foreground_cancel.subscribe();
        assert!(
            *receiver.borrow(),
            "a task subscribed after close must still observe shutdown"
        );
        Ok(())
    }
}

#[cfg(test)]
mod bootstrap_failure_attribution_tests {
    use super::*;
    use crate::identity_first::{LocalContinuityStore, LocalLeaseProvider};

    fn make_runtime() -> Result<IdentityRuntime, ContinuityStoreError> {
        Ok(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "bootstrap-failure-attribution-test".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: None,
            default_timeout: None,
        }))
    }

    /// Before embodiment became per identity, `restore_flow` failed the whole
    /// pass on one member's error and copied that cause onto every peer. The
    /// pass-failure mechanism remains for fleet-level roster, topology, and
    /// batch-store failures. This pins the three things that must hold there:
    /// terminality (or the wait barrier hangs), the pass cause in the
    /// pass-level slot, and no borrowed cause on a bystander.
    #[test]
    fn pass_failure_keeps_terminality_without_borrowing_one_members_cause()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = make_runtime()?;
        let culprit = AgentIdentity::parse("agent:culprit")?;
        let bystander = AgentIdentity::parse("agent:bystander")?;
        let survivor = AgentIdentity::parse("agent:survivor")?;
        let culprit_cause = "bridge create_session: culprit exploded";

        let generation =
            runtime.begin_identity_bootstrap_pending(IdentityBootstrapMode::EagerMaterialize);
        assert!(
            runtime.modify_bootstrap_status(Some(generation), |snapshot| {
                snapshot.identities.insert(
                    culprit.clone(),
                    IdentityBootstrapEntry {
                        state: IdentityBootstrapState::Broken,
                        error: Some(culprit_cause.to_string()),
                        restore: None,
                    },
                );
                snapshot.identities.insert(
                    bystander.clone(),
                    IdentityBootstrapEntry {
                        state: IdentityBootstrapState::Warming,
                        error: None,
                        restore: None,
                    },
                );
                snapshot.identities.insert(
                    survivor.clone(),
                    IdentityBootstrapEntry {
                        state: IdentityBootstrapState::Active,
                        error: None,
                        restore: None,
                    },
                );
                snapshot.refresh_aggregates();
            }),
            "seeding must land on the generation under test"
        );

        // Positive control: prove the pre-failure snapshot is NOT already in
        // the state the post-failure assertions claim. Without this, a seeding
        // bug that produced a Broken, cause-less bystander would let every
        // assertion below pass vacuously.
        let before = runtime.identity_bootstrap_status();
        assert!(before.error.is_none());
        assert!(!before.materialization_terminal());
        let before_bystander = before
            .identities
            .get(&bystander)
            .ok_or_else(|| "seeded bystander entry".to_string())?;
        assert_eq!(before_bystander.state, IdentityBootstrapState::Warming);
        assert!(before_bystander.error.is_none());

        // `IdentityRuntimeError`'s `Display` prefixes `Internal` ("internal:
        // {msg}"), and `fail_identity_bootstrap` stamps `error.to_string()`,
        // not the inner message. Pin the rendered form so the test tracks the
        // mechanism instead of one variant's wording.
        let pass_error = IdentityRuntimeError::Internal(culprit_cause.to_string());
        let pass_detail = pass_error.to_string();
        runtime.fail_identity_bootstrap(generation, &pass_error);

        let after = runtime.identity_bootstrap_status();
        // The pass cause rides the slot the type documents for it.
        assert_eq!(after.error.as_deref(), Some(pass_detail.as_str()));
        // Terminality survives: `wait_identity_bootstrap_terminal` returns a
        // truthful failure snapshot instead of waiting out its timeout.
        assert!(after.complete);
        assert!(after.materialization_terminal());
        assert!(!after.ready);

        // An identity that already reached Active is not retro-broken.
        let survivor_entry = after
            .identities
            .get(&survivor)
            .ok_or_else(|| "survivor entry".to_string())?;
        assert_eq!(survivor_entry.state, IdentityBootstrapState::Active);
        assert!(survivor_entry.error.is_none());

        // The identity that actually failed keeps ITS own cause verbatim.
        let culprit_entry = after
            .identities
            .get(&culprit)
            .ok_or_else(|| "culprit entry".to_string())?;
        assert_eq!(culprit_entry.state, IdentityBootstrapState::Broken);
        assert_eq!(culprit_entry.error.as_deref(), Some(culprit_cause));

        // The bystander transitions Warming -> Broken for terminality, but
        // must not report the culprit's cause as its own, and must not be
        // left cause-less either (17 Broken with no reason on record was the
        // original operator complaint).
        let bystander_entry = after
            .identities
            .get(&bystander)
            .ok_or_else(|| "bystander entry".to_string())?;
        assert_eq!(bystander_entry.state, IdentityBootstrapState::Broken);
        let bystander_error = bystander_entry
            .error
            .as_deref()
            .ok_or_else(|| "a Broken entry must carry a reason".to_string())?;
        assert_ne!(
            bystander_error, culprit_cause,
            "the culprit's raw cause must never be reported as the bystander's own"
        );
        // The discriminating assertion: the rendered pass detail is EXACTLY
        // what the previous implementation stamped on every non-Active entry,
        // so this is the one that fails if the borrowed-cause behaviour comes
        // back. (`assert_ne!` against `culprit_cause` alone would pass under
        // the old code too, because `Display` prefixes it.)
        assert_ne!(
            bystander_error, pass_detail,
            "a bystander must never carry the raw pass detail as its own cause"
        );
        assert!(
            bystander_error.starts_with("identity bootstrap pass failed before this identity"),
            "an unattributed entry must be labelled pass-level: {bystander_error}"
        );
        Ok(())
    }

    /// The `entry.error.is_none()` guard above is only truthful because a
    /// pass RESETS per-entry causes when it opens. Two failure paths stamp
    /// entries that `begin_identity_bootstrap` has not yet republished -
    /// `bootstrap_roster`'s prepare failure and `refresh_desired_topology`'s
    /// roster-provider failure - so without the reset a pass would report the
    /// PREVIOUS pass's cause per identity beside its own `status.error`.
    #[test]
    fn a_new_pass_does_not_report_the_previous_passs_cause()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = make_runtime()?;
        let identity = AgentIdentity::parse("agent:stale")?;
        let first_cause = "pass one: resume rejected";

        let first =
            runtime.begin_identity_bootstrap_pending(IdentityBootstrapMode::EagerMaterialize);
        assert!(
            runtime.modify_bootstrap_status(Some(first), |snapshot| {
                snapshot.identities.insert(
                    identity.clone(),
                    IdentityBootstrapEntry {
                        state: IdentityBootstrapState::Broken,
                        error: Some(first_cause.to_string()),
                        restore: None,
                    },
                );
                snapshot.refresh_aggregates();
            }),
            "seeding must land on the first generation"
        );

        // Positive control: the stale cause really IS on the entry before the
        // next pass opens, so the assertions below observe a transition rather
        // than a state that was never there.
        let stale = runtime.identity_bootstrap_status();
        let stale_entry = stale
            .identities
            .get(&identity)
            .ok_or_else(|| "seeded pass-one entry".to_string())?;
        assert_eq!(stale_entry.error.as_deref(), Some(first_cause));

        // Pass two fails before `begin_identity_bootstrap` republishes entries.
        let second =
            runtime.begin_identity_bootstrap_pending(IdentityBootstrapMode::EagerMaterialize);
        let cleared = runtime.identity_bootstrap_status();
        let cleared_entry = cleared
            .identities
            .get(&identity)
            .ok_or_else(|| "entry survives the pass boundary".to_string())?;
        assert!(
            cleared_entry.error.is_none(),
            "opening a pass must drop the previous pass's cause"
        );
        assert_eq!(
            cleared_entry.state,
            IdentityBootstrapState::Broken,
            "only causes are pass-scoped; states are not touched here"
        );

        let pass_error =
            IdentityRuntimeError::Internal("pass two: roster provider down".to_string());
        let pass_detail = pass_error.to_string();
        runtime.fail_identity_bootstrap(second, &pass_error);

        let after = runtime.identity_bootstrap_status();
        assert_eq!(after.error.as_deref(), Some(pass_detail.as_str()));
        let entry_error = after
            .identities
            .get(&identity)
            .and_then(|entry| entry.error.as_deref())
            .ok_or_else(|| "a Broken entry must carry a reason".to_string())?;
        assert!(
            entry_error.contains("pass two: roster provider down"),
            "an entry stamped by pass two must name pass two: {entry_error}"
        );
        assert!(
            !entry_error.contains("pass one"),
            "pass one's cause must not survive into pass two: {entry_error}"
        );
        Ok(())
    }
}

/// See [`IdentityRuntime::effective_runtime_mode`]. Free so it can be pinned
/// without a runtime: the resolution order is the contract.
pub(crate) fn effective_runtime_mode(
    definition: Option<&meerkat_mob::MobDefinition>,
    spec: &DurableAgentSpec,
) -> meerkat_mob::MobRuntimeMode {
    if let Some(mode) = spec.runtime_mode_override {
        return mode;
    }
    definition
        .and_then(|definition| definition.profiles.get(&spec.profile))
        .and_then(|binding| binding.as_inline())
        .map(|profile| profile.runtime_mode)
        .unwrap_or_default()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod effective_runtime_mode_tests {
    use super::*;

    fn spec(profile: &str, override_mode: Option<meerkat_mob::MobRuntimeMode>) -> DurableAgentSpec {
        DurableAgentSpec {
            identity: AgentIdentity::parse("identity:probe").expect("valid identity"),
            profile: meerkat_mob::ProfileName::from(profile),
            addressability: AgentAddressability::Addressable,
            display_name: None,
            labels: Default::default(),
            context: None,
            additional_instructions: Vec::new(),
            initial_message: None,
            runtime_mode_override: override_mode,
            backend: None,
            binding: None,
            placement: None,
        }
    }

    fn definition() -> meerkat_mob::MobDefinition {
        meerkat_mob::MobDefinition::from_toml(
            r#"
[mob]
id = "mode-probe"

[profiles.driven]
model = "gpt-5.5"
runtime_mode = "turn_driven"

[profiles.omitted]
model = "gpt-5.5"
"#,
        )
        .expect("definition parses")
    }

    /// The contract, in meerkat's order: override, else profile, else default.
    /// The first cut of the autonomous-host skip read only the override and
    /// skipped memory on every turn_driven profile whose roster set nothing.
    #[test]
    fn profile_runtime_mode_is_honoured_when_the_roster_sets_no_override() {
        let definition = definition();
        assert_eq!(
            effective_runtime_mode(Some(&definition), &spec("driven", None)),
            meerkat_mob::MobRuntimeMode::TurnDriven,
            "a turn_driven profile with no roster override must resolve turn_driven"
        );
        assert_eq!(
            effective_runtime_mode(Some(&definition), &spec("omitted", None)),
            meerkat_mob::MobRuntimeMode::AutonomousHost,
            "a profile that omits runtime_mode resolves to meerkat's default"
        );
    }

    #[test]
    fn roster_override_wins_over_the_profile() {
        let definition = definition();
        assert_eq!(
            effective_runtime_mode(
                Some(&definition),
                &spec("driven", Some(meerkat_mob::MobRuntimeMode::AutonomousHost)),
            ),
            meerkat_mob::MobRuntimeMode::AutonomousHost
        );
        assert_eq!(
            effective_runtime_mode(
                Some(&definition),
                &spec("omitted", Some(meerkat_mob::MobRuntimeMode::TurnDriven)),
            ),
            meerkat_mob::MobRuntimeMode::TurnDriven
        );
    }

    #[test]
    fn unknown_profile_or_missing_definition_resolves_to_the_default() {
        let definition = definition();
        assert_eq!(
            effective_runtime_mode(Some(&definition), &spec("nope", None)),
            meerkat_mob::MobRuntimeMode::default()
        );
        assert_eq!(
            effective_runtime_mode(None, &spec("driven", None)),
            meerkat_mob::MobRuntimeMode::default(),
            "with no definition installed MobKit has no profile table; default, documented"
        );
    }
}

/// Admission-first delivery and plan / wire / revalidate topology reconcile:
/// no send, steer or dispatch waits behind managed-topology wiring or a peer
/// build, and a binding change during wiring replans instead of committing a
/// stale plan.
#[cfg(test)]
mod admission_first_topology_tests {
    use super::*;
    use std::sync::Mutex as StdTestMutex;

    use super::super::bridge::{BridgeDelivery, BridgeError, ResumeSessionOutcome};
    use super::super::local_lease::LocalLeaseProvider;
    use super::super::local_store::LocalContinuityStore;
    use super::super::types::{AgentBuildDraft, SessionSnapshot};

    const ADMISSION_WAIT: Duration = Duration::from_secs(5);

    /// A session bridge whose managed-topology calls and member builds can
    /// be wedged behind `gate` (closed while false), and which records every
    /// admission and wire call it sees.
    struct WedgeableTopologyBridge {
        gate: watch::Receiver<bool>,
        wedge_inspection: bool,
        /// Park only the FIRST wire call (0 = never park).
        wedge_first_wire: bool,
        wedge_builds: bool,
        /// Park EVERY wire call until the test releases it (one
        /// `wire_release` permit per call).
        wedge_every_wire: bool,
        sessions: StdTestMutex<HashMap<String, SessionId>>,
        admissions: StdTestMutex<Vec<(String, HandlingMode)>>,
        wire_calls: StdTestMutex<Vec<Vec<(AgentRuntimeId, AgentRuntimeId)>>>,
        unwire_calls: StdTestMutex<Vec<(AgentRuntimeId, AgentRuntimeId)>>,
        /// The physical wires the inspection reports (a late-landing wire is
        /// simulated by adding to it).
        live_wires: StdTestMutex<Vec<(AgentRuntimeId, AgentRuntimeId)>>,
        /// Wire and admission calls in the order the bridge saw them.
        events: StdTestMutex<Vec<String>>,
        inspection_entered: Notify,
        wire_entered: Notify,
        wire_release: Notify,
        build_entered: Notify,
    }

    impl WedgeableTopologyBridge {
        fn new(gate: watch::Receiver<bool>) -> Self {
            Self {
                gate,
                wedge_inspection: false,
                wedge_first_wire: false,
                wedge_builds: false,
                wedge_every_wire: false,
                sessions: StdTestMutex::new(HashMap::new()),
                admissions: StdTestMutex::new(Vec::new()),
                wire_calls: StdTestMutex::new(Vec::new()),
                unwire_calls: StdTestMutex::new(Vec::new()),
                live_wires: StdTestMutex::new(Vec::new()),
                events: StdTestMutex::new(Vec::new()),
                inspection_entered: Notify::new(),
                wire_entered: Notify::new(),
                wire_release: Notify::new(),
                build_entered: Notify::new(),
            }
        }

        fn log(&self, event: String) {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(event);
        }

        fn events(&self) -> Vec<String> {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn unwire_calls(&self) -> Vec<(AgentRuntimeId, AgentRuntimeId)> {
            self.unwire_calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn land_wire(&self, a: &AgentRuntimeId, b: &AgentRuntimeId) {
            let mut live = self
                .live_wires
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            live.push((a.clone(), b.clone()));
            live.push((b.clone(), a.clone()));
        }

        fn bind_session(&self, runtime_id: &AgentRuntimeId, session_id: &SessionId) {
            self.sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(runtime_id.as_str().to_string(), session_id.clone());
        }

        fn admissions(&self) -> Vec<(String, HandlingMode)> {
            self.admissions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn wire_calls(&self) -> Vec<Vec<(AgentRuntimeId, AgentRuntimeId)>> {
            self.wire_calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        async fn park_until_open(&self) {
            let mut gate = self.gate.clone();
            while !*gate.borrow_and_update() {
                if gate.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        }
    }

    #[async_trait::async_trait]
    impl SessionBridge for WedgeableTopologyBridge {
        async fn create_session(
            &self,
            _identity: &AgentIdentity,
            runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            session_id: &SessionId,
        ) -> Result<SessionId, BridgeError> {
            self.build_entered.notify_one();
            if self.wedge_builds {
                // A member build parked in the host's build callback.
                self.park_until_open().await;
            }
            self.bind_session(runtime_id, session_id);
            Ok(session_id.clone())
        }

        async fn resume_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            session_id: &SessionId,
            _snapshot: &SessionSnapshot,
        ) -> Result<ResumeSessionOutcome, BridgeError> {
            self.build_entered.notify_one();
            if self.wedge_builds {
                self.park_until_open().await;
            }
            Ok(ResumeSessionOutcome::Resumed {
                session_id: session_id.clone(),
            })
        }

        async fn deliver_admitted(
            &self,
            runtime_id: &AgentRuntimeId,
            delivery: BridgeDelivery,
        ) -> Result<SessionId, BridgeError> {
            self.admissions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((runtime_id.as_str().to_string(), delivery.handling_mode));
            self.log(format!("admit:{runtime_id}"));
            self.sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(runtime_id.as_str())
                .cloned()
                .ok_or_else(|| BridgeError::Mob(format!("no session bound to {runtime_id}")))
        }

        async fn wire_peer(
            &self,
            a: &AgentRuntimeId,
            b: &AgentRuntimeId,
        ) -> Result<(), BridgeError> {
            let first = {
                let mut calls = self
                    .wire_calls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                calls.push(vec![(a.clone(), b.clone())]);
                calls.len() == 1
            };
            self.log(format!("wire:{a}:{b}"));
            self.wire_entered.notify_one();
            if first && self.wedge_first_wire {
                self.park_until_open().await;
            }
            if self.wedge_every_wire {
                self.wire_release.notified().await;
            }
            Ok(())
        }

        async fn current_member_wires(
            &self,
        ) -> Result<Vec<(AgentRuntimeId, AgentRuntimeId)>, BridgeError> {
            self.inspection_entered.notify_one();
            if self.wedge_inspection {
                self.park_until_open().await;
            }
            Ok(self
                .live_wires
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone())
        }

        async fn unwire_peer(
            &self,
            a: &AgentRuntimeId,
            b: &AgentRuntimeId,
        ) -> Result<(), BridgeError> {
            self.unwire_calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((a.clone(), b.clone()));
            self.live_wires
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retain(|(x, y)| !((x == a && y == b) || (x == b && y == a)));
            Ok(())
        }

        async fn checkpoint_session(
            &self,
            _runtime_id: &AgentRuntimeId,
            _session_id: &SessionId,
        ) -> Result<SessionSnapshot, BridgeError> {
            Err(BridgeError::Mob(
                "checkpoint not used in this test".to_string(),
            ))
        }

        async fn retire_member(&self, _runtime_id: &AgentRuntimeId) -> Result<(), BridgeError> {
            Ok(())
        }
    }

    fn spec(identity: &AgentIdentity) -> DurableAgentSpec {
        DurableAgentSpec {
            identity: identity.clone(),
            profile: meerkat_mob::ProfileName::from("default"),
            addressability: AgentAddressability::Addressable,
            display_name: None,
            labels: BTreeMap::new(),
            context: None,
            additional_instructions: Vec::new(),
            initial_message: None,
            runtime_mode_override: None,
            backend: None,
            binding: None,
            placement: None,
        }
    }

    fn runtime_with(
        bridge: Arc<WedgeableTopologyBridge>,
        name: &str,
    ) -> Result<Arc<IdentityRuntime>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: name.to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge),
            default_timeout: None,
        })))
    }

    /// Register `identity` Active on runtime generation `generation`, bound
    /// to a fresh session the bridge delivers to.
    async fn register_active(
        runtime: &IdentityRuntime,
        bridge: &WedgeableTopologyBridge,
        identity: &AgentIdentity,
        generation: u64,
    ) -> Result<AgentRuntimeId, Box<dyn std::error::Error + Send + Sync>> {
        register_active_as(runtime, bridge, spec(identity), generation).await
    }

    /// [`register_active`] with an explicit spec (for example a profile).
    async fn register_active_as(
        runtime: &IdentityRuntime,
        bridge: &WedgeableTopologyBridge,
        spec: DurableAgentSpec,
        generation: u64,
    ) -> Result<AgentRuntimeId, Box<dyn std::error::Error + Send + Sync>> {
        let identity = spec.identity.clone();
        let identity = &identity;
        let runtime_id = AgentRuntimeId::parse(&format!("rt:{}:{generation}", identity.as_str()))?;
        let session_id = SessionId::new();
        bridge.bind_session(&runtime_id, &session_id);
        runtime
            .register(
                spec,
                IdentityLifecycleState::Active,
                Some(ContinuityRecord {
                    identity: identity.clone(),
                    agent_runtime_id: runtime_id.clone(),
                    session_id,
                    generation: ContinuityGeneration::new(generation),
                    checkpoint_version: CheckpointVersion::new(0),
                }),
                Some(LeaseGrant {
                    identity: identity.clone(),
                    fencing_token: FencingToken::new(7 + generation),
                    ttl: Duration::from_mins(1),
                }),
            )
            .await;
        Ok(runtime_id)
    }

    /// A topology reconcile wedged inside a wiring call (a mob-actor round
    /// trip that cannot settle, e.g. behind a member mid-turn) holds no
    /// lifecycle lock, so a queue send, a dispatch and a steer to either
    /// endpoint are all admitted while it is parked. Before the fix the
    /// queue send and dispatch ran that same reconcile before admission (and
    /// waited on its lock), and the steer waited on its endpoint lifecycle
    /// lock, which the reconcile held for the whole wiring.
    #[tokio::test]
    async fn queue_dispatch_and_steer_are_admitted_while_topology_wiring_is_wedged()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let alpha = AgentIdentity::parse("agent:wedge-alpha")?;
        let beta = AgentIdentity::parse("agent:wedge-beta")?;
        let (open_gate, gate) = watch::channel(false);
        let mut bridge = WedgeableTopologyBridge::new(gate);
        bridge.wedge_inspection = true;
        let bridge = Arc::new(bridge);
        let runtime = runtime_with(bridge.clone(), "admission-first-wedged-wiring")?;
        let alpha_rt = register_active(&runtime, &bridge, &alpha, 0).await?;
        let beta_rt = register_active(&runtime, &bridge, &beta, 0).await?;
        let edges = vec![ManagedPeerEdge::new(alpha.clone(), beta.clone())?];
        runtime.set_desired_peer_edges(edges.clone()).await;

        let reconcile = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.reconcile_managed_peer_edges(&edges).await }
        });
        tokio::time::timeout(ADMISSION_WAIT, bridge.inspection_entered.notified())
            .await
            .map_err(|_| "the reconcile should reach its wiring inspection")?;

        tokio::time::timeout(
            ADMISSION_WAIT,
            runtime.send_with_mode_tracked(
                &alpha,
                &meerkat_core::ContentInput::Text("queued".to_string()),
                HandlingMode::Queue,
            ),
        )
        .await
        .map_err(|_| "a queue send must not wait behind wedged topology wiring")??;
        tokio::time::timeout(
            ADMISSION_WAIT,
            runtime.dispatch_tracked(&beta, &DispatchInput::system("dispatched")),
        )
        .await
        .map_err(|_| "a dispatch must not wait behind wedged topology wiring")??;
        tokio::time::timeout(
            ADMISSION_WAIT,
            runtime.send_with_mode_tracked(
                &beta,
                &meerkat_core::ContentInput::Text("steered".to_string()),
                HandlingMode::Steer,
            ),
        )
        .await
        .map_err(|_| "a steer must not wait behind wedged topology wiring")??;

        let admissions = bridge.admissions();
        assert!(
            admissions.contains(&(alpha_rt.as_str().to_string(), HandlingMode::Queue)),
            "queue send admitted: {admissions:?}"
        );
        assert!(
            admissions.contains(&(beta_rt.as_str().to_string(), HandlingMode::Steer)),
            "steer admitted: {admissions:?}"
        );
        assert_eq!(
            admissions
                .iter()
                .filter(|(runtime_id, _)| runtime_id == beta_rt.as_str())
                .count(),
            2,
            "dispatch and steer each admitted exactly once: {admissions:?}"
        );
        assert!(
            !reconcile.is_finished(),
            "the reconcile is still parked inside its wiring call"
        );

        open_gate.send_replace(true);
        tokio::time::timeout(ADMISSION_WAIT, reconcile)
            .await
            .map_err(|_| "the reconcile settles once its wiring does")???;
        assert!(
            runtime
                .managed_peer_edges_snapshot()
                .await
                .contains(&(alpha.clone(), beta.clone())),
            "the released reconcile commits the edge"
        );
        Ok(())
    }

    /// A peer whose member build is wedged (the host's build callback never
    /// answers) does not hold up a queue send or a steer to its neighbour:
    /// the neighbour is admitted first and the peer hydration that reaches
    /// the wedged build runs after admission.
    #[tokio::test]
    async fn admission_does_not_wait_behind_a_wedged_peer_build()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let target = AgentIdentity::parse("agent:build-target")?;
        let peer = AgentIdentity::parse("agent:build-peer")?;
        let (_open_gate, gate) = watch::channel(false);
        let mut bridge = WedgeableTopologyBridge::new(gate);
        bridge.wedge_builds = true;
        let bridge = Arc::new(bridge);
        let runtime = runtime_with(bridge.clone(), "admission-first-wedged-build")?;
        let target_rt = register_active(&runtime, &bridge, &target, 0).await?;
        runtime
            .register(spec(&peer), IdentityLifecycleState::Dormant, None, None)
            .await;
        runtime
            .set_desired_peer_edges(vec![ManagedPeerEdge::new(target.clone(), peer.clone())?])
            .await;

        tokio::time::timeout(
            ADMISSION_WAIT,
            runtime.send_with_mode_tracked(
                &target,
                &meerkat_core::ContentInput::Text("queued".to_string()),
                HandlingMode::Queue,
            ),
        )
        .await
        .map_err(|_| "a queue send must not wait behind a wedged peer build")??;
        tokio::time::timeout(ADMISSION_WAIT, bridge.build_entered.notified())
            .await
            .map_err(|_| "the post-admission hydration should reach the peer build")?;
        tokio::time::timeout(
            ADMISSION_WAIT,
            runtime.send_with_mode_tracked(
                &target,
                &meerkat_core::ContentInput::Text("steered".to_string()),
                HandlingMode::Steer,
            ),
        )
        .await
        .map_err(|_| "a steer must not wait behind a wedged peer build")??;

        assert_eq!(
            bridge.admissions(),
            vec![
                (target_rt.as_str().to_string(), HandlingMode::Queue),
                (target_rt.as_str().to_string(), HandlingMode::Steer),
            ]
        );
        Ok(())
    }

    /// An endpoint respawned (new runtime binding) while the reconcile's wire
    /// call is parked makes the plan stale: nothing is committed for the old
    /// incarnation, the reconcile replans from the live bindings and wires
    /// the new incarnation, then commits.
    #[tokio::test]
    async fn topology_reconcile_replans_when_an_endpoint_binding_changes_during_wiring()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let alpha = AgentIdentity::parse("agent:replan-alpha")?;
        let beta = AgentIdentity::parse("agent:replan-beta")?;
        let (open_gate, gate) = watch::channel(false);
        let mut bridge = WedgeableTopologyBridge::new(gate);
        bridge.wedge_first_wire = true;
        let bridge = Arc::new(bridge);
        let runtime = runtime_with(bridge.clone(), "topology-replan")?;
        let alpha_rt = register_active(&runtime, &bridge, &alpha, 0).await?;
        let beta_rt_old = register_active(&runtime, &bridge, &beta, 0).await?;
        let edges = vec![ManagedPeerEdge::new(alpha.clone(), beta.clone())?];
        runtime.set_desired_peer_edges(edges.clone()).await;

        let reconcile = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.reconcile_managed_peer_edges(&edges).await }
        });
        tokio::time::timeout(ADMISSION_WAIT, bridge.wire_entered.notified())
            .await
            .map_err(|_| "the reconcile should reach its first wire call")?;

        // beta is respawned onto a new incarnation while the wire is parked.
        let beta_rt_new = register_active(&runtime, &bridge, &beta, 1).await?;
        assert!(
            runtime.managed_peer_edges_snapshot().await.is_empty(),
            "nothing is committed while the wiring is in flight"
        );
        open_gate.send_replace(true);
        tokio::time::timeout(ADMISSION_WAIT, reconcile)
            .await
            .map_err(|_| "the reconcile settles after replanning")???;

        let calls = bridge.wire_calls();
        let wired_pair = |call: &Vec<(AgentRuntimeId, AgentRuntimeId)>,
                          beta_rt: &AgentRuntimeId| {
            call.iter()
                .any(|(a, b)| (a == &alpha_rt && b == beta_rt) || (a == beta_rt && b == &alpha_rt))
        };
        assert_eq!(
            calls.len(),
            2,
            "one stale wire, one replanned wire: {calls:?}"
        );
        assert!(
            wired_pair(&calls[0], &beta_rt_old),
            "first plan wired the old incarnation"
        );
        assert!(
            wired_pair(&calls[1], &beta_rt_new),
            "the replan wired the live incarnation: {calls:?}"
        );
        assert!(
            runtime
                .managed_peer_edges_snapshot()
                .await
                .contains(&(alpha.clone(), beta.clone())),
            "the replanned edge is committed"
        );
        Ok(())
    }

    /// Waking a Dormant member reconciles ITS managed edges before the input
    /// is admitted, so the woken member's first turn can message its peer:
    /// the bridge sees the wire before the admission.
    #[tokio::test]
    async fn waking_a_dormant_member_wires_its_peers_before_admitting_its_first_turn()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sleeper = AgentIdentity::parse("agent:wake-sleeper")?;
        let peer = AgentIdentity::parse("agent:wake-peer")?;
        let (_open_gate, gate) = watch::channel(false);
        let bridge = Arc::new(WedgeableTopologyBridge::new(gate));
        let runtime = runtime_with(bridge.clone(), "wake-wires-before-admission")?;
        let peer_rt = register_active(&runtime, &bridge, &peer, 0).await?;
        runtime
            .register(spec(&sleeper), IdentityLifecycleState::Dormant, None, None)
            .await;
        runtime
            .set_desired_peer_edges(vec![ManagedPeerEdge::new(sleeper.clone(), peer.clone())?])
            .await;

        tokio::time::timeout(
            ADMISSION_WAIT,
            runtime.send_with_mode_tracked(
                &sleeper,
                &meerkat_core::ContentInput::Text("ask agent:wake-peer for the plan".to_string()),
                HandlingMode::Queue,
            ),
        )
        .await
        .map_err(|_| "the wake send should be admitted")??;

        let sleeper_rt = runtime
            .status(&sleeper)
            .await?
            .agent_runtime_id
            .ok_or("the woken member has a runtime binding")?;
        let events = bridge.events();
        let wired = events.iter().position(|event| {
            event == &format!("wire:{sleeper_rt}:{peer_rt}")
                || event == &format!("wire:{peer_rt}:{sleeper_rt}")
        });
        let admitted = events
            .iter()
            .position(|event| event == &format!("admit:{sleeper_rt}"));
        match (wired, admitted) {
            (Some(wired), Some(admitted)) => assert!(
                wired < admitted,
                "the woken member's edge must exist before its first turn is admitted: {events:?}"
            ),
            _ => return Err(format!("expected a wire and an admission: {events:?}").into()),
        }
        let edge = ManagedPeerEdge::new(sleeper.clone(), peer.clone())?;
        assert!(
            runtime
                .managed_peer_edges_snapshot()
                .await
                .contains(&(edge.a().clone(), edge.b().clone())),
            "the woken member's edge is committed"
        );
        Ok(())
    }

    /// A wiring call that never answers settles as a typed
    /// `PeerWiringTimedOut` after its budget instead of parking the
    /// reconcile.
    #[tokio::test(start_paused = true)]
    async fn a_wedged_wiring_inspection_settles_as_peer_wiring_timed_out()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let alpha = AgentIdentity::parse("agent:timeout-alpha")?;
        let beta = AgentIdentity::parse("agent:timeout-beta")?;
        let (_open_gate, gate) = watch::channel(false);
        let mut bridge = WedgeableTopologyBridge::new(gate);
        bridge.wedge_inspection = true;
        let bridge = Arc::new(bridge);
        let runtime = runtime_with(bridge.clone(), "wiring-timeout")?;
        register_active(&runtime, &bridge, &alpha, 0).await?;
        register_active(&runtime, &bridge, &beta, 0).await?;
        let edges = vec![ManagedPeerEdge::new(alpha, beta)?];

        match runtime.reconcile_managed_peer_edges(&edges).await {
            Err(IdentityRuntimeError::PeerWiringTimedOut { operation, waited }) => {
                assert_eq!(operation, "current_member_wires");
                assert_eq!(waited, PEER_WIRING_CALL_BUDGET);
            }
            other => return Err(format!("expected PeerWiringTimedOut, got {other:?}").into()),
        }
        assert!(runtime.managed_peer_edges_snapshot().await.is_empty());
        Ok(())
    }

    /// A wire whose reply timed out may still land in the actor. Its edge is
    /// recorded as unsettled; once the desired topology no longer wants it,
    /// the next reconcile sends an ordered, tolerant unwire for it (whatever
    /// inspection says) and settles it, instead of leaking an unmanaged wire.
    #[tokio::test(start_paused = true)]
    async fn a_timed_out_wire_that_lands_late_is_unwired_once_no_longer_desired()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let alpha = AgentIdentity::parse("agent:late-alpha")?;
        let beta = AgentIdentity::parse("agent:late-beta")?;
        let (_open_gate, gate) = watch::channel(false);
        let mut bridge = WedgeableTopologyBridge::new(gate);
        bridge.wedge_first_wire = true;
        let bridge = Arc::new(bridge);
        let runtime = runtime_with(bridge.clone(), "late-landing-wire")?;
        let alpha_rt = register_active(&runtime, &bridge, &alpha, 0).await?;
        let beta_rt = register_active(&runtime, &bridge, &beta, 0).await?;
        let edge = ManagedPeerEdge::new(alpha.clone(), beta.clone())?;
        let key = (edge.a().clone(), edge.b().clone());

        match runtime
            .reconcile_managed_peer_edges(std::slice::from_ref(&edge))
            .await
        {
            Err(IdentityRuntimeError::PeerWiringTimedOut { operation, .. }) => {
                assert_eq!(operation, "wire_peers_batch");
            }
            other => return Err(format!("expected a wire timeout, got {other:?}").into()),
        }
        assert!(
            runtime.managed_peer_edges_snapshot().await.is_empty(),
            "a timed-out wire is not committed as managed"
        );
        assert!(
            runtime
                .unsettled_peer_wiring_snapshot()
                .await
                .contains(&key),
            "the timed-out edge's fate is recorded as unknown"
        );

        // The dropped wire command lands in the actor after all.
        bridge.land_wire(&alpha_rt, &beta_rt);
        // The desired topology no longer wants the edge.
        runtime.set_desired_peer_edges(Vec::new()).await;
        runtime.reconcile_managed_peer_edges(&[]).await?;

        let unwired = bridge.unwire_calls();
        assert!(
            unwired.iter().any(
                |(a, b)| (a == &alpha_rt && b == &beta_rt) || (a == &beta_rt && b == &alpha_rt)
            ),
            "the late-landed wire is unwired: {unwired:?}"
        );
        assert!(runtime.unsettled_peer_wiring_snapshot().await.is_empty());
        assert!(runtime.managed_peer_edges_snapshot().await.is_empty());
        Ok(())
    }

    /// Topology rule: member agents must not keep reaching the lead's agent
    /// once the topology separates them. `managed_peer_edges` is this
    /// process's memory and starts empty, while the mob's wiring is durable
    /// and replays on recovery. An edge between two of this runtime's
    /// identities that the topology no longer wants must be unwired after a
    /// restart, even though this process never wired it. An edge to a member
    /// that is not one of this runtime's identities is left alone.
    #[tokio::test]
    async fn an_edge_no_longer_desired_is_unwired_after_a_restart()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let lead = AgentIdentity::parse("identity:lead-1")?;
        let member = AgentIdentity::parse("identity:member-1")?;
        let peer_member = AgentIdentity::parse("identity:member-2")?;
        let (_open_gate, gate) = watch::channel(false);
        let bridge = Arc::new(WedgeableTopologyBridge::new(gate));
        // The process after the restart: nothing in its managed set. A
        // topology provider owns the topology.
        let runtime = runtime_with(bridge.clone(), "edge-prune-after-restart")?;
        runtime.set_identity_edge_ownership(IdentityEdgeOwnership::TopologyProvider);
        let lead_rt = register_active(&runtime, &bridge, &lead, 0).await?;
        let member_rt = register_active(&runtime, &bridge, &member, 0).await?;
        let peer_member_rt = register_active(&runtime, &bridge, &peer_member, 0).await?;
        let delegate_rt = AgentRuntimeId::parse("rt:delegate:0")?;
        // The previous process wired these; the mob replayed them.
        bridge.land_wire(&lead_rt, &member_rt);
        bridge.land_wire(&member_rt, &peer_member_rt);
        bridge.land_wire(&member_rt, &delegate_rt);
        assert!(runtime.managed_peer_edges_snapshot().await.is_empty());

        // The topology now keeps the members together and away from the
        // lead.
        let kept = ManagedPeerEdge::new(member.clone(), peer_member.clone())?;
        runtime
            .reconcile_managed_peer_edges(std::slice::from_ref(&kept))
            .await?;

        let unwired = bridge.unwire_calls();
        let unwired_pair = |x: &AgentRuntimeId, y: &AgentRuntimeId| {
            unwired
                .iter()
                .any(|(a, b)| (a == x && b == y) || (a == y && b == x))
        };
        assert!(
            unwired_pair(&lead_rt, &member_rt),
            "the edge the topology no longer wants is unwired: {unwired:?}"
        );
        assert!(
            !unwired_pair(&member_rt, &peer_member_rt),
            "the desired edge is kept: {unwired:?}"
        );
        assert!(
            !unwired_pair(&member_rt, &delegate_rt),
            "an edge to a member that is not one of this runtime's identities is left alone: \
             {unwired:?}"
        );
        let live = runtime.logical_peer_edges().await?;
        let stale = ManagedPeerEdge::new(lead.clone(), member.clone())?;
        assert!(
            live.contains(&kept),
            "the desired edge stays wired: {live:?}"
        );
        assert!(!live.contains(&stale), "the stale edge is gone: {live:?}");
        Ok(())
    }

    /// With a topology provider, a live identity edge the mob definition's
    /// wiring declares (here `role_wiring` worker-helper) is the definition's,
    /// so the topology never prunes it, even though its desired set does not
    /// contain it. An edge that no declared owner wants is pruned.
    #[tokio::test]
    async fn a_definition_declared_edge_is_not_the_topologys_to_prune()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let lead = AgentIdentity::parse("identity:lead")?;
        let worker = AgentIdentity::parse("identity:worker")?;
        let helper = AgentIdentity::parse("identity:helper")?;
        let as_profile = |identity: &AgentIdentity, profile: &str| {
            let mut spec = spec(identity);
            spec.profile = meerkat_mob::ProfileName::from(profile);
            spec
        };
        let (_open_gate, gate) = watch::channel(false);
        let bridge = Arc::new(WedgeableTopologyBridge::new(gate));
        let runtime = runtime_with(bridge.clone(), "edge-prune-definition-owned")?;
        runtime.set_identity_edge_ownership(IdentityEdgeOwnership::TopologyProvider);
        let definition = meerkat_mob::MobDefinition::from_toml(
            r#"
[mob]
id = "edge-prune-definition-owned"

[[wiring.role_wiring]]
a = "worker"
b = "helper"

[profiles.lead]
model = "gpt-5.5"

[profiles.worker]
model = "gpt-5.5"

[profiles.helper]
model = "gpt-5.5"
"#,
        )?;
        runtime.set_reset_roster_provider_context(
            Some(Arc::new(super::super::MutableRosterProvider::new(
                Vec::new(),
            ))),
            Some(definition),
        );
        let lead_rt = register_active_as(&runtime, &bridge, as_profile(&lead, "lead"), 0).await?;
        let worker_rt =
            register_active_as(&runtime, &bridge, as_profile(&worker, "worker"), 0).await?;
        let helper_rt =
            register_active_as(&runtime, &bridge, as_profile(&helper, "helper"), 0).await?;
        bridge.land_wire(&lead_rt, &worker_rt);
        bridge.land_wire(&worker_rt, &helper_rt);

        runtime.reconcile_managed_peer_edges(&[]).await?;

        let unwired = bridge.unwire_calls();
        let unwired_pair = |x: &AgentRuntimeId, y: &AgentRuntimeId| {
            unwired
                .iter()
                .any(|(a, b)| (a == x && b == y) || (a == y && b == x))
        };
        assert!(
            unwired_pair(&lead_rt, &worker_rt),
            "the edge no declared owner wants is pruned: {unwired:?}"
        );
        assert!(
            !unwired_pair(&worker_rt, &helper_rt),
            "the definition-declared edge is kept: {unwired:?}"
        );
        Ok(())
    }

    /// Without a topology provider the desired set covers only what was
    /// declared to this runtime: a live edge it did not wire (for example the
    /// mob definition's spawn-time wiring) is not its to remove, after a
    /// restart as before one.
    #[tokio::test]
    async fn without_a_topology_provider_an_unmanaged_edge_is_left_alone()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let lead = AgentIdentity::parse("identity:lead")?;
        let member = AgentIdentity::parse("identity:member")?;
        let (_open_gate, gate) = watch::channel(false);
        let bridge = Arc::new(WedgeableTopologyBridge::new(gate));
        let runtime = runtime_with(bridge.clone(), "edge-prune-no-provider")?;
        assert_eq!(
            runtime.identity_edge_ownership(),
            IdentityEdgeOwnership::WiredByThisProcess
        );
        let lead_rt = register_active(&runtime, &bridge, &lead, 0).await?;
        let member_rt = register_active(&runtime, &bridge, &member, 0).await?;
        bridge.land_wire(&lead_rt, &member_rt);

        runtime.reconcile_managed_peer_edges(&[]).await?;

        assert!(
            bridge.unwire_calls().is_empty(),
            "an edge this runtime did not wire is left alone: {:?}",
            bridge.unwire_calls()
        );
        let edge = ManagedPeerEdge::new(lead.clone(), member.clone())?;
        assert!(runtime.logical_peer_edges().await?.contains(&edge));
        Ok(())
    }

    /// An endpoint whose binding moves during every plan's wiring exhausts
    /// the bounded replans and settles as `PeerTopologySuperseded`, having
    /// committed nothing.
    #[tokio::test]
    async fn a_binding_that_keeps_moving_settles_as_peer_topology_superseded()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let alpha = AgentIdentity::parse("agent:superseded-alpha")?;
        let beta = AgentIdentity::parse("agent:superseded-beta")?;
        let (_open_gate, gate) = watch::channel(false);
        let mut bridge = WedgeableTopologyBridge::new(gate);
        bridge.wedge_every_wire = true;
        let bridge = Arc::new(bridge);
        let runtime = runtime_with(bridge.clone(), "topology-superseded")?;
        register_active(&runtime, &bridge, &alpha, 0).await?;
        register_active(&runtime, &bridge, &beta, 0).await?;
        let edges = vec![ManagedPeerEdge::new(alpha.clone(), beta.clone())?];

        let reconcile = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.reconcile_managed_peer_edges(&edges).await }
        });
        for generation in 1..=u64::from(PEER_TOPOLOGY_MAX_PLAN_ATTEMPTS) {
            tokio::time::timeout(ADMISSION_WAIT, bridge.wire_entered.notified())
                .await
                .map_err(|_| "each plan reaches its wire call")?;
            // beta is respawned while this plan's wire is in flight.
            register_active(&runtime, &bridge, &beta, generation).await?;
            bridge.wire_release.notify_one();
        }
        match tokio::time::timeout(ADMISSION_WAIT, reconcile).await?? {
            Err(IdentityRuntimeError::PeerTopologySuperseded { attempts }) => {
                assert_eq!(attempts, PEER_TOPOLOGY_MAX_PLAN_ATTEMPTS);
            }
            other => return Err(format!("expected PeerTopologySuperseded, got {other:?}").into()),
        }
        assert_eq!(
            bridge.wire_calls().len(),
            PEER_TOPOLOGY_MAX_PLAN_ATTEMPTS as usize,
            "one wire per plan"
        );
        assert!(
            runtime.managed_peer_edges_snapshot().await.is_empty(),
            "a stale plan commits nothing"
        );
        Ok(())
    }

    /// A topology mutation whose endpoint is respawned while its wire is in
    /// flight replans and wires the live incarnation, committing only that.
    #[tokio::test]
    async fn a_topology_mutation_replans_when_an_endpoint_binding_changes_during_wiring()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let alpha = AgentIdentity::parse("agent:mutate-alpha")?;
        let beta = AgentIdentity::parse("agent:mutate-beta")?;
        let (open_gate, gate) = watch::channel(false);
        let mut bridge = WedgeableTopologyBridge::new(gate);
        bridge.wedge_first_wire = true;
        let bridge = Arc::new(bridge);
        let runtime = runtime_with(bridge.clone(), "topology-mutation-replan")?;
        let alpha_rt = register_active(&runtime, &bridge, &alpha, 0).await?;
        register_active(&runtime, &bridge, &beta, 0).await?;
        let edge = ManagedPeerEdge::new(alpha.clone(), beta.clone())?;

        let mutation = tokio::spawn({
            let runtime = runtime.clone();
            let edge = edge.clone();
            async move {
                runtime
                    .mutate_managed_peer_edge_admitted(
                        crate::topology_control::TopologyAction::Connect,
                        &edge,
                    )
                    .await
            }
        });
        tokio::time::timeout(ADMISSION_WAIT, bridge.wire_entered.notified())
            .await
            .map_err(|_| "the mutation reaches its wire call")?;
        let beta_rt_new = register_active(&runtime, &bridge, &beta, 1).await?;
        open_gate.send_replace(true);
        tokio::time::timeout(ADMISSION_WAIT, mutation).await???;

        let calls = bridge.wire_calls();
        assert_eq!(
            calls.len(),
            2,
            "one stale wire, one replanned wire: {calls:?}"
        );
        assert!(
            calls[1]
                .iter()
                .any(|(a, b)| (a == &alpha_rt && b == &beta_rt_new)
                    || (a == &beta_rt_new && b == &alpha_rt)),
            "the replan wired the live incarnation: {calls:?}"
        );
        assert!(
            runtime
                .managed_peer_edges_snapshot()
                .await
                .contains(&(edge.a().clone(), edge.b().clone()))
        );
        Ok(())
    }

    /// A session bridge whose wiring commands run in order on one simulated
    /// mob actor: a command enqueued later runs after an earlier one, even
    /// when the earlier command's caller stopped waiting for its reply. The
    /// inspection, like the real one, reads live state directly (NOT through
    /// the actor queue), so a command still queued is invisible to it.
    struct OrderedActorBridge {
        actor: Arc<tokio::sync::Mutex<()>>,
        gate: watch::Receiver<bool>,
        park_first_wire: std::sync::atomic::AtomicBool,
        live_wires: Arc<StdTestMutex<Vec<(AgentRuntimeId, AgentRuntimeId)>>>,
        events: Arc<StdTestMutex<Vec<String>>>,
        wire_entered: Notify,
        unwire_entered: Notify,
    }

    impl OrderedActorBridge {
        fn new(gate: watch::Receiver<bool>) -> Self {
            Self {
                actor: Arc::new(tokio::sync::Mutex::new(())),
                gate,
                park_first_wire: std::sync::atomic::AtomicBool::new(true),
                live_wires: Arc::new(StdTestMutex::new(Vec::new())),
                events: Arc::new(StdTestMutex::new(Vec::new())),
                wire_entered: Notify::new(),
                unwire_entered: Notify::new(),
            }
        }

        fn events(&self) -> Vec<String> {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn live_wires(&self) -> Vec<(AgentRuntimeId, AgentRuntimeId)> {
            self.live_wires
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl SessionBridge for OrderedActorBridge {
        async fn create_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            session_id: &SessionId,
        ) -> Result<SessionId, BridgeError> {
            Ok(session_id.clone())
        }

        async fn resume_session(
            &self,
            _identity: &AgentIdentity,
            _runtime_id: &AgentRuntimeId,
            _spec: &DurableAgentSpec,
            _draft: &AgentBuildDraft,
            session_id: &SessionId,
            _snapshot: &SessionSnapshot,
        ) -> Result<ResumeSessionOutcome, BridgeError> {
            Ok(ResumeSessionOutcome::Resumed {
                session_id: session_id.clone(),
            })
        }

        async fn deliver_admitted(
            &self,
            _runtime_id: &AgentRuntimeId,
            _delivery: BridgeDelivery,
        ) -> Result<SessionId, BridgeError> {
            Err(BridgeError::Mob(
                "delivery not used in this test".to_string(),
            ))
        }

        async fn wire_peer(
            &self,
            a: &AgentRuntimeId,
            b: &AgentRuntimeId,
        ) -> Result<(), BridgeError> {
            // Enqueue on the actor: once holding its turn, the command runs
            // to completion in the actor even if this caller is dropped.
            let turn = self.actor.clone().lock_owned().await;
            let park = self
                .park_first_wire
                .swap(false, std::sync::atomic::Ordering::SeqCst);
            let (done, reply) = tokio::sync::oneshot::channel();
            let mut gate = self.gate.clone();
            let live = self.live_wires.clone();
            let events = self.events.clone();
            let (a, b) = (a.clone(), b.clone());
            self.wire_entered.notify_one();
            tokio::spawn(async move {
                let _turn = turn;
                if park {
                    while !*gate.borrow_and_update() {
                        if gate.changed().await.is_err() {
                            return;
                        }
                    }
                }
                {
                    let mut live = live
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    live.push((a.clone(), b.clone()));
                    live.push((b.clone(), a.clone()));
                }
                events
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(format!("wire-landed:{a}:{b}"));
                let _ = done.send(());
            });
            reply
                .await
                .map_err(|_| BridgeError::Mob("actor dropped the wire".to_string()))
        }

        async fn current_member_wires(
            &self,
        ) -> Result<Vec<(AgentRuntimeId, AgentRuntimeId)>, BridgeError> {
            Ok(self.live_wires())
        }

        async fn unwire_peer(
            &self,
            a: &AgentRuntimeId,
            b: &AgentRuntimeId,
        ) -> Result<(), BridgeError> {
            self.unwire_entered.notify_one();
            // Ordered behind every earlier command, and tolerant: removing a
            // wire that is not there is Ok.
            let _turn = self.actor.clone().lock_owned().await;
            self.live_wires
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retain(|(x, y)| !((x == a && y == b) || (x == b && y == a)));
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(format!("unwire-applied:{a}:{b}"));
            Ok(())
        }

        async fn checkpoint_session(
            &self,
            _runtime_id: &AgentRuntimeId,
            _session_id: &SessionId,
        ) -> Result<SessionSnapshot, BridgeError> {
            Err(BridgeError::Mob(
                "checkpoint not used in this test".to_string(),
            ))
        }

        async fn retire_member(&self, _runtime_id: &AgentRuntimeId) -> Result<(), BridgeError> {
            Ok(())
        }
    }

    /// A wire whose reply timed out is still queued in a wedged actor when
    /// the next reconcile runs, so inspection cannot see it. That reconcile
    /// must not trust inspection for the unsettled edge: it issues an
    /// ordered, tolerant unwire, which runs after the wire lands, and the
    /// edge stays unsettled until that unwire succeeded. Before the fix the
    /// edge was folded into the managed set, found absent, dropped at commit,
    /// and then landed unmanaged (a leaked wire).
    #[tokio::test(start_paused = true)]
    async fn a_wire_that_lands_after_the_next_reconcile_is_still_unwired()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let alpha = AgentIdentity::parse("agent:ordered-alpha")?;
        let beta = AgentIdentity::parse("agent:ordered-beta")?;
        let (open_gate, gate) = watch::channel(false);
        let bridge = Arc::new(OrderedActorBridge::new(gate));
        let runtime = Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
            continuity_store: Arc::new(LocalContinuityStore::in_memory()?),
            lease_provider: Arc::new(LocalLeaseProvider::new()),
            runtime_instance_id: "ordered-late-wire".to_string(),
            has_runtime_store: true,
            durability_policy: DurabilityPolicy::SyncWriteThrough,
            bridge: Some(bridge.clone()),
            default_timeout: None,
        }));
        let mut runtime_ids = Vec::new();
        for identity in [&alpha, &beta] {
            let runtime_id = AgentRuntimeId::parse(&format!("rt:{}:0", identity.as_str()))?;
            runtime
                .register(
                    spec(identity),
                    IdentityLifecycleState::Active,
                    Some(ContinuityRecord {
                        identity: identity.clone(),
                        agent_runtime_id: runtime_id.clone(),
                        session_id: SessionId::new(),
                        generation: ContinuityGeneration::new(0),
                        checkpoint_version: CheckpointVersion::new(0),
                    }),
                    Some(LeaseGrant {
                        identity: identity.clone(),
                        fencing_token: FencingToken::new(7),
                        ttl: Duration::from_mins(1),
                    }),
                )
                .await;
            runtime_ids.push(runtime_id);
        }
        let edge = ManagedPeerEdge::new(alpha.clone(), beta.clone())?;
        let key = (edge.a().clone(), edge.b().clone());

        // 1. The wire is enqueued, parks in the actor, and its reply times out.
        match runtime
            .reconcile_managed_peer_edges(std::slice::from_ref(&edge))
            .await
        {
            Err(IdentityRuntimeError::PeerWiringTimedOut { operation, .. }) => {
                assert_eq!(operation, "wire_peers_batch");
            }
            other => return Err(format!("expected a wire timeout, got {other:?}").into()),
        }
        assert!(
            runtime
                .unsettled_peer_wiring_snapshot()
                .await
                .contains(&key)
        );
        assert!(bridge.live_wires().is_empty(), "the wire is still queued");

        // 2. The desired set drops the edge and the next reconcile runs while
        //    the wire is STILL queued (inspection sees nothing).
        runtime.set_desired_peer_edges(Vec::new()).await;
        let reconcile = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.reconcile_managed_peer_edges(&[]).await }
        });
        tokio::time::timeout(ADMISSION_WAIT, bridge.unwire_entered.notified())
            .await
            .map_err(|_| "the reconcile issues an ordered unwire for the unsettled edge")?;
        assert!(
            runtime
                .unsettled_peer_wiring_snapshot()
                .await
                .contains(&key),
            "the edge stays unsettled until an ordered command succeeded"
        );

        // 3. Only now does the queued wire land; the unwire runs after it.
        open_gate.send_replace(true);
        tokio::time::timeout(ADMISSION_WAIT, reconcile)
            .await
            .map_err(|_| "the reconcile settles once the actor drains")???;

        let events = bridge.events();
        let landed = events
            .iter()
            .position(|event| event.starts_with("wire-landed:"));
        let unwired = events
            .iter()
            .position(|event| event.starts_with("unwire-applied:"));
        match (landed, unwired) {
            (Some(landed), Some(unwired)) => assert!(
                landed < unwired,
                "the ordered unwire runs after the late wire: {events:?}"
            ),
            _ => return Err(format!("expected a late wire then an unwire: {events:?}").into()),
        }
        assert!(bridge.live_wires().is_empty(), "no wire leaked");
        assert!(runtime.unsettled_peer_wiring_snapshot().await.is_empty());
        assert!(runtime.managed_peer_edges_snapshot().await.is_empty());
        Ok(())
    }

    /// The topology commit takes no lifecycle lock: a reconcile completes and
    /// commits its edge while the test holds an endpoint's lifecycle lock (as
    /// a lifecycle operation or an admission would).
    #[tokio::test]
    async fn a_topology_commit_goes_through_while_an_endpoint_lifecycle_lock_is_held()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let alpha = AgentIdentity::parse("agent:locked-alpha")?;
        let beta = AgentIdentity::parse("agent:locked-beta")?;
        let (_open_gate, gate) = watch::channel(false);
        let bridge = Arc::new(WedgeableTopologyBridge::new(gate));
        let runtime = runtime_with(bridge.clone(), "commit-without-lifecycle-locks")?;
        register_active(&runtime, &bridge, &alpha, 0).await?;
        register_active(&runtime, &bridge, &beta, 0).await?;
        let edge = ManagedPeerEdge::new(alpha.clone(), beta.clone())?;

        let alpha_lock = runtime.lifecycle_lock_for(&alpha).await;
        let beta_lock = runtime.lifecycle_lock_for(&beta).await;
        let _alpha_held = alpha_lock.lock().await;
        let _beta_held = beta_lock.lock().await;
        tokio::time::timeout(
            ADMISSION_WAIT,
            runtime.reconcile_managed_peer_edges(std::slice::from_ref(&edge)),
        )
        .await
        .map_err(|_| "the topology commit must not wait on an endpoint lifecycle lock")??;
        assert!(
            runtime
                .managed_peer_edges_snapshot()
                .await
                .contains(&(edge.a().clone(), edge.b().clone())),
            "the edge is committed while both lifecycle locks are held"
        );
        Ok(())
    }

    /// The wake path's target reconcile is bounded end to end: when its
    /// topology locks stay held past the budget, it settles as
    /// `PeerWiringTimedOut { operation: "wake_topology_locks" }`, which is
    /// logged, and the woken member's input is still admitted.
    #[tokio::test(start_paused = true)]
    async fn a_wake_topology_lock_timeout_does_not_refuse_admission()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sleeper = AgentIdentity::parse("agent:wake-lock-sleeper")?;
        let peer = AgentIdentity::parse("agent:wake-lock-peer")?;
        let (_open_gate, gate) = watch::channel(false);
        let bridge = Arc::new(WedgeableTopologyBridge::new(gate));
        let runtime = runtime_with(bridge.clone(), "wake-lock-timeout")?;
        register_active(&runtime, &bridge, &peer, 0).await?;
        runtime
            .register(spec(&sleeper), IdentityLifecycleState::Dormant, None, None)
            .await;
        runtime
            .set_desired_peer_edges(vec![ManagedPeerEdge::new(sleeper.clone(), peer.clone())?])
            .await;

        // Another topology operation holds the reconcile lock throughout.
        let _held = runtime.managed_peer_reconcile_lock.lock().await;
        let started = tokio::time::Instant::now();
        runtime
            .send_with_mode_tracked(
                &sleeper,
                &meerkat_core::ContentInput::Text("wake up".to_string()),
                HandlingMode::Queue,
            )
            .await?;
        assert!(
            started.elapsed() >= WAKE_TOPOLOGY_BUDGET,
            "the wake reconcile waited out its single budget"
        );
        let sleeper_rt = runtime
            .status(&sleeper)
            .await?
            .agent_runtime_id
            .ok_or("the woken member has a runtime binding")?;
        assert!(
            bridge
                .admissions()
                .iter()
                .any(|(runtime_id, mode)| runtime_id == sleeper_rt.as_str()
                    && *mode == HandlingMode::Queue),
            "the woken member's input is admitted despite the lock timeout"
        );
        assert!(
            bridge.wire_calls().is_empty(),
            "nothing was wired while the reconcile lock was held"
        );
        Ok(())
    }

    /// Concurrent admissions to one target merge their post-admission
    /// hydrations: while the first is parked (building a Dormant peer), two
    /// more admissions only request ONE more pass, and no second hydration
    /// runs in parallel.
    #[tokio::test]
    async fn concurrent_hydrations_to_one_target_merge_into_one_rerun()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let target = AgentIdentity::parse("agent:merge-target")?;
        let peer = AgentIdentity::parse("agent:merge-peer")?;
        let (open_gate, gate) = watch::channel(false);
        let mut bridge = WedgeableTopologyBridge::new(gate);
        bridge.wedge_builds = true;
        let bridge = Arc::new(bridge);
        let runtime = runtime_with(bridge.clone(), "merged-hydrations")?;
        register_active(&runtime, &bridge, &target, 0).await?;
        runtime
            .register(spec(&peer), IdentityLifecycleState::Dormant, None, None)
            .await;
        runtime
            .set_desired_peer_edges(vec![ManagedPeerEdge::new(target.clone(), peer.clone())?])
            .await;
        let passes = || {
            runtime
                .peer_hydration_passes
                .load(std::sync::atomic::Ordering::SeqCst)
        };
        let send = |text: &'static str| {
            let runtime = runtime.clone();
            let target = target.clone();
            async move {
                runtime
                    .send_with_mode_tracked(
                        &target,
                        &meerkat_core::ContentInput::Text(text.to_string()),
                        HandlingMode::Queue,
                    )
                    .await
            }
        };

        tokio::time::timeout(ADMISSION_WAIT, send("first"))
            .await
            .map_err(|_| "the first send is admitted")??;
        // Its hydration parks building the Dormant peer.
        tokio::time::timeout(ADMISSION_WAIT, bridge.build_entered.notified())
            .await
            .map_err(|_| "the first hydration reaches the peer build")?;
        for text in ["second", "third"] {
            tokio::time::timeout(ADMISSION_WAIT, send(text))
                .await
                .map_err(|_| "a later send is admitted while hydration is parked")??;
        }
        assert_eq!(passes(), 1, "no second hydration started in parallel");
        {
            let hydrations = runtime
                .peer_hydrations_in_flight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(hydrations.in_flight.len(), 1);
            assert!(
                hydrations
                    .in_flight
                    .get(&target)
                    .is_some_and(|claim| claim.rerun),
                "the later admissions requested one more pass"
            );
        }

        open_gate.send_replace(true);
        tokio::time::timeout(ADMISSION_WAIT, async {
            loop {
                let idle = runtime
                    .peer_hydrations_in_flight
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .in_flight
                    .is_empty();
                if idle {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|_| "the merged hydration finishes")?;
        assert_eq!(passes(), 2, "three admissions ran exactly one extra pass");
        assert_eq!(bridge.admissions().len(), 3);
        Ok(())
    }

    /// A stale release (a second removal after a panic, or a refused spawn)
    /// only releases its own claim, never a newer one.
    #[test]
    fn a_stale_hydration_release_never_drops_a_newer_claim()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let target = AgentIdentity::parse("agent:claim-target")?;
        let hydrations = StdMutex::new(PeerHydrations::default());
        let lock = || {
            hydrations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        };
        let first = lock().claim(&target).ok_or("first claim")?;
        {
            let slot = PeerHydrationSlot {
                hydrations: &hydrations,
                identity: target.clone(),
                token: first,
            };
            assert!(!slot.finish_pass(), "no rerun pending: released");
            // Dropping the slot releases again; it must be a no-op.
        }
        let second = lock().claim(&target).ok_or("the slot is free again")?;
        assert_ne!(first, second);
        // The first claim's stale release does not drop the second.
        lock().release(&target, first);
        assert!(
            lock()
                .in_flight
                .get(&target)
                .is_some_and(|claim| claim.token == second)
        );
        assert!(
            lock().claim(&target).is_none(),
            "the second claim still owns it"
        );
        Ok(())
    }
}
