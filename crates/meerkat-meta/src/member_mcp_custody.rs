//! Custody of the MCP servers a gateway's members own.
//!
//! A member's MCP router adapter belongs to its agent, which releases it when
//! its session ends. That release is not a cleanup receipt: it returns unit,
//! and a panicked or aborted session skips it. The member teardown that drives
//! it is also bounded ([`MOB_TERMINAL_SHUTDOWN_BUDGET`]) below a stdio close's
//! own bound ([`McpStdioCloseBounds::close_bound`]), so a release can be cut
//! off while its router's closes still run.
//!
//! So every member factory adopts each adapter into a gateway's custody at
//! the factory's ownership seam, before any server is staged or connected
//! ([`AgentFactory::with_mcp_adapter_custody`]). Custody keeps the adapter
//! itself until the adapter's own shutdown reported, and then only that
//! report's native outcomes, so no release, cut off or skipped, loses the
//! closes its router owns. Once custody is an adapter's last owner and no
//! shutdown reported, custody starts that shutdown itself, as an abandoned
//! construction's cleanup does, so it never keeps a server running past its
//! owners. Settlement freezes the custody and, for every adapter whose
//! shutdown has not reported, starts its closes at once
//! ([`McpRouterAdapter::begin_close`], behind no lock) and waits for each
//! close's native outcome until the deadline Meerkat derives from the closes
//! it started or joined. The settlement runs again at the gateway's
//! executor-exit boundary, after the runtime shutdown's observation ends for
//! whatever reason, and there no deadline ends a wait: the boundary keeps
//! waiting on each original close until its native outcome reports, so
//! neither a horizon cut-off nor a bound Meerkat broke leaves a close running
//! when the executor is destroyed. A native attempt that returned unsettled
//! is reported incomplete by its class, and a close that had not reported by
//! its deadline is reported under `deadline` as the bound Meerkat broke.
//!
//! [`MOB_TERMINAL_SHUTDOWN_BUDGET`]: crate::unified_runtime::lifecycle::MOB_TERMINAL_SHUTDOWN_BUDGET

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use meerkat::{AgentFactory, McpAdapterCustody, McpAdapterCustodyClosed};
use meerkat_mcp::{
    McpCloseTicket, McpRouterAdapter, McpRouterShutdownObservation, McpRouterShutdownObserver,
    McpRouterShutdownReport, McpServerCloseOutcome, McpServerCloseRecord, McpStdioCloseBounds,
    McpStdioLeaderOutcome, McpStdioWitnessOutcome,
};

use crate::unified_runtime::{McpCloseUnsettledClass, MemberMcpSettlement, MemberMcpUnsettled};

/// The longest one settlement can take: the stdio close bound of the members'
/// routers (Meta never sets one, so routers keep
/// [`McpStdioCloseBounds::DEFAULT`]) plus Meerkat's join allowance. A
/// settlement waits on each ticket's own deadline, which is at most this
/// after the settlement started: it is an upper bound, never a timer that
/// cuts a close. It is a bounded phase inside `UnifiedRuntime::shutdown`, and
/// again at a gateway's executor-exit boundary, so the advertised shutdown
/// horizon counts it twice. The exit boundary waits past a ticket's deadline
/// for its native outcome, which outlasts this budget only when Meerkat broke
/// its own close bound.
pub const MEMBER_MCP_SETTLEMENT_BUDGET: Duration = McpStdioCloseBounds::DEFAULT
    .close_bound()
    .saturating_add(McpCloseTicket::JOIN_ALLOWANCE);

/// One member adapter in custody whose own shutdown has not reported.
struct CustodyEntry {
    /// Kept, so its router's closes can still be started or joined here
    /// whatever happened to its release.
    adapter: Arc<McpRouterAdapter>,
    /// The adapter's one router shutdown, whoever starts it.
    shutdown: McpRouterShutdownObserver,
    /// Custody started that shutdown itself, as the adapter's last owner.
    shutdown_started: bool,
    /// A settlement reached the deadline Meerkat derived from this adapter's
    /// closes before they all reported: Meerkat broke its own bound, which
    /// counts under `deadline` whenever its outcomes are collected.
    missed_deadline: bool,
}

/// What a settlement does with a close whose native outcome has not reported
/// by the deadline Meerkat derived from it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PastDeadline {
    /// Leave it pending in custody for a later settlement: the runtime
    /// shutdown's own phase, which stays bounded.
    Leave,
    /// Keep waiting until its native outcome reports: the gateway's
    /// executor-exit boundary, after which no settlement runs.
    Join,
}

/// One adapter's close wait in a settlement.
enum CloseWait {
    /// The native outcomes reported, `late` when only after the deadline.
    Reported {
        records: Vec<McpServerCloseRecord>,
        late: bool,
    },
    /// Still pending at the deadline, and left in custody.
    Pending,
}

impl CustodyEntry {
    /// The adapter's own shutdown report, once it reported.
    fn reported(&self) -> Option<Arc<McpRouterShutdownReport>> {
        match self.shutdown.current() {
            Some(McpRouterShutdownObservation::Reported(report)) => Some(report),
            _ => None,
        }
    }
}

#[derive(Default)]
struct CustodyState {
    /// Adapters whose native outcomes have not reported.
    entries: Vec<CustodyEntry>,
    /// The routers whose native outcomes reported, and those outcomes by
    /// class. Under `deadline`, those of them that a settlement's deadline
    /// had already reached.
    reported: MemberMcpUnsettled,
    /// Set by the first settlement: no adapter is adopted after it, so its
    /// snapshot is every adapter that can own a server.
    frozen: bool,
}

impl CustodyState {
    /// Release every adapter whose own shutdown reported, keeping that
    /// shutdown's native outcomes, and start the shutdown of every adapter
    /// custody alone still owns.
    fn release_reported(&mut self) {
        let reported = &mut self.reported;
        self.entries.retain_mut(|entry| match entry.reported() {
            Some(report) => {
                reported.routers += 1;
                reported.deadline += usize::from(entry.missed_deadline);
                count_records(reported, report.servers());
                false
            }
            None => {
                if !entry.shutdown_started
                    && Arc::strong_count(&entry.adapter) == 1
                    && let Ok(runtime) = tokio::runtime::Handle::try_current()
                {
                    // Its owners dropped it without a release that reported
                    // (a panicked or aborted session skips it). The adapter's
                    // own shutdown joins one already under way.
                    let adapter = Arc::clone(&entry.adapter);
                    runtime.spawn(async move { adapter.shutdown().await });
                    entry.shutdown_started = true;
                }
                true
            }
        });
    }
}

/// A gateway's custody of its members' MCP router adapters.
///
/// Install it on every member factory with [`Self::install`], and hand it to
/// the runtime with
/// [`MobBootstrapSpec::with_member_mcp_custody`](crate::MobBootstrapSpec::with_member_mcp_custody).
/// It grants nothing: it holds closes the factory's adapters already own.
#[derive(Default)]
pub struct MemberMcpCustody {
    state: Mutex<CustodyState>,
    /// Serializes settlements: the runtime's own and the executor-exit one
    /// can overlap after a horizon cut-off.
    settling: tokio::sync::Mutex<()>,
    /// Test seam: every observation expires at once.
    #[cfg(test)]
    expire_observations: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for MemberMcpCustody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("MemberMcpCustody")
            .field("unreported_adapters", &state.entries.len())
            .field("reported_routers", &state.reported.routers)
            .field("frozen", &state.frozen)
            .finish()
    }
}

impl MemberMcpCustody {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// `factory`, adopting every MCP router adapter it constructs into this
    /// custody before any of its servers is staged.
    #[must_use]
    pub fn install(self: &Arc<Self>, factory: AgentFactory) -> AgentFactory {
        factory.with_mcp_adapter_custody(Arc::clone(self) as Arc<dyn McpAdapterCustody>)
    }

    /// Adapters ever adopted.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        let state = self.lock();
        state.entries.len() + state.reported.routers
    }

    /// Adapters in custody whose native outcomes have not reported.
    #[cfg(test)]
    pub(crate) fn adapters(&self) -> Vec<Arc<McpRouterAdapter>> {
        self.lock()
            .entries
            .iter()
            .map(|entry| Arc::clone(&entry.adapter))
            .collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CustodyState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Make every later observation expire at once, as if Meerkat broke its
    /// own close bound: the closes it observes are still pending at their
    /// deadline.
    #[cfg(test)]
    pub(crate) fn expire_observations_for_test(&self) {
        self.expire_observations
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Freeze the custody, start the closes of every adapter in it whose own
    /// shutdown has not reported, and wait for each close's native outcome.
    ///
    /// Initiation waits behind nothing: [`McpRouterAdapter::begin_close`]
    /// starts every stdio close through its custody, without the router lock
    /// that in-flight dispatch holds, and joins a close already running. Each
    /// adapter's wait ends at its ticket's deadline, which Meerkat derives
    /// from the closes it started or joined, so no admitted close is cut
    /// before its native attempt reports; one still pending then stays in
    /// custody, counted under `deadline`. A settlement is safe to repeat: an
    /// adapter whose outcomes reported is counted from them, and one whose
    /// closes did not report is started (or joined) again. The gateway's
    /// executor-exit boundary relies on that, with [`Self::settle_to_exit`].
    pub(crate) async fn settle(&self) -> MemberMcpSettlement {
        self.settle_with(PastDeadline::Leave).await
    }

    /// The settlement at a gateway's executor-exit boundary: [`Self::settle`],
    /// except that a close still pending at its deadline is waited for on its
    /// own ticket until its native outcome reports. No settlement runs after
    /// this one, so no deadline may end its wait: the executor destroyed
    /// after it owns no pending close. Such a close counts in its native
    /// outcome's class, and its router under `deadline`, as the bound Meerkat
    /// broke. The wait outlasts the ticket's deadline only then.
    pub(crate) async fn settle_to_exit(&self) -> MemberMcpSettlement {
        self.settle_with(PastDeadline::Join).await
    }

    async fn settle_with(&self, past_deadline: PastDeadline) -> MemberMcpSettlement {
        let _settling = self.settling.lock().await;
        let pending: Vec<Arc<McpRouterAdapter>> = {
            let mut state = self.lock();
            state.frozen = true;
            state.release_reported();
            state
                .entries
                .iter()
                .map(|entry| Arc::clone(&entry.adapter))
                .collect()
        };
        // Every ticket is begun here, before the first await.
        let waits = futures::future::join_all(pending.iter().map(|adapter| {
            let ticket = adapter.begin_close();
            let deadline = ticket.deadline();
            #[cfg(test)]
            let deadline = if self
                .expire_observations
                .load(std::sync::atomic::Ordering::Acquire)
            {
                tokio::time::Instant::now()
            } else {
                deadline
            };
            async move {
                let mut outcomes = std::pin::pin!(ticket.outcomes());
                match tokio::time::timeout_at(deadline, outcomes.as_mut()).await {
                    Ok(records) => CloseWait::Reported {
                        records,
                        late: false,
                    },
                    Err(_) if past_deadline == PastDeadline::Join => {
                        tracing::warn!(
                            "a member MCP close had not reported by the deadline Meerkat derived \
                             from it; the executor-exit boundary waits for its native outcome"
                        );
                        CloseWait::Reported {
                            records: outcomes.await,
                            late: true,
                        }
                    }
                    Err(_) => CloseWait::Pending,
                }
            }
        }))
        .await;

        let mut state = self.lock();
        // Once frozen, only a settlement (serialized) changes the entries, so
        // they are still `pending`, in order.
        let mut unreported = Vec::new();
        for (mut entry, wait) in std::mem::take(&mut state.entries).into_iter().zip(waits) {
            match wait {
                CloseWait::Reported { records, late } => {
                    state.reported.routers += 1;
                    state.reported.deadline += usize::from(late || entry.missed_deadline);
                    count_records(&mut state.reported, &records);
                }
                CloseWait::Pending => {
                    entry.missed_deadline = true;
                    unreported.push(entry);
                }
            }
        }
        state.entries = unreported;
        let mut counts = state.reported.clone();
        // Meerkat broke its own bound: these adapters' closes had not all
        // reported by the deadline it derived from them, and still have not.
        counts.routers += state.entries.len();
        counts.deadline += state.entries.len();
        drop(state);
        if counts.routers == 0 {
            return MemberMcpSettlement::NothingOwned;
        }
        let settlement = MemberMcpSettlement::from_counts(counts);
        if !settlement.is_settled() {
            tracing::warn!(
                ?settlement,
                "member MCP servers did not settle before shutdown reported"
            );
        }
        settlement
    }
}

impl McpAdapterCustody for MemberMcpCustody {
    fn adopt(&self, adapter: &Arc<McpRouterAdapter>) -> Result<(), McpAdapterCustodyClosed> {
        let mut state = self.lock();
        // A frozen custody refuses: the construction then fails at the seam,
        // before any server process exists, so nothing escapes the snapshot
        // a settlement took.
        if state.frozen {
            return Err(McpAdapterCustodyClosed);
        }
        // Adapters whose own shutdown reported since leave custody with their
        // outcomes, and an adapter custody alone still owns is shut down, so
        // custody keeps no adapter longer than its closes need.
        state.release_reported();
        state.entries.push(CustodyEntry {
            adapter: Arc::clone(adapter),
            shutdown: adapter.shutdown_observer(),
            shutdown_started: false,
            missed_deadline: false,
        });
        Ok(())
    }
}

/// The class `outcome` counts in, or `None` when it settled. Settled means
/// the final facts Meerkat reports ([`McpServerCloseOutcome::is_settled`]),
/// never a phase.
pub(crate) fn unsettled_class(outcome: &McpServerCloseOutcome) -> Option<McpCloseUnsettledClass> {
    if outcome.is_settled() {
        return None;
    }
    Some(match outcome {
        McpServerCloseOutcome::Stdio(receipt) => match (&receipt.leader, &receipt.witness) {
            (McpStdioLeaderOutcome::WaitFailed(_), _) => McpCloseUnsettledClass::WaitFailed,
            (McpStdioLeaderOutcome::NotReaped, _) => McpCloseUnsettledClass::NotReaped,
            (_, McpStdioWitnessOutcome::StillOpen) => McpCloseUnsettledClass::StdoutOpen,
            // Absent (off Unix included), ReadError, or a witness this
            // version cannot read: nothing showed that every stdout holder
            // closed it.
            _ => McpCloseUnsettledClass::StdoutUnobserved,
        },
        McpServerCloseOutcome::NotHeld(_) => McpCloseUnsettledClass::NotHeld,
        McpServerCloseOutcome::TaskFailed { .. } => McpCloseUnsettledClass::TaskFailed,
        // CloseFailed on any transport, or an outcome this version cannot
        // read.
        _ => McpCloseUnsettledClass::CloseFailed,
    })
}

/// Count every record once, in its class.
fn count_records(unsettled: &mut MemberMcpUnsettled, records: &[McpServerCloseRecord]) {
    for record in records {
        unsettled.servers += 1;
        match unsettled_class(&record.outcome) {
            None => {}
            Some(McpCloseUnsettledClass::WaitFailed) => unsettled.wait_failed += 1,
            Some(McpCloseUnsettledClass::NotReaped) => unsettled.not_reaped += 1,
            Some(McpCloseUnsettledClass::StdoutOpen) => unsettled.stdout_open += 1,
            Some(McpCloseUnsettledClass::StdoutUnobserved) => unsettled.stdout_unobserved += 1,
            Some(McpCloseUnsettledClass::NotHeld) => unsettled.not_held += 1,
            Some(McpCloseUnsettledClass::CloseFailed) => unsettled.close_failed += 1,
            Some(McpCloseUnsettledClass::TaskFailed) => unsettled.task_failed += 1,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn adapter() -> Arc<McpRouterAdapter> {
        Arc::new(McpRouterAdapter::new(
            meerkat_mcp::McpRouter::new_with_stdio_profile(
                meerkat_mcp::McpStdioLaunchProfile::trusted_host(),
            ),
        ))
    }

    #[test]
    fn the_budget_is_the_default_close_bound_plus_the_join_allowance() {
        assert_eq!(
            MEMBER_MCP_SETTLEMENT_BUDGET,
            McpStdioCloseBounds::DEFAULT.close_bound() + McpCloseTicket::JOIN_ALLOWANCE
        );
        assert_eq!(MEMBER_MCP_SETTLEMENT_BUDGET, Duration::from_secs(20));
    }

    #[tokio::test]
    async fn an_empty_custody_owns_nothing() {
        let custody = MemberMcpCustody::new();
        assert_eq!(custody.settle().await, MemberMcpSettlement::NothingOwned);
    }

    /// An adapter its agent never released settles through its close
    /// ticket, and a later settlement (the executor-exit one) repeats the
    /// same outcome from what was retained.
    #[tokio::test]
    async fn an_unreleased_adapter_settles_and_settlement_repeats() {
        let custody = MemberMcpCustody::new();
        let adapter = adapter();
        custody.adopt(&adapter).expect("adopt");
        let settled = MemberMcpSettlement::Settled {
            routers: 1,
            servers: 0,
        };
        assert_eq!(custody.settle().await, settled);
        assert_eq!(custody.settle().await, settled);
    }

    /// Custody keeps an adapter its agent dropped unreleased, so its closes
    /// are still started and joined at settlement.
    #[tokio::test]
    async fn an_adapter_dropped_unreleased_stays_in_custody_and_settles() {
        let custody = MemberMcpCustody::new();
        let adapter = adapter();
        custody.adopt(&adapter).expect("adopt");
        drop(adapter);
        assert_eq!(custody.adapters().len(), 1);
        assert_eq!(
            custody.settle().await,
            MemberMcpSettlement::Settled {
                routers: 1,
                servers: 0
            }
        );
    }

    /// Once custody is an adapter's last owner and no shutdown reported, the
    /// next adoption starts that adapter's own shutdown, so custody never
    /// keeps its servers running past their owners.
    #[tokio::test]
    async fn custody_shuts_down_an_adapter_it_alone_still_owns() {
        let custody = MemberMcpCustody::new();
        let abandoned = adapter();
        let mut shutdown = abandoned.shutdown_observer();
        custody.adopt(&abandoned).expect("adopt");
        drop(abandoned);
        custody.adopt(&adapter()).expect("adopt");
        let observed = tokio::time::timeout(Duration::from_secs(10), shutdown.observe())
            .await
            .expect("custody started the abandoned adapter's shutdown");
        assert!(
            matches!(observed, McpRouterShutdownObservation::Reported(_)),
            "{observed:?}"
        );
        assert_eq!(
            custody.settle().await,
            MemberMcpSettlement::Settled {
                routers: 2,
                servers: 0
            }
        );
    }

    /// An adapter whose own shutdown reported leaves custody at the next
    /// adoption, and its native outcomes still count.
    #[tokio::test]
    async fn a_shut_down_adapter_leaves_custody_with_its_outcomes() {
        let custody = MemberMcpCustody::new();
        let released = adapter();
        custody.adopt(&released).expect("adopt");
        released.shutdown().await;
        let live = adapter();
        custody.adopt(&live).expect("adopt");
        assert_eq!(
            Arc::strong_count(&released),
            1,
            "custody released the shut-down adapter"
        );
        assert_eq!(custody.adapters().len(), 1);
        assert_eq!(custody.len(), 2);
        assert_eq!(
            custody.settle().await,
            MemberMcpSettlement::Settled {
                routers: 2,
                servers: 0
            }
        );
    }

    /// The first settlement freezes the custody: a later adoption is refused,
    /// so the construction fails before any server exists.
    #[tokio::test]
    async fn a_settled_custody_refuses_later_adoption() {
        let custody = MemberMcpCustody::new();
        assert_eq!(custody.settle().await, MemberMcpSettlement::NothingOwned);
        assert!(custody.adopt(&adapter()).is_err());
        assert_eq!(custody.len(), 0);
    }

    fn record(outcome: McpServerCloseOutcome) -> McpServerCloseRecord {
        McpServerCloseRecord {
            server: "fixture".to_string(),
            outcome,
        }
    }

    fn receipt(
        leader: McpStdioLeaderOutcome,
        witness: McpStdioWitnessOutcome,
    ) -> McpServerCloseOutcome {
        McpServerCloseOutcome::Stdio(Arc::new(meerkat_mcp::McpStdioCloseReceipt {
            settled_in: None,
            leader_exit: meerkat_mcp::McpStdioLeaderExitObservation::NotSeen,
            witness,
            sigterm: None,
            sigkill: None,
            final_group_signal: None,
            leader,
            elapsed: Duration::from_secs(1),
        }))
    }

    fn reaped() -> McpStdioLeaderOutcome {
        use std::os::unix::process::ExitStatusExt as _;
        McpStdioLeaderOutcome::Reaped(std::process::ExitStatus::from_raw(9))
    }

    /// Settled means the final facts: a reaped leader and a closed stdout
    /// witness, a process-free transport closed, or a server never spawned.
    /// Every other outcome counts in its own class, never as settled; a
    /// missing witness (off Unix too) is unobserved, not success, and only an
    /// actual close error is `close_failed`.
    #[test]
    fn every_unsettled_outcome_counts_in_its_own_class() {
        let kill = meerkat_mcp::McpStdioClosePhase::Kill;
        let records = vec![
            record(receipt(reaped(), McpStdioWitnessOutcome::Closed(kill))),
            record(McpServerCloseOutcome::NoProcess),
            record(McpServerCloseOutcome::NeverSpawned),
            record(receipt(
                McpStdioLeaderOutcome::WaitFailed(std::io::Error::other("reaped elsewhere")),
                McpStdioWitnessOutcome::Closed(kill),
            )),
            record(receipt(
                McpStdioLeaderOutcome::NotReaped,
                McpStdioWitnessOutcome::Closed(kill),
            )),
            record(receipt(reaped(), McpStdioWitnessOutcome::StillOpen)),
            record(receipt(reaped(), McpStdioWitnessOutcome::Absent)),
            record(receipt(
                reaped(),
                McpStdioWitnessOutcome::ReadError {
                    phase: kill,
                    kind: std::io::ErrorKind::Other,
                },
            )),
            record(McpServerCloseOutcome::NotHeld(
                meerkat_mcp::McpCustodyNotHeld::AbandonedElsewhere,
            )),
            record(McpServerCloseOutcome::TaskFailed { panicked: true }),
            record(McpServerCloseOutcome::CloseFailed(Arc::new(
                meerkat_mcp::McpError::ConnectionFailed {
                    reason: "cancel did not finish".to_string(),
                },
            ))),
        ];
        let mut unsettled = MemberMcpUnsettled::default();
        count_records(&mut unsettled, &records);
        assert_eq!(unsettled.servers, 11);
        assert_eq!(
            (
                unsettled.wait_failed,
                unsettled.not_reaped,
                unsettled.stdout_open,
                unsettled.stdout_unobserved,
                unsettled.not_held,
                unsettled.task_failed,
                unsettled.close_failed,
                unsettled.deadline,
            ),
            (1, 1, 1, 2, 1, 1, 1, 0)
        );
    }

    #[cfg(target_os = "linux")]
    mod with_servers {
        use std::sync::Arc;
        use std::time::Duration;

        use meerkat::{AgentFactory, FactoryAgentBuilder};
        use meerkat_core::AgentToolDispatcher as _;
        use meerkat_session::SessionAgentBuilder as _;
        use tokio::task::JoinHandle;

        use super::super::*;
        use crate::test_mcp_fixture::{
            FixtureMode, McpFixture, ProcessState, process_state, recorded_pid,
            runtime_with_member_server, wait_for_recorded_pid,
        };

        const STARTUP: Duration = Duration::from_mins(1);

        fn request_with(
            servers: Vec<meerkat_core::McpServerConfig>,
        ) -> meerkat_core::service::CreateSessionRequest {
            let build = meerkat_core::service::SessionBuildOptions {
                mcp_servers: servers,
                ..Default::default()
            };
            meerkat_core::service::CreateSessionRequest {
                model: "mock-model".to_string(),
                prompt: meerkat_core::ContentInput::Text("noop".to_string()),
                system_prompt: meerkat_core::config::SystemPromptOverride::Inherit,
                max_tokens: None,
                event_tx: None,
                initial_turn: meerkat_core::service::InitialTurnPolicy::Defer,
                build: Some(build),
                labels: None,
                deferred_prompt_policy: meerkat_core::service::DeferredPromptPolicy::Discard,
                injected_context: Vec::new(),
            }
        }

        fn builder(factory: AgentFactory) -> FactoryAgentBuilder {
            let mut builder = FactoryAgentBuilder::new(factory, meerkat::Config::default());
            builder.default_llm_client = Some(Arc::new(meerkat_client::TestClient::default()));
            builder
        }

        struct Refusing;

        impl meerkat::McpAdapterComposer for Refusing {
            fn compose(
                &self,
                _adapter: &Arc<McpRouterAdapter>,
                _session_id: &meerkat_core::types::SessionId,
                _member: Option<&meerkat_core::MobMemberBinding>,
            ) -> Result<Arc<dyn meerkat_core::AgentToolDispatcher>, meerkat::McpAdapterComposeRefusal>
            {
                Err(meerkat::McpAdapterComposeRefusal::new(
                    "refused for the test",
                ))
            }
        }

        /// A construction that fails after its adapter was adopted (here the
        /// host composer refuses, after staging) is still in custody, and
        /// settles with its native outcomes.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_construction_failing_after_adoption_is_settled_in_custody() {
            let fixture = McpFixture::new();
            let custody = MemberMcpCustody::new();
            let factory = custody.install(
                AgentFactory::new(fixture.dir.path().join("sessions"))
                    .with_mcp_adapter_composer(Arc::new(Refusing)),
            );
            let (event_tx, _event_rx) = tokio::sync::mpsc::channel(8);
            let built = builder(factory)
                .build_agent(
                    &request_with(vec![fixture.server("stubborn", FixtureMode::Stubborn)]),
                    event_tx,
                )
                .await;
            assert!(built.is_err(), "the host composer refused the build");
            assert_eq!(custody.len(), 1, "the adapter was adopted before staging");
            let settlement = custody.settle().await;
            assert!(settlement.is_settled(), "{settlement:?}");
            if let Some(pid) = recorded_pid(&fixture.pid_path()) {
                assert_eq!(process_state(pid), ProcessState::Gone);
            }
        }

        /// Adoption after the freeze is refused at the seam, so the build
        /// fails before any server process exists.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_build_after_the_freeze_fails_before_any_server_starts() {
            let fixture = McpFixture::new();
            let custody = MemberMcpCustody::new();
            assert_eq!(custody.settle().await, MemberMcpSettlement::NothingOwned);
            let factory = custody.install(AgentFactory::new(fixture.dir.path().join("sessions")));
            let (event_tx, _event_rx) = tokio::sync::mpsc::channel(8);
            let built = builder(factory)
                .build_agent(
                    &request_with(vec![fixture.server("stubborn", FixtureMode::Stubborn)]),
                    event_tx,
                )
                .await;
            assert!(built.is_err(), "a frozen custody refuses the construction");
            tokio::time::sleep(Duration::from_millis(500)).await;
            assert_eq!(
                recorded_pid(&fixture.pid_path()),
                None,
                "no server process was started"
            );
        }

        /// Start a call on the custody's adapter that its `HangCall` server
        /// never answers, and prove the call holds the router's read guard:
        /// the call reached the server (its marker is written), it has not
        /// returned, and a writer queued after it cannot enter. The only path
        /// from a dispatch to the server holds `router.read()` across the call
        /// (`McpRouterAdapter::dispatch_with_context`). Returns the call (true
        /// when it succeeded) and the queued writer.
        async fn hold_the_router(
            fixture: &McpFixture,
            custody: &MemberMcpCustody,
        ) -> (JoinHandle<bool>, JoinHandle<()>) {
            let adapter = custody
                .adapters()
                .into_iter()
                .next()
                .expect("the adapter is in custody");
            // Nothing else drives a pending connection here: settle it, so the
            // server's tool is listed.
            adapter
                .wait_until_ready(STARTUP)
                .await
                .expect("the server connected");
            let tool = adapter
                .tools()
                .first()
                .map(|tool| tool.name.to_string())
                .expect("the server's tool is listed");
            let call = {
                let adapter = Arc::clone(&adapter);
                tokio::spawn(async move {
                    let args =
                        serde_json::value::RawValue::from_string("{}".to_string()).expect("args");
                    meerkat_core::AgentToolDispatcher::dispatch(
                        &*adapter,
                        meerkat_core::ToolCallView {
                            id: "call-1",
                            name: &tool,
                            args: &args,
                        },
                    )
                    .await
                    .is_ok()
                })
            };
            wait_for_recorded_pid(&fixture.call_marker_path(), STARTUP)
                .await
                .expect("the call reached the server");
            let writer = {
                let adapter = Arc::clone(&adapter);
                tokio::spawn(async move {
                    let _ = adapter.poll_lifecycle_actions().await;
                })
            };
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(
                !writer.is_finished(),
                "a writer is held off: the call holds the router's read guard"
            );
            assert!(
                !call.is_finished(),
                "the call is still in flight when the close starts"
            );
            (call, writer)
        }

        /// The held call ends with an error once its server is closed, and
        /// the queued writer then enters.
        async fn assert_released(call: JoinHandle<bool>, writer: JoinHandle<()>) {
            let call_succeeded = tokio::time::timeout(STARTUP, call)
                .await
                .expect("the held call ends once its server is closed")
                .expect("the call task");
            assert!(!call_succeeded, "the closed server's call fails");
            tokio::time::timeout(STARTUP, writer)
                .await
                .expect("the writer enters once the call released the router")
                .expect("the writer task");
        }

        /// Delayed initiation at the custody: while a call holds the router's
        /// read guard, settlement still starts the close at once; the close
        /// takes its full escalation (the server ignores EOF and SIGTERM) and
        /// settles within the budget; the held call then fails.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_close_starts_while_a_dispatch_holds_the_router() {
            let fixture = McpFixture::new();
            let custody = MemberMcpCustody::new();
            let factory = custody.install(AgentFactory::new(fixture.dir.path().join("sessions")));
            let (event_tx, _event_rx) = tokio::sync::mpsc::channel(8);
            let _built = builder(factory)
                .build_agent(
                    &request_with(vec![fixture.server("hanging", FixtureMode::HangCall)]),
                    event_tx,
                )
                .await
                .expect("build");
            let pid = wait_for_recorded_pid(&fixture.pid_path(), STARTUP)
                .await
                .expect("the server started");
            let (call, writer) = hold_the_router(&fixture, &custody).await;
            let started = tokio::time::Instant::now();
            let settlement = custody.settle().await;
            assert_eq!(
                settlement,
                MemberMcpSettlement::Settled {
                    routers: 1,
                    servers: 1
                }
            );
            assert!(started.elapsed() <= MEMBER_MCP_SETTLEMENT_BUDGET);
            assert_eq!(process_state(pid), ProcessState::Gone);
            assert_released(call, writer).await;
        }

        /// Delayed initiation at the runtime: a member's call holds its
        /// router's read guard when the runtime shuts down. The runtime's
        /// teardown cannot release that guard (the call is not the member's
        /// turn), so the close starts under it, from the member's release or
        /// the settlement, whichever begins it first (neither takes the
        /// router lock). Shutdown reports the server settled and cleanup
        /// complete only once the native attempt reported it reaped with its
        /// stdout closed.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn shutdown_settles_a_member_server_whose_call_holds_the_router() {
            let fixture = McpFixture::new();
            let custody = MemberMcpCustody::new();
            let (runtime, pid) =
                runtime_with_member_server(&fixture, FixtureMode::HangCall, &custody).await;
            let (call, writer) = hold_the_router(&fixture, &custody).await;
            let report = runtime.shutdown().await;
            assert!(
                matches!(
                    report.member_mcp_settlement,
                    MemberMcpSettlement::Settled { servers: 1, .. }
                ),
                "{:?}",
                report.member_mcp_settlement
            );
            assert!(report.cleanup_completed(), "{report:?}");
            assert_eq!(process_state(pid), ProcessState::Gone);
            assert_released(call, writer).await;
        }

        /// The counterexample at the runtime: a server that ignores EOF and
        /// SIGTERM outlives the 5 s member teardown. Shutdown now reports
        /// complete cleanup only once the native attempt reported the server
        /// reaped and its stdout closed.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn shutdown_reports_a_stubborn_member_server_settled_and_reaped() {
            let fixture = McpFixture::new();
            let custody = MemberMcpCustody::new();
            let (runtime, pid) =
                runtime_with_member_server(&fixture, FixtureMode::Stubborn, &custody).await;
            let report = runtime.shutdown().await;
            assert!(
                matches!(
                    report.member_mcp_settlement,
                    MemberMcpSettlement::Settled { servers: 1, .. }
                ),
                "{:?}",
                report.member_mcp_settlement
            );
            assert!(report.cleanup_completed(), "{report:?}");
            assert_eq!(process_state(pid), ProcessState::Gone);
        }

        /// A holder of the server's stdout outside its group keeps the close
        /// from settling. The native attempt runs its whole bound and returns
        /// `stdout_open` (never `deadline`), with the direct leader reaped. No
        /// claim is made about any other descendant.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn shutdown_reports_an_escaped_stdout_holder_from_the_native_attempt() {
            let fixture = McpFixture::new();
            let custody = MemberMcpCustody::new();
            let (runtime, pid) =
                runtime_with_member_server(&fixture, FixtureMode::Escaping, &custody).await;
            let escaped = wait_for_recorded_pid(&fixture.escaped_pid_path(), STARTUP)
                .await
                .expect("the stdout holder started");
            let started = tokio::time::Instant::now();
            let report = runtime.shutdown().await;
            let MemberMcpSettlement::Unsettled(unsettled) = &report.member_mcp_settlement else {
                panic!("an escaped stdout holder settled: {report:?}");
            };
            assert_eq!(
                (
                    unsettled.servers,
                    unsettled.stdout_open,
                    unsettled.deadline,
                    unsettled.not_reaped,
                    unsettled.wait_failed,
                ),
                (1, 1, 0, 0, 0),
                "{unsettled:?}"
            );
            assert!(
                started.elapsed() >= McpStdioCloseBounds::DEFAULT.close_bound(),
                "the native attempt reported only after its whole bound"
            );
            assert!(!report.cleanup_completed());
            assert_eq!(process_state(pid), ProcessState::Gone);
            assert!(matches!(process_state(escaped), ProcessState::Live(_)));
        }

        /// A deadline ends an observation, never the exit boundary's wait.
        /// With every observation expiring at once, as if Meerkat broke its
        /// own bound, the runtime's settlement leaves the stubborn server's
        /// close pending under `deadline`. The executor-exit settlement then
        /// waits on that same close until its native outcome reports the
        /// server reaped, counting the router under `deadline` once, and a
        /// later settlement repeats that outcome.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn the_exit_settlement_waits_past_the_deadline_for_the_native_outcome() {
            let fixture = McpFixture::new();
            let custody = MemberMcpCustody::new();
            let factory = custody.install(AgentFactory::new(fixture.dir.path().join("sessions")));
            let (event_tx, _event_rx) = tokio::sync::mpsc::channel(8);
            let _built = builder(factory)
                .build_agent(
                    &request_with(vec![fixture.server("stubborn", FixtureMode::Stubborn)]),
                    event_tx,
                )
                .await
                .expect("build");
            let pid = wait_for_recorded_pid(&fixture.pid_path(), STARTUP)
                .await
                .expect("the server started");
            custody.expire_observations_for_test();
            let pending = MemberMcpUnsettled {
                routers: 1,
                deadline: 1,
                ..MemberMcpUnsettled::default()
            };
            assert_eq!(
                custody.settle().await,
                MemberMcpSettlement::Unsettled(pending)
            );
            assert!(
                matches!(process_state(pid), ProcessState::Live(_)),
                "the close is still in its native attempt"
            );
            let joined = MemberMcpUnsettled {
                routers: 1,
                servers: 1,
                deadline: 1,
                ..MemberMcpUnsettled::default()
            };
            assert_eq!(
                custody.settle_to_exit().await,
                MemberMcpSettlement::Unsettled(joined.clone())
            );
            assert_eq!(process_state(pid), ProcessState::Gone);
            assert_eq!(
                custody.settle().await,
                MemberMcpSettlement::Unsettled(joined)
            );
        }
    }
}
