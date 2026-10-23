//! The durable worker resume gate over the mob realm's WorkGraph.
//!
//! A profile that declares `durable = true` spawns durable workers. After a
//! crash, kill or restart of a persisted mob, Meerkat rebuilds such a worker
//! on its session and resumes the run it was in from its last committed
//! round. Before that resume Meerkat asks the host's
//! [`meerkat_mob::DurableResumeGate`] whether the run's work is still the
//! worker's. Work leases stay host-side: Meerkat renews no WorkGraph claim,
//! so a worker that was down longer than its lease can come back to an item
//! that was released or taken. [`WorkGraphResumeGate`] reads the mob realm's
//! WorkGraph and answers:
//!
//! - **With the run's attention projection** (the
//!   `workgraph.attention_projection` one of its bound inputs was dispatched
//!   under), the projected item decides. The run is superseded when the item
//!   is gone or terminal, when another owner holds its claim, when the
//!   worker's own claim lease has lapsed, or when the projection's attention
//!   binding has stopped or now targets another owner. It continues
//!   otherwise.
//! - **Without one**, every non-terminal item whose claim the worker holds is
//!   checked. A lapsed lease supersedes the run; otherwise, including when
//!   the worker holds no claim, the run continues.
//! - A lapsed lease is first committed with `observe_lease_expiry`, as a
//!   lease sweep would, so the item can be claimed again.
//! - A WorkGraph read fault answers unavailable. Meerkat retries the gate a
//!   bounded number of times and then refuses the resume.
//!
//! The worker is "this worker" under either of its WorkGraph names: its
//! member owner key (`agent:mob/<mob_id>/agent/<identity>`, the only form a
//! mob lowers a member identity to) or its session (`session:<id>`).
//!
//! Known limit: take a run with no attention projection whose claim lapsed
//! and was taken by another worker while it was down. It holds no claim any
//! more, so the gate cannot see the move and the run continues. Its later
//! WorkGraph writes are still revision-checked and conflict.

use chrono::{DateTime, Utc};
use meerkat::{
    AttentionBindingRequest, AttentionContextProjection, ObserveLeaseExpiryRequest,
    WorkAttentionTarget, WorkClaim, WorkGraphError, WorkGraphService, WorkItem, WorkItemFilter,
    WorkOwnerKey, WorkStatus,
};
use meerkat_core::SessionId;

/// How many non-terminal items the claim scan reads: the WorkGraph's
/// largest list page. A realm with more open items is scanned up to here,
/// with a warning.
const CLAIM_SCAN_LIMIT: usize = 1000;

/// The gate's answer before it is handed to Meerkat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResumeCheck {
    /// The worker still holds its work: resume the run.
    Continue,
    /// The work moved: Meerkat stops the run as `superseded` and settles its
    /// inputs as a stop would.
    Superseded(String),
    /// The WorkGraph could not answer: Meerkat retries, then refuses the
    /// resume.
    Unavailable(String),
}

impl ResumeCheck {
    fn into_verdict(self) -> meerkat_mob::DurableResumeVerdict {
        match self {
            Self::Continue => meerkat_mob::DurableResumeVerdict::Continue,
            Self::Superseded(reason) => meerkat_mob::DurableResumeVerdict::Superseded { reason },
            Self::Unavailable(reason) => meerkat_mob::DurableResumeVerdict::Unavailable { reason },
        }
    }
}

/// The worker whose interrupted run Meerkat is about to resume, as the gate
/// reads it off the request.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ResumingWorker<'a> {
    pub mob_id: &'a meerkat_mob::MobId,
    pub agent_identity: &'a meerkat_mob::AgentIdentity,
    pub session_id: &'a SessionId,
    pub attention_projection: Option<&'a AttentionContextProjection>,
}

/// The worker's two WorkGraph names.
struct WorkerKeys<'a> {
    /// `None` when the identity cannot be lowered to a member owner key (it
    /// contains `/`): such a member can hold no member-key claim.
    member: Option<WorkOwnerKey>,
    session: Option<WorkOwnerKey>,
    session_id: &'a SessionId,
}

impl<'a> WorkerKeys<'a> {
    fn of(worker: ResumingWorker<'a>) -> Self {
        Self {
            member: meerkat_mob::lower_agent_identity_owner_key(
                worker.mob_id,
                worker.agent_identity,
            )
            .ok(),
            session: WorkOwnerKey::session(worker.session_id.to_string()).ok(),
            session_id: worker.session_id,
        }
    }

    fn names(&self, key: &WorkOwnerKey) -> bool {
        self.member.as_ref() == Some(key) || self.session.as_ref() == Some(key)
    }

    fn is_target(&self, target: &WorkAttentionTarget) -> bool {
        match target {
            WorkAttentionTarget::Session { session_id } => session_id == self.session_id,
            WorkAttentionTarget::LoweredOwner { owner_key } => self.names(owner_key),
        }
    }
}

/// [`meerkat_mob::DurableResumeGate`] over one mob realm's WorkGraph: the
/// same mob-scoped service the mob's attention overlays read.
pub(crate) struct WorkGraphResumeGate {
    service: WorkGraphService,
}

impl WorkGraphResumeGate {
    pub(crate) fn new(service: WorkGraphService) -> Self {
        Self { service }
    }

    /// The verdict for `worker`. Never fails: a WorkGraph fault is
    /// [`ResumeCheck::Unavailable`].
    pub(crate) async fn check_worker(&self, worker: ResumingWorker<'_>) -> ResumeCheck {
        match self.decide(worker).await {
            Ok(check) => check,
            Err(error) => {
                ResumeCheck::Unavailable(format!("the WorkGraph could not be read: {error}"))
            }
        }
    }

    async fn decide(&self, worker: ResumingWorker<'_>) -> Result<ResumeCheck, WorkGraphError> {
        let keys = WorkerKeys::of(worker);
        match worker.attention_projection {
            Some(projection) => self.decide_projected(&keys, projection).await,
            None => self.decide_by_claims(&keys).await,
        }
    }

    /// The run was dispatched for one item: that item and its attention
    /// binding decide.
    async fn decide_projected(
        &self,
        keys: &WorkerKeys<'_>,
        projection: &AttentionContextProjection,
    ) -> Result<ResumeCheck, WorkGraphError> {
        let work_ref = &projection.work_ref;
        if work_ref.realm_id != self.service.default_realm_id()
            || work_ref.namespace != *self.service.default_namespace()
        {
            return Ok(ResumeCheck::Unavailable(format!(
                "the run's work item {} lives in realm '{}' namespace '{}', which this gate \
                 cannot read (it reads realm '{}' namespace '{}')",
                work_ref.item_id,
                work_ref.realm_id,
                work_ref.namespace,
                self.service.default_realm_id(),
                self.service.default_namespace(),
            )));
        }
        let item = match self
            .service
            .get(
                Some(work_ref.realm_id.clone()),
                Some(work_ref.namespace.clone()),
                work_ref.item_id.clone(),
            )
            .await
        {
            Ok(item) => item,
            Err(WorkGraphError::NotFound { .. }) => {
                return Ok(ResumeCheck::Superseded(format!(
                    "its work item {} no longer exists",
                    work_ref.item_id
                )));
            }
            Err(error) => return Err(error),
        };
        if let Some(check) = self.decide_item(keys, &item).await? {
            return Ok(check);
        }
        let binding_id = &projection.binding_id;
        let binding = match self
            .service
            .attention_binding(AttentionBindingRequest {
                binding_id: binding_id.clone(),
                realm_id: Some(work_ref.realm_id.clone()),
                namespace: Some(work_ref.namespace.clone()),
            })
            .await
        {
            Ok(result) => result.attention,
            // Terminal bindings are pruned: a missing one has ended.
            Err(WorkGraphError::AttentionNotFound { .. }) => {
                return Ok(ResumeCheck::Superseded(format!(
                    "its attention binding {binding_id} no longer exists"
                )));
            }
            Err(error) => return Err(error),
        };
        if binding.status.is_terminal() {
            return Ok(ResumeCheck::Superseded(format!(
                "its attention binding {binding_id} is {}",
                binding.status.status_key()
            )));
        }
        if !keys.is_target(&binding.target) {
            return Ok(ResumeCheck::Superseded(format!(
                "its attention binding {binding_id} now targets {}",
                binding.target.target_key()
            )));
        }
        Ok(ResumeCheck::Continue)
    }

    /// The run named no item: every claim the worker holds decides.
    async fn decide_by_claims(&self, keys: &WorkerKeys<'_>) -> Result<ResumeCheck, WorkGraphError> {
        if keys.member.is_none() && keys.session.is_none() {
            return Ok(ResumeCheck::Continue);
        }
        let items = self
            .service
            .list(WorkItemFilter {
                limit: Some(CLAIM_SCAN_LIMIT),
                ..WorkItemFilter::default()
            })
            .await?;
        if items.len() >= CLAIM_SCAN_LIMIT {
            tracing::warn!(
                realm = self.service.default_realm_id(),
                limit = CLAIM_SCAN_LIMIT,
                "durable resume gate: the realm has at least {CLAIM_SCAN_LIMIT} open items; \
                 claims beyond the first page are not checked"
            );
        }
        let held = items
            .iter()
            .filter_map(|item| {
                let claim = item.claim.as_ref()?;
                keys.names(&claim.owner.key).then_some((item, claim))
            })
            .collect::<Vec<_>>();
        if held.is_empty() {
            return Ok(ResumeCheck::Continue);
        }
        let now = self.service.store().get_store_time_utc().await?;
        // Observe every lapsed claim, not only the first: the superseded run
        // gives all of them up.
        let mut verdict = ResumeCheck::Continue;
        for (item, claim) in held {
            if let Some(lapsed) = self.lapsed_claim(item, claim, now).await
                && verdict == ResumeCheck::Continue
            {
                verdict = lapsed;
            }
        }
        Ok(verdict)
    }

    /// The item-level verdict for the run's item: terminal, claimed by
    /// another owner, or the worker's own lapsed claim. `None` when none of
    /// those holds.
    async fn decide_item(
        &self,
        keys: &WorkerKeys<'_>,
        item: &WorkItem,
    ) -> Result<Option<ResumeCheck>, WorkGraphError> {
        if let Some(terminal) = terminal_status_name(item.status) {
            return Ok(Some(ResumeCheck::Superseded(format!(
                "its work item {} is {terminal}",
                item.id
            ))));
        }
        let Some(claim) = item.claim.as_ref() else {
            return Ok(None);
        };
        if !keys.names(&claim.owner.key) {
            return Ok(Some(ResumeCheck::Superseded(format!(
                "its work item {} is claimed by {}",
                item.id,
                claim.owner.key.canonical()
            ))));
        }
        let now = self.service.store().get_store_time_utc().await?;
        Ok(self.lapsed_claim(item, claim, now).await)
    }

    /// Superseded when the worker's claim on `item` has lapsed at `now` (the
    /// store's clock, which stamped the claim). The expiry is committed
    /// first, so the item can be claimed again; a commit that fails (the item
    /// moved meanwhile) still supersedes, because the lease has lapsed all
    /// the same. A claim without a lease never lapses.
    async fn lapsed_claim(
        &self,
        item: &WorkItem,
        claim: &WorkClaim,
        now: DateTime<Utc>,
    ) -> Option<ResumeCheck> {
        let lease_expires_at = claim.lease_expires_at?;
        if lease_expires_at > now {
            return None;
        }
        if let Err(error) = self
            .service
            .observe_lease_expiry(ObserveLeaseExpiryRequest {
                id: item.id.clone(),
                realm_id: Some(item.realm_id.clone()),
                namespace: Some(item.namespace.clone()),
                expected_revision: item.revision,
                observed_at: now,
            })
            .await
        {
            tracing::warn!(
                item = %item.id,
                %error,
                "durable resume gate: the lapsed claim's expiry was not committed; the run is \
                 superseded all the same"
            );
        }
        Some(ResumeCheck::Superseded(format!(
            "its claim on work item {} lapsed at {}",
            item.id,
            lease_expires_at.to_rfc3339()
        )))
    }
}

/// The wire name of a terminal work status; `None` for a live one.
fn terminal_status_name(status: WorkStatus) -> Option<&'static str> {
    match status {
        WorkStatus::Completed => Some("completed"),
        WorkStatus::Cancelled => Some("cancelled"),
        WorkStatus::Failed => Some("failed"),
        WorkStatus::Open | WorkStatus::InProgress | WorkStatus::Blocked => None,
    }
}

#[async_trait::async_trait]
impl meerkat_mob::DurableResumeGate for WorkGraphResumeGate {
    async fn check(
        &self,
        request: &meerkat_mob::DurableResumeRequest,
    ) -> meerkat_mob::DurableResumeVerdict {
        let check = self
            .check_worker(ResumingWorker {
                mob_id: &request.mob_id,
                agent_identity: &request.agent_identity,
                session_id: &request.session_id,
                attention_projection: request.attention_projection.as_ref(),
            })
            .await;
        match &check {
            ResumeCheck::Continue => tracing::debug!(
                mob_id = %request.mob_id,
                agent_identity = %request.agent_identity,
                session_id = %request.session_id,
                run_id = ?request.run_id,
                "durable resume gate: the worker still holds its work; the run resumes"
            ),
            ResumeCheck::Superseded(reason) => tracing::info!(
                mob_id = %request.mob_id,
                agent_identity = %request.agent_identity,
                session_id = %request.session_id,
                run_id = ?request.run_id,
                %reason,
                "durable resume gate: the run's work moved; the run is superseded"
            ),
            ResumeCheck::Unavailable(reason) => tracing::warn!(
                mob_id = %request.mob_id,
                agent_identity = %request.agent_identity,
                session_id = %request.session_id,
                run_id = ?request.run_id,
                %reason,
                "durable resume gate: no answer from the WorkGraph; Meerkat retries the gate"
            ),
        }
        check.into_verdict()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use meerkat::{
        AttentionProjectionRequest, ClaimWorkItemRequest, CloseWorkItemRequest,
        DisabledWorkGraphStore, GoalAttentionTarget, GoalCreateRequest, MemoryWorkGraphStore,
        WorkItemId, WorkNamespace, WorkOwner,
    };

    use super::*;

    const MOB: &str = "durable-mob";
    const WORKER: &str = "worker-1";
    const OTHER: &str = "worker-2";

    fn mob_realm() -> String {
        meerkat_core::mob_realm_id(MOB)
            .expect("mob realm")
            .as_str()
            .to_string()
    }

    fn service() -> WorkGraphService {
        WorkGraphService::with_scope(
            Arc::new(MemoryWorkGraphStore::new()),
            mob_realm(),
            WorkNamespace::default(),
        )
    }

    fn member_key(identity: &str) -> WorkOwnerKey {
        WorkOwnerKey::mob_agent(MOB, identity).expect("member owner key")
    }

    struct Worker {
        mob_id: meerkat_mob::MobId,
        identity: meerkat_mob::AgentIdentity,
        session_id: SessionId,
    }

    impl Worker {
        fn named(identity: &str) -> Self {
            Self {
                mob_id: meerkat_mob::MobId::from(MOB),
                identity: meerkat_mob::AgentIdentity::from(identity),
                session_id: SessionId::new(),
            }
        }

        fn resuming<'a>(
            &'a self,
            projection: Option<&'a AttentionContextProjection>,
        ) -> ResumingWorker<'a> {
            ResumingWorker {
                mob_id: &self.mob_id,
                agent_identity: &self.identity,
                session_id: &self.session_id,
                attention_projection: projection,
            }
        }
    }

    /// A goal whose attention binding targets `target`, and the projection
    /// a run dispatched for it carries.
    async fn goal_for(
        service: &WorkGraphService,
        target: GoalAttentionTarget,
    ) -> (WorkItem, AttentionContextProjection) {
        let request: GoalCreateRequest = serde_json::from_value(serde_json::json!({
            "title": "durable work",
            "target": target,
        }))
        .expect("goal request");
        let created = service.create_goal(request).await.expect("create goal");
        let projection = service
            .attention_projection(AttentionProjectionRequest {
                binding_id: created.attention.binding_id.clone(),
                realm_id: None,
                namespace: None,
            })
            .await
            .expect("attention projection")
            .projection;
        (created.item, projection)
    }

    async fn plain_item(service: &WorkGraphService) -> WorkItem {
        service
            .create(
                serde_json::from_value(serde_json::json!({ "title": "plain work" }))
                    .expect("create request"),
            )
            .await
            .expect("create item")
    }

    async fn claim(
        service: &WorkGraphService,
        item: &WorkItem,
        owner: WorkOwnerKey,
        lease_expires_at: Option<DateTime<Utc>>,
    ) -> WorkItem {
        service
            .claim(ClaimWorkItemRequest {
                id: item.id.clone(),
                realm_id: None,
                namespace: None,
                expected_revision: item.revision,
                owner: WorkOwner::new(owner),
                lease_seconds: None,
                lease_expires_at,
            })
            .await
            .expect("claim")
    }

    fn in_an_hour() -> Option<DateTime<Utc>> {
        Some(Utc::now() + chrono::Duration::hours(1))
    }

    /// A lease that lapses before [`let_the_lease_lapse`] returns.
    fn lapsing_soon() -> Option<DateTime<Utc>> {
        Some(Utc::now() + chrono::Duration::milliseconds(300))
    }

    async fn let_the_lease_lapse() {
        tokio::time::sleep(Duration::from_millis(700)).await;
    }

    fn superseded_reason(check: &ResumeCheck) -> &str {
        match check {
            ResumeCheck::Superseded(reason) => reason,
            other => panic!("expected a superseded verdict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_projected_item_the_worker_still_holds_resumes() {
        let service = service();
        let worker = Worker::named(WORKER);
        let (item, projection) = goal_for(
            &service,
            GoalAttentionTarget::Owner {
                owner_key: member_key(WORKER),
            },
        )
        .await;
        claim(&service, &item, member_key(WORKER), in_an_hour()).await;

        let gate = WorkGraphResumeGate::new(service);
        assert_eq!(
            gate.check_worker(worker.resuming(Some(&projection))).await,
            ResumeCheck::Continue
        );
    }

    #[tokio::test]
    async fn a_claim_under_the_workers_session_key_is_its_own() {
        let service = service();
        let worker = Worker::named(WORKER);
        let (item, projection) = goal_for(
            &service,
            GoalAttentionTarget::Session {
                session_id: worker.session_id.clone(),
            },
        )
        .await;
        let session_key = WorkOwnerKey::session(worker.session_id.to_string()).expect("key");
        claim(&service, &item, session_key, in_an_hour()).await;

        let gate = WorkGraphResumeGate::new(service);
        assert_eq!(
            gate.check_worker(worker.resuming(Some(&projection))).await,
            ResumeCheck::Continue
        );
    }

    #[tokio::test]
    async fn a_projected_item_another_owner_claimed_supersedes_the_run() {
        let service = service();
        let worker = Worker::named(WORKER);
        let (item, projection) = goal_for(
            &service,
            GoalAttentionTarget::Owner {
                owner_key: member_key(WORKER),
            },
        )
        .await;
        claim(&service, &item, member_key(OTHER), in_an_hour()).await;

        let gate = WorkGraphResumeGate::new(service);
        let check = gate.check_worker(worker.resuming(Some(&projection))).await;
        let reason = superseded_reason(&check);
        assert!(
            reason.contains(&member_key(OTHER).canonical()),
            "the reason names the new owner: {reason}"
        );
    }

    #[tokio::test]
    async fn the_workers_lapsed_claim_is_observed_and_supersedes_the_run() {
        let service = service();
        let worker = Worker::named(WORKER);
        let (item, projection) = goal_for(
            &service,
            GoalAttentionTarget::Owner {
                owner_key: member_key(WORKER),
            },
        )
        .await;
        claim(&service, &item, member_key(WORKER), lapsing_soon()).await;
        let_the_lease_lapse().await;

        let gate = WorkGraphResumeGate::new(service.clone());
        let check = gate.check_worker(worker.resuming(Some(&projection))).await;
        assert!(
            superseded_reason(&check).contains("lapsed"),
            "the reason says the claim lapsed: {check:?}"
        );
        let after = service
            .get(None, None, item.id.clone())
            .await
            .expect("read the item");
        assert_eq!(after.claim, None, "the gate committed the lease expiry");
        assert_eq!(
            after.status,
            WorkStatus::Open,
            "the item can be claimed again"
        );
    }

    #[tokio::test]
    async fn a_projected_item_that_ended_or_is_gone_supersedes_the_run() {
        let service = service();
        let worker = Worker::named(WORKER);
        let (item, projection) = goal_for(
            &service,
            GoalAttentionTarget::Owner {
                owner_key: member_key(WORKER),
            },
        )
        .await;
        service
            .close(CloseWorkItemRequest {
                id: item.id.clone(),
                realm_id: None,
                namespace: None,
                expected_revision: item.revision,
                status: WorkStatus::Cancelled,
            })
            .await
            .expect("cancel the item");

        let gate = WorkGraphResumeGate::new(service);
        let ended = gate.check_worker(worker.resuming(Some(&projection))).await;
        assert!(
            superseded_reason(&ended).contains("cancelled"),
            "the reason names the terminal status: {ended:?}"
        );

        let mut gone = projection.clone();
        gone.work_ref.item_id = WorkItemId::generated();
        let missing = gate.check_worker(worker.resuming(Some(&gone))).await;
        assert!(
            superseded_reason(&missing).contains("no longer exists"),
            "{missing:?}"
        );
    }

    #[tokio::test]
    async fn a_binding_that_now_targets_another_owner_supersedes_the_run() {
        let service = service();
        let (_item, projection) = goal_for(
            &service,
            GoalAttentionTarget::Owner {
                owner_key: member_key(OTHER),
            },
        )
        .await;
        let gate = WorkGraphResumeGate::new(service);

        // Control: the member the binding targets resumes on the unclaimed
        // item.
        let target = Worker::named(OTHER);
        assert_eq!(
            gate.check_worker(target.resuming(Some(&projection))).await,
            ResumeCheck::Continue
        );

        let worker = Worker::named(WORKER);
        let check = gate.check_worker(worker.resuming(Some(&projection))).await;
        assert!(
            superseded_reason(&check).contains("now targets"),
            "{check:?}"
        );
    }

    #[tokio::test]
    async fn a_projection_outside_the_gates_realm_is_unavailable() {
        let service = service();
        let worker = Worker::named(WORKER);
        let (_item, mut projection) = goal_for(
            &service,
            GoalAttentionTarget::Owner {
                owner_key: member_key(WORKER),
            },
        )
        .await;
        projection.work_ref.realm_id = "another-realm".to_string();

        let gate = WorkGraphResumeGate::new(service);
        let check = gate.check_worker(worker.resuming(Some(&projection))).await;
        assert!(
            matches!(&check, ResumeCheck::Unavailable(reason) if reason.contains("another-realm")),
            "{check:?}"
        );
    }

    #[tokio::test]
    async fn without_a_projection_a_worker_holding_no_claim_resumes() {
        let service = service();
        let worker = Worker::named(WORKER);
        let item = plain_item(&service).await;
        // Someone else's claim is not this worker's business without a
        // projection.
        claim(&service, &item, member_key(OTHER), in_an_hour()).await;

        let gate = WorkGraphResumeGate::new(service);
        assert_eq!(
            gate.check_worker(worker.resuming(None)).await,
            ResumeCheck::Continue
        );
    }

    #[tokio::test]
    async fn without_a_projection_every_lapsed_claim_is_observed_and_supersedes() {
        let service = service();
        let worker = Worker::named(WORKER);
        let live = plain_item(&service).await;
        claim(&service, &live, member_key(WORKER), in_an_hour()).await;

        let gate = WorkGraphResumeGate::new(service.clone());
        assert_eq!(
            gate.check_worker(worker.resuming(None)).await,
            ResumeCheck::Continue,
            "a live claim keeps the run"
        );

        let first = plain_item(&service).await;
        let second = plain_item(&service).await;
        claim(&service, &first, member_key(WORKER), lapsing_soon()).await;
        claim(&service, &second, member_key(WORKER), lapsing_soon()).await;
        let_the_lease_lapse().await;

        let check = gate.check_worker(worker.resuming(None)).await;
        assert!(superseded_reason(&check).contains("lapsed"), "{check:?}");
        for lapsed in [&first, &second] {
            let after = service
                .get(None, None, lapsed.id.clone())
                .await
                .expect("read the item");
            assert_eq!(after.claim, None, "every lapsed claim is observed");
        }
        let kept = service
            .get(None, None, live.id.clone())
            .await
            .expect("read the live item");
        assert!(kept.claim.is_some(), "a live claim is left alone");
    }

    #[tokio::test]
    async fn a_workgraph_read_fault_is_unavailable() {
        let disabled = WorkGraphService::with_scope(
            Arc::new(DisabledWorkGraphStore),
            mob_realm(),
            WorkNamespace::default(),
        );
        let worker = Worker::named(WORKER);
        let gate = WorkGraphResumeGate::new(disabled);

        let check = gate.check_worker(worker.resuming(None)).await;
        assert!(matches!(check, ResumeCheck::Unavailable(_)), "{check:?}");

        // A real projection, read back through a store that refuses every
        // read.
        let (_item, projection) = goal_for(
            &service(),
            GoalAttentionTarget::Owner {
                owner_key: member_key(WORKER),
            },
        )
        .await;
        let projected = gate.check_worker(worker.resuming(Some(&projection))).await;
        assert!(
            matches!(projected, ResumeCheck::Unavailable(_)),
            "{projected:?}"
        );
    }

    #[test]
    fn the_verdicts_map_onto_meerkats_vocabulary() {
        assert!(matches!(
            ResumeCheck::Continue.into_verdict(),
            meerkat_mob::DurableResumeVerdict::Continue
        ));
        assert!(matches!(
            ResumeCheck::Superseded("moved".to_string()).into_verdict(),
            meerkat_mob::DurableResumeVerdict::Superseded { reason } if reason == "moved"
        ));
        assert!(matches!(
            ResumeCheck::Unavailable("down".to_string()).into_verdict(),
            meerkat_mob::DurableResumeVerdict::Unavailable { reason } if reason == "down"
        ));
    }
}
