//! Shared fixtures for held identity bootstrap tests inside the crate: a
//! governed runtime whose fresh members a selecting host registers and
//! finishes under one application tool policy, so they can be held.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::BTreeMap;
use std::sync::Arc;

use meerkat_authorization::clock::HostAuthorizationClock;
use meerkat_authorization::grant_policy::{
    AdmittedWorkPolicyOwner, OperationPolicyOwner, WorkOwnerAllowance,
};
use meerkat_authorization::grants::{LocalGrantAuthority, LocalGrantConfiguration};
use meerkat_authorization::policy::{LocalPolicyAllowance, LocalPolicyPurpose};
use meerkat_authorization::publication::LocalAuthorizationPublication;
use meerkat_authorization_contracts::evidence::EvidenceId;
use meerkat_authorization_contracts::work_association::InputAuthorityAssociation;
use meerkat_core::auth::{PrincipalKind, PrincipalRef};
use meerkat_core::{OperationAuthorizationError, PreparedAuthorizationBinding};
use meerkat_runtime::input_authority::NativeAdmissionError;
use meerkat_runtime::meerkat_machine::NativeGrantWorkConfiguration;

use crate::UnifiedRuntime;
use crate::identity_first::{
    AgentAddressability, AgentIdentity, CheckpointVersion, ContinuityGeneration,
    ContinuityIncrementalSessions, ContinuityRecord, ContinuityResolveState, ContinuityStore,
    ContinuityStoreError, DurabilityPolicy, DurableAgentSpec, FencingToken,
    IdentityFirstRuntimeContext, IdentityRuntime, IdentityRuntimeConfig, LocalContinuityStore,
    LocalLeaseProvider, MobSessionBridge, RosterContext, RosterError, RosterProvider,
    SessionSnapshot, SessionSnapshotMatchCandidate,
};

pub(crate) struct RefusingOwner;

impl AdmittedWorkPolicyOwner for RefusingOwner {
    fn authorize_admitted_work(
        &self,
        _association: &InputAuthorityAssociation,
        _binding: &PreparedAuthorizationBinding,
        _purpose: LocalPolicyPurpose,
        _now_ms: u64,
    ) -> Result<WorkOwnerAllowance, OperationAuthorizationError> {
        Err(OperationAuthorizationError::Unavailable)
    }
}

impl OperationPolicyOwner for RefusingOwner {
    fn authorize_operation(
        &self,
        _association: &InputAuthorityAssociation,
        _binding: &PreparedAuthorizationBinding,
        _purpose: LocalPolicyPurpose,
        _now_ms: u64,
    ) -> Result<LocalPolicyAllowance, OperationAuthorizationError> {
        Err(OperationAuthorizationError::Unavailable)
    }
}

pub(crate) fn principal(kind: PrincipalKind, name: &str) -> PrincipalRef {
    PrincipalRef::in_domain(
        kind,
        name,
        meerkat_core::TrustDomainId::new("meta-held-race").expect("trust domain"),
    )
    .expect("principal")
}

pub(crate) fn host_configuration() -> NativeGrantWorkConfiguration {
    let ingress: Arc<meerkat_runtime::meerkat_machine::NativeIngressCheck> =
        Arc::new(|_, _, _, _| -> Result<(), NativeAdmissionError> {
            Err(OperationAuthorizationError::Unavailable.into())
        });
    NativeGrantWorkConfiguration {
        grants: Arc::new(
            LocalGrantAuthority::new(
                LocalGrantConfiguration {
                    root: principal(PrincipalKind::ServiceAccount, "grant-owner"),
                    namespace: EvidenceId::new("meta-held-race-grants").expect("namespace"),
                    generation: 1,
                },
                LocalAuthorizationPublication::new(),
                Arc::new(HostAuthorizationClock),
            )
            .expect("host grant owner"),
        ),
        ingress,
        invocation_owner: Arc::new(RefusingOwner),
        operation_owner: Arc::new(RefusingOwner),
        retained_resume: None,
        require_scoped_destination: true,
        configured_mcp_attachment: None,
    }
}

pub(crate) struct AllowsEverything;

impl meerkat_core::ToolConsequencePolicySnapshot for AllowsEverything {
    fn provenance(&self) -> meerkat_core::PolicyEvaluationProvenance {
        meerkat_core::PolicyEvaluationProvenance {
            revision: meerkat_core::PolicyRevision(1),
            digest: meerkat_core::PolicyDigest::from_canonical_bytes(b"allows-everything"),
        }
    }

    fn evaluate(
        &self,
        _request: &meerkat_core::ToolConsequenceRequest,
    ) -> meerkat_core::ToolConsequenceVerdict {
        meerkat_core::ToolConsequenceVerdict::Allow
    }
}

pub(crate) struct HostPolicy(pub(crate) meerkat_core::PolicyProviderId);

impl meerkat_core::ToolConsequenceNarrowingPolicy for HostPolicy {
    fn provider_id(&self) -> &meerkat_core::PolicyProviderId {
        &self.0
    }

    fn generation(&self) -> meerkat_core::PolicyProviderGeneration {
        meerkat_core::PolicyProviderGeneration(1)
    }

    fn snapshot(
        &self,
        _policy_id: &meerkat_core::PolicyId,
    ) -> Result<
        Arc<dyn meerkat_core::ToolConsequencePolicySnapshot>,
        meerkat_core::ToolConsequenceFailure,
    > {
        Ok(Arc::new(AllowsEverything))
    }
}

pub(crate) struct SelectingHook;

pub(crate) fn destination(
    identity: &meerkat_mob::MemberRuntimeBuildIdentity,
) -> meerkat_mob::MemberDestinationSelection {
    let owner = principal(
        PrincipalKind::SharedAgent,
        identity.member().member.as_str(),
    );
    meerkat_mob::MemberDestinationSelection {
        logical_owner: owner.clone(),
        context: identity.session_document().clone(),
        context_generation: 1,
        audience: meerkat_mob::AudienceRef::Principal { principal: owner },
    }
}

#[async_trait::async_trait]
impl meerkat_mob::MemberRuntimeRegistrationHook for SelectingHook {
    async fn select_destination(
        &self,
        identity: &meerkat_mob::MemberRuntimeBuildIdentity,
    ) -> Result<meerkat_mob::MemberDestinationSelection, meerkat_mob::MobError> {
        Ok(destination(identity))
    }

    async fn expected_destination(
        &self,
        identity: &meerkat_mob::MemberRuntimeBuildIdentity,
    ) -> Result<meerkat_mob::MemberDestinationSelection, meerkat_mob::MobError> {
        Ok(destination(identity))
    }

    async fn verify_member_surface(
        &self,
        _identity: &meerkat_mob::MemberRuntimeBuildIdentity,
        _surface: &[meerkat_core::ToolDef],
    ) -> Result<(), meerkat_mob::MobError> {
        Ok(())
    }

    async fn prepare_member_surface(
        &self,
        _identity: &meerkat_mob::MemberRuntimeBuildIdentity,
        surface: &meerkat_core::PreparedToolSurface,
    ) -> Result<meerkat_core::ToolSurfaceSelection, meerkat_mob::MobError> {
        Ok(meerkat_core::ToolSurfaceSelection {
            binding: meerkat_core::ApplicationToolPolicyBinding::Provider {
                provider_id: meerkat_core::PolicyProviderId::new("host").expect("provider id"),
                policy_id: meerkat_core::PolicyId::new("member-tools").expect("policy id"),
            },
            compiled_against: surface.clone(),
        })
    }
}

pub(crate) struct Roster(pub(crate) Vec<DurableAgentSpec>);

#[async_trait::async_trait]
impl RosterProvider for Roster {
    async fn roster(&self, _context: &RosterContext) -> Result<Vec<DurableAgentSpec>, RosterError> {
        Ok(self.0.clone())
    }
}

pub(crate) fn member(identity: &AgentIdentity) -> DurableAgentSpec {
    DurableAgentSpec {
        identity: identity.clone(),
        profile: "worker".into(),
        addressability: AgentAddressability::Addressable,
        display_name: None,
        labels: BTreeMap::new(),
        context: None,
        additional_instructions: Vec::new(),
        initial_message: None,
        runtime_mode_override: Some(meerkat_mob::MobRuntimeMode::TurnDriven),
        backend: None,
        binding: None,
        placement: None,
    }
}

/// The fake OpenAI key these fixtures' members resolve their provider
/// identity with: the same value the gateway fixtures give their child
/// processes, not a secret.
const FIXTURE_OPENAI_KEY: &str = "sk-regression-test";

/// Agent config whose global realm binds OpenAI to [`FIXTURE_OPENAI_KEY`].
/// Members built on it resolve their provider identity from the fixture,
/// never from an ambient `OPENAI_API_KEY`; they never call the provider.
fn fixture_provider_config() -> meerkat::Config {
    let mut config = meerkat::Config::default();
    config.realm.insert(
        meerkat_core::connection::GLOBAL_REALM_SLUG.to_string(),
        meerkat_core::RealmConfigSection::from_inline_api_keys(&[("openai", FIXTURE_OPENAI_KEY)]),
    );
    config
}

/// A governed runtime whose fresh members can be held, not yet carrying an
/// identity context.
pub(crate) async fn governed_runtime(dir: &tempfile::TempDir, mob_prefix: &str) -> UnifiedRuntime {
    let definition = meerkat_mob::MobDefinition::from_toml(&format!(
        "[mob]\nid = \"{mob_prefix}-{}\"\n\n[profiles.worker]\nmodel = \"gpt-5.5\"\nruntime_mode = \"turn_driven\"\n\n[profiles.worker.tools]\ncomms = true\n",
        uuid::Uuid::new_v4().simple()
    ))
    .expect("mob definition");
    UnifiedRuntime::builder()
        .definition(definition)
        .meerkat_config(fixture_provider_config())
        .persistent_state(dir.path().join("state"))
        .native_grant_work_configuration(host_configuration())
        .member_runtime_registration_hook(Arc::new(SelectingHook))
        .tool_consequence_policy_registry(Arc::new(
            meerkat_core::ToolConsequencePolicyRegistry::new(
                vec![Arc::new(HostPolicy(
                    meerkat_core::PolicyProviderId::new("host").expect("provider id"),
                ))],
                meerkat_core::PolicyEvaluationSupervisorConfig::default(),
                None,
            )
            .expect("host policy registry"),
        ))
        .build()
        .await
        .expect("build")
}

/// The identity runtime for `runtime`'s mob over `continuity`.
pub(crate) fn identity_runtime(
    runtime: &UnifiedRuntime,
    continuity: Arc<dyn ContinuityStore>,
    instance: &str,
) -> Arc<IdentityRuntime> {
    Arc::new(IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: continuity,
        lease_provider: Arc::new(LocalLeaseProvider::new()),
        runtime_instance_id: instance.to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(Arc::new(MobSessionBridge::new(runtime.mob_handle()))),
        default_timeout: None,
    }))
}

/// Install `identity_runtime` with `roster` as `runtime`'s identity context.
pub(crate) fn install_roster(
    runtime: &mut UnifiedRuntime,
    identity_runtime: Arc<IdentityRuntime>,
    roster: &[DurableAgentSpec],
) {
    runtime.install_identity_first_context(Arc::new(IdentityFirstRuntimeContext::new(
        identity_runtime,
        Arc::new(Roster(roster.to_vec())),
        None,
        None,
        Some(runtime.mob_handle().definition().clone()),
    )));
}

/// A governed runtime with `roster` installed as its identity context, not
/// bootstrapped, over a fresh in-memory continuity store.
pub(crate) async fn held_runtime(
    dir: &tempfile::TempDir,
    mob_prefix: &str,
    roster: &[DurableAgentSpec],
) -> (UnifiedRuntime, Arc<LocalContinuityStore>) {
    let continuity = Arc::new(LocalContinuityStore::in_memory().expect("continuity store"));
    let runtime = held_runtime_over(
        dir,
        mob_prefix,
        roster,
        Arc::clone(&continuity) as Arc<dyn ContinuityStore>,
    )
    .await;
    (runtime, continuity)
}

/// [`held_runtime`] over the given continuity store.
pub(crate) async fn held_runtime_over(
    dir: &tempfile::TempDir,
    mob_prefix: &str,
    roster: &[DurableAgentSpec],
    continuity: Arc<dyn ContinuityStore>,
) -> UnifiedRuntime {
    let mut runtime = governed_runtime(dir, mob_prefix).await;
    let identity_runtime = identity_runtime(&runtime, continuity, mob_prefix);
    install_roster(&mut runtime, identity_runtime, roster);
    runtime
}

/// [`held_runtime_over`] whose identity runtime publishes each roster
/// identity's customizer tools before a prepared lift, through `customizer`.
pub(crate) async fn held_runtime_with_customizer(
    dir: &tempfile::TempDir,
    mob_prefix: &str,
    roster: &[DurableAgentSpec],
    continuity: Arc<dyn ContinuityStore>,
    customizer: Arc<dyn crate::identity_first::AgentCustomizer>,
) -> UnifiedRuntime {
    let mut runtime = governed_runtime(dir, mob_prefix).await;
    let identity_runtime = identity_runtime(&runtime, continuity, mob_prefix);
    identity_runtime
        .set_customizer_tool_registry(Some(crate::identity_first::CustomizerToolRegistry::new()))
        .await;
    identity_runtime
        .set_agent_customizer(Some(customizer))
        .await;
    install_roster(&mut runtime, identity_runtime, roster);
    runtime
}

/// Stage an activation obligation for `runtime`'s already running mob, as a
/// prepared (persistent) restart stages one for its Stopped mob: the prepared
/// bootstrap steps before the lift run, and the lift itself is a no-op.
pub(crate) fn stage_running_mob_activation(runtime: &mut UnifiedRuntime) {
    let pending = crate::mob_handle_runtime::PendingMobActivation::already_running_for_test(
        &runtime.mob_handle(),
    );
    *runtime.pending_mob_activation.get_mut() = Some(pending);
}

/// One parked read: the receiver told when it parks, and the release.
type ParkedRead = (
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);

/// An in-memory continuity store whose next `resolve_many` can be parked
/// until the test releases it: a blocked store under the setup steps that
/// read it. Everything else is the local store's own behaviour.
pub(crate) struct ParkingContinuityStore {
    inner: LocalContinuityStore,
    park: std::sync::Mutex<Option<ParkedRead>>,
    /// Set once a parked read has returned from the local store: its exit
    /// witness, so a dropped (cancelled) read is told apart from a settled one.
    parked_returned: std::sync::atomic::AtomicBool,
}

impl ParkingContinuityStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: LocalContinuityStore::in_memory().expect("continuity store"),
            park: std::sync::Mutex::new(None),
            parked_returned: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Whether a parked read has returned from the local store.
    pub(crate) fn parked_read_returned(&self) -> bool {
        self.parked_returned
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Park the next `resolve_many` until the returned sender is used or
    /// dropped; the returned receiver is told when a read has parked.
    pub(crate) fn park_next_resolve(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *self.park.lock().unwrap() = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }
}

#[async_trait::async_trait]
impl ContinuityStore for ParkingContinuityStore {
    async fn resolve_many(
        &self,
        identities: &[AgentIdentity],
    ) -> Result<BTreeMap<AgentIdentity, ContinuityResolveState>, ContinuityStoreError> {
        let parked = self.park.lock().unwrap().take();
        let was_parked = parked.is_some();
        if let Some((entered, release)) = parked {
            let _ = entered.send(());
            let _ = release.await;
        }
        let resolved = self.inner.resolve_many(identities).await;
        if was_parked {
            self.parked_returned
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        resolved
    }

    async fn resolve_record_by_session(
        &self,
        session_id: &meerkat_core::types::SessionId,
    ) -> Result<Option<(ContinuityRecord, FencingToken, CheckpointVersion)>, ContinuityStoreError>
    {
        self.inner.resolve_record_by_session(session_id).await
    }

    async fn load_session_snapshot(
        &self,
        session_id: &meerkat_core::types::SessionId,
    ) -> Result<Option<SessionSnapshot>, ContinuityStoreError> {
        self.inner.load_session_snapshot(session_id).await
    }

    async fn session_snapshot_matches_current(
        &self,
        candidate: SessionSnapshotMatchCandidate,
    ) -> Result<bool, ContinuityStoreError> {
        self.inner.session_snapshot_matches_current(candidate).await
    }

    async fn delete_session_snapshot_if_current_revision(
        &self,
        session_id: &meerkat_core::types::SessionId,
        expected_current_revision: &str,
    ) -> Result<bool, ContinuityStoreError> {
        self.inner
            .delete_session_snapshot_if_current_revision(session_id, expected_current_revision)
            .await
    }

    async fn save_session_snapshot(
        &self,
        identity: &AgentIdentity,
        session_id: &meerkat_core::types::SessionId,
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

    async fn save_session_snapshot_owned(
        &self,
        identity: AgentIdentity,
        session_id: meerkat_core::types::SessionId,
        generation: ContinuityGeneration,
        version: CheckpointVersion,
        fencing_token: FencingToken,
        snapshot: SessionSnapshot,
    ) -> Result<(), ContinuityStoreError> {
        self.inner
            .save_session_snapshot_owned(
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

    fn as_incremental_sessions(&self) -> Option<Arc<dyn ContinuityIncrementalSessions>> {
        self.inner.as_incremental_sessions()
    }

    fn store_type_name(&self) -> &'static str {
        self.inner.store_type_name()
    }
}
