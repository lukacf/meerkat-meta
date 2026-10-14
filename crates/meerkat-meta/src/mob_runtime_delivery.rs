//! Host-owned composition of native job and continuation delivery.

use std::sync::{Arc, RwLock};

use super::{MobSessionService, PreBuildMobSessionService, no_op_pre_build_hook};

/// Why a host could not compose or start delivery. Native refusals retain
/// their types so callers can distinguish acquisition, binding and ownership.
#[derive(Debug)]
pub enum MobRuntimeDeliveryError {
    MissingComposition,
    MissingRuntime,
    MissingBinding,
    RuntimeStoreMismatch,
    CompositionChanged,
    Runtime(meerkat_runtime::RuntimeDriverError),
    Binding(meerkat_mob_mcp::BindContinuationsError),
    AlreadyArmed(meerkat_runtime::RuntimeDeliveryOwnerAlreadyArmed),
}

impl std::fmt::Display for MobRuntimeDeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingComposition => {
                f.write_str("agent mob tools require an explicit runtime delivery composition")
            }
            Self::MissingRuntime => {
                f.write_str("runtime delivery requires an acquired runtime machine")
            }
            Self::MissingBinding => {
                f.write_str("the replaced agent mob state has no continuation binding")
            }
            Self::RuntimeStoreMismatch => f.write_str(
                "runtime delivery and the acquired machine do not share runtime store authority",
            ),
            Self::CompositionChanged => f.write_str(
                "runtime delivery composition changed after agent mob tools were installed",
            ),
            Self::Runtime(error) => error.fmt(f),
            Self::Binding(error) => error.fmt(f),
            Self::AlreadyArmed(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for MobRuntimeDeliveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::Binding(error) => Some(error),
            Self::AlreadyArmed(error) => Some(error),
            _ => None,
        }
    }
}

impl From<meerkat_runtime::RuntimeDriverError> for MobRuntimeDeliveryError {
    fn from(error: meerkat_runtime::RuntimeDriverError) -> Self {
        Self::Runtime(error)
    }
}
impl From<meerkat_mob_mcp::BindContinuationsError> for MobRuntimeDeliveryError {
    fn from(error: meerkat_mob_mcp::BindContinuationsError) -> Self {
        Self::Binding(error)
    }
}
impl From<meerkat_runtime::RuntimeDeliveryOwnerAlreadyArmed> for MobRuntimeDeliveryError {
    fn from(error: meerkat_runtime::RuntimeDeliveryOwnerAlreadyArmed) -> Self {
        Self::AlreadyArmed(error)
    }
}

/// One host's inbox, continuation bindings and unarmed native delivery owner.
/// Clones share the inbox commit signal and binding slot. Arming two clones
/// concurrently is refused by the native inbox owner.
#[derive(Clone)]
pub struct MobRuntimeDelivery {
    inbox: meerkat_runtime::RuntimeDeliveryInbox,
    store: Option<Arc<dyn meerkat_runtime::RuntimeStore>>,
    bindings: Arc<meerkat::ContinuationHostBindings>,
    owner: meerkat::RuntimeDeliveryOwner,
    observation: Arc<RwLock<Option<DeliveryObservation>>>,
    /// The host's own committed-result owner, composed into the job source
    /// the agent mob-tool state binds.
    host_job_source: Option<meerkat_mob_mcp::HostJobSource>,
}

#[derive(Clone)]
struct DeliveryObservation {
    passes: tokio::sync::watch::Receiver<meerkat::RuntimeDeliveryPass>,
    running: std::sync::Weak<RunningMobRuntimeDelivery>,
}

impl MobRuntimeDelivery {
    /// Compose over the exact runtime-store facade and detached-job store
    /// installed on the session service and its FactoryAgentBuilder. This
    /// creates the inbox once; all producers must use [`Self::inbox`].
    pub fn new(
        store: Arc<dyn meerkat_runtime::RuntimeStore>,
        jobs: Arc<dyn meerkat::DetachedJobStore>,
    ) -> Self {
        let inbox = meerkat_runtime::RuntimeDeliveryInbox::new(Arc::clone(&store));
        Self::over(inbox, Some(store), jobs)
    }

    /// Only the stock truly ephemeral constructor declares separate memory
    /// delivery storage: its native machine intentionally has no store.
    pub(super) fn declared_ephemeral(jobs: Arc<dyn meerkat::DetachedJobStore>) -> Self {
        Self::over(
            meerkat_runtime::RuntimeDeliveryInbox::new(Arc::new(
                meerkat_runtime::InMemoryRuntimeStore::new(),
            )),
            None,
            jobs,
        )
    }

    fn over(
        inbox: meerkat_runtime::RuntimeDeliveryInbox,
        store: Option<Arc<dyn meerkat_runtime::RuntimeStore>>,
        jobs: Arc<dyn meerkat::DetachedJobStore>,
    ) -> Self {
        Self {
            owner: meerkat::RuntimeDeliveryOwner::new(jobs, inbox.clone()),
            inbox,
            store,
            bindings: Arc::default(),
            observation: Arc::default(),
            host_job_source: None,
        }
    }

    /// Compose the host's own committed-result owner, for exactly its
    /// declared `Host` namespaces, into the job source the agent mob-tool
    /// state binds over this delivery: the same delivery owner confirms the
    /// host's retained results, its fork_off and council completions, and
    /// restored rows from the first pass. The host submits its results
    /// through the bound state's `continuation_service()`.
    #[must_use]
    pub fn with_host_job_source(mut self, host: meerkat_mob_mcp::HostJobSource) -> Self {
        self.host_job_source = Some(host);
        self
    }

    pub(super) fn host_job_source(&self) -> Option<meerkat_mob_mcp::HostJobSource> {
        self.host_job_source.clone()
    }

    /// Clone the host's inbox, including its commit signal.
    pub fn inbox(&self) -> meerkat_runtime::RuntimeDeliveryInbox {
        self.inbox.clone()
    }

    /// Latest completed native pass. `None` means the owner has not armed or
    /// has not yet completed its initial reconciliation.
    pub fn last_pass(&self) -> Option<meerkat::RuntimeDeliveryPass> {
        let observation = self
            .observation
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pass = observation.as_ref()?.passes.borrow().clone();
        (pass.generation > 0).then_some(pass)
    }

    /// Whether an armed owner still runs. This is lifecycle evidence, not a
    /// claim that queued deliveries have drained.
    pub fn is_running(&self) -> bool {
        self.observation
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|value| value.running.upgrade())
            .is_some_and(|running| !running.is_stopped())
    }

    pub(super) fn shares_composition(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.bindings, &other.bindings)
    }

    /// Reject an already running owner before bootstrap creates a mob. This
    /// is only a preflight; native arm remains the final ownership authority.
    pub(super) fn preflight(&self) -> Result<(), MobRuntimeDeliveryError> {
        drop(self.inbox.claim_delivery_ownership()?);
        Ok(())
    }

    pub(super) fn validate_runtime(
        &self,
        runtime: Option<&meerkat_runtime::MeerkatMachine>,
    ) -> Result<(), MobRuntimeDeliveryError> {
        let runtime = runtime.ok_or(MobRuntimeDeliveryError::MissingRuntime)?;
        let matches = match &self.store {
            Some(store) => runtime.shares_runtime_store_authority(store),
            None => !runtime.has_runtime_persistence(),
        };
        if matches {
            Ok(())
        } else {
            Err(MobRuntimeDeliveryError::RuntimeStoreMismatch)
        }
    }

    pub(super) fn bind(
        &self,
        state: &Arc<meerkat_mob_mcp::MobMcpState>,
        replaced: Option<meerkat::ContinuationBindingGeneration>,
    ) -> Result<(), MobRuntimeDeliveryError> {
        match replaced {
            Some(generation) => {
                state.rebind_continuations(generation, self.inbox(), &self.bindings)?;
            }
            None => state.bind_continuations(self.inbox(), &self.bindings)?,
        }
        Ok(())
    }

    pub(super) fn arm(
        &self,
        service: Arc<dyn MobSessionService>,
        runtime: Arc<meerkat_runtime::MeerkatMachine>,
    ) -> Result<Arc<RunningMobRuntimeDelivery>, MobRuntimeDeliveryError> {
        self.validate_runtime(Some(&runtime))?;
        // The native generic host needs a sized service. This forwarding
        // facade contains the final composed service, including every host
        // wrapper, and adds no delivery semantics.
        let service = Arc::new(PreBuildMobSessionService {
            inner: service,
            hook: no_op_pre_build_hook(),
            dispatch_taint: None,
            after_create_hook: None,
            runtime_adapter_override: Some(Arc::clone(&runtime)),
            session_read_absorber: None,
            archived_terminal_authority: None,
        });
        let host = Arc::new(
            meerkat::surface::SessionServiceDeliveryHost::new(
                &service,
                &runtime,
                meerkat::DetachedJobService::new(self.owner.job_store()),
                None,
            )
            .with_continuation_bindings(Arc::clone(&self.bindings)),
        );
        let handle = self
            .owner
            .clone()
            .with_attachment_commits(runtime.subscribe_attachment_commits())
            .with_run_settlements(runtime.subscribe_run_settlements())
            // Subscribed before arming, as the attachment and settlement
            // signals: an announcement after the owner exists is never lost.
            .with_source_changes(self.bindings.subscribe_source_changes())
            .arm(host)?;
        let passes = handle.subscribe_passes();
        let running = Arc::new(RunningMobRuntimeDelivery {
            passes: passes.clone(),
            handle: std::sync::Mutex::new(Some(handle)),
            bindings: Arc::downgrade(&self.bindings),
            _service: service,
            _runtime: runtime,
        });
        *self
            .observation
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(DeliveryObservation {
            passes,
            running: Arc::downgrade(&running),
        });
        Ok(running)
    }
}

/// Retained by all clones of the live MobRuntime. [`Self::stop`] stops the
/// native owner explicitly when the mob stops for teardown; otherwise the
/// last clone drops the native handle, which stops its owner and releases
/// delivery ownership.
pub(super) struct RunningMobRuntimeDelivery {
    passes: tokio::sync::watch::Receiver<meerkat::RuntimeDeliveryPass>,
    handle: std::sync::Mutex<Option<meerkat::RuntimeDeliveryOwnerHandle>>,
    /// The continuation bindings the owner was armed over, held weakly for
    /// the host's [`ContinuationSourceAnnouncer`] only: it keeps neither
    /// them nor the owner alive.
    bindings: std::sync::Weak<meerkat::ContinuationHostBindings>,
    _service: Arc<PreBuildMobSessionService>,
    _runtime: Arc<meerkat_runtime::MeerkatMachine>,
}

impl RunningMobRuntimeDelivery {
    pub(super) fn last_pass(&self) -> meerkat::RuntimeDeliveryPass {
        self.passes.borrow().clone()
    }

    pub(super) fn source_announcer(&self) -> ContinuationSourceAnnouncer {
        ContinuationSourceAnnouncer {
            bindings: self.bindings.clone(),
        }
    }

    /// Stop the native owner now, even while clones of the runtime live:
    /// a stopped mob must not keep applying deliveries. Undelivered rows stay
    /// pending in the durable inbox.
    pub(super) fn stop(&self) {
        let handle = self
            .handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(handle);
    }

    fn is_stopped(&self) -> bool {
        self.handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_none_or(meerkat::RuntimeDeliveryOwnerHandle::is_stopped)
    }
}

/// Tells a runtime's continuation delivery that the custody its bound
/// continuation sources read may have changed. A host calls it once it has
/// restored that custody, after the facts are readable through the bound
/// source. The delivery owner subscribed to the same bindings then retries
/// the rows it holds blocked (see
/// [`meerkat::ContinuationHostBindings::source_custody_changed`]).
///
/// It is a hint, not a fact. It grants nothing, asserts nothing and carries
/// no source truth: the retry re-reads current custody and permissions, and
/// a row they still refuse stays where it is. Clones share one weak handle
/// to the bindings: holding one keeps neither the bindings nor the delivery
/// owner alive, and it exposes no binding, rebinding or other authority.
/// Nothing here waits for the owner to be ready.
#[derive(Clone, Debug)]
pub struct ContinuationSourceAnnouncer {
    bindings: std::sync::Weak<meerkat::ContinuationHostBindings>,
}

impl ContinuationSourceAnnouncer {
    /// Publish the hint to the delivery owner subscribed to these bindings.
    ///
    /// `true` means only that the bindings still existed and the hint was
    /// published. It does not mean the delivery owner is running, that the
    /// source custody is live, or that any row was admitted, settled or
    /// retried successfully: the bindings can outlive an explicitly stopped
    /// owner (a setup clone of the delivery composition keeps them), and
    /// then nothing acts on the hint. `false` means the bindings are gone.
    /// Neither answer is a delivery outcome: a caller must not replay,
    /// replace or fail a turn because of it.
    pub fn announce_source_custody_changed(&self) -> bool {
        match self.bindings.upgrade() {
            Some(bindings) => {
                bindings.source_custody_changed();
                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::mob_handle_runtime::{MobBootstrapSpec, MobRuntime, MobRuntimeError};

    struct Fixture {
        spec: MobBootstrapSpec,
        delivery: MobRuntimeDelivery,
        machine: Arc<meerkat_runtime::MeerkatMachine>,
        jobs: Arc<dyn meerkat::DetachedJobStore>,
    }

    fn fixture(path: &std::path::Path, store: Arc<dyn meerkat_runtime::RuntimeStore>) -> Fixture {
        let blobs: Arc<dyn meerkat_core::BlobStore> =
            Arc::new(crate::blob_store::Base64BlobStoreAdapter::new(Arc::new(
                crate::blob_store::ObjectStoreBlobStore::memory(),
            )));
        let machine = Arc::new(
            meerkat_runtime::MeerkatMachine::persistent(Arc::clone(&store), Arc::clone(&blobs))
                .expect("machine"),
        );
        let jobs: Arc<dyn meerkat::DetachedJobStore> =
            Arc::new(meerkat::MemoryDetachedJobStore::new());
        let mut builder = meerkat::FactoryAgentBuilder::new(
            meerkat::AgentFactory::new(path).builtins(false).comms(true),
            meerkat::Config::default(),
        );
        builder.default_detached_job_store = Some(Arc::clone(&jobs));
        let service = Arc::new(meerkat_session::PersistentSessionService::new(
            builder,
            8,
            Arc::new(meerkat::MemoryStore::new()),
            Arc::clone(&store),
            blobs,
        ));
        let definition = meerkat_mob::MobDefinition::from_toml(&format!(
            "[mob]\nid = \"delivery-{}\"\n\n[profiles.worker]\nmodel = \"gpt-5.5\"\n",
            uuid::Uuid::new_v4()
        ))
        .expect("definition");
        let delivery = MobRuntimeDelivery::new(store, Arc::clone(&jobs));
        let spec = MobBootstrapSpec::new(definition, meerkat_mob::MobStorage::in_memory(), service)
            .with_session_runtime_adapter(Arc::clone(&machine))
            .expect("acquire owner")
            .with_runtime_delivery(delivery.clone());
        Fixture {
            spec,
            delivery,
            machine,
            jobs,
        }
    }

    fn memory_fixture(path: &std::path::Path) -> Fixture {
        fixture(path, Arc::new(meerkat_runtime::InMemoryRuntimeStore::new()))
    }

    async fn first_pass(delivery: &MobRuntimeDelivery) -> meerkat::RuntimeDeliveryPass {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(pass) = delivery.last_pass() {
                    break pass;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("initial native owner pass")
    }

    async fn owner_released(delivery: &MobRuntimeDelivery) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if delivery.preflight().is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("native task releases inbox ownership after guard drop");
    }

    #[tokio::test]
    async fn custom_tools_require_delivery_without_publishing_a_factory() {
        let dir = tempfile::tempdir().expect("temp");
        let mut fixture = memory_fixture(dir.path());
        fixture.spec.runtime_delivery = None;
        let slot = Arc::new(std::sync::RwLock::new(None));
        assert!(matches!(
            fixture.spec.with_agent_mob_tools(Arc::clone(&slot)),
            Err(MobRuntimeDeliveryError::MissingComposition)
        ));
        assert!(slot.read().expect("slot").is_none());
    }

    #[tokio::test]
    async fn replacing_delivery_after_tool_install_refuses_before_mob_creation() {
        let dir = tempfile::tempdir().expect("temp");
        let store: Arc<dyn meerkat_runtime::RuntimeStore> =
            Arc::new(meerkat_runtime::InMemoryRuntimeStore::new());
        let fixture = fixture(dir.path(), Arc::clone(&store));
        let spec = fixture
            .spec
            .with_agent_mob_tools(Arc::new(std::sync::RwLock::new(None)))
            .expect("tools");
        let storage = spec.storage.clone();
        let spec = spec.with_runtime_delivery(MobRuntimeDelivery::new(store, fixture.jobs));
        assert!(matches!(
            MobRuntime::prepare(spec).await,
            Err(MobRuntimeError::Delivery(
                MobRuntimeDeliveryError::CompositionChanged
            ))
        ));
        assert!(storage.is_event_log_empty().await.expect("events"));
    }

    /// A host's own committed-result owner composed into the spec's
    /// SELECTED delivery after the constructor already bound the agent
    /// mob-tool state: bootstrap rebuilds and rebinds the state, so the bound
    /// job source answers the host's declared namespace (and only it), and
    /// the bound state exposes the service the host submits through.
    #[tokio::test]
    async fn a_host_job_source_added_after_binding_joins_the_bound_source_at_bootstrap() {
        struct SetupStore(meerkat::RetainedJobFacts);
        #[async_trait::async_trait]
        impl meerkat::RetainedJobSource for SetupStore {
            async fn retained_job(
                &self,
                _producer: &meerkat::ContinuationProducer,
                _job_id: &str,
            ) -> meerkat::RetainedJobLookup {
                meerkat::RetainedJobLookup::Found(self.0.clone())
            }
        }
        let host_producer = |namespace: &str| meerkat::ContinuationProducer::Host {
            namespace: namespace.into(),
        };
        let dir = tempfile::tempdir().expect("temp");
        let fixture = memory_fixture(dir.path());
        let facts = meerkat::RetainedJobFacts {
            owner_session_id: meerkat_core::types::SessionId::new(),
            retained_work: None,
            result_digest: "sha256:setup".into(),
        };
        let host = meerkat_mob_mcp::HostJobSource::new(
            ["toolkit.setup"],
            Arc::new(SetupStore(facts.clone())),
        )
        .expect("a named namespace");

        // A spec without a selected delivery refuses; it never builds one.
        let mut bare = memory_fixture(dir.path()).spec;
        bare.runtime_delivery = None;
        assert!(matches!(
            bare.with_host_job_source(host.clone()),
            Err(MobRuntimeDeliveryError::MissingComposition)
        ));

        let spec = fixture
            .spec
            .with_agent_mob_tools(Arc::new(std::sync::RwLock::new(None)))
            .expect("tools");
        let bound_before = spec
            .agent_mob_mcp_state
            .as_ref()
            .and_then(|state| state.continuation_binding_generation())
            .expect("the constructor bound the state");
        let before = fixture
            .delivery
            .bindings
            .job_source()
            .expect("a live binding");
        assert_eq!(
            before
                .retained_job(&host_producer("toolkit.setup"), "setup-1")
                .await,
            meerkat::RetainedJobLookup::Absent,
            "the first binding knows no host namespace"
        );
        drop(before);

        let spec = spec
            .with_host_job_source(host)
            .expect("a selected delivery");
        let (runtime, pending) = MobRuntime::prepare(spec)
            .await
            .expect("prepare with the host's job source");
        assert!(pending.is_none());
        let state = runtime.agent_mob_mcp_state.as_ref().expect("bound state");
        assert_ne!(
            state.continuation_binding_generation(),
            Some(bound_before),
            "bootstrap rebound the state"
        );
        assert!(
            state.continuation_service().is_some(),
            "the host submits through the bound service"
        );
        let bound = fixture
            .delivery
            .bindings
            .job_source()
            .expect("a live binding");
        assert_eq!(
            bound
                .retained_job(&host_producer("toolkit.setup"), "setup-1")
                .await,
            meerkat::RetainedJobLookup::Found(facts.clone())
        );
        assert_eq!(
            bound
                .retained_job(&host_producer("toolkit.other"), "setup-1")
                .await,
            meerkat::RetainedJobLookup::Absent
        );

        // The host commits its result through the bound service, confirmed
        // by the composed source: the record is read from that source.
        let record = meerkat::RetainedJobRecord::from_owner(
            &*bound,
            host_producer("toolkit.setup"),
            "setup-1",
        )
        .await
        .expect("read the composed source")
        .expect("the setup result is committed");
        let owner = meerkat::ContinuationOwner::Session {
            session_id: facts.owner_session_id.clone(),
        };
        let service = state.continuation_service().expect("bound service");
        let delivery = meerkat::ContinuationDelivery {
            key: meerkat::ContinuationKey::new("setup:setup-1").expect("key"),
            result: meerkat::ContinuationResultRef {
                producer: host_producer("toolkit.setup"),
                producer_id: "setup-1".into(),
                result_digest: facts.result_digest.clone(),
                summary: None,
            },
            body: "setup finished".into(),
            handling: meerkat::ContinuationHandling::Queue,
        };
        let receipt = service
            .submit_retained_completion(&owner, delivery.clone(), &record, 1)
            .await
            .expect("commit the host's retained result");
        assert_eq!(
            service
                .submit_retained_completion(&owner, delivery, &record, 2)
                .await
                .expect("replay"),
            receipt,
            "a replay returns the first receipt"
        );
        runtime.handle().shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn child_policy_reinstall_rebinds_exact_generation_with_old_state_alive() {
        let dir = tempfile::tempdir().expect("temp");
        let fixture = memory_fixture(dir.path());
        let spec = fixture
            .spec
            .with_agent_mob_tools(Arc::new(std::sync::RwLock::new(None)))
            .expect("tools");
        let old = spec.agent_mob_mcp_state.clone().expect("old state");
        let previous = old
            .continuation_binding_generation()
            .expect("old generation");
        let spec = spec.with_child_application_tool_policy(
            meerkat_core::ApplicationToolPolicyBinding::Unmanaged,
        );
        let (runtime, pending) = MobRuntime::prepare(spec)
            .await
            .expect("prepare after policy replacement");
        assert!(pending.is_none());
        let current = runtime.agent_mob_mcp_state.as_ref().expect("new state");
        assert!(!Arc::ptr_eq(&old, current));
        assert_ne!(current.continuation_binding_generation(), Some(previous));
        assert_eq!(current.detached_delivery_blocked_because(), None);
        let stale = meerkat_mob_mcp::MobMcpState::new_with_runtime_adapter(
            current.session_service(),
            Some(Arc::clone(&fixture.machine)),
            meerkat_mob::MobControlPrincipal::Owner,
        )
        .expect("state")
        .into_shared();
        assert!(matches!(
            fixture.delivery.bind(&stale, Some(previous)),
            Err(MobRuntimeDeliveryError::Binding(
                meerkat_mob_mcp::BindContinuationsError::Binding(
                    meerkat::ContinuationBindError::StaleGeneration { .. }
                )
            ))
        ));
        assert_ne!(current.continuation_binding_generation(), Some(previous));
        runtime.handle().shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn duplicate_owner_refuses_before_creating_a_mob_and_last_runtime_drop_releases_it() {
        let dir = tempfile::tempdir().expect("temp");
        let fixture = memory_fixture(dir.path());
        let service = Arc::clone(&fixture.spec.session_service);
        let runtime = MobRuntime::bootstrap(fixture.spec)
            .await
            .expect("first boot");
        first_pass(&fixture.delivery).await;
        let retained = runtime.clone();
        let definition = meerkat_mob::MobDefinition::from_toml(&format!(
            "[mob]\nid = \"duplicate-{}\"\n\n[profiles.worker]\nmodel = \"gpt-5.5\"\n",
            uuid::Uuid::new_v4()
        ))
        .expect("definition");
        let storage = meerkat_mob::MobStorage::in_memory();
        let spec = MobBootstrapSpec::new(definition, storage.clone(), service)
            .with_session_runtime_adapter(Arc::clone(&fixture.machine))
            .expect("owner")
            .with_runtime_delivery(fixture.delivery.clone());
        assert!(matches!(
            MobRuntime::prepare(spec).await,
            Err(MobRuntimeError::Delivery(
                MobRuntimeDeliveryError::AlreadyArmed(_)
            ))
        ));
        assert!(
            storage
                .is_event_log_empty()
                .await
                .expect("no bootstrap effects")
        );
        drop(runtime);
        assert!(
            fixture.delivery.is_running(),
            "a MobRuntime clone retains delivery ownership"
        );
        retained.handle().shutdown().await.expect("shutdown");
        drop(retained);
        owner_released(&fixture.delivery).await;
        assert!(!fixture.delivery.is_running());
    }

    #[tokio::test]
    async fn stopping_runtime_delivery_releases_the_owner_while_clones_live() {
        let dir = tempfile::tempdir().expect("temp");
        let fixture = memory_fixture(dir.path());
        let runtime = MobRuntime::bootstrap(fixture.spec)
            .await
            .expect("first boot");
        first_pass(&fixture.delivery).await;
        let retained = runtime.clone();
        assert!(fixture.delivery.is_running());
        runtime.stop_runtime_delivery();
        owner_released(&fixture.delivery).await;
        assert!(
            !fixture.delivery.is_running(),
            "an explicit stop releases delivery ownership while a runtime clone lives"
        );
        assert!(
            retained.runtime_delivery_pass().is_some(),
            "the last observed pass stays readable after the stop"
        );
        retained.handle().shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn restored_mobs_arm_delivery_before_deferred_identity_activation() {
        for identity_first in [false, true] {
            let dir = tempfile::tempdir().expect("temp");
            let fixture = memory_fixture(dir.path());
            let definition = fixture.spec.definition.clone();
            let mob_id = definition.id.clone();
            let storage = fixture.spec.storage.clone();
            let service = Arc::clone(&fixture.spec.session_service);
            let first = MobRuntime::bootstrap(fixture.spec)
                .await
                .expect("first boot");
            let stopped = first
                .handle()
                .stop()
                .await
                .expect("persist stopped lifecycle");
            assert!(stopped.members.is_empty(), "the fixture has no members");
            assert_eq!(
                first.handle().status().await.expect("stopped state"),
                meerkat_mob::MobState::Stopped
            );
            assert!(
                first
                    .handle()
                    .events()
                    .replay_all()
                    .await
                    .expect("replay durable stopped lifecycle")
                    .iter()
                    .any(|event| matches!(event.kind, meerkat_mob::MobEventKind::MobStopped)),
                "the fixture must record an explicit Stop before recovery"
            );
            first
                .handle()
                .shutdown()
                .await
                .expect("close stopped actor");
            drop(first);
            owner_released(&fixture.delivery).await;

            let mut spec = MobBootstrapSpec::new(definition, storage, service)
                .with_declared_ephemeral_mob_storage()
                .with_session_runtime_adapter(Arc::clone(&fixture.machine))
                .expect("same machine")
                .with_runtime_delivery(fixture.delivery.clone())
                .with_agent_mob_tools(Arc::new(std::sync::RwLock::new(None)))
                .expect("install tools on restore");
            if !identity_first {
                spec.identity_runtime_slot = None;
            }
            let (runtime, pending) = MobRuntime::prepare(spec).await.expect("prepare restore");
            assert!(fixture.delivery.is_running(), "owner is armed on return");
            let state = runtime
                .agent_mob_mcp_state
                .as_ref()
                .expect("resolver state");
            let restored_state = state.mob_status(&mob_id).await.expect("inserted handle");
            if identity_first {
                assert_eq!(restored_state, meerkat_mob::MobState::Stopped);
                pending
                    .expect("identity composition retains activation")
                    .activate()
                    .await
                    .expect("consume deferred activation for this empty mob");
            } else {
                assert!(pending.is_none(), "classic composition consumes activation");
                assert_eq!(restored_state, meerkat_mob::MobState::Running);
            }
            assert_eq!(
                runtime.handle().status().await.expect("running state"),
                meerkat_mob::MobState::Running
            );
            runtime.handle().shutdown().await.expect("shutdown");
            drop(runtime);
            owner_released(&fixture.delivery).await;
        }
    }

    fn completion(key: &str) -> meerkat::ContinuationDelivery {
        meerkat::ContinuationDelivery {
            key: meerkat::ContinuationKey::new(key).expect("key"),
            result: meerkat::ContinuationResultRef {
                producer: meerkat::ContinuationProducer::Host {
                    namespace: "meta-test".to_string(),
                },
                producer_id: key.to_string(),
                result_digest: "sha256:completion".to_string(),
                summary: None,
            },
            body: "completed".into(),
            handling: meerkat::ContinuationHandling::Queue,
        }
    }

    #[tokio::test]
    async fn pending_completion_reopens_and_applies_once_through_the_composed_owner() {
        let dir = tempfile::tempdir().expect("temp");
        let path = dir.path().join("runtime.sqlite3");
        let session = meerkat_core::SessionId::new();
        let owner = meerkat::ContinuationOwner::Session {
            session_id: session.clone(),
        };
        let receipt = {
            let store: Arc<dyn meerkat_runtime::RuntimeStore> =
                Arc::new(meerkat_runtime::store::SqliteRuntimeStore::new(&path).expect("open"));
            let fixture = fixture(dir.path(), store);
            let continuations = meerkat::ContinuationOwnerService::new(
                fixture.delivery.inbox(),
                Arc::new(meerkat::SessionAddressResolver),
                Arc::clone(&fixture.machine),
            );
            let receipt = continuations
                .submit(&owner, completion("reopen"), 10)
                .await
                .expect("commit pending completion");
            let runtime = MobRuntime::bootstrap(fixture.spec)
                .await
                .expect("first boot");
            let pass = first_pass(&fixture.delivery).await;
            assert_eq!(pass.applied, 0);
            assert_eq!(
                fixture
                    .delivery
                    .inbox()
                    .pending_delivery_total()
                    .await
                    .expect("backlog"),
                1
            );
            runtime.handle().shutdown().await.expect("shutdown");
            drop(runtime);
            owner_released(&fixture.delivery).await;
            receipt
        };
        let store: Arc<dyn meerkat_runtime::RuntimeStore> =
            Arc::new(meerkat_runtime::store::SqliteRuntimeStore::new(&path).expect("reopen"));
        let fixture = fixture(dir.path(), store);
        fixture
            .machine
            .register_session(session.clone())
            .await
            .expect("register recipient");
        let continuations = meerkat::ContinuationOwnerService::new(
            fixture.delivery.inbox(),
            Arc::new(meerkat::SessionAddressResolver),
            Arc::clone(&fixture.machine),
        );
        assert_eq!(
            continuations
                .submit(&owner, completion("reopen"), 99)
                .await
                .expect("idempotent reopen"),
            receipt
        );
        let runtime = MobRuntime::bootstrap(fixture.spec)
            .await
            .expect("second boot");
        let pass = first_pass(&fixture.delivery).await;
        assert_eq!(pass.applied, 1, "{pass:?}");
        let applied = continuations
            .continuation_status(
                &owner,
                &meerkat::ContinuationKey::new("reopen").expect("key"),
            )
            .await
            .expect("status");
        assert!(
            matches!(&applied, meerkat::ContinuationStatus::Applied { session: applied_session, .. } if applied_session == &session)
        );
        assert_eq!(
            fixture
                .delivery
                .inbox()
                .pending_delivery_total()
                .await
                .expect("backlog"),
            0
        );
        assert_eq!(
            continuations
                .submit(&owner, completion("reopen"), 100)
                .await
                .expect("replay"),
            receipt
        );
        assert_eq!(
            continuations
                .continuation_status(
                    &owner,
                    &meerkat::ContinuationKey::new("reopen").expect("key")
                )
                .await
                .expect("status after replay"),
            applied
        );
        runtime.handle().shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn restoring_one_mob_never_applies_or_settles_a_foreign_member_row() {
        struct ForeignMemberResolver {
            owner: meerkat::ContinuationOwner,
            address: meerkat_runtime::LogicalRuntimeId,
        }

        #[async_trait::async_trait]
        impl meerkat::ContinuationAddressResolver for ForeignMemberResolver {
            async fn current_address(
                &self,
                owner: &meerkat::ContinuationOwner,
            ) -> Result<Option<meerkat_runtime::LogicalRuntimeId>, String> {
                if owner == &self.owner {
                    Ok(Some(self.address.clone()))
                } else {
                    Err("unexpected fixture owner".to_string())
                }
            }

            async fn resolve_address(
                &self,
                address: &meerkat_runtime::LogicalRuntimeId,
            ) -> Result<meerkat::AddressResolution, String> {
                if address == &self.address {
                    Ok(meerkat::AddressResolution::NotServed)
                } else {
                    Err("unexpected fixture address".to_string())
                }
            }
        }

        let dir = tempfile::tempdir().expect("temp");
        let fixture = memory_fixture(dir.path());
        let spec = fixture
            .spec
            .with_agent_mob_tools(Arc::new(std::sync::RwLock::new(None)))
            .expect("tools");
        let owner = meerkat::ContinuationOwner::Member {
            mob_id: "foreign-mob".to_string(),
            identity: "foreign-member".to_string(),
        };
        let address = meerkat::member_delivery_address("foreign-mob", "foreign-member", 1)
            .expect("foreign address");
        let continuations = meerkat::ContinuationOwnerService::new(
            fixture.delivery.inbox(),
            Arc::new(ForeignMemberResolver {
                owner: owner.clone(),
                address: address.clone(),
            }),
            Arc::clone(&fixture.machine),
        );
        let receipt = continuations
            .submit(&owner, completion("foreign-key"), 1)
            .await
            .expect("commit native foreign continuation");
        let id = meerkat_runtime::RuntimeDeliveryId::new(receipt.delivery_id)
            .expect("native receipt delivery id");
        let runtime = MobRuntime::bootstrap(spec).await.expect("local root boot");
        let pass = first_pass(&fixture.delivery).await;
        assert_eq!(pass.applied, 0, "{pass:?}");
        assert_eq!(pass.refused, 0, "{pass:?}");
        assert_eq!(
            fixture
                .delivery
                .inbox()
                .pending_delivery_total()
                .await
                .expect("backlog"),
            1
        );
        assert!(
            fixture
                .delivery
                .inbox()
                .continuation_admission(&address, &id)
                .await
                .expect("admission")
                .is_none()
        );
        assert!(matches!(
            continuations
                .continuation_status(
                    &owner,
                    &meerkat::ContinuationKey::new("foreign-key").expect("key"),
                )
                .await
                .expect("foreign continuation status"),
            meerkat::ContinuationStatus::Pending { admitted: None, .. }
        ));
        runtime.handle().shutdown().await.expect("shutdown");
    }
}
