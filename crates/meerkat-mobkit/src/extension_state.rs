//! Optional provider-owned documents and native tool bundle factories.
//! No documents, dispatcher definitions or authority resolver exist unless
//! this feature is compiled and a factory is explicitly registered.
pub(crate) mod identity_publication;
pub use identity_publication::IdentityPublication;
pub(crate) mod mcp_binding;
pub use mcp_binding::{ChildMcpAvailability, HostMcpDocumentBinding};

use async_trait::async_trait;
use documents::{EdgeKind, HostPolicy, LineageLink, Principal, Role, VerifiedLineage};
use meerkat_core::{AgentToolDispatcher, ToolDispatchContext};
use meerkat_mob::{
    MemberCreationProvenance, MemberCreationSnapshot, MobReadHandle, MobSessionService,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub use documents::{DocumentService, HostAccessContext, Owner};
pub use mobkit_extension_state as documents;

/// Per-call authority from the runner's origin session. requested_owner is only
/// a recipient to validate, never the identity on whose behalf the call runs.
#[async_trait]
pub trait ToolCallerResolver: Send + Sync {
    async fn resolve(
        &self,
        context: &ToolDispatchContext,
        requested_owner: Option<&Owner>,
    ) -> documents::Result<HostAccessContext>;
}

#[derive(Clone)]
pub struct ToolBundleContext {
    pub documents: DocumentService,
    pub caller_resolver: Arc<dyn ToolCallerResolver>,
}

pub trait ToolBundleFactory: Send + Sync {
    fn build(&self, context: ToolBundleContext) -> Result<Arc<dyn AgentToolDispatcher>, String>;
}
impl<F> ToolBundleFactory for F
where
    F: Fn(ToolBundleContext) -> Result<Arc<dyn AgentToolDispatcher>, String> + Send + Sync,
{
    fn build(&self, context: ToolBundleContext) -> Result<Arc<dyn AgentToolDispatcher>, String> {
        self(context)
    }
}

#[derive(Debug, Clone)]
pub struct ToolBundleRequirements {
    pub namespace: String,
    pub read_tool: String,
    pub edit_tool: String,
}
impl ToolBundleRequirements {
    pub fn durable_documents(
        namespace: impl Into<String>,
        read_tool: impl Into<String>,
        edit_tool: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            read_tool: read_tool.into(),
            edit_tool: edit_tool.into(),
        }
    }
}

fn initialize_schema(tx: &rusqlite::Transaction<'_>) -> Result<(), rusqlite::Error> {
    tx.execute_batch(documents::SCHEMA)
}

pub(crate) const EXTENSION_SCHEMA_DOMAIN: meerkat_sqlite::SchemaDomain =
    meerkat_sqlite::SchemaDomain {
        name: "mobkit-extension-state",
        migrations: &[meerkat_sqlite::Migration {
            version: 1,
            name: "documents-and-receipts",
            apply: initialize_schema,
        }],
        initialize_current: initialize_schema,
        allowed_existing_versions: &[1],
        bridge_recoverable_versions: &[],
        released_predecessors: &[],
        owned_objects: &[
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "extension_documents",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "extension_receipts",
            },
        ],
        retired_objects: &[],
    };

struct DiskOperationFence(std::path::PathBuf);
impl documents::ConnectionGuard for DiskOperationFence {
    fn acquire(&self) -> documents::Result<Box<dyn Send>> {
        meerkat_sqlite::OperationGuard::for_database(&self.0)
            .map(|guard| Box::new(guard) as Box<dyn Send>)
            .map_err(|error| documents::Error::Storage(error.to_string()))
    }
}

pub(crate) fn open_disk_store(
    path: std::path::PathBuf,
) -> Result<documents::SqliteExtensionDocumentStore, String> {
    let mut connection = meerkat_sqlite::open(&path, meerkat_sqlite::ConnectionProfile::PRIMARY)
        .map_err(|error| error.to_string())?;
    meerkat_sqlite::apply_domain_migrations(&mut connection, &EXTENSION_SCHEMA_DOMAIN)
        .map_err(|error| error.to_string())?;
    Ok(documents::SqliteExtensionDocumentStore::from_connection(
        connection,
        Some(Arc::new(DiskOperationFence(path))),
    ))
}

/// Holds runtime query authorities, never copied lineage or document policy.
/// A handle is installed by Meerkat's pre-activation callback before its first
/// member can dispatch. Every operation reads current authority afresh.
pub(crate) struct NativeAuthorityRegistry {
    realm: String,
    identity_bridge_mob: String,
    handles: std::sync::RwLock<BTreeMap<String, MobReadHandle>>,
    sessions: Arc<dyn MobSessionService>,
    continuity: Option<Arc<dyn crate::identity_first::contracts::ContinuityStore>>,
    access: Option<crate::access::AccessController>,
    pub(crate) publications: Arc<identity_publication::IdentityPublications>,
}

impl NativeAuthorityRegistry {
    pub(crate) fn new(
        realm: String,
        identity_bridge_mob: String,
        sessions: Arc<dyn MobSessionService>,
        continuity: Option<Arc<dyn crate::identity_first::contracts::ContinuityStore>>,
        access: Option<crate::access::AccessController>,
    ) -> Arc<Self> {
        Arc::new(Self {
            realm,
            identity_bridge_mob,
            handles: std::sync::RwLock::new(BTreeMap::new()),
            sessions,
            continuity,
            access,
            publications: Arc::new(Default::default()),
        })
    }

    pub(crate) fn before_activation(self: &Arc<Self>) -> meerkat_mob::MobBeforeActivation {
        let registry = Arc::downgrade(self);
        Arc::new(move |handle| {
            let registry = registry.clone();
            Box::pin(async move {
                let registry = registry.upgrade().ok_or_else(|| {
                    meerkat_mob::MobError::Internal("extension authority owner unavailable".into())
                })?;
                if let Some(continuity) = &registry.continuity {
                    let cursor = handle
                        .member_creation_journal_cursor()
                        .await
                        .map_err(|e| meerkat_mob::MobError::Internal(e.to_string()))?;
                    continuity
                        .establish_identity_history_coverage(handle.mob_id().as_str(), cursor)
                        .await
                        .map_err(|e| meerkat_mob::MobError::Internal(e.to_string()))?;
                }
                let mut handles = registry.handles.write().map_err(|_| {
                    meerkat_mob::MobError::Internal("extension authority registry poisoned".into())
                })?;
                let id = handle.mob_id().to_string();
                if handles.get(&id).is_some_and(|existing| {
                    !matches!(
                        existing.status_observation_snapshot(),
                        meerkat_mob::MobState::Stopped
                            | meerkat_mob::MobState::Completed
                            | meerkat_mob::MobState::Destroyed
                    )
                }) {
                    return Err(meerkat_mob::MobError::Internal(
                        "extension authority already bound for mob".into(),
                    ));
                }
                handles.insert(id, handle);
                Ok(())
            })
        })
    }

    fn handle(&self, id: &str) -> documents::Result<MobReadHandle> {
        self.handles
            .read()
            .map_err(|_| documents::Error::AuthorityUnavailable)?
            .get(id)
            .cloned()
            .ok_or(documents::Error::AuthorityUnavailable)
    }

    async fn snapshot(
        &self,
        binding: &meerkat_core::MobMemberBinding,
        session: &meerkat_core::SessionId,
    ) -> documents::Result<MemberCreationSnapshot> {
        let result = self
            .handle(&binding.mob_id)?
            .member_creation_for_session(session)
            .await
            .map_err(|_| documents::Error::AuthorityUnavailable)?
            .ok_or(documents::Error::AuthorityUnavailable)?;
        if result.member_binding.mob_id != binding.mob_id
            || result.member_binding.member != binding.member
            || result.session_id != *session
        {
            return Err(documents::Error::AuthorityUnavailable);
        }
        Ok(result)
    }

    async fn principal(&self, snapshot: &MemberCreationSnapshot) -> documents::Result<Principal> {
        if let Some(continuity) = &self.continuity {
            if let Some(identity) = continuity
                .historical_identity_binding(&snapshot.session_id)
                .await
                .map_err(|_| documents::Error::AuthorityUnavailable)?
            {
                return Ok(Principal::Agent(identity.to_string()));
            }
            self.publications.wait(snapshot).await?;
            if let Some(identity) = continuity
                .historical_identity_binding(&snapshot.session_id)
                .await
                .map_err(|_| documents::Error::AuthorityUnavailable)?
            {
                return Ok(Principal::Agent(identity.to_string()));
            }
            if snapshot.member_binding.mob_id == self.identity_bridge_mob {
                let decoded =
                    crate::member_comms_id::runtime_alias_str(&snapshot.member_binding.member);
                if let Ok(identity) = crate::identity_first::AgentIdentity::parse(&decoded)
                    && crate::member_comms_id::mob_member_id(identity.as_str()).as_str()
                        == snapshot.member_binding.member
                    && continuity
                        .has_historical_identity(&identity)
                        .await
                        .map_err(|_| documents::Error::AuthorityUnavailable)?
                {
                    // Durable intent reserves the host target even after
                    // publication cancellation and process restart. Only
                    // exact-session history above can establish Agent.
                    return Err(documents::Error::AuthorityUnavailable);
                }
            }
            if !continuity
                .identity_history_covers_birth(
                    &snapshot.member_binding.mob_id,
                    snapshot.birth_cursor,
                )
                .await
                .map_err(|_| documents::Error::AuthorityUnavailable)?
            {
                return Err(documents::Error::AuthorityUnavailable);
            }
        }
        Ok(Principal::Worker {
            mob_id: snapshot.member_binding.mob_id.clone(),
            member_id: snapshot.member_binding.member.clone(),
            creation_id: snapshot
                .creation
                .creation_id
                .ok_or(documents::Error::AuthorityUnavailable)?
                .to_string(),
        })
    }

    async fn validate_owner(&self, owner: &Owner) -> documents::Result<()> {
        match owner {
            Owner::Realm => Ok(()),
            Owner::Mob(id) => self.handle(id).map(|_| ()),
            Owner::Agent(Principal::Agent(identity)) => {
                let continuity = self
                    .continuity
                    .as_ref()
                    .ok_or(documents::Error::AuthorityUnavailable)?;
                let identity = crate::identity_first::AgentIdentity::parse(identity)
                    .map_err(|_| documents::Error::NotFound)?;
                let states = continuity
                    .resolve_many(std::slice::from_ref(&identity))
                    .await
                    .map_err(|_| documents::Error::AuthorityUnavailable)?;
                match states.get(&identity) {
                    Some(crate::identity_first::ContinuityResolveState::Ready { .. }) => Ok(()),
                    _ => Err(documents::Error::NotFound),
                }
            }
            Owner::Agent(Principal::Worker {
                mob_id,
                member_id,
                creation_id,
            }) => {
                let handle = self.handle(mob_id)?;
                let entry = handle
                    .get_member(&meerkat_mob::AgentIdentity::from(member_id.as_str()))
                    .await
                    .map_err(|_| documents::Error::AuthorityUnavailable)?
                    .ok_or(documents::Error::NotFound)?;
                let session = entry
                    .bridge_session_id()
                    .ok_or(documents::Error::NotFound)?;
                let snapshot = handle
                    .member_creation_for_session(session)
                    .await
                    .map_err(|_| documents::Error::AuthorityUnavailable)?
                    .ok_or(documents::Error::NotFound)?;
                if snapshot
                    .creation
                    .creation_id
                    .is_some_and(|id| id.to_string() == *creation_id)
                    && self.principal(&snapshot).await?
                        == match owner {
                            Owner::Agent(principal) => principal.clone(),
                            _ => return Err(documents::Error::NotFound),
                        }
                {
                    Ok(())
                } else {
                    Err(documents::Error::NotFound)
                }
            }
        }
    }
}

pub(crate) struct NativeCallerResolver {
    pub(crate) registry: std::sync::Weak<NativeAuthorityRegistry>,
    pub(crate) requirements: ToolBundleRequirements,
    pub(crate) child_availability: Option<ChildMcpAvailability>,
}
impl NativeCallerResolver {
    fn policy(
        &self,
        policy: Option<&meerkat_core::ops::ToolAccessPolicy>,
    ) -> documents::Result<HostPolicy> {
        let policy = policy
            .cloned()
            .map(meerkat_core::ToolExecutionPolicy::resolve)
            .transpose()
            .map_err(|_| documents::Error::AuthorityUnavailable)?
            .unwrap_or_else(meerkat_core::ToolExecutionPolicy::unrestricted);
        let read = policy.permits_call(
            &self.requirements.read_tool,
            meerkat_core::ToolMutationClass::ReadOnly,
        );
        let edit = read
            && policy.permits_call(
                &self.requirements.edit_tool,
                meerkat_core::ToolMutationClass::Mutating,
            );
        Ok(HostPolicy {
            read,
            edit,
            manage: edit,
        })
    }
    async fn source_ceiling(
        &self,
        registry: &NativeAuthorityRegistry,
        source: &MemberCreationSnapshot,
    ) -> documents::Result<Option<Role>> {
        let metadata = registry
            .sessions
            .load_retained_session_metadata(&source.session_id)
            .await
            .map_err(|_| documents::Error::AuthorityUnavailable)?
            .filter(|view| view.session_id == source.session_id)
            .and_then(|view| view.session_metadata)
            .ok_or(documents::Error::AuthorityUnavailable)?;
        let binding = metadata
            .mob_member_binding
            .as_ref()
            .ok_or(documents::Error::AuthorityUnavailable)?;
        if binding.mob_id != source.member_binding.mob_id
            || binding.member != source.member_binding.member
        {
            return Err(documents::Error::AuthorityUnavailable);
        }
        self.ceiling(metadata.tooling.tool_access_policy.as_ref())
    }

    fn ceiling(
        &self,
        policy: Option<&meerkat_core::ops::ToolAccessPolicy>,
    ) -> documents::Result<Option<Role>> {
        let policy = self.policy(policy)?;
        Ok(if policy.edit {
            Some(Role::Editor)
        } else if policy.read {
            Some(Role::Reader)
        } else {
            None
        })
    }
}

/// The sole adapter from Meerkat dispatch provenance to this extension's
/// authority lookup. Meerkat's native runner stamps this field; tool arguments
/// never contribute. A future stamped turn-context contract can replace this
/// adapter without changing document or lineage authorization.
fn runtime_origin_session(
    context: &ToolDispatchContext,
) -> documents::Result<&meerkat_core::SessionId> {
    context
        .origin_session_id()
        .ok_or(documents::Error::AuthorityUnavailable)
}

#[async_trait]
impl ToolCallerResolver for NativeCallerResolver {
    async fn resolve(
        &self,
        context: &ToolDispatchContext,
        requested_owner: Option<&Owner>,
    ) -> documents::Result<HostAccessContext> {
        let registry = self
            .registry
            .upgrade()
            .ok_or(documents::Error::AuthorityUnavailable)?;
        let session_id = runtime_origin_session(context)?;
        let metadata = registry
            .sessions
            .load_persisted_session_metadata(session_id)
            .await
            .map_err(|_| documents::Error::AuthorityUnavailable)?
            .filter(|view| view.session_id == *session_id)
            .and_then(|view| view.session_metadata)
            .ok_or(documents::Error::AuthorityUnavailable)?;
        let binding = metadata
            .mob_member_binding
            .ok_or(documents::Error::AuthorityUnavailable)?;
        let handle = registry.handle(&binding.mob_id)?;
        if self.child_availability == Some(ChildMcpAvailability::HostOnly)
            && handle
                .owner_bridge_session_lifecycle_authority()
                .is_some_and(|authority| authority.destroy_on_owner_archive)
        {
            return Err(documents::Error::NotFound);
        }
        let entry = handle
            .get_member(&meerkat_mob::AgentIdentity::from(binding.member.as_str()))
            .await
            .map_err(|_| documents::Error::AuthorityUnavailable)?
            .ok_or(documents::Error::AuthorityUnavailable)?;
        if entry.bridge_session_id() != Some(session_id) || entry.role.as_str() != binding.role {
            return Err(documents::Error::AuthorityUnavailable);
        }
        let mut snapshot = registry.snapshot(&binding, session_id).await?;
        let principal = registry.principal(&snapshot).await?;
        let mut child = principal.clone();
        let mut links = Vec::new();
        let mut visited = BTreeSet::new();
        loop {
            if visited.len() >= 128 || !visited.insert(snapshot.session_id.0) {
                return Err(documents::Error::AuthorityUnavailable);
            }
            let (source_binding, source_session, creation_id, kind) =
                match &snapshot.creation.provenance {
                    MemberCreationProvenance::Root => break,
                    MemberCreationProvenance::LegacyUnknown
                    | MemberCreationProvenance::Unproven => {
                        return Err(documents::Error::AuthorityUnavailable);
                    }
                    MemberCreationProvenance::Spawn { source } => (
                        source.member_binding.clone(),
                        source.session_id.clone(),
                        source.creation_id,
                        Some(EdgeKind::Spawn),
                    ),
                    MemberCreationProvenance::Fork { source_creation_id } => {
                        let fork = snapshot
                            .fork_source
                            .as_ref()
                            .ok_or(documents::Error::AuthorityUnavailable)?;
                        (
                            fork.source_member.clone(),
                            fork.source_session_id.clone(),
                            *source_creation_id,
                            Some(EdgeKind::Fork),
                        )
                    }
                    MemberCreationProvenance::Successor {
                        predecessor_session_id,
                        predecessor_member_binding,
                        predecessor_creation_id,
                    } => (
                        predecessor_member_binding.clone(),
                        predecessor_session_id.clone(),
                        *predecessor_creation_id,
                        None,
                    ),
                };
            let parent_snapshot = registry.snapshot(&source_binding, &source_session).await?;
            if parent_snapshot.creation.creation_id != Some(creation_id) {
                return Err(documents::Error::AuthorityUnavailable);
            }
            let parent = registry.principal(&parent_snapshot).await?;
            if let Some(kind) = kind {
                let ceiling = self.source_ceiling(&registry, &parent_snapshot).await?;
                links.push(LineageLink {
                    child,
                    parent: parent.clone(),
                    kind,
                    ceiling,
                });
                child = parent;
            } else if parent != child
                || snapshot.creation.creation_id != parent_snapshot.creation.creation_id
            {
                return Err(documents::Error::AuthorityUnavailable);
            }
            snapshot = parent_snapshot;
        }
        let lineage = if links.is_empty() {
            VerifiedLineage::root()
        } else {
            VerifiedLineage::derived(links)?
        };
        let mut policy = self.policy(metadata.tooling.tool_access_policy.as_ref())?;
        let mut realm_admin = false;
        if let Some(access) = &registry.access {
            let subject = match &principal {
                Principal::Agent(identity) => format!("agent:{identity}"),
                Principal::Worker { creation_id, .. } => format!("worker:{creation_id}"),
            };
            let view = access.view_for_subject(Some(&subject));
            let resource = crate::access::AccessResource {
                identity: match &principal {
                    Principal::Agent(identity) => Some(identity.as_str()),
                    Principal::Worker { .. } => None,
                },
                agent_id: Some(&binding.member),
                role: Some(&binding.role),
                labels: Some(&entry.labels),
            };
            policy.read &= view.decide("extension.read", &resource).is_allow();
            policy.edit &= policy.read && view.decide("extension.edit", &resource).is_allow();
            policy.manage &= policy.edit && view.decide("extension.manage", &resource).is_allow();
            realm_admin = view.is_admin();
        }
        let member_mobs = BTreeSet::from([binding.mob_id.clone()]);
        let handles: Vec<_> = registry
            .handles
            .read()
            .map_err(|_| documents::Error::AuthorityUnavailable)?
            .values()
            .cloned()
            .collect();
        let mut managed_mobs = BTreeSet::new();
        for handle in handles {
            if handle
                .owner_bridge_session_lifecycle_authority()
                .is_some_and(|authority| authority.bridge_session_id == *session_id)
            {
                managed_mobs.insert(handle.mob_id().to_string());
            }
        }
        let mut caller =
            HostAccessContext::new(registry.realm.clone(), principal, lineage, policy)?
                .with_memberships(member_mobs, managed_mobs, realm_admin)?;
        if let Some(owner) = requested_owner {
            registry.validate_owner(owner).await?;
            caller = caller.with_valid_owners(BTreeSet::from([owner.clone()]));
        }
        Ok(caller)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn extension_native_retained_source_policy_observes_authoritative_commit() {
        let dir = tempfile::tempdir().unwrap();
        let definition = meerkat_mob::MobDefinition::from_toml(
            r#"
[mob]
id = "retained-policy"
[profiles.worker]
model = "gpt-5.5"
[profiles.worker.tools]
comms = true
"#,
        )
        .unwrap();
        let spec = crate::MobBootstrapSpec::persistent(
            definition,
            meerkat_mob::MobStorage::in_memory(),
            dir.path().to_path_buf(),
            4,
            Arc::new(
                meerkat_store::SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
                    .unwrap(),
            ),
        )
        .unwrap()
        .with_options(crate::MobBootstrapOptions {
            allow_ephemeral_sessions: false,
            notify_orchestrator_on_resume: false,
            default_llm_client: Some(Arc::new(meerkat_client::TestClient::default())),
        });
        // Keep the exact epoch-observing facade used by the session authority.
        // Mutating a SessionStore export would not change native policy.
        let runtime_store = spec.runtime_authority_prewarm.clone().unwrap();
        let sessions = spec.session_service.clone();
        let runtime = crate::MobRuntime::bootstrap(spec).await.unwrap();
        let handle = runtime.handle();
        let identity = meerkat_mob::AgentIdentity::from("source");
        handle
            .spawn_spec(meerkat_mob::SpawnMemberSpec::host_root(
                "worker",
                identity.clone(),
            ))
            .await
            .unwrap();
        let session_id = handle
            .get_member(&identity)
            .await
            .unwrap()
            .unwrap()
            .bridge_session_id()
            .unwrap()
            .clone();
        // Spawn returns before its autonomous initial turn has completed.
        // This fixture edits retained metadata outside that actor, so first
        // settle the native kickoff and prove no turn still owns a checkpoint.
        let kickoffs = handle
            .wait_for_members_kickoff_complete(
                std::slice::from_ref(&identity),
                Some(std::time::Duration::from_secs(5)),
            )
            .await
            .unwrap();
        let [(member, kickoff)] = kickoffs.as_slice() else {
            panic!("fixture requires the source's completed kickoff");
        };
        assert_eq!(member, &identity);
        assert_eq!(
            kickoff.kickoff.as_ref().map(|kickoff| kickoff.phase),
            Some(meerkat_mob::MobMemberKickoffPhase::Started)
        );
        let activity = sessions
            .subscribe_session_activity(&session_id)
            .await
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            activity.clone().wait_inactive(),
        )
        .await
        .expect("the native source activity must settle before external policy commits");
        assert!(
            !activity.is_active(),
            "external retained-policy edits require an idle native source actor"
        );
        // Native cleanup fencing waits out checkpoint writers and disables
        // later saves of the actor's old metadata during the raw commit loop.
        // No turn is admitted until the fixture restores the gates below.
        sessions.cancel_all_checkpointers().await;
        let snapshot = handle
            .member_creation_for_session(&session_id)
            .await
            .unwrap()
            .unwrap();
        let registry = NativeAuthorityRegistry::new(
            "test".into(),
            handle.mob_id().to_string(),
            sessions.clone(),
            None,
            None,
        );
        let resolver = NativeCallerResolver {
            registry: Arc::downgrade(&registry),
            child_availability: None,
            requirements: ToolBundleRequirements::durable_documents(
                "durable-documents-test",
                "documents_read",
                "documents_apply",
            ),
        };
        assert_eq!(
            resolver.source_ceiling(&registry, &snapshot).await.unwrap(),
            Some(Role::Editor)
        );
        let mut source = sessions
            .load_persisted_session(&session_id)
            .await
            .unwrap()
            .unwrap();
        let mut metadata = source.try_session_metadata().unwrap().unwrap();
        for (policy, expected) in [
            (
                Some(meerkat_core::ops::ToolAccessPolicy::ReadOnly),
                Some(Role::Reader),
            ),
            (
                Some(meerkat_core::ops::ToolAccessPolicy::DenyList(
                    ["documents_apply"].into_iter().collect(),
                )),
                Some(Role::Reader),
            ),
            (
                Some(meerkat_core::ops::ToolAccessPolicy::DenyList(
                    ["documents_read"].into_iter().collect(),
                )),
                None,
            ),
            (None, Some(Role::Editor)),
        ] {
            metadata.tooling.tool_access_policy = policy;
            source.set_session_metadata(metadata.clone()).unwrap();
            runtime_store
                .commit_session_snapshot(
                    &meerkat_runtime::LogicalRuntimeId::for_session(&session_id),
                    meerkat_runtime::SerializedSessionSnapshot {
                        session_snapshot: source.to_persisted_bytes().unwrap().into(),
                    },
                )
                .await
                .unwrap();
            let retained = sessions
                .load_retained_session_metadata(&session_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(retained.session_id, session_id);
            assert_eq!(
                serde_json::to_value(
                    retained
                        .session_metadata
                        .unwrap()
                        .tooling
                        .tool_access_policy
                )
                .unwrap(),
                serde_json::to_value(&metadata.tooling.tool_access_policy).unwrap(),
                "the native retained authority must observe this exact policy commit"
            );
            assert_eq!(
                resolver.source_ceiling(&registry, &snapshot).await.unwrap(),
                expected
            );
        }
        sessions.rearm_all_checkpointers().await;
        registry.before_activation()(handle.read_handle())
            .await
            .unwrap();
        let host_only = NativeCallerResolver {
            registry: Arc::downgrade(&registry),
            requirements: resolver.requirements.clone(),
            child_availability: Some(ChildMcpAvailability::HostOnly),
        };
        let root_context =
            ToolDispatchContext::default().with_runtime_identity(session_id.clone(), None);
        assert!(host_only.resolve(&root_context, None).await.is_ok());

        // Scope comes from the native owner bridge authority. A copied
        // descriptor or a root-looking profile name cannot change it.
        let mut child_definition = handle.definition().clone();
        child_definition.id = "document-child-scope".into();
        let child =
            meerkat_mob::MobBuilder::new(child_definition, meerkat_mob::MobStorage::in_memory())
                .with_session_service(sessions.clone())
                .with_default_llm_client(Arc::new(meerkat_client::TestClient::default()))
                .before_activation(registry.before_activation())
                .with_owner_bridge_session_create_authority(session_id, true, false)
                .create()
                .await
                .unwrap();
        let child_identity = meerkat_mob::AgentIdentity::from("child");
        child
            .spawn_spec(meerkat_mob::SpawnMemberSpec::host_root(
                "worker",
                child_identity.clone(),
            ))
            .await
            .unwrap();
        let child_session = child
            .get_member(&child_identity)
            .await
            .unwrap()
            .unwrap()
            .bridge_session_id()
            .unwrap()
            .clone();
        let child_context =
            ToolDispatchContext::default().with_runtime_identity(child_session, None);
        assert!(matches!(
            host_only.resolve(&child_context, None).await,
            Err(documents::Error::NotFound)
        ));
        let child_available = NativeCallerResolver {
            child_availability: Some(ChildMcpAvailability::ChildAvailable),
            ..host_only
        };
        assert!(child_available.resolve(&child_context, None).await.is_ok());
        child.stop().await.unwrap();
        handle.stop().await.unwrap();
    }
}
