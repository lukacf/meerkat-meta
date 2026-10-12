//! Actual runtime-origin dispatch, canonical lineage and durable document tests.
#![cfg(feature = "extension-state")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use meerkat_client::{LlmClient, LlmDoneOutcome, LlmError, LlmEvent, LlmRequest};
use meerkat_core::types::{
    ContentInput, HandlingMode, StopReason, ToolCallView, ToolDef, ToolResult,
};
use meerkat_core::{
    AgentToolDispatcher, ToolDispatchContext, ToolDispatchOutcome, ToolError, ToolMutationClass,
};
use meerkat_mob::{AgentIdentity, MobDefinition, SpawnMemberSpec};
use meerkat_mobkit::extension_state::documents::*;
use meerkat_mobkit::extension_state::{ToolBundleContext, ToolBundleRequirements};
use meerkat_mobkit::{MobBootstrapOptions, MobBootstrapSpec, UnifiedRuntime};
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

const NAME: &str = "durable-documents-test";

#[path = "support/extension_cross_mob.rs"]
mod cross_mob;

struct Probe {
    context: ToolBundleContext,
    calls: Arc<tokio::sync::Mutex<Vec<(meerkat_core::SessionId, Principal)>>>,
    cross_mob: Option<Arc<cross_mob::Scenario>>,
}
#[async_trait::async_trait]
impl AgentToolDispatcher for Probe {
    fn tools(&self) -> Arc<[Arc<ToolDef>]> {
        ["documents_read", "documents_apply"]
            .into_iter()
            .map(|name| {
                Arc::new(
                    ToolDef::new(
                        name,
                        "Probe extension authority",
                        serde_json::json!({"type":"object","properties":{}}),
                    )
                    .with_provenance(meerkat_core::ToolProvenance {
                        kind: meerkat_core::ToolSourceKind::RustBundle,
                        source_id: NAME.into(),
                    }),
                )
            })
            .collect::<Vec<_>>()
            .into()
    }
    fn tool_mutation_class(&self, name: &str) -> ToolMutationClass {
        if name == "documents_read" {
            ToolMutationClass::ReadOnly
        } else {
            ToolMutationClass::Mutating
        }
    }
    async fn dispatch(
        &self,
        _: ToolCallView<'_>,
    ) -> std::result::Result<ToolDispatchOutcome, ToolError> {
        panic!("native dispatch must carry runner identity")
    }
    async fn dispatch_with_context(
        &self,
        call: ToolCallView<'_>,
        context: &ToolDispatchContext,
    ) -> std::result::Result<ToolDispatchOutcome, ToolError> {
        // Deliberate identity/lineage-looking tool arguments have no authority.
        let caller = self
            .context
            .caller_resolver
            .resolve(context, None)
            .await
            .unwrap();
        if let Some(scenario) = &self.cross_mob {
            return scenario
                .dispatch(&self.context, &caller, call, context)
                .await;
        }
        if call.name == "documents_apply" {
            self.context
                .documents
                .mutate(
                    &caller,
                    &request("first-native-create"),
                    Mutation::Create(NewDocument {
                        content: content(),
                        owner: None,
                        access: DocumentAccess::default(),
                    }),
                )
                .await
                .unwrap();
        }
        self.context
            .documents
            .list(&caller, ListRequest::default())
            .await
            .unwrap();
        self.calls.lock().await.push((
            context.origin_session_id().unwrap().clone(),
            caller.principal().clone(),
        ));
        Ok(ToolResult::new(call.id.to_owned(), "{}".into(), false).into())
    }
}

struct ScriptClient {
    calls: AtomicUsize,
    tool: &'static str,
}
#[async_trait::async_trait]
impl LlmClient for ScriptClient {
    fn project_replay_messages(
        &self,
        messages: &[meerkat_core::Message],
    ) -> std::result::Result<Vec<meerkat_core::Message>, LlmError> {
        Ok(messages.to_vec())
    }
    fn provider(&self) -> meerkat_core::Provider {
        meerkat_core::Provider::OpenAI
    }
    async fn health_check(&self) -> std::result::Result<(), LlmError> {
        Ok(())
    }
    fn stream<'a>(
        &'a self,
        request: &'a LlmRequest,
    ) -> Pin<Box<dyn futures::Stream<Item = std::result::Result<LlmEvent, LlmError>> + Send + 'a>>
    {
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        let mut events = Vec::new();
        if first {
            events.push(Ok(LlmEvent::ToolCallComplete {
                id: "native-origin".into(),
                name: self.tool.into(),
                args: serde_json::json!({"principal":"forged-admin","lineage":["forged-root"]}),
                meta: None,
            }));
        } else {
            events.push(Ok(LlmEvent::TextDelta {
                delta: "done".into(),
                meta: None,
            }));
        }
        events.push(Ok(LlmEvent::UsageUpdate {
            usage: meerkat_core::TurnUsage::host_declared(
                self.provider(),
                &request.model,
                meerkat_core::Usage::default(),
            ),
        }));
        events.push(Ok(LlmEvent::Done {
            outcome: LlmDoneOutcome::Success {
                stop_reason: if first {
                    StopReason::ToolUse
                } else {
                    StopReason::EndTurn
                },
            },
        }));
        Box::pin(futures::stream::iter(events))
    }
}

fn definition(path: &std::path::Path) -> MobDefinition {
    MobDefinition::from_toml(
        r#"
[mob]
id = "{MOB_ID}"
[profiles.worker]
model = "gpt-5.5"
runtime_mode = "turn_driven"
external_addressable = true
[profiles.worker.tools]
comms = true
rust_bundles = ["durable-documents-test"]
"#
        .replace(
            "{MOB_ID}",
            &format!(
                "extension-{}",
                uuid::Uuid::new_v5(
                    &uuid::Uuid::NAMESPACE_OID,
                    path.as_os_str().as_encoded_bytes()
                )
            ),
        )
        .as_str(),
    )
    .unwrap()
}

async fn harness(
    path: &std::path::Path,
    access: Option<meerkat_mobkit::access::AccessController>,
) -> (
    UnifiedRuntime,
    ToolBundleContext,
    Arc<tokio::sync::Mutex<Vec<(meerkat_core::SessionId, Principal)>>>,
) {
    harness_with_client(
        path,
        access,
        Arc::new(ScriptClient {
            calls: AtomicUsize::new(0),
            tool: "documents_read",
        }),
        None,
    )
    .await
}

async fn harness_with_client(
    path: &std::path::Path,
    access: Option<meerkat_mobkit::access::AccessController>,
    client: Arc<dyn LlmClient>,
    cross_mob: Option<Arc<cross_mob::Scenario>>,
) -> (
    UnifiedRuntime,
    ToolBundleContext,
    Arc<tokio::sync::Mutex<Vec<(meerkat_core::SessionId, Principal)>>>,
) {
    let sessions =
        Arc::new(meerkat_store::SqliteSessionStore::open(path.join("sessions.sqlite3")).unwrap());
    let (storage, provenance) =
        meerkat_mobkit::mob_composition_manifest::persistent_mob_storage(path.join("mob.sqlite3"))
            .unwrap();
    let mut definition = definition(path);
    if cross_mob.is_some() {
        let meerkat_mob::ProfileBinding::Inline(profile) = definition
            .profiles
            .get_mut(&meerkat_mob::ProfileName::from("worker"))
            .unwrap()
        else {
            panic!("fixture declares an inline worker profile");
        };
        profile.tools.mob = true;
    }
    let spec = MobBootstrapSpec::persistent(definition, storage, path.to_path_buf(), 16, sessions)
        .unwrap()
        .with_mob_storage_provenance(provenance)
        // Force final child-policy installation so delegate/fork coverage
        // also proves extension bundles and authority survive replacement
        // of the preliminary agent mob-tool state.
        .with_child_application_tool_policy(meerkat_core::ApplicationToolPolicyBinding::Unmanaged)
        .with_options(MobBootstrapOptions {
            allow_ephemeral_sessions: false,
            notify_orchestrator_on_resume: false,
            default_llm_client: Some(client),
        });
    let captured = Arc::new(Mutex::new(None));
    let calls = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let factory_capture = captured.clone();
    let factory_calls = calls.clone();
    let mut builder = UnifiedRuntime::builder()
        .mob_spec(spec)
        .module_config(meerkat_mobkit::MobKitConfig {
            modules: vec![],
            discovery: meerkat_mobkit::DiscoverySpec {
                namespace: "extension-test".into(),
                modules: vec![],
            },
            pre_spawn: vec![],
        })
        .timeout(Duration::from_secs(10))
        .persistent_state(path)
        .register_tool_bundle_factory(
            NAME,
            ToolBundleRequirements::durable_documents(NAME, "documents_read", "documents_apply"),
            Arc::new(move |context: ToolBundleContext| {
                *factory_capture.lock().unwrap() = Some(context.clone());
                Ok(Arc::new(Probe {
                    context,
                    calls: factory_calls.clone(),
                    cross_mob: cross_mob.clone(),
                }) as Arc<dyn AgentToolDispatcher>)
            }),
        );
    if let Some(access) = access {
        builder = builder.access_controller(access);
    }
    let runtime = Box::pin(builder.build()).await.unwrap();
    let context = captured.lock().unwrap().take().unwrap();
    (runtime, context, calls)
}
fn worker(id: &str) -> SpawnMemberSpec {
    SpawnMemberSpec::host_root("worker", AgentIdentity::from(id))
}
async fn member_session(
    runtime: &UnifiedRuntime,
    identity: &AgentIdentity,
) -> meerkat_core::SessionId {
    runtime
        .mob_handle()
        .get_member(identity)
        .await
        .unwrap()
        .unwrap()
        .bridge_session_id()
        .unwrap()
        .clone()
}
async fn caller(
    context: &ToolBundleContext,
    session: &meerkat_core::SessionId,
) -> HostAccessContext {
    caller_at(context, session, "native caller").await
}
async fn caller_at(
    context: &ToolBundleContext,
    session: &meerkat_core::SessionId,
    stage: &str,
) -> HostAccessContext {
    context
        .caller_resolver
        .resolve(
            &ToolDispatchContext::default().with_runtime_identity(session.clone(), None),
            None,
        )
        .await
        .unwrap_or_else(|error| {
            panic!("{stage}: caller resolution failed for {session}: {error:?}")
        })
}
fn request(id: &str) -> RequestIdentity {
    RequestIdentity::new(id, id.as_bytes()).unwrap()
}
fn content() -> DocumentContent {
    DocumentContent {
        title: "private".into(),
        content_type: "application/test".into(),
        schema_version: 1,
        payload: vec![1, 2, 3],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn extension_native_dispatch_private_fork_spawn_revocation_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (runtime, context, calls) = harness(dir.path(), None).await;
    let root = runtime.spawn(worker("root")).await.unwrap();
    let root_session = member_session(&runtime, &root.agent_identity).await;
    let admission = runtime
        .start_member_turn(
            "root",
            ContentInput::Text("probe".into()),
            HandlingMode::Queue,
            meerkat_mob::MemberTurnOptions::new(),
            None,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), admission.turn.wait())
        .await
        .unwrap()
        .unwrap();
    let owner = caller(&context, &root_session).await;
    assert_eq!(
        calls.lock().await.as_slice(),
        &[(root_session.clone(), owner.principal().clone())]
    );
    assert!(matches!(
        context
            .caller_resolver
            .resolve(&ToolDispatchContext::default(), None)
            .await,
        Err(Error::AuthorityUnavailable)
    ));
    let book = context
        .documents
        .mutate(
            &owner,
            &request("create"),
            Mutation::Create(NewDocument {
                content: content(),
                owner: None,
                access: DocumentAccess::default(),
            }),
        )
        .await
        .unwrap()
        .receipt;
    let stranger = runtime.spawn(worker("stranger")).await.unwrap();
    let stranger = caller(
        &context,
        &member_session(&runtime, &stranger.agent_identity).await,
    )
    .await;
    assert!(matches!(
        context.documents.get(&stranger, &book.document_id).await,
        Err(Error::NotFound)
    ));
    let fork = runtime
        .mob_handle()
        .fork_member(&AgentIdentity::from("root"), worker("fork"), None)
        .await
        .unwrap();
    let fork_session = fork.session_id;
    let fork_caller = caller(&context, &fork_session).await;
    assert!(
        context
            .documents
            .get(&fork_caller, &book.document_id)
            .await
            .is_ok()
    );
    let source = runtime
        .mob_handle()
        .capture_member_creation_source(&root_session)
        .await
        .unwrap();
    let child = runtime
        .mob_handle()
        .spawn_spec(worker("child").with_creation_source(source))
        .await
        .unwrap();
    let child_session = member_session(&runtime, &child.agent_identity).await;
    let child_caller = caller(&context, &child_session).await;
    assert!(matches!(
        context
            .documents
            .get(&child_caller, &book.document_id)
            .await,
        Err(Error::NotFound)
    ));
    let shared = context
        .documents
        .mutate(
            &owner,
            &request("share"),
            Mutation::SetAccess {
                id: book.document_id.clone(),
                expected_revision: book.revision,
                access: DocumentAccess {
                    owner_reach: Reach::Descendants,
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap()
        .receipt;
    assert!(
        context
            .documents
            .get(&caller(&context, &child_session).await, &book.document_id)
            .await
            .is_ok()
    );
    let revoked = context
        .documents
        .mutate(
            &owner,
            &request("revoke"),
            Mutation::SetAccess {
                id: book.document_id.clone(),
                expected_revision: shared.revision,
                access: DocumentAccess {
                    owner_reach: Reach::SelfOnly,
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap()
        .receipt;
    assert!(matches!(
        context
            .documents
            .get(&caller(&context, &fork_session).await, &book.document_id)
            .await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        context
            .documents
            .get(&caller(&context, &child_session).await, &book.document_id)
            .await,
        Err(Error::NotFound)
    ));
    let shared = context
        .documents
        .mutate(
            &owner,
            &request("restore-forks"),
            Mutation::SetAccess {
                id: book.document_id.clone(),
                expected_revision: revoked.revision,
                access: DocumentAccess::default(),
            },
        )
        .await
        .unwrap()
        .receipt;
    runtime
        .mob_handle()
        .retire(AgentIdentity::from("root"))
        .await
        .unwrap();
    assert!(
        context
            .documents
            .get(&caller(&context, &fork_session).await, &book.document_id)
            .await
            .is_ok()
    );
    runtime.shutdown().await;
    drop(runtime);
    drop(context);
    let (reopened, context, _) = harness(dir.path(), None).await;
    let fork_caller = caller(&context, &fork_session).await;
    assert_eq!(
        context
            .documents
            .get(&fork_caller, &book.document_id)
            .await
            .unwrap()
            .revision,
        shared.revision
    );
    let replacement = reopened.spawn(worker("root")).await.unwrap();
    let replacement = caller(
        &context,
        &member_session(&reopened, &replacement.agent_identity).await,
    )
    .await;
    assert_ne!(owner.principal(), replacement.principal());
    assert!(matches!(
        context.documents.get(&replacement, &book.document_id).await,
        Err(Error::NotFound)
    ));
    reopened.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn extension_native_current_role_and_roster_labels_bound_host_abac() {
    use meerkat_mobkit::access::{AccessControlConfig, AccessController, AccessEffect, AccessRule};
    let allow = AccessRule {
        id: "allow".into(),
        actions: vec!["extension.*".into()],
        ..Default::default()
    };
    let deny = AccessRule {
        id: "deny".into(),
        effect: AccessEffect::Deny,
        actions: vec!["extension.read".into()],
        roles: vec!["worker".into()],
        match_labels: BTreeMap::from([("restricted".into(), "true".into())]),
        ..Default::default()
    };
    let access = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["operator".into()],
        rules: vec![allow, deny],
        ..Default::default()
    })
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (runtime, context, _) = harness(dir.path(), Some(access.clone())).await;
    let mut spec = worker("restricted");
    spec.labels = Some(BTreeMap::from([("restricted".into(), "true".into())]));
    let member = runtime.spawn(spec).await.unwrap();
    let session = member_session(&runtime, &member.agent_identity).await;
    let denied = caller(&context, &session).await;
    assert!(matches!(
        context
            .documents
            .mutate(
                &denied,
                &request("create"),
                Mutation::Create(NewDocument {
                    content: content(),
                    owner: None,
                    access: DocumentAccess::default()
                })
            )
            .await,
        Err(Error::NotFound)
    ));
    access.delete_rule("deny").unwrap();
    let permitted = caller(&context, &session).await;
    assert!(
        context
            .documents
            .mutate(
                &permitted,
                &request("create"),
                Mutation::Create(NewDocument {
                    content: content(),
                    owner: None,
                    access: DocumentAccess::default()
                })
            )
            .await
            .is_ok()
    );
    runtime.shutdown().await;
}

#[derive(serde::Serialize, serde::Deserialize)]
struct RestartEvidence {
    root: meerkat_core::SessionId,
    fork: meerkat_core::SessionId,
    document: DocumentId,
    revision: Revision,
}

/// Three OS processes share only the configured disk provider. In particular,
/// no resolver, dispatcher, grant or lineage cache survives a boundary.
#[test]
fn extension_native_fresh_process_restart_and_current_revocation() {
    let dir = tempfile::tempdir().unwrap();
    for phase in ["create", "revoke", "verify"] {
        subprocess_phase(dir.path(), phase);
    }
}

#[test]
fn extension_native_fresh_process_unpublished_late_birth_is_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    for phase in ["late-create", "late-reopen"] {
        subprocess_phase(dir.path(), phase);
    }
}

fn subprocess_phase(path: &std::path::Path, phase: &str) {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "extension_subprocess_fixture",
            "--ignored",
            "--nocapture",
        ])
        .env("MOBKIT_EXTENSION_RESTART_PATH", path)
        .env("MOBKIT_EXTENSION_RESTART_PHASE", phase)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "restart phase {phase} timed out:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "restart phase {phase}:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "launched by extension_native_fresh_process_restart_and_current_revocation"]
async fn extension_subprocess_fixture() {
    let path = std::path::PathBuf::from(std::env::var_os("MOBKIT_EXTENSION_RESTART_PATH").unwrap());
    let phase = std::env::var("MOBKIT_EXTENSION_RESTART_PHASE").unwrap();
    let evidence_path = path.join("restart-evidence.json");
    if phase.starts_with("late-") {
        unpublished_late_birth_phase(&path, &phase).await;
        return;
    }
    let (runtime, context, _) = harness(&path, None).await;
    if phase == "create" {
        let root = runtime.spawn(worker("root")).await.unwrap();
        let root = member_session(&runtime, &root.agent_identity).await;
        let owner = caller(&context, &root).await;
        let created = context
            .documents
            .mutate(
                &owner,
                &request("process-create"),
                Mutation::Create(NewDocument {
                    content: content(),
                    owner: None,
                    access: DocumentAccess::default(),
                }),
            )
            .await
            .unwrap()
            .receipt;
        let fork = runtime
            .mob_handle()
            .fork_member(&AgentIdentity::from("root"), worker("fork"), None)
            .await
            .unwrap()
            .session_id;
        let editor = caller(&context, &fork).await;
        assert_eq!(
            context
                .documents
                .get(&editor, &created.document_id)
                .await
                .unwrap()
                .revision,
            created.revision
        );
        let edited = context
            .documents
            .mutate(
                &editor,
                &request("process-edit"),
                Mutation::Replace {
                    id: created.document_id.clone(),
                    expected_revision: created.revision,
                    content: content(),
                },
            )
            .await
            .unwrap()
            .receipt;
        std::fs::write(
            &evidence_path,
            serde_json::to_vec(&RestartEvidence {
                root,
                fork,
                document: edited.document_id,
                revision: edited.revision,
            })
            .unwrap(),
        )
        .unwrap();
    } else {
        let evidence: RestartEvidence =
            serde_json::from_slice(&std::fs::read(&evidence_path).unwrap()).unwrap();
        let editor = caller(&context, &evidence.fork).await;
        if phase == "revoke" {
            assert_eq!(
                context
                    .documents
                    .get(&editor, &evidence.document)
                    .await
                    .unwrap()
                    .revision,
                evidence.revision
            );
            assert!(
                context
                    .documents
                    .lookup_receipt(&editor, &request("process-edit"))
                    .await
                    .unwrap()
                    .is_some()
            );
            let owner = caller(&context, &evidence.root).await;
            context
                .documents
                .mutate(
                    &owner,
                    &request("process-revoke"),
                    Mutation::SetAccess {
                        id: evidence.document.clone(),
                        expected_revision: evidence.revision,
                        access: DocumentAccess {
                            owner_reach: Reach::SelfOnly,
                            ..Default::default()
                        },
                    },
                )
                .await
                .unwrap();
        } else {
            assert_eq!(phase, "verify");
            assert!(matches!(
                context.documents.get(&editor, &evidence.document).await,
                Err(Error::NotFound)
            ));
            assert!(matches!(
                context
                    .documents
                    .lookup_receipt(&editor, &request("process-edit"))
                    .await,
                Err(Error::NotFound)
            ));
        }
    }
    runtime.shutdown().await;
}

struct StableRoster;
#[async_trait::async_trait]
impl meerkat_mobkit::identity_first::contracts::RosterProvider for StableRoster {
    async fn roster(
        &self,
        _: &meerkat_mobkit::identity_first::RosterContext,
    ) -> std::result::Result<
        Vec<meerkat_mobkit::identity_first::DurableAgentSpec>,
        meerkat_mobkit::identity_first::RosterError,
    > {
        use meerkat_mobkit::identity_first::{AgentAddressability, DurableAgentSpec};
        Ok(vec![DurableAgentSpec {
            identity: meerkat_mobkit::identity_first::AgentIdentity::parse("durable:alice")
                .unwrap(),
            profile: meerkat_mob::ProfileName::from("worker"),
            addressability: AgentAddressability::Addressable,
            display_name: None,
            labels: BTreeMap::new(),
            context: None,
            additional_instructions: vec![],
            initial_message: Some(ContentInput::Text("create a document".into())),
            runtime_mode_override: None,
            backend: None,
            binding: None,
            placement: None,
        }])
    }
}
async fn stable_harness(
    path: &std::path::Path,
    access: meerkat_mobkit::access::AccessController,
) -> (
    UnifiedRuntime,
    ToolBundleContext,
    Arc<tokio::sync::Mutex<Vec<(meerkat_core::SessionId, Principal)>>>,
) {
    let sessions =
        Arc::new(meerkat_store::SqliteSessionStore::open(path.join("sessions.sqlite3")).unwrap());
    let (storage, provenance) =
        meerkat_mobkit::mob_composition_manifest::persistent_mob_storage(path.join("mob.sqlite3"))
            .unwrap();
    // Session/continuity persistence alone does not retain the native mob
    // journal. These tests require the original creation facts after reopen.
    let spec =
        MobBootstrapSpec::persistent(definition(path), storage, path.to_path_buf(), 16, sessions)
            .unwrap()
            .with_mob_storage_provenance(provenance)
            .with_options(MobBootstrapOptions {
                allow_ephemeral_sessions: false,
                notify_orchestrator_on_resume: false,
                default_llm_client: Some(Arc::new(ScriptClient {
                    calls: AtomicUsize::new(0),
                    tool: "documents_apply",
                })),
            });
    let captured = Arc::new(Mutex::new(None));
    let calls = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let capture = captured.clone();
    let probe_calls = calls.clone();
    let runtime = Box::pin(
        UnifiedRuntime::builder()
            .mob_spec(spec)
            .module_config(meerkat_mobkit::MobKitConfig {
                modules: vec![],
                discovery: meerkat_mobkit::DiscoverySpec {
                    namespace: "extension-stable".into(),
                    modules: vec![],
                },
                pre_spawn: vec![],
            })
            .timeout(Duration::from_secs(10))
            .persistent_state(path)
            .continuity_from_state_dir(path)
            .await
            .unwrap()
            .roster_provider(Arc::new(StableRoster))
            .identity_bootstrap_mode(
                meerkat_mobkit::identity_first::IdentityBootstrapMode::EagerMaterialize,
            )
            .identity_runtime_instance_id("extension-stable")
            .comms(true)
            .access_controller(access)
            .register_tool_bundle_factory(
                NAME,
                ToolBundleRequirements::durable_documents(
                    NAME,
                    "documents_read",
                    "documents_apply",
                ),
                Arc::new(move |context: ToolBundleContext| {
                    *capture.lock().unwrap() = Some(context.clone());
                    Ok(Arc::new(Probe {
                        context,
                        calls: probe_calls.clone(),
                        cross_mob: None,
                    }) as Arc<dyn AgentToolDispatcher>)
                }),
            )
            .build(),
    )
    .await
    .unwrap();
    let context = captured.lock().unwrap().take().unwrap();
    (runtime, context, calls)
}

#[tokio::test(flavor = "multi_thread")]
async fn extension_native_first_turn_creation_has_stable_owner_after_reset_and_restore() {
    use meerkat_mobkit::access::{AccessControlConfig, AccessController, AccessEffect, AccessRule};
    let access = AccessController::new(AccessControlConfig {
        enabled: true,
        admins: vec!["operator".into()],
        rules: vec![AccessRule {
            id: "allow".into(),
            actions: vec!["extension.*".into()],
            ..Default::default()
        }],
        ..Default::default()
    })
    .unwrap();
    let path = tempfile::tempdir().unwrap();
    let (runtime, context, calls) = stable_harness(path.path(), access.clone()).await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while calls.lock().await.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("initial extension call should wait for publication without blocking spawn reply");
    let (session, principal) = calls.lock().await[0].clone();
    assert_eq!(principal, Principal::Agent("durable:alice".into()));
    let creation = runtime
        .mob_handle()
        .member_creation_for_session(&session)
        .await
        .unwrap()
        .unwrap();
    assert!(creation.creation.creation_id.is_some());
    assert!(matches!(
        creation.creation.provenance,
        meerkat_mob::MemberCreationProvenance::Root
    ));
    let owner = caller_at(&context, &session, "stable owner before reset").await;
    let receipt = context
        .documents
        .lookup_receipt(&owner, &request("first-native-create"))
        .await
        .unwrap()
        .unwrap();
    let book = context
        .documents
        .get(&owner, &receipt.document_id)
        .await
        .unwrap();
    assert_eq!(book.owner, Owner::Agent(principal.clone()));
    access
        .upsert_rule(AccessRule {
            id: "stable-selector".into(),
            effect: AccessEffect::Deny,
            actions: vec!["extension.read".into()],
            agents: vec!["durable:alice".into()],
            ..Default::default()
        })
        .unwrap();
    assert!(matches!(
        context
            .documents
            .get(&caller(&context, &session).await, &receipt.document_id)
            .await,
        Err(Error::NotFound)
    ));
    access.delete_rule("stable-selector").unwrap();
    let identity = meerkat_mobkit::identity_first::AgentIdentity::parse("durable:alice").unwrap();
    let successor = runtime
        .identity_runtime()
        .unwrap()
        .reset(&identity)
        .await
        .unwrap();
    let successor_creation = runtime
        .mob_handle()
        .member_creation_for_session(&successor.session_id)
        .await
        .unwrap()
        .expect("reset successor has a native creation record");
    assert_eq!(
        successor_creation.creation.creation_id,
        creation.creation.creation_id
    );
    assert!(matches!(
        &successor_creation.creation.provenance,
        meerkat_mob::MemberCreationProvenance::Successor {
            predecessor_session_id,
            ..
        } if *predecessor_session_id == session
    ));
    let after_reset = caller_at(&context, &successor.session_id, "stable owner after reset").await;
    assert_eq!(after_reset.principal(), &principal);
    assert!(
        context
            .documents
            .get(&after_reset, &receipt.document_id)
            .await
            .is_ok()
    );
    runtime.shutdown().await;
    drop(runtime);
    drop(context);
    let (runtime, context, _) = stable_harness(path.path(), access).await;
    let session = runtime
        .identity_runtime()
        .unwrap()
        .status(&identity)
        .await
        .unwrap()
        .session_id
        .unwrap();
    assert_eq!(
        session, successor.session_id,
        "restore keeps the reset session"
    );
    let restored_creation = runtime
        .mob_handle()
        .member_creation_for_session(&session)
        .await
        .unwrap()
        .expect("restored successor retains the native creation journal");
    assert_eq!(restored_creation.creation, successor_creation.creation);
    let restored = caller_at(&context, &session, "stable owner after reopen").await;
    assert_eq!(restored.principal(), &principal);
    assert!(
        context
            .documents
            .get(&restored, &receipt.document_id)
            .await
            .is_ok()
    );
    runtime.shutdown().await;
}

async fn late_birth_step<T>(
    phase: &str,
    step: &str,
    future: impl std::future::Future<Output = T>,
) -> T {
    eprintln!("{phase}: starting {step}");
    let result = tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .unwrap_or_else(|_| panic!("{phase}: {step} exceeded 30 seconds"));
    eprintln!("{phase}: completed {step}");
    result
}

async fn unpublished_late_birth_phase(path: &std::path::Path, phase: &str) {
    use meerkat_mobkit::identity_first::{
        AgentIdentity as StableIdentity, AgentRuntimeId, CheckpointVersion, ContinuityGeneration,
        ContinuityRecord, LeaseAcquireResult,
    };
    let (runtime, context, _) = late_birth_step(
        phase,
        "stable host bootstrap",
        stable_harness(
            path,
            meerkat_mobkit::access::AccessController::new(Default::default()).unwrap(),
        ),
    )
    .await;
    let identity = StableIdentity::parse("durable:late").unwrap();
    let target = meerkat_mobkit::member_comms_id::mob_member_id(identity.as_str());
    let evidence = path.join("late-session.json");
    let session = if phase == "late-create" {
        let identity_runtime = runtime.identity_runtime().unwrap();
        let mut grants = late_birth_step(
            phase,
            "acquire provisional identity lease",
            identity_runtime
                .lease_provider()
                .acquire_leases(std::slice::from_ref(&identity), "late-test"),
        )
        .await
        .unwrap();
        let LeaseAcquireResult::Acquired(grant) = grants.remove(&identity).unwrap() else {
            panic!("fresh identity lease");
        };
        let provisional = ContinuityRecord {
            identity: identity.clone(),
            agent_runtime_id: AgentRuntimeId::parse("late-runtime").unwrap(),
            session_id: meerkat_core::SessionId::new(),
            generation: ContinuityGeneration::new(0),
            checkpoint_version: CheckpointVersion::new(0),
        };
        late_birth_step(
            phase,
            "retain provisional identity intent",
            identity_runtime
                .continuity_store()
                .upsert_continuity_record(&provisional, grant.fencing_token),
        )
        .await
        .unwrap();
        let publication = identity_runtime
            .bridge()
            .unwrap()
            .begin_extension_identity_publication(&identity)
            .unwrap()
            .unwrap();
        drop(publication);
        // This is the authoritative outcome of a submitted native spawn that
        // commits after its materialization caller has been canceled. The
        // exact actual binding was never published into continuity.
        late_birth_step(
            phase,
            "commit native late birth",
            runtime
                .mob_handle()
                .spawn_spec(SpawnMemberSpec::host_root("worker", target.clone())),
        )
        .await
        .unwrap();
        let session = late_birth_step(
            phase,
            "read native late birth session",
            member_session(&runtime, &target),
        )
        .await;
        assert!(
            identity_runtime
                .continuity_store()
                .historical_identity_binding(&session)
                .await
                .unwrap()
                .is_none()
        );
        late_birth_step(
            phase,
            "delete provisional continuity record",
            identity_runtime
                .continuity_store()
                .delete_continuity_record(&identity, grant.fencing_token),
        )
        .await
        .unwrap();
        late_birth_step(
            phase,
            "release provisional identity lease",
            identity_runtime.lease_provider().release_leases(&[grant]),
        )
        .await
        .unwrap();
        std::fs::write(&evidence, serde_json::to_vec(&session).unwrap()).unwrap();
        session
    } else {
        serde_json::from_slice(&std::fs::read(&evidence).unwrap()).unwrap()
    };
    assert_eq!(
        late_birth_step(
            phase,
            "verify retained late birth session",
            member_session(&runtime, &target),
        )
        .await,
        session
    );
    eprintln!("{phase}: resolving unpublished late birth");
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            context.caller_resolver.resolve(
                &ToolDispatchContext::default().with_runtime_identity(session, None),
                None,
            )
        )
        .await
        .unwrap(),
        Err(Error::AuthorityUnavailable)
    ));
    eprintln!("{phase}: refused unpublished late birth");
    // An unrelated worker target still gets a fresh immutable worker identity.
    let worker_member = late_birth_step(
        phase,
        "spawn unrelated worker",
        runtime.spawn(worker(if phase == "late-create" {
            "unreserved-before"
        } else {
            "unreserved-after"
        })),
    )
    .await
    .unwrap();
    assert!(matches!(
        late_birth_step(phase, "resolve unrelated worker", async {
            caller(
                &context,
                &member_session(&runtime, &worker_member.agent_identity).await,
            )
            .await
        })
        .await
        .principal(),
        Principal::Worker { .. }
    ));
    late_birth_step(phase, "shutdown stable host", runtime.shutdown()).await;
}
