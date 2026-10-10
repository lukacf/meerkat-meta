//! Application-facing companion layer for the Meerkat runtime.
//!
//! Meerkat owns individual-agent execution and multi-agent mob orchestration.
//! MobKit packages gateway startup, module routing, operational policy,
//! persistence projections, SDK transport, console projection, and operator
//! surfaces.

pub(crate) mod identity_control_target;
pub mod identity_first;
#[cfg(test)]
#[allow(clippy::panic)]
mod test_process_isolation;

pub mod access;
pub mod actor_loop_health;
pub mod auth;
pub mod baseline;
pub mod blob_store;
pub mod capability_invariant;
pub mod compaction_policy;
pub mod config_convention;
pub mod console_aggregator;
pub mod console_auth_config;
pub mod console_config;
pub mod console_contracts;
pub(crate) mod console_spawn;
pub mod console_voice;
pub mod contact_directory;
pub mod council_wiring;
pub mod decisions;
pub mod detached_completion;
#[cfg(test)]
mod detached_delivery_route_tests;
#[cfg(feature = "extension-state")]
pub mod extension_state;
pub(crate) mod fallback_wake;
pub mod fork;
pub mod gateway_composition;
pub mod gateway_wiring;
pub mod governance;
pub mod http_auth;
pub mod http_console;
pub mod http_flow_editor;
pub mod http_sse;
pub mod live_contracts;
pub mod live_wiring;
pub mod member_comms_id;
pub(crate) mod member_status_observation;
pub mod member_tool_policy;
pub mod memory;
pub mod memory_wiring;
pub(crate) mod mob_activation_retry;
pub mod mob_composition_manifest;
pub mod mob_handle_runtime;
pub mod mobpack;
pub mod mocks;
pub mod process;
pub mod protocol;
pub mod public_live_config;
pub mod rpc;
pub mod runtime;
pub mod schedule_wiring;
pub mod shutdown_signal;
pub mod spec_update_ceremony;
pub mod storage_doctor;
pub mod storage_health;
pub mod storage_layout;
pub mod storage_migrate;
pub mod storage_provider;
#[cfg(test)]
mod test_diagnostics;
#[cfg(test)]
mod test_wait;
#[cfg(test)]
mod tool_application_test_support;
pub mod tool_compose;
pub mod topology_control;
pub mod types;
pub mod unified_runtime;
pub mod workgraph_admission;
pub mod workgraph_events;
pub mod workgraph_realm;
pub mod workgraph_wiring;

pub use access::{
    ACCESS_ACTIONS, AccessConfigError, AccessControlConfig, AccessController, AccessDecision,
    AccessEffect, AccessGroup, AccessPrincipal, AccessResource, AccessRule, AccessView,
    AgentResourceAttributes, evaluate_access, validate_access_config,
};
pub use actor_loop_health::{
    ActorLoopHealth, ActorLoopHealthKind, ActorLoopHealthReport, ActorLoopHealthState,
};
pub use auth::{
    GATEWAY_PEER_KEY_FILE, GatewayPeerKeyError, GatewayPeerKeys, Jwk, JwksCache, JwksCacheConfig,
    JwksCacheError, JwksDocument, JwtHeaderView, JwtValidationConfig, JwtValidationError,
    OidcContractError, OidcDiscoveryDocument, PubkeyDecodeError, ValidatedJwt, decode_pubkey_b64,
    extract_hs256_shared_secret, inspect_jwt_header, parse_jwks_json, parse_oidc_discovery_json,
    select_jwk_for_token, validate_jwt_locally,
};
pub use baseline::{
    BaselineVerificationError, BaselineVerificationReport, MEERKAT_REPO_ENV,
    REQUIRED_MEERKAT_SYMBOLS, verify_meerkat_baseline_symbols,
};
pub use blob_store::{
    Base64BlobStoreAdapter, BinaryBlobPayload, BinaryBlobStore, BinaryBlobStoreAdapter,
    ObjectStoreBlobStore,
};
pub use compaction_policy::{
    COMPACTION_POLICY_KEYS, apply_compaction_policy, parse_compaction_policy,
    validate_compaction_policy,
};
pub use config_convention::ConventionalPaths;
pub use console_aggregator::{
    AllowAllConsoleVisibilityPolicy, AppendDisposition, AppendOutcome, ConsoleAggregatorOptions,
    ConsoleCursor, ConsoleFrame, ConsoleFrameMemberProvenance, ConsoleFrameSource,
    ConsoleFrameSourceKind, ConsoleFrameStatus, ConsoleIdentityInspection, ConsoleIdentityRecord,
    ConsoleInteractionAccepted as ConsoleTimelineInteractionAccepted, ConsoleLogError,
    ConsoleLogResult, ConsoleLogStore, ConsoleReplayUnavailable, ConsoleRuntimeRegistration,
    ConsoleSendDedupeDurability, ConsoleSendRequest, ConsoleTimelineEvent, ConsoleTimelineMode,
    ConsoleTimelinePage, ConsoleTimelineQuery, ConsoleTimelineQueryError,
    ConsoleTimelineQueryResult, ConsoleTimelineWindowPage, ConsoleTimelineWindowQuery,
    ConsoleTurnOrigin, ConsoleVisibility, ConsoleVisibilityPolicy,
    HideImplicitDelegateMembersConsoleVisibilityPolicy, InMemoryConsoleLogStore,
    MobKitConsoleAggregator, NewConsoleFrame, ReplaySubscriptionEffect, ReplaySubscriptionState,
    ReplaySubscriptionTransition, SendEffect, SendState, SendTransition, SourceIngestionEffect,
    SourceIngestionState, SourceIngestionTransition, SqliteConsoleLogStore,
};
pub use console_config::{
    ConsoleActionsUiConfig, ConsoleAgentBadgeConfig, ConsoleAgentListConfig,
    ConsoleAgentSectionConfig, ConsoleAppearanceConfig, ConsoleBrandingConfig, ConsoleConfigError,
    ConsoleEnvironmentConfig, ConsoleLayoutConfig, ConsoleRailFilterPresetConfig,
    ConsoleRailUiConfig, ConsoleSidebarButtonConfig, ConsoleSidebarUiConfig, ConsoleUiConfig,
    load_console_ui_config_from_path_for_realm, load_console_ui_config_from_toml,
    load_console_ui_config_from_toml_for_realm,
};
pub use console_contracts::{
    ConsoleIdentityEventEnvelope, ConsoleInteractionRejectedError, ReplayUnavailableError,
};
pub use decisions::{
    AuthPolicy, AuthProvider, BigQueryNaming, ConsoleAccessRequest, ConsolePolicy,
    DecisionPolicyError, MetricsPolicy, REQUIRED_RELEASE_TARGETS, ReleaseMetadata,
    RuntimeOpsPolicy, enforce_console_route_access, load_trusted_mobkit_modules_from_toml,
    parse_release_metadata_json, validate_bigquery_naming, validate_release_metadata,
    validate_runtime_ops_policy,
};
#[allow(deprecated)]
pub use governance::validate_phase0_governance_contracts;
pub use governance::{
    GovernanceValidationError, STRICT_TRACEABILITY_STATUSES, validate_governance_contracts,
    validate_governance_state, validate_traceability_statuses,
};
pub use http_auth::{auth_middleware, with_auth_layer};
pub use http_console::{
    ConsoleJsonState, console_frontend_app_js_handler, console_frontend_index_handler,
    console_frontend_router, console_json_handler, console_json_router,
    console_json_router_with_aggregator, console_json_router_with_aggregator_and_access,
    console_json_router_with_runtime,
};
pub use http_flow_editor::{
    flow_editor_frontend_app_css_handler, flow_editor_frontend_app_js_handler,
    flow_editor_frontend_index_handler, flow_editor_frontend_router,
    flow_editor_frontend_vendor_js_handler, flow_editor_router,
    flow_editor_router_with_host_deploy, flow_editor_rpc_handler,
    flow_editor_rpc_handler_allowing_host_deploy, flow_editor_rpc_router,
    flow_editor_rpc_router_allowing_host_deploy,
};
pub use http_sse::{
    AgentEventSubscribeFn, MobEventSubscribeFn, agent_event_sse, agent_events_sse_router,
    agent_events_sse_router_with_access, mob_events_sse_router, mob_events_sse_router_with_access,
    mob_structural_events_sse_router, mob_structural_events_sse_router_with_access,
};
pub use identity_first::{
    AgentMemoryConfig, AgentMemoryCustomizer, AgentMemoryError, AgentMemoryLlmWrites,
    AgentMemoryOperatorScope, AgentMemoryPerTurnInjection, AgentMemoryProvider,
    AgentMemoryRecallFailurePolicy, AgentMemoryRecallRequest, AgentMemoryRecord,
    AgentMemoryRuntimeInjector, AgentMemorySelection, AuthoredWriteReceipt, MEMORY_TOOL_NAME,
    NewAgentMemory,
};
pub use live_contracts::{
    ActiveLiveChannelHandle, ExperimentalLiveChannelStatus, FeatureCapability,
    LIVE_EXECUTION_CLIENT_CONTEXT_V1, LIVE_EXECUTION_FUNCTION_BRIDGE_V1,
    LIVE_EXECUTION_IDENTITY_V1, LiveChannelHandle, LiveExecutionIdentityContractError,
    LiveExecutionIdentityV1, LiveExecutionIdentityVersion, LiveExecutionMode,
    LivePlaybackOwnerReadiness, PendingLiveChannelHandle, parse_live_open_execution_identity,
    validate_experimental_live_open_surface, validate_experimental_live_target_surface,
};
pub use meerkat_mob::{MemberTurnEventSender, MemberTurnHandle, MemberTurnOptions};
pub use meerkat_mob_mcp::ChildToolBundleAvailability;
pub use memory::{
    CompactionResetSink, ConsolePrincipalOperatorResolver, ContentTrustConfig, DispatchTaintSlot,
    DistillCause, DistillerConfig, DistillerEngine, DreamOutcome, DreamRun, HygieneCause,
    HygieneOutcome, HygienistConfig, HygienistEngine, ManifestTier, MemberAgentEventSink,
    MemoryConflictBridge, MemoryEventSink, MemoryGatingBridge, MemoryKind, MemoryPanelStore,
    MemoryRecord, MemoryScope, MemorySpawnCustomizer, MemoryTimelineEvent, MobPurposeSource,
    NewMemoryRecord, OperatorResolver, PromotionGateResolver, QuarantineDecision,
    QuarantineReviewError, QuarantineReviewOutcome, QuarantineReviewRefusal,
    QuarantineReviewRequest, QuarantineReviewer, RecordMeta, ReviewDecision, ReviewedRecordReceipt,
    SessionStoreEvidenceResolver, SessionTaintTracker, SqliteAgentMemoryStore, StagedMemoryStore,
    StagedMutationBatch, StagedOp, StewardConfig, StewardEngine, StewardStore, StewardTriggers,
    TaintLlmWriteGate, TaintObserverGuard, TaintableStore, TrustTier, spawn_member_event_observer,
    spawn_taint_observer,
};
pub use mob_handle_runtime::{
    AfterCreateHook, CapabilityFlags, MobBootstrapOptions, MobBootstrapSpec, MobRuntime,
    MobRuntimeDelivery, MobRuntimeDeliveryError, MobRuntimeError, RealMobRuntime,
    SessionCreatedContext, SessionHook, member_entry_to_json, send_message_on_mob,
};
pub use mobpack::{
    MOBPACK_MEDIA_TYPE, MOBPACK_SCHEMA_VERSION, MobpackDeployCommandResult, MobpackDiagnostic,
    MobpackDocument, MobpackExportResult, MobpackValidationResult, deploy_command_preview,
    export_mobpack, import_mobpack, mobpack_schema_response, validate_mobpack,
};
pub use mocks::{MockModuleProcess, MockProcessError};
pub use process::{ProcessBoundaryError, run_process_json_line};
pub use protocol::{ProtocolParseError, parse_module_event_line, parse_unified_event_line};
pub use rpc::{
    CAPABILITY_UNAVAILABLE_CODE, COMPOSITION_PROVENANCE_CODE,
    CONSOLE_TIMELINE_REPLAY_UNAVAILABLE_CODE, INIT_IN_PROGRESS_CODE, IdentityFirstContext,
    JsonRpcError, JsonRpcRequest, JsonRpcResponse, MEMORY_BACKEND_UNAVAILABLE_CODE,
    MEMORY_QUARANTINE_REVIEW_REFUSED_CODE, MOB_EVENTS_STALE_CURSOR_CODE, MOBKIT_CONTRACT_VERSION,
    STORAGE_RESOLUTION_CODE, SerializedRpcResponseDelivery, handle_console_ingress_json,
    handle_mobkit_rpc_json, handle_unified_rpc_json, handle_unified_rpc_json_arc,
    handle_unified_rpc_json_with_live_arc_delivery,
};
pub use rpc::{RpcCapabilities, RpcCapabilitiesError, parse_rpc_capabilities};
pub use runtime::{
    BaselineRuntimeError, BigQueryGcConfig, BigQuerySessionStoreAdapter, BigQuerySessionStoreError,
    ConfigResolutionError, ConsoleAgentLiveSnapshot, ConsoleLiveSnapshot, ConsoleModelCapabilities,
    ConsoleRestJsonRequest, ConsoleRestJsonResponse, DecisionRuntimeError,
    ElephantMemoryBackendConfig, ElephantMemoryStoreError, GatingAuditEntry, GatingDecideError,
    GatingDecideRequest, GatingDecision, GatingDecisionResult, GatingEvaluateRequest,
    GatingEvaluateResult, GatingIdUnavailable, GatingOrigin, GatingOutcome, GatingPendingEntry,
    GatingRiskTier, GatingStateRestoreError, GatingStateSnapshot, InMemoryMetadataStore,
    JsonFileSessionStore, JsonFileSessionStoreError, JsonStoreLockRecord, LifecycleEvent,
    LifecycleStage, LocalJsonMemoryBackendConfig, LocalJsonMemoryStoreError, McpBoundaryError,
    MemberIdleRetireOverrideRecord, MemoryAssertion, MemoryBackendConfig, MemoryConflictSignal,
    MemoryIndexError, MemoryIndexRequest, MemoryIndexResult, MemoryQueryRequest, MemoryQueryResult,
    MemoryStoreInfo, MetadataScope, MetadataStoreError, MobkitRuntimeError, MobkitRuntimeHandle,
    ModuleHealthState, ModuleHealthTransition, ModuleRouteError, ModuleRouteRequest,
    ModuleRouteResponse, NormalizationError, PersistentMetadataStore, RpcRouteError,
    RpcRuntimeError, RuntimeBoundaryError, RuntimeDecisionInputs, RuntimeDecisionState,
    RuntimeFromConfigError, RuntimeMetadataTable, RuntimeMutationError, RuntimeOptions,
    RuntimeRoute, RuntimeRouteMutationError, RuntimeShutdownReport, SessionPersistenceRow,
    SessionStoreContract, SessionStoreKind, SqliteMetadataStore, SubscribeRequest,
    SubscribeResponse, SubscribeScope, SupervisorReport, TrustedOidcRuntimeConfig, WILDCARD_ROUTE,
    build_runtime_decision_state, handle_console_rest_json_route,
    handle_console_rest_json_route_with_snapshot,
    handle_console_rest_json_route_with_snapshot_and_access, materialize_latest_session_rows,
    materialize_live_session_rows, normalize_event_line, route_module_call,
    route_module_call_rpc_json, route_module_call_rpc_subprocess, run_discovered_module_once,
    run_meerkat_baseline_verification_once, run_module_boundary_once,
    run_rpc_capabilities_boundary_once, session_store_contracts, start_mobkit_runtime,
    start_mobkit_runtime_with_options, validate_trusted_oidc_runtime_config,
};
pub use storage_doctor::{
    DoctorOptions, MobKitStorageMigrator, diagnose_state_dir, diagnose_state_dir_blocking,
    diagnose_state_dir_blocking_with_options, diagnose_state_dir_with_options,
    diagnose_state_dir_with_runtime,
};
pub use storage_health::{
    BlobDurability, BlobStoreResolutionError, ResolvedStorageSummary, RuntimeStoreResolutionError,
    StorageResolutionError, StorageSlotDurability, StorageSlotSummary,
    probe_session_store_incremental,
};
pub use storage_layout::{
    DatabaseProvenance, DatabaseResolution, DatabaseSlot, DatabaseSummary, MobKitStorageLayout,
    ResolvedDatabase, StateDirDurability, StorageLayoutError, StorageLayoutSummary,
    default_ephemeral_scratch_root, default_gateway_home,
};
pub use storage_migrate::{
    DivergenceStatus, FileRenameEntry, LedgerBaselineAction, LedgerBaselineEntry, MigrateMode,
    MobKitMaintenanceFence, MobKitMigrateReport, MobKitPruneArtifact, MobKitPruneReport,
    PruneAction, PruneArtifactKind, RenameAction, RowDivergenceEntry, SiblingRename, TwinReport,
    TwinResolution, enumerate_state_dir_artifacts, enumerate_state_dir_databases,
    is_registered_backup_artifact_name, is_registered_quarantine_artifact_name, migrate_state_dir,
    migrate_state_dir_acknowledging_skipped, prune_state_dir,
};
pub use storage_provider::{
    DiskMobKitStorageProvider, MobKitLeaseAuthority, MobKitRealmOpenContext, MobKitRealmStoreSet,
    MobKitStorageProvider, MobKitStorageProviderError, REQUIRED_MOBKIT_DURABILITY_DOMAINS,
    enforce_fail_closed_store_set,
};
pub use topology_control::{
    SameProcessTopologyCoordinator, TopologyAction, TopologyApplyRequest,
    TopologyBilateralApplyRequest, TopologyBilateralPlan, TopologyBilateralPlanRequest,
    TopologyBilateralSnapshot, TopologyBootstrapConfig, TopologyControlError, TopologyControlMode,
    TopologyControlPolicy, TopologyController, TopologyEdge, TopologyEdgeResult,
    TopologyEdgeResultStatus, TopologyEdgeSnapshot, TopologyEndpoint, TopologyMutation,
    TopologyNodeAffordances, TopologyNodeSnapshot, TopologyOperationReceipt,
    TopologyOperationRecord, TopologyOperationRecordStatus, TopologyOperationStatus, TopologyPlan,
    TopologyPlanRequest, TopologyPlannedEdge, TopologyRevisionTransition, TopologyRuntimeHandle,
    TopologySnapshot,
};
pub use types::{
    AgentDiscoverySpec, DiscoverySpec, EventEnvelope, MobKitConfig, MobStructuralEventEnvelope,
    ModuleConfig, ModuleEvent, PreSpawnData, RestartPolicy, UnifiedEvent,
};
pub use unified_runtime::{
    CompactionPreservedHistoryFit, DEFAULT_REFERENCE_APP_MAX_CONCURRENT_REQUESTS, DesiredPeerEdge,
    DesiredPeerEdgeError, Discovery, EdgeDiscovery, EdgeReconcileFailure, ErrorEvent, ErrorHook,
    EventLogConfig, EventLogStore, EventQuery, IdentityAuthorityReleaseOutcome,
    IdentityBootstrapMode, MOB_STOP_FLOW_SETTLE_BUDGET, MemberTurnAdmission,
    MobStopFlowRunsUnsettled, MobStopOutcome, MobTerminalShutdownOutcome, PersistedEvent,
    PostReconcileHook, PostSpawnHook, PreSpawnContext, PreSpawnHook, RediscoverReport,
    ShutdownDrainReport, UnifiedRuntime, UnifiedRuntimeBootstrapError,
    UnifiedRuntimeBootstrapPhase, UnifiedRuntimeBootstrapPhaseObserver, UnifiedRuntimeBuilder,
    UnifiedRuntimeBuilderError, UnifiedRuntimeBuilderField, UnifiedRuntimeError,
    UnifiedRuntimeReconcileEdgesReport, UnifiedRuntimeReconcileError,
    UnifiedRuntimeReconcileReport, UnifiedRuntimeReconcileRoutingReport, UnifiedRuntimeRunReport,
    UnifiedRuntimeShutdownReport, discovery_spec_to_spawn_spec,
};
pub use workgraph_realm::{
    AttentionTargetRealm, MigratedMemberBinding, WorkGraphRealmMigrationMode,
    WorkGraphRealmMigrationOutcome, WorkGraphRealmMigrationReport,
};
