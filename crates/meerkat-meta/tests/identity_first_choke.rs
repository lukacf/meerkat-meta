#![allow(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    unused_imports,
    redundant_semicolons,
    clippy::redundant_clone
)]
//! Integration chokepoint tests for identity-first continuity (Phase 4).
//!
//! CHOKE tests verify data boundaries between components. Each test traces
//! data flow across a boundary and asserts on both sides.
//!
//! Test naming convention: `identity_first_choke_NN_<description>`
//! to match the `test(identity_first)` filter.
//!
//! Note on gateway-dependent CHOKEs (CHOKE-08 through CHOKE-10, CHOKE-13
//! through CHOKE-15): These trace data across the gateway wire format and are
//! partially covered by the gateway bridge unit tests in `gateway_bridges.rs`.
//! The Rust-side chokepoint (serialization + deserialization of typed newtypes)
//! is verified here; the full SDK round-trip is in the Python/TypeScript test suites.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use meerkat_meta::identity_first::contracts::{
    AgentCustomizer, ContinuityStore, LeaseProvider, TopologyProvider,
};
use meerkat_meta::identity_first::gateway_bridges::{CallbackBridge, GatewayContinuityStore};
use meerkat_meta::identity_first::orchestrator::{RestoreOutcome, restore_flow};
use meerkat_meta::identity_first::{
    AgentAddressability, AgentBuildContext, AgentBuildDraft, AgentIdentity, AgentRuntimeId,
    CheckpointVersion, ContinuityFailure, ContinuityFailureKind, ContinuityGeneration,
    ContinuityRecord, ContinuityResolveState, ContinuityStoreError, CustomizerError, DispatchInput,
    DispatchOrigin, DurabilityPolicy, DurableAgentSpec, ExternalToolDef, FencingToken,
    IdentityLifecycleState, IdentityRuntime, IdentityRuntimeConfig, IdentityRuntimeError,
    LeaseAcquireResult, LeaseGrant, LeaseRenewResult, ManagedPeerEdge, SessionSnapshot,
    TopologyContext, TopologyError,
};
use meerkat_meta::identity_first::{
    ContinuitySessionStoreAdapter, LocalContinuityStore, LocalLeaseProvider,
    SessionHookCustomizerAdapter,
};
use serde_json::{Value, json};

// ===========================================================================
// Helpers
// ===========================================================================

fn make_identity(name: &str) -> AgentIdentity {
    AgentIdentity::parse(name).unwrap()
}

fn make_spec(name: &str) -> DurableAgentSpec {
    DurableAgentSpec {
        identity: make_identity(name),
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

fn make_internal_spec(name: &str) -> DurableAgentSpec {
    DurableAgentSpec {
        addressability: AgentAddressability::InternalOnly,
        ..make_spec(name)
    }
}

fn make_record(name: &str, generation: u64, cpv: u64) -> ContinuityRecord {
    ContinuityRecord {
        identity: make_identity(name),
        agent_runtime_id: AgentRuntimeId::parse(&format!("rt:{name}:{generation}")).unwrap(),
        session_id: meerkat_core::types::SessionId::new(),
        generation: ContinuityGeneration::new(generation),
        checkpoint_version: CheckpointVersion::new(cpv),
    }
}

fn make_grant(name: &str, token: u64) -> LeaseGrant {
    LeaseGrant {
        identity: make_identity(name),
        fencing_token: FencingToken::new(token),
        ttl: Duration::from_mins(5),
    }
}

fn make_runtime(store: Arc<dyn ContinuityStore>, lease: Arc<dyn LeaseProvider>) -> IdentityRuntime {
    IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: store,
        lease_provider: lease,
        runtime_instance_id: "choke-runtime".to_string(),
        has_runtime_store: false,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: None,
        default_timeout: None,
    })
}

fn make_content() -> meerkat_core::ContentInput {
    meerkat_core::ContentInput::Text("hello".to_string())
}

fn make_dispatch_input() -> DispatchInput {
    DispatchInput {
        content: make_content(),
        origin: DispatchOrigin::Connector,
        correlation_id: None,
        idempotency_key: None,
    }
}

// ===========================================================================
// Mock callback bridge for gateway tests
// ===========================================================================

struct MockBridgeInner {
    calls: tokio::sync::Mutex<Vec<(String, Value)>>,
    responses: tokio::sync::Mutex<std::collections::HashMap<String, Result<Value, String>>>,
}

/// Newtype wrapper to satisfy orphan rules for `CallbackBridge` impl.
struct MockBridge(Arc<MockBridgeInner>);

impl MockBridge {
    fn new() -> Self {
        Self(Arc::new(MockBridgeInner {
            calls: tokio::sync::Mutex::new(Vec::new()),
            responses: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        }))
    }

    async fn set_response(&self, method: &str, response: Result<Value, String>) {
        self.0
            .responses
            .lock()
            .await
            .insert(method.to_string(), response);
    }

    #[allow(dead_code)]
    async fn last_call(&self) -> (String, Value) {
        self.0
            .calls
            .lock()
            .await
            .last()
            .cloned()
            .unwrap_or_else(|| (String::new(), Value::Null))
    }
}

#[async_trait]
impl CallbackBridge for MockBridge {
    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        self.0.calls.lock().await.push((method.to_string(), params));
        let responses = self.0.responses.lock().await;
        match responses.get(method) {
            Some(Ok(v)) => Ok(v.clone()),
            Some(Err(e)) => Err(e.clone()),
            None => Ok(Value::Null),
        }
    }
}

// ===========================================================================
// CHOKE-01: RosterProvider → DurableAgentSpec → ContinuityStore.resolve_many
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_01_roster_to_resolve_many_mapping() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease_prov = Arc::new(LocalLeaseProvider::new());
    let runtime = make_runtime(store.clone(), lease_prov.clone());

    // Roster produces identities
    let roster = vec![
        make_spec("triage:main"),
        make_spec("worker:alpha"),
        make_spec("gate:main"),
    ];

    // restore_flow extracts identities and calls resolve_many
    let result = restore_flow(&runtime, &roster, None, None).await.unwrap();

    // The BTreeMap keys from resolve must map back to the correct DurableAgentSpec
    assert_eq!(result.outcomes.len(), 3);
    for spec in &roster {
        assert!(
            result.outcomes.contains_key(&spec.identity),
            "resolve result must contain key for roster identity {}",
            spec.identity
        );
    }

    // Verify the store got records for all identities
    let identities: Vec<AgentIdentity> = roster.iter().map(|s| s.identity.clone()).collect();
    let resolved = store.resolve_many(&identities).await.unwrap();
    assert_eq!(resolved.len(), 3);
    for id in &identities {
        assert!(
            resolved.contains_key(id),
            "store resolve_many must return entry for {id}"
        );
    }
}

// ===========================================================================
// CHOKE-02: LeaseProvider.acquire → FencingToken → ContinuityStore.save
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_02_fencing_token_flows_unmodified() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease_prov = Arc::new(LocalLeaseProvider::new());

    let id = make_identity("triage:main");

    // Acquire lease → get fencing token
    let grants = lease_prov
        .acquire_leases(&[id.clone()], "choke-runtime")
        .await
        .unwrap();
    let grant = match grants.get(&id).unwrap() {
        LeaseAcquireResult::Acquired(g) => g.clone(),
        _ => panic!("expected Acquired"),
    };
    let token = grant.fencing_token;

    // Upsert record with this token
    let record = make_record("triage:main", 0, 0);
    store
        .upsert_continuity_record(&record, token)
        .await
        .unwrap();

    // Save snapshot with same token → ok
    store
        .save_session_snapshot(
            &id,
            &record.session_id,
            record.generation,
            CheckpointVersion::new(1),
            token,
            &SessionSnapshot {
                data: b"data".to_vec(),
            },
        )
        .await
        .unwrap();

    // Save with a stale token (lower) → rejected
    let stale = FencingToken::new(token.get().saturating_sub(1));
    if stale.get() < token.get() {
        let err = store
            .save_session_snapshot(
                &id,
                &record.session_id,
                record.generation,
                CheckpointVersion::new(2),
                stale,
                &SessionSnapshot {
                    data: b"stale".to_vec(),
                },
            )
            .await;
        assert!(err.is_err(), "stale fencing token must be rejected");
    }
}

// ===========================================================================
// CHOKE-03: ContinuityStore.load_session_snapshot → Meerkat session restore
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_03_session_snapshot_restore_path() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());

    let id = make_identity("triage:main");
    let record = make_record("triage:main", 0, 0);

    // Persist record and snapshot
    store
        .upsert_continuity_record(&record, FencingToken::new(1))
        .await
        .unwrap();

    let original_data = b"session-history-data-v1";
    store
        .save_session_snapshot(
            &id,
            &record.session_id,
            record.generation,
            CheckpointVersion::new(1),
            FencingToken::new(1),
            &SessionSnapshot {
                data: original_data.to_vec(),
            },
        )
        .await
        .unwrap();

    // Load the snapshot back
    let loaded = store
        .load_session_snapshot(&record.session_id)
        .await
        .unwrap()
        .expect("snapshot should exist");

    // Verify data integrity
    assert_eq!(
        loaded.data, original_data,
        "snapshot data must survive save/load round-trip"
    );

    // Verify this works through the restore_flow path
    let lease_prov = Arc::new(LocalLeaseProvider::new());
    let runtime = make_runtime(store.clone(), lease_prov.clone());
    let roster = vec![make_spec("triage:main")];
    let result = restore_flow(&runtime, &roster, None, None).await.unwrap();

    match result.outcomes.get(&id).unwrap() {
        RestoreOutcome::Resumed { record, .. } => {
            assert_eq!(record.identity, id);
        }
        other => panic!("expected Resumed, got: {other:?}"),
    }
}

// ===========================================================================
// CHOKE-04: TopologyProvider edges → comms wiring
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_04_topology_edges_to_wiring() {
    struct ExactTopology;

    #[async_trait]
    impl TopologyProvider for ExactTopology {
        async fn compute_edges(
            &self,
            _target: &[AgentIdentity],
            _ctx: &TopologyContext,
        ) -> Result<Vec<ManagedPeerEdge>, TopologyError> {
            Ok(vec![
                ManagedPeerEdge::new(
                    AgentIdentity::parse("a:main").unwrap(),
                    AgentIdentity::parse("b:main").unwrap(),
                )
                .unwrap(),
            ])
        }
    }

    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease_prov = Arc::new(LocalLeaseProvider::new());
    let runtime = make_runtime(store.clone(), lease_prov.clone());

    let roster = vec![make_spec("a:main"), make_spec("b:main")];
    let result = restore_flow(&runtime, &roster, Some(&ExactTopology), None)
        .await
        .unwrap();

    // Verify the topology provider result flows through to restore_flow output
    assert_eq!(result.managed_edges.len(), 1);
    let edge = &result.managed_edges[0];
    assert_eq!(edge.a(), &make_identity("a:main"));
    assert_eq!(edge.b(), &make_identity("b:main"));

    // Verify both identities are registered (wiring would use their AgentRuntimeIds)
    assert!(runtime.contains(&make_identity("a:main")).await);
    assert!(runtime.contains(&make_identity("b:main")).await);
}

// ===========================================================================
// CHOKE-05: Builder persistent_state → bundled LocalContinuityStore + LocalLeaseProvider
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_05_builder_bundled_providers() {
    // Verify LocalContinuityStore and LocalLeaseProvider are functional and satisfy contracts
    let store = LocalContinuityStore::in_memory().unwrap();
    let lease_prov = LocalLeaseProvider::new();

    let id = make_identity("triage:main");

    // Lease: monotonic tokens
    let grants1 = lease_prov
        .acquire_leases(&[id.clone()], "inst-1")
        .await
        .unwrap();
    let t1 = match grants1.get(&id).unwrap() {
        LeaseAcquireResult::Acquired(g) => g.fencing_token,
        _ => panic!("expected Acquired"),
    };
    lease_prov
        .release_leases(&[LeaseGrant {
            identity: id.clone(),
            fencing_token: t1,
            ttl: Duration::from_secs(30),
        }])
        .await
        .unwrap();

    let grants2 = lease_prov
        .acquire_leases(&[id.clone()], "inst-1")
        .await
        .unwrap();
    let t2 = match grants2.get(&id).unwrap() {
        LeaseAcquireResult::Acquired(g) => g.fencing_token,
        _ => panic!("expected Acquired"),
    };
    assert!(
        t2.get() > t1.get(),
        "fencing tokens must be monotonic: t1={}, t2={}",
        t1.get(),
        t2.get()
    );

    // Store: functional CRUD
    let record = make_record("triage:main", 0, 0);
    store.upsert_continuity_record(&record, t2).await.unwrap();
    let resolved = store.resolve_many(&[id.clone()]).await.unwrap();
    assert!(matches!(
        resolved.get(&id).unwrap(),
        ContinuityResolveState::Ready { .. }
    ));
}

// ===========================================================================
// CHOKE-06: DurableAgentSpec.addressability → send/dispatch enforcement
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_06_addressability_to_delivery_enforcement() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease = Arc::new(LocalLeaseProvider::new());
    let runtime = make_runtime(store, lease);

    // Register Addressable
    runtime
        .register(
            make_spec("public:main"),
            IdentityLifecycleState::Active,
            Some(make_record("public:main", 0, 0)),
            Some(make_grant("public:main", 1)),
        )
        .await;

    // Register InternalOnly
    runtime
        .register(
            make_internal_spec("internal:main"),
            IdentityLifecycleState::Active,
            Some(make_record("internal:main", 0, 0)),
            Some(make_grant("internal:main", 2)),
        )
        .await;

    // Trace: addressability from roster → runtime → delivery enforcement
    let status_public = runtime.status(&make_identity("public:main")).await.unwrap();
    assert_eq!(
        status_public.addressability,
        AgentAddressability::Addressable
    );

    let status_internal = runtime
        .status(&make_identity("internal:main"))
        .await
        .unwrap();
    assert_eq!(
        status_internal.addressability,
        AgentAddressability::InternalOnly
    );

    // send() enforcement matches addressability
    assert!(
        runtime
            .send(&make_identity("public:main"), &make_content())
            .await
            .is_ok()
    );
    assert!(matches!(
        runtime
            .send(&make_identity("internal:main"), &make_content())
            .await,
        Err(IdentityRuntimeError::NotAddressable(_))
    ));

    // dispatch() works for both
    assert!(
        runtime
            .dispatch(&make_identity("public:main"), &make_dispatch_input())
            .await
            .is_ok()
    );
    assert!(
        runtime
            .dispatch(&make_identity("internal:main"), &make_dispatch_input())
            .await
            .is_ok()
    );
}

// ===========================================================================
// CHOKE-07: AgentIdentity → AgentRuntimeId → MeerkatId mapping
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_07_identity_to_runtime_id_mapping() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease_prov = Arc::new(LocalLeaseProvider::new());
    let runtime = make_runtime(store.clone(), lease_prov.clone());

    let roster = vec![make_spec("triage:main"), make_spec("worker:main")];
    let result = restore_flow(&runtime, &roster, None, None).await.unwrap();

    // Collect mappings from first boot
    let mut mappings: BTreeMap<AgentIdentity, AgentRuntimeId> = BTreeMap::new();
    for (id, outcome) in &result.outcomes {
        if let RestoreOutcome::Created { record, .. } = outcome {
            mappings.insert(id.clone(), record.agent_runtime_id.clone());
        }
    }

    // Verify mapping is stable: status() returns the same AgentRuntimeId
    for (id, expected_rid) in &mappings {
        let status = runtime.status(id).await.unwrap();
        assert_eq!(
            status.agent_runtime_id.as_ref().unwrap(),
            expected_rid,
            "AgentRuntimeId mapping must be stable in status for {id}"
        );
    }

    // Verify mapping persists in ContinuityStore
    let identities: Vec<AgentIdentity> = roster.iter().map(|s| s.identity.clone()).collect();
    let resolved = store.resolve_many(&identities).await.unwrap();
    for (id, expected_rid) in &mappings {
        match resolved.get(id).unwrap() {
            ContinuityResolveState::Ready { record } => {
                assert_eq!(
                    &record.agent_runtime_id, expected_rid,
                    "AgentRuntimeId in store must match runtime for {id}"
                );
            }
            other => panic!("expected Ready for {id}, got: {other:?}"),
        }
    }
}

// ===========================================================================
// CHOKE-08: Gateway wire format — newtypes as primitives
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_08_gateway_newtype_serialization() {
    // Verify Rust newtypes serialize to JSON primitives correctly
    let id = AgentIdentity::parse("triage:main").unwrap();
    let id_json = serde_json::to_value(&id).unwrap();
    assert!(
        id_json.is_string(),
        "AgentIdentity must serialize as string"
    );
    assert_eq!(id_json.as_str().unwrap(), "triage:main");

    let ft = FencingToken::new(42);
    let ft_json = serde_json::to_value(&ft).unwrap();
    assert!(
        ft_json.is_number(),
        "FencingToken must serialize as integer"
    );
    assert_eq!(ft_json.as_u64().unwrap(), 42);

    let cv = CheckpointVersion::new(7);
    let cv_json = serde_json::to_value(&cv).unwrap();
    assert!(
        cv_json.is_number(),
        "CheckpointVersion must serialize as integer"
    );
    assert_eq!(cv_json.as_u64().unwrap(), 7);

    let cg = ContinuityGeneration::new(3);
    let cg_json = serde_json::to_value(&cg).unwrap();
    assert!(
        cg_json.is_number(),
        "ContinuityGeneration must serialize as integer"
    );
    assert_eq!(cg_json.as_u64().unwrap(), 3);

    // LeaseGrant.ttl serialized as integer ms via custom serde
    let grant = LeaseGrant {
        identity: id.clone(),
        fencing_token: ft,
        ttl: Duration::from_secs(5),
    };
    let grant_json = serde_json::to_value(&grant).unwrap();
    let ttl_val = &grant_json["ttl"];
    assert!(
        ttl_val.is_number(),
        "ttl should serialize as ms integer, got: {grant_json}"
    );
    assert_eq!(ttl_val.as_u64().unwrap(), 5000);

    // Round-trip: deserialize back
    let id_back: AgentIdentity = serde_json::from_value(id_json).unwrap();
    assert_eq!(id_back, id);

    let ft_back: FencingToken = serde_json::from_value(ft_json).unwrap();
    assert_eq!(ft_back, ft);

    let cv_back: CheckpointVersion = serde_json::from_value(cv_json).unwrap();
    assert_eq!(cv_back, cv);
}

// ===========================================================================
// CHOKE-09: Python/TS DurableAgentSpec → gateway → Rust DurableAgentSpec
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_09_durable_agent_spec_gateway_round_trip() {
    let spec = DurableAgentSpec {
        identity: make_identity("triage:main"),
        profile: meerkat_mob::ProfileName::from("expert"),
        addressability: AgentAddressability::Addressable,
        display_name: Some(
            meerkat_meta::identity_first::DisplayName::parse("Triage Lead").unwrap(),
        ),
        labels: {
            let mut m = BTreeMap::new();
            m.insert("env".to_string(), "prod".to_string());
            m.insert("team".to_string(), "alpha".to_string());
            m
        },
        context: Some(serde_json::json!({"app_key": "value123"})),
        additional_instructions: vec!["be thorough".to_string()],
        initial_message: None,
        runtime_mode_override: None,
        backend: None,
        binding: None,
        placement: None,
    };

    // Serialize to JSON (simulates gateway wire)
    let json = serde_json::to_value(&spec).unwrap();

    // Verify key fields survive
    assert_eq!(json["identity"].as_str().unwrap(), "triage:main");
    assert_eq!(json["labels"]["env"].as_str().unwrap(), "prod");

    // Deserialize back (simulates Rust receiving from gateway)
    let recovered: DurableAgentSpec = serde_json::from_value(json).unwrap();
    assert_eq!(recovered.identity, spec.identity);
    assert_eq!(recovered.addressability, spec.addressability);
    assert_eq!(recovered.labels, spec.labels);
    assert_eq!(recovered.context, spec.context);
    assert_eq!(
        recovered.additional_instructions,
        spec.additional_instructions
    );
}

// ===========================================================================
// CHOKE-10: Provider → gateway callback → Rust trait impl
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_10_provider_callback_round_trip() {
    let mock = MockBridge::new();

    // Simulate a provider returning mixed resolve states
    let id_uninit = "uninit:main";
    let id_ready = "ready:main";
    let id_broken = "broken:main";

    let ready_record = ContinuityRecord {
        identity: make_identity(id_ready),
        agent_runtime_id: AgentRuntimeId::parse("rt:ready:0").unwrap(),
        session_id: meerkat_core::types::SessionId::new(),
        generation: ContinuityGeneration::new(0),
        checkpoint_version: CheckpointVersion::new(3),
    };

    let response = json!({
        id_uninit: { "state": "uninitialized" },
        id_ready: { "state": "ready", "record": serde_json::to_value(&ready_record).unwrap() },
        id_broken: { "state": "broken", "failure": {
            "identity": id_broken,
            "kind": "snapshot_corrupted",
            "record": null,
            "detail": "data corruption detected"
        }}
    });

    mock.set_response("callback/continuity_store/resolve_many", Ok(response))
        .await;

    let store = GatewayContinuityStore::new(mock);
    let ids = vec![
        make_identity(id_uninit),
        make_identity(id_ready),
        make_identity(id_broken),
    ];
    let result = store.resolve_many(&ids).await.unwrap();

    assert_eq!(result.len(), 3);
    assert!(matches!(
        result.get(&make_identity(id_uninit)).unwrap(),
        ContinuityResolveState::Uninitialized
    ));
    match result.get(&make_identity(id_ready)).unwrap() {
        ContinuityResolveState::Ready { record } => {
            assert_eq!(record.identity, make_identity(id_ready));
            assert_eq!(record.checkpoint_version, CheckpointVersion::new(3));
        }
        other => panic!("expected Ready, got: {other:?}"),
    }
    match result.get(&make_identity(id_broken)).unwrap() {
        ContinuityResolveState::Broken { failure } => {
            assert_eq!(failure.kind, ContinuityFailureKind::SnapshotCorrupted);
            assert_eq!(failure.detail, "data corruption detected");
        }
        other => panic!("expected Broken, got: {other:?}"),
    }
}

// ===========================================================================
// CHOKE-11: dispatch → runtime substrate → ack semantics
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_11_dispatch_ack_semantics() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease = Arc::new(LocalLeaseProvider::new());

    // Without runtime_store: ack = in-memory only
    let runtime_no_store = IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: store.clone(),
        lease_provider: lease.clone(),
        runtime_instance_id: "no-store".to_string(),
        has_runtime_store: false,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: None,
        default_timeout: None,
    });
    runtime_no_store
        .register(
            make_spec("triage:main"),
            IdentityLifecycleState::Active,
            Some(make_record("triage:main", 0, 0)),
            Some(make_grant("triage:main", 1)),
        )
        .await;

    let (_, is_durable) = runtime_no_store
        .dispatch(&make_identity("triage:main"), &make_dispatch_input())
        .await
        .unwrap();
    assert!(
        !is_durable,
        "without runtime_store, dispatch ack must be in-memory"
    );

    // With runtime_store: ack = durable
    let runtime_with_store = IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: store.clone(),
        lease_provider: lease.clone(),
        runtime_instance_id: "with-store".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: None,
        default_timeout: None,
    });
    runtime_with_store
        .register(
            make_spec("worker:main"),
            IdentityLifecycleState::Active,
            Some(make_record("worker:main", 0, 0)),
            Some(make_grant("worker:main", 2)),
        )
        .await;

    let (_, is_durable) = runtime_with_store
        .dispatch(&make_identity("worker:main"), &make_dispatch_input())
        .await
        .unwrap();
    assert!(
        is_durable,
        "with runtime_store, dispatch ack must be durable"
    );
}

// ===========================================================================
// CHOKE-12: Active owner → fence/retire → destructive continuity mutation
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_12_fence_then_destroy_sequencing() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease_prov = Arc::new(LocalLeaseProvider::new());
    let runtime = make_runtime(store.clone(), lease_prov.clone());

    let id = make_identity("triage:main");

    // Acquire real lease
    let grants = lease_prov
        .acquire_leases(&[id.clone()], "choke-runtime")
        .await
        .unwrap();
    let initial_grant = match grants.get(&id).unwrap() {
        LeaseAcquireResult::Acquired(g) => g.clone(),
        _ => panic!("expected Acquired"),
    };

    let record = make_record("triage:main", 0, 0);
    store
        .upsert_continuity_record(&record, initial_grant.fencing_token)
        .await
        .unwrap();

    runtime
        .register(
            make_spec("triage:main"),
            IdentityLifecycleState::Active,
            Some(record.clone()),
            Some(initial_grant.clone()),
        )
        .await;

    // Reset fences the old owner first, then advances generation
    let new_record = runtime.reset(&id).await.unwrap();
    assert_eq!(new_record.generation, ContinuityGeneration::new(1));

    // Old owner's late write with original token is rejected
    let late_write = store
        .save_session_snapshot(
            &id,
            &record.session_id,
            record.generation,
            CheckpointVersion::new(1),
            initial_grant.fencing_token, // old token
            &SessionSnapshot {
                data: b"late".to_vec(),
            },
        )
        .await;
    assert!(
        late_write.is_err(),
        "late write from fenced owner must be rejected"
    );
}

// ===========================================================================
// CHOKE-13: Lease renewal → behavioral transition on loss
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_13_lease_loss_behavioral_transition() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease = Arc::new(LocalLeaseProvider::new());
    let runtime = make_runtime(store, lease);

    let id = make_identity("triage:main");
    runtime
        .register(
            make_spec("triage:main"),
            IdentityLifecycleState::Active,
            Some(make_record("triage:main", 0, 0)),
            Some(make_grant("triage:main", 1)),
        )
        .await;

    // Before loss: operations work
    assert!(runtime.send(&id, &make_content()).await.is_ok());
    assert!(runtime.dispatch(&id, &make_dispatch_input()).await.is_ok());

    // Mark lease lost (simulates renewal returning Lost)
    runtime.mark_lease_lost(&id).await.unwrap();

    // After loss: operations rejected immediately
    assert!(matches!(
        runtime.send(&id, &make_content()).await,
        Err(IdentityRuntimeError::InvalidState {
            state: IdentityLifecycleState::Broken,
            ..
        })
    ));
    assert!(matches!(
        runtime.dispatch(&id, &make_dispatch_input()).await,
        Err(IdentityRuntimeError::InvalidState {
            state: IdentityLifecycleState::Broken,
            ..
        })
    ));

    // Status reflects lost lease
    let status = runtime.status(&id).await.unwrap();
    assert!(status.lease.is_none(), "status should reflect lost lease");
    assert_eq!(status.state, IdentityLifecycleState::Broken);
}

// ===========================================================================
// CHOKE-14: IdentityStatus wire format across languages
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_14_identity_status_wire_format() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease = Arc::new(LocalLeaseProvider::new());
    let runtime = make_runtime(store, lease);

    let mut spec = make_spec("triage:main");
    spec.labels
        .insert("env".to_string(), "production".to_string());
    spec.labels.insert("tier".to_string(), "1".to_string());

    runtime
        .register(
            spec,
            IdentityLifecycleState::Active,
            Some(make_record("triage:main", 2, 10)),
            Some(make_grant("triage:main", 5)),
        )
        .await;

    let status = runtime.status(&make_identity("triage:main")).await.unwrap();

    // Serialize to JSON (simulates gateway wire)
    let json = serde_json::to_value(&status).unwrap();

    // Verify all fields survive serialization
    assert_eq!(json["identity"].as_str().unwrap(), "triage:main");
    assert_eq!(json["state"].as_str().unwrap(), "active");

    // Labels as string map
    let labels = json["labels"].as_object().unwrap();
    assert_eq!(labels["env"].as_str().unwrap(), "production");
    assert_eq!(labels["tier"].as_str().unwrap(), "1");

    // Generation and checkpoint as integers
    assert!(json["generation"].is_number());
    assert_eq!(json["generation"].as_u64().unwrap(), 2);

    // ContinuityHealth with structured DurabilityPolicy
    let health = &json["continuity_health"];
    assert!(health["store_reachable"].as_bool().unwrap());
    let policy = &health["durability_policy"];
    // SyncWriteThrough should serialize as a recognizable value
    assert!(!policy.is_null(), "durability_policy must be present");

    // Lease info with fencing token
    let lease_info = &json["lease"];
    assert!(!lease_info.is_null(), "lease info must be present");
    assert_eq!(lease_info["fencing_token"].as_u64().unwrap(), 5);
}

// ===========================================================================
// CHOKE-15: Destructive lifecycle ops over gateway bridge
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_15_destructive_ops_bridge() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease_prov = Arc::new(LocalLeaseProvider::new());
    let runtime = make_runtime(store.clone(), lease_prov.clone());

    let id = make_identity("triage:main");

    // Acquire lease and seed record
    let grants = lease_prov
        .acquire_leases(&[id.clone()], "choke-runtime")
        .await
        .unwrap();
    let grant = match grants.get(&id).unwrap() {
        LeaseAcquireResult::Acquired(g) => g.clone(),
        _ => panic!("expected Acquired"),
    };
    let record = make_record("triage:main", 0, 0);
    store
        .upsert_continuity_record(&record, grant.fencing_token)
        .await
        .unwrap();

    runtime
        .register(
            make_spec("triage:main"),
            IdentityLifecycleState::Active,
            Some(record),
            Some(grant),
        )
        .await;

    // Reset: verify result has advanced generation
    let new_record = runtime.reset(&id).await.unwrap();
    let reset_json = serde_json::to_value(&new_record).unwrap();
    assert_eq!(
        reset_json["generation"].as_u64().unwrap(),
        1,
        "reset must advance generation"
    );

    // Re-register for delete test
    let grants2 = lease_prov
        .acquire_leases(&[id.clone()], "choke-runtime")
        .await
        .unwrap();
    let grant2 = match grants2.get(&id).unwrap() {
        LeaseAcquireResult::Acquired(g) => g.clone(),
        _ => panic!("expected Acquired"),
    };
    store
        .upsert_continuity_record(&new_record, grant2.fencing_token)
        .await
        .unwrap();
    runtime
        .register(
            make_spec("triage:main"),
            IdentityLifecycleState::Active,
            Some(new_record),
            Some(grant2),
        )
        .await;

    // Delete: verify identity removed
    runtime.delete_identity(&id).await.unwrap();
    assert!(!runtime.contains(&id).await);
    let resolved = store.resolve_many(&[id.clone()]).await.unwrap();
    assert!(matches!(
        resolved.get(&id).unwrap(),
        ContinuityResolveState::Uninitialized
    ));
}

// ===========================================================================
// CHOKE-16: ContinuityStore → SessionStore adapter → Meerkat SessionService
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_16_continuity_session_store_adapter() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let adapter = ContinuitySessionStoreAdapter::new(store.clone());

    let sid = meerkat_core::types::SessionId::new();

    // Meerkat session restore reads flow through to ContinuityStore
    let loaded: Option<meerkat_core::Session> =
        meerkat::SessionStore::load(&adapter, &sid).await.unwrap();
    assert!(loaded.is_none(), "load from empty store should return None");

    // The adapter delegates to ContinuityStore, not local SQLite.
    // No dual-write: there's one authoritative truth (ContinuityStore).
}

// ===========================================================================
// CHOKE-17: TopologyProvider → AgentBuildContext → AgentCustomizer
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_17_topology_to_customizer_context() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease_prov = Arc::new(LocalLeaseProvider::new());
    let runtime = make_runtime(store.clone(), lease_prov.clone());

    let seen_contexts = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    struct Tracker {
        seen: Arc<tokio::sync::Mutex<Vec<AgentBuildContext>>>,
    }

    #[async_trait]
    impl AgentCustomizer for Tracker {
        async fn customize_build(
            &self,
            context: &AgentBuildContext,
            _spec: &DurableAgentSpec,
            _draft: &mut AgentBuildDraft,
        ) -> Result<(), CustomizerError> {
            self.seen.lock().await.push(context.clone());
            Ok(())
        }
    }

    struct FullMeshTopology;

    #[async_trait]
    impl TopologyProvider for FullMeshTopology {
        async fn compute_edges(
            &self,
            target: &[AgentIdentity],
            _ctx: &TopologyContext,
        ) -> Result<Vec<ManagedPeerEdge>, TopologyError> {
            let mut edges = Vec::new();
            for i in 0..target.len() {
                for j in (i + 1)..target.len() {
                    if let Ok(e) = ManagedPeerEdge::new(target[i].clone(), target[j].clone()) {
                        edges.push(e);
                    }
                }
            }
            Ok(edges)
        }
    }

    let customizer = Tracker {
        seen: seen_contexts.clone(),
    };
    let roster = vec![
        make_spec("a:main"),
        make_spec("b:main"),
        make_spec("c:main"),
    ];
    restore_flow(
        &runtime,
        &roster,
        Some(&FullMeshTopology),
        Some(&customizer),
    )
    .await
    .unwrap();

    let contexts = seen_contexts.lock().await;
    assert_eq!(contexts.len(), 3, "customizer called once per identity");

    for ctx in contexts.iter() {
        // On cold boot, active_peers includes ALL identities being activated
        assert_eq!(ctx.active_peers.len(), 3);
        // managed_edges from topology: full mesh of 3 → 3 edges
        assert_eq!(ctx.managed_edges.len(), 3);
        // Context populated BEFORE customize_build (sequencing verified by the fact
        // that customize_build sees the data)
    }
}

// ===========================================================================
// CHOKE-18: Reconciliation hot-update → runtime registry → behavioral change
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_18_hot_update_registry_behavioral_change() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease = Arc::new(LocalLeaseProvider::new());
    let runtime = make_runtime(store, lease);

    // Register as InternalOnly
    runtime
        .register(
            make_internal_spec("agent:main"),
            IdentityLifecycleState::Active,
            Some(make_record("agent:main", 0, 0)),
            Some(make_grant("agent:main", 1)),
        )
        .await;

    let id = make_identity("agent:main");

    // send() rejected (InternalOnly)
    assert!(matches!(
        runtime.send(&id, &make_content()).await,
        Err(IdentityRuntimeError::NotAddressable(_))
    ));

    // Hot-update: flip addressability to Addressable
    let new_spec = make_spec("agent:main"); // Addressable
    runtime.update_spec(new_spec).await.unwrap();

    // send() now works immediately, no restart needed
    assert!(runtime.send(&id, &make_content()).await.is_ok());

    // Hot-update: labels change visible in status()
    let mut labeled_spec = make_spec("agent:main");
    labeled_spec
        .labels
        .insert("team".to_string(), "alpha".to_string());
    runtime.update_spec(labeled_spec).await.unwrap();

    let status = runtime.status(&id).await.unwrap();
    assert_eq!(status.labels.get("team"), Some(&"alpha".to_string()));
}

// ===========================================================================
// CHOKE-19: Roster identity uniqueness validation
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_19_roster_uniqueness_validation() {
    let store = Arc::new(LocalContinuityStore::in_memory().unwrap());
    let lease_prov = Arc::new(LocalLeaseProvider::new());
    let runtime = make_runtime(store.clone(), lease_prov.clone());

    // Duplicate identities
    let roster = vec![make_spec("dup:main"), make_spec("dup:main")];
    let result = restore_flow(&runtime, &roster, None, None).await;

    // Must fail with structured error
    match result {
        Err(IdentityRuntimeError::DuplicateIdentity(id)) => {
            assert_eq!(
                id,
                make_identity("dup:main"),
                "error must name the duplicated identity"
            );
        }
        Err(other) => panic!("expected DuplicateIdentity, got: {other}"),
        Ok(_) => panic!("expected error for duplicate identities"),
    }

    // No partial activation
    assert!(!runtime.contains(&make_identity("dup:main")).await);

    // No leases acquired, no sessions created, no continuity records written
    let resolved = store
        .resolve_many(&[make_identity("dup:main")])
        .await
        .unwrap();
    assert!(matches!(
        resolved.get(&make_identity("dup:main")).unwrap(),
        ContinuityResolveState::Uninitialized
    ));
}

// ===========================================================================
// CHOKE-20: SessionHook → AgentCustomizer adapter — unsupported mutation detection
// ===========================================================================

#[tokio::test]
async fn identity_first_choke_20_session_hook_adapter_unsupported_mutation() {
    use meerkat_meta::mob_handle_runtime::SessionHook;

    struct MutatesUnsupported;

    #[async_trait]
    impl SessionHook for MutatesUnsupported {
        async fn before_create(
            &self,
            req: &mut meerkat_core::service::CreateSessionRequest,
        ) -> Result<(), meerkat_core::service::SessionError> {
            // Supported mutations
            req.model = "claude-opus-4-6".to_string();
            // meerkat 0.7: CreateSessionRequest.system_prompt is the typed
            // tri-state SystemPromptOverride.
            req.system_prompt =
                meerkat_core::config::SystemPromptOverride::Set("new prompt".to_string());
            // Unsupported mutation
            req.max_tokens = Some(8192);
            Ok(())
        }
    }

    let hook: Arc<dyn SessionHook> = Arc::new(MutatesUnsupported);
    let adapter = SessionHookCustomizerAdapter::new(hook);

    let ctx = AgentBuildContext {
        identity: make_identity("triage:main"),
        active_peers: vec![],
        managed_edges: vec![],
        runtime_services: Default::default(),
    };
    let spec = make_spec("triage:main");
    let mut draft = AgentBuildDraft {
        compaction_curator: Default::default(),
        model: None,
        system_prompt: None,
        additional_instructions: vec![],
        labels: BTreeMap::new(),
        app_context: None,
        external_tools: vec![],
        local_external_tools: Default::default(),
        provider_params: None,
    };

    // Should succeed (unsupported mutations are warned, not errored)
    adapter
        .customize_build(&ctx, &spec, &mut draft)
        .await
        .unwrap();

    // Supported mutations ARE applied
    assert_eq!(draft.model.as_deref(), Some("claude-opus-4-6"));
    assert_eq!(draft.system_prompt.as_deref(), Some("new prompt"));

    // Unsupported mutations are NOT applied to draft
    // (draft has no max_tokens field — the adapter discards it after warning)
}
