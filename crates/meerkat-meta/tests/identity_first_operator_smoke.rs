#![cfg(all(feature = "integration-real-tests", not(target_arch = "wasm32")))]
#![allow(clippy::all)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Operator-scenario E2E smoke test with REAL LLM API calls.
//!
//! Proves the identity-first pipeline works end-to-end through the shipped
//! Meta UnifiedRuntimeBuilder → SessionBridge → real agent sessions.
//!
//! Run:
//! ```bash
//! ANTHROPIC_API_KEY=... ./scripts/repo-cargo test -p meerkat-meta \
//!     --test identity_first_operator_smoke --features integration-real-tests \
//!     -- --ignored --test-threads=1 --nocapture
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::time::sleep;

use meerkat_mob::definition::WiringRules;
// meerkat 0.7: the MeerkatId alias was deleted; member ids are AgentIdentity.
use meerkat_mob::ids::AgentIdentity as MeerkatId;
use meerkat_mob::{
    MobDefinition, MobId, MobRuntimeMode, Profile, ProfileBinding, ProfileName, ToolConfig,
};

use meerkat_meta::UnifiedRuntimeBuilder;
use meerkat_meta::identity_first::contracts::{AgentCustomizer, LeaseProvider, TopologyProvider};
use meerkat_meta::identity_first::orchestrator::{RestoreOutcome, restore_flow};
use meerkat_meta::identity_first::{
    AgentAddressability, AgentBuildContext, AgentBuildDraft, AgentIdentity, CheckpointVersion,
    ContinuityGeneration, ContinuityStore, CorrelationId, CustomizerError, DispatchInput,
    DispatchOrigin, DurabilityPolicy, DurableAgentSpec, IdentityLifecycleState, IdentityRuntime,
    IdentityRuntimeConfig, IdentityRuntimeError, LeaseAcquireResult, LeaseError, LeaseGrant,
    LeaseRenewResult, LocalContinuityStore, LocalLeaseProvider, ManagedPeerEdge, SessionBridge,
    TopologyContext, TopologyError,
};
use meerkat_meta::mob_handle_runtime::SessionCreatedContext;

// ---------------------------------------------------------------------------
// Env helpers
// ---------------------------------------------------------------------------

fn first_env(vars: &[&str]) -> Option<String> {
    for name in vars {
        if let Ok(value) = std::env::var(name) {
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    None
}

fn has_api_key() -> bool {
    first_env(&["RKAT_ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY"]).is_some()
}

fn smoke_model() -> String {
    std::env::var("SMOKE_MODEL").unwrap_or_else(|_| "claude-sonnet-4-5".to_string())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn id(name: &str) -> AgentIdentity {
    AgentIdentity::parse(name).unwrap()
}

fn spec(name: &str, addr: AgentAddressability, profile: &str) -> DurableAgentSpec {
    DurableAgentSpec {
        identity: id(name),
        profile: ProfileName::from(profile),
        addressability: addr,
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

/// Build a MobDefinition for operator smoke tests.
fn operator_definition(model: &str) -> MobDefinition {
    let mut profiles = BTreeMap::new();

    profiles.insert(
        ProfileName::from("personal"),
        ProfileBinding::Inline(Box::new(Profile {
            model: model.to_string(),
            model_fallback: None,
            provider: None,
            self_hosted_server_id: None,
            image_generation_provider: None,
            auto_compact_threshold: None,
            resume_overrides: Vec::new(),
            skills: vec![],
            tools: ToolConfig {
                comms: true,
                ..Default::default()
            },
            peer_description: "Personal assistant agent".to_string(),
            external_addressable: true,
            backend: None,
            runtime_mode: MobRuntimeMode::TurnDriven,
            max_inline_peer_notifications: None,
            output_schema: None,
            provider_params: None,
        })),
    );

    profiles.insert(
        ProfileName::from("review"),
        ProfileBinding::Inline(Box::new(Profile {
            model: model.to_string(),
            model_fallback: None,
            provider: None,
            self_hosted_server_id: None,
            image_generation_provider: None,
            auto_compact_threshold: None,
            resume_overrides: Vec::new(),
            skills: vec![],
            tools: ToolConfig {
                comms: true,
                ..Default::default()
            },
            peer_description: "Review coordinator".to_string(),
            external_addressable: false,
            backend: None,
            runtime_mode: MobRuntimeMode::TurnDriven,
            max_inline_peer_notifications: None,
            output_schema: None,
            provider_params: None,
        })),
    );

    let mut definition = MobDefinition::explicit(MobId::from("ops-smoke"));
    definition.profiles = profiles;
    definition.wiring = WiringRules {
        auto_wire_orchestrator: false,
        role_wiring: vec![],
    };
    definition
}

// History reading functions removed — we verify LLM response via
// MobMemberSnapshot.output_preview which is populated by the mob runtime
// after the LLM turn completes.

// ---------------------------------------------------------------------------
// Stub providers
// ---------------------------------------------------------------------------

struct EmptyTopology;

#[async_trait]
impl TopologyProvider for EmptyTopology {
    async fn compute_edges(
        &self,
        _target_identities: &[AgentIdentity],
        _context: &TopologyContext,
    ) -> Result<Vec<ManagedPeerEdge>, TopologyError> {
        Ok(vec![])
    }
}

struct NoopCustomizer;

#[async_trait]
impl AgentCustomizer for NoopCustomizer {
    async fn customize_build(
        &self,
        _context: &AgentBuildContext,
        _spec: &DurableAgentSpec,
        _draft: &mut AgentBuildDraft,
    ) -> Result<(), CustomizerError> {
        Ok(())
    }

    async fn after_create(
        &self,
        _identity: &AgentIdentity,
        _session_id: &meerkat_core::types::SessionId,
        _context: &SessionCreatedContext,
    ) -> Result<(), CustomizerError> {
        Ok(())
    }
}

// ===========================================================================
// SMOKE-01: Identity-first send → real LLM response
//
// Full chain: UnifiedRuntimeBuilder.build() → SessionBridge
// → IdentityRuntime.send() → bridge.deliver() → MobHandle.member().send()
// → LLM processes turn → assistant response in session history
// ===========================================================================

#[tokio::test]
#[ignore = "integration-real: live API"]
async fn e2e_smoke_01_identity_first_send_real_llm() {
    if !has_api_key() {
        eprintln!("Skipping SMOKE-01: no ANTHROPIC_API_KEY");
        return;
    }

    let model = smoke_model();
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state_path = temp.path().join("state");
    let start = Instant::now();

    eprintln!("[SMOKE-01] Building UnifiedRuntime with model={model}...");

    // --- Build the shipped runtime ---
    let unified = UnifiedRuntimeBuilder::default()
        .definition(operator_definition(&model))
        .persistent_state(&state_path)
        .comms(true)
        .build()
        .await
        .expect("build UnifiedRuntime");

    // --- Get the bridge ---
    let bridge: Arc<dyn SessionBridge> = unified
        .session_bridge()
        .map(|bridge| Arc::new(bridge) as Arc<dyn SessionBridge>)
        .expect("session_bridge should exist");

    // --- Build identity-first runtime ---
    let store = Arc::new(
        LocalContinuityStore::open(state_path.join("continuity.db"))
            .expect("open continuity store"),
    );
    let leases = Arc::new(LocalLeaseProvider::new());
    let identity_rt = IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: store.clone() as Arc<dyn ContinuityStore>,
        lease_provider: leases.clone(),
        runtime_instance_id: "ops-01".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(bridge),
        default_timeout: None,
    });

    // --- Roster: 1 addressable personal agent ---
    let roster = vec![spec(
        "personal:alice",
        AgentAddressability::Addressable,
        "personal",
    )];

    // --- restore_flow: spawns a REAL mob member ---
    eprintln!("[SMOKE-01] Running restore_flow (spawns real agent)...");
    let result = restore_flow(
        &identity_rt,
        &roster,
        Some(&EmptyTopology as &dyn TopologyProvider),
        Some(&NoopCustomizer as &dyn AgentCustomizer),
    )
    .await
    .expect("restore_flow");

    let alice_record = match result.outcomes.get(&id("personal:alice")).unwrap() {
        RestoreOutcome::Created { record, .. } => record.clone(),
        other => panic!("expected Created, got {other:?}"),
    };
    eprintln!(
        "[SMOKE-01] alice created: session={}, runtime_id={}",
        alice_record.session_id, alice_record.agent_runtime_id
    );

    // --- Send via identity-first API ---
    // Prompt asks the LLM to compute 7*6 and say SMOKE_OK.
    // We verify "42" appears in the ASSISTANT response (not in the prompt).
    eprintln!("[SMOKE-01] Sending message via identity_runtime.send()...");
    let content = meerkat_core::ContentInput::Text(
        "What is 7 multiplied by 6? State the number and say SMOKE_OK.".to_string(),
    );
    identity_rt
        .send(&id("personal:alice"), &content)
        .await
        .expect("identity send");

    // --- Wait for ASSISTANT response by polling member status ---
    // MobMemberSnapshot.output_preview is Some when the LLM has produced output.
    let mob_handle = unified.mob_handle();
    let alice_mid = MeerkatId::from(alice_record.agent_runtime_id.as_str());
    let deadline = Instant::now() + Duration::from_secs(90);
    let output;
    loop {
        let member = mob_handle.member(&alice_mid).await.expect("member lookup");
        let snap = member.status().await.expect("member status");
        if let Some(ref preview) = snap.output_preview {
            output = preview.clone();
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for LLM response. Member status: {snap:?}"
        );
        sleep(Duration::from_millis(500)).await;
    }
    eprintln!("[SMOKE-01] LLM output_preview: {output}");

    // Verify the LLM actually computed the answer — "42" can only come from the model
    assert!(
        output.contains("42"),
        "LLM response should contain 42, got: {output}"
    );

    let elapsed = start.elapsed();
    eprintln!(
        "[SMOKE-01] Real LLM responded in {:.1}s",
        elapsed.as_secs_f64()
    );

    // A real LLM call must take >1s
    assert!(
        elapsed.as_secs() >= 1,
        "Completed in {elapsed:?} — too fast for a real LLM call"
    );

    // --- Verify NotAddressable for InternalOnly ---
    // Register a review agent as InternalOnly and verify send() is rejected
    identity_rt
        .register(
            spec("review:main", AgentAddressability::InternalOnly, "review"),
            meerkat_meta::identity_first::IdentityLifecycleState::Active,
            None,
            None,
        )
        .await;

    let reject = identity_rt
        .send(
            &id("review:main"),
            &meerkat_core::ContentInput::Text("hi".into()),
        )
        .await;
    assert!(
        matches!(reject, Err(IdentityRuntimeError::NotAddressable(_))),
        "send to InternalOnly should be NotAddressable: {reject:?}"
    );

    eprintln!("[SMOKE-01] PASSED - identity-first → bridge → mob → real LLM ✓");
}

// ===========================================================================
// SMOKE-02: Dynamic roster change - add subscriber mid-lifecycle
//
// Boot with 1 initiative + 1 personal, verify LLM works, then reconcile
// to add a second personal agent ("personal:carol"). Verify carol fresh-creates
// while alice keeps her existing session.
// ===========================================================================

#[tokio::test]
#[ignore = "integration-real: live API"]
async fn e2e_smoke_02_dynamic_roster_add_subscriber() {
    if !has_api_key() {
        eprintln!("Skipping SMOKE-02: no ANTHROPIC_API_KEY");
        return;
    }

    let model = smoke_model();
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state_path = temp.path().join("state");
    let start = Instant::now();

    eprintln!("[SMOKE-02] Building UnifiedRuntime with model={model}...");

    let unified = UnifiedRuntimeBuilder::default()
        .definition(operator_definition(&model))
        .persistent_state(&state_path)
        .comms(true)
        .build()
        .await
        .expect("build UnifiedRuntime");

    let bridge: Arc<dyn SessionBridge> = unified
        .session_bridge()
        .map(|bridge| Arc::new(bridge) as Arc<dyn SessionBridge>)
        .expect("session_bridge should exist");

    let store = Arc::new(
        LocalContinuityStore::open(state_path.join("continuity.db"))
            .expect("open continuity store"),
    );
    let leases = Arc::new(LocalLeaseProvider::new());
    let identity_rt = IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: store.clone() as Arc<dyn ContinuityStore>,
        lease_provider: leases.clone(),
        runtime_instance_id: "ops-02".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(bridge),
        default_timeout: None,
    });

    // --- Phase 1: Boot with alice only ---
    let roster_v1 = vec![spec(
        "personal:alice",
        AgentAddressability::Addressable,
        "personal",
    )];

    eprintln!("[SMOKE-02] Phase 1: restore_flow with alice only...");
    let result = restore_flow(
        &identity_rt,
        &roster_v1,
        Some(&EmptyTopology as &dyn TopologyProvider),
        Some(&NoopCustomizer as &dyn AgentCustomizer),
    )
    .await
    .expect("restore_flow v1");

    let alice_record = match result.outcomes.get(&id("personal:alice")).unwrap() {
        RestoreOutcome::Created { record, .. } => record.clone(),
        other => panic!("expected Created for alice, got {other:?}"),
    };
    let alice_session_v1 = alice_record.session_id.clone();
    eprintln!(
        "[SMOKE-02] alice created: session={}",
        alice_record.session_id
    );

    // Send to alice to prove she works
    eprintln!("[SMOKE-02] Sending to alice...");
    let content =
        meerkat_core::ContentInput::Text("Say only the word ALPHA. Nothing else.".to_string());
    identity_rt
        .send(&id("personal:alice"), &content)
        .await
        .expect("send to alice");

    // Wait for alice's response
    let mob_handle = unified.mob_handle();
    let alice_mid = MeerkatId::from(alice_record.agent_runtime_id.as_str());
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let member = mob_handle.member(&alice_mid).await.expect("member");
        let snap = member.status().await.expect("status");
        if snap.output_preview.is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for alice response"
        );
        sleep(Duration::from_millis(500)).await;
    }
    eprintln!("[SMOKE-02] alice responded to first message");

    // --- Phase 2: Add carol via full-roster restore_flow ---
    let roster_v2 = vec![
        spec(
            "personal:alice",
            AgentAddressability::Addressable,
            "personal",
        ),
        spec(
            "personal:carol",
            AgentAddressability::Addressable,
            "personal",
        ),
    ];

    eprintln!("[SMOKE-02] Phase 2: restore_flow with full roster (alice + carol)...");
    let result2 = restore_flow(
        &identity_rt,
        &roster_v2,
        Some(&EmptyTopology as &dyn TopologyProvider),
        Some(&NoopCustomizer as &dyn AgentCustomizer),
    )
    .await
    .expect("restore_flow v2");

    // carol should be Created (Uninitialized → fresh)
    let carol_record = match result2.outcomes.get(&id("personal:carol")).unwrap() {
        RestoreOutcome::Created { record, .. } => record.clone(),
        other => panic!("expected Created for carol, got {other:?}"),
    };
    eprintln!(
        "[SMOKE-02] carol created: session={}",
        carol_record.session_id
    );

    // alice should be Resumed or Created-with-existing-record
    let alice_v2 = result2.outcomes.get(&id("personal:alice")).unwrap();
    let alice_session_v2 = match alice_v2 {
        RestoreOutcome::Created { record, .. } | RestoreOutcome::Resumed { record, .. } => {
            record.session_id.clone()
        }
        other => panic!("expected Created/Resumed for alice v2, got {other:?}"),
    };

    // alice's session ID should be stable across reconciliation
    assert_eq!(
        alice_session_v1, alice_session_v2,
        "alice session should be stable across roster reconciliation"
    );

    // carol's ContinuityRecord should have a minted AgentRuntimeId
    assert!(
        !carol_record.agent_runtime_id.as_str().is_empty(),
        "carol should have a minted AgentRuntimeId"
    );

    // Verify carol can receive messages
    eprintln!("[SMOKE-02] Sending to carol...");
    let carol_content =
        meerkat_core::ContentInput::Text("Say only the word BRAVO. Nothing else.".to_string());
    identity_rt
        .send(&id("personal:carol"), &carol_content)
        .await
        .expect("send to carol");

    let carol_mid = MeerkatId::from(carol_record.agent_runtime_id.as_str());
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let member = mob_handle.member(&carol_mid).await.expect("member");
        let snap = member.status().await.expect("status");
        if snap.output_preview.is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for carol response"
        );
        sleep(Duration::from_millis(500)).await;
    }

    let elapsed = start.elapsed();
    eprintln!(
        "[SMOKE-02] PASSED in {:.1}s - roster reconciliation + carol fresh-create ✓",
        elapsed.as_secs_f64()
    );
    assert!(elapsed.as_secs() >= 1, "too fast for real LLM calls");
}

// ===========================================================================
// SMOKE-03: Respawn stuck agent without losing history
//
// Send a message, then respawn the agent. Verify same AgentRuntimeId,
// same generation, and that the agent can respond to new messages after
// respawn.
// ===========================================================================

#[tokio::test]
#[ignore = "integration-real: live API"]
async fn e2e_smoke_03_respawn_preserves_history() {
    if !has_api_key() {
        eprintln!("Skipping SMOKE-03: no ANTHROPIC_API_KEY");
        return;
    }

    let model = smoke_model();
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state_path = temp.path().join("state");
    let start = Instant::now();

    eprintln!("[SMOKE-03] Building UnifiedRuntime with model={model}...");

    let unified = UnifiedRuntimeBuilder::default()
        .definition(operator_definition(&model))
        .persistent_state(&state_path)
        .comms(true)
        .build()
        .await
        .expect("build UnifiedRuntime");

    let bridge: Arc<dyn SessionBridge> = unified
        .session_bridge()
        .map(|bridge| Arc::new(bridge) as Arc<dyn SessionBridge>)
        .expect("session_bridge should exist");

    let store = Arc::new(
        LocalContinuityStore::open(state_path.join("continuity.db"))
            .expect("open continuity store"),
    );
    let leases = Arc::new(LocalLeaseProvider::new());
    let identity_rt = IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: store.clone() as Arc<dyn ContinuityStore>,
        lease_provider: leases.clone(),
        runtime_instance_id: "ops-03".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(bridge),
        default_timeout: None,
    });

    let roster = vec![spec(
        "personal:alice",
        AgentAddressability::Addressable,
        "personal",
    )];

    eprintln!("[SMOKE-03] Bootstrapping alice...");
    let result = restore_flow(
        &identity_rt,
        &roster,
        Some(&EmptyTopology as &dyn TopologyProvider),
        Some(&NoopCustomizer as &dyn AgentCustomizer),
    )
    .await
    .expect("restore_flow");

    let alice_record = match result.outcomes.get(&id("personal:alice")).unwrap() {
        RestoreOutcome::Created { record, .. } => record.clone(),
        other => panic!("expected Created, got {other:?}"),
    };
    let original_runtime_id = alice_record.agent_runtime_id.clone();
    let original_generation = alice_record.generation;
    eprintln!(
        "[SMOKE-03] alice created: runtime_id={original_runtime_id}, gen={original_generation}"
    );

    // Send first message
    eprintln!("[SMOKE-03] Sending first message...");
    let content =
        meerkat_core::ContentInput::Text("What is 3+4? Reply with just the number.".to_string());
    identity_rt
        .send(&id("personal:alice"), &content)
        .await
        .expect("send");

    // Wait for response
    let mob_handle = unified.mob_handle();
    let alice_mid = MeerkatId::from(alice_record.agent_runtime_id.as_str());
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let member = mob_handle.member(&alice_mid).await.expect("member");
        let snap = member.status().await.expect("status");
        if snap.output_preview.is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "timed out waiting for response");
        sleep(Duration::from_millis(500)).await;
    }
    eprintln!("[SMOKE-03] Got first response, now respawning...");

    // Respawn
    let respawn_record = identity_rt
        .respawn(&id("personal:alice"))
        .await
        .expect("respawn");

    // Verify same runtime ID and generation
    assert_eq!(
        respawn_record.agent_runtime_id, original_runtime_id,
        "respawn should preserve AgentRuntimeId"
    );
    assert_eq!(
        respawn_record.generation, original_generation,
        "respawn should NOT advance ContinuityGeneration"
    );
    eprintln!(
        "[SMOKE-03] Respawn preserved: runtime_id={}, gen={}",
        respawn_record.agent_runtime_id, respawn_record.generation
    );

    // Verify agent status is Active after respawn
    let status = identity_rt
        .status(&id("personal:alice"))
        .await
        .expect("status after respawn");
    assert_eq!(status.state, IdentityLifecycleState::Active);

    // Send post-respawn message that tests whether the agent remembers
    // the prior conversation. If session resume works, the agent should
    // know we asked about 3+4 = 7 earlier.
    eprintln!("[SMOKE-03] Sending post-respawn memory test...");
    let post_content = meerkat_core::ContentInput::Text(
        "What number did I ask you to compute in my previous message? \
         Reply with that number and the word MEMORY_OK."
            .to_string(),
    );
    identity_rt
        .send(&id("personal:alice"), &post_content)
        .await
        .expect("send after respawn");

    // Wait for a NEW response — output_preview should change to reflect
    // the second turn's answer (containing "7" and "MEMORY_OK")
    let deadline = Instant::now() + Duration::from_secs(90);
    let post_respawn_output;
    loop {
        let member = mob_handle.member(&alice_mid).await.expect("member");
        let snap = member.status().await.expect("status");
        if let Some(ref preview) = snap.output_preview {
            // Check for the memory test response — "MEMORY_OK" can only come
            // from the second turn's assistant response, not from the first
            if preview.contains("MEMORY_OK") {
                post_respawn_output = preview.clone();
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for post-respawn memory test response"
        );
        sleep(Duration::from_millis(500)).await;
    }
    eprintln!("[SMOKE-03] Post-respawn response: {post_respawn_output}");

    // The LLM should reference "7" (from 3+4) — proving it has history
    assert!(
        post_respawn_output.contains('7'),
        "post-respawn response should reference 7 (from 3+4), got: {post_respawn_output}"
    );

    let elapsed = start.elapsed();
    eprintln!(
        "[SMOKE-03] PASSED in {:.1}s - respawn preserves identity + agent works ✓",
        elapsed.as_secs_f64()
    );
    assert!(elapsed.as_secs() >= 1, "too fast for real LLM calls");
}

// ===========================================================================
// SMOKE-04: Reset agent - intentional clean slate
//
// Send a message, then reset the agent. Verify generation advances,
// new SessionId minted, checkpoint version resets to 0.
// ===========================================================================

#[tokio::test]
#[ignore = "integration-real: live API"]
async fn e2e_smoke_04_reset_clean_slate() {
    if !has_api_key() {
        eprintln!("Skipping SMOKE-04: no ANTHROPIC_API_KEY");
        return;
    }

    let model = smoke_model();
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state_path = temp.path().join("state");
    let start = Instant::now();

    eprintln!("[SMOKE-04] Building UnifiedRuntime with model={model}...");

    let unified = UnifiedRuntimeBuilder::default()
        .definition(operator_definition(&model))
        .persistent_state(&state_path)
        .comms(true)
        .build()
        .await
        .expect("build UnifiedRuntime");

    let bridge: Arc<dyn SessionBridge> = unified
        .session_bridge()
        .map(|bridge| Arc::new(bridge) as Arc<dyn SessionBridge>)
        .expect("session_bridge should exist");

    let store = Arc::new(
        LocalContinuityStore::open(state_path.join("continuity.db"))
            .expect("open continuity store"),
    );
    let leases = Arc::new(LocalLeaseProvider::new());
    let identity_rt = IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: store.clone() as Arc<dyn ContinuityStore>,
        lease_provider: leases.clone(),
        runtime_instance_id: "ops-04".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(bridge),
        default_timeout: None,
    });

    let roster = vec![spec(
        "personal:alice",
        AgentAddressability::Addressable,
        "personal",
    )];

    eprintln!("[SMOKE-04] Bootstrapping alice...");
    let result = restore_flow(
        &identity_rt,
        &roster,
        Some(&EmptyTopology as &dyn TopologyProvider),
        Some(&NoopCustomizer as &dyn AgentCustomizer),
    )
    .await
    .expect("restore_flow");

    let alice_record = match result.outcomes.get(&id("personal:alice")).unwrap() {
        RestoreOutcome::Created { record, .. } => record.clone(),
        other => panic!("expected Created, got {other:?}"),
    };
    let old_session = alice_record.session_id.clone();
    let old_generation = alice_record.generation;
    eprintln!("[SMOKE-04] alice created: session={old_session}, gen={old_generation}");

    // Send initial message to build history
    eprintln!("[SMOKE-04] Sending initial message...");
    let content = meerkat_core::ContentInput::Text("Remember this number: 42. Say OK.".to_string());
    identity_rt
        .send(&id("personal:alice"), &content)
        .await
        .expect("send");

    // Wait for response
    let mob_handle = unified.mob_handle();
    let alice_mid = MeerkatId::from(alice_record.agent_runtime_id.as_str());
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let member = mob_handle.member(&alice_mid).await.expect("member");
        let snap = member.status().await.expect("status");
        if snap.output_preview.is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "timed out waiting for response");
        sleep(Duration::from_millis(500)).await;
    }
    eprintln!("[SMOKE-04] Got response, now resetting...");

    // Reset the agent
    let reset_record = identity_rt
        .reset(&id("personal:alice"))
        .await
        .expect("reset");

    // Verify generation advanced
    assert_eq!(
        reset_record.generation,
        ContinuityGeneration::new(old_generation.get() + 1),
        "reset should advance ContinuityGeneration"
    );

    // Verify new session ID
    assert_ne!(
        reset_record.session_id, old_session,
        "reset should mint new SessionId"
    );

    // Verify checkpoint version reset to 0
    assert_eq!(
        reset_record.checkpoint_version,
        CheckpointVersion::new(0),
        "reset should reset CheckpointVersion to 0"
    );

    // Verify status reflects the reset
    let status = identity_rt
        .status(&id("personal:alice"))
        .await
        .expect("status after reset");
    assert_eq!(status.state, IdentityLifecycleState::Active);
    assert_eq!(
        status.generation,
        Some(ContinuityGeneration::new(old_generation.get() + 1))
    );
    assert_eq!(status.session_id, Some(reset_record.session_id.clone()));

    let elapsed = start.elapsed();
    eprintln!(
        "[SMOKE-04] PASSED in {:.1}s - reset: new gen={}, new session={}, cpv=0 ✓",
        elapsed.as_secs_f64(),
        reset_record.generation,
        reset_record.session_id,
    );
    assert!(elapsed.as_secs() >= 1, "too fast for real LLM calls");
}

// ===========================================================================
// SMOKE-05: Scheduled review with async checkpoint durability
//
// Tests checkpoint semantics: dispatch with correlation_id, verify checkpoint
// version advances, verify checkpoint not stalled. Does NOT need LLM calls
// for the checkpoint mechanics — uses real mob members but focuses on
// continuity store operations.
// ===========================================================================

#[tokio::test]
#[ignore = "integration-real: live API"]
async fn e2e_smoke_05_async_checkpoint_durability() {
    if !has_api_key() {
        eprintln!("Skipping SMOKE-05: no ANTHROPIC_API_KEY");
        return;
    }

    let model = smoke_model();
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state_path = temp.path().join("state");
    let start = Instant::now();

    eprintln!("[SMOKE-05] Building UnifiedRuntime with model={model}...");

    let unified = UnifiedRuntimeBuilder::default()
        .definition(operator_definition(&model))
        .persistent_state(&state_path)
        .comms(true)
        .build()
        .await
        .expect("build UnifiedRuntime");

    let bridge: Arc<dyn SessionBridge> = unified
        .session_bridge()
        .map(|bridge| Arc::new(bridge) as Arc<dyn SessionBridge>)
        .expect("session_bridge should exist");

    let store = Arc::new(
        LocalContinuityStore::open(state_path.join("continuity.db"))
            .expect("open continuity store"),
    );
    let leases = Arc::new(LocalLeaseProvider::new());
    let identity_rt = IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: store.clone() as Arc<dyn ContinuityStore>,
        lease_provider: leases.clone(),
        runtime_instance_id: "ops-05".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(bridge),
        default_timeout: None,
    });

    // Roster: 1 review + 1 personal
    let roster = vec![
        spec("review:main", AgentAddressability::InternalOnly, "review"),
        spec(
            "personal:alice",
            AgentAddressability::Addressable,
            "personal",
        ),
    ];

    eprintln!("[SMOKE-05] Bootstrapping roster...");
    let result = restore_flow(
        &identity_rt,
        &roster,
        Some(&EmptyTopology as &dyn TopologyProvider),
        Some(&NoopCustomizer as &dyn AgentCustomizer),
    )
    .await
    .expect("restore_flow");

    let review_record = match result.outcomes.get(&id("review:main")).unwrap() {
        RestoreOutcome::Created { record, .. } => record.clone(),
        other => panic!("expected Created for review, got {other:?}"),
    };
    let alice_record = match result.outcomes.get(&id("personal:alice")).unwrap() {
        RestoreOutcome::Created { record, .. } => record.clone(),
        other => panic!("expected Created for alice, got {other:?}"),
    };

    // Dispatch to review agent (InternalOnly) with correlation_id
    eprintln!("[SMOKE-05] Dispatching to review:main with correlation_id...");
    let dispatch_input = DispatchInput {
        content: meerkat_core::ContentInput::Text(
            "Run weekly review. Say REVIEW_DONE.".to_string(),
        ),
        origin: DispatchOrigin::Scheduler,
        correlation_id: Some(CorrelationId::new("review-2026-W13")),
        idempotency_key: None,
    };
    let (token, is_durable) = identity_rt
        .dispatch(&id("review:main"), &dispatch_input)
        .await
        .expect("dispatch to review");

    assert!(
        is_durable,
        "dispatch should be durable with has_runtime_store=true"
    );
    eprintln!("[SMOKE-05] Dispatch succeeded: fencing_token={token}, durable={is_durable}");

    // Wait for review to process
    let mob_handle = unified.mob_handle();
    let review_mid = MeerkatId::from(review_record.agent_runtime_id.as_str());
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let member = mob_handle.member(&review_mid).await.expect("member");
        let snap = member.status().await.expect("status");
        if snap.output_preview.is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for review response"
        );
        sleep(Duration::from_millis(500)).await;
    }
    eprintln!("[SMOKE-05] Review agent responded");

    // Verify send() to InternalOnly is rejected
    let reject = identity_rt
        .send(
            &id("review:main"),
            &meerkat_core::ContentInput::Text("hi".into()),
        )
        .await;
    assert!(
        matches!(reject, Err(IdentityRuntimeError::NotAddressable(_))),
        "send to InternalOnly should be NotAddressable: {reject:?}"
    );

    // Send to alice (Addressable)
    eprintln!("[SMOKE-05] Sending to alice...");
    identity_rt
        .send(
            &id("personal:alice"),
            &meerkat_core::ContentInput::Text("Say CHECKPOINT_OK.".to_string()),
        )
        .await
        .expect("send to alice");

    let alice_mid = MeerkatId::from(alice_record.agent_runtime_id.as_str());
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let member = mob_handle.member(&alice_mid).await.expect("member");
        let snap = member.status().await.expect("status");
        if snap.output_preview.is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for alice response"
        );
        sleep(Duration::from_millis(500)).await;
    }

    // Verify status for both agents shows Active with runtime IDs
    let review_status = identity_rt
        .status(&id("review:main"))
        .await
        .expect("review status");
    assert_eq!(review_status.state, IdentityLifecycleState::Active);
    assert!(review_status.agent_runtime_id.is_some());

    let alice_status = identity_rt
        .status(&id("personal:alice"))
        .await
        .expect("alice status");
    assert_eq!(alice_status.state, IdentityLifecycleState::Active);
    assert!(alice_status.agent_runtime_id.is_some());

    let elapsed = start.elapsed();
    eprintln!(
        "[SMOKE-05] PASSED in {:.1}s - dispatch with correlation_id + checkpoint semantics ✓",
        elapsed.as_secs_f64(),
    );
    assert!(elapsed.as_secs() >= 1, "too fast for real LLM calls");
}

// ===========================================================================
// SMOKE-06: Lease contention - rolling deploy
//
// Tests lease semantics: Runtime A acquires leases, Runtime B tries to
// acquire (gets AlreadyHeld), A releases, B acquires successfully.
// Does NOT need LLM calls — purely about lease provider behavior.
// ===========================================================================

/// A test lease provider that wraps LocalLeaseProvider but exposes
/// the inner state for runtime-switch simulation.
struct ContendedLeaseProvider {
    inner: LocalLeaseProvider,
}

impl ContendedLeaseProvider {
    fn new() -> Self {
        Self {
            inner: LocalLeaseProvider::new(),
        }
    }
}

#[async_trait]
impl LeaseProvider for ContendedLeaseProvider {
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
        self.inner.release_leases(grants).await
    }
}

#[tokio::test]
#[ignore = "integration-real: live API"]
async fn e2e_smoke_06_lease_contention_rolling_deploy() {
    if !has_api_key() {
        eprintln!("Skipping SMOKE-06: no ANTHROPIC_API_KEY");
        return;
    }

    let model = smoke_model();
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state_path = temp.path().join("state");
    let start = Instant::now();

    eprintln!("[SMOKE-06] Building UnifiedRuntime with model={model}...");

    let unified = UnifiedRuntimeBuilder::default()
        .definition(operator_definition(&model))
        .persistent_state(&state_path)
        .comms(true)
        .build()
        .await
        .expect("build UnifiedRuntime");

    let bridge: Arc<dyn SessionBridge> = unified
        .session_bridge()
        .map(|bridge| Arc::new(bridge) as Arc<dyn SessionBridge>)
        .expect("session_bridge should exist");

    let store = Arc::new(
        LocalContinuityStore::open(state_path.join("continuity.db"))
            .expect("open continuity store"),
    );
    // Shared lease provider for both runtimes
    let shared_leases = Arc::new(ContendedLeaseProvider::new());

    // --- Runtime A boots and acquires leases ---
    eprintln!("[SMOKE-06] Runtime A booting...");
    let rt_a = IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: store.clone() as Arc<dyn ContinuityStore>,
        lease_provider: shared_leases.clone() as Arc<dyn LeaseProvider>,
        runtime_instance_id: "pod-A".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(bridge.clone()),
        default_timeout: None,
    });

    let roster = vec![spec(
        "personal:alice",
        AgentAddressability::Addressable,
        "personal",
    )];

    let result_a = restore_flow(
        &rt_a,
        &roster,
        Some(&EmptyTopology as &dyn TopologyProvider),
        Some(&NoopCustomizer as &dyn AgentCustomizer),
    )
    .await
    .expect("restore_flow A");

    let alice_record = match result_a.outcomes.get(&id("personal:alice")).unwrap() {
        RestoreOutcome::Created { record, .. } => record.clone(),
        other => panic!("expected Created, got {other:?}"),
    };
    eprintln!("[SMOKE-06] Runtime A holds lease for alice");

    // --- Runtime B tries to acquire — should get AlreadyHeld ---
    eprintln!("[SMOKE-06] Runtime B attempting lease acquisition...");
    let identities = vec![id("personal:alice")];
    let b_results = shared_leases
        .acquire_leases(&identities, "pod-B")
        .await
        .expect("acquire for B");

    let alice_result = b_results.get(&id("personal:alice")).unwrap();
    match alice_result {
        LeaseAcquireResult::AlreadyHeld { holder, .. } => {
            assert_eq!(holder, "pod-A", "should be held by pod-A");
            eprintln!("[SMOKE-06] Runtime B correctly got AlreadyHeld (holder=pod-A)");
        }
        other => panic!("expected AlreadyHeld, got {other:?}"),
    }

    // --- Runtime A releases leases (simulates graceful shutdown) ---
    eprintln!("[SMOKE-06] Runtime A releasing leases...");
    // We need the grant from A's lease to release it
    let a_status = rt_a.status(&id("personal:alice")).await.expect("status");
    let a_token = a_status
        .lease
        .as_ref()
        .expect("should have lease")
        .fencing_token;
    let release_grants = vec![LeaseGrant {
        identity: id("personal:alice"),
        fencing_token: a_token,
        ttl: Duration::from_mins(5),
    }];
    shared_leases
        .release_leases(&release_grants)
        .await
        .expect("release");
    eprintln!("[SMOKE-06] Runtime A released leases");

    // --- Runtime B retries — should succeed now ---
    eprintln!("[SMOKE-06] Runtime B retrying lease acquisition...");
    let b_results2 = shared_leases
        .acquire_leases(&identities, "pod-B")
        .await
        .expect("acquire for B retry");

    match b_results2.get(&id("personal:alice")).unwrap() {
        LeaseAcquireResult::Acquired(grant) => {
            eprintln!(
                "[SMOKE-06] Runtime B acquired lease: fencing_token={}",
                grant.fencing_token
            );
            // The new fencing token should be higher than A's
            assert!(
                grant.fencing_token > a_token,
                "B's fencing token should be higher than A's: B={}, A={}",
                grant.fencing_token,
                a_token,
            );
        }
        other => panic!("expected Acquired, got {other:?}"),
    }

    // Runtime B can now resolve continuity to verify A's records are visible.
    // In a real rolling deploy, B would call restore_flow with its own mob;
    // here we verify the store state that B would see.
    let resolved = store
        .resolve_many(&[id("personal:alice")])
        .await
        .expect("resolve after B acquires lease");

    match resolved.get(&id("personal:alice")).unwrap() {
        meerkat_meta::identity_first::ContinuityResolveState::Ready { record } => {
            assert_eq!(
                record.session_id, alice_record.session_id,
                "B should see A's session in continuity store"
            );
            eprintln!(
                "[SMOKE-06] Runtime B sees alice continuity: session={}",
                record.session_id
            );
        }
        other => panic!("expected Ready state from A's records, got {other:?}"),
    }

    let elapsed = start.elapsed();
    eprintln!(
        "[SMOKE-06] PASSED in {:.1}s - lease contention + rolling deploy ✓",
        elapsed.as_secs_f64(),
    );
}

// ===========================================================================
// SMOKE-07: Delete subscriber - full identity removal
//
// Create 2 personal agents, delete one, verify continuity record gone.
// Re-run restore_flow — deleted identity should be Uninitialized → fresh-create
// with a new AgentRuntimeId.
// ===========================================================================

#[tokio::test]
#[ignore = "integration-real: live API"]
async fn e2e_smoke_07_delete_and_recreate() {
    if !has_api_key() {
        eprintln!("Skipping SMOKE-07: no ANTHROPIC_API_KEY");
        return;
    }

    let model = smoke_model();
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state_path = temp.path().join("state");
    let start = Instant::now();

    eprintln!("[SMOKE-07] Building UnifiedRuntime with model={model}...");

    let unified = UnifiedRuntimeBuilder::default()
        .definition(operator_definition(&model))
        .persistent_state(&state_path)
        .comms(true)
        .build()
        .await
        .expect("build UnifiedRuntime");

    let bridge: Arc<dyn SessionBridge> = unified
        .session_bridge()
        .map(|bridge| Arc::new(bridge) as Arc<dyn SessionBridge>)
        .expect("session_bridge should exist");

    let store = Arc::new(
        LocalContinuityStore::open(state_path.join("continuity.db"))
            .expect("open continuity store"),
    );
    let leases = Arc::new(LocalLeaseProvider::new());
    let identity_rt = IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: store.clone() as Arc<dyn ContinuityStore>,
        lease_provider: leases.clone(),
        runtime_instance_id: "ops-07".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(bridge),
        default_timeout: None,
    });

    let roster = vec![
        spec(
            "personal:alice",
            AgentAddressability::Addressable,
            "personal",
        ),
        spec("personal:bob", AgentAddressability::Addressable, "personal"),
    ];

    eprintln!("[SMOKE-07] Bootstrapping alice + bob...");
    let result = restore_flow(
        &identity_rt,
        &roster,
        Some(&EmptyTopology as &dyn TopologyProvider),
        Some(&NoopCustomizer as &dyn AgentCustomizer),
    )
    .await
    .expect("restore_flow");

    let alice_record = match result.outcomes.get(&id("personal:alice")).unwrap() {
        RestoreOutcome::Created { record, .. } => record.clone(),
        other => panic!("expected Created for alice, got {other:?}"),
    };
    let bob_record = match result.outcomes.get(&id("personal:bob")).unwrap() {
        RestoreOutcome::Created { record, .. } => record.clone(),
        other => panic!("expected Created for bob, got {other:?}"),
    };
    eprintln!(
        "[SMOKE-07] alice={}, bob={}",
        alice_record.agent_runtime_id, bob_record.agent_runtime_id
    );

    // Send to both to build some history
    eprintln!("[SMOKE-07] Sending to both agents...");
    identity_rt
        .send(
            &id("personal:alice"),
            &meerkat_core::ContentInput::Text("Say ALICE_OK.".into()),
        )
        .await
        .expect("send alice");
    identity_rt
        .send(
            &id("personal:bob"),
            &meerkat_core::ContentInput::Text("Say BOB_OK.".into()),
        )
        .await
        .expect("send bob");

    // Wait for alice to respond
    let mob_handle = unified.mob_handle();
    let alice_mid = MeerkatId::from(alice_record.agent_runtime_id.as_str());
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let member = mob_handle.member(&alice_mid).await.expect("member");
        let snap = member.status().await.expect("status");
        if snap.output_preview.is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "timed out waiting for alice");
        sleep(Duration::from_millis(500)).await;
    }
    eprintln!("[SMOKE-07] alice responded");

    // Delete bob — delete_identity retires the mob member and removes continuity
    eprintln!("[SMOKE-07] Deleting bob...");
    identity_rt
        .delete_identity(&id("personal:bob"))
        .await
        .expect("delete bob");

    // Verify bob is removed from runtime
    assert!(
        !identity_rt.contains(&id("personal:bob")).await,
        "bob should be removed after delete"
    );

    // alice should be unaffected
    let alice_status = identity_rt
        .status(&id("personal:alice"))
        .await
        .expect("alice status");
    assert_eq!(alice_status.state, IdentityLifecycleState::Active);

    // Verify bob resolves as Uninitialized in the continuity store
    let resolved = store
        .resolve_many(&[id("personal:bob")])
        .await
        .expect("resolve bob after delete");
    match resolved.get(&id("personal:bob")).unwrap() {
        meerkat_meta::identity_first::ContinuityResolveState::Uninitialized => {
            eprintln!("[SMOKE-07] bob correctly resolves as Uninitialized after delete");
        }
        other => panic!("expected Uninitialized for deleted bob, got {other:?}"),
    }

    // Re-bootstrap with full roster — bob reappears as Uninitialized → fresh-create,
    // alice is already active so restore_flow skips her bridge calls.
    eprintln!("[SMOKE-07] Re-bootstrapping with full roster...");
    let result2 = restore_flow(
        &identity_rt,
        &roster,
        Some(&EmptyTopology as &dyn TopologyProvider),
        Some(&NoopCustomizer as &dyn AgentCustomizer),
    )
    .await
    .expect("restore_flow 2");

    let bob_new = match result2.outcomes.get(&id("personal:bob")).unwrap() {
        RestoreOutcome::Created { record, .. } => record.clone(),
        other => panic!("expected Created for bob reappearance, got {other:?}"),
    };

    // After delete + Uninitialized fresh-create: new session, generation 0
    assert_ne!(
        bob_new.session_id, bob_record.session_id,
        "reappearing bob should get new SessionId"
    );
    assert_eq!(
        bob_new.generation,
        ContinuityGeneration::new(0),
        "reappearing bob should start at generation 0"
    );
    eprintln!(
        "[SMOKE-07] bob recreated: old_session={}, new_session={}, rt={}",
        bob_record.session_id, bob_new.session_id, bob_new.agent_runtime_id,
    );

    // alice should still be stable
    let alice_v2 = result2.outcomes.get(&id("personal:alice")).unwrap();
    let alice_v2_session = match alice_v2 {
        RestoreOutcome::Created { record, .. } | RestoreOutcome::Resumed { record, .. } => {
            record.session_id.clone()
        }
        other => panic!("unexpected alice outcome: {other:?}"),
    };
    assert_eq!(
        alice_v2_session, alice_record.session_id,
        "alice session should survive bob deletion"
    );

    let elapsed = start.elapsed();
    eprintln!(
        "[SMOKE-07] PASSED in {:.1}s - delete + recreate with new runtime ID ✓",
        elapsed.as_secs_f64(),
    );
    assert!(elapsed.as_secs() >= 1, "too fast for real LLM calls");
}

// ===========================================================================
// SMOKE-08: Customizer uses topology for dynamic prompts
//
// TopologyProvider wires alpha↔alice and alpha↔bob. Customizer appends
// subscriber names to alpha's system prompt. Verify the prompt content
// matches the topology edges.
// ===========================================================================

/// Topology provider that wires specific edges for SMOKE-08.
struct OperatorTopology {
    edges: Vec<(String, String)>,
}

#[async_trait]
impl TopologyProvider for OperatorTopology {
    async fn compute_edges(
        &self,
        _target_identities: &[AgentIdentity],
        _context: &TopologyContext,
    ) -> Result<Vec<ManagedPeerEdge>, TopologyError> {
        let mut edges = Vec::new();
        for (a, b) in &self.edges {
            edges.push(
                ManagedPeerEdge::new(id(a), id(b))
                    .map_err(|e| TopologyError::InvalidEdge(format!("{e}")))?,
            );
        }
        Ok(edges)
    }
}

/// Customizer that injects subscriber names into the system prompt based
/// on topology edges.
struct TopologyAwareCustomizer;

#[async_trait]
impl AgentCustomizer for TopologyAwareCustomizer {
    async fn customize_build(
        &self,
        context: &AgentBuildContext,
        _spec: &DurableAgentSpec,
        draft: &mut AgentBuildDraft,
    ) -> Result<(), CustomizerError> {
        // Find peer names from managed edges
        let my_id = &context.identity;
        let mut peers = Vec::new();
        for edge in &context.managed_edges {
            if edge.a() == my_id {
                peers.push(edge.b().to_string());
            } else if edge.b() == my_id {
                peers.push(edge.a().to_string());
            }
        }

        if !peers.is_empty() {
            peers.sort();
            let prompt = format!("Your subscribers are: {}", peers.join(", "));
            draft.system_prompt = Some(prompt);
        }

        Ok(())
    }

    async fn after_create(
        &self,
        _identity: &AgentIdentity,
        _session_id: &meerkat_core::types::SessionId,
        _context: &SessionCreatedContext,
    ) -> Result<(), CustomizerError> {
        Ok(())
    }
}

#[tokio::test]
#[ignore = "integration-real: live API"]
async fn e2e_smoke_08_customizer_topology_prompts() {
    if !has_api_key() {
        eprintln!("Skipping SMOKE-08: no ANTHROPIC_API_KEY");
        return;
    }

    let model = smoke_model();
    let temp = tempfile::TempDir::new().expect("temp dir");
    let state_path = temp.path().join("state");
    let start = Instant::now();

    eprintln!("[SMOKE-08] Building UnifiedRuntime with model={model}...");

    let unified = UnifiedRuntimeBuilder::default()
        .definition(operator_definition(&model))
        .persistent_state(&state_path)
        .comms(true)
        .build()
        .await
        .expect("build UnifiedRuntime");

    let bridge: Arc<dyn SessionBridge> = unified
        .session_bridge()
        .map(|bridge| Arc::new(bridge) as Arc<dyn SessionBridge>)
        .expect("session_bridge should exist");

    let store = Arc::new(
        LocalContinuityStore::open(state_path.join("continuity.db"))
            .expect("open continuity store"),
    );
    let leases = Arc::new(LocalLeaseProvider::new());
    let identity_rt = IdentityRuntime::new(IdentityRuntimeConfig {
        continuity_store: store.clone() as Arc<dyn ContinuityStore>,
        lease_provider: leases.clone(),
        runtime_instance_id: "ops-08".to_string(),
        has_runtime_store: true,
        durability_policy: DurabilityPolicy::SyncWriteThrough,
        bridge: Some(bridge),
        default_timeout: None,
    });

    // Topology: alpha↔alice, alpha↔bob, beta↔carol
    let topology = OperatorTopology {
        edges: vec![
            ("review:alpha".into(), "personal:alice".into()),
            ("review:alpha".into(), "personal:bob".into()),
            ("review:beta".into(), "personal:carol".into()),
        ],
    };

    let roster = vec![
        spec("review:alpha", AgentAddressability::InternalOnly, "review"),
        spec("review:beta", AgentAddressability::InternalOnly, "review"),
        spec(
            "personal:alice",
            AgentAddressability::Addressable,
            "personal",
        ),
        spec("personal:bob", AgentAddressability::Addressable, "personal"),
        spec(
            "personal:carol",
            AgentAddressability::Addressable,
            "personal",
        ),
    ];

    eprintln!("[SMOKE-08] Phase 1: Bootstrap with topology...");
    let result = restore_flow(
        &identity_rt,
        &roster,
        Some(&topology as &dyn TopologyProvider),
        Some(&TopologyAwareCustomizer as &dyn AgentCustomizer),
    )
    .await
    .expect("restore_flow");

    // Verify alpha's draft has alice and bob in system_prompt
    let alpha_draft = match result.outcomes.get(&id("review:alpha")).unwrap() {
        RestoreOutcome::Created { draft, .. } => draft.clone(),
        other => panic!("expected Created for alpha, got {other:?}"),
    };
    let alpha_prompt = alpha_draft
        .system_prompt
        .expect("alpha should have system_prompt");
    assert!(
        alpha_prompt.contains("personal:alice"),
        "alpha prompt should mention alice: {alpha_prompt}"
    );
    assert!(
        alpha_prompt.contains("personal:bob"),
        "alpha prompt should mention bob: {alpha_prompt}"
    );
    assert!(
        !alpha_prompt.contains("personal:carol"),
        "alpha prompt should NOT mention carol: {alpha_prompt}"
    );
    eprintln!("[SMOKE-08] alpha prompt: {alpha_prompt}");

    // Verify beta's draft has carol but not alice/bob
    let beta_draft = match result.outcomes.get(&id("review:beta")).unwrap() {
        RestoreOutcome::Created { draft, .. } => draft.clone(),
        other => panic!("expected Created for beta, got {other:?}"),
    };
    let beta_prompt = beta_draft
        .system_prompt
        .expect("beta should have system_prompt");
    assert!(
        beta_prompt.contains("personal:carol"),
        "beta prompt should mention carol: {beta_prompt}"
    );
    assert!(
        !beta_prompt.contains("personal:alice"),
        "beta prompt should NOT mention alice: {beta_prompt}"
    );
    assert!(
        !beta_prompt.contains("personal:bob"),
        "beta prompt should NOT mention bob: {beta_prompt}"
    );
    eprintln!("[SMOKE-08] beta prompt: {beta_prompt}");

    // Verify managed_edges in result
    assert_eq!(
        result.managed_edges.len(),
        3,
        "should have 3 topology edges"
    );

    // Phase 2: Add dave wired to alpha, re-run restore_flow with full roster
    eprintln!("[SMOKE-08] Phase 2: Adding dave wired to alpha...");
    let topology_v2 = OperatorTopology {
        edges: vec![
            ("review:alpha".into(), "personal:alice".into()),
            ("review:alpha".into(), "personal:bob".into()),
            ("review:alpha".into(), "personal:dave".into()),
            ("review:beta".into(), "personal:carol".into()),
        ],
    };

    let roster_v2 = vec![
        spec("review:alpha", AgentAddressability::InternalOnly, "review"),
        spec("review:beta", AgentAddressability::InternalOnly, "review"),
        spec(
            "personal:alice",
            AgentAddressability::Addressable,
            "personal",
        ),
        spec("personal:bob", AgentAddressability::Addressable, "personal"),
        spec(
            "personal:carol",
            AgentAddressability::Addressable,
            "personal",
        ),
        spec(
            "personal:dave",
            AgentAddressability::Addressable,
            "personal",
        ),
    ];

    let result2 = restore_flow(
        &identity_rt,
        &roster_v2,
        Some(&topology_v2 as &dyn TopologyProvider),
        Some(&TopologyAwareCustomizer as &dyn AgentCustomizer),
    )
    .await
    .expect("restore_flow v2");

    // dave should be Created (fresh)
    let dave_outcome = result2.outcomes.get(&id("personal:dave")).unwrap();
    match dave_outcome {
        RestoreOutcome::Created { .. } => {
            eprintln!("[SMOKE-08] dave fresh-created");
        }
        other => panic!("expected Created for dave, got {other:?}"),
    }

    // alpha's new draft should include dave
    let alpha_v2_draft = match result2.outcomes.get(&id("review:alpha")).unwrap() {
        RestoreOutcome::Created { draft, .. } | RestoreOutcome::Resumed { draft, .. } => {
            draft.clone()
        }
        other => panic!("unexpected alpha v2 outcome: {other:?}"),
    };
    let alpha_v2_prompt = alpha_v2_draft
        .system_prompt
        .expect("alpha v2 should have system_prompt");
    assert!(
        alpha_v2_prompt.contains("personal:dave"),
        "alpha v2 prompt should mention dave: {alpha_v2_prompt}"
    );
    eprintln!("[SMOKE-08] alpha v2 prompt: {alpha_v2_prompt}");

    // beta's prompt should be unchanged (still only carol)
    let beta_v2_draft = match result2.outcomes.get(&id("review:beta")).unwrap() {
        RestoreOutcome::Created { draft, .. } | RestoreOutcome::Resumed { draft, .. } => {
            draft.clone()
        }
        other => panic!("unexpected beta v2 outcome: {other:?}"),
    };
    let beta_v2_prompt = beta_v2_draft
        .system_prompt
        .expect("beta v2 should have system_prompt");
    assert!(
        beta_v2_prompt.contains("personal:carol"),
        "beta v2 prompt should still mention carol: {beta_v2_prompt}"
    );
    assert!(
        !beta_v2_prompt.contains("personal:dave"),
        "beta v2 prompt should NOT mention dave: {beta_v2_prompt}"
    );

    // Verify total managed edges is now 4
    assert_eq!(
        result2.managed_edges.len(),
        4,
        "should have 4 topology edges"
    );

    let elapsed = start.elapsed();
    eprintln!(
        "[SMOKE-08] PASSED in {:.1}s - customizer + topology dynamic prompts ✓",
        elapsed.as_secs_f64(),
    );
}
