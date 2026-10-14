//! Reusable assembly for the FULL agent-memory stack (§6 architecture): a
//! judgment-capable provider with the §10.1 taint firewall, plus the
//! judgment plane (Distiller, Steward) and the member-event observer feeding
//! them.
//!
//! Until this module existed the stack was hand-assembled inside
//! `rpc_gateway.rs`, which made the judgment plane unreachable for Rust
//! embedders driving `UnifiedRuntimeBuilder` directly (the production deployment
//! shape: no gateways, no SDKs — builder or nothing). The builder now exposes
//! it through [`crate::UnifiedRuntimeBuilder::persistent_agent_memory_stack`].
//!
//! M4 de-weld: assembly is capability-driven, not store-kind-driven. The
//! provider parameter is the plain [`AgentMemoryProvider`]; each piece of the
//! plane probes for the capability it needs ([`TaintableStore`] for the
//! firewall, [`StewardStore`] for the Steward, `TombstoneSource` for the
//! Distiller, [`MemoryPanelStore`] for the console panel handle on the
//! returned stack) and fails with a named error when a requested engine's
//! capability is missing. An externally supplied recall-only provider is
//! recall-only because it advertises none of these capabilities.
//!
//! Scope notes, deliberate for v1:
//! - The **Hygienist** engine stays internal and parked. Public SDK/gateway
//!   activation is refused until the release has bounded projection and real
//!   provider proof; the builder does not acquire an activation seam here.
//! - The gateway keeps its existing hand wiring (identical semantics, more
//!   seams: fail-init surfacing, gating/conflict bridges, schedule-host dream
//!   suppression). Converging it onto this module is tracked follow-up work.
//!
//! [`TaintableStore`]: crate::memory::capabilities::TaintableStore
//! [`StewardStore`]: crate::memory::capabilities::StewardStore
//! [`MemoryPanelStore`]: crate::memory::capabilities::MemoryPanelStore

use std::path::Path;
use std::sync::Arc;

use crate::MemberAgentEventSink;
use crate::identity_first::agent_memory::{AgentMemoryConfig, AgentMemoryProvider};
use crate::memory::capabilities::{MemoryPanelStore, StewardStore};
use crate::memory::distiller::{
    DistillerConfig, DistillerEngine, DistillerProfile, DistillerTriggers, FactoryDistillerHandle,
    HnswDiscardSource, SessionStoreTranscriptSource,
};
use crate::memory::events::MemoryEventSink;
use crate::memory::sqlite_store::SqliteAgentMemoryStore;
use crate::memory::steward::{
    FactoryStewardHandle, MemoryConflictBridge, MemoryGatingBridge, MobPurposeSource,
    SessionStoreEvidenceResolver, StewardConfig, StewardEngine, StewardProfile, StewardTriggers,
};
use crate::memory::taint::{SessionTaintTracker, TaintLlmWriteGate};

/// Engine enables + models for the judgment plane. `Default` = store +
/// firewall only (no background LLM work), matching the architecture's
/// "each stage ships alone" posture.
#[derive(Debug, Clone, Default)]
pub struct MemoryEnginesConfig {
    pub distiller: DistillerConfig,
    pub steward: StewardConfig,
}

/// Resolve the exact Distiller profile used by stack assembly.
///
/// This is public so gateway contract tests can prove that host JSON reaches
/// the same profile construction path as a live engine. Runtime callers should
/// normally use [`attach_memory_engines`] instead.
#[doc(hidden)]
pub fn effective_distiller_profile(config: &DistillerConfig) -> Result<DistillerProfile, String> {
    let mut profile = DistillerProfile::embedded_default();
    if let Some(model) = config.model.as_deref() {
        profile = profile
            .with_model_override(model)
            .map_err(|e| format!("agent memory distiller: {e}"))?;
    }
    if let Some(max_output_tokens) = config.max_output_tokens {
        profile = profile
            .with_max_output_tokens(max_output_tokens)
            .map_err(|e| format!("agent memory distiller: {e}"))?;
    }
    Ok(profile)
}

/// Resolve the exact Steward profile used by stack assembly.
///
/// Kept visible for the same gateway-to-wiring contract proof as
/// [`effective_distiller_profile`].
#[doc(hidden)]
pub fn effective_steward_profile(config: &StewardConfig) -> Result<StewardProfile, String> {
    let mut profile = StewardProfile::embedded_default();
    if let Some(model) = config.model.as_deref() {
        profile = profile
            .with_model_override(model)
            .map_err(|e| format!("agent memory steward: {e}"))?;
    }
    if let Some(max_output_tokens) = config.max_output_tokens {
        profile = profile
            .with_max_output_tokens(max_output_tokens)
            .map_err(|e| format!("agent memory steward: {e}"))?;
    }
    Ok(profile)
}

/// Host-supplied seams for the stack: where the engines read transcripts,
/// where events project, and the steward's late-binding bridges. Everything
/// optional degrades gracefully (engines that need a missing seam fail the
/// build with a named error; bridges just don't bind).
#[derive(Default)]
pub struct MemoryStackSeams {
    pub persistent_state: Option<std::path::PathBuf>,
    pub transcript_store: Option<Arc<dyn meerkat::SessionStore>>,
    pub event_sink: Option<Arc<dyn MemoryEventSink>>,
    pub mob_purpose: Option<Arc<dyn MobPurposeSource>>,
    pub steward_gating: Option<Arc<dyn MemoryGatingBridge>>,
    pub steward_conflicts: Option<Arc<dyn MemoryConflictBridge>>,
}

/// The assembled stack. The caller finishes the wiring that needs the live
/// runtime: register `panel` as the console panel store (when advertised),
/// pass `sinks` to [`crate::spawn_member_event_observer`], hand
/// `taint`/engines to the injector, and start the steward dream (schedule
/// host or `StewardEngine::spawn_dream_loop`).
pub struct AgentMemoryStack {
    pub provider: Arc<dyn AgentMemoryProvider>,
    /// The provider's steward read/write surface, when advertised. Present
    /// independent of whether the Steward engine is enabled — gateway
    /// extras (e.g. the Hygienist's span-reference source) read through it.
    pub steward_store: Option<Arc<dyn StewardStore>>,
    /// The provider's console Memory panel read API, when advertised.
    pub panel: Option<Arc<dyn MemoryPanelStore>>,
    pub taint: SessionTaintTracker,
    pub distiller: Option<Arc<DistillerEngine>>,
    pub steward: Option<Arc<StewardEngine>>,
    /// Observe-stream consumers (taint + engine triggers), in wiring order.
    pub sinks: Vec<Arc<dyn MemberAgentEventSink>>,
}

/// Open the bundled SQLite store with the §10.1 firewall and assemble the
/// enabled engines. `persistent_state` is the runtime's state dir (engine
/// LLM factories and the compaction-discard source live under it);
/// `transcript_store` is a read handle on the session store the mob bridge
/// persists to (required when either engine is enabled).
pub fn build_sqlite_memory_stack(
    memory_dir: &Path,
    config: &AgentMemoryConfig,
    engines: &MemoryEnginesConfig,
    seams: MemoryStackSeams,
) -> Result<AgentMemoryStack, String> {
    let store = SqliteAgentMemoryStore::open(memory_dir)
        .map_err(|e| format!("failed to open agent memory store: {e}"))?;
    attach_memory_engines(Arc::new(store), config, engines, seams)
}

/// Stage-2 assembly over an already-open provider: wire the §10.1 firewall
/// (tracker + write gate + event sinks) and the enabled engines. Used by the
/// builder path, where the provider doubles as the one handed to the
/// customizer before the runtime exists.
///
/// Capability requirements: the firewall needs [`TaintableStore`] (always —
/// the Recorder must not ship without it); the Distiller additionally needs
/// tombstone reads; the Steward additionally needs [`StewardStore`]. Each
/// missing capability is a named error, never a silent downgrade.
///
/// [`TaintableStore`]: crate::memory::capabilities::TaintableStore
pub fn attach_memory_engines(
    provider: Arc<dyn AgentMemoryProvider>,
    config: &AgentMemoryConfig,
    engines: &MemoryEnginesConfig,
    seams: MemoryStackSeams,
) -> Result<AgentMemoryStack, String> {
    let MemoryStackSeams {
        persistent_state,
        transcript_store,
        event_sink,
        mob_purpose,
        steward_gating,
        steward_conflicts,
    } = seams;
    let persistent_state = persistent_state.as_deref();
    let event_sink =
        event_sink.ok_or_else(|| "agent memory stack requires an event sink".to_string())?;
    let taintable = provider.as_taintable().ok_or_else(|| {
        "agent memory stack requires a provider with firewall controls (TaintableStore)".to_string()
    })?;
    // §10.1 taint firewall: the Recorder must not ship without it. This
    // deliberately REPLACES any trackerless posture gate installed earlier.
    let taint = SessionTaintTracker::new(config.content_trust.clone());
    taintable.set_llm_write_gate(Arc::new(TaintLlmWriteGate::new(
        Some(taint.clone()),
        config.llm_writes,
    )));
    taintable.set_event_sink(event_sink.clone());
    taint.set_event_sink(event_sink.clone());

    let mut sinks: Vec<Arc<dyn MemberAgentEventSink>> = vec![Arc::new(taint.clone())];
    let realm = config.realm.clone();

    let state_for_engines = |what: &str| -> Result<&Path, String> {
        persistent_state.ok_or_else(|| format!("agent memory {what} requires persistent_state"))
    };
    let transcripts_for_engines = |what: &str| -> Result<Arc<dyn meerkat::SessionStore>, String> {
        transcript_store
            .clone()
            .ok_or_else(|| format!("agent memory {what} requires a session transcript store"))
    };

    let distiller = if engines.distiller.enabled {
        let state = state_for_engines("distiller")?;
        let transcripts = transcripts_for_engines("distiller")?;
        let tombstones = provider.as_tombstone_source().ok_or_else(|| {
            "agent memory distiller requires a provider with tombstone reads (TombstoneSource)"
                .to_string()
        })?;
        let profile = effective_distiller_profile(&engines.distiller)?;
        let handle =
            FactoryDistillerHandle::new(state, meerkat::Config::default(), &realm, &profile);
        let engine = Arc::new(DistillerEngine::new(
            profile,
            engines.distiller.clone(),
            Arc::new(handle),
            provider.clone(),
            tombstones,
            Arc::new(SessionStoreTranscriptSource::new(transcripts)),
            // Meerkat's session semantic memory lives at
            // <persistent_state>/memory; absent dir ⇒ nothing preserved.
            Some(Arc::new(HnswDiscardSource::new(state.join("memory")))),
            Some(taint.clone()),
            realm.clone(),
        ));
        engine.set_event_sink(event_sink.clone());
        sinks.push(Arc::new(DistillerTriggers::new(engine.clone())));
        Some(engine)
    } else {
        None
    };

    let steward_store = provider.as_steward_store();
    let steward = if engines.steward.enabled {
        let store = steward_store.clone().ok_or_else(|| {
            "agent memory steward requires a provider with the steward surface (StewardStore)"
                .to_string()
        })?;
        let state = state_for_engines("steward")?;
        let transcripts = transcripts_for_engines("steward")?;
        let profile = effective_steward_profile(&engines.steward)?;
        let transcripts_source: Arc<dyn crate::memory::distiller::TranscriptSource> =
            Arc::new(SessionStoreTranscriptSource::new(transcripts));
        // §10.2 P3 validator extension: agent_verified retiers must cite
        // evidence that resolves against the session store.
        taintable.set_evidence_resolver(Arc::new(SessionStoreEvidenceResolver::new(
            transcripts_source.clone(),
            tokio::runtime::Handle::current(),
        )));
        let handle = FactoryStewardHandle::new(
            state.to_path_buf(),
            meerkat::Config::default(),
            realm.clone(),
            &profile,
        );
        let mut engine = StewardEngine::new(
            profile,
            engines.steward.clone(),
            Arc::new(handle),
            store,
            transcripts_source,
            realm,
        )
        .with_events(event_sink.clone())
        .with_operator_routing(
            config.operator_scope
                == crate::identity_first::agent_memory::AgentMemoryOperatorScope::Provisional,
        );
        if let Some(purpose) = mob_purpose {
            engine = engine.with_mob_context(purpose);
        }
        if let Some(gating) = steward_gating {
            engine = engine.with_gating(gating);
        }
        if let Some(conflicts) = steward_conflicts {
            engine = engine.with_conflicts(conflicts);
        }
        let engine = Arc::new(engine);
        sinks.push(Arc::new(StewardTriggers::new(engine.clone())));
        Some(engine)
    } else {
        None
    };

    Ok(AgentMemoryStack {
        steward_store,
        panel: provider.as_memory_panel_store(),
        provider,
        taint,
        distiller,
        steward,
        sinks,
    })
}
