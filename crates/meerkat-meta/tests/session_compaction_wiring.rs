//! Session-compaction wiring on Meta's built session path.
//!
//! # Why this file exists
//!
//! A production Meta deployment sent 616_000 input tokens to produce a
//! five-token reply, with turn latency scaling from 3.4s at 13 messages to
//! 400s at 466 messages. The reported diagnosis was "no compactor is
//! installed anywhere in meta's session-build path".
//!
//! That diagnosis is wrong, and nothing in the repo could prove it either
//! way — hence this file. Meerkat's `AgentFactory::build_agent` always
//! installs a `meerkat_session::DefaultCompactor` (the `session-compaction`
//! feature is unified on through `meerkat-mob`'s dependency declaration), and
//! Meta routes every session build through it. The real defect is the
//! *trigger*: with no host policy, meerkat replaces its documented `100_000`
//! default with `context_window * 4 / 5`, which on a million-token model is
//! `840_000` tokens — so 616_000 legitimately sails through.
//!
//! These tests pin both halves:
//!
//! * `built_member_session_path_carries_a_compactor` fails if the compactor
//!   ever stops being installed (feature drift, or a Meta path that stops
//!   routing through `AgentFactory`).
//! * `inherited_policy_lets_a_200k_token_turn_through` pins the inherited
//!   model-aware trigger that made the knob look dead.
//! * `host_compaction_policy_pins_the_trigger` proves
//!   `UnifiedRuntimeBuilder::compaction()` reaches the compactor the member's
//!   session is actually built with.

#![allow(clippy::expect_used, clippy::panic, clippy::uninlined_format_args)]

use std::sync::Arc;
use std::time::Duration;

use meerkat_client::{LlmDoneOutcome, LlmEvent, TestClient};
use meerkat_core::types::{ContentInput, HandlingMode};
use meerkat_core::{AgentEvent, StopReason, Usage};
use meerkat_meta::{MemberTurnOptions, UnifiedRuntime};
use meerkat_mob::{MobDefinition, SpawnMemberSpec};

/// The one definition of the normalized-provider-accounting contract every
/// Meta LLM double must satisfy under meerkat 0.8.22. See the module docs.
#[path = "support/llm_usage.rs"]
mod llm_usage;

/// A catalogued million-token model, so the inherited (model-aware) trigger
/// is the production one: `context_window * 4 / 5`.
const FIXTURE_MODEL: &str = "gpt-5.5";
/// Larger than four fifths of any catalogued context window, so a turn
/// reporting it must compact under ANY inherited threshold.
const ALWAYS_OVER_THRESHOLD: u64 = 10_000_000;
/// Enormous by every practical measure and still under four fifths of a
/// million-token window — the exact production condition.
const HUGE_BUT_UNDER_INHERITED_THRESHOLD: u64 = 200_000;
/// Half the fixture turn's reported tokens — structurally under it, so
/// declaring it flips the same turn from "never compacts" to "compacts".
const PINNED_THRESHOLD: u64 = HUGE_BUT_UNDER_INHERITED_THRESHOLD / 2;

const MEMBER_ALIAS: &str = "compaction-worker";
const TURN_TIMEOUT: Duration = Duration::from_secs(20);
/// How long the event drain waits for a straggler before concluding the turn
/// produced no further events.
const DRAIN_QUIET_PERIOD: Duration = Duration::from_millis(500);

/// A deterministic client that reports a fixed provider input-token count.
///
/// Provider-reported `input_tokens` is the signal meerkat's `DefaultCompactor`
/// prefers, so driving it directly exercises the real trigger without a
/// multi-megabyte fixture transcript. `TestClient` declares `Provider::Other`,
/// which the agent factory accepts against any catalogued model.
///
/// 0.8.22: `UsageUpdate` now carries a `TurnUsage`, and the agent checks its
/// accounting identity against the ADAPTER's provider/model, not the raw
/// client's - which differ here, since `TestClient` declares `Provider::Other`
/// while the adapter is bound to the canonical identity the catalog resolves
/// for [`FIXTURE_MODEL`]. Declaring `Provider::Other` in the accounting would
/// fail every turn with `normalized_provider_accounting_identity_mismatch`, so
/// the shared helper resolves the accounting provider from the MODEL through
/// the same catalog authority `AgentFactory` used, taking the client's own
/// declaration only as the uncatalogued fallback.
///
/// Note also that `TestClient::new` (unlike `TestClient::default`) leaves
/// `synthesize_usage` OFF, so nothing backfills a missing `UsageUpdate` here:
/// the event below is the only accounting this fixture will ever report.
/// `host_declared` is the honest convention for it - an embedded test client
/// asserting its own inclusive input total, which is exactly what the trigger
/// reads back as `last_input_tokens`.
fn usage_reporting_client(input_tokens: u64) -> Arc<TestClient> {
    Arc::new(TestClient::new(vec![
        LlmEvent::TextDelta {
            delta: "ok".to_string(),
            meta: None,
        },
        LlmEvent::UsageUpdate {
            usage: llm_usage::turn_usage(
                FIXTURE_MODEL,
                meerkat_core::Provider::Other,
                Usage {
                    input_tokens,
                    output_tokens: 1,
                    ..Default::default()
                },
            ),
        },
        LlmEvent::Done {
            outcome: LlmDoneOutcome::success(StopReason::EndTurn),
        },
    ]))
}

fn definition(mob_id: &str) -> MobDefinition {
    MobDefinition::from_toml(&format!(
        r#"
[mob]
id = "{mob_id}"

[profiles.worker]
model = "{FIXTURE_MODEL}"
runtime_mode = "turn_driven"
external_addressable = true

[profiles.worker.tools]
comms = true
"#
    ))
    .expect("compaction fixture definition parses")
}

/// Drive two real member turns and report whether compaction fired.
///
/// Two turns are the minimum the contract allows: meerkat never compacts on a
/// session's first LLM boundary, and `last_input_tokens` is only populated
/// once a response has been observed.
async fn compaction_fired(runtime: &UnifiedRuntime) -> bool {
    let (tx, mut rx) = tokio::sync::mpsc::channel(2048);
    for prompt in ["first", "second"] {
        let admission = runtime
            .start_member_turn(
                MEMBER_ALIAS,
                ContentInput::Text(prompt.to_string()),
                HandlingMode::Queue,
                MemberTurnOptions::new(),
                Some(tx.clone()),
            )
            .await
            .unwrap_or_else(|error| panic!("turn '{prompt}' must be admitted: {error}"));
        tokio::time::timeout(TURN_TIMEOUT, admission.turn.wait())
            .await
            .unwrap_or_else(|error| panic!("turn '{prompt}' timed out: {error}"))
            .unwrap_or_else(|error| panic!("turn '{prompt}' failed: {error}"));
    }
    drop(tx);

    // Bounded drain rather than `try_recv`: nonterminal events are forwarded
    // live, and the runtime may hold its own sender clone, so neither "the
    // channel is closed" nor "the channel is empty right now" is a sound
    // stopping condition on its own.
    let mut fired = false;
    while let Ok(Some(envelope)) = tokio::time::timeout(DRAIN_QUIET_PERIOD, rx.recv()).await {
        if matches!(envelope.payload, AgentEvent::CompactionStarted { .. }) {
            fired = true;
        }
    }
    fired
}

/// Build a runtime whose members answer with `usage_reporting_client`, and
/// optionally declare a host compaction policy.
async fn runtime_with(
    mob_id: &str,
    input_tokens: u64,
    compaction: Option<meerkat_core::config::CompactionRuntimeConfig>,
) -> UnifiedRuntime {
    let mut builder = UnifiedRuntime::builder()
        .definition(definition(mob_id))
        .default_llm_client(usage_reporting_client(input_tokens))
        .max_sessions(4);
    if let Some(policy) = compaction {
        builder = builder.compaction(policy);
    }
    let runtime = Box::pin(builder.build())
        .await
        .unwrap_or_else(|error| panic!("runtime builds: {error}"));
    runtime
        .spawn(SpawnMemberSpec::from_wire(
            "worker".to_string(),
            MEMBER_ALIAS.to_string(),
            None,
            None,
            None,
        ))
        .await
        .unwrap_or_else(|error| panic!("member spawns: {error}"));
    runtime
}

fn pinned_policy(threshold: u64) -> meerkat_core::config::CompactionRuntimeConfig {
    meerkat_core::config::CompactionRuntimeConfig {
        auto_compact_threshold: threshold,
        auto_compact_threshold_explicit: true,
        ..Default::default()
    }
}

/// The load-bearing assertion: the session every Meta member turn runs on
/// carries a compactor. If `session-compaction` ever stops being unified on,
/// or a Meta path stops routing through `AgentFactory::build_agent`,
/// `should_compact` is never consulted and this fails.
#[tokio::test]
async fn built_member_session_path_carries_a_compactor() {
    let runtime = runtime_with("compactor-installed", ALWAYS_OVER_THRESHOLD, None).await;
    assert!(
        compaction_fired(&runtime).await,
        "a turn reporting more input tokens than any plausible threshold must \
         compact; no compactor is installed on the built session path",
    );
}

/// The other half of the production diagnosis: with no host policy the trigger
/// is `context_window * 4 / 5`, so a transcript that is enormous by every
/// practical measure still sails through. This is why `auto_compact_threshold`
/// reads as dead config in a Meta deployment even though a compactor is
/// present.
#[tokio::test]
async fn inherited_policy_lets_a_200k_token_turn_through() {
    let runtime = runtime_with(
        "compactor-inherited",
        HUGE_BUT_UNDER_INHERITED_THRESHOLD,
        None,
    )
    .await;
    assert!(
        !compaction_fired(&runtime).await,
        "200k input tokens is under four fifths of a million-token window, so \
         the inherited policy does nothing; if this starts firing, meerkat's \
         model-aware default changed",
    );
}

/// `UnifiedRuntimeBuilder::compaction()` makes the threshold real: the same
/// turn that inherits its way past compaction fires immediately once the host
/// pins a threshold under it.
#[tokio::test]
async fn host_compaction_policy_pins_the_trigger() {
    let runtime = runtime_with(
        "compaction-policy-pinned",
        HUGE_BUT_UNDER_INHERITED_THRESHOLD,
        Some(pinned_policy(PINNED_THRESHOLD)),
    )
    .await;
    assert!(
        compaction_fired(&runtime).await,
        "a declared host threshold must reach the compactor the member's \
         session is built with",
    );
}
