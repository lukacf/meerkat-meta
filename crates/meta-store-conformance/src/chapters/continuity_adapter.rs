//! `ContinuitySessionStoreAdapter` chapter: the Meerkat `SessionStore`
//! surface the identity-first session path actually provides.
//!
//! The full upstream baseline profile is NOT runnable against this adapter
//! from outside `meerkat-meta`: registered saves require
//! `register_session_runtime_state`, whose only public route is a
//! `MobSessionBridge` over a live `MobHandle` (the adapter's own
//! `register_session` is crate-private). This chapter therefore pins the
//! adapter contract that exists without registration — which is exactly the
//! surface Meerkat's committed-authority resolver reads through at restore —
//! and pins the divergences from the baseline profile loudly instead of
//! failing them:
//!
//! - `list()` deliberately returns empty (the continuity model is
//!   identity-keyed, not session-list-keyed);
//! - an unregistered `save` parks bytes in memory and returns `Ok(())`
//!   without persisting anything;
//! - `as_incremental` is a pinned expectation parameter: `false` today (the
//!   H2 capability swallow — every identity-first deployment persists
//!   O(session) per save), flipping to `true` when Phase M4 adds the
//!   incremental continuity channel.

use std::sync::Arc;

use meerkat_core::SessionStore;
use meerkat_core::session_store::session_projection_cas_token;
use meerkat_core::{SessionStoreError, types::SessionId};
use meerkat_meta::identity_first::{
    AgentIdentity, CheckpointVersion, ContinuityGeneration, ContinuitySessionStoreAdapter,
    FencingToken,
};
use meerkat_store_conformance::ConformanceFailure;

use crate::factory::ContinuityStoreFactory;
use crate::fixtures;
use crate::steps::Steps;

const CHAPTER: &str = "continuity_session_adapter";

/// Adapter chapter over any `ContinuityStore` that supports session-scoped
/// CAS deletion (`delete_session_snapshot_if_current_revision`); the
/// adapter's `delete` escalates a declined CAS delete to an error, so stores
/// without that capability cannot host this chapter.
///
/// `expect_incremental` pins the `as_incremental` capability: the adapter
/// genuinely forwards its substrate's
/// `ContinuityStore::as_incremental_sessions` (M4b), so pass whatever the
/// substrate advertises — `false` for the bundled `LocalContinuityStore`
/// (which defers its delta channel deliberately; see its impl note) and for
/// the whole-snapshot-verb `GatewayContinuityStore`, `true` for a substrate
/// that ships one. The chapter fails loudly on any mismatch in either
/// direction.
pub async fn continuity_session_adapter(
    factory: &dyn ContinuityStoreFactory,
    expect_incremental: bool,
) -> Result<(), ConformanceFailure> {
    let steps = Steps::chapter(CHAPTER);
    let store = factory.open().await?;
    let adapter = Arc::new(ContinuitySessionStoreAdapter::new(Arc::clone(&store)));

    // --- capability pin ------------------------------------------------------
    let step = "as_incremental_capability";
    let incremental = Arc::clone(&adapter).as_incremental().is_some();
    steps.ensure(
        step,
        incremental == expect_incremental,
        if expect_incremental {
            "expected the adapter to expose the incremental session-store capability, but \
             as_incremental returned None — the M4 incremental continuity channel is pinned as \
             present and has regressed"
        } else {
            "as_incremental unexpectedly returned Some: the adapter has grown the incremental \
             capability — flip this chapter's expectation deliberately (the H2 whole-blob \
             degradation pin is obsolete)"
        },
    )?;

    // --- load of an absent session ------------------------------------------
    let step = "load_absent_is_none";
    let absent = SessionId::new();
    steps.ensure(
        step,
        steps.wrap(step, adapter.load(&absent).await)?.is_none(),
        "loading an absent session must return None, never a synthesized empty session",
    )?;

    // --- unregistered save parks, never persists ------------------------------
    let step = "unregistered_save_parks_not_persists";
    let parked = fixtures::session_with_texts(&["unregistered turn"])?;
    steps.wrap(step, adapter.save(&parked).await)?;
    steps.ensure(
        step,
        steps.wrap(step, adapter.load(parked.id()).await)?.is_none(),
        "an unregistered save must not become durable: the bytes are parked until \
         register_session_runtime_state publishes the owning identity",
    )?;
    steps.ensure(
        step,
        steps
            .wrap(step, store.load_session_snapshot(parked.id()).await)?
            .is_none(),
        "an unregistered save must write nothing to the continuity store",
    )?;

    // --- store-seeded snapshot loads through the adapter ----------------------
    let step = "store_seeded_snapshot_loads";
    let seeded_identity = steps.wrap(step, AgentIdentity::parse("adapter:seeded"))?;
    let seeded = fixtures::session_with_texts(&["seeded turn one", "seeded turn two"])?;
    let snapshot = fixtures::session_snapshot(&seeded)?;
    steps.wrap(
        step,
        store
            .save_session_snapshot(
                &seeded_identity,
                seeded.id(),
                ContinuityGeneration::new(1),
                CheckpointVersion::new(1),
                FencingToken::new(1),
                &snapshot,
            )
            .await,
    )?;
    let loaded = steps
        .wrap(step, adapter.load(seeded.id()).await)?
        .ok_or_else(|| {
            steps.fail(
                step,
                "a store-seeded snapshot must load through the adapter",
            )
        })?;
    let seeded_revision = steps.wrap(step, seeded.transcript_revision())?;
    let loaded_revision = steps.wrap(step, loaded.transcript_revision())?;
    steps.ensure(
        step,
        loaded.id() == seeded.id() && loaded_revision == seeded_revision,
        "the adapter must serve the continuity snapshot's session document unchanged",
    )?;

    // --- exists follows load ---------------------------------------------------
    let step = "exists_follows_load";
    steps.ensure(
        step,
        steps.wrap(step, adapter.exists(seeded.id()).await)?,
        "exists must report true for a session whose snapshot is durable",
    )?;
    steps.ensure(
        step,
        !steps.wrap(step, adapter.exists(&SessionId::new()).await)?,
        "exists must report false for an absent session",
    )?;

    // --- list is deliberately empty ---------------------------------------------
    let step = "list_returns_empty_by_contract";
    let listed = steps.wrap(
        step,
        adapter.list(meerkat_core::SessionFilter::default()).await,
    )?;
    steps.ensure(
        step,
        listed.is_empty(),
        "list() through the continuity adapter deliberately returns empty — the continuity \
         model is identity-keyed, not session-list-keyed. This is pinned contract, not a bug; \
         if the adapter grows listing, flip this step deliberately",
    )?;

    // --- an embedded-id mismatch is a typed error, never silent -----------------
    let step = "embedded_id_mismatch_is_typed_error";
    let foreign = fixtures::session_with_texts(&["foreign document"])?;
    let foreign_snapshot = fixtures::session_snapshot(&foreign)?;
    let mismatched_key = fixtures::session_with_texts(&["key holder"])?;
    let mismatch_identity = steps.wrap(step, AgentIdentity::parse("adapter:mismatch"))?;
    steps.wrap(
        step,
        store
            .save_session_snapshot(
                &mismatch_identity,
                mismatched_key.id(),
                ContinuityGeneration::new(1),
                CheckpointVersion::new(1),
                FencingToken::new(1),
                &foreign_snapshot,
            )
            .await,
    )?;
    match adapter.load(mismatched_key.id()).await {
        Err(SessionStoreError::Serialization(_)) => {}
        Err(other) => {
            return Err(steps.fail(
                step,
                format!(
                    "a snapshot whose embedded session id mismatches its key must fail with a \
                     Serialization error, got: {other}"
                ),
            ));
        }
        Ok(_) => {
            return Err(steps.fail(
                step,
                "a snapshot whose embedded session id mismatches its key must never load \
                 silently",
            ));
        }
    }

    // --- CAS delete honesty -------------------------------------------------------
    let step = "delete_if_current_revision_cas";
    let stale_token = "conformance-stale-token";
    steps.ensure(
        step,
        !steps.wrap(
            step,
            adapter
                .delete_if_current_revision(seeded.id(), stale_token)
                .await,
        )?,
        "delete_if_current_revision with a stale token must return false",
    )?;
    steps.ensure(
        step,
        steps.wrap(step, adapter.load(seeded.id()).await)?.is_some(),
        "a stale-token delete must leave the snapshot in place",
    )?;
    let current_token = steps.wrap(step, session_projection_cas_token(&seeded))?;
    steps.ensure(
        step,
        steps.wrap(
            step,
            adapter
                .delete_if_current_revision(seeded.id(), &current_token)
                .await,
        )?,
        "delete_if_current_revision with the current token must return true",
    )?;
    steps.ensure(
        step,
        steps.wrap(step, adapter.load(seeded.id()).await)?.is_none(),
        "a current-token delete must remove the snapshot",
    )?;

    // --- unconditional delete over a seeded snapshot -------------------------------
    let step = "delete_removes_seeded_snapshot";
    let doomed_identity = steps.wrap(step, AgentIdentity::parse("adapter:doomed"))?;
    let doomed = fixtures::session_with_texts(&["doomed turn"])?;
    let doomed_snapshot = fixtures::session_snapshot(&doomed)?;
    steps.wrap(
        step,
        store
            .save_session_snapshot(
                &doomed_identity,
                doomed.id(),
                ContinuityGeneration::new(1),
                CheckpointVersion::new(1),
                FencingToken::new(1),
                &doomed_snapshot,
            )
            .await,
    )?;
    steps.wrap(step, adapter.delete(doomed.id()).await)?;
    steps.ensure(
        step,
        steps.wrap(step, adapter.load(doomed.id()).await)?.is_none(),
        "delete must remove the seeded snapshot",
    )?;
    steps.ensure(
        step,
        steps
            .wrap(step, store.load_session_snapshot(doomed.id()).await)?
            .is_none(),
        "delete must remove the snapshot from the underlying continuity store",
    )?;
    Ok(())
}
