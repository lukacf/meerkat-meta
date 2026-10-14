//! `ContinuityStore` profile chapters: fencing-token CAS, checkpoint-version
//! monotonicity, generation rebinds, snapshot round-trips, reset rollback,
//! and the bundled fencing-floor pair.

use std::path::Path;

use meerkat_core::session_store::session_projection_cas_token;
use meerkat_meta::identity_first::{
    AgentIdentity, AgentRuntimeId, CheckpointVersion, ContinuityGeneration, ContinuityRecord,
    ContinuityResolveState, ContinuityStore, ContinuityStoreError, FencingToken,
    LeaseAcquireResult, LeaseProvider, LocalContinuityStore, LocalLeaseProvider,
};
use meerkat_store_conformance::ConformanceFailure;

use crate::factory::ContinuityStoreFactory;
use crate::fixtures;
use crate::steps::Steps;

const CHAPTER: &str = "continuity_store";

fn identity(
    steps: &Steps,
    step: &'static str,
    raw: &str,
) -> Result<AgentIdentity, ConformanceFailure> {
    steps.wrap(step, AgentIdentity::parse(raw))
}

fn runtime_id(
    steps: &Steps,
    step: &'static str,
    raw: &str,
) -> Result<AgentRuntimeId, ConformanceFailure> {
    steps.wrap(step, AgentRuntimeId::parse(raw))
}

/// Core `ContinuityStore` profile.
///
/// Pins: total `resolve_many`, pre-registration snapshot admission, fencing
/// CAS (stale rejected with the typed error, equal accepted, higher
/// accepted), strictly-increasing `CheckpointVersion` per save (an in-place
/// rewrite at the same version is inexpressible through the trait),
/// generation-monotonic upserts, version preservation across same-generation
/// rebinds and reset across generation advances, snapshot byte round-trips,
/// and honesty of the CAS deletes (`delete_session_snapshot_if_current_revision`
/// may be declared-unsupported via `Ok(false)`, but must never report a
/// successful no-op).
pub async fn continuity_store(
    factory: &dyn ContinuityStoreFactory,
) -> Result<(), ConformanceFailure> {
    let steps = Steps::chapter(CHAPTER);
    let store = factory.open().await?;

    resolve_many_total(&steps, store.as_ref()).await?;
    pre_registration_snapshot_admitted(&steps, store.as_ref()).await?;
    fencing_and_version_cas(&steps, store.as_ref()).await?;
    Ok(())
}

async fn resolve_many_total(
    steps: &Steps,
    store: &dyn ContinuityStore,
) -> Result<(), ConformanceFailure> {
    const STEP: &str = "resolve_many_total";
    let ghost = identity(steps, STEP, "conformance:ghost")?;
    let resolved = steps.wrap(STEP, store.resolve_many(std::slice::from_ref(&ghost)).await)?;
    steps.ensure(
        STEP,
        resolved.len() == 1,
        "resolve_many must return exactly one entry per requested identity",
    )?;
    steps.ensure(
        STEP,
        matches!(
            resolved.get(&ghost),
            Some(ContinuityResolveState::Uninitialized)
        ),
        "an unknown identity must resolve to an explicit Uninitialized entry, never a missing one",
    )?;
    Ok(())
}

async fn pre_registration_snapshot_admitted(
    steps: &Steps,
    store: &dyn ContinuityStore,
) -> Result<(), ConformanceFailure> {
    const STEP: &str = "pre_registration_snapshot_admitted";
    // A snapshot save with no continuity record yet is admitted (the
    // pre-registration path `PersistentSessionService` hits during member
    // creation) and its bytes round-trip unchanged.
    let seed_identity = identity(steps, STEP, "conformance:preseed")?;
    let session = fixtures::session_with_texts(&["pre-registration turn"])?;
    let snapshot = fixtures::session_snapshot(&session)?;
    steps.wrap(
        STEP,
        store
            .save_session_snapshot(
                &seed_identity,
                session.id(),
                ContinuityGeneration::new(1),
                CheckpointVersion::new(1),
                FencingToken::new(1),
                &snapshot,
            )
            .await,
    )?;
    let loaded = steps
        .wrap(STEP, store.load_session_snapshot(session.id()).await)?
        .ok_or_else(|| steps.fail(STEP, "pre-registration snapshot must load back"))?;
    steps.ensure(
        STEP,
        loaded.data == snapshot.data,
        "snapshot bytes must round-trip unchanged",
    )?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn fencing_and_version_cas(
    steps: &Steps,
    store: &dyn ContinuityStore,
) -> Result<(), ConformanceFailure> {
    let main = identity(steps, "record_upsert_resolve", "conformance:main")?;
    let runtime = runtime_id(steps, "record_upsert_resolve", "rt-conformance")?;

    // --- record upsert + resolve round trip -------------------------------
    let step = "record_upsert_resolve";
    let mut session = fixtures::session_with_texts(&["continuity turn one"])?;
    let record = ContinuityRecord {
        identity: main.clone(),
        agent_runtime_id: runtime.clone(),
        session_id: session.id().clone(),
        generation: ContinuityGeneration::new(1),
        checkpoint_version: CheckpointVersion::new(0),
    };
    steps.wrap(
        step,
        store
            .upsert_continuity_record(&record, FencingToken::new(1))
            .await,
    )?;
    let resolved = steps.wrap(step, store.resolve_many(std::slice::from_ref(&main)).await)?;
    match resolved.get(&main) {
        Some(ContinuityResolveState::Ready { record: current }) => {
            steps.ensure(
                step,
                current == &record,
                "resolve_many must serve back the upserted record unchanged",
            )?;
        }
        other => {
            return Err(steps.fail(step, format!("expected Ready after upsert, got {other:?}")));
        }
    }

    // --- fencing-token CAS -------------------------------------------------
    let step = "fencing_token_cas";
    let snapshot_v1 = fixtures::session_snapshot(&session)?;
    // Equal fence accepted (the CAS is monotonic `>=`, not strict).
    steps.wrap(
        step,
        store
            .save_session_snapshot(
                &main,
                session.id(),
                ContinuityGeneration::new(1),
                CheckpointVersion::new(1),
                FencingToken::new(1),
                &snapshot_v1,
            )
            .await,
    )?;
    // Stale fence rejected with the typed error naming presented and current.
    fixtures::push_text(&mut session, "continuity turn two")?;
    let snapshot_v2 = fixtures::session_snapshot(&session)?;
    match store
        .save_session_snapshot(
            &main,
            session.id(),
            ContinuityGeneration::new(1),
            CheckpointVersion::new(2),
            FencingToken::new(0),
            &snapshot_v2,
        )
        .await
    {
        Err(ContinuityStoreError::StaleFencingToken {
            presented, current, ..
        }) => {
            steps.ensure(
                step,
                presented.get() == 0 && current.get() >= 1,
                format!(
                    "StaleFencingToken must name presented (0) and current (>=1), got presented \
                     {presented}, current {current}"
                ),
            )?;
        }
        Err(other) => {
            return Err(steps.fail(
                step,
                format!("a stale fence must fail with StaleFencingToken, got: {other}"),
            ));
        }
        Ok(()) => {
            return Err(steps.fail(step, "a stale fencing token must be rejected"));
        }
    }
    // Higher fence accepted: monotonic issuance advances write authority.
    steps.wrap(
        step,
        store
            .save_session_snapshot(
                &main,
                session.id(),
                ContinuityGeneration::new(1),
                CheckpointVersion::new(2),
                FencingToken::new(2),
                &snapshot_v2,
            )
            .await,
    )?;

    // --- checkpoint version strictly increasing ----------------------------
    let step = "checkpoint_version_strictly_increasing";
    for stale_version in [2_u64, 1] {
        match store
            .save_session_snapshot(
                &main,
                session.id(),
                ContinuityGeneration::new(1),
                CheckpointVersion::new(stale_version),
                FencingToken::new(2),
                &snapshot_v2,
            )
            .await
        {
            Err(ContinuityStoreError::StaleCheckpointVersion { .. }) => {}
            Err(other) => {
                return Err(steps.fail(
                    step,
                    format!(
                        "version {stale_version} <= current must fail with \
                         StaleCheckpointVersion, got: {other}"
                    ),
                ));
            }
            Ok(()) => {
                return Err(steps.fail(
                    step,
                    format!(
                        "version {stale_version} <= current must be rejected: the version \
                         stream is strictly increasing per (identity, generation)"
                    ),
                ));
            }
        }
    }

    // --- snapshot bytes round trip -----------------------------------------
    let step = "snapshot_bytes_round_trip";
    let loaded = steps
        .wrap(step, store.load_session_snapshot(session.id()).await)?
        .ok_or_else(|| steps.fail(step, "saved snapshot must load back"))?;
    steps.ensure(
        step,
        loaded.data == snapshot_v2.data,
        "the latest committed snapshot bytes must round-trip unchanged",
    )?;

    // --- same-generation rebind preserves the version stream ---------------
    let step = "same_generation_rebind_preserves_version";
    let rebound_session = fixtures::session_with_texts(&["rebound session"])?;
    let rebind = ContinuityRecord {
        identity: main.clone(),
        agent_runtime_id: runtime.clone(),
        session_id: rebound_session.id().clone(),
        generation: ContinuityGeneration::new(1),
        checkpoint_version: CheckpointVersion::new(0),
    };
    steps.wrap(
        step,
        store
            .upsert_continuity_record(&rebind, FencingToken::new(3))
            .await,
    )?;
    let resolved = steps.wrap(step, store.resolve_many(std::slice::from_ref(&main)).await)?;
    match resolved.get(&main) {
        Some(ContinuityResolveState::Ready { record: current }) => {
            steps.ensure(
                step,
                current.session_id == *rebound_session.id(),
                "the rebind must take the new session id",
            )?;
            steps.ensure(
                step,
                current.checkpoint_version.get() == 2,
                format!(
                    "a same-generation rebind must not rewind checkpoint_version (expected 2, \
                     got {})",
                    current.checkpoint_version
                ),
            )?;
        }
        other => {
            return Err(steps.fail(step, format!("expected Ready after rebind, got {other:?}")));
        }
    }

    // --- generation-monotonic upsert ----------------------------------------
    let step = "generation_monotonic_upsert";
    let old_generation = ContinuityRecord {
        identity: main.clone(),
        agent_runtime_id: runtime.clone(),
        session_id: rebound_session.id().clone(),
        generation: ContinuityGeneration::new(0),
        checkpoint_version: CheckpointVersion::new(0),
    };
    match store
        .upsert_continuity_record(&old_generation, FencingToken::new(4))
        .await
    {
        Err(ContinuityStoreError::StaleContinuityGeneration { .. }) => {}
        Err(other) => {
            return Err(steps.fail(
                step,
                format!(
                    "an older generation must fail with StaleContinuityGeneration even under a \
                     newer fence, got: {other}"
                ),
            ));
        }
        Ok(()) => {
            return Err(steps.fail(
                step,
                "an older generation must be rejected even under a newer fence",
            ));
        }
    }

    // --- generation advance resets the version stream -----------------------
    let step = "generation_advance_resets_version_stream";
    let advanced_session = fixtures::session_with_texts(&["generation two session"])?;
    let advanced = ContinuityRecord {
        identity: main.clone(),
        agent_runtime_id: runtime.clone(),
        session_id: advanced_session.id().clone(),
        generation: ContinuityGeneration::new(2),
        checkpoint_version: CheckpointVersion::new(0),
    };
    steps.wrap(
        step,
        store
            .upsert_continuity_record(&advanced, FencingToken::new(4))
            .await,
    )?;
    let advanced_snapshot = fixtures::session_snapshot(&advanced_session)?;
    // Version 1 was already consumed in generation 1; the new generation's
    // stream restarts, so version 1 must be admitted again.
    steps.wrap(
        step,
        store
            .save_session_snapshot(
                &main,
                advanced_session.id(),
                ContinuityGeneration::new(2),
                CheckpointVersion::new(1),
                FencingToken::new(4),
                &advanced_snapshot,
            )
            .await,
    )?;

    // --- delete_session_snapshot_if_current_revision honesty ----------------
    let step = "delete_snapshot_if_current_revision";
    let stale_token = "conformance-stale-revision-token";
    steps.ensure(
        step,
        !steps.wrap(
            step,
            store
                .delete_session_snapshot_if_current_revision(advanced_session.id(), stale_token)
                .await,
        )?,
        "a stale revision token must never delete the snapshot",
    )?;
    steps.ensure(
        step,
        steps
            .wrap(
                step,
                store.load_session_snapshot(advanced_session.id()).await,
            )?
            .is_some(),
        "a stale-revision delete must leave the snapshot in place",
    )?;
    let current_token = steps.wrap(step, session_projection_cas_token(&advanced_session))?;
    let deleted = steps.wrap(
        step,
        store
            .delete_session_snapshot_if_current_revision(advanced_session.id(), &current_token)
            .await,
    )?;
    let remaining = steps.wrap(
        step,
        store.load_session_snapshot(advanced_session.id()).await,
    )?;
    if deleted {
        steps.ensure(
            step,
            remaining.is_none(),
            "a current-revision delete reporting true must have removed the snapshot",
        )?;
    } else {
        // Declared-unsupported per the trait: `Ok(false)` instead of a
        // successful no-op. Honesty demands the row is untouched.
        steps.ensure(
            step,
            remaining.is_some(),
            "a store declining session-scoped snapshot deletion (Ok(false)) must leave the row \
             untouched — never report a successful no-op",
        )?;
    }

    // --- delete_continuity_record -------------------------------------------
    let step = "delete_continuity_record";
    match store
        .delete_continuity_record(&main, FencingToken::new(3))
        .await
    {
        Err(ContinuityStoreError::StaleFencingToken { .. }) => {}
        Err(other) => {
            return Err(steps.fail(
                step,
                format!("a stale fence must fail the delete with StaleFencingToken, got: {other}"),
            ));
        }
        Ok(()) => {
            return Err(steps.fail(step, "a stale-fenced delete must be rejected"));
        }
    }
    steps.wrap(
        step,
        store
            .delete_continuity_record(&main, FencingToken::new(4))
            .await,
    )?;
    let resolved = steps.wrap(step, store.resolve_many(std::slice::from_ref(&main)).await)?;
    steps.ensure(
        step,
        matches!(
            resolved.get(&main),
            Some(ContinuityResolveState::Uninitialized)
        ),
        "after delete_continuity_record the identity must resolve Uninitialized",
    )?;
    steps.ensure(
        step,
        steps
            .wrap(step, store.load_session_snapshot(session.id()).await)?
            .is_none(),
        "deleting the continuity record must delete the identity's session snapshots",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Rollback
// ---------------------------------------------------------------------------

/// Which `rollback_continuity_record` implementation shape is under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackPath {
    /// The store overrides rollback with one atomic compare-and-swap
    /// transaction (`LocalContinuityStore`). Restoring the previous record
    /// works, the fence must match the current write authority exactly, and
    /// the previous generation's snapshots are retained.
    AtomicOverride,
    /// The store relies on the trait's non-atomic compatibility default.
    ///
    /// PINNED BEHAVIOR (fixed in M4b — the pre-fix default restored
    /// `previous` through the store's own generation-monotonic upsert and
    /// therefore always failed with `StaleContinuityGeneration` on a
    /// conforming store): the default now compensates via
    /// delete-then-reinsert, which a conforming store CAN satisfy. Restoring
    /// the previous record works, with the documented caveats: the path is
    /// non-atomic, and `delete_continuity_record` removes the identity's
    /// session snapshots — including the previous generation's rollback-
    /// authority snapshots, which the atomic override retains. This chapter
    /// pins both the working restore and that data caveat.
    CompatibilityDefault,
}

/// `rollback_continuity_record` chapter. See [`RollbackPath`] for the two
/// sanctioned implementation shapes and what each is pinned to.
pub async fn continuity_rollback(
    factory: &dyn ContinuityStoreFactory,
    path: RollbackPath,
) -> Result<(), ConformanceFailure> {
    let steps = Steps::chapter("continuity_rollback");
    let store = factory.open().await?;

    let main = identity(&steps, "setup", "rollback:main")?;
    let runtime = runtime_id(&steps, "setup", "rt-rollback")?;

    // Previous committed generation.
    let step = "setup";
    let previous_session = fixtures::session_with_texts(&["previous generation turn"])?;
    let previous = ContinuityRecord {
        identity: main.clone(),
        agent_runtime_id: runtime.clone(),
        session_id: previous_session.id().clone(),
        generation: ContinuityGeneration::new(1),
        checkpoint_version: CheckpointVersion::new(0),
    };
    steps.wrap(
        step,
        store
            .upsert_continuity_record(&previous, FencingToken::new(1))
            .await,
    )?;
    let previous_snapshot = fixtures::session_snapshot(&previous_session)?;
    steps.wrap(
        step,
        store
            .save_session_snapshot(
                &main,
                previous_session.id(),
                ContinuityGeneration::new(1),
                CheckpointVersion::new(1),
                FencingToken::new(1),
                &previous_snapshot,
            )
            .await,
    )?;
    let previous_head = ContinuityRecord {
        checkpoint_version: CheckpointVersion::new(1),
        ..previous.clone()
    };

    // Mismatch guard: a rollback whose expected attempt does not match the
    // durable row must be rejected before anything is touched.
    let step = "rollback_mismatch_rejected";
    let phantom_session = fixtures::session_with_texts(&["phantom attempt"])?;
    let phantom_attempt = ContinuityRecord {
        identity: main.clone(),
        agent_runtime_id: runtime.clone(),
        session_id: phantom_session.id().clone(),
        generation: ContinuityGeneration::new(2),
        checkpoint_version: CheckpointVersion::new(0),
    };
    match store
        .rollback_continuity_record(&phantom_attempt, Some(&previous_head), FencingToken::new(1))
        .await
    {
        Err(ContinuityStoreError::StaleContinuityGeneration { .. }) => {}
        Err(other) => {
            return Err(steps.fail(
                step,
                format!(
                    "a rollback whose expected attempt mismatches the durable row must fail \
                     with StaleContinuityGeneration, got: {other}"
                ),
            ));
        }
        Ok(()) => {
            return Err(steps.fail(step, "a mismatched rollback must be rejected"));
        }
    }

    // Publish the provisional reset attempt (generation 2) and one snapshot
    // under it.
    let step = "publish_provisional_attempt";
    let attempt_session = fixtures::session_with_texts(&["provisional generation turn"])?;
    let attempt = ContinuityRecord {
        identity: main.clone(),
        agent_runtime_id: runtime.clone(),
        session_id: attempt_session.id().clone(),
        generation: ContinuityGeneration::new(2),
        checkpoint_version: CheckpointVersion::new(0),
    };
    steps.wrap(
        step,
        store
            .upsert_continuity_record(&attempt, FencingToken::new(2))
            .await,
    )?;
    let attempt_snapshot = fixtures::session_snapshot(&attempt_session)?;
    steps.wrap(
        step,
        store
            .save_session_snapshot(
                &main,
                attempt_session.id(),
                ContinuityGeneration::new(2),
                CheckpointVersion::new(1),
                FencingToken::new(2),
                &attempt_snapshot,
            )
            .await,
    )?;
    let attempt_head = ContinuityRecord {
        checkpoint_version: CheckpointVersion::new(1),
        ..attempt.clone()
    };

    if path == RollbackPath::AtomicOverride {
        // The atomic override requires exact fence equality: a higher fence
        // is not the current write authority.
        let step = "rollback_fence_exact_equality";
        match store
            .rollback_continuity_record(&attempt_head, Some(&previous_head), FencingToken::new(3))
            .await
        {
            Err(ContinuityStoreError::StaleFencingToken { .. }) => {}
            Err(other) => {
                return Err(steps.fail(
                    step,
                    format!("the atomic rollback must require exact fence equality, got: {other}"),
                ));
            }
            Ok(()) => {
                return Err(steps.fail(
                    step,
                    "the atomic rollback must reject a fence that is not exactly the current \
                     write authority",
                ));
            }
        }
    }

    // Restore-previous.
    let step = "rollback_restores_previous";
    let restore = store
        .rollback_continuity_record(&attempt_head, Some(&previous_head), FencingToken::new(2))
        .await;
    match path {
        RollbackPath::AtomicOverride => {
            steps.wrap(step, restore)?;
            let resolved =
                steps.wrap(step, store.resolve_many(std::slice::from_ref(&main)).await)?;
            match resolved.get(&main) {
                Some(ContinuityResolveState::Ready { record: current }) => {
                    steps.ensure(
                        step,
                        current == &previous_head,
                        format!(
                            "rollback must restore the previous record exactly (expected \
                             {previous_head:?}, got {current:?})"
                        ),
                    )?;
                }
                other => {
                    return Err(steps.fail(
                        step,
                        format!("expected Ready(previous) after rollback, got {other:?}"),
                    ));
                }
            }
            steps.ensure(
                step,
                steps
                    .wrap(
                        step,
                        store.load_session_snapshot(attempt_session.id()).await,
                    )?
                    .is_none(),
                "rollback must delete the provisional generation's snapshots",
            )?;
            steps.ensure(
                step,
                steps
                    .wrap(
                        step,
                        store.load_session_snapshot(previous_session.id()).await,
                    )?
                    .is_some(),
                "rollback must retain the previous generation's snapshots",
            )?;
        }
        RollbackPath::CompatibilityDefault => {
            // PINNED BEHAVIOR (M4b fix): the compatibility default restores
            // the previous record via delete-then-reinsert — a conforming
            // generation-monotonic store admits the older generation as a
            // fresh insert after the fenced delete abandoned the attempt.
            steps.wrap(step, restore)?;
            let resolved =
                steps.wrap(step, store.resolve_many(std::slice::from_ref(&main)).await)?;
            match resolved.get(&main) {
                Some(ContinuityResolveState::Ready { record: current }) => {
                    steps.ensure(
                        step,
                        current == &previous_head,
                        format!(
                            "the compatibility rollback must restore the previous record \
                             exactly (expected {previous_head:?}, got {current:?})"
                        ),
                    )?;
                }
                other => {
                    return Err(steps.fail(
                        step,
                        format!("expected Ready(previous) after rollback, got {other:?}"),
                    ));
                }
            }
            steps.ensure(
                step,
                steps
                    .wrap(
                        step,
                        store.load_session_snapshot(attempt_session.id()).await,
                    )?
                    .is_none(),
                "rollback must delete the provisional generation's snapshots",
            )?;
            // PINNED DATA CAVEAT (documented on the trait): the delete verb
            // removes ALL of the identity's snapshots, so the previous
            // generation's rollback-authority snapshot does NOT survive the
            // compatibility path — stores wanting to retain it must override
            // with the atomic shape. Pinning this keeps the consequence
            // loud; if the default ever learns to retain prior-generation
            // snapshots, flip this expectation deliberately.
            steps.ensure(
                step,
                steps
                    .wrap(
                        step,
                        store.load_session_snapshot(previous_session.id()).await,
                    )?
                    .is_none(),
                "the compatibility rollback is documented to lose the previous generation's \
                 snapshots (delete_continuity_record removes them); a retained snapshot means \
                 the default grew atomic-override semantics — flip this pin deliberately",
            )?;
        }
    }

    // Delete path: rolling back a first-ever (no previous) attempt removes
    // the record and its snapshots on both implementation shapes.
    let step = "rollback_deletes_first_generation_attempt";
    let fresh = identity(&steps, step, "rollback:fresh")?;
    let fresh_runtime = runtime_id(&steps, step, "rt-rollback-fresh")?;
    let fresh_session = fixtures::session_with_texts(&["first ever attempt"])?;
    let fresh_attempt = ContinuityRecord {
        identity: fresh.clone(),
        agent_runtime_id: fresh_runtime,
        session_id: fresh_session.id().clone(),
        generation: ContinuityGeneration::new(1),
        checkpoint_version: CheckpointVersion::new(0),
    };
    steps.wrap(
        step,
        store
            .upsert_continuity_record(&fresh_attempt, FencingToken::new(1))
            .await,
    )?;
    let fresh_snapshot = fixtures::session_snapshot(&fresh_session)?;
    steps.wrap(
        step,
        store
            .save_session_snapshot(
                &fresh,
                fresh_session.id(),
                ContinuityGeneration::new(1),
                CheckpointVersion::new(1),
                FencingToken::new(1),
                &fresh_snapshot,
            )
            .await,
    )?;
    let fresh_head = ContinuityRecord {
        checkpoint_version: CheckpointVersion::new(1),
        ..fresh_attempt
    };
    steps.wrap(
        step,
        store
            .rollback_continuity_record(&fresh_head, None, FencingToken::new(1))
            .await,
    )?;
    let resolved = steps.wrap(step, store.resolve_many(std::slice::from_ref(&fresh)).await)?;
    steps.ensure(
        step,
        matches!(
            resolved.get(&fresh),
            Some(ContinuityResolveState::Uninitialized)
        ),
        "rolling back a first-ever attempt must leave the identity Uninitialized",
    )?;
    steps.ensure(
        step,
        steps
            .wrap(step, store.load_session_snapshot(fresh_session.id()).await)?
            .is_none(),
        "rolling back a first-ever attempt must delete its snapshots",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Bundled fencing-floor pair
// ---------------------------------------------------------------------------

/// Fencing-floor survival across reopen, scoped to the bundled
/// `LocalContinuityStore` + `LocalLeaseProvider` pair.
///
/// `max_fencing_token` is an inherent method on `LocalContinuityStore`
/// consumed via `open_with_fencing_floor` — it is NOT a `ContinuityStore`
/// trait obligation. External stores and lease providers own their fencing
/// floor internally, which is why this chapter takes a directory instead of
/// a [`ContinuityStoreFactory`].
pub async fn local_continuity_fencing_floor(dir: &Path) -> Result<(), ConformanceFailure> {
    let steps = Steps::chapter("local_continuity_fencing_floor");
    let db_path = dir.join("continuity-fencing-floor.sqlite");

    let step = "seed_high_water";
    let main = identity(&steps, step, "floor:main")?;
    let runtime = runtime_id(&steps, step, "rt-floor")?;
    let session = fixtures::session_with_texts(&["fencing floor turn"])?;
    {
        let store = steps.wrap(step, LocalContinuityStore::open(&db_path))?;
        let record = ContinuityRecord {
            identity: main.clone(),
            agent_runtime_id: runtime,
            session_id: session.id().clone(),
            generation: ContinuityGeneration::new(1),
            checkpoint_version: CheckpointVersion::new(0),
        };
        steps.wrap(
            step,
            store
                .upsert_continuity_record(&record, FencingToken::new(7))
                .await,
        )?;
        let snapshot = fixtures::session_snapshot(&session)?;
        steps.wrap(
            step,
            store
                .save_session_snapshot(
                    &main,
                    session.id(),
                    ContinuityGeneration::new(1),
                    CheckpointVersion::new(1),
                    FencingToken::new(9),
                    &snapshot,
                )
                .await,
        )?;
    }

    // Reopen: the persisted high-water mark must survive the restart.
    let step = "floor_survives_reopen";
    let (store, floor) = steps.wrap(
        step,
        LocalContinuityStore::open_with_fencing_floor(db_path.clone()).await,
    )?;
    steps.ensure(
        step,
        floor == 9,
        format!("reopen must report the persisted fencing high-water (expected 9, got {floor})"),
    )?;

    // A floorless provider re-arms the v0.7.8 restart abort: it mints token 1,
    // which the store's compare-and-set rejects as stale.
    let step = "floorless_provider_rearms_restart_abort";
    let floorless = LocalLeaseProvider::new();
    let stale_grant = acquire_grant(&steps, step, &floorless, &main).await?;
    steps.ensure(
        step,
        stale_grant.fencing_token.get() <= 9,
        "fixture error: the floorless provider must mint a token at or below the high-water",
    )?;
    let mut mutated = session;
    fixtures::push_text(&mut mutated, "post restart turn")?;
    let mutated_snapshot = fixtures::session_snapshot(&mutated)?;
    match store
        .save_session_snapshot(
            &main,
            mutated.id(),
            ContinuityGeneration::new(1),
            CheckpointVersion::new(2),
            stale_grant.fencing_token,
            &mutated_snapshot,
        )
        .await
    {
        Err(ContinuityStoreError::StaleFencingToken { .. }) => {}
        Err(other) => {
            return Err(steps.fail(
                step,
                format!("a floorless token must be rejected as stale, got: {other}"),
            ));
        }
        Ok(()) => {
            return Err(steps.fail(
                step,
                "a floorless token below the persisted high-water must be rejected",
            ));
        }
    }

    // The floor-seeded provider mints strictly above the persisted high-water
    // and its token passes the store's CAS.
    let step = "floor_seeded_token_passes_cas";
    let seeded = LocalLeaseProvider::with_floor(floor);
    let grant = acquire_grant(&steps, step, &seeded, &main).await?;
    steps.ensure(
        step,
        grant.fencing_token.get() > floor,
        format!(
            "a floor-seeded provider must mint strictly above the floor (floor {floor}, got {})",
            grant.fencing_token
        ),
    )?;
    steps.wrap(
        step,
        store
            .save_session_snapshot(
                &main,
                mutated.id(),
                ContinuityGeneration::new(1),
                CheckpointVersion::new(2),
                grant.fencing_token,
                &mutated_snapshot,
            )
            .await,
    )?;
    Ok(())
}

async fn acquire_grant(
    steps: &Steps,
    step: &'static str,
    provider: &LocalLeaseProvider,
    identity: &AgentIdentity,
) -> Result<meerkat_meta::identity_first::LeaseGrant, ConformanceFailure> {
    let mut results = steps.wrap(
        step,
        provider
            .acquire_leases(std::slice::from_ref(identity), "conformance-runner")
            .await,
    )?;
    match results.remove(identity) {
        Some(LeaseAcquireResult::Acquired(grant)) => Ok(grant),
        Some(LeaseAcquireResult::AlreadyHeld { holder, .. }) => Err(steps.fail(
            step,
            format!("fixture error: lease unexpectedly already held by {holder}"),
        )),
        None => Err(steps.fail(
            step,
            "lease provider must return an entry for every requested identity",
        )),
    }
}
