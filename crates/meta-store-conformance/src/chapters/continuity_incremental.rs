//! Incremental continuity chapter (M4b): the session-delta channel a
//! `ContinuityStore` may advertise through
//! `ContinuityStore::as_incremental_sessions`, exercised through the real
//! composition an identity-first gateway runs — substrate ->
//! `ContinuitySessionStoreAdapter` -> `as_incremental` wrapper.
//!
//! Two halves:
//!
//! 1. the UPSTREAM meerkat incremental profile
//!    (`meerkat_store_conformance::chapters::incremental`) run verbatim over
//!    the composed adapter, so a meta substrate cannot diverge from
//!    meerkat's own append / head-CAS / rewrite-adoption contract;
//! 2. the meta-specific pins the upstream profile cannot know about: the
//!    per-mutation continuity write discipline, the parked pre-registration
//!    layer, the frozen blob archive, and reset-rollback scoping.
//!
//! The upstream profile writes through unregistered session ids, so this
//! chapter wraps the adapter in an auto-registering harness: every fresh
//! session id is registered with a synthetic identity before its first
//! mutation, exactly as the identity runtime does through
//! `MobSessionBridge::register_session_runtime_state`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use meerkat_core::session_store::{
    IncrementalSessionStore, SessionHead, SessionHeadCas, TranscriptStrandId,
    session_head_cas_token,
};
use meerkat_core::{
    Message, Session, SessionFilter, SessionMeta, SessionStore, SessionStoreError,
    TranscriptRewriteCommit, TranscriptRewriteRecord, types::SessionId,
};
use meerkat_meta::identity_first::{
    AgentIdentity, AgentRuntimeId, CheckpointVersion, ContinuityGeneration, ContinuityRecord,
    ContinuitySessionStoreAdapter, ContinuityStore, FencingToken, SessionRuntimeState,
};
use meerkat_store_conformance::{ConformanceFailure, SessionStoreFactory};

use crate::factory::ContinuityStoreFactory;
use crate::fixtures;
use crate::steps::Steps;

const CHAPTER: &str = "continuity_incremental";

/// Auto-registering composition of substrate -> adapter.
///
/// Publishes a synthetic continuity cursor (`conformance:{n}` identity,
/// generation 1, fencing tokens from a counter seeded above the substrate's
/// floor) for every fresh session id on its first mutation, then delegates
/// every verb to the adapter.
struct RegisteringAdapter {
    store: Arc<dyn ContinuityStore>,
    adapter: Arc<ContinuitySessionStoreAdapter>,
    registered: Mutex<HashMap<String, SessionRuntimeState>>,
    next_identity: AtomicU64,
    fencing_token: FencingToken,
}

impl RegisteringAdapter {
    fn new(store: Arc<dyn ContinuityStore>, fencing_floor: u64) -> Arc<Self> {
        let adapter = Arc::new(ContinuitySessionStoreAdapter::new(Arc::clone(&store)));
        Arc::new(Self {
            store,
            adapter,
            registered: Mutex::new(HashMap::new()),
            next_identity: AtomicU64::new(0),
            fencing_token: FencingToken::new(fencing_floor + 1),
        })
    }

    fn already_registered(&self, id: &SessionId) -> bool {
        self.registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&id.to_string())
    }

    /// Publish a cursor for `id` if it does not have one yet. Mirrors the
    /// runtime's ordering: the continuity record is persisted first, then
    /// the session is registered with the adapter.
    async fn ensure_registered(&self, id: &SessionId) -> Result<(), SessionStoreError> {
        if self.already_registered(id) {
            return Ok(());
        }
        let ordinal = self.next_identity.fetch_add(1, Ordering::Relaxed);
        let identity =
            AgentIdentity::parse(&format!("conformance:{ordinal}")).map_err(|error| {
                SessionStoreError::Internal(format!("conformance identity: {error}"))
            })?;
        let runtime_id =
            AgentRuntimeId::parse(&format!("rt:conformance:{ordinal}")).map_err(|error| {
                SessionStoreError::Internal(format!("conformance runtime: {error}"))
            })?;
        let record = ContinuityRecord {
            identity: identity.clone(),
            agent_runtime_id: runtime_id,
            session_id: id.clone(),
            generation: ContinuityGeneration::new(1),
            checkpoint_version: CheckpointVersion::new(0),
        };
        self.store
            .upsert_continuity_record(&record, self.fencing_token)
            .await
            .map_err(|error| {
                SessionStoreError::Internal(format!("conformance continuity record: {error}"))
            })?;
        let state = SessionRuntimeState {
            identity,
            generation: ContinuityGeneration::new(1),
            fencing_token: self.fencing_token,
            checkpoint_version: CheckpointVersion::new(0),
        };
        self.adapter.register_session(id, state.clone()).await?;
        self.registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.to_string(), state);
        Ok(())
    }
}

#[async_trait]
impl SessionStore for RegisteringAdapter {
    async fn save(&self, session: &Session) -> Result<(), SessionStoreError> {
        self.ensure_registered(session.id()).await?;
        self.adapter.save(session).await
    }

    async fn save_transcript_rewrite(
        &self,
        session: &Session,
        commit: &TranscriptRewriteCommit,
    ) -> Result<(), SessionStoreError> {
        self.ensure_registered(session.id()).await?;
        self.adapter.save_transcript_rewrite(session, commit).await
    }

    async fn save_authoritative_projection(
        &self,
        session: &Session,
    ) -> Result<(), SessionStoreError> {
        self.ensure_registered(session.id()).await?;
        self.adapter.save_authoritative_projection(session).await
    }

    async fn save_authoritative_projection_if_current_revision(
        &self,
        session: &Session,
        expected_current_revision: Option<String>,
    ) -> Result<(), SessionStoreError> {
        self.ensure_registered(session.id()).await?;
        self.adapter
            .save_authoritative_projection_if_current_revision(session, expected_current_revision)
            .await
    }

    async fn load(&self, id: &SessionId) -> Result<Option<Session>, SessionStoreError> {
        self.adapter.load(id).await
    }

    async fn list(&self, filter: SessionFilter) -> Result<Vec<SessionMeta>, SessionStoreError> {
        self.adapter.list(filter).await
    }

    async fn delete(&self, id: &SessionId) -> Result<(), SessionStoreError> {
        self.adapter.delete(id).await
    }

    async fn delete_if_current_revision(
        &self,
        id: &SessionId,
        expected_current_revision: &str,
    ) -> Result<bool, SessionStoreError> {
        self.adapter
            .delete_if_current_revision(id, expected_current_revision)
            .await
    }

    fn as_incremental(self: Arc<Self>) -> Option<Arc<dyn IncrementalSessionStore>> {
        let inner = Arc::clone(&self.adapter).as_incremental()?;
        Some(Arc::new(RegisteringIncremental {
            harness: self,
            inner,
        }))
    }
}

struct RegisteringIncremental {
    harness: Arc<RegisteringAdapter>,
    inner: Arc<dyn IncrementalSessionStore>,
}

#[async_trait]
impl SessionStore for RegisteringIncremental {
    async fn save(&self, session: &Session) -> Result<(), SessionStoreError> {
        self.harness.save(session).await
    }

    async fn save_transcript_rewrite(
        &self,
        session: &Session,
        commit: &TranscriptRewriteCommit,
    ) -> Result<(), SessionStoreError> {
        self.harness.save_transcript_rewrite(session, commit).await
    }

    async fn save_authoritative_projection(
        &self,
        session: &Session,
    ) -> Result<(), SessionStoreError> {
        self.harness.save_authoritative_projection(session).await
    }

    async fn save_authoritative_projection_if_current_revision(
        &self,
        session: &Session,
        expected_current_revision: Option<String>,
    ) -> Result<(), SessionStoreError> {
        self.harness
            .save_authoritative_projection_if_current_revision(session, expected_current_revision)
            .await
    }

    async fn load(&self, id: &SessionId) -> Result<Option<Session>, SessionStoreError> {
        self.harness.load(id).await
    }

    async fn list(&self, filter: SessionFilter) -> Result<Vec<SessionMeta>, SessionStoreError> {
        self.harness.list(filter).await
    }

    async fn delete(&self, id: &SessionId) -> Result<(), SessionStoreError> {
        self.harness.delete(id).await
    }

    async fn delete_if_current_revision(
        &self,
        id: &SessionId,
        expected_current_revision: &str,
    ) -> Result<bool, SessionStoreError> {
        self.harness
            .delete_if_current_revision(id, expected_current_revision)
            .await
    }

    fn as_incremental(self: Arc<Self>) -> Option<Arc<dyn IncrementalSessionStore>> {
        Some(self)
    }
}

#[async_trait]
impl IncrementalSessionStore for RegisteringIncremental {
    // meerkat 0.8.22 made store activation a REQUIRED, no-default seam: the
    // backend must enumerate its physical session identities, cross and
    // verify each, and return the complete result from one write transaction,
    // never deriving the census from `SessionStore::list`. This conformance
    // wrapper owns no physical format authority - it only registers
    // observations - so activation delegates verbatim to the wrapped store,
    // exactly as the crossing below does. Registering identities here would
    // fabricate a census this wrapper did not perform.
    async fn activate_head_canonical_store(
        &self,
    ) -> Result<meerkat_core::session_store::HeadCanonicalStoreActivation, SessionStoreError> {
        self.inner.activate_head_canonical_store().await
    }

    // meerkat 0.8.21 format-door crossing: the conformance wrapper only
    // registers observations; physical format authority belongs to the
    // wrapped store, so the crossing delegates verbatim.
    async fn cross_head_canonical_authority(
        &self,
        id: &SessionId,
    ) -> Result<meerkat_core::session_store::HeadCanonicalAuthorityCrossing, SessionStoreError>
    {
        self.inner.cross_head_canonical_authority(id).await
    }

    async fn append_messages(
        &self,
        id: &SessionId,
        strand: &TranscriptStrandId,
        base_seq: u64,
        messages: &[Message],
    ) -> Result<(), SessionStoreError> {
        self.harness.ensure_registered(id).await?;
        self.inner
            .append_messages(id, strand, base_seq, messages)
            .await
    }

    async fn commit_rewrite(
        &self,
        id: &SessionId,
        record: &TranscriptRewriteRecord,
        expected: SessionHeadCas,
    ) -> Result<SessionHead, SessionStoreError> {
        self.harness.ensure_registered(id).await?;
        self.inner.commit_rewrite(id, record, expected).await
    }

    async fn save_head(
        &self,
        head: &SessionHead,
        expected: SessionHeadCas,
    ) -> Result<(), SessionStoreError> {
        self.harness.ensure_registered(&head.id).await?;
        self.inner.save_head(head, expected).await
    }

    async fn load_head(&self, id: &SessionId) -> Result<Option<SessionHead>, SessionStoreError> {
        self.inner.load_head(id).await
    }

    async fn load_messages(
        &self,
        id: &SessionId,
        strand: &TranscriptStrandId,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<Message>, SessionStoreError> {
        self.inner.load_messages(id, strand, range).await
    }

    async fn load_rewrites(
        &self,
        id: &SessionId,
    ) -> Result<Vec<TranscriptRewriteRecord>, SessionStoreError> {
        self.inner.load_rewrites(id).await
    }
}

/// Factory over one composed harness. The composed adapter is a process-local
/// registry over a durable substrate, so the sanctioned shared-handle model
/// applies: every `open` returns the same composition.
struct HarnessFactory {
    harness: Arc<RegisteringAdapter>,
}

#[async_trait]
impl SessionStoreFactory for HarnessFactory {
    async fn open(&self) -> Result<Arc<dyn SessionStore>, ConformanceFailure> {
        Ok(Arc::clone(&self.harness) as Arc<dyn SessionStore>)
    }
}

/// Incremental continuity profile. Invoke only for substrates whose
/// `as_incremental_sessions` returns `Some`; invoking it for a substrate
/// without the channel fails loudly at the capability probe.
///
/// `fencing_floor` must be at or above the substrate's `max_fencing_token()`
/// so the harness's synthetic cursor is current write authority.
pub async fn continuity_incremental(
    factory: &dyn ContinuityStoreFactory,
    fencing_floor: u64,
) -> Result<(), ConformanceFailure> {
    let steps = Steps::chapter(CHAPTER);
    let store = factory.open().await?;

    let step = "substrate_advertises_delta_channel";
    steps.ensure(
        step,
        store.as_incremental_sessions().is_some(),
        "the incremental continuity profile was invoked for a substrate whose \
         as_incremental_sessions() returned None",
    )?;

    // --- the upstream meerkat incremental profile, verbatim ------------------
    let harness = HarnessFactory {
        harness: RegisteringAdapter::new(Arc::clone(&store), fencing_floor),
    };
    meerkat_store_conformance::chapters::incremental(&harness).await?;

    // --- meta pins ---------------------------------------------------------
    parked_pre_registration_writes(&steps, &store, fencing_floor).await?;
    parked_flush_failure_retains_the_parked_write(&steps, &store, fencing_floor).await?;
    per_mutation_continuity_discipline(&steps, &store, fencing_floor).await?;
    archive_untouched_by_head_canonical_writes(&steps, &store, fencing_floor).await?;
    Ok(())
}

fn identity(
    steps: &Steps,
    step: &'static str,
    raw: &str,
) -> Result<AgentIdentity, ConformanceFailure> {
    AgentIdentity::parse(raw).map_err(|error| steps.fail(step, error.to_string()))
}

async fn seed_record(
    steps: &Steps,
    step: &'static str,
    store: &Arc<dyn ContinuityStore>,
    identity: &AgentIdentity,
    session_id: &SessionId,
    token: FencingToken,
) -> Result<(), ConformanceFailure> {
    let runtime_id = AgentRuntimeId::parse(&format!("rt:{}", identity.as_str()))
        .map_err(|error| steps.fail(step, error.to_string()))?;
    steps.wrap(
        step,
        store
            .upsert_continuity_record(
                &ContinuityRecord {
                    identity: identity.clone(),
                    agent_runtime_id: runtime_id,
                    session_id: session_id.clone(),
                    generation: ContinuityGeneration::new(1),
                    checkpoint_version: CheckpointVersion::new(0),
                },
                token,
            )
            .await,
    )?;
    Ok(())
}

/// The ship-blocking pin: with the capability advertised, the session service
/// routes EVERY save through the incremental branch, and creation-time saves
/// provably arrive before the owning identity is published. Those writes must
/// park (nothing durable), read back through the parked view, and flush on
/// registration.
async fn parked_pre_registration_writes(
    steps: &Steps,
    store: &Arc<dyn ContinuityStore>,
    fencing_floor: u64,
) -> Result<(), ConformanceFailure> {
    const STEP: &str = "unregistered_delta_writes_park_and_flush_on_register";
    let adapter = Arc::new(ContinuitySessionStoreAdapter::new(Arc::clone(store)));
    let incremental = Arc::clone(&adapter)
        .as_incremental()
        .ok_or_else(|| steps.fail(STEP, "the composed adapter must forward the delta channel"))?;

    let session = fixtures::session_with_texts(&["creation-window turn"])?;
    let root = TranscriptStrandId::root();
    steps.wrap(
        STEP,
        incremental
            .append_messages(session.id(), &root, 0, session.messages())
            .await,
    )?;
    let head = steps.wrap(STEP, SessionHead::from_session(&session, root.clone(), 0))?;
    steps.wrap(
        STEP,
        incremental.save_head(&head, SessionHeadCas::Create).await,
    )?;

    steps.ensure(
        STEP,
        steps
            .wrap(STEP, store.load_session_snapshot(session.id()).await)?
            .is_none(),
        "a pre-registration delta write must write NOTHING to the continuity store",
    )?;
    let parked_head = steps
        .wrap(STEP, incremental.load_head(session.id()).await)?
        .ok_or_else(|| {
            steps.fail(
                STEP,
                "the parked view must serve load_head so the service's continuity preflight and \
                 head CAS view stay coherent",
            )
        })?;
    steps.ensure(
        STEP,
        parked_head.head_revision == head.head_revision,
        "the parked head must be the head that was just written",
    )?;
    steps.ensure(
        STEP,
        steps
            .wrap(
                STEP,
                incremental
                    .load_messages(session.id(), &root, 0..head.message_count)
                    .await,
            )?
            .len() as u64
            == head.message_count,
        "the parked view must serve the rows the preflight reads back",
    )?;

    // Registration flushes the parked state under the real cursor.
    let identity = identity(steps, STEP, "conformance:parked")?;
    let token = FencingToken::new(fencing_floor + 2);
    seed_record(steps, STEP, store, &identity, session.id(), token).await?;
    steps.wrap(
        STEP,
        adapter
            .register_session(
                session.id(),
                SessionRuntimeState {
                    identity,
                    generation: ContinuityGeneration::new(1),
                    fencing_token: token,
                    checkpoint_version: CheckpointVersion::new(0),
                },
            )
            .await,
    )?;
    let durable = steps
        .wrap(STEP, store.load_session_snapshot(session.id()).await)?
        .ok_or_else(|| {
            steps.fail(
                STEP,
                "registration must flush the parked delta state into the durable channel",
            )
        })?;
    let flushed: Session = serde_json::from_slice(&durable.data)
        .map_err(|error| steps.fail(STEP, format!("flushed document does not decode: {error}")))?;
    steps.ensure(
        STEP,
        flushed.messages() == session.messages(),
        "the flushed document must be the parked document",
    )?;
    steps.ensure(
        STEP,
        steps
            .wrap(STEP, adapter.load(session.id()).await)?
            .is_some(),
        "the flushed session must load back through the adapter",
    )?;
    Ok(())
}

/// The other half of the parking contract: what happens when the flush
/// FAILS. A registration that presents a stale lease is refused by the
/// substrate's own write discipline mid-flush — the realistic production
/// shape, no fault injection needed. The registration must fail typed, the
/// parked write must survive (nothing durable, parked view unchanged), and
/// a re-registration under current authority must adopt it. A store that
/// drops the parked write here loses a member's creation-window turn.
async fn parked_flush_failure_retains_the_parked_write(
    steps: &Steps,
    store: &Arc<dyn ContinuityStore>,
    fencing_floor: u64,
) -> Result<(), ConformanceFailure> {
    const STEP: &str = "failed_parked_flush_retains_the_write_and_refuses_the_registration";
    let adapter = Arc::new(ContinuitySessionStoreAdapter::new(Arc::clone(store)));
    let incremental = Arc::clone(&adapter)
        .as_incremental()
        .ok_or_else(|| steps.fail(STEP, "the composed adapter must forward the delta channel"))?;

    let session = fixtures::session_with_texts(&["turn parked before a stale registration"])?;
    let root = TranscriptStrandId::root();
    steps.wrap(
        STEP,
        incremental
            .append_messages(session.id(), &root, 0, session.messages())
            .await,
    )?;
    let head = steps.wrap(STEP, SessionHead::from_session(&session, root.clone(), 0))?;
    steps.wrap(
        STEP,
        incremental.save_head(&head, SessionHeadCas::Create).await,
    )?;

    // Current authority sits at `current`; the registration presents the
    // token below it, so the flush's first durable mutation is refused.
    let identity = identity(steps, STEP, "conformance:flushfail")?;
    let current = FencingToken::new(fencing_floor + 30);
    seed_record(steps, STEP, store, &identity, session.id(), current).await?;
    let refused = adapter
        .register_session(
            session.id(),
            SessionRuntimeState {
                identity: identity.clone(),
                generation: ContinuityGeneration::new(1),
                fencing_token: FencingToken::new(fencing_floor + 29),
                checkpoint_version: CheckpointVersion::new(0),
            },
        )
        .await;
    steps.ensure(
        STEP,
        refused.is_err(),
        "a parked flush the store refuses must refuse the registration too, never report success",
    )?;

    steps.ensure(
        STEP,
        steps
            .wrap(STEP, store.load_session_snapshot(session.id()).await)?
            .is_none(),
        "a failed parked flush must leave nothing durable",
    )?;
    let retained = steps
        .wrap(STEP, incremental.load_head(session.id()).await)?
        .ok_or_else(|| {
            steps.fail(
                STEP,
                "a failed parked flush must RETAIN the parked state — dropping it here loses the \
                 creation-window write the parking layer exists to protect",
            )
        })?;
    steps.ensure(
        STEP,
        retained.head_revision == head.head_revision,
        "the retained parked head must be the head that was parked",
    )?;
    steps.ensure(
        STEP,
        steps
            .wrap(
                STEP,
                incremental
                    .load_messages(session.id(), &root, 0..head.message_count)
                    .await,
            )?
            .len() as u64
            == head.message_count,
        "the retained parked rows must survive the failed flush intact",
    )?;

    // Re-registration under CURRENT authority adopts the retained write.
    steps.wrap(
        STEP,
        adapter
            .register_session(
                session.id(),
                SessionRuntimeState {
                    identity,
                    generation: ContinuityGeneration::new(1),
                    fencing_token: current,
                    checkpoint_version: CheckpointVersion::new(0),
                },
            )
            .await,
    )?;
    let flushed = steps
        .wrap(STEP, store.load_session_snapshot(session.id()).await)?
        .ok_or_else(|| {
            steps.fail(
                STEP,
                "the retry under current authority must flush the retained parked state",
            )
        })?;
    let flushed: Session = serde_json::from_slice(&flushed.data)
        .map_err(|error| steps.fail(STEP, format!("flushed document does not decode: {error}")))?;
    steps.ensure(
        STEP,
        flushed.messages() == session.messages(),
        "the retry must flush the ORIGINAL parked document, byte-complete",
    )?;
    Ok(())
}

/// Every delta mutation carries the continuity write discipline: a stale
/// fencing token is refused durably, and an accepted mutation advances the
/// identity's checkpoint version atomically with its rows.
async fn per_mutation_continuity_discipline(
    steps: &Steps,
    store: &Arc<dyn ContinuityStore>,
    fencing_floor: u64,
) -> Result<(), ConformanceFailure> {
    const STEP: &str = "per_mutation_fence_and_version_discipline";
    let adapter = Arc::new(ContinuitySessionStoreAdapter::new(Arc::clone(store)));
    let incremental = Arc::clone(&adapter)
        .as_incremental()
        .ok_or_else(|| steps.fail(STEP, "the composed adapter must forward the delta channel"))?;

    let session = fixtures::session_with_texts(&["fenced turn"])?;
    let root = TranscriptStrandId::root();
    let identity = identity(steps, STEP, "conformance:fenced")?;
    let current = FencingToken::new(fencing_floor + 10);
    seed_record(steps, STEP, store, &identity, session.id(), current).await?;

    // A registered-but-stale lease token must be refused by the STORE, not
    // merely by the adapter's in-memory registry.
    steps.wrap(
        STEP,
        adapter
            .register_session(
                session.id(),
                SessionRuntimeState {
                    identity: identity.clone(),
                    generation: ContinuityGeneration::new(1),
                    fencing_token: FencingToken::new(fencing_floor + 1),
                    checkpoint_version: CheckpointVersion::new(0),
                },
            )
            .await,
    )?;
    let stale = incremental
        .append_messages(session.id(), &root, 0, session.messages())
        .await;
    steps.ensure(
        STEP,
        stale.is_err(),
        "a stale fencing token must be refused per append, not just per whole-blob save",
    )?;
    steps.ensure(
        STEP,
        steps
            .wrap(STEP, incremental.load_head(session.id()).await)?
            .is_none(),
        "a refused delta write must leave nothing durable",
    )?;

    // Current authority: accepted, and the durable cursor advances with it.
    let adapter = Arc::new(ContinuitySessionStoreAdapter::new(Arc::clone(store)));
    let incremental = Arc::clone(&adapter)
        .as_incremental()
        .ok_or_else(|| steps.fail(STEP, "the composed adapter must forward the delta channel"))?;
    steps.wrap(
        STEP,
        adapter
            .register_session(
                session.id(),
                SessionRuntimeState {
                    identity: identity.clone(),
                    generation: ContinuityGeneration::new(1),
                    fencing_token: current,
                    checkpoint_version: CheckpointVersion::new(0),
                },
            )
            .await,
    )?;
    steps.wrap(
        STEP,
        incremental
            .append_messages(session.id(), &root, 0, session.messages())
            .await,
    )?;
    let resolved = steps.wrap(
        STEP,
        store.resolve_many(std::slice::from_ref(&identity)).await,
    )?;
    let advanced = match resolved.get(&identity) {
        Some(meerkat_meta::identity_first::ContinuityResolveState::Ready { record }) => {
            record.checkpoint_version
        }
        other => {
            return Err(steps.fail(
                STEP,
                format!("the identity must stay resolvable after a delta write: {other:?}"),
            ));
        }
    };
    steps.ensure(
        STEP,
        advanced.get() > 0,
        "an accepted delta mutation must advance the identity's durable checkpoint version",
    )?;
    Ok(())
}

/// Two-authorities tripwire: once a session is head-canonical, a WHOLE
/// document save must convert into delta rows + a head and leave the frozen
/// blob archive byte-identical.
async fn archive_untouched_by_head_canonical_writes(
    steps: &Steps,
    store: &Arc<dyn ContinuityStore>,
    fencing_floor: u64,
) -> Result<(), ConformanceFailure> {
    const STEP: &str = "head_canonical_write_leaves_the_blob_archive_untouched";
    let adapter = Arc::new(ContinuitySessionStoreAdapter::new(Arc::clone(store)));
    let incremental = Arc::clone(&adapter)
        .as_incremental()
        .ok_or_else(|| steps.fail(STEP, "the composed adapter must forward the delta channel"))?;

    let session = fixtures::session_with_texts(&["archived turn"])?;
    let identity = identity(steps, STEP, "conformance:archived")?;
    let token = FencingToken::new(fencing_floor + 20);
    seed_record(steps, STEP, store, &identity, session.id(), token).await?;
    let snapshot = fixtures::session_snapshot(&session)?;
    let archived_bytes = snapshot.data.clone();
    steps.wrap(
        STEP,
        store
            .save_session_snapshot(
                &identity,
                session.id(),
                ContinuityGeneration::new(1),
                CheckpointVersion::new(1),
                token,
                &snapshot,
            )
            .await,
    )?;

    steps.wrap(
        STEP,
        adapter
            .register_session(
                session.id(),
                SessionRuntimeState {
                    identity,
                    generation: ContinuityGeneration::new(1),
                    fencing_token: token,
                    checkpoint_version: CheckpointVersion::new(1),
                },
            )
            .await,
    )?;

    // Migrate to head-canonical through the delta channel (the first delta
    // write on a blob-only session), then adopt.
    let head = steps
        .wrap(STEP, incremental.load_head(session.id()).await)?
        .ok_or_else(|| steps.fail(STEP, "a blob-only session must synthesize a read-only head"))?;
    let synthesized_token = steps.wrap(STEP, session_head_cas_token(&head))?;
    steps.wrap(
        STEP,
        incremental
            .save_head(&head, SessionHeadCas::IfToken(synthesized_token))
            .await,
    )?;

    // A whole-document save now converts instead of rewriting the blob.
    let mut extended = session.clone();
    fixtures::push_text(&mut extended, "appended after migration")?;
    steps.wrap(STEP, adapter.save(&extended).await)?;

    let served = steps
        .wrap(STEP, adapter.load(session.id()).await)?
        .ok_or_else(|| steps.fail(STEP, "the converted session must load"))?;
    steps.ensure(
        STEP,
        served.messages() == extended.messages(),
        "the converted session must serve the saved document",
    )?;

    // Byte authority uniqueness, expressed in trait terms: after conversion
    // the whole-snapshot read verb must serve the head-canonical document,
    // never the pre-migration archive bytes. (The archive row itself is
    // deliberately invisible through this trait; that it stays BYTE-identical
    // is pinned by the substrate's own in-crate test, which can see the
    // row.)
    let read_back = steps
        .wrap(STEP, store.load_session_snapshot(session.id()).await)?
        .ok_or_else(|| steps.fail(STEP, "the converted session must serve a snapshot"))?;
    steps.ensure(
        STEP,
        read_back.data != archived_bytes,
        "after conversion the snapshot read must serve head+rows, not the frozen archive",
    )?;
    let read_back_document: Session = serde_json::from_slice(&read_back.data)
        .map_err(|error| steps.fail(STEP, format!("served snapshot does not decode: {error}")))?;
    steps.ensure(
        STEP,
        read_back_document.messages() == extended.messages(),
        "the served snapshot must be byte-consistent with the document the adapter loads",
    )?;
    let cas_token = steps.wrap(
        STEP,
        meerkat_core::session_store::session_projection_cas_token(&read_back_document),
    )?;
    steps.ensure(
        STEP,
        !steps.wrap(
            STEP,
            store
                .delete_session_snapshot_if_current_revision(session.id(), "row-sha256:stale")
                .await,
        )?,
        "a stale CAS token must decline over head-canonical rows",
    )?;
    steps.ensure(
        STEP,
        steps.wrap(
            STEP,
            store
                .delete_session_snapshot_if_current_revision(session.id(), &cas_token)
                .await,
        )?,
        "the CAS token derived from the served head+rows document must be accepted",
    )?;
    Ok(())
}
