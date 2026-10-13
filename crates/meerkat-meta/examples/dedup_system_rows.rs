//! Operator surgery helper (a session whose context filled with replayed prompts):
//! drop byte-identical duplicate System rows from a live mob-hosted
//! session's durable transcript, keeping the first copy, through the
//! typed rewrite door - one full-range rewrite commit composed onto the
//! retained graph, never a hand edit of store rows.
//!
//! Procedure (single-writer discipline):
//!   1. STOP the gateway.
//!   2. Back up continuity.db (byte copy).
//!   3. Dry run:  cargo run --example dedup_system_rows -- \
//!      --db /path/to/continuity.db --session <uuid>
//!   4. Apply:    ... --apply
//!   5. Remove the runtime scratch store (runtime.db) so the next boot
//!      mints runtime authority from the healed durable row (the
//!      reset-reseed lane).
//!   6. Start the gateway.

use std::sync::Arc;

use meerkat_meta::identity_first::{
    ContinuitySessionStoreAdapter, ContinuityStore, LocalContinuityStore, SessionRuntimeState,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db: Option<String> = None;
    let mut session: Option<String> = None;
    let mut apply = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => db = args.next(),
            "--session" => session = args.next(),
            "--apply" => apply = true,
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    let db = db.ok_or("--db <continuity.db> is required")?;
    let session_id =
        meerkat_core::types::SessionId::parse(&session.ok_or("--session <uuid> is required")?)
            .map_err(|e| format!("--session must be a session UUID: {e}"))?;

    let continuity: Arc<dyn ContinuityStore> = Arc::new(LocalContinuityStore::open(&db)?);
    let adapter = Arc::new(ContinuitySessionStoreAdapter::new(Arc::clone(&continuity)));

    // Write authority = the durable continuity record, exactly the facts a
    // registered resume would carry (the parked-repair contract).
    let (record, fencing_token, fence_current) = continuity
        .resolve_record_by_session(&session_id)
        .await?
        .ok_or("no continuity record binds this session")?;
    adapter
        .register_session(
            &session_id,
            SessionRuntimeState {
                identity: record.identity.clone(),
                generation: record.generation,
                fencing_token,
                checkpoint_version: fence_current,
            },
        )
        .await?;

    // Load the durable session (slim: content only), then HYDRATE the
    // out-of-line rewrite graph from the store's own rewrite rows onto it
    // (meerkat lead's prescription; the WholeBlob snapshot proved one
    // generation BEHIND the durable chain in the field, so it is not a
    // valid source). Slim materializations drop the compact graph by
    // design - a rewrite committed on one composes at generation 1 and the
    // store refuses it against the retained chain. With the store-proved
    // graph installed the dedup rewrite composes at the correct next
    // generation; content stays the durable truth, and the install
    // validates that the live transcript extends the audited head.
    let mut session = meerkat::SessionStore::load(adapter.as_ref(), &session_id)
        .await?
        .ok_or("no durable row for this session")?;
    let channel = continuity
        .as_incremental_sessions()
        .ok_or("the continuity store provides no incremental channel")?;
    let rewrite_records = channel.load_rewrites(&session_id).await?;
    if let Some(validated) =
        meerkat_core::ValidatedTranscriptHistory::from_rewrite_records_with_proved(
            rewrite_records,
            None,
        )?
    {
        session.install_validated_audited_transcript_history_preserving_live(validated)?;
    }
    let messages = session.messages();
    println!(
        "session {session_id}: {} messages, hydrated rewrite generation {}, identity {}",
        messages.len(),
        session.transcript_rewrite_generation()?,
        record.identity
    );

    // CONTENT-identical System duplicates of the FIRST System row. Each
    // materialized copy carries its own created_at in the message envelope
    // (verified in the field: 12 differing bytes, all inside the
    // timestamp), so the comparison is on the system CONTENT - the
    // configured-prompt bytes themselves - never the envelope.
    let serialized: Vec<Vec<u8>> = messages
        .iter()
        .map(serde_json::to_vec)
        .collect::<Result<_, _>>()?;
    let values: Vec<serde_json::Value> = serialized
        .iter()
        .map(|bytes| serde_json::from_slice(bytes))
        .collect::<Result<_, _>>()?;
    let is_system = |value: &serde_json::Value| -> bool {
        value
            .get("role")
            .and_then(|r| r.as_str())
            .map(|r| r == "system")
            .unwrap_or(false)
    };
    // GROUP System rows by (content, identity), ignoring only the envelope
    // timestamp (amended for multi-group heads): the transcript can carry
    // SEVERAL distinct replayed prompts (for example one singleton plus two
    // large replay groups).
    // Keep the FIRST occurrence of each distinct group; drop the rest.
    let mut seen_groups: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut duplicate_indices: Vec<usize> = Vec::new();
    for (index, value) in values.iter().enumerate() {
        if !is_system(value) {
            continue;
        }
        let key = serde_json::to_string(&serde_json::json!({
            "content": value.get("content"),
            "identity": value.get("identity"),
        }))?;
        match seen_groups.entry(key) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(index);
            }
            std::collections::hash_map::Entry::Occupied(_) => duplicate_indices.push(index),
        }
    }
    if seen_groups.is_empty() {
        println!("no System rows at all; nothing to do");
        return Ok(());
    }
    let mut kept: Vec<usize> = seen_groups.values().copied().collect();
    kept.sort_unstable();
    println!(
        "{} System rows in {} distinct (content, identity) groups; keeping first \
         occurrences at indices {:?}; dropping {} replay copies",
        seen_groups.len() + duplicate_indices.len(),
        seen_groups.len(),
        kept,
        duplicate_indices.len()
    );
    if duplicate_indices.is_empty() {
        println!("nothing to drop");
        return Ok(());
    }

    let cleaned: Vec<meerkat_core::Message> = messages
        .iter()
        .enumerate()
        .filter(|(index, _)| !duplicate_indices.contains(index))
        .map(|(_, message)| message.clone())
        .collect();
    let before_bytes: usize = serialized.iter().map(Vec::len).sum();
    let after_bytes: usize = cleaned
        .iter()
        .map(|m| serde_json::to_vec(m).map(|v| v.len()).unwrap_or(0))
        .sum();
    println!(
        "rewrite plan: {} -> {} messages, ~{} -> ~{} bytes",
        messages.len(),
        cleaned.len(),
        before_bytes,
        after_bytes
    );

    if !apply {
        println!("DRY RUN ONLY - re-run with --apply to commit");
        return Ok(());
    }

    let parent_revision = session.transcript_revision()?;
    let mut rewritten = session.clone();
    let commit = rewritten.commit_transcript_rewrite(
        meerkat_core::TranscriptRewriteSelection::MessageRange {
            start: 0,
            end: messages.len(),
        },
        cleaned,
        meerkat_core::TranscriptRewriteReason::new(
            "operator dedup of byte-identical authored System copies",
        ),
        Some("operator/dedup_system_rows".to_string()),
        Some(parent_revision),
    )?;
    meerkat::SessionStore::save_transcript_rewrite(adapter.as_ref(), &rewritten, &commit).await?;
    meerkat::SessionStore::save_authoritative_projection(adapter.as_ref(), &rewritten).await?;
    println!(
        "APPLIED: rewrite generation {} committed, durable head now {} messages at revision {}",
        commit.rewrite_generation,
        rewritten.messages().len(),
        rewritten.transcript_revision()?
    );
    println!("now remove the runtime scratch store and boot; the mint reseeds from durable");
    Ok(())
}
