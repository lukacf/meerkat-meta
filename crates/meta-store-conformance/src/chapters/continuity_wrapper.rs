//! Wrapper chapter: a `ContinuityStore` decorator must keep the session-delta
//! channel of the store it wraps.
//!
//! `ContinuityStore::as_incremental_sessions` used to default to `None`, so a
//! decorator around an incremental-capable store (metrics, auth, caching,
//! tenancy) that never mentioned the method silently turned it into a
//! whole-snapshot store: meerkat then wrote the entire session document at
//! every turn boundary. The method is now required. This chapter checks the
//! embedder's real wrapping: given a substrate that advertises the channel,
//! the wrapped store, and the session adapter over it, must still advertise
//! it.

use std::sync::Arc;

use meerkat_core::SessionStore;
use meerkat_meta::identity_first::{ContinuitySessionStoreAdapter, ContinuityStore};
use meerkat_store_conformance::ConformanceFailure;

use crate::factory::ContinuityStoreFactory;
use crate::steps::Steps;

const CHAPTER: &str = "continuity_wrapper";

/// Check that `wrap` keeps the session-delta channel of an incremental-capable
/// substrate opened from `substrate`.
///
/// Run it with the bundled `LocalContinuityStore` (or your own incremental
/// store) as the substrate and `wrap` composing your decorators exactly as
/// production does.
pub async fn continuity_wrapper_preserves_incremental_channel(
    substrate: &dyn ContinuityStoreFactory,
    wrap: &(dyn Fn(Arc<dyn ContinuityStore>) -> Arc<dyn ContinuityStore> + Send + Sync),
) -> Result<(), ConformanceFailure> {
    let steps = Steps::chapter(CHAPTER);
    let inner = substrate.open().await?;
    steps.ensure(
        "substrate_advertises_delta_channel",
        inner.as_incremental_sessions().is_some(),
        "the wrapper chapter needs a substrate whose as_incremental_sessions() returns Some; \
         use LocalContinuityStore or another incremental-capable store",
    )?;
    let wrapped = wrap(inner);
    steps.ensure(
        "wrapper_forwards_delta_channel",
        wrapped.as_incremental_sessions().is_some(),
        "the wrapped store returned None from as_incremental_sessions() over a substrate that \
         advertises the channel; forward the inner store's channel \
         (`self.inner.as_incremental_sessions()`), or meerkat writes the whole session document \
         at every turn boundary",
    )?;
    let adapter = Arc::new(ContinuitySessionStoreAdapter::new(wrapped));
    steps.ensure(
        "session_adapter_over_wrapper_is_incremental",
        adapter.as_incremental().is_some(),
        "ContinuitySessionStoreAdapter over the wrapped store does not advertise incremental \
         persistence",
    )?;
    Ok(())
}
