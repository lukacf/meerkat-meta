//! Conformance chapters.
//!
//! Each chapter is an async entry point taking store factories (or, for the
//! bundled-implementation and legacy-data chapters, a filesystem root) and
//! returning `Result<(), ConformanceFailure>` with chapter/step context.
//! Chapters are per-trait capability profiles — instantiate exactly the
//! chapters a backend's declared capabilities support:
//!
//! | chapter                             | applies to |
//! |-------------------------------------|------------|
//! | [`continuity_store`]                | every `ContinuityStore` |
//! | [`continuity_rollback`]             | every `ContinuityStore` (parameterized by [`RollbackPath`]) |
//! | [`local_continuity_fencing_floor`]  | the bundled `LocalContinuityStore` + `LocalLeaseProvider` pair only |
//! | [`continuity_session_adapter`]      | `ContinuitySessionStoreAdapter` over any `ContinuityStore` with session-scoped CAS delete |
//! | [`continuity_incremental`]          | `ContinuityStore`s that advertise `as_incremental_sessions` (M4b delta channel) |
//! | [`event_log`]                       | every `EventLogStore` |
//! | [`console_log`]                     | every `ConsoleLogStore` |
//! | [`binary_blobs`]                    | every `BinaryBlobStore` |
//! | [`legacy_blob_layout`]              | the bundled `ObjectStoreBlobStore::local` only |
//! | [`agent_memory`]                    | every `AgentMemoryProvider` |
//! | [`legacy_continuity_database`]      | the bundled `LocalContinuityStore` only (fabricates its schema) |
//! | [`legacy_memory_database`]          | the bundled `SqliteAgentMemoryStore` only (fabricates its schema) |

mod agent_memory;
mod blobs;
mod console_log;
mod continuity;
mod continuity_adapter;
mod continuity_incremental;
mod continuity_wrapper;
mod event_log;
mod legacy_data;

pub use agent_memory::agent_memory;
pub use blobs::{binary_blobs, legacy_blob_layout};
pub use console_log::console_log;
pub use continuity::{
    RollbackPath, continuity_rollback, continuity_store, local_continuity_fencing_floor,
};
pub use continuity_adapter::continuity_session_adapter;
pub use continuity_incremental::continuity_incremental;
pub use continuity_wrapper::continuity_wrapper_preserves_incremental_channel;
pub use event_log::event_log;
pub use legacy_data::{legacy_continuity_database, legacy_memory_database};
