//! The mob announcement database under Meta's storage contract: ledgered
//! schema, the per-operation maintenance fence, and the disk provider's
//! opener.

use std::path::PathBuf;
use std::sync::Arc;

use super::sqlite::{ConnectionGuard, SCHEMA, SCHEMA_V2, SqliteAnnouncementStore};
use super::types::{AnnouncementStoreError, Result};

/// Version 1, as 0.9.4 released it: [`SCHEMA`] is frozen and pinned.
fn initialize_schema_v1(
    tx: &rusqlite::Transaction<'_>,
) -> std::result::Result<(), rusqlite::Error> {
    tx.execute_batch(SCHEMA)
}

/// Version 2 adds the source tables and changes no version 1 object.
fn migrate_to_v2(tx: &rusqlite::Transaction<'_>) -> std::result::Result<(), rusqlite::Error> {
    tx.execute_batch(SCHEMA_V2)
}

/// A fresh domain at the current version.
fn initialize_current(tx: &rusqlite::Transaction<'_>) -> std::result::Result<(), rusqlite::Error> {
    tx.execute_batch(SCHEMA)?;
    tx.execute_batch(SCHEMA_V2)
}

/// A store left at version 1 by 0.9.4 matches the released version 1 catalog
/// before it is migrated.
fn verify_released_0_9_4_schema(
    connection: &rusqlite::Connection,
) -> std::result::Result<(), String> {
    meerkat_sqlite::verify_released_schema_fingerprint(
        connection,
        &ANNOUNCEMENT_SCHEMA_DOMAIN,
        VERSION_1_OBJECTS,
        initialize_schema_v1,
    )
}

/// Every table and index version 1 created.
const VERSION_1_OBJECTS: &[meerkat_sqlite::SchemaObject] = &[
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "announcement_logs",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "announcements",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "announcement_topics",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "member_subscriptions",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "member_subscription_topics",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "member_subscription_mobs",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "owed_announcements",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "announcement_members",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "announcement_batches",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "announcement_batch_subscriptions",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "announcement_operations",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "mob_lineage",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "announcement_gaps",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Table,
        name: "member_routes",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Index,
        name: "member_subscriptions_by_member",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Index,
        name: "member_subscriptions_by_state",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Index,
        name: "owed_announcements_open",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Index,
        name: "owed_announcements_by_announcement",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Index,
        name: "owed_announcements_by_batch",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Index,
        name: "announcement_batches_by_state",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Index,
        name: "announcement_batches_by_member",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Index,
        name: "announcement_batches_late_reads",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Index,
        name: "mob_lineage_live",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Index,
        name: "mob_lineage_by_parent",
    },
    meerkat_sqlite::SchemaObject {
        kind: meerkat_sqlite::SchemaObjectKind::Index,
        name: "member_routes_by_route",
    },
];

/// The announcement store's ledgered schema. `owned_objects` is every table
/// and index [`SCHEMA`] and [`SCHEMA_V2`] create, so drift or absence in any
/// of them is caught when the store opens. A version freezes with the
/// release that ships it: version 1 with 0.9.4. Any later change is a new
/// version with its migration.
pub(crate) const ANNOUNCEMENT_SCHEMA_DOMAIN: meerkat_sqlite::SchemaDomain =
    meerkat_sqlite::SchemaDomain {
        name: "meta-mob-announcements",
        migrations: &[
            meerkat_sqlite::Migration {
                version: 1,
                name: "announcements-subscriptions-batches",
                apply: initialize_schema_v1,
            },
            meerkat_sqlite::Migration {
                version: 2,
                name: "announcement-sources",
                apply: migrate_to_v2,
            },
        ],
        initialize_current,
        allowed_existing_versions: &[1, 2],
        bridge_recoverable_versions: &[],
        released_predecessors: &[meerkat_sqlite::SchemaPredecessor {
            version: 1,
            verify: verify_released_0_9_4_schema,
        }],
        owned_objects: &[
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "announcement_logs",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "announcements",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "announcement_topics",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "member_subscriptions",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "member_subscription_topics",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "member_subscription_mobs",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "owed_announcements",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "announcement_members",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "announcement_batches",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "announcement_batch_subscriptions",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "announcement_operations",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "mob_lineage",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "announcement_gaps",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "member_routes",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Index,
                name: "member_subscriptions_by_member",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Index,
                name: "member_subscriptions_by_state",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Index,
                name: "owed_announcements_open",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Index,
                name: "owed_announcements_by_announcement",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Index,
                name: "owed_announcements_by_batch",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Index,
                name: "announcement_batches_by_state",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Index,
                name: "announcement_batches_by_member",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Index,
                name: "announcement_batches_late_reads",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Index,
                name: "mob_lineage_live",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Index,
                name: "mob_lineage_by_parent",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Index,
                name: "member_routes_by_route",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "announcement_sources",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Index,
                name: "announcement_sources_by_identity",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Table,
                name: "announcement_source_topics",
            },
            meerkat_sqlite::SchemaObject {
                kind: meerkat_sqlite::SchemaObjectKind::Index,
                name: "announcement_source_topics_by_source",
            },
        ],
        retired_objects: &[],
    };

struct DiskOperationFence(PathBuf);

impl ConnectionGuard for DiskOperationFence {
    fn acquire(&self) -> Result<Box<dyn Send>> {
        meerkat_sqlite::OperationGuard::for_database(&self.0)
            .map(|guard| Box::new(guard) as Box<dyn Send>)
            .map_err(|error| AnnouncementStoreError::Storage(error.to_string()))
    }
}

/// Open the mob announcement database at `path` under the ledgered schema
/// and the maintenance fence.
pub(crate) fn open_disk_store(
    path: PathBuf,
) -> std::result::Result<SqliteAnnouncementStore, String> {
    let mut connection = meerkat_sqlite::open(&path, meerkat_sqlite::ConnectionProfile::PRIMARY)
        .map_err(|error| error.to_string())?;
    meerkat_sqlite::apply_domain_migrations(&mut connection, &ANNOUNCEMENT_SCHEMA_DOMAIN)
        .map_err(|error| error.to_string())?;
    Ok(SqliteAnnouncementStore::from_connection(
        connection,
        Some(Arc::new(DiskOperationFence(path))),
    ))
}
