//! The mob announcement database under Meta's storage contract: ledgered
//! schema, the per-operation maintenance fence, and the disk provider's
//! opener.

use std::path::PathBuf;
use std::sync::Arc;

use super::sqlite::{ConnectionGuard, SCHEMA, SqliteAnnouncementStore};
use super::types::{AnnouncementStoreError, Result};

fn initialize_schema(tx: &rusqlite::Transaction<'_>) -> std::result::Result<(), rusqlite::Error> {
    tx.execute_batch(SCHEMA)
}

/// The announcement store's ledgered schema. `owned_objects` is every table
/// and index [`SCHEMA`] creates, so drift or absence in any of them is caught
/// when the store opens. Version 1 freezes with the release that ships it;
/// any later change is a new version with its migration.
pub(crate) const ANNOUNCEMENT_SCHEMA_DOMAIN: meerkat_sqlite::SchemaDomain =
    meerkat_sqlite::SchemaDomain {
        name: "meta-mob-announcements",
        migrations: &[meerkat_sqlite::Migration {
            version: 1,
            name: "announcements-subscriptions-batches",
            apply: initialize_schema,
        }],
        initialize_current: initialize_schema,
        allowed_existing_versions: &[1],
        bridge_recoverable_versions: &[],
        released_predecessors: &[],
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
