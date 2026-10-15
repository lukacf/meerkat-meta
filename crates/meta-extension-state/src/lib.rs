//! Opaque extension documents. Runtime authority is supplied by the host on
//! every call; document policy and conditional commits belong to the backend.
mod policy;
mod sqlite;
mod types;

pub use policy::{Operation, authorize, can_create};
pub use sqlite::{ConnectionGuard, SCHEMA, SqliteExtensionDocumentStore};
pub use types::*;

use async_trait::async_trait;
use std::sync::Arc;

/// Implementations must authorize and compare revisions in the same atomic
/// transaction as the mutation and receipt. No method accepts tool arguments
/// as authority. Implementations must preserve successful receipts indefinitely.
#[async_trait]
pub trait ExtensionDocumentStore: Send + Sync {
    async fn get(
        &self,
        scope: &DocumentScope,
        caller: &HostAccessContext,
        id: &DocumentId,
    ) -> Result<Document>;
    async fn list(
        &self,
        scope: &DocumentScope,
        caller: &HostAccessContext,
        request: ListRequest,
    ) -> Result<DocumentPage>;
    async fn lookup_receipt(
        &self,
        scope: &DocumentScope,
        caller: &HostAccessContext,
        request: &RequestIdentity,
    ) -> Result<Option<MutationReceipt>>;
    async fn mutate(
        &self,
        scope: &DocumentScope,
        caller: &HostAccessContext,
        request: &RequestIdentity,
        mutation: Mutation,
    ) -> Result<MutationOutcome>;
    /// Whether any live document of `realm`, in any namespace, names the mob
    /// `mob_id` as its owner or in an audience entry
    /// ([`Document::names_mob`]). A host asks before a new mob takes an id:
    /// the new mob would inherit what those documents grant the id. The
    /// answer covers the documents committed when the store reads them, and
    /// reading them costs one pass over every live document of the realm.
    /// A store that cannot answer returns an error, and the host then
    /// refuses the create.
    async fn references_mob(&self, realm: &str, mob_id: &str) -> Result<bool> {
        let _ = (realm, mob_id);
        Err(Error::Storage(
            "this document store cannot report which documents name a mob".into(),
        ))
    }
}

/// Namespace and realm are bound by host composition, never tool input.
#[derive(Clone)]
pub struct DocumentService {
    scope: DocumentScope,
    store: Arc<dyn ExtensionDocumentStore>,
}

impl DocumentService {
    pub fn new(
        realm: impl Into<String>,
        namespace: impl Into<String>,
        store: Arc<dyn ExtensionDocumentStore>,
    ) -> Result<Self> {
        Ok(Self {
            scope: DocumentScope::new(realm, namespace)?,
            store,
        })
    }
    pub fn scope(&self) -> &DocumentScope {
        &self.scope
    }
    pub async fn get(&self, caller: &HostAccessContext, id: &DocumentId) -> Result<Document> {
        self.store.get(&self.scope, caller, id).await
    }
    pub async fn list(
        &self,
        caller: &HostAccessContext,
        request: ListRequest,
    ) -> Result<DocumentPage> {
        self.store.list(&self.scope, caller, request).await
    }
    /// Call before loading a workbook or evaluating an edit. The backend checks
    /// again atomically during mutate to handle concurrent duplicate calls.
    pub async fn lookup_receipt(
        &self,
        caller: &HostAccessContext,
        request: &RequestIdentity,
    ) -> Result<Option<MutationReceipt>> {
        self.store
            .lookup_receipt(&self.scope, caller, request)
            .await
    }
    pub async fn mutate(
        &self,
        caller: &HostAccessContext,
        request: &RequestIdentity,
        mutation: Mutation,
    ) -> Result<MutationOutcome> {
        self.store
            .mutate(&self.scope, caller, request, mutation)
            .await
    }
}
