use crate::*;
use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Provider migration owners may apply this DDL through their own ledger.
pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS extension_documents (
 realm TEXT NOT NULL, namespace TEXT NOT NULL, id TEXT NOT NULL,
 metadata TEXT NOT NULL, payload BLOB NOT NULL, deleted INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(realm, namespace, id));
CREATE TABLE IF NOT EXISTS extension_receipts (
 realm TEXT NOT NULL, namespace TEXT NOT NULL, principal TEXT NOT NULL,
 request_id TEXT NOT NULL, fingerprint BLOB NOT NULL, receipt TEXT NOT NULL,
 document_id TEXT NOT NULL, operation TEXT NOT NULL,
 PRIMARY KEY(realm, namespace, principal, request_id));";

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
fn encode(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(storage)
}
fn decode<T: for<'a> Deserialize<'a>>(value: &str) -> Result<T> {
    serde_json::from_str(value).map_err(storage)
}
fn now() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(storage)?
        .as_millis()
        .try_into()
        .map_err(storage)
}

/// Connection customization lets a composite provider use its own connection
/// profiles, migration ledger and operation fencing without a Meerkat dependency.
pub trait ConnectionGuard: Send + Sync {
    fn acquire(&self) -> Result<Box<dyn Send>>;
}

#[derive(Clone)]
pub struct SqliteExtensionDocumentStore {
    connection: Arc<Mutex<Connection>>,
    guard: Option<Arc<dyn ConnectionGuard>>,
}
impl SqliteExtensionDocumentStore {
    /// Standalone reference opener. Production composition should supply the
    /// provider-owned path and its ledgered connection using from_connection.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path).map_err(storage)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(storage)?;
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(storage)?;
        connection.execute_batch(SCHEMA).map_err(storage)?;
        Ok(Self::from_connection(connection, None))
    }
    /// The provider must have applied SCHEMA under its migration contract.
    pub fn from_connection(
        connection: Connection,
        guard: Option<Arc<dyn ConnectionGuard>>,
    ) -> Self {
        Self {
            connection: Arc::new(Mutex::new(connection)),
            guard,
        }
    }
    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let connection = self.connection.clone();
        let guard = self.guard.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = connection
                .lock()
                .map_err(|_| Error::Storage("connection poisoned".into()))?;
            let _operation_guard = guard.map(|guard| guard.acquire()).transpose()?;
            f(&mut connection)
        })
        .await
        .map_err(storage)?
    }
}

fn load(
    connection: &Connection,
    scope: &DocumentScope,
    id: &DocumentId,
) -> Result<Option<(Document, bool)>> {
    let row: Option<(String, Vec<u8>, bool)> = connection.query_row(
        "SELECT metadata,payload,deleted FROM extension_documents WHERE realm=?1 AND namespace=?2 AND id=?3",
        params![scope.realm(), scope.namespace(), id.0], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    ).optional().map_err(storage)?;
    row.map(|(metadata, payload, deleted)| {
        let mut doc: Document = decode(&metadata)?;
        doc.content.payload = payload;
        Ok((doc, deleted))
    })
    .transpose()
}

fn receipt(
    connection: &Connection,
    scope: &DocumentScope,
    caller: &HostAccessContext,
    request: &RequestIdentity,
) -> Result<Option<MutationReceipt>> {
    scope.validate(caller)?;
    let row: Option<(Vec<u8>, String, String, String)> = connection.query_row(
        "SELECT fingerprint,receipt,document_id,operation FROM extension_receipts WHERE realm=?1 AND namespace=?2 AND principal=?3 AND request_id=?4",
        params![scope.realm(), scope.namespace(), encode(&caller.principal)?, request.id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    ).optional().map_err(storage)?;
    let Some((fingerprint, receipt, id, operation)) = row else {
        return Ok(None);
    };
    // A receipt is scoped to the actual caller but still requires current
    // authority. Tombstones retain only the last ACL, never deleted content.
    let (document, _) = load(connection, scope, &DocumentId(id))?.ok_or(Error::NotFound)?;
    if !authorize(caller, &document, decode(&operation)?) {
        return Err(Error::NotFound);
    }
    if fingerprint != request.fingerprint {
        return Err(Error::RequestIdReused);
    }
    Ok(Some(decode(&receipt)?))
}

fn write_document(
    connection: &Connection,
    scope: &DocumentScope,
    document: &Document,
    deleted: bool,
    create: bool,
) -> Result<()> {
    let mut metadata = document.clone();
    metadata.content.payload.clear();
    let sql = if create {
        "INSERT INTO extension_documents(realm,namespace,id,metadata,payload,deleted) VALUES (?1,?2,?3,?4,?5,?6)"
    } else {
        "UPDATE extension_documents SET metadata=?4,payload=?5,deleted=?6 WHERE realm=?1 AND namespace=?2 AND id=?3"
    };
    let empty: &[u8] = &[];
    connection
        .execute(
            sql,
            params![
                scope.realm(),
                scope.namespace(),
                document.id.0,
                encode(&metadata)?,
                if deleted {
                    empty
                } else {
                    document.content.payload.as_slice()
                },
                deleted
            ],
        )
        .map_err(storage)?;
    Ok(())
}

fn validate_owner(owner: &Owner) -> Result<()> {
    match owner {
        Owner::Agent(principal) => principal.validate(),
        Owner::Mob(id) => types::identifier(id),
        Owner::Realm => Ok(()),
    }
}

#[async_trait]
impl ExtensionDocumentStore for SqliteExtensionDocumentStore {
    async fn get(
        &self,
        scope: &DocumentScope,
        caller: &HostAccessContext,
        id: &DocumentId,
    ) -> Result<Document> {
        scope.validate(caller)?;
        let scope = scope.clone();
        let caller = caller.clone();
        let id = id.clone();
        self.run(move |connection| {
            let (document, deleted) = load(connection, &scope, &id)?.ok_or(Error::NotFound)?;
            if deleted || !authorize(&caller, &document, Operation::Read) {
                return Err(Error::NotFound);
            }
            Ok(document)
        })
        .await
    }
    async fn list(
        &self,
        scope: &DocumentScope,
        caller: &HostAccessContext,
        request: ListRequest,
    ) -> Result<DocumentPage> {
        scope.validate(caller)?;
        if request.limit == 0 || request.limit > 100 {
            return Err(Error::Invalid("list limit must be 1-100".into()));
        }
        let scope = scope.clone();
        let caller = caller.clone();
        self.run(move |connection| {
            // A read transaction keeps cursor validation and access filtering on
            // one snapshot when another process changes an ACL concurrently.
            let tx = connection.transaction().map_err(storage)?;
            if let Some(id) = &request.after {
                let (document, deleted) = load(&tx, &scope, id)?.ok_or(Error::NotFound)?;
                if deleted || !authorize(&caller, &document, Operation::Read) { return Err(Error::NotFound); }
            }
            let mut statement = tx.prepare("SELECT metadata FROM extension_documents WHERE realm=?1 AND namespace=?2 AND deleted=0 AND id>?3 ORDER BY id").map_err(storage)?;
            let mut rows = statement.query(params![scope.realm(), scope.namespace(), request.after.as_ref().map_or("", |id| id.0.as_str())]).map_err(storage)?;
            let mut documents = Vec::new(); let mut more = false;
            while let Some(row) = rows.next().map_err(storage)? {
                let metadata: String = row.get(0).map_err(storage)?; let document: Document = decode(&metadata)?;
                if !authorize(&caller, &document, Operation::Read) { continue; }
                if documents.len() == request.limit { more = true; break; }
                documents.push(DocumentSummary::from(&document));
            }
            let next = if more { documents.last().map(|doc| doc.id.clone()) } else { None };
            Ok(DocumentPage { documents, next })
        }).await
    }
    async fn lookup_receipt(
        &self,
        scope: &DocumentScope,
        caller: &HostAccessContext,
        request: &RequestIdentity,
    ) -> Result<Option<MutationReceipt>> {
        let scope = scope.clone();
        let caller = caller.clone();
        let request = request.clone();
        self.run(move |connection| {
            let tx = connection.transaction().map_err(storage)?;
            receipt(&tx, &scope, &caller, &request)
        })
        .await
    }
    async fn mutate(
        &self,
        scope: &DocumentScope,
        caller: &HostAccessContext,
        request: &RequestIdentity,
        mutation: Mutation,
    ) -> Result<MutationOutcome> {
        scope.validate(caller)?;
        let scope = scope.clone();
        let caller = caller.clone();
        let request = request.clone();
        self.run(move |connection| {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(storage)?;
            if let Some(receipt) = receipt(&tx, &scope, &caller, &request)? { return Ok(MutationOutcome { receipt, replayed: true }); }
            let create = matches!(&mutation, Mutation::Create(_));
            let deleted = matches!(&mutation, Mutation::Delete { .. });
            let operation = match &mutation { Mutation::Replace { .. } => Operation::Edit, _ => Operation::Manage };
            let timestamp = now()?;
            let mut document = match mutation {
                Mutation::Create(new) => {
                    let owner = new.owner.unwrap_or_else(|| Owner::Agent(caller.principal.clone()));
                    validate_owner(&owner)?; new.content.validate()?; new.access.validate()?;
                    if !can_create(&caller, &owner) { return Err(Error::NotFound); }
                    Document { id: DocumentId(uuid::Uuid::new_v4().to_string()), revision: Revision(String::new()), content: new.content, owner, access: new.access, created_at_ms: timestamp, updated_at_ms: timestamp }
                }
                other => {
                    let (id, expected_revision) = match &other {
                        Mutation::Replace { id, expected_revision, .. } | Mutation::SetAccess { id, expected_revision, .. } | Mutation::Transfer { id, expected_revision, .. } | Mutation::Delete { id, expected_revision } => (id, expected_revision),
                        Mutation::Create(_) => return Err(Error::Invalid("invalid mutation".into())),
                    };
                    let (mut doc, tombstone) = load(&tx, &scope, id)?.ok_or(Error::NotFound)?;
                    if tombstone || !authorize(&caller, &doc, operation) { return Err(Error::NotFound); }
                    if &doc.revision != expected_revision { return Err(Error::Conflict); }
                    match other {
                        Mutation::Replace { content, .. } => { content.validate()?; doc.content = content; }
                        Mutation::SetAccess { access, .. } => { access.validate()?; doc.access = access; }
                        Mutation::Transfer { owner, access, .. } => { validate_owner(&owner)?; if !caller.owner_is_valid(&owner) { return Err(Error::NotFound); } doc.owner = owner; if let Some(access) = access { access.validate()?; doc.access = access; } }
                        Mutation::Delete { .. } => { doc.content = DocumentContent { title: String::new(), content_type: "application/octet-stream".into(), schema_version: 1, payload: Vec::new() }; }
                        Mutation::Create(_) => return Err(Error::Invalid("invalid mutation".into())),
                    }
                    doc
                }
            };
            document.revision = Revision(uuid::Uuid::new_v4().to_string()); document.updated_at_ms = timestamp;
            write_document(&tx, &scope, &document, deleted, create)?;
            let result = MutationReceipt { document_id: document.id.clone(), revision: document.revision.clone(), deleted };
            tx.execute("INSERT INTO extension_receipts(realm,namespace,principal,request_id,fingerprint,receipt,document_id,operation) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![scope.realm(), scope.namespace(), encode(&caller.principal)?, request.id, request.fingerprint, encode(&result)?, document.id.0, encode(&operation)?]).map_err(storage)?;
            tx.commit().map_err(storage)?;
            Ok(MutationOutcome { receipt: result, replayed: false })
        }).await
    }
}
