use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    NotFound,
    Conflict,
    RequestIdReused,
    AuthorityUnavailable,
    Invalid(String),
    Storage(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => f.write_str("document unavailable"),
            Self::Conflict => f.write_str("document revision changed"),
            Self::RequestIdReused => f.write_str("request id already used for a different request"),
            Self::AuthorityUnavailable => f.write_str("caller authority unavailable"),
            Self::Invalid(s) => write!(f, "invalid document request: {s}"),
            Self::Storage(s) => write!(f, "document storage failed: {s}"),
        }
    }
}
impl std::error::Error for Error {}

/// Maximum UTF-8 bytes in any identifier, including provider-issued document
/// IDs and revisions. Providers must enforce this before committing so tools
/// can reserve a finite receipt size when preflighting bounded responses.
pub const MAX_IDENTIFIER_BYTES: usize = 512;

pub(crate) fn identifier(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES || value.chars().any(char::is_control)
    {
        Err(Error::Invalid("invalid identifier".into()))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DocumentId(pub String);
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Revision(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Principal {
    Agent(String),
    Worker {
        mob_id: String,
        member_id: String,
        creation_id: String,
    },
}
impl Principal {
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Agent(identity) => identifier(identity),
            Self::Worker {
                mob_id,
                member_id,
                creation_id,
            } => {
                identifier(mob_id)?;
                identifier(member_id)?;
                identifier(creation_id)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Reader,
    Editor,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reach {
    SelfOnly,
    #[default]
    Forks,
    Descendants,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    Fork,
    Spawn,
}

/// Host-attested immutable creation edge, ordered from caller toward root.
/// Deliberately not deserializable from tool input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageLink {
    pub child: Principal,
    pub parent: Principal,
    pub kind: EdgeKind,
    pub ceiling: Option<Role>,
}

/// Construction requires a complete path to a runtime-attested root.
/// Legacy unknown ancestry must fail in the resolver instead of using root().
#[derive(Debug, Clone)]
pub struct VerifiedLineage {
    pub(crate) links: Vec<LineageLink>,
}
impl VerifiedLineage {
    pub fn root() -> Self {
        Self { links: Vec::new() }
    }
    pub fn derived(links: Vec<LineageLink>) -> Result<Self> {
        if links.is_empty() || links.len() > 128 {
            return Err(Error::AuthorityUnavailable);
        }
        let mut seen = BTreeSet::new();
        for (index, link) in links.iter().enumerate() {
            link.child.validate()?;
            link.parent.validate()?;
            if !seen.insert(link.child.clone()) || seen.contains(&link.parent) {
                return Err(Error::AuthorityUnavailable);
            }
            if index > 0 && links[index - 1].parent != link.child {
                return Err(Error::AuthorityUnavailable);
            }
        }
        Ok(Self { links })
    }
}

/// Current host ABAC and tool policy, already evaluated by the runtime adapter.
#[derive(Debug, Clone, Copy)]
pub struct HostPolicy {
    pub read: bool,
    pub edit: bool,
    pub manage: bool,
}
impl HostPolicy {
    pub const ALLOW: Self = Self {
        read: true,
        edit: true,
        manage: true,
    };
    pub const READ_ONLY: Self = Self {
        read: true,
        edit: false,
        manage: false,
    };
    pub const DENY: Self = Self {
        read: false,
        edit: false,
        manage: false,
    };
}

/// Trusted host input, intentionally without Deserialize. Host callers must
/// refresh these facts for every tool dispatch, including restored forks.
#[derive(Debug, Clone)]
pub struct HostAccessContext {
    pub(crate) realm: String,
    pub(crate) principal: Principal,
    pub(crate) lineage: VerifiedLineage,
    pub(crate) member_mobs: BTreeSet<String>,
    pub(crate) managed_mobs: BTreeSet<String>,
    pub(crate) realm_admin: bool,
    pub(crate) policy: HostPolicy,
    pub(crate) valid_owners: BTreeSet<Owner>,
}
impl HostAccessContext {
    pub fn new(
        realm: impl Into<String>,
        principal: Principal,
        lineage: VerifiedLineage,
        policy: HostPolicy,
    ) -> Result<Self> {
        let realm = realm.into();
        identifier(&realm)?;
        principal.validate()?;
        if lineage
            .links
            .first()
            .is_some_and(|link| link.child != principal)
        {
            return Err(Error::AuthorityUnavailable);
        }
        Ok(Self {
            realm,
            principal,
            lineage,
            member_mobs: BTreeSet::new(),
            managed_mobs: BTreeSet::new(),
            realm_admin: false,
            policy,
            valid_owners: BTreeSet::new(),
        })
    }
    pub fn with_memberships(
        mut self,
        member_mobs: BTreeSet<String>,
        managed_mobs: BTreeSet<String>,
        realm_admin: bool,
    ) -> Result<Self> {
        for id in member_mobs.iter().chain(&managed_mobs) {
            identifier(id)?;
        }
        self.member_mobs = member_mobs;
        self.managed_mobs = managed_mobs;
        self.realm_admin = realm_admin;
        Ok(self)
    }
    pub fn principal(&self) -> &Principal {
        &self.principal
    }
    pub fn realm(&self) -> &str {
        &self.realm
    }
    /// The host has resolved these recipients in this realm's authoritative
    /// identity/member registry. Tool-supplied names alone are insufficient.
    pub fn with_valid_owners(mut self, owners: BTreeSet<Owner>) -> Self {
        self.valid_owners = owners;
        self
    }
    pub(crate) fn owner_is_valid(&self, owner: &Owner) -> bool {
        self.valid_owners.contains(owner)
            || match owner {
                Owner::Agent(principal) => principal == &self.principal,
                Owner::Mob(id) => self.managed_mobs.contains(id),
                Owner::Realm => self.realm_admin,
            }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Owner {
    Agent(Principal),
    Mob(String),
    Realm,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Audience {
    Agent {
        principal: Principal,
        #[serde(default)]
        reach: Reach,
    },
    Mob(String),
    Realm,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub audience: Audience,
    pub role: Role,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deny {
    pub audience: Audience,
    pub operation: DeniedOperation,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeniedOperation {
    Read,
    Edit,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentAccess {
    #[serde(default)]
    pub owner_reach: Reach,
    #[serde(default)]
    pub grants: Vec<Grant>,
    #[serde(default)]
    pub denies: Vec<Deny>,
}
impl DocumentAccess {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.grants.len() + self.denies.len() > 256 {
            return Err(Error::Invalid("too many access entries".into()));
        }
        for audience in self
            .grants
            .iter()
            .map(|g| &g.audience)
            .chain(self.denies.iter().map(|d| &d.audience))
        {
            match audience {
                Audience::Agent { principal, .. } => principal.validate()?,
                Audience::Mob(id) => identifier(id)?,
                Audience::Realm => {}
            }
        }
        Ok(())
    }
}

pub const MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentContent {
    pub title: String,
    pub content_type: String,
    pub schema_version: u32,
    pub payload: Vec<u8>,
}
impl DocumentContent {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.title.len() > 1024
            || self.payload.len() > MAX_PAYLOAD_BYTES
            || self.schema_version == 0
        {
            return Err(Error::Invalid(
                "content exceeds limits or has invalid schema version".into(),
            ));
        }
        identifier(&self.content_type)
    }
}
#[derive(Debug, Clone)]
pub struct NewDocument {
    pub content: DocumentContent,
    pub owner: Option<Owner>,
    pub access: DocumentAccess,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: DocumentId,
    pub revision: Revision,
    pub content: DocumentContent,
    pub owner: Owner,
    pub access: DocumentAccess,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentSummary {
    pub id: DocumentId,
    pub revision: Revision,
    pub title: String,
    pub content_type: String,
    pub schema_version: u32,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}
impl From<&Document> for DocumentSummary {
    fn from(doc: &Document) -> Self {
        Self {
            id: doc.id.clone(),
            revision: doc.revision.clone(),
            title: doc.content.title.clone(),
            content_type: doc.content.content_type.clone(),
            schema_version: doc.content.schema_version,
            created_at_ms: doc.created_at_ms,
            updated_at_ms: doc.updated_at_ms,
        }
    }
}
#[derive(Debug, Clone)]
pub struct ListRequest {
    pub limit: usize,
    pub after: Option<DocumentId>,
}
impl Default for ListRequest {
    fn default() -> Self {
        Self {
            limit: 50,
            after: None,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentPage {
    pub documents: Vec<DocumentSummary>,
    pub next: Option<DocumentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentScope {
    realm: String,
    namespace: String,
}
impl DocumentScope {
    pub fn new(realm: impl Into<String>, namespace: impl Into<String>) -> Result<Self> {
        let realm = realm.into();
        let namespace = namespace.into();
        identifier(&realm)?;
        identifier(&namespace)?;
        Ok(Self { realm, namespace })
    }
    pub fn realm(&self) -> &str {
        &self.realm
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub(crate) fn validate(&self, caller: &HostAccessContext) -> Result<()> {
        if self.realm == caller.realm {
            Ok(())
        } else {
            Err(Error::NotFound)
        }
    }
}

/// Fingerprint the normalized original action, before state reads/evaluation.
/// The host adapter must never fingerprint the newly calculated payload.
#[derive(Debug, Clone)]
pub struct RequestIdentity {
    pub(crate) id: String,
    pub(crate) fingerprint: Vec<u8>,
}
impl RequestIdentity {
    pub fn new(id: impl Into<String>, canonical_original_request: &[u8]) -> Result<Self> {
        let id = id.into();
        identifier(&id)?;
        if canonical_original_request.len() > MAX_PAYLOAD_BYTES {
            return Err(Error::Invalid("request too large".into()));
        }
        Ok(Self {
            id,
            fingerprint: Sha256::digest(canonical_original_request).to_vec(),
        })
    }
}
#[derive(Debug, Clone)]
pub enum Mutation {
    Create(NewDocument),
    Replace {
        id: DocumentId,
        expected_revision: Revision,
        content: DocumentContent,
    },
    SetAccess {
        id: DocumentId,
        expected_revision: Revision,
        access: DocumentAccess,
    },
    Transfer {
        id: DocumentId,
        expected_revision: Revision,
        owner: Owner,
        access: Option<DocumentAccess>,
    },
    Delete {
        id: DocumentId,
        expected_revision: Revision,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationReceipt {
    pub document_id: DocumentId,
    pub revision: Revision,
    pub deleted: bool,
}

/// Whether this call committed or replayed a prior commit. Adapters must not
/// attach newly calculated cell results to a replayed receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationOutcome {
    pub receipt: MutationReceipt,
    pub replayed: bool,
}
