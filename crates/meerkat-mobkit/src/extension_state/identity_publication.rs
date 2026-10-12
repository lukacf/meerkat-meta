//! Runtime-local readiness only. Durable continuity remains identity authority.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::documents;
use crate::identity_first::{AgentIdentity, bridge::BridgeError};
use meerkat_core::SessionId;
use meerkat_mob::{MemberCreationProvenance, MemberCreationSnapshot, MobReadHandle};

type Target = (String, String);
#[derive(Clone)]
struct Pending {
    identity: AgentIdentity,
    token: Arc<()>,
    receiver: tokio::sync::watch::Receiver<bool>,
}
struct Fence {
    pending: Pending,
    after_cursor: u64,
    predecessor: Option<SessionId>,
    exact_session: Option<SessionId>,
}
#[derive(Default)]
struct State {
    pending: BTreeMap<AgentIdentity, Pending>,
    targets: BTreeMap<Target, Fence>,
}

/// Dropping without publication closes waiters with AuthorityUnavailable.
/// No identity, role, grant, or lineage is carried by this readiness token.
pub struct IdentityPublication {
    owner: Arc<IdentityPublications>,
    identity: AgentIdentity,
    token: Arc<()>,
    sender: tokio::sync::watch::Sender<bool>,
    published: bool,
}
impl IdentityPublication {
    pub(crate) fn publish(mut self) {
        self.sender.send_replace(true);
        self.published = true;
    }
}
impl Drop for IdentityPublication {
    fn drop(&mut self) {
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .pending
            .get(&self.identity)
            .is_some_and(|p| Arc::ptr_eq(&p.token, &self.token))
        {
            state.pending.remove(&self.identity);
        }
        if self.published {
            state
                .targets
                .retain(|_, fence| !Arc::ptr_eq(&fence.pending.token, &self.token));
        }
        // A canceled spawn may still commit after its caller disappears.
        // Keep the closed fence until exact admission settlement is known;
        // sampling the journal head here would incorrectly exclude a late
        // commit and let it be classified as an unrelated worker.
    }
}

#[derive(Default)]
pub(crate) struct IdentityPublications {
    state: Mutex<State>,
}
impl IdentityPublications {
    pub(crate) fn begin(
        self: &Arc<Self>,
        identity: &AgentIdentity,
    ) -> Result<IdentityPublication, BridgeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| BridgeError::Mob("identity publication fence poisoned".into()))?;
        if state.pending.contains_key(identity) {
            return Err(BridgeError::Mob(
                "identity publication already pending".into(),
            ));
        }
        let (sender, receiver) = tokio::sync::watch::channel(false);
        let token = Arc::new(());
        state.pending.insert(
            identity.clone(),
            Pending {
                identity: identity.clone(),
                token: token.clone(),
                receiver,
            },
        );
        Ok(IdentityPublication {
            owner: self.clone(),
            identity: identity.clone(),
            token,
            sender,
            published: false,
        })
    }

    pub(crate) async fn bind(
        &self,
        identity: &AgentIdentity,
        handle: MobReadHandle,
        member: &meerkat_mob::AgentIdentity,
    ) -> Result<(), BridgeError> {
        let after_cursor = handle
            .member_creation_journal_cursor()
            .await
            .map_err(|error| BridgeError::Mob(error.to_string()))?;
        let predecessor = handle
            .get_member(member)
            .await
            .map_err(|error| BridgeError::Mob(error.to_string()))?
            .and_then(|entry| entry.bridge_session_id().cloned());
        let mut state = self
            .state
            .lock()
            .map_err(|_| BridgeError::Mob("identity publication fence poisoned".into()))?;
        let pending = state
            .pending
            .get(identity)
            .cloned()
            .ok_or_else(|| BridgeError::Mob("identity publication was not prepared".into()))?;
        let target = (handle.mob_id().to_string(), member.to_string());
        if state.targets.get(&target).is_some_and(|fence| {
            fence.pending.identity != *identity
                || (!Arc::ptr_eq(&fence.pending.token, &pending.token)
                    && fence.pending.receiver.has_changed().is_ok())
        }) {
            return Err(BridgeError::Mob(
                "target identity publication already pending".into(),
            ));
        }
        let after_cursor = state
            .targets
            .get(&target)
            .map_or(after_cursor, |old| old.after_cursor.min(after_cursor));
        state.targets.insert(
            target,
            Fence {
                pending,
                after_cursor,
                predecessor,
                exact_session: None,
            },
        );
        Ok(())
    }

    pub(crate) fn bind_session(
        &self,
        identity: &AgentIdentity,
        session: &SessionId,
    ) -> Result<(), BridgeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| BridgeError::Mob("identity publication fence poisoned".into()))?;
        let token = state
            .pending
            .get(identity)
            .map(|p| p.token.clone())
            .ok_or_else(|| BridgeError::Mob("identity publication was not prepared".into()))?;
        for fence in state
            .targets
            .values_mut()
            .filter(|fence| Arc::ptr_eq(&fence.pending.token, &token))
        {
            fence.exact_session = Some(session.clone());
        }
        Ok(())
    }

    pub(crate) async fn wait(&self, snapshot: &MemberCreationSnapshot) -> documents::Result<()> {
        let receiver = {
            let state = self
                .state
                .lock()
                .map_err(|_| documents::Error::AuthorityUnavailable)?;
            state
                .targets
                .get(&(
                    snapshot.member_binding.mob_id.clone(),
                    snapshot.member_binding.member.clone(),
                ))
                .filter(|fence| fence_applies(fence, snapshot))
                .map(|fence| fence.pending.receiver.clone())
        };
        if let Some(mut receiver) = receiver {
            receiver
                .wait_for(|ready| *ready)
                .await
                .map_err(|_| documents::Error::AuthorityUnavailable)?;
        }
        Ok(())
    }
}
fn fence_applies(fence: &Fence, snapshot: &MemberCreationSnapshot) -> bool {
    if let Some(exact) = &fence.exact_session {
        return *exact == snapshot.session_id;
    }
    let successor = match &snapshot.creation.provenance {
        MemberCreationProvenance::Successor {
            predecessor_session_id,
            ..
        } => fence.predecessor.as_ref() == Some(predecessor_session_id),
        _ => false,
    };
    successor || snapshot.birth_cursor > fence.after_cursor
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn fixture() -> (
        Arc<IdentityPublications>,
        IdentityPublication,
        MemberCreationSnapshot,
    ) {
        let publications = Arc::new(IdentityPublications::default());
        let identity = AgentIdentity::parse("stable:writer").unwrap();
        let publication = publications.begin(&identity).unwrap();
        let snapshot = MemberCreationSnapshot {
            birth_cursor: 11,
            session_id: SessionId::new(),
            member_binding: meerkat_core::MobMemberBinding {
                mob_id: "test".into(),
                role: "worker".into(),
                member: "writer".into(),
            },
            creation: meerkat_mob::MemberCreationRecord::default(),
            fork_source: None,
        };
        let mut state = publications.state.lock().unwrap();
        let pending = state.pending.get(&identity).unwrap().clone();
        state.targets.insert(
            ("test".into(), "writer".into()),
            Fence {
                pending,
                after_cursor: 10,
                predecessor: None,
                exact_session: None,
            },
        );
        drop(state);
        (publications, publication, snapshot)
    }

    #[tokio::test]
    async fn extension_publication_drop_unblocks_and_refuses_late_birth() {
        let (publications, publication, mut snapshot) = fixture();
        let waiting = {
            let publications = publications.clone();
            let snapshot = snapshot.clone();
            tokio::spawn(async move { publications.wait(&snapshot).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(publication);
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
                .await
                .unwrap()
                .unwrap(),
            Err(documents::Error::AuthorityUnavailable)
        ));
        // The native actor may commit well after cancellation. There is no
        // sampled end cursor that could reclassify that session as a worker.
        snapshot.birth_cursor = 1000;
        assert!(matches!(
            publications.wait(&snapshot).await,
            Err(documents::Error::AuthorityUnavailable)
        ));
    }

    #[tokio::test]
    async fn extension_publication_preserves_older_birth_and_publishes_new_session() {
        let (publications, publication, mut snapshot) = fixture();
        snapshot.birth_cursor = 9;
        publications.wait(&snapshot).await.unwrap();
        snapshot.birth_cursor = 11;
        publication.publish();
        publications.wait(&snapshot).await.unwrap();
        assert!(publications.state.lock().unwrap().targets.is_empty());
    }

    #[tokio::test]
    async fn extension_publication_exact_binding_does_not_block_other_history() {
        let (publications, publication, mut snapshot) = fixture();
        let identity = AgentIdentity::parse("stable:writer").unwrap();
        publications
            .bind_session(&identity, &snapshot.session_id)
            .unwrap();
        drop(publication);
        assert!(matches!(
            publications.wait(&snapshot).await,
            Err(documents::Error::AuthorityUnavailable)
        ));
        snapshot.session_id = SessionId::new();
        publications.wait(&snapshot).await.unwrap();
    }
}
