//! Who a console send's dedupe record belongs to.
//!
//! A console send is deduplicated on its idempotency key inside the scope of
//! the caller that sent it: a verified Console human (OIDC issuer and
//! subject), an authenticated principal with no verified human behind it,
//! or the shared local account of an anonymous caller. The same key under
//! two scopes names two different records, so a replay answers only the
//! caller whose send it was.

use serde_json::json;
use sha2::{Digest, Sha256};

/// Versioned domain of the scoped dedupe digest.
const SCOPED_SEND_KEY_DOMAIN: &str = "meta-console-send-v1";

/// Prefix of a scoped send dedupe key. Unscoped keys keep the `send:` shape.
const SCOPED_SEND_KEY_PREFIX: &str = "send-scoped:";

/// The caller a console send is deduplicated for.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum ConsoleSendScope {
    /// No Console caller: the public Rust send API. Its keys keep the shape
    /// those callers have always had.
    Unscoped,
    /// An anonymous caller of an open Console: the shared local account.
    #[default]
    Anonymous,
    /// A verified Console human, by OIDC issuer and subject.
    Human { issuer: String, subject: String },
    /// An authenticated principal with no verified human behind it: a
    /// service identity, or a token without a subject.
    Principal {
        principal: String,
        service_identity: bool,
    },
}

impl ConsoleSendScope {
    /// Whether this is a Console caller's scope. Only scoped sends are
    /// reconciled or recovered; the unscoped API only replays.
    pub(crate) fn is_scoped(&self) -> bool {
        !matches!(self, Self::Unscoped)
    }

    /// The dedupe key of one send in this scope.
    ///
    /// A scoped key is one digest over every component, so it neither
    /// carries the scope in clear nor links two sends of one caller.
    pub(crate) fn dedupe_key(
        &self,
        runtime_key: &str,
        identity: &str,
        origin: &str,
        idempotency_key: &str,
    ) -> String {
        let owner = match self {
            Self::Unscoped => {
                return unscoped_send_dedupe_key(runtime_key, identity, origin, idempotency_key);
            }
            Self::Anonymous => json!(["anonymous"]),
            Self::Human { issuer, subject } => json!(["human", issuer, subject]),
            Self::Principal {
                principal,
                service_identity,
            } => json!(["principal", principal, service_identity]),
        };
        let input = json!([
            SCOPED_SEND_KEY_DOMAIN,
            runtime_key,
            identity,
            origin,
            idempotency_key,
            owner
        ]);
        let digest = Sha256::digest(input.to_string().as_bytes());
        format!("{SCOPED_SEND_KEY_PREFIX}{digest:x}")
    }
}

/// The key shape of an unscoped send, and of every send recorded before
/// sends were scoped.
pub(crate) fn unscoped_send_dedupe_key(
    runtime_key: &str,
    identity: &str,
    origin: &str,
    idempotency_key: &str,
) -> String {
    format!("send:{runtime_key}:{identity}:{origin}:{idempotency_key}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn human(subject: &str) -> ConsoleSendScope {
        ConsoleSendScope::Human {
            issuer: "https://issuer.example".to_string(),
            subject: subject.to_string(),
        }
    }

    #[test]
    fn scoped_keys_are_stable_and_separate_every_component() {
        let key = |scope: &ConsoleSendScope| scope.dedupe_key("runtime", "agent", "console", "k1");
        let alice = human("alice-sub");
        assert_eq!(key(&alice), key(&alice));
        assert!(key(&alice).starts_with("send-scoped:"));
        let others = [
            human("bob-sub"),
            ConsoleSendScope::Human {
                issuer: "https://other.example".to_string(),
                subject: "alice-sub".to_string(),
            },
            ConsoleSendScope::Anonymous,
            ConsoleSendScope::Principal {
                principal: "alice-sub".to_string(),
                service_identity: false,
            },
            ConsoleSendScope::Principal {
                principal: "alice-sub".to_string(),
                service_identity: true,
            },
        ];
        for other in &others {
            assert_ne!(key(&alice), key(other), "{other:?}");
        }
        for (runtime, identity, origin, idempotency) in [
            ("runtime-b", "agent", "console", "k1"),
            ("runtime", "agent-b", "console", "k1"),
            ("runtime", "agent", "api", "k1"),
            ("runtime", "agent", "console", "k2"),
        ] {
            assert_ne!(
                key(&alice),
                alice.dedupe_key(runtime, identity, origin, idempotency)
            );
        }
    }

    #[test]
    fn scoped_keys_do_not_carry_the_scope_or_the_send_in_clear() {
        let key = human("alice-sub").dedupe_key("runtime", "agent", "console", "k1");
        for clear in ["alice-sub", "issuer.example", "agent", "console", "k1"] {
            assert!(!key.contains(clear), "{key} carries {clear}");
        }
    }

    #[test]
    fn component_boundaries_cannot_be_shifted() {
        let scope = ConsoleSendScope::Anonymous;
        assert_ne!(
            scope.dedupe_key("a:b", "c", "o", "k"),
            scope.dedupe_key("a", "b:c", "o", "k")
        );
        let principal = |principal: &str| ConsoleSendScope::Principal {
            principal: principal.to_string(),
            service_identity: false,
        };
        assert_ne!(
            principal("x\",\"y").dedupe_key("r", "i", "o", "k"),
            principal("x").dedupe_key("r", "i", "o", "\",\"y\"")
        );
    }

    #[test]
    fn unscoped_keys_keep_the_recorded_shape() {
        assert_eq!(
            ConsoleSendScope::Unscoped.dedupe_key("runtime", "agent", "console", "k1"),
            "send:runtime:agent:console:k1"
        );
        assert_ne!(
            ConsoleSendScope::Anonymous.dedupe_key("runtime", "agent", "console", "k1"),
            "send:runtime:agent:console:k1"
        );
    }
}
