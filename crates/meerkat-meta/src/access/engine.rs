//! Pure ABAC evaluation. No locks, no IO — config in, decision out.

use std::collections::{BTreeMap, BTreeSet};

use super::model::{AccessControlConfig, AccessEffect, AccessRule, action_pattern_matches};

/// The authenticated caller's attributes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccessPrincipal {
    /// Authenticated subject (email or token `sub`). `None` means the
    /// request was admitted without app auth (open console); such callers
    /// only match rules with no subject/group constraints.
    pub subject: Option<String>,
    /// Resolved group memberships for the subject.
    pub groups: BTreeSet<String>,
}

impl AccessPrincipal {
    pub fn anonymous() -> Self {
        Self::default()
    }
}

/// Attributes of the resource a check targets. All fields are optional:
/// checks for non-agent actions (e.g. `gating.decide`) carry no resource
/// at all and only match rules without resource selectors.
#[derive(Debug, Clone, Copy, Default)]
pub struct AccessResource<'a> {
    /// Agent identity (preferred key, e.g. `identity:ops-lead`).
    pub identity: Option<&'a str>,
    /// Runtime agent/member id, matched against rule `agents` as a fallback.
    pub agent_id: Option<&'a str>,
    /// Agent role/profile name.
    pub role: Option<&'a str>,
    /// Agent labels. `None` means the attributes are unknown (label
    /// selectors cannot match); `Some` empty means known-empty.
    pub labels: Option<&'a BTreeMap<String, String>>,
}

impl<'a> AccessResource<'a> {
    pub fn none() -> Self {
        Self::default()
    }

    pub fn for_identity(identity: &'a str) -> Self {
        Self {
            identity: Some(identity),
            ..Self::default()
        }
    }

    fn is_present(&self) -> bool {
        self.identity.is_some() || self.agent_id.is_some()
    }
}

/// Outcome of one access check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessDecision {
    Allow,
    Deny { reason: String },
}

impl AccessDecision {
    pub fn is_allow(&self) -> bool {
        matches!(self, Self::Allow)
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Allow => None,
            Self::Deny { reason } => Some(reason),
        }
    }
}

fn action_matches(pattern: &str, action: &str) -> bool {
    action_pattern_matches(pattern, action)
}

fn subject_matches(rule: &AccessRule, principal: &AccessPrincipal) -> bool {
    if rule.subjects.is_empty() && rule.groups.is_empty() {
        return true;
    }
    // `subjects: ["*"]` matches any *authenticated* subject. It does NOT
    // match the anonymous principal: an anonymous caller only matches rules
    // with no subject/group constraints at all (see `AccessPrincipal.subject`
    // and the resource selectors, where `"*"` likewise never matches an
    // absent attribute). This keeps `subjects:["*"]` from silently granting
    // unauthenticated callers on an open console.
    if let Some(subject) = principal.subject.as_deref()
        && rule
            .subjects
            .iter()
            .any(|candidate| candidate == "*" || candidate == subject)
    {
        return true;
    }
    rule.groups
        .iter()
        .any(|group| principal.groups.contains(group))
}

fn resource_matches(rule: &AccessRule, resource: &AccessResource<'_>) -> bool {
    if !rule.has_resource_selector() {
        return true;
    }
    // A constrained rule can never match a check that has no resource.
    if !resource.is_present() {
        return false;
    }
    if !rule.agents.is_empty() {
        let agent_listed = rule.agents.iter().any(|candidate| {
            candidate == "*"
                || resource
                    .identity
                    .is_some_and(|identity| identity == candidate)
                || resource
                    .agent_id
                    .is_some_and(|agent_id| agent_id == candidate)
        });
        if !agent_listed {
            return false;
        }
    }
    if !rule.roles.is_empty() {
        let role_listed = resource.role.is_some_and(|role| {
            rule.roles
                .iter()
                .any(|candidate| candidate == "*" || candidate == role)
        });
        if !role_listed {
            return false;
        }
    }
    if !rule.match_labels.is_empty() {
        let Some(labels) = resource.labels else {
            // Unknown attributes fail label selectors closed.
            return false;
        };
        let all_labels_match = rule
            .match_labels
            .iter()
            .all(|(key, value)| labels.get(key) == Some(value));
        if !all_labels_match {
            return false;
        }
    }
    true
}

/// Evaluate one access check against a configuration.
///
/// Deny-by-default with deny-overrides: a matching deny rule always wins,
/// then a matching allow rule allows, otherwise the check denies. Admin
/// subjects bypass rules entirely; a disabled config allows everything.
pub fn evaluate_access(
    config: &AccessControlConfig,
    principal: &AccessPrincipal,
    action: &str,
    resource: &AccessResource<'_>,
) -> AccessDecision {
    evaluate_access_lineage(config, principal, action, std::slice::from_ref(resource))
}

/// Evaluate one access check against an agent and its spawn lineage.
///
/// `resources` is the agent followed by its spawn ancestors (parent,
/// grandparent, ...). A rule applies when it matches *any* resource in the
/// chain, so permissions granted on a spawning agent extend to the members
/// it spawned — and a deny anywhere in the lineage denies the descendant
/// (deny-overrides is preserved across the chain).
pub(crate) fn evaluate_access_lineage(
    config: &AccessControlConfig,
    principal: &AccessPrincipal,
    action: &str,
    resources: &[AccessResource<'_>],
) -> AccessDecision {
    if !config.enabled {
        return AccessDecision::Allow;
    }
    if let Some(subject) = principal.subject.as_deref()
        && config.admins.iter().any(|admin| admin == subject)
    {
        return AccessDecision::Allow;
    }

    let mut allowed = false;
    for rule in &config.rules {
        let matches = rule
            .actions
            .iter()
            .any(|pattern| action_matches(pattern, action))
            && subject_matches(rule, principal)
            && resources
                .iter()
                .any(|resource| resource_matches(rule, resource));
        if !matches {
            continue;
        }
        match rule.effect {
            AccessEffect::Deny => {
                return AccessDecision::Deny {
                    reason: format!("denied by rule {}", rule.id),
                };
            }
            AccessEffect::Allow => allowed = true,
        }
    }
    if allowed {
        AccessDecision::Allow
    } else {
        AccessDecision::Deny {
            reason: format!("no rule allows {action}"),
        }
    }
}

/// True when the principal could perform `action` against at least one
/// resource — i.e. some allow rule matches the principal (by subject/group)
/// and names the action. Ignores resource selectors and deny rules, so it is
/// a coarse "is this affordance available at all" signal for capability
/// advertisement; per-resource checks (including deny-overrides) still apply
/// at call time. Always true when enforcement is disabled.
pub(crate) fn principal_may_perform(
    config: &AccessControlConfig,
    principal: &AccessPrincipal,
    action: &str,
) -> bool {
    if !config.enabled {
        return true;
    }
    config.rules.iter().any(|rule| {
        matches!(rule.effect, AccessEffect::Allow)
            && rule
                .actions
                .iter()
                .any(|pattern| action_matches(pattern, action))
            && subject_matches(rule, principal)
    })
}

/// Resolve the configured group memberships for a subject.
pub(crate) fn groups_for_subject(config: &AccessControlConfig, subject: &str) -> BTreeSet<String> {
    config
        .groups
        .iter()
        .filter(|(_, group)| group.members.iter().any(|member| member == subject))
        .map(|(name, _)| name.clone())
        .collect()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::access::model::AccessGroup;

    fn config_with_rules(rules: Vec<AccessRule>) -> AccessControlConfig {
        AccessControlConfig {
            enabled: true,
            admins: vec!["root@example.test".to_string()],
            groups: BTreeMap::from([(
                "ops".to_string(),
                AccessGroup {
                    description: None,
                    members: vec!["alice@example.test".to_string()],
                },
            )]),
            rules,
            approvers: BTreeMap::new(),
        }
    }

    fn principal(subject: &str, groups: &[&str]) -> AccessPrincipal {
        AccessPrincipal {
            subject: Some(subject.to_string()),
            groups: groups.iter().map(ToString::to_string).collect(),
        }
    }

    fn allow_rule(id: &str) -> AccessRule {
        AccessRule {
            id: id.to_string(),
            ..AccessRule::default()
        }
    }

    #[test]
    fn disabled_config_allows_everything() {
        let config = AccessControlConfig::default();
        let decision = evaluate_access(
            &config,
            &AccessPrincipal::anonymous(),
            "access.admin",
            &AccessResource::none(),
        );
        assert!(decision.is_allow());
    }

    #[test]
    fn enabled_config_denies_by_default() {
        let config = config_with_rules(vec![]);
        let decision = evaluate_access(
            &config,
            &principal("bob@example.test", &[]),
            "agent.view",
            &AccessResource::for_identity("identity:ops-lead"),
        );
        assert!(!decision.is_allow());
    }

    #[test]
    fn admins_bypass_rules() {
        let config = config_with_rules(vec![]);
        let decision = evaluate_access(
            &config,
            &principal("root@example.test", &[]),
            "access.admin",
            &AccessResource::none(),
        );
        assert!(decision.is_allow());
    }

    #[test]
    fn group_can_view_all_but_send_to_one() {
        let mut view_rule = allow_rule("ops-view-all");
        view_rule.groups = vec!["ops".to_string()];
        view_rule.actions = vec!["agent.view".to_string()];
        let mut send_rule = allow_rule("ops-send-lead");
        send_rule.groups = vec!["ops".to_string()];
        send_rule.actions = vec!["agent.send".to_string()];
        send_rule.agents = vec!["identity:ops-lead".to_string()];
        let config = config_with_rules(vec![view_rule, send_rule]);
        let alice = principal("alice@example.test", &["ops"]);

        let can_view_any = evaluate_access(
            &config,
            &alice,
            "agent.view",
            &AccessResource::for_identity("identity:scout-1"),
        );
        assert!(can_view_any.is_allow());
        let can_send_lead = evaluate_access(
            &config,
            &alice,
            "agent.send",
            &AccessResource::for_identity("identity:ops-lead"),
        );
        assert!(can_send_lead.is_allow());
        let cannot_send_other = evaluate_access(
            &config,
            &alice,
            "agent.send",
            &AccessResource::for_identity("identity:scout-1"),
        );
        assert!(!cannot_send_other.is_allow());
    }

    #[test]
    fn deny_overrides_allow() {
        let mut allow_all = allow_rule("everyone-views");
        allow_all.actions = vec!["agent.view".to_string()];
        let mut deny_secret = allow_rule("hide-secret");
        deny_secret.effect = AccessEffect::Deny;
        deny_secret.actions = vec!["agent.*".to_string()];
        deny_secret.agents = vec!["identity:secret".to_string()];
        let config = config_with_rules(vec![allow_all, deny_secret]);
        let bob = principal("bob@example.test", &[]);

        assert!(
            evaluate_access(
                &config,
                &bob,
                "agent.view",
                &AccessResource::for_identity("identity:scout-1"),
            )
            .is_allow()
        );
        assert!(
            !evaluate_access(
                &config,
                &bob,
                "agent.view",
                &AccessResource::for_identity("identity:secret"),
            )
            .is_allow()
        );
    }

    #[test]
    fn agent_and_role_selectors_support_wildcard() {
        let mut agents_wildcard = allow_rule("view-any-agent");
        agents_wildcard.actions = vec!["agent.view".to_string()];
        agents_wildcard.agents = vec!["*".to_string()];
        let mut roles_wildcard = allow_rule("send-any-role");
        roles_wildcard.actions = vec!["agent.send".to_string()];
        roles_wildcard.roles = vec!["*".to_string()];
        let config = config_with_rules(vec![agents_wildcard, roles_wildcard]);
        let bob = principal("bob@example.test", &[]);

        assert!(
            evaluate_access(
                &config,
                &bob,
                "agent.view",
                &AccessResource::for_identity("identity:anyone"),
            )
            .is_allow()
        );
        // roles: ["*"] matches any known role...
        let with_role = AccessResource {
            identity: Some("identity:anyone"),
            role: Some("scout"),
            ..AccessResource::default()
        };
        assert!(evaluate_access(&config, &bob, "agent.send", &with_role).is_allow());
        // ...but stays closed when the role attribute is unknown.
        assert!(
            !evaluate_access(
                &config,
                &bob,
                "agent.send",
                &AccessResource::for_identity("identity:anyone"),
            )
            .is_allow()
        );
    }

    #[test]
    fn label_selectors_require_known_labels() {
        let mut rule = allow_rule("payments-only");
        rule.actions = vec!["agent.view".to_string()];
        rule.match_labels = BTreeMap::from([("org".to_string(), "payments".to_string())]);
        let config = config_with_rules(vec![rule]);
        let bob = principal("bob@example.test", &[]);

        let labels = BTreeMap::from([("org".to_string(), "payments".to_string())]);
        let with_labels = AccessResource {
            identity: Some("identity:pay-1"),
            labels: Some(&labels),
            ..AccessResource::default()
        };
        assert!(evaluate_access(&config, &bob, "agent.view", &with_labels).is_allow());

        let unknown_labels = AccessResource::for_identity("identity:pay-1");
        assert!(!evaluate_access(&config, &bob, "agent.view", &unknown_labels).is_allow());
    }

    #[test]
    fn resourceless_checks_only_match_unconstrained_rules() {
        let mut constrained = allow_rule("constrained-decide");
        constrained.actions = vec!["gating.decide".to_string()];
        constrained.agents = vec!["identity:ops-lead".to_string()];
        let config = config_with_rules(vec![constrained]);
        let bob = principal("bob@example.test", &[]);
        assert!(
            !evaluate_access(&config, &bob, "gating.decide", &AccessResource::none()).is_allow()
        );

        let mut unconstrained = allow_rule("decide");
        unconstrained.actions = vec!["gating.decide".to_string()];
        let config = config_with_rules(vec![unconstrained]);
        assert!(
            evaluate_access(&config, &bob, "gating.decide", &AccessResource::none()).is_allow()
        );
    }

    #[test]
    fn anonymous_matches_only_unconstrained_subjects() {
        let mut open_rule = allow_rule("everyone-views");
        open_rule.actions = vec!["agent.view".to_string()];
        let mut named_rule = allow_rule("alice-sends");
        named_rule.subjects = vec!["alice@example.test".to_string()];
        named_rule.actions = vec!["agent.send".to_string()];
        let config = config_with_rules(vec![open_rule, named_rule]);
        let anonymous = AccessPrincipal::anonymous();

        assert!(
            evaluate_access(
                &config,
                &anonymous,
                "agent.view",
                &AccessResource::for_identity("identity:scout-1"),
            )
            .is_allow()
        );
        assert!(
            !evaluate_access(
                &config,
                &anonymous,
                "agent.send",
                &AccessResource::for_identity("identity:scout-1"),
            )
            .is_allow()
        );
    }

    #[test]
    fn wildcard_subject_matches_authenticated_but_not_anonymous() {
        let mut star = allow_rule("any-authenticated");
        star.subjects = vec!["*".to_string()];
        star.actions = vec!["agent.view".to_string()];
        let config = config_with_rules(vec![star]);

        // Any authenticated subject matches subjects:["*"].
        assert!(
            evaluate_access(
                &config,
                &principal("carol@example.test", &[]),
                "agent.view",
                &AccessResource::for_identity("identity:scout-1"),
            )
            .is_allow()
        );
        // The anonymous principal does NOT — it only matches unconstrained rules.
        assert!(
            !evaluate_access(
                &config,
                &AccessPrincipal::anonymous(),
                "agent.view",
                &AccessResource::for_identity("identity:scout-1"),
            )
            .is_allow()
        );
    }

    #[test]
    fn groups_resolve_from_config() {
        let config = config_with_rules(vec![]);
        let groups = groups_for_subject(&config, "alice@example.test");
        assert!(groups.contains("ops"));
        assert!(groups_for_subject(&config, "bob@example.test").is_empty());
    }
}
