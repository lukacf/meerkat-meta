//! `role_targets` on `meta/run_flow`.
//!
//! A run started with `role_targets` dispatches each role of its flow only to
//! the members the caller bound to that role. Meerkat's mob actor admits the
//! binding and refuses it typed; this module only reads the parameter, spells
//! its member ids the way the mob roster does, and spells the echo and a
//! refusal back in public aliases. Both `meta/run_flow` implementations (the
//! stdio RPC server and the console) go through it, so they cannot disagree.

use serde_json::{Map, Value};

use crate::member_comms_id::{
    roster_member_id_for_supplied_id, runtime_alias_str, validate_public_member_alias,
};

/// The `meta/capabilities` feature capability that says `meta/run_flow`
/// accepts `role_targets`. A server without it ignores the parameter and
/// runs the flow role-wide, so a client must not send `role_targets` to a
/// server that does not advertise it.
pub(crate) const FLOW_ROLE_TARGETS_FEATURE: &str = "flow_role_targets";

/// The `meta/run_flow` parameter, and the result field, that carry the
/// binding.
pub(crate) const ROLE_TARGETS_PARAM: &str = "role_targets";

/// Read the optional `role_targets` parameter of `meta/run_flow`.
///
/// Absent or `null` is a role-wide run (`Ok(None)`). Otherwise it is an
/// object mapping each role to a non-empty array of member ids, none listed
/// twice for one role. Each id is a public alias: the encoded roster
/// spelling is refused, and a generated runtime alias names its durable
/// identity. Membership, role and lifecycle are not checked here; the mob
/// actor admits the binding.
pub(crate) fn parse_role_targets(
    params: &Value,
) -> Result<Option<meerkat_mob::FlowRoleTargets>, String> {
    let raw = match params.get(ROLE_TARGETS_PARAM) {
        None | Some(Value::Null) => return Ok(None),
        Some(raw) => raw,
    };
    let Some(roles) = raw.as_object() else {
        return Err(
            "role_targets must be an object mapping each role to an array of member ids"
                .to_string(),
        );
    };
    let mut bound = Vec::with_capacity(roles.len());
    for (role, members) in roles {
        let Some(members) = members.as_array() else {
            return Err(format!(
                "role_targets.{role} must be an array of member ids"
            ));
        };
        let mut roster_ids = Vec::with_capacity(members.len());
        for member in members {
            let Some(member) = member.as_str() else {
                return Err(format!("role_targets.{role} must hold member id strings"));
            };
            if member.trim().is_empty() {
                return Err(format!("role_targets.{role} holds an empty member id"));
            }
            validate_public_member_alias(ROLE_TARGETS_PARAM, member)?;
            let roster_id = roster_member_id_for_supplied_id(member);
            if roster_ids.contains(&roster_id) {
                return Err(format!(
                    "role_targets.{role} names member '{member}' more than once"
                ));
            }
            roster_ids.push(roster_id);
        }
        bound.push((meerkat_mob::ProfileName::from(role.as_str()), roster_ids));
    }
    meerkat_mob::FlowRoleTargets::new(bound)
        .map(Some)
        .map_err(|error| error.to_string())
}

/// The binding as `meta/run_flow` echoes it: every bound role with its
/// members as public aliases. A client compares it with what it sent; a
/// server that does not echo a binding did not apply one.
pub(crate) fn role_targets_echo(role_targets: &meerkat_mob::FlowRoleTargets) -> Value {
    Value::Object(
        role_targets
            .bound_roles()
            .map(|(role, members)| {
                (
                    role.as_str().to_owned(),
                    Value::Array(
                        members
                            .iter()
                            .map(|member| {
                                Value::String(runtime_alias_str(member.as_str()).into_owned())
                            })
                            .collect(),
                    ),
                )
            })
            .collect::<Map<String, Value>>(),
    )
}

/// The `meta/run_flow` result: the run id, and the echoed binding for a
/// role-targeted run.
pub(crate) fn run_flow_result(run_id: &meerkat_mob::RunId, echo: Option<Value>) -> Value {
    match echo {
        Some(echo) => serde_json::json!({
            "run_id": run_id.to_string(),
            ROLE_TARGETS_PARAM: echo,
        }),
        None => serde_json::json!({ "run_id": run_id.to_string() }),
    }
}

/// The JSON-RPC error data of a failed `meta/run_flow`.
///
/// A refused binding carries Meerkat's typed refusal, a closed vocabulary:
/// `{"kind": "flow_role_targets_refused", "flow_id", "refusal": {"kind", ...}}`
/// with the refused member as a public alias. Any other failure carries no
/// data, as before.
pub(crate) fn run_flow_error_data(error: &meerkat_mob::MobError) -> Option<Value> {
    if !matches!(error, meerkat_mob::MobError::FlowRoleTargetsRefused { .. }) {
        return None;
    }
    let mut data = error.structured_data()?;
    let alias = data
        .pointer("/refusal/member")
        .and_then(Value::as_str)
        .map(|roster_id| runtime_alias_str(roster_id).into_owned());
    if let Some(alias) = alias
        && let Some(member) = data.pointer_mut("/refusal/member")
    {
        *member = Value::String(alias);
    }
    Some(data)
}

/// The bound members a `meta/run_flow` request names, as the public aliases
/// the console scopes its `agent.send` check to. Malformed entries are
/// skipped here; the handler refuses the request before anything runs.
pub(crate) fn role_target_members_for_access(params: &Value) -> Vec<String> {
    let Some(roles) = params.get(ROLE_TARGETS_PARAM).and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut members = roles
        .values()
        .filter_map(Value::as_array)
        .flatten()
        .filter_map(Value::as_str)
        .map(|member| {
            runtime_alias_str(roster_member_id_for_supplied_id(member).as_str()).into_owned()
        })
        .collect::<Vec<_>>();
    members.sort();
    members.dedup();
    members
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parsed(params: &Value) -> meerkat_mob::FlowRoleTargets {
        parse_role_targets(params)
            .expect("well-formed role targets parse")
            .expect("present role targets parse to a binding")
    }

    #[test]
    fn absent_or_null_role_targets_is_a_role_wide_run() {
        assert_eq!(parse_role_targets(&json!({"flow_id": "review"})), Ok(None));
        assert_eq!(
            parse_role_targets(&json!({"flow_id": "review", "role_targets": null})),
            Ok(None)
        );
    }

    #[test]
    fn malformed_role_targets_are_refused_before_the_runtime_is_asked() {
        for (role_targets, expected) in [
            (json!(["rev-a1"]), "must be an object"),
            (json!({"reviewer": "rev-a1"}), "must be an array"),
            (json!({"reviewer": [1]}), "member id strings"),
            (json!({"reviewer": [" "]}), "empty member id"),
            (
                json!({"reviewer": ["mk--rev_ca1"]}),
                "reserved encoded roster-id",
            ),
            (json!({"reviewer": ["rev-a1", "rev-a1"]}), "more than once"),
            (
                json!({"reviewer": ["rev-a1", "rt:rev-a1:3"]}),
                "more than once",
            ),
            (json!({"reviewer": []}), "to no members"),
            (json!({}), "at least one role"),
        ] {
            let error = parse_role_targets(&json!({ "role_targets": role_targets }))
                .expect_err("malformed role targets are refused");
            assert!(
                error.contains(expected),
                "{role_targets} should be refused with `{expected}`, got: {error}"
            );
        }
    }

    #[test]
    fn member_ids_take_the_roster_spelling_and_echo_as_public_aliases() {
        let params = json!({
            "role_targets": {
                "reviewer": ["rev-a2", "rt:rev-a1:3", "review:issue-7"],
                "coder": ["coder-a"],
            }
        });
        let role_targets = parsed(&params);
        let reviewers = role_targets
            .members(&meerkat_mob::ProfileName::from("reviewer"))
            .expect("the reviewer role is bound");
        assert!(reviewers.contains(&roster_member_id_for_supplied_id("rev-a1")));
        assert!(reviewers.contains(&roster_member_id_for_supplied_id("review:issue-7")));

        let echo = role_targets_echo(&role_targets);
        let mut echoed_reviewers = echo["reviewer"]
            .as_array()
            .expect("the echo maps each role to an array")
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        echoed_reviewers.sort();
        assert_eq!(
            echoed_reviewers,
            ["rev-a1", "rev-a2", "review:issue-7"],
            "the echo names durable identities, never the encoded roster spelling"
        );
        assert_eq!(echo["coder"], json!(["coder-a"]));

        let run_id = meerkat_mob::RunId::new();
        assert_eq!(
            run_flow_result(&run_id, Some(echo.clone())),
            json!({"run_id": run_id.to_string(), "role_targets": echo})
        );
        assert_eq!(
            run_flow_result(&run_id, None),
            json!({"run_id": run_id.to_string()}),
            "a role-wide run's result is unchanged"
        );
    }

    #[test]
    fn a_refused_binding_carries_the_typed_refusal_with_public_member_spelling() {
        let refused = meerkat_mob::MobError::FlowRoleTargetsRefused {
            flow_id: meerkat_mob::FlowId::from("review"),
            refusal: meerkat_mob::FlowRoleTargetRefusal::NotAMember {
                role: meerkat_mob::ProfileName::from("reviewer"),
                member: roster_member_id_for_supplied_id("review:issue-7"),
            },
        };
        let data = run_flow_error_data(&refused).expect("a refused binding carries data");
        assert_eq!(data["kind"], json!("flow_role_targets_refused"));
        assert_eq!(data["flow_id"], json!("review"));
        assert_eq!(
            data["refusal"],
            json!({"kind": "not_a_member", "role": "reviewer", "member": "review:issue-7"})
        );

        assert_eq!(
            run_flow_error_data(&meerkat_mob::MobError::FlowNotFound(
                meerkat_mob::FlowId::from("missing")
            )),
            None,
            "other run_flow failures keep their data-less error"
        );
    }

    #[test]
    fn access_targets_are_every_bound_member_once_in_public_spelling() {
        let params = json!({
            "role_targets": {
                "reviewer": ["rev-a1", "rt:rev-a1:2", "review:issue-7"],
                "coder": ["coder-a", 7],
            }
        });
        assert_eq!(
            role_target_members_for_access(&params),
            ["coder-a", "rev-a1", "review:issue-7"]
        );
        assert!(role_target_members_for_access(&json!({"flow_id": "review"})).is_empty());
        assert!(role_target_members_for_access(&json!({"role_targets": ["rev-a1"]})).is_empty());
    }
}
