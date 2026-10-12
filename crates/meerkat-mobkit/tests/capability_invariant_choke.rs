//! Structural mutation gates for the declared-versus-resolved capability
//! invariant's first-party composition coverage.
//!
//! Behavioral fresh/resume and category-mutation tests live beside the
//! private wrapper implementation. These assertions make the production
//! gateway roots fail if either persistent or ephemeral composition bypasses
//! `MobBootstrapSpec::new`, or if the wrapper stops evaluating after the
//! inner service has materialized the live catalog.

#![allow(clippy::expect_used, clippy::panic)]

const RUNTIME_SOURCE: &str = include_str!("../src/mob_handle_runtime.rs");
const BUILDER_SOURCE: &str = include_str!("../src/unified_runtime/builder.rs");
const MOBKIT_GATEWAY_SOURCE: &str = include_str!("../src/bin/mobkit_gateway.rs");
const RPC_GATEWAY_SOURCE: &str = include_str!("../src/bin/rpc_gateway.rs");

fn occurrences(haystack: &str, needle: &str) -> usize {
    haystack.match_indices(needle).count()
}

/// `source` without its `#[cfg(test)] mod tests { .. }` blocks, so a count
/// sees production composition roots only, not test fixtures that compose a
/// spec the same way. A rustfmt-formatted module indents its whole body, so
/// its closing brace is the first later line that is exactly `}`. Taking the
/// FIRST such line can only end a cut early (a column-0 `}` inside a raw
/// string), which leaves test code in and fails an exact count loudly; it can
/// never swallow production code.
fn production_source(source: &str) -> String {
    const TEST_MODULE: &str = "#[cfg(test)]\nmod tests {\n";
    let mut production = String::with_capacity(source.len());
    let mut rest = source;
    while let Some(start) = rest.find(TEST_MODULE) {
        production.push_str(&rest[..start]);
        let body = &rest[start + TEST_MODULE.len()..];
        let end = body.find("\n}\n").expect("test module closes") + "\n}\n".len();
        rest = &body[end..];
    }
    production.push_str(rest);
    production
}

#[test]
fn bootstrap_constructor_installs_the_post_materialization_wrapper() {
    let constructor = RUNTIME_SOURCE
        .split("impl MobBootstrapSpec {")
        .nth(1)
        .expect("MobBootstrapSpec impl")
        .split("pub fn dispatch_taint_slot")
        .next()
        .expect("constructor section");
    assert!(constructor.contains("PreBuildMobSessionService"));
    assert!(constructor.contains("inner: session_service"));

    let delegated_create = RUNTIME_SOURCE
        .split("macro_rules! delegate_mob_session_service")
        .nth(1)
        .expect("delegation macro")
        .split("async fn start_turn")
        .next()
        .expect("create_session implementation");
    let prepare = delegated_create
        .find("prepare_create_request")
        .expect("capture resolved declaration before materialization");
    let materialize = delegated_create
        .find("self.inner.create_session")
        .expect("inner materialization");
    let evaluate = delegated_create
        .find("complete_create")
        .expect("post-materialization evaluation");
    assert!(prepare < materialize && materialize < evaluate);
}

#[test]
fn both_gateways_funnel_persistent_and_ephemeral_roots_through_the_choke() {
    assert_eq!(
        occurrences(
            &production_source(MOBKIT_GATEWAY_SOURCE),
            "MobBootstrapSpec::new("
        ),
        2,
        "mobkit_gateway persistent and ephemeral roots must both use the common wrapper"
    );
    assert_eq!(
        occurrences(
            &production_source(RPC_GATEWAY_SOURCE),
            "MobBootstrapSpec::new("
        ),
        2,
        "rpc_gateway persistent and ephemeral roots must both use the common wrapper"
    );
}

/// The production view strips exactly the test module: every production
/// root survives, and a fixture inside `mod tests` is not counted.
#[test]
fn production_source_strips_only_the_test_module() {
    let source = "fn root() { MobBootstrapSpec::new(a) }\n#[cfg(test)]\nmod tests {\n    fn fixture() {\n        MobBootstrapSpec::new(b);\n    }\n}\nfn later_root() { MobBootstrapSpec::new(c) }\n";
    let production = production_source(source);
    assert_eq!(occurrences(&production, "MobBootstrapSpec::new("), 2);
    assert!(production.contains("new(a)") && production.contains("new(c)"));
    assert!(!production.contains("fixture"));
}

#[test]
fn unified_builder_roots_funnel_through_wrapped_stock_constructors() {
    for constructor in [
        "MobBootstrapSpec::persistent_inner_with_provider_stores(",
        "MobBootstrapSpec::ephemeral_runtime_backed_with_provider_stores(",
    ] {
        assert!(
            BUILDER_SOURCE.contains(constructor),
            "builder root must retain {constructor}"
        );
    }
    assert_eq!(
        occurrences(
            RUNTIME_SOURCE,
            "let mut spec = Self::new(definition, storage, session_service);"
        ),
        3,
        "every stock constructor used by the builder must install the common wrapper"
    );
}

#[test]
fn wrapper_does_not_skip_resume_materializations() {
    let delegation = RUNTIME_SOURCE
        .split("macro_rules! delegate_mob_session_service")
        .nth(1)
        .expect("delegation macro")
        .split("delegate_mob_session_service!(PreBuildMobSessionService)")
        .next()
        .expect("delegation implementation");
    assert!(
        !delegation.contains("resume_session.is_some"),
        "capability evaluation must not be gated out for either fresh or resumed requests"
    );
    assert_eq!(
        occurrences(delegation, "prepare_create_request(req).await?"),
        6,
        "ordinary, runtime-boundary, exact-witness, and archived-resume creation paths must all capture declared intent"
    );
    assert_eq!(
        occurrences(
            delegation,
            "complete_create(result, context, capability_context)"
        ),
        6,
        "every creation path must compare after successful materialization"
    );
}
