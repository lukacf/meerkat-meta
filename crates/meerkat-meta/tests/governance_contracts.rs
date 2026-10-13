#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::uninlined_format_args,
    clippy::collapsible_if,
    clippy::redundant_clone,
    clippy::needless_raw_string_hashes,
    clippy::single_match,
    clippy::redundant_closure_for_method_calls,
    clippy::redundant_pattern_matching,
    clippy::ignored_unit_patterns,
    clippy::clone_on_copy,
    clippy::manual_assert,
    clippy::unwrap_in_result,
    clippy::useless_vec
)]
use std::process::Command;

use meerkat_meta::{
    GovernanceValidationError, validate_governance_contracts, validate_governance_state,
    validate_traceability_statuses,
};

#[test]
fn governance_contracts_rejects_invalid_governance_state() {
    let err = validate_governance_state("spec", "governance_state: blocked")
        .expect_err("invalid governance state must fail");
    assert_eq!(
        err,
        GovernanceValidationError::InvalidGovernanceState {
            file: "spec".to_string(),
            found: "blocked".to_string(),
        }
    );
}

#[test]
fn governance_contracts_rejects_missing_governance_state() {
    let valid_traceability = "\
| REQ-ID | Phase | Implemented In | Runtime Caller | Evidence | Status |\n\
|--------|-------|----------------|----------------|----------|--------|\n\
| TYPE-001 | P0 | .rct/* | - | .rct/outputs/P0/ | MISSING |\n";
    let err = validate_governance_contracts(
        "project: test\n",
        "governance_state: realignment_in_progress\n",
        "governance_state: realignment_in_progress\n",
        valid_traceability,
    )
    .expect_err("missing governance_state must fail");
    assert_eq!(
        err,
        GovernanceValidationError::MissingGovernanceState {
            file: ".rct/spec.yaml".to_string(),
        }
    );
}

#[test]
fn governance_contracts_rejects_unknown_traceability_status() {
    let markdown = "\
| REQ-ID | Phase | Implemented In | Runtime Caller | Evidence | Status |\n\
|--------|-------|----------------|----------------|----------|--------|\n\
| CONTRACT-999 | P0 | - | - | .rct/outputs/P0/ | PENDING |\n";
    let err = validate_traceability_statuses(markdown)
        .expect_err("unknown status should fail validation");
    assert!(matches!(
        err,
        GovernanceValidationError::InvalidTraceabilityStatus { .. }
    ));
}

#[test]
fn governance_contracts_accepts_all_current_traceability_statuses() {
    let mut markdown = String::from(
        "\
| REQ-ID | Phase | Implemented In | Runtime Caller | Evidence | Status |\n\
|--------|-------|----------------|----------------|----------|--------|\n",
    );
    for (index, status) in [
        "TYPED",
        "WIRED",
        "VALIDATED",
        "PROVISIONAL",
        "MISSING",
        "DEFERRED",
        "STUBBED",
    ]
    .iter()
    .enumerate()
    {
        markdown.push_str(&format!(
            "| TYPE-{index:03} | P0 | .rct/* | - | .rct/outputs/P0/ | {status} |\n"
        ));
    }

    validate_traceability_statuses(&markdown).expect("all governance statuses should be accepted");
}

#[test]
fn governance_contracts_rejects_missing_traceability_evidence() {
    let markdown = "\
| REQ-ID | Phase | Implemented In | Runtime Caller | Evidence | Status |\n\
|--------|-------|----------------|----------------|----------|--------|\n\
| TYPE-001 | P0 | .rct/* | - |   | TYPED |\n";
    let err = validate_traceability_statuses(markdown)
        .expect_err("missing evidence should fail validation");
    assert!(matches!(
        err,
        GovernanceValidationError::MissingTraceabilityEvidence { .. }
    ));
}

#[test]
fn governance_contracts_rejects_placeholder_traceability_evidence() {
    let markdown = "\
| REQ-ID | Phase | Implemented In | Runtime Caller | Evidence | Status |\n\
|--------|-------|----------------|----------------|----------|--------|\n\
| TYPE-001 | P0 | .rct/* | - | - | TYPED |\n";
    let err = validate_traceability_statuses(markdown)
        .expect_err("placeholder evidence should fail validation");
    assert!(matches!(
        err,
        GovernanceValidationError::MissingTraceabilityEvidence { .. }
    ));
}

#[test]
fn governance_contracts_accepts_yaml_traceability_with_missing_rows_unimplemented() {
    let yaml = r#"
rows:
  - id: TYPE-001
    status: MISSING
    evidence: []
"#;

    validate_traceability_statuses(yaml).expect("yaml traceability should validate");
}

#[test]
fn governance_contracts_rejects_yaml_traceability_missing_typed_evidence() {
    let yaml = r#"
rows:
  - id: TYPE-001
    status: TYPED
    evidence: []
"#;

    let err = validate_traceability_statuses(yaml)
        .expect_err("typed yaml rows without evidence must fail validation");
    assert!(matches!(
        err,
        GovernanceValidationError::MissingTraceabilityEvidence { .. }
    ));
}

#[test]
fn governance_contracts_accepts_markdown_traceability_after_leading_prose() {
    let markdown = r#"
# Traceability

Intro text before the table is allowed.

| REQ-ID | Phase | Implemented In | Runtime Caller | Evidence | Status |
|--------|-------|----------------|----------------|----------|--------|
| TYPE-001 | P0 | .rct/* | - | .rct/phase-0-evidence.txt | TYPED |
"#;

    validate_traceability_statuses(markdown)
        .expect("markdown traceability with leading prose should validate");
}

/// RCT documents are local working state this repository does not track,
/// so the binary has no default root: without an explicit `--root` it
/// refuses with its usage instead of reading whatever directory it runs in.
#[test]
fn governance_check_requires_an_explicit_root() {
    let output = Command::new(env!("CARGO_BIN_EXE_governance_check"))
        .output()
        .expect("governance_check should run");
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--root <DIR>"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
