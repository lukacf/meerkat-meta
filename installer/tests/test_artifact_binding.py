from copy import deepcopy

import pytest

from launchkit.artifact_binding import frozen_matches, inputs, matches, package_matches, current_version
from launchkit.blueprint import starter_blueprint


def scope():
    return {"id": "guide", "version": 3, "proposal": starter_blueprint().model_dump(mode="json"),
            "brief": {"facts": {"outcome": {"summary": "Keep requested books", "status": "confirmed"}},
                      "requirements": [{"id": "ledger", "outcome": "Preserve a private reading ledger"}]}}


def test_exact_scope_retains_provenance_and_rejects_unbound_legacy_input():
    state = scope()
    frozen = {"guide_id": state["id"], "guide_version": 3, "blueprint": deepcopy(state["proposal"]), "brief": deepcopy(state["brief"])}
    receipt = {"guide_id": state["id"], "guide_version": 3, **inputs(state)}
    state["version"] = 4
    state["messages"] = [{"text": "What did the checker find?"}]
    assert frozen_matches(receipt, frozen, state)
    assert receipt["guide_version"] == frozen["guide_version"] == 3
    del frozen["brief"]
    state["brief"] = {}
    receipt.update(inputs(state))
    assert not frozen_matches(receipt, frozen, state)
    state["version"] = 3
    assert frozen_matches(receipt, frozen, state)
    frozen["guide_version"] = 2
    assert not frozen_matches(receipt, frozen, state)


@pytest.mark.parametrize("path,value", [
    (("brief", "requirements", 0, "outcome"), "Also support Telegram approvals"),
    (("brief", "facts", "outcome", "summary"), "A different application"),
    (("proposal", "target"), "gcp"),
    (("proposal", "port"), 18999),
    (("proposal", "storage"), "json"),
    (("proposal", "agent_classes", 0, "model"), "gpt-6-sol"),
    (("proposal", "agent_classes", 0, "instructions"), "A different context and authority"),
    (("proposal", "agent_classes", 0, "tools", "shell"), True),
    (("proposal", "topology", "pattern"), "custom"),
    (("proposal", "gcp", "project_id"), "other-project"),
])
def test_every_canonical_compiler_and_application_input_invalidation(path, value):
    state = scope()
    receipt = {"guide_id": state["id"], "guide_version": 3, **inputs(state)}
    parent = state
    for key in path[:-1]: parent = parent[key]
    parent[path[-1]] = value
    assert not matches(receipt, state)


def test_legacy_package_requires_full_brief_evidence_to_cross_transcript_version():
    state = scope()
    report = {"guide_version": 3, "blueprint_hash": inputs(state)["blueprint_hash"]}
    assert package_matches(report, state)
    state["version"] = 4
    assert not package_matches(report, state)
    report["acceptance"] = {"brief_digest": inputs(state)["brief_hash"]}
    assert package_matches(report, state)
    state["brief"]["requirements"][0]["outcome"] = "A new feature"
    assert not package_matches(report, state)
    report["acceptance"] = "model says accepted"
    assert not package_matches(report, state)


def test_current_version_is_only_the_host_response_snapshot():
    state = scope()
    state["version"] = 4
    receipt = {"guide_id": "guide", "guide_version": 3, "current": True}
    assert not current_version(receipt, state)
    receipt["current_guide_version"] = 4
    assert current_version(receipt, state)
    for value in (None, True, "4", 3):
        receipt["current_guide_version"] = value
        assert not current_version(receipt, state)


@pytest.mark.parametrize("value", [True, "1", None])
def test_malformed_receipt_provenance_cannot_equal_frozen_version(value):
    state = scope()
    state["version"] = 1
    frozen = {"guide_id": state["id"], "guide_version": 1, "blueprint": state["proposal"], "brief": state["brief"]}
    receipt = {"guide_id": state["id"], "guide_version": value, **inputs(state)}
    assert not frozen_matches(receipt, frozen, state)


@pytest.mark.parametrize("missing", [None, "not a full brief"])
def test_null_or_non_object_legacy_brief_cannot_prove_cross_revision_scope(missing):
    state = scope()
    state.update(version=4, brief={})
    frozen = {"guide_id": "guide", "guide_version": 3, "blueprint": state["proposal"], "brief": missing}
    receipt = {"guide_id": "guide", "guide_version": 3, **inputs(state)}
    assert not frozen_matches(receipt, frozen, state)
