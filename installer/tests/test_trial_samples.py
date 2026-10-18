import json
from types import SimpleNamespace
import uuid

import pytest

from launchkit.blueprint import starter_blueprint
from launchkit.generator import PackageFile
from launchkit.implementation import ImplementationService, IMPLEMENTATION_PROMPT, APPLICATION_CONTRACT
from launchkit.acceptance_reviewer import ACCEPTANCE_PROMPT
from launchkit.implementation_workspace import ImplementationWorkspace
from launchkit.projects import fingerprint
from launchkit.templates.storage_setup import write_private
from launchkit.trial_samples import trial_samples


DIGEST = "a" * 64
CASE = {"label": "Save a title twice", "request": "Add Dune to my wishlist twice and show the collection.",
        "fixtures": {"service": {"available": True}, "events": []}}


def samples(value):
    return trial_samples([PackageFile("application/trial.json", json.dumps(value))], DIGEST)


def test_authored_sample_projection_is_bounded_source_data_without_importing_code():
    files = [PackageFile("application/__init__.py", "raise AssertionError('never import authored code')\n"),
             PackageFile("application/trial.json", json.dumps({"cases": [CASE], "claims": {"accepted": True}}))]
    result = trial_samples(files, DIGEST)
    assert result["cases"] == [CASE] and result["source_digest"] == DIGEST
    assert "acceptance evidence" in result["scope"] and "claims" not in result
    assert result["detail"] is None
    assert samples({"cases": [{"label": "Try local state", "request": "Save a note"}]})["cases"][0]["fixtures"] == {}
    absent = trial_samples([], DIGEST)
    assert absent["cases"] == [] and "no authored sample choices" in absent["detail"]
    with pytest.raises(ValueError, match="exact checked source"):
        trial_samples(files, "unbound")


@pytest.mark.parametrize("value", [
    {"cases": []}, {"cases": [CASE] * 13}, {"cases": [None]},
    {"cases": [{**CASE, "label": " "}]}, {"cases": [{**CASE, "label": "x" * 161}]},
    {"cases": [{**CASE, "label": "hidden\nline"}]},
    {"cases": [{**CASE, "request": "x" * 6001}]},
    {"cases": [{**CASE, "fixtures": []}]}, {"cases": [{**CASE, "fixtures": None}]},
    {"cases": [{**CASE, "fixtures": {"value": float("inf")}}]},
    {"cases": [{**CASE, "fixtures": {"value": float("nan")}}]},
    {"cases": [CASE], "extra": "x" * 100_000},
])
def test_invalid_sample_metadata_never_creates_choices(value):
    result = samples(value)
    assert result["cases"] == [] and result["detail"]
    assert result["source_digest"] == DIGEST


@pytest.mark.parametrize("content", ["not JSON", '{"cases":[{"label":"A","request":"B","fixtures":{"x":1e999}}]}'])
def test_malformed_and_numeric_overflow_are_explicitly_unsupported(content):
    result = trial_samples([PackageFile("application/trial.json", content)], DIGEST)
    assert result["cases"] == [] and result["detail"]


def stamp_source_check(workspace):
    # Metadata projection uses an authentic host-signature fixture. Real sandbox
    # callback execution is covered by test_application_trial.py.
    with workspace._locked():
        stored = workspace._state()
        receipt = {"status": "pass", "tests_run": 1, "ready_to_package": True,
                   "digest": workspace._digest(workspace._files(stored)), "checks": []}
        receipt["signature"] = workspace._signed(receipt)
        stored["last_check"] = receipt
        workspace._save_state(stored)


@pytest.mark.asyncio
async def test_status_only_exposes_samples_for_current_authentic_checked_source(tmp_path):
    identity, build_id = str(uuid.uuid4()), str(uuid.uuid4())
    proposal = starter_blueprint().model_dump(mode="json")
    state = {"id": identity, "version": 3, "proposal": proposal, "brief": {"mission": "Keep my reading collection"}}
    service = ImplementationService(tmp_path, SimpleNamespace())
    directory = service._directory(identity, build_id)
    directory.mkdir(parents=True)
    write_private(directory.parent / "latest.json", {"build_id": build_id})
    frozen = {"guide_id": identity, "guide_version": 3, "blueprint": proposal,
              "brief": state["brief"], "acceptance_policy_version": 1}
    write_private(directory / "input.json", frozen)
    write_private(directory / "receipt.json", {"guide_id": identity, "guide_version": 3,
        "build_id": build_id, "blueprint_hash": fingerprint(proposal),
        "brief_hash": fingerprint(state["brief"]), "status": "failed"})
    workspace = ImplementationWorkspace(directory / "workspace", proposal)
    workspace.seed()
    workspace.write_file("application/__init__.py", "def build(context):\n    return object()\n")
    workspace.write_file("application/trial.json", json.dumps({"cases": [CASE]}))
    stamp_source_check(workspace)
    status = await service.status(identity, state=state)
    assert status["source_verified"] and status["trial_samples"]["cases"] == [CASE]
    assert status["trial_samples"]["source_digest"] == status["source_digest"] == workspace.digest()
    assert not status["acceptance_trusted"] and not status["ready_to_package"]
    harmless = await service.status(identity, state={**state, "version": 4,
        "messages": [{"text": "What can I try?"}]})
    assert harmless["trial_samples"] == status["trial_samples"]
    assert harmless["guide_version"] == 3 and harmless["current_guide_version"] == 4
    assert json.loads((directory / "input.json").read_text()) == frozen
    stale = await service.status(identity, state={**state, "version": 4,
        "brief": {"mission": "Keep my reading collection and accept Telegram requests"}})
    assert stale["trial_samples"] is None
    workspace.write_file("application/trial.json", json.dumps({"cases": [{**CASE, "label": "Changed source"}]}))
    changed = await service.status(identity, state=state)
    assert not changed["source_verified"] and changed["trial_samples"] is None
    assert changed["source_digest"] != status["source_digest"]
    stamp_source_check(workspace)
    checked = await service.status(identity, state=state)
    assert checked["trial_samples"]["cases"][0]["label"] == "Changed source"
    assert checked["trial_samples"]["source_digest"] != status["trial_samples"]["source_digest"]
    for invalid in ({key: value for key, value in frozen.items() if key != "guide_id"},
                    {key: value for key, value in frozen.items() if key != "guide_version"},
                    {**frozen, "guide_id": str(uuid.uuid4())},
                    {**frozen, "guide_version": True}):
        write_private(directory / "input.json", invalid)
        unbound = await service.status(identity, state=state)
        assert unbound["source_verified"] and not unbound["current"]
        assert unbound["trial_samples"] is None
    write_private(directory / "input.json", frozen)
    with workspace._locked():
        stored = workspace._state()
        stored["last_check"]["signature"] = "forged"
        workspace._save_state(stored)
    forged = await service.status(identity, state=state)
    assert forged["current"] and not forged["source_verified"]
    assert forged["trial_samples"] is None


def test_requested_trial_contract_requires_shared_behavior_and_independent_source_review():
    assert "For offline-implementable requested behavior, implement rehearse" in IMPLEMENTATION_PROMPT
    assert "Make empty\nfixtures useful when no external I/O is needed" in IMPLEMENTATION_PROMPT
    assert "application/trial.json" in APPLICATION_CONTRACT
    assert "Missing\nsupport is a concrete source gap" in ACCEPTANCE_PROMPT
    assert "parallel demonstration that bypasses production constraints" in ACCEPTANCE_PROMPT
    assert "do not require an application to fabricate unavailable live I/O" in ACCEPTANCE_PROMPT
