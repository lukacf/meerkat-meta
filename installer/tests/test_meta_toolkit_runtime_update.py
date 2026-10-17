"""Inert selected-product source/retention counterexamples, not native acceptance."""
from copy import deepcopy
from dataclasses import asdict, replace
import json
from pathlib import Path
import subprocess
import zipfile

import pytest

from launchkit import compiler, deployment_updates as updates, runtime_dependencies as dependencies
from launchkit import runtime_selection as selection
from launchkit.generator import PackageFile
from launchkit.projects import fingerprint
from launchkit.templates import install_candidate as adapter
from test_meta_toolkit_runtime_selection import (product, no_product_execution, put, pin, json_bytes,
    changed_assembly, digest)
from test_reading_runtime_refresh import previous_framework, inventory, report
from test_candidate_deployment_updates import installation, packed, candidate_proof
from test_candidate_host_integration import inert_pair, PLATFORM
from test_runtime_candidates import Bundle
from test_candidate_deployment_updates import crossing as legacy_crossing
from test_candidate_deployment_updates import completed as completed_legacy


def test_legacy_generated_startup_keeps_actual_selected_crossing_and_closed_history(legacy_crossing, tmp_path):
    _, target, previous, candidate, _, _ = legacy_crossing
    descriptor = updates.runtime_dependency_transition(previous, candidate, {}, runtime_target=target)
    assert descriptor is not None and descriptor["to_version"] == "0.8.48"
    assert digest(candidate["start.sh"][0]) != descriptor["required_supervisor_sources"]["launchkit/templates/start.sh"]
    directory, _, _, expected = completed_legacy(tmp_path, legacy_crossing)
    assert updates.transaction(directory, expected["update_id"])[2] == expected


@pytest.mark.parametrize("defect", ["retained-script", "source-map"])
def test_legacy_generated_startup_refuses_changed_retained_script_or_source_map(legacy_crossing, defect):
    _, target, previous, candidate, _, _ = legacy_crossing
    descriptor = updates.runtime_dependency_transition(previous, candidate, {}, runtime_target=target)
    assert descriptor is not None
    retained, bound = deepcopy(candidate), deepcopy(descriptor)
    if defect == "retained-script":
        content, mode = retained["start.sh"]
        retained["start.sh"] = (content.replace(b"interpreter is unavailable", b"interpreter is available"), mode)
    else:
        bound["required_supervisor_sources"]["launchkit/templates/start.sh"] = "0" * 64
    assert updates._archived_supervisor_sources(retained, bound, 4) is None


def catalog_target(product, model="gpt-6-astra"):
    product.catalog = {"models": [{"id": model, "provider": "openai", "display_name": "Inert model",
        "tier": "fixture", "vision": True, "image_tool_results": True,
        "profile": {"image_input": True, "image_tool_results": True}}], "provider_defaults": []}
    catalog = put(product.target.model_catalog.path, json_bytes(product.catalog))
    product.target = replace(product.target, model_catalog=catalog)
    product.report["cases"][0]["model_catalog"] = pin(catalog)
    verification = put(product.final / "gateway-verification.json", json_bytes(product.report))
    product.assembly["qualification"]["installed_gateway_verification_sha256"] = verification.sha256
    product.target = changed_assembly(product)
    return product.target


@pytest.fixture
def product_crossing(product, monkeypatch):
    target = catalog_target(product)
    spec, snapshot = previous_framework(monkeypatch, "0.8.47")
    old = {item.path: (item.as_bytes(), 0o755 if item.mode & 0o111 else 0o644) for item in snapshot.files}
    old_framework = {"kind": "host-framework-provenance", "version": 1,
        "blueprint_digest": fingerprint(spec), "baseline_digest": snapshot.baseline_digest,
        "current_compiler_baseline_digest": snapshot.baseline_digest, "basis": "current-compiler",
        "retained_template_paths": []}
    old_report = report(spec, old_framework, "inert-predecessor", "a" * 64)
    old["implementation-report.json"] = (json_bytes(old_report), 0o644)
    old["application/__init__.py"] = (b"raise RuntimeError('inert app must never import')\n", 0o644)
    old["requirements.application.txt"] = (b"# inert application with no extra dependencies\n", 0o644)
    source = [PackageFile(name, content.decode(), 0o600 if name == ".env.example" else mode)
        for name, (content, mode) in old.items() if name != "SHA256SUMS"]
    generated = compiler.generate_runtime_refresh(spec, source, runtime_target=target)
    ordinary = compiler.generate_package(spec, runtime_target=target)
    baseline, current = inventory(generated), inventory(ordinary)
    retained = sorted(name for name in baseline.keys() | current.keys()
        if name != "SHA256SUMS" and baseline.get(name) != current.get(name))
    binding = {"build_id": old_report["build_id"], "source_digest": old_report["verification"]["digest"]}
    framework = {"kind": "host-framework-provenance", "version": 1, "blueprint_digest": fingerprint(spec),
        "basis": "runtime-refresh", "reason": "explicit-runtime-refresh", "previous": binding, "origin": binding,
        "baseline_digest": fingerprint(baseline), "current_compiler_baseline_digest": fingerprint(current),
        "retained_template_paths": retained, "runtime_candidate": selection.candidate_selection(target)}
    new_report = report(spec, framework, "inert-successor", "b" * 64)
    candidate = {item.path: (item.as_bytes(), 0o755 if item.mode & 0o111 else 0o644) for item in generated}
    for name, value in old.items():
        if name.startswith("application/") or name == "requirements.application.txt":
            candidate[name] = value
    candidate["implementation-report.json"] = (json_bytes(new_report), 0o644)
    return product, target, old, candidate, old_report, new_report


def test_product_compiler_preserves_design_and_refuses_manual_or_unknown_model(product_crossing):
    product, target, old, candidate, _, _ = product_crossing
    for name in ("blueprint.json", "config/mob.toml", "config/console.toml", "config/connections.json", "application/__init__.py"):
        assert candidate[name] == old[name]
    for name in ("install.sh", "start.sh", "install_gateway.py"):
        text = candidate[name][0].decode()
        assert "offered installation, application setup, update or recovery controls" in text
        assert "Shell entrypoints cannot select its native setup inputs" in text
        assert "pip install" not in text and "--doctor" not in text
    assert b"meerkat-meta==0.9.0 --hash=sha256:" in candidate["requirements.txt"][0]
    assert b"meerkat-toolkit==0.9.0 --hash=sha256:" in candidate["requirements.txt"][0]
    assert b"complete native product prefix" in candidate["README.md"][0]
    spec = json.loads(candidate["blueprint.json"][0])
    spec["agent_classes"][0]["model"] = "gpt-6.1-sol"
    with pytest.raises(ValueError, match="catalog"):
        compiler.generate_package(spec, runtime_target=target)


def test_product_descriptor_requires_selected_current_framework_and_offline_bytes(product_crossing):
    product, target, old, candidate, _, _ = product_crossing
    assert updates.runtime_dependency_transition(old, candidate, {}) is None
    descriptor = updates.runtime_dependency_transition(old, candidate, {}, runtime_target=target)
    assert descriptor is not None
    assert descriptor["kind"] == "selected-meta-toolkit-runtime-dependency-update"
    assert descriptor["version"] == 4 and not descriptor["native_registry_update"]
    assert descriptor["runtime_candidate"] == selection.candidate_selection(target)
    assert descriptor["runtime_candidate_artifacts"]["sdk"] == selection._json_paths({"meta": asdict(target.meta), "toolkit": asdict(target.toolkit)})
    assert updates.runtime_dependency_transition(old, candidate, {"execution_mode": "background"}, runtime_target=target) is None
    changed = deepcopy(candidate)
    changed["application/__init__.py"] = (b"changed application\n", 0o644)
    assert updates.runtime_dependency_transition(old, changed, {}, runtime_target=target) is None
    target.model_catalog.path.write_bytes(b"changed catalog")
    assert updates.runtime_dependency_transition(old, candidate, {}, runtime_target=target) is None


def test_closed_product_byte_facts_retain_exact_selection_and_refuse_changes(product_crossing):
    _, target, old, candidate, _, _ = product_crossing
    descriptor = updates.runtime_dependency_transition(old, candidate, {}, runtime_target=target)
    expected = descriptor["runtime_candidate_artifacts"]
    sdk = expected["sdk"]
    native = {"source": "meta-toolkit-product", "version": target.meta.version,
        "platform": selection.selected_runtime_platform(target), "assembly_sha256": target.assembly.sha256,
        "catalog_sha256": target.model_catalog.sha256, "sdk": sdk,
        "binaries": {"toolkit_gateway": {"binary_sha256": expected["native"]["binary_sha256"]}},
        **{name: value for name, value in expected["native"].items() if name != "binary_sha256"}}
    selected = descriptor["runtime_candidate"]
    assert updates._retained_product_proof(native, sdk, selected, expected, "/inert-destination")
    for name in ("assembly_sha256", "catalog_sha256", "resources_sha256", "prefix_inventory_sha256"):
        assert not updates._retained_product_proof({**native, name: "f" * 64}, sdk, selected, expected, "/inert-destination")
    different_sdk = deepcopy(sdk)
    different_sdk["meta"]["source_payload"] = []
    assert not updates._retained_product_proof({**native, "sdk": different_sdk}, different_sdk,
        selected, expected, "/inert-destination")


@pytest.mark.parametrize("value", ["catalog", "candidate"])
def test_nested_verified_product_metadata_cannot_change_under_retained_hash(product_crossing, value):
    from launchkit.native_models import model_registry_lines
    _, target, _, candidate, _, _ = product_crossing
    verified = selection.selected_runtime_artifacts(target)
    if value == "catalog":
        verified.catalog["models"][0]["id"] = "mutated-unverified-model"
    else:
        verified.candidate["binary"]["sha256"] = "f" * 64
    with pytest.raises(ValueError, match="metadata changed"):
        model_registry_lines(json.loads(candidate["blueprint.json"][0]), runtime_target=target,
            verified_artifacts=verified)


@pytest.fixture
def previous_product_crossing(product, legacy_crossing):
    _, _, _, previous, previous_report, old_report = legacy_crossing
    target = catalog_target(product)
    spec = json.loads(previous["blueprint.json"][0])
    source = [PackageFile(name, content.decode(), 0o600 if name == ".env.example" else mode)
        for name, (content, mode) in previous.items() if name != "SHA256SUMS"]
    generated = compiler.generate_runtime_refresh(spec, source, runtime_target=target)
    ordinary = compiler.generate_package(spec, runtime_target=target)
    baseline, current = inventory(generated), inventory(ordinary)
    retained = sorted(name for name in baseline.keys() | current.keys()
        if name != "SHA256SUMS" and baseline.get(name) != current.get(name))
    binding = {"build_id": old_report["build_id"], "source_digest": old_report["verification"]["digest"]}
    framework = {"kind": "host-framework-provenance", "version": 1, "blueprint_digest": fingerprint(spec),
        "basis": "runtime-refresh", "reason": "explicit-runtime-refresh", "previous": binding,
        "origin": binding, "baseline_digest": fingerprint(baseline),
        "current_compiler_baseline_digest": fingerprint(current), "retained_template_paths": retained,
        "runtime_candidate": selection.candidate_selection(target)}
    new_report = report(spec, framework, "inert-product-successor", "e" * 64)
    candidate = {item.path: (item.as_bytes(), 0o755 if item.mode & 0o111 else 0o644) for item in generated}
    for name, value in previous.items():
        if name.startswith("application/") or name == "requirements.application.txt":
            candidate[name] = value
    candidate["implementation-report.json"] = (json_bytes(new_report), 0o644)
    return product, target, previous, candidate, old_report, new_report


@pytest.mark.parametrize("defect", [None, "receipt-selection", "missing-selection", "previous-bytes"])
def test_original_publication_joins_signed_predecessor_to_admitted_old_source(
        previous_product_crossing, tmp_path, defect):
    _, target, previous, candidate, _, new_report = previous_product_crossing
    directory, destination = installation(tmp_path, previous_product_crossing)
    receipt = updates.read_receipt(directory)
    receipt["runtime_candidate"] = json.loads(previous["runtime-candidate.json"][0])
    updates.write_private(directory / "receipt.json", receipt)
    staged = updates.stage(directory, packed(candidate), new_report, runtime_target=target)
    assert staged["update"]["blockers"] == []
    update_id = staged["update"]["update_id"]
    root = updates.update_directory(directory, update_id)
    if defect == "receipt-selection":
        receipt["runtime_candidate"]["accepted"]["source_sha"] = "f" * 40
        updates.write_private(directory / "receipt.json", receipt)
    elif defect == "missing-selection":
        del receipt["runtime_candidate"]
        updates.write_private(directory / "receipt.json", receipt)
    elif defect == "previous-bytes":
        (root / "previous.zip").write_bytes(b"changed original admitted predecessor")
    before = (destination / "deployment.json").read_bytes()
    if defect is not None:
        with pytest.raises(updates.DraftConflict):
            updates.activate(directory, update_id)
        assert (destination / "deployment.json").read_bytes() == before
    else:
        updates.activate(directory, update_id)
        current = updates.read_receipt(directory)
        assert current["runtime_candidate"] == selection.candidate_selection(target)
        assert (destination / "deployment.json").read_bytes() == candidate["deployment.json"][0]


def test_closed_product_transition_keeps_original_json_and_source_history(
        previous_product_crossing, tmp_path, monkeypatch):
    _, target, previous, candidate, _, new_report = previous_product_crossing
    directory, destination = installation(tmp_path, previous_product_crossing)
    receipt = updates.read_receipt(directory)
    receipt["runtime_candidate"] = json.loads(previous["runtime-candidate.json"][0])
    updates.write_private(directory / "receipt.json", receipt)
    staged = updates.stage(directory, packed(candidate), new_report, runtime_target=target)
    identity = staged["update"]["update_id"]
    updates.activate(directory, identity)
    _, root, current = updates.transaction(directory, identity)
    descriptor = current["runtime_dependency_update"]
    old_descriptor = {"runtime_candidate": descriptor["previous_runtime_candidate"],
        "runtime_candidate_artifacts": descriptor["previous_runtime_candidate_artifacts"]}
    _, previous_native = candidate_proof(old_descriptor, destination)
    expected = descriptor["runtime_candidate_artifacts"]
    native = {"source": "meta-toolkit-product", "version": target.meta.version,
        "platform": selection.selected_runtime_platform(target), "assembly_sha256": target.assembly.sha256,
        "catalog_sha256": target.model_catalog.sha256, "sdk": expected["sdk"],
        "binaries": {"toolkit_gateway": {"binary_sha256": expected["native"]["binary_sha256"]}},
        **{name: value for name, value in expected["native"].items() if name != "binary_sha256"}}
    updates.checkpoint(directory, root, current, status="completed", blockers=[], rollback_available=False,
        runtime_admission_possible=True,
        runtime_files_retained={"format": "mobkit-launch-runtime-retention-v1", "retained": True,
            "inventory_sha256": "e" * 64, "destination": str(destination), "backup": str(root / "runtime-before"),
            "previous_native": previous_native},
        runtime_artifacts={"sdk": expected["sdk"], "native": native})

    def forbidden(*args, **kwargs):
        pytest.fail("Closed product history must not read current artifacts or generate later compiler guards")

    monkeypatch.setattr(compiler, "product_entrypoint", forbidden)
    monkeypatch.setattr(selection, "selected_runtime_artifacts", forbidden)
    _, _, historical = updates.transaction(directory, identity)
    assert historical["status"] == "completed" and historical["runtime_dependency_update"] == descriptor
    corrupted = deepcopy(historical)
    corrupted["runtime_artifacts"]["sdk"]["meta"]["source_payload"][0][1] = "f" * 64
    assert updates._completed_runtime_dependency_transition(previous, candidate,
        updates.read_receipt(directory), corrupted, root) is None


@pytest.fixture
def retained_candidate(tmp_path, product, monkeypatch):
    root, transaction = tmp_path / "inert installed", tmp_path / "inert transaction"
    root.mkdir(mode=0o700); transaction.mkdir(mode=0o700)
    bundle = Bundle(tmp_path / "inert predecessor artifacts")
    target = selection.RuntimeCandidateTarget(bundle.tuple_path, bundle.root, bundle.binding)
    inert_pair(root, bundle, target, candidate=True)
    original = selection.candidate_selection(target)
    previous = {"deployment.json": json_bytes({"mobkitVersion": "0.8.48", "target": "mac",
        "runtimeCandidate": selection.selected_runtime_marker(target)}),
        "runtime-candidate.json": json_bytes(original)}
    # Source is already the successor; retention must use the admitted old ZIP.
    (root / "deployment.json").write_bytes(json_bytes({"mobkitVersion": "0.9.0"}))
    (root / "runtime-candidate.json").write_bytes(json_bytes(selection.candidate_selection(product.target)))
    def stdlib_path(argv, **kwargs):
        assert argv[1:4] == ["-I", "-S", "-c"] and "sysconfig.get_paths" in argv[4]
        site = str(Path(argv[-1]) / "lib/python-fixture/site-packages")
        return subprocess.CompletedProcess(argv, 0, json.dumps({"purelib": site, "platlib": site}))
    monkeypatch.setattr(adapter.subprocess, "run", stdlib_path)
    monkeypatch.setattr(dependencies, "platform_target", lambda: PLATFORM)
    return root, transaction, bundle, target, product.target, previous


def test_retention_uses_original_candidate_source_even_after_activation_and_rechecks_moved_bytes(retained_candidate):
    root, transaction, _, old, new, previous = retained_candidate
    options = {"expected_version": "0.8.48", "runtime_target": old, "next_runtime_target": new,
        "previous_source": previous, "require_current": lambda: None}
    proof = dependencies.retain_runtime(root, transaction, **options)
    assert proof["previous_native"]["source"] == "signed-candidate"
    assert proof["previous_native"]["sdk"]["site_packages"].startswith(str(root / ".venv"))
    assert not (root / ".venv").exists() and not (root / ".bin").exists()
    assert dependencies.retain_runtime(root, transaction, **options) == proof
    (transaction / "runtime-before/.bin/rpc_gateway").write_bytes(b"changed predecessor bytes")
    with pytest.raises(dependencies.RuntimeDependencyError, match="inventory"):
        dependencies.retain_runtime(root, transaction, **options)


@pytest.mark.parametrize("change", ["selection", "sdk", "native", "owner"])
def test_changed_predecessor_or_owner_refuses_before_runtime_move(retained_candidate, change):
    root, transaction, _, old, new, previous = retained_candidate
    current = lambda: None
    if change == "selection":
        document = json.loads(previous["runtime-candidate.json"])
        document["accepted"]["tuple_sha256"] = "f" * 64
        previous["runtime-candidate.json"] = json_bytes(document)
    elif change == "sdk":
        (root / ".venv/lib/python-fixture/site-packages/meerkat_mobkit/__init__.py").write_bytes(b"changed bytes")
    elif change == "native":
        (root / ".bin/rpc_gateway").write_bytes(b"changed predecessor bytes")
    else:
        current = lambda: False
    with pytest.raises((dependencies.RuntimeDependencyError, adapter.CandidateArtifactError)):
        dependencies.retain_runtime(root, transaction, expected_version="0.8.48", runtime_target=old,
            next_runtime_target=new, previous_source=previous, require_current=current)
    assert (root / ".bin").is_dir() and (root / ".venv").is_dir()
    assert not (transaction / "runtime-retained.json").exists()
