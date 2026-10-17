"""Inert integrity controls for the expressly unqualified development path."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import zipfile

import pytest

from launchkit import runtime_candidates as candidates
from test_meta_toolkit_runtime_selection import (product, no_product_execution,
    json_bytes, put, PREFIX)
from test_sdk_wheel_integrity import InertWheel
from test_distribution import bundle_source


def development(product, meta_version="0.9.0"):
    if meta_version != product.target.meta.version:
        meta = InertWheel("meerkat-meta", version=meta_version)
        meta.payload = dict(product.wheels[0].payload)
        toolkit = InertWheel("meerkat-toolkit", meta_version=meta_version)
        toolkit.payload = dict(product.wheels[1].payload)
        selected = [wheel.selected() for wheel in (meta, toolkit)]
        product.target = replace(product.target,
            meta_wheel=put(product.root / f"meerkat_meta-{meta_version}-py3-none-any.whl", selected[0][0]),
            meta=selected[0][1], toolkit_wheel=put(product.target.toolkit_wheel.path, selected[1][0]),
            toolkit=selected[1][1])
    old = product.assembly
    staging = {"schema_version": 1, "scope": "unqualified_product_prefix_for_verification",
        "target": product.target.target, "binary_kind": "toolkit_gateway", "prefix": PREFIX,
        "candidate": deepcopy(old["qualification"]["candidate"]),
        "build_record_sha256": old["build_record_sha256"],
        "resource_build_record_sha256": old["resource_build_record_sha256"],
        "resource_component_sha256": old["resource_component_sha256"],
        "assembler_sha256": "a" * 64}
    record = put(product.final / "staging.json", json_bytes(staging))
    descriptor = {"schema_version": 1, "scope": "source_built_development_gateway",
        "origin": "source-built-development", "target": product.target.target,
        "binary_kind": "toolkit_gateway", "candidate": deepcopy(staging["candidate"]),
        "staging_record_sha256": record.sha256,
        **{name: old[name] for name in ("build_record_sha256", "resource_build_record_sha256",
            "resource_component_sha256", "archive", "archive_sha256", "archive_bytes")},
        "model_catalog": {"sha256": product.target.model_catalog.sha256,
            "bytes": product.target.model_catalog.size},
        "sdk_wheels": {"meta_sha256": product.target.meta_wheel.sha256,
            "toolkit_sha256": product.target.toolkit_wheel.sha256}}
    target = replace(product.target, assembly=put(product.final / "development.json", json_bytes(descriptor)),
        source_manifest=product.source)
    return target, descriptor, staging


@pytest.mark.parametrize("meta_version", ["0.9.0", "0.9.1"])
def test_development_keeps_actual_prefix_and_source_without_inventing_verifier(product, meta_version):
    target, descriptor, _ = development(product, meta_version)
    (product.final / "gateway-verification.json").unlink()
    observed = candidates.read_meta_toolkit_artifacts(target)
    assert observed.origin == "source-built-development"
    assert observed.candidate == descriptor["candidate"]
    assert observed.archive == product.archive
    assert len(observed.entries) == len(product.nodes)
    assert not hasattr(observed, "qualified") and not hasattr(observed, "admitted")
    assert not any(item.path.name == "gateway-verification.json" for item in observed.evidence)
    assert candidates.recheck_meta_toolkit_artifacts(observed, target) is observed
    from launchkit.runtime_selection import candidate_selection, parse_candidate_selection
    serialized = candidate_selection(target, verified_artifacts=observed)
    assert serialized["meta"]["version"] == meta_version
    assert parse_candidate_selection(json.loads(json.dumps(serialized))) == target


@pytest.mark.parametrize("meta_version", ["0.9.0", "0.9.1"])
@pytest.mark.parametrize("defect", ["claim-final", "invent-qualification", "no-source", "source-bytes",
    "staging-candidate", "failed-build", "wrong-sdk", "selected-sdk-version", "changed-catalog", "changed-prefix"])
def test_development_cannot_replace_actual_owner_relationships(product, defect, meta_version):
    target, descriptor, staging = development(product, meta_version)
    if defect == "claim-final":
        descriptor["scope"] = "installed_gateway_schema4_resource_startup_and_shutdown_preview"
    elif defect == "invent-qualification":
        descriptor["qualification"] = {"status": "passed"}
    elif defect == "no-source":
        target = replace(target, source_manifest=None)
    elif defect == "source-bytes":
        product.source.path.write_bytes(json_bytes({"changed": True}))
    elif defect == "staging-candidate":
        staging["candidate"]["source_manifest_sha256"] = "f" * 64
        descriptor["staging_record_sha256"] = put(product.final / "staging.json", json_bytes(staging)).sha256
    elif defect == "failed-build":
        build = deepcopy(product.build)
        build["cargo"]["exit_code"] = 1
        changed = put(product.final / "build-record.json", json_bytes(build))
        descriptor["build_record_sha256"] = staging["build_record_sha256"] = changed.sha256
        descriptor["staging_record_sha256"] = put(product.final / "staging.json", json_bytes(staging)).sha256
    elif defect == "wrong-sdk":
        descriptor["sdk_wheels"]["meta_sha256"] = "f" * 64
    elif defect == "selected-sdk-version":
        wrong = "0.9.1" if meta_version == "0.9.0" else "0.9.0"
        target = replace(target, meta=replace(target.meta, version=wrong))
    elif defect == "changed-catalog":
        changed = put(target.model_catalog.path, json_bytes({"models": [], "changed": True}))
        target = replace(target, model_catalog=changed)
    else:
        product.archive.path.write_bytes(product.archive.path.read_bytes() + b"changed")
    target = replace(target, assembly=put(target.assembly.path, json_bytes(descriptor)))
    with pytest.raises(candidates.CandidateArtifactError):
        candidates.read_meta_toolkit_artifacts(target)


def test_final_path_still_requires_the_actual_installed_verifier(product):
    (product.final / "gateway-verification.json").unlink()
    with pytest.raises(candidates.CandidateArtifactError):
        candidates.read_meta_toolkit_artifacts(product.target)


def test_final_origin_retains_the_existing_source_only_fixture_limits(product):
    observed = candidates.read_meta_toolkit_artifacts(product.target)
    assert observed.origin == "installed-gateway-assembly"
    assert not hasattr(observed, "qualified")


def test_development_snapshot_rejects_changed_owned_input(product):
    target, _, _ = development(product)
    observed = candidates.read_meta_toolkit_artifacts(target)
    target.model_catalog.path.write_bytes(target.model_catalog.path.read_bytes() + b"changed")
    with pytest.raises(candidates.CandidateArtifactError):
        candidates.recheck_meta_toolkit_artifacts(observed, target)


def test_development_snapshot_cannot_relabel_origin(product):
    target, _, _ = development(product)
    observed = candidates.read_meta_toolkit_artifacts(target)
    object.__setattr__(observed, "origin", "installed-gateway-assembly")
    with pytest.raises(candidates.CandidateArtifactError):
        candidates.recheck_meta_toolkit_artifacts(observed, target)


@pytest.mark.parametrize("meta_version", ["0.9.0", "0.9.1"])
@pytest.mark.parametrize("defect", [None, "profile-type", "summary-drift"])
def test_development_catalog_uses_exact_full_meta_profiles(product, meta_version, defect):
    from launchkit.native_models import model_catalog_context

    summary = {"id": "inert-model", "provider": "inert-provider", "display_name": "Inert model",
        "tier": "standard", "context_window": 4096, "max_output_tokens": 1024}
    entry = {**summary, "profile": {"image_input": True, "image_tool_results": False}}
    if defect == "profile-type":
        entry["profile"]["image_input"] = "true"
    if defect == "summary-drift":
        summary["context_window"] += 1
    catalog = {"models": [entry], "provider_defaults": [{"provider": "inert-provider",
        "default_model_id": "inert-model", "models": [summary]}]}
    product.target = replace(product.target, model_catalog=put(product.target.model_catalog.path, json_bytes(catalog)))
    target, _, _ = development(product, meta_version)
    if defect is not None:
        with pytest.raises(ValueError):
            model_catalog_context(runtime_target=target)
        return
    observed = model_catalog_context(runtime_target=target)
    assert observed["status"] == "observed" and observed["native_version"] == meta_version
    assert observed["source"] == "selected_native_product_observed_catalog"
    assert observed["supplemental_models"] == []
    for model in [observed["models"][0], observed["provider_defaults"][0]["models"][0]]:
        assert model["vision"] is True and model["image_tool_results"] is False
    assert json.loads(target.model_catalog.path.read_bytes()) == catalog


@pytest.mark.parametrize("meta_version", ["0.9.0", "0.9.1"])
def test_development_distribution_keeps_guide_and_application_owners_separate(
        product, bundle_source, tmp_path, meta_version):
    from launchkit.bootstrap import SelectedGuideRuntime
    from launchkit.connect_installer_caller import ExactFile
    from launchkit import distribution, product_distribution, guide_sdk

    root, _, _ = bundle_source
    sdk = root / guide_sdk.WHEEL
    sdk.parent.mkdir(parents=True)
    sdk.write_bytes((Path(__file__).parents[1] / guide_sdk.WHEEL).read_bytes())
    for name in ("LICENSE-MIT", "LICENSE-APACHE"):
        (root / name).write_text("Inert public license.\n")
    gateway = put(product.root / "guide/rpc_gateway", b"inert guide, never execute\n")
    gateway.path.chmod(0o700)
    guide = SelectedGuideRuntime(ExactFile(gateway.path, gateway.sha256), "0.9.0", "0.9.0", "a" * 40, "b" * 40)
    target, _, _ = development(product, meta_version)
    output = tmp_path / "selected-product.zip"
    result = product_distribution.package(root, target, guide, output)
    assert result["native_executed"] is False and result["installed_use_accepted"] is False
    extracted = tmp_path / "selected-product"
    with zipfile.ZipFile(output) as archive:
        archive.extractall(extracted)
    observed = product_distribution.verify(extracted)
    assert observed["guide"]["version"] == observed["guide"]["core_version"] == "0.9.0"
    assert observed["product"]["selection"]["meta"]["version"] == meta_version
    assert observed["guide"]["gateway"]["sha256"] == gateway.sha256
    assert observed["guide"]["sdk"] == guide_sdk.selection()
    assert (extracted / observed["product"]["paths"]["meta_wheel"]).read_bytes() == target.meta_wheel.path.read_bytes()
    expected_clients = [guide_sdk.WHEEL, observed["product"]["paths"]["toolkit_wheel"]]
    assert distribution.sdk_wheels(extracted) == expected_clients
    assert distribution.bootstrap_inputs(extracted) == ["meta-toolkit", *expected_clients]
    readme = (extracted / "README.md").read_text()
    assert "The Forge guide uses Core 0.9.0 and Meta 0.9.0" in readme
    assert f"application uses the separately selected Meta {meta_version} and Toolkit" in readme
    # Each independently bound guide field still refuses drift. No target or
    # guide program runs, and the selected application bytes stay unchanged.
    manifest = extracted / distribution.MANIFEST
    original = manifest.read_bytes()
    for field, value in (("version", "0.9.1"), ("core_version", "0.9.1"),
            ("meta_source", "not-a-source-hash"), ("gateway", {**observed["guide"]["gateway"], "sha256": "f" * 64}),
            ("sdk", {**observed["guide"]["sdk"], "version": "0.9.0"})):
        changed = deepcopy(observed)
        changed["guide"][field] = value
        manifest.write_bytes(json_bytes(changed))
        with pytest.raises(distribution.DistributionError):
            product_distribution.verify(extracted)
        manifest.write_bytes(original)
    # A changed client wheel cannot be admitted by rehashing the outer manifest.
    wheel_copy = extracted / guide_sdk.WHEEL
    wheel_original = wheel_copy.read_bytes()
    changed_wheel = wheel_original[:-1] + bytes([wheel_original[-1] ^ 1])
    wheel_copy.write_bytes(changed_wheel)
    changed = deepcopy(observed)
    changed["files"][guide_sdk.WHEEL]["sha256"] = hashlib.sha256(changed_wheel).hexdigest()
    manifest.write_bytes(json_bytes(changed))
    with pytest.raises(distribution.DistributionError, match="reviewed wheel"):
        product_distribution.verify(extracted)
    wheel_copy.write_bytes(wheel_original)
    manifest.write_bytes(original)
    gateway_copy = extracted / "vendor/guide/rpc_gateway"
    gateway_copy.write_bytes(b"changed guide bytes\n")
    with pytest.raises(distribution.DistributionError, match="files changed"):
        product_distribution.verify(extracted)
    sdk.write_bytes(changed_wheel)
    refused_output = tmp_path / "changed-guide-sdk.zip"
    with pytest.raises(distribution.DistributionError, match="reviewed wheel"):
        product_distribution.package(root, target, guide, refused_output)
    assert not refused_output.exists()
