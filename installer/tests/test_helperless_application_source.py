"""Accepted native declarations through the real source owners, with inert I/O.

Source acceptance and native artifacts are explicit specimens. These controls
do not execute application code, install services or qualify native behavior.
"""
from copy import deepcopy
import json

import pytest

from launchkit import compiler
from launchkit.application_inventory_source import (
    INVENTORY_FIELD, INVENTORY_PATH, POLICY_PATH, application_inventory_source,
)
from launchkit.blueprint import Blueprint, starter_blueprint
from launchkit.connect_installer_caller import CustodyError, HELPERLESS_UPDATE_UNAVAILABLE
from launchkit.forge_application_host import ForgeApplicationHost, owner_choices_match
from launchkit.local_application_registry import LocalApplicationRegistry
from launchkit.local_application_recovery import bind_accepted_build_inventory, recover_local_build
from test_local_application_registry import (
    registry, product, no_product_execution, no_processes, build, workspace, accept_source_fixture,
)
from test_ordinary_application_registry import ordinary_guide


def declarations(spec):
    return [{"id": row.id, "profile": compiler.application_profiles(spec)[row.id],
        "factory": "application:" + row.id, "callbacks": [{"name": "read_context",
            "description": "Read this context", "input_schema": {"type": "object", "properties": {}}}],
        "grants": [{"tool_name": "read_context", "action": "invoke", "consequence": "r0"}]}
        for row in sorted(spec.agent_classes, key=lambda row: row.id) if row.instances == 0]


def source(*, empty_class=True):
    value = starter_blueprint().model_dump(mode="json")
    if empty_class:
        value["agent_classes"].append({**deepcopy(value["agent_classes"][0]),
            "id": "later", "instances": 0, "members": []})
    spec = Blueprint.model_validate(value)
    inventory = json.loads(compiler.application_inventory_input(spec, member_classes=declarations(spec)))
    files = {"blueprint.json": json.dumps(spec.model_dump(mode="json")).encode(),
        "deployment.json": json.dumps({INVENTORY_FIELD: INVENTORY_PATH}).encode(),
        INVENTORY_PATH: json.dumps(inventory, indent=3).encode() + b"\n"}
    baseline = {"provider_id": "forge:source", "policy_id": "source-tools", "revision": 1,
        "members": [{"member_identity": row["identity"], "grants": []} for row in inventory["roster"]]}
    files["application/tool-policy.json"] = json.dumps(baseline).encode()
    files[POLICY_PATH] = json.dumps(baseline, indent=2).encode() + b"\n"
    return spec, inventory, files


@pytest.mark.parametrize("empty_class", [False, True])
def test_exact_native_source_preserves_bytes_and_has_no_placeholder(empty_class):
    spec, inventory, files = source(empty_class=empty_class)
    before = deepcopy(files)
    assert application_inventory_source(files) == files[INVENTORY_PATH]
    assert files == before
    assert len(inventory["roster"]) == sum(row.instances for row in spec.agent_classes)
    assert all(not row["identity"].startswith("later:") for row in inventory["roster"])
    assert "helper_identity" not in inventory


@pytest.mark.parametrize("fault", ["missing", "path", "roster", "mob", "helper", "classes",
    "class-id", "profile", "duplicate", "undeclared", "duplicate-json"])
def test_source_refuses_redirects_and_incomplete_zero_instance_declarations(fault):
    _, inventory, files = source()
    if fault == "missing":
        del files[INVENTORY_PATH]
    elif fault == "path":
        files["deployment.json"] = json.dumps({INVENTORY_FIELD: "../foreign.json"}).encode()
    elif fault == "duplicate-json":
        files[INVENTORY_PATH] = b'{"schema":1,"schema":1}'
    else:
        if fault == "roster": inventory["roster"][0]["identity"] = "foreign"
        elif fault == "mob": inventory["mob_id"] = "foreign"
        elif fault == "helper": inventory["helper_identity"] = inventory["roster"][0]["identity"]
        elif fault == "classes": del inventory["member_classes"]
        elif fault == "class-id": inventory["member_classes"][0]["id"] = "foreign"
        elif fault == "profile": inventory["member_classes"][0]["profile"] = "foreign"
        elif fault == "duplicate": inventory["member_classes"] *= 2
        elif fault == "undeclared": inventory["member_classes"] = []
        files[INVENTORY_PATH] = json.dumps(inventory).encode()
    with pytest.raises((CustodyError, ValueError)):
        application_inventory_source(files)


@pytest.mark.parametrize("number", [-(1 << 63), (1 << 64) - 1, 0, 1.25])
def test_native_representable_schema_numbers_are_retained(number):
    _, inventory, files = source()
    inventory["member_classes"][0]["callbacks"][0]["input_schema"]["minimum"] = number
    files[INVENTORY_PATH] = json.dumps(inventory).encode()
    assert application_inventory_source(files) == files[INVENTORY_PATH]


@pytest.mark.parametrize("number", [-(1 << 63) - 1, 1 << 64, float("inf")])
def test_native_lossy_schema_numbers_are_refused(number):
    _, inventory, files = source()
    inventory["member_classes"][0]["callbacks"][0]["input_schema"]["minimum"] = number
    files[INVENTORY_PATH] = json.dumps(inventory).encode()
    with pytest.raises((CustodyError, ValueError)):
        application_inventory_source(files)


def test_application_file_cannot_switch_a_legacy_package_to_connect():
    _, _, files = source()
    files["deployment.json"] = b"{}"
    assert application_inventory_source(files) is None


@pytest.mark.parametrize("fault", ["missing", "provider_id", "policy_id", "revision", "member", "duplicate", "action"])
def test_fixed_seed_policy_is_explicit_and_preserves_original_owner(fault):
    _, _, files = source()
    selected = json.loads(files[POLICY_PATH])
    if fault == "missing":
        del files[POLICY_PATH]
    else:
        if fault in {"provider_id", "policy_id"}: selected[fault] = "foreign"
        elif fault == "revision": selected["revision"] += 1
        elif fault == "member": selected["members"][0]["member_identity"] = "foreign"
        elif fault == "duplicate": selected["members"].append(deepcopy(selected["members"][0]))
        elif fault == "action": selected["members"][0]["grants"] = [{"tool_name": "read_context", "action": "grant_all", "consequence": "r0"}]
        files[POLICY_PATH] = json.dumps(selected).encode()
    with pytest.raises((CustodyError, ValueError)):
        application_inventory_source(files)


@pytest.mark.parametrize("fault", [None, "classes", "policy", "roster"])
def test_bootstrap_plan_joins_accepted_classes_and_seed_policy(tmp_path, fault):
    from launchkit.deployment_application_bootstrap import _plan_source
    from launchkit.connect_policy_input import _compiler_input
    spec, inventory, files = source()
    files["config/mob.toml"] = compiler.mob_toml(spec).encode()
    files["deployment.json"] = json.dumps({INVENTORY_FIELD: INVENTORY_PATH,
        "agents": inventory["roster"]}).encode()
    # Explicit byte-only native-member specimen, never native admission.
    members = [{"member": {"mob_id": spec.project.slug, "member": "native-" + str(index), "role": row["profile"]}}
        for index, row in enumerate(inventory["roster"])]
    policy = json.loads(files[POLICY_PATH])
    for index, row in enumerate(policy["members"]):
        row["member_identity"] = members[index]["member"]["member"]
    application = {"realm": {"context_root": str(tmp_path), "state_root": str(tmp_path / ".mobkit/state")},
        "definition": {"format": "toml", "text": files["config/mob.toml"].decode()},
        "roster": deepcopy(inventory["roster"]), "members": members,
        "member_classes": deepcopy(inventory["member_classes"]),
        "tool_policy": json.loads(_compiler_input(policy, files[POLICY_PATH], "accepted-source"))}
    plan = {"deployment_root": str(tmp_path), "application": application}
    if fault == "classes": application["member_classes"][0]["factory"] = "foreign"
    elif fault == "policy": application["tool_policy"]["members"][0]["grants"] = [{"tool_name": "foreign", "action": "invoke", "consequence": "r0"}]
    elif fault == "roster": application["roster"][0]["labels"] = {"foreign": "selection"}
    package = {name: (content, 0o644) for name, content in files.items()}
    if fault is None:
        _plan_source(plan, package, tmp_path)
    else:
        with pytest.raises(CustodyError):
            _plan_source(plan, package, tmp_path)


def authored_inventory(f, result):
    project = workspace(f.implementation, result)
    spec = project.blueprint
    value = json.loads(project.read_file("native-inventory.example.json"))
    value["member_classes"] = declarations(spec)
    content = json.dumps(value, indent=3) + "\n"
    project.write_file(INVENTORY_PATH, content)
    policy = json.loads(project.read_file("application/tool-policy.json"))
    policy["members"][0]["grants"] = [{"tool_name": "read_context", "action": "invoke", "consequence": "r0"}]
    project.write_file(POLICY_PATH, json.dumps(policy, indent=3) + "\n")
    return content.encode()


async def helperless_build(f, *, empty_class=True):
    saved = ordinary_guide(f)
    if empty_class:
        value = saved["proposal"]
        value["agent_classes"].append({**deepcopy(value["agent_classes"][0]),
            "id": "later", "instances": 0, "members": []})
        saved["proposal"] = Blueprint.model_validate(value).model_dump(mode="json")
        saved = f.store.save(saved, saved["version"])
    result = await build(f, saved)
    return saved, result


@pytest.mark.parametrize("empty_class", [False, True])
async def test_default_build_and_install_bind_exact_authored_source_without_ordinary_gateway(registry, empty_class):
    f = registry
    saved, result = await helperless_build(f, empty_class=empty_class)
    host = f.registry.host_for_build(saved["id"], result["build_id"])
    assert type(host) is ForgeApplicationHost and f.registry.ordinary_meta_gateway is None
    original_pins = {pin: pin.read() for pin in host._pins}
    original_inventory = host._inventory_pin
    assert host._accepted_inventory_pin is None
    content = authored_inventory(f, result)
    await accept_source_fixture(f, result)
    from launchkit.deployment import package_files
    payload, _ = await f.implementation.package(saved["id"], result["build_id"])
    files = package_files(payload)
    assert files[INVENTORY_PATH][0] == content
    assert not owner_choices_match(host, files)  # No read may bind or publish.
    assert host._inventory_pin is original_inventory
    private = f.implementation._directory(saved["id"], result["build_id"]) / "accepted-application-inventory.json"
    assert not private.exists()
    f.registry.validate_settings(saved["id"], {"CONSOLE_USER_ID": "forge-" + saved["id"]}, build_id=result["build_id"])
    assert host is f.registry.host_for_build(saved["id"], result["build_id"])
    assert host._accepted_inventory_pin.read() == content and host._inventory_pin.path == private
    assert host._policy_source_path == POLICY_PATH
    assert host._policy_bytes == files[POLICY_PATH][0]
    assert host._policy["members"][0]["grants"][0]["tool_name"] == "read_context"
    assert owner_choices_match(host, files)
    assert original_pins == {pin: pin.read() for pin in original_pins}
    first_pin = host._inventory_pin
    f.registry.validate_settings(saved["id"], {"CONSOLE_USER_ID": "forge-" + saved["id"]}, build_id=result["build_id"])
    assert host._inventory_pin is first_pin
    assert not f.service.launched and not f.service.processes


async def test_missing_authored_inventory_cannot_be_packaged_despite_synthetic_acceptance(registry):
    saved, result = await helperless_build(registry)
    await accept_source_fixture(registry, result)
    with pytest.raises(CustodyError, match="missing its native inventory"):
        await registry.implementation.package(saved["id"], result["build_id"])


@pytest.mark.parametrize("changed", ["inventory", "policy"])
async def test_recovery_uses_same_private_inventory_and_refuses_changed_accepted_bytes(registry, changed):
    f = registry
    saved, result = await helperless_build(f)
    content = authored_inventory(f, result)
    await accept_source_fixture(f, result)
    host = f.registry.host_for_build(saved["id"], result["build_id"])
    bind_accepted_build_inventory(f.registry, host, saved["id"], result["build_id"], publish=True)
    other = LocalApplicationRegistry(guide_store=f.store, implementation=f.implementation,
        deployment=f.service, runtime_target=f.target)
    recovered = recover_local_build(other, saved["id"], result["build_id"])
    assert recovered[3] is not host
    assert recovered[3]._inventory_pin == host._inventory_pin
    assert recovered[3]._inventory_pin.read() == content
    pin = host._inventory_pin if changed == "inventory" else host._accepted_policy_pin
    pin.path.write_bytes(pin.read().replace(b"read_context", b"wrong_action"))
    with pytest.raises(CustodyError, match="exact accepted"):
        recover_local_build(other, saved["id"], result["build_id"])


async def test_interrupted_source_copy_resumes_only_during_explicit_install(registry, monkeypatch):
    from launchkit import connect_application_finalizer
    f = registry
    saved, result = await helperless_build(f)
    content = authored_inventory(f, result)
    await accept_source_fixture(f, result)
    host = f.registry.host_for_build(saved["id"], result["build_id"])
    original_write = connect_application_finalizer._write_bytes

    def interrupt_policy(path, data):
        if path.name == "accepted-application-tool-policy.json":
            raise OSError("Interrupted before the policy copy")
        original_write(path, data)

    with monkeypatch.context() as interrupted:
        interrupted.setattr(connect_application_finalizer, "_write_bytes", interrupt_policy)
        with pytest.raises(OSError, match="Interrupted"):
            bind_accepted_build_inventory(f.registry, host, saved["id"], result["build_id"], publish=True)
    directory = f.implementation._directory(saved["id"], result["build_id"])
    inventory_path = directory / "accepted-application-inventory.json"
    policy_path = directory / "accepted-application-tool-policy.json"
    assert inventory_path.read_bytes() == content and not policy_path.exists()
    other = LocalApplicationRegistry(guide_store=f.store, implementation=f.implementation,
        deployment=f.service, runtime_target=f.target)
    recovered = recover_local_build(other, saved["id"], result["build_id"])[3]
    assert recovered._accepted_inventory_pin is None and not policy_path.exists()
    bind_accepted_build_inventory(other, recovered, saved["id"], result["build_id"], publish=True)
    assert recovered._inventory_pin.read() == content
    assert recovered._accepted_policy_pin.path == policy_path


async def test_stage_shows_helperless_update_limit_from_installed_source(registry):
    f = registry
    saved, result = await helperless_build(f)
    authored_inventory(f, result)
    await accept_source_fixture(f, result)
    from launchkit.deployment import package_files
    from launchkit.deployment_updates import compatibility
    payload, _ = await f.implementation.package(saved["id"], result["build_id"])
    files = package_files(payload)
    changes, blockers = compatibility(files, files, {}, runtime_target=f.target)
    assert changes == [] and HELPERLESS_UPDATE_UNAVAILABLE in blockers
