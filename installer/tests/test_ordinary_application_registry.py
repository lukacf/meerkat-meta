"""Normal helperless Build/Install associations with inert native boundaries.

Actual guide storage, reserved workspace, protected framework, accepted-package
readers and installation completion are exercised. Signed acceptance, worker
execution and gateway bytes are specimens. This is not native or UI acceptance.
"""
from copy import deepcopy
import asyncio
import hashlib
from types import SimpleNamespace
from uuid import uuid4

import pytest

from launchkit import deployment
from launchkit.connect_installer_caller import CustodyError, ExactFile
from launchkit.local_application_registry import LocalApplicationRegistry
from launchkit.ordinary_application_source import OrdinaryApplicationHost
from launchkit.projects import DraftConflict
from test_local_application_registry import (
    registry, product, no_product_execution, no_processes, new_guide, build,
    accept_source_fixture, install_fixture, workspace,
)


def ordinary_guide(f):
    saved = new_guide(f, ordinary=True)
    # Use the same exact model in this inert selected native catalog as the
    # product fixture. Never widen the production model catalog to fit a test.
    from test_meta_toolkit_initial_package import setup_spec
    model = setup_spec()["agent_classes"][0]
    for row in saved["proposal"]["agent_classes"]:
        row["provider"], row["model"] = model["provider"], model["model"]
    return f.store.save(saved, saved["version"])


@pytest.fixture
async def ordinary_registry(registry):
    f = registry
    directory = f.root / "selected-ordinary-gateway"
    directory.mkdir(mode=0o700)
    path = directory / "rpc_gateway"
    data = b"inert selected ordinary Meta RPC specimen, never executed\n"
    path.write_bytes(data)
    path.chmod(0o700)
    f.gateway = ExactFile(path, hashlib.sha256(data).hexdigest())
    f.registry = LocalApplicationRegistry(guide_store=f.store, implementation=f.implementation,
        deployment=f.service, runtime_target=f.target, ordinary_meta_gateway=f.gateway)
    f.implementation.prepare_workspace = f.registry.prepare_workspace
    for name, method in (("application_host_selection", "selected_host"),
            ("application_setup_retention", "retain_installation_inputs"),
            ("application_setup_selection", "_selected_startup"),
            ("application_setup_preparation", "prepare_application"),
            ("application_setup_continuation", "continue_application_startup")):
        setattr(f.service, name, getattr(f.registry, method))
    return f


async def test_reserved_helperless_build_protects_console_source_without_adding_agents(ordinary_registry):
    f = ordinary_registry
    saved = ordinary_guide(f)
    expected = deepcopy(saved["proposal"])
    result = await build(f, saved)
    host = f.registry.host_for_build(saved["id"], result["build_id"])
    assert type(host) is OrdinaryApplicationHost and host.gateway is f.gateway
    project = workspace(f.implementation, result)
    assert project.blueprint.model_dump(mode="json") == expected
    protected = project.framework_provenance()["protected_application_paths"]
    assert sorted(protected) == ["application/configuration.json", "application/console.json",
        "application/runtime-selection.json", "config/access.toml"]
    assert "local-password" in project.read_file("application/console.json")
    assert "forge-" + saved["id"] in project.read_file("config/access.toml")
    assert not (project.project / "application/tool-policy.json").exists()
    assert not (f.implementation._directory(saved["id"], result["build_id"]) / "local-application-source").exists()
    assert saved["proposal"].get("acquisition") is None


async def test_normal_install_completion_retains_ordinary_startup_and_console_operator(ordinary_registry, monkeypatch):
    f = ordinary_registry
    saved = ordinary_guide(f)
    result = await build(f, saved)
    await accept_source_fixture(f, result)
    directory, destination = await install_fixture(f, saved, result, monkeypatch)
    receipt = deployment.read_receipt(directory)
    host = f.registry.selected_host(saved["id"], receipt["deployment_id"])
    assert type(host) is OrdinaryApplicationHost
    selected = f.registry._selected_startup(saved["id"], receipt["deployment_id"])
    assert selected["kind"] == "ordinary" and selected["gateway"] == f.gateway.pin()
    assert selected["compiled_policies"] == []
    assert selected["definition"]["path"] == str(destination / "config/mob.toml")
    assert "preparation_plan" not in selected and "activation_template" not in selected
    assert not (destination / "current").exists() and not (destination / "shared").exists()
    assert not (directory / "application-bootstrap-entry.json").exists()
    status = await f.registry.setup_status(f.service, saved["id"], receipt["deployment_id"])
    assert status["available"] and status["mode"] == "ordinary"
    assert receipt["console_url"] is None and receipt["runtime_pid"] is None
    with pytest.raises(DraftConflict, match="Console user"):
        f.registry.validate_settings(saved["id"], {"CONSOLE_USER_ID": "another-user"}, build_id=result["build_id"])


async def test_original_ordinary_gateway_cannot_change_during_or_after_build(ordinary_registry):
    f = ordinary_registry
    saved = ordinary_guide(f)
    result = await build(f, saved)
    host = f.registry.host_for_build(saved["id"], result["build_id"])
    f.gateway.path.write_bytes(b"changed gateway bytes\n")
    with pytest.raises(CustodyError, match="ordinary Meta gateway"):
        host.require_current()
    with pytest.raises(CustodyError, match="ordinary Meta gateway"):
        f.registry.host_for_build(saved["id"], result["build_id"])


async def test_same_plan_continuation_retains_original_ordinary_console_source(ordinary_registry):
    f = ordinary_registry
    saved = ordinary_guide(f)
    first = await build(f, saved)
    await accept_source_fixture(f, first)
    original = f.registry.host_for_build(saved["id"], first["build_id"])
    continued = await build(f, saved, "Clarify the same requested local job.", previous=first)
    current = f.registry.host_for_build(saved["id"], continued["build_id"])
    assert current is not original and type(current) is OrdinaryApplicationHost
    assert current._protected == original._protected and current.gateway is original.gateway
    assert current._blueprint == original._blueprint


async def test_acquisition_plan_keeps_existing_toolkit_factory(ordinary_registry):
    f = ordinary_registry
    saved = new_guide(f)
    result = await build(f, saved)
    from launchkit.forge_application_host import ForgeApplicationHost
    assert type(f.registry.host_for_build(saved["id"], result["build_id"])) is ForgeApplicationHost


def reloaded_registry(f, *, gateway=None):
    from launchkit.guide_store import GuideStore
    from launchkit.guide import GuideService
    from launchkit.implementation import ImplementationService
    store = GuideStore(f.store.root)
    guide = GuideService(store, architect=None)
    implementation = ImplementationService(f.implementation.root.parent, guide)
    service = deployment.DeploymentService(f.service.root.parent, guide, implementation)
    owner = LocalApplicationRegistry(guide_store=store, implementation=implementation,
        deployment=service, runtime_target=f.target, ordinary_meta_gateway=f.gateway if gateway is None else gateway)
    implementation.prepare_workspace = owner.prepare_workspace
    for name, method in (("application_host_selection", "selected_host"),
            ("application_setup_retention", "retain_installation_inputs"),
            ("application_setup_selection", "_selected_startup"),
            ("application_setup_preparation", "prepare_application"),
            ("application_setup_continuation", "continue_application_startup")):
        setattr(service, name, getattr(owner, method))
    return store, implementation, service, owner


async def test_install_after_reload_recovers_signed_ordinary_build_without_new_source(ordinary_registry):
    f = ordinary_registry
    saved = ordinary_guide(f)
    result = await build(f, saved)
    await accept_source_fixture(f, result)
    original = f.registry.host_for_build(saved["id"], result["build_id"])
    store, implementation, service, owner = reloaded_registry(f)
    assert not owner._builds and not owner._installations
    owner.validate_settings(saved["id"], {"CONSOLE_USER_ID": "forge-" + saved["id"]}, build_id=result["build_id"])
    recovered = owner.host_for_build(saved["id"], result["build_id"])
    assert type(recovered) is OrdinaryApplicationHost and recovered is not original
    assert recovered._protected == original._protected and recovered.gateway is f.gateway
    assert not service.tasks and not implementation.tasks


async def test_installed_reload_recovers_original_source_while_guide_evolves(ordinary_registry, monkeypatch):
    f = ordinary_registry
    saved = ordinary_guide(f)
    result = await build(f, saved)
    await accept_source_fixture(f, result)
    directory, destination = await install_fixture(f, saved, result, monkeypatch)
    original = f.registry._selected_startup(saved["id"], directory.name)
    changed = f.store.get(saved["id"])
    changed["proposal"]["project"]["name"] += " revised draft"
    f.store.save(changed, changed["version"])
    store, implementation, service, owner = reloaded_registry(f)
    before = (directory / "receipt.json").read_bytes()
    status = await owner.setup_status(service, saved["id"], directory.name)
    assert status["reason"] == "owner_reconnect" and not owner._builds
    assert (directory / "receipt.json").read_bytes() == before
    async with service.lock:
        assert await owner._recover_installation(service, saved["id"], directory.name)
    assert owner._selected_startup(saved["id"], directory.name) == original
    assert (directory / "receipt.json").read_bytes() == before
    assert not (destination / "current").exists() and not (destination / "shared").exists()
    assert not service.tasks and not implementation.tasks


async def test_normal_ui_setup_action_queues_ordinary_source_without_toolkit_preparation(ordinary_registry, monkeypatch):
    f = ordinary_registry
    saved = ordinary_guide(f)
    result = await build(f, saved)
    await accept_source_fixture(f, result)
    directory, destination = await install_fixture(f, saved, result, monkeypatch)
    await f.service.action(saved["id"], directory.name, "finish-setup", action_request_id=str(uuid4()))
    assert f.service.launched[-1][1] == "start"
    pin = f.service.launched[-1][2]["bootstrap_selection"]
    from launchkit import deployment_application_bootstrap
    selected = deployment_application_bootstrap.read_bootstrap_admission(directory, pin)
    assert selected["application_startup"]["kind"] == "ordinary"
    assert selected["application_startup"]["gateway"] == f.gateway.pin()
    assert not (directory / "application-bootstrap-entry.json").exists()
    assert not (destination / "current").exists() and not (destination / "shared").exists()


async def test_normal_restart_joins_stopped_source_and_replays_no_worker(ordinary_registry, monkeypatch):
    f = ordinary_registry
    saved = ordinary_guide(f)
    result = await build(f, saved)
    await accept_source_fixture(f, result)
    directory, destination = await install_fixture(f, saved, result, monkeypatch)
    original = f.registry._selected_startup(saved["id"], directory.name)
    request = str(uuid4())
    before = len(f.service.launched)
    first = await f.service.action(saved["id"], directory.name, "restart", action_request_id=request)
    assert first["action_request"]["phase"] == "settled"
    assert first["action_request"]["outcome"] == "returned"
    assert len(f.service.launched) == before + 1
    assert f.service.launched[-1][1] == "start"
    from launchkit.deployment_application_bootstrap import read_bootstrap_admission
    selected = read_bootstrap_admission(directory, f.service.launched[-1][2]["bootstrap_selection"])
    assert selected["application_startup"] == original
    second = await f.service.action(saved["id"], directory.name, "restart", action_request_id=request)
    assert second["action_request"] == first["action_request"]
    assert len(f.service.launched) == before + 1
    assert not (directory / "application-bootstrap-entry.json").exists()
    assert not (destination / "current").exists() and not (destination / "shared").exists()


async def test_restart_after_forge_reload_recovers_original_installed_source(ordinary_registry, monkeypatch):
    f = ordinary_registry
    saved = ordinary_guide(f)
    result = await build(f, saved)
    await accept_source_fixture(f, result)
    directory, _ = await install_fixture(f, saved, result, monkeypatch)
    original = f.registry._selected_startup(saved["id"], directory.name)
    store, implementation, service, owner = reloaded_registry(f)
    launched = []
    monkeypatch.setattr(service, "_launch", lambda *args, **kwargs: launched.append((args, kwargs)))
    await service.action(saved["id"], directory.name, "restart", action_request_id=str(uuid4()))
    assert owner._selected_startup(saved["id"], directory.name) == original
    assert len(launched) == 1 and launched[0][0][1] == "start"
    assert not service.tasks and not service.processes


@pytest.mark.parametrize("cancelled", [False, True])
async def test_restart_waits_for_original_interactive_worker_cleanup(ordinary_registry, monkeypatch, cancelled):
    """Exercise the real Stop wait with a bounded, never-executed worker."""
    f = ordinary_registry
    saved = ordinary_guide(f)
    result = await build(f, saved)
    await accept_source_fixture(f, result)
    directory, _ = await install_fixture(f, saved, result, monkeypatch)
    command, release = asyncio.Event(), asyncio.Event()
    before = len(f.service.launched)
    deployment.update_receipt(directory, status="queued", phase="start")

    class Input:
        def write(self, raw):
            assert raw == b'{"action":"stop"}\n'
            command.set()
        async def drain(self):
            pass

    process = SimpleNamespace(pid=981237, returncode=None, stdin=Input())
    async def cleanup():
        await release.wait()
        process.returncode = 0
        deployment.update_receipt(directory, status="stopped", phase="configure",
            runtime_pid=None, process_live=False)
        f.service.processes.pop(directory.name)
        f.service.tasks.pop(directory.name)

    worker = asyncio.create_task(cleanup())
    f.service.processes[directory.name] = process
    f.service.tasks[directory.name] = worker
    request = str(uuid4())
    restarting = asyncio.create_task(f.service.action(saved["id"], directory.name,
        "restart", action_request_id=request))
    try:
        await asyncio.wait_for(command.wait(), 2)
        assert not restarting.done() and len(f.service.launched) == before
        assert f.service.lock.locked()
        if cancelled:
            restarting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await restarting
            replay = await f.service.action(saved["id"], directory.name, "restart", action_request_id=request)
            assert replay["action_request"]["phase"] == "pending"
            assert len(f.service.launched) == before
        release.set()
        await asyncio.wait_for(worker, 5)
        if not cancelled:
            await asyncio.wait_for(restarting, 5)
        assert len(f.service.launched) == before + (0 if cancelled else 1)
        assert process.returncode == 0
    finally:
        release.set()
        await asyncio.gather(worker, restarting, return_exceptions=True)


@pytest.mark.parametrize("fault", ["unclaimed", "missing-owner", "uncertain-cleanup", "changed-owner", "fresh-substitution"])
async def test_restart_refuses_uncertainty_or_substituted_owner(ordinary_registry, monkeypatch, fault):
    f = ordinary_registry
    saved = ordinary_guide(f)
    result = await build(f, saved)
    await accept_source_fixture(f, result)
    directory, _ = await install_fixture(f, saved, result, monkeypatch)
    before = len(f.service.launched)
    if fault == "unclaimed":
        with pytest.raises(ValueError, match="exact action request UUID"):
            await f.service.action(saved["id"], directory.name, "restart")
        assert len(f.service.launched) == before
        return
    if fault == "missing-owner":
        f.service.application_setup_selection = None
    elif fault == "uncertain-cleanup":
        receipt = deployment.read_receipt(directory)
        deployment.update_receipt(directory, interactive_cleanup={**receipt["interactive_cleanup"], "confirmed": False})
    elif fault == "changed-owner":
        continuation = f.service.application_setup_continuation
        async def changed(*args):
            await continuation(*args)
            f.service.application_setup_selection = lambda *_: None
        f.service.application_setup_continuation = changed
    elif fault == "fresh-substitution":
        # A minimal genuine Fresh selection is not needed: no foreign shape
        # may cross the original ordinary restart boundary at all.
        f.service.application_setup_selection = lambda *_: {"kind": "fresh"}
    with pytest.raises((DraftConflict, CustodyError), match="original|shape|Fresh|cleanup"):
        await f.service.action(saved["id"], directory.name, "restart", action_request_id=str(uuid4()))
    assert len(f.service.launched) == before


async def test_reload_cannot_substitute_even_identical_gateway_at_a_different_path(ordinary_registry):
    f = ordinary_registry
    saved = ordinary_guide(f)
    result = await build(f, saved)
    await accept_source_fixture(f, result)
    path = f.gateway.path.with_name("another-rpc")
    path.write_bytes(f.gateway.path.read_bytes())
    path.chmod(0o700)
    replacement = ExactFile(path, f.gateway.sha256)
    store, implementation, service, owner = reloaded_registry(f, gateway=replacement)
    with pytest.raises(CustodyError, match="original signed ordinary gateway"):
        owner.validate_settings(saved["id"], {}, build_id=result["build_id"])
    assert not owner._builds and not owner._installations and not service.tasks


async def test_builder_cannot_edit_or_remove_original_runtime_selection(ordinary_registry):
    f = ordinary_registry
    saved = ordinary_guide(f)
    result = await build(f, saved)
    project = workspace(f.implementation, result)
    original = project.read_file("application/runtime-selection.json")
    from launchkit.implementation_workspace import WorkspaceError
    with pytest.raises(WorkspaceError):
        project.write_file("application/runtime-selection.json", '{"gateway":"another"}\n')
    assert project.read_file("application/runtime-selection.json") == original


async def test_installed_recovery_checks_domain_source_against_complete_accepted_build(ordinary_registry, monkeypatch):
    f = ordinary_registry
    saved = ordinary_guide(f)
    result = await build(f, saved)
    await accept_source_fixture(f, result)
    directory, _ = await install_fixture(f, saved, result, monkeypatch)
    receipt = deployment.read_receipt(directory)
    from launchkit.deployment_updates import checked_package
    payload, package = checked_package(directory, "package.zip", receipt["package_sha256"])
    changed = dict(package)
    changed["application/inert.txt"] = (b"changed domain source with unchanged Console and claimed source digest\n", package["application/inert.txt"][1])
    # Keep the actual retained archive untouched. At the joined-file boundary,
    # supply a mismatched domain row while preserving original archive modes.
    import launchkit.ordinary_application_source as source
    original = source.checked_package
    def altered(path, name, digest):
        data, value = original(path, name, digest)
        return data, changed
    monkeypatch.setattr(source, "checked_package", altered)
    store, implementation, service, owner = reloaded_registry(f)
    with pytest.raises(CustodyError, match="independently accepted complete Build"):
        source.recover_ordinary_application_source(owner, saved["id"], result["build_id"], installed=(receipt, changed))
    assert (directory / "package.zip").read_bytes() == payload
    assert not owner._builds and not owner._installations and not service.tasks
