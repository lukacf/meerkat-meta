"""Inert product initial-install/public-owner tests, not native qualification."""
import asyncio
from copy import deepcopy
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
from types import SimpleNamespace

import pytest

from launchkit import deployment, deployment_worker as worker
from launchkit import deployment_updates as updates, runtime_selection as selection
from launchkit import runtime_dependencies as dependencies
from launchkit import runtime_candidates as candidates
from launchkit.projects import DraftConflict
from launchkit.templates import install_candidate as adapter
from test_candidate_implementation import service as implementation_service, accepted_fixture
from test_implementation import workspace
from test_meta_toolkit_runtime_selection import product, no_product_execution
from test_meta_toolkit_runtime_update import catalog_target
from test_meta_toolkit_worker_update import InertExecutor


class AdmissionOnly(deployment.DeploymentService):
    def _launch(self, directory, mode, **kwargs):
        self.launched.append((directory, mode, kwargs))


@pytest.fixture(autouse=True)
def no_processes(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Initial source tests cannot execute processes or use the network.")
    for owner, name in ((subprocess, "run"), (subprocess, "Popen"), (os, "system"),
            (os, "kill"), (os, "killpg"), (socket, "create_connection"),
            (asyncio, "create_subprocess_exec"), (asyncio, "create_subprocess_shell")):
        monkeypatch.setattr(owner, name, forbidden)


@pytest.fixture
async def initial(implementation_service, product, tmp_path, monkeypatch):
    target = catalog_target(product)
    # These bytes model an Apple target even when pytest runs on Linux.
    selected_platform = selection.selected_runtime_platform(target)
    monkeypatch.setattr(dependencies, "platform_target", lambda: selected_platform)
    monkeypatch.setattr(deployment, "_local_host_target", lambda: "mac")
    implementation_service.guide.state["proposal"]["target"] = "mac"
    first = await implementation_service.submit(implementation_service.guide.identity, 3)
    project = workspace(implementation_service, first)
    definition = json.loads(project.read_file("deployment.json"))
    policy = {"provider_id": "application", "policy_id": "inert-initial-tools", "revision": 1,
        "members": [{"member_identity": row["identity"], "grants": []} for row in definition["agents"]]}
    project.write_file("application/tool-policy.json", json.dumps(policy) + "\n")
    await accepted_fixture(implementation_service, first)
    selected = await implementation_service.submit(implementation_service.guide.identity, 3,
        refresh_framework=True, preserve_design=True, expected_selected_build_id=first["build_id"], runtime_target=target)
    await accepted_fixture(implementation_service, selected)
    service = AdmissionOnly(tmp_path / "initial deployment owner", implementation_service.guide, implementation_service)
    service.launched = []
    async def capability():
        return {"available": True, "detail": "Inert service manager must not execute."}
    monkeypatch.setattr(service.service_manager, "capability", capability)
    monkeypatch.setattr(deployment, "require_available_console_port", lambda *args, **kwargs: None)
    monkeypatch.setattr(deployment, "observe_local_console_port", lambda *args, **kwargs: None)
    f = SimpleNamespace(service=service, implementation=implementation_service, target=target,
        guide_id=implementation_service.guide.identity, build_id=selected["build_id"], destination=tmp_path / "initial application")
    async def submit(**kwargs):
        return await service.submit(f.guide_id, 3, f.build_id, str(f.destination),
            settings={"OPENAI_API_KEY": "inert-not-a-provider-key"}, **kwargs)
    f.submit = submit
    yield f
    await implementation_service.shutdown()


async def test_real_protected_implementation_selection_reaches_original_initial_receipt(initial):
    f = initial
    options = await f.service.options(f.guide_id, f.build_id)
    assert options["fresh_install"] == {"available": True, "reason": None, "next_step": "configure"}
    background = next(row for row in options["execution_modes"] if row["id"] == "background")
    assert not background["available"] and background["reason"] == deployment.PRODUCT_CONFIGURE_REQUIRED
    receipt = await f.submit()
    assert receipt["runtime_candidate"] == selection.candidate_selection(f.target)
    assert receipt["status"] == "interrupted" and receipt["phase"] == "install"
    # This admission-only owner has no worker, so status cannot claim a live queued process.
    assert f.service.launched == [(f.service._directory(f.guide_id), "install", {})]
    assert not (f.destination / ".venv").exists() and not (f.destination / ".bin").exists()
    binding = f.service._configuration_binding(receipt)
    receipt["runtime_candidate"]["assembly"]["sha256"] = "f" * 64
    assert binding["runtime_candidate"] != receipt["runtime_candidate"]
    assert "runtime_candidate" not in f.service._configuration_binding({})


@pytest.mark.parametrize("change", ["missing-target", "changed-guide", "changed-build", "changed-source", "changed-artifact"])
async def test_final_owner_selection_refuses_before_initial_publication(initial, monkeypatch, change):
    f = initial
    reader = f.implementation.selected_runtime_target
    calls = 0
    def changed(*args, **kwargs):
        nonlocal calls
        calls += 1
        target = reader(*args, **kwargs)
        if calls == 2:
            if change == "missing-target": return None
            if change == "changed-guide": f.implementation.guide.state["version"] += 1
            elif change == "changed-build":
                path = f.implementation.root / f.guide_id / "latest.json"
                path.write_text(json.dumps({"build_id": "different-selected-build"}))
            elif change == "changed-source":
                path = f.implementation._directory(f.guide_id, f.build_id) / "workspace/project/application/inert.txt"
                path.write_text("changed retained authored source\n")
            else: target.assembly.path.write_bytes(b"changed exact selected assembly")
        return target
    monkeypatch.setattr(f.implementation, "selected_runtime_target", changed)
    with pytest.raises((DraftConflict, LookupError, ValueError)):
        await f.submit()
    assert not f.destination.exists() and not f.service.root.exists() and not f.service.launched


async def test_product_background_and_missing_dependency_refuse_before_publication(initial, monkeypatch):
    f = initial
    with pytest.raises(DraftConflict, match="Configure"):
        await f.submit(execution_mode="background")
    package = f.implementation.package
    async def incompatible(*args, **kwargs):
        payload, slug = await package(*args, **kwargs)
        files = deployment.package_files(payload)
        files["requirements.application.txt"] = (b"missing-application-dependency==1.2.3\n", 0o644)
        from test_candidate_deployment_updates import packed
        return packed(files), slug
    monkeypatch.setattr(f.implementation, "package", incompatible)
    with pytest.raises(ValueError, match="batch"):
        await f.submit()
    assert not f.destination.exists() and not f.service.root.exists() and not f.service.launched


@pytest.fixture
async def admitted(initial):
    f = initial
    f.receipt = await f.submit()
    f.directory = f.service._directory(f.guide_id)
    return f


@pytest.mark.parametrize("action", ["restart", "retry"])
async def test_public_product_restart_and_update_recovery_retry_do_not_queue_ordinary_start(admitted, action, monkeypatch):
    f = admitted
    deployment.materialize(f.directory)
    deployment.update_receipt(f.directory, status="stopped", phase="configure",
        **({"update": {"status": "candidate-restored-stopped"}} if action == "retry" else {}))
    if action == "retry":
        # Isolate the action guard from transaction inspection; this is not an admitted update fixture.
        async def retained_status(*args, **kwargs): return deployment.read_receipt(f.directory)
        monkeypatch.setattr(f.service, "status", retained_status)
    f.service.launched.clear()
    with pytest.raises(DraftConflict, match="Configure|original selected product update"):
        await f.service.action(f.guide_id, f.receipt["deployment_id"], action)
    assert not f.service.launched


async def test_public_background_product_restart_refuses_before_service_operations(admitted, monkeypatch):
    f = admitted
    deployment.update_receipt(f.directory, status="stopped", execution_mode="background", service={"inert": True})
    async def forbidden(*args, **kwargs): pytest.fail("Ordinary product restart reached the OS service owner.")
    monkeypatch.setattr(f.service.service_manager, "start", forbidden)
    monkeypatch.setattr(f.service.service_manager, "observe", forbidden)
    with pytest.raises(DraftConflict, match="Configure"):
        await f.service.action(f.guide_id, f.receipt["deployment_id"], "restart")


@pytest.fixture
async def worker_fixture(admitted, monkeypatch):
    f = admitted
    f.events = []
    f.executor = InertExecutor(f.destination, f.events)
    original_command = f.executor.command
    async def inert_command(argv, phase, timeout, **kwargs):
        if phase == "Installed application contract":
            assert argv == [str(f.destination / ".venv/bin/python"), "app.py", "--application-check"]
            f.events.append("application-check")
            f.executor.after_phase("application-check")
            return ""
        return await original_command(argv, phase, timeout, **kwargs)
    class InertSupervisor(worker.Supervisor):
        async def command(self, *args, **kwargs): return await inert_command(*args, **kwargs)
        async def install_selected_sdk_pair(self, *args, **kwargs):
            return await f.executor.install_selected_sdk_pair(*args, **kwargs)
        async def start(self, *args, **kwargs): pytest.fail("Initial product fell through to ordinary native startup.")
        async def terminate(self): f.events.append("terminated")
    monkeypatch.setattr(worker, "Supervisor", InertSupervisor)
    async def owner(supervisor):
        await supervisor.stop.wait()
        return "stopped"
    monkeypatch.setattr(worker, "owner_input", owner)
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda *args: None)
    def proof(destination, version, **kwargs):
        report_progress = kwargs.pop("report_progress")
        assert callable(report_progress)
        assert kwargs == {"runtime_target": f.target, "sdk_python": f.destination / ".venv/bin/python",
            "site_packages": f.executor.site}
        artifacts = selection.selected_runtime_artifacts(f.target)
        adapter.verify_meta_toolkit_prefix(artifacts, deployment_root=f.destination,
            report_progress=report_progress)
        f.events.append("byte-proof")
        return {"source": "meta-toolkit-product", "fixture": "inert-byte-proof-not-SDK-or-native-acceptance"}
    monkeypatch.setattr(dependencies, "verify_runtime", proof)
    return f


async def test_real_worker_initial_lane_installs_exact_prefix_and_settles_stopped_configure(worker_fixture):
    f = worker_fixture
    assert await worker.run(f.directory, "install") == 0
    receipt = deployment.read_receipt(f.directory)
    assert receipt["status"] == "stopped" and receipt["phase"] == "configure"
    assert receipt["runtime_candidate"] == selection.candidate_selection(f.target)
    assert receipt["console_url"] is None and receipt["runtime_pid"] is None
    assert receipt["installed_candidate_runtime_proof"]["fixture"] == "inert-byte-proof-not-SDK-or-native-acceptance"
    progress = [entry["message"] for entry in receipt["progress"]]
    assert "Preparing the application environment." in progress
    assert "Installing the agent software and its dependencies." in progress
    assert "Checking the installed runtime and application." in progress
    assert sum(message.startswith("Copying native runtime files: ") for message in progress) >= 2
    assert sum(message.startswith("Checking native runtime files: ") for message in progress) >= 4
    assert f.executor.sdk_calls[0][0] == (f.target.meta_wheel.path, f.target.toolkit_wheel.path)
    assert f.events == ["environment", "site", "sdk-pair", "byte-proof", "application-check", "terminated"]
    assert (f.destination / ".bin" / ("meerkat-connect-" + f.target.target) / "bin/toolkit_gateway").stat().st_mode & 0o777 == 0o755


def observe_initial_prefix(monkeypatch, after_write=lambda path: None):
    """Observe the real composition without replacing its fences or readers."""
    state = SimpleNamespace(active=False, boundaries=[], writes=[], settled=threading.Event())
    original_prefix = adapter.install_meta_toolkit_prefix
    original_recheck = candidates.recheck_meta_toolkit_artifacts
    original_write = adapter.atomic_write

    def recheck(*args, **kwargs):
        if state.active:
            assert state.boundaries, "An artifact observation needs its original prefix boundary."
            state.boundaries[-1] += 1
        return original_recheck(*args, **kwargs)

    def prefix(*args, **kwargs):
        current = kwargs["require_current"]
        def boundary():
            state.boundaries.append(0)
            return current()
        state.active = True
        try:
            return original_prefix(*args, **{**kwargs, "require_current": boundary})
        finally:
            state.active = False
            state.settled.set()

    def write(path, data, mode):
        original_write(path, data, mode)
        if state.active:
            state.writes.append(path)
            after_write(path)

    monkeypatch.setattr(candidates, "recheck_meta_toolkit_artifacts", recheck)
    monkeypatch.setattr(selection, "recheck_meta_toolkit_artifacts", recheck)
    monkeypatch.setattr(adapter, "install_meta_toolkit_prefix", prefix)
    monkeypatch.setattr(adapter, "atomic_write", write)
    return state


async def test_initial_prefix_observes_all_artifacts_once_at_each_original_boundary(worker_fixture, monkeypatch):
    f = worker_fixture
    observed = observe_initial_prefix(monkeypatch)
    assert await worker.run(f.directory, "install") == 0
    artifacts = selection.selected_runtime_artifacts(f.target)
    assert observed.settled.is_set()
    assert set(observed.writes) == {f.destination / ".bin" / entry.name
        for entry in artifacts.entries if entry.kind == "file"}
    assert observed.boundaries and set(observed.boundaries) == {1}
    adapter.verify_meta_toolkit_prefix(artifacts, deployment_root=f.destination)
    assert deployment.read_receipt(f.directory)["phase"] == "configure"


@pytest.mark.parametrize("changed", ["artifact", "source", "lease"])
async def test_initial_prefix_refuses_drift_between_writes_and_joins_worker(worker_fixture, monkeypatch, changed):
    f = worker_fixture
    writes = []
    def change(path):
        writes.append(path)
        if len(writes) != 1:
            return
        if changed == "artifact":
            original = f.target.toolkit_wheel.path
            replacement = original.with_name("replacement-inert-wheel")
            replacement.write_bytes(original.read_bytes())
            replacement.chmod(original.stat().st_mode & 0o777)
            replacement.replace(original)
        elif changed == "source":
            original = f.destination / "app.py"
            original.write_bytes(original.read_bytes() + b"# changed inert accepted source\n")
        else:
            original = f.destination / ".mobkit/launch.lock"
            original.unlink()
            original.write_bytes(b"changed inert project lease\n")
            original.chmod(0o600)
    observed = observe_initial_prefix(monkeypatch, change)
    assert await worker.run(f.directory, "install") in {None, 1}
    assert observed.settled.is_set() and len(writes) == 1
    assert observed.boundaries[:-1] and set(observed.boundaries[:-1]) == {1}
    # Artifact replacement is detected by the complete observation itself;
    # source/lease refusal happens before that observation or the next write.
    assert observed.boundaries[-1] == (1 if changed == "artifact" else 0)
    assert "byte-proof" not in f.events and "application-check" not in f.events
    assert f.events[-1] == "terminated"
    assert deployment.read_receipt(f.directory)["status"] == "failed"


@pytest.mark.parametrize("change", ["selection", "package", "settings", "owner", "lease", "update", "fresh", "replacement", "stop"])
async def test_original_initial_fence_prevents_next_sdk_or_prefix_mutation(worker_fixture, change):
    f = worker_fixture
    def after_phase(phase):
        if phase != "environment": return
        if change == "selection":
            selected = deepcopy(f.receipt["runtime_candidate"]); selected["assembly"]["sha256"] = "f" * 64
            deployment.update_receipt(f.directory, runtime_candidate=selected)
        elif change == "package": (f.directory / "package.zip").write_bytes(b"changed original package")
        elif change == "settings": (f.destination / ".env").write_text("OPENAI_API_KEY=changed-private-fixture\n")
        elif change == "owner": (f.destination / deployment.OWNER_FILE).write_text("{}")
        elif change == "update":
            deployment.update_receipt(f.directory, update={"status": "complete", "update_id": "inert-retained-owner"})
        elif change == "fresh":
            from launchkit import deployment_application_bootstrap as bootstrap
            path = f.directory / bootstrap.FRESH_ENTRY_NAME
            path.write_text("retained original attempt, not native qualification\n"); path.chmod(0o600)
        elif change == "replacement":
            from launchkit import deployment_application_startup as startup
            root = updates.update_directory(f.directory, "10000000-0000-4000-8000-000000000001")
            root.mkdir(parents=True, mode=0o700)
            path = root / startup.ENTRY_NAME
            path.write_text("retained original attempt, not native qualification\n"); path.chmod(0o600)
        elif change == "lease":
            path = f.destination / ".mobkit/launch.lock"
            path.unlink(); path.write_text("different owned lease\n"); path.chmod(0o600)
        else: f.executor.stop.set()
    f.executor.after_phase = after_phase
    # The executor and actual Supervisor must share the original stop owner.
    if change == "stop":
        original = worker._install_selected_product
        async def share_stop(supervisor, *args, **kwargs):
            f.executor.stop = supervisor.stop
            return await original(supervisor, *args, **kwargs)
        monkeypatch = pytest.MonkeyPatch(); monkeypatch.setattr(worker, "_install_selected_product", share_stop)
        try: result = await worker.run(f.directory, "install")
        finally: monkeypatch.undo()
    else: result = await worker.run(f.directory, "install")
    assert result in {None, 1}
    assert not f.executor.sdk_calls and not (f.destination / ".bin").exists()
    assert f.events[:1] == ["environment"]


@pytest.mark.parametrize("mode", ["start", "service"])
async def test_worker_product_ordinary_start_refuses_before_supervisor_or_service_entry(admitted, monkeypatch, mode):
    def forbidden(*args, **kwargs): pytest.fail("Unconfigured product entered a Supervisor.")
    monkeypatch.setattr(worker, "Supervisor", forbidden)
    with pytest.raises(DraftConflict, match="Configure"):
        await worker.run(admitted.directory, mode)


async def test_direct_supervisor_start_cannot_fall_back_to_app_py(admitted, monkeypatch):
    f = admitted
    files = deployment.materialize(f.directory)
    supervisor = worker.Supervisor(f.directory)
    monkeypatch.setattr(supervisor, "_verify_candidate_runtime", lambda *args: pytest.fail("Ordinary product startup reached SDK verification."))
    with pytest.raises(DraftConflict, match="Configure"):
        await supervisor.start(json.loads(files["deployment.json"][0]))


async def test_product_exact_architecture_mismatch_refuses_before_publication(initial, monkeypatch):
    monkeypatch.setattr(dependencies, "platform_target", lambda: "x86_64-unknown-linux-gnu")
    with pytest.raises(DraftConflict, match="exact local target"):
        await initial.submit()
    assert not initial.destination.exists() and not initial.service.root.exists()


async def test_public_initial_byte_retry_preserves_selection_and_only_queues_install(admitted):
    f = admitted
    deployment.materialize(f.directory)
    deployment.update_receipt(f.directory, status="stopped", phase="configure")
    f.service.launched.clear()
    await f.service.action(f.guide_id, f.receipt["deployment_id"], "retry")
    assert f.service.launched == [(f.directory, "install", {})]
    assert deployment.read_receipt(f.directory)["runtime_candidate"] == selection.candidate_selection(f.target)


async def test_product_byte_retry_does_not_replace_retained_fresh_custody(admitted):
    f = admitted
    deployment.materialize(f.directory)
    deployment.update_receipt(f.directory, status="stopped", phase="configure")
    from launchkit import deployment_application_bootstrap as bootstrap
    entry = f.directory / bootstrap.FRESH_ENTRY_NAME
    entry.write_text("retained original attempt, not native qualification\n"); entry.chmod(0o600)
    before = (f.destination / ".env").read_bytes()
    f.service.launched.clear()
    with pytest.raises(ValueError, match="Fresh bootstrap attempt"):
        await f.submit()
    assert (f.destination / ".env").read_bytes() == before and not f.service.launched


@pytest.mark.parametrize("when", ["before", "during-private-read"])
@pytest.mark.parametrize("owner", ["update", "replacement"])
async def test_same_product_submission_preserves_existing_owner_before_private_publication(admitted, monkeypatch, when, owner):
    f = admitted
    deployment.materialize(f.directory)
    deployment.update_receipt(f.directory, status="stopped", phase="configure")
    before = (f.destination / ".env").read_bytes()
    f.service.launched.clear()
    def retain_owner():
        # Only the original initial guard is exercised; this is not an admitted update fixture.
        if owner == "update":
            deployment.update_receipt(f.directory, update={"status": "complete", "update_id": "inert-retained-owner"})
        else:
            from launchkit import deployment_application_startup as startup
            root = updates.update_directory(f.directory, "10000000-0000-4000-8000-000000000001")
            root.mkdir(parents=True, mode=0o700)
            path = root / startup.ENTRY_NAME
            path.write_text("retained original attempt, not native qualification\n"); path.chmod(0o600)
    if when == "before":
        retain_owner()
        async def retained_status(*args, **kwargs): return deployment.read_receipt(f.directory)
        monkeypatch.setattr(f.service, "status", retained_status)
    else:
        original = deployment.prepare_private_settings
        def changed_during_read(*args, **kwargs):
            values = original(*args, **kwargs)
            retain_owner()
            return values
        monkeypatch.setattr(deployment, "prepare_private_settings", changed_during_read)
    with pytest.raises(DraftConflict, match="original selected product update|original installed product selection|retained native attempt"):
        await f.submit()
    current = deployment.read_receipt(f.directory)
    assert current["status"] == "stopped" and current["phase"] == "configure"
    if owner == "update": assert current["update"]["update_id"] == "inert-retained-owner"
    assert (f.destination / ".env").read_bytes() == before and not f.service.launched


async def test_public_initial_byte_retry_preserves_retained_replacement_owner_before_queue(admitted):
    from launchkit import deployment_application_startup as startup
    f = admitted
    deployment.materialize(f.directory)
    deployment.update_receipt(f.directory, status="stopped", phase="configure")
    root = updates.update_directory(f.directory, "10000000-0000-4000-8000-000000000001")
    root.mkdir(parents=True, mode=0o700)
    path = root / startup.ENTRY_NAME
    path.write_text("retained original attempt, not native qualification\n"); path.chmod(0o600)
    f.service.launched.clear()
    with pytest.raises(DraftConflict, match="retained native attempt"):
        await f.service.action(f.guide_id, f.receipt["deployment_id"], "retry")
    assert deployment.read_receipt(f.directory)["status"] == "stopped" and not f.service.launched


@pytest.mark.parametrize("change", ["missing", "changed-assembly", "changed-format"])
async def test_same_product_submit_cannot_rebase_changed_selection_during_private_read(admitted, monkeypatch, change):
    f = admitted
    deployment.materialize(f.directory)
    deployment.update_receipt(f.directory, status="stopped", phase="configure")
    before = (f.destination / ".env").read_bytes()
    f.service.launched.clear()
    original = deployment.prepare_private_settings
    def changed_during_read(*args, **kwargs):
        values = original(*args, **kwargs)
        current = deployment.read_receipt(f.directory)
        if change == "missing": current.pop("runtime_candidate")
        elif change == "changed-assembly": current["runtime_candidate"]["assembly"]["sha256"] = "f" * 64
        else: current["runtime_candidate"]["format"] = "changed-original-format"
        deployment.write_private(f.directory / "receipt.json", current)
        return values
    monkeypatch.setattr(deployment, "prepare_private_settings", changed_during_read)
    with pytest.raises(DraftConflict, match="original installed product selection"):
        await f.submit()
    current = deployment.read_receipt(f.directory)
    assert current["status"] == "stopped" and current["phase"] == "configure"
    assert (f.destination / ".env").read_bytes() == before and not f.service.launched


async def test_initial_worker_cannot_replace_existing_update_recovery(admitted, monkeypatch):
    deployment.update_receipt(admitted.directory, update={"status": "candidate-restored-stopped"})
    monkeypatch.setattr(worker, "Supervisor", lambda *args: pytest.fail("Update recovery entered initial Supervisor."))
    with pytest.raises(DraftConflict, match="original selected product update"):
        await worker.run(admitted.directory, "install")


async def test_initial_worker_cannot_replace_retained_fresh_custody(admitted, monkeypatch):
    from launchkit import deployment_application_bootstrap as bootstrap
    entry = admitted.directory / bootstrap.FRESH_ENTRY_NAME
    entry.write_text("retained original attempt, not native qualification\n"); entry.chmod(0o600)
    monkeypatch.setattr(worker, "Supervisor", lambda *args: pytest.fail("Retained Fresh entered initial Supervisor."))
    with pytest.raises(ValueError, match="Fresh bootstrap attempt"):
        await worker.run(admitted.directory, "install")


@pytest.mark.parametrize("mode", ["start", "service"])
async def test_original_queue_owner_refuses_ordinary_product_start_before_lifecycle_mutation(admitted, mode):
    f = admitted
    before = deployment.read_receipt(f.directory)
    with pytest.raises(DraftConflict, match="Configure"):
        deployment.DeploymentService._launch(f.service, f.directory, mode)
    assert deployment.read_receipt(f.directory) == before
    assert not f.service.tasks and not f.service.worker_admissions
