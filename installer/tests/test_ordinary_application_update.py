"""In-place source updates of an application without a setup helper.

The launcher's registry callbacks are bound, as every product launcher binds
them. Build, acceptance, Install, Stage, Apply admission and the update worker
entry are real. Worker commands and the application child are inert seams:
no application, provider or native runtime executes here.
"""
import json
from uuid import uuid4

import pytest

from launchkit import deployment, deployment_application_bootstrap as bootstrap
from launchkit import deployment_updates as updates, deployment_worker as worker
from launchkit.connect_installer_caller import CustodyError
from test_ordinary_application_registry import (  # noqa: F401 (fixtures)
    ordinary_registry, registry, product, no_product_execution, no_processes,
    ordinary_guide, build, accept_source_fixture, install_fixture, staged_improvement,
    reloaded_registry,
)


INERT_WORKER_PID = 981237


async def installed(f, monkeypatch):
    """A helperless application installed through the launcher's registry."""
    assert f.service.application_replacement_selection == f.registry._selected_replacement
    saved = ordinary_guide(f)
    first = await build(f, saved)
    await accept_source_fixture(f, first)
    directory, destination = await install_fixture(f, saved, first, monkeypatch)
    f.saved, f.first, f.guide_id = saved, first, saved["id"]
    f.directory, f.destination = directory, destination
    f.original = f.registry._selected_startup(saved["id"], directory.name)
    return f


async def staged_continuation(f):
    staged = await staged_improvement(f, f.saved, f.first, f.directory)
    update = staged["update"]
    assert update["status"] == "staged" and update["blockers"] == []
    return update


async def apply(f, update):
    """Apply as the browser sends it, with its own action request UUID."""
    f.service.launched.clear()
    applied = await f.service.action(f.guide_id, f.directory.name, "apply-update", update["update_id"],
        action_request_id=str(uuid4()))
    assert applied["action_request"]["outcome"] == "returned", applied["action_request"]
    directory, mode, options = f.service.launched[-1]
    assert (directory, mode) == (f.directory, "update")
    return options["update_selection"]


async def run_worker(f, monkeypatch, mode, selection, *, failing_builds=()):
    """The real interactive worker entry. It never starts a helperless product application."""
    class UpdateWorker(worker.Supervisor):
        checks, starts = [], []

        async def command(self, args, *other, **kwargs):
            build_id = deployment.read_receipt(self.directory)["build_id"]
            UpdateWorker.checks.append((build_id, args[-1]))
            if build_id in failing_builds:
                raise RuntimeError("The inert application contract check failed.")

        async def start(self, *args, **kwargs):
            UpdateWorker.starts.append(args)
            raise AssertionError("The update worker never starts a helperless product application.")

    async def owned_input(supervisor):
        await supervisor.stop.wait()
        return "stopped"

    lifecycle = str(uuid4())
    deployment.update_receipt(f.directory, interactive_lifecycle_id=lifecycle,
        interactive_cleanup_attempt_id=None, interactive_cleanup=None)
    with monkeypatch.context() as patch:
        patch.setattr(worker, "Supervisor", UpdateWorker)
        patch.setattr(worker, "owner_input", owned_input)
        UpdateWorker.result = await deployment.interactive_worker(f.directory, mode, lifecycle,
            selection["path"], selection["sha256"])
    # No worker process runs here; the fixture's inert process stands for the
    # one whose exit the fixed wrapper confirms.
    cleanup = deployment.read_receipt(f.directory)["interactive_cleanup"]
    deployment.update_receipt(f.directory, interactive_cleanup={**cleanup,
        "supervisor_pid": INERT_WORKER_PID, "worker_exit_confirmed": True})
    return UpdateWorker


async def start(f, service=None):
    """The user's next Start: the owner's ordinary startup with the installed source."""
    service = f.service if service is None else service
    service.launched.clear()
    started = await service.action(f.guide_id, f.directory.name, "restart", action_request_id=str(uuid4()))
    assert started["action_request"]["outcome"] == "returned", started["action_request"]
    directory, mode, options = service.launched[-1]
    assert (directory, mode) == (f.directory, "start")
    admitted = bootstrap.read_bootstrap_admission(directory, options["bootstrap_selection"])
    assert admitted["application_startup"]["kind"] == "ordinary"
    # The inert launch never ran an application; it stays stopped.
    deployment.update_receipt(f.directory, status="stopped")
    return admitted


async def test_helperless_application_updates_in_place_and_its_next_start_runs_the_new_source(
        ordinary_registry, monkeypatch):
    f = await installed(ordinary_registry, monkeypatch)
    update = await staged_continuation(f)
    worker_type = await run_worker(f, monkeypatch, "update", await apply(f, update))
    assert worker_type.result is None
    assert worker_type.starts == [] and worker_type.checks == [(update["to_build_id"], "--application-check")]
    receipt = deployment.read_receipt(f.directory)
    assert receipt["status"] == "stopped" and receipt["build_id"] == update["to_build_id"]
    assert receipt["update"]["status"] == "completed" and receipt["update"]["rollback_available"] is True
    assert "The application is stopped. Its next Start runs this source." in receipt["update"]["detail"]
    admitted = await start(f)
    assert admitted["installation"]["build_id"] == update["to_build_id"]
    assert admitted["application_startup"] == f.original
    status = await f.registry.setup_status(f.service, f.guide_id, f.directory.name)
    assert status["available"] and status["kind"] == "ordinary" and status["build_id"] == update["to_build_id"]
    # A later Start keeps the same owner and inputs.
    assert (await start(f))["application_startup"] == f.original


async def test_second_helperless_update_chains_from_the_first(ordinary_registry, monkeypatch):
    f = await installed(ordinary_registry, monkeypatch)
    first = await staged_continuation(f)
    await run_worker(f, monkeypatch, "update", await apply(f, first))
    second_build = await build(f, f.saved, "Improve the requested local job again.",
        previous={"build_id": first["to_build_id"]})
    await accept_source_fixture(f, second_build)
    staged = await f.service.stage_update(f.guide_id, f.directory.name, f.saved["version"], second_build["build_id"])
    second = staged["update"]
    assert second["status"] == "staged" and second["blockers"] == []
    await run_worker(f, monkeypatch, "update", await apply(f, second))
    assert deployment.read_receipt(f.directory)["build_id"] == second_build["build_id"]
    admitted = await start(f)
    assert admitted["installation"]["build_id"] == second_build["build_id"]
    assert admitted["application_startup"] == f.original


async def test_after_a_forge_restart_the_updated_helperless_application_starts(ordinary_registry, monkeypatch):
    f = await installed(ordinary_registry, monkeypatch)
    update = await staged_continuation(f)
    await run_worker(f, monkeypatch, "update", await apply(f, update))
    store, implementation, service, owner = reloaded_registry(f)
    service.launched = []
    monkeypatch.setattr(service, "_launch",
        lambda directory, mode, **kwargs: service.launched.append((directory, mode, kwargs)))
    try:
        admitted = await start(f, service)
        assert admitted["installation"]["build_id"] == update["to_build_id"]
        assert admitted["application_startup"] == f.original
    finally:
        await implementation.shutdown()


async def test_after_a_forge_restart_apply_waits_for_the_reconnected_helperless_owner(
        ordinary_registry, monkeypatch):
    f = await installed(ordinary_registry, monkeypatch)
    update = await staged_continuation(f)
    await f.implementation.shutdown()
    store, implementation, service, owner = reloaded_registry(f)
    service.launched = []
    monkeypatch.setattr(service, "_launch",
        lambda directory, mode, **kwargs: service.launched.append((directory, mode, kwargs)))
    try:
        # Before the owner reconnects, Apply refuses before any worker.
        with pytest.raises(CustodyError, match="needs its retained original host"):
            await service.action(f.guide_id, f.directory.name, "apply-update", update["update_id"],
                action_request_id=str(uuid4()))
        assert not service.launched
        # Start reconnects the owner; then Apply admits the same staged update.
        await start(f, service)
        applied = await service.action(f.guide_id, f.directory.name, "apply-update", update["update_id"],
            action_request_id=str(uuid4()))
        assert applied["action_request"]["outcome"] == "returned", applied["action_request"]
        assert service.launched[-1][1] == "update"
    finally:
        await implementation.shutdown()


async def test_restore_previous_returns_a_helperless_application_to_its_previous_source(
        ordinary_registry, monkeypatch):
    f = await installed(ordinary_registry, monkeypatch)
    update = await staged_continuation(f)
    await run_worker(f, monkeypatch, "update", await apply(f, update))
    f.service.launched.clear()
    await f.service.action(f.guide_id, f.directory.name, "rollback", update["update_id"],
        action_request_id=str(uuid4()))
    directory, mode, options = f.service.launched[-1]
    assert (directory, mode) == (f.directory, "rollback")
    worker_type = await run_worker(f, monkeypatch, "rollback", options["update_selection"])
    assert worker_type.starts == [] and worker_type.checks == [(f.first["build_id"], "--application-check")]
    receipt = deployment.read_receipt(f.directory)
    assert receipt["update"]["status"] == "rolled-back" and receipt["build_id"] == f.first["build_id"]
    admitted = await start(f)
    assert admitted["installation"]["build_id"] == f.first["build_id"]
    assert admitted["application_startup"] == f.original


async def test_failed_contract_check_restores_the_previous_helperless_source_without_a_start(
        ordinary_registry, monkeypatch):
    f = await installed(ordinary_registry, monkeypatch)
    update = await staged_continuation(f)
    worker_type = await run_worker(f, monkeypatch, "update", await apply(f, update),
        failing_builds={update["to_build_id"]})
    assert worker_type.starts == []
    assert worker_type.checks == [(update["to_build_id"], "--application-check"),
        (f.first["build_id"], "--application-check")]
    receipt = deployment.read_receipt(f.directory)
    assert receipt["status"] == "stopped" and receipt["build_id"] == f.first["build_id"]
    assert receipt["update"]["status"] == "rolled-back"
    admitted = await start(f)
    assert admitted["installation"]["build_id"] == f.first["build_id"]


async def test_stage_blocks_a_helperless_candidate_from_another_source(ordinary_registry, monkeypatch):
    f = await installed(ordinary_registry, monkeypatch)
    changed = f.store.get(f.guide_id)
    changed["proposal"]["project"]["name"] += " with a changed plan"
    changed = f.store.save(changed, changed["version"])
    other = await build(f, changed, "Implement the changed plan.")
    await accept_source_fixture(f, other)
    staged = await f.service.stage_update(f.guide_id, f.directory.name, changed["version"], other["build_id"])
    update = staged["update"]
    assert update["status"] == "blocked"
    assert any("not built from the installed application's own source" in row for row in update["blockers"])
    f.service.launched.clear()
    # Apply checks the owner's choices again before any worker.
    with pytest.raises(CustodyError, match="not built from the installed application's own source"):
        await f.service.action(f.guide_id, f.directory.name, "apply-update", update["update_id"],
            action_request_id=str(uuid4()))
    assert not f.service.launched
    assert deployment.read_receipt(f.directory)["build_id"] == f.first["build_id"]


async def test_stage_blocks_a_helperless_update_that_changes_native_instructions(tmp_path, monkeypatch):
    """Without a setup helper, Stage blocks an update that changes native inputs."""
    from test_deployment_updates import prepared
    service, directory, _, _, _ = await prepared(tmp_path)
    try:
        payload, _ = await service.implementation.package(service.guide.identity, service.implementation.identity)
        report = json.loads(updates.package_files(payload)["implementation-report.json"][0])
        monkeypatch.setattr(updates, "instruction_configuration_update", lambda previous, candidate: True)
        update = updates.stage(directory, payload, report, ordinary_owner=True)["update"]
    finally:
        await service.shutdown()
    assert update["status"] == "blocked" and updates.ORDINARY_REPLACEMENT_BLOCKER in update["blockers"]
    assert update["native_definition_update"] is True
