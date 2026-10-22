"""Apply refuses a product runtime update this Forge's interpreter cannot install.

The update worker retains the current runtime before it installs the selected
product, so its own interpreter refusal would come only after the installed
runtime has moved. Inert registry, product and package fixtures only; no
worker, application, SDK, provider or native program runs.
"""
import pytest

from launchkit import deployment, deployment_updates as updates
from launchkit.local_application_registry import LocalApplicationRegistry
from launchkit.projects import DraftConflict
from test_local_application_registry import (registry, new_guide, build, accept_source_fixture,
    install_fixture, no_processes)
from test_meta_toolkit_initial_install import CP312_LINUX, ORIGINAL_INTERPRETER_REFUSAL, closure
from test_meta_toolkit_preserved_refresh import products
from test_meta_toolkit_runtime_selection import product, no_product_execution


async def staged_product_refresh(f, products, monkeypatch):
    saved = new_guide(f)
    first = await build(f, saved)
    await accept_source_fixture(f, first)
    directory, _ = await install_fixture(f, saved, first, monkeypatch)
    new_target = products[1].target
    # The restarted launcher selects the newer product for a runtime refresh.
    current_host = LocalApplicationRegistry(guide_store=f.store, implementation=f.implementation,
        deployment=f.service, runtime_target=new_target)
    f.implementation.prepare_workspace = current_host.prepare_workspace
    saved = f.store.get(saved["id"])
    refreshed = await f.implementation.submit(saved["id"], saved["version"], refresh_framework=True,
        preserve_design=True, expected_selected_build_id=first["build_id"], runtime_target=new_target)
    await accept_source_fixture(f, refreshed)
    staged = await f.service.stage_update(saved["id"], directory.name, f.store.get(saved["id"])["version"],
        refreshed["build_id"])
    update = staged["update"]
    assert update["status"] == "staged" and update["blockers"] == []
    assert update["runtime_dependency_update"] is not None
    return saved["id"], directory, update


async def test_apply_refuses_a_runtime_update_this_interpreter_cannot_install(registry, products, monkeypatch):
    f = registry
    guide_id, directory, update = await staged_product_refresh(f, products, monkeypatch)
    # Forge restarted on another CPython after Stage: the selected product's
    # compiled closure no longer fits this interpreter.
    def changed_interpreter(target, interpreter=None):
        return ORIGINAL_INTERPRETER_REFUSAL(closure(
            "pydantic_core-2.27.2-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
            "httpx-0.28.1-py3-none-any.whl"), CP312_LINUX)
    monkeypatch.setattr(deployment, "selected_interpreter_refusal", changed_interpreter)
    journal = updates.update_directory(directory, update["update_id"]) / "update.json"
    before = (directory / "receipt.json").read_bytes(), journal.read_bytes()
    f.service.launched.clear()
    with pytest.raises(DraftConflict, match=r"built for CPython 3\.11, but this Forge runs CPython 3\.12.*PYTHON_BIN"):
        await f.service.action(guide_id, directory.name, "apply-update", update["update_id"])
    assert f.service.launched == [] and not f.service.tasks
    assert ((directory / "receipt.json").read_bytes(), journal.read_bytes()) == before


async def test_apply_admits_a_runtime_update_this_interpreter_can_install(registry, products, monkeypatch):
    f = registry
    guide_id, directory, update = await staged_product_refresh(f, products, monkeypatch)
    f.service.launched.clear()
    await f.service.action(guide_id, directory.name, "apply-update", update["update_id"])
    launched_directory, mode, options = f.service.launched[-1]
    assert (launched_directory, mode) == (directory, "update") and options["update_selection"]
