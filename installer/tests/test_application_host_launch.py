"""Inert launcher controls with safe suppliers and no production server import."""
from pathlib import Path
import inspect
import shlex
from types import SimpleNamespace

import pytest

from launchkit import application_host_launch as launch


class SelectedHost:
    pass


@pytest.fixture
def supplier(monkeypatch):
    calls = []
    selected = SelectedHost()
    selected.original_source = object()
    selected.startup_selection = lambda guide, deployment: (guide, deployment, selected.original_source)
    app = object()
    owners = {name: object() for name in ("implementation", "deployment", "critic", "reading")}

    def factory():
        calls.append(("factory", selected))
        return selected

    def select(value):
        calls.append(("select", value))

    modules = {
        "trusted_operator.selection": SimpleNamespace(make_host=factory),
        "launchkit.forge_application_host": SimpleNamespace(ForgeApplicationHost=SelectedHost),
        "launchkit.runtime_selection": SimpleNamespace(MetaToolkitRuntimeTarget=type("SelectedTarget", (), {})),
        "launchkit.server": SimpleNamespace(app=app, select_application_host=select, **owners),
    }

    def load(name):
        calls.append(("import", name))
        if name not in modules:
            raise AssertionError("Unexpected production import: " + name)
        return modules[name]

    monkeypatch.setenv(launch.FACTORY_ENV, "trusted_operator.selection:make_host")
    monkeypatch.setattr(launch.importlib, "import_module", load)
    return SimpleNamespace(calls=calls, selected=selected, app=app, owners=owners, modules=modules)


def test_selects_original_host_once_before_returning_original_app(supplier):
    original_callback = supplier.selected.startup_selection
    assert launch.create_app() is supplier.app
    assert supplier.calls == [
        ("import", "trusted_operator.selection"),
        ("import", "launchkit.forge_application_host"),
        ("import", "launchkit.runtime_selection"),
        ("factory", supplier.selected),
        ("import", "launchkit.server"),
        ("select", supplier.selected),
    ]
    assert supplier.selected.startup_selection is original_callback
    assert original_callback("guide", "deployment") == ("guide", "deployment", supplier.selected.original_source)
    for name, owner in supplier.owners.items():
        assert getattr(supplier.modules["launchkit.server"], name) is owner


def test_ordinary_meta_selection_forwards_original_gateway_and_sdk_only(supplier, tmp_path):
    from launchkit.connect_installer_caller import ExactFile
    from launchkit.runtime_candidates import MetaToolkitRuntimeTarget
    # Constructor type composition only; no artifact reader/native operation
    # runs in this launcher test. Registry controls verify actual inert bytes.
    target = object.__new__(MetaToolkitRuntimeTarget)
    gateway = ExactFile(tmp_path / "ordinary-rpc", "a" * 64)
    selected = launch.OrdinaryMetaApplicationSelection(target, gateway)
    supplier.modules["trusted_operator.selection"].make_host = lambda: selected
    forwarded = []
    supplier.modules["launchkit.server"].select_local_application_runtime = (
        lambda value, **options: forwarded.append((value, options)))
    assert launch.create_app() is supplier.app
    assert len(forwarded) == 1 and forwarded[0][0] is target
    assert forwarded[0][1] == {"ordinary_meta_gateway": gateway}
    assert forwarded[0][1]["ordinary_meta_gateway"] is gateway


@pytest.mark.parametrize("gateway", [None, "/tmp/unselected-gateway", {"path": "/tmp/gateway", "sha256": "a" * 64}])
def test_ordinary_selection_rejects_untyped_runtime_or_gateway(gateway):
    from launchkit.runtime_candidates import MetaToolkitRuntimeTarget
    target = object.__new__(MetaToolkitRuntimeTarget)
    with pytest.raises(TypeError, match="exact ordinary Meta"):
        launch.OrdinaryMetaApplicationSelection(target, gateway)


def test_ordinary_application_retains_explicit_model_owner_without_selecting_guide(supplier, tmp_path, monkeypatch):
    from launchkit.connect_installer_caller import ExactFile
    from launchkit.connect_application_startup import _ModelConfiguration
    from launchkit.runtime_candidates import MetaToolkitRuntimeTarget
    from test_managed_model_factory_selection import model_source
    selection = model_source(tmp_path, monkeypatch)
    monkeypatch.setenv("XDG_CONFIG_HOME", selection["credential_config_root"])
    owner = _ModelConfiguration.observe(selection)
    target = object.__new__(MetaToolkitRuntimeTarget)
    gateway = ExactFile(tmp_path / "ordinary-rpc", "a" * 64)
    chosen = launch.OrdinaryMetaApplicationSelection(target, gateway, owner)
    supplier.modules["trusted_operator.selection"].make_host = lambda: chosen
    calls = []
    supplier.modules["launchkit.server"].select_local_application_runtime = (
        lambda value, **options: calls.append((value, options)))
    assert launch.create_app() is supplier.app
    assert calls == [(target, {"ordinary_meta_gateway": gateway, "model_configuration": owner})]
    assert calls[0][1]["model_configuration"] is owner
    assert not any("guide_runtime" in str(call) for call in supplier.calls)


def test_changed_application_model_source_refuses_before_server_selection(supplier, tmp_path, monkeypatch):
    from launchkit.connect_installer_caller import ExactFile, CustodyError
    from launchkit.connect_application_startup import _ModelConfiguration
    from launchkit.runtime_candidates import MetaToolkitRuntimeTarget
    from test_managed_model_factory_selection import model_source
    selection = model_source(tmp_path, monkeypatch)
    monkeypatch.setenv("XDG_CONFIG_HOME", selection["credential_config_root"])
    owner = _ModelConfiguration.observe(selection)
    target = object.__new__(MetaToolkitRuntimeTarget)
    gateway = ExactFile(tmp_path / "ordinary-rpc", "a" * 64)
    chosen = launch.OrdinaryMetaApplicationSelection(target, gateway, owner)
    supplier.modules["trusted_operator.selection"].make_host = lambda: chosen
    supplier.modules["launchkit.server"].select_local_application_runtime = (
        lambda *args, **kwargs: pytest.fail("Changed source cannot reach server selection"))
    owner.host_config.path.write_text("Changed inert nonsecret model source.\n")
    with pytest.raises(CustodyError):
        launch.create_app()


@pytest.mark.parametrize("value", [{}, "/tmp/model-config", object()])
def test_application_model_configuration_needs_actual_custody_owner(tmp_path, value):
    from launchkit.connect_installer_caller import ExactFile
    from launchkit.runtime_candidates import MetaToolkitRuntimeTarget
    target = object.__new__(MetaToolkitRuntimeTarget)
    with pytest.raises(TypeError, match="original application model configuration owner"):
        launch.OrdinaryMetaApplicationSelection(target, ExactFile(tmp_path / "rpc", "a" * 64), value)


@pytest.mark.parametrize("value", [None, "", "module", ":factory", "module:",
    " module:factory", "module:factory ", "module:factory:other", "../owner.py:factory",
    ".owner:factory", "owner..selection:factory", "owner:object.factory", "owner:class",
    "owner:fact\u00f6ry", "owner:factory\x00"])
def test_invalid_reference_refuses_before_any_import(monkeypatch, supplier, value):
    monkeypatch.setattr(launch.os, "environ", {launch.FACTORY_ENV: value})
    with pytest.raises(ValueError, match="trusted Python module:factory"):
        launch.create_app()
    assert supplier.calls == []


def test_missing_trusted_module_does_not_import_server(monkeypatch, supplier):
    monkeypatch.setenv(launch.FACTORY_ENV, "missing_owner:make_host")
    failure = ImportError("Retained trusted owner is unavailable")

    def missing(name):
        assert name == "missing_owner"
        raise failure

    monkeypatch.setattr(launch.importlib, "import_module", missing)
    with pytest.raises(ImportError) as caught:
        launch.create_app()
    assert caught.value is failure
    assert supplier.calls == []


def test_missing_factory_does_not_import_server(supplier):
    del supplier.modules["trusted_operator.selection"].make_host
    with pytest.raises(AttributeError):
        launch.create_app()
    assert supplier.calls == [("import", "trusted_operator.selection")]


@pytest.mark.parametrize("kind", ["not_callable", "requires_argument", "async", "async_callable",
    "async_generator", "async_generator_callable"])
def test_invalid_callable_refuses_without_invocation_or_server_import(supplier, kind):
    def requires_argument(selection):
        raise AssertionError("An invalid factory must not run")

    async def asynchronous():
        raise AssertionError("An invalid factory must not run")

    class AsyncCallable:
        async def __call__(self):
            raise AssertionError("An invalid factory must not run")

    async def asynchronous_generator():
        raise AssertionError("An invalid factory must not run")
        yield

    class AsyncGeneratorCallable:
        async def __call__(self):
            raise AssertionError("An invalid factory must not run")
            yield

    supplier.modules["trusted_operator.selection"].make_host = {
        "not_callable": supplier.selected, "requires_argument": requires_argument,
        "async": asynchronous, "async_callable": AsyncCallable(),
        "async_generator": asynchronous_generator,
        "async_generator_callable": AsyncGeneratorCallable(),
    }[kind]
    with pytest.raises(TypeError, match="synchronous zero-argument factory"):
        launch.create_app()
    assert supplier.calls == [("import", "trusted_operator.selection")]


def test_sync_factory_coroutine_is_closed_without_execution_or_server_import(supplier):
    entered = []

    async def incorrectly_returned():
        entered.append("executed")
        return supplier.selected

    coroutine = incorrectly_returned()
    supplier.modules["trusted_operator.selection"].make_host = lambda: coroutine
    assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CREATED
    with pytest.raises(TypeError, match="exact ForgeApplicationHost"):
        launch.create_app()
    assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED
    assert entered == []
    assert ("import", "launchkit.server") not in supplier.calls


def test_factory_failure_stops_before_server_import(supplier):
    failure = RuntimeError("Original selection is no longer current")

    def stale():
        raise failure

    supplier.modules["trusted_operator.selection"].make_host = stale
    with pytest.raises(RuntimeError) as caught:
        launch.create_app()
    assert caught.value is failure
    assert ("import", "launchkit.server") not in supplier.calls


@pytest.mark.parametrize("kind", ["missing", "lookalike", "subclass"])
def test_wrong_host_type_refuses_before_server_import(supplier, kind):
    class Subclass(SelectedHost):
        pass

    value = {"missing": None, "lookalike": SimpleNamespace(require_current=lambda: None),
        "subclass": Subclass()}[kind]
    supplier.modules["trusted_operator.selection"].make_host = lambda: value
    with pytest.raises(TypeError, match="exact ForgeApplicationHost"):
        launch.create_app()
    assert ("import", "launchkit.server") not in supplier.calls


def test_existing_selector_failure_never_returns_an_unselected_app(supplier):
    failure = RuntimeError("Preserve the original selected application host")

    def retained_owner_refuses(selected):
        assert selected is supplier.selected
        raise failure

    supplier.modules["launchkit.server"].select_application_host = retained_owner_refuses
    with pytest.raises(RuntimeError) as caught:
        launch.create_app()
    assert caught.value is failure
    assert supplier.calls.count(("factory", supplier.selected)) == 1


def test_actual_start_declarations_keep_default_and_select_factory_only_when_configured():
    script = (Path(__file__).parents[1] / "start.sh").read_text()
    tail = script[script.index('echo "Keep this terminal open. Press Ctrl+C to stop Meta Forge."'):]
    lines = tail.splitlines()
    assert lines[1] == 'if [ -n "${META_FORGE_APPLICATION_HOST_FACTORY:-}" ]; then'
    assert lines[3] == "fi"
    configured = shlex.split(lines[2])
    assert lines[4] == 'if [ "$standalone_runtime" = meta-toolkit ]; then'
    packaged = shlex.split(lines[5])
    assert lines[6] == "fi"
    default = shlex.split(lines[7])
    assert configured == ["exec", ".venv/bin/python", "-m", "uvicorn",
        "launchkit.application_host_launch:create_app", "--factory", "--host", "127.0.0.1", "--port", "$launch_port"]
    assert default == ["exec", ".venv/bin/python", "-m", "uvicorn", "launchkit.server:app",
        "--host", "127.0.0.1", "--port", "$launch_port"]
    assert packaged == ["exec", ".venv/bin/python", "-m", "uvicorn",
        "launchkit.packaged_runtime:create_app", "--factory", "--host", "127.0.0.1", "--port", "$launch_port"]
    assert len(lines) == 8
    recovery = next(line for line in script.splitlines() if line.startswith("  for setting in "))
    assert launch.FACTORY_ENV in [word.rstrip(";") for word in recovery.split()]
