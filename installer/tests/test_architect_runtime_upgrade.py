"""Retained setup-runtime qualification with mock bytes, never native execution."""
import asyncio
import base64
from contextlib import contextmanager
import hashlib
import json
import os
import threading
from types import SimpleNamespace

import pytest

from launchkit import architect, bootstrap, planner, runtime_dependencies
from launchkit.connect_installer_caller import CustodyError
from launchkit.templates import storage_setup
from test_architect_startup_recovery import FakeRuntime, startup
from test_provider_settings import native_spec


TARGET = "aarch64-apple-darwin"
PROJECT_LOCK = storage_setup.project_lock


def mock_binary(path, version, name):
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    content = ("Never executed: " + name + ":" + version).encode()
    path.write_bytes(content)
    path.chmod(0o700)
    prefix = {"rpc_gateway": "mobkit-rpc-gateway", "mobkit_gateway": "mobkit-gateway"}[name]
    value = {"version": version, "archive": f"{prefix}-{version}-{TARGET}.tar.gz",
             "release_url": f"https://github.com/lukacf/meerkat-mobkit/releases/download/v{version}",
             "archive_sha256": "a" * 64, "binary_sha256": hashlib.sha256(content).hexdigest()}
    if version in {"0.8.46", "0.8.47"}:
        value["core_version"] = "0.8.51"
    receipt = path.with_name(name + ".receipt.json")
    storage_setup.write_private(receipt, value)
    return path


@pytest.fixture
def crossing(tmp_path, monkeypatch):
    monkeypatch.setattr(bootstrap, "VERSION", "0.8.46")
    monkeypatch.setattr(bootstrap, "platform_target", lambda: TARGET)
    monkeypatch.delenv("MOBKIT_GATEWAY_BIN", raising=False)
    monkeypatch.delenv("MOBKIT_RPC_GATEWAY_BIN", raising=False)
    root = tmp_path / "saved-state/architect-runtime-v2"
    root.mkdir(parents=True, mode=0o700)
    (root / ".bin").mkdir(mode=0o700)
    (root / ".mobkit/state").mkdir(parents=True, mode=0o700)
    (root / ".mobkit").chmod(0o700)
    history = root / ".mobkit/state/event_log.sqlite3"
    history.write_bytes(b"Fixture-owned saved native history, never opened as SQLite.")
    old = mock_binary(tmp_path / "original-package/.tools/mobkit_gateway", "0.8.42", "mobkit_gateway")
    package = tmp_path / "successor-package"
    maintenance = mock_binary(bootstrap.package_tool_path(package, "mobkit_gateway"), "0.8.46", "mobkit_gateway")
    rpc = mock_binary(maintenance.with_name("rpc_gateway"), "0.8.46", "rpc_gateway")
    monkeypatch.setattr(architect, "gateway_path", lambda: rpc)
    link = root / ".bin/mobkit_gateway"
    link.symlink_to(old)
    storage_setup.write_private(root / "deployment.json", {"slug": "launch-architect", "mobkitVersion": "0.8.42"})
    storage_setup.write_private(root / ".mobkit/launch-storage.json",
        {"format": 1, "project": "launch-architect", "version": "0.8.42", "phase": "ready"})
    fixture = SimpleNamespace(root=root, old=old, maintenance=maintenance, rpc=rpc, history=history, package=package,
                              events=[], leased=False, original_history=history.read_bytes(),
                              original_history_inode=history.stat().st_ino)

    @contextmanager
    def lease(selected):
        assert selected == root
        with PROJECT_LOCK(selected):
            fixture.leased = True
            fixture.events.append("lease-acquired")
            try:
                yield
            finally:
                fixture.leased = False
                fixture.events.append("lease-released")

    fixture.lease = lease

    def download(selected, *, binary_name):
        assert fixture.leased and selected == maintenance and binary_name == "mobkit_gateway"
        fixture.events.append("verified-download")
        return maintenance

    def upgrade(selected, from_version, to_version):
        assert fixture.leased and selected == root and (from_version, to_version) == ("0.8.42", "0.8.46")
        fixture.events.append("native-upgrade")
        assert os.readlink(link) == str(maintenance)
        assert json.loads((root / "deployment.json").read_text())["mobkitVersion"] == "0.8.46"
        assert (root / ".mobkit" / architect.SETUP_UPGRADE_RECEIPT).is_file()
        storage_setup.write_private(root / ".mobkit/launch-storage.json",
            {"format": 1, "project": "launch-architect", "version": "0.8.46", "phase": "ready"})
        return {"applied": True}

    monkeypatch.setattr(bootstrap, "download_gateway", download)
    monkeypatch.setattr(storage_setup, "upgrade_storage", upgrade)
    fixture.upgrade = upgrade
    return fixture


def prepare(fixture):
    with fixture.lease(fixture.root):
        return architect._prepare_setup_runtime(fixture.root, fixture.maintenance)


def retained(fixture):
    return fixture.root / ".mobkit" / architect.SETUP_UPGRADE_RECEIPT


def assert_history_unchanged(fixture):
    assert fixture.history.read_bytes() == fixture.original_history
    assert fixture.history.stat().st_ino == fixture.original_history_inode
    assert fixture.old.read_bytes().endswith(b"0.8.42")


def test_crossing_retains_exact_previous_proof_and_same_native_history(crossing):
    fixture = crossing
    previous_deployment = (fixture.root / "deployment.json").read_bytes()
    previous_marker = (fixture.root / ".mobkit/launch-storage.json").read_bytes()
    previous_receipt = fixture.old.with_name("mobkit_gateway.receipt.json").read_bytes()
    previous_link = architect._setup_link(fixture.root / ".bin/mobkit_gateway")
    proof = prepare(fixture)
    saved = json.loads(retained(fixture).read_text())
    assert saved["root"] == str(fixture.root) and saved["project"] == "launch-architect"
    assert saved["phase"] == "native_upgrade_forward"
    assert base64.b64decode(saved["previous_deployment"]) == previous_deployment
    assert base64.b64decode(saved["previous_marker"]) == previous_marker
    assert saved["previous_link"] == previous_link
    assert base64.b64decode(saved["previous_maintenance"]["receipt"]) == previous_receipt
    assert saved["native"] == proof["native"]
    assert proof["native"]["rpc_gateway"]["path"] == str(fixture.rpc)
    assert retained(fixture).stat().st_mode & 0o777 == 0o600
    assert fixture.events == ["lease-acquired", "verified-download", "native-upgrade", "lease-released"]
    assert_history_unchanged(fixture)


def test_completed_crossing_rechecks_pair_without_repeating_native_apply(crossing):
    fixture = crossing
    first = prepare(fixture)
    saved = retained(fixture).read_bytes()
    assert prepare(fixture) == first
    assert fixture.events.count("native-upgrade") == 1
    assert fixture.events.count("verified-download") == 1
    assert retained(fixture).read_bytes() == saved
    assert_history_unchanged(fixture)


@pytest.mark.parametrize("boundary", ["before-rebind", "after-rebind", "before-upgrade", "after-apply"])
def test_interrupted_crossing_resumes_forward_on_same_qualified_pair(crossing, monkeypatch, boundary):
    fixture = crossing
    rebind, verify, upgrade = architect._rebind_setup_link, architect._verify_setup_runtime, storage_setup.upgrade_storage
    if boundary in {"before-rebind", "after-rebind"}:
        def interrupted(*args):
            if boundary == "after-rebind":
                rebind(*args)
            raise InterruptedError("Retained interrupted binding.")
        monkeypatch.setattr(architect, "_rebind_setup_link", interrupted)
    elif boundary == "before-upgrade":
        def interrupted(*args, **kwargs):
            verify(*args, **kwargs)
            raise InterruptedError("Retained interrupted pre-native proof.")
        monkeypatch.setattr(architect, "_verify_setup_runtime", interrupted)
    else:
        def interrupted(*args):
            upgrade(*args)
            raise InterruptedError("Native apply finished before caller returned.")
        monkeypatch.setattr(storage_setup, "upgrade_storage", interrupted)
    with pytest.raises(InterruptedError):
        prepare(fixture)
    assert retained(fixture).exists()
    monkeypatch.setattr(architect, "_rebind_setup_link", rebind)
    monkeypatch.setattr(architect, "_verify_setup_runtime", verify)
    monkeypatch.setattr(storage_setup, "upgrade_storage", upgrade)
    prepare(fixture)
    assert fixture.events.count("native-upgrade") == 1
    assert_history_unchanged(fixture)


def test_interrupted_exclusive_receipt_publication_reconciles_only_its_exact_temp(crossing, monkeypatch):
    fixture = crossing
    unlink = runtime_dependencies._unlink_publication
    def interrupted(*args):
        raise InterruptedError("Canonical receipt linked, temp not yet removed.")
    monkeypatch.setattr(runtime_dependencies, "_unlink_publication", interrupted)
    with pytest.raises(InterruptedError):
        prepare(fixture)
    assert retained(fixture).stat().st_nlink == 2
    assert not "native-upgrade" in fixture.events
    monkeypatch.setattr(runtime_dependencies, "_unlink_publication", unlink)
    prepare(fixture)
    assert retained(fixture).stat().st_nlink == 1
    assert not list(retained(fixture).parent.glob(".setup-runtime-upgrade.json.publish-*"))
    assert_history_unchanged(fixture)


@pytest.mark.parametrize("change", ["no-old-link", "ordinary-link-path", "unknown-target", "changed-old-binary", "old-receipt-override",
                                  "new-core", "rpc-version", "rpc-selection", "override", "wrong-project", "wrong-phase",
                                  "bool-format", "float-format", "missing-deployment", "unknown-empty-state", "in-place-tool"])
def test_unrecognized_crossing_refuses_before_native_or_saved_authority(crossing, monkeypatch, change):
    fixture = crossing
    link = fixture.root / ".bin/mobkit_gateway"
    marker = fixture.root / ".mobkit/launch-storage.json"
    if change == "no-old-link":
        link.unlink()
    elif change == "ordinary-link-path":
        link.unlink()
        link.write_text("Do not replace me.")
    elif change == "unknown-target":
        link.unlink()
        link.symlink_to(fixture.old.with_name("unexpected"))
    elif change == "changed-old-binary":
        fixture.old.write_bytes(b"changed private old bytes")
    elif change in {"old-receipt-override", "new-core", "rpc-version"}:
        binary = fixture.old if change == "old-receipt-override" else fixture.maintenance if change == "new-core" else fixture.rpc
        receipt = binary.with_name(binary.name + ".receipt.json")
        value = json.loads(receipt.read_text())
        value[{"old-receipt-override": "source", "new-core": "core_version", "rpc-version": "version"}[change]] = "unexpected"
        storage_setup.write_private(receipt, value)
    elif change == "rpc-selection":
        monkeypatch.setattr(architect, "gateway_path", lambda: fixture.old)
    elif change == "override":
        monkeypatch.setenv("MOBKIT_GATEWAY_BIN", str(fixture.rpc))
    elif change in {"wrong-project", "wrong-phase", "bool-format", "float-format"}:
        value = json.loads(marker.read_text())
        key = "project" if change == "wrong-project" else "phase" if change == "wrong-phase" else "format"
        value[key] = True if change == "bool-format" else 1.0 if change == "float-format" else "unexpected"
        storage_setup.write_private(marker, value)
    elif change == "missing-deployment":
        (fixture.root / "deployment.json").unlink()
        value = json.loads(marker.read_text())
        value["version"] = "0.8.46"
        storage_setup.write_private(marker, value)
    elif change == "unknown-empty-state":
        fixture.history.unlink()
    else:
        fixture.maintenance = fixture.old
        fixture.rpc = mock_binary(fixture.old.with_name("rpc_gateway"), "0.8.46", "rpc_gateway")
        monkeypatch.setattr(architect, "gateway_path", lambda: fixture.rpc)
    with pytest.raises((RuntimeError, OSError)):
        prepare(fixture)
    assert not retained(fixture).exists()
    assert "native-upgrade" not in fixture.events
    if change not in {"new-core", "rpc-version"}:
        assert "verified-download" not in fixture.events
    if change != "unknown-empty-state":
        assert fixture.history.read_bytes() == fixture.original_history


@pytest.mark.parametrize("change", ["link", "deployment", "marker", "rpc-bytes", "new-receipt", "receipt-root",
                                  "receipt-bool-marker", "receipt-duplicate-marker", "receipt-duplicate-deployment", "receipt-hardlink"])
def test_recovery_refuses_changed_saved_or_current_authority(crossing, change):
    fixture = crossing
    prepare(fixture)
    receipt = retained(fixture)
    saved = json.loads(receipt.read_text())
    if change == "link":
        link = fixture.root / ".bin/mobkit_gateway"
        link.unlink()
        link.symlink_to(fixture.old)
    elif change == "deployment":
        storage_setup.write_private(fixture.root / "deployment.json", {"slug": "other-project", "mobkitVersion": "0.8.46"})
    elif change == "marker":
        storage_setup.write_private(fixture.root / ".mobkit/launch-storage.json",
            {"format": 1, "project": "launch-architect", "version": "0.8.46", "phase": "preparing"})
    elif change == "rpc-bytes":
        fixture.rpc.write_bytes(b"changed accepted runtime")
    elif change == "new-receipt":
        path = fixture.maintenance.with_name("mobkit_gateway.receipt.json")
        value = json.loads(path.read_text())
        value["archive_sha256"] = "b" * 64
        storage_setup.write_private(path, value)
    elif change == "receipt-root":
        saved["root"] = str(fixture.root.parent)
        storage_setup.write_private(receipt, saved)
    elif change == "receipt-bool-marker":
        value = json.loads(base64.b64decode(saved["previous_marker"]))
        value["format"] = True
        saved["previous_marker"] = base64.b64encode(json.dumps(value).encode()).decode("ascii")
        storage_setup.write_private(receipt, saved)
    elif change.startswith("receipt-duplicate-"):
        key = "previous_marker" if change.endswith("marker") else "previous_deployment"
        content = base64.b64decode(saved[key])
        duplicate = b'"format":1,' if key == "previous_marker" else b'"slug":"launch-architect",'
        saved[key] = base64.b64encode(b"{" + duplicate + content[1:]).decode("ascii")
        storage_setup.write_private(receipt, saved)
    else:
        os.link(receipt, fixture.root / ".mobkit/unrelated-receipt-link")
    with pytest.raises((RuntimeError, OSError)):
        prepare(fixture)
    assert fixture.events.count("native-upgrade") == 1
    assert fixture.history.read_bytes() == fixture.original_history


@pytest.mark.parametrize("existing", [False, True])
def test_fresh_or_current_exact_tuple_keeps_working(crossing, existing):
    fixture = crossing
    fixture.history.unlink()
    for path in (fixture.root / "deployment.json", fixture.root / ".mobkit/launch-storage.json", fixture.root / ".bin/mobkit_gateway"):
        path.unlink()
    if existing:
        fixture.history.write_bytes(fixture.original_history)
        storage_setup.write_private(fixture.root / "deployment.json", {"slug": "launch-architect", "mobkitVersion": "0.8.46"})
        storage_setup.write_private(fixture.root / ".mobkit/launch-storage.json",
            {"format": 1, "project": "launch-architect", "version": "0.8.46", "phase": "ready"})
    proof = prepare(fixture)
    assert proof["native"]["rpc_gateway"]["path"] == str(fixture.rpc)
    assert not retained(fixture).exists()
    assert "native-upgrade" not in fixture.events


def setup_service_for_crossing(startup, fixture, monkeypatch):
    service, controller = startup
    service.root = fixture.root.parent
    fresh_root = architect.setup_runtime_root(service.root)
    assert fresh_root != fixture.root

    @contextmanager
    def fresh_lease(root):
        assert root == fresh_root
        with PROJECT_LOCK(root):
            fixture.leased = True
            fixture.events.append("lease-acquired")
            try:
                yield
            finally:
                fixture.leased = False
                fixture.events.append("lease-released")

    monkeypatch.setattr(storage_setup, "project_lock", fresh_lease)
    async def catalog():
        assert fixture.leased
        fixture.events.append("toolkit-catalog")
    async def setup():
        assert fixture.leased
        fixture.events.append("toolkit-setup")
    async def inspect(*args):
        pytest.fail("Fresh Meta init must not inspect or upgrade the predecessor actor store")
    async def input_capability(**kwargs):
        return None
    monkeypatch.setattr(service.toolkit_catalog, "start", catalog)
    monkeypatch.setattr(service.toolkit_setup, "start", setup)
    monkeypatch.setattr(storage_setup, "prepare_storage", inspect)
    monkeypatch.setattr(service, "refresh_input_capability", input_capability)
    controller.outcomes = [FakeRuntime()]
    return service, controller


@pytest.mark.asyncio
async def test_meta_start_preserves_legacy_state_and_uses_separately_selected_gateway(startup, crossing, monkeypatch):
    service, controller = setup_service_for_crossing(startup, crossing, monkeypatch)
    selected = bootstrap.selected_guide_runtime()
    assert (await service.start())["status"] == "ready"
    assert crossing.events == ["lease-acquired", "toolkit-catalog", "toolkit-setup"]
    assert controller.builds == 1 and controller.gateways == [str(selected.gateway.path)]
    assert not retained(crossing).exists()
    assert_history_unchanged(crossing)
    await service.shutdown()
    assert crossing.events[-1] == "lease-released"


@pytest.mark.asyncio
async def test_selected_guide_drift_refuses_build_without_touching_predecessor(startup, crossing, monkeypatch):
    service, controller = setup_service_for_crossing(startup, crossing, monkeypatch)
    selected = bootstrap.selected_guide_runtime()
    async def changed_after_selection():
        selected.gateway.path.write_bytes(b"Changed after exact guide selection.")
    monkeypatch.setattr(service.toolkit_setup, "start", changed_after_selection)
    with pytest.raises((RuntimeError, CustodyError)):
        await service.start()
    assert controller.builds == 0 and not crossing.leased
    assert crossing.events == ["lease-acquired", "toolkit-catalog", "lease-released"]
    assert not retained(crossing).exists()
    assert_history_unchanged(crossing)


@pytest.mark.asyncio
async def test_live_project_lease_prevents_download_rebind_and_builder(startup, monkeypatch):
    service, controller = startup
    root = architect.setup_runtime_root(service.root)
    root.mkdir(parents=True, mode=0o700)
    monkeypatch.setattr(storage_setup, "project_lock", PROJECT_LOCK)
    with PROJECT_LOCK(root):
        with pytest.raises(RuntimeError, match="already running"):
            await service.start()
    assert controller.builders == controller.builds == 0
    assert not (root / "deployment.json").exists()
    assert not (root / ".bin/mobkit_gateway").is_symlink()


@pytest.mark.asyncio
async def test_cancelled_offline_preparation_keeps_lease_until_thread_exits(crossing, monkeypatch):
    fixture = crossing
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    def preparation(root, maintenance):
        assert fixture.leased
        entered.set()
        assert release.wait(5)
        assert fixture.leased
        finished.set()
        return {"native": {}}
    monkeypatch.setattr(architect, "_prepare_setup_runtime", preparation)
    async def owned():
        with fixture.lease(fixture.root):
            return await architect._prepare_setup_runtime_owned(fixture.root, fixture.maintenance)
    task = asyncio.create_task(owned())
    assert await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert fixture.leased and not task.done() and not finished.is_set()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set() and not fixture.leased
    assert fixture.events == ["lease-acquired", "lease-released"]


@pytest.mark.parametrize("from_version", ["0.8.42", "0.8.46"])
def test_047_setup_refresh_retains_history_and_completed_046_authority(crossing, monkeypatch, tmp_path, from_version):
    fixture = crossing
    if from_version == "0.8.46":
        prepare(fixture)
        old_receipt = retained(fixture).read_bytes()
        old_receipt_inode = retained(fixture).stat().st_ino
        previous_tool = fixture.maintenance
    else:
        old_receipt = None
        previous_tool = fixture.old
    previous_bytes = previous_tool.read_bytes()
    previous_inode = previous_tool.stat().st_ino
    current = mock_binary(tmp_path / "package-047/.tools/mobkit_gateway", "0.8.47", "mobkit_gateway")
    rpc = mock_binary(current.with_name("rpc_gateway"), "0.8.47", "rpc_gateway")
    # The current Core tuple is explicit in both synthetic release receipts.
    for binary in (current, rpc):
        path = binary.with_name(binary.name + ".receipt.json")
        value = json.loads(path.read_text()); value["core_version"] = "0.8.51"
        storage_setup.write_private(path, value)
    monkeypatch.setattr(bootstrap, "VERSION", "0.8.47")
    monkeypatch.setattr(architect, "gateway_path", lambda: rpc)
    def download(selected, *, binary_name):
        assert fixture.leased and selected == current and binary_name == "mobkit_gateway"
        return current
    monkeypatch.setattr(bootstrap, "download_gateway", download)
    calls = []
    def upgrade(root, previous, successor):
        assert fixture.leased and root == fixture.root and (previous, successor) == (from_version, "0.8.47")
        calls.append((previous, successor))
        storage_setup.write_private(root / ".mobkit/launch-storage.json",
            {"format": 1, "project": "launch-architect", "version": successor, "phase": "ready"})
        return {"applied": previous == "0.8.42"}
    monkeypatch.setattr(storage_setup, "upgrade_storage", upgrade)
    with fixture.lease(fixture.root):
        proof = architect._prepare_setup_runtime(fixture.root, current)
    receipt = fixture.root / f".mobkit/setup-runtime-upgrade-{from_version}-0.8.47.json"
    authority = receipt.read_bytes()
    assert json.loads(authority)["from_version"] == from_version
    assert json.loads(authority)["to_version"] == "0.8.47"
    with fixture.lease(fixture.root):
        assert architect._prepare_setup_runtime(fixture.root, current) == proof
    assert calls == [(from_version, "0.8.47")]
    assert receipt.read_bytes() == authority
    assert previous_tool.read_bytes() == previous_bytes and previous_tool.stat().st_ino == previous_inode
    assert_history_unchanged(fixture)
    if old_receipt is not None:
        assert retained(fixture).read_bytes() == old_receipt and retained(fixture).stat().st_ino == old_receipt_inode


def test_legacy_release_pair_preserves_cache_and_stopped_owner_without_selecting_guide(crossing, monkeypatch, tmp_path):
    fixture = crossing
    monkeypatch.setattr(bootstrap, "VERSION", "0.8.47")
    package = tmp_path / "same-checkout"
    legacy = mock_binary(package / ".tools/mobkit_gateway", "0.8.42", "mobkit_gateway")
    legacy_rpc = mock_binary(legacy.with_name("rpc_gateway"), "0.8.42", "rpc_gateway")
    current = bootstrap.package_tool_path(package, "mobkit_gateway")
    rpc = bootstrap.package_tool_path(package, "rpc_gateway")
    mock_binary(current, "0.8.47", "mobkit_gateway")
    mock_binary(rpc, "0.8.47", "rpc_gateway")
    legacy_files = [legacy, legacy_rpc] + [path.with_name(path.name + ".receipt.json") for path in (legacy, legacy_rpc)]
    retained_bytes = {path: (path.read_bytes(), path.stat().st_ino) for path in legacy_files}
    link = fixture.root / ".bin/mobkit_gateway"
    link.unlink()
    link.symlink_to(legacy)
    fixture.old = legacy
    stopped_link = tmp_path / "stopped-reading/.bin/mobkit_gateway"
    stopped_link.parent.mkdir(parents=True, mode=0o700)
    stopped_link.symlink_to(legacy)
    stopped_proof = architect._setup_link(stopped_link)
    monkeypatch.setattr(planner, "__file__", str(package / "launchkit/planner.py"))
    monkeypatch.setattr(planner.shutil, "which", lambda name: None)
    monkeypatch.setattr(architect, "gateway_path", planner.gateway_path)
    assert planner.gateway_path() is None, "Legacy cache presence cannot select the Meta guide"
    # The retained legacy helper is still tested with explicit inert inputs.
    monkeypatch.setattr(architect, "gateway_path", lambda: rpc)
    assert current.parent.name == ".tools"

    def download(selected, *, binary_name):
        assert fixture.leased and selected == current and binary_name == "mobkit_gateway"
        fixture.events.append("verified-download")
        return current

    def upgrade(root, previous, successor):
        assert fixture.leased and root == fixture.root and (previous, successor) == ("0.8.42", "0.8.47")
        fixture.events.append("native-upgrade")
        storage_setup.write_private(root / ".mobkit/launch-storage.json",
            {"format": 1, "project": "launch-architect", "version": successor, "phase": "ready"})
        return {"applied": True}

    monkeypatch.setattr(bootstrap, "download_gateway", download)
    monkeypatch.setattr(storage_setup, "upgrade_storage", upgrade)
    with fixture.lease(fixture.root):
        proof = architect._prepare_setup_runtime(fixture.root, current)
    receipt = fixture.root / ".mobkit/setup-runtime-upgrade-0.8.42-0.8.47.json"
    saved = json.loads(receipt.read_text())
    assert saved["previous_link"]["target"] == str(legacy)
    assert base64.b64decode(saved["previous_maintenance"]["receipt"]) == retained_bytes[legacy.with_name("mobkit_gateway.receipt.json")][0]
    assert proof["native"]["rpc_gateway"]["path"] == str(rpc)
    assert proof["native"]["mobkit_gateway"]["path"] == str(current)
    with fixture.lease(fixture.root):
        assert architect._prepare_setup_runtime(fixture.root, current) == proof
    assert fixture.events.count("native-upgrade") == 1
    assert fixture.events.count("verified-download") == 1
    assert architect._setup_link(stopped_link) == stopped_proof
    for path, (content, inode) in retained_bytes.items():
        assert path.read_bytes() == content and path.stat().st_ino == inode
    assert_history_unchanged(fixture)
