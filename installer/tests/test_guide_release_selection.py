"""The Guide's exact Meta 0.9.3 selection: runtime admission, the pending
client pin and the release inputs skeleton. Inert files only; nothing runs."""
import hashlib
import json
from pathlib import Path

import pytest

from launchkit import guide_sdk
from launchkit.bootstrap import GatewayInstallError, SelectedGuideRuntime
from launchkit.connect_installer_caller import ExactFile

SKELETON = Path(__file__).parents[1] / "release/inputs/forge-release-inputs-0.9.3.skeleton.json"
PIN = ("SOURCE", "TREE", "SHA256", "SIZE")
CLIENT = b"inert guide client wheel, never install\n"


@pytest.fixture
def gateway(tmp_path):
    path = tmp_path / "inert-rpc-gateway"
    path.write_bytes(b"inert test fixture, never execute\n")
    path.chmod(0o700)
    return ExactFile(path, hashlib.sha256(path.read_bytes()).hexdigest())


def test_the_guide_admits_the_selected_meta_093_runtime(gateway):
    selected = SelectedGuideRuntime(gateway, "0.9.3", "0.9.3", "a" * 40, "b" * 40)
    assert (selected.version, selected.core_version) == ("0.9.3", "0.9.3")


@pytest.mark.parametrize("version, core_version", [
    ("0.9.0", "0.9.0"), ("0.9.2", "0.9.2"), ("0.9.4", "0.9.4"),
    ("0.9.3", "0.9.0"), ("0.9.3", "0.9.2"), ("0.9.0", "0.9.3"), ("0.9.2", "0.9.3"),
    ("0.9.3+meta.0bc6b4fa", "0.9.3"), ("v0.9.3", "0.9.3"), ("0.9.30", "0.9.3"), ("0.9.3", "0.9.3 "),
])
def test_the_guide_refuses_every_other_runtime_version(gateway, version, core_version):
    with pytest.raises(GatewayInstallError, match="exact selected Meta"):
        SelectedGuideRuntime(gateway, version, core_version, "a" * 40, "b" * 40)


def test_the_committed_guide_client_pin_is_pending_until_meta_093_publishes():
    """The published wheel's bytes cannot exist before its release. Filling the
    pin replaces this test with one that checks the vendored wheel against it."""
    assert guide_sdk.VERSION == "0.9.3"
    assert guide_sdk.WHEEL == "vendor/guide/meerkat_meta-0.9.3-py3-none-any.whl"
    assert [getattr(guide_sdk, name) for name in PIN] == [None] * len(PIN)
    for use in (guide_sdk.require_pinned, guide_sdk.selection, lambda: guide_sdk.verify(CLIENT)):
        with pytest.raises(ValueError, match="pending the published Meta 0.9.3 wheel"):
            use()


def complete_pin(monkeypatch, content=CLIENT):
    values = ("c" * 40, "d" * 40, hashlib.sha256(content).hexdigest(), len(content))
    for name, value in zip(PIN, values):
        monkeypatch.setattr(guide_sdk, name, value)


@pytest.mark.parametrize("name, value", [
    ("SOURCE", None), ("SOURCE", "pending"), ("SOURCE", "C" * 40), ("SOURCE", "c" * 39),
    ("TREE", None), ("TREE", ""),
    ("SHA256", None), ("SHA256", "PENDING"), ("SHA256", "0" * 63), ("SHA256", "B" * 64),
    ("SIZE", None), ("SIZE", 0), ("SIZE", -1), ("SIZE", True), ("SIZE", str(len(CLIENT))),
])
def test_a_partial_or_placeholder_guide_client_pin_fails_closed(monkeypatch, name, value):
    complete_pin(monkeypatch)
    assert guide_sdk.selection()["sha256"] == hashlib.sha256(CLIENT).hexdigest()
    guide_sdk.verify(CLIENT)
    monkeypatch.setattr(guide_sdk, name, value)
    for use in (guide_sdk.require_pinned, guide_sdk.selection, lambda: guide_sdk.verify(CLIENT)):
        with pytest.raises(ValueError, match="pending"):
            use()


def test_an_invented_complete_pin_still_refuses_the_actual_wheel(monkeypatch):
    complete_pin(monkeypatch)
    monkeypatch.setattr(guide_sdk, "SHA256", "0" * 64)
    with pytest.raises(ValueError, match="differs from its reviewed wheel"):
        guide_sdk.verify(CLIENT)


def test_the_release_inputs_skeleton_selects_the_admitted_guide(gateway):
    guide = json.loads(SKELETON.read_text())["guide"]
    assert (guide["version"], guide["core_version"]) == ("0.9.3", "0.9.3")
    SelectedGuideRuntime(gateway, guide["version"], guide["core_version"], "c" * 40, guide["core_source"])
    # Meta's source is pending until its release, and the runtime refuses it.
    with pytest.raises(GatewayInstallError):
        SelectedGuideRuntime(gateway, guide["version"], guide["core_version"],
            guide["meta_source"], guide["core_source"])
