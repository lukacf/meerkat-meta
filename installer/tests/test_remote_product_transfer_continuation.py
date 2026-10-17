"""Original origin request/custody continuity with real inert framed receivers.

No SDK, pip, venv, application, native, SSH or cloud process is executed. Actual
selected product readers, compiler source, frozen trusted bundle, artifact/source
intake and origin connect/install/currentness/journal owners remain production.
Only host capability and final Install admission replies use the established
transport specimen. Actual factory byte Install has separate Stage A controls.
"""
import asyncio
from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import pwd
import sys
from types import SimpleNamespace

import pytest

from launchkit import compiler, remote_product_artifact_receiver as receiver
from launchkit import remote_product_artifacts as transfer
from launchkit import remote_product_destination_owner as source_owner
from launchkit import remote_product_control as control
from launchkit.artifact_binding import proposal_hash
from launchkit.deployment import DeploymentService
from launchkit.projects import fingerprint
from launchkit.remote_deployment import RemoteDeploymentService, read_receipt, controller_program
from launchkit.runtime_selection import candidate_selection
from launchkit.ssh_transport import SshTransportError
from test_meta_toolkit_initial_package import setup_spec, inputs
from test_meta_toolkit_runtime_selection import product, no_product_execution
import test_meta_toolkit_runtime_selection as product_fixtures
from test_meta_toolkit_runtime_update import catalog_target
from test_product_artifact_transfer import GUIDE, BUILD, SOURCE, frame
from test_product_artifact_transport import ENDPOINT, FINGERPRINT
from test_product_destination_owner import package_bytes, no_destination_execution
from test_remote_deployment import Guide, Implementation, Transport
from test_remote_product_control import selected_control


class AcceptedProduct(Implementation):
    def __init__(self, guide, target, package):
        super().__init__(guide)
        self.identity, self.digest, self.target, self.payload = BUILD, SOURCE, target, package
    async def package(self, identity, build_id, *, state=None):
        assert (identity, build_id) == (GUIDE, BUILD)
        if state is None:
            await self.guide.get(identity)
        else:
            assert state["id"] == identity
        return self.payload, self.original["proposal"]["project"]["slug"]
    def selected_runtime_target(self, guide_id, *, build_id, source_digest):
        assert (guide_id, build_id, source_digest) == (GUIDE, BUILD, SOURCE)
        return self.target


class OriginalFramedTransport(Transport):
    def __init__(self, home, destination):
        super().__init__()
        self.key = FINGERPRINT
        self.host.update(uid=os.getuid(), home=str(home), destination=str(destination),
            python=platform.python_version())
        self.frames, self.failure, self.failed = [], None, False
        self.origin = None
    def retained(self, endpoint, request, expected_fingerprint, deadline=None):
        assert endpoint == ENDPOINT and expected_fingerprint == self.key
        if deadline is not None:
            assert deadline > asyncio.get_running_loop().time()
        lane = ("artifact" if "transaction_id" in request.get("context", {}) else
            "source" if "context" in request else "control")
        self.frames.append((lane, deepcopy(request)))
        return lane
    def lose(self, boundary):
        if self.failure == boundary and not self.failed:
            self.failed = True
            raise SshTransportError("timeout")
    async def request(self, endpoint, program, payload, timeout=30,
            expected_fingerprint=None, deadline=None):
        if "context" in payload:
            lane = self.retained(endpoint, payload, expected_fingerprint, deadline)
            if lane == "artifact":
                parsed, stream = receiver.read_request(io.BytesIO(transfer._json(payload)))
                content = (self.origin.directory(GUIDE) / "artifact-transfer/receiver.py").read_bytes()
                value = receiver.receive(parsed, stream, receiver_source=content)
            else:
                parsed, stream = source_owner.read_request(io.BytesIO(transfer._json(payload)))
                content = (self.origin.directory(GUIDE) / "source-transfer/source-owner.py").read_bytes()
                value = source_owner.receive(parsed, stream, owner_source=content)
                if payload["action"] == "finish":
                    self.lose("source-finish-reply")
            return {**value, "_transport": {"host_key_fingerprint": self.key}}
        # Original inert transport observes real source transfer separately;
        # this final admission reply does not execute a guest worker or Fresh.
        if expected_fingerprint is not None:
            assert endpoint == ENDPOINT and expected_fingerprint == self.key
        value = await super().request(endpoint, program, payload, timeout, expected_fingerprint)
        if payload["action"] == "install":
            self.lose("install-reply")
        return value
    async def transfer_file(self, endpoint, program, payload, artifact, timeout,
            *, expected_fingerprint, require_current, deadline=None):
        lane = self.retained(endpoint, payload, expected_fingerprint, deadline)
        require_current()
        if lane == "artifact":
            parsed, stream = receiver.read_request(io.BytesIO(frame(payload, artifact.path.read_bytes())))
            content = (self.origin.directory(GUIDE) / "artifact-transfer/receiver.py").read_bytes()
            value = receiver.receive(parsed, stream, receiver_source=content)
            if payload["action"] == "manifest":
                self.lose("artifact-manifest-reply")
        elif lane == "source":
            parsed, stream = source_owner.read_request(io.BytesIO(frame(payload, artifact.path.read_bytes())))
            content = (self.origin.directory(GUIDE) / "source-transfer/source-owner.py").read_bytes()
            value = source_owner.receive(parsed, stream, owner_source=content)
        else:
            self.lose("control-before-owner")
            directory = self.origin.directory(GUIDE)
            content = (directory / "controller.py").read_bytes()
            assert program == controller_program(content)
            # Execute the trusted stdlib controller only, not received source.
            namespace = {"__name__": "inert_original_controller", "CONTROLLER_SOURCE": content}
            exec(compile(content, "<trusted-control-source-continuation>", "exec"), namespace)
            manifest = json.loads((directory / "product-control.json").read_bytes())
            request = {**payload, "transfer_file": manifest["bundle"]}
            value = namespace["handle"](request, product_stream=io.BytesIO(artifact.path.read_bytes()))
            assert "error" not in value
            self.lose("control-after-owner-reply")
        require_current()
        return {**value, "_transport": {"host_key_fingerprint": self.key}}


@pytest.fixture
async def origin_product(request, tmp_path, monkeypatch, inputs):
    monkeypatch.setattr(product_fixtures, "TARGET", "linux-x86_64")
    prefix = "meerkat-connect-linux-x86_64"
    monkeypatch.setattr(product_fixtures, "PREFIX", prefix)
    monkeypatch.setattr(product_fixtures, "RESOURCE", prefix + "/libexec/meerkat-connect")
    monkeypatch.setattr(platform, "system", lambda: "Linux")
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(platform, "libc_ver", lambda: ("glibc", "2.31"))
    product = request.getfixturevalue("product")
    target = catalog_target(product)
    home = tmp_path.resolve() / "actual inert destination account"
    home.mkdir(mode=0o700)
    destination = home / "project"
    monkeypatch.setattr(pwd, "getpwuid", lambda uid: SimpleNamespace(pw_dir=str(home)))
    spec = setup_spec()
    spec["target"] = "linux"
    guide = Guide()
    guide.identity = GUIDE
    guide.state = {"id": GUIDE, "version": 3, "proposal": spec}
    files = {item.path: item.as_bytes() for item in compiler.generate_package(spec, runtime_target=target)}
    # The compiler deliberately leaves protected application policy/viewer
    # source to its supplying owner. Reuse that existing original input fixture
    # instead of omitting required policy/access bytes or relaxing intake.
    files.update({name: path.read_bytes() for name, path in inputs.items()})
    assert {"application/tool-policy.json", "application/configuration.json",
        "application/console.json", "config/access.toml"} <= set(files)
    files.update({
        "implementation-report.json": transfer._json({"guide_id": GUIDE, "guide_version": 3,
            "build_id": BUILD, "brief_hash": fingerprint({}), "blueprint_hash": proposal_hash(spec),
            "verification": {"digest": SOURCE}, "framework": {"runtime_candidate": candidate_selection(target)}})})
    payload, files = package_bytes(files)
    implementation = AcceptedProduct(guide, target, payload)
    transport = OriginalFramedTransport(home, destination)
    selected = SimpleNamespace(selected=SimpleNamespace(custody=SimpleNamespace(
        home=tmp_path.resolve(), plan=SimpleNamespace(origin=target))), current=True)
    selection = selected_control(selected)
    local = DeploymentService(tmp_path / "origin", guide, implementation)
    service = RemoteDeploymentService(tmp_path / "origin", guide, implementation, local,
        transport=transport, product_control_selection=selection)
    transport.origin = service
    value = await service._connect(GUIDE, 3, BUILD, ENDPOINT, str(destination), product_owner=selection)
    await asyncio.gather(*list(service.tasks.values()))
    directory, receipt = service.stored(GUIDE)
    assert receipt["status"] == "configuration-required" and receipt["preflight"]["ready"]
    return SimpleNamespace(service=service, transport=transport, guide=guide, implementation=implementation,
        selection=selection, home=home, destination=destination, directory=directory,
        deployment_id=value["deployment_id"], files=files,
        settings={"OPENAI_API_KEY": "inert-origin-key", "CONSOLE_USER_ID": "fixture-operator",
            "CONSOLE_LOGIN_PASSWORD": "inert-original-private-password"})


@pytest.mark.parametrize("boundary", ["artifact-manifest-reply", "source-finish-reply",
    "control-before-owner", "control-after-owner-reply", "install-reply"])
async def test_original_origin_lost_response_continues_only_same_transfer_intents_and_uuid(origin_product, boundary):
    f = origin_product
    f.transport.failure = boundary
    await f.service.install(GUIDE, 3, BUILD, f.deployment_id, f.settings, product_owner=f.selection)
    first = read_receipt(f.directory)
    operation = first["install_operation_id"]
    assert first["pending_operation"] == {"id": operation, "action": "install"}
    assert f.transport.failed and first["status"] == "observation-unavailable"
    retained = {path: path.read_bytes() for folder in ("artifact-transfer", "source-transfer")
        for path in (f.directory / folder).rglob("*") if path.is_file()}
    frames = len(f.transport.frames)
    await f.service.install(GUIDE, 3, BUILD, f.deployment_id, f.settings, product_owner=f.selection)
    after = read_receipt(f.directory)
    assert after["install_operation_id"] == operation and after["product_phase"] == "byte-install"
    assert all(path.read_bytes() == content for path, content in retained.items())
    installs = [row["payload"] for row in f.transport.calls if row["payload"]["action"] == "install"]
    assert len(installs) == 1 and installs[0]["operation_id"] == operation
    if boundary == "install-reply":
        assert len(f.transport.frames) == frames
        assert f.transport.calls[-1]["payload"]["action"] == "status"
    else:
        assert len(f.transport.frames) > frames
        assert all(row["context"]["transaction_id"] == f.deployment_id
            for lane, row in f.transport.frames if lane == "artifact")
        assert all(row["context"]["deployment_id"] == f.deployment_id
            for lane, row in f.transport.frames if lane == "source")
        if boundary == "source-finish-reply":
            assert sum(lane == "source" and row["action"] == "package" for lane, row in f.transport.frames) == 1
        source_store = f.home / ".local/state/meta-forge/product-sources" / f.deployment_id
        assert json.loads((source_store / "owner.json").read_bytes())["deployment_id"] == f.deployment_id
        assert {name: (f.destination / name).read_bytes() for name in f.files} == f.files
    assert len([row for row in f.transport.calls if row["payload"]["action"] == "preflight"]) == 2
    assert not (f.destination / ".env").exists() and not (f.destination / ".venv").exists()
    public_bytes = transfer._json(after)
    assert f.settings["OPENAI_API_KEY"].encode() not in public_bytes
    assert f.settings["CONSOLE_LOGIN_PASSWORD"].encode() not in public_bytes


async def test_original_transfer_continuation_refuses_changed_retained_source_intent(origin_product):
    f = origin_product
    f.transport.failure = "source-finish-reply"
    await f.service.install(GUIDE, 3, BUILD, f.deployment_id, f.settings, product_owner=f.selection)
    before = read_receipt(f.directory)
    intent = f.directory / "source-transfer/intent.json"
    changed = intent.read_bytes() + b"\n"
    intent.write_bytes(changed)
    await f.service.install(GUIDE, 3, BUILD, f.deployment_id, f.settings, product_owner=f.selection)
    after = read_receipt(f.directory)
    assert after["install_operation_id"] == before["install_operation_id"]
    assert after["pending_operation"] == before["pending_operation"] and intent.read_bytes() == changed
    assert not any(row["payload"]["action"] == "install" for row in f.transport.calls)
    assert not (f.destination / ".env").exists() and not (f.destination / ".venv").exists()


async def test_freezer_refuses_requirements_mutated_after_real_authenticated_dependency_read(origin_product, monkeypatch):
    from launchkit.templates import install_candidate
    f = origin_product
    actual = install_candidate._read_selected_dependency_wheels
    requirements = f.selection._selection[1]
    def after_authenticated_read(*args, **kwargs):
        value = actual(*args, **kwargs)
        if args[0] is requirements:
            before = requirements.path.read_bytes()
            changed = before.replace(b"pydantic==1.0", b"pydantic==9.0")
            assert changed != before and len(changed) == len(before)
            requirements.path.write_bytes(changed)
        return value
    monkeypatch.setattr(install_candidate, "_read_selected_dependency_wheels", after_authenticated_read)
    with pytest.raises(ValueError, match="control requirements changed during freezing"):
        control.freeze_product_control(f.selection)
    assert not f.destination.exists() and not f.transport.frames
