"""Generated no-hook application + actual ASGI/adapter, with inert SDK calls.

This proves host enrollment wiring and lifetime fences, not native execution.
The existing adapter fixture retains real SDK types but opens no private pipe.
"""
import asyncio
import json
import sys

import httpx
import jwt
import pytest

from test_application_controller_binding import ADAPTER, HOOK, change_binding, package
from test_console_auth import login
import test_native_console_login as native
from test_native_console_login import adopted_source_imports


def ordinary(tmp_path, monkeypatch):
    monkeypatch.delitem(sys.modules, "native_console_login", raising=False)
    value = package(tmp_path, monkeypatch, adapter=ADAPTER.replace(HOOK, ""))
    sdk = sys.modules["meerkat_toolkit.host_control"]
    enrollment = native.Enrollment()
    client = native.Client(enrollment)

    class Runtime(sdk.ToolkitMobKitRuntime):
        def __new__(cls):
            # An inert typed double; neither SDK runtime constructor is entered.
            return object.__new__(cls)

        def __init__(self):
            self.client = client

        @property
        def host_control(self):
            return self.client

    value.context.runtime = Runtime()
    return value, client, enrollment


async def test_ordinary_application_enrolls_before_governed_send_and_retires_on_logout(tmp_path, monkeypatch):
    value, private, enrollment = ordinary(tmp_path, monkeypatch)
    await value.application.load()
    forwarded = []
    front = None

    def upstream(request):
        assert enrollment.result_release.is_set()
        auth = sys.modules[value.module.ConsoleFront.__module__]
        claims = jwt.decode(request.headers["authorization"].removeprefix("Bearer "),
            front.key, algorithms=["HS256"], audience=auth.AUDIENCE, issuer=auth.ISSUER)
        selected = private.selections[0]
        assert (claims["iss"], claims["sub"], claims["sid"]) == (
            selected.issuer, selected.subject, selected.login_scope_id)
        forwarded.append(request)
        body = json.loads(request.content)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": {
            "identity": "lead:main", "interaction_id": "interaction-1", "session_id": "session-1",
            "input_frame_id": "frame-1", "cursor": "console:1", "status": "accepted"}})

    front = value.module.ConsoleFront(value.root, 8855, "local-user", "local-password-123",
        value.application.on_console_input, console_login_factory=value.application.console_login_factory,
        runtime_namespace="meta", transport=httpx.MockTransport(upstream))
    app = front.application("http://127.0.0.1:61347")
    pending = None
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=front.origin) as client:
            await login(client, front)
            await native.entered(private.begin_entered)
            await native.entered(enrollment.result_entered)
            pending = asyncio.create_task(client.post("/console/rpc", headers={"Origin": front.origin}, json={
                "jsonrpc": "2.0", "id": "send-1", "method": "meta/console/send", "params": {
                    "identity": "lead:main", "content": "An ordinary task", "origin": "console:test",
                    "origin_kind": "operator", "idempotency_key": "request-1", "handling_mode": "queue"}}))
            await asyncio.sleep(0)
            assert not pending.done() and forwarded == []
            assert len(private.selections) == 1
            enrollment.result_release.set()
            response = await asyncio.wait_for(pending, 3)
            assert response.status_code == 200 and response.json()["result"]["interaction_id"] == "interaction-1"
            assert len(forwarded) == 1 and len(private.selections) == 1
            assert not hasattr(value.application.integration, "console_login_factory")
            assert not hasattr(value.application.integration, "controller_review_factory")
            with pytest.raises(ValueError, match="frontend support is undeclared"):
                value.application.frontend_host()
            assert (await client.post("/logout", headers={"Origin": front.origin})).status_code == 303
            assert enrollment.retire_calls == 1 and private.close_calls == 0
            assert (await client.post("/console/rpc", headers={"Origin": front.origin}, json={})).status_code == 401
    finally:
        enrollment.result_release.set()
        if pending is not None:
            await asyncio.gather(pending, return_exceptions=True)
        await front.close()
        await value.application.stop()


async def test_ordinary_enrollment_failure_never_forwards_or_reenrolls(tmp_path, monkeypatch):
    value, private, enrollment = ordinary(tmp_path, monkeypatch)
    await value.application.load()
    enrollment.result_error = RuntimeError("INERT_ENROLLMENT_REFUSAL")
    enrollment.result_release.set()
    forwarded = []
    front = value.module.ConsoleFront(value.root, 8855, "local-user", "local-password-123",
        console_login_factory=value.application.console_login_factory, runtime_namespace="meta",
        transport=httpx.MockTransport(lambda request: forwarded.append(request) or httpx.Response(200)))
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=front.application("http://127.0.0.1:61347")),
                base_url=front.origin) as client:
            await login(client, front)
            assert (await client.get("/console")).status_code == 503
            assert (await client.get("/console")).status_code == 503
            assert len(private.selections) == 1 and forwarded == []
    finally:
        await front.close()
        await value.application.stop()
    assert enrollment.retire_calls == 1 and private.close_calls == 0


@pytest.mark.parametrize("case", ["replaced-runtime", "application-source", "authentication-source",
    "imports-retired", "stop", "login", "private-client"])
async def test_default_login_selection_keeps_exact_original_runtime_client_source_and_login(tmp_path, monkeypatch, case):
    value, private, enrollment = ordinary(tmp_path, monkeypatch)
    try:
        await value.application.load()
        owner = value.application.console_login_factory(value.login)
        assert owner.is_current() is True and private.selections == []
        if case == "private-client":
            value.context.runtime.client = native.Client(native.Enrollment())
        else:
            change_binding(value, case)
        assert owner.is_current() is False
        error_type = sys.modules[owner.__class__.__module__].NativeConsoleLoginError
        with pytest.raises(error_type) as refusal:
            owner.start()
        assert refusal.value.code == "invalid_binding"
        assert private.selections == [] and private.close_calls == 0
    finally:
        enrollment.result_release.set()
        await value.application.stop()
