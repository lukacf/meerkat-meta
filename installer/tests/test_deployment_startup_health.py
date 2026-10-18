"""Real loopback HTTP and private child fixtures, not native deployment proof."""
import asyncio
from copy import deepcopy
import json
import os
import sys
import time
from types import SimpleNamespace

import pytest

from launchkit.deployment import materialize, read_receipt, update_receipt
from launchkit.deployment_worker import Supervisor
from test_deployment import Guide, Implementation, Service, submit


TOKEN = "private-health-fixture-token"


class ConsoleFixture:
    def __init__(self, responses, *, blocked=False):
        self.responses = responses
        self.blocked = blocked
        self.requests = []
        self.admitted = asyncio.Event()
        self.release = asyncio.Event()
        self.handlers = set()

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self.handlers.add(task)
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            self.requests.append(request)
            self.admitted.set()
            if self.blocked:
                await self.release.wait()
            response = self.responses[min(len(self.requests) - 1, len(self.responses) - 1)]
            status, payload = response
            if b"Authorization: Bearer " + TOKEN.encode() not in request:
                status, payload = 401, {"private": "untrusted token must not be logged"}
            body = json.dumps(payload).encode()
            writer.write((f"HTTP/1.1 {status} fixture\r\nContent-Type: application/json\r\n"
                          f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode() + body)
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            self.handlers.discard(task)

    async def close(self):
        self.release.set()
        for task in list(self.handlers):
            task.cancel()
        await asyncio.gather(*self.handlers, return_exceptions=True)


@pytest.fixture
async def fixture(tmp_path, monkeypatch):
    import launchkit.deployment_worker as module
    import launchkit.deployment as deployment
    # This fixture's HTTP listener stands in for an already-booted Console;
    # the separate dummy child does not bind it. Keep startup health assertions
    # independent of the real prelaunch bind observation.
    available = lambda port: {"scope": "local-console-port-observation", "port": port,
        "status": "available", "observed_at": "2026-10-03T08:00:00+00:00"}
    monkeypatch.setattr(module, "require_available_console_port", available)
    monkeypatch.setattr(deployment, "require_available_console_port", available)
    monkeypatch.setattr(module, "CONSOLE_START_TIMEOUT", 0.15)
    monkeypatch.setattr(module, "CONSOLE_START_RETRY_INTERVAL", 0.01)
    monkeypatch.setattr(module, "console_health_headers", lambda destination: {"Authorization": "Bearer " + TOKEN})
    consoles, servers, supervisors = [], [], []

    async def create(responses, *, blocked=False, child_lifetime=60):
        console = ConsoleFixture(responses, blocked=blocked)
        server = await asyncio.start_server(console.handle, "127.0.0.1", 0)
        consoles.append(console)
        servers.append(server)
        port = server.sockets[0].getsockname()[1]
        guide = Guide()
        implementation = Implementation(guide)
        definition = deepcopy(implementation.definition)
        definition["port"] = implementation.definition["port"] = port
        service = Service(tmp_path / str(port) / "private", guide, implementation)
        destination = tmp_path / str(port) / "application"
        await submit(service, destination)
        directory = service._directory(guide.identity)
        materialize(directory)
        await service.shutdown()
        interpreter = destination / ".venv/bin/python"
        interpreter.parent.mkdir(parents=True)
        interpreter.symlink_to(sys.executable)
        settings = destination / ".env"
        settings.write_text(settings.read_text().replace("MOBKIT_PORT=18088", "MOBKIT_PORT=" + str(port)))
        bootstrap = {"complete": True, "ready": True, "counts": {"broken": 0}, "identities": {"lead:main": {}}}
        (destination / "app.py").write_text(
            "import json,os,time\n"
            f"payload={{'pid':os.getpid(),'bootstrap':{bootstrap!r},'console_url':'http://127.0.0.1:{port}/console'}}\n"
            "fd=int(os.environ['MOBKIT_LAUNCH_READY_FD'])\n"
            "os.write(fd,(json.dumps(payload)+'\\n').encode())\nos.close(fd)\n"
            f"time.sleep({child_lifetime!r})\n")
        supervisor = Supervisor(directory)
        supervisors.append(supervisor)
        return supervisor, definition, console

    yield create
    for supervisor in supervisors:
        supervisor.stop.set()
        if supervisor.child:
            await supervisor.terminate()
    for server in servers:
        server.close()
    for console in consoles:
        await console.close()
    for server in servers:
        await server.wait_closed()


def assert_unready(supervisor):
    receipt = read_receipt(supervisor.directory)
    assert receipt["status"] == "starting"
    assert receipt["health"] == "unknown" and receipt["console_url"] is None
    assert supervisor.child is None
    assert TOKEN not in json.dumps(receipt)
    return receipt


async def test_delayed_authenticated_console_and_complete_identities_gate_ready(fixture):
    supervisor, definition, console = await fixture([
        (503, {"private": "response content must never be persisted"}),
        (200, {"identities": []}),
        (200, {"identities": [{"identity": "lead:main"}]}),
    ])
    published = []

    def ready():
        receipt = read_receipt(supervisor.directory)
        assert len(console.requests) == 3 and receipt["health"] == "unknown"
        assert receipt["status"] == "starting" and receipt["console_url"] is None
        published.append(True)
        supervisor.stop.set()

    await asyncio.wait_for(supervisor.start(definition, on_ready=ready), 3)
    receipt = read_receipt(supervisor.directory)
    assert published == [True]
    assert receipt["startup_console_health"]["status"] == "pass"
    assert receipt["startup_console_health"]["attempts"] == 3
    assert receipt["startup_console_health"]["attempt_id"] == receipt["startup_attempt_id"]
    assert receipt["health"] == "ready"
    assert all(request.startswith(b"GET /console/identities ") for request in console.requests)
    assert TOKEN not in json.dumps(receipt) and "response content" not in json.dumps(receipt)
    assert supervisor.child is None


async def test_pre_bootstrap_eof_cannot_retain_previous_ready_health(fixture):
    supervisor, definition, console = await fixture([(200, {"identities": [{"identity": "lead:main"}]})])
    update_receipt(supervisor.directory, health="ready", health_checked_at="2026-09-30T23:00:00+00:00")
    (supervisor.destination / "app.py").write_text(
        "import os,time\n"
        "os.close(int(os.environ['MOBKIT_LAUNCH_READY_FD']))\n"
        "time.sleep(60)\n")
    published = []
    with pytest.raises(RuntimeError, match="closed its startup verification channel"):
        await asyncio.wait_for(supervisor.start(definition, on_ready=lambda: published.append(True)), 3)
    receipt = assert_unready(supervisor)
    assert receipt["health_checked_at"] is None
    assert receipt["startup_console_health"] is None
    assert not published and not console.requests


@pytest.mark.parametrize("status,payload,category", [
    (503, {"private": TOKEN}, "http_status"),
    (200, {"identities": [{"identity": "foreign:main"}]}, "missing_identities"),
    (200, {"identities": [{}]}, "invalid_response"),
])
async def test_never_ready_is_bounded_and_reports_only_safe_diagnostic(fixture, monkeypatch, status, payload, category):
    supervisor, definition, console = await fixture([(status, payload)])
    import launchkit.deployment_worker as module
    original = module.update_receipt
    observed = []
    def capture(directory, **changes):
        if "startup_console_health" in changes:
            observed.append(changes["startup_console_health"])
        return original(directory, **changes)
    monkeypatch.setattr(module, "update_receipt", capture)
    published = []
    before = time.monotonic()
    with pytest.raises(RuntimeError, match="actual Console identities health check failed") as error:
        await asyncio.wait_for(supervisor.start(definition, on_ready=lambda: published.append(True)), 3)
    assert time.monotonic() - before < 2 and len(console.requests) >= 2
    receipt = assert_unready(supervisor)
    assert not published and receipt["startup_console_health"]["status"] == "fail"
    assert receipt["startup_console_health"]["category"] in {category, "timeout"}
    assert any(value.get("category") == category for value in observed)
    if status != 200:
        assert any(value.get("http_status") == status for value in observed)
    assert TOKEN not in str(error.value) and "127.0.0.1" not in str(error.value)


@pytest.mark.parametrize("status", [401, 403])
async def test_authentication_rejection_does_not_retry_or_publish_ready(fixture, status):
    supervisor, definition, console = await fixture([(status, {"detail": TOKEN})])
    with pytest.raises(RuntimeError, match=f"authentication, HTTP {status}"):
        await asyncio.wait_for(supervisor.start(definition, on_ready=lambda: pytest.fail("must not publish ready")), 3)
    receipt = assert_unready(supervisor)
    assert len(console.requests) == 1
    assert receipt["startup_console_health"]["category"] == "authentication"
    assert receipt["startup_console_health"]["attempt_id"] == receipt["startup_attempt_id"]


async def test_private_authorization_failure_is_safe_and_never_sends(fixture, monkeypatch):
    supervisor, definition, console = await fixture([(200, {"identities": [{"identity": "lead:main"}]})])
    def invalid(destination):
        raise ValueError(TOKEN)
    monkeypatch.setattr("launchkit.deployment_worker.console_health_headers", invalid)
    with pytest.raises(RuntimeError, match="authorization_configuration") as error:
        await asyncio.wait_for(supervisor.start(definition), 3)
    receipt = assert_unready(supervisor)
    assert not console.requests and receipt["startup_console_health"]["category"] == "authorization_configuration"
    assert TOKEN not in str(error.value)


async def test_deadline_cancels_inflight_probe_and_reaps_owned_child(fixture):
    supervisor, definition, console = await fixture([(200, {"identities": [{"identity": "lead:main"}]})], blocked=True)
    with pytest.raises(RuntimeError, match="health check failed \\(timeout\\)"):
        await asyncio.wait_for(supervisor.start(definition), 3)
    receipt = assert_unready(supervisor)
    assert len(console.requests) == 1 and receipt["startup_console_health"]["category"] == "timeout"


async def test_successful_identity_response_parsed_after_deadline_never_publishes_ready(fixture, monkeypatch):
    import httpx
    supervisor, definition, console = await fixture([(200, {"identities": [{"identity": "lead:main"}]})])
    loop = asyncio.get_running_loop()
    offset = 0

    class ProbeClock:
        def time(self):
            return loop.time() + offset

        def __getattr__(self, name):
            return getattr(loop, name)

    clock = ProbeClock()
    original = httpx.Response.json

    def parsed_after_budget(response, *args, **kwargs):
        nonlocal offset
        payload = original(response, *args, **kwargs)
        # The HTTP response is genuinely successful. Advance only the host's
        # startup deadline clock as if synchronous parsing delayed completion;
        # asyncio's transport and cancellation timers keep their real clock.
        offset = 60
        return payload

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: clock)
    monkeypatch.setattr(httpx.Response, "json", parsed_after_budget)
    with pytest.raises(RuntimeError, match="health check failed \\(timeout\\)"):
        await asyncio.wait_for(supervisor.start(definition, on_ready=lambda: pytest.fail("late success must not publish ready")), 3)
    receipt = assert_unready(supervisor)
    assert len(console.requests) == 1
    assert receipt["startup_console_health"]["status"] == "fail"
    assert receipt["startup_console_health"]["category"] == "timeout"


async def test_stop_interrupts_inflight_probe_without_waiting_for_deadline(fixture, monkeypatch):
    monkeypatch.setattr("launchkit.deployment_worker.CONSOLE_START_TIMEOUT", 60)
    supervisor, definition, console = await fixture([(200, {"identities": [{"identity": "lead:main"}]})], blocked=True)
    task = asyncio.create_task(supervisor.start(definition, on_ready=lambda: pytest.fail("must not publish ready")))
    await asyncio.wait_for(console.admitted.wait(), 3)
    supervisor.stop.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    receipt = assert_unready(supervisor)
    assert receipt["startup_console_health"]["status"] == "interrupted"


async def test_child_exit_interrupts_inflight_probe_without_readiness(fixture, monkeypatch):
    monkeypatch.setattr("launchkit.deployment_worker.CONSOLE_START_TIMEOUT", 60)
    supervisor, definition, console = await fixture([(200, {"identities": [{"identity": "lead:main"}]})], blocked=True, child_lifetime=0.15)
    with pytest.raises(RuntimeError, match="child_exited"):
        await asyncio.wait_for(supervisor.start(definition, on_ready=lambda: pytest.fail("must not publish ready")), 3)
    receipt = assert_unready(supervisor)
    assert len(console.requests) == 1 and receipt["startup_console_health"]["category"] == "child_exited"


async def test_task_cancellation_reaps_child_and_does_not_publish_ready(fixture, monkeypatch):
    monkeypatch.setattr("launchkit.deployment_worker.CONSOLE_START_TIMEOUT", 60)
    supervisor, definition, console = await fixture([(200, {"identities": [{"identity": "lead:main"}]})], blocked=True)
    task = asyncio.create_task(supervisor.start(definition, on_ready=lambda: pytest.fail("must not publish ready")))
    await asyncio.wait_for(console.admitted.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert_unready(supervisor)


@pytest.fixture
def delayed_local_admission(tmp_path, monkeypatch):
    """No actual child or signals: admission and cleanup advance independently."""
    from launchkit import deployment_worker as worker
    admitted, release, cleanup, settled, drained = (asyncio.Event() for _ in range(5))
    calls, cancelled_spawns, journals = [], [], []
    process = SimpleNamespace(pid=7777777, returncode=None)

    class Pipe:
        async def read(self, size):
            await drained.wait()
            return b""

    process.stdout, process.stderr = Pipe(), Pipe()
    supervisor = Supervisor.__new__(Supervisor)
    supervisor.directory = tmp_path
    supervisor.destination = tmp_path
    supervisor.environment = {}
    supervisor.stop = asyncio.Event()
    supervisor.child = None
    supervisor.port = 18088
    supervisor.record_log = lambda *args: None
    failures = []

    async def spawn(*args, **options):
        assert options["start_new_session"] is True
        calls.append((args, options))
        admitted.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled_spawns.append(True)
            raise
        return process

    async def terminate(timeout=None):
        assert supervisor.child is process
        cleanup.set()
        await settled.wait()
        if failures:
            process.returncode = 0  # Parent exit alone is not confirmed group cleanup.
            drained.set()
            raise failures[0]
        process.returncode = 0
        drained.set()

    async def wait():
        await drained.wait()
        return process.returncode

    process.wait = wait
    supervisor.terminate = terminate
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(os, "killpg", lambda *_: pytest.fail("Inert admission controls cannot signal an OS group."))
    monkeypatch.setattr(worker, "read_receipt", lambda _: {
        "deployment_id": "inert-admission", "package_sha256": "a" * 64})
    monkeypatch.setattr(worker, "journal", lambda *args, **fields: journals.append(fields))
    monkeypatch.setattr(worker, "require_available_console_port", lambda port: {"status": "available"})

    def operation(mode):
        if mode == "start":
            return supervisor.start({"agents": []}, on_ready=lambda: pytest.fail("Interrupted admission cannot publish Ready."))
        if mode == "query":
            return supervisor.query(["inert-inspection"])
        return supervisor.command(["inert-command"], "inert", 1, raw_outcome=mode == "raw")

    return SimpleNamespace(supervisor=supervisor, process=process, admitted=admitted,
        release=release, cleanup=cleanup, settled=settled, drained=drained,
        calls=calls, cancelled_spawns=cancelled_spawns, journals=journals,
        failures=failures, operation=operation)


@pytest.mark.parametrize("mode", ["command", "raw", "query", "start"])
@pytest.mark.parametrize("trigger", ["cancel", "stop"])
async def test_delayed_local_child_admission_keeps_custody_through_interruption(delayed_local_admission, mode, trigger):
    fixture = delayed_local_admission
    task = asyncio.create_task(fixture.operation(mode))
    try:
        await asyncio.wait_for(fixture.admitted.wait(), 1)
        if trigger == "cancel":
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done(), "Entered admission must return its actual child before cancellation escapes."
            assert not fixture.cancelled_spawns
        else:
            fixture.supervisor.stop.set()
        fixture.release.set()
        await asyncio.wait_for(fixture.cleanup.wait(), .5)
        assert fixture.supervisor.child is fixture.process
        assert len(fixture.calls) == 1 and not fixture.cancelled_spawns
        # Cleanup is an entered operation too. A second caller cancellation
        # must join it rather than abandon custody or spawn a successor.
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert fixture.supervisor.child is fixture.process
        fixture.settled.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert fixture.supervisor.child is None and fixture.process.returncode == 0
        assert not any(item.get("status") == "running" for item in fixture.journals)
    finally:
        fixture.release.set()
        fixture.settled.set()
        fixture.drained.set()
        fixture.process.returncode = 0
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def exception_leaves(error):
    if isinstance(error, BaseExceptionGroup):
        return [leaf for child in error.exceptions for leaf in exception_leaves(child)]
    return [error]


@pytest.mark.parametrize("mode", ["command", "raw", "query", "start"])
async def test_failed_local_admission_cleanup_preserves_cancellation_and_exact_child(delayed_local_admission, mode):
    fixture = delayed_local_admission
    failure = PermissionError("Inert group cleanup could not be confirmed.")
    fixture.failures.append(failure)
    task = asyncio.create_task(fixture.operation(mode))
    try:
        await asyncio.wait_for(fixture.admitted.wait(), 1)
        task.cancel()
        await asyncio.sleep(0)
        fixture.release.set()
        await asyncio.wait_for(fixture.cleanup.wait(), .5)
        task.cancel()
        await asyncio.sleep(0)
        fixture.settled.set()
        with pytest.raises(BaseExceptionGroup) as caught:
            await asyncio.wait_for(task, 1)
        leaves = exception_leaves(caught.value)
        assert failure in leaves and any(isinstance(item, asyncio.CancelledError) for item in leaves)
        assert fixture.supervisor.child is fixture.process
        assert fixture.process.returncode == 0 and len(fixture.calls) == 1
        # A reaped parent with unconfirmed cleanup cannot be overwritten by
        # another command, query or start selected on this same owner.
        with pytest.raises(RuntimeError, match="previous owned child"):
            await fixture.operation(mode)
        assert fixture.supervisor.child is fixture.process and len(fixture.calls) == 1
    finally:
        fixture.release.set()
        fixture.settled.set()
        fixture.drained.set()
        fixture.process.returncode = 0
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_local_query_observes_stop_after_admission(delayed_local_admission):
    fixture = delayed_local_admission
    reading = asyncio.Event()
    original = fixture.process.stdout.read

    async def read(size):
        reading.set()
        return await original(size)

    fixture.process.stdout.read = read
    fixture.release.set()
    task = asyncio.create_task(fixture.operation("query"))
    try:
        await asyncio.wait_for(reading.wait(), 1)
        fixture.supervisor.stop.set()
        await asyncio.wait_for(fixture.cleanup.wait(), .5)
        fixture.settled.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert fixture.supervisor.child is None and len(fixture.calls) == 1
    finally:
        fixture.settled.set()
        fixture.drained.set()
        fixture.process.returncode = 0
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_local_query_preserves_original_failure_and_cleanup_cancellation(delayed_local_admission):
    fixture = delayed_local_admission
    original = ValueError("Inert inspection stream failure.")
    cleanup_failure = PermissionError("Inert group cleanup could not be confirmed.")
    fixture.failures.append(cleanup_failure)

    async def read(size):
        raise original

    fixture.process.stdout.read = read
    fixture.release.set()
    task = asyncio.create_task(fixture.operation("query"))
    try:
        await asyncio.wait_for(fixture.cleanup.wait(), .5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        fixture.settled.set()
        with pytest.raises(BaseExceptionGroup) as caught:
            await asyncio.wait_for(task, 1)
        leaves = exception_leaves(caught.value)
        assert original in leaves and cleanup_failure in leaves
        assert any(isinstance(item, asyncio.CancelledError) for item in leaves)
        assert fixture.supervisor.child is fixture.process and len(fixture.calls) == 1
    finally:
        fixture.settled.set()
        fixture.drained.set()
        fixture.process.returncode = 0
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("mode", ["command", "raw", "query", "start"])
async def test_already_stopped_local_supervisor_does_not_enter_child_admission(delayed_local_admission, mode):
    fixture = delayed_local_admission
    fixture.supervisor.stop.set()
    with pytest.raises(asyncio.CancelledError):
        await fixture.operation(mode)
    assert not fixture.calls and fixture.supervisor.child is None


@pytest.mark.parametrize("first_mode", ["command", "raw", "query", "start"])
@pytest.mark.parametrize("second_mode", ["command", "raw", "query", "start"])
async def test_local_admission_slot_is_reserved_before_spawn_yields(delayed_local_admission, first_mode, second_mode):
    fixture = delayed_local_admission
    first = asyncio.create_task(fixture.operation(first_mode))
    second = None
    try:
        await asyncio.wait_for(fixture.admitted.wait(), 1)
        second = asyncio.create_task(fixture.operation(second_mode))
        done, _ = await asyncio.wait({second}, timeout=.05)
        assert len(fixture.calls) == 1, "One Supervisor cannot admit a second child while its first handle is pending."
        assert second in done, "A concurrent caller must be refused before waiting for the first admission."
        with pytest.raises(RuntimeError, match="admission is still in progress"):
            await second
        first.cancel()
        await asyncio.sleep(0)
        fixture.release.set()
        await asyncio.wait_for(fixture.cleanup.wait(), .5)
        fixture.settled.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(first, 1)
        assert fixture.supervisor.child is None and len(fixture.calls) == 1
    finally:
        fixture.release.set()
        fixture.settled.set()
        fixture.drained.set()
        fixture.process.returncode = 0
        tasks = [task for task in (first, second) if task is not None]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("mode", ["command", "raw", "query", "start"])
async def test_failed_local_spawn_releases_only_its_admission_slot(delayed_local_admission, monkeypatch, mode):
    fixture = delayed_local_admission
    original_spawn = asyncio.create_subprocess_exec
    failure = OSError("Inert admission failure before any child handle.")
    attempts = []

    async def spawn(*args, **options):
        attempts.append(True)
        if len(attempts) == 1:
            raise failure
        return await original_spawn(*args, **options)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    with pytest.raises(OSError) as caught:
        await fixture.operation(mode)
    assert caught.value is failure
    assert fixture.supervisor._child_admission is None and fixture.supervisor.child is None
    task = asyncio.create_task(fixture.operation(mode))
    try:
        await asyncio.wait_for(fixture.admitted.wait(), 1)
        task.cancel()
        await asyncio.sleep(0)
        fixture.release.set()
        await asyncio.wait_for(fixture.cleanup.wait(), .5)
        fixture.settled.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert fixture.supervisor.child is None and fixture.supervisor._child_admission is None
        assert len(attempts) == 2 and len(fixture.calls) == 1
    finally:
        fixture.release.set()
        fixture.settled.set()
        fixture.drained.set()
        fixture.process.returncode = 0
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("mode", ["command", "raw", "query", "start"])
async def test_cancelled_late_spawn_failure_preserves_errors_then_releases_local_slot(delayed_local_admission, monkeypatch, mode):
    fixture = delayed_local_admission
    original_spawn = asyncio.create_subprocess_exec
    failure = OSError("Inert late admission failure without a returned child.")
    attempts = []

    async def spawn(*args, **options):
        attempts.append(True)
        if len(attempts) == 1:
            fixture.admitted.set()
            await fixture.release.wait()
            raise failure
        return await original_spawn(*args, **options)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    task = asyncio.create_task(fixture.operation(mode))
    retry = None
    try:
        await asyncio.wait_for(fixture.admitted.wait(), 1)
        task.cancel("first-admission-cancellation")
        await asyncio.sleep(0)
        task.cancel("second-admission-cancellation")
        await asyncio.sleep(0)
        assert not task.done()
        fixture.release.set()
        with pytest.raises(BaseExceptionGroup) as caught:
            await asyncio.wait_for(task, 1)
        leaves = exception_leaves(caught.value)
        assert failure in leaves
        assert any(isinstance(error, asyncio.CancelledError) and error.args == ("first-admission-cancellation",)
                   for error in leaves)
        assert fixture.supervisor.child is None and fixture.supervisor._child_admission is None
        assert not fixture.cleanup.is_set()  # No actual handle was returned to clean.

        fixture.admitted.clear()
        fixture.release.clear()
        retry = asyncio.create_task(fixture.operation(mode))
        await asyncio.wait_for(fixture.admitted.wait(), 1)
        retry.cancel()
        await asyncio.sleep(0)
        fixture.release.set()
        await asyncio.wait_for(fixture.cleanup.wait(), .5)
        fixture.settled.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(retry, 1)
        assert fixture.supervisor.child is None and fixture.supervisor._child_admission is None
        assert len(attempts) == 2 and len(fixture.calls) == 1
    finally:
        fixture.release.set()
        fixture.settled.set()
        fixture.drained.set()
        fixture.process.returncode = 0
        tasks = [current for current in (task, retry) if current is not None]
        for current in tasks:
            if not current.done():
                current.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
