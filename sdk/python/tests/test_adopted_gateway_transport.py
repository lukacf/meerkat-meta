"""Adopting a gateway child the host started and keeps.

A host that supervises the gateway process hands the SDK that exact child
(``AdoptedGatewayChild``): its stdin and stdout pipes, a non-blocking
``request_retirement`` and a bounded ``wait_process``. These tests drive an
in-process stand-in: a thread plays the gateway child over two real pipes,
and a fake host retires it on its own event loop, on its own thread. No test
starts a process, and every test fails if ``subprocess.Popen`` or ``os.kill``
is called.

One contract each:
- the builder, and a constructor-given transport, adopt the child and never
  spawn, even with ``gateway()`` set;
- init settles ready over the adopted pipes and yields the HTTP paths;
- a callback handler registered before start answers during init;
- sync and async RPC round trips, and request timeouts;
- a stop requests the host's retirement once however often it runs,
  observes the child through ``wait_process`` on a worker thread, never
  kills, and refuses an event-loop thread;
- ``shutdown()``, a failed ``build()`` and the cleanup it hands its caller
  run that stop off the event loop;
- a pending exit is a typed outcome and the observation is kept;
- a failed start requests the retirement and observes the child the same
  way, and a cancelled start keeps that observation and its uncertainty;
- refusals before the hand-off leave the pipes untouched;
- the SDK never runs or awaits the host's own cleanup, and the reader never
  calls the host (no circular join);
- the native terminal failure, the child's exit and the SDK reader's
  completion stay apart.
"""
from __future__ import annotations

import asyncio
import io
import json
import os
import select
import subprocess
import threading
from typing import Any, Callable

import pytest

import meerkat_meta._adopted_gateway as adopted_module
from meerkat_meta import (
    AdoptedGatewayChild,
    AdoptedGatewayTransport,
    GatewayChildExit,
    GatewayChildPendingError,
    GatewayStartFailure,
    GatewayStartupCleanupError,
    GatewayTransportConflictError,
    RpcError,
    StorageResolutionError,
    TransportError,
    TransportTermination,
)
from meerkat_meta.builder import Meta
from meerkat_meta.runtime import MetaRuntime

# The longest any single expectation may take.
_WAIT = 5.0
# The host's own event-loop thread.
_HOST_LOOP = "host-loop"


@pytest.fixture(autouse=True)
def refuse_processes(monkeypatch):
    """No test may start, signal or kill a process."""

    def refuse_spawn(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs  # the call carries the whole environment
        raise AssertionError("an adopted gateway must never be spawned")

    def refuse_signal(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("an adopted gateway must never be signalled")

    monkeypatch.setattr(subprocess, "Popen", refuse_spawn)
    monkeypatch.setattr(os, "kill", refuse_signal)
    if hasattr(os, "killpg"):
        monkeypatch.setattr(os, "killpg", refuse_signal)


class FakeGatewayChild:
    """A thread that plays the gateway child over two real pipes.

    ``script(child)`` runs on the thread and returns the exit code. Returning
    is the child's exit: both of its pipe ends close, as a process's do.
    """

    def __init__(self, script: Callable[[FakeGatewayChild], int]):
        stdin_read, stdin_write = os.pipe()
        stdout_read, stdout_write = os.pipe()
        # The child's own ends.
        self.stdin = os.fdopen(stdin_read, "rb")
        self.stdout = os.fdopen(stdout_write, "wb")
        # The host's ends, handed to the SDK: fd-backed, like Popen's pipes.
        self.input = os.fdopen(stdin_write, "wb")
        self.output = os.fdopen(stdout_read, "rb")
        self.received: list[dict[str, Any]] = []
        self.exit_code: int | None = None
        self.exited = threading.Event()
        # Set by the host's retirement (or by a test standing in for it).
        self.retire = threading.Event()
        self.saw_end_of_input = threading.Event()
        self._script = script
        self._thread = threading.Thread(target=self._run, daemon=True, name="fake-gateway")

    def start(self) -> FakeGatewayChild:
        self._thread.start()
        return self

    def _run(self) -> None:
        code = 70
        try:
            code = self._script(self)
        finally:
            for end in (self.stdout, self.stdin):
                try:
                    end.close()
                except OSError:
                    pass
            self.exit_code = code
            self.exited.set()

    def read(self) -> dict[str, Any] | None:
        """The next line the SDK wrote, or ``None`` at the end of input."""
        line = self.stdin.readline()
        if not line:
            self.saw_end_of_input.set()
            return None
        message = json.loads(line)
        self.received.append(message)
        return message

    def emit(self, message: dict[str, Any]) -> None:
        self.stdout.write((json.dumps(message) + "\n").encode("utf-8"))
        self.stdout.flush()

    def methods(self) -> list[Any]:
        return [message.get("method") for message in self.received]

    def input_untouched(self) -> bool:
        """Nothing was written to the child's stdin and it was not closed."""
        readable, _, _ = select.select([self.stdin], [], [], 0)
        return not readable and not self.input.closed and not self.output.closed


class FakeHost:
    """The host's side: it keeps the child and retires it on its own loop.

    Its retirement is the host's own cleanup. The SDK may only request it;
    the cleanup runs on the host's loop thread, and a test can hold it open
    (``cleanup_gate``) or make it join the SDK's reader first.
    """

    def __init__(self, child: FakeGatewayChild, *, retire_ends_child: bool = True):
        self.child = child
        self.retire_ends_child = retire_ends_child
        self.loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self.loop.run_forever, daemon=True, name=_HOST_LOOP
        )
        self._loop_thread.start()
        self._tasks: list[asyncio.Task[None]] = []
        self.retirement_threads: list[str] = []
        self.wait_calls: list[tuple[float, str, bool]] = []
        self.cleanup_threads: list[str] = []
        self.cleanup_done = threading.Event()
        self.cleanup_gate: threading.Event | None = None
        self.cleanup_joins_reader_of: AdoptedGatewayTransport | None = None
        self.retirement_error: Exception | None = None

    def carrier(self) -> AdoptedGatewayChild:
        return AdoptedGatewayChild(
            input=self.child.input,
            output=self.child.output,
            request_retirement=self.request_retirement,
            wait_process=self.wait_process,
        )

    def request_retirement(self) -> None:
        self.retirement_threads.append(threading.current_thread().name)
        if self.retirement_error is not None:
            error, self.retirement_error = self.retirement_error, None
            raise error
        # Schedule the retirement on the host's own loop and return at once.
        self.loop.call_soon_threadsafe(self._start_retirement)

    def _start_retirement(self) -> None:
        self._tasks.append(self.loop.create_task(self._retire()))

    async def _retire(self) -> None:
        """The host's own cleanup: never run or awaited by the SDK."""
        self.cleanup_threads.append(threading.current_thread().name)
        if self.retire_ends_child:
            self.child.retire.set()
        joins = self.cleanup_joins_reader_of
        if joins is not None:
            # Joins the SDK's reader: a reader that waited for this cleanup
            # would never end.
            while not joins.reader_completed:
                await asyncio.sleep(0.01)
        gate = self.cleanup_gate
        if gate is not None:
            while not gate.is_set():
                await asyncio.sleep(0.01)
        self.cleanup_done.set()

    def wait_process(self, timeout: float) -> int:
        try:
            asyncio.get_running_loop()
            on_event_loop = True
        except RuntimeError:
            on_event_loop = False
        self.wait_calls.append((timeout, threading.current_thread().name, on_event_loop))
        if self.child.exited.wait(timeout):
            assert self.child.exit_code is not None
            return self.child.exit_code
        raise TimeoutError

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._loop_thread.join(_WAIT)
        self.loop.close()


@pytest.fixture
def adopt():
    """Make a stand-in child and its host; release both after the test."""
    made: list[tuple[FakeGatewayChild, FakeHost]] = []

    def make(
        script: Callable[[FakeGatewayChild], int], *, retire_ends_child: bool = True
    ) -> tuple[FakeGatewayChild, FakeHost]:
        child = FakeGatewayChild(script).start()
        host = FakeHost(child, retire_ends_child=retire_ends_child)
        made.append((child, host))
        return child, host

    yield make
    for child, host in made:
        if host.cleanup_gate is not None:
            host.cleanup_gate.set()
        child.retire.set()
        for stream in (child.input, child.output):
            try:
                stream.close()
            except OSError:
                pass
        child.exited.wait(_WAIT)
        host.close()


def accept_init(child: FakeGatewayChild, *, horizon_ms: int = 5_000) -> dict[str, Any]:
    init = child.read()
    assert init is not None and init["method"] == "meta/init"
    child.emit(
        {
            "jsonrpc": "2.0",
            "id": init["id"],
            "result": {
                "init_state": "accepted",
                "init_id": init["params"]["init_id"],
                "stdio_shutdown_handshake": True,
                "stdio_shutdown_horizon_ms": horizon_ms,
            },
        }
    )
    return init


def progress(child: FakeGatewayChild, init: dict[str, Any], phase: str) -> None:
    child.emit(
        {
            "jsonrpc": "2.0",
            "method": "meta/init_progress",
            "params": {"init_id": init["params"]["init_id"], "phase": phase},
        }
    )


def settle(child: FakeGatewayChild, init: dict[str, Any], **fields: Any) -> None:
    child.emit(
        {
            "jsonrpc": "2.0",
            "method": "meta/init_settled",
            "params": {"init_id": init["params"]["init_id"], **fields},
        }
    )


def serve(
    child: FakeGatewayChild, *, exit_code: int = 0, until_retired: bool = False
) -> int:
    """Answer requests until the input ends, then exit.

    With ``until_retired`` the child outlives its input until the host
    retires it.
    """
    while True:
        message = child.read()
        if message is None:
            break
        method = message.get("method")
        if method is None or method == "test/never_answered":
            continue
        if method == "meta/shutdown":
            result: dict[str, Any] = {"shutdown": True, "runtime_cleanup_completed": True}
        else:
            result = {"method": method, "params": message.get("params")}
        child.emit({"jsonrpc": "2.0", "id": message["id"], "result": result})
    if until_retired:
        child.retire.wait(30)
    return exit_code


def ready_gateway(
    *, exit_code: int = 0, until_retired: bool = False, horizon_ms: int = 5_000
) -> Callable[[FakeGatewayChild], int]:
    def script(child: FakeGatewayChild) -> int:
        init = accept_init(child, horizon_ms=horizon_ms)
        progress(child, init, "storage")
        settle(
            child,
            init,
            outcome="ready",
            http_base_url="http://127.0.0.1:9",
            http_public_base_url="https://gateway.example.test",
        )
        return serve(child, exit_code=exit_code, until_retired=until_retired)

    return script


def fails_closed(child: FakeGatewayChild) -> int:
    """Accepts init, settles it failed, and exits at once."""
    init = accept_init(child)
    settle(
        child,
        init,
        outcome="failed",
        code=-32014,
        message="refused for the test",
        durable_effects="possible",
    )
    return 0


def refuses_init(child: FakeGatewayChild) -> int:
    """Answers init with an error, then exits when its input ends."""
    init = child.read()
    assert init is not None
    child.emit(
        {
            "jsonrpc": "2.0",
            "id": init["id"],
            "error": {"code": -32602, "message": "refused for the test"},
        }
    )
    return serve(child)


async def wait_until(predicate: Callable[[], Any], timeout: float = _WAIT) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        assert loop.time() < deadline, "the expected state never came"
        await asyncio.sleep(0.01)


def start_build(
    transport: AdoptedGatewayTransport,
) -> tuple[asyncio.Task[MetaRuntime], dict[str, BaseException]]:
    """Run build() in a task that keeps the first exception it observes, as a
    caller without a runtime must (Python 3.10 re-raises a fresh
    CancelledError from a cancelled task)."""
    observed: dict[str, BaseException] = {}

    async def build() -> MetaRuntime:
        try:
            return await Meta.builder().gateway_transport(transport).build()
        except BaseException as error:
            observed["error"] = error
            raise

    return asyncio.create_task(build()), observed


def off_loop(host: FakeHost) -> bool:
    """Every wait_process call ran on a worker thread with a 0-60 s timeout."""
    return bool(host.wait_calls) and all(
        not on_event_loop and 0 <= timeout <= 60 and not thread.startswith(_HOST_LOOP)
        for timeout, thread, on_event_loop in host.wait_calls
    )


@pytest.mark.asyncio
async def test_the_builder_adopts_the_host_child_and_settles_ready(adopt):
    child, host = adopt(ready_gateway())
    transport = AdoptedGatewayTransport(host.carrier())
    runtime = await Meta.builder().gateway_transport(transport).build()
    try:
        assert runtime.is_running
        assert runtime.rust_http_base_url == "http://127.0.0.1:9"
        assert runtime.rust_http_public_base_url == "https://gateway.example.test"
        assert runtime.gateway_shutdown_horizon_ms == 5_000
        init_params = child.received[0]["params"]
        assert init_params["init_protocol"] == "accepted_then_settled"
        assert init_params["init_id"].startswith("init-")
        # Nothing of the host was used while the child serves.
        assert host.retirement_threads == [] and host.wait_calls == []
    finally:
        await runtime.shutdown()
    # The stock protocol shutdown: one shutdown request, then end of input.
    assert child.methods()[-1] == "meta/shutdown"
    assert child.saw_end_of_input.is_set()
    assert len(host.retirement_threads) == 1
    assert off_loop(host)
    assert transport.child_exit == GatewayChildExit("exited", 0)
    assert runtime.last_gateway_termination == TransportTermination(
        "exited", 0, None, "none", "acknowledged"
    )
    assert transport.reader_completed
    assert transport.start_failure is None
    assert not runtime.is_running


@pytest.mark.asyncio
async def test_a_callback_handler_set_before_start_answers_during_init(adopt):
    answers: list[dict[str, Any] | None] = []

    def script(child: FakeGatewayChild) -> int:
        init = accept_init(child)
        progress(child, init, "restore")
        child.emit(
            {
                "jsonrpc": "2.0",
                "id": "cb-1",
                "method": "callback/roster_provider/roster",
                "params": {"context": {}},
            }
        )
        answers.append(child.read())
        settle(child, init, outcome="ready", http_base_url="http://127.0.0.1:9")
        return serve(child)

    class Roster:
        def __init__(self) -> None:
            self.calls = 0

        async def roster(self, context: Any) -> list[Any]:
            self.calls += 1
            return []

    child, host = adopt(script)
    roster = Roster()
    transport = AdoptedGatewayTransport(host.carrier())
    runtime = await Meta.builder().roster(roster).gateway_transport(transport).build()
    await runtime.shutdown()
    assert child.received[0]["params"]["has_roster_provider"] is True
    assert roster.calls == 1
    # Answered by the provider: no "callback_handler_unavailable" refusal.
    assert answers == [{"jsonrpc": "2.0", "id": "cb-1", "result": []}]


@pytest.mark.asyncio
async def test_sync_and_async_rpc_and_request_timeouts(adopt):
    child, host = adopt(ready_gateway())
    transport = AdoptedGatewayTransport(host.carrier(), timeout=5.0)
    runtime = await Meta.builder().gateway_transport(transport).build()
    try:
        assert transport.request_timeout == 5.0
        assert await runtime._rpc("test/echo", {"n": 1}) == {
            "method": "test/echo",
            "params": {"n": 1},
        }
        answer = await asyncio.to_thread(
            transport.send_sync,
            {"jsonrpc": "2.0", "id": "sync-1", "method": "test/echo", "params": {"n": 2}},
        )
        assert answer["result"] == {"method": "test/echo", "params": {"n": 2}}
        never = {"jsonrpc": "2.0", "method": "test/never_answered", "params": {}}
        with pytest.raises(RuntimeError, match="timeout after 0.2s"):
            await transport.send_async({**never, "id": "late-1"}, timeout=0.2)
        with pytest.raises(RuntimeError, match="timeout after 0.2s"):
            await asyncio.to_thread(
                transport.send_sync, {**never, "id": "late-2"}, timeout=0.2
            )
        # A timed-out request leaves the channel usable.
        assert await runtime._rpc("test/echo", {"n": 3}) == {
            "method": "test/echo",
            "params": {"n": 3},
        }
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_repeated_stops_request_the_retirement_once_and_never_kill(adopt):
    child, host = adopt(ready_gateway())
    transport = AdoptedGatewayTransport(host.carrier())
    runtime = await Meta.builder().gateway_transport(transport).build()
    await runtime.shutdown()
    await runtime.shutdown()
    await asyncio.to_thread(transport.stop)
    await asyncio.to_thread(transport.stop)
    assert len(host.retirement_threads) == 1
    assert child.methods().count("meta/shutdown") == 1
    # The transport's view of an adopted child cannot wait on, signal or kill it.
    for method in ("wait", "terminate", "kill", "send_signal"):
        assert not hasattr(adopted_module._AdoptedProcess, method)
    # The host's retirement ran on the host's own loop, never on an SDK thread.
    await asyncio.to_thread(host.cleanup_done.wait, _WAIT)
    assert host.cleanup_threads == [_HOST_LOOP]
    assert transport.child_exit == GatewayChildExit("exited", 0)


@pytest.mark.asyncio
async def test_stop_refuses_the_event_loop_thread_and_runs_on_a_worker_thread(adopt):
    release_exit = threading.Event()

    def script(child: FakeGatewayChild) -> int:
        code = ready_gateway()(child)
        # Outlives its input until the test lets it exit.
        release_exit.wait(30)
        return code

    child, host = adopt(script, retire_ends_child=False)
    transport = AdoptedGatewayTransport(host.carrier())
    runtime = await Meta.builder().gateway_transport(transport).build()
    with pytest.raises(TransportError, match="worker thread"):
        transport.stop()
    # It did nothing: admission is open and nothing was asked or requested.
    assert runtime.is_running
    assert "meta/shutdown" not in child.methods()
    assert host.retirement_threads == []
    assert await runtime._rpc("test/echo", {}) == {"method": "test/echo", "params": {}}

    ticks = 0

    async def tick() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.01)

    ticker = asyncio.create_task(tick())
    stopping = asyncio.create_task(asyncio.to_thread(transport.stop))
    await wait_until(lambda: host.wait_calls)
    before = ticks
    await asyncio.sleep(0.1)
    # The loop kept running while the stop observed the child.
    assert ticks > before
    assert not stopping.done()
    release_exit.set()
    await asyncio.wait_for(stopping, _WAIT)
    ticker.cancel()
    assert transport.child_exit == GatewayChildExit("exited", 0)
    assert off_loop(host)
    # The runtime finds its transport stopped.
    await runtime.shutdown()
    assert len(host.retirement_threads) == 1


@pytest.mark.asyncio
async def test_shutdown_and_failed_builds_run_the_stop_off_the_loop(adopt, monkeypatch):
    # The worker-only stop, reached through the runtime's own paths: a builder
    # configured before build(), shutdown(), a failed build(), and the cleanup
    # a failed build hands its caller.
    loop_thread = threading.current_thread().name
    stops: list[tuple[str, bool]] = []
    stock_stop = AdoptedGatewayTransport.stop

    def observed_stop(self: AdoptedGatewayTransport) -> None:
        try:
            asyncio.get_running_loop()
            on_event_loop = True
        except RuntimeError:
            on_event_loop = False
        stops.append((threading.current_thread().name, on_event_loop))
        stock_stop(self)

    monkeypatch.setattr(AdoptedGatewayTransport, "stop", observed_stop)

    def configure_builder(transport: AdoptedGatewayTransport) -> Any:
        return Meta.builder().gateway_transport(transport)

    _, host = adopt(ready_gateway())
    runtime = await configure_builder(AdoptedGatewayTransport(host.carrier())).build()
    await runtime.shutdown()

    _, refused_host = adopt(refuses_init)
    with pytest.raises(RpcError):
        await configure_builder(AdoptedGatewayTransport(refused_host.carrier())).build()

    _, failed_host = adopt(fails_closed)
    with pytest.raises(GatewayStartupCleanupError) as raised:
        await configure_builder(AdoptedGatewayTransport(failed_host.carrier())).build()
    await raised.value.cleanup.shutdown()

    assert len(stops) == 4
    assert all(not on_event_loop and thread != loop_thread for thread, on_event_loop in stops)
    for each in (host, refused_host, failed_host):
        assert len(each.retirement_threads) == 1
        assert off_loop(each)


@pytest.mark.asyncio
async def test_a_pending_exit_is_typed_and_its_observation_is_kept(adopt, monkeypatch):
    # Scale the bound: a 100 ms horizon, 200 ms more, in 50 ms waits.
    monkeypatch.setattr(adopted_module, "_PENDING_RETRY_SECONDS", 0.2)
    monkeypatch.setattr(adopted_module, "_WAIT_PROCESS_MAX_SECONDS", 0.05)
    child, host = adopt(
        ready_gateway(exit_code=3, until_retired=True, horizon_ms=100),
        retire_ends_child=False,
    )
    transport = AdoptedGatewayTransport(host.carrier())
    runtime = await Meta.builder().gateway_transport(transport).build()
    with pytest.raises(GatewayChildPendingError) as raised:
        await runtime.shutdown()
    assert raised.value.child_exit == GatewayChildExit("pending", None)
    assert transport.child_exit == GatewayChildExit("pending", None)
    assert runtime.last_gateway_termination == TransportTermination(
        "unknown", None, None, "none", "acknowledged"
    )
    # Bounded retries, each a timeout of at most the cap, all off the loop.
    assert len(host.wait_calls) >= 3
    assert all(timeout <= 0.05 for timeout, _, _ in host.wait_calls)
    assert off_loop(host)
    assert len(host.retirement_threads) == 1
    # The runtime kept the transport: the next shutdown observes the same
    # child again, once the host has retired it.
    child.retire.set()
    await runtime.shutdown()
    assert transport.child_exit == GatewayChildExit("exited", 3)
    assert runtime.last_gateway_termination == TransportTermination(
        "exited", 3, None, "none", "acknowledged"
    )
    assert len(host.retirement_threads) == 1
    assert child.methods().count("meta/shutdown") == 1


@pytest.mark.asyncio
async def test_a_refused_init_retires_and_observes_the_child_without_a_spawn(adopt):
    child, host = adopt(refuses_init)
    transport = AdoptedGatewayTransport(host.carrier())
    with pytest.raises(RpcError) as raised:
        await (
            Meta.builder()
            .gateway("/nonexistent/rpc_gateway")
            .gateway_transport(transport)
            .build()
        )
    assert raised.value.code == -32602
    assert transport.start_failure == GatewayStartFailure(
        error=raised.value, init_outcome="refused"
    )
    assert transport.start_failure.error is raised.value
    assert len(host.retirement_threads) == 1
    # No shutdown request before acceptance: the stop only ends the input.
    assert "meta/shutdown" not in child.methods()
    assert child.saw_end_of_input.is_set()
    assert transport.child_exit == GatewayChildExit("exited", 0)
    assert transport.last_termination == TransportTermination(
        "exited", 0, None, "none", "not_negotiated"
    )
    assert off_loop(host)


@pytest.mark.asyncio
async def test_a_failed_settlement_keeps_the_native_failure_apart_from_the_exit(adopt):
    child, host = adopt(fails_closed)
    transport = AdoptedGatewayTransport(host.carrier())
    with pytest.raises(GatewayStartupCleanupError) as raised:
        await Meta.builder().gateway_transport(transport).build()
    failure = raised.value
    assert isinstance(failure.startup_error, StorageResolutionError)
    assert failure.startup_error.data["durable_effects"] == "possible"
    # The shutdown request found the gateway gone: the native shutdown answer
    # is its own outcome, apart from the observed exit.
    assert "gateway shutdown failed after bounded cleanup" in str(failure.cleanup_error)
    assert transport.start_failure == GatewayStartFailure(failure.startup_error, "failed")
    assert transport.child_exit == GatewayChildExit("exited", 0)
    record = TransportTermination("exited", 0, None, "none", "unanswered")
    assert failure.cleanup.last_termination == record
    # Nothing is left to stop: the exit was observed.
    await failure.cleanup.shutdown()
    assert len(host.retirement_threads) == 1


@pytest.mark.asyncio
async def test_a_cancelled_start_keeps_the_child_observation_and_its_uncertainty(adopt):
    init_seen = threading.Event()

    def script(child: FakeGatewayChild) -> int:
        init = child.read()
        assert init is not None
        init_seen.set()
        # Never answers: the caller gives up first.
        return serve(child)

    child, host = adopt(script)
    transport = AdoptedGatewayTransport(host.carrier())
    start, observed = start_build(transport)
    assert await asyncio.to_thread(init_seen.wait, _WAIT)
    start.cancel()
    with pytest.raises(asyncio.CancelledError):
        await start
    failure = transport.start_failure
    assert failure is not None
    assert type(failure.error) is asyncio.CancelledError
    assert failure.error is observed["error"]
    # The init request reached the gateway: its outcome stays unknown.
    assert failure.init_outcome == "unknown"
    assert len(host.retirement_threads) == 1
    assert transport.child_exit == GatewayChildExit("exited", 0)
    assert off_loop(host)


@pytest.mark.asyncio
async def test_a_second_cancellation_does_not_drop_the_child_observation(adopt):
    init_seen = threading.Event()

    def script(child: FakeGatewayChild) -> int:
        init = child.read()
        assert init is not None
        init_seen.set()
        # Never answers, and outlives its input until the host retires it.
        return serve(child, exit_code=5, until_retired=True)

    child, host = adopt(script, retire_ends_child=False)
    transport = AdoptedGatewayTransport(host.carrier())
    start, observed = start_build(transport)
    assert await asyncio.to_thread(init_seen.wait, _WAIT)
    start.cancel()
    # The failed start's stop is observing the child: cancel the caller again.
    await wait_until(lambda: host.wait_calls)
    start.cancel()
    with pytest.raises(asyncio.CancelledError):
        await start
    error = observed["error"]
    assert type(error) is asyncio.CancelledError
    cleanup = error.gateway_cleanup
    assert transport.child_exit is None
    # The same runtime-owned stop keeps observing: the host retires the child,
    # and the wait already in progress sees the exit. No stop starts again.
    calls_before = len(host.wait_calls)
    child.retire.set()
    await cleanup.shutdown()
    assert transport.child_exit == GatewayChildExit("exited", 5)
    assert cleanup.last_termination == TransportTermination(
        "exited", 5, None, "none", "not_negotiated"
    )
    assert len(host.wait_calls) == calls_before
    assert len(host.retirement_threads) == 1
    assert transport.start_failure is not None
    assert transport.start_failure.init_outcome == "unknown"
    assert off_loop(host)


@pytest.mark.asyncio
async def test_refusals_before_the_hand_off_leave_the_pipes_untouched(adopt, tmp_path):
    def script(child: FakeGatewayChild) -> int:
        return serve(child)

    child, host = adopt(script)

    def carrier(**changes: Any) -> AdoptedGatewayChild:
        fields = {
            "input": child.input,
            "output": child.output,
            "request_retirement": host.request_retirement,
            "wait_process": host.wait_process,
            **changes,
        }
        return AdoptedGatewayChild(**fields)

    text = io.StringIO()
    closed = open(tmp_path / "closed", "wb")
    closed.close()
    with pytest.raises(TypeError):
        carrier(input=text)
    with pytest.raises(TypeError):
        carrier(output=object())
    with pytest.raises(ValueError):
        carrier(input=closed)
    with pytest.raises(ValueError):
        carrier(output=child.input)
    with pytest.raises(TypeError):
        carrier(request_retirement=None)
    with pytest.raises(TypeError):
        carrier(wait_process="not callable")
    with pytest.raises(TypeError):
        AdoptedGatewayTransport(object())  # type: ignore[arg-type]
    for timeout in (0, -1, float("nan"), float("inf"), True):
        with pytest.raises(ValueError):
            AdoptedGatewayTransport(carrier(), timeout=timeout)

    # A stop before the hand-off makes a start refuse, typed.
    stopped = AdoptedGatewayTransport(carrier())
    await asyncio.to_thread(stopped.stop)
    assert stopped.last_termination == TransportTermination(
        "not_started", None, None, "none", "not_negotiated"
    )
    with pytest.raises(GatewayTransportConflictError):
        stopped.start()
    with pytest.raises(GatewayTransportConflictError):
        await Meta.builder().gateway_transport(stopped).build()

    # Two host transports for one gateway.
    fresh = AdoptedGatewayTransport(carrier())
    config = Meta.builder().gateway_transport(fresh)._config
    with pytest.raises(GatewayTransportConflictError):
        await MetaRuntime(config, AdoptedGatewayTransport(carrier())).connect()

    # A factory that returns no transport.
    with pytest.raises(TypeError):
        await Meta.builder().gateway_transport(lambda: object()).build()
    with pytest.raises(TypeError):
        Meta.builder().gateway_transport("not a transport")  # type: ignore[arg-type]

    assert child.input_untouched()
    assert host.retirement_threads == [] and host.wait_calls == []
    assert child.received == []


@pytest.mark.asyncio
async def test_a_host_transport_never_spawns_even_with_gateway_set(adopt):
    # Through the builder, with gateway() set as well.
    child, host = adopt(ready_gateway())
    transport = AdoptedGatewayTransport(host.carrier())
    runtime = await (
        Meta.builder()
        .gateway("/nonexistent/rpc_gateway")
        .gateway_transport(transport)
        .build()
    )
    assert runtime._transport is transport
    await runtime.shutdown()
    assert transport.child_exit == GatewayChildExit("exited", 0)

    # A transport given to the constructor, with a gateway binary configured,
    # is started and initialized in place of a spawned child.
    child, host = adopt(ready_gateway())
    transport = AdoptedGatewayTransport(host.carrier())
    config = Meta.builder().gateway("/nonexistent/rpc_gateway")._config
    runtime = MetaRuntime(config, transport)
    await runtime.connect()
    assert runtime.rust_http_base_url == "http://127.0.0.1:9"
    await runtime.shutdown()
    assert transport.child_exit == GatewayChildExit("exited", 0)
    # One child, one connect: it is never replaced, reconnected or replayed.
    with pytest.raises(GatewayTransportConflictError):
        await runtime.connect()

    # A factory serves each connect with the child the host started for it.
    hosts: list[FakeHost] = []

    def factory() -> AdoptedGatewayTransport:
        _, next_host = adopt(ready_gateway())
        hosts.append(next_host)
        return AdoptedGatewayTransport(next_host.carrier())

    runtime = await Meta.builder().gateway_transport(factory).build()
    await runtime.shutdown()
    await runtime.connect()
    await runtime.shutdown()
    assert len(hosts) == 2
    assert [len(each.retirement_threads) for each in hosts] == [1, 1]


@pytest.mark.asyncio
async def test_the_sdk_never_awaits_the_host_cleanup_and_the_reader_never_calls_it(adopt):
    # The host's cleanup joins the SDK's reader, then waits for a gate that
    # opens only after the SDK's stop returned. A reader that waited for that
    # cleanup, or a stop that awaited it, would never finish.
    child, host = adopt(ready_gateway())
    transport = AdoptedGatewayTransport(host.carrier())
    host.cleanup_joins_reader_of = transport
    host.cleanup_gate = threading.Event()
    runtime = await Meta.builder().gateway_transport(transport).build()
    await asyncio.wait_for(runtime.shutdown(), _WAIT)
    # The stop returned while the host's own cleanup is still open.
    assert not host.cleanup_done.is_set()
    assert transport.reader_completed
    host.cleanup_gate.set()
    assert await asyncio.to_thread(host.cleanup_done.wait, _WAIT)
    # The SDK only requested the retirement, from its stop's worker thread:
    # never from its reader, and never by running the host's cleanup.
    assert len(host.retirement_threads) == 1
    assert host.retirement_threads[0] != "meta-reader"
    assert host.cleanup_threads == [_HOST_LOOP]


@pytest.mark.asyncio
async def test_the_child_exit_and_the_reader_completion_stay_apart(adopt, monkeypatch):
    monkeypatch.setattr(adopted_module, "_PENDING_RETRY_SECONDS", 0.2)
    closed_stdout = threading.Event()

    # The child closes its stdout and keeps running until it is retired.
    def closes_stdout_first(child: FakeGatewayChild) -> int:
        init = accept_init(child, horizon_ms=100)
        settle(child, init, outcome="ready", http_base_url="http://127.0.0.1:9")
        child.stdout.close()
        closed_stdout.set()
        child.retire.wait(30)
        return 7

    child, host = adopt(closes_stdout_first, retire_ends_child=False)
    transport = AdoptedGatewayTransport(host.carrier())
    runtime = await Meta.builder().gateway_transport(transport).build()
    assert await asyncio.to_thread(closed_stdout.wait, _WAIT)
    await wait_until(lambda: transport.reader_completed)
    # The reader ended; the child has not exited.
    with pytest.raises(GatewayChildPendingError):
        await runtime.shutdown()
    assert transport.reader_completed
    assert transport.child_exit == GatewayChildExit("pending", None)
    assert transport.start_failure is None
    # Its shutdown answer is a native outcome of its own.
    assert transport.last_termination.native_shutdown == "unanswered"
    child.retire.set()
    with pytest.raises(RuntimeError, match="gateway shutdown failed"):
        await runtime.shutdown()
    assert transport.child_exit == GatewayChildExit("exited", 7)
    await runtime.shutdown()

    # The child exits while a descendant still holds its stdout.
    descendant_stdout: list[int] = []

    def exits_while_stdout_is_held(child: FakeGatewayChild) -> int:
        code = ready_gateway()(child)
        descendant_stdout.append(os.dup(child.stdout.fileno()))
        return code

    child, host = adopt(exits_while_stdout_is_held)
    transport = AdoptedGatewayTransport(host.carrier())
    runtime = await Meta.builder().gateway_transport(transport).build()
    await runtime.shutdown()
    assert transport.child_exit == GatewayChildExit("exited", 0)
    assert not transport.reader_completed
    os.close(descendant_stdout[0])
    await wait_until(lambda: transport.reader_completed)


@pytest.mark.asyncio
async def test_a_failed_retirement_request_is_typed_and_requested_again(adopt):
    child, host = adopt(ready_gateway())
    host.retirement_error = RuntimeError("host loop is busy")
    transport = AdoptedGatewayTransport(host.carrier())
    runtime = await Meta.builder().gateway_transport(transport).build()
    with pytest.raises(TransportError, match="request_retirement") as raised:
        await runtime.shutdown()
    assert isinstance(raised.value.__cause__, RuntimeError)
    # The child exited on its own end of input; the retirement is still owed.
    assert transport.child_exit == GatewayChildExit("exited", 0)
    await runtime.shutdown()
    assert len(host.retirement_threads) == 2
