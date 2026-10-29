"""Adopt a gateway child that the host started and keeps.

A host that supervises the gateway process itself (it spawns the process and
owns its lifetime, credentials and cleanup) hands the SDK that exact child:
its stdin and stdout pipes, a non-blocking retirement request, and a bounded
wait for the child's own exit. ``AdoptedGatewayTransport`` speaks the stock
stdio protocol over those pipes. It never spawns, signals, reconnects to or
replays a gateway, and it never awaits the host's own cleanup.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
from dataclasses import dataclass
import io
import logging
import math
import subprocess
import threading
import time
from typing import Any, BinaryIO, Callable, Literal

from ._transport import (
    _CHILD_EXIT_STDOUT_GRACE_SECONDS,
    _CURRENT_TRANSPORT,
    _NOT_STARTED,
    _PROCESS_KILL_GRACE_SECONDS,
    _PROCESS_TERMINATE_GRACE_SECONDS,
    PersistentTransport,
    _termination_of,
)
from .errors import (
    GatewayChildEndedError,
    GatewayChildPendingError,
    GatewayTransportConflictError,
    TransportError,
    TransportReaderFailedError,
)

_log = logging.getLogger("meerkat_meta")

# ``wait_process`` takes a finite timeout of 0 to 60 seconds.
_WAIT_PROCESS_MAX_SECONDS = 60.0
# A stop that finds the exit still pending after an earlier stop used its
# whole bound observes it again for this long: the stock stop's TERM and KILL
# graces together.
_PENDING_RETRY_SECONDS = _PROCESS_TERMINATE_GRACE_SECONDS + _PROCESS_KILL_GRACE_SECONDS
# A ``wait_process`` that reports "pending" before its timeout ran out is
# called again no sooner than this, so a stop never spins on the host.
_EARLY_PENDING_PAUSE_SECONDS = 0.05
# What ``wait_process`` raises while the exit is still pending. Python 3.10
# keeps the asyncio and concurrent.futures timeouts apart from TimeoutError,
# and a host that wraps ``Popen.wait`` raises ``TimeoutExpired``.
_EXIT_PENDING = (
    TimeoutError,
    asyncio.TimeoutError,
    concurrent.futures.TimeoutError,
    subprocess.TimeoutExpired,
)


@dataclass(frozen=True)
class AdoptedGatewayChild:
    """A gateway child process the host started and keeps, for the SDK to adopt.

    The host spawned the gateway (``rpc_gateway --persistent``) with piped
    stdin and stdout, and it keeps that process: its lifetime, credentials
    and cleanup. It hands the SDK exactly this child:

    - ``input``: a binary stream that writes the child's stdin, such as the
      host's ``Popen.stdin``, or ``os.fdopen(fd, "wb")`` over the write end of
      the child's stdin pipe.
    - ``output``: a binary stream that reads the child's stdout, such as
      ``Popen.stdout``, or ``os.fdopen(fd, "rb")`` over the read end of the
      child's stdout pipe.
    - ``request_retirement()``: schedules the host's retirement of the child
      on the host's own event loop and returns at once. It is thread-safe,
      repeat calls refer to the same retirement, and it never calls back into
      the SDK.
    - ``wait_process(timeout)``: waits at most ``timeout`` seconds (finite,
      from 0 to 60) for the direct child to exit and returns its exit code (a
      negative code names a signal, as ``subprocess.Popen.returncode`` does).
      It raises ``TimeoutError`` while the exit is still pending, which is
      not a failure. The SDK calls it only on its own worker threads, never
      on an event-loop thread. It never waits for the SDK's reader, the
      child's descendants or the host's own cleanup.

    Construction checks these shapes only: it reads, writes and closes
    nothing. The pipes are handed over when the transport starts (see
    ``AdoptedGatewayTransport``).
    """

    input: BinaryIO
    output: BinaryIO
    request_retirement: Callable[[], None]
    wait_process: Callable[[float], int]

    def __post_init__(self) -> None:
        _check_stream("input", self.input, ("write", "flush", "close"), "writable")
        _check_stream("output", self.output, ("readline", "close"), "readable")
        if self.input is self.output:
            raise ValueError(
                "input and output must be the child's two pipes, not one stream"
            )
        for name in ("request_retirement", "wait_process"):
            if not callable(getattr(self, name)):
                raise TypeError(f"{name} must be callable")


def _check_stream(
    name: str, stream: Any, methods: tuple[str, ...], mode: str
) -> None:
    """Refuse a stream the transport cannot use, without using it."""
    if isinstance(stream, io.TextIOBase):
        raise TypeError(f"{name} must be a binary stream, not a text stream")
    for method in methods:
        if not callable(getattr(stream, method, None)):
            raise TypeError(f"{name} must be a binary stream with {method}()")
    if getattr(stream, "closed", False) is True:
        raise ValueError(f"{name} is already closed")
    check = getattr(stream, mode, None)
    if callable(check) and check() is False:
        raise ValueError(f"{name} is not {mode}")


@dataclass(frozen=True)
class GatewayStartFailure:
    """The native terminal failure of a start that failed after the hand-off.

    It stays apart from the direct child's exit
    (``AdoptedGatewayTransport.child_exit``) and the SDK reader's completion
    (``AdoptedGatewayTransport.reader_completed``): a failed init proves
    neither, and neither proves how init ended.
    """

    #: The startup error exactly as raised to the caller: the typed
    #: ``RpcError`` of a refused or failed init, ``InitOutcomeUnknownError``,
    #: a transport error, or the ``CancelledError`` of a cancelled caller.
    error: BaseException
    #: What is known of ``meta/init``: ``"not_sent"`` (the start failed
    #: before the request was handed to the transport), ``"refused"`` (the
    #: gateway answered it with an error), ``"failed"`` (it was accepted and
    #: then settled failed; the error's ``data["durable_effects"]`` says what
    #: may have changed), ``"unknown"`` (the request was handed over and no
    #: outcome was learned, for example a lost answer, a deadline or a
    #: cancelled caller: native state may have changed), or ``"ready"`` (it
    #: settled ready and a later step of the start failed).
    init_outcome: Literal["not_sent", "refused", "failed", "unknown", "ready"]


@dataclass(frozen=True)
class GatewayChildExit:
    """The adopted direct child's own exit, as ``wait_process`` reported it."""

    #: ``"exited"`` once ``wait_process`` returned an exit code; ``"pending"``
    #: when a stop's bounded wait ended first, or ``wait_process`` failed.
    state: Literal["exited", "pending"]
    #: The exit code ``wait_process`` returned; ``None`` while pending.
    exit_code: int | None


class _AdoptedProcess:
    """The process view the shared transport code drives for an adopted child.

    ``poll()`` and ``returncode`` report only an exit a stop observed through
    ``wait_process``: they never call the host and never block. It has no
    ``wait()``, ``terminate()`` or ``kill()``: the transport never waits on,
    signals or kills a process it does not own.
    """

    def __init__(self, stdin: Any, stdout: Any) -> None:
        self.stdin = stdin
        self.stdout = stdout
        self._exit_code: int | None = None

    def poll(self) -> int | None:
        return self._exit_code

    @property
    def returncode(self) -> int | None:
        return self._exit_code


class AdoptedGatewayTransport(PersistentTransport):
    """The stock stdio gateway transport over a child the host started and keeps.

    It speaks the stock protocol over the adopted pipes: the ``meta/init``
    exchange with its accepted-then-settled readiness, host callbacks (a
    handler set before ``start()`` answers the first one), sync and async
    RPC with their timeouts, correlated init watches, and the
    ``meta/shutdown`` handshake. It never spawns, signals, reconnects to or
    replays a gateway.

    Ownership:

    - Construction validates and keeps the ``AdoptedGatewayChild``. ``start()``
      hands the child over: from then on the transport writes ``input`` and
      reads ``output``. A refusal before that (a wrong carrier, a transport
      already started or stopped, a stop that came first) leaves both pipes
      untouched with the host.
    - Call ``start()`` on the event loop that dispatches host callbacks, as
      for the stock transport.
    - ``stop()`` is synchronous and blocking. Run it on a worker thread, for
      example ``await asyncio.to_thread(transport.stop)``; on an event-loop
      thread it raises at once and does nothing. It makes the stock protocol
      shutdown (the one ``meta/shutdown`` request when the gateway negotiated
      it, then the end of ``input``), calls ``request_retirement()`` once per
      child however often it runs, and observes the direct child's exit
      through ``wait_process`` with bounded retries. It observes the child
      before it joins the SDK's reader, so a descendant that holds the
      child's stdout cannot use up that bound; once the child exited, the
      reader gets a short grace to reach the end of ``output``. It never
      awaits, models or reports the host's own cleanup.
    - ``MetaRuntime`` stops a start that fails after the hand-off the same
      way, on a runtime-owned worker task the caller's cancellation does not
      end, after it records the failure as ``start_failure``.
    - The SDK's reader closes ``output`` when it ends; ``stop()`` closes
      ``input``. Neither the reader nor its end calls into the host.

    Three outcomes stay apart: the native terminal failure
    (``start_failure``, and ``last_termination.native_shutdown`` for the
    answer to the shutdown request), the direct child's exit (``child_exit``,
    also recorded in ``last_termination``), and the SDK reader's completion
    (``reader_completed``).
    """

    def __init__(self, child: AdoptedGatewayChild, *, timeout: float = 60.0):
        if not isinstance(child, AdoptedGatewayChild):
            raise TypeError(
                "AdoptedGatewayTransport takes an AdoptedGatewayChild, got "
                f"{type(child).__name__}"
            )
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
            or timeout > threading.TIMEOUT_MAX
        ):
            raise ValueError("timeout must be a positive finite number of seconds")
        super().__init__("", timeout=timeout)
        self._child = child
        # Set once, at the hand-off: the transport never takes another child.
        self._handed_over = False
        # Serializes stops: a later stop finds what an earlier one did.
        self._stop_lock = threading.Lock()
        self._retirement_lock = threading.Lock()
        self._retirement_requested = False
        self._start_failure: GatewayStartFailure | None = None
        self._child_exit: GatewayChildExit | None = None
        # When the first stop began: its bound runs from then.
        self._first_stop_at: float | None = None
        self._reader_done = threading.Event()

    @property
    def child(self) -> AdoptedGatewayChild:
        """The adopted child this transport was given."""
        return self._child

    @property
    def start_failure(self) -> GatewayStartFailure | None:
        """The native terminal failure of a start that failed after the hand-off.

        ``None`` while no start failed. Set by ``MetaRuntime`` before it stops
        the transport; the first failure is kept.
        """
        return self._start_failure

    @property
    def child_exit(self) -> GatewayChildExit | None:
        """What the last stop observed of the direct child's exit.

        ``None`` before a stop observed it; ``state == "pending"`` while the
        exit was not observed within a stop's bound. It is the child's own
        exit only: it says nothing about its descendants or the host's own
        cleanup.
        """
        return self._child_exit

    @property
    def reader_completed(self) -> bool:
        """Whether the SDK's reader of ``output`` has ended.

        It ends at the end of the child's stdout or on a read failure, and
        closes ``output``. ``False`` before the hand-off and while a
        descendant of the child still holds that stdout open.
        """
        return self._reader_done.is_set()

    def _record_start_failure(self, failure: GatewayStartFailure) -> None:
        with self._pending_lock:
            if self._start_failure is None:
                self._start_failure = failure

    def _claim_start(
        self, expected_stop_epoch: int | None = None
    ) -> tuple[int, threading.Event] | None:
        """Take the one hand-off, or ``None`` when the adopted child is in use."""
        while True:
            with self._lifecycle_lock:
                if expected_stop_epoch is not None and (
                    self._closed or self._stop_epoch != expected_stop_epoch
                ):
                    raise GatewayChildEndedError(
                        "the transport was stopped", stop_requested=True
                    )
                pending = self._start_claim
                if pending is None:
                    if self._handed_over:
                        current = self._process
                        if current is not None and not self._closed:
                            lifetime = self._lifetime_of(current)
                            if lifetime is None or lifetime.error is None:
                                return None
                        raise GatewayTransportConflictError(
                            "an adopted gateway transport takes its child once: it "
                            "never starts, reconnects to or replays another gateway "
                            "child"
                        )
                    if self._closed:
                        raise GatewayTransportConflictError(
                            "this adopted gateway transport was stopped before its "
                            "child was handed over; the host keeps the child and its "
                            "pipes"
                        )
                    done = threading.Event()
                    self._start_claim = done
                    return self._stop_epoch, done
            pending.wait()

    def _start_claimed(self, stop_epoch: int) -> None:
        # Capture event loop for async callback dispatch.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        process = _AdoptedProcess(self._child.input, self._child.output)
        with self._lifecycle_lock:
            if self._closed or self._stop_epoch != stop_epoch:
                # stop() came first: nothing was handed over, and the host
                # keeps the child and its pipes untouched.
                raise GatewayChildEndedError(
                    "the transport was stopped before its adopted gateway child "
                    "was handed over",
                    stop_requested=True,
                )
            # The hand-off: from here the transport writes the child's stdin
            # and reads its stdout. The first and only child, so nothing is
            # retired and no waiter is failed.
            self._handed_over = True
            self._publish_child(
                process,
                None,
                loop,
                TransportReaderFailedError("the gateway process was replaced"),
            )
        try:
            reader = threading.Thread(
                target=self._reader_loop,
                args=(process,),
                daemon=True,
                name="meta-reader",
            )
            self._reader_thread = reader
            reader.start()
        except BaseException:
            # The child was handed over: its host still retires it.
            self._request_retirement_once()
            raise

    def _reader_loop(self, process: Any = _CURRENT_TRANSPORT) -> None:
        if process is _CURRENT_TRANSPORT:
            process = self._process
        try:
            super()._reader_loop(process)
        finally:
            # The reader is the only user of the child's stdout: it releases
            # it and reports its own completion. It never calls the host.
            stdout = getattr(process, "stdout", None)
            if stdout is not None:
                try:
                    stdout.close()
                except (OSError, ValueError):
                    pass
            self._reader_done.set()

    def stop(self) -> None:
        """End the protocol with the adopted child and hand the child back.

        Synchronous and blocking: run it on a worker thread. On an event-loop
        thread it raises ``TransportError`` at once and does nothing, so it
        never holds the loop that dispatches host callbacks or runs the
        host's retirement.

        It closes ordinary admission, makes the one ``meta/shutdown`` request
        when the gateway negotiated it, closes ``input``, calls
        ``request_retirement()`` (once per child), and observes the direct
        child's exit through ``wait_process``. The first stop observes it for
        the gateway's shutdown horizon plus the stock stop's TERM and KILL
        graces, from when that stop began; a later stop observes it for those
        graces again. ``TimeoutError`` from ``wait_process`` means pending, and
        it is called again until the bound ends.

        Raises ``GatewayChildPendingError`` while the exit was not observed:
        the transport keeps the child's observation, and a later stop
        observes it again without requesting the retirement twice. Raises
        ``TransportError`` when ``request_retirement()`` failed: the
        transport keeps the child, and a later stop requests it again. Raises
        ``RuntimeError`` when the shutdown request failed, as the stock
        transport does. A stop after the exit was observed and the retirement
        requested returns at once.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise TransportError(
                "AdoptedGatewayTransport.stop() blocks while it observes the child "
                "through wait_process: run it on a worker thread (for example with "
                "asyncio.to_thread), never on an event-loop thread"
            )
        with self._stop_lock:
            self._stop_adopted_child()

    def _stop_adopted_child(self) -> None:
        # Close ordinary admission first; the shutdown request and callback
        # replies use the internal path and stay open.
        with self._lifecycle_lock:
            self._closed = True
            self._stop_epoch += 1
            process = self._process
            stopping = self._lifetime_of(process)
            if stopping is not None:
                stopping.end("the transport was stopped", stop_requested=True)
        if process is None:
            # Nothing was handed over, or an earlier stop observed the exit:
            # that stop's record stays.
            if self._last_termination is None:
                self._last_termination = _NOT_STARTED
            return
        with self._pending_lock:
            supports_shutdown_handshake = self._supports_shutdown_handshake
            shutdown_horizon = self._shutdown_horizon_seconds
        if self._first_stop_at is None:
            self._first_stop_at = time.monotonic()
        native_shutdown, shutdown_error = self._recorded_shutdown_answer(
            supports_shutdown_handshake
        )
        retirement_error: Exception | None = None
        observation_error: Exception | None = None
        exit_code: int | None = None
        try:
            if native_shutdown is None:
                native_shutdown = "unanswered"
                native_shutdown, shutdown_error = self._ask_gateway_shutdown(
                    process, shutdown_horizon
                )
            self._close_gateway_input(process)
            retirement_error = self._request_retirement_once()
            deadline = max(
                self._first_stop_at + shutdown_horizon + _PENDING_RETRY_SECONDS,
                time.monotonic() + _PENDING_RETRY_SECONDS,
            )
            exit_code = process.poll()
            if exit_code is None:
                try:
                    exit_code = self._observe_child_exit(process, deadline)
                except Exception as exc:
                    observation_error = exc
            if exit_code is not None:
                self._join_reader(_CHILD_EXIT_STDOUT_GRACE_SECONDS)
        finally:
            exited = exit_code is not None
            child_exit = GatewayChildExit(
                "exited" if exited else "pending", exit_code
            )
            with self._pending_lock:
                if self._process is process:
                    self._child_exit = child_exit
                    self._last_termination = _termination_of(
                        exit_code, exited, "none", native_shutdown
                    )
            if exited:
                # The child can answer nothing more.
                self._fail_pending_after_reader_exit(
                    "the gateway process stopped", process=process
                )
                # Released only once its retirement was requested: until
                # then a later stop requests it again.
                with self._pending_lock:
                    if self._process is process and self._retirement_requested:
                        self._process = None
        if not exited:
            raise GatewayChildPendingError(child_exit) from observation_error
        if retirement_error is not None:
            raise TransportError(
                "persistent transport: the host's request_retirement() failed; a "
                "later stop() requests it again"
            ) from retirement_error
        if shutdown_error is not None:
            raise RuntimeError(
                "persistent transport: gateway shutdown failed after bounded cleanup"
            ) from shutdown_error

    def _request_retirement_once(self) -> Exception | None:
        """Ask the host to retire the child, once per child. Never blocks."""
        with self._retirement_lock:
            if self._retirement_requested:
                return None
            try:
                self._child.request_retirement()
            except Exception as exc:
                # Not requested: a later stop asks again.
                _log.warning("transport: request_retirement() failed: %s", exc)
                return exc
            self._retirement_requested = True
            return None

    def _observe_child_exit(self, process: _AdoptedProcess, deadline: float) -> int | None:
        """The direct child's exit code through ``wait_process``, or ``None``.

        Runs on this worker thread, until ``deadline``: each call waits at
        most 60 seconds, and ``TimeoutError`` (pending) is retried until the
        deadline passes.
        """
        wait_process = self._child.wait_process
        while True:
            remaining = max(0.0, deadline - time.monotonic())
            timeout = min(_WAIT_PROCESS_MAX_SECONDS, remaining)
            called = time.monotonic()
            try:
                exit_code = wait_process(timeout)
            except _EXIT_PENDING:
                now = time.monotonic()
                if now >= deadline:
                    return None
                if now - called < timeout:
                    time.sleep(min(_EARLY_PENDING_PAUSE_SECONDS, deadline - now))
                continue
            if isinstance(exit_code, bool) or not isinstance(exit_code, int):
                raise TypeError(
                    "wait_process must return the child's exit code as an int, got "
                    f"{type(exit_code).__name__}"
                )
            process._exit_code = exit_code
            lifetime = self._lifetime_of(process)
            if lifetime is not None:
                lifetime.observe_exit()
            return exit_code

    def _join_reader(self, timeout: float) -> None:
        reader = self._reader_thread
        if reader is not None and reader is not threading.current_thread():
            reader.join(timeout)

    def __del__(self) -> None:
        # A destructor never blocks or waits: a child that no stop handed
        # back is handed back to its host, and nothing else happens here.
        try:
            if getattr(self, "_handed_over", False):
                self._request_retirement_once()
        except Exception:
            pass
