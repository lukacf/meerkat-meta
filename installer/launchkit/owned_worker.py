"""Lifecycle of exact host-created worker sessions on macOS and Linux.

Only pass handles returned by spawn_owned_worker, never saved PID fields.
Process/group absence proves OS cleanup, not native cleanup acknowledgement or
the separate application sandbox's trusted child-reap receipt.
"""
from __future__ import annotations

import asyncio
from contextvars import ContextVar
import os
import signal
import threading
import time

from .deployment_deadlines import WORKER_STOP_TIMEOUT, PROCESS_FORCE_REAP_TIMEOUT

owned_job_deadline = ContextVar("owned_job_deadline", default=None)
EXIT_OBSERVATION_TIMEOUT = 1
_owner_loss_bound = None


def end_after_owner_loss(budget=None):
    """Bound this worker's own stop once its owner is gone (stdin EOF).

    No owner signals a worker it stopped waiting for: the worker stops itself
    on owner loss. Only a worker leading its own session, as its owner spawns
    it, arms this, once. If it still runs `budget` seconds later (by default
    WORKER_STOP_TIMEOUT, the bound its live owner waits), it applies that
    owner's escalation to its own group: SIGTERM, then SIGKILL.
    """
    global _owner_loss_bound
    if _owner_loss_bound is None and os.getsid(0) == os.getpid():
        budget = WORKER_STOP_TIMEOUT if budget is None else budget
        def end():
            try:
                os.killpg(os.getpid(), signal.SIGTERM)
                time.sleep(.2)
                os.killpg(os.getpid(), signal.SIGKILL)
            finally:
                os._exit(1)
        _owner_loss_bound = threading.Timer(budget, end)
        _owner_loss_bound.daemon = True
        _owner_loss_bound.start()
    return _owner_loss_bound


def begin_worker_cleanup(stop):
    cancelling = stop.is_set()
    stop.set()
    deadline = owned_job_deadline.get()
    if deadline is not None and not deadline.expired():
        # Keep work bounded without cancelling an already-admitted cleanup.
        deadline.reschedule(None)
    return cancelling


async def spawn_owned_worker(*command, preserve_admission_errors=False, **options):
    if type(preserve_admission_errors) is not bool:
        raise ValueError("Admission error preservation must be explicitly enabled or disabled.")
    options["start_new_session"] = True
    spawning = asyncio.create_task(asyncio.create_subprocess_exec(*command, **options))
    cancelled = False
    interruption = None
    while True:
        try:
            return await asyncio.shield(spawning), cancelled
        except asyncio.CancelledError as error:
            # Admission may have created a child. Repeated cancellation cannot
            # discard that child's handle before its owner can stop it.
            cancelled = True
            if spawning.cancelled():
                raise
            if interruption is None:
                interruption = error
        except BaseException as error:
            if preserve_admission_errors and interruption is not None:
                # A failed admission returns no process handle. Preserve both
                # facts without inventing process/group absence or cleanup.
                raise BaseExceptionGroup("Worker admission was cancelled and then failed.",
                                         [interruption, error]) from None
            raise


def _group_exists(process):
    if type(process.pid) is not int or process.pid <= 0 or process.pid == os.getpgrp():
        raise RuntimeError("An exact separately owned worker group is required.")
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        return False
    return True  # Permission errors are not proof of absence.


async def _parent_exit(process):
    while process.returncode is None:
        await asyncio.sleep(.01)


async def _signal_group(process, selected):
    try:
        os.killpg(process.pid, selected)
    except ProcessLookupError:
        pass
    except PermissionError:
        try:
            await asyncio.wait_for(_parent_exit(process), EXIT_OBSERVATION_TIMEOUT)
        except TimeoutError:
            pass
        if process.returncode is not None and not _group_exists(process):
            return
        raise


async def stop_owned_worker(process):
    if process.returncode is None:
        if process.stdin:
            try:
                process.stdin.close()
            except (OSError, ConnectionError):
                pass
        try:
            # Only the worker gets the initial signal. Its SDK gateway keeps
            # callback pipes available throughout the native shutdown horizon.
            process.terminate()
        except ProcessLookupError:
            pass
        except PermissionError:
            try:
                await asyncio.wait_for(_parent_exit(process), EXIT_OBSERVATION_TIMEOUT)
            except TimeoutError:
                pass
            if process.returncode is None:
                raise
        try:
            await asyncio.wait_for(_parent_exit(process), WORKER_STOP_TIMEOUT)
        except TimeoutError:
            pass
    if _group_exists(process):
        await _signal_group(process, signal.SIGTERM)
        await asyncio.sleep(.2)
        if _group_exists(process):
            await _signal_group(process, signal.SIGKILL)

    async def confirm_cleanup():
        await process.wait()
        while _group_exists(process):
            await asyncio.sleep(.01)
    await asyncio.wait_for(confirm_cleanup(), PROCESS_FORCE_REAP_TIMEOUT)
