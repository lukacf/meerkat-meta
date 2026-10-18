"""Retain one native enrollment for the application's existing Console login.

The trusted application supplies its shared private client and exact login.
This adapter owns only its blocking calls and workers. It cannot authenticate
a human, select application policy, replace a login or close the host client.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import inspect

from meerkat_toolkit.host_control import (
    ConsoleLoginEnrollment, ConsoleLoginRetirement, ConsoleLoginSelection,
    PrivateHostClient,
)

if __package__:
    from .console_auth import ConsoleConnectLogin
else:
    from console_auth import ConsoleConnectLogin


class NativeConsoleLoginError(RuntimeError):
    """A bounded refusal without private native diagnostics."""
    def __init__(self, code):
        self.code = code
        super().__init__("The protected Console login is unavailable.")


class NativeConsoleLogin:
    """One original enrollment, retained observation and joined retirement.

    Public methods belong to the ConsoleFront event loop. Ordinary blocking
    waits and retirement have separate owned workers: an occupied result wait
    must never consume retirement's executor capacity.
    """
    def __init__(self, client, login, *, issuer, is_current=None):
        if (not isinstance(client, PrivateHostClient)
                or not isinstance(login, ConsoleConnectLogin)
                or not callable(login.is_current)
                or inspect.iscoroutinefunction(login.is_current)
                or is_current is not None and (
                    not callable(is_current) or inspect.iscoroutinefunction(is_current))):
            raise ValueError("Native Console login requires trusted application inputs.")
        self._selection = ConsoleLoginSelection(
            issuer, login.principal, login.login_scope_id, login.expires_at_ms)
        self._client, self._login = client, login
        self._application_current = is_current
        self._revoked = False
        self._begin_task = self._result_task = self._retire_task = None
        self._enrollment = None
        self._calls = self._retirement_calls = None

    @property
    def issuer(self):
        return self._selection.issuer

    @property
    def subject(self):
        return self._selection.subject

    @property
    def login_scope_id(self):
        return self._selection.login_scope_id

    @property
    def expires_at_ms(self):
        return self._selection.expires_at_ms

    @property
    def origin(self):
        return self._login.origin

    def is_current(self):
        """Require the captured login and any selected application owner."""
        try:
            return (not self._revoked and self._login.is_current() is True
                    and (self._application_current is None
                         or self._application_current() is True))
        except Exception:
            return False

    def _require_current(self):
        if not self.is_current():
            raise NativeConsoleLoginError("invalid_binding")

    @staticmethod
    def _retain(task):
        # The task remains the original owner's result, including its failure.
        task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
        return task

    def start(self):
        """Claim once in the trusted login owner, before route token minting."""
        if self._begin_task is not None or self._retire_task is not None:
            raise NativeConsoleLoginError("invalid_binding")
        self._require_current()
        asyncio.get_running_loop()
        self._calls = ThreadPoolExecutor(max_workers=1, thread_name_prefix="forge-console-login")
        self._retirement_calls = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="forge-console-retire")
        self._begin_task = self._retain(asyncio.create_task(self._begin()))

    async def _begin(self):
        # start() already claimed this invocation. Retirement must collect its
        # original handle even if disclosure was revoked before this returns.
        try:
            enrollment = await asyncio.get_running_loop().run_in_executor(
                self._calls, self._client.begin_console_login, self._selection)
            if not isinstance(enrollment, ConsoleLoginEnrollment):
                raise ValueError("Invalid retained Console enrollment.")
            self._enrollment = enrollment
            self._result_task = self._retain(asyncio.create_task(self._result()))
        except BaseException:
            raise NativeConsoleLoginError("outcome_unknown") from None

    async def _result(self):
        try:
            await asyncio.get_running_loop().run_in_executor(
                self._calls, self._enrollment.result)
        except BaseException:
            # SDK result() caches its original exception. Never start another
            # enrollment or attempt to recover a discarded late observation.
            raise NativeConsoleLoginError("outcome_unknown") from None

    async def ready(self):
        """Trusted caller only; success returns the original same-pipe handle."""
        self._require_current()
        if self._begin_task is None:
            raise NativeConsoleLoginError("unavailable")
        await asyncio.shield(self._begin_task)
        self._require_current()
        await asyncio.shield(self._result_task)
        self._require_current()
        return self._enrollment

    def revoke(self):
        """Fence disclosure synchronously before any asynchronous cleanup."""
        self._revoked = True

    async def retire(self, reason):
        self.revoke()
        if self._retire_task is None:
            self._retire_task = self._retain(asyncio.create_task(self._retire()))
        return await asyncio.shield(self._retire_task)

    async def _retire(self):
        try:
            if self._begin_task is None:
                return None
            begun = await asyncio.gather(asyncio.shield(self._begin_task), return_exceptions=True)
            if any(isinstance(result, BaseException) for result in begun):
                raise NativeConsoleLoginError("cleanup_unconfirmed")
            retirement = None
            try:
                # Do not join result first: it may still occupy the ordinary
                # worker. The SDK retirement lock and this worker are separate.
                retirement = await asyncio.get_running_loop().run_in_executor(
                    self._retirement_calls, self._enrollment.retire)
            except BaseException:
                pass
            # A result timeout stays unknown, but does not erase a separately
            # confirmed local fence/drain. Join the original call either way.
            await asyncio.gather(asyncio.shield(self._result_task), return_exceptions=True)
            if (not isinstance(retirement, ConsoleLoginRetirement)
                    or retirement.fenced is not True
                    or retirement.reservations_drained is not True
                    or retirement.uncertain is not False):
                raise NativeConsoleLoginError("cleanup_unconfirmed")
            return retirement
        finally:
            # Entered SDK calls are joined above before either worker is joined.
            # These pools belong only to this login; the client remains shared.
            for worker in (self._calls, self._retirement_calls):
                if worker is not None:
                    worker.shutdown(wait=True)

    async def close(self):
        return await self.retire("application_stopping")
