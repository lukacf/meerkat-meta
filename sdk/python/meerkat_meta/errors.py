"""Typed error hierarchy for Meta SDK."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from ._transport import TransportTermination

# JSON-RPC error code returned by `meta/mob_events/{query,subscribe}` and
# the `/meta/mob_events/stream` SSE route when the caller's `after_seq`
# is past the current ledger frontier.
MOB_EVENTS_STALE_CURSOR_CODE: int = -32010
CAPABILITY_UNAVAILABLE_CODE: int = -32004
# Transient/recoverable identity-plane lease loss on a send/dispatch. Distinct
# from CAPABILITY_UNAVAILABLE_CODE (-32004) so a lease that merely needs
# re-acquisition is not mis-typed as a permanent capability gap.
LEASE_LOST_CODE: int = -32005
# A send named the registered row its input was authored from
# (``expected_spec_digest``) and the identity's registered row is no longer
# that row. Checked before delivery: nothing was submitted.
REGISTERED_SPEC_CHANGED_CODE: int = -32006
MEMORY_BACKEND_UNAVAILABLE_CODE: int = -32012
CONSOLE_TIMELINE_REPLAY_UNAVAILABLE_CODE: int = -32013
# Fail-closed storage refusal at gateway startup (meta/init): file-name
# twins the storage layout refuses to pick between, a store that failed to
# open where the silent fallback used to be, or a state-root creation
# failure. The message carries the remediation (the storage doctor, or the
# explicit ephemeral declaration).
STORAGE_RESOLUTION_CODE: int = -32014
# A saved delivery scope no longer matches the identity's binding: nothing
# was submitted and nothing was retargeted (`meta/dispatch` with
# `expected_scope`).
STALE_DELIVERY_SCOPE_CODE: int = -32007
# An ordinary request sent before meta/init settled: the stdio gateway
# refuses it at once instead of queueing it behind startup (a provider
# callback awaiting it during init would wait on itself).
INIT_IN_PROGRESS_CODE: int = -32018
# Mob composition provenance refusal at gateway startup (meta/init): the
# persistent mob storage's recorded composition cannot be proven to match the
# launch. The error data carries the refusal's ``kind`` and, where it has
# them, the diverged ``fields``.
COMPOSITION_PROVENANCE_CODE: int = -32019
# meerkat's typed refusal for a member whose runtime registration must be
# reloaded before it accepts work (`meta/force_cancel_member`). The error
# data carries ``kind = MEMBER_RELOAD_REQUIRED_KIND`` and names the member
# and the repair, ``required_action = "reload_member_registration"``.
MEMBER_RELOAD_REQUIRED_CODE: int = -32029
MEMBER_RELOAD_REQUIRED_KIND: str = "mob_member_reload_required"
# WorkGraph service not configured on the runtime.
WORKGRAPH_UNAVAILABLE_CODE: int = -32041
# WorkGraph CAS/revision conflict on a mutation (stale `expected_revision`).
WORKGRAPH_CONFLICT_CODE: int = -32042


class MetaError(Exception):
    """Base exception for all Meta SDK errors."""


class TransportError(MetaError):
    """Raised when the transport layer fails (subprocess died, connection refused, etc.)."""


class TransportReaderFailedError(TransportError):
    """The transport's reader stopped, so no response can arrive any more.

    Raised to every request still waiting when the gateway closes its stdout
    or the reader thread fails, and to every later request on the same
    gateway process. ``reason`` names what ended the reader. A request that
    was written before the reader stopped may still have been executed by
    the gateway.
    """

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"transport reader stopped: {reason}")


class GatewayChildEndedError(TransportReaderFailedError):
    """The transport's gateway child can no longer take requests.

    Raised to every request admitted after the child's lifetime ended, and
    never answered by starting a replacement: a request does not start an
    uninitialized gateway. Call ``start()`` (or ``shutdown()`` then
    ``connect()`` on a runtime) to replace it deliberately.

    ``reason`` is the first cause recorded for this child (its stdout closed,
    its reader failed, it was replaced, or the transport was stopped); a later
    stop does not relabel it. ``exit_code`` is the process exit status when
    the exit was observed and ``None`` when it was not: a failed channel does
    not prove that the process exited or was reaped. ``stop_requested`` says
    whether the caller has asked the transport to stop. As with any reader
    failure, a request written before the end may still have been executed.
    """

    def __init__(
        self,
        reason: str,
        *,
        exit_code: int | None = None,
        stop_requested: bool = False,
    ):
        super().__init__(reason)
        self.exit_code = exit_code
        self.stop_requested = stop_requested


class GatewayCleanup(Protocol):
    """The cleanup of a runtime's gateway transport, reached through that runtime.

    ``shutdown()`` joins its stop while it runs or retries a failed one
    (bounded per call), and ``last_termination`` reads what the last stop
    observed. A failed startup hands it over as
    ``GatewayStartupCleanupError.cleanup``, or, for a cancelled or interrupted
    startup, as the ``gateway_cleanup`` attribute of the exception that
    propagates; the stop may still be running then (see
    ``MetaRuntime.connect()``).
    """

    async def shutdown(self) -> None: ...

    @property
    def last_termination(self) -> TransportTermination | None: ...


class GatewayStartupCleanupError(TransportError):
    """A runtime's startup failed and the stop of the gateway transport it
    made then failed, so the runtime keeps that transport until a stop
    succeeds.

    The failure is not proof that the gateway process still runs: a process
    can exit while its transport's own cleanup fails, and
    ``cleanup.last_termination`` tells what the stop observed.
    ``startup_error`` (also the ``__cause__``) is the startup failure exactly
    as it is raised when the stop succeeds; ``cleanup_error`` is the stop's
    failure exactly as raised, an exception group included. ``cleanup``
    reaches the same runtime even when ``MetaBuilder.build()`` returned none;
    its ``shutdown()`` retries that stop. A startup failure whose stop
    succeeded raises its own exception unchanged. A cancelled or interrupted
    startup is never wrapped: while its stop has not succeeded, the exception
    that propagates carries the same cleanup as ``gateway_cleanup`` (see
    ``MetaRuntime.connect()``).
    """

    def __init__(
        self,
        startup_error: BaseException,
        cleanup_error: BaseException,
        cleanup: GatewayCleanup,
    ):
        self.startup_error = startup_error
        self.cleanup_error = cleanup_error
        self.cleanup = cleanup
        # A fixed message: formatting either failure can itself raise (a
        # failing ``__str__``) and would lose the cleanup this error carries.
        super().__init__(
            "gateway startup failed and the stop of its gateway transport failed "
            "(see startup_error and cleanup_error)"
        )


class InitOutcomeUnknownError(MetaError):
    """The SDK stopped waiting for ``meta/init`` before it settled.

    Raised when the init request was written but no settlement arrived: the
    acceptance was lost, the reader failed, the gateway exited, or the
    caller's init deadline ran out. The gateway may have changed native state
    (owner publication, identities, leases, continuity, schedules) before it
    stopped, so this is never reported as a refusal and never retried
    automatically. ``init_id`` correlates with the gateway's progress
    notifications; ``last_phase`` is the last ``meta/init_progress`` phase
    seen (``None`` when none arrived); ``reason`` names what ended the wait.
    """

    def __init__(self, init_id: str, last_phase: str | None, reason: str):
        self.init_id = init_id
        self.last_phase = last_phase
        self.reason = reason
        phase = last_phase or "none"
        super().__init__(
            f"meta/init outcome unknown (init_id={init_id}, last phase: {phase}): "
            f"{reason}; native state may have changed"
        )


class RpcError(MetaError):
    """Raised when a JSON-RPC call returns an error response."""

    def __init__(
        self,
        code: int,
        message: str,
        *,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.request_id = request_id
        self.method = method
        self.data = data


class MobEventsStaleError(RpcError):
    """Raised when the caller passes an ``after_seq`` past the current ledger frontier.

    The server's structured ``data`` payload carries ``after_cursor`` and
    ``latest_cursor``. Use ``latest_cursor`` to rewind and resume.
    """

    def __init__(
        self,
        message: str,
        *,
        after_cursor: int,
        latest_cursor: int,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(
            MOB_EVENTS_STALE_CURSOR_CODE,
            message,
            request_id=request_id,
            method=method,
            data=data,
        )
        self.after_cursor = after_cursor
        self.latest_cursor = latest_cursor

    @classmethod
    def from_rpc_error(cls, err: RpcError) -> MobEventsStaleError:
        """Reify a generic ``RpcError`` with code ``-32010`` into the typed form.

        Reads ``after_cursor`` / ``latest_cursor`` from ``err.data`` (the
        JSON-RPC ``error.data`` payload). Missing fields fall back to ``0``.
        """
        payload = err.data if isinstance(err.data, dict) else {}
        return cls(
            str(err),
            after_cursor=int(payload.get("after_cursor", 0)),
            latest_cursor=int(payload.get("latest_cursor", 0)),
            request_id=err.request_id,
            method=err.method,
            data=err.data,
        )


class CapabilityUnavailableError(RpcError):
    """Raised when a requested capability is not available on the runtime."""

    def __init__(
        self,
        message: str,
        *,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(
            CAPABILITY_UNAVAILABLE_CODE,
            message,
            request_id=request_id,
            method=method,
            data=data,
        )


class LeaseLostError(RpcError):
    """Raised when an identity's lease was lost mid send/dispatch.

    This is transient and recoverable: the identity simply needs to
    re-acquire its lease. Distinct from :class:`CapabilityUnavailableError`
    so callers do not treat a recoverable lease loss as a permanent
    capability gap.
    """

    def __init__(
        self,
        message: str,
        *,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(
            LEASE_LOST_CODE,
            message,
            request_id=request_id,
            method=method,
            data=data,
        )


class RegisteredSpecChangedError(RpcError):
    """Raised when a send's ``expected_spec_digest`` no longer names the
    identity's registered row.

    The gateway checks the registered row under the identity's lifecycle
    lock before delivery, so nothing was submitted. Read the current row with
    :meth:`MetaRuntime.registered_spec`, re-check the selection, and author
    the input again.
    """

    def __init__(
        self,
        message: str,
        *,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(
            REGISTERED_SPEC_CHANGED_CODE,
            message,
            request_id=request_id,
            method=method,
            data=data,
        )


class StaleScopeError(RpcError):
    """Raised when a scoped dispatch's saved delivery scope no longer matches.

    Nothing was submitted and the delivery was not retargeted to the
    identity's current binding. ``mismatch`` names the atom that moved:
    ``runtime_id``, ``generation``, ``lease_fencing_token``, ``member`` or
    ``member_binding`` (the member's session, incarnation or fence). An
    earlier attempt under the same scope is recovered with
    ``recover_delivery``, never by redispatching to a fresh scope.
    """

    def __init__(
        self,
        message: str,
        *,
        mismatch: str | None = None,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(
            STALE_DELIVERY_SCOPE_CODE,
            message,
            request_id=request_id,
            method=method,
            data=data,
        )
        self.mismatch = mismatch


class MemoryBackendUnavailableError(RpcError):
    """Raised when the configured memory backend cannot serve a request."""

    def __init__(
        self,
        message: str,
        *,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(
            MEMORY_BACKEND_UNAVAILABLE_CODE,
            message,
            request_id=request_id,
            method=method,
            data=data,
        )


class ConsoleTimelineReplayUnavailableError(RpcError):
    """Raised when a console timeline cursor cannot be replayed."""

    def __init__(
        self,
        message: str,
        *,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(
            CONSOLE_TIMELINE_REPLAY_UNAVAILABLE_CODE,
            message,
            request_id=request_id,
            method=method,
            data=data,
        )


class StorageResolutionError(RpcError):
    """Raised when the gateway refuses to start over a storage resolution gap.

    The refusals are deliberate (storage-unification fail-closed posture):
    file-name twins (e.g. ``sessions.sqlite3`` beside ``sessions.db``) the
    layout will not pick between, a session/runtime/blob/metadata/console
    store that failed to open where older gateways silently fell back to
    in-memory, or an uncreatable state root. The message names the
    remediation — run the storage doctor (``meta/storage/doctor``) for
    twins, fix the database file, or declare the ephemeral choice
    explicitly (e.g. ``runtime_store.memory()``).
    """

    def __init__(
        self,
        message: str,
        *,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(
            STORAGE_RESOLUTION_CODE,
            message,
            request_id=request_id,
            method=method,
            data=data,
        )


class InitInProgressError(RpcError):
    """An ordinary request reached the gateway before ``meta/init`` settled.

    The gateway serves ordinary requests only after init settles and refuses
    earlier ones at once. A startup provider callback (roster, topology,
    continuity, lease, session builder, customizer) must not issue ordinary
    runtime RPCs on the runtime that is starting.
    """

    def __init__(
        self,
        message: str,
        *,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(
            INIT_IN_PROGRESS_CODE,
            message,
            request_id=request_id,
            method=method,
            data=data,
        )


class CompositionProvenanceError(RpcError):
    """Raised when the gateway refuses to start over the mob's composition provenance.

    A persistent mob storage records the composition it was created for, and
    a launch whose composition cannot be proven to match it is refused before
    the mob actuates. The message names the remedy. ``kind`` names the
    refusal:

    - ``"divergent"``: the supplied definition differs from the one recorded
      for the storage (revert the change, or move the stored definition with
      ``MetaBuilder.declare_spec_update(expected_revision=...)``);
    - ``"candidate_divergent"``: a candidate launch supplied a definition that
      differs from the stored one it would boot. Only a gateway launched with
      ``runtime_options.mob_composition.authority = "candidate"`` raises it,
      which this builder never sends; the gateway option
      ``runtime_options.mob_composition.candidate_definition = "stored"``
      acknowledges the stored definition;
    - ``"created_by_rehearsal"``: the storage was created by a launch that did
      not speak for the durable composition;
    - ``"unreadable"``, ``"malformed"``, ``"unsupported_version"`` or
      ``"not_recorded"``: the provenance record beside the storage is
      unusable, or could not be written;
    - ``"missing"``: no provenance record beside non-empty storage. The
      gateway does not raise it today (an authoritative launch adopts and
      records the definition instead); library callers can see it;
    - ``"unproven_storage"``: non-empty storage with nothing declared about it.

    ``kind`` is ``None`` when the gateway sent no refusal data, and may be a
    kind newer than this list. ``fields`` lists the diverged definition fields
    as dotted paths (``profiles.lead.tools.deny``); it is empty unless the
    kind is ``divergent`` or ``candidate_divergent``. The full refusal stays
    available as ``data``.
    """

    def __init__(
        self,
        message: str,
        *,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(
            COMPOSITION_PROVENANCE_CODE,
            message,
            request_id=request_id,
            method=method,
            data=data,
        )
        refusal = data if isinstance(data, dict) else {}
        kind = refusal.get("kind")
        self.kind: str | None = kind if isinstance(kind, str) else None
        fields = refusal.get("fields")
        self.fields: list[str] = (
            [field for field in fields if isinstance(field, str)]
            if isinstance(fields, list)
            else []
        )


class MemberReloadRequiredError(RpcError):
    """Raised when a member needs a runtime reload before it accepts work.

    ``meta/force_cancel_member`` answers meerkat's typed refusal (code
    ``-32029``, data ``kind`` ``"mob_member_reload_required"``) for a member
    whose runtime registration cannot serve work. Both must match; any other
    ``-32029`` answer stays a plain :class:`RpcError`.
    ``member_id`` and ``reason`` come from the refusal's data, and
    ``required_action`` names meerkat's repair,
    ``"reload_member_registration"``. Each is ``None`` when the gateway sent
    no such field; the full refusal stays available as ``data``.
    """

    def __init__(
        self,
        message: str,
        *,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(
            MEMBER_RELOAD_REQUIRED_CODE,
            message,
            request_id=request_id,
            method=method,
            data=data,
        )
        refusal = data if isinstance(data, dict) else {}
        member_id = refusal.get("member_id")
        self.member_id: str | None = member_id if isinstance(member_id, str) else None
        reason = refusal.get("reason")
        self.reason: str | None = reason if isinstance(reason, str) else None
        required_action = refusal.get("required_action")
        self.required_action: str | None = (
            required_action if isinstance(required_action, str) else None
        )


class WorkGraphUnavailableError(RpcError):
    """Raised when the WorkGraph service is not configured on the runtime."""

    def __init__(
        self,
        message: str,
        *,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(
            WORKGRAPH_UNAVAILABLE_CODE,
            message,
            request_id=request_id,
            method=method,
            data=data,
        )


class WorkGraphConflictError(RpcError):
    """Raised on a WorkGraph CAS/revision conflict.

    The server's structured ``data`` payload carries ``detail`` (the
    upstream WorkGraph error message). Callers should refetch the item's or
    binding's current ``revision`` and retry.
    """

    def __init__(
        self,
        message: str,
        *,
        detail: str | None = None,
        request_id: str = "",
        method: str = "",
        data: Any | None = None,
    ):
        super().__init__(
            WORKGRAPH_CONFLICT_CODE,
            message,
            request_id=request_id,
            method=method,
            data=data,
        )
        self.detail = detail

    @classmethod
    def from_rpc_error(cls, err: RpcError) -> WorkGraphConflictError:
        """Reify a generic ``RpcError`` with code ``-32042`` into the typed form."""
        payload = err.data if isinstance(err.data, dict) else {}
        return cls(
            str(err),
            detail=payload.get("detail"),
            request_id=err.request_id,
            method=err.method,
            data=err.data,
        )


class ContractMismatchError(MetaError):
    """Raised when the SDK and runtime contract versions are incompatible."""


class NotConnectedError(MetaError):
    """Raised when an operation requires a connected runtime but none is available."""


class WaitEndedError(MetaError):
    """A server-side wait ended, typed, before what it waited for happened.

    ``outcome`` is the gateway's token: ``run_failed`` (a run on the identity
    failed or was cancelled during the wait), ``broken`` (the identity is
    parked Broken), ``retiring`` (retiring, retired or being deleted),
    ``identity_gone`` (no longer registered) or ``shutting_down`` (the
    gateway is shutting down). Tolerate future values.

    ``admission`` is the send/dispatch result when a ``*_and_wait`` call
    raised this after its delivery was admitted, else ``None``.
    """

    admission: Any = None

    def __init__(self, identity: str, outcome: str, detail: str = ""):
        message = f"wait on identity {identity!r} ended: {outcome}"
        super().__init__(f"{message} ({detail})" if detail else message)
        self.identity = identity
        self.outcome = outcome


class CompletionCursorUnavailableError(MetaError, RuntimeError):
    """The identity reports no completion cursor, so there is no completion
    to wait for.

    Raised by the cursor-driven waits (``wait_for_completion``,
    ``wait_for_output`` without ``turn=``, ``wait_for_output_containing``)
    when the handle names a live alias with no identity authority, or the
    gateway predates the completion contract. Wait on a turn ticket instead:
    send or dispatch with ``track_turn=True`` and pass the ticket as
    ``wait_for_output(turn=...)`` / ``wait_for_turn``.

    Subclasses ``RuntimeError``, which these waits raised before.
    """

    def __init__(self, identity: str, detail: str):
        super().__init__(f"identity {identity!r} reports no completion cursor; {detail}")
        self.identity = identity


class TurnFailedError(MetaError):
    """The turn a ticket names ran and failed (``meta/turn_result`` state
    ``failed``). ``reason`` carries the runtime's typed detail. ``admission``
    is the send/dispatch result when a ``*_and_wait`` call raised this, else
    ``None``."""

    admission: Any = None

    def __init__(self, identity: str, ticket: str, reason: str):
        super().__init__(f"turn {ticket} of identity {identity!r} failed: {reason}")
        self.identity = identity
        self.ticket = ticket
        self.reason = reason


class TurnUnknownError(MetaError):
    """The gateway knows no turn with this ticket for this identity: it was
    never admitted there, belongs to another identity, aged out, or the
    gateway restarted since. Never guessed in either direction. ``admission``
    is the send/dispatch result when a ``*_and_wait`` call raised this after
    its delivery was admitted, else ``None``: the delivery was admitted, so do
    not redispatch it because its ticket is no longer known."""

    admission: Any = None

    def __init__(self, identity: str, ticket: str):
        super().__init__(
            f"no turn {ticket} is known for identity {identity!r} (never admitted "
            "there, aged out, or the gateway restarted)"
        )
        self.identity = identity
        self.ticket = ticket


class TurnNotDeliveredError(MetaError):
    """A ``track_turn`` delivery was NOT delivered at all
    (``turn_unavailable.code == "not_delivered"``: the gateway has no session
    bridge, or the identity has no bound runtime). Nothing ran and there is no
    turn to wait for, so retrying is safe, unlike a failed or unknown turn.
    ``admission`` is the send/dispatch result that reported it."""

    admission: Any = None

    def __init__(self, identity: str, operation: str, code: str, reason: str):
        super().__init__(
            f"{operation} for identity {identity!r} was not delivered ({code}: {reason}); "
            "there is no turn to wait for"
        )
        self.identity = identity
        self.operation = operation
        self.code = code
        self.reason = reason


class TurnTrackingUnavailableError(MetaError):
    """A ``*_and_wait`` call made with ``require_attribution=True`` could not
    track its delivery's own turn, so it refused to wait identity-wide.

    The delivery was ADMITTED: the work was handed to the identity and will
    run, or already ran. Keep ``admission`` (the full send/dispatch result)
    and do not redispatch it. ``code`` and ``reason`` carry the typed
    ``turn_unavailable`` reason (``autonomous_host``, ``runtime_refused``,
    ...), or are ``None`` when the gateway predates turn tickets. Without
    ``require_attribution`` the call waits identity-wide instead and returns
    a non-attributed result."""

    def __init__(
        self,
        identity: str,
        operation: str,
        admission: Any,
        code: str | None,
        reason: str | None,
    ):
        detail = f"{code}: {reason}" if code is not None else "no turn ticket returned"
        super().__init__(
            f"{operation} for identity {identity!r} was admitted but its turn "
            f"cannot be tracked ({detail}); the admission result is retained, "
            "do not redispatch"
        )
        self.identity = identity
        self.operation = operation
        self.admission = admission
        self.code = code
        self.reason = reason


class PostAdmissionObservationError(MetaError):
    """Observing an ADMITTED delivery's turn failed for a transport or RPC
    reason (the gateway did not answer the wait), and retrying that exact
    observation ran out of the caller's deadline.

    The turn may still run, or may already have completed. ``admission`` is
    the full send/dispatch result and ``ticket`` names the turn (``None``
    only on the opt-in identity-wide fallback). Read the turn later with
    ``wait_for_turn(ticket)`` or ``turn_result(ticket)``; never redispatch the
    business work because of this error. ``__cause__`` is the last
    observation failure and ``attempts`` counts observation tries."""

    def __init__(
        self,
        identity: str,
        operation: str,
        admission: Any,
        ticket: str | None,
        *,
        attempts: int,
    ):
        turn = f"turn {ticket}" if ticket is not None else "its turn"
        super().__init__(
            f"{operation} for identity {identity!r} was admitted, but observing "
            f"{turn} failed after {attempts} attempt(s); the admission result is "
            "retained, do not redispatch"
        )
        self.identity = identity
        self.operation = operation
        self.admission = admission
        self.ticket = ticket
        self.attempts = attempts


class TurnWaitTimeoutError(MetaError, TimeoutError):
    """A ``*_and_wait`` call's wait did not settle by the caller's deadline.
    It is a ``TimeoutError``, so existing handlers keep working, and it
    carries ``admission`` (the send/dispatch result) and ``ticket`` (``None``
    when the delivery was untracked and the call waited identity-wide) so the
    turn can be read later instead of redispatched."""

    def __init__(
        self, identity: str, ticket: str | None, timeout: float, admission: Any,
    ):
        what = f"turn {ticket}" if ticket is not None else "the admitted delivery"
        super().__init__(
            f"{what} of identity {identity!r} did not complete within "
            f"{timeout}s; the admission result is retained, do not redispatch"
        )
        self.identity = identity
        self.ticket = ticket
        self.timeout = timeout
        self.admission = admission


class TurnOutputUnavailableError(MetaError):
    """The turn a ticket names completed, but no text of its own can be
    returned: ``status`` is ``empty`` (it committed no text),
    ``no_own_result`` (the runtime folded it into a run already in progress
    or deduplicated it onto an earlier admission) or ``unavailable`` (the
    gateway cannot report per-turn output). Raised only where a call must
    return text (``wait_for_output(turn=...)``); the turn is NOT failed, so do
    not retry it as if it were."""

    def __init__(self, identity: str, ticket: str, status: str):
        super().__init__(
            f"turn {ticket} of identity {identity!r} completed without output of "
            f"its own ({status})"
        )
        self.identity = identity
        self.ticket = ticket
        self.status = status


class TurnOutputTruncatedWarning(RuntimeWarning):
    """A ``*_and_wait`` call returned its turn's text cut at the gateway's
    bound (the text carries the runtime's truncation marker). Use
    ``wait_for_turn`` for the typed ``output_truncated`` flag."""


class TurnOutputUnavailableWarning(RuntimeWarning):
    """A ``*_and_wait`` call's turn completed without text of its own to
    return (``no_own_result`` or ``unavailable``), so it returned ``None``.
    This is not "the turn committed no text"; ``wait_for_turn`` returns the
    typed ``output_status``."""


class TurnTrackingUnavailableWarning(RuntimeWarning):
    """A ``*_and_wait`` call could not track its own turn and waited on the
    identity-wide completion cursor, which another delivery's completion can
    also satisfy, so the returned output is not attributed to the delivery
    (``AwaitedTurn.attributed`` is ``False``; ``untracked_code`` carries the
    ``turn_unavailable`` code named in the message). That is the default for
    every untracked delivery (``autonomous_host``, the default mode, among
    others) and for a gateway without tickets; ``require_attribution=True``
    raises :class:`TurnTrackingUnavailableError` instead."""
