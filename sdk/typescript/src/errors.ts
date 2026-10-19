/**
 * Typed error hierarchy for the Meta SDK.
 *
 * @example
 * ```ts
 * import { RpcError, MetaError } from "@rkat/meta-sdk";
 *
 * try {
 *   await handle.status();
 * } catch (err) {
 *   if (err instanceof RpcError) {
 *     console.error(`RPC ${err.method} failed: code=${err.code}`);
 *   }
 * }
 * ```
 */

import type { TransportTermination } from "./transport.js";

// -- Base error -----------------------------------------------------------

/** Base exception for all Meta SDK errors. */
export class MetaError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "MetaError";
  }
}

// -- Transport errors -----------------------------------------------------

/** Raised when the transport layer fails (subprocess died, connection refused, etc.). */
export class TransportError extends MetaError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "TransportError";
  }
}

/**
 * The SDK stopped waiting for `meta/init` before it settled.
 *
 * Thrown when the init request was written but no settlement arrived: the
 * acceptance was lost, the reader failed, the gateway exited, or the
 * caller's init deadline ran out. The gateway may have changed native state
 * (owner publication, identities, leases, continuity, schedules) before it
 * stopped, so this is never reported as a refusal and never retried
 * automatically. `initId` correlates with the gateway's progress
 * notifications; `lastPhase` is the last `meta/init_progress` phase seen
 * (`null` when none arrived); `reason` names what ended the wait.
 */
export class InitOutcomeUnknownError extends MetaError {
  constructor(
    readonly initId: string,
    readonly lastPhase: string | null,
    readonly reason: string,
  ) {
    super(
      `meta/init outcome unknown (init_id=${initId}, last phase: ${lastPhase ?? "none"}): ` +
        `${reason}; native state may have changed`,
    );
    this.name = "InitOutcomeUnknownError";
  }
}

/**
 * The transport's reader stopped, so no response can arrive any more.
 *
 * Rejects every request still waiting when the gateway closes its stdout,
 * and every later request on the same gateway process. `reason` names what
 * ended the reader. A request written before the reader stopped may still
 * have been executed by the gateway.
 */
export class TransportReaderFailedError extends TransportError {
  constructor(readonly reason: string) {
    super(`transport reader stopped: ${reason}`);
    this.name = "TransportReaderFailedError";
  }
}

/**
 * A host-composed gateway transport refused to respawn its gateway process.
 *
 * A transport built by a gateway transport factory, or one with its own
 * `startAsync()`, finishes a trusted host startup that a plain respawn would
 * skip. Once its process is gone, requests fail with this error until the
 * runtime reconnects, which builds and starts the transport again.
 */
export class GatewayRestartRefusedError extends TransportError {
  constructor(readonly reason: string) {
    super(`host-composed gateway transport will not respawn: ${reason}; reconnect the runtime`);
    this.name = "GatewayRestartRefusedError";
  }
}

/**
 * The cleanup of a runtime's gateway transport, reached through that same
 * runtime: `shutdown()` retries its retained stop (bounded per call) and
 * `lastTermination()` reads what the last stop observed.
 */
export interface GatewayCleanup {
  shutdown(): Promise<void>;
  lastTermination(): TransportTermination | null;
}

/**
 * A runtime's startup failed and the stop of the gateway transport it made
 * then rejected, so the runtime keeps that transport until a stop fulfils.
 * The rejection is not proof that the gateway process still runs: a process
 * can exit while its transport's own cleanup fails, and
 * `cleanup.lastTermination()` tells what the stop observed.
 *
 * `startupError` (also the `cause`) is the startup failure exactly as it is
 * thrown when the stop fulfils; `cleanupError` is the stop's rejection,
 * exactly as thrown. `cleanup` reaches the same runtime even when
 * `MetaBuilder.build()` returned none; its `shutdown()` retries that stop.
 * A startup failure whose stop fulfilled throws its original error unchanged.
 */
export class GatewayStartupCleanupError extends TransportError {
  constructor(
    readonly startupError: unknown,
    readonly cleanupError: unknown,
    readonly cleanup: GatewayCleanup,
  ) {
    // A fixed message: formatting either failure can itself throw (an object
    // without a prototype, a throwing `message` getter) and would lose the
    // cleanup this error carries.
    super(
      "gateway startup failed and the stop of its gateway transport was rejected " +
        "(see startupError and cleanupError)",
      { cause: startupError },
    );
    this.name = "GatewayStartupCleanupError";
  }
}

// -- RPC errors -----------------------------------------------------------

/** Raised when a JSON-RPC call returns an error response. */
export class RpcError extends MetaError {
  constructor(
    readonly code: number,
    message: string,
    readonly requestId: string,
    readonly method: string,
    /** Optional structured payload from the JSON-RPC `error.data` field. */
    readonly data?: unknown,
  ) {
    super(message);
    this.name = "RpcError";
  }
}

/**
 * JSON-RPC error code returned when the caller's `after_seq` is past the
 * current ledger frontier. Returned by `meta/mob_events/{query,subscribe}`
 * and the `/meta/mob_events/stream` SSE route (HTTP 410 Gone).
 */
export const MOB_EVENTS_STALE_CURSOR_CODE = -32010 as const;
export const CAPABILITY_UNAVAILABLE_CODE = -32004 as const;
/**
 * Transient/recoverable identity-plane lease loss on a send/dispatch. Distinct
 * from {@link CAPABILITY_UNAVAILABLE_CODE} (-32004) so a lease that merely needs
 * re-acquisition is not mis-typed as a permanent capability gap.
 */
export const LEASE_LOST_CODE = -32005 as const;
/**
 * A send named the registered row its input was authored from
 * (`expectedSpecDigest`) and the identity's registered row is no longer that
 * row. Checked before delivery: nothing was submitted.
 */
export const REGISTERED_SPEC_CHANGED_CODE = -32006 as const;
export const MEMORY_BACKEND_UNAVAILABLE_CODE = -32012 as const;
export const CONSOLE_TIMELINE_REPLAY_UNAVAILABLE_CODE = -32013 as const;
/**
 * Fail-closed storage refusal at gateway startup (`meta/init`): file-name
 * twins the storage layout refuses to pick between, a store that failed to
 * open where the silent fallback used to be, or a state-root creation
 * failure. The message carries the remediation (the storage doctor, or the
 * explicit ephemeral declaration).
 */
export const STORAGE_RESOLUTION_CODE = -32014 as const;
/**
 * An ordinary request sent before `meta/init` settled: the stdio gateway
 * refuses it at once instead of queueing it behind startup (a provider
 * callback awaiting it during init would wait on itself).
 */
export const INIT_IN_PROGRESS_CODE = -32018 as const;
/**
 * Mob composition provenance refusal at gateway startup (`meta/init`): the
 * persistent mob storage's recorded composition cannot be proven to match
 * the launch. The error data carries the refusal's `kind` and, where it has
 * them, the diverged `fields`.
 */
export const COMPOSITION_PROVENANCE_CODE = -32019 as const;
/**
 * meerkat's typed refusal for a member whose runtime registration must be
 * reloaded before it accepts work (`meta/force_cancel_member`). The error data
 * carries `kind: MEMBER_RELOAD_REQUIRED_KIND` and names the member and the
 * repair, `required_action: "reload_member_registration"`.
 */
export const MEMBER_RELOAD_REQUIRED_CODE = -32029 as const;
/** The `data.kind` of meerkat's reload-required refusal. */
export const MEMBER_RELOAD_REQUIRED_KIND = "mob_member_reload_required" as const;
/** WorkGraph service not configured on this runtime (memory-backend-unavailable pattern). */
export const WORKGRAPH_UNAVAILABLE_CODE = -32041 as const;
/**
 * A saved delivery scope no longer matches the identity's binding
 * (`meta/dispatch` with `expected_scope`): nothing was submitted and
 * nothing was retargeted.
 */
export const STALE_DELIVERY_SCOPE_CODE = -32007 as const;
/** WorkGraph CAS/revision conflict — refetch the item/binding's current revision and retry. */
export const WORKGRAPH_CONFLICT_CODE = -32042 as const;

/**
 * Raised when the caller passes an `after_seq` past the current ledger
 * frontier. The server's `error.data` payload carries `after_cursor` and
 * `latest_cursor` — use the latter to rewind and resume.
 */
export class MobEventsStaleError extends RpcError {
  constructor(
    message: string,
    readonly afterCursor: number,
    readonly latestCursor: number,
    requestId: string,
    method: string,
    data?: unknown,
  ) {
    super(MOB_EVENTS_STALE_CURSOR_CODE, message, requestId, method, data);
    this.name = "MobEventsStaleError";
  }

  /**
   * Reify a generic {@link RpcError} with code `-32010` into the typed
   * form. Reads `after_cursor` / `latest_cursor` from the JSON-RPC
   * `error.data` payload; missing fields fall back to `0`.
   */
  static fromRpcError(err: RpcError): MobEventsStaleError {
    const payload =
      typeof err.data === "object" && err.data !== null
        ? (err.data as Record<string, unknown>)
        : {};
    const afterCursor = Number(payload.after_cursor ?? 0);
    const latestCursor = Number(payload.latest_cursor ?? 0);
    return new MobEventsStaleError(
      err.message,
      Number.isFinite(afterCursor) ? afterCursor : 0,
      Number.isFinite(latestCursor) ? latestCursor : 0,
      err.requestId,
      err.method,
      err.data,
    );
  }
}

// -- Capability errors ----------------------------------------------------

/** Raised when a requested capability is not available on the runtime. */
export class CapabilityUnavailableError extends RpcError {
  constructor(
    message: string,
    requestId = "",
    method = "",
    data?: unknown,
  ) {
    super(CAPABILITY_UNAVAILABLE_CODE, message, requestId, method, data);
    this.name = "CapabilityUnavailableError";
  }
}

/**
 * Raised when an identity's lease was lost mid send/dispatch. Transient and
 * recoverable — the identity simply needs to re-acquire its lease. Distinct
 * from {@link CapabilityUnavailableError} so callers do not treat a recoverable
 * lease loss as a permanent capability gap.
 */
export class LeaseLostError extends RpcError {
  constructor(
    message: string,
    requestId = "",
    method = "",
    data?: unknown,
  ) {
    super(LEASE_LOST_CODE, message, requestId, method, data);
    this.name = "LeaseLostError";
  }
}

/**
 * A send's `expectedSpecDigest` no longer names the identity's registered
 * row. The gateway checks it under the identity's lifecycle lock before
 * delivery, so nothing was submitted.
 */
export class RegisteredSpecChangedError extends RpcError {
  constructor(
    message: string,
    requestId = "",
    method = "",
    data?: unknown,
  ) {
    super(REGISTERED_SPEC_CHANGED_CODE, message, requestId, method, data);
    this.name = "RegisteredSpecChangedError";
  }
}

/**
 * Raised when a scoped dispatch's saved delivery scope no longer matches.
 * Nothing was submitted and the delivery was not retargeted to the
 * identity's current binding. `mismatch` names the atom that moved:
 * `runtime_id`, `generation`, `lease_fencing_token`, `member` or
 * `member_binding` (the member's session, incarnation or fence). Recover an
 * earlier attempt under the same scope with `recoverDelivery`, never by
 * redispatching to a fresh scope.
 */
export class StaleScopeError extends RpcError {
  readonly mismatch: string | null;

  constructor(
    message: string,
    requestId = "",
    method = "",
    data?: unknown,
  ) {
    super(STALE_DELIVERY_SCOPE_CODE, message, requestId, method, data);
    this.name = "StaleScopeError";
    const mismatch =
      data !== null && typeof data === "object"
        ? (data as Record<string, unknown>).mismatch
        : undefined;
    this.mismatch = typeof mismatch === "string" ? mismatch : null;
  }
}

export class MemoryBackendUnavailableError extends RpcError {
  constructor(
    message: string,
    requestId: string,
    method: string,
    data?: unknown,
  ) {
    super(MEMORY_BACKEND_UNAVAILABLE_CODE, message, requestId, method, data);
    this.name = "MemoryBackendUnavailableError";
  }
}

export class ConsoleTimelineReplayUnavailableError extends RpcError {
  constructor(
    message: string,
    requestId: string,
    method: string,
    data?: unknown,
  ) {
    super(CONSOLE_TIMELINE_REPLAY_UNAVAILABLE_CODE, message, requestId, method, data);
    this.name = "ConsoleTimelineReplayUnavailableError";
  }
}

/**
 * Raised when the gateway refuses to start over a storage resolution gap.
 *
 * The refusals are deliberate (storage-unification fail-closed posture):
 * file-name twins (e.g. `sessions.sqlite3` beside `sessions.db`) the layout
 * will not pick between, a session/runtime/blob/metadata/console store that
 * failed to open where older gateways silently fell back to in-memory, or
 * an uncreatable state root. The message names the remediation — run the
 * storage doctor (`meta/storage/doctor`) for twins, fix the database
 * file, or declare the ephemeral choice explicitly (e.g.
 * `runtimeStore.memory()`).
 */
export class StorageResolutionError extends RpcError {
  constructor(
    message: string,
    requestId = "",
    method = "",
    data?: unknown,
  ) {
    super(STORAGE_RESOLUTION_CODE, message, requestId, method, data);
    this.name = "StorageResolutionError";
  }
}

/**
 * An ordinary request reached the gateway before `meta/init` settled. The
 * gateway serves ordinary requests only after init settles and refuses
 * earlier ones at once; startup provider callbacks must not issue ordinary
 * runtime RPCs on the runtime that is starting.
 */
export class InitInProgressError extends RpcError {
  constructor(
    message: string,
    requestId = "",
    method = "",
    data?: unknown,
  ) {
    super(INIT_IN_PROGRESS_CODE, message, requestId, method, data);
    this.name = "InitInProgressError";
  }
}

/**
 * The kinds of composition provenance refusal the gateway sends. A newer
 * gateway may send a kind not listed here, so `kind` is typed to admit any
 * string.
 */
export type CompositionProvenanceRefusalKind =
  | "divergent"
  | "candidate_divergent"
  | "created_by_rehearsal"
  | "missing"
  | "unreadable"
  | "malformed"
  | "unsupported_version"
  | "not_recorded"
  | "unproven_storage";

/**
 * Raised when the gateway refuses to start over the mob's composition
 * provenance.
 *
 * A persistent mob storage records the composition it was created for, and a
 * launch whose composition cannot be proven to match it is refused before the
 * mob actuates. The message names the remedy. `kind` names the refusal:
 *
 * - `"divergent"`: the supplied definition differs from the one recorded for
 *   the storage (revert the change, or move the stored definition with
 *   `MetaBuilder.declareSpecUpdate(expectedRevision)`);
 * - `"candidate_divergent"`: a candidate launch supplied a definition that
 *   differs from the stored one it would boot. Only a gateway launched with
 *   `runtime_options.mob_composition.authority = "candidate"` raises it,
 *   which this builder never sends; the gateway option
 *   `runtime_options.mob_composition.candidate_definition = "stored"`
 *   acknowledges the stored definition;
 * - `"created_by_rehearsal"`: the storage was created by a launch that did
 *   not speak for the durable composition;
 * - `"unreadable"`, `"malformed"`, `"unsupported_version"` or
 *   `"not_recorded"`: the provenance record beside the storage is unusable,
 *   or could not be written;
 * - `"missing"`: no provenance record beside non-empty storage. The gateway
 *   does not raise it today (an authoritative launch adopts and records the
 *   definition instead); library callers can see it;
 * - `"unproven_storage"`: non-empty storage with nothing declared about it.
 *
 * `kind` is `null` when the gateway sent no refusal data. `fields` lists the
 * diverged definition fields as dotted paths (`profiles.lead.tools.deny`);
 * it is empty unless the kind is `divergent` or `candidate_divergent`. The
 * full refusal stays available as `data`.
 */
export class CompositionProvenanceError extends RpcError {
  readonly kind: CompositionProvenanceRefusalKind | (string & {}) | null;
  readonly fields: readonly string[];

  constructor(
    message: string,
    requestId = "",
    method = "",
    data?: unknown,
  ) {
    super(COMPOSITION_PROVENANCE_CODE, message, requestId, method, data);
    this.name = "CompositionProvenanceError";
    const refusal =
      typeof data === "object" && data !== null && !Array.isArray(data)
        ? (data as Record<string, unknown>)
        : {};
    this.kind = typeof refusal.kind === "string" ? refusal.kind : null;
    this.fields = Array.isArray(refusal.fields)
      ? refusal.fields.filter((field): field is string => typeof field === "string")
      : [];
  }
}

/**
 * Raised when a member needs a runtime reload before it accepts work.
 *
 * `meta/force_cancel_member` answers meerkat's typed refusal (code `-32029`,
 * data `kind` `"mob_member_reload_required"`) for a member whose runtime
 * registration cannot serve work. Both must match; any other `-32029` answer
 * stays a plain `RpcError`. `memberId` and
 * `reason` come from the refusal's data, and `requiredAction` names meerkat's
 * repair, `"reload_member_registration"`. Each is `null` when the gateway sent
 * no such field; the full refusal stays available as `data`.
 */
export class MemberReloadRequiredError extends RpcError {
  readonly memberId: string | null;
  readonly reason: string | null;
  readonly requiredAction: string | null;

  constructor(
    message: string,
    requestId = "",
    method = "",
    data?: unknown,
  ) {
    super(MEMBER_RELOAD_REQUIRED_CODE, message, requestId, method, data);
    this.name = "MemberReloadRequiredError";
    const refusal =
      typeof data === "object" && data !== null && !Array.isArray(data)
        ? (data as Record<string, unknown>)
        : {};
    this.memberId = typeof refusal.member_id === "string" ? refusal.member_id : null;
    this.reason = typeof refusal.reason === "string" ? refusal.reason : null;
    this.requiredAction =
      typeof refusal.required_action === "string" ? refusal.required_action : null;
  }
}

/** Raised when a `meta/workgraph/*` call is made but no WorkGraph service is configured. */
export class WorkGraphUnavailableError extends RpcError {
  constructor(
    message: string,
    requestId: string,
    method: string,
    data?: unknown,
  ) {
    super(WORKGRAPH_UNAVAILABLE_CODE, message, requestId, method, data);
    this.name = "WorkGraphUnavailableError";
  }
}

/**
 * Raised on a WorkGraph CAS/revision conflict (the caller's `expected_revision`
 * is stale). `data.detail` carries the upstream message. Refetch the item or
 * attention binding's current revision and retry.
 */
export class WorkGraphConflictError extends RpcError {
  constructor(
    message: string,
    requestId: string,
    method: string,
    data?: unknown,
  ) {
    super(WORKGRAPH_CONFLICT_CODE, message, requestId, method, data);
    this.name = "WorkGraphConflictError";
  }
}

// -- Contract errors ------------------------------------------------------

/** Raised when the SDK and runtime contract versions are incompatible. */
export class ContractMismatchError extends MetaError {
  constructor(message: string) {
    super(message);
    this.name = "ContractMismatchError";
  }
}

// -- Connection errors ----------------------------------------------------

/** Raised when an operation requires a connected runtime but none is available. */
export class NotConnectedError extends MetaError {
  constructor(message: string) {
    super(message);
    this.name = "NotConnectedError";
  }
}

// -- Ticketed turns -------------------------------------------------------

/**
 * The turn a ticket names ran and failed (`meta/turn_result` state
 * `failed`). `reason` carries the runtime's typed detail.
 */
export class TurnFailedError extends MetaError {
  readonly identity: string;
  readonly ticket: string;
  readonly reason: string;
  /** The send/dispatch result when an `*AndWait` call threw this, else `null`. */
  admission: unknown = null;

  constructor(identity: string, ticket: string, reason: string) {
    super(`turn ${ticket} of identity ${identity} failed: ${reason}`);
    this.name = "TurnFailedError";
    this.identity = identity;
    this.ticket = ticket;
    this.reason = reason;
  }
}

/**
 * The gateway knows no turn with this ticket for this identity: never
 * admitted there, another identity's, aged out, or the gateway restarted
 * since. Never guessed in either direction.
 */
/**
 * A server-side wait ended, typed, before what it waited for happened.
 * `outcome` is the gateway's token: `run_failed` (a run failed or was
 * cancelled during the wait), `broken`, `retiring` (retiring, retired or
 * being deleted), `identity_gone` or `shutting_down`. Tolerate future values.
 */
export class WaitEndedError extends MetaError {
  readonly identity: string;
  readonly outcome: string;
  /** The send/dispatch result when an `*AndWait` call threw this, else `null`. */
  admission: unknown = null;
  /**
   * The ticket of the tracked turn the `*AndWait` call was waiting on, else
   * `null` (an untracked wait, or not thrown by an `*AndWait` call).
   */
  ticket: string | null = null;

  constructor(identity: string, outcome: string, detail = "") {
    const message = `wait on identity ${identity} ended: ${outcome}`;
    super(detail ? `${message} (${detail})` : message);
    this.name = "WaitEndedError";
    this.identity = identity;
    this.outcome = outcome;
  }
}

export class TurnUnknownError extends MetaError {
  readonly identity: string;
  readonly ticket: string;
  /**
   * The send/dispatch result when an `*AndWait` call threw this after its
   * delivery was admitted, else `null`: do not redispatch the delivery
   * because its ticket is no longer known.
   */
  admission: unknown = null;

  constructor(identity: string, ticket: string) {
    super(
      `no turn ${ticket} is known for identity ${identity} (never admitted ` +
        `there, aged out, or the gateway restarted)`,
    );
    this.name = "TurnUnknownError";
    this.identity = identity;
    this.ticket = ticket;
  }
}

/**
 * A `trackTurn` delivery was NOT delivered at all (`turnUnavailable.code ===
 * "not_delivered"`: the gateway has no session bridge, or the identity has no
 * bound runtime). Nothing ran and there is no turn to wait for, so retrying
 * is safe, unlike a failed or unknown turn.
 */
export class TurnNotDeliveredError extends MetaError {
  readonly identity: string;
  readonly operation: string;
  readonly code: string;
  readonly reason: string;
  /** The send/dispatch result that reported it, when an `*AndWait` call threw this. */
  admission: unknown = null;

  constructor(identity: string, operation: string, code: string, reason: string) {
    super(
      `${operation} for identity ${identity} was not delivered ` +
        `(${code}: ${reason}); there is no turn to wait for`,
    );
    this.name = "TurnNotDeliveredError";
    this.identity = identity;
    this.operation = operation;
    this.code = code;
    this.reason = reason;
  }
}

/**
 * An `*AndWait` call made with `requireAttribution: true` could not track its
 * delivery's own turn, so it refused to wait identity-wide.
 *
 * The delivery was ADMITTED: the work was handed to the identity and will
 * run, or already ran. Keep `admission` (the full send/dispatch result) and
 * do not redispatch it. `code` and `reason` carry the typed `turnUnavailable`
 * reason (`autonomous_host`, `runtime_refused`, ...), or are `null` when the
 * gateway predates turn tickets. Without `requireAttribution` the call waits
 * identity-wide instead and resolves a non-attributed result.
 */
export class TurnTrackingUnavailableError extends MetaError {
  readonly identity: string;
  readonly operation: string;
  readonly admission: unknown;
  readonly code: string | null;
  readonly reason: string | null;

  constructor(
    identity: string,
    operation: string,
    admission: unknown,
    code: string | null,
    reason: string | null,
  ) {
    const detail = code !== null ? `${code}: ${reason}` : "no turn ticket returned";
    super(
      `${operation} for identity ${identity} was admitted but its turn ` +
        `cannot be tracked (${detail}); the admission result is retained, ` +
        `do not redispatch`,
    );
    this.name = "TurnTrackingUnavailableError";
    this.identity = identity;
    this.operation = operation;
    this.admission = admission;
    this.code = code;
    this.reason = reason;
  }
}

/**
 * Observing an ADMITTED delivery's turn failed for a transport or RPC reason
 * (the gateway did not answer the wait), and retrying that exact observation
 * ran out of the caller's deadline.
 *
 * The turn may still run, or may already have completed. `admission` is the
 * full send/dispatch result and `ticket` names the turn (`null` only on the
 * identity-wide wait). Read the turn later with `waitForTurn(ticket)` or
 * `turnResult(ticket)`; never redispatch the business work because of this
 * error. `cause` is the last observation failure and `attempts` counts
 * observation tries.
 */
export class PostAdmissionObservationError extends MetaError {
  readonly identity: string;
  readonly operation: string;
  readonly admission: unknown;
  readonly ticket: string | null;
  readonly attempts: number;

  constructor(
    identity: string,
    operation: string,
    admission: unknown,
    ticket: string | null,
    attempts: number,
    cause: unknown,
  ) {
    const turn = ticket !== null ? `turn ${ticket}` : "its turn";
    super(
      `${operation} for identity ${identity} was admitted, but observing ` +
        `${turn} failed after ${attempts} attempt(s); the admission result is ` +
        `retained, do not redispatch`,
      { cause },
    );
    this.name = "PostAdmissionObservationError";
    this.identity = identity;
    this.operation = operation;
    this.admission = admission;
    this.ticket = ticket;
    this.attempts = attempts;
  }
}

/**
 * A ticketed turn did not settle by the caller's deadline. From an
 * `*AndWait` call it carries `admission` (the send/dispatch result) so the
 * turn can be read later instead of redispatched.
 */
export class TurnWaitTimeoutError extends MetaError {
  readonly identity: string;
  readonly ticket: string;
  readonly timeoutMs: number;
  /** The send/dispatch result when an `*AndWait` call threw this, else `null`. */
  admission: unknown = null;

  constructor(identity: string, ticket: string, timeoutMs: number) {
    super(
      `turn ${ticket} of identity ${identity} did not complete within ` +
        `${timeoutMs}ms`,
    );
    this.name = "TurnWaitTimeoutError";
    this.identity = identity;
    this.ticket = ticket;
    this.timeoutMs = timeoutMs;
  }
}

// -- Cross-module identity helpers ---------------------------------------

/**
 * Structural test for an `RpcError`. Pre-fix every site used
 * `err instanceof RpcError`, but JS class identity is module-scoped:
 * dual CJS+ESM packaging, vitest module isolation, and hoisted-vs-
 * nested workspace deps can produce two `RpcError` constructors that
 * fail `instanceof` for each other's instances. The structural check
 * survives those splits.
 */
export function isRpcError(err: unknown): err is RpcError {
  if (err instanceof RpcError) {
    return true;
  }
  if (err === null || typeof err !== "object") {
    return false;
  }
  const candidate = err as { name?: unknown; code?: unknown };
  return (
    candidate.name === "RpcError" ||
    candidate.name === "MobEventsStaleError" ||
    candidate.name === "CapabilityUnavailableError" ||
    candidate.name === "MemoryBackendUnavailableError" ||
    candidate.name === "ConsoleTimelineReplayUnavailableError" ||
    candidate.name === "StorageResolutionError" ||
    candidate.name === "CompositionProvenanceError" ||
    candidate.name === "WorkGraphUnavailableError" ||
    candidate.name === "WorkGraphConflictError"
  ) && typeof candidate.code === "number";
}

/** Structural test for a `MobEventsStaleError`. See `isRpcError`. */
export function isMobEventsStaleError(err: unknown): err is MobEventsStaleError {
  if (err instanceof MobEventsStaleError) {
    return true;
  }
  return (
    isRpcError(err) &&
    err.code === MOB_EVENTS_STALE_CURSOR_CODE &&
    (err as { name: string }).name === "MobEventsStaleError"
  );
}

// -- Backward compatibility -----------------------------------------------

/** @deprecated Use {@link RpcError} instead. */
export const MetaRpcError = RpcError;
/** @deprecated Use {@link RpcError} instead. */
export type MetaRpcError = RpcError;
