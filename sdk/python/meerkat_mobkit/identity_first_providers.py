"""Provider protocols and result models for identity-first continuity (REQ-42, REQ-43c)."""
from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from .identity_first_models import RosterContext


# ---------------------------------------------------------------------------
# Result models (REQ-43c)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ContinuityRecord:
    """Durable identity binding.

    checkpoint_version is monotonic for (identity, generation). It advances
    across session_id rotations and resets only after a destructive reset
    advances generation.
    """

    identity: str
    agent_runtime_id: str
    session_id: str
    generation: int
    checkpoint_version: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "identity": self.identity,
            "agent_runtime_id": self.agent_runtime_id,
            "session_id": self.session_id,
            "generation": self.generation,
            "checkpoint_version": self.checkpoint_version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ContinuityRecord:
        return cls(
            identity=data["identity"],
            agent_runtime_id=data["agent_runtime_id"],
            session_id=data["session_id"],
            generation=data["generation"],
            checkpoint_version=data["checkpoint_version"],
        )


@dataclass(frozen=True)
class ContinuityFailure:
    identity: str
    kind: str
    record: ContinuityRecord | None
    detail: str

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "identity": self.identity,
            "kind": self.kind,
            "detail": self.detail,
        }
        result["record"] = self.record.to_dict() if self.record else None
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ContinuityFailure:
        rec_raw = data.get("record")
        return cls(
            identity=data["identity"],
            kind=data["kind"],
            record=ContinuityRecord.from_dict(rec_raw) if rec_raw else None,
            detail=data["detail"],
        )


@dataclass
class ContinuityResolveState:
    """Tagged union: state is 'uninitialized', 'ready', or 'broken'."""

    state: str
    record: ContinuityRecord | None = None
    failure: ContinuityFailure | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"state": self.state}
        if self.record is not None:
            result["record"] = self.record.to_dict()
        if self.failure is not None:
            result["failure"] = self.failure.to_dict()
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ContinuityResolveState:
        rec_raw = data.get("record")
        fail_raw = data.get("failure")
        return cls(
            state=data["state"],
            record=ContinuityRecord.from_dict(rec_raw) if rec_raw else None,
            failure=ContinuityFailure.from_dict(fail_raw) if fail_raw else None,
        )


@dataclass(frozen=True)
class SessionSnapshot:
    """Opaque session snapshot. data is bytes; wire format is base64."""

    data: bytes

    def to_dict(self) -> dict[str, Any]:
        return {"data": base64.b64encode(self.data).decode()}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SessionSnapshot:
        return cls(data=base64.b64decode(data["data"]))


@dataclass(frozen=True)
class LeaseGrant:
    identity: str
    fencing_token: int
    ttl_ms: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "identity": self.identity,
            "fencing_token": self.fencing_token,
            "ttl": self.ttl_ms,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LeaseGrant:
        return cls(
            identity=data["identity"],
            fencing_token=data["fencing_token"],
            ttl_ms=data.get("ttl", data.get("ttl_ms", 0)),
        )


@dataclass
class LeaseAcquireResult:
    """Tagged union: status is 'acquired' or 'already_held'."""

    status: str
    grant: LeaseGrant | None = None
    identity: str | None = None
    holder: str | None = None

    def to_dict(self) -> dict[str, Any]:
        if self.status == "already_held":
            result: dict[str, Any] = {
                "result": "already_held",
                "identity": self.identity or (self.grant.identity if self.grant else ""),
            }
        else:
            result = {"result": "acquired"}
            if self.grant is not None:
                result.update(self.grant.to_dict())
        if self.holder is not None:
            result["holder"] = self.holder
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LeaseAcquireResult:
        grant_raw = data.get("grant")
        status = data.get("result", data.get("status", "acquired"))
        return cls(
            status="already_held" if status == "already_held" else status,
            grant=LeaseGrant.from_dict(grant_raw or data) if grant_raw or status == "acquired" else None,
            identity=data.get("identity"),
            holder=data.get("holder"),
        )


@dataclass
class LeaseRenewResult:
    """Tagged union: status is 'renewed' or 'lost'."""

    status: str
    grant: LeaseGrant | None = None
    identity: str | None = None

    def to_dict(self) -> dict[str, Any]:
        if self.status == "lost":
            return {
                "result": "lost",
                "identity": self.identity or (self.grant.identity if self.grant else ""),
            }
        result: dict[str, Any] = {"result": "renewed"}
        if self.grant is not None:
            result.update(self.grant.to_dict())
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LeaseRenewResult:
        grant_raw = data.get("grant")
        status = data.get("result", data.get("status", "renewed"))
        return cls(
            status=status,
            grant=LeaseGrant.from_dict(grant_raw or data) if grant_raw or status == "renewed" else None,
            identity=data.get("identity"),
        )


# ---------------------------------------------------------------------------
# Provider protocols (REQ-42)
# ---------------------------------------------------------------------------

@runtime_checkable
class ContinuityStoreProvider(Protocol):
    """Continuity provider; retained historical owners also implement SessionOwnershipProvider.

    Without that optional capability, owner lookup supports current bindings only.
    """

    async def resolve_many(
        self, identities: list[str],
    ) -> dict[str, ContinuityResolveState]: ...

    async def load_session_snapshot(
        self, session_id: str,
    ) -> SessionSnapshot | None: ...

    async def resolve_record_by_session(
        self, session_id: str,
    ) -> tuple[ContinuityRecord, int, int] | None:
        """Resolve the record bound to ``session_id``, with the facts a registration carries.

        Returns ``(record, fencing_token, checkpoint_version)``, or ``None`` for
        an AUTHORITATIVE absence only.

        A provider that cannot answer must RAISE rather than return ``None``.
        The platform trait requires this method precisely so those two stop
        sharing a value: both owner-authority passes treat ``None`` as "this
        identity has no durable owner" and skip, so a provider that answered
        ``None`` because it was never taught the question silently disabled
        owner pre-registration and made the prepare-time durable-tail refusal
        unclearable.
        """
        ...

    async def save_session_snapshot(
        self, identity: str, session_id: str, generation: int,
        version: int, fencing_token: int, snapshot: SessionSnapshot,
    ) -> None:
        """Persist a snapshot if fencing_token is current and version advances the identity/generation head."""
        ...

    async def upsert_continuity_record(
        self, record: ContinuityRecord, fencing_token: int,
    ) -> None:
        """Persist the current binding without rewinding checkpoint_version on session rebind."""
        ...

    async def delete_continuity_record(
        self, identity: str, fencing_token: int,
    ) -> None: ...


@runtime_checkable
class SessionOwnershipProvider(Protocol):
    """Optional historical ownership capability of a ContinuityStoreProvider."""

    async def session_owner(self, session_id: str) -> str | None:
        """Return the immutable owner of the exact session, including retained history.

        Return None for an authoritative absence. Raise on conflicting ownership
        or when the store cannot answer. Current bindings are not a substitute
        for retained ownership after a session rotates away.
        """
        ...


@runtime_checkable
class LeaseProviderProtocol(Protocol):
    async def acquire_leases(
        self, identities: list[str], runtime_instance: str,
    ) -> dict[str, LeaseAcquireResult]: ...

    async def renew_leases(
        self, grants: list[LeaseGrant],
    ) -> dict[str, LeaseRenewResult]:
        """Renew atomically from the caller's perspective.

        Raising is a pre-commit failure: the provider MUST leave every input
        grant unchanged. Each returned ``renewed`` or ``lost`` result is
        committed for that identity before the call returns.
        """
        ...

    async def release_leases(
        self, grants: list[LeaseGrant],
    ) -> None: ...


@runtime_checkable
class RosterProviderProtocol(Protocol):
    async def roster(self, context: RosterContext) -> list[Any]:
        """Return the desired ``DurableAgentSpec`` list.

        ``context`` carries the booted mob definition and the identities the
        runtime has already registered; see ``RosterContext``.
        """
        ...


@runtime_checkable
class AgentCustomizerProtocol(Protocol):
    async def customize_build(
        self, context: Any, spec: Any, draft: Any,
    ) -> None: ...

    async def after_create(
        self, identity: str, session_id: str, context: Any,
    ) -> None: ...


@runtime_checkable
class TopologyProviderProtocol(Protocol):
    async def compute_edges(
        self, target_identities: list[str], context: Any,
    ) -> list[Any]: ...
