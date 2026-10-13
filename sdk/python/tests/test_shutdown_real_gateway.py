"""Real SDK/gateway teardown regression for callback-backed lease providers."""
from __future__ import annotations

import os

import pytest

from meerkat_meta.builder import Meta
from meerkat_meta.identity_first_models import (
    DurableAgentSpec,
    IdentityBootstrapMode,
)
from meerkat_meta.identity_first_providers import (
    ContinuityRecord,
    ContinuityResolveState,
    LeaseAcquireResult,
    LeaseGrant,
    LeaseRenewResult,
)


def _resolve_gateway_bin() -> str:
    override = os.environ.get("META_GATEWAY_BIN", "").strip()
    if override:
        return override
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "..", "..", "target", "debug", "rpc_gateway",
    )


_GATEWAY_BIN = _resolve_gateway_bin()
_IDENTITY = "agent:shutdown"
_EXACT_GRANT = LeaseGrant(
    identity=_IDENTITY,
    fencing_token=991,
    ttl_ms=600_000,
)
_MOB_TOML = """\
[mob]
id = "shutdown-real-gateway"

[profiles.default]
model = "gpt-5.5"
external_addressable = true

[profiles.default.tools]
comms = true
"""


class _Roster:
    async def roster(self, context):
        del context
        return [DurableAgentSpec(identity=_IDENTITY, profile="default")]


class _ContinuityStore:
    def __init__(self):
        self.records: dict[str, ContinuityRecord] = {}
        self.snapshots: dict[str, object] = {}
        self.fencing_tokens: dict[str, int] = {}

    async def resolve_many(self, identities):
        return {
            identity: (
                ContinuityResolveState(state="ready", record=self.records[identity])
                if identity in self.records
                else ContinuityResolveState(state="uninitialized")
            )
            for identity in identities
        }

    async def load_session_snapshot(self, session_id):
        return self.snapshots.get(session_id)

    async def save_session_snapshot(
        self,
        identity,
        session_id,
        generation,
        version,
        fencing_token,
        snapshot,
    ):
        record = self.records[identity]
        assert record.session_id == session_id
        assert record.generation == generation
        assert self.fencing_tokens[identity] == fencing_token
        self.snapshots[session_id] = snapshot
        self.records[identity] = ContinuityRecord(
            identity=record.identity,
            agent_runtime_id=record.agent_runtime_id,
            session_id=record.session_id,
            generation=record.generation,
            checkpoint_version=version,
        )

    async def upsert_continuity_record(self, record, fencing_token):
        self.records[record.identity] = record
        self.fencing_tokens[record.identity] = fencing_token

    async def delete_continuity_record(self, identity, fencing_token):
        assert self.fencing_tokens.get(identity) == fencing_token
        self.records.pop(identity, None)
        self.fencing_tokens.pop(identity, None)

    async def session_snapshot_matches_current(
        self,
        identity,
        session_id,
        generation,
        checkpoint_version,
        fencing_token,
        snapshot,
    ):
        record = self.records.get(identity)
        return (
            record is not None
            and record.session_id == session_id
            and record.generation == generation
            and record.checkpoint_version == checkpoint_version
            and self.fencing_tokens.get(identity) == fencing_token
            and self.snapshots.get(session_id) == snapshot
        )

    async def delete_session_snapshot_if_current_revision(self, *args):
        del args
        return False


class _ExactLeaseProvider:
    def __init__(self):
        self.released: list[LeaseGrant] = []
        self.gateway_process = None

    async def acquire_leases(self, identities, runtime_instance):
        del runtime_instance
        assert identities == [_IDENTITY]
        return {
            _IDENTITY: LeaseAcquireResult(status="acquired", grant=_EXACT_GRANT)
        }

    async def renew_leases(self, grants):
        return {
            grant.identity: LeaseRenewResult(status="renewed", grant=grant)
            for grant in grants
        }

    async def release_leases(self, grants):
        assert self.gateway_process is not None
        assert self.gateway_process.poll() is None, (
            "release callback arrived only after the gateway process exited"
        )
        self.released.extend(grants)


@pytest.mark.skipif(
    not os.path.isfile(_GATEWAY_BIN),
    reason=f"Gateway binary not found at {_GATEWAY_BIN}",
)
@pytest.mark.asyncio
@pytest.mark.timeout(90)
async def test_runtime_shutdown_releases_exact_external_grant_before_process_exit(
    tmp_path,
):
    scratch_dir = tmp_path / "scratch"
    scratch_dir.mkdir()
    lease_provider = _ExactLeaseProvider()
    runtime = await (
        Meta.builder()
        .gateway(_GATEWAY_BIN)
        .mob_inline(_MOB_TOML)
        .roster(_Roster())
        .continuity_store(_ContinuityStore())
        .lease_provider(lease_provider)
        .scratch_dir(str(scratch_dir))
        .identity_bootstrap_mode(IdentityBootstrapMode.eager_materialize())
        .build()
    )

    transport = runtime._transport
    assert transport is not None
    process = transport._process
    assert process is not None and process.poll() is None
    lease_provider.gateway_process = process

    await runtime.shutdown()

    assert lease_provider.released == [_EXACT_GRANT]
    assert process.poll() is not None
