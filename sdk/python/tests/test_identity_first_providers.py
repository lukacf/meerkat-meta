"""TDD tests for provider protocols and result models (REQ-42, REQ-43c)."""
import pytest

from meerkat_meta.identity_first_models import (
    DurableAgentSpec,
    ManagedPeerEdge,
)
from meerkat_meta.identity_first_providers import (
    AgentCustomizerProtocol,
    ContinuityFailure,
    ContinuityRecord,
    ContinuityResolveState,
    ContinuityStoreProvider,
    LeaseAcquireResult,
    LeaseGrant,
    LeaseProviderProtocol,
    LeaseRenewResult,
    RosterProviderProtocol,
    SessionSnapshot,
    TopologyProviderProtocol,
)


# ---------------------------------------------------------------------------
# REQ-43c: Provider result models
# ---------------------------------------------------------------------------


class TestContinuityRecord:
    def test_fields(self):
        rec = ContinuityRecord(
            identity="a:main",
            agent_runtime_id="rt-1",
            session_id="s-1",
            generation=0,
            checkpoint_version=3,
        )
        assert rec.identity == "a:main"
        assert rec.generation == 0

    def test_to_dict(self):
        rec = ContinuityRecord(
            identity="a:main",
            agent_runtime_id="rt-1",
            session_id="s-1",
            generation=1,
            checkpoint_version=5,
        )
        d = rec.to_dict()
        assert d == {
            "identity": "a:main",
            "agent_runtime_id": "rt-1",
            "session_id": "s-1",
            "generation": 1,
            "checkpoint_version": 5,
        }

    def test_from_dict(self):
        rec = ContinuityRecord.from_dict({
            "identity": "b:main",
            "agent_runtime_id": "rt-2",
            "session_id": "s-2",
            "generation": 2,
            "checkpoint_version": 10,
        })
        assert rec.identity == "b:main"
        assert rec.checkpoint_version == 10

    def test_round_trip(self):
        original = ContinuityRecord(
            identity="x:1", agent_runtime_id="rt-x",
            session_id="s-x", generation=5, checkpoint_version=99,
        )
        restored = ContinuityRecord.from_dict(original.to_dict())
        assert restored == original


class TestContinuityFailure:
    def test_fields(self):
        f = ContinuityFailure(
            identity="a:main",
            kind="snapshot_missing",
            record=None,
            detail="no snapshot found",
        )
        assert f.kind == "snapshot_missing"
        assert f.detail == "no snapshot found"

    def test_to_dict_with_record(self):
        rec = ContinuityRecord(
            identity="a:main", agent_runtime_id="rt-1",
            session_id="s-1", generation=0, checkpoint_version=1,
        )
        f = ContinuityFailure(
            identity="a:main", kind="snapshot_corrupted",
            record=rec, detail="crc mismatch",
        )
        d = f.to_dict()
        assert d["kind"] == "snapshot_corrupted"
        assert d["record"]["session_id"] == "s-1"

    def test_from_dict(self):
        f = ContinuityFailure.from_dict({
            "identity": "a:main",
            "kind": "generation_mismatch",
            "record": None,
            "detail": "expected 3 got 1",
        })
        assert f.kind == "generation_mismatch"
        assert f.record is None

    def test_round_trip(self):
        original = ContinuityFailure(
            identity="a:main", kind="store_unavailable",
            record=None, detail="timeout",
        )
        restored = ContinuityFailure.from_dict(original.to_dict())
        assert restored.identity == original.identity
        assert restored.kind == original.kind
        assert restored.detail == original.detail


class TestContinuityResolveState:
    def test_uninitialized(self):
        s = ContinuityResolveState(state="uninitialized")
        assert s.state == "uninitialized"
        assert s.record is None
        assert s.failure is None

    def test_ready(self):
        rec = ContinuityRecord(
            identity="a:main", agent_runtime_id="rt-1",
            session_id="s-1", generation=0, checkpoint_version=0,
        )
        s = ContinuityResolveState(state="ready", record=rec)
        assert s.state == "ready"
        assert s.record is not None

    def test_broken(self):
        fail = ContinuityFailure(
            identity="a:main", kind="snapshot_missing",
            record=None, detail="gone",
        )
        s = ContinuityResolveState(state="broken", failure=fail)
        assert s.state == "broken"
        assert s.failure is not None

    def test_to_dict_uninitialized(self):
        s = ContinuityResolveState(state="uninitialized")
        d = s.to_dict()
        assert d == {"state": "uninitialized"}

    def test_to_dict_ready(self):
        rec = ContinuityRecord(
            identity="a:main", agent_runtime_id="rt-1",
            session_id="s-1", generation=0, checkpoint_version=0,
        )
        d = ContinuityResolveState(state="ready", record=rec).to_dict()
        assert d["state"] == "ready"
        assert d["record"]["identity"] == "a:main"

    def test_from_dict_uninitialized(self):
        s = ContinuityResolveState.from_dict({"state": "uninitialized"})
        assert s.state == "uninitialized"

    def test_from_dict_ready(self):
        s = ContinuityResolveState.from_dict({
            "state": "ready",
            "record": {
                "identity": "a:main",
                "agent_runtime_id": "rt-1",
                "session_id": "s-1",
                "generation": 0,
                "checkpoint_version": 0,
            },
        })
        assert s.state == "ready"
        assert s.record is not None
        assert s.record.identity == "a:main"

    def test_from_dict_broken(self):
        s = ContinuityResolveState.from_dict({
            "state": "broken",
            "failure": {
                "identity": "a:main",
                "kind": "store_unavailable",
                "record": None,
                "detail": "timeout",
            },
        })
        assert s.state == "broken"
        assert s.failure.kind == "store_unavailable"


class TestSessionSnapshot:
    def test_bytes_wrapper(self):
        snap = SessionSnapshot(data=b"\x00\x01\x02")
        assert snap.data == b"\x00\x01\x02"

    def test_to_dict_base64(self):
        snap = SessionSnapshot(data=b"hello")
        d = snap.to_dict()
        import base64
        assert d["data"] == base64.b64encode(b"hello").decode()

    def test_from_dict_base64(self):
        import base64
        encoded = base64.b64encode(b"world").decode()
        snap = SessionSnapshot.from_dict({"data": encoded})
        assert snap.data == b"world"

    def test_round_trip(self):
        original = SessionSnapshot(data=b"\xff\xfe\xfd")
        restored = SessionSnapshot.from_dict(original.to_dict())
        assert restored.data == original.data


class TestLeaseGrant:
    def test_fields(self):
        g = LeaseGrant(identity="a:main", fencing_token=42, ttl_ms=30000)
        assert g.identity == "a:main"
        assert g.fencing_token == 42
        assert g.ttl_ms == 30000

    def test_to_dict(self):
        g = LeaseGrant(identity="a:main", fencing_token=1, ttl_ms=5000)
        assert g.to_dict() == {
            "identity": "a:main",
            "fencing_token": 1,
            "ttl": 5000,
        }

    def test_from_dict(self):
        g = LeaseGrant.from_dict({
            "identity": "b:main",
            "fencing_token": 99,
            "ttl_ms": 10000,
        })
        assert g.identity == "b:main"
        assert g.fencing_token == 99

    def test_round_trip(self):
        original = LeaseGrant(identity="x:1", fencing_token=7, ttl_ms=60000)
        restored = LeaseGrant.from_dict(original.to_dict())
        assert restored == original


class TestLeaseAcquireResult:
    def test_acquired(self):
        grant = LeaseGrant(identity="a:main", fencing_token=1, ttl_ms=5000)
        r = LeaseAcquireResult(status="acquired", grant=grant)
        assert r.status == "acquired"
        assert r.grant is not None

    def test_already_held(self):
        r = LeaseAcquireResult(status="already_held", holder="other-runtime")
        assert r.status == "already_held"
        assert r.holder == "other-runtime"

    def test_to_dict_acquired(self):
        grant = LeaseGrant(identity="a:main", fencing_token=1, ttl_ms=5000)
        d = LeaseAcquireResult(status="acquired", grant=grant).to_dict()
        assert d["result"] == "acquired"
        assert d["fencing_token"] == 1
        assert d["ttl"] == 5000

    def test_to_dict_already_held(self):
        d = LeaseAcquireResult(
            status="already_held", identity="a:main", holder="other",
        ).to_dict()
        assert d["result"] == "already_held"
        assert d["identity"] == "a:main"
        assert d["holder"] == "other"

    def test_from_dict_acquired(self):
        r = LeaseAcquireResult.from_dict({
            "result": "acquired",
            "identity": "a:main",
            "fencing_token": 5,
            "ttl": 3000,
        })
        assert r.status == "acquired"
        assert r.grant.fencing_token == 5

    def test_from_dict_already_held(self):
        r = LeaseAcquireResult.from_dict({
            "status": "already_held",
            "holder": "runtime-2",
        })
        assert r.status == "already_held"
        assert r.holder == "runtime-2"


class TestLeaseRenewResult:
    def test_renewed(self):
        grant = LeaseGrant(identity="a:main", fencing_token=2, ttl_ms=5000)
        r = LeaseRenewResult(status="renewed", grant=grant)
        assert r.status == "renewed"

    def test_lost(self):
        r = LeaseRenewResult(status="lost")
        assert r.status == "lost"
        assert r.grant is None

    def test_to_dict_renewed(self):
        grant = LeaseGrant(identity="a:main", fencing_token=2, ttl_ms=5000)
        d = LeaseRenewResult(status="renewed", grant=grant).to_dict()
        assert d["result"] == "renewed"
        assert d["fencing_token"] == 2
        assert d["ttl"] == 5000

    def test_to_dict_lost(self):
        d = LeaseRenewResult(status="lost", identity="a:main").to_dict()
        assert d == {"result": "lost", "identity": "a:main"}

    def test_from_dict_renewed(self):
        r = LeaseRenewResult.from_dict({
            "result": "renewed",
            "identity": "a:main",
            "fencing_token": 10,
            "ttl": 9000,
        })
        assert r.status == "renewed"
        assert r.grant.fencing_token == 10

    def test_from_dict_lost(self):
        r = LeaseRenewResult.from_dict({"status": "lost"})
        assert r.status == "lost"
        assert r.grant is None


# ---------------------------------------------------------------------------
# REQ-42: Provider Protocol classes
# ---------------------------------------------------------------------------


class TestContinuityStoreProtocol:
    """ContinuityStoreProvider protocol has correct methods."""

    def test_protocol_methods(self):
        import inspect
        members = {name for name, _ in inspect.getmembers(
            ContinuityStoreProvider, predicate=inspect.isfunction
        )}
        assert "resolve_many" in members
        assert "load_session_snapshot" in members
        assert "save_session_snapshot" in members
        assert "upsert_continuity_record" in members
        assert "delete_continuity_record" in members

    def test_mock_implementation(self):
        """A class implementing all methods satisfies the protocol."""

        class MockStore:
            async def resolve_many(self, identities):
                return {}

            async def load_session_snapshot(self, session_id):
                return None

            async def resolve_record_by_session(self, session_id):
                # Required by the protocol: an authoritative absence must be
                # said out loud, not inherited from a missing method.
                return None

            async def save_session_snapshot(
                self, identity, session_id, generation, version,
                fencing_token, snapshot,
            ):
                pass

            async def upsert_continuity_record(self, record, fencing_token):
                pass

            async def delete_continuity_record(self, identity, fencing_token):
                pass

        store = MockStore()
        assert isinstance(store, ContinuityStoreProvider)


class TestLeaseProviderProtocol:
    """LeaseProviderProtocol has correct methods."""

    def test_protocol_methods(self):
        import inspect
        members = {name for name, _ in inspect.getmembers(
            LeaseProviderProtocol, predicate=inspect.isfunction
        )}
        assert "acquire_leases" in members
        assert "renew_leases" in members
        assert "release_leases" in members

    def test_mock_implementation(self):
        class MockLease:
            async def acquire_leases(self, identities, runtime_instance):
                return {}

            async def renew_leases(self, grants):
                return {}

            async def release_leases(self, grants):
                pass

        assert isinstance(MockLease(), LeaseProviderProtocol)


class TestRosterProviderProtocol:
    def test_protocol_methods(self):
        import inspect
        members = {name for name, _ in inspect.getmembers(
            RosterProviderProtocol, predicate=inspect.isfunction
        )}
        assert "roster" in members

    def test_mock_implementation(self):
        class MockRoster:
            async def roster(self, context):
                return []

        assert isinstance(MockRoster(), RosterProviderProtocol)


class TestAgentCustomizerProtocol:
    def test_protocol_methods(self):
        import inspect
        members = {name for name, _ in inspect.getmembers(
            AgentCustomizerProtocol, predicate=inspect.isfunction
        )}
        assert "customize_build" in members

    def test_mock_implementation(self):
        class MockCustomizer:
            async def customize_build(self, context, spec, draft):
                pass

            async def after_create(self, identity, session_id, context):
                pass

        assert isinstance(MockCustomizer(), AgentCustomizerProtocol)


class TestTopologyProviderProtocol:
    def test_protocol_methods(self):
        import inspect
        members = {name for name, _ in inspect.getmembers(
            TopologyProviderProtocol, predicate=inspect.isfunction
        )}
        assert "compute_edges" in members

    def test_mock_implementation(self):
        class MockTopology:
            async def compute_edges(self, target_identities, context):
                return []

        assert isinstance(MockTopology(), TopologyProviderProtocol)
