"""TDD tests for builder extensions (REQ-44) and callback dispatcher routing (REQ-45)."""
import pytest

from meerkat_meta.builder import Meta, MetaBuilder
from meerkat_meta.agent_builder import CallbackDispatcher
from meerkat_meta.identity_first_models import (
    AgentBuildContext,
    AgentBuildDraft,
    DurableAgentSpec,
    ManagedPeerEdge,
    RosterContext,
)
from meerkat_meta.identity_first_providers import (
    ContinuityRecord,
    ContinuityResolveState,
    LeaseAcquireResult,
    LeaseGrant,
    LeaseRenewResult,
    SessionSnapshot,
)


# ---------------------------------------------------------------------------
# REQ-44: Builder methods
# ---------------------------------------------------------------------------


class TestBuilderContinuityStore:
    def test_continuity_store_returns_builder(self):
        class FakeStore:
            pass

        b = Meta.builder().continuity_store(FakeStore())
        assert isinstance(b, MetaBuilder)

    def test_continuity_store_sets_config(self):
        class FakeStore:
            pass

        store = FakeStore()
        b = Meta.builder().continuity_store(store)
        assert b._config.continuity_store is store


class TestBuilderLeaseProvider:
    def test_lease_provider_returns_builder(self):
        class FakeLease:
            pass

        b = Meta.builder().lease_provider(FakeLease())
        assert isinstance(b, MetaBuilder)

    def test_lease_provider_sets_config(self):
        class FakeLease:
            pass

        lp = FakeLease()
        b = Meta.builder().lease_provider(lp)
        assert b._config.lease_provider is lp


class TestBuilderScratchDir:
    def test_scratch_dir_returns_builder(self):
        b = Meta.builder().scratch_dir("/tmp/scratch")
        assert isinstance(b, MetaBuilder)

    def test_scratch_dir_sets_config(self):
        b = Meta.builder().scratch_dir("/tmp/scratch")
        assert b._config.scratch_dir == "/tmp/scratch"


class TestBuilderMutualExclusivity:
    """persistent_state and continuity_store/lease_provider are mutually exclusive."""

    @pytest.mark.asyncio
    async def test_persistent_state_then_continuity_store_fails(self):
        class FakeStore:
            pass

        b = (
            Meta.builder()
            .persistent_state("/tmp/state")
            .continuity_store(FakeStore())
            .lease_provider(FakeStore())
            .scratch_dir("/tmp/scratch")
            .mob("nonexistent.toml")
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            await b.build()

    @pytest.mark.asyncio
    async def test_continuity_store_then_persistent_state_fails(self):
        class FakeStore:
            pass

        b = (
            Meta.builder()
            .continuity_store(FakeStore())
            .lease_provider(FakeStore())
            .scratch_dir("/tmp/scratch")
            .persistent_state("/tmp/state")
            .mob("nonexistent.toml")
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            await b.build()


# ---------------------------------------------------------------------------
# REQ-45: Callback dispatcher — provider routing
# ---------------------------------------------------------------------------


class TestCallbackDispatcherContinuityStore:
    """Dispatcher routes continuity store callbacks to registered provider."""

    @pytest.mark.asyncio
    async def test_resolve_many_routed(self):
        results = {}

        class MockStore:
            async def resolve_many(self, identities):
                return {
                    i: ContinuityResolveState(state="uninitialized")
                    for i in identities
                }

            async def load_session_snapshot(self, session_id):
                return None

            async def save_session_snapshot(self, *args):
                pass

            async def upsert_continuity_record(self, *args):
                pass

            async def delete_continuity_record(self, *args):
                pass

        d = CallbackDispatcher()
        d.register_continuity_store(MockStore())

        result = await d.handle_callback("callback/continuity_store/resolve_many", {
            "identities": ["a:main", "b:main"],
        })
        assert "a:main" in result
        assert result["a:main"]["state"] == "uninitialized"

    @pytest.mark.asyncio
    async def test_load_session_snapshot_routed(self):
        import base64

        class MockStore:
            async def resolve_many(self, identities):
                return {}

            async def load_session_snapshot(self, session_id):
                return SessionSnapshot(data=b"snapshot-data")

            async def save_session_snapshot(self, *args):
                pass

            async def upsert_continuity_record(self, *args):
                pass

            async def delete_continuity_record(self, *args):
                pass

        d = CallbackDispatcher()
        d.register_continuity_store(MockStore())

        result = await d.handle_callback(
            "callback/continuity_store/load_session_snapshot",
            {"session_id": "s-1"},
        )
        assert result is not None
        assert result["data"] == base64.b64encode(b"snapshot-data").decode()

    @pytest.mark.asyncio
    async def test_load_session_snapshot_returns_none(self):
        class MockStore:
            async def resolve_many(self, identities):
                return {}

            async def load_session_snapshot(self, session_id):
                return None

            async def save_session_snapshot(self, *args):
                pass

            async def upsert_continuity_record(self, *args):
                pass

            async def delete_continuity_record(self, *args):
                pass

        d = CallbackDispatcher()
        d.register_continuity_store(MockStore())

        result = await d.handle_callback(
            "callback/continuity_store/load_session_snapshot",
            {"session_id": "s-1"},
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_save_session_snapshot_routed(self):
        import base64
        saved = {}

        class MockStore:
            async def resolve_many(self, identities):
                return {}

            async def load_session_snapshot(self, session_id):
                return None

            async def save_session_snapshot(
                self, identity, session_id, generation, version,
                fencing_token, snapshot,
            ):
                saved["identity"] = identity
                saved["snapshot"] = snapshot

            async def upsert_continuity_record(self, *args):
                pass

            async def delete_continuity_record(self, *args):
                pass

        d = CallbackDispatcher()
        d.register_continuity_store(MockStore())

        await d.handle_callback(
            "callback/continuity_store/save_session_snapshot",
            {
                "identity": "a:main",
                "session_id": "s-1",
                "generation": 0,
                "version": 1,
                "fencing_token": 42,
                "snapshot": {"data": base64.b64encode(b"payload").decode()},
            },
        )
        assert saved["identity"] == "a:main"
        assert isinstance(saved["snapshot"], SessionSnapshot)
        assert saved["snapshot"].data == b"payload"

    @pytest.mark.asyncio
    async def test_upsert_continuity_record_routed(self):
        upserted = {}

        class MockStore:
            async def resolve_many(self, identities):
                return {}

            async def load_session_snapshot(self, session_id):
                return None

            async def save_session_snapshot(self, *args):
                pass

            async def upsert_continuity_record(self, record, fencing_token):
                upserted["record"] = record
                upserted["fencing_token"] = fencing_token

        d = CallbackDispatcher()
        d.register_continuity_store(MockStore())

        await d.handle_callback(
            "callback/continuity_store/upsert_continuity_record",
            {
                "record": {
                    "identity": "a:main",
                    "agent_runtime_id": "rt-1",
                    "session_id": "s-1",
                    "generation": 0,
                    "checkpoint_version": 1,
                },
                "fencing_token": 42,
            },
        )
        assert isinstance(upserted["record"], ContinuityRecord)
        assert upserted["fencing_token"] == 42

    @pytest.mark.asyncio
    async def test_delete_continuity_record_routed(self):
        deleted = {}

        class MockStore:
            async def resolve_many(self, identities):
                return {}

            async def load_session_snapshot(self, session_id):
                return None

            async def save_session_snapshot(self, *args):
                pass

            async def upsert_continuity_record(self, *args):
                pass

            async def delete_continuity_record(self, identity, fencing_token):
                deleted["identity"] = identity
                deleted["fencing_token"] = fencing_token

        d = CallbackDispatcher()
        d.register_continuity_store(MockStore())

        await d.handle_callback(
            "callback/continuity_store/delete_continuity_record",
            {"identity": "lead:main", "fencing_token": 42},
        )
        assert deleted == {"identity": "lead:main", "fencing_token": 42}

    @pytest.mark.asyncio
    async def test_delete_session_snapshot_if_current_revision_routed(self):
        deleted = {}

        class MockStore:
            async def resolve_many(self, identities):
                return {}

            async def load_session_snapshot(self, session_id):
                return None

            async def save_session_snapshot(self, *args):
                pass

            async def upsert_continuity_record(self, *args):
                pass

            async def delete_continuity_record(self, *args):
                pass

            async def delete_session_snapshot_if_current_revision(
                self, session_id, expected_current_revision
            ):
                deleted["session_id"] = session_id
                deleted["expected_current_revision"] = expected_current_revision
                return True

        d = CallbackDispatcher()
        d.register_continuity_store(MockStore())

        result = await d.handle_callback(
            "callback/continuity_store/delete_session_snapshot_if_current_revision",
            {
                "session_id": "s-1",
                "expected_current_revision": "row-sha256:abc",
            },
        )
        assert result is True
        assert deleted == {
            "session_id": "s-1",
            "expected_current_revision": "row-sha256:abc",
        }

    @pytest.mark.asyncio
    async def test_delete_session_snapshot_if_current_revision_defaults_false(self):
        class MockStore:
            async def resolve_many(self, identities):
                return {}

            async def load_session_snapshot(self, session_id):
                return None

            async def save_session_snapshot(self, *args):
                pass

            async def upsert_continuity_record(self, *args):
                pass

            async def delete_continuity_record(self, *args):
                pass

        d = CallbackDispatcher()
        d.register_continuity_store(MockStore())

        result = await d.handle_callback(
            "callback/continuity_store/delete_session_snapshot_if_current_revision",
            {
                "session_id": "s-1",
                "expected_current_revision": "row-sha256:abc",
            },
        )
        assert result is False


class TestCallbackDispatcherLeaseProvider:
    @pytest.mark.asyncio
    async def test_acquire_leases_routed(self):
        class MockLease:
            async def acquire_leases(self, identities, runtime_instance):
                return {
                    i: LeaseAcquireResult(
                        status="acquired",
                        grant=LeaseGrant(identity=i, fencing_token=1, ttl_ms=5000),
                    )
                    for i in identities
                }

            async def renew_leases(self, grants):
                return {}

            async def release_leases(self, grants):
                pass

        d = CallbackDispatcher()
        d.register_lease_provider(MockLease())

        result = await d.handle_callback("callback/lease_provider/acquire_leases", {
            "identities": ["a:main"],
            "runtime_instance": "rt-1",
        })
        assert "a:main" in result
        assert result["a:main"]["result"] == "acquired"
        assert result["a:main"]["ttl"] == 5000

    @pytest.mark.asyncio
    async def test_renew_leases_routed(self):
        class MockLease:
            async def acquire_leases(self, identities, runtime_instance):
                return {}

            async def renew_leases(self, grants):
                return {
                    g.identity: LeaseRenewResult(
                        status="renewed",
                        grant=LeaseGrant(
                            identity=g.identity,
                            fencing_token=g.fencing_token + 1,
                            ttl_ms=g.ttl_ms,
                        ),
                    )
                    for g in grants
                }

            async def release_leases(self, grants):
                pass

        d = CallbackDispatcher()
        d.register_lease_provider(MockLease())

        result = await d.handle_callback("callback/lease_provider/renew_leases", {
            "grants": [
                {"identity": "a:main", "fencing_token": 1, "ttl_ms": 5000},
            ],
        })
        assert "a:main" in result
        assert result["a:main"]["result"] == "renewed"
        assert result["a:main"]["ttl"] == 5000

    @pytest.mark.asyncio
    async def test_release_leases_routed(self):
        released = []

        class MockLease:
            async def acquire_leases(self, identities, runtime_instance):
                return {}

            async def renew_leases(self, grants):
                return {}

            async def release_leases(self, grants):
                released.extend(grants)

        d = CallbackDispatcher()
        d.register_lease_provider(MockLease())

        await d.handle_callback("callback/lease_provider/release_leases", {
            "grants": [
                {"identity": "a:main", "fencing_token": 1, "ttl_ms": 5000},
            ],
        })
        assert len(released) == 1
        assert isinstance(released[0], LeaseGrant)


class TestCallbackDispatcherRoster:
    @pytest.mark.asyncio
    async def test_roster_routed(self):
        class MockRoster:
            async def roster(self, context):
                return [
                    DurableAgentSpec(identity="a:main", profile="assistant"),
                ]

        d = CallbackDispatcher()
        d.register_roster_provider(MockRoster())

        result = await d.handle_callback("callback/roster_provider/roster", {
            "context": {},
        })
        assert len(result) == 1
        assert result[0]["identity"] == "a:main"

    @pytest.mark.asyncio
    async def test_roster_payload_carries_runtime_mode_override_and_initial_message(self):
        """The roster callback reply is what Rust deserializes into
        DurableAgentSpec; both per-identity knobs must be in it verbatim."""

        class MockRoster:
            async def roster(self, context):
                return [
                    DurableAgentSpec(
                        identity="archivist:main",
                        profile="archivist",
                        runtime_mode_override="turn_driven",
                        initial_message="Index yesterday's mail.",
                    ),
                    DurableAgentSpec(identity="worker:1", profile="worker"),
                ]

        d = CallbackDispatcher()
        d.register_roster_provider(MockRoster())

        payload = await d.handle_callback(
            "callback/roster_provider/roster", {"context": {}}
        )
        assert payload[0]["runtime_mode_override"] == "turn_driven"
        assert payload[0]["initial_message"] == "Index yesterday's mail."
        # An identity that sets neither sends neither (byte-compatible roster).
        assert "runtime_mode_override" not in payload[1]
        assert "initial_message" not in payload[1]

    @pytest.mark.asyncio
    async def test_roster_callback_receives_the_gateway_context(self):
        """Regression: the roster callback used to see an empty context.

        The gateway sends the Rust ``RosterContext`` under ``params["context"]``
        (same envelope as topology/customizer). The provider must receive the
        mob definition and previous identities from it, typed.
        """
        seen: list[RosterContext] = []

        class RecordingRoster:
            async def roster(self, context):
                seen.append(context)
                return [DurableAgentSpec(identity="a:main", profile="assistant")]

        d = CallbackDispatcher()
        d.register_roster_provider(RecordingRoster())

        await d.handle_callback("callback/roster_provider/roster", {
            "context": {
                "mob_definition": {
                    "id": "team",
                    "profiles": {"assistant": {"model": "gpt-5.5"}},
                },
                "previous_identities": ["a:main", "b:main"],
            },
        })
        assert len(seen) == 1
        context = seen[0]
        assert isinstance(context, RosterContext)
        assert context.mob_definition is not None
        assert context.mob_definition["id"] == "team"
        assert context.mob_definition["profiles"]["assistant"]["model"] == "gpt-5.5"
        assert context.previous_identities == ["a:main", "b:main"]

    @pytest.mark.asyncio
    async def test_roster_callback_tolerates_a_bare_context(self):
        """An older gateway that sends `{}` yields a typed context with defaults."""
        seen: list[RosterContext] = []

        class RecordingRoster:
            async def roster(self, context):
                seen.append(context)
                return []

        d = CallbackDispatcher()
        d.register_roster_provider(RecordingRoster())

        await d.handle_callback("callback/roster_provider/roster", {"context": {}})
        await d.handle_callback("callback/roster_provider/roster", {})
        assert [c.to_dict() for c in seen] == [
            {"mob_definition": None, "previous_identities": []},
            {"mob_definition": None, "previous_identities": []},
        ]


class TestCallbackDispatcherTopology:
    @pytest.mark.asyncio
    async def test_compute_edges_routed(self):
        class MockTopology:
            async def compute_edges(self, target_identities, context):
                return [ManagedPeerEdge(a="a:main", b="b:main")]

        d = CallbackDispatcher()
        d.register_topology_provider(MockTopology())

        result = await d.handle_callback(
            "callback/topology_provider/compute_edges",
            {
                "target_identities": ["a:main", "b:main"],
                "context": {"roster": []},
            },
        )
        assert len(result) == 1
        assert result[0] == {"a": "a:main", "b": "b:main"}


class TestCallbackDispatcherCustomizer:
    @pytest.mark.asyncio
    async def test_customize_build_routed(self):
        class MockCustomizer:
            async def customize_build(self, context, spec, draft):
                draft.model = "claude-sonnet-4-5"
                draft.system_prompt = "You are helpful."

        d = CallbackDispatcher()
        d.register_agent_customizer(MockCustomizer())

        result = await d.handle_callback(
            "callback/agent_customizer/customize_build",
            {
                "context": {
                    "identity": "a:main",
                    "active_peers": ["b:main"],
                    "managed_edges": [],
                },
                "spec": {
                    "identity": "a:main",
                    "profile": "assistant",
                },
                "draft": {
                    "model": None,
                    "system_prompt": None,
                    "additional_instructions": [],
                    "labels": {},
                    "app_context": None,
                    "external_tools": [],
                },
            },
        )
        # Returns the mutated draft
        assert result["model"] == "claude-sonnet-4-5"
        assert result["system_prompt"] == "You are helpful."

    @pytest.mark.asyncio
    async def test_after_create_routed(self):
        received = {}

        class MockCustomizer:
            async def customize_build(self, context, spec, draft):
                pass

            async def after_create(self, identity, session_id, context):
                received["identity"] = identity
                received["session_id"] = session_id

        d = CallbackDispatcher()
        d.register_agent_customizer(MockCustomizer())

        await d.handle_callback(
            "callback/agent_customizer/after_create",
            {
                "identity": "a:main",
                "session_id": "s-1",
                "context": {"model": "m1", "labels": {}, "system_prompt": None},
            },
        )
        assert received["identity"] == "a:main"
        assert received["session_id"] == "s-1"
