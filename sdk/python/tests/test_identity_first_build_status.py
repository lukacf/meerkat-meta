"""TDD tests for AgentBuildContext/Draft/ToolDef (REQ-43a) and IdentityStatus/Health (REQ-43b)."""
import pytest

from meerkat_meta.identity_first_models import (
    AgentBuildContext,
    AgentBuildDraft,
    ContinuityHealth,
    DurabilityPolicy,
    ExternalToolDef,
    IdentityStatus,
    LeaseInfo,
    ManagedPeerEdge,
)


# ---------------------------------------------------------------------------
# REQ-43a: AgentBuildContext, AgentBuildDraft, ExternalToolDef
# ---------------------------------------------------------------------------


class TestExternalToolDef:
    def test_fields(self):
        t = ExternalToolDef(
            name="search", description="Search the web",
            input_schema={"type": "object", "properties": {"q": {"type": "string"}}},
        )
        assert t.name == "search"
        assert t.description == "Search the web"
        assert "properties" in t.input_schema

    def test_to_dict(self):
        t = ExternalToolDef(name="calc", description="Calculator", input_schema={})
        d = t.to_dict()
        assert d == {"name": "calc", "description": "Calculator", "input_schema": {}}

    def test_from_dict(self):
        t = ExternalToolDef.from_dict({
            "name": "fetch", "description": "Fetch URL",
            "input_schema": {"type": "object"},
        })
        assert t.name == "fetch"

    def test_round_trip(self):
        original = ExternalToolDef(
            name="tool1", description="desc",
            input_schema={"type": "object"},
        )
        restored = ExternalToolDef.from_dict(original.to_dict())
        assert restored.name == original.name
        assert restored.input_schema == original.input_schema


class TestAgentBuildContext:
    """AgentBuildContext is frozen/read-only."""

    def test_fields(self):
        ctx = AgentBuildContext(
            identity="triage:main",
            active_peers=["gate:main", "worker:main"],
            managed_edges=[ManagedPeerEdge(a="gate:main", b="triage:main")],
        )
        assert ctx.identity == "triage:main"
        assert len(ctx.active_peers) == 2
        assert len(ctx.managed_edges) == 1

    def test_frozen(self):
        ctx = AgentBuildContext(
            identity="a:main", active_peers=[], managed_edges=[],
        )
        with pytest.raises(AttributeError):
            ctx.identity = "b:main"

    def test_from_dict(self):
        ctx = AgentBuildContext.from_dict({
            "identity": "triage:main",
            "active_peers": ["gate:main"],
            "managed_edges": [{"a": "gate:main", "b": "triage:main"}],
        })
        assert ctx.identity == "triage:main"
        assert len(ctx.managed_edges) == 1
        assert isinstance(ctx.managed_edges[0], ManagedPeerEdge)

    def test_to_dict(self):
        ctx = AgentBuildContext(
            identity="a:main",
            active_peers=["b:main"],
            managed_edges=[ManagedPeerEdge(a="a:main", b="b:main")],
        )
        d = ctx.to_dict()
        assert d["identity"] == "a:main"
        assert d["active_peers"] == ["b:main"]
        assert d["managed_edges"] == [{"a": "a:main", "b": "b:main"}]

    def test_round_trip(self):
        original = AgentBuildContext(
            identity="x:1",
            active_peers=["y:1", "z:1"],
            managed_edges=[ManagedPeerEdge(a="x:1", b="y:1")],
        )
        restored = AgentBuildContext.from_dict(original.to_dict())
        assert restored.identity == original.identity
        assert restored.active_peers == original.active_peers


class TestAgentBuildDraft:
    """AgentBuildDraft is mutable."""

    def test_defaults(self):
        draft = AgentBuildDraft()
        assert draft.model is None
        assert draft.system_prompt is None
        assert draft.additional_instructions == []
        assert draft.labels == {}
        assert draft.app_context is None
        assert draft.external_tools == []

    def test_mutable(self):
        draft = AgentBuildDraft()
        draft.model = "claude-sonnet-4-5"
        draft.system_prompt = "You are helpful."
        draft.additional_instructions.append("Be concise.")
        draft.external_tools.append(
            ExternalToolDef(name="t", description="d", input_schema={})
        )
        assert draft.model == "claude-sonnet-4-5"
        assert len(draft.additional_instructions) == 1
        assert len(draft.external_tools) == 1

    def test_to_dict(self):
        draft = AgentBuildDraft(
            model="m1",
            system_prompt="prompt",
            additional_instructions=["inst"],
            labels={"k": "v"},
            app_context={"data": 1},
            external_tools=[
                ExternalToolDef(name="t1", description="d1", input_schema={}),
            ],
        )
        d = draft.to_dict()
        assert d["model"] == "m1"
        assert d["system_prompt"] == "prompt"
        assert d["additional_instructions"] == ["inst"]
        assert d["labels"] == {"k": "v"}
        assert d["app_context"] == {"data": 1}
        assert len(d["external_tools"]) == 1
        assert d["external_tools"][0]["name"] == "t1"

    def test_from_dict(self):
        draft = AgentBuildDraft.from_dict({
            "model": "m2",
            "system_prompt": "sp",
            "additional_instructions": ["a"],
            "labels": {"x": "y"},
            "app_context": None,
            "external_tools": [
                {"name": "t", "description": "d", "input_schema": {}},
            ],
        })
        assert draft.model == "m2"
        assert len(draft.external_tools) == 1

    def test_round_trip(self):
        original = AgentBuildDraft(
            model="m", system_prompt="p",
            additional_instructions=["i1", "i2"],
            labels={"a": "b"},
            app_context={"nested": True},
            external_tools=[
                ExternalToolDef(name="x", description="y", input_schema={"k": 1}),
            ],
        )
        restored = AgentBuildDraft.from_dict(original.to_dict())
        assert restored.model == original.model
        assert restored.system_prompt == original.system_prompt
        assert restored.labels == original.labels
        assert len(restored.external_tools) == 1


# ---------------------------------------------------------------------------
# REQ-43b: IdentityStatus, LeaseInfo, ContinuityHealth, DurabilityPolicy
# ---------------------------------------------------------------------------


class TestDurabilityPolicy:
    def test_sync_write_through(self):
        p = DurabilityPolicy(kind="sync_write_through")
        assert p.kind == "sync_write_through"
        assert p.max_loss_window_ms is None

    def test_buffered_export(self):
        p = DurabilityPolicy(kind="buffered_export", max_loss_window_ms=5000)
        assert p.max_loss_window_ms == 5000

    def test_to_dict_without_max_loss(self):
        d = DurabilityPolicy(kind="async_replicated").to_dict()
        assert d == {"kind": "async_replicated"}

    def test_to_dict_with_max_loss(self):
        d = DurabilityPolicy(kind="buffered_export", max_loss_window_ms=1000).to_dict()
        assert d == {"kind": "buffered_export", "max_loss_window_ms": 1000}

    def test_from_dict(self):
        p = DurabilityPolicy.from_dict({"kind": "sync_write_through"})
        assert p.kind == "sync_write_through"

    def test_round_trip(self):
        original = DurabilityPolicy(kind="buffered_export", max_loss_window_ms=3000)
        restored = DurabilityPolicy.from_dict(original.to_dict())
        assert restored == original


class TestLeaseInfo:
    def test_fields(self):
        li = LeaseInfo(fencing_token=42, ttl_remaining_ms=30000, healthy=True)
        assert li.fencing_token == 42
        assert li.ttl_remaining_ms == 30000
        assert li.healthy is True

    def test_to_dict(self):
        d = LeaseInfo(fencing_token=1, ttl_remaining_ms=5000, healthy=False).to_dict()
        assert d == {"fencing_token": 1, "ttl_remaining_ms": 5000, "healthy": False}

    def test_from_dict(self):
        li = LeaseInfo.from_dict({
            "fencing_token": 99, "ttl_remaining_ms": 10000, "healthy": True,
        })
        assert li.fencing_token == 99

    def test_round_trip(self):
        original = LeaseInfo(fencing_token=7, ttl_remaining_ms=60000, healthy=True)
        restored = LeaseInfo.from_dict(original.to_dict())
        assert restored == original


class TestContinuityHealth:
    def test_fields(self):
        ch = ContinuityHealth(
            store_reachable=True,
            durability_policy=DurabilityPolicy(kind="sync_write_through"),
            last_checkpoint_version=5,
        )
        assert ch.store_reachable is True
        assert ch.last_checkpoint_version == 5

    def test_to_dict(self):
        ch = ContinuityHealth(
            store_reachable=False,
            durability_policy=DurabilityPolicy(kind="async_replicated"),
        )
        d = ch.to_dict()
        assert d["store_reachable"] is False
        assert d["durability_policy"] == {"kind": "async_replicated"}
        assert "last_checkpoint_version" not in d

    def test_from_dict(self):
        ch = ContinuityHealth.from_dict({
            "store_reachable": True,
            "durability_policy": {"kind": "sync_write_through"},
            "last_checkpoint_version": 10,
        })
        assert ch.last_checkpoint_version == 10
        assert ch.durability_policy.kind == "sync_write_through"

    def test_round_trip(self):
        original = ContinuityHealth(
            store_reachable=True,
            durability_policy=DurabilityPolicy(
                kind="buffered_export", max_loss_window_ms=2000,
            ),
            last_checkpoint_version=3,
        )
        restored = ContinuityHealth.from_dict(original.to_dict())
        assert restored == original


class TestIdentityStatus:
    def test_full_status(self):
        status = IdentityStatus(
            identity="triage:main",
            state="active",
            agent_runtime_id="rt-1",
            session_id="s-1",
            profile="assistant",
            addressability="addressable",
            display_name="Triage",
            labels={"env": "prod"},
            generation=0,
            checkpoint_version=3,
            lease=LeaseInfo(fencing_token=42, ttl_remaining_ms=30000, healthy=True),
            continuity_health=ContinuityHealth(
                store_reachable=True,
                durability_policy=DurabilityPolicy(kind="sync_write_through"),
                last_checkpoint_version=3,
            ),
        )
        assert status.identity == "triage:main"
        assert status.labels == {"env": "prod"}
        assert status.lease.fencing_token == 42
        assert status.continuity_health.store_reachable is True

    def test_minimal_status(self):
        status = IdentityStatus(identity="a:main", state="uninitialized")
        assert status.agent_runtime_id is None
        assert status.lease is None
        assert status.continuity_health is None

    def test_from_dict_full(self):
        status = IdentityStatus.from_dict({
            "identity": "triage:main",
            "state": "active",
            "agent_runtime_id": "rt-1",
            "session_id": "s-1",
            "profile": "assistant",
            "addressability": "internal_only",
            "display_name": "Triage",
            "labels": {"env": "prod"},
            "generation": 2,
            "checkpoint_version": 5,
            "lease": {
                "fencing_token": 10,
                "ttl_remaining_ms": 15000,
                "healthy": True,
            },
            "continuity_health": {
                "store_reachable": True,
                "durability_policy": {
                    "kind": "buffered_export",
                    "max_loss_window_ms": 5000,
                },
                "last_checkpoint_version": 5,
            },
        })
        assert status.identity == "triage:main"
        assert status.addressability == "internal_only"
        assert status.lease.fencing_token == 10
        assert status.continuity_health.durability_policy.kind == "buffered_export"
        assert status.continuity_health.durability_policy.max_loss_window_ms == 5000

    def test_from_dict_minimal(self):
        status = IdentityStatus.from_dict({
            "identity": "a:main",
            "state": "uninitialized",
        })
        assert status.identity == "a:main"
        assert status.lease is None

    def test_round_trip(self):
        original = IdentityStatus(
            identity="x:1",
            state="active",
            agent_runtime_id="rt-x",
            session_id="s-x",
            profile="worker",
            labels={"role": "compute"},
            generation=1,
            checkpoint_version=10,
            lease=LeaseInfo(fencing_token=5, ttl_remaining_ms=20000, healthy=True),
            continuity_health=ContinuityHealth(
                store_reachable=True,
                durability_policy=DurabilityPolicy(kind="sync_write_through"),
                last_checkpoint_version=10,
            ),
        )
        restored = IdentityStatus.from_dict(original.to_dict())
        assert restored.identity == original.identity
        assert restored.state == original.state
        assert restored.labels == original.labels
        assert restored.lease.fencing_token == original.lease.fencing_token
        assert restored.continuity_health.durability_policy.kind == "sync_write_through"
