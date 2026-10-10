"""Historical ownership callbacks must preserve the exact session boundary."""

import pytest

from meerkat_mobkit.agent_builder import CallbackDispatcher
from meerkat_mobkit.identity_first_providers import ContinuityRecord, SessionOwnershipProvider


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["triage:main", None])
async def test_session_owner_uses_historical_provider(owner):
    class Store:
        async def session_owner(self, session_id):
            assert session_id == "old-session"
            return owner

        async def resolve_record_by_session(self, session_id):
            raise AssertionError("historical owner must not depend on current binding")

    dispatcher = CallbackDispatcher()
    dispatcher.register_continuity_store(Store())
    assert await dispatcher.handle_callback(
        "callback/continuity_store/session_owner", {"session_id": "old-session"}
    ) == owner


@pytest.mark.asyncio
@pytest.mark.parametrize("returned_session", ["current-session", "different-session", None])
async def test_session_owner_current_provider_fallback_checks_exact_session(returned_session):
    class Store:
        async def resolve_record_by_session(self, session_id):
            if returned_session is None:
                return None
            return ContinuityRecord("triage:main", "rt-1", returned_session, 0, 1), 1, 1

    dispatcher = CallbackDispatcher()
    dispatcher.register_continuity_store(Store())
    call = dispatcher.handle_callback(
        "callback/continuity_store/session_owner", {"session_id": "current-session"}
    )
    if returned_session == "different-session":
        with pytest.raises(ValueError, match="different session"):
            await call
    else:
        assert await call == ("triage:main" if returned_session else None)


@pytest.mark.asyncio
async def test_session_owner_refuses_unsupported_or_malformed_provider():
    dispatcher = CallbackDispatcher()
    dispatcher.register_continuity_store(object())
    with pytest.raises(ValueError, match="cannot resolve session ownership"):
        await dispatcher.handle_callback(
            "callback/continuity_store/session_owner", {"session_id": "old-session"}
        )

    class Store:
        async def session_owner(self, session_id):
            return {"identity": "triage:main"}

    dispatcher.register_continuity_store(Store())
    with pytest.raises(ValueError, match="identity string"):
        await dispatcher.handle_callback(
            "callback/continuity_store/session_owner", {"session_id": "old-session"}
        )


def test_historical_owner_capability_is_optional_and_explicit():
    class Store:
        async def session_owner(self, session_id):
            return None

    assert isinstance(Store(), SessionOwnershipProvider)
    assert not isinstance(object(), SessionOwnershipProvider)
