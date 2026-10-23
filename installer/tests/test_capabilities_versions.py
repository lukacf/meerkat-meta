"""/api/capabilities names the Forge release apart from the runtimes it builds on."""
from pathlib import Path
from types import SimpleNamespace
import tomllib

from fastapi.testclient import TestClient

from launchkit import public_meta_runtime, server
from launchkit.local_application_registry import LocalApplicationRegistry

FORGE_RELEASE = tomllib.loads((Path(server.__file__).resolve().parents[1] / "pyproject.toml").read_text())[
    "project"]["version"]


def capabilities(monkeypatch, host=None):
    monkeypatch.setattr(server, "planner_capability", lambda: {"available": False})
    monkeypatch.setattr(server, "cloud_capability", lambda: {"available": False})
    monkeypatch.setattr(server, "application_host", host)
    response = TestClient(server.app).get("/api/capabilities")  # No lifespan; no native or store access.
    assert response.status_code == 200
    return response.json()


def test_version_is_the_forge_release_and_agent_teams_name_their_public_meta_release(monkeypatch):
    value = capabilities(monkeypatch)
    assert value["version"] == FORGE_RELEASE != "0.8.47"
    assert value["agent_team_runtime"] == {"sdk": "meerkat-meta", "version": public_meta_runtime.VERSION,
                                           "release": public_meta_runtime.RELEASE_URL}
    # The developer entry has no trusted ordinary selection.
    assert value["ordinary_runtime"] is None


def test_ordinary_runtime_is_the_trusted_launchers_selected_meta_and_gateway(monkeypatch):
    host = object.__new__(LocalApplicationRegistry)
    host.runtime_target = SimpleNamespace(target="linux-x86_64",
        meta=SimpleNamespace(distribution="meerkat-meta", version="0.9.4"))
    host.ordinary_meta_gateway = SimpleNamespace(sha256="c" * 64)
    value = capabilities(monkeypatch, host)
    assert value["version"] == FORGE_RELEASE
    assert value["ordinary_runtime"] == {"sdk": "meerkat-meta", "version": "0.9.4", "target": "linux-x86_64",
                                         "gateway_sha256": "c" * 64}
    assert value["agent_team_runtime"]["version"] == public_meta_runtime.VERSION
    # A Toolkit-only selection has no ordinary gateway, and a foreign host type is not a selection.
    host.ordinary_meta_gateway = None
    assert capabilities(monkeypatch, host)["ordinary_runtime"] is None
    foreign = SimpleNamespace(runtime_target=host.runtime_target, ordinary_meta_gateway=SimpleNamespace(sha256="c" * 64))
    assert capabilities(monkeypatch, foreign)["ordinary_runtime"] is None
