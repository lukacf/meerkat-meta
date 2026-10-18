import io
import zipfile

import pytest
from fastapi.testclient import TestClient

from launchkit.planner import starter_design
from launchkit.schema import DesignRequest
from launchkit.server import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def spec():
    return {"schema_version": 1, "project": {"name": "My home", "slug": "my-home", "description": "Help me organize household notes and tasks."},
            "template": "personal", "agents": starter_design(DesignRequest(description="Help me organize household notes and tasks.")).model_dump()["agents"],
            "connections": ["workspace"], "provider": "openai", "model": "gpt-5.5", "target": "mac", "storage": "sqlite",
            "gcp": {"project": "", "region": "europe-west1", "zone": "europe-west1-b", "machine_type": "e2-medium"}, "advanced": {"port": 4110}}


def test_preview_and_archive_have_identical_contents(client, spec):
    preview = client.post("/api/preview", json=spec)
    assert preview.status_code == 200, preview.text
    files = {f["path"]: f["content"] for f in preview.json()["files"]}
    assert {"app.py", "install.sh", "README.md"} <= files.keys()
    response = client.post("/api/package", json=spec)
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        assert {path.removeprefix("my-home/") for path in archive.namelist()} == files.keys()
        for path, content in files.items():
            assert archive.read(f"my-home/{path}").decode() == content
        assert archive.getinfo("my-home/install.sh").external_attr >> 16 & 0o111
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.parametrize("mutation", [
    lambda s: s["project"].update(slug="../../escape"),
    lambda s: s.update(api_key="secret-do-not-accept"),
    lambda s: s.update(connections=["slack"]),
    lambda s: s["advanced"].update(port=True),
    lambda s: s["agents"].append(s["agents"][0]),
    lambda s: s.update(target="gcp"),
    lambda s: s.update(model="gpt-5.5\n[tools]\nbuiltins=true"),
])
def test_rejects_invalid_or_unsafe_specs(client, spec, mutation):
    mutation(spec)
    assert client.post("/api/package", json=spec).status_code == 422


def test_local_api_rejects_cross_site_requests(client, spec):
    assert client.post("/api/package", json=spec, headers={"Origin": "https://unrelated.example"}).status_code == 403
    assert client.get("/api/capabilities", headers={"Host": "attacker.example"}).status_code == 400


def test_payload_limit(client):
    response = client.post("/api/design", content='{"description":"' + "x" * 100_000 + '"}', headers={"Content-Type": "application/json"})
    assert response.status_code == 413


def test_offline_designer_returns_explicit_starter(client, monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    response = client.post("/api/design", json={"description": "Help me organize household notes and tasks.", "provider": "openai", "model": "gpt-5.5"})
    assert response.status_code == 200
    assert response.json()["source"] == "template"
    assert response.json()["warnings"]


def test_provider_errors_do_not_leak_server_details(client, monkeypatch):
    async def broken(_):
        raise RuntimeError("secret-api-key-and-private-provider-payload")
    monkeypatch.setattr("launchkit.server.design_team", broken)
    response = client.post("/api/design", json={"description": "Help me organize household notes and tasks."})
    assert response.status_code == 502
    assert "secret-api-key" not in response.text
