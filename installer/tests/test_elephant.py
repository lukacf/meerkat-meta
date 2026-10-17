import base64
from contextlib import asynccontextmanager
import hashlib
import hmac
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
from types import ModuleType, SimpleNamespace

import pytest

from launchkit.elephant_setup import ELEPHANT_REVISION, generate_elephant_files


@pytest.fixture
def manager(tmp_path, monkeypatch):
    from launchkit import elephant_setup
    spec = importlib.util.spec_from_file_location("launch_elephant_manager_test", elephant_setup.TEMPLATES / "manage.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    folder = tmp_path / "knowledge"
    folder.mkdir()
    module.ROOT = folder
    monkeypatch.setattr(module, "environment", lambda: {"ELEPHANT_TOKEN": "opaque-read-token", "ELEPHANT_OPERATOR_TOKEN": "opaque-operator-token", "ELEPHANT_SPACE_ID": "test-project"})
    return module


def config(**overrides):
    return {"mode": "local", "url": "http://127.0.0.1:8091/mcp/agent", "token_env": "ELEPHANT_TOKEN", "space_env": "ELEPHANT_SPACE_ID", **overrides}


def test_local_compose_is_private_persistent_and_bootstraps_native_schema():
    files = generate_elephant_files(config(), "test-project")
    compose = json.loads(files["knowledge/compose.json"])
    services = compose["services"]
    assert services["surrealdb"]["image"] == "surrealdb/surrealdb:v2.4.1"
    assert "ports" not in services["surrealdb"]
    assert "ports" not in services["reader"]
    assert services["elephant"]["ports"] == ["127.0.0.1:8091:3000"]
    assert services["elephant"]["environment"]["SKIP_AUTH"] == "false"
    assert "MCP_BEARER_TOKEN" not in services["elephant"]["environment"]
    assert services["elephant"]["environment"]["PIPELINE_AGENTIC_MCP_URL"] == "http://reader:3000/mcp/agent"
    assert services["migrate"]["command"] == ["elephant", "migrate"]
    assert services["reader"]["depends_on"]["migrate"]["condition"] == "service_completed_successfully"
    assert "knowledge-data" in compose["volumes"]
    settings = json.loads(files["knowledge/settings.json"])
    assert settings["source_revision"] == ELEPHANT_REVISION
    assert "rust:1.94-bookworm" in files["knowledge/Dockerfile"]
    assert "--locked" in files["knowledge/Dockerfile"]
    assert "Dockerfile.dockerignore" in " ".join(files)


def test_remote_mode_exports_verification_without_local_service():
    files = generate_elephant_files(config(mode="remote", url="https://knowledge.example.org/mcp/agent"), "test-project")
    assert "knowledge/manage.py" in files
    assert "knowledge/compose.json" not in files
    assert generate_elephant_files({"mode": "none"}, "test-project") == {}


@pytest.mark.parametrize("overrides", [
    {"url": "http://127.0.0.1:8091/mcp/full"},
    {"url": "http://0.0.0.0:8091/mcp/agent"},
    {"url": "https://remote.example.org/mcp/agent"},
    {"source_revision": "main"},
    {"source_repository": "file:///private/repo"},
    {"token_env": "same", "space_env": "same"},
    {"token_env": "ELEPHANT_TOKEN", "space_env": "ELEPHANT_TOKEN"},
])
def test_invalid_local_setup_does_not_emit_scripts(overrides):
    with pytest.raises(ValueError):
        generate_elephant_files(config(**overrides), "test-project")


def test_token_is_signed_scoped_and_read_only(manager):
    token = manager.jwt_token("secret", "test-project", now=1000)
    header, body, signature = token.split(".")
    decode = lambda value: base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    claims = json.loads(decode(body))
    assert claims["space_id"] == "test-project"
    assert claims["aud"] == ["test-project"]
    assert claims["exp"] == 1000 + 90 * 86400
    assert "*" not in claims["scopes"]
    assert all(scope.startswith("read:") for scope in claims["scopes"])
    assert hmac.compare_digest(decode(signature), hmac.new(b"secret", f"{header}.{body}".encode(), hashlib.sha256).digest())


def test_setup_keeps_operator_credentials_out_of_project_env(manager, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-openai-value")
    settings = {**config(), "provider": "openai", "project": "test-project"}
    manager.setup(settings)
    local = manager.read_env(manager.ROOT / ".env")
    project = manager.read_env(manager.ROOT.parent / ".env")
    assert set(project) == {"ELEPHANT_TOKEN", "ELEPHANT_SPACE_ID"}
    assert "ELEPHANT_OPERATOR_TOKEN" in local
    assert local["ELEPHANT_OPERATOR_TOKEN"] != project["ELEPHANT_TOKEN"]
    assert (manager.ROOT / ".env").stat().st_mode & 0o777 == 0o600
    assert (manager.ROOT.parent / ".env").stat().st_mode & 0o777 == 0o600
    old_secret = local["JWT_SECRET"]
    manager.setup(settings, renew=True)
    assert manager.read_env(manager.ROOT / ".env")["JWT_SECRET"] == old_secret


def test_bad_env_value_does_not_truncate_existing_configuration(manager):
    path = manager.ROOT / ".env"
    path.write_text("PRESERVE='yes'\n")
    with pytest.raises(ValueError):
        manager.write_env(path, {"VALUE": "broken\nvalue"})
    assert path.read_text() == "PRESERVE='yes'\n"


@pytest.mark.parametrize("configured", [False, True])
def test_up_checks_local_prerequisites_before_credentials_and_source(manager, monkeypatch, configured):
    settings = {**config(), "provider": "openai", "project": "test-project"}
    monkeypatch.setattr(manager, "load_settings", lambda: settings)
    monkeypatch.setattr(manager.shutil, "which", lambda _: None)
    calls = []
    monkeypatch.setattr(manager.getpass, "getpass", lambda _: calls.append("credential-input"))
    monkeypatch.setattr(manager, "source_checkout", lambda _: calls.append("source-checkout"))
    monkeypatch.setattr(manager, "compose", lambda *_: calls.append("compose"))
    if configured:
        (manager.ROOT / ".env").write_text("PRIVATE='preserve-knowledge-value'\n")
        (manager.ROOT.parent / ".env").write_text("PRIVATE='preserve-project-value'\n")
    before = {path: path.read_bytes() for path in manager.ROOT.parent.rglob("*") if path.is_file()}
    monkeypatch.setattr(sys, "argv", ["manage.py", "up"])
    with pytest.raises(ValueError, match="Install Docker with Compose"):
        manager.main()
    assert calls == []
    assert {path: path.read_bytes() for path in manager.ROOT.parent.rglob("*") if path.is_file()} == before
    assert not (manager.ROOT / ".source").exists()


def test_unreachable_engine_stops_up_without_disclosing_or_replacing_private_settings(manager, monkeypatch):
    monkeypatch.setattr(manager, "load_settings", lambda: {**config(), "provider": "openai", "project": "test-project"})
    monkeypatch.setattr(manager.shutil, "which", lambda _: "/trusted/docker")
    paths = [manager.ROOT / ".env", manager.ROOT.parent / ".env"]
    for path in paths:
        path.write_text("PRIVATE='https://user:secret@private.invalid/token'\n")
    before = [path.read_bytes() for path in paths]
    calls = []
    def probe(args, failure):
        calls.append(args)
        if args[1] == "compose":
            return "  --wait Wait for running services\n  --wait-timeout int Maximum wait\n"
        raise ValueError(failure)
    monkeypatch.setattr(manager, "_docker_probe", probe)
    monkeypatch.setattr(manager, "source_checkout", lambda _: pytest.fail("source was accessed before engine readiness"))
    monkeypatch.setattr(manager, "setup", lambda *_: pytest.fail("credentials were accessed before engine readiness"))
    monkeypatch.setattr(sys, "argv", ["manage.py", "up"])
    with pytest.raises(ValueError, match="Docker engine is unreachable") as error:
        manager.main()
    assert "secret" not in str(error.value)
    assert "private.invalid" not in str(error.value)
    assert calls[-1] == ["docker", "info", "--format", "{{.OSType}}"]
    assert [path.read_bytes() for path in paths] == before


@pytest.mark.parametrize("help_text", ["  --wait-timeout int Wait duration\n", "  --wait Wait\n", "unavailable --wait and --wait-timeout"])
def test_compose_requires_actual_wait_options_before_engine_access(manager, monkeypatch, help_text):
    monkeypatch.setattr(manager.shutil, "which", lambda _: "/trusted/docker")
    calls = []
    monkeypatch.setattr(manager, "_docker_probe", lambda args, _: calls.append(args) or help_text)
    with pytest.raises(ValueError, match="needs --wait and --wait-timeout support"):
        manager.local_preflight()
    assert calls == [["docker", "compose", "up", "--help"]]


@pytest.mark.parametrize("engine", ["windows\n", "\n", "linux\nprivate-detail\n"])
def test_local_preflight_requires_linux_without_echoing_engine_output(manager, monkeypatch, engine):
    monkeypatch.setattr(manager.shutil, "which", lambda _: "/trusted/docker")
    monkeypatch.setattr(manager, "_docker_probe", lambda args, _: "  --wait Wait\n  --wait-timeout int Timeout\n" if args[1] == "compose" else engine)
    with pytest.raises(ValueError, match="Linux Docker engine") as error:
        manager.local_preflight()
    assert "private-detail" not in str(error.value)


def test_local_preflight_does_not_claim_stack_or_mcp_readiness(manager, monkeypatch):
    monkeypatch.setattr(manager.shutil, "which", lambda _: "/trusted/docker")
    calls = []
    def probe(args, _):
        calls.append(args)
        return "  --wait Wait\n  --wait-timeout int Timeout\n" if args[1] == "compose" else "linux\n"
    monkeypatch.setattr(manager, "_docker_probe", probe)
    assert manager.local_preflight() is None
    assert calls == [["docker", "compose", "up", "--help"], ["docker", "info", "--format", "{{.OSType}}"]]
    assert list(manager.ROOT.iterdir()) == []


def test_offline_setup_remains_usable_without_docker(manager, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-offline-provider-key")
    monkeypatch.setattr(manager, "load_settings", lambda: {**config(), "provider": "openai", "project": "test-project"})
    monkeypatch.setattr(manager.shutil, "which", lambda _: pytest.fail("offline setup must not inspect Docker"))
    monkeypatch.setattr(sys, "argv", ["manage.py", "setup"])
    manager.main()
    assert set(manager.read_env(manager.ROOT.parent / ".env")) == {"ELEPHANT_TOKEN", "ELEPHANT_SPACE_ID"}
    assert not (manager.ROOT / ".source").exists()


@pytest.mark.parametrize("program", [
    "import sys; print('https://user:secret@private.invalid/token'); sys.exit(1)",
    "import sys; sys.stdout.buffer.write(b'private-secret' * 10000)",
    "import sys; sys.stdout.buffer.write(b'\\xff')",
])
def test_docker_probe_has_bounded_safe_failure_without_raw_output(manager, program):
    message = "Docker prerequisite unavailable."
    with pytest.raises(ValueError) as error:
        manager._docker_probe([sys.executable, "-c", program], message)
    assert str(error.value) == message


def test_docker_probe_timeout_stops_and_reaps_owned_process(manager, monkeypatch):
    original = manager.subprocess.Popen
    children = []
    def start(*args, **kwargs):
        process = original(*args, **kwargs)
        children.append(process)
        return process
    monkeypatch.setattr(manager.subprocess, "Popen", start)
    monkeypatch.setattr(manager, "PREREQUISITE_TIMEOUT", 0.05)
    started = time.monotonic()
    with pytest.raises(ValueError, match="Docker prerequisite unavailable"):
        manager._docker_probe([sys.executable, "-c", "import time; time.sleep(30)"], "Docker prerequisite unavailable.")
    assert time.monotonic() - started < 3
    assert len(children) == 1
    assert children[0].poll() is not None
    assert children[0].stdout.closed


def test_wrong_jwt_space_is_rejected_before_network(manager):
    token = manager.jwt_token("secret", "other-project")
    with pytest.raises(ValueError, match="space"):
        manager.check_space(token, "test-project")


def mock_mcp(monkeypatch, calls, *, ready=True):
    @asynccontextmanager
    async def transport(url, headers):
        calls.append(("connect", url, headers))
        yield None, None, None
    class Session:
        def __init__(self, *_):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_):
            pass
        async def initialize(self):
            calls.append(("initialize",))
        async def list_tools(self):
            names = ["get_runtime_status", "search_all", "ingest_doc_simple"]
            if calls[0][1].endswith("/agent"):
                names.append("get_capabilities")
            return SimpleNamespace(tools=[SimpleNamespace(name=name, inputSchema={"properties": {"title": {}, "text": {}, "metadata": {}, "query": {}}}) for name in names])
        async def call_tool(self, name, args):
            calls.append((name, args))
            data = {"semantic_ingest_ready": ready} if name == "get_runtime_status" else {"accepted": True}
            return SimpleNamespace(isError=False, structuredContent={"data": data})
    module = ModuleType("mcp")
    module.ClientSession = Session
    transport_module = ModuleType("mcp.client.streamable_http")
    transport_module.streamablehttp_client = transport
    monkeypatch.setitem(sys.modules, "mcp", module)
    monkeypatch.setitem(sys.modules, "mcp.client", ModuleType("mcp.client"))
    monkeypatch.setitem(sys.modules, "mcp.client.streamable_http", transport_module)


@pytest.mark.asyncio
async def test_ingestion_waits_for_readiness_and_uses_operator_token(manager, monkeypatch):
    calls = []
    mock_mcp(monkeypatch, calls, ready=False)
    settings = {**config(), "project": "test-project"}
    with pytest.raises(ValueError, match="not ready"):
        await manager.mcp_operation(settings, "ingest", path="irrelevant.txt")
    assert calls[0] == ("connect", "http://127.0.0.1:8091/mcp", {"Authorization": "Bearer opaque-operator-token"})
    assert not any(call[0] == "ingest_doc_simple" for call in calls)


@pytest.mark.asyncio
async def test_ingestion_records_source_hash_only_after_schema_and_health(manager, monkeypatch):
    calls = []
    mock_mcp(monkeypatch, calls)
    source = manager.ROOT / "brief.txt"
    source.write_text("Project evidence")
    settings = {**config(), "project": "test-project"}
    result = await manager.mcp_operation(settings, "ingest", path=source)
    name, arguments = calls[-1]
    assert name == "ingest_doc_simple"
    assert arguments["metadata"]["source_sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert arguments["metadata"]["original_filename"] == "brief.txt"
    assert result["receipt"] == {"data": {"accepted": True}}
    assert [call[0] for call in calls][2:3] == ["get_runtime_status"]


@pytest.mark.asyncio
async def test_search_uses_read_only_profile_and_credential(manager, monkeypatch):
    calls = []
    mock_mcp(monkeypatch, calls)
    await manager.mcp_operation({**config(), "project": "test-project"}, "search", query="evidence")
    assert calls[0] == ("connect", "http://127.0.0.1:8091/mcp/agent", {"Authorization": "Bearer opaque-read-token"})
    assert calls[-1] == ("search_all", {"query": "evidence"})


@pytest.mark.parametrize("structured", [True, False])
def test_native_tool_error_cannot_be_reported_as_success(manager, structured):
    error = {"ok": False, "error": "Access denied"}
    result = SimpleNamespace(isError=False, structuredContent=error if structured else None, content=[SimpleNamespace(type="text", text=json.dumps(error))])
    with pytest.raises(ValueError, match="denied"):
        manager.result_data(result)


@pytest.mark.parametrize("field,name", [("token_env", "OPENAI_API_KEY"), ("space_env", "JWT_SECRET"), ("token_env", "ELEPHANT_OPERATOR_TOKEN"), ("space_env", "PATH")])
def test_knowledge_cannot_overwrite_provider_or_runtime_environment(field, name):
    from launchkit.blueprint import Knowledge
    with pytest.raises(ValueError, match="dedicated Elephant"):
        Knowledge(mode="local", **{field: name})
    with pytest.raises(ValueError, match="dedicated Elephant"):
        generate_elephant_files({"mode": "local", field: name}, "research-lab")
