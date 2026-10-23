"""A macOS background login item cannot read protected folders.

launchd runs a background installation with Forge's own Python and reads
Forge's saved deployment files and the installation folder. macOS privacy
protection stops a login item from reading Desktop, Documents, Downloads,
iCloud Drive and other volumes, so such a service stalls in Python start-up
with no message. Options and submit say so before anything is installed.

These rows use a fake home and a fake launchd manager. Nothing here reads a
real protected folder or changes a privacy setting.
"""
import os
from pathlib import Path
import sys

import pytest

import launchkit.deployment_service as deployment_service
from test_deployment import Guide, Implementation, Service, submit

# Third-party cloud folders (~/Library/CloudStorage) are refused too, failing closed.
PROTECTED = ("Desktop", "Documents", "Downloads", "Library/Mobile Documents", "Library/CloudStorage")
PROTECTED_IDS = ("Desktop", "Documents", "Downloads", "iCloud-Drive", "CloudStorage")
FOLDER_REFUSAL = ("macOS does not let a background login item read Desktop, Documents, Downloads, iCloud Drive or "
    "cloud storage folders. Move the Forge folder and the installation folder to a folder such as ~/MetaForge, or "
    "choose 'While Forge is open'.")
VOLUME_REFUSAL = ("macOS does not let a background login item read external or network volumes, which may also be "
    "unavailable at login. Move the Forge folder and the installation folder to a folder such as ~/MetaForge, "
    "or choose 'While Forge is open'.")


class Manager:
    """A logged-in user's service manager that never runs anything."""

    def __init__(self, manager="launchd"):
        self.manager = manager
        self.calls = []

    async def capability(self):
        return {"available": True, "manager": self.manager, "persistence": "Runs after you log in."}

    async def observe(self, service):
        self.calls.append("observe")
        return {"loaded": False, "pid": None, "state": "stopped"}

    async def start(self, service):
        self.calls.append("start")
        return {"loaded": True, "pid": 4567, "state": "running"}

    async def stop(self, service):
        self.calls.append("stop")
        return {"loaded": False, "pid": None, "state": "stopped"}


@pytest.fixture
def home(tmp_path, monkeypatch):
    home = tmp_path / "home"
    for folder in PROTECTED:
        (home / folder).mkdir(parents=True)
    monkeypatch.setenv("HOME", str(home))
    # Another volume is a folder under this fake /Volumes; the startup disk
    # is the link macOS keeps there.
    volumes = tmp_path / "Volumes"
    (volumes / "External").mkdir(parents=True)
    (volumes / "Startup Disk").symlink_to("/")
    monkeypatch.setattr(deployment_service, "MACOS_VOLUMES", str(volumes), raising=False)
    return home


def forge(root, manager="launchd"):
    guide = Guide()
    return Service(root, guide, Implementation(guide), service_manager=Manager(manager))


def mode(options, identity):
    return next(value for value in options["execution_modes"] if value["id"] == identity)


async def read_options(service):
    try:
        return await service.options(service.guide.identity)
    finally:
        await service.shutdown()


async def refused_background(service, destination):
    try:
        with pytest.raises(ValueError) as refused:
            await submit(service, destination, execution_mode="background")
        # Nothing was admitted: no receipt, worker, service or destination.
        with pytest.raises(LookupError):
            service._directory(service.guide.identity)
        assert not service.tasks and not service.service_manager.calls
        assert not destination.exists()
    finally:
        await service.shutdown()
    return str(refused.value)


@pytest.mark.parametrize("folder", PROTECTED, ids=PROTECTED_IDS)
async def test_options_refuse_background_when_forge_python_is_in_a_protected_folder(home, monkeypatch, folder):
    monkeypatch.setattr(sys, "executable", str(home / folder / "MetaForge/.venv/bin/python"))
    options = await read_options(forge(home / "MetaForge/.studio"))
    background = mode(options, "background")
    assert background["available"] is False and background["reason"] == FOLDER_REFUSAL
    assert mode(options, "interactive")["available"] is True


@pytest.mark.parametrize("folder", PROTECTED, ids=PROTECTED_IDS)
async def test_options_refuse_background_when_forge_state_is_in_a_protected_folder(home, folder):
    options = await read_options(forge(home / folder / "MetaForge/.studio"))
    background = mode(options, "background")
    assert background["available"] is False and background["reason"] == FOLDER_REFUSAL
    assert mode(options, "interactive")["available"] is True


async def test_options_offer_background_outside_protected_folders(home):
    # A folder with a protected name deeper in another folder is ordinary.
    options = await read_options(forge(home / "MetaForge/Documents/.studio"))
    background = mode(options, "background")
    assert background["available"] is True and background["reason"] is None


async def test_options_keep_background_for_a_linux_user_service(home):
    options = await read_options(forge(home / "Documents/MetaForge/.studio", manager="systemd-user"))
    assert mode(options, "background")["available"] is True


@pytest.mark.parametrize("folder", PROTECTED, ids=PROTECTED_IDS)
@pytest.mark.parametrize("where", ["python", "state", "destination"])
async def test_background_submit_refuses_protected_folders(home, monkeypatch, folder, where):
    if where == "python":
        monkeypatch.setattr(sys, "executable", str(home / folder / "MetaForge/.venv/bin/python"))
    root = home / (folder if where == "state" else "MetaForge") / "forge/.studio"
    destination = home / (folder if where == "destination" else "Projects") / "team"
    assert await refused_background(forge(root), destination) == FOLDER_REFUSAL


async def test_background_submit_outside_protected_folders_is_admitted(home):
    service = forge(home / "MetaForge/.studio")
    try:
        receipt = await submit(service, home / "Projects/team", execution_mode="background")
        assert receipt["execution_mode"] == "background" and receipt["status"] == "queued"
    finally:
        await service.shutdown()


async def test_interactive_submit_may_use_a_protected_folder(home):
    service = forge(home / "Documents/MetaForge/.studio")
    try:
        receipt = await submit(service, home / "Documents/team")
        assert receipt["execution_mode"] == "interactive" and receipt["status"] == "queued"
    finally:
        await service.shutdown()


async def test_background_refuses_another_volume(home, tmp_path):
    volume = tmp_path / "Volumes/External"
    options = await read_options(forge(volume / "MetaForge/.studio"))
    assert mode(options, "background")["reason"] == VOLUME_REFUSAL
    assert mode(options, "background")["available"] is False
    refusal = await refused_background(forge(home / "MetaForge/.studio"), volume / "team")
    assert refusal == VOLUME_REFUSAL


async def test_background_follows_the_startup_disk_link_in_volumes(home, tmp_path):
    # The startup disk's entry in /Volumes links to /, so a path through it
    # names an ordinary folder.
    root = tmp_path / "Volumes/Startup Disk" / home.relative_to("/") / "MetaForge/.studio"
    options = await read_options(forge(root))
    assert mode(options, "background")["available"] is True


async def test_background_folder_names_ignore_case(home):
    # macOS folders are case-insensitive by default.
    options = await read_options(forge(home / "downloads/MetaForge/.studio"))
    assert mode(options, "background")["reason"] == FOLDER_REFUSAL


async def test_background_follows_links_without_looking_inside_protected_folders(home, monkeypatch):
    # ~/MetaForge links into Documents. The check reads the link, then stops
    # at Documents: looking inside it could show a privacy prompt.
    (home / "Documents/MetaForge/.studio").mkdir(parents=True)
    (home / "MetaForge").symlink_to(home / "Documents/MetaForge")
    service = forge(home / "MetaForge/.studio")
    lookups = []
    for name in ("stat", "lstat", "readlink", "scandir", "listdir"):
        original = getattr(os, name)

        def record(*args, _original=original, **kwargs):
            if args and isinstance(args[0], (str, bytes, os.PathLike)):
                lookups.append(Path(os.fsdecode(args[0])))
            return _original(*args, **kwargs)
        monkeypatch.setattr(os, name, record)
    refusal = await refused_background(service, home / "Projects/team")
    monkeypatch.undo()
    assert refusal == FOLDER_REFUSAL
    assert home / "MetaForge" in lookups, "The check reads the link where it is."
    protected = home / "Documents"
    assert not [path for path in lookups if protected in path.parents], lookups
