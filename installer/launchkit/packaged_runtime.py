"""Open the exact colocated product selected by Forge's package owner.

The ordinary package launch needs no factory environment variable or SDK path
assembly. The manifest retains the existing native target serialization and
byte inventories. Configuring private model custody stays with its original
trusted execution owner and never becomes distribution payload.
"""
from __future__ import annotations

from copy import deepcopy
import importlib
from pathlib import Path, PurePosixPath


MANIFEST = "standalone-manifest.json"
FORMAT = "meta-forge-product-standalone-v1"
_private_model_route = None
_private_model_custody = None
_selected_target = None
_selected_root = None
_selected_ordinary_gateway = None
_application_model_custody = None
APPLICATION_MODEL_ROOT = ("Application model custody needs Linux and this Forge's one model credential root: its "
    "credential root must be the XDG_CONFIG_HOME this Forge runs with, the same root the Guide's model route uses.")


def configure_application_model_custody(custody):
    """Select a native owner's application model configuration for local applications.

    This is an explicit opt-in, separate from configure_private_model_route:
    the Guide's model route never supplies application credentials. Call it
    once, before the packaged runtime opens. custody is the application model
    configuration owner that the launcher selections carry; create_app passes
    it to the local application registry, which checks it again. It may be the
    same owner the Guide's route uses, or another one over the same root: a
    process has one supported model credential root, XDG_CONFIG_HOME.
    """
    global _application_model_custody
    from .connect_application_startup import _ModelConfiguration
    if _selected_target is not None:
        raise ValueError("Select application model custody before opening the packaged runtime.")
    if _application_model_custody is not None:
        raise ValueError("The original application model custody cannot be replaced.")
    if type(custody) is not _ModelConfiguration:
        raise TypeError("Select the original application model configuration owner.")
    from .connect_installer_caller import CustodyError
    try:
        custody.require_environment()
    except CustodyError as error:
        # One process has one supported model credential root. Refuse here,
        # not at a later Start.
        raise CustodyError(APPLICATION_MODEL_ROOT) from error
    custody.require_filesystem_current()
    _application_model_custody = custody


def configure_private_model_route(route, custody):
    """Keep a native owner's existing private model selection out of exports."""
    global _private_model_route, _private_model_custody
    if _selected_target is not None:
        raise ValueError("Select private model custody before opening the packaged runtime.")
    if _private_model_route is not None or _private_model_custody is not None:
        raise ValueError("The original private model selection cannot be replaced.")
    if type(route) is not dict:
        raise ValueError("Select the original trusted model route.")
    _private_model_route, _private_model_custody = deepcopy(route), custody


def _local(root, name):
    path = PurePosixPath(name)
    if (type(name) is not str or not name or path.is_absolute()
            or path.as_posix() != name or any(part in {"", ".", ".."} for part in path.parts)):
        raise ValueError("A packaged runtime locator must stay inside its distribution.")
    selected = root.joinpath(*path.parts)
    if any(parent.is_symlink() for parent in (selected, *selected.parents)):
        raise ValueError("A packaged runtime locator contains a link.")
    return selected


def relocate_selection(selection, paths, root):
    """Rebind only physical locators of the existing protected target format."""
    from .runtime_selection import PRODUCT_SELECTION_FORMAT, parse_candidate_selection

    if (type(selection) is not dict or selection.get("format") != PRODUCT_SELECTION_FORMAT
            or type(paths) is not dict):
        raise ValueError("The package needs its original selected Meta and Toolkit product.")
    value = deepcopy(selection)
    value["artifact_root"] = str(_local(root, paths["artifact_root"]))
    names = ("assembly", "meta_wheel", "toolkit_wheel", "dependency_requirements", "model_catalog")
    if "source_manifest" in value:
        names += ("source_manifest",)
    for name in names:
        value[name]["path"] = str(_local(root, paths[name]))
    dependencies = value["dependency_wheels"]
    if len(dependencies) != len(paths["dependency_wheels"]):
        raise ValueError("The packaged dependency inventory changed.")
    for item, name in zip(dependencies, paths["dependency_wheels"]):
        item["path"] = str(_local(root, name))
    return parse_candidate_selection(value)


def open_selection(root=None):
    """Verify public package bytes, then join the original native selection."""
    global _selected_target, _selected_root, _selected_ordinary_gateway
    from . import distribution
    from .bootstrap import (SelectedGuideRuntime, configure_selected_guide_runtime,
        selected_guide_runtime)
    from .connect_installer_caller import ExactFile
    from .runtime_selection import selected_runtime_artifacts

    root = Path(root or Path(__file__).resolve().parents[1]).resolve(strict=True)
    if _selected_target is not None:
        if root != _selected_root:
            raise ValueError("The original packaged runtime belongs to another distribution.")
        selected_guide_runtime().recheck()
        selected_runtime_artifacts(_selected_target)
        return _selected_target
    data = distribution.verify_product(root)
    target = relocate_selection(data["product"]["selection"], data["product"]["paths"], root)
    selected_runtime_artifacts(target)
    guide = data["guide"]
    gateway = ExactFile(_local(root, guide["gateway"]["path"]), guide["gateway"]["sha256"])
    selected = SelectedGuideRuntime(gateway=gateway, version=guide["version"],
        core_version=guide["core_version"], meta_source=guide["meta_source"],
        core_source=guide["core_source"], _model_route=_private_model_route,
        _model_custody=_private_model_custody)
    configure_selected_guide_runtime(selected)
    ordinary = data.get("ordinary")
    _selected_ordinary_gateway = (None if ordinary is None else
        ExactFile(_local(root, ordinary["gateway"]["path"]), ordinary["gateway"]["sha256"]))
    _selected_target, _selected_root = target, root
    return target


def selected_ordinary_gateway():
    """The package's Meta RPC gateway for the selected SDK, or None."""
    if _selected_target is None:
        raise ValueError("Open the packaged runtime before selecting its ordinary gateway.")
    return _selected_ordinary_gateway


def create_app():
    """Shipping Uvicorn factory entry: select before importing the server.

    A package that includes the selected Meta release's RPC gateway also
    builds ordinary applications, those without a connection setup helper.
    Application model custody, when a native owner selected it with
    configure_application_model_custody, reaches the local applications.
    """
    selected = open_selection()
    gateway = selected_ordinary_gateway()
    custody = _application_model_custody
    options = {} if gateway is None else {"ordinary_meta_gateway": gateway}
    if custody is not None:
        custody.require_environment()
        custody.require_filesystem_current()
        options["model_configuration"] = custody
    server = importlib.import_module("launchkit.server")
    server.select_local_application_runtime(selected, **options)
    if custody is not None:
        custody.require_environment()
        custody.require_filesystem_current()
    return server.app
