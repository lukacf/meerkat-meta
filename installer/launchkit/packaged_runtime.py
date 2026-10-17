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
    global _selected_target, _selected_root
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
    _selected_target, _selected_root = target, root
    return target


def create_app():
    """Shipping Uvicorn factory entry: select before importing the server."""
    selected = open_selection()
    server = importlib.import_module("launchkit.server")
    server.select_local_application_runtime(selected)
    return server.app
