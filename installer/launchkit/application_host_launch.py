"""Select an explicit trusted host in the existing server process before serving.

META_FORGE_APPLICATION_HOST_FACTORY names a synchronous zero-argument Python
factory as module:factory. It returns an original application host or an exact
qualified product runtime target, or original local and Linux control selections.
Targets enable source creation inside deliberate Build; the launcher itself
does not prepare, install or start applications.
"""
from __future__ import annotations

import importlib
import inspect
import keyword
import os
from dataclasses import dataclass


FACTORY_ENV = "META_FORGE_APPLICATION_HOST_FACTORY"


def _factory_reference(value):
    if not isinstance(value, str) or value != value.strip() or value.count(":") != 1:
        raise ValueError(f"{FACTORY_ENV} must name a trusted Python module:factory.")
    module, attribute = value.split(":")
    names = (*module.split("."), attribute)
    if any(not name.isascii() or not name.isidentifier() or keyword.iskeyword(name) for name in names):
        raise ValueError(f"{FACTORY_ENV} must name a trusted Python module:factory.")
    return module, attribute


@dataclass(frozen=True)
class ApplicationRuntimeSelection:
    """Original trusted local target plus a separately selected Linux control."""

    local_target: object
    remote_control: object

    def __post_init__(self):
        from .runtime_selection import MetaToolkitRuntimeTarget
        from .remote_product_control import ProductControlSelection
        if (type(self.local_target) is not MetaToolkitRuntimeTarget
                or self.local_target.target not in {"macos-arm64", "linux-x86_64"}
                or type(self.remote_control) is not ProductControlSelection):
            raise TypeError("Select exact local and Linux product/control owners.")


@dataclass(frozen=True)
class OrdinaryMetaApplicationSelection:
    """The trusted launcher selects an ordinary gateway and its Meta SDK pair.

    Constructing this value is not native producer qualification. It is never
    parsed from an HTTP request, blueprint, package or runtime environment path.
    The selecting owner must already have accepted the exact native producer.
    """

    runtime_target: object
    gateway: object
    model_configuration: object = None

    def __post_init__(self):
        from .connect_installer_caller import ExactFile
        from .runtime_selection import MetaToolkitRuntimeTarget
        if type(self.runtime_target) is not MetaToolkitRuntimeTarget or type(self.gateway) is not ExactFile:
            raise TypeError("Select the exact ordinary Meta gateway and original accepted SDK product.")
        if self.model_configuration is not None:
            from .connect_application_startup import _ModelConfiguration
            if type(self.model_configuration) is not _ModelConfiguration:
                raise TypeError("Select the original application model configuration owner.")
            self.model_configuration.require_environment()
            self.model_configuration.require_filesystem_current()


def create_app():
    """Uvicorn --factory entry point; configured failures stop startup."""
    module, attribute = _factory_reference(os.environ.get(FACTORY_ENV))
    factory = getattr(importlib.import_module(module), attribute)
    call = getattr(factory, "__call__", None)
    if not callable(factory) or any(
            inspect.iscoroutinefunction(value) or inspect.isasyncgenfunction(value)
            for value in (factory, call)):
        raise TypeError(f"{FACTORY_ENV} must select a synchronous zero-argument factory.")
    try:
        inspect.signature(factory).bind()
    except (TypeError, ValueError) as error:
        raise TypeError(f"{FACTORY_ENV} must select a synchronous zero-argument factory.") from error
    host_type = importlib.import_module("launchkit.forge_application_host").ForgeApplicationHost
    target_type = importlib.import_module("launchkit.runtime_selection").MetaToolkitRuntimeTarget
    selected = factory()
    if type(selected) not in (host_type, target_type, ApplicationRuntimeSelection, OrdinaryMetaApplicationSelection):
        if inspect.iscoroutine(selected):
            selected.close()
        raise TypeError("The trusted factory must return an exact ForgeApplicationHost, MetaToolkitRuntimeTarget, ApplicationRuntimeSelection or OrdinaryMetaApplicationSelection.")
    server = importlib.import_module("launchkit.server")
    if type(selected) is OrdinaryMetaApplicationSelection:
        options = {"ordinary_meta_gateway": selected.gateway}
        if selected.model_configuration is not None:
            selected.model_configuration.require_environment()
            selected.model_configuration.require_filesystem_current()
            options["model_configuration"] = selected.model_configuration
        server.select_local_application_runtime(selected.runtime_target, **options)
        if selected.model_configuration is not None:
            selected.model_configuration.require_environment()
            selected.model_configuration.require_filesystem_current()
    elif type(selected) is ApplicationRuntimeSelection:
        server.select_product_application_runtime(selected)
    elif type(selected) is target_type:
        server.select_local_application_runtime(selected)
    else:
        server.select_application_host(selected)
    return server.app
