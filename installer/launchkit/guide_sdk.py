"""The Guide's one exact selected Meta release and its reviewed client wheel.

SELECTED_VERSION names the single Meta release the Guide runs: its native
gateway, that gateway's Core release (Meta and Core release in lockstep) and
the client wheel below. The runtime admission, the product verifier and this
pin compare against it with exact equality; no other version is accepted.

Packaged products carry the published client wheel of that release, which
includes the public helper transport wait option. Its public bytes do not
exist before the release publishes, so the pin below is pending and every use
of it refuses. Fill the pending fields together from the published wheel and
its exact release commit. The wheel confers no application SDK, native gateway
or persisted-state compatibility.
"""

import hashlib
import re

SELECTED_VERSION = "0.9.3"
WHEEL = f"vendor/guide/meerkat_meta-{SELECTED_VERSION}-py3-none-any.whl"
# Pending until the selected release publishes: its release commit, that
# commit's sdk/python tree, and the published wheel's SHA-256 and byte size.
SOURCE = None
TREE = None
SHA256 = None
SIZE = None


def require_pinned():
    if not (type(SOURCE) is str and re.fullmatch("[0-9a-f]{40}", SOURCE)
            and type(TREE) is str and re.fullmatch("[0-9a-f]{40}", TREE)
            and type(SHA256) is str and re.fullmatch("[0-9a-f]{64}", SHA256)
            and type(SIZE) is int and SIZE > 0):
        raise ValueError(
            f"The Guide client SDK pin is pending the published Meta {SELECTED_VERSION} wheel.")


def selection():
    require_pinned()
    return {"path": WHEEL, "version": SELECTED_VERSION, "source": SOURCE,
        "tree": TREE, "sha256": SHA256, "bytes": SIZE}


def verify(content):
    require_pinned()
    if len(content) != SIZE or hashlib.sha256(content).hexdigest() != SHA256:
        raise ValueError("The Guide client SDK differs from its reviewed wheel.")
