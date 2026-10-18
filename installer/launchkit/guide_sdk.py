"""Exact reviewed Guide client wheel, independent of native runtime selection.

Packaged products carry the published Meta 0.9.3 Python SDK wheel, which
includes the public helper transport wait option. Its public bytes do not exist
before that release publishes, so the pin below is pending and every use of it
refuses. Fill the pending fields together from the published wheel and its
exact release commit. The wheel confers no application SDK, native gateway or
persisted-state compatibility.
"""

import hashlib
import re

VERSION = "0.9.3"
WHEEL = "vendor/guide/meerkat_meta-0.9.3-py3-none-any.whl"
# Pending until Meta 0.9.3 publishes: the release commit, its sdk/python tree,
# and the published wheel's SHA-256 and byte size.
SOURCE = None
TREE = None
SHA256 = None
SIZE = None


def require_pinned():
    if not (type(SOURCE) is str and re.fullmatch("[0-9a-f]{40}", SOURCE)
            and type(TREE) is str and re.fullmatch("[0-9a-f]{40}", TREE)
            and type(SHA256) is str and re.fullmatch("[0-9a-f]{64}", SHA256)
            and type(SIZE) is int and SIZE > 0):
        raise ValueError("The Guide client SDK pin is pending the published Meta 0.9.3 wheel.")


def selection():
    require_pinned()
    return {"path": WHEEL, "version": VERSION, "source": SOURCE,
        "tree": TREE, "sha256": SHA256, "bytes": SIZE}


def verify(content):
    require_pinned()
    if len(content) != SIZE or hashlib.sha256(content).hexdigest() != SHA256:
        raise ValueError("The Guide client SDK differs from its reviewed wheel.")
