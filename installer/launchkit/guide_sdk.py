"""Exact reviewed Guide client wheel, independent of native runtime selection.

This temporary development wheel adds the public helper transport wait option.
It confers no application SDK, native gateway or persisted-state compatibility.
"""

import hashlib

VERSION = "0.9.2+meta.0bc6b4fa"
SOURCE = "0bc6b4fa48051353c4f63517cde6d42fccb6a21a"
TREE = "c6f97032aa934dee2077f4976199f405aacbfec9"
WHEEL = "vendor/guide/meerkat_meta-0.9.2+meta.0bc6b4fa-py3-none-any.whl"
SHA256 = "b4a3ad249ca833eb44f086676d34d9504e9f9a503261124b8f5c4025d2101918"
SIZE = 166311


def selection():
    return {"path": WHEEL, "version": VERSION, "source": SOURCE,
        "tree": TREE, "sha256": SHA256, "bytes": SIZE}


def verify(content):
    if len(content) != SIZE or hashlib.sha256(content).hexdigest() != SHA256:
        raise ValueError("The Guide client SDK differs from its reviewed wheel.")
