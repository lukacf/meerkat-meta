#!/usr/bin/env python3
"""Regression tests for the Meta release-asset manifest contract."""

import importlib.util
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("verify-release-assets.py")
SPEC = importlib.util.spec_from_file_location("verify_release_assets", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
release_assets = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_assets)


class ReleaseAssetContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "downloaded"
        self.source.mkdir()
        self.output = self.root / "release-assets"

    def tearDown(self):
        self.temp.cleanup()

    def write_complete_source(self):
        for index, name in enumerate(release_assets.expected_archive_names("0.8.0")):
            artifact_dir = self.source / f"matrix-{index}"
            artifact_dir.mkdir()
            (artifact_dir / name).write_bytes(f"artifact-{index}".encode())

    def test_expected_inventory_includes_the_repair_binary_on_all_targets(self):
        # Release-blocker regression (v0.8.14 attempt 1): meta-repair
        # archives were built but rejected as unexpected because the
        # inventory constant was never taught about the new binary. The
        # repair tool ships as a release asset by explicit consumer
        # requirement - building a maintenance tool from source inside a
        # repair window is how deployment defects are born.
        names = release_assets.expected_archive_names("0.8.0")
        for target, ext in release_assets.TARGET_ARCHIVES:
            self.assertIn(f"meta-repair-0.8.0-{target}.{ext}", names)

    def test_prepare_emits_exact_flat_manifest_and_verified_checksums(self):
        self.write_complete_source()

        release_assets.prepare_assets(self.source, self.output, "v0.8.0")

        release_assets.verify_assets(self.output, "v0.8.0")
        self.assertEqual(
            sorted(path.name for path in self.output.iterdir()),
            sorted(
                release_assets.expected_archive_names("0.8.0")
                + [release_assets.CHECKSUMS_NAME, release_assets.MANIFEST_NAME]
            ),
        )
        self.assertTrue(all(path.parent == self.output for path in self.output.iterdir()))

    def test_prepare_rejects_missing_matrix_archive(self):
        self.write_complete_source()
        next(self.source.rglob("*.zip")).unlink()

        with self.assertRaisesRegex(release_assets.ReleaseAssetError, "missing"):
            release_assets.prepare_assets(self.source, self.output, "v0.8.0")

    def test_prepare_rejects_duplicate_basename_before_flattening(self):
        self.write_complete_source()
        archive = next(self.source.rglob("*.tar.gz"))
        duplicate_dir = self.source / "duplicate"
        duplicate_dir.mkdir()
        (duplicate_dir / archive.name).write_bytes(b"different")

        with self.assertRaisesRegex(release_assets.ReleaseAssetError, "duplicate"):
            release_assets.prepare_assets(self.source, self.output, "v0.8.0")

    def test_verify_rejects_tampered_archive(self):
        self.write_complete_source()
        release_assets.prepare_assets(self.source, self.output, "v0.8.0")
        next(self.output.glob("*.tar.gz")).write_bytes(b"tampered")

        with self.assertRaisesRegex(release_assets.ReleaseAssetError, "checksum mismatch"):
            release_assets.verify_assets(self.output, "v0.8.0")


if __name__ == "__main__":
    unittest.main()
