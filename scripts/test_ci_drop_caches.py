#!/usr/bin/env python3
"""Contracts for scripts/ci-drop-superseded-caches.sh, run against a stub `gh`."""

import json
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/ci-drop-superseded-caches.sh"
STARTED = "2026-10-14T12:00:00Z"

STUB = """#!/usr/bin/env bash
printf '%s\\n' "$*" >> "$GH_STUB_LOG"
case "$1 $2" in
  "api repos/owner/repo/actions/runs/42") [ -n "${GH_STUB_API_FAIL:-}" ] && exit 1; printf '%s\\n' "$GH_STUB_STARTED" ;;
  "cache list") [ -n "${GH_STUB_LIST_FAIL:-}" ] && exit 1; printf '%s' "$GH_STUB_LISTING" ;;
  "cache delete") [ "$3" = "${GH_STUB_DELETE_FAIL:-}" ] && exit 1; exit 0 ;;
  *) exit 2 ;;
esac
"""


def entry(id_, key, created):
    return {"id": id_, "key": key, "createdAt": created}


class DropSupersededCachesTests(unittest.TestCase):
    def run_script(self, family, listing=(), **env):
        with tempfile.TemporaryDirectory() as tmp:
            stub = Path(tmp) / "gh"
            stub.write_text(STUB)
            stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
            log = Path(tmp) / "log"
            log.touch()
            result = subprocess.run(
                ["bash", str(SCRIPT), family],
                env={
                    "PATH": f"{tmp}:{os.environ['PATH']}",
                    "GITHUB_REPOSITORY": "owner/repo",
                    "GITHUB_RUN_ID": "42",
                    "RUNNER_OS": "Linux",
                    "GH_STUB_LOG": str(log),
                    "GH_STUB_STARTED": STARTED,
                    "GH_STUB_LISTING": json.dumps(list(listing)),
                    **env,
                },
                capture_output=True,
                text=True,
            )
            return result, log.read_text().splitlines()

    def test_drops_only_earlier_generations_of_the_family(self):
        result, calls = self.run_script(
            "meta-dev-examples",
            [
                entry(11, "v0-rust-meta-dev-examples-Linux-x64-aaaa-1111", "2026-10-14T07:29:01.123456Z"),
                # Saved during this run by a sibling job of the group: kept.
                entry(12, "v0-rust-meta-dev-examples-Linux-x64-bbbb-2222", "2026-10-14T12:20:00.5Z"),
                entry(13, "v0-rust-meta-dev-examples-Linux-x64-aaaa-0000", "2026-10-13T23:59:59Z"),
            ],
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("api repos/owner/repo/actions/runs/42 --jq .run_started_at", calls[0])
        self.assertIn("cache list --repo owner/repo --key v0-rust-meta-dev-examples-Linux- ", calls[1])
        self.assertEqual(
            calls[2:],
            ["cache delete 11 --repo owner/repo", "cache delete 13 --repo owner/repo"],
        )

    def test_a_family_prefix_never_matches_a_longer_job_name(self):
        # The server filters by key prefix; the script re-checks it, so "test"
        # never reaches "test-voice" even if a listing returns it.
        _, calls = self.run_script(
            "test",
            [
                entry(21, "v0-rust-test-voice-Linux-x64-cccc-1111", "2026-10-14T07:00:00Z"),
                entry(22, "v0-rust-test-Linux-x64-dddd-1111", "2026-10-14T07:00:00Z"),
            ],
        )
        self.assertIn("--key v0-rust-test-Linux- ", calls[1])
        self.assertEqual(calls[2:], ["cache delete 22 --repo owner/repo"])

    def test_failures_are_warnings_and_never_fail_the_job(self):
        result, calls = self.run_script("test", GH_STUB_API_FAIL="1")
        self.assertEqual(result.returncode, 0)
        self.assertIn("::warning::could not read this run's start time", result.stdout)
        self.assertEqual(len(calls), 1)

        result, calls = self.run_script("test", GH_STUB_LIST_FAIL="1")
        self.assertEqual(result.returncode, 0)
        self.assertIn("::warning::could not list", result.stdout)
        self.assertEqual(len(calls), 2)

        result, _ = self.run_script(
            "test",
            [
                entry(7, "v0-rust-test-Linux-x64-a-1", "2026-10-14T01:00:00Z"),
                entry(8, "v0-rust-test-Linux-x64-a-2", "2026-10-14T02:00:00Z"),
            ],
            GH_STUB_DELETE_FAIL="7",
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("::warning::could not drop cache 7", result.stdout)
        self.assertIn("dropped cache 8", result.stdout)

    def test_nothing_older_is_quiet_success(self):
        result, calls = self.run_script(
            "fmt-lint", [entry(31, "v0-rust-fmt-lint-Linux-x64-e-1", "2026-10-14T12:30:00Z")]
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(len(calls), 2)
        self.assertIn("no v0-rust-fmt-lint-Linux-* cache older than this run", result.stdout)


if __name__ == "__main__":
    unittest.main()
