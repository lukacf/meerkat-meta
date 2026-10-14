#!/usr/bin/env python3
"""Contracts for scripts/ci-changes.py, which lets docs-only and
non-CI-workflow pull requests skip the Rust and console jobs."""

import importlib.util
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("ci_changes", ROOT / "scripts/ci-changes.py")
ci_changes = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci_changes)


class CiChangesTests(unittest.TestCase):
    def full(self, *paths, event="pull_request"):
        return ci_changes.needs_full_run(event, list(paths))

    def test_docs_and_top_level_markdown_skip_the_heavy_jobs(self):
        self.assertFalse(self.full("docs/concepts/events.mdx", "README.md", "CHANGELOG.md"))
        self.assertFalse(self.full("LICENSE-MIT"))

    def test_non_ci_workflows_and_their_python_contracts_skip_the_heavy_jobs(self):
        # The shape of PR #642: release.yml plus its python contract test.
        self.assertFalse(self.full(".github/workflows/release.yml", "scripts/test_release_workflow.py"))
        self.assertFalse(self.full(".github/workflows/e2e-live.yml", ".github/workflows/publish-docs.yml"))
        self.assertFalse(self.full(".github/CODEOWNERS", ".github/release.yml"))

    def test_files_the_heavy_jobs_read_need_the_full_matrix(self):
        for path in (
            # ci.yml defines the jobs, so its own pull request must run them.
            ".github/workflows/ci.yml",
            # Read by tests/console_route_auth.rs, governance_check and the
            # console contract tests.
            "docs/rct/console-rest-sse-contract-v0.5.0.json",
            "docs/rct/traceability.md",
            # src/mobpack.rs reads .claude/skills.
            ".claude/skills/meerkat-meta/SKILL.md",
            # Markdown below the top level can be include_str! input.
            "crates/meerkat-meta/src/memory/steward_prompt_v0.md",
            "crates/meerkat-meta/src/lib.rs",
            "Cargo.lock",
            "console/package.json",
            "packages/console-core/src/index.ts",
            "flow-editor/src/main.ts",
            "sdk/python/meerkat_meta/__init__.py",
            "examples/001-incident-command-center-pack/server.rs",
            "scripts/repo-cargo",
            "scripts/nested/helper.py",
            "Makefile",
            ".config/nextest.toml",
        ):
            with self.subTest(path=path):
                self.assertTrue(self.full(path))
                self.assertTrue(self.full("README.md", path), "one such file makes the whole run full")

    def test_pushes_dispatches_and_empty_diffs_run_everything(self):
        self.assertTrue(self.full("README.md", event="push"))
        self.assertTrue(self.full("README.md", event="workflow_dispatch"))
        self.assertTrue(self.full())

    def test_main_writes_the_github_output_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "output"
            ci_changes.main(["--event", "pull_request", "--github-output", str(out), "docs/a.md"])
            ci_changes.main(["--event", "pull_request", "--github-output", str(out), "crates/x.rs"])
            self.assertEqual(out.read_text().splitlines(), ["full=false", "full=true"])


if __name__ == "__main__":
    unittest.main()
