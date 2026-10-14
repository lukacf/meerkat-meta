#!/usr/bin/env python3
"""Contracts for scripts/ci-changes.py, which lets docs-only and
non-CI-workflow pull requests skip the Rust and console jobs."""

import importlib.util
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/ci-changes.py"
SPEC = importlib.util.spec_from_file_location("ci_changes", SCRIPT)
ci_changes = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci_changes)


def classify_commit(base: dict[str, str], head: dict[str, str]) -> str:
    """Run ci-changes.py as CI does, in a new repository whose HEAD commit
    turns the `base` tree into the `head` tree ({path: text}), so git
    itself lists the changed paths. Returns the GITHUB_OUTPUT text."""
    # A git hook exports GIT_DIR and GIT_INDEX_FILE; neither they nor the
    # caller's git configuration may reach the scratch repository.
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    env.update(GIT_AUTHOR_NAME="ci", GIT_AUTHOR_EMAIL="ci@example.invalid")
    env.update(GIT_COMMITTER_NAME="ci", GIT_COMMITTER_EMAIL="ci@example.invalid")
    with tempfile.TemporaryDirectory() as tmp:
        repo = Path(tmp) / "repo"
        repo.mkdir()
        output = Path(tmp) / "output"

        def git(*args: str) -> None:
            subprocess.run(["git", *args], cwd=repo, env=env, check=True, capture_output=True)

        git("init", "-q")
        for tree in (base, head):
            for path in set(base) - set(tree):
                (repo / path).unlink()
            for path, text in tree.items():
                (repo / path).parent.mkdir(parents=True, exist_ok=True)
                (repo / path).write_text(text, encoding="utf-8")
            git("add", "-A")
            git("commit", "-q", "-m", "tree")
        subprocess.run(
            [sys.executable, str(SCRIPT), "--event", "pull_request", "--github-output", str(output)],
            cwd=repo,
            env=env,
            check=True,
            capture_output=True,
        )
        return output.read_text(encoding="utf-8")


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

    def test_a_rename_out_of_a_built_tree_runs_everything(self):
        # Moving a component the console still imports into docs/ takes it
        # out of the console build. git reports the move as one rename, whose
        # destination alone is skippable.
        component = "export function ConsoleApp() {\n  return null;\n}\n" * 4
        index = "import { ConsoleApp } from './ConsoleApp';\n"
        base = {"console/src/ConsoleApp.tsx": component, "console/src/index.tsx": index}
        head = {"docs/ConsoleApp.tsx": component, "console/src/index.tsx": index}
        self.assertEqual(classify_commit(base, head), "full=true\n")

    def test_a_docs_only_commit_read_from_git_skips_the_heavy_jobs(self):
        # Control for the test above: the same path from git to verdict
        # skips a commit that edits, adds and renames files within docs/.
        guide = "# Guide\n\nSteps.\n" * 8
        base = {"docs/guide.md": guide, "docs/old.md": "old\n", "crates/x.rs": "fn main() {}\n"}
        head = {
            "docs/guides/guide.md": guide,
            "docs/old.md": "new\n",
            "docs/added.md": "added\n",
            "crates/x.rs": "fn main() {}\n",
        }
        self.assertEqual(classify_commit(base, head), "full=false\n")

    def test_names_git_would_quote_are_read_verbatim(self):
        # Without -z, git quotes this name and no rule could match it.
        base = {"docs/notes.md": "x\n"}
        head = {"docs/notes.md": "x\n", "docs/caf\u00e9 notes.md": "y\n"}
        self.assertEqual(classify_commit(base, head), "full=false\n")

    def test_main_writes_the_github_output_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "output"
            ci_changes.main(["--event", "pull_request", "--github-output", str(out), "docs/a.md"])
            ci_changes.main(["--event", "pull_request", "--github-output", str(out), "crates/x.rs"])
            self.assertEqual(out.read_text().splitlines(), ["full=false", "full=true"])


if __name__ == "__main__":
    unittest.main()
