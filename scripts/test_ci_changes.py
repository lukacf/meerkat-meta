#!/usr/bin/env python3
"""Contracts for scripts/ci-changes.py, which runs on a pull request only
the CI lanes its changed files can reach."""

import importlib.util
import os
import re
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


def expected_output(*lanes: str) -> str:
    return "".join(f"{lane}={'true' if lane in lanes else 'false'}\n" for lane in ci_changes.LANES)


EVERY_LANE = set(ci_changes.LANES)
CONSOLE_LANES = {"console", "console_perf", "flow_editor"}
RUST_LANES = EVERY_LANE - {"console_perf"}


# Where a file may name a path in a lane-limited or skippable area: a path
# below that area, repository-relative, reached with ../, or under a shell
# variable ($ROOT/, ${repo_root}/); a glob, cut at its first wildcard; or the
# bare area name as a quoted string (git grep and similar path arguments).
# Rust comments are skipped: they document, they do not read. Bare names are
# not read from Rust, where words like "console" name modes and routes.
AREA_NAMES = ("sdk/python", "sdk/typescript", "flow-editor", "console", "sdk", "scripts", "docs")
SKIPPABLE_NAMES = ("README.md", "CHANGELOG.md", "CONTRIBUTING.md", "CLAUDE.md", "LICENSE-MIT",
                   "LICENSE-APACHE", ".github/CODEOWNERS", ".github/release.yml",
                   ".github/workflows/release.yml", ".github/workflows/e2e-live.yml",
                   ".github/workflows/publish-docs.yml")
_NAMES = "|".join(re.escape(name) for name in sorted(AREA_NAMES, key=len, reverse=True))
_FILES = "|".join(re.escape(name) for name in SKIPPABLE_NAMES)
_REST = r"(/[^\s\"'`),;]*)?"
_ROOTED = r"(?:\.\./)+|\$\{?[A-Za-z_][A-Za-z0-9_]*\}?/"
PATH_REFERENCE = re.compile(
    # ../console/x, $ROOT/console, ${root}/sdk: a path whatever follows.
    rf"(?:{_ROOTED})({_NAMES})(?![\w-]){_REST}"
    # console/x relative to the repository root: needs the slash.
    rf"|(?<![\w./@$-])({_NAMES})(/[^\s\"'`),;]+)"
    # A skippable file, rooted or repository-relative.
    rf"|(?:{_ROOTED}|(?<![\w./@$-]))({_FILES})(?![\w.-])"
)
# A bare area name counts inside a bracketed or parenthesized list of quoted
# strings that names at least two areas (a git grep pathspec list, say), and
# a joined path ("scripts", "repo-cargo" or "scripts" / "repo-cargo") counts
# as the joined path. A quoted name that is an object key or value
# (id: "docs", "scripts": {...}) is an identifier, not a path.
QUOTED_LIST = re.compile(r"[\[(]((?:\s*[\"'`][^\"'`\n]*[\"'`]\s*,?)+)\s*[\])]")
QUOTED = re.compile(r"[\"'`]([^\"'`\n]*)[\"'`]")
JOINED = re.compile(rf"[\"'`]({_NAMES})[\"'`]\s*[,/]\s*[\"'`]([\w.-]+)[\"'`]")
COMMENT_LINE = re.compile(r"^\s*(?://|#|/\*|\*|--|<!--)")
OBJECT_VALUE = re.compile(r"\w+[\"']?\s*:\s*[\"'`]$")
BLOCKS = (re.compile(r'"""[\s\S]*?"""'), re.compile(r"'''[\s\S]*?'''"), re.compile(r"/\*[\s\S]*?\*/"))
SCANNED_SUFFIXES = {".rs", ".ts", ".tsx", ".js", ".cjs", ".mjs", ".py", ".sh", ".json", ".toml",
                    ".yml", ".yaml"}
SCANNED_NAMES = {"Makefile"}
UNSCANNED_DIRS = {"node_modules", "dist", ".tmp", "target", ".git", "__pycache__", "console-dist",
                  "flow-editor-dist", "output", ".venv", "docs", ".claude", ".codemob-mcp", "fixtures"}
MAX_SCANNED_BYTES = 1 << 20
WILDCARD = re.compile(r"[*?\[{]")

# Every place one area's files read files that changes there might not
# reach, and the lanes that must run when the read files change because
# the reading file runs in them. A pair missing here is a new cross-area
# read: decide its lanes in ci-changes.py and add it.
CROSS_AREA_READS = {
    # The console app imports sdk/typescript/src/live; console-perf bundles
    # the console sources and the flow editor's browser tests load them.
    ("console/", "sdk/typescript/"): CONSOLE_LANES,
    # console/fixture-ready.test.cjs (the console job) greps the flow
    # editor, the SDKs and the scripts for port reservations.
    ("console/", "flow-editor/"): {"console"},
    ("console/", "sdk/"): {"console"},
    ("console/", "sdk/python/"): {"console"},
    ("console/", "scripts/"): {"console"},
    # Rust integration tests run the Python SDK's parity and reference
    # scripts and read its sources.
    ("crates/*/tests/", "sdk/python/"): {"integration"},
    # Rust integration tests validate the TypeScript SDK and read its types.
    ("crates/*/tests/", "sdk/typescript/"): {"integration"},
    # examples/001 runs in console-acceptance and loads console sources; the
    # other packs name the console or the TypeScript SDK for manual runs.
    ("examples/", "console/"): {"console"},
    ("examples/", "sdk/typescript/"): {"console"},
    # The flow editor's browser tests start console fixtures.
    ("flow-editor/", "console/"): {"flow_editor"},
    # The console component packages' tests import console sources; the
    # console job runs them.
    ("packages/", "console/"): {"console"},
    # Workflow files: ci.yml runs console scripts and keys the console lane's
    # npm cache on console/package-lock.json, keys flow-editor's on its own,
    # and installs sdk/python in test-python. Its contract scripts run in
    # fmt-lint and test-typescript runs on every run, so neither needs a
    # lane; release, docs and live workflows are not CI lanes.
    (".github/", "console/"): {"console"},
    (".github/", "flow-editor/"): {"flow_editor"},
    (".github/", "sdk/python/"): {"python"},
    (".github/", "sdk/typescript/"): set(),
    (".github/", "scripts/"): set(),
    (".github/", "docs/"): set(),
    # make test-python reads sdk/python; the release targets and local
    # hooks are not CI lanes.
    ("Makefile", "sdk/python/"): {"python"},
    ("Makefile", "sdk/typescript/"): set(),
    ("Makefile", "scripts/"): set(),
    ("Makefile", "skippable"): set(),
    (".pre-commit-config.yaml", "scripts/"): set(),
    # Contract tests and release scripts run in fmt-lint, which always runs.
    ("scripts/", "console/"): set(),
    ("scripts/", "flow-editor/"): set(),
    ("scripts/", "sdk/"): set(),
    ("scripts/", "sdk/python/"): set(),
    ("scripts/", "sdk/typescript/"): set(),
    ("scripts/", "scripts/"): set(),
    ("scripts/", "docs/"): set(),
    ("scripts/", "skippable"): set(),
}


def reader_role(path: str) -> str:
    """The area a referencing file runs as: crates split by target kind."""
    parts = path.split("/")
    if parts[0] == "crates" and len(parts) > 2:
        return f"crates/*/{parts[2]}/"
    for prefix, _ in ci_changes.AREAS:
        if path.startswith(prefix):
            return prefix
    return parts[0] + "/" if len(parts) > 1 else parts[0]


def area_of(path: str) -> str:
    if ci_changes.skippable(path) and not path.startswith("docs/"):
        return "skippable"
    for prefix, _ in ci_changes.AREAS:
        if path.startswith(prefix):
            return prefix
    return path.split("/", 1)[0] + "/"


def tracked_files(root: Path, target: str) -> list[str]:
    """Files at or below `target`, or named by its extension-less start (an
    import such as ../sdk/typescript/src/live). Plain string comparison:
    the text may hold glob characters."""
    candidate = root / target
    if candidate.is_file():
        return [target]
    if candidate.is_dir():
        found = []
        for directory, subdirectories, files in os.walk(candidate):
            subdirectories[:] = [d for d in subdirectories if d not in {"node_modules", ".git", "dist", ".tmp"}]
            found += [Path(directory, name).relative_to(root).as_posix() for name in files]
        return found
    parent = candidate.parent
    if parent.is_dir():
        return [(parent / entry.name).relative_to(root).as_posix() for entry in parent.iterdir()
                if entry.is_file() and entry.name.startswith(candidate.name + ".")]
    return []


def target_lanes(root: Path, target: str) -> frozenset | None:
    """The lanes a change anywhere under `target` is sure to reach (the
    intersection over its files), or None when it names nothing."""
    files = tracked_files(root, target)
    if not files:
        return None
    lanes = ci_changes.EVERY_LANE
    for name in files:
        lanes &= ci_changes.path_lanes(name)
    return lanes


def references(text: str, *, bare: bool) -> list[str]:
    found = []
    for match in PATH_REFERENCE.finditer(text):
        if match.group(1):
            found.append(match.group(1) + (match.group(2) or ""))
        elif match.group(3):
            found.append(match.group(3) + match.group(4))
        elif not OBJECT_VALUE.search(text, max(0, match.start() - 40), match.start()):
            found.append(match.group(5))
    if bare:
        for match in QUOTED_LIST.finditer(text):
            names = [name for name in QUOTED.findall(match.group(1)) if name in AREA_NAMES]
            if len(names) >= 2:
                found += names
        found += [f"{match.group(1)}/{match.group(2)}" for match in JOINED.finditer(text)]
    cut = []
    for target in found:
        wildcard = WILDCARD.search(target)
        if wildcard:
            target = target[:wildcard.start()]
        cut.append(target.rstrip("./") or target)
    return cut


def cross_area_reads(root: Path) -> dict[tuple[str, str], set[str]]:
    reads: dict[tuple[str, str], set[str]] = {}
    for directory, subdirectories, files in os.walk(root):
        subdirectories[:] = [d for d in subdirectories
                             if d not in UNSCANNED_DIRS and not d.startswith(".release-workflow-fixture")]
        for name in files:
            if Path(name).suffix not in SCANNED_SUFFIXES and name not in SCANNED_NAMES:
                continue
            path = Path(directory, name)
            relative = path.relative_to(root).as_posix()
            try:
                if path.stat().st_size > MAX_SCANNED_BYTES:
                    continue
                text = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            # Comments and docstrings document; they do not read.
            for block in BLOCKS:
                text = block.sub("", text)
            text = "\n".join(line for line in text.splitlines() if not COMMENT_LINE.match(line))
            for target in references(text, bare=path.suffix != ".rs"):
                lanes = target_lanes(root, target)
                if lanes is None or lanes == ci_changes.EVERY_LANE:
                    continue
                reader, read = area_of(relative), area_of(target.rstrip("/") + ("/" if (root / target).is_dir() else ""))
                if (root / target).is_dir():
                    read = target.rstrip("/") + "/" if target.rstrip("/") in AREA_NAMES else read
                if reader == read or (read != "skippable" and relative.startswith(read)):
                    continue
                reads.setdefault((reader_role(relative), read), set()).add(f"{relative} -> {target}")
    return reads


class CiChangesTests(unittest.TestCase):
    def full(self, *paths, event="pull_request"):
        return ci_changes.needs_full_run(event, list(paths))

    def lanes(self, *paths, event="pull_request"):
        return set(ci_changes.needed_lanes(event, list(paths)))

    def test_docs_and_top_level_markdown_skip_the_heavy_jobs(self):
        self.assertEqual(self.lanes("docs/concepts/events.mdx", "README.md", "CHANGELOG.md"), set())
        self.assertEqual(self.lanes("LICENSE-MIT"), set())

    def test_non_ci_workflows_skip_the_heavy_jobs_and_python_scripts_run_the_console_lane(self):
        self.assertEqual(self.lanes(".github/workflows/e2e-live.yml", ".github/workflows/publish-docs.yml"), set())
        self.assertEqual(self.lanes(".github/CODEOWNERS", ".github/release.yml", ".github/workflows/release.yml"),
                         set())
        # The shape of PR #642: release.yml plus its python contract test.
        # fmt-lint runs the script; console/fixture-ready.test.cjs greps it.
        self.assertEqual(self.lanes(".github/workflows/release.yml", "scripts/test_release_workflow.py"),
                         {"console"})

    def test_the_classifier_and_its_contract_run_every_lane(self):
        for path in ("scripts/ci-changes.py", "scripts/test_ci_changes.py"):
            with self.subTest(path=path):
                self.assertEqual(self.lanes(path), EVERY_LANE)

    def test_files_every_lane_reads_run_every_lane(self):
        for path in (
            # ci.yml defines the jobs, so its own pull request must run them.
            ".github/workflows/ci.yml",
            # Read by tests/console_route_auth.rs, governance_check and the
            # console contract tests.
            "docs/rct/console-rest-sse-contract-v0.5.0.json",
            "docs/rct/traceability.md",
            # src/mobpack.rs reads .claude/skills.
            ".claude/skills/meerkat-meta/SKILL.md",
            # tests/console_customization_fixtures.rs reads console fixtures.
            "console/fixtures/configured-host-shell/config/console.toml",
            # Shared by the console, its components and the flow editor.
            "packages/console-core/src/index.ts",
            "examples/001-incident-command-center-pack/server.rs",
            "scripts/repo-cargo",
            "scripts/nested/helper.py",
            "Makefile",
            ".config/nextest.toml",
        ):
            with self.subTest(path=path):
                self.assertTrue(self.full(path))
                self.assertTrue(self.full("README.md", path), "one such file makes the whole run full")

    def test_rust_sources_run_every_lane_but_console_perf(self):
        # console-perf bundles only console/ and packages/console-*; every
        # other lane compiles the crates.
        for path in (
            # Markdown below the top level can be include_str! input.
            "crates/meerkat-meta/src/memory/steward_prompt_v0.md",
            "crates/meerkat-meta/src/lib.rs",
            "crates/meerkat-meta/console-dist/index.html",
            "Cargo.toml",
            "Cargo.lock",
            "rust-toolchain.toml",
            ".cargo/config.toml",
        ):
            with self.subTest(path=path):
                self.assertEqual(self.lanes(path), RUST_LANES)
        self.assertEqual(self.lanes("crates/meerkat-meta/src/lib.rs", "console/src/App.tsx"), EVERY_LANE)

    def test_area_changes_run_only_the_lanes_that_read_them(self):
        for paths, lanes in (
            (("console/package.json", "console/src/ConsoleApp.tsx"), CONSOLE_LANES),
            (("flow-editor/src/main.ts",), {"flow_editor", "console"}),
            (("sdk/python/meerkat_meta/__init__.py",), {"python", "integration", "console"}),
            (("sdk/typescript/src/live.ts",), {"integration"} | CONSOLE_LANES),
            (("sdk/python/meerkat_meta/types.py", "sdk/typescript/src/types.ts"),
             {"python", "integration"} | CONSOLE_LANES),
            (("flow-editor/src/main.ts", "README.md", "docs/guide.md"), {"flow_editor", "console"}),
        ):
            with self.subTest(paths=paths):
                self.assertEqual(self.lanes(*paths), lanes)

    def test_lanes_cover_every_gated_job_once(self):
        jobs = [job for lane_jobs in ci_changes.LANE_JOBS.values() for job in lane_jobs]
        self.assertEqual(len(jobs), len(set(jobs)))
        for _, lanes in ci_changes.AREAS:
            self.assertLessEqual(set(lanes), EVERY_LANE)

    def test_every_cross_area_read_has_audited_lanes(self):
        """A file that reads another area's files runs in some lanes, so a
        change to the read files must reach those lanes. The scan finds such
        reads in the Rust crates, the console, flow editor and SDK sources,
        the packages, examples and scripts; each must be audited above, and
        the area it reads must reach the lanes its reader runs in."""
        reads = cross_area_reads(ROOT)
        unaudited = {pair: sorted(found)[:3] for pair, found in reads.items() if pair not in CROSS_AREA_READS}
        self.assertEqual(unaudited, {}, "new cross-area reads: give them lanes in ci-changes.py's AREAS")
        for (_, area), required in CROSS_AREA_READS.items():
            with self.subTest(area=area):
                self.assertLessEqual(required, set(ci_changes.path_lanes(area)))
        # The scan sees the reads that motivated the lanes.
        self.assertIn(("console/", "sdk/typescript/"), reads)
        self.assertIn(("crates/*/tests/", "sdk/python/"), reads)

    def test_the_scan_catches_a_new_cross_area_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for relative, text in {
                "sdk/python/meerkat_meta/types.py": "X = 1\n",
                "sdk/typescript/src/types.ts": "export {}\n",
                "console/src/App.tsx": "export {}\n",
                "docs/guide.md": "# Guide\n",
                "scripts/tool.py": "\n",
                "crates/x/src/lib.rs": '// sdk/python/meerkat_meta/types.py is only named here\n'
                                      'const T: &str = include_str!("../../../sdk/python/meerkat_meta/types.py");\n',
                "flow-editor/src/a.ts": 'import { X } from "../../console/src/App";\n',
                # A git grep pathspec list of bare area names.
                "packages/p/grep.cjs": 'run(["grep", "--", "console", "sdk", "scripts"]);\n',
                # A shell variable root and a glob cut at its first wildcard.
                "examples/run.sh": 'cat "$ROOT/docs/guide.md"\nls sdk/typescript/src/*.ts\n',
                # A manifest reader.
                "tests/Makefile": "check:\n\tcat ../docs/guide.md\n",
                # Identifiers and docstrings are not reads.
                "packages/q/ids.ts": 'const a = { id: "docs", profile: "console" };\nconst b = ["code", "docs"];\n',
                "packages/q/doc.py": '"""See docs/guide.md."""\n',
            }.items():
                (root / relative).parent.mkdir(parents=True, exist_ok=True)
                (root / relative).write_text(text)
            reads = cross_area_reads(root)
        self.assertEqual(set(reads), {
            ("crates/*/src/", "sdk/python/"), ("flow-editor/", "console/"),
            ("packages/", "console/"), ("packages/", "sdk/"), ("packages/", "scripts/"),
            ("examples/", "docs/"), ("examples/", "sdk/typescript/"), ("tests/", "docs/"),
        })
        self.assertNotIn(("crates/*/src/", "sdk/python/"), CROSS_AREA_READS)

    def test_pushes_dispatches_and_empty_diffs_run_everything(self):
        self.assertEqual(self.lanes("README.md", event="push"), EVERY_LANE)
        self.assertEqual(self.lanes("console/src/App.tsx", event="push"), EVERY_LANE)
        self.assertEqual(self.lanes("README.md", event="workflow_dispatch"), EVERY_LANE)
        self.assertEqual(self.lanes(), EVERY_LANE)

    def test_a_rename_out_of_a_built_tree_runs_everything(self):
        # Moving a component the console still imports into docs/ takes it
        # out of the console build. git reports the move as one rename, whose
        # destination alone is skippable.
        component = "export function ConsoleApp() {\n  return null;\n}\n" * 4
        index = "import { ConsoleApp } from './ConsoleApp';\n"
        base = {"console/src/ConsoleApp.tsx": component, "console/src/index.tsx": index}
        head = {"docs/ConsoleApp.tsx": component, "console/src/index.tsx": index}
        self.assertEqual(classify_commit(base, head), expected_output(*CONSOLE_LANES))
        # Moving a crate's file into docs/ runs the Rust lanes the same way.
        base = {"crates/x/src/lib.rs": component}
        head = {"docs/lib.rs": component}
        self.assertEqual(classify_commit(base, head), expected_output(*RUST_LANES))

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
        self.assertEqual(classify_commit(base, head), expected_output())

    def test_names_git_would_quote_are_read_verbatim(self):
        # Without -z, git quotes this name and no rule could match it.
        base = {"docs/notes.md": "x\n"}
        head = {"docs/notes.md": "x\n", "docs/caf\u00e9 notes.md": "y\n"}
        self.assertEqual(classify_commit(base, head), expected_output())

    def test_main_writes_the_github_output_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "output"
            ci_changes.main(["--event", "pull_request", "--github-output", str(out), "docs/a.md"])
            ci_changes.main(["--event", "pull_request", "--github-output", str(out), "flow-editor/x.ts"])
            ci_changes.main(["--event", "push", "--github-output", str(out), "docs/a.md"])
            self.assertEqual(
                out.read_text(),
                expected_output() + expected_output("flow_editor", "console") + expected_output(*EVERY_LANE),
            )


if __name__ == "__main__":
    unittest.main()
