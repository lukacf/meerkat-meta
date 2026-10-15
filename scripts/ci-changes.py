#!/usr/bin/env python3
"""Decide which CI lanes a run needs.

Each lane is a group of jobs in .github/workflows/ci.yml that read the same
inputs (see LANE_JOBS). A pull request runs only the lanes its changed files
can reach; everything else runs every lane:

- docs, top-level markdown and the non-CI workflows (see `skippable`) reach
  no lane;
- a file under one of the AREAS prefixes reaches exactly that area's lanes;
- Rust sources and build configuration (RUST_INPUTS) reach every lane but
  console-perf, which bundles only console/ and packages/console-*;
- any other file, a change to ci.yml itself (the pull request must prove its
  own CI), a push, a dispatch and an empty or unreadable diff reach every
  lane.

An area may list fewer lanes only when no job outside them reads its files.
scripts/test_ci_changes.py scans the Rust crates and the console, flow
editor and SDK sources for references into another area and fails when a
reader's lane would not run, so a new cross-area read cannot silently skip
the job it affects.

Usage: ci-changes.py --event NAME [--github-output FILE] [PATH ...]
Paths default to `git diff --no-renames --name-only -z HEAD^1 HEAD`, the
pull request's own changes when HEAD is GitHub's merge commit for it. A
rename counts as both of its paths, so moving a file the jobs read into
docs/ still runs them. Prints and appends `<lane>=true|false` for every lane,
in one write, to FILE.
"""

import argparse
import subprocess
import sys

# The lanes, each named by the `changes` output its jobs test in ci.yml.
LANE_JOBS = {
    "rust": ("test", "test-extension-state", "extension-state-lint", "test-voice", "voice-clippy"),
    "integration": ("test-integration",),
    "python": ("test-python",),
    "console": ("console", "console-browser", "console-fixtures", "console-acceptance"),
    "console_perf": ("console-perf",),
    "flow_editor": ("flow-editor",),
}
LANES = tuple(LANE_JOBS)
EVERY_LANE = frozenset(LANES)

# Files nothing in the Rust, console, SDK or example jobs reads. docs/rct is
# not here: tests/console_route_auth.rs and the governance_check binary read
# it. Nor is .claude: src/mobpack.rs reads .claude/skills. Nor is any markdown
# below the top level: crates embed prompt files with include_str!. The python
# scripts run only in fmt-lint (which always runs) and in release.yml.
SKIPPABLE_FILES = frozenset(
    {
        ".github/CODEOWNERS",
        ".github/release.yml",
        ".github/workflows/release.yml",
        ".github/workflows/release-prewarm.yml",
        ".github/workflows/e2e-live.yml",
        ".github/workflows/publish-docs.yml",
    }
)

# Prefix -> the lanes a change below it can reach. The first match decides,
# so a narrower prefix comes before the area that contains it.
AREAS = (
    # tests/console_customization_fixtures.rs reads these.
    ("console/fixtures/", EVERY_LANE),
    # The console app and its scenario harnesses. The flow editor's browser
    # tests load console files too. The Rust crates embed only the built
    # bundle, crates/meerkat-meta/console-dist.
    ("console/", frozenset({"console", "console_perf", "flow_editor"})),
    # The flow editor app; the crates embed only crates/meerkat-meta/flow-editor-dist.
    ("flow-editor/", frozenset({"flow_editor"})),
    # The Python SDK tests, and the Rust integration tests that run its
    # parity and reference scripts.
    ("sdk/python/", frozenset({"python", "integration"})),
    # The Rust integration tests validate the TypeScript SDK, and the console
    # imports its live module. test-typescript runs on every pull request.
    ("sdk/typescript/", frozenset({"integration", "console", "console_perf", "flow_editor"})),
)

# Rust sources and build configuration reach every job that compiles Rust,
# which is every lane but console-perf.
RUST_INPUTS = ("crates/", ".cargo/")
RUST_INPUT_FILES = frozenset({"Cargo.toml", "Cargo.lock", "rust-toolchain.toml"})
RUST_LANES = EVERY_LANE - {"console_perf"}


def skippable(path: str) -> bool:
    if path.startswith("docs/rct/"):
        return False
    if path.startswith("docs/"):
        return True
    if "/" not in path and (path.endswith(".md") or path.startswith("LICENSE-")):
        return True
    if path in SKIPPABLE_FILES:
        return True
    return path.startswith("scripts/") and path.count("/") == 1 and path.endswith(".py")


def path_lanes(path: str) -> frozenset:
    """The lanes a change to this one path can reach."""
    if skippable(path):
        return frozenset()
    if path in RUST_INPUT_FILES or path.startswith(RUST_INPUTS):
        return RUST_LANES
    for prefix, lanes in AREAS:
        if path.startswith(prefix):
            return lanes
    return EVERY_LANE


def needed_lanes(event: str, paths: list[str]) -> frozenset:
    if event != "pull_request" or not paths:
        return EVERY_LANE
    lanes = frozenset()
    for path in paths:
        lanes |= path_lanes(path)
    return lanes


def needs_full_run(event: str, paths: list[str]) -> bool:
    return needed_lanes(event, paths) == EVERY_LANE


def output_lines(lanes: frozenset) -> list[str]:
    return [f"{lane}={'true' if lane in lanes else 'false'}" for lane in LANES]


def changed_paths() -> list[str]:
    # With rename detection git lists only a rename's destination, which
    # hides the removal of its source. -z keeps unusual names unquoted.
    result = subprocess.run(
        ["git", "diff", "--no-renames", "--name-only", "-z", "HEAD^1", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return [path for path in result.stdout.split("\0") if path]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--event", required=True)
    parser.add_argument("--github-output")
    parser.add_argument("paths", nargs="*")
    args = parser.parse_args(argv)
    paths = args.paths
    if not paths and args.event == "pull_request":
        try:
            paths = changed_paths()
        except (OSError, subprocess.CalledProcessError, UnicodeDecodeError) as error:
            print(f"cannot list changed files ({error}); running every lane")
            paths = []
    lanes = needed_lanes(args.event, paths)
    for path in paths:
        reached = path_lanes(path)
        print(f"{' '.join(lane for lane in LANES if lane in reached) or 'no lane'}: {path}")
    lines = output_lines(lanes)
    print("\n".join(lines))
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as out:
            out.write("".join(line + "\n" for line in lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
