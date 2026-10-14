#!/usr/bin/env python3
"""Decide whether a CI run needs the full Rust and console matrix.

A pull request whose changed files are all docs or non-CI workflow files
(see SKIPPABLE) cannot change what the Rust, console, SDK and example jobs
build or test, so CI skips those jobs and `gate` accepts them as skipped.
Everything else runs the full matrix: pushes, dispatches, any file outside
the list, a change to ci.yml itself (the pull request must prove its own CI)
and an empty or unreadable diff.

Usage: ci-changes.py --event NAME [--github-output FILE] [PATH ...]
Paths default to `git diff --name-only HEAD^1 HEAD`, the pull request's own
changes when HEAD is GitHub's merge commit for it. Prints `full=true` or
`full=false` and appends the same line to FILE.
"""

import argparse
import subprocess
import sys

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
        ".github/workflows/e2e-live.yml",
        ".github/workflows/publish-docs.yml",
    }
)


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


def needs_full_run(event: str, paths: list[str]) -> bool:
    if event != "pull_request":
        return True
    if not paths:
        return True
    return not all(skippable(path) for path in paths)


def changed_paths() -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--name-only", "HEAD^1", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line for line in result.stdout.splitlines() if line]


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
        except (OSError, subprocess.CalledProcessError) as error:
            print(f"cannot list changed files ({error}); running the full matrix")
            paths = []
    full = needs_full_run(args.event, paths)
    for path in paths:
        print(f"{'skippable' if skippable(path) else 'needs-full'}: {path}")
    line = f"full={'true' if full else 'false'}"
    print(line)
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as out:
            out.write(line + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
