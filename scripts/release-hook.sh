#!/usr/bin/env bash
# Pre-release hook for cargo-release.
# Called with the new version as the first argument.
# Bumps SDK versions and stages the changes for the release commit.

set -euo pipefail

VERSION="${1:?usage: release-hook.sh <version>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Prevent duplicate execution within the same release
SENTINEL="$ROOT/.release-hook-done"
if [ -f "$SENTINEL" ] && [ "$(cat "$SENTINEL")" = "$VERSION" ]; then
  echo "release-hook: already ran for $VERSION, skipping"
  exit 0
fi

echo "release-hook: bumping SDK versions to $VERSION"
"$ROOT/scripts/bump-sdk-versions.sh" "$VERSION"

echo "release-hook: verifying version parity"
"$ROOT/scripts/verify-version-parity.sh"

echo "release-hook: staging SDK version files"
# The SDK manifests are a fixed set, matching what bump-sdk-versions.sh seds by
# name. The BUILD.bazel files are NOT: they are generated per workspace crate,
# so they are staged by PATHSPEC. Naming one of them here is what let
# crates/meta-store-conformance/BUILD.bazel drift a release behind even after the
# generator repaired it in the worktree, because the release commit simply did
# not include the file.
git add \
  "$ROOT/sdk/python/pyproject.toml" \
  "$ROOT/sdk/typescript/package.json" \
  "$ROOT/sdk/typescript/package-lock.json" \
  "$ROOT/MODULE.bazel" \
  "$ROOT/docs/quickstart.mdx" \
  "$ROOT/docs/sdks/rust.mdx"
# Collect the tracked BUILD.bazel paths first. A literal or glob pathspec that
# matches nothing is FATAL to `git add`, and the release-script test fixture is
# a synthetic repo that may carry none, so passing the pattern straight to git
# would fail the hook on an empty match rather than on a real problem.
bazel_build_files=()
while IFS= read -r tracked; do
    bazel_build_files+=("$tracked")
done < <(git -C "$ROOT" ls-files -- '*BUILD.bazel')

if [ ${#bazel_build_files[@]} -gt 0 ]; then
    git -C "$ROOT" add -- "${bazel_build_files[@]}"

    # A release must not leave a regenerated artifact behind unstaged. If
    # anything the generator touched is still dirty, the commit would ship a
    # version whose generated files disagree with it, which is the failure this
    # hook exists to prevent rather than cause.
    if ! git -C "$ROOT" diff --quiet -- "${bazel_build_files[@]}"; then
        echo "release-hook: generated Bazel files remain unstaged after staging:" >&2
        git -C "$ROOT" diff --name-only -- "${bazel_build_files[@]}" >&2
        exit 1
    fi
fi

echo "$VERSION" > "$SENTINEL"
echo "release-hook: done ($VERSION)"
