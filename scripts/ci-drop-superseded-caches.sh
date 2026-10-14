#!/usr/bin/env bash
# ci-drop-superseded-caches.sh <family>: delete the rust-cache entries of one
# CI cache family (keys "v0-rust-<family>-<os>-...") that were created before
# this workflow run started. A job runs it on main as its last step when its
# restore found no exact key, right before rust-cache saves the new
# generation, so CI never holds two generations of a family and its saves do
# not evict other entries. Entries a sibling job saved during this run are
# kept. Best effort: any failure is a warning and never fails the job.
set -u
family="${1:?usage: ci-drop-superseded-caches.sh <family>}"
prefix="v0-rust-${family}-${RUNNER_OS:-Linux}-"
repo="${GITHUB_REPOSITORY:?}"
if ! started="$(gh api "repos/${repo}/actions/runs/${GITHUB_RUN_ID:?}" --jq .run_started_at)" || [ -z "${started}" ]; then
  echo "::warning::could not read this run's start time; keeping the ${prefix}* caches"
  exit 0
fi
if ! listing="$(gh cache list --repo "${repo}" --key "${prefix}" --limit 100 --json id,key,createdAt)"; then
  echo "::warning::could not list the ${prefix}* caches; keeping them"
  exit 0
fi
if ! ids="$(printf '%s' "${listing}" | jq -r --arg p "${prefix}" --arg t "${started}" \
  '.[] | select((.key | startswith($p)) and ((.createdAt | sub("\\.[0-9]+Z$"; "Z")) < $t)) | .id')"; then
  echo "::warning::could not read the ${prefix}* cache listing; keeping them"
  exit 0
fi
if [ -z "${ids}" ]; then
  echo "no ${prefix}* cache older than this run (${started})"
  exit 0
fi
for id in ${ids}; do
  if gh cache delete "${id}" --repo "${repo}"; then
    echo "dropped cache ${id} (${prefix}*, created before ${started})"
  else
    echo "::warning::could not drop cache ${id} (${prefix}*)"
  fi
done
exit 0
