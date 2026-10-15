# Contributing to Meta

Thank you for considering contributing to Meta.

## Development Setup

### Prerequisites

- Rust 1.97.0 via rustup, as pinned by `rust-toolchain.toml`; the crate's declared MSRV is 1.94.0
- Python 3.11+ for the release tooling and repository scripts that import `tomllib`; the Python SDK supports Python 3.10+
- Node.js 18+ (for console and TypeScript SDK)

### Building

All Rust commands use `scripts/repo-cargo`, which isolates `CARGO_HOME` and `CARGO_TARGET_DIR` per repo/worktree. The Makefile calls it automatically.

```bash
# Via Makefile (preferred)
make check
make build

# Or directly
./scripts/repo-cargo check --workspace
./scripts/repo-cargo build --workspace

# Python (no build step — pure Python)
PYTHONPATH=sdk/python python3 -c "import meerkat_meta; print('OK')"
```

### Testing

```bash
# Via Makefile (preferred)
make test          # Rust tests
make test-python   # Python tests
make test-all      # Rust + Python + Flow Editor

# Or directly
./scripts/repo-cargo nextest run --workspace -E 'not test(governance_contracts)' --no-fail-fast

# Python
PYTHONPATH=sdk/python python3 -m pytest sdk/python/tests/ -v

# TypeScript changes (typecheck, build, and tests)
npm --prefix sdk/typescript run validate
```

`make test-all` covers the local Rust, Python, and Flow Editor suites, not every
required hosted-CI gate. See [`.github/workflows/ci.yml`](.github/workflows/ci.yml)
for the console, voice, TypeScript, and other required hosted checks.

## Branch Conventions

- `main` — stable, PRs merge here
- `feat/<name>` — new features
- `fix/<name>` — bug fixes
- `docs/<name>` — documentation
- `refactor/<name>` — non-functional refactoring

## Pull Requests

1. Create a feature branch from `main`
2. Make your changes with clear commit messages
3. Run the applicable local checks above (`make test-all` covers Rust + Python + Flow Editor; TypeScript changes also need `npm --prefix sdk/typescript run validate`)
4. Open a PR against `main` with a description of changes; ensure the required hosted-CI gates pass before merging

## Code Style

- **Rust**: `make fmt` for formatting, `make lint` for clippy
- **Python**: Type annotations required on all public functions

Rust commands use `scripts/repo-cargo`, which defaults to repo/worktree-scoped
cache directories. Explicit `CARGO_HOME` and `CARGO_TARGET_DIR` values are
preserved for isolated build environments; `scripts/repo-cargo --print-env`
reports the selected roots and `--doctor` checks them. Keep custom roots outside
the checkout and use the same roots for validation and normal Git hooks.

## Releases: protocol v1

Repository development, CI and candidate builds use Rust `1.97.0`, pinned in
`rust-toolchain.toml` and the workflows. Floating `stable` selected Rust `1.98.1`
on hosted Linux and exceeded the query-depth limit while compiling the published
Meerkat `0.8.35` warm-revival future. The fixed toolchain keeps build inputs
reproducible without changing dependency bytes, features or recursion limits.
All required gates still run; upgrading the pin requires qualifying CI and the
complete candidate matrix together.

**Tag pushes validate only. They do not build or publish.** Publication requires
an explicit selection of accepted, immutable candidate artifacts. `make release`
still means a local optimized build; it is not a publication command.

Prepare the release-version/dependency changes in a PR and merge it. Candidate
production must run from that final merged commit, not a PR head or an earlier
build. Candidate runs may start as soon as the commit is on main: their build
legs run beside the commit's `ci.yml` **push-to-main** run, and collection
refuses (it never waits) unless that run is already green by then. A refused
candidate binds nothing; dispatch a new one once CI is green.

While the release PR is open, prewarm the candidate's build cache from its head.
The prewarm builds the same five targets without signing, packaging, attesting or
uploading anything; it only lets the candidate restore Meerkat Core and the other
dependencies instead of compiling them after the merge:

```bash
# Dispatch on main; SOURCE_SHA must be the head of a branch of this repository.
make release-prewarm SOURCE_SHA=<release-pr-head-sha>
```

`make ci-prewarm SOURCE_SHA=<release-pr-head-sha>` does the same for `ci.yml`:
a dispatch on main builds and tests that head and saves main's CI caches, so the
push-to-main run after the merge restores a bumped Meerkat Core instead of
compiling it in every job. A dispatch run is never release evidence. It is a
second full CI run beside the pull request's own, so use it when runners are
free, not while other workflows queue.

The five targets form two tiers, each built and bound by its own candidate run
from the same commit. The core tier (Linux x86_64 and aarch64, macOS arm64) is
what the release waits for; the trailing tier (macOS x86_64 and Windows, the
slowest legs) is added to the published release afterwards:

```bash
# Dispatch from main; refuse if GitHub's dispatch SHA differs from SOURCE_SHA.
make release-candidate SOURCE_SHA=<full-merged-commit-sha> TIER=core
make release-candidate SOURCE_SHA=<full-merged-commit-sha> TIER=trailing
```

Each candidate run builds its tier's targets and all three executables, checks
Linux portability, checks every macOS binary's architecture, signs macOS
executables, and packages them once. Candidate mode never runs GitHub release or
registry publishers. Archive versions come from the pinned workspace, not the
branch name.

Each completed candidate run's summary contains a versioned JSON selection.
Save it unchanged outside the repository, for example as
`accepted-core.json` and `accepted-trailing.json`. It binds the repository, run
and attempt, tier, immutable manifest artifact ID and ZIP digest, and manifest
file hash. The attested manifest binds the exact source, version, CI evidence,
the tier's target artifact identities, and its archive and inner-executable
hashes. The manifest's own artifact identity is outside the manifest to avoid a
self-referential hash.

Inner members follow the unchanged producer: Unix archives contain the executable
basename, and Windows ZIPs contain exactly
`target/<exact-target>/release/<exact-binary>.exe`. These paths are derived from
the supported target/binary inventory, never chosen by the manifest. Other nested
paths, links, duplicate members, and extra files are refused.

The selection has this strict shape (use the actual values emitted by the run,
not placeholders). IDs are integers; digests are 64 lowercase hex characters:

```json
{
  "schema_version": 2,
  "kind": "accepted-candidate",
  "repository": "lukacf/meerkat-meta",
  "run_id": 123,
  "run_attempt": 1,
  "tier": "core",
  "manifest_artifact": {
    "id": 456,
    "digest": "<artifact ZIP SHA-256>",
    "manifest_sha256": "<candidate-manifest.json SHA-256>"
  }
}
```

Promotion accepts only the core selection. It publishes the core archives with
`index.json` and `checksums.sha256` listing them, the receipt, manifest and
provenance, then the registries. Add the trailing tier to the published release
with its selection; this is add-only, bound to the published core receipt and
the same tag and source, and refuses any existing file with different bytes:

```bash
make release-assets-recover RELEASE_TAG=vX.Y.Z ARTIFACT_SELECTION=accepted-trailing.json
```

It adds the trailing archives, `index-trailing.json`,
`checksums-trailing.sha256`, `candidate-selection-trailing.json`,
`candidate-manifest-trailing.json` and its provenance. Verification and
registry retry require the core tier only. Schema 1 selections and releases (up
to v0.9.1) bind all five targets in one candidate and keep verifying as before.

Give consumers the selected artifacts and hashes for acceptance. Acceptance is
external to this workflow; the workflow does not claim to have performed consumer
tests. If the source, run, attempt, archives, or signed executable bytes change,
obtain a new candidate selection and repeat acceptance.

After acceptance, the release owner creates `vX.Y.Z` on the **same merged source
SHA**. Its automatic workflow validates the tag and exact-main CI only. The owner
then explicitly promotes the accepted selection:

```bash
make release-promote RELEASE_TAG=vX.Y.Z \
  ARTIFACT_SELECTION=/absolute/path/accepted-candidate.json

# Asset-only promotion (no registries):
make release-promote RELEASE_TAG=vX.Y.Z \
  ARTIFACT_SELECTION=/absolute/path/accepted-candidate.json \
  PUBLISH_RELEASE_PACKAGES=false

# Verify the selected candidate and exercise package dry-runs; publish nothing:
make release-promote RELEASE_TAG=vX.Y.Z \
  ARTIFACT_SELECTION=/absolute/path/accepted-candidate.json \
  REGISTRY_DRY_RUN=true
```

Promotion verifies the original signed provenance, including the certificate's
source digest and workflow identity, run/attempt, artifact ZIP digests, archive
hashes, and every inner executable hash. It copies the original archives, index,
and checksums without rebuilding, signing, stripping, or recompressing anything.
A tag mismatch, missing/expired artifact, changed attempt, or failed verification
refuses publication; there is no fallback build. Do not rerun an accepted
candidate's jobs and assume the old selection still applies.

Real publication proceeds in order: verified public binary assets, Python and
TypeScript packages, the Rust crates (`meta-extension-state`, then
`meerkat-meta`), then exact registry readback. If an SDK registry upload
fails, the crates still publish, but the readback then fails the run while any
registry lacks the exact version. Releases do not publish
documentation: the `Publish docs` workflow asks the docs site to mirror `docs/`
on every push to `main` that touches `docs/`.
New releases retain the original candidate manifest, selection, and provenance
proofs. Existing flat `index.json` and `checksums.sha256` keep their archive-only
contract.

### Recovery without replacing published bytes

```bash
# Registry-only retry against an explicit existing tag:
make release-registry-retry RELEASE_TAG=vX.Y.Z

# Registry-only dry-run; no GitHub asset mutation:
make release-registry-retry RELEASE_TAG=vX.Y.Z REGISTRY_DRY_RUN=true

# Add explicitly selected missing ORIGINAL assets to an existing release:
make release-assets-recover RELEASE_TAG=vX.Y.Z \
  ARTIFACT_SELECTION=/absolute/path/original-artifacts.json
```

Registry retries read back the complete published asset set and never invoke the
GitHub asset publisher. Complete candidate-era releases retain sufficient public
proof for registry retries after Actions artifact expiry. Legacy releases are not
retroactively assigned candidate receipts.

Asset recovery accepts the original accepted-candidate selection, or a versioned
`original-assets` selection for a legacy release. Legacy selections explicitly
name the source/version, original run/attempt and artifact IDs/ZIP digests, and
missing archive names with outer/inner hashes. Their signed source provenance must
match the existing tag and original release index/checksums. This is not permission
to create a release or rebuild a historical binary.

Legacy ZIPs may omit a retained provenance bundle. Only that legacy recovery path
can retrieve the original signed attestation from GitHub instead. An invalid
retained bundle does not trigger fallback, and accepted candidates always require
their retained original bundles.

The legacy selection uses the following shape. `metadata` hashes identify the
existing release metadata; `artifacts` identifies original Actions ZIPs, not
GitHub release asset IDs. List only the archives explicitly authorized for
addition, even when the original ZIP also contains sibling binaries:

```json
{
  "schema_version": 1,
  "kind": "original-assets",
  "repository": "lukacf/meerkat-meta",
  "source_sha": "<full tagged source SHA>",
  "version": "0.8.31",
  "tag": "v0.8.31",
  "run_id": 123,
  "run_attempt": 1,
  "metadata": {
    "index.json": "<original SHA-256>",
    "checksums.sha256": "<original SHA-256>"
  },
  "artifacts": [{
    "id": 456,
    "name": "<original Actions artifact name>",
    "digest": "<original artifact ZIP SHA-256>",
    "target": "x86_64-pc-windows-msvc",
    "archives": [{
      "name": "meta-rpc-gateway-0.8.31-x86_64-pc-windows-msvc.zip",
      "target": "x86_64-pc-windows-msvc",
      "binary": "rpc_gateway",
      "size": 1234,
      "sha256": "<original archive SHA-256>",
      "inner": {
        "name": "target/x86_64-pc-windows-msvc/release/rpc_gateway.exe",
        "size": 5678,
        "sha256": "<original executable SHA-256>"
      }
    }]
  }]
}
```

Uploads are add-only: a matching existing file is a no-op; any conflicting hash
refuses before writes. Never delete or replace published assets, including the
existing `v0.8.31` Windows archives. Missing originals, absent original provenance,
or missing legacy index/checksums with no original-byte source require explicit
operator intervention, not regeneration. A partial draft with no matching
selection receipt similarly requires explicit owner cleanup.

After creating a new draft, the publisher allows up to 30 successful list reads
for GitHub's draft visibility to settle. It never repeats creation or retries
API errors. If visibility still fails, the empty unbound draft is retained for
explicit owner cleanup; the tag and accepted candidate bytes remain unchanged.

All supported recovery runs use the current workflow on `main`, with the old tag
as an explicit input. **Do not rerun historical release jobs or re-push old tags.**
GitHub reruns historical workflow code; the new safeguards cannot rewrite those
old snapshots.

### Dispatch modes and publication booleans

The workflow's safe default mode is `existing`. `release_tag` by itself no longer
builds or publishes assets. `P` means `publish_release_packages`, and `D` means
`registry_dry_run`. Any required-field/schema/source validation failure refuses.

| Event / mode | Inputs | Result |
|---|---|---|
| Tag push | All dispatch inputs ignored | Validation only; no binary/package builds or publication |
| `candidate` | Exact source; P=false, D=false | Candidate build only |
| `candidate` | P=true or D=true | Refuse contradictory publication inputs |
| `promote` | Tag + selection; P=false, D=false | Publish original assets only |
| `promote` | Tag + selection; P=true, D=false | Assets, then registries |
| `promote` | Tag + selection; P=false, D=true | Candidate verification only |
| `promote` | Tag + selection; P=true, D=true | Candidate verification + package dry-runs; no publication |
| `existing` | Tag; P=true, D=false | Registry retry after public asset verification |
| `existing` | Tag; P=true, D=true | Tagged package dry-runs only |
| `existing` | P=false, either D | Validation only |
| `existing` | No tag; P=true, D=true | Pure package dry-run; the only exact-main CI exemption |
| `existing` | No tag; P=true, D=false | Refuse untagged publication |
| `assets` | Existing release/tag + original selection; P=false, D=false | Add selected missing original assets only |
| `assets` | P=true or D=true | Refuse ambiguous recovery inputs |

Facade commands use the owner's `gh` authentication, with `GH_TOKEN` and
`GITHUB_API_TOKEN` removed from the local dispatch subprocess environment. Workflow
jobs use their scoped GitHub token. No facade command creates a tag or selects a
candidate by "latest successful" run.

## License

By contributing, you agree that your contributions will be dual-licensed
under the MIT and Apache 2.0 licenses.
