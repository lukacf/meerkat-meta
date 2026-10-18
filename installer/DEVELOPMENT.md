# Forge source development

Forge uses the shared Meta Console components and transcript adapter from this
repository. Build those sources together. Installing a standalone Console release
or replacing its transcript with another renderer does not reproduce Forge.

From the repository root, with Node.js 20+ and npm available:

```bash
umask 077
npm --prefix console ci --ignore-scripts
npm --prefix installer/web ci
npm --prefix installer/web run build
node --test installer/web/build-config.test.mjs
```

The Console installation supplies development tools such as jsdom. Forge declares
its own browser dependencies and pins React and ReactDOM to one installation.
The private umask preserves the reviewed DOM closure's exact file permissions;
the checker authenticates modes as well as bytes. Install into a fresh checkout
if an earlier Console dependency installation used different permissions.
The build graph check bundles the actual application, including the shared chat,
and rejects browser dependencies resolved from a sibling installation.

The Python project selects the reviewed development client
`meerkat-meta==0.9.2+meta.0bc6b4fa` for the Guide. Its hash-pinned wheel and
rebuild provenance are in `vendor/guide/`. This pure Python client adds the
public helper transport-wait option; the Guide's native gateway is the selected
Meta 0.9.3 runtime (Core 0.9.3), admitted by exact version equality.
Packaged products carry the published Meta 0.9.3 client wheel instead. Its pin
in `launchkit/guide_sdk.py` stays pending, and packaging refuses, until that
release publishes.
This is separate from the repository's current Meta release version and from
each application's selected runtime. Updating repository source does not upgrade
an installed application, choose its gateway, or qualify a new SDK/native pair.
Tests for retained predecessor applications also require the historical
`meerkat-mobkit==0.8.42` SDK. That is a test dependency, not a runtime fallback.

## Run the regression checks

Use a source checkout for development, separate from a prepared installation and
its saved state. From the repository root, create a development environment if
one does not already exist. Python 3.12 matches hosted CI:

```bash
python3.12 -m venv installer/.venv
installer/.venv/bin/python -m pip install --no-index --no-deps --require-hashes --find-links installer/vendor/guide -r installer/requirements-guide-sdk.txt
installer/.venv/bin/python -m pip install -e './installer[test]'
```

On Linux, protected acceptance execution also requires `bubblewrap`,
`libseccomp2` and working user, PID, mount and network namespace isolation. The
Forge workflow installs those packages and runs the actual isolation probe
before the suite. A missing or failed sandbox cannot qualify generated-code
execution; keep that failure visible instead of using an unsandboxed fallback.
On Ubuntu, the workflow also loads the distribution's `bwrap-userns-restrict`
AppArmor profile so bubblewrap can prepare its namespaces while its children
remain restricted. See Ubuntu's [user-namespace release notes](https://documentation.ubuntu.com/release-notes/24.04/#unprivileged-user-namespace-restrictions).

Build the frontend with the commands above before running the complete backend
suite. This includes `npm --prefix console ci --ignore-scripts`, which supplies
the exact jsdom dependency closure for protected DOM checks. Some backend cases
also inspect or package the built browser files. Explicitly prepare the private,
hash-pinned Node checker before the suite:

```bash
PYTHONPATH=installer installer/.venv/bin/python -m launchkit.checker_runtime
```

This downloads and verifies the selected checker during preparation. Checks
themselves do not download it, replace the frontend's Node.js or run without
isolation. Use the same test commands as the Forge workflow:

```bash
NODE_OPTIONS=--max-old-space-size=3072 node --test --test-concurrency=2 installer/web/*.test.mjs
installer/.venv/bin/python -m pytest -vv installer/tests --maxfail=10 --tb=short --durations=20 --junitxml=installer/.artifacts/pytest.xml
```

Allow substantial time for the full backend suite. One hosted run reached its
ten-failure limit after 41 minutes; a separate local run of the affected and
remaining cases took another 48 minutes. Those are partial runs on different
machines, not a measured duration for the full suite. The hosted backend job has
a 120-minute limit and reports its 20 slowest tests. `--maxfail=10` stops after
ten failures; it does not sample passing cases. A run stopped by that limit or a
job timeout is incomplete even when its earlier cases passed. Preserve the
failure output and XML before making a correction.

Generated-host wiring checks use the installed Guide SDK, without a private
source intake. Toolkit-specific doctor checks additionally require the selected
product's genuine Toolkit wheel and skip explicitly when it is absent. Passing
these checks establishes SDK wiring with inert runtime fixtures, not a live
native application or compatibility of its persisted state.

For a focused change, run the affected files first, for example:

```bash
installer/.venv/bin/python -m pytest -q installer/tests/test_meta_guide_runtime.py installer/tests/test_visualizer_author_context.py
```

The animation-author tests use the selected Meta dispatcher's typed image
content and native member/session contract with inert runtime fixtures. They do
not establish rendered visual quality or a useful installed result. SDK
byte-installation tests forbid SDK imports during installation/readback, while
preserving genuine SDK modules already imported by the host. Do not replace the
selected SDK with the current repository source merely to make these cases pass.

The reviewed Guide SDK accepts the public `spawn_helper(transport_timeout=...)`
option. Forge passes the time remaining in the existing total 180-second visual
review window, including provisioning and retirement, rather than extending the
native execution budget. Other calls keep the SDK's default 60-second wait. The
dedicated
`test_same_helper_can_correct_invalid_publication_after_sixty_second_transport_default`
case exercises the installed SDK against a controlled transport that models a
late correction. It does not establish a real native helper's execution past
60 seconds. An unknown reply does not prove that native work stopped or
authorize another helper.

## Prepare a runnable product

`start.sh` requires a complete prepared product for Meta startup. A source
checkout and a built frontend are insufficient. The product owner supplies the
selected Guide gateway, SDKs, application runtime and private host configuration.
The host installs the Guide client wheel and Toolkit wheel; the application's
separately selected Meta wheel stays in the product closure for its own managed
environment. Native gateway versions and state directories keep their original
owners. A client SDK change does not migrate native state or replace a gateway.
Keep private settings and saved state outside source archives. Existing prepared
installations retain their own launcher and selected artifacts.

Frontend build checks and an empty browser opening establish source integration
only. Release acceptance still requires a useful installed job, correction,
restart and retained history through the supported product path.
