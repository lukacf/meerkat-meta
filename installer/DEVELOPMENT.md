# Forge source development

Forge uses the shared Meta Console components and transcript adapter from this
repository. Build those sources together. Installing a standalone Console release
or replacing its transcript with another renderer does not reproduce Forge.

From the repository root, with Node.js 20+ and npm available:

```bash
npm --prefix console ci --ignore-scripts
npm --prefix installer/web ci
npm --prefix installer/web run build
node --test installer/web/build-config.test.mjs
```

The Console installation supplies development tools such as jsdom. Forge declares
its own browser dependencies and pins React and ReactDOM to one installation.
The build graph check bundles the actual application, including the shared chat,
and rejects browser dependencies resolved from a sibling installation.

The Python project selects `meerkat-meta==0.9.0` for the retained Guide integration.
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
suite. Some backend cases inspect or package those built browser files. Use the
same commands as the Forge workflow:

```bash
NODE_OPTIONS=--max-old-space-size=3072 node --test --test-concurrency=2 installer/web/*.test.mjs
installer/.venv/bin/python -m pytest -vv installer/tests --maxfail=10 --tb=short --junitxml=installer/.artifacts/pytest.xml
```

The full backend suite can take more than 30 minutes. `--maxfail=10` stops after
ten failures; it does not sample passing cases. A run stopped by that limit or a
job timeout is incomplete even when its earlier cases passed. Preserve the
failure output and XML before making a correction.

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

The pinned Guide SDK currently has a known helper-wait limitation: its public
`spawn_helper` call has no transport-timeout override, so its default 60-second
client wait can end before Forge's 180-second visual review window. The dedicated
`test_same_helper_can_correct_invalid_publication_after_sixty_second_transport_default`
case remains enabled as a regression requirement. Closing it requires a public
SDK timeout option, a matching Forge caller and qualification of the selected
SDK. An outer Python timeout does not extend the transport wait. An unknown reply
also does not prove that native work stopped or authorize another helper.

## Prepare a runnable product

`start.sh` requires a complete prepared product for Meta startup. A source
checkout and a built frontend are insufficient. The product owner supplies the
selected Guide gateway, SDKs, application runtime and private host configuration.
Keep private settings and saved state outside source archives. Existing prepared
installations retain their own launcher and selected artifacts.

Frontend build checks and an empty browser opening establish source integration
only. Release acceptance still requires a useful installed job, correction,
restart and retained history through the supported product path.
