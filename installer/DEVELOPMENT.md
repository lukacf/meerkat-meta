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

`start.sh` requires a complete prepared product for Meta startup. A source
checkout and a built frontend are insufficient. The product owner supplies the
selected Guide gateway, SDKs, application runtime and private host configuration.
Keep private settings and saved state outside source archives. Existing prepared
installations retain their own launcher and selected artifacts.

Frontend build checks and an empty browser opening establish source integration
only. Release acceptance still requires a useful installed job, correction,
restart and retained history through the supported product path.
