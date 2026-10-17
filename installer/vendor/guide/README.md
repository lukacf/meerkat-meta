# Reviewed Guide client SDK

This directory carries a temporary, pure Python development wheel for Forge's
Guide. It is not a published Meta release, contains no gateway and does not
replace the application's separately selected SDK or native runtime.

- Repository: https://github.com/lukacf/meerkat-meta
- Reviewed branch: `fix/093-py-helper-transport-timeout`
- Exact commit: `0bc6b4fa48051353c4f63517cde6d42fccb6a21a`
- Base: `1791c8e6c`
- `sdk/python` tree: `c6f97032aa934dee2077f4976199f405aacbfec9`
- Local package version: `0.9.2+meta.0bc6b4fa`
- Wheel bytes: 166311
- Wheel SHA-256: `b4a3ad249ca833eb44f086676d34d9504e9f9a503261124b8f5c4025d2101918`
- Sorted member-list SHA-256: `2efbd0105e70b6ea0e6c6df2df671f557503585ef1a66c96c374c10e546be379`

Build from `git archive <exact-commit>:sdk/python` in a fresh directory. In the
build copy only, replace the single `version = "0.9.2"` line in `pyproject.toml`
with `version = "0.9.2+meta.0bc6b4fa"`. With Python 3.12, build 1.6.1 and setuptools
84.0.0 installed in a private virtual environment, run:

```sh
python -m build --no-isolation --wheel --outdir dist /absolute/path/to/build-copy
```

ZIP timestamps can change the outer wheel hash. The 28 member payloads match
the independent producer's reference exactly. To compare rebuilds, hash each
non-directory member, sort by path, concatenate `sha256`, two spaces, path and
a newline for every member, then hash that UTF-8 list. The result must match the
member-list digest above. Select new outer bytes explicitly before changing a
pin; the launcher always checks the committed wheel's exact hash.

The change adds public client transport waits to `spawn_helper` and
`fork_helper`, refusing invalid values before writing a request. It does not
send a native timeout option. Forge uses the remaining total review window.
Timeout leaves the native outcome unknown and does not authorize a retry.

Qualification imports this installed wheel, not SDK source. It covers the
SDK's helper-wait/RPC-method controls and Forge's callback, ownership and late
correction controls. The runtime owner also exercised ephemeral demo helpers
against the published Linux 0.9.0 and accepted-candidate 0.9.1 gateways. Those
checks do not establish installed application usefulness, persisted-state
compatibility, or a real native helper running past 60 seconds. The wheel does
not carry the separate r3 InitWatch EOF fix.

The existing Guide native 0.9.0 selection stays unchanged. Generated applications
keep the exact SDK/native pair in their own selected product. Move to a supported
final SDK/gateway pair through its separate qualification. Never upload this
development wheel to a registry serving published 0.9.2.
