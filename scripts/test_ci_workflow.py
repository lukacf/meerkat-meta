#!/usr/bin/env python3
"""Focused contracts for the pull-request CI workflow."""

import fnmatch
import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
import re
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github/workflows/ci.yml"
_CI_CHANGES_SPEC = importlib.util.spec_from_file_location("ci_changes", ROOT / "scripts/ci-changes.py")
ci_changes = importlib.util.module_from_spec(_CI_CHANGES_SPEC)
_CI_CHANGES_SPEC.loader.exec_module(ci_changes)

# The target kinds `cargo test` tests by default. Examples are only built,
# and benches and build scripts are left out, unless a manifest sets their
# test flag.
TESTED_BY_DEFAULT = {"lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro", "bin", "test"}
NOT_TESTED_BY_DEFAULT = {"example", "bench", "custom-build"}


def cargo_workspace_members(root: Path) -> list[dict]:
    """The workspace members as Cargo itself reads them: listed, glob and
    implicit path members alike, with their targets in any valid TOML
    spelling. `cargo metadata --no-deps` resolves nothing and needs no network."""
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--offline", "--format-version", "1"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    metadata = json.loads(result.stdout)
    members = set(metadata["workspace_members"])
    return [package for package in metadata["packages"] if package["id"] in members]


def split_coverage_problems(packages: list[dict], patterns: list[str]) -> list[str]:
    """Why the CI test jobs would not run what `cargo test` runs by default.

    `test` runs --lib --bins and builds --examples, and each integration
    half runs --test <pattern>. Their union is cargo's default selection
    only while every target keeps its kind's default test flag and has no
    required features, and every integration test target matches exactly
    one pattern. Turning autotests off would silently drop the files under
    tests/ from every run, so it is refused too.
    """
    problems = []
    for package in packages:
        manifest = tomllib.loads(Path(package["manifest_path"]).read_text())
        if manifest.get("package", {}).get("autotests") is False:
            problems.append(f"{package['name']}: autotests = false")
        for target in package["targets"]:
            kinds = set(target["kind"])
            label = f"{package['name']} {'/'.join(target['kind'])} {target['name']}"
            if kinds <= TESTED_BY_DEFAULT:
                default = True
            elif kinds <= NOT_TESTED_BY_DEFAULT:
                default = False
            else:
                problems.append(f"{label}: unknown target kind")
                continue
            if target.get("test") is not default:
                problems.append(f"{label}: test flag {target.get('test')}, default {default}")
            if target.get("required-features"):
                problems.append(f"{label}: required-features {target['required-features']}")
            if kinds == {"test"}:
                matched = [pattern for pattern in patterns if fnmatch.fnmatchcase(target["name"], pattern)]
                if len(matched) != 1:
                    problems.append(f"{label}: matches {matched}")
    return problems


def job_block(name: str) -> str:
    """Return exactly one top-level job, bounded by the next job header."""
    lines = CI_WORKFLOW.read_text().splitlines()
    start = lines.index(f"  {name}:")
    for offset, line in enumerate(lines[start + 1 :], start=start + 1):
        if line.startswith("  ") and not line.startswith("   ") and line.endswith(":"):
            return "\n".join(lines[start:offset])
    return "\n".join(lines[start:])


class CiWorkflowTests(unittest.TestCase):
    def test_ci_and_release_use_the_repository_toolchain(self):
        toolchain = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]
        version = toolchain["channel"]
        self.assertRegex(version, r"^\d+\.\d+\.\d+$")
        for name in ("ci.yml", "release.yml", "e2e-live.yml"):
            source = (ROOT / ".github/workflows" / name).read_text()
            installs = re.findall(r"uses:\s*dtolnay/rust-toolchain@(\S+)", source)
            self.assertTrue(installs, f"{name} must install its build toolchain")
            self.assertEqual(set(installs), {version}, name)

    def test_rust_cache_covers_the_wrapper_target(self):
        """rust-cache saves `<workspace>/target`; repo-cargo must build there.

        Without an explicit CARGO_TARGET_DIR, scripts/repo-cargo builds under
        ~/.cache, so every rust-cache step restored and saved a directory
        nothing was built in. Nothing failed - jobs were only slow - so pin the
        single workflow-wide target and refuse remaps that would split the two
        paths again.
        """
        source = CI_WORKFLOW.read_text()
        header = source.split("\njobs:\n", 1)[0]
        self.assertIn("\n  CARGO_TARGET_DIR: ${{ github.workspace }}/target\n", header)
        self.assertEqual(len(re.findall(r"(?m)^\s+CARGO_TARGET_DIR:", source)), 1)
        self.assertIsNone(re.search(r"(?m)^\s+(workspaces|cache-targets):", source))

    def test_rust_cache_saves_only_main_and_shares_identical_graphs(self):
        """Cached build output makes each rust-cache entry about 1 GB.

        Pull request entries are scoped to their own ref, so saving them only
        evicts main's entries from the 20 GB repository cache. Jobs that
        build the same dependency graph (same features, profile and
        dev-dependency use) share one entry; every other job keeps its own.
        """
        expected = {
            # The workspace lint keeps the key it had in fmt-lint, whose
            # main entry holds its build; fmt-lint itself caches nothing.
            "clippy": "fmt-lint",
            "test": "test",
            "test-integration": "test",
            "test-extension-state": None,
            "extension-state-lint": None,
            "test-voice": None,
            "voice-clippy": None,
            "test-python": "meta-dev-bins",
            "flow-editor": "meta-dev-bins",
            "console": "meta-dev-examples",
            "console-browser": "meta-dev-examples",
            "console-fixtures": "meta-dev-examples",
            "console-acceptance": "meta-dev-examples",
        }
        source = CI_WORKFLOW.read_text()
        self.assertEqual(source.count("uses: Swatinem/rust-cache@v2"), len(expected))
        hashed_env = re.compile(r"(?m)^      ((?:CARGO|CC|CFLAGS|CXX|CMAKE|RUST)\w*): (.*)$")
        group_envs = {}
        for name, shared_key in expected.items():
            step = re.search(
                r"- uses: Swatinem/rust-cache@v2\n((?: {8}.*\n)+)", job_block(name) + "\n"
            )
            self.assertIsNotNone(step, name)
            settings = step.group(1)
            self.assertIn("save-if: ${{ github.ref == 'refs/heads/main' }}", settings, name)
            keys = re.findall(r"shared-key: (\S+)", settings)
            self.assertEqual(keys, [shared_key] if shared_key else [], name)
            if shared_key:
                # rust-cache hashes the job environment into the key, so a
                # group shares one entry only while its jobs agree on it.
                # CARGO_INCREMENTAL is 0 in every job either way: the
                # toolchain action and rust-cache both set it before the hash.
                env = [e for e in hashed_env.findall(job_block(name)) if e != ("CARGO_INCREMENTAL", '"0"')]
                group_envs.setdefault(shared_key, set()).add(tuple(env))
            if (shared_key or "").startswith("meta-dev-"):
                self.assertIn("      CARGO_PROFILE_DEV_DEBUG: line-tables-only\n", job_block(name), name)
        for shared_key, envs in group_envs.items():
            self.assertEqual(len(envs), 1, f"{shared_key}: {envs}")
        header = source.split("\njobs:\n", 1)[0]
        self.assertIn("\npermissions:\n  contents: read\n", header)
        # Jobs that run pull request code stay read-only. Only the cleanup
        # job may delete caches: it runs for pushes to main alone, after every
        # job that saves a rust cache, and drops exactly their families.
        self.assertEqual(re.findall(r"(?m)^ +permissions:$", source), ["    permissions:"])
        self.assertEqual(re.findall(r"(?m)^ +actions: write$", source), ["      actions: write"])
        drop = job_block("drop-superseded-caches")
        self.assertIn("    if: github.event_name == 'push' && github.ref == 'refs/heads/main'\n", drop)
        self.assertIn("    continue-on-error: true\n", drop)
        self.assertIn("    permissions:\n      actions: write\n      contents: read\n", drop)
        needs = re.search(r"(?m)^    needs: \[(.*)\]$", drop).group(1).split(", ")
        self.assertEqual(sorted(needs), sorted(expected))
        families = []
        for name, shared_key in expected.items():
            if (shared_key or name) not in families:
                families.append(shared_key or name)
        actions = [line.strip() for line in drop.splitlines() if re.match(r" +(- )?(uses|run): ", line)]
        self.assertEqual(
            actions,
            ["- uses: actions/checkout@v4", f"run: scripts/ci-drop-superseded-caches.sh {' '.join(families)}"],
        )
        self.assertNotIn("drop-superseded-caches", job_block("gate"))
        self.assertNotIn("steps.rust-cache", source)
        self.assertIn("- run: python3 scripts/test_ci_drop_caches.py\n", job_block("fmt-lint"))

    def test_workspace_nextest_preserves_all_failure_evidence(self):
        """The workspace tests run as three jobs that together cover cargo's
        default test selection: lib and bin unit tests in `test`, and the
        integration tests in two `test-integration` halves split by target
        name, one of which also builds the examples. All use the same filter
        and --no-fail-fast. split_coverage_problems holds the conditions under
        which that union is the default selection, on Cargo's own reading of
        every workspace member."""
        block = job_block("test-integration")
        patterns = re.findall(r'(?m)^          - tests: "([^"]+)"$', block)
        self.assertEqual(len(patterns), 2, patterns)
        members = cargo_workspace_members(ROOT)
        integration_tests = [t for package in members for t in package["targets"] if t["kind"] == ["test"]]
        self.assertGreater(len(integration_tests), 100)
        self.assertEqual(split_coverage_problems(members, patterns), [])
        self.assertIn("    strategy:\n      fail-fast: false\n", block)
        filter_ = (
            "-E 'not test(governance_contracts) and not test(converged_idle_gateway_consumes_near_zero_cpu) "
            "and not test(live_event_burst_reaches_store_while_slow_backfill_is_running)'"
        )
        expected = {
            "test": f"scripts/repo-cargo nextest run --workspace --lib --bins {filter_} --no-fail-fast",
            "test-integration": (
                f"scripts/repo-cargo nextest run --workspace --test '${{{{ matrix.tests }}}}' {filter_} --no-fail-fast"
            ),
        }
        for name, command in expected.items():
            commands = [
                line.strip()[len("- run: "):]
                for line in job_block(name).splitlines()
                if line.strip().startswith("- run: scripts/repo-cargo nextest run")
            ]
            self.assertEqual(commands, [command], name)
            self.assertIn('CARGO_PROFILE_DEV_DEBUG: "0"', job_block(name))
            # Every disk report runs last, whatever failed before it. The
            # block ends at the next job, after the comments that precede it.
            lines = job_block(name).splitlines()
            while lines and (not lines[-1].strip() or lines[-1].lstrip().startswith("#")):
                lines.pop()
            self.assertEqual(lines[-3:], ["      - name: Report runner disk", "        if: always()",
                                          "        run: df -h /"], name)
        # The unit build fits the runner's free disk and reclaims nothing;
        # the integration halves reclaim in the background, never on the
        # critical path.
        self.assertNotIn("rm -rf", job_block("test"))
        reclaim = re.search(r"(?m)^      - name: Reclaim runner disk in the background\n        run: \|\n"
                            r"((?:          .*\n)+)", block + "\n")
        self.assertIsNotNone(reclaim)
        self.assertRegex(reclaim.group(1), r"^          sudo -n nohup ionice -c2 -n7 bash -c 'rm -rf ")
        self.assertRegex(reclaim.group(1), r"docker image prune -af' >/dev/null 2>&1 &\n          df -h /\n$")
        self.assertLess(block.index("Reclaim runner disk in the background"), block.index("actions/checkout"))
        # Exactly one half builds the examples.
        self.assertEqual(block.count("            examples: true\n"), 1)
        self.assertIn("      - if: matrix.examples\n        run: scripts/repo-cargo build --workspace --examples\n", block)
        source = CI_WORKFLOW.read_text()
        self.assertEqual(source.count("scripts/repo-cargo build --workspace --examples"), 1)
        self.assertEqual(source.count("scripts/repo-cargo nextest run --workspace"), 2)

    def test_the_split_coverage_guard_reads_targets_as_cargo_does(self):
        """Targets, test flags and autotests in any valid TOML spelling, and
        glob or implicit path members, all reach the guard: it reads `cargo
        metadata` and parses manifests as TOML, never the manifest text."""
        package = '[package]\nname = "{}"\nversion = "0.0.0"\nedition = "2021"\n'
        probe = package.format("guard-probe")
        cases = {
            "spaces inside the brackets": (
                {
                    "Cargo.toml": probe + '\n[[ test ]]\nname = "_extra_contract"\npath = "extra/contract.rs"\n\n[workspace]\n',
                    "extra/contract.rs": "",
                },
                ["guard-probe test _extra_contract: matches []"],
            ),
            "quoted keys": (
                {
                    "Cargo.toml": probe + '\n[["test"]]\n"name" = "_quoted_contract"\n"path" = "extra/q.rs"\n\n[workspace]\n',
                    "extra/q.rs": "",
                },
                ["guard-probe test _quoted_contract: matches []"],
            ),
            "an inline array of tables": (
                {
                    "Cargo.toml": 'test = [{ name = "_inline_contract", path = "extra/i.rs" }]\n\n' + probe + "\n[workspace]\n",
                    "extra/i.rs": "",
                },
                ["guard-probe test _inline_contract: matches []"],
            ),
            "a quoted test flag on a bin": (
                {
                    "Cargo.toml": probe + '\n[[bin]]\nname = "probe"\n"test" = false\n\n[workspace]\n',
                    "src/bin/probe.rs": "fn main() {}\n",
                },
                ["guard-probe bin probe: test flag False, default True"],
            ),
            "an inline test flag on an example": (
                {
                    "Cargo.toml": 'example = [{ name = "demo", test = true }]\n\n' + probe + "\n[workspace]\n",
                    "examples/demo.rs": "fn main() {}\n",
                },
                ["guard-probe example demo: test flag True, default False"],
            ),
            "autotests off through dotted keys": (
                {
                    "Cargo.toml": (
                        'package.name = "guard-probe"\npackage.version = "0.0.0"\npackage.edition = "2021"\n'
                        "package.autotests = false\n\n[workspace]\n"
                    ),
                    "tests/alpha.rs": "",
                },
                ["guard-probe: autotests = false"],
            ),
            "a glob member": (
                {
                    "Cargo.toml": '[workspace]\nmembers = ["crates/*"]\nresolver = "2"\n',
                    "crates/c/Cargo.toml": package.format("c"),
                    "crates/c/src/lib.rs": "",
                    "crates/c/tests/Upper.rs": "",
                },
                ["c test Upper: matches []"],
            ),
            "an implicit path member": (
                {
                    "Cargo.toml": '[workspace]\nmembers = ["a"]\nresolver = "2"\n',
                    "a/Cargo.toml": package.format("a") + '\n[dependencies]\nb = { path = "../b" }\n',
                    "a/src/lib.rs": "",
                    "b/Cargo.toml": package.format("b"),
                    "b/src/lib.rs": "",
                    "b/tests/_hidden.rs": "",
                },
                ["b test _hidden: matches []"],
            ),
            "required features": (
                {
                    "Cargo.toml": probe + (
                        '\n[features]\nslow = []\n\n[[test]]\nname = "gated"\npath = "extra/gated.rs"\n'
                        'required-features = ["slow"]\n\n[workspace]\n'
                    ),
                    "extra/gated.rs": "",
                },
                ["guard-probe test gated: required-features ['slow']"],
            ),
            # Control: explicit and discovered targets the patterns cover pass.
            "covered targets": (
                {
                    "Cargo.toml": probe + '\n[[test]]\nname = "middle"\npath = "extra/middle.rs"\n\n[workspace]\n',
                    "extra/middle.rs": "",
                    "tests/zeta.rs": "",
                },
                [],
            ),
        }
        for case, (files, expected) in cases.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                for relative, content in {"src/lib.rs": "", **files}.items():
                    path = Path(tmp) / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(content)
                members = cargo_workspace_members(Path(tmp))
                self.assertEqual(split_coverage_problems(members, ["[a-i]*", "[j-z]*"]), expected)

    def test_decorator_authority_gate_actually_runs_in_ci(self):
        """A structural gate nobody invokes is a control that cannot fail.

        `verify-decorator-authority.py` exists because a decorator omitting
        `request_attempt_authority` compiles clean, warns nothing, and stranded
        72 identities downstream. If it is ever dropped from `fmt-lint` the
        defect class goes unwatched again with no other signal, so pin the
        invocation here rather than trusting that the file's presence means it
        runs.
        """
        block = job_block("fmt-lint")
        self.assertIn(
            "- run: python3 scripts/verify-decorator-authority.py",
            block,
        )

    def test_the_workspace_lint_runs_in_its_own_rust_lane_job(self):
        """fmt-lint runs on every pull request and compiles nothing; the
        workspace clippy runs once, in the clippy job, when a Rust change
        reaches it, beside fmt-lint instead of after its contract tests."""
        source = CI_WORKFLOW.read_text()
        command = "- run: scripts/repo-cargo clippy --workspace --all-targets -- -D warnings\n"
        self.assertEqual(source.count(command), 1)
        lint = job_block("clippy")
        self.assertIn(command, lint)
        self.assertIn("    needs: changes\n    if: needs.changes.outputs.rust == 'true'\n", lint)
        self.assertIn("          components: clippy\n", lint)
        fmt_lint = "\n".join(line for line in job_block("fmt-lint").splitlines()
                             if not line.strip().startswith("#"))
        self.assertNotIn("needs:", fmt_lint)
        self.assertNotIn(" if:", fmt_lint)
        self.assertNotIn("repo-cargo clippy", fmt_lint)
        self.assertIn("          components: rustfmt\n", fmt_lint)
        self.assertNotIn("rust-cache", fmt_lint)
        for kept in ("- run: scripts/repo-cargo fmt --all -- --check\n",
                     "- run: node scripts/generate-bazel-rust-builds.mjs --check\n",
                     "- run: scripts/repo-cargo machete --with-metadata\n"):
            self.assertIn(kept, fmt_lint)

    def test_typescript_sdk_suite_actually_runs_in_ci(self):
        """705 TypeScript tests existed and gated nothing.

        No job ran `sdk/typescript` at all: the only Makefile reference is
        `publish-dry-run-typescript`, which builds and packs without testing.
        So a wrong RPC method name, or a parser reading camelCase where the
        gateway sends snake_case, reached npm behind a typecheck that cannot
        see either. Pin the invocation rather than the job's existence - a job
        that stops running its tests looks identical to one that passes.

        `validate` and not `test`: most of these tests import the built
        `dist/`, so `test` alone fails with ERR_MODULE_NOT_FOUND.
        """
        block = job_block("test-typescript")
        self.assertIn(
            "- run: npm --prefix sdk/typescript run validate --silent",
            block,
        )

    def test_extension_state_lint_and_tests_run_as_parallel_jobs(self):
        """check and clippy only emit metadata; the tests need full builds.

        Every command of the former serial job still runs exactly once,
        split between the two jobs.
        """
        def runs(name):
            return [
                line.strip()[len("- run: "):]
                for line in job_block(name).splitlines()
                if line.strip().startswith("- run: ")
            ]

        self.assertEqual(
            runs("extension-state-lint"),
            [
                "scripts/repo-cargo check -p meerkat-meta --locked --no-default-features",
                "scripts/repo-cargo clippy -p meerkat-meta --locked --features extension-state "
                "--lib --test extension_state -- -D warnings",
            ],
        )
        self.assertEqual(
            runs("test-extension-state"),
            [
                "scripts/repo-cargo test -p meta-extension-state --locked",
                "scripts/repo-cargo test -p meerkat-meta --locked --features extension-state "
                "--lib --test extension_state --no-run",
                "scripts/repo-cargo test -p meerkat-meta --locked --features extension-state "
                "--test extension_state",
                "scripts/repo-cargo test -p meerkat-meta --locked --features extension-state "
                "--lib -- extension_ document_mcp_child_supply",
            ],
        )
        for name in ("extension-state-lint", "test-extension-state"):
            self.assertIn('CARGO_INCREMENTAL: "0"', job_block(name))
            self.assertIn("CARGO_PROFILE_DEV_DEBUG: line-tables-only", job_block(name))

    def test_extension_state_test_targets_build_together_before_they_run(self):
        """The integration test and the --lib harness are separate compilations
        of meerkat-meta; one `--no-run` invocation builds both in parallel, so
        it must name every target the later meerkat-meta steps run, with the
        same package, features and flags, and come before them."""
        steps = [
            line.strip()[len("- run: "):]
            for line in job_block("test-extension-state").splitlines()
            if line.strip().startswith("- run: scripts/repo-cargo test -p meerkat-meta ")
        ]
        build, *runs = steps
        self.assertTrue(build.endswith(" --no-run"), build)
        prefix = "scripts/repo-cargo test -p meerkat-meta --locked --features extension-state "
        self.assertTrue(all(step.startswith(prefix) for step in steps), steps)
        built = set(re.findall(r"--(lib|test \S+)", build.split(" -- ")[0]))
        ran = {target for step in runs for target in re.findall(r"--(lib|test \S+)", step.split(" -- ")[0])}
        self.assertEqual(built, ran)

    def test_no_job_caps_build_parallelism(self):
        """A CARGO_BUILD_JOBS cap throttled the dependency graph of the four
        jobs that carried it. The dev-profile jobs run uncapped, like the
        rest of the workflow."""
        code = [line for line in CI_WORKFLOW.read_text().splitlines() if not line.strip().startswith("#")]
        self.assertEqual([line for line in code if "CARGO_BUILD_JOBS" in line], [])
        self.assertEqual([line for line in code if re.search(r"repo-cargo .*(\s-j\s*\d|--jobs)", line)], [])

    def test_voice_feature_runs_real_host_tests_and_strict_lint(self):
        block = job_block("test-voice")
        self.assertIn("--locked --features openai-live", block)
        self.assertIn("--test console_route_auth --test console_experience --test never_executed_persistence --test swarm_integration console_voice", block)
        self.assertIn("gateway_openai_live_registration", block)
        self.assertIn('CARGO_INCREMENTAL: "0"', block)
        # The strict lint runs in parallel, in its own job, on the same features.
        runs = [line for line in block.splitlines() if line.strip().startswith("- run:")]
        self.assertFalse([line for line in runs if " clippy " in line], runs)
        lint = job_block("voice-clippy")
        self.assertEqual(
            lint.count(
                "- run: scripts/repo-cargo clippy -p meerkat-meta --locked --features openai-live "
                "--lib --bin rpc_gateway --bin meta_gateway -- -D warnings\n"
            ),
            1,
        )
        self.assertIn('CARGO_INCREMENTAL: "0"', lint)

    def test_voice_lane_runs_the_live_registration_test_from_the_library(self):
        # src/bin/rpc_gateway.rs is a one-line wrapper around
        # meerkat_meta::rpc_gateway::main(), and the registration test lives in
        # the library module. A `--bin rpc_gateway` selector compiles the
        # wrapper and runs zero tests, so the step passed without testing.
        command = (
            "scripts/repo-cargo test -p meerkat-meta --locked "
            "--features openai-live --lib gateway_openai_live_registration"
        )
        block = job_block("test-voice")
        self.assertEqual(block.count(f"- run: {command}\n"), 1)
        self.assertNotIn("--bin rpc_gateway gateway_openai_live_registration", block)
        wrapper = (ROOT / "crates/meerkat-meta/src/bin/rpc_gateway.rs").read_text()
        self.assertNotIn("gateway_openai_live_registration", wrapper)
        library = (ROOT / "crates/meerkat-meta/src/rpc_gateway.rs").read_text()
        self.assertIn("fn gateway_openai_live_registration_is_explicit_and_strict()", library)

    def test_voice_lane_selects_every_persistent_http_restart_case(self):
        # Default workspace tests cannot see the feature-gated voice cases.
        # Select their integration binary and shared filter in the existing
        # voice lane, which already compiles the gateway with openai-live.
        command = (
            "scripts/repo-cargo test -p meerkat-meta --locked "
            "--features openai-live --test meta_gateway_bootstrap "
            "persistent_http_restarts"
        )
        block = job_block("test-voice")
        self.assertEqual(block.count(f"- run: {command}\n"), 1)
        source = (ROOT / "crates/meerkat-meta/tests/meta_gateway_bootstrap.rs").read_text()
        self.assertEqual(
            set(re.findall(r"fn (persistent_http_restarts_\w+)\(", source)),
            {
                "persistent_http_restarts_classic_without_voice",
                "persistent_http_restarts_identity_without_voice",
                "persistent_http_restarts_classic_with_voice",
                "persistent_http_restarts_identity_with_voice",
            },
        )

    def test_console_checks_live_voice_evidence_without_paid_calls(self):
        block = job_block("console")
        self.assertIn("node console/voice-e2e-live.cjs --self-test\n", block)
        self.assertIn("node console/voice-e2e-live.cjs --self-test-audio\n", block)
        self.assertNotIn("run e2e:voice:live", block)

    def test_console_fixture_unit_regressions_run_with_the_shared_backend_build(self):
        block = job_block("console")
        command = "scripts/repo-cargo test -p meerkat-meta --example console_acceptance_fixture"
        self.assertEqual(block.count(command), 1)
        self.assertLess(block.index("Build console acceptance backends"), block.index(command))
        self.assertLess(block.index(command), block.index("node console/api-e2e.cjs"))

    def test_console_browser_and_perf_run_beside_console(self):
        """The two longest console steps run in their own jobs, each exactly once.

        perf:typing:browser bundles its own host and needs no Rust. The
        browser scenarios need the acceptance backends and freshly built
        console bundles, in that order.
        """
        source = CI_WORKFLOW.read_text()
        for command, count in (
            ("- run: npm --prefix console run perf:typing:browser --silent\n", 1),
            ("node console/browser-e2e.cjs", 1),
            ("run: npm --prefix console run e2e:mcp-apps --silent\n", 2),
            ("- run: node console/api-e2e.cjs\n", 1),
        ):
            self.assertEqual(source.count(command), count, command)

        perf = job_block("console-perf")
        self.assertIn("- run: npm --prefix console run perf:typing:browser --silent\n", perf)
        self.assertIn("playwright install --with-deps chromium", perf)
        self.assertNotIn("repo-cargo", perf)

        browser = job_block("console-browser")
        order = [
            "- run: npm --prefix console run build --silent\n",
            "- run: npm --prefix console run build:acceptance --silent\n",
            "playwright install --with-deps chromium",
            "- name: Build console acceptance backends\n",
            "- name: Native MCP Apps rendering and reload\n",
            "- name: Native MCP Apps polling during an active model turn\n",
            "- run: node console/browser-e2e.cjs --shard=${{ matrix.shard }}/3\n",
            "name: console-browser-${{ github.run_id }}-${{ matrix.shard }}\n",
        ]
        positions = [browser.index(marker) for marker in order]
        self.assertEqual(positions, sorted(positions))
        # Three legs, each dealt its own scenarios by the registry's
        # --shard=i/3; the matrix and the shard count must agree. MCP Apps
        # run once.
        legs = re.search(r"(?m)^        shard: \[([0-9, ]+)\]$", browser).group(1).split(", ")
        self.assertEqual(legs, [str(index) for index in range(1, 4)])
        self.assertIn(f"--shard=${{{{ matrix.shard }}}}/{len(legs)}\n", browser)
        self.assertIn("      fail-fast: false\n", browser)
        self.assertEqual(browser.count("        if: matrix.shard == 1\n"), 2)
        self.assertIn("META_EXAMPLE_BIN_DIR=${target_dir}/debug/examples", browser)

        console = job_block("console")
        for kept in (
            "- run: node console/api-e2e.cjs\n",
            "- run: node console/voice-e2e.cjs\n",
            "- run: node console/memory-e2e.cjs\n",
            "- run: node console/workgraph-e2e.cjs\n",
            "- run: npm --prefix console run embedded:freshness --silent\n",
            "name: console-${{ github.run_id }}\n",
        ):
            self.assertIn(kept, console)

    def test_gate_requires_every_job_it_lists(self):
        """A job absent from `gate` can fail without failing the check suite.

        `needs` alone does not gate: with `if: always()` the gate runs
        regardless, so a job missing from the result checks is advisory only.

        Correspondence ALONE is not enough, and this test asserted only that
        until review caught it: deleting a job from `needs` and from the
        checks together keeps the two lists in agreement, so the job goes on
        running and gates nothing while this test stays green. The roster
        below is therefore a floor - every gating job must be present - and the
        correspondence check runs on top of it.

        A job in a lane may be skipped only when `changes` set that lane to
        exactly "false"; every such job is one the same lane output gates,
        the lanes and their jobs are the ones scripts/ci-changes.py
        classifies, and every other job must succeed.
        """
        block = job_block("gate")
        needs = block.split("needs: [", 1)[1].split("]", 1)[0]
        listed = {name.strip() for name in needs.split(",")}

        must_gate = {
            "changes",
            "fmt-lint",
            "clippy",
            "test",
            "test-integration",
            "test-extension-state",
            "extension-state-lint",
            "test-voice",
            "voice-clippy",
            "test-python",
            "test-typescript",
            "console",
            "console-browser",
            "console-perf",
            "console-fixtures",
            "console-acceptance",
            "flow-editor",
            "audit",
        }
        missing = must_gate - listed
        self.assertFalse(
            missing,
            f"these jobs must gate the suite and are absent from `needs`: "
            f"{sorted(missing)}. Removing a job from both lists keeps them "
            f"consistent while silently making it advisory.",
        )

        checks = dict(re.findall(r'(?m)^\s+check ([a-z-]+) "\$\{\{ needs\.\1\.result \}\}" (required|[a-z_]+)$', block))
        self.assertEqual(set(checks), listed, "every job in `needs` is checked exactly by name")
        self.assertIn('"$3" != "required" && "$2" == "skipped" && "${lane_value}" == "false"', block)
        self.assertIn('local variable="LANE_${3^^}"', block)
        for lane in ci_changes.LANES:
            self.assertIn(f"LANE_{lane.upper()}: ${{{{ needs.changes.outputs.{lane} }}}}\n", block)
        lanes = {}
        for name, mode in checks.items():
            gate = re.findall(r"(?m)^    if: needs\.changes\.outputs\.([a-z_]+) == 'true'$", job_block(name))
            if mode == "required":
                self.assertEqual(gate, [], name)
                continue
            self.assertEqual(gate, [mode], name)
            self.assertIn("    needs: changes\n", job_block(name))
            lanes.setdefault(mode, set()).add(name)
        # The workflow's lanes are exactly the classifier's, job for job.
        self.assertEqual(lanes, {lane: set(jobs) for lane, jobs in ci_changes.LANE_JOBS.items()})
        self.assertEqual(checks["changes"], "required")

    def run_gate(self, results: dict[str, str], lanes: dict[str, str]) -> subprocess.CompletedProcess:
        """Run the gate step's shell with job results substituted the way
        GitHub renders them and the lane outputs in its environment."""
        lines = job_block("gate").splitlines()
        start = lines.index("        run: |") + 1
        body = "\n".join(line[10:] for line in lines[start:] if line.startswith("          ") or not line)
        script = re.sub(
            r"\$\{\{ needs\.([a-z-]+)\.result \}\}",
            lambda match: results.get(match.group(1), "success"),
            body,
        )
        env = {key: value for key, value in os.environ.items() if not key.startswith("LANE_")}
        env.update({f"LANE_{lane.upper()}": value for lane, value in lanes.items()})
        return subprocess.run(["bash", "-e", "-c", script], env=env, capture_output=True, text=True)

    def test_gate_accepts_a_skip_only_for_a_lane_classified_false(self):
        every = {lane: "true" for lane in ci_changes.LANES}
        console_only = {lane: ("true" if lane in ("console", "console_perf", "flow_editor") else "false")
                        for lane in ci_changes.LANES}
        rust_jobs = {job: "skipped" for job in ci_changes.LANE_JOBS["rust"]}
        cases = (
            ("all green", {}, every, 0),
            ("rust lane skipped when classified false", rust_jobs | {"test-integration": "skipped",
                                                                      "test-python": "skipped"}, console_only, 0),
            ("a skipped job whose lane is true", {"test-voice": "skipped"}, every, 1),
            ("a skipped job of a running lane", {"console-perf": "skipped"}, console_only, 1),
            ("a failed job in a skipped lane", {"test": "failure"}, console_only, 1),
            ("lanes missing: classification failed", rust_jobs, {}, 1),
            ("lanes empty", rust_jobs, {lane: "" for lane in ci_changes.LANES}, 1),
            ("a required job skipped", {"fmt-lint": "skipped"}, console_only, 1),
            ("changes failed", {"changes": "failure"}, every, 1),
            ("the workspace lint skipped while rust runs", {"clippy": "skipped"}, every, 1),
        )
        for name, results, lanes, status in cases:
            with self.subTest(case=name):
                result = self.run_gate(results, lanes)
                self.assertEqual(result.returncode, status, result.stdout + result.stderr)

    def test_every_job_builds_the_commit_its_event_names(self):
        """Main's caches are restored by later main runs, whose result is the
        evidence release.yml accepts, so only main's own code may write them:
        no dispatch input and no checkout ref can point a job at another
        commit."""
        source = CI_WORKFLOW.read_text()
        triggers = source.split("\npermissions:", 1)[0]
        self.assertIn("  workflow_dispatch:\n", triggers)
        self.assertNotIn("inputs:", triggers)
        # Any version or pinned SHA of the checkout action.
        uses = re.findall(r"(?m)^\s*-?\s*uses:\s*actions/checkout@\S+", source)
        checkouts = re.findall(r"(?m)^ *- uses: actions/checkout@\S+\n((?: {8,}.*\n)*)", source)
        self.assertTrue(uses)
        self.assertEqual(len(checkouts), len(uses))
        for settings in checkouts:
            self.assertNotIn("ref:", settings)
            self.assertNotIn("repository:", settings)
        # Nor may a run step fetch or move to another commit; reading the
        # base commit's classifier (git show) is the only git use.
        code = "\n".join(line for line in source.splitlines() if not line.strip().startswith("#"))
        self.assertEqual(re.findall(r"\bgit\s+(fetch|checkout|switch|reset|pull|clone|worktree|restore)\b", code), [])

    def test_the_no_redirect_check_sees_every_checkout_and_git_use(self):
        probe = """
      - uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683
        with:
          ref: ${{ github.event.inputs.sha }}
      - run: git fetch origin pull/1/head && git checkout FETCH_HEAD
"""
        self.assertEqual(len(re.findall(r"(?m)^ *- uses: actions/checkout@\S+\n((?: {8,}.*\n)*)", probe)), 1)
        self.assertIn("ref:", re.findall(r"(?m)^ *- uses: actions/checkout@\S+\n((?: {8,}.*\n)*)", probe)[0])
        self.assertEqual(re.findall(r"\bgit\s+(fetch|checkout|switch|reset|pull|clone|worktree|restore)\b", probe),
                         ["fetch", "checkout"])

    def test_changes_job_classifies_the_pull_request_and_fails_closed(self):
        block = job_block("changes")
        self.assertIn("fetch-depth: 2", block)
        # Every lane is an output.
        outputs = re.findall(r"(?m)^      ([a-z_]+): \$\{\{ steps\.classify\.outputs\.\1 \}\}$", block)
        self.assertEqual(outputs, list(ci_changes.LANES))
        self.assertIn(f'          lanes="{" ".join(ci_changes.LANES)}"\n', block)
        self.assertIn("git show HEAD^1:scripts/ci-changes.py", block)
        self.assertNotIn("python3 scripts/ci-changes.py", block)
        self.assertNotIn("if:", block)
        self.assertIn("- run: python3 scripts/test_ci_changes.py\n", job_block("fmt-lint"))

    def run_classify_step(self, base_classifier: str | None, *, head_classifier: str | None = None,
                          event: str = "pull_request") -> dict[str, str]:
        """Run the classify step in a repository whose base commit holds
        `base_classifier` (None: no classifier at all) and whose head may
        change it; return the step's GITHUB_OUTPUT as a mapping."""
        lines = job_block("changes").splitlines()
        start = lines.index("      - id: classify")
        run = lines.index("        run: |", start)
        body = []
        for line in lines[run + 1:]:
            if line and not line.startswith("          "):
                break
            body.append(line[10:] if line else "")
        env = {key: value for key, value in os.environ.items() if not key.startswith("GIT")}
        env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                   GIT_AUTHOR_NAME="ci", GIT_AUTHOR_EMAIL="ci@example.invalid",
                   GIT_COMMITTER_NAME="ci", GIT_COMMITTER_EMAIL="ci@example.invalid")
        with tempfile.TemporaryDirectory() as tmp:
            repo, temp = Path(tmp) / "repo", Path(tmp) / "runner-temp"
            repo.mkdir()
            temp.mkdir()
            output = Path(tmp) / "output"

            def git(*args):
                subprocess.run(["git", *args], cwd=repo, env=env, check=True, capture_output=True)

            git("init", "-q")
            (repo / "scripts").mkdir()
            for index, classifier in enumerate(
                    (base_classifier, head_classifier if head_classifier is not None else base_classifier)):
                if classifier is not None:
                    (repo / "scripts/ci-changes.py").write_text(classifier)
                (repo / "touched.txt").write_text(f"commit {index}\n")
                git("add", "-A")
                git("commit", "-q", "-m", "tree")
            env.update(EVENT=event, RUNNER_TEMP=str(temp), GITHUB_OUTPUT=str(output))
            subprocess.run(["bash", "-e", "-c", "\n".join(body)], cwd=repo, env=env, check=True,
                           capture_output=True, text=True)
            return dict(line.split("=", 1) for line in output.read_text().splitlines())

    def test_the_base_commits_classifier_decides_and_fails_closed(self):
        every = {lane: "true" for lane in ci_changes.LANES}
        nothing_runs = ("import sys\nargs = sys.argv\nopen(args[args.index('--github-output') + 1], 'a')"
                        ".write(''.join(f'{lane}=false\\n' for lane in "
                        f"{list(ci_changes.LANES)!r}))\n")
        # Pushes and dispatches run every lane even if a classifier says none.
        for event in ("push", "workflow_dispatch", "schedule"):
            with self.subTest(event=event):
                self.assertEqual(self.run_classify_step(nothing_runs, event=event), every)

        def printing(values: dict[str, str]) -> str:
            text = "".join(f"{key}={value}\\n" for key, value in values.items())
            return ("import sys\nargs = sys.argv\nopen(args[args.index('--github-output') + 1], 'a')"
                    f".write('{text}')\n")

        console_only = {lane: ("true" if lane == "console" else "false") for lane in ci_changes.LANES}
        self.assertEqual(self.run_classify_step(printing(console_only)), console_only)
        # The pull request's own classifier never decides its lanes.
        nothing = {lane: "false" for lane in ci_changes.LANES}
        self.assertEqual(self.run_classify_step(printing(console_only), head_classifier=printing(nothing)),
                         console_only)
        for case, base in (("predates the lanes", printing({"full": "false"})),
                           ("misses a lane", printing({k: v for k, v in console_only.items() if k != "python"})),
                           ("fails", "raise SystemExit(3)\n"),
                           ("is absent", None)):
            with self.subTest(base=case):
                self.assertEqual(self.run_classify_step(base), every)

if __name__ == "__main__":
    unittest.main()
