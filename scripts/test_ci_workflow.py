#!/usr/bin/env python3
"""Focused contracts for the pull-request CI workflow."""

import unittest
import re
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github/workflows/ci.yml"


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
        evicts main's entries from the 10 GB repository cache. Jobs that
        build the same dependency graph (same features, profile and
        dev-dependency use) share one entry; every other job keeps its own.
        """
        expected = {
            "fmt-lint": None,
            "test": None,
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
        for name, shared_key in expected.items():
            step = re.search(
                r"- uses: Swatinem/rust-cache@v2\n((?: {8}.*\n)+)", job_block(name) + "\n"
            )
            self.assertIsNotNone(step, name)
            settings = step.group(1)
            self.assertIn("save-if: ${{ github.ref == 'refs/heads/main' }}", settings, name)
            keys = re.findall(r"shared-key: (\S+)", settings)
            self.assertEqual(keys, [shared_key] if shared_key else [], name)

    def test_workspace_nextest_preserves_all_failure_evidence(self):
        commands = [
            line.strip()
            for line in job_block("test").splitlines()
            if line.strip().startswith("- run: scripts/repo-cargo nextest run --workspace")
        ]

        self.assertEqual(len(commands), 1, commands)
        self.assertIn(" --no-fail-fast", commands[0])

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
                "--test extension_state",
                "scripts/repo-cargo test -p meerkat-meta --locked --features extension-state "
                "--lib extension_",
            ],
        )
        for name in ("extension-state-lint", "test-extension-state"):
            self.assertIn('CARGO_INCREMENTAL: "0"', job_block(name))
            self.assertIn("CARGO_PROFILE_DEV_DEBUG: line-tables-only", job_block(name))

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
            ("- run: node console/browser-e2e.cjs\n", 1),
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
            "- run: node console/browser-e2e.cjs\n",
            "name: console-browser-${{ github.run_id }}\n",
        ]
        positions = [browser.index(marker) for marker in order]
        self.assertEqual(positions, sorted(positions))
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

        A job may be skipped only when `changes` classified the pull request as
        docs-only (FULL is exactly "false"); every skippable job is one the
        classification gates, and every other job must succeed.
        """
        block = job_block("gate")
        needs = block.split("needs: [", 1)[1].split("]", 1)[0]
        listed = {name.strip() for name in needs.split(",")}

        must_gate = {
            "changes",
            "fmt-lint",
            "test",
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

        checks = dict(re.findall(r'(?m)^\s+check ([a-z-]+) "\$\{\{ needs\.\1\.result \}\}" (required|skippable)$', block))
        self.assertEqual(set(checks), listed, "every job in `needs` is checked exactly by name")
        self.assertIn('"$3" == "skippable" && "$2" == "skipped" && "${FULL}" == "false"', block)
        self.assertIn("FULL: ${{ needs.changes.outputs.full }}", block)
        for name, mode in checks.items():
            gated = "    if: needs.changes.outputs.full == 'true'\n" in job_block(name)
            self.assertEqual(mode == "skippable", gated, name)
            if gated:
                self.assertIn("    needs: changes\n", job_block(name))
        self.assertEqual(checks["changes"], "required")

    def test_changes_job_classifies_the_pull_request_and_fails_closed(self):
        block = job_block("changes")
        self.assertIn("fetch-depth: 2", block)
        self.assertIn(
            'python3 scripts/ci-changes.py --event "${{ github.event_name }}" --github-output "$GITHUB_OUTPUT"',
            block,
        )
        self.assertIn('echo "full=true" >> "$GITHUB_OUTPUT"', block)
        self.assertNotIn("if:", block)
        self.assertIn("- run: python3 scripts/test_ci_changes.py\n", job_block("fmt-lint"))


if __name__ == "__main__":
    unittest.main()
