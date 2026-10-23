"""A worker bounds its own stop once its owner, the Forge server, is gone.

Each row runs a real worker process in its own session, as Forge spawns it,
closes its stdin the way Forge's stop or exit does, and keeps the worker's
cleanup from ever finishing.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

INSTALLER = Path(__file__).resolve().parents[1]

DRIVER = r'''
import asyncio, json, os, signal, subprocess, sys
from pathlib import Path
from types import SimpleNamespace
directory, kind, bound = Path(sys.argv[1]), sys.argv[2], float(sys.argv[3])
# A helper in the worker's own group, like the SDK gateway a worker runs.
helper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
from launchkit import owned_worker
owned_worker.WORKER_STOP_TIMEOUT = bound

async def cleanup(*_args, **_kwargs):
    # Ordinary work until the stop; then a native cleanup that either ends at
    # once or holds through every cancellation and never ends.
    try:
        await asyncio.Event().wait()
    finally:
        while kind.endswith("hung"):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                continue

(directory / "helper.json").write_text(json.dumps({"helper": helper.pid}))
if kind.startswith("implementation"):
    from launchkit import implementation_worker as worker
    worker.execute = cleanup
    asyncio.run(worker.run(directory, owned_stdin=True))
elif kind.startswith("rehearsal"):
    from launchkit import rehearsal_worker as worker
    worker.execute = cleanup
    asyncio.run(worker.run(directory, owned_stdin=True))
else:
    from launchkit import deployment, deployment_worker
    def stop_budget(root):
        assert Path(root) == directory
        return bound
    deployment.worker_stop_timeout = stop_budget

    async def main():
        supervisor = SimpleNamespace(stop=asyncio.Event(), directory=directory)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, supervisor.stop.set)
        assert await deployment_worker.owner_input(supervisor) == "owner_lost"
        assert supervisor.stop.is_set()
        await cleanup()
    asyncio.run(main())
'''


def absent(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


def run_worker(tmp_path, kind, bound, *, own_session=True):
    os.chmod(tmp_path, 0o700)
    (tmp_path / "receipt.json").write_text(json.dumps({"status": "queued", "native_console_url": None}))
    os.chmod(tmp_path / "receipt.json", 0o600)
    return subprocess.Popen([sys.executable, "-c", DRIVER, str(tmp_path), kind, str(bound)], cwd=INSTALLER,
        env={**os.environ, "PYTHONPATH": str(INSTALLER)}, stdin=subprocess.PIPE, start_new_session=own_session)


def started(tmp_path, process):
    deadline = time.monotonic() + 20
    while not (tmp_path / "helper.json").exists():
        assert process.poll() is None and time.monotonic() < deadline
        time.sleep(.02)
    time.sleep(.3)  # The worker reads its owner's stdin before ordinary work waits.
    return json.loads((tmp_path / "helper.json").read_text())["helper"]


@pytest.mark.parametrize("kind", ["deployment-hung", "implementation-hung", "rehearsal-hung"])
def test_worker_whose_stop_hangs_after_owner_loss_ends_its_own_group_at_the_bound(tmp_path, kind):
    bound = 2.0
    process = run_worker(tmp_path, kind, bound)
    helper = None
    try:
        helper = started(tmp_path, process)
        process.stdin.close()
        lost = time.monotonic()
        time.sleep(bound - .6)
        # Before its bound the worker keeps its own stop: nothing signalled it.
        assert process.poll() is None and not absent(helper)
        process.wait(timeout=bound + 10)
        elapsed = time.monotonic() - lost
        assert bound - .6 <= elapsed < bound + 5
        # SIGTERM to its own group first, which its stop handler absorbs, then SIGKILL.
        assert process.returncode == -signal.SIGKILL
        deadline = time.monotonic() + 5
        while not absent(helper):
            assert time.monotonic() < deadline, "the worker's own group outlived its bound"
            time.sleep(.02)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        if helper is not None and not absent(helper):
            os.kill(helper, signal.SIGKILL)


def test_worker_whose_stop_settles_after_owner_loss_exits_normally_before_the_bound(tmp_path):
    bound = 5.0
    process = run_worker(tmp_path, "implementation-settles", bound)
    helper = None
    try:
        helper = started(tmp_path, process)
        process.stdin.close()
        lost = time.monotonic()
        process.wait(timeout=bound)
        assert process.returncode == 0 and time.monotonic() - lost < bound
        receipt = json.loads((tmp_path / "receipt.json").read_text())
        assert receipt["status"] == "interrupted"
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        if helper is not None and not absent(helper):
            os.kill(helper, signal.SIGKILL)


def test_process_outside_its_own_session_never_arms_the_owner_loss_bound(tmp_path):
    script = ("import sys, time\nfrom launchkit import owned_worker\n"
              "assert owned_worker.end_after_owner_loss(.2) is None\ntime.sleep(1)\n")
    process = subprocess.Popen([sys.executable, "-c", script], cwd=INSTALLER,
        env={**os.environ, "PYTHONPATH": str(INSTALLER)}, start_new_session=False)
    assert process.wait(timeout=20) == 0
