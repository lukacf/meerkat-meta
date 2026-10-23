"""Split the Forge backend suite into CI shards balanced on collected items.

Every forge.yml shard computes the same plan. Each installer test file goes,
largest first by its collected item count, to the shard with the fewest items
so far (ties go to the lower shard, equal files in name order), so the shards
together run every test file exactly once. A collection error fails every
shard: no shard runs a suite that silently lost a file.

Usage: forge_shards.py SHARD COUNT FILES_OUT STARTUP_OUT. FILES_OUT receives
this shard's test files; STARTUP_OUT receives those of them that
checkout_sdk_startup_tests.txt lists, which forge.yml reruns with this
checkout's Meta SDK.
"""
from __future__ import annotations

from pathlib import Path
import re
import subprocess
import sys

TESTS = Path(__file__).resolve().parent
INSTALLER = TESTS.parent
STARTUP_LIST = TESTS / "checkout_sdk_startup_tests.txt"
COLLECTED = re.compile(r"tests/(test_[A-Za-z0-9_]+\.py): ([0-9]+)")


def test_files():
    return sorted(path.name for path in TESTS.glob("test_*.py"))


def collected_items(files):
    """Each test file's collected item count, from pytest's own collection."""
    result = subprocess.run([sys.executable, "-m", "pytest", "--collect-only", "-qq", "-p", "no:cacheprovider", "tests"],
                            cwd=INSTALLER, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        sys.stderr.write(result.stdout[-20000:] + result.stderr[-20000:])
        raise SystemExit(f"The Forge test collection failed (exit {result.returncode}); no shard runs a partial suite.")
    counts = dict.fromkeys(files, 0)
    for line in result.stdout.splitlines():
        match = COLLECTED.fullmatch(line.strip())
        if match:
            if match[1] not in counts:
                raise SystemExit(f"The Forge test collection listed an unknown file: {match[1]}")
            counts[match[1]] = int(match[2])
    if not any(counts.values()):
        raise SystemExit("The Forge test collection listed no test items.")
    return counts


def plan(counts, shards):
    """Assign every file to one of `shards` lists, balancing the item counts."""
    loads, assigned = [0] * shards, [[] for _ in range(shards)]
    for name, items in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        shard = min(range(shards), key=lambda index: (loads[index], index))
        assigned[shard].append(name)
        loads[shard] += items
    return [sorted(names) for names in assigned]


def startup_files():
    names = []
    for line in STARTUP_LIST.read_text().splitlines():
        entry = line.split("#", 1)[0].strip()
        if entry:
            names.append(entry)
    return names


def main(argv):
    if len(argv) != 4:
        raise SystemExit(__doc__)
    shard, shards = int(argv[0]), int(argv[1])
    if not 0 <= shard < shards:
        raise SystemExit(f"Shard {shard} is outside 0..{shards - 1}.")
    files = test_files()
    startup = startup_files()
    unknown = sorted(set(startup) - set(files))
    if unknown:
        raise SystemExit(f"{STARTUP_LIST.name} lists missing test files: {', '.join(unknown)}")
    mine = plan(collected_items(files), shards)[shard]
    Path(argv[2]).write_text("".join(f"installer/tests/{name}\n" for name in mine))
    Path(argv[3]).write_text("".join(f"installer/tests/{name}\n" for name in mine if name in startup))


if __name__ == "__main__":
    main(sys.argv[1:])
