#!/usr/bin/env python3
"""Run the lock-discovery part of ci.yml's fixture provenance step offline.

The pr-static step "Verify protocol fixture copy provenance" must verify every
tracked intent-hq/intent fixture lock. Discovery is the shell before
`source_root=`; it runs here against throwaway Git repositories, so a lock
discovery error, including one after five valid rows, has to fail the step.
"""

import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]
STEP = "Verify protocol fixture copy provenance"
SHA = "0123456789abcdef0123456789abcdef01234567"


def discovery_script():
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    steps = [s for s in workflow["jobs"]["pr-static"]["steps"] if s.get("name") == STEP]
    if len(steps) != 1:
        raise AssertionError(f"expected one pr-static step named {STEP!r}")
    run = steps[0]["run"]
    head, sep, _ = run.partition("\nsource_root=")
    if not sep:
        raise AssertionError("the step no longer has a source_root= line after discovery")
    return head + '\nprintf "%s\\n" "${locks[@]}"\n'


def lock(commit=SHA, repository="intent-hq/intent", source="docs/protocol/fixtures/x.json"):
    return {"lockVersion": 1, "sourceRepository": repository, "sourcePath": source, "sourceCommit": commit}


class LockDiscoveryTest(unittest.TestCase):
    def run_discovery(self, locks):
        with tempfile.TemporaryDirectory() as tmp:
            subprocess.run(["git", "init", "-q", tmp], check=True)
            for name, body in locks.items():
                path = Path(tmp, name)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(body) + "\n")
            subprocess.run(["git", "-C", tmp, "add", "-A"], check=True)
            return subprocess.run(["bash", "-c", discovery_script()], cwd=tmp, capture_output=True, text=True,
                                  stdin=subprocess.DEVNULL)

    def valid(self, count):
        return {f"crates/c{i}/tests/fixtures/f{i}.lock.json": lock() for i in range(count)}

    def test_lists_every_intent_lock_and_skips_other_repositories(self):
        locks = {**self.valid(5), "other.lock.json": lock(repository="someone/else"), "package-lock.json": {}}
        result = self.run_discovery(locks)
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = result.stdout.splitlines()
        self.assertEqual(len(rows), 5)
        self.assertTrue(all(row.endswith(f" {SHA} docs/protocol/fixtures/x.json") for row in rows), rows)

    def test_a_discovery_error_after_five_valid_rows_fails(self):
        locks = {**self.valid(5), "z-extra.lock.json": lock(commit="bad")}
        result = self.run_discovery(locks)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("z-extra.lock.json must pin a full intent-hq/intent commit", result.stderr)

    def test_a_source_path_outside_the_fixtures_fails(self):
        locks = {**self.valid(5), "z.lock.json": lock(source="docs/protocol/fixtures/../../etc")}
        self.assertNotEqual(self.run_discovery(locks).returncode, 0)

    def test_fewer_than_five_locks_fails(self):
        result = self.run_discovery(self.valid(4))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("expected at least 5 intent-hq/intent fixture locks, found 4", result.stdout)


if __name__ == "__main__":
    unittest.main()
