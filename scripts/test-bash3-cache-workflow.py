#!/usr/bin/env python3
"""intentd's main fills the Bash 3.2 cache its consumer-checks runs restore.

monorepo-consumer-checks runs the monorepo's consumer-checks.yml, whose
Bash 3.2 step restores from intentd's own Actions cache; merge-queue runs read
only the default branch's entries, so .github/workflows/bash3-cache.yml
saves the entry on main by calling the monorepo's fill workflow.
"""
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / '.github/workflows'


class Bash3CacheWorkflow(unittest.TestCase):
    def setUp(self):
        self.text = (WORKFLOWS / 'bash3-cache.yml').read_text()
        self.on = self.text.split('\non:\n', 1)[1].split('\npermissions:', 1)[0]

    def test_runs_on_main_daily_and_on_demand_only(self):
        self.assertIn('  push:\n    branches: [main]\n    paths:\n'
                      '      - .github/workflows/bash3-cache.yml\n', self.on)
        self.assertIn('  schedule:\n    - cron: ', self.on)
        self.assertIn('  workflow_dispatch:\n', self.on)
        # A PR or queue run would save to its own ref, which nothing else reads.
        self.assertNotIn('pull_request', self.on)
        self.assertNotIn('merge_group', self.on)

    def test_calls_the_monorepo_fill_workflow_read_only(self):
        self.assertIn('\npermissions:\n  contents: read\n', self.text)
        jobs = self.text.split('\njobs:\n', 1)[1]
        self.assertEqual(jobs.strip().splitlines(), [
            'fill:',
            '    uses: intent-hq/intent/.github/workflows/bash3-cache.yml@main',
        ])

    def test_the_reader_is_the_same_monorepo_ref(self):
        ci = (WORKFLOWS / 'ci.yml').read_text()
        self.assertIn('    uses: intent-hq/intent/.github/workflows/consumer-checks.yml@main\n', ci)


if __name__ == '__main__':
    unittest.main()
