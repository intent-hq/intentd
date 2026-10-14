#!/usr/bin/env python3
"""Audit static, matrix and generated runner choices; maintenance is manual."""
from pathlib import Path
import re
import tomllib
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[1]
MAINTENANCE = {'m1mac-smoke.yml', 'provision-m1mac.yml', 'macos-prune.yml', 'tinybox-prune.yml'}
LABEL = re.compile(r'depot-(?:ubuntu-(?:22\.04|24\.04)(?:-arm)?(?:-(?:4|8|16|32|64))?|windows-(?:2022|2025)(?:-(?:4|8|16|32|64))?|macos-(?:14|15|26))\Z')


class DepotRunners(unittest.TestCase):
    def test_static_and_matrix_labels(self):
        def visit(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    if key in {'runs-on', 'os', 'runner'}:
                        for label in item if isinstance(item, list) else [item]:
                            if not isinstance(label, str):
                                continue
                            if '${{' not in label:
                                self.assertRegex(label, LABEL)
                            else:
                                self.assertNotRegex(label, r'(?<![\w-])(?:ubuntu|windows|macos|gh-linux|gh-windows)-[\w.-]+')
                    visit(item)
            elif isinstance(value, list):
                for item in value:
                    visit(item)
        for path in (ROOT / '.github/workflows').glob('*.yml'):
            with self.subTest(workflow=path.name):
                doc = yaml.safe_load(path.read_text())
                if path.name in MAINTENANCE:
                    self.assertEqual(set(doc.get('on', doc.get(True))), {'workflow_dispatch'})
                    continue
                for job in doc['jobs'].values():
                    self.assertNotIsInstance(job.get('runs-on'), list)
                    visit(job)
        config = tomllib.loads((ROOT / 'dist-workspace.toml').read_text())['dist']
        for value in config['github-custom-runners'].values():
            self.assertRegex(value if isinstance(value, str) else value['runner'], LABEL)

    def test_compiling_jobs_configure_managed_cache(self):
        doc = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text())
        for name in ['check', 'build', 'coverage-e2e', 'coverage-all', 'coverage-changed', 'daemon-lifecycle']:
            with self.subTest(job=name):
                steps = doc['jobs'][name]['steps']
                configure = next(i for i, step in enumerate(steps) if step.get('uses') == './.github/actions/release-rust-cache')
                builds = [i for i, s in enumerate(steps) if re.search(r'\bcargo (?:check|clippy|build|test|nextest|llvm-cov)\b', s.get('run', ''))]
                self.assertTrue(builds)
                self.assertLess(configure, min(builds))


if __name__ == '__main__':
    unittest.main()
