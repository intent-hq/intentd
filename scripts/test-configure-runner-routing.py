#!/usr/bin/env python3
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('routing', ROOT / 'scripts/configure-runner-routing.py')
routing = importlib.util.module_from_spec(spec)
spec.loader.exec_module(routing)


class ConfigureRouting(unittest.TestCase):
    def test_generated_jobs_receive_routing_without_losing_dependencies(self):
        source = '''name: Release
on: push
jobs:
  plan:
    runs-on: "depot-ubuntu-24.04"
  build:
    needs: [plan]
    runs-on: ${{ matrix.runner }}
  announce:
    needs: [plan, build]
    runs-on: depot-ubuntu-24.04
'''
        result = routing.configure(source)
        self.assertIn('needs: [plan, runner-route]', result)
        self.assertIn('needs: [plan, build, runner-route]', result)
        self.assertIn(".outputs.runners || '{}')[matrix.runner] || matrix.runner", result)
        self.assertEqual(result, routing.configure(result))

    def test_generated_block_dependencies_remain_intact(self):
        source = 'name: Release\njobs:\n  build:\n    needs:\n      - plan\n      - previous\n    runs-on: ${{ matrix.runner }}\n'
        result = routing.configure(source)
        self.assertIn('    needs:\n      - plan\n      - previous\n      - runner-route\n', result)
        self.assertEqual(result, routing.configure(result))

    def test_generated_python_steps_have_a_tomllib_capable_interpreter(self):
        source = 'name: Release\njobs:\n  plan:\n    runs-on: depot-ubuntu-24.04\n    steps:\n      - run: python3 scripts/optional-intel-release.py\n'
        result = routing.configure(source)
        self.assertIn("python-version: '3.11'", result)
        self.assertLess(result.index('uses: actions/setup-python@'), result.index('- run: python3'))
        self.assertEqual(result, routing.configure(result))

    def test_privileged_build_keeps_hosted_linux_legs(self):
        source = 'name: Release\njobs:\n  build-local-artifacts:\n    runs-on: ${{ matrix.runner }}\n  host:\n    runs-on: ${{ matrix.runner }}\n'
        result = routing.configure(source)
        self.assertIn("    runs-on: ${{ (contains(matrix.runner, 'ubuntu') || contains(matrix.runner, 'linux')) && matrix.runner || fromJSON(needs.runner-route.outputs.runners || '{}')[matrix.runner] || matrix.runner }}\n  host:", result)
        self.assertIn("  host:\n    needs: [runner-route]\n    runs-on: ${{ fromJSON(needs.runner-route.outputs.runners || '{}')[matrix.runner] || matrix.runner }}", result)
        self.assertEqual(result, routing.configure(result))
        self.assertTrue(result.startswith('name: Release\njobs:\n'), result)
        checked_in = (ROOT / '.github/workflows/v-release.yml').read_text()
        self.assertIn("contains(matrix.runner, 'ubuntu') || contains(matrix.runner, 'linux')", checked_in)

    def test_checked_in_release_is_already_routed(self):
        source = (ROOT / '.github/workflows/v-release.yml').read_text()
        self.assertEqual(routing.configure(source), source)


if __name__ == '__main__':
    unittest.main()
