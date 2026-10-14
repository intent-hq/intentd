#!/usr/bin/env python3
import importlib.util
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('storage', Path(__file__).with_name('ci-artifact-storage.py'))
storage = importlib.util.module_from_spec(spec); spec.loader.exec_module(storage)


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.env = patch.dict(os.environ, RUNNER_TEMP=str(self.root), GITHUB_ENV=str(self.root / 'env'),
                              GITHUB_RUN_ID='42', GITHUB_RUN_ATTEMPT='1', GITHUB_JOB='coverage-all', RUNNER_NAME='slot')
        self.env.start(); self.addCleanup(self.env.stop)

    def test_temp_preferred_and_cleanup_preserves_neighbours(self):
        neighbour = self.root / 'other-run'; neighbour.mkdir()
        with patch.object(storage, 'alternate') as alt:
            root = storage.initialize(1, True)
            alt.assert_not_called()
        (root / 'zip').write_bytes(b'x' * 100)
        self.assertGreater(storage.usage(root)['allocated_bytes'], 0)
        storage.cleanup(root)
        self.assertFalse(root.exists()); self.assertTrue(neighbour.exists())

    def test_missing_or_insufficient_alternate_fails_without_provisioning(self):
        for candidate in (None, self.root):
            with patch.object(storage.shutil, 'disk_usage', return_value=SimpleNamespace(free=0)), \
                 patch.object(storage, 'alternate', return_value=candidate):
                with self.assertRaisesRegex(ValueError, 'insufficient'):
                    storage.initialize(100, True)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_sufficient_existing_alternate_marked_and_unmarked(self):
        alternate = self.root / 'provisioned'; alternate.mkdir()
        with patch.object(storage.shutil, 'disk_usage', side_effect=lambda p: SimpleNamespace(free=0 if p == self.root else 200)), \
             patch.object(storage, 'alternate', return_value=alternate), patch.object(storage.subprocess, 'run') as run:
            root = storage.initialize(100, True)
            self.assertEqual(root.parent.parent, alternate)
            self.assertEqual(run.call_args.args[0][-2], 'mark')
            storage.cleanup(root)
            self.assertEqual(run.call_args.args[0][-2], 'unmark')

    def test_failed_attempt_is_never_reused_or_deleted(self):
        root = storage.initialize(1)
        with self.assertRaises(FileExistsError):
            storage.initialize(1)
        with patch.dict(os.environ, GITHUB_RUN_ATTEMPT='2'):
            with self.assertRaisesRegex(ValueError, 'another run'):
                storage.cleanup(root)
        self.assertTrue(root.exists())

    def test_sampler_counts_fresh_profiles_and_separate_compile_target(self):
        root = storage.initialize(1)
        (root / 'fresh.profraw').write_bytes(b'x' * 5000)
        target = self.root / 'cargo'; target.mkdir(); (target / 'object').write_bytes(b'y' * 9000)
        (root / 'storage-target.json').write_text(json.dumps({'target': str(target)}))
        with patch.object(storage.time, 'sleep', side_effect=RuntimeError('sample finished')):
            with self.assertRaisesRegex(RuntimeError, 'sample finished'):
                storage.watch(root)
        receipt = json.loads((root / 'storage-usage.json').read_text())
        self.assertGreaterEqual(receipt['sampled_high_water']['allocated_bytes'], 5000)
        self.assertGreaterEqual(receipt['compile_target_sampled_high_water']['allocated_bytes'], 9000)
        self.assertEqual(receipt['samples'], 1)

    def test_alternate_requires_mounted_provisioned_writable_capacity(self):
        for mounted, exists, writable in [(False, True, True), (True, False, True), (True, True, False)]:
            with patch.object(Path, 'is_mount', return_value=mounted), patch.object(Path, 'is_dir', return_value=exists), \
                 patch.object(os, 'access', return_value=writable):
                self.assertIsNone(storage.alternate())


if __name__ == '__main__':
    unittest.main()
