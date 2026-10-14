#!/usr/bin/env python3
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import threading
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

    def test_producer_aggregates_target_and_artifacts_on_same_volume(self):
        target, artifacts = self.root / 'target', self.root / 'artifacts'
        artifacts.mkdir()
        with patch.object(storage.shutil, 'disk_usage', return_value=SimpleNamespace(free=150)):
            with self.assertRaisesRegex(ValueError, 'producer storage'):
                storage.producer_capacity(target, artifacts, target_bytes=100, artifact_bytes=80, reserve_bytes=10)
        receipt = json.loads((artifacts / 'storage-producer-budget.json').read_text())
        self.assertEqual(receipt['filesystems'][0]['required_free_bytes'], 190)
        self.assertFalse(target.exists(), 'preflight must not provision or compile')

    def test_producer_checks_distinct_volumes_independently(self):
        target, artifacts = self.root / 'target', self.root / 'artifacts'
        target.mkdir(); artifacts.mkdir()
        def volume(path):
            return (1 if path == target else 2), path
        with patch.object(storage, 'filesystem', side_effect=volume), \
             patch.object(storage.shutil, 'disk_usage', side_effect=lambda p: SimpleNamespace(free=110 if p == target else 89)):
            with self.assertRaisesRegex(ValueError, 'producer storage'):
                storage.producer_capacity(target, artifacts, target_bytes=100, artifact_bytes=80, reserve_bytes=10)
        with patch.object(storage, 'filesystem', side_effect=volume), \
             patch.object(storage.shutil, 'disk_usage', return_value=SimpleNamespace(free=110)):
            receipt = storage.producer_capacity(target, artifacts, target_bytes=100, artifact_bytes=80, reserve_bytes=10)
        self.assertEqual([v['required_free_bytes'] for v in receipt['filesystems']], [110, 90])

    def test_producer_credits_existing_allocations_once_even_nested_or_linked(self):
        artifacts = self.root / 'artifacts'; artifacts.mkdir()
        target = artifacts / 'target'; target.mkdir()
        payload = target / 'existing'; payload.write_bytes(b'x' * 8192)
        os.link(payload, artifacts / 'alias')
        occupied = payload.stat().st_blocks * 512
        with patch.object(storage.shutil, 'disk_usage', return_value=SimpleNamespace(free=occupied + 100)):
            receipt = storage.producer_capacity(target, artifacts, target_bytes=occupied, artifact_bytes=occupied, reserve_bytes=100)
        self.assertEqual(receipt['filesystems'][0]['required_free_bytes'], occupied + 100)
        self.assertEqual(sum(x['credited_bytes'] for x in receipt['components']), occupied)
        self.assertTrue(payload.exists()); self.assertTrue((artifacts / 'alias').exists())

    def test_producer_never_credits_unrelated_occupied_space_or_follows_symlinks(self):
        target, artifacts = self.root / 'target', self.root / 'artifacts'
        artifacts.mkdir(); target.mkdir()
        outside = self.root / 'outside'; outside.write_bytes(b'x' * 8192)
        (target / 'link').symlink_to(outside)
        with patch.object(storage.shutil, 'disk_usage', return_value=SimpleNamespace(free=100)):
            with self.assertRaisesRegex(ValueError, 'producer storage'):
                storage.producer_capacity(target, artifacts, target_bytes=100, artifact_bytes=80, reserve_bytes=10)
        self.assertEqual(outside.read_bytes(), b'x' * 8192)

    def test_existing_disk_preflight_remains_reachable_before_storage_selection(self):
        import yaml
        root = Path(__file__).resolve().parents[1]
        for filename, names in [('ci.yml', ['check', 'coverage-e2e', 'coverage-all', 'coverage-changed']),
                                ('native-artifacts.yml', ['native'])]:
            workflow = yaml.safe_load((root / '.github/workflows' / filename).read_text())
            for name in names:
                steps = [step.get('name') for step in workflow['jobs'][name]['steps']]
                self.assertLess(steps.index('Disk-space preflight (tinybox only)'),
                                steps.index('Initialize fresh artifact storage'), name)

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

    def test_active_or_abandoned_consumer_prevents_cleanup(self):
        root = storage.initialize(1)
        (root / 'consumer-active.json').write_text('{"id":"owned-writer"}')
        with patch.object(storage, 'stop_watch') as stop, patch.object(storage.shutil, 'rmtree') as remove:
            with self.assertRaisesRegex(ValueError, 'consumer.*quiesc'):
                storage.cleanup(root)
            stop.assert_not_called(); remove.assert_not_called()
        self.assertTrue(root.exists())

    def test_cleanup_waits_for_sampler_ack_before_removing_owned_root(self):
        root = storage.initialize(1)
        watch_id = 'a' * 32
        (root / 'storage-watch.json').write_text(json.dumps({'id': watch_id}))
        original_remove = storage.shutil.rmtree
        def acknowledge(_):
            self.assertEqual((root / 'storage-watch-stop').read_text(), watch_id)
            (root / 'storage-watch-stopped').write_text(watch_id)
        def remove(path):
            self.assertEqual((root / 'storage-watch-stopped').read_text(), watch_id,
                             'cleanup must quiesce the writer before deleting its receipt directory')
            original_remove(path)
        with patch.object(storage.time, 'sleep', side_effect=acknowledge), \
             patch.object(storage.shutil, 'rmtree', side_effect=remove):
            storage.cleanup(root)
        self.assertFalse(root.exists())

    def test_cleanup_quiesces_actual_sampler_paused_during_measurement(self):
        root = storage.initialize(1)
        neighbour = self.root / 'other-run'; neighbour.mkdir()
        watch_id = 'b' * 32
        (root / 'storage-watch.json').write_text(json.dumps({'id': watch_id}))
        measuring, release, stopping = (threading.Event() for _ in range(3))
        failures = []
        original_usage, original_stop = storage.usage, storage.stop_watch
        def measure(path):
            if path == root:
                measuring.set()
                if not release.wait(5):
                    raise TimeoutError('test did not release sampler')
            return original_usage(path)
        def stop(path):
            stopping.set()
            original_stop(path)
        def run(function, *args):
            try:
                function(*args)
            except BaseException as error:
                failures.append(error)
        writer = threading.Thread(target=run, args=(storage.watch, root, watch_id))
        cleaner = threading.Thread(target=run, args=(storage.cleanup, root))
        with patch.object(storage, 'usage', side_effect=measure), patch.object(storage, 'stop_watch', side_effect=stop):
            writer.start()
            try:
                self.assertTrue(measuring.wait(5))
                cleaner.start()
                self.assertTrue(stopping.wait(5))
                self.assertTrue(root.exists())
                self.assertTrue(cleaner.is_alive())
            finally:
                release.set()
                writer.join(5)
                if cleaner.ident is not None:
                    cleaner.join(5)
        self.assertFalse(writer.is_alive()); self.assertFalse(cleaner.is_alive())
        self.assertEqual(failures, [])
        self.assertFalse(root.exists()); self.assertTrue(neighbour.exists())

    def test_real_sampler_can_be_cleaned_immediately_after_spawn(self):
        root = storage.initialize(1)
        neighbour = self.root / 'other-run'; neighbour.mkdir()
        writer = storage.launch_watch(root)
        try:
            storage.cleanup(root)
            self.assertEqual(writer.wait(timeout=5), 0)
        finally:
            if writer.poll() is None:
                writer.kill(); writer.wait(timeout=5)
        self.assertFalse(root.exists()); self.assertTrue(neighbour.exists())

    def test_acknowledgment_handle_is_closed_before_cleanup_can_remove_files(self):
        root = storage.initialize(1)
        watch_id = 'c' * 32
        (root / 'storage-watch.json').write_text(json.dumps({'id': watch_id}))
        flushed, release, closed, cleanup_checked = (threading.Event() for _ in range(4))
        failures = []
        original_write, original_remove, original_sleep = Path.write_text, storage.shutil.rmtree, storage.time.sleep
        def write(path, data, *args, **kwargs):
            if not path.name.startswith('storage-watch-stopped'):
                return original_write(path, data, *args, **kwargs)
            with path.open('w') as stream:
                count = stream.write(data); stream.flush(); flushed.set()
                if not release.wait(5):
                    raise TimeoutError('test did not release acknowledgment handle')
            closed.set()
            return count
        def remove(path):
            cleanup_checked.set()
            self.assertTrue(closed.is_set(), 'visible acknowledgment must have no open writer handle')
            original_remove(path)
        def sleep(seconds):
            cleanup_checked.set()
            original_sleep(seconds)
        def run(function, *args):
            try:
                function(*args)
            except BaseException as error:
                failures.append(error)
        writer = threading.Thread(target=run, args=(storage.watch, root, watch_id))
        cleaner = threading.Thread(target=run, args=(storage.cleanup, root))
        with patch.object(storage, 'sample'), patch.object(Path, 'write_text', write), \
             patch.object(storage.shutil, 'rmtree', side_effect=remove), patch.object(storage.time, 'sleep', side_effect=sleep):
            writer.start()
            try:
                self.assertTrue(flushed.wait(5))
                cleaner.start()
                self.assertTrue(cleanup_checked.wait(5))
            finally:
                release.set(); writer.join(5)
                if cleaner.ident is not None:
                    cleaner.join(5)
        self.assertFalse(writer.is_alive()); self.assertFalse(cleaner.is_alive())
        self.assertEqual(failures, [])
        self.assertFalse(root.exists())

    def test_wrong_ack_times_out_without_removal_or_unmark(self):
        root = storage.initialize(1)
        (root / 'storage-watch.json').write_text(json.dumps({'id': 'a' * 32}))
        (root / 'storage-watch-stopped').write_text('b' * 32)
        with patch.object(storage.time, 'monotonic', side_effect=[0, 31]), \
             patch.object(storage.shutil, 'rmtree') as remove, patch.object(storage.subprocess, 'run') as unmark:
            with self.assertRaisesRegex(TimeoutError, 'preserving owned storage'):
                storage.cleanup(root)
            remove.assert_not_called(); unmark.assert_not_called()
        self.assertTrue(root.exists())

    def test_wrong_owner_cannot_stop_sampler(self):
        root = storage.initialize(1)
        (root / 'storage-watch.json').write_text(json.dumps({'id': 'a' * 32}))
        with patch.dict(os.environ, GITHUB_RUN_ID='43'):
            with self.assertRaisesRegex(ValueError, 'another run'):
                storage.cleanup(root)
        self.assertFalse((root / 'storage-watch-stop').exists())

    def test_real_removal_error_is_not_suppressed_after_sampler_stops(self):
        root = storage.initialize(1)
        watch_id = 'a' * 32
        (root / 'storage-watch.json').write_text(json.dumps({'id': watch_id}))
        (root / 'storage-watch-stopped').write_text(watch_id)
        with patch.object(storage.shutil, 'rmtree', side_effect=OSError('deletion failed')), \
             patch.object(storage.subprocess, 'run') as unmark:
            with self.assertRaisesRegex(OSError, 'deletion failed'):
                storage.cleanup(root)
            unmark.assert_not_called()
        self.assertTrue(root.exists())

    def test_spawn_failure_removes_registration_and_duplicate_launch_is_rejected(self):
        root = storage.initialize(1)
        with patch.object(storage.subprocess, 'Popen', side_effect=OSError('spawn failed')):
            with self.assertRaisesRegex(OSError, 'spawn failed'):
                storage.launch_watch(root)
        self.assertFalse((root / 'storage-watch.json').exists())
        (root / 'storage-watch.json').write_text(json.dumps({'id': 'a' * 32}))
        with patch.object(storage.subprocess, 'Popen') as spawn:
            with self.assertRaises(FileExistsError):
                storage.launch_watch(root)
            spawn.assert_not_called()

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
