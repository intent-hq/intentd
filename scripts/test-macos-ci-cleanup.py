#!/usr/bin/env python3
"""Offline safety tests: real disposable trees, simulated disk and runner state."""
import importlib.util
import os
from pathlib import Path
import subprocess
import selectors
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('cleanup', Path(__file__).with_name('macos-ci-cleanup.py'))
cleanup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup)


class CleanupTests(unittest.TestCase):
    def setUp(self):
        previous_umask = os.umask(0o022)
        self.addCleanup(os.umask, previous_umask)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name).resolve()
        self.root = self.home / 'ci-target/intentd/build-aarch64-apple-darwin'
        self.profile = self.root / 'aarch64-apple-darwin/debug'
        (self.profile / 'deps').mkdir(parents=True)
        (self.profile / '.cargo-lock').touch()
        (self.profile / 'deps/old.rmeta').write_bytes(b'build output')
        self.guard = patch.object(cleanup, 'assert_idle_runner')
        self.guard_mock = self.guard.start()
        self.addCleanup(self.guard.stop)
        self.output = patch.object(cleanup, 'emit')
        self.output.start()
        self.addCleanup(self.output.stop)
        self.space = patch.object(cleanup, 'free_bytes', return_value=0)
        self.free = self.space.start()
        self.addCleanup(self.space.stop)

    def run_cleanup(self, apply=True):
        return cleanup.clean(self.home, min_free_bytes=100, apply=apply)

    def test_dry_run_lists_exact_candidate_without_deleting(self):
        result = self.run_cleanup(False)
        self.assertEqual(result['candidates'], [str(self.profile)])
        self.assertEqual(result['deleted'], [])
        self.assertTrue((self.profile / 'deps/old.rmeta').exists())

    def test_apply_removes_only_profile_and_stops_at_headroom(self):
        other = self.root / 'debug'
        (other / 'deps').mkdir(parents=True)
        (other / '.cargo-lock').touch()
        os.utime(self.profile, (1, 1))
        sentinel = self.home / '.cargo/credentials.toml'
        sentinel.parent.mkdir()
        sentinel.write_text('preserve')
        self.free.side_effect = [0, 0, 200, 200]
        result = self.run_cleanup()
        self.assertEqual(result['deleted'], [str(self.profile / 'deps')])
        self.assertTrue((self.profile / '.cargo-lock').is_file())
        self.assertEqual(list((self.profile / 'deps').iterdir()), [])
        self.assertTrue(other.exists())
        self.assertEqual(sentinel.read_text(), 'preserve')
        self.assertEqual(result['recovered_bytes'], 200)

    def test_enough_space_is_noop(self):
        self.free.return_value = 200
        self.assertEqual(self.run_cleanup()['deleted'], [])
        self.assertTrue(self.profile.exists())

    def test_insufficient_space_fails_after_safe_candidates_exhausted(self):
        with self.assertRaisesRegex(cleanup.Refusal, 'headroom'):
            self.run_cleanup()

    def test_release_profiles_never_selected_or_deleted(self):
        releases = [self.root / 'release', self.root / 'aarch64-apple-darwin/release']
        for path in releases:
            (path / 'deps').mkdir(parents=True)
            (path / '.cargo-lock').touch()
            (path / 'deps/preserve.rmeta').write_text('release cache')
            os.utime(path, (1, 1))
        preview = self.run_cleanup(False)
        self.assertEqual(preview['candidates'], [str(self.profile)])
        self.free.side_effect = [0, 0, 200]
        result = self.run_cleanup()
        self.assertEqual(result['candidates'], [str(self.profile)])
        for path in releases:
            self.assertEqual((path / 'deps/preserve.rmeta').read_text(), 'release cache')
            self.assertFalse(any(deleted.startswith(str(path) + '/') for deleted in result['deleted']))

    def test_low_disk_never_falls_back_to_release(self):
        release = self.root / 'release'
        self.profile.rename(release)
        self.assertEqual(self.run_cleanup(False)['candidates'], [])
        with self.assertRaisesRegex(cleanup.Refusal, 'headroom'):
            self.run_cleanup()
        self.assertTrue((release / 'deps/old.rmeta').exists())

    def test_no_unrecognized_directories_deleted(self):
        unknown = self.root / 'source'
        unknown.mkdir()
        (unknown / 'keep').touch()
        self.run_cleanup(False)
        self.assertTrue((unknown / 'keep').exists())

    def test_candidate_without_cargo_fingerprint_rejected(self):
        (self.profile / '.cargo-lock').unlink()
        with self.assertRaises(cleanup.Refusal):
            self.run_cleanup(False)

    def test_in_use_marker_never_expires(self):
        marker = self.root / '.in-use'
        marker.touch()
        os.utime(marker, (1, 1))
        with self.assertRaisesRegex(cleanup.Refusal, 'in-use'):
            self.run_cleanup()
        self.assertTrue(self.profile.exists())

    def test_symlink_candidate_rejected(self):
        self.profile.rename(self.profile.with_name('saved'))
        self.profile.symlink_to(self.profile.with_name('saved'), target_is_directory=True)
        with self.assertRaises(cleanup.Refusal):
            self.run_cleanup()
        self.assertTrue((self.profile / 'deps/old.rmeta').exists())

    def test_symlink_ancestor_rejected(self):
        ci = self.home / 'ci-target'
        ci.rename(self.home / 'elsewhere')
        ci.symlink_to(self.home / 'elsewhere', target_is_directory=True)
        with self.assertRaises(cleanup.Refusal):
            self.run_cleanup()
        self.assertTrue(self.profile.exists())

    def test_internal_symlink_never_follows_target(self):
        outside = self.home / 'source'
        outside.mkdir()
        (outside / 'keep').touch()
        (self.profile / 'deps/link').symlink_to(outside, target_is_directory=True)
        self.free.side_effect = [0, 0, 200, 200]
        self.run_cleanup()
        self.assertTrue((outside / 'keep').exists())

    def test_relative_and_parent_traversal_paths_rejected(self):
        for path in [Path('relative'), self.home / '..' / self.home.name]:
            with self.subTest(path=path), self.assertRaises(cleanup.Refusal):
                with cleanup.ExitStack() as stack:
                    cleanup.open_home(path, stack)

    def test_group_writable_profile_rejected(self):
        self.profile.chmod(0o775)
        with self.assertRaises(cleanup.Refusal):
            self.run_cleanup()
        self.assertTrue((self.profile / 'deps/old.rmeta').exists())

    def test_hardlinked_cargo_lock_rejected(self):
        os.link(self.profile / '.cargo-lock', self.home / 'other-lock')
        with self.assertRaises(cleanup.Refusal):
            self.run_cleanup()

    def test_cleaner_lock_held_by_other_process_refuses(self):
        lock = self.root / '.macos-cleanup.lock'
        lock.touch()
        code = 'import fcntl,sys; f=open(sys.argv[1], "r+"); fcntl.flock(f,fcntl.LOCK_EX); print("ready",flush=True); sys.stdin.read()'
        proc = subprocess.Popen(['python3', '-c', code, str(lock)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(proc.stdout.readline().strip(), 'ready')
            with self.assertRaises(cleanup.Refusal):
                self.run_cleanup()
            self.assertTrue(self.profile.exists())
        finally:
            proc.communicate(timeout=5)

    def assert_substitution_safe(self, substitute):
        source = self.home / 'source'
        (source / 'deps').mkdir(parents=True)
        (source / 'README').write_text('source README')
        (source / 'deps/source.txt').write_text('source code')
        def race(**event):
            if event.get('event') == 'deleting':
                substitute(source)
        self.free.side_effect = [0, 0, 200]
        with patch.object(cleanup, 'emit', side_effect=race):
            with self.assertRaises((cleanup.Refusal, OSError)):
                self.run_cleanup()
        self.assertEqual((source / 'README').read_text(), 'source README')
        self.assertEqual((source / 'deps/source.txt').read_text(), 'source code')

    def test_profile_symlink_substitution_after_validation_refused(self):
        def substitute(source):
            self.profile.rename(self.profile.with_name('original-debug'))
            self.profile.symlink_to(source, target_is_directory=True)
        self.assert_substitution_safe(substitute)

    def test_ancestor_symlink_substitution_after_validation_refused(self):
        def substitute(source):
            self.root.rename(self.root.with_name('original-target'))
            (source / 'aarch64-apple-darwin').mkdir()
            (source / 'aarch64-apple-darwin/debug').symlink_to(source, target_is_directory=True)
            self.root.symlink_to(source, target_is_directory=True)
        self.assert_substitution_safe(substitute)

    def test_profile_directory_substitution_after_validation_refused(self):
        def substitute(source):
            self.profile.rename(self.profile.with_name('original-debug'))
            source.rename(self.profile)
        self.free.side_effect = [0, 0, 200]
        source = self.home / 'source'
        (source / 'deps').mkdir(parents=True)
        (source / 'deps/source.txt').write_text('source code')
        def race(**event):
            if event.get('event') == 'deleting':
                substitute(source)
        with patch.object(cleanup, 'emit', side_effect=race):
            with self.assertRaises((cleanup.Refusal, OSError)):
                self.run_cleanup()
        self.assertEqual((self.profile / 'deps/source.txt').read_text(), 'source code')

    def assert_real_cargo_replacement_preserved(self, replace_locks):
        project = self.home / 'project'
        (project / 'src').mkdir(parents=True)
        (project / 'Cargo.toml').write_text('[package]\nname="cleanup-replacement-proof"\nversion="0.0.0"\nedition="2021"\n')
        (project / 'src/lib.rs').write_text('pub fn value() -> u8 { 42 }\n')
        # Native Cargo uses host/debug, independent of the test host platform.
        profile = self.root / 'debug'
        self.profile.rename(profile)
        def cargo(command):
            result = subprocess.run(['cargo', command, '--offline', '--manifest-path', str(project / 'Cargo.toml'), '--target-dir', str(self.root)], cwd=project, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        def race(**event):
            if event.get('event') != 'deleting':
                return
            if replace_locks:
                for name in cleanup.CARGO_LOCKS:
                    (profile / name).unlink()
                    (profile / name).touch()
            else:
                cargo('clean')
            cargo('check')
            self.assertTrue(list((profile / 'deps').glob('*.rmeta')))
        with patch.object(cleanup, 'emit', side_effect=race):
            with self.assertRaises((cleanup.Refusal, OSError)):
                self.run_cleanup()
        self.assertTrue(list((profile / 'deps').glob('*.rmeta')))

    def test_real_cargo_clean_recreation_preserves_new_build(self):
        self.assert_real_cargo_replacement_preserved(False)

    def test_real_cargo_replaced_lock_inodes_preserve_new_build(self):
        self.assert_real_cargo_replacement_preserved(True)

    def test_lock_replacement_after_acquisition_refused(self):
        lock = self.profile / '.cargo-lock'
        def race(**event):
            if event.get('event') == 'deleting':
                lock.rename(self.profile / 'old-lock')
                lock.touch()
        with patch.object(cleanup, 'emit', side_effect=race):
            with self.assertRaisesRegex(cleanup.Refusal, 'Lock identity changed'):
                self.run_cleanup()
        self.assertTrue((self.profile / 'deps/old.rmeta').exists())

    def test_nested_directory_swap_during_open_never_follows_symlink(self):
        source = self.home / 'source'
        source.mkdir()
        (source / 'keep').write_text('source code')
        original_open = os.open
        deleting = False
        def phase(**event):
            nonlocal deleting
            deleting = event.get('event') == 'deleting'
        def race(path, flags, *args, **kwargs):
            fd = original_open(path, flags, *args, **kwargs)
            if deleting and path == 'deps':
                (self.profile / 'deps').rename(self.profile / 'original-deps')
                (self.profile / 'deps').symlink_to(source, target_is_directory=True)
            return fd
        with patch.object(cleanup, 'emit', side_effect=phase), patch.object(cleanup.os, 'open', side_effect=race):
            with self.assertRaises(cleanup.Refusal):
                self.run_cleanup()
        self.assertEqual((source / 'keep').read_text(), 'source code')

    def test_ancestor_swap_during_lock_open_does_not_redirect_creation(self):
        source = self.home / 'source'
        (source / 'aarch64-apple-darwin/debug').mkdir(parents=True)
        (source / 'keep').write_text('source code')
        original_open = os.open
        changed = False
        def race(path, flags, *args, **kwargs):
            nonlocal changed
            if path == '.cargo-artifact-lock' and not changed:
                changed = True
                self.root.rename(self.root.with_name('original-target'))
                self.root.symlink_to(source, target_is_directory=True)
            return original_open(path, flags, *args, **kwargs)
        with patch.object(cleanup.os, 'open', side_effect=race):
            with self.assertRaises(cleanup.Refusal):
                self.run_cleanup()
        self.assertEqual((source / 'keep').read_text(), 'source code')
        self.assertEqual(list((source / 'aarch64-apple-darwin/debug').iterdir()), [])

    def test_lock_symlink_rejected(self):
        victim = self.home / 'keep'
        victim.write_text('keep')
        (self.root / '.macos-cleanup.lock').symlink_to(victim)
        with self.assertRaises((cleanup.Refusal, OSError)):
            self.run_cleanup()
        self.assertEqual(victim.read_text(), 'keep')

    def test_all_cargo_lock_inodes_preserved(self):
        files = [self.profile / name for name in ['.cargo-lock', '.cargo-artifact-lock', '.cargo-build-lock']]
        for path in files:
            path.touch()
        inodes = [path.stat().st_ino for path in files]
        self.free.side_effect = [0, 0, 200, 200]
        self.run_cleanup()
        self.assertEqual([path.stat().st_ino for path in files], inodes)

    def test_modern_cargo_locks_held_by_live_process_refuse(self):
        for name in ['.cargo-artifact-lock', '.cargo-build-lock']:
            lock = self.profile / name
            lock.touch()
            code = 'import fcntl,sys; f=open(sys.argv[1], "r+"); fcntl.flock(f,fcntl.LOCK_EX); print("ready",flush=True); sys.stdin.read()'
            proc = subprocess.Popen(['python3', '-c', code, str(lock)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            try:
                self.assertEqual(proc.stdout.readline().strip(), 'ready')
                with self.subTest(name=name), self.assertRaisesRegex(cleanup.Refusal, 'Lock held'):
                    self.run_cleanup()
                self.assertTrue((self.profile / 'deps/old.rmeta').exists())
            finally:
                proc.communicate(timeout=5)

    def test_cargo_lock_held_by_live_process_refuses(self):
        code = 'import fcntl,sys; f=open(sys.argv[1], "r+"); fcntl.flock(f,fcntl.LOCK_EX); print("ready",flush=True); sys.stdin.read()'
        proc = subprocess.Popen(['python3', '-c', code, str(self.profile / '.cargo-lock')], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(proc.stdout.readline().strip(), 'ready')
            with self.assertRaises(cleanup.Refusal):
                self.run_cleanup()
            self.assertTrue(self.profile.exists())
        finally:
            proc.communicate(timeout=5)

    def test_real_cargo_starting_during_cleanup_waits_for_preserved_lock(self):
        project = self.home / 'project'
        (project / 'src').mkdir(parents=True)
        (project / 'Cargo.toml').write_text('[package]\nname="cleanup-lock-proof"\nversion="0.0.0"\nedition="2021"\n')
        (project / 'src/lib.rs').write_text('pub fn value() -> u8 { 42 }\n')
        profile = self.root / 'debug'
        (profile / 'deps').mkdir(parents=True)
        lock = profile / '.cargo-lock'
        lock.touch()
        inode = lock.stat().st_ino
        os.utime(profile, (1, 1))
        proc = None
        original_remove = cleanup.remove_entry
        def remove(parent_fd, name):
            nonlocal proc
            if proc is None:
                proc = subprocess.Popen(['cargo', 'check', '--offline', '--manifest-path', str(project / 'Cargo.toml'), '--target-dir', str(self.root)], cwd=project, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                with selectors.DefaultSelector() as selector:
                    selector.register(proc.stderr, selectors.EVENT_READ)
                    self.assertTrue(selector.select(timeout=30), 'cargo never reported waiting for the lock')
                    self.assertIn('Blocking waiting for file lock on', proc.stderr.readline())
                self.assertIsNone(proc.poll())
            original_remove(parent_fd, name)
        self.free.side_effect = [0, 0, 200, 200]
        try:
            with patch.object(cleanup, 'remove_entry', side_effect=remove):
                self.run_cleanup()
            self.assertEqual(lock.stat().st_ino, inode)
            out, err = proc.communicate(timeout=60)
            self.assertEqual(proc.returncode, 0, out + err)
            self.assertTrue(list((profile / 'deps').glob('*.rmeta')))
        finally:
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.communicate(timeout=5)

    def test_live_job_appearing_before_delete_refuses(self):
        self.guard_mock.side_effect = [None, cleanup.Refusal('live job')]
        with self.assertRaisesRegex(cleanup.Refusal, 'live job'):
            self.run_cleanup()
        self.assertTrue(self.profile.exists())

    def test_delete_error_fails_and_keeps_failure_visible(self):
        with patch.object(cleanup, 'remove_entry', side_effect=OSError('denied')):
            with self.assertRaisesRegex(OSError, 'denied'):
                self.run_cleanup()

    def test_invalid_threshold_rejected(self):
        for value in [0, -1]:
            with self.assertRaises(cleanup.Refusal):
                cleanup.clean(self.home, min_free_bytes=value, apply=True)


class OwnershipTests(unittest.TestCase):
    good = '10 1 /runner/bin/Runner.Listener\n20 10 /runner/bin/Runner.Worker\n30 20 /bin/bash\n40 30 python3\n'

    def check(self, text):
        cleanup.validate_processes(text, pid=40)

    def test_only_own_worker_is_safe(self):
        self.check(self.good)

    def test_other_worker_listener_or_compiler_refused(self):
        for comm in ['Runner.Worker', 'cargo', 'rustc', 'rust-lld', 'clang', 'ld', 'sccache', 'build-script-build']:
            with self.subTest(comm=comm), self.assertRaises(cleanup.Refusal):
                self.check(self.good + '50 1 /bin/' + comm + '\n')

    def test_idle_second_listener_allowed_with_cargo_lock_protection(self):
        self.check(self.good + '50 1 /other/Runner.Listener\n')

    def test_missing_or_unrelated_worker_refused(self):
        for text in ['', self.good.replace('40 30', '40 1'), self.good.replace('20 10', '20 1')]:
            with self.subTest(text=text), self.assertRaises(cleanup.Refusal):
                self.check(text)

    def test_unreadable_or_malformed_process_table_refused(self):
        with self.assertRaises(cleanup.Refusal):
            self.check(self.good + 'broken row\n')
        with patch.object(cleanup.subprocess, 'run', side_effect=OSError('ps failed')):
            with self.assertRaises(OSError):
                cleanup.assert_idle_runner()


if __name__ == '__main__':
    unittest.main()
