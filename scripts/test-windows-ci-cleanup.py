#!/usr/bin/env python3
"""Policy tests everywhere; real handle, junction and Cargo tests on Windows."""
from contextlib import ExitStack
import importlib.util
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('cleanup', Path(__file__).with_name('windows-ci-cleanup.py'))
cleanup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cleanup)


def process(pid, parent, name):
    return dict(ProcessId=pid, ParentProcessId=parent, Name=name)


class PolicyTests(unittest.TestCase):
    def test_only_own_worker_and_no_compilers(self):
        valid = [process(1, 0, 'Runner.Listener.exe'), process(2, 1, 'Runner.Worker.exe'),
                 process(3, 2, 'python.exe')]
        cleanup.validate_processes(valid, 3)
        for extra in ['Runner.Worker.exe', 'cargo.exe', 'rustc.exe', 'sccache.exe', 'build-script-build.exe', 'link.exe']:
            with self.subTest(extra=extra), self.assertRaises(cleanup.Refusal):
                cleanup.validate_processes(valid + [process(9, 0, extra)], 3)
        for rows in [[], {}, valid[1:], valid[:2], valid + [valid[0]],
                     [process('1', 0, 'bad')], valid + [{'Name': 'unknown'}]]:
            with self.subTest(rows=rows), self.assertRaises(cleanup.Refusal):
                cleanup.validate_processes(rows, 3)

    def test_only_local_absolute_profiles(self):
        cleanup.validate_home(r'C:\Users\runner')
        for home in ['relative', 'C:\\', r'C:\Users\..\victim', r'\\server\share\user',
                     r'C:\Users\user:stream', 'C:\\Users\\user.']:
            with self.subTest(home=home), self.assertRaises(cleanup.Refusal):
                cleanup.validate_home(home)

    def test_cli_refuses_other_hosts_before_loading_native_api(self):
        with patch.object(cleanup.platform, 'system', return_value='Windows'), patch.dict(
                os.environ, RUNNER_NAME='not-5090pc', GITHUB_ACTIONS='true'), patch.object(
                cleanup, 'Native', side_effect=AssertionError('must not load')), patch.object(
                sys, 'argv', ['cleanup']), patch.object(cleanup, 'emit'):
            self.assertEqual(cleanup.main(), 1)


@unittest.skipUnless(os.name == 'nt', 'native Windows APIs')
class NativeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.api = cleanup.Native()
        self.target = self.home / 'ci-target/intentd' / cleanup.TARGETS[0]
        self.profile = self.target / 'x86_64-pc-windows-msvc/debug'
        (self.profile / 'deps').mkdir(parents=True)
        self.file = self.profile / 'deps/old.rmeta'
        self.file.write_text('disposable')
        (self.profile / '.cargo-lock').touch()
        for name, kwargs in [('assert_idle_runner', {}), ('emit', {}), ('free_bytes', {'return_value': 0})]:
            guard = patch.object(cleanup, name, **kwargs)
            setattr(self, name, guard.start())
            self.addCleanup(guard.stop)

    def clean(self, apply=False):
        return cleanup.clean(self.home, 100, apply, self.api)

    def test_dry_run_reports_and_preserves_content(self):
        result = self.clean()
        self.assertEqual(result['candidates'], [str(self.profile)])
        self.assertEqual(result['deleted'], [])
        self.assertEqual(self.file.read_text(), 'disposable')

    def test_apply_preserves_locks_and_excluded_paths_and_stops_at_headroom(self):
        preserved = [self.home / '.cargo/credentials.toml', self.home / '.rustup/keep',
                     self.target / 'release/keep', self.home / 'source/keep',
                     self.home / 'ci-target/other/debug/keep']
        for file in preserved:
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text('preserve')
        for lock in cleanup.CARGO_LOCKS:
            (self.profile / lock).touch()
        identities = [(self.profile / lock).stat().st_ino for lock in cleanup.CARGO_LOCKS]
        second = self.target / 'debug'
        (second / 'deps').mkdir(parents=True)
        (second / '.cargo-lock').touch()
        (second / 'deps/warm.rmeta').write_text('warm')
        os.utime(self.profile, (1, 1))
        self.free_bytes.side_effect = [0, 0, 200, 200]
        result = self.clean(True)
        self.assertEqual(result['recovered_bytes'], 200)
        self.assertFalse(self.file.exists())
        self.assertTrue((second / 'deps/warm.rmeta').exists())
        self.assertEqual(identities, [(self.profile / lock).stat().st_ino for lock in cleanup.CARGO_LOCKS])
        for file in preserved:
            self.assertEqual(file.read_text(), 'preserve')

    def test_adequate_space_is_noop(self):
        self.free_bytes.return_value = 200
        self.assertEqual(self.clean(True)['deleted'], [])
        self.assertTrue(self.file.exists())

    def test_insufficient_space_fails_visibly(self):
        with self.assertRaisesRegex(cleanup.Refusal, 'headroom'):
            self.clean(True)

    def test_missing_profile_never_broadens_scope(self):
        self.profile.rename(self.profile.with_name('release'))
        self.assertEqual(self.clean()['candidates'], [])
        with self.assertRaisesRegex(cleanup.Refusal, 'headroom'):
            self.clean(True)
        self.assertTrue((self.target / 'x86_64-pc-windows-msvc/release/deps/old.rmeta').exists())

    def test_missing_cargo_fingerprint_refuses(self):
        (self.profile / '.cargo-lock').unlink()
        with self.assertRaises(OSError):
            self.clean(True)
        self.assertTrue(self.file.exists())

    def test_in_use_marker_never_expires(self):
        for parent in [self.target, self.profile, self.target.parent]:
            marker = parent / '.in-use'
            marker.touch()
            os.utime(marker, (1, 1))
            with self.assertRaisesRegex(cleanup.Refusal, 'in-use'):
                self.clean(True)
            marker.unlink()
        self.assertTrue(self.file.exists())

    def test_live_worker_before_delete_refuses(self):
        self.assert_idle_runner.side_effect = [None, cleanup.Refusal('live job')]
        with self.assertRaisesRegex(cleanup.Refusal, 'live job'):
            self.clean(True)
        self.assertTrue(self.file.exists())

    def test_all_cargo_locks_and_cleaner_lock_contend(self):
        for path in [self.target.parent / '.windows-cleanup.lock',
                     *(self.profile / name for name in cleanup.CARGO_LOCKS)]:
            with self.subTest(path=path), self.api.lock(path, create=True):
                with self.assertRaises((cleanup.Refusal, OSError)):
                    self.clean(True)
            self.assertTrue(self.file.exists())

    def test_anchored_directories_cannot_be_replaced(self):
        with ExitStack() as stack:
            cleanup.open_home(self.api, self.home, stack)
            with self.assertRaises(OSError):
                self.home.rename(self.home.with_name(self.home.name + '-moved'))
        self.assertTrue(self.file.exists())

    def junction(self, link, destination):
        subprocess.run(['cmd.exe', '/d', '/c', 'mklink', '/J', str(link), str(destination)],
                       check=True, capture_output=True, text=True)
        self.addCleanup(lambda: os.rmdir(link) if os.path.lexists(link) else None)

    def test_junction_ancestor_refuses_without_touching_destination(self):
        original = self.home / 'ci-target'
        destination = self.home / 'saved-target'
        original.rename(destination)
        self.junction(original, destination)
        with self.assertRaisesRegex(cleanup.Refusal, 'Reparse'):
            self.clean(True)
        self.assertTrue((destination / 'intentd' / cleanup.TARGETS[0] / 'x86_64-pc-windows-msvc/debug/deps/old.rmeta').exists())

    def test_internal_junction_never_traverses_destination(self):
        outside = self.home / 'source'
        outside.mkdir()
        (outside / 'keep').write_text('source')
        self.junction(self.profile / 'deps/aaa-junction', outside)
        with self.assertRaisesRegex(cleanup.Refusal, 'Reparse'):
            self.clean(True)
        self.assertEqual((outside / 'keep').read_text(), 'source')

    def test_hardlink_refused(self):
        outside = self.home / 'source-file'
        outside.write_text('source')
        os.link(outside, self.profile / 'deps/aaa-hardlink')
        with self.assertRaisesRegex(cleanup.Refusal, 'Multiply-linked'):
            self.clean(True)
        self.assertEqual(outside.read_text(), 'source')

    def test_lock_file_cannot_be_replaced(self):
        path = self.profile / '.cargo-lock'
        with self.api.lock(path):
            with self.assertRaises(OSError):
                path.unlink()
            with self.assertRaises(OSError):
                path.rename(path.with_name('other-lock'))

    def test_delete_failure_is_not_hidden(self):
        with patch.object(self.api, 'delete', side_effect=OSError('denied')):
            with self.assertRaisesRegex(OSError, 'denied'):
                self.clean(True)
        self.assertTrue(self.file.exists())

    def test_real_cargo_waits_for_lock_and_builds_after_release(self):
        project = self.home / 'project'
        (project / 'src').mkdir(parents=True)
        (project / 'Cargo.toml').write_text('[package]\nname="cleanup-lock-proof"\nversion="0.0.0"\nedition="2021"\n')
        (project / 'src/lib.rs').write_text('pub fn value() -> u8 { 42 }\n')
        profile = self.target / 'debug'
        profile.mkdir()
        lock = profile / '.cargo-lock'
        lock.touch()
        child = None
        try:
            with self.api.lock(lock):
                env = {k: v for k, v in os.environ.items() if k not in ('RUSTC_WRAPPER', 'RUSTUP_TOOLCHAIN', 'CARGO_TARGET_DIR')}
                child = subprocess.Popen(['cargo', 'check', '--offline', '--manifest-path', str(project / 'Cargo.toml'),
                                          '--target-dir', str(self.target)], cwd=project, env=env,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                lines = queue.Queue()
                def read():
                    for line in child.stderr:
                        lines.put(line)
                reader = threading.Thread(target=read, daemon=True)
                reader.start()
                line = lines.get(timeout=45)
                self.assertIn('Blocking waiting for file lock', line)
                self.assertIsNone(child.poll())
            self.assertEqual(child.wait(timeout=90), 0)
            reader.join(timeout=5)
            self.assertTrue(list((profile / 'deps').glob('*.rmeta')))
        finally:
            if child:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=5)
                child.stdout.close()
                child.stderr.close()


if __name__ == '__main__':
    if '--require-windows' in sys.argv:
        sys.argv.remove('--require-windows')
        if os.name != 'nt':
            raise SystemExit('Native Windows proof is required')
    unittest.main()
