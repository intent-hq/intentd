#!/usr/bin/env python3
"""Reclaim only inactive m1mac Cargo profiles until 40 GiB is free.

Run on the m1mac Actions runner, before any build step. Refuse other active
workers or compilers, including orphaned builds. A local advisory lock serializes
cleaners. Hold Cargo profile locks and preserve their inodes and parent directories
so newly scheduled builds must wait, even with multiple runner listeners. No age-based expiry or force
flag: unknown ownership fails closed. This is not a general developer cleaner.

Only four exact profile paths below the account's CI target are eligible. No
sources, registry, toolchains, credentials, checkout or other repo are pruned.
Internal symlinks are unlinked by fd-safe rmtree, never traversed. All containing
paths and locks must be real owned directories/files. Reports use bytes; free
space changes are observed filesystem deltas, not summed apparent file sizes.
"""
import argparse
from contextlib import ExitStack, contextmanager
import fcntl
import json
import os
from pathlib import Path
import platform
import pwd
import shutil
import stat
import subprocess

GIB = 1024 ** 3
CARGO_LOCKS = ('.cargo-lock', '.cargo-artifact-lock', '.cargo-build-lock')
PROFILES = ('debug', 'release', 'aarch64-apple-darwin/debug', 'aarch64-apple-darwin/release')


class Refusal(RuntimeError):
    pass


def emit(**record):
    print(json.dumps(record, sort_keys=True), flush=True)


def free_bytes(path):
    return shutil.disk_usage(path).free


def validate_processes(output, pid):
    processes = {}
    for row in output.splitlines():
        fields = row.strip().split(None, 2)
        if len(fields) != 3 or not all(x.isdigit() for x in fields[:2]):
            raise Refusal('Cannot parse process ownership')
        ident, parent = map(int, fields[:2])
        processes[ident] = (parent, Path(fields[2]).name)
    ancestors = set()
    current = pid
    while current in processes and current not in ancestors:
        ancestors.add(current)
        current = processes[current][0]
    listeners = {p for p, (_, name) in processes.items() if name == 'Runner.Listener'}
    workers = {p for p, (_, name) in processes.items() if name == 'Runner.Worker'}
    if len(workers) != 1 or not workers <= ancestors or not listeners & ancestors:
        raise Refusal('Require our runner listener and only our own active worker')
    compilers = {'cargo', 'rustc', 'rustdoc', 'rust-lld', 'ld', 'clang', 'clang++',
                 'cc', 'c++', 'gcc', 'g++', 'sccache', 'cargo-clippy', 'clippy-driver'}
    active = [(p, name) for p, (_, name) in processes.items()
              if name in compilers or name.startswith('build-script-')]
    if active:
        raise Refusal('Active build processes: ' + repr(active))


def assert_idle_runner():
    result = subprocess.run(['ps', '-ww', '-axo', 'pid=,ppid=,comm='],
                            check=True, capture_output=True, text=True, timeout=15)
    validate_processes(result.stdout, os.getpid())


def validate_directory(path, home):
    if not path.is_absolute() or '..' in path.parts or not path.is_relative_to(home):
        raise Refusal('Unsafe path: ' + str(path))
    for part in [*reversed(path.parents), path]:
        info = part.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise Refusal('Not a real directory (symlink forbidden): ' + str(part))
        if part == home or home in part.parents:
            if info.st_uid != os.getuid() or info.st_mode & 0o022:
                raise Refusal('Directory must be owned and not group/world writable: ' + str(part))


@contextmanager
def lock_file(path, create=False):
    fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW | (os.O_CREAT if create else 0), 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or info.st_mode & 0o022:
            raise Refusal('Unsafe lock file: ' + str(path))
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise Refusal('Lock held by another build or cleaner: ' + str(path)) from error
        yield
    finally:
        os.close(fd)


def validate_profile(profile, root, home):
    validate_directory(profile, home)
    for parent in (root, profile.parent, profile):
        marker = parent / '.in-use'
        if marker.exists() or marker.is_symlink():
            raise Refusal('Preserving in-use marker: ' + str(marker))
    validate_directory(profile / 'deps', home)
    if not (profile / '.cargo-lock').is_file():
        raise Refusal('Missing Cargo profile lock: ' + str(profile))


def clean(home, min_free_bytes=40 * GIB, apply=False):
    if min_free_bytes <= 0:
        raise Refusal('Headroom must be positive')
    home = Path(home)
    root = home / 'ci-target/intentd/build-aarch64-apple-darwin'
    validate_directory(home, home)
    before = free_bytes(home)
    report = dict(before_free_bytes=before, min_free_bytes=min_free_bytes,
                  candidates=[], deleted=[], apply=apply)
    emit(event='before', **report)
    if before < min_free_bytes and (root.exists() or root.is_symlink()):
        validate_directory(root, home)
        if not shutil.rmtree.avoids_symlink_attacks:
            raise Refusal('Python lacks symlink-safe rmtree')
        assert_idle_runner()
        with lock_file(root / '.macos-cleanup.lock', create=True):
            profiles = [root / name for name in PROFILES
                        if (root / name).exists() or (root / name).is_symlink()]
            # Validate and lock ALL candidates before deleting any of them.
            with ExitStack() as locks:
                for profile in profiles:
                    validate_profile(profile, root, home)
                    for name in CARGO_LOCKS:
                        locks.enter_context(lock_file(profile / name, create=name != '.cargo-lock'))
                profiles.sort(key=lambda path: (path.stat().st_mtime_ns, str(path)))
                report['candidates'] = [str(path) for path in profiles]
                for profile in profiles:
                    emit(event='candidate', path=str(profile))
                    if not apply:
                        continue
                    if free_bytes(home) >= min_free_bytes:
                        break
                    assert_idle_runner()
                    validate_profile(profile, root, home)
                    emit(event='deleting', path=str(profile))
                    # Never unlink Cargo's lock or its parent: a Cargo process
                    # starting now must block on this same locked inode.
                    for child in sorted(profile.iterdir()):
                        if child.name in CARGO_LOCKS:
                            continue
                        if child.is_dir() and not child.is_symlink():
                            shutil.rmtree(child)
                        else:
                            child.unlink()
                        report['deleted'].append(str(child))
                    (profile / 'deps').mkdir(mode=0o755)
    after = free_bytes(home)
    report.update(after_free_bytes=after, recovered_bytes=after - before,
                  adequate_headroom=after >= min_free_bytes)
    emit(event='summary', **report)
    if apply and after < min_free_bytes:
        raise Refusal('Insufficient disk headroom after safe candidates exhausted')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Delete eligible profiles; default only reports')
    parser.add_argument('--min-free-gib', type=int, default=40)
    args = parser.parse_args()
    try:
        if platform.system() != 'Darwin' or os.environ.get('RUNNER_NAME') != 'm1mac' or os.environ.get('GITHUB_ACTIONS') != 'true':
            raise Refusal('Run only in an Actions job on m1mac')
        # Do not trust a user-supplied HOME or target path for deletion scope.
        clean(Path(pwd.getpwuid(os.getuid()).pw_dir), args.min_free_gib * GIB, args.apply)
    except (Refusal, OSError, subprocess.SubprocessError) as error:
        emit(event='refused', reason=str(error))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
