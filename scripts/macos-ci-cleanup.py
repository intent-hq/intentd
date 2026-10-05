#!/usr/bin/env python3
"""Reclaim only inactive m1mac Cargo profiles until 40 GiB is free.

Run on the m1mac Actions runner, before any build step. Refuse other active
workers or compilers, including orphaned builds. A local advisory lock serializes
cleaners. Hold Cargo profile locks and preserve their inodes and parent directories
so newly scheduled builds must wait, even with multiple runner listeners. No
age-based expiry or force flag: unknown ownership fails closed. This is not a general developer cleaner.

Only two exact debug profile paths below the account's CI target are eligible. No
release profiles, sources, registry, toolchains, credentials, checkout or other
repo are pruned.
Internal symlinks are unlinked relative to held directory descriptors, never
traversed. All containing paths and locks must be real owned directories/files. Reports use bytes; free
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
import stat
import subprocess

GIB = 1024 ** 3
CARGO_LOCKS = ('.cargo-lock', '.cargo-artifact-lock', '.cargo-build-lock')
PROFILES = ('debug', 'aarch64-apple-darwin/debug')


class Refusal(RuntimeError):
    pass


def emit(**record):
    print(json.dumps(record, sort_keys=True), flush=True)


def free_bytes(path):
    info = os.fstatvfs(path)
    return info.f_bavail * info.f_frsize


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


def identity(info):
    return info.st_dev, info.st_ino


class Directory:
    """An open directory plus its still-open, nofollow ancestry."""
    def __init__(self, fd, path, links=()):
        self.fd, self.path, self.links = fd, path, links

    def verify(self):
        for parent_fd, name, fd in self.links:
            info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode) or identity(info) != identity(os.fstat(fd)):
                raise Refusal('Directory identity changed: ' + str(self.path))

    def child(self, name, stack, owned=True):
        if name in ('', '.', '..') or '/' in name:
            raise Refusal('Unsafe directory component: ' + name)
        try:
            fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self.fd)
        except OSError as error:
            raise Refusal('Cannot open safe directory: ' + str(self.path / name)) from error
        stack.callback(os.close, fd)
        info = os.fstat(fd)
        if owned and (info.st_uid != os.getuid() or info.st_mode & 0o022):
            raise Refusal('Directory must be owned and not group/world writable: ' + str(self.path / name))
        result = Directory(fd, self.path / name, (*self.links, (self.fd, name, fd)))
        result.verify()
        return result


def open_home(home, stack):
    if not home.is_absolute() or '..' in home.parts:
        raise Refusal('Unsafe home path: ' + str(home))
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    stack.callback(os.close, fd)
    directory = Directory(fd, Path('/'))
    for component in home.parts[1:]:
        directory = directory.child(component, stack, owned=directory.path / component == home)
    return directory


class LockedFile:
    def __init__(self, directory, name, fd):
        self.directory, self.name, self.fd = directory, name, fd

    def verify(self):
        self.directory.verify()
        info = os.stat(self.name, dir_fd=self.directory.fd, follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode) or identity(info) != identity(os.fstat(self.fd)):
            raise Refusal('Lock identity changed: ' + str(self.directory.path / self.name))


@contextmanager
def lock_file(directory, name, create=False):
    directory.verify()
    fd = os.open(name, os.O_RDWR | os.O_NOFOLLOW | (os.O_CREAT if create else 0), 0o600,
                 dir_fd=directory.fd)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or info.st_mode & 0o022:
            raise Refusal('Unsafe lock file: ' + str(directory.path / name))
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise Refusal('Lock held by another build or cleaner: ' + str(directory.path / name)) from error
        locked = LockedFile(directory, name, fd)
        locked.verify()
        yield locked
    finally:
        os.close(fd)


def has_entry(directory, name):
    try:
        os.stat(name, dir_fd=directory.fd, follow_symlinks=False)
        return True
    except FileNotFoundError:
        return False


def validate_profile(profile, root, parent, stack):
    for directory in (root, parent, profile):
        directory.verify()
        if has_entry(directory, '.in-use'):
            raise Refusal('Preserving in-use marker: ' + str(directory.path))
    profile.child('deps', stack)
    if not has_entry(profile, '.cargo-lock'):
        raise Refusal('Missing Cargo profile lock: ' + str(profile.path))


def remove_entry(parent_fd, name):
    """Unlink relative to an anchored parent; never traverse a symlink.

    Implemented with dir_fd operations supported by macOS Python 3.9 too
    (shutil.rmtree only gained its dir_fd argument in Python 3.11).
    """
    info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if not stat.S_ISDIR(info.st_mode):
        os.unlink(name, dir_fd=parent_fd)
        return
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
    try:
        opened = os.fstat(fd)
        if identity(opened) != identity(info) or opened.st_dev != os.fstat(parent_fd).st_dev:
            raise Refusal('Directory replaced or mount boundary encountered: ' + name)
        for child in os.listdir(fd):
            remove_entry(fd, child)
        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if not stat.S_ISDIR(current.st_mode) or identity(current) != identity(opened):
            raise Refusal('Directory changed during deletion: ' + name)
        os.rmdir(name, dir_fd=parent_fd)
    finally:
        os.close(fd)


def clean(home, min_free_bytes=40 * GIB, apply=False):
    if min_free_bytes <= 0:
        raise Refusal('Headroom must be positive')
    home = Path(home)
    with ExitStack() as stack:
        home_dir = open_home(home, stack)
        before = free_bytes(home_dir.fd)
        report = dict(before_free_bytes=before, min_free_bytes=min_free_bytes,
                      candidates=[], deleted=[], apply=apply)
        emit(event='before', **report)
        if before < min_free_bytes:
            root = home_dir
            for component in ('ci-target', 'intentd', 'build-aarch64-apple-darwin'):
                if not has_entry(root, component):
                    root = None
                    break
                root = root.child(component, stack)
            if root is not None:
                assert_idle_runner()
                locks = [stack.enter_context(lock_file(root, '.macos-cleanup.lock', create=True))]
                profiles = []
                activity = {}
                # Open/validate/lock every eligible profile before any deletion.
                for relative in PROFILES:
                    parent = root
                    for component in relative.split('/')[:-1]:
                        if not has_entry(parent, component):
                            parent = None
                            break
                        parent = parent.child(component, stack)
                    if parent is None or not has_entry(parent, relative.split('/')[-1]):
                        continue
                    profile = parent.child(relative.split('/')[-1], stack)
                    validate_profile(profile, root, parent, stack)
                    activity[profile.fd] = os.fstat(profile.fd).st_mtime_ns
                    for name in CARGO_LOCKS:
                        locks.append(stack.enter_context(lock_file(profile, name, create=name != '.cargo-lock')))
                    profiles.append((profile, parent))
                profiles.sort(key=lambda pair: (activity[pair[0].fd], str(pair[0].path)))
                report['candidates'] = [str(profile.path) for profile, _ in profiles]
                for profile, parent in profiles:
                    emit(event='candidate', path=str(profile.path))
                    if not apply:
                        continue
                    if free_bytes(home_dir.fd) >= min_free_bytes:
                        break
                    assert_idle_runner()
                    validate_profile(profile, root, parent, stack)
                    emit(event='deleting', path=str(profile.path))
                    for child in sorted(os.listdir(profile.fd)):
                        if child in CARGO_LOCKS:
                            continue
                        for lock in locks:
                            lock.verify()
                        remove_entry(profile.fd, child)
                        report['deleted'].append(str(profile.path / child))
                    for lock in locks:
                        lock.verify()
                    os.mkdir('deps', mode=0o755, dir_fd=profile.fd)
        after = free_bytes(home_dir.fd)
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
