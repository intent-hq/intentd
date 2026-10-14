#!/usr/bin/env python3
"""Reclaim inactive 5090pc Cargo debug artifacts until 40 GiB is free.

Default: dry run. Only the two intentd CI targets are eligible. Sources,
release profiles, toolchains, registry, credentials and other caches are excluded.
Windows handles pin every ancestor against rename/deletion; reparse points and
multiply-linked files are refused. Cargo locks survive cleanup and remain held
through deletion. An unknown/live runner or compiler refuses cleanup.
"""
import argparse
from contextlib import ExitStack, contextmanager
import ctypes
from ctypes import wintypes as W
import json
import os
from pathlib import Path, PureWindowsPath
import platform
import shutil
import subprocess

GIB = 1024 ** 3
TARGETS = ('build-x86_64-pc-windows-msvc', 'daemon-lifecycle-windows')
PROFILES = ('debug', 'x86_64-pc-windows-msvc/debug')
CARGO_LOCKS = ('.cargo-lock', '.cargo-artifact-lock', '.cargo-build-lock')


class Refusal(RuntimeError):
    pass


def emit(**record):
    print(json.dumps(record, sort_keys=True), flush=True)


def validate_processes(rows, pid):
    if not isinstance(rows, list) or not rows:
        raise Refusal('Cannot read process ownership')
    processes = {}
    for row in rows:
        if (not isinstance(row, dict) or type(row.get('ProcessId')) is not int
                or type(row.get('ParentProcessId')) is not int or not isinstance(row.get('Name'), str)
                or not row['Name'] or row['ProcessId'] in processes):
            raise Refusal('Cannot parse process ownership')
        processes[row['ProcessId']] = (row['ParentProcessId'], row['Name'].lower())
    ancestors = set()
    current = pid
    while current in processes and current not in ancestors:
        ancestors.add(current)
        current = processes[current][0]
    workers = {p for p, (_, name) in processes.items() if name == 'runner.worker.exe'}
    listeners = {p for p, (_, name) in processes.items() if name == 'runner.listener.exe'}
    if len(workers) != 1 or not workers <= ancestors or not listeners & ancestors:
        raise Refusal('Require our listener and only our own active worker')
    compilers = {'cargo.exe', 'rustc.exe', 'rustdoc.exe', 'rust-lld.exe', 'link.exe',
                 'cl.exe', 'clang.exe', 'clang-cl.exe', 'lld-link.exe', 'sccache.exe',
                 'cargo-clippy.exe', 'clippy-driver.exe', 'msbuild.exe'}
    if any(name in compilers or name.startswith('build-script-') for _, name in processes.values()):
        raise Refusal('Active build processes')


def assert_idle_runner():
    command = ('@(Get-CimInstance Win32_Process -ErrorAction Stop | '
               'Select-Object ProcessId,ParentProcessId,Name) | ConvertTo-Json -Compress')
    result = subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', command],
                            capture_output=True, text=True, check=True, timeout=30)
    validate_processes(json.loads(result.stdout), os.getpid())


class FileInfo(ctypes.Structure):
    _fields_ = [('attributes', W.DWORD), ('creation', W.FILETIME), ('access', W.FILETIME),
                ('write', W.FILETIME), ('volume', W.DWORD), ('size_high', W.DWORD),
                ('size_low', W.DWORD), ('links', W.DWORD), ('index_high', W.DWORD), ('index_low', W.DWORD)]


class Overlapped(ctypes.Structure):
    _fields_ = [('internal', ctypes.c_size_t), ('internal_high', ctypes.c_size_t),
                ('offset', W.DWORD), ('offset_high', W.DWORD), ('event', W.HANDLE)]


class Native:
    """Small Win32 surface: handles deny delete sharing, locks cover the whole file.

    https://learn.microsoft.com/windows/win32/api/fileapi/nf-fileapi-createfilew
    https://learn.microsoft.com/windows/win32/api/fileapi/nf-fileapi-lockfileex
    """
    def __init__(self):
        if os.name != 'nt':
            raise Refusal('Windows native APIs required')
        self.k = ctypes.WinDLL('kernel32', use_last_error=True)
        self.a = ctypes.WinDLL('advapi32', use_last_error=True)
        self.u = ctypes.WinDLL('userenv', use_last_error=True)
        signatures = [
            (self.k.CreateFileW, W.HANDLE, [W.LPCWSTR, W.DWORD, W.DWORD, W.LPVOID, W.DWORD, W.DWORD, W.HANDLE]),
            (self.k.CloseHandle, W.BOOL, [W.HANDLE]),
            (self.k.GetCurrentProcess, W.HANDLE, []),
            (self.k.GetFileInformationByHandle, W.BOOL, [W.HANDLE, ctypes.POINTER(FileInfo)]),
            (self.k.LockFileEx, W.BOOL, [W.HANDLE, W.DWORD, W.DWORD, W.DWORD, W.DWORD, ctypes.POINTER(Overlapped)]),
            (self.k.UnlockFileEx, W.BOOL, [W.HANDLE, W.DWORD, W.DWORD, W.DWORD, ctypes.POINTER(Overlapped)]),
            (self.k.SetFileInformationByHandle, W.BOOL, [W.HANDLE, ctypes.c_int, W.LPVOID, W.DWORD]),
            (self.k.LocalFree, W.HANDLE, [W.HANDLE]),
            (self.a.OpenProcessToken, W.BOOL, [W.HANDLE, W.DWORD, ctypes.POINTER(W.HANDLE)]),
            (self.a.GetTokenInformation, W.BOOL, [W.HANDLE, ctypes.c_int, W.LPVOID, W.DWORD, ctypes.POINTER(W.DWORD)]),
            (self.a.GetSecurityInfo, W.DWORD, [W.HANDLE, ctypes.c_int, W.DWORD, ctypes.POINTER(W.LPVOID),
                                           W.LPVOID, W.LPVOID, W.LPVOID, ctypes.POINTER(W.LPVOID)]),
            (self.a.EqualSid, W.BOOL, [W.LPVOID, W.LPVOID]),
            (self.a.IsWellKnownSid, W.BOOL, [W.LPVOID, ctypes.c_int]),
            (self.u.GetUserProfileDirectoryW, W.BOOL, [W.HANDLE, W.LPWSTR, ctypes.POINTER(W.DWORD)]),
        ]
        for function, result, args in signatures:
            function.restype, function.argtypes = result, args
        token = W.HANDLE()
        self.check(self.a.OpenProcessToken(self.k.GetCurrentProcess(), 8, ctypes.byref(token)))
        try:
            # Elevated accounts can have Administrators as TokenOwner while
            # their existing profile and artifacts remain owned by TokenUser.
            self.owner_buffers, self.owners = [], []
            for token_class in (1, 4):  # TokenUser, TokenOwner: both start with a SID pointer
                size = W.DWORD()
                self.a.GetTokenInformation(token, token_class, None, 0, ctypes.byref(size))
                if not size.value:
                    raise Refusal('Cannot resolve account owner')
                buffer = ctypes.create_string_buffer(size.value)
                self.check(self.a.GetTokenInformation(token, token_class, buffer, size, ctypes.byref(size)))
                self.owner_buffers.append(buffer)
                self.owners.append(ctypes.cast(buffer, ctypes.POINTER(W.LPVOID))[0])
            size = W.DWORD()
            self.u.GetUserProfileDirectoryW(token, None, ctypes.byref(size))
            if not size.value:
                raise Refusal('Cannot resolve account profile')
            home = ctypes.create_unicode_buffer(size.value)
            self.check(self.u.GetUserProfileDirectoryW(token, home, ctypes.byref(size)))
            self.home = Path(home.value)
        finally:
            self.k.CloseHandle(token)

    @staticmethod
    def check(ok):
        if not ok:
            raise ctypes.WinError(ctypes.get_last_error())

    def info(self, handle):
        info = FileInfo()
        self.check(self.k.GetFileInformationByHandle(handle, ctypes.byref(info)))
        return info

    def owned(self, handle, path, *, allow_system_owner=False):
        owner, descriptor = W.LPVOID(), W.LPVOID()
        error = self.a.GetSecurityInfo(handle, 1, 1, ctypes.byref(owner), None, None, None, ctypes.byref(descriptor))
        if error:
            raise ctypes.WinError(error)
        try:
            account_owner = owner and any(self.a.EqualSid(owner, allowed) for allowed in self.owners)
            system_profile = owner and allow_system_owner and self.a.IsWellKnownSid(owner, 22)
            if not account_owner and not system_profile:
                raise Refusal('Path has an unrelated owner: ' + str(path))
        finally:
            if descriptor:
                self.k.LocalFree(descriptor)

    @contextmanager
    def open(self, path, *, directory=False, create=False, delete=False, owned=True, writable=False,
             allow_system_owner=False):
        if allow_system_owner and (not directory or delete or create or writable):
            raise Refusal('SYSTEM ownership is only allowed for a read-only profile anchor')
        # No FILE_SHARE_DELETE: an opened object cannot be renamed or replaced.
        access = 0x20000 | 0x80  # READ_CONTROL, FILE_READ_ATTRIBUTES
        if directory:
            # Metadata-only opens do not participate in Windows share checks.
            # FILE_LIST_DIRECTORY makes the no-delete-sharing pin effective.
            access |= 0x1
        if writable:
            access |= 0x80000000 | 0x40000000
        if delete:
            access |= 0x10000
        handle = self.k.CreateFileW(str(path), access, 3, None, 4 if create else 3,
                                    0x02000000 | 0x00200000, None)
        if handle == ctypes.c_void_p(-1).value:
            self.check(False)
        try:
            info = self.info(handle)
            if info.attributes & 0x400:
                raise Refusal('Reparse point refused: ' + str(path))
            if bool(info.attributes & 0x10) != directory:
                raise Refusal('Unexpected path type: ' + str(path))
            if not directory and info.links != 1:
                raise Refusal('Multiply-linked file refused: ' + str(path))
            if owned:
                self.owned(handle, path, allow_system_owner=allow_system_owner)
            yield handle
        finally:
            self.k.CloseHandle(handle)

    @contextmanager
    def lock(self, path, create=False):
        with self.open(path, create=create, writable=True) as handle:
            overlap = Overlapped()
            if not self.k.LockFileEx(handle, 3, 0, 0xffffffff, 0xffffffff, ctypes.byref(overlap)):
                raise Refusal('Lock held or unavailable: ' + str(path))
            try:
                yield handle
            finally:
                self.check(self.k.UnlockFileEx(handle, 0, 0xffffffff, 0xffffffff, ctypes.byref(overlap)))

    def delete(self, handle):
        disposition = ctypes.c_ubyte(1)  # FILE_DISPOSITION_INFO.DeleteFile (BOOLEAN)
        self.check(self.k.SetFileInformationByHandle(handle, 4, ctypes.byref(disposition), ctypes.sizeof(disposition)))


def validate_home(home):
    raw = PureWindowsPath(home)
    if not raw.is_absolute() or raw.drive.startswith('\\\\') or '..' in raw.parts or len(raw.parts) < 3:
        raise Refusal('Require a local account profile path')
    if any(':' in part or part.endswith((' ', '.')) for part in raw.parts[1:]):
        raise Refusal('Unsafe profile path')


def open_home(api, home, stack):
    validate_home(home)
    current = Path(home.anchor)
    stack.enter_context(api.open(current, directory=True, owned=False))
    for component in home.parts[1:]:
        current /= component
        # Windows may provision the account profile as SYSTEM. This is only
        # an anchor: all CI descendants and deletion handles require the account.
        stack.enter_context(api.open(current, directory=True, owned=current == home,
                                     allow_system_owner=current == home))


def missing(path):
    try:
        path.lstat()
        return False
    except FileNotFoundError:
        return True


def marker_guard(paths):
    if any(not missing(path / '.in-use') for path in paths):
        raise Refusal('Preserving in-use marker')


def remove_entry(api, path):
    # Ancestors are pinned. Open the entry itself before reading or deleting it.
    directory = bool(path.lstat().st_file_attributes & 0x10)
    with api.open(path, directory=directory, delete=True) as handle:
        if directory:
            for child in sorted(path.iterdir()):
                remove_entry(api, child)
        api.delete(handle)


def free_bytes(home):
    return shutil.disk_usage(home).free


def clean(home, min_free_bytes=40 * GIB, apply=False, api=None):
    if min_free_bytes <= 0:
        raise Refusal('Headroom must be positive')
    api = api or Native()
    home = Path(home)
    with ExitStack() as stack:
        open_home(api, home, stack)
        before = free_bytes(home)
        report = dict(before_free_bytes=before, min_free_bytes=min_free_bytes, candidates=[], deleted=[], apply=apply)
        emit(event='before', **report)
        if before < min_free_bytes:
            assert_idle_runner()
            root = home
            for component in ('ci-target', 'intentd'):
                root /= component
                if missing(root):
                    break
                stack.enter_context(api.open(root, directory=True))
            else:
                stack.enter_context(api.lock(root / '.windows-cleanup.lock', create=True))
                profiles = []
                # Acquire every profile and Cargo lock before deleting anything.
                for target in TARGETS:
                    target_dir = root / target
                    if missing(target_dir):
                        continue
                    stack.enter_context(api.open(target_dir, directory=True))
                    for relative in PROFILES:
                        current = target_dir
                        parents = [root, target_dir]
                        for component in relative.split('/'):
                            current /= component
                            if missing(current):
                                break
                            stack.enter_context(api.open(current, directory=True))
                            parents.append(current)
                        else:
                            marker_guard(parents)
                            with api.open(current / 'deps', directory=True):
                                pass
                            for name in CARGO_LOCKS:
                                stack.enter_context(api.lock(current / name, create=name != '.cargo-lock'))
                            profiles.append((current.stat().st_mtime_ns, str(current), current, parents))
                profiles.sort(key=lambda item: item[:2])
                report['candidates'] = [str(profile) for _, _, profile, _ in profiles]
                for _, _, profile, parents in profiles:
                    emit(event='candidate', path=str(profile))
                    if not apply:
                        continue
                    if free_bytes(home) >= min_free_bytes:
                        break
                    assert_idle_runner()
                    marker_guard(parents)
                    for child in sorted(profile.iterdir()):
                        if child.name in CARGO_LOCKS:
                            continue
                        emit(event='deleting', path=str(child))
                        remove_entry(api, child)
                        report['deleted'].append(str(child))
                    (profile / 'deps').mkdir(exist_ok=True)
        after = free_bytes(home)
        report.update(after_free_bytes=after, recovered_bytes=after-before, adequate_headroom=after >= min_free_bytes)
        emit(event='summary', **report)
        if apply and after < min_free_bytes:
            raise Refusal('Insufficient disk headroom after safe candidates exhausted')
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--min-free-gib', type=int, default=40)
    args = parser.parse_args()
    try:
        if (platform.system() != 'Windows' or os.environ.get('RUNNER_NAME', '').lower() != '5090pc'
                or os.environ.get('GITHUB_ACTIONS') != 'true'):
            raise Refusal('Run only in an Actions job on 5090pc')
        api = Native()
        clean(api.home, args.min_free_gib * GIB, args.apply, api)
    except (Refusal, OSError, ValueError, subprocess.SubprocessError) as error:
        emit(event='refused', reason=str(error))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
