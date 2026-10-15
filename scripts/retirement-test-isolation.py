#!/usr/bin/env python3
"""Default-deny isolation for retirement tests, including their child processes.

These suites run on Linux locally, in release-scripts, and in the cleanup job.
Install before loading production modules. Unsupported hosts fail closed: never
silently run cancellation tests without the inherited OS network restriction.
"""
import atexit
import ctypes
import errno
import os
from pathlib import Path
import platform
import sys
import tempfile


def install():
    if getattr(sys, '_retirement_tests_isolated', False):
        return
    syscall_tables = {
        'x86_64': (0xC000003E, (41, 42, 44, 46, 307)),
        'aarch64': (0xC00000B7, (198, 203, 206, 211, 269)),
    }
    if sys.platform != 'linux' or platform.machine() not in syscall_tables:
        raise RuntimeError('Retirement tests require Linux network isolation; refusing unguarded tests')
    arch, denied = syscall_tables[platform.machine()]

    class Filter(ctypes.Structure):
        _fields_ = [('code', ctypes.c_ushort), ('jt', ctypes.c_ubyte),
                    ('jf', ctypes.c_ubyte), ('k', ctypes.c_uint32)]

    class Program(ctypes.Structure):
        _fields_ = [('len', ctypes.c_ushort), ('filter', ctypes.POINTER(Filter))]

    # seccomp_data.arch at byte 4, syscall number at byte 0. Reject a changed
    # architecture and block socket/connect/send syscalls in all descendants.
    instructions = [(0x20, 0, 0, 4), (0x15, 1, 0, arch), (0x06, 0, 0, 0x80000000),
                    (0x20, 0, 0, 0), (0x54, 0, 0, 0xBFFFFFFF)]  # mask x32 bit
    for number in denied:
        instructions += [(0x15, 0, 1, number), (0x06, 0, 0, 0x00050000 | errno.EPERM)]
    instructions += [(0x06, 0, 0, 0x7FFF0000)]
    filters = (Filter * len(instructions))(*(Filter(*row) for row in instructions))
    program = Program(len(filters), filters)
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(38, 1, 0, 0, 0) != 0 or libc.prctl(22, 2, ctypes.byref(program), 0, 0) != 0:
        raise RuntimeError('Cannot install inherited test network denial; refusing tests')

    directory = tempfile.TemporaryDirectory(prefix='retirement-test-isolation-')
    atexit.register(directory.cleanup)
    root = Path(directory.name)
    gh = root / 'gh'
    gh.write_text('#!/bin/sh\necho RETIREMENT_TEST_GH_DENIED >&2\nexit 97\n')
    gh.chmod(0o700)
    os.environ['PATH'] = str(root) + os.pathsep + os.environ.get('PATH', '')
    os.environ['GH_CONFIG_DIR'] = str(root / 'empty-gh-config')
    for name in ('GH_TOKEN', 'GITHUB_TOKEN', 'GH_ENTERPRISE_TOKEN', 'GITHUB_ENTERPRISE_TOKEN',
                 'RETIREMENT_READ_TOKEN', 'CI_QUEUE_READ_TOKEN'):
        os.environ.pop(name, None)

    def audit(event, args):
        if event == 'subprocess.Popen' and Path(os.fsdecode(args[0])).name in ('gh', 'curl', 'wget'):
            raise RuntimeError('Retirement tests deny real network CLI; mock the transport explicitly')
    sys.addaudithook(audit)
    sys._retirement_tests_isolated = True
