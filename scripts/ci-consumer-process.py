#!/usr/bin/env python3
"""Supervise one Linux coverage command and reap its owned descendants."""
import argparse
import ctypes
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid


def signal_child(pid, number):
    """Pin the process identity and signal only our current direct children."""
    try:
        descriptor = os.pidfd_open(pid)
    except ProcessLookupError:
        return
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        if int(fields[1]) == os.getpid():
            signal.pidfd_send_signal(descriptor, number)
    except (FileNotFoundError, ProcessLookupError):
        pass
    finally:
        os.close(descriptor)


def supervise(root, command):
    root = root.resolve()
    owner = json.loads((root / 'storage.json').read_text())
    token = 'run-' + '-'.join(os.environ[k] for k in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_JOB'))
    if owner['root'] != str(root) or owner['token'] != token or root.name != token:
        raise ValueError('refusing to supervise another run')
    # Dedicated supervisor process: reparent even detached grandchildren here.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), 'cannot own consumer descendants')
    nonce = uuid.uuid4().hex
    active = root / 'consumer-active.json'
    with active.open('x') as stream:
        json.dump({'id': nonce, 'owner': token}, stream)
    started = time.monotonic()
    cancelled, cancelled_at = None, None
    def cancel(number, _frame):
        nonlocal cancelled, cancelled_at
        if cancelled is None:
            cancelled, cancelled_at = number, time.monotonic()
    for number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(number, cancel)
    process, main_result, forwarded, reaped = None, None, False, 0
    exited_at = None
    try:
        if cancelled is None:
            process = subprocess.Popen(command, start_new_session=True)
        while process is not None:
            while True:
                try:
                    pid, status = os.waitpid(-1, os.WNOHANG)
                except ChildProcessError:
                    pid = -1
                if pid <= 0:
                    break
                reaped += 1
                if pid == process.pid:
                    main_result = os.waitstatus_to_exitcode(status)
                    exited_at = time.monotonic()
                    process.returncode = main_result
            if pid == -1:
                break  # Every descendant has exited and been reaped.
            children = [int(p) for p in Path(f'/proc/self/task/{os.getpid()}/children').read_text().split()]
            if cancelled is not None and not forwarded:
                if main_result is None:
                    signal_child(process.pid, cancelled)
                forwarded = True
            stopping_at = cancelled_at if cancelled_at is not None else exited_at
            if stopping_at is not None:
                elapsed = time.monotonic() - stopping_at
                if main_result is not None or elapsed >= 2:
                    for child in children:
                        signal_child(child, signal.SIGKILL if elapsed >= 3 else signal.SIGTERM)
            time.sleep(0.02)
        result = 128 + cancelled if cancelled is not None else main_result if main_result is not None else 1
        result = result if result >= 0 else 128 - result
        receipt = root / ('consumer-process-' + nonce + '.json')
        temporary = receipt.with_suffix('.tmp')
        temporary.write_text(json.dumps({'id': nonce, 'quiesced': True, 'exit_code': result,
                                        'cancel_signal': cancelled, 'reaped': reaped,
                                        'seconds': time.monotonic() - started}, indent=2) + '\n')
        temporary.replace(receipt)
        active.unlink()  # Cleanup is allowed only after the final child is gone.
        return result
    except BaseException:
        # A supervisor failure/hard kill leaves the marker: cleanup must refuse
        # to race unknown writers. Never turn this into successful test evidence.
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=os.environ.get('NATIVE_ARTIFACT_ROOT'))
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if sys.platform != 'linux' or not args.root or not command:
        parser.error('requires Linux, owned storage and a command')
    sys.exit(supervise(args.root, command))
