#!/usr/bin/env python3
"""One native supervisor discriminator; no Rust build or boundary invocation."""
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import selectors
import signal
import subprocess
import sys
import time

SECONDS = 120
UNCONFIRMED = []  # Retain Popen anchors if cleanup cannot be established.


class Log:
    def __init__(self, path):
        self.start = time.monotonic()
        self.file = path.open('x')

    def emit(self, arm, stage, **fields):
        self.file.write(json.dumps(dict(arm=arm, stage=stage,
                                       elapsed=time.monotonic()-self.start, **fields))+'\n')
        self.file.flush()

    def close(self):
        self.file.close()


def operation(log, arm, stage, call):
    started = time.monotonic()
    try:
        result = dict(ok=True, value=call())
    except Exception as error:
        result = dict(ok=False, error_type=type(error).__name__,
                      errno=getattr(error, 'errno', None), error=str(error))
    log.emit(arm, stage, seconds=time.monotonic()-started, **result)
    return result


def remaining(deadline, cap=1):
    left = deadline-time.monotonic()
    if left <= 0:
        raise TimeoutError('control phase budget exhausted')
    return min(cap, left)


def parse_ps(text, pgid):
    result = []
    for line in text.splitlines():
        pid, ppid, group, state = line.split()
        if int(group) == pgid:
            result.append(dict(pid=int(pid), ppid=int(ppid), pgid=int(group), state=state))
    return result


def snapshot(log, arm, stage, pid, deadline):
    # ps never reaps the child. Avoid command arguments and environment values.
    return operation(log, arm, stage, lambda: parse_ps(subprocess.check_output(
        ['ps', '-axo', 'pid=,ppid=,pgid=,stat='], text=True,
        timeout=remaining(deadline)), pid))


def matches(members, pid, state):
    own = [r for r in members if r['pid'] == pid]
    if len(own) != 1:
        return False
    code = own[0]['state']
    return (code.startswith('Z') if state == 'exited' else
            code.startswith('T') if state == 'stopped' else
            not code.startswith(('Z', 'T')))


def observe(log, arm, stage, child, wanted, deadline):
    while True:
        result = snapshot(log, arm, stage, child.pid, deadline)
        if not result['ok']:
            return False
        if matches(result['value'], child.pid, wanted):
            return True
        if deadline-time.monotonic() <= .02:
            return False
        time.sleep(.02)


class OwnedChild:
    def __init__(self, process):
        self.process = process
        self.pid = process.pid if process else None
        self.owned = process is not None

    def send(self, sig, group=True):
        if not self.owned:
            raise RuntimeError('refusing signal after child ownership ended')
        # No poll, wait or Popen context manager before the final cleanup reap.
        if group:
            os.killpg(self.pid, sig)
        else:
            os.kill(self.pid, sig)


def read_eof(child, deadline):
    data = b''
    with selectors.DefaultSelector() as selector:
        selector.register(child.process.stdout, selectors.EVENT_READ)
        while True:
            for key, _ in selector.select(remaining(deadline, .1)):
                block = os.read(key.fileobj.fileno(), 1024)
                if not block:
                    return dict(eof=True, ready=data == b'ready\n')
                data += block
                if len(data) > 32:
                    raise RuntimeError('unexpected control output')


def cleanup(child, log, arm, deadline):
    # The children contain no subprocess/fork calls; the only signal fallback
    # is our own unreaped Popen PID, never an observed external worker identity.
    for sig in (signal.SIGCONT, signal.SIGKILL):
        result = operation(log, arm, 'cleanup.killpg.'+sig.name,
                           lambda: child.send(sig))
        if not result['ok']:
            operation(log, arm, 'cleanup.kill.'+sig.name,
                      lambda: child.send(sig, group=False))
    while time.monotonic() < deadline:
        view = snapshot(log, arm, 'cleanup.before_reap', child.pid, deadline)
        if not view['ok']:
            break
        if not any(not r['state'].startswith('Z') for r in view['value']):
            reaped = operation(log, arm, 'cleanup.wait',
                               lambda: child.process.wait(timeout=remaining(deadline)))
            if reaped['ok']:
                child.owned = False
                return dict(cleanup_confirmed=True, exit_code=reaped['value'],
                            final_owned_group=view['value'])
            break
        time.sleep(min(.02, max(0, deadline-time.monotonic())))
    UNCONFIRMED.append(child)
    return dict(cleanup_confirmed=False)


def run_arm(arm, log, global_deadline):
    remaining(global_deadline, 1)
    if arm not in ('normal_exit', 'live_child'):
        raise ValueError('unknown control arm')
    # Same EOF handshake in both arms; only the live arm waits after EOF.
    code = "import os,time; os.write(1,b'ready\\n'); os.close(1); os.close(2)"
    if arm == 'live_child':
        code += '; time.sleep(60)'
    started = time.monotonic()
    process = subprocess.Popen([sys.executable, '-I', '-S', '-c', code],
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               start_new_session=True)
    child = OwnedChild(process)
    result = dict(arm=arm, pid=child.pid, eof=False, state_confirmed=False,
                  cleanup_confirmed=False)
    try:
        phase_deadline = min(global_deadline-10, time.monotonic()+10)
        log.emit(arm, 'spawn', pid=child.pid, parent_pid=os.getpid(), expected_pgid=child.pid)
        snapshot(log, arm, 'before_eof', child.pid, phase_deadline)
        eof = operation(log, arm, 'read_eof', lambda: read_eof(child, phase_deadline))
        result['eof'] = eof['ok'] and eof['value']['eof'] and eof['value']['ready']
        if not result['eof']:
            return result
        wanted = 'exited' if arm == 'normal_exit' else 'live'
        result['state_confirmed'] = observe(log, arm, 'after_eof.'+wanted,
                                           child, wanted, phase_deadline)
        if not result['state_confirmed']:
            return result
        stopped = operation(log, arm, 'freeze.killpg.SIGSTOP', lambda: child.send(signal.SIGSTOP))
        result['stop'] = stopped
        snapshot(log, arm, 'after_freeze', child.pid, phase_deadline)
        if stopped['ok'] and arm == 'live_child':
            result['stop_observed'] = observe(log, arm, 'freeze.stopped', child, 'stopped', phase_deadline)
        resumed = operation(log, arm, 'resume.killpg.SIGCONT', lambda: child.send(signal.SIGCONT))
        result['resume'] = resumed
        snapshot(log, arm, 'after_resume', child.pid, phase_deadline)
        if resumed['ok'] and arm == 'live_child':
            result['resume_observed'] = observe(log, arm, 'resume.live', child, 'live', phase_deadline)
    except Exception as error:
        result['error'] = dict(type=type(error).__name__, errno=getattr(error, 'errno', None), message=str(error))
        log.emit(arm, 'arm_error', **result['error'])
    finally:
        try:
            result.update(cleanup(child, log, arm, min(global_deadline, time.monotonic()+8)))
        except Exception as error:
            if child.owned and child not in UNCONFIRMED:
                UNCONFIRMED.append(child)
            result['cleanup_error'] = dict(type=type(error).__name__, errno=getattr(error, 'errno', None), message=str(error))
            result['cleanup_confirmed'] = False
        finally:
            process.stdout.close()
            result['seconds'] = time.monotonic()-started
            log.emit(arm, 'arm_result', result=result)
    return result


def guard(env, actual, system, machine):
    if system != 'Darwin' or machine != 'arm64':
        raise RuntimeError('control requires native macOS ARM64')
    if env.get('GITHUB_EVENT_NAME') != 'workflow_dispatch' or env.get('GITHUB_RUN_ATTEMPT') != '1':
        raise RuntimeError('requires first manual dispatch; reruns prohibited')
    expected = env.get('EXPECTED_SOURCE_SHA', '')
    if not re.fullmatch('[0-9a-f]{40}', expected) or actual != expected or env.get('GITHUB_SHA') != expected:
        raise RuntimeError('reviewed source SHA mismatch')


def prepare_directory(directory, root):
    if not directory.is_absolute() or directory.resolve().is_relative_to(root.resolve()):
        raise RuntimeError('evidence must be absolute and outside checkout')
    directory.mkdir(parents=True, exist_ok=False)


def alarm(_signum, _frame):
    raise TimeoutError('control measurement budget exhausted; reserve final cleanup time')


def main():
    started = time.monotonic()
    deadline = started+SECONDS
    directory = Path(os.environ['SUPERVISOR_CONTROL_EVIDENCE'])
    prepare_directory(directory, Path.cwd())
    log = Log(directory/'events.jsonl')
    result = dict(kind='supervisor-only-control-not-boundary-proof', complete=False, arms=[])
    signal.signal(signal.SIGALRM, alarm)
    signal.setitimer(signal.ITIMER_REAL, SECONDS-10)
    try:
        actual = subprocess.check_output(['git','rev-parse','HEAD'],text=True,timeout=3).strip()
        guard(os.environ, actual, platform.system(), platform.machine())
        subprocess.run(['git','diff','--exit-code','HEAD'],check=True,timeout=3)
        result.update(source_sha=actual, run_id=os.environ.get('GITHUB_RUN_ID'), attempt=1,
                      system=platform.platform(), machine=platform.machine(),
                      python=dict(path=sys.executable,version=sys.version,
                                  sha256=hashlib.sha256(Path(sys.executable).read_bytes()).hexdigest()),
                      source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                      supervisor_pid=os.getpid(), supervisor_pgid=os.getpgrp(),
                      budget_seconds=SECONDS)
        (directory/'invoked').open('x').close()
        for name in ('normal_exit', 'live_child'):
            arm = run_arm(name,log,deadline)
            result['arms'].append(arm)
            if not arm['cleanup_confirmed']:
                break
        result['complete'] = (len(result['arms']) == 2 and
            all(a['cleanup_confirmed'] and a['state_confirmed'] and a['eof'] and
                'stop' in a and 'resume' in a and 'error' not in a for a in result['arms']))
    except Exception as error:
        result['error'] = dict(type=type(error).__name__, errno=getattr(error,'errno',None), message=str(error))
    finally:
        signal.setitimer(signal.ITIMER_REAL,0)
        result['seconds'] = time.monotonic()-started
        if result['seconds'] > SECONDS:
            result['complete'] = False
        (directory/'control.json').write_text(json.dumps(result,indent=2)+'\n')
        log.close()
        manifest=[dict(path=p.name,bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest())
                  for p in sorted(directory.iterdir()) if p.is_file()]
        (directory/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return 0 if result['complete'] else 1


if __name__ == '__main__':
    sys.exit(main())
