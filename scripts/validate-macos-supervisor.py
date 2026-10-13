#!/usr/bin/env python3
"""Bounded native regression of the actual diagnostic supervised() path only."""
import importlib.util
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT/filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


d = load('diagnostic', 'context-boundary-diagnostic.py')
control = load('control', 'macos-supervisor-control.py')
CASES = ('normal_exit', 'paired_owner', 'missing_owner', 'malformed_owner',
         'live_timeout', 'mixed_group', 'ambiguous_observation', 'live_unconfirmed_owner')


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def validate_record(name, record, signals, error):
    rejected = name in ('missing_owner', 'malformed_owner', 'ambiguous_observation', 'live_unconfirmed_owner')
    require(bool(error) == rejected, 'unexpected error outcome: '+name)
    require(record['cleaned'] is (not rejected), 'incorrect cleanup outcome: '+name)
    if name in ('normal_exit', 'paired_owner'):
        require(record['exit_code'] == 0 and not record['timeout'], 'normal exit changed')
        require(record['cleanup_path'] == 'already_quiescent' and not signals, 'unnecessary normal-exit signal')
        require(len(record['quiescent_group_observations']) == 2, 'missing stable full-group observations')
    elif name in ('live_timeout', 'mixed_group'):
        require(record['cleanup_path'] == 'freeze_owned_group', 'live group skipped freeze')
        require(signals == [signal.SIGSTOP, signal.SIGKILL], 'live group cleanup sequence changed')
        if name == 'mixed_group':
            first = record['quiescent_group_observations'][0]
            require(any(r['pid'] == record['pid'] and r['state'].startswith('Z') for r in first)
                    and any(not r['state'].startswith('Z') for r in first), 'mixed group not actually observed')
        else:
            require(record['timeout'] and record['exit_code'] != 0, 'live deadline not exercised')
    elif name == 'live_unconfirmed_owner':
        require(signals == [signal.SIGSTOP, signal.SIGCONT] and record['resume_sent'], 'unconfirmed live owner not resumed')
    else:
        require(not signals, 'ambiguous or unconfirmed dead group was signaled')


def run_case(name, directory, log, global_deadline):
    case = directory/name
    case.mkdir()
    output = case/'child.log'
    owner = None
    code = 'pass'
    allowance = 2
    if name in ('paired_owner', 'missing_owner', 'malformed_owner', 'live_unconfirmed_owner'):
        owner = case/'owners.jsonl'
        if name == 'paired_owner':
            fields = dict(pid=123, root=str(case/'absent-scratch'))
            owner.write_text(json.dumps(dict(event='owner_start', **fields))+'\n'+json.dumps(
                dict(event='owner_cleanup', cleaned=True, scratch_exists=False, **fields))+'\n')
        if name == 'malformed_owner':
            owner.write_text('{"event":')
    if name in ('live_timeout', 'live_unconfirmed_owner'):
        code = 'import time; time.sleep(60)'
        allowance = .2
    if name == 'mixed_group':
        code = ("import subprocess,sys; subprocess.Popen([sys.executable,'-I','-S','-c',"
                "'import time; time.sleep(60)'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)")
    anchors = []
    signals = []
    real_popen, real_group, real_signal = d.subprocess.Popen, d.group_members, d.signal_owned
    real_killpg = d.os.killpg
    error = None
    result = dict(case=name, validation_passed=False, harness_cleanup_confirmed=False)

    def retain(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        if kwargs.get('start_new_session'):
            anchors.append(process)
            log.emit(name, 'owned_spawn', pid=process.pid)
        return process

    def group(pgid, timeout=2):
        try:
            actual = real_group(pgid, timeout)
            returned = actual
            if name == 'ambiguous_observation':
                # Explicit observation fault, not a claimed native process state.
                returned = [r for r in actual if r['pid'] != pgid]
            log.emit(name, 'group_observation', pgid=pgid, actual=actual, returned=returned,
                     injected=name == 'ambiguous_observation')
            return returned
        except Exception as e:
            log.emit(name, 'group_observation_error', errno=getattr(e, 'errno', None), error=str(e))
            raise

    def killpg(pgid, sig):
        try:
            real_killpg(pgid, sig)
            log.emit(name, 'syscall.killpg.'+sig.name, pgid=pgid, ok=True)
        except OSError as e:
            log.emit(name, 'syscall.killpg.'+sig.name, pgid=pgid, ok=False,
                     errno=e.errno, error=str(e))
            raise

    def send(process, sig):
        signals.append(sig)
        try:
            real_signal(process, sig)
            log.emit(name, 'supervisor.killpg.'+sig.name, pid=process.pid, ok=True)
        except Exception as e:
            log.emit(name, 'supervisor.killpg.'+sig.name, pid=process.pid, ok=False,
                     errno=getattr(e, 'errno', None), error=str(e))
            raise

    try:
        control.remaining(global_deadline-10)
        with patch.object(d.subprocess, 'Popen', side_effect=retain), patch.object(d, 'group_members', side_effect=group), patch.object(d, 'signal_owned', side_effect=send), patch.object(d.os, 'killpg', side_effect=killpg):
            try:
                d.supervised([sys.executable, '-I', '-S', '-c', code], dict(os.environ), output,
                             min(global_deadline-10, time.monotonic()+allowance), owner_log=owner,
                             cleanup_seconds=3)
            except Exception as e:
                error = dict(type=type(e).__name__, errno=getattr(e, 'errno', None), message=str(e))
        record = json.loads(output.with_suffix('.process.json').read_text())
        result.update(supervisor=record, error=error, signals=[s.name for s in signals])
        validate_record(name, record, signals, error)
        result['validation_passed'] = True
    finally:
        # Validation fixtures have ONLY same-group children. Retained anchors
        # are the sole authority for final cleanup after expected rejection.
        # Never apply this fixture cleanup to an external native worker group.
        cleanup_records = []
        for process in anchors:
            item = dict(pid=process.pid, already_reaped=process.returncode is not None)
            if process.returncode is None:
                try:
                    members = real_group(process.pid, control.remaining(global_deadline))
                    d.checked_owned_group(members, process.pid)
                    item['before'] = members
                    if any(not r['state'].startswith('Z') for r in members):
                        for sig in (signal.SIGCONT, signal.SIGKILL):
                            real_signal(process, sig)
                            log.emit(name, 'harness.killpg.'+sig.name, pid=process.pid)
                    until = min(global_deadline, time.monotonic()+3)
                    while True:
                        members = real_group(process.pid, control.remaining(until))
                        d.checked_owned_group(members, process.pid)
                        item['before_reap'] = members
                        if all(r['state'].startswith('Z') for r in members):
                            item['exit_code'] = process.wait(timeout=control.remaining(until))
                            break
                        time.sleep(.02)
                except Exception as e:
                    item['error'] = str(e)
                    if process not in d.UNCONFIRMED:
                        d.UNCONFIRMED.append(process)
            if process.returncode is not None and process in d.UNCONFIRMED:
                d.UNCONFIRMED.remove(process)
            item['confirmed'] = process.returncode is not None
            cleanup_records.append(item)
        result['harness_cleanup'] = cleanup_records
        result['harness_cleanup_confirmed'] = bool(anchors) and all(r['confirmed'] for r in cleanup_records)
        log.emit(name, 'case_result', result=result)
        (case/'validation.json').write_text(json.dumps(result, indent=2)+'\n')
    require(result['harness_cleanup_confirmed'], 'validation fixture cleanup unconfirmed')
    return result


def main():
    started = time.monotonic()
    deadline = started+120
    directory = Path(os.environ['SUPERVISOR_VALIDATION_EVIDENCE'])
    control.prepare_directory(directory, Path.cwd())
    log = control.Log(directory/'events.jsonl')
    result = dict(kind='actual-supervisor-regression-not-boundary-proof', success=False, cases=[])
    signal.signal(signal.SIGALRM, control.alarm)
    signal.setitimer(signal.ITIMER_REAL, 110)
    try:
        actual = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True, timeout=3).strip()
        control.guard(os.environ, actual, platform.system(), platform.machine())
        subprocess.run(['git', 'diff', '--exit-code', 'HEAD'], check=True, timeout=3)
        result.update(source_sha=actual, run_id=os.environ.get('GITHUB_RUN_ID'), attempt=1,
                      machine=platform.machine(), system=platform.platform(),
                      python=dict(path=sys.executable, version=sys.version, sha256=d.sha(sys.executable)),
                      source_hashes={f:d.sha(ROOT/f) for f in ['context-boundary-diagnostic.py',
                                     'validate-macos-supervisor.py', 'macos-supervisor-control.py']})
        (directory/'invoked').open('x').close()
        for name in CASES:
            result['cases'].append(run_case(name, directory, log, deadline))
        result['success'] = True
    except Exception as e:
        result['error'] = dict(type=type(e).__name__, errno=getattr(e, 'errno', None), message=str(e))
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        result['seconds'] = time.monotonic()-started
        result['success'] = result['success'] and result['seconds'] <= 120
        (directory/'validation.json').write_text(json.dumps(result, indent=2)+'\n')
        log.close()
        manifest=[dict(path=str(p.relative_to(directory)),bytes=p.stat().st_size,sha256=d.sha(p))
                  for p in sorted(directory.rglob('*')) if p.is_file()]
        (directory/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    return 0 if result['success'] else 1


if __name__ == '__main__':
    sys.exit(main())
