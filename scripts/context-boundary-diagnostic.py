#!/usr/bin/env python3
"""Temporary one-shot macOS experiment. Not a canonical test receipt producer."""
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import selectors
import signal
import shutil
import struct
import subprocess
import sys
import time

TARGET = ('repository_read_policy::tests::context_consumer::'
          'context_consumer_ledger_counts_distinct_calls_at_sixty_four_and_sixty_five')
CARGO = ['cargo', 'test', '--locked', '-p', 'intent-services', '--lib']
BUILD_SECONDS, RUN_SECONDS, CLEANUP_SECONDS = 2100, 120, 30
UNCONFIRMED = []  # Retain unreaped anchors when native ownership cannot be proved.


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def rows(path):
    if not path.exists():
        return []
    data = path.read_text()
    if data and not data.endswith('\n'):
        raise RuntimeError('partial telemetry record; cleanup/measurement unconfirmed')
    return [json.loads(line) for line in data.splitlines()]


def owner_cleanup(path):
    """Trust only paired native-owner acknowledgments; never signal its group ourselves."""
    events = rows(path)
    starts = [r for r in events if r['event'] == 'owner_start']
    ends = [r for r in events if r['event'] == 'owner_cleanup']
    keys = [(r['pid'], r['root']) for r in starts]
    if not starts or len(keys) != len(set(keys)) or len(starts) != len(ends):
        return False
    return all(sum(e['pid'] == s['pid'] and e['root'] == s['root']
                   and e['cleaned'] is True and e['scratch_exists'] is False
                   and not Path(e['root']).exists() for e in ends) == 1 for s in starts)


def group_members(pgid, timeout=2):
    # No command arguments or environment dumps. The direct child is unreaped,
    # reserving pgid until we have stopped and observed every group member.
    text = subprocess.check_output(['ps', '-axo', 'pid=,ppid=,pgid=,stat=,comm='],
                                   text=True, timeout=timeout)
    result = []
    for line in text.splitlines():
        fields = line.split(None, 4)
        if len(fields) < 4:
            raise RuntimeError('partial process-group observation')
        if int(fields[2]) == pgid:
            result.append(dict(pid=int(fields[0]), ppid=int(fields[1]), pgid=pgid,
                               state=fields[3], executable=fields[4] if len(fields) == 5 else ''))
    return result


def signal_owned(process, sig):
    # No poll()/wait() before group cleanup: Popen is our live or zombie anchor.
    try:
        os.killpg(process.pid, sig)
    except ProcessLookupError:
        pass


def checked_owned_group(members, pgid):
    identities = [r['pid'] for r in members]
    if (pgid not in identities or len(identities) != len(set(identities))
            or any(r['pgid'] != pgid or not r['state']
                   or r['state'][0] not in 'RSDIUTZW' for r in members)):
        raise RuntimeError('owned group observation missing anchor or ambiguous')
    return members


def quiescent_owned_group(pgid, deadline, observations):
    # Retain the unreaped anchor. EOF or a zombie leader alone says nothing
    # about its other group members. Only two stable FULL all-zombie snapshots
    # allow skipping signals (Darwin can return EPERM for such a dead group).
    previous = None
    for _ in range(2):
        if time.monotonic() >= deadline:
            raise RuntimeError('cleanup budget exhausted before group observation')
        members = group_members(pgid, min(2, max(.001, deadline-time.monotonic())))
        observations.append(members)
        checked_owned_group(members, pgid)
        if not all(r['state'].startswith('Z') for r in members):
            return False
        identities = sorted((r['pid'], r['ppid'], r['pgid']) for r in members)
        if previous is not None:
            return previous == identities
        previous = identities
        time.sleep(min(.02, max(0, deadline-time.monotonic())))
    return False


def wait_frozen(pgid, deadline):
    # A delivered stop signal is not an acknowledgment. Require two stable
    # snapshots of stopped/zombie members before reading owner registrations.
    previous = None
    while time.monotonic() < deadline:
        members = group_members(pgid, min(2, max(.001, deadline-time.monotonic())))
        checked_owned_group(members, pgid)
        identities = {(r['pid'], r['ppid'], r['pgid']) for r in members}
        stopped = all(r['state'].startswith(('T', 'Z')) for r in members)
        if stopped and identities == previous:
            return members
        previous = identities if stopped else None
        time.sleep(min(.02, max(0, deadline-time.monotonic())))
    raise RuntimeError('owned group stop unconfirmed within cleanup budget')


def supervised(command, env, output, deadline, owner_log=None, cancel=None, cleanup_seconds=CLEANUP_SECONDS):
    if time.monotonic() >= deadline:
        raise RuntimeError('phase budget exhausted before launch')
    record = dict(command=command, started_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                  timeout=False, cleaned=False)
    phase_started=time.monotonic()
    cleanup_started=None
    process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, start_new_session=True)
    record['pid'] = process.pid
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    eof = False
    frozen = False
    with output.open('wb') as log:
        def drain(until):
            nonlocal eof
            while not eof and time.monotonic() < until:
                for key, _ in selector.select(min(.1, max(0, until - time.monotonic()))):
                    block = os.read(key.fileobj.fileno(), 65536)
                    if block:
                        log.write(block)
                        log.flush()
                    else:
                        eof = True
        try:
            drain(deadline)
            record['timeout'] = not eof
            cleanup_started = time.monotonic()
            cleanup_deadline = cleanup_started + cleanup_seconds
            if not eof and cancel is not None:
                cancel.write_text('cancel\n')
                # Existing owner remains alive to stop its independently grouped worker.
                drain(cleanup_deadline - 3)
            if eof:
                # EOF must not turn an otherwise successful process into SIGKILL.
                # Observe leader exit without poll/wait, retaining its PID lease.
                while time.monotonic() < cleanup_deadline - 1:
                    live_leader = any(r['pid'] == process.pid and not r['state'].startswith('Z')
                                      for r in group_members(process.pid, min(2, max(.001, cleanup_deadline-time.monotonic()))))
                    if not live_leader:
                        break
                    time.sleep(.02)
                else:
                    record['unexpected_live_leader_after_eof'] = True
            # Freeze our owned group before inspecting registration; a parent
            # cannot race that inspection by launching a new worker afterward.
            record['quiescent_group_observations'] = []
            quiescent = quiescent_owned_group(process.pid, cleanup_deadline,
                                              record['quiescent_group_observations'])
            record['cleanup_path'] = 'already_quiescent' if quiescent else 'freeze_owned_group'
            if not quiescent:
                frozen = True
                signal_owned(process, signal.SIGSTOP)
                record['frozen_group_members'] = wait_frozen(process.pid, cleanup_deadline)
            if owner_log is not None and not owner_cleanup(owner_log):
                raise RuntimeError('native owner cleanup unconfirmed; refused unsafe external worker-group signal')
            if not quiescent:
                signal_owned(process, signal.SIGKILL)
                frozen = False  # Owner cleanup is proven; never resume after reap.
            while time.monotonic() < cleanup_deadline:
                members = group_members(process.pid, min(2, max(.001, cleanup_deadline-time.monotonic())))
                record['last_group_members'] = members
                checked_owned_group(members, process.pid)
                if not any(not r['state'].startswith('Z') for r in members):
                    record['exit_code'] = process.wait(timeout=max(.01, cleanup_deadline-time.monotonic()))
                    record['cleaned'] = True
                    break
                # Bounded observation, not a retry of the experiment.
                time.sleep(.02)
            if not record['cleaned']:
                raise RuntimeError('owned Cargo group did not stop within cleanup allowance')
            record['cleanup_seconds'] = time.monotonic()-cleanup_started
            record['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            return record
        except Exception as error:
            record['error'] = str(error)
            if not record['cleaned']:
                UNCONFIRMED.append(process)
            raise
        finally:
            if frozen and not record['cleaned']:
                # Parsing, reads, stop-observation and signaling can all fail.
                # Always release a frozen owner so its native cleanup can run.
                try:
                    signal_owned(process, signal.SIGCONT)
                    record['resume_sent'] = True
                except OSError as error:
                    record['resume_error'] = str(error)
            selector.close()
            process.stdout.close()
            record['total_seconds']=time.monotonic()-phase_started
            if cleanup_started is not None: record['cleanup_seconds']=time.monotonic()-cleanup_started
            save(output.with_suffix('.process.json'), record)


def check_guard(env, actual_sha, system, machine):
    if system != 'Darwin' or machine != 'arm64':
        raise RuntimeError('diagnostic requires native macOS ARM64')
    if env.get('CARGO_BUILD_TARGET') not in (None, 'aarch64-apple-darwin'):
        raise RuntimeError('non-native CARGO_BUILD_TARGET prohibited; environment not rewritten')
    if env.get('GITHUB_RUN_ATTEMPT') != '1':
        raise RuntimeError('reruns prohibited')
    expected = env.get('EXPECTED_SOURCE_SHA', '')
    if not re.fullmatch('[0-9a-f]{40}', expected) or expected != actual_sha:
        raise RuntimeError('reviewed source SHA mismatch')
    if env.get('GITHUB_SHA') != expected:
        raise RuntimeError('dispatch ref does not match reviewed source')


def macho_identity(executable):
    # Rust's native output is a thin Mach-O executable. Reject fat/universal or
    # cross-target images instead of assuming the compiler host proves the target.
    with Path(executable).open('rb') as binary:
        header = binary.read(32)
    if len(header) != 32:
        raise RuntimeError('truncated Mach-O executable header')
    magic, cpu, subtype, kind, commands, size, flags, reserved = struct.unpack('<8I', header)
    if (magic, cpu, kind) != (0xfeedfacf, 0x0100000c, 2):
        raise RuntimeError('requires thin ARM64 Mach-O executable')
    return dict(format='Mach-O 64-bit little-endian', architecture='arm64',
                cpu_type=cpu, cpu_subtype=subtype, file_type=kind,
                load_commands=commands, load_commands_bytes=size, flags=flags)


def compiler_artifact(text):
    artifacts = []
    for line in text.splitlines():
        if not line.startswith('{'):
            continue
        r = json.loads(line)
        if (r.get('reason') == 'compiler-artifact' and r.get('target', {}).get('name') == 'intent_services'
                and r.get('profile', {}).get('test') and r.get('executable')):
            artifacts.append(r)
    if len(artifacts) != 1:
        raise RuntimeError('expected one full-services test compiler artifact')
    return artifacts[0]


def require_executed_binary(output, executable):
    paths = re.findall(r'^\s*Running unittests .* \(([^)]+)\)$', output, re.M)
    if len(paths) != 1 or Path(paths[0]).resolve() != executable.resolve():
        raise RuntimeError('Cargo executed path does not match compiler artifact')


def validate_trace(events, executable, expected_hash):
    workers = [r for r in events if r['event'] == 'worker_start']
    if len(workers) != 1:
        raise RuntimeError('expected exactly one isolated worker')
    fields = workers[0]['fields']
    if (fields.get('arch'), fields.get('os')) != ('aarch64', 'macos'):
        raise RuntimeError('worker compiled platform is not native macOS ARM64')
    worker_exe = Path(fields['current_exe']).resolve()
    if worker_exe != executable.resolve() or sha(worker_exe) != expected_hash:
        raise RuntimeError('worker/compiler executable identity mismatch')
    if fields['settings']['INTENTD_ASSERT_BOUND_CALLER'] != '1' or fields['settings']['INTENTD_TEST_TIMEOUT_MULTIPLIER'] != '1':
        raise RuntimeError('worker settings mismatch')
    git = Path(fields['git_path'])
    return dict(worker_pid=workers[0]['pid'], current_exe=str(worker_exe), binary_sha256=expected_hash,
                git_realpath=str(git), git_sha256=sha(git),
                worker_pgid=fields['pgid'], worker_group_after=group_members(fields['pgid']),
                git_version=subprocess.check_output([str(git), '--version'], text=True, timeout=5).strip())


def require_completed_trace(events):
    for event in ['arm_start', 'assertions_passed', 'arm_drained']:
        if [r['arm'] for r in events if r['event'] == event] != [63, 64]:
            raise RuntimeError('both original arms not proven: ' + event)
    for event in ['worker_completed', 'heartbeat_drop', 'logger_summary']:
        if sum(r['event'] == event for r in events) != 1:
            raise RuntimeError('missing terminal evidence: ' + event)
    # Records are from real admission slices; do not substitute host ordinals.
    for arm in [63, 64]:
        hosts = [r for r in events if r['arm'] == arm and r['event'] == 'span_end' and r['fields']['stage'] == 'host']
        required = [r['fields']['records'] for r in events if r['arm'] == arm and r['event'] == 'span_start' and r['fields']['stage'] == 'required_admit']
        optional = [r['fields']['records'] for r in events if r['arm'] == arm and r['event'] == 'span_start' and r['fields']['stage'] == 'optional_admit']
        if len(hosts) != arm or required != [2] + [1]*62 or optional != ([64] if arm == 63 else []):
            raise RuntimeError('real boundary counts differ')


def main():
    directory = Path(os.environ['CONTEXT_BOUNDARY_EVIDENCE'])
    if not directory.is_absolute():
        raise RuntimeError('evidence directory must be absolute')
    directory.mkdir(parents=True, exist_ok=False)  # exclusive attempt; never recycle
    receipt = dict(kind='supplemental-diagnostic-not-canonical-receipt', success=False, invocations=0)
    try:
        env = dict(os.environ)
        actual = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True, timeout=5).strip()
        check_guard(env, actual, platform.system(), platform.machine())
        subprocess.run(['git', 'diff', '--exit-code', 'HEAD'], check=True, timeout=5)
        receipt.update(source_sha=actual, run_id=env.get('GITHUB_RUN_ID'),
                       source_tree=subprocess.check_output(['git','rev-parse','HEAD^{tree}'],text=True,timeout=5).strip(),
                       pressure_start=dict(load=os.getloadavg(), cpus=os.cpu_count()),
                       source_hashes={str(p):sha(p) for p in [Path('Cargo.lock'),Path('Cargo.toml'),Path('rust-toolchain.toml'),Path('.cargo/config.toml')]})
        env.update(CARGO_TERM_PROGRESS_WHEN='never', CARGO_TERM_COLOR='never')
        for name, value in dict(INTENTD_ASSERT_BOUND_CALLER='1', INTENTD_TEST_TIMEOUT_MULTIPLIER='1',
                                CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0', CARGO_INCREMENTAL='0').items():
            if env.get(name) != value:
                raise RuntimeError('native environment mismatch: '+name)
        git_path=Path(shutil.which('git')).resolve()
        receipt['git_before']=dict(path=str(git_path),sha256=sha(git_path),version=subprocess.check_output([str(git_path),'--version'],text=True,timeout=5).strip())
        receipt['settings'] = {k:env.get(k) for k in ['INTENTD_ASSERT_BOUND_CALLER','INTENTD_TEST_TIMEOUT_MULTIPLIER','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','CARGO_INCREMENTAL']}
        receipt['api_timeout_metadata'] = {'present': 'INTENTD_WORKSPACE_API_TIMEOUT_MS' in env}
        receipt['sanitized_environment']={k:dict(bytes=len(v.encode()),sha256=hashlib.sha256(v.encode()).hexdigest())
            for k,v in env.items() if k.startswith('GIT_CONFIG') or k in ['HOME','PATH','RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','CARGO_BUILD_TARGET','INTENTD_WORKSPACE_API_TIMEOUT_MS','LLVM_PROFILE_FILE']}

        toolchain=subprocess.check_output(['rustc','-vV'],text=True,timeout=5)
        (directory/'rustc.log').write_text(toolchain)
        if 'host: aarch64-apple-darwin' not in toolchain:
            raise RuntimeError('compiler is not native macOS ARM64')
        build_deadline=time.monotonic()+BUILD_SECONDS
        build=supervised(CARGO+['--no-run','--message-format=json-render-diagnostics'],env,directory/'build.log',build_deadline)
        receipt['build']=build
        if build['timeout'] or build['exit_code'] != 0:
            raise RuntimeError('prebuild failed or exceeded budget; zero test invocations')
        artifact=compiler_artifact((directory/'build.log').read_text())
        save(directory/'compiler-artifact.json',artifact)
        executable=Path(artifact['executable'])
        receipt['executable_macho']=macho_identity(executable)
        before=sha(executable)
        receipt.update(executable=str(executable),binary_sha256_before=before)
        listing=supervised(CARGO+[TARGET,'--','--exact','--list','--format=terse'],env,directory/'listing.log',build_deadline)
        receipt['listing']=listing
        require_executed_binary((directory/'listing.log').read_text(),executable)
        names=re.findall(r'^(\S+): test$',(directory/'listing.log').read_text(),re.M)
        if listing['timeout'] or listing['exit_code'] != 0 or names != [TARGET] or sha(executable) != before:
            raise RuntimeError('exact discovery/hash failed; zero test invocations')
        owner_log=directory/'owners.jsonl'; cancel=directory/'cancel'; trace=directory/'timing.jsonl'
        env.update(CONTEXT_BOUNDARY_DIAGNOSTIC_LOG=str(trace),CONTEXT_BOUNDARY_OWNER_LOG=str(owner_log),CONTEXT_BOUNDARY_CANCEL=str(cancel))
        # Unique launch marker and exactly one run call site. No retry or fallback.
        (directory/'invoked').open('x').close()
        receipt['invocations']=1
        run=supervised(CARGO+[TARGET,'--','--exact','--test-threads=1','--format=pretty','--color=never','--nocapture'],
                       env,directory/'run.log',time.monotonic()+RUN_SECONDS,owner_log,cancel)
        receipt['run']=run
        require_executed_binary((directory/'run.log').read_text(),executable)
        receipt['binary_sha256_after']=sha(executable)
        events=rows(trace)
        receipt['worker_identity']=validate_trace(events,executable,before)
        receipt['owner_events']=rows(owner_log)
        identity=receipt['worker_identity']
        if identity['git_realpath'] != str(git_path) or identity['git_sha256'] != receipt['git_before']['sha256']:
            raise RuntimeError('worker Git identity differs from prebuild identity')
        if any(not r['state'].startswith('Z') for r in identity['worker_group_after']):
            raise RuntimeError('live process in former worker group; cleanup unconfirmed')
        if len([r for r in receipt['owner_events'] if r['event']=='owner_start']) != 1 or not owner_cleanup(owner_log):
            raise RuntimeError('exactly one cleaned owner not proven')
        if receipt['binary_sha256_after'] != before:
            raise RuntimeError('executable changed during execution')
        # Record identity even for a genuine timeout; never convert failure to success.
        if run['timeout'] or run['exit_code'] != 0 or run.get('unexpected_live_leader_after_eof'):
            raise RuntimeError('single invocation failed; inspect partial timeline, no repeat')
        require_completed_trace(events)
        output=(directory/'run.log').read_text()
        if not re.search(r'test result: ok\. 1 passed; 0 failed; 0 ignored;',output):
            raise RuntimeError('public parent did not prove exact test success')
        receipt['success']=True
    except Exception as error:
        receipt['error']=str(error)
    finally:
        receipt['pressure_end']=dict(load=os.getloadavg(),cpus=os.cpu_count())
        save(directory/'diagnostic.json',receipt)
        save(directory/'manifest.json',[dict(path=str(p.relative_to(directory)),bytes=p.stat().st_size,sha256=sha(p))
             for p in sorted(directory.rglob('*')) if p.is_file() and p.name!='manifest.json'])
    return 0 if receipt['success'] else 1


if __name__ == '__main__':
    sys.exit(main())
