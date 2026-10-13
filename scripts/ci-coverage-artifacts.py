#!/usr/bin/env python3
"""LLVM archive build identity and isolated, compile-free coverage reporting."""
import json
import hashlib
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tarfile
import time


def output(argv, source, env=None):
    return subprocess.check_output(argv, cwd=source, env=env, text=True).strip()


def capture(source, *, remap=True):
    """Observe llvm-cov's actual flags instead of approximating its wrapper.

    Logical paths let another checkout reproduce the same recipe. Profiles are
    runtime outputs, never archive inputs. Remapping also keeps report exclusions
    independent of the original checkout's absolute path.
    """
    source = Path(source).resolve()
    target = source.parent / '.intent-coverage-identity-target'
    env = dict(os.environ, CARGO_TARGET_DIR=str(target))
    text = output(['cargo', 'llvm-cov', 'show-env', '--sh', *(['--remap-path-prefix'] if remap else [])], source, env)
    values = {}
    for line in text.splitlines():
        words = shlex.split(line)
        if len(words) != 2 or words[0] != 'export' or '=' not in words[1]:
            raise ValueError('unsupported llvm-cov show-env output')
        name, value = words[1].split('=', 1)
        values[name] = value.replace(str(target), '{target}').replace(str(source), '{workspace}')
    if not values.get('CARGO_LLVM_COV') or not values.get('LLVM_PROFILE_FILE'):
        raise ValueError('llvm-cov did not provide instrumentation environment')
    return {'environment': values, 'tools': tool_versions(source)}


def tool_versions(source, env=None):
    rust = output(['rustc', '-vV'], source, env)
    host = next(line[6:] for line in rust.splitlines() if line.startswith('host: '))
    sysroot = Path(output(['rustc', '--print', 'sysroot'], source, env))
    llvm = sysroot / 'lib/rustlib' / host / 'bin'
    tools = {'cargo-llvm-cov': output([shutil.which('cargo-llvm-cov'), 'llvm-cov', '--version'], source, env)}
    for name in ('llvm-cov', 'llvm-profdata'):
        path = Path(os.environ.get(name.upper().replace('-', '_'), llvm / name))
        tools[name] = output([str(path), '--version'], source, env)
    return tools


def flags(coverage):
    return [name + '=' + value for name, value in sorted(coverage['environment'].items())]


def target_directory(base, captured):
    # llvm-cov's rustc wrapper injects flags that Cargo does not fingerprint.
    # Keep incompatible wrapper flags/tools apart while sharing one target for
    # every feature graph with identical instrumentation. No source SHA here.
    digest = hashlib.sha256(json.dumps(captured, sort_keys=True).encode()).hexdigest()
    return Path(base) / 'llvm-cov-target' / digest


def build_environment(coverage, source, target, *, remap=True):
    if capture(source) != coverage:
        raise ValueError('observed LLVM tools/flags differ from archive identity')
    # trybuild must render original source spans for its committed diagnostics.
    # Its fresh child compiler uses llvm-cov's original, unremapped flags, while
    # the archived test binary keeps the exact requested feature graph.
    values = coverage['environment'] if remap else capture(source, remap=False)['environment']
    return dict(os.environ, **{name: value.replace('{workspace}', str(source)).replace('{target}', str(target))
                               for name, value in values.items()})


def stage_report_objects(stage, target, extraction):
    """Keep workspace procedural macros in the original LLVM report scope.

    nextest needs no procedural-macro library at runtime and omits it. LLVM
    reports include these workspace objects, so snapshot the exact Cargo
    outputs before another feature graph can replace them. Never copy profiles.
    """
    metadata = json.loads((extraction / 'target/nextest/cargo-metadata.json').read_text())
    members = set(metadata['workspace_members'])
    objects = set()
    for line in (stage / 'build.jsonl').read_text().splitlines():
        try:
            artifact = json.loads(line)
        except json.JSONDecodeError:
            continue
        if artifact.get('reason') == 'compiler-artifact' and artifact['package_id'] in members and 'proc-macro' in artifact['target']['kind']:
            objects.update(Path(path) for path in artifact['filenames'] if Path(path).suffix == '.so')
    with tarfile.open(stage / 'report-objects.tar.gz', 'w:gz') as tar:
        for path in sorted(objects):
            relative = path.resolve().relative_to(target.resolve())
            destination = extraction / 'target' / relative
            if destination.exists():
                continue  # Already part of nextest's authenticated inventory.
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
            tar.add(destination, arcname='target/' + relative.as_posix(), recursive=False)


def report_environment(root):
    """Permit metadata/version probes only; all compilation entrypoints fail.

    llvm-cov report needs Cargo metadata and rustc's sysroot/version, but never
    builds. Resolve real tools before installing guards, including CARGO/RUSTC
    overrides, so a report cannot accidentally invoke a build through an alias.
    """
    root = Path(root).resolve()
    guards = root / 'report-guards'
    guards.mkdir(exist_ok=True)
    real = {name: shutil.which(name) for name in ('cargo', 'rustc')}
    if not all(real.values()):
        raise ValueError('report requires installed Cargo and rustc metadata tools')
    code = '''#!{python}
import json, os, sys
from pathlib import Path
name = Path(sys.argv[0]).name
args = sys.argv[1:]
real = {real!r}
while name == 'rustc' and args and args[0] in (real['rustc'], str(Path(sys.argv[0]).resolve())):
    args = args[1:]
allowed = (name == 'cargo' and (args in (['--version'], ['-vV']) or (args and args[0] in ('metadata', 'locate-project')))) or (name == 'rustc' and (args in (['-C', 'help'], ['-vV'], ['--version'], ['--print', 'sysroot'], ['--print', 'target-libdir']) or (len(args) == 4 and args[:3] == ['--print', 'cfg', '--target'] and args[3].replace('-', '').replace('_', '').isalnum())))
if allowed:
    with open({probes!r}, 'a') as stream: stream.write(json.dumps([name, *args]) + '\\n')
    os.execv(real[name], [real[name], *args])
with open({calls!r}, 'a') as stream: stream.write(json.dumps([name, *args]) + '\\n')
sys.exit('coverage consumer attempted compilation: ' + name)
'''.format(python=sys.executable, real=real, probes=str(root / 'metadata-probes.jsonl'), calls=str(root / 'compiler-calls.log'))
    for name in ('cargo', 'rustc', 'rustup', 'cc', 'gcc', 'clang', 'ld'):
        path = guards / name
        path.write_text(code)
        path.chmod(0o755)
    env = dict(os.environ, PATH=str(guards) + os.pathsep + os.environ['PATH'],
               CARGO=str(guards / 'cargo'), RUSTC=str(guards / 'rustc'),
               RUSTC_WRAPPER=str(guards / 'rustc'), RUSTC_WORKSPACE_WRAPPER=str(guards / 'rustc'))
    return env


def report(bundle, prepared, request, floor, *, lcov=True):
    if request['identity']['build']['instrumentation']['mode'] != 'llvm-cov':
        raise ValueError('cannot report coverage from ordinary binaries')
    source = Path(prepared['source'])
    root = Path(prepared['target']).parent
    env = report_environment(root)
    if tool_versions(source, env) != request['recipe']['coverage']['tools']:
        raise ValueError('report LLVM tools differ from instrumented archive')
    env['CARGO_LLVM_COV_TARGET_DIR'] = str(root)
    command = [shutil.which('cargo-llvm-cov'), 'llvm-cov', 'report', '--remap-path-prefix',
               '--nextest-archive-file', str(Path(bundle) / 'tests.tar.zst')]
    started = time.monotonic()
    if lcov:
        subprocess.run([*command, '--lcov', '--output-path', str(source / 'lcov.info')], cwd=source, env=env, check=True)
    result = subprocess.run([*command, '--summary-only', '--fail-under-lines', str(floor)],
                            cwd=source, env=env, text=True, stdout=subprocess.PIPE)
    (source / 'coverage-summary.txt').write_text(result.stdout)
    print(result.stdout, end='', flush=True)
    if result.returncode:
        print(f'ERROR: coverage report failed (required line coverage {floor}%)', file=sys.stderr)
    return {'exit_code': result.returncode, 'report_seconds': time.monotonic() - started}
