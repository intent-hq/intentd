#!/usr/bin/env python3
"""One native producer step, followed by compile-free archive consumers.

Publication is local to this run; uploading its directory preserves successful
variants even if subsequent lint/runtime work fails. Remote reuse/locking is a
separate layer. Native CI changes opt PRs into the full native proof set.
"""
import argparse
import fnmatch
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import time

_spec = importlib.util.spec_from_file_location('archives', Path(__file__).with_name('ci-test-artifacts.py'))
archives = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(archives)

NATIVE_INPUTS = {'.github/workflows/ci.yml', 'scripts/ci-native-artifacts.py',
                 'scripts/ci-test-artifacts.py', 'scripts/ci-build-identity.py',
                 'scripts/test-daemon-lifecycle.py', 'scripts/test-ci-native-consumers.py',
                 'scripts/test-ci-native-routing.py', 'scripts/test-ci-test-policy.py',
                 'scripts/test-ci-test-artifacts.py', 'scripts/test-ci-build-identity.py',
                 'scripts/test-ci-native-archives.py', 'scripts/test-daemon-lifecycle-proof.py',
                 'scripts/macos-ci-cleanup.py', 'scripts/ci-target-slot.sh'}
NATIVE_PATTERNS = ('.github/actions/*', '.github/workflows/*native*', '.cargo/*', '.config/*',
                   'rust-toolchain.toml', 'Cargo.toml', 'Cargo.lock', 'crates/*/Cargo.toml',
                   'crates/intentd-test-support/*', 'crates/intentd/tests/common/*',
                   'crates/intentd/tests/e2e_detached_lifecycle.rs', 'crates/intentd/tests/e2e_wss_tunnel.rs',
                   'crates/intentd/tests/e2e_wss_setup_headless.rs', 'crates/intentd/tests/e2e_doctor_codex.rs',
                   'crates/intentd-sitter/*', 'crates/intent-services/src/*codex*',
                   'crates/intent-services/src/tests.rs', 'crates/intent-services/src/test_support/*',
                   'crates/intent-services/src/test_support.rs', 'crates/intent-services/src/capability*',
                   'crates/intent-transport/src/tunnel*')


def native_scope(event, changed):
    if event not in ('pull_request', 'merge_group'):
        raise ValueError('native archives only support PR and merge-group validation')
    full = changed is None or any(path in NATIVE_INPUTS or any(fnmatch.fnmatchcase(path, pattern)
                 for pattern in NATIVE_PATTERNS) for path in changed)
    return 'merge_group' if event == 'merge_group' or full else 'pull_request'


def config_digest(source):
    """Hash Cargo's complete ancestor/home search, including legacy config.

    Config contents may contain credentials: only their hashes are published.
    CLI overrides are already in each recipe; environment is separately bound.
    """
    source = Path(source).resolve()
    directories = [source, *source.parents]
    paths = [directory / '.cargo' / name for directory in directories for name in ('config', 'config.toml')]
    home = Path(os.environ.get('CARGO_HOME', Path.home() / '.cargo'))
    paths += [home / name for name in ('config', 'config.toml')]
    inputs = [(str(path), archives.file_sha(path)) for path in paths if path.is_file()]
    return hashlib.sha256(json.dumps(inputs).encode()).hexdigest()


def probe(command, *, codes=(0,)):
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode not in codes or not result.stdout.strip():
        raise RuntimeError(f'cannot fingerprint native prerequisite {command}: {result.stdout}')
    return result.stdout.strip()


def msvc_tool(name):
    compiler = shutil.which('cl.exe')
    if not compiler:
        raise RuntimeError('MSVC developer environment must be provisioned')
    tool = Path(compiler).with_name(name + '.exe')
    if not tool.is_file():
        raise RuntimeError('missing provisioned MSVC tool: ' + str(tool))
    return str(tool)


def msvc_help(name):
    # MSVC help can return nonzero even when it successfully prints its version.
    # Validate the tool-specific banner and usage instead of treating help as a
    # build. Actual compilation and dependency inspection still require exit 0.
    banners = {'link': 'Incremental Linker', 'dumpbin': 'COFF/PE Dumper'}
    command = [msvc_tool(name), '/?']
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = result.stdout.strip()
    banner = r'^Microsoft \(R\) ' + re.escape(banners[name]) + r' Version [0-9]+(?:\.[0-9]+)+'
    usage = r'(?im)^\s*usage:\s*' + name + r'\s'
    if not re.search(banner, output) or not re.search(usage, output) or re.search(r'(?i)\berror\b', output):
        raise RuntimeError(f'cannot fingerprint MSVC prerequisite {command} (exit {result.returncode}): {output}')
    return output


def platform_facts(native_platform):
    rust = probe(['rustc', '-vV'])
    host = next(line.removeprefix('host: ') for line in rust.splitlines() if line.startswith('host: '))
    observed = {'system': platform.platform(), 'machine': platform.machine(),
                'image_os': os.environ.get('ImageOS', 'self-hosted'),
                'image_version': os.environ.get('ImageVersion', 'self-hosted')}
    if native_platform == 'macos':
        cc = probe(['xcrun', 'clang', '--version'])
        linker = probe(['xcrun', 'ld', '-v'], codes=(0, 1))
        sdk = probe(['xcrun', '--show-sdk-version'])
        sdk_path = Path(probe(['xcrun', '--show-sdk-path']))
        observed.update(os=probe(['sw_vers']), xcode=probe(['xcodebuild', '-version']),
                        sdk_settings=archives.file_sha(sdk_path / 'SDKSettings.json'))
        abi = observed['os'] + '\n' + cc
        probe(['otool', '-h', sys.executable])
    elif native_platform == 'windows':
        cc = probe([msvc_tool('cl')], codes=(0, 2))
        linker = msvc_help('link')
        os.environ['DUMPBIN'] = msvc_tool('dumpbin')
        msvc_help('dumpbin')
        os.environ['CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER'] = msvc_tool('link')
        required = ('VCToolsVersion', 'WindowsSDKVersion', 'UniversalCRTSdkDir', 'UCRTVersion')
        if any(not os.environ.get(name) for name in required):
            raise RuntimeError('MSVC developer environment and Windows SDK must be provisioned')
        observed.update({name: os.environ[name] for name in required})
        sdk = json.dumps({name: observed[name] for name in required}, sort_keys=True)
        abi = platform.version() + '\n' + cc + '\n' + sdk
    else:
        cc = probe(['cc', '--version'])
        linker = probe(['ld', '--version'])
        probe(['readelf', '--version'])
        observed.update(os=Path('/etc/os-release').read_text(), libc=probe(['ldd', '--version']),
                        openssl=probe(['pkg-config', '--modversion', 'openssl']),
                        zlib=probe(['pkg-config', '--modversion', 'zlib']))
        sdk = 'none'
        abi = observed['libc'] + '\nOpenSSL=' + observed['openssl'] + '\nzlib=' + observed['zlib']
    return {'platform': {'host': host, 'target': host, 'os': native_platform, 'arch': platform.machine(),
                         'abi': abi, 'sdk': sdk, 'image': json.dumps(observed, sort_keys=True)},
            'native_tools': {'cc': cc, 'linker': linker}}


def produce(source, root, native_platform, event):
    source, root = Path(source).resolve(), Path(root).resolve()
    if native_platform == 'macos':
        # Preserve the bounded native test-build parallelism across all variants.
        os.environ.setdefault('CARGO_BUILD_JOBS', '2')
    root.mkdir(parents=True, exist_ok=True)
    target = Path(os.environ.get('CARGO_TARGET_DIR', root / 'cargo-target')).resolve()
    facts = platform_facts(native_platform)
    if native_platform == 'windows':
        build_windows_tripwire(root)
    recipes = archives.native_recipes(native_platform, event, facts['platform']['target'])
    requests = {name: archives.capture_request(source, facts['platform'], recipe,
                config_sha256=config_digest(source), native_tools=facts['native_tools'])
                for name, recipe in recipes.items()}
    (root / 'requests.json').write_text(json.dumps(requests, indent=2) + '\n')
    # Every variant is staged before the following build can replace binaries.
    # Publish before linting too: a lint failure must not discard good archives.
    archives.produce(list(requests.values()), source, target, root / 'published')
    for command in check_commands(native_platform, event, facts['platform']['target'], target):
        print('+ ' + repr(command), flush=True)
        subprocess.run(command, cwd=source, check=True)


def check_commands(native_platform, event, triple, target):
    common = ['--locked', '--target', triple, '--target-dir', str(target)]
    checks = []
    if event == 'merge_group' and native_platform != 'linux':
        # Dev dependencies enable fixture features. Never add --all-targets to
        # this production feature-off check or substitute linked test binaries.
        checks.append(['cargo', 'check', '--workspace', *common])
        checks.append(['cargo', 'clippy', '--workspace', '--all-targets', *common, '--', '-D', 'warnings'])
    else:
        checks.append(['cargo', 'clippy', '-p', 'intentd-sitter', '-p', 'intentd', '--all-targets', *common, '--', '-D', 'warnings'])
    return checks


def build_windows_tripwire(root):
    """Create real .exe guards in the producer, never in a consumer.

    CreateProcess can bypass .cmd PATH entries. The tiny native tool must exist
    before archive extraction/runtime starts; it uses only the platform CRT.
    """
    source = root / 'compiler-tripwire.c'
    source.write_text(r'''#include <stdio.h>
#include <stdlib.h>
int main(int argc, char **argv) {
    (void)argc;
    const char *path = getenv("INTENTD_COMPILER_CALL_LOG");
    FILE *log = path ? fopen(path, "a") : NULL;
    if (log) { fprintf(log, "%s\n", argv[0]); fclose(log); }
    fputs("consumer attempted compilation\n", stderr);
    return 97;
}
''')
    command = [msvc_tool('cl'), '/nologo', '/MT', str(source), '/Fe:' + str(root / 'compiler-tripwire.exe'),
               '/Fo:' + str(root / 'compiler-tripwire.obj')]
    print('+ ' + repr(command), flush=True)
    subprocess.run(command, cwd=root, check=True)


def consumer_environment(root):
    """Tripwires are runtime-only; keep Git, Node and shells available."""
    root = Path(root).resolve()
    tripwires = root / 'no-compiler'
    tripwires.mkdir(parents=True, exist_ok=True)
    log = root / 'compiler-calls.log'
    for name in ('cargo', 'rustc', 'rustup'):
        if sys.platform == 'win32':
            executable = root / 'compiler-tripwire.exe'
            if not executable.is_file():
                raise RuntimeError('producer must provision Windows compiler tripwire')
            shutil.copy2(executable, tripwires / (name + '.exe'))
        else:
            path = tripwires / name
            path.write_text('#!/bin/sh\necho ' + name + ' >> "$INTENTD_COMPILER_CALL_LOG"\necho "consumer attempted compilation" >&2\nexit 97\n')
            path.chmod(0o755)
    paths = [p for p in os.environ['PATH'].split(os.pathsep) if p != str(tripwires)]
    env = dict(os.environ, PATH=os.pathsep.join([str(tripwires), *paths]),
               RUSTC=str(tripwires / ('rustc.exe' if sys.platform == 'win32' else 'rustc')),
               INTENTD_COMPILER_CALL_LOG=str(log))
    if sys.platform == 'win32':
        # Windows CreateProcess uses the parent's PATH for Python's executable
        # lookup even when a child's env supplies another PATH.
        os.environ.update(PATH=env['PATH'], RUSTC=env['RUSTC'])
    return env


def prepare(source, root):
    source, root = Path(source).resolve(), Path(root).resolve()
    requests = json.loads((root / 'requests.json').read_text())
    env = consumer_environment(root)
    probe_log = root / 'tripwire-probe.log'
    probe_env = dict(env, INTENTD_COMPILER_CALL_LOG=str(probe_log))
    for command in ('cargo', 'rustc', 'rustup'):
        result = subprocess.run([command, '--version'], env=probe_env, capture_output=True)
        if result.returncode != 97:
            raise RuntimeError('compiler tripwire did not intercept ' + command)
    prepared = {}
    for name, request in requests.items():
        key = archives.identity.build_key(request['identity'])
        prepared[name] = archives.prepare(root / 'published' / key, request, source, root / 'restored' / name, env=env)
    (root / 'prepared.json').write_text(json.dumps(prepared, indent=2) + '\n')
    return prepared


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('scope', 'produce', 'prepare', 'libtest', 'receipt'))
    parser.add_argument('--root', type=Path, default=Path(os.environ.get('RUNNER_TEMP', '.')) / 'native-artifacts')
    parser.add_argument('--source', type=Path, default=Path.cwd())
    parser.add_argument('--platform', choices=('linux', 'macos', 'windows'))
    parser.add_argument('--event', choices=('pull_request', 'merge_group'))
    parser.add_argument('--base')
    parser.add_argument('--variant')
    parser.add_argument('--binary')
    args, test_args = parser.parse_known_args()
    if args.command != 'libtest' and test_args:
        parser.error('unexpected arguments: ' + repr(test_args))
    if args.command == 'scope':
        changed = []
        if args.event == 'pull_request':
            try:
                if not args.base:
                    raise ValueError('PR base unavailable')
                changed = archives.output(['git', 'diff', '--name-only', args.base, 'HEAD'], args.source).splitlines()
            except (ValueError, OSError, subprocess.CalledProcessError):
                print('Changed-path discovery unavailable; requiring every native proof', file=sys.stderr)
                changed = None
        scope = native_scope(args.event, changed)
        with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
            output.write('event=' + scope + '\n')
        print('Native demand: ' + scope + (' (native CI verification)' if scope != args.event else ''))
    elif args.command == 'produce':
        produce(args.source, args.root, args.platform, args.event)
    elif args.command == 'prepare':
        prepare(args.source, args.root)
    elif args.command == 'libtest':
        prepared = json.loads((args.root / 'prepared.json').read_text())[args.variant]
        return archives.run_libtest(prepared, args.binary, test_args[1:] if test_args[:1] == ['--'] else test_args,
                                    env=consumer_environment(args.root))
    else:
        log = args.root / 'compiler-calls.log'
        if log.exists() and log.stat().st_size:
            raise RuntimeError('consumer invoked a compiler: ' + log.read_text())
        receipt = {'compiler_calls': 0, 'consumer_finished_at': time.time(),
                   'restorations': json.loads((args.root / 'prepared.json').read_text())}
        (args.root / 'consumer-receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
        print('Native consumers: zero compiler calls')
    return 0

if __name__ == '__main__':
    sys.exit(main())
