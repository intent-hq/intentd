#!/usr/bin/env python3
"""Require native lifecycle execution, using archives in CI or local Cargo builds."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def require_passed(output, minimum=1):
    results = re.findall(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored", output)
    if not results or sum(int(passed) for passed, _, _ in results) < minimum:
        raise RuntimeError("lifecycle proof selected no passing tests")
    if any(int(failed) or int(ignored) for _, failed, ignored in results):
        raise RuntimeError("lifecycle proof failed or skipped tests")


def run(args, env=None, *, cwd=None):
    print("+ " + " ".join(args), flush=True)
    with subprocess.Popen(args, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:
        lines = []
        for line in process.stdout:
            print(line, end="", flush=True)
            lines.append(line)
        output = "".join(lines)
        if process.wait():
            raise subprocess.CalledProcessError(process.returncode, args, output)
        return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archives', type=Path)
    args = parser.parse_args()
    if args.archives:
        return archived_lifecycle(args.archives)
    env = dict(os.environ, CARGO_TERM_PROGRESS_WHEN="never")
    artifacts = run(["cargo", "build", "--locked", "-p", "intentd-sitter", "--message-format=json"], env)
    sitter = None
    for line in artifacts.splitlines():
        if line.startswith("{"):
            item = json.loads(line)
            if item.get("reason") == "compiler-artifact" and item.get("target", {}).get("name") == "intentd-sitter" and item.get("executable"):
                sitter = Path(item["executable"]).resolve()
    if sitter is None or not sitter.is_file():
        raise RuntimeError("current build did not produce the sitter executable")
    env["INTENTD_TEST_SITTER_BIN"] = str(sitter)
    require_passed(run(["cargo", "test", "--locked", "-p", "intentd", "--test", "e2e_detached_lifecycle", "lifecycle_capture_"], env), minimum=3)
    require_passed(run(["cargo", "test", "--locked", "-p", "intentd-sitter", "--test", "cli_help"], env), minimum=2)
    require_passed(run(["cargo", "test", "--locked", "-p", "intentd-sitter", "--lib"], env))
    if os.name != "nt":
        require_passed(run(["cargo", "test", "--locked", "-p", "intentd-sitter", "--lib", "--", "--exact", "startup::tests::dead_supervisor_cleanup_closes_descendant_pipe"], env))
        require_passed(run(["cargo", "test", "--locked", "-p", "intentd-sitter", "--test", "supervisor_e2e", "background_start"], env))
    else:
        require_passed(run(["cargo", "test", "--locked", "-p", "intentd-sitter", "--lib", "--", "--exact", "windows::tests::restart_events_reject_foreign_identity_and_require_explicit_completion"], env))
        require_passed(run(["cargo", "test", "--locked", "-p", "intentd", "--test", "e2e_detached_lifecycle", "windows_"], env))
    require_passed(run(["cargo", "test", "--locked", "-p", "intentd", "--test", "e2e_detached_lifecycle", "--", "--ignored", "--exact", "detached_sitter_lifecycle_over_wss", "--nocapture"], env))


def archived_lifecycle(root):
    spec = importlib.util.spec_from_file_location('native', Path(__file__).with_name('ci-native-artifacts.py'))
    native = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(native)
    prepared = json.loads((root / 'prepared.json').read_text())
    env = native.consumer_environment(root)
    env['INTENTD_TEST_SITTER_BIN'] = str(native.archives.companion_path(
        prepared['sitter-production'], 'intentd-sitter', producer_group=prepared['daemon']['producer_group']))
    def test(variant, binary, selection, minimum=1):
        item = prepared[variant]
        runtime = item['runtime']['tests'][binary]
        command = [str(native.archives.safe_join(item['target'], runtime['path'])), *selection]
        environ = native.archives.runtime_env(item, env)
        cwd = native.archives.safe_join(item['source'], runtime['cwd'])
        environ['CARGO_MANIFEST_DIR'] = str(cwd)
        require_passed(run(command, environ, cwd=cwd), minimum)
    test('daemon', 'intentd::e2e_detached_lifecycle', ['lifecycle_capture_'], 3)
    test('sitter', 'intentd-sitter::cli_help', [], 2)
    test('sitter', 'intentd-sitter', [])
    if os.name != 'nt':
        test('sitter', 'intentd-sitter', ['--exact', 'startup::tests::dead_supervisor_cleanup_closes_descendant_pipe'])
        test('sitter', 'intentd-sitter::supervisor_e2e', ['background_start'])
    else:
        test('sitter', 'intentd-sitter', ['--exact', 'windows::tests::restart_events_reject_foreign_identity_and_require_explicit_completion'])
        test('daemon', 'intentd::e2e_detached_lifecycle', ['windows_'])
    test('daemon', 'intentd::e2e_detached_lifecycle', ['--ignored', '--exact', 'detached_sitter_lifecycle_over_wss', '--nocapture'])


if __name__ == "__main__":
    main()
