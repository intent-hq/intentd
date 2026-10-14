#!/usr/bin/env python3
"""Fresh job storage; never provision mounts, prune caches or remove another run."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time


def token():
    parts = [os.environ[k] for k in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_JOB')]
    if any(not re.fullmatch(r'[A-Za-z0-9_-]+', part) for part in parts):
        raise ValueError('invalid artifact storage ownership')
    return 'run-' + '-'.join(parts)


def alternate():
    raid = Path('/raid')
    slot = raid / 'ci-target/intentd' / os.environ['RUNNER_NAME']
    if raid.is_mount() and slot.is_dir() and os.access(slot, os.W_OK):
        return slot
    return None


def initialize(min_bytes, allow_alternate=False):
    temporary = Path(os.environ['RUNNER_TEMP']).resolve()
    base, slot = temporary, None
    if shutil.disk_usage(base).free < min_bytes:
        candidate = alternate() if allow_alternate else None
        if candidate is None or shutil.disk_usage(candidate).free < min_bytes:
            raise ValueError('insufficient artifact storage; no provisioned writable alternate has capacity')
        slot = candidate / (os.environ['GITHUB_JOB'] + '-artifacts')
        base = slot
    root = base / token()
    # No reuse of failed-attempt leftovers, nor cleanup of someone else's data.
    root.mkdir(parents=True, exist_ok=False)
    if slot:
        subprocess.run(['bash', str(Path(__file__).with_name('ci-target-slot.sh')), 'mark', str(slot)], check=True)
    owner = {'token': token(), 'root': str(root), 'slot': str(slot) if slot else None,
             'minimum_free_bytes': min_bytes, 'free_bytes_at_start': shutil.disk_usage(root).free}
    (root / 'storage.json').write_text(json.dumps(owner, indent=2) + '\n')
    with open(os.environ['GITHUB_ENV'], 'a') as stream:
        stream.write('NATIVE_ARTIFACT_ROOT=' + str(root) + '\n')
    return root


def cleanup(root):
    root = Path(root).resolve()
    owner = json.loads((root / 'storage.json').read_text())
    if owner['token'] != token() or owner['root'] != str(root) or root.name != token():
        raise ValueError('refusing artifact cleanup for another run')
    slot = owner['slot']
    if slot and root.parent != Path(slot):
        raise ValueError('invalid artifact cleanup slot')
    shutil.rmtree(root)
    if slot:
        subprocess.run(['bash', str(Path(__file__).with_name('ci-target-slot.sh')), 'unmark', slot], check=True)


def usage(root):
    logical = allocated = 0
    for directory, _, files in os.walk(root, followlinks=False):
        for filename in files:
            try:
                stat = (Path(directory) / filename).lstat()
                logical += stat.st_size
                allocated += getattr(stat, 'st_blocks', (stat.st_size + 4095) // 4096 * 8) * 512
            except FileNotFoundError:
                pass  # a just-completed ZIP or staging directory was removed
    return {'logical_bytes': logical, 'allocated_bytes': allocated}


def watch(root):
    root = Path(root)
    high = {'logical_bytes': 0, 'allocated_bytes': 0}
    target_high = {'logical_bytes': 0, 'allocated_bytes': 0}
    samples = 0
    while root.is_dir():
        measured = usage(root)
        sdk = usage(Path(os.environ['RUNNER_TEMP']) / 'intent-artifact-client')
        target_path = root / 'storage-target.json'
        target_usage = None
        if target_path.is_file():
            try:
                target = Path(json.loads(target_path.read_text())['target'])
                if not target.is_relative_to(root):
                    target_usage = usage(target)
            except (FileNotFoundError, json.JSONDecodeError):
                pass
        if target_usage:
            target_high = {key: max(target_high[key], value) for key, value in target_usage.items()}
        measured = {key: value + sdk[key] for key, value in measured.items()}
        high = {key: max(high[key], value) for key, value in measured.items()}
        samples += 1
        if not root.is_dir():
            return
        receipt = {'sample_interval_seconds': 10, 'samples': samples,
                   'scope': 'job artifact root (ZIPs, staging, extracted targets, profiles, reports) plus artifact SDK',
                   'current': measured, 'sampled_high_water': high,
                   'separate_persistent_compile_target': target_usage,
                   'compile_target_sampled_high_water': target_high,
                   'allocation_method': 'st_blocks' if os.name == 'posix' else '4096-byte estimate',
                   'filesystem_free_bytes': shutil.disk_usage(root).free}
        try:
            (root / 'storage-usage.json').write_text(json.dumps(receipt, indent=2) + '\n')
        except FileNotFoundError:
            return
        time.sleep(10)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['init', 'cleanup', 'watch'])
    parser.add_argument('--minimum-gib', type=int, default=1)
    parser.add_argument('--allow-alternate', action='store_true')
    parser.add_argument('--root', default=os.environ.get('NATIVE_ARTIFACT_ROOT'))
    args = parser.parse_args()
    if args.mode == 'init':
        root = initialize(args.minimum_gib * 1024**3, args.allow_alternate)
        subprocess.Popen([sys.executable, str(Path(__file__).resolve()), 'watch', '--root', str(root)],
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         start_new_session=True)
        print(root)
    elif args.mode == 'cleanup':
        if args.root and (Path(args.root) / 'storage.json').is_file():
            cleanup(args.root)
    else:
        watch(args.root)
