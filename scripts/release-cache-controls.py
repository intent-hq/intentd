#!/usr/bin/env python3
"""Bound stopped sccache files and retain one main-scoped generation per slot.

Only producers call this script. The 256 MiB pre-upload bound is independent of
sccache's asynchronous eviction. Ten slots retain <= 2.5 GiB of entry data;
archive overhead and temporary overlap during refresh are additional. GitHub's
repository LRU can still evict any slot. Consumers must always tolerate a miss.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess

TARGET_RUNNERS = {
    'aarch64-apple-darwin': ('macos-14', 'macos-14'),
    'x86_64-apple-darwin': ('macos-14', 'macos-14'),
    'aarch64-unknown-linux-musl': ('gh-linux-16x', 'gh-linux-16x'),
    'x86_64-unknown-linux-musl': ('gh-linux-16x', 'gh-linux-16x'),
    'x86_64-pc-windows-msvc': ('windows-2022', 'gh-windows-16x'),
}
SLOTS = {
    f'intentd-release-v1-{flavor}-{target}-{runners[index]}--'
    for index, flavor in enumerate(('daemon-dist', 'sitter-release'))
    for target, runners in TARGET_RUNNERS.items()
}


def bound(root, limit):
    if limit < 0 or root.is_symlink():
        raise ValueError('invalid cache root or byte limit')
    files = []
    for directory, dirs, names in os.walk(root, followlinks=False):
        for name in [*dirs, *names]:
            path = Path(directory) / name
            if path.is_symlink():
                path.unlink()
            elif path.is_file():
                stat = path.stat()
                files.append((stat.st_mtime_ns, str(path), stat.st_size))
    before = total = sum(size for _, _, size in files)
    removed = 0
    # Each sccache entry is one file; remove oldest whole entries. A missing
    # entry is an ordinary compiler miss, with no Cargo fingerprints to repair.
    for _, name, size in sorted(files):
        if total <= limit:
            break
        Path(name).unlink()
        total -= size
        removed += 1
    return {'before_bytes': before, 'after_bytes': total, 'removed_entries': removed}


def obsolete(rows):
    groups = {}
    for row in rows:
        slot = row['key'].split('--', 1)[0] + '--'
        if row['ref'] == 'refs/heads/main' and slot in SLOTS:
            groups.setdefault(slot, []).append(row)
    return sorted(row['id'] for group in groups.values()
                  for row in sorted(group, key=lambda r: (r['created_at'], r['id']), reverse=True)[1:])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    trim = sub.add_parser('bound')
    trim.add_argument('directory', type=Path)
    trim.add_argument('--limit', type=int, required=True)
    sub.add_parser('retention')
    args = parser.parse_args()
    if args.command == 'bound':
        result = bound(args.directory, args.limit)
        print(json.dumps(result))
        if summary := os.environ.get('GITHUB_STEP_SUMMARY'):
            with open(summary, 'a') as output:
                output.write(f'\nCache bytes before/after bound: {result["before_bytes"]} / {result["after_bytes"]}; removed entries: {result["removed_entries"]}.\n')
    else:
        if os.environ.get('GITHUB_REF') != 'refs/heads/main':
            raise SystemExit('retention is only allowed on main')
        repo = os.environ['GITHUB_REPOSITORY']
        if not re.fullmatch(r'[\w.-]+/[\w.-]+', repo):
            raise SystemExit('invalid repository')
        pages = json.loads(subprocess.check_output([
            'gh', 'api', '--paginate', '--slurp',
            f'repos/{repo}/actions/caches?ref=refs/heads/main&key=intentd-release-v1-&per_page=100',
        ], text=True))
        rows = [row for page in pages for row in page['actions_caches']]
        for cache_id in obsolete(rows):
            subprocess.run(['gh', 'api', '--method', 'DELETE', f'repos/{repo}/actions/caches/{cache_id}'], check=True)
            print(f'Deleted superseded release cache {cache_id}')


if __name__ == '__main__':
    main()
