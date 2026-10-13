#!/usr/bin/env python3
"""Select release-only Intel jobs and validate the exact published target set.

Only Intel Mac may be omitted. Archives and their checksum companions are an
atomic pair; a failed optional upload must not leak a partial pair downstream.
No network or release mutations happen here.
"""
import argparse
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
INTEL = 'x86_64-apple-darwin'


def release_matrix(matrix, event, ref):
    publishing = event == 'push' and re.fullmatch(r'refs/tags/v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?', ref)
    result = deepcopy(matrix)
    rows = []
    for row in result['include']:
        targets = row['targets']
        if INTEL in targets and targets != [INTEL]:
            raise ValueError('Intel must have its own cargo-dist matrix job (merge-tasks=false)')
        if INTEL in targets and not publishing:
            continue
        row['optional'] = targets == [INTEL]
        rows.append(row)
    result['include'] = rows
    return result


def valid_archive(root, target):
    suffix = 'zip' if 'windows' in target else 'tar.xz'
    archive = root / f'intentd-{target}.{suffix}'
    checksum = archive.with_name(archive.name + '.sha256')
    if (not archive.is_file() or not checksum.is_file() or archive.is_symlink()
            or checksum.is_symlink() or archive.stat().st_size == 0):
        return False
    try:
        sidecar = checksum.read_text()
    except (OSError, UnicodeError):
        return False
    match = re.fullmatch(r'([0-9a-f]{64})(?: [ *]' + re.escape(archive.name) + r')?\s*', sidecar)
    if not match:
        return False
    with archive.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return digest == match[1]


def omit_intel_metadata(manifest):
    """Remove planned Intel downloads before cargo-dist merges local manifests."""
    manifest = deepcopy(manifest)
    manifest['artifacts'] = {key: value for key, value in manifest.get('artifacts', {}).items()
                             if INTEL not in key}
    for release in manifest.get('releases', []):
        release['artifacts'] = [name for name in release.get('artifacts', []) if INTEL not in name]
    if 'upload_files' in manifest:
        manifest['upload_files'] = [name for name in manifest['upload_files'] if INTEL not in name]
    if manifest.get('ci', {}).get('github', {}).get('artifacts_matrix'):
        manifest['ci']['github']['artifacts_matrix'] = release_matrix(
            manifest['ci']['github']['artifacts_matrix'], 'omitted', '')
    if manifest.get('announcement_github_body'):
        manifest['announcement_github_body'] = ''.join(
            line for line in manifest['announcement_github_body'].splitlines(keepends=True)
            if not (line.startswith('|') and f'intentd-{INTEL}.' in line))
    return manifest


def reconcile_aggregate(root, included):
    """Keep an already-generated cargo-dist aggregate consistent at late cleanup."""
    aggregate = root / 'sha256.sum'
    if not aggregate.exists():
        return
    retained = []
    names = set()
    for line in aggregate.read_text().splitlines(keepends=True):
        if not line.strip():
            retained.append(line)
            continue
        match = re.fullmatch(r'([0-9a-f]{64}) [ *]([^/\\\r\n]+)\n?', line)
        if not match:
            raise ValueError('invalid sha256.sum entry')
        digest, name = match.groups()
        if INTEL not in included and INTEL in name:
            continue
        path = root / name
        if name in names or path.is_symlink() or not path.is_file():
            raise ValueError(f'invalid sha256.sum file: {name}')
        with path.open('rb') as stream:
            if hashlib.file_digest(stream, 'sha256').hexdigest() != digest:
                raise ValueError(f'invalid sha256.sum checksum: {name}')
        names.add(name)
        retained.append(line)
    for target in included:
        suffix = 'zip' if 'windows' in target else 'tar.xz'
        if f'intentd-{target}.{suffix}' not in names:
            raise ValueError(f'missing sha256.sum archive: {target}')
    aggregate.write_text(''.join(retained))


def validated_targets(root):
    targets = tomllib.loads((ROOT / 'dist-workspace.toml').read_text())['dist']['targets']
    included = []
    for target in targets:
        if valid_archive(root, target):
            included.append(target)
        elif target == INTEL:
            for path in root.glob(f'*{INTEL}*'):
                if path.is_file():
                    path.unlink()
            print('Intel Mac artifacts absent or incomplete; excluding this target', file=sys.stderr)
        else:
            raise ValueError(f'missing or invalid required archive/checksum: {target}')
    if INTEL not in included:
        for path in root.glob('*dist-manifest.json'):
            path.write_text(json.dumps(omit_intel_metadata(json.loads(path.read_text())), indent=2) + '\n')
    reconcile_aggregate(root, included)
    return included


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    matrix = sub.add_parser('matrix')
    matrix.add_argument('manifest', type=Path)
    matrix.add_argument('--event', default=os.environ.get('GITHUB_EVENT_NAME', ''))
    matrix.add_argument('--ref', default=os.environ.get('GITHUB_REF', ''))
    targets = sub.add_parser('targets')
    targets.add_argument('directory', type=Path)
    args = parser.parse_args()
    if args.command == 'matrix':
        manifest = json.loads(args.manifest.read_text())
        manifest['ci']['github']['artifacts_matrix'] = release_matrix(
            manifest['ci']['github']['artifacts_matrix'], args.event, args.ref)
        args.manifest.write_text(json.dumps(manifest, indent=2) + '\n')
    else:
        print(' '.join('--target=' + target for target in validated_targets(args.directory)))


if __name__ == '__main__':
    main()
