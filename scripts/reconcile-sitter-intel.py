#!/usr/bin/env python3
"""Reconcile existing optional Intel assets before publishing sitter archives.

Versioned releases describe only this run. The rolling release may retain an
older Intel pair, but only after downloading and verifying its actual bytes.
Failed lookups/downloads stop publication rather than guessing remote state.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile

spec = importlib.util.spec_from_file_location('policy', Path(__file__).with_name('optional-intel-release.py'))
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)


def reconcile(tag, repo, artifacts, retain_valid=False):
    if retain_valid and tag != 'sitter-latest':
        raise ValueError('only sitter-latest may retain previous Intel assets')
    if policy.valid_archive(artifacts, policy.INTEL):
        return  # The upload replaces both remote files with this complete pair.
    names = [f'intentd-{policy.INTEL}.tar.xz', f'intentd-{policy.INTEL}.tar.xz.sha256']
    release = json.loads(subprocess.check_output(
        ['gh', 'release', 'view', tag, '--repo', repo, '--json', 'assets'], text=True))
    present = {asset['name'] for asset in release['assets']}
    if retain_valid and all(name in present for name in names):
        with tempfile.TemporaryDirectory(prefix='sitter-retained-intel-') as temp:
            for name in names:
                subprocess.run(['gh', 'release', 'download', tag, '--repo', repo,
                                '--pattern', name, '--dir', temp], check=True)
            if policy.valid_archive(Path(temp), policy.INTEL):
                return
    for name in names:
        if name in present:
            subprocess.run(['gh', 'release', 'delete-asset', tag, name,
                            '--repo', repo, '--yes'], check=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('tag')
    parser.add_argument('artifacts', type=Path)
    parser.add_argument('--repo', required=True)
    parser.add_argument('--retain-valid', action='store_true')
    args = parser.parse_args()
    reconcile(args.tag, args.repo, args.artifacts, args.retain_valid)
