#!/usr/bin/env python3
"""Recognize release-plz metadata without executing anything from the PR.

Each crate may advance independently. Only package versions and exact matching
local dependency versions may change; external dependencies, features, sources,
checksums, file modes and code remain unchanged. Unknown shapes fail closed.
"""
import copy
import posixpath
import re
import subprocess
import sys
import tomllib


class NoMatch(Exception):
    pass


def git(*args):
    return subprocess.check_output(['git', *args], text=True, stderr=subprocess.PIPE)


def manifest(rev, path):
    return tomllib.loads(git('show', f'{rev}:{path}'))


def require(condition, reason):
    if not condition:
        raise NoMatch(reason)


def dependency_tables(doc):
    for section in [doc, *doc.get('target', {}).values()]:
        for key in ('dependencies', 'dev-dependencies', 'build-dependencies'):
            yield section.get(key, {})


def matches(base, head):
    base = git('rev-parse', '--verify', f'{base}^{{commit}}').strip()
    head = git('rev-parse', '--verify', f'{head}^{{commit}}').strip()
    try:
        base = git('merge-base', base, head).strip()
    except subprocess.CalledProcessError:
        pass  # Keep the existing shallow-checkout fallback to the supplied base.
    changes = git('diff', '--raw', '--no-abbrev', '--no-renames', base, head).splitlines()
    require(changes, 'empty diff')
    paths = []
    for change in changes:
        meta, path = change.split('\t')
        old_mode, new_mode, _, _, status = meta.lstrip(':').split()
        require(status == 'M' and old_mode == new_mode == '100644', f'not a regular-file modification: {path}')
        require(path in ('CHANGELOG.md', 'Cargo.lock') or re.fullmatch(r'crates/[^/]+/Cargo\.toml', path), f'disallowed file: {path}')
        paths.append(path)

    crate_paths = [p for p in git('ls-tree', '-r', '--name-only', base, '--', 'crates').splitlines() if re.fullmatch(r'crates/[^/]+/Cargo\.toml', p)]
    before = {p: manifest(base, p) for p in crate_paths}
    after = {p: manifest(head, p) if p in paths else copy.deepcopy(doc) for p, doc in before.items()}
    bumps = {}
    names = {}
    for path, old in before.items():
        new = after[path]
        name = old['package']['name']
        require(name not in names, f'duplicate package: {name}')
        names[name] = path
        a, b = old['package'].get('version'), new['package'].get('version')
        if a != b:
            require(all(isinstance(v, str) and re.fullmatch(r'\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.+-]+)?', v) for v in (a, b)), f'unknown package version shape: {path}')
            bumps[name] = (a, b)
    require(bumps, 'no package version change')

    for path in paths:
        if path not in before:
            continue
        old, norm = before[path], copy.deepcopy(after[path])
        name = old['package']['name']
        if name in bumps:
            norm['package']['version'] = bumps[name][0]
        for dependencies in dependency_tables(norm):
            for alias, dep in dependencies.items():
                if not isinstance(dep, dict) or not isinstance(dep.get('path'), str):
                    continue
                target = posixpath.normpath(posixpath.join(posixpath.dirname(path), dep['path'], 'Cargo.toml'))
                package = dep.get('package', alias)
                if target != names.get(package) or package not in bumps or 'git' in dep or 'registry' in dep:
                    continue
                a, b = bumps[package]
                if dep.get('version') == b:
                    dep['version'] = a
        require(norm == old, f'non-release manifest change: {path}')

    if 'Cargo.lock' in paths:
        old, norm = manifest(base, 'Cargo.lock'), manifest(head, 'Cargo.lock')
        for package in norm.get('package', []):
            name = package['name']
            if name in bumps and 'source' not in package:
                a, b = bumps[name]
                require(package['version'] == b, f'lock version does not match package: {name}')
                package['version'] = a
            # Cargo sometimes disambiguates dependencies as "name version".
            # Source-qualified references must never be normalized.
            for i, dep in enumerate(package.get('dependencies', [])):
                parts = dep.split(' ')
                if len(parts) == 2 and parts[0] in bumps:
                    a, b = bumps[parts[0]]
                    if parts[1] == b:
                        package['dependencies'][i] = f'{parts[0]} {a}'
        require(norm == old, 'Cargo.lock changed beyond matching local package versions')


def main():
    if not 2 <= len(sys.argv) <= 3:
        print(f'usage: {sys.argv[0]} <base> [<head>]', file=sys.stderr)
        return 2
    try:
        matches(sys.argv[1], sys.argv[2] if len(sys.argv) == 3 else 'HEAD')
    except (NoMatch, subprocess.CalledProcessError, ValueError, KeyError, TypeError) as error:
        print(f'release-pr-fast-path: no match: {error}', file=sys.stderr)
        print('fast_path=false')
    else:
        print('fast_path=true')
    return 0


if __name__ == '__main__':
    sys.exit(main())
