#!/usr/bin/env python3
"""Generate the build-only producer from release setup sources (requires PyYAML).

Run after changing .github/dist-build-setup.yml or sitter build setup. Daemon
matrix/native dependencies come from `dist plan` at run time, never a second
hand-maintained approximation of cargo-dist's flags or cross compiler choices.
"""
import argparse
from copy import deepcopy
import json
from pathlib import Path
import sys
import runpy
import tomllib

import yaml

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / '.github/workflows/release-cache.yml'
MAIN_ONLY = "github.ref == 'refs/heads/main' && (github.event_name == 'push' || github.event_name == 'workflow_dispatch')"
# A release-metadata push (release-plz version bumps on the tree the previous
# push already warmed) changes only workspace crate versions, which the tag's
# release build compiles at the same time, so warming it buys nothing. Pushes
# of that shape skip the build jobs; workflow_dispatch always warms.
NOT_RELEASE_PUSH = "needs.plan.outputs.release_push != 'true'"
CLASSIFY_RUN = '''# Same classifier and contract as ci.yml's release-fast-path job: any
# failure or unknown shape is a miss, so the warm still runs.
git fetch --no-tags --depth=2 origin "$GITHUB_SHA" || true
out=$(./scripts/release-pr-fast-path.sh --exact-parent "$BASE_SHA" HEAD) || out="fast_path=false"
printf '%s\\n' "$out"
line=$(printf '%s\\n' "$out" | grep -E '^fast_path=(true|false)$' | tail -n1 || true)
line=${line:-fast_path=false}
echo "release_push=${line#fast_path=}" >> "$GITHUB_OUTPUT"
'''
CACHE_RUN = 'python3 scripts/setup-sccache.py'
# Producers run on main with secrets, so they write the self-hosted cache when
# INTENT_SCCACHE_ENDPOINT is set (INTENT_SCCACHE_MODE unset means READ_WRITE).
CACHE_ENV = {'INTENT_SCCACHE_ENDPOINT': '${{ vars.INTENT_SCCACHE_ENDPOINT }}',
             'INTENT_SCCACHE_TOKEN': '${{ secrets.INTENT_SCCACHE_TOKEN }}',
             'INTENT_SCCACHE_MODE': '${{ vars.INTENT_SCCACHE_MODE }}'}
CHECKOUT = {'uses': 'actions/checkout@v7', 'with': {'persist-credentials': False}}


class Dumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


def string(dumper, value):
    return dumper.represent_scalar('tag:yaml.org,2002:str', value, style='|' if '\n' in value else None)


Dumper.add_representer(str, string)


def timed_build(step):
    step = deepcopy(step)
    if step.get('shell') != 'bash' or 'run' not in step:
        raise ValueError('cache build timing requires an explicit bash run step')
    step['run'] = ('start=$SECONDS\n' + step['run'].rstrip() + '\n'
                   'echo "Build duration: $((SECONDS - start)) seconds" >> "$GITHUB_STEP_SUMMARY"\n')
    return step


def cache_steps(flavor, target, runner, builds):
    return [
        {'name': 'Configure the self-hosted compiler cache', 'shell': 'bash', 'env': deepcopy(CACHE_ENV), 'run': CACHE_RUN},
        *[timed_build(step) for step in builds],
        {'name': 'Report compiler cache statistics', 'shell': 'bash',
         'run': 'sccache --show-stats >> "$GITHUB_STEP_SUMMARY"'},
    ]


def generate():
    config = tomllib.loads((ROOT / 'dist-workspace.toml').read_text())['dist']
    version = config['cargo-dist-version']
    install = f'curl --proto \'=https\' --tlsv1.2 -LsSf https://github.com/axodotdev/cargo-dist/releases/download/v{version}/cargo-dist-installer.sh | sh'
    # Consumers append the cache setup script. Exclude that step
    # here: producers call it exactly once below.
    daemon_setup = [s for s in yaml.safe_load((ROOT / '.github/dist-build-setup.yml').read_text())
                    if s.get('run') != CACHE_RUN]
    sitter_source = yaml.safe_load((ROOT / '.github/workflows/release-sitter.yml').read_text())['jobs']['build']
    source_steps = sitter_source['steps']
    build_start = next(i for i, step in enumerate(source_steps) if step.get('name') == 'Build (cargo)')
    sitter_setup = [step for step in source_steps[:build_start] if step.get('run') != CACHE_RUN]
    sitter_strategy = deepcopy(sitter_source['strategy'])
    sitter_strategy['matrix']['include'] = [row for row in sitter_strategy['matrix']['include']
                                            if row['target'] != 'x86_64-apple-darwin']
    sitter_builds = source_steps[build_start:build_start + 2]
    if [step.get('name') for step in sitter_builds] != ['Build (cargo)', 'Build (cargo-zigbuild)']:
        raise ValueError('expected adjacent sitter cargo/zigbuild steps; review the producer boundary')
    doc = {
        'name': 'Warm release Rust caches',
        'on': {
            'push': {'branches': ['main'], 'paths': [
                'Cargo.lock', '**/Cargo.toml', 'rust-toolchain.toml', '.cargo/config*',
                'dist-workspace.toml', '**/build.rs', '**/*.c', '**/*.cc', '**/*.cpp', '**/*.h',
                '.github/dist-build-setup.yml',
                'scripts/setup-sccache.py', '.github/workflows/release-cache.yml',
                '.github/workflows/release-sitter.yml', 'scripts/*release*cache*.py', 'scripts/optional-intel-release.py',
            ]},
            'workflow_dispatch': {},
        },
        'permissions': {'contents': 'read'},
        # Distinct from release writer locks; never warm on a source-only push.
        'concurrency': {'group': 'intentd-release-cache-producers', 'cancel-in-progress': False},
        'jobs': {
            'plan': {
                'if': MAIN_ONLY, 'runs-on': 'ubuntu-24.04', 'timeout-minutes': 10,
                'outputs': {'matrix': '${{ steps.plan.outputs.matrix }}',
                            'release_push': '${{ steps.release.outputs.release_push }}'},
                'steps': [deepcopy(CHECKOUT),
                          {'id': 'release', 'name': 'Classify release-metadata push', 'if': "github.event_name == 'push'",
                           'shell': 'bash', 'env': {'BASE_SHA': '${{ github.event.before }}'}, 'run': CLASSIFY_RUN},
                          {'name': 'Install dist', 'shell': 'bash', 'run': install},
                          {'id': 'plan', 'name': 'Plan build-only matrix', 'shell': 'bash',
                           'run': '''dist plan --output-format=json > cache-plan.json
python3 scripts/optional-intel-release.py matrix cache-plan.json --event cache --ref refs/heads/main
echo "matrix=$(jq -c '.ci.github.artifacts_matrix' cache-plan.json)" >> "$GITHUB_OUTPUT"
'''}],
            },
            'daemon': {
                'if': f'{MAIN_ONLY} && {NOT_RELEASE_PUSH}', 'needs': 'plan',
                'strategy': {'fail-fast': False, 'matrix': '${{ fromJson(needs.plan.outputs.matrix) }}'},
                'runs-on': '${{ matrix.runner }}', 'timeout-minutes': 60,
                'steps': [{'name': 'Enable Windows long paths', 'shell': 'bash', 'run': 'git config --global core.longpaths true'}, deepcopy(CHECKOUT), *daemon_setup,
                          {'name': 'Install dist (Unix)', 'if': "runner.os != 'Windows'", 'shell': 'bash', 'run': '${{ matrix.install_dist.run }}'},
                          {'name': 'Install dist (Windows)', 'if': "runner.os == 'Windows'", 'shell': 'pwsh', 'run': '${{ matrix.install_dist.run }}'},
                          {'name': 'Install native build dependencies', 'shell': 'bash', 'run': '${{ matrix.packages_install }}'},
                          *cache_steps('daemon-dist', "${{ join(matrix.targets, ' ') }}", '${{ matrix.runner }}',
                                       [{'name': 'Build cache inputs', 'shell': 'bash',
                                         'run': 'dist build --output-format=json ${{ matrix.dist_args }} > cache-build-manifest.json'}])],
            },
            'sitter': {
                'if': f'{MAIN_ONLY} && {NOT_RELEASE_PUSH}', 'needs': 'plan', 'strategy': sitter_strategy,
                'runs-on': '${{ matrix.os }}', 'timeout-minutes': 30,
                'env': deepcopy(sitter_source.get('env', {})),
                'steps': [*sitter_setup,
                          *cache_steps('sitter-release', '${{ matrix.target }}', '${{ matrix.os }}',
                                       sitter_builds)],
            },

        },
    }
    output = '''# Generated by scripts/configure-release-cache-producer.py; do not edit.
# Main-only, build-only: warm the self-hosted sccache using the release build inputs.
# No cache archives, retention permissions, publication or notifications.
# Compare actual compiler hits and build duration before claiming a speedup.
''' + yaml.dump(doc, Dumper=Dumper, sort_keys=False, width=110)
    routed = runpy.run_path(str(ROOT / 'scripts/configure-runner-routing.py'))['configure'](output)
    return fit_sitter_slots(routed, sitter_strategy['matrix']['include'])


def fit_sitter_slots(text, rows):
    """Give each heavy Linux sitter leg its own slot so parallel legs never share one free slot."""
    legs = [row['target'] for row in rows if row['os'] in ('gh-linux-8x', 'gh-linux-16x')]
    labels = {row['os'] for row in rows if row['target'] in legs}
    if len(labels) != 1:
        raise ValueError('sitter Linux legs must share one hosted label; review the slot list')
    slot_jobs = json.dumps([{'key': 'sitter', 'label': labels.pop(), 'rust': True, 'minutes': 20, 'legs': legs}],
                           separators=(',', ':'))
    route = '  runner-route:\n    uses: ./.github/workflows/runner-route.yml\n'
    old = "    runs-on: ${{ fromJSON(needs.runner-route.outputs.runners || '{}')[matrix.os] || matrix.os }}\n    timeout-minutes: 30\n"
    new = ("    runs-on: ${{ contains(matrix.os, 'gh-linux') && fromJSON(needs.runner-route.outputs.slots || '{}')"
           "[format('sitter-{0}', matrix.target)] || fromJSON(needs.runner-route.outputs.runners || '{}')[matrix.os]"
           " || matrix.os }}\n    timeout-minutes: 30\n")
    if text.count(route) != 1 or text.count(old) != 1:
        raise ValueError('release-cache routing changed; review the sitter slot list')
    text = text.replace(route, route + '    with:\n      # Each heavy Linux sitter leg reads its own slot (see runner-route.yml).\n'
                        '      slot_jobs: >-\n        ' + slot_jobs + '\n')
    return text.replace(old, new)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    expected = generate()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != expected:
            sys.exit('release-cache.yml is stale; run python3 scripts/configure-release-cache-producer.py')
    else:
        OUTPUT.write_text(expected)


if __name__ == '__main__':
    main()
