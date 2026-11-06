#!/usr/bin/env python3
"""Reapply self-hosted-first routing after cargo-dist regenerates v-release.yml."""
from pathlib import Path
import re

ROUTE = '''  runner-route:
    uses: ./.github/workflows/runner-route.yml
    permissions:
      contents: read
      actions: read
    secrets:
      CI_QUEUE_READ_TOKEN: ${{ secrets.CI_QUEUE_READ_TOKEN }}

'''
# cargo-dist's per-target build job (v-release.yml build-local-artifacts and
# its release-cache.yml copy, daemon) runs sudo on Linux (apt configuration and
# cross-build packages), which the unprivileged intent-cache runner containers
# lack, so its Linux legs take their burst runner (`cloud`: WarpBuild, or the
# GitHub label when WarpBuild is off) and never a self-hosted slot.
HOSTED_LINUX_JOBS = {'build-local-artifacts', 'daemon'}


def hosted_linux(key):
    return (f"(contains({key}, 'ubuntu') || contains({key}, 'linux')) && "
            f"(fromJSON(needs.runner-route.outputs.cloud || '{{}}')[{key}] || {key}) || ")


def configure(text):
    prefix, jobs = text.split('\njobs:\n', 1)
    blocks = re.split(r'(?=^  [\w-]+:\n)', jobs, flags=re.M)
    result = []
    for block in blocks:
        match = re.search(r'^    runs-on: (.+)$', block, re.M)
        if match and 'python3' in block and 'uses: actions/setup-python@' not in block:
            steps = re.search(r'^    steps:\n( +)-', block, re.M)
            if steps:
                indent = steps[1]
                setup = f"{indent}- uses: actions/setup-python@v6\n{indent}  with:\n{indent}    python-version: '3.11'\n"
                block = block.replace('    steps:\n', '    steps:\n' + setup, 1)
            match = re.search(r'^    runs-on: (.+)$', block, re.M)
        if not match or 'needs.runner-route.outputs.runners' in match[1]:
            result.append(block)
            continue
        runner = match[1].strip('"\'')
        if runner.startswith('${{'):
            key = runner[3:-2].strip()
            fallback = key
        else:
            key = "'" + runner + "'"
            fallback = "fromJSON('[\"" + runner + "\"]')"
        job = re.match(r'  ([\w-]+):', block)
        hosted = hosted_linux(key) if job and job[1] in HOSTED_LINUX_JOBS and runner.startswith('${{') else ''
        routed = "${{ " + hosted + "fromJSON(needs.runner-route.outputs.runners || '{}')[" + key + "] || " + fallback + " }}"
        block = block[:match.start(1)] + routed + block[match.end(1):]
        needs = re.search(r'^    needs: (\[[^\n]+\]|[\w-]+)$', block, re.M)
        if needs:
            names = needs[1].strip('[]').split(',')
            names = [n.strip() for n in names]
            if 'runner-route' not in names:
                names.append('runner-route')
            block = block[:needs.start(1)] + '[' + ', '.join(names) + ']' + block[needs.end(1):]
        elif (needs_block := re.search(r'^    needs:\n((?:      - [\w-]+\n)+)', block, re.M)):
            block = block[:needs_block.end()] + '      - runner-route\n' + block[needs_block.end():]
        elif re.search(r'^    needs:', block, re.M):
            raise ValueError('cargo-dist needs format changed; inspect routing before proceeding')
        else:
            block = block.replace('    runs-on:', '    needs: [runner-route]\n    runs-on:', 1)
        result.append(block)
    jobs = ''.join(result)
    if not re.search(r'^  runner-route:', jobs, re.M):
        jobs = ROUTE + jobs
    # Nested publisher must retain Actions read permission for its own probe.
    jobs = jobs.replace('    permissions:\n      contents: write\n    uses:',
                        '    permissions:\n      contents: write\n      actions: read\n    uses:')
    return prefix + '\njobs:\n' + jobs


if __name__ == '__main__':
    path = Path(__file__).resolve().parents[1] / '.github/workflows/v-release.yml'
    path.write_text(configure(path.read_text()))
