#!/usr/bin/env python3
"""Reapply optional Intel publication after cargo-dist generates v-release.yml."""
from pathlib import Path
import re


def replace_once(text, old, new):
    if new in text:
        return text
    if text.count(old) != 1:
        raise ValueError(f'cargo-dist template changed; expected exactly one {old!r}')
    return text.replace(old, new, 1)


def configure(text):
    text = replace_once(text,
        '          echo "dist ran successfully"\n          cat plan-dist-manifest.json',
        '          python3 scripts/optional-intel-release.py matrix plan-dist-manifest.json\n'
        '          echo "dist ran successfully"\n          cat plan-dist-manifest.json')
    runners = re.findall(r'^    runs-on: .*matrix\.runner.*$', text, re.M)
    if len(runners) != 1:
        raise ValueError('cargo-dist template changed; expected one local artifact runner')
    text = replace_once(text, runners[0],
        '    # LOCAL CUSTOMIZATION: only the isolated Intel Mac leg may fail.\n'
        '    continue-on-error: ${{ matrix.optional }}\n' + runners[0])
    text = replace_once(text,
        '          dist build ${{ needs.plan.outputs.tag-flag }} --output-format=json "--artifacts=global" > dist-manifest.json',
        '          targets=$(python3 scripts/optional-intel-release.py targets target/distrib)\n'
        '          dist build ${{ needs.plan.outputs.tag-flag }} $targets --output-format=json "--artifacts=global" > dist-manifest.json')
    text = replace_once(text,
        '          dist host ${{ needs.plan.outputs.tag-flag }} --steps=upload --steps=release --output-format=json > dist-manifest.json',
        '          targets=$(python3 scripts/optional-intel-release.py targets target/distrib)\n'
        '          dist host ${{ needs.plan.outputs.tag-flag }} $targets --steps=upload --steps=release --output-format=json > dist-manifest.json')
    text = replace_once(text,
        '          rm -f artifacts/*-dist-manifest.json',
        '          rm -f artifacts/*-dist-manifest.json\n'
        '          python3 scripts/optional-intel-release.py targets artifacts >/dev/null')
    return text


if __name__ == '__main__':
    path = Path(__file__).resolve().parents[1] / '.github/workflows/v-release.yml'
    path.write_text(configure(path.read_text()))
