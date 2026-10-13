"""Resolve the local native workflow for existing semantic CI regression suites.

Keep the caller's routing/matrix and the callee's actual steps and timeout.
Only the two explicit native call inputs are supported; drift fails closed.
"""
from pathlib import Path
import yaml


def load_ci(root):
    root = Path(root)
    workflow = yaml.safe_load((root / '.github/workflows/ci.yml').read_text())
    caller = workflow['jobs']['daemon-lifecycle']
    if caller.get('uses') != './.github/workflows/native-artifacts.yml':
        raise ValueError('native workflow call changed')
    if caller['with'] != {'platform': '${{ matrix.platform }}', 'runner_labels': '${{ toJSON(matrix.os) }}'}:
        raise ValueError('native workflow inputs changed')
    text = (root / '.github/workflows/native-artifacts.yml').read_text()
    text = text.replace('fromJSON(inputs.runner_labels)', 'matrix.os').replace('inputs.platform', 'matrix.platform')
    callee = yaml.safe_load(text)['jobs']['native']
    workflow['jobs']['daemon-lifecycle'] = {**callee, **caller}
    return workflow
