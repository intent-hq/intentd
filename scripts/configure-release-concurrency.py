#!/usr/bin/env python3
"""Reapply daemon writer exclusion after cargo dist regenerates v-release.yml."""
from pathlib import Path


BLOCK = """# LOCAL CUSTOMIZATION: hold exclusion from dist planning through alpha/mirror
# publication. The reusable post-announce workflow MUST NOT acquire this lock.
# PR plans do not publish and must not block releases. After `dist generate`, run
# python3 scripts/configure-release-concurrency.py (source of this block).
concurrency:
  group: ${{ github.event_name == 'pull_request' && format('intentd-release-plan-{0}', github.run_id) || 'intentd-release-writers' }}
  cancel-in-progress: false
  queue: max

"""


def configure(text):
    if BLOCK in text:
        return text
    if text.startswith("concurrency:") or "\nconcurrency:" in text or text.count("\njobs:\n") != 1:
        raise ValueError("unexpected generated workflow; review concurrency before replacing it")
    return text.replace("\njobs:\n", "\n" + BLOCK + "jobs:\n", 1)


if __name__ == "__main__":
    path = Path(__file__).resolve().parents[1] / ".github/workflows/v-release.yml"
    path.write_text(configure(path.read_text()))
