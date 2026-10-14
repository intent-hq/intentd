#!/usr/bin/env bash
# Usage: scripts/docs-ci-scope.sh BASE HEAD
# Python 3.11+ is required for tomllib; failures select full CI.
set -euo pipefail
if ! "${PYTHON:-python3}" -S "$(dirname -- "${BASH_SOURCE[0]}")/docs-ci-scope.py" "$@"; then
  echo 'docs-ci-scope: classifier failed; selecting full CI' >&2
  printf '%s\n' '{"tier":"full","packages":[],"tests":[]}'
fi
