#!/usr/bin/env bash
# Test launch boundary shared by component and monorepo gates. Invoke as:
#   bash scripts/with-test-policy.sh <command> [args...]
# Override inherited opt-outs only for this process tree; exact-child unbound
# probes can still remove the variable inside a test. Never source this file
# or wrap production daemon launches. exec preserves argv, I/O and exit status.
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 <command> [args...]" >&2
  exit 2
fi
export INTENTD_ASSERT_BOUND_CALLER=1
exec "$@"
