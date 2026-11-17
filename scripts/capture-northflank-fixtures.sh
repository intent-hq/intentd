#!/usr/bin/env bash
# Record the real Northflank API into
# crates/intent-nodes/src/northflank/fixtures/northflank.json, the file the
# fake Northflank's contract test replays. Run it by hand, never in CI: it
# makes one small service, volume and team tag for a few minutes and deletes
# them all. Refresh the fixtures when Northflank changes behaviour, a live run
# disagrees with the fake, or intentd starts reading a new response field.
#
#   NORTHFLANK_API_TOKEN_FILE=~/.northflank-token NORTHFLANK_PROJECT_ID=... \
#     scripts/capture-northflank-fixtures.sh
#
# NORTHFLANK_API_TOKEN works instead of the file. Review the diff before
# committing: ids, names, times and the token are replaced, but read it.
set -euo pipefail

if [ -z "${NORTHFLANK_PROJECT_ID:-}" ] || { [ -z "${NORTHFLANK_API_TOKEN:-}" ] && [ -z "${NORTHFLANK_API_TOKEN_FILE:-}" ]; }; then
  echo "set NORTHFLANK_PROJECT_ID and NORTHFLANK_API_TOKEN (or NORTHFLANK_API_TOKEN_FILE)" >&2
  exit 2
fi

# Plain cargo test, not nextest: nextest kills a test after 180 s, before
# the capture's own clean-up could run. Interrupted anyway? The next run
# deletes what this one left before it starts.
cd "$(dirname "$0")/.."
NORTHFLANK_CAPTURE=1 exec cargo test -p intent-nodes --lib -- --exact --nocapture \
  northflank::capture::capture_northflank_fixtures
