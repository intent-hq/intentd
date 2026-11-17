#!/usr/bin/env bash
# Record the real Agent37 API into
# crates/intent-nodes/src/agent37/fixtures/agent37.json, the file the fake
# Agent37's contract test replays. Run it by hand, never in CI: it imports
# one template and makes one default and one vm instance for a few minutes,
# and deletes them all. Refresh the fixtures when Agent37 changes behaviour,
# a live run disagrees with the fake, or intentd starts reading a new
# response field.
#
#   AGENT37_API_KEY_FILE=~/.config/agent37/api_key scripts/capture-agent37-fixtures.sh
#
# AGENT37_API_KEY works instead of the file. Review the diff before
# committing: instance ids, times, digests and the key are replaced, but
# read it.
set -euo pipefail

if [ -z "${AGENT37_API_KEY:-}" ] && [ -z "${AGENT37_API_KEY_FILE:-}" ]; then
  echo "set AGENT37_API_KEY (or AGENT37_API_KEY_FILE)" >&2
  exit 2
fi

# Plain cargo test, not nextest: nextest kills a test after 180 s, before
# the capture's own clean-up could run. Interrupted anyway? The next run
# deletes what this one left before it starts.
cd "$(dirname "$0")/.."
AGENT37_CAPTURE=1 exec cargo test -p intent-nodes --lib -- --exact --nocapture \
  agent37::capture::capture_agent37_fixtures
