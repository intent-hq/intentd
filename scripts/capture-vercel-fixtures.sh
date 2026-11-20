#!/usr/bin/env bash
# Record the real Vercel Sandbox API into
# crates/intent-nodes/src/vercel/fixtures/vercel.json, the file the fake
# Vercel's contract test replays. Run it by hand, never in CI: it makes one
# persistent sandbox for a minute or two, stops and resumes it, and deletes
# it with its snapshots. Refresh the fixtures when Vercel changes behaviour,
# a live run disagrees with the fake, or intentd starts reading a new
# response field.
#
#   VERCEL_TOKEN_FILE=~/.config/vercel-sandbox/token \
#   VERCEL_TEAM_ID=team_... VERCEL_PROJECT_ID=prj_... \
#   scripts/capture-vercel-fixtures.sh
#
# VERCEL_TOKEN works instead of the file. Review the diff before
# committing: the team, project, session and snapshot ids, times, digests
# and the token are replaced, but read it.
set -euo pipefail

if [ -z "${VERCEL_TOKEN:-}" ] && [ -z "${VERCEL_TOKEN_FILE:-}" ]; then
  echo "set VERCEL_TOKEN (or VERCEL_TOKEN_FILE)" >&2
  exit 2
fi
if [ -z "${VERCEL_TEAM_ID:-}" ] || [ -z "${VERCEL_PROJECT_ID:-}" ]; then
  echo "set VERCEL_TEAM_ID and VERCEL_PROJECT_ID" >&2
  exit 2
fi

# Plain cargo test, not nextest: nextest kills a test after 180 s, before
# the capture's own clean-up could run. Interrupted anyway? The next run
# deletes what this one left before it starts.
cd "$(dirname "$0")/.."
VERCEL_CAPTURE=1 exec cargo test -p intent-nodes --lib -- --exact --nocapture \
  vercel::capture::capture_vercel_fixtures
