#!/usr/bin/env bash
# Run cargo-dist's planned installer command with bounded retries.
#
# The Unix build legs of v-release.yml and release-cache.yml install dist with
# the command `dist plan` puts in matrix.install_dist: `curl ... | sh` (shell
# sh). Windows legs keep cargo-dist's own pwsh step (`irm ... | iex`), not
# retried yet: the installer ends in `exit 1`, so a retry needs a child pwsh
# per attempt. One GitHub 5xx on that download failed the
# leg (the same single-shot pattern as intent-hq/intent#8613). The command
# comes from the plan, so the whole command is retried: four attempts with
# linear backoff. A permanent failure (for example a missing release) fails
# after the last attempt with the installer's own error.
#
# Usage: INSTALL_DIST_RUN=<command> INSTALL_DIST_SHELL=sh install-dist.sh
# INSTALL_DIST_DELAY (seconds, default 10) scales the backoff; tests set 0.
# The download-retry suite (test-download-retry) covers this script.
set -euo pipefail

run=${INSTALL_DIST_RUN:?INSTALL_DIST_RUN must hold the installer command}
delay=${INSTALL_DIST_DELAY:-10}
attempts=4

case "${INSTALL_DIST_SHELL:-}" in
  # bash, not sh: pipefail makes a failed curl fail the pipeline instead of
  # feeding sh an empty script that "succeeds".
  sh) run_installer() { bash -o pipefail -c "$run"; } ;;
  *) echo "::error::install-dist.sh: unsupported INSTALL_DIST_SHELL '${INSTALL_DIST_SHELL:-}' (want sh; Windows uses its own pwsh step)" >&2
     exit 2 ;;
esac

attempt=1
until run_installer; do
  if [ "$attempt" -ge "$attempts" ]; then
    echo "::error::cargo-dist installer failed after $attempts attempts" >&2
    exit 1
  fi
  wait=$((attempt * delay))
  echo "::warning::cargo-dist installer attempt $attempt failed; retrying in ${wait}s" >&2
  sleep "$wait"
  attempt=$((attempt + 1))
done
