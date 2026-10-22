#!/usr/bin/env bash
# Bazel workspace status: the build commit embedded in intent-transport.
#
# Mirrors crates/intent-transport/build.rs: INTENTD_BUILD_COMMIT wins when it
# is a 7-64 character hex string, otherwise `git rev-parse HEAD`. Keys with the
# STABLE_ prefix invalidate the stamped actions when they change.
set -euo pipefail

valid_commit() {
  [[ "$1" =~ ^[0-9a-fA-F]{7,64}$ ]]
}

commit="${INTENTD_BUILD_COMMIT:-}"
if ! valid_commit "$commit"; then
  commit="$(git rev-parse --verify HEAD 2>/dev/null || true)"
fi
if valid_commit "$commit"; then
  echo "STABLE_INTENTD_BUILD_COMMIT ${commit}"
fi
