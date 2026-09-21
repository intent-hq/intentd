#!/usr/bin/env bash
# Narrow retry wrapper for cargo invocations on the tinybox runners
# (intent-hq/intent#5256). Merge-queue run 35200715354 failed `check` in 6 s
# because rust-lld could not open object files rustc had just written under
# the job's own CARGO_TARGET_DIR:
#
#   rust-lld: error: cannot open /raid/ci-target/intentd/tinybox/check/debug/
#     deps/<crate>.<hash>.rcgu.o: No such file or directory
#
# Concurrent jobs, the in-job preflight and the scheduled prune were ruled
# out; the remaining candidates are host-level. Until the host side is
# understood, this wrapper keeps that one signature from ejecting a queue
# entry: it runs the command once and, ONLY when the combined output matches
# `rust-lld: error: cannot open .* No such file or directory`, prints host
# diagnostics (listing of the vanished object's directory, live cargo/rustc
# processes, disk state) and retries exactly once. Any other failure — a
# genuine lint, a compile error, a different linker error — propagates
# immediately with the original exit code, so the wrapper never hides a real
# red. That includes a mixed run: a workspace Clippy pass can report a lint
# in one crate alongside the vanished-object error in another, and the lint
# alone decides — any rustc/clippy `error:` diagnostic in the output other
# than the linker-failure envelope (`linking with … failed`, `could not
# compile`, `aborting due to`) makes the failure genuine, no retry.
#
# Usage: ci-lld-retry.sh CMD [ARGS...]
# The command's output is streamed unchanged (stderr merged into stdout, as
# Actions renders them anyway) and captured for the signature check.
set -euo pipefail

[ $# -ge 1 ] || { echo "usage: $0 CMD [ARGS...]" >&2; exit 2; }

SIGNATURE='rust-lld: error: cannot open .* No such file or directory'
# rustc / clippy diagnostics start the line with `error:` or `error[E0xxx]:`;
# the rust-lld line does not (`rust-lld: error:`). The linker-failure
# envelope rustc and cargo print around a failed link is the only `error:`
# the retried path may contain.
DIAGNOSTIC='^error(\[E[0-9]+\])?: '
LINK_ENVELOPE='^error: (linking with .* failed|could not compile |aborting due to )'

log=$(mktemp)
trap 'rm -f "$log"' EXIT

# A genuine diagnostic in the captured output — colour codes stripped, since
# cargo may emit them under CARGO_TERM_COLOR=always. One awk pass that reads
# to EOF: a `grep -q` at the end of a pipeline exits early and SIGPIPEs the
# upstream stages, which under pipefail turns a large mixed log into 141.
has_genuine_diagnostic() {
  awk -v diag="$DIAGNOSTIC" -v envelope="$LINK_ENVELOPE" '
    { gsub(/\033\[[0-9;]*m/, "") }
    $0 ~ diag && $0 !~ envelope { found = 1 }
    END { exit found ? 0 : 1 }
  ' "$log"
}

run_once() {
  local rc
  : >"$log"
  # The command's own status, not tee's: read PIPESTATUS right after the
  # pipeline, with -e off so a red command does not abort the wrapper.
  set +e
  "$@" 2>&1 | tee "$log"
  rc="${PIPESTATUS[0]}"
  set -e
  return "$rc"
}

diagnostics() {
  echo "::group::rust-lld vanished-object diagnostics (intent-hq/intent#5256)"
  local line path dir
  line=$(grep -E -m1 "$SIGNATURE" "$log" || true)
  echo "signature line: $line"
  path=$(printf '%s\n' "$line" | sed -nE 's/.*rust-lld: error: cannot open (.*): No such file or directory.*/\1/p')
  if [ -n "$path" ]; then
    dir=$(dirname -- "$path")
    echo "missing object: $path"
    echo "--- ls -la $dir"
    ls -la -- "$dir" 2>&1 || true
    echo "--- stat $path"
    stat -- "$path" 2>&1 || true
  else
    echo "could not extract the missing object's path from the signature line"
  fi
  echo "--- pgrep -af 'cargo|rustc'"
  pgrep -af 'cargo|rustc' 2>&1 || echo "(no cargo/rustc processes)"
  if [ -n "${CARGO_TARGET_DIR:-}" ]; then
    echo "--- CARGO_TARGET_DIR=$CARGO_TARGET_DIR"
    ls -la -- "$CARGO_TARGET_DIR" 2>&1 || true
    df -h -- "$CARGO_TARGET_DIR" 2>&1 || true
  fi
  echo "--- uptime / dmesg tail (best effort)"
  uptime 2>&1 || true
  dmesg 2>/dev/null | tail -n 20 || true
  echo "::endgroup::"
}

rc=0
run_once "$@" || rc=$?
[ "$rc" -eq 0 ] && exit 0

if ! grep -qE "$SIGNATURE" "$log"; then
  # Not the vanished-object signature: a real failure, fail fast.
  exit "$rc"
fi
if has_genuine_diagnostic; then
  # The signature alongside a real lint / compile error: the real error
  # decides, and a retry would only hide it behind a second run.
  echo "::notice::rust-lld vanished-object signature seen, but the output also carries a genuine rustc/clippy diagnostic — not retrying (intent-hq/intent#5256)"
  exit "$rc"
fi

echo "::warning::rust-lld could not open an object rustc had just written (intent-hq/intent#5256); collecting diagnostics and retrying once"
diagnostics
echo "retrying once: $*"
rc=0
run_once "$@" || rc=$?
if [ "$rc" -ne 0 ]; then
  echo "::error::retry after the rust-lld vanished-object signature failed too (exit $rc)"
fi
exit "$rc"
