#!/usr/bin/env bash
# Run `cargo llvm-cov report "$@"` after the instrumented tests, keeping
# coverage-tooling faults apart from test results.
#
# A process killed while it writes its profile at exit (a test daemon
# SIGKILLed mid-dump, or one still exiting when the merge starts) leaves a
# .profraw with a truncated header. llvm-profdata's default failure mode then
# rejects the whole merge with "no profile can be merged", which ejected a
# merge-queue run whose 2101 tests had all passed (intentd run 42133300480).
# --failure-mode=all drops the unreadable profiles with a warning and fails
# only when none can be merged; this script names the dropped files and turns
# the empty and all-unreadable cases into an explicit tooling error.
#
# Exit status: cargo-llvm-cov's own (1 on a floor miss), or 3 for a coverage
# tooling fault (no profiles, or none readable).
set -euo pipefail

# Runs from the Cargo workspace root, like the coverage scripts that call it.
CARGO=${CARGO:-cargo}
target_dir=${CARGO_TARGET_DIR:-target}
profile_dir=${CARGO_LLVM_COV_TARGET_DIR:-$target_dir/llvm-cov-target}

annotate() {
    local level=$1 message=$2
    if [ -n "${GITHUB_ACTIONS:-}" ]; then
        echo "::$level title=Coverage tooling (not a test failure)::$message" >&2
    fi
}

tooling_error() {
    {
        echo ""
        echo "ERROR: coverage tooling problem, not a test failure: $1"
        echo "  Profile directory: $profile_dir"
        echo "  The test results above stand; re-run the job to collect coverage again."
    } >&2
    annotate error "$1 Re-run the job; the test results stand."
    exit 3
}

profiles=0
if [ -d "$profile_dir" ]; then
    profiles=$(find "$profile_dir" -maxdepth 1 -name '*.profraw' -type f | wc -l)
fi
if [ "$profiles" -eq 0 ]; then
    tooling_error "the instrumented test run wrote no .profraw files, so there is no coverage data to merge."
fi

stderr_log=$(mktemp)
trap 'rm -f "$stderr_log"' EXIT
status=0
"$CARGO" llvm-cov report --failure-mode=all "$@" 2>"$stderr_log" || status=$?
cat "$stderr_log" >&2

dropped=$(grep -cE '^warning: .*\.profraw: ' "$stderr_log" || true)
if [ "$status" -ne 0 ] && grep -q 'no profile can be merged' "$stderr_log"; then
    tooling_error "none of the $profiles .profraw files could be read (llvm-profdata: no profile can be merged)."
fi
if [ "$dropped" -gt 0 ]; then
    message="dropped $dropped of $profiles .profraw files that llvm-profdata could not read (truncated by a process killed while writing its profile); coverage is computed from the rest."
    echo "WARNING: $message" >&2
    annotate warning "$message"
fi
exit "$status"
