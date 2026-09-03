#!/usr/bin/env bash
set -euo pipefail

# Combined coverage script
# Measures line coverage from ALL workspace tests (unit + integration + e2e)
# auggie_context_e2e test is env-gated (INTENTD_AUGGIE_E2E) and self-skips when the env var is unset

cd "$(dirname "$0")/.."

echo "Installing cargo-llvm-cov, cargo-nextest and llvm-tools..."
if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
    cargo install cargo-llvm-cov --locked
fi
if ! command -v cargo-nextest >/dev/null 2>&1; then
    cargo install cargo-nextest --locked
fi
if ! rustup component list --installed | grep -q llvm-tools; then
    rustup component add llvm-tools-preview
fi

echo "Cleaning coverage data..."
cargo llvm-cov clean --workspace
rm -f coverage-summary.txt

echo "Running all workspace tests with coverage instrumentation (nextest)..."

# Run all workspace tests (unit + integration + e2e) under nextest for parallelism
# auggie_context_e2e test is env-gated (INTENTD_AUGGIE_E2E) and skips cleanly in CI
# STAB-40, STAB-42, STAB-43, STAB-44 fixed (file sync, multi-threaded runtime, timeout multiplier)
# Note: nextest does not run doctests; the workspace has none, so nothing is lost
INTENTD_TEST_TIMEOUT_MULTIPLIER=3 cargo llvm-cov --no-report nextest --workspace

# Generate lcov.info if requested (for CI artifact upload)
# Do this BEFORE the floor check so the artifact is available even on failure
if [ "${GENERATE_LCOV:-}" = "1" ]; then
    echo ""
    echo "Generating lcov.info..."
    cargo llvm-cov report --lcov --output-path lcov.info
fi

# One report invocation prints the summary AND enforces the floor (if a numeric
# floor is provided as first argument): every `cargo llvm-cov report` re-merges
# the whole workspace's profdata (~30s in CI), so summary, floor check and the
# CI step summary used to cost three passes (intent-hq/monorepo#4260). The
# summary is tee'd to coverage-summary.txt for CI to reuse; `set -o pipefail`
# keeps a floor failure fatal through the pipe (cargo-llvm-cov exits 1 without
# a message on a floor miss, hence the explicit diagnostic).
REPORT_ARGS=(--summary-only)
FLOOR=""
echo ""
if [ $# -gt 0 ]; then
    FLOOR="$1"
    echo "Generating coverage report and enforcing line coverage floor: ${FLOOR}%"
    REPORT_ARGS+=(--fail-under-lines "$FLOOR")
else
    echo "Generating coverage report..."
fi
if ! cargo llvm-cov report "${REPORT_ARGS[@]}" | tee coverage-summary.txt; then
    if [ -n "$FLOOR" ]; then
        echo "ERROR: line coverage is below the ${FLOOR}% floor (see TOTAL above)" >&2
    fi
    exit 1
fi
