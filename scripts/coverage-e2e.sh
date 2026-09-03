#!/usr/bin/env bash
set -euo pipefail

# E2E coverage script
# Measures line coverage from ALL daemon-level integration tests in crates/intentd/tests/
# Explicitly skips auggie_context_e2e via the nextest filterset below (requires real auggie binary)

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

echo "Running e2e tests with coverage instrumentation (nextest)..."

# Run intentd's integration-test binaries (crates/intentd/tests/*)
# under nextest so the test binaries execute in parallel instead of one at a time.
# Skip auggie_context_e2e (needs real auggie binary)
# STAB-40, STAB-42, STAB-44 fixed (monitoring-only, multi-threaded runtime, timeout multiplier)
# Note: capture_login_shell_path_with_fake_shell (STAB-43) is an intent-core unit test,
# not an intentd integration test, so it runs in coverage-all.sh but not here
INTENTD_TEST_TIMEOUT_MULTIPLIER=3 cargo llvm-cov --no-report nextest -p intentd \
    -E 'kind(test) and not binary(intentd) and not binary(auggie_context_e2e)'

# Generate lcov.info if requested (for CI artifact upload)
# Do this BEFORE the floor check so the artifact is available even on failure
if [ "${GENERATE_LCOV:-}" = "1" ]; then
    echo ""
    echo "Generating lcov.info..."
    cargo llvm-cov report --lcov --output-path lcov.info
fi

# One report invocation prints the summary AND enforces the floor (if a numeric
# floor is provided as first argument) — same consolidation as coverage-all.sh
# (intent-hq/monorepo#4260). The summary is tee'd to coverage-summary.txt for
# CI to reuse; `set -o pipefail` keeps a floor failure fatal through the pipe
# (cargo-llvm-cov exits 1 without a message on a floor miss, hence the explicit
# diagnostic).
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
