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

echo "Running all workspace tests with coverage instrumentation (nextest)..."

# Run all workspace tests (unit + integration + e2e) under nextest for parallelism
# auggie_context_e2e test is env-gated (INTENTD_AUGGIE_E2E) and skips cleanly in CI
# STAB-40, STAB-42, STAB-43, STAB-44 fixed (file sync, multi-threaded runtime, timeout multiplier)
# Note: nextest does not run doctests; the workspace has none, so nothing is lost
INTENTD_TEST_TIMEOUT_MULTIPLIER=3 cargo llvm-cov --no-report nextest --workspace

echo ""
echo "Generating coverage report..."
cargo llvm-cov report --summary-only

# Generate lcov.info if requested (for CI artifact upload)
# Do this BEFORE the floor check so the artifact is available even on failure
if [ "${GENERATE_LCOV:-}" = "1" ]; then
    echo ""
    echo "Generating lcov.info..."
    cargo llvm-cov report --lcov --output-path lcov.info
fi

# If a numeric floor is provided as first argument, enforce it
if [ $# -gt 0 ]; then
    FLOOR="$1"
    echo ""
    echo "Enforcing line coverage floor: ${FLOOR}%"
    cargo llvm-cov report --fail-under-lines "$FLOOR"
fi
