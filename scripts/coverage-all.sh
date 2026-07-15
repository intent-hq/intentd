#!/usr/bin/env bash
set -euo pipefail

# Combined coverage script
# Measures line coverage from ALL workspace tests (unit + integration + e2e)
# Excludes auggie_context_e2e (requires real auggie binary)

cd "$(dirname "$0")/.."

echo "Installing cargo-llvm-cov and llvm-tools..."
if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
    cargo install cargo-llvm-cov --locked
fi
if ! rustup component list --installed | grep -q llvm-tools; then
    rustup component add llvm-tools-preview
fi

echo "Cleaning coverage data..."
cargo llvm-cov clean --workspace

echo "Running all workspace tests with coverage instrumentation..."

# Run all workspace tests (unit + integration + e2e)
# auggie_context_e2e test is env-gated (INTENTD_AUGGIE_E2E) and skips cleanly in CI
# Skip known flaky tests under llvm-cov instrumentation (STAB-40, STAB-42)
# These skips DEFLATE coverage (we lose their contribution) but prevent spurious CI failures
cargo llvm-cov --no-report --workspace -- \
    --skip wss_note_save_asset_round_trip \
    --skip slow_host_exec_does_not_block_fast_workspace_list

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

# If --fail-under-lines is provided as first argument, enforce the floor
if [ $# -gt 0 ]; then
    FLOOR="$1"
    echo ""
    echo "Enforcing line coverage floor: ${FLOOR}%"
    cargo llvm-cov report --fail-under-lines "$FLOOR"
fi
