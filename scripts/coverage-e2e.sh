#!/usr/bin/env bash
set -euo pipefail

# E2E coverage script
# Measures line coverage from ALL daemon-level integration tests in crates/intentd/tests/
# Excludes auggie_context_e2e (requires real auggie binary)

cd "$(dirname "$0")/.."

echo "Installing cargo-llvm-cov and llvm-tools..."
if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
    cargo install cargo-llvm-cov --locked
fi
rustup component add llvm-tools-preview

echo "Cleaning coverage data..."
cargo llvm-cov clean --workspace

echo "Running e2e tests with coverage instrumentation..."

# Enumerate all test targets dynamically from crates/intentd/tests/*.rs
# Skip auggie_context_e2e (needs real auggie binary)
for test_file in crates/intentd/tests/*.rs; do
    test_name=$(basename "$test_file" .rs)
    if [ "$test_name" = "auggie_context_e2e" ]; then
        echo "Skipping $test_name (requires auggie binary)"
        continue
    fi
    echo "Running test: $test_name"
    cargo llvm-cov --no-report -p intentd --test "$test_name"
done

echo ""
echo "Generating coverage report..."
cargo llvm-cov report --summary-only

# If --fail-under-lines is provided as first argument, enforce the floor
if [ $# -gt 0 ]; then
    FLOOR="$1"
    echo ""
    echo "Enforcing line coverage floor: ${FLOOR}%"
    cargo llvm-cov report --fail-under-lines "$FLOOR"
fi

# Generate lcov.info if requested (for CI artifact upload)
if [ "${GENERATE_LCOV:-}" = "1" ]; then
    echo ""
    echo "Generating lcov.info..."
    cargo llvm-cov report --lcov --output-path lcov.info
fi
