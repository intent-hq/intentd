#!/usr/bin/env bash
set -euo pipefail

# E2E coverage script
# Measures line coverage from ALL daemon-level integration tests in crates/intentd/tests/
# auggie_context_e2e test is env-gated (INTENTD_AUGGIE_E2E) and self-skips when the env var is unset

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

echo "Running e2e tests with coverage instrumentation..."

# Enumerate all test targets dynamically from crates/intentd/tests/*.rs
# Skip auggie_context_e2e (needs real auggie binary)
# Guard against empty glob with nullglob
shopt -s nullglob
test_files=(crates/intentd/tests/*.rs)
if [ ${#test_files[@]} -eq 0 ]; then
    echo "Error: No test files found in crates/intentd/tests/" >&2
    exit 1
fi

for test_file in "${test_files[@]}"; do
    test_name=$(basename "$test_file" .rs)
    if [ "$test_name" = "auggie_context_e2e" ]; then
        echo "Skipping $test_name (requires auggie binary)"
        continue
    fi
    echo "Running test: $test_name"
    # Skip known flaky tests under llvm-cov instrumentation (STAB-40, STAB-42, STAB-43)
    # These skips DEFLATE coverage (we lose their contribution) but prevent spurious CI failures
    cargo llvm-cov --no-report -p intentd --test "$test_name" -- \
        --skip wss_note_save_asset_round_trip \
        --skip slow_host_exec_does_not_block_fast_workspace_list \
        --skip capture_login_shell_path_with_fake_shell
done

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
