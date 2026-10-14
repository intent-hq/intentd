#!/usr/bin/env bash
# Temporary single-test diagnosis; remove with the passive Rust timing edit.
set -euo pipefail
diagnostic="$GITHUB_WORKSPACE/diagnostic"
component="$GITHUB_WORKSPACE/subject"
contract="$GITHUB_WORKSPACE/contract"
evidence="$GITHUB_WORKSPACE/evidence"
test "$(git -C "$diagnostic" rev-parse HEAD)" = "$DIAGNOSTIC_SHA"
test "$(git -C "$component" rev-parse HEAD)" = "$SOURCE_SHA"
git -C "$component" merge-base --is-ancestor "$PRODUCT_SHA" HEAD
test "$(git -C "$contract" rev-parse HEAD)" = "$ROOT_SHA"
test -z "$(git -C "$component" status --porcelain)"
# The diagnostic history contains its four harness files. Exclude exactly those
# from the product comparison; all compiled-source changes must be the trace edit.
product_paths=(. ':(exclude).github/workflows/startup-callback-diagnostic.yml'
  ':(exclude)scripts/startup-callback-guard.py' ':(exclude)scripts/startup-callback-run.sh'
  ':(exclude)scripts/test-startup-callback-guard.py')
test "$(git -C "$component" diff --name-only "$PRODUCT_SHA" HEAD -- "${product_paths[@]}")" = crates/intent-services/src/agent_manager/repository_origin/callback_delivery/tests.rs
git -C "$component" diff "$PRODUCT_SHA" HEAD -- "${product_paths[@]}" > "$evidence/diagnostic-source.patch"
printf 'diagnostic=%s\nproduct=%s\nsource=%s\ncontract=%s\n' \
  "$DIAGNOSTIC_SHA" "$PRODUCT_SHA" "$SOURCE_SHA" "$ROOT_SHA" > "$evidence/source-shas.txt"
uname -a
df -h "$GITHUB_WORKSPACE"
free -h
rustc -vV
cargo llvm-cov --version
cargo nextest --version
test "$(rustc --version | awk 'NR == 1 { print $2 }')" = 1.96.0
test "$(cargo llvm-cov --version | awk 'NR == 1 { print $2 }')" = 0.9.1
test "$(cargo nextest --version | awk 'NR == 1 { print $2 }')" = 0.9.148
test "$(node --version)" = v24.21.0
sudo -n apt-get -o DPkg::Lock::Timeout=120 update -qq
sudo -n apt-get -o DPkg::Lock::Timeout=120 install -y --no-install-recommends \
  build-essential pkg-config libssl-dev liblzma-dev clang lld cmake protobuf-compiler
fixture=$(python3 -I -B -S "$component/scripts/prepare-acp-callback-fixture.py" \
  --cache-dir "$RUNNER_TEMP/startup-callback" --build-from-source \
  2> "$evidence/callback-build.log")
export INTENT_ACP_CALLBACK_ADAPTER_FIXTURE="$fixture"
offline=$(python3 -I -B -S "$component/scripts/prepare-acp-callback-fixture.py" \
  --cache-dir "$RUNNER_TEMP/startup-callback" --offline \
  2> "$evidence/callback-offline.log")
test "$offline" = "$fixture"
cp "$fixture/FIXTURE-MANIFEST.json" "$evidence/callback-manifest.json"
cp "$component/crates/intent-acp/tests/fixtures/claude-callback-adapter.json" "$evidence/callback-descriptor.json"
export PATH="$CALLBACK_RUNTIME_BIN:$PATH"
test "$(node --version)" = v22.23.1
node --version
export TRANSFER_SELECTION_FIXTURE_ROOT="$contract/docs/protocol/fixtures/transfer-selection"
node "$contract/scripts/check-transfer-selection-contract.mjs"
cd "$component"
export CARGO_LLVM_COV_TARGET_DIR="$component/target/llvm-cov-target"
cargo metadata --locked --format-version 1 > "$evidence/cargo-metadata.json"
printf 'build_jobs=%s\ntest_threads=%s\ntimeout_multiplier=%s\nbound_caller=%s\n' \
  "$CARGO_BUILD_JOBS" "$NEXTEST_TEST_THREADS" "$INTENTD_TEST_TIMEOUT_MULTIPLIER" \
  "$INTENTD_ASSERT_BOUND_CALLER" > "$evidence/test-policy.txt"
test_name=agent_manager::repository_origin::callback_delivery::tests::confirmed_aliases::confirmed_alias_mixed_names_share_one_acquisition_budget
filter="test(=$test_name)"
printf '%s\n' "$test_name" > "$evidence/expected-test.txt"
cp .config/nextest.toml "$evidence/nextest.toml"
# Preserve every existing nextest policy, adding only an absolute JUnit output.
printf '\n[profile.default.junit]\npath = "%s/junit.xml"\nstore-success-output = true\nstore-failure-output = true\n' \
  "$evidence" >> "$evidence/nextest.toml"
(
  coverage_env=$(CARGO_TARGET_DIR="$CARGO_LLVM_COV_TARGET_DIR" cargo llvm-cov show-env --sh)
  eval "$coverage_env"
  cargo nextest list -p intent-services --locked --target-dir "$CARGO_LLVM_COV_TARGET_DIR" \
    --config-file "$evidence/nextest.toml" -E "$filter" --message-format json > "$evidence/listing.json"
)
python3 "$diagnostic/scripts/startup-callback-guard.py" verify-selection "$evidence"
command=(bash scripts/with-test-policy.sh cargo llvm-cov --no-report nextest \
  -p intent-services --locked --config-file "$evidence/nextest.toml" -E "$filter" \
  --retries 0 --no-fail-fast --show-progress none --status-level pass \
  --final-status-level all --success-output immediate --failure-output immediate)
printf '%q ' "${command[@]}" > "$evidence/command.txt"
printf '\n' >> "$evidence/command.txt"
set +e
"${command[@]}" > "$evidence/test.log" 2>&1
test_status=$?
set -e
printf '%s\n' "$test_status" > "$evidence/test-exit-code.txt"
git diff --exit-code
if (( test_status != 0 )); then exit "$test_status"; fi
python3 "$diagnostic/scripts/startup-callback-guard.py" verify-result "$evidence"
