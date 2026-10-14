#!/usr/bin/env bash
# Temporary startup full validation; remove before product PR.
set -euo pipefail
diagnostic="$GITHUB_WORKSPACE/diagnostic"
subject="$GITHUB_WORKSPACE/subject"
evidence="$GITHUB_WORKSPACE/evidence"
component="$subject/packages/intentd"
test "$(git -C "$diagnostic" rev-parse HEAD)" = "$DIAGNOSTIC_SHA"
test "$(git -C "$subject" rev-parse HEAD)" = "$ROOT_SHA"
test "$(git -C "$component" rev-parse HEAD)" = "$SOURCE_SHA"
test -z "$(git -C "$component" status --porcelain)"
printf 'diagnostic=%s\nroot=%s\nsource=%s\n' "$DIAGNOSTIC_SHA" "$ROOT_SHA" "$SOURCE_SHA" > "$evidence/source-shas.txt"
test "$(git -C "$subject/packages/cloudlands-fe" rev-parse HEAD)" = "$FE_SHA"
printf 'frontend=%s\n' "$FE_SHA" >> "$evidence/source-shas.txt"
uname -a
df -h "$GITHUB_WORKSPACE"
free -h
rustc -vV
node --version
npm --version
test "$(rustc --version | cut -d ' ' -f 2)" = 1.96.0
test "$(node --version)" = v24.21.0

# Same public prerequisites as the successful disposable-container build.
sudo -n apt-get -o DPkg::Lock::Timeout=120 update -qq
sudo -n apt-get -o DPkg::Lock::Timeout=120 install -y --no-install-recommends \
  build-essential pkg-config libssl-dev liblzma-dev clang lld cmake \
  protobuf-compiler jq xz-utils file ripgrep gh shellcheck
mkdir -p "$RUNNER_TEMP/startup-nextest"
curl --fail --silent --show-error --location --max-time 120 \
  https://get.nexte.st/0.9.143/linux -o "$RUNNER_TEMP/startup-nextest.tar.gz"
tar -xzf "$RUNNER_TEMP/startup-nextest.tar.gz" -C "$RUNNER_TEMP/startup-nextest"
export PATH="$RUNNER_TEMP/startup-nextest:$PATH"
cargo nextest --version
test "$(cargo nextest --version | awk 'NR == 1 { print $2 }')" = 0.9.143
# Hosted Linux CI preinstalls pwsh; fail clearly rather than let its tests self-skip.
export XDG_CACHE_HOME="$RUNNER_TEMP/startup-xdg-cache"
mkdir -p "$XDG_CACHE_HOME"
command -v pwsh
pwsh --version
test "$(pwsh -NoProfile -NonInteractive -Command 'Write-Output pwsh-ok')" = pwsh-ok
# Match the repository's full Linux CI watcher prerequisite on this isolated runner.
sudo -n sysctl -q -w fs.inotify.max_user_instances=4096
fixture=$(python3 -I -B -S "$component/scripts/prepare-acp-callback-fixture.py" \
  --cache-dir "$RUNNER_TEMP/startup-callback" --build-from-source \
  2> "$evidence/callback-build.log")
export INTENT_ACP_CALLBACK_ADAPTER_FIXTURE="$fixture"
offline=$(python3 -I -B -S "$component/scripts/prepare-acp-callback-fixture.py" \
  --cache-dir "$RUNNER_TEMP/startup-callback" --offline \
  2> "$evidence/callback-offline.log")
test "$offline" = "$fixture"
cd "$subject"
make check-callback-fixture check-transfer-fixture
export GATE_CACHE_DIR="$evidence/gate-runs"
printf '%s\n' 'make check BUILD_JOBS=1 CHECK_MAKEFILE_TARGETS_GITLINK=HEAD' 'make test COMPACT=1 BUILD_JOBS=1 TEST_THREADS=2 NO_FAIL_FAST=1 RESUME=0 GATE_FORCE=1' > "$evidence/selection.txt"
set +e
make check BUILD_JOBS=1 CHECK_MAKEFILE_TARGETS_GITLINK=HEAD > "$evidence/check.log" 2>&1
check_status=$?
printf '%s\n' "$check_status" > "$evidence/check-exit-code.txt"
make test COMPACT=1 BUILD_JOBS=1 TEST_THREADS=2 NO_FAIL_FAST=1 RESUME=0 GATE_FORCE=1 > "$evidence/test.log" 2>&1
test_status=$?
printf '%s\n' "$test_status" > "$evidence/test-exit-code.txt"
set -e
if (( test_status == 0 )); then
  python3 "$diagnostic/scripts/startup-native-guard.py" verify-full "$evidence"
fi
git -C "$component" diff --exit-code
if (( check_status != 0 )); then exit "$check_status"; fi
exit "$test_status"
