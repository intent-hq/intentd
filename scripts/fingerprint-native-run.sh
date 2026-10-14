#!/usr/bin/env bash
# Temporary full gate after run 38486968362 passed all 13 native triage targets.
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
mkdir -p "$RUNNER_TEMP/fingerprint-nextest"
curl --fail --silent --show-error --location --max-time 120 \
  https://get.nexte.st/0.9.143/linux -o "$RUNNER_TEMP/fingerprint-nextest.tar.gz"
tar -xzf "$RUNNER_TEMP/fingerprint-nextest.tar.gz" -C "$RUNNER_TEMP/fingerprint-nextest"
export PATH="$RUNNER_TEMP/fingerprint-nextest:$PATH"
cargo nextest --version
test "$(cargo nextest --version | awk 'NR == 1 { print $2 }')" = 0.9.143
# Hosted Linux CI preinstalls pwsh; fail clearly rather than let its tests self-skip.
export XDG_CACHE_HOME="$RUNNER_TEMP/fingerprint-xdg-cache"
mkdir -p "$XDG_CACHE_HOME"
command -v pwsh
pwsh --version
test "$(pwsh -NoProfile -NonInteractive -Command 'Write-Output pwsh-ok')" = pwsh-ok
# Match the repository's full Linux CI watcher prerequisite on this isolated runner.
sudo -n sysctl -q -w fs.inotify.max_user_instances=4096
fixture=$(python3 -I -B -S "$component/scripts/prepare-acp-callback-fixture.py" \
  --cache-dir "$RUNNER_TEMP/fingerprint-callback" --build-from-source \
  2> "$evidence/callback-build.log")
export INTENT_ACP_CALLBACK_ADAPTER_FIXTURE="$fixture"
offline=$(python3 -I -B -S "$component/scripts/prepare-acp-callback-fixture.py" \
  --cache-dir "$RUNNER_TEMP/fingerprint-callback" --offline \
  2> "$evidence/callback-offline.log")
test "$offline" = "$fixture"
cd "$subject"
make check-callback-fixture check-transfer-fixture
printf '%s\n' 'make test BUILD_JOBS=2 TEST_THREADS=2 NO_FAIL_FAST=1 COMPACT=1' > "$evidence/selection.txt"
export GATE_CACHE_DIR="$evidence/gate-runs"
set +e
make test BUILD_JOBS=2 TEST_THREADS=2 NO_FAIL_FAST=1 COMPACT=1
test_status=$?
set -e
printf '%s\n' "$test_status" > "$evidence/test-exit-code.txt"
if [ "$test_status" -eq 0 ]; then
  python3 "$diagnostic/scripts/fingerprint-native-guard.py" verify-full "$evidence"
fi
git -C "$component" diff --exit-code
exit "$test_status"
