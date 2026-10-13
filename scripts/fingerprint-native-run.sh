#!/usr/bin/env bash
# Temporary focused native triage only. Full suite/base comparison need evidence review.
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
plan=$(python3 "$diagnostic/scripts/fingerprint-native-guard.py" plan \
  "$diagnostic/scripts/fingerprint-native-targets.json")
printf '%s\n' "$plan" > "$evidence/selection.txt"
set +e
bash "$component/scripts/with-test-policy.sh" python3 scripts/resumable_nextest.py \
  --repo-root "$subject" --intentd-dir "$component" \
  --cache-dir "$evidence/gate-runs" --resume 0 --force 1 \
  --no-fail-fast 1 --build-jobs 2 --test-threads 2 --plan "$plan"
test_status=$?
set -e
printf '%s\n' "$test_status" > "$evidence/test-exit-code.txt"
python3 "$diagnostic/scripts/fingerprint-native-guard.py" verify \
  "$diagnostic/scripts/fingerprint-native-targets.json" "$evidence"
git -C "$component" diff --exit-code
exit "$test_status"
