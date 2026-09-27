#!/usr/bin/env bash
# Regression tests for ci-lld-retry.sh (intent-hq/intent#5256): drives the
# real wrapper around a fake `cargo` whose per-invocation behaviour is
# scripted through files in a scratch dir, and asserts the retry is exactly
# as narrow as intended — the rust-lld vanished-object signature earns one
# retry with diagnostics, a genuine lint failure fails fast with its own
# exit code, a signature that persists is retried once, not forever, and a
# mixed run (genuine lint alongside the signature) is not retried at all.
#
# Run directly: ./scripts/test-ci-lld-retry.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
script="$here/ci-lld-retry.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/objs"

# Fake cargo: appends one line per call to $CALLS; behaviour for call N is
# read from $PLAN/N (missing = succeed). Each plan file holds an exit code
# on line 1 and the text to print on line 2+.
cat >"$tmp/fake-cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "$*" >>"$CALLS"
n=$(wc -l <"$CALLS")
plan="$PLAN/$n"
if [ -f "$plan" ]; then
  rc=$(sed -n 1p "$plan")
  sed -n '2,$p' "$plan" >&2
  exit "$rc"
fi
echo "    Finished dev profile"
exit 0
EOF
chmod +x "$tmp/fake-cargo"

signature_line="rust-lld: error: cannot open $tmp/objs/intent_core-3f1a.rcgu.o: No such file or directory"
# What rustc and cargo print around a failed link, as in run 35200715354
# (the backticks are rustc's own quoting, meant literally).
# shellcheck disable=SC2016
link_envelope=$(printf '%s\n' \
  'error: linking with `cc` failed: exit status: 1' \
  '  |' \
  '  = note: some arguments are omitted' \
  "  = note: $signature_line" \
  'error: could not compile `intent-core` (lib) due to 1 previous error' \
  'error: aborting due to 1 previous error')
lint_line="error: this looks like a lint (clippy::needless_return)"
compile_error_line=$'\e[1m\e[31merror[E0425]\e[0m\e[1m: cannot find value `nope` in this scope\e[0m'

fail() { echo "FAIL: $*" >&2; exit 1; }
assert_contains() { grep -q -F -- "$2" "$1" || fail "$3: expected output to contain: $2"; }
assert_not_contains() { ! grep -q -F -- "$2" "$1" || fail "$3: expected output NOT to contain: $2"; }
assert_calls() { [ "$(wc -l <"$CALLS")" -eq "$2" ] || fail "$1: expected $2 invocation(s), got $(wc -l <"$CALLS")"; }

# scenario NAME: fresh call log + plan dir; plan files are written between
# `scenario` and `run`.
scenario() {
  export CALLS="$tmp/calls.$1" PLAN="$tmp/plan.$1"
  : >"$CALLS"
  mkdir -p "$PLAN"
}
# run SCENARIO EXPECTED_RC: the real wrapper around the fake cargo.
run() {
  local rc=0
  "$script" "$tmp/fake-cargo" clippy --workspace -- -D warnings >"$tmp/out.$1" 2>&1 || rc=$?
  [ "$rc" -eq "$2" ] || { cat "$tmp/out.$1" >&2; fail "$1: expected exit $2, got $rc"; }
}

echo "scenario 1: clean success runs once"
scenario s1
run s1 0
assert_calls s1 1
assert_not_contains "$tmp/out.s1" "retrying once" s1

echo "scenario 2: the vanished-object signature (with the linker envelope) is retried exactly once and recovers"
scenario s2
printf '1\n%s\n' "$link_envelope" >"$PLAN/1"
touch "$tmp/objs/neighbour.o"
run s2 0
assert_calls s2 2
assert_contains "$tmp/out.s2" "retrying once:" s2
assert_contains "$tmp/out.s2" "rust-lld vanished-object diagnostics" s2
assert_contains "$tmp/out.s2" "missing object: $tmp/objs/intent_core-3f1a.rcgu.o" s2
assert_contains "$tmp/out.s2" "--- ls -la $tmp/objs" s2
assert_contains "$tmp/out.s2" "neighbour.o" s2
assert_contains "$tmp/out.s2" "--- pgrep -af 'cargo|rustc'" s2
assert_contains "$tmp/out.s2" "::warning::rust-lld could not open an object" s2
assert_not_contains "$tmp/out.s2" "::error::" s2

echo "scenario 3: a genuine lint failure fails fast with its exit code, no retry"
scenario s3
printf '101\n%s\n' "$lint_line" >"$PLAN/1"
run s3 101
assert_calls s3 1
assert_contains "$tmp/out.s3" "$lint_line" s3
assert_not_contains "$tmp/out.s3" "retrying once" s3
assert_not_contains "$tmp/out.s3" "diagnostics" s3

echo "scenario 4: a non-lld 'No such file' failure is not retried"
scenario s4
printf '1\n%s\n' "error: couldn't read src/lib.rs: No such file or directory" >"$PLAN/1"
run s4 1
assert_calls s4 1
assert_not_contains "$tmp/out.s4" "retrying once" s4

echo "scenario 5: a persisting signature is retried once, then fails with the retry's exit code"
scenario s5
printf '1\n%s\n' "$signature_line" >"$PLAN/1"
printf '3\n%s\n' "$signature_line" >"$PLAN/2"
run s5 3
assert_calls s5 2
assert_contains "$tmp/out.s5" "::error::retry after the rust-lld vanished-object signature failed too (exit 3)" s5

echo "scenario 6: the signature followed by a genuine lint on retry propagates the lint exit"
scenario s6
printf '1\n%s\n' "$signature_line" >"$PLAN/1"
printf '101\n%s\n' "$lint_line" >"$PLAN/2"
run s6 101
assert_calls s6 2
assert_contains "$tmp/out.s6" "$lint_line" s6

echo "scenario 7: a genuine lint alongside the signature in the same run is not retried; first exit code wins"
scenario s7
printf '101\n%s\n%s\n' "$lint_line" "$link_envelope" >"$PLAN/1"
run s7 101
assert_calls s7 1
assert_contains "$tmp/out.s7" "$lint_line" s7
assert_contains "$tmp/out.s7" "::notice::rust-lld vanished-object signature seen, but the output also carries a genuine rustc/clippy diagnostic" s7
assert_not_contains "$tmp/out.s7" "retrying once" s7
assert_not_contains "$tmp/out.s7" "diagnostics (intent-hq/intent#5256)" s7

echo "scenario 8: a coloured error[E0xxx] compile diagnostic alongside the signature is not retried"
scenario s8
printf '1\n%s\n%s\n' "$compile_error_line" "$link_envelope" >"$PLAN/1"
run s8 1
assert_calls s8 1
assert_not_contains "$tmp/out.s8" "retrying once" s8

echo "scenario 9: a large mixed log (10k lint lines + signature) is still not retried — no SIGPIPE in the diagnostic scan"
scenario s9
{
  echo 101
  for _ in $(seq 10000); do echo "$lint_line"; done
  printf '%s\n' "$link_envelope"
} >"$PLAN/1"
run s9 101
assert_calls s9 1
assert_contains "$tmp/out.s9" "::notice::rust-lld vanished-object signature seen, but the output also carries a genuine rustc/clippy diagnostic" s9
assert_not_contains "$tmp/out.s9" "retrying once" s9

echo "OK: all scenarios passed"
