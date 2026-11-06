#!/usr/bin/env bash
# Regression tests for ci-killed-rustc-retry.sh: drives the real wrapper
# around a fake `cargo` whose per-invocation output is scripted through
# files in a scratch dir. The killed-compiler fixtures are trimmed copies of
# the real CI logs (runs 41190030739, 40293024372, 40496877837), so the
# retry stays exactly as narrow as intended: a compiler killed with no
# diagnostic earns one retry with evidence and a job-summary entry; a real
# compile error, a mixed run, a build-script failure or a failure after
# tests started fails fast with its own exit code; a kill that repeats is
# retried once, not forever; outside GitHub Actions the command runs as is.
#
# Run directly: ./scripts/test-ci-killed-rustc-retry.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
script="$here/ci-killed-rustc-retry.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin" "$tmp/cgroup"

# Host-wide state is stubbed so the output is fixed text: the kernel log,
# memory, and sccache's stats.
cat >"$tmp/bin/dmesg" <<'STUB'
#!/usr/bin/env bash
echo "[1234.5] Memory cgroup out of memory: Killed process 4242 (rustc)"
echo "[1234.6] unrelated kernel line"
STUB
cat >"$tmp/bin/free" <<'STUB'
#!/usr/bin/env bash
echo "stub free -m"
STUB
cat >"$tmp/bin/sccache" <<'STUB'
#!/usr/bin/env bash
echo "stub sccache $*"
STUB
chmod +x "$tmp/bin/dmesg" "$tmp/bin/free" "$tmp/bin/sccache"
export PATH="$tmp/bin:$PATH" KILLED_RUSTC_CGROUP="$tmp/cgroup" RUSTC_WRAPPER=sccache
export GITHUB_ACTIONS=true CARGO_BUILD_JOBS=8
printf '%s\n' 53687091200 >"$tmp/cgroup/memory.max"
printf '%s\n' 53687091200 >"$tmp/cgroup/memory.peak"

# Fake cargo: appends one line per call to $CALLS (its CARGO_BUILD_JOBS and
# argv); behaviour for call N is read from $PLAN/N (missing = succeed). A
# plan file holds an exit code on line 1 and the output on line 2+. A file
# N.oom bumps the fake cgroup's oom and oom_kill counters (the job hit its
# own limit); N.hostoom bumps only oom_kill (a limit above the job did).
cat >"$tmp/fake-cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "jobs=${CARGO_BUILD_JOBS:-unset} $*" >>"$CALLS"
n=$(wc -l <"$CALLS")
if [ -f "$PLAN/$n.oom" ] || [ -f "$PLAN/$n.hostoom" ]; then
  read -r oom kills < <(awk '$1 == "oom" { o = $2 } $1 == "oom_kill" { k = $2 } END { print o, k }' "$KILLED_RUSTC_CGROUP/memory.events")
  [ -f "$PLAN/$n.hostoom" ] || oom=$((oom + 1))
  printf 'low 0\noom %s\noom_kill %s\n' "$oom" "$((kills + 1))" >"$KILLED_RUSTC_CGROUP/memory.events"
fi
plan="$PLAN/$n"
if [ -f "$plan" ]; then
  rc=$(sed -n 1p "$plan")
  sed -n '2,$p' "$plan" >&2
  exit "$rc"
fi
echo "    Finished test profile"
echo "     Starting 12 tests across 3 binaries"
exit 0
EOF
chmod +x "$tmp/fake-cargo"

# The backticks below are cargo's own quoting, meant literally.
# shellcheck disable=SC2016
{
rustc='/home/runner/.rustup/toolchains/1.96.0-x86_64-unknown-linux-gnu/bin/rustc --crate-name intent_services --edition=2021 crates/intent-services/src/lib.rs --error-format=json --test -C instrument-coverage --cfg=coverage'
# Run 41190030739 (coverage-all, intent-ci2-xl-1): sccache's exit 2, the
# signal line lost to nextest, nothing else.
coverage_all_kill=$(printf '%s\n' \
  '   Compiling intent-app v0.11.90 (/home/runner/work/intentd/intentd/crates/intent-app)' \
  "error: process didn't exit successfully: \`sccache $rustc\` (exit status: 2)" \
  $'\e[1m\e[31merror\e[0m\e[1m:\e[0m could not compile `intent-services` (lib test)' \
  '' 'Caused by:' \
  "  process didn't exit successfully: \`/home/runner/.cargo/bin/cargo-llvm-cov $rustc\` (exit status: 2)" \
  'warning: build failed, waiting for other jobs to finish...' \
  "error: command \`/home/runner/.rustup/toolchains/1.96.0-x86_64-unknown-linux-gnu/bin/cargo '--color=always' test --no-run --message-format json-render-diagnostics --workspace\` exited with code 101" \
  "error: process didn't exit successfully: \`/home/runner/.rustup/toolchains/1.96.0-x86_64-unknown-linux-gnu/bin/cargo nextest run --workspace\` (exit status: 101)")
# Run 40293024372 (check, intent-ci3-heavy-3): plain cargo test, so the
# sccache signal line survives.
check_kill=$(printf '%s\n' \
  '   Compiling intent-services v0.11.60 (/home/runner/work/intentd/intentd/crates/intent-services)' \
  'sccache: Compile terminated by signal 9' \
  'error: could not compile `intent-services` (lib test)' \
  '' 'Caused by:' \
  "  process didn't exit successfully: \`sccache $rustc\` (exit status: 2)")
# Run 40496877837 (coverage-changed, WarpBuild 16x): rustc's own banner.
segv_kill=$(printf '%s\n' \
  'error: rustc interrupted by SIGSEGV, printing backtrace' '' \
  '/home/runner/.rustup/toolchains/1.96.0-x86_64-unknown-linux-gnu/lib/libLLVM.so.22.1-rust-1.96.0-stable(_ZNK4llvm6MCExpr22findAssociatedFragmentEv+0xb)[0x7f5c1977dbcb]' \
  'note: we would appreciate a report at https://github.com/rust-lang/rust' \
  "help: you can increase rustc's stack size by setting RUST_MIN_STACK=16777216" \
  "error: process didn't exit successfully: \`sccache $rustc\` (exit status: 2)" \
  'error: could not compile `intent-services` (lib test)' \
  "error: command \`cargo test --no-run --message-format json-render-diagnostics --package intent-services\` exited with code 101")
# No sccache configured: cargo names the signal itself.
plain_kill=$(printf '%s\n' \
  'error: could not compile `intent-services` (lib test)' '' 'Caused by:' \
  "  process didn't exit successfully: \`$rustc\` (signal: 9, SIGKILL: kill)")
compile_error=$(printf '%s\n' \
  $'\e[1m\e[31merror[E0425]\e[0m\e[1m: cannot find value `nope` in this scope\e[0m' \
  'error: could not compile `intent-services` (lib test) due to 1 previous error')
# A genuine error in one crate while another crate's compiler was killed.
mixed=$(printf '%s\n%s\n' "$coverage_all_kill" "error[E0308]: mismatched types")
# A build script that exits 2: its path contains `rustc`, but it is not
# the compiler.
build_script=$(printf '%s\n' \
  'error: could not compile `intent-store` (build script)' '' 'Caused by:' \
  "  process didn't exit successfully: \`/home/runner/work/intentd/intentd/target/debug/build/rustc_version-1a2b/build-script-build\` (exit status: 2)")
# sccache's server path (a cacheable crate) exits 254; under nextest its
# `sccache: Compiler killed by signal 9` stdout line is lost, as above.
server_kill=$(printf '%s\n' \
  'error: could not compile `intent-store` (lib)' '' 'Caused by:' \
  "  process didn't exit successfully: \`sccache $rustc\` (exit status: 254)")
# rustc's banner alone decides when a wrapper rewrote the exit status.
segv_rewritten=$(printf '%s\n' \
  'error: rustc interrupted by SIGSEGV, printing backtrace' \
  'error: could not compile `intent-services` (lib test)' '' 'Caused by:' \
  "  process didn't exit successfully: \`/usr/local/bin/some-wrapper $rustc\` (exit status: 1)")
# A compile killed after an earlier test binary already ran.
after_tests=$(printf '%s\n%s\n' '     Starting 40 tests across 2 binaries' "$coverage_all_kill")
}

fail() { echo "FAIL: $*" >&2; exit 1; }
assert_contains() { grep -q -F -- "$2" "$1" || fail "$3: expected $1 to contain: $2"; }
assert_not_contains() { ! grep -q -F -- "$2" "$1" || fail "$3: expected $1 NOT to contain: $2"; }
assert_calls() { [ "$(wc -l <"$CALLS")" -eq "$2" ] || fail "$1: expected $2 invocation(s), got $(wc -l <"$CALLS")"; }
assert_call() { [ "$(sed -n "${2}p" "$CALLS")" = "$3" ] || fail "$1: call $2 was '$(sed -n "${2}p" "$CALLS")', expected '$3'"; }

# scenario NAME: fresh call log, plan dir, job summary and OOM counter;
# plan files are written between `scenario` and `run`.
scenario() {
  export CALLS="$tmp/calls.$1" PLAN="$tmp/plan.$1" GITHUB_STEP_SUMMARY="$tmp/summary.$1"
  : >"$CALLS"
  : >"$GITHUB_STEP_SUMMARY"
  mkdir -p "$PLAN"
  printf 'low 0\noom 0\noom_kill 0\n' >"$tmp/cgroup/memory.events"
}
plan() { printf '%s\n%s\n' "$3" "$4" >"$PLAN/$2"; }
# run SCENARIO EXPECTED_RC: the real wrapper around the fake cargo.
run() {
  local rc=0
  "$script" "$tmp/fake-cargo" llvm-cov --no-report nextest --workspace >"$tmp/out.$1" 2>&1 || rc=$?
  [ "$rc" -eq "$2" ] || { cat "$tmp/out.$1" >&2; fail "$1: expected exit $2, got $rc"; }
}
assert_no_retry() {
  assert_calls "$1" 1
  assert_not_contains "$tmp/out.$1" "retrying once" "$1"
  assert_not_contains "$tmp/out.$1" "::warning" "$1"
  [ ! -s "$tmp/summary.$1" ] || fail "$1: expected an empty job summary, got: $(cat "$tmp/summary.$1")"
}

echo "scenario 1: clean success runs once"
scenario s1
run s1 0
assert_no_retry s1
assert_contains "$tmp/out.s1" "Finished test profile" s1

echo "scenario 2: the coverage-all kill (sccache exit 2, no diagnostic) is retried once with half the build jobs, with evidence and a job summary"
scenario s2
plan s2 1 101 "$coverage_all_kill"
touch "$PLAN/1.oom"
run s2 0
assert_calls s2 2
assert_call s2 1 "jobs=8 llvm-cov --no-report nextest --workspace"
assert_call s2 2 "jobs=4 llvm-cov --no-report nextest --workspace"
assert_contains "$tmp/out.s2" "::warning title=CI infrastructure failure - compiler killed::The compiler was killed while building intent-services (lib test) and printed no compiler error. This is the CI runner, not your code: the kernel's out-of-memory killer" s2
assert_contains "$tmp/out.s2" "the kernel's out-of-memory killer stopped it at this job's own memory limit (50.0 GiB)" s2
assert_contains "$tmp/out.s2" "Retrying the build once with 4 build jobs" s2
assert_contains "$tmp/out.s2" "::group::Killed-compiler evidence" s2
assert_contains "$tmp/out.s2" "killed unit(s): intent-services (lib test)" s2
assert_contains "$tmp/out.s2" "memory.max: 53687091200 (50.0 GiB)" s2
assert_contains "$tmp/out.s2" "memory.events: oom_kill 1" s2
assert_contains "$tmp/out.s2" "stub free -m" s2
assert_contains "$tmp/out.s2" "Killed process 4242 (rustc)" s2
assert_not_contains "$tmp/out.s2" "unrelated kernel line" s2
assert_contains "$tmp/out.s2" "stub sccache --show-stats" s2
assert_contains "$tmp/out.s2" "retrying once with CARGO_BUILD_JOBS=4:" s2
assert_not_contains "$tmp/out.s2" "::error" s2
assert_contains "$tmp/summary.s2" "### ⚠️ CI infrastructure failure - compiler killed" s2
assert_contains "$tmp/summary.s2" 'The compiler was killed while building `intent-services (lib test)` and printed no compiler error. This is the CI runner, not your code:' s2
assert_contains "$tmp/summary.s2" "Retried once with 4 build jobs (was 8)." s2
assert_contains "$tmp/summary.s2" "The retry passed." s2

echo "scenario 3: the check-job kill (sccache's signal 9 line) is retried; no readable counter says so"
scenario s3
plan s3 1 101 "$check_kill"
rm "$tmp/cgroup/memory.events"
run s3 0
assert_calls s3 2
assert_contains "$tmp/out.s3" "most likely the runner ran out of memory (no OOM-kill counter readable here to confirm)" s3
assert_contains "$tmp/out.s3" "sccache: Compile terminated by signal 9" s3

echo "scenario 4: rustc's SIGSEGV banner (WarpBuild) is retried; no OOM kill recorded says so"
scenario s4
plan s4 1 101 "$segv_kill"
run s4 0
assert_calls s4 2
assert_contains "$tmp/out.s4" "rustc interrupted by SIGSEGV" s4
assert_contains "$tmp/out.s4" "The compiler crashed with SIGSEGV while building intent-services (lib test) and printed no compiler error. A compiler crash without a compiler error is almost always the runner, not your code" s4
assert_contains "$tmp/out.s4" "the kernel recorded no out-of-memory kill in this job, so the signal came from elsewhere" s4

echo "scenario 5: without sccache, cargo's (signal: 9, SIGKILL) is retried; a kill from a limit above the job names the shared host"
scenario s5
plan s5 1 101 "$plain_kill"
touch "$PLAN/1.hostoom"
run s5 0
assert_calls s5 2
assert_contains "$tmp/out.s5" "because the machine ran out of memory, not this job's own limit: other jobs on the same host share that memory" s5

echo "scenario 6: a genuine compile error fails fast with its exit code, no retry"
scenario s6
plan s6 1 101 "$compile_error"
run s6 101
assert_no_retry s6

echo "scenario 7: a killed compiler alongside a genuine error is not retried (the error decides)"
scenario s7
plan s7 1 101 "$mixed"
run s7 101
assert_calls s7 1
assert_contains "$tmp/out.s7" "::notice::a compiler process died by a signal, but the output also has a real compiler error" s7
assert_not_contains "$tmp/out.s7" "retrying once" s7

echo "scenario 8: a build script exiting 2 (its path holds rustc) is not the compiler, no retry"
scenario s8
plan s8 1 101 "$build_script"
run s8 101
assert_no_retry s8

echo "scenario 9: a kill after tests started is not retried (a test result decides)"
scenario s9
plan s9 1 101 "$after_tests"
run s9 101
assert_calls s9 1
assert_contains "$tmp/out.s9" "::notice::a compiler process died by a signal after tests had started" s9

echo "scenario 10: a kill that repeats is retried exactly once and fails with a plain error"
scenario s10
plan s10 1 101 "$coverage_all_kill"
plan s10 2 101 "$coverage_all_kill"
run s10 101
assert_calls s10 2
assert_contains "$tmp/out.s10" "::error title=CI infrastructure failure - compiler killed twice::The compiler was killed again while building intent-services (lib test) on the retry, with no compiler error. This is the CI runner (most likely out of memory), not your code" s10
assert_contains "$tmp/summary.s10" "**The retry was killed too.**" s10

echo "scenario 11: a retry that fails another way keeps its exit code and says so in the summary"
scenario s11
plan s11 1 101 "$coverage_all_kill"
plan s11 2 100 "test failed"
run s11 100
assert_calls s11 2
assert_not_contains "$tmp/out.s11" "::error" s11
assert_contains "$tmp/summary.s11" "The retry failed for another reason (exit 100); see the step log." s11

echo "scenario 12: one build job is halved to one, not zero"
scenario s12
plan s12 1 101 "$coverage_all_kill"
CARGO_BUILD_JOBS=1 run s12 0
assert_call s12 2 "jobs=1 llvm-cov --no-report nextest --workspace"

echo "scenario 13: outside GitHub Actions the command runs once, unchanged, with no job summary"
scenario s13
plan s13 1 101 "$coverage_all_kill"
GITHUB_ACTIONS='' run s13 101
assert_no_retry s13
assert_not_contains "$tmp/out.s13" "Killed-compiler evidence" s13

echo "scenario 14: no job summary file is fine"
scenario s14
plan s14 1 101 "$check_kill"
unset GITHUB_STEP_SUMMARY
run s14 0
assert_calls s14 2

echo "scenario 15: sccache's server path (exit 254, its signal line lost) is retried"
scenario s15
plan s15 1 101 "$server_kill"
run s15 0
assert_calls s15 2
assert_contains "$tmp/out.s15" "killed unit(s): intent-store (lib)" s15

echo "scenario 16: rustc's SIGSEGV banner is enough when a wrapper rewrote the exit status"
scenario s16
plan s16 1 101 "$segv_rewritten"
run s16 0
assert_calls s16 2

echo "scenario 17: a large log (a full build's worth of lines) is read to the end without SIGPIPE"
scenario s17
big="$tmp/big.txt"
awk 'BEGIN { for (i = 0; i < 200000; i++) printf "   Compiling crate-%d v0.1.0 (/home/runner/work/intentd/intentd/crates/c%d)\n", i, i }' >"$big"
# The banner comes first, so a reader that stops at the first match leaves
# the rest of the log unread (and its producer SIGPIPEd).
printf '101\n%s\n' "$segv_kill" >"$PLAN/1"
cat "$big" >>"$PLAN/1"
run s17 0
assert_calls s17 2
assert_contains "$tmp/out.s17" "The compiler crashed with SIGSEGV while building intent-services (lib test)" s17

echo "scenario 18: usage error without a command"
rc=0
"$script" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 2 ] || fail "s18: expected exit 2, got $rc"

echo "ok: ci-killed-rustc-retry.sh"
