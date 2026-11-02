#!/usr/bin/env bash
# Narrow retry wrapper for a rustc that the runner killed mid-build.
#
# Merge-queue runs 40205607311, 41190030739 (coverage-all) and 40496877837
# (coverage-changed), plus PR run 40293024372 (check and coverage-changed on
# one host two minutes apart), all failed the same way with sound code:
#
#   error: process didn't exit successfully: `sccache …/rustc --crate-name
#     intent_services … --test …` (exit status: 2)
#   error: could not compile `intent-services` (lib test)
#
# and no compiler diagnostic. sccache cannot cache `--test` builds (no
# --crate-type), so it runs rustc itself; when rustc dies by a signal it
# prints `sccache: Compile terminated by signal N` on stdout and exits 2
# (rustc never exits 2 on its own). Under nextest, cargo's stdout is the
# JSON message stream, so that one line is dropped and the log goes silent.
# The one run outside nextest (40293024372's check) printed signal 9: the
# kernel's out-of-memory killer, from the slot's Docker memory limit. The
# WarpBuild run printed rustc's own SIGSEGV banner instead. So this is the
# runner, not the code, and rung 1 (make it impossible in the code) does not
# apply.
#
# The wrapper runs the command once and retries exactly once, ONLY when all
# of these hold for its output:
#   * cargo reports `could not compile` without `due to N previous errors`;
#   * a compiler process died by a signal: sccache's exit status 2, a
#     `(signal: N, …)` exit, sccache's `terminated by signal` line, or
#     rustc's `rustc interrupted by SIG…` banner;
#   * there is no other rustc/clippy `error:` diagnostic (a real error in
#     another crate decides, and a retry would only hide it);
#   * no test had started (a later failure would be a test failure).
# Before the retry it prints evidence (the job's cgroup memory and OOM-kill
# counters, host memory, kernel OOM lines, sccache stats) and writes a job
# summary entry naming the failure plainly. The retry runs with half the
# build jobs, which lowers peak memory: the compiled crates are reused, so
# mostly the killed crate and its dependents rebuild.
#
# Outside GitHub Actions (GITHUB_ACTIONS != true) it execs the command
# unchanged, so local runs keep their terminal and exit status.
#
# Usage: ci-killed-rustc-retry.sh CMD [ARGS...]
# The command's output is streamed unchanged (stderr merged into stdout, as
# Actions renders them anyway) and captured for the signature check.
set -euo pipefail

[ $# -ge 1 ] || { echo "usage: $0 CMD [ARGS...]" >&2; exit 2; }

if [ "${GITHUB_ACTIONS:-}" != "true" ]; then
  exec "$@"
fi

# `could not compile `crate` (target)` — cargo's line for a failed unit.
COULD_NOT='^error: could not compile `[^`]+`'
GENUINE_COUNT='due to [0-9]+ previous errors?'
# A compiler process that died by a signal. sccache's local-compile path
# maps a signal death to exit 2; without sccache cargo prints the signal.
# The backticked command must run rustc/clippy-driver itself (possibly
# behind sccache or cargo-llvm-cov), not a build script such as
# `…/rustc_version-…/build-script-build` or the outer cargo. sccache's
# server path reports the same death as exit 254 (-2).
KILLED="process didn't exit successfully: \`([^\`]*[ /])?(rustc|clippy-driver) [^\`]*\` \\((exit status: (2|254)|signal: [0-9]+[^)]*)\\)"
SIGNAL_LINE='(sccache: Compile terminated by signal [0-9]+|sccache: Compiler killed by signal [0-9]+|rustc interrupted by SIG[A-Z]+)'
DIAGNOSTIC='^error(\[E[0-9]+\])?: '
# The `error:` lines a killed compile prints around itself: cargo's
# could-not-compile and process lines, nextest's / cargo-llvm-cov's exit
# lines for the outer cargo, and rustc's signal banner.
ENVELOPE='^error: (could not compile |process didn.t exit successfully: |command `[^`]*cargo[^`]*` exited with code |rustc interrupted by SIG)'
# nextest's run header and libtest's: once tests start, a failure is theirs.
TESTS_STARTED='^ *(Starting [0-9]+ tests? across|running [0-9]+ tests?$)'
# awk reads the patterns from ENVIRON: `awk -v` would process their
# backslashes as string escapes and turn `\(` into a group.
export COULD_NOT GENUINE_COUNT KILLED SIGNAL_LINE DIAGNOSTIC ENVELOPE TESTS_STARTED

log=$(mktemp)
plain=$(mktemp)
trap 'rm -f "$log" "$plain"' EXIT

# The captured output without colour codes (cargo may emit them under
# CARGO_TERM_COLOR=always), written once per run by run_once.
strip() { cat "$plain"; }

# One pass that reads to EOF: a `grep -q` at the end of a pipeline exits
# early and SIGPIPEs upstream stages, which under pipefail turns a large log
# into 141. Prints one word: the reason not to retry, or `killed`.
classify() {
  strip | awk '
    BEGIN {
      could = ENVIRON["COULD_NOT"]; genuine = ENVIRON["GENUINE_COUNT"]
      killed = ENVIRON["KILLED"]; sigline = ENVIRON["SIGNAL_LINE"]
      diag = ENVIRON["DIAGNOSTIC"]; envelope = ENVIRON["ENVELOPE"]
      started = ENVIRON["TESTS_STARTED"]
    }
    $0 ~ could { c = 1; if ($0 ~ genuine) g = 1 }
    $0 ~ killed || $0 ~ sigline { k = 1 }
    $0 ~ diag && $0 !~ envelope { d = 1 }
    $0 ~ started { t = 1 }
    END {
      if (!c) print "no-compile-failure"
      else if (!k) print "no-killed-compiler"
      else if (g || d) print "genuine-diagnostic"
      else if (t) print "tests-started"
      else print "killed"
    }'
}

run_once() {
  local rc
  : >"$log"
  # The command's own status, not tee's: read PIPESTATUS right after the
  # pipeline, with -e off so a red command does not abort the wrapper.
  set +e
  "$@" 2>&1 | tee "$log"
  rc="${PIPESTATUS[0]}"
  set -e
  sed -E $'s/\x1b\\[[0-9;]*[A-Za-z]//g' "$log" >"$plain"
  return "$rc"
}

# The job's own cgroup (Docker slots and hosted VMs alike, cgroup v2).
# KILLED_RUSTC_CGROUP points the tests at a fake one.
cgroup_dir() {
  local rel
  if [ -n "${KILLED_RUSTC_CGROUP:-}" ]; then echo "$KILLED_RUSTC_CGROUP"; return; fi
  rel=$(sed -n 's/^0:://p' /proc/self/cgroup 2>/dev/null || true)
  [ -n "$rel" ] && [ -d "/sys/fs/cgroup$rel" ] && echo "/sys/fs/cgroup$rel"
}

# "<oom> <oom_kill>" from the job cgroup's memory.events, or "? ?". `oom`
# counts this cgroup's own limit being hit; `oom_kill` counts its processes
# killed by any OOM killer, so a kill without an `oom` came from a limit
# above the job: the host's shared runner cap or the whole machine.
oom_counters() {
  local dir
  dir=$(cgroup_dir) || true
  if [ -z "$dir" ] || [ ! -r "$dir/memory.events" ]; then echo "? ?"; return; fi
  awk '$1 == "oom" { o = $2 } $1 == "oom_kill" { k = $2 }
    END { print (o == "" ? "?" : o) " " (k == "" ? "?" : k) }' "$dir/memory.events"
}

# The plain-language cause, from the counters before and after the run.
oom_cause() {
  local before_oom before_kill after_oom after_kill limit
  read -r before_oom before_kill <<<"$1"
  read -r after_oom after_kill <<<"$2"
  if ! [[ "$before_kill$after_kill$before_oom$after_oom" =~ ^[0-9]+$ ]]; then
    echo "most likely the runner ran out of memory (no OOM-kill counter readable here to confirm)"
  elif [ "$after_kill" -le "$before_kill" ]; then
    echo "the kernel recorded no out-of-memory kill in this job, so the signal came from elsewhere (see the evidence group)"
  elif [ "$after_oom" -gt "$before_oom" ]; then
    limit=$(cat "$(cgroup_dir)/memory.max" 2>/dev/null || echo "?")
    echo "the kernel's out-of-memory killer stopped it at this job's own memory limit ($(gib "$limit"))"
  else
    echo "the kernel's out-of-memory killer stopped it because the machine ran out of memory, not this job's own limit: other jobs on the same host share that memory"
  fi
}

gib() { awk -v b="$1" 'BEGIN { if (b ~ /^[0-9]+$/) printf "%.1f GiB", b / 1073741824; else print b }'; }

# What died, as one short line per unit: `intent-services (lib test)`.
killed_units() {
  strip | sed -nE 's/^error: could not compile `([^`]+)` (\([^)]*\)).*/\1 \2/p' | sort -u | paste -sd ',' - | sed 's/,/, /g'
}

signal_lines() {
  strip | grep -oE "$SIGNAL_LINE|\\((exit status: (2|254)|signal: [0-9]+[^)]*)\\)\$" | sort | uniq -c || true
}

evidence() {
  local dir
  echo "::group::Killed-compiler evidence"
  echo "killed unit(s): $(killed_units)"
  echo "--- compiler exit lines"
  strip | grep -E "$KILLED" | cut -c1-160 | sed 's/$/ …/' || true
  echo "--- signal lines"
  signal_lines
  dir=$(cgroup_dir) || true
  if [ -n "$dir" ]; then
    echo "--- job cgroup $dir"
    for f in memory.max memory.peak memory.current memory.swap.max; do
      [ -r "$dir/$f" ] && echo "$f: $(cat "$dir/$f") ($(gib "$(cat "$dir/$f")"))"
    done
    [ -r "$dir/memory.events" ] && sed 's/^/memory.events: /' "$dir/memory.events"
  else
    echo "--- no cgroup v2 memory files for this job"
  fi
  echo "--- free -m"
  free -m 2>&1 || true
  echo "--- nproc $(nproc 2>/dev/null || echo ?), CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-unset}"
  echo "--- kernel OOM lines (best effort; containers usually cannot read the kernel log)"
  dmesg 2>/dev/null | grep -iE 'out of memory|oom-kill|killed process' | tail -n 10 || echo "(kernel log not readable)"
  if [[ "${RUSTC_WRAPPER:-}" == *sccache* ]] && command -v sccache >/dev/null 2>&1; then
    echo "--- sccache --show-stats"
    timeout 20 sccache --show-stats 2>&1 || true
  fi
  echo "::endgroup::"
}

summary() {
  [ -n "${GITHUB_STEP_SUMMARY:-}" ] || return 0
  printf '%s\n' "$@" >>"$GITHUB_STEP_SUMMARY"
}

oom_before=$(oom_counters)
rc=0
run_once "$@" || rc=$?
[ "$rc" -eq 0 ] && exit 0

verdict=$(classify)
case "$verdict" in
  killed) ;;
  genuine-diagnostic)
    echo "::notice::a compiler process died by a signal, but the output also has a real compiler error — not retrying (that error decides)"
    exit "$rc" ;;
  tests-started)
    echo "::notice::a compiler process died by a signal after tests had started — not retrying (a test result decides)"
    exit "$rc" ;;
  *) exit "$rc" ;;
esac

units=$(killed_units)
cause=$(oom_cause "$oom_before" "$(oom_counters)")
cpus=$(nproc 2>/dev/null || echo 2)
jobs=${CARGO_BUILD_JOBS:-$cpus}
[[ "$jobs" =~ ^[0-9]+$ ]] || jobs=$cpus
retry_jobs=$(( jobs / 2 ))
[ "$retry_jobs" -ge 1 ] || retry_jobs=1

title="CI infrastructure failure - compiler killed"
# rustc's own SIGSEGV banner is a crash, not a kill: almost always the
# runner too (it did not reproduce on retry), but say what was seen.
if strip | grep -E 'rustc interrupted by SIG' >/dev/null; then
  event="crashed with $(strip | awk '!f && match($0, /rustc interrupted by SIG[A-Z]+/) { print substr($0, RSTART + 21, RLENGTH - 21); f = 1 }')"
  verdict_text="A compiler crash without a compiler error is almost always the runner, not your code"
else
  event="was killed"
  verdict_text="This is the CI runner, not your code"
fi
echo "::warning title=$title::The compiler $event while building $units and printed no compiler error. $verdict_text: $cause. Retrying the build once with $retry_jobs build jobs. Details in the 'Killed-compiler evidence' group."
evidence
summary "### ⚠️ $title" "" \
  "The compiler $event while building \`$units\` and printed no compiler error. $verdict_text: $cause." \
  "" "Retried once with $retry_jobs build jobs (was $jobs)."
echo "retrying once with CARGO_BUILD_JOBS=$retry_jobs: $*"
export CARGO_BUILD_JOBS=$retry_jobs
rc=0
run_once "$@" || rc=$?
if [ "$rc" -eq 0 ]; then
  summary "" "The retry passed." ""
  exit 0
fi
if [ "$(classify)" = killed ]; then
  echo "::error title=$title twice::The compiler $event again while building $(killed_units) on the retry, with no compiler error. This is the CI runner (most likely out of memory), not your code: re-queue the PR, and if it keeps happening the runner slot needs more memory."
  summary "" "**The retry was killed too.** Re-queue the PR; if it repeats, the runner slot needs more memory." ""
else
  summary "" "The retry failed for another reason (exit $rc); see the step log." ""
fi
exit "$rc"
