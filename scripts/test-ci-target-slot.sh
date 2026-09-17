#!/usr/bin/env bash
# Regression tests for ci-target-slot.sh (intent-hq/intent#5256): builds a
# fake /raid/ci-target tree in a scratch dir with controlled mtimes and
# asserts the purge selection every cleaner should share — a dir carrying a
# fresh `.in-use` marker is never a candidate (even when nothing else inside
# it looks active), an unmarked idle dir is, an expired marker no longer
# protects, the own dir / own slot are skipped, the recursive activity check
# still shields unmarked live dirs, mark/unmark round-trip, and the
# consumer's is-purgeable recheck stops a candidate marked after selection
# from being deleted.
#
# Run directly: ./scripts/test-ci-target-slot.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
script="$here/ci-target-slot.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
root="$tmp/ci-target"

fail() { echo "FAIL: $*" >&2; exit 1; }
# mkslot SLOT JOB AGE_MIN: a target dir at exactly root/intentd/SLOT/JOB
# whose top level and one artifact inside are AGE_MIN minutes old. Give
# sibling dirs distinct ages: candidates are ordered oldest first.
mkslot() {
  local dir="$root/intentd/$1/$2"
  mkdir -p "$dir/debug/deps"
  echo obj >"$dir/debug/deps/a.o"
  find "$dir" -exec touch -d "-$3 minutes" {} +
  echo "$dir"
}
# candidates ARGS...: purge-candidates output as one space-joined line, plus
# the skip log in $tmp/skips.
candidates() {
  "$script" purge-candidates "$root" "$@" 2>"$tmp/skips" | tr '\n' ' ' | sed 's/ $//'
}
assert_eq() { [ "$1" = "$2" ] || fail "$3: expected [$2], got [$1]"; }
assert_skip() { grep -q -F -- "$1" "$tmp/skips" || { cat "$tmp/skips" >&2; fail "$2: expected skip log to contain: $1"; }; }

echo "scenario 1: mark writes the marker with run facts; unmark removes it"
own=$(mkslot self check 5000)
GITHUB_RUN_ID=424242 GITHUB_JOB=check RUNNER_NAME=self "$script" mark "$own" >/dev/null
[ -f "$own/.in-use" ] || fail "s1: marker not written"
grep -q '^run_id=424242$' "$own/.in-use" || fail "s1: run_id fact missing"
grep -q '^job=check$' "$own/.in-use" || fail "s1: job fact missing"
grep -q '^runner=self$' "$own/.in-use" || fail "s1: runner fact missing"
"$script" is-in-use "$own" || fail "s1: is-in-use should succeed on a fresh marker"
"$script" unmark "$own" >/dev/null
[ ! -e "$own/.in-use" ] || fail "s1: marker not removed"
"$script" is-in-use "$own" && fail "s1: is-in-use should fail once unmarked"
"$script" unmark "$own" >/dev/null || fail "s1: unmark must be a no-op when absent"
"$script" mark "$tmp/fresh/intentd/slot9/check" >/dev/null
[ -f "$tmp/fresh/intentd/slot9/check/.in-use" ] || fail "s1: mark must create a missing dir"

echo "scenario 2: an idle unmarked dir is a candidate; a marked idle dir is not"
rm -rf "$root"
idle=$(mkslot slot1 check 5001)
marked=$(mkslot slot2 check 5000)
"$script" mark "$marked" >/dev/null
# The job marked its dir 60 min ago and is in a quiet phase: nothing inside
# (marker included) was touched within the LRU tier's 15-minute activity
# window, so only the marker protects it.
touch -d "-5000 minutes" "$marked" "$marked/debug" "$marked/debug/deps" "$marked/debug/deps/a.o"
touch -d "-60 minutes" "$marked/.in-use"
assert_eq "$(candidates --quiet-min 15)" "$idle" s2
assert_skip "skipping (in use, .in-use < 240min): $marked" s2
assert_eq "$(candidates --quiet-min 2880)" "$idle" s2-48h

echo "scenario 3: an expired marker (older than IN_USE_MAX_MIN) no longer protects"
touch -d "-300 minutes" "$marked/.in-use"
assert_eq "$(candidates --quiet-min 15)" "$idle $marked" s3
# ...unless the caller widens the validity window.
assert_eq "$(IN_USE_MAX_MIN=600 candidates --quiet-min 15)" "$idle" s3-wide

echo "scenario 4: --skip-dir skips exactly the own dir, --skip-slot the whole slot"
rm -rf "$root"
a=$(mkslot self check 5002)
b=$(mkslot self coverage-e2e 5001)
c=$(mkslot other check 5000)
assert_eq "$(candidates --quiet-min 2880 --skip-dir "$a")" "$b $c" s4-dir
assert_skip "skipping (own dir): $a" s4-dir
assert_eq "$(candidates --quiet-min 2880 --skip-slot self)" "$c" s4-slot
assert_skip "skipping (own slot): $a" s4-slot
assert_skip "skipping (own slot): $b" s4-slot

echo "scenario 5: recursive activity still shields an unmarked live dir"
rm -rf "$root"
live=$(mkslot slot1 check 5001)
touch "$live/debug/deps/a.o"
cold=$(mkslot slot2 check 5000)
assert_eq "$(candidates --quiet-min 15)" "$cold" s5
assert_skip "skipping (active <15min): $live" s5
# A 48 h window treats a dir touched 60 min ago as active; a 15 min one does not.
touch -d "-60 minutes" "$live/debug/deps/a.o"
assert_eq "$(candidates --quiet-min 2880)" "$cold" s5-48h
assert_eq "$(candidates --quiet-min 15)" "$live $cold" s5-15m

echo "scenario 6: ordering is oldest top-level mtime first; the allowlist holds"
rm -rf "$root"
newer=$(mkslot slot1 check 3000)
older=$(mkslot slot2 check 9000)
mkdir -p "$root/stray-depth1" "$root/intentd/stray-depth2" "$older/debug/depth4"
touch -d "-9000 minutes" "$root/stray-depth1" "$root/intentd/stray-depth2"
find "$older" -exec touch -d "-9000 minutes" {} +
assert_eq "$(candidates --quiet-min 15)" "$older $newer" s6
"$script" purge-candidates "$root" 2>/dev/null && fail "s6: --quiet-min must be required"
"$script" purge-candidates "$root" --quiet-min 0 2>/dev/null && fail "s6: --quiet-min 0 must be rejected"
"$script" purge-candidates relative/root --quiet-min 15 2>/dev/null && fail "s6: relative ROOT must be rejected"

echo "scenario 7: is-purgeable is the consumer's live recheck — a candidate marked after selection is not deleted"
rm -rf "$root"
first=$(mkslot slot1 check 9000)
second=$(mkslot slot2 check 5000)
"$script" is-purgeable "$second" --quiet-min 15 2>/dev/null || fail "s7: an idle unmarked dir must be purgeable"
"$script" is-purgeable "$tmp/nowhere" --quiet-min 15 2>"$tmp/skips" && fail "s7: a missing dir must not be purgeable"
assert_skip "skipping (gone): $tmp/nowhere" s7-gone
"$script" is-purgeable "$second" 2>/dev/null && fail "s7: --quiet-min must be required"
# The ci.yml preflight loop shape: the producer has already emitted both
# candidates when the loop starts; a job marks the second one while the
# first is being deleted. Without the recheck the second dir is deleted too.
deleted=()
while IFS= read -r dir; do
  "$script" is-purgeable "$dir" --quiet-min 15 2>>"$tmp/skips" || continue
  deleted+=("$dir")
  rm -rf -- "$dir"
  "$script" mark "$second" >/dev/null
done < <("$script" purge-candidates "$root" --quiet-min 15 2>/dev/null)
assert_eq "${deleted[*]}" "$first" s7-race
assert_skip "skipping (in use, .in-use < 240min): $second" s7-race
[ -f "$second/debug/deps/a.o" ] || fail "s7: the dir marked mid-loop must survive"
# The recheck also honours late activity, not just the marker.
"$script" unmark "$second" >/dev/null
touch "$second/debug/deps/a.o"
"$script" is-purgeable "$second" --quiet-min 15 2>"$tmp/skips" && fail "s7: a dir active since selection must not be purgeable"
assert_skip "skipping (active <15min): $second" s7-active

echo "OK: all scenarios passed"
