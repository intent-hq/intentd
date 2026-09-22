#!/usr/bin/env bash
# Regression tests for the repo-root Makefile (the make-gate forwarder): runs
# the real Makefile as packages/intentd inside a fixture monorepo whose root
# Makefile is a stub that logs the target it was asked for and the RESUME /
# BASE / GATE_FORCE values it saw, then asserts the forwarding, the variable
# pass-through, the standalone-clone refusal, and that unrelated parents are
# never forwarded to. No cargo, no network. Bash 3.2 compatible.
#
# Run directly: ./scripts/test-makefile-forwarder.sh
set -euo pipefail

# The caller's environment must not steer make under test.
unset RESUME BASE GATE_FORCE DRY_RUN ARGS MAKEFLAGS MFLAGS MAKELEVEL ROOT_EXIT

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
forwarder="$here/../Makefile"
make_bin=${FORWARDER_TEST_MAKE:-make}
temp_dir=$(cd "$(mktemp -d)" && pwd -P)
trap 'rm -rf "$temp_dir"' EXIT

fail() {
  echo "makefile-forwarder test failed: $*" >&2
  exit 1
}

[[ -f "$forwarder" ]] || fail "forwarder Makefile not found at $forwarder"
"$make_bin" --version 2>/dev/null | grep 'GNU Make' >/dev/null || fail "$make_bin is not GNU make"

# The targets the forwarder claims to forward, read from the Makefile itself
# so the stub root covers exactly that list; the spec's gate names must be in
# it.
targets=$(grep -E '^FORWARDED_TARGETS[[:space:]]*:=' "$forwarder" | sed 's/^[^=]*:=[[:space:]]*//')
[[ -n "$targets" ]] || fail "could not read FORWARDED_TARGETS from $forwarder"
for required in check test test-changed gate list-tests; do
  [[ " $targets " == *" $required "* ]] || fail "FORWARDED_TARGETS lacks '$required': $targets"
done

# Stub root Makefile: every forwarded target appends
# "<target> RESUME=<v> BASE=<v> GATE_FORCE=<v>" to ROOT_LOG and exits with
# ROOT_EXIT (both read from the environment; default success).
root_log="$temp_dir/root.log"
export ROOT_LOG="$root_log"
write_stub_root() {
  cat >"$1/Makefile" <<MK
ROOT_EXIT ?= 0
.PHONY: $targets
$targets:
	@printf '%s\\n' '\$@ RESUME=\$(RESUME) BASE=\$(BASE) GATE_FORCE=\$(GATE_FORCE)' >>"\$(ROOT_LOG)"
	@exit \$(ROOT_EXIT)
MK
}

# Fixture 1: the monorepo layout — stub root, .gitmodules naming
# packages/intentd, and the real forwarder at packages/intentd/Makefile.
monorepo="$temp_dir/monorepo"
mkdir -p "$monorepo/packages/intentd"
write_stub_root "$monorepo"
printf '[submodule "packages/intentd"]\n\tpath = packages/intentd\n\turl = https://github.com/intent-hq/intentd\n' >"$monorepo/.gitmodules"
cp "$forwarder" "$monorepo/packages/intentd/Makefile"

# Fixture 2: a standalone clone — nothing two levels up.
standalone="$temp_dir/alone/clone/intentd"
mkdir -p "$standalone"
cp "$forwarder" "$standalone/Makefile"

# Fixture 3: an unrelated Makefile two levels up, first with no .gitmodules,
# then with one naming a different path. Neither may be forwarded to.
unrelated="$temp_dir/unrelated/x"
mkdir -p "$unrelated/y/intentd"
write_stub_root "$unrelated"
cp "$forwarder" "$unrelated/y/intentd/Makefile"

# run_make <dir> [make args...]: runs make in <dir> with a fresh root log,
# capturing status, stdout, stderr and the root log.
run_make() {
  local dir=$1
  shift
  : >"$root_log"
  set +e
  (cd "$dir" && "$make_bin" "$@") >"$temp_dir/stdout" 2>"$temp_dir/stderr"
  status=$?
  set -e
  stdout=$(<"$temp_dir/stdout")
  stderr=$(<"$temp_dir/stderr")
  log=$(<"$root_log")
}

expect_status() {
  [[ "$status" -eq "$1" ]] || fail "$case_name: exited $status, expected $1"$'\n'"stdout: $stdout"$'\n'"stderr: $stderr"
}

expect_log() {
  [[ "$log" == "$1" ]] || fail "$case_name: root log was"$'\n'"$log"$'\n'"expected"$'\n'"$1"
}

expect_standalone_refusal() {
  expect_status 2
  expect_log ""
  [[ "$stderr" == *"make -C"* && "$stderr" == *"intent"* ]] || fail "$case_name: stderr does not name the monorepo command: $stderr"
  [[ "$stderr" == *"make $1:"* ]] || fail "$case_name: stderr does not name the target: $stderr"
}

# (1) Each forwarded target reaches the root Makefile under the same name.
for target in $targets; do
  case_name="forward $target"
  run_make "$monorepo/packages/intentd" "$target"
  expect_status 0
  expect_log "$target RESUME= BASE= GATE_FORCE="
done

# (2) Command-line variables on the submodule invocation reach the root make.
case_name="variables pass through"
run_make "$monorepo/packages/intentd" RESUME=1 BASE=abc GATE_FORCE=1 test-changed
expect_status 0
expect_log "test-changed RESUME=1 BASE=abc GATE_FORCE=1"

# The root recipe's failure is the forwarder's failure.
case_name="root failure propagates"
ROOT_EXIT=3 run_make "$monorepo/packages/intentd" check
expect_log "check RESUME= BASE= GATE_FORCE="
[[ "$status" -ne 0 ]] || fail "$case_name: exited 0 although the root recipe failed"

# (3) A standalone clone exits 2 and names the monorepo command.
case_name="standalone clone"
run_make "$standalone" check
expect_standalone_refusal check

# (4) An unrelated parent Makefile takes the standalone path.
case_name="unrelated parent without .gitmodules"
run_make "$unrelated/y/intentd" check
expect_standalone_refusal check

case_name="unrelated parent with non-matching .gitmodules"
printf '[submodule "packages/other"]\n\tpath = packages/other\n\turl = https://example.invalid/other\n' >"$unrelated/.gitmodules"
run_make "$unrelated/y/intentd" test
expect_standalone_refusal test

# (5) Unknown targets keep make's ordinary failure and never reach the root.
case_name="unknown target"
run_make "$monorepo/packages/intentd" nosuchtarget
expect_status 2
expect_log ""
[[ "$stderr" == *"No rule to make target"* ]] || fail "$case_name: unexpected stderr: $stderr"

# (6) Bare make is help only: exit 0, nothing forwarded.
case_name="bare make"
run_make "$monorepo/packages/intentd"
expect_status 0
expect_log ""
[[ "$stdout" == *"Targets:"* ]] || fail "$case_name: help text missing from stdout: $stdout"

bash -n "${BASH_SOURCE[0]}" || fail "test-makefile-forwarder.sh does not parse"
echo "makefile-forwarder tests passed"
