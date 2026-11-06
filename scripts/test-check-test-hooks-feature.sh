#!/usr/bin/env bash
# Regression tests for check-test-hooks-feature.sh: runs the real check
# against throwaway cargo workspaces (path dependencies only, so no network).
# A `test-hooks` feature turned on through a normal, build, target-specific or
# forwarded edge must fail and name the crate (also via a relative manifest
# path); one turned on only from
# [dev-dependencies], or a look-alike feature name, must pass. Finally the
# check must pass on this repo.
#
# Run directly: ./scripts/test-check-test-hooks-feature.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
check="$here/check-test-hooks-feature.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
export CARGO_TARGET_DIR="$tmp/target"

failures=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

# new_workspace NAME BIN_DEPS [BIN_EXTRA]: a workspace with
#   core   — a library with `test-hooks` and `test-hooks-docs` features,
#   mid    — a library depending on core, forwarding `test-hooks`,
#   app    — the binary under check, with BIN_DEPS as its [dependencies]
#            and BIN_EXTRA appended to its manifest.
new_workspace() {
  ws="$tmp/$1"
  mkdir -p "$ws/core/src" "$ws/mid/src" "$ws/app/src"
  cat >"$ws/Cargo.toml" <<'EOF'
[workspace]
resolver = "2"
members = ["core", "mid", "app"]
EOF
  cat >"$ws/core/Cargo.toml" <<'EOF'
[package]
name = "core"
version = "0.1.0"
edition = "2021"

[features]
test-hooks = []
test-hooks-docs = []
EOF
  cat >"$ws/mid/Cargo.toml" <<'EOF'
[package]
name = "mid"
version = "0.1.0"
edition = "2021"

[dependencies]
core = { path = "../core" }

[features]
test-hooks = ["core/test-hooks"]
EOF
  printf '[package]\nname = "app"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\n%s\n%s\n' \
    "$2" "${3:-}" >"$ws/app/Cargo.toml"
  : >"$ws/core/src/lib.rs"
  : >"$ws/mid/src/lib.rs"
  echo 'fn main() {}' >"$ws/app/src/main.rs"
}

# run_check NAME: run the check on workspace NAME, package app.
run_check() {
  set +e
  out=$(CARGO_TREE_FLAGS=--offline "$check" "$tmp/$1/Cargo.toml" app 2>&1)
  rc=$?
  set -e
}

expect_pass() {
  run_check "$1"
  [ "$rc" -eq 0 ] || fail "$1: expected exit 0, got $rc: $out"
}

# expect_fail NAME CRATE: exit 1 and an ::error line naming CRATE.
expect_fail() {
  run_check "$1"
  [ "$rc" -eq 1 ] || fail "$1: expected exit 1, got $rc: $out"
  grep -q "^::error::$2 feature \"test-hooks\"" <<<"$out" || fail "$1: no ::error line for $2: $out"
}

new_workspace clean 'core = { path = "../core" }'
expect_pass clean

new_workspace dev_only 'core = { path = "../core" }' '
[dev-dependencies]
core = { path = "../core", features = ["test-hooks"] }
mid = { path = "../mid", features = ["test-hooks"] }'
expect_pass dev_only

new_workspace look_alike 'core = { path = "../core", features = ["test-hooks-docs"] }'
expect_pass look_alike

new_workspace normal 'core = { path = "../core", features = ["test-hooks"] }'
expect_fail normal core

# A relative manifest path resolves against the caller's directory.
set +e
out=$(cd "$tmp" && CARGO_TREE_FLAGS=--offline "$check" normal/Cargo.toml app 2>&1)
rc=$?
set -e
[ "$rc" -eq 1 ] || fail "relative manifest: expected exit 1, got $rc: $out"
grep -q '^::error::core feature "test-hooks"' <<<"$out" || fail "relative manifest: no ::error line for core: $out"

new_workspace forwarded 'mid = { path = "../mid", features = ["test-hooks"] }'
expect_fail forwarded core
expect_fail forwarded mid

new_workspace build_dep '' '
[build-dependencies]
core = { path = "../core", features = ["test-hooks"] }'
expect_fail build_dep core

new_workspace other_target '' '
[target."cfg(windows)".dependencies]
core = { path = "../core", features = ["test-hooks"] }'
expect_fail other_target core

# An unreadable graph is an error, not a pass.
set +e
out=$(CARGO_TREE_FLAGS=--offline "$check" "$tmp/missing/Cargo.toml" app 2>&1)
rc=$?
set -e
[ "$rc" -eq 2 ] || fail "missing manifest: expected exit 2, got $rc: $out"

# This repo's shipped binaries must be clean.
"$check" >/dev/null || fail "this repo: check-test-hooks-feature.sh failed"

if [ "$failures" -ne 0 ]; then
  echo "$failures test(s) failed" >&2
  exit 1
fi
echo "ok: check-test-hooks-feature.sh regression tests passed"
