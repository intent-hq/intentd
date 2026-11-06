#!/usr/bin/env bash
# Regression tests for check-feature-crate-deps.py: runs the real check
# against throwaway cargo workspaces (path dependencies only, so no network)
# that reuse this repo's crate names. The allowed layering must pass. Each
# wrong-way edge must fail and name it: core or intent-transport depending on
# a feature (normal, dev, build or target-specific), a feature on intent-app,
# and a feature-to-feature edge the split plan does not list (also in the
# reverse direction of an allowed one). Finally the check must pass on this
# repo.
#
# Run directly: ./scripts/test-check-feature-crate-deps.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
check="$here/check-feature-crate-deps.py"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

failures=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

crates=(intent-core intent-services intent-transport intent-svc-agents intent-svc-providers
  intent-svc-scm intent-svc-runtime intent-svc-transfer intent-svc-collab intent-app intentd)

# new_workspace NAME [CRATE EXTRA]...: a workspace with every crate in
# `crates`, wired as the allowed layering (core -> intent-core, transport and
# every feature -> core, the plan's feature edges, intent-app -> every
# feature, intentd -> intent-app and transport). Each CRATE EXTRA pair appends
# EXTRA to CRATE's manifest.
new_workspace() {
  ws="$tmp/$1"
  shift
  mkdir -p "$ws"
  {
    echo '[workspace]'
    echo 'resolver = "2"'
    printf 'members = ['
    printf '"%s", ' "${crates[@]}"
    printf ']\n'
  } >"$ws/Cargo.toml"
  local crate deps
  for crate in "${crates[@]}"; do
    case $crate in
      intent-core) deps='' ;;
      intent-services) deps='intent-core' ;;
      intent-svc-agents) deps='intent-services intent-svc-providers intent-svc-scm intent-svc-runtime' ;;
      intent-svc-transfer) deps='intent-services intent-svc-agents' ;;
      intent-svc-* | intent-transport) deps='intent-services' ;;
      intent-app) deps='intent-services intent-svc-agents intent-svc-providers intent-svc-scm intent-svc-runtime intent-svc-transfer intent-svc-collab' ;;
      intentd) deps='intent-app intent-transport' ;;
    esac
    mkdir -p "$ws/$crate/src"
    : >"$ws/$crate/src/lib.rs"
    {
      printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\n' "$crate"
      for dep in $deps; do
        printf '%s = { path = "../%s" }\n' "$dep" "$dep"
      done
    } >"$ws/$crate/Cargo.toml"
  done
  while [ "$#" -gt 0 ]; do
    printf '%s\n' "$2" >>"$ws/$1/Cargo.toml"
    shift 2
  done
}

# run_check NAME: run the check on workspace NAME from that workspace's own
# directory (cargo reads .cargo/config.toml upward from where it runs).
run_check() {
  set +e
  out=$(cd "$tmp/$1" && CARGO_METADATA_FLAGS=--offline python3 -I -B "$check" Cargo.toml 2>&1)
  rc=$?
  set -e
}

expect_pass() {
  run_check "$1"
  [ "$rc" -eq 0 ] || fail "$1: expected exit 0, got $rc: $out"
}

# expect_fail NAME FROM TO: exit 1 and an ::error line naming FROM and TO.
expect_fail() {
  run_check "$1"
  [ "$rc" -eq 1 ] || fail "$1: expected exit 1, got $rc: $out"
  grep -q "^::error::.*$2.* $3 " <<<"$out" || fail "$1: no ::error line for $2 -> $3: $out"
}

new_workspace layered
expect_pass layered

# Dev-dependencies on core with features, and intent-app dev-depending on
# transport, are fine.
new_workspace dev_down \
  intent-svc-transfer '[dev-dependencies]
intent-services = { path = "../intent-services" }' \
  intent-app '[dev-dependencies]
intent-transport = { path = "../intent-transport" }'
expect_pass dev_down

new_workspace core_normal intent-services 'intent-svc-agents = { path = "../intent-svc-agents" }'
expect_fail core_normal intent-services intent-svc-agents

# A dev-dependency from core on a feature compiles two copies of core.
new_workspace core_dev intent-services '[dev-dependencies]
intent-svc-transfer = { path = "../intent-svc-transfer" }'
expect_fail core_dev intent-services intent-svc-transfer

new_workspace core_app intent-services '[build-dependencies]
intent-app = { path = "../intent-app" }'
expect_fail core_app intent-services intent-app

new_workspace transport_dev intent-transport '[dev-dependencies]
intent-svc-transfer = { path = "../intent-svc-transfer" }'
expect_fail transport_dev intent-transport intent-svc-transfer

new_workspace transport_target intent-transport '[target."cfg(windows)".dependencies]
intent-svc-runtime = { path = "../intent-svc-runtime" }'
expect_fail transport_target intent-transport intent-svc-runtime

new_workspace feature_app intent-svc-collab 'intent-app = { path = "../intent-app" }'
expect_fail feature_app intent-svc-collab intent-app

new_workspace unlisted intent-svc-runtime 'intent-svc-collab = { path = "../intent-svc-collab" }'
expect_fail unlisted intent-svc-runtime intent-svc-collab

new_workspace reversed intent-svc-agents '[dev-dependencies]
intent-svc-transfer = { path = "../intent-svc-transfer" }'
expect_fail reversed intent-svc-agents intent-svc-transfer

# intentd reaches features through intent-app.
new_workspace intentd_direct intentd 'intent-svc-transfer = { path = "../intent-svc-transfer" }'
expect_fail intentd_direct intentd intent-svc-transfer

# A renamed dependency is still caught by its package name.
new_workspace renamed intent-services 'svc = { package = "intent-svc-scm", path = "../intent-svc-scm" }'
expect_fail renamed intent-services intent-svc-scm

# An unreadable workspace is an error, not a pass.
set +e
out=$(CARGO_METADATA_FLAGS=--offline python3 -I -B "$check" "$tmp/missing/Cargo.toml" 2>&1)
rc=$?
set -e
[ "$rc" -eq 2 ] || fail "missing manifest: expected exit 2, got $rc: $out"

# This repo must obey the layering.
python3 -I -B "$check" >/dev/null || fail "this repo: check-feature-crate-deps.py failed"

if [ "$failures" -ne 0 ]; then
  echo "$failures test(s) failed" >&2
  exit 1
fi
echo "ok: check-feature-crate-deps.py regression tests passed"
