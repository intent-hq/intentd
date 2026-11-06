#!/usr/bin/env bash
# Regression tests for check-rdeps-parity.py: runs the real check against
# throwaway cargo workspaces (path dependencies only, so no network), each
# with a copy of changed-tests.sh at scripts/ so the scanner reads that
# workspace's crates. Every manifest style the scanner supports must pass. A
# scanner with its `package =` branches removed (the gap intentd#2704 fixed)
# must fail and name the dropped crates and edges; so must a scanner that
# over-reports (a registry dependency named like a member), one that
# propagates from no root or an unknown one, and a member outside crates/. Unreadable metadata or scanner output exits 2. Finally the
# check must pass on this repo.
#
# Run directly: ./scripts/test-check-rdeps-parity.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
check="$here/check-rdeps-parity.py"
scanner="$here/changed-tests.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

failures=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

# crate NAME [MANIFEST TAIL]: add crates/NAME to the current workspace.
crate() {
  mkdir -p "$ws/crates/$1/src"
  : >"$ws/crates/$1/src/lib.rs"
  printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\n\n%s\n' "$1" "${2-}" >"$ws/crates/$1/Cargo.toml"
}

# new_workspace NAME: a workspace with one crate per supported manifest style,
# every one reaching intent-core (directly or transitively) except iota.
new_workspace() {
  ws="$tmp/$1"
  mkdir -p "$ws/scripts"
  cp "$scanner" "$ws/scripts/changed-tests.sh"
  cat >"$ws/Cargo.toml" <<'TOML'
[workspace]
resolver = "2"
members = ["crates/*"]

[workspace.dependencies]
intent-core = { path = "crates/intent-core" }
core = { package = "intent-core", path = "crates/intent-core" }
TOML
  crate intent-core
  crate alpha $'[dependencies]\nintent-core = { path = "../intent-core", version = "0.1" }'
  crate beta $'[dependencies]\n# A comment naming gamma = { path = "../gamma" } is not an edge.\nintent-core.workspace = true'
  crate gamma $'[dependencies]\ncore = { package = "intent-core", path = "../intent-core/" }'
  crate delta $'[dev-dependencies]\ncore = { package = "intent-core", workspace = true }'
  crate epsilon $'[dependencies.intent-core]\nworkspace = true'
  crate zeta $'[target.\'cfg(unix)\'.dependencies]\nalpha = { path = "../alpha" }'
  crate eta $'[build-dependencies]\nbeta = { path = "../beta" }'
  crate theta $'[dependencies.core]\npackage = "intent-core"\nworkspace = true'
  crate lambda $'[target."cfg(unix)".dev-dependencies]\nalpha = { path = "../alpha" }'
  crate mu $'[target."cfg(windows)".build-dependencies]\ngamma = { path = "../gamma" }'
  crate nu $'[dependencies.delta]\npath = "../delta"'
  crate iota
}

# run_check NAME: run the check on workspace NAME from its own directory
# (cargo reads .cargo/config.toml upward from where it runs).
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

# expect_fail NAME CODE NEEDLE...: exit CODE, output containing every NEEDLE.
expect_fail() {
  local name=$1 code=$2 needle
  shift 2
  run_check "$name"
  [ "$rc" -eq "$code" ] || fail "$name: expected exit $code, got $rc: $out"
  for needle in "$@"; do
    case $out in
      *"$needle"*) ;;
      *) fail "$name: output does not name '$needle': $out" ;;
    esac
  done
}

# break_scanner NAME OLD [NEW]: replace the exact text OLD in NAME's scanner
# copy with NEW (default: remove it), failing loudly when OLD is gone (the
# scanner changed shape: update OLD).
break_scanner() {
  python3 -I -B - "$tmp/$1/scripts/changed-tests.sh" "$2" "${3-}" <<'PY' || fail "$1: mutation target not found in changed-tests.sh"
import pathlib, sys
path, old, new = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
text = path.read_text()
if text.count(old) != 1:
    sys.exit(1)
path.write_text(text.replace(old, new))
PY
}

new_workspace every-style
expect_pass every-style
case $out in
  *"(11 workspace dependency edges; intent-core: 11 reverse dependencies)"*) ;;
  *) fail "every-style: unexpected summary: $out" ;;
esac

# The inline rename without a path (`core = { package = "intent-core",
# workspace = true }`) and the [dependencies.<key>] rename are each read by a
# `package =` branch. Without them delta and theta drop out of intent-core's
# reverse dependencies, and the check names both.
new_workspace no-package-branch
break_scanner no-package-branch '          elif [[ "$line" == *package*=*\"*\"* ]]; then
            # A rename inheriting from [workspace.dependencies] has no path.
            dep=${line#*package*=*\"}
            dep=${dep%%\"*}
'
break_scanner no-package-branch '          elif [[ "$line" == package*=*\"*\"* ]]; then
            dep=${line#*\"}
            dep=${dep%%\"*}
'
expect_fail no-package-branch 1 \
  "delta -> intent-core (dev dependency) is missing from the scanner" \
  "theta -> intent-core (normal dependency) is missing from the scanner" \
  "intent-core reverse dependencies differ from cargo metadata: missing delta, nu, theta"

# The reverse-dependency comparison cannot be skipped by a scanner that
# propagates from no root, or from a name that is not a member.
new_workspace no-root
break_scanner no-root 'rdeps_roots="intent-core"' 'rdeps_roots=""'
expect_fail no-root 1 "--print-rdeps names no root"
new_workspace misspelled-root
break_scanner misspelled-root 'rdeps_roots="intent-core"' 'rdeps_roots="intent_core"'
expect_fail misspelled-root 1 "propagates from intent_core, which is not a workspace member"

# A registry dependency that shares a member's name is not a workspace edge;
# the scanner (which matches names against crates/<dir>) over-reports it.
new_workspace registry-namesake
printf '[dependencies]\nintent-core = "1"\n' >>"$ws/crates/iota/Cargo.toml"
expect_fail registry-namesake 1 \
  "iota -> intent-core is reported by the scanner but is not a path dependency in cargo metadata" \
  "intent-core reverse dependencies differ from cargo metadata: extra iota"

# A member outside crates/ is invisible to the scanner.
new_workspace outside-crates
mkdir -p "$ws/tools/kappa/src"
: >"$ws/tools/kappa/src/lib.rs"
printf '[package]\nname = "kappa"\nversion = "0.1.0"\nedition = "2021"\n' >"$ws/tools/kappa/Cargo.toml"
sed -i.bak 's|members = \["crates/\*"\]|members = ["crates/*", "tools/kappa"]|' "$ws/Cargo.toml"
expect_fail outside-crates 1 "workspace member kappa"

new_workspace bad-metadata
echo 'not toml [' >>"$ws/Cargo.toml"
expect_fail bad-metadata 2 "cannot read the workspace metadata"

new_workspace no-scanner
rm "$ws/scripts/changed-tests.sh"
expect_fail no-scanner 2 "--print-edges exited"

set +e
out=$(python3 -I -B "$check" 2>&1)
rc=$?
set -e
[ "$rc" -eq 0 ] || fail "this repo: expected exit 0, got $rc: $out"

if [ "$failures" -ne 0 ]; then
  echo "check-rdeps-parity tests: $failures failure(s)" >&2
  exit 1
fi
echo "check-rdeps-parity tests passed"
