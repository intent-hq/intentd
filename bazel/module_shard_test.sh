#!/usr/bin/env bash
# Runs the tests of a libtest binary that are under a module path in INCLUDE
# (when non-empty) and under none in EXCLUDE. A module path is a top-level
# module (`tests`) or a nested one (`tests::rules`); a test is under it when its
# name starts with the path followed by `::` (or is the path itself).
# shared_binary_test (bazel/shared_binary_test.bzl) expands the @@ markers.
#
# libtest's own filters are substring matches, so a module filter such as
# `tests::` would also select (or, with --skip, drop) every nested
# `foo::tests::`. This lists the binary's tests, picks by real module path,
# and runs the picks with --exact. Other arguments (--skip patterns,
# --test-threads) apply to the listing and are forwarded to the run.
#
# Plain bash on purpose: an external filter (awk) failing inside the
# selection would go unseen by `set -e` and leave every shard silently empty.
set -euo pipefail

binary=@@BINARY@@
include=(@@INCLUDE@@)
exclude=(@@EXCLUDE@@)

# True when test $1 is under one of the module paths that follow it.
under() {
  local name="$1" path
  shift
  for path in "$@"; do
    case "$name" in
      "$path" | "$path::"*) return 0 ;;
    esac
  done
  return 1
}

listing="$("$binary" --list --format terse "$@")"
names=()
while IFS= read -r line; do
  case "$line" in
    *": test") ;;
    *) continue ;;
  esac
  name="${line%: test}"
  if [ "${#include[@]}" -gt 0 ] && ! under "$name" "${include[@]}"; then
    continue
  fi
  if [ "${#exclude[@]}" -gt 0 ] && under "$name" "${exclude[@]}"; then
    continue
  fi
  names+=("$name")
done <<< "$listing"

if [ "${#names[@]}" -eq 0 ]; then
  echo "module_shard_test: no tests in this shard's modules"
  exit 0
fi
exec "$binary" --exact "${names[@]}" "$@"
