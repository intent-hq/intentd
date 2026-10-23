#!/usr/bin/env bash
# Runs the tests of a libtest binary whose top-level module (the name up to the
# first `::`) is in INCLUDE (when non-empty) and not in EXCLUDE.
# shared_binary_test (bazel/shared_binary_test.bzl) expands the @@ markers.
#
# libtest's own filters are substring matches, so a module filter such as
# `tests::` would also select (or, with --skip, drop) every nested
# `foo::tests::`. This lists the binary's tests, picks by real top-level
# module, and runs the picks with --exact. Other arguments (--skip patterns,
# --test-threads) apply to the listing and are forwarded to the run.
#
# Plain bash on purpose: an external filter (awk) failing inside the
# selection would go unseen by `set -e` and leave every shard silently empty.
set -euo pipefail

binary=@@BINARY@@
include=" @@INCLUDE@@ "
exclude=" @@EXCLUDE@@ "

listing="$("$binary" --list --format terse "$@")"
names=()
while IFS= read -r line; do
  case "$line" in
    *": test") ;;
    *) continue ;;
  esac
  name="${line%: test}"
  top="${name%%::*}"
  if [ "$include" != "  " ]; then
    case "$include" in
      *" $top "*) ;;
      *) continue ;;
    esac
  fi
  case "$exclude" in
    *" $top "*) continue ;;
  esac
  names+=("$name")
done <<< "$listing"

if [ "${#names[@]}" -eq 0 ]; then
  echo "module_shard_test: no tests in this shard's modules"
  exit 0
fi
exec "$binary" --exact "${names[@]}" "$@"
