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
set -euo pipefail

binary=@@BINARY@@
include=" @@INCLUDE@@ "
exclude=" @@EXCLUDE@@ "

listing="$("$binary" --list --format terse "$@")"
names=()
while IFS= read -r name; do
  [ -n "$name" ] && names+=("$name")
done <<EOF
$(printf '%s\n' "$listing" | awk -v include="$include" -v exclude="$exclude" '
  / test$/ {
    name = substr($0, 1, length($0) - length(": test"))
    top = name
    sub(/::.*/, "", top)
    if (include != "  " && index(include, " " top " ") == 0) next
    if (index(exclude, " " top " ") > 0) next
    print name
  }')
EOF

if [ "${#names[@]}" -eq 0 ]; then
  echo "module_shard_test: no tests in this shard's modules"
  exit 0
fi
exec "$binary" --exact "${names[@]}" "$@"
