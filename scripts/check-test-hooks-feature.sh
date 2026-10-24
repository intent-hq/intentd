#!/usr/bin/env bash
# Fail when a `test-hooks` cargo feature is on in the production feature
# graph of a shipped binary. `test-hooks` compiles test seams and fixtures
# into a library (intent-services exposes `test_support` and the
# `cfg(any(test, feature = "test-hooks"))` items under it) so feature crates
# can use them from their tests. Only dev-dependencies may enable it; CI
# (ci.yml check job) runs this guard.
#
# The graph is `cargo tree -e normal,build,features --target all`: normal and
# build edges for every target platform, with dev-dependencies left out of
# both the edges and the feature resolution.
#
# Usage: ./scripts/check-test-hooks-feature.sh [manifest-path [package...]]
#   default manifest: this repo's Cargo.toml; default packages: intentd and
#   intentd-sitter.
#   CARGO_TREE_FLAGS: extra cargo flags (default --locked).
# Exit 0 when no crate enables it, 1 when one does (one ::error line per
# crate), 2 when the graph cannot be read.
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
manifest=${1:-"$repo/Cargo.toml"}
if [ "$#" -gt 0 ]; then
  shift
fi
if [ "$#" -gt 0 ]; then
  packages=("$@")
else
  packages=(intentd intentd-sitter)
fi

read -r -a flags <<<"${CARGO_TREE_FLAGS---locked}"
args=()
for package in "${packages[@]}"; do
  args+=(-p "$package")
done

# Run from the manifest's directory: cargo reads .cargo/config.toml from the
# invocation directory upward, and another workspace's config (patches,
# sources) must not leak into this graph.
if ! tree=$(cd "$(dirname "$manifest")" 2>/dev/null &&
  cargo tree "${flags[@]}" --manifest-path "$manifest" "${args[@]}" \
    -e normal,build,features --target all --prefix none); then
  echo "error: cannot read the production feature graph of ${packages[*]}" >&2
  exit 2
fi

# A feature node prints as `<crate> feature "<name>"`.
offenders=$(printf '%s\n' "$tree" | sed -nE 's/^([A-Za-z0-9_-]+) feature "test-hooks"$/\1/p' | sort -u)
if [ -z "$offenders" ]; then
  echo "ok: no test-hooks feature in the production graph of ${packages[*]}"
  exit 0
fi

while IFS= read -r crate; do
  echo "::error::$crate feature \"test-hooks\" is enabled in the production graph of ${packages[*]}. Enable it from [dev-dependencies] only; find the edge with: cargo tree -e normal,build,features --target all -i $crate"
done <<<"$offenders"
exit 1
