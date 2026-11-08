#!/usr/bin/env bash
# Install `dist` at dist-workspace.toml's cargo-dist-version into DIR.
#
# CI's release-scripts job needs the real binary. GitHub release downloads
# returned 504s for over an hour on 2026-11-08 (intentd#3156's own runs), so:
#   1. keep a DIR/dist that already reports the pin (restored by actions/cache);
#   2. otherwise run cargo-dist's installer through install-dist.sh's retries;
#   3. otherwise build the same version from crates.io with `cargo install`.
#
# Usage: scripts/install-cargo-dist.sh DIR
# INSTALL_DIST_DELAY is passed through to install-dist.sh (tests set 0).
set -euo pipefail

dir=${1:?usage: scripts/install-cargo-dist.sh DIR}
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root=$(cd "$script_dir/.." && pwd -P)
version=$(python3 -c 'import sys, tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["dist"]["cargo-dist-version"])' "$root/dist-workspace.toml" 2>/dev/null || true)
if [ -z "$version" ]; then
  echo "::error::install-cargo-dist.sh: no cargo-dist-version in dist-workspace.toml" >&2
  exit 2
fi

installed() {
  local out
  [ -x "$dir/dist" ] || return 1
  out=$("$dir/dist" --version 2>/dev/null) || return 1
  case " $out " in *" $version "*) return 0 ;; esac
  return 1
}

if installed; then
  echo "install-cargo-dist: using cached dist $version at $dir/dist"
  exit 0
fi
mkdir -p "$dir"
url="https://github.com/axodotdev/cargo-dist/releases/download/v${version}/cargo-dist-installer.sh"
if INSTALL_DIST_SHELL=sh \
    INSTALL_DIST_RUN="curl --proto '=https' --tlsv1.2 --retry 2 --retry-connrefused -LsSf '$url' | CARGO_DIST_UNMANAGED_INSTALL='$dir' sh" \
    bash "$script_dir/install-dist.sh" && installed; then
  exit 0
fi
if ! command -v cargo >/dev/null 2>&1; then
  echo "::error::install-cargo-dist: the cargo-dist $version release download failed and cargo is not on PATH to build it" >&2
  exit 1
fi
echo "::warning::install-cargo-dist: the cargo-dist $version release download failed; building it from crates.io" >&2
staging=$(mktemp -d "$dir/.cargo-dist-staging.XXXXXX")
trap 'rm -rf "$staging"' EXIT
cargo install cargo-dist --locked --version "$version" --root "$staging" --bin dist
mv -f "$staging/bin/dist" "$dir/dist"
if ! installed; then
  echo "::error::install-cargo-dist: $dir/dist does not report cargo-dist $version" >&2
  exit 1
fi
echo "install-cargo-dist: built dist $version with cargo install at $dir/dist"
