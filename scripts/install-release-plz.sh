#!/usr/bin/env bash
# Put release-plz VERSION in DIR, for CI's release-scripts job.
#
# taiki-e/install-action downloads release-plz from GitHub releases; during a
# GitHub outage each 504 took minutes and the job hit its timeout (intentd#3156,
# job 128709740868). The workflow caches DIR by version and bounds the action
# with a step timeout; this script then:
#   1. keeps a DIR/release-plz that already reports VERSION (cache hit);
#   2. otherwise copies the one the action installed on PATH;
#   3. otherwise builds VERSION from crates.io with `cargo install`.
#
# Usage: scripts/install-release-plz.sh DIR VERSION
set -euo pipefail

dir=${1:?usage: scripts/install-release-plz.sh DIR VERSION}
version=${2:?usage: scripts/install-release-plz.sh DIR VERSION}

reports_pin() {
  local out
  out=$("$1" --version 2>/dev/null) || return 1
  case " $out " in *" $version "*) return 0 ;; esac
  return 1
}

mkdir -p "$dir"
if [ -x "$dir/release-plz" ] && reports_pin "$dir/release-plz"; then
  echo "install-release-plz: using cached release-plz $version at $dir/release-plz"
  exit 0
fi
if found=$(command -v release-plz 2>/dev/null) && reports_pin "$found"; then
  cp -f "$found" "$dir/release-plz"
  echo "install-release-plz: using release-plz $version from $found"
  exit 0
fi
if ! command -v cargo >/dev/null 2>&1; then
  echo "::error::install-release-plz: release-plz $version could not be downloaded and cargo is not on PATH to build it" >&2
  exit 1
fi
echo "::warning::install-release-plz: release-plz $version could not be downloaded; building it from crates.io" >&2
staging=$(mktemp -d "$dir/.release-plz-staging.XXXXXX")
trap 'rm -rf "$staging"' EXIT
cargo install release-plz --locked --version "$version" --root "$staging" --bin release-plz
mv -f "$staging/bin/release-plz" "$dir/release-plz"
if ! reports_pin "$dir/release-plz"; then
  echo "::error::install-release-plz: $dir/release-plz does not report release-plz $version" >&2
  exit 1
fi
echo "install-release-plz: built release-plz $version with cargo install at $dir/release-plz"
