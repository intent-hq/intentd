#!/usr/bin/env bash
# Render packaging/homebrew/intentd.rb.template into a publishable formula,
# filling {{VERSION}} and the per-platform {{SHA256_*}} placeholders from the
# sitter release archives built by .github/workflows/release-sitter.yml.
#
# Usage: scripts/render-sitter-homebrew-formula.sh <version> <artifacts-dir> <output>
#   <version>       sitter version (release tag without the "sitter-v" prefix)
#   <artifacts-dir> directory containing the four unix archives
#                   intentd-<triple>.tar.xz (darwin + linux musl, both arches)
#   <output>        path to write the rendered intentd.rb
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <version> <artifacts-dir> <output>" >&2
  exit 2
fi

version=$1
artifacts=$2
output=$3

# The version lands inside sed replacements and formula strings — keep it to
# the same shape the release workflow enforces for tags.
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]]; then
  echo "error: '$version' is not a valid sitter version (expected X.Y.Z[-prerelease])" >&2
  exit 1
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
template="$repo_root/packaging/homebrew/intentd.rb.template"

sha256_of() {
  local archive="$artifacts/intentd-$1.tar.xz"
  if [[ ! -f "$archive" ]]; then
    echo "error: missing archive $archive" >&2
    exit 1
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$archive" | cut -d' ' -f1
  else
    shasum -a 256 "$archive" | cut -d' ' -f1
  fi
}

sha_darwin_arm=$(sha256_of aarch64-apple-darwin)
sha_darwin_x86=$(sha256_of x86_64-apple-darwin)
sha_linux_arm=$(sha256_of aarch64-unknown-linux-musl)
sha_linux_x86=$(sha256_of x86_64-unknown-linux-musl)

sed \
  -e "s/{{VERSION}}/$version/g" \
  -e "s/{{SHA256_AARCH64_APPLE_DARWIN}}/$sha_darwin_arm/g" \
  -e "s/{{SHA256_X86_64_APPLE_DARWIN}}/$sha_darwin_x86/g" \
  -e "s/{{SHA256_AARCH64_UNKNOWN_LINUX_MUSL}}/$sha_linux_arm/g" \
  -e "s/{{SHA256_X86_64_UNKNOWN_LINUX_MUSL}}/$sha_linux_x86/g" \
  "$template" >"$output"

if grep -E '\{\{[A-Z0-9_]+\}\}' "$output" >/dev/null; then
  echo "error: unrendered placeholders remain in $output" >&2
  exit 1
fi
