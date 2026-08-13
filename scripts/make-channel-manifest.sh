#!/usr/bin/env bash
# Build a channel manifest (stable.json / beta.json / alpha.json) for an
# intentd release.
#
# Usage: make-channel-manifest.sh <tag> <channel> <output-file>
#
# <channel> is "stable", "beta", or "alpha". The caller decides the routing:
# CI writes alpha.json for every release (publish-channel-manifest.yml),
# beta.json when a release is promoted to beta (promote-beta.yml), and
# stable.json when a release is promoted to stable (promote-stable.yml).
#
# Reads the GitHub Release for <tag> (via `gh`), pairs every platform archive
# (intentd-<target>.tar.xz / .tar.gz / .zip) with its .sha256 sidecar, and writes
# a JSON manifest. Requires: gh (authenticated via GH_TOKEN), jq, awk.
#
# The release is read from GITHUB_REPOSITORY (default intent-hq/intentd). Set
# ASSET_REPO to point the platform `url`s at a different repo hosting the same
# assets under the same tag (e.g. the public intent-hq/intentd-releases
# mirror); it defaults to the repo the release is read from.
#
# Manifest schema (schema version 1):
# {
#   "schema": 1,
#   "channel": "stable" | "beta" | "alpha",
#   "version": "0.1.0",
#   "tag": "v0.1.0",
#   "published_at": "2026-07-21T00:00:00Z",
#   "platforms": {
#     "<rust-target-triple>": {
#       "asset": "intentd-<triple>.tar.xz",
#       "url": "https://github.com/<repo>/releases/download/<tag>/<asset>",
#       "sha256": "<hex digest of the archive>"
#     }
#   }
# }
set -euo pipefail

usage="usage: make-channel-manifest.sh <tag> <channel> <output-file>"
TAG="${1:?$usage}"
CHANNEL="${2:?$usage}"
OUT="${3:?$usage}"
REPO="${GITHUB_REPOSITORY:-intent-hq/intentd}"
ASSET_REPO="${ASSET_REPO:-$REPO}"

if [[ "$CHANNEL" != "stable" && "$CHANNEL" != "beta" && "$CHANNEL" != "alpha" ]]; then
  echo "error: channel must be 'stable', 'beta', or 'alpha', got: $CHANNEL" >&2
  exit 1
fi
# Leading v is optional to match dist's tag parsing (release tags are vX.Y.Z;
# v-release.yml only triggers on v-prefixed tags via tag-namespace).
if [[ ! "$TAG" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+(-.+)?$ ]]; then
  echo "error: tag must look like [v]X.Y.Z or [v]X.Y.Z-<prerelease>, got: $TAG" >&2
  exit 1
fi
VERSION="${TAG#v}"

release_json=$(gh release view "$TAG" --repo "$REPO" --json assets,publishedAt)
published_at=$(jq -r '.publishedAt // empty' <<<"$release_json")
if [[ -z "$published_at" ]]; then
  echo "error: release $TAG has no publishedAt (draft/unpublished?)" >&2
  exit 1
fi

# Platform archives look like intentd-<target-triple>.tar.xz / .tar.gz / .zip.
# Capture jq output via command substitution (not process substitution) so a
# jq parse error stops the script instead of looking like "no archives".
archive_names=$(jq -r \
  '.assets[].name | select(test("^intentd-[a-z0-9_]+-[a-z0-9-]+\\.(tar\\.xz|tar\\.gz|zip)$"))' \
  <<<"$release_json")
mapfile -t archives <<<"$archive_names"
if [[ ${#archives[@]} -eq 1 && -z "${archives[0]}" ]]; then
  archives=()
fi

if [[ ${#archives[@]} -eq 0 ]]; then
  echo "error: no intentd platform archives found on release $TAG" >&2
  exit 1
fi

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

platforms="{}"
for asset in "${archives[@]}"; do
  target="${asset#intentd-}"
  target="${target%.tar.xz}"
  target="${target%.tar.gz}"
  target="${target%.zip}"

  gh release download "$TAG" --repo "$REPO" --pattern "${asset}.sha256" \
    --dir "$tmpdir" --clobber
  sha256=$(awk '{print $1}' "$tmpdir/${asset}.sha256")
  if [[ ! "$sha256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "error: bad sha256 for $asset: $sha256" >&2
    exit 1
  fi

  url="https://github.com/${ASSET_REPO}/releases/download/${TAG}/${asset}"
  platforms=$(jq --arg t "$target" --arg a "$asset" --arg u "$url" --arg s "$sha256" \
    '.[$t] = {asset: $a, url: $u, sha256: $s}' <<<"$platforms")
done

jq -n \
  --arg channel "$CHANNEL" \
  --arg version "$VERSION" \
  --arg tag "$TAG" \
  --arg published_at "$published_at" \
  --argjson platforms "$platforms" \
  '{schema: 1, channel: $channel, version: $version, tag: $tag, published_at: $published_at, platforms: $platforms}' \
  >"$OUT"

echo "wrote $OUT:" >&2
cat "$OUT" >&2
