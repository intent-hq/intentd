#!/usr/bin/env bash
# Upload a channel manifest to its fixed channel release, creating the release
# if it does not exist yet.
#
# Usage: publish-channel-manifest.sh <channel> <manifest-file>
#
# <channel> is "stable" or "beta"; the manifest lands as an asset on the
# `channel-<channel>` release (created with --latest=false so it never shadows
# real releases). Requires: gh (authenticated via GH_TOKEN).
set -euo pipefail

usage="usage: publish-channel-manifest.sh <channel> <manifest-file>"
CHANNEL="${1:?$usage}"
MANIFEST="${2:?$usage}"
REPO="${GITHUB_REPOSITORY:-intent-hq/intentd}"

if [[ "$CHANNEL" != "stable" && "$CHANNEL" != "beta" ]]; then
  echo "error: channel must be 'stable' or 'beta', got: $CHANNEL" >&2
  exit 1
fi
if [[ ! -f "$MANIFEST" ]]; then
  echo "error: manifest file not found: $MANIFEST" >&2
  exit 1
fi

channel_tag="channel-$CHANNEL"
if ! gh release view "$channel_tag" --repo "$REPO" >/dev/null 2>&1; then
  if ! gh release create "$channel_tag" \
    --repo "$REPO" \
    --latest=false \
    --title "Channel manifest: $CHANNEL" \
    --notes "Machine-readable pointer to the latest $CHANNEL intentd release. Do not consume the tag itself; download the $CHANNEL.json asset."
  then
    # Tolerate only a lost create race (two publishes close together): the
    # release must exist now; otherwise fail loudly.
    if ! gh release view "$channel_tag" --repo "$REPO" >/dev/null 2>&1; then
      echo "error: failed to create release $channel_tag and it does not exist" >&2
      exit 1
    fi
  fi
fi
gh release upload "$channel_tag" \
  "$MANIFEST" \
  --repo "$REPO" \
  --clobber
echo "uploaded $MANIFEST to $channel_tag" >&2
