#!/usr/bin/env bash
# Upload a channel manifest to its fixed channel release, creating the release
# if it does not exist yet.
#
# Usage: publish-channel-manifest.sh <channel> <manifest-file> [repo]
#
# <channel> is "stable", "beta", or "alpha"; the manifest lands as an asset on
# the `channel-<channel>` release (created with --latest=false so it never shadows
# real releases). [repo] defaults to GITHUB_REPOSITORY; pass it explicitly to
# publish to another repo (e.g. the public intent-hq/intentd-releases mirror).
# Requires: gh (authenticated via GH_TOKEN) and an explicit repo — no default,
# so a local run can never push a manifest to the upstream repo by accident.
set -euo pipefail

usage="usage: publish-channel-manifest.sh <channel> <manifest-file> [repo]"
CHANNEL="${1:?$usage}"
MANIFEST="${2:?$usage}"
REPO="${3:-${GITHUB_REPOSITORY:?GITHUB_REPOSITORY (owner/repo) must be set (or pass [repo])}}"

if [[ "$CHANNEL" != "stable" && "$CHANNEL" != "beta" && "$CHANNEL" != "alpha" ]]; then
  echo "error: channel must be 'stable', 'beta', or 'alpha', got: $CHANNEL" >&2
  exit 1
fi
if [[ ! -f "$MANIFEST" ]]; then
  echo "error: manifest file not found: $MANIFEST" >&2
  exit 1
fi

channel_tag="channel-$CHANNEL"
# Pin the channel tag to a deterministic commit in CI (GITHUB_SHA); locally gh
# falls back to the default branch HEAD. The tag itself is never consumed.
# GITHUB_SHA only exists in the repo the workflow runs in, so skip the pin
# when publishing to a different repo (the mirror has unrelated history).
target_args=()
if [[ -n "${GITHUB_SHA:-}" && "$REPO" == "${GITHUB_REPOSITORY:-}" ]]; then
  target_args=(--target "$GITHUB_SHA")
fi
# A failed tag lookup (even HTTP 404) can mean missing repository access, not
# a missing release. Only a successful, complete listing establishes absence.
# --paginate also finds old channel releases and drafts visible to this token.
lookup_release() {
  local release_tags
  if ! release_tags=$(gh api --paginate "repos/$REPO/releases?per_page=100" --jq '.[].tag_name'); then
    echo "error: could not list releases on $REPO; cannot determine whether $channel_tag exists" >&2
    return 1
  fi
  release_found=false
  if grep -Fxq -- "$channel_tag" <<<"$release_tags"; then
    release_found=true
  fi
}

lookup_release
if [[ "$release_found" != true ]]; then
  if ! gh release create "$channel_tag" \
    --repo "$REPO" \
    --latest=false \
    "${target_args[@]}" \
    --title "Channel manifest: $CHANNEL" \
    --notes "Machine-readable pointer to the latest $CHANNEL intentd release. Do not consume the tag itself; download the $CHANNEL.json asset."
  then
    # Tolerate only a lost create race (two publishes close together): the
    # release must exist now; otherwise fail loudly.
    if ! lookup_release; then
      echo "error: failed to create release $channel_tag and could not verify its existence" >&2
      exit 1
    fi
    if [[ "$release_found" != true ]]; then
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

# Re-stamp published_at so the channel release reflects when its manifest was
# last refreshed (GitHub only sets published_at on the draft->published
# transition, never on asset uploads).
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
"$script_dir/refresh-release-published-at.sh" "$channel_tag" "$REPO"
