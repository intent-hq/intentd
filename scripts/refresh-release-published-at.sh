#!/usr/bin/env bash
# Re-stamp a release's published_at by toggling it through draft and back.
#
# Usage: refresh-release-published-at.sh <tag> [repo]
#
# GitHub sets published_at only on the draft->published transition, so rolling
# releases that are refreshed in place (channel-beta / channel-stable /
# sitter-latest) keep their original publish date forever and look stale in
# the releases UI. PATCHing draft:true then draft:false re-publishes the
# release, refreshing published_at without touching the tag, assets, or notes.
#
# The un-draft re-applies the release's prior `prerelease` flag and always
# sends make_latest:false — every rolling release is created with
# --latest=false and must never shadow real releases. The refresh is cosmetic,
# so any failure BEFORE the release is drafted (missing release, transient API
# error on the GET or the draft:true PATCH) is fail-soft: warn and exit 0, so
# a hiccup here can never block a release pipeline. Once the release IS
# drafted, re-publish is mandatory: the un-draft is retried (5 attempts with
# backoff) and the script exits non-zero if the release is left drafted, since
# a drafted release is invisible to consumers. A release found already drafted
# (an earlier run interrupted mid-toggle) is repaired the same way — straight
# to the mandatory re-publish, no re-draft. [repo] defaults to
# GITHUB_REPOSITORY; pass it explicitly to refresh a release on another repo
# (e.g. the public intent-hq/intentd-releases mirror).
# Requires: gh (authenticated via GH_TOKEN) and jq.
set -euo pipefail

usage="usage: refresh-release-published-at.sh <tag> [repo]"
TAG="${1:?$usage}"
REPO="${2:-${GITHUB_REPOSITORY:?GITHUB_REPOSITORY (owner/repo) must be set (or pass [repo])}}"

# releases/tags/<tag> resolves only published releases, which is what a
# refresh needs (a draft has no published_at to refresh) — but it also means a
# release left drafted by an interrupted previous run is invisible here. So on
# a miss, look for a draft with this tag via the releases list (the
# authenticated API returns drafts) and repair it: skip the draft:true PATCH
# and go straight to the mandatory re-publish. Any other miss is fail-soft:
# the release is still published, so skipping the cosmetic refresh must not
# fail the caller.
already_drafted=false
if ! release_json=$(gh api "repos/$REPO/releases/tags/$TAG"); then
  stuck_draft=""
  if releases_json=$(gh api --paginate "repos/$REPO/releases?per_page=100"); then
    # -s slurps the per-page arrays --paginate emits; .[][] flattens them.
    stuck_draft=$(jq -cs --arg tag "$TAG" \
      '[.[][] | select(.draft and .tag_name == $tag)] | first // empty' \
      <<<"$releases_json")
  fi
  if [[ -z "$stuck_draft" ]]; then
    echo "warning: no release for tag $TAG on $REPO (or API error); skipping published_at refresh" >&2
    exit 0
  fi
  echo "warning: release $TAG on $REPO is stuck in DRAFT (interrupted earlier refresh?); re-publishing it" >&2
  release_json="$stuck_draft"
  already_drafted=true
fi
release_id=$(jq -r '.id // empty' <<<"$release_json")
prerelease=$(jq -r '.prerelease' <<<"$release_json")
if [[ -z "$release_id" ]]; then
  echo "warning: could not resolve release id for tag $TAG on $REPO; skipping published_at refresh" >&2
  exit 0
fi

# Fail-soft too: a failed draft:true PATCH leaves the release published, so
# there is nothing to repair — skip the refresh instead of failing the caller.
if [[ "$already_drafted" != "true" ]]; then
  if ! gh api --method PATCH "repos/$REPO/releases/$release_id" \
    -F draft=true --silent; then
    echo "warning: failed to draft release $TAG on $REPO; skipping published_at refresh" >&2
    exit 0
  fi
fi

# From here the release is drafted; every failure path below must end in the
# loud non-zero exit so a release can never silently stay in draft.
undrafted=false
for attempt in 1 2 3 4 5; do
  # make_latest is a string enum ("true"/"false"/"legacy"), hence -f not -F.
  if patched=$(gh api --method PATCH "repos/$REPO/releases/$release_id" \
      -F draft=false -F "prerelease=$prerelease" -f make_latest=false) \
    && [[ $(jq -r '.draft' <<<"$patched") == "false" ]]; then
    undrafted=true
    break
  fi
  echo "warning: un-draft attempt $attempt/5 failed for $TAG on $REPO; retrying" >&2
  sleep $((attempt * 2))
done

if [[ "$undrafted" != "true" ]]; then
  echo "error: release $TAG on $REPO is stuck in DRAFT after 5 un-draft attempts;" \
    "fix manually: gh release edit $TAG --repo $REPO --draft=false" >&2
  exit 1
fi
echo "refreshed published_at on $TAG ($REPO)" >&2
