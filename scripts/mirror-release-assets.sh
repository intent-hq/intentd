#!/usr/bin/env bash
# Mirror an intentd release's platform archives + .sha256 sidecars to an
# identically-tagged release on the public mirror repo.
#
# Usage: mirror-release-assets.sh <tag>
#
# Reads the release for <tag> from SOURCE_REPO (via gh, authenticated with
# GH_TOKEN), downloads every platform archive (intentd-<target>.tar.xz /
# .tar.gz / .zip) plus its .sha256 sidecar, and uploads them to a release with
# the same tag on DEST_REPO, creating that release if it does not exist yet.
# Uploads use --clobber, so re-runs are idempotent (promote-stable relies on
# this to backfill releases cut before mirroring existed). Requires: gh, jq.
#
# Env:
#   SOURCE_REPO     repo to read the release from (default: intent-hq/intentd)
#   DEST_REPO       repo to mirror to (required; no default so a local run can
#                   never push to the public mirror by accident)
#   DEST_GH_TOKEN   token with contents:write on DEST_REPO (required)
set -euo pipefail

usage="usage: mirror-release-assets.sh <tag>"
TAG="${1:?$usage}"
SOURCE_REPO="${SOURCE_REPO:-intent-hq/intentd}"
DEST_REPO="${DEST_REPO:?DEST_REPO (owner/repo) must be set}"
: "${DEST_GH_TOKEN:?DEST_GH_TOKEN must be set (contents:write on DEST_REPO)}"

# Same tag shapes make-channel-manifest.sh accepts.
if [[ ! "$TAG" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+(-.+)?$ ]]; then
  echo "error: tag must look like [v]X.Y.Z or [v]X.Y.Z-<prerelease>, got: $TAG" >&2
  exit 1
fi

release_json=$(gh release view "$TAG" --repo "$SOURCE_REPO" --json assets,isPrerelease,publishedAt)
if [[ -z $(jq -r '.publishedAt // empty' <<<"$release_json") ]]; then
  echo "error: release $TAG on $SOURCE_REPO has no publishedAt (draft/unpublished?)" >&2
  exit 1
fi
is_prerelease=$(jq -r '.isPrerelease' <<<"$release_json")

# Platform archives (same pattern as make-channel-manifest.sh) plus their
# .sha256 sidecars. Command substitution (not process substitution) so a jq
# parse error stops the script instead of looking like "no assets".
asset_names=$(jq -r \
  '.assets[].name | select(test("^intentd-[a-z0-9_]+-[a-z0-9-]+\\.(tar\\.xz|tar\\.gz|zip)(\\.sha256)?$"))' \
  <<<"$release_json")
mapfile -t assets <<<"$asset_names"
if [[ ${#assets[@]} -eq 1 && -z "${assets[0]}" ]]; then
  assets=()
fi
if [[ ${#assets[@]} -eq 0 ]]; then
  echo "error: no intentd platform archives found on release $TAG of $SOURCE_REPO" >&2
  exit 1
fi

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

for asset in "${assets[@]}"; do
  gh release download "$TAG" --repo "$SOURCE_REPO" --pattern "$asset" \
    --dir "$tmpdir" --clobber
done

if ! GH_TOKEN="$DEST_GH_TOKEN" gh release view "$TAG" --repo "$DEST_REPO" >/dev/null 2>&1; then
  prerelease_args=()
  if [[ "$is_prerelease" == "true" ]]; then
    prerelease_args=(--prerelease)
  fi
  # --latest=false so a backfilled old version never grabs the Latest badge;
  # consumers discover releases via the channel manifests, not via Latest.
  if ! GH_TOKEN="$DEST_GH_TOKEN" gh release create "$TAG" \
    --repo "$DEST_REPO" \
    --latest=false \
    "${prerelease_args[@]}" \
    --title "intentd $TAG" \
    --notes "Mirror of the $TAG intentd release: platform archives and .sha256 sidecars for the daemon auto-updater."
  then
    # Tolerate only a lost create race (two mirrors close together): the
    # release must exist now; otherwise fail loudly.
    if ! GH_TOKEN="$DEST_GH_TOKEN" gh release view "$TAG" --repo "$DEST_REPO" >/dev/null 2>&1; then
      echo "error: failed to create release $TAG on $DEST_REPO and it does not exist" >&2
      exit 1
    fi
  fi
fi

GH_TOKEN="$DEST_GH_TOKEN" gh release upload "$TAG" \
  "$tmpdir"/* \
  --repo "$DEST_REPO" \
  --clobber
echo "mirrored ${#assets[@]} assets from $SOURCE_REPO@$TAG to $DEST_REPO@$TAG" >&2
