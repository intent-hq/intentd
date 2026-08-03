#!/usr/bin/env bash
# Mirror a release's assets to an identically-tagged release on the public
# mirror repo.
#
# Usage: mirror-release-assets.sh <tag>
#
# Reads the release for <tag> from SOURCE_REPO (via gh, authenticated with
# GH_TOKEN), downloads every asset whose name matches ASSET_REGEX (default:
# the daemon platform archives intentd-<target>.tar.xz / .tar.gz / .zip plus
# their .sha256 sidecars), and uploads them to a release with the same tag on
# DEST_REPO, creating that release if it does not exist yet. Uploads use
# --clobber, so re-runs are idempotent (promote-stable relies on this to
# backfill releases cut before mirroring existed). Requires: gh, jq.
#
# Release notes: when RELEASE_NOTES is not set (daemon tags), the mirror
# release copies the source release body with asset download URLs rewritten
# from SOURCE_REPO to DEST_REPO (asset names are identical on the mirror), and
# re-runs sync those notes onto an already-existing mirror release
# (mirror-release.yml relies on this to backfill notes). When RELEASE_NOTES is
# set (sitter tags), the notes are used verbatim at create time only and
# existing releases are left untouched.
#
# Env:
#   SOURCE_REPO     repo to read the release from (default: intent-hq/intentd)
#   DEST_REPO       repo to mirror to (required; no default so a local run can
#                   never push to the public mirror by accident)
#   DEST_GH_TOKEN   token with contents:write on DEST_REPO (required)
#   ASSET_REGEX     jq test() regex selecting which asset names to mirror
#                   (default: daemon platform archives + .sha256 sidecars)
#   RELEASE_TITLE   title when creating the DEST_REPO release
#                   (default: "intentd <tag>")
#   RELEASE_NOTES   notes for the DEST_REPO release; when unset, the source
#                   release body is copied with download URLs rewritten to
#                   DEST_REPO (falling back to a short daemon mirror blurb if
#                   the source body is empty) and synced on re-runs
#   PRUNE_STALE     "true" to delete DEST_REPO release assets that match
#                   ASSET_REGEX but are absent from the source set — for
#                   refreshed fixed releases (e.g. sitter-latest) where a
#                   plain --clobber upload would leave stale assets behind
#                   (default: false)
set -euo pipefail

usage="usage: mirror-release-assets.sh <tag>"
TAG="${1:?$usage}"
SOURCE_REPO="${SOURCE_REPO:-intent-hq/intentd}"
DEST_REPO="${DEST_REPO:?DEST_REPO (owner/repo) must be set}"
: "${DEST_GH_TOKEN:?DEST_GH_TOKEN must be set (contents:write on DEST_REPO)}"
ASSET_REGEX="${ASSET_REGEX:-^intentd-[a-z0-9_]+-[a-z0-9-]+\\.(tar\\.xz|tar\\.gz|zip)(\\.sha256)?\$}"
RELEASE_TITLE="${RELEASE_TITLE:-intentd $TAG}"
# Track whether the caller set RELEASE_NOTES: explicit notes (sitter tags) are
# used verbatim at create time only; otherwise the source body is mirrored and
# kept in sync on re-runs.
notes_explicit=false
if [[ -n "${RELEASE_NOTES:-}" ]]; then
  notes_explicit=true
fi
PRUNE_STALE="${PRUNE_STALE:-false}"

# Daemon tag shapes (same as make-channel-manifest.sh) plus the sitter tags
# published by release-sitter.yml (sitter-vX.Y.Z and the fixed sitter-latest)
# and the guest-image tags published by release-guest-image.yml
# (guest-image-vX.Y.Z). Prerelease suffix is charset-restricted to semver
# identifiers so a tag that passes validation is safe to echo in logs (no
# workflow-command injection).
if [[ ! "$TAG" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ \
   && ! "$TAG" =~ ^sitter-v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ \
   && ! "$TAG" =~ ^guest-image-v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ \
   && "$TAG" != sitter-latest ]]; then
  echo "error: tag must look like [v]X.Y.Z[-<prerelease>], sitter-vX.Y.Z[-<prerelease>], guest-image-vX.Y.Z[-<prerelease>], or sitter-latest (prerelease limited to [0-9A-Za-z.-])" >&2
  exit 1
fi

release_json=$(gh release view "$TAG" --repo "$SOURCE_REPO" --json assets,body,isPrerelease,publishedAt)
if [[ -z $(jq -r '.publishedAt // empty' <<<"$release_json") ]]; then
  echo "error: release $TAG on $SOURCE_REPO has no publishedAt (draft/unpublished?)" >&2
  exit 1
fi
is_prerelease=$(jq -r '.isPrerelease' <<<"$release_json")

# Command substitution (not process substitution) so a jq parse error stops
# the script instead of looking like "no assets".
asset_names=$(jq -r --arg re "$ASSET_REGEX" \
  '.assets[].name | select(test($re))' \
  <<<"$release_json")
mapfile -t assets <<<"$asset_names"
if [[ ${#assets[@]} -eq 1 && -z "${assets[0]}" ]]; then
  assets=()
fi
if [[ ${#assets[@]} -eq 0 ]]; then
  echo "error: no assets matching ASSET_REGEX found on release $TAG of $SOURCE_REPO" >&2
  exit 1
fi

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT
# Assets live in a subdirectory so the notes file is never swept up by the
# wildcard upload below.
assets_dir="$tmpdir/assets"
mkdir -p "$assets_dir"

# Notes go through --notes-file so large markdown bodies survive intact.
notes_file="$tmpdir/notes.md"
if [[ "$notes_explicit" == "true" ]]; then
  printf '%s' "$RELEASE_NOTES" >"$notes_file"
else
  # Mirror the source release body, rewriting asset download URLs so the
  # Download table resolves against the mirrored assets (identical names on
  # DEST_REPO). Empty body falls back to the short daemon mirror blurb.
  source_notes=$(jq -r '.body // ""' <<<"$release_json" \
    | sed "s|github.com/$SOURCE_REPO/releases/download/|github.com/$DEST_REPO/releases/download/|g")
  if [[ -z "${source_notes//[[:space:]]/}" ]]; then
    source_notes="Mirror of the $TAG intentd release: platform archives and .sha256 sidecars for the daemon auto-updater."
  fi
  printf '%s' "$source_notes" >"$notes_file"
fi

for asset in "${assets[@]}"; do
  gh release download "$TAG" --repo "$SOURCE_REPO" --pattern "$asset" \
    --dir "$assets_dir" --clobber
done

release_exists=true
if ! GH_TOKEN="$DEST_GH_TOKEN" gh release view "$TAG" --repo "$DEST_REPO" >/dev/null 2>&1; then
  release_exists=false
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
    --title "$RELEASE_TITLE" \
    --notes-file "$notes_file"
  then
    # Tolerate only a lost create race (two mirrors close together): the
    # release must exist now; otherwise fail loudly.
    if ! GH_TOKEN="$DEST_GH_TOKEN" gh release view "$TAG" --repo "$DEST_REPO" >/dev/null 2>&1; then
      echo "error: failed to create release $TAG on $DEST_REPO and it does not exist" >&2
      exit 1
    fi
  fi
fi

# Sync mirrored notes onto a pre-existing release so re-runs (mirror-release.yml)
# backfill notes on releases mirrored before notes were copied. Title stays
# as-is; explicit RELEASE_NOTES keeps its create-only semantics.
if [[ "$release_exists" == "true" && "$notes_explicit" == "false" ]]; then
  GH_TOKEN="$DEST_GH_TOKEN" gh release edit "$TAG" \
    --repo "$DEST_REPO" \
    --notes-file "$notes_file"
fi

# Refresh mode: drop dest assets that match the pattern but are gone from the
# source set, so a refreshed fixed release (sitter-latest) never keeps stale
# assets that --clobber alone would leave behind.
if [[ "$PRUNE_STALE" == "true" ]]; then
  dest_names=$(GH_TOKEN="$DEST_GH_TOKEN" gh release view "$TAG" --repo "$DEST_REPO" --json assets \
    | jq -r --arg re "$ASSET_REGEX" '.assets[].name | select(test($re))')
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    if [[ ! -e "$assets_dir/$name" ]]; then
      GH_TOKEN="$DEST_GH_TOKEN" gh release delete-asset "$TAG" "$name" \
        --repo "$DEST_REPO" --yes
      echo "pruned stale asset $name from $DEST_REPO@$TAG" >&2
    fi
  done <<<"$dest_names"
fi

GH_TOKEN="$DEST_GH_TOKEN" gh release upload "$TAG" \
  "$assets_dir"/* \
  --repo "$DEST_REPO" \
  --clobber
echo "mirrored ${#assets[@]} assets from $SOURCE_REPO@$TAG to $DEST_REPO@$TAG" >&2
