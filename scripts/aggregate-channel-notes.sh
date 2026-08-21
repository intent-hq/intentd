#!/usr/bin/env bash
# Rewrite a rolling channel release body with aggregated release notes.
#
# Usage: aggregate-channel-notes.sh <channel> <promoted-version> [<previous-version>]
#
# <channel> is "stable" or "beta"; the notes land on the fixed
# `channel-<channel>` release. Collects the bodies of every published
# (non-draft) vX.Y.Z release in (previous, promoted] (semver order, plain
# X.Y.Z tags only) and rewrites the channel release body as a
# "Stable/Beta channel — currently vX.Y.Z" header followed by one "## vX.Y.Z"
# section per release, newest first. With no previous version (first
# promotion) the notes contain just the promoted version's body.
#
# The base of the aggregated range is persisted in the body as an invisible
# `<!-- notes-base: X.Y.Z -->` marker. When the range is empty (idempotent
# re-promotion where previous == promoted, or a promoted version older than
# the previous channel version) — or when the previous version is unknown
# but the current body carries a marker (re-promotion with an unreadable
# <channel>.json) — the marker is used to rebuild the full aggregate instead
# of collapsing it to a single section; with no usable marker (legacy body)
# an empty range leaves the body untouched.
#
# This script is best-effort by design: callers (promote-stable.yml,
# promote-beta.yml) run it fail-soft so a notes failure never blocks a
# promotion. Requires: gh (authenticated via GH_TOKEN), jq, and an explicit
# GITHUB_REPOSITORY (owner/repo) — no default, so a local run can never edit
# the upstream repo by accident.
set -euo pipefail

usage="usage: aggregate-channel-notes.sh <channel> <promoted-version> [<previous-version>]"
CHANNEL="${1:?$usage}"
PROMOTED="${2:?$usage}"
PREV="${3:-}"
REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY (owner/repo) must be set}"

case "$CHANNEL" in
  stable) channel_name="Stable" ;;
  beta) channel_name="Beta" ;;
  *)
    echo "error: channel must be 'stable' or 'beta', got: $CHANNEL" >&2
    exit 1
    ;;
esac
channel_tag="channel-$CHANNEL"

semver_re='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
if [[ ! "$PROMOTED" =~ $semver_re ]]; then
  echo "error: promoted version must be X.Y.Z, got: $PROMOTED" >&2
  exit 1
fi
if [[ -n "$PREV" && ! "$PREV" =~ $semver_re ]]; then
  # Unvalidated data (came from a <channel>.json asset): don't echo it raw.
  echo "warning: previous $CHANNEL version is not plain X.Y.Z; treating as unknown" >&2
  PREV=""
fi

# semver_le A B: true when A <= B (component-wise, via sort -V).
semver_le() {
  [[ "$1" == "$2" || "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -n1)" == "$1" ]]
}

# collect_range BASE: print the versions in (BASE, PROMOTED] from
# $all_versions, newest first.
collect_range() {
  local base="$1" v
  while IFS= read -r v; do
    [[ -n "$v" ]] || continue
    semver_le "$v" "$PROMOTED" || continue
    semver_le "$v" "$base" && continue
    printf '%s\n' "$v"
  done < <(sort -Vru <<<"$all_versions")
}

# list_versions: enumerate published (non-draft) releases with plain vX.Y.Z
# tags. The prerelease flag is deliberately not a filter: every tagged
# release is published as a Pre-release (beta) and only loses the flag once
# promoted, so filtering on it would shrink the range to already-promoted
# versions.
list_versions() {
  gh release list --repo "$REPO" --limit 1000 \
    --json tagName,isDraft \
    --jq '.[] | select(.isDraft | not) | .tagName
          | select(test("^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$"))
          | ltrimstr("v")'
}

# marker_base: print the notes-base marker version from the current
# channel body (empty when absent). Tolerates a trailing CR in case
# the body was ever hand-edited into CRLF line endings.
marker_base() {
  gh release view "$channel_tag" --repo "$REPO" --json body --jq '.body // ""' \
    | sed -n 's/^<!-- notes-base: \([0-9.]*\) -->\r\{0,1\}$/\1/p' | head -n1
}

versions=()
notes_base=""
if [[ -z "$PREV" ]]; then
  # Previous channel version unknown. Only a true first promotion (no marker
  # in the current body) may write just the promoted version's section: a
  # re-promotion with an unreadable <channel>.json would otherwise clobber
  # the existing aggregate, so with a marker present, rebuild from it — and
  # if that range is empty too, leave the body untouched.
  base=$(marker_base || true)
  if [[ "$base" =~ $semver_re ]]; then
    echo "previous $CHANNEL unknown; rebuilding from notes-base marker $base" >&2
    all_versions=$(list_versions)
    mapfile -t versions < <(collect_range "$base")
    notes_base="$base"
    if [[ ${#versions[@]} -eq 0 ]]; then
      echo "no versions to aggregate; leaving $channel_tag notes untouched" >&2
      exit 0
    fi
  else
    versions=("$PROMOTED")
  fi
else
  all_versions=$(list_versions)
  mapfile -t versions < <(collect_range "$PREV")
  notes_base="$PREV"

  # Empty range (idempotent re-promotion where PREV == PROMOTED, or
  # PROMOTED < PREV): rebuild the previous aggregate from the notes-base
  # marker persisted in the current channel body instead of collapsing it
  # to a single section.
  if [[ ${#versions[@]} -eq 0 ]]; then
    base=$(marker_base || true)
    if [[ "$base" =~ $semver_re ]]; then
      echo "no versions in ($PREV, $PROMOTED]; rebuilding from notes-base marker $base" >&2
      mapfile -t versions < <(collect_range "$base")
      notes_base="$base"
    fi
    if [[ ${#versions[@]} -eq 0 ]]; then
      # No usable marker (legacy body) or the marker range is empty too:
      # never shrink the aggregate — leave the body untouched.
      echo "no versions to aggregate; leaving $channel_tag notes untouched" >&2
      exit 0
    fi
  fi
fi

echo "aggregating notes for: ${versions[*]}" >&2

notes_file=$(mktemp)
trap 'rm -f "$notes_file"' EXIT
{
  echo "$channel_name channel — currently v$PROMOTED"
  echo
  echo "Machine-readable pointer to the latest $CHANNEL intentd release. Do not consume the tag itself; download the $CHANNEL.json asset."
  echo
  if [[ -n "$notes_base" ]]; then
    echo "<!-- notes-base: $notes_base -->"
    echo
  fi
  for v in "${versions[@]}"; do
    body=$(gh release view "v$v" --repo "$REPO" --json body --jq '.body // ""')
    echo "## v$v"
    echo
    if [[ -n "$body" ]]; then
      printf '%s\n' "$body"
    else
      echo "_No release notes._"
    fi
    echo
  done
} >"$notes_file"

gh release edit "$channel_tag" --repo "$REPO" --notes-file "$notes_file"
echo "updated $channel_tag release body with notes for ${#versions[@]} version(s)" >&2
