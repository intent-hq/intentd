#!/usr/bin/env bash
# Rewrite the channel-stable release body with aggregated release notes.
#
# Usage: aggregate-stable-notes.sh <promoted-version> [<previous-stable-version>]
#
# Collects the bodies of every published (non-draft) vX.Y.Z release in
# (previous-stable, promoted] (semver order, plain X.Y.Z tags only) and
# rewrites the `channel-stable` release body as a
# "Stable channel — currently vX.Y.Z" header followed by one "## vX.Y.Z"
# section per release, newest first. With no previous version (first
# promotion) the notes contain just the promoted version's body.
#
# The base of the aggregated range is persisted in the body as an invisible
# `<!-- notes-base: X.Y.Z -->` marker. When the range is empty (idempotent
# re-promotion where previous == promoted, or a promoted version older than
# the previous stable), the marker from the current channel-stable body is
# used to rebuild the full aggregate instead of collapsing it to a single
# section; with no usable marker (legacy body) the body is left untouched.
#
# This script is best-effort by design: callers (promote-stable.yml) run it
# fail-soft so a notes failure never blocks a promotion. Requires: gh
# (authenticated via GH_TOKEN), jq, and an explicit GITHUB_REPOSITORY
# (owner/repo) — no default, so a local run can never edit the upstream repo
# by accident.
set -euo pipefail

usage="usage: aggregate-stable-notes.sh <promoted-version> [<previous-stable-version>]"
PROMOTED="${1:?$usage}"
PREV="${2:-}"
REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY (owner/repo) must be set}"

semver_re='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
if [[ ! "$PROMOTED" =~ $semver_re ]]; then
  echo "error: promoted version must be X.Y.Z, got: $PROMOTED" >&2
  exit 1
fi
if [[ -n "$PREV" && ! "$PREV" =~ $semver_re ]]; then
  # Unvalidated data (came from a stable.json asset): don't echo it raw.
  echo "warning: previous stable version is not plain X.Y.Z; treating as unknown" >&2
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

versions=()
notes_base=""
if [[ -z "$PREV" ]]; then
  # First promotion (or unreadable previous stable.json): no range to span,
  # just the promoted version's body.
  versions=("$PROMOTED")
else
  # Enumerate published (non-draft) releases with plain vX.Y.Z tags. The
  # prerelease flag is deliberately not a filter: every tagged release is
  # published as a Pre-release (beta) and only loses the flag once promoted,
  # so filtering on it would shrink the range to already-promoted versions.
  all_versions=$(gh release list --repo "$REPO" --limit 1000 \
    --json tagName,isDraft \
    --jq '.[] | select(.isDraft | not) | .tagName
          | select(test("^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$"))
          | ltrimstr("v")')

  mapfile -t versions < <(collect_range "$PREV")
  notes_base="$PREV"

  # Empty range (idempotent re-promotion where PREV == PROMOTED, or
  # PROMOTED < PREV): rebuild the previous aggregate from the notes-base
  # marker persisted in the current channel-stable body instead of
  # collapsing it to a single section.
  if [[ ${#versions[@]} -eq 0 ]]; then
    current_body=$(gh release view channel-stable --repo "$REPO" --json body --jq '.body // ""')
    base=$(sed -n 's/^<!-- notes-base: \([0-9.]*\) -->$/\1/p' <<<"$current_body" | head -n1)
    if [[ "$base" =~ $semver_re ]]; then
      echo "no versions in ($PREV, $PROMOTED]; rebuilding from notes-base marker $base" >&2
      mapfile -t versions < <(collect_range "$base")
      notes_base="$base"
    fi
    if [[ ${#versions[@]} -eq 0 ]]; then
      # No usable marker (legacy body) or the marker range is empty too:
      # never shrink the aggregate — leave the body untouched.
      echo "no versions to aggregate; leaving channel-stable notes untouched" >&2
      exit 0
    fi
  fi
fi

echo "aggregating notes for: ${versions[*]}" >&2

notes_file=$(mktemp)
trap 'rm -f "$notes_file"' EXIT
{
  echo "Stable channel — currently v$PROMOTED"
  echo
  echo "Machine-readable pointer to the latest stable intentd release. Do not consume the tag itself; download the stable.json asset."
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

gh release edit channel-stable --repo "$REPO" --notes-file "$notes_file"
echo "updated channel-stable release body with notes for ${#versions[@]} version(s)" >&2
