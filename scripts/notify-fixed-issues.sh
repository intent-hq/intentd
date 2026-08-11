#!/usr/bin/env bash
# Comment on monorepo issues fixed by releases in a tag range.
#
# Usage: notify-fixed-issues.sh [--dry-run] <component> <version> <channel> <from-ref> <to-ref>
#
# Collects ISSUES_REPO issue references (intent-hq/monorepo#N or the full
# issue URL) from commit messages in <from-ref>..<to-ref>, additionally
# resolving squash-merge "(#N)" subject suffixes to PR bodies on SOURCE_REPO
# via the GitHub API and scanning those too. Posts one comment per referenced
# issue naming the component, version, and channel.
#
# Idempotent: each comment embeds a hidden marker
# (<!-- release-notifier: <component> vX.Y.Z <channel> -->) and issues that
# already carry the marker are skipped, so tag rebuilds / workflow re-runs
# never double-post. With --dry-run, prints the issue list and comment bodies
# without posting (ISSUES_GH_TOKEN is then optional and the marker check is
# best-effort).
#
# This script is best-effort by design: callers (publish-channel-manifest.yml,
# promote-stable.yml) run it fail-soft so a notification failure never blocks
# a release or promotion.
# Requires: git (a checkout with full history for the range) and gh
# (authenticated via GH_TOKEN for the SOURCE_REPO PR-body reads).
#
# Env:
#   SOURCE_REPO      repo the range's PRs live on (default: intent-hq/intentd)
#   ISSUES_REPO      repo whose issues are commented on
#                    (default: intent-hq/monorepo)
#   ISSUES_GH_TOKEN  token with issues:write on ISSUES_REPO; required unless
#                    --dry-run (also used to read existing comments; falls
#                    back to ambient gh auth when unset)
set -euo pipefail

# Callers run this script fail-soft (continue-on-error), so an unexpected
# set -e exit would otherwise be invisible (intent-hq/monorepo#1921). Log
# where it died before the shell unwinds — as a ::error:: annotation under
# GitHub Actions so it surfaces without opening the step log. -o errtrace
# propagates the trap into functions and subshells.
set -o errtrace
on_err() {
  local msg="notify-fixed-issues.sh: command failed (exit $1) at line $2: $3"
  echo "error: $msg" >&2
  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
    echo "::error::$msg"
  fi
}
trap 'on_err "$?" "$LINENO" "$BASH_COMMAND"' ERR

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
  shift
fi

usage="usage: notify-fixed-issues.sh [--dry-run] <component> <version> <channel> <from-ref> <to-ref>"
COMPONENT="${1:?$usage}"
VERSION="${2:?$usage}"
CHANNEL="${3:?$usage}"
FROM_REF="${4:?$usage}"
TO_REF="${5:?$usage}"
SOURCE_REPO="${SOURCE_REPO:-intent-hq/intentd}"
ISSUES_REPO="${ISSUES_REPO:-intent-hq/monorepo}"
ISSUES_GH_TOKEN="${ISSUES_GH_TOKEN:-}"

VERSION="${VERSION#v}"

# Validate before echoing anything (workflow-command / log injection).
if [[ ! "$COMPONENT" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "error: component must match ^[A-Za-z0-9._-]+\$" >&2
  exit 1
fi
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: version must look like [v]X.Y.Z[-<prerelease>] (prerelease limited to [0-9A-Za-z.-])" >&2
  exit 1
fi
if [[ "$CHANNEL" != "beta" && "$CHANNEL" != "stable" ]]; then
  echo "error: channel must be beta or stable" >&2
  exit 1
fi
repo_name_re='^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$'
if [[ ! "$SOURCE_REPO" =~ $repo_name_re || ! "$ISSUES_REPO" =~ $repo_name_re ]]; then
  echo "error: SOURCE_REPO and ISSUES_REPO must be owner/repo" >&2
  exit 1
fi
if [[ "$DRY_RUN" == false && -z "$ISSUES_GH_TOKEN" ]]; then
  echo "error: ISSUES_GH_TOKEN must be set (issues:write on $ISSUES_REPO) unless --dry-run" >&2
  exit 1
fi
for ref in "$FROM_REF" "$TO_REF"; do
  if [[ ! "$ref" =~ ^[A-Za-z0-9._/-]+$ ]]; then
    # Deliberately not echoing the raw ref: it is unvalidated (log injection).
    echo "error: from-ref/to-ref must match ^[A-Za-z0-9._/-]+\$" >&2
    exit 1
  fi
  if ! git rev-parse -q --verify "${ref}^{commit}" >/dev/null; then
    echo "error: ref not found in this checkout: $ref" >&2
    exit 1
  fi
done

# gh invocations against ISSUES_REPO use ISSUES_GH_TOKEN when set; dry-runs
# without it fall back to whatever auth gh already has.
gh_issues() {
  if [[ -n "$ISSUES_GH_TOKEN" ]]; then
    GH_TOKEN="$ISSUES_GH_TOKEN" gh "$@"
  else
    gh "$@"
  fi
}

range="${FROM_REF}..${TO_REF}"
issues_repo_re=${ISSUES_REPO//./\\.}
issue_ref_re="(${issues_repo_re}#|https://github\\.com/${issues_repo_re}/issues/)[0-9]+"

refs_file=$(mktemp)
trap 'rm -f "$refs_file"' EXIT

# (a) direct references in commit messages in the range.
messages=$(git log --format=%B "$range")
grep -oE "$issue_ref_re" <<<"$messages" >>"$refs_file" || true

# (b) squash-merge "(#N)" subject suffixes -> PR bodies. A suffix that does
# not resolve to a PR (or an API hiccup) is skipped with a warning: direct
# commit-message references still work and callers run fail-soft anyway.
subjects=$(git log --format=%s "$range")
pr_nums=$(grep -oE '\(#[0-9]+\)$' <<<"$subjects" | grep -oE '[0-9]+' | sort -un || true)
while IFS= read -r pr; do
  [[ -n "$pr" ]] || continue
  if body=$(gh pr view "$pr" --repo "$SOURCE_REPO" --json body --jq '.body // ""' 2>/dev/null); then
    grep -oE "$issue_ref_re" <<<"$body" >>"$refs_file" || true
  else
    echo "warning: could not read PR #$pr on $SOURCE_REPO; skipping its body" >&2
  fi
done <<<"$pr_nums"

issue_nums=$(grep -oE '[0-9]+$' "$refs_file" | sort -un || true)
if [[ -z "$issue_nums" ]]; then
  echo "no $ISSUES_REPO issue references found in $range; nothing to do" >&2
  exit 0
fi
echo "issues referenced in $range: $(tr '\n' ' ' <<<"$issue_nums")" >&2

marker="<!-- release-notifier: ${COMPONENT} v${VERSION} ${CHANNEL} -->"
if [[ "$CHANNEL" == "beta" ]]; then
  message="Fixed in ${COMPONENT} v${VERSION} (beta)."
else
  message="${COMPONENT} v${VERSION} promoted to stable."
fi
comment_body="${message}
${marker}"

posted=0
skipped=0
failed=0
while IFS= read -r n; do
  [[ -n "$n" ]] || continue
  # Idempotency: skip issues that already carry the marker for this
  # component+version+channel.
  if existing=$(gh_issues api "repos/${ISSUES_REPO}/issues/${n}/comments" \
    --paginate --jq '.[].body' 2>/dev/null); then
    if grep -qF "$marker" <<<"$existing"; then
      echo "issue #$n: already notified for ${COMPONENT} v${VERSION} (${CHANNEL}); skipping" >&2
      skipped=$((skipped + 1))
      continue
    fi
  elif [[ "$DRY_RUN" == true ]]; then
    echo "warning: issue #$n: could not read existing comments (marker check skipped in dry-run)" >&2
  else
    echo "warning: issue #$n: could not read existing comments; skipping to avoid double-posting" >&2
    failed=1
    continue
  fi
  if [[ "$DRY_RUN" == true ]]; then
    echo "--- would comment on ${ISSUES_REPO}#${n}: ---"
    printf '%s\n' "$comment_body"
  elif gh_issues issue comment "$n" --repo "$ISSUES_REPO" --body "$comment_body" >/dev/null; then
    echo "issue #$n: commented (${COMPONENT} v${VERSION} ${CHANNEL})" >&2
    posted=$((posted + 1))
  else
    echo "warning: issue #$n: failed to post comment" >&2
    failed=1
  fi
done <<<"$issue_nums"

if [[ "$DRY_RUN" == true ]]; then
  echo "dry-run: nothing posted (skipped $skipped already-notified issue(s))" >&2
else
  echo "posted $posted comment(s), skipped $skipped already-notified issue(s)" >&2
fi
if [[ "$failed" -ne 0 ]]; then
  echo "error: one or more notifications failed" >&2
  exit 1
fi
