#!/usr/bin/env bash
# Comment on monorepo issues fixed by a release, gated on completeness.
#
# Usage: notify-fixed-issues.sh [--dry-run] <component> <version> <from-ref> <to-ref>
#
# Collects ISSUES_REPO issue references (intent-hq/intent#N or the full
# issue URL; the tracker's pre-rename name intent-hq/monorepo is accepted
# too) from commit messages in <from-ref>..<to-ref>, additionally
# resolving squash-merge "(#N)" subject suffixes to PR bodies on SOURCE_REPO
# via the GitHub API and scanning those too. Posts one channel-free
# "This fix is included in <component> vX.Y.Z." comment per referenced
# issue. Only the release (tag build) workflow comments; channel promotions
# post nothing.
#
# Completeness gate ("stay silent until complete"): before posting, the
# issue's linked fix PRs (GraphQL closedByPullRequestsReferences on the
# ISSUES_REPO issue) are filtered to SOURCE_REPO, and the comment is posted
# only when none are open and every merged one's merge commit is contained
# in <to-ref>. Otherwise the issue is skipped with a log line — a later
# release whose range re-references the issue will carry the comment. When
# completeness cannot be determined (API error, token cannot see SOURCE_REPO
# PRs), the issue is skipped with a warning: never post a possibly-false
# claim. Issues with no SOURCE_REPO-linked fix PRs at all (commit-message-only
# references) fall back to the range-scan evidence and post — best effort,
# "at the time of writing".
#
# Idempotent: each comment embeds a hidden marker
# (<!-- release-notifier: <component> vX.Y.Z -->) and issues that already
# carry a marker for this component+version (including legacy
# channel-suffixed markers) are skipped, so tag rebuilds / workflow re-runs
# never double-post. With --dry-run, prints the issue list and comment bodies
# without posting (ISSUES_GH_TOKEN is then optional; the marker check and
# the completeness gate fall back to ambient gh auth, best-effort — a failed
# gate preflight warns and continues instead of aborting, since dry-run posts
# nothing anyway).
#
# This script is best-effort by design: the caller
# (publish-channel-manifest.yml) runs it fail-soft so a notification failure
# never blocks a release.
# Requires: git (a checkout with full history for the range — also used for
# the merge-commit containment check) and gh (authenticated via GH_TOKEN for
# the SOURCE_REPO PR-body reads).
#
# Env:
#   SOURCE_REPO      repo the range's PRs live on (default: intent-hq/intentd)
#   ISSUES_REPO      repo whose issues are commented on
#                    (default: intent-hq/intent)
#   ISSUES_GH_TOKEN  token with issues:write on ISSUES_REPO AND
#                    pull-requests:read on SOURCE_REPO (the completeness
#                    gate enumerates the issue's linked SOURCE_REPO PRs, so
#                    a token that cannot see SOURCE_REPO would silently
#                    defeat the gate — the script verifies this up front);
#                    required unless --dry-run (also used to read existing
#                    comments; falls back to ambient gh auth when unset)
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

usage="usage: notify-fixed-issues.sh [--dry-run] <component> <version> <from-ref> <to-ref>"
COMPONENT="${1:?$usage}"
VERSION="${2:?$usage}"
FROM_REF="${3:?$usage}"
TO_REF="${4:?$usage}"
SOURCE_REPO="${SOURCE_REPO:-intent-hq/intentd}"
ISSUES_REPO="${ISSUES_REPO:-intent-hq/intent}"
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
# The tracker was renamed intent-hq/monorepo -> intent-hq/intent; refs written
# with either name point at the same issues (GitHub redirects the old name),
# so the extraction regex permanently accepts both.
issue_repo_names=("$ISSUES_REPO")
if [[ "$ISSUES_REPO" == "intent-hq/intent" ]]; then
  issue_repo_names+=("intent-hq/monorepo")
fi
issues_repo_re=""
for name in "${issue_repo_names[@]}"; do
  issues_repo_re+="${issues_repo_re:+|}${name//./\\.}"
done
issue_ref_re="((${issues_repo_re})#|https://github\\.com/(${issues_repo_re})/issues/)[0-9]+"

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

# Gate preflight: the completeness gate enumerates the issue's linked
# SOURCE_REPO fix PRs with the same token used for ISSUES_REPO. A token that
# cannot see SOURCE_REPO PRs gets them silently omitted from the GraphQL
# response, which would defeat the gate — so verify visibility up front, on
# the same GraphQL surface the gate reads (REST and GraphQL visibility can
# differ, e.g. for fine-grained PATs), and refuse to post anything when it
# is missing (fail-safe: never post a possibly-false claim). Dry-run warns
# and continues instead: it posts nothing, and the per-issue gate still
# surfaces enumeration failures as skips.
preflight_query='query($owner: String!, $repo: String!) {
  repository(owner: $owner, name: $repo) { pullRequests(first: 1) { totalCount } }
}'
if ! gh_issues api graphql -f query="$preflight_query" \
  -f owner="${SOURCE_REPO%/*}" -f repo="${SOURCE_REPO#*/}" >/dev/null 2>&1; then
  msg="the notifier token (ISSUES_GH_TOKEN; MONOREPO_ISSUES_TOKEN secret in workflows) cannot list pull requests on ${SOURCE_REPO} via GraphQL; grant it pull-requests:read on ${SOURCE_REPO} — completeness is indeterminate, skipping all notifications"
  echo "warning: $msg" >&2
  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
    echo "::warning::$msg"
  fi
  if [[ "$DRY_RUN" != true ]]; then
    exit 1
  fi
fi

marker="<!-- release-notifier: ${COMPONENT} v${VERSION} -->"
# Idempotency matches on the component+version prefix (trailing space) so
# legacy channel-suffixed markers ("... vX.Y.Z alpha -->") also count as
# already-notified.
marker_match="release-notifier: ${COMPONENT} v${VERSION} "
message="This fix is included in ${COMPONENT} v${VERSION}."
comment_body="${message}
${marker}"

linked_prs_query='query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    issue(number: $number) {
      closedByPullRequestsReferences(first: 100, includeClosedPrs: true) {
        pageInfo { hasNextPage }
        nodes { repository { nameWithOwner } number state mergeCommit { oid } }
      }
    }
  }
}'

posted=0
skipped=0
failed=0
while IFS= read -r n; do
  [[ -n "$n" ]] || continue
  # Idempotency: skip issues that already carry a marker for this
  # component+version.
  if existing=$(gh_issues api "repos/${ISSUES_REPO}/issues/${n}/comments" \
    --paginate --jq '.[].body' 2>/dev/null); then
    if grep -qF "$marker_match" <<<"$existing"; then
      echo "issue #$n: already notified for ${COMPONENT} v${VERSION}; skipping" >&2
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
  # Completeness gate: enumerate the issue's linked fix PRs on SOURCE_REPO.
  # Post only when none are open and every merged one's merge commit is
  # contained in TO_REF; an empty set falls back to the range-scan evidence
  # that put the issue on the list. Enumeration failure (API error, issue
  # number actually a PR, ...) is indeterminate => skip with a warning.
  # The jq filter emits pageInfo.hasNextPage as the first line, then one
  # "<number> <state> <oid>" line per SOURCE_REPO-linked PR.
  if ! linked=$(gh_issues api graphql \
    -f query="$linked_prs_query" \
    -f owner="${ISSUES_REPO%/*}" -f repo="${ISSUES_REPO#*/}" -F number="$n" \
    --jq ".data.repository.issue.closedByPullRequestsReferences
      | (.pageInfo.hasNextPage | tostring),
        (.nodes[]
          | select(.repository.nameWithOwner == \"${SOURCE_REPO}\")
          | \"\(.number) \(.state) \(.mergeCommit.oid // \"\")\")" 2>/dev/null); then
    echo "warning: issue #$n: could not enumerate linked ${SOURCE_REPO} fix PRs; completeness indeterminate, skipping" >&2
    failed=1
    continue
  fi
  # A truncated connection (>100 linked PRs) could hide an open or
  # unreleased SOURCE_REPO PR beyond the first page: indeterminate => skip.
  if [[ "$(head -n1 <<<"$linked")" != "false" ]]; then
    echo "warning: issue #$n: more than 100 linked PRs (result truncated); completeness indeterminate, skipping" >&2
    failed=1
    continue
  fi
  linked=$(tail -n +2 <<<"$linked")
  incomplete=""
  while read -r pr state oid; do
    [[ -n "$pr" ]] || continue
    case "$state" in
      OPEN)
        incomplete="linked fix PR ${SOURCE_REPO}#${pr} is still open"
        ;;
      MERGED)
        # Contained when the merge commit is an ancestor of the released
        # tag. An oid missing from this full-history checkout cannot be in
        # the tag (e.g. merged to a non-default branch).
        if [[ -z "$oid" ]] || ! git cat-file -e "$oid" 2>/dev/null \
          || ! git merge-base --is-ancestor "$oid" "$TO_REF"; then
          incomplete="merged fix PR ${SOURCE_REPO}#${pr} is not contained in ${TO_REF}"
        fi
        ;;
      *)
        # CLOSED without merge: not a pending fix; ignore.
        ;;
    esac
    [[ -z "$incomplete" ]] || break
  done <<<"$linked"
  if [[ -n "$incomplete" ]]; then
    echo "issue #$n: $incomplete; staying silent (a later release will pick it up)" >&2
    skipped=$((skipped + 1))
    continue
  fi
  if [[ "$DRY_RUN" == true ]]; then
    echo "--- would comment on ${ISSUES_REPO}#${n}: ---"
    printf '%s\n' "$comment_body"
  elif gh_issues issue comment "$n" --repo "$ISSUES_REPO" --body "$comment_body" >/dev/null; then
    echo "issue #$n: commented (${COMPONENT} v${VERSION})" >&2
    posted=$((posted + 1))
  else
    echo "warning: issue #$n: failed to post comment" >&2
    failed=1
  fi
done <<<"$issue_nums"

if [[ "$DRY_RUN" == true ]]; then
  echo "dry-run: nothing posted (skipped $skipped already-notified or incomplete issue(s))" >&2
else
  echo "posted $posted comment(s), skipped $skipped already-notified or incomplete issue(s)" >&2
fi
if [[ "$failed" -ne 0 ]]; then
  # Callers run this fail-soft (continue-on-error), and a notification
  # dropped here is dropped permanently — later release ranges will not
  # re-reference the issue. Annotate loudly so it is visible without
  # opening the step log (same rationale as the on_err trap;
  # intent-hq/monorepo#1921).
  msg="one or more issue notifications failed and will not be retried by later releases; check the log and comment manually if needed"
  echo "error: $msg" >&2
  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
    echo "::error::$msg"
  fi
  exit 1
fi
