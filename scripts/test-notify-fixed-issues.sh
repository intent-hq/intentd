#!/usr/bin/env bash
# Regression tests for notify-fixed-issues.sh: runs the real script in
# --dry-run against a stubbed `gh` (no network, no credentials) and a
# throwaway git repo, and asserts which issues it would comment on. Guards
# the completeness gate: a mention-only reference (the range names an issue
# that has no delivered linked closing PR) and a still-open issue must never
# produce a comment, while a closed issue whose linked SOURCE_REPO PR is
# merged and contained in the released tag does.
#
# Every scenario references two issues in the same range: #10 varies per
# scenario and #11 is a fixed positive control (closed, delivered fix PR), so
# a "no comment on #10" assertion is never satisfied by a run that posts
# nothing at all.
#
# Run directly: ./scripts/test-notify-fixed-issues.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
script="$here/notify-fixed-issues.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin" "$tmp/issues"

# Stub gh, answering exactly what the real calls' --jq filters would leave:
#   api graphql (no -F number)     -> the token-visibility preflight; succeeds
#   api graphql -F number=N        -> $STUB_ISSUES_DIR/N verbatim (issue state,
#                                     pageInfo.hasNextPage, then one
#                                     "<pr> <state> <oid>" line per linked
#                                     SOURCE_REPO PR); a missing file fails the
#                                     call like an API error would
#   api repos/*/issues/N/comments  -> no existing comments
# Anything else (pr view, issue comment, ...) fails loudly: the fixture range
# has no "(#N)" subjects, and a dry-run must never post.
cat >"$tmp/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1 ${2:-}" in
  "api graphql")
    number=""
    for a in "$@"; do
      case "$a" in number=*) number="${a#number=}" ;; esac
    done
    [[ -n "$number" ]] || exit 0
    cat "$STUB_ISSUES_DIR/$number"
    ;;
  "api repos/"*"/issues/"*"/comments")
    ;;
  *)
    echo "stub gh: unhandled: $*" >&2
    exit 1
    ;;
esac
EOF
chmod +x "$tmp/bin/gh"
export PATH="$tmp/bin:$PATH"
export STUB_ISSUES_DIR="$tmp/issues"

# Fixture repo: v1.2.2..v1.2.3 holds one commit whose body references issues
# #10 and #11; its sha doubles as a contained merge-commit oid. A side branch
# provides a sha that exists in the checkout but is not contained in v1.2.3.
repo="$tmp/repo"
git init -q -b main "$repo"
git_commit() {
  git -C "$repo" -c user.name=test -c user.email=test@example.com \
    -c commit.gpgsign=false commit -q --allow-empty "$@"
}
git_commit -m "chore: base"
git -C "$repo" tag v1.2.2
git_commit -m "fix: tighten the gate" -m "Refs intent-hq/intent#10 and intent-hq/intent#11."
git -C "$repo" tag v1.2.3
contained_sha=$(git -C "$repo" rev-parse HEAD)
git -C "$repo" checkout -q -b side v1.2.2
git_commit -m "fix: merged elsewhere"
uncontained_sha=$(git -C "$repo" rev-parse HEAD)
git -C "$repo" checkout -q main

fail() {
  echo "FAIL: $1" >&2
  exit 1
}
assert_contains() {
  grep -qF -- "$2" "$1" || fail "$3: expected output to contain: $2"
}
assert_not_contains() {
  ! grep -qF -- "$2" "$1" || fail "$3: expected output to NOT contain: $2"
}
# fixture N STATE [linked-pr-line...]: writes the stub's answer for issue N;
# FIXTURE_HAS_NEXT_PAGE (default false) is the pageInfo.hasNextPage line.
fixture() {
  local n="$1" state="$2"
  shift 2
  { printf '%s\n%s\n' "$state" "${FIXTURE_HAS_NEXT_PAGE:-false}"; printf '%s\n' "$@"; } >"$STUB_ISSUES_DIR/$n"
}
# run SCENARIO EXPECTED_STATUS: runs a dry-run for intentd v1.2.3 over the
# fixture range, capturing stdout (comment previews) and stderr (log).
run() {
  local status=0
  (cd "$repo" && "$script" --dry-run intentd 1.2.3 v1.2.2 v1.2.3) \
    >"$tmp/out.$1" 2>"$tmp/err.$1" || status=$?
  [[ "$status" -eq "$2" ]] || fail "$1: expected exit $2, got $status: $(cat "$tmp/err.$1")"
}
would_comment_11="--- would comment on intent-hq/intent#11: ---"
would_comment_10="--- would comment on intent-hq/intent#10: ---"
expected_message="This fix is included in intentd v1.2.3."
expected_marker="<!-- release-notifier: intentd v1.2.3 -->"
# assert_comments SCENARIO COUNT: exactly COUNT comment previews, each
# carrying the message and the idempotency marker.
assert_comments() {
  local pattern
  for pattern in '--- would comment on' "$expected_message" "$expected_marker"; do
    [[ "$(grep -c -F -- "$pattern" "$tmp/out.$1")" -eq "$2" ]] \
      || fail "$1: expected $2 occurrence(s) of: $pattern"
  done
}
# assert_control SCENARIO: the #11 control posted, and nothing else did.
assert_control() {
  assert_contains "$tmp/out.$1" "$would_comment_11" "$1"
  assert_not_contains "$tmp/out.$1" "$would_comment_10" "$1"
  assert_comments "$1" 1
}
fixture 11 CLOSED "88 MERGED $contained_sha"

echo "scenario 1: mention-only reference to a closed issue stays silent"
fixture 10 CLOSED
run s1 0
assert_control s1
assert_contains "$tmp/err.s1" "issue #10: no delivered linked fix PR on intent-hq/intentd; mention-only reference, staying silent" s1

echo "scenario 2: mention-only reference to an open issue stays silent"
fixture 10 OPEN
run s2 0
assert_control s2
assert_contains "$tmp/err.s2" "issue #10: issue is still open; staying silent" s2

echo "scenario 3: open issue with a merged, contained fix PR stays silent"
fixture 10 OPEN "77 MERGED $contained_sha"
run s3 0
assert_control s3
assert_contains "$tmp/err.s3" "issue #10: issue is still open; staying silent" s3

echo "scenario 4: closed issue with a merged, contained fix PR is commented on"
fixture 10 CLOSED "77 MERGED $contained_sha"
run s4 0
assert_contains "$tmp/out.s4" "$would_comment_10" s4
assert_contains "$tmp/out.s4" "$would_comment_11" s4
assert_comments s4 2

echo "scenario 5: closed issue with an open fix PR stays silent"
fixture 10 CLOSED "77 OPEN " "78 MERGED $contained_sha"
run s5 0
assert_control s5
assert_contains "$tmp/err.s5" "issue #10: linked fix PR intent-hq/intentd#77 is still open; staying silent" s5

echo "scenario 6: closed issue with a merged fix PR outside the tag stays silent"
fixture 10 CLOSED "77 MERGED $uncontained_sha"
run s6 0
assert_control s6
assert_contains "$tmp/err.s6" "issue #10: merged fix PR intent-hq/intentd#77 is not contained in v1.2.3; staying silent" s6

echo "scenario 7: only an abandoned (closed, unmerged) fix PR counts as mention-only"
fixture 10 CLOSED "77 CLOSED "
run s7 0
assert_control s7
assert_contains "$tmp/err.s7" "issue #10: no delivered linked fix PR on intent-hq/intentd; mention-only reference, staying silent" s7

echo "scenario 8: gate enumeration failure skips the issue with a warning and exits 1"
rm -f "$STUB_ISSUES_DIR/10"
run s8 1
assert_control s8
assert_contains "$tmp/err.s8" "warning: issue #10: could not enumerate linked intent-hq/intentd fix PRs; completeness indeterminate, skipping" s8

truncated_warning="warning: issue #10: more than 100 linked PRs (result truncated); completeness indeterminate, skipping"

echo "scenario 9: truncated linked-PR list on a closed issue is indeterminate and exits 1"
FIXTURE_HAS_NEXT_PAGE=true fixture 10 CLOSED "77 MERGED $contained_sha"
run s9 1
assert_control s9
assert_contains "$tmp/err.s9" "$truncated_warning" s9

echo "scenario 10: truncated linked-PR list on an open issue is indeterminate and exits 1"
FIXTURE_HAS_NEXT_PAGE=true fixture 10 OPEN "77 MERGED $contained_sha"
run s10 1
assert_control s10
assert_contains "$tmp/err.s10" "$truncated_warning" s10
assert_not_contains "$tmp/err.s10" "issue #10: issue is still open; staying silent" s10

echo "OK: all scenarios passed"
