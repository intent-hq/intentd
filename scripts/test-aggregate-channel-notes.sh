#!/usr/bin/env bash
# Regression tests for aggregate-channel-notes.sh: runs the real script
# against a stubbed `gh` (no network, no credentials) and asserts the body
# it writes — or that it refuses to write one. Guards the idempotent
# re-promotion behavior: an empty (prev, promoted] range must rebuild the
# full aggregate from the notes-base marker, and a legacy body without a
# marker must be left untouched, never collapsed to a single section.
#
# Run directly: ./scripts/test-aggregate-channel-notes.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
script="$here/aggregate-channel-notes.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin" "$tmp/out"

# Stub gh: release list -> $STUB_RELEASES (one X.Y.Z per line, as the real
# call's --jq leaves it); release view $STUB_EXPECT_TAG -> $STUB_CURRENT_BODY
# (any other channel-* tag fails: the marker must never be read from the
# wrong channel's body); release view vX.Y.Z -> a deterministic per-version
# body; release edit -> copies --notes-file to $STUB_EDIT_OUT and records
# the edited tag in $STUB_EDIT_OUT.tag so tests can assert on both.
cat >"$tmp/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1 $2" in
  "release list")
    cat "$STUB_RELEASES"
    ;;
  "release view")
    if [[ "$3" == channel-* ]]; then
      if [[ "$3" != "$STUB_EXPECT_TAG" ]]; then
        echo "stub gh: unexpected channel tag viewed: $3 (want $STUB_EXPECT_TAG)" >&2
        exit 1
      fi
      cat "$STUB_CURRENT_BODY"
    else
      echo "notes body for $3"
    fi
    ;;
  "release edit")
    notes_file="" prev=""
    for a in "$@"; do
      [[ "$prev" == "--notes-file" ]] && notes_file="$a"
      prev="$a"
    done
    printf '%s\n' "$3" >"$STUB_EDIT_OUT.tag"
    cp "$notes_file" "$STUB_EDIT_OUT"
    ;;
  *)
    echo "stub gh: unhandled: $*" >&2
    exit 1
    ;;
esac
EOF
chmod +x "$tmp/bin/gh"

export PATH="$tmp/bin:$PATH"
export GITHUB_REPOSITORY=example/intentd
printf '%s\n' 0.7.28 0.7.29 0.7.30 0.7.31 >"$tmp/releases.txt"
export STUB_RELEASES="$tmp/releases.txt"

fail() {
  echo "FAIL: $1" >&2
  exit 1
}
assert_contains() {
  grep -qF -- "$2" "$1" || fail "$3: expected body to contain: $2"
}
assert_not_contains() {
  ! grep -qF -- "$2" "$1" || fail "$3: expected body to NOT contain: $2"
}
# assert_edited_tag EXPECTED SCENARIO: the release edit must have targeted
# the expected channel tag (guards against cross-channel clobbering).
assert_edited_tag() {
  [[ "$(cat "$STUB_EDIT_OUT.tag")" == "$1" ]] || fail "$2: expected release edit on tag $1, got: $(cat "$STUB_EDIT_OUT.tag")"
}

export STUB_EXPECT_TAG=channel-stable

echo "scenario 1: first promotion (no previous stable)"
export STUB_CURRENT_BODY=/dev/null STUB_EDIT_OUT="$tmp/out/s1.md"
"$script" stable 0.7.28 >/dev/null 2>&1
assert_contains "$tmp/out/s1.md" "currently v0.7.28" s1
assert_contains "$tmp/out/s1.md" "## v0.7.28" s1
assert_not_contains "$tmp/out/s1.md" "notes-base" s1
assert_edited_tag channel-stable s1

echo "scenario 2: normal promotion aggregates (prev, promoted] + marker"
export STUB_EDIT_OUT="$tmp/out/s2.md"
"$script" stable 0.7.31 0.7.29 >/dev/null 2>&1
assert_contains "$tmp/out/s2.md" "currently v0.7.31" s2
assert_contains "$tmp/out/s2.md" "## v0.7.31" s2
assert_contains "$tmp/out/s2.md" "## v0.7.30" s2
assert_not_contains "$tmp/out/s2.md" "## v0.7.29" s2
assert_contains "$tmp/out/s2.md" "<!-- notes-base: 0.7.29 -->" s2
assert_edited_tag channel-stable s2

echo "scenario 3: idempotent re-promotion rebuilds the aggregate from the marker"
cp "$tmp/out/s2.md" "$tmp/current-body.md"
export STUB_CURRENT_BODY="$tmp/current-body.md" STUB_EDIT_OUT="$tmp/out/s3.md"
"$script" stable 0.7.31 0.7.31 >/dev/null 2>&1
cmp -s "$tmp/out/s2.md" "$tmp/out/s3.md" || fail "s3: rebuilt body differs from the original aggregate"
assert_edited_tag channel-stable s3

echo "scenario 4: re-promotion over a legacy body without a marker leaves it untouched"
printf 'Stable channel — currently v0.7.31\n\nlegacy body, no marker\n' >"$tmp/legacy-body.md"
export STUB_CURRENT_BODY="$tmp/legacy-body.md" STUB_EDIT_OUT="$tmp/out/s4.md"
"$script" stable 0.7.31 0.7.31 >/dev/null 2>&1
[[ ! -e "$tmp/out/s4.md" ]] || fail "s4: expected no release edit"

echo "scenario 5: rollback below the marker base leaves the body untouched"
export STUB_CURRENT_BODY="$tmp/current-body.md" STUB_EDIT_OUT="$tmp/out/s5.md"
"$script" stable 0.7.28 0.7.31 >/dev/null 2>&1
[[ ! -e "$tmp/out/s5.md" ]] || fail "s5: expected no release edit"

echo "scenario 6: rollback above the marker base aggregates (marker, promoted]"
export STUB_EDIT_OUT="$tmp/out/s6.md"
"$script" stable 0.7.30 0.7.31 >/dev/null 2>&1
assert_contains "$tmp/out/s6.md" "currently v0.7.30" s6
assert_contains "$tmp/out/s6.md" "## v0.7.30" s6
assert_not_contains "$tmp/out/s6.md" "## v0.7.31" s6
assert_contains "$tmp/out/s6.md" "<!-- notes-base: 0.7.29 -->" s6
assert_edited_tag channel-stable s6

echo "scenario 7: unknown previous stable + marker rebuilds instead of clobbering"
export STUB_EDIT_OUT="$tmp/out/s7.md"
"$script" stable 0.7.31 >/dev/null 2>&1
cmp -s "$tmp/out/s2.md" "$tmp/out/s7.md" || fail "s7: rebuilt body differs from the original aggregate"
assert_edited_tag channel-stable s7

echo "scenario 8: unknown previous stable + marker with empty range leaves the body untouched"
export STUB_EDIT_OUT="$tmp/out/s8.md"
"$script" stable 0.7.29 >/dev/null 2>&1
[[ ! -e "$tmp/out/s8.md" ]] || fail "s8: expected no release edit"

echo "scenario 9: CRLF body still matches the marker on re-promotion"
sed 's/$/\r/' "$tmp/current-body.md" >"$tmp/crlf-body.md"
export STUB_CURRENT_BODY="$tmp/crlf-body.md" STUB_EDIT_OUT="$tmp/out/s9.md"
"$script" stable 0.7.31 0.7.31 >/dev/null 2>&1
cmp -s "$tmp/out/s2.md" "$tmp/out/s9.md" || fail "s9: rebuilt body differs from the original aggregate"
assert_edited_tag channel-stable s9

export STUB_EXPECT_TAG=channel-beta

echo "scenario 10: beta first promotion writes the beta header + pointer"
export STUB_CURRENT_BODY=/dev/null STUB_EDIT_OUT="$tmp/out/s10.md"
"$script" beta 0.7.28 >/dev/null 2>&1
assert_contains "$tmp/out/s10.md" "Beta channel — currently v0.7.28" s10
assert_contains "$tmp/out/s10.md" "download the beta.json asset" s10
assert_contains "$tmp/out/s10.md" "## v0.7.28" s10
assert_not_contains "$tmp/out/s10.md" "notes-base" s10
assert_edited_tag channel-beta s10

echo "scenario 11: beta normal promotion aggregates (prev, promoted] + marker"
export STUB_EDIT_OUT="$tmp/out/s11.md"
"$script" beta 0.7.31 0.7.29 >/dev/null 2>&1
assert_contains "$tmp/out/s11.md" "Beta channel — currently v0.7.31" s11
assert_contains "$tmp/out/s11.md" "## v0.7.31" s11
assert_contains "$tmp/out/s11.md" "## v0.7.30" s11
assert_not_contains "$tmp/out/s11.md" "## v0.7.29" s11
assert_contains "$tmp/out/s11.md" "<!-- notes-base: 0.7.29 -->" s11
assert_edited_tag channel-beta s11

echo "scenario 12: beta idempotent re-promotion rebuilds the aggregate from the marker"
cp "$tmp/out/s11.md" "$tmp/beta-current-body.md"
export STUB_CURRENT_BODY="$tmp/beta-current-body.md" STUB_EDIT_OUT="$tmp/out/s12.md"
"$script" beta 0.7.31 0.7.31 >/dev/null 2>&1
cmp -s "$tmp/out/s11.md" "$tmp/out/s12.md" || fail "s12: rebuilt body differs from the original aggregate"
assert_edited_tag channel-beta s12

echo "scenario 13: beta re-promotion over a legacy pointer-only body leaves it untouched"
printf 'Machine-readable pointer to the latest beta intentd release. Do not consume the tag itself; download the beta.json asset.\n' >"$tmp/beta-legacy-body.md"
export STUB_CURRENT_BODY="$tmp/beta-legacy-body.md" STUB_EDIT_OUT="$tmp/out/s13.md"
"$script" beta 0.7.31 0.7.31 >/dev/null 2>&1
[[ ! -e "$tmp/out/s13.md" ]] || fail "s13: expected no release edit"

echo "scenario 14: invalid channel is rejected"
export STUB_CURRENT_BODY=/dev/null STUB_EDIT_OUT="$tmp/out/s14.md"
! "$script" alpha 0.7.31 >/dev/null 2>&1 || fail "s14: expected nonzero exit for invalid channel"
[[ ! -e "$tmp/out/s14.md" ]] || fail "s14: expected no release edit"

echo "OK: all scenarios passed"
