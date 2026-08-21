#!/usr/bin/env bash
# Regression tests for aggregate-stable-notes.sh: runs the real script
# against a stubbed `gh` (no network, no credentials) and asserts the body
# it writes — or that it refuses to write one. Guards the idempotent
# re-promotion behavior: an empty (prev, promoted] range must rebuild the
# full aggregate from the notes-base marker, and a legacy body without a
# marker must be left untouched, never collapsed to a single section.
#
# Run directly: ./scripts/test-aggregate-stable-notes.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
script="$here/aggregate-stable-notes.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin" "$tmp/out"

# Stub gh: release list -> $STUB_RELEASES (one X.Y.Z per line, as the real
# call's --jq leaves it); release view channel-stable -> $STUB_CURRENT_BODY;
# release view vX.Y.Z -> a deterministic per-version body; release edit ->
# copies --notes-file to $STUB_EDIT_OUT so tests can assert on it.
cat >"$tmp/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1 $2" in
  "release list")
    cat "$STUB_RELEASES"
    ;;
  "release view")
    if [[ "$3" == "channel-stable" ]]; then
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

echo "scenario 1: first promotion (no previous stable)"
export STUB_CURRENT_BODY=/dev/null STUB_EDIT_OUT="$tmp/out/s1.md"
"$script" 0.7.28 >/dev/null 2>&1
assert_contains "$tmp/out/s1.md" "currently v0.7.28" s1
assert_contains "$tmp/out/s1.md" "## v0.7.28" s1
assert_not_contains "$tmp/out/s1.md" "notes-base" s1

echo "scenario 2: normal promotion aggregates (prev, promoted] + marker"
export STUB_EDIT_OUT="$tmp/out/s2.md"
"$script" 0.7.31 0.7.29 >/dev/null 2>&1
assert_contains "$tmp/out/s2.md" "currently v0.7.31" s2
assert_contains "$tmp/out/s2.md" "## v0.7.31" s2
assert_contains "$tmp/out/s2.md" "## v0.7.30" s2
assert_not_contains "$tmp/out/s2.md" "## v0.7.29" s2
assert_contains "$tmp/out/s2.md" "<!-- notes-base: 0.7.29 -->" s2

echo "scenario 3: idempotent re-promotion rebuilds the aggregate from the marker"
cp "$tmp/out/s2.md" "$tmp/current-body.md"
export STUB_CURRENT_BODY="$tmp/current-body.md" STUB_EDIT_OUT="$tmp/out/s3.md"
"$script" 0.7.31 0.7.31 >/dev/null 2>&1
cmp -s "$tmp/out/s2.md" "$tmp/out/s3.md" || fail "s3: rebuilt body differs from the original aggregate"

echo "scenario 4: re-promotion over a legacy body without a marker leaves it untouched"
printf 'Stable channel — currently v0.7.31\n\nlegacy body, no marker\n' >"$tmp/legacy-body.md"
export STUB_CURRENT_BODY="$tmp/legacy-body.md" STUB_EDIT_OUT="$tmp/out/s4.md"
"$script" 0.7.31 0.7.31 >/dev/null 2>&1
[[ ! -e "$tmp/out/s4.md" ]] || fail "s4: expected no release edit"

echo "scenario 5: rollback below the marker base leaves the body untouched"
export STUB_CURRENT_BODY="$tmp/current-body.md" STUB_EDIT_OUT="$tmp/out/s5.md"
"$script" 0.7.28 0.7.31 >/dev/null 2>&1
[[ ! -e "$tmp/out/s5.md" ]] || fail "s5: expected no release edit"

echo "scenario 6: rollback above the marker base aggregates (marker, promoted]"
export STUB_EDIT_OUT="$tmp/out/s6.md"
"$script" 0.7.30 0.7.31 >/dev/null 2>&1
assert_contains "$tmp/out/s6.md" "currently v0.7.30" s6
assert_contains "$tmp/out/s6.md" "## v0.7.30" s6
assert_not_contains "$tmp/out/s6.md" "## v0.7.31" s6
assert_contains "$tmp/out/s6.md" "<!-- notes-base: 0.7.29 -->" s6

echo "OK: all scenarios passed"
