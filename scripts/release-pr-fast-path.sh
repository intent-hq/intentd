#!/usr/bin/env bash
#
# release-pr-fast-path.sh — decide whether a PR diff has the exact shape of a
# release-plz release PR (version/changelog-only), so CI can skip heavy jobs.
#
# Usage: scripts/release-pr-fast-path.sh <base-ref-or-sha> [<head-ref-or-sha>]
#   <head> defaults to HEAD.
#
# Output contract (for CI):
#   - Prints `fast_path=true` and exits 0 when the diff matches the shape.
#   - Prints `fast_path=false` and exits 0 on any non-match (the reason is
#     logged to stderr). A non-match must never fail the calling job.
#   - Exits non-zero only on unexpected errors; callers must treat a non-zero
#     exit as a non-match.
#
# Match conditions (all must hold; diff is taken from merge-base(base, head)):
#   1. Changed files ⊆ { CHANGELOG.md, Cargo.lock, crates/*/Cargo.toml }, all
#      pure modifications (no adds/deletes/renames), and
#      crates/intentd/Cargo.toml is among them.
#   2. Exactly one old version A and one new version B, taken from the
#      [package].version delta of crates/intentd/Cargo.toml (A != B).
#   3. Every changed crates/*/Cargo.toml is byte-identical to its base blob
#      after replacing the literal string `version = "B"` with
#      `version = "A"` — i.e. nothing but uniform version strings changed.
#   4. Cargo.lock is byte-identical to its base blob after rewriting
#      `version = "B"` to `version = "A"` only on version lines inside
#      [[package]] stanzas whose name is a workspace member crate (derived
#      from crates/*/Cargo.toml at head). Any external-dep version or
#      checksum change therefore fails the match.
#   CHANGELOG.md content is unconstrained (it does not affect the build).
#
# Git history requirements (shallow CI checkouts): the <base> and <head>
# commits — and ideally their merge-base — must be present locally. With
# actions/checkout, either use `fetch-depth: 0`, or fetch the PR base sha
# explicitly and pass it as <base>:
#   git fetch --depth=1 origin "$PR_BASE_SHA"
# If the merge-base cannot be computed (too-shallow history), the script
# falls back to diffing directly against <base>. No network access is
# performed by this script.

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <base-ref-or-sha> [<head-ref-or-sha>]" >&2
  exit 2
fi

BASE=$(git rev-parse --verify --quiet "$1^{commit}") || {
  echo "error: cannot resolve base '$1'" >&2
  exit 2
}
HEAD_REV=$(git rev-parse --verify --quiet "${2:-HEAD}^{commit}") || {
  echo "error: cannot resolve head '${2:-HEAD}'" >&2
  exit 2
}

no_match() {
  echo "release-pr-fast-path: no match: $1" >&2
  echo "fast_path=false"
  exit 0
}

TMPDIR_FP=$(mktemp -d)
trap 'rm -rf "$TMPDIR_FP"' EXIT

MB=$(git merge-base "$BASE" "$HEAD_REV" 2>/dev/null) || MB="$BASE"

# --- Condition 1: allowed file set, modifications only -----------------------
git diff --name-status "$MB" "$HEAD_REV" >"$TMPDIR_FP/diff" || exit 2
[[ -s "$TMPDIR_FP/diff" ]] || no_match "empty diff"

saw_intentd_toml=false
while IFS=$'\t' read -r status path _rest; do
  [[ "$status" == "M" ]] || no_match "non-modification change ($status $path)"
  case "$path" in
    CHANGELOG.md | Cargo.lock) ;;
    crates/*/Cargo.toml) [[ "$path" == crates/*/*/* ]] && no_match "disallowed file: $path" ;;
    *) no_match "disallowed file: $path" ;;
  esac
  [[ "$path" == "crates/intentd/Cargo.toml" ]] && saw_intentd_toml=true
done <"$TMPDIR_FP/diff"
$saw_intentd_toml || no_match "crates/intentd/Cargo.toml unchanged (no version delta)"

# --- Condition 2: single uniform version delta A -> B -------------------------
pkg_version() { # $1 = <rev>:<path>; prints [package].version
  git show "$1" | awk '
    /^\[/ { in_pkg = ($0 == "[package]") }
    in_pkg && /^version[[:space:]]*=[[:space:]]*"/ { split($0, p, "\""); print p[2]; exit }'
}

VER_A=$(pkg_version "$MB:crates/intentd/Cargo.toml")
VER_B=$(pkg_version "$HEAD_REV:crates/intentd/Cargo.toml")
[[ "$VER_A" =~ ^[0-9A-Za-z][0-9A-Za-z.+-]*$ ]] || no_match "unparseable base version '$VER_A'"
[[ "$VER_B" =~ ^[0-9A-Za-z][0-9A-Za-z.+-]*$ ]] || no_match "unparseable head version '$VER_B'"
[[ "$VER_A" != "$VER_B" ]] || no_match "no version change in crates/intentd/Cargo.toml"

# Literal (non-regex) replacement of `version = "B"` -> `version = "A"`.
normalize_versions() {
  awk -v from="version = \"$VER_B\"" -v to="version = \"$VER_A\"" '
    {
      line = $0; out = ""
      while ((i = index(line, from)) > 0) {
        out = out substr(line, 1, i - 1) to
        line = substr(line, i + length(from))
      }
      print out line
    }'
}

# --- Condition 3: changed Cargo.tomls are version-only ------------------------
while IFS=$'\t' read -r _status path _rest; do
  [[ "$path" == crates/*/Cargo.toml ]] || continue
  git show "$MB:$path" >"$TMPDIR_FP/base"
  git show "$HEAD_REV:$path" | normalize_versions >"$TMPDIR_FP/norm"
  cmp -s "$TMPDIR_FP/norm" "$TMPDIR_FP/base" || no_match "non-version change in $path"
done <"$TMPDIR_FP/diff"

# --- Condition 4: Cargo.lock only bumps workspace member stanzas --------------
if grep -q $'^M\tCargo.lock$' "$TMPDIR_FP/diff"; then
  # Workspace members derived from crates/*/Cargo.toml at head (never hardcoded).
  git ls-tree -r --name-only "$HEAD_REV" -- crates |
    grep -E '^crates/[^/]+/Cargo\.toml$' |
    while IFS= read -r f; do
      git show "$HEAD_REV:$f" | awk '
        /^\[/ { in_pkg = ($0 == "[package]") }
        in_pkg && /^name[[:space:]]*=[[:space:]]*"/ { split($0, p, "\""); print p[2]; exit }'
    done >"$TMPDIR_FP/members"
  [[ -s "$TMPDIR_FP/members" ]] || no_match "could not derive workspace members"

  git show "$MB:Cargo.lock" >"$TMPDIR_FP/lock_base"
  git show "$HEAD_REV:Cargo.lock" >"$TMPDIR_FP/lock_head"
  awk -v vb="$VER_B" -v va="$VER_A" '
    NR == FNR { members[$0] = 1; next }
    /^\[\[package\]\]$/ { current = "" }
    /^name = "/ { split($0, p, "\""); current = p[2] }
    $0 == "version = \"" vb "\"" && (current in members) { print "version = \"" va "\""; next }
    { print }
  ' "$TMPDIR_FP/members" "$TMPDIR_FP/lock_head" >"$TMPDIR_FP/lock_norm"
  cmp -s "$TMPDIR_FP/lock_norm" "$TMPDIR_FP/lock_base" ||
    no_match "Cargo.lock has changes beyond workspace-member version bumps"
fi

echo "fast_path=true"
