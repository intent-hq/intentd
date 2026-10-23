#!/usr/bin/env bash
# Regression tests for check-no-python-cache.sh: runs the real check against
# throwaway git repos. A commit that force-adds a .pyc, or any file under a
# __pycache__ directory, must fail and name the path; look-alike names and a
# clean repo must pass. Also asserts this repo's .gitignore ignores both
# patterns, so a direct script run cannot leave untracked bytecode behind.
#
# Run directly: ./scripts/test-check-no-python-cache.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
check="$here/check-no-python-cache.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Isolate git from the caller's config (hooks, signing, templates).
export HOME="$tmp/home" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
export GIT_AUTHOR_NAME=test GIT_AUTHOR_EMAIL=test@example.invalid
export GIT_COMMITTER_NAME=test GIT_COMMITTER_EMAIL=test@example.invalid
mkdir -p "$HOME"

failures=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

# new_repo NAME: an empty repo with this repo's .gitignore and one ordinary file.
new_repo() {
  local dir="$tmp/$1"
  git init -q "$dir"
  cp "$here/../.gitignore" "$dir/.gitignore"
  mkdir -p "$dir/scripts"
  echo 'print("hi")' >"$dir/scripts/tool.py"
  git -C "$dir" add .gitignore scripts/tool.py
  git -C "$dir" commit -q -m init
  echo "$dir"
}

# force_commit DIR PATH...: create each path and commit it past .gitignore.
force_commit() {
  local dir=$1 path
  shift
  for path in "$@"; do
    mkdir -p "$dir/$(dirname "$path")"
    printf 'bytes' >"$dir/$path"
  done
  git -C "$dir" add -f -- "$@"
  git -C "$dir" commit -q -m "add $*"
}

# expect_pass NAME DIR / expect_fail NAME DIR PATH...: run the check, assert
# its exit code and that each PATH is reported as an ::error.
expect_pass() {
  local out status=0
  out=$("$check" "$2" 2>&1) || status=$?
  [ "$status" -eq 0 ] || fail "$1: expected exit 0, got $status — $out"
}

expect_fail() {
  local name=$1 dir=$2 out status=0 path
  shift 2
  out=$("$check" "$dir" 2>&1) || status=$?
  if [ "$status" -ne 1 ]; then
    fail "$name: expected exit 1, got $status — $out"
    return
  fi
  for path in "$@"; do
    case "$out" in
      *"::error file=$path::"*) ;;
      *) fail "$name: output does not name $path — $out" ;;
    esac
  done
}

repo=$(new_repo clean)
expect_pass "clean repo" "$repo"

repo=$(new_repo nested-pyc)
force_commit "$repo" scripts/__pycache__/m.cpython-312.pyc
expect_fail "force-added scripts/__pycache__/*.pyc" "$repo" scripts/__pycache__/m.cpython-312.pyc

repo=$(new_repo top-pyc)
force_commit "$repo" m.pyc
expect_fail "force-added top-level .pyc" "$repo" m.pyc

repo=$(new_repo cache-dir-file)
force_commit "$repo" __pycache__/notes.txt pkg/__pycache__/README
expect_fail "non-pyc files under __pycache__/" "$repo" __pycache__/notes.txt pkg/__pycache__/README

repo=$(new_repo staged-only)
mkdir -p "$repo/lib"
printf 'bytes' >"$repo/lib/x.pyc"
git -C "$repo" add -f lib/x.pyc
expect_fail "staged but uncommitted .pyc" "$repo" lib/x.pyc

repo=$(new_repo look-alikes)
force_commit "$repo" pycache_notes.txt foo.pyc.txt docs/__pycache__notes.md src/my__pycache__/a.py
expect_pass "look-alike names" "$repo"

# A missing repository is a setup error (exit 2), never a pass.
status=0
"$check" "$tmp/does-not-exist" >/dev/null 2>&1 || status=$?
[ "$status" -eq 2 ] || fail "missing repo: expected exit 2, got $status"

# This repo's .gitignore must ignore caches a direct script run writes.
repo=$(new_repo ignore-rules)
for path in scripts/__pycache__/test_runner_policy.cpython-312.pyc __pycache__/a.cpython-311.pyc top.pyc; do
  git -C "$repo" check-ignore -q --no-index -- "$path" || fail ".gitignore does not ignore $path"
done

if [ "$failures" -ne 0 ]; then
  echo "$failures check-no-python-cache test(s) failed" >&2
  exit 1
fi
echo "check-no-python-cache tests passed"
