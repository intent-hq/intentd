#!/usr/bin/env bash
# Fail when git tracks Python bytecode: any *.pyc file or any path with a
# __pycache__ component. .gitignore keeps new caches untracked, but
# `git add -f` bypasses it, so CI (ci.yml release-scripts) runs this check.
# A committed cache file (scripts/__pycache__/*.pyc, intentd#2529) showed up
# in reviews and was silently deleted by worktree cleanups.
#
# Usage: ./scripts/check-no-python-cache.sh [repo-dir]   (default: this repo)
# Exit 0 when nothing is tracked, 1 when something is (one ::error line per
# path), 2 when the file list cannot be read.
set -euo pipefail

repo=${1:-"$(cd "$(dirname "$0")/.." && pwd)"}
# Read [repo-dir] itself, never a repo or index named by an exported GIT_DIR /
# GIT_INDEX_FILE (set inside git hooks and `git rebase --exec`).
# shellcheck disable=SC2046
unset $(git rev-parse --local-env-vars)

listing=$(mktemp)
trap 'rm -f "$listing"' EXIT

# NUL-delimited via a temp file: robust to unusual pathnames, and a failing
# `git ls-files` fails the check instead of passing an empty list. With glob
# magic a leading **/ also matches the top level.
if ! git -C "$repo" ls-files -z -- ':(glob)**/*.pyc' ':(glob)**/__pycache__' ':(glob)**/__pycache__/**' >"$listing"; then
  echo "error: cannot list tracked files in $repo" >&2
  exit 2
fi

# Escape a value for a workflow-command property (file=...).
annotation_property() {
  local value=${1//%/%25}
  value=${value//$'\r'/%0D}
  value=${value//$'\n'/%0A}
  value=${value//:/%3A}
  printf '%s' "${value//,/%2C}"
}

found=0
while IFS= read -r -d '' path; do
  echo "::error file=$(annotation_property "$path")::tracked Python bytecode cache; untrack it with git rm --cached"
  printf '  %q\n' "$path" >&2
  found=1
done <"$listing"

if [ "$found" -ne 0 ]; then
  echo "Python cache files must not be tracked (.gitignore ignores __pycache__/ and *.pyc); remove them with: git rm --cached -- <path>" >&2
  exit 1
fi
echo "No tracked Python bytecode."
