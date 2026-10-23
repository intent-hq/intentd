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
listing=$(mktemp)
trap 'rm -f "$listing"' EXIT

# NUL-delimited via a temp file: robust to unusual pathnames, and a failing
# `git ls-files` fails the check instead of passing an empty list.
if ! git -C "$repo" ls-files -z >"$listing"; then
  echo "error: cannot list tracked files in $repo" >&2
  exit 2
fi

found=0
while IFS= read -r -d '' path; do
  case "$path" in
    *.pyc | __pycache__ | __pycache__/* | */__pycache__ | */__pycache__/*)
      echo "::error file=$path::tracked Python bytecode cache; remove it with: git rm --cached '$path'"
      found=1
      ;;
  esac
done <"$listing"

if [ "$found" -ne 0 ]; then
  echo "Python cache files must not be tracked (.gitignore ignores __pycache__/ and *.pyc)." >&2
  exit 1
fi
echo "No tracked Python bytecode."
