#!/usr/bin/env bash
#
# `make lint-workflows`: actionlint over .github/workflows, including the
# ShellCheck pass over `run:` scripts. CI's release-scripts job runs it on every
# PR, in the merge queue and on main.
#
# Workflow YAML has no type system we control, and nothing checked it here:
# main's ci.yml alone carried 15 shellcheck findings, so a reviewer changing a
# workflow had to run actionlint by hand and diff the noise against main
# (found while building intentd#3026). Main is now clean, so any finding is new.
#
# Rule policy (runner labels, path-scoped ignores) lives in
# .github/actionlint.yaml, which actionlint reads from the repo root. Results
# must not depend on the host beyond the two tools:
#   - no shellcheck rc file: a ~/.shellcheckrc must not hide findings CI would
#     report. actionlint 1.7.12 already passes --norc; SHELLCHECK_OPTS repeats
#     it so a future pin cannot silently start reading one;
#   - -pyflakes=: CI has no pyflakes, so a host that happens to have it would
#     otherwise report findings CI never sees.
# The shellcheck binary is not pinned: a newer local copy can report a finding on
# an untouched workflow that CI's does not.
#
# Preconditions fail fast instead of degrading:
#   - actionlint must be exactly ACTIONLINT_VERSION below. Keep the version and
#     checksums equal to the monorepo's scripts/bootstrap-dev-host.sh (which
#     installs it) and cloudlands-fe's scripts/check-workflows.mjs;
#     scripts/test-lint-workflows.sh compares them inside a monorepo checkout.
#   - shellcheck must be on PATH: actionlint silently skips the `run:` script
#     checks without it, which would make a green run check nothing.
#
# Usage:
#   scripts/lint-workflows.sh [actionlint args...]  lint (e.g. one workflow file)
#   scripts/lint-workflows.sh --install DIR         download the pinned release
#       archive, verify its SHA-256 and write DIR/actionlint (CI; also a way to
#       get the pin outside the monorepo). Needs curl, tar and sha256sum/shasum.
# Exit 0 clean, 1 findings or a missing tool, 2 usage or install failure;
# any other status is actionlint's own error exit.
# Bash 3.2 compatible.

set -euo pipefail

ACTIONLINT_VERSION="1.7.12"

# From the release's actionlint_<version>_checksums.txt.
actionlint_sha256() {
  case "$1" in
    linux_amd64) echo 8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8 ;;
    linux_arm64) echo 325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6 ;;
    darwin_amd64) echo 5b44c3bc2255115c9b69e30efc0fecdf498fdb63c5d58e17084fd5f16324c644 ;;
    darwin_arm64) echo aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f ;;
    *) return 1 ;;
  esac
}

# Builtins only (no dirname), so the self-test can run it under a PATH of
# stubs; a bare-name call (`bash lint-workflows.sh` from scripts/) has no slash.
case ${BASH_SOURCE[0]} in
  */*) script_dir=${BASH_SOURCE[0]%/*} ;;
  *) script_dir=. ;;
esac
script_dir=$(cd "$script_dir" && pwd -P)
repo_root=$(cd "$script_dir/.." && pwd -P)
install_hint="run make bootstrap-dev-host in the intent monorepo, or scripts/lint-workflows.sh --install DIR and put DIR on PATH"

missing() {
  echo "[missing]  $*" >&2
  exit 1
}

# The release archive suffix for this host (LINT_WORKFLOWS_PLATFORM overrides
# it for the self-test).
platform() {
  if [ -n "${LINT_WORKFLOWS_PLATFORM:-}" ]; then
    echo "$LINT_WORKFLOWS_PLATFORM"
    return
  fi
  case "$(uname -s):$(uname -m)" in
    Linux:x86_64|Linux:amd64) echo linux_amd64 ;;
    Linux:aarch64|Linux:arm64) echo linux_arm64 ;;
    Darwin:x86_64) echo darwin_amd64 ;;
    Darwin:arm64) echo darwin_arm64 ;;
    *) return 1 ;;
  esac
}

sha256_of() {
  local output
  if command -v sha256sum >/dev/null 2>&1; then
    output=$(sha256sum "$1") || return 1
  elif command -v shasum >/dev/null 2>&1; then
    output=$(shasum -a 256 "$1") || return 1
  else
    return 1
  fi
  echo "${output%% *}"
}

install_actionlint() {
  local dir=$1 key expected archive url staging actual
  if ! key=$(platform) || ! expected=$(actionlint_sha256 "$key"); then
    echo "lint-workflows --install: no pinned actionlint $ACTIONLINT_VERSION release for $(uname -s)/$(uname -m)" >&2
    return 2
  fi
  archive="actionlint_${ACTIONLINT_VERSION}_${key}.tar.gz"
  url="https://github.com/rhysd/actionlint/releases/download/v$ACTIONLINT_VERSION/$archive"
  mkdir -p "$dir"
  staging=$(mktemp -d "$dir/.actionlint-staging.XXXXXX")
  # Staged then renamed, so an interrupted or rejected download never leaves
  # a binary behind.
  # GitHub release downloads have returned 504s for minutes at a time
  # (intentd#3153's queue runs), so retry for up to about five minutes.
  if ! curl --fail --silent --show-error --location --retry 8 --retry-delay 10 --retry-connrefused \
      --retry-max-time 300 --connect-timeout 30 --max-time 120 \
      --output "$staging/$archive" "$url"; then
    rm -rf "$staging"
    echo "lint-workflows --install: downloading $url failed" >&2
    return 2
  fi
  if ! actual=$(sha256_of "$staging/$archive"); then
    rm -rf "$staging"
    echo "lint-workflows --install: neither sha256sum nor shasum is available" >&2
    return 2
  fi
  if [ "$actual" != "$expected" ]; then
    rm -rf "$staging"
    echo "lint-workflows --install: $archive SHA-256 mismatch (expected $expected, got $actual)" >&2
    return 2
  fi
  if ! tar -xzf "$staging/$archive" -C "$staging" actionlint; then
    rm -rf "$staging"
    echo "lint-workflows --install: extracting $archive failed" >&2
    return 2
  fi
  chmod 755 "$staging/actionlint"
  mv -f "$staging/actionlint" "$dir/actionlint"
  rm -rf "$staging"
  echo "lint-workflows: installed actionlint $ACTIONLINT_VERSION at $dir/actionlint"
}

if [ "${1:-}" = --install ]; then
  if [ $# -ne 2 ] || [ -z "$2" ]; then
    echo "usage: scripts/lint-workflows.sh --install DIR" >&2
    exit 2
  fi
  install_actionlint "$2"
  exit $?
fi

if ! command -v actionlint >/dev/null 2>&1; then
  missing "actionlint $ACTIONLINT_VERSION: required by make lint-workflows; $install_hint"
fi
# The first line of `actionlint --version` is the version: bare for release
# archives, v-prefixed for a `go install ...@vX.Y.Z` build.
if ! version_output=$(actionlint --version 2>/dev/null); then
  missing "actionlint: $(command -v actionlint) is on PATH but actionlint --version fails; reinstall it ($install_hint)"
fi
found=${version_output%%$'\n'*}
found=${found#v}
if [ "$found" != "$ACTIONLINT_VERSION" ]; then
  missing "actionlint: found $found; make lint-workflows pins $ACTIONLINT_VERSION; $install_hint"
fi

if ! shellcheck_path=$(command -v shellcheck 2>/dev/null); then
  missing "shellcheck: required by make lint-workflows (actionlint skips run: script checks without it); install it (make bootstrap-dev-host in the intent monorepo, apt-get install shellcheck, or brew install shellcheck)"
fi

cd "$repo_root"
status=0
SHELLCHECK_OPTS=--norc actionlint -shellcheck "$shellcheck_path" -pyflakes= "$@" || status=$?
if [ "$status" -eq 0 ]; then
  echo "lint-workflows: .github/workflows is clean (actionlint $found)"
elif [ "$status" -eq 1 ]; then
  echo "lint-workflows: fix the findings above, or add a path-scoped ignore with its reason to .github/actionlint.yaml (an inline # shellcheck disable= for one intentional line)" >&2
fi
exit "$status"
