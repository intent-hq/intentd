#!/usr/bin/env bash
#
# Self-test for scripts/lint-workflows.sh (`make lint-workflows`).
#
# The precondition and pass-through cases run the lint under a PATH holding
# only stubs, so a host shellcheck in /usr/bin cannot hide the "shellcheck
# missing" path. The --install cases use a stub curl, so they need no network.
# The real-actionlint cases run the pinned binary against a temp git repo
# carrying this repo's .github/actionlint.yaml: a broken `run:` block must
# fail, the configured runner labels must pass, and each ignore must stay
# scoped. They are skipped when the pinned actionlint or shellcheck is absent,
# unless REQUIRE_ACTIONLINT=1 (CI's release-scripts job sets it). Inside the intent
# monorepo the pin is also compared with its scripts/bootstrap-dev-host.sh.
# Bash 3.2 compatible.

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
# Resolved, because the lint cds to its `pwd -P` repo root (macOS /var is a
# symlink to /private/var).
temp_dir=$(cd "$(mktemp -d)" && pwd -P)
trap 'rm -rf "$temp_dir"' EXIT

lint=$repo_root/scripts/lint-workflows.sh
pin=$(sed -n 's/^ACTIONLINT_VERSION="\(.*\)"$/\1/p' "$lint")
[ -n "$pin" ] || { echo "lint-workflows test: cannot read ACTIONLINT_VERSION" >&2; exit 1; }

failures=0
fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}
pass() {
  echo "ok: $*"
}

# A fake repo: the lint under scripts/, the real config.
fake=$temp_dir/repo
mkdir -p "$fake/scripts" "$fake/.github/workflows"
cp "$lint" "$fake/scripts/"
cp "$repo_root/.github/actionlint.yaml" "$fake/.github/actionlint.yaml"

stub_bin=$temp_dir/stub-bin
mkdir -p "$stub_bin"
calls=$temp_dir/actionlint-calls
# Stub actionlint: STUB_VERSION / STUB_VERSION_FAIL drive --version; a lint
# call records its cwd, SHELLCHECK_OPTS and arguments and exits STUB_EXIT.
cat >"$stub_bin/actionlint" <<'SH'
#!/bin/sh
if [ "$1" = --version ]; then
  [ -z "${STUB_VERSION_FAIL:-}" ] || exit 1
  printf '%s\ninstalled by stub\n' "$STUB_VERSION"
  exit 0
fi
{ printf 'cwd=%s\nopts=%s\n' "$(pwd)" "${SHELLCHECK_OPTS:-}"; for arg in "$@"; do printf 'arg=%s\n' "$arg"; done; } >>"$STUB_CALLS"
[ "${STUB_EXIT:-0}" -eq 0 ] || echo "stub: finding"
exit "${STUB_EXIT:-0}"
SH
printf '#!/bin/sh\nexit 0\n' >"$stub_bin/shellcheck"
chmod +x "$stub_bin/actionlint" "$stub_bin/shellcheck"
no_shellcheck_bin=$temp_dir/no-shellcheck-bin
mkdir -p "$no_shellcheck_bin"
cp "$stub_bin/actionlint" "$no_shellcheck_bin/actionlint"
empty_bin=$temp_dir/empty-bin
mkdir -p "$empty_bin"

# run_lint PATH [args...]: runs the copied lint; sets status and output.
run_lint() {
  local path=$1
  shift
  : >"$calls"
  status=0
  output=$(cd "$temp_dir" && PATH=$path STUB_CALLS=$calls "$BASH" "$fake/scripts/lint-workflows.sh" "$@" 2>&1) || status=$?
}

expect() {
  local name=$1 want_status=$2 want_text=$3
  if [ "$status" -ne "$want_status" ]; then
    fail "$name: exit $status, want $want_status; output: $output"
  elif [ -n "$want_text" ] && [[ "$output" != *"$want_text"* ]]; then
    fail "$name: output lacks '$want_text'; output: $output"
  else
    pass "$name"
  fi
}

expect_no_lint_call() {
  if [ -s "$calls" ]; then fail "$1: actionlint lint ran anyway: $(cat "$calls")"; fi
}

# --- Preconditions -----------------------------------------------------------

run_lint "$empty_bin"
expect "actionlint missing names the pin and how to install it" 1 "[missing]  actionlint $pin: required by make lint-workflows; run make bootstrap-dev-host in the intent monorepo, or scripts/lint-workflows.sh --install DIR"

STUB_VERSION=1.0.0 run_lint "$stub_bin"
expect "actionlint off the pin fails" 1 "[missing]  actionlint: found 1.0.0; make lint-workflows pins $pin"
expect_no_lint_call "actionlint off the pin"

STUB_VERSION=$pin STUB_VERSION_FAIL=1 run_lint "$stub_bin"
expect "unrunnable actionlint fails" 1 "is on PATH but actionlint --version fails"

STUB_VERSION=$pin run_lint "$no_shellcheck_bin"
expect "shellcheck missing fails instead of skipping shell checks" 1 "[missing]  shellcheck: required by make lint-workflows"
expect_no_lint_call "shellcheck missing"

# --- Pass-through --------------------------------------------------------------

STUB_VERSION=$pin run_lint "$stub_bin" extra.yml
expect "clean run exits 0 and says so" 0 "lint-workflows: .github/workflows is clean (actionlint $pin)"
want_calls=$(printf 'cwd=%s\nopts=--norc\narg=-shellcheck\narg=%s\narg=-pyflakes=\narg=extra.yml\n' "$fake" "$stub_bin/shellcheck")
if [ "$(cat "$calls")" = "$want_calls" ]; then
  pass "lint runs from the repo root with the resolved shellcheck, no rc file or pyflakes, and passes arguments"
else
  fail "lint call: got $(cat "$calls"), want $want_calls"
fi

# Called by bare name from scripts/ (no slash in BASH_SOURCE).
: >"$calls"
status=0
output=$(cd "$fake/scripts" && PATH=$stub_bin STUB_CALLS=$calls STUB_VERSION=$pin "$BASH" lint-workflows.sh 2>&1) || status=$?
expect "a bare-name call from scripts/ still finds the repo root" 0 "is clean"
if ! grep -qx "cwd=$fake" "$calls"; then fail "bare-name call: lint did not run from the repo root: $(cat "$calls")"; fi

STUB_VERSION=v$pin run_lint "$stub_bin"
expect "a v-prefixed go-install version matches the pin" 0 "is clean"

STUB_VERSION=$pin STUB_EXIT=1 run_lint "$stub_bin"
expect "findings fail with actionlint's exit code and say how to resolve them" 1 "add a path-scoped ignore with its reason to .github/actionlint.yaml"
if [[ "$output" == *"is clean"* ]]; then fail "findings run still printed the clean line"; fi

STUB_VERSION=$pin STUB_EXIT=3 run_lint "$stub_bin"
expect "an actionlint error exit passes through" 3 ""

# --- --install -----------------------------------------------------------------

# Stub curl: writes STUB_ARCHIVE to --output, or fails with STUB_CURL_FAIL.
install_bin=$temp_dir/install-bin
mkdir -p "$install_bin"
cat >"$install_bin/curl" <<'SH'
#!/bin/sh
[ -z "${STUB_CURL_FAIL:-}" ] || exit 22
while [ $# -gt 0 ]; do
  if [ "$1" = --output ]; then cp "$STUB_ARCHIVE" "$2"; exit 0; fi
  shift
done
exit 2
SH
chmod +x "$install_bin/curl"
for tool in tar sha256sum shasum mktemp mkdir chmod mv rm uname cp; do
  if real=$(command -v "$tool" 2>/dev/null); then ln -s "$real" "$install_bin/$tool"; fi
done
# A well-formed archive whose checksum is not the pinned one.
mkdir -p "$temp_dir/archive-src"
printf '#!/bin/sh\necho tampered\n' >"$temp_dir/archive-src/actionlint"
tar -czf "$temp_dir/tampered.tar.gz" -C "$temp_dir/archive-src" actionlint

run_install() {
  status=0
  output=$(cd "$temp_dir" && PATH=$install_bin STUB_ARCHIVE=$temp_dir/tampered.tar.gz "$BASH" "$fake/scripts/lint-workflows.sh" "$@" 2>&1) || status=$?
}

run_install --install
expect "--install without a directory is a usage error" 2 "usage: scripts/lint-workflows.sh --install DIR"

LINT_WORKFLOWS_PLATFORM=linux_amd64 run_install --install "$temp_dir/installed"
expect "--install rejects an archive off the pinned checksum" 2 "SHA-256 mismatch (expected 8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8"
if [ -e "$temp_dir/installed/actionlint" ]; then
  fail "--install left the rejected binary behind"
elif [ -n "$(ls -A "$temp_dir/installed")" ]; then
  fail "--install left staging files behind: $(ls -A "$temp_dir/installed")"
else
  pass "--install leaves nothing behind after a rejected archive"
fi

STUB_CURL_FAIL=1 LINT_WORKFLOWS_PLATFORM=linux_amd64 run_install --install "$temp_dir/installed"
expect "--install reports a failed download" 2 "downloading https://github.com/rhysd/actionlint/releases/download/v$pin/actionlint_${pin}_linux_amd64.tar.gz failed"

# Stub go: a GitHub outage falls back to `go install` of the pinned tag.
cat >"$install_bin/go" <<'SH'
#!/bin/sh
[ "$*" = "install github.com/rhysd/actionlint/cmd/actionlint@v$STUB_GO_PIN" ] || exit 1
printf '#!/bin/sh\necho built\n' >"$GOBIN/actionlint"
chmod +x "$GOBIN/actionlint"
SH
chmod +x "$install_bin/go"
STUB_GO_PIN=$pin STUB_CURL_FAIL=1 LINT_WORKFLOWS_PLATFORM=linux_amd64 run_install --install "$temp_dir/installed"
expect "--install falls back to go install when the download fails" 0 "built actionlint $pin with go install"
if [ -x "$temp_dir/installed/actionlint" ] && [ -z "$(ls -A "$temp_dir/installed" | grep -v '^actionlint$')" ]; then
  pass "--install go fallback leaves only the binary"
else
  fail "--install go fallback left: $(ls -A "$temp_dir/installed")"
fi
rm -f "$install_bin/go" "$temp_dir/installed/actionlint"

LINT_WORKFLOWS_PLATFORM=plan9_mips run_install --install "$temp_dir/installed"
expect "--install refuses a platform with no pinned checksum" 2 "no pinned actionlint $pin release"

# --- Monorepo pin parity ------------------------------------------------------

monorepo=$repo_root/../..
bootstrap=$monorepo/scripts/bootstrap-dev-host.sh
if [ -f "$bootstrap" ] && grep -Eqs '^[[:space:]]*path[[:space:]]*=[[:space:]]*packages/intentd[[:space:]]*$' "$monorepo/.gitmodules"; then
  monorepo_pin=$(sed -n 's/^ACTIONLINT_VERSION="\(.*\)"$/\1/p' "$bootstrap")
  if [ "$monorepo_pin" = "$pin" ]; then
    pass "pin matches the monorepo's bootstrap-dev-host.sh ($pin)"
  else
    fail "pin $pin differs from the monorepo's bootstrap-dev-host.sh ACTIONLINT_VERSION ($monorepo_pin); keep them equal"
  fi
  for key in linux_amd64 linux_arm64 darwin_amd64 darwin_arm64; do
    ours=$(sed -n "s/^ *$key) echo \([0-9a-f]*\) ;;\$/\1/p" "$lint")
    theirs=$(sed -n "s/^ *$key) echo \([0-9a-f]*\) ;;\$/\1/p" "$bootstrap")
    if [ -n "$ours" ] && [ "$ours" = "$theirs" ]; then
      pass "$key checksum matches the monorepo's"
    else
      fail "$key checksum '$ours' differs from the monorepo's '$theirs'"
    fi
  done
else
  echo "skip: monorepo pin parity (not inside an intent monorepo checkout)"
fi

# --- Real actionlint -----------------------------------------------------------

real_actionlint=$(command -v actionlint 2>/dev/null || true)
real_shellcheck=$(command -v shellcheck 2>/dev/null || true)
real_version=
if [ -n "$real_actionlint" ]; then
  real_version=$("$real_actionlint" --version 2>/dev/null | sed -n '1{s/^v//;p;}' || true)
fi
if [ -z "$real_shellcheck" ] || [ "$real_version" != "$pin" ]; then
  if [ "${REQUIRE_ACTIONLINT:-0}" = 1 ]; then
    fail "REQUIRE_ACTIONLINT=1 but actionlint $pin (found '${real_version:-none}') or shellcheck ('${real_shellcheck:-none}') is missing"
  else
    echo "skip: real-actionlint cases (needs actionlint $pin and shellcheck; REQUIRE_ACTIONLINT=1 forbids the skip)"
  fi
else
  real_bin=$temp_dir/real-bin
  mkdir -p "$real_bin"
  ln -s "$real_actionlint" "$real_bin/actionlint"
  ln -s "$real_shellcheck" "$real_bin/shellcheck"
  git -C "$fake" init -q

  write_workflow() {
    rm -f "$fake"/.github/workflows/*.yml
    cat >"$fake/.github/workflows/$1"
  }

  write_workflow labels.yml <<'YAML'
on: push
jobs:
  heavy:
    runs-on: gh-linux-16x
    steps:
      - run: echo heavy
  mac:
    runs-on: [self-hosted, m1mac]
    steps:
      - run: echo mac
YAML
  run_lint "$real_bin"
  expect "real: configured runner labels are known" 0 "is clean"

  write_workflow broken.yml <<'YAML'
on: push
jobs:
  broken:
    runs-on: [self-hosted, not-a-runner-label]
    steps:
      - run: echo "${{ github.no_such_property }}"
YAML
  run_lint "$real_bin"
  expect "real: an unknown runner label fails" 1 'label "not-a-runner-label" is unknown'
  expect "real: a bad expression fails" 1 'property "no_such_property" is not defined'

  # A deliberately broken run: block (unterminated test) must fail.
  write_workflow broken-shell.yml <<'YAML'
on: push
jobs:
  shell:
    runs-on: ubuntu-24.04
    steps:
      - run: |
          if [ -n "$FOO" ; then echo yes; fi
YAML
  run_lint "$real_bin"
  expect "real: a broken run: block fails" 1 "SC1073"

  shell_job='on: push
jobs:
  shell:
    runs-on: ubuntu-24.04
    steps:
      - run: |
          echo "## Summary" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          echo "done" >> $GITHUB_STEP_SUMMARY'
  printf '%s\n' "$shell_job" | write_workflow shell.yml
  run_lint "$real_bin"
  expect "real: run: scripts get shellcheck (SC2086)" 1 "SC2086"
  expect "real: run: scripts get shellcheck (SC2129)" 1 "SC2129"

  # A host ~/.shellcheckrc must not apply to run: scripts (the pinned
  # actionlint passes --norc itself; the stub case above covers ours).
  mkdir -p "$temp_dir/home"
  printf 'disable=SC2016\n' >"$temp_dir/home/.shellcheckrc"
  write_workflow quoting.yml <<'YAML'
on: push
jobs:
  quoting:
    runs-on: ubuntu-24.04
    steps:
      - run: echo '$HOME'
YAML
  HOME=$temp_dir/home XDG_CONFIG_HOME=$temp_dir/home run_lint "$real_bin"
  expect "real: a ~/.shellcheckrc does not apply to run: scripts" 1 "SC2016"

  write_workflow queue.yml <<'YAML'
on: push
concurrency:
  group: writers
  cancel-in-progress: false
  queue: max
jobs:
  write:
    runs-on: ubuntu-24.04
    steps:
      - run: echo write
YAML
  run_lint "$real_bin"
  expect "real: concurrency.queue is accepted" 0 "is clean"

  # The v-release.yml shellcheck ignore covers only the dist-generated file.
  printf '%s\n' "$shell_job" | write_workflow v-release.yml
  run_lint "$real_bin"
  expect "real: SC2086/SC2129 are ignored in v-release.yml" 0 "is clean"
  printf '%s\n' "$shell_job" | write_workflow ci.yml
  run_lint "$real_bin"
  expect "real: SC2086 still fails outside v-release.yml" 1 "SC2086"
fi

if [ "$failures" -ne 0 ]; then
  echo "lint-workflows test: $failures failure(s)" >&2
  exit 1
fi
echo "lint-workflows test: all cases passed"
