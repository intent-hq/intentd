#!/usr/bin/env bash
# Run a command with every provider CLI hidden, the way CI's machine looks.
#
#   scripts/test-without-provider-clis.sh <command> [args...]
#   e.g. scripts/test-without-provider-clis.sh cargo nextest run -p intent-services
#
# CI runners install Node but none of the provider CLIs (codex, claude,
# opencode, auggie, ...). A test that reaches provider discovery passes on a
# developer machine that has them and fails on CI (intentd#2462). This script
# reproduces CI's view locally: inside a private mount namespace (bubblewrap)
# every provider CLI the daemon could discover is masked with /dev/null and
# the provider home folders (~/.codex, ~/.claude, ...) are replaced by empty
# tmpfs mounts. Nothing outside the namespace changes. Daemons spawned by
# e2e tests get an empty user specialists folder, as on CI (unit tests never
# read the real one). Node stays visible, as on CI: a version-manager shim
# shared with node/npx (Volta, mise) is left alone and reported instead.
#
# Mark a provider installed in a test with intent-services'
# test_support::provider_installed instead of relying on the host.
#
# Environment:
#   HIDE_NODE=1         also hide node / npx / npm / corepack (stricter than
#                       CI, which installs Node; Node-requiring tests fail)
#   SKIP_SELF_CHECK=1   skip the discovery self-check below
#
# Before running the command, a self-check asks the daemon's own provider
# discovery (intent-providers `no_provider_cli_visible`, an ignored test)
# whether any provider CLI is still visible, and fails naming the path if so.
# Linux only: needs bwrap (Debian/Ubuntu: apt-get install bubblewrap).
set -euo pipefail

# Keep in sync with the provider registry; intent-providers'
# `hide_list_covers_every_provider_cli` test fails when a name is missing.
PROVIDER_CLIS=(
  antigravity-acp
  auggie
  claude
  claude-agent-acp
  codex
  codex-acp
  cortex-acp
  droid
  grok
  opencode
  pi
  pi-acp
  unsloth
)
NODE_TOOLS=(node npx npm corepack)
# Provider home folders: native installers, credentials and adapter state.
PROVIDER_HOMES=(.augment .claude .codex .factory .grok .opencode .pi .unsloth)

die() {
  echo "test-without-provider-clis: $*" >&2
  exit 2
}

[[ $# -gt 0 ]] || die "usage: $0 <command> [args...]"
[[ "$(uname -s)" == Linux ]] || die "Linux only (needs a private mount namespace via bwrap)"
command -v bwrap >/dev/null 2>&1 ||
  die "bwrap not found: install bubblewrap (Debian/Ubuntu: sudo apt-get install bubblewrap)"
[[ -n "${HOME:-}" ]] || die "HOME is not set"

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

names=("${PROVIDER_CLIS[@]}")
if [[ "${HIDE_NODE:-0}" == 1 ]]; then
  names+=("${NODE_TOOLS[@]}")
fi

# Every directory provider discovery scans (intent_core::path_utils): the
# inherited PATH, the login shell's PATH, the fixed tool dirs and the
# user-local and native installer dirs.
dirs=()
IFS=: read -r -a path_dirs <<<"${PATH:-}"
dirs+=("${path_dirs[@]}")
# setsid: an interactive shell attached to a terminal is stopped by job
# control (SIGTTIN/SIGTTOU), and a stopped process ignores timeout's TERM.
login_path=$(timeout -s KILL 10 setsid "${SHELL:-/bin/sh}" -ilc 'printf "__INTENT_PATH__%s__INTENT_PATH__" "$PATH"' </dev/null 2>/dev/null || true)
if [[ "$login_path" == *__INTENT_PATH__*__INTENT_PATH__* ]]; then
  login_path=${login_path#*__INTENT_PATH__}
  login_path=${login_path%%__INTENT_PATH__*}
  IFS=: read -r -a login_dirs <<<"$login_path"
  dirs+=("${login_dirs[@]}")
fi
dirs+=(/usr/local/bin /usr/bin /bin /usr/sbin /sbin /opt/homebrew/bin /usr/local/opt/node/bin)
for sub in .npm-global/bin .npm-packages/bin .local/bin .volta/bin .asdf/shims; do
  dirs+=("$HOME/$sub")
done
for dir in "$HOME"/.nvm/versions/node/*/bin; do
  if [[ -d "$dir" ]]; then dirs+=("$dir"); fi
done
declare -A seen_dirs=()
unique_dirs=()
for dir in "${dirs[@]}"; do
  [[ -n "$dir" && -d "$dir" && -z "${seen_dirs[$dir]:-}" ]] || continue
  seen_dirs[$dir]=1
  unique_dirs+=("$dir")
done
dirs=("${unique_dirs[@]}")

# Files node/npx resolve to stay usable unless HIDE_NODE=1: masking a shim a
# provider CLI shares with them (Volta's volta-shim, mise) would hide Node.
declare -A kept=()
if [[ "${HIDE_NODE:-0}" != 1 ]]; then
  for dir in "${dirs[@]}"; do
    [[ -n "$dir" && -d "$dir" ]] || continue
    for name in "${NODE_TOOLS[@]}"; do
      if [[ -e "$dir/$name" ]]; then kept[$(readlink -f "$dir/$name")]=1; fi
    done
  done
fi

args=(--dev-bind / /)
declare -A masked=()
for dir in "${dirs[@]}"; do
  [[ -n "$dir" && -d "$dir" ]] || continue
  for name in "${names[@]}"; do
    candidate="$dir/$name"
    [[ -e "$candidate" ]] || continue
    real=$(readlink -f "$candidate")
    [[ -f "$real" && -z "${masked[$real]:-}" ]] || continue
    if [[ -n "${kept[$real]:-}" ]]; then
      echo "test-without-provider-clis: cannot hide $candidate: it shares $real with Node (the self-check will report it)" >&2
      continue
    fi
    masked[$real]=1
    args+=(--ro-bind /dev/null "$real")
  done
done
for home_dir in "${PROVIDER_HOMES[@]}"; do
  if [[ -d "$HOME/$home_dir" ]]; then args+=(--tmpfs "$HOME/$home_dir"); fi
done

scratch=$(mktemp -d "${TMPDIR:-/tmp}/intentd-no-provider-clis.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/user-specialists"
args+=(--setenv INTENTD_USER_SPECIALISTS_DIR "$scratch/user-specialists")
if [[ "${HIDE_NODE:-0}" == 1 ]]; then
  args+=(--setenv INTENTD_EXPECT_NO_NODE 1)
fi

echo "test-without-provider-clis: hiding ${#masked[@]} CLI file(s) and provider home folders" >&2
if [[ "${SKIP_SELF_CHECK:-0}" != 1 ]]; then
  (cd "$repo_root" && bwrap "${args[@]}" cargo test --quiet -p intent-providers --lib \
    -- --ignored --exact provider_cli_guard::no_provider_cli_visible) ||
    die "self-check failed: a provider CLI is still visible (see the test output above)"
fi
bwrap "${args[@]}" "$@"
