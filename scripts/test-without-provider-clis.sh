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

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)

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
# The login shell's environment, captured the way intent_core::path_utils
# does: a shell-neutral command (works under bash, zsh and fish) printing a
# NUL-separated `env -0` dump between sentinels, in a new session (setsid)
# because an interactive shell on a terminal is stopped by job control and a
# stopped process ignores timeout's TERM. Falls back from -ilc to -lc.
login_env=
for flags in -ilc -lc; do
  login_env=$(timeout -s KILL 10 setsid "${SHELL:-/bin/sh}" "$flags" \
    "printf '__INTENT_ENV__'; /usr/bin/env -0 2>/dev/null; printf '__INTENT_END__'" \
    </dev/null 2>/dev/null | tr '\0' '\n' || true)
  [[ "$login_env" == *__INTENT_ENV__*__INTENT_END__* ]] && break
  login_env=
done
login_env=${login_env#*__INTENT_ENV__}
login_env=${login_env%%__INTENT_END__*}
login_value() { sed -n "s/^$1=//p" <<<"$login_env" | head -n 1; }
IFS=: read -r -a login_dirs <<<"$(login_value PATH)"
dirs+=("${login_dirs[@]}")
# Volta's home as discovery resolves it (path_utils::enriched_tool_dirs_for):
# the inherited VOLTA_HOME when set (even empty), else the login shell's;
# an empty value falls back to ~/.volta.
if [[ -v VOLTA_HOME ]]; then
  volta_home=$VOLTA_HOME
else
  volta_home=$(login_value VOLTA_HOME)
fi
volta_home=${volta_home:-$HOME/.volta}
dirs+=(/usr/local/bin /usr/bin /bin /usr/sbin /sbin /opt/homebrew/bin /usr/local/opt/node/bin)
dirs+=("$volta_home/bin")
# User-local tool dirs plus the native installer tiers discovery probes
# directly (find_provider_binary: ~/.grok/bin, ~/.opencode/bin,
# ~/.augment/bin), so their CLIs are masked even when the provider home
# itself is kept below.
for sub in .npm-global/bin .npm-packages/bin .local/bin .asdf/shims .grok/bin .opencode/bin .augment/bin; do
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
candidates=()
for dir in "${dirs[@]}"; do
  [[ -n "$dir" && -d "$dir" ]] || continue
  for name in "${names[@]}"; do
    candidates+=("$dir/$name")
  done
done
# auggie's install record (~/.augment/auggie-path): the first non-blank line
# names the binary discovery runs (auggie_marker_path_with_home). An
# unreadable record names nothing, as in discovery (read_to_string().ok()).
if [[ -f "$HOME/.augment/auggie-path" && -r "$HOME/.augment/auggie-path" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    line=$(printf '%s' "$line" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
    if [[ -n "$line" ]]; then
      if [[ "$line" == /* ]]; then candidates+=("$line"); fi
      break
    fi
  done <"$HOME/.augment/auggie-path"
fi
for candidate in "${candidates[@]}"; do
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
# An empty tmpfs would also hide a checkout (or cwd) living under a provider
# home, e.g. a Codex worktree in ~/.codex/worktrees: keep that home, whose
# CLI files are still masked one by one above.
cwd_real=$(pwd -P)
for home_dir in "${PROVIDER_HOMES[@]}"; do
  home_path="$HOME/$home_dir"
  [[ -d "$home_path" ]] || continue
  home_real=$(cd "$home_path" && pwd -P)
  case "$repo_root/" in "$home_real"/*)
    echo "test-without-provider-clis: keeping $home_path (it contains $repo_root); its CLIs are still masked" >&2
    continue ;;
  esac
  case "$cwd_real/" in "$home_real"/*)
    echo "test-without-provider-clis: keeping $home_path (it contains $cwd_real); its CLIs are still masked" >&2
    continue ;;
  esac
  args+=(--tmpfs "$home_path")
done

scratch=$(mktemp -d "${TMPDIR:-/tmp}/intentd-no-provider-clis.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/user-specialists"
args+=(--setenv INTENTD_USER_SPECIALISTS_DIR "$scratch/user-specialists")
if [[ "${HIDE_NODE:-0}" == 1 ]]; then
  args+=(--setenv INTENTD_EXPECT_NO_NODE 1)
fi
# Set by this script only (not by callers): tells a test runner inside that
# it really runs sandboxed, and whether the self-check vouched for it, e.g.
# so resumable gate records keep sandboxed passes separate.
self_check=checked
if [[ "${SKIP_SELF_CHECK:-0}" == 1 ]]; then self_check=skipped; fi
# A PID namespace (with its own /proc) so that when this script is killed,
# --die-with-parent takes down the whole tree (cargo, nextest, test daemons),
# not just the immediate command.
args+=(--setenv INTENTD_PROVIDER_CLIS_HIDDEN "$self_check" --unshare-pid --proc /proc --die-with-parent)

echo "test-without-provider-clis: hiding ${#masked[@]} CLI file(s) and provider home folders" >&2
if [[ "${SKIP_SELF_CHECK:-0}" != 1 ]]; then
  (cd "$repo_root" && bwrap "${args[@]}" cargo test --quiet -p intent-providers --lib \
    -- --ignored --exact provider_cli_guard::no_provider_cli_visible) ||
    die "self-check failed: a provider CLI is still visible (see the test output above)"
fi
# Foreground, so a terminal Ctrl-C reaches the whole sandboxed run. A
# supervisor that signals only this script kills it, and --die-with-parent
# then tears the sandbox down instead of leaving the run orphaned.
bwrap "${args[@]}" "$@"
