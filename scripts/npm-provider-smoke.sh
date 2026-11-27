#!/usr/bin/env bash
#
# Run every pinned npm provider package the way Intent runs it, on one npm
# version and from an empty npm cache: the ignored test
# intent-services npm_provider_smoke (see its module docs).
#
#   scripts/npm-provider-smoke.sh <latest|oldest|VERSION> [OUT_DIR]
#
# `oldest` is NPX_MIN_NPM_VERSION, the oldest npm Intent accepts. The npm is
# installed into a temp folder with the host's npm and node; the host's own
# npm cache is never read. Writes OUT_DIR/npm-<spec>.log (default
# target/npm-provider-smoke) and exits non-zero when any launch fails.
# Inherited npm_config_* settings and the user npmrc are ignored. Run
# nightly by .github/workflows/nightly-npm-provider-smoke.yml.
# Bash 3.2 compatible.

set -euo pipefail

# A shell started by npx (an Intent agent, an npm script) carries its
# npm_config_* settings, and a user npmrc can skip scripts: neither may
# change what Intent's own launches would do on a clean host.
for name in $(compgen -e); do
  case $name in npm_config_* | NPM_CONFIG_*) unset "$name" ;; esac
done
export npm_config_userconfig=/dev/null

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
spec=${1:?usage: scripts/npm-provider-smoke.sh <latest|oldest|VERSION> [OUT_DIR]}
out=${2:-$repo_root/target/npm-provider-smoke}

if [ "$spec" = oldest ]; then
  spec=$(sed -nE 's/^pub const NPX_MIN_NPM_VERSION: &str = "([^"]+)";$/\1/p' \
    "$repo_root/crates/intent-providers/src/config.rs")
  [ -n "$spec" ] || { echo "npm-provider-smoke: cannot read NPX_MIN_NPM_VERSION" >&2; exit 2; }
fi
case "$spec" in
  latest | [0-9]*) ;;
  *) echo "npm-provider-smoke: unknown npm version '$spec'" >&2; exit 2 ;;
esac

mkdir -p "$out"
out=$(cd "$out" && pwd -P)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

npm_config_cache="$work/install-cache" npm install --prefix "$work/npm" \
  --no-save --no-package-lock --no-audit --no-fund --loglevel=error "npm@$spec" >&2
# Intent runs the npx beside node (intent_providers::find_npx), so the npm
# under test gets a node of its own: a copy, since a symlink resolves back to
# the host toolchain's npx.
bin="$work/bin"
mkdir -p "$bin"
cp "$(command -v node)" "$bin/node"
ln -s "$work/npm/node_modules/npm/bin/npm-cli.js" "$bin/npm"
ln -s "$work/npm/node_modules/npm/bin/npx-cli.js" "$bin/npx"
version=$("$bin/npm" --version)
log="$out/npm-${1}.log"
echo "npm $version (npm@$spec), node $(node --version)" | tee "$log"

status=0
(
  cd "$repo_root"
  PATH="$bin:$PATH" \
    npm_config_cache="$work/cache" \
    npm_config_update_notifier=false \
    INTENT_NPM_SMOKE_VERSION="$version" \
    INTENTD_TEST_ALLOW_EGRESS=1 \
    cargo test --locked -p intent-services --lib -- --ignored --exact --nocapture \
    npm_provider_smoke::pinned_npm_provider_packages_run_from_an_empty_npm_cache
) 2>&1 | tee -a "$log" || status=$?
exit "$status"
