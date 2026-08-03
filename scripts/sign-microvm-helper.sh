#!/usr/bin/env bash
# Ad-hoc signs intentd-microvm-helper with the Hypervisor entitlement.
#
# libkrun needs hv_vm_create, which macOS grants only to binaries signed with
# the com.apple.security.hypervisor entitlement — an unsigned helper fails at
# boot with krun_start_enter = -22 (EINVAL). Ad-hoc identity (`-`) is enough
# for local development on Apple Silicon; release builds will use the real
# signing identity in CI (tracked with the orchestrator packaging work).
#
# Usage:
#   cargo build -p intentd-microvm-helper
#   scripts/sign-microvm-helper.sh [path-to-binary]
#
# Default binary path: target/debug/intentd-microvm-helper (relative to the
# repo root, i.e. this script's parent directory).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${1:-$REPO_ROOT/target/debug/intentd-microvm-helper}"
ENTITLEMENTS="$REPO_ROOT/crates/intentd-microvm-helper/entitlements/hypervisor.entitlements"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "sign-microvm-helper: macOS only (codesign)" >&2
  exit 1
fi
if [[ ! -f "$BIN" ]]; then
  echo "sign-microvm-helper: binary not found: $BIN" >&2
  echo "  build it first: cargo build -p intentd-microvm-helper" >&2
  exit 1
fi

codesign --force --sign - --entitlements "$ENTITLEMENTS" "$BIN"
echo "signed (ad-hoc, hypervisor entitlement): $BIN"
