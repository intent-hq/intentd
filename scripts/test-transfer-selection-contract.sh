#!/usr/bin/env bash
# Cargo builds the normal intent-services lib test. The compile-time revision
# makes an old test binary unable to claim provenance from a newer checkout.
set -euo pipefail
component_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$component_root"
export CARGO_TERM_PROGRESS_WHEN=never
export TRANSFER_SELECTION_BUILD_REVISION
TRANSFER_SELECTION_BUILD_REVISION=$(git rev-parse HEAD)
unset TRANSFER_SELECTION_OUTPUT TRANSFER_SELECTION_REGENERATE
case "${1:-}" in
  '') [[ $# == 0 ]] ;;
  --output) [[ $# == 2 && -n "$2" ]] || exit 2; export TRANSFER_SELECTION_OUTPUT="$2" ;;
  --regenerate) [[ $# == 1 ]] || exit 2; export TRANSFER_SELECTION_REGENERATE=1 ;;
  *) echo "Usage: $0 [--output ABSOLUTE_PATH | --regenerate]" >&2; exit 2 ;;
esac
if [[ $# != 0 && -n "$(git status --porcelain --untracked-files=all)" ]]; then
  echo 'Export requires clean committed intentd source; checking never rewrites fixtures.' >&2
  exit 1
fi
cargo test -p intent-services --lib transfer_selection_contract::public_import_matches_contract -- --exact --nocapture
