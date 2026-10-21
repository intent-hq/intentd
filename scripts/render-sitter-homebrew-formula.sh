#!/usr/bin/env bash
# Render required platforms plus a successful Intel build. If Intel is absent,
# retain the prior formula's Intel URL/checksum with its architecture-scoped
# version; never relabel an old binary as the current release.
# Usage: render-sitter-homebrew-formula.sh <version> <artifacts-dir> <output> [previous-formula]
set -euo pipefail
exec python3 -I "$(dirname "${BASH_SOURCE[0]}")/render-sitter-homebrew-formula.py" "$@"
