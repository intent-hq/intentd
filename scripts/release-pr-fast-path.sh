#!/usr/bin/env bash
# Usage: scripts/release-pr-fast-path.sh <base> [<head>]
# Prints fast_path=true only for release-plz metadata; false for unknown shapes.
# Requires Python 3.11+ (tomllib). A nonzero exit must also be treated as a miss.
set -euo pipefail
exec python3 -S "$(dirname -- "${BASH_SOURCE[0]}")/release-pr-fast-path.py" "$@"
