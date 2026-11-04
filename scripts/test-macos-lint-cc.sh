#!/usr/bin/env bash
# Regression tests for macos-lint-cc.sh, the stub target compiler behind
# CI's `macos-cross-lint` job and `make check-macos`. Each case is a call
# shape a -sys build script or sccache makes when cross-linting for
# aarch64-apple-darwin on Linux:
#
# - a compile with darwin-only flags (cc-rs's ring flags: -arch arm64,
#   -mmacosx-version-min, -gfull) writes an object, and the dependency file
#   when -MD/-MF asks for one (sccache failed without it on openssl-src);
# - cc-rs's compiler-family probe (a bare source, no -E) preprocesses with
#   the host compiler, so the stub is detected as GNU, not MSVC;
# - `-?` / `--version` probes with no source fail, never read stdin;
# - an expand probe sees macros and include paths (openssl-sys's version
#   probe), while darwin flags never reach the host compiler;
# - a source the host cannot preprocess (OpenSSL's arm_arch.h #error on x86)
#   is emitted verbatim instead of failing the build script.
#
# Run directly: ./scripts/test-macos-lint-cc.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
stub="$here/macos-lint-cc.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
failures=0

fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

mkdir -p "$tmp/inc"
printf '#define LINT_HEADER_VALUE 42\n' >"$tmp/inc/lint.h"
cat >"$tmp/probe.c" <<'EOF'
#include "lint.h"
lint_value LINT_HEADER_VALUE LINT_DEFINED
EOF
cat >"$tmp/arm.c" <<'EOF'
#if !defined(__aarch64__) && !defined(__arm__)
#error "unsupported ARM architecture"
#endif
int arm_only;
EOF
printf 'int real_code(void) { return 1; }\n' >"$tmp/real.c"
cat >"$tmp/detect.c" <<'EOF'
#ifdef __clang__
detected_clang
#elif defined(__GNUC__)
detected_gnu
#else
detected_other
#endif
EOF

# Compile with the darwin flags cc-rs passes for ring: an object, no dep file.
if ! "$stub" -O0 -fPIC -arch arm64 -mmacosx-version-min=11.0 -gfull -Wall \
    -I "$tmp/inc" -DNDEBUG -o "$tmp/real.o" -c "$tmp/real.c"; then
  fail "compile with darwin flags exited non-zero"
fi
[ -f "$tmp/real.o" ] || fail "compile did not write the -o object"
[ ! -e "$tmp/real.d" ] || fail "compile without -MD wrote a dep file"

# -MD -MF (openssl-src's make rules, sccache's dep-file read).
"$stub" -MMD -MF "$tmp/dep.d.tmp" -MT "$tmp/dep.o" -c -o "$tmp/dep.o" "$tmp/real.c" \
  || fail "compile with -MF exited non-zero"
[ -f "$tmp/dep.o" ] || fail "-MF compile did not write the object"
grep -qx "$tmp/dep.o:" "$tmp/dep.d.tmp" 2>/dev/null || fail "-MF dep file missing or wrong"

# -MD without -MF writes <object stem>.d next to the object.
"$stub" -MD -c "$tmp/real.c" -o "$tmp/implicit.o" || fail "-MD compile exited non-zero"
[ -f "$tmp/implicit.d" ] || fail "-MD without -MF did not write implicit.d"

# Joined -o<path>.
"$stub" -c "$tmp/real.c" "-o$tmp/joined.o" || fail "joined -o compile exited non-zero"
[ -f "$tmp/joined.o" ] || fail "joined -o<path> did not write the object"

# A compile of a source the host cannot even preprocess still succeeds:
# the stub never reads it.
"$stub" -arch arm64 -c "$tmp/arm.c" -o "$tmp/arm.o" || fail "compile of an ARM-only source failed"

# cc-rs family detection: a bare source, no -E, preprocessed by the host.
detected=$("$stub" "$tmp/detect.c" 2>/dev/null) || fail "family detection exited non-zero"
case "$detected" in
  *detected_gnu*|*detected_clang*) ;;
  *) fail "family detection did not report a GNU/clang compiler: $detected" ;;
esac

# MSVC and version probes have no source: they must fail without reading stdin.
for probe in "-?" "--version" "-vV"; do
  if echo "not a source" | "$stub" "$probe" >/dev/null 2>&1; then
    fail "probe $probe succeeded (cc-rs would detect MSVC)"
  fi
done

# Expand probe: macros and include paths reach the host preprocessor, the
# darwin-only flags do not (gcc rejects -arch and -gfull outright).
expanded=$("$stub" -E -arch arm64 -isysroot /nonexistent -gfull -mmacosx-version-min=11.0 \
  -I "$tmp/inc" -DLINT_DEFINED=yes "$tmp/probe.c") || fail "expand probe exited non-zero"
case "$expanded" in
  *"lint_value 42 yes"*) ;;
  *) fail "expand probe lost the include path or macro: $expanded" ;;
esac
"$stub" -E -I"$tmp/inc" -DLINT_DEFINED=joined "$tmp/probe.c" -o "$tmp/probe.i" \
  || fail "expand probe with -o exited non-zero"
grep -q "lint_value 42 joined" "$tmp/probe.i" 2>/dev/null || fail "expand -o output missing joined flags"

# sccache preprocesses a target-only source for its cache key: the host
# rejects it, so the source comes back verbatim.
verbatim=$("$stub" -E -arch arm64 "$tmp/arm.c") || fail "ARM-only preprocess failed"
case "$verbatim" in
  *"int arm_only;"*) ;;
  *) fail "ARM-only preprocess did not fall back to the source: $verbatim" ;;
esac

if [ "$failures" -gt 0 ]; then
  echo "$failures macos-lint-cc.sh check(s) failed" >&2
  exit 1
fi
echo "macos-lint-cc.sh: all checks passed"
