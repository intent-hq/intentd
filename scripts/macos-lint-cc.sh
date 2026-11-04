#!/usr/bin/env bash
# Stub C/C++ compiler for lint-only aarch64-apple-darwin builds on Linux.
#
# `cargo check` / `cargo clippy --target aarch64-apple-darwin` never link, but
# they still run every build script, and the -sys crates' build scripts
# (libz-sys, libgit2-sys, libssh2-sys, liblzma-sys, libsqlite3-sys, ring,
# rquickjs-sys, openssl-src via openssl-sys's `vendored` feature) compile C
# for the target. A real darwin cross toolchain needs the macOS SDK, which
# Linux runners do not have. Clippy only needs those build scripts to
# succeed and to print their cargo: directives; it never reads the objects.
# So this stub stands in for the target compiler (CC_/CXX_aarch64_apple_darwin):
#
# - a compile (`-c` without `-E`) writes an empty host object to the `-o`
#   path, plus the `-MF` (or `-MD`) dependency file that make and sccache
#   expect, and succeeds;
# - anything else (cc-rs compiler-family detection, `cc::Build::expand` such
#   as openssl-sys's version probe) is preprocessing: it
#   runs the host `cc -E` with only the preprocessor flags (include paths,
#   macros, language, standard), so cc-rs detects a GNU compiler and expand
#   probes read the real headers. Target-only flags (-arch, -isysroot,
#   -mmacosx-version-min, -gfull, ...) are dropped.
# - A call with no source file (a bare `-?` or `--version` probe) fails.
# - When host preprocessing fails (a header that rejects the host
#   architecture, as OpenSSL's arm_arch.h does when sccache preprocesses a
#   source for its cache key), the sources are emitted verbatim instead.
#
# Used by CI's `macos-cross-lint` job and `make check-macos`. Never use it
# for a build that links: every object it writes is empty.
set -u

cc_host=${MACOS_LINT_HOST_CC:-cc}
out=""
dep=""
md=0
compile=0
pre=0
args=()
while [ $# -gt 0 ]; do
  case "$1" in
    -c) compile=1 ;;
    -E) pre=1; args+=("$1") ;;
    -o) out=${2-}; shift ;;
    -o?*) out=${1#-o} ;;
    -MF) dep=${2-}; shift ;;
    -MD|-MMD) md=1 ;;
    -I|-D|-U|-include|-x|-isystem|-iquote) args+=("$1" "${2-}"); shift ;;
    -I?*|-D?*|-U?*|-std=*|-P|-dM|-w|-x?*) args+=("$1") ;;
    -arch|-isysroot|-target|-MT|-MQ|-Xclang|-Xassembler|-Xlinker|-Xpreprocessor) shift ;;
    -*) ;;
    *) args+=("$1") ;;
  esac
  [ $# -eq 0 ] || shift
done

if [ "$compile" = 1 ] && [ "$pre" = 0 ]; then
  [ -n "$out" ] || exit 0
  "$cc_host" -c -x c /dev/null -o "$out" || exit 1
  if [ "$md" = 1 ] && [ -z "$dep" ]; then dep="${out%.*}.d"; fi
  if [ -n "$dep" ]; then printf '%s:\n' "$out" > "$dep" || exit 1; fi
  exit 0
fi

srcs=()
for a in ${args[@]+"${args[@]}"}; do
  case "$a" in
    -*) ;;
    *) if [ -f "$a" ]; then srcs+=("$a"); fi ;;
  esac
done
# No readable source (a bare `-?`, `--version` or `-vV` probe): fail, so
# cc-rs never mistakes this stub for MSVC, and never read stdin.
if [ "${#srcs[@]}" -eq 0 ]; then
  echo "macos-lint-cc.sh: no source file to preprocess" >&2
  exit 1
fi

if [ "$pre" = 0 ]; then args=(-E "${args[@]}"); fi
outargs=()
if [ -n "$out" ]; then outargs=(-o "$out"); fi
if "$cc_host" "${args[@]}" ${outargs[@]+"${outargs[@]}"} 2>/dev/null </dev/null; then exit 0; fi
if [ -n "$out" ]; then
  cat "${srcs[@]}" > "$out"
else
  cat "${srcs[@]}"
fi
